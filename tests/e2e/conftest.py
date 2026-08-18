"""Deterministic real-service fixtures for the cross-interface workflow."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Iterable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from makolet.adapters.archive.local import LocalContentAddressedArchive
from makolet.adapters.parsers import RetailXmlParser
from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.destructive_target import (
    DestructiveDatabaseTargetError,
    require_test_database_target,
)
from makolet.adapters.persistence.ingestion import PostgresIngestionRepository
from makolet.adapters.persistence.leases import PostgresLeaseManager
from makolet.adapters.persistence.queries import PostgresQueryRepository
from makolet.adapters.persistence.schema import metadata
from makolet.application.ingestion import IngestionPolicy, IngestionService
from makolet.application.models import (
    ApplySummary,
    ArchivedDownload,
    DiscoveryCursor,
    DiscoveryPage,
    DiscoveryRunBudget,
    DownloadEvidence,
    IngestionResult,
    RegisteredSourceFile,
    StageSummary,
)
from makolet.application.ports import DownloadSession
from makolet.application.queries import QueryService
from makolet.domain.enums import CompressionFormat, DocumentType, SourceProtocol
from makolet.domain.models import ParsedEvent, RemoteFile
from tests.fakes.ingestion import FakeMetrics, FixedClock

_FIXTURE_ROOT = Path(__file__).parent / "fixtures"
_BARCODE = "4006381333931"
_RETAILER = "e2e-clean-room-market"
_PORTAL = "e2e-fixture-portal"
_DISCOVERED_AT = datetime(2026, 8, 11, 10, 0, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class ArchivedFixture:
    source_file_id: UUID
    object_key: str
    sha256: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class WorkflowEvent:
    remote_id: str
    phase: str


@dataclass(frozen=True, slots=True)
class SeededWorkflow:
    database_url: str
    archive_root: Path
    product_id: UUID
    store_id: UUID
    query_at: datetime
    initial_price: IngestionResult
    changed_price: IngestionResult
    duplicate_price: IngestionResult
    promotion: IngestionResult
    replayed_price: IngestionResult
    archives: tuple[ArchivedFixture, ...]
    discovered_remote_ids: tuple[str, ...]
    events: tuple[WorkflowEvent, ...]


@dataclass(frozen=True, slots=True)
class RealQueryServices:
    database: Database
    queries: QueryService


class QueryClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


class WorkflowTrace:
    """Record which production boundary completed for each discovered fixture."""

    def __init__(self) -> None:
        self.events: list[WorkflowEvent] = []
        self._remote_ids_by_source_file: dict[UUID, str] = {}

    def discover(self, remote_file: RemoteFile) -> None:
        self.events.append(WorkflowEvent(remote_file.remote_id, "discover"))

    def bind(self, registered: RegisteredSourceFile) -> None:
        self._remote_ids_by_source_file[registered.source_file_id] = (
            registered.remote_file.remote_id
        )

    def remote_phase(self, remote_file: RemoteFile, phase: str) -> None:
        self.events.append(WorkflowEvent(remote_file.remote_id, phase))

    def source_file_phase(self, source_file_id: UUID, phase: str) -> None:
        try:
            remote_id = self._remote_ids_by_source_file[source_file_id]
        except KeyError as error:
            raise AssertionError("workflow phase preceded source-file registration") from error
        self.events.append(WorkflowEvent(remote_id, phase))


class CleanRoomFixtureSource:
    """Paginated fixture discovery; downloading remains a separate port."""

    source_id = _RETAILER

    def __init__(self, files: tuple[RemoteFile, ...], trace: WorkflowTrace) -> None:
        self._files = files
        self._trace = trace

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: DiscoveryRunBudget | None = None,
    ) -> DiscoveryPage:
        del budget
        if not 1 <= limit <= 500:
            raise ValueError("fixture discovery limit is invalid")
        try:
            offset = int(cursor.value) if cursor is not None and cursor.value is not None else 0
        except ValueError as error:
            raise ValueError("fixture discovery cursor is invalid") from error
        if not 0 <= offset <= len(self._files):
            raise ValueError("fixture discovery cursor is out of range")
        selected = self._files[offset : offset + limit]
        for remote_file in selected:
            self._trace.discover(remote_file)
        next_offset = offset + len(selected)
        next_cursor = DiscoveryCursor(str(next_offset)) if next_offset < len(self._files) else None
        return DiscoveryPage(selected, next_cursor, next_cursor is None)


class CleanRoomFixtureDownloadSession:
    """Stream independently authored fixture bytes through the downloader port."""

    def __init__(self, payload: bytes, clock: FixedClock) -> None:
        self._payload = payload
        self._clock = clock
        self._started_at = clock.now()
        self._consumed = 0

    @property
    def transferred_bytes(self) -> int:
        return self._consumed

    async def iter_raw(self) -> AsyncIterator[bytes]:
        midpoint = len(self._payload) // 2
        for chunk in (self._payload[:midpoint], self._payload[midpoint:]):
            if chunk:
                self._consumed += len(chunk)
                yield chunk

    async def finish(self, content_length: int) -> DownloadEvidence:
        if content_length != self._consumed:
            raise AssertionError("archive did not consume the exact downloaded fixture bytes")
        return DownloadEvidence(
            started_at=self._started_at,
            finished_at=self._clock.now(),
            status_code=200,
            content_length=content_length,
            media_type="application/xml",
            etag=None,
            last_modified=None,
        )


class CleanRoomFixtureDownloader:
    """Download fixture payloads separately from discovery and parsing."""

    def __init__(
        self,
        payloads_by_url: dict[str, bytes],
        clock: FixedClock,
        trace: WorkflowTrace,
    ) -> None:
        self._payloads_by_url = payloads_by_url
        self._clock = clock
        self._trace = trace

    def open(
        self,
        remote_file: RemoteFile,
        *,
        maximum_bytes: int | None = None,
    ) -> AbstractAsyncContextManager[DownloadSession]:
        del maximum_bytes
        return self._open(remote_file)

    @asynccontextmanager
    async def _open(self, remote_file: RemoteFile) -> AsyncIterator[DownloadSession]:
        self._trace.remote_phase(remote_file, "download")
        try:
            payload = self._payloads_by_url[remote_file.download_url]
        except KeyError as error:
            raise AssertionError("fixture discovery returned an unknown download URL") from error
        yield CleanRoomFixtureDownloadSession(payload, self._clock)


class TracedRetailXmlParser(RetailXmlParser):
    def __init__(self, trace: WorkflowTrace) -> None:
        super().__init__()
        self._trace = trace

    def parse(
        self,
        chunks: AsyncIterator[bytes],
        *,
        source_file_id: UUID,
        document_type: DocumentType,
        compression: CompressionFormat,
        filename: str,
    ) -> AsyncIterator[ParsedEvent]:
        self._trace.source_file_phase(source_file_id, "parse")
        return super().parse(
            chunks,
            source_file_id=source_file_id,
            document_type=document_type,
            compression=compression,
            filename=filename,
        )


class TracedPostgresIngestionRepository(PostgresIngestionRepository):
    def __init__(self, database: Database, trace: WorkflowTrace) -> None:
        super().__init__(database.engine)
        self._trace = trace

    async def register_discovery(
        self,
        remote_file: RemoteFile,
        *,
        owned_refresh: bool = False,
    ) -> RegisteredSourceFile:
        registered = await super().register_discovery(
            remote_file,
            owned_refresh=owned_refresh,
        )
        self._trace.bind(registered)
        return registered

    async def record_archive(
        self,
        source_file_id: UUID,
        archived: ArchivedDownload,
        *,
        parser_version: str,
    ) -> bool:
        duplicate = await super().record_archive(
            source_file_id,
            archived,
            parser_version=parser_version,
        )
        self._trace.source_file_phase(source_file_id, "archive")
        return duplicate

    async def stage(
        self,
        source_file_id: UUID,
        events: Iterable[ParsedEvent],
    ) -> StageSummary:
        summary = await super().stage(source_file_id, events)
        self._trace.source_file_phase(source_file_id, "stage")
        return summary

    async def apply(
        self,
        source_file_id: UUID,
        document_type: DocumentType,
        *,
        minimum_full_records: int,
        maximum_drop_fraction: float,
    ) -> ApplySummary:
        summary = await super().apply(
            source_file_id,
            document_type,
            minimum_full_records=minimum_full_records,
            maximum_drop_fraction=maximum_drop_fraction,
        )
        self._trace.source_file_phase(source_file_id, "apply")
        return summary


def _dedicated_database_url() -> str:
    url = os.environ.get("MAKOLET_TEST_DATABASE_URL")
    if not url:
        pytest.skip("MAKOLET_TEST_DATABASE_URL is not configured")
    try:
        return require_test_database_target(
            url,
            confirmation=os.environ.get("MAKOLET_TEST_DATABASE_CONFIRM"),
        )
    except DestructiveDatabaseTargetError as error:
        pytest.fail(str(error))


def _migrate_database(url: str) -> None:
    previous_url = os.environ.get("MAKOLET_DATABASE_URL")
    os.environ["MAKOLET_DATABASE_URL"] = url
    try:
        command.upgrade(Config("alembic.ini"), "head")
    finally:
        if previous_url is None:
            os.environ.pop("MAKOLET_DATABASE_URL", None)
        else:
            os.environ["MAKOLET_DATABASE_URL"] = previous_url


def _remote_file(
    *,
    remote_id: str,
    filename: str,
    document_type: DocumentType,
    payload: bytes,
    source_timestamp: datetime | None = None,
) -> RemoteFile:
    return RemoteFile(
        retailer_id=_RETAILER,
        portal_id=_PORTAL,
        protocol=SourceProtocol.FIXTURE,
        remote_id=remote_id,
        download_url=f"fixture://{remote_id}",
        original_filename=filename,
        document_type=document_type,
        compression=CompressionFormat.NONE,
        discovered_at=_DISCOVERED_AT,
        source_timestamp=source_timestamp or _DISCOVERED_AT,
        content_length=len(payload),
        media_type="application/xml",
    )


async def _reset_tables(database: Database) -> None:
    names = ", ".join(f'"{table.name}"' for table in metadata.sorted_tables)
    async with database.engine.begin() as connection:
        await connection.execute(text(f"TRUNCATE TABLE {names} CASCADE"))


async def _seed_workflow(url: str, archive_root: Path) -> SeededWorkflow:
    trace = WorkflowTrace()
    payloads_by_filename = {
        "stores.xml": (_FIXTURE_ROOT / "stores.xml").read_bytes(),
        "price-full.xml": (_FIXTURE_ROOT / "price-full.xml").read_bytes(),
        "price-delta.xml": (_FIXTURE_ROOT / "price-delta.xml").read_bytes(),
        "promo-full.xml": (_FIXTURE_ROOT / "promo-full.xml").read_bytes(),
    }
    remotes: tuple[RemoteFile, ...] = (
        _remote_file(
            remote_id="stores-20260811",
            filename="stores.xml",
            document_type=DocumentType.STORES,
            payload=payloads_by_filename["stores.xml"],
        ),
        _remote_file(
            remote_id="price-full-20260810",
            filename="price-full.xml",
            document_type=DocumentType.PRICE_FULL,
            payload=payloads_by_filename["price-full.xml"],
            source_timestamp=_DISCOVERED_AT - timedelta(days=1),
        ),
        _remote_file(
            remote_id="price-delta-20260811",
            filename="price-delta.xml",
            document_type=DocumentType.PRICE_DELTA,
            payload=payloads_by_filename["price-delta.xml"],
            source_timestamp=_DISCOVERED_AT,
        ),
        _remote_file(
            remote_id="price-delta-duplicate-20260811",
            filename="price-delta.xml",
            document_type=DocumentType.PRICE_DELTA,
            payload=payloads_by_filename["price-delta.xml"],
            source_timestamp=_DISCOVERED_AT + timedelta(minutes=1),
        ),
        _remote_file(
            remote_id="promo-full-20260811",
            filename="promo-full.xml",
            document_type=DocumentType.PROMOTION_FULL,
            payload=payloads_by_filename["promo-full.xml"],
        ),
    )
    source = CleanRoomFixtureSource(remotes, trace)
    discovered: list[RemoteFile] = []
    cursor: DiscoveryCursor | None = None
    while True:
        page = await source.discover(cursor, limit=2)
        discovered.extend(page.files)
        if page.next_cursor is None:
            if not page.complete:
                raise AssertionError("fixture discovery ended without a complete page")
            break
        cursor = page.next_cursor
    remotes = tuple(discovered)
    payloads_by_url = {
        remote.download_url: payloads_by_filename[remote.original_filename] for remote in remotes
    }
    clock = FixedClock()
    database = Database.from_url(url, pool_size=2, max_overflow=1)
    archive = LocalContentAddressedArchive(archive_root, maximum_object_bytes=1024 * 1024)
    repository = TracedPostgresIngestionRepository(database, trace)
    service = IngestionService(
        repository,
        CleanRoomFixtureDownloader(payloads_by_url, clock, trace),
        archive,
        TracedRetailXmlParser(trace),
        PostgresLeaseManager(database),
        clock,
        FakeMetrics(),
        worker_id="e2e-fixture-worker",
        policy=IngestionPolicy(
            # One metadata row plus the promotion parent, two items, one effective
            # store, and one club keep every tiny fixture in a single stage call.
            stage_batch_size=6,
            download_attempts=1,
            minimum_full_records=1,
        ),
    )
    try:
        await _reset_tables(database)
        await service.ingest(remotes[0])
        initial = await service.ingest(remotes[1])
        changed = await service.ingest(remotes[2])
        replayed = await service.replay(changed.source_file_id)
        duplicate = await service.ingest(remotes[3])
        promotion = await service.ingest(remotes[4])

        async with database.engine.connect() as connection:
            query_at = (
                await connection.execute(
                    text(
                        "SELECT valid_from FROM promotions "
                        "WHERE source_file_id = :source_file_id "
                        "ORDER BY id LIMIT 1"
                    ),
                    {"source_file_id": promotion.source_file_id},
                )
            ).scalar_one()
        if not isinstance(query_at, datetime) or query_at.tzinfo is None:
            raise AssertionError("seeded promotion has no aware system-valid timestamp")

        query_repository = PostgresQueryRepository(database.engine)
        product = await query_repository.find_product_by_barcode(_BARCODE)
        if product is None:
            raise AssertionError("seeded GTIN was not assigned to a canonical product")
        product_id = UUID(str(product["id"]))
        prices = await query_repository.current_prices(
            product_id,
            retailer_id=None,
            store_id=None,
            limit=10,
            cursor=None,
        )
        if not prices.items:
            raise AssertionError("seeded product has no current price")
        store_id = UUID(str(prices.items[0]["store_id"]))

        archived: list[ArchivedFixture] = []
        for remote, result in zip(
            remotes,
            (None, initial, changed, duplicate, promotion),
            strict=True,
        ):
            registered = await repository.get(
                result.source_file_id
                if result is not None
                else (await repository.register_discovery(remote)).source_file_id
            )
            if registered.archive_object_key is None or registered.content_sha256 is None:
                raise AssertionError("seeded source file has no immutable archive identity")
            archived.append(
                ArchivedFixture(
                    source_file_id=registered.source_file_id,
                    object_key=registered.archive_object_key,
                    sha256=registered.content_sha256,
                    payload=payloads_by_url[remote.download_url],
                )
            )

        return SeededWorkflow(
            database_url=url,
            archive_root=archive_root,
            product_id=product_id,
            store_id=store_id,
            query_at=query_at,
            initial_price=initial,
            changed_price=changed,
            duplicate_price=duplicate,
            promotion=promotion,
            replayed_price=replayed,
            archives=tuple(archived),
            discovered_remote_ids=tuple(remote.remote_id for remote in remotes),
            events=tuple(trace.events),
        )
    finally:
        await database.dispose()


@pytest.fixture(scope="session")
def seeded_workflow(tmp_path_factory: pytest.TempPathFactory) -> SeededWorkflow:
    url = _dedicated_database_url()
    _migrate_database(url)
    archive_root = tmp_path_factory.mktemp("e2e-raw-archive")
    return asyncio.run(_seed_workflow(url, archive_root))


@pytest_asyncio.fixture
async def real_queries(seeded_workflow: SeededWorkflow) -> AsyncIterator[RealQueryServices]:
    database = Database.from_url(seeded_workflow.database_url, pool_size=2, max_overflow=1)
    try:
        yield RealQueryServices(
            database=database,
            queries=QueryService(
                PostgresQueryRepository(database.engine),
                QueryClock(seeded_workflow.query_at),
            ),
        )
    finally:
        await database.dispose()
