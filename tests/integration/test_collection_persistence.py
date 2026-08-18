"""Real-PostgreSQL proofs for durable bounded source traversal."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine

from makolet.adapters.persistence.collection import (
    PostgresCollectionLeaseManager,
    PostgresCollectionRepository,
)
from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.errors import PersistenceConflictError
from makolet.adapters.persistence.ingestion import PostgresIngestionRepository
from makolet.adapters.persistence.queries import PostgresQueryRepository
from makolet.adapters.persistence.registry import PostgresRegistryRepository
from makolet.adapters.persistence.schema import (
    collection_attempts,
    collection_checkpoints,
    collection_transfer_charges,
    source_files,
)
from makolet.application.collection import CollectionOperations, CollectionPolicy
from makolet.application.models import (
    CollectionScope,
    DiscoveryCursor,
    DiscoveryPage,
    DiscoveryRunBudget,
    IngestionResult,
    PortalRegistration,
    RegisteredSourceFile,
    RetailerRegistration,
)
from makolet.domain.enums import (
    CompressionFormat,
    DocumentType,
    IngestionStatus,
    SourceProtocol,
)
from makolet.domain.errors import LeaseUnavailableError, SourceAccessError, SourceResponseError
from makolet.domain.models import RemoteFile

pytestmark = pytest.mark.integration
NOW = datetime(2026, 8, 12, 9, tzinfo=UTC)
SOURCE_ID = "discovery-retailer"
PORTAL_ID = "discovery-portal"


class PagedCatalogAdapter:
    source_id = SOURCE_ID

    def __init__(self, files: tuple[RemoteFile, ...]) -> None:
        self.files = files
        self.calls: list[int] = []

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: DiscoveryRunBudget | None = None,
    ) -> DiscoveryPage:
        del budget
        offset = int(cursor.value) if cursor is not None and cursor.value is not None else 0
        if offset < 0 or offset > len(self.files):
            raise SourceResponseError("test publisher cursor is stale")
        self.calls.append(offset)
        selected = self.files[offset : offset + limit]
        next_offset = offset + len(selected)
        next_cursor = DiscoveryCursor(str(next_offset)) if next_offset < len(self.files) else None
        return DiscoveryPage(selected, next_cursor, next_cursor is None)


class PersistentIngestion:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self.calls: list[str] = []
        self.fail_once: set[str] = set()

    async def register(self, remote_file: RemoteFile) -> RegisteredSourceFile:
        return await PostgresIngestionRepository(self._engine).register_discovery(remote_file)

    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        del maximum_charged_bytes
        self.calls.append(remote_file.remote_id)
        if remote_file.remote_id in self.fail_once:
            self.fail_once.remove(remote_file.remote_id)
            raise SourceAccessError("simulated retryable publisher failure")
        source_file_id = await self._complete(remote_file)
        return IngestionResult(
            source_file_id=source_file_id,
            status=IngestionStatus.COMPLETED,
            content_sha256=None,
            stage=None,
            apply=None,
        )

    async def archive_only(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        del maximum_charged_bytes
        return await self.ingest(remote_file)

    async def replay(
        self,
        source_file_id: UUID,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> IngestionResult:
        del rebuild_run_id
        return IngestionResult(
            source_file_id=source_file_id,
            status=IngestionStatus.COMPLETED,
            content_sha256=None,
            stage=None,
            apply=None,
            replayed=True,
        )

    async def _complete(self, remote_file: RemoteFile) -> UUID:
        async with self._engine.begin() as connection:
            source_file_id = (
                await connection.execute(
                    text(
                        """
                        INSERT INTO source_files (
                            retailer_id, portal_id, remote_id, download_url,
                            original_filename, document_type, compression, protocol,
                            status, discovered_at, source_timestamp
                        )
                        SELECT retailer.id, portal.id, :remote_id, :download_url,
                               :filename, :document_type, :compression, :protocol,
                               'completed', :discovered_at, :source_timestamp
                          FROM retailers retailer
                          JOIN portals portal ON portal.retailer_id = retailer.id
                         WHERE retailer.source_key = :source_id
                           AND portal.source_key = :portal_id
                        ON CONFLICT (portal_id, remote_id) DO UPDATE
                              SET status = 'completed', updated_at = clock_timestamp()
                        RETURNING id
                        """
                    ),
                    {
                        "remote_id": remote_file.remote_id,
                        "download_url": remote_file.download_url,
                        "filename": remote_file.original_filename,
                        "document_type": remote_file.document_type.value,
                        "compression": remote_file.compression.value,
                        "protocol": remote_file.protocol.value,
                        "discovered_at": remote_file.discovered_at,
                        "source_timestamp": remote_file.source_timestamp,
                        "source_id": remote_file.retailer_id,
                        "portal_id": remote_file.portal_id,
                    },
                )
            ).scalar_one()
            return UUID(str(source_file_id))


class BlockingIngestion(PersistentIngestion):
    def __init__(self, engine: AsyncEngine) -> None:
        super().__init__(engine)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        del maximum_charged_bytes
        self.entered.set()
        await self.release.wait()
        return await super().ingest(remote_file)


class FailOnceCatalogBootstrap:
    def __init__(self) -> None:
        self.calls: list[UUID] = []
        self._failed = False

    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]:
        self.calls.append(source_file_id)
        if not self._failed:
            self._failed = True
            raise SourceAccessError("simulated retryable catalog bootstrap failure")
        return {"source_file_id": source_file_id}


async def _register_source(database: Database) -> None:
    await PostgresRegistryRepository(database.engine).synchronize(
        (
            RetailerRegistration(
                source_key=SOURCE_ID,
                legal_name="Clean-room discovery retailer",
                display_name="Discovery Retailer",
                edi="7290000000999",
                is_active=True,
            ),
        ),
        (
            PortalRegistration(
                retailer_source_key=SOURCE_ID,
                source_key=PORTAL_ID,
                family="test",
                protocol=SourceProtocol.FIXTURE,
                base_url="https://fixtures.invalid/",
                is_active=True,
            ),
        ),
    )


def _remote(index: int, *, unknown: bool = False) -> RemoteFile:
    filename = f"PriceFull7290000000999-001-20260812{index:04d}.xml"
    return RemoteFile(
        retailer_id=SOURCE_ID,
        portal_id=PORTAL_ID,
        protocol=SourceProtocol.FIXTURE,
        remote_id=f"catalog:{index}",
        download_url=f"https://fixtures.invalid/{filename}",
        original_filename=filename,
        document_type=DocumentType.UNKNOWN if unknown else DocumentType.PRICE_FULL,
        compression=CompressionFormat.NONE,
        discovered_at=NOW,
        source_timestamp=NOW + timedelta(seconds=index),
    )


def _subject(
    database: Database,
    adapter: PagedCatalogAdapter,
    ingestion: PersistentIngestion,
    *,
    maximum_files: int,
    maximum_discovery_records: int = 100_000,
    maximum_source_identities_per_day: int = 2_000,
    maximum_transfer_attempts_per_day: int = 4_000,
    maximum_successes_per_day: int = 2_000,
    worker_id: str = "collection-integration",
    catalog_bootstrap: FailOnceCatalogBootstrap | None = None,
) -> CollectionOperations:
    return CollectionOperations(
        lambda _source_id: adapter,
        (SOURCE_ID,),
        ingestion,
        policy=CollectionPolicy(
            discovery_page_size=100,
            maximum_files_per_source_run=maximum_files,
            maximum_discovery_records_per_source_run=maximum_discovery_records,
            maximum_reported_files=maximum_files,
            maximum_source_identities_per_source_day=maximum_source_identities_per_day,
            maximum_transfer_attempts_per_source_day=maximum_transfer_attempts_per_day,
            maximum_successes_per_source_day=maximum_successes_per_day,
        ),
        repository=PostgresCollectionRepository(database.engine),
        leases=PostgresCollectionLeaseManager(database),
        worker_id=worker_id,
        source_portal_ids={SOURCE_ID: (PORTAL_ID,)},
        catalog_bootstrap=catalog_bootstrap,
    )


async def test_collection_cap_resumes_100_100_50_and_rolls_without_repeats(
    database: Database,
) -> None:
    await _register_source(database)
    adapter = PagedCatalogAdapter(tuple(_remote(index) for index in range(250)))
    ingestion = PersistentIngestion(database.engine)
    subject = _subject(database, adapter, ingestion, maximum_files=100)

    results = [await subject.ingest_source(SOURCE_ID) for _ in range(4)]
    adapter.files = (*adapter.files, _remote(250))
    results.append(await subject.ingest_source(SOURCE_ID))

    assert [result["file_count"] for result in results] == [100, 100, 50, 0, 1]
    assert [result["status"] for result in results] == [
        "bounded",
        "bounded",
        "completed",
        "completed",
        "completed",
    ]
    assert ingestion.calls == [f"catalog:{index}" for index in range(251)]
    async with database.engine.connect() as connection:
        attempts = (
            await connection.execute(
                select(
                    collection_attempts.c.generation,
                    collection_attempts.c.status,
                    collection_attempts.c.discovered_count,
                    collection_attempts.c.processed_count,
                    collection_attempts.c.truncated,
                ).order_by(collection_attempts.c.started_at, collection_attempts.c.id)
            )
        ).all()
    assert [tuple(row) for row in attempts] == [
        (1, "bounded", 100, 100, True),
        (1, "bounded", 100, 100, True),
        (1, "completed", 50, 50, False),
        (2, "completed", 250, 0, False),
        (3, "completed", 251, 1, False),
    ]
    source_status = await PostgresQueryRepository(database.engine).source_status(limit=10)
    status = next(item for item in source_status.items if item["portal_key"] == PORTAL_ID)
    assert status["last_good_source_file_id"] is not None
    assert status["collection_attempt_status"] == "completed"
    assert status["collection_generation"] == 3
    assert status["collection_processed_count"] == 1
    assert status["collection_truncated"] is False


async def test_tiny_identity_flood_stops_at_independent_rolling_cardinality_limit(
    database: Database,
) -> None:
    await _register_source(database)
    adapter = PagedCatalogAdapter(tuple(_remote(index) for index in range(3)))
    subject = _subject(
        database,
        adapter,
        PersistentIngestion(database.engine),
        maximum_files=3,
        maximum_source_identities_per_day=2,
        maximum_transfer_attempts_per_day=3,
        maximum_successes_per_day=3,
    )

    result = await subject.ingest_source(SOURCE_ID)

    assert result["status"] == "bounded"
    assert result["file_count"] == 2
    assert result["truncation_reason"] == "identity_day_limit"
    async with database.engine.connect() as connection:
        counts = (
            await connection.execute(
                text(
                    """
                    SELECT budget.identity_count, budget.attempt_count,
                           budget.success_count,
                           (SELECT count(*) FROM collection_identity_observations),
                           (SELECT count(*) FROM collection_transfer_charges),
                           (SELECT count(*) FROM collection_budget_buckets)
                      FROM collection_charge_budgets budget
                      JOIN retailers retailer ON retailer.id = budget.retailer_id
                     WHERE retailer.source_key = :source_id
                    """
                ),
                {"source_id": SOURCE_ID},
            )
        ).one()
        plan = (
            await connection.execute(
                text(
                    """
                    EXPLAIN (ANALYZE, FORMAT JSON)
                    SELECT sum(bucket.charged_bytes), sum(bucket.identity_count),
                           sum(bucket.attempt_count), sum(bucket.success_count)
                      FROM collection_budget_buckets bucket
                      JOIN retailers retailer ON retailer.id = bucket.retailer_id
                     WHERE retailer.source_key = :source_id
                       AND bucket.bucket_started_at >=
                           date_bin(
                               INTERVAL '5 minutes', clock_timestamp(),
                               TIMESTAMPTZ '1970-01-01 00:00:00+00'
                           ) - INTERVAL '24 hours'
                    """
                ),
                {"source_id": SOURCE_ID},
            )
        ).scalar_one()[0]["Plan"]
    assert tuple(counts) == (2, 2, 0, 2, 2, 1)
    plan_text = json.dumps(plan, sort_keys=True)
    assert "pk_collection_budget_buckets" in plan_text
    assert "collection_transfer_charges" not in plan_text
    assert "collection_archive_charges" not in plan_text


async def test_transfer_settlement_above_reservation_rolls_back_and_retains_charge(
    database: Database,
) -> None:
    await _register_source(database)
    remote_file = _remote(900)
    registered = await PostgresIngestionRepository(database.engine).register_discovery(remote_file)
    repository = PostgresCollectionRepository(database.engine)
    attempt = await repository.begin_attempt(
        CollectionScope(
            source_id=SOURCE_ID,
            portal_ids=(PORTAL_ID,),
            operation="ordinary",
        )
    )
    await repository.reserve_transfer(
        attempt.attempt_id,
        registered.source_file_id,
        remote_file,
        10,
    )

    with pytest.raises(PersistenceConflictError, match="exceeds its reservation"):
        await repository.settle_transfer(
            attempt.attempt_id,
            registered.source_file_id,
            remote_file,
            11,
        )

    retained = await repository.charge_budget(attempt.attempt_id)
    async with database.engine.connect() as connection:
        reservation = (
            await connection.execute(
                select(
                    collection_transfer_charges.c.content_length,
                    collection_transfer_charges.c.settled,
                ).where(
                    collection_transfer_charges.c.attempt_id == attempt.attempt_id,
                    collection_transfer_charges.c.source_file_id == registered.source_file_id,
                )
            )
        ).one()
    assert retained.run_charged_bytes == 10
    assert retained.day_charged_bytes == 10
    assert tuple(reservation) == (10, False)

    settled = await repository.settle_transfer(
        attempt.attempt_id,
        registered.source_file_id,
        remote_file,
        10,
    )

    assert settled.run_charged_bytes == 10
    assert settled.day_charged_bytes == 10


async def test_retryable_interruption_retries_boundary_without_skipping(
    database: Database,
) -> None:
    await _register_source(database)
    adapter = PagedCatalogAdapter(tuple(_remote(index) for index in range(3)))
    ingestion = PersistentIngestion(database.engine)
    ingestion.fail_once.add("catalog:1")
    subject = _subject(database, adapter, ingestion, maximum_files=3)

    with pytest.raises(SourceAccessError):
        await subject.ingest_source(SOURCE_ID)
    result = await subject.ingest_source(SOURCE_ID)

    assert result["file_count"] == 2
    assert ingestion.calls == ["catalog:0", "catalog:1", "catalog:1", "catalog:2"]
    async with database.engine.connect() as connection:
        attempts = (
            await connection.execute(
                select(
                    collection_attempts.c.status,
                    collection_attempts.c.discovered_count,
                    collection_attempts.c.processed_count,
                    collection_attempts.c.error_message,
                ).order_by(collection_attempts.c.started_at, collection_attempts.c.id)
            )
        ).all()
    assert [attempt.status for attempt in attempts] == ["failed", "completed"]
    assert [attempt.discovered_count for attempt in attempts] == [2, 2]
    assert [attempt.processed_count for attempt in attempts] == [1, 2]
    assert "simulated" not in str(attempts[0].error_message)


async def test_cancellation_resumes_before_the_uncommitted_file(
    database: Database,
) -> None:
    await _register_source(database)
    adapter = PagedCatalogAdapter((_remote(0),))
    interrupted_ingestion = BlockingIngestion(database.engine)
    interrupted = _subject(
        database,
        adapter,
        interrupted_ingestion,
        maximum_files=1,
        worker_id="collector-interrupted",
    )

    task = asyncio.create_task(interrupted.ingest_source(SOURCE_ID))
    await interrupted_ingestion.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    resumed_ingestion = PersistentIngestion(database.engine)
    resumed = _subject(
        database,
        adapter,
        resumed_ingestion,
        maximum_files=1,
        worker_id="collector-resumed",
    )
    result = await resumed.ingest_source(SOURCE_ID)

    assert result["file_count"] == 1
    assert resumed_ingestion.calls == ["catalog:0"]
    async with database.engine.connect() as connection:
        attempts = (
            await connection.execute(
                select(
                    collection_attempts.c.status,
                    collection_attempts.c.discovered_count,
                    collection_attempts.c.processed_count,
                    collection_attempts.c.error_code,
                    collection_attempts.c.error_message,
                ).order_by(collection_attempts.c.started_at, collection_attempts.c.id)
            )
        ).all()
    assert [attempt.status for attempt in attempts] == ["failed", "completed"]
    assert [attempt.discovered_count for attempt in attempts] == [1, 1]
    assert [attempt.processed_count for attempt in attempts] == [0, 1]
    assert attempts[0].error_code == "operation_cancelled"
    assert attempts[0].error_message == (
        "Collection was interrupted before its next retry-safe boundary"
    )


async def test_retryable_postprocess_failure_replays_only_its_boundary(
    database: Database,
) -> None:
    await _register_source(database)
    adapter = PagedCatalogAdapter((_remote(0), _remote(1)))
    ingestion = PersistentIngestion(database.engine)
    bootstrap = FailOnceCatalogBootstrap()
    subject = _subject(
        database,
        adapter,
        ingestion,
        maximum_files=2,
        catalog_bootstrap=bootstrap,
    )

    with pytest.raises(SourceAccessError):
        await subject.ingest_source(SOURCE_ID)
    result = await subject.ingest_source(SOURCE_ID)

    assert result["file_count"] == 2
    assert ingestion.calls == ["catalog:0", "catalog:0", "catalog:1"]
    assert bootstrap.calls[0] == bootstrap.calls[1]
    async with database.engine.connect() as connection:
        attempts = (
            await connection.execute(
                select(
                    collection_attempts.c.status,
                    collection_attempts.c.discovered_count,
                    collection_attempts.c.processed_count,
                    collection_attempts.c.checkpoint_page_offset,
                ).order_by(collection_attempts.c.started_at, collection_attempts.c.id)
            )
        ).all()
    assert [tuple(attempt) for attempt in attempts] == [
        ("failed", 1, 0, 0),
        ("completed", 2, 2, 0),
    ]


async def test_backfill_range_has_independent_checkpoint_and_in_range_cap(
    database: Database,
) -> None:
    await _register_source(database)
    adapter = PagedCatalogAdapter(tuple(_remote(index) for index in range(250)))
    ingestion = PersistentIngestion(database.engine)
    subject = _subject(
        database,
        adapter,
        ingestion,
        maximum_files=3,
        maximum_discovery_records=100,
    )
    backfill_results = [
        await subject.backfill(
            SOURCE_ID,
            since=NOW + timedelta(seconds=245),
            until=NOW + timedelta(seconds=249),
        )
        for _ in range(4)
    ]
    ordinary = await subject.ingest_source(SOURCE_ID)

    assert [result["file_count"] for result in backfill_results] == [0, 0, 3, 2]
    assert ordinary["file_count"] == 3
    assert ingestion.calls == [
        *(f"catalog:{index}" for index in range(245, 250)),
        "catalog:0",
        "catalog:1",
        "catalog:2",
    ]
    async with database.engine.connect() as connection:
        checkpoints = (
            (
                await connection.execute(
                    select(collection_checkpoints).order_by(collection_checkpoints.c.operation)
                )
            )
            .mappings()
            .all()
        )
    assert [checkpoint.operation for checkpoint in checkpoints] == ["backfill", "ordinary"]
    assert checkpoints[0].range_since == NOW + timedelta(seconds=245)
    assert checkpoints[0].range_until == NOW + timedelta(seconds=249)
    assert checkpoints[0].traversal_complete is True
    assert checkpoints[1].range_since is None
    assert checkpoints[1].range_until is None


async def test_unknown_files_are_durable_warnings_and_all_unknown_fails_closed(
    database: Database,
) -> None:
    await _register_source(database)
    mixed = PagedCatalogAdapter((_remote(0), _remote(1, unknown=True)))
    ingestion = PersistentIngestion(database.engine)
    subject = _subject(database, mixed, ingestion, maximum_files=2)

    result = await subject.ingest_source(SOURCE_ID)

    assert result["file_count"] == 1
    assert result["skipped_unknown_count"] == 1
    all_unknown = PagedCatalogAdapter((_remote(2, unknown=True), _remote(3, unknown=True)))
    failed_subject = _subject(database, all_unknown, ingestion, maximum_files=2)
    with pytest.raises(SourceResponseError, match="only unknown"):
        await failed_subject.ingest_source(SOURCE_ID)
    empty_result = await _subject(
        database,
        PagedCatalogAdapter(()),
        ingestion,
        maximum_files=2,
    ).ingest_source(SOURCE_ID)
    assert empty_result["status"] == "completed"
    assert empty_result["file_count"] == 0
    async with database.engine.connect() as connection:
        attempts = (
            await connection.execute(
                select(
                    collection_attempts.c.status,
                    collection_attempts.c.warning_count,
                    collection_attempts.c.skipped_unknown_count,
                ).order_by(collection_attempts.c.started_at, collection_attempts.c.id)
            )
        ).all()
    assert [tuple(row) for row in attempts] == [
        ("completed", 1, 1),
        ("failed", 2, 2),
        ("completed", 0, 0),
    ]


async def test_source_lease_prevents_concurrent_traversal_leapfrog(
    database: Database,
) -> None:
    await _register_source(database)
    adapter = PagedCatalogAdapter((_remote(0),))
    ingestion = BlockingIngestion(database.engine)
    first_subject = _subject(
        database,
        adapter,
        ingestion,
        maximum_files=1,
        worker_id="collector-one",
    )
    second_subject = _subject(
        database,
        adapter,
        ingestion,
        maximum_files=1,
        worker_id="collector-two",
    )

    first = asyncio.create_task(first_subject.ingest_source(SOURCE_ID))
    await ingestion.entered.wait()
    with pytest.raises(LeaseUnavailableError):
        await second_subject.ingest_source(SOURCE_ID)
    ingestion.release.set()
    await first

    assert ingestion.calls == ["catalog:0"]


async def test_stale_publisher_cursor_fails_closed_without_moving_checkpoint(
    database: Database,
) -> None:
    await _register_source(database)
    adapter = PagedCatalogAdapter(tuple(_remote(index) for index in range(150)))
    ingestion = PersistentIngestion(database.engine)
    subject = _subject(database, adapter, ingestion, maximum_files=100)

    first = await subject.ingest_source(SOURCE_ID)
    adapter.files = adapter.files[:50]
    with pytest.raises(SourceResponseError, match="cursor is stale"):
        await subject.ingest_source(SOURCE_ID)

    assert first["status"] == "bounded"
    async with database.engine.connect() as connection:
        checkpoint = (await connection.execute(select(collection_checkpoints))).mappings().one()
        attempts = (
            (
                await connection.execute(
                    select(collection_attempts.c.status).order_by(
                        collection_attempts.c.started_at, collection_attempts.c.id
                    )
                )
            )
            .scalars()
            .all()
        )
    assert checkpoint.publisher_cursor == "100"
    assert checkpoint.page_offset == 0
    assert checkpoint.generation == 1
    assert attempts == ["bounded", "failed"]


async def test_completed_rediscovery_preserves_download_provenance_and_redacts_urls(
    database: Database,
) -> None:
    await _register_source(database)
    original = RemoteFile(
        retailer_id=SOURCE_ID,
        portal_id=PORTAL_ID,
        protocol=SourceProtocol.FIXTURE,
        remote_id="stable-signed-url",
        download_url="https://fixtures.invalid/price.xml?signature=original-secret",
        original_filename="PriceFull-original.xml",
        document_type=DocumentType.PRICE_FULL,
        compression=CompressionFormat.NONE,
        discovered_at=NOW,
        source_timestamp=NOW - timedelta(minutes=2),
        content_length=17,
        media_type="application/xml",
        etag="original-etag",
        last_modified=NOW - timedelta(minutes=3),
        response_metadata=(("listing-revision", "original"),),
    )
    repository = PostgresIngestionRepository(database.engine)
    registered = await repository.register_discovery(original)
    async with database.engine.begin() as connection:
        await connection.execute(
            update(source_files)
            .where(source_files.c.id == registered.source_file_id)
            .values(status=IngestionStatus.COMPLETED.value)
        )
    rediscovery = RemoteFile(
        retailer_id=SOURCE_ID,
        portal_id=PORTAL_ID,
        protocol=SourceProtocol.FIXTURE,
        remote_id=original.remote_id,
        download_url="https://fixtures.invalid/price.xml?signature=rotated-secret",
        original_filename=original.original_filename,
        document_type=original.document_type,
        compression=original.compression,
        discovered_at=NOW + timedelta(hours=1),
        source_timestamp=NOW + timedelta(minutes=2),
        content_length=99,
        media_type="application/octet-stream",
        etag="rotated-etag",
        last_modified=NOW + timedelta(minutes=3),
        response_metadata=(("listing-revision", "rotated"),),
    )

    repeated = await repository.register_discovery(rediscovery)

    assert repeated.source_file_id == registered.source_file_id
    assert repeated.remote_file == original
    ingestion = PersistentIngestion(database.engine)
    collection = _subject(
        database,
        PagedCatalogAdapter((rediscovery,)),
        ingestion,
        maximum_files=1,
    )
    result = await collection.ingest_source(SOURCE_ID)
    status_page = await PostgresQueryRepository(database.engine).source_status(limit=10)
    status = next(item for item in status_page.items if item["portal_key"] == PORTAL_ID)

    assert result["discovered_count"] == 1
    assert result["file_count"] == 0
    assert ingestion.calls == []
    assert status["source_file_id"] == registered.source_file_id
    assert status["last_good_source_file_id"] == registered.source_file_id
    assert status["collection_attempt_id"] == result["collection_attempt_id"]
    serialized_status = json.dumps(status, default=str)
    assert "original-secret" not in serialized_status
    assert "rotated-secret" not in serialized_status
