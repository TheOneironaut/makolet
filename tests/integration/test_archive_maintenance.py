"""Real-PostgreSQL archive replay and normalized rebuild behavior."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
from sqlalchemy import func, select, text

from makolet.adapters.archive.local import LocalContentAddressedArchive
from makolet.adapters.export.postgres import PostgresParquetExportOperations
from makolet.adapters.parsers import RetailXmlParser, XmlParserLimits
from makolet.adapters.persistence.catalog_matching import PostgresCatalogMatchingRepository
from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.ingestion import PostgresIngestionRepository
from makolet.adapters.persistence.leases import PostgresLeaseManager
from makolet.adapters.persistence.maintenance import PostgresArchiveMaintenanceRepository
from makolet.adapters.persistence.queries import PostgresQueryRepository
from makolet.adapters.persistence.schema import normalized_rebuild_snapshots, replay_runs, retailers
from makolet.application.catalog_matching import CatalogMatchingService
from makolet.application.ingestion import IngestionPolicy, IngestionService
from makolet.application.maintenance import ArchiveMaintenanceService
from makolet.application.models import IngestionResult
from makolet.application.queries import QueryService
from makolet.domain.enums import CompressionFormat, DocumentType, SourceProtocol
from makolet.domain.errors import (
    DomainValidationError,
    MaintenanceModeError,
    NormalizedRebuildInterruptedError,
)
from makolet.domain.models import ParsedEvent, PriceRecord, RemoteFile
from tests.fakes.ingestion import FakeDownloader, FakeMetrics, FixedClock

pytestmark = pytest.mark.integration
_FIXTURE = Path(__file__).parents[1] / "e2e" / "fixtures" / "price-full.xml"
_DELTA_FIXTURE = Path(__file__).parents[1] / "e2e" / "fixtures" / "price-delta.xml"
_PROMOTION_FIXTURE = Path(__file__).parents[1] / "fixtures" / "standard" / "promo-full.xml"
_URL_A = "https://fixtures.invalid/rebuild/retailer-a.xml"
_URL_B = "https://fixtures.invalid/rebuild/retailer-b.xml"
_URL_DELTA = "https://fixtures.invalid/rebuild/retailer-a-delta.xml"
_URL_PROMOTION = "https://fixtures.invalid/rebuild/retailer-a-promotion.xml"
_URL_BLOCKED = "https://fixtures.invalid/rebuild/blocked.xml"


class _FailAfterFirstReplay:
    """Simulate process failure after replay commit but before rebuild checkpoint."""

    def __init__(self, ingestion: IngestionService) -> None:
        self._ingestion = ingestion
        self._failed = False

    @property
    def parser_version(self) -> str:
        return self._ingestion.parser_version

    async def replay(
        self,
        source_file_id: UUID,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> IngestionResult:
        result = await self._ingestion.replay(
            source_file_id,
            rebuild_run_id=rebuild_run_id,
        )
        if not self._failed:
            self._failed = True
            raise DomainValidationError("simulated post-replay interruption")
        return result


class _FailBeforeFirstReplay:
    """Simulate process failure before archived bytes are replayed."""

    def __init__(self, ingestion: IngestionService) -> None:
        self._ingestion = ingestion
        self._failed = False

    @property
    def parser_version(self) -> str:
        return self._ingestion.parser_version

    async def replay(
        self,
        source_file_id: UUID,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> IngestionResult:
        if not self._failed:
            self._failed = True
            raise DomainValidationError("simulated pre-replay interruption")
        return await self._ingestion.replay(
            source_file_id,
            rebuild_run_id=rebuild_run_id,
        )


class _CorrectingParser:
    parser_version = "retail-xml/correction-test"

    def __init__(self, delegate: RetailXmlParser) -> None:
        self._delegate = delegate

    def parse(
        self,
        chunks: AsyncIterator[bytes],
        *,
        source_file_id: UUID,
        document_type: DocumentType,
        compression: CompressionFormat,
        filename: str,
    ) -> AsyncIterator[ParsedEvent]:
        return self._parse(
            chunks,
            source_file_id=source_file_id,
            document_type=document_type,
            compression=compression,
            filename=filename,
        )

    async def _parse(
        self,
        chunks: AsyncIterator[bytes],
        *,
        source_file_id: UUID,
        document_type: DocumentType,
        compression: CompressionFormat,
        filename: str,
    ) -> AsyncIterator[ParsedEvent]:
        events = self._delegate.parse(
            chunks,
            source_file_id=source_file_id,
            document_type=document_type,
            compression=compression,
            filename=filename,
        )
        async for event in events:
            if isinstance(event, PriceRecord) and event.item_price == Decimal("19.90"):
                yield replace(event, item_price=Decimal("20.00"))
            else:
                yield event


class _PendingCheckpointCatalog:
    """Record that catalog bootstrap runs before the rebuild file checkpoint."""

    def __init__(self, database: Database, delegate: CatalogMatchingService) -> None:
        self._database = database
        self._delegate = delegate
        self.observed_statuses: list[str] = []

    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]:
        async with self._database.engine.connect() as connection:
            status = await connection.scalar(
                text(
                    """
                    SELECT status
                      FROM normalized_rebuild_files
                     WHERE source_file_id = :source_file_id
                    """
                ),
                {"source_file_id": source_file_id},
            )
        self.observed_statuses.append(str(status))
        return await self._delegate.bootstrap_source_file(source_file_id)


def _remote(retailer: str, url: str, clock: FixedClock) -> RemoteFile:
    return RemoteFile(
        retailer_id=retailer,
        portal_id=f"{retailer}-portal",
        protocol=SourceProtocol.FIXTURE,
        remote_id=f"{retailer}-price-full",
        download_url=url,
        original_filename=f"PriceFull-{retailer}.xml",
        document_type=DocumentType.PRICE_FULL,
        compression=CompressionFormat.NONE,
        discovered_at=clock.now(),
        source_timestamp=datetime(2026, 8, 10, 12, tzinfo=UTC),
        media_type="application/xml",
    )


def _typed_remote(
    *,
    retailer: str,
    url: str,
    remote_id: str,
    document_type: DocumentType,
    source_timestamp: datetime,
    clock: FixedClock,
) -> RemoteFile:
    return RemoteFile(
        retailer_id=retailer,
        portal_id=f"{retailer}-portal",
        protocol=SourceProtocol.FIXTURE,
        remote_id=remote_id,
        download_url=url,
        original_filename=f"{remote_id}.xml",
        document_type=document_type,
        compression=CompressionFormat.NONE,
        discovered_at=clock.now(),
        source_timestamp=source_timestamp,
        media_type="application/xml",
    )


async def _snapshot_rows(database: Database) -> dict[str, list[dict[str, object]]]:
    entities = (
        "stores",
        "store_aliases",
        "retailer_items",
        "canonical_products",
        "product_identifiers",
        "identifier_match_groups",
        "retailer_identifier_assertions",
        "product_match_candidates",
        "confirmed_product_matches",
        "current_prices",
        "price_history",
        "current_availability",
        "availability_history",
        "promotions",
        "promotion_items",
        "promotion_stores",
        "promotion_clubs",
        "applied_source_contents",
        "source_scope_watermarks",
    )
    async with database.engine.connect() as connection:
        return {
            entity: [
                dict(row)
                for row in (
                    await connection.execute(
                        text(f"SELECT * FROM {entity} ORDER BY 1")  # noqa: S608
                    )
                )
                .mappings()
                .all()
            ]
            for entity in entities
        }


def _export_artifacts(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _export_summary(result: dict[str, object]) -> tuple[object, ...]:
    manifests = cast(tuple[dict[str, object], ...], result["manifests"])
    return (
        result["partition_count"],
        result["row_count"],
        tuple(
            (
                manifest["entity"],
                manifest["retailer_id"],
                manifest["partition_date"],
                manifest["dataset_id"],
                manifest["row_count"],
                manifest["file_count"],
            )
            for manifest in manifests
        ),
    )


async def _retailer_id(database: Database, source_key: str) -> UUID:
    async with database.engine.connect() as connection:
        return cast(
            UUID,
            (
                await connection.execute(
                    select(retailers.c.id).where(retailers.c.source_key == source_key)
                )
            ).scalar_one(),
        )


async def test_range_replay_and_full_rebuild_use_only_archived_bytes(
    database: Database,
    tmp_path: Path,
) -> None:
    payload = _FIXTURE.read_bytes()
    clock = FixedClock()
    downloader = FakeDownloader({_URL_A: payload, _URL_B: payload}, clock)
    archive = LocalContentAddressedArchive(tmp_path / "archive")
    await archive.initialize()
    ingestion_repository = PostgresIngestionRepository(database.engine)
    maintenance_repository = PostgresArchiveMaintenanceRepository(database.engine)
    ingestion = IngestionService(
        ingestion_repository,
        downloader,
        archive,
        RetailXmlParser(XmlParserLimits(temporary_directory=tmp_path / "parser-spool")),
        PostgresLeaseManager(database),
        clock,
        FakeMetrics(),
        worker_id="maintenance-integration",
        policy=IngestionPolicy(minimum_full_records=1),
    )
    maintenance = ArchiveMaintenanceService(
        maintenance_repository,
        maintenance_repository,
        ingestion,
    )
    queries = PostgresQueryRepository(database.engine)

    ingested_a = await ingestion.ingest(_remote("retailer-a", _URL_A, clock))
    ingested_b = await ingestion.ingest(_remote("retailer-b", _URL_B, clock))
    retailer_a = await _retailer_id(database, "retailer-a")
    retailer_b = await _retailer_id(database, "retailer-b")

    product_a = await queries.find_product_by_retailer_item_code(
        retailer_a,
        "4006381333931",
    )
    product_b = await queries.find_product_by_retailer_item_code(
        retailer_b,
        "4006381333931",
    )
    assert product_a is not None
    assert product_b is not None
    assert product_a["retailer_id"] == retailer_a
    assert product_b["retailer_id"] == retailer_b
    assert product_a["retailer_item_id"] != product_b["retailer_item_id"]
    assert product_a["id"] == product_b["id"]

    first_page = await maintenance.replay_range(
        since=datetime(2026, 8, 11, 11, tzinfo=UTC),
        until=datetime(2026, 8, 11, 13, tzinfo=UTC),
        limit=1,
    )
    assert first_page.next_cursor is not None
    second_page = await maintenance.replay_range(
        since=datetime(2026, 8, 11, 11, tzinfo=UTC),
        until=datetime(2026, 8, 11, 13, tzinfo=UTC),
        limit=1,
        cursor=first_page.next_cursor,
    )
    assert second_page.next_cursor is None
    assert {result.source_file_id for result in (*first_page.files, *second_page.files)} == {
        ingested_a.source_file_id,
        ingested_b.source_file_id,
    }
    assert downloader.open_count == 2

    rebuild = await maintenance_repository.begin_rebuild(
        requested_by="integration-test",
        parser_version=ingestion.parser_version,
    )
    assert rebuild.source_files_total == 2
    assert rebuild.source_files_completed == 0
    active_status = await queries.maintenance_status()
    assert active_status["active"] is True
    assert active_status["rebuild_run_id"] == rebuild.rebuild_run_id
    assert "partial" in str(active_status["warning"])
    assert (
        await queries.find_product_by_retailer_item_code(
            retailer_a,
            "4006381333931",
        )
        is None
    )

    blocked_remote = _remote("retailer-c", _URL_BLOCKED, clock)
    with pytest.raises(MaintenanceModeError, match="ordinary ingestion"):
        await ingestion.ingest(blocked_remote)
    with pytest.raises(MaintenanceModeError, match="ordinary ingestion"):
        await ingestion.replay(ingested_a.source_file_id)
    assert downloader.open_count == 2

    pre_replay_maintenance = ArchiveMaintenanceService(
        maintenance_repository,
        maintenance_repository,
        _FailBeforeFirstReplay(ingestion),
    )
    with pytest.raises(NormalizedRebuildInterruptedError, match=str(rebuild.rebuild_run_id)):
        await pre_replay_maintenance.resume_rebuild(rebuild.rebuild_run_id)
    failed_before_replay = await maintenance_repository.get_rebuild(rebuild.rebuild_run_id)
    assert failed_before_replay.status == "failed"
    assert failed_before_replay.source_files_completed == 0
    assert (await queries.maintenance_status())["active"] is True

    interrupted_maintenance = ArchiveMaintenanceService(
        maintenance_repository,
        maintenance_repository,
        _FailAfterFirstReplay(ingestion),
    )
    with pytest.raises(NormalizedRebuildInterruptedError, match=str(rebuild.rebuild_run_id)):
        await interrupted_maintenance.resume_rebuild(rebuild.rebuild_run_id)
    failed = await maintenance_repository.get_rebuild(rebuild.rebuild_run_id)
    assert failed.status == "failed"
    assert failed.source_files_completed == 0
    assert (await queries.maintenance_status())["active"] is True

    completed = await maintenance.resume_rebuild(rebuild.rebuild_run_id)
    assert completed.status == "completed"
    assert completed.source_files_total == completed.source_files_completed == 2
    assert (await queries.maintenance_status()) == {"active": False, "mode": "normal"}
    assert downloader.open_count == 2

    rebuilt_a = await queries.find_product_by_retailer_item_code(
        retailer_a,
        "4006381333931",
    )
    rebuilt_b = await queries.find_product_by_retailer_item_code(
        retailer_b,
        "4006381333931",
    )
    assert rebuilt_a is not None
    assert rebuilt_b is not None
    assert rebuilt_a["id"] == rebuilt_b["id"]

    async with database.engine.connect() as connection:
        rebuilt_replays = int(
            (
                await connection.execute(
                    select(func.count())
                    .select_from(replay_runs)
                    .where(replay_runs.c.rebuild_run_id == rebuild.rebuild_run_id)
                )
            ).scalar_one()
        )
        counts = (
            (
                await connection.execute(
                    text(
                        """
                    SELECT
                        (SELECT count(*) FROM source_files) AS source_files,
                        (SELECT count(*) FROM raw_archive_objects) AS raw_objects,
                        (SELECT count(*) FROM applied_source_contents) AS applied_contents,
                        (SELECT count(*) FROM retailer_items) AS retailer_items,
                        (SELECT count(*) FROM current_prices) AS current_prices
                    """
                    )
                )
            )
            .mappings()
            .one()
        )
    assert rebuilt_replays == 3
    assert dict(counts) == {
        "source_files": 2,
        "raw_objects": 1,
        "applied_contents": 2,
        "retailer_items": 2,
        "current_prices": 2,
    }


async def test_parser_unchanged_rebuild_preserves_exact_ids_history_and_review_overlay(
    database: Database,
    tmp_path: Path,
) -> None:
    clock = FixedClock()
    payloads = {
        _URL_A: _FIXTURE.read_bytes(),
        _URL_DELTA: _DELTA_FIXTURE.read_bytes(),
        _URL_PROMOTION: _PROMOTION_FIXTURE.read_bytes(),
    }
    downloader = FakeDownloader(payloads, clock)
    archive = LocalContentAddressedArchive(tmp_path / "stable-archive")
    await archive.initialize()
    ingestion_repository = PostgresIngestionRepository(database.engine)
    maintenance_repository = PostgresArchiveMaintenanceRepository(database.engine)
    ingestion = IngestionService(
        ingestion_repository,
        downloader,
        archive,
        RetailXmlParser(XmlParserLimits(temporary_directory=tmp_path / "stable-spool")),
        PostgresLeaseManager(database),
        clock,
        FakeMetrics(),
        worker_id="stable-rebuild-integration",
        policy=IngestionPolicy(minimum_full_records=1),
    )
    maintenance = ArchiveMaintenanceService(
        maintenance_repository,
        maintenance_repository,
        ingestion,
    )

    initial = await ingestion.ingest(
        _typed_remote(
            retailer="retailer-a",
            url=_URL_A,
            remote_id="price-full",
            document_type=DocumentType.PRICE_FULL,
            source_timestamp=datetime(2026, 8, 10, 12, tzinfo=UTC),
            clock=clock,
        )
    )
    await ingestion.ingest(
        _typed_remote(
            retailer="retailer-a",
            url=_URL_DELTA,
            remote_id="price-delta",
            document_type=DocumentType.PRICE_DELTA,
            source_timestamp=datetime(2026, 8, 11, 12, tzinfo=UTC),
            clock=clock,
        )
    )
    await ingestion.ingest(
        _typed_remote(
            retailer="retailer-a",
            url=_URL_PROMOTION,
            remote_id="promotion-full",
            document_type=DocumentType.PROMOTION_FULL,
            source_timestamp=datetime(2026, 8, 11, 13, tzinfo=UTC),
            clock=clock,
        )
    )
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                UPDATE canonical_products
                   SET name = 'Reviewed product name',
                       brand = 'Reviewed brand',
                       updated_at = '2026-08-11T14:00:00Z'
                """
            )
        )
        await connection.execute(
            text(
                """
                UPDATE confirmed_product_matches
                   SET method = 'manual_review',
                       evidence = '{"reason":"operator confirmation"}'::jsonb,
                       confirmed_at = '2026-08-11T14:00:00Z',
                       confirmed_by = 'reviewer@example.test'
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO product_match_candidates (
                    retailer_item_id, canonical_product_id, method, score,
                    status, evidence, reviewed_at, reviewed_by
                )
                SELECT confirmed.retailer_item_id, confirmed.canonical_product_id,
                       decision.method, decision.score, decision.status,
                       jsonb_build_object('reason', decision.status),
                       '2026-08-11T14:00:00Z', 'reviewer@example.test'
                  FROM confirmed_product_matches confirmed
                 CROSS JOIN (
                       VALUES
                           ('operator_accepted', 0.9::numeric, 'accepted'),
                           ('operator_rejected', 0.5::numeric, 'rejected'),
                           ('operator_superseded', 0.7::numeric, 'superseded')
                 ) AS decision(method, score, status)
                """
            )
        )

    queries = PostgresQueryRepository(database.engine)
    public_queries = QueryService(queries, clock)
    product = await public_queries.find_product_by_barcode("4006381333931")
    assert product is not None
    product_id = cast(UUID, product["id"])
    history_until = datetime.now(UTC) + timedelta(days=1)
    history_since = history_until - timedelta(days=366)
    history_before_first = await public_queries.price_history(
        product_id,
        store_id=None,
        since=history_since,
        until=history_until,
        limit=1,
        cursor=None,
    )
    assert history_before_first.next_cursor is not None
    history_before_second = await public_queries.price_history(
        product_id,
        store_id=None,
        since=history_since,
        until=history_until,
        limit=1,
        cursor=history_before_first.next_cursor,
    )
    export_operations = PostgresParquetExportOperations(
        database.engine,
        spool_directory=tmp_path / "stable-export-spool",
    )
    export_before_root = tmp_path / "export-before"
    export_before = await export_operations.export_parquet(
        export_before_root,
        since=history_since,
        until=history_until,
    )
    before = await _snapshot_rows(database)
    for populated_entity in (
        "product_identifiers",
        "retailer_identifier_assertions",
        "confirmed_product_matches",
        "current_prices",
        "price_history",
        "current_availability",
        "availability_history",
        "promotions",
        "promotion_items",
        "promotion_stores",
        "applied_source_contents",
        "source_scope_watermarks",
    ):
        assert before[populated_entity], populated_entity
    assert {row["status"] for row in before["product_match_candidates"]} == {
        "accepted",
        "rejected",
        "superseded",
    }
    run = await maintenance_repository.begin_rebuild(
        requested_by="stable-contract",
        parser_version=ingestion.parser_version,
    )
    async with database.engine.connect() as connection:
        snapshot_count = await connection.scalar(
            select(func.count())
            .select_from(normalized_rebuild_snapshots)
            .where(normalized_rebuild_snapshots.c.rebuild_run_id == run.rebuild_run_id)
        )
    assert int(snapshot_count or 0) > 0

    completed = await maintenance.resume_rebuild(run.rebuild_run_id)
    after = await _snapshot_rows(database)
    history_after_first = await public_queries.price_history(
        product_id,
        store_id=None,
        since=history_since,
        until=history_until,
        limit=1,
        cursor=None,
    )
    history_after_second = await public_queries.price_history(
        product_id,
        store_id=None,
        since=history_since,
        until=history_until,
        limit=1,
        cursor=history_before_first.next_cursor,
    )
    export_after_root = tmp_path / "export-after"
    export_after = await export_operations.export_parquet(
        export_after_root,
        since=history_since,
        until=history_until,
    )

    assert completed.status == "completed"
    assert after == before
    assert history_after_first == history_before_first
    assert history_after_second == history_before_second
    assert _export_summary(export_after) == _export_summary(export_before)
    assert _export_artifacts(export_after_root) == _export_artifacts(export_before_root)
    assert downloader.open_count == 3
    async with database.engine.connect() as connection:
        remaining_snapshots = int(
            (
                await connection.execute(
                    select(func.count())
                    .select_from(normalized_rebuild_snapshots)
                    .where(normalized_rebuild_snapshots.c.rebuild_run_id == run.rebuild_run_id)
                )
            ).scalar_one()
        )
        replay_count = int(
            (
                await connection.execute(
                    select(func.count())
                    .select_from(replay_runs)
                    .where(replay_runs.c.rebuild_run_id == run.rebuild_run_id)
                )
            ).scalar_one()
        )
    assert remaining_snapshots == 0
    assert replay_count == 3
    assert initial.status.value == "completed"


async def test_archive_only_file_bootstraps_isolated_catalog_before_checkpoint(
    database: Database,
    tmp_path: Path,
) -> None:
    payload = _FIXTURE.read_bytes().replace(b"4006381333931", b"CLEAN-ROOM-SKU")
    clock = FixedClock()
    downloader = FakeDownloader({_URL_A: payload}, clock)
    archive = LocalContentAddressedArchive(tmp_path / "archive-only")
    await archive.initialize()
    ingestion_repository = PostgresIngestionRepository(database.engine)
    maintenance_repository = PostgresArchiveMaintenanceRepository(database.engine)
    ingestion = IngestionService(
        ingestion_repository,
        downloader,
        archive,
        RetailXmlParser(XmlParserLimits(temporary_directory=tmp_path / "archive-only-spool")),
        PostgresLeaseManager(database),
        clock,
        FakeMetrics(),
        worker_id="archive-only-rebuild",
        policy=IngestionPolicy(minimum_full_records=1),
    )
    catalog = _PendingCheckpointCatalog(
        database,
        CatalogMatchingService(PostgresCatalogMatchingRepository(database.engine)),
    )
    maintenance = ArchiveMaintenanceService(
        maintenance_repository,
        maintenance_repository,
        ingestion,
        catalog_bootstrap=catalog,
    )

    archived = await ingestion.archive_only(
        _typed_remote(
            retailer="retailer-a",
            url=_URL_A,
            remote_id="archive-only-price",
            document_type=DocumentType.PRICE_FULL,
            source_timestamp=datetime(2026, 8, 10, 12, tzinfo=UTC),
            clock=clock,
        )
    )
    async with database.engine.connect() as connection:
        assert int((await connection.scalar(text("SELECT count(*) FROM retailer_items"))) or 0) == 0

    completed = await maintenance.start_rebuild(
        confirmation="REBUILD-NORMALIZED-STATE",
        requested_by="archive-only-contract",
    )
    retailer_id = await _retailer_id(database, "retailer-a")
    product = await PostgresQueryRepository(database.engine).find_product_by_retailer_item_code(
        retailer_id, "CLEAN-ROOM-SKU"
    )

    assert completed.source_files_total == completed.source_files_completed == 1
    assert product is not None
    assert product["match_method"] == "isolated_retailer_item"
    assert catalog.observed_statuses == ["pending"]
    async with database.engine.connect() as connection:
        checkpointed = await connection.scalar(
            text(
                """
                SELECT status FROM normalized_rebuild_files
                 WHERE rebuild_run_id = :run_id AND source_file_id = :source_file_id
                """
            ),
            {
                "run_id": completed.rebuild_run_id,
                "source_file_id": archived.source_file_id,
            },
        )
    assert checkpointed == "completed"
    assert downloader.open_count == 1


async def test_parser_correction_retains_superseded_temporal_audit(
    database: Database,
    tmp_path: Path,
) -> None:
    clock = FixedClock()
    payloads = {_URL_A: _FIXTURE.read_bytes(), _URL_DELTA: _DELTA_FIXTURE.read_bytes()}
    downloader = FakeDownloader(payloads, clock)
    archive = LocalContentAddressedArchive(tmp_path / "correction-archive")
    await archive.initialize()
    repository = PostgresIngestionRepository(database.engine)
    original = IngestionService(
        repository,
        downloader,
        archive,
        RetailXmlParser(XmlParserLimits(temporary_directory=tmp_path / "original-spool")),
        PostgresLeaseManager(database),
        clock,
        FakeMetrics(),
        worker_id="original-parser",
        policy=IngestionPolicy(minimum_full_records=1),
    )
    await original.ingest(
        _typed_remote(
            retailer="retailer-a",
            url=_URL_A,
            remote_id="price-full",
            document_type=DocumentType.PRICE_FULL,
            source_timestamp=datetime(2026, 8, 10, 12, tzinfo=UTC),
            clock=clock,
        )
    )
    await original.ingest(
        _typed_remote(
            retailer="retailer-a",
            url=_URL_DELTA,
            remote_id="price-delta",
            document_type=DocumentType.PRICE_DELTA,
            source_timestamp=datetime(2026, 8, 11, 12, tzinfo=UTC),
            clock=clock,
        )
    )
    async with database.engine.connect() as connection:
        old_initial_id = cast(
            UUID,
            await connection.scalar(text("SELECT id FROM price_history WHERE item_price = 19.90")),
        )

    correcting = IngestionService(
        repository,
        downloader,
        archive,
        _CorrectingParser(
            RetailXmlParser(XmlParserLimits(temporary_directory=tmp_path / "correction-spool"))
        ),
        PostgresLeaseManager(database),
        clock,
        FakeMetrics(),
        worker_id="correcting-parser",
        policy=IngestionPolicy(minimum_full_records=1),
    )
    maintenance_repository = PostgresArchiveMaintenanceRepository(database.engine)
    completed = await ArchiveMaintenanceService(
        maintenance_repository,
        maintenance_repository,
        correcting,
    ).start_rebuild(
        confirmation="REBUILD-NORMALIZED-STATE",
        requested_by="parser-correction-contract",
    )

    async with database.engine.connect() as connection:
        history = (
            await connection.execute(
                text("SELECT id, item_price FROM price_history ORDER BY valid_from")
            )
        ).all()
        retained = (
            await connection.execute(
                select(
                    normalized_rebuild_snapshots.c.entity,
                    normalized_rebuild_snapshots.c.row_key,
                    normalized_rebuild_snapshots.c.outcome,
                ).where(
                    normalized_rebuild_snapshots.c.rebuild_run_id == completed.rebuild_run_id,
                    normalized_rebuild_snapshots.c.phase == "original",
                )
            )
        ).all()
    assert [row.item_price for row in history] == [Decimal("20.0000"), Decimal("21.5000")]
    assert history[0].id != old_initial_id
    assert any(
        row.entity == "price_history"
        and row.row_key == str(old_initial_id)
        and row.outcome == "superseded"
        for row in retained
    )
    assert all(row.outcome in {"preserved", "superseded"} for row in retained)


async def test_finish_conflict_rolls_back_and_keeps_barrier_and_original_snapshot(
    database: Database,
    tmp_path: Path,
) -> None:
    payload = _FIXTURE.read_bytes()
    clock = FixedClock()
    downloader = FakeDownloader({_URL_A: payload}, clock)
    archive = LocalContentAddressedArchive(tmp_path / "conflict-archive")
    await archive.initialize()
    repository = PostgresIngestionRepository(database.engine)
    maintenance_repository = PostgresArchiveMaintenanceRepository(database.engine)
    ingestion = IngestionService(
        repository,
        downloader,
        archive,
        RetailXmlParser(XmlParserLimits(temporary_directory=tmp_path / "conflict-spool")),
        PostgresLeaseManager(database),
        clock,
        FakeMetrics(),
        worker_id="conflict-rebuild",
        policy=IngestionPolicy(minimum_full_records=1),
    )
    await ingestion.ingest(_remote("retailer-a", _URL_A, clock))
    run = await maintenance_repository.begin_rebuild(
        requested_by="conflict-contract",
        parser_version=ingestion.parser_version,
    )
    async with database.engine.begin() as connection:
        await connection.execute(
            text("UPDATE canonical_products SET name = 'Concurrent curated change'")
        )

    with pytest.raises(NormalizedRebuildInterruptedError):
        await ArchiveMaintenanceService(
            maintenance_repository,
            maintenance_repository,
            ingestion,
        ).resume_rebuild(run.rebuild_run_id)

    failed = await maintenance_repository.get_rebuild(run.rebuild_run_id)
    status = await maintenance_repository.maintenance_status()
    async with database.engine.connect() as connection:
        phase_rows = (
            await connection.execute(
                text(
                    """
                    SELECT phase, count(*) AS row_count
                      FROM normalized_rebuild_snapshots
                     WHERE rebuild_run_id = :run_id
                     GROUP BY phase
                    """
                ),
                {"run_id": run.rebuild_run_id},
            )
        ).mappings()
        phase_counts: dict[str, int] = {
            str(row["phase"]): int(row["row_count"]) for row in phase_rows
        }
    assert failed.status == "failed"
    assert status["active"] is True
    assert status["rebuild_run_id"] == run.rebuild_run_id
    assert phase_counts.get("original", 0) > 0
    assert phase_counts.get("rebuilt", 0) == 0
