"""Isolated PostgreSQL ingestion and query scale benchmarks.

The benchmark uses the production SQLAlchemy metadata and production persistence
repositories in a fixed, disposable schema.  It never truncates application tables.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final, cast
from uuid import UUID

from sqlalchemy import event, make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from benchmarks.measure import bytes_to_mib, latency_summary, measured_memory, rows_per_second
from benchmarks.synthetic import (
    SYNTHETIC_TIMESTAMP,
    gtin14,
    price_grid_record_batches,
    product_name,
)
from makolet.adapters.persistence.destructive_target import require_benchmark_database_target
from makolet.adapters.persistence.ingestion import (
    _MISSING_PRICE_KEYS_SELECT,
    PostgresIngestionRepository,
    _materialize_mapped_price_incoming,
    _materialize_price_maps,
)
from makolet.adapters.persistence.queries import (
    _CURRENT_PRICES_FIRST_PAGE_QUERY,
    _FRESHNESS_QUERY,
    _FUZZY_STORES_CURSOR_QUERY,
    _FUZZY_STORES_FIRST_PAGE_QUERY,
    _PRICE_HISTORY_STORE_FIRST_PAGE_QUERY,
    _PRODUCT_SEARCH_QUERY,
    _PROMOTION_HISTORY_QUERY,
    MAXIMUM_HISTORY_PROBE_RESULTS,
    MAXIMUM_PROMOTION_CHILDREN_PER_PAGE,
    MAXIMUM_PROMOTION_PROBE_RESULTS,
    MAXIMUM_PROMOTION_RELATIONS,
    MAXIMUM_SEARCH_CANDIDATES,
    PostgresQueryRepository,
)
from makolet.adapters.persistence.schema import QUERY_PROJECTION_MAINTENANCE_DDL, metadata
from makolet.application.models import (
    MAXIMUM_FRESHNESS_ITEMS_PER_STORE,
    ArchivedDownload,
    DownloadEvidence,
    Page,
)
from makolet.domain.enums import CompressionFormat, DocumentType, IngestionStatus, SourceProtocol
from makolet.domain.models import ArchiveReceipt, DocumentMetadata, RemoteFile
from makolet.domain.normalization import normalize_search_text

BENCHMARK_SCHEMA: Final = "makolet_benchmark"
_RETAILER_KEY: Final = "benchmark-retailer"
_PORTAL_KEY: Final = "benchmark-portal"
_INITIAL_SNAPSHOT_TIMESTAMP: Final = SYNTHETIC_TIMESTAMP
_RECONCILIATION_TIMESTAMP: Final = SYNTHETIC_TIMESTAMP + timedelta(hours=1)
_MINIMUM_DISK_HEADROOM_BYTES: Final = 2 * 1024 * 1024 * 1024
_PATHOLOGICAL_NESTED_LOOP_INNER_EXECUTIONS: Final = 100_000
_PATHOLOGICAL_NESTED_LOOP_INNER_TUPLE_VISITS: Final = 10_000_000
_EXPECTED_QUERY_PLANS: Final = frozenset(
    {
        "product_search",
        "barcode_lookup",
        "cross_store_price_comparison",
        "price_history",
        "promotion_history_bounded_page",
        "freshness_bounded_page",
        "fuzzy_store_cursor_page",
        "fuzzy_store_first_page",
    }
)
_EXPECTED_APPLY_PLANS: Final = frozenset(
    {
        "incoming_availability_change_detection",
        "incoming_availability_history_close_update",
        "incoming_availability_history_insert",
        "full_snapshot_missing_detection",
    }
)


class BenchmarkPlanError(RuntimeError):
    """The standard scale run produced an unacceptable or incomplete plan set."""

    def __init__(self, summary: dict[str, object]) -> None:
        super().__init__("standard PostgreSQL plan gate failed")
        self.summary = summary


def _emit_progress(phase: str, **measurements: object) -> None:
    payload = {"benchmark_progress": phase, **measurements}
    print(json.dumps(payload, sort_keys=True), flush=True)


@dataclass(frozen=True, slots=True)
class DatabaseScale:
    normalized_records: int
    ingestion_store_count: int
    store_count: int
    reconciliation_drop_records: int
    history_rows: int
    query_repetitions: int
    query_warmups: int

    def __post_init__(self) -> None:
        if self.normalized_records <= 0:
            raise ValueError("normalized_records must be positive")
        if self.store_count <= 0:
            raise ValueError("store_count must be positive")
        if not 0 < self.ingestion_store_count <= self.store_count:
            raise ValueError("ingestion_store_count must be within the final store count")
        if self.normalized_records % self.ingestion_store_count:
            raise ValueError("normalized_records must divide evenly across ingestion stores")
        if not 0 <= self.reconciliation_drop_records < self.normalized_records:
            raise ValueError("reconciliation drop must be nonnegative and below the record count")
        if self.history_rows <= 0:
            raise ValueError("history_rows must be positive")
        if self.query_repetitions <= 0 or self.query_warmups < 0:
            raise ValueError("query repetitions/warmups are invalid")

    @property
    def expected_current_prices(self) -> int:
        return self.unique_products * self.store_count

    @property
    def unique_products(self) -> int:
        return self.normalized_records // self.ingestion_store_count


@dataclass(slots=True)
class _StatementAggregate:
    calls: int = 0
    total_seconds: float = 0.0
    maximum_seconds: float = 0.0


class StatementTrace:
    """Aggregate DBAPI statement latency without retaining SQL parameters."""

    def __init__(self) -> None:
        self._started: dict[int, float] = {}
        self._aggregates: dict[str, _StatementAggregate] = {}

    def attach(self, engine: AsyncEngine) -> None:
        event.listen(engine.sync_engine, "before_cursor_execute", self._before)
        event.listen(engine.sync_engine, "after_cursor_execute", self._after)

    def results(self) -> list[dict[str, object]]:
        rows = [
            {
                "statement_family": family,
                "calls": aggregate.calls,
                "total_seconds": round(aggregate.total_seconds, 6),
                "maximum_seconds": round(aggregate.maximum_seconds, 6),
            }
            for family, aggregate in self._aggregates.items()
        ]
        return sorted(rows, key=lambda row: cast(float, row["total_seconds"]), reverse=True)

    def _before(self, *arguments: object) -> None:
        context = arguments[4]
        self._started[id(context)] = time.perf_counter()

    def _after(self, *arguments: object) -> None:
        statement = str(arguments[2])
        context = arguments[4]
        started = self._started.pop(id(context), None)
        if started is None:
            return
        duration = time.perf_counter() - started
        family = _statement_family(statement)
        aggregate = self._aggregates.setdefault(family, _StatementAggregate())
        aggregate.calls += 1
        aggregate.total_seconds += duration
        aggregate.maximum_seconds = max(aggregate.maximum_seconds, duration)


@dataclass(slots=True)
class BenchmarkSandbox:
    """Own the fixed benchmark schema and its search-path-bound engine."""

    database_url: str
    admin_engine: AsyncEngine | None = None
    engine: AsyncEngine | None = None

    async def create(self) -> AsyncEngine:
        self.admin_engine = create_async_engine(
            self.database_url,
            connect_args={
                "server_settings": {
                    "application_name": "makolet-benchmark-admin",
                    "statement_timeout": "0",
                    "timezone": "UTC",
                }
            },
            hide_parameters=True,
            pool_size=1,
            max_overflow=0,
        )
        async with self.admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{BENCHMARK_SCHEMA}" CASCADE'))
            await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            await connection.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gist"))
            await connection.execute(text(f'CREATE SCHEMA "{BENCHMARK_SCHEMA}"'))
        self.engine = create_async_engine(
            self.database_url,
            connect_args={
                "server_settings": {
                    "application_name": "makolet-scale-benchmark",
                    "search_path": f"{BENCHMARK_SCHEMA},public",
                    "statement_timeout": "0",
                    "timezone": "UTC",
                }
            },
            hide_parameters=True,
            pool_size=2,
            max_overflow=0,
            pool_pre_ping=True,
        )
        async with self.engine.begin() as connection:
            # PostgreSQL's inspector considers same-named tables later in the
            # search path visible.  `checkfirst=False` ensures we create the exact
            # metadata in the benchmark schema even when public is already migrated.
            await connection.run_sync(lambda sync: metadata.create_all(sync, checkfirst=False))
            for statement in QUERY_PROJECTION_MAINTENANCE_DDL:
                await connection.execute(text(statement))
        return self.engine

    async def close(self, *, keep_schema: bool) -> None:
        if self.engine is not None:
            await self.engine.dispose()
        if self.admin_engine is not None:
            if not keep_schema:
                async with self.admin_engine.begin() as connection:
                    await connection.execute(
                        text(f'DROP SCHEMA IF EXISTS "{BENCHMARK_SCHEMA}" CASCADE')
                    )
            await self.admin_engine.dispose()


async def run_database_benchmark(
    database_url: str,
    scale: DatabaseScale,
    *,
    database_confirmation: str | None,
    keep_schema: bool = False,
) -> dict[str, object]:
    """Run real staging/apply/query workloads and always recover the sandbox."""

    database_url = require_benchmark_database_target(
        database_url,
        confirmation=database_confirmation,
    )
    sandbox = BenchmarkSandbox(database_url)
    started = time.perf_counter()
    try:
        engine = await sandbox.create()
        statement_trace = StatementTrace()
        statement_trace.attach(engine)
        repository = PostgresIngestionRepository(engine)
        initial_environment = await _database_environment(engine)
        _emit_progress(
            "database_sandbox_ready",
            normalized_records=scale.normalized_records,
            expected_current_price_rows=scale.expected_current_prices,
        )

        first_source_id, first_digest = await _create_archived_source(
            repository,
            remote_id="price-full-initial",
            digest_seed=f"initial:{scale.normalized_records}",
            logical_bytes=scale.normalized_records * 586,
            source_timestamp=_INITIAL_SNAPSHOT_TIMESTAMP,
        )
        initial_ingestion = await _stage_and_apply(
            repository,
            first_source_id,
            scale.normalized_records,
            unique_products=scale.unique_products,
            maximum_drop_fraction=0.05,
            phase="initial_full_snapshot",
            source_updated_at=_INITIAL_SNAPSHOT_TIMESTAMP,
        )

        duplicate_detection = await _measure_duplicate_detection(
            repository,
            digest=first_digest,
            logical_bytes=scale.normalized_records * 586,
        )
        _emit_progress("duplicate_detection_completed", **duplicate_detection)

        reconciliation_count = scale.normalized_records - scale.reconciliation_drop_records
        reconciliation_id, _ = await _create_archived_source(
            repository,
            remote_id="price-full-reconciliation",
            digest_seed=f"reconciliation:{reconciliation_count}",
            logical_bytes=reconciliation_count * 586,
            source_timestamp=_RECONCILIATION_TIMESTAMP,
        )
        reconciliation = await _stage_and_apply(
            repository,
            reconciliation_id,
            reconciliation_count,
            unique_products=scale.unique_products,
            maximum_drop_fraction=(scale.reconciliation_drop_records / scale.unique_products)
            + 0.01,
            phase="full_snapshot_reconciliation",
            source_updated_at=_RECONCILIATION_TIMESTAMP,
        )
        apply_query_plans = await _apply_computation_plans(engine, reconciliation_id)
        _emit_progress("apply_explain_plans_completed")
        await _clear_staging_tables(engine)

        amplification = await _amplify_current_prices(
            engine,
            source_file_id=first_source_id,
            store_count=scale.store_count,
            ingestion_store_count=scale.ingestion_store_count,
            unique_products=scale.unique_products,
        )
        _emit_progress(
            "current_price_amplification_completed",
            rows=amplification["rows"],
            additional_rows=amplification["additional_rows"],
            duration_seconds=amplification["duration_seconds"],
            rows_per_second=amplification["rows_per_second"],
        )
        target_index = scale.unique_products // 2
        target = await _target_identity(engine, gtin14(target_index))
        history_seed = await _seed_target_history(
            engine,
            source_file_id=first_source_id,
            retailer_item_id=target[1],
            store_id=target[2],
            requested_rows=scale.history_rows,
        )
        _emit_progress("price_history_seed_completed", **history_seed)
        await _analyze_query_tables(engine)
        queries = await _measure_queries(
            engine,
            product_id=target[0],
            store_id=target[2],
            barcode=gtin14(target_index),
            search_query=product_name(target_index),
            repetitions=scale.query_repetitions,
            warmups=scale.query_warmups,
        )
        _emit_progress("query_measurements_completed", query_count=len(queries))
        plans = await _query_plans(
            engine,
            product_id=target[0],
            store_id=target[2],
            barcode=gtin14(target_index),
            search_query=product_name(target_index),
        )
        plan_gate = _plan_gate_summary(
            query_plans=plans,
            apply_plans=apply_query_plans,
            enforced=_is_standard_scale(scale),
        )
        _emit_progress(
            "plan_gate_completed",
            enforced=plan_gate["enforced"],
            passed=plan_gate["passed"],
            failure_count=plan_gate["failure_count"],
        )
        _enforce_plan_gate(plan_gate)
        final_environment = await _database_environment(engine)
        return {
            "scenario": "postgresql_ingestion_and_queries",
            "database_url": make_url(database_url).render_as_string(hide_password=True),
            "schema": BENCHMARK_SCHEMA,
            "scale": {
                "normalized_records": scale.normalized_records,
                "unique_products": scale.unique_products,
                "ingestion_stores": scale.ingestion_store_count,
                "final_stores": scale.store_count,
                "expected_current_price_rows": scale.expected_current_prices,
                "reconciliation_drop_records": scale.reconciliation_drop_records,
                "target_history_rows": scale.history_rows,
            },
            "database_environment_before": initial_environment,
            "initial_full_snapshot": initial_ingestion,
            "duplicate_file_detection": duplicate_detection,
            "full_snapshot_reconciliation": reconciliation,
            "apply_query_plans": apply_query_plans,
            "current_price_amplification": amplification,
            "price_history_seed": history_seed,
            "queries": queries,
            "query_plans": plans,
            "plan_gate": plan_gate,
            "database_statement_timings": statement_trace.results(),
            "database_environment_after": final_environment,
            "total_duration_seconds": round(time.perf_counter() - started, 6),
            "cleanup": (
                f"schema {BENCHMARK_SCHEMA} retained by request"
                if keep_schema
                else f"schema {BENCHMARK_SCHEMA} dropped after result capture"
            ),
            "memory_scope": (
                "process RSS covers the Python benchmark client; PostgreSQL shared buffers and "
                "on-disk relation sizes are reported separately"
            ),
        }
    finally:
        await sandbox.close(keep_schema=keep_schema)


async def _create_archived_source(
    repository: PostgresIngestionRepository,
    *,
    remote_id: str,
    digest_seed: str,
    logical_bytes: int,
    source_timestamp: datetime,
) -> tuple[UUID, str]:
    registered = await repository.register_discovery(
        RemoteFile(
            retailer_id=_RETAILER_KEY,
            portal_id=_PORTAL_KEY,
            protocol=SourceProtocol.FIXTURE,
            remote_id=remote_id,
            download_url=f"https://synthetic.invalid/{remote_id}.xml",
            original_filename=f"PriceFull-{remote_id}.xml",
            document_type=DocumentType.PRICE_FULL,
            compression=CompressionFormat.NONE,
            discovered_at=SYNTHETIC_TIMESTAMP,
            source_timestamp=source_timestamp,
            content_length=logical_bytes,
            media_type="application/xml",
        )
    )
    await repository.transition(
        registered.source_file_id,
        (IngestionStatus.DISCOVERED,),
        IngestionStatus.DOWNLOADING,
    )
    digest = hashlib.sha256(digest_seed.encode()).hexdigest()
    archived_at = datetime.now(UTC)
    duplicate = await repository.record_archive(
        registered.source_file_id,
        ArchivedDownload(
            archive=ArchiveReceipt(
                content_sha256=digest,
                object_key=f"sha256/{digest}",
                content_length=logical_bytes,
                archived_at=archived_at,
                created=True,
            ),
            evidence=DownloadEvidence(
                started_at=archived_at - timedelta(seconds=1),
                finished_at=archived_at,
                status_code=200,
                content_length=logical_bytes,
                media_type="application/xml",
                etag=None,
                last_modified=SYNTHETIC_TIMESTAMP,
            ),
        ),
        parser_version="benchmark-synthetic/1",
    )
    if duplicate:
        raise RuntimeError("A newly seeded benchmark source unexpectedly matched applied content")
    return registered.source_file_id, digest


async def _stage_and_apply(
    repository: PostgresIngestionRepository,
    source_file_id: UUID,
    record_count: int,
    *,
    unique_products: int,
    maximum_drop_fraction: float,
    phase: str,
    source_updated_at: datetime,
) -> dict[str, object]:
    await repository.transition(
        source_file_id,
        (IngestionStatus.ARCHIVED,),
        IngestionStatus.PARSING,
    )
    await repository.clear_staging(source_file_id)
    await repository.stage(
        source_file_id,
        (
            DocumentMetadata(
                source_file_id=source_file_id,
                document_type=DocumentType.PRICE_FULL,
                chain_id="synthetic-chain",
                subchain_id="synthetic-subchain",
                store_id="store-0001",
                audit_number="synthetic-audit",
                source_updated_at=source_updated_at,
            ),
        ),
    )
    staged_records = 0
    stage_started = time.perf_counter()
    with measured_memory() as stage_memory:
        for batch in price_grid_record_batches(
            source_file_id,
            record_count,
            unique_products=unique_products,
        ):
            summary = await repository.stage(source_file_id, batch)
            staged_records += summary.price_records
            if staged_records % 100_000 == 0:
                _require_disk_headroom()
    stage_duration = time.perf_counter() - stage_started
    if staged_records != record_count:
        raise RuntimeError("PostgreSQL COPY staging lost normalized benchmark records")
    _emit_progress(
        f"{phase}.stage_completed",
        records=record_count,
        duration_seconds=round(stage_duration, 6),
        rows_per_second=rows_per_second(record_count, stage_duration),
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.PARSING,),
        IngestionStatus.STAGED,
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.STAGED,),
        IngestionStatus.VALIDATING,
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.VALIDATING,),
        IngestionStatus.APPLYING,
    )
    apply_started = time.perf_counter()
    _require_disk_headroom()
    with measured_memory() as apply_memory:
        applied = await repository.apply(
            source_file_id,
            DocumentType.PRICE_FULL,
            minimum_full_records=unique_products,
            maximum_drop_fraction=maximum_drop_fraction,
        )
    apply_duration = time.perf_counter() - apply_started
    _emit_progress(
        f"{phase}.apply_completed",
        records=record_count,
        duration_seconds=round(apply_duration, 6),
        rows_per_second=rows_per_second(record_count, apply_duration),
        inserted=applied.inserted,
        updated=applied.updated,
        unchanged=applied.unchanged,
        unavailable=applied.unavailable,
        history_events=applied.history_events,
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.APPLYING,),
        IngestionStatus.COMPLETED,
    )
    stage_reading = stage_memory.reading
    apply_reading = apply_memory.reading
    return {
        "records": record_count,
        "stage": {
            "duration_seconds": round(stage_duration, 6),
            "rows_per_second": rows_per_second(record_count, stage_duration),
            "peak_client_rss_bytes": stage_reading.peak_rss_bytes,
            "peak_client_rss_mib": bytes_to_mib(stage_reading.peak_rss_bytes),
            "peak_client_rss_delta_bytes": stage_reading.peak_delta_bytes,
            "peak_client_rss_delta_mib": bytes_to_mib(stage_reading.peak_delta_bytes),
            "copy_batch_size": 5_000,
        },
        "apply": {
            "duration_seconds": round(apply_duration, 6),
            "rows_per_second": rows_per_second(record_count, apply_duration),
            "peak_client_rss_bytes": apply_reading.peak_rss_bytes,
            "peak_client_rss_mib": bytes_to_mib(apply_reading.peak_rss_bytes),
            "peak_client_rss_delta_bytes": apply_reading.peak_delta_bytes,
            "peak_client_rss_delta_mib": bytes_to_mib(apply_reading.peak_delta_bytes),
            "inserted": applied.inserted,
            "updated": applied.updated,
            "unchanged": applied.unchanged,
            "unavailable": applied.unavailable,
            "history_events": applied.history_events,
        },
    }


async def _measure_duplicate_detection(
    repository: PostgresIngestionRepository,
    *,
    digest: str,
    logical_bytes: int,
) -> dict[str, object]:
    registered = await repository.register_discovery(
        RemoteFile(
            retailer_id=_RETAILER_KEY,
            portal_id=_PORTAL_KEY,
            protocol=SourceProtocol.FIXTURE,
            remote_id="price-full-exact-duplicate",
            download_url="https://synthetic.invalid/price-full-exact-duplicate.xml",
            original_filename="PriceFull-exact-duplicate.xml",
            document_type=DocumentType.PRICE_FULL,
            compression=CompressionFormat.NONE,
            discovered_at=SYNTHETIC_TIMESTAMP,
            source_timestamp=SYNTHETIC_TIMESTAMP,
            content_length=logical_bytes,
            media_type="application/xml",
        )
    )
    await repository.transition(
        registered.source_file_id,
        (IngestionStatus.DISCOVERED,),
        IngestionStatus.DOWNLOADING,
    )
    observed_at = datetime.now(UTC)
    started = time.perf_counter()
    duplicate = await repository.record_archive(
        registered.source_file_id,
        ArchivedDownload(
            archive=ArchiveReceipt(
                content_sha256=digest,
                object_key=f"sha256/{digest}",
                content_length=logical_bytes,
                archived_at=observed_at,
                created=False,
            ),
            evidence=DownloadEvidence(
                started_at=observed_at,
                finished_at=observed_at,
                status_code=200,
                content_length=logical_bytes,
                media_type="application/xml",
                etag=None,
                last_modified=SYNTHETIC_TIMESTAMP,
            ),
        ),
        parser_version="benchmark-synthetic/1",
    )
    duration = time.perf_counter() - started
    if not duplicate:
        raise RuntimeError("Exact archived content was not reused from the immutable archive")
    return {
        "scenario": "immutable_archive_cas_duplicate_detection",
        "archive_object_reused": True,
        "duration_seconds": round(duration, 6),
        "latency_ms": round(duration * 1_000, 3),
        "source_status_after": IngestionStatus.ARCHIVED.value,
        "normalized_processing": (
            "not measured; a distinct source identity remains eligible for validated "
            "staging reuse and apply"
        ),
    }


async def _clear_staging_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "TRUNCATE staged_promotion_stores, staged_promotion_items, "
                "staged_promotion_clubs, staged_promotions, staged_prices, "
                "staged_stores, staged_documents"
            )
        )


async def _amplify_current_prices(
    engine: AsyncEngine,
    *,
    source_file_id: UUID,
    store_count: int,
    ingestion_store_count: int,
    unique_products: int,
) -> dict[str, object]:
    async with engine.begin() as connection:
        identity = (
            await connection.execute(
                text(
                    """
                    SELECT retailer.id AS retailer_id, portal.id AS portal_id,
                           store.id AS base_store_id
                      FROM stores store
                      JOIN retailers retailer
                        ON retailer.id = store.retailer_id
                       AND retailer.source_key = :retailer_source_key
                      JOIN portals portal
                        ON portal.id = store.portal_id
                       AND portal.retailer_id = retailer.id
                       AND portal.source_key = :portal_source_key
                     WHERE store.subchain_code = 'synthetic-subchain'
                       AND store.source_store_code = 'store-0001'
                    """
                ),
                {
                    "retailer_source_key": _RETAILER_KEY,
                    "portal_source_key": _PORTAL_KEY,
                },
            )
        ).one()
        retailer_id, portal_id, base_store_id = cast("tuple[UUID, UUID, UUID]", tuple(identity))
        if store_count > ingestion_store_count:
            await connection.execute(
                text(
                    """
                    INSERT INTO stores (
                        retailer_id, portal_id, chain_code, subchain_code, source_store_code,
                        name, is_active, first_seen_at, last_seen_at, last_source_file_id
                    )
                    SELECT :retailer_id, :portal_id,
                           'synthetic-chain', 'synthetic-subchain',
                           'store-' || lpad(number::text, 4, '0'),
                           'Synthetic Store ' || number, true,
                           clock_timestamp(), clock_timestamp(), :source_file_id
                      FROM generate_series(
                               CAST(:first_store AS integer),
                               CAST(:store_count AS integer)
                           ) number
                    """
                ),
                {
                    "retailer_id": retailer_id,
                    "portal_id": portal_id,
                    "source_file_id": source_file_id,
                    "first_store": ingestion_store_count + 1,
                    "store_count": store_count,
                },
            )
    per_store_seconds: list[float] = []
    started = time.perf_counter()
    with measured_memory() as memory:
        for number in range(ingestion_store_count + 1, store_count + 1):
            _require_disk_headroom()
            store_code = f"store-{number:04d}"
            store_started = time.perf_counter()
            async with engine.begin() as connection:
                inserted = int(
                    (
                        await connection.execute(
                            text(
                                """
                                INSERT INTO current_prices (
                                    retailer_item_id, store_id, item_price,
                                    unit_of_measure_price, allow_discount,
                                    source_updated_at, last_sale_at, audit_number,
                                    source_file_id, first_observed_at, last_observed_at
                                )
                                SELECT price.retailer_item_id, target.id,
                                       price.item_price, price.unit_of_measure_price,
                                       price.allow_discount, price.source_updated_at,
                                       price.last_sale_at, price.audit_number,
                                       :source_file_id, price.first_observed_at,
                                       price.last_observed_at
                                  FROM current_prices price
                                  JOIN stores base ON base.id = price.store_id
                                  JOIN stores target
                                    ON target.retailer_id = base.retailer_id
                                   AND target.portal_id = base.portal_id
                                   AND target.source_store_code = :store_code
                                 WHERE price.store_id = :base_store_id
                                """
                            ),
                            {
                                "source_file_id": source_file_id,
                                "store_code": store_code,
                                "base_store_id": base_store_id,
                            },
                        )
                    ).rowcount
                )
            if inserted != unique_products:
                raise RuntimeError(
                    f"Current-price amplification inserted {inserted}, expected {unique_products}"
                )
            per_store_seconds.append(time.perf_counter() - store_started)
    duration = time.perf_counter() - started
    async with engine.connect() as connection:
        final_count = int(
            (await connection.execute(text("SELECT count(*) FROM current_prices"))).scalar_one()
        )
    expected_count = unique_products * store_count
    if final_count != expected_count:
        raise RuntimeError(f"Current-price row count is {final_count}; expected {expected_count}")
    reading = memory.reading
    return {
        "rows": final_count,
        "additional_rows": unique_products * (store_count - ingestion_store_count),
        "duration_seconds": round(duration, 6),
        "rows_per_second": rows_per_second(
            unique_products * max(0, store_count - ingestion_store_count), duration
        ),
        "per_additional_store_seconds": [round(value, 6) for value in per_store_seconds],
        "peak_client_rss_bytes": reading.peak_rss_bytes,
        "peak_client_rss_mib": bytes_to_mib(reading.peak_rss_bytes),
        "peak_client_rss_delta_bytes": reading.peak_delta_bytes,
        "peak_client_rss_delta_mib": bytes_to_mib(reading.peak_delta_bytes),
    }


async def _target_identity(engine: AsyncEngine, barcode: str) -> tuple[UUID, UUID, UUID]:
    async with engine.connect() as connection:
        row = (
            await connection.execute(
                text(
                    """
                    SELECT identifier.product_id, item.id AS retailer_item_id,
                           store.id AS store_id
                      FROM product_identifiers identifier
                      JOIN confirmed_product_matches match
                        ON match.canonical_product_id = identifier.product_id
                      JOIN retailer_items item ON item.id = match.retailer_item_id
                      JOIN stores store ON store.retailer_id = item.retailer_id
                     WHERE identifier.kind = 'gtin'
                       AND identifier.normalized_value = :barcode
                       AND store.source_store_code = 'store-0001'
                     ORDER BY item.id
                     LIMIT 1
                    """
                ),
                {"barcode": barcode},
            )
        ).one()
    return cast("tuple[UUID, UUID, UUID]", tuple(row))


async def _seed_target_history(
    engine: AsyncEngine,
    *,
    source_file_id: UUID,
    retailer_item_id: UUID,
    store_id: UUID,
    requested_rows: int,
) -> dict[str, object]:
    async with engine.connect() as connection:
        existing = int(
            (
                await connection.execute(
                    text(
                        "SELECT count(*) FROM price_history "
                        "WHERE retailer_item_id = :retailer_item_id AND store_id = :store_id"
                    ),
                    {"retailer_item_id": retailer_item_id, "store_id": store_id},
                )
            ).scalar_one()
        )
    additions = max(0, requested_rows - existing)
    started = time.perf_counter()
    if additions:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO price_history (
                        retailer_item_id, store_id, item_price,
                        unit_of_measure_price, allow_discount, source_updated_at,
                        last_sale_at, audit_number, source_file_id, valid_from, valid_to
                    )
                    SELECT :retailer_item_id, :store_id,
                           (10 + (number % 500)::numeric / 100)::numeric(14, 4),
                           (10 + (number % 500)::numeric / 100)::numeric(14, 4),
                           true, timestamptz '2025-01-01 00:00:00+00', NULL,
                           'synthetic-audit', :source_file_id,
                           timestamptz '2025-01-01 00:00:00+00'
                               + number * interval '1 minute',
                           timestamptz '2025-01-01 00:00:00+00'
                               + (number + 1) * interval '1 minute'
                      FROM generate_series(1, :additions) number
                    """
                ),
                {
                    "retailer_item_id": retailer_item_id,
                    "store_id": store_id,
                    "source_file_id": source_file_id,
                    "additions": additions,
                },
            )
    duration = time.perf_counter() - started
    async with engine.connect() as connection:
        final_count = int(
            (
                await connection.execute(
                    text(
                        "SELECT count(*) FROM price_history "
                        "WHERE retailer_item_id = :retailer_item_id AND store_id = :store_id"
                    ),
                    {"retailer_item_id": retailer_item_id, "store_id": store_id},
                )
            ).scalar_one()
        )
    if final_count != requested_rows:
        raise RuntimeError(f"Price-history target has {final_count}, expected {requested_rows}")
    return {
        "rows_for_target_item_store": final_count,
        "additional_rows": additions,
        "duration_seconds": round(duration, 6),
        "rows_per_second": rows_per_second(additions, duration),
    }


async def _analyze_query_tables(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        await connection.execution_options(isolation_level="AUTOCOMMIT")
        for table_name in (
            "canonical_products",
            "product_identifiers",
            "confirmed_product_matches",
            "retailer_items",
            "stores",
            "current_prices",
            "price_history",
            "current_availability",
        ):
            await connection.execute(text(f'ANALYZE "{table_name}"'))


async def _measure_queries(
    engine: AsyncEngine,
    *,
    product_id: UUID,
    store_id: UUID,
    barcode: str,
    search_query: str,
    repetitions: int,
    warmups: int,
) -> dict[str, object]:
    repository = PostgresQueryRepository(engine)

    async def search() -> Page:
        return await repository.search_products(
            search_query,
            quantity=None,
            unit=None,
            limit=50,
            cursor=None,
        )

    async def barcode_lookup() -> dict[str, object] | None:
        return await repository.find_product_by_barcode(barcode)

    async def comparison() -> Page:
        return await repository.current_prices(
            product_id,
            retailer_id=None,
            store_id=None,
            limit=50,
            cursor=None,
        )

    async def history() -> Page:
        return await repository.price_history(
            product_id,
            store_id=store_id,
            since=datetime(2024, 1, 1, tzinfo=UTC),
            until=datetime(2027, 1, 1, tzinfo=UTC),
            limit=50,
            cursor=None,
        )

    results: dict[str, object] = {}
    for name, operation, expected_minimum in (
        ("product_search", search, 1),
        ("barcode_lookup", barcode_lookup, 1),
        ("cross_store_price_comparison", comparison, 1),
        ("price_history", history, 1),
    ):
        summary, last = await _measure_async(
            operation,
            repetitions=repetitions,
            warmups=warmups,
        )
        result_count = _result_count(last)
        if result_count < expected_minimum:
            raise RuntimeError(f"Measured query {name} returned no representative result")
        summary["returned_rows"] = result_count
        results[name] = summary
    return results


async def _measure_async[T](
    operation: Callable[[], Awaitable[T]],
    *,
    repetitions: int,
    warmups: int,
) -> tuple[dict[str, float | int], T]:
    result: T | None = None
    for _ in range(warmups):
        result = await operation()
    latencies: list[float] = []
    for _ in range(repetitions):
        started = time.perf_counter()
        result = await operation()
        latencies.append((time.perf_counter() - started) * 1_000)
    if result is None:
        raise RuntimeError("Measured async operation produced no result")
    return latency_summary(latencies), result


def _result_count(result: object) -> int:
    if isinstance(result, Page):
        return len(result.items)
    return int(result is not None)


def _query_plan_statements(
    *,
    product_id: UUID,
    store_id: UUID,
    barcode: str,
    search_query: str,
) -> dict[str, tuple[str, Mapping[str, object], set[str]]]:
    return {
        "product_search": (
            _PRODUCT_SEARCH_QUERY,
            {
                "query": normalize_search_text(search_query),
                "query_quantity": None,
                "query_unit": None,
                "cursor_id": None,
                "limit": 51,
                "candidate_limit": MAXIMUM_SEARCH_CANDIDATES,
            },
            {
                "canonical_products",
                "product_identifiers",
                "retailer_items",
                "confirmed_product_matches",
            },
        ),
        "barcode_lookup": (
            """
            SELECT product.id, product.name
              FROM product_identifiers identifier
              JOIN canonical_products product ON product.id = identifier.product_id
             WHERE identifier.kind = 'gtin'
               AND identifier.normalized_value = :barcode
               AND identifier.is_validated
               AND product.status = 'active'
             ORDER BY product.id LIMIT 1
            """,
            {"barcode": barcode},
            {"product_identifiers", "canonical_products"},
        ),
        "cross_store_price_comparison": (
            _CURRENT_PRICES_FIRST_PAGE_QUERY,
            {"product_id": product_id, "candidate_limit": 51},
            {"current_prices"},
        ),
        "price_history": (
            _PRICE_HISTORY_STORE_FIRST_PAGE_QUERY,
            {
                "product_id": product_id,
                "store_id": store_id,
                "since": datetime(2024, 1, 1, tzinfo=UTC),
                "until": datetime(2027, 1, 1, tzinfo=UTC),
                "candidate_limit": 51,
                "probe_limit": MAXIMUM_HISTORY_PROBE_RESULTS,
            },
            {"price_history"},
        ),
        "promotion_history_bounded_page": (
            _PROMOTION_HISTORY_QUERY,
            {
                "product_id": None,
                "store_id": None,
                "since": datetime(2024, 1, 1, tzinfo=UTC),
                "until": datetime(2027, 1, 1, tzinfo=UTC),
                "cursor_id": None,
                "candidate_limit": 2,
                "page_limit": 1,
                "relation_page_limit": MAXIMUM_PROMOTION_CHILDREN_PER_PAGE,
                "relation_limit": MAXIMUM_PROMOTION_RELATIONS,
                "relation_probe_limit": MAXIMUM_PROMOTION_RELATIONS + 1,
                "probe_limit": MAXIMUM_PROMOTION_PROBE_RESULTS,
            },
            set(),
        ),
        "freshness_bounded_page": (
            _FRESHNESS_QUERY,
            {
                "cursor_id": None,
                "limit": 2,
                "item_limit": MAXIMUM_FRESHNESS_ITEMS_PER_STORE,
                "item_probe_limit": MAXIMUM_FRESHNESS_ITEMS_PER_STORE + 1,
            },
            {"current_availability"},
        ),
        "fuzzy_store_first_page": (
            _FUZZY_STORES_FIRST_PAGE_QUERY,
            {
                "candidate_limit": MAXIMUM_SEARCH_CANDIDATES,
                "candidate_probe_limit": MAXIMUM_SEARCH_CANDIDATES + 1,
                "city": None,
                "page_limit": 51,
                "query": normalize_search_text(search_query),
                "retailer_id": None,
            },
            set(),
        ),
        "fuzzy_store_cursor_page": (
            _FUZZY_STORES_CURSOR_QUERY,
            {
                "candidate_limit": MAXIMUM_SEARCH_CANDIDATES,
                "candidate_probe_limit": MAXIMUM_SEARCH_CANDIDATES + 1,
                "city": None,
                "cursor_id": store_id,
                "page_limit": 51,
                "query": normalize_search_text(search_query),
                "retailer_id": None,
            },
            set(),
        ),
    }


async def _query_plans(
    engine: AsyncEngine,
    *,
    product_id: UUID,
    store_id: UUID,
    barcode: str,
    search_query: str,
) -> dict[str, object]:
    statements = _query_plan_statements(
        product_id=product_id,
        store_id=store_id,
        barcode=barcode,
        search_query=search_query,
    )
    results: dict[str, object] = {}
    async with engine.connect() as connection:
        for name, (sql, parameters, important_relations) in statements.items():
            raw = (
                await connection.execute(
                    text(f"EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) {sql}"),
                    parameters,
                )
            ).scalar_one()
            payload = json.loads(raw) if isinstance(raw, str) else raw
            root = payload[0] if isinstance(payload, list) else payload
            plan = root["Plan"]
            scans = _scan_nodes(plan)
            sequential = sorted(
                {
                    str(scan["relation"])
                    for scan in scans
                    if scan["node_type"] == "Seq Scan"
                    and scan.get("relation") in important_relations
                }
            )
            results[name] = {
                "planning_time_ms": root.get("Planning Time"),
                "execution_time_ms": root.get("Execution Time"),
                "plan_nodes": scans,
                "join_nodes": _join_nodes(plan),
                "sequential_scans_on_important_relations": sequential,
                "uses_no_important_sequential_scan": not sequential,
                "plan": root,
            }
    return results


def _apply_plan_statements() -> dict[str, str]:
    return {
        "incoming_availability_change_detection": """
            SELECT count(*)
              FROM pg_temp.makolet_mapped_price_incoming incoming
              JOIN current_availability current
                ON current.retailer_item_id = incoming.retailer_item_id
               AND current.store_id = incoming.store_id
             WHERE current.is_available IS DISTINCT FROM incoming.is_available
                OR current.item_status IS DISTINCT FROM incoming.item_status
            """,
        "full_snapshot_missing_detection": _MISSING_PRICE_KEYS_SELECT,
        "incoming_availability_history_close_update": """
            WITH changed AS MATERIALIZED (
                SELECT incoming.*
                  FROM pg_temp.makolet_mapped_price_incoming incoming
                  JOIN current_availability current
                    ON current.retailer_item_id = incoming.retailer_item_id
                   AND current.store_id = incoming.store_id
                 WHERE current.is_available IS DISTINCT FROM incoming.is_available
                    OR current.item_status IS DISTINCT FROM incoming.item_status
            )
            UPDATE availability_history history
               SET valid_to = :applied_at
              FROM changed
             WHERE history.retailer_item_id = changed.retailer_item_id
               AND history.store_id = changed.store_id
               AND history.valid_to IS NULL
            """,
        "incoming_availability_history_insert": """
            INSERT INTO availability_history (
                id, retailer_item_id, store_id, is_available, item_status,
                source_file_id, valid_from, valid_to
            )
            SELECT uuidv7(), incoming.retailer_item_id, incoming.store_id,
                   incoming.is_available, incoming.item_status,
                   :source_file_id, :applied_at, NULL
              FROM pg_temp.makolet_mapped_price_incoming incoming
              LEFT JOIN current_availability current
                ON current.retailer_item_id = incoming.retailer_item_id
               AND current.store_id = incoming.store_id
             WHERE current.id IS NULL
                OR current.is_available IS DISTINCT FROM incoming.is_available
                OR current.item_status IS DISTINCT FROM incoming.item_status
            """,
    }


async def _apply_computation_plans(
    engine: AsyncEngine,
    source_file_id: UUID,
) -> dict[str, object]:
    """Explain apply consumers against the same analyzed maps as production."""

    async with engine.connect() as connection:
        source_scope = (
            await connection.execute(
                text("SELECT retailer_id, portal_id FROM source_files WHERE id = :source_file_id"),
                {"source_file_id": source_file_id},
            )
        ).one()
        parameters = {
            "source_file_id": source_file_id,
            "retailer_id": source_scope.retailer_id,
            "portal_id": source_scope.portal_id,
            "applied_at": datetime.now(UTC),
        }
        await _materialize_price_maps(
            connection,
            source_file_id=source_file_id,
            retailer_id=source_scope.retailer_id,
            portal_id=source_scope.portal_id,
        )
        await _materialize_mapped_price_incoming(connection, parameters)
        statements = _apply_plan_statements()
        results: dict[str, object] = {}
        for name, statement in statements.items():
            raw = (
                await connection.execute(
                    text(f"EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) {statement}"),
                    parameters,
                )
            ).scalar_one()
            payload = json.loads(raw) if isinstance(raw, str) else raw
            root = payload[0] if isinstance(payload, list) else payload
            results[name] = {
                "planning_time_ms": root.get("Planning Time"),
                "execution_time_ms": root.get("Execution Time"),
                "plan_nodes": _scan_nodes(root["Plan"]),
                "join_nodes": _join_nodes(root["Plan"]),
                "plan": root,
            }
    return results


def _scan_nodes(plan: Mapping[str, Any]) -> list[dict[str, object]]:
    nodes: list[dict[str, object]] = []
    queue: list[Mapping[str, Any]] = [plan]
    while queue:
        node = queue.pop()
        node_type = str(node.get("Node Type", ""))
        if "Scan" in node_type:
            nodes.append(
                {
                    "node_type": node_type,
                    "relation": node.get("Relation Name"),
                    "index": node.get("Index Name"),
                    "actual_rows": node.get("Actual Rows"),
                    "actual_loops": node.get("Actual Loops"),
                    "shared_hit_blocks": node.get("Shared Hit Blocks"),
                    "shared_read_blocks": node.get("Shared Read Blocks"),
                }
            )
        queue.extend(cast("list[Mapping[str, Any]]", node.get("Plans", [])))
    return nodes


def _join_nodes(plan: Mapping[str, Any]) -> list[dict[str, object]]:
    """Summarize joins and expose high-cardinality nested-loop inner execution."""

    nodes: list[dict[str, object]] = []
    queue: list[Mapping[str, Any]] = [plan]
    while queue:
        node = queue.pop()
        children = cast("list[Mapping[str, Any]]", node.get("Plans", []))
        node_type = str(node.get("Node Type", ""))
        if node_type in {"Nested Loop", "Hash Join", "Merge Join"}:
            inner_executions = (
                _numeric_plan_value(children[1], "Actual Loops") if len(children) > 1 else 0
            )
            inner_rows = _numeric_plan_value(children[1], "Actual Rows") if len(children) > 1 else 0
            inner_tuple_visits = inner_executions * inner_rows
            nodes.append(
                {
                    "node_type": node_type,
                    "join_type": node.get("Join Type"),
                    "actual_rows": node.get("Actual Rows"),
                    "actual_loops": node.get("Actual Loops"),
                    "inner_node_type": children[1].get("Node Type") if len(children) > 1 else None,
                    "inner_actual_loops": inner_executions,
                    "inner_actual_rows_per_loop": inner_rows,
                    "inner_tuple_visits": inner_tuple_visits,
                    "pathological_nested_loop": (
                        node_type == "Nested Loop"
                        and inner_executions > _PATHOLOGICAL_NESTED_LOOP_INNER_EXECUTIONS
                        and inner_tuple_visits > _PATHOLOGICAL_NESTED_LOOP_INNER_TUPLE_VISITS
                    ),
                }
            )
        queue.extend(children)
    return nodes


def _numeric_plan_value(node: Mapping[str, Any], key: str) -> float:
    value = node.get(key, 0)
    return float(value) if isinstance(value, (int, float)) else 0.0


def _plan_gate_summary(
    *,
    query_plans: Mapping[str, object],
    apply_plans: Mapping[str, object],
    enforced: bool,
) -> dict[str, object]:
    """Return a machine-readable plan gate; callers enforce it for standard scale."""

    missing_query_plans = sorted(_EXPECTED_QUERY_PLANS.difference(query_plans))
    missing_apply_plans = sorted(_EXPECTED_APPLY_PLANS.difference(apply_plans))
    important_sequential_scans: list[dict[str, str]] = []
    for name, result in query_plans.items():
        if not isinstance(result, Mapping):
            continue
        scans = result.get("sequential_scans_on_important_relations", [])
        if not isinstance(scans, (list, tuple)):
            continue
        important_sequential_scans.extend(
            {"plan": name, "relation": str(relation)} for relation in scans
        )

    pathological_joins: list[dict[str, object]] = []
    for name, result in apply_plans.items():
        if not isinstance(result, Mapping):
            continue
        join_nodes = result.get("join_nodes", [])
        if not isinstance(join_nodes, (list, tuple)):
            continue
        pathological_joins.extend(
            {"plan": name, **dict(node)}
            for node in join_nodes
            if isinstance(node, Mapping) and node.get("pathological_nested_loop") is True
        )

    failures: list[dict[str, object]] = []
    if missing_query_plans:
        failures.append({"kind": "missing_query_plans", "plans": missing_query_plans})
    if missing_apply_plans:
        failures.append({"kind": "missing_apply_plans", "plans": missing_apply_plans})
    if important_sequential_scans:
        failures.append(
            {
                "kind": "important_permanent_relation_sequential_scan",
                "scans": important_sequential_scans,
            }
        )
    if pathological_joins:
        failures.append(
            {
                "kind": "pathological_nested_loop",
                "joins": pathological_joins,
            }
        )
    return {
        "enforced": enforced,
        "passed": not failures,
        "failure_count": len(failures),
        "failures": failures,
        "policy": {
            "expected_query_plans": sorted(_EXPECTED_QUERY_PLANS),
            "expected_apply_plans": sorted(_EXPECTED_APPLY_PLANS),
            "reject_important_query_sequential_scans": True,
            "maximum_nested_loop_inner_executions": (_PATHOLOGICAL_NESTED_LOOP_INNER_EXECUTIONS),
            "maximum_nested_loop_inner_tuple_visits": (
                _PATHOLOGICAL_NESTED_LOOP_INNER_TUPLE_VISITS
            ),
        },
    }


def _is_standard_scale(scale: DatabaseScale) -> bool:
    return (
        scale.normalized_records == 1_000_000
        and scale.ingestion_store_count == 10
        and scale.store_count == 100
        and scale.reconciliation_drop_records == 10_000
        and scale.history_rows == 10_000
        and scale.query_repetitions == 100
        and scale.query_warmups == 10
    )


def _enforce_plan_gate(summary: dict[str, object]) -> None:
    if summary.get("enforced") is True and summary.get("passed") is not True:
        raise BenchmarkPlanError(summary)


async def _database_environment(engine: AsyncEngine) -> dict[str, object]:
    async with engine.connect() as connection:
        row = (
            (
                await connection.execute(
                    text(
                        """
                    SELECT version() AS version,
                           current_setting('server_version_num') AS server_version_num,
                           current_setting('shared_buffers') AS shared_buffers,
                           current_setting('effective_cache_size') AS effective_cache_size,
                           current_setting('work_mem') AS work_mem,
                           current_setting('max_connections') AS max_connections
                    """
                    )
                )
            )
            .mappings()
            .one()
        )
        schema_bytes = int(
            (
                await connection.execute(
                    text(
                        """
                        SELECT COALESCE(sum(pg_total_relation_size(class.oid)), 0)::bigint
                          FROM pg_class class
                          JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
                         WHERE namespace.nspname = :schema
                           AND class.relkind IN ('r', 'm')
                        """
                    ),
                    {"schema": BENCHMARK_SCHEMA},
                )
            ).scalar_one()
        )
        counts = (
            (
                await connection.execute(
                    text(
                        """
                    SELECT (SELECT count(*) FROM canonical_products) AS canonical_products,
                           (SELECT count(*) FROM retailer_items) AS retailer_items,
                           (SELECT count(*) FROM current_prices) AS current_prices,
                           (SELECT count(*) FROM price_history) AS price_history
                    """
                    )
                )
            )
            .mappings()
            .one()
        )
    return {
        **dict(row),
        "schema_total_relation_bytes": schema_bytes,
        "schema_total_relation_mib": bytes_to_mib(schema_bytes),
        "row_counts": {key: int(value) for key, value in counts.items()},
    }


def _statement_family(statement: str) -> str:
    normalized = " ".join(statement.casefold().split())
    if "missing as materialized" in normalized:
        if "update availability_history" in normalized:
            return "full snapshot: close missing availability history"
        if "insert into availability_history" in normalized:
            return "full snapshot: insert missing availability history"
        if "update current_availability" in normalized:
            return "full snapshot: mark missing unavailable"
    signatures = (
        ("makolet:gtin:", "apply: advisory locks per GTIN"),
        ("insert into canonical_products", "apply: insert canonical products/identifiers"),
        ("insert into confirmed_product_matches", "apply: confirm exact GTIN matches"),
        ("insert into retailer_items", "apply: upsert retailer items"),
        ("insert into stores", "apply/seed: upsert stores"),
        ("insert into price_history", "apply/seed: insert price history"),
        ("update price_history", "apply: close price history"),
        ("insert into current_prices", "apply/seed: upsert current prices"),
        ("insert into availability_history", "apply: insert availability history"),
        ("update availability_history", "apply: close availability history"),
        ("insert into current_availability", "apply: upsert current availability"),
        ("update current_availability", "apply: reconcile availability"),
        ("insert into applied_source_contents", "apply: claim content"),
        ("explain (analyze", "query: explain analyze"),
        ("analyze ", "maintenance: analyze"),
    )
    for signature, family in signatures:
        if signature in normalized:
            return family
    if "staged_prices" in normalized:
        return "apply/stage: staged-price scan or metadata"
    if normalized.startswith(("select", "with")):
        return "query/apply: other select or CTE"
    if normalized.startswith("update"):
        return "ingestion state: update"
    if normalized.startswith("insert"):
        return "ingestion state: insert"
    if normalized.startswith(("delete", "truncate")):
        return "cleanup"
    return "other"


def _require_disk_headroom() -> None:
    free_bytes = shutil.disk_usage(Path(__file__).anchor).free
    if free_bytes < _MINIMUM_DISK_HEADROOM_BYTES:
        raise RuntimeError(
            f"Benchmark stopped before exhausting the host filesystem: {free_bytes} bytes remain"
        )
