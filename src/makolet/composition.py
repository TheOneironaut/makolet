"""Production dependency composition shared by CLI, API, MCP, and workers."""

from __future__ import annotations

import asyncio
import os
import re
import sys
from collections.abc import AsyncIterator, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from http.cookiejar import Cookie, CookieJar, DefaultCookiePolicy
from pathlib import Path
from typing import Protocol
from urllib.request import Request
from uuid import UUID

import httpx
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from fastapi import FastAPI
from sqlalchemy import text

from makolet.adapters.archive.local import LocalContentAddressedArchive
from makolet.adapters.archive.s3 import (
    S3ContentAddressedArchive,
    S3UploadProcessConfig,
)
from makolet.adapters.download import FtpDownloader, HttpDownloader, ProtocolDownloader
from makolet.adapters.export import PostgresParquetExportOperations
from makolet.adapters.observability import (
    PrometheusMetrics,
    PrometheusWorkerTelemetry,
    configure_logging,
    get_lifecycle_logger,
)
from makolet.adapters.parsers import RetailXmlParser, XmlParserLimits
from makolet.adapters.persistence import (
    Database,
    PostgresArchiveMaintenanceRepository,
    PostgresCatalogMatchingRepository,
    PostgresCollectionLeaseManager,
    PostgresCollectionRepository,
    PostgresIngestionRepository,
    PostgresLeaseManager,
    PostgresOperationalRepository,
    PostgresQueryRepository,
    PostgresRegistryRepository,
)
from makolet.adapters.sources.disabled import DisabledSourceConfig
from makolet.adapters.sources.http import SafeHttpListingClient
from makolet.adapters.sources.ncr import EnvironmentCredentialProvider, StdlibFtpCatalogClient
from makolet.adapters.sources.registry import RetailerSourceDefinition, SourceRegistry
from makolet.application.catalog_matching import CandidateStatus, CatalogMatchingService
from makolet.application.collection import CollectionOperations, CollectionPolicy
from makolet.application.ingestion import IngestionPolicy, IngestionService
from makolet.application.maintenance import ArchiveMaintenanceService
from makolet.application.models import (
    CatalogCandidateGenerationResult,
    DiscoveryCursor,
    NormalizedRebuildRun,
    Page,
    ReplayRangeResult,
)
from makolet.application.ports import (
    MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
    MAXIMUM_HTTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
    MAXIMUM_TRANSFER_CHUNK_BYTES,
    SourceAdapter,
)
from makolet.application.queries import QueryService
from makolet.application.worker import Worker, WorkerPolicy
from makolet.config import ConfigurationError, MakoletSettings
from makolet.domain.errors import NotFoundError
from makolet.interfaces.api import create_app, create_metrics_app
from makolet.interfaces.mcp import MakoletMcpServer

_REVISION = re.compile(r"(?:head|base|[A-Za-z0-9_-]{1,64})\Z")
_MIGRATION_TIMEOUT_SECONDS = 10 * 60
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_REPOSITORY_ALEMBIC_CONFIGURATION = _REPOSITORY_ROOT / "alembic.ini"
_REPOSITORY_MIGRATIONS = _REPOSITORY_ROOT / "migrations"
_PACKAGE_ROOT = Path(__file__).resolve().parent
_PACKAGED_ALEMBIC_CONFIGURATION = _PACKAGE_ROOT / "_alembic.ini"
_PACKAGED_MIGRATIONS = _PACKAGE_ROOT / "_migrations"
type RuntimeArchive = LocalContentAddressedArchive | S3ContentAddressedArchive


class _RejectAllCookiePolicy(DefaultCookiePolicy):
    def set_ok(self, _cookie: Cookie, _request: Request) -> bool:
        return False

    def return_ok(self, _cookie: Cookie, _request: Request) -> bool:
        return False

    def domain_return_ok(self, _domain: str, _request: Request) -> bool:
        return False

    def path_return_ok(self, _path: str, _request: Request) -> bool:
        return False


def _publisher_cookie_jar() -> CookieJar:
    return CookieJar(policy=_RejectAllCookiePolicy())


class CapabilityUnavailableError(RuntimeError):
    code = "capability_unavailable"

    def __init__(self, capability: str) -> None:
        super().__init__(f"{capability} is not wired into this runtime")


class RuntimeOperationError(RuntimeError):
    code = "runtime_operation_failed"


class DatabaseOperations(Protocol):
    async def migrate(self, *, revision: str = "head") -> dict[str, object]: ...

    async def status(self) -> dict[str, object]: ...


class SourceOperations(Protocol):
    async def list_sources(self) -> tuple[dict[str, object], ...]: ...

    async def inspect_source(self, source_id: str) -> dict[str, object]: ...

    async def test_source(self, source_id: str) -> dict[str, object]: ...


class IngestionOperations(Protocol):
    async def ingest_source(self, source_id: str) -> dict[str, object]: ...

    async def ingest_retailer(self, retailer_id: str) -> dict[str, object]: ...

    async def ingest_all(self) -> dict[str, object]: ...

    async def backfill(
        self,
        source_id: str,
        *,
        since: datetime,
        until: datetime,
        archive_only: bool = False,
    ) -> dict[str, object]: ...

    async def replay(self, source_file_id: UUID) -> dict[str, object]: ...

    async def replay_range(
        self,
        *,
        since: datetime,
        until: datetime,
        limit: int,
        cursor: str | None,
    ) -> ReplayRangeResult: ...

    async def rebuild_normalized(
        self,
        *,
        confirmation: str,
        requested_by: str,
    ) -> NormalizedRebuildRun: ...

    async def resume_normalized_rebuild(
        self,
        rebuild_run_id: UUID,
    ) -> NormalizedRebuildRun: ...

    async def normalized_rebuild_status(
        self,
        rebuild_run_id: UUID,
    ) -> NormalizedRebuildRun: ...


class OperationalOperations(Protocol):
    async def failures(self, *, limit: int, cursor: str | None) -> Page: ...

    async def list_quarantine(self, *, limit: int, cursor: str | None) -> Page: ...

    async def inspect_quarantine(self, quarantine_id: UUID) -> dict[str, object]: ...


class MatchingOperations(Protocol):
    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]: ...

    async def generate_candidates(
        self,
        *,
        cursor: str | None = None,
        item_limit: int | None = None,
        candidate_limit: int | None = None,
        review_threshold: Decimal | str = Decimal("0.65"),
    ) -> CatalogCandidateGenerationResult: ...

    async def list_candidates(
        self,
        *,
        status: CandidateStatus | str = CandidateStatus.PENDING,
        retailer_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page: ...

    async def inspect_candidate(self, candidate_id: UUID) -> dict[str, object]: ...

    async def accept_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]: ...

    async def reject_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]: ...


class ExportOperations(Protocol):
    async def export_parquet(
        self,
        output: Path,
        *,
        since: datetime | None,
        until: datetime | None,
    ) -> dict[str, object]: ...


class DiagnosticOperations(Protocol):
    async def doctor(self) -> dict[str, object]: ...


class _CollectionWorkerBackend:
    def __init__(
        self,
        collection: CollectionOperations,
        operations: PostgresOperationalRepository,
    ) -> None:
        self._collection = collection
        self._operations = operations

    async def recover_stale_jobs(self, *, stale_after: timedelta) -> int:
        return await self._operations.recover_stale_jobs(stale_after=stale_after)

    async def ingest_source(self, source_id: str) -> object:
        return await self._collection.ingest_source(source_id)


class _RuntimeIngestionOperations:
    def __init__(
        self,
        collection: CollectionOperations,
        maintenance: ArchiveMaintenanceService,
    ) -> None:
        self._collection = collection
        self._maintenance = maintenance

    async def ingest_source(self, source_id: str) -> dict[str, object]:
        return await self._collection.ingest_source(source_id)

    async def ingest_retailer(self, retailer_id: str) -> dict[str, object]:
        return await self._collection.ingest_retailer(retailer_id)

    async def ingest_all(self) -> dict[str, object]:
        return await self._collection.ingest_all()

    async def backfill(
        self,
        source_id: str,
        *,
        since: datetime,
        until: datetime,
        archive_only: bool = False,
    ) -> dict[str, object]:
        if not archive_only:
            return await self._collection.backfill(source_id, since=since, until=until)
        return await self._collection.backfill(
            source_id,
            since=since,
            until=until,
            archive_only=archive_only,
        )

    async def replay(self, source_file_id: UUID) -> dict[str, object]:
        return await self._collection.replay(source_file_id)

    async def replay_range(
        self,
        *,
        since: datetime,
        until: datetime,
        limit: int,
        cursor: str | None,
    ) -> ReplayRangeResult:
        return await self._maintenance.replay_range(
            since=since,
            until=until,
            limit=limit,
            cursor=cursor,
        )

    async def rebuild_normalized(
        self,
        *,
        confirmation: str,
        requested_by: str,
    ) -> NormalizedRebuildRun:
        return await self._maintenance.start_rebuild(
            confirmation=confirmation,
            requested_by=requested_by,
        )

    async def resume_normalized_rebuild(
        self,
        rebuild_run_id: UUID,
    ) -> NormalizedRebuildRun:
        return await self._maintenance.resume_rebuild(rebuild_run_id)

    async def normalized_rebuild_status(
        self,
        rebuild_run_id: UUID,
    ) -> NormalizedRebuildRun:
        return await self._maintenance.rebuild_status(rebuild_run_id)


@dataclass(frozen=True, slots=True)
class RuntimeExtensions:
    """Optional adapters assembled by source/export operational modules."""

    sources: SourceOperations | None = None
    ingestion: IngestionOperations | None = None
    operations: OperationalOperations | None = None
    worker: Worker | None = None
    exporter: ExportOperations | None = None


@dataclass(frozen=True, slots=True)
class RuntimeServices:
    settings: MakoletSettings
    query_service: QueryService
    database_operations: DatabaseOperations
    diagnostics: DiagnosticOperations
    api_app: FastAPI
    metrics_app: FastAPI
    mcp_server: MakoletMcpServer
    source_operations: SourceOperations | None
    ingestion_operations: IngestionOperations | None
    operational_operations: OperationalOperations | None
    matching_operations: MatchingOperations
    worker: Worker | None
    exporter: ExportOperations | None


@asynccontextmanager
async def open_runtime(
    settings: MakoletSettings,
    *,
    extensions: RuntimeExtensions | None = None,
) -> AsyncIterator[RuntimeServices]:
    """Open one process-wide database engine and its shared application services."""

    async with AsyncExitStack() as resources:
        yield _assemble_runtime(settings, extensions=extensions, resources=resources)


def _assemble_runtime(
    settings: MakoletSettings,
    *,
    extensions: RuntimeExtensions | None,
    resources: AsyncExitStack,
) -> RuntimeServices:
    configure_logging(level=settings.log_level)
    lifecycle_events = get_lifecycle_logger()
    clock = _SystemClock()
    database = Database.from_url(
        settings.database_dsn(),
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        statement_timeout_ms=settings.database_statement_timeout_ms,
    )
    resources.push_async_callback(database.dispose)
    archive = _create_archive(settings)
    if isinstance(archive, S3ContentAddressedArchive):
        resources.push_async_callback(archive.close)
    source_http_client = httpx.AsyncClient(
        cookies=_publisher_cookie_jar(),
        follow_redirects=False,
        trust_env=False,
        timeout=httpx.Timeout(settings.source_listing_timeout_seconds),
        limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
    )
    resources.push_async_callback(source_http_client.aclose)
    credentials = EnvironmentCredentialProvider()
    source_registry = SourceRegistry(
        SafeHttpListingClient(source_http_client),
        StdlibFtpCatalogClient(
            credentials,
            allow_insecure_ftp=settings.allow_insecure_ftp,
        ),
        clock,
    )
    ingestion_repository = PostgresIngestionRepository(
        database.engine,
        maximum_validation_issues=settings.ingestion_maximum_validation_issues,
        maximum_validation_issue_bytes=(settings.ingestion_maximum_validation_issue_bytes),
        maximum_validation_issue_evidence=(settings.ingestion_maximum_validation_issue_evidence),
    )
    ingestion_leases = PostgresLeaseManager(database)
    archive_maintenance_repository = PostgresArchiveMaintenanceRepository(database.engine)
    operational_repository = PostgresOperationalRepository(
        database.engine,
        ingestion_repository,
        ingestion_leases,
    )
    matching = CatalogMatchingService(PostgresCatalogMatchingRepository(database.engine))
    metrics = PrometheusMetrics()
    downloader = ProtocolDownloader(
        HttpDownloader(
            source_http_client,
            clock,
            source_registry.http_download_policies(
                maximum_response_bytes=settings.archive_maximum_object_bytes
            ),
            maximum_download_seconds=settings.source_download_timeout_seconds,
        ),
        FtpDownloader(
            source_registry.ftp_feeds(),
            credentials,
            clock,
            maximum_response_bytes=settings.archive_maximum_object_bytes,
            maximum_download_seconds=settings.source_download_timeout_seconds,
            temporary_directory=settings.archive_root,
            minimum_free_bytes=settings.archive_minimum_free_bytes,
            allow_insecure_ftp=settings.allow_insecure_ftp,
        ),
    )
    ingestion_service = IngestionService(
        ingestion_repository,
        downloader,
        archive,
        RetailXmlParser(
            XmlParserLimits(
                temporary_directory=settings.archive_root / ".parser-spool",
                minimum_free_bytes=settings.archive_minimum_free_bytes,
            )
        ),
        ingestion_leases,
        clock,
        metrics,
        worker_id=settings.worker_id,
        policy=IngestionPolicy(
            minimum_full_records=settings.ingestion_minimum_full_records,
            minimum_full_store_records=settings.ingestion_minimum_full_store_records,
            minimum_full_price_records=settings.ingestion_minimum_full_price_records,
            minimum_full_promotion_records=(settings.ingestion_minimum_full_promotion_records),
            maximum_full_snapshot_drop_fraction=(
                settings.ingestion_maximum_full_snapshot_drop_fraction
            ),
            maximum_record_rejection_fraction=(
                settings.ingestion_maximum_record_rejection_fraction
            ),
            maximum_validation_issues=settings.ingestion_maximum_validation_issues,
            maximum_validation_issue_bytes=(settings.ingestion_maximum_validation_issue_bytes),
        ),
        events=lifecycle_events,
    )
    available_source_ids = tuple(
        definition.retailer_id
        for definition in source_registry.definitions
        if not isinstance(definition.config, DisabledSourceConfig)
    )
    configured_source_ids = settings.configured_source_ids()
    if configured_source_ids and not set(configured_source_ids).issubset(available_source_ids):
        raise ConfigurationError(
            "Configured worker sources include an unknown or externally disabled source"
        )
    collection = CollectionOperations(
        source_registry.create,
        available_source_ids,
        ingestion_service,
        batch_source_ids=configured_source_ids or available_source_ids,
        policy=CollectionPolicy(
            discovery_page_size=settings.ingestion_discovery_page_size,
            maximum_files_per_source_run=settings.ingestion_maximum_files_per_source_run,
            maximum_archive_object_bytes=settings.archive_maximum_object_bytes,
            maximum_transfer_chunk_bytes=MAXIMUM_TRANSFER_CHUNK_BYTES,
            maximum_transfer_protocol_overhead_bytes=(MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES),
            maximum_http_transfer_protocol_overhead_bytes=(
                MAXIMUM_HTTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES
            ),
            maximum_charged_bytes_per_source_run=(
                settings.ingestion_maximum_charged_bytes_per_source_run
            ),
            maximum_charged_bytes_per_source_day=(
                settings.ingestion_maximum_charged_bytes_per_source_day
            ),
            maximum_source_identities_per_source_day=(
                settings.ingestion_maximum_source_identities_per_source_day
            ),
            maximum_transfer_attempts_per_source_day=(
                settings.ingestion_maximum_transfer_attempts_per_source_day
            ),
            maximum_successes_per_source_day=(settings.ingestion_maximum_successes_per_source_day),
            maximum_reported_files=min(
                100,
                settings.ingestion_maximum_files_per_source_run,
            ),
        ),
        metrics=metrics,
        clock=clock,
        catalog_bootstrap=matching,
        events=lifecycle_events,
        repository=PostgresCollectionRepository(database.engine),
        leases=PostgresCollectionLeaseManager(database),
        worker_id=settings.worker_id,
        source_portal_ids={
            source_id: tuple(
                registration.source_key
                for registration in source_registry.portal_registrations()
                if registration.retailer_source_key == source_id
            )
            for source_id in available_source_ids
        },
    )
    archive_maintenance = ArchiveMaintenanceService(
        archive_maintenance_repository,
        archive_maintenance_repository,
        ingestion_service,
        events=lifecycle_events,
        catalog_bootstrap=matching,
    )
    ingestion_operations = _RuntimeIngestionOperations(collection, archive_maintenance)
    worker = Worker(
        _CollectionWorkerBackend(collection, operational_repository),
        PrometheusWorkerTelemetry(metrics),
        worker_id=settings.worker_id,
        policy=WorkerPolicy(
            concurrency=settings.worker_concurrency,
            queue_capacity=settings.worker_queue_capacity,
            maximum_sources=settings.worker_maximum_sources,
            heartbeat_interval=timedelta(seconds=settings.worker_heartbeat_seconds),
            scheduler_resolution=timedelta(seconds=settings.worker_poll_seconds),
            stale_after=timedelta(seconds=settings.worker_stale_after_seconds),
            stale_recovery_interval=timedelta(seconds=settings.worker_stale_recovery_seconds),
            shutdown_grace=timedelta(seconds=settings.worker_shutdown_grace_seconds),
        ),
        events=lifecycle_events,
    )
    production_extensions = RuntimeExtensions(
        ingestion=ingestion_operations,
        operations=operational_repository,
        worker=worker,
        exporter=PostgresParquetExportOperations(
            database.engine,
            spool_directory=settings.export_root / ".spool",
        ),
    )
    selected_extensions = production_extensions if extensions is None else extensions
    source_operations = selected_extensions.sources or _RegistrySourceOperations(source_registry)
    queries = QueryService(PostgresQueryRepository(database.engine), clock)
    database_operations = _PostgresDatabaseOperations(
        database,
        settings.database_dsn(),
        PostgresRegistryRepository(database.engine),
        source_registry,
    )
    diagnostics = _RuntimeDiagnostics(
        database,
        archive,
        capabilities=_capabilities(selected_extensions, sources_available=True),
    )

    async def ready() -> bool:
        return await _database_is_ready(database_operations)

    return RuntimeServices(
        settings=settings,
        query_service=queries,
        database_operations=database_operations,
        diagnostics=diagnostics,
        api_app=create_app(
            queries,
            ready,
            metrics_registry=metrics.registry,
            maximum_concurrency=settings.api_http_maximum_concurrency,
        ),
        metrics_app=create_metrics_app(metrics.registry),
        mcp_server=MakoletMcpServer(queries),
        source_operations=source_operations,
        ingestion_operations=selected_extensions.ingestion,
        operational_operations=selected_extensions.operations,
        matching_operations=matching,
        worker=selected_extensions.worker,
        exporter=selected_extensions.exporter,
    )


class _SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


def _create_archive(settings: MakoletSettings) -> RuntimeArchive:
    if settings.archive_backend == "local":
        return LocalContentAddressedArchive(
            settings.archive_root,
            maximum_object_bytes=settings.archive_maximum_object_bytes,
            minimum_free_bytes=settings.archive_minimum_free_bytes,
        )
    access_key, secret_key = settings.s3_credentials()
    return S3ContentAddressedArchive(
        None,
        settings.s3_bucket,
        key_prefix=settings.s3_key_prefix,
        maximum_object_bytes=settings.archive_maximum_object_bytes,
        minimum_free_bytes=settings.archive_minimum_free_bytes,
        temporary_directory=settings.archive_root,
        upload_process_config=S3UploadProcessConfig(
            endpoint_url=settings.s3_endpoint,
            region_name=settings.s3_region,
            access_key_id=access_key,
            secret_access_key=secret_key,
            path_style=settings.s3_path_style,
            direct_connection=settings.s3_direct_connection_required(),
        ),
    )


class _RegistrySourceOperations:
    def __init__(self, registry: SourceRegistry) -> None:
        self._registry = registry

    async def list_sources(self) -> tuple[dict[str, object], ...]:
        return tuple(_source_summary(definition) for definition in self._registry.definitions)

    async def inspect_source(self, source_id: str) -> dict[str, object]:
        return _source_details(self._definition(source_id))

    async def test_source(self, source_id: str) -> dict[str, object]:
        adapter = self._adapter(source_id)
        page = await adapter.discover(DiscoveryCursor(), limit=1)
        return {
            "source_id": source_id,
            "status": "reachable",
            "files": tuple(
                {
                    "remote_id": remote_file.remote_id,
                    "filename": remote_file.original_filename,
                    "document_type": remote_file.document_type.value,
                    "compression": remote_file.compression.value,
                    "source_timestamp": remote_file.source_timestamp,
                    "content_length": remote_file.content_length,
                }
                for remote_file in page.files
            ),
            "listing_complete": page.complete,
            "has_more": page.next_cursor is not None,
        }

    def _definition(self, source_id: str) -> RetailerSourceDefinition:
        try:
            return self._registry.definition(source_id)
        except KeyError as error:
            raise NotFoundError("Source was not found") from error

    def _adapter(self, source_id: str) -> SourceAdapter:
        self._definition(source_id)
        return self._registry.create(source_id)


class _PostgresDatabaseOperations:
    def __init__(
        self,
        database: Database,
        database_url: str,
        registry_repository: PostgresRegistryRepository,
        source_registry: SourceRegistry,
        *,
        expected_migration_revisions: Sequence[str] | None = None,
    ) -> None:
        self._database = database
        self._database_url = database_url
        self._registry_repository = registry_repository
        self._source_registry = source_registry
        selected_revisions = (
            _expected_alembic_heads()
            if expected_migration_revisions is None
            else tuple(expected_migration_revisions)
        )
        if not selected_revisions:
            raise RuntimeOperationError("The Alembic migration graph has no head revision")
        self._expected_migration_revisions = tuple(sorted(selected_revisions))

    async def migrate(self, *, revision: str = "head") -> dict[str, object]:
        if _REVISION.fullmatch(revision) is None:
            raise ValueError("migration revision is invalid")
        await _run_alembic(self._database_url, "upgrade", revision)
        registry = await self._registry_repository.synchronize(
            self._source_registry.retailer_registrations(),
            self._source_registry.portal_registrations(),
        )
        return {**await self.status(), "registry": registry}

    async def status(self) -> dict[str, object]:
        health = await self._database.health()
        current_migration_revisions: tuple[str, ...] = ()
        async with self._database.engine.connect() as connection:
            version_table = (
                await connection.execute(text("SELECT to_regclass('public.alembic_version')"))
            ).scalar_one_or_none()
            if version_table is not None:
                current_migration_revisions = tuple(
                    sorted(
                        str(revision)
                        for revision in (
                            await connection.execute(
                                text("SELECT version_num FROM alembic_version ORDER BY version_num")
                            )
                        ).scalars()
                    )
                )
        schema_ready = current_migration_revisions == self._expected_migration_revisions
        return {
            "status": "ready" if schema_ready else "not_ready",
            "postgresql_version_number": health.server_version_number,
            "postgresql_version": health.server_version,
            "schema_ready": schema_ready,
            "expected_migration_heads": self._expected_migration_revisions,
            "current_migration_revisions": current_migration_revisions,
            "migration_revision": (
                current_migration_revisions[0] if len(current_migration_revisions) == 1 else None
            ),
        }


def _expected_alembic_heads() -> tuple[str, ...]:
    configuration = AlembicConfig(str(_alembic_configuration_path()))
    return tuple(sorted(ScriptDirectory.from_config(configuration).get_heads()))


def _alembic_configuration_path() -> Path:
    candidates = (
        (_REPOSITORY_ALEMBIC_CONFIGURATION, _REPOSITORY_MIGRATIONS),
        (_PACKAGED_ALEMBIC_CONFIGURATION, _PACKAGED_MIGRATIONS),
    )
    for configuration, migrations in candidates:
        if configuration.is_file() and migrations.is_dir():
            return configuration
    raise RuntimeOperationError("Alembic configuration and migrations are missing")


async def _database_is_ready(database_operations: DatabaseOperations) -> bool:
    try:
        status = await database_operations.status()
    except Exception:
        return False
    return status.get("schema_ready") is True


class _RuntimeDiagnostics:
    def __init__(
        self,
        database: Database,
        archive: RuntimeArchive,
        *,
        capabilities: Sequence[tuple[str, bool]],
    ) -> None:
        self._database = database
        self._archive = archive
        self._capabilities = tuple(capabilities)

    async def doctor(self) -> dict[str, object]:
        checks: list[dict[str, object]] = []
        try:
            health = await self._database.health()
            checks.append(
                {
                    "name": "database",
                    "ok": True,
                    "detail": f"PostgreSQL {health.server_version_number}",
                }
            )
        except Exception:
            checks.append({"name": "database", "ok": False, "code": "database_unavailable"})
        try:
            await self._archive.initialize()
            checks.append({"name": "archive", "ok": True})
        except Exception:
            checks.append({"name": "archive", "ok": False, "code": "archive_unavailable"})
        checks.extend(
            {
                "name": name,
                "ok": available,
                **({} if available else {"code": "capability_unavailable"}),
            }
            for name, available in self._capabilities
        )
        return {"ok": all(bool(check["ok"]) for check in checks), "checks": checks}


def _source_summary(definition: RetailerSourceDefinition) -> dict[str, object]:
    disabled = definition.config if isinstance(definition.config, DisabledSourceConfig) else None
    return {
        "position": definition.position,
        "source_id": definition.retailer_id,
        "display_name": definition.display_name,
        "family": definition.family,
        "status": disabled.status.value if disabled is not None else "configured",
    }


def _source_details(definition: RetailerSourceDefinition) -> dict[str, object]:
    details = {
        **_source_summary(definition),
        "official_entity": definition.official_entity,
        "observed_chain_ids": definition.observed_chain_ids,
    }
    if isinstance(definition.config, DisabledSourceConfig):
        details["reason"] = definition.config.reason
        details["public_lead"] = definition.config.public_lead
    return details


def _capabilities(
    extensions: RuntimeExtensions, *, sources_available: bool
) -> tuple[tuple[str, bool], ...]:
    return (
        ("sources", sources_available),
        ("ingestion", extensions.ingestion is not None),
        ("operations", extensions.operations is not None),
        ("worker", extensions.worker is not None),
        ("export", extensions.exporter is not None),
    )


async def _run_alembic(database_url: str, *arguments: str) -> None:
    configuration = await asyncio.to_thread(_alembic_configuration_path)
    environment = os.environ.copy()
    environment["MAKOLET_DATABASE_URL"] = database_url
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "-c",
        str(configuration),
        *arguments,
        cwd=configuration.parent,
        env=environment,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        async with asyncio.timeout(_MIGRATION_TIMEOUT_SECONDS):
            await process.wait()
    except TimeoutError as error:
        process.kill()
        await process.wait()
        raise RuntimeOperationError("Database migration exceeded its time limit") from error
    if process.returncode != 0:
        raise RuntimeOperationError("Database migration failed")
