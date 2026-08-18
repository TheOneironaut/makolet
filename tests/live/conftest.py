"""Safety envelope for the representative live-ingestion acceptance test."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import os
import re
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import suppress
from dataclasses import dataclass, field, replace
from functools import partial
from pathlib import Path
from typing import Any, Protocol, cast
from unittest.mock import patch
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.awsrequest import AWSResponse
from botocore.config import Config as BotoConfig
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

import makolet.adapters.download.http as download_http_module
import makolet.adapters.sources.http as source_http_module
import makolet.composition as composition_module
from makolet.adapters.archive._process import (
    ProcessDeadlineError,
    ProcessWorkerError,
    run_in_spawn_process,
)
from makolet.adapters.download.http import ResolvedHttpTarget
from makolet.adapters.persistence.database import Database
from makolet.adapters.sources.bina import BinaPartition, BinaSourceConfig
from makolet.adapters.sources.http import HttpListingClient
from makolet.adapters.sources.ncr import FtpCatalogClient
from makolet.adapters.sources.registry import RETAILER_REGISTRY, SourceRegistry
from makolet.application.models import DiscoveryCursor, DiscoveryPage, DiscoveryRunBudget
from makolet.application.ports import (
    MAXIMUM_TRANSFER_RESERVATION_HEADROOM_BYTES,
    Clock,
    SourceAdapter,
)
from makolet.composition import open_runtime
from makolet.config import MakoletSettings
from makolet.domain.enums import DocumentType, SourceProtocol
from makolet.domain.errors import SourceResponseError

LIVE_ACCEPTANCE_TOKEN = "maayan-2000-price-store-001"
LIVE_SOURCE_ID = "maayan-2000"
MAXIMUM_ARCHIVE_BYTES = 4 * 1024 * 1024
TRANSFER_HEADROOM_BYTES = MAXIMUM_TRANSFER_RESERVATION_HEADROOM_BYTES
MAXIMUM_PUBLISHER_REQUESTS = 12
_MAXIMUM_S3_LIST_RESPONSE_BYTES = 2 * 1024 * 1024
_S3_LIST_RESPONSE_CHUNK_BYTES = 64 * 1024
_S3_CLEANUP_PROCESS_TIMEOUT_SECONDS = 30.0

_DATABASE_NAME = re.compile(r"makolet_live_acceptance_test_[0-9a-f]{24}\Z")
_OPT_IN_VARIABLE = "MAKOLET_LIVE_INGESTION_ACCEPT"
_ADMIN_DATABASE_VARIABLE = "MAKOLET_LIVE_ACCEPTANCE_ADMIN_DATABASE_URL"
_S3_ENDPOINT_VARIABLE = "MAKOLET_LIVE_ACCEPTANCE_S3_ENDPOINT"
_S3_BUCKET_VARIABLE = "MAKOLET_LIVE_ACCEPTANCE_S3_BUCKET"
_S3_REGION_VARIABLE = "MAKOLET_LIVE_ACCEPTANCE_S3_REGION"
_S3_ACCESS_KEY_VARIABLE = "MAKOLET_LIVE_ACCEPTANCE_S3_ACCESS_KEY"
_S3_SECRET_KEY_VARIABLE = "MAKOLET_LIVE_ACCEPTANCE_S3_SECRET_KEY"


class _PublisherSender(Protocol):
    async def __call__(
        self,
        client: httpx.AsyncClient,
        target: ResolvedHttpTarget,
        *,
        headers: Mapping[str, str],
    ) -> httpx.Response: ...


class _S3CleanupClient(Protocol):
    def list_objects_v2(self, **kwargs: object) -> Mapping[str, object]: ...

    def delete_objects(self, **kwargs: object) -> Mapping[str, object]: ...

    def get_object(self, **kwargs: object) -> Mapping[str, object]: ...

    def close(self) -> None: ...


class _S3StreamingBody(Protocol):
    def read(self, amount: int | None = None) -> bytes: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class LiveServiceConfiguration:
    admin_database_url: str = field(repr=False)
    s3_endpoint: str
    s3_bucket: str
    s3_region: str
    s3_access_key: str = field(repr=False)
    s3_secret_key: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class LiveWorkflow:
    settings: MakoletSettings = field(repr=False)
    source_file_id: UUID
    product_id: UUID
    content_sha256: str
    archive_object_key: str
    publisher_request_count: int
    price_record_count: int
    replay_history_events: int
    archived_byte_count: int = 0


@dataclass(frozen=True, slots=True)
class _S3CleanupLimits:
    page_size: int = 1_000
    maximum_pages: int = 16
    maximum_keys: int = 16_000
    maximum_key_bytes: int = 1_024
    maximum_no_progress_pages: int = 1
    maximum_token_bytes: int = 4_096
    maximum_requests: int = 64
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if (
            self.page_size < 1
            or self.maximum_pages < 1
            or self.maximum_keys < 1
            or self.maximum_key_bytes < 1
            or self.maximum_no_progress_pages < 0
            or self.maximum_token_bytes < 1
            or self.maximum_requests < 1
            or self.timeout_seconds <= 0
        ):
            raise ValueError("S3 cleanup limits must be finite and positive")


_S3_CLEANUP_LIMITS = _S3CleanupLimits()


class _S3CleanupBudget:
    def __init__(self, limits: _S3CleanupLimits) -> None:
        self.limits = limits
        self._deadline = time.monotonic() + limits.timeout_seconds
        self._request_count = 0

    def before_request(self) -> None:
        self.checkpoint()
        if self._request_count >= self.limits.maximum_requests:
            raise AssertionError("S3 cleanup exceeded its request limit")
        self._request_count += 1

    def after_request(self) -> None:
        self.checkpoint()

    def checkpoint(self) -> None:
        if time.monotonic() >= self._deadline:
            raise AssertionError("S3 cleanup exceeded its monotonic deadline")


class _BoundedPublisherRequests:
    def __init__(self, sender: _PublisherSender) -> None:
        self._sender = sender
        self.count = 0

    async def __call__(
        self,
        client: httpx.AsyncClient,
        target: ResolvedHttpTarget,
        *,
        headers: Mapping[str, str],
    ) -> httpx.Response:
        self.count += 1
        if self.count > MAXIMUM_PUBLISHER_REQUESTS:
            raise AssertionError("representative live ingestion exceeded its request budget")
        return await self._sender(client, target, headers=headers)


async def _publisher_network_forbidden(
    client: httpx.AsyncClient,
    target: ResolvedHttpTarget,
    *,
    headers: Mapping[str, str],
) -> httpx.Response:
    del client, target, headers
    raise AssertionError("archive replay attempted publisher network access")


class _OneFileSource:
    """Terminate discovery after one production-adapter result."""

    source_id = LIVE_SOURCE_ID

    def __init__(self, delegate: SourceAdapter) -> None:
        self._delegate = delegate

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: DiscoveryRunBudget | None = None,
    ) -> DiscoveryPage:
        if cursor is not None:
            raise AssertionError("representative discovery unexpectedly requested a second page")
        page = await self._delegate.discover(None, limit=min(limit, 1), budget=budget)
        if len(page.files) != 1:
            raise SourceResponseError(
                "Representative BINA partition did not return exactly one selectable file"
            )
        remote_file = page.files[0]
        parsed_url = urlsplit(remote_file.download_url)
        if (
            remote_file.retailer_id != LIVE_SOURCE_ID
            or remote_file.document_type is not DocumentType.PRICE_DELTA
            or remote_file.protocol is not SourceProtocol.HTTPS
            or parsed_url.scheme != "https"
            or parsed_url.username is not None
            or parsed_url.password is not None
        ):
            raise SourceResponseError(
                "Representative discovery returned a file outside its fixed HTTPS scope"
            )
        if (
            remote_file.content_length is not None
            and remote_file.content_length > MAXIMUM_ARCHIVE_BYTES
        ):
            raise SourceResponseError("Representative source file exceeds its byte budget")
        return DiscoveryPage(files=(remote_file,), next_cursor=None, complete=True)


class RepresentativeSourceRegistry(SourceRegistry):
    """Use the registered Maayan portal with one evidence-backed price partition."""

    def __init__(
        self,
        http: HttpListingClient,
        ftp: FtpCatalogClient,
        clock: Clock,
    ) -> None:
        selected_definitions = []
        for definition in RETAILER_REGISTRY:
            if definition.retailer_id != LIVE_SOURCE_ID:
                selected_definitions.append(definition)
                continue
            if not isinstance(definition.config, BinaSourceConfig):
                raise TypeError("Maayan 2000 is no longer configured as a BINA source")
            selected_definitions.append(
                replace(
                    definition,
                    config=replace(
                        definition.config,
                        partitions=(BinaPartition(file_type="2", store_id="001"),),
                    ),
                )
            )
        super().__init__(http, ftp, clock, definitions=tuple(selected_definitions))

    def create(self, retailer_id: str) -> SourceAdapter:
        adapter = super().create(retailer_id)
        return _OneFileSource(adapter) if retailer_id == LIVE_SOURCE_ID else adapter


@pytest.fixture(scope="module")
def representative_live_workflow(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[LiveWorkflow]:
    """Create isolated services, run once, and attempt both cleanup paths unconditionally."""

    opt_in = os.environ.get(_OPT_IN_VARIABLE)
    if opt_in is None:
        pytest.skip(f"set {_OPT_IN_VARIABLE}={LIVE_ACCEPTANCE_TOKEN} to opt into live ingestion")
    if opt_in != LIVE_ACCEPTANCE_TOKEN:
        pytest.fail(f"{_OPT_IN_VARIABLE} must equal the documented fixed acceptance token")

    service = _live_service_configuration()
    run_id = uuid4().hex
    database_name = f"makolet_live_acceptance_test_{run_id[:24]}"
    if _DATABASE_NAME.fullmatch(database_name) is None:
        raise AssertionError("generated live-acceptance database name is unsafe")
    database_url = _database_url(service.admin_database_url, database_name)
    key_prefix = f"live-acceptance/{run_id}"
    spool_root = tmp_path_factory.mktemp("representative-live-spool")
    settings = _runtime_settings(
        service,
        database_url=database_url,
        key_prefix=key_prefix,
        spool_root=spool_root,
        run_id=run_id,
    )

    database_created = False
    prefix_owned = False
    try:
        try:
            asyncio.run(_create_database(service.admin_database_url, database_name))
        except Exception as error:
            raise RuntimeError(
                "database creation was not confirmed; inspect generated database "
                f"{database_name} before retrying"
            ) from error
        database_created = True
        try:
            _assert_s3_prefix_empty(service, key_prefix)
        except Exception as error:
            raise RuntimeError(
                "S3 prefix ownership was not confirmed; inspect generated prefix "
                f"{key_prefix} before retrying"
            ) from error
        prefix_owned = True
        workflow = asyncio.run(_run_representative_ingestion(settings))
        expected_service_key = f"{key_prefix}/{workflow.archive_object_key}"
        if _run_s3_operation(
            _s3_keys,
            service,
            key_prefix,
            timeout_seconds=_S3_CLEANUP_PROCESS_TIMEOUT_SECONDS,
        ) != (expected_service_key,):
            raise AssertionError("live ingestion did not create exactly one scoped archive object")
        archived_byte_count, archived_digest = _run_s3_operation(
            _s3_object_evidence,
            service,
            expected_service_key,
            timeout_seconds=_S3_CLEANUP_PROCESS_TIMEOUT_SECONDS,
        )
        if archived_digest != workflow.content_sha256:
            raise AssertionError("S3 archive bytes differ from ingestion and database provenance")
        workflow = replace(workflow, archived_byte_count=archived_byte_count)
        yield workflow
    finally:
        _cleanup_live_resources(
            service,
            key_prefix=key_prefix,
            prefix_owned=prefix_owned,
            database_name=database_name,
            database_created=database_created,
        )


def _cleanup_live_resources(
    service: LiveServiceConfiguration,
    *,
    key_prefix: str,
    prefix_owned: bool,
    database_name: str,
    database_created: bool,
    s3_cleanup_target: Callable[[LiveServiceConfiguration, str], None] | None = None,
    s3_cleanup_timeout_seconds: float = _S3_CLEANUP_PROCESS_TIMEOUT_SECONDS,
) -> None:
    cleanup_errors: list[Exception] = []
    if prefix_owned:
        try:
            target = _purge_s3_prefix if s3_cleanup_target is None else s3_cleanup_target
            _run_s3_operation(
                target,
                service,
                key_prefix,
                timeout_seconds=s3_cleanup_timeout_seconds,
            )
        except Exception as error:
            cleanup_errors.append(error)
    if database_created:
        try:
            asyncio.run(_drop_database(service.admin_database_url, database_name))
        except Exception as error:
            cleanup_errors.append(error)
    if cleanup_errors:
        raise ExceptionGroup("representative live-ingestion cleanup failed", cleanup_errors)


def _run_s3_operation[T](
    target: Callable[..., T],
    *args: object,
    timeout_seconds: float,
) -> T:
    return asyncio.run(
        _run_s3_operation_in_child(
            target,
            *args,
            timeout_seconds=timeout_seconds,
        )
    )


async def _run_s3_operation_in_child[T](
    target: Callable[..., T],
    *args: object,
    timeout_seconds: float,
) -> T:
    try:
        return await run_in_spawn_process(
            target,
            *args,
            timeout_seconds=timeout_seconds,
        )
    except ProcessDeadlineError as error:
        raise AssertionError("S3 operation exceeded its killable process deadline") from error
    except ProcessWorkerError as error:
        raise AssertionError("S3 operation child process failed") from error


def _live_service_configuration() -> LiveServiceConfiguration:
    admin_database_url = _required_environment(_ADMIN_DATABASE_VARIABLE)
    s3_endpoint = _required_environment(_S3_ENDPOINT_VARIABLE)
    _require_loopback_database_url(admin_database_url)
    _require_loopback_url(
        s3_endpoint,
        label=_S3_ENDPOINT_VARIABLE,
        schemes=frozenset({"http", "https"}),
    )
    return LiveServiceConfiguration(
        admin_database_url=admin_database_url,
        s3_endpoint=s3_endpoint,
        s3_bucket=_required_environment(_S3_BUCKET_VARIABLE),
        s3_region=_required_environment(_S3_REGION_VARIABLE),
        s3_access_key=_required_environment(_S3_ACCESS_KEY_VARIABLE),
        s3_secret_key=_required_environment(_S3_SECRET_KEY_VARIABLE),
    )


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        pytest.fail(f"{name} is required after live-ingestion opt-in")
    return value


def _require_loopback_url(raw_url: str, *, label: str, schemes: frozenset[str]) -> None:
    parsed = urlsplit(raw_url)
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    if parsed.scheme.casefold() not in schemes or not hostname:
        pytest.fail(f"{label} must be an absolute URL using its documented scheme")
    _require_loopback_hostname(hostname, label=label)


def _require_loopback_database_url(raw_url: str) -> None:
    lexical_url = urlsplit(raw_url)
    if lexical_url.query or lexical_url.fragment:
        pytest.fail(f"{_ADMIN_DATABASE_VARIABLE} must not contain query parameters or a fragment")
    parsed = make_url(raw_url)
    if parsed.get_backend_name() != "postgresql":
        pytest.fail(f"{_ADMIN_DATABASE_VARIABLE} must use PostgreSQL")
    hostname = (parsed.host or "").casefold().rstrip(".")
    if not hostname:
        pytest.fail(f"{_ADMIN_DATABASE_VARIABLE} must identify a host")
    _require_loopback_hostname(hostname, label=_ADMIN_DATABASE_VARIABLE)


def _require_loopback_hostname(hostname: str, *, label: str) -> None:
    if hostname == "localhost":
        return
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        pytest.fail(f"{label} must target a loopback service")
    if not address.is_loopback:
        pytest.fail(f"{label} must target a loopback service")


def _database_url(admin_database_url: str, database_name: str) -> str:
    parsed = make_url(admin_database_url)
    if parsed.database is None:
        pytest.fail(f"{_ADMIN_DATABASE_VARIABLE} must identify an administrative database")
    return parsed.set(database=database_name).render_as_string(hide_password=False)


def _runtime_settings(
    service: LiveServiceConfiguration,
    *,
    database_url: str,
    key_prefix: str,
    spool_root: Path,
    run_id: str,
) -> MakoletSettings:
    charged_byte_limit = MAXIMUM_ARCHIVE_BYTES + TRANSFER_HEADROOM_BYTES
    return MakoletSettings(
        _env_file=None,
        environment="test",
        database_url=database_url,
        database_pool_size=2,
        database_max_overflow=0,
        archive_backend="s3",
        archive_root=spool_root,
        archive_maximum_object_bytes=MAXIMUM_ARCHIVE_BYTES,
        archive_minimum_free_bytes=0,
        s3_endpoint=service.s3_endpoint,
        s3_bucket=service.s3_bucket,
        s3_region=service.s3_region,
        s3_access_key=service.s3_access_key,
        s3_secret_key=service.s3_secret_key,
        s3_key_prefix=key_prefix,
        s3_path_style=True,
        export_root=spool_root / "exports",
        source_listing_timeout_seconds=20,
        source_download_timeout_seconds=60,
        ingestion_discovery_page_size=1,
        ingestion_maximum_files_per_source_run=1,
        ingestion_maximum_charged_bytes_per_source_run=charged_byte_limit,
        ingestion_maximum_charged_bytes_per_source_day=charged_byte_limit,
        source_intervals_seconds={LIVE_SOURCE_ID: 86_400},
        enabled_sources=(LIVE_SOURCE_ID,),
        worker_id=f"live-acceptance-{run_id[:16]}",
    )


async def _create_database(admin_database_url: str, database_name: str) -> None:
    engine = create_async_engine(
        _asyncpg_url(admin_database_url),
        isolation_level="AUTOCOMMIT",
        poolclass=NullPool,
        hide_parameters=True,
    )
    try:
        async with engine.connect() as connection:
            existing = (
                await connection.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :database_name"),
                    {"database_name": database_name},
                )
            ).scalar_one_or_none()
            if existing is not None:
                raise AssertionError("generated live-acceptance database already exists")
            await connection.execute(text(f'CREATE DATABASE "{database_name}" TEMPLATE template0'))
    finally:
        await engine.dispose()


async def _drop_database(admin_database_url: str, database_name: str) -> None:
    if _DATABASE_NAME.fullmatch(database_name) is None:
        raise AssertionError("refusing to drop a database outside the acceptance namespace")
    engine = create_async_engine(
        _asyncpg_url(admin_database_url),
        isolation_level="AUTOCOMMIT",
        poolclass=NullPool,
        hide_parameters=True,
    )
    try:
        async with engine.connect() as connection:
            await connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                ),
                {"database_name": database_name},
            )
            await connection.execute(text(f'DROP DATABASE IF EXISTS "{database_name}"'))
    finally:
        await engine.dispose()


def _asyncpg_url(raw_url: str) -> URL:
    _require_loopback_database_url(raw_url)
    parsed = make_url(raw_url)
    return parsed.set(drivername="postgresql+asyncpg")


async def _run_representative_ingestion(settings: MakoletSettings) -> LiveWorkflow:
    original_sender: _PublisherSender = download_http_module.send_pinned_request
    request_budget = _BoundedPublisherRequests(original_sender)
    with (
        patch.object(composition_module, "SourceRegistry", RepresentativeSourceRegistry),
        patch.object(download_http_module, "send_pinned_request", request_budget),
        patch.object(source_http_module, "send_pinned_request", request_budget),
    ):
        async with open_runtime(settings) as runtime:
            migration = await runtime.database_operations.migrate()
            if migration.get("status") != "ready":
                raise AssertionError("disposable database did not reach the migration head")
            diagnostics = await runtime.diagnostics.doctor()
            if diagnostics.get("ok") is not True:
                raise AssertionError("live-acceptance runtime did not become ready")
            ingestion = runtime.ingestion_operations
            if ingestion is None:
                raise AssertionError("production ingestion capability is unavailable")
            result = await ingestion.ingest_source(LIVE_SOURCE_ID)
            file_result = _single_file_result(result)
            source_file_id = _uuid_value(file_result.get("source_file_id"), "source_file_id")
            content_sha256 = _sha256_value(file_result.get("content_sha256"))
            stage = file_result.get("stage")
            if not isinstance(stage, Mapping):
                raise TypeError("live ingestion did not return a staging summary")
            price_record_count = stage.get("price_records")
            if not isinstance(price_record_count, int) or price_record_count <= 0:
                raise AssertionError("representative price file produced no accepted price records")

            with (
                patch.object(
                    download_http_module,
                    "send_pinned_request",
                    _publisher_network_forbidden,
                ),
                patch.object(
                    source_http_module,
                    "send_pinned_request",
                    _publisher_network_forbidden,
                ),
            ):
                replay = await ingestion.replay(source_file_id)
            if replay.get("replayed") is not True or replay.get("content_sha256") != content_sha256:
                raise AssertionError(
                    "archive replay did not preserve the immutable source identity"
                )
            replay_apply = replay.get("apply")
            if not isinstance(replay_apply, Mapping):
                raise TypeError("archive replay did not return an apply summary")
            replay_history_events = replay_apply.get("history_events")
            if not isinstance(replay_history_events, int) or replay_history_events != 0:
                raise AssertionError("unchanged archive replay created price history")

    product_id, archive_object_key, database_digest = await _source_identity(
        settings.database_dsn(),
        source_file_id,
    )
    if database_digest != content_sha256:
        raise AssertionError("database provenance digest differs from ingestion output")
    if not 2 <= request_budget.count <= MAXIMUM_PUBLISHER_REQUESTS:
        raise AssertionError("representative publisher request count is outside its fixed bounds")
    return LiveWorkflow(
        settings=settings,
        source_file_id=source_file_id,
        product_id=product_id,
        content_sha256=content_sha256,
        archive_object_key=archive_object_key,
        publisher_request_count=request_budget.count,
        price_record_count=price_record_count,
        replay_history_events=replay_history_events,
    )


def _single_file_result(result: Mapping[str, object]) -> Mapping[str, object]:
    if (
        result.get("status") != "completed"
        or result.get("file_count") != 1
        or result.get("discovered_count") != 1
        or result.get("run_truncated") is not False
    ):
        raise AssertionError("representative collection did not complete exactly one file")
    reported_files = result.get("reported_files")
    if not isinstance(reported_files, tuple) or len(reported_files) != 1:
        raise AssertionError("representative collection did not report exactly one file")
    file_result = reported_files[0]
    if not isinstance(file_result, Mapping):
        raise TypeError("representative collection returned an invalid file result")
    if file_result.get("status") != "completed" or file_result.get("duplicate") is not False:
        raise AssertionError("representative source file was not newly completed")
    return file_result


def _uuid_value(value: object, field_name: str) -> UUID:
    if isinstance(value, UUID):
        return value
    raise AssertionError(f"representative result omitted {field_name}")


def _sha256_value(value: object) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise AssertionError("representative result omitted a canonical content digest")
    return value


async def _source_identity(database_url: str, source_file_id: UUID) -> tuple[UUID, str, str]:
    database = Database.from_url(database_url, pool_size=1, max_overflow=0)
    try:
        async with database.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        text(
                            """
                        SELECT match.canonical_product_id,
                               archive.object_key,
                               archive.content_sha256
                          FROM retailer_items item
                          JOIN confirmed_product_matches match
                            ON match.retailer_item_id = item.id
                          JOIN source_files source
                            ON source.id = item.last_source_file_id
                          JOIN raw_archive_objects archive
                            ON archive.id = source.raw_archive_object_id
                         WHERE item.last_source_file_id = :source_file_id
                         ORDER BY match.canonical_product_id
                         LIMIT 1
                        """
                        ),
                        {"source_file_id": source_file_id},
                    )
                )
                .mappings()
                .all()
            )
    finally:
        await database.dispose()
    if len(rows) != 1:
        raise AssertionError("matching did not produce a queryable product for the live file")
    row = rows[0]
    return (
        UUID(str(row["canonical_product_id"])),
        str(row["object_key"]),
        _sha256_value(row["content_sha256"]),
    )


class _BoundedS3ListResponseBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def stream(
        self,
        amount: int | None = None,
        *,
        decode_content: bool = False,
    ) -> Iterator[bytes]:
        if decode_content:
            raise AssertionError("S3 cleanup listing response decoding is not permitted")
        chunk_size = (len(self._payload) or 1) if amount is None else amount
        if chunk_size <= 0:
            raise AssertionError("S3 cleanup listing requested an invalid chunk size")
        for offset in range(0, len(self._payload), chunk_size):
            yield self._payload[offset : offset + chunk_size]

    def close(self) -> None:
        return


def _s3_response_header(headers: Mapping[str, Any], name: str) -> str | None:
    sought = name.casefold()
    for raw_name, raw_value in headers.items():
        if str(raw_name).casefold() != sought:
            continue
        if isinstance(raw_value, bytes):
            return raw_value.decode("latin-1")
        return str(raw_value)
    return None


def _bounded_s3_list_response(
    http_session: Any,
    request: Any,
    *,
    maximum_bytes: int = _MAXIMUM_S3_LIST_RESPONSE_BYTES,
    **_kwargs: Any,
) -> AWSResponse:
    """Bound one signed listing response before botocore materializes its XML."""

    if maximum_bytes <= 0:
        raise AssertionError("S3 cleanup listing response byte limit must be positive")
    original_stream_output = request.stream_output
    request.stream_output = True
    response: Any | None = None
    try:
        response = http_session.send(request)
        raw = response.raw
        content_encoding = _s3_response_header(response.headers, "content-encoding")
        if content_encoding is not None and content_encoding.strip().casefold() not in {
            "",
            "identity",
        }:
            raise AssertionError("S3 cleanup listing uses unsupported content encoding")
        declared_length_text = _s3_response_header(response.headers, "content-length")
        declared_length: int | None = None
        if declared_length_text is not None:
            try:
                declared_length = int(declared_length_text)
            except ValueError as error:
                raise AssertionError("S3 cleanup listing has invalid content length") from error
            if declared_length < 0:
                raise AssertionError("S3 cleanup listing has invalid content length")
            if declared_length > maximum_bytes:
                raise AssertionError("S3 cleanup listing response exceeds its byte limit")
        payload = bytearray()
        for chunk in raw.stream(_S3_LIST_RESPONSE_CHUNK_BYTES, decode_content=False):
            if not isinstance(chunk, bytes):
                raise TypeError("S3 cleanup listing response returned non-byte content")
            if len(chunk) > maximum_bytes - len(payload):
                raise AssertionError("S3 cleanup listing response exceeds its byte limit")
            payload.extend(chunk)
        if declared_length is not None and declared_length != len(payload):
            raise AssertionError("S3 cleanup listing response length is inconsistent")
        return AWSResponse(
            response.url,
            response.status_code,
            response.headers,
            _BoundedS3ListResponseBody(bytes(payload)),
        )
    finally:
        request.stream_output = original_stream_output
        if response is not None:
            with suppress(Exception):
                response.raw.close()


def _install_bounded_s3_listing_transport(client: Any) -> Any:
    endpoint = getattr(client, "_endpoint", None)
    http_session = getattr(endpoint, "http_session", None)
    events = getattr(getattr(client, "meta", None), "events", None)
    register_first = getattr(events, "register_first", None)
    if http_session is None or not callable(register_first):
        raise AssertionError("S3 cleanup client cannot enforce bounded listing responses")
    register_first(
        "before-send.s3.ListObjectsV2",
        partial(_bounded_s3_list_response, http_session),
    )
    return client


def _s3_client(service: LiveServiceConfiguration) -> _S3CleanupClient:
    _require_loopback_url(
        service.s3_endpoint,
        label=_S3_ENDPOINT_VARIABLE,
        schemes=frozenset({"http", "https"}),
    )
    direct_connection = urlsplit(service.s3_endpoint).scheme.casefold() == "http"
    client = boto3.client(
        "s3",
        endpoint_url=service.s3_endpoint,
        region_name=service.s3_region,
        aws_access_key_id=service.s3_access_key,
        aws_secret_access_key=service.s3_secret_key,
        config=BotoConfig(
            connect_timeout=5,
            read_timeout=5,
            signature_version="s3v4",
            retries={"mode": "standard", "max_attempts": 2},
            s3={"addressing_style": "path"},
            proxies={} if direct_connection else None,
        ),
    )
    return cast(_S3CleanupClient, _install_bounded_s3_listing_transport(client))


def _assert_s3_prefix_empty(service: LiveServiceConfiguration, key_prefix: str) -> None:
    keys = _run_s3_operation(
        _s3_keys,
        service,
        key_prefix,
        timeout_seconds=_S3_CLEANUP_PROCESS_TIMEOUT_SECONDS,
    )
    if keys:
        raise AssertionError("generated live-acceptance S3 prefix is not empty")


def _s3_keys(service: LiveServiceConfiguration, key_prefix: str) -> tuple[str, ...]:
    client = _s3_client(service)
    try:
        return _list_s3_keys(client, service.s3_bucket, f"{key_prefix}/")
    finally:
        client.close()


def _s3_object_evidence(
    service: LiveServiceConfiguration,
    service_key: str,
) -> tuple[int, str]:
    client = _s3_client(service)
    try:
        response = client.get_object(Bucket=service.s3_bucket, Key=service_key)
        body = response.get("Body")
        if body is None or not hasattr(body, "read") or not hasattr(body, "close"):
            raise TypeError("S3 archive returned an invalid streaming body")
        streaming_body = cast(_S3StreamingBody, body)
        digest = hashlib.sha256()
        content_length = 0
        try:
            while True:
                remaining_with_probe = MAXIMUM_ARCHIVE_BYTES - content_length + 1
                chunk = streaming_body.read(min(64 * 1024, remaining_with_probe))
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("S3 archive returned a non-byte chunk")
                content_length += len(chunk)
                if content_length > MAXIMUM_ARCHIVE_BYTES:
                    raise AssertionError("S3 archive bytes exceed the acceptance byte limit")
                digest.update(chunk)
        finally:
            streaming_body.close()
        if content_length <= 0:
            raise AssertionError("S3 archive object is empty")
        declared_length = response.get("ContentLength")
        if declared_length is not None and declared_length != content_length:
            raise AssertionError("S3 archive declared length differs from streamed bytes")
        return content_length, digest.hexdigest()
    finally:
        client.close()


def _list_s3_keys(
    client: _S3CleanupClient,
    bucket: str,
    prefix: str,
    *,
    limits: _S3CleanupLimits = _S3_CLEANUP_LIMITS,
) -> tuple[str, ...]:
    keys: list[str] = []
    budget = _S3CleanupBudget(limits)
    for page_keys in _iter_s3_key_pages(client, bucket, prefix, budget=budget):
        keys.extend(page_keys)
    return tuple(sorted(keys))


def _iter_s3_key_pages(
    client: _S3CleanupClient,
    bucket: str,
    prefix: str,
    *,
    budget: _S3CleanupBudget,
) -> Iterator[tuple[str, ...]]:
    limits = budget.limits
    continuation_token: str | None = None
    seen_tokens: set[str] = set()
    seen_keys: set[str] = set()
    page_count = 0
    key_count = 0
    no_progress_pages = 0
    while True:
        budget.checkpoint()
        if page_count >= limits.maximum_pages:
            raise AssertionError("S3 cleanup listing exceeded its page limit")
        arguments: dict[str, object] = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": limits.page_size,
        }
        if continuation_token is not None:
            arguments["ContinuationToken"] = continuation_token
        budget.before_request()
        response = client.list_objects_v2(**arguments)
        budget.after_request()
        page_count += 1
        if not isinstance(response, Mapping):
            raise TypeError("S3 cleanup listing returned an invalid response")
        contents = response.get("Contents", ())
        if not isinstance(contents, list | tuple):
            raise TypeError("S3 cleanup listing returned invalid contents")
        if len(contents) > limits.page_size:
            raise AssertionError("S3 cleanup listing exceeded its requested page size")
        page_keys: list[str] = []
        for entry in contents:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("Key"), str):
                raise TypeError("S3 cleanup listing returned an invalid key")
            key = cast(str, entry["Key"])
            if not key.startswith(prefix):
                raise AssertionError("S3 cleanup listing escaped its unique prefix")
            if len(key.encode("utf-8")) > limits.maximum_key_bytes:
                raise AssertionError("S3 cleanup listing exceeded its key byte limit")
            page_keys.append(key)
        key_count += len(page_keys)
        if key_count > limits.maximum_keys:
            raise AssertionError("S3 cleanup listing exceeded its key limit")
        new_keys = set(page_keys).difference(seen_keys)
        seen_keys.update(page_keys)
        if response.get("IsTruncated") is True and not new_keys:
            no_progress_pages += 1
            if no_progress_pages > limits.maximum_no_progress_pages:
                raise AssertionError("S3 cleanup listing continued without key progress")
        else:
            no_progress_pages = 0
        yield tuple(dict.fromkeys(page_keys))
        if response.get("IsTruncated") is not True:
            return
        raw_token = response.get("NextContinuationToken")
        if not isinstance(raw_token, str) or not raw_token:
            raise AssertionError("S3 cleanup listing omitted its continuation token")
        if len(raw_token.encode("utf-8")) > limits.maximum_token_bytes:
            raise AssertionError("S3 cleanup listing exceeded its continuation token limit")
        if raw_token in seen_tokens:
            raise AssertionError("S3 cleanup listing repeated continuation token")
        seen_tokens.add(raw_token)
        continuation_token = raw_token


def _purge_s3_prefix(service: LiveServiceConfiguration, key_prefix: str) -> None:
    client = _s3_client(service)
    prefix = f"{key_prefix}/"
    budget = _S3CleanupBudget(_S3_CLEANUP_LIMITS)
    try:
        for batch in _iter_s3_key_pages(
            client,
            service.s3_bucket,
            prefix,
            budget=budget,
        ):
            if not batch:
                continue
            budget.before_request()
            response = client.delete_objects(
                Bucket=service.s3_bucket,
                Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
            )
            budget.after_request()
            if not isinstance(response, Mapping):
                raise TypeError("S3 cleanup deletion returned an invalid response")
            errors = response.get("Errors", ())
            if errors:
                raise AssertionError("S3 cleanup failed to delete every scoped object")
        for remaining in _iter_s3_key_pages(
            client,
            service.s3_bucket,
            prefix,
            budget=budget,
        ):
            if remaining:
                raise AssertionError("S3 cleanup left objects under the unique prefix")
    finally:
        client.close()
