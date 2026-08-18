"""Portable, bounded backup/verification/restore for the raw S3 archive."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import queue
import stat
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from functools import partial
from itertools import zip_longest
from pathlib import Path
from typing import Any, BinaryIO, Final
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import text

from makolet.adapters.archive.keys import (
    digest_for_key,
    normalize_key_prefix,
    object_key_from_service_key,
    service_key_for_object,
)
from makolet.adapters.archive.s3_transport import (
    DEFAULT_MAXIMUM_S3_CONTROL_RESPONSE_BYTES,
    bounded_s3_response,
    install_bounded_s3_response_transport,
)
from makolet.adapters.filesystem_capacity import (
    FileSystemCapacityGuard,
    FileSystemCapacityUnavailableError,
)
from makolet.adapters.persistence.database import Database
from makolet.config import load_settings, validate_s3_endpoint_transport
from makolet.domain.errors import ArchiveIntegrityError

_FORMAT: Final = "makolet-raw-archive-backup-v2"
_MANIFEST_NAME: Final = "manifest.json"
_CHECKSUM_NAME: Final = f"{_MANIFEST_NAME}.sha256"
_AUTHENTICATION_NAME: Final = f"{_MANIFEST_NAME}.hmac-sha256"
_AUTHENTICATION_KEY_ENVIRONMENT: Final = "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"
_WINDOWS_BIND_STAGING_ENVIRONMENT: Final = "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_WINDOWS_BIND"
_WINDOWS_BIND_STAGING_PREFIX: Final = "windows-bind-staging-v1:"
_WINDOWS_BIND_KEY_PATH: Final = Path("/run/secrets/makolet-archive-backup-auth.key.host")
_WINDOWS_BIND_RUNTIME_PLATFORM: Final = os.name
_WINDOWS_BIND_USER_ID: Final = 10001
_WINDOWS_BIND_GROUP_ID: Final = 10001
_WINDOWS_BIND_FILE_MODE: Final = 0o777
_READ_ONLY_FILE_SYSTEM_FLAG: Final = getattr(os, "ST_RDONLY", 1)
_PRIVATE_KEY_DIRECTORY: Final = Path(os.sep) / "tmp"
_AUTHENTICATION_DOMAIN: Final = b"makolet-raw-archive-manifest-hmac-sha256-v1\0"
_AUTHENTICATION_PREFIX: Final = b"makolet-raw-archive-manifest-hmac-sha256-v1:"
_AUTHENTICATION_BYTES: Final = len(_AUTHENTICATION_PREFIX) + 64 + 1
_CHECKSUM_BYTES: Final = 64 + 2 + len(_MANIFEST_NAME.encode("ascii")) + 1
_AUTHENTICATION_KEY_BYTES: Final = 32
_REQUIRE_OWNER_ONLY_KEY_PERMISSIONS: Final = os.name != "nt"
_CHUNK_SIZE: Final = 1024 * 1024
_MAXIMUM_MANIFEST_BYTES: Final = 16 * 1024 * 1024
_DEFAULT_MAXIMUM_OBJECT_BYTES: Final = 16 * 1024 * 1024 * 1024
_DEFAULT_MAXIMUM_OBJECTS: Final = 1_000_000
_DEFAULT_MAXIMUM_BACKUP_BYTES: Final = 128 * 1024 * 1024 * 1024
_DEFAULT_MINIMUM_FREE_BYTES: Final = 1024 * 1024 * 1024
_MAXIMUM_CONFIGURED_BYTES: Final = 2**63 - 1
_DEFAULT_MAXIMUM_LIST_PAGES: Final = 10_000
_DEFAULT_MAXIMUM_NO_PROGRESS_PAGES: Final = 3
_MAXIMUM_LIST_RESPONSE_BYTES: Final = DEFAULT_MAXIMUM_S3_CONTROL_RESPONSE_BYTES
_DEFAULT_LIST_TIMEOUT_SECONDS: Final = 300.0
_MAXIMUM_LIST_TIMEOUT_SECONDS: Final = 86_400.0
_DEFAULT_OPERATION_TIMEOUT_SECONDS: Final = 3_600.0
_MAXIMUM_OPERATION_TIMEOUT_SECONDS: Final = 86_400.0
_DEFAULT_CLEANUP_TIMEOUT_SECONDS: Final = 30.0
_MAXIMUM_CLEANUP_TIMEOUT_SECONDS: Final = 300.0
_RESTORE_STAGING_SEGMENT: Final = "restore-staging"
_RESTORE_CLEANUP_ATTEMPTS: Final = 5

_ARCHIVE_TOOL_S3_RESPONSE_OPERATIONS: Final = {
    "ListObjectsV2": False,
    "GetObject": True,
    "HeadObject": False,
    "PutObject": False,
    "CopyObject": False,
    "DeleteObject": False,
}


class BackupError(RuntimeError):
    """A secret-safe archive backup failure."""


def _close_ignoring_errors(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception:
        return


class _BlockingCall:
    def __init__(self, operation: Callable[[], Any]) -> None:
        self.operation = operation
        self.completed = threading.Event()
        self.results: list[Any] = []
        self.errors: list[BaseException] = []


class _BlockingCallRunner:
    """One daemon worker so a blocked SDK call cannot pin the calling thread."""

    def __init__(self) -> None:
        self._calls: queue.Queue[_BlockingCall | None] = queue.Queue()
        self._closed = False
        self._worker = threading.Thread(target=self._run, daemon=True)
        self._worker.start()

    def _run(self) -> None:
        while (call := self._calls.get()) is not None:
            try:
                call.results.append(call.operation())
            except BaseException as error:
                call.errors.append(error)
            finally:
                call.completed.set()

    def submit(self, operation: Callable[[], Any]) -> _BlockingCall:
        if self._closed:
            raise BackupError("Raw archive blocking operation runner is closed")
        call = _BlockingCall(operation)
        self._calls.put(call)
        return call

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._calls.put(None)


class _OperationDeadline:
    """One monotonic work deadline plus a bounded abort/cleanup phase."""

    def __init__(
        self,
        *,
        work_deadline: float,
        cleanup_deadline: float,
        description: str = "Raw archive operation",
    ) -> None:
        if cleanup_deadline < work_deadline:
            raise ValueError("cleanup deadline cannot precede the work deadline")
        self.work_deadline = work_deadline
        self.cleanup_deadline = cleanup_deadline
        self._description = description
        self._work_expired = threading.Event()
        self._cleanup_expired = threading.Event()
        self._work_abort_complete = threading.Event()
        self._cleanup_abort_complete = threading.Event()
        self._lock = threading.Lock()
        self._resources: dict[int, Any] = {}
        self._runner: _BlockingCallRunner | None = None

    @classmethod
    def configured(cls) -> _OperationDeadline:
        work_seconds = _positive_seconds(
            "MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS",
            _DEFAULT_OPERATION_TIMEOUT_SECONDS,
            _MAXIMUM_OPERATION_TIMEOUT_SECONDS,
        )
        cleanup_seconds = _positive_seconds(
            "MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS",
            _DEFAULT_CLEANUP_TIMEOUT_SECONDS,
            _MAXIMUM_CLEANUP_TIMEOUT_SECONDS,
        )
        started_at = time.monotonic()
        return cls(
            work_deadline=started_at + work_seconds,
            cleanup_deadline=started_at + work_seconds + cleanup_seconds,
        )

    def child(self, seconds: float, *, description: str) -> _OperationDeadline:
        started_at = time.monotonic()
        work_deadline = min(self.work_deadline, started_at + seconds)
        cleanup_grace = self.cleanup_deadline - self.work_deadline
        return _OperationDeadline(
            work_deadline=work_deadline,
            cleanup_deadline=min(self.cleanup_deadline, work_deadline + cleanup_grace),
            description=description,
        )

    def remaining(self, *, cleanup: bool = False) -> float:
        selected = self.cleanup_deadline if cleanup else self.work_deadline
        return max(0.0, selected - time.monotonic())

    def checkpoint(self, *, cleanup: bool = False) -> None:
        expired = self._cleanup_expired if cleanup else self._work_expired
        if expired.is_set() or self.remaining(cleanup=cleanup) <= 0:
            self._expire(cleanup=cleanup)
            abort_complete = self._cleanup_abort_complete if cleanup else self._work_abort_complete
            abort_complete.wait(self.remaining(cleanup=True))
            if cleanup:
                raise BackupError(f"{self._description} exceeded its cleanup phase deadline")
            raise BackupError(f"{self._description} exceeded its total operation deadline")

    def run(self, operation: Callable[[], Any], *, cleanup: bool = False) -> Any:
        self.checkpoint(cleanup=cleanup)
        with self._lock:
            if self._runner is None:
                self._runner = _BlockingCallRunner()
            runner = self._runner
        call = runner.submit(operation)
        if not call.completed.wait(self.remaining(cleanup=cleanup)):
            self._expire(cleanup=cleanup)
            self.checkpoint(cleanup=cleanup)
            raise AssertionError("deadline checkpoint unexpectedly returned")
        self.checkpoint(cleanup=cleanup)
        if call.errors:
            raise call.errors[0]
        if not call.results:
            raise BackupError("Raw archive blocking operation returned no result")
        return call.results[0]

    def _expire(self, *, cleanup: bool) -> None:
        event = self._cleanup_expired if cleanup else self._work_expired
        if event.is_set():
            return
        event.set()
        with self._lock:
            resources = tuple(self._resources.values())
        abort_complete = self._cleanup_abort_complete if cleanup else self._work_abort_complete

        def close_resources() -> None:
            closers: list[threading.Thread] = []
            for resource in resources:
                closer = threading.Thread(
                    target=_close_ignoring_errors,
                    args=(resource,),
                    daemon=True,
                )
                closer.start()
                closers.append(closer)
            for closer in closers:
                closer.join(self.remaining(cleanup=True))
            abort_complete.set()

        coordinator = threading.Thread(target=close_resources, daemon=True)
        coordinator.start()

    @contextmanager
    def activate(self) -> Iterator[_OperationDeadline]:
        self.checkpoint()
        work_timer = threading.Timer(self.remaining(), self._expire, kwargs={"cleanup": False})
        cleanup_timer = threading.Timer(
            self.remaining(cleanup=True),
            self._expire,
            kwargs={"cleanup": True},
        )
        work_timer.daemon = True
        cleanup_timer.daemon = True
        work_timer.start()
        cleanup_timer.start()
        try:
            try:
                yield self
            except Exception:
                self.checkpoint()
                raise
            else:
                self.checkpoint()
        finally:
            work_timer.cancel()
            cleanup_timer.cancel()
            with self._lock:
                runner = self._runner
                self._runner = None
            if runner is not None:
                runner.close()

    @contextmanager
    def track(self, resource: Any) -> Iterator[None]:
        close = getattr(resource, "close", None)
        if not callable(close):
            yield
            return
        self.checkpoint(cleanup=True)
        identity = id(resource)
        with self._lock:
            cleanup_expired = self._cleanup_expired.is_set()
            if not cleanup_expired:
                self._resources[identity] = resource
        if cleanup_expired:
            self._expire(cleanup=True)
            raise BackupError(f"{self._description} exceeded its cleanup phase deadline")
        try:
            yield
        finally:
            with self._lock:
                self._resources.pop(identity, None)


def _close_resource(resource: Any, deadline: _OperationDeadline) -> None:
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    deadline.checkpoint(cleanup=True)
    completed = threading.Event()
    errors: list[Exception] = []

    def close_resource() -> None:
        try:
            close()
        except Exception as error:
            errors.append(error)
        finally:
            completed.set()

    closer = threading.Thread(target=close_resource, daemon=True)
    closer.start()
    if not completed.wait(deadline.remaining(cleanup=True)):
        deadline._expire(cleanup=True)
        raise BackupError("Raw archive operation resource close exceeded its cleanup deadline")
    deadline.checkpoint(cleanup=True)
    if errors:
        raise BackupError("Raw archive operation resource could not be closed") from errors[0]


def _write_and_flush(output: BinaryIO, payload: bytes) -> None:
    output.write(payload)
    output.flush()


def _environment(name: str, *, default: str | None = None) -> str:
    value = os.environ.get(name, default or "").strip()
    if not value or any(ord(character) < 32 for character in value):
        raise BackupError(f"Required setting {name} is missing or invalid")
    return value


def _boolean_environment(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    selected = raw.strip().casefold()
    if selected in {"1", "true", "yes", "on"}:
        return True
    if selected in {"0", "false", "no", "off"}:
        return False
    raise BackupError(f"{name} must be a boolean")


def _s3_endpoint() -> str:
    endpoint = _environment("MAKOLET_S3_ENDPOINT")
    environment = _environment("MAKOLET_ENVIRONMENT", default="development").casefold()
    allow_insecure_local = _boolean_environment("MAKOLET_S3_ALLOW_INSECURE_LOCAL")
    try:
        return validate_s3_endpoint_transport(
            endpoint,
            environment=environment,
            allow_insecure_local=allow_insecure_local,
            path_style=True,
        )
    except ValueError as error:
        raise BackupError("S3 transport configuration is invalid or unsafe") from error


def _positive_limit(name: str, default: int, maximum: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as error:
        raise BackupError(f"{name} must be an integer") from error
    if value <= 0 or value > maximum:
        raise BackupError(f"{name} is outside the allowed range")
    return value


def _nonnegative_limit(name: str, default: int, maximum: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as error:
        raise BackupError(f"{name} must be an integer") from error
    if value < 0 or value > maximum:
        raise BackupError(f"{name} is outside the allowed range")
    return value


def _positive_seconds(name: str, default: float, maximum: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        value = float(raw)
    except ValueError as error:
        raise BackupError(f"{name} must be a number") from error
    if not 0 < value <= maximum:
        raise BackupError(f"{name} is outside the allowed range")
    return value


def _configured_key_prefix() -> str:
    raw = os.environ.get("MAKOLET_S3_KEY_PREFIX", "raw")
    if any(ord(character) < 32 for character in raw):
        raise BackupError("Required setting MAKOLET_S3_KEY_PREFIX is invalid")
    try:
        selected = normalize_key_prefix(raw)
    except ValueError as error:
        raise BackupError("Required setting MAKOLET_S3_KEY_PREFIX is invalid") from error
    if not selected:
        raise BackupError("Required setting MAKOLET_S3_KEY_PREFIX is missing or invalid")
    return selected


def _content_digest_for_service_key(service_key: str, key_prefix: str) -> str:
    try:
        object_key = object_key_from_service_key(service_key, key_prefix=key_prefix)
        return digest_for_key(object_key)
    except ArchiveIntegrityError as error:
        raise BackupError("Raw archive object key is not canonical") from error


def _bounded_s3_list_response(
    http_session: Any,
    request: Any,
    *,
    maximum_bytes: int = _MAXIMUM_LIST_RESPONSE_BYTES,
    **kwargs: Any,
) -> Any:
    """Send one signed list request and bound its bytes before botocore parses XML."""
    return bounded_s3_response(
        http_session,
        request,
        operation_name="ListObjectsV2",
        stream_success=False,
        error_factory=BackupError,
        maximum_bytes=maximum_bytes,
        **kwargs,
    )


def _install_bounded_s3_transport(client: Any) -> Any:
    """Bound every S3 response body that Botocore would materialize."""

    return install_bounded_s3_response_transport(
        client,
        operations=_ARCHIVE_TOOL_S3_RESPONSE_OPERATIONS,
        error_factory=BackupError,
        maximum_bytes=_MAXIMUM_LIST_RESPONSE_BYTES,
    )


def _client() -> Any:
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
    environment = _environment("MAKOLET_ENVIRONMENT", default="development").casefold()
    endpoint = _s3_endpoint()
    direct_loopback = environment == "production" and urlsplit(endpoint).scheme == "http"
    arguments: dict[str, Any] = {
        "endpoint_url": endpoint,
        "region_name": _environment("MAKOLET_S3_REGION", default="us-east-1"),
        "config": Config(
            signature_version="s3v4",
            connect_timeout=10,
            read_timeout=60,
            retries={"mode": "standard", "max_attempts": 4},
            s3={"addressing_style": "path"},
            proxies={} if direct_loopback else None,
        ),
    }
    if access_key and secret_key:
        arguments.update(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
    elif access_key or secret_key:
        raise BackupError("S3 access key and secret key must be configured together")
    elif environment == "production":
        raise BackupError("Authenticated S3 credentials are required in production")
    client = boto3.client("s3", **arguments)
    try:
        return _install_bounded_s3_transport(client)
    except Exception:
        _close_ignoring_errors(client)
        raise


def _destination(value: str) -> Path:
    selected = Path(value).resolve()
    selected.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not selected.is_dir() or selected.is_symlink():
        raise BackupError("Backup destination must be a real directory")
    objects = selected / "objects"
    objects.mkdir(mode=0o700, exist_ok=True)
    if not objects.is_dir() or objects.is_symlink():
        raise BackupError("Backup objects path must be a real directory")
    return selected


def _object_rows(
    client: Any,
    bucket: str,
    key_prefix: str,
    maximum_objects: int,
    *,
    deadline: _OperationDeadline | None = None,
) -> Iterator[dict[str, Any]]:
    listing_timeout_seconds = _positive_seconds(
        "MAKOLET_ARCHIVE_BACKUP_LIST_TIMEOUT_SECONDS",
        _DEFAULT_LIST_TIMEOUT_SECONDS,
        _MAXIMUM_LIST_TIMEOUT_SECONDS,
    )
    if deadline is None:
        started_at = time.monotonic()
        active_deadline = _OperationDeadline(
            work_deadline=started_at + listing_timeout_seconds,
            cleanup_deadline=started_at
            + listing_timeout_seconds
            + _DEFAULT_CLEANUP_TIMEOUT_SECONDS,
            description="S3 object listing",
        )
    else:
        active_deadline = deadline.child(
            listing_timeout_seconds,
            description="S3 object listing",
        )
    with active_deadline.activate(), active_deadline.track(client):
        yield from _iter_object_rows(
            client,
            bucket,
            key_prefix,
            maximum_objects,
            deadline=active_deadline,
        )


def _iter_object_rows(
    client: Any,
    bucket: str,
    key_prefix: str,
    maximum_objects: int,
    *,
    deadline: _OperationDeadline,
) -> Iterator[dict[str, Any]]:
    deadline.checkpoint()
    paginator = client.get_paginator("list_objects_v2")
    maximum_pages = _positive_limit(
        "MAKOLET_ARCHIVE_BACKUP_MAX_LIST_PAGES",
        _DEFAULT_MAXIMUM_LIST_PAGES,
        10_000_000,
    )
    maximum_no_progress_pages = _positive_limit(
        "MAKOLET_ARCHIVE_BACKUP_MAX_NO_PROGRESS_PAGES",
        _DEFAULT_MAXIMUM_NO_PROGRESS_PAGES,
        10_000,
    )
    count = 0
    no_progress_pages = 0
    previous_key: str | None = None
    listing_prefix = f"{key_prefix}/sha256/"
    pages = iter(
        paginator.paginate(
            Bucket=bucket,
            Prefix=listing_prefix,
            PaginationConfig={"PageSize": 1000},
        )
    )
    page_count = 0
    while True:
        try:
            page = deadline.run(lambda: next(pages))
        except StopIteration:
            break
        page_count += 1
        deadline.checkpoint()
        if page_count > maximum_pages:
            raise BackupError("S3 object listing exceeded its page/request limit")
        if not isinstance(page, Mapping):
            raise BackupError("S3 returned an invalid object listing page")
        contents = page.get("Contents", [])
        if not isinstance(contents, list):
            raise BackupError("S3 returned an invalid object listing")
        if contents:
            no_progress_pages = 0
        else:
            no_progress_pages += 1
            if no_progress_pages > maximum_no_progress_pages:
                raise BackupError("S3 object listing continued without object progress")
        for entry in contents:
            deadline.checkpoint()
            if not isinstance(entry, Mapping):
                raise BackupError("S3 returned an invalid object entry")
            key = entry.get("Key")
            size = entry.get("Size")
            if (
                not isinstance(key, str)
                or len(key) > 4096
                or any(ord(character) < 32 for character in key)
                or not isinstance(size, int)
                or size < 0
            ):
                raise BackupError("S3 returned unsafe object metadata")
            if previous_key is not None and key <= previous_key:
                raise BackupError("S3 object listing is not strictly ordered")
            previous_key = key
            count += 1
            if count > maximum_objects:
                raise BackupError("Raw archive contains more objects than the configured limit")
            yield {
                "key": key,
                "sha256": _content_digest_for_service_key(key, key_prefix),
                "size": size,
            }


async def _query_authoritative_object_rows(
    *,
    key_prefix: str,
    maximum_objects: int,
    deadline: _OperationDeadline | None = None,
) -> tuple[dict[str, Any], ...]:
    active_deadline = deadline or _OperationDeadline.configured()
    active_deadline.checkpoint()
    settings = load_settings()
    database = Database.unpooled_from_url(
        settings.database_dsn(),
        application_name="makolet-archive-backup-inventory",
    )
    rows: list[dict[str, Any]] = []
    previous_key: str | None = None
    encoded_inventory_bytes = 2
    try:
        async with asyncio.timeout(active_deadline.remaining()):
            async with database.engine.connect() as connection, connection.begin():
                statement_timeout_ms = max(1, int(active_deadline.remaining() * 1_000))
                await connection.execute(
                    text("SELECT set_config('statement_timeout', :timeout, true)"),
                    {"timeout": f"{statement_timeout_ms}ms"},
                )
                result = await connection.stream(
                    text(
                        """
                        SELECT object_key, content_sha256, content_length
                        FROM raw_archive_objects
                        ORDER BY object_key
                        LIMIT :row_limit
                        """
                    ),
                    {"row_limit": maximum_objects + 1},
                )
                async for row in result:
                    active_deadline.checkpoint()
                    object_key = row.object_key
                    digest = row.content_sha256
                    content_length = row.content_length
                    if (
                        not isinstance(object_key, str)
                        or len(object_key) > 4096
                        or not isinstance(digest, str)
                        or not isinstance(content_length, int)
                        or isinstance(content_length, bool)
                        or content_length < 0
                    ):
                        raise BackupError("PostgreSQL returned unsafe raw archive inventory")
                    try:
                        digest_for_key(object_key, expected_sha256=digest)
                        service_key = service_key_for_object(object_key, key_prefix=key_prefix)
                    except (ArchiveIntegrityError, ValueError) as error:
                        raise BackupError(
                            "PostgreSQL returned inconsistent raw archive inventory"
                        ) from error
                    if previous_key is not None and service_key <= previous_key:
                        raise BackupError(
                            "PostgreSQL raw archive inventory is not strictly ordered"
                        )
                    previous_key = service_key
                    entry = {
                        "key": service_key,
                        "sha256": digest,
                        "size": content_length,
                    }
                    encoded_entry = json.dumps(
                        entry,
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    ).encode("utf-8")
                    nested_entry_bytes = len(encoded_entry) + 4 * (encoded_entry.count(b"\n") + 1)
                    encoded_inventory_bytes += nested_entry_bytes + (2 if rows else 4)
                    if encoded_inventory_bytes > _MAXIMUM_MANIFEST_BYTES:
                        raise BackupError(
                            "PostgreSQL raw archive inventory exceeds the manifest size limit"
                        )
                    rows.append(entry)
                    if len(rows) > maximum_objects:
                        raise BackupError(
                            "PostgreSQL raw archive inventory exceeds the configured object limit"
                        )
    except TimeoutError as error:
        raise BackupError(
            "Authoritative PostgreSQL archive inventory exceeded its total operation deadline"
        ) from error
    finally:
        cleanup_remaining = active_deadline.remaining(cleanup=True)
        if cleanup_remaining <= 0:
            raise BackupError(
                "Authoritative PostgreSQL archive inventory exceeded its cleanup phase deadline"
            )
        try:
            await asyncio.wait_for(database.dispose(), timeout=cleanup_remaining)
        except TimeoutError as error:
            raise BackupError(
                "Authoritative PostgreSQL archive inventory exceeded its cleanup phase deadline"
            ) from error
    return tuple(rows)


def _authoritative_object_rows(
    *,
    key_prefix: str,
    maximum_objects: int,
    deadline: _OperationDeadline | None = None,
) -> Iterator[dict[str, Any]]:
    try:
        rows = asyncio.run(
            _query_authoritative_object_rows(
                key_prefix=key_prefix,
                maximum_objects=maximum_objects,
                deadline=deadline,
            )
        )
    except BackupError:
        raise
    except Exception as error:
        raise BackupError("Authoritative PostgreSQL archive inventory could not be read") from error
    return iter(rows)


def _hash_stream(
    source: BinaryIO,
    *,
    maximum_bytes: int,
    deadline: _OperationDeadline | None = None,
) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while True:
        if deadline is not None:
            deadline.checkpoint()
        chunk = (
            deadline.run(lambda: source.read(_CHUNK_SIZE))
            if deadline is not None
            else source.read(_CHUNK_SIZE)
        )
        if not chunk:
            break
        size += len(chunk)
        if size > maximum_bytes:
            raise BackupError("Backup object exceeds the configured size limit")
        digest.update(chunk)
    return digest.hexdigest(), size


def _hash_file(
    path: Path,
    *,
    maximum_bytes: int,
    deadline: _OperationDeadline | None = None,
) -> tuple[str, int]:
    with _open_regular_file(
        path,
        description="Backup object",
        maximum_bytes=maximum_bytes,
    ) as source:
        if deadline is None:
            return _hash_stream(source, maximum_bytes=maximum_bytes)
        with deadline.track(source):
            return _hash_stream(source, maximum_bytes=maximum_bytes, deadline=deadline)


def _download(
    client: Any,
    bucket: str,
    key: str,
    expected_size: int,
    objects_directory: Path,
    maximum_bytes: int,
    capacity: FileSystemCapacityGuard,
    deadline: _OperationDeadline,
) -> tuple[str, int]:
    deadline.checkpoint()
    if expected_size > maximum_bytes:
        raise BackupError("Raw archive object exceeds the configured size limit")
    response = deadline.run(lambda: client.get_object(Bucket=bucket, Key=key))
    deadline.checkpoint()
    body = response.get("Body")
    metadata = response.get("Metadata", {})
    if body is None or not hasattr(body, "iter_chunks") or not hasattr(body, "close"):
        if body is not None:
            _close_resource(body, deadline)
        raise BackupError("S3 returned an invalid object stream")
    digest = hashlib.sha256()
    size = 0
    descriptor, temporary_name = tempfile.mkstemp(prefix=".makolet-", dir=objects_directory)
    temporary_path = Path(temporary_name)
    try:
        try:
            with deadline.track(body):
                try:
                    with os.fdopen(descriptor, "wb") as output, deadline.track(output):
                        chunks = iter(body.iter_chunks(chunk_size=_CHUNK_SIZE))
                        while True:
                            try:
                                chunk = deadline.run(lambda: next(chunks))
                            except StopIteration:
                                break
                            deadline.checkpoint()
                            if not isinstance(chunk, bytes):
                                raise BackupError("S3 returned non-byte object content")
                            size += len(chunk)
                            if size > maximum_bytes:
                                raise BackupError(
                                    "Raw archive object exceeds the configured size limit"
                                )
                            if size > expected_size:
                                raise BackupError("S3 object length changed during backup")
                            digest.update(chunk)
                            try:
                                with capacity.reserve(len(chunk)):
                                    deadline.run(partial(_write_and_flush, output, chunk))
                            except FileSystemCapacityUnavailableError as error:
                                raise BackupError(
                                    "Raw archive backup reached its configured free-space reserve"
                                ) from error
                        try:
                            with capacity.reserve(0):
                                deadline.run(lambda: output.flush())
                                deadline.run(lambda: os.fsync(output.fileno()))
                        except FileSystemCapacityUnavailableError as error:
                            raise BackupError(
                                "Raw archive backup reached its configured free-space reserve"
                            ) from error
                finally:
                    _close_resource(body, deadline)
        except Exception:
            deadline.checkpoint()
            raise
        digest_hex = digest.hexdigest()
        if size != expected_size:
            raise BackupError("S3 object length changed during backup")
        metadata_digest = metadata.get("sha256") if isinstance(metadata, Mapping) else None
        if metadata_digest is not None and metadata_digest != digest_hex:
            raise BackupError("S3 object metadata checksum did not match its content")
        destination = objects_directory / digest_hex
        if destination.exists():
            existing_digest, existing_size = _hash_file(
                destination,
                maximum_bytes=maximum_bytes,
                deadline=deadline,
            )
            if existing_digest != digest_hex or existing_size != size:
                raise BackupError("Existing backup object failed checksum verification")
        else:
            deadline.run(lambda: temporary_path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH))
            deadline.run(lambda: temporary_path.replace(destination))
        return digest_hex, size
    finally:
        temporary_path.unlink(missing_ok=True)


def _write_atomic(
    path: Path,
    payload: bytes,
    *,
    capacity: FileSystemCapacityGuard,
    mode: int = 0o444,
    deadline: _OperationDeadline | None = None,
) -> None:
    if deadline is not None:
        deadline.checkpoint()
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    temporary_path = Path(temporary_name)
    created = False
    try:
        with os.fdopen(descriptor, "wb") as output:
            tracking = deadline.track(output) if deadline is not None else nullcontext()
            with tracking:
                try:
                    with capacity.reserve(len(payload)):
                        if deadline is None:
                            output.write(payload)
                            output.flush()
                            os.fsync(output.fileno())
                        else:
                            deadline.run(lambda: _write_and_flush(output, payload))
                            deadline.run(lambda: os.fsync(output.fileno()))
                except FileSystemCapacityUnavailableError as error:
                    raise BackupError(
                        "Raw archive backup reached its configured free-space reserve"
                    ) from error
        try:
            os.link(temporary_path, path, follow_symlinks=False)
            created = True
            temporary_path.unlink()
            path.chmod(mode)
        except FileExistsError as error:
            raise BackupError("Raw archive backup metadata already exists") from error
        except OSError as error:
            if created:
                path.unlink(missing_ok=True)
            raise BackupError("Raw archive backup metadata cannot be created") from error
    finally:
        temporary_path.unlink(missing_ok=True)


def _require_new_metadata(destination: Path) -> tuple[Path, Path, Path]:
    paths = (
        destination / _MANIFEST_NAME,
        destination / _CHECKSUM_NAME,
        destination / _AUTHENTICATION_NAME,
    )
    for path in paths:
        try:
            path.lstat()
        except FileNotFoundError:
            continue
        except OSError as error:
            raise BackupError("Raw archive backup metadata is unavailable") from error
        raise BackupError("Raw archive backup metadata already exists")
    return paths


def _remove_created_metadata(
    paths: list[Path],
    deadline: _OperationDeadline | None = None,
) -> None:
    cleanup_error: OSError | None = None
    for path in reversed(paths):
        if deadline is not None:
            deadline.checkpoint(cleanup=True)
        try:
            path.chmod(0o600)
            path.unlink()
        except FileNotFoundError:
            continue
        except OSError as error:
            cleanup_error = error
    if cleanup_error is not None:
        raise BackupError(
            "Incomplete raw archive backup metadata cannot be removed"
        ) from cleanup_error


def _regular_file_status(path: Path, *, description: str) -> os.stat_result:
    try:
        status = path.lstat()
    except OSError as error:
        raise BackupError(f"{description} is missing or unavailable") from error
    attributes = getattr(status, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    if (
        not stat.S_ISREG(status.st_mode)
        or stat.S_ISLNK(status.st_mode)
        or (isinstance(attributes, int) and bool(attributes & reparse_flag))
    ):
        raise BackupError(f"{description} must be a regular file")
    return status


def _open_regular_file(
    path: Path,
    *,
    description: str,
    maximum_bytes: int,
) -> BinaryIO:
    before = _regular_file_status(path, description=description)
    if before.st_size > maximum_bytes:
        raise BackupError(f"{description} exceeds its size limit")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise BackupError(f"{description} cannot be opened") from error
    try:
        after = os.fstat(descriptor)
    except OSError as error:
        os.close(descriptor)
        raise BackupError(f"{description} cannot be inspected") from error
    if (
        not stat.S_ISREG(after.st_mode)
        or before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
    ):
        os.close(descriptor)
        raise BackupError(f"{description} changed while it was opened")
    if after.st_size > maximum_bytes:
        os.close(descriptor)
        raise BackupError(f"{description} exceeds its size limit")
    try:
        return os.fdopen(descriptor, "rb")
    except OSError as error:
        os.close(descriptor)
        raise BackupError(f"{description} cannot be opened") from error


def _read_bounded_regular_file(
    path: Path,
    *,
    description: str,
    maximum_bytes: int,
    deadline: _OperationDeadline | None = None,
) -> bytes:
    with _open_regular_file(
        path,
        description=description,
        maximum_bytes=maximum_bytes,
    ) as source:
        tracking = deadline.track(source) if deadline is not None else nullcontext()
        with tracking:
            if deadline is not None:
                deadline.checkpoint()
            before = os.fstat(source.fileno())
            payload = (
                deadline.run(lambda: source.read(maximum_bytes + 1))
                if deadline is not None
                else source.read(maximum_bytes + 1)
            )
            after = os.fstat(source.fileno())
            if deadline is not None:
                deadline.checkpoint()
    if len(payload) > maximum_bytes:
        raise BackupError(f"{description} exceeds its size limit")
    if (
        before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or len(payload) != after.st_size
    ):
        raise BackupError(f"{description} changed while it was read")
    return payload


def _require_separate_key_directory(destination: Path, key_file: Path) -> None:
    try:
        backup_directory = destination.resolve(strict=True)
        key_path = key_file.resolve(strict=True)
    except OSError as error:
        raise BackupError("Archive backup authentication paths are invalid") from error
    key_directory = key_path.parent
    if (
        backup_directory == key_directory
        or backup_directory in key_directory.parents
        or key_directory in backup_directory.parents
    ):
        raise BackupError("Archive backup authentication key must be outside the backup tree")


def _read_authentication_key_file(
    key_file: Path,
    *,
    description: str,
    require_owner_only: bool,
    required_mode: int | None = None,
    deadline: _OperationDeadline | None = None,
) -> bytes:
    with _open_regular_file(
        key_file,
        description=description,
        maximum_bytes=_AUTHENTICATION_KEY_BYTES,
    ) as source:
        tracking = deadline.track(source) if deadline is not None else nullcontext()
        with tracking:
            if deadline is not None:
                deadline.checkpoint()
            status = os.fstat(source.fileno())
            if status.st_nlink != 1:
                raise BackupError("Archive backup authentication key has an unsafe link count")
            if require_owner_only and stat.S_IMODE(status.st_mode) & 0o077:
                raise BackupError("Archive backup authentication key is not protected")
            if required_mode is not None and stat.S_IMODE(status.st_mode) != required_mode:
                raise BackupError("Archive backup staged authentication key is invalid")
            key = (
                deadline.run(lambda: source.read(_AUTHENTICATION_KEY_BYTES + 1))
                if deadline is not None
                else source.read(_AUTHENTICATION_KEY_BYTES + 1)
            )
    if len(key) != _AUTHENTICATION_KEY_BYTES:
        raise BackupError("Archive backup authentication key is invalid")
    return key


def _authentication_key(
    destination: Path,
    deadline: _OperationDeadline | None = None,
) -> bytes:
    configured_path = _environment(_AUTHENTICATION_KEY_ENVIRONMENT)
    key_file = Path(configured_path)
    _require_separate_key_directory(destination, key_file)
    return _read_authentication_key_file(
        key_file,
        description="Archive backup authentication key",
        require_owner_only=_REQUIRE_OWNER_ONLY_KEY_PERMISSIONS,
        deadline=deadline,
    )


def _write_private_authentication_key(key: bytes) -> Path:
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".makolet-archive-backup-auth-",
            dir=_PRIVATE_KEY_DIRECTORY,
        )
    except OSError as error:
        raise BackupError("Archive backup private authentication key cannot be created") from error
    private_file = Path(temporary_name)
    open_descriptor: int | None = descriptor
    complete = False
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            open_descriptor = None
            output.write(key)
            output.flush()
            os.fsync(output.fileno())
        complete = True
    except OSError as error:
        raise BackupError("Archive backup private authentication key cannot be written") from error
    finally:
        if open_descriptor is not None:
            os.close(open_descriptor)
        if not complete:
            private_file.unlink(missing_ok=True)
    return private_file


def _effective_user_and_group() -> tuple[int, int]:
    get_effective_user = getattr(os, "geteuid", None)
    get_effective_group = getattr(os, "getegid", None)
    if get_effective_user is None or get_effective_group is None:
        return -1, -1
    return get_effective_user(), get_effective_group()


def _file_system_is_read_only(path: Path) -> bool:
    stat_file_system = getattr(os, "statvfs", None)
    if stat_file_system is None:
        return False
    try:
        status = stat_file_system(path)
    except OSError as error:
        raise BackupError("Archive backup staged authentication key is invalid") from error
    return bool(status.f_flag & _READ_ONLY_FILE_SYSTEM_FLAG)


@contextmanager
def _authentication_key_scope() -> Iterator[None]:
    staging = os.environ.get(_WINDOWS_BIND_STAGING_ENVIRONMENT, "")
    if not staging:
        yield
        return
    configured_path = os.environ.get(_AUTHENTICATION_KEY_ENVIRONMENT, "")
    encoded_digest = staging.removeprefix(_WINDOWS_BIND_STAGING_PREFIX)
    effective_user, effective_group = _effective_user_and_group()
    if (
        not staging.startswith(_WINDOWS_BIND_STAGING_PREFIX)
        or len(encoded_digest) != 64
        or any(character not in "0123456789abcdef" for character in encoded_digest)
        or configured_path != str(_WINDOWS_BIND_KEY_PATH)
        or _WINDOWS_BIND_RUNTIME_PLATFORM != "posix"
        or effective_user != _WINDOWS_BIND_USER_ID
        or effective_group != _WINDOWS_BIND_GROUP_ID
        or not _file_system_is_read_only(_WINDOWS_BIND_KEY_PATH)
    ):
        raise BackupError("Archive backup authentication key staging is invalid")
    key = _read_authentication_key_file(
        _WINDOWS_BIND_KEY_PATH,
        description="Archive backup staged authentication key",
        require_owner_only=False,
        required_mode=_WINDOWS_BIND_FILE_MODE,
    )
    if not hmac.compare_digest(hashlib.sha256(key).hexdigest(), encoded_digest):
        raise BackupError("Archive backup authentication key staging is invalid")
    private_file = _write_private_authentication_key(key)
    try:
        os.environ[_AUTHENTICATION_KEY_ENVIRONMENT] = str(private_file)
        yield
    finally:
        os.environ[_AUTHENTICATION_KEY_ENVIRONMENT] = configured_path
        try:
            private_file.unlink(missing_ok=True)
        except OSError as error:
            raise BackupError(
                "Archive backup private authentication key cannot be removed"
            ) from error


def _canonical_manifest_payload(manifest: Mapping[str, Any]) -> bytes:
    return (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def _authentication_payload(key: bytes, manifest_payload: bytes) -> bytes:
    digest = _manifest_hmac(key, manifest_payload)
    return _AUTHENTICATION_PREFIX + digest.encode("ascii") + b"\n"


def _manifest_hmac(key: bytes, manifest_payload: bytes) -> str:
    authenticator = hmac.new(key, digestmod=hashlib.sha256)
    authenticator.update(_AUTHENTICATION_DOMAIN)
    authenticator.update(manifest_payload)
    return authenticator.hexdigest()


def _read_authentication(
    destination: Path,
    deadline: _OperationDeadline | None = None,
) -> str:
    payload = _read_bounded_regular_file(
        destination / _AUTHENTICATION_NAME,
        description="Backup manifest authentication sidecar",
        maximum_bytes=_AUTHENTICATION_BYTES,
        deadline=deadline,
    )
    if len(payload) != _AUTHENTICATION_BYTES or not payload.endswith(b"\n"):
        raise BackupError("Backup manifest authentication sidecar is invalid")
    encoded_digest = payload[len(_AUTHENTICATION_PREFIX) : -1]
    if (
        not payload.startswith(_AUTHENTICATION_PREFIX)
        or len(encoded_digest) != 64
        or any(byte not in b"0123456789abcdef" for byte in encoded_digest)
    ):
        raise BackupError("Backup manifest authentication sidecar is invalid")
    return encoded_digest.decode("ascii")


def _read_checksum(
    destination: Path,
    deadline: _OperationDeadline | None = None,
) -> str:
    payload = _read_bounded_regular_file(
        destination / _CHECKSUM_NAME,
        description="Backup manifest checksum sidecar",
        maximum_bytes=_CHECKSUM_BYTES,
        deadline=deadline,
    )
    expected_suffix = f"  {_MANIFEST_NAME}\n".encode("ascii")
    encoded_digest = payload[:64]
    if (
        len(payload) != _CHECKSUM_BYTES
        or payload[64:] != expected_suffix
        or len(encoded_digest) != 64
        or any(byte not in b"0123456789abcdef" for byte in encoded_digest)
    ):
        raise BackupError("Backup manifest checksum sidecar is invalid")
    return encoded_digest.decode("ascii")


def _bounded_backup_manifest(
    client: Any,
    bucket: str,
    key_prefix: str,
    *,
    maximum_objects: int,
    maximum_object_bytes: int,
    maximum_backup_bytes: int,
    deadline: _OperationDeadline,
) -> tuple[dict[str, Any], bytes, int]:
    """Build one bounded manifest without retaining an over-limit inventory."""

    rows: list[dict[str, Any]] = []
    manifest: dict[str, Any] = {
        "format": _FORMAT,
        "created_at": datetime.now(UTC).isoformat(),
        "bucket": bucket,
        "key_prefix": key_prefix,
        "objects": rows,
    }
    empty_payload_bytes = len(_canonical_manifest_payload(manifest))
    prospective_manifest_bytes = empty_payload_bytes
    object_bytes = 0
    missing = object()
    authoritative_rows = _authoritative_object_rows(
        key_prefix=key_prefix,
        maximum_objects=maximum_objects,
        deadline=deadline,
    )
    listed_rows = _object_rows(
        client,
        bucket,
        key_prefix,
        maximum_objects,
        deadline=deadline,
    )
    for authoritative_entry, listed_entry in zip_longest(
        authoritative_rows,
        listed_rows,
        fillvalue=missing,
    ):
        deadline.checkpoint()
        if (
            authoritative_entry is missing
            or listed_entry is missing
            or authoritative_entry != listed_entry
            or not isinstance(authoritative_entry, dict)
        ):
            raise BackupError("S3 listing does not match the authoritative PostgreSQL inventory")
        entry = authoritative_entry
        entry_size = entry["size"]
        if entry_size > maximum_object_bytes:
            raise BackupError("Raw archive object exceeds the configured size limit")
        encoded_entry = json.dumps(
            entry,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        nested_entry_bytes = len(encoded_entry) + 4 * (encoded_entry.count(b"\n") + 1)
        # Replacing the empty ``[]`` adds four framing bytes for the first
        # nested object; each later object adds only ``,\n`` before its block.
        prospective_manifest_bytes += nested_entry_bytes + (2 if rows else 4)
        if prospective_manifest_bytes > _MAXIMUM_MANIFEST_BYTES:
            raise BackupError("Raw archive backup manifest exceeds its size limit")
        object_bytes += entry_size
        prospective_total = (
            object_bytes + prospective_manifest_bytes + _CHECKSUM_BYTES + _AUTHENTICATION_BYTES
        )
        if prospective_total > maximum_backup_bytes:
            raise BackupError("Raw archive backup exceeds its aggregate byte limit")
        rows.append(entry)
    payload = _canonical_manifest_payload(manifest)
    if len(payload) != prospective_manifest_bytes:
        raise BackupError("Raw archive backup manifest accounting is inconsistent")
    return manifest, payload, object_bytes


def backup(destination: Path) -> dict[str, Any]:
    deadline = _OperationDeadline.configured()
    with deadline.activate():
        client = deadline.run(_client)
        with deadline.track(client):
            try:
                return _backup_with_deadline(destination, client=client, deadline=deadline)
            finally:
                _close_resource(client, deadline)


def _backup_with_deadline(
    destination: Path,
    *,
    client: Any,
    deadline: _OperationDeadline,
) -> dict[str, Any]:
    authentication_key = _authentication_key(destination, deadline)
    manifest_path, checksum_path, authentication_path = _require_new_metadata(destination)
    bucket = _environment("MAKOLET_S3_BUCKET")
    key_prefix = _configured_key_prefix()
    maximum_objects = _positive_limit(
        "MAKOLET_ARCHIVE_BACKUP_MAX_OBJECTS", _DEFAULT_MAXIMUM_OBJECTS, 10_000_000
    )
    maximum_bytes = _positive_limit(
        "MAKOLET_ARCHIVE_BACKUP_MAX_OBJECT_BYTES",
        _DEFAULT_MAXIMUM_OBJECT_BYTES,
        _DEFAULT_MAXIMUM_OBJECT_BYTES,
    )
    maximum_backup_bytes = _positive_limit(
        "MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES",
        _DEFAULT_MAXIMUM_BACKUP_BYTES,
        _MAXIMUM_CONFIGURED_BYTES,
    )
    minimum_free_bytes = _nonnegative_limit(
        "MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES",
        _DEFAULT_MINIMUM_FREE_BYTES,
        _MAXIMUM_CONFIGURED_BYTES,
    )
    manifest, payload, object_bytes = _bounded_backup_manifest(
        client,
        bucket,
        key_prefix,
        maximum_objects=maximum_objects,
        maximum_object_bytes=maximum_bytes,
        maximum_backup_bytes=maximum_backup_bytes,
        deadline=deadline,
    )
    rows = manifest["objects"]
    if not isinstance(rows, list):
        raise BackupError("Raw archive backup manifest accounting is inconsistent")
    manifest_digest = hashlib.sha256(payload).hexdigest()
    checksum_payload = f"{manifest_digest}  {_MANIFEST_NAME}\n".encode("ascii")
    authentication_payload = _authentication_payload(authentication_key, payload)
    aggregate_bytes = (
        object_bytes + len(payload) + len(checksum_payload) + len(authentication_payload)
    )
    if aggregate_bytes > maximum_backup_bytes:
        raise BackupError("Raw archive backup exceeds its aggregate byte limit")
    capacity = FileSystemCapacityGuard(
        destination,
        minimum_free_bytes=minimum_free_bytes,
        coordination_directory=destination,
    )
    for entry in rows:
        if not isinstance(entry, dict):
            raise BackupError("Raw archive backup manifest accounting is inconsistent")
        digest, size = _download(
            client,
            bucket,
            entry["key"],
            entry["size"],
            destination / "objects",
            maximum_bytes,
            capacity,
            deadline,
        )
        if digest != entry["sha256"] or size != entry["size"]:
            raise BackupError("Raw archive object key and content checksum disagree")
    created_metadata: list[Path] = []
    try:
        for path, metadata_payload in (
            (manifest_path, payload),
            (checksum_path, checksum_payload),
            (authentication_path, authentication_payload),
        ):
            _write_atomic(path, metadata_payload, capacity=capacity, deadline=deadline)
            created_metadata.append(path)
    except Exception:
        _remove_created_metadata(created_metadata, deadline)
        raise
    return {
        "status": "backed_up",
        "objects": len(rows),
        "manifest_sha256": manifest_digest,
        "authentication": "hmac-sha256-v1",
    }


def _load_verified_manifest(
    destination: Path,
    deadline: _OperationDeadline | None = None,
) -> dict[str, Any]:
    authentication_key = _authentication_key(destination, deadline)
    payload = _read_bounded_regular_file(
        destination / _MANIFEST_NAME,
        description="Backup manifest",
        maximum_bytes=_MAXIMUM_MANIFEST_BYTES,
        deadline=deadline,
    )
    expected_authentication = _read_authentication(destination, deadline)
    actual_authentication = _manifest_hmac(authentication_key, payload)
    if not hmac.compare_digest(actual_authentication, expected_authentication):
        raise BackupError("Backup manifest authentication failed")
    expected_checksum = _read_checksum(destination, deadline)
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), expected_checksum):
        raise BackupError("Backup manifest checksum did not match")
    try:
        manifest = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError, RecursionError) as error:
        raise BackupError("Backup manifest contains invalid JSON") from error
    if not isinstance(manifest, dict) or manifest.get("format") != _FORMAT:
        raise BackupError("Backup manifest format is unsupported")
    try:
        canonical_payload = _canonical_manifest_payload(manifest)
    except (TypeError, ValueError, RecursionError) as error:
        raise BackupError("Backup manifest contains invalid JSON") from error
    if not hmac.compare_digest(payload, canonical_payload):
        raise BackupError("Backup manifest is not canonical")
    return manifest


def _verify_manifest(
    destination: Path,
    manifest: dict[str, Any],
    deadline: _OperationDeadline | None = None,
) -> dict[str, Any]:
    configured_key_prefix = _configured_key_prefix()
    manifest_key_prefix = manifest.get("key_prefix")
    if manifest_key_prefix != configured_key_prefix:
        raise BackupError("Backup manifest key prefix does not match the configured archive")
    objects = manifest.get("objects")
    if not isinstance(objects, list):
        raise BackupError("Backup manifest objects must be a list")
    maximum_objects = _positive_limit(
        "MAKOLET_ARCHIVE_BACKUP_MAX_OBJECTS", _DEFAULT_MAXIMUM_OBJECTS, 10_000_000
    )
    maximum_bytes = _positive_limit(
        "MAKOLET_ARCHIVE_BACKUP_MAX_OBJECT_BYTES",
        _DEFAULT_MAXIMUM_OBJECT_BYTES,
        _DEFAULT_MAXIMUM_OBJECT_BYTES,
    )
    maximum_backup_bytes = _positive_limit(
        "MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES",
        _DEFAULT_MAXIMUM_BACKUP_BYTES,
        _MAXIMUM_CONFIGURED_BYTES,
    )
    if len(objects) > maximum_objects:
        raise BackupError("Backup manifest exceeds the configured object limit")
    seen_keys: set[str] = set()
    aggregate_bytes = 0
    for entry in objects:
        if deadline is not None:
            deadline.checkpoint()
        if not isinstance(entry, dict):
            raise BackupError("Backup manifest contains an invalid object entry")
        key = entry.get("key")
        digest = entry.get("sha256")
        expected_size = entry.get("size")
        if (
            not isinstance(key, str)
            or key in seen_keys
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or not isinstance(expected_size, int)
            or expected_size < 0
            or expected_size > maximum_bytes
        ):
            raise BackupError("Backup manifest contains unsafe object metadata")
        aggregate_bytes += expected_size
        if aggregate_bytes > maximum_backup_bytes:
            raise BackupError("Backup manifest exceeds the configured aggregate byte limit")
        try:
            object_key = object_key_from_service_key(
                key,
                key_prefix=configured_key_prefix,
                expected_sha256=digest,
            )
            digest_for_key(object_key, expected_sha256=digest)
        except ArchiveIntegrityError as error:
            raise BackupError("Backup manifest contains a non-canonical archive key") from error
        seen_keys.add(key)
        actual_digest, actual_size = _hash_file(
            destination / "objects" / digest,
            maximum_bytes=maximum_bytes,
            deadline=deadline,
        )
        if actual_digest != digest or actual_size != expected_size:
            raise BackupError("Backup object failed checksum or length verification")
    return {"status": "verified", "objects": len(objects)}


def verify(destination: Path) -> dict[str, Any]:
    deadline = _OperationDeadline.configured()
    with deadline.activate():
        manifest = _load_verified_manifest(destination, deadline)
        return _verify_manifest(destination, manifest, deadline)


def _remote_digest(
    client: Any,
    bucket: str,
    key: str,
    maximum_bytes: int,
    *,
    version_id: str | None = None,
    deadline: _OperationDeadline,
) -> tuple[str, int, str, str | None]:
    deadline.checkpoint()
    arguments = {"Bucket": bucket, "Key": key}
    if version_id is not None:
        arguments["VersionId"] = version_id
    response = deadline.run(lambda: client.get_object(**arguments))
    deadline.checkpoint()
    body = response.get("Body")
    etag = response.get("ETag")
    returned_version_id = _version_id(response)
    if body is None or not hasattr(body, "iter_chunks") or not hasattr(body, "close"):
        if body is not None:
            _close_resource(body, deadline)
        raise BackupError("S3 returned an invalid object stream")
    if (
        not isinstance(etag, str)
        or not etag
        or len(etag) > 1024
        or any(ord(character) < 32 for character in etag)
    ):
        _close_resource(body, deadline)
        raise BackupError("S3 returned invalid object identity metadata")
    digest = hashlib.sha256()
    size = 0
    try:
        with deadline.track(body):
            chunks = iter(body.iter_chunks(chunk_size=_CHUNK_SIZE))
            while True:
                try:
                    chunk = deadline.run(lambda: next(chunks))
                except StopIteration:
                    break
                deadline.checkpoint()
                if not isinstance(chunk, bytes):
                    raise BackupError("S3 returned non-byte object content")
                size += len(chunk)
                if size > maximum_bytes:
                    raise BackupError("Remote archive object exceeds the configured size limit")
                digest.update(chunk)
    finally:
        _close_resource(body, deadline)
    if version_id is not None and returned_version_id not in {None, version_id}:
        raise BackupError("S3 returned inconsistent object version metadata")
    return digest.hexdigest(), size, etag, returned_version_id or version_id


def _is_precondition_failure(error: ClientError) -> bool:
    status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    error_code = str(error.response.get("Error", {}).get("Code", ""))
    return status_code == 412 or error_code in {"412", "PreconditionFailed"}


def _is_not_found(error: ClientError) -> bool:
    status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    error_code = str(error.response.get("Error", {}).get("Code", ""))
    return status_code == 404 or error_code in {"404", "NoSuchKey", "NoSuchVersion", "NotFound"}


def _version_id(response: Mapping[str, Any] | None) -> str | None:
    if response is None:
        return None
    value = response.get("VersionId")
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 1024
        or any(ord(character) < 32 for character in value)
    ):
        raise BackupError("S3 returned invalid object version metadata")
    return value


def _restore_staging_key(key_prefix: str, service_key: str, digest: str) -> str:
    identity = hashlib.sha256(
        b"makolet-raw-restore-staging-v1\0"
        + service_key.encode("utf-8")
        + b"\0"
        + digest.encode("ascii")
    ).hexdigest()
    return f"{key_prefix}/{_RESTORE_STAGING_SEGMENT}/{identity}"


def _head_staging_version(
    client: Any,
    bucket: str,
    staging_key: str,
    *,
    version_id: str | None,
    deadline: _OperationDeadline,
    cleanup: bool = False,
) -> tuple[bool, str | None]:
    deadline.checkpoint(cleanup=cleanup)
    arguments = {"Bucket": bucket, "Key": staging_key}
    if version_id is not None:
        arguments["VersionId"] = version_id
    try:
        response = deadline.run(
            lambda: client.head_object(**arguments),
            cleanup=cleanup,
        )
    except ClientError as error:
        deadline.checkpoint(cleanup=cleanup)
        if _is_not_found(error):
            return False, None
        raise
    deadline.checkpoint(cleanup=cleanup)
    if not isinstance(response, Mapping):
        raise BackupError("S3 returned invalid staging object metadata")
    discovered_version = _version_id(response)
    if version_id is not None and discovered_version not in {None, version_id}:
        raise BackupError("S3 returned inconsistent staging object version metadata")
    return True, discovered_version or version_id


def _cleanup_staging_object(
    client: Any,
    bucket: str,
    staging_key: str,
    *,
    version_id: str | None = None,
    deadline: _OperationDeadline,
) -> None:
    pending_version = version_id
    last_error: Exception | None = None
    for _attempt in range(_RESTORE_CLEANUP_ATTEMPTS):
        deadline.checkpoint(cleanup=True)
        try:
            exists, discovered_version = _head_staging_version(
                client,
                bucket,
                staging_key,
                version_id=pending_version,
                deadline=deadline,
                cleanup=True,
            )
            if not exists:
                return
            pending_version = discovered_version
            delete_arguments = {"Bucket": bucket, "Key": staging_key}
            if pending_version is not None:
                delete_arguments["VersionId"] = pending_version
            deadline.run(
                partial(client.delete_object, **delete_arguments),
                cleanup=True,
            )
            deadline.checkpoint(cleanup=True)
            exists, _discovered_version = _head_staging_version(
                client,
                bucket,
                staging_key,
                version_id=pending_version,
                deadline=deadline,
                cleanup=True,
            )
            if not exists:
                return
        except (BotoCoreError, ClientError) as error:
            deadline.checkpoint(cleanup=True)
            last_error = error
    raise BackupError("Raw archive restore staging cleanup failed") from last_error


def _restore_object(
    client: Any,
    bucket: str,
    key_prefix: str,
    destination: Path,
    entry: Mapping[str, Any],
    maximum_bytes: int,
    deadline: _OperationDeadline,
) -> bool:
    deadline.checkpoint()
    key = entry["key"]
    digest = entry["sha256"]
    expected_size = entry["size"]
    path = destination / "objects" / digest
    staging_key = _restore_staging_key(key_prefix, key, digest)
    _cleanup_staging_object(client, bucket, staging_key, deadline=deadline)
    staging_version_id: str | None = None
    try:
        with (
            _open_regular_file(
                path,
                description="Backup object",
                maximum_bytes=maximum_bytes,
            ) as body,
            deadline.track(body),
        ):
            actual_digest, actual_size = _hash_stream(
                body,
                maximum_bytes=maximum_bytes,
                deadline=deadline,
            )
            if actual_digest != digest or actual_size != expected_size:
                raise BackupError("Backup object failed checksum or length verification")
            body.seek(0)
            try:
                deadline.checkpoint()
                response = deadline.run(
                    lambda: client.put_object(
                        Bucket=bucket,
                        Key=staging_key,
                        Body=body,
                        ContentLength=expected_size,
                        ContentType="application/octet-stream",
                        IfNoneMatch="*",
                        Metadata={"sha256": digest},
                    )
                )
                deadline.checkpoint()
                if response is not None and not isinstance(response, Mapping):
                    raise BackupError("S3 returned invalid staging upload metadata")
                staging_version_id = _version_id(response)
            except (BotoCoreError, ClientError) as error:
                deadline.checkpoint()
                raise BackupError("Raw archive restore staging upload failed") from error
        staging_digest, staging_size, staging_etag, verified_version_id = _remote_digest(
            client,
            bucket,
            staging_key,
            maximum_bytes,
            version_id=staging_version_id,
            deadline=deadline,
        )
        staging_version_id = verified_version_id
        if staging_digest != digest or staging_size != expected_size:
            raise BackupError("Raw archive restore staging object failed verification")
        created = False
        copy_source = {"Bucket": bucket, "Key": staging_key}
        if staging_version_id is not None:
            copy_source["VersionId"] = staging_version_id
        try:
            deadline.checkpoint()
            deadline.run(
                lambda: client.copy_object(
                    Bucket=bucket,
                    Key=key,
                    CopySource=copy_source,
                    CopySourceIfMatch=staging_etag,
                    IfNoneMatch="*",
                    ContentType="application/octet-stream",
                    Metadata={"sha256": digest},
                    MetadataDirective="REPLACE",
                )
            )
            deadline.checkpoint()
            created = True
        except (BotoCoreError, ClientError) as error:
            deadline.checkpoint()
            if not isinstance(error, ClientError) or not _is_precondition_failure(error):
                raise BackupError("Raw archive restore publication failed") from error
        remote_digest, remote_size, _remote_etag, _remote_version_id = _remote_digest(
            client,
            bucket,
            key,
            maximum_bytes,
            deadline=deadline,
        )
        if remote_digest != digest or remote_size != expected_size:
            raise BackupError("Restored raw archive object failed verification")
        return created
    finally:
        _cleanup_staging_object(
            client,
            bucket,
            staging_key,
            version_id=staging_version_id,
            deadline=deadline,
        )


def restore(destination: Path, *, confirmed_bucket: str | None) -> dict[str, Any]:
    deadline = _OperationDeadline.configured()
    with deadline.activate():
        manifest = _load_verified_manifest(destination, deadline)
        verification = _verify_manifest(destination, manifest, deadline)
        bucket = _environment("MAKOLET_S3_BUCKET")
        if confirmed_bucket != bucket:
            raise BackupError("Restore confirmation does not match the configured archive bucket")
        client = deadline.run(_client)
        with deadline.track(client):
            try:
                return _restore_with_deadline(
                    destination,
                    confirmed_bucket=confirmed_bucket,
                    manifest=manifest,
                    verification=verification,
                    client=client,
                    deadline=deadline,
                )
            finally:
                _close_resource(client, deadline)


def _restore_with_deadline(
    destination: Path,
    *,
    confirmed_bucket: str | None,
    manifest: dict[str, Any],
    verification: dict[str, Any],
    client: Any,
    deadline: _OperationDeadline,
) -> dict[str, Any]:
    objects = manifest["objects"]
    bucket = _environment("MAKOLET_S3_BUCKET")
    if confirmed_bucket != bucket:
        raise BackupError("Restore confirmation does not match the configured archive bucket")
    maximum_bytes = _positive_limit(
        "MAKOLET_ARCHIVE_BACKUP_MAX_OBJECT_BYTES",
        _DEFAULT_MAXIMUM_OBJECT_BYTES,
        _DEFAULT_MAXIMUM_OBJECT_BYTES,
    )
    created = 0
    for entry in objects:
        deadline.checkpoint()
        if _restore_object(
            client,
            bucket,
            manifest["key_prefix"],
            destination,
            entry,
            maximum_bytes,
            deadline,
        ):
            created += 1
    return {
        "status": "restored",
        "objects": verification["objects"],
        "objects_created": created,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("backup", "verify", "restore"))
    parser.add_argument("destination", help="Backup directory (mounted at /backup in Compose).")
    parser.add_argument(
        "--confirm-bucket",
        help="Exact destination bucket name; required for restore.",
    )
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        destination = _destination(arguments.destination)
        with _authentication_key_scope():
            if arguments.operation == "backup":
                result = backup(destination)
            elif arguments.operation == "verify":
                result = verify(destination)
            else:
                result = restore(destination, confirmed_bucket=arguments.confirm_bucket)
    except BackupError as error:
        sys.stderr.write(f"archive backup error: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
