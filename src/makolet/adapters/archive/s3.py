"""Immutable content-addressed archive backed by an S3-compatible service."""

from __future__ import annotations

import hashlib
import ipaddress
import math
import os
import tempfile
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol
from urllib.parse import urlsplit

import anyio
from botocore.exceptions import ClientError

from makolet.adapters.filesystem_capacity import (
    FileSystemCapacityGuard,
    FileSystemCapacityUnavailableError,
)
from makolet.domain.errors import ArchiveCapacityError, ArchiveIntegrityError, DownloadLimitError

from ._process import (
    DuplicatedFileDescriptor,
    ProcessDeadlineError,
    ProcessWorkerError,
    run_in_spawn_process,
)
from .keys import (
    digest_for_key,
    key_for_digest,
    normalize_key_prefix,
    service_key_for_object,
)
from .s3_transport import install_bounded_s3_response_transport

DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_OPERATION_TIMEOUT_SECONDS = 300.0

_RUNTIME_S3_RESPONSE_OPERATIONS: Final = {
    "HeadBucket": False,
    "PutObject": False,
    "HeadObject": False,
    "GetObject": True,
}


class _S3Client(Protocol):
    def close(self) -> None: ...

    def head_bucket(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def head_object(self, **kwargs: Any) -> Mapping[str, Any]: ...


class _S3OperationRunner(Protocol):
    async def head_bucket(self, *, bucket: str, deadline: float) -> None: ...

    async def put_once(
        self,
        temporary_path: Path,
        *,
        bucket: str,
        service_key: str,
        digest: str,
        content_length: int,
        deadline: float,
    ) -> bool: ...

    async def download(
        self,
        duplicated_spool: DuplicatedFileDescriptor,
        *,
        bucket: str,
        service_key: str,
        expected_digest: str,
        maximum_object_bytes: int,
        minimum_free_bytes: int,
        chunk_size: int,
        temporary_directory: Path,
        deadline: float,
    ) -> tuple[str, int]: ...

    async def exists(
        self,
        *,
        bucket: str,
        service_key: str,
        deadline: float,
    ) -> bool: ...


if TYPE_CHECKING:
    from types_boto3_s3.client import S3Client as BotoS3Client
else:
    BotoS3Client = _S3Client


@dataclass(frozen=True, slots=True, repr=False)
class S3UploadProcessConfig:
    """Serializable settings used to build an isolated upload client."""

    endpoint_url: str | None
    region_name: str | None
    access_key_id: str | None
    secret_access_key: str | None
    path_style: bool
    unsigned: bool = False
    direct_connection: bool = False

    def __post_init__(self) -> None:
        if self.direct_connection and not _is_http_literal_loopback_endpoint(self.endpoint_url):
            raise ValueError(
                "Direct S3 connections are limited to plaintext literal-loopback endpoints"
            )

    def __repr__(self) -> str:
        return (
            "S3UploadProcessConfig("
            f"endpoint_url={self.endpoint_url!r}, region_name={self.region_name!r}, "
            "access_key_id=<redacted>, secret_access_key=<redacted>, "
            f"path_style={self.path_style!r}, unsigned={self.unsigned!r}, "
            f"direct_connection={self.direct_connection!r})"
        )


class _ProcessS3OperationRunner:
    def __init__(self, configuration: S3UploadProcessConfig) -> None:
        self._configuration = configuration

    async def head_bucket(self, *, bucket: str, deadline: float) -> None:
        await self._run(
            _head_s3_bucket_in_child,
            self._configuration,
            bucket,
            deadline=deadline,
            failure_message="S3 archive bucket is unavailable",
        )

    async def put_once(
        self,
        temporary_path: Path,
        *,
        bucket: str,
        service_key: str,
        digest: str,
        content_length: int,
        deadline: float,
    ) -> bool:
        result = await self._run(
            _put_s3_object_in_child,
            self._configuration,
            temporary_path,
            bucket,
            service_key,
            digest,
            content_length,
            deadline=deadline,
            failure_message="S3 archive upload failed",
        )
        return bool(result)

    async def download(
        self,
        duplicated_spool: DuplicatedFileDescriptor,
        *,
        bucket: str,
        service_key: str,
        expected_digest: str,
        maximum_object_bytes: int,
        minimum_free_bytes: int,
        chunk_size: int,
        temporary_directory: Path,
        deadline: float,
    ) -> tuple[str, int]:
        result = await self._run(
            _download_s3_object_in_child,
            self._configuration,
            duplicated_spool,
            bucket,
            service_key,
            expected_digest,
            maximum_object_bytes,
            minimum_free_bytes,
            chunk_size,
            temporary_directory,
            deadline=deadline,
            failure_message="S3 archive read failed",
        )
        if (
            not isinstance(result, tuple)
            or len(result) != 2
            or not isinstance(result[0], str)
            or not isinstance(result[1], int)
        ):
            raise ArchiveIntegrityError("S3 archive reader returned an invalid result")
        return result

    async def exists(
        self,
        *,
        bucket: str,
        service_key: str,
        deadline: float,
    ) -> bool:
        result = await self._run(
            _head_s3_object_in_child,
            self._configuration,
            bucket,
            service_key,
            deadline=deadline,
            failure_message="S3 archive metadata read failed",
        )
        return bool(result)

    async def _run(
        self,
        target: Any,
        *args: object,
        deadline: float,
        failure_message: str,
    ) -> object:
        remaining = deadline - anyio.current_time()
        if remaining <= 0:
            raise ArchiveIntegrityError(
                f"{failure_message} because its total operation deadline expired"
            )
        try:
            return await run_in_spawn_process(
                target,
                *args,
                timeout_seconds=remaining,
            )
        except ProcessDeadlineError as error:
            raise ArchiveIntegrityError(
                f"{failure_message} because its total operation deadline expired"
            ) from error
        except ProcessWorkerError as error:
            raise ArchiveIntegrityError(failure_message) from error


class S3ContentAddressedArchive:
    """Store exact bytes once with conditional S3 object creation.

    Objects use a SHA-256-derived key and ``If-None-Match: *`` so concurrent
    writers cannot replace an existing object. The application identity should
    additionally receive no S3 delete permission in production.
    """

    def __init__(
        self,
        client: _S3Client | BotoS3Client | None,
        bucket: str,
        *,
        key_prefix: str = "raw",
        maximum_object_bytes: int = 2 * 1024 * 1024 * 1024,
        minimum_free_bytes: int = 0,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        temporary_directory: Path | None = None,
        verify_after_write: bool = True,
        operation_timeout_seconds: float = DEFAULT_OPERATION_TIMEOUT_SECONDS,
        upload_process_config: S3UploadProcessConfig | None = None,
        _operation_runner: _S3OperationRunner | None = None,
    ) -> None:
        if not bucket or any(character in bucket for character in "\r\n\x00"):
            raise ValueError("S3 bucket is empty or unsafe")
        normalized_prefix = normalize_key_prefix(key_prefix)
        if (
            maximum_object_bytes <= 0
            or minimum_free_bytes < 0
            or chunk_size <= 0
            or operation_timeout_seconds <= 0
            or not math.isfinite(operation_timeout_seconds)
        ):
            raise ValueError(
                "Archive byte limits and operation timeout must be positive and finite"
            )
        self._bucket = bucket
        self._key_prefix = normalized_prefix
        self._maximum_object_bytes = maximum_object_bytes
        self._minimum_free_bytes = minimum_free_bytes
        self._chunk_size = chunk_size
        self._temporary_directory = (
            temporary_directory.resolve()
            if temporary_directory is not None
            else Path(tempfile.gettempdir()).resolve()
        )
        self._capacity = FileSystemCapacityGuard(
            self._temporary_directory,
            minimum_free_bytes=minimum_free_bytes,
        )
        self._verify_after_write = verify_after_write
        self._operation_timeout_seconds = operation_timeout_seconds
        if upload_process_config is not None:
            if _operation_runner is not None:
                raise ValueError("Configure exactly one S3 operation runner")
            if client is not None:
                raise ValueError("A parent S3 client cannot be used with isolated operations")
            operation_runner: _S3OperationRunner = _ProcessS3OperationRunner(upload_process_config)
        else:
            if _operation_runner is None:
                raise ValueError("S3 archive requires an isolated operation process configuration")
            operation_runner = _operation_runner
        self._operation_runner = operation_runner

    async def initialize(self) -> None:
        """Verify that the configured bucket is reachable without mutating it."""

        await self._operation_runner.head_bucket(
            bucket=self._bucket,
            deadline=anyio.current_time() + self._operation_timeout_seconds,
        )

    async def close(self) -> None:
        return None

    async def put(
        self,
        chunks: AsyncIterator[bytes],
        *,
        original_filename: str,
    ) -> tuple[str, int, bool]:
        if not original_filename or "\x00" in original_filename:
            raise ValueError("original_filename is empty or unsafe")
        temporary_path = await anyio.to_thread.run_sync(self._temporary_path)
        digest = hashlib.sha256()
        content_length = 0
        try:
            handle = await anyio.open_file(temporary_path, "xb")
            async with handle:
                async for chunk in chunks:
                    if not isinstance(chunk, bytes):
                        raise TypeError("Archive chunks must be bytes")
                    if not chunk:
                        continue
                    content_length += len(chunk)
                    if content_length > self._maximum_object_bytes:
                        raise DownloadLimitError(
                            f"Object exceeds {self._maximum_object_bytes} archived bytes",
                            transferred_bytes=content_length,
                        )
                    try:
                        async with self._capacity.reserve_async(len(chunk)):
                            await handle.write(chunk)
                            await handle.flush()
                    except FileSystemCapacityUnavailableError as error:
                        raise ArchiveCapacityError(
                            "S3 spool reached its configured free-space reserve",
                            transferred_bytes=content_length,
                        ) from error
                    digest.update(chunk)
                try:
                    async with self._capacity.reserve_async(0):
                        await handle.flush()
                        await anyio.to_thread.run_sync(os.fsync, handle.wrapped.fileno())
                except FileSystemCapacityUnavailableError as error:
                    raise ArchiveCapacityError(
                        "S3 spool reached its configured free-space reserve",
                        transferred_bytes=content_length,
                    ) from error

            digest_hex = digest.hexdigest()
            object_key = key_for_digest(digest_hex)
            deadline = anyio.current_time() + self._operation_timeout_seconds
            try:
                created = await self._operation_runner.put_once(
                    temporary_path,
                    bucket=self._bucket,
                    service_key=self._service_key(object_key),
                    digest=digest_hex,
                    content_length=content_length,
                    deadline=deadline,
                )
                if self._verify_after_write or not created:
                    verified_length = await self._verify_with_deadline(
                        object_key,
                        digest_hex,
                        deadline=deadline,
                    )
                    if verified_length != content_length:
                        _raise_archive_length_changed()
            except Exception as error:
                if isinstance(error, ArchiveIntegrityError):
                    error.transferred_bytes = content_length
                    raise
                raise ArchiveIntegrityError(
                    "S3 archive commit could not be confirmed",
                    transferred_bytes=content_length,
                ) from error
            return object_key, content_length, created
        finally:
            with anyio.CancelScope(shield=True):
                with suppress(FileNotFoundError):
                    await anyio.Path(temporary_path).unlink()

    @asynccontextmanager
    async def open(
        self,
        object_key: str,
        *,
        _deadline: float | None = None,
    ) -> AsyncIterator[AsyncIterator[bytes]]:
        expected_digest = digest_for_key(object_key)
        deadline = (
            anyio.current_time() + self._operation_timeout_seconds
            if _deadline is None
            else _deadline
        )
        self._temporary_directory.mkdir(parents=True, exist_ok=True)
        spool = tempfile.TemporaryFile(  # noqa: SIM115 - owned by yielded context
            mode="w+b",
            dir=self._temporary_directory,
        )
        try:
            status, expected_length = await self._operation_runner.download(
                DuplicatedFileDescriptor(spool.fileno()),
                bucket=self._bucket,
                service_key=self._service_key(object_key),
                expected_digest=expected_digest,
                maximum_object_bytes=self._maximum_object_bytes,
                minimum_free_bytes=self._minimum_free_bytes,
                chunk_size=self._chunk_size,
                temporary_directory=self._temporary_directory,
                deadline=deadline,
            )
            if status == "missing":
                raise ArchiveIntegrityError("Archived object is missing")
            if status == "capacity":
                raise ArchiveCapacityError(
                    "S3 read spool reached its configured free-space reserve"
                )
            if status == "integrity":
                raise ArchiveIntegrityError("Archived object failed SHA-256 verification")
            if status == "nonbyte":
                raise ArchiveIntegrityError("S3 archive returned a non-byte chunk")
            if status == "invalid_length":
                raise ArchiveIntegrityError("S3 archive returned an invalid content length")
            if status == "length":
                raise ArchiveIntegrityError("Archived S3 object length changed during read")
            if status == "bounds":
                raise ArchiveIntegrityError("Archived S3 object exceeds expected bounds")
            if status != "ok":
                raise ArchiveIntegrityError("S3 archive reader returned an invalid result")
            if os.fstat(spool.fileno()).st_size != expected_length:
                raise ArchiveIntegrityError("Archived S3 object length changed during read")
            spool.seek(0)
            handle = anyio.wrap_file(spool)
            parser_digest = hashlib.sha256()
            parser_length = 0
            parser_reached_eof = False

            def ensure_parser_stream_verified() -> None:
                if parser_length != expected_length:
                    raise ArchiveIntegrityError(
                        "Parser-visible S3 object length changed during read"
                    )
                if parser_digest.hexdigest() != expected_digest:
                    raise ArchiveIntegrityError("Parser-visible object failed SHA-256 verification")

            async def chunks() -> AsyncIterator[bytes]:
                nonlocal parser_length, parser_reached_eof
                while True:
                    chunk = await handle.read(self._chunk_size)
                    if not chunk:
                        parser_reached_eof = True
                        ensure_parser_stream_verified()
                        return
                    parser_length += len(chunk)
                    if parser_length > expected_length:
                        raise ArchiveIntegrityError(
                            "Parser-visible S3 object exceeds expected bounds"
                        )
                    parser_digest.update(chunk)
                    yield chunk

            parser_chunks = chunks()
            parser_completed_normally = False
            try:
                yield parser_chunks
                parser_completed_normally = True
            finally:
                try:
                    if parser_completed_normally:
                        if not parser_reached_eof:
                            async for _remaining_chunk in parser_chunks:
                                pass
                        ensure_parser_stream_verified()
                finally:
                    with anyio.CancelScope(shield=True):
                        await handle.aclose()
        finally:
            if not spool.closed:
                spool.close()

    async def exists(self, object_key: str) -> bool:
        digest_for_key(object_key)
        return await self._operation_runner.exists(
            bucket=self._bucket,
            service_key=self._service_key(object_key),
            deadline=anyio.current_time() + self._operation_timeout_seconds,
        )

    async def verify(self, object_key: str, expected_sha256: str) -> int:
        return await self._verify_with_deadline(
            object_key,
            expected_sha256,
            deadline=anyio.current_time() + self._operation_timeout_seconds,
        )

    async def _verify_with_deadline(
        self,
        object_key: str,
        expected_sha256: str,
        *,
        deadline: float,
    ) -> int:
        digest_for_key(object_key, expected_sha256=expected_sha256)
        digest = hashlib.sha256()
        content_length = 0
        async with self.open(object_key, _deadline=deadline) as chunks:
            async for chunk in chunks:
                content_length += len(chunk)
                if content_length > self._maximum_object_bytes:
                    raise ArchiveIntegrityError("Archived S3 object exceeds configured bounds")
                digest.update(chunk)
        if digest.hexdigest() != expected_sha256:
            raise ArchiveIntegrityError("Archived object failed SHA-256 verification")
        return content_length

    def _temporary_path(self) -> Path:
        self._temporary_directory.mkdir(parents=True, exist_ok=True)
        descriptor, path = tempfile.mkstemp(
            prefix="makolet-s3-",
            suffix=".part",
            dir=str(self._temporary_directory),
        )
        os.close(descriptor)
        temporary_path = Path(path)
        temporary_path.unlink()
        return temporary_path

    def _service_key(self, object_key: str) -> str:
        return service_key_for_object(object_key, key_prefix=self._key_prefix)


def _put_s3_object_in_child(
    configuration: S3UploadProcessConfig,
    temporary_path: Path,
    bucket: str,
    service_key: str,
    digest: str,
    content_length: int,
) -> bool:
    client = _make_s3_client_in_child(configuration)
    try:
        with temporary_path.open("rb") as body:
            try:
                client.put_object(
                    Bucket=bucket,
                    Key=service_key,
                    Body=body,
                    ContentLength=content_length,
                    ContentType="application/octet-stream",
                    IfNoneMatch="*",
                    Metadata={"sha256": digest},
                )
            except ClientError as error:
                if _is_precondition_failure(error):
                    return False
                raise
        return True
    finally:
        client.close()


def _head_s3_bucket_in_child(
    configuration: S3UploadProcessConfig,
    bucket: str,
) -> None:
    client = _make_s3_client_in_child(configuration)
    try:
        client.head_bucket(Bucket=bucket)
    finally:
        client.close()


def _head_s3_object_in_child(
    configuration: S3UploadProcessConfig,
    bucket: str,
    service_key: str,
) -> bool:
    client = _make_s3_client_in_child(configuration)
    try:
        try:
            client.head_object(Bucket=bucket, Key=service_key)
        except ClientError as error:
            if _is_missing(error):
                return False
            raise
        return True
    finally:
        client.close()


def _download_s3_object_in_child(
    configuration: S3UploadProcessConfig,
    duplicated_spool: DuplicatedFileDescriptor,
    bucket: str,
    service_key: str,
    expected_digest: str,
    maximum_object_bytes: int,
    minimum_free_bytes: int,
    chunk_size: int,
    temporary_directory: Path,
) -> tuple[str, int]:
    client = _make_s3_client_in_child(configuration)
    spool_descriptor = duplicated_spool.detach()
    spool = os.fdopen(spool_descriptor, "w+b", closefd=True)
    body: Any = None
    try:
        try:
            response = client.get_object(Bucket=bucket, Key=service_key)
        except ClientError as error:
            if _is_missing(error):
                return "missing", 0
            raise
        expected_length = response.get("ContentLength")
        if (
            not isinstance(expected_length, int)
            or isinstance(expected_length, bool)
            or expected_length < 0
            or expected_length > maximum_object_bytes
        ):
            return "invalid_length", 0
        body = response.get("Body")
        if body is None or not hasattr(body, "read") or not hasattr(body, "close"):
            return "nonbyte", 0
        capacity = FileSystemCapacityGuard(
            temporary_directory,
            minimum_free_bytes=minimum_free_bytes,
        )
        digest = hashlib.sha256()
        content_length = 0
        while True:
            chunk = body.read(chunk_size)
            if not isinstance(chunk, bytes):
                return "nonbyte", content_length
            if not chunk:
                break
            content_length += len(chunk)
            if content_length > expected_length or content_length > maximum_object_bytes:
                return "bounds", content_length
            try:
                with capacity.reserve(len(chunk)):
                    if spool.write(chunk) != len(chunk):
                        raise OSError("Short write to S3 verification spool")
                    spool.flush()
            except FileSystemCapacityUnavailableError:
                return "capacity", content_length
            digest.update(chunk)
        if content_length != expected_length:
            return "length", content_length
        if digest.hexdigest() != expected_digest:
            return "integrity", content_length
        try:
            with capacity.reserve(0):
                spool.flush()
                os.fsync(spool.fileno())
        except FileSystemCapacityUnavailableError:
            return "capacity", content_length
        return "ok", content_length
    finally:
        if body is not None and hasattr(body, "close"):
            body.close()
        spool.close()
        client.close()


def _make_s3_client_in_child(configuration: S3UploadProcessConfig) -> Any:
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config as BotoConfig

    signature_version = UNSIGNED if configuration.unsigned else "s3v4"
    client = boto3.client(
        "s3",
        endpoint_url=configuration.endpoint_url,
        region_name=configuration.region_name,
        aws_access_key_id=configuration.access_key_id,
        aws_secret_access_key=configuration.secret_access_key,
        config=BotoConfig(
            signature_version=signature_version,
            retries={"mode": "standard", "max_attempts": 4},
            s3={"addressing_style": "path" if configuration.path_style else "virtual"},
            proxies={} if configuration.direct_connection else None,
        ),
    )
    try:
        return install_bounded_s3_response_transport(
            client,
            operations=_RUNTIME_S3_RESPONSE_OPERATIONS,
            error_factory=ArchiveIntegrityError,
        )
    except Exception:
        close = getattr(client, "close", None)
        if callable(close):
            with suppress(Exception):
                close()
        raise


def _is_http_literal_loopback_endpoint(endpoint_url: str | None) -> bool:
    if endpoint_url is None:
        return False
    parsed = urlsplit(endpoint_url)
    if parsed.scheme.casefold() != "http" or not parsed.hostname:
        return False
    try:
        return ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        return False


def _status_code(error: ClientError) -> int | None:
    metadata = error.response.get("ResponseMetadata", {})
    value = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
    return value if isinstance(value, int) else None


def _raise_archive_length_changed() -> None:
    raise ArchiveIntegrityError("Archived object length changed after upload")


def _error_code(error: ClientError) -> str:
    details = error.response.get("Error", {})
    value = details.get("Code") if isinstance(details, dict) else ""
    return str(value)


def _is_missing(error: ClientError) -> bool:
    return _status_code(error) == 404 or _error_code(error) in {"404", "NoSuchKey", "NotFound"}


def _is_precondition_failure(error: ClientError) -> bool:
    return _status_code(error) == 412 or _error_code(error) in {
        "412",
        "PreconditionFailed",
        "ConditionalRequestConflict",
    }
