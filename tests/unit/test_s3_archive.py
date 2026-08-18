from __future__ import annotations

import hashlib
import io
import os
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import boto3
import pytest
from botocore.awsrequest import AWSResponse
from botocore.compat import HTTPHeaders
from botocore.config import Config
from botocore.exceptions import ClientError

from makolet.adapters.archive._process import (
    DuplicatedFileDescriptor,
    ProcessDeadlineError,
    run_in_spawn_process,
)
from makolet.adapters.archive.keys import (
    object_key_from_service_key,
    service_key_for_object,
)
from makolet.adapters.archive.s3 import (
    S3ContentAddressedArchive,
    S3UploadProcessConfig,
    _make_s3_client_in_child,
)
from makolet.adapters.filesystem_capacity import (
    CAPACITY_LOCK_FILENAME,
    FileSystemCapacityGuard,
    FileSystemCapacityUnavailableError,
)
from makolet.domain.errors import ArchiveCapacityError, ArchiveIntegrityError, DownloadLimitError


class FakeBody(io.BytesIO):
    pass


_MAXIMUM_S3_CONTROL_RESPONSE_BYTES = 8 * 1024 * 1024


class _ExactBotocoreRawBody:
    """Record exactly how far botocore reads one synthetic HTTP response."""

    def __init__(self, chunks: tuple[bytes, ...], *, fail_after_chunks: bool = False) -> None:
        self._chunks = chunks
        self._fail_after_chunks = fail_after_chunks
        self.bytes_yielded = 0
        self.closed = False

    def stream(
        self,
        _amount: int | None = None,
        *,
        decode_content: bool = False,
    ) -> Iterator[bytes]:
        assert decode_content is False
        for chunk in self._chunks:
            self.bytes_yielded += len(chunk)
            yield chunk
        if self._fail_after_chunks:
            raise AssertionError("botocore read beyond the configured response boundary")

    def close(self) -> None:
        self.closed = True


class _ExactBotocoreStreamingRawBody:
    def __init__(self, payload: bytes) -> None:
        self._body = io.BytesIO(payload)
        self.read_calls = 0
        self.closed = False

    def read(self, amount: int | None = None) -> bytes:
        self.read_calls += 1
        return self._body.read(amount)

    def close(self) -> None:
        self.closed = True


class _ExactBotocoreHttpSession:
    def __init__(self, response: AWSResponse) -> None:
        self._response = response

    def send(self, request: Any) -> AWSResponse:
        assert request.stream_output is True
        return self._response

    def close(self) -> None:
        return


class _ConfiguredClientEvents:
    def __init__(self) -> None:
        self.registered: list[str] = []

    def register_first(self, name: str, _handler: object) -> None:
        self.registered.append(name)


class _ConfiguredS3Client:
    def __init__(self) -> None:
        self.events = _ConfiguredClientEvents()
        self.meta = SimpleNamespace(events=self.events)
        self._endpoint = SimpleNamespace(http_session=object())

    def close(self) -> None:
        return


def _exact_runtime_s3_client(
    monkeypatch: pytest.MonkeyPatch,
    response: AWSResponse,
) -> Any:
    client = boto3.client(
        "s3",
        endpoint_url="https://objects.example.test",
        region_name="us-east-1",
        aws_access_key_id="test-access-key",  # secret-scan: allow
        aws_secret_access_key="test-secret-key",  # secret-scan: allow
        config=Config(signature_version="s3v4", retries={"max_attempts": 0}),
    )
    endpoint: Any = vars(client)["_endpoint"]
    endpoint.http_session = _ExactBotocoreHttpSession(response)
    monkeypatch.setattr(boto3, "client", lambda *_args, **_kwargs: client)
    return _make_s3_client_in_child(
        S3UploadProcessConfig(
            endpoint_url="https://objects.example.test",
            region_name="us-east-1",
            access_key_id="test-access-key",  # secret-scan: allow
            secret_access_key="test-secret-key",  # secret-scan: allow
            path_style=True,
        )
    )


@pytest.mark.parametrize(
    ("operation", "method_name", "status_code", "arguments"),
    [
        (
            "PutObject",
            "put_object",
            200,
            {
                "Bucket": "raw-archive",
                "Key": "raw/sha256/object",
                "Body": b"value",
                "ContentLength": 5,
            },
        ),
        (
            "GetObject",
            "get_object",
            404,
            {"Bucket": "raw-archive", "Key": "raw/sha256/object"},
        ),
    ],
)
def test_runtime_s3_rejects_oversized_control_responses_before_botocore_parse(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    method_name: str,
    status_code: int,
    arguments: dict[str, Any],
) -> None:
    raw = _ExactBotocoreRawBody(
        (b"x" * _MAXIMUM_S3_CONTROL_RESPONSE_BYTES, b"x"),
        fail_after_chunks=True,
    )
    client = _exact_runtime_s3_client(
        monkeypatch,
        AWSResponse(
            "https://objects.example.test/raw/object",
            status_code,
            HTTPHeaders(),
            raw,
        ),
    )
    try:
        with pytest.raises(
            ArchiveIntegrityError,
            match=rf"S3 {operation} response exceeds its byte limit",
        ):
            getattr(client, method_name)(**arguments)
    finally:
        client.close()

    assert raw.bytes_yielded == _MAXIMUM_S3_CONTROL_RESPONSE_BYTES + 1
    assert raw.closed is True


def test_runtime_s3_preserves_successful_get_object_streaming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _ExactBotocoreStreamingRawBody(b"exact archived bytes")
    client = _exact_runtime_s3_client(
        monkeypatch,
        AWSResponse(
            "https://objects.example.test/raw/object",
            200,
            HTTPHeaders.from_dict({"content-length": "20"}),
            raw,
        ),
    )
    try:
        response = client.get_object(Bucket="raw-archive", Key="raw/sha256/object")
        assert raw.read_calls == 0
        assert response["Body"].read(5) == b"exact"
        assert response["Body"].read() == b" archived bytes"
        response["Body"].close()
    finally:
        client.close()

    assert raw.closed is True


@pytest.mark.parametrize(
    "endpoint",
    ["http://127.0.0.1:8333", "http://[::1]:8333"],
)
def test_isolated_production_s3_client_bypasses_ambient_proxy_for_plaintext_loopback(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
) -> None:
    captured: dict[str, object] = {}
    client = _ConfiguredS3Client()

    def create_client(service: str, **arguments: object) -> object:
        captured["service"] = service
        captured.update(arguments)
        return client

    monkeypatch.setenv("HTTP_PROXY", "http://203.0.113.10:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://203.0.113.10:8080")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.setattr(boto3, "client", create_client)
    configuration = S3UploadProcessConfig(
        endpoint_url=endpoint,
        region_name="us-east-1",
        access_key_id="operator-access",  # secret-scan: allow
        secret_access_key="operator-secret",  # secret-scan: allow
        path_style=True,
        direct_connection=True,
    )

    assert _make_s3_client_in_child(configuration) is client
    assert captured["service"] == "s3"
    assert getattr(captured["config"], "proxies", None) == {}
    assert client.events.registered == [
        "before-send.s3.HeadBucket",
        "before-send.s3.PutObject",
        "before-send.s3.HeadObject",
        "before-send.s3.GetObject",
    ]


def test_isolated_production_s3_client_retains_proxy_semantics_for_https(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    client = _ConfiguredS3Client()

    def create_client(_service: str, **arguments: object) -> object:
        captured.update(arguments)
        return client

    monkeypatch.setenv("HTTPS_PROXY", "http://203.0.113.10:8080")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.setattr(boto3, "client", create_client)

    assert (
        _make_s3_client_in_child(
            S3UploadProcessConfig(
                endpoint_url="https://objects.example.test",
                region_name="us-east-1",
                access_key_id="operator-access",  # secret-scan: allow
                secret_access_key="operator-secret",  # secret-scan: allow
                path_style=True,
            )
        )
        is client
    )

    assert getattr(captured["config"], "proxies", None) is None


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://127.0.0.1:8333",
        "http://localhost:8333",
        "http://objects.example.test:8333",
    ],
)
def test_direct_s3_configuration_rejects_non_plaintext_literal_loopback(
    endpoint: str,
) -> None:
    with pytest.raises(ValueError, match="literal-loopback"):
        S3UploadProcessConfig(
            endpoint_url=endpoint,
            region_name="us-east-1",
            access_key_id="operator-access",  # secret-scan: allow
            secret_access_key="operator-secret",  # secret-scan: allow
            path_style=True,
            direct_connection=True,
        )


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.put_calls = 0

    def head_bucket(self, **kwargs: Any) -> Mapping[str, Any]:
        assert kwargs["Bucket"] == "makolet-raw"
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def close(self) -> None:
        return None

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]:
        self.put_calls += 1
        identity = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if identity in self.objects and kwargs.get("IfNoneMatch") == "*":
            raise _client_error("PreconditionFailed", 412, "PutObject")
        body = kwargs["Body"]
        value = body.read()
        assert isinstance(value, bytes)
        assert len(value) == kwargs["ContentLength"]
        self.objects[identity] = value
        return {"ETag": '"fake"'}

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]:
        identity = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if identity not in self.objects:
            raise _client_error("NoSuchKey", 404, "GetObject")
        payload = self.objects[identity]
        return {"Body": FakeBody(payload), "ContentLength": len(payload)}

    def head_object(self, **kwargs: Any) -> Mapping[str, Any]:
        identity = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if identity not in self.objects:
            raise _client_error("NotFound", 404, "HeadObject")
        return {"ContentLength": len(self.objects[identity])}


class InlineUploadRunner:
    """In-process unit-test seam; production always uses the spawn runner."""

    def __init__(self, client: FakeS3Client) -> None:
        self._client = client

    async def head_bucket(self, *, bucket: str, deadline: float) -> None:
        del deadline
        await anyio.lowlevel.checkpoint()
        self._client.head_bucket(Bucket=bucket)

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
        del deadline
        await anyio.lowlevel.checkpoint()
        with temporary_path.open("rb") as body:
            try:
                self._client.put_object(
                    Bucket=bucket,
                    Key=service_key,
                    Body=body,
                    ContentLength=content_length,
                    ContentType="application/octet-stream",
                    IfNoneMatch="*",
                    Metadata={"sha256": digest},
                )
            except ClientError as error:
                if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 412:
                    return False
                raise
        return True


class InlineOperationRunner(InlineUploadRunner):
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
        del deadline
        await anyio.lowlevel.checkpoint()
        descriptor = duplicated_spool.detach()
        spool = os.fdopen(descriptor, "w+b", closefd=True)
        try:
            response = self._client.get_object(Bucket=bucket, Key=service_key)
        except ClientError as error:
            spool.close()
            if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return "missing", 0
            raise
        expected_length = response.get("ContentLength")
        if not isinstance(expected_length, int) or expected_length > maximum_object_bytes:
            spool.close()
            return "invalid_length", 0
        body = response.get("Body")
        if body is None or not hasattr(body, "read") or not hasattr(body, "close"):
            spool.close()
            return "nonbyte", 0
        capacity = FileSystemCapacityGuard(
            temporary_directory,
            minimum_free_bytes=minimum_free_bytes,
        )
        digest = hashlib.sha256()
        content_length = 0
        try:
            while True:
                chunk = body.read(chunk_size)
                if not isinstance(chunk, bytes):
                    return "nonbyte", content_length
                if not chunk:
                    break
                content_length += len(chunk)
                if content_length > expected_length:
                    return "bounds", content_length
                try:
                    with capacity.reserve(len(chunk)):
                        spool.write(chunk)
                except FileSystemCapacityUnavailableError:
                    return "capacity", content_length
                digest.update(chunk)
            spool.flush()
            os.fsync(spool.fileno())
        finally:
            body.close()
            spool.close()
        if content_length != expected_length:
            return "length", content_length
        if digest.hexdigest() != expected_digest:
            return "integrity", content_length
        return "ok", content_length

    async def exists(
        self,
        *,
        bucket: str,
        service_key: str,
        deadline: float,
    ) -> bool:
        del deadline
        await anyio.lowlevel.checkpoint()
        try:
            self._client.head_object(Bucket=bucket, Key=service_key)
        except ClientError as error:
            if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return False
            raise
        return True


def _stall_child_process(
    duplicated_spool: DuplicatedFileDescriptor | None = None,
) -> None:
    spool = (
        os.fdopen(duplicated_spool.detach(), "w+b", closefd=True)
        if duplicated_spool is not None
        else None
    )
    try:
        threading.Event().wait()
    finally:
        if spool is not None:
            spool.close()


class StalledUploadProcessRunner(InlineOperationRunner):
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
        del temporary_path, bucket, service_key, digest, content_length
        try:
            await run_in_spawn_process(
                _stall_child_process,
                timeout_seconds=max(0.0, deadline - anyio.current_time()),
            )
        except ProcessDeadlineError as error:
            raise ArchiveIntegrityError(
                "S3 archive upload exceeded its total operation deadline"
            ) from error
        raise AssertionError("Stalled upload child unexpectedly returned")


class StalledReadProcessRunner(InlineOperationRunner):
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
        del (
            bucket,
            service_key,
            expected_digest,
            maximum_object_bytes,
            minimum_free_bytes,
            chunk_size,
            temporary_directory,
        )
        try:
            await run_in_spawn_process(
                _stall_child_process,
                duplicated_spool,
                timeout_seconds=max(0.0, deadline - anyio.current_time()),
            )
        except ProcessDeadlineError as error:
            raise ArchiveIntegrityError(
                "S3 archive read exceeded its total operation deadline"
            ) from error
        raise AssertionError("Stalled read child unexpectedly returned")


class SlowUploadStalledReadProcessRunner(StalledReadProcessRunner):
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
        await anyio.sleep(0.075)
        return await InlineUploadRunner.put_once(
            self,
            temporary_path,
            bucket=bucket,
            service_key=service_key,
            digest=digest,
            content_length=content_length,
            deadline=deadline,
        )


def _archive(
    client: FakeS3Client,
    bucket: str = "makolet-test",
    **kwargs: Any,
) -> S3ContentAddressedArchive:
    return S3ContentAddressedArchive(
        client,
        bucket,
        _operation_runner=InlineOperationRunner(client),
        **kwargs,
    )


def _client_error(code: str, status: int, operation: str) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {
                "RequestId": "test-request",
                "HostId": "test-host",
                "HTTPStatusCode": status,
                "HTTPHeaders": {},
                "RetryAttempts": 0,
            },
        },
        operation,
    )


async def _chunks(*values: bytes) -> AsyncIterator[bytes]:
    for value in values:
        yield value


@pytest.mark.asyncio
async def test_s3_archive_writes_exact_bytes_once_and_streams_them_back(tmp_path: Path) -> None:
    client = FakeS3Client()
    archive = _archive(
        client,
        temporary_directory=tmp_path,
        chunk_size=3,
    )
    payload = "מחיר coffee".encode()

    object_key, length, created = await archive.put(
        _chunks(payload[:5], b"", payload[5:]),
        original_filename="Price.xml.gz",
    )
    second_key, second_length, second_created = await archive.put(
        _chunks(payload),
        original_filename="same-content.GZ",
    )
    received = bytearray()
    async with archive.open(object_key) as chunks:
        async for chunk in chunks:
            received.extend(chunk)

    digest = hashlib.sha256(payload).hexdigest()
    assert object_key == f"sha256/{digest[:2]}/{digest[2:4]}/{digest}"
    assert (length, created) == (len(payload), True)
    assert (second_key, second_length, second_created) == (object_key, len(payload), False)
    assert bytes(received) == payload
    assert await archive.exists(object_key)
    assert await archive.verify(object_key, digest) == len(payload)
    assert client.put_calls == 2
    assert [path async for path in anyio.Path(tmp_path).iterdir()] == []


@pytest.mark.asyncio
async def test_duplicate_never_hides_corrupted_existing_object(tmp_path: Path) -> None:
    client = FakeS3Client()
    archive = _archive(
        client,
        temporary_directory=tmp_path,
    )
    payload = b"original"
    object_key, _, _ = await archive.put(_chunks(payload), original_filename="price.xml")
    service_key = ("makolet-test", f"raw/{object_key}")
    client.objects[service_key] = b"corrupted"

    with pytest.raises(ArchiveIntegrityError, match="SHA-256") as caught:
        await archive.put(_chunks(payload), original_filename="price.xml")

    assert caught.value.transferred_bytes == len(payload)


@pytest.mark.asyncio
async def test_s3_archive_rejects_an_equivocating_parse_read_before_yielding_bytes(
    tmp_path: Path,
) -> None:
    verified_payload = b"verified archive bytes"
    equivocated_payload = b"attacker archive bytes"
    assert len(equivocated_payload) == len(verified_payload)
    digest = hashlib.sha256(verified_payload).hexdigest()
    object_key = f"sha256/{digest[:2]}/{digest[2:4]}/{digest}"
    service_identity = ("makolet-test", f"raw/{object_key}")

    class EquivocatingClient(FakeS3Client):
        reads = 0

        def get_object(self, **kwargs: Any) -> Mapping[str, Any]:
            assert (str(kwargs["Bucket"]), str(kwargs["Key"])) == service_identity
            self.reads += 1
            payload = verified_payload if self.reads == 1 else equivocated_payload
            return {"Body": FakeBody(payload), "ContentLength": len(payload)}

    client = EquivocatingClient()
    client.objects[service_identity] = verified_payload
    archive = _archive(
        client,
        temporary_directory=tmp_path,
    )

    assert await archive.verify(object_key, digest) == len(verified_payload)
    parser_stream_opened = False

    async def consume_equivocated_read() -> None:
        nonlocal parser_stream_opened
        async with archive.open(object_key) as chunks:
            parser_stream_opened = True
            _ = [chunk async for chunk in chunks]

    with pytest.raises(ArchiveIntegrityError, match="SHA-256"):
        await consume_equivocated_read()

    assert not parser_stream_opened


@pytest.mark.asyncio
async def test_s3_archive_total_read_deadline_kills_a_stalled_child(tmp_path: Path) -> None:
    payload = b"deadline-bound archive bytes"
    digest = hashlib.sha256(payload).hexdigest()
    object_key = f"sha256/{digest[:2]}/{digest[2:4]}/{digest}"
    client = FakeS3Client()
    archive = S3ContentAddressedArchive(
        client,
        "makolet-test",
        _operation_runner=StalledReadProcessRunner(client),
        temporary_directory=tmp_path,
        operation_timeout_seconds=0.05,
    )

    began = time.monotonic()
    with pytest.raises(ArchiveIntegrityError, match="deadline"):
        async with archive.open(object_key):
            pass

    assert time.monotonic() - began < 1


@pytest.mark.asyncio
async def test_s3_archive_total_upload_deadline_kills_a_stalled_child(tmp_path: Path) -> None:
    client = FakeS3Client()
    archive = S3ContentAddressedArchive(
        client,
        "makolet-test",
        _operation_runner=StalledUploadProcessRunner(client),
        temporary_directory=tmp_path,
        operation_timeout_seconds=0.05,
    )

    began = time.monotonic()
    with pytest.raises(ArchiveIntegrityError, match=r"upload.*deadline") as caught:
        await archive.put(_chunks(b"bounded upload"), original_filename="price.xml")

    assert time.monotonic() - began < 1
    assert caught.value.transferred_bytes == len(b"bounded upload")
    assert [path async for path in anyio.Path(tmp_path).glob("*.part")] == []


@pytest.mark.asyncio
async def test_s3_upload_and_verification_share_one_total_deadline(tmp_path: Path) -> None:
    payload = b"one upload and verification budget"
    client = FakeS3Client()
    archive = S3ContentAddressedArchive(
        client,
        "makolet-test",
        _operation_runner=SlowUploadStalledReadProcessRunner(client),
        temporary_directory=tmp_path,
        operation_timeout_seconds=0.12,
    )

    began = time.monotonic()
    with pytest.raises(ArchiveIntegrityError, match="deadline"):
        await archive.put(_chunks(payload), original_filename="price.xml")

    assert time.monotonic() - began < 0.5


@pytest.mark.asyncio
async def test_s3_upload_caller_cancellation_kills_child_without_waiting(
    tmp_path: Path,
) -> None:
    client = FakeS3Client()
    archive = S3ContentAddressedArchive(
        client,
        "makolet-test",
        _operation_runner=StalledUploadProcessRunner(client),
        temporary_directory=tmp_path,
        operation_timeout_seconds=5,
    )

    async def upload() -> None:
        await archive.put(_chunks(b"cancel me"), original_filename="price.xml")

    began = time.monotonic()
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(upload)
        await anyio.sleep(0.1)
        tasks.cancel_scope.cancel()
    elapsed = time.monotonic() - began

    assert elapsed < 1
    assert [path async for path in anyio.Path(tmp_path).glob("*.part")] == []


@pytest.mark.asyncio
async def test_s3_archive_rejects_a_nonbyte_empty_end_of_stream(tmp_path: Path) -> None:
    payload = b""
    digest = hashlib.sha256(payload).hexdigest()
    object_key = f"sha256/{digest[:2]}/{digest[2:4]}/{digest}"

    class InvalidBody:
        def read(self, _amount: int | None = None) -> str:
            return ""

        def close(self) -> None:
            return None

    class InvalidClient(FakeS3Client):
        def get_object(self, **_kwargs: Any) -> Mapping[str, Any]:
            return {"Body": InvalidBody(), "ContentLength": 0}

    invalid_client = InvalidClient()
    archive = _archive(
        invalid_client,
        temporary_directory=tmp_path,
    )

    with pytest.raises(ArchiveIntegrityError, match="non-byte"):
        async with archive.open(object_key):
            pass


@pytest.mark.asyncio
async def test_s3_archive_rejects_oversize_missing_and_unsafe_keys(tmp_path: Path) -> None:
    client = FakeS3Client()
    archive = _archive(
        client,
        maximum_object_bytes=4,
        temporary_directory=tmp_path,
    )
    missing_digest = hashlib.sha256(b"missing").hexdigest()
    missing_key = f"sha256/{missing_digest[:2]}/{missing_digest[2:4]}/{missing_digest}"

    with pytest.raises(DownloadLimitError):
        await archive.put(_chunks(b"123", b"45"), original_filename="large.xml")
    assert not await archive.exists(missing_key)
    with pytest.raises(ArchiveIntegrityError, match="missing"):
        async with archive.open(missing_key):
            pass
    with pytest.raises(ArchiveIntegrityError, match="canonical"):
        await archive.exists("../secrets")
    assert [path async for path in anyio.Path(tmp_path).iterdir()] == []


@pytest.mark.asyncio
async def test_s3_archive_spool_preserves_configured_free_space_reserve(tmp_path: Path) -> None:
    client = FakeS3Client()
    archive = _archive(
        client,
        minimum_free_bytes=2**63,
        temporary_directory=tmp_path,
    )

    with pytest.raises(ArchiveCapacityError, match="free-space reserve"):
        await archive.put(_chunks(b"bounded"), original_filename="bounded.xml")

    assert client.put_calls == 0
    assert [path async for path in anyio.Path(tmp_path).iterdir()] == [
        anyio.Path(tmp_path / CAPACITY_LOCK_FILENAME)
    ]


def test_s3_archive_configuration_is_bounded_and_canonical() -> None:
    client = FakeS3Client()

    with pytest.raises(ValueError, match="bucket"):
        S3ContentAddressedArchive(client, "bad\nbucket")
    with pytest.raises(ValueError, match="prefix"):
        S3ContentAddressedArchive(client, "valid-bucket", key_prefix="raw/../private")
    with pytest.raises(ValueError, match="positive"):
        S3ContentAddressedArchive(client, "valid-bucket", maximum_object_bytes=0)
    with pytest.raises(ValueError, match="positive"):
        S3ContentAddressedArchive(client, "valid-bucket", operation_timeout_seconds=0)


def test_service_archive_keys_bind_prefix_and_digest() -> None:
    digest = hashlib.sha256(b"exact archive bytes").hexdigest()
    object_key = f"sha256/{digest[:2]}/{digest[2:4]}/{digest}"
    service_key = service_key_for_object(object_key, key_prefix="tenant/raw/")

    assert service_key == f"tenant/raw/{object_key}"
    assert (
        object_key_from_service_key(
            service_key,
            key_prefix="/tenant/raw",
            expected_sha256=digest,
        )
        == object_key
    )
    with pytest.raises(ArchiveIntegrityError, match="prefix"):
        object_key_from_service_key(service_key, key_prefix="other/raw")
    with pytest.raises(ArchiveIntegrityError, match="disagree"):
        object_key_from_service_key(
            service_key,
            key_prefix="tenant/raw",
            expected_sha256="0" * 64,
        )


def test_s3_stalled_sdk_close_cannot_keep_python_process_alive(tmp_path: Path) -> None:
    fake_modules = tmp_path / "fake-modules"
    fake_modules.mkdir()
    (fake_modules / "boto3.py").write_text(
        textwrap.dedent(
            """
            import threading

            class Client:
                def put_object(self, **kwargs):
                    kwargs["Body"].read()
                    return {"ETag": '"stored"'}

                def close(self):
                    threading.Event().wait()

            def client(*args, **kwargs):
                return Client()
            """
        ),
        encoding="utf-8",
    )
    script = textwrap.dedent(
        f"""
        import asyncio
        from pathlib import Path

        from makolet.adapters.archive.s3 import (
            S3ContentAddressedArchive,
            S3UploadProcessConfig,
        )
        from makolet.domain.errors import ArchiveIntegrityError

        async def chunks():
            yield b"bounded process upload"

        async def main():
            archive = S3ContentAddressedArchive(
                None,
                "makolet-test",
                temporary_directory=Path({str(tmp_path / "spool")!r}),
                operation_timeout_seconds=0.1,
                verify_after_write=False,
                upload_process_config=S3UploadProcessConfig(
                    endpoint_url="https://s3.invalid",
                    region_name="us-east-1",
                    access_key_id="access",
                    secret_access_key="secret",
                    path_style=True,
                ),
            )
            try:
                await archive.put(chunks(), original_filename="price.xml")
            except ArchiveIntegrityError as error:
                assert "deadline" in str(error)
            else:
                raise AssertionError("stalled SDK close unexpectedly completed")

        asyncio.run(main())
        print("bounded-exit")
        """
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(fake_modules), environment.get("PYTHONPATH", ""))
    )

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and authored test script
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        env=environment,
        text=True,
        timeout=5,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "bounded-exit"
