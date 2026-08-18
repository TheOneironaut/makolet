from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import shutil
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import boto3
import pytest
from botocore.awsrequest import AWSResponse
from botocore.compat import HTTPHeaders
from botocore.config import Config
from botocore.exceptions import ClientError

from deployment import archive_backup
from makolet.adapters.archive.keys import key_for_digest
from makolet.adapters.filesystem_capacity import FileSystemCapacityGuard

_AUTHENTICATION_DOMAIN = b"makolet-raw-archive-manifest-hmac-sha256-v1\0"
_AUTHENTICATION_PREFIX = b"makolet-raw-archive-manifest-hmac-sha256-v1:"
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

_WINDOWS_DOCKER_LAUNCHER_SOURCE = r"""
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;

public static class DockerShim
{
    private static string Quote(string value)
    {
        return "\"" + value.Replace("\"", "\\\"") + "\"";
    }

    public static int Main(string[] arguments)
    {
        string python = Environment.GetEnvironmentVariable("MAKOLET_DOCKER_SHIM_PYTHON");
        if (String.IsNullOrEmpty(python))
        {
            Console.Error.WriteLine("MAKOLET_DOCKER_SHIM_PYTHON is required");
            return 90;
        }
        string executable = Assembly.GetExecutingAssembly().Location;
        string handler = Path.ChangeExtension(executable, ".py");
        string argumentPath = Path.Combine(
            Path.GetDirectoryName(executable),
            ".docker-arguments-" + Guid.NewGuid().ToString("N")
        );
        using (StreamWriter writer = new StreamWriter(
            argumentPath,
            false,
            new UTF8Encoding(false)
        ))
        {
            foreach (string argument in arguments)
            {
                writer.WriteLine(Convert.ToBase64String(Encoding.UTF8.GetBytes(argument)));
            }
        }
        try
        {
            ProcessStartInfo start = new ProcessStartInfo();
            start.FileName = python;
            start.Arguments = Quote(handler) + " " + Quote(argumentPath);
            start.UseShellExecute = false;
            start.CreateNoWindow = true;
            start.RedirectStandardOutput = true;
            start.RedirectStandardError = true;
            using (Process process = Process.Start(start))
            {
                string standardOutput = process.StandardOutput.ReadToEnd();
                string standardError = process.StandardError.ReadToEnd();
                process.WaitForExit();
                Console.Out.Write(standardOutput);
                Console.Error.Write(standardError);
                return process.ExitCode;
            }
        }
        finally
        {
            if (File.Exists(argumentPath))
            {
                File.Delete(argumentPath);
            }
        }
    }
}
"""

_WINDOWS_DOCKER_HANDLER_SOURCE = r"""from __future__ import annotations

import base64
import os
import sys
import time
from pathlib import Path


def read_arguments() -> list[str]:
    argument_path = Path(sys.argv[1])
    try:
        return [
            base64.b64decode(line).decode("utf-8")
            for line in argument_path.read_text(encoding="utf-8").splitlines()
        ]
    finally:
        argument_path.unlink(missing_ok=True)


def contains(arguments: list[str], *needle: str) -> bool:
    width = len(needle)
    return any(
        tuple(arguments[offset : offset + width]) == needle
        for offset in range(len(arguments))
    )


def append_line(path: Path, value: str) -> None:
    lock_path = path.with_name(f"{path.name}.append-lock")
    deadline = time.monotonic() + 5
    while True:
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(descriptor)
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.005)
    try:
        with path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(f"{value}\n")
    finally:
        lock_path.unlink(missing_ok=True)


def touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def lock_owner(arguments: list[str]) -> str:
    marker = "com.makolet.archive-backup-lock.owner="
    return next(argument[len(marker) :] for argument in arguments if argument.startswith(marker))


arguments = read_arguments()
mode = os.environ["MAKOLET_DOCKER_SHIM_MODE"]
overlap_state_value = os.environ.get("MAKOLET_OVERLAP_STATE")
if overlap_state_value is not None:
    state = Path(overlap_state_value)
    capture = Path(os.environ["MAKOLET_OVERLAP_CAPTURE"])
else:
    state = None
    capture = Path(os.environ["MAKOLET_CAPTURE"])
append_line(capture, " ".join(arguments))

if arguments[:2] == ["volume", "create"]:
    owner = lock_owner(arguments)
    lock_name = arguments[-1]
    if mode == "overlap":
        assert state is not None
        lock_directory = state / "lock"
        lock_directory.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(
                lock_directory / "claim",
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
            (lock_directory / "owner").write_text(owner, encoding="utf-8")
    else:
        Path(f"{capture}.lock").write_text(owner, encoding="utf-8")
    print(lock_name)
    raise SystemExit(0)

if arguments[:2] == ["volume", "inspect"]:
    if mode == "overlap":
        assert state is not None
        owner_path = state / "lock" / "owner"
    else:
        owner_path = Path(f"{capture}.lock")
    if owner_path.is_file():
        print(owner_path.read_text(encoding="utf-8"), end="")
    raise SystemExit(0)

if arguments[:2] == ["volume", "rm"]:
    if mode == "overlap":
        assert state is not None
        lock_directory = state / "lock"
        for name in ("owner", "claim"):
            (lock_directory / name).unlink(missing_ok=True)
        lock_directory.rmdir()
    else:
        Path(f"{capture}.lock").unlink(missing_ok=True)
    print(arguments[-1])
    raise SystemExit(0)

if contains(arguments, "ps", "--status", "running", "--status", "restarting", "--services"):
    if mode == "overlap":
        assert state is not None
        if (state / "worker-restarting").is_file() and not (state / "worker-stopped").exists():
            print("worker")
    elif mode != "capture-backup" and not Path(f"{capture}.worker-stopped").exists():
        print("worker")
    raise SystemExit(0)

if contains(arguments, "stop", "worker"):
    if mode == "phase-stall" and os.environ["MAKOLET_STALL_PHASE"] == "stop":
        time.sleep(10)
    if mode == "overlap":
        assert state is not None
        touch(state / "worker-stopped")
    elif mode != "capture-backup":
        touch(Path(f"{capture}.worker-stopped"))
    raise SystemExit(0)

if contains(arguments, "container", "rm", "--force"):
    if mode == "phase-stall":
        container_state = Path(os.environ["MAKOLET_CONTAINER_STATE"])
        if os.environ["MAKOLET_STALL_PHASE"] == "cleanup":
            time.sleep(10)
        container_state.unlink(missing_ok=True)
    raise SystemExit(0)

if contains(arguments, "up", "-d", "--wait", "worker"):
    if mode == "restart-stall":
        time.sleep(10)
    if mode == "phase-stall" and Path(os.environ["MAKOLET_CONTAINER_STATE"]).exists():
        raise SystemExit(9)
    if mode == "overlap":
        assert state is not None
        (state / "worker-stopped").unlink(missing_ok=True)
        touch(state / "worker-restarted")
    elif mode != "capture-backup":
        Path(f"{capture}.worker-stopped").unlink(missing_ok=True)
    raise SystemExit(0)

if contains(arguments, "run", "--rm"):
    if mode == "restart-stall":
        raise SystemExit(7)
    if mode == "phase-stall":
        container_state = Path(os.environ["MAKOLET_CONTAINER_STATE"])
        stall_phase = os.environ["MAKOLET_STALL_PHASE"]
        if stall_phase in {"run", "cleanup"}:
            container_state.write_text("live", encoding="utf-8")
        if stall_phase == "run":
            time.sleep(10)
        if stall_phase == "cleanup":
            raise SystemExit(7)
    if mode == "overlap":
        assert state is not None
        touch(state / "backup-held")
        time.sleep(4)
    print('{"status":"backed_up"}')
    raise SystemExit(0)

raise SystemExit(0)
"""


def _compile_windows_docker_shim(fake_bin: Path) -> None:
    source_path = fake_bin / "docker.cs"
    source_path.write_text(_WINDOWS_DOCKER_LAUNCHER_SOURCE, encoding="utf-8")
    (fake_bin / "docker.py").write_text(_WINDOWS_DOCKER_HANDLER_SOURCE, encoding="utf-8")
    windows_directory = Path(os.environ.get("WINDIR", "C:/Windows"))
    compiler = next(
        (
            candidate
            for candidate in (
                windows_directory / "Microsoft.NET/Framework64/v4.0.30319/csc.exe",
                windows_directory / "Microsoft.NET/Framework/v4.0.30319/csc.exe",
            )
            if candidate.is_file()
        ),
        None,
    )
    if compiler is None:
        pytest.skip("the Windows C# compiler is unavailable")
    completed = subprocess.run(  # noqa: S603 - controlled local compiler harness
        [str(compiler), "/nologo", f"/out:{fake_bin / 'docker.exe'}", str(source_path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr


def _configure_windows_docker_shim(
    environment: dict[str, str],
    fake_bin: Path,
    *,
    mode: str,
) -> None:
    _compile_windows_docker_shim(fake_bin)
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["MAKOLET_DOCKER_SHIM_PYTHON"] = sys.executable
    environment["MAKOLET_DOCKER_SHIM_MODE"] = mode


class _ObjectBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.closed = False

    def iter_chunks(self, *, chunk_size: int) -> Iterator[bytes]:
        for offset in range(0, len(self._payload), chunk_size):
            yield self._payload[offset : offset + chunk_size]

    def close(self) -> None:
        self.closed = True


class _ListingRawBody:
    def __init__(self, chunks: tuple[bytes, ...]) -> None:
        self._chunks = chunks
        self.closed = False

    def stream(self, amount: int, *, decode_content: bool) -> Iterator[bytes]:
        assert amount > 0
        assert decode_content is False
        yield from self._chunks

    def close(self) -> None:
        self.closed = True


class _ExactBotocoreRawBody:
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


class _ExactBotocoreHttpSession:
    def __init__(self, responses: tuple[AWSResponse, ...]) -> None:
        self._responses = iter(responses)

    def send(self, request: Any) -> AWSResponse:
        assert request.stream_output is True
        return next(self._responses)

    def close(self) -> None:
        return


class _ListingHttpSession:
    def __init__(self, raw: _ListingRawBody, *, declared_length: str | None = None) -> None:
        headers = {} if declared_length is None else {"content-length": declared_length}
        self.response = SimpleNamespace(
            url="https://objects.example.test/raw?list-type=2",
            status_code=200,
            headers=headers,
            raw=raw,
        )

    def send(self, request: SimpleNamespace) -> SimpleNamespace:
        assert request.stream_output is True
        return self.response

    def close(self) -> None:
        return


class _SequenceListingHttpSession:
    def __init__(self, responses: tuple[SimpleNamespace, ...]) -> None:
        self._responses = iter(responses)
        self.authorization_headers: list[bytes] = []

    def send(self, request: Any) -> SimpleNamespace:
        assert request.stream_output is True
        authorization = request.headers.get("Authorization")
        assert isinstance(authorization, bytes)
        self.authorization_headers.append(authorization)
        return next(self._responses)

    def close(self) -> None:
        return


class _ClientEvents:
    def __init__(self) -> None:
        self.registered: list[tuple[str, object]] = []

    def register_first(self, name: str, handler: object) -> None:
        self.registered.append((name, handler))


class _ConfiguredS3Client:
    def __init__(self) -> None:
        self.events = _ClientEvents()
        self.meta = SimpleNamespace(events=self.events)
        self._endpoint = SimpleNamespace(http_session=object())


def test_s3_listing_transport_rejects_oversized_body_before_sdk_parse() -> None:
    raw = _ListingRawBody((b"12345", b"6789"))
    session = _ListingHttpSession(raw)
    request = SimpleNamespace(stream_output=False, context={})

    with pytest.raises(archive_backup.BackupError, match="ListObjectsV2 response exceeds"):
        archive_backup._bounded_s3_list_response(
            session,
            request,
            maximum_bytes=8,
        )

    assert request.stream_output is False
    assert raw.closed is True


def test_s3_listing_transport_preserves_one_bounded_signed_response() -> None:
    payload = b"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
    raw = _ListingRawBody((payload[:17], payload[17:]))
    session = _ListingHttpSession(raw, declared_length=str(len(payload)))
    request = SimpleNamespace(stream_output=False, context={})

    response = archive_backup._bounded_s3_list_response(
        session,
        request,
        maximum_bytes=len(payload),
    )

    assert response.content == payload
    assert response.status_code == 200
    assert request.stream_output is False
    assert raw.closed is True


@pytest.mark.parametrize(
    ("headers", "payload", "match"),
    [
        ({"content-encoding": "gzip"}, b"value", "content encoding"),
        ({"content-length": "invalid"}, b"value", "invalid content length"),
        ({"content-length": "4"}, b"value", "length is inconsistent"),
    ],
)
def test_s3_listing_transport_rejects_unsafe_response_metadata(
    headers: dict[str, str],
    payload: bytes,
    match: str,
) -> None:
    raw = _ListingRawBody((payload,))
    session = _ListingHttpSession(raw)
    session.response.headers = headers
    request = SimpleNamespace(stream_output=False, context={})

    with pytest.raises(archive_backup.BackupError, match=match):
        archive_backup._bounded_s3_list_response(session, request)

    assert request.stream_output is False
    assert raw.closed is True


def test_archive_client_installs_every_required_preparse_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ConfiguredS3Client()
    monkeypatch.setenv("MAKOLET_ENVIRONMENT", "development")
    monkeypatch.setenv("MAKOLET_S3_ENDPOINT", "https://objects.example.test")
    monkeypatch.setenv("MAKOLET_S3_REGION", "us-east-1")
    monkeypatch.setattr(boto3, "client", lambda *_args, **_kwargs: client)

    assert archive_backup._client() is client
    assert [event_name for event_name, _handler in client.events.registered] == [
        "before-send.s3.ListObjectsV2",
        "before-send.s3.GetObject",
        "before-send.s3.HeadObject",
        "before-send.s3.PutObject",
        "before-send.s3.CopyObject",
        "before-send.s3.DeleteObject",
    ]
    assert all(callable(handler) for _event_name, handler in client.events.registered)


@pytest.mark.parametrize(
    ("operation", "method_name", "status_code", "arguments"),
    [
        (
            "GetObject",
            "get_object",
            404,
            {"Bucket": "raw-archive", "Key": "raw/sha256/object"},
        ),
        (
            "HeadObject",
            "head_object",
            200,
            {"Bucket": "raw-archive", "Key": "raw/sha256/object"},
        ),
        (
            "PutObject",
            "put_object",
            200,
            {
                "Bucket": "raw-archive",
                "Key": "raw/restore-staging/object",
                "Body": b"value",
                "ContentLength": 5,
            },
        ),
        (
            "CopyObject",
            "copy_object",
            200,
            {
                "Bucket": "raw-archive",
                "Key": "raw/sha256/object",
                "CopySource": {"Bucket": "raw-archive", "Key": "raw/restore-staging/object"},
            },
        ),
        (
            "DeleteObject",
            "delete_object",
            204,
            {"Bucket": "raw-archive", "Key": "raw/restore-staging/object"},
        ),
    ],
)
def test_archive_transport_rejects_oversized_sibling_responses_before_sdk_parse(
    operation: str,
    method_name: str,
    status_code: int,
    arguments: dict[str, Any],
) -> None:
    raw = _ExactBotocoreRawBody(
        (b"x" * archive_backup._MAXIMUM_LIST_RESPONSE_BYTES, b"x"),
        fail_after_chunks=True,
    )
    client = boto3.client(
        "s3",
        endpoint_url="https://objects.example.test",
        region_name="us-east-1",
        aws_access_key_id="test-access-key",  # secret-scan: allow
        aws_secret_access_key="test-secret-key",  # secret-scan: allow
        config=Config(signature_version="s3v4", retries={"max_attempts": 0}),
    )
    endpoint: Any = vars(client)["_endpoint"]
    endpoint.http_session = _ExactBotocoreHttpSession(
        (
            AWSResponse(
                "https://objects.example.test/raw/object",
                status_code,
                HTTPHeaders(),
                raw,
            ),
        )
    )
    archive_backup._install_bounded_s3_transport(client)
    try:
        with pytest.raises(
            archive_backup.BackupError,
            match=rf"S3 {operation} response exceeds its byte limit",
        ):
            getattr(client, method_name)(**arguments)
    finally:
        client.close()

    assert raw.bytes_yielded == archive_backup._MAXIMUM_LIST_RESPONSE_BYTES + 1
    assert raw.closed is True


def test_archive_transport_preserves_head_object_representation_headers() -> None:
    represented_object_bytes = archive_backup._MAXIMUM_LIST_RESPONSE_BYTES * 4
    raw = _ExactBotocoreRawBody(())
    client = boto3.client(
        "s3",
        endpoint_url="https://objects.example.test",
        region_name="us-east-1",
        aws_access_key_id="test-access-key",  # secret-scan: allow
        aws_secret_access_key="test-secret-key",  # secret-scan: allow
        config=Config(signature_version="s3v4", retries={"max_attempts": 0}),
    )
    endpoint: Any = vars(client)["_endpoint"]
    endpoint.http_session = _ExactBotocoreHttpSession(
        (
            AWSResponse(
                "https://objects.example.test/raw/object",
                200,
                HTTPHeaders.from_dict(
                    {
                        "content-length": str(represented_object_bytes),
                        "content-encoding": "gzip",
                    }
                ),
                raw,
            ),
        )
    )
    archive_backup._install_bounded_s3_transport(client)
    try:
        response = client.head_object(Bucket="raw-archive", Key="raw/sha256/object")
    finally:
        client.close()

    assert response["ContentLength"] == represented_object_bytes
    assert response["ContentEncoding"] == "gzip"
    assert raw.bytes_yielded == 0
    assert raw.closed is True


def test_preparse_transport_retries_share_one_response_byte_budget() -> None:
    maximum_bytes = archive_backup._MAXIMUM_LIST_RESPONSE_BYTES
    first_payload = (
        b"<Error><Code>InternalError</Code><Message>"
        + b"x" * (maximum_bytes // 2)
        + b"</Message></Error>"
    )
    second_payload = (
        b"<?xml version='1.0' encoding='UTF-8'?>"
        b"<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>"
        b"<Name>"
        + b"y"
        * (maximum_bytes // 2)
        + b"</Name><Prefix>raw/sha256/</Prefix><KeyCount>0</KeyCount>"
        b"<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>"
        b"</ListBucketResult>"
    )
    first_raw = _ExactBotocoreRawBody((first_payload,))
    second_raw = _ExactBotocoreRawBody((second_payload,))
    client = boto3.client(
        "s3",
        endpoint_url="https://objects.example.test",
        region_name="us-east-1",
        aws_access_key_id="test-access-key",  # secret-scan: allow
        aws_secret_access_key="test-secret-key",  # secret-scan: allow
        config=Config(signature_version="s3v4", retries={"max_attempts": 1}),
    )
    endpoint: Any = vars(client)["_endpoint"]
    endpoint.http_session = _ExactBotocoreHttpSession(
        (
            AWSResponse(
                "https://objects.example.test/raw?list-type=2",
                500,
                HTTPHeaders.from_dict({"content-length": str(len(first_payload))}),
                first_raw,
            ),
            AWSResponse(
                "https://objects.example.test/raw?list-type=2",
                200,
                HTTPHeaders.from_dict({"content-length": str(len(second_payload))}),
                second_raw,
            ),
        )
    )

    def retry_once(attempts: int, **_kwargs: Any) -> int | bool:
        return 0 if attempts == 1 else False

    retry_handler: Any = retry_once
    client.meta.events.register_first(
        "needs-retry.s3.ListObjectsV2",
        retry_handler,
    )
    archive_backup._install_bounded_s3_transport(client)
    try:
        with pytest.raises(archive_backup.BackupError, match="response exceeds its byte limit"):
            client.list_objects_v2(
                Bucket="raw-archive",
                Prefix="raw/sha256/",
                MaxKeys=1_000,
            )
    finally:
        client.close()

    assert first_raw.bytes_yielded == len(first_payload)
    assert first_raw.closed is True
    assert second_raw.bytes_yielded == 0
    assert second_raw.closed is True


def test_preparse_transport_integrates_with_signed_botocore_listing() -> None:
    payload = (
        b"<?xml version='1.0' encoding='UTF-8'?>"
        b"<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>"
        b"<Name>raw-archive</Name><Prefix>raw/sha256/</Prefix><KeyCount>0</KeyCount>"
        b"<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>"
        b"</ListBucketResult>"
    )
    raw = _ListingRawBody((payload,))
    session = _ListingHttpSession(raw, declared_length=str(len(payload)))
    client = boto3.client(
        "s3",
        endpoint_url="https://objects.example.test",
        region_name="us-east-1",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
        config=Config(signature_version="s3v4", retries={"max_attempts": 0}),
    )
    endpoint: Any = vars(client)["_endpoint"]
    endpoint.http_session = session
    archive_backup._install_bounded_s3_transport(client)
    try:
        response = client.list_objects_v2(
            Bucket="raw-archive",
            Prefix="raw/sha256/",
            MaxKeys=1_000,
        )
    finally:
        client.close()

    assert response["IsTruncated"] is False
    assert response["KeyCount"] == 0
    assert raw.closed is True


def test_preparse_transport_preserves_signed_botocore_retry() -> None:
    retry_payload = b"<Error><Code>InternalError</Code><Message>retry</Message></Error>"
    success_payload = (
        b"<?xml version='1.0' encoding='UTF-8'?>"
        b"<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>"
        b"<Name>raw-archive</Name><Prefix>raw/sha256/</Prefix><KeyCount>0</KeyCount>"
        b"<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>"
        b"</ListBucketResult>"
    )
    retry_raw = _ListingRawBody((retry_payload,))
    success_raw = _ListingRawBody((success_payload,))
    session = _SequenceListingHttpSession(
        (
            SimpleNamespace(
                url="https://objects.example.test/raw?list-type=2",
                status_code=500,
                headers={"content-length": str(len(retry_payload))},
                raw=retry_raw,
            ),
            SimpleNamespace(
                url="https://objects.example.test/raw?list-type=2",
                status_code=200,
                headers={"content-length": str(len(success_payload))},
                raw=success_raw,
            ),
        )
    )
    client = boto3.client(
        "s3",
        endpoint_url="https://objects.example.test",
        region_name="us-east-1",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
        config=Config(signature_version="s3v4", retries={"max_attempts": 1}),
    )
    endpoint: Any = vars(client)["_endpoint"]
    endpoint.http_session = session
    archive_backup._install_bounded_s3_transport(client)
    try:
        response = client.list_objects_v2(
            Bucket="raw-archive",
            Prefix="raw/sha256/",
            MaxKeys=1_000,
        )
    finally:
        client.close()

    assert response["IsTruncated"] is False
    assert len(session.authorization_headers) == 2
    assert all(header.startswith(b"AWS4-HMAC-SHA256 ") for header in session.authorization_headers)
    assert retry_raw.closed is True
    assert success_raw.closed is True


class _Paginator:
    def __init__(self, client: _ArchiveClient) -> None:
        self._client = client

    def paginate(self, **arguments: Any) -> Iterator[dict[str, Any]]:
        prefix = arguments["Prefix"]
        yield {
            "Contents": [
                {"Key": key, "Size": len(payload)}
                for key, payload in sorted(self._client.objects.items())
                if key.startswith(prefix)
            ]
        }


class _ArchiveClient:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)

    def get_paginator(self, operation: str) -> Any:
        assert operation == "list_objects_v2"
        return _Paginator(self)

    def get_object(self, **arguments: str) -> dict[str, Any]:
        payload = self.objects[arguments["Key"]]
        return {
            "Body": _ObjectBody(payload),
            "ETag": f'"{hashlib.md5(payload, usedforsecurity=False).hexdigest()}"',
            "Metadata": {"sha256": hashlib.sha256(payload).hexdigest()},
        }

    def head_object(self, **arguments: str) -> dict[str, Any]:
        key = arguments["Key"]
        if key not in self.objects:
            response: Any = {
                "Error": {"Code": "NoSuchKey"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            }
            raise ClientError(response, "HeadObject")
        payload = self.objects[key]
        return {
            "ContentLength": len(payload),
            "ETag": f'"{hashlib.md5(payload, usedforsecurity=False).hexdigest()}"',
            "Metadata": {"sha256": hashlib.sha256(payload).hexdigest()},
        }

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        key = arguments["Key"]
        assert arguments["IfNoneMatch"] == "*"
        if key in self.objects:
            response: Any = {
                "Error": {"Code": "PreconditionFailed"},
                "ResponseMetadata": {"HTTPStatusCode": 412},
            }
            raise ClientError(
                response,
                "PutObject",
            )
        payload = arguments["Body"].read()
        assert len(payload) == arguments["ContentLength"]
        self.objects[key] = payload
        return {}

    def copy_object(self, **arguments: Any) -> None:
        key = arguments["Key"]
        source = arguments["CopySource"]
        assert isinstance(source, dict)
        source_payload = self.objects[source["Key"]]
        source_etag = f'"{hashlib.md5(source_payload, usedforsecurity=False).hexdigest()}"'
        assert arguments["CopySourceIfMatch"] == source_etag
        assert arguments["IfNoneMatch"] == "*"
        if key in self.objects:
            response: Any = {
                "Error": {"Code": "PreconditionFailed"},
                "ResponseMetadata": {"HTTPStatusCode": 412},
            }
            raise ClientError(response, "CopyObject")
        self.objects[key] = source_payload

    def delete_object(self, **arguments: str) -> None:
        self.objects.pop(arguments["Key"], None)


def _configured_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, bytes]:
    key_directory = tmp_path / "protected"
    key_directory.mkdir()
    key_file = key_directory / "archive-backup-auth.key"
    key = bytes(range(32))
    key_file.write_bytes(key)
    key_file.chmod(0o600)
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE", str(key_file))
    monkeypatch.setenv("MAKOLET_S3_ENDPOINT", "https://s3.invalid")
    monkeypatch.setenv("MAKOLET_S3_REGION", "us-east-1")
    monkeypatch.setenv("MAKOLET_S3_BUCKET", "raw-archive")
    monkeypatch.setenv("MAKOLET_S3_KEY_PREFIX", "raw")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    def authoritative_rows(
        *,
        key_prefix: str,
        maximum_objects: int,
        deadline: archive_backup._OperationDeadline | None = None,
    ) -> Iterator[dict[str, Any]]:
        del deadline
        client = archive_backup._client()
        objects = getattr(client, "objects", None)
        if not isinstance(objects, dict):
            return archive_backup._object_rows(
                client,
                "raw-archive",
                key_prefix,
                maximum_objects,
            )
        prefix = f"{key_prefix}/sha256/"
        rows = (
            {
                "key": service_key,
                "sha256": archive_backup._content_digest_for_service_key(
                    service_key,
                    key_prefix,
                ),
                "size": len(payload),
            }
            for service_key, payload in sorted(objects.items())
            if service_key.startswith(prefix)
        )
        return iter(rows)

    monkeypatch.setattr(
        archive_backup,
        "_authoritative_object_rows",
        authoritative_rows,
        raising=False,
    )
    return archive_backup._destination(str(tmp_path / "backup")), key_file, key


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://objects.example.test:9000",
        "http://seaweedfs:8333",
        "http://seaweedfs.example.test:8333",
        "http://localhost:8333",
    ],
)
def test_archive_tool_rejects_remote_plaintext_in_production_before_client_creation(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
) -> None:
    monkeypatch.setenv("MAKOLET_ENVIRONMENT", "production")
    monkeypatch.setenv("MAKOLET_S3_ENDPOINT", endpoint)
    monkeypatch.setenv("MAKOLET_S3_ALLOW_INSECURE_LOCAL", "true")
    monkeypatch.setenv("MAKOLET_S3_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    def fail_if_connected(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("unsafe S3 endpoint reached boto3")

    monkeypatch.setattr(boto3, "client", fail_if_connected)

    with pytest.raises(archive_backup.BackupError, match="invalid or unsafe"):
        archive_backup._client()


@pytest.mark.parametrize(
    ("endpoint", "allow_insecure_local"),
    [
        ("https://objects.example.test:9000", "false"),
        ("http://127.0.0.1:8333", "true"),
        ("http://[::1]:8333", "true"),
    ],
)
def test_archive_tool_preserves_verified_remote_and_explicit_local_transports(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
    allow_insecure_local: str,
) -> None:
    captured: dict[str, object] = {}
    client = _ConfiguredS3Client()
    monkeypatch.setenv("MAKOLET_ENVIRONMENT", "production")
    monkeypatch.setenv("MAKOLET_S3_ENDPOINT", endpoint)
    monkeypatch.setenv("MAKOLET_S3_ALLOW_INSECURE_LOCAL", allow_insecure_local)
    monkeypatch.setenv("MAKOLET_S3_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "operator-access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "operator-secret-key")
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example.test:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example.test:8080")
    monkeypatch.delenv("NO_PROXY", raising=False)

    def create_client(service: str, **arguments: object) -> object:
        captured["service"] = service
        captured.update(arguments)
        return client

    monkeypatch.setattr(boto3, "client", create_client)

    assert archive_backup._client() is client
    assert captured["service"] == "s3"
    assert captured["endpoint_url"] == endpoint
    assert getattr(captured["config"], "signature_version", None) == "s3v4"
    expected_proxies: dict[str, str] | None = {} if endpoint.startswith("http://") else None
    assert getattr(captured["config"], "proxies", None) == expected_proxies


def test_archive_tool_rejects_missing_production_credentials_before_client_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAKOLET_ENVIRONMENT", "production")
    monkeypatch.setenv("MAKOLET_S3_ENDPOINT", "https://objects.example.test:9000")
    monkeypatch.setenv("MAKOLET_S3_ALLOW_INSECURE_LOCAL", "false")
    monkeypatch.setenv("MAKOLET_S3_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    def fail_if_connected(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("credential-less production S3 reached boto3")

    monkeypatch.setattr(boto3, "client", fail_if_connected)

    with pytest.raises(archive_backup.BackupError, match="credentials"):
        archive_backup._client()


def test_archive_tool_requires_an_explicit_local_plaintext_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAKOLET_ENVIRONMENT", "production")
    monkeypatch.setenv("MAKOLET_S3_ENDPOINT", "http://seaweedfs:8333")
    monkeypatch.delenv("MAKOLET_S3_ALLOW_INSECURE_LOCAL", raising=False)
    monkeypatch.setenv("MAKOLET_S3_REGION", "us-east-1")

    def fail_if_connected(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("unsafe S3 endpoint reached boto3")

    monkeypatch.setattr(boto3, "client", fail_if_connected)

    with pytest.raises(archive_backup.BackupError, match="invalid or unsafe"):
        archive_backup._client()


def test_archive_tool_preserves_development_remote_http_compatibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ConfiguredS3Client()
    monkeypatch.setenv("MAKOLET_ENVIRONMENT", "development")
    monkeypatch.setenv("MAKOLET_S3_ENDPOINT", "http://objects.example.test:9000")
    monkeypatch.delenv("MAKOLET_S3_ALLOW_INSECURE_LOCAL", raising=False)
    monkeypatch.setenv("MAKOLET_S3_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr(boto3, "client", lambda *_args, **_kwargs: client)

    assert archive_backup._client() is client


def _manifest(*, objects: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "format": "makolet-raw-archive-backup-v2",
        "created_at": "2026-08-16T00:00:00+00:00",
        "bucket": "raw-archive",
        "key_prefix": "raw",
        "objects": [] if objects is None else objects,
    }


def _write_authenticated_manifest(
    destination: Path,
    key: bytes,
    manifest: dict[str, Any],
) -> bytes:
    payload = archive_backup._canonical_manifest_payload(manifest)
    digest = hashlib.sha256(payload).hexdigest()
    (destination / "manifest.json").write_bytes(payload)
    (destination / "manifest.json.sha256").write_bytes(f"{digest}  manifest.json\n".encode("ascii"))
    authentication = hmac.new(
        key,
        _AUTHENTICATION_DOMAIN + payload,
        hashlib.sha256,
    ).hexdigest()
    (destination / "manifest.json.hmac-sha256").write_bytes(
        _AUTHENTICATION_PREFIX + authentication.encode("ascii") + b"\n"
    )
    return payload


def test_authenticated_backup_verify_and_create_only_restore_are_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    first_payload = b"independently authored archive object one"
    second_payload = b"independently authored archive object two"
    first_digest = hashlib.sha256(first_payload).hexdigest()
    second_digest = hashlib.sha256(second_payload).hexdigest()
    first_key = f"raw/{key_for_digest(first_digest)}"
    second_key = f"raw/{key_for_digest(second_digest)}"
    client = _ArchiveClient({first_key: first_payload, second_key: second_payload})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)

    result = archive_backup.backup(destination)

    manifest_payload = (destination / "manifest.json").read_bytes()
    expected_authentication = hmac.new(
        key,
        _AUTHENTICATION_DOMAIN + manifest_payload,
        hashlib.sha256,
    ).hexdigest()
    assert result["authentication"] == "hmac-sha256-v1"
    assert (destination / "manifest.json.hmac-sha256").read_bytes() == (
        _AUTHENTICATION_PREFIX + expected_authentication.encode("ascii") + b"\n"
    )
    assert archive_backup.verify(destination) == {"status": "verified", "objects": 2}

    client.objects.clear()
    assert archive_backup.restore(destination, confirmed_bucket="raw-archive") == {
        "status": "restored",
        "objects": 2,
        "objects_created": 2,
    }
    assert client.objects == {first_key: first_payload, second_key: second_payload}
    assert archive_backup.restore(destination, confirmed_bucket="raw-archive") == {
        "status": "restored",
        "objects": 2,
        "objects_created": 0,
    }


def test_restore_never_publishes_bytes_replaced_after_backup_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    verified_payload = b"independently authored verified object"
    replacement_payload = b"attacker-controlled replacement bytes!"
    assert len(replacement_payload) == len(verified_payload)
    digest = hashlib.sha256(verified_payload).hexdigest()
    service_key = f"raw/{key_for_digest(digest)}"
    object_path = destination / "objects" / digest
    object_path.write_bytes(verified_payload)
    _write_authenticated_manifest(
        destination,
        key,
        _manifest(
            objects=[
                {
                    "key": service_key,
                    "sha256": digest,
                    "size": len(verified_payload),
                }
            ]
        ),
    )
    client = _ArchiveClient({})

    def replace_after_verification() -> _ArchiveClient:
        object_path.write_bytes(replacement_payload)
        return client

    monkeypatch.setattr(archive_backup, "_client", replace_after_verification)

    with pytest.raises(archive_backup.BackupError):
        archive_backup.restore(destination, confirmed_bucket="raw-archive")

    assert service_key not in client.objects


def test_archive_backup_rejects_aggregate_inventory_before_downloading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, _key = _configured_destination(tmp_path, monkeypatch)
    first_payload = b"first"
    second_payload = b"second"
    client = _ArchiveClient(
        {
            f"raw/{key_for_digest(hashlib.sha256(first_payload).hexdigest())}": first_payload,
            f"raw/{key_for_digest(hashlib.sha256(second_payload).hexdigest())}": second_payload,
        }
    )
    monkeypatch.setattr(archive_backup, "_client", lambda: client)
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES", "10")
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES", "0")

    with pytest.raises(archive_backup.BackupError, match="aggregate byte limit"):
        archive_backup.backup(destination)

    assert list((destination / "objects").iterdir()) == []


def test_archive_backup_rejects_listing_omission_against_database_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, _key = _configured_destination(tmp_path, monkeypatch)
    first_payload = b"first authoritative object"
    second_payload = b"second authoritative object"
    first_digest = hashlib.sha256(first_payload).hexdigest()
    second_digest = hashlib.sha256(second_payload).hexdigest()
    first_key = f"raw/{key_for_digest(first_digest)}"
    second_key = f"raw/{key_for_digest(second_digest)}"

    class IncompletePaginator:
        def paginate(self, **_arguments: str) -> Iterator[dict[str, Any]]:
            yield {"Contents": [{"Key": first_key, "Size": len(first_payload)}]}

    class IncompleteClient(_ArchiveClient):
        def get_paginator(self, operation: str) -> IncompletePaginator:
            assert operation == "list_objects_v2"
            return IncompletePaginator()

    client = IncompleteClient({first_key: first_payload, second_key: second_payload})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)
    monkeypatch.setattr(
        archive_backup,
        "_authoritative_object_rows",
        lambda **_kwargs: iter(
            (
                {"key": first_key, "sha256": first_digest, "size": len(first_payload)},
                {"key": second_key, "sha256": second_digest, "size": len(second_payload)},
            )
        ),
        raising=False,
    )

    with pytest.raises(archive_backup.BackupError, match="authoritative PostgreSQL inventory"):
        archive_backup.backup(destination)

    assert not (destination / "manifest.json").exists()
    assert list((destination / "objects").iterdir()) == []


@pytest.mark.asyncio
async def test_archive_backup_reads_a_bounded_ordered_postgresql_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"authoritative inventory bytes"
    digest = hashlib.sha256(payload).hexdigest()
    object_key = key_for_digest(digest)
    captured: dict[str, object] = {}

    class FakeResult:
        def __aiter__(self) -> FakeResult:
            return self

        async def __anext__(self) -> SimpleNamespace:
            if captured.get("row_yielded"):
                raise StopAsyncIteration
            captured["row_yielded"] = True
            return SimpleNamespace(
                object_key=object_key,
                content_sha256=digest,
                content_length=len(payload),
            )

    class FakeConnection:
        async def __aenter__(self) -> FakeConnection:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        def begin(self) -> FakeTransaction:
            return FakeTransaction()

        async def execute(self, statement: object, parameters: object) -> None:
            captured["timeout_statement"] = str(statement)
            captured["timeout_parameters"] = parameters

        async def stream(self, statement: object, parameters: object) -> FakeResult:
            captured["statement"] = str(statement)
            captured["parameters"] = parameters
            return FakeResult()

    class FakeTransaction:
        async def __aenter__(self) -> FakeTransaction:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    class FakeEngine:
        def connect(self) -> FakeConnection:
            return FakeConnection()

    class FakeDatabase:
        engine = FakeEngine()

        async def dispose(self) -> None:
            captured["disposed"] = True

    class FakeSettings:
        def database_dsn(self) -> str:
            return "postgresql://inventory.invalid/makolet"

    database = FakeDatabase()
    monkeypatch.setattr(archive_backup, "load_settings", FakeSettings)
    monkeypatch.setattr(
        "deployment.archive_backup.Database.unpooled_from_url",
        lambda *_args, **_kwargs: database,
    )

    rows = await archive_backup._query_authoritative_object_rows(
        key_prefix="raw",
        maximum_objects=5,
    )

    assert rows == (
        {
            "key": f"raw/{object_key}",
            "sha256": digest,
            "size": len(payload),
        },
    )
    assert "FROM raw_archive_objects" in str(captured["statement"])
    assert captured["parameters"] == {"row_limit": 6}
    assert "statement_timeout" in str(captured["timeout_statement"])
    timeout_parameters = captured["timeout_parameters"]
    assert isinstance(timeout_parameters, dict)
    assert str(timeout_parameters["timeout"]).endswith("ms")
    assert captured["disposed"] is True


def test_archive_listing_rejects_unique_empty_pages_without_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EmptyPagePaginator:
        def paginate(self, **_arguments: str) -> Iterator[dict[str, Any]]:
            for index in range(4):
                yield {
                    "Contents": [],
                    "IsTruncated": True,
                    "NextContinuationToken": f"unique-token-{index}",
                }

    class EmptyPageClient:
        def get_paginator(self, operation: str) -> EmptyPagePaginator:
            assert operation == "list_objects_v2"
            return EmptyPagePaginator()

    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MAX_NO_PROGRESS_PAGES", "2")

    with pytest.raises(archive_backup.BackupError, match="without object progress"):
        list(archive_backup._object_rows(EmptyPageClient(), "bucket", "raw", 10))


def test_archive_listing_enforces_page_request_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PagedPaginator:
        def paginate(self, **_arguments: Any) -> Iterator[dict[str, Any]]:
            for index in range(3):
                digest = f"{index:064x}"
                yield {
                    "Contents": [
                        {
                            "Key": f"raw/{key_for_digest(digest)}",
                            "Size": 0,
                        }
                    ]
                }

    class PagedClient:
        def get_paginator(self, operation: str) -> PagedPaginator:
            assert operation == "list_objects_v2"
            return PagedPaginator()

    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MAX_LIST_PAGES", "2")

    with pytest.raises(archive_backup.BackupError, match="page/request limit"):
        list(archive_backup._object_rows(PagedClient(), "bucket", "raw", 10))


def test_archive_listing_enforces_total_operation_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OnePagePaginator:
        def paginate(self, **_arguments: Any) -> Iterator[dict[str, Any]]:
            yield {"Contents": []}

    class OnePageClient:
        def get_paginator(self, operation: str) -> OnePagePaginator:
            assert operation == "list_objects_v2"
            return OnePagePaginator()

    monotonic_values = iter((100.0, 102.0))
    monkeypatch.setattr(
        "deployment.archive_backup.time.monotonic",
        lambda: next(monotonic_values, 102.0),
    )
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_LIST_TIMEOUT_SECONDS", "1")

    with pytest.raises(archive_backup.BackupError, match="total operation deadline"):
        list(archive_backup._object_rows(OnePageClient(), "bucket", "raw", 10))


def test_archive_listing_deadline_aborts_a_blocked_client() -> None:
    released = threading.Event()

    class StalledPaginator:
        def paginate(self, **_arguments: Any) -> Iterator[dict[str, Any]]:
            released.wait(5)
            yield {"Contents": []}

    class StalledClient:
        close_count = 0

        def get_paginator(self, operation: str) -> StalledPaginator:
            assert operation == "list_objects_v2"
            return StalledPaginator()

        def close(self) -> None:
            self.close_count += 1
            released.set()

    client = StalledClient()
    started_at = time.monotonic()
    deadline = archive_backup._OperationDeadline(
        work_deadline=started_at + 0.05,
        cleanup_deadline=started_at + 0.5,
    )

    with pytest.raises(archive_backup.BackupError, match="total operation deadline"):
        list(
            archive_backup._object_rows(
                client,
                "bucket",
                "raw",
                10,
                deadline=deadline,
            )
        )

    assert time.monotonic() - started_at < 1
    assert client.close_count >= 1
    assert released.is_set()


def test_archive_body_deadline_closes_the_stream_and_client(tmp_path: Path) -> None:
    released = threading.Event()

    class StalledBody:
        closed = False

        def iter_chunks(self, *, chunk_size: int) -> Iterator[bytes]:
            del chunk_size
            released.wait(5)
            yield from ()

        def close(self) -> None:
            self.closed = True
            released.set()

    body = StalledBody()

    class StalledClient:
        close_count = 0

        def get_object(self, **_arguments: str) -> dict[str, object]:
            return {"Body": body, "Metadata": {}}

        def close(self) -> None:
            self.close_count += 1
            body.close()

    client = StalledClient()
    objects = tmp_path / "objects"
    objects.mkdir()
    capacity = FileSystemCapacityGuard(
        objects,
        minimum_free_bytes=0,
        coordination_directory=tmp_path,
    )
    started_at = time.monotonic()
    deadline = archive_backup._OperationDeadline(
        work_deadline=started_at + 0.05,
        cleanup_deadline=started_at + 0.5,
    )

    with (
        pytest.raises(archive_backup.BackupError, match="total operation deadline"),
        deadline.activate(),
        deadline.track(client),
    ):
        archive_backup._download(
            client,
            "bucket",
            "raw/sha256/aa/" + "a" * 62,
            1,
            objects,
            1024,
            capacity,
            deadline,
        )

    assert time.monotonic() - started_at < 1
    assert body.closed
    assert client.close_count >= 1
    assert list(objects.iterdir()) == []


def test_archive_deadline_returns_when_a_blocking_call_ignores_abort() -> None:
    blocked = threading.Event()

    class UncooperativeResource:
        close_count = 0

        def close(self) -> None:
            self.close_count += 1

    resource = UncooperativeResource()
    started_at = time.monotonic()
    deadline = archive_backup._OperationDeadline(
        work_deadline=started_at + 0.05,
        cleanup_deadline=started_at + 0.2,
    )

    with (
        pytest.raises(archive_backup.BackupError, match="total operation deadline"),
        deadline.activate(),
        deadline.track(resource),
    ):
        deadline.run(lambda: blocked.wait(1))

    assert time.monotonic() - started_at < 0.5
    assert resource.close_count >= 1


def test_archive_child_deadline_inherits_the_parent_cleanup_grace() -> None:
    started_at = time.monotonic()
    parent = archive_backup._OperationDeadline(
        work_deadline=started_at + 3_600,
        cleanup_deadline=started_at + 3_630,
    )

    child = parent.child(300, description="S3 object listing")

    assert child.work_deadline == pytest.approx(started_at + 300, abs=0.1)
    assert child.cleanup_deadline - child.work_deadline == pytest.approx(30)


@pytest.mark.asyncio
async def test_archive_inventory_deadline_cancels_postgres_and_disposes_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeTransaction:
        async def __aenter__(self) -> FakeTransaction:
            return self

        async def __aexit__(self, *_args: object) -> None:
            captured["transaction_closed"] = True

    class FakeConnection:
        async def __aenter__(self) -> FakeConnection:
            return self

        async def __aexit__(self, *_args: object) -> None:
            captured["connection_closed"] = True

        def begin(self) -> FakeTransaction:
            return FakeTransaction()

        async def execute(self, statement: object, parameters: object) -> None:
            captured["timeout_statement"] = str(statement)
            captured["timeout_parameters"] = parameters

        async def stream(self, *_args: object, **_kwargs: object) -> object:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    class FakeEngine:
        def connect(self) -> FakeConnection:
            return FakeConnection()

    class FakeDatabase:
        engine = FakeEngine()

        async def dispose(self) -> None:
            captured["disposed"] = True

    class FakeSettings:
        def database_dsn(self) -> str:
            return "postgresql://inventory.invalid/makolet"

    monkeypatch.setattr(archive_backup, "load_settings", FakeSettings)
    monkeypatch.setattr(
        "deployment.archive_backup.Database.unpooled_from_url",
        lambda *_args, **_kwargs: FakeDatabase(),
    )
    started_at = time.monotonic()
    deadline = archive_backup._OperationDeadline(
        work_deadline=started_at + 0.05,
        cleanup_deadline=started_at + 0.5,
    )

    with pytest.raises(archive_backup.BackupError, match="total operation deadline"):
        await archive_backup._query_authoritative_object_rows(
            key_prefix="raw",
            maximum_objects=5,
            deadline=deadline,
        )

    assert time.monotonic() - started_at < 1
    assert "statement_timeout" in str(captured["timeout_statement"])
    assert captured["transaction_closed"] is True
    assert captured["connection_closed"] is True
    assert captured["disposed"] is True


def test_archive_backup_accounts_for_exact_manifest_and_sidecars_before_downloading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, _key = _configured_destination(tmp_path, monkeypatch)
    payload = b"one archived object"
    digest = hashlib.sha256(payload).hexdigest()

    class CountingClient(_ArchiveClient):
        downloads = 0

        def get_object(self, **arguments: str) -> dict[str, Any]:
            self.downloads += 1
            return super().get_object(**arguments)

    client = CountingClient({f"raw/{key_for_digest(digest)}": payload})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)
    monkeypatch.setenv(
        "MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES",
        str(len(payload) + archive_backup._CHECKSUM_BYTES + archive_backup._AUTHENTICATION_BYTES),
    )
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES", "0")

    with pytest.raises(archive_backup.BackupError, match="aggregate byte limit"):
        archive_backup.backup(destination)

    assert client.downloads == 0
    assert list((destination / "objects").iterdir()) == []
    assert not (destination / "manifest.json").exists()


def test_archive_backup_stops_listing_at_the_prospective_manifest_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, _key = _configured_destination(tmp_path, monkeypatch)
    yielded = 0

    def oversized_inventory(*_args: object, **_kwargs: object) -> Iterator[dict[str, Any]]:
        nonlocal yielded
        for index in range(10_000):
            yielded += 1
            digest = f"{index:064x}"
            yield {
                "key": f"raw/{key_for_digest(digest)}",
                "sha256": digest,
                "size": 0,
            }

    monkeypatch.setattr(archive_backup, "_object_rows", oversized_inventory)
    monkeypatch.setattr(archive_backup, "_client", object)
    monkeypatch.setattr(archive_backup, "_MAXIMUM_MANIFEST_BYTES", 1_024)
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES", "0")

    with pytest.raises(archive_backup.BackupError, match="manifest exceeds"):
        archive_backup.backup(destination)

    assert yielded < 100
    assert list((destination / "objects").iterdir()) == []


def test_archive_backup_computes_metadata_before_download_and_uses_one_capacity_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, _key = _configured_destination(tmp_path, monkeypatch)
    payload = b"one archived object"
    digest = hashlib.sha256(payload).hexdigest()
    client = _ArchiveClient({f"raw/{key_for_digest(digest)}": payload})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES", "0")
    events: list[str] = []
    guards: list[tuple[Path, Path | None]] = []
    original_canonical = archive_backup._canonical_manifest_payload
    original_download = archive_backup._download
    original_guard = FileSystemCapacityGuard

    def canonical(manifest: dict[str, Any]) -> bytes:
        events.append("metadata")
        return original_canonical(manifest)

    def download(*args: Any, **kwargs: Any) -> tuple[str, int]:
        events.append("download")
        return original_download(*args, **kwargs)

    def capacity_guard(
        storage_directory: Path,
        *,
        minimum_free_bytes: int,
        coordination_directory: Path | None = None,
    ) -> FileSystemCapacityGuard:
        guards.append((storage_directory, coordination_directory))
        return original_guard(
            storage_directory,
            minimum_free_bytes=minimum_free_bytes,
            coordination_directory=coordination_directory,
        )

    monkeypatch.setattr(archive_backup, "_canonical_manifest_payload", canonical)
    monkeypatch.setattr(archive_backup, "_download", download)
    monkeypatch.setattr(archive_backup, "FileSystemCapacityGuard", capacity_guard)

    archive_backup.backup(destination)

    assert events.index("metadata") < events.index("download")
    assert guards == [(destination, destination)]


def test_archive_backup_refuses_existing_authenticated_metadata_before_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, _key = _configured_destination(tmp_path, monkeypatch)
    sentinel = b"existing authenticated manifest"
    (destination / "manifest.json").write_bytes(sentinel)
    payload = b"one archived object"
    digest = hashlib.sha256(payload).hexdigest()

    class CountingClient(_ArchiveClient):
        downloads = 0

        def get_object(self, **arguments: str) -> dict[str, Any]:
            self.downloads += 1
            return super().get_object(**arguments)

    client = CountingClient({f"raw/{key_for_digest(digest)}": payload})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)

    with pytest.raises(archive_backup.BackupError, match="already exists"):
        archive_backup.backup(destination)

    assert client.downloads == 0
    assert (destination / "manifest.json").read_bytes() == sentinel


def test_archive_backup_removes_read_only_metadata_after_partial_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, _key = _configured_destination(tmp_path, monkeypatch)
    monkeypatch.setattr(archive_backup, "_client", lambda: _ArchiveClient({}))
    original_write = archive_backup._write_atomic
    writes = 0

    def fail_second_write(*args: Any, **kwargs: Any) -> None:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise archive_backup.BackupError("simulated sidecar failure")
        original_write(*args, **kwargs)

    monkeypatch.setattr(archive_backup, "_write_atomic", fail_second_write)

    with pytest.raises(archive_backup.BackupError, match="simulated sidecar failure"):
        archive_backup.backup(destination)

    assert not (destination / "manifest.json").exists()
    assert not (destination / "manifest.json.sha256").exists()
    assert not (destination / "manifest.json.hmac-sha256").exists()


def test_archive_backup_checks_free_space_before_each_output_chunk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, _key = _configured_destination(tmp_path, monkeypatch)
    payload = b"12345"
    digest = hashlib.sha256(payload).hexdigest()
    client = _ArchiveClient({f"raw/{key_for_digest(digest)}": payload})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES", "4096")
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES", "1")
    monkeypatch.setattr(
        "makolet.adapters.filesystem_capacity.shutil.disk_usage",
        lambda _path: SimpleNamespace(total=5, used=0, free=5),
    )

    with pytest.raises(archive_backup.BackupError, match="free-space reserve"):
        archive_backup.backup(destination)

    assert not (destination / "objects" / digest).exists()


def test_archive_backup_checks_free_space_before_manifest_sidecars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, _key = _configured_destination(tmp_path, monkeypatch)
    monkeypatch.setattr(archive_backup, "_client", lambda: _ArchiveClient({}))
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES", "4096")
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES", "1")
    monkeypatch.setattr(
        "makolet.adapters.filesystem_capacity.shutil.disk_usage",
        lambda _path: SimpleNamespace(total=1, used=0, free=1),
    )

    with pytest.raises(archive_backup.BackupError, match="free-space reserve"):
        archive_backup.backup(destination)

    assert not (destination / "manifest.json").exists()
    assert not (destination / "manifest.json.sha256").exists()
    assert not (destination / "manifest.json.hmac-sha256").exists()


def test_recomputed_unkeyed_subset_is_rejected_before_inventory_is_parsed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    first_digest = hashlib.sha256(b"first").hexdigest()
    second_digest = hashlib.sha256(b"second").hexdigest()
    complete_manifest = _manifest(
        objects=[
            {
                "key": f"raw/{key_for_digest(first_digest)}",
                "sha256": first_digest,
                "size": 5,
            },
            {
                "key": f"raw/{key_for_digest(second_digest)}",
                "sha256": second_digest,
                "size": 6,
            },
        ]
    )
    _write_authenticated_manifest(destination, key, complete_manifest)
    subset = dict(complete_manifest)
    subset["objects"] = complete_manifest["objects"][:1]
    subset_payload = archive_backup._canonical_manifest_payload(subset)
    manifest_path = destination / "manifest.json"
    checksum_path = destination / "manifest.json.sha256"
    manifest_path.write_bytes(subset_payload)
    checksum_path.write_bytes(
        f"{hashlib.sha256(subset_payload).hexdigest()}  manifest.json\n".encode("ascii")
    )

    def fail_if_parsed(_payload: bytes) -> Any:
        raise AssertionError("unauthenticated inventory was parsed")

    monkeypatch.setattr(json, "loads", fail_if_parsed)
    with pytest.raises(archive_backup.BackupError, match="authentication failed"):
        archive_backup.verify(destination)


@pytest.mark.parametrize(
    ("filename", "maximum_bytes", "message"),
    [
        ("manifest.json", archive_backup._MAXIMUM_MANIFEST_BYTES, "manifest exceeds"),
        (
            "manifest.json.sha256",
            archive_backup._CHECKSUM_BYTES,
            "checksum sidecar exceeds",
        ),
        (
            "manifest.json.hmac-sha256",
            archive_backup._AUTHENTICATION_BYTES,
            "authentication sidecar exceeds",
        ),
    ],
)
def test_sparse_oversized_metadata_is_rejected_before_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    maximum_bytes: int,
    message: str,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    _write_authenticated_manifest(destination, key, _manifest())
    target = destination / filename
    with target.open("wb") as output:
        output.seek(maximum_bytes)
        output.write(b"x")

    with pytest.raises(archive_backup.BackupError, match=message):
        archive_backup.verify(destination)


@pytest.mark.parametrize(
    ("filename", "payload", "message"),
    [
        (
            "manifest.json.hmac-sha256",
            _AUTHENTICATION_PREFIX + b"A" * 64 + b"\n",
            "authentication sidecar is invalid",
        ),
        (
            "manifest.json.hmac-sha256",
            _AUTHENTICATION_PREFIX + b"0" * 64,
            "authentication sidecar is invalid",
        ),
        (
            "manifest.json.sha256",
            b"0" * 64 + b"  other.json\n",
            "checksum sidecar",
        ),
        (
            "manifest.json.sha256",
            b"A" * 64 + b"  manifest.json\n",
            "checksum sidecar is invalid",
        ),
    ],
)
def test_malformed_manifest_sidecars_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    payload: bytes,
    message: str,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    _write_authenticated_manifest(destination, key, _manifest())
    (destination / filename).write_bytes(payload)

    with pytest.raises(archive_backup.BackupError, match=message):
        archive_backup.verify(destination)


@pytest.mark.parametrize(
    "filename",
    ["manifest.json", "manifest.json.sha256", "manifest.json.hmac-sha256"],
)
def test_manifest_metadata_symlinks_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    _write_authenticated_manifest(destination, key, _manifest())
    target = destination / filename
    replacement = tmp_path / f"real-{filename}"
    replacement.write_bytes(target.read_bytes())
    target.unlink()
    try:
        target.symlink_to(replacement)
    except OSError:
        pytest.skip("test host does not permit symlink creation")

    with pytest.raises(archive_backup.BackupError, match="regular file"):
        archive_backup.verify(destination)


def test_authentication_key_must_be_exact_protected_and_outside_backup_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, key_file, key = _configured_destination(tmp_path, monkeypatch)
    _write_authenticated_manifest(destination, key, _manifest())
    nested_key = destination / "archive-backup-auth.key"
    nested_key.write_bytes(key)
    nested_key.chmod(0o600)
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE", str(nested_key))

    with pytest.raises(archive_backup.BackupError, match="outside the backup tree"):
        archive_backup.verify(destination)

    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE", str(key_file))
    key_file.write_bytes(key + b"x")
    with pytest.raises(archive_backup.BackupError, match="size limit"):
        archive_backup.verify(destination)


def test_authentication_key_with_an_additional_hard_link_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, key_file, key = _configured_destination(tmp_path, monkeypatch)
    _write_authenticated_manifest(destination, key, _manifest())
    try:
        os.link(key_file, tmp_path / "second-key-link")
    except OSError:
        pytest.skip("test filesystem does not support hard links")

    with pytest.raises(archive_backup.BackupError, match="unsafe link count"):
        archive_backup.verify(destination)


def test_direct_tool_still_rejects_a_non_owner_only_posix_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, key_file, key = _configured_destination(tmp_path, monkeypatch)
    _write_authenticated_manifest(destination, key, _manifest())
    key_file.chmod(0o644)
    monkeypatch.setattr(
        archive_backup,
        "_REQUIRE_OWNER_ONLY_KEY_PERMISSIONS",
        True,
    )
    monkeypatch.setenv(
        archive_backup._WINDOWS_BIND_STAGING_ENVIRONMENT,
        archive_backup._WINDOWS_BIND_STAGING_PREFIX + hashlib.sha256(key).hexdigest(),
    )

    with pytest.raises(archive_backup.BackupError, match="not protected"):
        archive_backup.verify(destination)


def test_windows_bind_launcher_uses_and_removes_an_owner_private_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    destination, source_key, key = _configured_destination(tmp_path, monkeypatch)
    _write_authenticated_manifest(destination, key, _manifest())
    source_key.chmod(0o644)
    private_directory = tmp_path / "container-tmp"
    private_directory.mkdir()
    monkeypatch.setattr(archive_backup, "_WINDOWS_BIND_KEY_PATH", source_key)
    monkeypatch.setattr(archive_backup, "_PRIVATE_KEY_DIRECTORY", private_directory)
    monkeypatch.setattr(archive_backup, "_WINDOWS_BIND_RUNTIME_PLATFORM", "posix")
    monkeypatch.setattr(
        archive_backup,
        "_WINDOWS_BIND_FILE_MODE",
        stat.S_IMODE(source_key.stat().st_mode),
    )
    monkeypatch.setattr(
        archive_backup,
        "_effective_user_and_group",
        lambda: (
            archive_backup._WINDOWS_BIND_USER_ID,
            archive_backup._WINDOWS_BIND_GROUP_ID,
        ),
    )
    monkeypatch.setattr(archive_backup, "_file_system_is_read_only", lambda _path: True)
    monkeypatch.setenv(
        archive_backup._WINDOWS_BIND_STAGING_ENVIRONMENT,
        archive_backup._WINDOWS_BIND_STAGING_PREFIX + hashlib.sha256(key).hexdigest(),
    )
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE", str(source_key))

    with archive_backup._authentication_key_scope():
        private_file = Path(os.environ["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"])
        assert private_file.parent == private_directory
        assert private_file != source_key
        assert private_file.read_bytes() == key
        if os.name != "nt":
            assert stat.S_IMODE(private_file.stat().st_mode) == 0o600
        assert archive_backup._authentication_key(destination) == key

    assert os.environ["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] == str(source_key)
    assert not private_file.exists()

    monkeypatch.setattr(
        sys,
        "argv",
        ["archive_backup.py", "verify", str(destination)],
    )
    assert archive_backup.main() == 0
    assert json.loads(capsys.readouterr().out) == {"objects": 0, "status": "verified"}
    assert not list(private_directory.glob(".makolet-archive-backup-auth-*"))


@pytest.mark.parametrize(
    "failure",
    [
        "absent",
        "malformed",
        "wrong-digest",
        "wrong-path",
        "writable-mount",
        "wrong-identity",
        "wrong-mode",
    ],
)
def test_main_rejects_unsafe_windows_bind_staging_without_retaining_a_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: str,
) -> None:
    destination, source_key, key = _configured_destination(tmp_path, monkeypatch)
    _write_authenticated_manifest(destination, key, _manifest())
    source_key.chmod(0o644)
    private_directory = tmp_path / "container-tmp"
    private_directory.mkdir()
    monkeypatch.setattr(archive_backup, "_WINDOWS_BIND_KEY_PATH", source_key)
    monkeypatch.setattr(archive_backup, "_PRIVATE_KEY_DIRECTORY", private_directory)
    monkeypatch.setattr(archive_backup, "_WINDOWS_BIND_RUNTIME_PLATFORM", "posix")
    monkeypatch.setattr(
        archive_backup,
        "_WINDOWS_BIND_FILE_MODE",
        stat.S_IMODE(source_key.stat().st_mode),
    )
    monkeypatch.setattr(
        archive_backup,
        "_effective_user_and_group",
        lambda: (
            archive_backup._WINDOWS_BIND_USER_ID,
            archive_backup._WINDOWS_BIND_GROUP_ID,
        ),
    )
    monkeypatch.setattr(archive_backup, "_file_system_is_read_only", lambda _path: True)
    signal = archive_backup._WINDOWS_BIND_STAGING_PREFIX + hashlib.sha256(key).hexdigest()
    monkeypatch.setenv(archive_backup._WINDOWS_BIND_STAGING_ENVIRONMENT, signal)
    if failure == "absent":
        monkeypatch.delenv(archive_backup._WINDOWS_BIND_STAGING_ENVIRONMENT)
        monkeypatch.setattr(
            archive_backup,
            "_REQUIRE_OWNER_ONLY_KEY_PERMISSIONS",
            True,
        )
    elif failure == "malformed":
        monkeypatch.setenv(
            archive_backup._WINDOWS_BIND_STAGING_ENVIRONMENT,
            "windows-bind-staging-v1:not-a-digest",
        )
    elif failure == "wrong-digest":
        monkeypatch.setenv(
            archive_backup._WINDOWS_BIND_STAGING_ENVIRONMENT,
            archive_backup._WINDOWS_BIND_STAGING_PREFIX + "0" * 64,
        )
    elif failure == "wrong-path":
        monkeypatch.setenv(
            "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE",
            str(source_key.with_name("different.key")),
        )
    elif failure == "writable-mount":
        monkeypatch.setattr(
            archive_backup,
            "_file_system_is_read_only",
            lambda _path: False,
        )
    elif failure == "wrong-identity":
        monkeypatch.setattr(
            archive_backup,
            "_effective_user_and_group",
            lambda: (10002, archive_backup._WINDOWS_BIND_GROUP_ID),
        )
    else:
        monkeypatch.setattr(
            archive_backup,
            "_WINDOWS_BIND_FILE_MODE",
            stat.S_IMODE(source_key.stat().st_mode) ^ stat.S_IXUSR,
        )
    monkeypatch.setattr(
        sys,
        "argv",
        ["archive_backup.py", "verify", str(destination)],
    )

    assert archive_backup.main() == 1

    output = capsys.readouterr()
    assert output.out == ""
    assert output.err.startswith("archive backup error: ")
    assert str(source_key) not in output.err
    assert key.hex() not in output.err
    assert not list(private_directory.glob(".makolet-archive-backup-auth-*"))


def test_windows_bind_launcher_removes_private_key_when_operation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, source_key, key = _configured_destination(tmp_path, monkeypatch)
    private_directory = tmp_path / "container-tmp"
    private_directory.mkdir()
    source_key.chmod(0o644)
    monkeypatch.setattr(archive_backup, "_WINDOWS_BIND_KEY_PATH", source_key)
    monkeypatch.setattr(archive_backup, "_PRIVATE_KEY_DIRECTORY", private_directory)
    monkeypatch.setattr(archive_backup, "_WINDOWS_BIND_RUNTIME_PLATFORM", "posix")
    monkeypatch.setattr(
        archive_backup,
        "_WINDOWS_BIND_FILE_MODE",
        stat.S_IMODE(source_key.stat().st_mode),
    )
    monkeypatch.setattr(
        archive_backup,
        "_effective_user_and_group",
        lambda: (
            archive_backup._WINDOWS_BIND_USER_ID,
            archive_backup._WINDOWS_BIND_GROUP_ID,
        ),
    )
    monkeypatch.setattr(archive_backup, "_file_system_is_read_only", lambda _path: True)
    monkeypatch.setenv(
        archive_backup._WINDOWS_BIND_STAGING_ENVIRONMENT,
        archive_backup._WINDOWS_BIND_STAGING_PREFIX + hashlib.sha256(key).hexdigest(),
    )
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE", str(source_key))
    observed_private_key: Path | None = None

    def fail_after_staging(_destination: Path) -> dict[str, Any]:
        nonlocal observed_private_key
        observed_private_key = Path(os.environ["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"])
        assert observed_private_key.is_file()
        raise archive_backup.BackupError("intentional operation failure")

    monkeypatch.setattr(archive_backup, "verify", fail_after_staging)
    monkeypatch.setattr(
        sys,
        "argv",
        ["archive_backup.py", "verify", str(destination)],
    )

    assert archive_backup.main() == 1
    assert observed_private_key is not None
    assert not observed_private_key.exists()


def test_restore_authenticates_and_verifies_one_manifest_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    _write_authenticated_manifest(destination, key, _manifest())
    client = _ArchiveClient({})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)
    load_count = 0
    original_load = archive_backup._load_verified_manifest

    def counted_load(
        selected: Path,
        deadline: archive_backup._OperationDeadline | None = None,
    ) -> dict[str, Any]:
        nonlocal load_count
        load_count += 1
        return original_load(selected, deadline)

    monkeypatch.setattr(archive_backup, "_load_verified_manifest", counted_load)

    assert archive_backup.restore(destination, confirmed_bucket="raw-archive") == {
        "status": "restored",
        "objects": 0,
        "objects_created": 0,
    }
    assert load_count == 1


def test_restore_verifies_staging_bytes_before_canonical_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    payload = b"verified archive object"
    corrupt_payload = b"corrupted archive bytes"
    assert len(corrupt_payload) == len(payload)
    digest = hashlib.sha256(payload).hexdigest()
    service_key = f"raw/{key_for_digest(digest)}"
    (destination / "objects" / digest).write_bytes(payload)
    _write_authenticated_manifest(
        destination,
        key,
        _manifest(objects=[{"key": service_key, "sha256": digest, "size": len(payload)}]),
    )

    class CorruptingClient(_ArchiveClient):
        def put_object(self, **arguments: Any) -> dict[str, Any]:
            body = arguments["Body"]
            assert len(body.read()) == arguments["ContentLength"]
            self.objects[arguments["Key"]] = corrupt_payload
            return {}

    client = CorruptingClient({})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)

    with pytest.raises(archive_backup.BackupError, match="staging object failed"):
        archive_backup.restore(destination, confirmed_bucket="raw-archive")

    assert service_key not in client.objects
    assert client.objects == {}


def test_restore_cleans_staging_after_an_ambiguous_upload_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    payload = b"verified archive object"
    digest = hashlib.sha256(payload).hexdigest()
    service_key = f"raw/{key_for_digest(digest)}"
    (destination / "objects" / digest).write_bytes(payload)
    _write_authenticated_manifest(
        destination,
        key,
        _manifest(objects=[{"key": service_key, "sha256": digest, "size": len(payload)}]),
    )

    class LostResponseClient(_ArchiveClient):
        def put_object(self, **arguments: Any) -> dict[str, Any]:
            super().put_object(**arguments)
            response: Any = {
                "Error": {"Code": "InternalError"},
                "ResponseMetadata": {"HTTPStatusCode": 500},
            }
            raise ClientError(response, "PutObject")

    client = LostResponseClient({})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)

    with pytest.raises(archive_backup.BackupError, match="staging upload failed"):
        archive_backup.restore(destination, confirmed_bucket="raw-archive")

    assert client.objects == {}


def test_restore_removes_deterministic_pending_staging_after_transient_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    payload = b"verified archive object"
    digest = hashlib.sha256(payload).hexdigest()
    service_key = f"raw/{key_for_digest(digest)}"
    staging_key = archive_backup._restore_staging_key("raw", service_key, digest)
    (destination / "objects" / digest).write_bytes(payload)
    _write_authenticated_manifest(
        destination,
        key,
        _manifest(objects=[{"key": service_key, "sha256": digest, "size": len(payload)}]),
    )

    class TransientDeleteClient(_ArchiveClient):
        delete_attempts = 0

        def delete_object(self, **arguments: str) -> None:
            if arguments["Key"] == staging_key:
                self.delete_attempts += 1
                if self.delete_attempts == 1:
                    response: Any = {
                        "Error": {"Code": "InternalError"},
                        "ResponseMetadata": {"HTTPStatusCode": 500},
                    }
                    raise ClientError(response, "DeleteObject")
            super().delete_object(**arguments)

    client = TransientDeleteClient({staging_key: payload})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)

    assert archive_backup.restore(destination, confirmed_bucket="raw-archive") == {
        "status": "restored",
        "objects": 1,
        "objects_created": 1,
    }
    assert client.delete_attempts >= 3
    assert client.objects == {service_key: payload}


def test_restore_recovers_when_a_successful_staging_delete_response_is_lost(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    payload = b"verified archive object"
    digest = hashlib.sha256(payload).hexdigest()
    service_key = f"raw/{key_for_digest(digest)}"
    staging_key = archive_backup._restore_staging_key("raw", service_key, digest)
    (destination / "objects" / digest).write_bytes(payload)
    _write_authenticated_manifest(
        destination,
        key,
        _manifest(objects=[{"key": service_key, "sha256": digest, "size": len(payload)}]),
    )

    class LostDeleteResponseClient(_ArchiveClient):
        delete_attempts = 0

        def delete_object(self, **arguments: str) -> None:
            if arguments["Key"] == staging_key:
                self.delete_attempts += 1
                if self.delete_attempts == 1:
                    super().delete_object(**arguments)
                    response: Any = {
                        "Error": {"Code": "InternalError"},
                        "ResponseMetadata": {"HTTPStatusCode": 500},
                    }
                    raise ClientError(response, "DeleteObject")
            super().delete_object(**arguments)

    client = LostDeleteResponseClient({staging_key: payload})
    monkeypatch.setattr(archive_backup, "_client", lambda: client)

    assert (
        archive_backup.restore(destination, confirmed_bucket="raw-archive")["objects_created"] == 1
    )
    assert client.delete_attempts == 2
    assert client.objects == {service_key: payload}


def test_restore_reads_copies_and_deletes_the_exact_staging_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, _key_file, key = _configured_destination(tmp_path, monkeypatch)
    payload = b"verified versioned archive object"
    digest = hashlib.sha256(payload).hexdigest()
    service_key = f"raw/{key_for_digest(digest)}"
    staging_key = archive_backup._restore_staging_key("raw", service_key, digest)
    version_id = "restore-version-1"
    (destination / "objects" / digest).write_bytes(payload)
    _write_authenticated_manifest(
        destination,
        key,
        _manifest(objects=[{"key": service_key, "sha256": digest, "size": len(payload)}]),
    )

    class VersionedClient(_ArchiveClient):
        deleted: list[dict[str, str]]

        def __init__(self) -> None:
            super().__init__({})
            self.deleted = []

        def head_object(self, **arguments: str) -> dict[str, Any]:
            response = super().head_object(**arguments)
            if arguments["Key"] == staging_key:
                response["VersionId"] = version_id
            return response

        def put_object(self, **arguments: Any) -> dict[str, Any]:
            super().put_object(**arguments)
            return {"VersionId": version_id}

        def get_object(self, **arguments: str) -> dict[str, Any]:
            if arguments["Key"] == staging_key:
                assert arguments["VersionId"] == version_id
            response = super().get_object(**arguments)
            if arguments["Key"] == staging_key:
                response["VersionId"] = version_id
            return response

        def copy_object(self, **arguments: Any) -> None:
            source = arguments["CopySource"]
            assert source == {
                "Bucket": "raw-archive",
                "Key": staging_key,
                "VersionId": version_id,
            }
            super().copy_object(**arguments)

        def delete_object(self, **arguments: str) -> None:
            if arguments["Key"] == staging_key:
                assert arguments["VersionId"] == version_id
                self.deleted.append(dict(arguments))
            super().delete_object(**arguments)

    client = VersionedClient()
    monkeypatch.setattr(archive_backup, "_client", lambda: client)

    assert (
        archive_backup.restore(destination, confirmed_bucket="raw-archive")["objects_created"] == 1
    )
    assert client.deleted == [
        {"Bucket": "raw-archive", "Key": staging_key, "VersionId": version_id}
    ]
    assert client.objects == {service_key: payload}


def test_archive_entry_points_inject_one_dedicated_read_only_key() -> None:
    container_key = "/run/secrets/makolet-archive-backup-auth.key"
    for filename in ("archive-backup.sh", "archive-verify.sh", "archive-restore.sh"):
        script = (_REPOSITORY_ROOT / "scripts" / filename).read_text(encoding="utf-8")
        assert "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE" in script
        assert container_key in script
        assert '"$authentication_key:$container_authentication_key:ro"' in script
        assert '--env "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE=' in script
        assert "MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES" in script
        assert "MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES" in script
        assert "MAKOLET_ARCHIVE_BACKUP_MAX_LIST_PAGES" in script
        assert "MAKOLET_ARCHIVE_BACKUP_MAX_NO_PROGRESS_PAGES" in script
        assert "MAKOLET_ARCHIVE_BACKUP_LIST_TIMEOUT_SECONDS" in script
        assert "MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS" in script
        assert "MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS" in script
        assert "archive-compose-watchdog.sh" in script
        assert "run_with_bounded_watchdog" in script
        assert '--name "$archive_container_name"' in script
        assert '--user "$host_user_id:$host_group_id"' in script
        assert "2147483647" in script

    backup_script = (_REPOSITORY_ROOT / "scripts" / "archive-backup.sh").read_text(encoding="utf-8")
    assert "ps --status running --status restarting --services" in backup_script
    assert "stop worker" in backup_script
    assert "up -d --wait worker" in backup_script
    assert "acquire_archive_backup_lock" in backup_script
    assert "release_archive_backup_lock" in backup_script

    powershell = (_REPOSITORY_ROOT / "scripts" / "operations.ps1").read_text(encoding="utf-8")
    archive_function = powershell.split("function Invoke-ArchiveOperation", 1)[1]
    assert "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE" in archive_function
    assert "MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES" in archive_function
    assert "MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES" in archive_function
    assert "MAKOLET_ARCHIVE_BACKUP_MAX_LIST_PAGES" in archive_function
    assert "MAKOLET_ARCHIVE_BACKUP_MAX_NO_PROGRESS_PAGES" in archive_function
    assert "MAKOLET_ARCHIVE_BACKUP_LIST_TIMEOUT_SECONDS" in archive_function
    assert "MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS" in archive_function
    assert "MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS" in archive_function
    assert container_key in archive_function
    assert "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_WINDOWS_BIND" in archive_function
    assert "windows-bind-staging-v1:$KeyDigest" in archive_function
    assert "/run/secrets/makolet-archive-backup-auth.key.host" in archive_function
    assert '"--user", "${HostUserId}:${HostGroupId}"' in archive_function
    assert '"--user", "10001:10001"' in archive_function
    assert "[System.IO.FileShare]::Read" in archive_function
    assert 'Properties["LinkType"]' in archive_function
    assert '"stop", "worker"' in archive_function
    assert '"ps", "--status", "running", "--status", "restarting", "--services"' in (
        archive_function
    )
    assert '"up", "-d", "--wait", "worker"' in archive_function
    assert "Enter-ArchiveBackupLock" in archive_function
    assert "Exit-ArchiveBackupLock" in archive_function
    assert "Invoke-ComposeWithWatchdog" in powershell
    assert "Remove-ArchiveOperationContainer" in powershell
    assert '"--name", $ArchiveContainerName' in archive_function
    assert '[guid]::NewGuid().ToString("N")' in archive_function
    assert "MAKOLET_ARCHIVE_WORKER_RESTART_TIMEOUT_SECONDS" in archive_function

    watchdog = (_REPOSITORY_ROOT / "scripts" / "archive-compose-watchdog.sh").read_text(
        encoding="utf-8"
    )
    assert "docker container rm --force" in watchdog
    assert "$$-$RANDOM-$RANDOM-$RANDOM" in watchdog

    smoke = (_REPOSITORY_ROOT / "scripts" / "container-smoke.sh").read_text(encoding="utf-8")
    assert "protected/database-backup-auth.key" in smoke
    assert "protected/archive-backup-auth.key" in smoke
    assert smoke.count("generate-key") == 2


def test_backup_restore_wrappers_pin_compose_and_reject_ambient_selectors() -> None:
    compose_path_marker = '"$repository_root/compose.yaml"'
    bash_scripts = (
        "archive-backup.sh",
        "archive-verify.sh",
        "archive-restore.sh",
        "database-backup.sh",
        "database-restore.sh",
    )
    for filename in bash_scripts:
        script = (_REPOSITORY_ROOT / "scripts" / filename).read_text(encoding="utf-8")
        assert compose_path_marker in script
        for selector in (
            "COMPOSE_FILE",
            "COMPOSE_ENV_FILES",
            "COMPOSE_PATH_SEPARATOR",
            "COMPOSE_PROJECT_NAME",
            "COMPOSE_PROFILES",
            "COMPOSE_DISABLE_ENV_FILE",
            "DOCKER_CONFIG",
            "DOCKER_CONTEXT",
            "DOCKER_HOST",
        ):
            assert selector in script

    powershell = (_REPOSITORY_ROOT / "scripts" / "operations.ps1").read_text(encoding="utf-8")
    assert 'Join-Path $RepositoryRoot "compose.yaml"' in powershell
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
    ):
        assert selector in powershell


def test_powershell_wrapper_rejects_hostile_compose_file_before_docker(
    tmp_path: Path,
) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "docker-was-invoked.txt"
    fake_docker = fake_bin / ("docker.cmd" if os.name == "nt" else "docker")
    if os.name == "nt":
        fake_docker.write_text(
            '@echo off\r\n> "%MAKOLET_CAPTURE%" echo invoked\r\nexit /b 0\r\n',
            encoding="ascii",
        )
    else:
        fake_docker.write_text(
            '#!/usr/bin/env sh\nprintf invoked >"$MAKOLET_CAPTURE"\n',
            encoding="utf-8",
        )
        fake_docker.chmod(0o755)
    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
    ):
        environment.pop(selector, None)
    environment["COMPOSE_FILE"] = str(tmp_path / "attacker-compose.yaml")
    environment["MAKOLET_CAPTURE"] = str(capture)
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(_REPOSITORY_ROOT / "scripts" / "operations.ps1"),
            "archive-backup",
            str(tmp_path / "backup"),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode != 0
    assert "COMPOSE_FILE" in completed.stderr
    assert not capture.exists()

    environment.pop("COMPOSE_FILE")
    environment["COMPOSE_PROJECT_NAME"] = "attacker-project"
    project_destination = tmp_path / "project-backup"
    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(_REPOSITORY_ROOT / "scripts" / "operations.ps1"),
            "archive-backup",
            str(project_destination),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode != 0
    assert "COMPOSE_PROJECT_NAME" in completed.stderr
    assert not project_destination.exists()
    assert not capture.exists()


def test_bash_wrapper_rejects_hostile_compose_file_before_backup_changes(
    tmp_path: Path,
) -> None:
    if os.name == "nt":
        git_bash = Path("C:/Program Files/Git/bin/bash.exe")
        bash = str(git_bash) if git_bash.is_file() else None
    else:
        bash = shutil.which("bash")
    if bash is None:
        pytest.skip("Git/POSIX Bash is unavailable")
    destination = tmp_path / "backup"
    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
    ):
        environment.pop(selector, None)
    environment["COMPOSE_FILE"] = "attacker-compose.yaml"

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            bash,
            str(_REPOSITORY_ROOT / "scripts" / "archive-backup.sh"),
            str(destination),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode != 0
    assert "COMPOSE_FILE" in completed.stderr
    assert not destination.exists()

    environment.pop("COMPOSE_FILE")
    environment["COMPOSE_PROJECT_NAME"] = "attacker-project"
    project_destination = tmp_path / "project-backup"
    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            bash,
            str(_REPOSITORY_ROOT / "scripts" / "archive-backup.sh"),
            str(project_destination),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode != 0
    assert "COMPOSE_PROJECT_NAME" in completed.stderr
    assert not project_destination.exists()


@pytest.mark.skipif(os.name == "nt", reason="POSIX wrapper harness")
def test_bash_backup_wrapper_passes_the_invoking_identity_without_docker(
    tmp_path: Path,
) -> None:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("Bash is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "docker-arguments.txt"
    fake_id = fake_bin / "id"
    fake_id.write_text(
        '#!/usr/bin/env sh\ncase "$1" in -u) echo 12345;; -g) echo 23456;; *) exit 2;; esac\n',
        encoding="utf-8",
    )
    fake_id.chmod(0o755)
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
case " $* " in
  *" volume create "*)
    for argument in "$@"; do
      case "$argument" in
        com.makolet.archive-backup-lock.owner=*)
          printf '%s\n' "${argument#*=}" >"$MAKOLET_CAPTURE.lock"
          ;;
      esac
    done
    printf '%s\n' "${@: -1}"
    exit 0
    ;;
  *" volume inspect "*) cat "$MAKOLET_CAPTURE.lock"; exit 0 ;;
  *" volume rm "*) rm -f "$MAKOLET_CAPTURE.lock"; printf '%s\n' "${@: -1}"; exit 0 ;;
  *" run --rm "*) printf '%s\n' "$@" >"$MAKOLET_CAPTURE"; exit 0 ;;
esac
exit 0
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    key_file = tmp_path / "protected" / "archive.key"
    key_file.parent.mkdir()
    key_file.write_bytes(bytes(range(32)))
    key_file.chmod(0o600)
    destination = tmp_path / "backup"
    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
    ):
        environment.pop(selector, None)
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["MAKOLET_CAPTURE"] = str(capture)
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = str(key_file)
    environment["COMPOSE_PROJECT_NAME"] = "makolet-smoke-local-12345"
    environment["MAKOLET_COMPOSE_ENV_FILE"] = ".env.example"
    environment["MAKOLET_ENVIRONMENT"] = "development"
    environment["POSTGRES_DB"] = "makolet_test_coverage"

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            bash,
            str(_REPOSITORY_ROOT / "scripts" / "archive-backup.sh"),
            str(destination),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr
    arguments = capture.read_text(encoding="utf-8").splitlines()
    user_index = arguments.index("--user")
    assert arguments[user_index + 1] == "12345:23456"
    project_index = arguments.index("--project-name")
    assert arguments[project_index + 1] == "makolet-smoke-local-12345"
    assert str(key_file.resolve()) in "\n".join(arguments)


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell wrapper harness")
def test_powershell_backup_wrapper_passes_digest_bound_staging_without_docker(
    tmp_path: Path,
) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "docker-arguments.txt"
    key = bytes(range(32))
    key_file = tmp_path / "protected" / "archive.key"
    key_file.parent.mkdir()
    key_file.write_bytes(key)
    destination = tmp_path / "backup"
    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
    ):
        environment.pop(selector, None)
    _configure_windows_docker_shim(environment, fake_bin, mode="capture-backup")
    environment["MAKOLET_CAPTURE"] = str(capture)
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = str(key_file)
    environment["COMPOSE_PROJECT_NAME"] = "makolet-smoke-local-12345"
    environment["MAKOLET_COMPOSE_ENV_FILE"] = ".env.example"
    environment["MAKOLET_ENVIRONMENT"] = "development"
    environment["POSTGRES_DB"] = "makolet_test_coverage"

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            powershell,
            "-NoProfile",
            "-File",
            str(_REPOSITORY_ROOT / "scripts" / "operations.ps1"),
            "archive-backup",
            str(destination),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode == 0, completed.stderr
    assert '{"status":"backed_up"}' in completed.stdout
    arguments = capture.read_text(encoding="utf-8")
    expected_signal = (
        "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_WINDOWS_BIND=windows-bind-staging-v1:"
        f"{hashlib.sha256(key).hexdigest()}"
    )
    assert "--user 10001:10001" in arguments
    assert "--project-name makolet-smoke-local-12345" in arguments
    assert "compose.yaml" in arguments
    assert "/run/secrets/makolet-archive-backup-auth.key.host:ro" in arguments
    assert expected_signal in arguments


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell wrapper harness")
def test_powershell_archive_backup_stops_and_restarts_a_running_worker(
    tmp_path: Path,
) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "docker-calls.txt"
    key_file = tmp_path / "protected" / "archive.key"
    key_file.parent.mkdir()
    key_file.write_bytes(bytes(range(32)))
    destination = tmp_path / "backup"
    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
    ):
        environment.pop(selector, None)
    _configure_windows_docker_shim(environment, fake_bin, mode="worker-success")
    environment["MAKOLET_CAPTURE"] = str(capture)
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = str(key_file)
    environment.pop("MAKOLET_COMPOSE_ENV_FILE", None)

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(_REPOSITORY_ROOT / "scripts" / "operations.ps1"),
            "archive-backup",
            str(destination),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode == 0, completed.stderr
    calls = capture.read_text(encoding="utf-8").splitlines()
    compose_calls = [call for call in calls if call.startswith("compose ")]
    assert compose_calls
    assert all("--project-name makolet" in call for call in compose_calls)
    assert calls[0].startswith("volume create --label ")
    assert calls[-1] == "volume rm makolet_archive_backup_lock_makolet"
    stop_index = next(index for index, call in enumerate(calls) if " stop worker" in call)
    backup_index = next(index for index, call in enumerate(calls) if " run --rm" in call)
    restart_index = next(
        index for index, call in enumerate(calls) if " up -d --wait worker" in call
    )
    assert stop_index < backup_index < restart_index


def test_bash_archive_backup_failure_restarts_worker_under_bounded_watchdog(
    tmp_path: Path,
) -> None:
    if os.name == "nt":
        git_bash = Path("C:/Program Files/Git/bin/bash.exe")
        bash = str(git_bash) if git_bash.is_file() else None
    else:
        bash = shutil.which("bash")
    if bash is None:
        pytest.skip("Git/POSIX Bash is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "docker-calls.txt"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
printf '%s\n' "$*" >>"$MAKOLET_CAPTURE"
case " $* " in
  *" volume create "*)
    for argument in "$@"; do
      case "$argument" in
        com.makolet.archive-backup-lock.owner=*)
          printf '%s\n' "${argument#*=}" >"$MAKOLET_CAPTURE.lock"
          ;;
      esac
    done
    printf '%s\n' "${@: -1}"
    exit 0
    ;;
  *" volume inspect "*) cat "$MAKOLET_CAPTURE.lock"; exit 0 ;;
  *" volume rm "*) rm -f "$MAKOLET_CAPTURE.lock"; printf '%s\n' "${@: -1}"; exit 0 ;;
  *" ps --status running --status restarting --services "*)
    [[ -e "$MAKOLET_CAPTURE.worker-stopped" ]] || printf 'worker\n'
    exit 0
    ;;
  *" stop worker "*) touch "$MAKOLET_CAPTURE.worker-stopped"; exit 0 ;;
  *" up -d --wait worker "*) sleep 10; exit 0 ;;
  *" run --rm "*) exit 7 ;;
esac
exit 0
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    key_file = tmp_path / "protected" / "archive.key"
    key_file.parent.mkdir()
    key_file.write_bytes(bytes(range(32)))
    destination = tmp_path / "backup"

    def bash_path(path: Path) -> str:
        resolved = path.resolve().as_posix()
        if os.name == "nt":
            return f"/{resolved[0].lower()}{resolved[2:]}"
        return resolved

    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "MAKOLET_COMPOSE_ENV_FILE",
    ):
        environment.pop(selector, None)
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["MAKOLET_CAPTURE"] = bash_path(capture)
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = bash_path(key_file)
    environment["MAKOLET_ARCHIVE_WORKER_RESTART_TIMEOUT_SECONDS"] = "1"
    started_at = time.monotonic()

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            bash,
            str(_REPOSITORY_ROOT / "scripts" / "archive-backup.sh"),
            bash_path(destination),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=8,
    )

    assert completed.returncode != 0
    assert time.monotonic() - started_at < 7
    assert "bounded watchdog" in completed.stderr
    calls = capture.read_text(encoding="utf-8").splitlines()
    stop_indexes = [index for index, call in enumerate(calls) if " stop worker" in call]
    assert len(stop_indexes) == 2
    stop_index = stop_indexes[0]
    backup_index = next(index for index, call in enumerate(calls) if " run --rm" in call)
    restart_index = next(
        index for index, call in enumerate(calls) if " up -d --wait worker" in call
    )
    assert stop_index < backup_index < restart_index
    assert restart_index < stop_indexes[1]
    assert "stopped after archive backup restart failure" in completed.stderr
    assert Path(f"{capture}.lock").is_file()
    assert not any(call.startswith("volume rm ") for call in calls)


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell watchdog harness")
def test_powershell_archive_failure_restarts_worker_under_bounded_watchdog(
    tmp_path: Path,
) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "docker-calls.txt"
    key_file = tmp_path / "protected" / "archive.key"
    key_file.parent.mkdir()
    key_file.write_bytes(bytes(range(32)))
    destination = tmp_path / "backup"
    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "MAKOLET_COMPOSE_ENV_FILE",
    ):
        environment.pop(selector, None)
    _configure_windows_docker_shim(environment, fake_bin, mode="restart-stall")
    environment["MAKOLET_CAPTURE"] = str(capture)
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = str(key_file)
    environment["MAKOLET_ARCHIVE_WORKER_RESTART_TIMEOUT_SECONDS"] = "1"
    started_at = time.monotonic()

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(_REPOSITORY_ROOT / "scripts" / "operations.ps1"),
            "archive-backup",
            str(destination),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode != 0
    assert time.monotonic() - started_at < 9
    assert "bounded watchdog" in completed.stderr
    calls = capture.read_text(encoding="utf-8").splitlines()
    stop_indexes = [index for index, call in enumerate(calls) if " stop worker" in call]
    assert len(stop_indexes) == 2
    assert any(" run --rm" in call for call in calls)
    restart_index = next(
        index for index, call in enumerate(calls) if " up -d --wait worker" in call
    )
    assert restart_index < stop_indexes[1]
    assert "stopped after archive backup restart failure" in completed.stderr
    assert Path(f"{capture}.lock").is_file()
    assert not any(call.startswith("volume rm ") for call in calls)


@pytest.mark.parametrize("stall_phase", ["stop", "run", "cleanup"])
def test_bash_archive_watchdogs_restart_a_previously_running_worker(
    tmp_path: Path,
    stall_phase: str,
) -> None:
    if os.name == "nt":
        git_bash = Path("C:/Program Files/Git/bin/bash.exe")
        bash = str(git_bash) if git_bash.is_file() else None
    else:
        bash = shutil.which("bash")
    if bash is None:
        pytest.skip("Git/POSIX Bash is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "docker-calls.txt"
    container_state = tmp_path / "archive-container-state"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
printf '%s\n' "$*" >>"$MAKOLET_CAPTURE"
case " $* " in
  *" volume create "*)
    for argument in "$@"; do
      case "$argument" in
        com.makolet.archive-backup-lock.owner=*)
          printf '%s\n' "${argument#*=}" >"$MAKOLET_CAPTURE.lock"
          ;;
      esac
    done
    printf '%s\n' "${@: -1}"
    exit 0
    ;;
  *" volume inspect "*) cat "$MAKOLET_CAPTURE.lock"; exit 0 ;;
  *" volume rm "*) rm -f "$MAKOLET_CAPTURE.lock"; printf '%s\n' "${@: -1}"; exit 0 ;;
  *" ps --status running --status restarting --services "*)
    [[ -e "$MAKOLET_CAPTURE.worker-stopped" ]] || printf 'worker\n'
    exit 0
    ;;
  *" stop worker "*)
    if [[ "$MAKOLET_STALL_PHASE" == stop ]]; then
      exec >/dev/null 2>&1
      while :; do :; done
    fi
    touch "$MAKOLET_CAPTURE.worker-stopped"
    exit 0
    ;;
  *" container rm --force "*)
    if [[ "$MAKOLET_STALL_PHASE" == cleanup ]]; then
      exec >/dev/null 2>&1
      while :; do :; done
    fi
    rm -f "$MAKOLET_CONTAINER_STATE"
    exit 0
    ;;
  *" up -d --wait worker "*)
    [[ ! -e "$MAKOLET_CONTAINER_STATE" ]] || exit
    rm -f "$MAKOLET_CAPTURE.worker-stopped"
    exit 0
    ;;
  *" run --rm "*)
    if [[ "$MAKOLET_STALL_PHASE" == cleanup ]]; then
      printf 'live\n' >"$MAKOLET_CONTAINER_STATE"
      exit 7
    fi
    if [[ "$MAKOLET_STALL_PHASE" != run ]]; then
      exit 0
    fi
    printf 'live\n' >"$MAKOLET_CONTAINER_STATE"
    exec >/dev/null 2>&1
    while :; do :; done
    ;;
esac
exit 0
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    key_file = tmp_path / "protected" / "archive.key"
    key_file.parent.mkdir()
    key_file.write_bytes(bytes(range(32)))
    destination = tmp_path / "backup"

    def bash_path(path: Path) -> str:
        resolved = path.resolve().as_posix()
        if os.name == "nt":
            return f"/{resolved[0].lower()}{resolved[2:]}"
        return resolved

    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "MAKOLET_COMPOSE_ENV_FILE",
    ):
        environment.pop(selector, None)
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["MAKOLET_CAPTURE"] = bash_path(capture)
    environment["MAKOLET_CONTAINER_STATE"] = bash_path(container_state)
    environment["MAKOLET_STALL_PHASE"] = stall_phase
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = bash_path(key_file)
    environment["MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS"] = "0.1"
    environment["MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS"] = "0.1"
    environment["MAKOLET_ARCHIVE_WORKER_RESTART_TIMEOUT_SECONDS"] = "1"
    started_at = time.monotonic()

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            bash,
            str(_REPOSITORY_ROOT / "scripts" / "archive-backup.sh"),
            bash_path(destination),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=6,
    )

    assert completed.returncode != 0
    assert time.monotonic() - started_at < 5
    assert "bounded watchdog" in completed.stderr
    calls = capture.read_text(encoding="utf-8").splitlines()
    stop_index = next(index for index, call in enumerate(calls) if " stop worker" in call)
    if stall_phase == "stop":
        assert not any(" up -d --wait worker" in call for call in calls)
        assert not any(" run --rm" in call for call in calls)
        assert not any("container rm --force" in call for call in calls)
        assert Path(f"{capture}.lock").is_file()
        assert "lock intentionally retained" in completed.stderr
        assert not container_state.exists()
    elif stall_phase == "run":
        restart_index = next(
            index for index, call in enumerate(calls) if " up -d --wait worker" in call
        )
        backup_index = next(index for index, call in enumerate(calls) if " run --rm" in call)
        cleanup_index = next(
            index for index, call in enumerate(calls) if "container rm --force" in call
        )
        assert stop_index < backup_index < cleanup_index < restart_index
        backup_arguments = calls[backup_index].split()
        container_name = backup_arguments[backup_arguments.index("--name") + 1]
        assert container_name.startswith("makolet-archive-backup-")
        assert calls[cleanup_index].endswith(f"container rm --force {container_name}")
        assert not container_state.exists()
    else:
        backup_index = next(index for index, call in enumerate(calls) if " run --rm" in call)
        cleanup_index = next(
            index for index, call in enumerate(calls) if "container rm --force" in call
        )
        assert stop_index < backup_index < cleanup_index
        assert not any(" up -d --wait worker" in call for call in calls)
        backup_arguments = calls[backup_index].split()
        container_name = backup_arguments[backup_arguments.index("--name") + 1]
        assert container_name in completed.stderr
        assert "restart suppressed" in completed.stderr
        assert container_state.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell watchdog harness")
@pytest.mark.parametrize("stall_phase", ["stop", "run", "cleanup"])
def test_powershell_archive_watchdogs_restart_a_previously_running_worker(
    tmp_path: Path,
    stall_phase: str,
) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "docker-calls.txt"
    container_state = tmp_path / "archive-container-state"
    key_file = tmp_path / "protected" / "archive.key"
    key_file.parent.mkdir()
    key_file.write_bytes(bytes(range(32)))
    destination = tmp_path / "backup"
    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "MAKOLET_COMPOSE_ENV_FILE",
    ):
        environment.pop(selector, None)
    _configure_windows_docker_shim(environment, fake_bin, mode="phase-stall")
    environment["MAKOLET_CAPTURE"] = str(capture)
    environment["MAKOLET_CONTAINER_STATE"] = str(container_state)
    environment["MAKOLET_STALL_PHASE"] = stall_phase
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = str(key_file)
    environment["MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS"] = "0.1"
    environment["MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS"] = "0.1"
    environment["MAKOLET_ARCHIVE_WORKER_RESTART_TIMEOUT_SECONDS"] = "1"
    started_at = time.monotonic()

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(_REPOSITORY_ROOT / "scripts" / "operations.ps1"),
            "archive-backup",
            str(destination),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=8,
    )

    assert completed.returncode != 0
    assert time.monotonic() - started_at < 7
    assert "bounded watchdog" in completed.stderr
    calls = capture.read_text(encoding="utf-8").splitlines()
    stop_index = next(index for index, call in enumerate(calls) if " stop worker" in call)
    if stall_phase == "stop":
        assert not any(" up -d --wait worker" in call for call in calls)
        assert not any(" run --rm" in call for call in calls)
        assert not any("container rm --force" in call for call in calls)
        assert Path(f"{capture}.lock").is_file()
        assert "lock intentionally retained" in completed.stderr
        assert not container_state.exists()
    elif stall_phase == "run":
        restart_index = next(
            index for index, call in enumerate(calls) if " up -d --wait worker" in call
        )
        backup_index = next(index for index, call in enumerate(calls) if " run --rm" in call)
        cleanup_index = next(
            index for index, call in enumerate(calls) if "container rm --force" in call
        )
        assert stop_index < backup_index < cleanup_index < restart_index
        backup_arguments = calls[backup_index].split()
        container_name = backup_arguments[backup_arguments.index("--name") + 1]
        assert container_name.startswith("makolet-archive-backup-")
        assert calls[cleanup_index].endswith(f"container rm --force {container_name}")
        assert not container_state.exists()
    else:
        backup_index = next(index for index, call in enumerate(calls) if " run --rm" in call)
        cleanup_index = next(
            index for index, call in enumerate(calls) if "container rm --force" in call
        )
        assert stop_index < backup_index < cleanup_index
        assert not any(" up -d --wait worker" in call for call in calls)
        backup_arguments = calls[backup_index].split()
        container_name = backup_arguments[backup_arguments.index("--name") + 1]
        assert container_name in completed.stderr
        assert "restart suppressed" in completed.stderr
        assert container_state.exists()


def test_bash_overlapping_backups_serialize_a_restarting_worker(tmp_path: Path) -> None:
    if os.name == "nt":
        git_bash = Path("C:/Program Files/Git/bin/bash.exe")
        bash = str(git_bash) if git_bash.is_file() else None
    else:
        bash = shutil.which("bash")
    if bash is None:
        pytest.skip("Git/POSIX Bash is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    state = tmp_path / "state"
    state.mkdir()
    (state / "worker-restarting").touch()
    capture = state / "docker-calls.txt"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$MAKOLET_OVERLAP_CAPTURE"
case " $* " in
  *" volume create "*)
    owner=
    for argument in "$@"; do
      case "$argument" in
        com.makolet.archive-backup-lock.owner=*) owner="${argument#*=}" ;;
      esac
    done
    if mkdir "$MAKOLET_OVERLAP_STATE/lock" 2>/dev/null; then
      printf '%s\n' "$owner" >"$MAKOLET_OVERLAP_STATE/lock/owner"
    fi
    printf '%s\n' "${@: -1}"
    exit 0
    ;;
  *" volume inspect "*)
    cat "$MAKOLET_OVERLAP_STATE/lock/owner"
    exit 0
    ;;
  *" volume rm "*)
    rm "$MAKOLET_OVERLAP_STATE/lock/owner"
    rmdir "$MAKOLET_OVERLAP_STATE/lock"
    printf '%s\n' "${@: -1}"
    exit 0
    ;;
  *" ps "*)
    [[ " $* " == *" --status running --status restarting --services "* ]] || exit 9
    if [[ -f "$MAKOLET_OVERLAP_STATE/worker-restarting" && \
          ! -f "$MAKOLET_OVERLAP_STATE/worker-stopped" ]]; then
      printf 'worker\n'
    fi
    exit 0
    ;;
  *" stop worker "*)
    : >"$MAKOLET_OVERLAP_STATE/worker-stopped"
    exit 0
    ;;
  *" run --rm "*)
    : >"$MAKOLET_OVERLAP_STATE/backup-held"
    sleep 4
    printf '{"status":"backed_up"}\n'
    exit 0
    ;;
  *" up -d --wait worker "*)
    rm -f "$MAKOLET_OVERLAP_STATE/worker-stopped"
    : >"$MAKOLET_OVERLAP_STATE/worker-restarted"
    exit 0
    ;;
  *" container rm --force "*) exit 0 ;;
esac
exit 0
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    key_file = tmp_path / "protected" / "archive.key"
    key_file.parent.mkdir()
    key_file.write_bytes(bytes(range(32)))
    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "MAKOLET_COMPOSE_ENV_FILE",
    ):
        environment.pop(selector, None)
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["MAKOLET_OVERLAP_CAPTURE"] = str(capture)
    environment["MAKOLET_OVERLAP_STATE"] = str(state)
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = str(key_file)
    command = [
        bash,
        str(_REPOSITORY_ROOT / "scripts" / "archive-backup.sh"),
    ]
    first = subprocess.Popen(  # noqa: S603 - controlled wrapper harness
        [*command, str(tmp_path / "first-backup")],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 8
        while not (state / "backup-held").exists() and first.poll() is None:
            if time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        assert (state / "backup-held").is_file()
        second = subprocess.run(  # noqa: S603 - controlled wrapper harness
            [*command, str(tmp_path / "second-backup")],
            cwd=_REPOSITORY_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
        first_stdout, first_stderr = first.communicate(timeout=12)
    except BaseException:
        if first.poll() is None:
            first.kill()
        first.communicate(timeout=5)
        raise

    assert second.returncode != 0
    assert "already owned by another operation" in second.stderr
    assert first.returncode == 0, first_stderr
    assert '{"status":"backed_up"}' in first_stdout
    calls = capture.read_text(encoding="utf-8").splitlines()
    assert sum(call.startswith("volume create ") for call in calls) == 2
    assert sum(" stop worker" in call for call in calls) == 1
    assert sum(" run --rm" in call for call in calls) == 1
    assert sum(" up -d --wait worker" in call for call in calls) == 1
    assert sum(call.startswith("volume rm ") for call in calls) == 1
    backup_index = next(index for index, call in enumerate(calls) if " run --rm" in call)
    competing_index = next(
        index
        for index, call in enumerate(calls[backup_index + 1 :], backup_index + 1)
        if call.startswith("volume create ")
    )
    restart_index = next(
        index for index, call in enumerate(calls) if " up -d --wait worker" in call
    )
    release_index = next(index for index, call in enumerate(calls) if call.startswith("volume rm "))
    assert backup_index < competing_index < restart_index < release_index
    assert (state / "worker-restarted").is_file()
    assert not (state / "worker-stopped").exists()
    assert not (state / "lock").exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell wrapper harness")
def test_powershell_overlapping_backups_serialize_a_restarting_worker(
    tmp_path: Path,
) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    state = tmp_path / "state"
    state.mkdir()
    (state / "worker-restarting").touch()
    capture = state / "docker-calls.txt"
    key_file = tmp_path / "protected" / "archive.key"
    key_file.parent.mkdir()
    key_file.write_bytes(bytes(range(32)))
    environment = dict(os.environ)
    for selector in (
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "MAKOLET_COMPOSE_ENV_FILE",
    ):
        environment.pop(selector, None)
    _configure_windows_docker_shim(environment, fake_bin, mode="overlap")
    environment["MAKOLET_OVERLAP_CAPTURE"] = str(capture)
    environment["MAKOLET_OVERLAP_STATE"] = str(state)
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = str(key_file)
    command = [
        powershell,
        "-NoProfile",
        "-NonInteractive",
        "-File",
        str(_REPOSITORY_ROOT / "scripts" / "operations.ps1"),
        "archive-backup",
    ]
    first = subprocess.Popen(  # noqa: S603 - controlled wrapper harness
        [*command, str(tmp_path / "first-backup")],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 8
        while not (state / "backup-held").exists() and first.poll() is None:
            if time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        assert (state / "backup-held").is_file()
        second = subprocess.run(  # noqa: S603 - controlled wrapper harness
            [*command, str(tmp_path / "second-backup")],
            cwd=_REPOSITORY_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
        first_stdout, first_stderr = first.communicate(timeout=12)
    except BaseException:
        if first.poll() is None:
            first.kill()
        first.communicate(timeout=5)
        raise

    assert second.returncode != 0
    assert "already owned by another operation" in second.stderr
    assert first.returncode == 0, first_stderr
    assert '{"status":"backed_up"}' in first_stdout
    calls = capture.read_text(encoding="utf-8").splitlines()
    assert sum(call.startswith("volume create ") for call in calls) == 2
    assert sum(" stop worker" in call for call in calls) == 1
    assert sum(" run --rm" in call for call in calls) == 1
    assert sum(" up -d --wait worker" in call for call in calls) == 1
    assert sum(call.startswith("volume rm ") for call in calls) == 1
    backup_index = next(index for index, call in enumerate(calls) if " run --rm" in call)
    competing_index = next(
        index
        for index, call in enumerate(calls[backup_index + 1 :], backup_index + 1)
        if call.startswith("volume create ")
    )
    restart_index = next(
        index for index, call in enumerate(calls) if " up -d --wait worker" in call
    )
    release_index = next(index for index, call in enumerate(calls) if call.startswith("volume rm "))
    assert backup_index < competing_index < restart_index < release_index
    assert (state / "worker-restarted").is_file()
    assert not (state / "worker-stopped").exists()
    assert not (state / "lock").exists()
