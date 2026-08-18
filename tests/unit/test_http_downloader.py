from __future__ import annotations

import gzip
import ssl
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import anyio
import httpx
import pytest
from anyio.streams.tls import TLSStream
from httpcore._backends.anyio import AnyIOStream

import makolet.adapters.download.http as http_download_module
from makolet.adapters.archive.local import LocalContentAddressedArchive
from makolet.adapters.download.http import HttpDownloader, RemoteAccessPolicy
from makolet.application.models import DownloadEvidence
from makolet.application.ports import (
    HTTP_CONTROL_BYTES_PER_ATTEMPT,
    MAXIMUM_HTTP_CONTROL_BYTES_PER_OPEN,
    MAXIMUM_HTTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
    DownloadSession,
)
from makolet.domain.enums import CompressionFormat, DocumentType, SourceProtocol
from makolet.domain.errors import (
    DownloadLimitError,
    SourceAccessError,
    SourceResponseError,
    UnsafeRemoteError,
)
from makolet.domain.models import RemoteFile

_EXPECTED_DNS_CONTROL_BYTES = 64 * 1024
_MAXIMUM_SUPPORTED_ARCHIVE_BYTES = 16 * 1024 * 1024 * 1024
_TLS_APPLICATION_DATA_BYTES_PER_RECORD = 16 * 1024
_TLS13_BYTES_PER_NORMAL_RECORD = 22
_EXPECTED_MAXIMUM_TLS13_FRAMING_BYTES = (
    _MAXIMUM_SUPPORTED_ARCHIVE_BYTES // _TLS_APPLICATION_DATA_BYTES_PER_RECORD
) * _TLS13_BYTES_PER_NORMAL_RECORD
_EXPECTED_HTTP_BYTES_PER_OPEN = (
    MAXIMUM_HTTP_CONTROL_BYTES_PER_OPEN + _EXPECTED_MAXIMUM_TLS13_FRAMING_BYTES
)
_EXPECTED_HTTP_RETRY_OVERHEAD_BYTES = 4 * _EXPECTED_HTTP_BYTES_PER_OPEN


class AdvancingClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 8, 11, tzinfo=UTC)

    def now(self) -> datetime:
        result = self.current
        self.current += timedelta(seconds=1)
        return result


class WireStream(httpx.AsyncByteStream):
    def __init__(self, *chunks: bytes) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


class SlowClosableStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"early"
        await anyio.sleep(0.05)
        yield b"late"

    async def aclose(self) -> None:
        self.closed = True


class RawTlsRecordStream(anyio.abc.ByteStream):
    def __init__(self, *incoming: bytes) -> None:
        self.incoming = list(incoming)
        self.sent: list[bytes] = []
        self.eof_sent = False
        self.closed = False

    async def receive(self, max_bytes: int = 65_536) -> bytes:
        if not self.incoming:
            raise anyio.EndOfStream
        value = self.incoming.pop(0)
        assert len(value) <= max_bytes
        return value

    async def send(self, item: bytes) -> None:
        self.sent.append(item)

    async def send_eof(self) -> None:
        self.eof_sent = True

    async def aclose(self) -> None:
        self.closed = True


class FakeTlsByteStream(anyio.abc.ByteStream):
    def __init__(self, raw_stream: anyio.abc.ByteStream) -> None:
        self._raw_stream = raw_stream

    async def receive(self, max_bytes: int = 65_536) -> bytes:
        encrypted = await self._raw_stream.receive(max_bytes)
        assert encrypted.startswith(b"tls-record:")
        return encrypted.removeprefix(b"tls-record:")

    async def send(self, item: bytes) -> None:
        await self._raw_stream.send(b"tls-record:" + item)

    async def send_eof(self) -> None:
        await self._raw_stream.send_eof()

    async def aclose(self) -> None:
        await self._raw_stream.aclose()


class UnsupportedTlsNetworkStream:
    def __init__(self, *, close_fails: bool = False) -> None:
        self.close_fails = close_fails
        self.close_attempted = False
        self.closed = False

    async def read(self, _max_bytes: int, _timeout: float | None = None) -> bytes:
        return b""

    async def write(self, _buffer: bytes, _timeout: float | None = None) -> None:
        return None

    async def aclose(self) -> None:
        self.close_attempted = True
        await anyio.lowlevel.checkpoint()
        if self.close_fails:
            raise RuntimeError("simulated transport close failure")
        self.closed = True

    async def start_tls(
        self,
        _ssl_context: ssl.SSLContext,
        _server_hostname: str | None = None,
        _timeout: float | None = None,
    ) -> UnsupportedTlsNetworkStream:
        raise AssertionError("unsupported delegate reached TLS")

    def get_extra_info(self, _info: str) -> object | None:
        return None


async def public_resolver(_host: str, _port: int) -> tuple[str, ...]:
    return ("93.184.216.34",)


def remote(url: str = "https://files.example/prices.gz") -> RemoteFile:
    return RemoteFile(
        retailer_id="demo",
        portal_id="demo-portal",
        protocol=SourceProtocol.HTTPS,
        remote_id="demo:prices.gz",
        download_url=url,
        original_filename="prices.gz",
        document_type=DocumentType.PRICE_FULL,
        compression=CompressionFormat.GZIP,
        discovered_at=datetime(2026, 8, 11, tzinfo=UTC),
    )


def downloader(
    handler: httpx.AsyncBaseTransport,
    *,
    policy: RemoteAccessPolicy | None = None,
    maximum_download_seconds: float = 600,
) -> tuple[httpx.AsyncClient, HttpDownloader]:
    client = httpx.AsyncClient(transport=handler)
    selected = policy or RemoteAccessPolicy(allowed_hosts=frozenset({"files.example"}))
    return client, HttpDownloader(
        client,
        AdvancingClock(),
        {"demo-portal": selected},
        resolver=public_resolver,
        maximum_download_seconds=maximum_download_seconds,
    )


async def consume(
    context: AbstractAsyncContextManager[DownloadSession],
) -> tuple[bytes, DownloadEvidence]:
    collected = bytearray()
    async with context as session:
        async for chunk in session.iter_raw():
            collected.extend(chunk)
        evidence = await session.finish(len(collected))
    return bytes(collected), evidence


@pytest.mark.asyncio
async def test_http_download_preserves_content_encoded_wire_bytes() -> None:
    compressed = gzip.compress(b"<Root />", mtime=0)

    async def response(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            stream=WireStream(compressed[:3], compressed[3:]),
            headers={
                "Content-Length": str(len(compressed)),
                "Content-Encoding": "gzip",
                "Content-Type": "application/xml; charset=utf-8",
                "ETag": '"abc"',
                "Last-Modified": "Tue, 11 Aug 2026 12:00:00 GMT",
                "Set-Cookie": "secret=never-record-this",
            },
        )

    client, subject = downloader(httpx.MockTransport(response))
    async with client:
        body, evidence = await consume(subject.open(remote()))

    assert body == compressed
    assert evidence.content_length == len(compressed)
    assert evidence.media_type == "application/xml"
    assert evidence.last_modified == datetime(2026, 8, 11, 12, tzinfo=UTC)
    assert all(name != "set-cookie" for name, _ in evidence.response_metadata)


@pytest.mark.asyncio
async def test_tls_meter_charges_ciphertext_handshake_and_records_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_stream = RawTlsRecordStream(
        b"server-handshake-ciphertext",
        b"tls-record:response",
        b"server-handshake-ciphertext",
    )
    observed_hostname: list[str | None] = []

    async def fake_tls_wrap(
        _stream_type: type[TLSStream],
        transport_stream: anyio.abc.ByteStream,
        *,
        server_side: bool | None = None,
        hostname: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        standard_compatible: bool = True,
    ) -> FakeTlsByteStream:
        del server_side, ssl_context, standard_compatible
        observed_hostname.append(hostname)
        await transport_stream.send(b"client-handshake-ciphertext")
        assert await transport_stream.receive() == b"server-handshake-ciphertext"
        return FakeTlsByteStream(transport_stream)

    monkeypatch.setattr(
        TLSStream,
        "wrap",
        classmethod(fake_tls_wrap),
    )
    meter = http_download_module.HttpTransferMeter(
        1024 * 1024,
        limit_error=lambda transferred_bytes: DownloadLimitError(
            "simulated TLS wire limit",
            transferred_bytes=transferred_bytes,
        ),
    )
    delegate = AnyIOStream(raw_stream)
    subject = http_download_module._MeteredNetworkStream(delegate, meter)

    secured = await subject.start_tls(
        ssl.create_default_context(),
        server_hostname="files.example",
    )
    handshake_bytes = len(b"client-handshake-ciphertext") + len(b"server-handshake-ciphertext")
    assert meter.transferred_bytes == handshake_bytes

    await secured.write(b"request")
    response = await secured.read(64 * 1024)

    assert response == b"response"
    assert observed_hostname == ["files.example"]
    assert raw_stream.sent == [
        b"client-handshake-ciphertext",
        b"tls-record:request",
    ]
    assert meter.transferred_bytes == (
        handshake_bytes + len(b"tls-record:request") + len(b"tls-record:response")
    )

    before_second_handshake = meter.transferred_bytes
    await subject.start_tls(
        ssl.create_default_context(),
        server_hostname="files.example",
    )
    assert meter.transferred_bytes == before_second_handshake + handshake_bytes
    assert observed_hostname == ["files.example", "files.example"]

    metered_raw = delegate._stream
    assert isinstance(metered_raw, http_download_module._MeteredTlsByteStream)
    assert metered_raw.extra_attributes == raw_stream.extra_attributes
    await metered_raw.send_eof()
    await metered_raw.aclose()
    assert raw_stream.eof_sent
    assert raw_stream.closed


@pytest.mark.asyncio
async def test_tls_meter_stops_oversized_ciphertext_during_handshake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_stream = RawTlsRecordStream(b"server-handshake-ciphertext")

    async def fake_tls_wrap(
        _stream_type: type[TLSStream],
        /,
        transport_stream: anyio.abc.ByteStream,
        **_kwargs: object,
    ) -> FakeTlsByteStream:
        await transport_stream.send(b"client-hello")
        await transport_stream.receive()
        raise AssertionError("oversized TLS handshake reached plaintext setup")

    monkeypatch.setattr(TLSStream, "wrap", classmethod(fake_tls_wrap))
    meter = http_download_module.HttpTransferMeter(
        16,
        limit_error=lambda transferred_bytes: DownloadLimitError(
            "simulated TLS wire limit",
            transferred_bytes=transferred_bytes,
        ),
    )
    subject = http_download_module._MeteredNetworkStream(AnyIOStream(raw_stream), meter)

    with pytest.raises(DownloadLimitError, match="simulated TLS wire limit") as caught:
        await subject.start_tls(
            ssl.create_default_context(),
            server_hostname="files.example",
        )

    assert caught.value.transferred_bytes == (
        len(b"client-hello") + len(b"server-handshake-ciphertext")
    )
    assert meter.transferred_bytes == caught.value.transferred_bytes


def test_http_transfer_meter_preflights_floors_and_rejects_invalid_charges() -> None:
    def limit_error(transferred_bytes: int) -> DownloadLimitError:
        return DownloadLimitError(
            "simulated HTTP wire limit",
            transferred_bytes=transferred_bytes,
        )

    with pytest.raises(ValueError, match="must be positive"):
        http_download_module.HttpTransferMeter(0, limit_error=limit_error)

    dns_limited = http_download_module.HttpTransferMeter(
        _EXPECTED_DNS_CONTROL_BYTES - 1,
        limit_error=limit_error,
    )
    with pytest.raises(DownloadLimitError) as dns_error:
        dns_limited.begin_dns_lookup()
    assert dns_error.value.transferred_bytes == 0

    attempt_limited = http_download_module.HttpTransferMeter(
        HTTP_CONTROL_BYTES_PER_ATTEMPT - 1,
        limit_error=limit_error,
    )
    with pytest.raises(DownloadLimitError) as attempt_error:
        attempt_limited.begin_attempt()
    assert attempt_error.value.transferred_bytes == 0

    charges: list[int] = []
    meter = http_download_module.HttpTransferMeter(
        1024 * 1024,
        limit_error=limit_error,
        on_charge=charges.append,
    )
    with pytest.raises(ValueError, match="wire charge"):
        meter.record_wire(-1)
    with pytest.raises(ValueError, match="payload charge"):
        meter.record_payload(-1)
    with pytest.raises(ValueError, match="response-control charge"):
        meter.record_response_control(-1)

    meter.begin_attempt()
    meter.record_response_control(HTTP_CONTROL_BYTES_PER_ATTEMPT + 1)
    assert meter.transferred_bytes == HTTP_CONTROL_BYTES_PER_ATTEMPT + 1
    assert charges == [HTTP_CONTROL_BYTES_PER_ATTEMPT, 1]


def test_http_retry_headroom_covers_supported_normal_tls13_record_framing() -> None:
    assert _EXPECTED_MAXIMUM_TLS13_FRAMING_BYTES == 22 * 1024 * 1024
    assert _EXPECTED_HTTP_BYTES_PER_OPEN == 24 * 1024 * 1024 + 256 * 1024
    assert MAXIMUM_HTTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES == (_EXPECTED_HTTP_RETRY_OVERHEAD_BYTES)


@pytest.mark.asyncio
async def test_http_default_meter_includes_supported_normal_tls13_record_framing() -> None:
    client, subject = downloader(
        httpx.MockTransport(lambda _: httpx.Response(200, content=b"ok")),
        policy=RemoteAccessPolicy(
            allowed_hosts=frozenset({"files.example"}),
            maximum_response_bytes=_MAXIMUM_SUPPORTED_ARCHIVE_BYTES,
        ),
    )

    async with client, subject.open(remote()) as session:
        session_state: Any = session
        assert session_state._meter.maximum_bytes == (
            _MAXIMUM_SUPPORTED_ARCHIVE_BYTES + _EXPECTED_HTTP_BYTES_PER_OPEN
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("close_fails", [False, True])
async def test_unsupported_tls_delegate_is_closed_without_hiding_security_error(
    close_fails: bool,
) -> None:
    delegate = UnsupportedTlsNetworkStream(close_fails=close_fails)
    meter = http_download_module.HttpTransferMeter(
        1024,
        limit_error=lambda transferred_bytes: DownloadLimitError(
            "simulated TLS wire limit",
            transferred_bytes=transferred_bytes,
        ),
    )
    subject = http_download_module._MeteredNetworkStream(delegate, meter)

    with pytest.raises(SourceAccessError, match="cannot enforce raw-byte accounting"):
        await subject.start_tls(
            ssl.create_default_context(),
            server_hostname="files.example",
        )

    assert delegate.close_attempted
    assert delegate.closed is not close_fails


@pytest.mark.asyncio
async def test_unsupported_tls_delegate_close_is_shielded_from_active_cancellation() -> None:
    delegate = UnsupportedTlsNetworkStream()
    meter = http_download_module.HttpTransferMeter(
        1024,
        limit_error=lambda transferred_bytes: DownloadLimitError(
            "simulated TLS wire limit",
            transferred_bytes=transferred_bytes,
        ),
    )
    subject = http_download_module._MeteredNetworkStream(delegate, meter)

    with anyio.CancelScope() as cancel_scope:
        cancel_scope.cancel()
        with pytest.raises(SourceAccessError, match="cannot enforce raw-byte accounting"):
            await subject.start_tls(
                ssl.create_default_context(),
                server_hostname="files.example",
            )

    assert delegate.close_attempted
    assert delegate.closed


@pytest.mark.asyncio
async def test_http_download_validates_each_redirect_host() -> None:
    async def response(request: httpx.Request) -> httpx.Response:
        if request.headers["host"] == "files.example":
            return httpx.Response(302, headers={"Location": "https://blob.example/object"})
        return httpx.Response(200, stream=WireStream(b"ok"), headers={"Content-Length": "2"})

    policy = RemoteAccessPolicy(
        allowed_hosts=frozenset({"files.example"}),
        redirect_hosts=frozenset({"blob.example"}),
    )
    client, subject = downloader(httpx.MockTransport(response), policy=policy)
    async with client:
        body, _ = await consume(subject.open(remote()))
    assert body == b"ok"


@pytest.mark.asyncio
async def test_http_download_pins_vetted_address_and_preserves_host_and_tls_sni() -> None:
    seen: list[httpx.Request] = []

    async def response(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, stream=WireStream(b"ok"), headers={"Content-Length": "2"})

    client, subject = downloader(httpx.MockTransport(response))
    async with client:
        body, _ = await consume(subject.open(remote()))

    assert body == b"ok"
    assert seen[0].url.host == "93.184.216.34"
    assert seen[0].headers["host"] == "files.example"
    assert seen[0].extensions["sni_hostname"] == "files.example"
    assert seen[0].headers["connection"] == "close"


@pytest.mark.asyncio
async def test_http_download_rejects_redirect_outside_allowlist() -> None:
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(302, headers={"Location": "http://127.0.0.1/admin"})
    )
    client, subject = downloader(transport)
    async with client:
        with pytest.raises(UnsafeRemoteError, match="allowlist"):
            async with subject.open(remote()):
                pass


@pytest.mark.asyncio
async def test_http_download_rejects_private_dns_result() -> None:
    async def private_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return ("10.0.0.1",)

    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    subject = HttpDownloader(
        client,
        AdvancingClock(),
        {"demo-portal": RemoteAccessPolicy(allowed_hosts=frozenset({"files.example"}))},
        resolver=private_resolver,
    )
    async with client:
        with pytest.raises(UnsafeRemoteError, match="non-public") as caught:
            async with subject.open(remote()):
                pass

    assert caught.value.transferred_bytes == _EXPECTED_DNS_CONTROL_BYTES


@pytest.mark.asyncio
async def test_http_download_rejects_excess_dns_answers_before_send() -> None:
    requested = False

    async def excessive_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return tuple(f"93.184.216.{index}" for index in range(30, 35))

    def response(_request: httpx.Request) -> httpx.Response:
        nonlocal requested
        requested = True
        return httpx.Response(200, content=b"unexpected")

    client = httpx.AsyncClient(transport=httpx.MockTransport(response))
    subject = HttpDownloader(
        client,
        AdvancingClock(),
        {"demo-portal": RemoteAccessPolicy(allowed_hosts=frozenset({"files.example"}))},
        resolver=excessive_resolver,
    )

    async with client:
        with pytest.raises(UnsafeRemoteError, match="too many addresses"):
            async with subject.open(remote()):
                pass

    assert not requested


@pytest.mark.asyncio
async def test_http_download_charges_each_address_attempt_and_preserves_failover() -> None:
    attempted_hosts: list[str] = []

    async def resolver(_host: str, _port: int) -> tuple[str, ...]:
        return ("93.184.216.34", "93.184.216.35")

    def response(request: httpx.Request) -> httpx.Response:
        attempted_hosts.append(request.url.host)
        if request.url.host == "93.184.216.34":
            raise httpx.ConnectError("first vetted address unavailable", request=request)
        return httpx.Response(200, stream=WireStream(b"ok"), headers={"Content-Length": "2"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(response))
    subject = HttpDownloader(
        client,
        AdvancingClock(),
        {"demo-portal": RemoteAccessPolicy(allowed_hosts=frozenset({"files.example"}))},
        resolver=resolver,
    )
    maximum_bytes = _EXPECTED_DNS_CONTROL_BYTES + 2 * HTTP_CONTROL_BYTES_PER_ATTEMPT + 2

    async with client, subject.open(remote(), maximum_bytes=maximum_bytes) as session:
        body = b"".join([chunk async for chunk in session.iter_raw()])
        await session.finish(len(body))

    assert body == b"ok"
    assert attempted_hosts == ["93.184.216.34", "93.184.216.35"]
    assert session.transferred_bytes == maximum_bytes


@pytest.mark.asyncio
async def test_http_download_transport_failure_reports_control_charge_for_retry_settlement() -> (
    None
):
    def response(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("publisher unavailable", request=request)

    client, subject = downloader(httpx.MockTransport(response))
    async with client:
        with pytest.raises(SourceAccessError, match="request failed") as caught:
            async with subject.open(remote()):
                pass

    assert caught.value.transferred_bytes == (
        _EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT
    )


@pytest.mark.asyncio
async def test_http_download_rejects_oversized_declared_response() -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(200, headers={"Content-Length": "101"}, content=b"")
    )
    policy = RemoteAccessPolicy(
        allowed_hosts=frozenset({"files.example"}), maximum_response_bytes=100
    )
    client, subject = downloader(transport, policy=policy)
    async with client:
        with pytest.raises(DownloadLimitError, match="declares") as caught:
            async with subject.open(remote()):
                pass

    assert caught.value.budget_limited is False


@pytest.mark.asyncio
async def test_http_download_uses_smaller_collection_limit_for_declared_length() -> None:
    requested = False

    async def response(_request: httpx.Request) -> httpx.Response:
        nonlocal requested
        requested = True
        return httpx.Response(
            200,
            stream=WireStream(b"never-consumed"),
            headers={"Content-Length": "11"},
        )

    client, subject = downloader(
        httpx.MockTransport(response),
        policy=RemoteAccessPolicy(
            allowed_hosts=frozenset({"files.example"}),
            maximum_response_bytes=100,
        ),
    )
    async with client:
        with pytest.raises(DownloadLimitError, match="declares more than 10") as caught:
            async with subject.open(
                remote(),
                maximum_bytes=(_EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + 10),
            ):
                pass

    assert requested
    assert caught.value.budget_limited is True


@pytest.mark.asyncio
async def test_http_download_uses_smaller_collection_limit_for_streamed_bytes() -> None:
    client, subject = downloader(
        httpx.MockTransport(lambda _: httpx.Response(200, stream=WireStream(b"123456", b"78901"))),
        policy=RemoteAccessPolicy(
            allowed_hosts=frozenset({"files.example"}),
            maximum_response_bytes=100,
        ),
    )

    async with (
        client,
        subject.open(
            remote(),
            maximum_bytes=(_EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + 10),
        ) as session,
    ):
        chunks = session.iter_raw()
        first_chunk = await anext(chunks)
        with pytest.raises(DownloadLimitError, match="exceeds 10 downloaded bytes") as caught:
            await anext(chunks)

    assert first_chunk == b"123456"
    assert caught.value.transferred_bytes == (
        _EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + 11
    )
    assert caught.value.budget_limited is True
    assert session.transferred_bytes == (
        _EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + 11
    )


@pytest.mark.asyncio
async def test_http_hostile_large_transport_frame_is_rechunked_to_bounded_evidence() -> None:
    payload = b"x" * (3 * 64 * 1024)
    client, subject = downloader(
        httpx.MockTransport(lambda _: httpx.Response(200, stream=WireStream(payload))),
        policy=RemoteAccessPolicy(
            allowed_hosts=frozenset({"files.example"}),
            maximum_response_bytes=len(payload),
        ),
    )

    async with (
        client,
        subject.open(
            remote(),
            maximum_bytes=(_EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + 10),
        ) as session,
    ):
        with pytest.raises(DownloadLimitError) as caught:
            _ = [chunk async for chunk in session.iter_raw()]

    assert caught.value.transferred_bytes == (
        _EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + 64 * 1024
    )
    assert session.transferred_bytes == (
        _EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + 64 * 1024
    )


@pytest.mark.asyncio
async def test_http_download_accepts_body_exactly_at_collection_limit() -> None:
    payload = b"0123456789"
    client, subject = downloader(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                stream=WireStream(payload),
                headers={"Content-Length": str(len(payload))},
            )
        ),
        policy=RemoteAccessPolicy(
            allowed_hosts=frozenset({"files.example"}),
            maximum_response_bytes=100,
        ),
    )

    async with client:
        body, evidence = await consume(
            subject.open(
                remote(),
                maximum_bytes=(
                    _EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + len(payload)
                ),
            )
        )

    assert body == payload
    assert evidence.content_length == len(payload)


@pytest.mark.asyncio
async def test_http_download_cannot_be_finished_without_consumption() -> None:
    client, subject = downloader(httpx.MockTransport(lambda _: httpx.Response(200, content=b"x")))
    async with client, subject.open(remote()) as session:
        with pytest.raises(SourceResponseError, match="not consumed"):
            await session.finish(0)


@pytest.mark.asyncio
async def test_http_declared_length_mismatch_fails_before_archive_commit(tmp_path: Path) -> None:
    client, subject = downloader(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                stream=WireStream(b"short"),
                headers={"Content-Length": "6"},
            )
        )
    )

    archive = LocalContentAddressedArchive(tmp_path / "raw")
    source = remote()
    async with client, subject.open(source) as session:
        with pytest.raises(SourceResponseError, match="declared content length"):
            await archive.put(session.iter_raw(), original_filename=source.original_filename)

    assert [path for path in (tmp_path / "raw").rglob("*") if path.is_file()] == []


@pytest.mark.asyncio
async def test_http_download_total_deadline_closes_the_live_response() -> None:
    stream = SlowClosableStream()
    client, subject = downloader(
        httpx.MockTransport(lambda _: httpx.Response(200, stream=stream)),
        maximum_download_seconds=0.001,
    )

    async with client:
        with pytest.raises(SourceAccessError, match="total deadline") as caught:
            await consume(subject.open(remote()))

    assert stream.closed
    assert caught.value.transferred_bytes == (
        _EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + len(b"early")
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("port", [0, 80])
async def test_http_download_rejects_https_on_non_default_port_before_resolution(
    port: int,
) -> None:
    resolved = False

    async def resolver(_host: str, _port: int) -> tuple[str, ...]:
        nonlocal resolved
        resolved = True
        return ("93.184.216.34",)

    policy = RemoteAccessPolicy(
        allowed_hosts=frozenset({"files.example"}),
        allowed_ports=frozenset({80, 443}),
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    subject = HttpDownloader(
        client,
        AdvancingClock(),
        {"demo-portal": policy},
        resolver=resolver,
    )

    async with client:
        with pytest.raises(UnsafeRemoteError, match="disallowed port"):
            await consume(subject.open(remote(f"https://files.example:{port}/prices.gz")))

    assert not resolved


@pytest.mark.asyncio
@pytest.mark.parametrize("port", [0, 80])
async def test_http_download_rejects_redirect_to_non_default_https_port_before_second_request(
    port: int,
) -> None:
    requests: list[httpx.Request] = []
    resolutions: list[tuple[str, int]] = []

    def response(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            302,
            headers={"Location": f"https://blob.example:{port}/object"},
        )

    async def resolver(host: str, port: int) -> tuple[str, ...]:
        resolutions.append((host, port))
        return ("93.184.216.34",)

    policy = RemoteAccessPolicy(
        allowed_hosts=frozenset({"files.example"}),
        redirect_hosts=frozenset({"blob.example"}),
        allowed_ports=frozenset({80, 443}),
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(response))
    subject = HttpDownloader(
        client,
        AdvancingClock(),
        {"demo-portal": policy},
        resolver=resolver,
    )

    async with client:
        with pytest.raises(UnsafeRemoteError, match="disallowed port"):
            await consume(subject.open(remote()))

    assert len(requests) == 1
    assert resolutions == [("files.example", 443)]


@pytest.mark.asyncio
async def test_http_download_total_deadline_includes_hostname_resolution() -> None:
    async def slow_resolver(_host: str, _port: int) -> tuple[str, ...]:
        await anyio.sleep(0.05)
        return ("93.184.216.34",)

    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    subject = HttpDownloader(
        client,
        AdvancingClock(),
        {"demo-portal": RemoteAccessPolicy(allowed_hosts=frozenset({"files.example"}))},
        resolver=slow_resolver,
        maximum_download_seconds=0.001,
    )

    async with client:
        with pytest.raises(SourceAccessError, match="total deadline"):
            async with subject.open(remote()):
                pass


@pytest.mark.asyncio
async def test_http_download_closes_response_when_header_validation_fails() -> None:
    stream = SlowClosableStream()
    client, subject = downloader(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                stream=stream,
                headers={"Content-Length": "invalid"},
            )
        )
    )

    async with client:
        with pytest.raises(SourceResponseError, match="Content-Length"):
            async with subject.open(remote()):
                pass

    assert stream.closed
