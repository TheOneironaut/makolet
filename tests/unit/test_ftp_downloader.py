from __future__ import annotations

import ftplib
import socket
import ssl
import threading
from collections.abc import Awaitable, Buffer, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import ClassVar, cast

import anyio
import pytest

from makolet.adapters.archive.local import LocalContentAddressedArchive
from makolet.adapters.download._blocking import (
    BlockingOperationCancellation,
    run_bounded_blocking,
)
from makolet.adapters.download.ftp import FtpDownloader
from makolet.adapters.filesystem_capacity import CAPACITY_LOCK_FILENAME
from makolet.adapters.ftp_control import (
    FtpControlBudget,
    FtpControlLimitError,
    install_bounded_ftp_control_reader,
    install_metered_ftp_tls_context,
)
from makolet.adapters.sources.ncr import FtpCredentials, NcrFeedConfig
from makolet.application.models import DownloadEvidence
from makolet.application.ports import (
    MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
    DownloadSession,
)
from makolet.domain.enums import CompressionFormat, DocumentType, SourceProtocol
from makolet.domain.errors import (
    ArchiveCapacityError,
    DomainValidationError,
    DownloadLimitError,
    SourceAccessError,
    SourceBlockedError,
    SourceResponseError,
    UnsafeRemoteError,
)
from makolet.domain.models import RemoteFile


class AdvancingClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 8, 11, tzinfo=UTC)

    def now(self) -> datetime:
        result = self.current
        self.current += timedelta(seconds=1)
        return result


class StaticCredentials:
    def get(self, key: str) -> FtpCredentials:
        assert key == "DEMO"
        return FtpCredentials("public-user", "public-password")


class FakeFtp:
    payload = b"exact\x00wire\xffbytes"
    latest: FakeFtp | None = None

    def __init__(self, *, timeout: float, **_kwargs: object) -> None:
        self.timeout = timeout
        self.context = _kwargs.get("context")
        self.passive: bool | None = None
        self.directory: str | None = None
        self.connected_address: str | None = None
        self.host: str | None = None
        self.protected = False
        self.closed = False
        type(self).latest = self

    def connect(self, host: str, port: int, *, timeout: float) -> str:
        assert host == "93.184.216.34"
        assert port == 21
        assert timeout == self.timeout
        self.connected_address = host
        self.host = host
        return "220 ready"

    def login(self, username: str, password: str) -> str:
        assert (username, password) == ("public-user", "public-password")
        return "230 logged in"

    def prot_p(self) -> str:
        self.protected = True
        return "200 protected"

    def set_pasv(self, passive: bool) -> None:
        self.passive = passive

    def cwd(self, directory: str) -> str:
        self.directory = directory
        return "250 changed"

    def voidcmd(self, command: str) -> str:
        assert command == "TYPE I"
        return "200 type set"

    def transfercmd(self, command: str) -> FakeDataConnection:
        assert command == "RETR Price7290000000008-001-202608111200.GZ"
        return FakeDataConnection(self.payload)

    def voidresp(self) -> str:
        return "226 complete"

    def quit(self) -> str:
        self.closed = True
        return "221 bye"

    def close(self) -> None:
        self.closed = True


class FakeFtpTls(FakeFtp):
    latest: FakeFtpTls | None = None


class BlockingFtp(FakeFtp):
    active = threading.Event()
    released = threading.Event()
    data_connection: BlockingDataConnection | None = None

    def transfercmd(self, command: str) -> BlockingDataConnection:
        assert command == "RETR Price7290000000008-001-202608111200.GZ"
        connection = BlockingDataConnection(self.active, self.released)
        type(self).data_connection = connection
        return connection

    def close(self) -> None:
        super().close()
        self.released.set()


class OvershootingFtp(FakeFtp):
    payload = b"12345678901"


class PartialPermissionFtp(FakeFtp):
    payload = b"partial"

    def voidresp(self) -> str:
        raise ftplib.error_perm("550 permission changed")  # noqa: S321


class FakeDataConnection:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0
        self.closed = False

    def recv(self, amount: int) -> bytes:
        result = self._payload[self._offset : self._offset + amount]
        self._offset += len(result)
        return result

    def close(self) -> None:
        self.closed = True


class BlockingDataConnection(FakeDataConnection):
    def __init__(self, active: threading.Event, released: threading.Event) -> None:
        super().__init__(b"partial")
        self._active = active
        self._released = released

    def recv(self, amount: int) -> bytes:
        chunk = super().recv(amount)
        if chunk:
            return chunk
        self._active.set()
        self._released.wait(timeout=5)
        self._active.clear()
        return b""

    def close(self) -> None:
        super().close()
        self._released.set()


class ScriptedControlFtp(ftplib.FTP):
    response_sets: ClassVar[list[tuple[str, ...]]] = []
    payload = FakeFtp.payload
    latest: ScriptedControlFtp | None = None

    def __init__(self, *, timeout: float, **_kwargs: object) -> None:
        super().__init__(timeout=timeout)
        responses = type(self).response_sets.pop(0)
        self.file = StringIO("".join(responses))
        self.closed = False
        type(self).latest = self

    def connect(
        self,
        host: str = "",
        port: int = 0,
        timeout: float = -999,
        source_address: tuple[str, int] | None = None,
    ) -> str:
        assert source_address is None
        self.host = host
        self.port = port
        self.timeout = timeout
        return self.getresp()

    def putcmd(self, _line: str) -> None:
        return None

    def transfercmd(
        self,
        command: str,
        rest: int | str | None = None,
    ) -> socket.socket:
        assert rest is None
        response = self.sendcmd(command)
        assert response.startswith("1")
        return cast(socket.socket, FakeDataConnection(self.payload))

    def close(self) -> None:
        self.closed = True


class ScriptedControlFtpTls(ftplib.FTP_TLS):
    response_sets: ClassVar[list[tuple[str, ...]]] = []
    payload = FakeFtp.payload
    latest: ScriptedControlFtpTls | None = None

    def __init__(self, *, timeout: float, **kwargs: object) -> None:
        context = kwargs.get("context")
        assert isinstance(context, ssl.SSLContext)
        super().__init__(timeout=timeout, context=context)
        responses = type(self).response_sets.pop(0)
        self.file = StringIO("".join(responses))
        self.closed = False
        type(self).latest = self

    def connect(
        self,
        host: str = "",
        port: int = 0,
        timeout: float = -999,
        source_address: tuple[str, int] | None = None,
    ) -> str:
        assert source_address is None
        self.host = host
        self.port = port
        self.timeout = timeout
        return self.getresp()

    def auth(self) -> str:
        return self.voidcmd("AUTH TLS")

    def putcmd(self, _line: str) -> None:
        return None

    def transfercmd(
        self,
        command: str,
        rest: int | str | None = None,
    ) -> socket.socket:
        assert rest is None
        response = self.sendcmd(command)
        assert response.startswith("1")
        return cast(socket.socket, FakeDataConnection(self.payload))

    def close(self) -> None:
        self.closed = True


def _control_reply(code: str, *messages: str) -> str:
    assert messages
    if len(messages) == 1:
        return f"{code} {messages[0]}\r\n"
    first, *middle, last = messages
    return "".join(
        (
            f"{code}-{first}\r\n",
            *(f"notice {message}\r\n" for message in middle),
            f"{code} {last}\r\n",
        )
    )


def _download_control_responses(
    protocol: SourceProtocol,
    *,
    welcome: str | None = None,
    completion: str | None = None,
    quit_reply: str | None = None,
    multiline: bool = False,
) -> tuple[str, ...]:
    def reply(code: str, message: str) -> str:
        if multiline:
            return _control_reply(code, f"first {message}", message)
        return _control_reply(code, message)

    responses = [welcome or reply("220", "ready")]
    if protocol is SourceProtocol.FTPS:
        responses.append(reply("234", "tls ready"))
    responses.extend((reply("331", "password required"), reply("230", "logged in")))
    if protocol is SourceProtocol.FTPS:
        responses.extend((reply("200", "pbsz set"), reply("200", "protected")))
    responses.extend(
        (
            reply("250", "changed"),
            reply("200", "binary type"),
            reply("150", "opening data"),
            completion or reply("226", "complete"),
            quit_reply or reply("221", "bye"),
        )
    )
    return tuple(responses)


def _install_scripted_download_client(
    monkeypatch: pytest.MonkeyPatch,
    protocol: SourceProtocol,
    *response_sets: tuple[str, ...],
) -> type[ScriptedControlFtp] | type[ScriptedControlFtpTls]:
    client_type: type[ScriptedControlFtp] | type[ScriptedControlFtpTls]
    if protocol is SourceProtocol.FTPS:
        client_type = ScriptedControlFtpTls
        monkeypatch.setattr(ftplib, "FTP_TLS", client_type)
    else:
        client_type = ScriptedControlFtp
        monkeypatch.setattr(ftplib, "FTP", client_type)
    client_type.response_sets = list(response_sets)
    client_type.latest = None
    return client_type


@pytest.mark.asyncio
async def test_blocking_bridge_does_not_wait_indefinitely_for_a_stubborn_worker() -> None:
    cancellation = BlockingOperationCancellation()
    started = threading.Event()
    release = threading.Event()

    class StubbornResource:
        closed = False

        def close(self) -> None:
            self.closed = True

    resource = StubbornResource()

    def operation() -> str:
        assert cancellation.bind(resource)
        started.set()
        release.wait(timeout=5)
        cancellation.release(resource)
        return "released"

    began = anyio.current_time()
    result, timed_out = await run_bounded_blocking(
        operation,
        cancellation.cancel,
        timeout_seconds=0.01,
        cancellation_join_seconds=0.01,
    )
    elapsed = anyio.current_time() - began
    release.set()

    assert started.is_set()
    assert resource.closed
    assert timed_out
    assert result is None
    assert elapsed < 0.2


async def public_resolver(_host: str, _port: int) -> tuple[str, ...]:
    return ("93.184.216.34",)


def feed(protocol: SourceProtocol = SourceProtocol.FTP) -> NcrFeedConfig:
    return NcrFeedConfig(
        portal_id="demo-portal",
        chain_id="7290000000008",
        credential_key="DEMO",
        protocol=protocol,
        remote_directory="/",
        passive=False,
        timeout_seconds=3,
    )


def remote(protocol: SourceProtocol = SourceProtocol.FTP) -> RemoteFile:
    filename = "Price7290000000008-001-202608111200.GZ"
    return RemoteFile(
        retailer_id="demo",
        portal_id="demo-portal",
        protocol=protocol,
        remote_id=f"demo:{filename}",
        download_url=f"{protocol.value}://url.retail.publishedprices.co.il/{filename}",
        original_filename=filename,
        document_type=DocumentType.PRICE_DELTA,
        compression=CompressionFormat.GZIP,
        discovered_at=datetime(2026, 8, 11, tzinfo=UTC),
        content_length=len(FakeFtp.payload),
        media_type="application/gzip",
    )


def downloader(
    monkeypatch: pytest.MonkeyPatch,
    *,
    protocol: SourceProtocol = SourceProtocol.FTP,
    maximum_response_bytes: int = 1024,
    temporary_directory: Path | None = None,
    resolver: Callable[[str, int], Awaitable[tuple[str, ...]]] = public_resolver,
    maximum_download_seconds: float = 600,
    minimum_free_bytes: int = 0,
    allow_insecure_ftp: bool = True,
) -> FtpDownloader:
    monkeypatch.setattr("makolet.adapters.download.ftp.ftplib.FTP", FakeFtp)
    monkeypatch.setattr("makolet.adapters.download.ftp.ftplib.FTP_TLS", FakeFtpTls)
    return FtpDownloader(
        {"demo-portal": feed(protocol)},
        StaticCredentials(),
        AdvancingClock(),
        maximum_response_bytes=maximum_response_bytes,
        chunk_size=3,
        temporary_directory=temporary_directory,
        resolver=resolver,
        maximum_download_seconds=maximum_download_seconds,
        minimum_free_bytes=minimum_free_bytes,
        allow_insecure_ftp=allow_insecure_ftp,
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
async def test_ftp_download_preserves_exact_bytes_and_uses_active_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body, evidence = await consume(downloader(monkeypatch).open(remote()))

    client = FakeFtp.latest
    assert client is not None
    assert body == FakeFtp.payload
    assert evidence.status_code is None
    assert evidence.content_length == len(FakeFtp.payload)
    assert evidence.media_type == "application/gzip"
    assert evidence.response_metadata == (("transport-security", "unauthenticated"),)
    assert client.passive is False
    assert client.directory == "/"
    assert client.connected_address == "93.184.216.34"
    assert client.host == "url.retail.publishedprices.co.il"
    assert client.closed


@pytest.mark.asyncio
async def test_explicit_ftps_protects_the_data_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, evidence = await consume(
        downloader(
            monkeypatch,
            protocol=SourceProtocol.FTPS,
            allow_insecure_ftp=False,
        ).open(remote(SourceProtocol.FTPS))
    )

    client = FakeFtpTls.latest
    assert client is not None
    assert client.protected
    assert isinstance(client.context, ssl.SSLContext)
    assert client.context.check_hostname
    assert client.context.verify_mode == ssl.CERT_REQUIRED
    assert evidence.response_metadata == (("transport-security", "tls"),)


def test_metered_ftps_context_charges_ciphertext_from_wrap_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    budget = FtpControlBudget(maximum_total_bytes=1024)
    context = ssl.create_default_context()
    client = ftplib.FTP_TLS(context=context)  # noqa: S321
    install_metered_ftp_tls_context(client, budget)
    handshake_records = (b"WIRE1", b"WIRE2")

    class FakeRaw(socket.socket):
        def __new__(cls) -> FakeRaw:
            return socket.socket.__new__(cls)

        def send(self, data: Buffer, flags: int = 0) -> int:
            del flags
            return len(bytes(data))

        def recv(self, buflen: int = 1024, flags: int = 0) -> bytes:
            del buflen, flags
            return b""

    raw = FakeRaw()

    class FakeOutgoing:
        def __init__(self) -> None:
            self.chunks = list(handshake_records)

        def read(self, _amount: int = -1) -> bytes:
            return self.chunks.pop(0) if self.chunks else b""

    class FakeIncoming:
        def write(self, data: bytes) -> int:
            return len(data)

        def write_eof(self) -> None:
            return None

    class FakeSslObject:
        def do_handshake(self) -> None:
            if getattr(self, "_armed", False):
                return
            self._armed = True
            raise ssl.SSLWantWriteError

    bios = iter((FakeIncoming(), FakeOutgoing()))
    monkeypatch.setattr(ssl, "MemoryBIO", lambda: next(bios))
    monkeypatch.setattr(context, "wrap_bio", lambda *_args, **_kwargs: FakeSslObject())

    wrapped = context.wrap_socket(raw, server_hostname="example.test")

    assert isinstance(wrapped, ssl.SSLSocket)
    assert budget.wire_bytes == sum(len(record) for record in handshake_records)


def test_bounded_control_reader_enforces_reply_bytes_and_operation_lines() -> None:
    oversized_reply = _control_reply("220", "12345678", "abcdefgh")
    reply_client = ftplib.FTP()  # noqa: S321 - in-memory protocol parser harness
    reply_client.file = StringIO(oversized_reply)
    reply_budget = FtpControlBudget(
        maximum_reply_bytes=20,
        maximum_reply_lines=8,
        maximum_operation_bytes=100,
        maximum_operation_lines=8,
    )
    install_bounded_ftp_control_reader(reply_client, reply_budget)

    with pytest.raises(FtpControlLimitError) as reply_error:
        reply_client.getresp()

    assert reply_error.value.reason == "reply_bytes"
    assert reply_budget.control_bytes == len(oversized_reply.encode())

    operation_replies = tuple(_control_reply("200", value) for value in ("one", "two", "three"))
    operation_client = ftplib.FTP()  # noqa: S321 - in-memory protocol parser harness
    operation_client.file = StringIO("".join(operation_replies))
    operation_budget = FtpControlBudget(
        maximum_reply_bytes=100,
        maximum_reply_lines=2,
        maximum_operation_bytes=100,
        maximum_operation_lines=2,
    )
    install_bounded_ftp_control_reader(operation_client, operation_budget)

    assert operation_client.getresp().startswith("200")
    assert operation_client.getresp().startswith("200")
    with pytest.raises(FtpControlLimitError) as operation_error:
        operation_client.getresp()

    assert operation_error.value.reason == "operation_lines"
    assert operation_budget.control_lines == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", [SourceProtocol.FTP, SourceProtocol.FTPS])
async def test_ftp_download_accepts_bounded_multiline_controls_and_accounts_them_once(
    monkeypatch: pytest.MonkeyPatch,
    protocol: SourceProtocol,
) -> None:
    responses = _download_control_responses(protocol, multiline=True)
    _install_scripted_download_client(monkeypatch, protocol, responses)
    subject = downloader(
        monkeypatch,
        protocol=protocol,
        allow_insecure_ftp=protocol is SourceProtocol.FTP,
    )
    _install_scripted_download_client(monkeypatch, protocol, responses)

    async with subject.open(remote(protocol)) as session:
        body = b"".join([chunk async for chunk in session.iter_raw()])
        evidence = await session.finish(len(body))
        transferred_bytes = session.transferred_bytes

    assert body == FakeFtp.payload
    assert evidence.content_length == len(FakeFtp.payload)
    assert transferred_bytes == len(FakeFtp.payload) + sum(
        len(response.encode()) for response in responses
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", [SourceProtocol.FTP, SourceProtocol.FTPS])
async def test_ftp_download_rejects_multiline_welcome_before_authentication(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    protocol: SourceProtocol,
) -> None:
    welcome = _control_reply("220", "one", "two", "three")
    responses = _download_control_responses(protocol, welcome=welcome)
    subject = downloader(
        monkeypatch,
        protocol=protocol,
        temporary_directory=tmp_path,
        allow_insecure_ftp=protocol is SourceProtocol.FTP,
    )
    client_type = _install_scripted_download_client(monkeypatch, protocol, responses)

    def tight_budget(*, maximum_total_bytes: int | None = None) -> FtpControlBudget:
        return FtpControlBudget(
            maximum_reply_bytes=1024,
            maximum_reply_lines=2,
            maximum_operation_bytes=4096,
            maximum_operation_lines=32,
            maximum_total_bytes=maximum_total_bytes,
        )

    monkeypatch.setattr("makolet.adapters.download.ftp.FtpControlBudget", tight_budget)

    with pytest.raises(SourceResponseError, match="control replies") as caught:
        async with subject.open(remote(protocol)):
            pass

    assert caught.value.transferred_bytes == len(welcome.encode())
    assert client_type.latest is not None
    assert client_type.latest.closed
    assert await anyio.to_thread.run_sync(lambda: list(tmp_path.iterdir())) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["completion", "quit"])
async def test_ftp_download_rejects_late_multiline_control_flood_and_charges_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    stage: str,
) -> None:
    flooded_reply = _control_reply("226" if stage == "completion" else "221", "a", "b", "c")
    responses = _download_control_responses(
        SourceProtocol.FTP,
        completion=flooded_reply if stage == "completion" else None,
        quit_reply=flooded_reply if stage == "quit" else None,
    )
    subject = downloader(monkeypatch, temporary_directory=tmp_path)
    _install_scripted_download_client(monkeypatch, SourceProtocol.FTP, responses)

    def tight_budget(*, maximum_total_bytes: int | None = None) -> FtpControlBudget:
        return FtpControlBudget(
            maximum_reply_bytes=1024,
            maximum_reply_lines=2,
            maximum_operation_bytes=4096,
            maximum_operation_lines=32,
            maximum_total_bytes=maximum_total_bytes,
        )

    monkeypatch.setattr("makolet.adapters.download.ftp.FtpControlBudget", tight_budget)

    with pytest.raises(SourceResponseError, match="control replies") as caught:
        async with subject.open(remote()):
            pass

    replies_before_flood = responses[: (7 if stage == "completion" else 8)]
    assert caught.value.transferred_bytes == len(FakeFtp.payload) + sum(
        len(response.encode()) for response in replies_before_flood
    )
    assert await anyio.to_thread.run_sync(lambda: list(tmp_path.iterdir())) == []


@pytest.mark.asyncio
async def test_plain_ftp_download_is_fail_closed_without_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subject = downloader(monkeypatch, allow_insecure_ftp=False)

    with pytest.raises(SourceBlockedError, match="explicit operator opt-in"):
        async with subject.open(remote()):
            pass


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_ftp_download_rejects_excess_dns_answers_before_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def excessive_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return tuple(f"93.184.216.{index}" for index in range(30, 35))

    subject = downloader(monkeypatch, resolver=excessive_resolver)
    with pytest.raises(UnsafeRemoteError, match="too many addresses"):
        async with subject.open(remote()):
            pass


async def test_ftp_download_rejects_private_dns_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def private_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return ("10.0.0.1",)

    subject = downloader(monkeypatch, resolver=private_resolver)
    with pytest.raises(UnsafeRemoteError, match="non-public"):
        async with subject.open(remote()):
            pass


@pytest.mark.asyncio
async def test_ftp_download_total_deadline_includes_dns_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def slow_resolver(_host: str, _port: int) -> tuple[str, ...]:
        await anyio.sleep(0.05)
        return ("93.184.216.34",)

    subject = downloader(
        monkeypatch,
        resolver=slow_resolver,
        maximum_download_seconds=0.001,
    )

    with pytest.raises(SourceAccessError, match="timed out") as caught:
        async with subject.open(remote()):
            pass

    assert caught.value.transferred_bytes == 0


@pytest.mark.asyncio
async def test_ftp_download_enforces_streamed_byte_limit_and_cleans_partial_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    subject = downloader(
        monkeypatch,
        maximum_response_bytes=4,
        temporary_directory=tmp_path,
    )

    with pytest.raises(DownloadLimitError, match="exceeds 4"):
        async with subject.open(remote()):
            pass
    entries = await anyio.to_thread.run_sync(lambda: list(tmp_path.iterdir()))
    assert entries == []


@pytest.mark.asyncio
async def test_ftp_spool_preserves_configured_free_space_reserve(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    subject = downloader(
        monkeypatch,
        temporary_directory=tmp_path,
        minimum_free_bytes=2**63,
    )

    with pytest.raises(ArchiveCapacityError, match="free-space reserve"):
        async with subject.open(remote()):
            pass

    entries = await anyio.to_thread.run_sync(lambda: list(tmp_path.iterdir()))
    assert entries == [tmp_path / CAPACITY_LOCK_FILENAME]


@pytest.mark.asyncio
async def test_ftp_download_uses_smaller_collection_limit_and_cleans_partial_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    subject = downloader(
        monkeypatch,
        maximum_response_bytes=1_024,
        temporary_directory=tmp_path,
    )

    with pytest.raises(DownloadLimitError, match="exceeds 4 bytes") as caught:
        async with subject.open(remote(), maximum_bytes=4):
            pass

    assert caught.value.transferred_bytes == 6
    assert caught.value.budget_limited is True
    entries = await anyio.to_thread.run_sync(lambda: list(tmp_path.iterdir()))
    assert entries == []


@pytest.mark.asyncio
async def test_ftp_final_frame_overshoot_reports_exact_collection_limit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    subject = downloader(monkeypatch, temporary_directory=tmp_path)
    monkeypatch.setattr("makolet.adapters.download.ftp.ftplib.FTP", OvershootingFtp)

    with pytest.raises(DownloadLimitError, match="exceeds 10 bytes") as caught:
        async with subject.open(remote(), maximum_bytes=10):
            pass

    assert caught.value.transferred_bytes == 11
    assert caught.value.budget_limited is True
    assert await anyio.to_thread.run_sync(lambda: list(tmp_path.iterdir())) == []


@pytest.mark.asyncio
async def test_ftp_default_path_keeps_a_finite_combined_control_and_payload_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_limits: list[int | None] = []
    budget_type = FtpControlBudget

    def capture_budget(*, maximum_total_bytes: int | None = None) -> FtpControlBudget:
        captured_limits.append(maximum_total_bytes)
        return budget_type(maximum_total_bytes=maximum_total_bytes)

    monkeypatch.setattr("makolet.adapters.download.ftp.FtpControlBudget", capture_budget)
    body, _evidence = await consume(
        downloader(monkeypatch, maximum_response_bytes=1_024).open(remote())
    )

    assert body == FakeFtp.payload
    assert captured_limits == [1_024 + MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES]


@pytest.mark.asyncio
async def test_ftp_permanent_payload_limit_is_not_reclassified_as_collection_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    subject = downloader(
        monkeypatch,
        maximum_response_bytes=10,
        temporary_directory=tmp_path,
    )
    monkeypatch.setattr("makolet.adapters.download.ftp.ftplib.FTP", OvershootingFtp)

    with pytest.raises(DownloadLimitError, match="exceeds 10 bytes") as caught:
        async with subject.open(
            remote(),
            maximum_bytes=10 + MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
        ):
            pass

    assert caught.value.transferred_bytes == 11
    assert caught.value.budget_limited is False


@pytest.mark.asyncio
async def test_ftp_permission_failure_preserves_partial_transfer_count(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    subject = downloader(monkeypatch, temporary_directory=tmp_path)
    monkeypatch.setattr("makolet.adapters.download.ftp.ftplib.FTP", PartialPermissionFtp)

    with pytest.raises(SourceBlockedError, match="denied file access") as caught:
        async with subject.open(remote()):
            pass

    assert caught.value.transferred_bytes == len(b"partial")
    assert await anyio.to_thread.run_sync(lambda: list(tmp_path.iterdir())) == []


@pytest.mark.asyncio
async def test_ftp_download_accepts_body_exactly_at_collection_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body, evidence = await consume(
        downloader(monkeypatch, maximum_response_bytes=1_024).open(
            remote(),
            maximum_bytes=len(FakeFtp.payload),
        )
    )

    assert body == FakeFtp.payload
    assert evidence.content_length == len(FakeFtp.payload)


@pytest.mark.asyncio
async def test_ftp_session_reports_full_spooled_transfer_before_sink_consumes_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subject = downloader(monkeypatch)

    async with subject.open(remote()) as session:
        chunks = session.iter_raw()
        first_chunk = await anext(chunks)
        assert session.transferred_bytes == len(FakeFtp.payload)
        _ = [chunk async for chunk in chunks]
    assert len(first_chunk) < len(FakeFtp.payload)


@pytest.mark.asyncio
async def test_ftp_timeout_closes_socket_joins_worker_and_cleans_partial_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    BlockingFtp.active.clear()
    BlockingFtp.released.clear()
    monkeypatch.setattr("makolet.adapters.download.ftp.ftplib.FTP", BlockingFtp)
    subject = FtpDownloader(
        {"demo-portal": feed()},
        StaticCredentials(),
        AdvancingClock(),
        maximum_response_bytes=1_024,
        maximum_download_seconds=0.01,
        temporary_directory=tmp_path,
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )

    with pytest.raises(SourceAccessError, match="timed out") as caught:
        async with subject.open(remote()):
            pass

    assert BlockingFtp.released.is_set()
    assert not BlockingFtp.active.is_set()
    assert BlockingFtp.data_connection is not None
    assert BlockingFtp.data_connection.closed
    assert caught.value.transferred_bytes == len(b"partial")
    entries = await anyio.to_thread.run_sync(lambda: list(tmp_path.iterdir()))
    assert entries == []


@pytest.mark.asyncio
async def test_ftp_caller_cancellation_closes_data_socket_and_cleans_partial_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    BlockingFtp.active.clear()
    BlockingFtp.released.clear()
    monkeypatch.setattr("makolet.adapters.download.ftp.ftplib.FTP", BlockingFtp)
    subject = FtpDownloader(
        {"demo-portal": feed()},
        StaticCredentials(),
        AdvancingClock(),
        maximum_response_bytes=1_024,
        maximum_download_seconds=5,
        temporary_directory=tmp_path,
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )

    async def download() -> None:
        async with subject.open(remote()):
            pass

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(download)
        await anyio.to_thread.run_sync(BlockingFtp.active.wait)
        tasks.cancel_scope.cancel()

    assert BlockingFtp.released.is_set()
    assert BlockingFtp.data_connection is not None
    assert BlockingFtp.data_connection.closed
    assert await anyio.to_thread.run_sync(lambda: list(tmp_path.iterdir())) == []


@pytest.mark.asyncio
async def test_ftp_session_requires_complete_single_consumption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with downloader(monkeypatch).open(remote()) as session:
        with pytest.raises(SourceResponseError, match="not consumed"):
            await session.finish(0)
        body = b"".join([chunk async for chunk in session.iter_raw()])
        with pytest.raises(SourceResponseError, match="only be consumed once"):
            _ = [chunk async for chunk in session.iter_raw()]
        with pytest.raises(SourceResponseError, match="length differs"):
            await session.finish(len(body) - 1)


@pytest.mark.asyncio
async def test_ftp_listing_length_mismatch_fails_during_stream_consumption(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = replace(remote(), content_length=len(FakeFtp.payload) + 1)
    archive = LocalContentAddressedArchive(tmp_path / "raw")

    async with downloader(monkeypatch).open(source) as session:
        with pytest.raises(SourceResponseError, match="listing metadata"):
            await archive.put(session.iter_raw(), original_filename=source.original_filename)

    assert [path for path in (tmp_path / "raw").rglob("*") if path.is_file()] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [
        "ftp://url.retail.publishedprices.co.il/other.GZ",
        "ftp://url.retail.publishedprices.co.il/Price7290000000008-001-202608111200.GZ?token=x",
    ],
)
async def test_ftp_download_rejects_urls_outside_registry_policy(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    source = remote()
    unsafe = RemoteFile(
        retailer_id=source.retailer_id,
        portal_id=source.portal_id,
        protocol=source.protocol,
        remote_id=source.remote_id,
        download_url=value,
        original_filename=source.original_filename,
        document_type=source.document_type,
        compression=source.compression,
        discovered_at=source.discovered_at,
    )

    with pytest.raises(UnsafeRemoteError):
        async with downloader(monkeypatch).open(unsafe):
            pass


def test_remote_file_rejects_ftp_url_credentials_before_download() -> None:
    source = remote()

    with pytest.raises(DomainValidationError, match="credentials"):
        RemoteFile(
            retailer_id=source.retailer_id,
            portal_id=source.portal_id,
            protocol=source.protocol,
            remote_id=source.remote_id,
            download_url=(
                "ftp://user:password@url.retail.publishedprices.co.il/"
                "Price7290000000008-001-202608111200.GZ"
            ),
            original_filename=source.original_filename,
            document_type=source.document_type,
            compression=source.compression,
            discovered_at=source.discovered_at,
        )
