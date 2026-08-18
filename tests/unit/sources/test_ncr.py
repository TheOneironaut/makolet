from __future__ import annotations

import ftplib
import socket
import ssl
import threading
from datetime import UTC, datetime
from io import StringIO
from typing import ClassVar, cast

import anyio
import pytest

from makolet.adapters.ftp_control import FtpControlBudget
from makolet.adapters.sources.ncr import (
    EnvironmentCredentialProvider,
    FtpCatalogEntry,
    FtpCredentials,
    NcrFeedConfig,
    NcrSourceAdapter,
    NcrSourceConfig,
    StdlibFtpCatalogClient,
)
from makolet.application.models import DiscoveryRunBudget
from makolet.domain.enums import CompressionFormat, DocumentType, SourceProtocol
from makolet.domain.errors import (
    DiscoveryBudgetExceededError,
    SourceAccessError,
    SourceBlockedError,
    SourceResponseError,
    UnsafeRemoteError,
)
from tests.unit.sources.support import FixedClock, FixtureFtpClient


class StaticCredentials:
    def get(self, _key: str) -> FtpCredentials:
        return FtpCredentials("fixture-user", "fixture-password")


async def public_resolver(_host: str, _port: int) -> tuple[str, ...]:
    return ("93.184.216.34",)


class FakeFtp:
    records: tuple[tuple[str, dict[str, str]], ...] = (
        (
            "Price7290000000008-001-202608111200.GZ",
            {"type": "file", "size": "9", "modify": "20260811120000"},
        ),
        ("folder", {"type": "dir"}),
    )
    fallback_names: tuple[str, ...] = ()
    fail_mlsd = False
    fail_login = False
    latest: FakeFtp | None = None

    def __init__(self, *, timeout: float, context: ssl.SSLContext | None = None) -> None:
        self.timeout = timeout
        self.context = context
        self.passive: bool | None = None
        self.directory: str | None = None
        self.connected_address: str | None = None
        self.host: str | None = None
        self.protected = False
        self.encoding = "utf-8"
        type(self).latest = self

    def connect(self, host: str, port: int, *, timeout: float) -> str:
        assert host == "93.184.216.34"
        assert port == 21
        assert timeout == self.timeout
        self.connected_address = host
        self.host = host
        return "220 ready"

    def login(self, user: str, password: str) -> str:
        assert user == "fixture-user"
        assert password == "fixture-password"
        if self.fail_login:
            raise ftplib.error_perm("530 denied")  # noqa: S321
        return "230 logged in"

    def set_pasv(self, passive: bool) -> None:
        self.passive = passive

    def cwd(self, directory: str) -> str:
        self.directory = directory
        return "250 changed"

    def sendcmd(self, command: str) -> str:
        assert command == "OPTS MLST type;size;modify;"
        if self.fail_mlsd:
            raise ftplib.error_perm("500 MLSD unsupported")  # noqa: S321
        return "200 options accepted"

    def voidcmd(self, command: str) -> str:
        assert command == "TYPE A"
        return "200 type set"

    def transfercmd(self, command: str) -> FakeDataConnection:
        if command == "MLSD":
            payload = b"".join(_mlsd_line(name, facts) for name, facts in self.records)
        else:
            assert command == "NLST"
            payload = b"".join(name.encode() + b"\r\n" for name in self.fallback_names)
        return FakeDataConnection(payload)

    def voidresp(self) -> str:
        return "226 complete"

    def quit(self) -> str:
        return "221 bye"

    def close(self) -> None:
        pass


class FakeFtpTls(FakeFtp):
    def prot_p(self) -> str:
        self.protected = True
        return "200 protected"


class BlockingCatalogFtp(FakeFtp):
    active = threading.Event()
    released = threading.Event()

    data_connection: BlockingDataConnection | None = None

    def transfercmd(self, command: str) -> BlockingDataConnection:
        assert command == "MLSD"
        connection = BlockingDataConnection(self.active, self.released)
        type(self).data_connection = connection
        return connection

    def close(self) -> None:
        self.released.set()


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
        super().__init__(b"")
        self._active = active
        self._released = released

    def recv(self, amount: int) -> bytes:
        del amount
        self._active.set()
        self._released.wait(timeout=5)
        self._active.clear()
        return b""

    def close(self) -> None:
        super().close()
        self._released.set()


def _mlsd_line(name: str, facts: dict[str, str]) -> bytes:
    fact_text = "".join(f"{key}={value};" for key, value in facts.items())
    return f"{fact_text} {name}\r\n".encode()


class ScriptedCatalogFtp(ftplib.FTP):
    response_sets: ClassVar[list[tuple[str, ...]]] = []
    payload = _mlsd_line(
        "Price7290000000008-001-202608111200.GZ",
        {"type": "file", "size": "9", "modify": "20260811120000"},
    )
    instances: ClassVar[list[ScriptedCatalogFtp]] = []

    def __init__(self, *, timeout: float, **_kwargs: object) -> None:
        super().__init__(timeout=timeout)
        responses = type(self).response_sets.pop(0)
        self.file = StringIO("".join(responses))
        self.closed = False
        type(self).instances.append(self)

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


class ScriptedCatalogFtpTls(ftplib.FTP_TLS):
    response_sets: ClassVar[list[tuple[str, ...]]] = []
    payload = ScriptedCatalogFtp.payload
    instances: ClassVar[list[ScriptedCatalogFtpTls]] = []

    def __init__(self, *, timeout: float, **kwargs: object) -> None:
        context = kwargs.get("context")
        assert isinstance(context, ssl.SSLContext)
        super().__init__(timeout=timeout, context=context)
        responses = type(self).response_sets.pop(0)
        self.file = StringIO("".join(responses))
        self.closed = False
        type(self).instances.append(self)

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


def _catalog_control_responses(
    protocol: SourceProtocol,
    *,
    login: str | None = None,
    completion: str | None = None,
    multiline: bool = False,
) -> tuple[str, ...]:
    def reply(code: str, message: str) -> str:
        if multiline:
            return _control_reply(code, f"first {message}", message)
        return _control_reply(code, message)

    responses = [reply("220", "ready")]
    if protocol is SourceProtocol.FTPS:
        responses.append(reply("234", "tls ready"))
    responses.extend((login or reply("331", "password required"), reply("230", "logged in")))
    if protocol is SourceProtocol.FTPS:
        responses.extend((reply("200", "pbsz set"), reply("200", "protected")))
    responses.extend(
        (
            reply("250", "changed"),
            reply("200", "options accepted"),
            reply("200", "ascii type"),
            reply("150", "opening data"),
            completion or reply("226", "complete"),
            reply("221", "bye"),
        )
    )
    return tuple(responses)


def _install_scripted_catalog_client(
    monkeypatch: pytest.MonkeyPatch,
    protocol: SourceProtocol,
    *response_sets: tuple[str, ...],
) -> type[ScriptedCatalogFtp] | type[ScriptedCatalogFtpTls]:
    client_type: type[ScriptedCatalogFtp] | type[ScriptedCatalogFtpTls]
    if protocol is SourceProtocol.FTPS:
        client_type = ScriptedCatalogFtpTls
        monkeypatch.setattr(ftplib, "FTP_TLS", client_type)
    else:
        client_type = ScriptedCatalogFtp
        monkeypatch.setattr(ftplib, "FTP", client_type)
    client_type.response_sets = list(response_sets)
    client_type.instances = []
    return client_type


async def test_ncr_paginates_feeds_and_never_embeds_credentials_in_urls() -> None:
    ftp_feed = NcrFeedConfig(
        "ncr:demo:ftp",
        "7290000000008",
        "DEMO_FTP",
    )
    ftps_feed = NcrFeedConfig(
        "ncr:demo:ftps",
        "7290000000009",
        "DEMO_FTPS",
        protocol=SourceProtocol.FTPS,
    )
    ftp = FixtureFtpClient(
        {
            ftp_feed.portal_id: (
                FtpCatalogEntry("Stores7290000000008.XML", 100),
                FtpCatalogEntry("OddPublisherObject.GZ", 12),
            ),
            ftps_feed.portal_id: (
                FtpCatalogEntry(
                    "PriceFull7290000000009-001-202608111400.gZ",
                    200,
                    datetime(2026, 8, 11, 11, tzinfo=UTC),
                ),
            ),
        }
    )
    subject = NcrSourceAdapter(
        NcrSourceConfig("demo-ncr", (ftp_feed, ftps_feed)),
        ftp,
        FixedClock(),
    )

    first = await subject.discover(None, limit=10)
    second = await subject.discover(first.next_cursor, limit=10)

    assert len(first.files) == 2
    assert first.next_cursor is not None
    assert next(item for item in first.files if item.document_type is DocumentType.STORES)
    unknown = next(item for item in first.files if item.document_type is DocumentType.UNKNOWN)
    assert unknown.compression is CompressionFormat.GZIP
    assert second.files[0].protocol is SourceProtocol.FTPS
    assert second.files[0].download_url.startswith("ftps://")
    assert "@" not in second.files[0].download_url
    assert second.complete is True


def test_environment_credentials_are_required_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = EnvironmentCredentialProvider()
    monkeypatch.delenv("MAKOLET_SOURCE_DEMO_USERNAME", raising=False)
    monkeypatch.delenv("MAKOLET_SOURCE_DEMO_PASSWORD", raising=False)
    with pytest.raises(SourceBlockedError, match="unavailable"):
        provider.get("DEMO")

    monkeypatch.setenv("MAKOLET_SOURCE_DEMO_USERNAME", "public-user")
    monkeypatch.setenv("MAKOLET_SOURCE_DEMO_PASSWORD", "public-password")
    credentials = provider.get("DEMO")

    assert credentials.username == "public-user"
    assert "public-user" not in repr(credentials)
    assert "public-password" not in repr(credentials)


async def test_stdlib_ftp_client_rejects_excess_dns_answers_before_connect() -> None:
    async def excessive_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return tuple(f"93.184.216.{index}" for index in range(30, 35))

    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=excessive_resolver,
        allow_insecure_ftp=True,
    )

    with pytest.raises(UnsafeRemoteError, match="too many addresses"):
        await subject.list(NcrFeedConfig("ncr:demo", "7290000000008", "DEMO"))


async def test_stdlib_ftp_client_rejects_private_dns_before_authentication() -> None:
    async def private_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return ("192.168.1.2",)

    subject = StdlibFtpCatalogClient(
        EnvironmentCredentialProvider(),
        resolver=private_resolver,
        allow_insecure_ftp=True,
    )
    feed = NcrFeedConfig("ncr:demo", "7290000000008", "DEMO")

    with pytest.raises(UnsafeRemoteError, match="non-public"):
        await subject.list(feed)


async def test_stdlib_ftp_client_uses_bounded_mlsd_and_explicit_tls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def public_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return ("93.184.216.34",)

    monkeypatch.setattr(ftplib, "FTP", FakeFtp)
    monkeypatch.setattr(ftplib, "FTP_TLS", FakeFtpTls)
    subject = StdlibFtpCatalogClient(StaticCredentials(), resolver=public_resolver)
    feed = NcrFeedConfig(
        "ncr:demo",
        "7290000000008",
        "DEMO",
        protocol=SourceProtocol.FTPS,
        passive=False,
    )

    entries = await subject.list(feed)

    assert len(entries) == 1
    assert entries[0].content_length == 9
    assert entries[0].modified_at == datetime(2026, 8, 11, 12, tzinfo=UTC)
    assert FakeFtpTls.latest is not None
    assert FakeFtpTls.latest.protected is True
    assert FakeFtpTls.latest.passive is False
    assert FakeFtpTls.latest.connected_address == "93.184.216.34"
    assert FakeFtpTls.latest.host == "url.retail.publishedprices.co.il"
    assert isinstance(FakeFtpTls.latest.context, ssl.SSLContext)
    assert FakeFtpTls.latest.context.check_hostname
    assert FakeFtpTls.latest.context.verify_mode == ssl.CERT_REQUIRED


@pytest.mark.parametrize("protocol", [SourceProtocol.FTP, SourceProtocol.FTPS])
async def test_stdlib_ftp_client_accepts_and_charges_bounded_multiline_controls(
    monkeypatch: pytest.MonkeyPatch,
    protocol: SourceProtocol,
) -> None:
    responses = _catalog_control_responses(protocol, multiline=True)
    client_type = _install_scripted_catalog_client(monkeypatch, protocol, responses)
    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=public_resolver,
        allow_insecure_ftp=protocol is SourceProtocol.FTP,
    )
    budget = DiscoveryRunBudget(maximum_bytes=4096)
    configured_feed = NcrFeedConfig(
        "ncr:demo",
        "7290000000008",
        "DEMO",
        protocol=protocol,
    )

    entries = await subject.list(configured_feed, budget=budget)

    assert len(entries) == 1
    assert budget.consumed_bytes == len(client_type.payload) + sum(
        len(response.encode()) for response in responses
    )
    assert len(client_type.instances) == 1
    assert client_type.instances[0].closed


@pytest.mark.parametrize("stage", ["login", "completion"])
async def test_stdlib_ftp_client_rejects_and_settles_multiline_control_floods(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    flooded_reply = _control_reply("331" if stage == "login" else "226", "a", "b", "c")
    responses = _catalog_control_responses(
        SourceProtocol.FTP,
        login=flooded_reply if stage == "login" else None,
        completion=flooded_reply if stage == "completion" else None,
    )
    client_type = _install_scripted_catalog_client(
        monkeypatch,
        SourceProtocol.FTP,
        responses,
    )

    def tight_budget(*, maximum_total_bytes: int | None = None) -> FtpControlBudget:
        return FtpControlBudget(
            maximum_reply_bytes=1024,
            maximum_reply_lines=2,
            maximum_operation_bytes=4096,
            maximum_operation_lines=32,
            maximum_total_bytes=maximum_total_bytes,
        )

    monkeypatch.setattr("makolet.adapters.sources.ncr.FtpControlBudget", tight_budget)
    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )
    budget = DiscoveryRunBudget(maximum_bytes=4096)

    with pytest.raises(SourceResponseError, match="control replies"):
        await subject.list(
            NcrFeedConfig("ncr:demo", "7290000000008", "DEMO"),
            budget=budget,
        )

    replies_before_flood = responses[: (2 if stage == "login" else 8)]
    expected_data_bytes = 0 if stage == "login" else len(client_type.payload)
    assert budget.consumed_bytes == expected_data_bytes + sum(
        len(response.encode()) for response in replies_before_flood
    )
    assert len(client_type.instances) == 1
    assert client_type.instances[0].closed


async def test_stdlib_ftp_client_charges_failed_address_before_successful_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malformed_welcome = "publisher-not-ftp\r\n"
    responses = _catalog_control_responses(SourceProtocol.FTP)
    client_type = _install_scripted_catalog_client(
        monkeypatch,
        SourceProtocol.FTP,
        (malformed_welcome,),
        responses,
    )

    async def two_address_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return ("93.184.216.34", "93.184.216.35")

    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=two_address_resolver,
        allow_insecure_ftp=True,
    )
    budget = DiscoveryRunBudget(maximum_bytes=4096)

    entries = await subject.list(
        NcrFeedConfig("ncr:demo", "7290000000008", "DEMO"),
        budget=budget,
    )

    assert len(entries) == 1
    assert budget.consumed_bytes == (
        len(malformed_welcome.encode())
        + sum(len(response.encode()) for response in responses)
        + len(client_type.payload)
    )
    assert len(client_type.instances) == 2
    assert budget.request_count == 2
    assert all(client.closed for client in client_type.instances)


async def test_stdlib_ftp_client_charges_ignored_wire_lines_and_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ftplib, "FTP", FakeFtp)
    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )
    budget = DiscoveryRunBudget(maximum_bytes=1024)

    entries = await subject.list(
        NcrFeedConfig("ncr:demo", "7290000000008", "DEMO"),
        budget=budget,
    )

    assert len(entries) == 1
    assert budget.consumed_bytes == sum(
        len(_mlsd_line(name, facts)) for name, facts in FakeFtp.records
    )


async def test_stdlib_plain_ftp_listing_is_fail_closed_without_explicit_opt_in() -> None:
    subject = StdlibFtpCatalogClient(StaticCredentials(), resolver=public_resolver)

    with pytest.raises(SourceBlockedError, match="explicit operator opt-in"):
        await subject.list(NcrFeedConfig("ncr:demo", "7290000000008", "DEMO"))


async def test_stdlib_ftp_client_falls_back_to_bounded_nlst(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def public_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return ("93.184.216.34",)

    class FallbackFtp(FakeFtp):
        fail_mlsd = True
        fallback_names = ("Stores7290000000008.XML", "Other.GZ")

    monkeypatch.setattr(ftplib, "FTP", FallbackFtp)
    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )

    entries = await subject.list(NcrFeedConfig("ncr:demo", "7290000000008", "DEMO"))

    assert tuple(entry.filename for entry in entries) == (
        "Stores7290000000008.XML",
        "Other.GZ",
    )


async def test_stdlib_ftp_client_never_falls_back_after_ignored_mlsd_wire_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PartialMlsdFtp(FakeFtp):
        records = (("ignored-directory", {"type": "dir"}),)
        fallback_names = ("Stores7290000000008.XML",)
        response_calls = 0

        def voidresp(self) -> str:
            type(self).response_calls += 1
            raise ftplib.error_perm("550 partial listing rejected")  # noqa: S321

    monkeypatch.setattr(ftplib, "FTP", PartialMlsdFtp)
    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )

    with pytest.raises(SourceResponseError, match="partial data"):
        await subject.list(NcrFeedConfig("ncr:demo", "7290000000008", "DEMO"))

    assert PartialMlsdFtp.response_calls == 1


@pytest.mark.parametrize(
    ("limit_name", "expected"),
    [
        ("bytes", "byte limit"),
        ("line", "oversized line"),
        ("facts", "too many facts"),
    ],
)
async def test_stdlib_ftp_client_bounds_raw_mlsd_before_filtering(
    monkeypatch: pytest.MonkeyPatch,
    limit_name: str,
    expected: str,
) -> None:
    class HostileIgnoredFtp(FakeFtp):
        records = (
            (
                "ignored-directory",
                {"type": "dir", "unknown": "x" * 64},
            ),
        )

    monkeypatch.setattr(ftplib, "FTP", HostileIgnoredFtp)
    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )
    configured_feed = NcrFeedConfig(
        "ncr:demo",
        "7290000000008",
        "DEMO",
        maximum_listing_bytes=32 if limit_name == "bytes" else 1024,
        maximum_listing_line_bytes=16 if limit_name == "line" else 1024,
        maximum_mlsd_facts=1 if limit_name == "facts" else 32,
    )

    with pytest.raises(SourceResponseError, match=expected):
        await subject.list(configured_feed)


async def test_stdlib_ftp_client_counts_ignored_raw_lines_as_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DirectoryFloodFtp(FakeFtp):
        records = (
            ("ignored-one", {"type": "dir"}),
            ("ignored-two", {"type": "dir"}),
        )

    monkeypatch.setattr(ftplib, "FTP", DirectoryFloodFtp)
    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )

    with pytest.raises(SourceResponseError, match="entry limit"):
        await subject.list(
            NcrFeedConfig(
                "ncr:demo",
                "7290000000008",
                "DEMO",
                maximum_entries=1,
            )
        )


async def test_stdlib_ftp_client_enforces_cumulative_raw_byte_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ftplib, "FTP", FakeFtp)
    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )
    budget = DiscoveryRunBudget(maximum_bytes=8)

    with pytest.raises(DiscoveryBudgetExceededError) as raised:
        await subject.list(
            NcrFeedConfig("ncr:demo", "7290000000008", "DEMO"),
            budget=budget,
        )

    assert raised.value.reason == "listing_byte_limit"


async def test_stdlib_ftp_total_deadline_includes_dns_resolution() -> None:
    async def slow_resolver(_host: str, _port: int) -> tuple[str, ...]:
        await anyio.sleep(0.05)
        return ("93.184.216.34",)

    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=slow_resolver,
        allow_insecure_ftp=True,
    )

    with pytest.raises(SourceAccessError, match="timed out"):
        await subject.list(
            NcrFeedConfig(
                "ncr:demo",
                "7290000000008",
                "DEMO",
                timeout_seconds=0.001,
            )
        )


async def test_stdlib_ftp_client_classifies_denied_login_without_secret_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def public_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return ("93.184.216.34",)

    class DeniedFtp(FakeFtp):
        fail_login = True

    monkeypatch.setattr(ftplib, "FTP", DeniedFtp)
    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )

    with pytest.raises(SourceBlockedError) as captured:
        await subject.list(NcrFeedConfig("ncr:demo", "7290000000008", "DEMO"))

    assert "fixture-user" not in str(captured.value)
    assert "fixture-password" not in str(captured.value)


async def test_stdlib_ftp_listing_timeout_closes_socket_and_joins_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    BlockingCatalogFtp.active.clear()
    BlockingCatalogFtp.released.clear()
    monkeypatch.setattr(ftplib, "FTP", BlockingCatalogFtp)
    subject = StdlibFtpCatalogClient(
        StaticCredentials(),
        resolver=public_resolver,
        allow_insecure_ftp=True,
    )
    configured_feed = NcrFeedConfig(
        "ncr:demo",
        "7290000000008",
        "DEMO",
        timeout_seconds=0.01,
    )

    with pytest.raises(SourceAccessError, match="timed out"):
        await subject.list(configured_feed)

    assert BlockingCatalogFtp.released.is_set()
    assert not BlockingCatalogFtp.active.is_set()
    assert BlockingCatalogFtp.data_connection is not None
    assert BlockingCatalogFtp.data_connection.closed


def test_ncr_configuration_rejects_directory_traversal() -> None:
    with pytest.raises(ValueError, match="traversal"):
        NcrFeedConfig(
            "ncr:demo",
            "7290000000008",
            "DEMO",
            remote_directory="/../../private",
        )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("maximum_entries", 1_000_001),
        ("maximum_listing_bytes", 1024 * 1024 * 1024 + 1),
        ("maximum_listing_line_bytes", 1024 * 1024 + 1),
        ("maximum_mlsd_facts", 1025),
    ],
)
def test_ncr_configuration_rejects_unbounded_listing_limits(name: str, value: int) -> None:
    arguments = {
        "maximum_entries": 10_000,
        "maximum_listing_bytes": 8 * 1024 * 1024,
        "maximum_listing_line_bytes": 64 * 1024,
        "maximum_mlsd_facts": 32,
    }
    arguments[name] = value

    with pytest.raises(ValueError, match="hard ceilings"):
        NcrFeedConfig(
            "ncr:demo",
            "7290000000008",
            "DEMO",
            maximum_entries=arguments["maximum_entries"],
            maximum_listing_bytes=arguments["maximum_listing_bytes"],
            maximum_listing_line_bytes=arguments["maximum_listing_line_bytes"],
            maximum_mlsd_facts=arguments["maximum_mlsd_facts"],
        )
