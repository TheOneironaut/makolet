"""NCR Published Prices FTP and explicit-FTPS directory discovery."""

from __future__ import annotations

import ftplib
import ipaddress
import os
import re
import socket
import ssl
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Protocol, cast
from urllib.parse import quote

import anyio

from makolet.adapters.download._blocking import (
    BlockingOperationCancellation,
    run_bounded_blocking,
)
from makolet.adapters.ftp_control import (
    FtpControlBudget,
    FtpControlLimitError,
    install_bounded_ftp_control_reader,
    install_metered_ftp_tls_context,
)
from makolet.adapters.sources.common import (
    build_remote_file,
    cursor_int,
    decode_cursor,
    deduplicate_files,
    encode_cursor,
    validate_limit,
)
from makolet.application.models import DiscoveryCursor, DiscoveryPage, DiscoveryRunBudget
from makolet.application.ports import MAXIMUM_FTP_RESOLVED_ADDRESSES, Clock
from makolet.domain.enums import SourceProtocol
from makolet.domain.errors import (
    DiscoveryBudgetExceededError,
    DomainValidationError,
    SourceAccessError,
    SourceBlockedError,
    SourceResponseError,
    UnsafeRemoteError,
)
from makolet.domain.filenames import safe_basename

type FtpHostResolver = Callable[[str, int], Awaitable[tuple[str, ...]]]
_CREDENTIAL_KEY = re.compile(r"^[A-Z0-9_]{1,64}$")
_DEFAULT_MAXIMUM_LISTING_BYTES = 8 * 1024 * 1024
_DEFAULT_MAXIMUM_LISTING_LINE_BYTES = 64 * 1024
_DEFAULT_MAXIMUM_MLSD_FACTS = 32
_FTP_DATA_CHUNK_BYTES = 64 * 1024
_HARD_MAXIMUM_LISTING_BYTES = 1024 * 1024 * 1024
_HARD_MAXIMUM_LISTING_LINE_BYTES = 1024 * 1024
_HARD_MAXIMUM_LISTING_ENTRIES = 1_000_000
_HARD_MAXIMUM_MLSD_FACTS = 1024


class _PassiveAddressPolicy(Protocol):
    trust_server_pasv_ipv4_address: bool


@dataclass(slots=True)
class _FtpListingLimits:
    """Charge bounded raw data-channel bytes before decoding or filtering."""

    maximum_bytes: int
    maximum_lines: int
    maximum_line_bytes: int
    maximum_fact_tokens: int
    wire_bytes: int = 0
    wire_lines: int = 0

    def consume_wire_bytes(self, byte_count: int) -> None:
        self.wire_bytes += byte_count
        if self.wire_bytes > self.maximum_bytes:
            raise SourceResponseError("FTP directory exceeds its configured byte limit")

    def accept_wire_line(self, byte_count: int) -> None:
        if byte_count > self.maximum_line_bytes:
            raise SourceResponseError("FTP directory contains an oversized line")
        self.wire_lines += 1
        if self.wire_lines > self.maximum_lines:
            raise SourceResponseError("FTP directory exceeds its configured entry limit")

    def check_pending_line(self, byte_count: int) -> None:
        if byte_count > self.maximum_line_bytes:
            raise SourceResponseError("FTP directory contains an oversized line")

    def check_fact_tokens(self, token_count: int) -> None:
        if token_count > self.maximum_fact_tokens:
            raise SourceResponseError("FTP MLSD line contains too many facts")


@dataclass(frozen=True, slots=True, repr=False)
class FtpCredentials:
    username: str
    password: str

    def __repr__(self) -> str:
        return "FtpCredentials(username=<redacted>, password=<redacted>)"


class CredentialProvider(Protocol):
    def get(self, key: str) -> FtpCredentials: ...


class EnvironmentCredentialProvider:
    """Read chain-scoped public credentials without persisting or rendering them."""

    def __init__(self, prefix: str = "MAKOLET_SOURCE_") -> None:
        self._prefix = prefix

    def get(self, key: str) -> FtpCredentials:
        normalized = key.upper().replace("-", "_")
        if not _CREDENTIAL_KEY.fullmatch(normalized):
            raise DomainValidationError("FTP credential key is invalid")
        username = os.environ.get(f"{self._prefix}{normalized}_USERNAME")
        password = os.environ.get(f"{self._prefix}{normalized}_PASSWORD")
        if username is None or password is None:
            raise SourceBlockedError("Configured public FTP credentials are unavailable")
        if not username or "\x00" in username or "\x00" in password:
            raise SourceBlockedError("Configured public FTP credentials are invalid")
        return FtpCredentials(username, password)


@dataclass(frozen=True, slots=True)
class FtpCatalogEntry:
    filename: str
    content_length: int | None = None
    modified_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class NcrFeedConfig:
    portal_id: str
    chain_id: str
    credential_key: str
    protocol: SourceProtocol = SourceProtocol.FTP
    host: str = "url.retail.publishedprices.co.il"
    remote_directory: str = "/"
    passive: bool = False
    timeout_seconds: float = 20.0
    maximum_entries: int = 10_000
    maximum_listing_bytes: int = _DEFAULT_MAXIMUM_LISTING_BYTES
    maximum_listing_line_bytes: int = _DEFAULT_MAXIMUM_LISTING_LINE_BYTES
    maximum_mlsd_facts: int = _DEFAULT_MAXIMUM_MLSD_FACTS

    def __post_init__(self) -> None:
        if self.protocol not in {SourceProtocol.FTP, SourceProtocol.FTPS}:
            raise ValueError("NCR feed must use FTP or explicit FTPS")
        if (
            self.timeout_seconds <= 0
            or not 1 <= self.maximum_entries <= _HARD_MAXIMUM_LISTING_ENTRIES
            or not 1 <= self.maximum_listing_bytes <= _HARD_MAXIMUM_LISTING_BYTES
            or not 1 <= self.maximum_listing_line_bytes <= _HARD_MAXIMUM_LISTING_LINE_BYTES
            or not 1 <= self.maximum_mlsd_facts <= _HARD_MAXIMUM_MLSD_FACTS
        ):
            raise ValueError("NCR feed limits must be positive and within hard ceilings")
        path = PurePosixPath(self.remote_directory)
        if ".." in path.parts:
            raise ValueError("NCR remote directory cannot contain parent traversal")


@dataclass(frozen=True, slots=True)
class NcrSourceConfig:
    retailer_id: str
    feeds: tuple[NcrFeedConfig, ...]


class FtpCatalogClient(Protocol):
    async def list(
        self,
        feed: NcrFeedConfig,
        *,
        budget: DiscoveryRunBudget | None = None,
    ) -> tuple[FtpCatalogEntry, ...]: ...


class StdlibFtpCatalogClient:
    def __init__(
        self,
        credentials: CredentialProvider,
        *,
        resolver: FtpHostResolver | None = None,
        allow_insecure_ftp: bool = False,
    ) -> None:
        self._credentials = credentials
        self._resolver = resolver or _resolve_public_addresses
        self._allow_insecure_ftp = allow_insecure_ftp

    async def list(
        self,
        feed: NcrFeedConfig,
        *,
        budget: DiscoveryRunBudget | None = None,
    ) -> tuple[FtpCatalogEntry, ...]:
        active_budget = budget or DiscoveryRunBudget()
        if feed.protocol is SourceProtocol.FTP and not self._allow_insecure_ftp:
            raise SourceBlockedError("Plain FTP transport requires explicit operator opt-in")
        deadline = anyio.current_time() + min(
            feed.timeout_seconds,
            active_budget.remaining_elapsed_seconds,
        )
        try:
            with anyio.fail_after(_remaining_seconds(deadline)):
                addresses = await _validated_public_addresses(feed, self._resolver)
        except TimeoutError as error:
            if active_budget.elapsed_exhausted:
                raise DiscoveryBudgetExceededError("listing_elapsed_limit") from error
            raise SourceAccessError("Public FTP listing timed out") from error
        active_budget.checkpoint()
        cancellation = BlockingOperationCancellation()
        transfer_budget = FtpControlBudget(maximum_total_bytes=active_budget.remaining_bytes)
        limits = _FtpListingLimits(
            maximum_bytes=feed.maximum_listing_bytes,
            maximum_lines=feed.maximum_entries,
            maximum_line_bytes=feed.maximum_listing_line_bytes,
            maximum_fact_tokens=feed.maximum_mlsd_facts,
        )
        try:
            result, timed_out = await run_bounded_blocking(
                lambda: self._list_sync(
                    feed,
                    addresses,
                    cancellation,
                    limits,
                    transfer_budget,
                    active_budget,
                ),
                cancellation.cancel,
                timeout_seconds=_remaining_seconds(deadline),
            )
        finally:
            active_budget.consume_bytes(transfer_budget.total_bytes)
        if timed_out and active_budget.elapsed_exhausted:
            raise DiscoveryBudgetExceededError("listing_elapsed_limit")
        if timed_out:
            raise SourceAccessError("Public FTP listing timed out")
        if result is None:
            raise SourceAccessError("Public FTP listing returned no result")
        active_budget.checkpoint()
        return result

    def _list_sync(
        self,
        feed: NcrFeedConfig,
        addresses: tuple[str, ...],
        cancellation: BlockingOperationCancellation,
        limits: _FtpListingLimits,
        transfer_budget: FtpControlBudget,
        request_budget: DiscoveryRunBudget,
    ) -> tuple[FtpCatalogEntry, ...]:
        credentials = self._credentials.get(feed.credential_key)
        client: ftplib.FTP | None = None
        try:
            client = _connect_pinned(
                feed,
                addresses,
                cancellation,
                transfer_budget,
                request_budget=request_budget,
            )
            cancellation.checkpoint()
            client.login(credentials.username, credentials.password)
            if isinstance(client, ftplib.FTP_TLS):
                client.prot_p()
            # Never follow an address supplied by PASV; use the vetted control peer.
            cast(_PassiveAddressPolicy, client).trust_server_pasv_ipv4_address = False
            client.set_pasv(feed.passive)
            client.cwd(feed.remote_directory)
            entries = self._machine_listing(client, cancellation, limits, transfer_budget)
            client.quit()
        except FtpControlLimitError as error:
            if error.reason == "total_bytes":
                raise DiscoveryBudgetExceededError("listing_byte_limit") from error
            raise SourceResponseError(
                "FTP control replies exceed their configured safety limits"
            ) from error
        except ftplib.error_perm as error:
            raise SourceBlockedError("Public FTP server denied listing access") from error
        except ftplib.all_errors as error:
            raise SourceAccessError("Public FTP listing failed") from error
        finally:
            if client is not None:
                client.close()
                cancellation.release(client)
        return entries

    def _machine_listing(
        self,
        client: ftplib.FTP,
        cancellation: BlockingOperationCancellation,
        limits: _FtpListingLimits,
        transfer_budget: FtpControlBudget,
    ) -> tuple[FtpCatalogEntry, ...]:
        entries: list[FtpCatalogEntry] = []

        def collect_machine_line(raw_line: bytes) -> None:
            cancellation.checkpoint()
            line = _decode_ftp_line(client, raw_line)
            facts_text, separator, name = line.partition(" ")
            if not separator or not name:
                raise SourceResponseError("FTP MLSD line is malformed")
            facts: dict[str, str] = {}
            token_count = 0
            token_start = 0
            while token_start <= len(facts_text):
                token_end = facts_text.find(";", token_start)
                if token_end < 0:
                    token_end = len(facts_text)
                fact = facts_text[token_start:token_end]
                token_start = token_end + 1
                if not fact:
                    if token_end == len(facts_text):
                        break
                    continue
                token_count += 1
                limits.check_fact_tokens(token_count)
                fact_name, separator, fact_value = fact.partition("=")
                if not separator or not fact_name:
                    raise SourceResponseError("FTP MLSD fact is malformed")
                facts[fact_name.casefold()] = fact_value
                if token_end == len(facts_text):
                    break
            if facts.get("type", "file").casefold() != "file":
                return
            entries.append(
                FtpCatalogEntry(
                    filename=safe_basename(name),
                    content_length=_ftp_size(facts),
                    modified_at=_ftp_modified(facts),
                )
            )

        try:
            client.sendcmd("OPTS MLST type;size;modify;")
            _retrieve_ftp_lines(
                client,
                "MLSD",
                collect_machine_line,
                cancellation=cancellation,
                limits=limits,
                transfer_budget=transfer_budget,
            )
        except ftplib.error_perm:
            if limits.wire_lines:
                raise SourceResponseError("FTP MLSD failed after returning partial data") from None
            entries.clear()

            def collect_name(raw_line: bytes) -> None:
                cancellation.checkpoint()
                entries.append(FtpCatalogEntry(safe_basename(_decode_ftp_line(client, raw_line))))

            _retrieve_ftp_lines(
                client,
                "NLST",
                collect_name,
                cancellation=cancellation,
                limits=limits,
                transfer_budget=transfer_budget,
            )
        return tuple(entries)


class NcrSourceAdapter:
    def __init__(
        self,
        config: NcrSourceConfig,
        ftp: FtpCatalogClient,
        clock: Clock,
    ) -> None:
        if not config.feeds:
            raise ValueError("NCR configuration requires at least one feed")
        self._config = config
        self._ftp = ftp
        self._clock = clock
        self.source_id = config.retailer_id

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: DiscoveryRunBudget | None = None,
    ) -> DiscoveryPage:
        validate_limit(limit)
        active_budget = budget or DiscoveryRunBudget()
        state = decode_cursor(self.source_id, cursor.value if cursor else None)
        feed_index = cursor_int(state, "feed", maximum=len(self._config.feeds) - 1)
        offset = cursor_int(state, "offset")
        feed = self._config.feeds[feed_index]
        entries = await self._ftp.list(feed, budget=active_budget)
        if len(entries) > feed.maximum_entries:
            raise SourceResponseError("FTP directory exceeds its configured entry limit")
        discovered_at = self._clock.now()
        files = deduplicate_files(
            build_remote_file(
                retailer_id=self._config.retailer_id,
                portal_id=feed.portal_id,
                download_url=_download_url(feed, entry.filename),
                original_filename=entry.filename,
                discovered_at=discovered_at,
                allowed_hosts=frozenset({feed.host}),
                allowed_schemes=frozenset({feed.protocol.value}),
                content_length=entry.content_length,
                last_modified=entry.modified_at,
            )
            for entry in entries
        )
        if offset > len(files):
            raise SourceResponseError("NCR discovery cursor is beyond its directory listing")
        selected = files[offset : offset + limit]
        next_offset = offset + len(selected)
        if next_offset < len(files):
            next_state: dict[str, int] | None = {"feed": feed_index, "offset": next_offset}
        elif feed_index + 1 < len(self._config.feeds):
            next_state = {"feed": feed_index + 1, "offset": 0}
        else:
            next_state = None
        next_cursor = (
            DiscoveryCursor(encode_cursor(self.source_id, next_state))
            if next_state is not None
            else None
        )
        return DiscoveryPage(selected, next_cursor, next_cursor is None)


async def _resolve_public_addresses(host: str, port: int) -> tuple[str, ...]:
    try:
        records = await anyio.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as error:
        raise SourceAccessError("Public FTP hostname could not be resolved") from error
    return tuple(dict.fromkeys(str(record[4][0]) for record in records))


async def _validated_public_addresses(
    feed: NcrFeedConfig,
    resolver: FtpHostResolver,
) -> tuple[str, ...]:
    addresses = await resolver(feed.host, 21)
    if not addresses:
        raise SourceAccessError("Public FTP hostname resolved to no addresses")
    validated: list[str] = []
    for value in addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError as error:
            raise UnsafeRemoteError("FTP resolver returned an invalid address") from error
        if not address.is_global:
            raise UnsafeRemoteError("Public FTP hostname resolved to a non-public address")
        canonical = str(address)
        if canonical in validated:
            continue
        validated.append(canonical)
        if len(validated) > MAXIMUM_FTP_RESOLVED_ADDRESSES:
            raise UnsafeRemoteError("Public FTP hostname resolved to too many addresses")
    return tuple(validated)


def _retrieve_ftp_lines(
    client: ftplib.FTP,
    command: str,
    callback: Callable[[bytes], None],
    *,
    cancellation: BlockingOperationCancellation,
    limits: _FtpListingLimits,
    transfer_budget: FtpControlBudget,
) -> None:
    """Read one FTP data channel incrementally without ``ftplib`` buffering."""

    client.voidcmd("TYPE A")
    connection = client.transfercmd(command)
    if not cancellation.bind(connection):
        raise TimeoutError("Public FTP listing was cancelled")
    pending = bytearray()
    try:
        while True:
            cancellation.checkpoint()
            data = connection.recv(_FTP_DATA_CHUNK_BYTES)
            cancellation.checkpoint()
            if not data:
                break
            if not isinstance(data, bytes):
                raise SourceResponseError("FTP listing returned non-byte data")
            transfer_budget.consume_data_bytes(len(data))
            limits.consume_wire_bytes(len(data))
            pending.extend(data)
            line_start = 0
            while True:
                newline = pending.find(b"\n", line_start)
                if newline < 0:
                    break
                wire_line = bytes(pending[line_start : newline + 1])
                limits.accept_wire_line(len(wire_line))
                callback(wire_line.removesuffix(b"\n").removesuffix(b"\r"))
                line_start = newline + 1
            if line_start:
                del pending[:line_start]
            limits.check_pending_line(len(pending))
        if pending:
            limits.accept_wire_line(len(pending))
            callback(bytes(pending).removesuffix(b"\r"))
    finally:
        cancellation.release(connection)
        connection.close()
    cancellation.checkpoint()
    client.voidresp()


def _decode_ftp_line(client: ftplib.FTP, raw_line: bytes) -> str:
    try:
        return raw_line.decode(client.encoding, errors="strict")
    except UnicodeError as error:
        raise SourceResponseError("FTP listing contains invalid text encoding") from error


def _remaining_seconds(deadline: float) -> float:
    # A tiny positive duration preserves the normal timeout classification when
    # DNS consumes the final scheduler tick of the total operation deadline.
    return max(deadline - anyio.current_time(), 1e-9)


def _connect_pinned(
    feed: NcrFeedConfig,
    addresses: tuple[str, ...],
    cancellation: BlockingOperationCancellation,
    control_budget: FtpControlBudget,
    request_budget: DiscoveryRunBudget | None = None,
) -> ftplib.FTP:
    last_error: BaseException | None = None
    for address in addresses:
        cancellation.checkpoint()
        client: ftplib.FTP
        if feed.protocol is SourceProtocol.FTPS:
            client = ftplib.FTP_TLS(  # noqa: S321 - required publisher protocol
                timeout=feed.timeout_seconds,
                context=ssl.create_default_context(),
            )
            install_metered_ftp_tls_context(client, control_budget)
        else:
            client = ftplib.FTP(timeout=feed.timeout_seconds)  # noqa: S321
        install_bounded_ftp_control_reader(client, control_budget)
        if request_budget is not None:
            request_budget.begin_request()
        if not cancellation.bind(client):
            raise TimeoutError("Public FTP connection was cancelled")
        try:
            client.connect(address, 21, timeout=feed.timeout_seconds)
        except FtpControlLimitError:
            cancellation.release(client)
            client.close()
            raise
        except ftplib.all_errors as error:
            last_error = error
            cancellation.release(client)
            client.close()
            continue
        client.host = feed.host
        return client
    if last_error is not None:
        raise last_error
    raise SourceAccessError("Public FTP hostname resolved to no usable addresses")


def _download_url(feed: NcrFeedConfig, filename: str) -> str:
    basename = safe_basename(filename)
    directory = feed.remote_directory.strip("/")
    path = "/".join(part for part in (directory, basename) if part)
    return f"{feed.protocol.value}://{feed.host}/{quote(path, safe='/')}"


def _ftp_size(facts: Mapping[str, str]) -> int | None:
    value = facts.get("size")
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError as error:
        raise SourceResponseError("FTP MLSD size fact is invalid") from error
    if parsed < 0:
        raise SourceResponseError("FTP MLSD size fact is negative")
    return parsed


def _ftp_modified(facts: Mapping[str, str]) -> datetime | None:
    value = facts.get("modify")
    if not value:
        return None
    try:
        parsed = datetime.strptime(value.removesuffix(".0"), "%Y%m%d%H%M%S")  # noqa: DTZ007
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC)


__all__ = [
    "CredentialProvider",
    "EnvironmentCredentialProvider",
    "FtpCatalogClient",
    "FtpCatalogEntry",
    "FtpCredentials",
    "NcrFeedConfig",
    "NcrSourceAdapter",
    "NcrSourceConfig",
    "StdlibFtpCatalogClient",
]
