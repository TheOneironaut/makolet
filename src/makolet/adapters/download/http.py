"""Streaming HTTP downloader with explicit SSRF and redirect policy."""

# ruff: noqa: ASYNC109
# The adapter methods below deliberately mirror httpcore's timeout-bearing contract.

from __future__ import annotations

import ipaddress
import socket
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, NoReturn, Protocol, cast

import anyio
import httpx

from makolet.application.models import DownloadEvidence
from makolet.application.ports import (
    HTTP_CONTROL_BYTES_PER_ATTEMPT,
    HTTP_DNS_CONTROL_BYTES_PER_LOOKUP,
    MAXIMUM_HTTP_REDIRECTS,
    MAXIMUM_HTTP_RESOLVED_ADDRESSES,
    MAXIMUM_HTTP_TRANSFER_OVERHEAD_BYTES_PER_OPEN,
    MAXIMUM_TRANSFER_CHUNK_BYTES,
    Clock,
    DownloadSession,
)
from makolet.domain.enums import SourceProtocol
from makolet.domain.errors import (
    DownloadLimitError,
    MakoletError,
    SourceAccessError,
    SourceBlockedError,
    SourceResponseError,
    UnsafeRemoteError,
)
from makolet.domain.models import RemoteFile

type HostResolver = Callable[[str, int], Awaitable[tuple[str, ...]]]
type _SocketOption = (
    tuple[int, int, int] | tuple[int, int, bytes | bytearray] | tuple[int, int, None, int]
)
type _LimitErrorFactory = Callable[[int], MakoletError]

_SAFE_METADATA_HEADERS = frozenset(
    {
        "content-disposition",
        "content-encoding",
        "content-length",
        "content-type",
        "etag",
        "last-modified",
        "x-ms-blob-type",
        "x-ms-creation-time",
        "x-ms-version",
    }
)


class _AsyncNetworkStream(Protocol):
    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes: ...

    async def write(self, buffer: bytes, timeout: float | None = None) -> None: ...

    async def aclose(self) -> None: ...

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> _AsyncNetworkStream: ...

    def get_extra_info(self, info: str) -> object | None: ...


class _AsyncNetworkBackend(Protocol):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[_SocketOption] | None = None,
    ) -> _AsyncNetworkStream: ...

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[_SocketOption] | None = None,
    ) -> _AsyncNetworkStream: ...

    async def sleep(self, seconds: float) -> None: ...


class _AsyncConnectionPool(Protocol):
    _network_backend: _AsyncNetworkBackend

    @property
    def connections(self) -> list[object]: ...


class _AnyioBackedNetworkStream(_AsyncNetworkStream, Protocol):
    _stream: object


class HttpTransferMeter:
    """Account exact raw I/O when available and a conservative attempt floor."""

    def __init__(
        self,
        maximum_bytes: int,
        *,
        limit_error: _LimitErrorFactory,
        on_charge: Callable[[int], None] | None = None,
        on_attempt: Callable[[], None] | None = None,
    ) -> None:
        if maximum_bytes <= 0:
            raise ValueError("HTTP transfer meter limit must be positive")
        self.maximum_bytes = maximum_bytes
        self._limit_error = limit_error
        self._on_charge = on_charge
        self._on_attempt = on_attempt
        self._wire_bytes = 0
        self._control_estimate = 0
        self._payload_bytes = 0
        self._accounted_bytes = 0
        self._current_attempt_control = 0

    @property
    def transferred_bytes(self) -> int:
        return self._accounted_bytes

    @property
    def remaining_bytes(self) -> int:
        return max(0, self.maximum_bytes - self._accounted_bytes)

    def begin_attempt(self) -> None:
        proposed_control = self._control_estimate + HTTP_CONTROL_BYTES_PER_ATTEMPT
        proposed_total = max(self._wire_bytes, proposed_control + self._payload_bytes)
        if self._on_attempt is not None:
            self._on_attempt()
        if proposed_total > self.maximum_bytes:
            raise self._limit_error(self._accounted_bytes)
        self._control_estimate = proposed_control
        self._current_attempt_control = HTTP_CONTROL_BYTES_PER_ATTEMPT
        self._commit()

    def begin_dns_lookup(self) -> None:
        proposed_control = self._control_estimate + HTTP_DNS_CONTROL_BYTES_PER_LOOKUP
        proposed_total = max(self._wire_bytes, proposed_control + self._payload_bytes)
        if proposed_total > self.maximum_bytes:
            raise self._limit_error(self._accounted_bytes)
        self._control_estimate = proposed_control
        self._commit()

    def record_wire(self, byte_count: int) -> None:
        if byte_count < 0:
            raise ValueError("HTTP wire charge cannot be negative")
        self._wire_bytes += byte_count
        self._commit()

    def record_payload(self, byte_count: int) -> None:
        if byte_count < 0:
            raise ValueError("HTTP payload charge cannot be negative")
        self._payload_bytes += byte_count
        self._commit()

    def record_response_control(self, byte_count: int) -> None:
        if byte_count < 0:
            raise ValueError("HTTP response-control charge cannot be negative")
        if byte_count > self._current_attempt_control:
            difference = byte_count - self._current_attempt_control
            self._current_attempt_control = byte_count
            self._control_estimate += difference
            self._commit()

    def _commit(self) -> None:
        accounted_bytes = max(
            self._wire_bytes,
            self._control_estimate + self._payload_bytes,
        )
        if accounted_bytes > self.maximum_bytes:
            self._accounted_bytes = accounted_bytes
            raise self._limit_error(accounted_bytes)
        charge = accounted_bytes - self._accounted_bytes
        if charge and self._on_charge is not None:
            self._on_charge(charge)
        self._accounted_bytes = accounted_bytes


_ACTIVE_HTTP_TRANSFER_METER: ContextVar[HttpTransferMeter | None] = ContextVar(
    "makolet_http_transfer_meter",
    default=None,
)


@contextmanager
def activate_http_transfer_meter(
    meter: HttpTransferMeter,
) -> Iterator[None]:
    previous_context = _ACTIVE_HTTP_TRANSFER_METER.set(meter)
    try:
        yield
    finally:
        _ACTIVE_HTTP_TRANSFER_METER.reset(previous_context)


class _MeteredNetworkStream:
    def __init__(self, delegate: _AsyncNetworkStream, meter: HttpTransferMeter) -> None:
        self._delegate = delegate
        self._meter = meter

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        value = await self._delegate.read(max_bytes, timeout)
        self._meter.record_wire(len(value))
        return value

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        await self._delegate.write(buffer, timeout)
        self._meter.record_wire(len(buffer))

    async def aclose(self) -> None:
        await self._delegate.aclose()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> _AsyncNetworkStream:
        delegate = cast(_AnyioBackedNetworkStream, self._delegate)
        try:
            raw_stream = delegate._stream
        except AttributeError as error:
            await self._raise_unsupported_tls(cause=error)
        if not isinstance(raw_stream, anyio.abc.ByteStream):
            await self._raise_unsupported_tls()
        if not isinstance(raw_stream, _MeteredTlsByteStream):
            delegate._stream = _MeteredTlsByteStream(raw_stream, self._meter)
        return await delegate.start_tls(ssl_context, server_hostname, timeout)

    async def _raise_unsupported_tls(
        self,
        *,
        cause: BaseException | None = None,
    ) -> NoReturn:
        with anyio.CancelScope(shield=True):
            with suppress(Exception):
                await self._delegate.aclose()
        error = SourceAccessError(
            "HTTP TLS transport cannot enforce raw-byte accounting",
            transferred_bytes=self._meter.transferred_bytes,
        )
        if cause is not None:
            raise error from cause
        raise error

    def get_extra_info(self, info: str) -> object | None:
        return self._delegate.get_extra_info(info)


class _MeteredTlsByteStream(anyio.abc.ByteStream):
    """Charge ciphertext below AnyIO's TLS stream, including its handshake."""

    def __init__(self, delegate: anyio.abc.ByteStream, meter: HttpTransferMeter) -> None:
        self._delegate = delegate
        self._meter = meter

    async def receive(self, max_bytes: int = 65_536) -> bytes:
        value = await self._delegate.receive(max_bytes)
        self._meter.record_wire(len(value))
        return value

    async def send(self, item: bytes) -> None:
        await self._delegate.send(item)
        self._meter.record_wire(len(item))

    async def send_eof(self) -> None:
        await self._delegate.send_eof()

    async def aclose(self) -> None:
        await self._delegate.aclose()

    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:
        return self._delegate.extra_attributes


class _MeteredNetworkBackend:
    def __init__(self, delegate: _AsyncNetworkBackend) -> None:
        self._delegate = delegate

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[_SocketOption] | None = None,
    ) -> _AsyncNetworkStream:
        stream = await self._delegate.connect_tcp(
            host,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )
        meter = _ACTIVE_HTTP_TRANSFER_METER.get()
        return _MeteredNetworkStream(stream, meter) if meter is not None else stream

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[_SocketOption] | None = None,
    ) -> _AsyncNetworkStream:
        stream = await self._delegate.connect_unix_socket(
            path,
            timeout=timeout,
            socket_options=socket_options,
        )
        meter = _ACTIVE_HTTP_TRANSFER_METER.get()
        return _MeteredNetworkStream(stream, meter) if meter is not None else stream

    async def sleep(self, seconds: float) -> None:
        await self._delegate.sleep(seconds)


def install_http_wire_accounting(client: httpx.AsyncClient) -> None:
    """Install raw-I/O accounting on the pinned production HTTP transport once."""

    transport = getattr(client, "_transport", None)
    if not isinstance(transport, httpx.AsyncHTTPTransport):
        # Mock/custom transports retain the conservative per-attempt and payload
        # accounting; production composition uses AsyncHTTPTransport below.
        return
    pool = cast(_AsyncConnectionPool, transport._pool)
    backend = pool._network_backend
    if isinstance(backend, _MeteredNetworkBackend):
        return
    if pool.connections:
        raise RuntimeError("HTTP wire accounting must be installed before the first request")
    pool._network_backend = _MeteredNetworkBackend(backend)


@dataclass(frozen=True, slots=True)
class RemoteAccessPolicy:
    """Per-portal URL allowlist; no discovered hostname is trusted implicitly."""

    allowed_hosts: frozenset[str]
    redirect_hosts: frozenset[str] = frozenset()
    allowed_schemes: frozenset[str] = frozenset({"https"})
    allowed_ports: frozenset[int] = frozenset({443})
    maximum_redirects: int = 3
    maximum_response_bytes: int = 2 * 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        if not self.allowed_hosts:
            raise ValueError("At least one source host must be allowlisted")
        if not self.allowed_schemes or not self.allowed_schemes <= {"http", "https"}:
            raise ValueError("Remote access schemes must be HTTP or HTTPS")
        if not self.allowed_ports or any(not 1 <= port <= 65_535 for port in self.allowed_ports):
            raise ValueError("Remote access ports are invalid")
        if not 0 <= self.maximum_redirects <= MAXIMUM_HTTP_REDIRECTS:
            raise ValueError(
                f"Remote access redirects must be between 0 and {MAXIMUM_HTTP_REDIRECTS}"
            )
        if self.maximum_response_bytes <= 0:
            raise ValueError("Remote access limits are invalid")


@dataclass(frozen=True, slots=True)
class ResolvedHttpTarget:
    """A logical HTTP URL paired with its already-vetted connection addresses."""

    logical_url: httpx.URL
    hostname: str
    port: int
    addresses: tuple[str, ...]


class HttpDownloader:
    def __init__(
        self,
        client: httpx.AsyncClient,
        clock: Clock,
        policies: dict[str, RemoteAccessPolicy],
        *,
        resolver: HostResolver | None = None,
        maximum_download_seconds: float = 10 * 60,
    ) -> None:
        if maximum_download_seconds <= 0:
            raise ValueError("HTTP download timeout must be positive")
        install_http_wire_accounting(client)
        self._client = client
        self._clock = clock
        self._policies = policies.copy()
        self._resolver = resolver or resolve_public_addresses
        self._maximum_download_seconds = maximum_download_seconds

    def open(
        self,
        remote_file: RemoteFile,
        *,
        maximum_bytes: int | None = None,
    ) -> AbstractAsyncContextManager[DownloadSession]:
        return self._open(remote_file, maximum_bytes=maximum_bytes)

    @asynccontextmanager
    async def _open(
        self,
        remote_file: RemoteFile,
        *,
        maximum_bytes: int | None,
    ) -> AsyncIterator[DownloadSession]:
        if remote_file.protocol not in {SourceProtocol.HTTP, SourceProtocol.HTTPS}:
            raise UnsafeRemoteError("HTTP downloader received a non-HTTP source")
        try:
            policy = self._policies[remote_file.portal_id]
        except KeyError as error:
            raise UnsafeRemoteError("No remote access policy exists for this portal") from error
        maximum_transfer_bytes, budget_limited = _effective_response_limit(
            policy.maximum_response_bytes + MAXIMUM_HTTP_TRANSFER_OVERHEAD_BYTES_PER_OPEN,
            maximum_bytes,
        )
        meter = HttpTransferMeter(
            maximum_transfer_bytes,
            limit_error=lambda transferred_bytes: DownloadLimitError(
                f"HTTP transfer exceeds {maximum_transfer_bytes} charged bytes",
                transferred_bytes=transferred_bytes,
                budget_limited=budget_limited,
            ),
        )

        started_at = self._clock.now()
        session: _HttpDownloadSession | None = None
        with anyio.move_on_after(self._maximum_download_seconds) as timeout_scope:
            response = await self._open_validated_response(
                remote_file.download_url,
                policy,
                meter=meter,
                budget_limited=budget_limited,
            )
            session = _HttpDownloadSession(
                response,
                min(policy.maximum_response_bytes, meter.remaining_bytes),
                budget_limited=(
                    budget_limited and meter.remaining_bytes < policy.maximum_response_bytes
                ),
                meter=meter,
                clock=self._clock,
                started_at=started_at,
            )
            try:
                yield session
            finally:
                with anyio.CancelScope(shield=True):
                    await response.aclose()
        if timeout_scope.cancel_called:
            raise SourceAccessError(
                "Public source download exceeded its total deadline",
                transferred_bytes=(
                    session.transferred_bytes if session is not None else meter.transferred_bytes
                ),
            )

    async def _open_validated_response(
        self,
        raw_url: str,
        policy: RemoteAccessPolicy,
        *,
        meter: HttpTransferMeter,
        budget_limited: bool,
    ) -> httpx.Response:
        url = httpx.URL(raw_url)
        allowed_hosts = policy.allowed_hosts
        try:
            for redirect_number in range(policy.maximum_redirects + 1):
                meter.begin_dns_lookup()
                target = await resolve_http_target(url, policy, allowed_hosts, self._resolver)
                try:
                    with activate_http_transfer_meter(meter):
                        response = await send_pinned_request(
                            self._client,
                            target,
                            headers={"Accept-Encoding": "identity"},
                        )
                except httpx.TransportError as error:
                    raise SourceAccessError(
                        "Public source request failed",
                        transferred_bytes=meter.transferred_bytes,
                    ) from error
                keep_response = False
                try:
                    if response.is_redirect:
                        if redirect_number == policy.maximum_redirects:
                            raise UnsafeRemoteError("Public source exceeded its redirect limit")
                        location = response.headers.get("location")
                        if not location:
                            raise SourceResponseError("Redirect response omitted Location")
                        url = target.logical_url.join(location)
                        allowed_hosts = policy.redirect_hosts or policy.allowed_hosts
                        continue

                    _raise_for_status(response.status_code)
                    maximum_response_bytes = min(
                        policy.maximum_response_bytes,
                        meter.remaining_bytes,
                    )
                    _validate_declared_length(
                        response.headers.get("content-length"),
                        maximum_response_bytes,
                        budget_limited=(
                            budget_limited
                            and maximum_response_bytes < policy.maximum_response_bytes
                        ),
                        transferred_bytes=meter.transferred_bytes,
                    )
                    keep_response = True
                    return response
                finally:
                    if not keep_response:
                        await response.aclose()
        except MakoletError as error:
            error.transferred_bytes = max(error.transferred_bytes, meter.transferred_bytes)
            raise

        raise AssertionError("redirect loop must return or raise")


class _HttpDownloadSession:
    def __init__(
        self,
        response: httpx.Response,
        maximum_response_bytes: int,
        budget_limited: bool,
        meter: HttpTransferMeter,
        clock: Clock,
        started_at: datetime,
    ) -> None:
        self._response = response
        self._maximum_response_bytes = maximum_response_bytes
        self._budget_limited = budget_limited
        self._meter = meter
        self._clock = clock
        self._started_at = started_at
        self._iterated_bytes = 0
        self._iterated = False

    @property
    def transferred_bytes(self) -> int:
        return self._meter.transferred_bytes

    async def iter_raw(self) -> AsyncIterator[bytes]:
        if self._iterated:
            raise SourceResponseError("A download response can only be consumed once")
        self._iterated = True
        try:
            async for raw_chunk in self._response.aiter_raw():
                for offset in range(0, len(raw_chunk), MAXIMUM_TRANSFER_CHUNK_BYTES):
                    chunk = raw_chunk[offset : offset + MAXIMUM_TRANSFER_CHUNK_BYTES]
                    self._iterated_bytes += len(chunk)
                    try:
                        self._meter.record_payload(len(chunk))
                    except DownloadLimitError as error:
                        if self._iterated_bytes > self._maximum_response_bytes:
                            raise DownloadLimitError(
                                f"Response exceeds {self._maximum_response_bytes} downloaded bytes",
                                transferred_bytes=error.transferred_bytes,
                                budget_limited=self._budget_limited,
                            ) from error
                        raise
                    if self._iterated_bytes > self._maximum_response_bytes:
                        raise DownloadLimitError(
                            f"Response exceeds {self._maximum_response_bytes} downloaded bytes",
                            transferred_bytes=self._meter.transferred_bytes,
                            budget_limited=self._budget_limited,
                        )
                    yield chunk
        except httpx.TransportError as error:
            raise SourceAccessError(
                "Public source response failed while streaming",
                transferred_bytes=self._meter.transferred_bytes,
            ) from error
        declared_length = _parse_content_length(self._response.headers.get("content-length"))
        if declared_length is not None and declared_length != self._iterated_bytes:
            # Validate at iterator EOF so an archive sink sees the failure before
            # it commits the content-addressed object.
            raise SourceResponseError(
                "Downloaded response ended before its declared content length",
                transferred_bytes=self._meter.transferred_bytes,
            )

    async def finish(self, content_length: int) -> DownloadEvidence:
        if not self._iterated:
            raise SourceResponseError("Download response was not consumed")
        if content_length != self._iterated_bytes:
            raise SourceResponseError("Archive length differs from downloaded response length")
        declared_length = _parse_content_length(self._response.headers.get("content-length"))
        if declared_length is not None and declared_length != content_length:
            raise SourceResponseError(
                "Downloaded response ended before its declared content length"
            )
        return DownloadEvidence(
            started_at=self._started_at,
            finished_at=self._clock.now(),
            status_code=self._response.status_code,
            content_length=content_length,
            media_type=_media_type(self._response.headers.get("content-type")),
            etag=_clean_header(self._response.headers.get("etag")),
            last_modified=_parse_http_date(self._response.headers.get("last-modified")),
            response_metadata=tuple(
                (name.lower(), cleaned)
                for name, value in self._response.headers.multi_items()
                if name.lower() in _SAFE_METADATA_HEADERS and (cleaned := _clean_header(value))
            ),
        )


async def resolve_public_addresses(host: str, port: int) -> tuple[str, ...]:
    try:
        records = await anyio.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as error:
        raise SourceAccessError("Public source hostname could not be resolved") from error
    return tuple(dict.fromkeys(str(record[4][0]) for record in records))


async def resolve_http_target(
    url: httpx.URL,
    policy: RemoteAccessPolicy,
    allowed_hosts: frozenset[str],
    resolver: HostResolver,
) -> ResolvedHttpTarget:
    scheme = url.scheme.casefold()
    host = (url.host or "").casefold().rstrip(".")
    default_port = _default_port_for_scheme(scheme)
    port = default_port if url.port is None else url.port
    if scheme not in policy.allowed_schemes or host not in {
        item.casefold().rstrip(".") for item in allowed_hosts
    }:
        raise UnsafeRemoteError("Discovered URL is outside the configured source allowlist")
    if url.userinfo or port != default_port or port not in policy.allowed_ports:
        raise UnsafeRemoteError("Discovered URL contains credentials or a disallowed port")
    addresses = await resolver(host, port)
    if not addresses:
        raise SourceAccessError("Public source hostname resolved to no addresses")
    validated_addresses: list[str] = []
    for address in addresses:
        try:
            parsed = ipaddress.ip_address(address)
        except ValueError as error:
            raise UnsafeRemoteError("Hostname resolver returned an invalid address") from error
        if not parsed.is_global:
            raise UnsafeRemoteError("Public source hostname resolved to a non-public address")
        canonical_address = str(parsed)
        if canonical_address in validated_addresses:
            continue
        validated_addresses.append(canonical_address)
        if len(validated_addresses) > MAXIMUM_HTTP_RESOLVED_ADDRESSES:
            raise UnsafeRemoteError("Public source hostname resolved to too many addresses")
    return ResolvedHttpTarget(url, host, port, tuple(validated_addresses))


def _default_port_for_scheme(scheme: str) -> int:
    if scheme == "https":
        return 443
    if scheme == "http":
        return 80
    # The allowlist check reports unsupported schemes before this value is used.
    return -1


async def send_pinned_request(
    client: httpx.AsyncClient,
    target: ResolvedHttpTarget,
    *,
    headers: Mapping[str, str],
) -> httpx.Response:
    """Connect to a vetted IP literal while preserving Host and HTTPS SNI."""

    meter = _ACTIVE_HTTP_TRANSFER_METER.get()
    if meter is None:
        raise RuntimeError("Pinned HTTP requests require an active transfer meter")
    last_error: httpx.TransportError | None = None
    request_headers = {
        **headers,
        "Connection": "close",
        "Host": _logical_authority(target),
    }
    for address in target.addresses:
        meter.begin_attempt()
        request = client.build_request(
            "GET",
            target.logical_url.copy_with(host=address),
            headers=request_headers,
        )
        if target.logical_url.scheme.casefold() == "https":
            request.extensions["sni_hostname"] = target.hostname
        try:
            response = await client.send(request, stream=True, follow_redirects=False)
        except httpx.TransportError as error:
            last_error = error
            continue
        try:
            meter.record_response_control(_response_control_bytes(response))
        except BaseException:
            await response.aclose()
            raise
        return response
    if last_error is not None:
        raise last_error
    raise SourceAccessError("Public source hostname resolved to no usable addresses")


def _logical_authority(target: ResolvedHttpTarget) -> str:
    hostname = f"[{target.hostname}]" if ":" in target.hostname else target.hostname
    default_port = 443 if target.logical_url.scheme.casefold() == "https" else 80
    return hostname if target.port == default_port else f"{hostname}:{target.port}"


def _response_control_bytes(response: httpx.Response) -> int:
    status_line_bytes = len(response.http_version.encode("ascii", errors="replace")) + 16
    header_bytes = sum(
        len(name.encode("ascii", errors="replace"))
        + len(value.encode("latin-1", errors="replace"))
        + 4
        for name, value in response.headers.multi_items()
    )
    return status_line_bytes + header_bytes + 2


def _raise_for_status(status_code: int) -> None:
    if status_code == 200:
        return
    if status_code in {401, 403}:
        raise SourceBlockedError("Public source denied access")
    if status_code in {408, 425, 429} or status_code >= 500:
        raise SourceAccessError(f"Public source returned transient HTTP status {status_code}")
    raise SourceResponseError(f"Public source returned HTTP status {status_code}")


def _parse_content_length(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError as error:
        raise SourceResponseError("Response Content-Length is invalid") from error
    if parsed < 0:
        raise SourceResponseError("Response Content-Length is negative")
    return parsed


def _effective_response_limit(configured: int, requested: int | None) -> tuple[int, bool]:
    if requested is not None and requested <= 0:
        raise ValueError("maximum_bytes must be positive when supplied")
    if requested is None or requested >= configured:
        return configured, False
    return requested, True


def _validate_declared_length(
    value: str | None,
    maximum_response_bytes: int,
    *,
    budget_limited: bool,
    transferred_bytes: int = 0,
) -> None:
    parsed = _parse_content_length(value)
    if parsed is not None and parsed > maximum_response_bytes:
        raise DownloadLimitError(
            f"Response declares more than {maximum_response_bytes} downloaded bytes",
            transferred_bytes=transferred_bytes,
            budget_limited=budget_limited,
        )


def _clean_header(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.replace("\x00", "").split())[:1024]
    return cleaned or None


def _media_type(value: str | None) -> str | None:
    cleaned = _clean_header(value)
    return cleaned.split(";", 1)[0].casefold() if cleaned else None


def _parse_http_date(value: str | None) -> datetime | None:
    cleaned = _clean_header(value)
    if cleaned is None:
        return None
    try:
        parsed = parsedate_to_datetime(cleaned)
    except TypeError, ValueError, OverflowError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
