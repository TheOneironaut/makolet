# ruff: noqa: ASYNC109
# The scripted transport deliberately mirrors httpcore's timeout-bearing contract.

from __future__ import annotations

import ssl
from collections.abc import AsyncIterator
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any

import anyio
import httpx
import pytest

from makolet import composition
from makolet.adapters.download.http import RemoteAccessPolicy
from makolet.adapters.sources.http import SafeHttpListingClient
from makolet.application.models import DiscoveryRunBudget
from makolet.application.ports import HTTP_CONTROL_BYTES_PER_ATTEMPT
from makolet.config import MakoletSettings
from makolet.domain.errors import (
    DiscoveryBudgetExceededError,
    DownloadLimitError,
    SourceAccessError,
    SourceBlockedError,
    SourceResponseError,
    UnsafeRemoteError,
)

_EXPECTED_DNS_CONTROL_BYTES = 64 * 1024


async def public_resolver(_host: str, _port: int) -> tuple[str, ...]:
    return ("93.184.216.34",)


class BodyStream(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"x" * 101


class TrackingStream(httpx.AsyncByteStream):
    def __init__(self, body: bytes = b"body") -> None:
        self.body = body
        self.iterated = False
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.iterated = True
        yield self.body

    async def aclose(self) -> None:
        self.closed = True


class ScriptedNetworkStream:
    def __init__(self, response: bytes) -> None:
        self._response = response
        self._offset = 0
        self.closed = False
        self.tls_started = False

    async def read(self, _max_bytes: int, timeout: float | None = None) -> bytes:
        del timeout
        if self._offset >= len(self._response):
            return b""
        end = min(len(self._response), self._offset + _max_bytes)
        chunk = self._response[self._offset : end]
        self._offset = end
        return chunk

    async def write(self, _buffer: bytes, timeout: float | None = None) -> None:
        del timeout

    async def aclose(self) -> None:
        self.closed = True

    async def start_tls(
        self,
        _ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> ScriptedNetworkStream:
        del server_hostname, timeout
        self.tls_started = True
        return self

    def get_extra_info(self, _info: str) -> object | None:
        return None


class ScriptedNetworkBackend:
    def __init__(self, response: bytes) -> None:
        self.stream = ScriptedNetworkStream(response)

    async def connect_tcp(
        self,
        _host: str,
        _port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: object | None = None,
    ) -> ScriptedNetworkStream:
        del timeout, local_address, socket_options
        return self.stream

    async def connect_unix_socket(
        self,
        _path: str,
        timeout: float | None = None,
        socket_options: object | None = None,
    ) -> ScriptedNetworkStream:
        del timeout, socket_options
        return self.stream

    async def sleep(self, seconds: float) -> None:
        await anyio.sleep(seconds)


class InvalidRawTlsNetworkStream(ScriptedNetworkStream):
    def __init__(self) -> None:
        super().__init__(b"")
        self._stream = object()


async def test_publisher_cookie_jar_never_retains_or_emits_cookie_state() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        cookie_number = len(seen)
        return httpx.Response(
            200,
            headers={
                "Set-Cookie": (f"publisher-{cookie_number}=value; Domain=listing.example; Path=/")
            },
            content=b"ok",
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        cookies=composition._publisher_cookie_jar(),
    ) as client:
        await client.get("https://one.listing.example/first")
        await client.get("https://two.listing.example/second")
        await client.get("https://one.listing.example/third")

        assert len(client.cookies.jar) == 0

    assert len(seen) == 3
    assert all("cookie" not in request.headers for request in seen)


async def test_production_composition_wires_cookie_rejection_without_removing_pooling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_options: dict[str, object] = {}
    cleanup_events: list[str] = []

    class FakeDatabase:
        engine = object()

        async def dispose(self) -> None:
            cleanup_events.append("database")

    class FakeArchive:
        pass

    class FakeHttpClient:
        async def aclose(self) -> None:
            cleanup_events.append("http")

    def create_database(*_args: object, **_kwargs: object) -> FakeDatabase:
        return FakeDatabase()

    def create_archive(_settings: MakoletSettings) -> FakeArchive:
        return FakeArchive()

    def create_http_client(*_args: object, **kwargs: object) -> FakeHttpClient:
        captured_options.update(kwargs)
        return FakeHttpClient()

    def stop_after_client(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("captured production HTTP client")

    monkeypatch.setattr(
        "makolet.composition.Database.from_url",
        staticmethod(create_database),
    )
    monkeypatch.setattr(composition, "_create_archive", create_archive)
    monkeypatch.setattr("makolet.composition.httpx.AsyncClient", create_http_client)
    monkeypatch.setattr(composition, "SourceRegistry", stop_after_client)
    settings = MakoletSettings(
        _env_file=None,
        environment="test",
        archive_root=tmp_path / "archive",
    )

    with pytest.raises(RuntimeError, match="captured production HTTP client"):
        async with composition.open_runtime(settings):
            pytest.fail("runtime yielded after the deliberate setup stop")

    assert isinstance(captured_options["cookies"], CookieJar)
    limits = captured_options["limits"]
    assert isinstance(limits, httpx.Limits)
    assert limits.max_connections == 8
    assert limits.max_keepalive_connections == 4
    assert captured_options["follow_redirects"] is False
    assert captured_options["trust_env"] is False
    assert cleanup_events == ["http", "database"]


async def test_listing_client_follows_only_allowlisted_redirects() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.headers["host"] == "listing.example":
            return httpx.Response(302, headers={"Location": "https://cdn.example/index.json"})
        return httpx.Response(200, stream=TrackingStream(b'{"files": []}'))

    policy = RemoteAccessPolicy(
        allowed_hosts=frozenset({"listing.example"}),
        redirect_hosts=frozenset({"cdn.example"}),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        response = await subject.get("https://listing.example/start", policy=policy)

    assert response.final_url == "https://cdn.example/index.json"
    assert response.body == b'{"files": []}'


async def test_listing_client_pins_vetted_address_and_preserves_logical_origin() -> None:
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, stream=TrackingStream(b"ok"))

    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        response = await subject.get("https://listing.example/index", policy=policy)

    assert response.final_url == "https://listing.example/index"
    assert seen[0].url.host == "93.184.216.34"
    assert seen[0].headers["host"] == "listing.example"
    assert seen[0].extensions["sni_hostname"] == "listing.example"


async def test_listing_address_failover_consumes_physical_request_and_control_budgets() -> None:
    attempted_hosts: list[str] = []

    async def resolver(_host: str, _port: int) -> tuple[str, ...]:
        return ("93.184.216.34", "93.184.216.35")

    def handler(request: httpx.Request) -> httpx.Response:
        attempted_hosts.append(request.url.host)
        if request.url.host == "93.184.216.34":
            raise httpx.ConnectError("first address unavailable", request=request)
        return httpx.Response(200, stream=TrackingStream(b"ok"))

    budget = DiscoveryRunBudget(
        maximum_requests=2,
        maximum_bytes=(_EXPECTED_DNS_CONTROL_BYTES + 2 * HTTP_CONTROL_BYTES_PER_ATTEMPT + 2),
        maximum_elapsed_seconds=5,
    )
    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        response = await SafeHttpListingClient(client, resolver=resolver).get(
            "https://listing.example/index",
            policy=policy,
            budget=budget,
        )

    assert response.body == b"ok"
    assert attempted_hosts == ["93.184.216.34", "93.184.216.35"]
    assert budget.request_count == 2
    assert budget.consumed_bytes == budget.maximum_bytes


async def test_listing_header_only_redirects_consume_control_bytes() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers["host"] == "listing.example":
            return httpx.Response(302, headers={"Location": "https://cdn.example/final"})
        return httpx.Response(200, stream=TrackingStream(b"ok"))

    budget = DiscoveryRunBudget(
        maximum_requests=2,
        maximum_bytes=(2 * _EXPECTED_DNS_CONTROL_BYTES + 2 * HTTP_CONTROL_BYTES_PER_ATTEMPT + 2),
        maximum_elapsed_seconds=5,
    )
    policy = RemoteAccessPolicy(
        allowed_hosts=frozenset({"listing.example"}),
        redirect_hosts=frozenset({"cdn.example"}),
        maximum_redirects=1,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        response = await SafeHttpListingClient(client, resolver=public_resolver).get(
            "https://listing.example/start",
            policy=policy,
            budget=budget,
        )

    assert response.body == b"ok"
    assert budget.request_count == 2
    assert budget.consumed_bytes == budget.maximum_bytes


async def test_listing_wire_budget_stops_informational_flood_before_header_parse() -> None:
    informational = b"HTTP/1.1 103 Early Hints\r\nX-Pad: " + b"x" * (60 * 1024) + b"\r\n\r\n"
    backend = ScriptedNetworkBackend(
        informational * 4 + b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    transport = httpx.AsyncHTTPTransport()
    pool: Any = transport._pool
    pool._network_backend = backend
    budget = DiscoveryRunBudget(
        maximum_requests=1,
        maximum_bytes=(_EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + 32 * 1024),
        maximum_elapsed_seconds=5,
    )
    policy = RemoteAccessPolicy(
        allowed_hosts=frozenset({"listing.example"}),
        allowed_schemes=frozenset({"http"}),
        allowed_ports=frozenset({80}),
    )

    async with httpx.AsyncClient(transport=transport) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        with pytest.raises(DiscoveryBudgetExceededError) as caught:
            await subject.get("http://listing.example/index", policy=policy, budget=budget)

    assert caught.value.reason == "listing_byte_limit"
    assert backend.stream.closed


@pytest.mark.parametrize("invalid_raw_stream", [False, True])
async def test_listing_tls_backend_without_raw_stream_fails_closed_before_handshake(
    invalid_raw_stream: bool,
) -> None:
    backend = ScriptedNetworkBackend(b"")
    if invalid_raw_stream:
        backend.stream = InvalidRawTlsNetworkStream()
    transport = httpx.AsyncHTTPTransport()
    pool: Any = transport._pool
    pool._network_backend = backend
    budget = DiscoveryRunBudget(
        maximum_requests=1,
        maximum_bytes=_EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT,
        maximum_elapsed_seconds=5,
    )
    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))

    async with httpx.AsyncClient(transport=transport) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        with pytest.raises(SourceAccessError, match="cannot enforce raw-byte accounting"):
            await subject.get("https://listing.example/index", policy=policy, budget=budget)

    assert not backend.stream.tls_started
    assert backend.stream.closed
    assert budget.request_count == 1
    assert budget.consumed_bytes == budget.maximum_bytes


async def test_listing_client_rejects_redirect_to_private_or_unlisted_target() -> None:
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(302, headers={"Location": "http://127.0.0.1/admin"})
    )
    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    async with httpx.AsyncClient(transport=transport) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        with pytest.raises(UnsafeRemoteError, match="allowlist"):
            await subject.get("https://listing.example/start", policy=policy)


@pytest.mark.parametrize("port", [0, 80])
async def test_listing_client_rejects_https_on_non_default_port_before_resolution(
    port: int,
) -> None:
    resolved = False

    async def resolver(_host: str, _port: int) -> tuple[str, ...]:
        nonlocal resolved
        resolved = True
        return ("93.184.216.34",)

    policy = RemoteAccessPolicy(
        allowed_hosts=frozenset({"listing.example"}),
        allowed_ports=frozenset({80, 443}),
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200))
    ) as client:
        subject = SafeHttpListingClient(client, resolver=resolver)
        with pytest.raises(UnsafeRemoteError, match="disallowed port"):
            await subject.get(f"https://listing.example:{port}/index", policy=policy)

    assert not resolved


async def test_listing_client_accepts_explicit_default_https_port() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, stream=TrackingStream(b"ok"))

    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        response = await subject.get("https://listing.example:443/index", policy=policy)

    assert response.body == b"ok"
    assert seen[0].headers["host"] == "listing.example"


async def test_listing_client_rejects_private_dns_resolution() -> None:
    async def private_resolver(_host: str, _port: int) -> tuple[str, ...]:
        return ("10.0.0.8",)

    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    budget = DiscoveryRunBudget(
        maximum_requests=1,
        maximum_bytes=_EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT,
        maximum_elapsed_seconds=5,
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200))
    ) as client:
        subject = SafeHttpListingClient(client, resolver=private_resolver)
        with pytest.raises(UnsafeRemoteError, match="non-public"):
            await subject.get(
                "https://listing.example/start",
                policy=policy,
                budget=budget,
            )

    assert budget.request_count == 0
    assert budget.consumed_bytes == _EXPECTED_DNS_CONTROL_BYTES


async def test_listing_client_bounds_declared_and_received_bytes() -> None:
    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    declared = httpx.MockTransport(
        lambda _: httpx.Response(200, headers={"Content-Length": "101"}, content=b"")
    )
    async with httpx.AsyncClient(transport=declared) as client:
        subject = SafeHttpListingClient(
            client,
            resolver=public_resolver,
            maximum_listing_bytes=100,
        )
        with pytest.raises(DownloadLimitError, match="declares"):
            await subject.get("https://listing.example/", policy=policy)

    received = httpx.MockTransport(
        lambda _: httpx.Response(
            200,
            stream=BodyStream(),
        )
    )
    async with httpx.AsyncClient(transport=received) as client:
        subject = SafeHttpListingClient(
            client,
            resolver=public_resolver,
            maximum_listing_bytes=100,
        )
        with pytest.raises(DownloadLimitError, match="byte limit"):
            await subject.get("https://listing.example/", policy=policy)


async def test_listing_client_has_an_independent_wall_clock_timeout() -> None:
    async def slow(_request: httpx.Request) -> httpx.Response:
        await anyio.sleep(0.05)
        return httpx.Response(200, content=b"ok")

    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
        subject = SafeHttpListingClient(
            client,
            resolver=public_resolver,
            request_timeout_seconds=0.001,
        )
        with pytest.raises(SourceAccessError, match="total deadline"):
            await subject.get("https://listing.example/", policy=policy)


async def test_listing_client_enforces_one_request_budget_across_calls() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, stream=TrackingStream(b"ok"))

    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    budget = DiscoveryRunBudget(
        maximum_requests=1,
        maximum_bytes=(2 * _EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + 100),
        maximum_elapsed_seconds=5,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        await subject.get("https://listing.example/one", policy=policy, budget=budget)
        with pytest.raises(DiscoveryBudgetExceededError) as raised:
            await subject.get("https://listing.example/two", policy=policy, budget=budget)

    assert raised.value.reason == "listing_request_limit"
    assert calls == 1


async def test_listing_client_rejects_declared_body_beyond_cumulative_remaining_bytes() -> None:
    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    budget = DiscoveryRunBudget(
        maximum_requests=2,
        maximum_bytes=(_EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT + 5),
        maximum_elapsed_seconds=5,
    )
    transport = httpx.MockTransport(lambda _: httpx.Response(200, content=b"123456"))
    async with httpx.AsyncClient(transport=transport) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        with pytest.raises(DiscoveryBudgetExceededError) as raised:
            await subject.get("https://listing.example/", policy=policy, budget=budget)

    assert raised.value.reason == "listing_byte_limit"
    assert budget.consumed_bytes == (_EXPECTED_DNS_CONTROL_BYTES + HTTP_CONTROL_BYTES_PER_ATTEMPT)


async def test_listing_client_rejects_encoded_body_before_decoding_or_iteration() -> None:
    stream = TrackingStream()
    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    transport = httpx.MockTransport(
        lambda _: httpx.Response(
            200,
            headers={"Content-Encoding": "gzip"},
            stream=stream,
        )
    )

    async with httpx.AsyncClient(transport=transport) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        with pytest.raises(SourceResponseError, match="Content-Encoding"):
            await subject.get("https://listing.example/", policy=policy)

    assert not stream.iterated
    assert stream.closed


async def test_listing_client_rejects_nonpositive_limits() -> None:
    async with httpx.AsyncClient() as client:
        with pytest.raises(ValueError, match="positive"):
            SafeHttpListingClient(client, maximum_listing_bytes=0)


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (403, SourceBlockedError),
        (429, SourceAccessError),
        (503, SourceAccessError),
        (404, SourceResponseError),
    ],
)
async def test_listing_client_classifies_http_statuses(
    status: int,
    error_type: type[Exception],
) -> None:
    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(status))
    ) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        with pytest.raises(error_type):
            await subject.get("https://listing.example/", policy=policy)


@pytest.mark.parametrize(
    ("headers", "maximum_redirects", "expected"),
    [
        ({"Location": "https://listing.example/again"}, 0, "redirect limit"),
        ({}, 1, "omitted Location"),
    ],
)
async def test_listing_client_rejects_exhausted_or_malformed_redirects(
    headers: dict[str, str],
    maximum_redirects: int,
    expected: str,
) -> None:
    policy = RemoteAccessPolicy(
        allowed_hosts=frozenset({"listing.example"}),
        maximum_redirects=maximum_redirects,
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(302, headers=headers))
    ) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        with pytest.raises((UnsafeRemoteError, SourceResponseError), match=expected):
            await subject.get("https://listing.example/", policy=policy)


async def test_listing_client_rejects_credentials_empty_dns_and_invalid_dns() -> None:
    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    async with httpx.AsyncClient() as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        with pytest.raises(UnsafeRemoteError, match="credentials"):
            await subject.get("https://user:pass@listing.example/", policy=policy)

        async def empty_resolver(_host: str, _port: int) -> tuple[str, ...]:
            return ()

        subject = SafeHttpListingClient(client, resolver=empty_resolver)
        with pytest.raises(SourceAccessError, match="no addresses"):
            await subject.get("https://listing.example/", policy=policy)

        async def invalid_resolver(_host: str, _port: int) -> tuple[str, ...]:
            return ("not-an-address",)

        subject = SafeHttpListingClient(client, resolver=invalid_resolver)
        with pytest.raises(UnsafeRemoteError, match="invalid address"):
            await subject.get("https://listing.example/", policy=policy)


@pytest.mark.parametrize("length", ["invalid", "-1"])
async def test_listing_client_rejects_invalid_content_length(length: str) -> None:
    policy = RemoteAccessPolicy(allowed_hosts=frozenset({"listing.example"}))
    transport = httpx.MockTransport(
        lambda _: httpx.Response(200, headers={"Content-Length": length}, content=b"")
    )
    async with httpx.AsyncClient(transport=transport) as client:
        subject = SafeHttpListingClient(client, resolver=public_resolver)
        with pytest.raises(SourceResponseError, match="Content-Length"):
            await subject.get("https://listing.example/", policy=policy)
