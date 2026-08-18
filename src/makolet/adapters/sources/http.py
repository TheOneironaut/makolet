"""Bounded HTTP transport for JSON and HTML discovery listings."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

import anyio
import httpx

from makolet.adapters.download.http import (
    HostResolver,
    HttpTransferMeter,
    RemoteAccessPolicy,
    activate_http_transfer_meter,
    install_http_wire_accounting,
    resolve_http_target,
    resolve_public_addresses,
    send_pinned_request,
)
from makolet.application.models import DiscoveryRunBudget
from makolet.application.ports import (
    HTTP_CONTROL_BYTES_PER_ATTEMPT,
    MAXIMUM_HTTP_RESOLVED_ADDRESSES,
    MAXIMUM_HTTP_TRANSFER_OVERHEAD_BYTES_PER_OPEN,
)
from makolet.domain.errors import (
    DiscoveryBudgetExceededError,
    DownloadLimitError,
    SourceAccessError,
    SourceBlockedError,
    SourceResponseError,
    UnsafeRemoteError,
)


@dataclass(frozen=True, slots=True)
class ListingResponse:
    body: bytes
    final_url: str
    headers: Mapping[str, str]


class HttpListingClient(Protocol):
    """Structural listing transport implemented by production and fixture clients."""

    async def get(
        self,
        url: str,
        *,
        policy: RemoteAccessPolicy,
        query: Sequence[tuple[str, str]] = (),
        budget: DiscoveryRunBudget | None = None,
    ) -> ListingResponse: ...


class SafeHttpListingClient(HttpListingClient):
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        resolver: HostResolver | None = None,
        maximum_listing_bytes: int = 8 * 1024 * 1024,
        request_timeout_seconds: float = 20.0,
    ) -> None:
        if maximum_listing_bytes <= 0 or request_timeout_seconds <= 0:
            raise ValueError("HTTP listing limits must be positive")
        install_http_wire_accounting(client)
        self._client = client
        self._resolver = resolver or resolve_public_addresses
        self._maximum_listing_bytes = maximum_listing_bytes
        self._request_timeout_seconds = request_timeout_seconds

    async def get(
        self,
        url: str,
        *,
        policy: RemoteAccessPolicy,
        query: Sequence[tuple[str, str]] = (),
        budget: DiscoveryRunBudget | None = None,
    ) -> ListingResponse:
        maximum_listing_body_bytes = min(
            self._maximum_listing_bytes,
            policy.maximum_response_bytes,
        )
        active_budget = budget or DiscoveryRunBudget(
            maximum_requests=((policy.maximum_redirects + 1) * MAXIMUM_HTTP_RESOLVED_ADDRESSES),
            maximum_bytes=(
                maximum_listing_body_bytes + MAXIMUM_HTTP_TRANSFER_OVERHEAD_BYTES_PER_OPEN
            ),
        )
        active_budget.checkpoint()
        if active_budget.remaining_bytes <= 0:
            raise DiscoveryBudgetExceededError("listing_byte_limit")
        meter = HttpTransferMeter(
            active_budget.remaining_bytes,
            limit_error=lambda _transferred_bytes: DiscoveryBudgetExceededError(
                "listing_byte_limit"
            ),
            on_charge=active_budget.consume_bytes,
            on_attempt=lambda: active_budget.begin_request(
                minimum_bytes=HTTP_CONTROL_BYTES_PER_ATTEMPT
            ),
        )
        current = httpx.URL(url, params=query or None)
        allowed_hosts = policy.allowed_hosts
        timeout_seconds = min(
            self._request_timeout_seconds,
            active_budget.remaining_elapsed_seconds,
        )
        with anyio.move_on_after(timeout_seconds) as timeout_scope:
            for redirect_number in range(policy.maximum_redirects + 1):
                meter.begin_dns_lookup()
                target = await resolve_http_target(
                    current,
                    policy,
                    allowed_hosts,
                    self._resolver,
                )
                try:
                    with activate_http_transfer_meter(meter):
                        response = await send_pinned_request(
                            self._client,
                            target,
                            headers={
                                "Accept": "application/json, text/html;q=0.9",
                                "Accept-Encoding": "identity",
                            },
                        )
                except httpx.TransportError as error:
                    raise SourceAccessError("Public source listing request failed") from error
                try:
                    if response.is_redirect:
                        if redirect_number == policy.maximum_redirects:
                            raise UnsafeRemoteError("Source listing exceeded its redirect limit")
                        location = response.headers.get("location")
                        if not location:
                            raise SourceResponseError("Source listing redirect omitted Location")
                        current = target.logical_url.join(location)
                        allowed_hosts = policy.redirect_hosts or policy.allowed_hosts
                        continue

                    _raise_for_status(response.status_code)
                    _validate_content_encoding(response.headers.get("content-encoding"))
                    maximum_bytes = maximum_listing_body_bytes
                    declared_length = _content_length(response.headers.get("content-length"))
                    if declared_length is not None and declared_length > maximum_bytes:
                        raise DownloadLimitError("Source listing declares too many bytes")
                    if (
                        declared_length is not None
                        and declared_length > active_budget.remaining_bytes
                    ):
                        raise DiscoveryBudgetExceededError("listing_byte_limit")
                    body = bytearray()
                    async for chunk in response.aiter_raw():
                        if len(chunk) > maximum_bytes - len(body):
                            raise DownloadLimitError("Source listing exceeds its byte limit")
                        meter.record_payload(len(chunk))
                        body.extend(chunk)
                    active_budget.checkpoint()
                    return ListingResponse(
                        body=bytes(body),
                        final_url=str(target.logical_url),
                        headers={key.casefold(): value for key, value in response.headers.items()},
                    )
                finally:
                    with anyio.CancelScope(shield=True):
                        await response.aclose()
        if timeout_scope.cancel_called and budget is not None and active_budget.elapsed_exhausted:
            raise DiscoveryBudgetExceededError("listing_elapsed_limit")
        if timeout_scope.cancel_called:
            raise SourceAccessError("Public source listing request exceeded its total deadline")
        raise AssertionError("redirect loop must return or raise")


def _content_length(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError as error:
        raise SourceResponseError("Source listing Content-Length is invalid") from error
    if parsed < 0:
        raise SourceResponseError("Source listing Content-Length is negative")
    return parsed


def _validate_content_encoding(value: str | None) -> None:
    if value is None or value.strip().casefold() == "identity":
        return
    raise SourceResponseError("Source listing returned an unsupported Content-Encoding")


def _raise_for_status(status_code: int) -> None:
    if status_code == 200:
        return
    if status_code in {401, 403}:
        raise SourceBlockedError("Public source listing denied access")
    if status_code in {408, 425, 429} or status_code >= 500:
        raise SourceAccessError(f"Public source listing returned transient status {status_code}")
    raise SourceResponseError(f"Public source listing returned HTTP status {status_code}")
