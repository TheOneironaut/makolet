from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

from makolet.adapters.download.http import RemoteAccessPolicy
from makolet.adapters.sources.common import append_query
from makolet.adapters.sources.http import HttpListingClient, ListingResponse
from makolet.adapters.sources.ncr import FtpCatalogClient, FtpCatalogEntry, NcrFeedConfig
from makolet.application.models import DiscoveryRunBudget


class FixedClock:
    def __init__(self, value: datetime | None = None) -> None:
        self.value = value or datetime(2026, 8, 11, 12, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value


class FixtureHttpClient(HttpListingClient):
    def __init__(
        self,
        responses: Mapping[tuple[str, tuple[tuple[str, str], ...]], bytes],
    ) -> None:
        self._responses = dict(responses)
        self.calls: list[tuple[str, tuple[tuple[str, str], ...]]] = []

    async def get(
        self,
        url: str,
        *,
        policy: RemoteAccessPolicy,
        query: Sequence[tuple[str, str]] = (),
        budget: DiscoveryRunBudget | None = None,
    ) -> ListingResponse:
        del policy
        active_budget = budget or DiscoveryRunBudget()
        active_budget.begin_request()
        key = (url, tuple(query))
        self.calls.append(key)
        try:
            body = self._responses[key]
        except KeyError as error:
            raise AssertionError(f"Unexpected fixture request: {key}") from error
        active_budget.consume_bytes(len(body))
        return ListingResponse(body, append_query(url, tuple(query)), {})


class FixtureFtpClient(FtpCatalogClient):
    def __init__(self, entries: Mapping[str, tuple[FtpCatalogEntry, ...]]) -> None:
        self._entries = dict(entries)
        self.calls: list[str] = []

    async def list(
        self,
        feed: NcrFeedConfig,
        *,
        budget: DiscoveryRunBudget | None = None,
    ) -> tuple[FtpCatalogEntry, ...]:
        active_budget = budget or DiscoveryRunBudget()
        active_budget.begin_request()
        self.calls.append(feed.portal_id)
        entries = self._entries[feed.portal_id]
        active_budget.consume_bytes(
            sum(len(entry.filename.encode("utf-8")) + 32 for entry in entries)
        )
        return entries
