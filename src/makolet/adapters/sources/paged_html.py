"""Bounded server-rendered HTML catalog adapter shared by observed custom portals."""

from __future__ import annotations

from dataclasses import dataclass, replace
from urllib.parse import urlsplit

from makolet.adapters.download.http import RemoteAccessPolicy
from makolet.adapters.sources.common import (
    absolute_url,
    build_remote_file,
    cursor_int,
    decode_cursor,
    deduplicate_files,
    encode_cursor,
    filename_from_link,
    listing_page_numbers,
    optional_listing_timestamp,
    parse_html_links,
    validate_limit,
)
from makolet.adapters.sources.http import HttpListingClient
from makolet.application.models import DiscoveryCursor, DiscoveryPage, DiscoveryRunBudget
from makolet.application.ports import Clock
from makolet.domain.enums import CompressionFormat
from makolet.domain.errors import SourceResponseError
from makolet.domain.normalization import ISRAEL_TIMEZONE


@dataclass(frozen=True, slots=True)
class HtmlListingPartition:
    query: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class PagedHtmlSourceConfig:
    retailer_id: str
    portal_id: str
    listing_url: str
    listing_hosts: frozenset[str]
    download_hosts: frozenset[str]
    partitions: tuple[HtmlListingPartition, ...] = (HtmlListingPartition(),)
    date_parameter: str | None = None
    page_parameter: str = "page"
    first_page: int = 1
    maximum_pages: int = 1_000
    page_size_hint: int | None = None
    assume_gzip_when_suffix_missing: bool = False

    @property
    def policy(self) -> RemoteAccessPolicy:
        return RemoteAccessPolicy(
            allowed_hosts=self.listing_hosts,
            redirect_hosts=self.listing_hosts,
            maximum_response_bytes=8 * 1024 * 1024,
        )


class PagedHtmlSourceAdapter:
    def __init__(
        self,
        config: PagedHtmlSourceConfig,
        http: HttpListingClient,
        clock: Clock,
    ) -> None:
        if not config.partitions or config.maximum_pages < config.first_page:
            raise ValueError("Paged HTML source configuration is invalid")
        self._config = config
        self._http = http
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
        partition_index = cursor_int(
            state,
            "partition",
            maximum=len(self._config.partitions) - 1,
        )
        page = cursor_int(
            state,
            "page",
            default=self._config.first_page,
            maximum=self._config.maximum_pages,
        )
        if page < self._config.first_page:
            raise SourceResponseError("HTML discovery cursor page is below the first page")
        offset = cursor_int(state, "offset")
        query = list(self._config.partitions[partition_index].query)
        if self._config.date_parameter is not None:
            local_date = self._clock.now().astimezone(ISRAEL_TIMEZONE).date().isoformat()
            query.append((self._config.date_parameter, local_date))
        query.append((self._config.page_parameter, str(page)))
        response = await self._http.get(
            self._config.listing_url,
            policy=self._config.policy,
            query=tuple(query),
            budget=active_budget,
        )
        links = parse_html_links(response.body)
        discovered_at = self._clock.now()
        files = []
        for link in links:
            download_url = absolute_url(response.final_url, link.href)
            if (urlsplit(download_url).hostname or "").casefold() not in {
                host.casefold() for host in self._config.download_hosts
            }:
                continue
            filename = filename_from_link(link)
            if filename is None:
                continue
            remote_file = build_remote_file(
                retailer_id=self._config.retailer_id,
                portal_id=self._config.portal_id,
                download_url=download_url,
                original_filename=filename,
                discovered_at=discovered_at,
                allowed_hosts=self._config.download_hosts,
                allowed_schemes=frozenset({"https"}),
                last_modified=optional_listing_timestamp(link.attribute("data-updated")),
            )
            if (
                self._config.assume_gzip_when_suffix_missing
                and remote_file.compression is CompressionFormat.UNKNOWN
            ):
                remote_file = replace(remote_file, compression=CompressionFormat.GZIP)
            files.append(remote_file)
        ordered = deduplicate_files(files)
        if not ordered:
            raise SourceResponseError("HTML source listing contains no recognized source files")
        if offset > len(ordered):
            raise SourceResponseError("HTML discovery cursor is beyond its current page")
        selected = ordered[offset : offset + limit]
        next_offset = offset + len(selected)
        if next_offset < len(ordered):
            next_state: dict[str, int] | None = {
                "partition": partition_index,
                "page": page,
                "offset": next_offset,
            }
        elif self._has_next_page(page, len(ordered), listing_page_numbers(links)):
            next_state = {"partition": partition_index, "page": page + 1, "offset": 0}
        elif partition_index + 1 < len(self._config.partitions):
            next_state = {
                "partition": partition_index + 1,
                "page": self._config.first_page,
                "offset": 0,
            }
        else:
            next_state = None
        next_cursor = (
            DiscoveryCursor(encode_cursor(self.source_id, next_state))
            if next_state is not None
            else None
        )
        return DiscoveryPage(selected, next_cursor, next_cursor is None)

    def _has_next_page(
        self,
        page: int,
        entry_count: int,
        listed_pages: tuple[int, ...],
    ) -> bool:
        explicit = any(candidate > page for candidate in listed_pages)
        implicit = (
            self._config.page_size_hint is not None and entry_count >= self._config.page_size_hint
        )
        return page < self._config.maximum_pages and (explicit or implicit)


__all__ = [
    "HtmlListingPartition",
    "PagedHtmlSourceAdapter",
    "PagedHtmlSourceConfig",
]
