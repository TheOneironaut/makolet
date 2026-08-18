"""Static daily HTML indexes used by Carrefour and Wolt."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import quote

from makolet.adapters.download.http import RemoteAccessPolicy
from makolet.adapters.sources.common import (
    absolute_url,
    bounded_json_value_span,
    build_remote_file,
    cursor_int,
    cursor_text,
    decode_cursor,
    deduplicate_files,
    encode_cursor,
    filename_from_link,
    json_records,
    optional_nonnegative_int,
    parse_html_links,
    required_text,
    validate_limit,
)
from makolet.adapters.sources.http import HttpListingClient
from makolet.application.models import DiscoveryCursor, DiscoveryPage, DiscoveryRunBudget
from makolet.application.ports import Clock
from makolet.domain.errors import SourceResponseError, UnsafeRemoteError
from makolet.domain.models import RemoteFile

_DATE_TOKEN = re.compile(r"(?<!\d)(20\d{2})-?(\d{2})-?(\d{2})(?!\d)")
_EMBEDDED_PATH = re.compile(r"\bconst\s+path\s*=\s*(['\"])(?P<path>20\d{6})\1\s*;")
_EMBEDDED_FILES = re.compile(r"\bconst\s+files\s*=\s*")


@dataclass(frozen=True, slots=True)
class StaticDailyFeedConfig:
    portal_id: str
    index_url: str
    listing_hosts: frozenset[str]
    download_hosts: frozenset[str]

    @property
    def policy(self) -> RemoteAccessPolicy:
        return RemoteAccessPolicy(
            allowed_hosts=self.listing_hosts,
            redirect_hosts=self.listing_hosts,
            maximum_response_bytes=8 * 1024 * 1024,
        )


@dataclass(frozen=True, slots=True)
class StaticDailySourceConfig:
    retailer_id: str
    feeds: tuple[StaticDailyFeedConfig, ...]


class StaticDailySourceAdapter:
    def __init__(
        self,
        config: StaticDailySourceConfig,
        http: HttpListingClient,
        clock: Clock,
    ) -> None:
        if not config.feeds:
            raise ValueError("Static daily configuration requires at least one feed")
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
        feed_index = cursor_int(state, "feed", maximum=len(self._config.feeds) - 1)
        offset = cursor_int(state, "offset")
        cursor_date = cursor_text(state, "date")
        feed = self._config.feeds[feed_index]
        date_token, files = await self._catalog(feed, cursor_date, budget=active_budget)
        if offset > len(files):
            raise SourceResponseError("Static daily cursor is beyond the selected catalog")
        selected = files[offset : offset + limit]
        next_offset = offset + len(selected)
        if next_offset < len(files):
            next_state: dict[str, int | str] | None = {
                "feed": feed_index,
                "date": date_token,
                "offset": next_offset,
            }
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

    async def _catalog(
        self,
        feed: StaticDailyFeedConfig,
        selected_date: str | None,
        *,
        budget: DiscoveryRunBudget,
    ) -> tuple[str, tuple[RemoteFile, ...]]:
        index_response = await self._http.get(
            feed.index_url,
            policy=feed.policy,
            budget=budget,
        )
        index_links = parse_html_links(index_response.body)
        embedded = _embedded_catalog(index_response.body)
        dates = sorted(
            {
                normalized
                for link in index_links
                if (normalized := _date_in(f"{link.href} {link.text} {link.row_text}"))
            }
            | ({embedded[0]} if embedded is not None else set())
        )
        date_token = selected_date or (dates[-1] if dates else None)
        if date_token is None:
            raise SourceResponseError("Static source index contains no dated publication")
        if selected_date is not None and selected_date not in dates:
            raise SourceResponseError("Static daily cursor date is no longer listed")
        if embedded is not None and embedded[0] == date_token:
            return date_token, self._embedded_files(
                feed,
                index_response.final_url,
                date_token,
                embedded[1],
            )
        matching_file_links = tuple(
            link
            for link in index_links
            if _date_in(link.href) == date_token and filename_from_link(link) is not None
        )
        base_url = index_response.final_url
        file_links = matching_file_links
        if not file_links:
            date_link = next(
                (
                    link
                    for link in index_links
                    if _date_in(f"{link.href} {link.text}") == date_token
                ),
                None,
            )
            if date_link is None:
                raise SourceResponseError("Static daily index omitted the selected date link")
            page_url = absolute_url(index_response.final_url, date_link.href)
            _validate_listing_page_url(page_url, feed.listing_hosts)
            day_response = await self._http.get(
                page_url,
                policy=feed.policy,
                budget=budget,
            )
            base_url = day_response.final_url
            file_links = tuple(
                link for link in parse_html_links(day_response.body) if filename_from_link(link)
            )
            if not file_links:
                raise SourceResponseError("Static daily page contains no recognized source files")
        discovered_at = self._clock.now()
        files = []
        for link in file_links:
            filename = filename_from_link(link)
            if filename is None:
                continue
            download_url = absolute_url(base_url, link.href)
            files.append(
                build_remote_file(
                    retailer_id=self._config.retailer_id,
                    portal_id=feed.portal_id,
                    download_url=download_url,
                    original_filename=filename,
                    discovered_at=discovered_at,
                    allowed_hosts=feed.download_hosts,
                    allowed_schemes=frozenset({"https"}),
                )
            )
        return date_token, deduplicate_files(files)

    def _embedded_files(
        self,
        feed: StaticDailyFeedConfig,
        base_url: str,
        date_token: str,
        records: tuple[Mapping[str, object], ...],
    ) -> tuple[RemoteFile, ...]:
        discovered_at = self._clock.now()
        path = date_token.replace("-", "")
        files = []
        for record in records:
            filename = required_text(record, "name", "fileName", "filename")
            download_url = absolute_url(
                base_url,
                f"{path}/{quote(filename, safe='')}",
            )
            files.append(
                build_remote_file(
                    retailer_id=self._config.retailer_id,
                    portal_id=feed.portal_id,
                    download_url=download_url,
                    original_filename=filename,
                    discovered_at=discovered_at,
                    allowed_hosts=feed.download_hosts,
                    allowed_schemes=frozenset({"https"}),
                    content_length=optional_nonnegative_int(record, "size", "fileSize"),
                )
            )
        return deduplicate_files(files)


def _date_in(value: str) -> str | None:
    match = _DATE_TOKEN.search(value)
    return "-".join(match.groups()) if match else None


def _embedded_catalog(
    body: bytes,
) -> tuple[str, tuple[Mapping[str, object], ...]] | None:
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise SourceResponseError("Static source HTML is not UTF-8") from error
    path_match = _EMBEDDED_PATH.search(text)
    files_match = _EMBEDDED_FILES.search(text)
    if path_match is None and files_match is None:
        return None
    if path_match is None or files_match is None:
        raise SourceResponseError("Static source embedded catalog is incomplete")
    value_start, value_end = bounded_json_value_span(text, files_match.end())
    try:
        payload, decoded_end = json.JSONDecoder().raw_decode(text, value_start)
    except (json.JSONDecodeError, RecursionError) as error:
        raise SourceResponseError("Static source embedded catalog is malformed") from error
    if decoded_end != value_end:
        raise SourceResponseError("Static source embedded catalog is malformed")
    date_token = _date_in(path_match.group("path"))
    if date_token is None:
        raise SourceResponseError("Static source embedded path has no valid date")
    return date_token, json_records(payload, ())


def _validate_listing_page_url(url: str, allowed_hosts: frozenset[str]) -> None:
    from urllib.parse import urlsplit

    parsed = urlsplit(url)
    if parsed.scheme.casefold() != "https" or (parsed.hostname or "").casefold() not in {
        host.casefold() for host in allowed_hosts
    }:
        raise UnsafeRemoteError("Static date page is outside its listing allowlist")
    if parsed.username is not None or parsed.password is not None:
        raise UnsafeRemoteError("Static date page contains credentials")
    try:
        port = parsed.port
    except ValueError as error:
        raise UnsafeRemoteError("Static date page has an invalid port") from error
    if port not in {None, 443}:
        raise UnsafeRemoteError("Static date page has a non-default HTTPS port")


__all__ = [
    "StaticDailyFeedConfig",
    "StaticDailySourceAdapter",
    "StaticDailySourceConfig",
]
