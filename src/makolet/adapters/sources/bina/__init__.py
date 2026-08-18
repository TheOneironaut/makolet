"""BINA ASP.NET JSON listing and download-path resolver."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date
from urllib.parse import urlsplit

from makolet.adapters.download.http import RemoteAccessPolicy
from makolet.adapters.sources.common import (
    absolute_url,
    build_remote_file,
    cursor_int,
    cursor_text,
    decode_cursor,
    deduplicate_files,
    encode_cursor,
    json_records,
    mapping_value,
    optional_nonnegative_int,
    parse_json_listing,
    required_text,
    validate_discovered_url,
    validate_limit,
)
from makolet.adapters.sources.http import HttpListingClient
from makolet.application.models import DiscoveryCursor, DiscoveryPage, DiscoveryRunBudget
from makolet.application.ports import Clock
from makolet.domain.enums import CompressionFormat
from makolet.domain.errors import SourceResponseError
from makolet.domain.filenames import safe_basename
from makolet.domain.normalization import ISRAEL_TIMEZONE

_DATE_PATTERN = re.compile(r"^20\d{2}-\d{2}-\d{2}$")


@dataclass(frozen=True, slots=True)
class BinaPartition:
    file_type: str
    store_id: str = ""


DEFAULT_PARTITIONS = tuple(
    BinaPartition(file_type)
    # Observed BINA category codes: Stores, Price, Promo, PriceFull, PromoFull.
    for file_type in ("1", "2", "3", "4", "5")
)


@dataclass(frozen=True, slots=True)
class BinaSourceConfig:
    retailer_id: str
    portal_id: str
    base_url: str
    query_identifier: str
    chain_id: str
    network_filter: str = ""
    partitions: tuple[BinaPartition, ...] = DEFAULT_PARTITIONS
    server_result_cap: int = 1_000
    zip_wrapped_file_types: frozenset[str] = frozenset()

    @property
    def host(self) -> str:
        return urlsplit(self.base_url).hostname or ""

    @property
    def listing_url(self) -> str:
        return absolute_url(self.base_url, "/MainIO_Hok.aspx")

    @property
    def resolver_url(self) -> str:
        return absolute_url(self.base_url, "/Download.aspx")

    @property
    def policy(self) -> RemoteAccessPolicy:
        return RemoteAccessPolicy(
            allowed_hosts=frozenset({self.host}),
            maximum_response_bytes=8 * 1024 * 1024,
        )


@dataclass(frozen=True, slots=True)
class _BinaEntry:
    filename: str
    path: str | None
    content_length: int | None


class BinaSourceAdapter:
    def __init__(
        self,
        config: BinaSourceConfig,
        http: HttpListingClient,
        clock: Clock,
    ) -> None:
        if not config.partitions:
            raise ValueError("BINA configuration requires at least one listing partition")
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
        offset = cursor_int(state, "offset")
        discovery_date = cursor_text(state, "date") or self._current_date().isoformat()
        if not _DATE_PATTERN.fullmatch(discovery_date):
            raise SourceResponseError("BINA discovery cursor contains an invalid date")
        while True:
            partition = self._config.partitions[partition_index]
            response = await self._http.get(
                self._config.listing_url,
                policy=self._config.policy,
                query=(
                    ("_", self._config.query_identifier),
                    ("wReshet", self._config.network_filter),
                    ("WFileType", partition.file_type),
                    ("WDate", date.fromisoformat(discovery_date).strftime("%d/%m/%Y")),
                    ("WStore", partition.store_id),
                ),
                budget=active_budget,
            )
            records = _partition_records(parse_json_listing(response.body))
            if len(records) >= self._config.server_result_cap:
                raise SourceResponseError(
                    "BINA partition reached the server cap; configure narrower store partitions"
                )
            entries = tuple(
                sorted((_entry(record) for record in records), key=lambda item: item.filename)
            )
            if offset > len(entries):
                raise SourceResponseError("BINA discovery cursor is beyond its listing partition")
            selected_entries = entries[offset : offset + limit]
            if (
                selected_entries
                or offset > 0
                or partition_index + 1 >= len(self._config.partitions)
            ):
                break
            partition_index += 1
            offset = 0
        discovered_at = self._clock.now()
        files = []
        for entry in selected_entries:
            download_url = await self._download_url(entry, budget=active_budget)
            remote_file = build_remote_file(
                retailer_id=self._config.retailer_id,
                portal_id=self._config.portal_id,
                download_url=download_url,
                original_filename=entry.filename,
                discovered_at=discovered_at,
                allowed_hosts=frozenset({self._config.host}),
                allowed_schemes=frozenset({"https"}),
                content_length=entry.content_length,
            )
            if partition.file_type in self._config.zip_wrapped_file_types:
                remote_file = replace(remote_file, compression=CompressionFormat.ZIP)
            files.append(remote_file)
        next_offset = offset + len(selected_entries)
        next_state: dict[str, int | str] | None
        if next_offset < len(entries):
            next_state = {
                "date": discovery_date,
                "partition": partition_index,
                "offset": next_offset,
            }
        elif partition_index + 1 < len(self._config.partitions):
            next_state = {
                "date": discovery_date,
                "partition": partition_index + 1,
                "offset": 0,
            }
        else:
            next_state = None
        next_cursor = (
            DiscoveryCursor(encode_cursor(self.source_id, next_state))
            if next_state is not None
            else None
        )
        return DiscoveryPage(
            files=deduplicate_files(files),
            next_cursor=next_cursor,
            complete=next_cursor is None,
        )

    def _current_date(self) -> date:
        return self._clock.now().astimezone(ISRAEL_TIMEZONE).date()

    async def _download_url(
        self,
        entry: _BinaEntry,
        *,
        budget: DiscoveryRunBudget,
    ) -> str:
        path = entry.path
        if path is None:
            response = await self._http.get(
                self._config.resolver_url,
                policy=self._config.policy,
                query=(("FileNm", entry.filename),),
                budget=budget,
            )
            payload = parse_json_listing(response.body)
            resolver_record = _resolver_record(payload)
            resolved = mapping_value(resolver_record, "SPath", "path", "url")
            if not isinstance(resolved, str) or not resolved.strip():
                raise SourceResponseError("BINA download resolver omitted SPath")
            path = resolved.strip()
        if path.endswith("/"):
            path += safe_basename(entry.filename)
        download_url = absolute_url(self._config.base_url, path)
        validate_discovered_url(
            download_url,
            allowed_hosts=frozenset({self._config.host}),
            allowed_schemes=frozenset({"https"}),
        )
        return download_url


def _entry(record: Mapping[str, object]) -> _BinaEntry:
    filename = required_text(record, "FileNm", "fileName", "filename", "name")
    raw_path = mapping_value(record, "SPath", "path", "url", "downloadUrl")
    path = raw_path.strip() if isinstance(raw_path, str) and raw_path.strip() else None
    return _BinaEntry(
        filename=filename,
        path=path,
        content_length=optional_nonnegative_int(record, "FileSize", "size", "length"),
    )


def _partition_records(payload: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(payload, list):
        return json_records(payload, ())
    return json_records(payload, ("files", "data", "results", "d"))


def _resolver_record(payload: object) -> Mapping[str, object]:
    if isinstance(payload, Mapping):
        record = {str(key): value for key, value in payload.items()}
        if mapping_value(record, "SPath", "path", "url") is not None:
            return record
    records = json_records(payload, ())
    if len(records) != 1:
        raise SourceResponseError("BINA download resolver did not return exactly one path")
    return records[0]


__all__ = [
    "DEFAULT_PARTITIONS",
    "BinaPartition",
    "BinaSourceAdapter",
    "BinaSourceConfig",
]
