"""Laib/Matrix whole-catalog JSON source discovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import quote

from makolet.adapters.download.http import RemoteAccessPolicy
from makolet.adapters.sources.common import (
    build_remote_file,
    cursor_int,
    decode_cursor,
    deduplicate_files,
    encode_cursor,
    json_records,
    mapping_value,
    optional_listing_timestamp,
    optional_nonnegative_int,
    parse_json_listing,
    required_text,
    validate_limit,
)
from makolet.adapters.sources.http import HttpListingClient
from makolet.application.models import DiscoveryCursor, DiscoveryPage, DiscoveryRunBudget
from makolet.application.ports import Clock
from makolet.domain.errors import SourceResponseError
from makolet.domain.models import RemoteFile


@dataclass(frozen=True, slots=True)
class MatrixSourceConfig:
    retailer_id: str
    portal_id: str
    edi: str
    api_root: str = "https://laibcatalog.co.il/webapi"
    maximum_catalog_entries: int = 10_000

    @property
    def host(self) -> str:
        return "laibcatalog.co.il"

    @property
    def listing_url(self) -> str:
        return f"{self.api_root.rstrip('/')}/api/getfiles"

    @property
    def policy(self) -> RemoteAccessPolicy:
        return RemoteAccessPolicy(
            allowed_hosts=frozenset({self.host}),
            maximum_response_bytes=8 * 1024 * 1024,
        )


class MatrixSourceAdapter:
    def __init__(
        self,
        config: MatrixSourceConfig,
        http: HttpListingClient,
        clock: Clock,
    ) -> None:
        self._config = config
        self._http = http
        self._clock = clock
        self._catalog: tuple[RemoteFile, ...] | None = None
        self.source_id = config.retailer_id

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: DiscoveryRunBudget | None = None,
    ) -> DiscoveryPage:
        validate_limit(limit)
        state = decode_cursor(self.source_id, cursor.value if cursor else None)
        offset = cursor_int(state, "offset")
        active_budget = budget or DiscoveryRunBudget()
        if cursor is None or self._catalog is None:
            self._catalog = await self._load_catalog(active_budget)
        ordered = self._catalog
        if offset > len(ordered):
            raise SourceResponseError("Matrix discovery cursor is beyond the current catalog")
        selected = ordered[offset : offset + limit]
        next_offset = offset + len(selected)
        next_cursor = (
            DiscoveryCursor(encode_cursor(self.source_id, {"offset": next_offset}))
            if next_offset < len(ordered)
            else None
        )
        return DiscoveryPage(files=selected, next_cursor=next_cursor, complete=next_cursor is None)

    async def _load_catalog(self, budget: DiscoveryRunBudget) -> tuple[RemoteFile, ...]:
        response = await self._http.get(
            self._config.listing_url,
            policy=self._config.policy,
            query=(("edi", self._config.edi),),
            budget=budget,
        )
        records = _catalog_records(parse_json_listing(response.body))
        if len(records) > self._config.maximum_catalog_entries:
            raise SourceResponseError("Matrix catalog exceeds its configured entry limit")
        discovered_at = self._clock.now()
        files = []
        for record in records:
            filename = required_text(record, "fileName", "filename", "name")
            download_url = (
                f"{self._config.api_root.rstrip('/')}/{quote(self._config.edi, safe='')}/"
                f"{quote(filename, safe='')}"
            )
            files.append(
                build_remote_file(
                    retailer_id=self._config.retailer_id,
                    portal_id=self._config.portal_id,
                    download_url=download_url,
                    original_filename=filename,
                    discovered_at=discovered_at,
                    allowed_hosts=frozenset({self._config.host}),
                    allowed_schemes=frozenset({"https"}),
                    content_length=_exact_file_size(record),
                    last_modified=optional_listing_timestamp(
                        mapping_value(record, "lastModified", "fileDate", "updatedAt")
                    ),
                )
            )
        return deduplicate_files(files)


def _catalog_records(payload: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(payload, list):
        records = json_records(payload, ())
    else:
        records = json_records(payload, ("files", "data", "results", "items"))
    if not records:
        raise SourceResponseError("Matrix catalog has no expected record collection")
    return records


def _exact_file_size(record: Mapping[str, object]) -> int | None:
    value = mapping_value(record, "fileSize", "size")
    if isinstance(value, str) and not value.strip().isdigit():
        # The live API renders rounded values such as "122.69 KB"; that is not
        # exact transport evidence and must not be converted into a claimed byte count.
        return None
    return optional_nonnegative_int(record, "fileSize", "size")


__all__ = ["MatrixSourceAdapter", "MatrixSourceConfig"]
