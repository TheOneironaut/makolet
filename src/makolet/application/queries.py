"""Bounded read-only query use cases shared by HTTP, CLI, and MCP."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Final
from uuid import UUID

from makolet.application.models import Page
from makolet.application.ports import Clock, QueryRepository
from makolet.domain.errors import DomainValidationError, QueryLimitError
from makolet.domain.normalization import (
    normalize_identifier,
    normalize_search_text,
    parse_quantity_text,
    without_first_quantity_text,
)

MAXIMUM_PRODUCT_IDENTIFIERS: Final = 200
PRODUCT_IDENTIFIER_LIMIT_MESSAGE: Final = (
    f"Product identifier count exceeds the {MAXIMUM_PRODUCT_IDENTIFIERS}-item public query limit"
)

# Public ``limit`` values remain request ceilings.  The repository receives the
# smaller route-specific value so publisher-controlled text cannot make a valid
# page exceed either the HTTP body or duplicated MCP result working set before
# serialization.  A non-final short page always carries the ordinary cursor.
MAXIMUM_MATERIALIZED_RETAILERS: Final = 50
MAXIMUM_MATERIALIZED_STORES: Final = 5
MAXIMUM_MATERIALIZED_PRODUCT_RESULTS: Final = 5
MAXIMUM_MATERIALIZED_PRICE_RESULTS: Final = 28
MAXIMUM_MATERIALIZED_HISTORY_RESULTS: Final = 28
MAXIMUM_MATERIALIZED_PROMOTION_RESULTS: Final = 1
MAXIMUM_MATERIALIZED_AVAILABILITY_RESULTS: Final = 28
MAXIMUM_MATERIALIZED_FRESHNESS_RESULTS: Final = 50
MAXIMUM_MATERIALIZED_SOURCE_STATUS_RESULTS: Final = 32

_CURSOR_PREFIX = "mq1"
_CURSOR_VERSION = 1
_CURSOR_MAXIMUM_CHARACTERS = 512
_CURSOR_DOMAIN = b"makolet-public-query-cursor-v1\0"


@dataclass(frozen=True, slots=True)
class _CursorScope:
    route: str
    fingerprint: str


@dataclass(frozen=True, slots=True)
class _DecodedCursor:
    position: str | None
    state: dict[str, str]


@dataclass(frozen=True, slots=True)
class QueryLimits:
    default_page_size: int = 50
    maximum_page_size: int = 200
    maximum_history_page_size: int = 1_000
    default_history_span: timedelta = timedelta(days=366)
    maximum_history_span: timedelta = timedelta(days=366 * 10)
    minimum_query_characters: int = 3
    maximum_query_characters: int = 200

    def __post_init__(self) -> None:
        if self.default_page_size <= 0 or self.maximum_page_size < self.default_page_size:
            raise ValueError("Query page limits are inconsistent")
        if self.maximum_history_page_size < self.maximum_page_size:
            raise ValueError("History page limit cannot be smaller than the normal limit")
        if self.maximum_history_span <= timedelta(0):
            raise ValueError("Query history span must be positive")
        if self.default_history_span <= timedelta(0):
            raise ValueError("Default history span must be positive")
        if (
            self.minimum_query_characters <= 0
            or self.maximum_query_characters < self.minimum_query_characters
        ):
            raise ValueError("Query text limits are inconsistent")


class QueryService:
    def __init__(
        self,
        repository: QueryRepository,
        clock: Clock,
        limits: QueryLimits | None = None,
    ) -> None:
        self._repository = repository
        self._clock = clock
        self._limits = limits or QueryLimits()

    async def list_retailers(self, *, limit: int | None = None, cursor: str | None = None) -> Page:
        scope = _cursor_scope("retailers")
        decoded = _decode_cursor(cursor, scope)
        page = await self._repository.list_retailers(
            limit=self._limit(
                limit,
                materialization_maximum=MAXIMUM_MATERIALIZED_RETAILERS,
            ),
            cursor=decoded.position,
        )
        return _bind_page(page, scope)

    async def find_stores(
        self,
        *,
        query: str | None = None,
        retailer_id: UUID | None = None,
        city: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        normalized_query = self._optional_query(query)
        normalized_city = self._optional_query(city)
        scope = _cursor_scope(
            "stores.find",
            query=normalized_query,
            retailer_id=retailer_id,
            city=normalized_city,
        )
        decoded = _decode_cursor(cursor, scope)
        page = await self._repository.find_stores(
            query=normalized_query,
            retailer_id=retailer_id,
            city=normalized_city,
            limit=self._limit(
                limit,
                materialization_maximum=MAXIMUM_MATERIALIZED_STORES,
            ),
            cursor=decoded.position,
        )
        return _bind_page(page, scope)

    async def search_products(
        self,
        query: str,
        *,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        structured_quantity = parse_quantity_text(query)
        textual_query = without_first_quantity_text(query) if structured_quantity else query
        normalized_query = self._required_query(textual_query)
        scope = _cursor_scope(
            "products.search",
            query=normalized_query,
            quantity=structured_quantity.amount if structured_quantity else None,
            unit=structured_quantity.unit if structured_quantity else None,
        )
        decoded = _decode_cursor(cursor, scope)
        page = await self._repository.search_products(
            normalized_query,
            quantity=structured_quantity.amount if structured_quantity else None,
            unit=structured_quantity.unit if structured_quantity else None,
            limit=self._limit(
                limit,
                materialization_maximum=MAXIMUM_MATERIALIZED_PRODUCT_RESULTS,
            ),
            cursor=decoded.position,
        )
        return _bind_page(page, scope)

    async def get_product(self, product_id: UUID) -> dict[str, object] | None:
        return await self._repository.get_product(product_id)

    async def find_product_by_barcode(self, barcode: str) -> dict[str, object] | None:
        normalized = normalize_identifier(barcode)
        if not normalized.isascii() or not normalized.isdigit() or len(normalized) > 32:
            raise DomainValidationError("Barcode must be an ASCII numeric identifier")
        return await self._repository.find_product_by_barcode(normalized)

    async def find_product_by_retailer_item_code(
        self,
        retailer_id: UUID,
        item_code: str,
        *,
        portal_id: UUID | None = None,
    ) -> dict[str, object] | None:
        normalized = normalize_identifier(item_code)
        if not normalized:
            raise DomainValidationError("Retailer item code is empty after normalization")
        if len(normalized) > 128:
            raise DomainValidationError("Retailer item code exceeds 128 characters")
        return await self._repository.find_product_by_retailer_item_code(
            retailer_id,
            normalized,
            portal_id=portal_id,
        )

    async def current_prices(
        self,
        product_id: UUID,
        *,
        retailer_id: UUID | None = None,
        store_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        scope = _cursor_scope(
            "prices.current",
            product_id=product_id,
            retailer_id=retailer_id,
            store_id=store_id,
        )
        decoded = _decode_cursor(cursor, scope)
        page = await self._repository.current_prices(
            product_id,
            retailer_id=retailer_id,
            store_id=store_id,
            limit=self._limit(
                limit,
                materialization_maximum=MAXIMUM_MATERIALIZED_PRICE_RESULTS,
            ),
            cursor=decoded.position,
        )
        return _bind_page(page, scope)

    async def compare_product_prices(
        self,
        product_id: UUID,
        *,
        retailer_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        scope = _cursor_scope(
            "prices.compare",
            product_id=product_id,
            retailer_id=retailer_id,
        )
        decoded = _decode_cursor(cursor, scope)
        page = await self._repository.current_prices(
            product_id,
            retailer_id=retailer_id,
            store_id=None,
            limit=self._limit(
                limit,
                materialization_maximum=MAXIMUM_MATERIALIZED_PRICE_RESULTS,
            ),
            cursor=decoded.position,
        )
        return _bind_page(page, scope)

    async def price_history(
        self,
        product_id: UUID,
        *,
        store_id: UUID | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        requested_start, requested_end = self._requested_history_range(since, until)
        scope = _cursor_scope(
            "prices.history",
            product_id=product_id,
            store_id=store_id,
            since=requested_start,
            until=requested_end,
        )
        decoded = _decode_cursor(cursor, scope, state_keys=frozenset({"since", "until"}))
        start, end = self._resolved_history_range(
            requested_start,
            requested_end,
            decoded,
        )
        page = await self._repository.price_history(
            product_id,
            store_id=store_id,
            since=start,
            until=end,
            limit=self._limit(
                limit,
                maximum=self._limits.maximum_history_page_size,
                materialization_maximum=MAXIMUM_MATERIALIZED_HISTORY_RESULTS,
            ),
            cursor=decoded.position,
        )
        return _bind_page(
            page,
            scope,
            state={"since": _timestamp(start), "until": _timestamp(end)},
        )

    async def promotions(
        self,
        *,
        product_id: UUID | None = None,
        store_id: UUID | None = None,
        at: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        requested_time = _aware(at, "at") if at is not None else None
        scope = _cursor_scope(
            "promotions.active",
            product_id=product_id,
            store_id=store_id,
            at=requested_time,
        )
        decoded = _decode_cursor(cursor, scope, state_keys=frozenset({"at"}))
        if decoded.position is None:
            selected_time = requested_time or _aware(self._clock.now(), "at")
        else:
            selected_time = _cursor_datetime(decoded.state["at"])
            if requested_time is not None and selected_time != requested_time:
                raise DomainValidationError("Cursor is invalid or does not match this query")
        page = await self._repository.active_promotions(
            product_id=product_id,
            store_id=store_id,
            at=selected_time,
            limit=self._limit(
                limit,
                materialization_maximum=MAXIMUM_MATERIALIZED_PROMOTION_RESULTS,
            ),
            cursor=decoded.position,
        )
        return _bind_page(page, scope, state={"at": _timestamp(selected_time)})

    async def promotion_history(
        self,
        *,
        product_id: UUID | None = None,
        store_id: UUID | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        requested_start, requested_end = self._requested_history_range(since, until)
        scope = _cursor_scope(
            "promotions.history",
            product_id=product_id,
            store_id=store_id,
            since=requested_start,
            until=requested_end,
        )
        decoded = _decode_cursor(cursor, scope, state_keys=frozenset({"since", "until"}))
        start, end = self._resolved_history_range(
            requested_start,
            requested_end,
            decoded,
        )
        page = await self._repository.promotion_history(
            product_id=product_id,
            store_id=store_id,
            since=start,
            until=end,
            limit=self._limit(
                limit,
                maximum=self._limits.maximum_history_page_size,
                materialization_maximum=MAXIMUM_MATERIALIZED_PROMOTION_RESULTS,
            ),
            cursor=decoded.position,
        )
        return _bind_page(
            page,
            scope,
            state={"since": _timestamp(start), "until": _timestamp(end)},
        )

    async def item_availability(
        self,
        product_id: UUID,
        *,
        store_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        scope = _cursor_scope(
            "availability.current",
            product_id=product_id,
            store_id=store_id,
        )
        decoded = _decode_cursor(cursor, scope)
        page = await self._repository.item_availability(
            product_id,
            store_id=store_id,
            limit=self._limit(
                limit,
                materialization_maximum=MAXIMUM_MATERIALIZED_AVAILABILITY_RESULTS,
            ),
            cursor=decoded.position,
        )
        return _bind_page(page, scope)

    async def freshness(self, *, limit: int | None = None, cursor: str | None = None) -> Page:
        scope = _cursor_scope("freshness")
        decoded = _decode_cursor(cursor, scope)
        page = await self._repository.freshness(
            limit=self._limit(
                limit,
                materialization_maximum=MAXIMUM_MATERIALIZED_FRESHNESS_RESULTS,
            ),
            cursor=decoded.position,
        )
        return _bind_page(page, scope)

    async def source_status(self, *, limit: int | None = None, cursor: str | None = None) -> Page:
        scope = _cursor_scope("source-status")
        decoded = _decode_cursor(cursor, scope)
        page = await self._repository.source_status(
            limit=self._limit(
                limit,
                materialization_maximum=MAXIMUM_MATERIALIZED_SOURCE_STATUS_RESULTS,
            ),
            cursor=decoded.position,
        )
        return _bind_page(page, scope)

    async def maintenance_status(self) -> dict[str, object]:
        return await self._repository.maintenance_status()

    async def platform_status(
        self,
        *,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> dict[str, object]:
        scope = _cursor_scope("platform-status")
        decoded = _decode_cursor(cursor, scope)
        source_status, maintenance = await asyncio.gather(
            self._repository.source_status(
                limit=self._limit(
                    limit,
                    materialization_maximum=MAXIMUM_MATERIALIZED_SOURCE_STATUS_RESULTS,
                ),
                cursor=decoded.position,
            ),
            self.maintenance_status(),
        )
        return {"maintenance": maintenance, "sources": _bind_page(source_status, scope)}

    def _limit(
        self,
        value: int | None,
        *,
        maximum: int | None = None,
        materialization_maximum: int | None = None,
    ) -> int:
        selected = self._limits.default_page_size if value is None else value
        upper = maximum or self._limits.maximum_page_size
        if selected <= 0 or selected > upper:
            raise QueryLimitError(f"limit must be between 1 and {upper}")
        if materialization_maximum is None:
            return selected
        if not 1 <= materialization_maximum <= upper:
            raise ValueError("Materialization page maximum is inconsistent")
        return min(selected, materialization_maximum)

    def _required_query(self, value: str) -> str:
        normalized = normalize_search_text(value)
        if not normalized:
            raise DomainValidationError("Search query is empty after normalization")
        if len(normalized) < self._limits.minimum_query_characters:
            raise QueryLimitError(
                "Search query must contain at least "
                f"{self._limits.minimum_query_characters} searchable characters"
            )
        if len(normalized) > self._limits.maximum_query_characters:
            raise QueryLimitError(
                f"Search query exceeds {self._limits.maximum_query_characters} characters"
            )
        return normalized

    def _optional_query(self, value: str | None) -> str | None:
        return None if value is None else self._required_query(value)

    def _requested_history_range(
        self, since: datetime | None, until: datetime | None
    ) -> tuple[datetime | None, datetime | None]:
        start = _aware(since, "since") if since is not None else None
        end = _aware(until, "until") if until is not None else None
        if (start is None) is not (end is None):
            raise DomainValidationError("since and until must be provided together")
        if start is not None and end is not None:
            self._validate_history_range(start, end)
        return start, end

    def _resolved_history_range(
        self,
        requested_start: datetime | None,
        requested_end: datetime | None,
        cursor: _DecodedCursor,
    ) -> tuple[datetime, datetime]:
        if cursor.position is not None:
            start = _cursor_datetime(cursor.state["since"])
            end = _cursor_datetime(cursor.state["until"])
            self._validate_history_range(start, end)
            return start, end
        if requested_start is not None and requested_end is not None:
            return requested_start, requested_end
        end = _aware(self._clock.now(), "history window end")
        try:
            start = end - min(
                self._limits.default_history_span,
                self._limits.maximum_history_span,
            )
        except OverflowError as error:
            raise QueryLimitError("Default history range cannot be represented") from error
        self._validate_history_range(start, end)
        return start, end

    def _validate_history_range(self, start: datetime, end: datetime) -> None:
        if start >= end:
            raise DomainValidationError("since must be before until")
        if end - start > self._limits.maximum_history_span:
            raise QueryLimitError("Requested history range is too wide")


def _cursor_scope(route: str, **filters: object) -> _CursorScope:
    normalized = {name: _cursor_filter_value(value) for name, value in filters.items()}
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _CursorScope(route=route, fingerprint=hashlib.sha256(encoded).hexdigest())


def _cursor_filter_value(value: object) -> str | int | bool | None:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _timestamp(value)
    raise TypeError(f"Unsupported cursor filter value: {type(value).__name__}")


def _decode_cursor(
    value: str | None,
    scope: _CursorScope,
    *,
    state_keys: frozenset[str] = frozenset(),
) -> _DecodedCursor:
    if value is None:
        return _DecodedCursor(position=None, state={})
    invalid = DomainValidationError("Cursor is invalid or does not match this query")
    cleaned = value.strip()
    if (
        not cleaned
        or len(cleaned) > _CURSOR_MAXIMUM_CHARACTERS
        or any(ord(character) < 32 or ord(character) > 126 for character in cleaned)
    ):
        raise invalid
    try:
        prefix, encoded, supplied_checksum = cleaned.split(".")
        padding = "=" * (-len(encoded) % 4)
        body = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        expected_checksum = hashlib.sha256(_CURSOR_DOMAIN + body).hexdigest()
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError) as error:
        raise invalid from error
    if (
        prefix != _CURSOR_PREFIX
        or len(supplied_checksum) != 64
        or not hmac.compare_digest(supplied_checksum, expected_checksum)
        or not isinstance(payload, dict)
        or set(payload) != {"f", "p", "r", "s", "v"}
        or payload.get("v") != _CURSOR_VERSION
        or payload.get("r") != scope.route
        or payload.get("f") != scope.fingerprint
    ):
        raise invalid
    position = payload.get("p")
    state = payload.get("s")
    if (
        not isinstance(position, str)
        or not position
        or len(position) > 256
        or any(ord(character) < 32 for character in position)
        or not isinstance(state, dict)
        or set(state) != state_keys
        or any(
            not isinstance(key, str)
            or not isinstance(item, str)
            or not item
            or len(item) > 64
            or any(ord(character) < 32 for character in item)
            for key, item in state.items()
        )
    ):
        raise invalid
    return _DecodedCursor(position=position, state=state)


def _bind_page(
    page: Page,
    scope: _CursorScope,
    *,
    state: dict[str, str] | None = None,
) -> Page:
    if page.next_cursor is None:
        return Page(items=page.items, next_cursor=None)
    position = page.next_cursor
    if not position or len(position) > 256 or any(ord(character) < 32 for character in position):
        raise DomainValidationError("Repository returned an invalid pagination cursor")
    payload: dict[str, Any] = {
        "f": scope.fingerprint,
        "p": position,
        "r": scope.route,
        "s": state or {},
        "v": _CURSOR_VERSION,
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    encoded = base64.urlsafe_b64encode(body).rstrip(b"=").decode("ascii")
    checksum = hashlib.sha256(_CURSOR_DOMAIN + body).hexdigest()
    cursor = f"{_CURSOR_PREFIX}.{encoded}.{checksum}"
    if len(cursor) > _CURSOR_MAXIMUM_CHARACTERS:
        raise DomainValidationError("Generated pagination cursor exceeds its size bound")
    return Page(items=page.items, next_cursor=cursor)


def _timestamp(value: datetime) -> str:
    return _aware(value, "cursor timestamp").isoformat()


def _cursor_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise DomainValidationError("Cursor is invalid or does not match this query") from error
    return _aware(parsed, "cursor timestamp")


def _aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DomainValidationError(f"{field_name} must include a timezone")
    return value.astimezone(UTC)
