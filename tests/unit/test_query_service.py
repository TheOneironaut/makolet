from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

from makolet.application.models import Page
from makolet.application.queries import _CURSOR_DOMAIN, QueryLimits, QueryService
from makolet.domain.errors import DomainValidationError, QueryLimitError
from tests.fakes.ingestion import FixedClock

PRODUCT_ID = UUID("10000000-0000-0000-0000-000000000001")


class RecordingQueryRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.page = Page(items=({"id": str(PRODUCT_ID)},), next_cursor="next")

    async def list_retailers(self, **kwargs: Any) -> Page:
        self.calls.append(("list_retailers", kwargs))
        return self.page

    async def find_stores(self, **kwargs: Any) -> Page:
        self.calls.append(("find_stores", kwargs))
        return self.page

    async def search_products(self, query: str, **kwargs: Any) -> Page:
        self.calls.append(("search_products", {"query": query, **kwargs}))
        return self.page

    async def get_product(self, product_id: UUID) -> dict[str, object] | None:
        self.calls.append(("get_product", {"product_id": product_id}))
        return self.page.items[0]

    async def find_product_by_barcode(self, barcode: str) -> dict[str, object] | None:
        self.calls.append(("find_product_by_barcode", {"barcode": barcode}))
        return self.page.items[0]

    async def find_product_by_retailer_item_code(
        self,
        retailer_id: UUID,
        item_code: str,
        *,
        portal_id: UUID | None = None,
    ) -> dict[str, object] | None:
        self.calls.append(
            (
                "find_product_by_retailer_item_code",
                {
                    "retailer_id": retailer_id,
                    "portal_id": portal_id,
                    "item_code": item_code,
                },
            )
        )
        return self.page.items[0]

    async def current_prices(self, product_id: UUID, **kwargs: Any) -> Page:
        self.calls.append(("current_prices", {"product_id": product_id, **kwargs}))
        return self.page

    async def price_history(self, product_id: UUID, **kwargs: Any) -> Page:
        self.calls.append(("price_history", {"product_id": product_id, **kwargs}))
        return self.page

    async def active_promotions(self, **kwargs: Any) -> Page:
        self.calls.append(("active_promotions", kwargs))
        return self.page

    async def promotion_history(self, **kwargs: Any) -> Page:
        self.calls.append(("promotion_history", kwargs))
        return self.page

    async def item_availability(self, product_id: UUID, **kwargs: Any) -> Page:
        self.calls.append(("item_availability", {"product_id": product_id, **kwargs}))
        return self.page

    async def freshness(self, **kwargs: Any) -> Page:
        self.calls.append(("freshness", kwargs))
        return self.page

    async def source_status(self, **kwargs: Any) -> Page:
        self.calls.append(("source_status", kwargs))
        return self.page

    async def maintenance_status(self) -> dict[str, object]:
        self.calls.append(("maintenance_status", {}))
        return {"active": False, "mode": "normal"}


def service() -> tuple[QueryService, RecordingQueryRepository]:
    repository = RecordingQueryRepository()
    return QueryService(repository, FixedClock()), repository


def _decode_cursor_envelope(cursor: str) -> tuple[str, object]:
    prefix, encoded, _checksum = cursor.split(".")
    body = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    return prefix, json.loads(body)


def _encode_cursor_envelope(prefix: str, payload: object) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    encoded = base64.urlsafe_b64encode(body).rstrip(b"=").decode("ascii")
    checksum = hashlib.sha256(_CURSOR_DOMAIN + body).hexdigest()
    return f"{prefix}.{encoded}.{checksum}"


@pytest.mark.asyncio
async def test_product_search_normalizes_mixed_hebrew_latin_query() -> None:
    subject, repository = service()

    page = await subject.search_products("  קפה—NESPRESSO, 10  ", limit=20)
    assert page.next_cursor is not None
    await subject.search_products("קפה nespresso 10", limit=20, cursor=page.next_cursor)

    assert repository.calls[-1] == (
        "search_products",
        {
            "query": "קפה nespresso 10",
            "quantity": None,
            "unit": None,
            "limit": 5,
            "cursor": "next",
        },
    )


@pytest.mark.asyncio
async def test_product_identifier_limit_error_propagates_unchanged() -> None:
    subject, repository = service()

    async def reject_product(_product_id: UUID) -> dict[str, object] | None:
        raise QueryLimitError("Product identifier count exceeds the 200-item public query limit")

    repository.get_product = reject_product  # type: ignore[assignment]

    with pytest.raises(
        QueryLimitError,
        match="Product identifier count exceeds the 200-item public query limit",
    ):
        await subject.get_product(PRODUCT_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "amount", "unit"),
    [
        ("milk 1 L", "1000", "ml"),
        ("milk 1000 ml", "1000", "ml"),
        ('אורז 1 ק"ג', "1000", "g"),
        ("אורז 1000 גרם", "1000", "g"),
    ],
)
async def test_product_search_passes_normalized_structured_quantity(
    query: str,
    amount: str,
    unit: str,
) -> None:
    subject, repository = service()

    await subject.search_products(query)

    assert repository.calls[-1][1]["query"] in {"milk", "אורז"}
    assert str(repository.calls[-1][1]["quantity"]) == amount
    assert repository.calls[-1][1]["unit"] == unit


@pytest.mark.asyncio
async def test_product_search_rejects_quantity_without_searchable_product_text() -> None:
    subject, _repository = service()

    with pytest.raises(DomainValidationError, match="empty"):
        await subject.search_products("1000 ml")


@pytest.mark.asyncio
async def test_price_comparison_reuses_bounded_current_price_query() -> None:
    subject, repository = service()

    await subject.compare_product_prices(PRODUCT_ID, limit=10)

    assert repository.calls[-1][0] == "current_prices"
    assert repository.calls[-1][1]["limit"] == 10


@pytest.mark.asyncio
async def test_promotions_default_to_clock_time() -> None:
    subject, repository = service()

    await subject.promotions(product_id=PRODUCT_ID)

    assert repository.calls[-1][0] == "active_promotions"
    assert repository.calls[-1][1]["at"] == datetime(2026, 8, 11, 12, tzinfo=UTC)


@pytest.mark.asyncio
async def test_history_normalizes_offsets_and_allows_larger_page() -> None:
    subject, repository = service()
    since = datetime(2026, 1, 1, tzinfo=UTC)
    until = datetime(2026, 2, 1, tzinfo=UTC)

    await subject.price_history(PRODUCT_ID, since=since, until=until, limit=500)

    call = repository.calls[-1][1]
    assert call["since"] == since
    assert call["until"] == until
    assert call["limit"] == 28


@pytest.mark.asyncio
async def test_history_defaults_to_a_bounded_clock_pinned_window() -> None:
    class AdvancingClock:
        def __init__(self) -> None:
            self.calls = 0

        def now(self) -> datetime:
            self.calls += 1
            return datetime(2026, 8, 11, 12 + self.calls, tzinfo=UTC)

    repository = RecordingQueryRepository()
    clock = AdvancingClock()
    subject = QueryService(repository, clock)

    first = await subject.price_history(PRODUCT_ID)
    assert first.next_cursor is not None
    first_call = repository.calls[-1][1]
    assert first_call["since"] == datetime(2025, 8, 10, 13, tzinfo=UTC)
    assert first_call["until"] == datetime(2026, 8, 11, 13, tzinfo=UTC)

    await subject.price_history(PRODUCT_ID, cursor=first.next_cursor)

    second_call = repository.calls[-1][1]
    assert second_call["since"] == first_call["since"]
    assert second_call["until"] == first_call["until"]
    assert clock.calls == 1


@pytest.mark.asyncio
async def test_history_rejects_one_sided_ranges_before_repository_work() -> None:
    subject, repository = service()
    now = datetime(2026, 8, 11, tzinfo=UTC)

    with pytest.raises(DomainValidationError, match="provided together"):
        await subject.price_history(PRODUCT_ID, since=now)
    with pytest.raises(DomainValidationError, match="provided together"):
        await subject.price_history(PRODUCT_ID, until=now)
    with pytest.raises(DomainValidationError, match="provided together"):
        await subject.promotion_history(since=now)

    assert repository.calls == []


@pytest.mark.asyncio
async def test_promotion_history_uses_the_same_bounded_history_contract() -> None:
    subject, repository = service()
    since = datetime(2026, 1, 1, tzinfo=UTC)
    until = datetime(2026, 2, 1, tzinfo=UTC)

    await subject.promotion_history(since=since, until=until, limit=1_000)

    method, call = repository.calls[-1]
    assert method == "promotion_history"
    assert call["since"] == since
    assert call["until"] == until
    assert call["limit"] == 1


@pytest.mark.asyncio
async def test_public_pages_cap_repository_materialization_before_fetch() -> None:
    subject, repository = service()
    since = datetime(2026, 1, 1, tzinfo=UTC)
    until = datetime(2026, 2, 1, tzinfo=UTC)

    calls: tuple[tuple[Callable[[], Awaitable[object]], str, int], ...] = (
        (lambda: subject.list_retailers(limit=200), "list_retailers", 50),
        (lambda: subject.find_stores(limit=200), "find_stores", 5),
        (lambda: subject.search_products("coffee", limit=200), "search_products", 5),
        (lambda: subject.current_prices(PRODUCT_ID, limit=200), "current_prices", 28),
        (lambda: subject.compare_product_prices(PRODUCT_ID, limit=200), "current_prices", 28),
        (
            lambda: subject.price_history(
                PRODUCT_ID,
                since=since,
                until=until,
                limit=1_000,
            ),
            "price_history",
            28,
        ),
        (lambda: subject.promotions(limit=200), "active_promotions", 1),
        (
            lambda: subject.promotion_history(since=since, until=until, limit=1_000),
            "promotion_history",
            1,
        ),
        (lambda: subject.item_availability(PRODUCT_ID, limit=200), "item_availability", 28),
        (lambda: subject.freshness(limit=200), "freshness", 50),
        (lambda: subject.source_status(limit=200), "source_status", 32),
        (lambda: subject.platform_status(limit=200), "source_status", 32),
    )

    for operation, method, expected_limit in calls:
        await operation()
        matching = [values for name, values in repository.calls if name == method]
        assert matching[-1]["limit"] == expected_limit


@pytest.mark.asyncio
async def test_barcode_lookup_rejects_ambiguous_identifier() -> None:
    subject, _ = service()

    with pytest.raises(DomainValidationError, match="numeric"):
        await subject.find_product_by_barcode("ABC-123")


@pytest.mark.asyncio
async def test_short_text_queries_are_rejected_before_repository_work() -> None:
    subject, repository = service()

    with pytest.raises(QueryLimitError, match="at least 3"):
        await subject.search_products("ab")
    with pytest.raises(QueryLimitError, match="at least 3"):
        await subject.find_stores(query="ab")
    with pytest.raises(QueryLimitError, match="at least 3"):
        await subject.find_stores(city="ab")

    assert repository.calls == []


@pytest.mark.asyncio
async def test_barcode_lookup_bypasses_text_minimum() -> None:
    subject, repository = service()

    await subject.find_product_by_barcode("12345678")

    assert repository.calls == [("find_product_by_barcode", {"barcode": "12345678"})]


@pytest.mark.asyncio
async def test_retailer_item_lookup_preserves_explicit_scope_and_normalizes_code() -> None:
    subject, repository = service()
    retailer_id = UUID("20000000-0000-0000-0000-000000000001")
    portal_id = UUID("30000000-0000-0000-0000-000000000001")

    await subject.find_product_by_retailer_item_code(
        retailer_id,
        "  SKU \uff11\uff12\uff13  ",
        portal_id=portal_id,
    )

    assert repository.calls == [
        (
            "find_product_by_retailer_item_code",
            {
                "retailer_id": retailer_id,
                "portal_id": portal_id,
                "item_code": "SKU123",
            },
        )
    ]


@pytest.mark.asyncio
async def test_retailer_item_lookup_rejects_empty_or_oversized_normalized_code() -> None:
    subject, repository = service()
    retailer_id = UUID("20000000-0000-0000-0000-000000000001")

    with pytest.raises(DomainValidationError, match="empty"):
        await subject.find_product_by_retailer_item_code(retailer_id, "   ")
    with pytest.raises(DomainValidationError, match="128"):
        await subject.find_product_by_retailer_item_code(retailer_id, "x" * 129)

    assert repository.calls == []


@pytest.mark.asyncio
async def test_platform_status_combines_sources_with_maintenance_barrier() -> None:
    subject, repository = service()

    result = await subject.platform_status(limit=7, cursor=None)

    assert result["maintenance"] == {"active": False, "mode": "normal"}
    sources = result["sources"]
    assert isinstance(sources, Page)
    assert sources.items == repository.page.items
    assert sources.next_cursor not in {None, "next"}
    assert {name for name, _ in repository.calls} == {
        "source_status",
        "maintenance_status",
    }


@pytest.mark.asyncio
async def test_query_limit_and_cursor_are_bounded_before_repository_call() -> None:
    subject, repository = service()

    with pytest.raises(QueryLimitError, match="between"):
        await subject.list_retailers(limit=201)
    with pytest.raises(DomainValidationError, match="Cursor"):
        await subject.list_retailers(cursor="bad\nvalue")
    assert repository.calls == []


@pytest.mark.asyncio
async def test_cursor_is_bound_to_route_normalized_filters_and_checksum() -> None:
    subject, repository = service()
    first = await subject.search_products("espresso beans")
    assert first.next_cursor is not None
    calls_after_first = list(repository.calls)

    with pytest.raises(DomainValidationError, match="does not match"):
        await subject.search_products("different beans", cursor=first.next_cursor)
    with pytest.raises(DomainValidationError, match="does not match"):
        await subject.find_stores(query="espresso beans", cursor=first.next_cursor)
    corrupted = first.next_cursor[:-1] + ("0" if first.next_cursor[-1] != "0" else "1")
    with pytest.raises(DomainValidationError, match="does not match"):
        await subject.search_products("espresso beans", cursor=corrupted)

    assert repository.calls == calls_after_first


@pytest.mark.asyncio
async def test_comparison_cursor_cannot_be_reused_for_current_price_route() -> None:
    subject, repository = service()
    first = await subject.compare_product_prices(PRODUCT_ID)
    assert first.next_cursor is not None

    with pytest.raises(DomainValidationError, match="does not match"):
        await subject.current_prices(PRODUCT_ID, cursor=first.next_cursor)

    assert len(repository.calls) == 1


@pytest.mark.asyncio
async def test_default_promotion_instant_is_stable_across_cursor_pages() -> None:
    class AdvancingClock:
        def __init__(self) -> None:
            self.calls = 0

        def now(self) -> datetime:
            self.calls += 1
            return datetime(2026, 8, 11, 12 + self.calls, tzinfo=UTC)

    repository = RecordingQueryRepository()
    clock = AdvancingClock()
    subject = QueryService(repository, clock)

    first = await subject.promotions(product_id=PRODUCT_ID)
    assert first.next_cursor is not None
    first_at = repository.calls[-1][1]["at"]
    await subject.promotions(product_id=PRODUCT_ID, cursor=first.next_cursor)

    assert repository.calls[-1][1]["at"] == first_at
    assert clock.calls == 1


@pytest.mark.asyncio
async def test_history_rejects_reversed_or_excessive_range() -> None:
    subject, _ = service()
    now = datetime(2026, 8, 11, tzinfo=UTC)

    with pytest.raises(DomainValidationError, match="before"):
        await subject.price_history(PRODUCT_ID, since=now, until=now - timedelta(days=1))

    with pytest.raises(DomainValidationError, match="before"):
        await subject.price_history(PRODUCT_ID, since=now, until=now)
    with pytest.raises(DomainValidationError, match="before"):
        await subject.promotion_history(since=now, until=now)

    narrow_limits = QueryLimits(maximum_history_span=timedelta(days=30))
    repository = RecordingQueryRepository()
    bounded = QueryService(repository, FixedClock(), narrow_limits)
    with pytest.raises(QueryLimitError, match="too wide"):
        await bounded.price_history(PRODUCT_ID, since=now - timedelta(days=31), until=now)


def test_query_limits_reject_inconsistent_or_nonpositive_configuration() -> None:
    with pytest.raises(ValueError, match="page limits"):
        QueryLimits(default_page_size=0)
    with pytest.raises(ValueError, match="page limits"):
        QueryLimits(default_page_size=51, maximum_page_size=50)
    with pytest.raises(ValueError, match="History page limit"):
        QueryLimits(maximum_page_size=200, maximum_history_page_size=199)
    with pytest.raises(ValueError, match="history span"):
        QueryLimits(maximum_history_span=timedelta(0))
    with pytest.raises(ValueError, match="Default history span"):
        QueryLimits(default_history_span=timedelta(0))
    with pytest.raises(ValueError, match="text limits"):
        QueryLimits(minimum_query_characters=0)
    with pytest.raises(ValueError, match="text limits"):
        QueryLimits(minimum_query_characters=10, maximum_query_characters=9)


@pytest.mark.asyncio
async def test_checksum_valid_hostile_cursor_envelopes_never_reach_repository() -> None:
    subject, repository = service()
    first = await subject.list_retailers()
    assert first.next_cursor is not None
    prefix, decoded = _decode_cursor_envelope(first.next_cursor)
    assert isinstance(decoded, dict)
    repository.calls.clear()

    without_position = dict(decoded)
    without_position.pop("p")
    hostile_payloads: tuple[object, ...] = (
        [],
        without_position,
        {**decoded, "v": 2},
        {**decoded, "p": ""},
        {**decoded, "p": "x" * 257},
        {**decoded, "p": "bad\nposition"},
        {**decoded, "s": {"unexpected": "value"}},
    )

    for payload in hostile_payloads:
        cursor = _encode_cursor_envelope(prefix, payload)
        with pytest.raises(DomainValidationError, match="does not match"):
            await subject.list_retailers(cursor=cursor)

    assert repository.calls == []


@pytest.mark.asyncio
async def test_checksum_valid_cursor_state_is_strictly_bounded_and_typed() -> None:
    subject, repository = service()
    first = await subject.promotions(product_id=PRODUCT_ID)
    assert first.next_cursor is not None
    prefix, decoded = _decode_cursor_envelope(first.next_cursor)
    assert isinstance(decoded, dict)
    repository.calls.clear()

    hostile_states: tuple[dict[str, object], ...] = (
        {"at": 1},
        {"at": ""},
        {"at": "x" * 65},
        {"at": "bad\nvalue"},
    )
    for state in hostile_states:
        cursor = _encode_cursor_envelope(prefix, {**decoded, "s": state})
        with pytest.raises(DomainValidationError, match="does not match"):
            await subject.promotions(product_id=PRODUCT_ID, cursor=cursor)

    assert repository.calls == []


@pytest.mark.asyncio
async def test_repository_cursor_is_absent_or_rejected_before_public_encoding() -> None:
    subject, repository = service()
    repository.page = Page(items=repository.page.items, next_cursor=None)

    terminal = await subject.list_retailers()

    assert terminal.next_cursor is None
    for invalid_position in ("", "bad\nposition", "x" * 257):
        repository.page = Page(items=repository.page.items, next_cursor=invalid_position)
        with pytest.raises(DomainValidationError, match="Repository returned"):
            await subject.list_retailers()


@pytest.mark.asyncio
async def test_query_times_are_timezone_aware_and_default_range_must_be_representable() -> None:
    subject, repository = service()
    naive = datetime(2026, 8, 11, 12, tzinfo=UTC).replace(tzinfo=None)

    with pytest.raises(DomainValidationError, match="timezone"):
        await subject.promotions(at=naive)

    class EarliestClock:
        def now(self) -> datetime:
            return datetime.min.replace(tzinfo=UTC)

    earliest = QueryService(repository, EarliestClock())
    with pytest.raises(QueryLimitError, match="cannot be represented"):
        await earliest.price_history(PRODUCT_ID)

    assert repository.calls == []
