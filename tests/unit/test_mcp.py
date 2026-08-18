from __future__ import annotations

import io
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

import anyio
import httpx
import pytest
from pydantic import BaseModel, ValidationError

from makolet.adapters.persistence.queries import MAXIMUM_PROMOTION_RELATIONS
from makolet.application.models import Page
from makolet.application.queries import (
    MAXIMUM_MATERIALIZED_AVAILABILITY_RESULTS,
    MAXIMUM_MATERIALIZED_HISTORY_RESULTS,
    MAXIMUM_MATERIALIZED_PRICE_RESULTS,
    MAXIMUM_MATERIALIZED_PRODUCT_RESULTS,
    MAXIMUM_MATERIALIZED_STORES,
    QueryService,
)
from makolet.domain.errors import QueryLimitError
from makolet.interfaces.api import SourceStatusOutput
from makolet.interfaces.mcp import (
    LATEST_PROTOCOL_VERSION,
    MAXIMUM_MESSAGE_BYTES,
    MakoletMcpServer,
    _validation_issues,
    create_mcp_http_app,
    serve_stdio,
)
from tests.fakes.ingestion import FixedClock

PRODUCT_ID = UUID("10000000-0000-0000-0000-000000000001")
RETAILER_ID = UUID("20000000-0000-0000-0000-000000000001")
PORTAL_ID = UUID("30000000-0000-0000-0000-000000000001")
STORE_ID = UUID("40000000-0000-0000-0000-000000000001")
SOURCE_FILE_ID = UUID("50000000-0000-0000-0000-000000000001")
RETAILER_ITEM_ID = UUID("60000000-0000-0000-0000-000000000001")
PRICE_ID = UUID("70000000-0000-0000-0000-000000000001")
OBSERVED_AT = datetime(2026, 8, 11, tzinfo=UTC)
LATER_AT = datetime(2026, 8, 12, tzinfo=UTC)
MAXIMUM_FOUR_BYTE_TEXT = "🛒"


def _product_detail() -> dict[str, object]:
    return {
        "id": PRODUCT_ID,
        "name": "Demo coffee",
        "brand": "Demo brand",
        "manufacturer": None,
        "quantity": Decimal("500.000000"),
        "unit_of_measure": "g",
        "status": "active",
        "created_at": OBSERVED_AT,
        "updated_at": OBSERVED_AT,
        "identifiers": [],
    }


def _barcode_product() -> dict[str, object]:
    return {
        "id": PRODUCT_ID,
        "name": "Demo coffee",
        "brand": "Demo brand",
        "manufacturer": None,
        "quantity": Decimal("500.000000"),
        "unit_of_measure": "g",
        "globally_validated": True,
        "barcode_validated": True,
        "identifier_provenance": [],
        "barcode": "7290000000015",
        "identifier_scope": "global",
    }


def _retailer_item_product() -> dict[str, object]:
    return {
        "id": PRODUCT_ID,
        "name": "Demo coffee",
        "brand": "Demo brand",
        "manufacturer": None,
        "quantity": Decimal("500.000000"),
        "unit_of_measure": "g",
        "retailer_item_id": RETAILER_ITEM_ID,
        "retailer_item_code": "SKU-1",
        "retailer_item_name": "Retailer demo",
        "retailer_item_source_file_id": SOURCE_FILE_ID,
        "retailer_id": RETAILER_ID,
        "retailer_key": "demo-retailer",
        "retailer_name": "Demo retailer",
        "portal_id": PORTAL_ID,
        "portal_key": "demo-portal",
        "match_method": "exact_identifier",
        "match_evidence": {"fixture": True},
    }


def _freshness() -> dict[str, object]:
    return {
        "source_file_id": SOURCE_FILE_ID,
        "source_document_type": "price_full",
        "source_timestamp": OBSERVED_AT,
        "source_discovered_at": OBSERVED_AT,
        "content_sha256": "a" * 64,
        "retailer_id": RETAILER_ID,
        "retailer_key": "demo-retailer",
        "retailer_name": "Demo retailer",
        "portal_id": PORTAL_ID,
        "portal_key": "demo-portal",
        "store_id": PRODUCT_ID,
        "store_name": "Demo store",
        "last_observed_at": OBSERVED_AT,
        "available_items": 1_000,
        "observed_items": 1_000,
        "item_probe_limit": 1_000,
        "items_truncated": True,
    }


def _source_status(truncation_reason: str) -> dict[str, object]:
    status: dict[str, object] = dict.fromkeys(SourceStatusOutput.model_fields)
    status.update(
        {
            "portal_id": PORTAL_ID,
            "portal_key": "demo-portal",
            "family": "fixture",
            "protocol": "fixture",
            "retailer_id": RETAILER_ID,
            "retailer_name": "Demo retailer",
            "collection_attempt_id": SOURCE_FILE_ID,
            "collection_attempt_status": "bounded",
            "collection_operation": "ordinary",
            "collection_generation": 1,
            "collection_archive_only": False,
            "collection_started_at": OBSERVED_AT,
            "collection_finished_at": OBSERVED_AT,
            "collection_discovered_count": 1,
            "collection_processed_count": 0,
            "collection_skipped_unknown_count": 0,
            "collection_warning_count": 0,
            "collection_charged_bytes": 1,
            "collection_truncated": True,
            "collection_truncation_reason": truncation_reason,
        }
    )
    return status


class StubQueries(QueryService):
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.product: dict[str, object] | None = _product_detail()

    def _page(self, method: str, arguments: dict[str, Any]) -> Page:
        self.calls.append((method, arguments))
        return Page((), "next")

    async def search_products(self, query: str, **kwargs: Any) -> Page:
        return self._page("search_products", {"query": query, **kwargs})

    async def get_product(self, product_id: UUID) -> dict[str, object] | None:
        self.calls.append(("get_product", {"product_id": product_id}))
        return self.product

    async def find_product_by_barcode(self, barcode: str) -> dict[str, object] | None:
        self.calls.append(("find_product_by_barcode", {"barcode": barcode}))
        return _barcode_product() if self.product is not None else None

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
        return _retailer_item_product() if self.product is not None else None

    async def list_retailers(self, **kwargs: Any) -> Page:
        return self._page("list_retailers", kwargs)

    async def find_stores(self, **kwargs: Any) -> Page:
        return self._page("find_stores", kwargs)

    async def current_prices(self, product_id: UUID, **kwargs: Any) -> Page:
        return self._page("current_prices", {"product_id": product_id, **kwargs})

    async def compare_product_prices(self, product_id: UUID, **kwargs: Any) -> Page:
        return self._page("compare_product_prices", {"product_id": product_id, **kwargs})

    async def price_history(self, product_id: UUID, **kwargs: Any) -> Page:
        return self._page("price_history", {"product_id": product_id, **kwargs})

    async def promotions(self, **kwargs: Any) -> Page:
        return self._page("promotions", kwargs)

    async def promotion_history(self, **kwargs: Any) -> Page:
        return self._page("promotion_history", kwargs)

    async def item_availability(self, product_id: UUID, **kwargs: Any) -> Page:
        return self._page("item_availability", {"product_id": product_id, **kwargs})

    async def freshness(self, **kwargs: Any) -> Page:
        self.calls.append(("freshness", kwargs))
        return Page((_freshness(),), "next")

    async def source_status(self, **kwargs: Any) -> Page:
        return self._page("source_status", kwargs)

    async def platform_status(self, **kwargs: Any) -> dict[str, object]:
        self.calls.append(("platform_status", kwargs))
        return {
            "maintenance": {"active": False, "mode": "normal"},
            "sources": Page((), "next"),
        }


class CardinalityStatusQueries(StubQueries):
    def __init__(self, truncation_reason: str) -> None:
        super().__init__()
        self._truncation_reason = truncation_reason

    async def platform_status(self, **kwargs: Any) -> dict[str, object]:
        self.calls.append(("platform_status", kwargs))
        return {
            "maintenance": {"active": False, "mode": "normal"},
            "sources": Page((_source_status(self._truncation_reason),), None),
        }


class BoundedQueryRepository:
    def __init__(self) -> None:
        self.barcode_calls: list[str] = []

    async def find_product_by_barcode(self, barcode: str) -> dict[str, object] | None:
        self.barcode_calls.append(barcode)
        return None

    async def search_products(self, *args: object, **kwargs: object) -> Page:
        del args, kwargs
        raise AssertionError("short product query reached repository")

    async def find_stores(self, *args: object, **kwargs: object) -> Page:
        del args, kwargs
        raise AssertionError("short store query reached repository")


class HistoryQueryRepository(BoundedQueryRepository):
    def __init__(self) -> None:
        super().__init__()
        self.history_calls: list[dict[str, Any]] = []

    async def price_history(self, product_id: UUID, **kwargs: Any) -> Page:
        self.history_calls.append({"product_id": product_id, **kwargs})
        return Page((), "repository-next")


class MalformedOutputQueries(StubQueries):
    async def list_retailers(self, **kwargs: Any) -> Page:
        del kwargs
        return Page(({"credential": "sentinel-mcp-token"},), None)


class OversizedOutputQueries(StubQueries):
    async def get_product(self, product_id: UUID) -> dict[str, object] | None:
        product = _product_detail()
        product["id"] = product_id
        product["name"] = "x" * MAXIMUM_MESSAGE_BYTES
        return product


def _maximum_store() -> dict[str, object]:
    text = MAXIMUM_FOUR_BYTE_TEXT
    return {
        "id": STORE_ID,
        "retailer_id": RETAILER_ID,
        "retailer_name": text * 512,
        "portal_id": PORTAL_ID,
        "portal_key": text * 128,
        "chain_code": text * 128,
        "subchain_code": text * 128,
        "source_store_code": text * 128,
        "name": text * 1_024,
        "address": text * 10_000,
        "city": text * 10_000,
        "postal_code": text * 32,
        "is_active": True,
        "first_seen_at": OBSERVED_AT,
        "last_seen_at": OBSERVED_AT,
        "last_source_file_id": SOURCE_FILE_ID,
    }


def _maximum_search_product() -> dict[str, object]:
    text = MAXIMUM_FOUR_BYTE_TEXT
    return {
        "id": PRODUCT_ID,
        "name": text * 2_048,
        "brand": text * 10_000,
        "manufacturer": text * 10_000,
        "quantity": Decimal("1000000.000000"),
        "unit_of_measure": text * 64,
        "rank": Decimal("9.9999999"),
    }


def _maximum_price() -> dict[str, object]:
    text = MAXIMUM_FOUR_BYTE_TEXT
    return {
        "id": PRICE_ID,
        "item_price": Decimal("9999999999.9999"),
        "unit_of_measure_price": Decimal("9999999999.9999"),
        "allow_discount": True,
        "source_updated_at": OBSERVED_AT,
        "first_observed_at": OBSERVED_AT,
        "last_observed_at": OBSERVED_AT,
        "source_file_id": SOURCE_FILE_ID,
        "source_document_type": "price_full",
        "source_timestamp": OBSERVED_AT,
        "source_discovered_at": OBSERVED_AT,
        "content_sha256": "f" * 64,
        "is_available": True,
        "item_status": 2_147_483_647,
        "availability_source_file_id": SOURCE_FILE_ID,
        "retailer_item_id": RETAILER_ITEM_ID,
        "source_item_code": text * 128,
        "retailer_item_name": text * 2_048,
        "portal_id": PORTAL_ID,
        "portal_key": text * 128,
        "store_id": STORE_ID,
        "store_name": text * 1_024,
        "retailer_id": RETAILER_ID,
        "retailer_key": text * 128,
        "retailer_name": text * 512,
    }


def _maximum_price_history() -> dict[str, object]:
    row = _maximum_price()
    row.pop("first_observed_at")
    row.pop("last_observed_at")
    row.pop("is_available")
    row.pop("item_status")
    row.pop("availability_source_file_id")
    row["valid_from"] = OBSERVED_AT
    row["valid_to"] = LATER_AT
    return row


def _maximum_availability() -> dict[str, object]:
    text = MAXIMUM_FOUR_BYTE_TEXT
    return {
        "id": PRICE_ID,
        "is_available": True,
        "item_status": 2_147_483_647,
        "last_observed_at": OBSERVED_AT,
        "retailer_item_id": RETAILER_ITEM_ID,
        "source_item_code": text * 128,
        "retailer_item_name": text * 2_048,
        "portal_id": PORTAL_ID,
        "portal_key": text * 128,
        "retailer_id": RETAILER_ID,
        "retailer_key": text * 128,
        "retailer_name": text * 512,
        "store_id": STORE_ID,
        "store_name": text * 1_024,
        "source_file_id": SOURCE_FILE_ID,
        "source_document_type": "price_full",
        "source_timestamp": OBSERVED_AT,
        "source_discovered_at": OBSERVED_AT,
        "content_sha256": "f" * 64,
    }


def _maximum_promotion() -> dict[str, object]:
    text = MAXIMUM_FOUR_BYTE_TEXT
    relation_items = tuple(
        {
            "retailer_item_id": RETAILER_ITEM_ID,
            "source_item_code": text * 128,
            "name": text * 2_048,
            "item_type": 2_147_483_647,
            "is_gift": True,
            "canonical_product_id": PRODUCT_ID,
        }
        for _ in range(MAXIMUM_PROMOTION_RELATIONS)
    )
    relation_stores = tuple(
        {
            "store_id": STORE_ID,
            "source_store_code": text * 128,
            "name": text * 1_024,
            "city": text * 10_000,
        }
        for _ in range(MAXIMUM_PROMOTION_RELATIONS)
    )
    return {
        "id": PRODUCT_ID,
        "retailer_id": RETAILER_ID,
        "retailer_key": text * 128,
        "retailer_name": text * 512,
        "portal_id": PORTAL_ID,
        "portal_key": text * 128,
        "subchain_code": text * 128,
        "source_promotion_id": text * 256,
        "source_scope_store_code": text * 128,
        "description": text * 10_000,
        "discount_kind": "quantity",
        "starts_at": OBSERVED_AT,
        "ends_at": OBSERVED_AT,
        "reward_type": 2_147_483_647,
        "allows_multiple_discounts": True,
        "minimum_quantity": Decimal("1000000.000000"),
        "maximum_quantity": Decimal("1000000.000000"),
        "discount_rate": Decimal("100.000000"),
        "minimum_purchase": Decimal("9999999999.9999"),
        "discounted_price": Decimal("9999999999.9999"),
        "discounted_unit_price": Decimal("9999999999.9999"),
        "minimum_items_offered": 1_000_000,
        "additional_restrictions": text * 10_000,
        "remarks": text * 10_000,
        "is_active": True,
        "valid_from": OBSERVED_AT,
        "valid_to": LATER_AT,
        "last_observed_at": OBSERVED_AT,
        "source_file_id": SOURCE_FILE_ID,
        "source_document_type": "promotion_full",
        "source_timestamp": OBSERVED_AT,
        "source_discovered_at": OBSERVED_AT,
        "content_sha256": "f" * 64,
        "items": relation_items,
        "returned_item_count": MAXIMUM_PROMOTION_RELATIONS,
        "items_truncated": True,
        "stores": relation_stores,
        "returned_store_count": MAXIMUM_PROMOTION_RELATIONS,
        "stores_truncated": True,
        "clubs": (text * 128,) * MAXIMUM_PROMOTION_RELATIONS,
        "returned_club_count": MAXIMUM_PROMOTION_RELATIONS,
        "clubs_truncated": True,
    }


class MaximumPublisherPageRepository:
    @staticmethod
    def _page(row: dict[str, object], limit: int) -> Page:
        return Page(tuple(dict(row) for _ in range(limit)), "repository-next")

    async def find_stores(self, **kwargs: Any) -> Page:
        return self._page(_maximum_store(), kwargs["limit"])

    async def search_products(self, _query: str, **kwargs: Any) -> Page:
        return self._page(_maximum_search_product(), kwargs["limit"])

    async def current_prices(self, _product_id: UUID, **kwargs: Any) -> Page:
        return self._page(_maximum_price(), kwargs["limit"])

    async def price_history(self, _product_id: UUID, **kwargs: Any) -> Page:
        return self._page(_maximum_price_history(), kwargs["limit"])

    async def active_promotions(self, **kwargs: Any) -> Page:
        return self._page(_maximum_promotion(), kwargs["limit"])

    async def promotion_history(self, **kwargs: Any) -> Page:
        return self._page(_maximum_promotion(), kwargs["limit"])

    async def item_availability(self, _product_id: UUID, **kwargs: Any) -> Page:
        return self._page(_maximum_availability(), kwargs["limit"])


def _meta() -> dict[str, object]:
    return {
        "io.modelcontextprotocol/protocolVersion": LATEST_PROTOCOL_VERSION,
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
    }


def _request(
    method: str,
    params: dict[str, object] | None = None,
    *,
    request_id: int = 1,
) -> dict[str, object]:
    merged = {"_meta": _meta(), **(params or {})}
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": merged}


def _headers(method: str, *, name: str | None = None) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": LATEST_PROTOCOL_VERSION,
        "Mcp-Method": method,
    }
    if name is not None:
        headers["Mcp-Name"] = name
    return headers


@pytest.mark.asyncio
async def test_modern_discovery_and_tool_order_are_deterministic() -> None:
    server = MakoletMcpServer(StubQueries())

    discovery = await server.handle(_request("server/discover"))
    listing = await server.handle(_request("tools/list", request_id=2))

    assert discovery is not None
    assert discovery["result"]["resultType"] == "complete"
    assert discovery["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"] == "makolet"
    assert "serverInfo" not in discovery["result"]
    assert listing is not None
    names = [tool["name"] for tool in listing["result"]["tools"]]
    assert names == [
        "search_products",
        "get_product",
        "find_product_by_barcode",
        "find_product_by_retailer_item_code",
        "list_retailers",
        "find_stores",
        "get_current_prices",
        "compare_product_prices",
        "get_price_history",
        "get_active_promotions",
        "get_promotion_history",
        "get_item_availability",
        "get_data_freshness",
        "get_source_status",
    ]
    assert not any("sql" in name.casefold() for name in names)
    assert all(tool["annotations"]["readOnlyHint"] for tool in listing["result"]["tools"])
    assert listing["result"]["ttlMs"] == 300_000
    assert listing["result"]["cacheScope"] == "public"
    retailer_item_tool = next(
        tool
        for tool in listing["result"]["tools"]
        if tool["name"] == "find_product_by_retailer_item_code"
    )
    assert "portal_id" in retailer_item_tool["inputSchema"]["properties"]
    assert "portal_id" not in retailer_item_tool["inputSchema"]["required"]
    tools = {tool["name"]: tool for tool in listing["result"]["tools"]}
    expected_success_models = {
        "search_products": "_SearchProductOutput",
        "get_product": "ProductDetailResponse",
        "find_product_by_barcode": "BarcodeProductResponse",
        "find_product_by_retailer_item_code": "RetailerItemProductResponse",
        "list_retailers": "_RetailerPageOutput",
        "find_stores": "_StorePageOutput",
        "get_current_prices": "_PricePageOutput",
        "compare_product_prices": "_PricePageOutput",
        "get_price_history": "_PriceHistoryPageOutput",
        "get_active_promotions": "_PromotionPageOutput",
        "get_promotion_history": "_PromotionHistoryPageOutput",
        "get_item_availability": "_AvailabilityPageOutput",
        "get_data_freshness": "_FreshnessPageOutput",
        "get_source_status": "_SourceStatusToolOutput",
    }
    for name, model_name in expected_success_models.items():
        output_schema = tools[name]["outputSchema"]
        assert output_schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert output_schema["type"] == "object"
        assert output_schema["anyOf"][0] == {"$ref": f"#/$defs/{model_name}"}
        assert output_schema["anyOf"][1] == {"$ref": "#/$defs/_ToolErrorOutput"}
        assert output_schema["$defs"][model_name]["additionalProperties"] is False
        assert not _contains_open_object_schema(output_schema)
    assert (
        tools["search_products"]["outputSchema"]["$defs"]["ProductSearchOutput"]["properties"][
            "id"
        ]["format"]
        == "uuid"
    )
    assert (
        tools["get_current_prices"]["outputSchema"]["$defs"]["PriceOutput"]["properties"][
            "item_price"
        ]["type"]
        == "string"
    )
    promotion_schema = tools["get_active_promotions"]["outputSchema"]["$defs"]["PromotionOutput"]
    assert promotion_schema["properties"]["items"]["maxItems"] == 200
    assert promotion_schema["properties"]["stores"]["maxItems"] == 200
    assert promotion_schema["properties"]["clubs"]["maxItems"] == 200
    source_schema = tools["get_source_status"]["outputSchema"]["$defs"]["SourceStatusOutput"]
    assert "collection_attempt_status" in source_schema["properties"]
    assert "collection_charged_bytes" in source_schema["properties"]
    assert "collection_truncation_reason" in source_schema["properties"]
    freshness_schema = tools["get_data_freshness"]["outputSchema"]["$defs"]["FreshnessOutput"]
    assert "content_sha256" in freshness_schema["properties"]
    assert freshness_schema["properties"]["observed_items"]["maximum"] == 1_000
    assert freshness_schema["properties"]["available_items"]["maximum"] == 1_000
    assert freshness_schema["properties"]["item_probe_limit"]["const"] == 1_000
    assert freshness_schema["properties"]["items_truncated"]["type"] == "boolean"


@pytest.mark.asyncio
async def test_all_read_only_tools_execute_with_bounded_arguments() -> None:
    queries = StubQueries()
    server = MakoletMcpServer(queries)
    calls: tuple[tuple[str, dict[str, object]], ...] = (
        ("search_products", {"query": "קפה"}),
        ("get_product", {"product_id": str(PRODUCT_ID)}),
        ("find_product_by_barcode", {"barcode": "7290000000015"}),
        (
            "find_product_by_retailer_item_code",
            {
                "retailer_id": str(PRODUCT_ID),
                "portal_id": str(UUID("20000000-0000-0000-0000-000000000001")),
                "item_code": "SKU-1",
            },
        ),
        ("list_retailers", {}),
        ("find_stores", {"city": "ירושלים"}),
        ("get_current_prices", {"product_id": str(PRODUCT_ID)}),
        ("compare_product_prices", {"product_id": str(PRODUCT_ID)}),
        (
            "get_price_history",
            {
                "product_id": str(PRODUCT_ID),
                "since": "2026-08-01T00:00:00Z",
                "until": "2026-08-11T00:00:00Z",
            },
        ),
        ("get_active_promotions", {}),
        ("get_promotion_history", {}),
        ("get_item_availability", {"product_id": str(PRODUCT_ID)}),
        ("get_data_freshness", {}),
        ("get_source_status", {}),
    )

    for index, (name, arguments) in enumerate(calls, start=1):
        response = await server.handle(
            _request(
                "tools/call",
                {"name": name, "arguments": arguments},
                request_id=index,
            )
        )
        assert response is not None
        assert response["result"]["resultType"] == "complete"
        assert response["result"]["isError"] is False
        assert "structuredContent" in response["result"]

    assert len(queries.calls) == len(calls)


@pytest.mark.parametrize(
    ("name", "arguments", "expected_count"),
    [
        ("find_stores", {"limit": 200}, MAXIMUM_MATERIALIZED_STORES),
        (
            "search_products",
            {"query": "coffee", "limit": 200},
            MAXIMUM_MATERIALIZED_PRODUCT_RESULTS,
        ),
        (
            "get_current_prices",
            {"product_id": str(PRODUCT_ID), "limit": 200},
            MAXIMUM_MATERIALIZED_PRICE_RESULTS,
        ),
        (
            "compare_product_prices",
            {"product_id": str(PRODUCT_ID), "limit": 200},
            MAXIMUM_MATERIALIZED_PRICE_RESULTS,
        ),
        (
            "get_price_history",
            {"product_id": str(PRODUCT_ID), "limit": 1_000},
            MAXIMUM_MATERIALIZED_HISTORY_RESULTS,
        ),
        ("get_active_promotions", {"limit": 200}, 1),
        ("get_promotion_history", {"limit": 1_000}, 1),
        (
            "get_item_availability",
            {"product_id": str(PRODUCT_ID), "limit": 200},
            MAXIMUM_MATERIALIZED_AVAILABILITY_RESULTS,
        ),
    ],
)
@pytest.mark.asyncio
async def test_maximum_four_byte_publisher_page_fits_mcp_result_budget(
    name: str,
    arguments: dict[str, object],
    expected_count: int,
) -> None:
    repository = MaximumPublisherPageRepository()
    server = MakoletMcpServer(QueryService(cast(Any, repository), FixedClock()))

    response = await server.handle(_request("tools/call", {"name": name, "arguments": arguments}))

    assert response is not None
    result = response["result"]
    assert result["isError"] is False
    assert len(result["structuredContent"]["items"]) == expected_count
    assert result["structuredContent"]["nextCursor"] is not None
    tool_result = {key: result[key] for key in ("content", "structuredContent", "isError")}
    tool_result_bytes = len(
        json.dumps(tool_result, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )
    response_bytes = len(
        json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )
    assert tool_result_bytes <= MAXIMUM_MESSAGE_BYTES - 4_096
    assert response_bytes < MAXIMUM_MESSAGE_BYTES


@pytest.mark.asyncio
async def test_freshness_tool_preserves_the_bounded_probe_contract() -> None:
    server = MakoletMcpServer(StubQueries())

    response = await server.handle(
        _request("tools/call", {"name": "get_data_freshness", "arguments": {}})
    )

    assert response is not None
    item = response["result"]["structuredContent"]["items"][0]
    assert item["available_items"] == item["observed_items"] == 1_000
    assert item["item_probe_limit"] == 1_000
    assert item["items_truncated"] is True


@pytest.mark.parametrize(
    "truncation_reason",
    ["identity_day_limit", "attempt_day_limit", "success_day_limit"],
)
@pytest.mark.asyncio
async def test_source_status_tool_serializes_cardinality_truncation_reasons(
    truncation_reason: str,
) -> None:
    server = MakoletMcpServer(CardinalityStatusQueries(truncation_reason))

    response = await server.handle(
        _request("tools/call", {"name": "get_source_status", "arguments": {"limit": 1}})
    )

    assert response is not None
    assert response["result"]["isError"] is False
    item = response["result"]["structuredContent"]["items"][0]
    assert item["collection_truncation_reason"] == truncation_reason


@pytest.mark.asyncio
async def test_tool_output_serializes_typed_values_after_validation() -> None:
    server = MakoletMcpServer(StubQueries())

    response = await server.handle(
        _request(
            "tools/call",
            {"name": "get_product", "arguments": {"product_id": str(PRODUCT_ID)}},
        )
    )

    assert response is not None
    data = response["result"]["structuredContent"]["data"]
    assert data["id"] == str(PRODUCT_ID)
    assert data["quantity"] == "500.000000"
    assert data["updated_at"] == "2026-08-11T00:00:00Z"


@pytest.mark.asyncio
async def test_product_identifier_overflow_returns_stable_tool_error() -> None:
    class IdentifierLimitQueries(StubQueries):
        async def get_product(self, product_id: UUID) -> dict[str, object] | None:
            del product_id
            raise QueryLimitError(
                "Product identifier count exceeds the 200-item public query limit"
            )

    server = MakoletMcpServer(IdentifierLimitQueries())

    response = await server.handle(
        _request(
            "tools/call",
            {"name": "get_product", "arguments": {"product_id": str(PRODUCT_ID)}},
        )
    )

    assert response is not None
    assert response["result"]["isError"] is True
    assert response["result"]["structuredContent"] == {
        "error": {
            "code": "query_limit_exceeded",
            "message": "Product identifier count exceeds the 200-item public query limit",
        }
    }


@pytest.mark.asyncio
async def test_malformed_service_output_becomes_secret_safe_internal_tool_error() -> None:
    server = MakoletMcpServer(MalformedOutputQueries())

    response = await server.handle(
        _request("tools/call", {"name": "list_retailers", "arguments": {}})
    )

    assert response is not None
    result = response["result"]
    assert result["isError"] is True
    assert result["structuredContent"] == {
        "error": {"code": "internal_error", "message": "Tool execution failed"}
    }
    assert "sentinel-mcp-token" not in json.dumps(result)


@pytest.mark.asyncio
async def test_duplicated_tool_output_is_replaced_before_exceeding_response_budget() -> None:
    server = MakoletMcpServer(OversizedOutputQueries())

    response = await server.handle(
        _request(
            "tools/call",
            {"name": "get_product", "arguments": {"product_id": str(PRODUCT_ID)}},
        )
    )

    assert response is not None
    encoded = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode()
    assert len(encoded) <= MAXIMUM_MESSAGE_BYTES
    assert response["result"]["isError"] is True
    assert response["result"]["structuredContent"]["error"]["code"] == ("query_limit_exceeded")
    assert "x" * 1_000 not in encoded.decode()


@pytest.mark.asyncio
async def test_argument_failures_are_protocol_errors_and_domain_misses_are_tool_errors() -> None:
    queries = StubQueries()
    server = MakoletMcpServer(queries)

    invalid = await server.handle(
        _request(
            "tools/call",
            {
                "name": "search_products",
                "arguments": {"query": "coffee", "limit": 201, "extra": True},
            },
        )
    )
    queries.product = None
    missing = await server.handle(
        _request(
            "tools/call",
            {"name": "get_product", "arguments": {"product_id": str(PRODUCT_ID)}},
            request_id=2,
        )
    )

    assert invalid is not None
    assert invalid["error"]["code"] == -32602
    assert queries.calls == [("get_product", {"product_id": PRODUCT_ID})]
    assert missing is not None
    assert missing["result"]["isError"] is True
    assert missing["result"]["structuredContent"]["error"]["code"] == "not_found"


@pytest.mark.asyncio
async def test_unknown_tool_argument_flood_returns_one_bounded_protocol_issue() -> None:
    server = MakoletMcpServer(StubQueries())
    arguments = {f"unknown_{index}": 0 for index in range(20_000)}
    payload = json.dumps(
        _request(
            "tools/call",
            {"name": "list_retailers", "arguments": arguments},
        ),
        separators=(",", ":"),
    ).encode()

    assert len(payload) < MAXIMUM_MESSAGE_BYTES
    response = await server.parse_and_handle(payload)

    assert response is not None
    assert response["id"] == 1
    assert response["error"]["code"] == -32602
    assert response["error"]["data"] == {
        "issues": [
            {
                "location": ["arguments"],
                "message": "Tool arguments contain unknown fields",
            }
        ]
    }


@pytest.mark.asyncio
async def test_known_tool_argument_failures_keep_typed_bounded_diagnostics() -> None:
    server = MakoletMcpServer(StubQueries())

    response = await server.handle(
        _request(
            "tools/call",
            {
                "name": "search_products",
                "arguments": {"query": "", "limit": 201},
            },
        )
    )

    assert response is not None
    assert response["error"]["code"] == -32602
    issues = response["error"]["data"]["issues"]
    assert len(issues) == 2
    assert {tuple(issue["location"]) for issue in issues} == {("query",), ("limit",)}
    assert all(set(issue) == {"location", "message"} for issue in issues)


def test_validation_issue_cap_precedes_diagnostic_materialization() -> None:
    class ManyRequiredFields(BaseModel):
        field_00: int
        field_01: int
        field_02: int
        field_03: int
        field_04: int
        field_05: int
        field_06: int
        field_07: int
        field_08: int
        field_09: int
        field_10: int
        field_11: int
        field_12: int
        field_13: int
        field_14: int
        field_15: int
        field_16: int

    with pytest.raises(ValidationError) as caught:
        ManyRequiredFields.model_validate({})

    assert _validation_issues(caught.value) == [
        {
            "location": ["arguments"],
            "message": "Tool arguments produced too many validation issues",
        }
    ]


@pytest.mark.asyncio
async def test_json_rpc_envelope_rejects_invalid_shapes_before_dispatch() -> None:
    queries = StubQueries()
    server = MakoletMcpServer(queries)
    cases: tuple[tuple[object, int, int | None], ...] = (
        ([], -32600, None),
        ({"jsonrpc": "2.0", "id": True, "method": "ping"}, -32600, None),
        ({"jsonrpc": "1.0", "id": 2, "method": "ping"}, -32600, 2),
        (
            {"jsonrpc": "2.0", "id": 3, "method": "ping", "params": []},
            -32602,
            3,
        ),
    )

    for message, expected_code, expected_id in cases:
        response = await server.handle(message)
        assert response is not None
        assert response["error"]["code"] == expected_code
        assert response.get("id") == expected_id

    notification = await server.handle({"jsonrpc": "2.0", "method": "ping", "params": {}})
    assert notification is None
    assert queries.calls == []


@pytest.mark.asyncio
async def test_protocol_rejects_malformed_tool_selectors_before_execution() -> None:
    queries = StubQueries()
    server = MakoletMcpServer(queries)

    responses = (
        await server.handle(_request("tools/list", {"cursor": "unexpected"})),
        await server.handle(_request("tools/call", {"name": "unknown", "arguments": {}})),
        await server.handle(_request("tools/call", {"name": "list_retailers", "arguments": []})),
    )

    assert all(response is not None for response in responses)
    assert [response["error"]["code"] for response in responses if response is not None] == [
        -32602,
        -32602,
        -32602,
    ]
    assert queries.calls == []


@pytest.mark.asyncio
async def test_unexpected_tool_exception_is_redacted_from_protocol_result() -> None:
    class ExplodingQueries(StubQueries):
        async def list_retailers(self, **kwargs: Any) -> Page:
            del kwargs
            raise RuntimeError("sentinel-tool-exception-secret")

    response = await MakoletMcpServer(ExplodingQueries()).handle(
        _request("tools/call", {"name": "list_retailers", "arguments": {}})
    )

    assert response is not None
    result = response["result"]
    assert result["structuredContent"] == {
        "error": {"code": "internal_error", "message": "Tool execution failed"}
    }
    assert "sentinel-tool-exception-secret" not in json.dumps(result)


@pytest.mark.asyncio
async def test_json_preflight_rejects_invalid_utf8_and_unbalanced_documents() -> None:
    server = MakoletMcpServer(StubQueries())
    malformed_payloads = (
        b"\xff",
        b"}",
        b'{"jsonrpc":"2.0","unterminated":"value\\',
    )

    for payload in malformed_payloads:
        response = await server.parse_and_handle(payload)
        assert response is not None
        assert response["error"] == {"code": -32700, "message": "Invalid JSON"}

    oversized = await server.parse_and_handle(b"x" * (MAXIMUM_MESSAGE_BYTES + 1))
    assert oversized is not None
    assert oversized["error"] == {
        "code": -32600,
        "message": "Request exceeds the message size limit",
    }


def test_http_app_rejects_unsafe_timeout_and_concurrency_configuration() -> None:
    server = MakoletMcpServer(StubQueries())

    with pytest.raises(ValueError, match="body timeout"):
        create_mcp_http_app(server, body_timeout_seconds=0)
    with pytest.raises(ValueError, match="body timeout"):
        create_mcp_http_app(server, body_timeout_seconds=301)
    with pytest.raises(ValueError, match="concurrency limit"):
        create_mcp_http_app(server, maximum_concurrency=0)
    with pytest.raises(ValueError, match="concurrency limit"):
        create_mcp_http_app(server, maximum_concurrency=10_001)


@pytest.mark.asyncio
async def test_short_text_tools_return_stable_errors_without_blocking_barcode_lookup() -> None:
    repository = BoundedQueryRepository()
    service = QueryService(cast(Any, repository), FixedClock())
    server = MakoletMcpServer(service)

    short_calls = (
        ("search_products", {"query": "ab"}),
        ("find_stores", {"query": "ab"}),
        ("find_stores", {"city": "ab"}),
    )
    for request_id, (name, arguments) in enumerate(short_calls, start=1):
        response = await server.handle(
            _request(
                "tools/call",
                {"name": name, "arguments": arguments},
                request_id=request_id,
            )
        )
        assert response is not None
        assert response["result"]["isError"] is True
        assert response["result"]["structuredContent"]["error"]["code"] == "query_limit_exceeded"

    barcode = await server.handle(
        _request(
            "tools/call",
            {"name": "find_product_by_barcode", "arguments": {"barcode": "12345678"}},
            request_id=10,
        )
    )
    assert barcode is not None
    assert barcode["result"]["structuredContent"]["error"]["code"] == "not_found"
    assert repository.barcode_calls == ["12345678"]


@pytest.mark.asyncio
async def test_history_tools_enforce_paired_and_cursor_pinned_windows() -> None:
    class CountingClock:
        def __init__(self) -> None:
            self.calls = 0

        def now(self) -> datetime:
            self.calls += 1
            return datetime(2026, 8, 11, 12 + self.calls, tzinfo=UTC)

    repository = HistoryQueryRepository()
    clock = CountingClock()
    server = MakoletMcpServer(QueryService(cast(Any, repository), clock))

    one_sided_calls = (
        (
            "get_price_history",
            {"product_id": str(PRODUCT_ID), "since": "2026-08-01T00:00:00Z"},
        ),
        ("get_promotion_history", {"until": "2026-08-11T00:00:00Z"}),
    )
    for request_id, (name, arguments) in enumerate(one_sided_calls, start=1):
        response = await server.handle(
            _request(
                "tools/call",
                {"name": name, "arguments": arguments},
                request_id=request_id,
            )
        )
        assert response is not None
        result = response["result"]
        assert result["isError"] is True
        assert result["structuredContent"] == {
            "error": {
                "code": "domain_validation_error",
                "message": "since and until must be provided together",
            }
        }

    first = await server.handle(
        _request(
            "tools/call",
            {"name": "get_price_history", "arguments": {"product_id": str(PRODUCT_ID)}},
            request_id=10,
        )
    )
    assert first is not None
    first_result = first["result"]
    assert first_result["isError"] is False
    cursor = first_result["structuredContent"]["nextCursor"]
    assert isinstance(cursor, str)

    second = await server.handle(
        _request(
            "tools/call",
            {
                "name": "get_price_history",
                "arguments": {"product_id": str(PRODUCT_ID), "cursor": cursor},
            },
            request_id=11,
        )
    )
    assert second is not None
    assert second["result"]["isError"] is False
    assert len(repository.history_calls) == 2
    assert repository.history_calls[0]["since"] == repository.history_calls[1]["since"]
    assert repository.history_calls[0]["until"] == repository.history_calls[1]["until"]
    assert clock.calls == 1


@pytest.mark.asyncio
async def test_legacy_negotiation_and_invalid_json_remain_compatible() -> None:
    server = MakoletMcpServer(StubQueries())

    initialized = await server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2025-11-25"},
        }
    )
    listing = await server.handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    invalid = await server.parse_and_handle(b"not json")

    assert initialized is not None
    assert initialized["result"]["protocolVersion"] == "2025-11-25"
    assert listing is not None
    assert "resultType" not in listing["result"]
    assert invalid is not None
    assert invalid["error"]["code"] == -32700


@pytest.mark.asyncio
async def test_json_decoder_bombs_are_bounded_without_terminating_stdio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = MakoletMcpServer(StubQueries())
    deeply_nested = b"[" * 1_100 + b"0" + b"]" * 1_100
    oversized_integer = b'{"jsonrpc":"2.0","id":' + b"9" * 5_000 + b',"method":"ping","params":{}}'
    oversized_string = b'{"jsonrpc":"' + b"x" * (64 * 1024 + 1) + b'"}'
    excessive_structure = b"[" + b"0," * 100_000 + b"0]"

    def must_not_decode(_value: object) -> object:
        raise AssertionError("json.loads must not receive structurally unbounded input")

    with monkeypatch.context() as patcher:
        patcher.setattr("makolet.interfaces.mcp.json.loads", must_not_decode)
        nested_response = await server.parse_and_handle(deeply_nested)
        integer_response = await server.parse_and_handle(oversized_integer)
        string_response = await server.parse_and_handle(oversized_string)
        structure_response = await server.parse_and_handle(excessive_structure)

    assert nested_response is not None
    assert nested_response["error"] == {"code": -32700, "message": "Invalid JSON"}
    assert integer_response is not None
    assert integer_response["error"] == {"code": -32700, "message": "Invalid JSON"}
    assert string_response is not None
    assert string_response["error"] == {"code": -32700, "message": "Invalid JSON"}
    assert structure_response is not None
    assert structure_response["error"] == {"code": -32700, "message": "Invalid JSON"}

    valid_request = json.dumps(
        {"jsonrpc": "2.0", "id": "after-bomb", "method": "ping", "params": {}}
    ).encode()
    input_stream = io.BytesIO(oversized_integer + b"\n" + valid_request + b"\n")
    output_stream = io.BytesIO()

    await serve_stdio(server, reader=input_stream, writer=output_stream)

    responses = [json.loads(line) for line in output_stream.getvalue().splitlines()]
    assert responses[0]["error"] == {"code": -32700, "message": "Invalid JSON"}
    assert responses[1] == {
        "jsonrpc": "2.0",
        "id": "after-bomb",
        "result": {},
    }


@pytest.mark.asyncio
async def test_http_json_decoder_bomb_returns_protocol_error() -> None:
    app = create_mcp_http_app(MakoletMcpServer(StubQueries()))
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    oversized_integer = b'{"jsonrpc":"2.0","id":' + b"9" * 5_000 + b',"method":"ping","params":{}}'

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/mcp",
            content=oversized_integer,
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            },
        )

    assert response.status_code == 200
    assert response.json()["error"] == {"code": -32700, "message": "Invalid JSON"}


@pytest.mark.asyncio
async def test_http_rejects_declared_oversize_before_reading_the_body() -> None:
    app = create_mcp_http_app(MakoletMcpServer(StubQueries()))
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/mcp",
            content=b"{}",
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
                "Content-Length": str(MAXIMUM_MESSAGE_BYTES + 1),
            },
        )

    assert response.status_code == 413


@pytest.mark.asyncio
async def test_http_rejects_chunked_oversize_and_invalid_encoded_tool_name() -> None:
    queries = StubQueries()
    app = create_mcp_http_app(MakoletMcpServer(queries))
    transport = httpx.ASGITransport(app=app)

    async def oversized_body() -> AsyncIterator[bytes]:
        yield b"x" * (MAXIMUM_MESSAGE_BYTES + 1)

    payload = _request("tools/call", {"name": "list_retailers", "arguments": {}})
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        oversized = await client.post(
            "/mcp",
            content=oversized_body(),
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            },
        )
        invalid_name = await client.post(
            "/mcp",
            json=payload,
            headers=_headers("tools/call", name="=?base64?%%%?="),
        )

    assert oversized.status_code == 413
    assert invalid_name.status_code == 400
    assert invalid_name.json()["error"]["code"] == -32020
    assert queries.calls == []


@pytest.mark.asyncio
async def test_http_applies_one_total_deadline_to_a_trickled_body() -> None:
    async def slow_body() -> AsyncIterator[bytes]:
        yield b"{"
        await anyio.sleep(0.05)
        yield b"}"

    app = create_mcp_http_app(
        MakoletMcpServer(StubQueries()),
        body_timeout_seconds=0.01,
    )
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/mcp",
            content=slow_body(),
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            },
        )

    assert response.status_code == 408


def test_http_app_publishes_its_bounded_uvicorn_concurrency() -> None:
    app = create_mcp_http_app(
        MakoletMcpServer(StubQueries()),
        maximum_concurrency=37,
    )

    assert app.state.uvicorn_limit_concurrency == 37


@pytest.mark.asyncio
async def test_stdio_uses_utf8_json_lines_without_log_pollution() -> None:
    server = MakoletMcpServer(StubQueries())
    input_stream = io.BytesIO(
        json.dumps({"jsonrpc": "2.0", "id": "ping-1", "method": "ping", "params": {}}).encode()
        + b"\n"
    )
    output_stream = io.BytesIO()

    await serve_stdio(server, reader=input_stream, writer=output_stream)

    assert json.loads(output_stream.getvalue()) == {
        "jsonrpc": "2.0",
        "id": "ping-1",
        "result": {},
    }


@pytest.mark.asyncio
async def test_stdio_bounds_framing_before_allocating_the_complete_line() -> None:
    class BoundedReader(io.BytesIO):
        maximum_requested = 0

        def readline(self, size: int | None = -1, /) -> bytes:
            if size is not None:
                self.maximum_requested = max(self.maximum_requested, size)
            return super().readline(size)

    valid_request = (
        json.dumps({"jsonrpc": "2.0", "id": "ignored", "method": "ping", "params": {}}).encode()
        + b"\n"
    )
    input_stream = BoundedReader(b"x" * (MAXIMUM_MESSAGE_BYTES + 1) + b"\n" + valid_request)
    output_stream = io.BytesIO()

    await serve_stdio(MakoletMcpServer(StubQueries()), reader=input_stream, writer=output_stream)

    response = json.loads(output_stream.getvalue())
    assert response["error"] == {
        "code": -32600,
        "message": "Request exceeds the message size limit",
    }
    assert input_stream.maximum_requested == MAXIMUM_MESSAGE_BYTES + 1
    assert b"ignored" not in output_stream.getvalue()


@pytest.mark.asyncio
async def test_http_transport_validates_modern_headers_before_tool_execution() -> None:
    queries = StubQueries()
    app = create_mcp_http_app(queries_server := MakoletMcpServer(queries))
    transport = httpx.ASGITransport(app=app)
    payload = _request(
        "tools/call",
        {"name": "list_retailers", "arguments": {}},
    )

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        missing_version = await client.post(
            "/mcp",
            json=payload,
            headers={
                "Accept": "application/json, text/event-stream",
                "Mcp-Method": "tools/call",
                "Mcp-Name": "list_retailers",
            },
        )
        wrong_method = await client.post(
            "/mcp",
            json=payload,
            headers={**_headers("tools/list", name="list_retailers")},
        )
        wrong_name = await client.post(
            "/mcp",
            json=payload,
            headers={**_headers("tools/call", name="get_product")},
        )
        valid = await client.post(
            "/mcp",
            json=payload,
            headers=_headers("tools/call", name="list_retailers"),
        )
        encoded_name = await client.post(
            "/mcp",
            json=payload,
            headers=_headers("tools/call", name="=?base64?bGlzdF9yZXRhaWxlcnM=?="),
        )

    assert queries_server is not None
    assert [missing_version.status_code, wrong_method.status_code, wrong_name.status_code] == [
        400,
        400,
        400,
    ]
    assert all(
        response.json()["error"]["code"] == -32020
        for response in (missing_version, wrong_method, wrong_name)
    )
    assert valid.status_code == 200
    assert encoded_name.status_code == 200
    assert valid.headers["mcp-protocol-version"] == LATEST_PROTOCOL_VERSION
    assert valid.json()["result"]["structuredContent"]["nextCursor"] == "next"
    assert [call[0] for call in queries.calls] == ["list_retailers", "list_retailers"]


@pytest.mark.asyncio
async def test_http_transport_rejects_untrusted_origins_and_unsupported_methods() -> None:
    app = create_mcp_http_app(MakoletMcpServer(StubQueries()))
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        blocked = await client.post(
            "/mcp",
            json=_request("server/discover"),
            headers={**_headers("server/discover"), "Origin": "https://evil.example"},
        )
        get_response = await client.get("/mcp")
        delete_response = await client.delete("/mcp")

    assert blocked.status_code == 403
    assert get_response.status_code == delete_response.status_code == 405
    assert get_response.headers["allow"] == "POST"


@pytest.mark.asyncio
async def test_http_transport_enforces_media_negotiation_and_modern_method_status() -> None:
    app = create_mcp_http_app(MakoletMcpServer(StubQueries()))
    transport = httpx.ASGITransport(app=app)
    unknown = _request("unknown/read")

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        false_positive_content_type = await client.post(
            "/mcp",
            content=json.dumps(unknown),
            headers={
                **_headers("unknown/read"),
                "Content-Type": "text/plain; note=application/json",
            },
        )
        incomplete_accept = await client.post(
            "/mcp",
            json=unknown,
            headers={**_headers("unknown/read"), "Accept": "application/json"},
        )
        not_found = await client.post(
            "/mcp",
            json=unknown,
            headers=_headers("unknown/read"),
        )

    assert false_positive_content_type.status_code == 415
    assert incomplete_accept.status_code == 406
    assert not_found.status_code == 404
    assert not_found.json()["error"]["code"] == -32601


def _contains_open_object_schema(value: object) -> bool:
    if isinstance(value, dict):
        if value.get("additionalProperties") is True:
            return True
        return any(_contains_open_object_schema(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_open_object_schema(item) for item in value)
    return False
