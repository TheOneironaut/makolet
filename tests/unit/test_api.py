from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any, cast
from uuid import UUID

import httpx
import pytest
from fastapi import Body

from makolet.adapters.persistence.queries import MAXIMUM_PROMOTION_RELATIONS
from makolet.application.models import Page
from makolet.application.queries import QueryService
from makolet.domain.errors import QueryLimitError
from makolet.interfaces import api as api_module
from makolet.interfaces.api import (
    HistoryPageResponse,
    PromotionOutput,
    SourceStatusOutput,
    create_app,
)
from makolet.interfaces.mcp import LATEST_PROTOCOL_VERSION, MakoletMcpServer
from makolet.interfaces.response_limits import (
    MAXIMUM_PUBLIC_RESPONSE_BYTES,
    compact_json_fits,
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


def _retailer() -> dict[str, object]:
    return {
        "id": RETAILER_ID,
        "source_key": "demo-retailer",
        "legal_name": None,
        "display_name": "Demo retailer",
        "edi": None,
        "is_active": True,
        "created_at": OBSERVED_AT,
        "updated_at": OBSERVED_AT,
    }


def _price() -> dict[str, object]:
    return {
        "id": PRICE_ID,
        "item_price": Decimal("19.90"),
        "unit_of_measure_price": Decimal("3.98"),
        "allow_discount": True,
        "source_updated_at": OBSERVED_AT,
        "first_observed_at": OBSERVED_AT,
        "last_observed_at": OBSERVED_AT,
        "source_file_id": SOURCE_FILE_ID,
        "source_document_type": "price_full",
        "source_timestamp": OBSERVED_AT,
        "source_discovered_at": OBSERVED_AT,
        "content_sha256": "a" * 64,
        "is_available": True,
        "item_status": 1,
        "availability_source_file_id": SOURCE_FILE_ID,
        "retailer_item_id": RETAILER_ITEM_ID,
        "source_item_code": "SKU-1",
        "retailer_item_name": "Retailer demo",
        "portal_id": PORTAL_ID,
        "portal_key": "demo-portal",
        "store_id": STORE_ID,
        "store_name": "Demo store",
        "retailer_id": RETAILER_ID,
        "retailer_key": "demo-retailer",
        "retailer_name": "Demo retailer",
    }


def _price_history(retailer_item_name: str) -> dict[str, object]:
    return {
        "id": PRICE_ID,
        "item_price": Decimal("19.90"),
        "unit_of_measure_price": Decimal("3.98"),
        "allow_discount": True,
        "source_updated_at": OBSERVED_AT,
        "valid_from": OBSERVED_AT,
        "valid_to": None,
        "source_file_id": SOURCE_FILE_ID,
        "source_document_type": "price_full",
        "source_timestamp": OBSERVED_AT,
        "source_discovered_at": OBSERVED_AT,
        "content_sha256": "a" * 64,
        "retailer_item_id": RETAILER_ITEM_ID,
        "source_item_code": "SKU-1",
        "retailer_item_name": retailer_item_name,
        "portal_id": PORTAL_ID,
        "portal_key": "demo-portal",
        "retailer_id": RETAILER_ID,
        "retailer_key": "demo-retailer",
        "retailer_name": "Demo retailer",
        "store_id": STORE_ID,
        "store_name": "Demo store",
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
        "store_id": STORE_ID,
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

    async def list_retailers(self, **kwargs: Any) -> Page:
        self.calls.append(("list_retailers", kwargs))
        return Page((_retailer(),), "next")

    async def find_stores(self, **kwargs: Any) -> Page:
        self.calls.append(("find_stores", kwargs))
        return Page((), None)

    async def search_products(self, query: str, **kwargs: Any) -> Page:
        self.calls.append(("search_products", {"query": query, **kwargs}))
        return Page(
            (
                {
                    "id": PRODUCT_ID,
                    "name": "Demo coffee",
                    "brand": "Demo brand",
                    "manufacturer": None,
                    "quantity": Decimal("500.000000"),
                    "unit_of_measure": "g",
                    "rank": 50.0,
                },
            ),
            None,
        )

    async def get_product(self, product_id: UUID) -> dict[str, object] | None:
        self.calls.append(("get_product", {"product_id": product_id}))
        return self.product

    async def find_product_by_barcode(self, barcode: str) -> dict[str, object] | None:
        self.calls.append(("barcode", {"barcode": barcode}))
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
                "retailer_item",
                {
                    "retailer_id": retailer_id,
                    "portal_id": portal_id,
                    "item_code": item_code,
                },
            )
        )
        return _retailer_item_product() if self.product is not None else None

    async def current_prices(self, product_id: UUID, **kwargs: Any) -> Page:
        self.calls.append(("current_prices", {"product_id": product_id, **kwargs}))
        return Page((_price(),), None)

    async def compare_product_prices(self, product_id: UUID, **kwargs: Any) -> Page:
        return await self.current_prices(product_id, **kwargs)

    async def price_history(self, product_id: UUID, **kwargs: Any) -> Page:
        self.calls.append(("price_history", {"product_id": product_id, **kwargs}))
        return Page((), None)

    async def item_availability(self, product_id: UUID, **kwargs: Any) -> Page:
        self.calls.append(("availability", {"product_id": product_id, **kwargs}))
        return Page((), None)

    async def promotions(self, **kwargs: Any) -> Page:
        self.calls.append(("promotions", kwargs))
        return Page((), None)

    async def promotion_history(self, **kwargs: Any) -> Page:
        self.calls.append(("promotion_history", kwargs))
        return Page((), None)

    async def freshness(self, **kwargs: Any) -> Page:
        self.calls.append(("freshness", kwargs))
        return Page((_freshness(),), None)

    async def source_status(self, **kwargs: Any) -> Page:
        self.calls.append(("source_status", kwargs))
        return Page((), None)

    async def platform_status(self, **kwargs: Any) -> dict[str, object]:
        self.calls.append(("platform_status", kwargs))
        return {
            "maintenance": {"active": False, "mode": "normal"},
            "sources": Page((), None),
        }


class CardinalityStatusQueries(StubQueries):
    def __init__(self, truncation_reason: str) -> None:
        super().__init__()
        self._truncation_reason = truncation_reason

    async def source_status(self, **kwargs: Any) -> Page:
        self.calls.append(("source_status", kwargs))
        return Page((_source_status(self._truncation_reason),), None)


async def ready() -> bool:
    return True


async def not_ready() -> bool:
    return False


def test_compact_json_preflight_counts_utf8_and_escaped_bytes_exactly() -> None:
    payload = {"value": 'עברית "מצוטטת" \\\n\t'}
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    assert compact_json_fits(payload, len(encoded)) is True
    assert compact_json_fits(payload, len(encoded) - 1) is False


def test_maximum_publisher_promotion_page_fits_before_interface_serialization() -> None:
    publisher_text = "🛒" * 10_000
    relationship_items = [
        {
            "retailer_item_id": RETAILER_ITEM_ID,
            "source_item_code": "🛒" * 128,
            "name": "🛒" * 2_048,
            "item_type": 1,
            "is_gift": False,
            "canonical_product_id": PRODUCT_ID,
        }
        for _ in range(MAXIMUM_PROMOTION_RELATIONS)
    ]
    relationship_stores = [
        {
            "store_id": STORE_ID,
            "source_store_code": "🛒" * 128,
            "name": "🛒" * 1_024,
            "city": publisher_text,
        }
        for _ in range(MAXIMUM_PROMOTION_RELATIONS)
    ]
    promotion = PromotionOutput.model_validate(
        {
            "id": PRODUCT_ID,
            "retailer_id": RETAILER_ID,
            "retailer_key": "r" * 128,
            "retailer_name": "🛒" * 512,
            "portal_id": PORTAL_ID,
            "portal_key": "p" * 128,
            "subchain_code": "s" * 128,
            "source_promotion_id": "x" * 256,
            "source_scope_store_code": "s" * 128,
            "description": publisher_text,
            "discount_kind": "quantity",
            "starts_at": OBSERVED_AT,
            "ends_at": OBSERVED_AT,
            "reward_type": 1,
            "allows_multiple_discounts": False,
            "minimum_quantity": Decimal("1"),
            "maximum_quantity": Decimal("2"),
            "discount_rate": Decimal("0.5"),
            "minimum_purchase": Decimal("1"),
            "discounted_price": Decimal("1"),
            "discounted_unit_price": Decimal("1"),
            "minimum_items_offered": 1,
            "additional_restrictions": publisher_text,
            "remarks": publisher_text,
            "is_active": True,
            "valid_from": OBSERVED_AT,
            "valid_to": None,
            "last_observed_at": OBSERVED_AT,
            "source_file_id": SOURCE_FILE_ID,
            "source_document_type": "promotion_full",
            "source_timestamp": OBSERVED_AT,
            "source_discovered_at": OBSERVED_AT,
            "content_sha256": "a" * 64,
            "items": relationship_items,
            "returned_item_count": MAXIMUM_PROMOTION_RELATIONS,
            "items_truncated": True,
            "stores": relationship_stores,
            "returned_store_count": MAXIMUM_PROMOTION_RELATIONS,
            "stores_truncated": True,
            "clubs": ["🛒" * 128] * MAXIMUM_PROMOTION_RELATIONS,
            "returned_club_count": MAXIMUM_PROMOTION_RELATIONS,
            "clubs_truncated": True,
        }
    )
    page = HistoryPageResponse[PromotionOutput](
        items=[promotion],
        next_cursor="c" * 512,
    ).model_dump(mode="json")

    assert compact_json_fits(page, MAXIMUM_PUBLIC_RESPONSE_BYTES)


class CapturingLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, object]]] = []

    def error(self, event: str, **values: object) -> None:
        self.records.append((event, values))


class FailingQueries(StubQueries):
    async def list_retailers(self, **kwargs: Any) -> Page:
        del kwargs
        raise RuntimeError("sentinel-database-password")


class IdentifierLimitQueries(StubQueries):
    async def get_product(self, product_id: UUID) -> dict[str, object] | None:
        del product_id
        raise QueryLimitError("Product identifier count exceeds the 200-item public query limit")


class MalformedQueries(StubQueries):
    async def list_retailers(self, **kwargs: Any) -> Page:
        del kwargs
        return Page(({"id": RETAILER_ID, "credential": "sentinel-api-key"},), None)


class HistoryQueries(StubQueries):
    def __init__(self, *, count: int, retailer_item_name: str) -> None:
        super().__init__()
        self.count = count
        self.retailer_item_name = retailer_item_name

    async def price_history(self, product_id: UUID, **kwargs: Any) -> Page:
        self.calls.append(("price_history", {"product_id": product_id, **kwargs}))
        return Page(
            tuple(_price_history(self.retailer_item_name) for _ in range(self.count)),
            None,
        )


def _mcp_history_request() -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": LATEST_PROTOCOL_VERSION,
                "io.modelcontextprotocol/clientCapabilities": {},
                "io.modelcontextprotocol/clientInfo": {
                    "name": "response-budget-test",
                    "version": "1",
                },
            },
            "name": "get_price_history",
            "arguments": {"product_id": str(PRODUCT_ID), "limit": 1_000},
        },
    }


def client() -> tuple[httpx.AsyncClient, StubQueries]:
    queries = StubQueries()
    app = create_app(queries, ready)
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test"), queries


def test_http_app_publishes_its_bounded_uvicorn_concurrency() -> None:
    app = create_app(StubQueries(), ready, maximum_concurrency=37)

    assert app.state.uvicorn_limit_concurrency == 37


@pytest.mark.parametrize("maximum_concurrency", [0, 10_001])
def test_http_app_rejects_invalid_uvicorn_concurrency(maximum_concurrency: int) -> None:
    with pytest.raises(ValueError, match="concurrency limit"):
        create_app(StubQueries(), ready, maximum_concurrency=maximum_concurrency)


class NeverCalledQueryRepository:
    def __init__(self) -> None:
        self.called = False

    async def search_products(self, *args: object, **kwargs: object) -> Page:
        del args, kwargs
        self.called = True
        raise AssertionError("short product query reached repository")

    async def find_stores(self, *args: object, **kwargs: object) -> Page:
        del args, kwargs
        self.called = True
        raise AssertionError("short store query reached repository")


def bounded_client() -> tuple[httpx.AsyncClient, NeverCalledQueryRepository]:
    repository = NeverCalledQueryRepository()
    queries = QueryService(cast(Any, repository), FixedClock())
    app = create_app(queries, ready)
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test"), repository


@pytest.mark.asyncio
async def test_health_readiness_and_openapi_are_public() -> None:
    test_client, _ = client()

    async with test_client:
        assert (await test_client.get("/healthz")).json() == {"status": "ok"}
        assert (await test_client.get("/readyz")).json() == {"status": "ready"}
        metrics = await test_client.get("/metrics")
        schema = (await test_client.get("/openapi.json")).json()
    assert metrics.status_code == 200
    assert metrics.headers["content-type"].startswith("text/plain")
    assert b"python_gc_objects_collected_total" in metrics.content
    assert "/api/v1/products/search" in schema["paths"]
    assert "/api/v1/retailer-items/lookup" in schema["paths"]
    assert "/api/v1/promotions/history" in schema["paths"]
    assert "/api/v1/status" in schema["paths"]
    assert "/metrics" not in schema["paths"]
    assert schema["info"]["title"] == "Makolet API"
    lookup_parameters = {
        parameter["name"]: parameter
        for parameter in schema["paths"]["/api/v1/retailer-items/lookup"]["get"]["parameters"]
    }
    assert lookup_parameters["retailer_id"]["required"] is True
    history_parameters = {
        parameter["name"]: parameter
        for parameter in schema["paths"]["/api/v1/products/{product_id}/history"]["get"][
            "parameters"
        ]
    }
    assert "pair with until" in history_parameters["since"]["description"]
    assert "trailing 366 days" in history_parameters["until"]["description"]
    assert lookup_parameters["portal_id"]["required"] is False
    assert lookup_parameters["item_code"]["required"] is True
    schemas = schema["components"]["schemas"]
    assert schemas["RetailerPageResponse"]["properties"]["items"]["maxItems"] == 200
    assert schemas["PriceHistoryPageResponse"]["properties"]["items"]["maxItems"] == 1_000
    assert set(schemas["ProductSearchOutput"]["properties"]) >= {
        "id",
        "name",
        "quantity",
        "rank",
    }
    assert schemas["ProductSearchOutput"]["properties"]["id"]["format"] == "uuid"
    assert schemas["PriceOutput"]["properties"]["item_price"]["type"] == "string"
    assert schemas["PriceOutput"]["properties"]["source_discovered_at"]["format"] == ("date-time")
    assert schemas["PromotionOutput"]["properties"]["items"]["maxItems"] == 200
    freshness_properties = schemas["FreshnessOutput"]["properties"]
    assert freshness_properties["observed_items"]["maximum"] == 1_000
    assert freshness_properties["available_items"]["maximum"] == 1_000
    assert freshness_properties["item_probe_limit"]["const"] == 1_000
    assert freshness_properties["items_truncated"]["type"] == "boolean"
    assert "collection_attempt_status" in schemas["SourceStatusOutput"]["properties"]
    assert "collection_charged_bytes" in schemas["SourceStatusOutput"]["properties"]
    assert "collection_truncation_reason" in schemas["SourceStatusOutput"]["properties"]
    assert all(
        response["content"]["application/json"]["schema"]
        == {"$ref": "#/components/schemas/ErrorResponse"}
        for status, response in schema["paths"]["/api/v1/products/search"]["get"][
            "responses"
        ].items()
        if status in {"400", "404", "422", "500", "503"}
    )
    assert not _contains_open_object_schema(schema)


@pytest.mark.asyncio
async def test_readiness_is_unavailable_when_runtime_check_fails() -> None:
    app = create_app(StubQueries(), not_ready)
    test_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")

    async with test_client:
        liveness = await test_client.get("/healthz")
        readiness = await test_client.get("/readyz")

    assert liveness.status_code == 200
    assert liveness.json() == {"status": "ok"}
    assert readiness.status_code == 503
    assert readiness.json() == {"status": "not_ready"}


@pytest.mark.asyncio
async def test_unexpected_exception_is_generic_and_secret_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = CapturingLogger()
    monkeypatch.setattr(api_module, "_LOGGER", logger)
    app = create_app(FailingQueries(), ready)
    test_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    )

    async with test_client:
        response = await test_client.get(
            "/api/v1/retailers", headers={"X-Request-ID": "failure-boundary-1"}
        )

    assert response.status_code == 500
    assert response.headers["x-request-id"] == "failure-boundary-1"
    assert response.json() == {
        "error": {
            "code": "internal_server_error",
            "message": "The request could not be completed",
            "request_id": "failure-boundary-1",
        }
    }
    assert "sentinel-database-password" not in response.text
    assert logger.records == [
        (
            "api.unexpected_error",
            {
                "correlation_id": "failure-boundary-1",
                "error_type": "RuntimeError",
                "request_id": "failure-boundary-1",
            },
        )
    ]
    assert "sentinel-database-password" not in repr(logger.records)
    assert Exception in app.exception_handlers
    assert BaseException not in app.exception_handlers


@pytest.mark.asyncio
async def test_product_identifier_overflow_uses_stable_query_limit_error() -> None:
    app = create_app(IdentifierLimitQueries(), ready)
    test_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )

    async with test_client:
        response = await test_client.get(
            f"/api/v1/products/{PRODUCT_ID}",
            headers={"X-Request-ID": "product-identifiers-1"},
        )

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "query_limit_exceeded",
            "message": "Product identifier count exceeds the 200-item public query limit",
            "request_id": "product-identifiers-1",
        }
    }


@pytest.mark.asyncio
async def test_malformed_service_output_is_a_secret_safe_internal_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = CapturingLogger()
    monkeypatch.setattr(api_module, "_LOGGER", logger)
    app = create_app(MalformedQueries(), ready)
    test_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    )

    async with test_client:
        response = await test_client.get(
            "/api/v1/retailers",
            headers={"X-Request-ID": "malformed-output-1"},
        )

    assert response.status_code == 500
    assert response.json()["error"] == {
        "code": "internal_server_error",
        "message": "The request could not be completed",
        "request_id": "malformed-output-1",
    }
    assert "sentinel-api-key" not in response.text
    assert logger.records[0][1]["error_type"] == "ValidationError"
    assert "sentinel-api-key" not in repr(logger.records)


@pytest.mark.asyncio
async def test_request_validation_uses_one_bounded_non_echoing_error_contract() -> None:
    app = create_app(StubQueries(), ready)

    @app.post("/validation-probe", include_in_schema=False)
    async def validation_probe(payload: Annotated[int, Body()]) -> dict[str, int]:
        return {"payload": payload}

    test_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )

    async with test_client:
        responses = (
            await test_client.get("/api/v1/products/not-a-uuid"),
            await test_client.get("/api/v1/products/search?query=coffee&limit=201"),
            await test_client.post(
                "/validation-probe",
                json={"secret": "sentinel-request-secret"},
            ),
        )

    for response in responses:
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "request_validation_error"
        assert response.json()["error"]["message"] == "Request validation failed"
        assert response.json()["error"]["request_id"] == response.headers["x-request-id"]
        assert "sentinel-request-secret" not in response.text


@pytest.mark.asyncio
async def test_retailer_page_and_request_id_are_stable() -> None:
    test_client, _ = client()

    async with test_client:
        response = await test_client.get(
            "/api/v1/retailers?limit=10", headers={"X-Request-ID": "test-123"}
        )

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "test-123"
    assert response.json()["next_cursor"] == "next"


@pytest.mark.asyncio
async def test_http_and_mcp_reject_oversized_valid_multibyte_history_pages() -> None:
    escape_heavy_hebrew = 'עברית "מצוטטת" \\\n\t' * 160
    queries = HistoryQueries(count=1_000, retailer_item_name=escape_heavy_hebrew)
    app = create_app(queries, ready)
    test_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )

    async with test_client:
        http_response = await test_client.get(
            f"/api/v1/products/{PRODUCT_ID}/history?limit=1000",
            headers={"X-Request-ID": "oversized-history-1"},
        )
    mcp_response = await MakoletMcpServer(queries).handle(_mcp_history_request())

    assert http_response.status_code == 422
    assert len(http_response.content) <= MAXIMUM_PUBLIC_RESPONSE_BYTES
    assert http_response.json() == {
        "error": {
            "code": "query_limit_exceeded",
            "message": ("Response exceeds the 1 MiB public body limit; request a smaller page"),
            "request_id": "oversized-history-1",
        }
    }
    assert escape_heavy_hebrew[:100] not in http_response.text
    assert mcp_response is not None
    encoded_mcp = json.dumps(
        mcp_response,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    assert len(encoded_mcp) <= MAXIMUM_PUBLIC_RESPONSE_BYTES
    assert mcp_response["result"]["isError"] is True
    assert mcp_response["result"]["structuredContent"]["error"]["code"] == ("query_limit_exceeded")


@pytest.mark.asyncio
async def test_http_history_rejects_one_sided_ranges_before_repository_work() -> None:
    queries = QueryService(cast(Any, object()), FixedClock())
    app = create_app(queries, ready)
    test_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )

    async with test_client:
        price = await test_client.get(
            f"/api/v1/products/{PRODUCT_ID}/history",
            params={"since": OBSERVED_AT.isoformat()},
        )
        promotion = await test_client.get(
            "/api/v1/promotions/history",
            params={"until": OBSERVED_AT.isoformat()},
        )

    for response in (price, promotion):
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "domain_validation_error"


@pytest.mark.asyncio
async def test_http_and_mcp_preserve_normal_hebrew_and_json_escapes() -> None:
    retailer_item_name = 'קפה "מיוחד" \\\n\t'
    queries = HistoryQueries(count=1, retailer_item_name=retailer_item_name)
    app = create_app(queries, ready)
    test_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )

    async with test_client:
        http_response = await test_client.get(f"/api/v1/products/{PRODUCT_ID}/history")
    mcp_response = await MakoletMcpServer(queries).handle(_mcp_history_request())

    assert http_response.status_code == 200
    assert http_response.json()["items"][0]["retailer_item_name"] == retailer_item_name
    assert len(http_response.content) <= MAXIMUM_PUBLIC_RESPONSE_BYTES
    assert mcp_response is not None
    assert mcp_response["result"]["isError"] is False
    structured = mcp_response["result"]["structuredContent"]
    assert structured["items"][0]["retailer_item_name"] == retailer_item_name
    assert json.loads(mcp_response["result"]["content"][0]["text"]) == structured


@pytest.mark.asyncio
async def test_product_response_serializes_uuid_and_datetime_without_float_money() -> None:
    test_client, _ = client()

    async with test_client:
        response = await test_client.get(f"/api/v1/products/{PRODUCT_ID}")

    assert response.status_code == 200
    assert response.json()["data"] == {
        "id": str(PRODUCT_ID),
        "name": "Demo coffee",
        "brand": "Demo brand",
        "manufacturer": None,
        "quantity": "500.000000",
        "unit_of_measure": "g",
        "status": "active",
        "created_at": "2026-08-11T00:00:00Z",
        "updated_at": "2026-08-11T00:00:00Z",
        "identifiers": [],
    }


@pytest.mark.asyncio
async def test_missing_product_returns_structured_non_leaking_error() -> None:
    test_client, queries = client()
    queries.product = None

    async with test_client:
        response = await test_client.get(f"/api/v1/products/{PRODUCT_ID}")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"
    assert response.json()["error"]["request_id"] == response.headers["x-request-id"]


@pytest.mark.asyncio
async def test_retailer_item_lookup_requires_scope_and_returns_structured_not_found() -> None:
    test_client, queries = client()
    retailer_id = UUID("20000000-0000-0000-0000-000000000001")
    portal_id = UUID("30000000-0000-0000-0000-000000000001")

    async with test_client:
        success = await test_client.get(
            "/api/v1/retailer-items/lookup",
            params={
                "retailer_id": str(retailer_id),
                "portal_id": str(portal_id),
                "item_code": "SKU-1",
            },
        )
        missing_scope = await test_client.get(
            "/api/v1/retailer-items/lookup",
            params={"item_code": "SKU-1"},
        )
        queries.product = None
        missing = await test_client.get(
            "/api/v1/retailer-items/lookup",
            params={"retailer_id": str(retailer_id), "item_code": "SKU-404"},
        )

    assert success.status_code == 200
    assert queries.calls[0] == (
        "retailer_item",
        {
            "retailer_id": retailer_id,
            "portal_id": portal_id,
            "item_code": "SKU-1",
        },
    )
    assert missing_scope.status_code == 422
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "not_found"


@pytest.mark.asyncio
async def test_status_exposes_normalized_rebuild_maintenance_state() -> None:
    test_client, _ = client()

    async with test_client:
        response = await test_client.get("/api/v1/status?limit=5")

    assert response.status_code == 200
    assert response.json() == {
        "maintenance": {"active": False, "mode": "normal"},
        "sources": {"items": [], "next_cursor": None},
    }


@pytest.mark.parametrize(
    "truncation_reason",
    ["identity_day_limit", "attempt_day_limit", "success_day_limit"],
)
@pytest.mark.asyncio
async def test_source_status_serializes_cardinality_truncation_reasons(
    truncation_reason: str,
) -> None:
    queries = CardinalityStatusQueries(truncation_reason)
    app = create_app(queries, ready)
    test_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    )

    async with test_client:
        response = await test_client.get("/api/v1/source-status?limit=1")

    assert response.status_code == 200
    assert response.json()["items"][0]["collection_truncation_reason"] == truncation_reason


@pytest.mark.asyncio
async def test_freshness_exposes_explicit_bounded_probe_contract() -> None:
    test_client, queries = client()

    async with test_client:
        response = await test_client.get("/api/v1/freshness?limit=1")

    assert response.status_code == 200
    assert queries.calls[-1] == ("freshness", {"limit": 1, "cursor": None})
    item = response.json()["items"][0]
    assert item["available_items"] == item["observed_items"] == 1_000
    assert item["item_probe_limit"] == 1_000
    assert item["items_truncated"] is True


@pytest.mark.asyncio
async def test_framework_bounds_expensive_limit() -> None:
    test_client, queries = client()

    async with test_client:
        response = await test_client.get("/api/v1/products/search?query=coffee&limit=10000")

    assert response.status_code == 422
    assert queries.calls == []


@pytest.mark.asyncio
async def test_short_search_and_store_filters_return_structured_limit_errors() -> None:
    test_client, repository = bounded_client()

    async with test_client:
        responses = (
            await test_client.get("/api/v1/products/search?query=ab"),
            await test_client.get("/api/v1/stores?query=ab"),
            await test_client.get("/api/v1/stores?city=ab"),
        )

    assert {response.status_code for response in responses} == {422}
    assert {response.json()["error"]["code"] for response in responses} == {"query_limit_exceeded"}
    assert not repository.called


def _contains_open_object_schema(value: object) -> bool:
    if isinstance(value, dict):
        if value.get("additionalProperties") is True:
            return True
        return any(_contains_open_object_schema(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_open_object_schema(item) for item in value)
    return False
