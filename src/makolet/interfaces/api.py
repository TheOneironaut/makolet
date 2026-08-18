"""Versioned read-only FastAPI surface."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Final, Literal, cast
from uuid import UUID, uuid4

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, CollectorRegistry, generate_latest
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response
from structlog.contextvars import bind_contextvars, clear_contextvars

from makolet.adapters.observability import get_logger
from makolet.application.models import MAXIMUM_FRESHNESS_ITEMS_PER_STORE, Page
from makolet.application.queries import MAXIMUM_PRODUCT_IDENTIFIERS, QueryService
from makolet.domain.enums import (
    DiscountKind,
    DocumentType,
    IdentifierKind,
    IngestionStatus,
    SourceProtocol,
)
from makolet.domain.errors import (
    DomainValidationError,
    MakoletError,
    NotFoundError,
    QueryLimitError,
)
from makolet.interfaces.response_limits import (
    MAXIMUM_PUBLIC_RESPONSE_BYTES,
    compact_json_fits,
)

type ReadinessCheck = Callable[[], Awaitable[bool]]

_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")
_LOGGER = get_logger(__name__)
DEFAULT_HTTP_MAXIMUM_CONCURRENCY: Final = 100
_RESPONSE_LIMIT_MESSAGE: Final = (
    "Response exceeds the 1 MiB public body limit; request a smaller page"
)


class PublicOutput(BaseModel):
    """Closed public response value shared by HTTP and MCP schemas."""

    model_config = ConfigDict(extra="forbid", validate_default=True)


class PageResponse[ItemT](PublicOutput):
    items: Annotated[list[ItemT], Field(max_length=200)]
    next_cursor: Annotated[str, Field(max_length=512)] | None = None


class HistoryPageResponse[ItemT](PublicOutput):
    items: Annotated[list[ItemT], Field(max_length=1_000)]
    next_cursor: Annotated[str, Field(max_length=512)] | None = None


class RetailerOutput(PublicOutput):
    id: UUID
    source_key: str
    legal_name: str | None
    display_name: str
    edi: str | None
    is_active: bool
    created_at: AwareDatetime
    updated_at: AwareDatetime


class StoreOutput(PublicOutput):
    id: UUID
    retailer_id: UUID
    retailer_name: str
    portal_id: UUID
    portal_key: str
    chain_code: str
    subchain_code: str
    source_store_code: str
    name: str
    address: str | None
    city: str | None
    postal_code: str | None
    is_active: bool
    first_seen_at: AwareDatetime
    last_seen_at: AwareDatetime
    last_source_file_id: UUID | None


class ProductSearchOutput(PublicOutput):
    id: UUID
    name: str
    brand: str | None
    manufacturer: str | None
    quantity: Decimal | None
    unit_of_measure: str | None
    rank: Decimal


class ProductIdentifierOutput(PublicOutput):
    kind: IdentifierKind
    value: str
    validated: bool
    issuer_retailer_id: UUID | None
    issuer_retailer_key: str | None
    issuer_portal_id: UUID | None
    issuer_portal_key: str | None
    validation_method: str | None
    validation_evidence: dict[str, JsonValue]


class ProductDetailOutput(PublicOutput):
    id: UUID
    name: str
    brand: str | None
    manufacturer: str | None
    quantity: Decimal | None
    unit_of_measure: str | None
    status: Literal["active", "merged", "retired"]
    created_at: AwareDatetime
    updated_at: AwareDatetime
    identifiers: Annotated[
        list[ProductIdentifierOutput], Field(max_length=MAXIMUM_PRODUCT_IDENTIFIERS)
    ]


class IdentifierProvenanceOutput(PublicOutput):
    identifier_id: UUID
    value: str
    validated: bool
    issuer_retailer_id: UUID | None
    issuer_retailer_key: str | None
    issuer_portal_id: UUID | None
    issuer_portal_key: str | None
    validation_method: str | None
    validation_evidence: dict[str, JsonValue]


class BarcodeProductOutput(PublicOutput):
    id: UUID
    name: str
    brand: str | None
    manufacturer: str | None
    quantity: Decimal | None
    unit_of_measure: str | None
    globally_validated: bool
    barcode_validated: bool
    identifier_provenance: Annotated[list[IdentifierProvenanceOutput], Field(max_length=200)]
    barcode: Annotated[str, Field(pattern=r"^[0-9]+$", max_length=32)]
    identifier_scope: Literal["global", "portal_asserted"]


class RetailerItemProductOutput(PublicOutput):
    id: UUID
    name: str
    brand: str | None
    manufacturer: str | None
    quantity: Decimal | None
    unit_of_measure: str | None
    retailer_item_id: UUID
    retailer_item_code: str
    retailer_item_name: str
    retailer_item_source_file_id: UUID
    retailer_id: UUID
    retailer_key: str
    retailer_name: str
    portal_id: UUID
    portal_key: str
    match_method: str
    match_evidence: dict[str, JsonValue]


class SourceProvenanceOutput(PublicOutput):
    source_file_id: UUID
    source_document_type: DocumentType
    source_timestamp: AwareDatetime | None
    source_discovered_at: AwareDatetime
    content_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None


class PriceOutput(SourceProvenanceOutput):
    id: UUID
    item_price: Decimal
    unit_of_measure_price: Decimal | None
    allow_discount: bool | None
    source_updated_at: AwareDatetime | None
    first_observed_at: AwareDatetime
    last_observed_at: AwareDatetime
    is_available: bool | None
    item_status: int | None
    availability_source_file_id: UUID | None
    retailer_item_id: UUID
    source_item_code: str
    retailer_item_name: str
    portal_id: UUID
    portal_key: str
    store_id: UUID
    store_name: str
    retailer_id: UUID
    retailer_key: str
    retailer_name: str


class PriceHistoryOutput(SourceProvenanceOutput):
    id: UUID
    item_price: Decimal
    unit_of_measure_price: Decimal | None
    allow_discount: bool | None
    source_updated_at: AwareDatetime | None
    valid_from: AwareDatetime
    valid_to: AwareDatetime | None
    retailer_item_id: UUID
    source_item_code: str
    retailer_item_name: str
    portal_id: UUID
    portal_key: str
    retailer_id: UUID
    retailer_key: str
    retailer_name: str
    store_id: UUID
    store_name: str


class AvailabilityOutput(SourceProvenanceOutput):
    id: UUID
    is_available: bool
    item_status: int | None
    last_observed_at: AwareDatetime
    retailer_item_id: UUID
    source_item_code: str
    retailer_item_name: str
    portal_id: UUID
    portal_key: str
    retailer_id: UUID
    retailer_key: str
    retailer_name: str
    store_id: UUID
    store_name: str


class PromotionItemOutput(PublicOutput):
    retailer_item_id: UUID
    source_item_code: str
    name: str
    item_type: int | None
    is_gift: bool
    canonical_product_id: UUID | None


class PromotionStoreOutput(PublicOutput):
    store_id: UUID
    source_store_code: str
    name: str
    city: str | None


class PromotionOutput(SourceProvenanceOutput):
    id: UUID
    retailer_id: UUID
    retailer_key: str
    retailer_name: str
    portal_id: UUID
    portal_key: str
    subchain_code: str
    source_promotion_id: str
    source_scope_store_code: str
    description: str | None
    discount_kind: DiscountKind
    starts_at: AwareDatetime | None
    ends_at: AwareDatetime | None
    reward_type: int | None
    allows_multiple_discounts: bool | None
    minimum_quantity: Decimal | None
    maximum_quantity: Decimal | None
    discount_rate: Decimal | None
    minimum_purchase: Decimal | None
    discounted_price: Decimal | None
    discounted_unit_price: Decimal | None
    minimum_items_offered: int | None
    additional_restrictions: str | None
    remarks: str | None
    is_active: bool | None
    valid_from: AwareDatetime
    valid_to: AwareDatetime | None
    last_observed_at: AwareDatetime
    items: Annotated[list[PromotionItemOutput], Field(max_length=200)]
    returned_item_count: Annotated[int, Field(ge=0, le=200)]
    items_truncated: bool
    stores: Annotated[list[PromotionStoreOutput], Field(max_length=200)]
    returned_store_count: Annotated[int, Field(ge=0, le=200)]
    stores_truncated: bool
    clubs: Annotated[list[Annotated[str, Field(max_length=128)]], Field(max_length=200)]
    returned_club_count: Annotated[int, Field(ge=0, le=200)]
    clubs_truncated: bool


class FreshnessOutput(SourceProvenanceOutput):
    retailer_id: UUID
    retailer_key: str
    retailer_name: str
    portal_id: UUID
    portal_key: str
    store_id: UUID
    store_name: str
    last_observed_at: AwareDatetime
    available_items: Annotated[
        int,
        Field(ge=0, le=MAXIMUM_FRESHNESS_ITEMS_PER_STORE),
    ]
    observed_items: Annotated[
        int,
        Field(ge=1, le=MAXIMUM_FRESHNESS_ITEMS_PER_STORE),
    ]
    item_probe_limit: Literal[1_000]
    items_truncated: bool


class SourceStatusOutput(PublicOutput):
    portal_id: UUID
    portal_key: str
    family: str
    protocol: SourceProtocol
    retailer_id: UUID
    retailer_name: str
    source_file_id: UUID | None
    status: IngestionStatus | None
    document_type: DocumentType | None
    source_timestamp: AwareDatetime | None
    discovered_at: AwareDatetime | None
    updated_at: AwareDatetime | None
    error_code: str | None
    warning_count: Annotated[int, Field(ge=0)] | None
    record_rejection_count: Annotated[int, Field(ge=0)] | None
    file_quarantine_count: Annotated[int, Field(ge=0)] | None
    source_failure_count: Annotated[int, Field(ge=0)] | None
    system_failure_count: Annotated[int, Field(ge=0)] | None
    error_message: str | None
    last_good_source_file_id: UUID | None
    last_good_document_type: DocumentType | None
    last_good_source_timestamp: AwareDatetime | None
    last_good_discovered_at: AwareDatetime | None
    last_good_updated_at: AwareDatetime | None
    collection_attempt_id: UUID | None
    collection_attempt_status: Literal["running", "completed", "bounded", "failed"] | None
    collection_operation: Literal["ordinary", "backfill"] | None
    collection_generation: Annotated[int, Field(gt=0)] | None
    collection_range_since: AwareDatetime | None
    collection_range_until: AwareDatetime | None
    collection_archive_only: bool | None
    collection_started_at: AwareDatetime | None
    collection_finished_at: AwareDatetime | None
    collection_discovered_count: Annotated[int, Field(ge=0)] | None
    collection_processed_count: Annotated[int, Field(ge=0)] | None
    collection_skipped_unknown_count: Annotated[int, Field(ge=0)] | None
    collection_warning_count: Annotated[int, Field(ge=0)] | None
    collection_charged_bytes: Annotated[int, Field(ge=0)] | None
    collection_truncated: bool | None
    collection_truncation_reason: (
        Literal[
            "file_limit",
            "discovery_limit",
            "charged_byte_run_limit",
            "charged_byte_day_limit",
            "identity_day_limit",
            "attempt_day_limit",
            "success_day_limit",
            "legacy_limit",
        ]
        | None
    )
    collection_error_code: str | None
    collection_error_message: str | None


class NormalMaintenanceOutput(PublicOutput):
    active: Literal[False]
    mode: Literal["normal"]


class RebuildMaintenanceOutput(PublicOutput):
    active: Literal[True]
    mode: Literal["normalized_rebuild"]
    warning: str
    rebuild_run_id: UUID
    status: Literal["running", "failed", "completed"]
    archive_cutoff_at: AwareDatetime
    source_files_total: Annotated[int, Field(ge=0)]
    source_files_completed: Annotated[int, Field(ge=0)]
    last_source_file_id: UUID | None
    started_at: AwareDatetime
    updated_at: AwareDatetime
    finished_at: AwareDatetime | None


type MaintenanceOutput = NormalMaintenanceOutput | RebuildMaintenanceOutput


class RetailerPageResponse(PageResponse[RetailerOutput]):
    pass


class StorePageResponse(PageResponse[StoreOutput]):
    pass


class ProductSearchPageResponse(PageResponse[ProductSearchOutput]):
    pass


class PricePageResponse(PageResponse[PriceOutput]):
    pass


class PriceHistoryPageResponse(HistoryPageResponse[PriceHistoryOutput]):
    pass


class AvailabilityPageResponse(PageResponse[AvailabilityOutput]):
    pass


class PromotionPageResponse(PageResponse[PromotionOutput]):
    pass


class PromotionHistoryPageResponse(HistoryPageResponse[PromotionOutput]):
    pass


class FreshnessPageResponse(PageResponse[FreshnessOutput]):
    pass


class SourceStatusPageResponse(PageResponse[SourceStatusOutput]):
    pass


class ProductDetailResponse(PublicOutput):
    data: ProductDetailOutput


class BarcodeProductResponse(PublicOutput):
    data: BarcodeProductOutput


class RetailerItemProductResponse(PublicOutput):
    data: RetailerItemProductOutput


class ErrorBody(PublicOutput):
    code: str
    message: str
    request_id: str


class ErrorResponse(PublicOutput):
    error: ErrorBody


class HealthResponse(PublicOutput):
    status: Literal["ok", "ready", "not_ready"]


class PlatformStatusResponse(PublicOutput):
    maintenance: MaintenanceOutput
    sources: SourceStatusPageResponse


_PUBLIC_ERRORS: dict[int | str, dict[str, object]] = {
    400: {"model": ErrorResponse},
    404: {"model": ErrorResponse},
    422: {"model": ErrorResponse},
    500: {"model": ErrorResponse},
    503: {"model": ErrorResponse},
}


class _RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        supplied = request.headers.get("x-request-id", "")
        request_id = supplied if _REQUEST_ID.fullmatch(supplied) else str(uuid4())
        request.state.request_id = request_id
        clear_contextvars()
        bind_contextvars(correlation_id=request_id)
        try:
            response = await call_next(request)
        finally:
            clear_contextvars()
        response.headers["X-Request-ID"] = request_id
        return response


def create_app(
    query_service: QueryService,
    readiness: ReadinessCheck,
    *,
    metrics_registry: CollectorRegistry | None = None,
    maximum_concurrency: int = DEFAULT_HTTP_MAXIMUM_CONCURRENCY,
) -> FastAPI:
    if not 1 <= maximum_concurrency <= 10_000:
        raise ValueError("API HTTP concurrency limit must be between 1 and 10,000")
    app = FastAPI(
        title="Makolet API",
        summary="Read-only Israeli supermarket price-transparency queries",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )
    app.state.uvicorn_limit_concurrency = maximum_concurrency
    app.add_middleware(_RequestIdMiddleware)

    @app.exception_handler(RequestValidationError)
    async def handle_request_validation(
        request: Request, error: RequestValidationError
    ) -> JSONResponse:
        del error
        return _error_response(
            request,
            status_code=422,
            code="request_validation_error",
            message="Request validation failed",
        )

    @app.exception_handler(MakoletError)
    async def handle_makolet_error(request: Request, error: MakoletError) -> JSONResponse:
        if isinstance(error, NotFoundError):
            status_code = 404
        elif isinstance(error, (DomainValidationError, QueryLimitError)):
            status_code = 422
        else:
            status_code = 503 if error.retryable else 400
        return _error_response(
            request,
            status_code=status_code,
            code=error.code,
            message=str(error),
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, error: Exception) -> JSONResponse:
        request_id = _request_id(request)
        _LOGGER.error(
            "api.unexpected_error",
            correlation_id=request_id,
            error_type=type(error).__name__,
            request_id=request_id,
        )
        return _error_response(
            request,
            status_code=500,
            code="internal_server_error",
            message="The request could not be completed",
        )

    @app.get(
        "/healthz",
        response_model=HealthResponse,
        responses={500: {"model": ErrorResponse}},
        tags=["health"],
    )
    async def health() -> HealthResponse:
        return _bounded_output(HealthResponse(status="ok"))

    @app.get(
        "/readyz",
        response_model=HealthResponse,
        responses={500: {"model": ErrorResponse}, 503: {"model": HealthResponse}},
        tags=["health"],
    )
    async def ready() -> Response:
        if await readiness():
            return JSONResponse({"status": "ready"})
        return JSONResponse({"status": "not_ready"}, status_code=503)

    @app.get("/metrics", include_in_schema=False, tags=["health"])
    async def metrics() -> Response:
        return Response(
            content=generate_latest(metrics_registry or REGISTRY),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    @app.get(
        "/api/v1/retailers",
        response_model=RetailerPageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["retailers"],
    )
    async def list_retailers(
        limit: int = Query(50, ge=1, le=200), cursor: str | None = None
    ) -> RetailerPageResponse:
        return _page(
            await query_service.list_retailers(limit=limit, cursor=cursor),
            RetailerPageResponse,
        )

    @app.get(
        "/api/v1/stores",
        response_model=StorePageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["stores"],
    )
    async def find_stores(
        query: str | None = None,
        retailer_id: UUID | None = None,
        city: str | None = None,
        limit: int = Query(50, ge=1, le=200),
        cursor: str | None = None,
    ) -> StorePageResponse:
        return _page(
            await query_service.find_stores(
                query=query,
                retailer_id=retailer_id,
                city=city,
                limit=limit,
                cursor=cursor,
            ),
            StorePageResponse,
        )

    @app.get(
        "/api/v1/products/search",
        response_model=ProductSearchPageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["products"],
    )
    async def search_products(
        query: str = Query(min_length=1, max_length=200),
        limit: int = Query(50, ge=1, le=200),
        cursor: str | None = None,
    ) -> ProductSearchPageResponse:
        return _page(
            await query_service.search_products(query, limit=limit, cursor=cursor),
            ProductSearchPageResponse,
        )

    @app.get(
        "/api/v1/barcodes/{barcode}",
        response_model=BarcodeProductResponse,
        responses=_PUBLIC_ERRORS,
        tags=["products"],
    )
    async def barcode_lookup(barcode: str) -> BarcodeProductResponse:
        product = await query_service.find_product_by_barcode(barcode)
        if product is None:
            raise NotFoundError("Product barcode was not found")
        return _bounded_output(BarcodeProductResponse.model_validate({"data": product}))

    @app.get(
        "/api/v1/retailer-items/lookup",
        response_model=RetailerItemProductResponse,
        responses=_PUBLIC_ERRORS,
        tags=["products"],
    )
    async def retailer_item_lookup(
        retailer_id: UUID,
        portal_id: UUID | None = None,
        item_code: str = Query(min_length=1, max_length=128),
    ) -> RetailerItemProductResponse:
        product = await query_service.find_product_by_retailer_item_code(
            retailer_id,
            item_code,
            portal_id=portal_id,
        )
        if product is None:
            raise NotFoundError("Product retailer item code was not found for this retailer")
        return _bounded_output(RetailerItemProductResponse.model_validate({"data": product}))

    @app.get(
        "/api/v1/products/{product_id}",
        response_model=ProductDetailResponse,
        responses=_PUBLIC_ERRORS,
        tags=["products"],
    )
    async def get_product(product_id: UUID) -> ProductDetailResponse:
        product = await query_service.get_product(product_id)
        if product is None:
            raise NotFoundError("Product was not found")
        return _bounded_output(ProductDetailResponse.model_validate({"data": product}))

    @app.get(
        "/api/v1/products/{product_id}/prices",
        response_model=PricePageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["prices"],
    )
    async def current_prices(
        product_id: UUID,
        retailer_id: UUID | None = None,
        store_id: UUID | None = None,
        limit: int = Query(50, ge=1, le=200),
        cursor: str | None = None,
    ) -> PricePageResponse:
        return _page(
            await query_service.current_prices(
                product_id,
                retailer_id=retailer_id,
                store_id=store_id,
                limit=limit,
                cursor=cursor,
            ),
            PricePageResponse,
        )

    @app.get(
        "/api/v1/products/{product_id}/compare",
        response_model=PricePageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["prices"],
    )
    async def compare_prices(
        product_id: UUID,
        retailer_id: UUID | None = None,
        limit: int = Query(50, ge=1, le=200),
        cursor: str | None = None,
    ) -> PricePageResponse:
        return _page(
            await query_service.compare_product_prices(
                product_id, retailer_id=retailer_id, limit=limit, cursor=cursor
            ),
            PricePageResponse,
        )

    @app.get(
        "/api/v1/products/{product_id}/history",
        response_model=PriceHistoryPageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["prices"],
    )
    async def price_history(
        product_id: UUID,
        store_id: UUID | None = None,
        since: Annotated[
            datetime | None,
            Query(
                description=("Inclusive bound; pair with until or omit both for trailing 366 days.")
            ),
        ] = None,
        until: Annotated[
            datetime | None,
            Query(
                description=("Exclusive bound; pair with since or omit both for trailing 366 days.")
            ),
        ] = None,
        limit: int = Query(50, ge=1, le=1_000),
        cursor: str | None = None,
    ) -> PriceHistoryPageResponse:
        return _page(
            await query_service.price_history(
                product_id,
                store_id=store_id,
                since=since,
                until=until,
                limit=limit,
                cursor=cursor,
            ),
            PriceHistoryPageResponse,
        )

    @app.get(
        "/api/v1/products/{product_id}/availability",
        response_model=AvailabilityPageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["prices"],
    )
    async def availability(
        product_id: UUID,
        store_id: UUID | None = None,
        limit: int = Query(50, ge=1, le=200),
        cursor: str | None = None,
    ) -> AvailabilityPageResponse:
        return _page(
            await query_service.item_availability(
                product_id, store_id=store_id, limit=limit, cursor=cursor
            ),
            AvailabilityPageResponse,
        )

    @app.get(
        "/api/v1/promotions",
        response_model=PromotionPageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["promotions"],
    )
    async def promotions(
        product_id: UUID | None = None,
        store_id: UUID | None = None,
        at: datetime | None = None,
        limit: int = Query(50, ge=1, le=200),
        cursor: str | None = None,
    ) -> PromotionPageResponse:
        return _page(
            await query_service.promotions(
                product_id=product_id,
                store_id=store_id,
                at=at,
                limit=limit,
                cursor=cursor,
            ),
            PromotionPageResponse,
        )

    @app.get(
        "/api/v1/promotions/history",
        response_model=PromotionHistoryPageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["promotions"],
    )
    async def promotion_history(
        product_id: UUID | None = None,
        store_id: UUID | None = None,
        since: Annotated[
            datetime | None,
            Query(
                description=("Inclusive bound; pair with until or omit both for trailing 366 days.")
            ),
        ] = None,
        until: Annotated[
            datetime | None,
            Query(
                description=("Exclusive bound; pair with since or omit both for trailing 366 days.")
            ),
        ] = None,
        limit: int = Query(50, ge=1, le=1_000),
        cursor: str | None = None,
    ) -> PromotionHistoryPageResponse:
        return _page(
            await query_service.promotion_history(
                product_id=product_id,
                store_id=store_id,
                since=since,
                until=until,
                limit=limit,
                cursor=cursor,
            ),
            PromotionHistoryPageResponse,
        )

    @app.get(
        "/api/v1/freshness",
        response_model=FreshnessPageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["status"],
    )
    async def freshness(
        limit: int = Query(50, ge=1, le=200), cursor: str | None = None
    ) -> FreshnessPageResponse:
        return _page(
            await query_service.freshness(limit=limit, cursor=cursor),
            FreshnessPageResponse,
        )

    @app.get(
        "/api/v1/source-status",
        response_model=SourceStatusPageResponse,
        responses=_PUBLIC_ERRORS,
        tags=["status"],
    )
    async def source_status(
        limit: int = Query(50, ge=1, le=200), cursor: str | None = None
    ) -> SourceStatusPageResponse:
        return _page(
            await query_service.source_status(limit=limit, cursor=cursor),
            SourceStatusPageResponse,
        )

    @app.get(
        "/api/v1/status",
        response_model=PlatformStatusResponse,
        responses=_PUBLIC_ERRORS,
        tags=["status"],
    )
    async def platform_status(
        limit: int = Query(50, ge=1, le=200), cursor: str | None = None
    ) -> PlatformStatusResponse:
        status = await query_service.platform_status(limit=limit, cursor=cursor)
        return _bounded_output(
            PlatformStatusResponse.model_validate(
                {
                    "maintenance": cast(dict[str, Any], status["maintenance"]),
                    "sources": _page_payload(cast(Page, status["sources"])),
                }
            )
        )

    return app


def create_metrics_app(metrics_registry: CollectorRegistry) -> FastAPI:
    """Expose only process-local health and metrics for a non-API service."""
    app = FastAPI(
        title="Makolet process metrics",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.get("/healthz", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(
            content=generate_latest(metrics_registry),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    return app


def _page[ResponseT: PublicOutput](
    page: Page,
    response_model: type[ResponseT],
) -> ResponseT:
    return _bounded_output(response_model.model_validate(_page_payload(page)))


def _bounded_output[ResponseT: PublicOutput](response: ResponseT) -> ResponseT:
    payload = response.model_dump(mode="json")
    if not compact_json_fits(payload, MAXIMUM_PUBLIC_RESPONSE_BYTES):
        raise QueryLimitError(_RESPONSE_LIMIT_MESSAGE)
    return response


def _page_payload(page: Page) -> dict[str, object]:
    return {"items": list(page.items), "next_cursor": page.next_cursor}


def _error_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
) -> JSONResponse:
    request_id = _request_id(request)
    payload = ErrorResponse(
        error=ErrorBody(code=code, message=message, request_id=request_id)
    ).model_dump(mode="json")
    return JSONResponse(
        status_code=status_code,
        headers={"X-Request-ID": request_id},
        content=payload,
    )


def _request_id(request: Request) -> str:
    value = getattr(request.state, "request_id", None)
    return str(value) if value else str(uuid4())
