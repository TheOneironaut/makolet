"""Dependency-free read-only MCP 2026-07-28 server with legacy negotiation."""

from __future__ import annotations

import json
import sys
from base64 import b64decode
from binascii import Error as Base64Error
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, BinaryIO, cast
from uuid import UUID

import anyio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from makolet.application.models import Page
from makolet.application.queries import QueryService
from makolet.domain.errors import MakoletError, NotFoundError
from makolet.interfaces.api import (
    AvailabilityOutput,
    BarcodeProductResponse,
    FreshnessOutput,
    MaintenanceOutput,
    PriceHistoryOutput,
    PriceOutput,
    ProductDetailResponse,
    ProductSearchOutput,
    PromotionOutput,
    PublicOutput,
    RetailerItemProductResponse,
    RetailerOutput,
    SourceStatusOutput,
    StoreOutput,
)
from makolet.interfaces.response_limits import (
    MAXIMUM_PUBLIC_RESPONSE_BYTES,
    compact_json_fits,
)

LATEST_PROTOCOL_VERSION = "2026-07-28"
LEGACY_PROTOCOL_VERSIONS = ("2025-11-25", "2025-06-18")
SUPPORTED_PROTOCOL_VERSIONS = (LATEST_PROTOCOL_VERSION, *LEGACY_PROTOCOL_VERSIONS)
MAXIMUM_MESSAGE_BYTES = MAXIMUM_PUBLIC_RESPONSE_BYTES
DEFAULT_HTTP_BODY_TIMEOUT_SECONDS = 10.0
DEFAULT_HTTP_MAXIMUM_CONCURRENCY = 100
_MAXIMUM_TOOL_RESULT_BYTES = MAXIMUM_MESSAGE_BYTES - 4_096
_MAXIMUM_RESPONSE_REQUEST_ID_CHARACTERS = 128
_MAXIMUM_JSON_DEPTH = 64
_MAXIMUM_JSON_STRUCTURAL_TOKENS = 100_000
_MAXIMUM_JSON_STRING_CHARACTERS = 64 * 1024
_MAXIMUM_JSON_SCALAR_CHARACTERS = 4_096
_MAXIMUM_VALIDATION_ISSUES = 16
_EMPTY_DUPLICATED_TOOL_RESULT = {
    "content": [{"type": "text", "text": ""}],
    "structuredContent": {},
    "isError": False,
}
_DUPLICATED_TOOL_RESULT_OVERHEAD_BYTES = len(
    json.dumps(
        _EMPTY_DUPLICATED_TOOL_RESULT,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
) - len(b'{}""')

_SERVER_INFO = {
    "name": "makolet",
    "title": "Makolet supermarket price data",
    "version": "0.1.0",
    "description": (
        "Read-only Israeli supermarket products, prices, stores, promotions, and history"
    ),
}
_INSTRUCTIONS = (
    "Use barcode lookup for exact identifiers and product search for names. "
    "All list tools are bounded and may return a nextCursor; pass it back unchanged. "
    "Price and promotion results include source freshness and provenance where available."
)


class _Arguments(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _PageArguments(_Arguments):
    limit: int = Field(default=50, ge=1, le=200)
    cursor: str | None = Field(default=None, max_length=512)


class _SearchProducts(_PageArguments):
    query: str = Field(min_length=1, max_length=200)


class _Product(_Arguments):
    product_id: UUID


class _Barcode(_Arguments):
    barcode: str = Field(min_length=1, max_length=32, pattern=r"^[0-9]+$")


class _RetailerItem(_Arguments):
    retailer_id: UUID
    portal_id: UUID | None = None
    item_code: str = Field(min_length=1, max_length=128)


class _FindStores(_PageArguments):
    query: str | None = Field(default=None, max_length=200)
    retailer_id: UUID | None = None
    city: str | None = Field(default=None, max_length=200)


class _Prices(_PageArguments):
    product_id: UUID
    retailer_id: UUID | None = None
    store_id: UUID | None = None


class _ComparePrices(_PageArguments):
    product_id: UUID
    retailer_id: UUID | None = None


class _History(_Arguments):
    product_id: UUID
    store_id: UUID | None = None
    since: datetime | None = None
    until: datetime | None = None
    limit: int = Field(default=50, ge=1, le=1_000)
    cursor: str | None = Field(default=None, max_length=512)


class _Promotions(_PageArguments):
    product_id: UUID | None = None
    store_id: UUID | None = None
    at: datetime | None = None


class _PromotionHistory(_Arguments):
    product_id: UUID | None = None
    store_id: UUID | None = None
    since: datetime | None = None
    until: datetime | None = None
    limit: int = Field(default=50, ge=1, le=1_000)
    cursor: str | None = Field(default=None, max_length=512)


class _Availability(_PageArguments):
    product_id: UUID
    store_id: UUID | None = None


class _McpPage[ItemT](PublicOutput):
    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        validate_default=True,
    )

    items: Annotated[list[ItemT], Field(max_length=200)]
    next_cursor: Annotated[str, Field(max_length=512)] | None = Field(
        default=None,
        alias="nextCursor",
    )


class _McpHistoryPage[ItemT](PublicOutput):
    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        validate_default=True,
    )

    items: Annotated[list[ItemT], Field(max_length=1_000)]
    next_cursor: Annotated[str, Field(max_length=512)] | None = Field(
        default=None,
        alias="nextCursor",
    )


class _SearchProductOutput(_McpPage[ProductSearchOutput]):
    pass


class _RetailerPageOutput(_McpPage[RetailerOutput]):
    pass


class _StorePageOutput(_McpPage[StoreOutput]):
    pass


class _PricePageOutput(_McpPage[PriceOutput]):
    pass


class _PriceHistoryPageOutput(_McpHistoryPage[PriceHistoryOutput]):
    pass


class _PromotionPageOutput(_McpPage[PromotionOutput]):
    pass


class _PromotionHistoryPageOutput(_McpHistoryPage[PromotionOutput]):
    pass


class _AvailabilityPageOutput(_McpPage[AvailabilityOutput]):
    pass


class _FreshnessPageOutput(_McpPage[FreshnessOutput]):
    pass


class _SourceStatusToolOutput(_McpPage[SourceStatusOutput]):
    maintenance: MaintenanceOutput


class _ToolErrorBody(PublicOutput):
    code: Annotated[str, Field(min_length=1, max_length=128)]
    message: Annotated[str, Field(max_length=1_000)]


class _ToolErrorOutput(PublicOutput):
    error: _ToolErrorBody


@dataclass(frozen=True, slots=True)
class _ToolDefinition:
    name: str
    title: str
    description: str
    arguments: type[_Arguments]
    output: type[PublicOutput]

    def schema(self) -> dict[str, Any]:
        output_schema = TypeAdapter(self.output | _ToolErrorOutput).json_schema(
            mode="serialization"
        )
        output_schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
        output_schema["type"] = "object"
        return {
            "name": self.name,
            "title": self.title,
            "description": self.description,
            "inputSchema": self.arguments.model_json_schema(),
            "outputSchema": output_schema,
            "annotations": {
                "readOnlyHint": True,
                "destructiveHint": False,
                "idempotentHint": True,
                "openWorldHint": False,
            },
        }


_TOOLS = (
    _ToolDefinition(
        "search_products",
        "Search products",
        "Search Hebrew or English normalized product names.",
        _SearchProducts,
        _SearchProductOutput,
    ),
    _ToolDefinition(
        "get_product",
        "Get product",
        "Get one canonical product by stable UUID.",
        _Product,
        ProductDetailResponse,
    ),
    _ToolDefinition(
        "find_product_by_barcode",
        "Find barcode",
        "Find a canonical product by exact numeric barcode.",
        _Barcode,
        BarcodeProductResponse,
    ),
    _ToolDefinition(
        "find_product_by_retailer_item_code",
        "Find retailer item",
        (
            "Find a canonical product by exact item code in a retailer and optional source "
            "portal scope; ambiguous retailer-wide matches return a bounded error."
        ),
        _RetailerItem,
        RetailerItemProductResponse,
    ),
    _ToolDefinition(
        "list_retailers",
        "List retailers",
        "List configured supermarket retailers.",
        _PageArguments,
        _RetailerPageOutput,
    ),
    _ToolDefinition(
        "find_stores",
        "Find stores",
        "Find stores by name, city, or retailer.",
        _FindStores,
        _StorePageOutput,
    ),
    _ToolDefinition(
        "get_current_prices",
        "Current prices",
        "List current store prices for a product.",
        _Prices,
        _PricePageOutput,
    ),
    _ToolDefinition(
        "compare_product_prices",
        "Compare prices",
        "Compare current product prices across stores.",
        _ComparePrices,
        _PricePageOutput,
    ),
    _ToolDefinition(
        "get_price_history",
        "Price history",
        "Get bounded price-event history.",
        _History,
        _PriceHistoryPageOutput,
    ),
    _ToolDefinition(
        "get_active_promotions",
        "Active promotions",
        "Get promotions active at a specified instant.",
        _Promotions,
        _PromotionPageOutput,
    ),
    _ToolDefinition(
        "get_promotion_history",
        "Promotion history",
        "Get bounded observed promotion-version history.",
        _PromotionHistory,
        _PromotionHistoryPageOutput,
    ),
    _ToolDefinition(
        "get_item_availability",
        "Item availability",
        "Get current item availability across stores.",
        _Availability,
        _AvailabilityPageOutput,
    ),
    _ToolDefinition(
        "get_data_freshness",
        "Data freshness",
        "Get last successful data timestamps.",
        _PageArguments,
        _FreshnessPageOutput,
    ),
    _ToolDefinition(
        "get_source_status",
        "Source status",
        "Get bounded source ingestion and quality status.",
        _PageArguments,
        _SourceStatusToolOutput,
    ),
)
_TOOL_BY_NAME = {tool.name: tool for tool in _TOOLS}


@dataclass(frozen=True, slots=True)
class _RpcError(Exception):
    code: int
    message: str
    data: Any = None


class _InvalidJsonPayloadError(ValueError):
    """One hostile JSON frame failed bounded structural decoding."""


class MakoletMcpServer:
    """Small protocol core; transports only frame and validate their own metadata."""

    def __init__(self, queries: QueryService) -> None:
        self._queries = queries

    async def handle(
        self,
        message: Any,
        *,
        transport_protocol_version: str | None = None,
    ) -> dict[str, Any] | None:
        request_id = _safe_request_id(message)
        try:
            response = await self._handle_request(
                message,
                transport_protocol_version=transport_protocol_version,
            )
        except _RpcError as error:
            fault: dict[str, Any] = {"code": error.code, "message": error.message}
            if error.data is not None:
                fault["data"] = error.data
            response = {"jsonrpc": "2.0", "error": fault}
            if request_id is not None:
                response["id"] = request_id
        except Exception:
            response = {
                "jsonrpc": "2.0",
                "error": {"code": -32603, "message": "Internal server error"},
            }
            if request_id is not None:
                response["id"] = request_id
        if response is not None and not compact_json_fits(response, MAXIMUM_MESSAGE_BYTES - 1):
            return _error_response(None, -32603, "Response exceeds the message size limit")
        return response

    async def _handle_request(
        self,
        message: Any,
        *,
        transport_protocol_version: str | None,
    ) -> dict[str, Any] | None:
        if not isinstance(message, dict):
            raise _RpcError(-32600, "Request must be a JSON object")
        raw_id = message.get("id")
        if raw_id is not None and (not isinstance(raw_id, (str, int)) or isinstance(raw_id, bool)):
            raise _RpcError(-32600, "Request id must be a string or integer")
        if message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str):
            raise _RpcError(-32600, "Invalid JSON-RPC request")
        method = str(message["method"])
        params = message.get("params", {})
        if not isinstance(params, dict):
            raise _RpcError(-32602, "Method params must be an object")
        if raw_id is None:
            # MCP notifications deliberately receive no response.
            return None
        result = await self._dispatch(
            method,
            params,
            transport_protocol_version=transport_protocol_version,
        )
        return {"jsonrpc": "2.0", "id": raw_id, "result": result}

    async def parse_and_handle(
        self,
        payload: bytes,
        *,
        transport_protocol_version: str | None = None,
    ) -> dict[str, Any] | None:
        if len(payload) > MAXIMUM_MESSAGE_BYTES:
            return _error_response(None, -32600, "Request exceeds the message size limit")
        try:
            message = _decode_json_payload(payload)
        except _InvalidJsonPayloadError:
            return _error_response(None, -32700, "Invalid JSON")
        return await self.handle(message, transport_protocol_version=transport_protocol_version)

    async def _dispatch(
        self,
        method: str,
        params: dict[str, Any],
        *,
        transport_protocol_version: str | None,
    ) -> dict[str, Any]:
        if method == "initialize":
            return _legacy_initialize(params)
        modern = method == "server/discover" or _modern_version(params) is not None
        if modern:
            _validate_modern_request(params, transport_protocol_version)
        if method == "server/discover":
            return _modern_result(
                {
                    "supportedVersions": list(SUPPORTED_PROTOCOL_VERSIONS),
                    "capabilities": {"tools": {"listChanged": False}},
                    "instructions": _INSTRUCTIONS,
                    "ttlMs": 300_000,
                    "cacheScope": "public",
                }
            )
        if method == "tools/list":
            cursor = params.get("cursor")
            if cursor not in {None, ""}:
                raise _RpcError(-32602, "Tool-list cursor is invalid")
            result: dict[str, Any] = {
                "tools": [tool.schema() for tool in _TOOLS],
                "ttlMs": 300_000,
                "cacheScope": "public",
            }
            return _modern_result(result) if modern else result
        if method == "tools/call":
            result = await self._call_tool(params)
            return _modern_result(result) if modern else result
        if method == "ping" and not modern:
            return {}
        raise _RpcError(-32601, "Method not found")

    async def _call_tool(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(name, str) or name not in _TOOL_BY_NAME:
            raise _RpcError(-32602, "Unknown tool name")
        if not isinstance(arguments, dict):
            raise _RpcError(-32602, "Tool arguments must be an object")
        definition = _TOOL_BY_NAME[name]
        _reject_unknown_tool_argument_keys(definition, arguments)
        try:
            validated = definition.arguments.model_validate(arguments)
        except ValidationError as error:
            raise _RpcError(
                -32602,
                "Tool arguments failed validation",
                {"issues": _validation_issues(error)},
            ) from error
        try:
            structured = await self._execute(name, validated)
        except (MakoletError, ValueError) as error:
            return _tool_error(
                error.code if isinstance(error, MakoletError) else "invalid_value", str(error)
            )
        except Exception:
            return _tool_error("internal_error", "Tool execution failed")
        try:
            safe = definition.output.model_validate(structured).model_dump(
                mode="json",
                by_alias=True,
            )
        except Exception:
            return _tool_error("internal_error", "Tool execution failed")
        safe_text = _duplicated_json_text(safe, _MAXIMUM_TOOL_RESULT_BYTES)
        if safe_text is None:
            return _tool_error(
                "query_limit_exceeded",
                "Tool response exceeds the message size limit; request a smaller page",
            )
        result = {
            "content": [
                {
                    "type": "text",
                    "text": safe_text,
                }
            ],
            "structuredContent": safe,
            "isError": False,
        }
        if not compact_json_fits(result, _MAXIMUM_TOOL_RESULT_BYTES):
            return _tool_error(
                "query_limit_exceeded",
                "Tool response exceeds the message size limit; request a smaller page",
            )
        return result

    async def _execute(self, name: str, arguments: _Arguments) -> dict[str, Any]:
        if name == "search_products" and isinstance(arguments, _SearchProducts):
            return _page(
                await self._queries.search_products(
                    arguments.query, limit=arguments.limit, cursor=arguments.cursor
                )
            )
        if name == "get_product" and isinstance(arguments, _Product):
            value = await self._queries.get_product(arguments.product_id)
            if value is None:
                raise NotFoundError("Product was not found")
            return {"data": value}
        if name == "find_product_by_barcode" and isinstance(arguments, _Barcode):
            value = await self._queries.find_product_by_barcode(arguments.barcode)
            if value is None:
                raise NotFoundError("Product barcode was not found")
            return {"data": value}
        if name == "find_product_by_retailer_item_code" and isinstance(arguments, _RetailerItem):
            value = await self._queries.find_product_by_retailer_item_code(
                arguments.retailer_id,
                arguments.item_code,
                portal_id=arguments.portal_id,
            )
            if value is None:
                raise NotFoundError("Product retailer item code was not found for this retailer")
            return {"data": value}
        if name == "list_retailers" and isinstance(arguments, _PageArguments):
            return _page(
                await self._queries.list_retailers(limit=arguments.limit, cursor=arguments.cursor)
            )
        if name == "find_stores" and isinstance(arguments, _FindStores):
            return _page(
                await self._queries.find_stores(
                    query=arguments.query,
                    retailer_id=arguments.retailer_id,
                    city=arguments.city,
                    limit=arguments.limit,
                    cursor=arguments.cursor,
                )
            )
        if name == "get_current_prices" and isinstance(arguments, _Prices):
            return _page(
                await self._queries.current_prices(
                    arguments.product_id,
                    retailer_id=arguments.retailer_id,
                    store_id=arguments.store_id,
                    limit=arguments.limit,
                    cursor=arguments.cursor,
                )
            )
        if name == "compare_product_prices" and isinstance(arguments, _ComparePrices):
            return _page(
                await self._queries.compare_product_prices(
                    arguments.product_id,
                    retailer_id=arguments.retailer_id,
                    limit=arguments.limit,
                    cursor=arguments.cursor,
                )
            )
        if name == "get_price_history" and isinstance(arguments, _History):
            return _page(
                await self._queries.price_history(
                    arguments.product_id,
                    store_id=arguments.store_id,
                    since=arguments.since,
                    until=arguments.until,
                    limit=arguments.limit,
                    cursor=arguments.cursor,
                )
            )
        if name == "get_active_promotions" and isinstance(arguments, _Promotions):
            return _page(
                await self._queries.promotions(
                    product_id=arguments.product_id,
                    store_id=arguments.store_id,
                    at=arguments.at,
                    limit=arguments.limit,
                    cursor=arguments.cursor,
                )
            )
        if name == "get_promotion_history" and isinstance(arguments, _PromotionHistory):
            return _page(
                await self._queries.promotion_history(
                    product_id=arguments.product_id,
                    store_id=arguments.store_id,
                    since=arguments.since,
                    until=arguments.until,
                    limit=arguments.limit,
                    cursor=arguments.cursor,
                )
            )
        if name == "get_item_availability" and isinstance(arguments, _Availability):
            return _page(
                await self._queries.item_availability(
                    arguments.product_id,
                    store_id=arguments.store_id,
                    limit=arguments.limit,
                    cursor=arguments.cursor,
                )
            )
        if name == "get_data_freshness" and isinstance(arguments, _PageArguments):
            return _page(
                await self._queries.freshness(limit=arguments.limit, cursor=arguments.cursor)
            )
        if name == "get_source_status" and isinstance(arguments, _PageArguments):
            status = await self._queries.platform_status(
                limit=arguments.limit,
                cursor=arguments.cursor,
            )
            sources = _page(cast(Page, status["sources"]))
            return {"maintenance": status["maintenance"], **sources}
        raise _RpcError(-32602, "Tool arguments do not match the selected tool")


def create_mcp_http_app(
    server: MakoletMcpServer,
    *,
    allowed_origins: frozenset[str] = frozenset(),
    body_timeout_seconds: float = DEFAULT_HTTP_BODY_TIMEOUT_SECONDS,
    maximum_concurrency: int = DEFAULT_HTTP_MAXIMUM_CONCURRENCY,
) -> FastAPI:
    """Create the stateless Streamable HTTP endpoint for the latest protocol."""

    if not 0 < body_timeout_seconds <= 300:
        raise ValueError("MCP HTTP body timeout must be positive and at most 300 seconds")
    if not 1 <= maximum_concurrency <= 10_000:
        raise ValueError("MCP HTTP concurrency limit must be between 1 and 10,000")

    app = FastAPI(title="Makolet MCP", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.uvicorn_limit_concurrency = maximum_concurrency

    @app.post("/mcp")
    async def mcp_post(request: Request) -> Response:
        origin = request.headers.get("origin")
        if origin is not None and origin not in allowed_origins:
            return Response(status_code=403)
        media_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
        if media_type != "application/json":
            return Response(status_code=415)
        accepted_types = {
            value.partition(";")[0].strip().casefold()
            for value in request.headers.get("accept", "").split(",")
        }
        if not {"application/json", "text/event-stream"}.issubset(accepted_types):
            return Response(status_code=406)
        declared_length = _content_length(request)
        if declared_length is None:
            return Response(status_code=400)
        if declared_length > MAXIMUM_MESSAGE_BYTES:
            return Response(status_code=413)
        try:
            payload = await _bounded_request_body(
                request,
                timeout_seconds=body_timeout_seconds,
            )
        except TimeoutError:
            return Response(status_code=408)
        if payload is None:
            return Response(status_code=413)
        protocol_header = request.headers.get("mcp-protocol-version")
        request_id, method, tool_name, body_version = _request_metadata(payload)
        if body_version is not None or protocol_header == LATEST_PROTOCOL_VERSION:
            response_headers = {"MCP-Protocol-Version": body_version or LATEST_PROTOCOL_VERSION}
            if protocol_header != body_version:
                return JSONResponse(
                    _error_response(
                        request_id,
                        -32020,
                        "MCP-Protocol-Version header mismatch",
                    ),
                    status_code=400,
                    headers=response_headers,
                )
            if request.headers.get("mcp-method") != method:
                return JSONResponse(
                    _error_response(request_id, -32020, "Mcp-Method header mismatch"),
                    status_code=400,
                    headers=response_headers,
                )
            encoded_name = request.headers.get("mcp-name")
            if method == "tools/call" and _decode_mcp_header(encoded_name) != tool_name:
                return JSONResponse(
                    _error_response(request_id, -32020, "Mcp-Name header mismatch"),
                    status_code=400,
                    headers=response_headers,
                )
        response = await server.parse_and_handle(
            payload, transport_protocol_version=protocol_header
        )
        if response is None:
            return Response(status_code=202)
        error_code = _response_error_code(response)
        if error_code in {-32020, -32021, -32022}:
            status_code = 400
        elif error_code == -32601 and body_version is not None:
            status_code = 404
        else:
            status_code = 200
        headers = {"MCP-Protocol-Version": protocol_header} if protocol_header else None
        return JSONResponse(response, status_code=status_code, headers=headers)

    @app.get("/mcp")
    async def mcp_get() -> Response:
        return Response(status_code=405, headers={"Allow": "POST"})

    @app.delete("/mcp")
    async def mcp_delete() -> Response:
        return Response(status_code=405, headers={"Allow": "POST"})

    return app


async def serve_stdio(
    server: MakoletMcpServer,
    *,
    reader: BinaryIO | None = None,
    writer: BinaryIO | None = None,
) -> None:
    """Serve newline-delimited UTF-8 JSON-RPC without writing logs to stdout."""

    input_stream = reader or sys.stdin.buffer
    output_stream = writer or sys.stdout.buffer
    while line := await anyio.to_thread.run_sync(input_stream.readline, MAXIMUM_MESSAGE_BYTES + 1):
        response: dict[str, Any] | None
        if len(line) > MAXIMUM_MESSAGE_BYTES:
            response = _error_response(None, -32600, "Request exceeds the message size limit")
            terminate_transport = True
        else:
            response = await server.parse_and_handle(line)
            terminate_transport = False
        if response is None:
            continue
        encoded = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
        await anyio.to_thread.run_sync(output_stream.write, encoded)
        await anyio.to_thread.run_sync(output_stream.flush)
        if terminate_transport:
            return


def _content_length(request: Request) -> int | None:
    raw_value = request.headers.get("content-length")
    if raw_value is None:
        return 0
    try:
        value = int(raw_value, 10)
    except ValueError:
        return None
    return value if value >= 0 else None


async def _bounded_request_body(
    request: Request,
    *,
    timeout_seconds: float,
) -> bytes | None:
    body = bytearray()
    with anyio.fail_after(timeout_seconds):
        async for chunk in request.stream():
            if len(chunk) > MAXIMUM_MESSAGE_BYTES - len(body):
                return None
            body.extend(chunk)
    return bytes(body)


def _legacy_initialize(params: dict[str, Any]) -> dict[str, Any]:
    requested = params.get("protocolVersion")
    if requested not in LEGACY_PROTOCOL_VERSIONS:
        requested = LEGACY_PROTOCOL_VERSIONS[0]
    return {
        "protocolVersion": requested,
        "capabilities": {"tools": {"listChanged": False}},
        "serverInfo": _SERVER_INFO,
        "instructions": _INSTRUCTIONS,
    }


def _modern_version(params: dict[str, Any]) -> str | None:
    meta = params.get("_meta")
    if not isinstance(meta, dict):
        return None
    value = meta.get("io.modelcontextprotocol/protocolVersion")
    return value if isinstance(value, str) else None


def _validate_modern_request(
    params: dict[str, Any], transport_protocol_version: str | None
) -> None:
    meta = params.get("_meta")
    if not isinstance(meta, dict):
        raise _RpcError(-32602, "Modern MCP request requires _meta")
    version = meta.get("io.modelcontextprotocol/protocolVersion")
    capabilities = meta.get("io.modelcontextprotocol/clientCapabilities")
    if version != LATEST_PROTOCOL_VERSION:
        raise _RpcError(
            -32022,
            "Unsupported MCP protocol version",
            {"supported": list(SUPPORTED_PROTOCOL_VERSIONS), "requested": version},
        )
    if not isinstance(capabilities, dict):
        raise _RpcError(-32602, "Modern MCP request requires client capabilities")
    if transport_protocol_version is not None and transport_protocol_version != version:
        raise _RpcError(-32020, "Protocol header and request metadata differ")


def _modern_result(value: dict[str, Any]) -> dict[str, Any]:
    return {
        "resultType": "complete",
        "_meta": {"io.modelcontextprotocol/serverInfo": _SERVER_INFO},
        **value,
    }


def _tool_error(code: str, message: str) -> dict[str, Any]:
    safe_message = message[:1_000]
    structured = _ToolErrorOutput(
        error=_ToolErrorBody(
            code=code[:128] or "internal_error",
            message=safe_message,
        )
    ).model_dump(mode="json")
    return {
        "content": [{"type": "text", "text": safe_message}],
        "structuredContent": structured,
        "isError": True,
    }


def _duplicated_json_text(value: object, maximum_result_bytes: int) -> str | None:
    chunks: list[str] = []
    structured_bytes = 0
    escaped_text_bytes = 0
    encoder = json.JSONEncoder(
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    for chunk in encoder.iterencode(value):
        structured_bytes += len(chunk.encode("utf-8"))
        escaped_chunk = json.dumps(chunk, ensure_ascii=False)
        escaped_text_bytes += len(escaped_chunk[1:-1].encode("utf-8"))
        projected_bytes = (
            _DUPLICATED_TOOL_RESULT_OVERHEAD_BYTES
            + structured_bytes
            + escaped_text_bytes
            + len(b'""')
        )
        if projected_bytes > maximum_result_bytes:
            return None
        chunks.append(chunk)
    return "".join(chunks)


def _page(page: Page) -> dict[str, Any]:
    return {"items": list(page.items), "nextCursor": page.next_cursor}


def _validation_issues(error: ValidationError) -> list[dict[str, Any]]:
    if error.error_count() > _MAXIMUM_VALIDATION_ISSUES:
        return [
            {
                "location": ["arguments"],
                "message": "Tool arguments produced too many validation issues",
            }
        ]
    return [
        {"location": [str(part) for part in issue["loc"]], "message": issue["msg"]}
        for issue in error.errors(include_url=False, include_context=False, include_input=False)
    ]


def _reject_unknown_tool_argument_keys(
    definition: _ToolDefinition,
    arguments: dict[str, Any],
) -> None:
    known_fields = definition.arguments.model_fields
    if len(arguments) <= len(known_fields) and all(key in known_fields for key in arguments):
        return
    raise _RpcError(
        -32602,
        "Tool arguments failed validation",
        {
            "issues": [
                {
                    "location": ["arguments"],
                    "message": "Tool arguments contain unknown fields",
                }
            ]
        },
    )


def _error_response(request_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    response: dict[str, Any] = {"jsonrpc": "2.0", "error": error}
    if request_id is not None:
        response["id"] = request_id
    return response


def _safe_request_id(message: Any) -> str | int | None:
    if not isinstance(message, dict):
        return None
    value = message.get("id")
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    if len(str(value)) > _MAXIMUM_RESPONSE_REQUEST_ID_CHARACTERS:
        return None
    return value


def _request_metadata(
    payload: bytes,
) -> tuple[str | int | None, str | None, str | None, str | None]:
    try:
        value = _decode_json_payload(payload)
    except _InvalidJsonPayloadError:
        return None, None, None, None
    if isinstance(value, dict):
        request_id = _safe_request_id(value)
        method = value.get("method") if isinstance(value.get("method"), str) else None
        params = value.get("params")
        if isinstance(params, dict):
            name = params.get("name")
            return (
                request_id,
                method,
                name if isinstance(name, str) else None,
                _modern_version(params),
            )
        return request_id, method, None, None
    return None, None, None, None


def _decode_json_payload(payload: bytes) -> Any:
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise _InvalidJsonPayloadError from error
    _preflight_json_text(text)
    try:
        return json.loads(text)
    except (json.JSONDecodeError, RecursionError, ValueError) as error:
        raise _InvalidJsonPayloadError from error


def _preflight_json_text(text: str) -> None:
    depth = 0
    structural_tokens = 0
    scalar_characters = 0
    string_characters = 0
    in_string = False
    escaped = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
                string_characters = 0
                continue
            string_characters += 1
            if string_characters > _MAXIMUM_JSON_STRING_CHARACTERS:
                raise _InvalidJsonPayloadError
            continue

        if character == '"':
            in_string = True
            scalar_characters = 0
        elif character in "[{":
            depth += 1
            structural_tokens += 1
            scalar_characters = 0
            if depth > _MAXIMUM_JSON_DEPTH:
                raise _InvalidJsonPayloadError
        elif character in "]}":
            depth -= 1
            structural_tokens += 1
            scalar_characters = 0
            if depth < 0:
                raise _InvalidJsonPayloadError
        elif character in ",:":
            structural_tokens += 1
            scalar_characters = 0
        elif character.isspace():
            scalar_characters = 0
        else:
            scalar_characters += 1
            if scalar_characters > _MAXIMUM_JSON_SCALAR_CHARACTERS:
                raise _InvalidJsonPayloadError
        if structural_tokens > _MAXIMUM_JSON_STRUCTURAL_TOKENS:
            raise _InvalidJsonPayloadError
    if in_string or escaped or depth != 0:
        raise _InvalidJsonPayloadError


def _decode_mcp_header(value: str | None) -> str | None:
    if value is None:
        return None
    prefix = "=?base64?"
    suffix = "?="
    if value.startswith(prefix) and value.endswith(suffix):
        encoded = value[len(prefix) : -len(suffix)]
        try:
            return b64decode(encoded, validate=True).decode("utf-8")
        except Base64Error, UnicodeDecodeError, ValueError:
            return None
    if value != value.strip() or any(
        ord(character) < 0x20 or ord(character) > 0x7E for character in value
    ):
        return None
    return value


def _response_error_code(response: dict[str, Any]) -> int | None:
    error = response.get("error")
    if isinstance(error, dict) and isinstance(error.get("code"), int):
        return int(error["code"])
    return None
