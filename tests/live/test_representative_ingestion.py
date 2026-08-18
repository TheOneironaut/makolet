"""One bounded live source file through production ingestion and public reads."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

import httpx
import pytest
from typer.testing import CliRunner

import makolet.interfaces.cli as cli_module
from makolet.composition import open_runtime
from makolet.config import MakoletSettings
from makolet.interfaces.cli import CliRuntime, build_cli
from makolet.interfaces.mcp import LATEST_PROTOCOL_VERSION, create_mcp_http_app
from tests.live.conftest import (
    MAXIMUM_ARCHIVE_BYTES,
    MAXIMUM_PUBLISHER_REQUESTS,
    LiveWorkflow,
)

pytestmark = [pytest.mark.live, pytest.mark.integration, pytest.mark.e2e]

_PROVENANCE_FIELDS = (
    "source_file_id",
    "source_document_type",
    "source_timestamp",
    "source_discovered_at",
    "content_sha256",
    "portal_id",
    "portal_key",
    "retailer_id",
    "retailer_key",
)


def test_representative_live_file_completed_archived_matched_and_replayed(
    representative_live_workflow: LiveWorkflow,
) -> None:
    workflow = representative_live_workflow

    assert workflow.price_record_count > 0
    assert workflow.replay_history_events == 0
    assert 2 <= workflow.publisher_request_count <= MAXIMUM_PUBLISHER_REQUESTS
    assert 0 < workflow.archived_byte_count <= MAXIMUM_ARCHIVE_BYTES
    assert workflow.settings.archive_maximum_object_bytes == MAXIMUM_ARCHIVE_BYTES
    assert workflow.settings.ingestion_maximum_files_per_source_run == 1
    assert workflow.archive_object_key.startswith("sha256/")
    assert len(workflow.content_sha256) == 64


def test_cli_api_mcp_and_query_service_return_identical_live_provenance(
    representative_live_workflow: LiveWorkflow,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow = representative_live_workflow
    direct_row, api_row, mcp_row = asyncio.run(_read_non_cli_interfaces(workflow))

    def runtime_factory(settings: MakoletSettings) -> AbstractAsyncContextManager[CliRuntime]:
        del settings
        return _open_cli_runtime(workflow.settings)

    monkeypatch.setattr(cli_module, "load_settings", lambda: workflow.settings)
    result = CliRunner().invoke(
        build_cli(runtime_factory),
        ["prices", "current", str(workflow.product_id), "--limit", "1", "--json"],
    )
    assert result.exit_code == 0, result.stdout
    cli_body = json.loads(result.stdout)
    cli_row = _single_mapping(cli_body.get("items"), "CLI")

    expected = _provenance(direct_row)
    assert _provenance(api_row) == expected
    assert _provenance(mcp_row) == expected
    assert _provenance(cli_row) == expected
    assert expected["source_file_id"] == str(workflow.source_file_id)
    assert expected["content_sha256"] == workflow.content_sha256


async def _read_non_cli_interfaces(
    workflow: LiveWorkflow,
) -> tuple[Mapping[str, object], Mapping[str, object], Mapping[str, object]]:
    async with open_runtime(workflow.settings) as runtime:
        direct_page = await runtime.query_service.current_prices(workflow.product_id, limit=1)
        direct_row = _single_mapping(direct_page.items, "query service")

        api_transport = httpx.ASGITransport(app=runtime.api_app)
        async with httpx.AsyncClient(
            transport=api_transport,
            base_url="http://makolet.test",
        ) as api_client:
            api_response = await api_client.get(
                f"/api/v1/products/{workflow.product_id}/prices",
                params={"limit": 1},
            )
        assert api_response.status_code == 200
        api_body = api_response.json()
        api_row = _single_mapping(api_body.get("items"), "HTTP API")

        mcp_app = create_mcp_http_app(runtime.mcp_server)
        mcp_transport = httpx.ASGITransport(app=mcp_app)
        async with httpx.AsyncClient(
            transport=mcp_transport,
            base_url="http://makolet.test",
        ) as mcp_client:
            mcp_row = await _mcp_current_price(mcp_client, workflow.product_id)
    return direct_row, api_row, mcp_row


@asynccontextmanager
async def _open_cli_runtime(settings: MakoletSettings) -> AsyncIterator[CliRuntime]:
    async with open_runtime(settings) as runtime:
        yield runtime


async def _mcp_current_price(
    client: httpx.AsyncClient,
    product_id: UUID,
) -> Mapping[str, object]:
    method = "tools/call"
    tool_name = "get_current_prices"
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": {
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": LATEST_PROTOCOL_VERSION,
                "io.modelcontextprotocol/clientCapabilities": {},
                "io.modelcontextprotocol/clientInfo": {
                    "name": "live-acceptance",
                    "version": "1",
                },
            },
            "name": tool_name,
            "arguments": {"product_id": str(product_id), "limit": 1},
        },
    }
    response = await client.post(
        "/mcp",
        json=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": LATEST_PROTOCOL_VERSION,
            "Mcp-Method": method,
            "Mcp-Name": tool_name,
        },
    )
    assert response.status_code == 200
    body = response.json()
    result = body.get("result")
    assert isinstance(result, Mapping)
    assert result.get("isError") is False
    structured = result.get("structuredContent")
    assert isinstance(structured, Mapping)
    return _single_mapping(structured.get("items"), "MCP")


def _single_mapping(value: object, interface: str) -> Mapping[str, object]:
    if not isinstance(value, list | tuple) or len(value) != 1:
        raise AssertionError(f"{interface} did not return exactly one current price")
    row = value[0]
    if not isinstance(row, Mapping):
        raise TypeError(f"{interface} returned an invalid current-price row")
    return cast_mapping(row)


def cast_mapping(value: Mapping[Any, Any]) -> Mapping[str, object]:
    if any(not isinstance(key, str) for key in value):
        raise AssertionError("public query row contains a non-string key")
    return {str(key): item for key, item in value.items()}


def _provenance(row: Mapping[str, object]) -> dict[str, str | None]:
    missing = set(_PROVENANCE_FIELDS).difference(row)
    if missing:
        raise AssertionError("public current-price row omitted required provenance fields")
    return {field: _canonical_value(field, row[field]) for field in _PROVENANCE_FIELDS}


def _canonical_value(field: str, value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _canonical_datetime(value)
    if field in {"source_timestamp", "source_discovered_at"} and isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _canonical_datetime(parsed)
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, UUID | Decimal):
        return str(value)
    if isinstance(value, str | int | float | bool):
        return str(value)
    raise AssertionError("public provenance field has an unsupported value type")


def _canonical_datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise AssertionError("public provenance timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
