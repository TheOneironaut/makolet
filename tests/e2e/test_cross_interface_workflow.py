"""One clean-room source workflow observed through every public query interface."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

from makolet.adapters.observability.logging import configure_logging
from makolet.interfaces.api import create_app
from makolet.interfaces.cli import build_cli
from makolet.interfaces.mcp import (
    LATEST_PROTOCOL_VERSION,
    MakoletMcpServer,
    create_mcp_http_app,
)
from tests.e2e.conftest import RealQueryServices, SeededWorkflow, WorkflowEvent

pytestmark = [pytest.mark.integration, pytest.mark.e2e]
_BARCODE = "4006381333931"


def test_exact_bytes_are_archived_before_parse_and_replay_is_history_idempotent(
    seeded_workflow: SeededWorkflow,
) -> None:
    assert seeded_workflow.discovered_remote_ids == (
        "stores-20260811",
        "price-full-20260810",
        "price-delta-20260811",
        "price-delta-duplicate-20260811",
        "promo-full-20260811",
    )
    assert seeded_workflow.events == (
        WorkflowEvent("stores-20260811", "discover"),
        WorkflowEvent("price-full-20260810", "discover"),
        WorkflowEvent("price-delta-20260811", "discover"),
        WorkflowEvent("price-delta-duplicate-20260811", "discover"),
        WorkflowEvent("promo-full-20260811", "discover"),
        WorkflowEvent("stores-20260811", "download"),
        WorkflowEvent("stores-20260811", "archive"),
        WorkflowEvent("stores-20260811", "parse"),
        WorkflowEvent("stores-20260811", "stage"),
        WorkflowEvent("stores-20260811", "apply"),
        WorkflowEvent("price-full-20260810", "download"),
        WorkflowEvent("price-full-20260810", "archive"),
        WorkflowEvent("price-full-20260810", "parse"),
        WorkflowEvent("price-full-20260810", "stage"),
        WorkflowEvent("price-full-20260810", "apply"),
        WorkflowEvent("price-delta-20260811", "download"),
        WorkflowEvent("price-delta-20260811", "archive"),
        WorkflowEvent("price-delta-20260811", "parse"),
        WorkflowEvent("price-delta-20260811", "stage"),
        WorkflowEvent("price-delta-20260811", "apply"),
        WorkflowEvent("price-delta-20260811", "parse"),
        WorkflowEvent("price-delta-20260811", "stage"),
        WorkflowEvent("price-delta-20260811", "apply"),
        WorkflowEvent("price-delta-duplicate-20260811", "download"),
        WorkflowEvent("price-delta-duplicate-20260811", "archive"),
        WorkflowEvent("price-delta-duplicate-20260811", "parse"),
        WorkflowEvent("price-delta-duplicate-20260811", "stage"),
        WorkflowEvent("price-delta-duplicate-20260811", "apply"),
        WorkflowEvent("promo-full-20260811", "download"),
        WorkflowEvent("promo-full-20260811", "archive"),
        WorkflowEvent("promo-full-20260811", "parse"),
        WorkflowEvent("promo-full-20260811", "stage"),
        WorkflowEvent("promo-full-20260811", "apply"),
    )
    for archived in seeded_workflow.archives:
        stored = (seeded_workflow.archive_root / archived.object_key).read_bytes()
        assert stored == archived.payload
        assert archived.sha256 == hashlib.sha256(archived.payload).hexdigest()

    assert seeded_workflow.initial_price.apply is not None
    # Initial application opens one price-history and one availability-history row.
    assert seeded_workflow.initial_price.apply.history_events == 2
    assert seeded_workflow.changed_price.apply is not None
    assert seeded_workflow.changed_price.apply.updated == 1
    assert seeded_workflow.changed_price.apply.history_events == 1
    assert seeded_workflow.duplicate_price.duplicate is True
    assert seeded_workflow.duplicate_price.apply is not None
    assert seeded_workflow.duplicate_price.apply.unchanged == 1
    assert seeded_workflow.duplicate_price.apply.history_events == 0
    assert seeded_workflow.promotion.apply is not None
    assert seeded_workflow.promotion.apply.inserted >= 1
    assert seeded_workflow.replayed_price.replayed is True
    assert seeded_workflow.replayed_price.apply is not None
    assert seeded_workflow.replayed_price.apply.unchanged == 1
    assert seeded_workflow.replayed_price.apply.history_events == 0
    assert seeded_workflow.archives[2].object_key == seeded_workflow.archives[3].object_key


async def test_query_service_reads_the_ingested_active_promotion_from_postgres(
    seeded_workflow: SeededWorkflow,
    real_queries: RealQueryServices,
) -> None:
    promotions = await real_queries.queries.promotions(
        product_id=seeded_workflow.product_id,
        store_id=seeded_workflow.store_id,
        limit=10,
    )

    assert len(promotions.items) == 1
    assert promotions.items[0]["source_promotion_id"] == "P-100"
    assert str(promotions.items[0]["discounted_price"]) == "30.0000"


async def test_http_api_reads_prices_history_and_promotion_from_postgres(
    seeded_workflow: SeededWorkflow,
    real_queries: RealQueryServices,
) -> None:
    async def ready() -> bool:
        await real_queries.database.health()
        return True

    app = create_app(real_queries.queries, ready)
    transport = httpx.ASGITransport(app=app)
    product_id = str(seeded_workflow.product_id)
    async with httpx.AsyncClient(transport=transport, base_url="http://makolet.test") as client:
        barcode = await client.get(
            f"/api/v1/barcodes/{_BARCODE}", headers={"X-Request-ID": "e2e-api"}
        )
        search = await client.get("/api/v1/products/search", params={"query": "espresso"})
        current = await client.get(f"/api/v1/products/{product_id}/prices")
        history = await client.get(f"/api/v1/products/{product_id}/history")
        promotions = await client.get(
            "/api/v1/promotions",
            params={
                "product_id": product_id,
                "store_id": str(seeded_workflow.store_id),
                "at": seeded_workflow.query_at.isoformat(),
            },
        )

    assert barcode.status_code == search.status_code == current.status_code == 200
    assert history.status_code == promotions.status_code == 200
    assert barcode.headers["x-request-id"] == "e2e-api"
    assert barcode.json()["data"]["id"] == product_id
    assert search.json()["items"][0]["id"] == product_id
    assert current.json()["items"][0]["item_price"] == "21.5000"
    assert [row["item_price"] for row in history.json()["items"]] == [
        "21.5000",
        "19.9000",
    ]
    assert promotions.json()["items"][0]["source_promotion_id"] == "P-100"
    assert promotions.json()["items"][0]["discounted_price"] == "30.0000"


def test_cli_reads_the_same_product_price_history_and_promotion(
    seeded_workflow: SeededWorkflow,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAKOLET_ENVIRONMENT", "test")
    monkeypatch.setenv("MAKOLET_DATABASE_URL", seeded_workflow.database_url)
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKEND", "local")
    monkeypatch.setenv("MAKOLET_ARCHIVE_ROOT", str(seeded_workflow.archive_root))
    monkeypatch.delenv("MAKOLET_ENABLED_SOURCES", raising=False)
    monkeypatch.delenv("MAKOLET_SOURCE_INTERVALS_SECONDS", raising=False)
    runner = CliRunner()
    application = build_cli()
    product_id = str(seeded_workflow.product_id)

    try:
        search = runner.invoke(application, ["products", "search", "espresso", "--json"])
        current = runner.invoke(application, ["prices", "current", product_id, "--json"])
        history = runner.invoke(application, ["prices", "history", product_id, "--json"])
        promotions = runner.invoke(
            application,
            [
                "promotions",
                "active",
                "--product-id",
                product_id,
                "--store-id",
                str(seeded_workflow.store_id),
                "--at",
                seeded_workflow.query_at.isoformat(),
                "--json",
            ],
        )
    finally:
        # CliRunner closes its captured stderr after each invocation. Restore a
        # process-stable handler before later in-process API/MCP tests emit logs.
        configure_logging()

    assert search.exit_code == current.exit_code == history.exit_code == promotions.exit_code == 0
    assert json.loads(search.stdout)["items"][0]["id"] == product_id
    assert json.loads(current.stdout)["items"][0]["item_price"] == "21.5000"
    assert [row["item_price"] for row in json.loads(history.stdout)["items"]] == [
        "21.5000",
        "19.9000",
    ]
    assert json.loads(promotions.stdout)["items"][0]["source_promotion_id"] == "P-100"
    assert json.loads(promotions.stdout)["items"][0]["discounted_price"] == "30.0000"


async def test_mcp_http_tools_read_the_same_prices_history_and_promotion(
    seeded_workflow: SeededWorkflow,
    real_queries: RealQueryServices,
) -> None:
    app = create_mcp_http_app(MakoletMcpServer(real_queries.queries))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://makolet.test") as client:
        product = await _mcp_call(client, "find_product_by_barcode", {"barcode": _BARCODE}, 1)
        current = await _mcp_call(
            client,
            "get_current_prices",
            {"product_id": str(seeded_workflow.product_id)},
            2,
        )
        history = await _mcp_call(
            client,
            "get_price_history",
            {"product_id": str(seeded_workflow.product_id)},
            3,
        )
        promotions = await _mcp_call(
            client,
            "get_active_promotions",
            {
                "product_id": str(seeded_workflow.product_id),
                "store_id": str(seeded_workflow.store_id),
                "at": seeded_workflow.query_at.isoformat(),
            },
            4,
        )

    assert product["data"]["id"] == str(seeded_workflow.product_id)
    assert current["items"][0]["item_price"] == "21.5000"
    assert [row["item_price"] for row in history["items"]] == ["21.5000", "19.9000"]
    assert promotions["items"][0]["source_promotion_id"] == "P-100"
    assert promotions["items"][0]["discounted_price"] == "30.0000"


async def _mcp_call(
    client: httpx.AsyncClient,
    name: str,
    arguments: Mapping[str, object],
    request_id: int,
) -> dict[str, Any]:
    method = "tools/call"
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": {
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": LATEST_PROTOCOL_VERSION,
                "io.modelcontextprotocol/clientCapabilities": {},
                "io.modelcontextprotocol/clientInfo": {"name": "e2e", "version": "1"},
            },
            "name": name,
            "arguments": dict(arguments),
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
            "Mcp-Name": name,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["result"]["isError"] is False
    structured = body["result"]["structuredContent"]
    assert isinstance(structured, dict)
    return structured
