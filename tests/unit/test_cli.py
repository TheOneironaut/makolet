from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest
from fastapi import FastAPI
from typer.core import TyperGroup
from typer.main import get_command
from typer.testing import CliRunner

import makolet.interfaces.cli as cli_module
from makolet.application.maintenance import REBUILD_CONFIRMATION_TOKEN
from makolet.application.models import (
    CatalogCandidateGenerationResult,
    NormalizedRebuildRun,
    Page,
    ReplayRangeResult,
)
from makolet.application.queries import QueryService
from makolet.application.worker import SourceHealth, Worker, WorkerSnapshot
from makolet.composition import (
    DatabaseOperations,
    DiagnosticOperations,
    ExportOperations,
    IngestionOperations,
    MatchingOperations,
    OperationalOperations,
    SourceOperations,
)
from makolet.config import MakoletSettings
from makolet.domain.errors import QueryLimitError, SourceAccessError
from makolet.interfaces.api import create_app
from makolet.interfaces.cli import CliRuntime, InterfaceServers, _human, _render, build_cli
from makolet.interfaces.mcp import MakoletMcpServer
from scripts.check_distribution import ADVERTISED_CLI_HELP_PATHS
from tests.fakes.ingestion import FixedClock

PRODUCT_ID = UUID("00000000-0000-0000-0000-000000000101")


def test_human_renderer_escapes_terminal_and_bidi_controls() -> None:
    rendered = _human(
        {
            "filename": "safe\x1b\r\n\x7f\u009b\ud800\u2028\u2029\u202ename.xml",
            "label": "מחירים בישראל",
        }
    )

    assert "\x1b" not in rendered
    assert "\r" not in rendered
    assert "\nname" not in rendered
    assert "\x7f" not in rendered
    assert "\u009b" not in rendered
    assert "\ud800" not in rendered
    assert "\u2028" not in rendered
    assert "\u2029" not in rendered
    assert "\u202e" not in rendered
    assert r"\u001b\u000d\u000a\u007f\u009b\ud800\u2028\u2029\u202e" in rendered
    assert "מחירים בישראל" in rendered


def test_json_renderer_is_one_line_and_preserves_escaped_unicode_semantics(
    capsys: pytest.CaptureFixture[str],
) -> None:
    unsafe = "safe\x7f\u009b\ud800\u2028\u2029\u202e"

    _render({"unsafe": unsafe, "label": "מחירים בישראל"}, json_output=True)

    rendered = capsys.readouterr().out
    assert len(rendered.splitlines()) == 1
    assert all(character not in rendered for character in "\x7f\u009b\ud800\u2028\u2029\u202e")
    assert "מחירים בישראל" in rendered
    assert json.loads(rendered) == {"unsafe": unsafe, "label": "מחירים בישראל"}


class StubQueries(QueryService):
    async def list_retailers(self, *, limit: int | None = None, cursor: str | None = None) -> Page:
        return Page(
            items=({"id": PRODUCT_ID, "name": "Retailer", "limit": limit},),
            next_cursor=cursor,
        )

    async def find_stores(
        self,
        *,
        query: str | None = None,
        retailer_id: UUID | None = None,
        city: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        return Page(
            items=(
                {
                    "id": PRODUCT_ID,
                    "query": query,
                    "retailer_id": retailer_id,
                    "city": city,
                    "limit": limit,
                },
            ),
            next_cursor=cursor,
        )

    async def search_products(
        self, query: str, *, limit: int | None = None, cursor: str | None = None
    ) -> Page:
        return Page(
            items=({"id": PRODUCT_ID, "name": query, "limit": limit},),
            next_cursor=cursor,
        )

    async def get_product(self, product_id: UUID) -> dict[str, object] | None:
        if product_id != PRODUCT_ID:
            return None
        return {"id": product_id, "name": "Milk"}

    async def find_product_by_barcode(self, barcode: str) -> dict[str, object] | None:
        if barcode == "000":
            return None
        return {"id": PRODUCT_ID, "name": "Milk", "barcode": barcode}

    async def find_product_by_retailer_item_code(
        self,
        retailer_id: UUID,
        item_code: str,
        *,
        portal_id: UUID | None = None,
    ) -> dict[str, object] | None:
        if item_code == "missing":
            return None
        return {
            "id": PRODUCT_ID,
            "name": "Milk",
            "retailer_id": retailer_id,
            "portal_id": portal_id,
            "retailer_item_code": item_code,
        }

    async def current_prices(
        self,
        product_id: UUID,
        *,
        retailer_id: UUID | None = None,
        store_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        return Page(items=({"product_id": product_id, "price": "5.90"},), next_cursor=None)

    async def compare_product_prices(
        self,
        product_id: UUID,
        *,
        retailer_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        return Page(
            items=(
                {
                    "product_id": product_id,
                    "retailer_id": retailer_id,
                    "price": "5.90",
                },
            ),
            next_cursor=cursor,
        )

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
        return Page(items=({"product_id": product_id, "observed_at": since},), next_cursor=None)

    async def promotions(
        self,
        *,
        product_id: UUID | None = None,
        store_id: UUID | None = None,
        at: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        return Page(items=({"promotion": "two for ten", "at": at},), next_cursor=None)

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
        return Page(
            items=(
                {
                    "promotion": "historical two for ten",
                    "product_id": product_id,
                    "store_id": store_id,
                    "since": since,
                    "until": until,
                },
            ),
            next_cursor=cursor,
        )

    async def item_availability(
        self,
        product_id: UUID,
        *,
        store_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        return Page(
            items=(
                {
                    "product_id": product_id,
                    "store_id": store_id,
                    "available": True,
                    "limit": limit,
                },
            ),
            next_cursor=cursor,
        )

    async def freshness(self, *, limit: int | None = None, cursor: str | None = None) -> Page:
        return Page(
            items=(
                {
                    "store_id": PRODUCT_ID,
                    "available_items": 400,
                    "observed_items": 1_000,
                    "item_probe_limit": 1_000,
                    "items_truncated": True,
                },
            ),
            next_cursor=None,
        )

    async def source_status(self, *, limit: int | None = None, cursor: str | None = None) -> Page:
        return Page(items=({"source_id": "alpha", "status": "healthy"},), next_cursor=None)

    async def maintenance_status(self) -> dict[str, object]:
        return {"active": False, "mode": "normal"}

    async def platform_status(
        self, *, limit: int | None = None, cursor: str | None = None
    ) -> dict[str, object]:
        return {
            "maintenance": await self.maintenance_status(),
            "sources": await self.source_status(limit=limit, cursor=cursor),
        }


class NeverCalledQueryRepository:
    async def search_products(self, *args: object, **kwargs: object) -> Page:
        del args, kwargs
        raise AssertionError("short product query reached repository")


@dataclass
class StubDatabaseOperations:
    migrated: bool = False

    async def migrate(self, *, revision: str = "head") -> dict[str, object]:
        self.migrated = True
        return {"revision": revision, "status": "ready"}

    async def status(self) -> dict[str, object]:
        return {"status": "ready", "migration_revision": "0001"}


@dataclass
class StubSourceOperations:
    failure: str | None = None

    async def list_sources(self) -> tuple[dict[str, object], ...]:
        self._maybe_fail()
        return ({"source_id": "alpha", "enabled": True},)

    async def inspect_source(self, source_id: str) -> dict[str, object]:
        self._maybe_fail()
        return {"source_id": source_id, "family": "fixture"}

    async def test_source(self, source_id: str) -> dict[str, object]:
        self._maybe_fail()
        return {"source_id": source_id, "files": 1}

    def _maybe_fail(self) -> None:
        if self.failure == "retryable":
            raise SourceAccessError("source is temporarily unavailable")
        if self.failure == "unexpected":
            raise RuntimeError("postgresql://user:secret@example.test/database")


@dataclass
class StubIngestionOperations:
    calls: list[str] = field(default_factory=list)

    async def ingest_source(self, source_id: str) -> dict[str, object]:
        self.calls.append(f"source:{source_id}")
        return {"source_id": source_id, "status": "completed"}

    async def ingest_retailer(self, retailer_id: str) -> dict[str, object]:
        self.calls.append(f"retailer:{retailer_id}")
        return {"retailer_id": retailer_id, "status": "completed"}

    async def ingest_all(self) -> dict[str, object]:
        self.calls.append("all")
        return {"status": "completed"}

    async def backfill(
        self,
        source_id: str,
        *,
        since: datetime,
        until: datetime,
        archive_only: bool = False,
    ) -> dict[str, object]:
        self.calls.append(f"backfill:{source_id}")
        return {
            "source_id": source_id,
            "since": since,
            "until": until,
            "archive_only": archive_only,
        }

    async def replay(self, source_file_id: UUID) -> dict[str, object]:
        self.calls.append(f"replay:{source_file_id}")
        return {"source_file_id": source_file_id, "replayed": True}

    async def replay_range(
        self,
        *,
        since: datetime,
        until: datetime,
        limit: int,
        cursor: str | None,
    ) -> ReplayRangeResult:
        self.calls.append(f"replay-range:{limit}")
        return ReplayRangeResult(since=since, until=until, files=(), next_cursor=cursor)

    async def rebuild_normalized(
        self,
        *,
        confirmation: str,
        requested_by: str,
    ) -> NormalizedRebuildRun:
        self.calls.append(f"rebuild:{confirmation}:{requested_by}")
        return _rebuild_run()

    async def resume_normalized_rebuild(
        self,
        rebuild_run_id: UUID,
    ) -> NormalizedRebuildRun:
        self.calls.append(f"resume-rebuild:{rebuild_run_id}")
        return _rebuild_run()

    async def normalized_rebuild_status(
        self,
        rebuild_run_id: UUID,
    ) -> NormalizedRebuildRun:
        self.calls.append(f"rebuild-status:{rebuild_run_id}")
        return _rebuild_run()


def _rebuild_run() -> NormalizedRebuildRun:
    return NormalizedRebuildRun(
        rebuild_run_id=PRODUCT_ID,
        status="completed",
        archive_cutoff_at=datetime(2026, 8, 12, tzinfo=UTC),
        source_files_total=1,
        source_files_completed=1,
    )


class StubOperationalOperations:
    async def failures(self, *, limit: int, cursor: str | None) -> Page:
        return Page(items=({"code": "source_access_error", "limit": limit},), next_cursor=None)

    async def list_quarantine(self, *, limit: int, cursor: str | None) -> Page:
        return Page(items=({"id": PRODUCT_ID, "reason": "malformed_document"},), next_cursor=None)

    async def inspect_quarantine(self, quarantine_id: UUID) -> dict[str, object]:
        return {"id": quarantine_id, "issues": 1}


@dataclass
class StubMatchingOperations:
    calls: list[str] = field(default_factory=list)

    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]:
        return {"source_file_id": source_file_id, "bootstrapped_items": 1}

    async def generate_candidates(
        self,
        *,
        cursor: str | None = None,
        item_limit: int | None = None,
        candidate_limit: int | None = None,
        review_threshold: Decimal | str = Decimal("0.65"),
    ) -> CatalogCandidateGenerationResult:
        self.calls.append(f"generate:{item_limit}:{candidate_limit}:{review_threshold}")
        return CatalogCandidateGenerationResult(1, 1, 2, cursor)

    async def list_candidates(
        self,
        *,
        status: object = "pending",
        retailer_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        selected_status = getattr(status, "value", status)
        self.calls.append(f"list:{selected_status}:{limit}")
        return Page(
            items=({"id": PRODUCT_ID, "status": selected_status},),
            next_cursor=cursor,
        )

    async def inspect_candidate(self, candidate_id: UUID) -> dict[str, object]:
        self.calls.append(f"inspect:{candidate_id}")
        return {"id": candidate_id, "status": "pending"}

    async def accept_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]:
        self.calls.append(f"accept:{candidate_id}:{reviewed_by}")
        return {"id": candidate_id, "status": "accepted", "reviewed_by": reviewed_by}

    async def reject_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]:
        self.calls.append(f"reject:{candidate_id}:{reviewed_by}")
        return {"id": candidate_id, "status": "rejected", "reviewed_by": reviewed_by}


@dataclass
class StubExporter:
    calls: list[Path] = field(default_factory=list)

    async def export_parquet(
        self,
        output: Path,
        *,
        since: datetime | None,
        until: datetime | None,
    ) -> dict[str, object]:
        self.calls.append(output)
        return {"format": "parquet", "output": output, "files": 1}


@dataclass
class StubDiagnostics:
    ok: bool = True

    async def doctor(self) -> dict[str, object]:
        return {"ok": self.ok, "checks": ({"name": "database", "ok": self.ok},)}


@dataclass
class StubWorkerBackend:
    calls: list[str] = field(default_factory=list)
    failed_source_ids: frozenset[str] = frozenset()

    async def recover_stale_jobs(self, *, stale_after: timedelta) -> int:
        return 0

    async def ingest_source(self, source_id: str) -> object:
        self.calls.append(source_id)
        if source_id in self.failed_source_ids:
            raise SourceAccessError(f"Source {source_id} is temporarily unavailable")
        return {"source_id": source_id}


class StubTelemetry:
    async def record_heartbeat(self, snapshot: WorkerSnapshot) -> None:
        return None

    async def record_source_health(self, health: SourceHealth) -> None:
        return None


@dataclass
class StubInterfaceServers:
    http_calls: list[tuple[str, int]] = field(default_factory=list)
    stdio_calls: int = 0

    async def http(self, application: FastAPI, *, host: str, port: int) -> None:
        self.http_calls.append((host, port))

    async def stdio(self, server: MakoletMcpServer) -> None:
        self.stdio_calls += 1

    def interfaces(self) -> InterfaceServers:
        return InterfaceServers(http=self.http, stdio=self.stdio)


@dataclass
class StubBenchmarkRunner:
    exit_code: int = 0
    failure: Exception | None = None
    calls: list[tuple[str, ...]] = field(default_factory=list)

    def __call__(self, arguments: Sequence[str]) -> int:
        self.calls.append(tuple(arguments))
        if self.failure is not None:
            raise self.failure
        return self.exit_code


@dataclass(frozen=True)
class StubRuntime:
    settings: MakoletSettings
    query_service: QueryService
    database_operations: DatabaseOperations
    diagnostics: DiagnosticOperations
    api_app: FastAPI
    metrics_app: FastAPI
    mcp_server: MakoletMcpServer
    source_operations: SourceOperations | None
    ingestion_operations: IngestionOperations | None
    operational_operations: OperationalOperations | None
    matching_operations: MatchingOperations
    worker: Worker | None
    exporter: ExportOperations | None


@dataclass
class StubRuntimeFactory:
    runtime: StubRuntime
    opens: int = 0
    closes: int = 0

    def __call__(self, settings: MakoletSettings) -> AbstractAsyncContextManager[CliRuntime]:
        return self.open()

    @asynccontextmanager
    async def open(self) -> AsyncIterator[CliRuntime]:
        self.opens += 1
        try:
            yield self.runtime
        finally:
            self.closes += 1


def _runtime(
    *,
    source_failure: str | None = None,
    worker_failed_source_ids: frozenset[str] = frozenset(),
    doctor_ok: bool = True,
    exporter_available: bool = True,
    query_service: QueryService | None = None,
) -> tuple[StubRuntime, StubRuntimeFactory]:
    queries = query_service or StubQueries.__new__(StubQueries)
    worker_backend = StubWorkerBackend(failed_source_ids=worker_failed_source_ids)
    settings = MakoletSettings(
        _env_file=None,
        environment="test",
        source_intervals_seconds={"alpha": 60},
        enabled_sources=("alpha",),
    )
    runtime = StubRuntime(
        settings=settings,
        query_service=queries,
        database_operations=StubDatabaseOperations(),
        diagnostics=StubDiagnostics(ok=doctor_ok),
        api_app=FastAPI(),
        metrics_app=FastAPI(),
        mcp_server=MakoletMcpServer(queries),
        source_operations=StubSourceOperations(failure=source_failure),
        ingestion_operations=StubIngestionOperations(),
        operational_operations=StubOperationalOperations(),
        matching_operations=StubMatchingOperations(),
        worker=Worker(
            worker_backend,
            StubTelemetry(),
            worker_id="cli-test",
            jitter=lambda _maximum: 0,
        ),
        exporter=StubExporter() if exporter_available else None,
    )
    return runtime, StubRuntimeFactory(runtime)


def test_help_exercises_every_required_command_without_opening_runtime() -> None:
    _runtime_value, factory = _runtime()
    runner = CliRunner()
    application = build_cli(factory)

    def command_paths(command: object, prefix: tuple[str, ...] = ()) -> set[tuple[str, ...]]:
        paths = {prefix}
        if isinstance(command, TyperGroup):
            for name, child in command.commands.items():
                paths.update(command_paths(child, (*prefix, name)))
        return paths

    assert command_paths(get_command(application)) == set(ADVERTISED_CLI_HELP_PATHS)
    for path in ADVERTISED_CLI_HELP_PATHS:
        result = runner.invoke(application, [*path, "--help"])
        assert result.exit_code == 0, (path, result.output)
    assert factory.opens == 0


def test_product_search_json_uses_runtime_and_closes_it() -> None:
    _runtime_value, factory = _runtime()
    result = CliRunner().invoke(
        build_cli(factory),
        ["products", "search", "milk", "--limit", "1", "--json"],
    )

    assert result.exit_code == 0
    assert '"name":"milk"' in result.stdout
    assert str(PRODUCT_ID) in result.stdout
    assert factory.opens == 1
    assert factory.closes == 1


def test_short_product_search_returns_stable_query_limit_error() -> None:
    query_service = QueryService(cast(Any, NeverCalledQueryRepository()), FixedClock())
    _runtime_value, factory = _runtime(query_service=query_service)

    result = CliRunner().invoke(
        build_cli(factory),
        ["products", "search", "ab", "--json"],
    )

    assert result.exit_code == 1
    assert '"code":"query_limit_exceeded"' in result.output
    assert "at least 3" in result.output
    assert factory.opens == factory.closes == 1


def test_product_identifier_overflow_returns_stable_cli_error() -> None:
    class IdentifierLimitQueries(StubQueries):
        async def get_product(self, product_id: UUID) -> dict[str, object] | None:
            del product_id
            raise QueryLimitError(
                "Product identifier count exceeds the 200-item public query limit"
            )

    _runtime_value, factory = _runtime(
        query_service=IdentifierLimitQueries.__new__(IdentifierLimitQueries)
    )

    result = CliRunner().invoke(
        build_cli(factory),
        ["products", "get", str(PRODUCT_ID), "--json"],
    )

    assert result.exit_code == 1
    assert '"code":"query_limit_exceeded"' in result.output
    assert "Product identifier count exceeds the 200-item public query limit" in result.output
    assert factory.opens == factory.closes == 1


@pytest.mark.asyncio
async def test_api_app_passes_exact_non_null_concurrency_to_uvicorn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class CapturingConfig:
        def __init__(self, application: FastAPI, **options: object) -> None:
            captured["application"] = application
            captured.update(options)

    class StubServer:
        def __init__(self, configuration: object) -> None:
            captured["configuration"] = configuration

        async def serve(self) -> None:
            captured["served"] = True

    async def ready() -> bool:
        return True

    monkeypatch.setattr("makolet.interfaces.cli.uvicorn.Config", CapturingConfig)
    monkeypatch.setattr("makolet.interfaces.cli.uvicorn.Server", StubServer)
    app = create_app(
        StubQueries.__new__(StubQueries),
        ready,
        maximum_concurrency=37,
    )

    await cli_module._serve_uvicorn(app, host="127.0.0.1", port=8000)

    assert captured["application"] is app
    assert captured["limit_concurrency"] == 37
    assert captured["served"] is True


def test_worker_once_and_backfill_delegate_to_application_boundaries() -> None:
    runtime, factory = _runtime()
    runner = CliRunner()

    worker_result = runner.invoke(
        build_cli(factory), ["ingest", "worker", "--once", "--source", "alpha", "--json"]
    )
    backfill_result = runner.invoke(
        build_cli(factory),
        [
            "ingest",
            "backfill",
            "alpha",
            "--since",
            "2026-01-01T00:00:00Z",
            "--until",
            "2026-01-02T00:00:00Z",
            "--json",
        ],
    )

    assert worker_result.exit_code == 0
    assert '"succeeded":true' in worker_result.stdout
    assert backfill_result.exit_code == 0
    assert isinstance(runtime.ingestion_operations, StubIngestionOperations)
    assert runtime.ingestion_operations.calls == ["backfill:alpha"]


def test_worker_once_renders_mixed_summary_and_exits_temporary_failure() -> None:
    _runtime_value, factory = _runtime(worker_failed_source_ids=frozenset({"broken"}))

    result = CliRunner().invoke(
        build_cli(factory),
        [
            "ingest",
            "worker",
            "--once",
            "--source",
            "alpha",
            "--source",
            "broken",
            "--json",
        ],
    )

    assert result.exit_code == 4
    assert '"source_id":"alpha","succeeded":true' in result.stdout
    assert '"source_id":"broken","succeeded":false' in result.stdout
    assert '"error_code":"source_access_error"' in result.stdout
    assert '"stale_jobs_recovered":0' in result.stdout


def test_successful_leaf_commands_delegate_through_the_shared_runtime() -> None:
    _runtime_value, factory = _runtime()
    runner = CliRunner()
    servers = StubInterfaceServers()
    application = build_cli(factory, servers=servers.interfaces())
    commands = (
        ("database", "migrate", "--revision", "head", "--json"),
        ("database", "status", "--json"),
        ("sources", "list", "--json"),
        ("sources", "inspect", "alpha", "--json"),
        ("sources", "test", "alpha", "--json"),
        ("ingest", "source", "alpha", "--json"),
        ("ingest", "retailer", "retailer-a", "--json"),
        ("ingest", "all", "--json"),
        ("ingest", "replay", str(PRODUCT_ID), "--json"),
        (
            "ingest",
            "replay-range",
            "--since",
            "2026-01-01T00:00:00Z",
            "--until",
            "2026-01-02T00:00:00Z",
            "--json",
        ),
        (
            "ingest",
            "rebuild-normalized",
            "--confirm",
            REBUILD_CONFIRMATION_TOKEN,
            "--requested-by",
            "cli-test",
            "--json",
        ),
        ("ingest", "resume-rebuild", str(PRODUCT_ID), "--json"),
        ("ingest", "rebuild-status", str(PRODUCT_ID), "--json"),
        ("status", "--json"),
        ("freshness", "--json"),
        ("source-status", "--json"),
        ("failures", "--json"),
        ("quarantine", "list", "--json"),
        ("quarantine", "inspect", str(PRODUCT_ID), "--json"),
        ("doctor", "--json"),
        ("matching", "generate", "--json"),
        ("matching", "list", "--json"),
        ("matching", "inspect", str(PRODUCT_ID), "--json"),
        (
            "matching",
            "accept",
            str(PRODUCT_ID),
            "--reviewed-by",
            "cli-reviewer",
            "--json",
        ),
        (
            "matching",
            "reject",
            str(PRODUCT_ID),
            "--reviewed-by",
            "cli-reviewer",
            "--json",
        ),
        ("products", "get", str(PRODUCT_ID), "--json"),
        ("retailers", "list", "--limit", "1", "--json"),
        (
            "stores",
            "find",
            "--query",
            "central",
            "--retailer-id",
            str(PRODUCT_ID),
            "--city",
            "Jerusalem",
            "--json",
        ),
        ("products", "find-barcode", "7290000000015", "--json"),
        (
            "products",
            "find-retailer-item",
            str(PRODUCT_ID),
            "SKU-1",
            "--portal-id",
            str(PRODUCT_ID),
            "--json",
        ),
        ("prices", "current", str(PRODUCT_ID), "--json"),
        ("prices", "compare", str(PRODUCT_ID), "--retailer-id", str(PRODUCT_ID), "--json"),
        (
            "prices",
            "history",
            str(PRODUCT_ID),
            "--since",
            "2026-01-01T00:00:00Z",
            "--until",
            "2026-01-02T00:00:00Z",
            "--json",
        ),
        ("promotions", "active", "--at", "2026-01-01T00:00:00Z", "--json"),
        (
            "promotions",
            "history",
            "--since",
            "2026-01-01T00:00:00Z",
            "--until",
            "2026-01-02T00:00:00Z",
            "--json",
        ),
        (
            "availability",
            "current",
            str(PRODUCT_ID),
            "--store-id",
            str(PRODUCT_ID),
            "--json",
        ),
        ("api", "serve"),
        ("mcp", "serve"),
        ("mcp", "serve", "--transport", "http"),
        ("export", "parquet", "exports", "--json"),
    )

    for command in commands:
        result = runner.invoke(application, list(command))
        assert result.exit_code == 0, (command, result.output)
    assert servers.stdio_calls == 1
    assert servers.http_calls == [("127.0.0.1", 8000), ("127.0.0.1", 8001)]


def test_freshness_json_preserves_the_bounded_probe_contract() -> None:
    _runtime_value, factory = _runtime()
    result = CliRunner().invoke(build_cli(factory), ["freshness", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout)["items"] == [
        {
            "store_id": str(PRODUCT_ID),
            "available_items": 400,
            "observed_items": 1_000,
            "item_probe_limit": 1_000,
            "items_truncated": True,
        }
    ]


def test_not_found_and_retryable_errors_have_stable_exit_codes() -> None:
    _runtime_value, factory = _runtime(source_failure="retryable")
    runner = CliRunner()

    missing = runner.invoke(
        build_cli(factory),
        ["products", "get", "00000000-0000-0000-0000-000000000999", "--json"],
    )
    retryable = runner.invoke(build_cli(factory), ["sources", "list", "--json"])

    assert missing.exit_code == 3
    assert '"code":"not_found"' in missing.output
    assert retryable.exit_code == 4
    assert '"code":"source_access_error"' in retryable.output


def test_history_range_errors_are_structured_and_nonzero() -> None:
    queries = QueryService(cast(Any, object()), FixedClock())
    _runtime_value, factory = _runtime(query_service=queries)
    runner = CliRunner()
    application = build_cli(factory)
    equal_range = ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")

    price = runner.invoke(
        application,
        [
            "prices",
            "history",
            str(PRODUCT_ID),
            "--since",
            equal_range[0],
            "--until",
            equal_range[1],
            "--json",
        ],
    )
    promotion = runner.invoke(
        application,
        [
            "promotions",
            "history",
            "--since",
            equal_range[0],
            "--until",
            equal_range[1],
            "--json",
        ],
    )
    one_sided_price = runner.invoke(
        application,
        [
            "prices",
            "history",
            str(PRODUCT_ID),
            "--since",
            equal_range[0],
            "--json",
        ],
    )
    one_sided_promotion = runner.invoke(
        application,
        ["promotions", "history", "--until", equal_range[1], "--json"],
    )

    for result in (price, promotion, one_sided_price, one_sided_promotion):
        assert result.exit_code == 1
        assert '"code":"domain_validation_error"' in result.output


def test_history_help_documents_paired_or_default_bounds() -> None:
    _runtime_value, factory = _runtime()
    application = build_cli(factory)
    runner = CliRunner()

    price_help = runner.invoke(application, ["prices", "history", "--help"])
    promotion_help = runner.invoke(application, ["promotions", "history", "--help"])

    assert price_help.exit_code == promotion_help.exit_code == 0
    assert "pair with --until" in price_help.output
    assert "pair with --since" in promotion_help.output


def test_unexpected_error_never_echoes_exception_or_secret() -> None:
    _runtime_value, factory = _runtime(source_failure="unexpected")

    result = CliRunner().invoke(build_cli(factory), ["sources", "list", "--json"])

    assert result.exit_code == 1
    assert "unexpected_error" in result.output
    assert "postgresql://" not in result.output
    assert "user:secret" not in result.output
    assert "Traceback" not in result.output


def test_doctor_and_missing_capability_return_configuration_exit() -> None:
    _runtime_value, factory = _runtime(doctor_ok=False)
    _unavailable_runtime, unavailable_factory = _runtime(exporter_available=False)
    runner = CliRunner()

    doctor = runner.invoke(build_cli(factory), ["doctor", "--json"])
    unavailable = runner.invoke(
        build_cli(unavailable_factory), ["export", "parquet", "exports", "--json"]
    )

    assert doctor.exit_code == 5
    assert '"ok":false' in doctor.output
    assert unavailable.exit_code == 5
    assert "capability_unavailable" in unavailable.output


def test_benchmark_delegates_arguments_without_opening_runtime(tmp_path: Path) -> None:
    _runtime_value, factory = _runtime()
    benchmark_runner = StubBenchmarkRunner()
    output = tmp_path / "result with spaces.json"
    database_url = "postgresql+asyncpg://benchmark.invalid/db;not-a-command"

    result = CliRunner().invoke(
        build_cli(factory, benchmark_runner=benchmark_runner),
        [
            "benchmark",
            "run",
            "--quick",
            "--scenario",
            "database",
            "--database-url",
            database_url,
            "--database-confirmation",
            "makolet_benchmark",
            "--output",
            str(output),
            "--keep-schema",
        ],
    )

    assert result.exit_code == 0, result.output
    assert benchmark_runner.calls == [
        (
            "--profile",
            "quick",
            "--scenario",
            "database",
            "--database-url",
            database_url,
            "--database-confirmation",
            "makolet_benchmark",
            "--output",
            str(output.resolve()),
            "--keep-schema",
        )
    ]
    assert factory.opens == 0
    assert factory.closes == 0


def test_standard_parser_benchmark_delegates_only_relevant_options() -> None:
    _runtime_value, factory = _runtime()
    benchmark_runner = StubBenchmarkRunner()

    result = CliRunner().invoke(
        build_cli(factory, benchmark_runner=benchmark_runner),
        ["benchmark", "run", "--standard", "--scenario", "parser"],
    )

    assert result.exit_code == 0, result.output
    assert benchmark_runner.calls == [("--profile", "standard", "--scenario", "parser")]
    assert factory.opens == 0


def test_benchmark_profiles_are_required_and_mutually_exclusive() -> None:
    _runtime_value, factory = _runtime()
    benchmark_runner = StubBenchmarkRunner()
    application = build_cli(factory, benchmark_runner=benchmark_runner)
    runner = CliRunner()

    missing = runner.invoke(application, ["benchmark", "run"])
    conflicting = runner.invoke(application, ["benchmark", "run", "--quick", "--standard"])
    invalid_scenario = runner.invoke(
        application,
        ["benchmark", "run", "--quick", "--scenario", "unbounded"],
    )

    assert missing.exit_code == 2
    assert "choose exactly one of --quick or --standard" in missing.output
    assert conflicting.exit_code == 2
    assert "--quick and --standard are mutually exclusive" in conflicting.output
    assert invalid_scenario.exit_code == 2
    assert "Invalid value for '--scenario'" in invalid_scenario.output
    assert benchmark_runner.calls == []
    assert factory.opens == 0


def test_parser_benchmark_rejects_database_only_options() -> None:
    _runtime_value, factory = _runtime()
    benchmark_runner = StubBenchmarkRunner()
    application = build_cli(factory, benchmark_runner=benchmark_runner)
    runner = CliRunner()

    database_url = runner.invoke(
        application,
        [
            "benchmark",
            "run",
            "--quick",
            "--scenario",
            "parser",
            "--database-url",
            "postgresql://benchmark.invalid/db",
        ],
    )
    keep_schema = runner.invoke(
        application,
        ["benchmark", "run", "--quick", "--scenario", "parser", "--keep-schema"],
    )
    confirmation = runner.invoke(
        application,
        [
            "benchmark",
            "run",
            "--quick",
            "--scenario",
            "parser",
            "--database-confirmation",
            "makolet_benchmark",
        ],
    )

    assert database_url.exit_code == 2
    assert "--database-url is only valid for database scenarios" in database_url.output
    assert keep_schema.exit_code == 2
    assert "--keep-schema is only valid for database scenarios" in keep_schema.output
    assert confirmation.exit_code == 2
    assert "--database-confirmation is only valid" in confirmation.output
    assert benchmark_runner.calls == []


def test_benchmark_preserves_nonzero_runner_exit_code() -> None:
    _runtime_value, factory = _runtime()
    benchmark_runner = StubBenchmarkRunner(exit_code=23)

    result = CliRunner().invoke(
        build_cli(factory, benchmark_runner=benchmark_runner),
        ["benchmark", "run", "--quick"],
    )

    assert result.exit_code == 23
    assert factory.opens == 0


def test_benchmark_unexpected_error_is_secret_safe() -> None:
    _runtime_value, factory = _runtime()
    benchmark_runner = StubBenchmarkRunner(
        failure=RuntimeError("postgresql://user:secret@benchmark.invalid/database")
    )

    result = CliRunner().invoke(
        build_cli(factory, benchmark_runner=benchmark_runner),
        ["benchmark", "run", "--quick"],
    )

    assert result.exit_code == 1
    assert "benchmark_failed" in result.output
    assert "postgresql://" not in result.output
    assert "user:secret" not in result.output
    assert factory.opens == 0


def test_default_benchmark_runner_reports_missing_database_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _runtime_value, factory = _runtime()
    monkeypatch.delenv("MAKOLET_BENCHMARK_DATABASE_URL", raising=False)

    result = CliRunner().invoke(
        build_cli(factory),
        ["benchmark", "run", "--quick", "--scenario", "database"],
    )

    assert result.exit_code == 2
    assert "requires --database-url or MAKOLET_BENCHMARK_DATABASE_URL" in result.output
    assert factory.opens == 0


def test_default_benchmark_runner_explains_missing_optional_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _runtime_value, factory = _runtime()
    for module_name in (
        "benchmarks.run",
        "benchmarks.database",
        "benchmarks.measure",
        "benchmarks.parser",
    ):
        monkeypatch.delitem(sys.modules, module_name, raising=False)
    monkeypatch.setitem(sys.modules, "psutil", None)

    result = CliRunner().invoke(
        build_cli(factory),
        ["benchmark", "run", "--quick", "--scenario", "parser"],
    )

    assert result.exit_code == 5
    assert "Benchmark support requires the optional benchmark dependency" in result.output
    assert "makolet[benchmark]" in result.output
    assert "uv sync --all-groups --frozen" in result.output
    assert factory.opens == 0


def test_cli_watchdog_bounds_asyncio_runner_shutdown(tmp_path: Path) -> None:
    script = textwrap.dedent(
        """
        import asyncio
        from contextlib import asynccontextmanager

        import makolet.interfaces.cli as cli
        from makolet.application.worker import ProcessShutdownWatchdog

        cli.load_settings = lambda: object()

        @asynccontextmanager
        async def runtime_factory(_settings):
            yield object()

        async def stubborn_task():
            while True:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    continue

        class FakeWorker:
            shutdown_grace_seconds = 0.05

            async def run_forever(
                self,
                _schedules,
                *,
                stop_event,
                install_signal_handlers,
            ):
                assert not install_signal_handlers
                await stop_event.wait()
                asyncio.create_task(stubborn_task())

        async def server(_app, *, host, port):
            del host, port

        watchdog = ProcessShutdownWatchdog(exit_code=77)

        async def operation(_runtime):
            await cli._run_worker_with_metrics(
                FakeWorker(),
                (),
                None,
                server,
                host="127.0.0.1",
                port=0,
                shutdown_watchdog=watchdog,
                shutdown_overhead_seconds=0.05,
            )
            return {"status": "stopped"}

        cli._execute(
            runtime_factory,
            operation,
            json_output=True,
            render_result=False,
            shutdown_watchdog=watchdog,
        )
        raise AssertionError("asyncio.run unexpectedly completed")
        """
    )
    environment = os.environ.copy()
    began = time.monotonic()

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and authored test script
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        env=environment,
        text=True,
        timeout=5,
    )

    assert completed.returncode == 77, completed.stderr
    assert time.monotonic() - began < 3


def test_worker_signal_arms_watchdog_before_stalled_uvicorn_shutdown(
    tmp_path: Path,
) -> None:
    ready_path = tmp_path / "uvicorn-ready"
    script = textwrap.dedent(
        f"""
        import asyncio
        from contextlib import asynccontextmanager
        from pathlib import Path

        from fastapi import FastAPI

        import makolet.interfaces.cli as cli
        from makolet.application.worker import ProcessShutdownWatchdog

        cli.load_settings = lambda: object()
        ready_path = Path({str(ready_path)!r})

        @asynccontextmanager
        async def runtime_factory(_settings):
            yield object()

        @asynccontextmanager
        async def stalled_lifespan(_application):
            ready_path.write_text("ready", encoding="ascii")
            try:
                yield
            finally:
                while True:
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        continue

        application = FastAPI(lifespan=stalled_lifespan)

        class FakeWorker:
            shutdown_grace_seconds = 0.05

            async def run_forever(
                self,
                _schedules,
                *,
                stop_event,
                install_signal_handlers,
            ):
                assert not install_signal_handlers
                await stop_event.wait()

        watchdog = ProcessShutdownWatchdog(exit_code=78)

        async def operation(_runtime):
            await cli._run_worker_with_metrics(
                FakeWorker(),
                (),
                application,
                cli._serve_uvicorn,
                host="127.0.0.1",
                port=0,
                shutdown_watchdog=watchdog,
                shutdown_overhead_seconds=0.05,
            )
            return {{"status": "stopped"}}

        cli._execute(
            runtime_factory,
            operation,
            json_output=True,
            render_result=False,
            shutdown_watchdog=watchdog,
        )
        raise AssertionError("stalled Uvicorn shutdown unexpectedly completed")
        """
    )
    creation_flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0
    process = subprocess.Popen(  # noqa: S603 - fixed interpreter and authored test script
        [sys.executable, "-c", script],
        creationflags=creation_flags,
        cwd=tmp_path,
        env=os.environ.copy(),
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5
        while not ready_path.exists() and time.monotonic() < deadline:
            if process.poll() is not None:
                break
            time.sleep(0.02)
        assert ready_path.exists(), process.stderr.read() if process.stderr else ""
        began = time.monotonic()
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.send_signal(signal.SIGTERM)
        return_code = process.wait(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)

    assert return_code == 78
    assert time.monotonic() - began < 3
