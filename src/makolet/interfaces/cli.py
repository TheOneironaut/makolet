"""One discoverable, secret-safe command surface for the Makolet platform."""

from __future__ import annotations

import asyncio
import json
import signal
import threading
import unicodedata
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import (
    AbstractAsyncContextManager,
    asynccontextmanager,
    contextmanager,
    suppress,
)
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum, StrEnum
from pathlib import Path
from typing import Annotated, Protocol
from uuid import UUID

import typer
import uvicorn
from fastapi import FastAPI

from makolet.application.catalog_matching import CandidateStatus
from makolet.application.maintenance import REBUILD_CONFIRMATION_TOKEN
from makolet.application.models import Page
from makolet.application.worker import (
    ProcessShutdownWatchdog,
    SourceSchedule,
    Worker,
    WorkerRunSummary,
)
from makolet.composition import (
    CapabilityUnavailableError,
    DatabaseOperations,
    DiagnosticOperations,
    ExportOperations,
    IngestionOperations,
    MatchingOperations,
    OperationalOperations,
    RuntimeOperationError,
    SourceOperations,
    open_runtime,
)
from makolet.config import ConfigurationError, MakoletSettings, load_settings
from makolet.domain.errors import MakoletError, NotFoundError
from makolet.interfaces.mcp import MakoletMcpServer, create_mcp_http_app, serve_stdio

EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_NOT_FOUND = 3
EXIT_TEMPORARY = 4
EXIT_CONFIGURATION = 5
_PROCESS_SHUTDOWN_OVERHEAD_SECONDS = 5.0

JsonOption = Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON.")]
LimitOption = Annotated[int, typer.Option(min=1, max=200, help="Maximum result count.")]
CursorOption = Annotated[str | None, typer.Option(help="Opaque pagination cursor.")]


class McpTransport(StrEnum):
    STDIO = "stdio"
    HTTP = "http"


class BenchmarkScenario(StrEnum):
    ALL = "all"
    PARSER = "parser"
    DATABASE = "database"


class QueryCommands(Protocol):
    async def list_retailers(
        self, *, limit: int | None = None, cursor: str | None = None
    ) -> Page: ...

    async def find_stores(
        self,
        *,
        query: str | None = None,
        retailer_id: UUID | None = None,
        city: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page: ...

    async def search_products(
        self, query: str, *, limit: int | None = None, cursor: str | None = None
    ) -> Page: ...

    async def get_product(self, product_id: UUID) -> dict[str, object] | None: ...

    async def find_product_by_barcode(self, barcode: str) -> dict[str, object] | None: ...

    async def find_product_by_retailer_item_code(
        self,
        retailer_id: UUID,
        item_code: str,
        *,
        portal_id: UUID | None = None,
    ) -> dict[str, object] | None: ...

    async def current_prices(
        self,
        product_id: UUID,
        *,
        retailer_id: UUID | None = None,
        store_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page: ...

    async def compare_product_prices(
        self,
        product_id: UUID,
        *,
        retailer_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page: ...

    async def price_history(
        self,
        product_id: UUID,
        *,
        store_id: UUID | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page: ...

    async def promotions(
        self,
        *,
        product_id: UUID | None = None,
        store_id: UUID | None = None,
        at: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page: ...

    async def promotion_history(
        self,
        *,
        product_id: UUID | None = None,
        store_id: UUID | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page: ...

    async def item_availability(
        self,
        product_id: UUID,
        *,
        store_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page: ...

    async def freshness(self, *, limit: int | None = None, cursor: str | None = None) -> Page: ...

    async def source_status(
        self, *, limit: int | None = None, cursor: str | None = None
    ) -> Page: ...

    async def platform_status(
        self, *, limit: int | None = None, cursor: str | None = None
    ) -> dict[str, object]: ...


class CliRuntime(Protocol):
    @property
    def settings(self) -> MakoletSettings: ...

    @property
    def query_service(self) -> QueryCommands: ...

    @property
    def database_operations(self) -> DatabaseOperations: ...

    @property
    def diagnostics(self) -> DiagnosticOperations: ...

    @property
    def api_app(self) -> FastAPI: ...

    @property
    def metrics_app(self) -> FastAPI: ...

    @property
    def mcp_server(self) -> MakoletMcpServer: ...

    @property
    def source_operations(self) -> SourceOperations | None: ...

    @property
    def ingestion_operations(self) -> IngestionOperations | None: ...

    @property
    def operational_operations(self) -> OperationalOperations | None: ...

    @property
    def matching_operations(self) -> MatchingOperations: ...

    @property
    def worker(self) -> Worker | None: ...

    @property
    def exporter(self) -> ExportOperations | None: ...


class RuntimeFactory(Protocol):
    def __call__(self, settings: MakoletSettings) -> AbstractAsyncContextManager[CliRuntime]: ...


class HttpServer(Protocol):
    async def __call__(self, application: FastAPI, *, host: str, port: int) -> None: ...


class StdioServer(Protocol):
    async def __call__(self, server: MakoletMcpServer) -> None: ...


class BenchmarkRunner(Protocol):
    def __call__(self, arguments: Sequence[str]) -> int: ...


@dataclass(frozen=True, slots=True)
class InterfaceServers:
    http: HttpServer
    stdio: StdioServer


@asynccontextmanager
async def _default_runtime_factory(settings: MakoletSettings) -> AsyncIterator[CliRuntime]:
    async with open_runtime(settings) as runtime:
        yield runtime


def build_cli(
    runtime_factory: RuntimeFactory = _default_runtime_factory,
    *,
    servers: InterfaceServers | None = None,
    benchmark_runner: BenchmarkRunner | None = None,
) -> typer.Typer:
    """Build an isolated CLI, allowing tests and alternate deployments to inject adapters."""

    selected_servers = servers or InterfaceServers(http=_serve_uvicorn, stdio=serve_stdio)
    selected_benchmark_runner = benchmark_runner or _default_benchmark_runner
    application = typer.Typer(
        name="makolet",
        help="Collect, archive, replay, and query supermarket transparency data.",
        no_args_is_help=True,
        pretty_exceptions_enable=False,
    )
    database = typer.Typer(help="Manage and inspect the PostgreSQL schema.", no_args_is_help=True)
    sources = typer.Typer(help="Discover and diagnose configured publishers.", no_args_is_help=True)
    ingest = typer.Typer(help="Run ingestion, replay, backfill, or workers.", no_args_is_help=True)
    quarantine = typer.Typer(help="Inspect quarantined source files.", no_args_is_help=True)
    retailers = typer.Typer(help="List configured retailers.", no_args_is_help=True)
    stores = typer.Typer(help="Find and filter stores.", no_args_is_help=True)
    products = typer.Typer(help="Search and inspect canonical products.", no_args_is_help=True)
    matching = typer.Typer(
        help="Generate and review staged catalog match candidates.",
        no_args_is_help=True,
    )
    prices = typer.Typer(help="Query current and historical prices.", no_args_is_help=True)
    availability = typer.Typer(help="Query current item availability.", no_args_is_help=True)
    promotions = typer.Typer(help="Query active and historical promotions.", no_args_is_help=True)
    api = typer.Typer(help="Serve the read-only HTTP API.", no_args_is_help=True)
    mcp = typer.Typer(help="Serve the read-only MCP interface.", no_args_is_help=True)
    export = typer.Typer(help="Export open analytical datasets.", no_args_is_help=True)
    benchmark = typer.Typer(
        help=(
            "Run deterministic parser and PostgreSQL scale benchmarks; "
            "requires the optional benchmark dependency."
        ),
        no_args_is_help=True,
    )

    application.add_typer(database, name="database")
    application.add_typer(sources, name="sources")
    application.add_typer(ingest, name="ingest")
    application.add_typer(quarantine, name="quarantine")
    application.add_typer(retailers, name="retailers")
    application.add_typer(stores, name="stores")
    application.add_typer(products, name="products")
    application.add_typer(matching, name="matching")
    application.add_typer(prices, name="prices")
    application.add_typer(availability, name="availability")
    application.add_typer(promotions, name="promotions")
    application.add_typer(api, name="api")
    application.add_typer(mcp, name="mcp")
    application.add_typer(export, name="export")
    application.add_typer(benchmark, name="benchmark")

    @database.command("migrate")
    def database_migrate(
        revision: Annotated[str, typer.Option(help="Alembic revision or 'head'.")] = "head",
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.database_operations.migrate(revision=revision),
            json_output=json_output,
        )

    @database.command("status")
    def database_status(json_output: JsonOption = False) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.database_operations.status(),
            json_output=json_output,
        )

    @sources.command("list")
    def sources_list(json_output: JsonOption = False) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.source_operations, "source adapters").list_sources()

        _execute(runtime_factory, operation, json_output=json_output)

    @sources.command("inspect")
    def sources_inspect(source_id: str, json_output: JsonOption = False) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.source_operations, "source adapters").inspect_source(
                source_id
            )

        _execute(runtime_factory, operation, json_output=json_output)

    @sources.command("test")
    def sources_test(source_id: str, json_output: JsonOption = False) -> None:
        """Perform one bounded listing request without ingesting source files."""

        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.source_operations, "source adapters").test_source(
                source_id
            )

        _execute(runtime_factory, operation, json_output=json_output)

    @ingest.command("source")
    def ingest_source(source_id: str, json_output: JsonOption = False) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.ingestion_operations, "ingestion").ingest_source(
                source_id
            )

        _execute(runtime_factory, operation, json_output=json_output)

    @ingest.command("retailer")
    def ingest_retailer(retailer_id: str, json_output: JsonOption = False) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.ingestion_operations, "ingestion").ingest_retailer(
                retailer_id
            )

        _execute(runtime_factory, operation, json_output=json_output)

    @ingest.command("all")
    def ingest_all(json_output: JsonOption = False) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.ingestion_operations, "ingestion").ingest_all()

        _execute(runtime_factory, operation, json_output=json_output)

    @ingest.command("backfill")
    def ingest_backfill(
        source_id: str,
        since: Annotated[str, typer.Option(help="Inclusive ISO-8601 timestamp.")],
        until: Annotated[str, typer.Option(help="Inclusive ISO-8601 timestamp.")],
        archive_only: Annotated[
            bool,
            typer.Option(
                "--archive-only",
                help=(
                    "Download and immutably archive without applying; include the files "
                    "in the next normalized rebuild."
                ),
            ),
        ] = False,
        json_output: JsonOption = False,
    ) -> None:
        start = _timestamp(since, "since")
        end = _timestamp(until, "until")
        if start > end:
            raise typer.BadParameter("--since must not be after --until")

        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.ingestion_operations, "ingestion").backfill(
                source_id,
                since=start,
                until=end,
                archive_only=archive_only,
            )

        _execute(runtime_factory, operation, json_output=json_output)

    @ingest.command("replay")
    def ingest_replay(source_file_id: UUID, json_output: JsonOption = False) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.ingestion_operations, "ingestion").replay(source_file_id)

        _execute(runtime_factory, operation, json_output=json_output)

    @ingest.command("replay-range")
    def ingest_replay_range(
        since: Annotated[
            str,
            typer.Option(help="Inclusive timezone-aware archive timestamp."),
        ],
        until: Annotated[
            str,
            typer.Option(help="Exclusive timezone-aware archive timestamp."),
        ],
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        start = _timestamp(since, "since")
        end = _timestamp(until, "until")

        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.ingestion_operations, "ingestion").replay_range(
                since=start,
                until=end,
                limit=limit,
                cursor=cursor,
            )

        _execute(runtime_factory, operation, json_output=json_output)

    @ingest.command("rebuild-normalized")
    def ingest_rebuild_normalized(
        confirmation: Annotated[
            str,
            typer.Option(
                "--confirm",
                help=f"Required destructive acknowledgement: {REBUILD_CONFIRMATION_TOKEN}",
            ),
        ],
        requested_by: Annotated[
            str,
            typer.Option(
                "--requested-by",
                help="Auditable operator or service label (not a credential).",
            ),
        ],
        json_output: JsonOption = False,
    ) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(
                runtime.ingestion_operations,
                "normalized rebuild",
            ).rebuild_normalized(
                confirmation=confirmation,
                requested_by=requested_by,
            )

        _execute(runtime_factory, operation, json_output=json_output)

    @ingest.command("resume-rebuild")
    def ingest_resume_rebuild(
        rebuild_run_id: UUID,
        json_output: JsonOption = False,
    ) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(
                runtime.ingestion_operations,
                "normalized rebuild",
            ).resume_normalized_rebuild(rebuild_run_id)

        _execute(runtime_factory, operation, json_output=json_output)

    @ingest.command("rebuild-status")
    def ingest_rebuild_status(
        rebuild_run_id: UUID,
        json_output: JsonOption = False,
    ) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(
                runtime.ingestion_operations,
                "normalized rebuild",
            ).normalized_rebuild_status(rebuild_run_id)

        _execute(runtime_factory, operation, json_output=json_output)

    @ingest.command("worker")
    def ingest_worker(
        source_ids: Annotated[
            list[str] | None,
            typer.Option("--source", help="Source to schedule; repeat for multiple sources."),
        ] = None,
        once: Annotated[bool, typer.Option(help="Run selected sources once, then exit.")] = False,
        json_output: JsonOption = False,
    ) -> None:
        shutdown_watchdog = None if once else ProcessShutdownWatchdog(exit_code=EXIT_TEMPORARY)

        async def operation(runtime: CliRuntime) -> object:
            worker = _require(runtime.worker, "worker")
            selected = tuple(source_ids or runtime.settings.configured_source_ids())
            if not selected:
                raise ConfigurationError("No worker sources are configured")
            if once:
                return await worker.run_once(selected)
            schedules = tuple(
                SourceSchedule(
                    source_id=source_id,
                    interval=runtime.settings.source_interval(source_id),
                    jitter_ratio=runtime.settings.worker_jitter_ratio,
                )
                for source_id in selected
            )
            await _run_worker_with_metrics(
                worker,
                schedules,
                runtime.metrics_app,
                selected_servers.http,
                host=runtime.settings.worker_metrics_host,
                port=runtime.settings.worker_metrics_port,
                shutdown_watchdog=shutdown_watchdog,
            )
            return {"status": "stopped"}

        _execute(
            runtime_factory,
            operation,
            json_output=json_output,
            failure_predicate=lambda result: (
                isinstance(result, WorkerRunSummary)
                and any(not outcome.succeeded for outcome in result.outcomes)
            ),
            failure_exit_code=EXIT_TEMPORARY,
            shutdown_watchdog=shutdown_watchdog,
        )

    @application.command("status")
    def status(
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.platform_status(limit=limit, cursor=cursor),
            json_output=json_output,
        )

    @application.command("freshness")
    def freshness(
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.freshness(limit=limit, cursor=cursor),
            json_output=json_output,
        )

    @application.command("source-status")
    def source_status(
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.source_status(limit=limit, cursor=cursor),
            json_output=json_output,
        )

    @application.command("failures")
    def failures(
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.operational_operations, "failure inspection").failures(
                limit=limit,
                cursor=cursor,
            )

        _execute(runtime_factory, operation, json_output=json_output)

    @quarantine.command("list")
    def quarantine_list(
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(
                runtime.operational_operations, "quarantine inspection"
            ).list_quarantine(limit=limit, cursor=cursor)

        _execute(runtime_factory, operation, json_output=json_output)

    @quarantine.command("inspect")
    def quarantine_inspect(quarantine_id: UUID, json_output: JsonOption = False) -> None:
        async def operation(runtime: CliRuntime) -> object:
            return await _require(
                runtime.operational_operations, "quarantine inspection"
            ).inspect_quarantine(quarantine_id)

        _execute(runtime_factory, operation, json_output=json_output)

    @application.command("doctor")
    def doctor(json_output: JsonOption = False) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.diagnostics.doctor(),
            json_output=json_output,
            failure_predicate=lambda result: (
                isinstance(result, Mapping) and result.get("ok") is False
            ),
            failure_exit_code=EXIT_CONFIGURATION,
        )

    @matching.command("generate")
    def matching_generate(
        item_limit: Annotated[
            int,
            typer.Option(min=1, max=200, help="Maximum retailer items in this keyset page."),
        ] = 50,
        candidate_limit: Annotated[
            int,
            typer.Option(min=1, max=200, help="Maximum blocked products scored per item."),
        ] = 50,
        review_threshold: Annotated[
            float,
            typer.Option(min=0.0, max=1.0, help="Minimum review score."),
        ] = 0.65,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.matching_operations.generate_candidates(
                cursor=cursor,
                item_limit=item_limit,
                candidate_limit=candidate_limit,
                review_threshold=str(review_threshold),
            ),
            json_output=json_output,
        )

    @matching.command("list")
    def matching_list(
        status: Annotated[CandidateStatus, typer.Option()] = CandidateStatus.PENDING,
        retailer_id: Annotated[UUID | None, typer.Option()] = None,
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.matching_operations.list_candidates(
                status=status,
                retailer_id=retailer_id,
                limit=limit,
                cursor=cursor,
            ),
            json_output=json_output,
        )

    @matching.command("inspect")
    def matching_inspect(candidate_id: UUID, json_output: JsonOption = False) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.matching_operations.inspect_candidate(candidate_id),
            json_output=json_output,
        )

    @matching.command("accept")
    def matching_accept(
        candidate_id: UUID,
        reviewed_by: Annotated[
            str,
            typer.Option(help="Required human or system auditor label."),
        ],
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.matching_operations.accept_candidate(
                candidate_id,
                reviewed_by=reviewed_by,
            ),
            json_output=json_output,
        )

    @matching.command("reject")
    def matching_reject(
        candidate_id: UUID,
        reviewed_by: Annotated[
            str,
            typer.Option(help="Required human or system auditor label."),
        ],
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.matching_operations.reject_candidate(
                candidate_id,
                reviewed_by=reviewed_by,
            ),
            json_output=json_output,
        )

    @retailers.command("list")
    def retailers_list(
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.list_retailers(limit=limit, cursor=cursor),
            json_output=json_output,
        )

    @stores.command("find")
    def stores_find(
        query: Annotated[str | None, typer.Option(help="Normalized name or address text.")] = None,
        retailer_id: Annotated[UUID | None, typer.Option()] = None,
        city: Annotated[str | None, typer.Option(help="Normalized city text.")] = None,
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.find_stores(
                query=query,
                retailer_id=retailer_id,
                city=city,
                limit=limit,
                cursor=cursor,
            ),
            json_output=json_output,
        )

    @products.command("search")
    def products_search(
        query: str,
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.search_products(
                query, limit=limit, cursor=cursor
            ),
            json_output=json_output,
        )

    @products.command("get")
    def products_get(product_id: UUID, json_output: JsonOption = False) -> None:
        async def operation(runtime: CliRuntime) -> object:
            product = await runtime.query_service.get_product(product_id)
            if product is None:
                raise NotFoundError("Product was not found")
            return {"data": product}

        _execute(runtime_factory, operation, json_output=json_output)

    @products.command("find-barcode")
    def products_find_barcode(barcode: str, json_output: JsonOption = False) -> None:
        async def operation(runtime: CliRuntime) -> object:
            product = await runtime.query_service.find_product_by_barcode(barcode)
            if product is None:
                raise NotFoundError("Product barcode was not found")
            return {"data": product}

        _execute(runtime_factory, operation, json_output=json_output)

    @products.command("find-retailer-item")
    def products_find_retailer_item(
        retailer_id: UUID,
        item_code: str,
        portal_id: Annotated[
            UUID | None,
            typer.Option(
                help=(
                    "Source portal UUID. Required when this retailer uses the same item "
                    "code in more than one portal."
                )
            ),
        ] = None,
        json_output: JsonOption = False,
    ) -> None:
        async def operation(runtime: CliRuntime) -> object:
            product = await runtime.query_service.find_product_by_retailer_item_code(
                retailer_id,
                item_code,
                portal_id=portal_id,
            )
            if product is None:
                raise NotFoundError("Product retailer item code was not found for this retailer")
            return {"data": product}

        _execute(runtime_factory, operation, json_output=json_output)

    @prices.command("current")
    def prices_current(
        product_id: UUID,
        retailer_id: Annotated[UUID | None, typer.Option()] = None,
        store_id: Annotated[UUID | None, typer.Option()] = None,
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.current_prices(
                product_id,
                retailer_id=retailer_id,
                store_id=store_id,
                limit=limit,
                cursor=cursor,
            ),
            json_output=json_output,
        )

    @prices.command("compare")
    def prices_compare(
        product_id: UUID,
        retailer_id: Annotated[UUID | None, typer.Option()] = None,
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.compare_product_prices(
                product_id,
                retailer_id=retailer_id,
                limit=limit,
                cursor=cursor,
            ),
            json_output=json_output,
        )

    @prices.command("history")
    def prices_history(
        product_id: UUID,
        store_id: Annotated[UUID | None, typer.Option()] = None,
        since: Annotated[
            str | None,
            typer.Option(
                help=(
                    "Inclusive ISO-8601 timestamp; pair with --until or omit both for "
                    "trailing 366 days."
                )
            ),
        ] = None,
        until: Annotated[
            str | None,
            typer.Option(
                help=(
                    "Exclusive ISO-8601 timestamp; pair with --since or omit both for "
                    "trailing 366 days."
                )
            ),
        ] = None,
        limit: Annotated[int, typer.Option(min=1, max=1_000)] = 200,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        start = _timestamp(since, "since") if since is not None else None
        end = _timestamp(until, "until") if until is not None else None
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.price_history(
                product_id,
                store_id=store_id,
                since=start,
                until=end,
                limit=limit,
                cursor=cursor,
            ),
            json_output=json_output,
        )

    @promotions.command("active")
    def promotions_active(
        product_id: Annotated[UUID | None, typer.Option()] = None,
        store_id: Annotated[UUID | None, typer.Option()] = None,
        at: Annotated[str | None, typer.Option(help="ISO-8601 observation timestamp.")] = None,
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        selected_time = _timestamp(at, "at") if at is not None else None
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.promotions(
                product_id=product_id,
                store_id=store_id,
                at=selected_time,
                limit=limit,
                cursor=cursor,
            ),
            json_output=json_output,
        )

    @promotions.command("history")
    def promotions_history(
        product_id: Annotated[UUID | None, typer.Option()] = None,
        store_id: Annotated[UUID | None, typer.Option()] = None,
        since: Annotated[
            str | None,
            typer.Option(
                help=(
                    "Inclusive ISO-8601 timestamp; pair with --until or omit both for "
                    "trailing 366 days."
                )
            ),
        ] = None,
        until: Annotated[
            str | None,
            typer.Option(
                help=(
                    "Exclusive ISO-8601 timestamp; pair with --since or omit both for "
                    "trailing 366 days."
                )
            ),
        ] = None,
        limit: Annotated[int, typer.Option(min=1, max=1_000)] = 200,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        start = _timestamp(since, "since") if since is not None else None
        end = _timestamp(until, "until") if until is not None else None
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.promotion_history(
                product_id=product_id,
                store_id=store_id,
                since=start,
                until=end,
                limit=limit,
                cursor=cursor,
            ),
            json_output=json_output,
        )

    @availability.command("current")
    def availability_current(
        product_id: UUID,
        store_id: Annotated[UUID | None, typer.Option()] = None,
        limit: LimitOption = 50,
        cursor: CursorOption = None,
        json_output: JsonOption = False,
    ) -> None:
        _execute(
            runtime_factory,
            lambda runtime: runtime.query_service.item_availability(
                product_id,
                store_id=store_id,
                limit=limit,
                cursor=cursor,
            ),
            json_output=json_output,
        )

    @api.command("serve")
    def api_serve(
        host: Annotated[str | None, typer.Option(help="Bind host override.")] = None,
        port: Annotated[int | None, typer.Option(min=1, max=65_535)] = None,
    ) -> None:
        async def operation(runtime: CliRuntime) -> object:
            await selected_servers.http(
                runtime.api_app,
                host=host or runtime.settings.api_host,
                port=port or runtime.settings.api_port,
            )
            return {"status": "stopped"}

        _execute(runtime_factory, operation, json_output=False)

    @mcp.command("serve")
    def mcp_serve(
        transport: Annotated[McpTransport, typer.Option()] = McpTransport.STDIO,
        host: Annotated[str | None, typer.Option(help="HTTP bind host override.")] = None,
        port: Annotated[int | None, typer.Option(min=1, max=65_535)] = None,
    ) -> None:
        async def operation(runtime: CliRuntime) -> object:
            if transport is McpTransport.STDIO:
                await selected_servers.stdio(runtime.mcp_server)
            else:
                mcp_app = create_mcp_http_app(
                    runtime.mcp_server,
                    allowed_origins=frozenset(runtime.settings.mcp_allowed_origins),
                    body_timeout_seconds=runtime.settings.mcp_http_body_timeout_seconds,
                    maximum_concurrency=runtime.settings.mcp_http_maximum_concurrency,
                )
                await selected_servers.http(
                    mcp_app,
                    host=host or runtime.settings.mcp_host,
                    port=port or runtime.settings.mcp_port,
                )
            return {"status": "stopped"}

        _execute(runtime_factory, operation, json_output=False, render_result=False)

    @export.command("parquet")
    def export_parquet(
        output: Annotated[Path, typer.Argument(help="Destination directory.")],
        since: Annotated[str | None, typer.Option(help="Inclusive ISO-8601 timestamp.")] = None,
        until: Annotated[str | None, typer.Option(help="Inclusive ISO-8601 timestamp.")] = None,
        json_output: JsonOption = False,
    ) -> None:
        start = _timestamp(since, "since") if since is not None else None
        end = _timestamp(until, "until") if until is not None else None
        if start is not None and end is not None and start > end:
            raise typer.BadParameter("--since must not be after --until")

        async def operation(runtime: CliRuntime) -> object:
            return await _require(runtime.exporter, "Parquet export").export_parquet(
                output.resolve(),
                since=start,
                until=end,
            )

        _execute(runtime_factory, operation, json_output=json_output)

    @benchmark.command("run")
    def benchmark_run(
        quick: Annotated[
            bool,
            typer.Option("--quick", help="Run the diagnostic 10,000-record profile."),
        ] = False,
        standard: Annotated[
            bool,
            typer.Option(
                "--standard",
                help="Run the one-million-record scale-acceptance profile.",
            ),
        ] = False,
        scenario: Annotated[
            BenchmarkScenario,
            typer.Option(help="Limit the run to one bounded scenario."),
        ] = BenchmarkScenario.ALL,
        database_url: Annotated[
            str | None,
            typer.Option(
                help=("Isolated PostgreSQL URL; alternatively set MAKOLET_BENCHMARK_DATABASE_URL.")
            ),
        ] = None,
        database_confirmation: Annotated[
            str | None,
            typer.Option(
                help=(
                    "Exact isolated database name; alternatively set "
                    "MAKOLET_BENCHMARK_DATABASE_CONFIRM."
                )
            ),
        ] = None,
        output: Annotated[
            Path | None,
            typer.Option(
                dir_okay=False,
                resolve_path=True,
                help="JSON result file; defaults under benchmarks/results/.",
            ),
        ] = None,
        keep_schema: Annotated[
            bool,
            typer.Option(help="Retain the isolated database schema for plan inspection."),
        ] = False,
    ) -> None:
        if quick and standard:
            raise typer.BadParameter("--quick and --standard are mutually exclusive")
        if not quick and not standard:
            raise typer.BadParameter("choose exactly one of --quick or --standard")
        if scenario is BenchmarkScenario.PARSER and database_url is not None:
            raise typer.BadParameter("--database-url is only valid for database scenarios")
        if scenario is BenchmarkScenario.PARSER and database_confirmation is not None:
            raise typer.BadParameter("--database-confirmation is only valid for database scenarios")
        if scenario is BenchmarkScenario.PARSER and keep_schema:
            raise typer.BadParameter("--keep-schema is only valid for database scenarios")

        arguments = [
            "--profile",
            "quick" if quick else "standard",
            "--scenario",
            scenario.value,
        ]
        if database_url is not None:
            arguments.extend(("--database-url", database_url))
        if database_confirmation is not None:
            arguments.extend(("--database-confirmation", database_confirmation))
        if output is not None:
            arguments.extend(("--output", str(output)))
        if keep_schema:
            arguments.append("--keep-schema")
        _execute_benchmark(selected_benchmark_runner, arguments)

    return application


def _default_benchmark_runner(arguments: Sequence[str]) -> int:
    try:
        from benchmarks.run import main as benchmark_main
    except ModuleNotFoundError as error:
        if error.name == "psutil":
            raise ConfigurationError(
                "Benchmark support requires the optional benchmark dependency; "
                "install 'makolet[benchmark]' or run 'uv sync --all-groups --frozen'"
            ) from None
        raise
    return benchmark_main(arguments)


def _execute_benchmark(runner: BenchmarkRunner, arguments: Sequence[str]) -> None:
    try:
        exit_code = runner(arguments)
    except KeyboardInterrupt:
        raise typer.Exit(130) from None
    except ConfigurationError as error:
        _render_error(error.code, str(error), json_output=False)
        raise typer.Exit(EXIT_CONFIGURATION) from None
    except SystemExit as error:
        if isinstance(error.code, int):
            raise typer.Exit(error.code) from None
        _render_error(
            "benchmark_failed",
            "Benchmark argument processing failed",
            json_output=False,
        )
        raise typer.Exit(EXIT_FAILURE) from None
    except Exception:
        _render_error(
            "benchmark_failed",
            "Benchmark failed unexpectedly; inspect its secret-safe diagnostics",
            json_output=False,
        )
        raise typer.Exit(EXIT_FAILURE) from None
    if exit_code != 0:
        raise typer.Exit(exit_code)


def _require[T](value: T | None, capability: str) -> T:
    if value is None:
        raise CapabilityUnavailableError(capability)
    return value


type Operation = Callable[[CliRuntime], Awaitable[object]]
type FailurePredicate = Callable[[object], bool]


def _execute(
    runtime_factory: RuntimeFactory,
    operation: Operation,
    *,
    json_output: bool,
    render_result: bool = True,
    failure_predicate: FailurePredicate | None = None,
    failure_exit_code: int = EXIT_FAILURE,
    shutdown_watchdog: ProcessShutdownWatchdog | None = None,
) -> None:
    try:
        result = asyncio.run(_within_runtime(runtime_factory, operation))
    except KeyboardInterrupt:
        raise typer.Exit(130) from None
    except ConfigurationError as error:
        _render_error(error.code, str(error), json_output=json_output)
        raise typer.Exit(EXIT_CONFIGURATION) from None
    except CapabilityUnavailableError as error:
        _render_error(error.code, str(error), json_output=json_output)
        raise typer.Exit(EXIT_CONFIGURATION) from None
    except NotFoundError as error:
        _render_error(error.code, str(error), json_output=json_output)
        raise typer.Exit(EXIT_NOT_FOUND) from None
    except MakoletError as error:
        _render_error(error.code, str(error), json_output=json_output)
        raise typer.Exit(EXIT_TEMPORARY if error.retryable else EXIT_FAILURE) from None
    except ValueError as error:
        _render_error("invalid_argument", str(error), json_output=json_output)
        raise typer.Exit(EXIT_USAGE) from None
    except RuntimeOperationError as error:
        _render_error(error.code, str(error), json_output=json_output)
        raise typer.Exit(EXIT_FAILURE) from None
    except Exception:
        _render_error(
            "unexpected_error",
            "Operation failed unexpectedly; inspect secret-safe application logs",
            json_output=json_output,
        )
        raise typer.Exit(EXIT_FAILURE) from None
    finally:
        if shutdown_watchdog is not None:
            shutdown_watchdog.disarm()
    if render_result:
        _render(result, json_output=json_output)
    if failure_predicate is not None and failure_predicate(result):
        raise typer.Exit(failure_exit_code)


async def _within_runtime(runtime_factory: RuntimeFactory, operation: Operation) -> object:
    settings = load_settings()
    async with runtime_factory(settings) as runtime:
        return await operation(runtime)


async def _serve_uvicorn(application: FastAPI, *, host: str, port: int) -> None:
    limit_concurrency = getattr(application.state, "uvicorn_limit_concurrency", None)
    configuration = uvicorn.Config(
        application,
        host=host,
        port=port,
        access_log=False,
        log_config=None,
        server_header=False,
        limit_concurrency=limit_concurrency,
    )
    server_type = (
        _ExternallySignalledUvicornServer
        if getattr(application.state, "makolet_external_signal_management", False)
        else uvicorn.Server
    )
    server = server_type(configuration)
    await server.serve()


class _ExternallySignalledUvicornServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


async def _run_worker_with_metrics(
    worker: Worker,
    schedules: Sequence[SourceSchedule],
    metrics_app: FastAPI,
    http_server: HttpServer,
    *,
    host: str,
    port: int,
    shutdown_watchdog: ProcessShutdownWatchdog | None = None,
    shutdown_overhead_seconds: float = _PROCESS_SHUTDOWN_OVERHEAD_SECONDS,
) -> None:
    """Own process signals outside Uvicorn and bound the complete shutdown."""
    stop_event = asyncio.Event()
    shutdown_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    watchdog_timeout = worker.shutdown_grace_seconds + shutdown_overhead_seconds
    metrics_state = getattr(metrics_app, "state", None)
    if metrics_state is not None:
        metrics_state.makolet_external_signal_management = True

    def request_shutdown() -> None:
        if shutdown_watchdog is not None:
            shutdown_watchdog.arm(watchdog_timeout)
        loop.call_soon_threadsafe(shutdown_requested.set)

    with _worker_signal_handlers(request_shutdown):
        worker_task = asyncio.create_task(
            worker.run_forever(
                schedules,
                stop_event=stop_event,
                install_signal_handlers=False,
            ),
            name="makolet-worker",
        )
        server_task = asyncio.create_task(
            http_server(metrics_app, host=host, port=port),
            name="makolet-worker-metrics",
        )
        shutdown_task = asyncio.create_task(
            shutdown_requested.wait(),
            name="makolet-worker-shutdown-signal",
        )
        try:
            done, _ = await asyncio.wait(
                {worker_task, server_task, shutdown_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if shutdown_task not in done:
                completed = worker_task if worker_task in done else server_task
                await completed
        finally:
            if shutdown_watchdog is not None:
                shutdown_watchdog.arm(watchdog_timeout)
            deadline = loop.time() + worker.shutdown_grace_seconds
            stop_event.set()
            shutdown_task.cancel()
            if not server_task.done():
                server_task.cancel()
            background_tasks = {worker_task, server_task, shutdown_task}
            done, pending = await asyncio.wait(
                background_tasks,
                timeout=max(0.0, deadline - loop.time()),
            )
            for task in done:
                _observe_background_task(task)
            for task in pending:
                task.cancel()
                task.add_done_callback(_observe_background_task)


@contextmanager
def _worker_signal_handlers(request_shutdown: Callable[[], None]) -> Iterator[None]:
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    handled_signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        handled_signals.append(signal.SIGBREAK)
    original_handlers: dict[signal.Signals, signal._HANDLER] = {}

    def handle_signal(_signum: int, _frame: object) -> None:
        request_shutdown()

    try:
        for handled_signal in handled_signals:
            original_handlers[handled_signal] = signal.signal(handled_signal, handle_signal)
        yield
    finally:
        for handled_signal, original_handler in original_handlers.items():
            signal.signal(handled_signal, original_handler)


def _observe_background_task[T](task: asyncio.Future[T]) -> None:
    with suppress(asyncio.CancelledError, Exception):
        task.result()


def _timestamp(value: str, field_name: str) -> datetime:
    try:
        selected = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise typer.BadParameter(f"--{field_name} must be an ISO-8601 timestamp") from error
    if selected.tzinfo is None or selected.utcoffset() is None:
        raise typer.BadParameter(f"--{field_name} must include a timezone")
    return selected


def _render(value: object, *, json_output: bool) -> None:
    serializable = _jsonable(value)
    if json_output:
        typer.echo(_json_text(serializable))
        return
    typer.echo(_human(serializable))


def _render_error(code: str, message: str, *, json_output: bool) -> None:
    if json_output:
        typer.echo(
            _json_text(
                {"error": {"code": code, "message": message}},
            ),
            err=True,
        )
        return
    typer.echo(f"Error [{_display_scalar(code)}]: {_display_scalar(message)}", err=True)


def _jsonable(value: object) -> object:
    if isinstance(value, Page):
        return {
            "items": [_jsonable(item) for item in value.items],
            "next_cursor": value.next_cursor,
        }
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_jsonable(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID | Decimal | Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _human(value: object) -> str:
    if isinstance(value, Mapping):
        return "\n".join(f"{_display_scalar(key)}: {_compact(item)}" for key, item in value.items())
    if isinstance(value, list):
        return "\n".join(f"- {_compact(item)}" for item in value) or "(no results)"
    return _display_scalar(value)


def _compact(value: object) -> str:
    if isinstance(value, Mapping | list):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    return _display_scalar(value)


def _display_scalar(value: object) -> str:
    """Escape every terminal-affecting Unicode character in human output."""

    rendered = str(value)
    return "".join(
        character
        if unicodedata.category(character) not in {"Cc", "Cs", "Cf", "Zl", "Zp"}
        else f"\\u{ord(character):04x}"
        for character in rendered
    )


def _json_text(value: object) -> str:
    """Render one JSON line without raw terminal or Unicode line controls."""

    rendered = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return "".join(
        _json_escape(character)
        if unicodedata.category(character) in {"Cc", "Cs", "Cf", "Zl", "Zp"}
        else character
        for character in rendered
    )


def _json_escape(character: str) -> str:
    codepoint = ord(character)
    if codepoint <= 0xFFFF:
        return f"\\u{codepoint:04x}"
    scalar = codepoint - 0x10000
    high = 0xD800 + (scalar >> 10)
    low = 0xDC00 + (scalar & 0x3FF)
    return f"\\u{high:04x}\\u{low:04x}"


app = build_cli()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
