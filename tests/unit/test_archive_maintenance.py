from __future__ import annotations

import io
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from makolet.adapters.observability.logging import configure_logging, get_lifecycle_logger
from makolet.application.maintenance import (
    REBUILD_CONFIRMATION_TOKEN,
    ArchiveMaintenanceLimits,
    ArchiveMaintenanceService,
)
from makolet.application.models import (
    ArchivedSourceFile,
    ArchivedSourceFilePage,
    IngestionResult,
    NormalizedRebuildRun,
)
from makolet.application.observability import LifecycleLogger
from makolet.domain.enums import IngestionStatus
from makolet.domain.errors import (
    DomainValidationError,
    LeaseUnavailableError,
    NormalizedRebuildInterruptedError,
    QueryLimitError,
)

RUN_ID = UUID("00000000-0000-0000-0000-000000000010")
FILE_A = UUID("00000000-0000-0000-0000-000000000101")
FILE_B = UUID("00000000-0000-0000-0000-000000000102")
ARCHIVED_A = datetime(2026, 8, 1, 10, tzinfo=UTC)
ARCHIVED_B = datetime(2026, 8, 1, 11, tzinfo=UTC)


class RecordingArchiveRepository:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.page = ArchivedSourceFilePage(
            files=(ArchivedSourceFile(FILE_A, ARCHIVED_A),),
            next_cursor=str(FILE_A),
        )

    async def list_archived_files(self, **kwargs: object) -> ArchivedSourceFilePage:
        self.calls.append(dict(kwargs))
        return self.page


class RecordingRebuildRepository:
    def __init__(self) -> None:
        self.lock_acquired = True
        self.pages: list[tuple[tuple[int, ArchivedSourceFile], ...]] = [
            (
                (1, ArchivedSourceFile(FILE_A, ARCHIVED_A)),
                (2, ArchivedSourceFile(FILE_B, ARCHIVED_B)),
            ),
            (),
        ]
        self.events: list[tuple[str, object]] = []
        self.run = _run(status="running", completed=0)

    @asynccontextmanager
    async def lock_rebuild(self, rebuild_run_id: UUID) -> AsyncIterator[bool]:
        self.events.append(("lock", rebuild_run_id))
        yield self.lock_acquired

    async def begin_rebuild(
        self, *, requested_by: str, parser_version: str
    ) -> NormalizedRebuildRun:
        self.events.append(
            ("begin", {"requested_by": requested_by, "parser_version": parser_version})
        )
        return self.run

    async def resume_rebuild(self, rebuild_run_id: UUID) -> NormalizedRebuildRun:
        self.events.append(("resume", rebuild_run_id))
        return self.run

    async def get_rebuild(self, rebuild_run_id: UUID) -> NormalizedRebuildRun:
        self.events.append(("get", rebuild_run_id))
        return self.run

    async def next_rebuild_files(
        self, rebuild_run_id: UUID, *, limit: int
    ) -> tuple[tuple[int, ArchivedSourceFile], ...]:
        self.events.append(("page", {"run_id": rebuild_run_id, "limit": limit}))
        return self.pages.pop(0)

    async def complete_rebuild_file(
        self,
        rebuild_run_id: UUID,
        *,
        sequence: int,
        source_file: ArchivedSourceFile,
    ) -> None:
        self.events.append(
            (
                "complete_file",
                {
                    "run_id": rebuild_run_id,
                    "sequence": sequence,
                    "source_file": source_file,
                },
            )
        )

    async def fail_rebuild(
        self,
        rebuild_run_id: UUID,
        *,
        error_code: str,
        error_message: str,
    ) -> None:
        self.events.append(
            (
                "fail",
                {
                    "run_id": rebuild_run_id,
                    "error_code": error_code,
                    "error_message": error_message,
                },
            )
        )

    async def finish_rebuild(self, rebuild_run_id: UUID) -> NormalizedRebuildRun:
        self.events.append(("finish", rebuild_run_id))
        return _run(status="completed", completed=2)

    async def maintenance_status(self) -> dict[str, object]:
        return {"active": True, "rebuild_run_id": RUN_ID}


class RecordingIngestion:
    parser_version = "parser-test-v3"

    def __init__(self, *, failure: Exception | None = None) -> None:
        self.calls: list[tuple[UUID, UUID | None]] = []
        self.failure = failure

    async def replay(
        self,
        source_file_id: UUID,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> IngestionResult:
        self.calls.append((source_file_id, rebuild_run_id))
        if self.failure is not None:
            raise self.failure
        return IngestionResult(
            source_file_id=source_file_id,
            status=IngestionStatus.COMPLETED,
            content_sha256="a" * 64,
            stage=None,
            apply=None,
            replayed=True,
        )


class RecordingCatalogBootstrap:
    def __init__(self, *, failure: Exception | None = None) -> None:
        self.calls: list[UUID] = []
        self.events: list[tuple[str, object]] | None = None
        self.failure = failure

    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]:
        self.calls.append(source_file_id)
        if self.events is not None:
            self.events.append(("bootstrap", source_file_id))
        if self.failure is not None:
            raise self.failure
        return {"source_file_id": source_file_id, "bootstrapped_items": 1}


def _run(*, status: str, completed: int) -> NormalizedRebuildRun:
    return NormalizedRebuildRun(
        rebuild_run_id=RUN_ID,
        status=status,
        archive_cutoff_at=datetime(2026, 8, 2, tzinfo=UTC),
        source_files_total=2,
        source_files_completed=completed,
    )


def _service(
    *,
    ingestion: RecordingIngestion | None = None,
    events: LifecycleLogger | None = None,
    catalog_bootstrap: RecordingCatalogBootstrap | None = None,
) -> tuple[
    ArchiveMaintenanceService,
    RecordingArchiveRepository,
    RecordingRebuildRepository,
    RecordingIngestion,
]:
    archive = RecordingArchiveRepository()
    rebuild = RecordingRebuildRepository()
    selected_ingestion = ingestion or RecordingIngestion()
    return (
        ArchiveMaintenanceService(
            archive,
            rebuild,
            selected_ingestion,
            ArchiveMaintenanceLimits(rebuild_page_size=10),
            events=events,
            catalog_bootstrap=catalog_bootstrap,
        ),
        archive,
        rebuild,
        selected_ingestion,
    )


@pytest.mark.asyncio
async def test_range_replay_uses_utc_half_open_bounds_and_one_bounded_page() -> None:
    service, archive, _, ingestion = _service()
    offset = timezone(timedelta(hours=3))

    result = await service.replay_range(
        since=datetime(2026, 8, 1, 12, tzinfo=offset),
        until=datetime(2026, 8, 1, 14, tzinfo=offset),
        limit=17,
        cursor=f" {FILE_B} ",
    )

    assert archive.calls == [
        {
            "since": datetime(2026, 8, 1, 9, tzinfo=UTC),
            "until": datetime(2026, 8, 1, 11, tzinfo=UTC),
            "limit": 17,
            "cursor": str(FILE_B),
        }
    ]
    assert ingestion.calls == [(FILE_A, None)]
    assert result.next_cursor == str(FILE_A)
    assert tuple(item.source_file_id for item in result.files) == (FILE_A,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("since", "until", "limit", "cursor", "error_type"),
    [
        (
            datetime(2026, 8, 1, tzinfo=UTC).replace(tzinfo=None),
            datetime(2026, 8, 2, tzinfo=UTC),
            10,
            None,
            DomainValidationError,
        ),
        (
            datetime(2026, 8, 2, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
            10,
            None,
            DomainValidationError,
        ),
        (
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
            201,
            None,
            QueryLimitError,
        ),
        (
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
            10,
            "\n",
            DomainValidationError,
        ),
    ],
)
async def test_range_replay_rejects_unbounded_or_ambiguous_inputs_before_storage(
    since: datetime,
    until: datetime,
    limit: int,
    cursor: str | None,
    error_type: type[Exception],
) -> None:
    service, archive, _, ingestion = _service()

    with pytest.raises(error_type):
        await service.replay_range(
            since=since,
            until=until,
            limit=limit,
            cursor=cursor,
        )

    assert archive.calls == []
    assert ingestion.calls == []


@pytest.mark.asyncio
async def test_rebuild_requires_exact_confirmation_before_destructive_repository_call() -> None:
    service, _, rebuild, _ = _service()

    with pytest.raises(DomainValidationError, match="exactly equal"):
        await service.start_rebuild(
            confirmation="rebuild-normalized-state",
            requested_by="operator",
        )

    assert rebuild.events == []


@pytest.mark.asyncio
async def test_rebuild_replays_snapshot_in_pages_and_checkpoints_every_file() -> None:
    service, _, rebuild, ingestion = _service()

    result = await service.start_rebuild(
        confirmation=REBUILD_CONFIRMATION_TOKEN,
        requested_by="  operations service  ",
    )

    assert result.status == "completed"
    assert result.source_files_completed == 2
    assert ingestion.calls == [(FILE_A, RUN_ID), (FILE_B, RUN_ID)]
    assert rebuild.events[0] == (
        "begin",
        {"requested_by": "operations service", "parser_version": "parser-test-v3"},
    )
    assert [event for event, _ in rebuild.events].count("complete_file") == 2
    assert rebuild.events[-1] == ("finish", RUN_ID)


@pytest.mark.asyncio
async def test_rebuild_failure_is_safely_audited_and_same_run_can_resume() -> None:
    failure = DomainValidationError("source payload secret must not be copied")
    service, _, rebuild, ingestion = _service(ingestion=RecordingIngestion(failure=failure))

    with pytest.raises(NormalizedRebuildInterruptedError, match=str(RUN_ID)):
        await service.resume_rebuild(RUN_ID)

    assert ingestion.calls == [(FILE_A, RUN_ID)]
    failure_event = next(payload for event, payload in rebuild.events if event == "fail")
    assert isinstance(failure_event, dict)
    assert failure_event["error_code"] == "domain_validation_error"
    assert "secret" not in str(failure_event["error_message"])
    assert all(event != "complete_file" for event, _ in rebuild.events)


@pytest.mark.asyncio
async def test_rebuild_refuses_concurrent_runner_before_any_replay() -> None:
    service, _, rebuild, ingestion = _service()
    rebuild.lock_acquired = False

    with pytest.raises(LeaseUnavailableError):
        await service.resume_rebuild(RUN_ID)

    assert ingestion.calls == []
    assert [event for event, _ in rebuild.events] == ["lock"]


@pytest.mark.asyncio
async def test_replay_and_rebuild_bootstrap_catalog_before_rebuild_checkpoint() -> None:
    bootstrap = RecordingCatalogBootstrap()
    service, _, rebuild, _ = _service(catalog_bootstrap=bootstrap)

    await service.replay_range(
        since=datetime(2026, 8, 1, 9, tzinfo=UTC),
        until=datetime(2026, 8, 1, 12, tzinfo=UTC),
        limit=10,
    )
    assert bootstrap.calls == [FILE_A]

    bootstrap.events = rebuild.events
    await service.start_rebuild(
        confirmation=REBUILD_CONFIRMATION_TOKEN,
        requested_by="operations service",
    )

    assert bootstrap.calls == [FILE_A, FILE_A, FILE_B]
    names = [event for event, _ in rebuild.events]
    assert names.index("bootstrap") < names.index("complete_file")
    first_checkpoint = names.index("complete_file")
    second_bootstrap = names.index("bootstrap", names.index("bootstrap") + 1)
    second_checkpoint = names.index("complete_file", first_checkpoint + 1)
    assert second_bootstrap < second_checkpoint


@pytest.mark.asyncio
async def test_rebuild_catalog_bootstrap_failure_does_not_checkpoint_file() -> None:
    bootstrap = RecordingCatalogBootstrap(failure=DomainValidationError("catalog bootstrap failed"))
    service, _, rebuild, _ = _service(catalog_bootstrap=bootstrap)

    with pytest.raises(NormalizedRebuildInterruptedError):
        await service.start_rebuild(
            confirmation=REBUILD_CONFIRMATION_TOKEN,
            requested_by="operations service",
        )

    assert bootstrap.calls == [FILE_A]
    assert "complete_file" not in [event for event, _ in rebuild.events]
    assert "fail" in [event for event, _ in rebuild.events]


@pytest.mark.asyncio
async def test_maintenance_logs_correlated_replay_and_rebuild_lifecycle() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    events = get_lifecycle_logger("maintenance-test")
    service, _, _, _ = _service(events=events)

    await service.replay_range(
        since=datetime(2026, 8, 1, 9, tzinfo=UTC),
        until=datetime(2026, 8, 1, 12, tzinfo=UTC),
        limit=10,
    )
    await service.start_rebuild(
        confirmation=REBUILD_CONFIRMATION_TOKEN,
        requested_by="operations service",
    )

    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    names = [event["event"] for event in logged]
    assert names == [
        "replay.range_started",
        "replay.range_completed",
        "rebuild.started",
        "rebuild.progress",
        "rebuild.progress",
        "rebuild.completed",
    ]
    range_events = logged[:2]
    assert len({event["correlation_id"] for event in range_events}) == 1
    rebuild_events = logged[2:]
    assert all(event["rebuild_run_id"] == str(RUN_ID) for event in rebuild_events)
    assert all(event["correlation_id"] == str(RUN_ID) for event in rebuild_events)


@pytest.mark.asyncio
async def test_rebuild_failure_log_contains_code_without_exception_detail() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    service, _, _, _ = _service(
        ingestion=RecordingIngestion(  # secret-scan: allow
            failure=DomainValidationError(
                "password=private-value\nforged"  # secret-scan: allow
            )
        ),
        events=get_lifecycle_logger("maintenance-failure-test"),
    )

    with pytest.raises(NormalizedRebuildInterruptedError):
        await service.resume_rebuild(RUN_ID)

    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [event["event"] for event in logged] == [
        "rebuild.started",
        "rebuild.failed",
    ]
    assert logged[-1]["error_code"] == DomainValidationError.code
    assert "private-value" not in stream.getvalue()
    assert "forged" not in stream.getvalue()
