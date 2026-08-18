"""Bounded archive replay and operator-confirmed normalized-state rebuilds."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, Protocol
from uuid import UUID, uuid4

from makolet.application.models import (
    IngestionResult,
    NormalizedRebuildRun,
    ReplayRangeResult,
)
from makolet.application.observability import (
    NULL_LIFECYCLE_LOGGER,
    LifecycleEvent,
    LifecycleLogger,
)
from makolet.application.ports import ArchiveReplayRepository, NormalizedRebuildRepository
from makolet.domain.enums import IngestionStatus
from makolet.domain.errors import (
    DomainValidationError,
    LeaseUnavailableError,
    NormalizedRebuildInterruptedError,
    QueryLimitError,
)
from makolet.domain.normalization import clean_source_text

# This is a destructive-action acknowledgement phrase, not a credential.
REBUILD_CONFIRMATION_TOKEN: Final = "REBUILD-NORMALIZED-STATE"  # noqa: S105
_REBUILD_FAILURE_MESSAGE: Final = (
    "Archived replay failed; inspect the rebuild and replay audit records before resuming"
)


class ReplayableIngestion(Protocol):
    @property
    def parser_version(self) -> str: ...

    async def replay(
        self,
        source_file_id: UUID,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> IngestionResult: ...


class CatalogBootstrap(Protocol):
    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class ArchiveMaintenanceLimits:
    default_page_size: int = 50
    maximum_page_size: int = 200
    rebuild_page_size: int = 50

    def __post_init__(self) -> None:
        if self.default_page_size <= 0:
            raise ValueError("The default archive page size must be positive")
        if self.maximum_page_size < self.default_page_size:
            raise ValueError("The maximum archive page size is inconsistent")
        if self.rebuild_page_size <= 0 or self.rebuild_page_size > self.maximum_page_size:
            raise ValueError("The rebuild page size is inconsistent")


class ArchiveMaintenanceService:
    """Replay immutable bytes without invoking discovery or download adapters."""

    def __init__(
        self,
        archive_repository: ArchiveReplayRepository,
        rebuild_repository: NormalizedRebuildRepository,
        ingestion: ReplayableIngestion,
        limits: ArchiveMaintenanceLimits | None = None,
        events: LifecycleLogger | None = None,
        catalog_bootstrap: CatalogBootstrap | None = None,
    ) -> None:
        self._archive_repository = archive_repository
        self._rebuild_repository = rebuild_repository
        self._ingestion = ingestion
        self._limits = limits or ArchiveMaintenanceLimits()
        self._events = events or NULL_LIFECYCLE_LOGGER
        self._catalog_bootstrap = catalog_bootstrap

    async def replay_range(
        self,
        *,
        since: datetime,
        until: datetime,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> ReplayRangeResult:
        """Replay one deterministic ``[since, until)`` archive page."""

        start = _aware(since, "since")
        end = _aware(until, "until")
        if start >= end:
            raise DomainValidationError("since must be before until")
        selected_limit = self._limit(limit)
        selected_cursor = _cursor(cursor)
        run_id = str(uuid4())
        started = time.perf_counter()
        with self._events.context(correlation_id=run_id, run_id=run_id):
            self._events.info(
                LifecycleEvent.REPLAY_RANGE_STARTED,
                limit=selected_limit,
                operation="range",
                status="running",
            )
            try:
                page = await self._archive_repository.list_archived_files(
                    since=start,
                    until=end,
                    limit=selected_limit,
                    cursor=selected_cursor,
                )
                results = []
                for source_file in page.files:
                    result = await self._ingestion.replay(source_file.source_file_id)
                    await self._bootstrap_catalog(result)
                    results.append(result)
            except BaseException as error:
                self._events.warning(
                    LifecycleEvent.REPLAY_RANGE_FAILED,
                    duration_seconds=time.perf_counter() - started,
                    error_code=_error_code(error),
                    operation="range",
                    status="failed",
                )
                raise
            self._events.info(
                LifecycleEvent.REPLAY_RANGE_COMPLETED,
                duration_seconds=time.perf_counter() - started,
                file_count=len(results),
                operation="range",
                status="completed",
            )
            return ReplayRangeResult(
                since=start,
                until=end,
                files=tuple(results),
                next_cursor=page.next_cursor,
            )

    async def start_rebuild(
        self,
        *,
        confirmation: str,
        requested_by: str,
    ) -> NormalizedRebuildRun:
        if confirmation != REBUILD_CONFIRMATION_TOKEN:
            raise DomainValidationError(
                f"confirmation must exactly equal {REBUILD_CONFIRMATION_TOKEN}"
            )
        operator = clean_source_text(requested_by, max_length=128)
        if operator is None:
            raise DomainValidationError("requested_by must identify an operator or service")
        run = await self._rebuild_repository.begin_rebuild(
            requested_by=operator,
            parser_version=self._ingestion.parser_version,
        )
        return await self.resume_rebuild(run.rebuild_run_id)

    async def resume_rebuild(self, rebuild_run_id: UUID) -> NormalizedRebuildRun:
        started = time.perf_counter()
        with self._events.context(
            correlation_id=str(rebuild_run_id),
            rebuild_run_id=rebuild_run_id,
            run_id=rebuild_run_id,
        ):
            async with self._rebuild_repository.lock_rebuild(rebuild_run_id) as acquired:
                if not acquired:
                    raise LeaseUnavailableError("Another process is already running this rebuild")
                run = await self._rebuild_repository.resume_rebuild(rebuild_run_id)
                self._events.info(
                    LifecycleEvent.REBUILD_STARTED,
                    source_files_completed=run.source_files_completed,
                    source_files_total=run.source_files_total,
                    status="running",
                )
                try:
                    while True:
                        files = await self._rebuild_repository.next_rebuild_files(
                            rebuild_run_id,
                            limit=self._limits.rebuild_page_size,
                        )
                        if not files:
                            completed = await self._rebuild_repository.finish_rebuild(
                                rebuild_run_id
                            )
                            self._events.info(
                                LifecycleEvent.REBUILD_COMPLETED,
                                duration_seconds=time.perf_counter() - started,
                                source_files_completed=completed.source_files_completed,
                                source_files_total=completed.source_files_total,
                                status="completed",
                            )
                            return completed
                        for sequence, source_file in files:
                            result = await self._ingestion.replay(
                                source_file.source_file_id,
                                rebuild_run_id=rebuild_run_id,
                            )
                            await self._bootstrap_catalog(result)
                            await self._rebuild_repository.complete_rebuild_file(
                                rebuild_run_id,
                                sequence=sequence,
                                source_file=source_file,
                            )
                            self._events.info(
                                LifecycleEvent.REBUILD_PROGRESS,
                                sequence=sequence,
                                source_file_id=source_file.source_file_id,
                                status="running",
                            )
                except BaseException as error:
                    error_code = _error_code(error)
                    await asyncio.shield(
                        self._rebuild_repository.fail_rebuild(
                            rebuild_run_id,
                            error_code=error_code,
                            error_message=_REBUILD_FAILURE_MESSAGE,
                        )
                    )
                    self._events.warning(
                        LifecycleEvent.REBUILD_FAILED,
                        duration_seconds=time.perf_counter() - started,
                        error_code=error_code,
                        status="failed",
                    )
                    if isinstance(error, asyncio.CancelledError):
                        raise
                    raise NormalizedRebuildInterruptedError(
                        f"Normalized rebuild {rebuild_run_id} stopped; resume the same run after "
                        "inspecting its audit record"
                    ) from error

    async def rebuild_status(self, rebuild_run_id: UUID) -> NormalizedRebuildRun:
        return await self._rebuild_repository.get_rebuild(rebuild_run_id)

    async def maintenance_status(self) -> dict[str, object]:
        return await self._rebuild_repository.maintenance_status()

    async def _bootstrap_catalog(self, result: IngestionResult) -> None:
        if self._catalog_bootstrap is not None and result.status is IngestionStatus.COMPLETED:
            await self._catalog_bootstrap.bootstrap_source_file(result.source_file_id)

    def _limit(self, value: int | None) -> int:
        selected = self._limits.default_page_size if value is None else value
        if selected <= 0 or selected > self._limits.maximum_page_size:
            raise QueryLimitError(f"limit must be between 1 and {self._limits.maximum_page_size}")
        return selected


def _aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DomainValidationError(f"{field_name} must include a timezone")
    return value.astimezone(UTC)


def _cursor(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned or len(cleaned) > 128 or any(ord(character) < 32 for character in cleaned):
        raise DomainValidationError("Cursor is empty, too long, or contains control characters")
    return cleaned


def _error_code(error: BaseException) -> str:
    code = getattr(error, "code", None)
    return code if isinstance(code, str) and 0 < len(code) <= 128 else "rebuild_replay_failed"
