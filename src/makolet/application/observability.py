"""Framework-free lifecycle event names and logging port."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from enum import StrEnum
from typing import Protocol


class LifecycleEvent(StrEnum):
    """Stable machine-readable names for operational lifecycle events."""

    DISCOVERY_STARTED = "discovery.started"
    DISCOVERY_PAGE_COMPLETED = "discovery.page_completed"
    DISCOVERY_COMPLETED = "discovery.completed"
    DISCOVERY_FAILED = "discovery.failed"
    DOWNLOAD_STARTED = "download.started"
    DOWNLOAD_COMPLETED = "download.completed"
    DOWNLOAD_FAILED = "download.failed"
    ARCHIVE_STORED = "archive.stored"
    ARCHIVE_DEDUPLICATED = "archive.deduplicated"
    PARSE_STARTED = "parse.started"
    PARSE_COMPLETED = "parse.completed"
    PARSE_FAILED = "parse.failed"
    STAGE_STARTED = "stage.started"
    STAGE_REUSED = "stage.reused"
    STAGE_COMPLETED = "stage.completed"
    STAGE_FAILED = "stage.failed"
    APPLY_STARTED = "apply.started"
    APPLY_COMPLETED = "apply.completed"
    APPLY_FAILED = "apply.failed"
    INGESTION_COMPLETED = "ingestion.completed"
    INGESTION_QUARANTINED = "ingestion.quarantined"
    INGESTION_FAILED = "ingestion.failed"
    REPLAY_STARTED = "replay.started"
    REPLAY_COMPLETED = "replay.completed"
    REPLAY_FAILED = "replay.failed"
    REPLAY_RANGE_STARTED = "replay.range_started"
    REPLAY_RANGE_COMPLETED = "replay.range_completed"
    REPLAY_RANGE_FAILED = "replay.range_failed"
    REBUILD_STARTED = "rebuild.started"
    REBUILD_PROGRESS = "rebuild.progress"
    REBUILD_COMPLETED = "rebuild.completed"
    REBUILD_FAILED = "rebuild.failed"
    WORKER_RUN_STARTED = "worker.run_started"
    WORKER_RUN_COMPLETED = "worker.run_completed"
    WORKER_RUN_FAILED = "worker.run_failed"
    WORKER_SOURCE_STARTED = "worker.source_started"
    WORKER_SOURCE_COMPLETED = "worker.source_completed"
    WORKER_SOURCE_FAILED = "worker.source_failed"
    WORKER_SOURCE_STOPPED = "worker.source_stopped"
    WORKER_HEARTBEAT = "worker.heartbeat"
    WORKER_RECOVERY_COMPLETED = "worker.recovery_completed"
    WORKER_RECOVERY_FAILED = "worker.recovery_failed"
    WORKER_SHUTDOWN_STARTED = "worker.shutdown_started"
    WORKER_SHUTDOWN_COMPLETED = "worker.shutdown_completed"


class LifecycleLogger(Protocol):
    """Application port for safe structured operational events."""

    def context(self, **fields: object) -> AbstractContextManager[None]: ...

    def context_if_absent(self, **fields: object) -> AbstractContextManager[None]: ...

    def info(self, event: LifecycleEvent, **fields: object) -> None: ...

    def warning(self, event: LifecycleEvent, **fields: object) -> None: ...


class NullLifecycleLogger:
    """No-op default that keeps application services framework independent."""

    @contextmanager
    def context(self, **fields: object) -> Iterator[None]:
        del fields
        yield

    @contextmanager
    def context_if_absent(self, **fields: object) -> Iterator[None]:
        del fields
        yield

    def info(self, event: LifecycleEvent, **fields: object) -> None:
        del event, fields

    def warning(self, event: LifecycleEvent, **fields: object) -> None:
        del event, fields


NULL_LIFECYCLE_LOGGER = NullLifecycleLogger()


__all__ = [
    "NULL_LIFECYCLE_LOGGER",
    "LifecycleEvent",
    "LifecycleLogger",
    "NullLifecycleLogger",
]
