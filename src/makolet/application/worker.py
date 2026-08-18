"""Bounded in-process scheduling for repeatable source ingestion."""

from __future__ import annotations

import asyncio
import math
import os
import random
import re
import signal
import threading
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Protocol
from uuid import uuid4

from makolet.application.observability import (
    NULL_LIFECYCLE_LOGGER,
    LifecycleEvent,
    LifecycleLogger,
)
from makolet.domain.errors import MakoletError

_SOURCE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}\Z")


@dataclass(frozen=True, slots=True)
class SourceSchedule:
    """One source's continuous collection cadence."""

    source_id: str
    interval: timedelta
    jitter_ratio: float = 0.10
    run_immediately: bool = True

    def __post_init__(self) -> None:
        if _SOURCE_ID.fullmatch(self.source_id) is None:
            raise ValueError("source_id contains unsupported characters")
        if self.interval <= timedelta(0):
            raise ValueError("source interval must be positive")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class WorkerPolicy:
    """Process-level resource and lifecycle limits."""

    concurrency: int = 4
    queue_capacity: int = 64
    maximum_sources: int = 1_000
    heartbeat_interval: timedelta = timedelta(seconds=30)
    scheduler_resolution: timedelta = timedelta(seconds=1)
    stale_after: timedelta = timedelta(hours=2)
    stale_recovery_interval: timedelta = timedelta(minutes=15)
    shutdown_grace: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        durations = (
            self.heartbeat_interval,
            self.scheduler_resolution,
            self.stale_after,
            self.stale_recovery_interval,
            self.shutdown_grace,
        )
        if self.concurrency <= 0 or self.queue_capacity <= 0 or self.maximum_sources <= 0:
            raise ValueError(
                "worker concurrency, queue capacity, and source limit must be positive"
            )
        if any(duration <= timedelta(0) for duration in durations):
            raise ValueError("worker durations must be positive")


@dataclass(frozen=True, slots=True)
class SourceHealth:
    source_id: str
    in_flight: bool = False
    successful_runs: int = 0
    failed_runs: int = 0
    consecutive_failures: int = 0
    last_started_at: datetime | None = None
    last_succeeded_at: datetime | None = None
    last_failed_at: datetime | None = None
    last_error_code: str | None = None


@dataclass(frozen=True, slots=True)
class SourceRunOutcome:
    source_id: str
    succeeded: bool
    started_at: datetime
    finished_at: datetime
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
    worker_id: str
    observed_at: datetime
    running: bool
    stopping: bool
    queue_depth: int
    sources: tuple[SourceHealth, ...]


@dataclass(frozen=True, slots=True)
class WorkerRunSummary:
    outcomes: tuple[SourceRunOutcome, ...]
    stale_jobs_recovered: int


class WorkerBackend(Protocol):
    """Application integration required by the scheduler."""

    async def recover_stale_jobs(self, *, stale_after: timedelta) -> int: ...

    async def ingest_source(self, source_id: str) -> object: ...


class WorkerTelemetry(Protocol):
    """Durable or metrics-backed worker observations."""

    async def record_heartbeat(self, snapshot: WorkerSnapshot) -> None: ...

    async def record_source_health(self, health: SourceHealth) -> None: ...


class WorkerClock(Protocol):
    def now(self) -> datetime: ...

    def monotonic(self) -> float: ...


class SystemWorkerClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return monotonic()


type Jitter = Callable[[float], float]
type ProcessTerminator = Callable[[int], object]


class ProcessShutdownWatchdog:
    """Force process exit if graceful async-loop teardown exceeds its hard bound."""

    def __init__(self, *, exit_code: int, terminator: ProcessTerminator = os._exit) -> None:
        if not 0 <= exit_code <= 255:
            raise ValueError("watchdog exit code must be between 0 and 255")
        self._exit_code = exit_code
        self._terminator = terminator
        self._lock = threading.Lock()
        self._disarmed: threading.Event | None = None

    def arm(self, timeout_seconds: float) -> None:
        if timeout_seconds <= 0 or not math.isfinite(timeout_seconds):
            raise ValueError("watchdog timeout must be positive and finite")
        with self._lock:
            if self._disarmed is not None:
                return
            disarmed = threading.Event()
            self._disarmed = disarmed
            threading.Thread(
                target=self._terminate_unless_disarmed,
                args=(disarmed, timeout_seconds),
                name="makolet-shutdown-watchdog",
                daemon=True,
            ).start()

    def disarm(self) -> None:
        with self._lock:
            if self._disarmed is not None:
                self._disarmed.set()

    def _terminate_unless_disarmed(
        self,
        disarmed: threading.Event,
        timeout_seconds: float,
    ) -> None:
        if not disarmed.wait(timeout_seconds):
            self._terminator(self._exit_code)


class Worker:
    """Run configured sources once or on a bounded continuous schedule."""

    def __init__(
        self,
        backend: WorkerBackend,
        telemetry: WorkerTelemetry,
        *,
        worker_id: str,
        policy: WorkerPolicy | None = None,
        clock: WorkerClock | None = None,
        jitter: Jitter | None = None,
        events: LifecycleLogger | None = None,
    ) -> None:
        if (
            not worker_id
            or len(worker_id) > 200
            or any(ord(character) < 32 for character in worker_id)
        ):
            raise ValueError("worker_id is empty, too long, or contains control characters")
        self._backend = backend
        self._telemetry = telemetry
        self._worker_id = worker_id
        self._policy = policy or WorkerPolicy()
        self._clock = clock or SystemWorkerClock()
        self._jitter = jitter or _system_jitter
        self._events = events or NULL_LIFECYCLE_LOGGER
        self._health: dict[str, SourceHealth] = {}

    @property
    def source_health(self) -> tuple[SourceHealth, ...]:
        return tuple(self._health[source_id] for source_id in sorted(self._health))

    @property
    def shutdown_grace_seconds(self) -> float:
        return self._policy.shutdown_grace.total_seconds()

    async def run_once(self, source_ids: Iterable[str]) -> WorkerRunSummary:
        """Run a stable, de-duplicated source set with fixed task cardinality."""

        selected = _source_ids(source_ids, maximum=self._policy.maximum_sources)
        if not selected:
            raise ValueError("at least one source is required")
        run_id = str(uuid4())
        run_started = self._clock.monotonic()
        with self._events.context(
            correlation_id=run_id,
            worker_id=self._worker_id,
            worker_run_id=run_id,
        ):
            self._events.info(
                LifecycleEvent.WORKER_RUN_STARTED,
                scheduled_source_count=len(selected),
                status="running",
            )
            try:
                recovered = await self._recover_stale_jobs()
                queue: asyncio.Queue[str | None] = asyncio.Queue(self._policy.queue_capacity)
                outcomes: dict[str, SourceRunOutcome] = {}

                async def consume() -> None:
                    while (source_id := await queue.get()) is not None:
                        try:
                            outcomes[source_id] = await self._run_source(source_id)
                        finally:
                            queue.task_done()
                    queue.task_done()

                consumers = [
                    asyncio.create_task(consume(), name=f"makolet-worker-once-{index}")
                    for index in range(min(self._policy.concurrency, len(selected)))
                ]
                try:
                    for source_id in selected:
                        await queue.put(source_id)
                    for _ in consumers:
                        await queue.put(None)
                    await queue.join()
                    await asyncio.gather(*consumers)
                finally:
                    for consumer in consumers:
                        if not consumer.done():
                            consumer.cancel()
                    await asyncio.gather(*consumers, return_exceptions=True)
            except BaseException as error:
                self._events.warning(
                    LifecycleEvent.WORKER_RUN_FAILED,
                    duration_seconds=self._clock.monotonic() - run_started,
                    error_code=_worker_error_code(error),
                    status="failed",
                )
                raise
            summary = WorkerRunSummary(
                outcomes=tuple(outcomes[source_id] for source_id in selected),
                stale_jobs_recovered=recovered,
            )
            self._events.info(
                LifecycleEvent.WORKER_RUN_COMPLETED,
                duration_seconds=self._clock.monotonic() - run_started,
                failed_count=sum(not outcome.succeeded for outcome in summary.outcomes),
                status="completed",
                successful_count=sum(outcome.succeeded for outcome in summary.outcomes),
            )
            return summary

    async def run_forever(
        self,
        schedules: Sequence[SourceSchedule],
        *,
        stop_event: asyncio.Event | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        """Schedule until stopped, then drain queued work within the grace period."""

        selected = _schedules(schedules, maximum=self._policy.maximum_sources)
        if not selected:
            raise ValueError("at least one source schedule is required")
        requested_stop = stop_event or asyncio.Event()
        queue: asyncio.Queue[str] = asyncio.Queue(self._policy.queue_capacity)
        pending: set[str] = set()
        now = self._clock.monotonic()
        next_due = {
            schedule.source_id: now + (0.0 if schedule.run_immediately else self._delay(schedule))
            for schedule in selected
        }
        run_id = str(uuid4())
        run_started = self._clock.monotonic()
        with self._events.context(
            correlation_id=run_id,
            worker_id=self._worker_id,
            worker_run_id=run_id,
        ):
            self._events.info(
                LifecycleEvent.WORKER_RUN_STARTED,
                scheduled_source_count=len(selected),
                status="running",
            )
            try:
                await self._recover_stale_jobs()
                consumers = [
                    asyncio.create_task(
                        self._consume_continuously(queue, pending),
                        name=f"makolet-worker-{index}",
                    )
                    for index in range(self._policy.concurrency)
                ]
                next_heartbeat = now
                next_recovery = now + self._policy.stale_recovery_interval.total_seconds()
                try:
                    with _signal_handlers(requested_stop, enabled=install_signal_handlers):
                        while not requested_stop.is_set():
                            now = self._clock.monotonic()
                            for schedule in selected:
                                source_id = schedule.source_id
                                if now < next_due[source_id] or source_id in pending:
                                    continue
                                try:
                                    queue.put_nowait(source_id)
                                except asyncio.QueueFull:
                                    break
                                pending.add(source_id)
                                next_due[source_id] = now + self._delay(schedule)
                            if now >= next_recovery:
                                await self._recover_stale_jobs()
                                next_recovery = (
                                    now + self._policy.stale_recovery_interval.total_seconds()
                                )
                            if now >= next_heartbeat:
                                await self._heartbeat(
                                    running=True,
                                    stopping=False,
                                    queue_depth=queue.qsize(),
                                )
                                next_heartbeat = (
                                    now + self._policy.heartbeat_interval.total_seconds()
                                )
                            await _wait_for_stop(
                                requested_stop,
                                timeout_seconds=self._next_sleep(
                                    now, next_due, next_heartbeat, next_recovery
                                ),
                            )
                finally:
                    await self._shutdown(queue, consumers)
            except BaseException as error:
                self._events.warning(
                    LifecycleEvent.WORKER_RUN_FAILED,
                    duration_seconds=self._clock.monotonic() - run_started,
                    error_code=_worker_error_code(error),
                    status="failed",
                )
                raise
            self._events.info(
                LifecycleEvent.WORKER_RUN_COMPLETED,
                duration_seconds=self._clock.monotonic() - run_started,
                failed_count=sum(health.failed_runs for health in self._health.values()),
                status="completed",
                successful_count=sum(health.successful_runs for health in self._health.values()),
            )

    async def _consume_continuously(
        self,
        queue: asyncio.Queue[str],
        pending: set[str],
    ) -> None:
        while True:
            source_id = await queue.get()
            try:
                await self._run_source(source_id)
            finally:
                pending.discard(source_id)
                queue.task_done()

    async def _run_source(self, source_id: str) -> SourceRunOutcome:
        run_id = str(uuid4())
        monotonic_started = self._clock.monotonic()
        started_at = self._clock.now()
        with self._events.context(
            correlation_id=run_id,
            source_id=source_id,
            source_run_id=run_id,
        ):
            self._events.info(
                LifecycleEvent.WORKER_SOURCE_STARTED,
                status="running",
            )
            current = self._health.get(source_id, SourceHealth(source_id=source_id))
            started = replace(current, in_flight=True, last_started_at=started_at)
            self._health[source_id] = started
            await self._telemetry.record_source_health(started)
            try:
                await self._backend.ingest_source(source_id)
            except asyncio.CancelledError:
                stopped = replace(
                    started,
                    in_flight=False,
                    last_error_code="worker_shutdown",
                )
                self._health[source_id] = stopped
                with suppress(asyncio.CancelledError):
                    await asyncio.shield(self._telemetry.record_source_health(stopped))
                self._events.info(
                    LifecycleEvent.WORKER_SOURCE_STOPPED,
                    duration_seconds=self._clock.monotonic() - monotonic_started,
                    error_code="worker_shutdown",
                    status="stopped",
                )
                raise
            except Exception as error:
                finished_at = self._clock.now()
                error_code = _worker_error_code(error)
                failed = replace(
                    started,
                    in_flight=False,
                    failed_runs=started.failed_runs + 1,
                    consecutive_failures=started.consecutive_failures + 1,
                    last_failed_at=finished_at,
                    last_error_code=error_code,
                )
                self._health[source_id] = failed
                await self._telemetry.record_source_health(failed)
                self._events.warning(
                    LifecycleEvent.WORKER_SOURCE_FAILED,
                    duration_seconds=self._clock.monotonic() - monotonic_started,
                    error_code=error_code,
                    status="failed",
                )
                return SourceRunOutcome(
                    source_id=source_id,
                    succeeded=False,
                    started_at=started_at,
                    finished_at=finished_at,
                    error_code=error_code,
                )
            finished_at = self._clock.now()
            succeeded = replace(
                started,
                in_flight=False,
                successful_runs=started.successful_runs + 1,
                consecutive_failures=0,
                last_succeeded_at=finished_at,
                last_error_code=None,
            )
            self._health[source_id] = succeeded
            await self._telemetry.record_source_health(succeeded)
            self._events.info(
                LifecycleEvent.WORKER_SOURCE_COMPLETED,
                duration_seconds=self._clock.monotonic() - monotonic_started,
                status="completed",
            )
            return SourceRunOutcome(
                source_id=source_id,
                succeeded=True,
                started_at=started_at,
                finished_at=finished_at,
            )

    async def _recover_stale_jobs(self) -> int:
        started = self._clock.monotonic()
        try:
            recovered = await self._backend.recover_stale_jobs(stale_after=self._policy.stale_after)
        except BaseException as error:
            self._events.warning(
                LifecycleEvent.WORKER_RECOVERY_FAILED,
                duration_seconds=self._clock.monotonic() - started,
                error_code=_worker_error_code(error),
                status="failed",
            )
            raise
        self._events.info(
            LifecycleEvent.WORKER_RECOVERY_COMPLETED,
            duration_seconds=self._clock.monotonic() - started,
            recovered_count=recovered,
            status="completed",
        )
        return recovered

    async def _shutdown(
        self,
        queue: asyncio.Queue[str],
        consumers: Sequence[asyncio.Task[None]],
    ) -> None:
        started = self._clock.monotonic()
        self._events.info(
            LifecycleEvent.WORKER_SHUTDOWN_STARTED,
            queue_depth=queue.qsize(),
            status="stopping",
        )
        try:
            await self._finish_consumers(queue, consumers)
            await self._heartbeat(running=False, stopping=True, queue_depth=queue.qsize())
        except BaseException as error:
            self._events.warning(
                LifecycleEvent.WORKER_SHUTDOWN_COMPLETED,
                duration_seconds=self._clock.monotonic() - started,
                error_code=_worker_error_code(error),
                status="failed",
            )
            raise
        self._events.info(
            LifecycleEvent.WORKER_SHUTDOWN_COMPLETED,
            duration_seconds=self._clock.monotonic() - started,
            status="completed",
        )

    async def _finish_consumers(
        self,
        queue: asyncio.Queue[str],
        consumers: Sequence[asyncio.Task[None]],
    ) -> None:
        deadline = asyncio.get_running_loop().time() + self._policy.shutdown_grace.total_seconds()
        try:
            async with asyncio.timeout_at(deadline):
                await queue.join()
        except TimeoutError:
            pass
        finally:
            for consumer in consumers:
                consumer.cancel()
            if consumers:
                remaining = max(0.0, deadline - asyncio.get_running_loop().time())
                done, pending = await asyncio.wait(
                    consumers,
                    timeout=remaining,
                )
                for consumer in done:
                    _observe_consumer_result(consumer)
                for consumer in pending:
                    consumer.cancel()
                    consumer.add_done_callback(_observe_consumer_result)

    async def _heartbeat(self, *, running: bool, stopping: bool, queue_depth: int) -> None:
        snapshot = WorkerSnapshot(
            worker_id=self._worker_id,
            observed_at=self._clock.now(),
            running=running,
            stopping=stopping,
            queue_depth=queue_depth,
            sources=self.source_health,
        )
        await self._telemetry.record_heartbeat(snapshot)
        self._events.info(
            LifecycleEvent.WORKER_HEARTBEAT,
            active_source_count=sum(health.in_flight for health in snapshot.sources),
            failed_source_count=sum(health.consecutive_failures > 0 for health in snapshot.sources),
            healthy_source_count=sum(
                not health.in_flight and health.consecutive_failures == 0
                for health in snapshot.sources
            ),
            queue_depth=queue_depth,
            running=running,
            stopping=stopping,
        )

    def _delay(self, schedule: SourceSchedule) -> float:
        interval = schedule.interval.total_seconds()
        maximum_jitter = interval * schedule.jitter_ratio
        return interval + self._jitter(maximum_jitter)

    def _next_sleep(
        self,
        now: float,
        next_due: dict[str, float],
        next_heartbeat: float,
        next_recovery: float,
    ) -> float:
        next_action = min(*next_due.values(), next_heartbeat, next_recovery)
        return max(
            0.001,
            min(self._policy.scheduler_resolution.total_seconds(), next_action - now),
        )


def _source_ids(values: Iterable[str], *, maximum: int) -> tuple[str, ...]:
    selected: dict[str, None] = {}
    for value in values:
        source_id = value.strip()
        if _SOURCE_ID.fullmatch(source_id) is None:
            raise ValueError("source IDs contain unsupported characters")
        selected[source_id] = None
        if len(selected) > maximum:
            raise ValueError(f"worker source count exceeds {maximum}")
    return tuple(selected)


def _schedules(values: Sequence[SourceSchedule], *, maximum: int) -> tuple[SourceSchedule, ...]:
    if len(values) > maximum:
        raise ValueError(f"worker source count exceeds {maximum}")
    selected: dict[str, SourceSchedule] = {}
    for schedule in values:
        if schedule.source_id in selected:
            raise ValueError(f"duplicate source schedule: {schedule.source_id}")
        selected[schedule.source_id] = schedule
    return tuple(selected.values())


def _system_jitter(maximum: float) -> float:
    return random.SystemRandom().uniform(0.0, maximum)


def _worker_error_code(error: BaseException) -> str:
    if isinstance(error, asyncio.CancelledError):
        return "worker_shutdown"
    return error.code if isinstance(error, MakoletError) else "unexpected_error"


async def _wait_for_stop(stop_event: asyncio.Event, *, timeout_seconds: float) -> None:
    try:
        async with asyncio.timeout(timeout_seconds):
            await stop_event.wait()
    except TimeoutError:
        pass


def _observe_consumer_result(consumer: asyncio.Task[None]) -> None:
    """Retrieve a detached consumer result without extending shutdown."""

    with suppress(asyncio.CancelledError):
        consumer.exception()


@contextmanager
def _signal_handlers(stop_event: asyncio.Event, *, enabled: bool) -> Iterator[None]:
    if not enabled:
        yield
        return
    loop = asyncio.get_running_loop()
    selected = tuple(
        selected_signal
        for selected_signal in (signal.SIGINT, getattr(signal, "SIGTERM", None))
        if selected_signal is not None
    )
    loop_handlers: list[signal.Signals] = []
    previous_handlers: dict[signal.Signals, signal._HANDLER] = {}
    try:
        for selected_signal in selected:
            try:
                loop.add_signal_handler(selected_signal, stop_event.set)
                loop_handlers.append(selected_signal)
            except NotImplementedError, RuntimeError:
                previous_handlers[selected_signal] = signal.getsignal(selected_signal)
                signal.signal(
                    selected_signal,
                    lambda _signum, _frame: loop.call_soon_threadsafe(stop_event.set),
                )
        yield
    finally:
        for selected_signal in loop_handlers:
            loop.remove_signal_handler(selected_signal)
        for selected_signal, previous in previous_handlers.items():
            signal.signal(selected_signal, previous)
