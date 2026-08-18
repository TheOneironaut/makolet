from __future__ import annotations

import asyncio
import io
import json
import time
from dataclasses import dataclass, field, replace
from datetime import timedelta

import pytest

from makolet.adapters.observability.logging import configure_logging, get_lifecycle_logger
from makolet.application.worker import (
    ProcessShutdownWatchdog,
    SourceHealth,
    SourceSchedule,
    Worker,
    WorkerPolicy,
    WorkerSnapshot,
)
from makolet.domain.errors import SourceAccessError


@dataclass
class RecordingBackend:
    failing_sources: frozenset[str] = frozenset()
    unexpected_sources: frozenset[str] = frozenset()
    delay_seconds: float = 0.005
    recovery_calls: int = 0
    calls: list[str] = field(default_factory=list)
    active: int = 0
    maximum_active: int = 0
    progress: asyncio.Event = field(default_factory=asyncio.Event)
    ingestion_started: asyncio.Event = field(default_factory=asyncio.Event)

    async def recover_stale_jobs(self, *, stale_after: timedelta) -> int:
        assert stale_after > timedelta(0)
        self.recovery_calls += 1
        self.progress.set()
        return 2

    async def ingest_source(self, source_id: str) -> object:
        self.calls.append(source_id)
        self.progress.set()
        self.ingestion_started.set()
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        try:
            await asyncio.sleep(self.delay_seconds)
            if source_id in self.failing_sources:
                raise SourceAccessError("publisher is temporarily unavailable")
            if source_id in self.unexpected_sources:
                raise RuntimeError("unclassified adapter failure")
            return {"source_id": source_id}
        finally:
            self.active -= 1


@dataclass
class RecordingTelemetry:
    health: list[SourceHealth] = field(default_factory=list)
    heartbeats: list[WorkerSnapshot] = field(default_factory=list)

    async def record_heartbeat(self, snapshot: WorkerSnapshot) -> None:
        self.heartbeats.append(snapshot)

    async def record_source_health(self, health: SourceHealth) -> None:
        self.health.append(health)


def _policy(*, concurrency: int = 2) -> WorkerPolicy:
    return WorkerPolicy(
        concurrency=concurrency,
        queue_capacity=2,
        heartbeat_interval=timedelta(milliseconds=5),
        scheduler_resolution=timedelta(milliseconds=2),
        stale_after=timedelta(minutes=1),
        stale_recovery_interval=timedelta(milliseconds=8),
        shutdown_grace=timedelta(seconds=1),
    )


@pytest.mark.asyncio
async def test_run_once_is_deduplicated_bounded_and_failure_isolated() -> None:
    backend = RecordingBackend(failing_sources=frozenset({"broken"}))
    telemetry = RecordingTelemetry()
    worker = Worker(
        backend,
        telemetry,
        worker_id="worker-1",
        policy=_policy(),
        jitter=lambda _maximum: 0.0,
    )

    summary = await worker.run_once(("alpha", "broken", "alpha", "beta"))

    assert tuple(outcome.source_id for outcome in summary.outcomes) == (
        "alpha",
        "broken",
        "beta",
    )
    assert [outcome.succeeded for outcome in summary.outcomes] == [True, False, True]
    assert summary.outcomes[1].error_code == SourceAccessError.code
    assert summary.stale_jobs_recovered == 2
    assert backend.maximum_active == 2
    assert backend.recovery_calls == 1
    broken_health = next(health for health in worker.source_health if health.source_id == "broken")
    assert broken_health.consecutive_failures == 1


@pytest.mark.asyncio
async def test_continuous_worker_emits_heartbeats_recovers_stale_jobs_and_stops() -> None:
    backend = RecordingBackend(delay_seconds=0.001)
    telemetry = RecordingTelemetry()
    worker = Worker(
        backend,
        telemetry,
        worker_id="worker-continuous",
        policy=_policy(concurrency=1),
        jitter=lambda _maximum: 0.0,
    )
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        worker.run_forever(
            (SourceSchedule("alpha", timedelta(milliseconds=4), jitter_ratio=0),),
            stop_event=stop_event,
            install_signal_handlers=True,
        )
    )

    async with asyncio.timeout(1):
        while len(backend.calls) < 4 or backend.recovery_calls < 2:
            backend.progress.clear()
            if len(backend.calls) < 4 or backend.recovery_calls < 2:
                await backend.progress.wait()
    stop_event.set()
    await task

    assert len(backend.calls) >= 4
    assert backend.maximum_active == 1
    assert backend.recovery_calls >= 2
    assert telemetry.heartbeats
    assert telemetry.heartbeats[0].running is True
    assert telemetry.heartbeats[-1].running is False
    assert telemetry.heartbeats[-1].stopping is True
    assert worker.source_health[0].successful_runs >= 4


def test_worker_policy_and_schedules_reject_invalid_bounds() -> None:
    with pytest.raises(ValueError, match="positive"):
        WorkerPolicy(concurrency=0)
    with pytest.raises(ValueError, match="positive"):
        SourceSchedule("alpha", timedelta(0))
    with pytest.raises(ValueError, match="jitter"):
        SourceSchedule("alpha", timedelta(minutes=1), jitter_ratio=1.1)
    with pytest.raises(ValueError, match="unsupported characters"):
        SourceSchedule("alpha\nforged", timedelta(minutes=1))


@pytest.mark.asyncio
async def test_continuous_worker_rejects_duplicate_schedules() -> None:
    worker = Worker(
        RecordingBackend(),
        RecordingTelemetry(),
        worker_id="worker-duplicate",
        policy=_policy(),
    )

    with pytest.raises(ValueError, match="duplicate source schedule"):
        await worker.run_forever(
            (
                SourceSchedule("alpha", timedelta(minutes=1)),
                SourceSchedule("alpha", timedelta(minutes=2)),
            ),
            install_signal_handlers=False,
        )


@pytest.mark.asyncio
async def test_worker_rejects_empty_or_invalid_source_sets() -> None:
    worker = Worker(
        RecordingBackend(),
        RecordingTelemetry(),
        worker_id="worker-validation",
        policy=_policy(),
    )

    with pytest.raises(ValueError, match="at least one source"):
        await worker.run_once(())
    with pytest.raises(ValueError, match="source IDs"):
        await worker.run_once((" ",))
    with pytest.raises(ValueError, match="at least one source schedule"):
        await worker.run_forever((), install_signal_handlers=False)

    bounded_worker = Worker(
        RecordingBackend(),
        RecordingTelemetry(),
        worker_id="worker-bounded",
        policy=replace(_policy(), maximum_sources=1),
    )
    with pytest.raises(ValueError, match="source count exceeds"):
        await bounded_worker.run_once(("alpha", "beta"))


@pytest.mark.asyncio
async def test_forced_shutdown_clears_in_flight_health_without_source_failure() -> None:
    backend = RecordingBackend(delay_seconds=1)
    telemetry = RecordingTelemetry()
    worker = Worker(
        backend,
        telemetry,
        worker_id="worker-shutdown",
        policy=replace(_policy(concurrency=1), shutdown_grace=timedelta(milliseconds=3)),
        jitter=lambda _maximum: 0,
    )
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        worker.run_forever(
            (SourceSchedule("alpha", timedelta(minutes=1)),),
            stop_event=stop_event,
            install_signal_handlers=False,
        )
    )
    async with asyncio.timeout(1):
        await backend.ingestion_started.wait()

    stop_event.set()
    await task

    assert worker.source_health[0].in_flight is False
    assert worker.source_health[0].failed_runs == 0
    assert worker.source_health[0].last_error_code == "worker_shutdown"
    assert telemetry.heartbeats[-1].running is False


@pytest.mark.asyncio
async def test_shutdown_bounds_final_consumer_gather_after_first_cancellation_is_ignored() -> None:
    class FirstCancellationIgnoringBackend(RecordingBackend):
        cancellation_observed = asyncio.Event()

        async def ingest_source(self, source_id: str) -> object:
            self.calls.append(source_id)
            self.ingestion_started.set()
            never = asyncio.Event()
            try:
                await never.wait()
            except asyncio.CancelledError:
                self.cancellation_observed.set()
                # The worker's final bounded gather issues a second cancellation
                # instead of waiting forever for this uncooperative operation.
                await never.wait()
            raise AssertionError("unreachable")

    backend = FirstCancellationIgnoringBackend()
    worker = Worker(
        backend,
        RecordingTelemetry(),
        worker_id="worker-bounded-shutdown",
        policy=replace(
            _policy(concurrency=1),
            shutdown_grace=timedelta(milliseconds=10),
        ),
        jitter=lambda _maximum: 0,
    )
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        worker.run_forever(
            (SourceSchedule("alpha", timedelta(minutes=1)),),
            stop_event=stop_event,
            install_signal_handlers=False,
        )
    )
    async with asyncio.timeout(1):
        await backend.ingestion_started.wait()

    started = asyncio.get_running_loop().time()
    stop_event.set()
    async with asyncio.timeout(0.2):
        await task

    assert backend.cancellation_observed.is_set()
    assert asyncio.get_running_loop().time() - started < 0.1


def test_process_shutdown_watchdog_can_be_disarmed_without_terminating() -> None:
    exit_codes: list[int] = []
    watchdog = ProcessShutdownWatchdog(
        exit_code=77,
        terminator=lambda exit_code: exit_codes.append(exit_code),
    )

    watchdog.arm(0.02)
    watchdog.disarm()
    time.sleep(0.05)

    assert exit_codes == []


@pytest.mark.asyncio
async def test_unexpected_source_exception_is_classified_without_leaking_details() -> None:
    worker = Worker(
        RecordingBackend(unexpected_sources=frozenset({"alpha"})),
        RecordingTelemetry(),
        worker_id="worker-errors",
        policy=_policy(),
    )

    summary = await worker.run_once(("alpha",))

    assert summary.outcomes[0].succeeded is False
    assert summary.outcomes[0].error_code == "unexpected_error"


@pytest.mark.asyncio
async def test_run_once_logs_correlated_success_and_failure_lifecycle() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    worker = Worker(
        RecordingBackend(failing_sources=frozenset({"broken"})),
        RecordingTelemetry(),
        worker_id="worker-events",
        policy=_policy(concurrency=1),
        events=get_lifecycle_logger("worker-test"),
    )

    await worker.run_once(("alpha", "broken"))

    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [event["event"] for event in logged] == [
        "worker.run_started",
        "worker.recovery_completed",
        "worker.source_started",
        "worker.source_completed",
        "worker.source_started",
        "worker.source_failed",
        "worker.run_completed",
    ]
    assert all(event["worker_id"] == "worker-events" for event in logged)
    assert len({event["worker_run_id"] for event in logged}) == 1
    source_events = [event for event in logged if "source_id" in event]
    assert {event["source_id"] for event in source_events} == {"alpha", "broken"}
    assert all("source_run_id" in event for event in source_events)
    failed = next(event for event in logged if event["event"] == "worker.source_failed")
    assert failed["error_code"] == SourceAccessError.code
    assert "publisher" not in stream.getvalue()


@pytest.mark.asyncio
async def test_continuous_worker_logs_heartbeat_recovery_and_ordered_shutdown() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    backend = RecordingBackend(delay_seconds=0.001)
    stop_event = asyncio.Event()
    worker = Worker(
        backend,
        RecordingTelemetry(),
        worker_id="worker-continuous-events",
        policy=_policy(concurrency=1),
        jitter=lambda _maximum: 0,
        events=get_lifecycle_logger("worker-continuous-test"),
    )
    task = asyncio.create_task(
        worker.run_forever(
            (SourceSchedule("alpha", timedelta(milliseconds=4), jitter_ratio=0),),
            stop_event=stop_event,
            install_signal_handlers=False,
        )
    )
    async with asyncio.timeout(1):
        await backend.ingestion_started.wait()
    stop_event.set()
    await task

    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    names = [event["event"] for event in logged]
    assert names[0] == "worker.run_started"
    assert "worker.recovery_completed" in names
    assert "worker.heartbeat" in names
    assert names.index("worker.shutdown_started") < names.index("worker.shutdown_completed")
    assert names[-1] == "worker.run_completed"
    final_heartbeat = [event for event in logged if event["event"] == "worker.heartbeat"][-1]
    assert final_heartbeat["running"] is False
    assert final_heartbeat["stopping"] is True
