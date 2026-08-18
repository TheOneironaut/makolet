"""Measurement helpers shared by benchmark scenarios."""

from __future__ import annotations

import math
import os
import platform
import statistics
import sys
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import psutil  # type: ignore[import-untyped]


@dataclass(frozen=True, slots=True)
class MemoryReading:
    baseline_rss_bytes: int
    peak_rss_bytes: int

    @property
    def peak_delta_bytes(self) -> int:
        return max(0, self.peak_rss_bytes - self.baseline_rss_bytes)


class PeakRssSampler:
    """Sample the current process RSS in a short-lived background thread."""

    def __init__(self, interval_seconds: float = 0.01) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._interval_seconds = interval_seconds
        self._process = psutil.Process(os.getpid())
        self._baseline = self._process.memory_info().rss
        self._peak = self._baseline
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._sample, name="benchmark-rss", daemon=True)

    def __enter__(self) -> PeakRssSampler:
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._stopped.set()
        self._thread.join(timeout=max(1.0, self._interval_seconds * 10))
        self._peak = max(self._peak, self._process.memory_info().rss)

    @property
    def reading(self) -> MemoryReading:
        return MemoryReading(self._baseline, self._peak)

    def _sample(self) -> None:
        while not self._stopped.wait(self._interval_seconds):
            self._peak = max(self._peak, self._process.memory_info().rss)


@contextmanager
def measured_memory(interval_seconds: float = 0.01) -> Iterator[PeakRssSampler]:
    sampler = PeakRssSampler(interval_seconds)
    with sampler:
        yield sampler


def latency_summary(milliseconds: Sequence[float]) -> dict[str, float | int]:
    """Summarize observed samples without assuming a probability distribution."""

    if not milliseconds:
        raise ValueError("At least one latency sample is required")
    ordered = sorted(milliseconds)
    return {
        "samples": len(ordered),
        "minimum_ms": round(ordered[0], 3),
        "mean_ms": round(statistics.fmean(ordered), 3),
        "p50_ms": round(_percentile(ordered, 0.50), 3),
        "p95_ms": round(_percentile(ordered, 0.95), 3),
        "p99_ms": round(_percentile(ordered, 0.99), 3),
        "maximum_ms": round(ordered[-1], 3),
    }


def hardware_environment(workspace: Path) -> dict[str, object]:
    """Capture hardware and runtime facts relevant to interpreting measurements."""

    memory = psutil.virtual_memory()
    disk = psutil.disk_usage(workspace.anchor or str(workspace))
    frequency = psutil.cpu_freq()
    return {
        "platform": platform.platform(),
        "os": platform.system(),
        "os_release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "physical_cpu_cores": psutil.cpu_count(logical=False),
        "logical_cpu_cores": psutil.cpu_count(logical=True),
        "cpu_max_mhz": round(frequency.max, 1) if frequency is not None else None,
        "physical_memory_bytes": memory.total,
        "available_memory_at_start_bytes": memory.available,
        "filesystem_total_bytes": disk.total,
        "filesystem_free_at_start_bytes": disk.free,
        "python": sys.version,
        "python_implementation": platform.python_implementation(),
        "process_id": os.getpid(),
    }


def bytes_to_mib(value: int) -> float:
    return round(value / (1024 * 1024), 3)


def rows_per_second(rows: int, duration_seconds: float) -> float:
    if duration_seconds <= 0:
        return math.inf
    return round(rows / duration_seconds, 2)


def _percentile(ordered: Sequence[float], fraction: float) -> float:
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1))
    return ordered[index]
