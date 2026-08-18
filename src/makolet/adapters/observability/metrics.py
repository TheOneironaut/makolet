"""Bounded Prometheus implementation of the application metrics port."""

from __future__ import annotations

from collections.abc import Sequence

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from makolet.application.worker import SourceHealth, WorkerSnapshot


class PrometheusMetrics:
    """Pre-register metrics so callers cannot create unbounded series by name."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry(auto_describe=True)
        self._counters = {
            "ingestion_completed_total": _counter(
                self.registry, "ingestion_completed_total", "Completed source files.", ("retailer",)
            ),
            "ingestion_archive_deduplicated_total": _counter(
                self.registry,
                "ingestion_archive_deduplicated_total",
                "Downloaded source files whose exact bytes reused an immutable CAS object.",
                ("retailer",),
            ),
            "ingestion_replay_completed_total": _counter(
                self.registry,
                "ingestion_replay_completed_total",
                "Successful archive replays.",
                ("retailer",),
            ),
            "ingestion_archived_without_apply_total": _counter(
                self.registry,
                "ingestion_archived_without_apply_total",
                "Source files immutably archived without normalized apply.",
                ("retailer",),
            ),
            "ingestion_failure_total": _counter(
                self.registry,
                "ingestion_failure_total",
                "Failed source-file ingestion attempts.",
                ("retailer", "status"),
            ),
            "ingestion_parser_failure_total": _counter(
                self.registry,
                "ingestion_parser_failure_total",
                "Files rejected while parsing or validating source content.",
                ("retailer", "error_code"),
            ),
            "ingestion_files_downloaded_total": _counter(
                self.registry,
                "ingestion_files_downloaded_total",
                "Successfully downloaded and archived source files.",
                ("retailer",),
            ),
            "ingestion_records_staged_total": _counter(
                self.registry,
                "ingestion_records_staged_total",
                "Source records accepted into staging.",
                ("retailer",),
            ),
            "ingestion_records_rejected_total": _counter(
                self.registry,
                "ingestion_records_rejected_total",
                "Source records rejected before apply.",
                ("retailer",),
            ),
            "ingestion_warnings_total": _counter(
                self.registry,
                "ingestion_warnings_total",
                "Non-fatal source validation warnings.",
                ("retailer",),
            ),
        }
        duration_buckets = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 180, 600)
        self._histograms = {
            "ingestion_duration_seconds": _histogram(
                self.registry,
                "ingestion_duration_seconds",
                "End-to-end source-file ingestion time.",
                ("retailer",),
                duration_buckets,
            ),
            "ingestion_download_duration_seconds": _histogram(
                self.registry,
                "ingestion_download_duration_seconds",
                "Remote download and immutable archival time per attempt.",
                ("retailer",),
                duration_buckets,
            ),
            "ingestion_parsing_duration_seconds": _histogram(
                self.registry,
                "ingestion_parsing_duration_seconds",
                "Streaming parse and staging time.",
                ("retailer",),
                duration_buckets,
            ),
            "ingestion_database_apply_duration_seconds": _histogram(
                self.registry,
                "ingestion_database_apply_duration_seconds",
                "Set-based database validation and apply time.",
                ("retailer",),
                duration_buckets,
            ),
            "ingestion_file_bytes": _histogram(
                self.registry,
                "ingestion_file_bytes",
                "Exact compressed bytes archived per source file.",
                ("retailer",),
                (1_024, 16_384, 262_144, 1_048_576, 16_777_216, 268_435_456, 2_147_483_648),
            ),
        }
        self._gauges = {
            "source_freshness_timestamp_seconds": _gauge(
                self.registry,
                "source_freshness_timestamp_seconds",
                "Unix timestamp of a source's most recent successful file.",
                ("source", "retailer"),
            ),
            "source_healthy": _gauge(
                self.registry,
                "source_healthy",
                "Whether the most recent source run was healthy (1 or 0).",
                ("source",),
            ),
            "worker_heartbeat_timestamp_seconds": _gauge(
                self.registry,
                "worker_heartbeat_timestamp_seconds",
                "Unix timestamp of the worker's most recent scheduling heartbeat.",
                (),
            ),
            "worker_active_sources": _gauge(
                self.registry,
                "worker_active_sources",
                "Number of source jobs currently executing.",
                (),
            ),
        }

    def increment(
        self,
        name: str,
        *,
        labels: dict[str, str] | None = None,
        value: int = 1,
    ) -> None:
        if value < 0:
            raise ValueError("Prometheus counters cannot be decremented")
        metric = self._counters.get(name)
        if metric is None:
            raise ValueError(f"Unknown counter metric: {name}")
        values = _validated_labels(metric, labels)
        target = metric.labels(**values) if values else metric
        target.inc(value)

    def observe(
        self,
        name: str,
        value: float,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        if value < 0:
            raise ValueError("Prometheus observations must be non-negative")
        metric = self._histograms.get(name)
        if metric is None:
            raise ValueError(f"Unknown histogram metric: {name}")
        values = _validated_labels(metric, labels)
        target = metric.labels(**values) if values else metric
        target.observe(value)

    def set_gauge(
        self,
        name: str,
        value: float,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        metric = self._gauges.get(name)
        if metric is None:
            raise ValueError(f"Unknown gauge metric: {name}")
        values = _validated_labels(metric, labels)
        target = metric.labels(**values) if values else metric
        target.set(value)

    def render(self) -> bytes:
        return generate_latest(self.registry)


class PrometheusWorkerTelemetry:
    """Translate worker lifecycle events into bounded process metrics."""

    def __init__(self, metrics: PrometheusMetrics) -> None:
        self._metrics = metrics

    async def record_heartbeat(self, snapshot: WorkerSnapshot) -> None:
        self._metrics.set_gauge(
            "worker_heartbeat_timestamp_seconds", snapshot.observed_at.timestamp()
        )
        self._metrics.set_gauge(
            "worker_active_sources",
            float(sum(health.in_flight for health in snapshot.sources)),
        )

    async def record_source_health(self, health: SourceHealth) -> None:
        healthy = health.last_succeeded_at is not None and health.consecutive_failures == 0
        self._metrics.set_gauge(
            "source_healthy",
            float(healthy),
            labels={"source": health.source_id},
        )


type _Metric = Counter | Histogram | Gauge


def _validated_labels(metric: _Metric, labels: dict[str, str] | None) -> dict[str, str]:
    supplied = labels or {}
    expected = tuple(metric._labelnames)
    if set(supplied) != set(expected):
        raise ValueError(
            f"Metric {metric._name} requires labels {sorted(expected)}, got {sorted(supplied)}"
        )
    return supplied


def _counter(
    registry: CollectorRegistry,
    name: str,
    documentation: str,
    labels: Sequence[str],
) -> Counter:
    return Counter(f"makolet_{name}", documentation, labels, registry=registry)


def _histogram(
    registry: CollectorRegistry,
    name: str,
    documentation: str,
    labels: Sequence[str],
    buckets: Sequence[float],
) -> Histogram:
    return Histogram(f"makolet_{name}", documentation, labels, buckets=buckets, registry=registry)


def _gauge(
    registry: CollectorRegistry,
    name: str,
    documentation: str,
    labels: Sequence[str],
) -> Gauge:
    return Gauge(f"makolet_{name}", documentation, labels, registry=registry)
