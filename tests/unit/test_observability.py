from __future__ import annotations

import io
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry
from structlog.contextvars import bound_contextvars

from makolet.adapters.observability.logging import (
    configure_logging,
    get_lifecycle_logger,
    get_logger,
)
from makolet.adapters.observability.metrics import PrometheusMetrics, PrometheusWorkerTelemetry
from makolet.application.observability import LifecycleEvent
from makolet.application.worker import SourceHealth, WorkerSnapshot
from makolet.interfaces.api import create_metrics_app


def test_prometheus_metrics_are_predeclared_bounded_and_renderable() -> None:
    metrics = PrometheusMetrics(CollectorRegistry())

    metrics.increment("ingestion_completed_total", labels={"retailer": "demo"})
    metrics.increment(
        "ingestion_archive_deduplicated_total",
        labels={"retailer": "demo"},
    )
    metrics.increment("ingestion_records_staged_total", labels={"retailer": "demo"}, value=12)
    metrics.observe("ingestion_download_duration_seconds", 0.25, labels={"retailer": "demo"})
    metrics.set_gauge(
        "source_healthy",
        1,
        labels={"source": "demo-source"},
    )
    metrics.set_gauge("worker_heartbeat_timestamp_seconds", 1_786_444_800)
    rendered = metrics.render().decode()

    assert 'makolet_ingestion_completed_total{retailer="demo"} 1.0' in rendered
    assert 'makolet_ingestion_archive_deduplicated_total{retailer="demo"} 1.0' in rendered
    assert 'makolet_ingestion_records_staged_total{retailer="demo"} 12.0' in rendered
    assert 'makolet_ingestion_download_duration_seconds_count{retailer="demo"} 1.0' in rendered
    assert 'makolet_source_healthy{source="demo-source"} 1.0' in rendered
    assert "makolet_worker_heartbeat_timestamp_seconds 1.7864448e+09" in rendered


def test_metrics_reject_unknown_names_wrong_labels_and_negative_values() -> None:
    metrics = PrometheusMetrics(CollectorRegistry())

    with pytest.raises(ValueError, match="Unknown counter"):
        metrics.increment("runtime_created_from_user_input_total")
    with pytest.raises(ValueError, match="requires labels"):
        metrics.increment("ingestion_completed_total", labels={"source": "wrong"})
    with pytest.raises(ValueError, match="decremented"):
        metrics.increment("ingestion_completed_total", labels={"retailer": "demo"}, value=-1)
    with pytest.raises(ValueError, match="non-negative"):
        metrics.observe("ingestion_duration_seconds", -0.1, labels={"retailer": "demo"})


def test_metrics_only_app_exposes_the_owning_process_registry() -> None:
    metrics = PrometheusMetrics(CollectorRegistry())
    metrics.set_gauge("worker_heartbeat_timestamp_seconds", 1_786_449_600)
    client = TestClient(create_metrics_app(metrics.registry))

    assert client.get("/healthz").json() == {"status": "ok"}
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "makolet_worker_heartbeat_timestamp_seconds 1.7864496e+09" in response.text
    assert client.get("/api/v1/products/search?query=milk").status_code == 404


def test_compose_prometheus_disables_remote_lifecycle_management() -> None:
    compose = (Path(__file__).parents[2] / "compose.yaml").read_text(encoding="utf-8")

    assert "--web.listen-address=0.0.0.0:9090" in compose
    assert "--web.enable-lifecycle" not in compose


def test_structured_logging_redacts_credentials_and_neutralizes_log_injection() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)

    get_logger("test").info(
        "source\nfailed",
        source="demo",
        source_file_id="file-1",
        password="do-not-print",
        nested={"access_key": "also-secret", "object_key": "raw/sha256/ok"},
        remote_url="https://user:password@example.test/path?token=secret-value",
        payload=b"raw secret-looking bytes",
        terminal="unsafe\x1b[2J",
        unicode_terminal="unsafe\x7f\u009b\ud800\u2028\u2029\u202e",
    )
    rendered = stream.getvalue()
    event = json.loads(rendered)

    assert event["event"] == "source\\nfailed"
    assert event["password"] == "[REDACTED]"
    assert event["nested"] == {
        "access_key": "[REDACTED]",
        "object_key": "raw/sha256/ok",
    }
    assert event["remote_url"] == ("https://[REDACTED]@example.test/path?token=[REDACTED]")
    assert event["payload"] == "[BYTES:24]"
    assert event["source_file_id"] == "file-1"
    assert event["terminal"] == "unsafe\\u001b[2J"
    assert event["unicode_terminal"] == ("unsafe\\u007f\\u009b\\ud800\\u2028\\u2029\\u202e")
    assert len(rendered.splitlines()) == 1


def test_stdlib_httpx_logging_redacts_azure_sas_signature() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)

    sas_url = (
        "https://storage.example.test/container/object.xml"
        "?sv=2026-05-05&sp=r&sig=must-not-appear&se=2026-08-13T00%3A00%3A00Z"
    )  # secret-scan: allow
    logging.getLogger("httpx").info(
        'HTTP Request: %s %s "%s %d %s"',
        "GET",
        sas_url,
        "HTTP/1.1",
        200,
        "OK",
    )

    rendered = stream.getvalue()
    event = json.loads(rendered)

    assert event["event"] == (
        "HTTP Request: GET https://storage.example.test/container/object.xml?sv=2026-05-05"
        '&sp=r&sig=[REDACTED]&se=2026-08-13T00%3A00%3A00Z "HTTP/1.1 200 OK"'
    )
    assert "must-not-appear" not in rendered
    assert len(rendered.splitlines()) == 1


def test_stdlib_httpx_logging_redacts_encoded_query_secret_keys_fail_closed() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)

    encoded_url = (
        "https://storage.example.test/container/object.xml"
        "?view=keep%26sig%3Dnot-a-parameter"
        "&redirect=https://identity.example.test/callback?to%6Ben=nested-must-not-appear"
        "&s%26ig=keep-encoded-ampersand-key"
        "&sig%3Dshadow=keep-encoded-equals-key"
        "&S%69G=encoded-signature-must-not-appear"
        "&Sign%61ture=long-signature-must-not-appear"
        "&To%6Ben=token-must-not-appear"
        "&K%65Y=key-must-not-appear"
        "&Pass%77ord=password-must-not-appear"
        "&Se%63ret=secret-must-not-appear"
        "&X-Amz-Cred%65ntial=encoded-credential-must-not-appear"
        "&A%75TH=auth-must-not-appear"
        "&malformed%2G=malformed-key-value-must-not-appear"
        "&note=keep%2Gverbatim"
    )  # secret-scan: allow
    logging.getLogger("httpx").info(
        'HTTP Request: %s %s "%s %d %s"',
        "GET",
        encoded_url,
        "HTTP/1.1",
        200,
        "OK",
    )

    rendered = stream.getvalue()
    event = json.loads(rendered)

    assert event["event"] == (
        "HTTP Request: GET https://storage.example.test/container/object.xml"
        "?view=keep%26sig%3Dnot-a-parameter"
        "&redirect=https://identity.example.test/callback?to%6Ben=[REDACTED]"
        "&s%26ig=keep-encoded-ampersand-key"
        "&sig%3Dshadow=keep-encoded-equals-key"
        "&S%69G=[REDACTED]"
        "&Sign%61ture=[REDACTED]"
        "&To%6Ben=[REDACTED]"
        "&K%65Y=[REDACTED]"
        "&Pass%77ord=[REDACTED]"
        "&Se%63ret=[REDACTED]"
        "&X-Amz-Cred%65ntial=[REDACTED]"
        "&A%75TH=[REDACTED]"
        "&malformed%2G=[REDACTED]"
        '&note=keep%2Gverbatim "HTTP/1.1 200 OK"'
    )
    assert "must-not-appear" not in rendered
    assert len(rendered.splitlines()) == 1


def test_stdlib_httpx_logging_redacts_overlong_query_keys_fail_closed() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)

    overlong_key = f"namespace.{'a' * 256}.token"
    logging.getLogger("httpx").info(
        "HTTP Request: GET https://example.test/object?%s=%s",
        overlong_key,
        "overlong-key-secret-must-not-appear",  # secret-scan: allow
    )

    rendered = stream.getvalue()
    event = json.loads(rendered)

    assert event["event"] == (
        f"HTTP Request: GET https://example.test/object?{overlong_key}=[REDACTED]"
    )
    assert "must-not-appear" not in rendered
    assert len(rendered.splitlines()) == 1


def test_logging_suppresses_tracebacks_and_reconfiguration_has_one_current_handler() -> None:
    first = io.StringIO()
    second = io.StringIO()
    configure_logging(level="INFO", stream=first)
    logger = get_logger("reconfigured")
    logger.info("before")

    configure_logging(level="INFO", stream=second)
    logger.error(
        "after",
        exc_info=(
            RuntimeError,
            RuntimeError("password=must-not-appear"),  # secret-scan: allow
            None,
        ),
    )

    assert [json.loads(line)["event"] for line in first.getvalue().splitlines()] == ["before"]
    second_event = json.loads(second.getvalue())
    assert second_event["event"] == "after"
    assert second_event["exception"] == "[SUPPRESSED]"
    assert "must-not-appear" not in second.getvalue()
    assert len(logging.getLogger().handlers) == 1


def test_lifecycle_logger_accepts_only_bounded_safe_operational_context() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    events = get_lifecycle_logger("lifecycle-test")

    with events.context(
        correlation_id="run-1",
        run_id="run-1",
        source_id="source-1",
    ):
        events.info(
            LifecycleEvent.DOWNLOAD_COMPLETED,
            content_length=123,
            duration_seconds=0.25,
            retailer_id="retailer-1",
            status="completed",
        )
    events.info(LifecycleEvent.WORKER_HEARTBEAT, running=True, stopping=False)
    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    event = logged[0]

    assert event["event"] == "download.completed"
    assert event["correlation_id"] == "run-1"
    assert event["content_length"] == 123
    assert event["duration_seconds"] == 0.25
    assert "correlation_id" not in logged[1]
    assert "run_id" not in logged[1]
    assert "source_id" not in logged[1]
    with pytest.raises(ValueError, match="Unsupported lifecycle log fields"):
        events.info(LifecycleEvent.DOWNLOAD_STARTED, remote_url="https://secret.test")
    with pytest.raises(ValueError, match="Unsupported lifecycle log fields"):
        events.info(LifecycleEvent.ARCHIVE_STORED, raw_data="payload")
    with (
        pytest.raises(ValueError, match="identifier"),
        events.context(source_id="source\nforged"),
    ):
        pass


def test_lifecycle_processor_drops_untrusted_ambient_context() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    events = get_lifecycle_logger("ambient-context-test")

    with bound_contextvars(
        remote_url="https://user:password@example.test/private?token=secret",
        raw_data="retailer payload",
        source_id="source\nforged",
    ):
        events.info(LifecycleEvent.DOWNLOAD_STARTED, status="running")
    event = json.loads(stream.getvalue())

    assert "remote_url" not in event
    assert "raw_data" not in event
    assert event["source_id"] == "[INVALID]"
    assert "example.test" not in stream.getvalue()
    assert "retailer payload" not in stream.getvalue()


def test_lifecycle_run_context_preserves_an_existing_request_correlation() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    events = get_lifecycle_logger("request-context-test")

    with (
        events.context(correlation_id="request-1"),
        events.context_if_absent(correlation_id="generated-run", run_id="generated-run"),
    ):
        events.info(LifecycleEvent.DISCOVERY_STARTED, status="running")
    event = json.loads(stream.getvalue())

    assert event["correlation_id"] == "request-1"
    assert event["run_id"] == "generated-run"


@pytest.mark.asyncio
async def test_worker_telemetry_exports_heartbeat_activity_and_source_health() -> None:
    metrics = PrometheusMetrics(CollectorRegistry())
    telemetry = PrometheusWorkerTelemetry(metrics)
    observed_at = datetime(2026, 8, 11, 12, tzinfo=UTC)
    health = SourceHealth(
        source_id="demo-source",
        in_flight=True,
        successful_runs=1,
        last_succeeded_at=observed_at,
    )

    await telemetry.record_source_health(health)
    await telemetry.record_heartbeat(
        WorkerSnapshot(
            worker_id="worker-1",
            observed_at=observed_at,
            running=True,
            stopping=False,
            queue_depth=0,
            sources=(health,),
        )
    )
    rendered = metrics.render().decode()

    assert 'makolet_source_healthy{source="demo-source"} 1.0' in rendered
    assert "makolet_worker_active_sources 1.0" in rendered
    assert "makolet_worker_heartbeat_timestamp_seconds 1.7864496e+09" in rendered


def test_logging_rejects_unknown_level() -> None:
    with pytest.raises(ValueError, match="Unknown log level"):
        configure_logging(level="LOUD")
