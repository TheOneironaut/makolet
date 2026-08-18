"""Offline safety regressions for the opt-in representative-ingestion harness."""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.engine import make_url

import tests.live.conftest as live_conftest
from tests.live.conftest import (
    MAXIMUM_ARCHIVE_BYTES,
    TRANSFER_HEADROOM_BYTES,
    LiveServiceConfiguration,
    _asyncpg_url,
    _database_url,
    _require_loopback_database_url,
    _require_loopback_url,
    _runtime_settings,
)
from tests.live.test_representative_ingestion import _canonical_value


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql://test@127.0.0.1:5432/postgres",
        "postgresql://test@localhost:5432/postgres",
        "postgresql+asyncpg://test@[::1]:5432/postgres",
    ],
)
def test_live_admin_database_accepts_only_direct_loopback_authorities(
    database_url: str,
) -> None:
    _require_loopback_database_url(database_url)

    parsed = _asyncpg_url(database_url)

    assert parsed.get_driver_name() == "asyncpg"
    assert parsed.host in {"127.0.0.1", "localhost", "::1"}
    assert not parsed.query


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql://test@203.0.113.10:5432/postgres",
        "postgresql://test@127.0.0.1:5432/postgres?host=203.0.113.10",
        "postgresql://test@127.0.0.1:5432/postgres?h%6fst=203.0.113.10",
        "postgresql://test@127.0.0.1:5432/postgres?HOST=203.0.113.10",
        "postgresql://test@127.0.0.1:5432/postgres?host=127.0.0.1&host=203.0.113.10",
        "postgresql://test@127.0.0.1:5432/postgres?service=remote",
        "postgresql://test@127.0.0.1:5432/postgres?servicefile=remote.conf",
        "postgresql://test@127.0.0.1:5432/postgres#?host=203.0.113.10",
        "mysql://test@127.0.0.1:3306/mysql",
    ],
)
def test_live_admin_database_rejects_remote_and_parser_override_paths(
    database_url: str,
) -> None:
    with pytest.raises(pytest.fail.Exception):
        _asyncpg_url(database_url)


def test_live_s3_endpoint_rejects_non_loopback_host() -> None:
    with pytest.raises(pytest.fail.Exception):
        _require_loopback_url(
            "https://203.0.113.10:8333",
            label="MAKOLET_LIVE_ACCEPTANCE_S3_ENDPOINT",
            schemes=frozenset({"http", "https"}),
        )


def test_live_database_and_runtime_settings_preserve_disposable_bounds(
    tmp_path: Path,
) -> None:
    admin_url = "postgresql://test@127.0.0.1:5432/postgres"
    database_name = "makolet_live_acceptance_test_0123456789abcdef01234567"
    database_url = _database_url(admin_url, database_name)
    parsed = make_url(database_url)
    service = LiveServiceConfiguration(
        admin_database_url=admin_url,
        s3_endpoint="http://127.0.0.1:8333",
        s3_bucket="makolet-raw",
        s3_region="us-east-1",
        s3_access_key="test-access",
        s3_secret_key="test-secret",
    )

    settings = _runtime_settings(
        service,
        database_url=database_url,
        key_prefix="live-acceptance/0123456789abcdef",
        spool_root=tmp_path,
        run_id="0123456789abcdef0123456789abcdef",
    )

    assert parsed.database == database_name
    assert parsed.host == "127.0.0.1"
    assert not parsed.query
    assert settings.ingestion_maximum_files_per_source_run == 1
    assert settings.archive_maximum_object_bytes == MAXIMUM_ARCHIVE_BYTES
    assert (
        settings.ingestion_maximum_charged_bytes_per_source_run
        == MAXIMUM_ARCHIVE_BYTES + TRANSFER_HEADROOM_BYTES
    )
    assert (
        settings.ingestion_maximum_charged_bytes_per_source_day
        == MAXIMUM_ARCHIVE_BYTES + TRANSFER_HEADROOM_BYTES
    )


def test_live_provenance_compares_equivalent_utc_timestamp_encodings() -> None:
    moment = datetime(2026, 8, 16, 11, 3, 40, tzinfo=UTC)

    assert _canonical_value("source_timestamp", moment) == "2026-08-16T11:03:40Z"
    assert _canonical_value("source_timestamp", "2026-08-16T11:03:40+00:00") == (
        "2026-08-16T11:03:40Z"
    )


class _ScriptedS3Client:
    def __init__(self, pages: list[dict[str, object]]) -> None:
        self._pages = iter(pages)
        self.events: list[str] = []
        self.closed = False

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        self.events.append("list")
        return next(self._pages)

    def delete_objects(self, **kwargs: object) -> dict[str, object]:
        objects = kwargs["Delete"]
        assert isinstance(objects, dict)
        entries = objects["Objects"]
        assert isinstance(entries, list)
        self.events.append(f"delete:{len(entries)}")
        return {}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        raise AssertionError(f"unexpected get_object call: {kwargs}")

    def close(self) -> None:
        self.closed = True


class _ListingRawBody:
    def __init__(self, chunks: tuple[bytes, ...]) -> None:
        self._chunks = chunks
        self.closed = False

    def stream(self, amount: int, *, decode_content: bool) -> Iterator[bytes]:
        assert amount > 0
        assert decode_content is False
        yield from self._chunks

    def close(self) -> None:
        self.closed = True


class _ListingSession:
    def __init__(self, raw: _ListingRawBody, *, content_length: int | None = None) -> None:
        headers = {} if content_length is None else {"content-length": str(content_length)}
        self.response = SimpleNamespace(
            url="http://127.0.0.1:8333/makolet-raw?list-type=2",
            status_code=200,
            headers=headers,
            raw=raw,
        )

    def send(self, request: SimpleNamespace) -> object:
        assert request.stream_output is True
        return self.response


class _ClientEvents:
    def __init__(self) -> None:
        self.registered: list[tuple[str, object]] = []

    def register_first(self, name: str, handler: object) -> None:
        self.registered.append((name, handler))


class _ConfiguredS3Client(_ScriptedS3Client):
    def __init__(self) -> None:
        super().__init__([])
        self.registered_events = _ClientEvents()
        self.meta = SimpleNamespace(events=self.registered_events)
        self._endpoint = SimpleNamespace(http_session=object())


def _failing_s3_cleanup(
    _service: LiveServiceConfiguration,
    _prefix: str,
) -> None:
    raise AssertionError("deliberate S3 cleanup failure")


def _successful_s3_cleanup(
    _service: LiveServiceConfiguration,
    _prefix: str,
) -> None:
    return


def _blocking_s3_list_cleanup(
    service: LiveServiceConfiguration,
    prefix: str,
) -> None:
    class BlockingClient:
        def list_objects_v2(self, **_kwargs: object) -> dict[str, object]:
            time.sleep(60)
            return {"Contents": [], "IsTruncated": False}

        def delete_objects(self, **kwargs: object) -> dict[str, object]:
            raise AssertionError(f"unexpected delete call: {kwargs}")

        def get_object(self, **kwargs: object) -> dict[str, object]:
            raise AssertionError(f"unexpected get call: {kwargs}")

        def close(self) -> None:
            return

    live_conftest._list_s3_keys(
        BlockingClient(),
        service.s3_bucket,
        f"{prefix}/",
    )


def _cleanup_limits(**overrides: Any) -> live_conftest._S3CleanupLimits:
    defaults: dict[str, Any] = {
        "page_size": 2,
        "maximum_pages": 4,
        "maximum_keys": 8,
        "maximum_key_bytes": 1_024,
        "maximum_no_progress_pages": 1,
        "maximum_token_bytes": 16,
        "maximum_requests": 12,
        "timeout_seconds": 30.0,
    }
    defaults.update(overrides)
    return live_conftest._S3CleanupLimits(**defaults)


@pytest.mark.parametrize(
    "endpoint",
    ["http://127.0.0.1:8333", "http://[::1]:8333"],
)
def test_live_s3_client_bypasses_ambient_proxy_for_plaintext_loopback(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
) -> None:
    captured: dict[str, object] = {}
    client = _ConfiguredS3Client()
    service = LiveServiceConfiguration(
        admin_database_url="postgresql://test@127.0.0.1:5432/postgres",
        s3_endpoint=endpoint,
        s3_bucket="makolet-raw",
        s3_region="us-east-1",
        s3_access_key="test-access",
        s3_secret_key="test-secret",
    )

    def create_client(name: str, **arguments: object) -> object:
        captured["name"] = name
        captured.update(arguments)
        return client

    monkeypatch.setenv("HTTP_PROXY", "http://203.0.113.10:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://203.0.113.10:8080")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.setattr("tests.live.conftest.boto3.client", create_client)

    assert live_conftest._s3_client(service) is client
    assert captured["name"] == "s3"
    assert getattr(captured["config"], "proxies", None) == {}
    assert [name for name, _handler in client.registered_events.registered] == [
        "before-send.s3.ListObjectsV2"
    ]


def test_live_s3_client_retains_proxy_semantics_for_https(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    client = _ConfiguredS3Client()
    service = LiveServiceConfiguration(
        admin_database_url="postgresql://test@127.0.0.1:5432/postgres",
        s3_endpoint="https://127.0.0.1:8333",
        s3_bucket="makolet-raw",
        s3_region="us-east-1",
        s3_access_key="test-access",
        s3_secret_key="test-secret",
    )

    def create_client(_name: str, **arguments: object) -> object:
        captured.update(arguments)
        return client

    monkeypatch.setenv("HTTPS_PROXY", "http://203.0.113.10:8080")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.setattr("tests.live.conftest.boto3.client", create_client)

    live_conftest._s3_client(service)

    assert getattr(captured["config"], "proxies", None) is None


def test_live_s3_listing_rejects_oversized_response_before_sdk_parse() -> None:
    raw = _ListingRawBody((b"12345", b"6789"))
    session = _ListingSession(raw)
    request = SimpleNamespace(stream_output=False)

    with pytest.raises(AssertionError, match="listing response exceeds"):
        live_conftest._bounded_s3_list_response(
            session,
            request,
            maximum_bytes=8,
        )

    assert request.stream_output is False
    assert raw.closed is True


def test_live_s3_listing_preserves_one_bounded_response() -> None:
    payload = b"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
    raw = _ListingRawBody((payload[:17], payload[17:]))
    session = _ListingSession(raw, content_length=len(payload))
    request = SimpleNamespace(stream_output=False)

    response = live_conftest._bounded_s3_list_response(
        session,
        request,
        maximum_bytes=len(payload),
    )

    assert b"".join(response.raw.stream(13, decode_content=False)) == payload
    assert request.stream_output is False
    assert raw.closed is True


def test_live_s3_listing_rejects_repeated_continuation_token() -> None:
    prefix = "live-acceptance/run/"
    client = _ScriptedS3Client(
        [
            {
                "Contents": [{"Key": f"{prefix}one"}],
                "IsTruncated": True,
                "NextContinuationToken": "repeat",
            },
            {
                "Contents": [{"Key": f"{prefix}two"}],
                "IsTruncated": True,
                "NextContinuationToken": "repeat",
            },
        ]
    )

    with pytest.raises(AssertionError, match="repeated continuation token"):
        live_conftest._list_s3_keys(
            client,
            "makolet-raw",
            prefix,
            limits=_cleanup_limits(),
        )


@pytest.mark.parametrize(
    ("limits", "pages", "message"),
    [
        (
            {"maximum_pages": 1},
            [
                {"Contents": [], "IsTruncated": True, "NextContinuationToken": "one"},
                {"Contents": [], "IsTruncated": False},
            ],
            "page limit",
        ),
        (
            {"maximum_keys": 1},
            [
                {
                    "Contents": [
                        {"Key": "live-acceptance/run/one"},
                        {"Key": "live-acceptance/run/two"},
                    ],
                    "IsTruncated": False,
                }
            ],
            "key limit",
        ),
        (
            {"maximum_no_progress_pages": 0},
            [{"Contents": [], "IsTruncated": True, "NextContinuationToken": "one"}],
            "without key progress",
        ),
        (
            {"maximum_token_bytes": 3},
            [{"Contents": [], "IsTruncated": True, "NextContinuationToken": "long"}],
            "continuation token limit",
        ),
    ],
)
def test_live_s3_listing_enforces_page_key_progress_and_token_bounds(
    limits: dict[str, object],
    pages: list[dict[str, object]],
    message: str,
) -> None:
    client = _ScriptedS3Client(pages)

    with pytest.raises(AssertionError, match=message):
        live_conftest._list_s3_keys(
            client,
            "makolet-raw",
            "live-acceptance/run/",
            limits=_cleanup_limits(**limits),
        )


def test_live_s3_listing_enforces_request_and_monotonic_deadline_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]

    class SlowClient(_ScriptedS3Client):
        def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
            response = super().list_objects_v2(**kwargs)
            clock[0] = 2.0
            return response

    monkeypatch.setattr("tests.live.conftest.time.monotonic", lambda: clock[0])
    slow_client = SlowClient([{"Contents": [], "IsTruncated": False}])
    with pytest.raises(AssertionError, match="deadline"):
        live_conftest._list_s3_keys(
            slow_client,
            "makolet-raw",
            "live-acceptance/run/",
            limits=_cleanup_limits(timeout_seconds=1.0),
        )

    client = _ScriptedS3Client(
        [{"Contents": [], "IsTruncated": True, "NextContinuationToken": "next"}]
    )
    with pytest.raises(AssertionError, match="request limit"):
        live_conftest._list_s3_keys(
            client,
            "makolet-raw",
            "live-acceptance/run/",
            limits=_cleanup_limits(maximum_requests=1),
        )


def test_live_s3_purge_streams_bounded_pages_and_preserves_one_object_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = "live-acceptance/run/"
    client = _ScriptedS3Client(
        [
            {
                "Contents": [{"Key": f"{prefix}one"}],
                "IsTruncated": True,
                "NextContinuationToken": "next",
            },
            {"Contents": [{"Key": f"{prefix}two"}], "IsTruncated": False},
            {"Contents": [], "IsTruncated": False},
        ]
    )
    service = LiveServiceConfiguration(
        admin_database_url="postgresql://test@127.0.0.1:5432/postgres",
        s3_endpoint="http://127.0.0.1:8333",
        s3_bucket="makolet-raw",
        s3_region="us-east-1",
        s3_access_key="test-access",
        s3_secret_key="test-secret",
    )
    monkeypatch.setattr(live_conftest, "_s3_client", lambda _service: client)

    live_conftest._purge_s3_prefix(service, "live-acceptance/run")

    assert client.events == ["list", "delete:1", "list", "delete:1", "list"]
    assert client.closed is True


def test_live_cleanup_attempts_database_drop_after_s3_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = LiveServiceConfiguration(
        admin_database_url="postgresql://test@127.0.0.1:5432/postgres",
        s3_endpoint="http://127.0.0.1:8333",
        s3_bucket="makolet-raw",
        s3_region="us-east-1",
        s3_access_key="test-access",
        s3_secret_key="test-secret",
    )
    dropped: list[str] = []

    async def record_drop(_url: str, database_name: str) -> None:
        dropped.append(database_name)

    monkeypatch.setattr(live_conftest, "_drop_database", record_drop)

    with pytest.raises(ExceptionGroup, match="cleanup failed") as captured:
        live_conftest._cleanup_live_resources(
            service,
            key_prefix="live-acceptance/run",
            prefix_owned=True,
            database_name="makolet_live_acceptance_test_0123456789abcdef01234567",
            database_created=True,
            s3_cleanup_target=_failing_s3_cleanup,
            s3_cleanup_timeout_seconds=5,
        )

    assert dropped == ["makolet_live_acceptance_test_0123456789abcdef01234567"]
    assert len(captured.value.exceptions) == 1


def test_live_cleanup_process_boundary_preserves_successful_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = LiveServiceConfiguration(
        admin_database_url="postgresql://test@127.0.0.1:5432/postgres",
        s3_endpoint="http://127.0.0.1:8333",
        s3_bucket="makolet-raw",
        s3_region="us-east-1",
        s3_access_key="test-access",
        s3_secret_key="test-secret",
    )
    dropped: list[str] = []

    async def record_drop(_url: str, database_name: str) -> None:
        dropped.append(database_name)

    monkeypatch.setattr(live_conftest, "_drop_database", record_drop)

    live_conftest._cleanup_live_resources(
        service,
        key_prefix="live-acceptance/run",
        prefix_owned=True,
        database_name="makolet_live_acceptance_test_0123456789abcdef01234567",
        database_created=True,
        s3_cleanup_target=_successful_s3_cleanup,
        s3_cleanup_timeout_seconds=5,
    )

    assert dropped == ["makolet_live_acceptance_test_0123456789abcdef01234567"]


def test_live_cleanup_kills_blocked_s3_list_before_database_drop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = LiveServiceConfiguration(
        admin_database_url="postgresql://test@127.0.0.1:5432/postgres",
        s3_endpoint="http://127.0.0.1:8333",
        s3_bucket="makolet-raw",
        s3_region="us-east-1",
        s3_access_key="test-access",
        s3_secret_key="test-secret",
    )
    dropped: list[str] = []

    async def record_drop(_url: str, database_name: str) -> None:
        dropped.append(database_name)

    monkeypatch.setattr(live_conftest, "_drop_database", record_drop)
    started_at = time.monotonic()

    with pytest.raises(ExceptionGroup, match="cleanup failed") as captured:
        live_conftest._cleanup_live_resources(
            service,
            key_prefix="live-acceptance/run",
            prefix_owned=True,
            database_name="makolet_live_acceptance_test_0123456789abcdef01234567",
            database_created=True,
            s3_cleanup_target=_blocking_s3_list_cleanup,
            s3_cleanup_timeout_seconds=3,
        )

    assert time.monotonic() - started_at < 7
    assert dropped == ["makolet_live_acceptance_test_0123456789abcdef01234567"]
    assert len(captured.value.exceptions) == 1
    assert "deadline" in str(captured.value.exceptions[0])
