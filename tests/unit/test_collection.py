from __future__ import annotations

import asyncio
import io
import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import Protocol, cast
from uuid import UUID

import pytest

from makolet.adapters.observability.logging import configure_logging, get_lifecycle_logger
from makolet.application import collection as collection_module
from makolet.application.collection import CollectionOperations, CollectionPolicy
from makolet.application.models import (
    CollectionAttempt,
    CollectionChargeBudget,
    CollectionScope,
    DiscoveryCursor,
    DiscoveryPage,
    DiscoveryRunBudget,
    IngestionResult,
    RegisteredSourceFile,
)
from makolet.application.observability import LifecycleEvent, LifecycleLogger
from makolet.application.ports import MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES
from makolet.domain.enums import (
    CompressionFormat,
    DocumentType,
    IngestionStatus,
    SourceProtocol,
)
from makolet.domain.errors import (
    ChargeBudgetExceededError,
    DownloadLimitError,
    QuarantinedFileError,
    SourceResponseError,
)
from makolet.domain.models import RemoteFile


class FakeAdapter:
    source_id = "demo"

    def __init__(self, pages: dict[str | None, DiscoveryPage]) -> None:
        self._pages = pages
        self.calls: list[str | None] = []

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: DiscoveryRunBudget | None = None,
    ) -> DiscoveryPage:
        del budget
        assert limit == 100
        value = cursor.value if cursor is not None else None
        self.calls.append(value)
        return self._pages[value]


class ListingBudgetProbe(Protocol):
    def begin_request(self) -> None: ...

    def consume_bytes(self, byte_count: int) -> None: ...


class BudgetedTerminalAdapter:
    source_id = "demo"

    def __init__(self) -> None:
        self.calls: list[str | None] = []

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: object | None = None,
    ) -> DiscoveryPage:
        assert limit == 100
        assert budget is not None
        probe = cast(ListingBudgetProbe, budget)
        probe.begin_request()
        probe.consume_bytes(6)
        value = cursor.value if cursor is not None else None
        self.calls.append(value)
        index = int(value or 0)
        source_file = remote(index)
        if index % 2 == 0:
            source_file = replace(
                source_file,
                document_type=DocumentType.UNKNOWN,
                compression=CompressionFormat.UNKNOWN,
            )
        next_cursor = DiscoveryCursor(str(index + 1)) if index < 3 else None
        return DiscoveryPage((source_file,), next_cursor, next_cursor is None)


class SlowBudgetedAdapter:
    source_id = "demo"

    def __init__(self) -> None:
        self.calls: list[str | None] = []

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: object | None = None,
    ) -> DiscoveryPage:
        assert limit == 100
        assert budget is not None
        cast(ListingBudgetProbe, budget).begin_request()
        self.calls.append(cursor.value if cursor is not None else None)
        await asyncio.sleep(0.05)
        return DiscoveryPage((remote(1),), None, True)


class AllRecognizedFilesTerminalRepository(collection_module._MemoryCollectionRepository):
    def __init__(self) -> None:
        super().__init__()
        self.finished_truncation_reasons: list[str | None] = []

    async def is_terminal(self, remote_file: RemoteFile, *, archive_only: bool) -> bool:
        del remote_file, archive_only
        return True

    async def finish_attempt(
        self,
        attempt_id: UUID,
        *,
        status: str,
        truncated: bool,
        traversal_complete: bool,
        truncation_reason: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        self.finished_truncation_reasons.append(truncation_reason)
        await super().finish_attempt(
            attempt_id,
            status=status,
            truncated=truncated,
            traversal_complete=traversal_complete,
            truncation_reason=truncation_reason,
            error_code=error_code,
            error_message=error_message,
        )


class FakeIngestion:
    def __init__(
        self,
        *,
        enforce_charge_budget: bool = False,
        duplicate_remote_ids: frozenset[str] = frozenset(),
    ) -> None:
        self.files: list[RemoteFile] = []
        self.attempted_files: list[RemoteFile] = []
        self.maximum_charged_bytes: list[int | None] = []
        self._enforce_charge_budget = enforce_charge_budget
        self._duplicate_remote_ids = duplicate_remote_ids
        self._registered: dict[str, UUID] = {}

    async def register(self, remote_file: RemoteFile) -> RegisteredSourceFile:
        already_registered = remote_file.remote_id in self._registered
        source_file_id = self._registered.setdefault(
            remote_file.remote_id,
            UUID(int=len(self._registered) + 1),
        )
        return RegisteredSourceFile(
            source_file_id=source_file_id,
            remote_file=remote_file,
            status=IngestionStatus.DISCOVERED,
            already_registered=already_registered,
        )

    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        self.attempted_files.append(remote_file)
        self.maximum_charged_bytes.append(maximum_charged_bytes)
        if remote_file in self.files:
            return IngestionResult(
                source_file_id=self._registered[remote_file.remote_id],
                status=IngestionStatus.COMPLETED,
                content_sha256="0" * 64,
                stage=None,
                apply=None,
                duplicate=True,
                transferred_bytes=0,
            )
        if (
            self._enforce_charge_budget
            and maximum_charged_bytes is not None
            and remote_file.content_length is not None
            and remote_file.content_length > maximum_charged_bytes
        ):
            raise ChargeBudgetExceededError("simulated remaining archive budget")
        self.files.append(remote_file)
        return IngestionResult(
            source_file_id=self._registered[remote_file.remote_id],
            status=IngestionStatus.COMPLETED,
            content_sha256="0" * 64,
            stage=None,
            apply=None,
            duplicate=remote_file.remote_id in self._duplicate_remote_ids,
            transferred_bytes=remote_file.content_length or 0,
        )

    async def archive_only(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        result = await self.ingest(
            remote_file,
            maximum_charged_bytes=maximum_charged_bytes,
        )
        return IngestionResult(
            source_file_id=result.source_file_id,
            status=IngestionStatus.ARCHIVED,
            content_sha256=result.content_sha256,
            stage=None,
            apply=None,
            duplicate=result.duplicate,
            transferred_bytes=result.transferred_bytes,
        )

    async def replay(
        self,
        source_file_id: UUID,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> IngestionResult:
        del rebuild_run_id
        return IngestionResult(
            source_file_id=source_file_id,
            status=IngestionStatus.COMPLETED,
            content_sha256="0" * 64,
            stage=None,
            apply=None,
            replayed=True,
        )


class TransferringBudgetFailureIngestion(FakeIngestion):
    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        self.attempted_files.append(remote_file)
        self.maximum_charged_bytes.append(maximum_charged_bytes)
        raise ChargeBudgetExceededError(
            "simulated streamed budget exhaustion",
            transferred_bytes=(
                maximum_charged_bytes + 1 if maximum_charged_bytes is not None else 100
            ),
        )


class SuccessfulRetryOverheadIngestion(FakeIngestion):
    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        result = await super().ingest(
            remote_file,
            maximum_charged_bytes=maximum_charged_bytes,
        )
        return replace(result, transferred_bytes=result.transferred_bytes + 25)


class AmbiguousPostCommitFailureIngestion(FakeIngestion):
    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        self.attempted_files.append(remote_file)
        self.maximum_charged_bytes.append(maximum_charged_bytes)
        raise OSError("simulated ambiguous post-commit failure")


class PermanentOversizeIngestion(FakeIngestion):
    def __init__(
        self,
        repository: collection_module._MemoryCollectionRepository,
        *,
        transferred_bytes: int,
    ) -> None:
        super().__init__()
        self._repository = repository
        self._transferred_bytes = transferred_bytes

    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        self.attempted_files.append(remote_file)
        self.maximum_charged_bytes.append(maximum_charged_bytes)
        await self._repository.note_terminal(remote_file, archive_only=False)
        raise DownloadLimitError(
            "simulated permanent object limit",
            transferred_bytes=self._transferred_bytes,
        )


class FinalFrameOvershootIngestion(FakeIngestion):
    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        if self.files:
            self.attempted_files.append(remote_file)
            self.maximum_charged_bytes.append(maximum_charged_bytes)
            assert maximum_charged_bytes == 10
            raise ChargeBudgetExceededError(
                "simulated final transport frame",
                transferred_bytes=11,
            )
        return await super().ingest(
            remote_file,
            maximum_charged_bytes=maximum_charged_bytes,
        )


class LifecycleFakeIngestion(FakeIngestion):
    def __init__(self, events: LifecycleLogger) -> None:
        super().__init__()
        self._events = events

    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        result = await super().ingest(
            remote_file,
            maximum_charged_bytes=maximum_charged_bytes,
        )
        self._events.info(
            LifecycleEvent.INGESTION_COMPLETED,
            source_file_id=result.source_file_id,
            status="completed",
        )
        return result


class FakeMetrics:
    def __init__(self) -> None:
        self.gauges: list[tuple[str, float, dict[str, str] | None]] = []

    def increment(
        self,
        name: str,
        *,
        labels: dict[str, str] | None = None,
        value: int = 1,
    ) -> None:
        raise AssertionError((name, labels, value))

    def observe(
        self,
        name: str,
        value: float,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        raise AssertionError((name, value, labels))

    def set_gauge(
        self,
        name: str,
        value: float,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        self.gauges.append((name, value, labels))


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 8, 11, 15, tzinfo=UTC)


class FakeCatalogBootstrap:
    def __init__(self) -> None:
        self.source_file_ids: list[UUID] = []

    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]:
        self.source_file_ids.append(source_file_id)
        return {"source_file_id": source_file_id, "bootstrapped_items": 1}


def remote(
    index: int,
    *,
    source_timestamp: datetime | None = None,
    content_length: int | None = None,
) -> RemoteFile:
    filename = f"PriceFull7290000000001-001-20260811{index:04d}.gz"
    return RemoteFile(
        retailer_id="demo-retailer",
        portal_id="demo-portal",
        protocol=SourceProtocol.HTTPS,
        remote_id=f"demo:{filename}",
        download_url=f"https://example.test/{filename}",
        original_filename=filename,
        document_type=DocumentType.PRICE_FULL,
        compression=CompressionFormat.GZIP,
        discovered_at=datetime(2026, 8, 11, 12, tzinfo=UTC),
        source_timestamp=source_timestamp,
        content_length=content_length,
    )


class InterruptAfterChargeRepository(collection_module._MemoryCollectionRepository):
    def __init__(self) -> None:
        super().__init__()
        self.charge_totals: list[int] = []
        self._interrupt_next_boundary = True

    async def settle_transfer(
        self,
        attempt_id: UUID,
        source_file_id: UUID,
        remote_file: RemoteFile,
        transferred_bytes: int,
    ) -> CollectionChargeBudget:
        budget = await super().settle_transfer(
            attempt_id,
            source_file_id,
            remote_file,
            transferred_bytes,
        )
        self.charge_totals.append(budget.day_charged_bytes)
        return budget

    async def advance_attempt(
        self,
        attempt_id: UUID,
        *,
        expected_cursor: str | None,
        expected_page_offset: int,
        cursor: str | None,
        page_offset: int,
        discovered_delta: int = 0,
        recognized_delta: int = 0,
        unknown_delta: int = 0,
        processed_delta: int = 0,
        duplicate_delta: int = 0,
    ) -> CollectionAttempt:
        if self.charge_totals and self._interrupt_next_boundary:
            self._interrupt_next_boundary = False
            raise SourceResponseError("simulated boundary commit interruption")
        return await super().advance_attempt(
            attempt_id,
            expected_cursor=expected_cursor,
            expected_page_offset=expected_page_offset,
            cursor=cursor,
            page_offset=page_offset,
            discovered_delta=discovered_delta,
            recognized_delta=recognized_delta,
            unknown_delta=unknown_delta,
            processed_delta=processed_delta,
            duplicate_delta=duplicate_delta,
        )


class TerminalRejectingIngestion(FakeIngestion):
    def __init__(
        self,
        repository: collection_module._MemoryCollectionRepository,
    ) -> None:
        super().__init__()
        self._repository = repository

    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        self.attempted_files.append(remote_file)
        self.maximum_charged_bytes.append(maximum_charged_bytes)
        await self._repository.note_terminal(remote_file, archive_only=False)
        raise QuarantinedFileError("simulated rejected archived file")


@pytest.mark.asyncio
async def test_collection_stops_at_run_bound_and_exports_success_freshness() -> None:
    timestamp = datetime(2026, 8, 11, 14, tzinfo=UTC)
    adapter = FakeAdapter(
        {
            None: DiscoveryPage(
                files=(remote(1, source_timestamp=timestamp), remote(2)),
                next_cursor=DiscoveryCursor("next"),
                complete=False,
            )
        }
    )
    ingestion = FakeIngestion()
    metrics = FakeMetrics()
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        ingestion,
        policy=CollectionPolicy(
            maximum_files_per_source_run=1,
            maximum_reported_files=1,
        ),
        metrics=metrics,
        clock=FixedClock(),
    )

    result = await subject.ingest_source("demo")

    assert result["status"] == "bounded"
    assert result["run_truncated"] is True
    assert result["file_count"] == 1
    assert adapter.calls == [None]
    assert ingestion.files == [remote(1, source_timestamp=timestamp)]
    assert metrics.gauges == [
        (
            "source_freshness_timestamp_seconds",
            timestamp.timestamp(),
            {"source": "demo", "retailer": "demo-retailer"},
        )
    ]


@pytest.mark.asyncio
async def test_listing_request_budget_counts_unknown_and_terminal_pages_then_resumes() -> None:
    adapter = BudgetedTerminalAdapter()
    repository = AllRecognizedFilesTerminalRepository()
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        FakeIngestion(),
        repository=repository,
        policy=CollectionPolicy(
            maximum_listing_requests_per_source_run=2,
            maximum_listing_bytes_per_source_run=100,
            maximum_listing_elapsed_seconds_per_source_run=5,
        ),
    )

    first = await subject.ingest_source("demo")
    second = await subject.ingest_source("demo")

    assert first["status"] == "bounded"
    assert first["truncation_reason"] == "listing_request_limit"
    assert first["discovered_count"] == 2
    assert first["skipped_unknown_count"] == 1
    assert first["listing_request_count"] == 2
    assert first["listing_bytes"] == 12
    assert second["status"] == "completed"
    assert second["discovered_count"] == 2
    assert second["listing_request_count"] == 2
    assert second["listing_bytes"] == 12
    assert adapter.calls == [None, "1", "2", "3"]
    assert repository.finished_truncation_reasons == ["discovery_limit", None]


@pytest.mark.asyncio
async def test_listing_byte_budget_stops_before_materializing_the_next_unique_page() -> None:
    adapter = BudgetedTerminalAdapter()
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        FakeIngestion(),
        repository=AllRecognizedFilesTerminalRepository(),
        policy=CollectionPolicy(
            maximum_listing_requests_per_source_run=10,
            maximum_listing_bytes_per_source_run=10,
            maximum_listing_elapsed_seconds_per_source_run=5,
        ),
    )

    result = await subject.ingest_source("demo")

    assert result["status"] == "bounded"
    assert result["truncation_reason"] == "listing_byte_limit"
    assert result["discovered_count"] == 1
    assert result["listing_request_count"] == 2
    assert result["listing_bytes"] == 6
    assert adapter.calls == [None]


@pytest.mark.asyncio
async def test_listing_elapsed_budget_cancels_adapter_work_without_advancing_cursor() -> None:
    adapter = SlowBudgetedAdapter()
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        FakeIngestion(),
        policy=CollectionPolicy(
            maximum_listing_requests_per_source_run=10,
            maximum_listing_bytes_per_source_run=100,
            maximum_listing_elapsed_seconds_per_source_run=0.01,
        ),
    )

    result = await subject.ingest_source("demo")

    assert result["status"] == "bounded"
    assert result["truncation_reason"] == "listing_elapsed_limit"
    assert result["discovered_count"] == 0
    assert result["listing_request_count"] == 1
    assert result["listing_bytes"] == 0
    assert adapter.calls == [None]


@pytest.mark.asyncio
async def test_collection_enforces_actual_byte_run_budget_before_next_download() -> None:
    files = (
        remote(1, content_length=40),
        remote(2, content_length=60),
        remote(3, content_length=1),
    )
    adapter = FakeAdapter({None: DiscoveryPage(files, None, True)})
    ingestion = FakeIngestion()
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        ingestion,
        policy=CollectionPolicy(
            maximum_files_per_source_run=3,
            maximum_reported_files=3,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=100,
            maximum_charged_bytes_per_source_day=1_000,
        ),
    )

    result = await subject.ingest_source("demo")

    assert result["status"] == "bounded"
    assert result["truncation_reason"] == "charged_byte_run_limit"
    assert result["charged_bytes"] == 100
    assert result["file_count"] == 2
    assert ingestion.files == list(files[:2])
    assert ingestion.maximum_charged_bytes == [99, 59]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("protocol", "expected_overhead"),
    [
        (SourceProtocol.HTTP, 97 * 1024 * 1024),
        (SourceProtocol.HTTPS, 97 * 1024 * 1024),
        (SourceProtocol.FTP, MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES),
        (SourceProtocol.FTPS, MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES),
    ],
)
async def test_collection_full_reservation_keeps_a_finite_cumulative_transfer_limit(
    protocol: SourceProtocol,
    expected_overhead: int,
) -> None:
    source_file = remote(1, content_length=1)
    if protocol is not SourceProtocol.HTTPS:
        source_file = replace(
            source_file,
            protocol=protocol,
            download_url=(f"{protocol.value}://example.test/{source_file.original_filename}"),
        )
    ingestion = FakeIngestion()
    policy = CollectionPolicy(
        maximum_files_per_source_run=1,
        maximum_reported_files=1,
    )
    subject = CollectionOperations(
        lambda _source_id: FakeAdapter({None: DiscoveryPage((source_file,), None, True)}),
        ("demo",),
        ingestion,
        policy=policy,
    )

    result = await subject.ingest_source("demo")

    assert result["status"] == "completed"
    assert ingestion.maximum_charged_bytes == [
        policy.maximum_archive_object_bytes + expected_overhead
    ]


def test_collection_policy_rejects_objects_above_the_supported_archive_ceiling() -> None:
    with pytest.raises(ValueError, match="archive object"):
        CollectionPolicy(
            maximum_archive_object_bytes=16 * 1024 * 1024 * 1024 + 1,
            maximum_charged_bytes_per_source_run=18 * 1024 * 1024 * 1024,
            maximum_charged_bytes_per_source_day=18 * 1024 * 1024 * 1024,
        )


@pytest.mark.asyncio
async def test_collection_settlement_rejects_bytes_above_the_durable_reservation() -> None:
    repository = collection_module._MemoryCollectionRepository()
    attempt = await repository.begin_attempt(
        CollectionScope(
            source_id="demo-retailer",
            portal_ids=("demo-portal",),
            operation="ordinary",
        )
    )
    source_file = remote(1, content_length=None)
    source_file_id = UUID(int=1)
    await repository.reserve_transfer(
        attempt.attempt_id,
        source_file_id,
        source_file,
        100,
    )

    with pytest.raises(RuntimeError, match="exceeds its reservation"):
        await repository.settle_transfer(
            attempt.attempt_id,
            source_file_id,
            source_file,
            101,
        )

    budget = await repository.charge_budget(attempt.attempt_id)
    assert budget.run_charged_bytes == 100
    assert budget.day_charged_bytes == 100


@pytest.mark.asyncio
async def test_collection_settlement_rejects_archive_charge_above_reservation() -> None:
    repository = collection_module._MemoryCollectionRepository()
    attempt = await repository.begin_attempt(
        CollectionScope(
            source_id="demo-retailer",
            portal_ids=("demo-portal",),
            operation="ordinary",
        )
    )
    source_file = remote(1, content_length=100)
    source_file_id = UUID(int=1)
    await repository.reserve_transfer(
        attempt.attempt_id,
        source_file_id,
        source_file,
        50,
    )
    await repository.note_terminal(source_file, archive_only=False)

    with pytest.raises(RuntimeError, match="exceeds its reservation"):
        await repository.settle_transfer(
            attempt.attempt_id,
            source_file_id,
            source_file,
            1,
        )

    budget = await repository.charge_budget(attempt.attempt_id)
    assert budget.run_charged_bytes == 50
    assert budget.day_charged_bytes == 50
    assert budget.day_successes == 0


@pytest.mark.asyncio
async def test_collection_resumes_file_that_did_not_fit_previous_run_budget() -> None:
    files = (remote(1, content_length=70), remote(2, content_length=50))
    adapter = FakeAdapter({None: DiscoveryPage(files, None, True)})
    ingestion = FakeIngestion(enforce_charge_budget=True)
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        ingestion,
        policy=CollectionPolicy(
            maximum_files_per_source_run=2,
            maximum_reported_files=2,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=100,
            maximum_charged_bytes_per_source_day=1_000,
        ),
    )

    first = await subject.ingest_source("demo")
    second = await subject.ingest_source("demo")

    assert first["status"] == "bounded"
    assert first["truncation_reason"] == "charged_byte_run_limit"
    assert first["charged_bytes"] == 70
    assert second["status"] == "completed"
    assert second["charged_bytes"] == 50
    assert ingestion.attempted_files == [files[0], files[1], files[1]]
    assert ingestion.maximum_charged_bytes == [99, 29, 99]


@pytest.mark.asyncio
async def test_failed_boundary_transfer_consumes_durable_day_budget() -> None:
    boundary = remote(1, content_length=None)
    adapter = FakeAdapter({None: DiscoveryPage((boundary,), None, True)})
    ingestion = TransferringBudgetFailureIngestion()
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        ingestion,
        policy=CollectionPolicy(
            maximum_files_per_source_run=1,
            maximum_reported_files=1,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=100,
            maximum_charged_bytes_per_source_day=100,
        ),
    )

    first = await subject.ingest_source("demo")
    second = await subject.ingest_source("demo")

    assert first["status"] == "bounded"
    assert first["charged_bytes"] == 100
    assert first["truncation_reason"] == "charged_byte_run_limit"
    assert second["status"] == "bounded"
    assert second["charged_bytes"] == 0
    assert second["truncation_reason"] == "charged_byte_day_limit"
    assert ingestion.attempted_files == [boundary]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("transferred_bytes", "case"),
    [(0, "declared"), (100, "chunked")],
)
async def test_permanent_object_oversize_advances_instead_of_starving_checkpoint(
    transferred_bytes: int,
    case: str,
) -> None:
    oversized = remote(1, content_length=None)
    adapter = FakeAdapter({None: DiscoveryPage((oversized,), None, True)})
    repository = collection_module._MemoryCollectionRepository()
    ingestion = PermanentOversizeIngestion(
        repository,
        transferred_bytes=transferred_bytes,
    )
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        ingestion,
        repository=repository,
        policy=CollectionPolicy(
            maximum_files_per_source_run=1,
            maximum_reported_files=1,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=100,
            maximum_charged_bytes_per_source_day=1_000,
        ),
    )

    with pytest.raises(DownloadLimitError, match="permanent object limit"):
        await subject.ingest_source("demo")
    resumed = await subject.ingest_source("demo")

    assert case in {"declared", "chunked"}
    assert ingestion.maximum_charged_bytes == [99]
    assert ingestion.attempted_files == [oversized]
    assert resumed["status"] == "completed"
    assert resumed["file_count"] == 0


@pytest.mark.asyncio
async def test_final_transport_frame_fits_reservation_and_hard_charge_ceiling() -> None:
    files = (remote(1, content_length=89), remote(2, content_length=None))
    ingestion = FinalFrameOvershootIngestion()
    subject = CollectionOperations(
        lambda _source_id: FakeAdapter({None: DiscoveryPage(files, None, True)}),
        ("demo",),
        ingestion,
        policy=CollectionPolicy(
            maximum_files_per_source_run=2,
            maximum_reported_files=2,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=100,
            maximum_charged_bytes_per_source_day=100,
        ),
    )

    result = await subject.ingest_source("demo")

    assert result["status"] == "bounded"
    assert result["truncation_reason"] == "charged_byte_run_limit"
    assert result["charged_bytes"] == 100
    assert ingestion.maximum_charged_bytes == [99, 10]


@pytest.mark.asyncio
async def test_collection_persists_source_day_budget_across_attempts() -> None:
    first_file = remote(1, content_length=60)
    second_file = remote(2, content_length=40)
    third_file = remote(3, content_length=1)
    adapter = FakeAdapter({None: DiscoveryPage((first_file,), None, True)})
    ingestion = FakeIngestion()
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        ingestion,
        policy=CollectionPolicy(
            maximum_files_per_source_run=3,
            maximum_reported_files=3,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=100,
            maximum_charged_bytes_per_source_day=100,
        ),
    )

    first = await subject.ingest_source("demo")
    adapter._pages[None] = DiscoveryPage((first_file, second_file), None, True)
    second = await subject.ingest_source("demo")
    adapter._pages[None] = DiscoveryPage((first_file, second_file, third_file), None, True)
    third = await subject.ingest_source("demo")

    assert first["status"] == "completed"
    assert second["status"] == "bounded"
    assert second["truncation_reason"] == "charged_byte_day_limit"
    assert second["charged_bytes"] == 40
    assert third["status"] == "bounded"
    assert third["truncation_reason"] == "charged_byte_day_limit"
    assert third["charged_bytes"] == 0
    assert ingestion.files == [first_file, second_file]
    assert ingestion.maximum_charged_bytes == [99, 39]


@pytest.mark.asyncio
async def test_collection_archive_charge_is_idempotent_across_boundary_retry_and_cas() -> None:
    first_file = remote(1, content_length=64)
    cas_duplicate = remote(2, content_length=36)
    adapter = FakeAdapter({None: DiscoveryPage((first_file,), None, True)})
    repository = InterruptAfterChargeRepository()
    ingestion = FakeIngestion(duplicate_remote_ids=frozenset({cas_duplicate.remote_id}))
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        ingestion,
        repository=repository,
        policy=CollectionPolicy(
            maximum_files_per_source_run=2,
            maximum_reported_files=2,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=100,
            maximum_charged_bytes_per_source_day=100,
        ),
    )

    with pytest.raises(SourceResponseError, match="boundary commit interruption"):
        await subject.ingest_source("demo")
    retry = await subject.ingest_source("demo")
    adapter._pages[None] = DiscoveryPage((first_file, cas_duplicate), None, True)
    final = await subject.ingest_source("demo")

    assert retry["status"] == "completed"
    assert retry["charged_bytes"] == 0
    assert repository.charge_totals[:2] == [64, 64]
    assert final["status"] == "bounded"
    assert final["truncation_reason"] == "charged_byte_day_limit"
    assert final["charged_bytes"] == 36
    assert repository.charge_totals == [64, 64, 100]
    assert ingestion.maximum_charged_bytes == [99, None, 35]


@pytest.mark.asyncio
async def test_collection_charges_terminal_rejected_archive_before_advancing() -> None:
    rejected = remote(1, content_length=75)
    adapter = FakeAdapter({None: DiscoveryPage((rejected,), None, True)})
    repository = collection_module._MemoryCollectionRepository()
    ingestion = TerminalRejectingIngestion(repository)
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        ingestion,
        repository=repository,
        policy=CollectionPolicy(
            maximum_files_per_source_run=1,
            maximum_reported_files=1,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=100,
            maximum_charged_bytes_per_source_day=100,
        ),
    )

    with pytest.raises(QuarantinedFileError):
        await subject.ingest_source("demo")
    resumed = await subject.ingest_source("demo")

    assert resumed["status"] == "completed"
    assert resumed["file_count"] == 0
    attempt = await repository.begin_attempt(
        CollectionScope(
            source_id="demo",
            portal_ids=("demo",),
            operation="ordinary",
        )
    )
    budget = await repository.charge_budget(attempt.attempt_id)
    assert budget.day_charged_bytes == 75
    assert ingestion.attempted_files == [rejected]


@pytest.mark.asyncio
async def test_collection_charges_successful_retry_transfer_overhead() -> None:
    source_file = remote(1, content_length=40)
    adapter = FakeAdapter({None: DiscoveryPage((source_file,), None, True)})
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        SuccessfulRetryOverheadIngestion(),
        policy=CollectionPolicy(
            maximum_files_per_source_run=1,
            maximum_reported_files=1,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=100,
            maximum_charged_bytes_per_source_day=100,
        ),
    )

    result = await subject.ingest_source("demo")

    assert result["status"] == "completed"
    assert result["charged_bytes"] == 65


@pytest.mark.asyncio
async def test_collection_bounds_tiny_file_identity_cardinality_independently_of_bytes() -> None:
    files = tuple(remote(index, content_length=1) for index in range(3))
    adapter = FakeAdapter({None: DiscoveryPage(files, None, True)})
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        FakeIngestion(),
        policy=CollectionPolicy(
            maximum_files_per_source_run=3,
            maximum_reported_files=3,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=1_000,
            maximum_charged_bytes_per_source_day=1_000,
            maximum_source_identities_per_source_day=2,
            maximum_transfer_attempts_per_source_day=3,
            maximum_successes_per_source_day=3,
        ),
    )

    result = await subject.ingest_source("demo")

    assert result["status"] == "bounded"
    assert result["file_count"] == 2
    assert result["truncation_reason"] == "identity_day_limit"


@pytest.mark.asyncio
async def test_collection_keeps_reservation_on_ambiguous_post_commit_failure() -> None:
    source_file = remote(1, content_length=40)
    adapter = FakeAdapter({None: DiscoveryPage((source_file,), None, True)})
    repository = collection_module._MemoryCollectionRepository()
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        AmbiguousPostCommitFailureIngestion(),
        repository=repository,
        policy=CollectionPolicy(
            maximum_files_per_source_run=1,
            maximum_reported_files=1,
            maximum_archive_object_bytes=99,
            maximum_transfer_chunk_bytes=1,
            maximum_transfer_protocol_overhead_bytes=0,
            maximum_http_transfer_protocol_overhead_bytes=0,
            maximum_charged_bytes_per_source_run=100,
            maximum_charged_bytes_per_source_day=100,
        ),
    )

    with pytest.raises(OSError, match="ambiguous post-commit"):
        await subject.ingest_source("demo")

    attempt = await repository.begin_attempt(
        CollectionScope(
            source_id="demo",
            portal_ids=("demo",),
            operation="ordinary",
        )
    )
    budget = await repository.charge_budget(attempt.attempt_id)
    assert budget.day_charged_bytes == 100


@pytest.mark.asyncio
async def test_collection_rejects_a_repeated_pagination_cursor() -> None:
    adapter = FakeAdapter(
        {
            None: DiscoveryPage((remote(0),), DiscoveryCursor("same"), False),
            "same": DiscoveryPage((remote(1),), DiscoveryCursor("same"), False),
        }
    )
    subject = CollectionOperations(lambda _source_id: adapter, ("demo",), FakeIngestion())

    with pytest.raises(SourceResponseError, match="repeated"):
        await subject.ingest_source("demo")


@pytest.mark.asyncio
async def test_completed_ingestion_and_replay_automatically_bootstrap_catalog() -> None:
    adapter = FakeAdapter({None: DiscoveryPage((remote(0),), None, True)})
    bootstrap = FakeCatalogBootstrap()
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        FakeIngestion(),
        catalog_bootstrap=bootstrap,
    )

    await subject.ingest_source("demo")
    await subject.replay(UUID(int=9))

    assert bootstrap.source_file_ids == [UUID(int=1), UUID(int=9)]


@pytest.mark.asyncio
async def test_archive_only_backfill_does_not_bootstrap_normalized_catalog() -> None:
    timestamp = datetime(2026, 8, 11, 14, tzinfo=UTC)
    adapter = FakeAdapter(
        {None: DiscoveryPage((remote(0, source_timestamp=timestamp),), None, True)}
    )
    bootstrap = FakeCatalogBootstrap()
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        FakeIngestion(),
        catalog_bootstrap=bootstrap,
    )

    await subject.backfill(
        "demo",
        since=timestamp,
        until=timestamp,
        archive_only=True,
    )

    assert bootstrap.source_file_ids == []


def test_collection_requires_metrics_and_clock_as_one_boundary() -> None:
    adapter = FakeAdapter({None: DiscoveryPage((), None, True)})
    with pytest.raises(ValueError, match="configured together"):
        CollectionOperations(
            lambda _source_id: adapter,
            ("demo",),
            FakeIngestion(),
            metrics=FakeMetrics(),
        )


@pytest.mark.asyncio
async def test_collection_logs_bounded_lifecycle_and_propagates_safe_file_context() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    events = get_lifecycle_logger("collection-test")
    adapter = FakeAdapter({None: DiscoveryPage((remote(1),), None, True)})
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        LifecycleFakeIngestion(events),
        events=events,
    )

    await subject.ingest_source("demo")

    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [event["event"] for event in logged] == [
        "discovery.started",
        "discovery.page_completed",
        "ingestion.completed",
        "discovery.completed",
    ]
    assert len({event["correlation_id"] for event in logged}) == 1
    assert all(event["run_id"] == logged[0]["run_id"] for event in logged)
    file_event = logged[2]
    assert file_event["source_id"] == "demo"
    assert file_event["retailer_id"] == "demo-retailer"
    assert file_event["portal_id"] == "demo-portal"
    assert file_event["source_file_id"] == str(UUID(int=1))
    assert "example.test" not in stream.getvalue()
    assert "PriceFull" not in stream.getvalue()


@pytest.mark.asyncio
async def test_collection_logs_classified_discovery_failure_without_error_detail() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    events = get_lifecycle_logger("collection-failure-test")
    adapter = FakeAdapter(
        {
            None: DiscoveryPage((remote(0),), DiscoveryCursor("same"), False),
            "same": DiscoveryPage((remote(1),), DiscoveryCursor("same"), False),
        }
    )
    subject = CollectionOperations(
        lambda _source_id: adapter,
        ("demo",),
        FakeIngestion(),
        events=events,
    )

    with pytest.raises(SourceResponseError):
        await subject.ingest_source("demo")

    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert logged[-1]["event"] == "discovery.failed"
    assert logged[-1]["error_code"] == SourceResponseError.code
    assert "repeated" not in stream.getvalue()
