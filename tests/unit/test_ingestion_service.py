from __future__ import annotations

import gzip
import io
import json
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from makolet.adapters.archive.local import LocalContentAddressedArchive
from makolet.adapters.observability.logging import configure_logging, get_lifecycle_logger
from makolet.adapters.parsers.xml import RetailXmlParser
from makolet.application.ingestion import IngestionPolicy, IngestionService
from makolet.application.models import StageSummary
from makolet.application.observability import LifecycleLogger
from makolet.application.ports import DocumentParser, DownloadSession
from makolet.domain.enums import (
    CompressionFormat,
    DiscountKind,
    DocumentType,
    IngestionStatus,
    IssueSeverity,
    SourceProtocol,
)
from makolet.domain.errors import (
    ArchiveCapacityError,
    ChargeBudgetExceededError,
    DownloadLimitError,
    MalformedDocumentError,
    QuarantinedFileError,
    RepositoryError,
    SourceAccessError,
)
from makolet.domain.models import (
    DocumentMetadata,
    ParsedEvent,
    PromotionItem,
    PromotionRecord,
    RemoteFile,
    ValidationIssue,
)
from tests.fakes.ingestion import (
    FakeDownloader,
    FakeDownloadSession,
    FakeIngestionRepository,
    FakeLeaseManager,
    FakeMetrics,
    FixedClock,
)

FIXTURE = Path(__file__).parents[1] / "fixtures" / "standard" / "price-full.xml"


class CountingParser:
    def __init__(self, *, parser_version: str = RetailXmlParser.parser_version) -> None:
        self.parser_version = parser_version
        self.calls = 0
        self._delegate = RetailXmlParser()

    async def parse(
        self,
        chunks: AsyncIterator[bytes],
        *,
        source_file_id: UUID,
        document_type: DocumentType,
        compression: CompressionFormat,
        filename: str,
    ) -> AsyncIterator[ParsedEvent]:
        self.calls += 1
        async for event in self._delegate.parse(
            chunks,
            source_file_id=source_file_id,
            document_type=document_type,
            compression=compression,
            filename=filename,
        ):
            yield event


class IssueFloodParser:
    parser_version = RetailXmlParser.parser_version

    def __init__(self, issue_count: int) -> None:
        self._issue_count = issue_count
        self._delegate = RetailXmlParser()

    async def parse(
        self,
        chunks: AsyncIterator[bytes],
        *,
        source_file_id: UUID,
        document_type: DocumentType,
        compression: CompressionFormat,
        filename: str,
    ) -> AsyncIterator[ParsedEvent]:
        async for event in self._delegate.parse(
            chunks,
            source_file_id=source_file_id,
            document_type=document_type,
            compression=compression,
            filename=filename,
        ):
            yield event
        for record_index in range(self._issue_count):
            yield ValidationIssue(
                source_file_id=source_file_id,
                severity=IssueSeverity.WARNING,
                code="publisher_warning",
                message="bounded publisher warning",
                record_index=record_index,
            )


class PromotionFanoutParser:
    parser_version = RetailXmlParser.parser_version

    def __init__(
        self,
        *,
        description: str | None = None,
        item_count: int = 1,
        promotion_count: int = 3,
        source_store_id: str | None = None,
        store_ids: tuple[str, ...] | None = None,
    ) -> None:
        self._description = description
        self._item_count = item_count
        self._promotion_count = promotion_count
        self._source_store_id = source_store_id
        self._store_ids = store_ids

    async def parse(
        self,
        chunks: AsyncIterator[bytes],
        *,
        source_file_id: UUID,
        document_type: DocumentType,
        compression: CompressionFormat,
        filename: str,
    ) -> AsyncIterator[ParsedEvent]:
        del compression, filename
        async for _ in chunks:
            pass
        for record_index in range(1, self._promotion_count + 1):
            yield PromotionRecord(
                source_file_id=source_file_id,
                record_index=record_index,
                chain_id="1",
                subchain_id="1",
                source_store_id=self._source_store_id,
                promotion_id=f"promotion-{record_index}",
                description=self._description,
                discount_kind=DiscountKind.UNKNOWN,
                starts_at=None,
                ends_at=None,
                items=tuple(
                    PromotionItem(item_code=f"item-{record_index}-{item_index}")
                    for item_index in range(self._item_count)
                ),
                store_ids=(
                    self._store_ids if self._store_ids is not None else (f"store-{record_index}",)
                ),
                club_ids=(f"club-{record_index}",),
            )
        yield DocumentMetadata(source_file_id=source_file_id, document_type=document_type)


class SpoolFirstDownloadSession(FakeDownloadSession):
    """Model FTP, whose complete network body exists before archive consumption."""

    @property
    def transferred_bytes(self) -> int:
        return len(self._payload)


class SpoolFirstDownloader(FakeDownloader):
    @asynccontextmanager
    async def _open(self, remote_file: RemoteFile) -> AsyncIterator[DownloadSession]:
        self.open_count += 1
        yield SpoolFirstDownloadSession(
            self.payloads[remote_file.download_url],
            self.clock,
        )


class ControlBudgetDownloadSession(FakeDownloadSession):
    """Model a successful FTP transfer with separately metered control bytes."""

    CONTROL_BYTES = 17

    @property
    def transferred_bytes(self) -> int:
        return len(self._payload) + self.CONTROL_BYTES


class ControlBudgetDownloader(FakeDownloader):
    @asynccontextmanager
    async def _open(self, remote_file: RemoteFile) -> AsyncIterator[DownloadSession]:
        self.open_count += 1
        yield ControlBudgetDownloadSession(
            self.payloads[remote_file.download_url],
            self.clock,
        )


class EarlyRejectingArchive(LocalContentAddressedArchive):
    async def put(
        self,
        chunks: AsyncIterator[bytes],
        *,
        original_filename: str,
    ) -> tuple[str, int, bool]:
        del original_filename
        first_chunk = await anext(chunks)
        raise ArchiveCapacityError(
            "simulated archive refusal",
            transferred_bytes=len(first_chunk),
        )


def remote(remote_id: str, url: str, *, compression: CompressionFormat) -> RemoteFile:
    return RemoteFile(
        retailer_id="demo",
        portal_id="fixture",
        protocol=SourceProtocol.FIXTURE,
        remote_id=remote_id,
        download_url=url,
        original_filename="PriceFull-demo.xml.gz"
        if compression is CompressionFormat.GZIP
        else "bad.xml",
        document_type=DocumentType.PRICE_FULL,
        compression=compression,
        discovered_at=datetime(2026, 8, 11, tzinfo=UTC),
    )


def build_service(
    tmp_path: Path,
    payloads: dict[str, bytes],
    *,
    events: LifecycleLogger | None = None,
    parser: DocumentParser | None = None,
    stage_batch_size: int = 1,
    maximum_stage_batch_bytes: int = 64 * 1024 * 1024,
    maximum_validation_issues: int = 100_000,
    maximum_validation_issue_bytes: int = 64 * 1024 * 1024,
) -> tuple[IngestionService, FakeIngestionRepository, FakeDownloader, LocalContentAddressedArchive]:
    clock = FixedClock()
    repository = FakeIngestionRepository(clock)
    downloader = FakeDownloader(payloads, clock)
    archive = LocalContentAddressedArchive(tmp_path / "raw")
    service = IngestionService(
        repository,
        downloader,
        archive,
        parser or RetailXmlParser(),
        FakeLeaseManager(),
        clock,
        FakeMetrics(),
        worker_id="test-worker",
        policy=IngestionPolicy(
            stage_batch_size=stage_batch_size,
            maximum_stage_batch_bytes=maximum_stage_batch_bytes,
            download_attempts=2,
            minimum_full_records=1,
            maximum_record_rejection_fraction=0.50,
            maximum_validation_issues=maximum_validation_issues,
            maximum_validation_issue_bytes=maximum_validation_issue_bytes,
        ),
        events=events,
    )
    return service, repository, downloader, archive


@pytest.mark.asyncio
async def test_validation_issue_flood_stops_at_the_cumulative_attempt_ceiling(
    tmp_path: Path,
) -> None:
    payload = FIXTURE.read_bytes()
    source = remote(
        "fixture:issue-flood", "fixture:///issue-flood", compression=CompressionFormat.NONE
    )
    source = replace(source, original_filename="PriceFull.xml")
    service, repository, _, _ = build_service(
        tmp_path,
        {source.download_url: payload},
        parser=IssueFloodParser(4),
        maximum_validation_issues=3,
    )

    with pytest.raises(QuarantinedFileError, match="Validation issue evidence"):
        await service.ingest(source)

    staged_issues = [
        event
        for events in repository.staged.values()
        for event in events
        if isinstance(event, ValidationIssue)
    ]
    assert len(staged_issues) == 3


@pytest.mark.asyncio
async def test_validation_issue_ceiling_preserves_a_legitimate_bounded_file(
    tmp_path: Path,
) -> None:
    payload = FIXTURE.read_bytes()
    source = remote(
        "fixture:bounded-issues", "fixture:///bounded-issues", compression=CompressionFormat.NONE
    )
    source = replace(source, original_filename="PriceFull.xml")
    service, _, _, _ = build_service(
        tmp_path,
        {source.download_url: payload},
        parser=IssueFloodParser(2),
        maximum_validation_issues=3,
    )

    result = await service.ingest(source)

    assert result.status is IngestionStatus.COMPLETED
    assert result.stage is not None
    assert result.stage.warnings == 2
    assert result.stage.warnings + result.stage.rejected_records == 3


@pytest.mark.asyncio
async def test_promotion_relationships_count_toward_stage_batch_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = remote("fixture:fanout", "fixture:///fanout", compression=CompressionFormat.NONE)
    source = replace(
        source,
        original_filename="Promo.xml",
        document_type=DocumentType.PROMOTION_DELTA,
    )
    service, repository, _, _ = build_service(
        tmp_path,
        {source.download_url: b"ignored"},
        parser=PromotionFanoutParser(
            source_store_id="shared-store",
            store_ids=("shared-store",),
        ),
        stage_batch_size=5,
    )
    original_stage = repository.stage
    staged_units: list[int] = []

    async def record_stage(
        source_file_id: UUID,
        events: Iterable[ParsedEvent],
    ) -> StageSummary:
        batch = tuple(events)
        staged_units.append(
            sum(
                1
                + len(event.items)
                + len(
                    dict.fromkeys(
                        (
                            *event.store_ids,
                            *((event.source_store_id,) if event.source_store_id else ()),
                        )
                    )
                )
                + len(event.club_ids)
                if isinstance(event, PromotionRecord)
                else 1
                for event in batch
            )
        )
        return await original_stage(source_file_id, batch)

    monkeypatch.setattr(repository, "stage", record_stage)

    result = await service.ingest(source)

    assert result.status is IngestionStatus.COMPLETED
    assert staged_units == [4, 4, 5]


@pytest.mark.asyncio
async def test_promotion_source_scope_store_counts_toward_stage_batch_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = replace(
        remote("fixture:scope-store", "fixture:///scope-store", compression=CompressionFormat.NONE),
        original_filename="Promo.xml",
        document_type=DocumentType.PROMOTION_DELTA,
    )
    service, repository, _, _ = build_service(
        tmp_path,
        {source.download_url: b"ignored"},
        parser=PromotionFanoutParser(source_store_id="scope-store"),
        stage_batch_size=5,
    )
    original_stage = repository.stage
    staged_units: list[int] = []

    async def record_stage(
        source_file_id: UUID,
        events: Iterable[ParsedEvent],
    ) -> StageSummary:
        batch = tuple(events)
        staged_units.append(
            sum(
                1
                + len(event.items)
                + len(
                    dict.fromkeys(
                        (
                            *event.store_ids,
                            *((event.source_store_id,) if event.source_store_id else ()),
                        )
                    )
                )
                + len(event.club_ids)
                if isinstance(event, PromotionRecord)
                else 1
                for event in batch
            )
        )
        return await original_stage(source_file_id, batch)

    monkeypatch.setattr(repository, "stage", record_stage)

    result = await service.ingest(source)

    assert result.status is IngestionStatus.COMPLETED
    assert staged_units == [5, 5, 5, 1]


@pytest.mark.asyncio
async def test_retained_event_size_counts_toward_stage_batch_memory_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = replace(
        remote(
            "fixture:large-events", "fixture:///large-events", compression=CompressionFormat.NONE
        ),
        original_filename="Promo.xml",
        document_type=DocumentType.PROMOTION_DELTA,
    )
    service, repository, _, _ = build_service(
        tmp_path,
        {source.download_url: b"ignored"},
        parser=PromotionFanoutParser(description="x" * 4_096),
        stage_batch_size=5_000,
        maximum_stage_batch_bytes=24 * 1024,
    )
    original_stage = repository.stage
    staged_event_counts: list[int] = []

    async def record_stage(
        source_file_id: UUID,
        events: Iterable[ParsedEvent],
    ) -> StageSummary:
        batch = tuple(events)
        staged_event_counts.append(len(batch))
        return await original_stage(source_file_id, batch)

    monkeypatch.setattr(repository, "stage", record_stage)

    result = await service.ingest(source)

    assert result.status is IngestionStatus.COMPLETED
    assert staged_event_counts == [1, 1, 1, 1]


@pytest.mark.asyncio
async def test_single_promotion_exceeding_stage_row_limit_is_quarantined(
    tmp_path: Path,
) -> None:
    source = replace(
        remote(
            "fixture:wide-promotion",
            "fixture:///wide-promotion",
            compression=CompressionFormat.NONE,
        ),
        original_filename="Promo.xml",
        document_type=DocumentType.PROMOTION_DELTA,
    )
    service, repository, _, _ = build_service(
        tmp_path,
        {source.download_url: b"ignored"},
        parser=PromotionFanoutParser(item_count=5, promotion_count=1, store_ids=()),
        stage_batch_size=6,
    )

    with pytest.raises(QuarantinedFileError, match="staging row limit"):
        await service.ingest(source)

    stored = next(iter(repository.files.values()))
    assert stored.status is IngestionStatus.QUARANTINED
    assert repository.staged[stored.source_file_id] == []


@pytest.mark.asyncio
async def test_single_promotion_exceeding_stage_memory_limit_is_quarantined(
    tmp_path: Path,
) -> None:
    source = replace(
        remote(
            "fixture:large-promotion",
            "fixture:///large-promotion",
            compression=CompressionFormat.NONE,
        ),
        original_filename="Promo.xml",
        document_type=DocumentType.PROMOTION_DELTA,
    )
    service, repository, _, _ = build_service(
        tmp_path,
        {source.download_url: b"ignored"},
        parser=PromotionFanoutParser(
            description="x" * 4_096,
            promotion_count=1,
        ),
        stage_batch_size=5_000,
        maximum_stage_batch_bytes=8 * 1024,
    )

    with pytest.raises(QuarantinedFileError, match="staging memory limit"):
        await service.ingest(source)

    stored = next(iter(repository.files.values()))
    assert stored.status is IngestionStatus.QUARANTINED
    assert repository.staged[stored.source_file_id] == []


@pytest.mark.asyncio
async def test_ingestion_archives_parses_applies_and_replays_idempotently(tmp_path: Path) -> None:
    exact_bytes = gzip.compress(FIXTURE.read_bytes(), mtime=0)
    source = remote("fixture:one", "fixture:///one", compression=CompressionFormat.GZIP)
    service, repository, downloader, archive = build_service(
        tmp_path, {source.download_url: exact_bytes}
    )

    first = await service.ingest(source)
    duplicate = await service.ingest(source)
    replay = await service.replay(first.source_file_id)

    assert first.status is IngestionStatus.COMPLETED
    assert first.stage is not None
    assert first.stage.price_records == 1
    assert first.stage.rejected_records == 1
    assert first.apply is not None
    assert first.apply.history_events == 1
    assert duplicate.duplicate
    assert downloader.open_count == 1
    assert replay.replayed
    assert replay.apply is not None
    assert replay.apply.unchanged == 1
    stored = await repository.get(first.source_file_id)
    assert stored.archive_object_key is not None
    assert await archive.verify(stored.archive_object_key, first.content_sha256 or "") == len(
        exact_bytes
    )
    assert all(value is True for value in repository.replays.values())


@pytest.mark.asyncio
async def test_equal_content_under_new_identity_reuses_staging_and_applies_provenance(
    tmp_path: Path,
) -> None:
    exact_bytes = gzip.compress(FIXTURE.read_bytes(), mtime=0)
    first_source = remote("fixture:first", "fixture:///first", compression=CompressionFormat.GZIP)
    second_source = remote(
        "fixture:second", "fixture:///second", compression=CompressionFormat.GZIP
    )
    parser = CountingParser()
    service, repository, _, _ = build_service(
        tmp_path,
        {first_source.download_url: exact_bytes, second_source.download_url: exact_bytes},
        parser=parser,
    )

    first = await service.ingest(first_source)
    second = await service.ingest(second_source)

    assert not first.duplicate
    assert second.duplicate
    assert second.content_sha256 == first.content_sha256
    assert second.stage == first.stage
    assert second.apply is not None
    assert second.apply.unchanged == 1
    assert parser.calls == 1
    assert (await repository.get(second.source_file_id)).status is IngestionStatus.COMPLETED


@pytest.mark.asyncio
async def test_repeated_a_b_a_content_reuses_a_parse_but_restores_price_history(
    tmp_path: Path,
) -> None:
    payload_a = FIXTURE.read_bytes()
    payload_b = payload_a.replace(b"<ItemPrice>19.90</ItemPrice>", b"<ItemPrice>21.90</ItemPrice>")
    sources = [
        remote(f"fixture:{name}", f"fixture:///{name}", compression=CompressionFormat.GZIP)
        for name in ("a-first", "b", "a-return")
    ]
    parser = CountingParser()
    service, _, _, _ = build_service(
        tmp_path,
        {
            sources[0].download_url: gzip.compress(payload_a, mtime=0),
            sources[1].download_url: gzip.compress(payload_b, mtime=0),
            sources[2].download_url: gzip.compress(payload_a, mtime=0),
        },
        parser=parser,
    )

    first, changed, restored = [await service.ingest(source) for source in sources]

    assert first.apply is not None
    assert first.apply.history_events == 1
    assert changed.apply is not None
    assert changed.apply.history_events == 1
    assert restored.apply is not None
    assert restored.apply.history_events == 1
    assert restored.apply.updated == 1
    assert restored.duplicate
    assert parser.calls == 2


@pytest.mark.asyncio
async def test_staging_reuse_falls_back_to_parse_for_version_or_integrity_mismatch(
    tmp_path: Path,
) -> None:
    exact_bytes = gzip.compress(FIXTURE.read_bytes(), mtime=0)
    sources = [
        remote(f"fixture:{index}", f"fixture:///{index}", compression=CompressionFormat.GZIP)
        for index in range(3)
    ]
    parser = CountingParser(parser_version="test-parser/1")
    service, repository, _, _ = build_service(
        tmp_path,
        {source.download_url: exact_bytes for source in sources},
        parser=parser,
    )

    first = await service.ingest(sources[0])
    parser.parser_version = "test-parser/2"
    await service.ingest(sources[1])
    repository.staged[first.source_file_id] = []
    parser.parser_version = "test-parser/1"
    await service.ingest(sources[2])

    assert parser.calls == 3


@pytest.mark.asyncio
async def test_malformed_download_is_preserved_and_quarantined(tmp_path: Path) -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    source = remote("fixture:html", "fixture:///html", compression=CompressionFormat.NONE)
    service, repository, _, archive = build_service(
        tmp_path,
        {source.download_url: b"<html>upstream failure</html>"},
        events=get_lifecycle_logger("ingestion-quarantine-test"),
    )

    with pytest.raises(MalformedDocumentError, match="HTML"):
        await service.ingest(source)

    stored = next(iter(repository.files.values()))
    assert stored.status is IngestionStatus.QUARANTINED
    assert stored.archive_object_key is not None
    assert await archive.exists(stored.archive_object_key)
    events = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert "parse.failed" in [event["event"] for event in events]
    assert "stage.failed" not in [event["event"] for event in events]
    assert events[-1]["event"] == "ingestion.quarantined"
    assert events[-1]["error_code"] == MalformedDocumentError.code
    assert events[-1]["status"] == IngestionStatus.QUARANTINED.value
    assert "upstream failure" not in stream.getvalue()

    stream.seek(0)
    stream.truncate(0)
    with pytest.raises(MalformedDocumentError, match="HTML"):
        await service.replay(stored.source_file_id)
    assert tuple(repository.replay_errors.values()) == (
        (
            "malformed_document",
            "Archived source document was quarantined by validation",
        ),
    )
    replay_events = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert replay_events[-1]["event"] == "replay.failed"


@pytest.mark.asyncio
async def test_apply_failure_logs_phase_and_ingestion_outcome_without_detail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    exact_bytes = gzip.compress(FIXTURE.read_bytes(), mtime=0)
    source = remote(
        "fixture:apply-failure",
        "fixture:///apply-failure",
        compression=CompressionFormat.GZIP,
    )
    service, repository, _, _ = build_service(
        tmp_path,
        {source.download_url: exact_bytes},
        events=get_lifecycle_logger("ingestion-apply-failure-test"),
    )

    async def fail_apply(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RepositoryError(  # secret-scan: allow
            "password=private-database-detail"  # secret-scan: allow
        )

    monkeypatch.setattr(repository, "apply", fail_apply)
    with pytest.raises(RepositoryError):
        await service.ingest(source)

    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    names = [event["event"] for event in logged]
    assert "apply.started" in names
    assert "apply.failed" in names
    assert "apply.completed" not in names
    assert names[-1] == "ingestion.failed"
    assert logged[-2]["error_code"] == RepositoryError.code
    assert "private-database-detail" not in stream.getvalue()


@pytest.mark.asyncio
async def test_stage_failure_is_classified_once_without_parser_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    exact_bytes = gzip.compress(FIXTURE.read_bytes(), mtime=0)
    source = remote(
        "fixture:stage-failure",
        "fixture:///stage-failure",
        compression=CompressionFormat.GZIP,
    )
    service, repository, _, _ = build_service(
        tmp_path,
        {source.download_url: exact_bytes},
        events=get_lifecycle_logger("ingestion-stage-failure-test"),
    )

    async def fail_stage(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RepositoryError("raw staged value must stay private")

    monkeypatch.setattr(repository, "stage", fail_stage)
    with pytest.raises(RepositoryError):
        await service.ingest(source)

    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    names = [event["event"] for event in logged]
    assert names.count("stage.failed") == 1
    assert "parse.failed" not in names
    assert names[-1] == "ingestion.failed"
    assert "raw staged value" not in stream.getvalue()


@pytest.mark.asyncio
async def test_file_with_only_rejected_records_is_quarantined(tmp_path: Path) -> None:
    payload = (
        b"<Root><ChainID>1</ChainID><SubChainID>1</SubChainID><StoreID>1</StoreID>"
        b"<Items><Item><ItemCode>123</ItemCode></Item></Items></Root>"
    )
    source = remote("fixture:rejected", "fixture:///rejected", compression=CompressionFormat.NONE)
    service, repository, _, archive = build_service(
        tmp_path,
        {source.download_url: payload},
    )

    with pytest.raises(QuarantinedFileError, match="rejection fraction"):
        await service.ingest(source)

    stored = next(iter(repository.files.values()))
    assert stored.status is IngestionStatus.QUARANTINED
    assert stored.archive_object_key is not None
    assert await archive.exists(stored.archive_object_key)


@pytest.mark.asyncio
async def test_transient_download_failure_is_retried_before_apply(tmp_path: Path) -> None:
    exact_bytes = gzip.compress(FIXTURE.read_bytes(), mtime=0)
    source = remote("fixture:retry", "fixture:///retry", compression=CompressionFormat.GZIP)
    service, _, downloader, _ = build_service(tmp_path, {source.download_url: exact_bytes})
    downloader.failures_remaining = 1

    result = await service.ingest(source)

    assert result.status is IngestionStatus.COMPLETED
    assert downloader.open_count == 2


@pytest.mark.asyncio
async def test_download_retries_report_cumulative_transferred_bytes(tmp_path: Path) -> None:
    source = remote(
        "fixture:retry-bytes",
        "fixture:///retry-bytes",
        compression=CompressionFormat.GZIP,
    )
    service, _, downloader, _ = build_service(tmp_path, {source.download_url: b"unused"})
    downloader.failures_remaining = 2
    downloader.failure_error = SourceAccessError(
        "simulated reset after bytes",
        transferred_bytes=41,
    )

    with pytest.raises(SourceAccessError) as caught:
        await service.ingest(source)

    assert caught.value.transferred_bytes == 82
    assert downloader.open_count == 2


@pytest.mark.asyncio
async def test_successful_retry_reports_and_bounds_all_transferred_bytes(tmp_path: Path) -> None:
    exact_bytes = gzip.compress(FIXTURE.read_bytes(), mtime=0)
    source = remote(
        "fixture:retry-success-bytes",
        "fixture:///retry-success-bytes",
        compression=CompressionFormat.GZIP,
    )
    service, _, downloader, _ = build_service(
        tmp_path,
        {source.download_url: exact_bytes},
    )
    downloader.failures_remaining = 1
    downloader.failure_error = SourceAccessError(
        "simulated reset after bytes",
        transferred_bytes=41,
    )

    result = await service.ingest(
        source,
        maximum_charged_bytes=len(exact_bytes) + 41,
    )

    assert result.transferred_bytes == len(exact_bytes) + 41
    assert downloader.maximum_bytes == [len(exact_bytes) + 41, len(exact_bytes)]


@pytest.mark.asyncio
async def test_successful_download_charges_session_control_bytes(tmp_path: Path) -> None:
    exact_bytes = gzip.compress(FIXTURE.read_bytes(), mtime=0)
    source = remote(
        "fixture:successful-control-bytes",
        "fixture:///successful-control-bytes",
        compression=CompressionFormat.GZIP,
    )
    clock = FixedClock()
    repository = FakeIngestionRepository(clock)
    downloader = ControlBudgetDownloader({source.download_url: exact_bytes}, clock)
    service = IngestionService(
        repository,
        downloader,
        LocalContentAddressedArchive(tmp_path / "raw"),
        RetailXmlParser(),
        FakeLeaseManager(),
        clock,
        FakeMetrics(),
        worker_id="test-worker",
        policy=IngestionPolicy(
            download_attempts=2,
            minimum_full_records=1,
            maximum_record_rejection_fraction=0.50,
        ),
    )

    result = await service.ingest(source)

    assert result.transferred_bytes == len(exact_bytes) + ControlBudgetDownloadSession.CONTROL_BYTES


@pytest.mark.asyncio
async def test_retry_stops_before_network_when_prior_bytes_exhaust_charge_limit(
    tmp_path: Path,
) -> None:
    source = remote(
        "fixture:retry-exhausted",
        "fixture:///retry-exhausted",
        compression=CompressionFormat.GZIP,
    )
    service, _, downloader, _ = build_service(tmp_path, {source.download_url: b"unused"})
    downloader.failures_remaining = 2
    downloader.failure_error = SourceAccessError(
        "simulated reset after bytes",
        transferred_bytes=41,
    )

    with pytest.raises(ChargeBudgetExceededError) as caught:
        await service.ingest(source, maximum_charged_bytes=41)

    assert caught.value.transferred_bytes == 41
    assert downloader.open_count == 1
    assert downloader.maximum_bytes == [41]


@pytest.mark.asyncio
async def test_requested_download_limit_becomes_retryable_collection_exhaustion(
    tmp_path: Path,
) -> None:
    source = remote(
        "fixture:requested-limit",
        "fixture:///requested-limit",
        compression=CompressionFormat.GZIP,
    )
    service, _, downloader, _ = build_service(tmp_path, {source.download_url: b"unused"})
    downloader.failures_remaining = 1
    downloader.failure_error = DownloadLimitError(
        "simulated requested limit",
        transferred_bytes=41,
        budget_limited=True,
    )

    with pytest.raises(ChargeBudgetExceededError) as caught:
        await service.ingest(source, maximum_charged_bytes=41)

    assert caught.value.transferred_bytes == 41
    assert downloader.open_count == 1


@pytest.mark.asyncio
async def test_permanent_download_limit_remains_terminal_with_a_finite_safety_ceiling(
    tmp_path: Path,
) -> None:
    source = remote(
        "fixture:permanent-limit",
        "fixture:///permanent-limit",
        compression=CompressionFormat.GZIP,
    )
    service, _, downloader, _ = build_service(tmp_path, {source.download_url: b"unused"})
    downloader.failures_remaining = 1
    downloader.failure_error = DownloadLimitError("simulated permanent object limit")

    with pytest.raises(DownloadLimitError, match="permanent object limit") as caught:
        await service.ingest(source, maximum_charged_bytes=100)

    assert caught.value.budget_limited is False
    assert downloader.open_count == 1


@pytest.mark.asyncio
async def test_spool_first_download_charges_full_body_when_archive_stops_early(
    tmp_path: Path,
) -> None:
    payload = b"fully-downloaded-ftp-body"
    source = remote(
        "fixture:spool-first",
        "fixture:///spool-first",
        compression=CompressionFormat.GZIP,
    )
    clock = FixedClock()
    repository = FakeIngestionRepository(clock)
    downloader = SpoolFirstDownloader({source.download_url: payload}, clock)
    archive = EarlyRejectingArchive(tmp_path / "raw")
    service = IngestionService(
        repository,
        downloader,
        archive,
        RetailXmlParser(),
        FakeLeaseManager(),
        clock,
        FakeMetrics(),
        worker_id="test-worker",
        policy=IngestionPolicy(download_attempts=2),
    )

    with pytest.raises(ArchiveCapacityError, match="simulated archive refusal") as caught:
        await service.ingest(source)

    assert caught.value.transferred_bytes == len(payload)
    assert downloader.open_count == 1


@pytest.mark.asyncio
async def test_ingestion_logs_complete_correlated_success_and_replay_lifecycle(
    tmp_path: Path,
) -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    events = get_lifecycle_logger("ingestion-success-test")
    exact_bytes = gzip.compress(FIXTURE.read_bytes(), mtime=0)
    source = remote("fixture:events", "fixture:///events", compression=CompressionFormat.GZIP)
    service, _, _, _ = build_service(
        tmp_path,
        {source.download_url: exact_bytes},
        events=events,
    )

    ingested = await service.ingest(source)

    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [event["event"] for event in logged] == [
        "download.started",
        "archive.stored",
        "download.completed",
        "parse.started",
        "stage.started",
        "parse.completed",
        "stage.completed",
        "apply.started",
        "apply.completed",
        "ingestion.completed",
    ]
    assert len({event["correlation_id"] for event in logged}) == 1
    assert all(event["source_file_id"] == str(ingested.source_file_id) for event in logged)
    assert all(event["retailer_id"] == "demo" for event in logged)
    assert all(event["portal_id"] == "fixture" for event in logged)
    assert "fixture:///events" not in stream.getvalue()
    assert "PriceFull-demo" not in stream.getvalue()

    stream.seek(0)
    stream.truncate(0)
    await service.replay(ingested.source_file_id)
    replayed = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [event["event"] for event in replayed] == [
        "replay.started",
        "archive.deduplicated",
        "parse.started",
        "stage.started",
        "parse.completed",
        "stage.completed",
        "apply.started",
        "apply.completed",
        "replay.completed",
    ]
    parse_event = next(event for event in replayed if event["event"] == "parse.started")
    assert "replay_id" in parse_event


@pytest.mark.asyncio
async def test_persisted_failure_is_classified_and_log_omits_exception_detail(
    tmp_path: Path,
) -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream)
    source = remote(
        "fixture:safe-error", "fixture:///safe-error", compression=CompressionFormat.GZIP
    )
    service, repository, downloader, _ = build_service(
        tmp_path,
        {source.download_url: b"unused"},
        events=get_lifecycle_logger("ingestion-failure-test"),
    )
    downloader.failures_remaining = 2
    downloader.failure_error = SourceAccessError(
        "postgresql://operator:super-secret@internal.invalid/private"  # secret-scan: allow
        "?token=query-secret"  # secret-scan: allow
    )
    with pytest.raises(SourceAccessError):
        await service.ingest(source)

    assert repository.transition_errors == [
        (
            "source_access_error",
            "Ingestion failed temporarily and may be retried",
        )
    ]
    persisted = repr(repository.transition_errors)
    logged = stream.getvalue()
    for secret in ("super-secret", "query-secret"):
        assert secret not in persisted
        assert secret not in logged
    events = [json.loads(line) for line in logged.splitlines()]
    assert [event["event"] for event in events] == [
        "download.started",
        "download.failed",
        "download.started",
        "download.failed",
        "ingestion.failed",
    ]
    assert events[-1]["error_code"] == SourceAccessError.code
    assert events[-1]["status"] == IngestionStatus.FAILED_RETRYABLE.value
    assert "internal.invalid" not in logged
