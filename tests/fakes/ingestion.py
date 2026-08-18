from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4, uuid5

from makolet.application.ingestion import stage_events
from makolet.application.models import (
    ApplySummary,
    ArchivedDownload,
    DownloadEvidence,
    RegisteredSourceFile,
    ReplayAttempt,
    StageSummary,
)
from makolet.application.ports import DownloadSession
from makolet.domain.enums import CompressionFormat, DocumentType, IngestionStatus, IssueSeverity
from makolet.domain.errors import QuarantinedFileError, RepositoryError, SourceAccessError
from makolet.domain.models import (
    ParsedEvent,
    PriceRecord,
    PromotionRecord,
    RemoteFile,
    StoreRecord,
    ValidationIssue,
)

_NAMESPACE = UUID("9ff9f8b7-6cef-46e8-82f5-11f02d7b586c")


class FixedClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 8, 11, 12, tzinfo=UTC)

    def now(self) -> datetime:
        result = self.current
        self.current += timedelta(milliseconds=1)
        return result


class FakeDownloadSession:
    def __init__(self, payload: bytes, clock: FixedClock) -> None:
        self._payload = payload
        self._clock = clock
        self._started = clock.now()
        self._consumed = 0

    @property
    def transferred_bytes(self) -> int:
        return self._consumed

    async def iter_raw(self) -> AsyncIterator[bytes]:
        midpoint = len(self._payload) // 2
        for chunk in (self._payload[:midpoint], self._payload[midpoint:]):
            if chunk:
                self._consumed += len(chunk)
                yield chunk

    async def finish(self, content_length: int) -> DownloadEvidence:
        if content_length != self._consumed:
            raise AssertionError("test archive did not consume exact download bytes")
        return DownloadEvidence(
            started_at=self._started,
            finished_at=self._clock.now(),
            status_code=200,
            content_length=content_length,
            media_type="application/octet-stream",
            etag=None,
            last_modified=None,
        )


class FakeDownloader:
    def __init__(self, payloads: dict[str, bytes], clock: FixedClock) -> None:
        self.payloads = payloads
        self.clock = clock
        self.open_count = 0
        self.maximum_bytes: list[int | None] = []
        self.failures_remaining = 0
        self.failure_error: Exception = SourceAccessError("simulated transient source failure")

    def open(
        self,
        remote_file: RemoteFile,
        *,
        maximum_bytes: int | None = None,
    ) -> AbstractAsyncContextManager[DownloadSession]:
        self.maximum_bytes.append(maximum_bytes)
        return self._open(remote_file)

    @asynccontextmanager
    async def _open(self, remote_file: RemoteFile) -> AsyncIterator[DownloadSession]:
        self.open_count += 1
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise self.failure_error
        yield FakeDownloadSession(self.payloads[remote_file.download_url], self.clock)


class FakeLeaseManager:
    def __init__(self) -> None:
        self.held: set[str] = set()

    def acquire(
        self, resource: str, owner: str, ttl: timedelta
    ) -> AbstractAsyncContextManager[bool]:
        del owner, ttl
        return self._acquire(resource)

    @asynccontextmanager
    async def _acquire(self, resource: str) -> AsyncIterator[bool]:
        if resource in self.held:
            yield False
            return
        self.held.add(resource)
        try:
            yield True
        finally:
            self.held.remove(resource)


class FakeMetrics:
    def __init__(self) -> None:
        self.counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}
        self.observations: list[tuple[str, float]] = []
        self.gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}

    def increment(self, name: str, *, labels: dict[str, str] | None = None, value: int = 1) -> None:
        key = (name, tuple(sorted((labels or {}).items())))
        self.counters[key] = self.counters.get(key, 0) + value

    def observe(self, name: str, value: float, *, labels: dict[str, str] | None = None) -> None:
        del labels
        self.observations.append((name, value))

    def set_gauge(self, name: str, value: float, *, labels: dict[str, str] | None = None) -> None:
        key = (name, tuple(sorted((labels or {}).items())))
        self.gauges[key] = value


class FakeIngestionRepository:
    def __init__(self, clock: FixedClock) -> None:
        self.clock = clock
        self.files: dict[UUID, RegisteredSourceFile] = {}
        self.by_remote_id: dict[str, UUID] = {}
        self.staged: dict[UUID, list[ParsedEvent]] = {}
        self.archive_hashes: set[str] = set()
        self.parse_contexts: dict[UUID, tuple[str, DocumentType, CompressionFormat]] = {}
        self.validated_staging: set[UUID] = set()
        self.validated_summaries: dict[UUID, StageSummary] = {}
        self.applied_sources: set[UUID] = set()
        self.replayed_sources: set[UUID] = set()
        self.current_fingerprints: dict[tuple[object, ...], str] = {}
        self.replays: dict[UUID, bool | None] = {}
        self.transition_errors: list[tuple[str | None, str | None]] = []
        self.replay_errors: dict[UUID, tuple[str | None, str | None]] = {}
        self.ingestion_allowed_calls: list[UUID | None] = []

    async def assert_ingestion_allowed(
        self,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> None:
        self.ingestion_allowed_calls.append(rebuild_run_id)

    async def register_discovery(
        self,
        remote_file: RemoteFile,
        *,
        owned_refresh: bool = False,
    ) -> RegisteredSourceFile:
        if remote_file.remote_id in self.by_remote_id:
            existing = self.files[self.by_remote_id[remote_file.remote_id]]
            if owned_refresh and existing.archive_object_key is None:
                existing = replace(
                    existing,
                    remote_file=replace(
                        existing.remote_file,
                        download_url=remote_file.download_url,
                    ),
                )
                self.files[existing.source_file_id] = existing
            return replace(existing, already_registered=True)
        source_file_id = uuid5(_NAMESPACE, remote_file.remote_id)
        registered = RegisteredSourceFile(
            source_file_id=source_file_id,
            remote_file=remote_file,
            status=IngestionStatus.DISCOVERED,
            already_registered=False,
        )
        self.files[source_file_id] = registered
        self.by_remote_id[remote_file.remote_id] = source_file_id
        return registered

    async def get(self, source_file_id: UUID) -> RegisteredSourceFile:
        try:
            return self.files[source_file_id]
        except KeyError as error:
            raise RepositoryError("unknown source file") from error

    async def transition(
        self,
        source_file_id: UUID,
        expected: Sequence[IngestionStatus],
        target: IngestionStatus,
        *,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if error_code is not None or error_message is not None:
            self.transition_errors.append((error_code, error_message))
        current = await self.get(source_file_id)
        if current.status not in expected:
            raise AssertionError(f"unexpected transition from {current.status} to {target}")
        self.files[source_file_id] = replace(current, status=target)

    async def record_archive(
        self,
        source_file_id: UUID,
        archived: ArchivedDownload,
        *,
        parser_version: str,
    ) -> bool:
        archive_reused = (
            not archived.archive.created or archived.archive.content_sha256 in self.archive_hashes
        )
        self.archive_hashes.add(archived.archive.content_sha256)
        current = await self.get(source_file_id)
        self.parse_contexts[source_file_id] = (
            parser_version,
            current.remote_file.document_type,
            current.remote_file.compression,
        )
        self.files[source_file_id] = replace(
            current,
            status=IngestionStatus.ARCHIVED,
            archive_object_key=archived.archive.object_key,
            content_sha256=archived.archive.content_sha256,
            archive_content_length=archived.archive.content_length,
        )
        return archive_reused

    async def clear_staging(self, source_file_id: UUID) -> None:
        self.staged[source_file_id] = []

    async def reuse_validated_staging(
        self,
        source_file_id: UUID,
        *,
        parser_version: str,
        document_type: DocumentType,
        compression: CompressionFormat,
    ) -> StageSummary | None:
        self.staged[source_file_id] = []
        target = await self.get(source_file_id)
        if (
            target.remote_file.document_type is not document_type
            or target.remote_file.compression is not compression
        ):
            raise AssertionError("staging reuse context differs from discovery")
        candidates = sorted(
            self.files.values(), key=lambda candidate: str(candidate.source_file_id)
        )
        for candidate in candidates:
            candidate_id = candidate.source_file_id
            if (
                candidate_id == source_file_id
                or candidate.status is not IngestionStatus.COMPLETED
                or candidate.content_sha256 != target.content_sha256
                or candidate.remote_file.retailer_id != target.remote_file.retailer_id
                or candidate.remote_file.portal_id != target.remote_file.portal_id
                or candidate_id not in self.validated_staging
                or candidate_id not in self.applied_sources
                or candidate_id in self.replayed_sources
                or self.parse_contexts.get(candidate_id)
                != (parser_version, document_type, compression)
            ):
                continue
            self.staged[source_file_id] = [
                replace(event, source_file_id=source_file_id)
                for event in self.staged.get(candidate_id, [])
            ]
            summary = stage_events(self.staged[source_file_id])
            if summary != self.validated_summaries.get(candidate_id):
                self.staged[source_file_id] = []
                return None
            return summary
        return None

    async def stage(self, source_file_id: UUID, events: Iterable[ParsedEvent]) -> StageSummary:
        batch = list(events)
        self.staged.setdefault(source_file_id, []).extend(batch)
        return stage_events(batch)

    async def finalize_staging(
        self,
        source_file_id: UUID,
        document_type: DocumentType,
    ) -> StageSummary:
        del document_type
        summary = stage_events(self.staged.get(source_file_id, []))
        self.validated_staging.add(source_file_id)
        self.validated_summaries[source_file_id] = summary
        return summary

    async def has_file_quarantine_issue(self, source_file_id: UUID) -> bool:
        return any(
            isinstance(event, ValidationIssue) and event.severity is IssueSeverity.FILE_QUARANTINE
            for event in self.staged.get(source_file_id, [])
        )

    async def apply(
        self,
        source_file_id: UUID,
        document_type: DocumentType,
        *,
        minimum_full_records: int,
        maximum_drop_fraction: float,
    ) -> ApplySummary:
        del maximum_drop_fraction
        records = [
            event
            for event in self.staged.get(source_file_id, [])
            if not isinstance(event, ValidationIssue)
            and event.__class__.__name__ != "DocumentMetadata"
        ]
        if document_type.is_full_snapshot and len(records) < minimum_full_records:
            raise QuarantinedFileError("full snapshot is unexpectedly small")
        inserted = updated = unchanged = history = 0
        for record in records:
            identity = _record_identity(record)
            fingerprint = repr(replace(record, source_file_id=UUID(int=0)))
            previous = self.current_fingerprints.get(identity)
            if previous == fingerprint:
                unchanged += 1
            else:
                self.current_fingerprints[identity] = fingerprint
                if previous is None:
                    inserted += 1
                else:
                    updated += 1
                if isinstance(record, PriceRecord):
                    history += 1
        self.applied_sources.add(source_file_id)
        return ApplySummary(inserted, updated, unchanged, 0, history)

    async def archive_key(self, source_file_id: UUID) -> str:
        value = (await self.get(source_file_id)).archive_object_key
        if value is None:
            raise RepositoryError("source file has no archive key")
        return value

    async def archive_sha256(self, source_file_id: UUID) -> str:
        value = (await self.get(source_file_id)).content_sha256
        if value is None:
            raise RepositoryError("source file has no archive digest")
        return value

    async def begin_replay(
        self,
        source_file_id: UUID,
        *,
        parser_version: str,
        rebuild_run_id: UUID | None = None,
    ) -> ReplayAttempt:
        del parser_version, rebuild_run_id
        replay = ReplayAttempt(uuid4(), source_file_id, self.clock.now())
        self.replayed_sources.add(source_file_id)
        self.replays[replay.replay_id] = None
        return replay

    async def finish_replay(
        self,
        replay_id: UUID,
        *,
        succeeded: bool,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if error_code is not None or error_message is not None:
            self.replay_errors[replay_id] = (error_code, error_message)
        self.replays[replay_id] = succeeded


def _record_identity(record: ParsedEvent) -> tuple[object, ...]:
    if isinstance(record, PriceRecord):
        return (
            PriceRecord,
            record.chain_id,
            record.subchain_id,
            record.store_id,
            record.item_code,
        )
    if isinstance(record, StoreRecord):
        return StoreRecord, record.chain_id, record.subchain_id, record.store_id
    if isinstance(record, PromotionRecord):
        return (
            PromotionRecord,
            record.chain_id,
            record.subchain_id,
            record.source_store_id,
            record.promotion_id,
        )
    raise AssertionError(f"unsupported applied record {type(record).__name__}")
