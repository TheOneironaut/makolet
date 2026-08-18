"""Idempotent discovery-file ingestion and deterministic archive replay."""

from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_random_exponential

from makolet.application.models import (
    DEFAULT_MAXIMUM_VALIDATION_ISSUE_BYTES_PER_ATTEMPT,
    DEFAULT_MAXIMUM_VALIDATION_ISSUES_PER_ATTEMPT,
    ApplySummary,
    ArchivedDownload,
    IngestionResult,
    RegisteredSourceFile,
    StageSummary,
    validation_issue_charge,
)
from makolet.application.observability import (
    NULL_LIFECYCLE_LOGGER,
    LifecycleEvent,
    LifecycleLogger,
)
from makolet.application.ports import (
    Clock,
    DocumentParser,
    Downloader,
    DownloadSession,
    IngestionRepository,
    LeaseManager,
    MetricRecorder,
    RawArchive,
)
from makolet.domain.enums import DocumentType, IngestionStatus, IssueSeverity
from makolet.domain.errors import (
    ArchiveCapacityError,
    ArchiveIntegrityError,
    ChargeBudgetExceededError,
    DownloadLimitError,
    LeaseUnavailableError,
    MakoletError,
    MalformedDocumentError,
    QuarantinedFileError,
    RepositoryError,
    UnsafeArchiveError,
)
from makolet.domain.models import (
    ArchiveReceipt,
    DocumentMetadata,
    ParsedEvent,
    PriceRecord,
    PromotionRecord,
    RemoteFile,
    StoreRecord,
    ValidationIssue,
    effective_promotion_store_ids,
)

_DEFAULT_MAXIMUM_STAGE_BATCH_BYTES = 64 * 1024 * 1024
_STAGE_CONTAINER_OVERHEAD_BYTES = 256
_STAGE_SCALAR_OVERHEAD_BYTES = 128
_STAGE_PERSISTENCE_ROW_OVERHEAD_BYTES = 512


@dataclass(frozen=True, slots=True)
class IngestionPolicy:
    """Resource and correctness limits for one ingestion attempt.

    ``stage_batch_size`` bounds staged rows, including promotion relationship rows,
    while ``maximum_stage_batch_bytes`` bounds a conservative retained-memory charge.
    """

    stage_batch_size: int = 5_000
    maximum_stage_batch_bytes: int = _DEFAULT_MAXIMUM_STAGE_BATCH_BYTES
    lease_ttl: timedelta = timedelta(minutes=30)
    download_attempts: int = 4
    minimum_full_records: int | None = None
    minimum_full_store_records: int = 1
    minimum_full_price_records: int = 100
    minimum_full_promotion_records: int = 1
    maximum_full_snapshot_drop_fraction: float = 0.50
    maximum_record_rejection_fraction: float = 0.10
    maximum_validation_issues: int = DEFAULT_MAXIMUM_VALIDATION_ISSUES_PER_ATTEMPT
    maximum_validation_issue_bytes: int = DEFAULT_MAXIMUM_VALIDATION_ISSUE_BYTES_PER_ATTEMPT

    def __post_init__(self) -> None:
        if (
            self.stage_batch_size <= 0
            or self.maximum_stage_batch_bytes <= 0
            or self.download_attempts <= 0
            or self.maximum_validation_issues <= 0
            or self.maximum_validation_issue_bytes <= 0
        ):
            raise ValueError("Ingestion batch and retry limits must be positive")
        if self.lease_ttl <= timedelta(0):
            raise ValueError("Ingestion lease TTL must be positive")
        minima = (
            self.minimum_full_store_records,
            self.minimum_full_price_records,
            self.minimum_full_promotion_records,
        )
        if self.minimum_full_records is not None and self.minimum_full_records < 0:
            raise ValueError("minimum_full_records cannot be negative")
        if any(minimum < 0 for minimum in minima):
            raise ValueError("document-family full snapshot minima cannot be negative")
        if not 0 <= self.maximum_full_snapshot_drop_fraction < 1:
            raise ValueError("maximum_full_snapshot_drop_fraction must be in [0, 1)")
        if not 0 <= self.maximum_record_rejection_fraction < 1:
            raise ValueError("maximum_record_rejection_fraction must be in [0, 1)")

    def minimum_full_records_for(self, document_type: DocumentType) -> int:
        if self.minimum_full_records is not None:
            return self.minimum_full_records
        if document_type is DocumentType.STORES:
            return self.minimum_full_store_records
        if document_type.is_price:
            return self.minimum_full_price_records
        return self.minimum_full_promotion_records


class IngestionService:
    def __init__(
        self,
        repository: IngestionRepository,
        downloader: Downloader,
        archive: RawArchive,
        parser: DocumentParser,
        leases: LeaseManager,
        clock: Clock,
        metrics: MetricRecorder,
        *,
        worker_id: str,
        policy: IngestionPolicy | None = None,
        events: LifecycleLogger | None = None,
    ) -> None:
        if not worker_id:
            raise ValueError("worker_id is required")
        self._repository = repository
        self._downloader = downloader
        self._archive = archive
        self._parser = parser
        self._leases = leases
        self._clock = clock
        self._metrics = metrics
        self._worker_id = worker_id
        self._policy = policy or IngestionPolicy()
        self._events = events or NULL_LIFECYCLE_LOGGER

    @property
    def parser_version(self) -> str:
        return self._parser.parser_version

    async def register(self, remote_file: RemoteFile) -> RegisteredSourceFile:
        """Durably register source identity before collection reserves network bytes."""

        await self._repository.assert_ingestion_allowed()
        return await self._repository.register_discovery(remote_file)

    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        _validate_archive_byte_limit(maximum_charged_bytes)
        run_id = str(uuid4())
        with self._events.context_if_absent(
            correlation_id=run_id,
            portal_id=remote_file.portal_id,
            retailer_id=remote_file.retailer_id,
            run_id=run_id,
            source_id=remote_file.retailer_id,
        ):
            return await self._ingest(
                remote_file,
                maximum_charged_bytes=maximum_charged_bytes,
            )

    async def _ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None,
    ) -> IngestionResult:
        await self._repository.assert_ingestion_allowed()
        registered = await self._repository.register_discovery(remote_file)
        operation_started = time.perf_counter()
        with self._events.context(source_file_id=registered.source_file_id):
            if registered.status is IngestionStatus.COMPLETED:
                result = _duplicate_result(
                    registered.source_file_id,
                    registered.content_sha256 or registered.completed_content_sha256,
                )
                self._log_ingestion_completed(
                    result,
                    archive_only=False,
                    started=operation_started,
                )
                return result

            resource = f"source-file:{registered.source_file_id}"
            async with self._leases.acquire(
                resource, self._worker_id, self._policy.lease_ttl
            ) as acquired:
                if not acquired:
                    error = LeaseUnavailableError("Source file is already being processed")
                    self._events.warning(
                        LifecycleEvent.INGESTION_FAILED,
                        archive_only=False,
                        duration_seconds=time.perf_counter() - operation_started,
                        error_code=error.code,
                        status="failed_retryable",
                    )
                    raise error
                current = await self._repository.register_discovery(
                    remote_file,
                    owned_refresh=True,
                )
                if current.status is IngestionStatus.COMPLETED:
                    result = _duplicate_result(
                        current.source_file_id,
                        current.content_sha256,
                    )
                    self._log_ingestion_completed(
                        result,
                        archive_only=False,
                        started=operation_started,
                    )
                    return result
                metric_started = time.perf_counter()
                transferred_bytes = 0
                try:
                    (
                        object_key,
                        content_sha256,
                        duplicate,
                        transferred_bytes,
                    ) = await self._ensure_archived(
                        current,
                        maximum_charged_bytes=maximum_charged_bytes,
                    )
                    stage, applied = await self._parse_validate_apply(
                        current.source_file_id,
                        current.remote_file,
                        object_key,
                        transition_states=True,
                    )
                    self._metrics.increment(
                        "ingestion_completed_total",
                        labels={"retailer": remote_file.retailer_id},
                    )
                    if duplicate:
                        self._metrics.increment(
                            "ingestion_archive_deduplicated_total",
                            labels={"retailer": remote_file.retailer_id},
                        )
                    result = IngestionResult(
                        source_file_id=current.source_file_id,
                        status=IngestionStatus.COMPLETED,
                        content_sha256=content_sha256,
                        stage=stage,
                        apply=applied,
                        duplicate=duplicate,
                        transferred_bytes=transferred_bytes,
                    )
                except Exception as original_error:
                    propagated_error: Exception = original_error
                    if transferred_bytes and isinstance(original_error, MakoletError):
                        original_error.transferred_bytes = max(
                            original_error.transferred_bytes,
                            transferred_bytes,
                        )
                        propagated_error = original_error
                    elif transferred_bytes:
                        propagated_error = RepositoryError(
                            "Archived source processing failed",
                            transferred_bytes=transferred_bytes,
                        )
                    await self._record_failure(
                        current.source_file_id,
                        propagated_error,
                        archive_only=False,
                        started=operation_started,
                    )
                    if propagated_error is not original_error:
                        raise propagated_error from original_error
                    raise
                else:
                    self._log_ingestion_completed(
                        result,
                        archive_only=False,
                        started=operation_started,
                    )
                    return result
                finally:
                    self._metrics.observe(
                        "ingestion_duration_seconds",
                        time.perf_counter() - metric_started,
                        labels={"retailer": remote_file.retailer_id},
                    )

    async def archive_only(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult:
        """Download and immutably archive a source without changing normalized state."""

        _validate_archive_byte_limit(maximum_charged_bytes)
        run_id = str(uuid4())
        with self._events.context_if_absent(
            correlation_id=run_id,
            portal_id=remote_file.portal_id,
            retailer_id=remote_file.retailer_id,
            run_id=run_id,
            source_id=remote_file.retailer_id,
        ):
            return await self._archive_only(
                remote_file,
                maximum_charged_bytes=maximum_charged_bytes,
            )

    async def _archive_only(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None,
    ) -> IngestionResult:
        await self._repository.assert_ingestion_allowed()
        registered = await self._repository.register_discovery(remote_file)
        operation_started = time.perf_counter()
        with self._events.context(source_file_id=registered.source_file_id):
            if registered.status is IngestionStatus.COMPLETED:
                result = _duplicate_result(
                    registered.source_file_id,
                    registered.content_sha256 or registered.completed_content_sha256,
                )
                self._log_ingestion_completed(
                    result,
                    archive_only=True,
                    started=operation_started,
                )
                return result
            resource = f"source-file:{registered.source_file_id}"
            async with self._leases.acquire(
                resource, self._worker_id, self._policy.lease_ttl
            ) as acquired:
                if not acquired:
                    error = LeaseUnavailableError("Source file is already being processed")
                    self._events.warning(
                        LifecycleEvent.INGESTION_FAILED,
                        archive_only=True,
                        duration_seconds=time.perf_counter() - operation_started,
                        error_code=error.code,
                        status="failed_retryable",
                    )
                    raise error
                current = await self._repository.register_discovery(
                    remote_file,
                    owned_refresh=True,
                )
                transferred_bytes = 0
                try:
                    (
                        _object_key,
                        content_sha256,
                        duplicate,
                        transferred_bytes,
                    ) = await self._ensure_archived(
                        current,
                        maximum_charged_bytes=maximum_charged_bytes,
                    )
                    if (
                        current.archive_object_key is not None
                        and current.status is not IngestionStatus.ARCHIVED
                    ):
                        await self._repository.transition(
                            current.source_file_id,
                            (IngestionStatus.FAILED_RETRYABLE,),
                            IngestionStatus.ARCHIVED,
                        )
                    self._metrics.increment(
                        "ingestion_archived_without_apply_total",
                        labels={"retailer": remote_file.retailer_id},
                    )
                    if duplicate:
                        self._metrics.increment(
                            "ingestion_archive_deduplicated_total",
                            labels={"retailer": remote_file.retailer_id},
                        )
                    result = IngestionResult(
                        source_file_id=current.source_file_id,
                        status=IngestionStatus.ARCHIVED,
                        content_sha256=content_sha256,
                        stage=None,
                        apply=None,
                        duplicate=duplicate,
                        transferred_bytes=transferred_bytes,
                    )
                except Exception as original_error:
                    propagated_error: Exception = original_error
                    if transferred_bytes and isinstance(original_error, MakoletError):
                        original_error.transferred_bytes = max(
                            original_error.transferred_bytes,
                            transferred_bytes,
                        )
                        propagated_error = original_error
                    elif transferred_bytes:
                        propagated_error = RepositoryError(
                            "Archived source processing failed",
                            transferred_bytes=transferred_bytes,
                        )
                    await self._record_failure(
                        current.source_file_id,
                        propagated_error,
                        archive_only=True,
                        started=operation_started,
                    )
                    if propagated_error is not original_error:
                        raise propagated_error from original_error
                    raise
                else:
                    self._log_ingestion_completed(
                        result,
                        archive_only=True,
                        started=operation_started,
                    )
                    return result

    async def replay(
        self,
        source_file_id: UUID,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> IngestionResult:
        run_id = str(uuid4())
        context: dict[str, object] = {
            "correlation_id": run_id,
            "run_id": run_id,
            "source_file_id": source_file_id,
        }
        if rebuild_run_id is not None:
            context["rebuild_run_id"] = rebuild_run_id
        with self._events.context_if_absent(**context):
            started = time.perf_counter()
            try:
                await self._repository.assert_ingestion_allowed(rebuild_run_id=rebuild_run_id)
                registered = await self._repository.get(source_file_id)
                with self._events.context(
                    portal_id=registered.remote_file.portal_id,
                    retailer_id=registered.remote_file.retailer_id,
                    source_id=registered.remote_file.retailer_id,
                ):
                    self._events.info(
                        LifecycleEvent.REPLAY_STARTED,
                        status="running",
                    )
                    result, replay_id = await self._replay_registered(
                        registered,
                        rebuild_run_id=rebuild_run_id,
                    )
                    with self._events.context(replay_id=replay_id):
                        self._events.info(
                            LifecycleEvent.REPLAY_COMPLETED,
                            duration_seconds=time.perf_counter() - started,
                            replayed=True,
                            status="completed",
                            **_result_count_fields(result),
                        )
                    return result
            except Exception as error:
                self._events.warning(
                    LifecycleEvent.REPLAY_FAILED,
                    duration_seconds=time.perf_counter() - started,
                    error_code=_error_code(error),
                    replayed=True,
                    status="failed",
                )
                raise

    async def _replay_registered(
        self,
        registered: RegisteredSourceFile,
        *,
        rebuild_run_id: UUID | None,
    ) -> tuple[IngestionResult, UUID]:
        source_file_id = registered.source_file_id
        resource = f"source-file:{source_file_id}"
        async with self._leases.acquire(
            resource, self._worker_id, self._policy.lease_ttl
        ) as acquired:
            if not acquired:
                raise LeaseUnavailableError("Source file is already being processed")
            current = await self._repository.get(source_file_id)
            if current.archive_object_key is None or current.content_sha256 is None:
                raise MalformedDocumentError("Source file has no archived bytes to replay")
            verified_length = await self._archive.verify(
                current.archive_object_key,
                current.content_sha256,
            )
            _ensure_archive_length(current, verified_length)
            self._events.info(
                LifecycleEvent.ARCHIVE_DEDUPLICATED,
                content_length=verified_length,
                created=False,
                duplicate=True,
                status="verified",
            )
            attempt = await self._repository.begin_replay(
                source_file_id,
                parser_version=self._parser.parser_version,
                rebuild_run_id=rebuild_run_id,
            )
            with self._events.context(replay_id=attempt.replay_id):
                try:
                    stage, applied = await self._parse_validate_apply(
                        source_file_id,
                        current.remote_file,
                        current.archive_object_key,
                        transition_states=False,
                    )
                except Exception as error:
                    await self._repository.finish_replay(
                        attempt.replay_id,
                        succeeded=False,
                        error_code=_error_code(error),
                        error_message=_persisted_error_message(error),
                    )
                    raise
                await self._repository.finish_replay(attempt.replay_id, succeeded=True)
                self._metrics.increment(
                    "ingestion_replay_completed_total",
                    labels={"retailer": current.remote_file.retailer_id},
                )
                return (
                    IngestionResult(
                        source_file_id=source_file_id,
                        # This is the normalized apply outcome. An archive-only
                        # source may retain its ARCHIVED lifecycle state, while
                        # downstream catalog bootstrap still needs COMPLETED.
                        status=IngestionStatus.COMPLETED,
                        content_sha256=current.content_sha256,
                        stage=stage,
                        apply=applied,
                        replayed=True,
                    ),
                    attempt.replay_id,
                )

    async def _ensure_archived(
        self,
        registered: RegisteredSourceFile,
        *,
        maximum_charged_bytes: int | None,
    ) -> tuple[str, str, bool, int]:
        if registered.archive_object_key is not None:
            digest = registered.content_sha256 or await self._repository.archive_sha256(
                registered.source_file_id
            )
            verified_length = await self._archive.verify(registered.archive_object_key, digest)
            _ensure_archive_length(registered, verified_length)
            self._events.info(
                LifecycleEvent.ARCHIVE_DEDUPLICATED,
                content_length=verified_length,
                created=False,
                duplicate=True,
                status="verified",
            )
            return registered.archive_object_key, digest, False, 0
        archived, duplicate = await self._download_and_archive(
            registered,
            maximum_charged_bytes=maximum_charged_bytes,
        )
        return (
            archived.archive.object_key,
            archived.archive.content_sha256,
            duplicate,
            archived.transferred_bytes,
        )

    async def _download_and_archive(
        self,
        registered: RegisteredSourceFile,
        *,
        maximum_charged_bytes: int | None,
    ) -> tuple[ArchivedDownload, bool]:
        await self._repository.transition(
            registered.source_file_id,
            (IngestionStatus.DISCOVERED, IngestionStatus.FAILED_RETRYABLE),
            IngestionStatus.DOWNLOADING,
        )

        transferred_bytes = 0
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._policy.download_attempts),
            wait=wait_random_exponential(multiplier=0.25, max=8),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        ):
            with attempt:
                started = time.perf_counter()
                attempt_number = attempt.retry_state.attempt_number
                attempt_transferred_bytes = 0
                session: DownloadSession | None = None
                self._events.info(
                    LifecycleEvent.DOWNLOAD_STARTED,
                    attempt=attempt_number,
                    attempt_limit=self._policy.download_attempts,
                    status="running",
                )
                try:
                    remaining_bytes = (
                        None
                        if maximum_charged_bytes is None
                        else maximum_charged_bytes - transferred_bytes
                    )
                    if remaining_bytes is not None and remaining_bytes <= 0:
                        _raise_retry_budget_exhausted(transferred_bytes)
                    download = (
                        self._downloader.open(registered.remote_file)
                        if remaining_bytes is None
                        else self._downloader.open(
                            registered.remote_file,
                            maximum_bytes=remaining_bytes,
                        )
                    )
                    async with download as session:
                        object_key, content_length, created = await self._archive.put(
                            session.iter_raw(),
                            original_filename=registered.remote_file.original_filename,
                        )
                        attempt_transferred_bytes = max(content_length, session.transferred_bytes)
                        evidence = await session.finish(content_length)
                        content_sha256 = object_key.rsplit("/", 1)[-1]
                        archived = ArchivedDownload(
                            archive=ArchiveReceipt(
                                content_sha256=content_sha256,
                                object_key=object_key,
                                content_length=content_length,
                                archived_at=evidence.finished_at,
                                created=created,
                            ),
                            evidence=evidence,
                        )
                        duplicate = await self._repository.record_archive(
                            registered.source_file_id,
                            archived,
                            parser_version=self._parser.parser_version,
                        )
                        transferred_bytes += attempt_transferred_bytes
                        self._metrics.increment(
                            "ingestion_files_downloaded_total",
                            labels={"retailer": registered.remote_file.retailer_id},
                        )
                        self._metrics.observe(
                            "ingestion_file_bytes",
                            float(content_length),
                            labels={"retailer": registered.remote_file.retailer_id},
                        )
                        archive_event = (
                            LifecycleEvent.ARCHIVE_DEDUPLICATED
                            if duplicate or not created
                            else LifecycleEvent.ARCHIVE_STORED
                        )
                        self._events.info(
                            archive_event,
                            content_length=content_length,
                            created=created,
                            duplicate=duplicate or not created,
                            status="completed",
                        )
                        self._events.info(
                            LifecycleEvent.DOWNLOAD_COMPLETED,
                            attempt=attempt_number,
                            attempt_limit=self._policy.download_attempts,
                            content_length=content_length,
                            duration_seconds=time.perf_counter() - started,
                            status="completed",
                        )
                        return replace(archived, transferred_bytes=transferred_bytes), duplicate
                except DownloadLimitError as error:
                    transferred_bytes += max(
                        attempt_transferred_bytes,
                        error.transferred_bytes,
                        session.transferred_bytes if session is not None else 0,
                    )
                    limit_error: Exception = error
                    if error.budget_limited:
                        limit_error = ChargeBudgetExceededError(
                            "Source transfer exceeds the remaining collection charged-byte budget",
                            transferred_bytes=transferred_bytes,
                        )
                    else:
                        error.transferred_bytes = transferred_bytes
                    self._events.warning(
                        LifecycleEvent.DOWNLOAD_FAILED,
                        attempt=attempt_number,
                        attempt_limit=self._policy.download_attempts,
                        duration_seconds=time.perf_counter() - started,
                        error_code=_error_code(limit_error),
                        status="failed",
                    )
                    if limit_error is not error:
                        raise limit_error from error
                    raise
                except Exception as error:
                    error_transferred_bytes = (
                        error.transferred_bytes if isinstance(error, MakoletError) else 0
                    )
                    transferred_bytes += max(
                        attempt_transferred_bytes,
                        error_transferred_bytes,
                        session.transferred_bytes if session is not None else 0,
                    )
                    download_error: Exception = error
                    if isinstance(error, MakoletError):
                        error.transferred_bytes = transferred_bytes
                    elif transferred_bytes:
                        download_error = RepositoryError(
                            "Archive evidence could not be recorded",
                            transferred_bytes=transferred_bytes,
                        )
                    self._events.warning(
                        LifecycleEvent.DOWNLOAD_FAILED,
                        attempt=attempt_number,
                        attempt_limit=self._policy.download_attempts,
                        duration_seconds=time.perf_counter() - started,
                        error_code=_error_code(download_error),
                        status="failed",
                    )
                    if download_error is not error:
                        raise download_error from error
                    raise
                finally:
                    self._metrics.observe(
                        "ingestion_download_duration_seconds",
                        time.perf_counter() - started,
                        labels={"retailer": registered.remote_file.retailer_id},
                    )
        raise AssertionError("download retry loop must return or raise")

    async def _parse_validate_apply(
        self,
        source_file_id: UUID,
        remote_file: RemoteFile,
        object_key: str,
        *,
        transition_states: bool,
    ) -> tuple[StageSummary, ApplySummary]:
        if transition_states:
            await self._repository.transition(
                source_file_id,
                (IngestionStatus.ARCHIVED, IngestionStatus.FAILED_RETRYABLE),
                IngestionStatus.PARSING,
            )
        summary = StageSummary()
        batch: list[ParsedEvent] = []
        batch_units = 0
        batch_bytes = 0
        validation_issue_count = 0
        validation_issue_bytes = 0
        parsing_started = time.perf_counter()
        stage_failure_logged = False
        parser_started = False

        async def flush_batch() -> None:
            nonlocal batch_bytes, batch_units, stage_failure_logged, summary
            try:
                staged = await self._repository.stage(source_file_id, batch)
            except Exception as error:
                stage_failure_logged = True
                self._events.warning(
                    LifecycleEvent.STAGE_FAILED,
                    document_type=remote_file.document_type.value,
                    duration_seconds=time.perf_counter() - parsing_started,
                    error_code=_error_code(error),
                    status="failed",
                )
                raise
            summary = _combine_stage(summary, staged)
            batch.clear()
            batch_units = 0
            batch_bytes = 0

        try:
            try:
                reused = await self._repository.reuse_validated_staging(
                    source_file_id,
                    parser_version=self._parser.parser_version,
                    document_type=remote_file.document_type,
                    compression=remote_file.compression,
                )
            except Exception as error:
                stage_failure_logged = True
                self._events.info(
                    LifecycleEvent.STAGE_STARTED,
                    document_type=remote_file.document_type.value,
                    status="running",
                )
                self._events.warning(
                    LifecycleEvent.STAGE_FAILED,
                    document_type=remote_file.document_type.value,
                    duration_seconds=time.perf_counter() - parsing_started,
                    error_code=_error_code(error),
                    status="failed",
                )
                raise
            if reused is None:
                parser_started = True
                self._events.info(
                    LifecycleEvent.PARSE_STARTED,
                    compression=remote_file.compression.value,
                    document_type=remote_file.document_type.value,
                    status="running",
                )
                self._events.info(
                    LifecycleEvent.STAGE_STARTED,
                    document_type=remote_file.document_type.value,
                    status="running",
                )
                async with self._archive.open(object_key) as chunks:
                    async for event in self._parser.parse(
                        chunks,
                        source_file_id=source_file_id,
                        document_type=remote_file.document_type,
                        compression=remote_file.compression,
                        filename=remote_file.original_filename,
                    ):
                        if isinstance(event, ValidationIssue):
                            validation_issue_count += 1
                            validation_issue_bytes += validation_issue_charge(event)
                            _ensure_validation_issue_attempt_within_limits(
                                validation_issue_count,
                                validation_issue_bytes,
                                policy=self._policy,
                            )
                        event_units = _stage_units(event)
                        event_bytes = _stage_memory_charge(event, event_units)
                        _ensure_stage_event_within_limits(
                            event_units,
                            event_bytes,
                            policy=self._policy,
                        )
                        if batch and (
                            batch_units + event_units > self._policy.stage_batch_size
                            or batch_bytes + event_bytes > self._policy.maximum_stage_batch_bytes
                        ):
                            await flush_batch()
                        batch.append(event)
                        batch_units += event_units
                        batch_bytes += event_bytes
                        if (
                            batch_units >= self._policy.stage_batch_size
                            or batch_bytes >= self._policy.maximum_stage_batch_bytes
                        ):
                            await flush_batch()
                if batch:
                    await flush_batch()
            else:
                summary = reused
                self._events.info(
                    LifecycleEvent.STAGE_STARTED,
                    document_type=remote_file.document_type.value,
                    status="running",
                )
                self._events.info(
                    LifecycleEvent.STAGE_REUSED,
                    accepted_records=summary.accepted_records,
                    document_type=remote_file.document_type.value,
                    rejected_records=summary.rejected_records,
                    status="completed",
                    warning_count=summary.warnings,
                )
            try:
                summary = await self._repository.finalize_staging(
                    source_file_id,
                    remote_file.document_type,
                )
            except Exception as error:
                stage_failure_logged = True
                self._events.warning(
                    LifecycleEvent.STAGE_FAILED,
                    document_type=remote_file.document_type.value,
                    duration_seconds=time.perf_counter() - parsing_started,
                    error_code=_error_code(error),
                    status="failed",
                )
                raise
        except Exception as error:
            if not stage_failure_logged:
                self._events.warning(
                    (
                        LifecycleEvent.PARSE_FAILED
                        if parser_started
                        else LifecycleEvent.STAGE_FAILED
                    ),
                    compression=remote_file.compression.value,
                    document_type=remote_file.document_type.value,
                    duration_seconds=time.perf_counter() - parsing_started,
                    error_code=_error_code(error),
                    status="failed",
                )
            raise
        finally:
            self._metrics.observe(
                "ingestion_parsing_duration_seconds",
                time.perf_counter() - parsing_started,
                labels={"retailer": remote_file.retailer_id},
            )
        parsing_duration = time.perf_counter() - parsing_started
        if parser_started:
            self._events.info(
                LifecycleEvent.PARSE_COMPLETED,
                accepted_records=summary.accepted_records,
                document_type=remote_file.document_type.value,
                duration_seconds=parsing_duration,
                rejected_records=summary.rejected_records,
                status="completed",
                warning_count=summary.warnings,
            )
        self._events.info(
            LifecycleEvent.STAGE_COMPLETED,
            accepted_records=summary.accepted_records,
            duration_seconds=parsing_duration,
            metadata_records=summary.metadata_records,
            price_records=summary.price_records,
            promotion_records=summary.promotion_records,
            rejected_records=summary.rejected_records,
            status="completed",
            store_records=summary.store_records,
            warning_count=summary.warnings,
        )

        staged_records = (
            summary.metadata_records
            + summary.store_records
            + summary.price_records
            + summary.promotion_records
        )
        self._metrics.increment(
            "ingestion_records_staged_total",
            labels={"retailer": remote_file.retailer_id},
            value=staged_records,
        )
        self._metrics.increment(
            "ingestion_records_rejected_total",
            labels={"retailer": remote_file.retailer_id},
            value=summary.rejected_records,
        )
        self._metrics.increment(
            "ingestion_warnings_total",
            labels={"retailer": remote_file.retailer_id},
            value=summary.warnings,
        )

        if transition_states:
            await self._repository.transition(
                source_file_id, (IngestionStatus.PARSING,), IngestionStatus.STAGED
            )
            await self._repository.transition(
                source_file_id, (IngestionStatus.STAGED,), IngestionStatus.VALIDATING
            )
        if await self._repository.has_file_quarantine_issue(source_file_id):
            raise QuarantinedFileError("Staged records contain a file-quarantine issue")
        evaluated_records = summary.accepted_records + summary.rejected_records
        if summary.rejected_records and (
            summary.accepted_records == 0
            or summary.rejected_records / evaluated_records
            > self._policy.maximum_record_rejection_fraction
        ):
            raise QuarantinedFileError(
                "Record rejection fraction exceeds the configured file threshold"
            )
        if transition_states:
            await self._repository.transition(
                source_file_id, (IngestionStatus.VALIDATING,), IngestionStatus.APPLYING
            )
        apply_started = time.perf_counter()
        self._events.info(
            LifecycleEvent.APPLY_STARTED,
            document_type=remote_file.document_type.value,
            status="running",
        )
        try:
            applied = await self._repository.apply(
                source_file_id,
                remote_file.document_type,
                minimum_full_records=self._policy.minimum_full_records_for(
                    remote_file.document_type
                ),
                maximum_drop_fraction=self._policy.maximum_full_snapshot_drop_fraction,
            )
        except Exception as error:
            self._events.warning(
                LifecycleEvent.APPLY_FAILED,
                document_type=remote_file.document_type.value,
                duration_seconds=time.perf_counter() - apply_started,
                error_code=_error_code(error),
                status="failed",
            )
            raise
        finally:
            self._metrics.observe(
                "ingestion_database_apply_duration_seconds",
                time.perf_counter() - apply_started,
                labels={"retailer": remote_file.retailer_id},
            )
        self._events.info(
            LifecycleEvent.APPLY_COMPLETED,
            duration_seconds=time.perf_counter() - apply_started,
            history_event_count=applied.history_events,
            inserted_count=applied.inserted,
            status="completed",
            unavailable_count=applied.unavailable,
            unchanged_count=applied.unchanged,
            updated_count=applied.updated,
        )
        if transition_states:
            await self._repository.transition(
                source_file_id, (IngestionStatus.APPLYING,), IngestionStatus.COMPLETED
            )
        return summary, applied

    def _log_ingestion_completed(
        self,
        result: IngestionResult,
        *,
        archive_only: bool,
        started: float,
    ) -> None:
        self._events.info(
            LifecycleEvent.INGESTION_COMPLETED,
            archive_only=archive_only,
            duplicate=result.duplicate,
            duration_seconds=time.perf_counter() - started,
            replayed=result.replayed,
            status=result.status.value,
            **_result_count_fields(result),
        )

    async def _record_failure(
        self,
        source_file_id: UUID,
        error: Exception,
        *,
        archive_only: bool,
        started: float,
    ) -> None:
        current = await self._repository.get(source_file_id)
        if current.status in {
            IngestionStatus.COMPLETED,
            IngestionStatus.QUARANTINED,
            IngestionStatus.FAILED_TERMINAL,
        }:
            return
        quarantine_error = isinstance(
            error, (MalformedDocumentError, UnsafeArchiveError, QuarantinedFileError)
        )
        can_quarantine = current.status in {
            IngestionStatus.ARCHIVED,
            IngestionStatus.PARSING,
            IngestionStatus.STAGED,
            IngestionStatus.VALIDATING,
            IngestionStatus.APPLYING,
        }
        if quarantine_error and can_quarantine:
            target = IngestionStatus.QUARANTINED
        elif isinstance(error, MakoletError) and error.retryable:
            target = IngestionStatus.FAILED_RETRYABLE
        else:
            target = IngestionStatus.FAILED_TERMINAL
        await self._repository.transition(
            source_file_id,
            (current.status,),
            target,
            error_code=_error_code(error),
            error_message=_persisted_error_message(error),
        )
        event = (
            LifecycleEvent.INGESTION_QUARANTINED
            if target is IngestionStatus.QUARANTINED
            else LifecycleEvent.INGESTION_FAILED
        )
        self._events.warning(
            event,
            archive_only=archive_only,
            duration_seconds=time.perf_counter() - started,
            error_code=_error_code(error),
            status=target.value,
        )
        self._metrics.increment(
            "ingestion_failure_total",
            labels={"retailer": current.remote_file.retailer_id, "status": target.value},
        )
        if quarantine_error:
            self._metrics.increment(
                "ingestion_parser_failure_total",
                labels={
                    "retailer": current.remote_file.retailer_id,
                    "error_code": _error_code(error),
                },
            )


def _ensure_archive_length(registered: RegisteredSourceFile, verified_length: int) -> None:
    expected = registered.archive_content_length
    if expected is not None and verified_length != expected:
        raise ArchiveIntegrityError("Archived byte length differs from durable metadata")


def _validate_archive_byte_limit(maximum_charged_bytes: int | None) -> None:
    if maximum_charged_bytes is not None and maximum_charged_bytes <= 0:
        raise ValueError("maximum_charged_bytes must be positive when supplied")


def _combine_stage(left: StageSummary, right: StageSummary) -> StageSummary:
    return StageSummary(
        metadata_records=left.metadata_records + right.metadata_records,
        store_records=left.store_records + right.store_records,
        price_records=left.price_records + right.price_records,
        promotion_records=left.promotion_records + right.promotion_records,
        warnings=left.warnings + right.warnings,
        rejected_records=left.rejected_records + right.rejected_records,
        file_quarantines=left.file_quarantines + right.file_quarantines,
        validation_issue_bytes=(left.validation_issue_bytes + right.validation_issue_bytes),
        sampled_validation_issues=(
            left.sampled_validation_issues + right.sampled_validation_issues
        ),
    )


def _ensure_validation_issue_attempt_within_limits(
    issue_count: int,
    issue_bytes: int,
    *,
    policy: IngestionPolicy,
) -> None:
    if (
        issue_count > policy.maximum_validation_issues
        or issue_bytes > policy.maximum_validation_issue_bytes
    ):
        raise QuarantinedFileError("Validation issue evidence exceeds the configured attempt limit")


def _stage_units(event: ParsedEvent) -> int:
    if isinstance(event, PromotionRecord):
        return (
            1 + len(event.items) + len(effective_promotion_store_ids(event)) + len(event.club_ids)
        )
    return 1


def _ensure_stage_event_within_limits(
    stage_units: int,
    stage_bytes: int,
    *,
    policy: IngestionPolicy,
) -> None:
    if stage_units > policy.stage_batch_size:
        raise QuarantinedFileError("Parsed event exceeds the configured staging row limit")
    if stage_bytes > policy.maximum_stage_batch_bytes:
        raise QuarantinedFileError("Parsed event exceeds the configured staging memory limit")


def _stage_memory_charge(event: ParsedEvent, stage_units: int) -> int:
    return _retained_value_charge(event) + (stage_units * _STAGE_PERSISTENCE_ROW_OVERHEAD_BYTES)


def _retained_value_charge(value: object) -> int:
    if isinstance(value, str):
        return _STAGE_SCALAR_OVERHEAD_BYTES + (4 * len(value))
    if isinstance(value, tuple):
        return _STAGE_CONTAINER_OVERHEAD_BYTES + sum(_retained_value_charge(item) for item in value)
    if isinstance(value, Decimal):
        return _STAGE_SCALAR_OVERHEAD_BYTES + (4 * len(value.as_tuple().digits))
    if isinstance(value, int):
        return _STAGE_SCALAR_OVERHEAD_BYTES + ((value.bit_length() + 7) // 8)
    if is_dataclass(value) and not isinstance(value, type):
        return _STAGE_CONTAINER_OVERHEAD_BYTES + sum(
            _retained_value_charge(getattr(value, value_field.name))
            for value_field in fields(value)
        )
    return _STAGE_SCALAR_OVERHEAD_BYTES


def _duplicate_result(source_file_id: UUID, content_sha256: str | None) -> IngestionResult:
    return IngestionResult(
        source_file_id=source_file_id,
        status=IngestionStatus.COMPLETED,
        content_sha256=content_sha256,
        stage=None,
        apply=None,
        duplicate=True,
    )


def _is_retryable(error: BaseException) -> bool:
    return (
        isinstance(error, MakoletError)
        and error.retryable
        and not isinstance(error, (ChargeBudgetExceededError, ArchiveCapacityError))
    )


def _raise_retry_budget_exhausted(_transferred_bytes: int) -> None:
    raise ChargeBudgetExceededError("Source retries exhausted the remaining collection byte budget")


def _error_code(error: BaseException) -> str:
    return error.code if isinstance(error, MakoletError) else "unexpected_error"


def _persisted_error_message(error: BaseException) -> str:
    if isinstance(error, (MalformedDocumentError, UnsafeArchiveError, QuarantinedFileError)):
        return "Archived source document was quarantined by validation"
    if isinstance(error, MakoletError) and error.retryable:
        return "Ingestion failed temporarily and may be retried"
    if isinstance(error, MakoletError):
        return "Ingestion failed with a classified application error"
    return "Ingestion failed unexpectedly"


def _result_count_fields(result: IngestionResult) -> dict[str, object]:
    fields: dict[str, object] = {}
    if result.stage is not None:
        fields.update(
            {
                "accepted_records": result.stage.accepted_records,
                "metadata_records": result.stage.metadata_records,
                "price_records": result.stage.price_records,
                "promotion_records": result.stage.promotion_records,
                "rejected_records": result.stage.rejected_records,
                "store_records": result.stage.store_records,
                "warning_count": result.stage.warnings,
            }
        )
    if result.apply is not None:
        fields.update(
            {
                "history_event_count": result.apply.history_events,
                "inserted_count": result.apply.inserted,
                "unavailable_count": result.apply.unavailable,
                "unchanged_count": result.apply.unchanged,
                "updated_count": result.apply.updated,
            }
        )
    return fields


def stage_events(events: Iterable[ParsedEvent]) -> StageSummary:
    """Count one batch consistently for repository adapters and tests."""

    metadata = stores = prices = promotions = warnings = rejected = quarantines = 0
    issue_bytes = 0
    for event in events:
        if isinstance(event, DocumentMetadata):
            metadata += 1
        elif isinstance(event, StoreRecord):
            stores += 1
        elif isinstance(event, PriceRecord):
            prices += 1
        elif isinstance(event, PromotionRecord):
            promotions += 1
        elif isinstance(event, ValidationIssue):
            issue_bytes += validation_issue_charge(event)
            if event.severity is IssueSeverity.WARNING:
                warnings += 1
            elif event.severity is IssueSeverity.RECORD_REJECTION:
                rejected += 1
            elif event.severity is IssueSeverity.FILE_QUARANTINE:
                quarantines += 1
    issue_count = warnings + rejected + quarantines
    return StageSummary(
        metadata,
        stores,
        prices,
        promotions,
        warnings,
        rejected,
        quarantines,
        issue_bytes,
        issue_count,
    )
