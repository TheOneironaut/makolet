"""Bounded source discovery orchestration shared by CLI and workers."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from typing import Protocol, cast
from uuid import UUID, uuid4

from makolet.application.models import (
    DEFAULT_MAXIMUM_LISTING_BYTES_PER_SOURCE_RUN,
    DEFAULT_MAXIMUM_LISTING_ELAPSED_SECONDS_PER_SOURCE_RUN,
    DEFAULT_MAXIMUM_LISTING_REQUESTS_PER_SOURCE_RUN,
    CollectionAttempt,
    CollectionChargeBudget,
    CollectionScope,
    DiscoveryCursor,
    DiscoveryPage,
    DiscoveryRunBudget,
    IngestionResult,
    RegisteredSourceFile,
)
from makolet.application.observability import (
    NULL_LIFECYCLE_LOGGER,
    LifecycleEvent,
    LifecycleLogger,
)
from makolet.application.ports import (
    MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
    MAXIMUM_HTTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
    MAXIMUM_SUPPORTED_ARCHIVE_OBJECT_BYTES,
    MAXIMUM_TRANSFER_CHUNK_BYTES,
    Clock,
    LeaseManager,
    MetricRecorder,
    SourceAdapter,
)
from makolet.domain.enums import (
    CompressionFormat,
    DocumentType,
    IngestionStatus,
    SourceProtocol,
)
from makolet.domain.errors import (
    ChargeBudgetExceededError,
    DiscoveryBudgetExceededError,
    LeaseUnavailableError,
    NotFoundError,
    SourceResponseError,
)
from makolet.domain.models import RemoteFile

type SourceFactory = Callable[[str], SourceAdapter]


class FileIngestion(Protocol):
    async def register(self, remote_file: RemoteFile) -> RegisteredSourceFile: ...

    async def ingest(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult: ...

    async def archive_only(
        self,
        remote_file: RemoteFile,
        *,
        maximum_charged_bytes: int | None = None,
    ) -> IngestionResult: ...

    async def replay(
        self,
        source_file_id: UUID,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> IngestionResult: ...


class CatalogBootstrap(Protocol):
    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]: ...


class CollectionRepository(Protocol):
    """Persistence boundary for retry-safe publisher traversal progress."""

    async def begin_attempt(self, scope: CollectionScope) -> CollectionAttempt: ...

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
    ) -> CollectionAttempt: ...

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
    ) -> None: ...

    async def observe_attempt(
        self,
        attempt_id: UUID,
        *,
        discovered_delta: int,
    ) -> None: ...

    async def is_terminal(self, remote_file: RemoteFile, *, archive_only: bool) -> bool: ...

    async def note_terminal(self, remote_file: RemoteFile, *, archive_only: bool) -> None: ...

    async def charge_budget(self, attempt_id: UUID) -> CollectionChargeBudget: ...

    async def reserve_transfer(
        self,
        attempt_id: UUID,
        source_file_id: UUID,
        remote_file: RemoteFile,
        content_length: int,
    ) -> CollectionChargeBudget: ...

    async def settle_transfer(
        self,
        attempt_id: UUID,
        source_file_id: UUID,
        remote_file: RemoteFile,
        transferred_bytes: int,
    ) -> CollectionChargeBudget: ...


@dataclass(frozen=True, slots=True)
class CollectionPolicy:
    discovery_page_size: int = 100
    maximum_listing_requests_per_source_run: int = DEFAULT_MAXIMUM_LISTING_REQUESTS_PER_SOURCE_RUN
    maximum_listing_bytes_per_source_run: int = DEFAULT_MAXIMUM_LISTING_BYTES_PER_SOURCE_RUN
    maximum_listing_elapsed_seconds_per_source_run: float = (
        DEFAULT_MAXIMUM_LISTING_ELAPSED_SECONDS_PER_SOURCE_RUN
    )
    maximum_files_per_source_run: int = 10_000
    maximum_discovery_records_per_source_run: int = 100_000
    maximum_reported_files: int = 100
    maximum_archive_object_bytes: int = 2 * 1024 * 1024 * 1024
    maximum_transfer_chunk_bytes: int = MAXIMUM_TRANSFER_CHUNK_BYTES
    maximum_transfer_protocol_overhead_bytes: int = MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES
    maximum_http_transfer_protocol_overhead_bytes: int = (
        MAXIMUM_HTTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES
    )
    maximum_charged_bytes_per_source_run: int = 8 * 1024 * 1024 * 1024
    maximum_charged_bytes_per_source_day: int = 32 * 1024 * 1024 * 1024
    maximum_source_identities_per_source_day: int = 2_000
    maximum_transfer_attempts_per_source_day: int = 4_000
    maximum_successes_per_source_day: int = 2_000
    source_lease_ttl: timedelta = timedelta(hours=24)

    def __post_init__(self) -> None:
        if not 1 <= self.discovery_page_size <= 500:
            raise ValueError("discovery_page_size must be between 1 and 500")
        DiscoveryRunBudget(
            maximum_requests=self.maximum_listing_requests_per_source_run,
            maximum_bytes=self.maximum_listing_bytes_per_source_run,
            maximum_elapsed_seconds=self.maximum_listing_elapsed_seconds_per_source_run,
        )
        if not 1 <= self.maximum_files_per_source_run <= 10_000:
            raise ValueError("maximum_files_per_source_run must be between 1 and 10,000")
        if not (
            self.maximum_files_per_source_run
            <= self.maximum_discovery_records_per_source_run
            <= 1_000_000
        ):
            raise ValueError(
                "maximum_discovery_records_per_source_run must be between the file "
                "limit and 1,000,000"
            )
        if not 1 <= self.maximum_reported_files <= self.maximum_files_per_source_run:
            raise ValueError("maximum_reported_files must be positive and bounded by the run")
        if not (
            1 <= self.maximum_archive_object_bytes <= MAXIMUM_SUPPORTED_ARCHIVE_OBJECT_BYTES
            and 1 <= self.maximum_transfer_chunk_bytes <= MAXIMUM_TRANSFER_CHUNK_BYTES
            and 0
            <= self.maximum_transfer_protocol_overhead_bytes
            <= MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES
            and 0
            <= self.maximum_http_transfer_protocol_overhead_bytes
            <= MAXIMUM_HTTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES
            and self.maximum_archive_object_bytes
            + self.maximum_transfer_chunk_bytes
            + max(
                self.maximum_transfer_protocol_overhead_bytes,
                self.maximum_http_transfer_protocol_overhead_bytes,
            )
            <= self.maximum_charged_bytes_per_source_run
            <= self.maximum_charged_bytes_per_source_day
            <= 16 * 1024 * 1024 * 1024 * 1024
        ):
            raise ValueError(
                "charged-byte budgets must cover one archive object plus protocol and "
                "transfer-frame headroom, be ordered by run then day, and be at most 16 TiB"
            )
        if not timedelta(0) < self.source_lease_ttl <= timedelta(hours=24):
            raise ValueError("source_lease_ttl must be positive and no longer than 24 hours")
        if not (
            1
            <= self.maximum_source_identities_per_source_day
            <= self.maximum_transfer_attempts_per_source_day
            <= 100_000
        ):
            raise ValueError("source identity and transfer-attempt limits are inconsistent")
        if not 1 <= self.maximum_successes_per_source_day <= 100_000:
            raise ValueError("source success limit must be between 1 and 100,000")


class CollectionOperations:
    """Discover and ingest a bounded source set without retaining whole file catalogs."""

    def __init__(
        self,
        source_factory: SourceFactory,
        source_ids: Sequence[str],
        ingestion: FileIngestion,
        *,
        batch_source_ids: Sequence[str] | None = None,
        policy: CollectionPolicy | None = None,
        metrics: MetricRecorder | None = None,
        clock: Clock | None = None,
        catalog_bootstrap: CatalogBootstrap | None = None,
        events: LifecycleLogger | None = None,
        repository: CollectionRepository | None = None,
        leases: LeaseManager | None = None,
        worker_id: str = "collection",
        source_portal_ids: Mapping[str, Sequence[str]] | None = None,
    ) -> None:
        if not source_ids or len(source_ids) != len(set(source_ids)):
            raise ValueError("source_ids must be a non-empty unique sequence")
        self._source_factory = source_factory
        self._source_ids = tuple(source_ids)
        self._source_id_set = frozenset(source_ids)
        selected_batch = (
            tuple(batch_source_ids) if batch_source_ids is not None else self._source_ids
        )
        if not selected_batch or not set(selected_batch).issubset(self._source_id_set):
            raise ValueError("batch_source_ids must be a non-empty subset of source_ids")
        self._batch_source_ids = selected_batch
        self._ingestion = ingestion
        self._policy = policy or CollectionPolicy()
        if (metrics is None) != (clock is None):
            raise ValueError("metrics and clock must be configured together")
        self._metrics = metrics
        self._clock = clock
        self._catalog_bootstrap = catalog_bootstrap
        self._events = events or NULL_LIFECYCLE_LOGGER
        self._repository = repository or _MemoryCollectionRepository()
        self._leases = leases or _NullLeaseManager()
        self._strict_source_scope = source_portal_ids is not None
        if not worker_id or len(worker_id) > 256:
            raise ValueError("collection worker_id must contain at most 256 characters")
        self._worker_id = worker_id
        configured_portals = source_portal_ids or {
            source_id: (source_id,) for source_id in self._source_ids
        }
        if set(configured_portals) != self._source_id_set:
            raise ValueError("source_portal_ids must define every collection source exactly once")
        self._source_portal_ids = {
            source_id: _portal_ids(configured_portals[source_id]) for source_id in self._source_ids
        }

    async def ingest_source(self, source_id: str) -> dict[str, object]:
        return await self._collect(source_id, since=None, until=None)

    async def ingest_retailer(self, retailer_id: str) -> dict[str, object]:
        return await self._collect(retailer_id, since=None, until=None)

    async def ingest_all(self) -> dict[str, object]:
        results = [
            await self._collect(source_id, since=None, until=None)
            for source_id in self._batch_source_ids
        ]
        return {
            "status": "completed",
            "sources": tuple(results),
            "source_count": len(results),
            "file_count": sum(cast(int, result["file_count"]) for result in results),
        }

    async def backfill(
        self,
        source_id: str,
        *,
        since: datetime,
        until: datetime,
        archive_only: bool = False,
    ) -> dict[str, object]:
        if since.tzinfo is None or since.utcoffset() is None:
            raise ValueError("since must include a timezone")
        if until.tzinfo is None or until.utcoffset() is None:
            raise ValueError("until must include a timezone")
        if since > until:
            raise ValueError("since must not be after until")
        return await self._collect(
            source_id,
            since=since,
            until=until,
            archive_only=archive_only,
        )

    async def replay(self, source_file_id: UUID) -> dict[str, object]:
        run_id = str(uuid4())
        with self._events.context_if_absent(
            correlation_id=run_id,
            run_id=run_id,
            source_file_id=source_file_id,
        ):
            result = await self._ingestion.replay(source_file_id)
            await self._bootstrap_catalog(result)
            return _ingestion_result(result)

    async def _bootstrap_catalog(self, result: IngestionResult) -> None:
        if self._catalog_bootstrap is not None and result.status is IngestionStatus.COMPLETED:
            await self._catalog_bootstrap.bootstrap_source_file(result.source_file_id)

    async def _collect(
        self,
        source_id: str,
        *,
        since: datetime | None,
        until: datetime | None,
        archive_only: bool = False,
    ) -> dict[str, object]:
        adapter = self._adapter(source_id)
        scope = CollectionScope(
            source_id=source_id,
            portal_ids=self._source_portal_ids[source_id],
            operation="ordinary" if since is None and until is None else "backfill",
            since=since,
            until=until,
            archive_only=archive_only,
        )
        run_id = str(uuid4())
        started = time.perf_counter()
        with self._events.context_if_absent(
            correlation_id=run_id,
            run_id=run_id,
            source_id=source_id,
        ):
            resource = f"collection-source:{source_id}"
            async with self._leases.acquire(
                resource,
                self._worker_id,
                self._policy.source_lease_ttl,
            ) as acquired:
                if not acquired:
                    raise LeaseUnavailableError(
                        "Another worker owns this source's discovery traversal"
                    )
                self._events.info(
                    LifecycleEvent.DISCOVERY_STARTED,
                    archive_only=archive_only,
                    status="running",
                )
                try:
                    attempt = await self._repository.begin_attempt(scope)
                    return await self._collect_with_context(
                        adapter,
                        scope=scope,
                        attempt=attempt,
                        started=started,
                    )
                except BaseException as error:
                    self._events.warning(
                        LifecycleEvent.DISCOVERY_FAILED,
                        archive_only=archive_only,
                        duration_seconds=time.perf_counter() - started,
                        error_code=_collection_error_code(error),
                        status="failed",
                    )
                    raise

    async def _collect_with_context(
        self,
        adapter: SourceAdapter,
        *,
        scope: CollectionScope,
        attempt: CollectionAttempt,
        started: float,
    ) -> dict[str, object]:
        cursor = DiscoveryCursor(attempt.cursor) if attempt.cursor is not None else None
        page_offset = attempt.page_offset
        seen_cursors: set[str] = set()
        discovered = ingested = duplicates = skipped_unknown = 0
        reported: list[dict[str, object]] = []
        freshest_success: dict[str, datetime] = {}
        run_truncated = False
        truncation_reason: str | None = None
        traversal_complete = False
        page_index = 0
        retry_boundary = attempt.retry_boundary
        postprocess_pending = False
        listing_budget = DiscoveryRunBudget(
            maximum_requests=self._policy.maximum_listing_requests_per_source_run,
            maximum_bytes=self._policy.maximum_listing_bytes_per_source_run,
            maximum_elapsed_seconds=(self._policy.maximum_listing_elapsed_seconds_per_source_run),
        )

        charged_bytes = 0

        async def process(
            remote_file: RemoteFile,
            *,
            source_file_id: UUID,
            maximum_charged_bytes: int | None,
        ) -> tuple[IngestionResult, CollectionChargeBudget]:
            nonlocal duplicates, ingested, postprocess_pending
            with self._events.context(
                portal_id=remote_file.portal_id,
                retailer_id=remote_file.retailer_id,
            ):
                result = (
                    await self._ingestion.archive_only(
                        remote_file,
                        maximum_charged_bytes=maximum_charged_bytes,
                    )
                    if scope.archive_only
                    else await self._ingestion.ingest(
                        remote_file,
                        maximum_charged_bytes=maximum_charged_bytes,
                    )
                )
            budget = await self._repository.settle_transfer(
                attempt.attempt_id,
                source_file_id,
                remote_file,
                result.transferred_bytes,
            )
            if not scope.archive_only:
                postprocess_pending = True
                await self._bootstrap_catalog(result)
                postprocess_pending = False
            ingested += 1
            duplicates += result.duplicate
            observed_at = remote_file.source_timestamp or (
                self._clock.now() if self._clock is not None else remote_file.discovered_at
            )
            current_freshest = freshest_success.get(remote_file.retailer_id)
            if current_freshest is None or observed_at > current_freshest:
                freshest_success[remote_file.retailer_id] = observed_at
            if len(reported) < self._policy.maximum_reported_files:
                reported.append(
                    {
                        "filename": remote_file.original_filename,
                        **_ingestion_result(result),
                    }
                )
            return result, budget

        def invalid_page(message: str) -> None:
            raise SourceResponseError(message)

        try:
            while True:
                page_index += 1
                page_start_cursor = cursor.value if cursor is not None else None
                try:
                    page = await _discover_page(
                        adapter,
                        cursor,
                        limit=self._policy.discovery_page_size,
                        budget=listing_budget,
                    )
                except DiscoveryBudgetExceededError as error:
                    run_truncated = True
                    truncation_reason = error.reason
                    break
                if page_offset > len(page.files):
                    invalid_page("Durable discovery offset is beyond the publisher page")
                if not page.files and page.next_cursor is not None:
                    invalid_page("Source returned an empty non-terminal discovery page")
                self._events.info(
                    LifecycleEvent.DISCOVERY_PAGE_COMPLETED,
                    complete=page.complete,
                    page_file_count=len(page.files) - page_offset,
                    page_index=page_index,
                    status="completed",
                )
                for index in range(page_offset, len(page.files)):
                    try:
                        listing_budget.checkpoint()
                    except DiscoveryBudgetExceededError as error:
                        run_truncated = True
                        truncation_reason = error.reason
                        break
                    if discovered >= self._policy.maximum_discovery_records_per_source_run:
                        run_truncated = True
                        truncation_reason = "discovery_limit"
                        break
                    remote_file = page.files[index]
                    if self._strict_source_scope:
                        _validate_remote_scope(remote_file, scope)
                    next_offset = index + 1
                    unknown = (
                        remote_file.document_type is DocumentType.UNKNOWN
                        or remote_file.compression is CompressionFormat.UNKNOWN
                    )
                    if unknown:
                        discovered += 1
                        skipped_unknown += 1
                        attempt = await self._repository.advance_attempt(
                            attempt.attempt_id,
                            expected_cursor=page_start_cursor,
                            expected_page_offset=index,
                            cursor=page_start_cursor,
                            page_offset=next_offset,
                            discovered_delta=1,
                            unknown_delta=1,
                        )
                        retry_boundary = False
                        continue
                    in_range = _within_range(
                        remote_file,
                        since=scope.since,
                        until=scope.until,
                    )
                    already_terminal = await self._repository.is_terminal(
                        remote_file,
                        archive_only=scope.archive_only,
                    )
                    if not in_range or (already_terminal and not retry_boundary):
                        discovered += 1
                        attempt = await self._repository.advance_attempt(
                            attempt.attempt_id,
                            expected_cursor=page_start_cursor,
                            expected_page_offset=index,
                            cursor=page_start_cursor,
                            page_offset=next_offset,
                            discovered_delta=1,
                            recognized_delta=1,
                        )
                        retry_boundary = False
                        continue
                    if ingested >= self._policy.maximum_files_per_source_run:
                        run_truncated = True
                        truncation_reason = "file_limit"
                        break
                    budget = await self._repository.charge_budget(attempt.attempt_id)
                    charged_bytes = budget.run_charged_bytes
                    budget_reason = self._charge_budget_reason(budget)
                    retrying_terminal = already_terminal and retry_boundary
                    if budget_reason is not None and not retrying_terminal:
                        run_truncated = True
                        truncation_reason = budget_reason
                        break
                    remaining_bytes = min(
                        self._policy.maximum_charged_bytes_per_source_run
                        - budget.run_charged_bytes,
                        self._policy.maximum_charged_bytes_per_source_day
                        - budget.day_charged_bytes,
                    )
                    transfer_frame_headroom = self._policy.maximum_transfer_chunk_bytes
                    protocol_overhead = (
                        self._policy.maximum_transfer_protocol_overhead_bytes
                        if remote_file.protocol in {SourceProtocol.FTP, SourceProtocol.FTPS}
                        else (
                            self._policy.maximum_http_transfer_protocol_overhead_bytes
                            if remote_file.protocol in {SourceProtocol.HTTP, SourceProtocol.HTTPS}
                            else 0
                        )
                    )
                    full_reservation = (
                        self._policy.maximum_archive_object_bytes
                        + protocol_overhead
                        + transfer_frame_headroom
                    )
                    if remaining_bytes <= transfer_frame_headroom and not retrying_terminal:
                        run_truncated = True
                        truncation_reason = (
                            "charged_byte_run_limit"
                            if (
                                self._policy.maximum_charged_bytes_per_source_run
                                - budget.run_charged_bytes
                            )
                            <= (
                                self._policy.maximum_charged_bytes_per_source_day
                                - budget.day_charged_bytes
                            )
                            else "charged_byte_day_limit"
                        )
                        break
                    registered = await self._ingestion.register(remote_file)
                    reserved_bytes = (
                        0
                        if retrying_terminal
                        else min(
                            remaining_bytes,
                            full_reservation,
                        )
                    )
                    budget = await self._repository.reserve_transfer(
                        attempt.attempt_id,
                        registered.source_file_id,
                        remote_file,
                        reserved_bytes,
                    )
                    charged_bytes = budget.run_charged_bytes
                    try:
                        ingestion_byte_limit = (
                            None if retrying_terminal else reserved_bytes - transfer_frame_headroom
                        )
                        result, budget = await process(
                            remote_file,
                            source_file_id=registered.source_file_id,
                            maximum_charged_bytes=ingestion_byte_limit,
                        )
                        await self._repository.note_terminal(
                            remote_file,
                            archive_only=scope.archive_only,
                        )
                        charged_bytes = budget.run_charged_bytes
                        postprocess_pending = False
                    except ChargeBudgetExceededError as error:
                        discovered += 1
                        budget = await self._repository.settle_transfer(
                            attempt.attempt_id,
                            registered.source_file_id,
                            remote_file,
                            error.transferred_bytes,
                        )
                        charged_bytes = budget.run_charged_bytes
                        await self._repository.observe_attempt(
                            attempt.attempt_id,
                            discovered_delta=1,
                        )
                        run_truncated = True
                        truncation_reason = (
                            "charged_byte_run_limit"
                            if (
                                self._policy.maximum_charged_bytes_per_source_run
                                - budget.run_charged_bytes
                            )
                            <= (
                                self._policy.maximum_charged_bytes_per_source_day
                                - budget.day_charged_bytes
                            )
                            else "charged_byte_day_limit"
                        )
                        break
                    except BaseException as error:
                        boundary_was_pending = postprocess_pending
                        postprocess_pending = True
                        transferred_bytes = getattr(error, "transferred_bytes", None)
                        if isinstance(transferred_bytes, int) and transferred_bytes >= 0:
                            budget = await self._repository.settle_transfer(
                                attempt.attempt_id,
                                registered.source_file_id,
                                remote_file,
                                transferred_bytes,
                            )
                        charged_bytes = budget.run_charged_bytes
                        postprocess_pending = boundary_was_pending
                        if not postprocess_pending and await self._repository.is_terminal(
                            remote_file,
                            archive_only=scope.archive_only,
                        ):
                            discovered += 1
                            attempt = await self._repository.advance_attempt(
                                attempt.attempt_id,
                                expected_cursor=page_start_cursor,
                                expected_page_offset=index,
                                cursor=page_start_cursor,
                                page_offset=next_offset,
                                discovered_delta=1,
                                recognized_delta=1,
                                processed_delta=1,
                            )
                        else:
                            discovered += 1
                            await self._repository.observe_attempt(
                                attempt.attempt_id,
                                discovered_delta=1,
                            )
                        raise
                    discovered += 1
                    attempt = await self._repository.advance_attempt(
                        attempt.attempt_id,
                        expected_cursor=page_start_cursor,
                        expected_page_offset=index,
                        cursor=page_start_cursor,
                        page_offset=next_offset,
                        discovered_delta=1,
                        recognized_delta=1,
                        processed_delta=1,
                        duplicate_delta=int(result.duplicate),
                    )
                    retry_boundary = False
                    budget_reason = self._charge_budget_reason(budget)
                    if budget_reason is not None:
                        run_truncated = True
                        truncation_reason = budget_reason
                        break
                if run_truncated:
                    break
                if page.next_cursor is None:
                    if not page.complete:
                        invalid_page(
                            "Source marked discovery incomplete without a continuation cursor"
                        )
                    attempt = await self._repository.advance_attempt(
                        attempt.attempt_id,
                        expected_cursor=page_start_cursor,
                        expected_page_offset=len(page.files),
                        cursor=None,
                        page_offset=0,
                    )
                    traversal_complete = True
                    break
                value = page.next_cursor.value
                if value is None or value == page_start_cursor or value in seen_cursors:
                    invalid_page("Source repeated or omitted its discovery cursor")
                selected_cursor = _publisher_cursor(cast(str, value))
                seen_cursors.add(selected_cursor)
                attempt = await self._repository.advance_attempt(
                    attempt.attempt_id,
                    expected_cursor=page_start_cursor,
                    expected_page_offset=len(page.files),
                    cursor=selected_cursor,
                    page_offset=0,
                )
                cursor = DiscoveryCursor(selected_cursor)
                page_offset = 0
                if (
                    ingested >= self._policy.maximum_files_per_source_run
                    or discovered >= self._policy.maximum_discovery_records_per_source_run
                ):
                    run_truncated = True
                    truncation_reason = (
                        "file_limit"
                        if ingested >= self._policy.maximum_files_per_source_run
                        else "discovery_limit"
                    )
                    break
            if (
                traversal_complete
                and attempt.generation_recognized_count == 0
                and attempt.generation_unknown_count > 0
            ):
                invalid_page("Non-empty source listing contained only unknown file formats")
        except BaseException as error:
            await asyncio.shield(
                self._repository.finish_attempt(
                    attempt.attempt_id,
                    status="failed",
                    truncated=False,
                    traversal_complete=traversal_complete,
                    truncation_reason=None,
                    error_code=_collection_error_code(error),
                    error_message=_safe_collection_error_message(error),
                )
            )
            raise

        await self._repository.finish_attempt(
            attempt.attempt_id,
            status="bounded" if run_truncated else "completed",
            truncated=run_truncated,
            traversal_complete=traversal_complete,
            truncation_reason=_durable_truncation_reason(truncation_reason),
        )
        if self._metrics is not None:
            for retailer_id, observed_at in freshest_success.items():
                self._metrics.set_gauge(
                    "source_freshness_timestamp_seconds",
                    observed_at.timestamp(),
                    labels={"source": scope.source_id, "retailer": retailer_id},
                )
        collection_result: dict[str, object] = {
            "status": "bounded" if run_truncated else "completed",
            "source_id": scope.source_id,
            "file_count": ingested,
            "discovered_count": discovered,
            "duplicate_count": duplicates,
            "skipped_unknown_count": skipped_unknown,
            "reported_files": tuple(reported),
            "report_truncated": ingested > len(reported),
            "run_truncated": run_truncated,
            "truncation_reason": truncation_reason,
            "listing_request_count": listing_budget.request_count,
            "listing_bytes": listing_budget.consumed_bytes,
            "charged_bytes": charged_bytes,
            "since": scope.since,
            "until": scope.until,
            "archive_only": scope.archive_only,
            "collection_attempt_id": attempt.attempt_id,
            "collection_generation": attempt.generation,
        }
        self._events.info(
            LifecycleEvent.DISCOVERY_COMPLETED,
            archive_only=scope.archive_only,
            discovered_count=discovered,
            duplicate_count=duplicates,
            duration_seconds=time.perf_counter() - started,
            file_count=ingested,
            reported_count=len(reported),
            skipped_unknown_count=skipped_unknown,
            status="bounded" if run_truncated else "completed",
            truncated=run_truncated,
        )
        return collection_result

    def _charge_budget_reason(self, budget: CollectionChargeBudget) -> str | None:
        if budget.run_charged_bytes >= self._policy.maximum_charged_bytes_per_source_run:
            return "charged_byte_run_limit"
        if budget.day_charged_bytes >= self._policy.maximum_charged_bytes_per_source_day:
            return "charged_byte_day_limit"
        if budget.day_source_identities >= self._policy.maximum_source_identities_per_source_day:
            return "identity_day_limit"
        if budget.day_transfer_attempts >= self._policy.maximum_transfer_attempts_per_source_day:
            return "attempt_day_limit"
        if budget.day_successes >= self._policy.maximum_successes_per_source_day:
            return "success_day_limit"
        return None

    def _adapter(self, source_id: str) -> SourceAdapter:
        if source_id not in self._source_id_set:
            raise NotFoundError("Source was not found or is not enabled for ingestion")
        try:
            return self._source_factory(source_id)
        except KeyError as error:
            raise NotFoundError("Source was not found") from error


async def _discover_page(
    adapter: SourceAdapter,
    cursor: DiscoveryCursor | None,
    *,
    limit: int,
    budget: DiscoveryRunBudget,
) -> DiscoveryPage:
    budget.checkpoint()
    timeout_scope = asyncio.timeout(budget.remaining_elapsed_seconds)
    try:
        async with timeout_scope:
            page = await adapter.discover(cursor, limit=limit, budget=budget)
    except TimeoutError:
        if timeout_scope.expired():
            raise DiscoveryBudgetExceededError("listing_elapsed_limit") from None
        raise
    budget.checkpoint()
    return page


def _durable_truncation_reason(reason: str | None) -> str | None:
    if reason in {
        "listing_request_limit",
        "listing_byte_limit",
        "listing_elapsed_limit",
    }:
        # The existing durable taxonomy treats all publisher-discovery work caps
        # as discovery limits; the immediate result retains the precise cause.
        return "discovery_limit"
    return reason


def _within_range(
    remote_file: RemoteFile,
    *,
    since: datetime | None,
    until: datetime | None,
) -> bool:
    if since is None and until is None:
        return True
    timestamp = remote_file.source_timestamp
    if timestamp is None:
        return False
    return (since is None or timestamp >= since) and (until is None or timestamp <= until)


def _ingestion_result(result: IngestionResult) -> dict[str, object]:
    return {
        "source_file_id": result.source_file_id,
        "status": result.status.value,
        "content_sha256": result.content_sha256,
        "duplicate": result.duplicate,
        "replayed": result.replayed,
        "stage": asdict(result.stage) if result.stage is not None else None,
        "apply": asdict(result.apply) if result.apply is not None else None,
    }


def _collection_error_code(error: BaseException) -> str:
    if isinstance(error, asyncio.CancelledError):
        return "operation_cancelled"
    code = getattr(error, "code", None)
    return code if isinstance(code, str) and 0 < len(code) <= 128 else "unexpected_error"


def _safe_collection_error_message(error: BaseException) -> str:
    if isinstance(error, asyncio.CancelledError):
        return "Collection was interrupted before its next retry-safe boundary"
    return "Collection failed; inspect operator logs using the collection attempt identifier"


def _portal_ids(values: Sequence[str]) -> tuple[str, ...]:
    selected = tuple(sorted(set(values)))
    if not selected or len(selected) != len(values):
        raise ValueError("collection portal IDs must be non-empty and unique")
    if any(not value or len(value) > 256 for value in selected):
        raise ValueError("collection portal IDs must contain at most 256 characters")
    return selected


def _publisher_cursor(value: str) -> str:
    if not 1 <= len(value.encode("utf-8")) <= 8_192:
        raise SourceResponseError("Source discovery cursor is empty or exceeds 8192 bytes")
    return value


def _validate_remote_scope(remote_file: RemoteFile, scope: CollectionScope) -> None:
    if remote_file.retailer_id != scope.source_id or remote_file.portal_id not in scope.portal_ids:
        raise SourceResponseError(
            "Source returned a file outside its durable retailer and portal generation"
        )


@dataclass(slots=True)
class _MemoryCheckpoint:
    checkpoint_id: UUID
    generation: int = 1
    cursor: str | None = None
    page_offset: int = 0
    recognized_count: int = 0
    unknown_count: int = 0
    complete: bool = False
    retry_boundary: bool = False


class _MemoryCollectionRepository:
    """Process-local fallback for tests; production composition uses PostgreSQL."""

    def __init__(self) -> None:
        self._checkpoints: dict[CollectionScope, _MemoryCheckpoint] = {}
        self._attempts: dict[UUID, CollectionAttempt] = {}
        self._running: set[UUID] = set()
        self._terminal: set[tuple[str, str, str, bool]] = set()
        self._attempt_sources: dict[UUID, str] = {}
        self._attempt_charged_bytes: dict[UUID, int] = {}
        self._source_charged_bytes: dict[str, int] = {}
        self._charged_sources: set[tuple[str, str, str]] = set()
        self._charged_transfers: dict[tuple[UUID, UUID], tuple[int, bool]] = {}
        self._source_identity_files: dict[str, set[UUID]] = {}
        self._source_attempt_counts: dict[str, int] = {}
        self._source_success_counts: dict[str, int] = {}

    async def begin_attempt(self, scope: CollectionScope) -> CollectionAttempt:
        checkpoint = self._checkpoints.setdefault(
            scope,
            _MemoryCheckpoint(checkpoint_id=uuid4()),
        )
        if checkpoint.complete:
            checkpoint.generation += 1
            checkpoint.cursor = None
            checkpoint.page_offset = 0
            checkpoint.recognized_count = 0
            checkpoint.unknown_count = 0
            checkpoint.complete = False
            checkpoint.retry_boundary = False
        attempt = CollectionAttempt(
            attempt_id=uuid4(),
            checkpoint_id=checkpoint.checkpoint_id,
            generation=checkpoint.generation,
            cursor=checkpoint.cursor,
            page_offset=checkpoint.page_offset,
            generation_recognized_count=checkpoint.recognized_count,
            generation_unknown_count=checkpoint.unknown_count,
            retry_boundary=checkpoint.retry_boundary,
        )
        self._attempts[attempt.attempt_id] = attempt
        self._running.add(attempt.attempt_id)
        self._attempt_sources[attempt.attempt_id] = scope.source_id
        self._attempt_charged_bytes[attempt.attempt_id] = 0
        return attempt

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
        del discovered_delta, processed_delta, duplicate_delta
        if attempt_id not in self._running:
            raise RuntimeError("collection attempt is not running")
        attempt = self._attempts[attempt_id]
        checkpoint = next(
            value
            for value in self._checkpoints.values()
            if value.checkpoint_id == attempt.checkpoint_id
        )
        if checkpoint.cursor != expected_cursor or checkpoint.page_offset != expected_page_offset:
            raise RuntimeError("collection checkpoint changed unexpectedly")
        checkpoint.cursor = cursor
        checkpoint.page_offset = page_offset
        checkpoint.recognized_count += recognized_delta
        checkpoint.unknown_count += unknown_delta
        checkpoint.retry_boundary = False
        updated = replace(
            attempt,
            cursor=cursor,
            page_offset=page_offset,
            generation_recognized_count=checkpoint.recognized_count,
            generation_unknown_count=checkpoint.unknown_count,
        )
        self._attempts[attempt_id] = updated
        return updated

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
        del truncated, truncation_reason, error_code, error_message
        self._running.discard(attempt_id)
        attempt = self._attempts[attempt_id]
        checkpoint = next(
            value
            for value in self._checkpoints.values()
            if value.checkpoint_id == attempt.checkpoint_id
        )
        checkpoint.complete = traversal_complete
        checkpoint.retry_boundary = status == "failed" and not traversal_complete

    async def observe_attempt(
        self,
        attempt_id: UUID,
        *,
        discovered_delta: int,
    ) -> None:
        if discovered_delta < 0 or attempt_id not in self._running:
            raise RuntimeError("collection attempt observation is invalid")

    async def is_terminal(self, remote_file: RemoteFile, *, archive_only: bool) -> bool:
        identity = (
            remote_file.retailer_id,
            remote_file.portal_id,
            remote_file.remote_id,
        )
        return (*identity, False) in self._terminal or (
            archive_only and (*identity, True) in self._terminal
        )

    async def note_terminal(self, remote_file: RemoteFile, *, archive_only: bool) -> None:
        self._terminal.add(
            (
                remote_file.retailer_id,
                remote_file.portal_id,
                remote_file.remote_id,
                archive_only,
            )
        )

    async def charge_budget(self, attempt_id: UUID) -> CollectionChargeBudget:
        if attempt_id not in self._running:
            raise RuntimeError("collection attempt is not running")
        source_id = self._attempt_sources[attempt_id]
        return CollectionChargeBudget(
            run_charged_bytes=self._attempt_charged_bytes[attempt_id],
            day_charged_bytes=self._source_charged_bytes.get(source_id, 0),
            day_source_identities=len(self._source_identity_files.get(source_id, set())),
            day_transfer_attempts=self._source_attempt_counts.get(source_id, 0),
            day_successes=self._source_success_counts.get(source_id, 0),
        )

    async def reserve_transfer(
        self,
        attempt_id: UUID,
        source_file_id: UUID,
        remote_file: RemoteFile,
        content_length: int,
    ) -> CollectionChargeBudget:
        budget = await self.charge_budget(attempt_id)
        if content_length < 0:
            raise ValueError("reserved transfer bytes cannot be negative")
        identity = (attempt_id, source_file_id)
        if identity in self._charged_transfers:
            return budget
        self._charged_transfers[identity] = (content_length, False)
        source_id = self._attempt_sources[attempt_id]
        self._source_identity_files.setdefault(source_id, set()).add(source_file_id)
        self._source_attempt_counts[source_id] = self._source_attempt_counts.get(source_id, 0) + 1
        self._attempt_charged_bytes[attempt_id] += content_length
        self._source_charged_bytes[source_id] = (
            self._source_charged_bytes.get(source_id, 0) + content_length
        )
        return await self.charge_budget(attempt_id)

    async def settle_transfer(
        self,
        attempt_id: UUID,
        source_file_id: UUID,
        remote_file: RemoteFile,
        transferred_bytes: int,
    ) -> CollectionChargeBudget:
        budget = await self.charge_budget(attempt_id)
        if transferred_bytes < 0:
            raise ValueError("transferred bytes cannot be negative")
        identity = (attempt_id, source_file_id)
        reserved_bytes, settled = self._charged_transfers[identity]
        if settled:
            return budget
        source_id = self._attempt_sources[attempt_id]
        archive_identity = (
            remote_file.retailer_id,
            remote_file.portal_id,
            remote_file.remote_id,
        )
        archive_attached = (
            (*archive_identity, False) in self._terminal
            or (*archive_identity, True) in self._terminal
            or (
                remote_file.content_length is not None
                and transferred_bytes >= remote_file.content_length
            )
        )
        archive_bytes = remote_file.content_length or transferred_bytes if archive_attached else 0
        new_archive_charge = archive_attached and archive_identity not in self._charged_sources
        archive_charge = archive_bytes if new_archive_charge else 0
        transfer_bytes = (
            max(0, transferred_bytes - archive_bytes) if archive_attached else transferred_bytes
        )
        exact_attempt_charge = transfer_bytes + archive_charge
        if exact_attempt_charge > reserved_bytes:
            raise RuntimeError("Collection transfer charge exceeds its reservation")
        if new_archive_charge:
            self._charged_sources.add(archive_identity)
            self._source_success_counts[source_id] = (
                self._source_success_counts.get(source_id, 0) + 1
            )
        self._charged_transfers[identity] = (transfer_bytes, True)
        delta = exact_attempt_charge - reserved_bytes
        self._attempt_charged_bytes[attempt_id] += delta
        self._source_charged_bytes[source_id] = self._source_charged_bytes.get(source_id, 0) + delta
        return await self.charge_budget(attempt_id)


class _NullLeaseManager:
    @asynccontextmanager
    async def acquire(
        self,
        resource: str,
        owner: str,
        ttl: timedelta,
    ) -> AsyncIterator[bool]:
        del resource, owner, ttl
        yield True


__all__ = ["CollectionOperations", "CollectionPolicy", "CollectionRepository"]
