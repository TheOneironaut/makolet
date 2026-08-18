"""Application-layer command and result values."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final
from uuid import UUID

from makolet.domain.enums import IngestionStatus, SourceProtocol
from makolet.domain.errors import DiscoveryBudgetExceededError
from makolet.domain.models import ArchiveReceipt, RemoteFile, ValidationIssue

MAXIMUM_FRESHNESS_ITEMS_PER_STORE: Final = 1_000
DEFAULT_MAXIMUM_LISTING_REQUESTS_PER_SOURCE_RUN: Final = 256
DEFAULT_MAXIMUM_LISTING_BYTES_PER_SOURCE_RUN: Final = 8 * 1024 * 1024
DEFAULT_MAXIMUM_LISTING_ELAPSED_SECONDS_PER_SOURCE_RUN: Final = 300.0
DEFAULT_MAXIMUM_VALIDATION_ISSUES_PER_ATTEMPT: Final = 100_000
DEFAULT_MAXIMUM_VALIDATION_ISSUE_BYTES_PER_ATTEMPT: Final = 64 * 1024 * 1024
DEFAULT_MAXIMUM_VALIDATION_ISSUE_EVIDENCE_PER_ATTEMPT: Final = 1_000


def validation_issue_charge(issue: ValidationIssue) -> int:
    """Exact logical UTF-8 evidence charge shared by staging implementations."""

    values = (issue.code, issue.message, issue.field_name, issue.rejected_value)
    return 64 + sum(len(value.encode("utf-8")) for value in values if value is not None)


@dataclass(frozen=True, slots=True)
class DiscoveryCursor:
    """Opaque incremental-discovery checkpoint owned by one source adapter."""

    value: str | None = None


@dataclass(frozen=True, slots=True)
class DiscoveryPage:
    files: tuple[RemoteFile, ...]
    next_cursor: DiscoveryCursor | None
    complete: bool


@dataclass(slots=True)
class DiscoveryRunBudget:
    """One explicit cumulative request, byte, and elapsed-work budget."""

    maximum_requests: int = DEFAULT_MAXIMUM_LISTING_REQUESTS_PER_SOURCE_RUN
    maximum_bytes: int = DEFAULT_MAXIMUM_LISTING_BYTES_PER_SOURCE_RUN
    maximum_elapsed_seconds: float = DEFAULT_MAXIMUM_LISTING_ELAPSED_SECONDS_PER_SOURCE_RUN
    request_count: int = field(default=0, init=False)
    consumed_bytes: int = field(default=0, init=False)
    _started_at: float = field(default_factory=time.monotonic, init=False, repr=False)

    def __post_init__(self) -> None:
        if not 1 <= self.maximum_requests <= 1_000_000:
            raise ValueError("maximum discovery requests must be between 1 and 1,000,000")
        if not 1 <= self.maximum_bytes <= 1024 * 1024 * 1024:
            raise ValueError("maximum discovery bytes must be between 1 byte and 1 GiB")
        if not 0 < self.maximum_elapsed_seconds <= 86_400:
            raise ValueError("maximum discovery elapsed time must be positive and at most one day")

    @property
    def remaining_bytes(self) -> int:
        return self.maximum_bytes - self.consumed_bytes

    @property
    def remaining_elapsed_seconds(self) -> float:
        return max(0.0, self.maximum_elapsed_seconds - (time.monotonic() - self._started_at))

    @property
    def elapsed_exhausted(self) -> bool:
        return self.remaining_elapsed_seconds <= 0

    def begin_request(self, *, minimum_bytes: int = 0) -> None:
        if minimum_bytes < 0:
            raise ValueError("minimum discovery request charge cannot be negative")
        self.checkpoint()
        if self.request_count >= self.maximum_requests:
            raise DiscoveryBudgetExceededError("listing_request_limit")
        if minimum_bytes > self.remaining_bytes or self.remaining_bytes <= 0:
            raise DiscoveryBudgetExceededError("listing_byte_limit")
        self.request_count += 1

    def consume_bytes(self, byte_count: int) -> None:
        if byte_count < 0:
            raise ValueError("discovery byte charge cannot be negative")
        self.checkpoint()
        if byte_count > self.remaining_bytes:
            raise DiscoveryBudgetExceededError("listing_byte_limit")
        self.consumed_bytes += byte_count

    def checkpoint(self) -> None:
        if self.elapsed_exhausted:
            raise DiscoveryBudgetExceededError("listing_elapsed_limit")


@dataclass(frozen=True, slots=True)
class CollectionScope:
    """Durable traversal identity for one bounded publisher collection."""

    source_id: str
    portal_ids: tuple[str, ...]
    operation: str
    since: datetime | None = None
    until: datetime | None = None
    archive_only: bool = False

    def __post_init__(self) -> None:
        if not self.source_id or len(self.source_id) > 128:
            raise ValueError("collection source_id must contain at most 128 characters")
        if (
            not self.portal_ids
            or tuple(sorted(set(self.portal_ids))) != self.portal_ids
            or any(not portal_id or len(portal_id) > 256 for portal_id in self.portal_ids)
        ):
            raise ValueError("collection portal_ids must be a sorted, unique non-empty tuple")
        if self.operation not in {"ordinary", "backfill"}:
            raise ValueError("collection operation must be ordinary or backfill")
        if (self.since is None) != (self.until is None):
            raise ValueError("collection range endpoints must both be set or both be absent")
        for name, value in (("since", self.since), ("until", self.until)):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError(f"collection {name} must include a timezone")
        if self.since is not None and self.until is not None and self.since > self.until:
            raise ValueError("collection since must not be after until")
        if self.operation == "ordinary" and (
            self.since is not None or self.until is not None or self.archive_only
        ):
            raise ValueError("ordinary collection cannot carry a range or archive-only mode")

    @property
    def portal_generation(self) -> str:
        """Fingerprint configuration so a cursor never crosses portal generations."""

        payload = json.dumps(
            self.portal_ids,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class CollectionAttempt:
    """One durable attempt and its currently committed traversal boundary."""

    attempt_id: UUID
    checkpoint_id: UUID
    generation: int
    cursor: str | None
    page_offset: int
    generation_recognized_count: int = 0
    generation_unknown_count: int = 0
    retry_boundary: bool = False


@dataclass(frozen=True, slots=True)
class CollectionChargeBudget:
    """Archive and transfer charges for one attempt and its source's 24-hour window."""

    run_charged_bytes: int
    day_charged_bytes: int
    day_source_identities: int = 0
    day_transfer_attempts: int = 0
    day_successes: int = 0

    def __post_init__(self) -> None:
        if (
            min(
                self.run_charged_bytes,
                self.day_charged_bytes,
                self.day_source_identities,
                self.day_transfer_attempts,
                self.day_successes,
            )
            < 0
        ):
            raise ValueError("collection budget counts cannot be negative")


@dataclass(frozen=True, slots=True)
class RegisteredSourceFile:
    """Durable identity assigned to discovered source metadata."""

    source_file_id: UUID
    remote_file: RemoteFile
    status: IngestionStatus
    already_registered: bool
    completed_content_sha256: str | None = None
    archive_object_key: str | None = None
    content_sha256: str | None = None
    archive_content_length: int | None = None


@dataclass(frozen=True, slots=True)
class DownloadEvidence:
    """Secret-scrubbed transport evidence captured around exact response bytes."""

    started_at: datetime
    finished_at: datetime
    status_code: int | None
    content_length: int
    media_type: str | None
    etag: str | None
    last_modified: datetime | None
    response_metadata: tuple[tuple[str, str], ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class ArchivedDownload:
    archive: ArchiveReceipt
    evidence: DownloadEvidence
    transferred_bytes: int = 0

    def __post_init__(self) -> None:
        if self.transferred_bytes < 0:
            raise ValueError("transferred_bytes cannot be negative")


@dataclass(frozen=True, slots=True)
class StageSummary:
    metadata_records: int = 0
    store_records: int = 0
    price_records: int = 0
    promotion_records: int = 0
    warnings: int = 0
    rejected_records: int = 0
    file_quarantines: int = 0
    validation_issue_bytes: int = 0
    sampled_validation_issues: int = 0

    @property
    def accepted_records(self) -> int:
        return self.store_records + self.price_records + self.promotion_records


@dataclass(frozen=True, slots=True)
class ApplySummary:
    inserted: int
    updated: int
    unchanged: int
    unavailable: int
    history_events: int


@dataclass(frozen=True, slots=True)
class IngestionResult:
    source_file_id: UUID
    status: IngestionStatus
    content_sha256: str | None
    stage: StageSummary | None
    apply: ApplySummary | None
    duplicate: bool = False
    replayed: bool = False
    transferred_bytes: int = 0

    def __post_init__(self) -> None:
        if self.transferred_bytes < 0:
            raise ValueError("transferred_bytes cannot be negative")


@dataclass(frozen=True, slots=True)
class ReplayAttempt:
    replay_id: UUID
    source_file_id: UUID
    started_at: datetime


@dataclass(frozen=True, slots=True)
class ArchivedSourceFile:
    """One immutable source file selected in deterministic archive order."""

    source_file_id: UUID
    archived_at: datetime


@dataclass(frozen=True, slots=True)
class ArchivedSourceFilePage:
    files: tuple[ArchivedSourceFile, ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class ReplayRangeResult:
    since: datetime
    until: datetime
    files: tuple[IngestionResult, ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class NormalizedRebuildRun:
    """Durable progress for a full raw-derived normalized-state rebuild."""

    rebuild_run_id: UUID
    status: str
    archive_cutoff_at: datetime
    source_files_total: int
    source_files_completed: int
    last_sequence: int | None = None
    last_source_file_id: UUID | None = None
    last_archived_at: datetime | None = None
    started_at: datetime | None = None
    updated_at: datetime | None = None
    finished_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class PageRequest:
    limit: int = 50
    cursor: str | None = None


@dataclass(frozen=True, slots=True)
class Page:
    items: tuple[dict[str, Any], ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class CatalogCandidateGenerationResult:
    """One bounded keyset page of isolated-item candidate generation."""

    processed_items: int
    bootstrapped_items: int
    candidates_written: int
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class RetailerRegistration:
    source_key: str
    legal_name: str
    display_name: str
    edi: str | None
    is_active: bool


@dataclass(frozen=True, slots=True)
class PortalRegistration:
    retailer_source_key: str
    source_key: str
    family: str
    protocol: SourceProtocol
    base_url: str
    is_active: bool
