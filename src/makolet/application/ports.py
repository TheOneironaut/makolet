"""Narrow structural ports at I/O and transaction boundaries."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol
from uuid import UUID

from makolet.application.models import (
    ApplySummary,
    ArchivedDownload,
    ArchivedSourceFile,
    ArchivedSourceFilePage,
    CatalogCandidateGenerationResult,
    DiscoveryCursor,
    DiscoveryPage,
    DiscoveryRunBudget,
    DownloadEvidence,
    NormalizedRebuildRun,
    Page,
    RegisteredSourceFile,
    ReplayAttempt,
    StageSummary,
)
from makolet.domain.enums import CompressionFormat, DocumentType, IngestionStatus
from makolet.domain.models import ParsedEvent, RemoteFile

MAXIMUM_TRANSFER_CHUNK_BYTES = 64 * 1024
MAXIMUM_FTP_RESOLVED_ADDRESSES = 4
FTP_CONTROL_BYTES_PER_ATTEMPT = 128 * 1024
FTP_TLS13_NORMAL_RECORD_OVERHEAD_BYTES = 22
FTP_TLS_APPLICATION_DATA_BYTES_PER_RECORD = 16 * 1024
# FTP/FTPS control replies remain bounded. TLS framing for a supported 16 GiB
# object is reserved separately so ciphertext can be charged below the TLS layer.
MAXIMUM_TRANSFER_PROTOCOL_OVERHEAD_BYTES = 256 * 1024
# HTTP permits four logical hops. Each hop reserves 64 KiB for DNS plus four
# vetted-address attempts at 128 KiB each. TLS 1.3 adds 22 bytes to each normal
# 16 KiB application-data record; reserve that framing through the supported
# 16 GiB object ceiling. Pathological padding/fragmentation remains bounded by
# the wire meter. Production may retry four independently metered opens.
HTTP_CONTROL_BYTES_PER_ATTEMPT = 128 * 1024
HTTP_DNS_CONTROL_BYTES_PER_LOOKUP = 64 * 1024
MAXIMUM_HTTP_RESOLVED_ADDRESSES = MAXIMUM_FTP_RESOLVED_ADDRESSES
MAXIMUM_HTTP_REDIRECTS = 3
MAXIMUM_HTTP_LOGICAL_HOPS = MAXIMUM_HTTP_REDIRECTS + 1
MAXIMUM_HTTP_TLS_APPLICATION_DATA_BYTES_PER_RECORD = 16 * 1024
MAXIMUM_HTTP_TLS13_NORMAL_RECORD_OVERHEAD_BYTES = 22
MAXIMUM_SUPPORTED_ARCHIVE_OBJECT_BYTES = 16 * 1024 * 1024 * 1024
MAXIMUM_HTTP_TLS13_FRAMING_BYTES_PER_OPEN = (
    (
        MAXIMUM_SUPPORTED_ARCHIVE_OBJECT_BYTES
        + MAXIMUM_HTTP_TLS_APPLICATION_DATA_BYTES_PER_RECORD
        - 1
    )
    // MAXIMUM_HTTP_TLS_APPLICATION_DATA_BYTES_PER_RECORD
) * MAXIMUM_HTTP_TLS13_NORMAL_RECORD_OVERHEAD_BYTES
MAXIMUM_HTTP_CONTROL_BYTES_PER_OPEN = (
    HTTP_CONTROL_BYTES_PER_ATTEMPT * MAXIMUM_HTTP_RESOLVED_ADDRESSES * MAXIMUM_HTTP_LOGICAL_HOPS
    + HTTP_DNS_CONTROL_BYTES_PER_LOOKUP * MAXIMUM_HTTP_LOGICAL_HOPS
)
MAXIMUM_HTTP_TRANSFER_OVERHEAD_BYTES_PER_OPEN = (
    MAXIMUM_HTTP_CONTROL_BYTES_PER_OPEN + MAXIMUM_HTTP_TLS13_FRAMING_BYTES_PER_OPEN
)
MAXIMUM_HTTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES = (
    MAXIMUM_HTTP_TRANSFER_OVERHEAD_BYTES_PER_OPEN * MAXIMUM_HTTP_LOGICAL_HOPS
)
MAXIMUM_FTP_TLS13_FRAMING_BYTES_PER_OPEN = (
    (MAXIMUM_SUPPORTED_ARCHIVE_OBJECT_BYTES + FTP_TLS_APPLICATION_DATA_BYTES_PER_RECORD - 1)
    // FTP_TLS_APPLICATION_DATA_BYTES_PER_RECORD
) * FTP_TLS13_NORMAL_RECORD_OVERHEAD_BYTES
MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES = (
    MAXIMUM_TRANSFER_PROTOCOL_OVERHEAD_BYTES
    + FTP_CONTROL_BYTES_PER_ATTEMPT * MAXIMUM_FTP_RESOLVED_ADDRESSES
    + MAXIMUM_FTP_TLS13_FRAMING_BYTES_PER_OPEN
)
MAXIMUM_TRANSFER_RESERVATION_HEADROOM_BYTES = MAXIMUM_TRANSFER_CHUNK_BYTES + max(
    MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
    MAXIMUM_HTTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
)


class Clock(Protocol):
    def now(self) -> datetime: ...


class SourceAdapter(Protocol):
    """Discover public files; downloading is intentionally a separate port."""

    source_id: str

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: DiscoveryRunBudget | None = None,
    ) -> DiscoveryPage: ...


class DownloadSession(Protocol):
    """One validated transport response whose chunks are exact wire payload bytes."""

    @property
    def transferred_bytes(self) -> int:
        """Return network bytes incurred so far for this transport session."""
        ...

    def iter_raw(self) -> AsyncIterator[bytes]: ...

    async def finish(self, content_length: int) -> DownloadEvidence: ...


class Downloader(Protocol):
    def open(
        self,
        remote_file: RemoteFile,
        *,
        maximum_bytes: int | None = None,
    ) -> AbstractAsyncContextManager[DownloadSession]: ...


class RawArchive(Protocol):
    """Immutable content-addressed source-byte storage."""

    async def put(
        self,
        chunks: AsyncIterator[bytes],
        *,
        original_filename: str,
    ) -> tuple[str, int, bool]: ...

    def open(self, object_key: str) -> AbstractAsyncContextManager[AsyncIterator[bytes]]: ...

    async def exists(self, object_key: str) -> bool: ...

    async def verify(self, object_key: str, expected_sha256: str) -> int: ...


class DocumentParser(Protocol):
    parser_version: str

    def parse(
        self,
        chunks: AsyncIterator[bytes],
        *,
        source_file_id: UUID,
        document_type: DocumentType,
        compression: CompressionFormat,
        filename: str,
    ) -> AsyncIterator[ParsedEvent]: ...


class IngestionRepository(Protocol):
    """Transactional ingestion state and bulk staging/apply operations."""

    async def assert_ingestion_allowed(
        self,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> None: ...

    async def register_discovery(
        self,
        remote_file: RemoteFile,
        *,
        owned_refresh: bool = False,
    ) -> RegisteredSourceFile: ...

    async def get(self, source_file_id: UUID) -> RegisteredSourceFile: ...

    async def transition(
        self,
        source_file_id: UUID,
        expected: Sequence[IngestionStatus],
        target: IngestionStatus,
        *,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None: ...

    async def record_archive(
        self,
        source_file_id: UUID,
        archived: ArchivedDownload,
        *,
        parser_version: str,
    ) -> bool:
        """Attach archive evidence; return true when the immutable raw object was reused."""
        ...

    async def clear_staging(self, source_file_id: UUID) -> None: ...

    async def reuse_validated_staging(
        self,
        source_file_id: UUID,
        *,
        parser_version: str,
        document_type: DocumentType,
        compression: CompressionFormat,
    ) -> StageSummary | None:
        """Clear target staging and clone an exact compatible successful parse, if any."""
        ...

    async def stage(self, source_file_id: UUID, events: Iterable[ParsedEvent]) -> StageSummary: ...

    async def finalize_staging(
        self,
        source_file_id: UUID,
        document_type: DocumentType,
    ) -> StageSummary: ...

    async def has_file_quarantine_issue(self, source_file_id: UUID) -> bool: ...

    async def apply(
        self,
        source_file_id: UUID,
        document_type: DocumentType,
        *,
        minimum_full_records: int,
        maximum_drop_fraction: float,
    ) -> ApplySummary: ...

    async def archive_key(self, source_file_id: UUID) -> str: ...

    async def archive_sha256(self, source_file_id: UUID) -> str: ...

    async def begin_replay(
        self,
        source_file_id: UUID,
        *,
        parser_version: str,
        rebuild_run_id: UUID | None = None,
    ) -> ReplayAttempt: ...

    async def finish_replay(
        self,
        replay_id: UUID,
        *,
        succeeded: bool,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None: ...


class ArchiveReplayRepository(Protocol):
    """Select archived source files with stable, bounded keyset pagination."""

    async def list_archived_files(
        self,
        *,
        since: datetime,
        until: datetime,
        limit: int,
        cursor: str | None,
    ) -> ArchivedSourceFilePage: ...


class NormalizedRebuildRepository(Protocol):
    """Audit and coordinate one in-place normalized-state rebuild."""

    def lock_rebuild(self, rebuild_run_id: UUID) -> AbstractAsyncContextManager[bool]: ...

    async def begin_rebuild(
        self,
        *,
        requested_by: str,
        parser_version: str,
    ) -> NormalizedRebuildRun: ...

    async def resume_rebuild(self, rebuild_run_id: UUID) -> NormalizedRebuildRun: ...

    async def get_rebuild(self, rebuild_run_id: UUID) -> NormalizedRebuildRun: ...

    async def next_rebuild_files(
        self,
        rebuild_run_id: UUID,
        *,
        limit: int,
    ) -> tuple[tuple[int, ArchivedSourceFile], ...]: ...

    async def complete_rebuild_file(
        self,
        rebuild_run_id: UUID,
        *,
        sequence: int,
        source_file: ArchivedSourceFile,
    ) -> None: ...

    async def fail_rebuild(
        self,
        rebuild_run_id: UUID,
        *,
        error_code: str,
        error_message: str,
    ) -> None: ...

    async def finish_rebuild(self, rebuild_run_id: UUID) -> NormalizedRebuildRun: ...

    async def maintenance_status(self) -> dict[str, object]: ...


class LeaseManager(Protocol):
    def acquire(
        self,
        resource: str,
        owner: str,
        ttl: timedelta,
    ) -> AbstractAsyncContextManager[bool]: ...


class MetricRecorder(Protocol):
    def increment(
        self, name: str, *, labels: dict[str, str] | None = None, value: int = 1
    ) -> None: ...

    def observe(self, name: str, value: float, *, labels: dict[str, str] | None = None) -> None: ...

    def set_gauge(
        self, name: str, value: float, *, labels: dict[str, str] | None = None
    ) -> None: ...


class TemporaryFiles(Protocol):
    def create(self, *, suffix: str = "") -> AbstractAsyncContextManager[Path]: ...


class QueryRepository(Protocol):
    async def list_retailers(self, *, limit: int, cursor: str | None) -> Page: ...

    async def find_stores(
        self,
        *,
        query: str | None,
        retailer_id: UUID | None,
        city: str | None,
        limit: int,
        cursor: str | None,
    ) -> Page: ...

    async def search_products(
        self,
        query: str,
        *,
        quantity: Decimal | None,
        unit: str | None,
        limit: int,
        cursor: str | None,
    ) -> Page: ...

    async def get_product(self, product_id: UUID) -> dict[str, object] | None: ...

    async def find_product_by_barcode(self, barcode: str) -> dict[str, object] | None: ...

    async def find_product_by_retailer_item_code(
        self,
        retailer_id: UUID,
        item_code: str,
        *,
        portal_id: UUID | None = None,
    ) -> dict[str, object] | None: ...

    async def current_prices(
        self,
        product_id: UUID,
        *,
        retailer_id: UUID | None,
        store_id: UUID | None,
        limit: int,
        cursor: str | None,
    ) -> Page: ...

    async def price_history(
        self,
        product_id: UUID,
        *,
        store_id: UUID | None,
        since: datetime | None,
        until: datetime | None,
        limit: int,
        cursor: str | None,
    ) -> Page: ...

    async def active_promotions(
        self,
        *,
        product_id: UUID | None,
        store_id: UUID | None,
        at: datetime,
        limit: int,
        cursor: str | None,
    ) -> Page: ...

    async def promotion_history(
        self,
        *,
        product_id: UUID | None,
        store_id: UUID | None,
        since: datetime | None,
        until: datetime | None,
        limit: int,
        cursor: str | None,
    ) -> Page: ...

    async def item_availability(
        self,
        product_id: UUID,
        *,
        store_id: UUID | None,
        limit: int,
        cursor: str | None,
    ) -> Page: ...

    async def freshness(self, *, limit: int, cursor: str | None) -> Page: ...

    async def source_status(self, *, limit: int, cursor: str | None) -> Page: ...

    async def maintenance_status(self) -> dict[str, object]: ...


class CatalogMatchingRepository(Protocol):
    """Bounded candidate generation and transactional operator review."""

    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]: ...

    async def generate_candidates(
        self,
        *,
        cursor: str | None,
        item_limit: int,
        candidate_limit: int,
        review_threshold: str,
    ) -> CatalogCandidateGenerationResult: ...

    async def list_candidates(
        self,
        *,
        status: str,
        retailer_id: UUID | None,
        limit: int,
        cursor: str | None,
    ) -> Page: ...

    async def inspect_candidate(self, candidate_id: UUID) -> dict[str, object]: ...

    async def accept_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]: ...

    async def reject_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]: ...
