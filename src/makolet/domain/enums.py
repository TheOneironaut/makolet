"""Stable domain enumerations."""

from __future__ import annotations

from enum import StrEnum

from makolet.domain.errors import InvalidStateTransitionError


class DocumentType(StrEnum):
    """Regulated source-document semantics."""

    STORES = "stores"
    PRICE_FULL = "price_full"
    PRICE_DELTA = "price_delta"
    PROMOTION_FULL = "promotion_full"
    PROMOTION_DELTA = "promotion_delta"
    UNKNOWN = "unknown"

    @property
    def is_full_snapshot(self) -> bool:
        return self in {
            DocumentType.STORES,
            DocumentType.PRICE_FULL,
            DocumentType.PROMOTION_FULL,
        }

    @property
    def is_delta(self) -> bool:
        return self in {DocumentType.PRICE_DELTA, DocumentType.PROMOTION_DELTA}

    @property
    def is_price(self) -> bool:
        return self in {DocumentType.PRICE_FULL, DocumentType.PRICE_DELTA}

    @property
    def is_promotion(self) -> bool:
        return self in {DocumentType.PROMOTION_FULL, DocumentType.PROMOTION_DELTA}


class CompressionFormat(StrEnum):
    """Supported outer compression/container formats."""

    NONE = "none"
    GZIP = "gzip"
    ZIP = "zip"
    ZSTANDARD = "zstandard"
    UNKNOWN = "unknown"


class SourceProtocol(StrEnum):
    HTTPS = "https"
    HTTP = "http"
    FTP = "ftp"
    FTPS = "ftps"
    FIXTURE = "fixture"


class IngestionStatus(StrEnum):
    """Auditable source-file lifecycle."""

    DISCOVERED = "discovered"
    DOWNLOADING = "downloading"
    ARCHIVED = "archived"
    PARSING = "parsing"
    STAGED = "staged"
    VALIDATING = "validating"
    APPLYING = "applying"
    COMPLETED = "completed"
    QUARANTINED = "quarantined"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_TERMINAL = "failed_terminal"


_ALLOWED_TRANSITIONS: dict[IngestionStatus, frozenset[IngestionStatus]] = {
    IngestionStatus.DISCOVERED: frozenset(
        {
            IngestionStatus.DOWNLOADING,
            IngestionStatus.FAILED_RETRYABLE,
            IngestionStatus.FAILED_TERMINAL,
        }
    ),
    IngestionStatus.DOWNLOADING: frozenset(
        {
            IngestionStatus.ARCHIVED,
            IngestionStatus.FAILED_RETRYABLE,
            IngestionStatus.FAILED_TERMINAL,
        }
    ),
    IngestionStatus.ARCHIVED: frozenset(
        {
            IngestionStatus.PARSING,
            IngestionStatus.QUARANTINED,
            IngestionStatus.FAILED_RETRYABLE,
            IngestionStatus.FAILED_TERMINAL,
        }
    ),
    IngestionStatus.PARSING: frozenset(
        {
            IngestionStatus.STAGED,
            IngestionStatus.QUARANTINED,
            IngestionStatus.FAILED_RETRYABLE,
            IngestionStatus.FAILED_TERMINAL,
        }
    ),
    IngestionStatus.STAGED: frozenset(
        {
            IngestionStatus.VALIDATING,
            IngestionStatus.QUARANTINED,
            IngestionStatus.FAILED_RETRYABLE,
            IngestionStatus.FAILED_TERMINAL,
        }
    ),
    IngestionStatus.VALIDATING: frozenset(
        {
            IngestionStatus.APPLYING,
            IngestionStatus.QUARANTINED,
            IngestionStatus.FAILED_RETRYABLE,
            IngestionStatus.FAILED_TERMINAL,
        }
    ),
    IngestionStatus.APPLYING: frozenset(
        {
            IngestionStatus.COMPLETED,
            IngestionStatus.QUARANTINED,
            IngestionStatus.FAILED_RETRYABLE,
            IngestionStatus.FAILED_TERMINAL,
        }
    ),
    IngestionStatus.FAILED_RETRYABLE: frozenset(
        {
            IngestionStatus.ARCHIVED,
            IngestionStatus.DOWNLOADING,
            IngestionStatus.PARSING,
            IngestionStatus.VALIDATING,
            IngestionStatus.APPLYING,
            IngestionStatus.FAILED_TERMINAL,
        }
    ),
    IngestionStatus.COMPLETED: frozenset(),
    IngestionStatus.QUARANTINED: frozenset(),
    IngestionStatus.FAILED_TERMINAL: frozenset(),
}


def ensure_ingestion_transition(
    current: IngestionStatus,
    target: IngestionStatus,
) -> None:
    """Raise when a source file cannot move directly to ``target``."""

    if target not in _ALLOWED_TRANSITIONS[current]:
        message = f"Ingestion status cannot transition from {current.value} to {target.value}"
        raise InvalidStateTransitionError(message)


class IssueCategory(StrEnum):
    """Stable five-way data-quality and operational-failure contract."""

    WARNING = "warning"
    RECORD_REJECTION = "record_rejection"
    FILE_QUARANTINE = "file_quarantine"
    SOURCE_FAILURE = "source_failure"
    SYSTEM_FAILURE = "system_failure"


# Kept as a source-compatible alias for existing parser and persistence callers.
IssueSeverity = IssueCategory


class IdentifierKind(StrEnum):
    GTIN = "gtin"
    RETAILER_ITEM = "retailer_item"
    MANUFACTURER = "manufacturer"
    UNKNOWN = "unknown"


class DiscountKind(StrEnum):
    FIXED_PRICE = "fixed_price"
    PERCENTAGE = "percentage"
    AMOUNT = "amount"
    QUANTITY = "quantity"
    SECOND_ITEM = "second_item"
    MIX_AND_MATCH = "mix_and_match"
    CONDITIONAL = "conditional"
    UNKNOWN = "unknown"
