"""Domain and application errors with stable public codes."""

from __future__ import annotations


class MakoletError(Exception):
    """Base error carrying a stable code and retry classification."""

    code = "makolet_error"
    retryable = False

    def __init__(self, *arguments: object, transferred_bytes: int = 0) -> None:
        if transferred_bytes < 0:
            raise ValueError("transferred_bytes cannot be negative")
        self.transferred_bytes = transferred_bytes
        super().__init__(*arguments)


class DomainValidationError(MakoletError, ValueError):
    """A value violates a domain invariant."""

    code = "domain_validation_error"


class InvalidStateTransitionError(MakoletError):
    """An ingestion lifecycle transition is not allowed."""

    code = "invalid_state_transition"


class SourceAccessError(MakoletError):
    """A source could not be reached or returned a retryable failure."""

    code = "source_access_error"
    retryable = True


class SourceResponseError(MakoletError):
    """A source returned a permanent or structurally invalid transport response."""

    code = "source_response_error"


class SourceBlockedError(MakoletError):
    """A source requires unavailable access or is externally blocked."""

    code = "source_blocked"


class UnsafeRemoteError(MakoletError):
    """A discovered URL violates the configured SSRF policy."""

    code = "unsafe_remote"


class DownloadLimitError(MakoletError):
    """A response exceeded a configured byte, time, or redirect bound."""

    code = "download_limit_exceeded"

    def __init__(
        self,
        *arguments: object,
        transferred_bytes: int = 0,
        budget_limited: bool = False,
    ) -> None:
        self.budget_limited = budget_limited
        super().__init__(*arguments, transferred_bytes=transferred_bytes)


class DiscoveryBudgetExceededError(MakoletError):
    """A source listing exhausted one cumulative collection-run work budget."""

    code = "discovery_budget_exceeded"
    retryable = True

    def __init__(self, reason: str) -> None:
        if reason not in {
            "listing_request_limit",
            "listing_byte_limit",
            "listing_elapsed_limit",
        }:
            raise ValueError("Unknown discovery budget exhaustion reason")
        self.reason = reason
        super().__init__("Source listing exhausted its cumulative run budget")


class ChargeBudgetExceededError(MakoletError):
    """A source transfer does not fit the collection's remaining charged-byte budget."""

    code = "charge_budget_exceeded"
    retryable = True


class ArchiveCapacityError(MakoletError):
    """Archive storage reached its configured free-space reserve."""

    code = "archive_capacity_exhausted"
    retryable = True


class ArchiveIntegrityError(MakoletError):
    """Archived bytes do not match immutable content metadata."""

    code = "archive_integrity_error"


class UnsafeArchiveError(MakoletError):
    """Compressed content violates path, entry, size, or expansion limits."""

    code = "unsafe_archive"


class MalformedDocumentError(MakoletError):
    """A complete source document cannot be parsed safely."""

    code = "malformed_document"


class QuarantinedFileError(MakoletError):
    """A source file failed whole-file validation and was quarantined."""

    code = "file_quarantined"


class RepositoryError(MakoletError):
    """Persistence operation failed."""

    code = "repository_error"
    retryable = True


class LeaseUnavailableError(MakoletError):
    """Another worker currently owns the bounded ingestion resource."""

    code = "lease_unavailable"
    retryable = True


class MaintenanceModeError(MakoletError):
    """A maintenance barrier intentionally prevents ordinary ingestion."""

    code = "maintenance_mode_active"
    retryable = True


class NormalizedRebuildInterruptedError(MakoletError):
    """An archived file stopped a restartable normalized-state rebuild."""

    code = "normalized_rebuild_interrupted"
    retryable = True


class QueryLimitError(MakoletError, ValueError):
    """A query exceeds a documented public bound."""

    code = "query_limit_exceeded"


class NotFoundError(MakoletError):
    """A requested public resource does not exist."""

    code = "not_found"


class CatalogMatchConflictError(MakoletError):
    """A review decision conflicts with an established catalog assignment."""

    code = "catalog_match_conflict"
