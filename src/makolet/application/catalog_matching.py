"""Administrative staged catalog matching and review use cases."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from uuid import UUID

from makolet.application.models import CatalogCandidateGenerationResult, Page
from makolet.application.ports import CatalogMatchingRepository
from makolet.domain.errors import DomainValidationError, QueryLimitError


class CandidateStatus(StrEnum):
    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"


@dataclass(frozen=True, slots=True)
class CatalogMatchingLimits:
    default_item_batch: int = 50
    maximum_item_batch: int = 200
    default_candidate_block: int = 50
    maximum_candidate_block: int = 200
    default_review_page: int = 50
    maximum_review_page: int = 200
    maximum_reviewer_characters: int = 200

    def __post_init__(self) -> None:
        bounded_pairs = (
            (self.default_item_batch, self.maximum_item_batch, "item batch"),
            (
                self.default_candidate_block,
                self.maximum_candidate_block,
                "candidate block",
            ),
            (self.default_review_page, self.maximum_review_page, "review page"),
        )
        for default, maximum, name in bounded_pairs:
            if not 1 <= default <= maximum <= 1_000:
                raise ValueError(f"Catalog matching {name} limits are inconsistent")
        if not 1 <= self.maximum_reviewer_characters <= 1_000:
            raise ValueError("Catalog matching reviewer length must be between 1 and 1,000")


class CatalogMatchingService:
    def __init__(
        self,
        repository: CatalogMatchingRepository,
        limits: CatalogMatchingLimits | None = None,
    ) -> None:
        self._repository = repository
        self._limits = limits or CatalogMatchingLimits()

    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]:
        """Idempotently represent every item last changed by one completed source file."""

        return await self._repository.bootstrap_source_file(source_file_id)

    async def generate_candidates(
        self,
        *,
        cursor: str | None = None,
        item_limit: int | None = None,
        candidate_limit: int | None = None,
        review_threshold: Decimal | str = Decimal("0.65"),
    ) -> CatalogCandidateGenerationResult:
        threshold = _threshold(review_threshold)
        return await self._repository.generate_candidates(
            cursor=_catalog_cursor(cursor),
            item_limit=_bounded(
                item_limit,
                default=self._limits.default_item_batch,
                maximum=self._limits.maximum_item_batch,
                name="item_limit",
            ),
            candidate_limit=_bounded(
                candidate_limit,
                default=self._limits.default_candidate_block,
                maximum=self._limits.maximum_candidate_block,
                name="candidate_limit",
            ),
            review_threshold=str(threshold),
        )

    async def list_candidates(
        self,
        *,
        status: CandidateStatus | str = CandidateStatus.PENDING,
        retailer_id: UUID | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        try:
            selected_status = CandidateStatus(status)
        except ValueError as error:
            raise DomainValidationError("Unknown candidate review status") from error
        return await self._repository.list_candidates(
            status=selected_status.value,
            retailer_id=retailer_id,
            limit=_bounded(
                limit,
                default=self._limits.default_review_page,
                maximum=self._limits.maximum_review_page,
                name="limit",
            ),
            cursor=_catalog_cursor(cursor),
        )

    async def inspect_candidate(self, candidate_id: UUID) -> dict[str, object]:
        return await self._repository.inspect_candidate(candidate_id)

    async def accept_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]:
        return await self._repository.accept_candidate(
            candidate_id,
            reviewed_by=self._reviewer(reviewed_by),
        )

    async def reject_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]:
        return await self._repository.reject_candidate(
            candidate_id,
            reviewed_by=self._reviewer(reviewed_by),
        )

    def _reviewer(self, value: str) -> str:
        reviewer = value.strip()
        if (
            not reviewer
            or len(reviewer) > self._limits.maximum_reviewer_characters
            or any(ord(character) < 32 for character in reviewer)
        ):
            raise DomainValidationError(
                "reviewed_by must be nonempty, bounded, and contain no control characters"
            )
        return reviewer


def _bounded(value: int | None, *, default: int, maximum: int, name: str) -> int:
    selected = default if value is None else value
    if not 1 <= selected <= maximum:
        raise QueryLimitError(f"{name} must be between 1 and {maximum}")
    return selected


def _cursor(value: str | None) -> str | None:
    if value is None:
        return None
    selected = value.strip()
    if not selected or len(selected) > 512 or any(ord(character) < 32 for character in selected):
        raise DomainValidationError("Cursor is empty, too long, or contains control characters")
    return selected


def _catalog_cursor(value: str | None) -> str | None:
    selected = _cursor(value)
    if selected is None:
        return None
    try:
        return str(UUID(selected))
    except ValueError as error:
        raise DomainValidationError("Catalog cursor is not a valid UUID") from error


def _threshold(value: Decimal | str) -> Decimal:
    try:
        selected = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise DomainValidationError("review_threshold must be a decimal") from error
    if not Decimal(0) <= selected <= Decimal(1):
        raise DomainValidationError("review_threshold must be between 0 and 1")
    return selected


__all__ = ["CandidateStatus", "CatalogMatchingLimits", "CatalogMatchingService"]
