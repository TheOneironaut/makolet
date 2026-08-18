from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from uuid import UUID

import pytest

from makolet.application.catalog_matching import (
    CandidateStatus,
    CatalogMatchingLimits,
    CatalogMatchingService,
)
from makolet.application.models import CatalogCandidateGenerationResult, Page
from makolet.domain.errors import DomainValidationError, QueryLimitError

ITEM_ID = UUID("10000000-0000-0000-0000-000000000001")
RETAILER_ID = UUID("20000000-0000-0000-0000-000000000001")


@dataclass
class StubCatalogRepository:
    calls: list[tuple[str, object]] = field(default_factory=list)

    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]:
        self.calls.append(("bootstrap", source_file_id))
        return {"source_file_id": source_file_id, "bootstrapped_items": 2}

    async def generate_candidates(
        self,
        *,
        cursor: str | None,
        item_limit: int,
        candidate_limit: int,
        review_threshold: str,
    ) -> CatalogCandidateGenerationResult:
        self.calls.append(("generate", (cursor, item_limit, candidate_limit, review_threshold)))
        return CatalogCandidateGenerationResult(2, 1, 3, str(ITEM_ID))

    async def list_candidates(
        self,
        *,
        status: str,
        retailer_id: UUID | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        self.calls.append(("list", (status, retailer_id, limit, cursor)))
        return Page(items=({"id": ITEM_ID, "status": status},), next_cursor=None)

    async def inspect_candidate(self, candidate_id: UUID) -> dict[str, object]:
        self.calls.append(("inspect", candidate_id))
        return {"id": candidate_id, "status": "pending"}

    async def accept_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]:
        self.calls.append(("accept", (candidate_id, reviewed_by)))
        return {"id": candidate_id, "status": "accepted"}

    async def reject_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]:
        self.calls.append(("reject", (candidate_id, reviewed_by)))
        return {"id": candidate_id, "status": "rejected"}


@pytest.mark.asyncio
async def test_service_enforces_bounds_and_normalizes_review_input() -> None:
    repository = StubCatalogRepository()
    service = CatalogMatchingService(repository)

    generated = await service.generate_candidates(
        cursor=f" {ITEM_ID} ",
        item_limit=10,
        candidate_limit=20,
        review_threshold=Decimal("0.70"),
    )
    listed = await service.list_candidates(
        status=CandidateStatus.REJECTED,
        retailer_id=RETAILER_ID,
        limit=5,
    )
    accepted = await service.accept_candidate(ITEM_ID, reviewed_by=" reviewer@example.test ")

    assert generated.candidates_written == 3
    assert listed.items[0]["status"] == "rejected"
    assert accepted["status"] == "accepted"
    assert repository.calls == [
        ("generate", (str(ITEM_ID), 10, 20, "0.70")),
        ("list", ("rejected", RETAILER_ID, 5, None)),
        ("accept", (ITEM_ID, "reviewer@example.test")),
    ]


@pytest.mark.asyncio
async def test_service_rejects_unbounded_or_unaudited_operations() -> None:
    service = CatalogMatchingService(StubCatalogRepository())

    with pytest.raises(QueryLimitError, match="item_limit"):
        await service.generate_candidates(item_limit=201)
    with pytest.raises(DomainValidationError, match="between 0 and 1"):
        await service.generate_candidates(review_threshold="1.1")
    with pytest.raises(DomainValidationError, match="reviewed_by"):
        await service.reject_candidate(ITEM_ID, reviewed_by="   ")
    with pytest.raises(DomainValidationError, match="Unknown"):
        await service.list_candidates(status="invented")
    with pytest.raises(DomainValidationError, match="valid UUID"):
        await service.list_candidates(cursor="not-a-uuid")


def test_custom_limits_fail_closed_when_inconsistent() -> None:
    with pytest.raises(ValueError, match=r"limits are inconsistent|reviewer length"):
        CatalogMatchingLimits(default_item_batch=0)
    with pytest.raises(ValueError, match=r"limits are inconsistent|reviewer length"):
        CatalogMatchingLimits(default_candidate_block=201, maximum_candidate_block=200)
    with pytest.raises(ValueError, match=r"limits are inconsistent|reviewer length"):
        CatalogMatchingLimits(default_review_page=2, maximum_review_page=1)
    with pytest.raises(ValueError, match=r"limits are inconsistent|reviewer length"):
        CatalogMatchingLimits(maximum_reviewer_characters=0)
