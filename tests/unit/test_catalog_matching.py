from __future__ import annotations

from decimal import Decimal
from uuid import UUID

import pytest

from makolet.domain.catalog import (
    CanonicalProductDescriptor,
    MatchDisposition,
    MatchRule,
    ProductDescriptor,
    ProductIdentifier,
    generate_match_candidates,
    product_descriptor,
    score_catalog_candidate,
)
from makolet.domain.enums import IdentifierKind
from makolet.domain.errors import DomainValidationError, QueryLimitError

RETAILER_A = UUID("00000000-0000-0000-0000-00000000000a")
RETAILER_B = UUID("00000000-0000-0000-0000-00000000000b")
ITEM_A = UUID("10000000-0000-0000-0000-00000000000a")
ITEM_B = UUID("10000000-0000-0000-0000-00000000000b")


def descriptor(
    item_id: UUID,
    retailer_id: UUID,
    *,
    gtin: str | None = None,
    retailer_code: str | None = None,
    name: str = "קפה Espresso קפסולות",
    brand: str = "Demo",
    quantity: str = "10 יחידות",
) -> ProductDescriptor:
    identifiers: list[ProductIdentifier] = []
    if gtin:
        identifiers.append(ProductIdentifier(IdentifierKind.GTIN, gtin))
    if retailer_code:
        identifiers.append(ProductIdentifier(IdentifierKind.RETAILER_ITEM, retailer_code))
    return product_descriptor(
        retailer_item_id=item_id,
        retailer_id=retailer_id,
        name=name,
        identifiers=tuple(identifiers),
        brand=brand,
        manufacturer="Demo Foods",
        quantity_text=quantity,
        packaging="box",
    )


def test_checksum_gtin_with_independent_retailer_assertions_can_auto_confirm() -> None:
    left = descriptor(ITEM_A, RETAILER_A, gtin="4006381333931")
    right = descriptor(ITEM_B, RETAILER_B, gtin="4006381333931")

    assert left.identifiers[0].is_validated is False
    assert right.identifiers[0].is_validated is False

    match = generate_match_candidates(left, (right,))[0]

    assert match.rule is MatchRule.EXACT_GTIN
    assert match.disposition is MatchDisposition.AUTO_CONFIRM
    assert match.confidence == Decimal("1.00")
    assert "independent retailer assertions" in match.explanations[0]


def test_checksum_gtin_inside_one_retailer_requires_review() -> None:
    left = descriptor(ITEM_A, RETAILER_A, gtin="4006381333931")
    right = descriptor(ITEM_B, RETAILER_A, gtin="4006381333931")

    match = generate_match_candidates(left, (right,))[0]

    assert match.rule is MatchRule.EXACT_GTIN
    assert match.disposition is MatchDisposition.REVIEW
    assert "not independent" in match.explanations[0]


def test_exact_retailer_identifier_creates_reviewable_candidate() -> None:
    left = descriptor(ITEM_A, RETAILER_A, retailer_code=" SKU-1 ")
    right = descriptor(ITEM_B, RETAILER_B, retailer_code="SKU-1")

    match = generate_match_candidates(left, (right,))[0]

    assert match.rule is MatchRule.EXACT_NORMALIZED_IDENTIFIER
    assert match.disposition is MatchDisposition.REVIEW


def test_structured_similarity_never_silently_merges() -> None:
    left = descriptor(ITEM_A, RETAILER_A)
    right = descriptor(ITEM_B, RETAILER_B)

    match = generate_match_candidates(left, (right,))[0]

    assert match.rule is MatchRule.STRUCTURED_CANDIDATE
    assert match.disposition is MatchDisposition.REVIEW
    assert match.confidence == Decimal("1.000") - Decimal("0.010")
    assert any("quantity" in reason for reason in match.explanations)


def test_different_validated_gtins_reduce_but_do_not_hide_review_candidate() -> None:
    left = descriptor(ITEM_A, RETAILER_A, gtin="4006381333931")
    right = descriptor(ITEM_B, RETAILER_B, gtin="96385074")

    match = generate_match_candidates(left, (right,))[0]

    assert match.disposition is MatchDisposition.REVIEW
    assert match.confidence == Decimal("0.750")
    assert any("GTINs" in reason for reason in match.explanations)


def test_candidate_block_is_bounded_before_scoring() -> None:
    left = descriptor(ITEM_A, RETAILER_A)
    right = descriptor(ITEM_B, RETAILER_B)

    with pytest.raises(QueryLimitError, match="positive"):
        generate_match_candidates(left, (right,), maximum_candidates=0)
    with pytest.raises(DomainValidationError, match="between 0 and 1"):
        generate_match_candidates(left, (right,), review_threshold=Decimal("1.1"))


def test_invalid_gtin_cannot_enter_exact_identifier_stage() -> None:
    with pytest.raises(DomainValidationError, match="checksum"):
        ProductIdentifier(IdentifierKind.GTIN, "4006381333932")


def test_staged_canonical_scoring_keeps_exact_identifier_reviewable() -> None:
    subject = descriptor(ITEM_A, RETAILER_A, retailer_code="SKU-1")
    candidate = CanonicalProductDescriptor(
        canonical_product_id=ITEM_B,
        name="Espresso capsules",
        normalized_name="espresso capsules",
        identifiers=(
            ProductIdentifier(
                IdentifierKind.RETAILER_ITEM,
                "SKU-1",
                issuer_retailer_id=RETAILER_B,
                is_validated=True,
            ),
        ),
    )

    match = score_catalog_candidate(subject, candidate)

    assert match is not None
    assert match.rule is MatchRule.EXACT_NORMALIZED_IDENTIFIER
    assert match.disposition is MatchDisposition.REVIEW
    assert match.confidence == Decimal("0.95")
