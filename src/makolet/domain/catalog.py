"""Conservative, explainable canonical-product matching policies."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from makolet.domain.enums import IdentifierKind
from makolet.domain.errors import DomainValidationError, QueryLimitError
from makolet.domain.normalization import (
    is_valid_gtin,
    normalize_identifier,
    normalize_search_text,
    parse_quantity_text,
)


class MatchRule(StrEnum):
    EXACT_GTIN = "exact_gtin"
    EXACT_NORMALIZED_IDENTIFIER = "exact_normalized_identifier"
    STRUCTURED_CANDIDATE = "structured_candidate"


class MatchDisposition(StrEnum):
    AUTO_CONFIRM = "auto_confirm"
    REVIEW = "review"


@dataclass(frozen=True, slots=True)
class ProductIdentifier:
    kind: IdentifierKind
    value: str
    issuer_retailer_id: UUID | None = None
    is_validated: bool = False
    validation_method: str | None = None

    def __post_init__(self) -> None:
        normalized = normalize_identifier(self.value)
        object.__setattr__(self, "value", normalized)
        if self.kind is IdentifierKind.GTIN and not is_valid_gtin(normalized):
            raise DomainValidationError("GTIN identifier failed its checksum")


@dataclass(frozen=True, slots=True)
class ProductDescriptor:
    retailer_item_id: UUID
    retailer_id: UUID
    name: str
    normalized_name: str
    identifiers: tuple[ProductIdentifier, ...]
    brand: str | None = None
    manufacturer: str | None = None
    quantity: Decimal | None = None
    unit: str | None = None
    packaging: str | None = None


@dataclass(frozen=True, slots=True)
class MatchCandidate:
    subject_retailer_item_id: UUID
    candidate_retailer_item_id: UUID
    rule: MatchRule
    disposition: MatchDisposition
    confidence: Decimal
    explanations: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CanonicalProductDescriptor:
    canonical_product_id: UUID
    name: str
    normalized_name: str
    identifiers: tuple[ProductIdentifier, ...]
    brand: str | None = None
    manufacturer: str | None = None
    quantity: Decimal | None = None
    unit: str | None = None
    packaging: str | None = None


@dataclass(frozen=True, slots=True)
class CanonicalMatchCandidate:
    subject_retailer_item_id: UUID
    canonical_product_id: UUID
    rule: MatchRule
    disposition: MatchDisposition
    confidence: Decimal
    explanations: tuple[str, ...]


def product_descriptor(
    *,
    retailer_item_id: UUID,
    retailer_id: UUID,
    name: str,
    identifiers: tuple[ProductIdentifier, ...],
    brand: str | None = None,
    manufacturer: str | None = None,
    quantity_text: str | None = None,
    quantity: Decimal | None = None,
    unit: str | None = None,
    packaging: str | None = None,
) -> ProductDescriptor:
    normalized_name = normalize_search_text(name)
    if not normalized_name:
        raise DomainValidationError("Product name is empty after normalization")
    if quantity_text is not None and (quantity is not None or unit is not None):
        raise DomainValidationError("quantity_text cannot be combined with quantity or unit")
    if quantity is not None and quantity < 0:
        raise DomainValidationError("Product quantity cannot be negative")
    normalized_quantity = parse_quantity_text(quantity_text) if quantity_text else None
    return ProductDescriptor(
        retailer_item_id=retailer_item_id,
        retailer_id=retailer_id,
        name=name,
        normalized_name=normalized_name,
        identifiers=identifiers,
        brand=_optional_search_text(brand),
        manufacturer=_optional_search_text(manufacturer),
        quantity=normalized_quantity.amount if normalized_quantity else quantity,
        unit=(normalized_quantity.unit if normalized_quantity else _optional_search_text(unit)),
        packaging=_optional_search_text(packaging),
    )


def canonical_product_descriptor(
    *,
    canonical_product_id: UUID,
    name: str,
    identifiers: tuple[ProductIdentifier, ...],
    brand: str | None = None,
    manufacturer: str | None = None,
    quantity: Decimal | None = None,
    unit: str | None = None,
    packaging: str | None = None,
) -> CanonicalProductDescriptor:
    normalized_name = normalize_search_text(name)
    if not normalized_name:
        raise DomainValidationError("Canonical product name is empty after normalization")
    if quantity is not None and quantity < 0:
        raise DomainValidationError("Canonical product quantity cannot be negative")
    return CanonicalProductDescriptor(
        canonical_product_id=canonical_product_id,
        name=name,
        normalized_name=normalized_name,
        identifiers=identifiers,
        brand=_optional_search_text(brand),
        manufacturer=_optional_search_text(manufacturer),
        quantity=quantity,
        unit=_optional_search_text(unit),
        packaging=_optional_search_text(packaging),
    )


def score_catalog_candidate(
    subject: ProductDescriptor,
    candidate: CanonicalProductDescriptor,
    *,
    review_threshold: Decimal = Decimal("0.65"),
) -> CanonicalMatchCandidate | None:
    """Score one database-blocked canonical candidate without merging it.

    Even exact identifiers remain review decisions in this staged workflow. The
    ingestion path separately owns automatic, independently corroborated GTIN
    projections.
    """

    if not Decimal(0) <= review_threshold <= Decimal(1):
        raise DomainValidationError("review_threshold must be between 0 and 1")
    shared_gtin = _shared_catalog_identifier(subject, candidate, IdentifierKind.GTIN)
    if shared_gtin is not None:
        validated = any(
            identifier.kind is IdentifierKind.GTIN
            and identifier.value == shared_gtin
            and identifier.issuer_retailer_id is None
            and identifier.is_validated
            for identifier in candidate.identifiers
        )
        confidence = Decimal("1.00") if validated else Decimal("0.98")
        explanation = (
            f"globally validated GTIN {shared_gtin} is identical"
            if validated
            else f"retailer-scoped GTIN {shared_gtin} is identical but not global proof"
        )
        return CanonicalMatchCandidate(
            subject.retailer_item_id,
            candidate.canonical_product_id,
            MatchRule.EXACT_GTIN,
            MatchDisposition.REVIEW,
            confidence,
            (explanation,),
        )

    shared_identifier = _shared_catalog_non_gtin_identifier(subject, candidate)
    if shared_identifier is not None:
        return CanonicalMatchCandidate(
            subject.retailer_item_id,
            candidate.canonical_product_id,
            MatchRule.EXACT_NORMALIZED_IDENTIFIER,
            MatchDisposition.REVIEW,
            Decimal("0.95"),
            (
                f"normalized {shared_identifier.kind.value} identifier is identical; "
                "issuer scope still requires review",
            ),
        )

    score, explanations = _structured_score(subject, candidate)
    confidence = min(Decimal("0.99"), score.quantize(Decimal("0.001")))
    if confidence < review_threshold:
        return None
    return CanonicalMatchCandidate(
        subject.retailer_item_id,
        candidate.canonical_product_id,
        MatchRule.STRUCTURED_CANDIDATE,
        MatchDisposition.REVIEW,
        confidence,
        explanations,
    )


def generate_match_candidates(
    subject: ProductDescriptor,
    blocked_candidates: tuple[ProductDescriptor, ...],
    *,
    maximum_candidates: int = 500,
    review_threshold: Decimal = Decimal("0.65"),
) -> tuple[MatchCandidate, ...]:
    """Score a bounded, database-blocked set; never perform an all-pairs scan."""

    if maximum_candidates <= 0:
        raise QueryLimitError("maximum_candidates must be positive")
    if len(blocked_candidates) > maximum_candidates:
        raise QueryLimitError("Candidate block exceeds the configured bound")
    if not Decimal(0) <= review_threshold <= Decimal(1):
        raise DomainValidationError("review_threshold must be between 0 and 1")
    results: list[MatchCandidate] = []
    for candidate in blocked_candidates:
        if candidate.retailer_item_id == subject.retailer_item_id:
            continue
        match = _compare(subject, candidate, review_threshold)
        if match is not None:
            results.append(match)
    return tuple(
        sorted(
            results,
            key=lambda item: (-item.confidence, str(item.candidate_retailer_item_id)),
        )
    )


def _compare(
    subject: ProductDescriptor,
    candidate: ProductDescriptor,
    review_threshold: Decimal,
) -> MatchCandidate | None:
    shared_gtin = _shared_identifier(subject, candidate, IdentifierKind.GTIN)
    if shared_gtin is not None:
        independently_corroborated = subject.retailer_id != candidate.retailer_id
        return MatchCandidate(
            subject.retailer_item_id,
            candidate.retailer_item_id,
            MatchRule.EXACT_GTIN,
            (
                MatchDisposition.AUTO_CONFIRM
                if independently_corroborated
                else MatchDisposition.REVIEW
            ),
            Decimal("1.00") if independently_corroborated else Decimal("0.98"),
            (
                f"checksum-valid GTIN {shared_gtin} has independent retailer assertions"
                if independently_corroborated
                else (
                    f"checksum-valid GTIN {shared_gtin} is shared only inside one retailer; "
                    "checksum validity is not independent identity evidence"
                ),
            ),
        )

    shared_identifier = _shared_non_gtin_identifier(subject, candidate)
    if shared_identifier is not None:
        return MatchCandidate(
            subject.retailer_item_id,
            candidate.retailer_item_id,
            MatchRule.EXACT_NORMALIZED_IDENTIFIER,
            MatchDisposition.REVIEW,
            Decimal("0.95"),
            (f"normalized {shared_identifier.kind.value} identifier is identical",),
        )

    score, explanations = _structured_score(subject, candidate)
    confidence = min(Decimal("0.99"), score.quantize(Decimal("0.001")))
    if confidence < review_threshold:
        return None
    return MatchCandidate(
        subject.retailer_item_id,
        candidate.retailer_item_id,
        MatchRule.STRUCTURED_CANDIDATE,
        MatchDisposition.REVIEW,
        confidence,
        explanations,
    )


def _structured_score(
    subject: ProductDescriptor,
    candidate: ProductDescriptor | CanonicalProductDescriptor,
) -> tuple[Decimal, tuple[str, ...]]:
    explanations: list[str] = []
    score = _token_similarity(subject.normalized_name, candidate.normalized_name) * Decimal("0.40")
    explanations.append(f"normalized-name token similarity contributes {score:.3f}")
    if _equal_present(subject.brand, candidate.brand):
        score += Decimal("0.20")
        explanations.append("brand matches exactly after normalization")
    if _equal_present(subject.manufacturer, candidate.manufacturer):
        score += Decimal("0.15")
        explanations.append("manufacturer matches exactly after normalization")
    if _same_quantity(subject, candidate):
        score += Decimal("0.20")
        explanations.append("normalized quantity and unit match")
    if _equal_present(subject.packaging, candidate.packaging):
        score += Decimal("0.05")
        explanations.append("packaging matches exactly after normalization")
    if _conflicting_gtins(subject, candidate):
        score = max(Decimal(0), score - Decimal("0.25"))
        explanations.append("different validated GTINs reduce confidence")
    return score, tuple(explanations)


def _shared_identifier(
    left: ProductDescriptor,
    right: ProductDescriptor,
    kind: IdentifierKind,
) -> str | None:
    left_values = {identifier.value for identifier in left.identifiers if identifier.kind is kind}
    right_values = {identifier.value for identifier in right.identifiers if identifier.kind is kind}
    return min(left_values & right_values, default=None)


def _shared_non_gtin_identifier(
    left: ProductDescriptor, right: ProductDescriptor
) -> ProductIdentifier | None:
    right_keys = {
        (identifier.kind, identifier.value)
        for identifier in right.identifiers
        if identifier.kind not in {IdentifierKind.GTIN, IdentifierKind.UNKNOWN}
    }
    return next(
        (
            identifier
            for identifier in left.identifiers
            if (identifier.kind, identifier.value) in right_keys
        ),
        None,
    )


def _shared_catalog_identifier(
    subject: ProductDescriptor,
    candidate: CanonicalProductDescriptor,
    kind: IdentifierKind,
) -> str | None:
    subject_values = {
        identifier.value for identifier in subject.identifiers if identifier.kind is kind
    }
    candidate_values = {
        identifier.value for identifier in candidate.identifiers if identifier.kind is kind
    }
    return min(subject_values & candidate_values, default=None)


def _shared_catalog_non_gtin_identifier(
    subject: ProductDescriptor,
    candidate: CanonicalProductDescriptor,
) -> ProductIdentifier | None:
    candidate_keys = {
        (identifier.kind, identifier.value)
        for identifier in candidate.identifiers
        if identifier.kind not in {IdentifierKind.GTIN, IdentifierKind.UNKNOWN}
    }
    return next(
        (
            identifier
            for identifier in subject.identifiers
            if (identifier.kind, identifier.value) in candidate_keys
        ),
        None,
    )


def _token_similarity(left: str, right: str) -> Decimal:
    left_tokens = frozenset(left.split())
    right_tokens = frozenset(right.split())
    union = left_tokens | right_tokens
    if not union:
        return Decimal(0)
    return Decimal(len(left_tokens & right_tokens)) / Decimal(len(union))


def _same_quantity(
    left: ProductDescriptor,
    right: ProductDescriptor | CanonicalProductDescriptor,
) -> bool:
    return (
        left.quantity is not None
        and right.quantity is not None
        and left.unit is not None
        and left.quantity == right.quantity
        and left.unit == right.unit
    )


def _equal_present(left: str | None, right: str | None) -> bool:
    return left is not None and right is not None and left == right


def _conflicting_gtins(
    left: ProductDescriptor,
    right: ProductDescriptor | CanonicalProductDescriptor,
) -> bool:
    left_values = {
        identifier.value
        for identifier in left.identifiers
        if identifier.kind is IdentifierKind.GTIN
    }
    right_values = {
        identifier.value
        for identifier in right.identifiers
        if identifier.kind is IdentifierKind.GTIN
    }
    return bool(left_values and right_values and left_values.isdisjoint(right_values))


def _optional_search_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = normalize_search_text(value)
    return normalized or None
