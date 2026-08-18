from __future__ import annotations

from itertools import pairwise

import pytest

from makolet.domain.enums import DocumentType, IngestionStatus, ensure_ingestion_transition
from makolet.domain.errors import InvalidStateTransitionError


@pytest.mark.parametrize(
    ("document_type", "is_full", "is_delta"),
    [
        (DocumentType.STORES, True, False),
        (DocumentType.PRICE_FULL, True, False),
        (DocumentType.PRICE_DELTA, False, True),
        (DocumentType.PROMOTION_FULL, True, False),
        (DocumentType.PROMOTION_DELTA, False, True),
        (DocumentType.UNKNOWN, False, False),
    ],
)
def test_document_type_semantics(
    document_type: DocumentType,
    is_full: bool,
    is_delta: bool,
) -> None:
    assert document_type.is_full_snapshot is is_full
    assert document_type.is_delta is is_delta


def test_ingestion_happy_path_transitions_are_allowed() -> None:
    path = (
        IngestionStatus.DISCOVERED,
        IngestionStatus.DOWNLOADING,
        IngestionStatus.ARCHIVED,
        IngestionStatus.PARSING,
        IngestionStatus.STAGED,
        IngestionStatus.VALIDATING,
        IngestionStatus.APPLYING,
        IngestionStatus.COMPLETED,
    )

    for current, target in pairwise(path):
        ensure_ingestion_transition(current, target)


def test_completed_file_cannot_return_to_parsing() -> None:
    with pytest.raises(InvalidStateTransitionError, match="completed to parsing"):
        ensure_ingestion_transition(IngestionStatus.COMPLETED, IngestionStatus.PARSING)


def test_archived_file_can_be_quarantined() -> None:
    ensure_ingestion_transition(IngestionStatus.ARCHIVED, IngestionStatus.QUARANTINED)


def test_archived_file_cannot_complete_without_apply() -> None:
    with pytest.raises(InvalidStateTransitionError, match="archived to completed"):
        ensure_ingestion_transition(IngestionStatus.ARCHIVED, IngestionStatus.COMPLETED)
