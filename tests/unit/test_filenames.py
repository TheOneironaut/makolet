from __future__ import annotations

from datetime import UTC, datetime

import pytest

from makolet.domain.enums import CompressionFormat, DocumentType
from makolet.domain.errors import DomainValidationError
from makolet.domain.filenames import (
    canonical_remote_id,
    detect_document_type,
    extract_source_timestamp,
    infer_compression,
    safe_basename,
    timestamp_from_listing,
)


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("PriceFull7290000000001-001-007-20260811-030000.gz", DocumentType.PRICE_FULL),
        ("PRICE7290000000001-001-007-202608112015.GZ", DocumentType.PRICE_DELTA),
        ("PromoFull7290000000001-001-007-20260811.xml.gz", DocumentType.PROMOTION_FULL),
        ("Promos7290000000001-001-007-20260811.zip", DocumentType.PROMOTION_DELTA),
        ("StoresFull7290000000001-000-20260811-050014.GZ", DocumentType.STORES),
        ("unrelated-20260811.xml", DocumentType.UNKNOWN),
    ],
)
def test_detect_document_type_is_case_and_suffix_tolerant(
    filename: str,
    expected: DocumentType,
) -> None:
    assert detect_document_type(filename) is expected


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("file.xml.gz", CompressionFormat.GZIP),
        ("file.XML.ZIP", CompressionFormat.ZIP),
        ("file.json.zst", CompressionFormat.ZSTANDARD),
        ("file.xml", CompressionFormat.NONE),
        ("file.bin", CompressionFormat.UNKNOWN),
    ],
)
def test_infer_compression(filename: str, expected: CompressionFormat) -> None:
    assert infer_compression(filename) is expected


def test_extract_source_timestamp_uses_last_date_and_is_utc() -> None:
    result = extract_source_timestamp("PriceFull7290000000001-001-007-20260811-030000.gz")

    assert result == datetime(2026, 8, 11, 0, 0, tzinfo=UTC)


def test_invalid_calendar_date_does_not_become_source_timestamp() -> None:
    assert extract_source_timestamp("PriceFull7290000000001-20260231-030000.gz") is None


def test_safe_basename_never_preserves_source_directories() -> None:
    assert safe_basename("../../unsafe/path/PriceFull.xml.gz") == "PriceFull.xml.gz"
    assert safe_basename(r"..\unsafe\PriceFull.xml.gz") == "PriceFull.xml.gz"


@pytest.mark.parametrize(
    "unsafe",
    [
        "bad\x00name.gz",
        "bad\x1bname.gz",
        "bad\r\nname.gz",
        "bad\x7fname.gz",
        "bad\u009bname.gz",
        "bad\ud800name.gz",
        "bad\u2028name.gz",
        "bad\u2029name.gz",
        "bad\u202ename.gz",
        "bad\u2067name.gz",
    ],
)
def test_safe_basename_rejects_controls_surrogates_and_line_separators(unsafe: str) -> None:
    with pytest.raises(DomainValidationError, match="unsafe Unicode controls"):
        safe_basename(unsafe)


def test_safe_basename_accepts_ordinary_hebrew_filename() -> None:
    filename = "מחירים-סניף-ירושלים-20260812.xml.gz"

    assert safe_basename(filename) == filename


def test_safe_basename_enforces_utf8_byte_bound() -> None:
    with pytest.raises(DomainValidationError, match="255-byte"):
        safe_basename("א" * 128 + ".gz")


def test_canonical_remote_id_excludes_expiring_query_and_fragment() -> None:
    remote_id = canonical_remote_id(
        "https://OBJECTS.EXAMPLE/price/PriceFull.xml.gz?signature=secret#fragment"
    )

    assert remote_id == "objects.example/price/PriceFull.xml.gz"


def test_listing_timestamp_is_interpreted_in_israel_timezone() -> None:
    assert timestamp_from_listing("8/11/2026 8:00:00 PM") == datetime(
        2026, 8, 11, 17, 0, tzinfo=UTC
    )


@pytest.mark.parametrize("value", ["3/27/2026 2:30:00 AM", "10/25/2026 1:30:00 AM"])
def test_listing_timestamp_rejects_jerusalem_gap_and_fold(value: str) -> None:
    with pytest.raises(DomainValidationError, match="expected format"):
        timestamp_from_listing(value)
