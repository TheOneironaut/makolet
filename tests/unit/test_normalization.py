from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from makolet.domain.enums import IdentifierKind
from makolet.domain.errors import DomainValidationError
from makolet.domain.normalization import (
    classify_identifier,
    clean_source_text,
    combine_source_date_time,
    is_valid_gtin,
    normalize_identifier,
    normalize_search_text,
    parse_bool,
    parse_decimal,
    parse_int,
    parse_quantity_text,
    parse_source_datetime,
)


def test_search_normalization_handles_hebrew_latin_and_punctuation() -> None:
    assert normalize_search_text("  קפה—NESPRESSO,  10  יח\u05f3 ") == "קפה nespresso 10 יח"


def test_clean_source_text_removes_controls_and_collapses_space() -> None:
    assert clean_source_text("  חלב\x00\n  טרי  ") == "חלב טרי"
    assert clean_source_text(" \t ") is None


def test_normalize_identifier_preserves_meaningful_punctuation() -> None:
    assert normalize_identifier(" AB- 123 ") == "AB-123"


def test_normalize_identifier_rejects_controls() -> None:
    with pytest.raises(DomainValidationError, match="control"):
        normalize_identifier("AB\x00123")


@pytest.mark.parametrize("value", ["4006381333931", "96385074"])
def test_valid_gtins_pass_checksum(value: str) -> None:
    assert is_valid_gtin(value)
    assert classify_identifier(value) is IdentifierKind.GTIN


def test_invalid_gtin_remains_retailer_item() -> None:
    assert not is_valid_gtin("4006381333932")
    assert classify_identifier("4006381333932") is IdentifierKind.RETAILER_ITEM


def test_decimal_parser_never_uses_binary_floating_point() -> None:
    assert parse_decimal("12,50", field_name="price", required=True) == Decimal("12.50")
    assert parse_decimal("1,234.50", field_name="price", required=True) == Decimal("1234.50")


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-0.01", "not-a-price"])
def test_decimal_parser_rejects_invalid_money(value: str) -> None:
    with pytest.raises(DomainValidationError):
        parse_decimal(value, field_name="price", required=True)


def test_optional_number_parsers_accept_empty_values() -> None:
    assert parse_decimal("", field_name="price") is None
    assert parse_int("", field_name="status") is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [("1", True), ("0", False), ("כן", True), ("לא", False), (True, True)],
)
def test_boolean_parser_handles_observed_source_forms(
    value: str | bool,
    expected: bool,
) -> None:
    assert parse_bool(value, field_name="flag") is expected


def test_boolean_parser_rejects_unknown_value() -> None:
    with pytest.raises(DomainValidationError, match="recognized boolean"):
        parse_bool("maybe", field_name="flag")


def test_source_datetime_converts_israel_summer_time_to_utc() -> None:
    assert parse_source_datetime("2026-08-11T20:00:00") == datetime(2026, 8, 11, 17, 0, tzinfo=UTC)


def test_source_datetime_preserves_explicit_offset() -> None:
    assert parse_source_datetime("2026-08-11T20:00:00+02:00") == datetime(
        2026, 8, 11, 18, 0, tzinfo=UTC
    )


@pytest.mark.parametrize(
    ("value", "classification"),
    [
        ("2026-03-27T02:30:00", "nonexistent"),
        ("2026-10-25T01:30:00", "ambiguous"),
    ],
)
def test_source_datetime_rejects_dst_gap_and_fold_without_offset(
    value: str,
    classification: str,
) -> None:
    with pytest.raises(DomainValidationError, match=classification):
        parse_source_datetime(value)


def test_source_datetime_accepts_explicit_offset_during_dst_fold() -> None:
    assert parse_source_datetime("2026-10-25T01:30:00+03:00") == datetime(
        2026, 10, 24, 22, 30, tzinfo=UTC
    )
    assert parse_source_datetime("2026-10-25T01:30:00+02:00") == datetime(
        2026, 10, 24, 23, 30, tzinfo=UTC
    )


def test_combine_source_date_time() -> None:
    assert combine_source_date_time("2026-08-11", "02:01:02") == datetime(
        2026, 8, 10, 23, 1, 2, tzinfo=UTC
    )


def test_combine_source_date_time_accepts_observed_fractional_seconds() -> None:
    assert combine_source_date_time("2026-08-11", "05:00:21.336") == datetime(
        2026,
        8,
        11,
        2,
        0,
        21,
        336_000,
        tzinfo=UTC,
    )


def test_source_time_without_date_is_invalid() -> None:
    with pytest.raises(DomainValidationError, match="without a date"):
        combine_source_date_time(None, "12:00:00")


@pytest.mark.parametrize(
    ("text", "amount", "unit"),
    [
        ("חלב 1 ליטר", Decimal("1000"), "ml"),
        ('אורז 2 ק"ג', Decimal("2000"), "g"),
        ("מארז 6 יחידות", Decimal("6"), "each"),
        ("נייר 30 מטרים", Decimal("30"), "m"),
    ],
)
def test_quantity_text_is_normalized_to_base_units(
    text: str,
    amount: Decimal,
    unit: str,
) -> None:
    result = parse_quantity_text(text)

    assert result is not None
    assert result.amount == amount
    assert result.unit == unit


def test_quantity_text_returns_none_without_explicit_unit() -> None:
    assert parse_quantity_text("מוצר ללא כמות") is None
