"""Deterministic normalization for identifiers, text, numbers, and source dates."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from makolet.domain.enums import IdentifierKind
from makolet.domain.errors import DomainValidationError

ISRAEL_TIMEZONE = ZoneInfo("Asia/Jerusalem")
MAX_MONEY = Decimal("9999999999.9999")
_WHITESPACE = re.compile(r"\s+")
_NON_DIGITS = re.compile(r"\D+")
_NUMBER_WITH_UNIT = re.compile(
    r"(?P<number>\d+(?:[.,]\d+)?)\s*(?P<unit>ק[\"״']?ג|קילוגר(?:ם|מים)|kg|"
    r"גר(?:ם|מים)|g|מ[\"״']?ל|מיליליטר(?:ים)?|ml|ליטר(?:ים)?|l|"
    r"יחיד(?:ה|ות)|יח'|ea|each|מטר(?:ים)?|m)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class NormalizedQuantity:
    amount: Decimal
    unit: str


def clean_source_text(value: str | None, *, max_length: int = 10_000) -> str | None:
    """Normalize Unicode/control characters while preserving source-language text."""

    if value is None:
        return None
    normalized = unicodedata.normalize("NFKC", value).strip()
    normalized = "".join(
        character
        for character in normalized
        if character in "\t\n\r" or not unicodedata.category(character).startswith("C")
    )
    normalized = _WHITESPACE.sub(" ", normalized).strip()
    if not normalized:
        return None
    if len(normalized) > max_length:
        raise DomainValidationError(f"Text exceeds the {max_length}-character limit")
    return normalized


def normalize_search_text(value: str) -> str:
    """Create a language-neutral, punctuation-insensitive search representation."""

    normalized = unicodedata.normalize("NFKC", value).casefold()
    characters: list[str] = []
    for character in normalized:
        category = unicodedata.category(character)
        if category.startswith(("L", "N")):
            characters.append(character)
        elif category.startswith(("P", "S", "Z")) or character.isspace():
            characters.append(" ")
        elif category == "Mn":
            # Hebrew cantillation/vowel points and Latin combining marks do not
            # distinguish product identity for search.
            continue
    return _WHITESPACE.sub(" ", "".join(characters)).strip()


def normalize_identifier(value: str) -> str:
    """Normalize an opaque source identifier without assuming it is a GTIN."""

    normalized = unicodedata.normalize("NFKC", value).strip()
    normalized = "".join(character for character in normalized if not character.isspace())
    if not normalized or len(normalized) > 128:
        raise DomainValidationError("Identifier is empty or exceeds 128 characters")
    if any(unicodedata.category(character).startswith("C") for character in normalized):
        raise DomainValidationError("Identifier contains control characters")
    return normalized


def is_valid_gtin(value: str) -> bool:
    """Return whether ``value`` is a checksum-valid GTIN-8/12/13/14."""

    if len(value) not in {8, 12, 13, 14} or not value.isascii() or not value.isdigit():
        return False
    expected = int(value[-1])
    checksum_sum = 0
    for offset, character in enumerate(reversed(value[:-1])):
        checksum_sum += int(character) * (3 if offset % 2 == 0 else 1)
    calculated = (10 - checksum_sum % 10) % 10
    return calculated == expected


def classify_identifier(value: str) -> IdentifierKind:
    normalized = normalize_identifier(value)
    if is_valid_gtin(normalized):
        return IdentifierKind.GTIN
    return IdentifierKind.RETAILER_ITEM


def parse_decimal(
    value: str | int | Decimal | None,
    *,
    field_name: str,
    required: bool = False,
    allow_negative: bool = False,
    maximum: Decimal = MAX_MONEY,
) -> Decimal | None:
    """Parse a finite fixed-precision source value without binary floating point."""

    if value is None or (isinstance(value, str) and not value.strip()):
        if required:
            raise DomainValidationError(f"{field_name} is required")
        return None
    source = str(value).strip().replace("\u00a0", "")
    if source.count(",") == 1 and "." not in source:
        source = source.replace(",", ".")
    elif "," in source:
        source = source.replace(",", "")
    try:
        parsed = Decimal(source)
    except InvalidOperation as error:
        raise DomainValidationError(f"{field_name} is not a decimal value") from error
    if not parsed.is_finite():
        raise DomainValidationError(f"{field_name} must be finite")
    if not allow_negative and parsed < 0:
        raise DomainValidationError(f"{field_name} cannot be negative")
    if abs(parsed) > maximum:
        raise DomainValidationError(f"{field_name} exceeds the allowed maximum")
    return parsed


def parse_int(value: str | int | None, *, field_name: str) -> int | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return int(str(value).strip())
    except ValueError as error:
        raise DomainValidationError(f"{field_name} is not an integer") from error


def parse_bool(value: str | int | bool | None, *, field_name: str) -> bool | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().casefold()
    if normalized in {"1", "true", "yes", "y", "כן"}:
        return True
    if normalized in {"0", "false", "no", "n", "לא"}:
        return False
    raise DomainValidationError(f"{field_name} is not a recognized boolean")


_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %I:%M:%S %p",
    "%Y%m%d%H%M%S",
    "%Y%m%d%H%M",
    "%Y%m%d",
    "%Y-%m-%d",
    "%Y/%m/%d",
)


def parse_source_datetime(value: str | None) -> datetime | None:
    """Parse documented retailer date forms and return a timezone-aware instant."""

    cleaned = clean_source_text(value, max_length=64)
    if cleaned is None:
        return None
    source = cleaned.replace("Z", "+00:00") if cleaned.endswith("Z") else cleaned
    for date_format in _DATETIME_FORMATS:
        try:
            # Formats without offsets are explicitly interpreted as publisher-local
            # Israel time immediately below; aware formats preserve their offset.
            parsed = datetime.strptime(source, date_format)  # noqa: DTZ007
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = _unambiguous_israel_local_time(parsed)
        return parsed.astimezone(UTC)
    raise DomainValidationError("Source date is not in a supported unambiguous format")


def _unambiguous_israel_local_time(value: datetime) -> datetime:
    """Localize one naive wall time only when it maps to exactly one UTC instant."""

    candidates: dict[datetime, datetime] = {}
    for fold in (0, 1):
        localized = value.replace(tzinfo=ISRAEL_TIMEZONE, fold=fold)
        instant = localized.astimezone(UTC)
        round_trip = instant.astimezone(ISRAEL_TIMEZONE).replace(tzinfo=None)
        if round_trip == value:
            candidates[instant] = localized
    if len(candidates) != 1:
        classification = "nonexistent" if not candidates else "ambiguous"
        raise DomainValidationError(
            f"Source local date is {classification} in Asia/Jerusalem; "
            "an explicit offset is required"
        )
    return next(iter(candidates.values()))


def combine_source_date_time(date_value: str | None, time_value: str | None) -> datetime | None:
    date_text = clean_source_text(date_value, max_length=32)
    time_text = clean_source_text(time_value, max_length=32)
    if date_text is None and time_text is None:
        return None
    if date_text is None:
        raise DomainValidationError("Source time is present without a date")
    return parse_source_datetime(f"{date_text} {time_text}" if time_text else date_text)


def parse_quantity_text(value: str) -> NormalizedQuantity | None:
    """Extract the first explicit amount/unit and normalize to base retail units."""

    normalized = unicodedata.normalize("NFKC", value)
    match = _NUMBER_WITH_UNIT.search(normalized)
    if match is None:
        return None
    amount = parse_decimal(match.group("number"), field_name="quantity", required=True)
    if amount is None:  # The required=True contract makes this defensive only.
        raise DomainValidationError("quantity is required")
    raw_unit = match.group("unit").casefold().replace("״", '"').replace("'", '"')
    if raw_unit in {'ק"ג', "קילוגרם", "קילוגרמים", "kg"}:
        return NormalizedQuantity(amount * 1000, "g")
    if raw_unit in {"גרם", "גרמים", "g"}:
        return NormalizedQuantity(amount, "g")
    if raw_unit in {'מ"ל', "מיליליטר", "מיליליטרים", "ml"}:
        return NormalizedQuantity(amount, "ml")
    if raw_unit in {"ליטר", "ליטרים", "l"}:
        return NormalizedQuantity(amount * 1000, "ml")
    if raw_unit in {"יחידה", "יחידות", "יח'", "ea", "each"}:
        return NormalizedQuantity(amount, "each")
    if raw_unit in {"מטר", "מטרים", "m"}:
        return NormalizedQuantity(amount, "m")
    return None


def without_first_quantity_text(value: str) -> str:
    """Remove the first recognized amount/unit expression from search text."""

    normalized = unicodedata.normalize("NFKC", value)
    return _NUMBER_WITH_UNIT.sub(" ", normalized, count=1)


def digits_only(value: str) -> str:
    return _NON_DIGITS.sub("", unicodedata.normalize("NFKC", value))
