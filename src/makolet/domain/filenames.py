"""Source filename, type, compression, and timestamp recognition."""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from pathlib import PurePosixPath, PureWindowsPath
from urllib.parse import unquote, urlsplit

from makolet.domain.enums import CompressionFormat, DocumentType
from makolet.domain.errors import DomainValidationError
from makolet.domain.normalization import parse_source_datetime

_TYPE_PATTERNS: tuple[tuple[re.Pattern[str], DocumentType], ...] = (
    (re.compile(r"^prices?full", re.IGNORECASE), DocumentType.PRICE_FULL),
    (re.compile(r"^promos?(?:tions?)?full", re.IGNORECASE), DocumentType.PROMOTION_FULL),
    (re.compile(r"^stores?full", re.IGNORECASE), DocumentType.STORES),
    (re.compile(r"^stores?", re.IGNORECASE), DocumentType.STORES),
    (re.compile(r"^prices?", re.IGNORECASE), DocumentType.PRICE_DELTA),
    (re.compile(r"^promos?(?:tions?)?", re.IGNORECASE), DocumentType.PROMOTION_DELTA),
)
_TIMESTAMP_PATTERN = re.compile(
    r"(?<!\d)(?P<date>20\d{6})(?:[-_T]?(?P<time>\d{4}(?:\d{2})?))?(?!\d)"
)
_UNSAFE_BIDI_CONTROLS = frozenset(
    {
        "\u061c",  # Arabic letter mark
        "\u200e",  # left-to-right mark
        "\u200f",  # right-to-left mark
        "\u202a",  # embeddings and overrides
        "\u202b",
        "\u202c",
        "\u202d",
        "\u202e",
        "\u2066",  # directional isolates
        "\u2067",
        "\u2068",
        "\u2069",
    }
)


def validate_display_filename(value: str) -> None:
    """Reject filenames that can alter paths, terminals, or visual direction."""

    if not value:
        raise DomainValidationError("Filename is empty")
    if any(
        unicodedata.category(character) in {"Cc", "Cs", "Zl", "Zp"}
        or character in _UNSAFE_BIDI_CONTROLS
        for character in value
    ):
        raise DomainValidationError("Filename contains unsafe Unicode controls")
    if len(value) > 255 or len(value.encode("utf-8")) > 255:
        raise DomainValidationError("Filename exceeds the 255-byte/character limit")


def safe_basename(value: str) -> str:
    """Return a decoded basename while rejecting path/control ambiguity."""

    if not value:
        raise DomainValidationError("Filename is empty")
    parsed_path = unquote(urlsplit(value).path) if "://" in value else unquote(value)
    posix_name = PurePosixPath(parsed_path.replace("\\", "/")).name
    windows_name = PureWindowsPath(posix_name).name
    if windows_name in {"", ".", ".."}:
        raise DomainValidationError("Filename basename is empty or reserved")
    validate_display_filename(windows_name)
    return windows_name


def infer_compression(filename: str) -> CompressionFormat:
    lowered = safe_basename(filename).casefold()
    if lowered.endswith((".gz", ".gzip")):
        return CompressionFormat.GZIP
    if lowered.endswith(".zip"):
        return CompressionFormat.ZIP
    if lowered.endswith((".zst", ".zstd")):
        return CompressionFormat.ZSTANDARD
    if lowered.endswith((".xml", ".json", ".csv", ".html", ".htm")):
        return CompressionFormat.NONE
    return CompressionFormat.UNKNOWN


def strip_known_suffixes(filename: str) -> str:
    basename = safe_basename(filename)
    suffixes = (".gzip", ".zstd", ".gz", ".zip", ".zst", ".xml", ".json", ".csv")
    result = basename
    while True:
        lowered = result.casefold()
        matched = next((suffix for suffix in suffixes if lowered.endswith(suffix)), None)
        if matched is None:
            return result
        result = result[: -len(matched)]


def detect_document_type(filename: str) -> DocumentType:
    stem = strip_known_suffixes(filename).lstrip("_- ")
    for pattern, document_type in _TYPE_PATTERNS:
        if pattern.search(stem):
            return document_type
    return DocumentType.UNKNOWN


def extract_source_timestamp(filename: str) -> datetime | None:
    """Extract the last filename timestamp and normalize it to UTC."""

    matches = tuple(_TIMESTAMP_PATTERN.finditer(strip_known_suffixes(filename)))
    if not matches:
        return None
    match = matches[-1]
    date_value = match.group("date")
    time_value = match.group("time") or ""
    try:
        return parse_source_datetime(f"{date_value}{time_value}")
    except DomainValidationError:
        return None


def canonical_remote_id(url: str, filename: str | None = None) -> str:
    """Build a stable remote identity without expiring signatures or fragments."""

    parsed = urlsplit(url)
    basename = safe_basename(filename or parsed.path)
    if parsed.scheme.casefold() == "fixture":
        return f"fixture:{basename}"
    host = (parsed.hostname or "").casefold()
    if not host:
        raise DomainValidationError("Remote URL has no host")
    path = parsed.path or f"/{basename}"
    return f"{host}{path}"


def timestamp_from_listing(value: str) -> datetime:
    """Parse Shufersal-style US listing timestamps as Israel local time."""

    try:
        # Validate this portal's exact display format before applying the shared
        # gap/fold-safe publisher-local timezone rule.
        datetime.strptime(value.strip(), "%m/%d/%Y %I:%M:%S %p")  # noqa: DTZ007
        parsed = parse_source_datetime(value)
    except (DomainValidationError, ValueError) as error:
        raise DomainValidationError("Listing timestamp is not in the expected format") from error
    if parsed is None:
        raise DomainValidationError("Listing timestamp is not in the expected format")
    return parsed
