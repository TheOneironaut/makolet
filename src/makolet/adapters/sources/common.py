"""Shared, source-agnostic discovery values and hostile-listing helpers."""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlsplit, urlunsplit

from makolet.domain.enums import CompressionFormat, SourceProtocol
from makolet.domain.errors import DomainValidationError, SourceResponseError, UnsafeRemoteError
from makolet.domain.filenames import (
    canonical_remote_id,
    detect_document_type,
    extract_source_timestamp,
    infer_compression,
    safe_basename,
)
from makolet.domain.models import RemoteFile
from makolet.domain.normalization import parse_source_datetime

MAXIMUM_CURSOR_BYTES = 2_048
MAXIMUM_FILENAME_BYTES = 1_024
MAXIMUM_LISTING_BYTES = 8 * 1024 * 1024
MAXIMUM_LISTING_RECORDS = 20_000
MAXIMUM_HTML_ATTRIBUTES = 64
MAXIMUM_HTML_TAG_CHARACTERS = 64 * 1024
MAXIMUM_HTML_TAGS = 200_000
MAXIMUM_JSON_DEPTH = 64
MAXIMUM_JSON_STRING_CHARACTERS = 64 * 1024
MAXIMUM_JSON_STRUCTURAL_TOKENS = 500_000
MAXIMUM_JSON_SCALAR_CHARACTERS = 4_096
_RELEVANT_HTML_CONTAINERS = frozenset(
    {
        "article",
        "aside",
        "body",
        "details",
        "dialog",
        "div",
        "fieldset",
        "footer",
        "form",
        "head",
        "header",
        "html",
        "main",
        "nav",
        "ol",
        "script",
        "section",
        "style",
        "table",
        "template",
        "textarea",
        "title",
        "ul",
    }
)
_SOURCE_NAME_PATTERN = re.compile(
    r"(?i)\b(?:prices?full|prices?|promos?(?:tions?)?full|promos?(?:tions?)?|stores?full|stores?)"
    r"[^\s<>\"']{1,240}"
)


@dataclass(frozen=True, slots=True)
class HtmlLink:
    href: str
    text: str
    attributes: tuple[tuple[str, str], ...]
    row_text: str

    def attribute(self, name: str) -> str | None:
        sought = name.casefold()
        return next((value for key, value in self.attributes if key.casefold() == sought), None)


@dataclass(slots=True)
class _OpenLink:
    href: str
    attributes: tuple[tuple[str, str], ...]
    text: list[str]
    text_characters: int = 0


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[HtmlLink] = []
        self._row_depth = 0
        self._row_text: list[str] = []
        self._row_text_characters = 0
        self._row_links: list[_OpenLink] = []
        self._open_link: _OpenLink | None = None
        self._open_containers: list[str] = []
        self._saw_start_tag = False
        self._link_count = 0
        self._tag_count = 0

    def handle_starttag(self, tag: str, attributes: list[tuple[str, str | None]]) -> None:
        self._require_bounded_markup(self.get_starttag_text())
        self._saw_start_tag = True
        self._tag_count += 1
        if self._tag_count > MAXIMUM_HTML_TAGS:
            raise SourceResponseError("Source listing HTML contains too many tags")
        if len(attributes) > MAXIMUM_HTML_ATTRIBUTES:
            raise SourceResponseError("Source listing HTML tag contains too many attributes")
        lowered = tag.casefold()
        if lowered in _RELEVANT_HTML_CONTAINERS:
            self._open_containers.append(lowered)
        if lowered == "tr":
            self._row_depth += 1
            if self._row_depth == 1:
                self._row_text = []
                self._row_text_characters = 0
                self._row_links = []
        if lowered != "a":
            return
        if self._open_link is not None:
            raise SourceResponseError("Source listing HTML contains nested anchors")
        self._link_count += 1
        if self._link_count > MAXIMUM_LISTING_RECORDS:
            raise SourceResponseError("Source listing contains too many links")
        clean_attributes = tuple((key, value or "") for key, value in attributes)
        href = next((value for key, value in clean_attributes if key.casefold() == "href"), "")
        link = _OpenLink(href, clean_attributes, [])
        self._open_link = link
        if self._row_depth:
            self._row_links.append(link)

    def handle_data(self, data: str) -> None:
        if self._row_depth:
            self._row_text_characters = _append_bounded_text(
                self._row_text,
                self._row_text_characters,
                data,
            )
        if self._open_link is not None:
            self._open_link.text_characters = _append_bounded_text(
                self._open_link.text,
                self._open_link.text_characters,
                data,
            )

    def handle_endtag(self, tag: str) -> None:
        self._require_bounded_markup(f"</{tag}>")
        lowered = tag.casefold()
        if lowered == "a":
            if self._open_link is not None and not self._row_depth:
                normalized = _collapse_text("".join(self._open_link.text))
                self.links.append(
                    HtmlLink(
                        self._open_link.href,
                        normalized,
                        self._open_link.attributes,
                        normalized,
                    )
                )
            self._open_link = None
            return
        if lowered != "tr" or not self._row_depth:
            if lowered in _RELEVANT_HTML_CONTAINERS:
                if not self._open_containers or self._open_containers[-1] != lowered:
                    raise SourceResponseError("Source listing HTML is malformed")
                self._open_containers.pop()
            return
        self._row_depth -= 1
        if self._row_depth:
            return
        row_text = _collapse_text(" ".join(self._row_text))
        self.links.extend(
            HtmlLink(
                link.href,
                _collapse_text("".join(link.text)),
                link.attributes,
                row_text,
            )
            for link in self._row_links
        )
        self._row_text = []
        self._row_text_characters = 0
        self._row_links = []

    def handle_comment(self, data: str) -> None:
        self._require_bounded_markup(f"<!--{data}-->")

    def handle_decl(self, declaration: str) -> None:
        self._require_bounded_markup(f"<!{declaration}>")

    def handle_pi(self, data: str) -> None:
        self._require_bounded_markup(f"<?{data}>")

    def unknown_decl(self, data: str) -> None:
        self._require_bounded_markup(f"<![{data}]>")

    def require_bounded_pending_markup(self) -> None:
        pending = self._pending_markup()
        if pending is not None:
            self._require_bounded_markup(pending)

    def require_complete_lexical_input(self) -> None:
        if self._pending_markup() is not None:
            raise SourceResponseError("Source listing HTML ended inside markup")

    def require_complete_structure(self) -> None:
        open_containers_are_incomplete = self._open_containers not in ([], ["html"])
        if self._open_link is not None or self._row_depth or open_containers_are_incomplete:
            raise SourceResponseError(
                "Source listing HTML ended before relevant structure was closed"
            )

    def require_html_markup(self) -> None:
        if not self._saw_start_tag:
            raise SourceResponseError("Source listing contains no HTML markup")

    def _pending_markup(self) -> str | None:
        pending = self.rawdata.lstrip()
        return pending if pending.startswith("<") else None

    @staticmethod
    def _require_bounded_markup(markup: str | None) -> None:
        if markup is not None and len(markup) > MAXIMUM_HTML_TAG_CHARACTERS:
            raise SourceResponseError("Source listing HTML contains an oversized tag")


def parse_html_links(body: bytes) -> tuple[HtmlLink, ...]:
    """Parse links without executing scripts or tolerating unbounded text."""

    _validate_listing_size(body)
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise SourceResponseError("Source listing HTML is not UTF-8") from error
    _validate_html_tag_lengths(text)
    parser = _LinkParser()
    try:
        for offset in range(0, len(text), 64 * 1024):
            parser.feed(text[offset : offset + 64 * 1024])
            parser.require_bounded_pending_markup()
        parser.require_complete_lexical_input()
        parser.close()
        parser.require_html_markup()
        parser.require_complete_structure()
    except SourceResponseError:
        raise
    except (AssertionError, ValueError) as error:
        raise SourceResponseError("Source listing HTML is malformed") from error
    return tuple(parser.links)


def filename_from_link(link: HtmlLink) -> str | None:
    """Recover a publisher filename while preserving its original spelling."""

    candidates = (
        link.attribute("data-filename"),
        link.attribute("data-file-name"),
        link.attribute("download"),
        _basename_or_none(link.href),
        _source_name_from_text(link.row_text),
        link.text,
    )
    for candidate in candidates:
        if candidate is None:
            continue
        cleaned = candidate.strip()
        if not cleaned or len(cleaned.encode("utf-8")) > MAXIMUM_FILENAME_BYTES:
            continue
        try:
            basename = safe_basename(cleaned)
        except DomainValidationError:
            continue
        if _looks_like_source_filename(basename):
            return basename
    return None


def listing_page_numbers(links: Iterable[HtmlLink], parameter: str = "page") -> tuple[int, ...]:
    pages: set[int] = set()
    for link in links:
        values = parse_qs(urlsplit(link.href).query).get(parameter, ())
        for value in values:
            try:
                page = int(value)
            except ValueError:
                continue
            if 1 <= page <= 10_000:
                pages.add(page)
        total = link.attribute("data-total-pages")
        if total is not None:
            try:
                maximum = int(total)
            except ValueError:
                continue
            if 1 <= maximum <= 10_000:
                pages.add(maximum)
    return tuple(sorted(pages))


def parse_json_listing(body: bytes) -> object:
    _validate_listing_size(body)
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise SourceResponseError("Source listing JSON is not UTF-8") from error
    _validate_json_shape(text)
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise SourceResponseError("Source listing returned malformed JSON") from error


def bounded_json_value_span(text: str, start: int = 0) -> tuple[int, int]:
    """Preflight one JSON value and return its exact decoder span."""

    if not 0 <= start <= len(text):
        raise SourceResponseError("Source listing returned malformed JSON")
    value_start = start
    while value_start < len(text) and text[value_start] in " \t\r\n":
        value_start += 1
    if value_start == len(text):
        raise SourceResponseError("Source listing returned malformed JSON")
    first = text[value_start]
    if first in "[{":
        value_end = _bounded_json_container_end(text, value_start)
    elif first == '"':
        value_end = _bounded_json_string_end(text, value_start)
    else:
        value_end = _bounded_json_scalar_end(text, value_start)
    return value_start, value_end


def json_records(
    payload: object, collection_names: Sequence[str]
) -> tuple[Mapping[str, object], ...]:
    if collection_names:
        if not isinstance(payload, Mapping):
            raise SourceResponseError("Source listing JSON has no expected record collection")
        selected_keys = tuple(name for name in dict.fromkeys(collection_names) if name in payload)
        if len(selected_keys) != 1:
            raise SourceResponseError("Source listing JSON has no expected record collection")
        raw_records = payload[selected_keys[0]]
    else:
        raw_records = payload
    if not isinstance(raw_records, list):
        raise SourceResponseError("Source listing JSON has no record collection")
    if len(raw_records) > MAXIMUM_LISTING_RECORDS:
        raise SourceResponseError("Source listing JSON contains too many records")
    records: list[Mapping[str, object]] = []
    for raw_record in raw_records:
        if not isinstance(raw_record, Mapping):
            raise SourceResponseError("Source listing JSON contains a non-object record")
        records.append({str(key): value for key, value in raw_record.items()})
    return tuple(records)


def mapping_value(record: Mapping[str, object], *names: str) -> object | None:
    folded = {key.casefold(): value for key, value in record.items()}
    return next((folded[name.casefold()] for name in names if name.casefold() in folded), None)


def required_text(record: Mapping[str, object], *names: str) -> str:
    value = mapping_value(record, *names)
    if not isinstance(value, str) or not value.strip():
        raise SourceResponseError(f"Source listing record omitted {names[0]}")
    return value.strip()


def optional_nonnegative_int(record: Mapping[str, object], *names: str) -> int | None:
    value = mapping_value(record, *names)
    if value in {None, ""}:
        return None
    if isinstance(value, bool):
        raise SourceResponseError(f"Source listing field {names[0]} is not an integer")
    try:
        parsed = int(value) if isinstance(value, int | str) else int("")
    except ValueError as error:
        raise SourceResponseError(f"Source listing field {names[0]} is not an integer") from error
    if parsed < 0:
        raise SourceResponseError(f"Source listing field {names[0]} is negative")
    return parsed


def optional_listing_timestamp(value: object | None) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return parse_source_datetime(value.strip())
    except DomainValidationError:
        # Listing modification times are optional transport hints. An invalid,
        # nonexistent, or ambiguous value must not discard an otherwise valid file.
        return None


def build_remote_file(
    *,
    retailer_id: str,
    portal_id: str,
    download_url: str,
    original_filename: str,
    discovered_at: datetime,
    allowed_hosts: frozenset[str],
    allowed_schemes: frozenset[str],
    content_length: int | None = None,
    source_timestamp: datetime | None = None,
    last_modified: datetime | None = None,
    media_type: str | None = None,
) -> RemoteFile:
    validate_discovered_url(
        download_url,
        allowed_hosts=allowed_hosts,
        allowed_schemes=allowed_schemes,
    )
    filename = safe_basename(original_filename)
    parsed = urlsplit(download_url)
    protocol = SourceProtocol(parsed.scheme.casefold())
    return RemoteFile(
        retailer_id=retailer_id,
        portal_id=portal_id,
        protocol=protocol,
        remote_id=canonical_remote_id(download_url, filename),
        download_url=download_url,
        original_filename=filename,
        document_type=detect_document_type(filename),
        compression=infer_compression(filename),
        discovered_at=discovered_at,
        source_timestamp=source_timestamp or extract_source_timestamp(filename),
        content_length=content_length,
        media_type=media_type,
        last_modified=last_modified,
    )


def validate_discovered_url(
    url: str,
    *,
    allowed_hosts: frozenset[str],
    allowed_schemes: frozenset[str],
) -> None:
    parsed = urlsplit(url)
    scheme = parsed.scheme.casefold()
    host = (parsed.hostname or "").casefold().rstrip(".")
    normalized_hosts = {value.casefold().rstrip(".") for value in allowed_hosts}
    if scheme not in allowed_schemes or host not in normalized_hosts:
        raise UnsafeRemoteError("Discovered source URL is outside its portal allowlist")
    if parsed.username is not None or parsed.password is not None:
        raise UnsafeRemoteError("Discovered source URL must not contain credentials")
    default_port = {"http": 80, "https": 443, "ftp": 21, "ftps": 21}.get(scheme)
    try:
        port = parsed.port
    except ValueError as error:
        raise UnsafeRemoteError("Discovered source URL has an invalid port") from error
    if port is not None and port != default_port:
        raise UnsafeRemoteError("Discovered source URL has a non-default port")


def absolute_url(base_url: str, href: str) -> str:
    return urljoin(base_url, href.strip())


def append_query(url: str, values: Sequence[tuple[str, str]]) -> str:
    parsed = urlsplit(url)
    existing = parse_qs(parsed.query, keep_blank_values=True)
    pairs = [(key, item) for key, items in existing.items() for item in items]
    pairs.extend(values)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(pairs), ""))


def quoted_child_url(base_url: str, filename: str) -> str:
    safe_name = safe_basename(filename)
    return absolute_url(base_url.rstrip("/") + "/", quote(safe_name, safe=""))


def encode_cursor(source_id: str, state: Mapping[str, int | str]) -> str:
    payload = {"v": 1, "source": source_id, **state}
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(serialized).rstrip(b"=").decode("ascii")


def decode_cursor(source_id: str, value: str | None) -> Mapping[str, object]:
    if value is None:
        return {}
    if not value or len(value.encode("ascii", errors="ignore")) > MAXIMUM_CURSOR_BYTES:
        raise DomainValidationError("Discovery cursor is empty or too large")
    try:
        padding = "=" * (-len(value) % 4)
        raw = base64.b64decode(value + padding, altchars=b"-_", validate=True)
        decoded = json.loads(raw)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DomainValidationError("Discovery cursor is invalid") from error
    if not isinstance(decoded, dict) or decoded.get("v") != 1:
        raise DomainValidationError("Discovery cursor has an unsupported version")
    if decoded.get("source") != source_id:
        raise DomainValidationError("Discovery cursor belongs to another source")
    return {str(key): item for key, item in decoded.items()}


def cursor_int(
    state: Mapping[str, object], name: str, *, default: int = 0, maximum: int = 1_000_000
) -> int:
    value = state.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise DomainValidationError(f"Discovery cursor field {name} is invalid")
    return value


def cursor_text(state: Mapping[str, object], name: str) -> str | None:
    value = state.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 128:
        raise DomainValidationError(f"Discovery cursor field {name} is invalid")
    return value


def validate_limit(limit: int, *, maximum: int = 500) -> None:
    if isinstance(limit, bool) or not 1 <= limit <= maximum:
        raise DomainValidationError(f"Discovery limit must be between 1 and {maximum}")


def deduplicate_files(files: Iterable[RemoteFile]) -> tuple[RemoteFile, ...]:
    by_identity: dict[str, RemoteFile] = {}
    for remote_file in files:
        by_identity.setdefault(remote_file.remote_id, remote_file)
    return tuple(sorted(by_identity.values(), key=lambda item: item.remote_id))


def _basename_or_none(value: str) -> str | None:
    try:
        return safe_basename(value)
    except DomainValidationError:
        return None


def _source_name_from_text(value: str) -> str | None:
    match = _SOURCE_NAME_PATTERN.search(value)
    return match.group(0) if match else None


def _looks_like_source_filename(value: str) -> bool:
    compression = infer_compression(value)
    return detect_document_type(value).value != "unknown" or compression in {
        CompressionFormat.GZIP,
        CompressionFormat.ZIP,
        CompressionFormat.ZSTANDARD,
    }


def _collapse_text(value: str) -> str:
    return " ".join(value.split())[:4_096]


def _validate_listing_size(body: bytes) -> None:
    if len(body) > MAXIMUM_LISTING_BYTES:
        raise SourceResponseError("Source listing exceeds the parser byte limit")


def _append_bounded_text(parts: list[str], current: int, value: str) -> int:
    remaining = 4_096 - current
    if remaining <= 0:
        return current
    selected = value[:remaining]
    if selected:
        parts.append(selected)
    return current + len(selected)


def _validate_html_tag_lengths(text: str) -> None:
    tag_start: int | None = None
    for index, character in enumerate(text):
        if character == "<" and tag_start is None:
            tag_start = index
        elif character == ">" and tag_start is not None:
            if index - tag_start + 1 > MAXIMUM_HTML_TAG_CHARACTERS:
                raise SourceResponseError("Source listing HTML contains an oversized tag")
            tag_start = None
        elif tag_start is not None and index - tag_start + 1 > MAXIMUM_HTML_TAG_CHARACTERS:
            raise SourceResponseError("Source listing HTML contains an oversized tag")


def _bounded_json_container_end(text: str, start: int) -> int:
    depth = 0
    structural_tokens = 0
    in_string = False
    escaped = False
    string_characters = 0
    scalar_characters = 0
    value_bytes = 0
    for index in range(start, len(text)):
        character = text[index]
        value_bytes = _bounded_json_byte_count(value_bytes, character)
        if in_string:
            if escaped:
                escaped = False
                string_characters = _bounded_json_string_character_count(string_characters)
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
                string_characters = 0
            else:
                string_characters = _bounded_json_string_character_count(string_characters)
            continue

        if character == '"':
            in_string = True
            scalar_characters = 0
        elif character in "[{":
            depth += 1
            structural_tokens += 1
            scalar_characters = 0
            if depth > MAXIMUM_JSON_DEPTH:
                raise SourceResponseError("Source listing JSON is nested too deeply")
        elif character in "]}":
            depth -= 1
            structural_tokens += 1
            scalar_characters = 0
            if depth < 0:
                raise SourceResponseError("Source listing returned malformed JSON")
            if structural_tokens > MAXIMUM_JSON_STRUCTURAL_TOKENS:
                raise SourceResponseError("Source listing JSON is structurally too complex")
            if depth == 0:
                return index + 1
        elif character in ",:":
            structural_tokens += 1
            scalar_characters = 0
        elif character.isspace():
            scalar_characters = 0
        else:
            scalar_characters += 1
            if scalar_characters > MAXIMUM_JSON_SCALAR_CHARACTERS:
                raise SourceResponseError("Source listing JSON contains an oversized scalar")

        if structural_tokens > MAXIMUM_JSON_STRUCTURAL_TOKENS:
            raise SourceResponseError("Source listing JSON is structurally too complex")

    raise SourceResponseError("Source listing returned malformed JSON")


def _bounded_json_string_end(text: str, start: int) -> int:
    escaped = False
    string_characters = 0
    value_bytes = _bounded_json_byte_count(0, '"')
    for index in range(start + 1, len(text)):
        character = text[index]
        value_bytes = _bounded_json_byte_count(value_bytes, character)
        if escaped:
            escaped = False
            string_characters = _bounded_json_string_character_count(string_characters)
        elif character == "\\":
            escaped = True
        elif character == '"':
            return index + 1
        else:
            string_characters = _bounded_json_string_character_count(string_characters)
    raise SourceResponseError("Source listing returned malformed JSON")


def _bounded_json_scalar_end(text: str, start: int) -> int:
    value_bytes = 0
    for index in range(start, len(text)):
        character = text[index]
        if character in " \t\r\n;<>(),]}":
            if index == start:
                raise SourceResponseError("Source listing returned malformed JSON")
            return index
        value_bytes = _bounded_json_byte_count(value_bytes, character)
        if index - start + 1 > MAXIMUM_JSON_SCALAR_CHARACTERS:
            raise SourceResponseError("Source listing JSON contains an oversized scalar")
    return len(text)


def _bounded_json_byte_count(current: int, character: str) -> int:
    codepoint = ord(character)
    character_bytes = 1 if codepoint <= 0x7F else 2 if codepoint <= 0x7FF else 3
    if codepoint > 0xFFFF:
        character_bytes = 4
    total = current + character_bytes
    if total > MAXIMUM_LISTING_BYTES:
        raise SourceResponseError("Source listing exceeds the parser byte limit")
    return total


def _bounded_json_string_character_count(current: int) -> int:
    total = current + 1
    if total > MAXIMUM_JSON_STRING_CHARACTERS:
        raise SourceResponseError("Source listing JSON contains an oversized string")
    return total


def _validate_json_shape(text: str) -> None:
    _value_start, value_end = bounded_json_value_span(text)
    for index in range(value_end, len(text)):
        if text[index] not in " \t\r\n":
            raise SourceResponseError("Source listing returned malformed JSON")
