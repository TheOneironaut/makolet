"""Streaming parser for the regulated Israeli retail XML families."""

from __future__ import annotations

import re
import tempfile
from collections import deque
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID
from xml.etree import ElementTree as ET
from xml.parsers import expat

import anyio

from makolet.adapters.filesystem_capacity import (
    FileSystemCapacityGuard,
    FileSystemCapacityUnavailableError,
)
from makolet.adapters.parsers.encoding import XmlEncodingProbe, incremental_decoder
from makolet.adapters.parsers.streams import (
    CompressionLimits,
    decompress_chunks,
    sync_spooled_file,
)
from makolet.domain.enums import CompressionFormat, DiscountKind, DocumentType, IssueSeverity
from makolet.domain.errors import (
    ArchiveCapacityError,
    DomainValidationError,
    MalformedDocumentError,
    UnsafeArchiveError,
)
from makolet.domain.models import (
    DocumentMetadata,
    ParsedEvent,
    PriceRecord,
    PromotionItem,
    PromotionRecord,
    StoreRecord,
    ValidationIssue,
)
from makolet.domain.normalization import (
    clean_source_text,
    combine_source_date_time,
    normalize_identifier,
    parse_bool,
    parse_decimal,
    parse_int,
    parse_source_datetime,
)

_NORMALIZE_TAG = re.compile(r"[^a-z0-9]+")
_SECURITY_MARKERS = (b"<!DOCTYPE", b"<!ENTITY")
_HTML_PREFIXES = (b"<HTML", b"<!DOCTYPEHTML")
_MINIMUM_EXPAT = (2, 7, 2)
_MARKUP_SPECIAL = re.compile(r"['\">]")
_TOKEN_TEXT = 0
_TOKEN_OPEN = 1
_TOKEN_ELEMENT = 2
_TOKEN_DECLARATION = 3
_TOKEN_COMMENT = 4
_TOKEN_CDATA = 5
_TOKEN_PROCESSING_INSTRUCTION = 6
_MAXIMUM_SCHEMA_FIELD_NAMES = 32
_MAXIMUM_SCHEMA_FIELD_NAME_CHARACTERS = 256
_MAXIMUM_SCHEMA_WARNING_EVIDENCE_CHARACTERS = 2_000


@dataclass(frozen=True, slots=True)
class XmlParserLimits:
    compression: CompressionLimits = field(default_factory=CompressionLimits)
    maximum_depth: int = 64
    maximum_elements: int = 20_000_000
    maximum_records: int = 5_000_000
    maximum_record_bytes: int = 2 * 1024 * 1024
    maximum_field_characters: int = 10_000
    maximum_token_characters: int = 64 * 1024
    temporary_directory: Path | None = None
    minimum_free_bytes: int = 0

    def __post_init__(self) -> None:
        values = (
            self.maximum_depth,
            self.maximum_elements,
            self.maximum_records,
            self.maximum_record_bytes,
            self.maximum_field_characters,
            self.maximum_token_characters,
        )
        if any(value <= 0 for value in values):
            raise ValueError("XML parser limits must be positive")
        if self.minimum_free_bytes < 0:
            raise ValueError("XML parser free-space reserve cannot be negative")
        if self.temporary_directory is not None:
            object.__setattr__(self, "temporary_directory", self.temporary_directory.resolve())


@dataclass(slots=True)
class _SchemaFindings:
    """Retain a fixed-size summary of additive XML schema observations."""

    maximum_evidence_characters: int
    count: int = 0
    field_names: list[str] = field(default_factory=list)
    evidence: str = ""
    has_discount_amount: bool = False
    has_conditional_field: bool = False

    def add(
        self,
        path: tuple[str, ...],
        value: str | None = None,
        *,
        semantic_tag: str | None = None,
    ) -> None:
        self.count += 1
        tag = semantic_tag or (path[-1] if path else "")
        self.has_discount_amount |= tag == "discountamount"
        self.has_conditional_field |= tag in {
            "condition",
            "conditiondescription",
            "eligibility",
        }
        name = ".".join(path)[:_MAXIMUM_SCHEMA_FIELD_NAME_CHARACTERS]
        if (
            name
            and len(self.field_names) < _MAXIMUM_SCHEMA_FIELD_NAMES
            and name not in self.field_names
        ):
            self.field_names.append(name)
        if len(self.evidence) >= self.maximum_evidence_characters:
            return
        piece = name if value is None else f"{name}={value}"
        separator = "; " if self.evidence else ""
        remaining = self.maximum_evidence_characters - len(self.evidence)
        self.evidence += f"{separator}{piece}"[:remaining]

    @property
    def field_name(self) -> str | None:
        value = ",".join(self.field_names)[:_MAXIMUM_SCHEMA_FIELD_NAME_CHARACTERS]
        return value or None

    @property
    def warning_evidence(self) -> str | None:
        value = self.evidence[:_MAXIMUM_SCHEMA_WARNING_EVIDENCE_CHARACTERS]
        return value or None


@dataclass(slots=True)
class _ParseState:
    depth: int = 0
    elements: int = 0
    records: int = 0
    decompressed_bytes: int = 0
    current_feed_start: int = 0
    active_record_depth: int | None = None
    active_record_start: int = 0
    active_record_earliest_start: int = 0
    root_seen: bool = False
    containers: int = 0
    container_depth: int | None = None
    element_stack: list[ET.Element[str]] = field(default_factory=list)
    context_previous_by_depth: dict[int, dict[str, str | None]] = field(default_factory=dict)
    document_findings: _SchemaFindings = field(
        default_factory=lambda: _SchemaFindings(_MAXIMUM_SCHEMA_WARNING_EVIDENCE_CHARACTERS)
    )


class _XmlTokenGuard:
    """Bound lexical tokens before Expat can retain an unfinished construct."""

    def __init__(self, limits: XmlParserLimits) -> None:
        self._maximum_field_characters = limits.maximum_field_characters
        self._maximum_token_characters = limits.maximum_token_characters
        self._mode = _TOKEN_TEXT
        self._token_characters = 0
        self._text_characters = 0
        self._quote: str | None = None
        self._quoted_characters = 0
        self._declaration_prefix = ""

    def feed(self, text: str) -> None:
        position = 0
        while position < len(text):
            if self._mode == _TOKEN_TEXT:
                position = self._scan_text(text, position)
            elif self._mode == _TOKEN_OPEN:
                position = self._scan_open(text, position)
            elif self._mode == _TOKEN_ELEMENT:
                position = self._scan_element(text, position)
            elif self._mode == _TOKEN_DECLARATION:
                position = self._scan_declaration(text, position)
            elif self._mode == _TOKEN_COMMENT:
                position = self._scan_delimited(text, position, "-->")
            elif self._mode == _TOKEN_CDATA:
                position = self._scan_cdata(text, position)
            else:
                position = self._scan_delimited(text, position, "?>")

    def close(self) -> None:
        if self._mode != _TOKEN_TEXT:
            raise MalformedDocumentError("XML document ended inside an unfinished token")

    def _scan_text(self, text: str, position: int) -> int:
        markup_start = text.find("<", position)
        if markup_start < 0:
            self._add_text(len(text) - position)
            return len(text)
        self._add_text(markup_start - position)
        self._mode = _TOKEN_OPEN
        self._token_characters = 1
        self._text_characters = 0
        self._declaration_prefix = "<"
        return markup_start + 1

    def _scan_open(self, text: str, position: int) -> int:
        character = text[position]
        if character == "?":
            self._add_token(1)
            self._mode = _TOKEN_PROCESSING_INSTRUCTION
            return position + 1
        if character == "!":
            self._add_token(1)
            self._mode = _TOKEN_DECLARATION
            self._declaration_prefix = "<!"
            return position + 1
        self._mode = _TOKEN_ELEMENT
        return position

    def _scan_element(self, text: str, position: int) -> int:
        if self._quote is not None:
            quote_end = text.find(self._quote, position)
            content_end = len(text) if quote_end < 0 else quote_end
            characters = content_end - position
            self._add_token(characters)
            self._add_quoted(characters)
            if quote_end < 0:
                return len(text)
            self._add_token(1)
            self._quote = None
            self._quoted_characters = 0
            return quote_end + 1

        match = _MARKUP_SPECIAL.search(text, position)
        if match is None:
            self._add_token(len(text) - position)
            return len(text)
        self._add_token(match.start() - position + 1)
        character = match.group()
        if character == ">":
            self._finish_markup()
        else:
            self._quote = character
            self._quoted_characters = 0
        return match.end()

    def _scan_declaration(self, text: str, position: int) -> int:
        character = text[position]
        self._add_token(1)
        self._declaration_prefix += character
        position += 1
        candidates = ("<!--", "<![CDATA[")
        if self._declaration_prefix == candidates[0]:
            self._mode = _TOKEN_COMMENT
        elif self._declaration_prefix == candidates[1]:
            self._mode = _TOKEN_CDATA
            self._text_characters = 0
        elif not any(candidate.startswith(self._declaration_prefix) for candidate in candidates):
            self._mode = _TOKEN_ELEMENT
        return position

    def _scan_delimited(self, text: str, position: int, delimiter: str) -> int:
        token_end = text.find(delimiter, position)
        content_end = len(text) if token_end < 0 else token_end + len(delimiter)
        self._add_token(content_end - position)
        if token_end < 0:
            return len(text)
        self._finish_markup()
        return content_end

    def _scan_cdata(self, text: str, position: int) -> int:
        token_end = text.find("]]>", position)
        content_end = len(text) if token_end < 0 else token_end
        characters = content_end - position
        self._add_text(characters)
        self._add_token(characters)
        if token_end < 0:
            return len(text)
        self._add_token(3)
        self._finish_markup()
        return token_end + 3

    def _add_text(self, characters: int) -> None:
        self._text_characters += characters
        if self._text_characters > self._maximum_field_characters:
            raise MalformedDocumentError("XML character data exceeds the field limit")

    def _add_quoted(self, characters: int) -> None:
        self._quoted_characters += characters
        if self._quoted_characters > self._maximum_field_characters:
            raise MalformedDocumentError("XML attribute value exceeds the field limit")

    def _add_token(self, characters: int) -> None:
        self._token_characters += characters
        if self._token_characters > self._maximum_token_characters:
            raise MalformedDocumentError("XML token exceeds the tokenizer limit")

    def _finish_markup(self) -> None:
        self._mode = _TOKEN_TEXT
        self._token_characters = 0
        self._text_characters = 0
        self._quote = None
        self._quoted_characters = 0
        self._declaration_prefix = ""


class _BoundedTreeTarget:
    """Create ElementTree events while bounding every retained text destination."""

    def __init__(self, limits: XmlParserLimits) -> None:
        self._builder = ET.TreeBuilder()
        self._events: deque[tuple[str, ET.Element[str]]] = deque()
        self._maximum_field_characters = limits.maximum_field_characters
        self._maximum_token_characters = limits.maximum_token_characters
        self._data_characters = 0

    def start(self, tag: str, attributes: dict[str, str]) -> ET.Element[str]:
        retained_characters = len(tag)
        for name, value in attributes.items():
            retained_characters += len(name) + len(value)
            if len(value) > self._maximum_field_characters:
                raise MalformedDocumentError("XML attribute value exceeds the field limit")
        if retained_characters > self._maximum_token_characters:
            raise MalformedDocumentError("XML start tag exceeds the tokenizer limit")
        self._data_characters = 0
        element = self._builder.start(tag, attributes)
        self._events.append(("start", element))
        return element

    def end(self, tag: str) -> ET.Element[str]:
        element = self._builder.end(tag)
        self._events.append(("end", element))
        self._data_characters = 0
        return element

    def data(self, data: str) -> None:
        self._data_characters += len(data)
        if self._data_characters > self._maximum_field_characters:
            raise MalformedDocumentError("XML character data exceeds the field limit")
        self._builder.data(data)

    def comment(self, text: str) -> ET.Element[str] | None:
        return self._builder.comment(text)

    def pi(self, target: str, text: str | None) -> ET.Element[str] | None:
        return self._builder.pi(target, text)

    def close(self) -> ET.Element[str]:
        return self._builder.close()

    def read_events(self) -> Iterator[tuple[str, ET.Element[str]]]:
        while self._events:
            yield self._events.popleft()


class RetailXmlParser:
    """Parse Stores, Price/PriceFull, and Promo/PromoFull XML without network access."""

    parser_version = "retail-xml/10"

    def __init__(self, limits: XmlParserLimits | None = None) -> None:
        ensure_secure_expat()
        self._limits = limits or XmlParserLimits()

    def parse(
        self,
        chunks: AsyncIterator[bytes],
        *,
        source_file_id: UUID,
        document_type: DocumentType,
        compression: CompressionFormat,
        filename: str,
    ) -> AsyncIterator[ParsedEvent]:
        del filename  # Provenance is retained by the source-file record, not XML values.
        return self._parse(
            chunks,
            source_file_id=source_file_id,
            document_type=document_type,
            compression=compression,
        )

    async def _parse(
        self,
        chunks: AsyncIterator[bytes],
        *,
        source_file_id: UUID,
        document_type: DocumentType,
        compression: CompressionFormat,
    ) -> AsyncIterator[ParsedEvent]:
        record_tag = _record_tag(document_type)
        security_tail = b""
        prefix = bytearray()

        try:
            # A late non-UTF-8 byte can prove that a missing or incorrect declaration
            # describes Windows-1255. Spooling permits a safe replay after validating
            # the complete stream, while the configured threshold bounds memory use.
            spool_path = await anyio.to_thread.run_sync(
                _resolved_parser_spool_directory,
                self._limits.temporary_directory,
            )
            await anyio.to_thread.run_sync(_ensure_spool_directory, spool_path)
            capacity_directory = (
                spool_path.parent if self._limits.temporary_directory is not None else spool_path
            )
            capacity = FileSystemCapacityGuard(
                spool_path,
                minimum_free_bytes=self._limits.minimum_free_bytes,
                coordination_directory=capacity_directory,
            )
            with tempfile.SpooledTemporaryFile(
                max_size=self._limits.compression.spool_memory_bytes,
                mode="w+b",
                prefix="makolet-parser-",
                suffix=".xml",
                dir=str(spool_path),
            ) as spool:
                spooled_bytes = 0
                probe = XmlEncodingProbe()
                decompressed = decompress_chunks(
                    chunks,
                    compression,
                    limits=self._limits.compression,
                    temporary_directory=spool_path,
                    capacity_directory=capacity_directory,
                    minimum_free_bytes=self._limits.minimum_free_bytes,
                )
                async for chunk in decompressed:
                    security_tail = _check_security_markers(security_tail, chunk)
                    if len(prefix) < 1024:
                        prefix.extend(chunk[: 1024 - len(prefix)])
                        _reject_html_prefix(prefix)
                    probe.feed(chunk)
                    next_spooled_bytes = spooled_bytes + len(chunk)
                    if next_spooled_bytes > self._limits.compression.spool_memory_bytes:
                        required_bytes = (
                            next_spooled_bytes
                            if spooled_bytes <= self._limits.compression.spool_memory_bytes
                            else len(chunk)
                        )
                        try:
                            async with capacity.reserve_async(required_bytes):
                                await anyio.to_thread.run_sync(spool.write, chunk)
                                await anyio.to_thread.run_sync(spool.flush)
                        except FileSystemCapacityUnavailableError as error:
                            raise ArchiveCapacityError(
                                "XML parser spool reached its configured free-space reserve"
                            ) from error
                    else:
                        await anyio.to_thread.run_sync(spool.write, chunk)
                    spooled_bytes = next_spooled_bytes

                if spooled_bytes > self._limits.compression.spool_memory_bytes:
                    try:
                        async with capacity.reserve_async(0):
                            await anyio.to_thread.run_sync(sync_spooled_file, spool)
                    except FileSystemCapacityUnavailableError as error:
                        raise ArchiveCapacityError(
                            "XML parser spool reached its configured free-space reserve"
                        ) from error

                codec = probe.resolve()
                await anyio.to_thread.run_sync(spool.seek, 0)
                async for event in self._parse_spooled(
                    spool,
                    codec=codec,
                    source_file_id=source_file_id,
                    document_type=document_type,
                    record_tag=record_tag,
                ):
                    yield event
        except (ET.ParseError, UnicodeError) as error:
            raise MalformedDocumentError(
                "XML document is malformed or incorrectly encoded"
            ) from error
        except UnsafeArchiveError:
            raise

    async def _parse_spooled(
        self,
        spool: tempfile.SpooledTemporaryFile[bytes],
        *,
        codec: str,
        source_file_id: UUID,
        document_type: DocumentType,
        record_tag: str,
    ) -> AsyncIterator[ParsedEvent]:
        target = _BoundedTreeTarget(self._limits)
        # Expat is version-gated; declarations, tokens, depth, and retained text
        # are bounded before the target receives attacker-controlled structures.
        parser = ET.XMLParser(target=target)  # noqa: S314
        token_guard = _XmlTokenGuard(self._limits)
        decoder = incremental_decoder(codec)
        state = _ParseState()
        context: dict[str, str] = {}

        while chunk := await anyio.to_thread.run_sync(
            spool.read, self._limits.compression.maximum_chunk_bytes
        ):
            state.current_feed_start = state.decompressed_bytes
            state.decompressed_bytes += len(chunk)
            decoded = decoder.decode(chunk, final=False)
            if decoded:
                token_guard.feed(decoded)
                parser.feed(decoded)
                for event in self._drain(
                    target.read_events(),
                    state,
                    context,
                    source_file_id,
                    document_type,
                    record_tag,
                    codec,
                ):
                    yield event
            if (
                state.active_record_depth is not None
                and state.decompressed_bytes - state.active_record_start
                > self._limits.maximum_record_bytes
            ):
                raise MalformedDocumentError("XML record exceeds the byte limit")

        final_text = decoder.decode(b"", final=True)
        if final_text:
            token_guard.feed(final_text)
            parser.feed(final_text)
        token_guard.close()
        parser.close()
        for event in self._drain(
            target.read_events(),
            state,
            context,
            source_file_id,
            document_type,
            record_tag,
            codec,
        ):
            yield event

        if not state.root_seen or state.depth != 0:
            raise MalformedDocumentError("XML document did not close its root element")
        if state.active_record_depth is not None:
            raise MalformedDocumentError("XML document ended inside a record")
        if state.container_depth is not None:
            raise MalformedDocumentError("XML document ended inside its record container")
        if state.containers == 0:
            raise MalformedDocumentError("XML document lacks the expected record container")

        if state.document_findings.count:
            yield ValidationIssue(
                source_file_id=source_file_id,
                severity=IssueSeverity.WARNING,
                code="unexpected_xml_field",
                message=(
                    "XML document contains "
                    f"{state.document_findings.count} unrecognized schema field(s)"
                ),
                field_name=state.document_findings.field_name,
                rejected_value=state.document_findings.warning_evidence,
            )
        yield DocumentMetadata(
            source_file_id=source_file_id,
            document_type=document_type,
            chain_id=_value(context, {}, "chainid"),
            subchain_id=_value(context, {}, "subchainid"),
            store_id=_value(context, {}, "storeid"),
            audit_number=_value(context, {}, "bikoretno", "auditnumber"),
            source_updated_at=_date_time(
                context,
                {},
                ("lastupdatedate", "updatedate", "priceupdatedate"),
                ("lastupdatetime", "updatetime", "priceupdatetime"),
            ),
        )

    def _drain(
        self,
        events: Iterator[tuple[str, ET.Element[str]]],
        state: _ParseState,
        context: dict[str, str],
        source_file_id: UUID,
        document_type: DocumentType,
        record_tag: str,
        record_codec: str,
    ) -> Iterator[ParsedEvent]:
        expected_container = _record_container(document_type)
        for event, element in events:
            tag = _tag_name(element.tag)
            if event == "start":
                if state.depth == 0:
                    if state.root_seen or tag != "root":
                        raise MalformedDocumentError(
                            "XML root does not match the regulated document shape"
                        )
                    state.root_seen = True
                state.depth += 1
                state.elements += 1
                state.element_stack.append(element)
                if state.depth > self._limits.maximum_depth:
                    raise MalformedDocumentError("XML nesting exceeds the depth limit")
                if state.elements > self._limits.maximum_elements:
                    raise MalformedDocumentError("XML document exceeds the element limit")
                if tag in _RECORD_CONTAINERS:
                    container_path = tuple(_tag_name(node.tag) for node in state.element_stack)
                    if tag != expected_container:
                        raise MalformedDocumentError(
                            "XML container does not match the declared document type"
                        )
                    if not _is_expected_container_path(
                        container_path,
                        document_type,
                    ):
                        raise MalformedDocumentError(
                            "XML record container is outside an allowed document path"
                        )
                    if (
                        container_path == _NESTED_STORE_CONTAINER_PATH
                        and "subchainid"
                        not in state.context_previous_by_depth.get(state.depth - 1, {})
                    ):
                        raise MalformedDocumentError(
                            "A nested SubChain with Stores must declare its own SubChainID"
                        )
                    if state.container_depth is not None:
                        raise MalformedDocumentError("XML record containers may not be nested")
                    state.containers += 1
                    state.container_depth = state.depth
                if tag == record_tag:
                    if state.active_record_depth is not None:
                        raise MalformedDocumentError("XML records may not be nested")
                    if state.container_depth is None or state.depth != state.container_depth + 1:
                        raise MalformedDocumentError(
                            "XML record is outside the expected record container"
                        )
                    state.active_record_depth = state.depth
                    state.active_record_earliest_start = state.current_feed_start
                    state.active_record_start = state.decompressed_bytes
                continue

            if not state.element_stack or state.element_stack[-1] is not element:
                raise MalformedDocumentError("XML element stack became inconsistent")
            parent = state.element_stack[-2] if len(state.element_stack) > 1 else None
            parsed_events: tuple[ParsedEvent, ...] = ()
            if state.active_record_depth == state.depth and tag == record_tag:
                state.records += 1
                if state.records > self._limits.maximum_records:
                    raise MalformedDocumentError("XML document exceeds the record limit")
                # Feed boundaries give a cheap upper bound for ordinary small
                # records. Serialize only a boundary-straddling record whose upper
                # bound exceeds the limit; the active-record check already stops a
                # definitely oversized record before its closing tag arrives.
                record_upper_bound = state.decompressed_bytes - state.active_record_earliest_start
                if (
                    record_upper_bound > self._limits.maximum_record_bytes
                    and _encoded_record_size(element, record_codec)
                    > self._limits.maximum_record_bytes
                ):
                    raise MalformedDocumentError("XML record exceeds the byte limit")
                try:
                    parsed_events = _parse_record(
                        element,
                        context,
                        source_file_id,
                        document_type,
                        state.records,
                        self._limits.maximum_field_characters,
                    )
                except DomainValidationError as error:
                    parsed_events = (
                        ValidationIssue(
                            source_file_id=source_file_id,
                            severity=IssueSeverity.RECORD_REJECTION,
                            code=error.code,
                            message=str(error),
                            record_index=state.records,
                        ),
                    )
                element.clear()
                state.active_record_depth = None
            elif state.active_record_depth is None:
                path = tuple(_tag_name(node.tag) for node in state.element_stack)
                text = _inspect_document_element(
                    element,
                    path,
                    document_type,
                    state.document_findings,
                    self._limits.maximum_field_characters,
                )
                if text is not None and _is_context_path(path, document_type):
                    context_scope_depth = state.depth - 1
                    if context_scope_depth > 1:
                        previous_context = state.context_previous_by_depth.setdefault(
                            context_scope_depth,
                            {},
                        )
                        if tag not in previous_context:
                            previous_context[tag] = context.get(tag)
                    context[tag] = text
                element.clear()
            if parent is not None and state.active_record_depth is None:
                parent.remove(element)
            if tag == expected_container and state.container_depth == state.depth:
                state.container_depth = None
            if previous_context := state.context_previous_by_depth.pop(state.depth, {}):
                for context_tag, previous_value in previous_context.items():
                    if previous_value is None:
                        context.pop(context_tag, None)
                    else:
                        context[context_tag] = previous_value
            state.element_stack.pop()
            state.depth -= 1
            yield from parsed_events


_CONTEXT_TAGS = frozenset(
    {
        "chainid",
        "subchainid",
        "storeid",
        "bikoretno",
        "auditnumber",
        "lastupdatedate",
        "lastupdatetime",
        "updatedate",
        "updatetime",
        "priceupdatedate",
        "priceupdatetime",
    }
)
_STORE_CONTEXT_TAGS = _CONTEXT_TAGS | frozenset({"chainname", "subchainname"})
_RECORD_CONTAINERS = frozenset({"items", "promotions", "stores"})
_NESTED_STORE_CONTAINER_PATH = (
    "root",
    "chain",
    "subchains",
    "subchain",
    "stores",
)
_STORE_DOCUMENT_WRAPPER_PATHS = frozenset(
    {
        ("root", "chain"),
        ("root", "chain", "subchains"),
        ("root", "chain", "subchains", "subchain"),
    }
)


def _is_expected_container_path(path: tuple[str, ...], document_type: DocumentType) -> bool:
    expected = _record_container(document_type)
    if path == ("root", expected):
        return True
    return document_type is DocumentType.STORES and path == _NESTED_STORE_CONTAINER_PATH


def _is_context_path(path: tuple[str, ...], document_type: DocumentType) -> bool:
    if len(path) == 2 and path[0] == "root":
        tags = _STORE_CONTEXT_TAGS if document_type is DocumentType.STORES else _CONTEXT_TAGS
        return path[1] in tags
    if document_type is not DocumentType.STORES:
        return False
    if len(path) == 3 and path[:2] == ("root", "chain"):
        return path[2] in _STORE_CONTEXT_TAGS - {"subchainid", "subchainname", "storeid"}
    if len(path) == 5 and path[:4] == (
        "root",
        "chain",
        "subchains",
        "subchain",
    ):
        return path[4] in _STORE_CONTEXT_TAGS - {"chainid", "chainname", "storeid"}
    return False


def _is_document_structure_path(path: tuple[str, ...], document_type: DocumentType) -> bool:
    return (
        path == ("root",)
        or _is_expected_container_path(path, document_type)
        or (document_type is DocumentType.STORES and path in _STORE_DOCUMENT_WRAPPER_PATHS)
    )


def _inspect_document_element(
    element: ET.Element[str],
    path: tuple[str, ...],
    document_type: DocumentType,
    findings: _SchemaFindings,
    maximum_field_characters: int,
) -> str | None:
    text = _clean_element_text(element, maximum_field_characters)
    is_context = _is_context_path(path, document_type)
    is_structure = _is_document_structure_path(path, document_type)
    if (is_context and len(element) != 0) or (not is_context and not is_structure):
        findings.add(path, text, semantic_tag=path[-1])
        accepted_text = None
    elif is_structure:
        if text is not None:
            findings.add((*path, "#text"), text, semantic_tag="#text")
        accepted_text = None
    else:
        accepted_text = text
    for attribute_name, raw_value in element.attrib.items():
        name = _tag_name(attribute_name) or "attribute"
        value = clean_source_text(raw_value, max_length=maximum_field_characters)
        findings.add((*path, f"@{name}"), value, semantic_tag=name)
    return accepted_text


def _parse_record(
    element: ET.Element,
    context: dict[str, str],
    source_file_id: UUID,
    document_type: DocumentType,
    record_index: int,
    maximum_field_characters: int,
) -> tuple[ParsedEvent, ...]:
    values = _element_values(element, maximum_field_characters)
    findings = _record_schema_findings(
        element,
        document_type,
        maximum_field_characters,
    )
    if document_type is DocumentType.STORES:
        record: ParsedEvent = _parse_store(values, context, source_file_id, record_index)
    elif document_type.is_price:
        record = _parse_price(values, context, source_file_id, record_index)
    elif document_type.is_promotion:
        record = _parse_promotion(
            element,
            values,
            context,
            source_file_id,
            record_index,
            maximum_field_characters,
            findings,
        )
    else:
        raise DomainValidationError("Unsupported XML document type")
    if not findings.count:
        return (record,)
    return (
        record,
        ValidationIssue(
            source_file_id=source_file_id,
            severity=IssueSeverity.WARNING,
            code="unexpected_xml_field",
            message=f"XML record contains {findings.count} unrecognized schema field(s)",
            record_index=record_index,
            field_name=findings.field_name,
            rejected_value=findings.warning_evidence,
        ),
    )


def _parse_store(
    values: dict[str, str],
    context: dict[str, str],
    source_file_id: UUID,
    record_index: int,
) -> StoreRecord:
    return StoreRecord(
        source_file_id=source_file_id,
        record_index=record_index,
        chain_id=_required(values, context, "chainid"),
        subchain_id=_required(values, context, "subchainid"),
        store_id=_required(values, context, "storeid"),
        audit_number=_value(values, context, "bikoretno", "auditnumber"),
        store_type=_value(values, context, "storetype"),
        chain_name=_value(values, context, "chainname"),
        subchain_name=_value(values, context, "subchainname"),
        store_name=_required(values, context, "storename"),
        address=_value(values, context, "address", "storeaddress"),
        city=_value(values, context, "city"),
        postal_code=_value(values, context, "zipcode", "postalcode"),
    )


def _parse_price(
    values: dict[str, str],
    context: dict[str, str],
    source_file_id: UUID,
    record_index: int,
) -> PriceRecord:
    price = parse_decimal(
        _value(values, context, "itemprice", "price"), field_name="item_price", required=True
    )
    if price is None:  # required=True makes this defensive only.
        raise DomainValidationError("item_price is required")
    return PriceRecord(
        source_file_id=source_file_id,
        record_index=record_index,
        chain_id=_required(values, context, "chainid"),
        subchain_id=_required(values, context, "subchainid"),
        store_id=_required(values, context, "storeid"),
        item_code=normalize_identifier(_required(values, context, "itemcode", "barcode")),
        item_type=parse_int(_value(values, context, "itemtype"), field_name="item_type"),
        item_name=_required(values, context, "itemname", "itemnm"),
        manufacturer_name=_value(values, context, "manufacturername"),
        manufacturer_country=_value(
            values,
            context,
            "manufacturercountry",
            "manufacturecountry",
        ),
        manufacturer_description=_value(values, context, "manufactureritemdescription"),
        unit_quantity=_value(values, context, "unitqty", "unitquantity"),
        quantity=parse_decimal(_value(values, context, "quantity"), field_name="quantity"),
        unit_of_measure=_value(values, context, "unitofmeasure", "unitmeasure"),
        is_weighted=parse_bool(
            _value(values, context, "isweighted", "bisweighted"),
            field_name="is_weighted",
        ),
        quantity_in_package=parse_decimal(
            _value(values, context, "qtyinpackage", "quantityinpackage"),
            field_name="quantity_in_package",
        ),
        item_price=price,
        unit_of_measure_price=parse_decimal(
            _value(values, context, "unitofmeasureprice", "unitprice"),
            field_name="unit_of_measure_price",
        ),
        allow_discount=parse_bool(
            _value(values, context, "allowdiscount"), field_name="allow_discount"
        ),
        item_status=parse_int(
            _value(values, context, "itemstatus", "status"), field_name="item_status"
        ),
        price_updated_at=_date_time(
            values,
            context,
            ("priceupdatedate", "updatedate"),
            ("priceupdatetime", "updatetime"),
        ),
        last_sale_at=_date_time(
            values,
            context,
            ("lastsaledate",),
            ("lastsaletime", "lastsaledatetime"),
        ),
        audit_number=_value(values, context, "bikoretno", "auditnumber"),
    )


def _parse_promotion(
    element: ET.Element,
    values: dict[str, str],
    context: dict[str, str],
    source_file_id: UUID,
    record_index: int,
    maximum_field_characters: int,
    findings: _SchemaFindings,
) -> PromotionRecord:
    description = _value(values, context, "promotiondescription", "description")
    items = _promotion_items(element, maximum_field_characters)
    if not items:
        raise DomainValidationError("Promotion contains no eligible item identifiers")
    discounted_price = parse_decimal(
        _value(values, context, "discountedprice", "promotionprice"),
        field_name="discounted_price",
    )
    discount_rate = parse_decimal(
        _value(values, context, "discountrate", "discountpercent"),
        field_name="discount_rate",
    )
    minimum_quantity = parse_decimal(
        _value(values, context, "minqty", "minimumquantity"),
        field_name="minimum_quantity",
    )
    minimum_purchase = parse_decimal(
        _value(values, context, "minpurchaseamount", "minimumpurchase"),
        field_name="minimum_purchase",
    )
    return PromotionRecord(
        source_file_id=source_file_id,
        record_index=record_index,
        chain_id=_required(values, context, "chainid"),
        subchain_id=_required(values, context, "subchainid"),
        source_store_id=_value(context, {}, "storeid"),
        promotion_id=normalize_identifier(
            _required(values, context, "promotionid", "promotioncode")
        ),
        description=description,
        discount_kind=_discount_kind(
            description,
            discounted_price,
            discount_rate,
            minimum_quantity,
            minimum_purchase,
            items,
            findings,
        ),
        starts_at=_date_time(
            values,
            context,
            ("promotionstartdate", "startdate"),
            ("promotionstarttime", "starttime"),
        ),
        ends_at=_date_time(
            values,
            context,
            ("promotionenddate", "enddate"),
            ("promotionendtime", "endtime"),
        ),
        items=items,
        store_ids=_nested_identifiers(
            element,
            {"promotionstores"},
            {"store", "promotionstore"},
            "storeid",
            relationship_name="store",
            maximum_field_characters=maximum_field_characters,
        ),
        club_ids=_nested_identifiers(
            element,
            {"clubs"},
            {"club", "promotionclub"},
            "clubid",
            relationship_name="club",
            maximum_field_characters=maximum_field_characters,
        ),
        reward_type=parse_int(_value(values, context, "rewardtype"), field_name="reward_type"),
        allows_multiple_discounts=parse_bool(
            _value(values, context, "allowmultiplediscounts"),
            field_name="allows_multiple_discounts",
        ),
        minimum_quantity=minimum_quantity,
        maximum_quantity=parse_decimal(
            _value(values, context, "maxqty", "maximumquantity"),
            field_name="maximum_quantity",
        ),
        discount_rate=discount_rate,
        minimum_purchase=minimum_purchase,
        discounted_price=discounted_price,
        discounted_unit_price=parse_decimal(
            _value(values, context, "discountedpricepermida", "discountedunitprice"),
            field_name="discounted_unit_price",
        ),
        minimum_items_offered=parse_int(
            _value(values, context, "minnoofitemofered", "minimumitemsoffered"),
            field_name="minimum_items_offered",
        ),
        additional_restrictions=_promotion_restrictions(
            _value(values, context, "additionalrestrictions"),
            findings,
            maximum_field_characters,
        ),
        remarks=_value(values, context, "remarks"),
        is_active=parse_bool(_value(values, context, "isactive"), field_name="is_active"),
    )


def _promotion_items(
    element: ET.Element, maximum_field_characters: int
) -> tuple[PromotionItem, ...]:
    result: list[PromotionItem] = []
    seen: set[str] = set()
    containers = [child for child in element if _tag_name(child.tag) == "promotionitems"]
    for container in containers:
        relations = [
            child for child in container if _tag_name(child.tag) in {"item", "promotionitem"}
        ]
        for relation in relations:
            values = _element_values(relation, maximum_field_characters)
            code = _value(values, {}, "itemcode", "barcode")
            if code is None:
                raise DomainValidationError(
                    "Promotion item relationship is missing an item identifier"
                )
            normalized = normalize_identifier(code)
            if normalized in seen:
                continue
            seen.add(normalized)
            result.append(
                PromotionItem(
                    item_code=normalized,
                    item_type=parse_int(_value(values, {}, "itemtype"), field_name="item_type"),
                    is_gift=(
                        parse_bool(_value(values, {}, "isgift"), field_name="is_gift") or False
                    ),
                )
            )
        if not relations:
            _append_direct_promotion_item_codes(
                container,
                result,
                seen,
                maximum_field_characters,
            )
    if result:
        return tuple(result)
    _append_direct_promotion_item_codes(
        element,
        result,
        seen,
        maximum_field_characters,
    )
    return tuple(result)


def _append_direct_promotion_item_codes(
    parent: ET.Element,
    result: list[PromotionItem],
    seen: set[str],
    maximum_field_characters: int,
) -> None:
    for child in parent:
        if len(child) != 0 or _tag_name(child.tag) not in {"itemcode", "barcode"}:
            continue
        code = _clean_element_text(child, maximum_field_characters)
        if code is not None and (normalized := normalize_identifier(code)) not in seen:
            seen.add(normalized)
            result.append(PromotionItem(item_code=normalized))


def _nested_identifiers(
    element: ET.Element,
    container_tags: set[str],
    relationship_tags: set[str],
    identifier_tag: str,
    *,
    relationship_name: str,
    maximum_field_characters: int,
) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()
    for container in element:
        if _tag_name(container.tag) not in container_tags:
            continue
        for relationship in container:
            if _tag_name(relationship.tag) not in relationship_tags:
                continue
            fields = _element_values(relationship, maximum_field_characters)
            identifier = _value(fields, {}, identifier_tag)
            if identifier is None:
                raise DomainValidationError(
                    f"Promotion {relationship_name} relationship is missing {identifier_tag}"
                )
            normalized = normalize_identifier(identifier)
            if normalized not in seen:
                seen.add(normalized)
                values.append(normalized)
    return tuple(values)


def _discount_kind(
    description: str | None,
    discounted_price: Decimal | None,
    discount_rate: Decimal | None,
    minimum_quantity: Decimal | None,
    minimum_purchase: Decimal | None,
    items: tuple[PromotionItem, ...],
    findings: _SchemaFindings,
) -> DiscountKind:
    normalized_description = (description or "").casefold()
    if "second" in normalized_description or "שני" in normalized_description:
        return DiscountKind.SECOND_ITEM
    if findings.has_discount_amount:
        return DiscountKind.AMOUNT
    if discount_rate is not None:
        return DiscountKind.PERCENTAGE
    if minimum_purchase is not None or findings.has_conditional_field:
        return DiscountKind.CONDITIONAL
    if minimum_quantity is not None and len(items) > 1:
        return DiscountKind.MIX_AND_MATCH
    if minimum_quantity is not None:
        return DiscountKind.QUANTITY
    if discounted_price is not None:
        return DiscountKind.FIXED_PRICE
    return DiscountKind.UNKNOWN


def _element_values(element: ET.Element, maximum_field_characters: int) -> dict[str, str]:
    values: dict[str, str] = {}
    for child in element:
        if len(child) != 0:
            continue
        text = _clean_element_text(child, maximum_field_characters)
        if text is not None:
            values.setdefault(_tag_name(child.tag), text)
    return values


_STORE_LEAF_TAGS = _CONTEXT_TAGS | frozenset(
    {
        "storetype",
        "chainname",
        "subchainname",
        "storename",
        "address",
        "storeaddress",
        "city",
        "zipcode",
        "postalcode",
    }
)
_PRICE_LEAF_TAGS = _CONTEXT_TAGS | frozenset(
    {
        "itemcode",
        "barcode",
        "itemtype",
        "itemname",
        "itemnm",
        "manufacturername",
        "manufacturercountry",
        "manufacturecountry",
        "manufactureritemdescription",
        "unitqty",
        "unitquantity",
        "quantity",
        "unitofmeasure",
        "unitmeasure",
        "isweighted",
        "bisweighted",
        "qtyinpackage",
        "quantityinpackage",
        "itemprice",
        "price",
        "unitofmeasureprice",
        "unitprice",
        "allowdiscount",
        "itemstatus",
        "status",
        "priceupdatedate",
        "priceupdatetime",
        "updatedate",
        "updatetime",
        "lastsaledate",
        "lastsaletime",
        "lastsaledatetime",
    }
)
_PROMOTION_LEAF_TAGS = _CONTEXT_TAGS | frozenset(
    {
        "promotionid",
        "promotioncode",
        "promotiondescription",
        "description",
        "discountedprice",
        "promotionprice",
        "discountrate",
        "discountpercent",
        "minqty",
        "minimumquantity",
        "promotionstartdate",
        "startdate",
        "promotionstarttime",
        "starttime",
        "promotionenddate",
        "enddate",
        "promotionendtime",
        "endtime",
        "itemcode",
        "barcode",
        "itemtype",
        "isgift",
        "clubid",
        "rewardtype",
        "allowmultiplediscounts",
        "maxqty",
        "maximumquantity",
        "minpurchaseamount",
        "minimumpurchase",
        "discountedpricepermida",
        "discountedunitprice",
        "minnoofitemofered",
        "minimumitemsoffered",
        "additionalrestrictions",
        "remarks",
        "isactive",
    }
)
_PROMOTION_RELATION_CONTAINERS = frozenset({"promotionitems", "promotionstores", "clubs"})
_PROMOTION_ITEM_RELATIONS = frozenset({"item", "promotionitem"})
_PROMOTION_ITEM_LEAVES = frozenset({"itemcode", "barcode", "itemtype", "isgift"})
_PROMOTION_STORE_RELATIONS = frozenset({"store", "promotionstore"})
_PROMOTION_CLUB_RELATIONS = frozenset({"club", "promotionclub"})


def _record_schema_findings(
    element: ET.Element,
    document_type: DocumentType,
    maximum_field_characters: int,
) -> _SchemaFindings:
    findings = _SchemaFindings(
        max(
            maximum_field_characters,
            _MAXIMUM_SCHEMA_WARNING_EVIDENCE_CHARACTERS,
        )
    )
    record_tag = _tag_name(element.tag)
    pending: list[tuple[ET.Element, tuple[str, ...]]] = [(element, ())]
    while pending:
        node, relative_path = pending.pop()
        tag = _tag_name(node.tag)
        full_path = (record_tag, *relative_path)
        text = _clean_element_text(node, maximum_field_characters)
        node_kind = _record_node_kind(relative_path, node, document_type)
        if node_kind == "unknown":
            findings.add(full_path, text, semantic_tag=tag)
        elif node_kind == "structure" and text is not None:
            findings.add((*full_path, "#text"), text, semantic_tag="#text")
        for attribute_name, raw_value in node.attrib.items():
            name = _tag_name(attribute_name) or "attribute"
            value = clean_source_text(raw_value, max_length=maximum_field_characters)
            findings.add((*full_path, f"@{name}"), value, semantic_tag=name)
        pending.extend((child, (*relative_path, _tag_name(child.tag))) for child in reversed(node))
    return findings


def _record_node_kind(
    path: tuple[str, ...],
    element: ET.Element,
    document_type: DocumentType,
) -> str:
    if not path:
        return "structure"
    allowed_leaves = (
        _STORE_LEAF_TAGS
        if document_type is DocumentType.STORES
        else _PRICE_LEAF_TAGS
        if document_type.is_price
        else _PROMOTION_LEAF_TAGS
    )
    if len(path) == 1:
        if path[0] in allowed_leaves and len(element) == 0:
            return "leaf"
        if document_type.is_promotion and path[0] in _PROMOTION_RELATION_CONTAINERS:
            return "structure"
        return "unknown"
    if not document_type.is_promotion:
        return "unknown"
    if path[0] == "promotionitems":
        if len(path) == 2:
            if path[1] in _PROMOTION_ITEM_RELATIONS:
                return "structure"
            if path[1] in {"itemcode", "barcode"} and len(element) == 0:
                return "leaf"
        if (
            len(path) == 3
            and path[1] in _PROMOTION_ITEM_RELATIONS
            and path[2] in _PROMOTION_ITEM_LEAVES
            and len(element) == 0
        ):
            return "leaf"
    if path[0] == "promotionstores":
        if len(path) == 2 and path[1] in _PROMOTION_STORE_RELATIONS:
            return "structure"
        if (
            len(path) == 3
            and path[1] in _PROMOTION_STORE_RELATIONS
            and path[2] == "storeid"
            and len(element) == 0
        ):
            return "leaf"
    if path[0] == "clubs":
        if len(path) == 2 and path[1] in _PROMOTION_CLUB_RELATIONS:
            return "structure"
        if (
            len(path) == 3
            and path[1] in _PROMOTION_CLUB_RELATIONS
            and path[2] == "clubid"
            and len(element) == 0
        ):
            return "leaf"
    return "unknown"


def _promotion_restrictions(
    existing: str | None,
    findings: _SchemaFindings,
    maximum_characters: int,
) -> str | None:
    if not findings.count:
        return existing
    prefix = f"{existing}\n" if existing else ""
    marker = "[unparsed XML conditions] "
    remaining = maximum_characters - len(prefix) - len(marker)
    if remaining <= 0:
        return (existing or "")[:maximum_characters]
    evidence = findings.evidence[:remaining]
    return f"{prefix}{marker}{evidence}"[:maximum_characters]


def _clean_element_text(element: ET.Element, maximum_field_characters: int) -> str | None:
    return clean_source_text(element.text, max_length=maximum_field_characters)


def _encoded_record_size(element: ET.Element[str], codec: str) -> int:
    size_codec = {"utf-8-sig": "utf-8", "utf-16": "utf-16-le"}.get(codec, codec)
    serialized = ET.tostring(element, encoding="unicode")
    return len(serialized.encode(size_codec, errors="strict"))


def _required(values: dict[str, str], context: dict[str, str], *names: str) -> str:
    value = _value(values, context, *names)
    if value is None:
        raise DomainValidationError(f"Required XML field {names[0]} is missing")
    return value


def _value(values: dict[str, str], context: dict[str, str], *names: str) -> str | None:
    for name in names:
        normalized = _normalize_name(name)
        if normalized in values:
            return values[normalized]
        if normalized in context:
            return context[normalized]
    return None


def _date_time(
    values: dict[str, str],
    context: dict[str, str],
    date_names: tuple[str, ...],
    time_names: tuple[str, ...],
) -> datetime | None:
    date_value = _value(values, context, *date_names)
    time_value = _value(values, context, *time_names)
    if date_value is not None and ("T" in date_value or " " in date_value):
        return parse_source_datetime(date_value)
    if date_value is None and time_value is not None and ("T" in time_value or " " in time_value):
        return parse_source_datetime(time_value)
    return combine_source_date_time(date_value, time_value)


def _record_tag(document_type: DocumentType) -> str:
    if document_type is DocumentType.STORES:
        return "store"
    if document_type.is_price:
        return "item"
    if document_type.is_promotion:
        return "promotion"
    raise MalformedDocumentError("No XML parser exists for the document type")


def _record_container(document_type: DocumentType) -> str:
    if document_type is DocumentType.STORES:
        return "stores"
    if document_type.is_price:
        return "items"
    if document_type.is_promotion:
        return "promotions"
    raise MalformedDocumentError("No XML parser exists for the document type")


def _tag_name(tag: str) -> str:
    local = tag.rsplit("}", 1)[-1]
    return _normalize_name(local)


def _normalize_name(value: str) -> str:
    return _NORMALIZE_TAG.sub("", value.casefold())


def _check_security_markers(tail: bytes, chunk: bytes) -> bytes:
    probe = (tail + chunk).replace(b"\x00", b"").upper()
    if any(marker in probe for marker in _SECURITY_MARKERS):
        raise MalformedDocumentError("DOCTYPE and entity declarations are not allowed")
    return (tail + chunk)[-32:]


def _reject_html_prefix(prefix: bytearray) -> None:
    normalized = bytes(prefix).replace(b"\x00", b"").lstrip(b"\xef\xbb\xbf\xff\xfe \t\r\n").upper()
    if normalized.startswith(b"<?XML"):
        declaration_end = normalized.find(b"?>")
        if declaration_end < 0:
            return
        normalized = normalized[declaration_end + 2 :].lstrip()
    compact = re.sub(rb"\s+", b"", normalized[:128])
    if any(compact.startswith(marker) for marker in _HTML_PREFIXES):
        raise MalformedDocumentError("Source returned HTML instead of a data document")


def _ensure_spool_directory(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    if not directory.is_dir():
        raise OSError("Configured XML spool path is not a directory")


def _resolved_parser_spool_directory(directory: Path | None) -> Path:
    return directory if directory is not None else Path(tempfile.gettempdir()).resolve()


def ensure_secure_expat() -> None:
    numbers = tuple(int(value) for value in re.findall(r"\d+", expat.EXPAT_VERSION)[:3])
    if numbers < _MINIMUM_EXPAT:
        raise RuntimeError("Makolet requires Expat 2.7.2 or newer")
