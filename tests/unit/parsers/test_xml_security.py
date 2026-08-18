from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from uuid import UUID
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from makolet.adapters.parsers.streams import CompressionLimits
from makolet.adapters.parsers.xml import RetailXmlParser, XmlParserLimits
from makolet.domain.enums import CompressionFormat, DocumentType
from makolet.domain.errors import ArchiveCapacityError, MalformedDocumentError

SOURCE_FILE_ID = UUID("00000000-0000-0000-0000-000000000099")


async def _chunks(payload: bytes, size: int = 7) -> AsyncIterator[bytes]:
    for offset in range(0, len(payload), size):
        yield payload[offset : offset + size]


async def _parse(
    payload: bytes,
    document_type: DocumentType,
    *,
    limits: XmlParserLimits | None = None,
    compression: CompressionFormat = CompressionFormat.NONE,
) -> list[object]:
    parser = RetailXmlParser(limits)
    return [
        event
        async for event in parser.parse(
            _chunks(payload),
            source_file_id=SOURCE_FILE_ID,
            document_type=document_type,
            compression=compression,
            filename="hostile.xml",
        )
    ]


@pytest.mark.asyncio
async def test_xml_declaration_does_not_hide_html_error_page() -> None:
    payload = b'<?xml version="1.0" encoding="UTF-8"?><html><body>error</body></html>'

    with pytest.raises(MalformedDocumentError, match="HTML"):
        await _parse(payload, DocumentType.PRICE_FULL)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"<Envelope><Items /></Envelope>", "root"),
        (b"<Root><Stores /></Root>", "declared document type"),
        (b"<Root><Item /></Root>", "outside the expected"),
        (b"<Root><Envelope><Items /></Envelope></Root>", "allowed document path"),
        (b"<Root><Other /></Root>", "expected record container"),
    ],
)
async def test_document_type_requires_regulated_root_and_container(
    payload: bytes,
    message: str,
) -> None:
    with pytest.raises(MalformedDocumentError, match=message):
        await _parse(payload, DocumentType.PRICE_FULL)


@pytest.mark.asyncio
async def test_unknown_element_text_is_bounded_before_tree_retention() -> None:
    limits = XmlParserLimits(maximum_field_characters=16)
    payload = b"<Root><Ignored>" + b"x" * 100 + b"</Ignored><Items /></Root>"

    with pytest.raises(MalformedDocumentError, match="character data exceeds"):
        await _parse(payload, DocumentType.PRICE_FULL, limits=limits)


@pytest.mark.asyncio
async def test_text_fragments_separated_by_comments_share_a_retention_limit() -> None:
    limits = XmlParserLimits(maximum_field_characters=10)
    payload = b"<Root><Ignored>123456<!---->789012</Ignored><Items /></Root>"

    with pytest.raises(MalformedDocumentError, match="character data exceeds"):
        await _parse(payload, DocumentType.PRICE_FULL, limits=limits)


@pytest.mark.asyncio
async def test_attribute_value_is_bounded_across_decoder_chunks() -> None:
    limits = XmlParserLimits(
        compression=CompressionLimits(maximum_chunk_bytes=4),
        maximum_field_characters=8,
    )
    payload = b'<Root attacker="' + b"x" * 100 + b'"><Items /></Root>'

    with pytest.raises(MalformedDocumentError, match="attribute value exceeds"):
        await _parse(payload, DocumentType.PRICE_FULL, limits=limits)


@pytest.mark.asyncio
async def test_unfinished_markup_is_bounded_across_parser_feeds() -> None:
    limits = XmlParserLimits(
        compression=CompressionLimits(maximum_chunk_bytes=4),
        maximum_token_characters=16,
    )
    payload = b"<Root><Items><Unfinished" + b"x" * 100

    with pytest.raises(MalformedDocumentError, match="tokenizer limit"):
        await _parse(payload, DocumentType.PRICE_FULL, limits=limits)


@pytest.mark.asyncio
async def test_short_unfinished_attribute_is_rejected_by_token_guard_close() -> None:
    payload = b'<Root><Items><Node attacker="short'

    with pytest.raises(MalformedDocumentError, match="unfinished token"):
        await _parse(payload, DocumentType.PRICE_FULL)


@pytest.mark.asyncio
async def test_cdata_is_bounded_as_character_data() -> None:
    limits = XmlParserLimits(maximum_field_characters=8)
    payload = b"<Root><Ignored><![CDATA[0123456789]]></Ignored><Items /></Root>"

    with pytest.raises(MalformedDocumentError, match="character data exceeds"):
        await _parse(payload, DocumentType.PRICE_FULL, limits=limits)


@pytest.mark.asyncio
async def test_parser_spills_into_configured_directory_and_cleans_up(tmp_path: Path) -> None:
    spool_directory = tmp_path / "parser-spool"
    limits = XmlParserLimits(
        compression=CompressionLimits(spool_memory_bytes=1),
        temporary_directory=spool_directory,
    )
    payload = (
        b"<Root><ChainId>1</ChainId><SubChainId>1</SubChainId><Stores>"
        b"<Store><StoreId>1</StoreId><StoreName>One</StoreName></Store>"
        b"</Stores></Root>"
    )

    events = await _parse(payload, DocumentType.STORES, limits=limits)

    assert events
    assert spool_directory.is_dir()
    assert list(spool_directory.iterdir()) == []


@pytest.mark.asyncio
async def test_parser_spool_preserves_configured_free_space_reserve(tmp_path: Path) -> None:
    spool_directory = tmp_path / "parser-spool"
    limits = XmlParserLimits(
        compression=CompressionLimits(spool_memory_bytes=1),
        temporary_directory=spool_directory,
        minimum_free_bytes=2**63,
    )

    with pytest.raises(ArchiveCapacityError, match="free-space reserve"):
        await _parse(b"<Root><Items /></Root>", DocumentType.PRICE_FULL, limits=limits)

    assert list(spool_directory.iterdir()) == []


@pytest.mark.asyncio
async def test_xml_and_zip_spools_share_the_configured_directory(tmp_path: Path) -> None:
    spool_directory = tmp_path / "parser-spool"
    limits = XmlParserLimits(
        compression=CompressionLimits(spool_memory_bytes=1),
        temporary_directory=spool_directory,
    )
    xml = (
        b"<Root><ChainId>1</ChainId><SubChainId>1</SubChainId><Stores>"
        b"<Store><StoreId>1</StoreId><StoreName>One</StoreName></Store>"
        b"</Stores></Root>"
    )
    compressed = BytesIO()
    with ZipFile(compressed, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("stores.xml", xml)

    with patch("tempfile.SpooledTemporaryFile", wraps=tempfile.SpooledTemporaryFile) as spool:
        events = await _parse(
            compressed.getvalue(),
            DocumentType.STORES,
            limits=limits,
            compression=CompressionFormat.ZIP,
        )

    assert events
    assert [call.kwargs.get("dir") for call in spool.call_args_list] == [
        str(spool_directory.resolve()),
        str(spool_directory.resolve()),
    ]
    assert list(spool_directory.iterdir()) == []


def test_token_limit_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        XmlParserLimits(maximum_token_characters=0)
