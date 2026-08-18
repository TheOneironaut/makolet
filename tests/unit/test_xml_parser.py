from __future__ import annotations

import gzip
import tracemalloc
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from makolet.adapters.parsers.streams import CompressionLimits
from makolet.adapters.parsers.xml import RetailXmlParser, XmlParserLimits
from makolet.domain.enums import CompressionFormat, DiscountKind, DocumentType, IssueSeverity
from makolet.domain.errors import MalformedDocumentError
from makolet.domain.models import (
    DocumentMetadata,
    PriceRecord,
    PromotionRecord,
    StoreRecord,
    ValidationIssue,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "standard"
SOURCE_FILE_ID = UUID("00000000-0000-0000-0000-000000000001")


async def chunks(payload: bytes, size: int = 17) -> AsyncIterator[bytes]:
    for offset in range(0, len(payload), size):
        yield payload[offset : offset + size]


async def repeated_price_document(records: int) -> AsyncIterator[bytes]:
    yield b"<Root><ChainId>1</ChainId><SubChainId>1</SubChainId><StoreId>1</StoreId><Items>"
    pending: list[bytes] = []
    for index in range(records):
        pending.append(
            (
                f"<Item><ItemCode>{index:013d}</ItemCode><ItemName>X</ItemName>"
                "<ItemPrice>1.00</ItemPrice></Item>"
            ).encode()
        )
        if len(pending) == 100:
            yield b"".join(pending)
            pending.clear()
    if pending:
        yield b"".join(pending)
    yield b"</Items></Root>"


async def parse(
    payload: bytes,
    document_type: DocumentType,
    *,
    compression: CompressionFormat = CompressionFormat.NONE,
    parser: RetailXmlParser | None = None,
    chunk_size: int = 17,
) -> list[object]:
    subject = parser or RetailXmlParser()
    return [
        event
        async for event in subject.parse(
            chunks(payload, chunk_size),
            source_file_id=SOURCE_FILE_ID,
            document_type=document_type,
            compression=compression,
            filename="fixture.xml",
        )
    ]


def store_document(declaration: str | None = 'encoding="UTF-8"') -> str:
    prolog = f'<?xml version="1.0" {declaration}?>' if declaration is not None else ""
    return (
        f"{prolog}<Root><ChainId>7290000000001</ChainId>"
        "<SubChainId>1</SubChainId><Stores><Store>"
        "<StoreId>042</StoreId>"
        "<StoreName>\u05de\u05db\u05d5\u05dc\u05ea \u05e9\u05dc\u05d5\u05dd</StoreName>"
        "<City>\u05ea\u05dc \u05d0\u05d1\u05d9\u05d1</City></Store></Stores></Root>"
    )


@pytest.mark.asyncio
async def test_price_full_parses_mixed_case_and_rejects_only_bad_record() -> None:
    payload = (FIXTURES / "price-full.xml").read_bytes()
    events = await parse(
        gzip.compress(payload, mtime=0), DocumentType.PRICE_FULL, compression=CompressionFormat.GZIP
    )

    prices = [event for event in events if isinstance(event, PriceRecord)]
    issues = [event for event in events if isinstance(event, ValidationIssue)]
    assert len(prices) == 1
    assert prices[0].item_code == "4006381333931"
    assert str(prices[0].item_price) == "19.90"
    assert len(issues) == 1
    assert issues[0].severity is IssueSeverity.RECORD_REJECTION
    assert isinstance(events[-1], DocumentMetadata)


@pytest.mark.asyncio
async def test_price_parser_accepts_observed_combined_datetime_tags() -> None:
    payload = b"""<Root><ChainID>1</ChainID><SubChainID>1</SubChainID><StoreID>1</StoreID>
    <Items><Item><PriceUpdateTime>2026-08-10T23:58:00.000</PriceUpdateTime>
    <LastSaleDateTime>2026-08-10T17:04:00.000</LastSaleDateTime>
    <ItemCode>12345</ItemCode><ItemName>Deposit</ItemName><ItemPrice>0.30</ItemPrice>
    </Item></Items></Root>"""

    events = await parse(payload, DocumentType.PRICE_DELTA)

    price = next(event for event in events if isinstance(event, PriceRecord))
    assert price.price_updated_at == datetime(2026, 8, 10, 20, 58, tzinfo=UTC)
    assert price.last_sale_at == datetime(2026, 8, 10, 14, 4, tzinfo=UTC)


@pytest.mark.asyncio
async def test_price_parser_accepts_observed_bina_price_aliases() -> None:
    payload = b"""<Root><ChainID>1</ChainID><SubChainID>1</SubChainID><StoreID>1</StoreID>
    <Items><Item><ItemCode>12345</ItemCode><ItemNm>Observed alias</ItemNm>
    <ItemPrice>1.00</ItemPrice><ManufactureCountry>IL</ManufactureCountry>
    <bIsWeighted>1</bIsWeighted></Item></Items></Root>"""

    events = await parse(payload, DocumentType.PRICE_DELTA)

    price = next(event for event in events if isinstance(event, PriceRecord))
    assert price.item_name == "Observed alias"
    assert price.manufacturer_country == "IL"
    assert price.is_weighted is True
    assert not any(isinstance(event, ValidationIssue) for event in events)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_time",
    ["2026-03-27T02:30:00", "2026-10-25T01:30:00"],
)
async def test_price_parser_rejects_ambiguous_or_nonexistent_local_source_time(
    source_time: str,
) -> None:
    payload = f"""<Root><ChainID>1</ChainID><SubChainID>1</SubChainID><StoreID>1</StoreID>
    <Items><Item><PriceUpdateTime>{source_time}</PriceUpdateTime>
    <ItemCode>12345</ItemCode><ItemName>Clock edge</ItemName><ItemPrice>1.00</ItemPrice>
    </Item></Items></Root>""".encode()

    events = await parse(payload, DocumentType.PRICE_DELTA)

    assert not any(isinstance(event, PriceRecord) for event in events)
    issue = next(event for event in events if isinstance(event, ValidationIssue))
    assert issue.code == "domain_validation_error"
    assert issue.severity is IssueSeverity.RECORD_REJECTION
    assert "explicit offset" in issue.message


@pytest.mark.asyncio
async def test_unknown_additive_price_field_is_a_durable_warning() -> None:
    payload = b"""<Root><ChainID>1</ChainID><SubChainID>1</SubChainID><StoreID>1</StoreID>
    <Items><Item><ItemCode>12345</ItemCode><ItemName>New field</ItemName>
    <ItemPrice>1.00</ItemPrice><PublisherExtension>alpha</PublisherExtension>
    </Item></Items></Root>"""

    events = await parse(payload, DocumentType.PRICE_DELTA)

    assert any(isinstance(event, PriceRecord) for event in events)
    issue = next(event for event in events if isinstance(event, ValidationIssue))
    assert issue.severity is IssueSeverity.WARNING
    assert issue.code == "unexpected_xml_field"
    assert issue.field_name == "item.publisherextension"
    assert issue.rejected_value == "item.publisherextension=alpha"


@pytest.mark.asyncio
async def test_nested_known_leaf_cannot_shadow_a_direct_record_field() -> None:
    payload = b"""<Root><ChainID>1</ChainID><SubChainID>1</SubChainID><StoreID>1</StoreID>
    <Items><Item><ItemCode>12345</ItemCode><ItemName>Structural field</ItemName>
    <PublisherExtension><ItemPrice>0.01</ItemPrice></PublisherExtension>
    <ItemPrice>10.00</ItemPrice></Item></Items></Root>"""

    events = await parse(payload, DocumentType.PRICE_DELTA)

    price = next(event for event in events if isinstance(event, PriceRecord))
    issue = next(event for event in events if isinstance(event, ValidationIssue))
    assert price.item_price == Decimal("10.00")
    assert issue.severity is IssueSeverity.WARNING
    assert issue.field_name is not None
    assert "item.publisherextension" in issue.field_name
    assert issue.rejected_value is not None
    assert "item.publisherextension.itemprice=0.01" in issue.rejected_value


@pytest.mark.asyncio
async def test_required_field_below_unknown_wrapper_rejects_the_record() -> None:
    payload = b"""<Root><ChainID>1</ChainID><SubChainID>1</SubChainID><StoreID>1</StoreID>
    <Items><Item><PublisherExtension><ItemCode>12345</ItemCode></PublisherExtension>
    <ItemName>Wrapped identifier</ItemName><ItemPrice>10.00</ItemPrice>
    </Item></Items></Root>"""

    events = await parse(payload, DocumentType.PRICE_DELTA)

    assert not any(isinstance(event, PriceRecord) for event in events)
    issue = next(event for event in events if isinstance(event, ValidationIssue))
    assert issue.severity is IssueSeverity.RECORD_REJECTION
    assert "itemcode" in issue.message


@pytest.mark.asyncio
async def test_document_and_promotion_attributes_are_bounded_schema_warnings() -> None:
    payload = b"""<Root schemaVersion="2"><ChainID>1</ChainID>
    <PublisherContext><ChainID>999</ChainID></PublisherContext><SubChainID>1</SubChainID>
    <Promotions listingRevision="new"><PublisherBatch>new</PublisherBatch>
    <Promotion recordExtension="alpha">
    <PromotionID>P-ATTR</PromotionID><PromotionDescription>sale</PromotionDescription>
    <PromotionItems relationVersion="2"><Item eligibility="club">
    <ItemCode>12345</ItemCode></Item></PromotionItems>
    </Promotion></Promotions></Root>"""

    events = await parse(payload, DocumentType.PROMOTION_DELTA)

    promotion = next(event for event in events if isinstance(event, PromotionRecord))
    issues = [event for event in events if isinstance(event, ValidationIssue)]
    record_issue = next(event for event in issues if event.record_index == 1)
    document_issue = next(event for event in issues if event.record_index is None)
    assert promotion.chain_id == "1"
    assert [item.item_code for item in promotion.items] == ["12345"]
    assert promotion.additional_restrictions is not None
    assert "@recordextension=alpha" in promotion.additional_restrictions
    assert record_issue.field_name is not None
    assert "promotion.@recordextension" in record_issue.field_name
    assert document_issue.field_name is not None
    assert "root.publishercontext" in document_issue.field_name
    assert "root.promotions.publisherbatch" in document_issue.field_name
    assert len(record_issue.field_name) <= 256
    assert len(record_issue.rejected_value or "") <= 2_000
    assert len(document_issue.field_name) <= 256
    assert len(document_issue.rejected_value or "") <= 2_000


@pytest.mark.asyncio
async def test_many_additive_promotion_fields_have_fixed_size_evidence() -> None:
    additions = "".join(
        f"<FutureCondition{index:03d}>value-{index:03d}</FutureCondition{index:03d}>"
        for index in range(200)
    )
    payload = (
        "<Root><ChainID>1</ChainID><SubChainID>1</SubChainID><Promotions><Promotion>"
        "<PromotionID>P-MANY</PromotionID><PromotionItems>"
        "<Item><ItemCode>12345</ItemCode></Item></PromotionItems>"
        f"{additions}</Promotion></Promotions></Root>"
    ).encode()
    parser = RetailXmlParser(XmlParserLimits(maximum_field_characters=128))

    events = await parse(payload, DocumentType.PROMOTION_DELTA, parser=parser)

    promotion = next(event for event in events if isinstance(event, PromotionRecord))
    issue = next(event for event in events if isinstance(event, ValidationIssue))
    assert promotion.additional_restrictions is not None
    assert promotion.additional_restrictions.startswith("[unparsed XML conditions] ")
    assert len(promotion.additional_restrictions) <= 128
    assert issue.field_name is not None
    assert len(issue.field_name) <= 256
    assert issue.rejected_value is not None
    assert len(issue.rejected_value) <= 2_000
    assert "200 unrecognized schema field(s)" in issue.message


@pytest.mark.asyncio
async def test_unknown_promotion_condition_is_warned_and_preserved() -> None:
    payload = b"""<Root><ChainID>1</ChainID><SubChainID>1</SubChainID><StoreID>1</StoreID>
    <Promotions><Promotion><PromotionID>P-NEW</PromotionID>
    <PromotionDescription>Coupon offer</PromotionDescription>
    <PromotionItems><Item><ItemCode>12345</ItemCode></Item></PromotionItems>
    <CouponEligibility>members-only</CouponEligibility>
    </Promotion></Promotions></Root>"""

    events = await parse(payload, DocumentType.PROMOTION_DELTA)

    promotion = next(event for event in events if isinstance(event, PromotionRecord))
    issue = next(event for event in events if isinstance(event, ValidationIssue))
    assert promotion.additional_restrictions is not None
    assert "couponeligibility=members-only" in promotion.additional_restrictions
    assert issue.code == "unexpected_xml_field"
    assert issue.severity is IssueSeverity.WARNING


@pytest.mark.asyncio
async def test_renamed_required_promotion_identifier_rejects_record() -> None:
    payload = b"""<Root><ChainID>1</ChainID><SubChainID>1</SubChainID>
    <Promotions><Promotion><PromoIdentifier>P-RENAMED</PromoIdentifier>
    <PromotionItems><Item><ItemCode>12345</ItemCode></Item></PromotionItems>
    </Promotion></Promotions></Root>"""

    events = await parse(payload, DocumentType.PROMOTION_DELTA)

    assert not any(isinstance(event, PromotionRecord) for event in events)
    issue = next(event for event in events if isinstance(event, ValidationIssue))
    assert issue.severity is IssueSeverity.RECORD_REJECTION
    assert "promotionid" in issue.message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("extra", "description", "expected"),
    [
        ("<DiscountedPrice>8.00</DiscountedPrice>", "sale", DiscountKind.FIXED_PRICE),
        ("<DiscountRate>20</DiscountRate>", "sale", DiscountKind.PERCENTAGE),
        ("<DiscountAmount>2.00</DiscountAmount>", "sale", DiscountKind.AMOUNT),
        ("<MinQty>2</MinQty>", "sale", DiscountKind.QUANTITY),
        ("<MinPurchaseAmount>50</MinPurchaseAmount>", "sale", DiscountKind.CONDITIONAL),
        ("<DiscountedPrice>8.00</DiscountedPrice>", "second item", DiscountKind.SECOND_ITEM),
        ("", "sale", DiscountKind.UNKNOWN),
    ],
)
async def test_promotion_discount_kind_contract(
    extra: str,
    description: str,
    expected: DiscountKind,
) -> None:
    payload = f"""<Root><ChainID>1</ChainID><SubChainID>1</SubChainID>
    <Promotions><Promotion><PromotionID>P-KIND</PromotionID>
    <PromotionDescription>{description}</PromotionDescription>{extra}
    <PromotionItems><Item><ItemCode>12345</ItemCode></Item></PromotionItems>
    </Promotion></Promotions></Root>""".encode()

    events = await parse(payload, DocumentType.PROMOTION_DELTA)

    promotion = next(event for event in events if isinstance(event, PromotionRecord))
    assert promotion.discount_kind is expected


@pytest.mark.asyncio
async def test_store_parser_handles_utf16_bom_and_nested_context() -> None:
    text = (FIXTURES / "stores.xml").read_text(encoding="utf-8")
    payload = text.replace('encoding="UTF-8"', 'encoding="UTF-16"').encode("utf-16")
    events = await parse(payload, DocumentType.STORES)

    store = next(event for event in events if isinstance(event, StoreRecord))
    assert (store.chain_id, store.subchain_id, store.store_id) == ("7290000000001", "1", "042")
    assert (store.chain_name, store.subchain_name) == ("Demo Chain", "Demo Format")
    assert store.city == "תל אביב"


@pytest.mark.asyncio
async def test_store_parser_scopes_context_to_each_nested_subchain() -> None:
    payload = b"""<Root><Chain><ChainID>1</ChainID><SubChains>
    <SubChain><SubChainID>10</SubChainID><SubChainName>First format</SubChainName>
    <Stores><Store><StoreID>100</StoreID><StoreName>First</StoreName></Store></Stores>
    </SubChain>
    <SubChain><SubChainID>20</SubChainID>
    <Stores><Store><StoreID>200</StoreID><StoreName>Second</StoreName></Store></Stores>
    </SubChain>
    </SubChains></Chain></Root>"""

    events = await parse(payload, DocumentType.STORES)

    stores = [event for event in events if isinstance(event, StoreRecord)]
    assert [(store.subchain_id, store.subchain_name) for store in stores] == [
        ("10", "First format"),
        ("20", None),
    ]


@pytest.mark.asyncio
async def test_nested_store_subchain_cannot_fall_back_to_root_subchain_id() -> None:
    payload = b"""<Root><SubChainID>999</SubChainID><Chain><ChainID>1</ChainID><SubChains>
    <SubChain><Stores>
    <Store><StoreID>100</StoreID><StoreName>Missing local identity</StoreName></Store>
    </Stores></SubChain></SubChains></Chain></Root>"""
    parser = RetailXmlParser()
    events = parser.parse(
        chunks(payload),
        source_file_id=SOURCE_FILE_ID,
        document_type=DocumentType.STORES,
        compression=CompressionFormat.NONE,
        filename="fixture.xml",
    )

    with pytest.raises(
        MalformedDocumentError,
        match="A nested SubChain with Stores must declare its own SubChainID",
    ):
        await anext(events)


@pytest.mark.asyncio
async def test_second_nested_store_subchain_requires_own_identifier() -> None:
    payload = b"""<Root><Chain><ChainID>1</ChainID><SubChains>
    <SubChain><SubChainID>10</SubChainID>
    <Stores><Store><StoreID>100</StoreID><StoreName>First</StoreName></Store></Stores>
    </SubChain>
    <SubChain><Stores>
    <Store><StoreID>200</StoreID><StoreName>Missing local identity</StoreName></Store>
    </Stores></SubChain>
    </SubChains></Chain></Root>"""
    parser = RetailXmlParser()
    events = parser.parse(
        chunks(payload),
        source_file_id=SOURCE_FILE_ID,
        document_type=DocumentType.STORES,
        compression=CompressionFormat.NONE,
        filename="fixture.xml",
    )

    first_event = await anext(events)
    assert isinstance(first_event, StoreRecord)
    assert (first_event.subchain_id, first_event.store_id) == ("10", "100")
    with pytest.raises(
        MalformedDocumentError,
        match="A nested SubChain with Stores must declare its own SubChainID",
    ):
        await anext(events)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "chunk_size"),
    [
        (store_document().encode("utf-8"), 1),
        (store_document().encode("utf-8-sig"), 2),
        (
            b"\xff\xfe" + store_document('encoding="Windows-1255"').encode("utf-16-le"),
            3,
        ),
        (b"\xfe\xff" + store_document('encoding="UTF-8"').encode("utf-16-be"), 5),
        (store_document(None).encode("utf-16-le"), 7),
        (store_document(None).encode("utf-16-be"), 11),
        (store_document('encoding="Windows-1255"').encode("cp1255"), 13),
        (store_document('encoding="UTF-8"').encode("cp1255"), 17),
        (store_document(None).encode("cp1255"), 19),
        (store_document('encoding="Windows-1255"').encode("utf-8"), 23),
    ],
    ids=[
        "utf8",
        "utf8-bom",
        "utf16-le-bom-wrong-declaration",
        "utf16-be-bom-wrong-declaration",
        "utf16-le-missing-declaration",
        "utf16-be-missing-declaration",
        "windows-1255",
        "windows-1255-wrong-declaration",
        "windows-1255-missing-declaration",
        "utf8-wrong-windows-1255-declaration",
    ],
)
async def test_encoding_variants_preserve_hebrew(payload: bytes, chunk_size: int) -> None:
    original = bytes(payload)

    events = await parse(payload, DocumentType.STORES, chunk_size=chunk_size)

    store = next(event for event in events if isinstance(event, StoreRecord))
    assert store.store_name == "\u05de\u05db\u05d5\u05dc\u05ea \u05e9\u05dc\u05d5\u05dd"
    assert store.city == "\u05ea\u05dc \u05d0\u05d1\u05d9\u05d1"
    assert payload == original


@pytest.mark.asyncio
async def test_missing_declaration_is_resolved_using_the_complete_stream() -> None:
    ignored = "".join(f"<Ignored>{index}</Ignored>" for index in range(500))
    document = store_document(None).replace("<Root>", f"<Root>{ignored}")
    parser = RetailXmlParser(XmlParserLimits(compression=CompressionLimits(spool_memory_bytes=64)))

    events = await parse(
        document.encode("cp1255"),
        DocumentType.STORES,
        parser=parser,
        chunk_size=29,
    )

    store = next(event for event in events if isinstance(event, StoreRecord))
    assert store.store_name == "\u05de\u05db\u05d5\u05dc\u05ea \u05e9\u05dc\u05d5\u05dd"


@pytest.mark.asyncio
async def test_completed_records_are_detached_from_the_xml_parent() -> None:
    record_count = 10_000
    parser = RetailXmlParser(XmlParserLimits(compression=CompressionLimits(spool_memory_bytes=1)))
    parsed = 0
    retained_at_last_record = 0
    tracemalloc.start()
    try:
        async for event in parser.parse(
            repeated_price_document(record_count),
            source_file_id=SOURCE_FILE_ID,
            document_type=DocumentType.PRICE_FULL,
            compression=CompressionFormat.NONE,
            filename="repeated-prices.xml",
        ):
            if isinstance(event, PriceRecord):
                parsed += 1
                if parsed == record_count:
                    retained_at_last_record = tracemalloc.get_traced_memory()[0]
    finally:
        tracemalloc.stop()

    assert parsed == record_count
    assert retained_at_last_record < 2_500_000


@pytest.mark.asyncio
async def test_windows_1255_gzip_is_decoded_after_bounded_decompression() -> None:
    payload = store_document('encoding="UTF-8"').encode("cp1255")

    events = await parse(
        gzip.compress(payload, mtime=0),
        DocumentType.STORES,
        compression=CompressionFormat.GZIP,
        chunk_size=5,
    )

    store = next(event for event in events if isinstance(event, StoreRecord))
    assert store.city == "\u05ea\u05dc \u05d0\u05d1\u05d9\u05d1"


@pytest.mark.asyncio
async def test_promotion_preserves_items_stores_clubs_and_conditions() -> None:
    payload = (FIXTURES / "promo-full.xml").read_bytes()
    events = await parse(payload, DocumentType.PROMOTION_FULL)

    promotion = next(event for event in events if isinstance(event, PromotionRecord))
    assert promotion.discount_kind is DiscountKind.MIX_AND_MATCH
    assert [item.item_code for item in promotion.items] == ["4006381333931", "96385074"]
    assert promotion.store_ids == ("042",)
    assert promotion.club_ids == ("MEMBERS",)
    assert promotion.additional_restrictions == "לחברי מועדון"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fixture_name", "relationship_name"),
    [
        ("promo-malformed-item-relation.xml", "item"),
        ("promo-malformed-store-relation.xml", "store"),
        ("promo-malformed-club-relation.xml", "club"),
    ],
)
async def test_promotion_rejects_malformed_relationship_child_without_partial_record(
    fixture_name: str,
    relationship_name: str,
) -> None:
    events = await parse((FIXTURES / fixture_name).read_bytes(), DocumentType.PROMOTION_FULL)

    promotions = [event for event in events if isinstance(event, PromotionRecord)]
    issues = [event for event in events if isinstance(event, ValidationIssue)]
    assert promotions == []
    assert len(issues) == 1
    assert issues[0].severity is IssueSeverity.RECORD_REJECTION
    assert issues[0].code == "domain_validation_error"
    assert issues[0].record_index == 1
    assert relationship_name in issues[0].message.casefold()
    assert isinstance(events[-1], DocumentMetadata)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"<!DOCTYPE Root [<!ENTITY x 'boom'>]><Root>&x;</Root>", "DOCTYPE"),
        (b"<html><body>upstream error</body></html>", "HTML"),
        (b"<Root><Items><Item></Root>", "malformed"),
    ],
)
async def test_unsafe_or_malformed_whole_file_is_rejected(payload: bytes, message: str) -> None:
    with pytest.raises(MalformedDocumentError, match=message):
        await parse(payload, DocumentType.PRICE_FULL)


@pytest.mark.asyncio
@pytest.mark.parametrize("codec", ["utf-16-le", "utf-16-be"])
async def test_utf16_doctype_is_rejected_across_single_byte_chunks(codec: str) -> None:
    bom = b"\xff\xfe" if codec == "utf-16-le" else b"\xfe\xff"
    payload = bom + "<!DOCTYPE Root [<!ENTITY x 'boom'>]><Root>&x;</Root>".encode(codec)

    with pytest.raises(MalformedDocumentError, match="DOCTYPE"):
        await parse(payload, DocumentType.PRICE_FULL, chunk_size=1)


@pytest.mark.asyncio
async def test_utf16_html_error_page_is_rejected() -> None:
    payload = b"\xff\xfe" + "<html><body>upstream error</body></html>".encode("utf-16-le")

    with pytest.raises(MalformedDocumentError, match="HTML"):
        await parse(payload, DocumentType.PRICE_FULL, chunk_size=3)


@pytest.mark.asyncio
async def test_bytes_invalid_in_all_supported_encodings_are_rejected() -> None:
    payload = b"<Root><Value>\x81</Value></Root>"

    with pytest.raises(MalformedDocumentError, match="neither valid UTF-8 nor Windows-1255"):
        await parse(payload, DocumentType.PRICE_FULL, chunk_size=1)


@pytest.mark.asyncio
async def test_record_byte_limit_stops_oversized_record() -> None:
    parser = RetailXmlParser(XmlParserLimits(maximum_record_bytes=32))
    payload = b"<Root><Items><Item><ItemName>" + b"x" * 100 + b"</ItemName></Item></Items></Root>"

    with pytest.raises(MalformedDocumentError, match="record exceeds"):
        await parse(payload, DocumentType.PRICE_FULL, parser=parser)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limits", "payload", "message"),
    [
        (XmlParserLimits(maximum_depth=2), b"<Root><Level><Leaf/></Level></Root>", "depth"),
        (XmlParserLimits(maximum_elements=2), b"<Root><One/><Two/></Root>", "element"),
        (
            XmlParserLimits(maximum_records=1),
            (
                b"<Root><ChainId>1</ChainId><SubChainId>1</SubChainId><Stores>"
                b"<Store><StoreId>1</StoreId><StoreName>One</StoreName></Store>"
                b"<Store><StoreId>2</StoreId><StoreName>Two</StoreName></Store>"
                b"</Stores></Root>"
            ),
            "record",
        ),
    ],
)
async def test_structural_limits_remain_enforced_after_decoding(
    limits: XmlParserLimits, payload: bytes, message: str
) -> None:
    with pytest.raises(MalformedDocumentError, match=message):
        await parse(payload, DocumentType.STORES, parser=RetailXmlParser(limits))


def test_xml_limits_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        XmlParserLimits(maximum_depth=0)


@pytest.mark.asyncio
async def test_unknown_document_type_has_no_parser() -> None:
    with pytest.raises(MalformedDocumentError, match="No XML parser"):
        await parse(b"<Root/>", DocumentType.UNKNOWN)
