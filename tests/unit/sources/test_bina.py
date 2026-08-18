from __future__ import annotations

import json
from pathlib import Path

import pytest

from makolet.adapters.sources.bina import (
    DEFAULT_PARTITIONS,
    BinaPartition,
    BinaSourceAdapter,
    BinaSourceConfig,
)
from makolet.application.models import DiscoveryCursor
from makolet.domain.enums import CompressionFormat, DocumentType
from makolet.domain.errors import SourceResponseError
from tests.unit.sources.support import FixedClock, FixtureHttpClient

FIXTURES = Path(__file__).with_name("fixtures")
LISTING_URL = "https://demo.binaprojects.com/MainIO_Hok.aspx"
RESOLVER_URL = "https://demo.binaprojects.com/Download.aspx"
LISTING_QUERY = (
    ("_", "query-id"),
    ("wReshet", ""),
    ("WFileType", "4"),
    ("WDate", "11/08/2026"),
    ("WStore", "001"),
)


def config(
    *,
    server_result_cap: int = 1_000,
    zip_wrapped_file_types: frozenset[str] = frozenset(),
) -> BinaSourceConfig:
    return BinaSourceConfig(
        retailer_id="demo-bina",
        portal_id="bina:demo",
        base_url="https://demo.binaprojects.com/",
        query_identifier="query-id",
        chain_id="7290000000001",
        partitions=(BinaPartition("4", "001"),),
        server_result_cap=server_result_cap,
        zip_wrapped_file_types=zip_wrapped_file_types,
    )


async def test_bina_uses_partition_query_resolver_and_case_insensitive_filenames() -> None:
    listing = (FIXTURES / "bina-listing.json").read_bytes()
    resolver = (FIXTURES / "bina-resolver.json").read_bytes()
    resolver_query = (("FileNm", "PriceFull7290000000001-001-202608111200.GZ"),)
    http = FixtureHttpClient(
        {
            (LISTING_URL, LISTING_QUERY): listing,
            (RESOLVER_URL, resolver_query): resolver,
        }
    )
    subject = BinaSourceAdapter(config(), http, FixedClock())

    first = await subject.discover(None, limit=1)
    second = await subject.discover(first.next_cursor, limit=1)

    assert first.complete is False
    assert first.files[0].original_filename.endswith(".GZ")
    assert first.files[0].compression is CompressionFormat.GZIP
    assert first.files[0].document_type is DocumentType.PRICE_FULL
    assert first.files[0].download_url.endswith(first.files[0].original_filename)
    assert second.complete is True
    assert second.files[0].document_type is DocumentType.PROMOTION_DELTA
    assert http.calls[0] == (LISTING_URL, LISTING_QUERY)
    assert http.calls.count((LISTING_URL, LISTING_QUERY)) == 2


async def test_bina_accepts_a_nonempty_direct_array_partition() -> None:
    listing = json.dumps(
        [
            {
                "FileNm": "StoresFull7290000000001-000-20260811.xml",
                "SPath": "/Download/StoresFull7290000000001-000-20260811.xml",
            }
        ]
    ).encode()
    subject = BinaSourceAdapter(
        config(),
        FixtureHttpClient({(LISTING_URL, LISTING_QUERY): listing}),
        FixedClock(),
    )

    page = await subject.discover(None, limit=10)

    assert len(page.files) == 1
    assert page.complete is True


async def test_bina_accepts_an_empty_direct_array_as_an_empty_terminal_page() -> None:
    subject = BinaSourceAdapter(
        config(),
        FixtureHttpClient({(LISTING_URL, LISTING_QUERY): b"[]"}),
        FixedClock(),
    )

    page = await subject.discover(None, limit=10)

    assert page.files == ()
    assert page.next_cursor is None
    assert page.complete is True


async def test_bina_skips_an_empty_leading_partition() -> None:
    price_listing = json.dumps(
        [
            {
                "FileNm": "Price7290000000001-001-20260811.xml",
                "SPath": "/Download/Price7290000000001-001-20260811.xml",
            }
        ]
    ).encode()
    http = FixtureHttpClient(
        {
            (
                LISTING_URL,
                (
                    ("_", "query-id"),
                    ("wReshet", ""),
                    ("WFileType", "1"),
                    ("WDate", "11/08/2026"),
                    ("WStore", ""),
                ),
            ): b"[]",
            (
                LISTING_URL,
                (
                    ("_", "query-id"),
                    ("wReshet", ""),
                    ("WFileType", "2"),
                    ("WDate", "11/08/2026"),
                    ("WStore", ""),
                ),
            ): price_listing,
        }
    )
    subject = BinaSourceAdapter(
        BinaSourceConfig(
            retailer_id="demo-bina",
            portal_id="bina:demo",
            base_url="https://demo.binaprojects.com/",
            query_identifier="query-id",
            chain_id="7290000000001",
            partitions=(BinaPartition("1"), BinaPartition("2")),
        ),
        http,
        FixedClock(),
    )

    page = await subject.discover(None, limit=1)

    assert [remote.document_type for remote in page.files] == [DocumentType.PRICE_DELTA]
    assert page.complete is True
    assert [dict(query)["WFileType"] for _, query in http.calls] == ["1", "2"]


async def test_bina_default_type_partitions_enumerate_every_document_family_once() -> None:
    records = json.loads((FIXTURES / "bina-partitions.json").read_bytes())
    responses: dict[tuple[str, tuple[tuple[str, str], ...]], bytes] = {
        (
            LISTING_URL,
            (
                ("_", "query-id"),
                ("wReshet", ""),
                ("WFileType", partition.file_type),
                ("WDate", "11/08/2026"),
                ("WStore", ""),
            ),
        ): json.dumps({"d": [records[partition.file_type]]}).encode()
        for partition in DEFAULT_PARTITIONS
    }
    http = FixtureHttpClient(responses)
    subject = BinaSourceAdapter(
        BinaSourceConfig(
            retailer_id="demo-bina",
            portal_id="bina:demo",
            base_url="https://demo.binaprojects.com/",
            query_identifier="query-id",
            chain_id="7290000000001",
        ),
        http,
        FixedClock(),
    )

    cursor = None
    document_types: list[DocumentType] = []
    completion_states: list[bool] = []
    while True:
        page = await subject.discover(cursor, limit=500)
        document_types.extend(remote.document_type for remote in page.files)
        completion_states.append(page.complete)
        if page.next_cursor is None:
            break
        cursor = page.next_cursor

    assert document_types == [
        DocumentType.STORES,
        DocumentType.PRICE_DELTA,
        DocumentType.PROMOTION_DELTA,
        DocumentType.PRICE_FULL,
        DocumentType.PROMOTION_FULL,
    ]
    assert completion_states == [False, False, False, False, True]
    assert [dict(query)["WFileType"] for _, query in http.calls] == ["1", "2", "3", "4", "5"]
    assert len(http.calls) == len(DEFAULT_PARTITIONS)


async def test_bina_applies_only_an_explicit_observed_zip_wrapper_override() -> None:
    listing = (FIXTURES / "bina-listing.json").read_bytes()
    resolver = (FIXTURES / "bina-resolver.json").read_bytes()
    resolver_query = (("FileNm", "PriceFull7290000000001-001-202608111200.GZ"),)
    http = FixtureHttpClient(
        {
            (LISTING_URL, LISTING_QUERY): listing,
            (RESOLVER_URL, resolver_query): resolver,
        }
    )
    subject = BinaSourceAdapter(
        config(zip_wrapped_file_types=frozenset({"4"})),
        http,
        FixedClock(),
    )

    page = await subject.discover(None, limit=1)

    assert page.files[0].original_filename.endswith(".GZ")
    assert page.files[0].compression is CompressionFormat.ZIP


async def test_bina_resolver_rejects_a_named_wrapper_for_its_direct_array_contract() -> None:
    listing = (FIXTURES / "bina-listing.json").read_bytes()
    resolver_records = json.loads((FIXTURES / "bina-resolver.json").read_bytes())
    resolver_query = (("FileNm", "PriceFull7290000000001-001-202608111200.GZ"),)
    subject = BinaSourceAdapter(
        config(),
        FixtureHttpClient(
            {
                (LISTING_URL, LISTING_QUERY): listing,
                (RESOLVER_URL, resolver_query): json.dumps({"d": resolver_records}).encode(),
            }
        ),
        FixedClock(),
    )

    with pytest.raises(SourceResponseError, match="record collection"):
        await subject.discover(None, limit=1)


async def test_bina_refuses_to_claim_completeness_at_server_cap() -> None:
    listing = json.dumps({"d": [{"FileNm": "StoresFull7290000000001.xml"}]}).encode()
    http = FixtureHttpClient({(LISTING_URL, LISTING_QUERY): listing})
    subject = BinaSourceAdapter(config(server_result_cap=1), http, FixedClock())

    with pytest.raises(SourceResponseError, match="server cap"):
        await subject.discover(None, limit=10)


async def test_bina_rejects_a_missing_expected_partition_collection() -> None:
    http = FixtureHttpClient({(LISTING_URL, LISTING_QUERY): b'{"error": []}'})
    subject = BinaSourceAdapter(config(), http, FixedClock())

    with pytest.raises(SourceResponseError, match="expected record collection"):
        await subject.discover(None, limit=10)


async def test_bina_rejects_cursor_beyond_changed_partition() -> None:
    http = FixtureHttpClient({(LISTING_URL, LISTING_QUERY): b'{"d": []}'})
    subject = BinaSourceAdapter(config(), http, FixedClock())
    cursor = DiscoveryCursor(
        "eyJkYXRlIjoiMjAyNi0wOC0xMSIsIm9mZnNldCI6MSwicGFydGl0aW9uIjowLCJzb3VyY2UiOiJkZW1vLWJpbmEiLCJ2IjoxfQ"
    )

    with pytest.raises(SourceResponseError, match="beyond"):
        await subject.discover(cursor, limit=1)
