from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from makolet.adapters.sources.matrix import MatrixSourceAdapter, MatrixSourceConfig
from makolet.domain.enums import CompressionFormat, DocumentType
from makolet.domain.errors import SourceResponseError
from tests.unit.sources.support import FixedClock, FixtureHttpClient

FIXTURES = Path(__file__).with_name("fixtures")
LISTING_URL = "https://laibcatalog.co.il/webapi/api/getfiles"
QUERY = (("edi", "7290000000002"),)


async def test_matrix_catalog_is_bounded_paginated_and_preserves_listing_metadata() -> None:
    body = b"\xef\xbb\xbf" + (FIXTURES / "matrix-listing.json").read_bytes()
    http = FixtureHttpClient({(LISTING_URL, QUERY): body})
    subject = MatrixSourceAdapter(
        MatrixSourceConfig("demo-matrix", "matrix:demo", "7290000000002"),
        http,
        FixedClock(),
    )

    first = await subject.discover(None, limit=1)
    second = await subject.discover(first.next_cursor, limit=1)

    assert first.files[0].document_type is DocumentType.PRICE_DELTA
    assert first.files[0].content_length == 456
    assert first.files[0].last_modified == datetime(2026, 8, 11, 10, tzinfo=UTC)
    assert first.files[0].download_url.startswith("https://laibcatalog.co.il/webapi/7290000000002/")
    assert second.files[0].document_type is DocumentType.STORES
    assert second.files[0].compression is CompressionFormat.GZIP
    assert second.files[0].content_length is None
    assert second.files[0].last_modified == datetime(2026, 8, 11, 18, 8, 28, tzinfo=UTC)
    assert second.complete is True
    assert http.calls == [(LISTING_URL, QUERY)]


async def test_matrix_accepts_a_nonempty_direct_array_catalog() -> None:
    body = b'[{"fileName":"Stores7290000000002-000-20260811.xml.gz","fileSize":42}]'
    subject = MatrixSourceAdapter(
        MatrixSourceConfig("demo-matrix", "matrix:demo", "7290000000002"),
        FixtureHttpClient({(LISTING_URL, QUERY): body}),
        FixedClock(),
    )

    page = await subject.discover(None, limit=10)

    assert len(page.files) == 1
    assert page.complete is True


async def test_matrix_accepts_a_nonempty_named_catalog_wrapper() -> None:
    body = b'{"files":[{"fileName":"Stores7290000000002-000-20260811.xml.gz","fileSize":42}]}'
    subject = MatrixSourceAdapter(
        MatrixSourceConfig("demo-matrix", "matrix:demo", "7290000000002"),
        FixtureHttpClient({(LISTING_URL, QUERY): body}),
        FixedClock(),
    )

    page = await subject.discover(None, limit=10)

    assert len(page.files) == 1
    assert page.complete is True


async def test_matrix_rejects_a_missing_expected_catalog_collection() -> None:
    http = FixtureHttpClient({(LISTING_URL, QUERY): b'{"error": []}'})
    subject = MatrixSourceAdapter(
        MatrixSourceConfig("demo-matrix", "matrix:demo", "7290000000002"),
        http,
        FixedClock(),
    )

    with pytest.raises(SourceResponseError, match="expected record collection"):
        await subject.discover(None, limit=10)


@pytest.mark.parametrize(
    "body",
    [
        b"[]",
        b'{"files": []}',
        b'{"data": []}',
        b'{"results": []}',
        b'{"items": []}',
        b'{"files": [], "data": []}',
    ],
    ids=(
        "direct-array",
        "empty-files-wrapper",
        "empty-data-wrapper",
        "empty-results-wrapper",
        "empty-items-wrapper",
        "ambiguous-wrapper",
    ),
)
async def test_matrix_requires_exactly_one_named_catalog_collection(body: bytes) -> None:
    subject = MatrixSourceAdapter(
        MatrixSourceConfig("demo-matrix", "matrix:demo", "7290000000002"),
        FixtureHttpClient({(LISTING_URL, QUERY): body}),
        FixedClock(),
    )

    with pytest.raises(SourceResponseError, match="expected record collection"):
        await subject.discover(None, limit=10)
