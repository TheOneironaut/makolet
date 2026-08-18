from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from makolet.adapters.sources.city import CitySourceAdapter, CitySourceConfig
from makolet.adapters.sources.hazi import HaziSourceAdapter, HaziSourceConfig
from makolet.adapters.sources.shufersal import ShufersalSourceAdapter, ShufersalSourceConfig
from makolet.domain.enums import CompressionFormat, DocumentType
from makolet.domain.errors import SourceResponseError
from tests.unit.sources.support import FixedClock, FixtureHttpClient

FIXTURES = Path(__file__).with_name("fixtures")


async def test_hazi_discovers_only_stable_allowlisted_azure_objects() -> None:
    listing_url = "https://shop.hazi-hinam.co.il/Prices"
    query = (("d", "2026-08-11"), ("page", "1"))
    http = FixtureHttpClient({(listing_url, query): (FIXTURES / "hazi-page.html").read_bytes()})
    subject = HaziSourceAdapter(HaziSourceConfig(), http, FixedClock())

    page = await subject.discover(None, limit=10)

    assert len(page.files) == 1
    assert page.files[0].portal_id == "hazi-hinam:prices"
    assert page.files[0].download_url.startswith(
        "https://hazihinamprod01.blob.core.windows.net/regulatories/"
    )
    assert page.files[0].last_modified == datetime(2026, 8, 11, 9, tzinfo=UTC)
    assert page.next_cursor is not None


async def test_hazi_rejects_lexically_truncated_html_instead_of_terminal_empty() -> None:
    listing_url = "https://shop.hazi-hinam.co.il/Prices"
    query = (("d", "2026-08-11"), ("page", "1"))
    subject = HaziSourceAdapter(
        HaziSourceConfig(),
        FixtureHttpClient({(listing_url, query): b"<html"}),
        FixedClock(),
    )

    with pytest.raises(SourceResponseError, match="ended inside markup"):
        await subject.discover(None, limit=10)


@pytest.mark.parametrize(
    "body",
    [
        b"temporarily unavailable",
        b"<html><body><div>temporarily unavailable</div></body></html>",
    ],
    ids=("plain-text", "complete-error-page"),
)
async def test_hazi_rejects_error_responses_instead_of_terminal_empty(body: bytes) -> None:
    listing_url = "https://shop.hazi-hinam.co.il/Prices"
    query = (("d", "2026-08-11"), ("page", "1"))
    subject = HaziSourceAdapter(
        HaziSourceConfig(),
        FixtureHttpClient({(listing_url, query): body}),
        FixedClock(),
    )

    with pytest.raises(SourceResponseError):
        await subject.discover(None, limit=10)


async def test_city_preserves_opaque_uuid_route_and_marks_extensionless_file_as_gzip() -> None:
    listing_url = "https://www.citymarket-shops.co.il/"
    query = (("d", "2026-08-11"), ("page", "1"))
    http = FixtureHttpClient({(listing_url, query): (FIXTURES / "city-page.html").read_bytes()})
    subject = CitySourceAdapter(CitySourceConfig(), http, FixedClock())

    page = await subject.discover(None, limit=10)

    assert page.complete is True
    assert page.files[0].original_filename == "Price7290000000006-001-202608111210"
    assert page.files[0].document_type is DocumentType.PRICE_DELTA
    assert page.files[0].compression is CompressionFormat.GZIP
    assert page.files[0].download_url.endswith("123e4567-e89b-12d3-a456-426614174000")


async def test_city_accepts_a_complete_page_with_optional_html_end_tag() -> None:
    listing_url = "https://www.citymarket-shops.co.il/"
    query = (("d", "2026-08-11"), ("page", "1"))
    body = (FIXTURES / "city-page.html").read_bytes().replace(b"</html>", b"")
    subject = CitySourceAdapter(
        CitySourceConfig(),
        FixtureHttpClient({(listing_url, query): body}),
        FixedClock(),
    )

    page = await subject.discover(None, limit=10)

    assert len(page.files) == 1
    assert page.complete is True


async def test_city_rejects_a_structurally_incomplete_terminal_listing() -> None:
    listing_url = "https://www.citymarket-shops.co.il/"
    query = (("d", "2026-08-11"), ("page", "1"))
    body = (FIXTURES / "city-page.html").read_bytes().replace(b"</body></html>", b"")
    subject = CitySourceAdapter(
        CitySourceConfig(),
        FixtureHttpClient({(listing_url, query): body}),
        FixedClock(),
    )

    with pytest.raises(SourceResponseError, match="ended before"):
        await subject.discover(None, limit=10)


async def test_shufersal_signed_url_is_downloadable_but_not_stable_identity() -> None:
    listing_url = "https://prices.shufersal.co.il/FileObject/UpdateCategory"
    query = (("catID", "0"), ("storeId", "0"), ("page", "1"))
    first_body = (FIXTURES / "shufersal-page.html").read_bytes()
    second_body = first_body.replace(b"sig=first", b"sig=second")
    first_http = FixtureHttpClient({(listing_url, query): first_body})
    second_http = FixtureHttpClient({(listing_url, query): second_body})

    first = await ShufersalSourceAdapter(
        ShufersalSourceConfig(), first_http, FixedClock()
    ).discover(None, limit=10)
    second = await ShufersalSourceAdapter(
        ShufersalSourceConfig(), second_http, FixedClock()
    ).discover(None, limit=10)

    assert first.files[0].download_url != second.files[0].download_url
    assert first.files[0].remote_id == second.files[0].remote_id
    assert "sig=" not in first.files[0].remote_id
    assert first.files[0].compression is CompressionFormat.GZIP
    assert first.next_cursor is not None
