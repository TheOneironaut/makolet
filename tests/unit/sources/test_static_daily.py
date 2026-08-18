from __future__ import annotations

import json
from pathlib import Path

import pytest

from makolet.adapters.sources import common as source_common
from makolet.adapters.sources.static_daily import (
    StaticDailyFeedConfig,
    StaticDailySourceAdapter,
    StaticDailySourceConfig,
)
from makolet.domain.enums import CompressionFormat, DocumentType
from makolet.domain.errors import SourceResponseError, UnsafeRemoteError
from tests.unit.sources.support import FixedClock, FixtureHttpClient

FIXTURES = Path(__file__).with_name("fixtures")


async def test_wolt_selects_latest_published_day_instead_of_assuming_today() -> None:
    index_url = "https://wm-gateway.wolt.com/isr-prices/public/v1/index.html"
    day_url = "https://wm-gateway.wolt.com/isr-prices/public/v1/2026-05-29.html"
    http = FixtureHttpClient(
        {
            (index_url, ()): (FIXTURES / "wolt-index.html").read_bytes(),
            (day_url, ()): (FIXTURES / "wolt-day.html").read_bytes(),
        }
    )
    feed = StaticDailyFeedConfig(
        "wolt:public",
        index_url,
        frozenset({"wm-gateway.wolt.com"}),
        frozenset({"wm-gateway.wolt.com"}),
    )
    subject = StaticDailySourceAdapter(
        StaticDailySourceConfig("wolt-demo", (feed,)),
        http,
        FixedClock(),
    )

    page = await subject.discover(None, limit=10)

    assert page.complete is True
    assert len(page.files) == 2
    assert {item.document_type for item in page.files} == {
        DocumentType.PRICE_FULL,
        DocumentType.STORES,
    }
    assert all("2026-05-29" in item.download_url for item in page.files)


async def test_wolt_rejects_a_structurally_incomplete_terminal_day_page() -> None:
    index_url = "https://wm-gateway.wolt.com/isr-prices/public/v1/index.html"
    day_url = "https://wm-gateway.wolt.com/isr-prices/public/v1/2026-05-29.html"
    incomplete_day = (FIXTURES / "wolt-day.html").read_bytes().replace(b"</body></html>", b"")
    http = FixtureHttpClient(
        {
            (index_url, ()): (FIXTURES / "wolt-index.html").read_bytes(),
            (day_url, ()): incomplete_day,
        }
    )
    feed = StaticDailyFeedConfig(
        "wolt:public",
        index_url,
        frozenset({"wm-gateway.wolt.com"}),
        frozenset({"wm-gateway.wolt.com"}),
    )
    subject = StaticDailySourceAdapter(
        StaticDailySourceConfig("wolt-demo", (feed,)),
        http,
        FixedClock(),
    )

    with pytest.raises(SourceResponseError, match="ended before"):
        await subject.discover(None, limit=10)


@pytest.mark.parametrize(
    "day_body",
    [
        b"<div>temporarily unavailable</div>",
        b"<a href='status.html'>status</a>",
    ],
    ids=("error-page", "unrelated-link"),
)
async def test_wolt_rejects_a_complete_day_page_without_source_files(day_body: bytes) -> None:
    index_url = "https://wm-gateway.wolt.com/isr-prices/public/v1/index.html"
    day_url = "https://wm-gateway.wolt.com/isr-prices/public/v1/2026-05-29.html"
    http = FixtureHttpClient(
        {
            (index_url, ()): (FIXTURES / "wolt-index.html").read_bytes(),
            (day_url, ()): day_body,
        }
    )
    feed = StaticDailyFeedConfig(
        "wolt:public",
        index_url,
        frozenset({"wm-gateway.wolt.com"}),
        frozenset({"wm-gateway.wolt.com"}),
    )
    subject = StaticDailySourceAdapter(
        StaticDailySourceConfig("wolt-demo", (feed,)),
        http,
        FixedClock(),
    )

    with pytest.raises(SourceResponseError, match="no recognized source files"):
        await subject.discover(None, limit=10)


@pytest.mark.parametrize("port", [0, 80])
async def test_static_daily_rejects_https_date_page_on_non_default_port(port: int) -> None:
    index_url = "https://wm-gateway.wolt.com/isr-prices/public/v1/index.html"
    hostile_index = b"""
    <html><body>
      <a href="https://wm-gateway.wolt.com:{port}/2026-05-29.html">2026-05-29</a>
    </body></html>
    """.replace(b"{port}", str(port).encode("ascii"))
    feed = StaticDailyFeedConfig(
        "wolt:public",
        index_url,
        frozenset({"wm-gateway.wolt.com"}),
        frozenset({"wm-gateway.wolt.com"}),
    )
    subject = StaticDailySourceAdapter(
        StaticDailySourceConfig("wolt-demo", (feed,)),
        FixtureHttpClient({(index_url, ()): hostile_index}),
        FixedClock(),
    )

    with pytest.raises(UnsafeRemoteError, match="non-default HTTPS port"):
        await subject.discover(None, limit=10)


async def test_carrefour_reads_direct_daily_objects_and_mixed_case_xml() -> None:
    index_url = "https://prices.carrefour.co.il/"
    http = FixtureHttpClient({(index_url, ()): (FIXTURES / "carrefour-index.html").read_bytes()})
    feed = StaticDailyFeedConfig(
        "carrefour:primary",
        index_url,
        frozenset({"prices.carrefour.co.il"}),
        frozenset({"prices.carrefour.co.il"}),
    )
    subject = StaticDailySourceAdapter(
        StaticDailySourceConfig("carrefour-demo", (feed,)),
        http,
        FixedClock(),
    )

    page = await subject.discover(None, limit=10)

    assert len(page.files) == 2
    stores = next(item for item in page.files if item.document_type is DocumentType.STORES)
    assert stores.original_filename.endswith(".XML")
    assert stores.compression is CompressionFormat.NONE
    assert stores.content_length == 61_394
    assert all("20260811" in item.download_url for item in page.files)


@pytest.mark.parametrize(
    ("limit_name", "message"),
    [
        ("depth", "nested too deeply"),
        ("tokens", "structurally too complex"),
        ("string", "oversized string"),
        ("scalar", "oversized scalar"),
    ],
)
async def test_embedded_catalog_preflights_json_limits_before_decoder(
    monkeypatch: pytest.MonkeyPatch,
    limit_name: str,
    message: str,
) -> None:
    decoder_calls = 0

    def must_not_decode(
        _decoder: json.JSONDecoder,
        _document: str,
        _index: int = 0,
    ) -> tuple[object, int]:
        nonlocal decoder_calls
        decoder_calls += 1
        raise AssertionError("raw_decode must not receive an unbounded embedded value")

    monkeypatch.setattr(json.JSONDecoder, "raw_decode", must_not_decode)
    subject = _embedded_subject(_embedded_page(_over_limit_json_value(limit_name)))

    with pytest.raises(SourceResponseError, match=message):
        await subject.discover(None, limit=10)

    assert decoder_calls == 0


@pytest.mark.parametrize(
    "decoder_error",
    [
        json.JSONDecodeError("invalid", "[]", 0),
        RecursionError("decoder recursion limit"),
    ],
    ids=("json", "recursion"),
)
async def test_embedded_catalog_maps_decoder_failures_to_source_error(
    monkeypatch: pytest.MonkeyPatch,
    decoder_error: Exception,
) -> None:
    def fail_decode(
        _decoder: json.JSONDecoder,
        _document: str,
        _index: int = 0,
    ) -> tuple[object, int]:
        raise decoder_error

    monkeypatch.setattr(json.JSONDecoder, "raw_decode", fail_decode)
    subject = _embedded_subject(_embedded_page('[{"name":"Stores.xml","size":42}]'))

    with pytest.raises(SourceResponseError, match="embedded catalog is malformed"):
        await subject.discover(None, limit=10)


async def test_embedded_catalog_accepts_one_json_value_before_trailing_javascript() -> None:
    body = _embedded_page(
        '[{"name":"Stores7290000000004-000-20260811.xml","size":42}]',
        trailing="window.catalogReady = true; renderPage();",
    )

    page = await _embedded_subject(body).discover(None, limit=10)

    assert page.complete is True
    assert len(page.files) == 1
    assert page.files[0].original_filename == "Stores7290000000004-000-20260811.xml"
    assert page.files[0].content_length == 42


async def test_embedded_catalog_requires_a_direct_array_and_preserves_explicit_empty() -> None:
    with pytest.raises(SourceResponseError, match="record collection"):
        await _embedded_subject(_embedded_page('{"files": []}')).discover(None, limit=10)

    page = await _embedded_subject(_embedded_page("[]")).discover(None, limit=10)

    assert page.files == ()
    assert page.complete is True


def _embedded_subject(body: bytes) -> StaticDailySourceAdapter:
    index_url = "https://prices.carrefour.co.il/"
    feed = StaticDailyFeedConfig(
        "carrefour:primary",
        index_url,
        frozenset({"prices.carrefour.co.il"}),
        frozenset({"prices.carrefour.co.il"}),
    )
    return StaticDailySourceAdapter(
        StaticDailySourceConfig("carrefour-demo", (feed,)),
        FixtureHttpClient({(index_url, ()): body}),
        FixedClock(),
    )


def _embedded_page(value: str, *, trailing: str = "") -> bytes:
    return (
        "<!doctype html><html><body><script>"
        "const path = '20260811';"
        f"const files = {value};"
        f"{trailing}"
        "</script></body></html>"
    ).encode()


def _over_limit_json_value(limit_name: str) -> str:
    if limit_name == "depth":
        depth = source_common.MAXIMUM_JSON_DEPTH + 1
        return "[" * depth + "0" + "]" * depth
    if limit_name == "tokens":
        return "[" + "0," * (source_common.MAXIMUM_JSON_STRUCTURAL_TOKENS + 1) + "0]"
    if limit_name == "string":
        value = '"' * (source_common.MAXIMUM_JSON_STRING_CHARACTERS + 1)
        return json.dumps([{"name": value}], separators=(",", ":"))
    if limit_name == "scalar":
        return "[" + "1" * (source_common.MAXIMUM_JSON_SCALAR_CHARACTERS + 1) + "]"
    raise AssertionError(f"unknown JSON limit {limit_name}")
