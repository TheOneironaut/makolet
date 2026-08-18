from __future__ import annotations

from datetime import UTC, datetime

import pytest

from makolet.adapters.sources import common as source_common
from makolet.adapters.sources.common import (
    build_remote_file,
    decode_cursor,
    encode_cursor,
    filename_from_link,
    json_records,
    listing_page_numbers,
    optional_listing_timestamp,
    parse_html_links,
    parse_json_listing,
)
from makolet.domain.enums import CompressionFormat, DocumentType
from makolet.domain.errors import DomainValidationError, SourceResponseError, UnsafeRemoteError
from makolet.domain.models import RemoteFile


def test_html_rows_recover_filename_and_bounded_pagination_metadata() -> None:
    links = parse_html_links(
        b"""
        <table><tr><td>PriceFull7290000000001-001-202608111200.GZ</td>
        <td><a href='/download/object'>download</a></td></tr></table>
        <a href='?page=2' data-total-pages='7'>2</a>
        """
    )

    assert filename_from_link(links[0]) == "PriceFull7290000000001-001-202608111200.GZ"
    assert listing_page_numbers(links) == (2, 7)


def test_html_date_page_is_not_mistaken_for_regulated_file() -> None:
    link = parse_html_links(b"<a href='2026-05-29.html'>2026-05-29</a>")[0]
    assert filename_from_link(link) is None


def test_html_parser_bounds_structure_and_retained_text_during_parsing() -> None:
    with pytest.raises(SourceResponseError, match="oversized tag"):
        parse_html_links(b"<a data-value='" + b"x" * (64 * 1024) + b"'>file</a>")

    too_many_links = b"<a>x</a>" * (source_common.MAXIMUM_LISTING_RECORDS + 1)
    with pytest.raises(SourceResponseError, match="too many links"):
        parse_html_links(too_many_links)

    retained = parse_html_links(b"<tr><td>" + b"x" * 100_000 + b"</td><td><a>x</a></td></tr>")
    assert len(retained[0].row_text) <= 4_096


@pytest.mark.parametrize(
    "body",
    [
        b"<a href='Stores.xml'>stores",
        b"<tr><td>Stores.xml</td><td><a href='/download'>download</a>",
        b"<table><a href='Stores.xml'>stores</a>",
        b"<html><body><a href='Stores.xml'>stores</a>",
        b"<div><a href='Stores.xml'>stores</a>",
        b"<script>window.catalog = []",
    ],
    ids=("anchor", "row", "table", "document", "generic-container", "raw-text-container"),
)
def test_html_parser_rejects_eof_with_relevant_open_structure(body: bytes) -> None:
    with pytest.raises(SourceResponseError, match="ended before"):
        parse_html_links(body)


@pytest.mark.parametrize(
    "body",
    [
        b"<html",
        b"<a href=",
        b"<!-- dangling",
        b"<!DOCTYPE html",
        b"<?xml version='1.0'",
    ],
    ids=("start-tag", "attribute", "comment", "doctype", "instruction"),
)
def test_html_parser_rejects_lexically_incomplete_markup_at_eof(body: bytes) -> None:
    with pytest.raises(SourceResponseError, match="ended inside markup"):
        parse_html_links(body)


def test_html_parser_rejects_nested_anchors_instead_of_overwriting_open_state() -> None:
    with pytest.raises(SourceResponseError, match="nested anchors"):
        parse_html_links(b"<a href='first'>one<a href='second'>two</a></a>")


@pytest.mark.parametrize("body", [b"", b"temporarily unavailable"])
def test_html_parser_rejects_responses_without_html_markup(body: bytes) -> None:
    with pytest.raises(SourceResponseError, match="HTML markup"):
        parse_html_links(body)


def test_html_parser_accepts_complete_pages_and_complete_fragments() -> None:
    full_page = parse_html_links(
        b"<html><body><table><tr><td><a href='Stores.xml'>stores</a></td>"
        b"</tr></table></body></html>"
    )
    anchor_fragment = parse_html_links(b"<a href='PriceFull.xml'>price</a>")
    row_fragment = parse_html_links(
        b"<tr><td>Stores.xml</td><td><a href='/download'>download</a></td></tr>"
    )
    optional_root_end = parse_html_links(b"<html><body><a href='Stores.xml'>stores</a></body>")

    assert [link.href for link in full_page] == ["Stores.xml"]
    assert [link.href for link in anchor_fragment] == ["PriceFull.xml"]
    assert [link.href for link in row_fragment] == ["/download"]
    assert [link.href for link in optional_root_end] == ["Stores.xml"]


def test_json_preflight_rejects_object_graph_bombs_before_json_loads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = b"[" + b"0," * (source_common.MAXIMUM_JSON_STRUCTURAL_TOKENS + 1) + b"0]"

    def must_not_load(_value: str) -> object:
        raise AssertionError("json.loads must not receive structurally unbounded input")

    monkeypatch.setattr("makolet.adapters.sources.common.json.loads", must_not_load)
    with pytest.raises(SourceResponseError, match="structurally too complex"):
        parse_json_listing(body)


def test_json_preflight_accepts_a_legitimate_listing() -> None:
    assert parse_json_listing(b'{"files":[{"name":"Stores.xml","size":42}]}') == {
        "files": [{"name": "Stores.xml", "size": 42}]
    }


def test_json_records_requires_exact_named_or_direct_list_collections() -> None:
    assert json_records({"files": []}, ("files",)) == ()
    assert json_records([], ()) == ()

    with pytest.raises(SourceResponseError, match="record collection"):
        json_records([], ("files",))
    with pytest.raises(SourceResponseError, match="record collection"):
        json_records({"error": "temporarily unavailable"}, ("files",))
    with pytest.raises(SourceResponseError, match="record collection"):
        json_records({"unrelated": []}, ("files",))
    with pytest.raises(SourceResponseError, match="record collection"):
        json_records({"Files": []}, ("files",))
    with pytest.raises(SourceResponseError, match="record collection"):
        json_records({"files": [], "data": []}, ("files", "data"))
    with pytest.raises(SourceResponseError, match="record collection"):
        json_records({"files": []}, ())


def test_embedded_json_value_preflight_enforces_listing_byte_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(source_common, "MAXIMUM_LISTING_BYTES", 4)

    with pytest.raises(SourceResponseError, match="parser byte limit"):
        source_common.bounded_json_value_span("[    ]")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-08-11T10:00:00Z", datetime(2026, 8, 11, 10, tzinfo=UTC)),
        ("2026-08-11 21:08:28", datetime(2026, 8, 11, 18, 8, 28, tzinfo=UTC)),
        ("8/11/2026 8:00:00 PM", datetime(2026, 8, 11, 17, tzinfo=UTC)),
        ("2026-10-25T01:30:00+03:00", datetime(2026, 10, 24, 22, 30, tzinfo=UTC)),
    ],
)
def test_optional_listing_timestamp_preserves_or_safely_localizes_valid_instants(
    value: str,
    expected: datetime,
) -> None:
    assert optional_listing_timestamp(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "2026-03-27T02:30:00",
        "2026-10-25T01:30:00",
        "not-a-time",
        "",
        None,
        42,
    ],
)
def test_optional_listing_timestamp_omits_invalid_optional_metadata(value: object) -> None:
    assert optional_listing_timestamp(value) is None


def test_cursor_is_source_bound_and_tamper_evident() -> None:
    encoded = encode_cursor("source-a", {"offset": 3})
    assert decode_cursor("source-a", encoded)["offset"] == 3
    with pytest.raises(DomainValidationError, match="another source"):
        decode_cursor("source-b", encoded)
    with pytest.raises(DomainValidationError, match="invalid"):
        decode_cursor("source-a", "not+base64")


def test_remote_file_builder_strips_signed_query_from_identity() -> None:
    first = _remote("https://files.example/PriceFull7290000000001.GZ?sig=one")
    second = _remote("https://files.example/PriceFull7290000000001.GZ?sig=two")

    assert first.remote_id == second.remote_id
    assert first.document_type is DocumentType.PRICE_FULL
    assert first.compression is CompressionFormat.GZIP


def test_remote_file_builder_rejects_credentials_and_unlisted_hosts() -> None:
    with pytest.raises(UnsafeRemoteError, match="credentials"):
        _remote("https://user:pass@files.example/Stores.xml", filename="Stores.xml")
    with pytest.raises(UnsafeRemoteError, match="allowlist"):
        _remote("https://evil.example/Stores.xml", filename="Stores.xml")


def _remote(
    url: str,
    *,
    filename: str = "PriceFull7290000001-001-202608111200.GZ",
) -> RemoteFile:
    return build_remote_file(
        retailer_id="demo",
        portal_id="demo:portal",
        download_url=url,
        original_filename=filename,
        discovered_at=datetime(2026, 8, 11, tzinfo=UTC),
        allowed_hosts=frozenset({"files.example"}),
        allowed_schemes=frozenset({"https"}),
    )
