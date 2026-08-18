from __future__ import annotations

import pytest

from makolet.adapters.parsers.encoding import XmlEncodingProbe
from makolet.domain.errors import MalformedDocumentError


def resolve(*chunks: bytes) -> str:
    probe = XmlEncodingProbe()
    for chunk in chunks:
        probe.feed(chunk)
    return probe.resolve()


@pytest.mark.parametrize(
    "signature",
    [b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\x00\x00\x00<", b"<\x00\x00\x00"],
)
def test_utf32_signatures_are_rejected(signature: bytes) -> None:
    with pytest.raises(MalformedDocumentError, match="UTF-32"):
        resolve(signature + b"Root")


def test_utf8_bom_is_authoritative() -> None:
    with pytest.raises(MalformedDocumentError, match="BOM conflicts"):
        resolve(b"\xef\xbb\xbf<Root>\x81</Root>")


def test_incomplete_utf8_sequence_falls_back_to_windows_1255() -> None:
    assert resolve(b"<Root>\xe2\x82") == "cp1255"


def test_ascii_declaration_without_encoding_uses_utf8() -> None:
    assert resolve(b'<?xml version="1.0"?><Root/>') == "utf-8-sig"


@pytest.mark.parametrize(
    ("codec", "payload"),
    [
        ("utf-16-le", " \t\n<Root/>".encode("utf-16-le")),
        ("utf-16-be", " \t\n<Root/>".encode("utf-16-be")),
    ],
)
def test_utf16_without_bom_allows_xml_whitespace(codec: str, payload: bytes) -> None:
    assert resolve(payload) == codec


def test_probe_resolution_is_stable_and_closes_input() -> None:
    probe = XmlEncodingProbe()
    probe.feed(b"<Root/>")

    assert probe.resolve() == "utf-8-sig"
    assert probe.resolve() == "utf-8-sig"
    with pytest.raises(RuntimeError, match="already finished"):
        probe.feed(b"ignored")
