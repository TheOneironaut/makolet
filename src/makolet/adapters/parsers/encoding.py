"""Deterministic encoding detection for Israeli price-transparency XML."""

from __future__ import annotations

import codecs
import re

from makolet.domain.errors import MalformedDocumentError

_PROBE_BYTES = 4096
_UTF8_BOM = b"\xef\xbb\xbf"
_UTF16_LE_BOM = b"\xff\xfe"
_UTF16_BE_BOM = b"\xfe\xff"
_UTF32_SIGNATURES = (
    b"\x00\x00\xfe\xff",
    b"\xff\xfe\x00\x00",
    b"\x00\x00\x00<",
    b"<\x00\x00\x00",
)
_XML_DECLARATION = re.compile(
    rb"^(?:\xef\xbb\xbf)?[ \t\r\n]*<\?xml\b(?P<attributes>.*?)\?>",
    flags=re.IGNORECASE | re.DOTALL,
)
_ENCODING_ATTRIBUTE = re.compile(
    rb"\bencoding\s*=\s*([\"'])(?P<encoding>[a-z0-9._:-]+)\1",
    flags=re.IGNORECASE,
)
_UTF8_NAMES = frozenset({"utf8", "utf-8"})
_WINDOWS_1255_NAMES = frozenset({"cp1255", "windows1255", "windows-1255", "x-cp1255"})


class _StrictEncodingValidator:
    def __init__(self, codec: str) -> None:
        decoder_type = codecs.getincrementaldecoder(codec)
        self._decoder: codecs.IncrementalDecoder = decoder_type(errors="strict")
        self.valid = True

    def feed(self, chunk: bytes) -> None:
        if not self.valid:
            return
        try:
            self._decoder.decode(chunk, final=False)
        except UnicodeDecodeError:
            self.valid = False

    def finish(self) -> None:
        if not self.valid:
            return
        try:
            self._decoder.decode(b"", final=True)
        except UnicodeDecodeError:
            self.valid = False


class XmlEncodingProbe:
    """Validate candidate encodings without retaining the complete document."""

    def __init__(self) -> None:
        self._prefix = bytearray()
        self._utf8 = _StrictEncodingValidator("utf-8-sig")
        self._windows_1255 = _StrictEncodingValidator("cp1255")
        self._finished = False

    def feed(self, chunk: bytes) -> None:
        if self._finished:
            raise RuntimeError("XML encoding probe has already finished")
        if len(self._prefix) < _PROBE_BYTES:
            self._prefix.extend(chunk[: _PROBE_BYTES - len(self._prefix)])
        self._utf8.feed(chunk)
        self._windows_1255.feed(chunk)

    def resolve(self) -> str:
        if not self._finished:
            self._utf8.finish()
            self._windows_1255.finish()
            self._finished = True

        prefix = bytes(self._prefix)
        if any(prefix.startswith(signature) for signature in _UTF32_SIGNATURES):
            raise MalformedDocumentError("UTF-32 XML is not supported")
        if prefix.startswith((_UTF16_LE_BOM, _UTF16_BE_BOM)):
            return "utf-16"
        if utf16_codec := _utf16_without_bom(prefix):
            return utf16_codec
        if prefix.startswith(_UTF8_BOM):
            if not self._utf8.valid:
                raise MalformedDocumentError("UTF-8 BOM conflicts with the document bytes")
            return "utf-8-sig"

        declared = _declared_encoding(prefix)
        if declared in _WINDOWS_1255_NAMES and self._windows_1255.valid:
            return "cp1255"
        if declared in _UTF8_NAMES and self._utf8.valid:
            return "utf-8-sig"

        if self._utf8.valid and not self._windows_1255.valid:
            return "utf-8-sig"
        if self._windows_1255.valid and not self._utf8.valid:
            return "cp1255"
        if self._utf8.valid and self._windows_1255.valid:
            # ASCII is identical in both encodings. For the rare byte sequence that
            # is valid in both, UTF-8 is the interoperable default unless the source
            # explicitly and validly declares Windows-1255.
            return "utf-8-sig"
        raise MalformedDocumentError("XML bytes are neither valid UTF-8 nor Windows-1255")


def incremental_decoder(codec: str) -> codecs.IncrementalDecoder:
    decoder_type = codecs.getincrementaldecoder(codec)
    return decoder_type(errors="strict")


def _declared_encoding(prefix: bytes) -> str | None:
    declaration = _XML_DECLARATION.match(prefix)
    if declaration is None:
        return None
    match = _ENCODING_ATTRIBUTE.search(declaration.group("attributes"))
    if match is None:
        return None
    return match.group("encoding").decode("ascii").casefold()


def _utf16_without_bom(prefix: bytes) -> str | None:
    little_endian_whitespace = {b" \x00", b"\t\x00", b"\n\x00", b"\r\x00"}
    big_endian_whitespace = {b"\x00 ", b"\x00\t", b"\x00\n", b"\x00\r"}
    for offset in range(0, min(len(prefix), 128) - 1, 2):
        pair = prefix[offset : offset + 2]
        if pair in little_endian_whitespace:
            continue
        if pair == b"<\x00":
            return "utf-16-le"
        break
    for offset in range(0, min(len(prefix), 128) - 1, 2):
        pair = prefix[offset : offset + 2]
        if pair in big_endian_whitespace:
            continue
        if pair == b"\x00<":
            return "utf-16-be"
        break
    return None
