from __future__ import annotations

import gzip
import io
import struct
import zipfile
from collections.abc import AsyncIterator, Callable
from compression import zstd

import pytest

from makolet.adapters.parsers.streams import CompressionLimits, decompress_chunks
from makolet.domain.enums import CompressionFormat
from makolet.domain.errors import UnsafeArchiveError

_TWO_GIB_WINDOW_EMPTY_ZSTANDARD_FRAME = bytes.fromhex("28b52ffd00a8010000")


async def _chunks(payload: bytes, size: int | None = None) -> AsyncIterator[bytes]:
    chunk_size = size or len(payload) or 1
    for offset in range(0, len(payload), chunk_size):
        yield payload[offset : offset + chunk_size]


async def _collect(
    payload: bytes,
    compression: CompressionFormat,
    limits: CompressionLimits,
) -> bytes:
    output = bytearray()
    async for chunk in decompress_chunks(_chunks(payload), compression, limits=limits):
        output.extend(chunk)
    return bytes(output)


async def _consume_into(
    output: bytearray,
    payload: bytes,
    compression: CompressionFormat,
    limits: CompressionLimits,
) -> None:
    async for chunk in decompress_chunks(_chunks(payload), compression, limits=limits):
        output.extend(chunk)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("compression", "compress"),
    [
        (CompressionFormat.GZIP, lambda payload: gzip.compress(payload, mtime=0)),
        (CompressionFormat.ZSTANDARD, zstd.compress),
    ],
)
async def test_expansion_ratio_is_enforced_before_excess_output_is_yielded(
    compression: CompressionFormat,
    compress: Callable[[bytes], bytes],
) -> None:
    encoded = compress(b"x" * 10_000)
    limits = CompressionLimits(maximum_expansion_ratio=2, maximum_chunk_bytes=8)
    emitted = bytearray()

    with pytest.raises(UnsafeArchiveError, match="expansion-ratio"):
        await _consume_into(emitted, encoded, compression, limits)

    assert len(emitted) <= len(encoded) * limits.maximum_expansion_ratio


@pytest.mark.asyncio
async def test_zstandard_decoder_is_constructed_with_window_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class RejectingDecompressor:
        eof = False
        needs_input = True
        unused_data = b""

        def decompress(self, data: bytes, max_length: int) -> bytes:
            del data, max_length
            raise zstd.ZstdError("controlled test rejection")

    def build_decompressor(
        zstd_dict: object = None,
        options: object = None,
    ) -> RejectingDecompressor:
        del zstd_dict
        observed["options"] = options
        return RejectingDecompressor()

    monkeypatch.setattr(
        "makolet.adapters.parsers.streams.zstd.ZstdDecompressor",
        build_decompressor,
    )
    limits = CompressionLimits(maximum_zstandard_window_log=20)

    with pytest.raises(UnsafeArchiveError, match="Zstandard payload is invalid"):
        await _collect(b"frame", CompressionFormat.ZSTANDARD, limits)

    assert observed["options"] == {
        zstd.DecompressionParameter.window_log_max: limits.maximum_zstandard_window_log
    }


def test_zstandard_runtime_rejects_large_window_with_configured_bound() -> None:
    decompressor = zstd.ZstdDecompressor(options={zstd.DecompressionParameter.window_log_max: 20})

    with pytest.raises(zstd.ZstdError, match="requires too much memory"):
        decompressor.decompress(_TWO_GIB_WINDOW_EMPTY_ZSTANDARD_FRAME)


@pytest.mark.asyncio
async def test_zstandard_window_limit_preserves_supported_frames() -> None:
    payload = b"bounded-zstandard-frame" * 100
    limits = CompressionLimits(maximum_zstandard_window_log=20)

    assert await _collect(zstd.compress(payload), CompressionFormat.ZSTANDARD, limits) == payload


@pytest.mark.parametrize("value", [9, 32])
def test_zstandard_window_log_must_be_supported_by_runtime(value: int) -> None:
    with pytest.raises(ValueError, match="window log"):
        CompressionLimits(maximum_zstandard_window_log=value)


def _zip_with_entries(*names: str) -> bytearray:
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in names:
            archive.writestr(name, b"<Root />")
    return bytearray(target.getvalue())


def _forbid_zipfile_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_called(*args: object, **kwargs: object) -> None:
        del args, kwargs
        pytest.fail("zipfile.ZipFile was constructed before bounded metadata validation")

    monkeypatch.setattr("makolet.adapters.parsers.streams.zipfile.ZipFile", fail_if_called)


@pytest.mark.asyncio
async def test_entry_count_is_rejected_before_zipfile_allocates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encoded = bytes(_zip_with_entries("one.xml", "two.xml"))
    _forbid_zipfile_construction(monkeypatch)

    with pytest.raises(UnsafeArchiveError, match="too many entries"):
        await _collect(
            encoded,
            CompressionFormat.ZIP,
            CompressionLimits(maximum_zip_entries=1),
        )


@pytest.mark.asyncio
async def test_inconsistent_central_directory_count_is_rejected_before_zipfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encoded = _zip_with_entries("one.xml", "two.xml")
    end_offset = encoded.rfind(b"PK\x05\x06")
    struct.pack_into("<HH", encoded, end_offset + 8, 1, 1)
    _forbid_zipfile_construction(monkeypatch)

    with pytest.raises(UnsafeArchiveError, match="entry count is inconsistent"):
        await _collect(bytes(encoded), CompressionFormat.ZIP, CompressionLimits())


@pytest.mark.asyncio
async def test_zip64_sentinel_is_rejected_before_zipfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encoded = _zip_with_entries("prices.xml")
    end_offset = encoded.rfind(b"PK\x05\x06")
    struct.pack_into("<HH", encoded, end_offset + 8, 0xFFFF, 0xFFFF)
    _forbid_zipfile_construction(monkeypatch)

    with pytest.raises(UnsafeArchiveError, match="ZIP64"):
        await _collect(bytes(encoded), CompressionFormat.ZIP, CompressionLimits())


@pytest.mark.asyncio
async def test_central_directory_byte_limit_precedes_zipfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encoded = bytes(_zip_with_entries("prices.xml"))
    _forbid_zipfile_construction(monkeypatch)

    with pytest.raises(UnsafeArchiveError, match="central directory exceeds"):
        await _collect(
            encoded,
            CompressionFormat.ZIP,
            CompressionLimits(maximum_zip_directory_bytes=10),
        )


def test_zip_directory_limit_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        CompressionLimits(maximum_zip_directory_bytes=0)
