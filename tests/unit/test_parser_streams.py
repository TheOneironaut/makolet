from __future__ import annotations

import gzip
import io
import zipfile
from collections.abc import AsyncIterator
from compression import zstd

import pytest

from makolet.adapters.parsers.streams import CompressionLimits, decompress_chunks
from makolet.domain.enums import CompressionFormat
from makolet.domain.errors import UnsafeArchiveError


async def stream(value: bytes, size: int = 3) -> AsyncIterator[bytes]:
    for offset in range(0, len(value), size):
        yield value[offset : offset + size]


async def collect(
    value: bytes,
    compression: CompressionFormat,
    limits: CompressionLimits | None = None,
) -> bytes:
    output = bytearray()
    async for chunk in decompress_chunks(
        stream(value), compression, limits=limits or CompressionLimits()
    ):
        output.extend(chunk)
    return bytes(output)


@pytest.mark.asyncio
@pytest.mark.parametrize("compression", [CompressionFormat.NONE, CompressionFormat.GZIP])
async def test_stream_round_trip(compression: CompressionFormat) -> None:
    payload = b"<Root>safe</Root>" * 100
    encoded = gzip.compress(payload, mtime=0) if compression is CompressionFormat.GZIP else payload

    assert await collect(encoded, compression) == payload


@pytest.mark.asyncio
async def test_zstandard_stream_round_trip() -> None:
    payload = b"<Root>zstd</Root>" * 100
    assert await collect(zstd.compress(payload), CompressionFormat.ZSTANDARD) == payload


@pytest.mark.asyncio
@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
async def test_zip_stream_round_trip(method: int) -> None:
    payload = b"<Root>zip</Root>"
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w", compression=method) as archive:
        archive.writestr("nested/prices.xml", payload)

    assert await collect(target.getvalue(), CompressionFormat.ZIP) == payload


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method",
    [zipfile.ZIP_BZIP2, zipfile.ZIP_LZMA, zipfile.ZIP_ZSTANDARD],
)
async def test_zip_unsupported_member_is_rejected_before_decompression(
    method: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w", compression=method) as archive:
        archive.writestr("prices.xml", b"<Root>unsupported</Root>")

    def fail_if_opened(*args: object, **kwargs: object) -> None:
        del args, kwargs
        pytest.fail("unsupported ZIP member decoder was opened")

    monkeypatch.setattr(
        "makolet.adapters.parsers.streams.zipfile.ZipFile.open",
        fail_if_opened,
    )

    with pytest.raises(UnsafeArchiveError, match="compression method"):
        await collect(target.getvalue(), CompressionFormat.ZIP)


@pytest.mark.asyncio
async def test_truncated_gzip_is_rejected() -> None:
    encoded = gzip.compress(b"payload", mtime=0)
    with pytest.raises(UnsafeArchiveError, match="truncated"):
        await collect(encoded[:-4], CompressionFormat.GZIP)


@pytest.mark.asyncio
async def test_decompressed_byte_limit_is_enforced_during_streaming() -> None:
    encoded = gzip.compress(b"x" * 100, mtime=0)
    limits = CompressionLimits(maximum_decompressed_bytes=50)
    with pytest.raises(UnsafeArchiveError, match="decompressed-byte"):
        await collect(encoded, CompressionFormat.GZIP, limits)


@pytest.mark.asyncio
async def test_zip_slip_member_is_rejected() -> None:
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w") as archive:
        archive.writestr("../outside.xml", b"<Root />")

    with pytest.raises(UnsafeArchiveError, match="path"):
        await collect(target.getvalue(), CompressionFormat.ZIP)


@pytest.mark.asyncio
async def test_zip_with_multiple_documents_is_rejected() -> None:
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w") as archive:
        archive.writestr("one.xml", b"<One />")
        archive.writestr("two.xml", b"<Two />")

    with pytest.raises(UnsafeArchiveError, match="exactly one"):
        await collect(target.getvalue(), CompressionFormat.ZIP)
