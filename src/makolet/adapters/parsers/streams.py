"""Bounded decompression of untrusted archived content."""

from __future__ import annotations

import os
import stat
import struct
import tempfile
import zipfile
import zlib
from collections.abc import AsyncIterator
from compression import zstd
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import anyio

from makolet.adapters.filesystem_capacity import (
    FileSystemCapacityGuard,
    FileSystemCapacityUnavailableError,
)
from makolet.domain.enums import CompressionFormat
from makolet.domain.errors import ArchiveCapacityError, UnsafeArchiveError

_DEFAULT_MAXIMUM_ZSTANDARD_WINDOW_LOG = 27  # 128 MiB decoder history.
_SUPPORTED_ZIP_COMPRESSION_METHODS = frozenset(
    {
        zipfile.ZIP_STORED,
        zipfile.ZIP_DEFLATED,
    }
)


@dataclass(frozen=True, slots=True)
class CompressionLimits:
    maximum_compressed_bytes: int = 2 * 1024 * 1024 * 1024
    maximum_decompressed_bytes: int = 8 * 1024 * 1024 * 1024
    maximum_expansion_ratio: int = 250
    maximum_zip_entries: int = 16
    maximum_zip_directory_bytes: int = 4 * 1024 * 1024
    maximum_chunk_bytes: int = 64 * 1024
    spool_memory_bytes: int = 8 * 1024 * 1024
    maximum_zstandard_window_log: int = _DEFAULT_MAXIMUM_ZSTANDARD_WINDOW_LOG

    def __post_init__(self) -> None:
        values = (
            self.maximum_compressed_bytes,
            self.maximum_decompressed_bytes,
            self.maximum_expansion_ratio,
            self.maximum_zip_entries,
            self.maximum_zip_directory_bytes,
            self.maximum_chunk_bytes,
            self.spool_memory_bytes,
        )
        if any(value <= 0 for value in values):
            raise ValueError("Compression limits must be positive")
        minimum_window_log, maximum_window_log = zstd.DecompressionParameter.window_log_max.bounds()
        if not minimum_window_log <= self.maximum_zstandard_window_log <= maximum_window_log:
            raise ValueError("Zstandard decoder window log is outside the runtime-supported range")


async def decompress_chunks(
    chunks: AsyncIterator[bytes],
    compression: CompressionFormat,
    *,
    limits: CompressionLimits,
    temporary_directory: Path | None = None,
    capacity_directory: Path | None = None,
    minimum_free_bytes: int = 0,
) -> AsyncIterator[bytes]:
    if minimum_free_bytes < 0:
        raise ValueError("decompression free-space reserve cannot be negative")
    if compression is CompressionFormat.NONE:
        async for output in _uncompressed(chunks, limits):
            yield output
        return
    if compression is CompressionFormat.GZIP:
        async for output in _gzip(chunks, limits):
            yield output
        return
    if compression is CompressionFormat.ZSTANDARD:
        async for output in _zstandard(chunks, limits):
            yield output
        return
    if compression is CompressionFormat.ZIP:
        async for output in _zip(
            chunks,
            limits,
            temporary_directory=temporary_directory,
            capacity_directory=capacity_directory,
            minimum_free_bytes=minimum_free_bytes,
        ):
            yield output
        return
    raise UnsafeArchiveError("Compression format is unknown")


async def _uncompressed(
    chunks: AsyncIterator[bytes], limits: CompressionLimits
) -> AsyncIterator[bytes]:
    total = 0
    async for chunk in chunks:
        total += len(chunk)
        if total > limits.maximum_decompressed_bytes:
            raise UnsafeArchiveError("Document exceeds the decompressed-byte limit")
        for offset in range(0, len(chunk), limits.maximum_chunk_bytes):
            yield chunk[offset : offset + limits.maximum_chunk_bytes]


async def _gzip(chunks: AsyncIterator[bytes], limits: CompressionLimits) -> AsyncIterator[bytes]:
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    compressed = 0
    decompressed = 0
    try:
        async for chunk in chunks:
            compressed += len(chunk)
            _check_compressed(compressed, limits)
            pending = chunk
            while pending:
                output = decompressor.decompress(pending, limits.maximum_chunk_bytes)
                pending = decompressor.unconsumed_tail
                if output:
                    decompressed += len(output)
                    _check_decompressed(decompressed, limits)
                    _check_ratio(compressed, decompressed, limits)
                    yield output
                if decompressor.eof:
                    if decompressor.unused_data or pending:
                        raise UnsafeArchiveError(
                            "Concatenated or trailing Gzip data is not allowed"
                        )
                    break
                if not output and not pending:
                    break
    except zlib.error as error:
        raise UnsafeArchiveError("Gzip payload is invalid") from error
    if not decompressor.eof:
        raise UnsafeArchiveError("Gzip payload is truncated")
    _check_ratio(compressed, decompressed, limits)


async def _zstandard(
    chunks: AsyncIterator[bytes], limits: CompressionLimits
) -> AsyncIterator[bytes]:
    decompressor = zstd.ZstdDecompressor(
        options={zstd.DecompressionParameter.window_log_max: (limits.maximum_zstandard_window_log)}
    )
    compressed = 0
    decompressed = 0
    try:
        async for chunk in chunks:
            compressed += len(chunk)
            _check_compressed(compressed, limits)
            pending = chunk
            while True:
                output = decompressor.decompress(pending, limits.maximum_chunk_bytes)
                pending = b""
                if output:
                    decompressed += len(output)
                    _check_decompressed(decompressed, limits)
                    _check_ratio(compressed, decompressed, limits)
                    yield output
                if decompressor.eof:
                    if decompressor.unused_data:
                        raise UnsafeArchiveError(
                            "Multiple or trailing Zstandard frames are not allowed"
                        )
                    break
                if decompressor.needs_input:
                    break
    except zstd.ZstdError as error:
        raise UnsafeArchiveError("Zstandard payload is invalid") from error
    if not decompressor.eof:
        raise UnsafeArchiveError("Zstandard payload is truncated")
    _check_ratio(compressed, decompressed, limits)


async def _zip(
    chunks: AsyncIterator[bytes],
    limits: CompressionLimits,
    *,
    temporary_directory: Path | None,
    capacity_directory: Path | None,
    minimum_free_bytes: int,
) -> AsyncIterator[bytes]:
    spool_path, coordination_path = await anyio.to_thread.run_sync(
        _resolved_spool_paths,
        temporary_directory,
        capacity_directory,
    )
    await anyio.Path(spool_path).mkdir(parents=True, exist_ok=True)
    capacity = FileSystemCapacityGuard(
        spool_path,
        minimum_free_bytes=minimum_free_bytes,
        coordination_directory=coordination_path,
    )
    with tempfile.SpooledTemporaryFile(
        max_size=limits.spool_memory_bytes,
        mode="w+b",
        prefix="makolet-compressed-",
        suffix=".zip",
        dir=str(spool_path),
    ) as spool:
        compressed = await _spool_compressed(
            chunks,
            spool,
            limits,
            capacity=capacity,
        )
        if compressed > limits.spool_memory_bytes:
            try:
                async with capacity.reserve_async(0):
                    await anyio.to_thread.run_sync(sync_spooled_file, spool)
            except FileSystemCapacityUnavailableError as error:
                raise ArchiveCapacityError(
                    "ZIP parser spool reached its configured free-space reserve"
                ) from error
        await anyio.to_thread.run_sync(_preflight_zip, spool, compressed, limits)
        await anyio.to_thread.run_sync(spool.seek, 0)
        try:
            archive = zipfile.ZipFile(spool)
        except zipfile.BadZipFile as error:
            raise UnsafeArchiveError("ZIP payload is invalid") from error
        with archive:
            entries = archive.infolist()
            if len(entries) > limits.maximum_zip_entries:
                raise UnsafeArchiveError("ZIP archive contains too many entries")
            payloads = [entry for entry in entries if not entry.is_dir()]
            if len(payloads) != 1:
                raise UnsafeArchiveError("ZIP archive must contain exactly one document")
            entry = payloads[0]
            _validate_zip_entry(entry, limits)
            decompressed = 0
            try:
                member = archive.open(entry, "r")
                with member:
                    while output := await anyio.to_thread.run_sync(
                        member.read, limits.maximum_chunk_bytes
                    ):
                        decompressed += len(output)
                        _check_decompressed(decompressed, limits)
                        _check_ratio(entry.compress_size, decompressed, limits)
                        yield output
            except (zipfile.BadZipFile, RuntimeError, NotImplementedError, OSError) as error:
                raise UnsafeArchiveError("ZIP member could not be decompressed safely") from error
            if decompressed != entry.file_size:
                raise UnsafeArchiveError("ZIP member length differs from its directory metadata")
            _check_ratio(compressed, decompressed, limits)


async def _spool_compressed(
    chunks: AsyncIterator[bytes],
    spool: tempfile.SpooledTemporaryFile[bytes],
    limits: CompressionLimits,
    *,
    capacity: FileSystemCapacityGuard,
) -> int:
    total = 0
    async for chunk in chunks:
        total += len(chunk)
        _check_compressed(total, limits)
        if total > limits.spool_memory_bytes:
            required_bytes = (
                total if total - len(chunk) <= limits.spool_memory_bytes else len(chunk)
            )
            try:
                async with capacity.reserve_async(required_bytes):
                    await anyio.to_thread.run_sync(spool.write, chunk)
                    await anyio.to_thread.run_sync(spool.flush)
            except FileSystemCapacityUnavailableError as error:
                raise ArchiveCapacityError(
                    "ZIP parser spool reached its configured free-space reserve"
                ) from error
        else:
            await anyio.to_thread.run_sync(spool.write, chunk)
    return total


def _resolved_spool_paths(
    temporary_directory: Path | None,
    capacity_directory: Path | None,
) -> tuple[Path, Path]:
    spool_path = (
        temporary_directory.resolve()
        if temporary_directory is not None
        else Path(tempfile.gettempdir()).resolve()
    )
    if capacity_directory is not None:
        coordination_path = capacity_directory.resolve()
    elif temporary_directory is not None:
        coordination_path = spool_path.parent
    else:
        coordination_path = spool_path
    return spool_path, coordination_path


def sync_spooled_file(spool: tempfile.SpooledTemporaryFile[bytes]) -> None:
    """Flush one already-spilled spool and make final allocation observable."""

    spool.flush()
    os.fsync(spool.fileno())


_ZIP_END_SIGNATURE = b"PK\x05\x06"
_ZIP64_LOCATOR_SIGNATURE = b"PK\x06\x07"
_ZIP_CENTRAL_SIGNATURE = b"PK\x01\x02"
_ZIP_END_RECORD = struct.Struct("<4s4H2LH")
_ZIP_CENTRAL_HEADER = struct.Struct("<4s6H3L5H2L")
_ZIP_MAXIMUM_COMMENT_BYTES = (1 << 16) - 1
_ZIP64_EXTRA_FIELD = 0x0001


@dataclass(frozen=True, slots=True)
class _ZipEndRecord:
    entries: int
    directory_size: int
    directory_offset: int
    record_offset: int


def _preflight_zip(
    spool: tempfile.SpooledTemporaryFile[bytes],
    file_size: int,
    limits: CompressionLimits,
) -> None:
    """Bound central-directory allocation before ``zipfile`` reads it wholesale."""

    end_record = _read_zip_end_record(spool, file_size)
    if end_record.entries > limits.maximum_zip_entries:
        raise UnsafeArchiveError("ZIP archive contains too many entries")
    if end_record.directory_size > limits.maximum_zip_directory_bytes:
        raise UnsafeArchiveError("ZIP central directory exceeds the byte limit")
    directory_end = end_record.directory_offset + end_record.directory_size
    if directory_end != end_record.record_offset:
        raise UnsafeArchiveError("ZIP central directory has invalid bounds")

    spool.seek(end_record.directory_offset)
    directory = spool.read(end_record.directory_size)
    if len(directory) != end_record.directory_size:
        raise UnsafeArchiveError("ZIP central directory is truncated")
    _validate_central_directory(directory, end_record)


def _read_zip_end_record(
    spool: tempfile.SpooledTemporaryFile[bytes], file_size: int
) -> _ZipEndRecord:
    if file_size < _ZIP_END_RECORD.size:
        raise UnsafeArchiveError("ZIP payload is invalid")
    tail_size = min(file_size, _ZIP_END_RECORD.size + _ZIP_MAXIMUM_COMMENT_BYTES)
    spool.seek(file_size - tail_size)
    tail = spool.read(tail_size)
    if len(tail) != tail_size:
        raise UnsafeArchiveError("ZIP payload is truncated")

    search_end = len(tail)
    while (relative_offset := tail.rfind(_ZIP_END_SIGNATURE, 0, search_end)) >= 0:
        search_end = relative_offset
        record_end = relative_offset + _ZIP_END_RECORD.size
        if record_end > len(tail):
            continue
        unpacked = _ZIP_END_RECORD.unpack_from(tail, relative_offset)
        comment_size = unpacked[-1]
        if record_end + comment_size != len(tail):
            continue
        record_offset = file_size - tail_size + relative_offset
        if (
            relative_offset >= 20
            and tail[relative_offset - 20 : relative_offset - 16] == _ZIP64_LOCATOR_SIGNATURE
        ):
            raise UnsafeArchiveError("ZIP64 archives are not supported")

        _, disk_number, directory_disk, disk_entries, entries, size, offset, _ = unpacked
        if disk_number != 0 or directory_disk != 0 or disk_entries != entries:
            raise UnsafeArchiveError("Multi-disk ZIP archives are not supported")
        if entries == 0xFFFF or size == 0xFFFFFFFF or offset == 0xFFFFFFFF:
            raise UnsafeArchiveError("ZIP64 archives are not supported")
        return _ZipEndRecord(
            entries=entries,
            directory_size=size,
            directory_offset=offset,
            record_offset=record_offset,
        )
    raise UnsafeArchiveError("ZIP end-of-directory record is missing")


def _validate_central_directory(directory: bytes, end_record: _ZipEndRecord) -> None:
    cursor = 0
    for _ in range(end_record.entries):
        header_end = cursor + _ZIP_CENTRAL_HEADER.size
        if header_end > len(directory):
            raise UnsafeArchiveError("ZIP central directory is truncated")
        header = _ZIP_CENTRAL_HEADER.unpack_from(directory, cursor)
        if header[0] != _ZIP_CENTRAL_SIGNATURE:
            raise UnsafeArchiveError("ZIP central directory entry is invalid")
        compressed_size = header[8]
        file_size = header[9]
        name_size, extra_size, comment_size = header[10:13]
        disk_number = header[13]
        local_header_offset = header[16]
        entry_end = header_end + name_size + extra_size + comment_size
        if entry_end > len(directory):
            raise UnsafeArchiveError("ZIP central directory entry is truncated")
        if disk_number != 0:
            raise UnsafeArchiveError("Multi-disk ZIP archives are not supported")
        if 0xFFFFFFFF in (compressed_size, file_size, local_header_offset):
            raise UnsafeArchiveError("ZIP64 archives are not supported")
        if local_header_offset >= end_record.directory_offset:
            raise UnsafeArchiveError("ZIP member offset is outside the payload area")

        name_end = header_end + name_size
        if b"\x00" in directory[header_end:name_end]:
            raise UnsafeArchiveError("ZIP member name contains a null byte")
        extra_end = name_end + extra_size
        _reject_zip64_extra(directory[name_end:extra_end])
        cursor = entry_end
    if cursor != len(directory):
        raise UnsafeArchiveError("ZIP central directory entry count is inconsistent")


def _reject_zip64_extra(extra: bytes) -> None:
    cursor = 0
    while cursor < len(extra):
        if cursor + 4 > len(extra):
            raise UnsafeArchiveError("ZIP extra field is truncated")
        field_type, field_size = struct.unpack_from("<HH", extra, cursor)
        cursor += 4
        field_end = cursor + field_size
        if field_end > len(extra):
            raise UnsafeArchiveError("ZIP extra field is truncated")
        if field_type == _ZIP64_EXTRA_FIELD:
            raise UnsafeArchiveError("ZIP64 archives are not supported")
        cursor = field_end


def _validate_zip_entry(entry: zipfile.ZipInfo, limits: CompressionLimits) -> None:
    normalized_name = entry.filename.replace("\\", "/")
    path = PurePosixPath(normalized_name)
    if path.is_absolute() or ".." in path.parts or "\x00" in normalized_name:
        raise UnsafeArchiveError("ZIP member path is unsafe")
    unix_mode = entry.external_attr >> 16
    if unix_mode and stat.S_ISLNK(unix_mode):
        raise UnsafeArchiveError("ZIP symbolic links are not allowed")
    if entry.flag_bits & 0x1:
        raise UnsafeArchiveError("Encrypted ZIP members are not supported")
    if entry.compress_type not in _SUPPORTED_ZIP_COMPRESSION_METHODS:
        raise UnsafeArchiveError("ZIP member compression method is not supported")
    if entry.file_size > limits.maximum_decompressed_bytes:
        raise UnsafeArchiveError("ZIP member exceeds the decompressed-byte limit")
    if entry.compress_size == 0 and entry.file_size:
        raise UnsafeArchiveError("ZIP member has an invalid compressed length")
    if (
        entry.compress_size
        and entry.file_size > entry.compress_size * limits.maximum_expansion_ratio
    ):
        raise UnsafeArchiveError("ZIP member exceeds the expansion-ratio limit")


def _check_compressed(total: int, limits: CompressionLimits) -> None:
    if total > limits.maximum_compressed_bytes:
        raise UnsafeArchiveError("Archive exceeds the compressed-byte limit")


def _check_decompressed(total: int, limits: CompressionLimits) -> None:
    if total > limits.maximum_decompressed_bytes:
        raise UnsafeArchiveError("Document exceeds the decompressed-byte limit")


def _check_ratio(compressed: int, decompressed: int, limits: CompressionLimits) -> None:
    if compressed == 0 and decompressed:
        raise UnsafeArchiveError("Compressed document has no input bytes")
    if compressed and decompressed > compressed * limits.maximum_expansion_ratio:
        raise UnsafeArchiveError("Document exceeds the expansion-ratio limit")
