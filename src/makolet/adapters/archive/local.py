"""Local immutable content-addressed archive."""

from __future__ import annotations

import hashlib
import math
import os
import stat
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from uuid import uuid4

import anyio

from makolet.adapters.filesystem_capacity import (
    FileSystemCapacityGuard,
    FileSystemCapacityUnavailableError,
)
from makolet.domain.errors import ArchiveCapacityError, ArchiveIntegrityError, DownloadLimitError

from ._process import (
    DuplicatedFileDescriptor,
    ProcessDeadlineError,
    ProcessWorkerError,
    run_in_spawn_process,
)
from .keys import digest_for_key, key_for_digest

DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_VERIFY_TIMEOUT_SECONDS = 3600.0

_SAFE_LOCAL_INTEGRITY_MESSAGES = frozenset(
    {
        "Archived local object exceeds configured bounds",
        "Archived local object changed during verification",
        "Archived object failed SHA-256 verification",
        "Archived object must be one regular non-linked file",
        "Archive object path contains an unsafe directory",
        "Archive object key escaped its root",
        "Archived object identity changed while opening",
        "Archived object identity changed during verification",
    }
)


class LocalContentAddressedArchive:
    """Store exact source bytes once under their SHA-256 digest.

    The temporary file and final object live on the same filesystem. Committing with
    a hard link gives create-if-absent semantics even when concurrent workers receive
    identical content; no existing object is ever replaced.
    """

    def __init__(
        self,
        root: Path,
        *,
        maximum_object_bytes: int = 2 * 1024 * 1024 * 1024,
        minimum_free_bytes: int = 0,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        verify_timeout_seconds: float = DEFAULT_VERIFY_TIMEOUT_SECONDS,
    ) -> None:
        if (
            maximum_object_bytes <= 0
            or minimum_free_bytes < 0
            or chunk_size <= 0
            or verify_timeout_seconds <= 0
            or not math.isfinite(verify_timeout_seconds)
        ):
            raise ValueError("Archive byte limits and verification timeout must be positive")
        self._root = root.resolve()
        self._temporary_root = self._root / ".incoming"
        self._maximum_object_bytes = maximum_object_bytes
        self._minimum_free_bytes = minimum_free_bytes
        self._chunk_size = chunk_size
        self._verify_timeout_seconds = verify_timeout_seconds
        self._capacity = FileSystemCapacityGuard(
            self._temporary_root,
            minimum_free_bytes=minimum_free_bytes,
            coordination_directory=self._root,
        )

    async def initialize(self) -> None:
        await anyio.Path(self._temporary_root).mkdir(parents=True, exist_ok=True)
        try:
            async with self._capacity.reserve_async(0):
                pass
        except FileSystemCapacityUnavailableError as error:
            raise ArchiveCapacityError(
                "Local archive reached its configured free-space reserve"
            ) from error

    async def put(
        self,
        chunks: AsyncIterator[bytes],
        *,
        original_filename: str,
    ) -> tuple[str, int, bool]:
        if not original_filename or "\x00" in original_filename:
            raise ValueError("original_filename is empty or unsafe")
        await self.initialize()
        temporary_path = self._temporary_root / f"{uuid4().hex}.part"
        digest = hashlib.sha256()
        content_length = 0
        try:
            handle = await anyio.open_file(temporary_path, "xb")
            async with handle:
                async for chunk in chunks:
                    if not isinstance(chunk, bytes):
                        raise TypeError("Archive chunks must be bytes")
                    if not chunk:
                        continue
                    content_length += len(chunk)
                    if content_length > self._maximum_object_bytes:
                        raise DownloadLimitError(
                            f"Object exceeds {self._maximum_object_bytes} archived bytes",
                            transferred_bytes=content_length,
                        )
                    try:
                        async with self._capacity.reserve_async(len(chunk)):
                            await handle.write(chunk)
                            await handle.flush()
                    except FileSystemCapacityUnavailableError as error:
                        raise ArchiveCapacityError(
                            "Local archive reached its configured free-space reserve",
                            transferred_bytes=content_length,
                        ) from error
                    digest.update(chunk)
                try:
                    async with self._capacity.reserve_async(0):
                        await handle.flush()
                        await anyio.to_thread.run_sync(os.fsync, handle.wrapped.fileno())
                except FileSystemCapacityUnavailableError as error:
                    raise ArchiveCapacityError(
                        "Local archive reached its configured free-space reserve",
                        transferred_bytes=content_length,
                    ) from error

            digest_hex = digest.hexdigest()
            object_key = self.key_for_digest(digest_hex)
            object_path = self._path_for_key(object_key)
            try:
                await anyio.Path(object_path.parent).mkdir(parents=True, exist_ok=True)
                await anyio.to_thread.run_sync(
                    _assert_safe_parent_chain,
                    self._root,
                    object_path,
                )
                created = await self._commit_once(temporary_path, object_path)
                if created:
                    # A Windows read-only attribute applies to every hard link for the
                    # inode, so remove the temporary name before protecting the object.
                    await anyio.Path(temporary_path).unlink()
                    await self._protect_read_only(object_path)
                else:
                    await self.verify(object_key, digest_hex)
            except Exception as error:
                if isinstance(error, ArchiveIntegrityError):
                    error.transferred_bytes = content_length
                    raise
                raise ArchiveIntegrityError(
                    "Local archive commit could not be confirmed",
                    transferred_bytes=content_length,
                ) from error
            return object_key, content_length, created
        finally:
            with anyio.CancelScope(shield=True):
                with suppress(FileNotFoundError):
                    await anyio.Path(temporary_path).unlink()

    @asynccontextmanager
    async def open(self, object_key: str) -> AsyncIterator[AsyncIterator[bytes]]:
        expected_digest = digest_for_key(object_key)
        path = self._path_for_key(object_key)
        await self.initialize()
        with tempfile.TemporaryFile(mode="w+b", dir=self._temporary_root) as spool:
            try:
                result = await run_in_spawn_process(
                    _verified_spool_in_child,
                    DuplicatedFileDescriptor(spool.fileno()),
                    self._root,
                    path,
                    expected_digest,
                    self._maximum_object_bytes,
                    self._minimum_free_bytes,
                    self._chunk_size,
                    self._temporary_root,
                    timeout_seconds=self._verify_timeout_seconds,
                )
            except ProcessDeadlineError as error:
                raise ArchiveIntegrityError(
                    "Local archive verification exceeded its total operation deadline"
                ) from error
            except ProcessWorkerError as error:
                raise ArchiveIntegrityError("Archived object could not be read safely") from error
            status, detail = result
            if status == "missing":
                raise ArchiveIntegrityError("Archived object is missing")
            if status == "capacity":
                raise ArchiveCapacityError(
                    "Local archive read spool reached its configured free-space reserve"
                )
            if status == "integrity":
                raise ArchiveIntegrityError(str(detail))
            if status == "oserror":
                raise ArchiveIntegrityError("Archived object could not be read safely")
            if status != "ok" or not isinstance(detail, int):
                raise ArchiveIntegrityError("Local archive verifier returned an invalid result")
            expected_length = detail
            if os.fstat(spool.fileno()).st_size != expected_length:
                raise ArchiveIntegrityError("Archived local object changed during verification")
            spool.seek(0)
            handle = anyio.wrap_file(spool)
            parser_digest = hashlib.sha256()
            parser_length = 0
            parser_reached_eof = False

            def ensure_parser_stream_verified() -> None:
                if parser_length != expected_length:
                    raise ArchiveIntegrityError("Parser-visible local object length changed")
                if parser_digest.hexdigest() != expected_digest:
                    raise ArchiveIntegrityError("Parser-visible object failed SHA-256 verification")

            async def chunks() -> AsyncIterator[bytes]:
                nonlocal parser_length, parser_reached_eof
                while chunk := await handle.read(self._chunk_size):
                    parser_length += len(chunk)
                    if parser_length > expected_length:
                        raise ArchiveIntegrityError(
                            "Parser-visible local object exceeds expected bounds"
                        )
                    parser_digest.update(chunk)
                    yield chunk
                parser_reached_eof = True
                ensure_parser_stream_verified()

            parser_chunks = chunks()
            parser_completed_normally = False
            try:
                yield parser_chunks
                parser_completed_normally = True
            finally:
                try:
                    if parser_completed_normally:
                        if not parser_reached_eof:
                            async for _remaining_chunk in parser_chunks:
                                pass
                        ensure_parser_stream_verified()
                finally:
                    with anyio.CancelScope(shield=True):
                        await handle.aclose()

    async def exists(self, object_key: str) -> bool:
        path = self._path_for_key(object_key)
        try:
            descriptor = await anyio.to_thread.run_sync(
                _open_regular_file_no_follow,
                self._root,
                path,
            )
        except FileNotFoundError:
            return False
        await anyio.to_thread.run_sync(os.close, descriptor)
        return True

    async def verify(self, object_key: str, expected_sha256: str) -> int:
        digest_for_key(object_key, expected_sha256=expected_sha256)
        content_length = 0
        async with self.open(object_key) as chunks:
            async for chunk in chunks:
                content_length += len(chunk)
        return content_length

    @staticmethod
    def key_for_digest(digest: str) -> str:
        return key_for_digest(digest)

    def _path_for_key(self, object_key: str) -> Path:
        digest_for_key(object_key)
        candidate = self._root.joinpath(*object_key.split("/"))
        if not candidate.is_relative_to(self._root):
            raise ArchiveIntegrityError("Archive object key escaped its root")
        return candidate

    @staticmethod
    async def _commit_once(temporary_path: Path, object_path: Path) -> bool:
        try:
            await anyio.to_thread.run_sync(os.link, temporary_path, object_path)
        except FileExistsError:
            return False
        return True

    @staticmethod
    async def _protect_read_only(object_path: Path) -> None:
        read_only = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
        await anyio.to_thread.run_sync(os.chmod, object_path, read_only)


def _verified_spool_in_child(
    duplicated_spool: DuplicatedFileDescriptor,
    root: Path,
    path: Path,
    expected_digest: str,
    maximum_object_bytes: int,
    minimum_free_bytes: int,
    chunk_size: int,
    temporary_root: Path,
) -> tuple[str, int | str]:
    try:
        content_length = _copy_verified_local_object(
            duplicated_spool,
            root,
            path,
            expected_digest,
            maximum_object_bytes,
            minimum_free_bytes,
            chunk_size,
            temporary_root,
        )
    except FileNotFoundError:
        return "missing", 0
    except ArchiveCapacityError:
        return "capacity", 0
    except ArchiveIntegrityError as error:
        message = str(error)
        safe_message = (
            message
            if message in _SAFE_LOCAL_INTEGRITY_MESSAGES
            else "Archived object could not be read safely"
        )
        return "integrity", safe_message
    except OSError:
        return "oserror", 0
    else:
        return "ok", content_length


def _copy_verified_local_object(
    duplicated_spool: DuplicatedFileDescriptor,
    root: Path,
    path: Path,
    expected_digest: str,
    maximum_object_bytes: int,
    minimum_free_bytes: int,
    chunk_size: int,
    temporary_root: Path,
) -> int:
    descriptor = _open_regular_file_no_follow(root, path)
    spool_descriptor = duplicated_spool.detach()
    capacity = FileSystemCapacityGuard(
        temporary_root,
        minimum_free_bytes=minimum_free_bytes,
        coordination_directory=root,
    )
    with (
        os.fdopen(descriptor, "rb", closefd=True) as source,
        os.fdopen(spool_descriptor, "w+b", closefd=True) as spool,
    ):
        initial_status = os.fstat(source.fileno())
        if initial_status.st_size > maximum_object_bytes:
            raise ArchiveIntegrityError("Archived local object exceeds configured bounds")
        _assert_directory_non_reparse(os.lstat(temporary_root))
        digest = hashlib.sha256()
        content_length = 0
        while chunk := source.read(chunk_size):
            content_length += len(chunk)
            if content_length > maximum_object_bytes:
                raise ArchiveIntegrityError("Archived local object exceeds configured bounds")
            try:
                with capacity.reserve(len(chunk)):
                    if spool.write(chunk) != len(chunk):
                        raise OSError("Short write to local archive verification spool")
                    spool.flush()
            except FileSystemCapacityUnavailableError as error:
                raise ArchiveCapacityError(
                    "Local archive read spool reached its configured free-space reserve"
                ) from error
            digest.update(chunk)
        final_status = os.fstat(source.fileno())
        if (
            not _same_file_identity(initial_status, final_status)
            or final_status.st_size != initial_status.st_size
            or content_length != initial_status.st_size
        ):
            raise ArchiveIntegrityError("Archived local object changed during verification")
        _assert_path_still_identifies_file(root, path, final_status)
        if digest.hexdigest() != expected_digest:
            raise ArchiveIntegrityError("Archived object failed SHA-256 verification")
        try:
            with capacity.reserve(0):
                spool.flush()
                os.fsync(spool.fileno())
        except FileSystemCapacityUnavailableError as error:
            raise ArchiveCapacityError(
                "Local archive read spool reached its configured free-space reserve"
            ) from error
        return content_length


def _open_regular_file_no_follow(root: Path, path: Path) -> int:
    _assert_safe_parent_chain(root, path)
    path_status = os.lstat(path)
    _assert_regular_non_reparse(path_status)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags)
    try:
        descriptor_status = os.fstat(descriptor)
        _assert_regular_non_reparse(descriptor_status)
        _assert_same_file_identity(
            path_status,
            descriptor_status,
            message="Archived object identity changed while opening",
        )
        _assert_path_still_identifies_file(root, path, descriptor_status)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _assert_safe_parent_chain(root: Path, path: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise ArchiveIntegrityError("Archive object key escaped its root") from error
    current = root
    _assert_directory_non_reparse(os.lstat(current))
    for part in relative.parts[:-1]:
        current /= part
        _assert_directory_non_reparse(os.lstat(current))


def _assert_path_still_identifies_file(
    root: Path,
    path: Path,
    expected_status: os.stat_result,
) -> None:
    _assert_safe_parent_chain(root, path)
    path_status = os.lstat(path)
    _assert_regular_non_reparse(path_status)
    if not _same_file_identity(path_status, expected_status):
        raise ArchiveIntegrityError("Archived object identity changed during verification")


def _assert_regular_non_reparse(status: os.stat_result) -> None:
    if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1 or _is_reparse_point(status):
        raise ArchiveIntegrityError("Archived object must be one regular non-linked file")


def _assert_directory_non_reparse(status: os.stat_result) -> None:
    if not stat.S_ISDIR(status.st_mode) or _is_reparse_point(status):
        raise ArchiveIntegrityError("Archive object path contains an unsafe directory")


def _is_reparse_point(status: os.stat_result) -> bool:
    attributes = getattr(status, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return isinstance(attributes, int) and bool(attributes & reparse_flag)


def _same_file_identity(first: os.stat_result, second: os.stat_result) -> bool:
    return first.st_dev == second.st_dev and first.st_ino == second.st_ino


def _assert_same_file_identity(
    first: os.stat_result,
    second: os.stat_result,
    *,
    message: str,
) -> None:
    if not _same_file_identity(first, second):
        raise ArchiveIntegrityError(message)
