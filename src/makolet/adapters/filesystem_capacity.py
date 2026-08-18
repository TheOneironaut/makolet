"""Cross-process coordination for filesystem free-space reserve checks."""

from __future__ import annotations

import errno
import os
import shutil
import stat
import threading
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from pathlib import Path

import anyio

CAPACITY_LOCK_FILENAME = ".makolet-capacity.lock"

_DEFAULT_ACQUISITION_TIMEOUT_SECONDS = 30.0
_LOCK_POLL_SECONDS = 0.05
_PROCESS_LOCKS_GUARD = threading.Lock()
_PROCESS_LOCKS: dict[str, threading.Lock] = {}


class FileSystemCapacityUnavailableError(RuntimeError):
    """A coordinated filesystem write cannot preserve its configured floor."""


@dataclass(slots=True)
class _CapacityLease:
    descriptor: int
    process_lock: threading.Lock
    released: bool = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        try:
            os.close(self.descriptor)
        finally:
            self.process_lock.release()


class FileSystemCapacityGuard:
    """Serialize one capacity check and bounded write for a shared storage root.

    Every cooperating writer must use the same ``coordination_directory``. The
    lock is advisory, so unrelated host processes remain outside this contract.
    """

    def __init__(
        self,
        storage_directory: Path,
        *,
        minimum_free_bytes: int,
        coordination_directory: Path | None = None,
        acquisition_timeout_seconds: float = _DEFAULT_ACQUISITION_TIMEOUT_SECONDS,
    ) -> None:
        if minimum_free_bytes < 0 or acquisition_timeout_seconds <= 0:
            raise ValueError("Filesystem capacity limits are invalid")
        self._storage_directory = storage_directory.resolve(strict=False)
        self._coordination_directory = (
            coordination_directory.resolve(strict=False)
            if coordination_directory is not None
            else self._storage_directory
        )
        self._minimum_free_bytes = minimum_free_bytes
        self._acquisition_timeout_seconds = acquisition_timeout_seconds

    @contextmanager
    def reserve(self, additional_bytes: int) -> Iterator[None]:
        """Hold the shared lock while the caller writes ``additional_bytes``."""

        if additional_bytes < 0:
            raise ValueError("Reserved filesystem bytes cannot be negative")
        if self._minimum_free_bytes == 0:
            yield
            return
        lease = self._acquire(additional_bytes)
        try:
            yield
            self._ensure_capacity(0)
        finally:
            lease.release()

    @asynccontextmanager
    async def reserve_async(self, additional_bytes: int) -> AsyncIterator[None]:
        """Async adapter for :meth:`reserve` without blocking the event loop."""

        if additional_bytes < 0:
            raise ValueError("Reserved filesystem bytes cannot be negative")
        if self._minimum_free_bytes == 0:
            yield
            return
        lease = await anyio.to_thread.run_sync(lambda: self._acquire(additional_bytes))
        try:
            # A cancellation between buffered write and flush would move allocation
            # outside the lock. Callers keep this section to one bounded local chunk.
            with anyio.CancelScope(shield=True):
                yield
                await anyio.to_thread.run_sync(lambda: self._ensure_capacity(0))
        finally:
            with anyio.CancelScope(shield=True):
                await anyio.to_thread.run_sync(lease.release)

    def _acquire(self, additional_bytes: int) -> _CapacityLease:
        deadline = time.monotonic() + self._acquisition_timeout_seconds
        lock_path = self._coordination_directory / CAPACITY_LOCK_FILENAME
        process_lock = _process_lock(lock_path)
        remaining = max(0.0, deadline - time.monotonic())
        if not process_lock.acquire(timeout=remaining):
            raise FileSystemCapacityUnavailableError("Filesystem capacity coordination timed out")
        descriptor: int | None = None
        try:
            descriptor = _open_lock_descriptor(lock_path)
            while not _try_lock_file(descriptor):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise _capacity_unavailable("Filesystem capacity coordination timed out")
                time.sleep(min(_LOCK_POLL_SECONDS, remaining))
            self._validate_directories_share_filesystem()
            self._ensure_capacity(additional_bytes)
            return _CapacityLease(descriptor, process_lock)
        except FileSystemCapacityUnavailableError:
            if descriptor is not None:
                os.close(descriptor)
            process_lock.release()
            raise
        except (OSError, ValueError) as error:
            if descriptor is not None:
                os.close(descriptor)
            process_lock.release()
            raise FileSystemCapacityUnavailableError(
                "Filesystem capacity coordination failed"
            ) from error

    def _ensure_capacity(self, additional_bytes: int) -> None:
        usage = shutil.disk_usage(self._storage_directory)
        if usage.free - additional_bytes < self._minimum_free_bytes:
            raise _capacity_unavailable("Filesystem free-space reserve would be crossed")

    def _validate_directories_share_filesystem(self) -> None:
        storage = self._storage_directory.stat()
        coordination = self._coordination_directory.stat()
        if not stat.S_ISDIR(storage.st_mode) or not stat.S_ISDIR(coordination.st_mode):
            raise FileSystemCapacityUnavailableError("Filesystem capacity directory is unavailable")
        if storage.st_dev != coordination.st_dev:
            raise FileSystemCapacityUnavailableError(
                "Filesystem capacity directories are on different filesystems"
            )


def _process_lock(path: Path) -> threading.Lock:
    identity = str(path).casefold() if os.name == "nt" else str(path)
    with _PROCESS_LOCKS_GUARD:
        return _PROCESS_LOCKS.setdefault(identity, threading.Lock())


def _capacity_unavailable(message: str) -> FileSystemCapacityUnavailableError:
    return FileSystemCapacityUnavailableError(message)


def _open_lock_descriptor(path: Path) -> int:
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags, 0o600)
    status = os.fstat(descriptor)
    attributes = getattr(status, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    if (
        not stat.S_ISREG(status.st_mode)
        or status.st_nlink != 1
        or (isinstance(attributes, int) and bool(attributes & reparse_flag))
    ):
        os.close(descriptor)
        raise FileSystemCapacityUnavailableError("Filesystem capacity lock path is unsafe")
    return descriptor


def _try_lock_file(descriptor: int) -> bool:
    try:
        if os.name == "nt":
            import msvcrt

            os.lseek(descriptor, 0, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            # POSIX-only members are absent from the Windows typeshed module.
            fcntl.flock(  # type: ignore[attr-defined]
                descriptor,
                fcntl.LOCK_EX  # type: ignore[attr-defined]
                | fcntl.LOCK_NB,  # type: ignore[attr-defined]
            )
    except OSError as error:
        if error.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
            return False
        raise
    return True


__all__ = [
    "CAPACITY_LOCK_FILENAME",
    "FileSystemCapacityGuard",
    "FileSystemCapacityUnavailableError",
]
