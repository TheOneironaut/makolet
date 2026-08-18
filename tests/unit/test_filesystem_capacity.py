from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import anyio
import pytest

from makolet.adapters.filesystem_capacity import (
    CAPACITY_LOCK_FILENAME,
    FileSystemCapacityGuard,
    FileSystemCapacityUnavailableError,
)

_COMPETING_WRITER = r"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from types import SimpleNamespace

from makolet.adapters import filesystem_capacity
from makolet.adapters.filesystem_capacity import (
    FileSystemCapacityGuard,
    FileSystemCapacityUnavailableError,
)

root, state, started, entered, release, result, hold = map(Path, sys.argv[1:])


def fake_disk_usage(_directory: Path) -> SimpleNamespace:
    free = int(state.read_text(encoding="ascii"))
    return SimpleNamespace(total=100, used=100 - free, free=free)


filesystem_capacity.shutil.disk_usage = fake_disk_usage
guard = FileSystemCapacityGuard(root, minimum_free_bytes=40)
started.write_text("started", encoding="ascii")
try:
    with guard.reserve(60):
        entered.write_text("entered", encoding="ascii")
        if hold.read_text(encoding="ascii") == "yes":
            deadline = time.monotonic() + 10
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise RuntimeError("test release signal timed out")
                time.sleep(0.01)
        remaining = int(state.read_text(encoding="ascii")) - 60
        state.write_text(str(remaining), encoding="ascii")
    result.write_text("written", encoding="ascii")
except FileSystemCapacityUnavailableError:
    result.write_text("rejected", encoding="ascii")
"""


def _wait_for(path: Path) -> None:
    deadline = time.monotonic() + 10
    while not path.exists():
        if time.monotonic() >= deadline:
            raise AssertionError(f"subprocess did not create {path.name}")
        time.sleep(0.01)


def _start_writer(
    root: Path,
    state: Path,
    started: Path,
    entered: Path,
    release: Path,
    result: Path,
    hold: Path,
) -> subprocess.Popen[bytes]:
    return subprocess.Popen(  # noqa: S603 - fixed interpreter and test-only script
        [
            sys.executable,
            "-c",
            _COMPETING_WRITER,
            str(root),
            str(state),
            str(started),
            str(entered),
            str(release),
            str(result),
            str(hold),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_two_processes_cannot_jointly_cross_the_free_space_floor(tmp_path: Path) -> None:
    root = tmp_path / "shared-spool"
    root.mkdir()
    state = tmp_path / "free-bytes"
    state.write_text("100", encoding="ascii")
    first_started = tmp_path / "first-started"
    first_entered = tmp_path / "first-entered"
    first_release = tmp_path / "first-release"
    first_result = tmp_path / "first-result"
    first_hold = tmp_path / "first-hold"
    first_hold.write_text("yes", encoding="ascii")
    second_started = tmp_path / "second-started"
    second_entered = tmp_path / "second-entered"
    second_release = tmp_path / "second-release"
    second_result = tmp_path / "second-result"
    second_hold = tmp_path / "second-hold"
    second_hold.write_text("no", encoding="ascii")

    first = _start_writer(
        root,
        state,
        first_started,
        first_entered,
        first_release,
        first_result,
        first_hold,
    )
    _wait_for(first_entered)
    second = _start_writer(
        root,
        state,
        second_started,
        second_entered,
        second_release,
        second_result,
        second_hold,
    )
    _wait_for(second_started)
    time.sleep(0.2)
    first_release.touch()

    first_output, first_errors = first.communicate(timeout=10)
    second_output, second_errors = second.communicate(timeout=10)

    assert first.returncode == 0, (first_output, first_errors)
    assert second.returncode == 0, (second_output, second_errors)
    assert first_result.read_text(encoding="ascii") == "written"
    assert second_result.read_text(encoding="ascii") == "rejected"
    assert not second_entered.exists()
    assert state.read_text(encoding="ascii") == "40"
    assert (root / CAPACITY_LOCK_FILENAME).is_file()


def test_capacity_guard_rechecks_the_actual_floor_after_the_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remaining = 100

    def fake_disk_usage(_directory: Path) -> SimpleNamespace:
        return SimpleNamespace(total=100, used=100 - remaining, free=remaining)

    monkeypatch.setattr(
        "makolet.adapters.filesystem_capacity.shutil.disk_usage",
        fake_disk_usage,
    )
    guard = FileSystemCapacityGuard(tmp_path, minimum_free_bytes=40)

    with (
        pytest.raises(FileSystemCapacityUnavailableError, match="would be crossed"),
        guard.reserve(60),
    ):
        remaining = 39


def test_zero_floor_remains_an_explicit_lock_free_opt_out(tmp_path: Path) -> None:
    guard = FileSystemCapacityGuard(tmp_path, minimum_free_bytes=0)

    with guard.reserve(2**63):
        pass

    assert not (tmp_path / CAPACITY_LOCK_FILENAME).exists()


def test_capacity_lock_wait_is_bounded(tmp_path: Path) -> None:
    holder = FileSystemCapacityGuard(tmp_path, minimum_free_bytes=1)
    waiter = FileSystemCapacityGuard(
        tmp_path,
        minimum_free_bytes=1,
        acquisition_timeout_seconds=0.05,
    )

    started = time.monotonic()
    with (
        holder.reserve(0),
        pytest.raises(FileSystemCapacityUnavailableError, match="timed out"),
        waiter.reserve(0),
    ):
        pass

    assert time.monotonic() - started < 1


@pytest.mark.asyncio
async def test_async_cancellation_finishes_one_bounded_write_and_releases_lock(
    tmp_path: Path,
) -> None:
    guard = FileSystemCapacityGuard(tmp_path, minimum_free_bytes=1)
    started = anyio.Event()
    release = anyio.Event()
    completed = False

    async def writer() -> None:
        nonlocal completed
        async with guard.reserve_async(1):
            started.set()
            await release.wait()
            completed = True

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(writer)
        await started.wait()
        tasks.cancel_scope.cancel()
        release.set()

    assert completed
    with guard.reserve(1):
        pass
