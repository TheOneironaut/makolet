from __future__ import annotations

import gc
import os
import socket
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import anyio
import psutil  # type: ignore[import-untyped]
import pytest

from makolet.adapters.archive import _process as archive_process
from makolet.adapters.archive._process import (
    DuplicatedFileDescriptor,
    ProcessDeadlineError,
    ProcessWorkerError,
    _SocketChannel,
    run_in_spawn_process,
    run_in_spawn_process_with_input,
)
from makolet.adapters.archive.local import LocalContentAddressedArchive
from makolet.domain.errors import ArchiveIntegrityError

_REPETITIONS = 8
_CURRENT_PROCESS = psutil.Process()
_PROCESS_BOUNDARY_WARMED = False
_HEALTHY_PROCESS_TIMEOUT_SECONDS = 30.0


def _raise_before_detach(_descriptor: DuplicatedFileDescriptor) -> None:
    raise RuntimeError("controlled child startup failure")


def _stall_before_detach(_descriptor: DuplicatedFileDescriptor) -> None:
    threading.Event().wait()


def _collect_streamed_chunks(chunks: Iterator[bytes], prefix: bytes) -> bytes:
    return prefix + b"".join(chunks)


@pytest.mark.asyncio
async def test_stream_process_receives_multiple_bounded_chunks() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"one"
        await anyio.lowlevel.checkpoint()
        yield b"two"

    assert (
        await run_in_spawn_process_with_input(
            _collect_streamed_chunks,
            chunks(),
            b"prefix-",
            timeout_seconds=_HEALTHY_PROCESS_TIMEOUT_SECONDS,
        )
        == b"prefix-onetwo"
    )


def _stall_during_child_unpickle(pid_path: str) -> object:
    Path(pid_path).write_text(str(os.getpid()), encoding="ascii")
    threading.Event().wait()
    raise AssertionError("unpickle stall unexpectedly returned")


class _StallOnChildUnpickle:
    def __init__(self, pid_path: Path) -> None:
        self._pid_path = pid_path

    def __reduce__(self) -> tuple[object, tuple[str]]:
        return _stall_during_child_unpickle, (str(self._pid_path),)


class _StallOnDescriptorReceive(DuplicatedFileDescriptor):
    def __init__(self, descriptor: int, pid_path: Path) -> None:
        super().__init__(descriptor)
        self._pid_path = pid_path

    def __getstate__(self) -> dict[str, object]:
        return {"pid_path": str(self._pid_path)}

    def __setstate__(self, state: dict[str, object]) -> None:
        super().__setstate__({})
        self._pid_path = Path(str(state["pid_path"]))

    def _receive(self, connection: object) -> None:
        del connection
        self._pid_path.write_text(str(os.getpid()), encoding="ascii")
        threading.Event().wait()


def _ignore_argument(_argument: object) -> None:
    return None


def _resource_count() -> int:
    count = _CURRENT_PROCESS.num_handles() if os.name == "nt" else _CURRENT_PROCESS.num_fds()
    return int(count)


async def _settle_resource_cleanup() -> None:
    gc.collect()
    await anyio.sleep(0.05)


async def _warm_process_boundary() -> None:
    global _PROCESS_BOUNDARY_WARMED
    if _PROCESS_BOUNDARY_WARMED:
        return
    assert (
        await run_in_spawn_process(
            str,
            "warmup",
            timeout_seconds=_HEALTHY_PROCESS_TIMEOUT_SECONDS,
        )
        == "warmup"
    )
    await _settle_resource_cleanup()
    _PROCESS_BOUNDARY_WARMED = True


async def _wait_for_path(path: Path) -> None:
    with anyio.fail_after(3):
        while True:
            try:
                value = path.read_text(encoding="ascii")  # noqa: ASYNC240 - process signal
            except FileNotFoundError:
                value = ""
            if value.isascii() and value.isdecimal() and int(value) > 0:
                return
            await anyio.sleep(0.01)


def _assert_process_disappears(pid_path: Path) -> None:
    child_pid = int(pid_path.read_text(encoding="ascii"))
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if not psutil.pid_exists(child_pid):
            break
        time.sleep(0.02)
    assert not psutil.pid_exists(child_pid)


def _assert_no_bootstrap_thread() -> None:
    assert not any(
        thread.is_alive() and thread.name == "makolet-archive-bootstrap"
        for thread in threading.enumerate()
    )


@pytest.mark.asyncio
async def test_missing_local_reads_do_not_leak_parent_handles_or_descriptors(
    tmp_path: Path,
) -> None:
    archive = LocalContentAddressedArchive(tmp_path / "archive")
    missing_digest = "0" * 64
    missing_key = LocalContentAddressedArchive.key_for_digest(missing_digest)

    with pytest.raises(ArchiveIntegrityError, match="missing"):
        async with archive.open(missing_key):
            pass
    await _settle_resource_cleanup()
    baseline = _resource_count()

    for _ in range(_REPETITIONS):
        with pytest.raises(ArchiveIntegrityError, match="missing"):
            async with archive.open(missing_key):
                pass

    await _settle_resource_cleanup()
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_child_startup_failure_before_detach_does_not_leak_parent_resources() -> None:
    with tempfile.TemporaryFile(mode="w+b") as spool, pytest.raises(ProcessWorkerError):
        await run_in_spawn_process(
            _raise_before_detach,
            DuplicatedFileDescriptor(spool.fileno()),
            timeout_seconds=_HEALTHY_PROCESS_TIMEOUT_SECONDS,
        )
    await _settle_resource_cleanup()
    baseline = _resource_count()

    for _ in range(_REPETITIONS):
        with tempfile.TemporaryFile(mode="w+b") as spool, pytest.raises(ProcessWorkerError):
            await run_in_spawn_process(
                _raise_before_detach,
                DuplicatedFileDescriptor(spool.fileno()),
                timeout_seconds=_HEALTHY_PROCESS_TIMEOUT_SECONDS,
            )

    await _settle_resource_cleanup()
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_payload_startup_failure_does_not_leak_parent_resources() -> None:
    def local_unpicklable_target(_descriptor: DuplicatedFileDescriptor) -> None:
        return None

    async def fail_once() -> None:
        with (
            tempfile.TemporaryFile(mode="w+b") as spool,
            pytest.raises(ProcessWorkerError),
        ):
            await run_in_spawn_process(
                local_unpicklable_target,
                DuplicatedFileDescriptor(spool.fileno()),
                timeout_seconds=_HEALTHY_PROCESS_TIMEOUT_SECONDS,
            )

    await fail_once()
    await _settle_resource_cleanup()
    baseline = _resource_count()

    for _ in range(_REPETITIONS):
        await fail_once()

    await _settle_resource_cleanup()
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_process_launch_failure_does_not_leak_parent_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_process_launch(*_args: object, **_kwargs: object) -> None:
        raise OSError("controlled process launch failure")

    monkeypatch.setattr(subprocess, "Popen", fail_process_launch)
    await _settle_resource_cleanup()
    baseline = _resource_count()

    for _ in range(_REPETITIONS):
        with pytest.raises(OSError, match="controlled process launch failure"):
            await run_in_spawn_process(str, "unused", timeout_seconds=1)

    await _settle_resource_cleanup()
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_timeout_before_detach_does_not_leak_parent_resources() -> None:
    with tempfile.TemporaryFile(mode="w+b") as spool, pytest.raises(ProcessDeadlineError):
        await run_in_spawn_process(
            _stall_before_detach,
            DuplicatedFileDescriptor(spool.fileno()),
            timeout_seconds=0.05,
        )
    await _settle_resource_cleanup()
    baseline = _resource_count()

    for _ in range(_REPETITIONS):
        with (
            tempfile.TemporaryFile(mode="w+b") as spool,
            pytest.raises(ProcessDeadlineError),
        ):
            await run_in_spawn_process(
                _stall_before_detach,
                DuplicatedFileDescriptor(spool.fileno()),
                timeout_seconds=0.05,
            )

    await _settle_resource_cleanup()
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_cancellation_before_detach_does_not_leak_parent_resources() -> None:
    async def cancel_one() -> None:
        with tempfile.TemporaryFile(mode="w+b") as spool:

            async def run_stalled_child() -> None:
                await run_in_spawn_process(
                    _stall_before_detach,
                    DuplicatedFileDescriptor(spool.fileno()),
                    timeout_seconds=5,
                )

            async with anyio.create_task_group() as tasks:
                tasks.start_soon(run_stalled_child)
                await anyio.sleep(0.05)
                tasks.cancel_scope.cancel()

    await cancel_one()
    await _settle_resource_cleanup()
    baseline = _resource_count()

    for _ in range(_REPETITIONS):
        await cancel_one()

    await _settle_resource_cleanup()
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_deadline_during_blocked_bootstrap_send_leaves_no_residue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    child_pids: list[int] = []
    original_socketpair = socket.socketpair
    original_launch = archive_process._launch_child
    original_send = _SocketChannel.send

    def small_buffer_socketpair() -> tuple[socket.socket, socket.socket]:
        parent_socket, child_socket = original_socketpair()
        parent_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        child_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        return parent_socket, child_socket

    def launch_suspended_child() -> object:
        channel = original_launch()
        child_pids.append(channel.process.pid)
        psutil.Process(channel.process.pid).suspend()
        return channel

    def send_until_channel_closes(channel: _SocketChannel, value: object) -> None:
        original_send(channel, value)
        while True:
            channel._socket.sendall(b"x" * (64 * 1024))

    assert (
        await run_in_spawn_process(
            str,
            "warmup",
            timeout_seconds=_HEALTHY_PROCESS_TIMEOUT_SECONDS,
        )
        == "warmup"
    )
    await _settle_resource_cleanup()
    baseline = _resource_count()
    monkeypatch.setattr(socket, "socketpair", small_buffer_socketpair)
    monkeypatch.setattr(archive_process, "_launch_child", launch_suspended_child)
    monkeypatch.setattr(_SocketChannel, "send", send_until_channel_closes)

    with pytest.raises(ProcessDeadlineError):
        await run_in_spawn_process(
            _ignore_argument,
            b"x",
            timeout_seconds=0.2,
        )

    await _settle_resource_cleanup()
    _assert_no_bootstrap_thread()
    assert child_pids
    assert not psutil.pid_exists(child_pids[0])
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_cancellation_during_blocked_bootstrap_send_leaves_no_residue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launched = anyio.Event()
    child_pids: list[int] = []
    original_socketpair = socket.socketpair
    original_launch = archive_process._launch_child
    original_send = _SocketChannel.send

    def small_buffer_socketpair() -> tuple[socket.socket, socket.socket]:
        parent_socket, child_socket = original_socketpair()
        parent_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        child_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        return parent_socket, child_socket

    def launch_suspended_child() -> object:
        channel = original_launch()
        child_pids.append(channel.process.pid)
        psutil.Process(channel.process.pid).suspend()
        launched.set()
        return channel

    def send_until_channel_closes(channel: _SocketChannel, value: object) -> None:
        original_send(channel, value)
        while True:
            channel._socket.sendall(b"x" * (64 * 1024))

    assert (
        await run_in_spawn_process(
            str,
            "warmup",
            timeout_seconds=_HEALTHY_PROCESS_TIMEOUT_SECONDS,
        )
        == "warmup"
    )
    await _settle_resource_cleanup()
    baseline = _resource_count()
    monkeypatch.setattr(socket, "socketpair", small_buffer_socketpair)
    monkeypatch.setattr(archive_process, "_launch_child", launch_suspended_child)
    monkeypatch.setattr(_SocketChannel, "send", send_until_channel_closes)

    async def run_blocked_send() -> None:
        await run_in_spawn_process(
            _ignore_argument,
            b"x",
            timeout_seconds=5,
        )

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(run_blocked_send)
        with anyio.fail_after(3):
            await launched.wait()
        await anyio.sleep(0.05)
        assert any(
            thread.is_alive() and thread.name == "makolet-archive-bootstrap"
            for thread in threading.enumerate()
        )
        tasks.cancel_scope.cancel()

    await _settle_resource_cleanup()
    _assert_no_bootstrap_thread()
    assert child_pids
    assert not psutil.pid_exists(child_pids[0])
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_deadline_during_child_unpickle_leaves_no_residue(tmp_path: Path) -> None:
    pid_path = tmp_path / "deadline-unpickle.pid"
    await _warm_process_boundary()
    await _settle_resource_cleanup()
    baseline = _resource_count()

    with pytest.raises(ProcessDeadlineError):
        await run_in_spawn_process(
            _ignore_argument,
            _StallOnChildUnpickle(pid_path),
            timeout_seconds=1.5,
        )

    assert pid_path.exists()
    await _settle_resource_cleanup()
    _assert_no_bootstrap_thread()
    _assert_process_disappears(pid_path)
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_cancellation_during_child_unpickle_leaves_no_residue(tmp_path: Path) -> None:
    pid_path = tmp_path / "cancel-unpickle.pid"
    await _warm_process_boundary()
    await _settle_resource_cleanup()
    baseline = _resource_count()

    async def run_stalled_unpickle() -> None:
        await run_in_spawn_process(
            _ignore_argument,
            _StallOnChildUnpickle(pid_path),
            timeout_seconds=5,
        )

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(run_stalled_unpickle)
        await _wait_for_path(pid_path)
        tasks.cancel_scope.cancel()

    await _settle_resource_cleanup()
    _assert_no_bootstrap_thread()
    _assert_process_disappears(pid_path)
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_deadline_during_descriptor_receive_leaves_no_residue(
    tmp_path: Path,
) -> None:
    pid_path = tmp_path / "deadline-receive.pid"
    await _warm_process_boundary()
    await _settle_resource_cleanup()
    baseline = _resource_count()

    with (
        tempfile.TemporaryFile(mode="w+b") as spool,
        pytest.raises(ProcessDeadlineError),
    ):
        await run_in_spawn_process(
            _ignore_argument,
            _StallOnDescriptorReceive(spool.fileno(), pid_path),
            timeout_seconds=1.5,
        )

    assert pid_path.exists()
    await _settle_resource_cleanup()
    _assert_no_bootstrap_thread()
    _assert_process_disappears(pid_path)
    assert _resource_count() <= baseline


@pytest.mark.asyncio
async def test_cancellation_during_descriptor_receive_leaves_no_residue(
    tmp_path: Path,
) -> None:
    pid_path = tmp_path / "cancel-receive.pid"
    await _warm_process_boundary()
    await _settle_resource_cleanup()
    baseline = _resource_count()

    with tempfile.TemporaryFile(mode="w+b") as spool:

        async def run_stalled_receive() -> None:
            await run_in_spawn_process(
                _ignore_argument,
                _StallOnDescriptorReceive(spool.fileno(), pid_path),
                timeout_seconds=5,
            )

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(run_stalled_receive)
            await _wait_for_path(pid_path)
            tasks.cancel_scope.cancel()

    await _settle_resource_cleanup()
    _assert_no_bootstrap_thread()
    _assert_process_disappears(pid_path)
    assert _resource_count() <= baseline


def test_archive_child_exits_when_parent_hard_exits(tmp_path: Path) -> None:
    support = tmp_path / "support"
    support.mkdir()
    pid_path = tmp_path / "archive-child.pid"
    (support / "orphan_target.py").write_text(
        textwrap.dedent(
            """
            import os
            import threading
            from pathlib import Path

            def record_and_stall(pid_path):
                Path(pid_path).write_text(str(os.getpid()), encoding="ascii")
                threading.Event().wait()
            """
        ),
        encoding="utf-8",
    )
    script = textwrap.dedent(
        f"""
        import asyncio
        import os

        from orphan_target import record_and_stall
        from makolet.adapters.archive._process import run_in_spawn_process

        async def main():
            asyncio.create_task(
                run_in_spawn_process(
                    record_and_stall,
                    {str(pid_path)!r},
                    timeout_seconds=30,
                )
            )
            while not os.path.exists({str(pid_path)!r}):
                await asyncio.sleep(0.01)
            os._exit(77)

        asyncio.run(main())
        """
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join((str(support), environment.get("PYTHONPATH", "")))

    parent = subprocess.run(  # noqa: S603 - fixed interpreter and authored test script
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        env=environment,
        text=True,
        timeout=5,
    )

    assert parent.returncode == 77, parent.stderr
    _assert_process_disappears(pid_path)


def test_archive_child_exits_when_parent_dies_immediately_after_launch(
    tmp_path: Path,
) -> None:
    pid_path = tmp_path / "early-orphan-child.pid"
    script = textwrap.dedent(
        f"""
        import asyncio
        import os
        from pathlib import Path

        from makolet.adapters.archive import _process as archive_process

        original_launch = archive_process._launch_child

        def launch_then_exit():
            channel = original_launch()
            Path({str(pid_path)!r}).write_text(
                str(channel.process.pid),
                encoding="ascii",
            )
            os._exit(77)

        archive_process._launch_child = launch_then_exit
        asyncio.run(
            archive_process.run_in_spawn_process(
                str,
                "unused",
                timeout_seconds=30,
            )
        )
        """
    )

    parent = subprocess.run(  # noqa: S603 - fixed interpreter and authored test script
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        env=os.environ.copy(),
        text=True,
        timeout=5,
    )

    assert parent.returncode == 77, parent.stderr
    assert pid_path.exists()
    _assert_process_disappears(pid_path)


def test_pre_ready_unpickle_stall_exits_when_parent_hard_exits(tmp_path: Path) -> None:
    pid_path = tmp_path / "unpickle-child.pid"
    script = textwrap.dedent(
        f"""
        import asyncio
        import os
        from pathlib import Path

        from tests.unit.test_archive_process import (
            _StallOnChildUnpickle,
            _ignore_argument,
        )
        from makolet.adapters.archive._process import run_in_spawn_process

        pid_path = Path({str(pid_path)!r})

        async def main():
            asyncio.create_task(
                run_in_spawn_process(
                    _ignore_argument,
                    _StallOnChildUnpickle(pid_path),
                    timeout_seconds=30,
                )
            )
            while not pid_path.exists():
                await asyncio.sleep(0.01)
            os._exit(77)

        asyncio.run(main())
        """
    )

    parent = subprocess.run(  # noqa: S603 - fixed interpreter and authored test script
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        env=os.environ.copy(),
        text=True,
        timeout=5,
    )

    assert parent.returncode == 77, parent.stderr
    _assert_process_disappears(pid_path)
