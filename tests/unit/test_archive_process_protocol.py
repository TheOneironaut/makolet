from __future__ import annotations

import ctypes
import os
import pickle
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import AbstractContextManager
from types import ModuleType, TracebackType
from typing import Any, Literal, cast

import anyio
import pytest

from makolet.adapters.archive import _process as archive_process


class _ExitCalledError(RuntimeError):
    def __init__(self, code: int) -> None:
        super().__init__(str(code))
        self.code = code


def _raise_exit(code: int) -> None:
    raise _ExitCalledError(code)


class _SendSocket:
    def __init__(self, *, always_block: bool = False) -> None:
        self.always_block = always_block
        self.calls = 0

    def send(self, value: memoryview) -> int:
        self.calls += 1
        if self.always_block or self.calls == 1:
            raise BlockingIOError
        return len(value)


class _ImmediateTimeout(AbstractContextManager[None]):
    def __enter__(self) -> None:
        raise TimeoutError

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        del exc_type, exc_value, traceback
        return False


def _immediate_timeout(_seconds: float) -> _ImmediateTimeout:
    return _ImmediateTimeout()


class _DescriptorChannel:
    def __init__(self, received_descriptor: int) -> None:
        self.received_descriptor = received_descriptor
        self.sent: list[tuple[int, int]] = []

    def send_descriptor(self, descriptor: int, child_pid: int) -> None:
        self.sent.append((descriptor, child_pid))

    def receive_descriptor(self) -> int:
        return self.received_descriptor


@pytest.mark.asyncio
async def test_socket_channel_frames_incremental_messages_and_bounds_async_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sender_socket, receiver_socket = socket.socketpair()
    sender = archive_process._SocketChannel(sender_socket)
    receiver = archive_process._SocketChannel(receiver_socket)
    try:
        sender.send({"value": 7})
        assert receiver.recv() == {"value": 7}

        frame = archive_process._SocketChannel._frame(("partial", 3))
        sender_socket.sendall(frame[:4])
        receiver.set_nonblocking()
        assert receiver.try_recv() is archive_process._NO_MESSAGE
        sender_socket.sendall(frame[4:])
        assert receiver.try_recv() == ("partial", 3)
    finally:
        sender.close()
        receiver.close()

    eof_sender, eof_receiver = socket.socketpair()
    eof_channel = archive_process._SocketChannel(eof_receiver)
    eof_channel.set_nonblocking()
    eof_sender.close()
    with pytest.raises(EOFError, match="IPC channel closed"):
        eof_channel.try_recv()
    eof_channel.close()

    with monkeypatch.context() as context:
        context.setattr(archive_process, "_MAX_FRAME_BYTES", 1)
        with pytest.raises(ValueError, match="size limit"):
            archive_process._SocketChannel._frame("too large")
        with pytest.raises(ValueError, match="size limit"):
            archive_process._SocketChannel._validate_frame_size(2)

    send_socket = _SendSocket()
    channel = archive_process._SocketChannel(cast(socket.socket, send_socket))

    async def writable(_socket: socket.socket) -> None:
        return None

    monkeypatch.setattr(anyio, "wait_socket_writable", writable)
    await channel.send_async("bounded", deadline=anyio.current_time() + 1)
    assert send_socket.calls == 2

    blocked_channel = archive_process._SocketChannel(
        cast(socket.socket, _SendSocket(always_block=True))
    )
    with pytest.raises(archive_process.ProcessDeadlineError, match="input exceeded"):
        await blocked_channel.send_async("expired", deadline=anyio.current_time())

    monkeypatch.setattr(anyio, "fail_after", _immediate_timeout)
    with pytest.raises(archive_process.ProcessDeadlineError, match="input exceeded"):
        await blocked_channel.send_async("timed-out", deadline=anyio.current_time() + 1)


def test_descriptor_transfer_is_single_use_and_closes_received_descriptors() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        archive_process.DuplicatedFileDescriptor(-1)

    with tempfile.TemporaryFile(mode="w+b") as handle:
        transferred = archive_process.DuplicatedFileDescriptor(handle.fileno())
        channel = _DescriptorChannel(os.dup(handle.fileno()))
        transferred._send(cast(Any, channel), 123)
        assert channel.sent == [(handle.fileno(), 123)]
        with pytest.raises(RuntimeError, match="already consumed"):
            transferred._send(cast(Any, channel), 123)

        child_copy = pickle.loads(pickle.dumps(transferred))  # noqa: S301 - private test IPC
        assert isinstance(child_copy, archive_process.DuplicatedFileDescriptor)
        child_copy._receive(cast(Any, channel))
        received = child_copy.detach()
        os.fstat(received)
        os.close(received)
        with pytest.raises(RuntimeError, match="not available"):
            child_copy.detach()

        close_copy = archive_process.DuplicatedFileDescriptor(handle.fileno())
        close_copy.__setstate__({})
        close_copy._receive(cast(Any, _DescriptorChannel(os.dup(handle.fileno()))))
        received_to_close = close_copy._received_descriptor
        assert received_to_close is not None
        close_copy._close_received()
        with pytest.raises(OSError, match=r"Bad file descriptor|handle is invalid"):
            os.fstat(received_to_close)


def test_socket_channel_transfers_a_real_descriptor_and_rejects_bad_frames() -> None:
    with tempfile.TemporaryFile(mode="w+b") as handle:
        sender_socket, receiver_socket = socket.socketpair()
        sender = archive_process._SocketChannel(sender_socket)
        receiver = archive_process._SocketChannel(receiver_socket)
        try:
            sender.send_descriptor(handle.fileno(), os.getpid())
            received = receiver.receive_descriptor()
            try:
                os.write(received, b"exact")
                os.lseek(handle.fileno(), 0, os.SEEK_SET)
                assert handle.read() == b"exact"
                assert not os.get_inheritable(received)
            finally:
                os.close(received)
        finally:
            sender.close()
            receiver.close()

    invalid_sender, invalid_receiver = socket.socketpair()
    invalid_channel = archive_process._SocketChannel(invalid_receiver)
    invalid_sender.sendall(archive_process._FRAME_HEADER.pack(archive_process._MAX_FRAME_BYTES + 1))
    with pytest.raises(ValueError, match="size limit"):
        invalid_channel.recv()
    invalid_sender.close()
    invalid_channel.close()


class _PollProcess:
    def __init__(self, return_code: int | None) -> None:
        self.return_code = return_code

    def poll(self) -> int | None:
        return self.return_code


class _TryChannel:
    def __init__(self, messages: list[object]) -> None:
        self.messages = messages

    def try_recv(self) -> object:
        if self.messages:
            return self.messages.pop(0)
        return archive_process._NO_MESSAGE


@pytest.mark.asyncio
async def test_wait_state_machines_reject_worker_protocol_failures() -> None:
    incomplete = archive_process._BootstrapState()
    with pytest.raises(archive_process.ProcessWorkerError, match="bootstrap"):
        await archive_process._wait_for_bootstrap(
            cast(subprocess.Popen[bytes], _PollProcess(17)),
            incomplete,
            deadline=anyio.current_time() + 1,
        )

    for acknowledgement, message in [
        (("error", "BrokenTarget"), "BrokenTarget"),
        (("unexpected", 0), "invalid startup"),
    ]:
        with pytest.raises(archive_process.ProcessWorkerError, match=message):
            await archive_process._wait_for_startup(
                cast(subprocess.Popen[bytes], _PollProcess(None)),
                cast(Any, _TryChannel([acknowledgement])),
                expected_descriptors=0,
                deadline=anyio.current_time() + 1,
            )

    with pytest.raises(archive_process.ProcessWorkerError, match="exited during startup"):
        await archive_process._wait_for_startup(
            cast(subprocess.Popen[bytes], _PollProcess(9)),
            cast(Any, _TryChannel([])),
            expected_descriptors=0,
            deadline=anyio.current_time() + 1,
        )
    with pytest.raises(archive_process.ProcessDeadlineError, match="startup exceeded"):
        await archive_process._wait_for_startup(
            cast(subprocess.Popen[bytes], _PollProcess(None)),
            cast(Any, _TryChannel([])),
            expected_descriptors=0,
            deadline=anyio.current_time(),
        )

    for result, message in [
        (("unexpected", 1), "invalid result"),
        (("error", "ChildFailure"), "ChildFailure"),
    ]:
        with pytest.raises(archive_process.ProcessWorkerError, match=message):
            await archive_process._wait_for_result(
                cast(subprocess.Popen[bytes], _PollProcess(None)),
                cast(Any, _TryChannel([result])),
                deadline=anyio.current_time() + 1,
            )
    with pytest.raises(archive_process.ProcessWorkerError, match="without a result"):
        await archive_process._wait_for_result(
            cast(subprocess.Popen[bytes], _PollProcess(23)),
            cast(Any, _TryChannel([])),
            deadline=anyio.current_time() + 1,
        )
    with pytest.raises(archive_process.ProcessDeadlineError, match="total deadline"):
        await archive_process._wait_for_result(
            cast(subprocess.Popen[bytes], _PollProcess(None)),
            cast(Any, _TryChannel([])),
            deadline=anyio.current_time(),
        )


async def _never_yields() -> AsyncIterator[bytes]:
    await anyio.sleep_forever()
    yield b"unreachable"


@pytest.mark.asyncio
async def test_input_deadlines_and_public_runner_deadline_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iterator = cast(Any, _never_yields())
    with pytest.raises(archive_process.ProcessDeadlineError, match="input exceeded"):
        await archive_process._next_input_chunk(iterator, deadline=anyio.current_time())
    await iterator.aclose()

    iterator = cast(Any, _never_yields())
    monkeypatch.setattr(anyio, "fail_after", _immediate_timeout)
    with pytest.raises(archive_process.ProcessDeadlineError, match="input exceeded"):
        await archive_process._next_input_chunk(iterator, deadline=anyio.current_time() + 1)
    await iterator.aclose()

    with pytest.raises(archive_process.ProcessDeadlineError, match="positive deadline"):
        await archive_process.run_in_spawn_process(str, timeout_seconds=0)
    with pytest.raises(archive_process.ProcessDeadlineError, match="termination deadline"):
        await archive_process.run_in_spawn_process(
            str,
            timeout_seconds=1,
            termination_timeout_seconds=float("nan"),
        )
    with pytest.raises(archive_process.ProcessDeadlineError, match="positive deadline"):
        await archive_process.run_in_spawn_process_with_input(
            str,
            cast(Any, _never_yields()),
            timeout_seconds=float("inf"),
        )


class _EscalatingProcess:
    def __init__(self, *, stubborn: bool = False) -> None:
        self.stubborn = stubborn
        self.killed = False
        self.terminated = False
        self.waits: list[float | None] = []
        self._handle: _FakeHandle | None = _FakeHandle()

    def poll(self) -> int | None:
        if self.stubborn or not self.killed:
            return None
        return 9

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    def wait(self, timeout: float | None = None) -> int:
        self.waits.append(timeout)
        if self.stubborn or not self.killed:
            raise subprocess.TimeoutExpired("child", timeout or 0.0)
        return 9


class _FakeHandle:
    def __init__(self) -> None:
        self.closed = False

    def Close(self) -> None:  # noqa: N802 - mirrors subprocess' Windows handle
        self.closed = True


class _FakeConnection:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeBootstrapThread:
    def __init__(self, *, alive: bool) -> None:
        self.alive = alive
        self.joined: list[float | None] = []

    def join(self, timeout: float | None = None) -> None:
        self.joined.append(timeout)

    def is_alive(self) -> bool:
        return self.alive


class _FakeChildChannel:
    def __init__(self, process: object) -> None:
        self.process = process
        self.connection = _FakeConnection()
        self.bootstrap_closed = False

    def close_bootstrap(self) -> None:
        self.bootstrap_closed = True


def test_process_cleanup_escalates_and_never_hides_termination_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escalating = _EscalatingProcess()
    with monkeypatch.context() as context:
        context.setattr(os, "name", "nt")
        archive_process._terminate_process(cast(subprocess.Popen[bytes], escalating))
    assert escalating.terminated
    assert escalating.killed
    assert escalating._handle is None

    stubborn = _EscalatingProcess(stubborn=True)
    with pytest.raises(RuntimeError, match="could not be terminated"):
        archive_process._terminate_process(
            cast(subprocess.Popen[bytes], stubborn),
            timeout_seconds=0.01,
        )

    child = _FakeChildChannel(_PollProcess(0))

    def fail_termination(_process: object, *, timeout_seconds: float | None = None) -> None:
        del timeout_seconds
        raise OSError("controlled termination failure")

    monkeypatch.setattr(archive_process, "_terminate_process", fail_termination)
    with pytest.raises(OSError, match="controlled termination failure"):
        archive_process._close_child(cast(Any, child), None, timeout_seconds=1)
    assert child.connection.closed
    assert child.bootstrap_closed

    child = _FakeChildChannel(_PollProcess(0))
    monkeypatch.setattr(archive_process, "_terminate_process", lambda *_args, **_kwargs: None)
    thread = _FakeBootstrapThread(alive=True)
    with pytest.raises(RuntimeError, match="bootstrap thread"):
        archive_process._close_child(cast(Any, child), cast(Any, thread), timeout_seconds=1)
    assert thread.joined


def _join_values(left: str, right: str) -> str:
    return left + right


def _join_stream(chunks: Iterator[bytes], prefix: bytes) -> bytes:
    return prefix + b"".join(chunks)


def _fail_target() -> None:
    raise RuntimeError("controlled target failure")


class _ProtocolChannel:
    def __init__(self, received: list[object], descriptor: int | None = None) -> None:
        self.received = received
        self.descriptor = descriptor
        self.sent: list[object] = []
        self.closed = False

    def recv(self) -> object:
        if not self.received:
            raise EOFError("empty protocol")
        return self.received.pop(0)

    def send(self, value: object) -> None:
        self.sent.append(value)

    def receive_descriptor(self) -> int:
        if self.descriptor is None:
            raise OSError("missing descriptor")
        return self.descriptor

    def close(self) -> None:
        self.closed = True


def _run_child_main(
    monkeypatch: pytest.MonkeyPatch,
    channel: _ProtocolChannel,
) -> int:
    monkeypatch.setattr(archive_process, "_install_parent_guard", lambda _pid: None)
    monkeypatch.setattr(archive_process, "_child_connection", lambda _arguments: channel)
    return archive_process._child_main(["--socket", "1", "123"])


def test_child_main_executes_plain_and_streaming_protocols_and_cleans_descriptors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plain = _ProtocolChannel([(_join_values, ("left", "right"))])
    assert _run_child_main(monkeypatch, plain) == 0
    assert plain.sent == [("ready", 0), ("ok", "leftright")]
    assert plain.closed

    streaming = _ProtocolChannel(
        [
            ("stream", _join_stream, (b"prefix-",)),
            ("chunk", b"one"),
            ("chunk", b"two"),
            ("end",),
        ]
    )
    assert _run_child_main(monkeypatch, streaming) == 0
    assert streaming.sent == [("ready", 0), ("ok", b"prefix-onetwo")]

    with tempfile.TemporaryFile(mode="w+b") as handle:
        received_descriptor = os.dup(handle.fileno())
        descriptor = archive_process.DuplicatedFileDescriptor(handle.fileno())
        descriptor.__setstate__({})
        descriptor_channel = _ProtocolChannel(
            [(_join_values, ("descriptor", "-closed"), descriptor)],
            descriptor=received_descriptor,
        )

        def ignore_descriptor(
            left: str,
            right: str,
            _descriptor: archive_process.DuplicatedFileDescriptor,
        ) -> str:
            return _join_values(left, right)

        descriptor_channel.received[0] = (
            ignore_descriptor,
            ("descriptor", "-closed", descriptor),
        )
        assert _run_child_main(monkeypatch, descriptor_channel) == 0
        assert descriptor_channel.sent == [("ready", 1), ("ok", "descriptor-closed")]
        with pytest.raises(OSError, match=r"Bad file descriptor|handle is invalid"):
            os.fstat(received_descriptor)


@pytest.mark.parametrize(
    ("payload", "error_name"),
    [
        (("invalid",), "TypeError"),
        ((1, ()), "TypeError"),
        ((_join_values, []), "TypeError"),
        ((_fail_target, ()), "RuntimeError"),
    ],
)
def test_child_main_reports_only_error_types_and_closes_the_channel(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
    error_name: str,
) -> None:
    channel = _ProtocolChannel([payload])
    assert _run_child_main(monkeypatch, channel) == 0
    assert channel.sent[-1] == ("error", error_name)
    assert channel.closed


def test_stream_and_bootstrap_payload_validation_rejects_malformed_frames() -> None:
    payloads: list[object] = [("invalid",), (1, ()), (_join_values, [])]
    for payload in payloads:
        with pytest.raises(TypeError):
            archive_process._validated_payload(payload)

    invalid_stream = _ProtocolChannel([("chunk", "not-bytes")])
    with pytest.raises(TypeError, match="input frame"):
        next(archive_process._streamed_input_chunks(cast(Any, invalid_stream)))

    with pytest.raises(ValueError, match="child bootstrap"):
        archive_process._child_connection(["--wrong", "1", "2"])
    with pytest.raises(ValueError, match="parent identity"):
        archive_process._parent_pid(["--wrong", "1", "2"])
    assert archive_process._parent_pid(["--socket", "1", "27"]) == 27


class _ChildSocket:
    def __init__(self) -> None:
        self.inheritable: bool | None = None

    def set_inheritable(self, inheritable: bool) -> None:
        self.inheritable = inheritable


def test_child_connection_uses_only_the_inherited_socket_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    child_socket = _ChildSocket()
    captured: list[int] = []

    def socket_factory(*, fileno: int) -> socket.socket:
        captured.append(fileno)
        return cast(socket.socket, child_socket)

    monkeypatch.setattr(socket, "socket", socket_factory)
    channel = archive_process._child_connection(["--socket", "41", "9"])
    assert captured == [41]
    assert child_socket.inheritable is False
    assert cast(object, channel._socket) is child_socket


class _FakeThread:
    starts = 0

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        return None

    def start(self) -> None:
        type(self).starts += 1


def test_parent_guard_dispatches_without_starting_unbounded_test_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_pid = 101
    calls: list[tuple[str, int]] = []

    with monkeypatch.context() as context:
        context.setattr(os, "getppid", lambda: parent_pid + 1)
        context.setattr(os, "_exit", _raise_exit)
        with pytest.raises(_ExitCalledError) as caught:
            archive_process._install_parent_guard(parent_pid)
        assert caught.value.code == archive_process._ORPHAN_EXIT_CODE

    with monkeypatch.context() as context:
        context.setattr(os, "name", "nt")
        context.setattr(os, "getppid", lambda: parent_pid)
        context.setattr(
            archive_process,
            "_install_windows_parent_guard",
            lambda pid: calls.append(("windows", pid)),
        )
        archive_process._install_parent_guard(parent_pid)

    with monkeypatch.context() as context:
        context.setattr(os, "name", "posix")
        context.setattr(os, "getppid", lambda: parent_pid)
        context.setattr(sys, "platform", "linux")
        context.setattr(
            archive_process,
            "_install_linux_parent_guard",
            lambda pid: calls.append(("linux", pid)),
        )
        archive_process._install_parent_guard(parent_pid)

    _FakeThread.starts = 0
    with monkeypatch.context() as context:
        context.setattr(os, "name", "posix")
        context.setattr(os, "getppid", lambda: parent_pid)
        context.setattr(sys, "platform", "darwin")
        context.setattr(threading, "Thread", _FakeThread)
        archive_process._install_parent_guard(parent_pid)

    assert calls == [("windows", parent_pid), ("linux", parent_pid)]
    assert _FakeThread.starts == 1


def _windows_module(
    *,
    open_process: Any,
    wait_for_single_object: Any,
    closed_handles: list[int],
) -> ModuleType:
    module = ModuleType("_winapi")
    module.PROCESS_DUP_HANDLE = 64  # type: ignore[attr-defined]
    module.SYNCHRONIZE = 1  # type: ignore[attr-defined]
    module.WAIT_OBJECT_0 = 0  # type: ignore[attr-defined]
    module.INFINITE = 0xFFFFFFFF  # type: ignore[attr-defined]
    module.OpenProcess = open_process  # type: ignore[attr-defined]
    module.WaitForSingleObject = wait_for_single_object  # type: ignore[attr-defined]
    module.CloseHandle = closed_handles.append  # type: ignore[attr-defined]
    return module


def test_windows_parent_guard_validates_identity_and_closes_its_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_pid = 303
    closed: list[int] = []

    def fail_open(*_args: object) -> int:
        raise OSError("gone")

    failed_module = _windows_module(
        open_process=fail_open,
        wait_for_single_object=lambda *_args: 1,
        closed_handles=closed,
    )
    with monkeypatch.context() as context:
        context.setitem(sys.modules, "_winapi", failed_module)
        context.setattr(os, "_exit", _raise_exit)
        with pytest.raises(_ExitCalledError):
            archive_process._install_windows_parent_guard(parent_pid)

    live_module = _windows_module(
        open_process=lambda *_args: 77,
        wait_for_single_object=lambda *_args: 1,
        closed_handles=closed,
    )
    _FakeThread.starts = 0
    with monkeypatch.context() as context:
        context.setitem(sys.modules, "_winapi", live_module)
        context.setattr(os, "getppid", lambda: parent_pid)
        context.setattr(threading, "Thread", _FakeThread)
        archive_process._install_windows_parent_guard(parent_pid)
    assert _FakeThread.starts == 1

    dead_module = _windows_module(
        open_process=lambda *_args: 88,
        wait_for_single_object=lambda *_args: 0,
        closed_handles=closed,
    )
    with monkeypatch.context() as context:
        context.setitem(sys.modules, "_winapi", dead_module)
        context.setattr(os, "getppid", lambda: parent_pid)
        context.setattr(os, "_exit", _raise_exit)
        with pytest.raises(_ExitCalledError):
            archive_process._install_windows_parent_guard(parent_pid)
    assert 88 in closed

    waited: list[tuple[int, int]] = []
    wait_module = _windows_module(
        open_process=lambda *_args: 99,
        wait_for_single_object=lambda handle, timeout: waited.append((handle, timeout)),
        closed_handles=closed,
    )
    with monkeypatch.context() as context:
        context.setitem(sys.modules, "_winapi", wait_module)
        context.setattr(os, "_exit", _raise_exit)
        with pytest.raises(_ExitCalledError):
            archive_process._wait_for_windows_parent(99)
    assert waited == [(99, 0xFFFFFFFF)]
    assert 99 in closed


class _FakeLibc:
    def __init__(self, result: int) -> None:
        self.result = result
        self.calls: list[tuple[int, int, int, int, int]] = []

    def prctl(self, *arguments: int) -> int:
        self.calls.append(cast(tuple[int, int, int, int, int], arguments))
        return self.result


def test_linux_and_polling_parent_guards_exit_on_lost_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_pid = 404
    failed_libc = _FakeLibc(1)
    with monkeypatch.context() as context:
        context.setattr(ctypes, "CDLL", lambda *_args, **_kwargs: failed_libc)
        context.setattr(os, "_exit", _raise_exit)
        with pytest.raises(_ExitCalledError):
            archive_process._install_linux_parent_guard(parent_pid)
    assert failed_libc.calls

    live_libc = _FakeLibc(0)
    with monkeypatch.context() as context:
        context.setattr(ctypes, "CDLL", lambda *_args, **_kwargs: live_libc)
        context.setattr(os, "getppid", lambda: parent_pid)
        archive_process._install_linux_parent_guard(parent_pid)

    with monkeypatch.context() as context:
        context.setattr(ctypes, "CDLL", lambda *_args, **_kwargs: live_libc)
        context.setattr(os, "getppid", lambda: parent_pid + 1)
        context.setattr(os, "_exit", _raise_exit)
        with pytest.raises(_ExitCalledError):
            archive_process._install_linux_parent_guard(parent_pid)

    identities = iter((parent_pid, parent_pid + 1))
    sleeps: list[float] = []
    with monkeypatch.context() as context:
        context.setattr(os, "getppid", lambda: next(identities))
        context.setattr(time, "sleep", sleeps.append)
        context.setattr(os, "_exit", _raise_exit)
        with pytest.raises(_ExitCalledError):
            archive_process._poll_parent_identity(parent_pid)
    assert sleeps == [archive_process._POLL_INTERVAL_SECONDS]
