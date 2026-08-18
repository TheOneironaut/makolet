"""Killable process boundary for blocking archive operations."""

from __future__ import annotations

import math
import os
import pickle
import select
import socket
import struct
import subprocess
import sys
import threading
import time
from array import array
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, cast

import anyio

_MODULE_NAME = "makolet.adapters.archive._process"
_CHILD_BOOTSTRAP = (
    f"import sys; from {_MODULE_NAME} import _child_main; "
    "raise SystemExit(_child_main(sys.argv[1:]))"
)
_POLL_INTERVAL_SECONDS = 0.01
_TERMINATE_GRACE_SECONDS = 0.5
_KILL_GRACE_SECONDS = 0.5
_BOOTSTRAP_THREAD_GRACE_SECONDS = 0.5
_ORPHAN_EXIT_CODE = 70
_FRAME_HEADER = struct.Struct("!Q")
_MAX_FRAME_BYTES = 64 * 1024 * 1024
_NO_MESSAGE = object()


class ProcessDeadlineError(TimeoutError):
    """A child operation did not finish before its total deadline."""


class ProcessWorkerError(RuntimeError):
    """A child operation failed without exposing its exception text."""


class DuplicatedFileDescriptor:
    """Transfer a parent descriptor only after a child has connected."""

    def __init__(self, descriptor: int) -> None:
        if descriptor < 0:
            raise ValueError("file descriptor must be non-negative")
        self._source_descriptor: int | None = descriptor
        self._received_descriptor: int | None = None

    def __getstate__(self) -> dict[str, object]:
        return {}

    def __setstate__(self, _state: dict[str, object]) -> None:
        self._source_descriptor = None
        self._received_descriptor = None

    def _send(self, connection: _SocketChannel, child_pid: int) -> None:
        if self._source_descriptor is None:
            raise RuntimeError("file descriptor transfer was already consumed")
        connection.send_descriptor(self._source_descriptor, child_pid)
        self._source_descriptor = None

    def _receive(self, connection: _SocketChannel) -> None:
        self._received_descriptor = connection.receive_descriptor()

    def detach(self) -> int:
        if self._received_descriptor is not None:
            descriptor = self._received_descriptor
            self._received_descriptor = None
            return descriptor
        if self._source_descriptor is not None:
            descriptor = os.dup(self._source_descriptor)
            self._source_descriptor = None
            return descriptor
        raise RuntimeError("file descriptor transfer is not available")

    def _close_received(self) -> None:
        if self._received_descriptor is not None:
            os.close(self._received_descriptor)
            self._received_descriptor = None


class _SocketChannel:
    """Length-framed messages and descriptor transfer over one inherited socket."""

    def __init__(self, channel_socket: socket.socket) -> None:
        self._socket = channel_socket
        self._receive_buffer = bytearray()

    def send(self, value: object) -> None:
        self._socket.sendall(self._frame(value))

    async def send_async(self, value: object, *, deadline: float) -> None:
        frame = memoryview(self._frame(value))
        while frame:
            try:
                sent = self._socket.send(frame)
            except BlockingIOError:
                sent = 0
            if sent > 0:
                frame = frame[sent:]
                continue
            remaining = deadline - anyio.current_time()
            if remaining <= 0:
                raise ProcessDeadlineError("Archive child input exceeded its total deadline")
            try:
                with anyio.fail_after(remaining):
                    await anyio.wait_socket_writable(self._socket)
            except TimeoutError as error:
                raise ProcessDeadlineError(
                    "Archive child input exceeded its total deadline"
                ) from error

    def recv(self) -> object:
        header = self._read_exact(_FRAME_HEADER.size)
        (payload_size,) = _FRAME_HEADER.unpack(header)
        self._validate_frame_size(payload_size)
        return pickle.loads(self._read_exact(payload_size))  # noqa: S301 - private child IPC

    def try_recv(self) -> object:
        message = self._pop_buffered_message()
        if message is not _NO_MESSAGE:
            return message
        while select.select([self._socket], [], [], 0)[0]:
            try:
                chunk = self._socket.recv(64 * 1024)
            except BlockingIOError:
                break
            if not chunk:
                raise EOFError("archive child IPC channel closed")
            self._receive_buffer.extend(chunk)
            message = self._pop_buffered_message()
            if message is not _NO_MESSAGE:
                return message
        return _NO_MESSAGE

    def set_nonblocking(self) -> None:
        self._socket.setblocking(False)

    def send_descriptor(self, descriptor: int, child_pid: int) -> None:
        if os.name == "nt":
            self._send_windows_descriptor(descriptor, child_pid)
            return
        posix_socket = cast(Any, self._socket)
        socket_module = cast(Any, socket)
        rights = array("i", [descriptor])
        sent = posix_socket.sendmsg(
            [b"\0"],
            [(socket.SOL_SOCKET, int(socket_module.SCM_RIGHTS), rights)],
        )
        if sent != 1:
            raise OSError("archive descriptor transfer did not send its marker")

    def receive_descriptor(self) -> int:
        if os.name == "nt":
            return self._receive_windows_descriptor()
        posix_socket = cast(Any, self._socket)
        socket_module = cast(Any, socket)
        rights = array("i")
        marker, ancillary, flags, _address = posix_socket.recvmsg(
            1,
            socket_module.CMSG_SPACE(rights.itemsize),
        )
        if marker != b"\0" or flags & getattr(socket, "MSG_CTRUNC", 0):
            raise OSError("archive descriptor transfer was truncated")
        received: list[int] = []
        for level, kind, data in ancillary:
            if level != socket.SOL_SOCKET or kind != socket_module.SCM_RIGHTS:
                continue
            usable = len(data) - (len(data) % rights.itemsize)
            rights.frombytes(data[:usable])
            received.extend(rights)
            rights = array("i")
        if len(received) != 1:
            for descriptor in received:
                os.close(descriptor)
            raise OSError("archive descriptor transfer returned an invalid descriptor count")
        descriptor = received[0]
        try:
            os.set_inheritable(descriptor, False)
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    def close(self) -> None:
        self._socket.close()

    def _read_exact(self, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = self._socket.recv(size - len(chunks))
            if not chunk:
                raise EOFError("archive child IPC channel closed")
            chunks.extend(chunk)
        return bytes(chunks)

    def _pop_buffered_message(self) -> object:
        if len(self._receive_buffer) < _FRAME_HEADER.size:
            return _NO_MESSAGE
        (payload_size,) = _FRAME_HEADER.unpack_from(self._receive_buffer)
        self._validate_frame_size(payload_size)
        frame_size = _FRAME_HEADER.size + payload_size
        if len(self._receive_buffer) < frame_size:
            return _NO_MESSAGE
        payload = bytes(self._receive_buffer[_FRAME_HEADER.size : frame_size])
        del self._receive_buffer[:frame_size]
        return pickle.loads(payload)  # noqa: S301 - private child IPC

    @staticmethod
    def _validate_frame_size(payload_size: int) -> None:
        if payload_size > _MAX_FRAME_BYTES:
            raise ValueError("archive child IPC frame exceeded its size limit")

    @staticmethod
    def _frame(value: object) -> bytes:
        payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        if len(payload) > _MAX_FRAME_BYTES:
            raise ValueError("archive child IPC frame exceeded its size limit")
        return _FRAME_HEADER.pack(len(payload)) + payload

    def _send_windows_descriptor(self, descriptor: int, child_pid: int) -> None:
        import _winapi
        import msvcrt

        child_process = _winapi.OpenProcess(_winapi.PROCESS_DUP_HANDLE, False, child_pid)
        try:
            child_handle = _winapi.DuplicateHandle(
                _winapi.GetCurrentProcess(),
                msvcrt.get_osfhandle(descriptor),
                child_process,
                0,
                False,
                _winapi.DUPLICATE_SAME_ACCESS,
            )
        finally:
            _winapi.CloseHandle(child_process)
        self.send(("descriptor", int(child_handle)))

    def _receive_windows_descriptor(self) -> int:
        import _winapi
        import msvcrt

        message = self.recv()
        if (
            not isinstance(message, tuple)
            or len(message) != 2
            or message[0] != "descriptor"
            or not isinstance(message[1], int)
            or message[1] < 0
        ):
            raise OSError("archive descriptor transfer returned an invalid handle")
        handle = message[1]
        try:
            return msvcrt.open_osfhandle(
                handle,
                os.O_RDWR | getattr(os, "O_BINARY", 0),
            )
        except BaseException:
            _winapi.CloseHandle(handle)
            raise


@dataclass(slots=True)
class _BootstrapState:
    completed: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None


@dataclass(slots=True)
class _ChildChannel:
    process: subprocess.Popen[bytes]
    connection: _SocketChannel

    def close_bootstrap(self) -> None:
        return None


async def run_in_spawn_process[T](
    target: Callable[..., T],
    *args: object,
    timeout_seconds: float,
    termination_timeout_seconds: float | None = None,
) -> T:
    """Run ``target`` in an isolated child killed on timeout or cancellation."""

    if timeout_seconds <= 0 or not math.isfinite(timeout_seconds):
        raise ProcessDeadlineError("Archive child received no positive deadline")
    if termination_timeout_seconds is not None and (
        termination_timeout_seconds <= 0 or not math.isfinite(termination_timeout_seconds)
    ):
        raise ProcessDeadlineError("Archive child received no positive termination deadline")
    deadline = anyio.current_time() + timeout_seconds
    channel = _launch_child()
    descriptors = tuple(
        argument for argument in args if isinstance(argument, DuplicatedFileDescriptor)
    )
    bootstrap_state: _BootstrapState | None = None
    bootstrap_thread: threading.Thread | None = None
    try:
        connection = channel.connection
        bootstrap_state = _BootstrapState()
        bootstrap_thread = threading.Thread(
            target=_send_bootstrap,
            args=(
                connection,
                target,
                args,
                descriptors,
                channel.process.pid,
                bootstrap_state,
                False,
            ),
            name="makolet-archive-bootstrap",
            daemon=True,
        )
        bootstrap_thread.start()
        await _wait_for_bootstrap(
            channel.process,
            bootstrap_state,
            deadline=deadline,
        )
        connection.set_nonblocking()
        await _wait_for_startup(
            channel.process,
            connection,
            expected_descriptors=len(descriptors),
            deadline=deadline,
        )
        return cast("T", await _wait_for_result(channel.process, connection, deadline=deadline))
    finally:
        _close_child(
            channel,
            bootstrap_thread,
            timeout_seconds=termination_timeout_seconds,
        )


async def run_in_spawn_process_with_input[T](
    target: Callable[..., T],
    input_chunks: AsyncIterable[bytes],
    *args: object,
    timeout_seconds: float,
    termination_timeout_seconds: float | None = None,
) -> T:
    """Stream bounded byte chunks to a killable synchronous child operation."""

    if timeout_seconds <= 0 or not math.isfinite(timeout_seconds):
        raise ProcessDeadlineError("Archive child received no positive deadline")
    if termination_timeout_seconds is not None and (
        termination_timeout_seconds <= 0 or not math.isfinite(termination_timeout_seconds)
    ):
        raise ProcessDeadlineError("Archive child received no positive termination deadline")
    deadline = anyio.current_time() + timeout_seconds
    channel = _launch_child()
    descriptors = tuple(
        argument for argument in args if isinstance(argument, DuplicatedFileDescriptor)
    )
    bootstrap_thread: threading.Thread | None = None
    try:
        connection = channel.connection
        bootstrap_state = _BootstrapState()
        bootstrap_thread = threading.Thread(
            target=_send_bootstrap,
            args=(
                connection,
                target,
                args,
                descriptors,
                channel.process.pid,
                bootstrap_state,
                True,
            ),
            name="makolet-archive-bootstrap",
            daemon=True,
        )
        bootstrap_thread.start()
        await _wait_for_bootstrap(channel.process, bootstrap_state, deadline=deadline)
        connection.set_nonblocking()
        await _wait_for_startup(
            channel.process,
            connection,
            expected_descriptors=len(descriptors),
            deadline=deadline,
        )
        iterator = input_chunks.__aiter__()
        while True:
            chunk = await _next_input_chunk(iterator, deadline=deadline)
            if chunk is None:
                break
            if not isinstance(chunk, bytes):
                raise TypeError("Archive child input chunks must be bytes")
            await connection.send_async(("chunk", chunk), deadline=deadline)
        await connection.send_async(("end",), deadline=deadline)
        return cast("T", await _wait_for_result(channel.process, connection, deadline=deadline))
    finally:
        _close_child(
            channel,
            bootstrap_thread,
            timeout_seconds=termination_timeout_seconds,
        )


def _close_child(
    channel: _ChildChannel,
    bootstrap_thread: threading.Thread | None,
    *,
    timeout_seconds: float | None,
) -> None:
    cleanup_budget = (
        _TERMINATE_GRACE_SECONDS + _KILL_GRACE_SECONDS
        if timeout_seconds is None
        else timeout_seconds
    )
    cleanup_deadline = time.monotonic() + cleanup_budget
    channel.connection.close()
    termination_error: BaseException | None = None
    try:
        _terminate_process(
            channel.process,
            timeout_seconds=max(0.0, cleanup_deadline - time.monotonic()),
        )
    except BaseException as error:
        termination_error = error
    if bootstrap_thread is not None:
        bootstrap_thread.join(
            min(
                _BOOTSTRAP_THREAD_GRACE_SECONDS,
                max(0.0, cleanup_deadline - time.monotonic()),
            )
        )
        if bootstrap_thread.is_alive():
            raise RuntimeError("Archive bootstrap thread could not be stopped") from (
                termination_error
            )
    channel.close_bootstrap()
    if termination_error is not None:
        raise termination_error


def _launch_child() -> _ChildChannel:
    parent_socket, child_socket = socket.socketpair()
    process: subprocess.Popen[bytes] | None = None
    try:
        child_descriptor = child_socket.fileno()
        child_socket.set_inheritable(True)
        command = [
            sys.executable,
            "-c",
            _CHILD_BOOTSTRAP,
            "--socket",
            str(child_descriptor),
            str(os.getpid()),
        ]
        popen_options: dict[str, Any] = {
            "close_fds": True,
            "stderr": subprocess.DEVNULL,
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
        }
        if os.name == "nt":
            startup_info = subprocess.STARTUPINFO()
            startup_info.lpAttributeList = {"handle_list": [child_descriptor]}
            popen_options.update(
                {
                    "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    "startupinfo": startup_info,
                }
            )
            child_environment = os.environ.copy()
            child_environment["__PYVENV_LAUNCHER__"] = sys.executable
            popen_options["env"] = child_environment
            command[0] = getattr(sys, "_base_executable", sys.executable)
        else:
            popen_options["pass_fds"] = (child_descriptor,)
        process = subprocess.Popen(  # noqa: S603 - fixed interpreter and internal module
            command,
            **popen_options,
        )
        child_socket.close()
    except BaseException:
        parent_socket.close()
        child_socket.close()
        if process is not None:
            _terminate_process(process)
        raise
    return _ChildChannel(process=process, connection=_SocketChannel(parent_socket))


def _send_bootstrap(
    connection: _SocketChannel,
    target: Callable[..., object],
    args: tuple[object, ...],
    descriptors: tuple[DuplicatedFileDescriptor, ...],
    child_pid: int,
    state: _BootstrapState,
    streaming: bool,
) -> None:
    try:
        payload = ("stream", target, args) if streaming else (target, args)
        connection.send(payload)
        for descriptor in descriptors:
            descriptor._send(connection, child_pid)
    except BaseException as error:
        state.error = error
    finally:
        state.completed.set()


async def _wait_for_bootstrap(
    process: subprocess.Popen[bytes],
    state: _BootstrapState,
    *,
    deadline: float,
) -> None:
    while True:
        if state.completed.is_set():
            if state.error is not None:
                raise ProcessWorkerError("Archive child bootstrap transfer failed") from state.error
            return
        return_code = process.poll()
        if return_code is not None:
            raise ProcessWorkerError(
                f"Archive child exited during bootstrap (exit code {return_code})"
            )
        remaining = deadline - anyio.current_time()
        if remaining <= 0:
            raise ProcessDeadlineError("Archive child bootstrap exceeded its total deadline")
        await anyio.sleep(min(_POLL_INTERVAL_SECONDS, remaining))


async def _wait_for_startup(
    process: subprocess.Popen[bytes],
    connection: _SocketChannel,
    *,
    expected_descriptors: int,
    deadline: float,
) -> None:
    while True:
        acknowledgement = connection.try_recv()
        if acknowledgement is not _NO_MESSAGE:
            if acknowledgement != ("ready", expected_descriptors):
                if (
                    isinstance(acknowledgement, tuple)
                    and len(acknowledgement) == 2
                    and acknowledgement[0] == "error"
                ):
                    raise ProcessWorkerError(
                        f"Archive child failed with {str(acknowledgement[1])[:80]} during startup"
                    )
                raise ProcessWorkerError(
                    "Archive child returned an invalid startup acknowledgement"
                )
            return
        return_code = process.poll()
        if return_code is not None:
            raise ProcessWorkerError(
                f"Archive child exited during startup (exit code {return_code})"
            )
        remaining = deadline - anyio.current_time()
        if remaining <= 0:
            raise ProcessDeadlineError("Archive child startup exceeded its total deadline")
        await anyio.sleep(min(_POLL_INTERVAL_SECONDS, remaining))


async def _next_input_chunk(
    iterator: AsyncIterator[bytes],
    *,
    deadline: float,
) -> bytes | None:
    remaining = deadline - anyio.current_time()
    if remaining <= 0:
        raise ProcessDeadlineError("Archive child input exceeded its total deadline")
    try:
        with anyio.fail_after(remaining):
            return await anext(iterator)
    except StopAsyncIteration:
        return None
    except TimeoutError as error:
        raise ProcessDeadlineError("Archive child input exceeded its total deadline") from error


async def _wait_for_result(
    process: subprocess.Popen[bytes],
    connection: _SocketChannel,
    *,
    deadline: float,
) -> object:
    while True:
        message = connection.try_recv()
        if message is not _NO_MESSAGE:
            if (
                not isinstance(message, tuple)
                or len(message) != 2
                or message[0] not in {"ok", "error"}
            ):
                raise ProcessWorkerError("Archive child returned an invalid result")
            if message[0] == "error":
                raise ProcessWorkerError(f"Archive child failed with {str(message[1])[:80]}")
            return message[1]
        return_code = process.poll()
        if return_code is not None:
            raise ProcessWorkerError(
                f"Archive child exited without a result (exit code {return_code})"
            )
        remaining = deadline - anyio.current_time()
        if remaining <= 0:
            raise ProcessDeadlineError("Archive child exceeded its total deadline")
        await anyio.sleep(min(_POLL_INTERVAL_SECONDS, remaining))


def _terminate_process(
    process: subprocess.Popen[bytes],
    *,
    timeout_seconds: float | None = None,
) -> None:
    """Reap a child without ever performing an unbounded wait."""

    termination_budget = (
        _TERMINATE_GRACE_SECONDS + _KILL_GRACE_SECONDS
        if timeout_seconds is None
        else timeout_seconds
    )
    deadline = time.monotonic() + termination_budget
    if process.poll() is None:
        process.terminate()
        with suppress(subprocess.TimeoutExpired):
            process.wait(
                min(
                    _TERMINATE_GRACE_SECONDS,
                    termination_budget / 2,
                    max(0.0, deadline - time.monotonic()),
                )
            )
    if process.poll() is None:
        process.kill()
        with suppress(subprocess.TimeoutExpired):
            process.wait(
                min(
                    _KILL_GRACE_SECONDS,
                    max(0.0, deadline - time.monotonic()),
                )
            )
    if process.poll() is None:
        raise RuntimeError("Archive child could not be terminated")
    process.wait(0)
    if os.name == "nt":
        process_handle: Any = getattr(process, "_handle", None)
        if process_handle is not None:
            process_handle.Close()
            process._handle = None  # type: ignore[attr-defined]


def _child_main(arguments: list[str]) -> int:
    connection: _SocketChannel | None = None
    descriptors: tuple[DuplicatedFileDescriptor, ...] = ()
    try:
        _install_parent_guard(_parent_pid(arguments))
        connection = _child_connection(arguments)
        payload: Any = connection.recv()
        streaming, target, raw_args = _validated_payload(payload)
        descriptors = tuple(
            argument for argument in raw_args if isinstance(argument, DuplicatedFileDescriptor)
        )
        for descriptor in descriptors:
            descriptor._receive(connection)
        connection.send(("ready", len(descriptors)))
        result = (
            target(_streamed_input_chunks(connection), *raw_args)
            if streaming
            else target(*raw_args)
        )
        connection.send(("ok", result))
    except BaseException as error:
        if connection is not None:
            with suppress(BrokenPipeError, EOFError, OSError):
                connection.send(("error", type(error).__name__))
    finally:
        for descriptor in descriptors:
            descriptor._close_received()
        if connection is not None:
            connection.close()
    return 0


def _validated_payload(
    payload: object,
) -> tuple[bool, Callable[..., object], tuple[object, ...]]:
    streaming = False
    if isinstance(payload, tuple) and len(payload) == 2:
        target, raw_args = payload
    elif isinstance(payload, tuple) and len(payload) == 3 and payload[0] == "stream":
        streaming = True
        _stream, target, raw_args = payload
    else:
        raise TypeError("invalid child payload")
    if not callable(target) or not isinstance(raw_args, tuple):
        raise TypeError("invalid child target")
    return streaming, target, raw_args


def _streamed_input_chunks(connection: _SocketChannel) -> Iterator[bytes]:
    while True:
        message = connection.recv()
        if message == ("end",):
            return
        if (
            not isinstance(message, tuple)
            or len(message) != 2
            or message[0] != "chunk"
            or not isinstance(message[1], bytes)
        ):
            raise TypeError("invalid archive child input frame")
        yield message[1]


def _child_connection(arguments: list[str]) -> _SocketChannel:
    if len(arguments) == 3 and arguments[0] == "--socket":
        channel_socket = socket.socket(fileno=int(arguments[1]))
        channel_socket.set_inheritable(False)
        return _SocketChannel(channel_socket)
    raise ValueError("invalid archive child bootstrap")


def _parent_pid(arguments: list[str]) -> int:
    if len(arguments) == 3 and arguments[0] == "--socket":
        return int(arguments[2])
    raise ValueError("invalid archive parent identity")


def _install_parent_guard(parent_pid: int) -> None:
    if parent_pid <= 0 or os.getppid() != parent_pid:
        os._exit(_ORPHAN_EXIT_CODE)
    if os.name == "nt":
        _install_windows_parent_guard(parent_pid)
        return
    if sys.platform.startswith("linux"):
        _install_linux_parent_guard(parent_pid)
        return
    threading.Thread(
        target=_poll_parent_identity,
        args=(parent_pid,),
        name="makolet-archive-parent-monitor",
        daemon=True,
    ).start()


def _install_windows_parent_guard(parent_pid: int) -> None:
    import _winapi

    try:
        parent_handle = _winapi.OpenProcess(_winapi.SYNCHRONIZE, False, parent_pid)
    except OSError:
        os._exit(_ORPHAN_EXIT_CODE)
    if (
        os.getppid() != parent_pid
        or _winapi.WaitForSingleObject(parent_handle, 0) == _winapi.WAIT_OBJECT_0
    ):
        _winapi.CloseHandle(parent_handle)
        os._exit(_ORPHAN_EXIT_CODE)
    threading.Thread(
        target=_wait_for_windows_parent,
        args=(parent_handle,),
        name="makolet-archive-parent-monitor",
        daemon=True,
    ).start()


def _wait_for_windows_parent(parent_handle: int) -> None:
    import _winapi

    _winapi.WaitForSingleObject(parent_handle, _winapi.INFINITE)
    _winapi.CloseHandle(parent_handle)
    os._exit(_ORPHAN_EXIT_CODE)


def _install_linux_parent_guard(parent_pid: int) -> None:
    import ctypes
    import signal

    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, int(getattr(signal, "SIGKILL", 9)), 0, 0, 0) != 0:
        os._exit(_ORPHAN_EXIT_CODE)
    if os.getppid() != parent_pid:
        os._exit(_ORPHAN_EXIT_CODE)


def _poll_parent_identity(parent_pid: int) -> None:
    while os.getppid() == parent_pid:
        time.sleep(_POLL_INTERVAL_SECONDS)
    os._exit(_ORPHAN_EXIT_CODE)


if __name__ == "__main__":
    raise SystemExit(_child_main(sys.argv[1:]))
