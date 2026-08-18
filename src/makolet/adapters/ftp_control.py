"""Bounded control-channel replies for stdlib FTP clients."""

from __future__ import annotations

import ftplib
import io
import socket
import ssl
import threading
from collections.abc import Buffer, Callable
from contextlib import suppress
from types import MethodType
from typing import Protocol, cast

from makolet.application.ports import (
    MAXIMUM_TRANSFER_CHUNK_BYTES,
    MAXIMUM_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
)
from makolet.domain.errors import SourceAccessError

_MAXIMUM_REPLY_BYTES = 64 * 1024
_MAXIMUM_REPLY_LINES = 128
_MAXIMUM_OPERATION_LINES = 1024


class _ControlFile(Protocol):
    def readline(self, limit: int = -1) -> str: ...


class FtpControlLimitError(ftplib.Error):
    """A publisher crossed a control-channel or combined transfer boundary."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__("FTP control reply exceeds a configured safety limit")


class FtpControlBudget:
    """Account one FTP operation before any multiline reply is assembled."""

    def __init__(
        self,
        *,
        maximum_reply_bytes: int = _MAXIMUM_REPLY_BYTES,
        maximum_reply_lines: int = _MAXIMUM_REPLY_LINES,
        maximum_operation_bytes: int = MAXIMUM_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
        maximum_operation_lines: int = _MAXIMUM_OPERATION_LINES,
        maximum_total_bytes: int | None = None,
    ) -> None:
        if (
            maximum_reply_bytes <= 0
            or maximum_reply_lines <= 0
            or maximum_operation_bytes <= 0
            or maximum_operation_lines <= 0
            or (maximum_total_bytes is not None and maximum_total_bytes <= 0)
        ):
            raise ValueError("FTP control limits must be positive")
        self.maximum_reply_bytes = maximum_reply_bytes
        self.maximum_reply_lines = maximum_reply_lines
        self.maximum_operation_bytes = maximum_operation_bytes
        self.maximum_operation_lines = maximum_operation_lines
        self.maximum_total_bytes = maximum_total_bytes
        self._control_bytes = 0
        self._control_lines = 0
        self._data_bytes = 0
        self._wire_bytes = 0
        self._lock = threading.Lock()

    @property
    def control_bytes(self) -> int:
        with self._lock:
            return self._control_bytes

    @property
    def control_lines(self) -> int:
        with self._lock:
            return self._control_lines

    @property
    def data_bytes(self) -> int:
        with self._lock:
            return self._data_bytes

    @property
    def total_bytes(self) -> int:
        with self._lock:
            return self._accounted_bytes_unlocked()

    @property
    def wire_bytes(self) -> int:
        with self._lock:
            return self._wire_bytes

    def accept_control_line(
        self,
        byte_count: int,
        *,
        reply_bytes: int,
        reply_lines: int,
    ) -> None:
        if byte_count < 0 or reply_bytes < byte_count or reply_lines <= 0:
            raise ValueError("FTP control accounting values are invalid")
        with self._lock:
            self._control_bytes += byte_count
            self._control_lines += 1
            if reply_bytes > self.maximum_reply_bytes:
                raise FtpControlLimitError("reply_bytes")
            if reply_lines > self.maximum_reply_lines:
                raise FtpControlLimitError("reply_lines")
            if self._control_bytes > self.maximum_operation_bytes:
                raise FtpControlLimitError("operation_bytes")
            if self._control_lines > self.maximum_operation_lines:
                raise FtpControlLimitError("operation_lines")
            if (
                self.maximum_total_bytes is not None
                and self._accounted_bytes_unlocked() > self.maximum_total_bytes
            ):
                raise FtpControlLimitError("total_bytes")

    def consume_data_bytes(self, byte_count: int) -> None:
        if byte_count < 0:
            raise ValueError("FTP data accounting cannot be negative")
        with self._lock:
            self._data_bytes += byte_count
            if (
                self.maximum_total_bytes is not None
                and self._accounted_bytes_unlocked() > self.maximum_total_bytes
            ):
                raise FtpControlLimitError("total_bytes")

    def record_wire_bytes(self, byte_count: int) -> None:
        if byte_count < 0:
            raise ValueError("FTP wire accounting cannot be negative")
        with self._lock:
            self._wire_bytes += byte_count
            if (
                self.maximum_total_bytes is not None
                and self._accounted_bytes_unlocked() > self.maximum_total_bytes
            ):
                raise FtpControlLimitError("total_bytes")

    def _accounted_bytes_unlocked(self) -> int:
        return max(self._wire_bytes, self._control_bytes + self._data_bytes)


def install_bounded_ftp_control_reader(
    client: ftplib.FTP,
    budget: FtpControlBudget,
) -> None:
    """Replace ``getmultiline`` before ``connect`` reads the welcome reply."""

    def getmultiline(bound_client: ftplib.FTP) -> str:
        return _get_bounded_multiline(bound_client, budget)

    client.getmultiline = MethodType(getmultiline, client)  # type: ignore[method-assign]


def _get_bounded_multiline(client: ftplib.FTP, budget: FtpControlBudget) -> str:
    lines: list[str] = []
    reply_bytes = 0
    reply_lines = 0
    code: str | None = None
    while True:
        raw_line, byte_count = _read_control_line(client)
        reply_bytes += byte_count
        reply_lines += 1
        budget.accept_control_line(
            byte_count,
            reply_bytes=reply_bytes,
            reply_lines=reply_lines,
        )
        if len(raw_line) > client.maxline:
            raise ftplib.Error(  # noqa: S321 - preserve stdlib FTP line rejection
                f"got more than {client.maxline} bytes"
            )
        line = _strip_control_line(raw_line)
        lines.append(line)
        if code is None:
            if line[3:4] != "-":
                break
            code = line[:3]
        elif line[:3] == code and line[3:4] != "-":
            break
    return "\n".join(lines)


def _read_control_line(client: ftplib.FTP) -> tuple[str, int]:
    file = cast(_ControlFile | None, client.file)
    if file is None:
        raise EOFError
    line = file.readline(client.maxline + 1)
    if not line:
        raise EOFError
    return line, _encoded_control_bytes(client, line)


def _strip_control_line(line: str) -> str:
    if line[-2:] == ftplib.CRLF:
        line = line[:-2]
    elif line[-1:] in ftplib.CRLF:
        line = line[:-1]
    return line


def _encoded_control_bytes(client: ftplib.FTP, line: str) -> int:
    encoded_bytes = len(line.encode(client.encoding, errors="strict"))
    # ``socket.makefile`` may normalize a standards-compliant CRLF to LF.
    # Charge the missing carriage return conservatively; malformed bare-LF
    # replies can only be overcharged, never used to bypass a byte boundary.
    if line.endswith("\n") and not line.endswith("\r\n"):
        encoded_bytes += 1
    return encoded_bytes


class _MeteredTlsSocket(ssl.SSLSocket):
    """Present an SSLSocket while pumping ciphertext across a raw socket."""

    def __new__(cls, *args: object, **kwargs: object) -> _MeteredTlsSocket:
        return ssl.SSLSocket.__new__(cls)

    def __init__(
        self,
        raw: socket.socket,
        context: ssl.SSLContext,
        budget: FtpControlBudget,
        *,
        server_side: bool,
        do_handshake_on_connect: bool,
        suppress_ragged_eofs: bool,
        server_hostname: str | None,
        session: ssl.SSLSession | None,
    ) -> None:
        # Do not call SSLSocket.__init__: the public constructor is blocked
        # and the fd is owned by MemoryBIO pumping rather than OpenSSL.
        incoming = ssl.MemoryBIO()
        outgoing = ssl.MemoryBIO()
        self._raw = raw
        self._budget = budget
        self._incoming = incoming
        self._outgoing = outgoing
        self._ssl_object = context.wrap_bio(
            incoming,
            outgoing,
            server_side=server_side,
            server_hostname=server_hostname,
            session=session,
        )
        self._closed = False
        self.server_side = server_side
        self.server_hostname = server_hostname
        self.suppress_ragged_eofs = suppress_ragged_eofs
        self._context = context
        if do_handshake_on_connect:
            self.do_handshake()

    @property
    def context(self) -> ssl.SSLContext:
        return self._context

    @context.setter
    def context(self, value: ssl.SSLContext) -> None:
        self._context = value

    def do_handshake(self, block: bool = False) -> None:
        del block
        self._run_ssl(self._ssl_object.do_handshake)

    def recv(self, buflen: int = 1024, flags: int = 0) -> bytes:
        if flags:
            raise ValueError("metered FTPS sockets do not support recv flags")
        if buflen <= 0:
            return b""
        try:
            return self._run_ssl(lambda: self._ssl_object.read(buflen))
        except ssl.SSLZeroReturnError:
            return b""
        except ssl.SSLError:
            if self.suppress_ragged_eofs:
                return b""
            raise

    def recv_into(
        self,
        buffer: Buffer,
        nbytes: int | None = None,
        flags: int = 0,
    ) -> int:
        if flags:
            raise ValueError("metered FTPS sockets do not support recv flags")
        writable = memoryview(buffer)
        requested = len(writable) if not nbytes else min(len(writable), nbytes)
        data = self.recv(requested)
        writable[: len(data)] = data
        return len(data)

    def send(self, data: Buffer, flags: int = 0) -> int:
        if flags:
            raise ValueError("metered FTPS sockets do not support send flags")
        payload = bytes(data)
        if not payload:
            return 0
        return self._run_ssl(lambda: self._ssl_object.write(payload))

    def sendall(self, data: Buffer, flags: int = 0) -> None:
        view = memoryview(data)
        offset = 0
        while offset < len(view):
            sent = self.send(view[offset:], flags)
            if sent <= 0:
                raise SourceAccessError(
                    "FTPS socket closed before the complete write was charged",
                    transferred_bytes=self._budget.total_bytes,
                )
            offset += sent

    def makefile(  # type: ignore[override]
        self,
        mode: str = "r",
        buffering: int | None = None,
        *,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> io.TextIOBase | io.BufferedIOBase | io.RawIOBase:
        raw = _MeteredSocketFile(self)
        if buffering == 0:
            if "b" not in mode:
                raise ValueError("unbuffered FTPS makefile requires binary mode")
            return raw
        buffer_size = io.DEFAULT_BUFFER_SIZE if not buffering or buffering < 0 else buffering
        buffered = io.BufferedReader(raw, buffer_size)
        if "b" in mode:
            return buffered
        return io.TextIOWrapper(buffered, encoding=encoding, errors=errors, newline=newline)

    def fileno(self) -> int:
        return self._raw.fileno()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with suppress(OSError):
            self._raw.close()

    def shutdown(self, how: int) -> None:
        self._raw.shutdown(how)

    def unwrap(self) -> socket.socket:
        self._run_ssl(self._ssl_object.unwrap)
        return self._raw

    def gettimeout(self) -> float | None:
        return self._raw.gettimeout()

    def settimeout(self, value: float | None) -> None:
        self._raw.settimeout(value)

    def setblocking(self, flag: bool) -> None:
        self._raw.setblocking(flag)

    def getpeername(self) -> object:
        return self._raw.getpeername()

    def getsockname(self) -> object:
        return self._raw.getsockname()

    def _run_ssl[Result](self, operation: Callable[[], Result]) -> Result:
        while True:
            try:
                result = operation()
            except ssl.SSLWantReadError:
                self._flush_outgoing()
                self._pull_incoming()
                continue
            except ssl.SSLWantWriteError:
                self._flush_outgoing()
                continue
            self._flush_outgoing()
            return result

    def _flush_outgoing(self) -> None:
        pending = self._outgoing.read()
        if not pending:
            return
        view = memoryview(pending)
        offset = 0
        while offset < len(view):
            sent = self._raw.send(view[offset:])
            if sent <= 0:
                raise SourceAccessError(
                    "FTPS socket closed before ciphertext could be charged",
                    transferred_bytes=self._budget.total_bytes,
                )
            self._budget.record_wire_bytes(sent)
            offset += sent

    def _pull_incoming(self) -> None:
        data = self._raw.recv(MAXIMUM_TRANSFER_CHUNK_BYTES)
        if not data:
            self._incoming.write_eof()
            return
        self._budget.record_wire_bytes(len(data))
        self._incoming.write(data)


class _MeteredSocketFile(io.RawIOBase):
    def __init__(self, sock: _MeteredTlsSocket) -> None:
        super().__init__()
        self._sock = sock

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Buffer) -> int:
        writable = memoryview(buffer)
        data = self._sock.recv(len(writable))
        writable[: len(data)] = data
        return len(data)


def install_metered_ftp_tls_context(client: ftplib.FTP_TLS, budget: FtpControlBudget) -> None:
    """Charge TLS ciphertext by wrapping sockets with MemoryBIO before handshake."""

    context = client.context
    if context is None:
        raise SourceAccessError("FTPS client is missing a TLS context")

    def wrap_socket(
        sock: socket.socket,
        server_side: bool = False,
        do_handshake_on_connect: bool = True,
        suppress_ragged_eofs: bool = True,
        server_hostname: str | bytes | None = None,
        session: ssl.SSLSession | None = None,
    ) -> ssl.SSLSocket:
        if not isinstance(sock, socket.socket):
            raise SourceAccessError(
                "FTPS TLS transport cannot enforce raw-byte accounting",
                transferred_bytes=budget.total_bytes,
            )
        hostname = (
            server_hostname.decode() if isinstance(server_hostname, bytes) else server_hostname
        )
        return _MeteredTlsSocket(
            sock,
            context,
            budget,
            server_side=server_side,
            do_handshake_on_connect=do_handshake_on_connect,
            suppress_ragged_eofs=suppress_ragged_eofs,
            server_hostname=hostname,
            session=session,
        )

    context.wrap_socket = wrap_socket  # type: ignore[method-assign]


__all__ = [
    "FtpControlBudget",
    "FtpControlLimitError",
    "install_bounded_ftp_control_reader",
    "install_metered_ftp_tls_context",
]
