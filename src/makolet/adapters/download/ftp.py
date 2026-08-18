"""Bounded exact-byte FTP and explicit-FTPS downloads for NCR feeds."""

from __future__ import annotations

import ftplib
import ipaddress
import os
import socket
import ssl
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Protocol, cast
from urllib.parse import unquote, urlsplit

import anyio

from makolet.adapters.download._blocking import (
    BlockingOperationCancellation,
    run_bounded_blocking,
)
from makolet.adapters.filesystem_capacity import (
    FileSystemCapacityGuard,
    FileSystemCapacityUnavailableError,
)
from makolet.adapters.ftp_control import (
    FtpControlBudget,
    FtpControlLimitError,
    install_bounded_ftp_control_reader,
    install_metered_ftp_tls_context,
)
from makolet.adapters.sources.ncr import CredentialProvider, NcrFeedConfig
from makolet.application.models import DownloadEvidence
from makolet.application.ports import (
    MAXIMUM_FTP_RESOLVED_ADDRESSES,
    MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES,
    MAXIMUM_TRANSFER_CHUNK_BYTES,
    Clock,
    DownloadSession,
)
from makolet.domain.enums import SourceProtocol
from makolet.domain.errors import (
    ArchiveCapacityError,
    DownloadLimitError,
    SourceAccessError,
    SourceBlockedError,
    SourceResponseError,
    UnsafeRemoteError,
)
from makolet.domain.filenames import safe_basename
from makolet.domain.models import RemoteFile

type FtpHostResolver = Callable[[str, int], Awaitable[tuple[str, ...]]]


class _PassiveAddressPolicy(Protocol):
    trust_server_pasv_ipv4_address: bool


class FtpDownloader:
    """Download only files belonging to registry-declared public NCR feeds."""

    def __init__(
        self,
        feeds: Mapping[str, NcrFeedConfig],
        credentials: CredentialProvider,
        clock: Clock,
        *,
        maximum_response_bytes: int = 2 * 1024 * 1024 * 1024,
        maximum_download_seconds: float = 10 * 60,
        chunk_size: int = 64 * 1024,
        temporary_directory: Path | None = None,
        minimum_free_bytes: int = 0,
        resolver: FtpHostResolver | None = None,
        allow_insecure_ftp: bool = False,
    ) -> None:
        if (
            maximum_response_bytes <= 0
            or maximum_download_seconds <= 0
            or not 1 <= chunk_size <= MAXIMUM_TRANSFER_CHUNK_BYTES
            or minimum_free_bytes < 0
        ):
            raise ValueError("FTP download limits must be positive")
        self._feeds = dict(feeds)
        self._credentials = credentials
        self._clock = clock
        self._maximum_response_bytes = maximum_response_bytes
        self._maximum_download_seconds = maximum_download_seconds
        self._chunk_size = chunk_size
        self._temporary_directory = (
            temporary_directory.resolve()
            if temporary_directory is not None
            else Path(tempfile.gettempdir()).resolve()
        )
        self._capacity = FileSystemCapacityGuard(
            self._temporary_directory,
            minimum_free_bytes=minimum_free_bytes,
        )
        self._resolver = resolver or _resolve_public_addresses
        self._allow_insecure_ftp = allow_insecure_ftp

    def open(
        self,
        remote_file: RemoteFile,
        *,
        maximum_bytes: int | None = None,
    ) -> AbstractAsyncContextManager[DownloadSession]:
        return self._open(remote_file, maximum_bytes=maximum_bytes)

    @asynccontextmanager
    async def _open(
        self,
        remote_file: RemoteFile,
        *,
        maximum_bytes: int | None,
    ) -> AsyncIterator[DownloadSession]:
        feed, filename = self._validated_target(remote_file)
        if maximum_bytes is not None and maximum_bytes <= 0:
            raise ValueError("maximum_bytes must be positive when supplied")
        maximum_payload_bytes = (
            self._maximum_response_bytes
            if maximum_bytes is None
            else min(self._maximum_response_bytes, maximum_bytes)
        )
        payload_budget_limited = (
            maximum_bytes is not None and maximum_bytes < self._maximum_response_bytes
        )
        maximum_total_bytes = (
            self._maximum_response_bytes + MAXIMUM_FTP_TRANSFER_PROTOCOL_OVERHEAD_BYTES
            if maximum_bytes is None
            else maximum_bytes
        )
        transfer_budget = FtpControlBudget(maximum_total_bytes=maximum_total_bytes)
        if feed.protocol is SourceProtocol.FTP and not self._allow_insecure_ftp:
            raise SourceBlockedError("Plain FTP transport requires explicit operator opt-in")
        started_at = self._clock.now()
        deadline = anyio.current_time() + self._maximum_download_seconds
        try:
            with anyio.fail_after(_remaining_seconds(deadline)):
                addresses = await _validated_public_addresses(feed.host, self._resolver)
        except TimeoutError as error:
            raise SourceAccessError(
                "Public FTP file download timed out",
                transferred_bytes=0,
            ) from error
        temporary_path = await anyio.to_thread.run_sync(self._temporary_path)
        cancellation = BlockingOperationCancellation()
        try:
            result, timed_out = await run_bounded_blocking(
                lambda: self._download_sync(
                    feed,
                    filename,
                    temporary_path,
                    addresses,
                    cancellation,
                    maximum_payload_bytes,
                    payload_budget_limited,
                    maximum_bytes is not None,
                    transfer_budget,
                ),
                cancellation.cancel,
                timeout_seconds=_remaining_seconds(deadline),
            )
            if timed_out:
                raise SourceAccessError(
                    "Public FTP file download timed out",
                    transferred_bytes=transfer_budget.total_bytes,
                )
            if result is None:
                raise SourceAccessError("Public FTP file download returned no result")
            content_length, transferred_bytes = result
            session = _FtpDownloadSession(
                temporary_path,
                content_length,
                transferred_bytes,
                remote_file,
                self._clock,
                started_at,
                chunk_size=self._chunk_size,
            )
            yield session
        finally:
            with anyio.CancelScope(shield=True):
                with suppress(FileNotFoundError):
                    await anyio.Path(temporary_path).unlink()

    def _validated_target(self, remote_file: RemoteFile) -> tuple[NcrFeedConfig, str]:
        if remote_file.protocol not in {SourceProtocol.FTP, SourceProtocol.FTPS}:
            raise UnsafeRemoteError("FTP downloader received a non-FTP source")
        try:
            feed = self._feeds[remote_file.portal_id]
        except KeyError as error:
            raise UnsafeRemoteError("No FTP feed policy exists for this portal") from error
        parsed = urlsplit(remote_file.download_url)
        if (
            parsed.scheme.casefold() != feed.protocol.value
            or (parsed.hostname or "").casefold().rstrip(".") != feed.host.casefold().rstrip(".")
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.port not in {None, 21}
        ):
            raise UnsafeRemoteError("FTP source URL differs from its configured feed")
        filename = safe_basename(unquote(PurePosixPath(parsed.path).name))
        if filename != remote_file.original_filename:
            raise UnsafeRemoteError("FTP source filename differs from discovered metadata")
        directory = (
            "/" + feed.remote_directory.strip("/") if feed.remote_directory.strip("/") else ""
        )
        expected_path = f"{directory}/{filename}"
        if unquote(parsed.path) != expected_path:
            raise UnsafeRemoteError("FTP source path differs from its configured directory")
        return feed, filename

    def _download_sync(
        self,
        feed: NcrFeedConfig,
        filename: str,
        path: Path,
        addresses: tuple[str, ...],
        cancellation: BlockingOperationCancellation,
        maximum_payload_bytes: int,
        payload_budget_limited: bool,
        total_budget_limited: bool,
        transfer_budget: FtpControlBudget,
    ) -> tuple[int, int]:
        credentials = self._credentials.get(feed.credential_key)
        client: ftplib.FTP | None = None
        content_length = 0
        try:
            client = _connect_pinned(feed, addresses, cancellation, transfer_budget)
            cancellation.checkpoint()
            client.login(credentials.username, credentials.password)
            if isinstance(client, ftplib.FTP_TLS):
                client.prot_p()
            # Never follow an address supplied by PASV; use the vetted control peer.
            cast(_PassiveAddressPolicy, client).trust_server_pasv_ipv4_address = False
            client.set_pasv(feed.passive)
            client.cwd(feed.remote_directory)
            with path.open("xb") as handle:

                def write_chunk(chunk: bytes) -> None:
                    nonlocal content_length
                    cancellation.checkpoint()
                    content_length += len(chunk)
                    try:
                        transfer_budget.consume_data_bytes(len(chunk))
                    except FtpControlLimitError:
                        if content_length > maximum_payload_bytes:
                            _raise_response_limit(
                                maximum_payload_bytes,
                                transfer_budget.total_bytes,
                                budget_limited=payload_budget_limited,
                            )
                        raise
                    if content_length > maximum_payload_bytes:
                        _raise_response_limit(
                            maximum_payload_bytes,
                            transfer_budget.total_bytes,
                            budget_limited=payload_budget_limited,
                        )
                    try:
                        with self._capacity.reserve(len(chunk)):
                            handle.write(chunk)
                            handle.flush()
                    except FileSystemCapacityUnavailableError as error:
                        raise ArchiveCapacityError(
                            "FTP download spool reached its configured free-space reserve",
                            transferred_bytes=transfer_budget.total_bytes,
                        ) from error

                _retrieve_ftp_binary(
                    client,
                    f"RETR {filename}",
                    write_chunk,
                    block_size=self._chunk_size,
                    cancellation=cancellation,
                )
                cancellation.checkpoint()
                try:
                    with self._capacity.reserve(0):
                        handle.flush()
                        os.fsync(handle.fileno())
                except FileSystemCapacityUnavailableError as error:
                    raise ArchiveCapacityError(
                        "FTP download spool reached its configured free-space reserve",
                        transferred_bytes=transfer_budget.total_bytes,
                    ) from error
            client.quit()
        except DownloadLimitError:
            raise
        except FtpControlLimitError as error:
            if error.reason == "total_bytes":
                maximum_total_bytes = transfer_budget.maximum_total_bytes
                if maximum_total_bytes is None:
                    raise RuntimeError("FTP transfer budget has no byte ceiling") from error
                raise DownloadLimitError(
                    f"FTP response exceeds {maximum_total_bytes} bytes",
                    transferred_bytes=transfer_budget.total_bytes,
                    budget_limited=total_budget_limited,
                ) from error
            raise SourceResponseError(
                "FTP control replies exceed their configured safety limits",
                transferred_bytes=transfer_budget.total_bytes,
            ) from error
        except ftplib.error_perm as error:
            raise SourceBlockedError(
                "Public FTP server denied file access",
                transferred_bytes=transfer_budget.total_bytes,
            ) from error
        except ftplib.all_errors as error:
            raise SourceAccessError(
                "Public FTP file download failed",
                transferred_bytes=transfer_budget.total_bytes,
            ) from error
        finally:
            if client is not None:
                client.close()
                cancellation.release(client)
        return content_length, transfer_budget.total_bytes

    def _temporary_path(self) -> Path:
        self._temporary_directory.mkdir(parents=True, exist_ok=True)
        descriptor, value = tempfile.mkstemp(
            prefix="makolet-ftp-",
            suffix=".part",
            dir=str(self._temporary_directory),
        )
        os.close(descriptor)
        path = Path(value)
        path.unlink()
        return path


class _FtpDownloadSession:
    def __init__(
        self,
        path: Path,
        content_length: int,
        transferred_bytes: int,
        remote_file: RemoteFile,
        clock: Clock,
        started_at: datetime,
        *,
        chunk_size: int,
    ) -> None:
        self._path = path
        self._content_length = content_length
        self._transferred_bytes = transferred_bytes
        self._remote_file = remote_file
        self._clock = clock
        self._started_at = started_at
        self._chunk_size = chunk_size
        self._iterated = False

    @property
    def transferred_bytes(self) -> int:
        # The complete payload and its bounded control exchange crossed the
        # network before this session was yielded.
        return self._transferred_bytes

    async def iter_raw(self) -> AsyncIterator[bytes]:
        if self._iterated:
            raise SourceResponseError("An FTP download can only be consumed once")
        self._iterated = True
        iterated_bytes = 0
        handle = await anyio.open_file(self._path, "rb")
        async with handle:
            while chunk := await handle.read(self._chunk_size):
                iterated_bytes += len(chunk)
                yield chunk
        if iterated_bytes != self._content_length:
            raise SourceResponseError(
                "FTP stream length differs from downloaded bytes",
                transferred_bytes=iterated_bytes,
            )
        declared = self._remote_file.content_length
        if declared is not None and declared != iterated_bytes:
            # Validate at iterator EOF so an archive sink sees the failure before
            # it commits the content-addressed object.
            raise SourceResponseError(
                "FTP file length differs from listing metadata",
                transferred_bytes=iterated_bytes,
            )

    async def finish(self, content_length: int) -> DownloadEvidence:
        if not self._iterated:
            raise SourceResponseError("FTP download was not consumed")
        if content_length != self._content_length:
            raise SourceResponseError("Archive length differs from downloaded FTP bytes")
        declared = self._remote_file.content_length
        if declared is not None and declared != content_length:
            raise SourceResponseError("FTP file length differs from listing metadata")
        return DownloadEvidence(
            started_at=self._started_at,
            finished_at=self._clock.now(),
            status_code=None,
            content_length=content_length,
            media_type=self._remote_file.media_type,
            etag=None,
            last_modified=self._remote_file.last_modified,
            response_metadata=(
                (
                    "transport-security",
                    "tls"
                    if self._remote_file.protocol is SourceProtocol.FTPS
                    else "unauthenticated",
                ),
            ),
        )


async def _resolve_public_addresses(host: str, port: int) -> tuple[str, ...]:
    try:
        records = await anyio.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as error:
        raise SourceAccessError("Public FTP hostname could not be resolved") from error
    return tuple(dict.fromkeys(str(record[4][0]) for record in records))


def _raise_response_limit(
    maximum_response_bytes: int,
    transferred_bytes: int,
    *,
    budget_limited: bool,
) -> None:
    raise DownloadLimitError(
        f"FTP response exceeds {maximum_response_bytes} bytes",
        transferred_bytes=transferred_bytes,
        budget_limited=budget_limited,
    )


def _retrieve_ftp_binary(
    client: ftplib.FTP,
    command: str,
    callback: Callable[[bytes], None],
    *,
    block_size: int,
    cancellation: BlockingOperationCancellation,
) -> None:
    """Own the FTP data socket so cancellation can close the active transfer."""

    client.voidcmd("TYPE I")
    connection = client.transfercmd(command)
    if not cancellation.bind(connection):
        raise TimeoutError("Public FTP file download was cancelled")
    try:
        while True:
            cancellation.checkpoint()
            chunk = connection.recv(block_size)
            cancellation.checkpoint()
            if not chunk:
                break
            if not isinstance(chunk, bytes):
                raise SourceResponseError("FTP download returned non-byte data")
            callback(chunk)
    finally:
        cancellation.release(connection)
        connection.close()
    cancellation.checkpoint()
    client.voidresp()


def _remaining_seconds(deadline: float) -> float:
    return max(deadline - anyio.current_time(), 1e-9)


async def _validated_public_addresses(
    host: str,
    resolver: FtpHostResolver,
) -> tuple[str, ...]:
    addresses = await resolver(host, 21)
    if not addresses:
        raise SourceAccessError("Public FTP hostname resolved to no addresses")
    validated: list[str] = []
    for value in addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError as error:
            raise UnsafeRemoteError("FTP resolver returned an invalid address") from error
        if not address.is_global:
            raise UnsafeRemoteError("Public FTP hostname resolved to a non-public address")
        canonical = str(address)
        if canonical in validated:
            continue
        validated.append(canonical)
        if len(validated) > MAXIMUM_FTP_RESOLVED_ADDRESSES:
            raise UnsafeRemoteError("Public FTP hostname resolved to too many addresses")
    return tuple(validated)


def _connect_pinned(
    feed: NcrFeedConfig,
    addresses: tuple[str, ...],
    cancellation: BlockingOperationCancellation,
    control_budget: FtpControlBudget,
) -> ftplib.FTP:
    last_error: BaseException | None = None
    for address in addresses:
        cancellation.checkpoint()
        client: ftplib.FTP
        if feed.protocol is SourceProtocol.FTPS:
            client = ftplib.FTP_TLS(  # noqa: S321 - required publisher protocol
                timeout=feed.timeout_seconds,
                context=ssl.create_default_context(),
            )
            install_metered_ftp_tls_context(client, control_budget)
        else:
            client = ftplib.FTP(timeout=feed.timeout_seconds)  # noqa: S321
        install_bounded_ftp_control_reader(client, control_budget)
        if not cancellation.bind(client):
            raise TimeoutError("Public FTP connection was cancelled")
        try:
            client.connect(address, 21, timeout=feed.timeout_seconds)
        except FtpControlLimitError:
            cancellation.release(client)
            client.close()
            raise
        except ftplib.all_errors as error:
            last_error = error
            cancellation.release(client)
            client.close()
            continue
        client.host = feed.host
        return client
    if last_error is not None:
        raise last_error
    raise SourceAccessError("Public FTP hostname resolved to no usable addresses")


__all__ = ["FtpDownloader"]
