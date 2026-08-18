"""Protocol routing at the download port boundary."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager

from makolet.application.ports import Downloader, DownloadSession
from makolet.domain.enums import SourceProtocol
from makolet.domain.errors import UnsafeRemoteError
from makolet.domain.models import RemoteFile


class ProtocolDownloader:
    """Route only explicitly supported source protocols to bounded transports."""

    def __init__(self, http: Downloader, ftp: Downloader) -> None:
        self._http = http
        self._ftp = ftp

    def open(
        self,
        remote_file: RemoteFile,
        *,
        maximum_bytes: int | None = None,
    ) -> AbstractAsyncContextManager[DownloadSession]:
        if remote_file.protocol in {SourceProtocol.HTTP, SourceProtocol.HTTPS}:
            return self._http.open(remote_file, maximum_bytes=maximum_bytes)
        if remote_file.protocol in {SourceProtocol.FTP, SourceProtocol.FTPS}:
            return self._ftp.open(remote_file, maximum_bytes=maximum_bytes)
        raise UnsafeRemoteError("No downloader exists for the discovered source protocol")


__all__ = ["ProtocolDownloader"]
