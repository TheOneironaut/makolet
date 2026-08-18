from __future__ import annotations

from datetime import UTC, datetime

import pytest

from makolet.application.models import StageSummary
from makolet.domain.enums import CompressionFormat, DocumentType, SourceProtocol
from makolet.domain.errors import DomainValidationError
from makolet.domain.models import (
    MAXIMUM_DOWNLOAD_URL_CHARACTERS,
    MAXIMUM_REMOTE_ID_CHARACTERS,
    ArchiveReceipt,
    RemoteFile,
)


def _remote_file(
    *,
    remote_id: str = "fixture:prices.xml",
    download_url: str = "fixture:///prices.xml",
    original_filename: str = "prices.xml",
    protocol: SourceProtocol = SourceProtocol.FIXTURE,
) -> RemoteFile:
    return RemoteFile(
        retailer_id="demo",
        portal_id="fixture",
        protocol=protocol,
        remote_id=remote_id,
        download_url=download_url,
        original_filename=original_filename,
        document_type=DocumentType.PRICE_FULL,
        compression=CompressionFormat.NONE,
        discovered_at=datetime.now(UTC),
    )


def test_remote_file_requires_aware_discovery_time() -> None:
    with pytest.raises(DomainValidationError, match="timezone"):
        RemoteFile(
            retailer_id="demo",
            portal_id="fixture",
            protocol=SourceProtocol.FIXTURE,
            remote_id="fixture:prices.xml",
            download_url="fixture:///prices.xml",
            original_filename="prices.xml",
            document_type=DocumentType.PRICE_FULL,
            compression=CompressionFormat.NONE,
            discovered_at=datetime(2026, 8, 11),  # noqa: DTZ001 - intentionally naive
        )


def test_remote_file_rejects_negative_content_length() -> None:
    with pytest.raises(DomainValidationError, match="cannot be negative"):
        RemoteFile(
            retailer_id="demo",
            portal_id="fixture",
            protocol=SourceProtocol.FIXTURE,
            remote_id="fixture:prices.xml",
            download_url="fixture:///prices.xml",
            original_filename="prices.xml",
            document_type=DocumentType.PRICE_FULL,
            compression=CompressionFormat.NONE,
            discovered_at=datetime.now(UTC),
            content_length=-1,
        )


@pytest.mark.parametrize(
    "unsafe",
    [
        "bad\x7f.xml",
        "bad\u009b.xml",
        "bad\ud800.xml",
        "bad\u2028.xml",
        "bad\u2029.xml",
        "bad\u202e.xml",
    ],
)
def test_remote_file_rejects_unsafe_display_filename(unsafe: str) -> None:
    with pytest.raises(DomainValidationError, match="unsafe Unicode controls"):
        _remote_file(original_filename=unsafe)


@pytest.mark.parametrize(
    "unsafe",
    ["../prices.xml", r"..\prices.xml", "/prices.xml", "prices%2Exml"],
)
def test_remote_file_requires_a_decoded_basename(unsafe: str) -> None:
    with pytest.raises(DomainValidationError, match="decoded basename"):
        _remote_file(original_filename=unsafe)


def test_remote_file_bounds_stable_remote_identity() -> None:
    assert len(_remote_file(remote_id="r" * MAXIMUM_REMOTE_ID_CHARACTERS).remote_id) == 4_096
    with pytest.raises(DomainValidationError, match="remote_id exceeds 4096"):
        _remote_file(remote_id="r" * (MAXIMUM_REMOTE_ID_CHARACTERS + 1))


@pytest.mark.parametrize(
    ("protocol", "download_url", "message"),
    [
        (SourceProtocol.HTTPS, "http://files.example/prices.xml", "scheme"),
        (
            SourceProtocol.HTTPS,
            "https://user:secret@files.example/prices.xml",
            "credentials",
        ),
        (SourceProtocol.HTTPS, "https:///prices.xml", "valid host"),
        (SourceProtocol.HTTPS, "https://[invalid/prices.xml", "malformed"),
        (SourceProtocol.HTTPS, "https://files.example:not-a-port/prices.xml", "malformed"),
        (
            SourceProtocol.HTTPS,
            "https://files.example/prices.xml\u2028forged",
            "unsafe Unicode controls",
        ),
        (
            SourceProtocol.HTTPS,
            "https://files.example/prices.xml\u202eforged",
            "unsafe Unicode controls",
        ),
    ],
)
def test_remote_file_rejects_unsafe_or_inconsistent_download_url(
    protocol: SourceProtocol,
    download_url: str,
    message: str,
) -> None:
    with pytest.raises(DomainValidationError, match=message):
        _remote_file(protocol=protocol, download_url=download_url)


def test_remote_file_bounds_download_url() -> None:
    prefix = "https://files.example/"
    accepted = prefix + "x" * (MAXIMUM_DOWNLOAD_URL_CHARACTERS - len(prefix))
    assert len(_remote_file(protocol=SourceProtocol.HTTPS, download_url=accepted).download_url) == (
        MAXIMUM_DOWNLOAD_URL_CHARACTERS
    )
    with pytest.raises(DomainValidationError, match="download_url exceeds 8192"):
        _remote_file(protocol=SourceProtocol.HTTPS, download_url=accepted + "x")


def test_fixture_protocol_accepts_a_non_network_test_locator() -> None:
    remote = _remote_file(
        protocol=SourceProtocol.FIXTURE,
        download_url="https://fixtures.invalid/prices.xml",
    )

    assert remote.download_url == "https://fixtures.invalid/prices.xml"


def test_archive_receipt_validates_sha256_shape() -> None:
    with pytest.raises(DomainValidationError, match="lowercase SHA-256"):
        ArchiveReceipt(
            content_sha256="ABC",
            object_key="sha256/ab/ABC",
            content_length=3,
            archived_at=datetime.now(UTC),
            created=True,
        )


def test_archive_receipt_accepts_valid_empty_object_hash() -> None:
    receipt = ArchiveReceipt(
        content_sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        object_key="sha256/e3/b0/e3b0c44298fc",
        content_length=0,
        archived_at=datetime.now(UTC),
        created=False,
    )

    assert receipt.content_length == 0


def test_stage_summary_counts_only_domain_records_as_accepted() -> None:
    summary = StageSummary(
        metadata_records=1,
        store_records=2,
        price_records=3,
        promotion_records=4,
        warnings=5,
        rejected_records=6,
    )

    assert summary.accepted_records == 9
