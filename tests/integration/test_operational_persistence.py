"""Real-PostgreSQL contracts for failure inspection and stale-job recovery."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import update

from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.ingestion import PostgresIngestionRepository
from makolet.adapters.persistence.leases import PostgresLeaseManager
from makolet.adapters.persistence.operations import PostgresOperationalRepository
from makolet.adapters.persistence.schema import source_files
from makolet.application.models import ArchivedDownload, DownloadEvidence, RegisteredSourceFile
from makolet.domain.enums import (
    CompressionFormat,
    DocumentType,
    IngestionStatus,
    IssueSeverity,
    SourceProtocol,
)
from makolet.domain.errors import NotFoundError
from makolet.domain.models import ArchiveReceipt, RemoteFile, ValidationIssue

pytestmark = pytest.mark.integration
_OBSERVED_AT = datetime(2026, 8, 12, 1, 0, tzinfo=UTC)


async def _register(
    repository: PostgresIngestionRepository,
    remote_id: str,
) -> tuple[RemoteFile, RegisteredSourceFile]:
    remote = RemoteFile(
        retailer_id="operational-test-retailer",
        portal_id="operational-test-portal",
        protocol=SourceProtocol.FIXTURE,
        remote_id=remote_id,
        download_url=f"https://fixtures.invalid/{remote_id}",
        original_filename=f"{remote_id}.xml",
        document_type=DocumentType.PRICE_DELTA,
        compression=CompressionFormat.NONE,
        discovered_at=_OBSERVED_AT,
        source_timestamp=_OBSERVED_AT,
    )
    registered = await repository.register_discovery(remote)
    return remote, registered


async def test_failure_and_quarantine_views_are_bounded_and_auditable(
    database: Database,
) -> None:
    ingestion = PostgresIngestionRepository(database.engine)
    operations = PostgresOperationalRepository(
        database.engine,
        ingestion,
        PostgresLeaseManager(database),
    )

    failed_ids = []
    for remote_id in ("failed-one", "failed-two"):
        _remote, registered = await _register(ingestion, remote_id)
        failed_ids.append(registered.source_file_id)
        await ingestion.transition(
            registered.source_file_id,
            (IngestionStatus.DISCOVERED,),
            IngestionStatus.FAILED_TERMINAL,
            error_code="fixture_failure",
            error_message=f"failure for {remote_id}",
        )

    first_page = await operations.failures(limit=1, cursor=None)
    assert len(first_page.items) == 1
    assert first_page.next_cursor is not None
    second_page = await operations.failures(limit=1, cursor=first_page.next_cursor)
    assert len(second_page.items) == 1
    assert second_page.next_cursor is None
    failure_items = (*first_page.items, *second_page.items)
    assert {item["id"] for item in failure_items} == set(failed_ids)
    assert all(item["error_code"] == "fixture_failure" for item in failure_items)

    _remote, quarantined = await _register(ingestion, "quarantined-one")
    await ingestion.transition(
        quarantined.source_file_id,
        (IngestionStatus.DISCOVERED,),
        IngestionStatus.DOWNLOADING,
    )
    payload = b"independently-authored-quarantine-fixture"
    digest = hashlib.sha256(payload).hexdigest()
    duplicate = await ingestion.record_archive(
        quarantined.source_file_id,
        ArchivedDownload(
            archive=ArchiveReceipt(
                content_sha256=digest,
                object_key=f"sha256/{digest}",
                content_length=len(payload),
                archived_at=_OBSERVED_AT,
                created=True,
            ),
            evidence=DownloadEvidence(
                started_at=_OBSERVED_AT,
                finished_at=_OBSERVED_AT + timedelta(seconds=1),
                status_code=200,
                content_length=len(payload),
                media_type="application/xml",
                etag=None,
                last_modified=None,
            ),
        ),
        parser_version="operational-test-parser",
    )
    assert duplicate is False
    await ingestion.transition(
        quarantined.source_file_id,
        (IngestionStatus.ARCHIVED,),
        IngestionStatus.PARSING,
    )
    await ingestion.stage(
        quarantined.source_file_id,
        (
            ValidationIssue(
                source_file_id=quarantined.source_file_id,
                severity=IssueSeverity.FILE_QUARANTINE,
                code="hostile_fixture",
                message="Fixture intentionally exercises quarantine inspection",
                record_index=7,
                field_name="document",
                rejected_value="bounded evidence",
            ),
        ),
    )
    await ingestion.transition(
        quarantined.source_file_id,
        (IngestionStatus.PARSING,),
        IngestionStatus.QUARANTINED,
        error_code="hostile_fixture",
        error_message="Fixture was quarantined",
    )

    page = await operations.list_quarantine(limit=10, cursor=None)
    assert [item["id"] for item in page.items] == [quarantined.source_file_id]
    assert page.items[0]["issue_count"] == 1
    inspected = await operations.inspect_quarantine(quarantined.source_file_id)
    assert inspected["object_key"] == f"sha256/{digest}"
    assert inspected["issues_truncated"] is False
    issues = inspected["issues"]
    assert isinstance(issues, tuple)
    first_issue = issues[0]
    assert isinstance(first_issue, dict)
    assert first_issue["severity"] == IssueSeverity.FILE_QUARANTINE.value
    assert first_issue["code"] == "hostile_fixture"

    with pytest.raises(NotFoundError, match="not found"):
        await operations.inspect_quarantine(uuid4())
    with pytest.raises(ValueError, match="between 1 and 200"):
        await operations.failures(limit=0, cursor=None)
    with pytest.raises(ValueError, match="valid UUID"):
        await operations.list_quarantine(limit=10, cursor="not-a-uuid")
    with pytest.raises(ValueError, match="too long"):
        await operations.failures(limit=10, cursor="x" * 65)


async def test_stale_recovery_changes_only_abandoned_active_jobs(database: Database) -> None:
    ingestion = PostgresIngestionRepository(database.engine)
    operations = PostgresOperationalRepository(
        database.engine,
        ingestion,
        PostgresLeaseManager(database),
    )
    _remote, stale = await _register(ingestion, "stale-download")
    _remote, fresh = await _register(ingestion, "fresh-download")
    for registered in (stale, fresh):
        await ingestion.transition(
            registered.source_file_id,
            (IngestionStatus.DISCOVERED,),
            IngestionStatus.DOWNLOADING,
        )

    async with database.engine.begin() as connection:
        await connection.execute(
            update(source_files)
            .where(source_files.c.id == stale.source_file_id)
            .values(updated_at=_OBSERVED_AT - timedelta(days=1))
        )

    assert await operations.recover_stale_jobs(stale_after=timedelta(hours=1)) == 1
    assert (await ingestion.get(stale.source_file_id)).status is IngestionStatus.FAILED_RETRYABLE
    assert (await ingestion.get(fresh.source_file_id)).status is IngestionStatus.DOWNLOADING


async def test_stale_recovery_cannot_take_over_a_session_locked_file(database: Database) -> None:
    ingestion = PostgresIngestionRepository(database.engine)
    leases = PostgresLeaseManager(database)
    operations = PostgresOperationalRepository(database.engine, ingestion, leases)
    _remote, active = await _register(ingestion, "session-locked-download")
    await ingestion.transition(
        active.source_file_id,
        (IngestionStatus.DISCOVERED,),
        IngestionStatus.DOWNLOADING,
    )
    async with database.engine.begin() as connection:
        await connection.execute(
            update(source_files)
            .where(source_files.c.id == active.source_file_id)
            .values(updated_at=_OBSERVED_AT - timedelta(days=1))
        )

    async with leases.acquire(
        f"source-file:{active.source_file_id}",
        "active-worker",
        timedelta(minutes=1),
    ) as acquired:
        assert acquired
        assert await operations.recover_stale_jobs(stale_after=timedelta(hours=1)) == 0
        assert (await ingestion.get(active.source_file_id)).status is IngestionStatus.DOWNLOADING

    assert await operations.recover_stale_jobs(stale_after=timedelta(hours=1)) == 1
    assert (await ingestion.get(active.source_file_id)).status is IngestionStatus.FAILED_RETRYABLE
    assert await operations.recover_stale_jobs(stale_after=timedelta(hours=1)) == 0

    with pytest.raises(ValueError, match="positive"):
        await operations.recover_stale_jobs(stale_after=timedelta(0))
    with pytest.raises(ValueError, match="seven days"):
        await operations.recover_stale_jobs(stale_after=timedelta(days=8))
