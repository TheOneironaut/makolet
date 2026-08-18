"""Portal-wide full Stores roster reconciliation contracts."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import select

from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.errors import SnapshotValidationError
from makolet.adapters.persistence.ingestion import PostgresIngestionRepository
from makolet.adapters.persistence.schema import stores
from makolet.application.models import ArchivedDownload, DownloadEvidence
from makolet.domain.enums import (
    CompressionFormat,
    DocumentType,
    IngestionStatus,
    SourceProtocol,
)
from makolet.domain.models import ArchiveReceipt, DocumentMetadata, RemoteFile, StoreRecord

pytestmark = pytest.mark.integration
NOW = datetime(2026, 8, 12, 10, tzinfo=UTC)


async def _archived_stores_file(
    repository: PostgresIngestionRepository,
    *,
    remote_id: str,
    source_timestamp: datetime,
) -> UUID:
    remote_file = RemoteFile(
        retailer_id="store-roster-retailer",
        portal_id="store-roster-portal",
        protocol=SourceProtocol.FIXTURE,
        remote_id=remote_id,
        download_url=f"https://fixtures.invalid/{remote_id}.xml",
        original_filename=f"{remote_id}.xml",
        document_type=DocumentType.STORES,
        compression=CompressionFormat.NONE,
        discovered_at=source_timestamp,
        source_timestamp=source_timestamp,
    )
    registered = await repository.register_discovery(remote_file)
    await repository.transition(
        registered.source_file_id,
        (IngestionStatus.DISCOVERED,),
        IngestionStatus.DOWNLOADING,
    )
    digest = hashlib.sha256(remote_id.encode()).hexdigest()
    await repository.record_archive(
        registered.source_file_id,
        ArchivedDownload(
            archive=ArchiveReceipt(
                content_sha256=digest,
                object_key=f"sha256/{digest}",
                content_length=len(remote_id),
                archived_at=source_timestamp,
                created=True,
            ),
            evidence=DownloadEvidence(
                started_at=source_timestamp,
                finished_at=source_timestamp + timedelta(milliseconds=1),
                status_code=200,
                content_length=len(remote_id),
                media_type="application/xml",
                etag=None,
                last_modified=source_timestamp,
            ),
        ),
        parser_version="store-roster-v1",
    )
    return registered.source_file_id


def _store(
    source_file_id: UUID,
    *,
    record_index: int,
    subchain: str,
    store_code: str,
) -> StoreRecord:
    return StoreRecord(
        source_file_id=source_file_id,
        record_index=record_index,
        chain_id="chain",
        subchain_id=subchain,
        store_id=store_code,
        audit_number=None,
        store_type="1",
        chain_name="Clean Room Chain",
        subchain_name=f"Subchain {subchain}",
        store_name=f"Store {store_code}",
        address=None,
        city="Test City",
        postal_code=None,
    )


async def _stage_and_apply(
    repository: PostgresIngestionRepository,
    source_file_id: UUID,
    records: tuple[StoreRecord, ...],
    *,
    maximum_drop_fraction: float,
) -> None:
    await repository.transition(
        source_file_id,
        (IngestionStatus.ARCHIVED,),
        IngestionStatus.PARSING,
    )
    await repository.clear_staging(source_file_id)
    await repository.stage(
        source_file_id,
        (
            DocumentMetadata(
                source_file_id=source_file_id,
                document_type=DocumentType.STORES,
                chain_id="chain",
                source_updated_at=(
                    await repository.get(source_file_id)
                ).remote_file.source_timestamp,
            ),
            *records,
        ),
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.PARSING,),
        IngestionStatus.STAGED,
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.STAGED,),
        IngestionStatus.VALIDATING,
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.VALIDATING,),
        IngestionStatus.APPLYING,
    )
    await repository.apply(
        source_file_id,
        DocumentType.STORES,
        minimum_full_records=1,
        maximum_drop_fraction=maximum_drop_fraction,
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.APPLYING,),
        IngestionStatus.COMPLETED,
    )


async def _seed_subchains(
    repository: PostgresIngestionRepository,
    *,
    a_count: int,
    b_count: int,
) -> UUID:
    source_file_id = await _archived_stores_file(
        repository,
        remote_id=f"stores-a{a_count}-b{b_count}",
        source_timestamp=NOW,
    )
    records = tuple(
        _store(
            source_file_id,
            record_index=index,
            subchain=subchain,
            store_code=f"{subchain}-{offset}",
        )
        for index, (subchain, offset) in enumerate(
            (
                *(("A", offset) for offset in range(a_count)),
                *(("B", offset) for offset in range(b_count)),
            ),
            start=1,
        )
    )
    await _stage_and_apply(
        repository,
        source_file_id,
        records,
        maximum_drop_fraction=0.99,
    )
    return source_file_id


async def test_full_stores_snapshot_deactivates_an_omitted_small_subchain(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    await _seed_subchains(repository, a_count=4, b_count=1)
    source_file_id = await _archived_stores_file(
        repository,
        remote_id="stores-a-only-accepted",
        source_timestamp=NOW + timedelta(minutes=1),
    )
    records = tuple(
        _store(
            source_file_id,
            record_index=index,
            subchain="A",
            store_code=f"A-{offset}",
        )
        for index, offset in enumerate(range(4), start=1)
    )

    await _stage_and_apply(
        repository,
        source_file_id,
        records,
        maximum_drop_fraction=0.25,
    )

    async with database.engine.connect() as connection:
        states = (
            await connection.execute(
                select(stores.c.subchain_code, stores.c.is_active).order_by(
                    stores.c.subchain_code, stores.c.source_store_code
                )
            )
        ).all()
    assert [tuple(row) for row in states] == [
        ("A", True),
        ("A", True),
        ("A", True),
        ("A", True),
        ("B", False),
    ]


async def test_full_stores_snapshot_rejects_omitted_large_subchain_atomically(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    original_source_file_id = await _seed_subchains(repository, a_count=1, b_count=3)
    source_file_id = await _archived_stores_file(
        repository,
        remote_id="stores-a-only-rejected",
        source_timestamp=NOW + timedelta(minutes=1),
    )

    with pytest.raises(SnapshotValidationError, match=r"drops 75\.0%"):
        await _stage_and_apply(
            repository,
            source_file_id,
            (
                _store(
                    source_file_id,
                    record_index=1,
                    subchain="A",
                    store_code="A-0",
                ),
            ),
            maximum_drop_fraction=0.50,
        )

    async with database.engine.connect() as connection:
        states = (
            await connection.execute(
                select(
                    stores.c.subchain_code,
                    stores.c.is_active,
                    stores.c.last_source_file_id,
                ).order_by(stores.c.subchain_code, stores.c.source_store_code)
            )
        ).all()
    assert all(row.is_active for row in states)
    assert {row.last_source_file_id for row in states} == {original_source_file_id}
