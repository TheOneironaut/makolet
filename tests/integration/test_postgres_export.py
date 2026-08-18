"""Real-PostgreSQL export range, provenance, and snapshot contracts."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy import insert, update
from sqlalchemy.ext.asyncio import AsyncConnection

from makolet.adapters.archive.keys import key_for_digest
from makolet.adapters.export.models import ExportLimits, ExportValidationError
from makolet.adapters.export.postgres import (
    PostgresParquetExportOperations,
    _EntityExport,
    _PlannedPartition,
)
from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.schema import (
    current_prices,
    portals,
    price_history,
    raw_archive_objects,
    retailer_items,
    retailers,
    source_files,
    stores,
)
from makolet.domain.enums import (
    CompressionFormat,
    DocumentType,
    IngestionStatus,
    SourceProtocol,
)
from tests.unit.export.parquet_reader import rows

pytestmark = pytest.mark.integration
WINDOW_START = datetime(2026, 8, 12, 12, tzinfo=UTC)
WINDOW_END = datetime(2026, 8, 12, 13, tzinfo=UTC)


async def _seed_portal_row(
    database: Database,
    *,
    retailer_id: UUID,
    portal_key: str,
    content_sha256: str,
    price: str,
    observed_at: datetime,
    history_from: datetime,
    history_to: datetime | None,
) -> tuple[UUID, UUID, UUID]:
    portal_id = uuid4()
    archive_id = uuid4()
    source_file_id = uuid4()
    store_id = uuid4()
    item_id = uuid4()
    async with database.engine.begin() as connection:
        await connection.execute(
            insert(portals).values(
                id=portal_id,
                retailer_id=retailer_id,
                source_key=portal_key,
                family="fixture",
                protocol=SourceProtocol.FIXTURE.value,
            )
        )
        await connection.execute(
            insert(raw_archive_objects).values(
                id=archive_id,
                content_sha256=content_sha256,
                object_key=key_for_digest(content_sha256),
                content_length=17,
                archived_at=observed_at,
            )
        )
        await connection.execute(
            insert(source_files).values(
                id=source_file_id,
                retailer_id=retailer_id,
                portal_id=portal_id,
                remote_id=f"{portal_key}-source",
                download_url=f"fixture:///{portal_key}",
                original_filename="PriceFull.xml",
                document_type=DocumentType.PRICE_FULL.value,
                compression=CompressionFormat.NONE.value,
                protocol=SourceProtocol.FIXTURE.value,
                status=IngestionStatus.COMPLETED.value,
                discovered_at=observed_at - timedelta(minutes=2),
                source_timestamp=observed_at - timedelta(minutes=1),
                raw_archive_object_id=archive_id,
                parser_version="export-test/1",
                download_started_at=observed_at - timedelta(seconds=1),
                download_finished_at=observed_at,
                download_status_code=200,
                download_content_length=17,
            )
        )
        await connection.execute(
            insert(stores).values(
                id=store_id,
                retailer_id=retailer_id,
                portal_id=portal_id,
                chain_code="chain",
                subchain_code="subchain",
                source_store_code="shared-store",
                name=f"{portal_key} store",
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                last_source_file_id=source_file_id,
            )
        )
        await connection.execute(
            insert(retailer_items).values(
                id=item_id,
                retailer_id=retailer_id,
                portal_id=portal_id,
                source_item_code="shared-item",
                name=f"{portal_key} coffee",
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                last_source_file_id=source_file_id,
            )
        )
        await connection.execute(
            insert(current_prices).values(
                retailer_item_id=item_id,
                store_id=store_id,
                item_price=Decimal(price),
                source_file_id=source_file_id,
                first_observed_at=observed_at,
                last_observed_at=observed_at,
            )
        )
        await connection.execute(
            insert(price_history).values(
                retailer_item_id=item_id,
                store_id=store_id,
                item_price=Decimal(price),
                source_file_id=source_file_id,
                valid_from=history_from,
                valid_to=history_to,
            )
        )
    return source_file_id, item_id, store_id


async def _seed_export_contract(database: Database) -> dict[str, object]:
    retailer_id = uuid4()
    async with database.engine.begin() as connection:
        await connection.execute(
            insert(retailers).values(
                id=retailer_id,
                source_key="export-retailer",
                display_name="Export Retailer",
            )
        )
    first = await _seed_portal_row(
        database,
        retailer_id=retailer_id,
        portal_key="portal-a",
        content_sha256="a" * 64,
        price="10.00",
        observed_at=WINDOW_START + timedelta(minutes=15),
        history_from=WINDOW_START - timedelta(hours=1),
        history_to=WINDOW_START + timedelta(minutes=15),
    )
    second = await _seed_portal_row(
        database,
        retailer_id=retailer_id,
        portal_key="portal-b",
        content_sha256="b" * 64,
        price="12.00",
        observed_at=WINDOW_START + timedelta(minutes=30),
        history_from=WINDOW_START + timedelta(minutes=30),
        history_to=None,
    )
    await _seed_portal_row(
        database,
        retailer_id=retailer_id,
        portal_key="portal-outside",
        content_sha256="c" * 64,
        price="99.00",
        observed_at=WINDOW_END + timedelta(minutes=1),
        history_from=WINDOW_END + timedelta(minutes=1),
        history_to=None,
    )
    await _seed_portal_row(
        database,
        retailer_id=retailer_id,
        portal_key="portal-boundary-partition",
        content_sha256="d" * 64,
        price="77.00",
        observed_at=WINDOW_END + timedelta(minutes=2),
        history_from=WINDOW_START - timedelta(hours=13),
        history_to=WINDOW_START,
    )
    await _seed_portal_row(
        database,
        retailer_id=retailer_id,
        portal_key="portal-boundary-row",
        content_sha256="e" * 64,
        price="78.00",
        observed_at=WINDOW_END + timedelta(minutes=3),
        history_from=WINDOW_START - timedelta(hours=1),
        history_to=WINDOW_START,
    )
    return {"retailer_id": retailer_id, "first": first, "second": second}


def _entity_rows(result: dict[str, object], entity: str) -> list[dict[str, object | None]]:
    manifests = cast(tuple[dict[str, object], ...], result["manifests"])
    manifest_result = next(item for item in manifests if item["entity"] == entity)
    manifest_path = cast(Path, manifest_result["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return [row for file in manifest["files"] for row in rows(manifest_path.parent / file["path"])]


def _decoded(row: dict[str, object | None], name: str) -> str:
    value = row[name]
    assert isinstance(value, bytes)
    return value.decode("utf-8")


async def test_export_applies_exact_range_and_carries_portal_archive_provenance(
    database: Database,
    tmp_path: Path,
) -> None:
    await _seed_export_contract(database)
    operations = PostgresParquetExportOperations(database.engine)

    first = await operations.export_parquet(
        tmp_path / "export",
        since=WINDOW_START,
        until=WINDOW_END,
    )
    second = await operations.export_parquet(
        tmp_path / "export",
        since=WINDOW_START,
        until=WINDOW_END,
    )

    assert first["row_count"] == 4
    assert first["partition_count"] == 2
    assert first["database_snapshot"]
    assert isinstance(first["snapshot_started_at"], datetime)
    assert [
        item["created"] for item in cast(tuple[dict[str, object], ...], second["manifests"])
    ] == [
        False,
        False,
    ]
    current = _entity_rows(first, "current_prices")
    history = _entity_rows(first, "price_history")
    assert {_decoded(row, "source_portal_key") for row in current} == {
        "portal-a",
        "portal-b",
    }
    assert {_decoded(row, "archive_content_sha256") for row in current} == {
        "a" * 64,
        "b" * 64,
    }
    assert {_decoded(row, "source_item_code") for row in current} == {"shared-item"}
    assert {_decoded(row, "source_store_code") for row in current} == {"shared-store"}
    assert {_decoded(row, "source_document_type") for row in current} == {
        DocumentType.PRICE_FULL.value
    }
    assert len(history) == 2
    assert {_decoded(row, "source_portal_key") for row in history}.isdisjoint(
        {
            "portal-boundary-partition",
            "portal-boundary-row",
            "portal-outside",
        }
    )


async def test_export_preflight_rejects_aggregate_rows_before_publication(
    database: Database,
    tmp_path: Path,
) -> None:
    await _seed_export_contract(database)
    operations = PostgresParquetExportOperations(
        database.engine,
        limits=ExportLimits(max_rows_per_file=3, max_dataset_rows=3),
    )
    output = tmp_path / "preflight-export"

    with pytest.raises(ExportValidationError, match="max_dataset_rows"):
        await operations.export_parquet(
            output,
            since=WINDOW_START,
            until=WINDOW_END,
        )

    assert not list(output.rglob("_manifest.json"))
    assert not list(output.rglob("dataset=*"))


class _PausingExport(PostgresParquetExportOperations):
    def __init__(self, database: Database) -> None:
        super().__init__(database.engine)
        self.partitioned = asyncio.Event()
        self.resume = asyncio.Event()
        self._paused = False

    async def _partitions(
        self,
        connection: AsyncConnection,
        entity: _EntityExport,
        *,
        since: datetime | None,
        until: datetime | None,
    ) -> tuple[_PlannedPartition, ...]:
        partitions = await super()._partitions(
            connection,
            entity,
            since=since,
            until=until,
        )
        if not self._paused:
            self._paused = True
            self.partitioned.set()
            await self.resume.wait()
        return partitions


async def test_export_uses_one_repeatable_read_snapshot_across_partition_and_rows(
    database: Database,
    tmp_path: Path,
) -> None:
    seeded = await _seed_export_contract(database)
    first_source, first_item, first_store = cast(tuple[UUID, UUID, UUID], seeded["first"])
    operations = _PausingExport(database)

    task = asyncio.create_task(
        operations.export_parquet(
            tmp_path / "snapshot-export",
            since=WINDOW_START,
            until=WINDOW_END,
        )
    )
    await operations.partitioned.wait()
    async with database.engine.begin() as connection:
        await connection.execute(
            update(current_prices)
            .where(
                current_prices.c.retailer_item_id == first_item,
                current_prices.c.store_id == first_store,
            )
            .values(item_price=Decimal("88.00"), source_file_id=first_source)
        )
    operations.resume.set()
    result = await task

    current = _entity_rows(result, "current_prices")
    portal_a = next(row for row in current if _decoded(row, "source_portal_key") == "portal-a")
    assert _decoded(portal_a, "item_price") == "10.0000"
