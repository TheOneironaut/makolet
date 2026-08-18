"""Behavior-level PostgreSQL 18 persistence checks."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import count
from uuid import UUID

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.exc import DBAPIError, InvalidRequestError

from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.errors import PersistenceConflictError, SnapshotValidationError
from makolet.adapters.persistence.ingestion import (
    _MISSING_PRICE_KEYS_SELECT,
    PostgresIngestionRepository,
    _materialize_mapped_price_incoming,
    _materialize_price_maps,
)
from makolet.adapters.persistence.leases import PostgresLeaseManager, current_ingestion_lock
from makolet.adapters.persistence.queries import PostgresQueryRepository
from makolet.adapters.persistence.schema import (
    applied_source_contents,
    availability_history,
    canonical_products,
    confirmed_product_matches,
    current_availability,
    current_prices,
    identifier_match_groups,
    ingestion_runs,
    metadata,
    price_history,
    product_identifiers,
    product_match_candidates,
    replay_runs,
    retailer_identifier_assertions,
    retailer_items,
    source_files,
    staged_prices,
    stores,
    validation_issues,
)
from makolet.application.models import (
    ApplySummary,
    ArchivedDownload,
    DownloadEvidence,
    validation_issue_charge,
)
from makolet.domain.enums import (
    CompressionFormat,
    DocumentType,
    IngestionStatus,
    IssueSeverity,
    SourceProtocol,
)
from makolet.domain.models import (
    ArchiveReceipt,
    DocumentMetadata,
    PriceRecord,
    RemoteFile,
    ValidationIssue,
)

pytestmark = pytest.mark.integration
NOW = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)
_SOURCE_TIME_SEQUENCE = count()


async def _archived_file(
    repository: PostgresIngestionRepository,
    *,
    remote_id: str,
    document_type: DocumentType,
    retailer: str = "test-retailer",
    archive_payload: str | None = None,
    expected_duplicate: bool = False,
    source_timestamp: datetime | None = None,
) -> UUID:
    effective_source_timestamp = source_timestamp or NOW + timedelta(
        seconds=next(_SOURCE_TIME_SEQUENCE)
    )
    remote = RemoteFile(
        retailer_id=retailer,
        portal_id="test-portal",
        protocol=SourceProtocol.FIXTURE,
        remote_id=remote_id,
        download_url=f"https://fixtures.invalid/{remote_id}",
        original_filename=f"{remote_id}.xml",
        document_type=document_type,
        compression=CompressionFormat.NONE,
        discovered_at=NOW,
        source_timestamp=effective_source_timestamp,
    )
    registered = await repository.register_discovery(remote)
    await repository.transition(
        registered.source_file_id,
        (IngestionStatus.DISCOVERED,),
        IngestionStatus.DOWNLOADING,
    )
    payload = archive_payload or remote_id
    digest = hashlib.sha256(payload.encode()).hexdigest()
    archived = ArchivedDownload(
        archive=ArchiveReceipt(
            content_sha256=digest,
            object_key=f"sha256/{digest}",
            content_length=len(payload),
            archived_at=NOW,
            created=True,
        ),
        evidence=DownloadEvidence(
            started_at=NOW,
            finished_at=NOW + timedelta(seconds=1),
            status_code=200,
            content_length=len(payload),
            media_type="application/xml",
            etag=None,
            last_modified=NOW,
        ),
    )
    duplicate = await repository.record_archive(
        registered.source_file_id,
        archived,
        parser_version="integration-v1",
    )
    assert duplicate is expected_duplicate
    assert (await repository.get(registered.source_file_id)).status is IngestionStatus.ARCHIVED
    return registered.source_file_id


def _price(
    source_file_id: UUID,
    *,
    record_index: int,
    item_code: str,
    item_name: str,
    amount: str,
    store_id: str = "store-1",
) -> PriceRecord:
    return PriceRecord(
        source_file_id=source_file_id,
        record_index=record_index,
        chain_id="chain-1",
        subchain_id="subchain-1",
        store_id=store_id,
        item_code=item_code,
        item_type=1,
        item_name=item_name,
        manufacturer_name="Clean Room Foods",
        manufacturer_country="IL",
        manufacturer_description=None,
        unit_quantity="1 each",
        quantity=Decimal("1"),
        unit_of_measure="each",
        is_weighted=False,
        quantity_in_package=Decimal("1"),
        item_price=Decimal(amount),
        unit_of_measure_price=Decimal(amount),
        allow_discount=True,
        item_status=1,
        price_updated_at=NOW,
        last_sale_at=None,
    )


async def _stage_prices(
    repository: PostgresIngestionRepository,
    source_file_id: UUID,
    document_type: DocumentType,
    records: tuple[PriceRecord, ...],
) -> None:
    registered = await repository.get(source_file_id)
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
                document_type=document_type,
                chain_id="chain-1",
                subchain_id="subchain-1",
                store_id="store-1",
                source_updated_at=registered.remote_file.source_timestamp,
            ),
        ),
    )
    if records:
        await repository.stage(source_file_id, records)
    await repository.transition(
        source_file_id,
        (IngestionStatus.PARSING,),
        IngestionStatus.STAGED,
    )


async def _apply_prices(
    repository: PostgresIngestionRepository,
    source_file_id: UUID,
    document_type: DocumentType,
    records: tuple[PriceRecord, ...],
    *,
    maximum_drop_fraction: float = 0.5,
) -> None:
    await _stage_prices(repository, source_file_id, document_type, records)
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
        document_type,
        minimum_full_records=1,
        maximum_drop_fraction=maximum_drop_fraction,
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.APPLYING,),
        IngestionStatus.COMPLETED,
    )


async def test_validation_issue_summaries_remain_exact_with_bounded_evidence(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(
        database.engine,
        maximum_validation_issues=10,
        maximum_validation_issue_bytes=10_000,
        maximum_validation_issue_evidence=3,
    )
    source_file_id = await _archived_file(
        repository,
        remote_id="bounded-validation-evidence",
        document_type=DocumentType.PRICE_DELTA,
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.ARCHIVED,),
        IngestionStatus.PARSING,
    )
    warnings = tuple(
        ValidationIssue(
            source_file_id=source_file_id,
            severity=IssueSeverity.WARNING,
            code="tiny_warning",
            message=f"warning-{index}",
            record_index=index,
        )
        for index in range(7)
    )
    warning_summary = await repository.stage(source_file_id, warnings)
    quarantine = ValidationIssue(
        source_file_id=source_file_id,
        severity=IssueSeverity.FILE_QUARANTINE,
        code="hostile_xml",
        message="cumulative issue budget exceeded",
    )
    quarantine_summary = await repository.stage(source_file_id, (quarantine,))

    assert warning_summary.warnings == 7
    assert warning_summary.sampled_validation_issues == 3
    assert warning_summary.validation_issue_bytes == sum(
        validation_issue_charge(issue) for issue in warnings
    )
    assert quarantine_summary.file_quarantines == 1
    assert quarantine_summary.sampled_validation_issues == 0
    assert await repository.has_file_quarantine_issue(source_file_id) is True

    async with database.engine.connect() as connection:
        persisted = (
            await connection.execute(
                select(
                    ingestion_runs.c.warnings,
                    ingestion_runs.c.file_quarantine_issues,
                    ingestion_runs.c.validation_issue_bytes,
                    ingestion_runs.c.validation_issue_samples,
                    select(func.count())
                    .select_from(validation_issues)
                    .where(validation_issues.c.source_file_id == source_file_id)
                    .scalar_subquery(),
                    select(func.count())
                    .select_from(validation_issues)
                    .where(
                        validation_issues.c.source_file_id == source_file_id,
                        validation_issues.c.severity == IssueSeverity.FILE_QUARANTINE.value,
                    )
                    .scalar_subquery(),
                ).where(ingestion_runs.c.source_file_id == source_file_id)
            )
        ).one()
    assert tuple(persisted) == (
        7,
        1,
        warning_summary.validation_issue_bytes + validation_issue_charge(quarantine),
        3,
        3,
        0,
    )

    status = await PostgresQueryRepository(database.engine).source_status(limit=10)
    item = next(row for row in status.items if row["source_file_id"] == source_file_id)
    assert item["warning_count"] == 7
    assert item["file_quarantine_count"] == 1


async def test_migration_matches_runtime_metadata(database: Database) -> None:
    health = await database.health()
    assert health.server_version_number >= 180000

    async with database.engine.connect() as connection:
        differences = await connection.run_sync(
            lambda sync_connection: compare_metadata(
                MigrationContext.configure(
                    sync_connection,
                    opts={"compare_type": True, "compare_server_default": True},
                ),
                metadata,
            )
        )
    assert differences == []


async def test_price_delta_preserves_absent_items_and_unchanged_history(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    full_id = await _archived_file(
        repository,
        remote_id="full-1",
        document_type=DocumentType.PRICE_FULL,
    )
    await _apply_prices(
        repository,
        full_id,
        DocumentType.PRICE_FULL,
        (
            _price(
                full_id,
                record_index=1,
                item_code="1234567890128",
                item_name="Coffee One",
                amount="10.00",
            ),
            _price(
                full_id,
                record_index=2,
                item_code="96385074",
                item_name="Coffee Two",
                amount="20.00",
            ),
        ),
    )

    delta_id = await _archived_file(
        repository,
        remote_id="delta-1",
        document_type=DocumentType.PRICE_DELTA,
    )
    await _apply_prices(
        repository,
        delta_id,
        DocumentType.PRICE_DELTA,
        (
            _price(
                delta_id,
                record_index=1,
                item_code="1234567890128",
                item_name="Coffee One",
                amount="11.00",
            ),
        ),
    )
    unchanged_id = await _archived_file(
        repository,
        remote_id="delta-2",
        document_type=DocumentType.PRICE_DELTA,
    )
    await _apply_prices(
        repository,
        unchanged_id,
        DocumentType.PRICE_DELTA,
        (
            _price(
                unchanged_id,
                record_index=1,
                item_code="1234567890128",
                item_name="Coffee One",
                amount="11.00",
            ),
        ),
    )

    async with database.engine.connect() as connection:
        first_item_id = (
            await connection.execute(
                select(retailer_items.c.id).where(
                    retailer_items.c.source_item_code == "1234567890128"
                )
            )
        ).scalar_one()
        history_count = (
            await connection.execute(
                select(func.count())
                .select_from(price_history)
                .where(price_history.c.retailer_item_id == first_item_id)
            )
        ).scalar_one()
        available_count = (
            await connection.execute(
                select(func.count())
                .select_from(current_availability)
                .where(current_availability.c.is_available)
            )
        ).scalar_one()
    assert history_count == 2
    assert available_count == 2

    queries = PostgresQueryRepository(database.engine)
    product = await queries.find_product_by_barcode("1234567890128")
    assert product is not None
    price_page = await queries.current_prices(
        UUID(str(product["id"])),
        retailer_id=None,
        store_id=None,
        limit=10,
        cursor=None,
    )
    assert price_page.items[0]["item_price"] == Decimal("11.0000")
    history_page = await queries.price_history(
        UUID(str(product["id"])),
        store_id=None,
        since=datetime(2020, 1, 1, tzinfo=UTC),
        until=datetime(2030, 1, 1, tzinfo=UTC),
        limit=10,
        cursor=None,
    )
    assert len(history_page.items) == 2


async def test_full_snapshot_drop_guard_rolls_back_absence(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    full_id = await _archived_file(
        repository,
        remote_id="guard-full-1",
        document_type=DocumentType.PRICE_FULL,
    )
    await _apply_prices(
        repository,
        full_id,
        DocumentType.PRICE_FULL,
        (
            _price(
                full_id,
                record_index=1,
                item_code="1234567890128",
                item_name="Guard One",
                amount="10",
            ),
            _price(
                full_id,
                record_index=2,
                item_code="96385074",
                item_name="Guard Two",
                amount="20",
            ),
        ),
    )
    suspicious_id = await _archived_file(
        repository,
        remote_id="guard-full-2",
        document_type=DocumentType.PRICE_FULL,
    )
    with pytest.raises(SnapshotValidationError, match=r"drops 50\.0%"):
        await _apply_prices(
            repository,
            suspicious_id,
            DocumentType.PRICE_FULL,
            (
                _price(
                    suspicious_id,
                    record_index=1,
                    item_code="1234567890128",
                    item_name="Guard One",
                    amount="10",
                ),
            ),
            maximum_drop_fraction=0.25,
        )
    async with database.engine.connect() as connection:
        available_count = (
            await connection.execute(
                select(func.count())
                .select_from(current_availability)
                .where(current_availability.c.is_available)
            )
        ).scalar_one()
    assert available_count == 2


async def test_allowed_full_snapshot_drop_closes_and_reopens_availability_history(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    first_id = await _archived_file(
        repository,
        remote_id="allowed-drop-full-1",
        document_type=DocumentType.PRICE_FULL,
    )
    await _apply_prices(
        repository,
        first_id,
        DocumentType.PRICE_FULL,
        tuple(
            _price(
                first_id,
                record_index=index + 1,
                item_code=f"allowed-drop-{index:02d}",
                item_name=f"Allowed Drop {index}",
                amount=str(index + 1),
            )
            for index in range(10)
        ),
    )
    second_id = await _archived_file(
        repository,
        remote_id="allowed-drop-full-2",
        document_type=DocumentType.PRICE_FULL,
    )
    await _apply_prices(
        repository,
        second_id,
        DocumentType.PRICE_FULL,
        tuple(
            _price(
                second_id,
                record_index=index + 1,
                item_code=f"allowed-drop-{index:02d}",
                item_name=f"Allowed Drop {index}",
                amount=str(index + 1),
            )
            for index in range(9)
        ),
        maximum_drop_fraction=0.20,
    )

    async with database.engine.connect() as connection:
        missing_item_id = (
            await connection.execute(
                select(retailer_items.c.id).where(
                    retailer_items.c.source_item_code == "allowed-drop-09"
                )
            )
        ).scalar_one()
        current = (
            await connection.execute(
                select(current_availability.c.is_available).where(
                    current_availability.c.retailer_item_id == missing_item_id
                )
            )
        ).scalar_one()
        history = (
            await connection.execute(
                select(
                    availability_history.c.is_available,
                    availability_history.c.valid_to,
                )
                .where(availability_history.c.retailer_item_id == missing_item_id)
                .order_by(availability_history.c.valid_from)
            )
        ).all()
    assert current is False
    assert [(row.is_available, row.valid_to is None) for row in history] == [
        (True, False),
        (False, True),
    ]


async def test_full_snapshot_missing_set_uses_composite_linear_plan(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    item_count = 500
    first_id = await _archived_file(
        repository,
        remote_id="missing-plan-full-1",
        document_type=DocumentType.PRICE_FULL,
    )
    await _apply_prices(
        repository,
        first_id,
        DocumentType.PRICE_FULL,
        tuple(
            _price(
                first_id,
                record_index=(store_index * item_count) + item_index + 1,
                item_code=f"missing-plan-{item_index:04d}",
                item_name=f"Missing Plan {item_index}",
                amount=str((item_index % 100) + 1),
                store_id=f"store-{store_index + 1}",
            )
            for store_index in range(2)
            for item_index in range(item_count)
        ),
    )

    second_id = await _archived_file(
        repository,
        remote_id="missing-plan-full-2",
        document_type=DocumentType.PRICE_FULL,
    )
    second_records = tuple(
        _price(
            second_id,
            record_index=(store_index * item_count) + item_index + 1,
            item_code=f"missing-plan-{item_index:04d}",
            item_name=f"Missing Plan {item_index}",
            amount=str((item_index % 100) + 1),
            store_id=f"store-{store_index + 1}",
        )
        for store_index in range(2)
        for item_index in range(item_count)
        if store_index == 0 or item_index < item_count - 1
    )
    await _stage_prices(repository, second_id, DocumentType.PRICE_FULL, second_records)

    async with database.engine.connect() as connection:
        # Production apply analyzes COPY staging before selecting an execution plan.
        # Mirror that boundary so this guard measures the production planner state.
        await connection.execute(text("ANALYZE staged_prices, staged_documents"))
        source_scope = (
            await connection.execute(
                select(
                    source_files.c.retailer_id,
                    source_files.c.portal_id,
                ).where(source_files.c.id == second_id)
            )
        ).one()
        await _materialize_price_maps(
            connection,
            source_file_id=second_id,
            retailer_id=source_scope.retailer_id,
            portal_id=source_scope.portal_id,
        )
        await _materialize_mapped_price_incoming(
            connection,
            {
                "source_file_id": second_id,
                "retailer_id": source_scope.retailer_id,
                "portal_id": source_scope.portal_id,
            },
        )
        estimated_rows = {
            str(row.relation): int(row.estimated_rows)
            for row in (
                await connection.execute(
                    text(
                        """
                    SELECT relation::text AS relation, reltuples::bigint AS estimated_rows
                      FROM unnest(ARRAY[
                               'pg_temp.makolet_exact_gtin_items'::regclass,
                               'pg_temp.makolet_price_stores'::regclass,
                               'pg_temp.makolet_mapped_price_incoming'::regclass
                           ]) relation
                      JOIN pg_class ON pg_class.oid = relation
                    """
                    )
                )
            ).all()
        }
        plan = (
            await connection.execute(
                text(f"EXPLAIN (ANALYZE, FORMAT JSON) {_MISSING_PRICE_KEYS_SELECT}"),
                {
                    "source_file_id": second_id,
                    "retailer_id": source_scope.retailer_id,
                    "portal_id": source_scope.portal_id,
                },
            )
        ).scalar_one()
    assert estimated_rows == {
        "makolet_exact_gtin_items": item_count,
        "makolet_price_stores": 2,
        "makolet_mapped_price_incoming": len(second_records),
    }
    parsed_plan: object = json.loads(plan) if isinstance(plan, str) else plan
    assert isinstance(parsed_plan, list)
    assert parsed_plan
    assert isinstance(parsed_plan[0], Mapping)
    root_plan = parsed_plan[0].get("Plan")
    assert isinstance(root_plan, Mapping)
    plan_nodes = [root_plan]
    saw_composite_set_operation = False
    anti_merge_join_with_filter = False
    rows_removed_by_join_filter = 0
    while plan_nodes:
        node = plan_nodes.pop()
        if node.get("Node Type") == "SetOp":
            saw_composite_set_operation = True
        if (
            node.get("Node Type") == "Merge Join"
            and node.get("Join Type") == "Anti"
            and "Join Filter" in node
        ):
            anti_merge_join_with_filter = True
        rows_removed_by_join_filter += int(node.get("Rows Removed by Join Filter", 0))
        children = node.get("Plans", ())
        if isinstance(children, list):
            plan_nodes.extend(child for child in children if isinstance(child, Mapping))
    assert saw_composite_set_operation
    assert not anti_merge_join_with_filter
    assert rows_removed_by_join_filter <= len(second_records)

    await repository.transition(
        second_id,
        (IngestionStatus.STAGED,),
        IngestionStatus.VALIDATING,
    )
    await repository.transition(
        second_id,
        (IngestionStatus.VALIDATING,),
        IngestionStatus.APPLYING,
    )
    summary = await repository.apply(
        second_id,
        DocumentType.PRICE_FULL,
        minimum_full_records=1,
        maximum_drop_fraction=0.01,
    )
    await repository.transition(
        second_id,
        (IngestionStatus.APPLYING,),
        IngestionStatus.COMPLETED,
    )
    assert summary.unavailable == 1

    async with database.engine.connect() as connection:
        availability = (
            await connection.execute(
                select(
                    stores.c.source_store_code,
                    current_availability.c.is_available,
                )
                .select_from(current_availability)
                .join(
                    retailer_items,
                    retailer_items.c.id == current_availability.c.retailer_item_id,
                )
                .join(stores, stores.c.id == current_availability.c.store_id)
                .where(retailer_items.c.source_item_code == f"missing-plan-{item_count - 1:04d}")
                .order_by(stores.c.source_store_code)
            )
        ).all()
    assert [(row.source_store_code, row.is_available) for row in availability] == [
        ("store-1", True),
        ("store-2", False),
    ]


async def test_replay_and_duplicate_content_preserve_archive_identity(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    source_file_id = await _archived_file(
        repository,
        remote_id="replay-full",
        document_type=DocumentType.PRICE_FULL,
    )
    record = _price(
        source_file_id,
        record_index=1,
        item_code="1234567890128",
        item_name="Replay Coffee",
        amount="12.00",
    )
    await _apply_prices(
        repository,
        source_file_id,
        DocumentType.PRICE_FULL,
        (record,),
    )
    archived = await repository.get(source_file_id)
    assert archived.archive_object_key is not None
    assert archived.content_sha256 == hashlib.sha256(b"replay-full").hexdigest()

    replay = await repository.begin_replay(source_file_id, parser_version="integration-v2")
    await repository.clear_staging(source_file_id)
    await repository.stage(
        source_file_id,
        (
            DocumentMetadata(
                source_file_id=source_file_id,
                document_type=DocumentType.PRICE_FULL,
                chain_id="chain-1",
                subchain_id="subchain-1",
                store_id="store-1",
                source_updated_at=archived.remote_file.source_timestamp,
            ),
            record,
        ),
    )
    replay_summary = await repository.apply(
        source_file_id,
        DocumentType.PRICE_FULL,
        minimum_full_records=1,
        maximum_drop_fraction=0.5,
    )
    assert replay_summary.unchanged == 1
    assert replay_summary.history_events == 0
    await repository.finish_replay(replay.replay_id, succeeded=True)
    async with database.engine.connect() as connection:
        replay_status = (
            await connection.execute(
                select(replay_runs.c.status).where(replay_runs.c.id == replay.replay_id)
            )
        ).scalar_one()
    assert replay_status == IngestionStatus.COMPLETED.value

    duplicate_id = await _archived_file(
        repository,
        remote_id="replay-duplicate",
        document_type=DocumentType.PRICE_FULL,
        archive_payload="replay-full",
        expected_duplicate=True,
    )
    duplicate = await repository.get(duplicate_id)
    assert duplicate.archive_object_key == archived.archive_object_key
    assert duplicate.content_sha256 == archived.content_sha256
    await repository.transition(
        duplicate_id,
        (IngestionStatus.ARCHIVED,),
        IngestionStatus.PARSING,
    )
    reused = await repository.reuse_validated_staging(
        duplicate_id,
        parser_version="integration-v2",
        document_type=DocumentType.PRICE_FULL,
        compression=CompressionFormat.NONE,
    )
    # A source whose retained staging was subsequently mutated by a replay is not
    # a safe parse-cache candidate. Production falls back to the parser.
    assert reused is None
    await repository.stage(
        duplicate_id,
        (
            DocumentMetadata(
                source_file_id=duplicate_id,
                document_type=DocumentType.PRICE_FULL,
                chain_id="chain-1",
                subchain_id="subchain-1",
                store_id="store-1",
                source_updated_at=duplicate.remote_file.source_timestamp,
            ),
            _price(
                duplicate_id,
                record_index=1,
                item_code="1234567890128",
                item_name="Replay Coffee",
                amount="12.00",
            ),
        ),
    )
    finalized = await repository.finalize_staging(
        duplicate_id,
        DocumentType.PRICE_FULL,
    )
    assert finalized.metadata_records == 1
    assert finalized.price_records == 1
    await repository.transition(
        duplicate_id,
        (IngestionStatus.PARSING,),
        IngestionStatus.STAGED,
    )
    await repository.transition(
        duplicate_id,
        (IngestionStatus.STAGED,),
        IngestionStatus.VALIDATING,
    )
    await repository.transition(
        duplicate_id,
        (IngestionStatus.VALIDATING,),
        IngestionStatus.APPLYING,
    )
    duplicate_summary = await repository.apply(
        duplicate_id,
        DocumentType.PRICE_FULL,
        minimum_full_records=1,
        maximum_drop_fraction=0.5,
    )
    assert duplicate_summary.unchanged == 1
    assert duplicate_summary.history_events == 0
    await repository.transition(
        duplicate_id,
        (IngestionStatus.APPLYING,),
        IngestionStatus.COMPLETED,
    )
    async with database.engine.connect() as connection:
        provenance = (
            await connection.execute(
                select(
                    current_prices.c.source_file_id,
                    current_prices.c.last_observed_at,
                    func.count(price_history.c.id).over().label("history_count"),
                )
                .select_from(current_prices)
                .join(
                    price_history,
                    price_history.c.retailer_item_id == current_prices.c.retailer_item_id,
                )
                .where(price_history.c.store_id == current_prices.c.store_id)
                .limit(1)
            )
        ).one()
    assert provenance.source_file_id == duplicate_id
    assert provenance.history_count == 1


async def test_repeated_a_b_a_content_restores_history_from_reused_staging(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    first_a_id = await _archived_file(
        repository,
        remote_id="chronology-a-first",
        document_type=DocumentType.PRICE_DELTA,
        retailer="chronology-retailer",
        archive_payload="content-a",
        source_timestamp=NOW,
    )
    await _apply_prices(
        repository,
        first_a_id,
        DocumentType.PRICE_DELTA,
        (
            _price(
                first_a_id,
                record_index=1,
                item_code="chronology-item",
                item_name="Chronology Coffee",
                amount="10.00",
            ),
        ),
    )
    changed_id = await _archived_file(
        repository,
        remote_id="chronology-b",
        document_type=DocumentType.PRICE_DELTA,
        retailer="chronology-retailer",
        archive_payload="content-b",
        source_timestamp=NOW + timedelta(seconds=1),
    )
    await _apply_prices(
        repository,
        changed_id,
        DocumentType.PRICE_DELTA,
        (
            _price(
                changed_id,
                record_index=1,
                item_code="chronology-item",
                item_name="Chronology Coffee",
                amount="12.00",
            ),
        ),
    )
    restored_id = await _archived_file(
        repository,
        remote_id="chronology-a-restored",
        document_type=DocumentType.PRICE_DELTA,
        retailer="chronology-retailer",
        archive_payload="content-a",
        expected_duplicate=True,
        source_timestamp=NOW + timedelta(seconds=2),
    )
    await repository.transition(
        restored_id,
        (IngestionStatus.ARCHIVED,),
        IngestionStatus.PARSING,
    )
    reused = await repository.reuse_validated_staging(
        restored_id,
        parser_version="integration-v1",
        document_type=DocumentType.PRICE_DELTA,
        compression=CompressionFormat.NONE,
    )
    assert reused is not None
    await repository.finalize_staging(restored_id, DocumentType.PRICE_DELTA)
    for expected, target in (
        (IngestionStatus.PARSING, IngestionStatus.STAGED),
        (IngestionStatus.STAGED, IngestionStatus.VALIDATING),
        (IngestionStatus.VALIDATING, IngestionStatus.APPLYING),
    ):
        await repository.transition(restored_id, (expected,), target)
    restored = await repository.apply(
        restored_id,
        DocumentType.PRICE_DELTA,
        minimum_full_records=1,
        maximum_drop_fraction=0.5,
    )
    assert restored.updated == 1
    assert restored.history_events == 1
    await repository.transition(
        restored_id,
        (IngestionStatus.APPLYING,),
        IngestionStatus.COMPLETED,
    )
    async with database.engine.connect() as connection:
        history = (
            await connection.execute(
                select(
                    price_history.c.item_price,
                    price_history.c.source_file_id,
                    price_history.c.valid_to,
                )
                .join(
                    retailer_items,
                    retailer_items.c.id == price_history.c.retailer_item_id,
                )
                .where(retailer_items.c.source_item_code == "chronology-item")
                .order_by(price_history.c.valid_from)
            )
        ).all()
    assert [(row.item_price, row.source_file_id) for row in history] == [
        (Decimal("10.0000"), first_a_id),
        (Decimal("12.0000"), changed_id),
        (Decimal("10.0000"), restored_id),
    ]
    assert [row.valid_to is None for row in history] == [False, False, True]


async def test_concurrent_distinct_identities_reuse_staging_and_each_apply(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    original_id = await _archived_file(
        repository,
        remote_id="concurrent-reuse-original",
        document_type=DocumentType.PRICE_DELTA,
        retailer="concurrent-reuse-retailer",
        archive_payload="concurrent-reuse-content",
        source_timestamp=NOW,
    )
    await _apply_prices(
        repository,
        original_id,
        DocumentType.PRICE_DELTA,
        (
            _price(
                original_id,
                record_index=1,
                item_code="concurrent-reuse-item",
                item_name="Concurrent Coffee",
                amount="9.00",
            ),
        ),
    )
    target_ids = tuple(
        [
            await _archived_file(
                repository,
                remote_id=f"concurrent-reuse-{index}",
                document_type=DocumentType.PRICE_DELTA,
                retailer="concurrent-reuse-retailer",
                archive_payload="concurrent-reuse-content",
                expected_duplicate=True,
                source_timestamp=NOW + timedelta(seconds=index),
            )
            for index in (1, 2)
        ]
    )

    async def reuse_and_apply(source_file_id: UUID) -> ApplySummary:
        await repository.transition(
            source_file_id,
            (IngestionStatus.ARCHIVED,),
            IngestionStatus.PARSING,
        )
        reused = await repository.reuse_validated_staging(
            source_file_id,
            parser_version="integration-v1",
            document_type=DocumentType.PRICE_DELTA,
            compression=CompressionFormat.NONE,
        )
        assert reused is not None
        await repository.finalize_staging(source_file_id, DocumentType.PRICE_DELTA)
        for expected, target in (
            (IngestionStatus.PARSING, IngestionStatus.STAGED),
            (IngestionStatus.STAGED, IngestionStatus.VALIDATING),
            (IngestionStatus.VALIDATING, IngestionStatus.APPLYING),
        ):
            await repository.transition(source_file_id, (expected,), target)
        result = await repository.apply(
            source_file_id,
            DocumentType.PRICE_DELTA,
            minimum_full_records=1,
            maximum_drop_fraction=0.5,
        )
        await repository.transition(
            source_file_id,
            (IngestionStatus.APPLYING,),
            IngestionStatus.COMPLETED,
        )
        return result

    results = await asyncio.gather(*(reuse_and_apply(source_id) for source_id in target_ids))

    assert [result.unchanged for result in results] == [1, 1]
    assert [result.history_events for result in results] == [0, 0]
    async with database.engine.connect() as connection:
        ledgers = int(
            (
                await connection.execute(
                    select(func.count())
                    .select_from(applied_source_contents)
                    .where(applied_source_contents.c.source_file_id.in_(target_ids))
                )
            ).scalar_one()
        )
    assert ledgers == 2


@pytest.mark.parametrize(
    "terminal_status",
    [IngestionStatus.QUARANTINED, IngestionStatus.FAILED_TERMINAL],
)
async def test_first_replay_uses_immutable_effective_time_after_terminal_failure(
    database: Database,
    terminal_status: IngestionStatus,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    source_timestamp = NOW - timedelta(days=1)
    source_file_id = await _archived_file(
        repository,
        remote_id=f"first-replay-{terminal_status.value}",
        document_type=DocumentType.PRICE_DELTA,
        retailer=f"first-replay-{terminal_status.value}",
        source_timestamp=source_timestamp,
    )
    await repository.transition(
        source_file_id,
        (IngestionStatus.ARCHIVED,),
        terminal_status,
    )
    replay = await repository.begin_replay(source_file_id, parser_version="integration-v1")
    await repository.clear_staging(source_file_id)
    await repository.stage(
        source_file_id,
        (
            DocumentMetadata(
                source_file_id=source_file_id,
                document_type=DocumentType.PRICE_DELTA,
                chain_id="chain-1",
                subchain_id="subchain-1",
                store_id="store-1",
                source_updated_at=source_timestamp,
            ),
            _price(
                source_file_id,
                record_index=1,
                item_code=f"item-{terminal_status.value}",
                item_name="Recovered Coffee",
                amount="15.00",
            ),
        ),
    )
    await repository.finalize_staging(source_file_id, DocumentType.PRICE_DELTA)
    applied = await repository.apply(
        source_file_id,
        DocumentType.PRICE_DELTA,
        minimum_full_records=1,
        maximum_drop_fraction=0.5,
    )
    assert applied.inserted == 1
    await repository.finish_replay(replay.replay_id, succeeded=True)
    async with database.engine.connect() as connection:
        applied_at = (
            await connection.execute(
                select(applied_source_contents.c.applied_at).where(
                    applied_source_contents.c.source_file_id == source_file_id
                )
            )
        ).scalar_one()
    assert applied_at == source_timestamp


async def test_concurrent_equal_gtin_uses_one_canonical_identity_without_orphans(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    first_source = await _archived_file(
        repository,
        remote_id="concurrent-a",
        document_type=DocumentType.PRICE_DELTA,
        retailer="concurrent-retailer-a",
    )
    second_source = await _archived_file(
        repository,
        remote_id="concurrent-b",
        document_type=DocumentType.PRICE_DELTA,
        retailer="concurrent-retailer-b",
    )
    item_code = "1234567890128"

    await asyncio.gather(
        _apply_prices(
            repository,
            first_source,
            DocumentType.PRICE_DELTA,
            (
                _price(
                    first_source,
                    record_index=1,
                    item_code=item_code,
                    item_name="Concurrent Coffee A",
                    amount="12.00",
                ),
            ),
        ),
        _apply_prices(
            repository,
            second_source,
            DocumentType.PRICE_DELTA,
            (
                _price(
                    second_source,
                    record_index=1,
                    item_code=item_code,
                    item_name="Concurrent Coffee B",
                    amount="13.00",
                ),
            ),
        ),
    )

    async with database.engine.connect() as connection:
        product_count = int(
            (
                await connection.execute(select(func.count()).select_from(canonical_products))
            ).scalar_one()
        )
        identifier_count = int(
            (
                await connection.execute(select(func.count()).select_from(product_identifiers))
            ).scalar_one()
        )
        match_count = int(
            (
                await connection.execute(
                    select(func.count()).select_from(confirmed_product_matches)
                )
            ).scalar_one()
        )
        assertion_count = int(
            (
                await connection.execute(
                    select(func.count()).select_from(retailer_identifier_assertions)
                )
            ).scalar_one()
        )
        group_count = int(
            (
                await connection.execute(select(func.count()).select_from(identifier_match_groups))
            ).scalar_one()
        )
        match_methods = set(
            (await connection.execute(select(confirmed_product_matches.c.method))).scalars()
        )

    assert (product_count, identifier_count, match_count) == (1, 3, 2)
    assert assertion_count == 2
    assert group_count == 1
    assert match_methods == {"exact_validated_gtin"}


async def test_single_retailer_gtin_is_scoped_provisional_evidence(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    source_file_id = await _archived_file(
        repository,
        remote_id="provisional-gtin",
        document_type=DocumentType.PRICE_DELTA,
    )
    gtin = "1234567890128"
    await _apply_prices(
        repository,
        source_file_id,
        DocumentType.PRICE_DELTA,
        (
            _price(
                source_file_id,
                record_index=1,
                item_code=gtin,
                item_name="Provisional Coffee",
                amount="12.00",
            ),
        ),
    )

    queries = PostgresQueryRepository(database.engine)
    barcode = await queries.find_product_by_barcode(gtin)
    assert barcode is not None
    assert barcode["barcode_validated"] is False
    assert barcode["identifier_scope"] == "portal_asserted"

    async with database.engine.connect() as connection:
        identifiers = (
            await connection.execute(
                select(
                    product_identifiers.c.issuer_retailer_id,
                    product_identifiers.c.is_validated,
                    product_identifiers.c.validation_method,
                )
            )
        ).all()
        assertion = (
            (await connection.execute(select(retailer_identifier_assertions))).mappings().one()
        )
        match = (await connection.execute(select(confirmed_product_matches))).mappings().one()

    assert len(identifiers) == 1
    assert identifiers[0].issuer_retailer_id is not None
    assert identifiers[0].is_validated is False
    assert identifiers[0].validation_method == "retailer_assertion"
    assert assertion.source_file_id == source_file_id
    assert assertion.superseded_at is None
    assert match.method == "exact_provisional_gtin"
    assert match.evidence["assertion_id"] == str(assertion.id)


async def test_gtin_correction_supersedes_lineage_and_replaces_automatic_match(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    original_gtin = "1234567890128"
    corrected_gtin = "4006381333931"
    original_source = await _archived_file(
        repository,
        remote_id="identity-before-correction",
        document_type=DocumentType.PRICE_DELTA,
    )
    await _apply_prices(
        repository,
        original_source,
        DocumentType.PRICE_DELTA,
        (
            _price(
                original_source,
                record_index=1,
                item_code=original_gtin,
                item_name="Correctable Coffee",
                amount="12.00",
            ),
        ),
    )
    async with database.engine.connect() as connection:
        original_product_id = (
            await connection.execute(select(confirmed_product_matches.c.canonical_product_id))
        ).scalar_one()

    correction_source = await _archived_file(
        repository,
        remote_id="identity-after-correction",
        document_type=DocumentType.PRICE_DELTA,
    )
    correction_registered = await repository.get(correction_source)
    await repository.transition(
        correction_source,
        (IngestionStatus.ARCHIVED,),
        IngestionStatus.PARSING,
    )
    await repository.clear_staging(correction_source)
    await repository.stage(
        correction_source,
        (
            DocumentMetadata(
                source_file_id=correction_source,
                document_type=DocumentType.PRICE_DELTA,
                chain_id="chain-1",
                subchain_id="subchain-1",
                store_id="store-1",
                source_updated_at=correction_registered.remote_file.source_timestamp,
            ),
            _price(
                correction_source,
                record_index=1,
                item_code=original_gtin,
                item_name="Correctable Coffee",
                amount="13.00",
            ),
        ),
    )
    async with database.engine.begin() as connection:
        await connection.execute(
            update(staged_prices)
            .where(staged_prices.c.source_file_id == correction_source)
            .values(gtin=corrected_gtin)
        )
    for expected, target in (
        (IngestionStatus.PARSING, IngestionStatus.STAGED),
        (IngestionStatus.STAGED, IngestionStatus.VALIDATING),
        (IngestionStatus.VALIDATING, IngestionStatus.APPLYING),
    ):
        await repository.transition(correction_source, (expected,), target)
    await repository.apply(
        correction_source,
        DocumentType.PRICE_DELTA,
        minimum_full_records=1,
        maximum_drop_fraction=0.5,
    )
    await repository.transition(
        correction_source,
        (IngestionStatus.APPLYING,),
        IngestionStatus.COMPLETED,
    )

    queries = PostgresQueryRepository(database.engine)
    assert await queries.find_product_by_barcode(original_gtin) is None
    corrected = await queries.find_product_by_barcode(corrected_gtin)
    assert corrected is not None
    assert corrected["id"] != original_product_id

    async with database.engine.connect() as connection:
        evidence = (
            (
                await connection.execute(
                    select(retailer_identifier_assertions).order_by(
                        retailer_identifier_assertions.c.asserted_at,
                        retailer_identifier_assertions.c.id,
                    )
                )
            )
            .mappings()
            .all()
        )
        item = (await connection.execute(select(retailer_items))).mappings().one()
        match = (await connection.execute(select(confirmed_product_matches))).mappings().one()

    assert item.gtin == corrected_gtin
    assert [row.normalized_value for row in evidence] == [original_gtin, corrected_gtin]
    assert evidence[0].superseded_at is not None
    assert evidence[1].source_file_id == correction_source
    assert evidence[1].superseded_at is None
    assert match.canonical_product_id == corrected["id"]
    assert match.evidence["source_file_id"] == str(correction_source)


async def test_exact_identifier_conflict_preserves_manual_match_for_review(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    gtin = "1234567890128"
    first_source = await _archived_file(
        repository,
        remote_id="manual-conflict-first",
        document_type=DocumentType.PRICE_DELTA,
    )
    await _apply_prices(
        repository,
        first_source,
        DocumentType.PRICE_DELTA,
        (
            _price(
                first_source,
                record_index=1,
                item_code=gtin,
                item_name="Review Coffee",
                amount="12.00",
            ),
        ),
    )
    async with database.engine.begin() as connection:
        alternate_product_id = (
            await connection.execute(
                insert(canonical_products)
                .values(name="Operator Selected Product", status="active")
                .returning(canonical_products.c.id)
            )
        ).scalar_one()
        await connection.execute(
            update(confirmed_product_matches).values(
                canonical_product_id=alternate_product_id,
                method="operator_review",
                evidence={"ticket": "review-1"},
                confirmed_by="operator:test",
            )
        )

    second_source = await _archived_file(
        repository,
        remote_id="manual-conflict-second",
        document_type=DocumentType.PRICE_DELTA,
    )
    await _apply_prices(
        repository,
        second_source,
        DocumentType.PRICE_DELTA,
        (
            _price(
                second_source,
                record_index=1,
                item_code=gtin,
                item_name="Review Coffee",
                amount="13.00",
            ),
        ),
    )

    async with database.engine.connect() as connection:
        match = (await connection.execute(select(confirmed_product_matches))).mappings().one()
        candidate = (await connection.execute(select(product_match_candidates))).mappings().one()

    assert match.canonical_product_id == alternate_product_id
    assert match.method == "operator_review"
    assert match.confirmed_by == "operator:test"
    assert candidate.method == "exact_identifier_conflict"
    assert candidate.status == "pending"
    assert candidate.evidence["reason"] == "manual_match_disagrees"


async def test_lease_exclusion_and_keyset_pagination(database: Database) -> None:
    leases = PostgresLeaseManager(database)
    async with leases.acquire("integration-resource", "worker-1", timedelta(minutes=1)) as first:
        assert first
        async with leases.acquire(
            "integration-resource", "worker-2", timedelta(minutes=1)
        ) as second:
            assert not second
    async with leases.acquire(
        "integration-resource", "worker-2", timedelta(minutes=1)
    ) as after_release:
        assert after_release

    repository = PostgresIngestionRepository(database.engine)
    await repository.register_discovery(
        RemoteFile(
            retailer_id="pagination-a",
            portal_id="portal-a",
            protocol=SourceProtocol.FIXTURE,
            remote_id="page-a",
            download_url="https://fixtures.invalid/a",
            original_filename="a.xml",
            document_type=DocumentType.STORES,
            compression=CompressionFormat.NONE,
            discovered_at=NOW,
        )
    )
    await repository.register_discovery(
        RemoteFile(
            retailer_id="pagination-b",
            portal_id="portal-b",
            protocol=SourceProtocol.FIXTURE,
            remote_id="page-b",
            download_url="https://fixtures.invalid/b",
            original_filename="b.xml",
            document_type=DocumentType.STORES,
            compression=CompressionFormat.NONE,
            discovered_at=NOW,
        )
    )
    queries = PostgresQueryRepository(database.engine)
    first_page = await queries.list_retailers(limit=1, cursor=None)
    assert len(first_page.items) == 1
    assert first_page.next_cursor is not None
    second_page = await queries.list_retailers(limit=1, cursor=first_page.next_cursor)
    assert len(second_page.items) == 1
    assert first_page.items[0]["id"] != second_page.items[0]["id"]


async def test_ingestion_lease_sql_obeys_database_statement_timeout(
    migrated_database_url: str,
) -> None:
    database = Database.from_url(
        migrated_database_url,
        pool_size=1,
        max_overflow=0,
        statement_timeout_ms=100,
    )
    leases = PostgresLeaseManager(database)
    try:
        async with leases.acquire(
            "source-file:statement-timeout",
            "worker-timeout",
            timedelta(minutes=1),
        ) as acquired:
            assert acquired
            lock = current_ingestion_lock()
            assert lock is not None
            with pytest.raises(DBAPIError, match="statement timeout"):
                async with lock.connection.begin():
                    await lock.connection.execute(text("SELECT pg_sleep(1)"))
            assert (await lock.connection.execute(text("SELECT 1"))).scalar_one() == 1
            await lock.connection.commit()
    finally:
        await database.dispose()


async def test_ingestion_lease_cannot_be_overtaken_after_its_legacy_ttl(
    database: Database,
) -> None:
    leases = PostgresLeaseManager(database)
    async with leases.acquire(
        "source-file:long-running",
        "worker-1",
        timedelta(milliseconds=5),
    ) as first:
        assert first
        await asyncio.sleep(0.03)
        async with leases.acquire(
            "source-file:long-running",
            "worker-2",
            timedelta(milliseconds=5),
        ) as takeover:
            assert not takeover


async def test_ingestion_lease_releases_after_owner_cancellation(database: Database) -> None:
    leases = PostgresLeaseManager(database)
    entered = asyncio.Event()

    async def hold() -> None:
        async with leases.acquire(
            "source-file:cancelled",
            "worker-1",
            timedelta(minutes=1),
        ) as acquired:
            assert acquired
            entered.set()
            await asyncio.Future()

    owner = asyncio.create_task(hold())
    await entered.wait()
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner

    async with leases.acquire(
        "source-file:cancelled",
        "worker-2",
        timedelta(minutes=1),
    ) as after_cancellation:
        assert after_cancellation


async def test_released_owner_context_cannot_mutate_after_file_takeover(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    registered = await repository.register_discovery(
        RemoteFile(
            retailer_id="fenced-owner-retailer",
            portal_id="fenced-owner-portal",
            protocol=SourceProtocol.FIXTURE,
            remote_id="fenced-owner-file",
            download_url="https://fixtures.invalid/fenced-owner-file",
            original_filename="fenced-owner-file.xml",
            document_type=DocumentType.PRICE_DELTA,
            compression=CompressionFormat.NONE,
            discovered_at=NOW,
        )
    )
    leases = PostgresLeaseManager(database)
    stale_call_started = asyncio.Event()
    allow_stale_call = asyncio.Event()

    async def stale_owner_call() -> None:
        stale_call_started.set()
        await allow_stale_call.wait()
        await repository.clear_staging(registered.source_file_id)

    async with leases.acquire(
        f"source-file:{registered.source_file_id}",
        "worker-1",
        timedelta(minutes=1),
    ) as first:
        assert first
        stale_owner = asyncio.create_task(stale_owner_call())
        await stale_call_started.wait()

    async with leases.acquire(
        f"source-file:{registered.source_file_id}",
        "worker-2",
        timedelta(minutes=1),
    ) as takeover:
        assert takeover
        allow_stale_call.set()
        with pytest.raises(InvalidRequestError, match="closed"):
            await stale_owner


async def test_archived_rediscovery_freezes_chronology_and_listing_evidence(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    original = RemoteFile(
        retailer_id="rediscovery-retailer",
        portal_id="rediscovery-portal",
        protocol=SourceProtocol.HTTPS,
        remote_id="stable-archived-identity",
        download_url="https://fixtures.invalid/file.xml?signature=first-secret",
        original_filename="PriceFull.xml",
        document_type=DocumentType.PRICE_FULL,
        compression=CompressionFormat.NONE,
        discovered_at=NOW,
        source_timestamp=NOW - timedelta(minutes=2),
        content_length=17,
        media_type="application/xml",
        etag="first-etag",
        last_modified=NOW - timedelta(minutes=3),
        response_metadata=(("listing-revision", "first"),),
    )
    registered = await repository.register_discovery(original)
    await repository.transition(
        registered.source_file_id,
        (IngestionStatus.DISCOVERED,),
        IngestionStatus.DOWNLOADING,
    )
    payload = b"archived-evidence"
    digest = hashlib.sha256(payload).hexdigest()
    await repository.record_archive(
        registered.source_file_id,
        ArchivedDownload(
            archive=ArchiveReceipt(
                content_sha256=digest,
                object_key=f"sha256/{digest}",
                content_length=len(payload),
                archived_at=NOW + timedelta(seconds=2),
                created=True,
            ),
            evidence=DownloadEvidence(
                started_at=NOW,
                finished_at=NOW + timedelta(seconds=2),
                status_code=200,
                content_length=len(payload),
                media_type="application/xml",
                etag="first-etag",
                last_modified=NOW - timedelta(minutes=3),
            ),
        ),
        parser_version="integration-v1",
    )
    rotated = replace(
        original,
        download_url="https://fixtures.invalid/file.xml?signature=rotated-secret",
        discovered_at=NOW + timedelta(hours=1),
    )

    repeated = await repository.register_discovery(rotated)

    assert repeated.status is IngestionStatus.ARCHIVED
    assert repeated.remote_file == original

    conflict = replace(rotated, source_timestamp=NOW + timedelta(minutes=5))
    with pytest.raises(PersistenceConflictError, match="source_timestamp"):
        await repository.register_discovery(conflict)
    after_conflict = await repository.get(registered.source_file_id)
    assert after_conflict.remote_file == repeated.remote_file


async def test_owned_prearchive_retry_can_rotate_only_a_signed_download_url(
    database: Database,
) -> None:
    repository = PostgresIngestionRepository(database.engine)
    original = RemoteFile(
        retailer_id="signed-retry-retailer",
        portal_id="signed-retry-portal",
        protocol=SourceProtocol.HTTPS,
        remote_id="signed-retry-identity",
        download_url="https://fixtures.invalid/file.xml?tenant=public&sig=first-secret",
        original_filename="Price.xml",
        document_type=DocumentType.PRICE_DELTA,
        compression=CompressionFormat.NONE,
        discovered_at=NOW,
        source_timestamp=NOW - timedelta(minutes=1),
    )
    registered = await repository.register_discovery(original)
    await repository.transition(
        registered.source_file_id,
        (IngestionStatus.DISCOVERED,),
        IngestionStatus.FAILED_RETRYABLE,
    )
    rotated = replace(
        original,
        download_url="https://fixtures.invalid/file.xml?tenant=public&sig=second-secret",
        discovered_at=NOW + timedelta(hours=1),
    )

    unowned = await repository.register_discovery(rotated)
    leases = PostgresLeaseManager(database)
    async with leases.acquire(
        f"source-file:{registered.source_file_id}",
        "signed-retry-owner",
        timedelta(minutes=1),
    ) as acquired:
        assert acquired
        owned = await repository.register_discovery(rotated, owned_refresh=True)

    assert unowned.remote_file == original
    assert owned.remote_file == replace(original, download_url=rotated.download_url)


async def test_new_locked_replay_closes_an_interrupted_open_attempt(database: Database) -> None:
    repository = PostgresIngestionRepository(database.engine)
    source_file_id = await _archived_file(
        repository,
        remote_id="interrupted-replay-owner",
        document_type=DocumentType.PRICE_DELTA,
    )
    interrupted = await repository.begin_replay(
        source_file_id,
        parser_version="integration-v1",
    )

    resumed = await repository.begin_replay(
        source_file_id,
        parser_version="integration-v2",
    )

    async with database.engine.connect() as connection:
        rows = (
            (
                await connection.execute(
                    select(
                        replay_runs.c.id,
                        replay_runs.c.status,
                        replay_runs.c.finished_at,
                        replay_runs.c.result_summary,
                    )
                    .where(replay_runs.c.source_file_id == source_file_id)
                    .order_by(replay_runs.c.started_at, replay_runs.c.id)
                )
            )
            .mappings()
            .all()
        )
    assert [row.id for row in rows] == [interrupted.replay_id, resumed.replay_id]
    assert rows[0].status == IngestionStatus.FAILED_RETRYABLE.value
    assert rows[0].finished_at is not None
    assert rows[0].result_summary["error_code"] == "interrupted_replay_recovered"
    assert rows[1].status == IngestionStatus.PARSING.value
    assert rows[1].finished_at is None


async def test_public_queries_bound_short_search_and_sanitize_latest_source_error(
    database: Database,
) -> None:
    ingestion = PostgresIngestionRepository(database.engine)
    registered = await ingestion.register_discovery(
        RemoteFile(
            retailer_id="public-status-retailer",
            portal_id="public-status-portal",
            protocol=SourceProtocol.FIXTURE,
            remote_id="public-status-failure",
            download_url="https://fixtures.invalid/status",
            original_filename="status.xml",
            document_type=DocumentType.STORES,
            compression=CompressionFormat.NONE,
            discovered_at=NOW,
        )
    )
    await ingestion.transition(
        registered.source_file_id,
        (IngestionStatus.DISCOVERED,),
        IngestionStatus.DOWNLOADING,
    )
    await ingestion.transition(
        registered.source_file_id,
        (IngestionStatus.DOWNLOADING,),
        IngestionStatus.FAILED_TERMINAL,
        error_code="unexpected_error",
        error_message="postgresql://operator:secret@internal.invalid/private",
    )

    queries = PostgresQueryRepository(database.engine)
    with pytest.raises(ValueError, match="at least 3"):
        await queries.search_products("ab", quantity=None, unit=None, limit=10, cursor=None)

    status = await queries.source_status(limit=10)
    item = next(row for row in status.items if row["portal_key"] == "public-status-portal")
    assert item["error_code"] == "unexpected_error"
    assert item["error_message"] == (
        "Ingestion did not complete; use source_file_id with operator logs for details"
    )
    assert "secret" not in str(item)
    assert "internal.invalid" not in str(item)


async def test_latest_source_status_plan_uses_bounded_portal_index(database: Database) -> None:
    ingestion = PostgresIngestionRepository(database.engine)
    registered = await ingestion.register_discovery(
        RemoteFile(
            retailer_id="plan-retailer",
            portal_id="plan-portal",
            protocol=SourceProtocol.FIXTURE,
            remote_id="latest-source",
            download_url="https://fixtures.invalid/latest",
            original_filename="latest.xml",
            document_type=DocumentType.STORES,
            compression=CompressionFormat.NONE,
            discovered_at=NOW,
        )
    )
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO source_files (
                    retailer_id, portal_id, remote_id, download_url,
                    original_filename, document_type, compression, protocol,
                    status, discovered_at
                )
                SELECT template.retailer_id, template.portal_id,
                       'historical-' || series.value,
                       'https://fixtures.invalid/historical/' || series.value,
                       'historical-' || series.value || '.xml',
                       'stores', 'none', 'fixture', 'discovered',
                       CAST(:now AS timestamptz) - make_interval(secs => series.value)
                  FROM source_files template
                  CROSS JOIN generate_series(1, 2000) AS series(value)
                 WHERE template.id = :source_file_id
                """
            ),
            {"now": NOW, "source_file_id": registered.source_file_id},
        )
        await connection.execute(text("ANALYZE source_files"))
        await connection.execute(text("SET LOCAL enable_seqscan = off"))
        plan = (
            await connection.execute(
                text(
                    """
                    EXPLAIN (FORMAT JSON)
                    SELECT portal.id, latest.id
                      FROM portals portal
                      LEFT JOIN LATERAL (
                          SELECT source.id
                            FROM source_files source
                           WHERE source.portal_id = portal.id
                           ORDER BY source.discovered_at DESC, source.id DESC
                           LIMIT 1
                      ) latest ON true
                     ORDER BY portal.id
                     LIMIT 10
                    """
                )
            )
        ).scalar_one()

    assert "ix_source_files_portal_latest" in str(plan)
