"""Fail-closed portal-identity migration contracts against PostgreSQL 18."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncConnection

from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.schema import metadata

pytestmark = pytest.mark.integration


async def _run_database_operation[ResultT](
    url: str,
    operation: Callable[[AsyncConnection], Awaitable[ResultT]],
) -> ResultT:
    database = Database.from_url(url, pool_size=1, max_overflow=0)
    try:
        async with database.engine.begin() as connection:
            return await operation(connection)
    finally:
        await database.dispose()


def _run_database[ResultT](
    url: str,
    operation: Callable[[AsyncConnection], Awaitable[ResultT]],
) -> ResultT:
    return asyncio.run(_run_database_operation(url, operation))


def _config() -> Config:
    return Config("alembic.ini")


@contextmanager
def _with_migration_url(url: str) -> Iterator[None]:
    previous = os.environ.get("MAKOLET_DATABASE_URL")
    os.environ["MAKOLET_DATABASE_URL"] = url
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("MAKOLET_DATABASE_URL", None)
        else:
            os.environ["MAKOLET_DATABASE_URL"] = previous


async def _revision(connection: AsyncConnection) -> str:
    result = await connection.execute(text("SELECT version_num FROM alembic_version"))
    return str(result.scalar_one())


async def _table_names(connection: AsyncConnection) -> set[str]:
    return await connection.run_sync(
        lambda sync_connection: set(inspect(sync_connection).get_table_names())
    )


async def _column_names(connection: AsyncConnection, table_name: str) -> set[str]:
    return await connection.run_sync(
        lambda sync_connection: {
            str(column["name"]) for column in inspect(sync_connection).get_columns(table_name)
        }
    )


async def _truncate_managed_tables(connection: AsyncConnection) -> None:
    names = ", ".join(f'"{table.name}"' for table in metadata.sorted_tables)
    await connection.execute(text(f"TRUNCATE TABLE {names} CASCADE"))
    await connection.execute(
        text(
            "INSERT INTO normalized_rebuild_control "
            "(singleton_id, active_rebuild_run_id) VALUES (1, NULL)"
        )
    )


def test_active_rebuild_refuses_online_upgrade_before_schema_mutation(
    migrated_database_url: str,
) -> None:
    url = migrated_database_url
    config = _config()
    _run_database(url, _truncate_managed_tables)
    with _with_migration_url(url):
        command.downgrade(config, "0003_normalized_rebuilds")

    async def seed_active_rebuild(connection: AsyncConnection) -> None:
        result = await connection.execute(
            text(
                """
                INSERT INTO normalized_rebuild_runs (
                    status, requested_by, requested_parser_version,
                    archive_cutoff_at
                ) VALUES ('running', 'migration-test', 'test/1', clock_timestamp())
                RETURNING id
                """
            )
        )
        rebuild_id = result.scalar_one()
        await connection.execute(
            text(
                """
                UPDATE normalized_rebuild_control
                   SET active_rebuild_run_id = :rebuild_id
                 WHERE singleton_id = 1
                """
            ),
            {"rebuild_id": rebuild_id},
        )

    _run_database(url, seed_active_rebuild)
    with _with_migration_url(url), pytest.raises(RuntimeError, match="active normalized rebuild"):
        command.upgrade(config, "head")

    assert _run_database(url, _revision) == "0003_normalized_rebuilds"
    assert "source_scope_watermarks" not in _run_database(url, _table_names)

    async def clear_active_rebuild(connection: AsyncConnection) -> None:
        await connection.execute(
            text(
                "UPDATE normalized_rebuild_control SET active_rebuild_run_id = NULL "
                "WHERE singleton_id = 1"
            )
        )

    _run_database(url, clear_active_rebuild)
    with _with_migration_url(url):
        command.upgrade(config, "head")


def test_ambiguous_legacy_portal_evidence_refuses_upgrade_without_ddl(
    migrated_database_url: str,
) -> None:
    url = migrated_database_url
    config = _config()
    _run_database(url, _truncate_managed_tables)
    with _with_migration_url(url):
        command.downgrade(config, "0005_catalog_matching")

    async def seed_ambiguous_evidence(connection: AsyncConnection) -> None:
        result = await connection.execute(
            text(
                """
                INSERT INTO retailers (source_key, display_name)
                VALUES ('ambiguous-retailer', 'Ambiguous Retailer')
                RETURNING id
                """
            )
        )
        retailer_id = result.scalar_one()
        await connection.execute(
            text(
                """
                INSERT INTO portals (retailer_id, source_key, protocol)
                VALUES (:retailer_id, 'ambiguous-a', 'fixture'),
                       (:retailer_id, 'ambiguous-b', 'fixture')
                """
            ),
            {"retailer_id": retailer_id},
        )
        portals = (
            (
                await connection.execute(
                    text(
                        "SELECT id FROM portals WHERE retailer_id = :retailer_id "
                        "ORDER BY source_key"
                    ),
                    {"retailer_id": retailer_id},
                )
            )
            .scalars()
            .all()
        )
        source_ids: list[object] = []
        for index, portal_id in enumerate(portals):
            result = await connection.execute(
                text(
                    """
                    INSERT INTO source_files (
                        retailer_id, portal_id, remote_id, download_url,
                        original_filename, document_type, compression,
                        protocol, status, discovered_at
                    ) VALUES (
                        :retailer_id, :portal_id, :remote_id, 'fixture:///ambiguous',
                        'ambiguous.xml', 'price_full', 'none', 'fixture',
                        'completed', clock_timestamp()
                    ) RETURNING id
                    """
                ),
                {
                    "retailer_id": retailer_id,
                    "portal_id": portal_id,
                    "remote_id": f"ambiguous-{index}",
                },
            )
            source_ids.append(result.scalar_one())
        result = await connection.execute(
            text(
                """
                INSERT INTO retailer_items (
                    retailer_id, source_item_code, name,
                    first_seen_at, last_seen_at, last_source_file_id
                ) VALUES (
                    :retailer_id, 'AMBIGUOUS', 'Ambiguous item',
                    clock_timestamp(), clock_timestamp(), :source_file_id
                ) RETURNING id
                """
            ),
            {"retailer_id": retailer_id, "source_file_id": source_ids[0]},
        )
        item_id = result.scalar_one()
        await connection.execute(
            text(
                """
                INSERT INTO retailer_identifier_assertions (
                    retailer_item_id, kind, value, normalized_value,
                    source_file_id, validation_method, asserted_at
                ) VALUES (
                    :item_id, 'gtin', '4006381333931', '4006381333931',
                    :source_file_id, 'migration-test', clock_timestamp()
                )
                """
            ),
            {"item_id": item_id, "source_file_id": source_ids[1]},
        )

    _run_database(url, seed_ambiguous_evidence)
    with _with_migration_url(url), pytest.raises(RuntimeError, match="spans multiple portals"):
        command.upgrade(config, "head")

    assert _run_database(url, _revision) == "0005_catalog_matching"
    assert "portal_id" not in _run_database(
        url, lambda connection: _column_names(connection, "retailer_items")
    )

    async def clear_ambiguous_evidence(connection: AsyncConnection) -> None:
        await connection.execute(text("TRUNCATE TABLE retailers CASCADE"))

    _run_database(url, clear_ambiguous_evidence)
    with _with_migration_url(url):
        command.upgrade(config, "head")


def test_portal_collision_refuses_downgrade_without_schema_or_data_mutation(
    migrated_database_url: str,
) -> None:
    url = migrated_database_url
    config = _config()
    _run_database(url, _truncate_managed_tables)
    starting_revision = _run_database(url, _revision)

    async def seed_portal_collision(connection: AsyncConnection) -> None:
        result = await connection.execute(
            text(
                "INSERT INTO retailers (source_key, display_name) "
                "VALUES ('collision-retailer', 'Collision Retailer') RETURNING id"
            )
        )
        retailer_id = result.scalar_one()
        await connection.execute(
            text(
                """
                INSERT INTO portals (retailer_id, source_key, protocol)
                VALUES (:retailer_id, 'collision-a', 'fixture'),
                       (:retailer_id, 'collision-b', 'fixture')
                """
            ),
            {"retailer_id": retailer_id},
        )
        portals = (
            (
                await connection.execute(
                    text(
                        "SELECT id FROM portals WHERE retailer_id = :retailer_id "
                        "ORDER BY source_key"
                    ),
                    {"retailer_id": retailer_id},
                )
            )
            .scalars()
            .all()
        )
        for index, portal_id in enumerate(portals):
            result = await connection.execute(
                text(
                    """
                    INSERT INTO source_files (
                        retailer_id, portal_id, remote_id, download_url,
                        original_filename, document_type, compression,
                        protocol, status, discovered_at
                    ) VALUES (
                        :retailer_id, :portal_id, :remote_id, 'fixture:///collision',
                        'collision.xml', 'price_full', 'none', 'fixture',
                        'completed', clock_timestamp()
                    ) RETURNING id
                    """
                ),
                {
                    "retailer_id": retailer_id,
                    "portal_id": portal_id,
                    "remote_id": f"collision-{index}",
                },
            )
            source_id = result.scalar_one()
            await connection.execute(
                text(
                    """
                    INSERT INTO retailer_items (
                        retailer_id, portal_id, source_item_code, name,
                        first_seen_at, last_seen_at, last_source_file_id
                    ) VALUES (
                        :retailer_id, :portal_id, 'COLLIDE', 'Collision item',
                        clock_timestamp(), clock_timestamp(), :source_file_id
                    )
                    """
                ),
                {
                    "retailer_id": retailer_id,
                    "portal_id": portal_id,
                    "source_file_id": source_id,
                },
            )

    _run_database(url, seed_portal_collision)
    with _with_migration_url(url), pytest.raises(RuntimeError, match="cross-portal retailer_item"):
        command.downgrade(config, "0005_catalog_matching")

    assert _run_database(url, _revision) == starting_revision
    assert "portal_id" in _run_database(
        url, lambda connection: _column_names(connection, "retailer_items")
    )

    async def collision_count(connection: AsyncConnection) -> int:
        result = await connection.execute(
            text(
                """
                SELECT count(*) FROM retailer_items item
                  JOIN retailers retailer ON retailer.id = item.retailer_id
                 WHERE retailer.source_key = 'collision-retailer'
                   AND item.source_item_code = 'COLLIDE'
                """
            )
        )
        return int(result.scalar_one())

    assert _run_database(url, collision_count) == 2


def test_bounded_query_projection_upgrade_backfills_and_downgrades_cleanly(
    migrated_database_url: str,
) -> None:
    url = migrated_database_url
    config = _config()
    _run_database(url, _truncate_managed_tables)
    with _with_migration_url(url):
        command.downgrade(config, "0009_collection_charge_budgets")

    async def seed_pre_projection_rows(connection: AsyncConnection) -> None:
        await connection.execute(
            text(
                """
                INSERT INTO retailers (id, source_key, display_name)
                VALUES (
                    '91000000-0000-0000-0000-000000000001',
                    'projection-migration-retailer', 'Projection Migration Retailer'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO portals (id, retailer_id, source_key, protocol)
                VALUES (
                    '92000000-0000-0000-0000-000000000001',
                    '91000000-0000-0000-0000-000000000001',
                    'projection-migration-portal', 'fixture'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO source_files (
                    id, retailer_id, portal_id, remote_id, download_url,
                    original_filename, document_type, compression,
                    protocol, status, discovered_at
                ) VALUES (
                    '93000000-0000-0000-0000-000000000001',
                    '91000000-0000-0000-0000-000000000001',
                    '92000000-0000-0000-0000-000000000001',
                    'projection-migration-source', 'fixture:///projection',
                    'projection.xml', 'price_full', 'none', 'fixture',
                    'completed', '2026-08-11T12:00:00Z'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO stores (
                    id, retailer_id, portal_id, chain_code, subchain_code,
                    source_store_code, name, city, last_source_file_id
                ) VALUES (
                    '94000000-0000-0000-0000-000000000001',
                    '91000000-0000-0000-0000-000000000001',
                    '92000000-0000-0000-0000-000000000001',
                    'chain', 'subchain', '001', 'Projection Store',
                    '  Jerusalem  ',
                    '93000000-0000-0000-0000-000000000001'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO retailer_items (
                    id, retailer_id, portal_id, source_item_code, name,
                    first_seen_at, last_seen_at, last_source_file_id
                ) VALUES (
                    '95000000-0000-0000-0000-000000000001',
                    '91000000-0000-0000-0000-000000000001',
                    '92000000-0000-0000-0000-000000000001',
                    'ITEM-1', 'Projection Item',
                    '2026-08-11T12:00:00Z', '2026-08-11T12:00:00Z',
                    '93000000-0000-0000-0000-000000000001'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO canonical_products (id, name, status)
                VALUES (
                    '96000000-0000-0000-0000-000000000001',
                    'Projection Product', 'active'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO confirmed_product_matches (
                    retailer_item_id, canonical_product_id, method, confirmed_by
                ) VALUES (
                    '95000000-0000-0000-0000-000000000001',
                    '96000000-0000-0000-0000-000000000001',
                    'manual_review', 'migration-test'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO current_prices (
                    retailer_item_id, store_id, item_price, source_file_id,
                    first_observed_at, last_observed_at
                ) VALUES (
                    '95000000-0000-0000-0000-000000000001',
                    '94000000-0000-0000-0000-000000000001', 12.34,
                    '93000000-0000-0000-0000-000000000001',
                    '2026-08-11T12:00:00Z', '2026-08-11T12:00:00Z'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO price_history (
                    retailer_item_id, store_id, item_price, source_file_id,
                    valid_from, valid_to
                ) VALUES (
                    '95000000-0000-0000-0000-000000000001',
                    '94000000-0000-0000-0000-000000000001', 12.34,
                    '93000000-0000-0000-0000-000000000001',
                    '2026-08-11T12:00:00Z', NULL
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO current_availability (
                    retailer_item_id, store_id, is_available, source_file_id,
                    first_observed_at, last_observed_at
                ) VALUES (
                    '95000000-0000-0000-0000-000000000001',
                    '94000000-0000-0000-0000-000000000001', true,
                    '93000000-0000-0000-0000-000000000001',
                    '2026-08-11T12:00:00Z', '2026-08-11T12:00:00Z'
                )
                """
            )
        )

    _run_database(url, seed_pre_projection_rows)
    with _with_migration_url(url):
        command.upgrade(config, "head")

    async def projection_values(connection: AsyncConnection) -> tuple[object, ...]:
        row = (
            await connection.execute(
                text(
                    """
                    SELECT price.canonical_product_id,
                           price.query_retailer_id,
                           history.canonical_product_id AS history_product_id,
                           availability.canonical_product_id AS availability_product_id,
                           store.city_search
                      FROM current_prices price
                      JOIN price_history history
                        ON history.retailer_item_id = price.retailer_item_id
                       AND history.store_id = price.store_id
                      JOIN current_availability availability
                        ON availability.retailer_item_id = price.retailer_item_id
                       AND availability.store_id = price.store_id
                      JOIN stores store ON store.id = price.store_id
                    """
                )
            )
        ).one()
        return tuple(row)

    projected = _run_database(url, projection_values)
    assert tuple(str(value) for value in projected[:4]) == (
        "96000000-0000-0000-0000-000000000001",
        "91000000-0000-0000-0000-000000000001",
        "96000000-0000-0000-0000-000000000001",
        "96000000-0000-0000-0000-000000000001",
    )
    assert projected[4] == "jerusalem"

    with _with_migration_url(url):
        command.downgrade(config, "0009_collection_charge_budgets")
    assert "canonical_product_id" not in _run_database(
        url,
        lambda connection: _column_names(connection, "current_prices"),
    )
    with _with_migration_url(url):
        command.upgrade(config, "head")


def test_resource_probe_upgrade_backfills_bounded_summaries_and_buckets(
    migrated_database_url: str,
) -> None:
    url = migrated_database_url
    config = _config()
    _run_database(url, _truncate_managed_tables)
    with _with_migration_url(url):
        command.downgrade(config, "0010_bounded_query_paths")

    issue_count = 10_005

    async def seed_legacy_rows(connection: AsyncConnection) -> None:
        await connection.execute(
            text(
                """
                INSERT INTO retailers (id, source_key, display_name)
                VALUES (
                    'a1000000-0000-0000-0000-000000000001',
                    'resource-probe-migration', 'Resource Probe Migration'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO portals (id, retailer_id, source_key, protocol)
                VALUES (
                    'a2000000-0000-0000-0000-000000000001',
                    'a1000000-0000-0000-0000-000000000001',
                    'resource-probe-portal', 'fixture'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO source_files (
                    id, retailer_id, portal_id, remote_id, download_url,
                    original_filename, document_type, compression,
                    protocol, status, discovered_at
                ) VALUES (
                    'a3000000-0000-0000-0000-000000000001',
                    'a1000000-0000-0000-0000-000000000001',
                    'a2000000-0000-0000-0000-000000000001',
                    'resource-probe-source', 'fixture:///resource-probe',
                    'resource-probe.xml', 'price_full', 'none', 'fixture',
                    'completed', clock_timestamp()
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO ingestion_runs (
                    id, source_file_id, attempt, status, finished_at
                ) VALUES (
                    'a4000000-0000-0000-0000-000000000001',
                    'a3000000-0000-0000-0000-000000000001',
                    1, 'completed', clock_timestamp()
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO validation_issues (
                    source_file_id, ingestion_run_id, severity,
                    code, message, record_index, created_at
                )
                SELECT 'a3000000-0000-0000-0000-000000000001',
                       'a4000000-0000-0000-0000-000000000001',
                       'file_quarantine', 'c', 'm', series.value,
                       clock_timestamp() + series.value * INTERVAL '1 microsecond'
                  FROM generate_series(1, :issue_count) AS series(value)
                """
            ),
            {"issue_count": issue_count},
        )
        await connection.execute(
            text(
                """
                INSERT INTO collection_checkpoints (
                    id, retailer_id, portal_ids, portal_generation,
                    operation
                ) VALUES (
                    'a5000000-0000-0000-0000-000000000001',
                    'a1000000-0000-0000-0000-000000000001',
                    '["resource-probe-portal"]'::jsonb, repeat('a', 64),
                    'ordinary'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO collection_attempts (
                    id, checkpoint_id, generation, status,
                    started_at, finished_at,
                    start_page_offset, checkpoint_page_offset
                ) VALUES (
                    'a6000000-0000-0000-0000-000000000001',
                    'a5000000-0000-0000-0000-000000000001',
                    1, 'completed', clock_timestamp(), clock_timestamp(), 0, 0
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO collection_charge_budgets (
                    retailer_id, window_started_at, charged_bytes
                ) VALUES (
                    'a1000000-0000-0000-0000-000000000001',
                    clock_timestamp() - INTERVAL '24 hours', 0
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO collection_transfer_charges (
                    attempt_id, source_file_id, retailer_id,
                    content_length, settled, charged_at
                ) VALUES (
                    'a6000000-0000-0000-0000-000000000001',
                    'a3000000-0000-0000-0000-000000000001',
                    'a1000000-0000-0000-0000-000000000001',
                    11, true, clock_timestamp() - INTERVAL '10 minutes'
                )
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO collection_archive_charges (
                    source_file_id, retailer_id, attempt_id,
                    content_length, charged_at
                ) VALUES (
                    'a3000000-0000-0000-0000-000000000001',
                    'a1000000-0000-0000-0000-000000000001',
                    'a6000000-0000-0000-0000-000000000001',
                    13, clock_timestamp() - INTERVAL '5 minutes'
                )
                """
            )
        )

    _run_database(url, seed_legacy_rows)
    with _with_migration_url(url):
        command.upgrade(config, "head")

    async def migrated_values(connection: AsyncConnection) -> tuple[object, ...]:
        return tuple(
            (
                await connection.execute(
                    text(
                        """
                        SELECT
                            (SELECT version_num FROM alembic_version),
                            run.file_quarantine_issues,
                            run.validation_issue_bytes,
                            run.validation_issue_samples,
                            (SELECT count(*) FROM validation_issues),
                            budget.charged_bytes,
                            budget.identity_count,
                            budget.attempt_count,
                            budget.success_count,
                            (SELECT sum(charged_bytes)
                               FROM collection_budget_buckets),
                            (SELECT count(*)
                               FROM collection_identity_observations),
                            EXISTS (
                                SELECT 1 FROM pg_available_extensions
                                 WHERE name = 'btree_gist'
                            ),
                            EXISTS (
                                SELECT 1 FROM pg_extension
                                 WHERE extname = 'btree_gist'
                            ),
                            (
                                SELECT role.rolsuper OR has_database_privilege(
                                    current_user, current_database(), 'CREATE'
                                )
                                  FROM pg_roles role
                                 WHERE role.rolname = current_user
                            )
                          FROM ingestion_runs run
                          JOIN collection_charge_budgets budget
                            ON budget.retailer_id =
                               'a1000000-0000-0000-0000-000000000001'
                         WHERE run.id = 'a4000000-0000-0000-0000-000000000001'
                        """
                    )
                )
            ).one()
        )

    values = _run_database(url, migrated_values)
    assert values == (
        "0011_resource_probe_budgets",
        issue_count,
        issue_count * 66,
        1_000,
        1_000,
        24,
        1,
        1,
        1,
        24,
        1,
        True,
        True,
        True,
    )

    _run_database(url, _truncate_managed_tables)
    with _with_migration_url(url):
        command.downgrade(config, "0010_bounded_query_paths")
    assert _run_database(url, _revision) == "0010_bounded_query_paths"
    with _with_migration_url(url):
        command.upgrade(config, "head")
