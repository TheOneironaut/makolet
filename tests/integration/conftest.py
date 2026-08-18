"""Real-PostgreSQL integration fixtures.

Set ``MAKOLET_TEST_DATABASE_URL`` to a dedicated loopback database named with the
``makolet_test_`` prefix and set ``MAKOLET_TEST_DATABASE_CONFIRM`` to that exact
database name. The suite refuses destructive setup without both values.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.destructive_target import (
    DestructiveDatabaseTargetError,
    require_test_database_target,
)
from makolet.adapters.persistence.schema import metadata


def _test_database_url() -> str:
    url = os.environ.get("MAKOLET_TEST_DATABASE_URL")
    if not url:
        pytest.skip("MAKOLET_TEST_DATABASE_URL is not configured")
    try:
        return require_test_database_target(
            url,
            confirmation=os.environ.get("MAKOLET_TEST_DATABASE_CONFIRM"),
        )
    except DestructiveDatabaseTargetError as error:
        pytest.fail(str(error))


async def _public_tables(url: str) -> set[str]:
    database = Database.from_url(url, pool_size=1, max_overflow=0)
    try:
        async with database.engine.connect() as connection:
            return await connection.run_sync(
                lambda sync_connection: set(inspect(sync_connection).get_table_names())
            )
    finally:
        await database.dispose()


async def _truncate_managed_tables(url: str, existing_tables: set[str]) -> None:
    """Remove prior test data before a downgrade can collapse business keys."""

    managed_names = [
        table.name for table in metadata.sorted_tables if table.name in existing_tables
    ]
    if not managed_names:
        return
    quoted_names = ", ".join(f'"{name}"' for name in managed_names)
    database = Database.from_url(url, pool_size=1, max_overflow=0)
    try:
        async with database.engine.begin() as connection:
            await connection.execute(text(f"TRUNCATE TABLE {quoted_names} CASCADE"))
    finally:
        await database.dispose()


@pytest.fixture(scope="session")
def migrated_database_url() -> str:
    url = _test_database_url()
    tables = asyncio.run(_public_tables(url))
    alembic_config = Config("alembic.ini")
    previous_url = os.environ.get("MAKOLET_DATABASE_URL")
    os.environ["MAKOLET_DATABASE_URL"] = url
    try:
        if "alembic_version" in tables:
            asyncio.run(_truncate_managed_tables(url, tables))
            command.downgrade(alembic_config, "base")
        elif tables:
            pytest.fail(
                "Dedicated test database contains unmanaged tables; refusing destructive setup"
            )
        command.upgrade(alembic_config, "head")
    finally:
        if previous_url is None:
            os.environ.pop("MAKOLET_DATABASE_URL", None)
        else:
            os.environ["MAKOLET_DATABASE_URL"] = previous_url
    return url


@pytest_asyncio.fixture
async def database(migrated_database_url: str) -> AsyncIterator[Database]:
    instance = Database.from_url(migrated_database_url, pool_size=2, max_overflow=1)
    try:
        table_names = [table.name for table in metadata.sorted_tables]
        quoted_names = ", ".join(f'"{name}"' for name in table_names)
        async with instance.engine.begin() as connection:
            await connection.execute(text(f"TRUNCATE TABLE {quoted_names} CASCADE"))
        yield instance
    finally:
        await instance.dispose()
