"""Alembic environment for the asyncpg production engine."""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, text

from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.schema import metadata
from makolet.config import load_settings

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = metadata


def _database_url() -> str:
    return load_settings().database_dsn()


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_server_default=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def _run_sync_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_server_default=True,
        compare_type=True,
    )
    _assert_no_active_rebuild_before_migration(connection)
    with context.begin_transaction():
        context.run_migrations()


def _assert_no_active_rebuild_before_migration(connection: Connection) -> None:
    """Refuse online schema changes while normalized state is partially rebuilt."""

    migration_context = context.get_context()
    if migration_context.as_sql or migration_context.opts.get("dont_mutate") is True:
        return
    control_exists = connection.execute(
        text("SELECT to_regclass(:qualified_name)"),
        {"qualified_name": "public.normalized_rebuild_control"},
    ).scalar_one()
    if control_exists is None:
        return
    active_rebuild = connection.execute(
        text(
            """
            SELECT active_rebuild_run_id
              FROM normalized_rebuild_control
             WHERE singleton_id = :singleton_id
            """
        ),
        {"singleton_id": 1},
    ).scalar_one_or_none()
    if active_rebuild is not None:
        raise RuntimeError(
            "An active normalized rebuild must be completed or recovered before "
            "running an online database migration"
        )


async def run_migrations_online() -> None:
    database = Database.unpooled_from_url(
        _database_url(),
        application_name="makolet-migrations",
    )
    try:
        async with database.engine.connect() as connection:
            await connection.run_sync(_run_sync_migrations)
    finally:
        await database.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
