"""Crash-releasing PostgreSQL session locks for whole-file ingestion ownership."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from makolet.adapters.persistence.database import Database

MAXIMUM_LEASE_TTL = timedelta(hours=24)


@dataclass(frozen=True, slots=True)
class _IngestionLockSession:
    resource: str
    connection: AsyncConnection


_CURRENT_INGESTION_LOCK: ContextVar[_IngestionLockSession | None] = ContextVar(
    "makolet_current_ingestion_lock",
    default=None,
)


def current_ingestion_lock() -> _IngestionLockSession | None:
    """Return the lock session inherited by repository calls in the owning task."""

    return _CURRENT_INGESTION_LOCK.get()


class PostgresLeaseManager:
    """Hold one session advisory lock until file ingestion or replay exits."""

    def __init__(self, database: Database) -> None:
        self._engine: AsyncEngine = database.create_unpooled_engine(
            application_name="makolet-ingestion-lock",
        )

    @asynccontextmanager
    async def acquire(
        self,
        resource: str,
        owner: str,
        ttl: timedelta,
    ) -> AsyncIterator[bool]:
        if not resource or len(resource) > 512:
            raise ValueError("resource must contain at most 512 characters")
        if not owner or len(owner) > 256:
            raise ValueError("owner must contain at most 256 characters")
        if ttl <= timedelta(0) or ttl > MAXIMUM_LEASE_TTL:
            raise ValueError("ttl must be positive and no longer than 24 hours")
        del owner, ttl
        lock_resource = f"makolet:ingestion-file:{resource}"
        async with self._engine.connect() as connection:
            acquired = bool(
                (
                    await connection.execute(
                        text("SELECT pg_try_advisory_lock(hashtextextended(:resource, 0))"),
                        {"resource": lock_resource},
                    )
                ).scalar_one()
            )
            lock_context = None
            if acquired:
                # End the implicit transaction while retaining the session lock.
                # Repository transactions then use this exact connection, so a lost
                # lock session cannot continue staging/apply through another pool.
                await connection.commit()
                lock_context = _CURRENT_INGESTION_LOCK.set(
                    _IngestionLockSession(resource=resource, connection=connection)
                )
            try:
                yield acquired
            finally:
                if acquired:
                    if lock_context is None:
                        raise AssertionError("Acquired ingestion lock omitted its context")
                    _CURRENT_INGESTION_LOCK.reset(lock_context)
                    unlock = asyncio.create_task(
                        connection.execute(
                            text("SELECT pg_advisory_unlock(hashtextextended(:resource, 0))"),
                            {"resource": lock_resource},
                        )
                    )
                    try:
                        await asyncio.shield(unlock)
                    except asyncio.CancelledError:
                        await unlock
                        raise
                    except BaseException:
                        await connection.invalidate()
                        raise
