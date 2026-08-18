"""Durable, portal-scoped discovery traversal checkpoints and attempt audit."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID

from sqlalchemy import delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.errors import (
    PersistenceConflictError,
    PersistenceNotFoundError,
)
from makolet.adapters.persistence.schema import (
    collection_archive_charges,
    collection_attempts,
    collection_budget_buckets,
    collection_charge_budgets,
    collection_checkpoints,
    collection_identity_observations,
    collection_transfer_charges,
    portals,
    raw_archive_objects,
    retailers,
    source_files,
)
from makolet.application.models import (
    CollectionAttempt,
    CollectionChargeBudget,
    CollectionScope,
)
from makolet.domain.enums import IngestionStatus
from makolet.domain.models import RemoteFile

_MAXIMUM_CURSOR_BYTES = 8_192
_MAXIMUM_LEASE_TTL = timedelta(hours=24)
_ARCHIVE_BUDGET_WINDOW = timedelta(hours=24)
_BUDGET_BUCKET_WIDTH = timedelta(minutes=5)
_TRUNCATION_REASONS = frozenset(
    {
        "file_limit",
        "discovery_limit",
        "charged_byte_run_limit",
        "charged_byte_day_limit",
        "identity_day_limit",
        "attempt_day_limit",
        "success_day_limit",
        "legacy_limit",
    }
)
_TERMINAL_INGESTION_STATUSES = (
    IngestionStatus.COMPLETED.value,
    IngestionStatus.QUARANTINED.value,
    IngestionStatus.FAILED_TERMINAL.value,
)


class PostgresCollectionRepository:
    """Advance one source traversal only across durable retry-safe boundaries."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def begin_attempt(self, scope: CollectionScope) -> CollectionAttempt:
        async with self._engine.begin() as connection:
            retailer_id = await self._retailer_id(connection, scope)
            await self._validate_portals(connection, retailer_id, scope.portal_ids)
            resource = f"{retailer_id}:{scope.portal_generation}:{scope.operation}:"
            await connection.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:resource, 0))"),
                {"resource": f"makolet:collection-checkpoint:{resource}"},
            )
            await connection.execute(
                pg_insert(collection_checkpoints)
                .values(
                    retailer_id=retailer_id,
                    portal_ids=list(scope.portal_ids),
                    portal_generation=scope.portal_generation,
                    operation=scope.operation,
                    range_since=scope.since,
                    range_until=scope.until,
                    archive_only=scope.archive_only,
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        collection_checkpoints.c.retailer_id,
                        collection_checkpoints.c.portal_generation,
                        collection_checkpoints.c.operation,
                        collection_checkpoints.c.range_since,
                        collection_checkpoints.c.range_until,
                        collection_checkpoints.c.archive_only,
                    ]
                )
            )
            checkpoint = (
                (
                    await connection.execute(
                        select(collection_checkpoints)
                        .where(
                            collection_checkpoints.c.retailer_id == retailer_id,
                            collection_checkpoints.c.portal_generation == scope.portal_generation,
                            collection_checkpoints.c.operation == scope.operation,
                            collection_checkpoints.c.range_since.is_not_distinct_from(scope.since),
                            collection_checkpoints.c.range_until.is_not_distinct_from(scope.until),
                            collection_checkpoints.c.archive_only == scope.archive_only,
                        )
                        .with_for_update()
                    )
                )
                .mappings()
                .one()
            )
            if tuple(checkpoint.portal_ids) != scope.portal_ids:
                raise PersistenceConflictError(
                    "Collection checkpoint portal scope does not match its generation"
                )
            rolled_generation = bool(checkpoint.traversal_complete)
            if rolled_generation:
                checkpoint = (
                    (
                        await connection.execute(
                            update(collection_checkpoints)
                            .where(collection_checkpoints.c.id == checkpoint.id)
                            .values(
                                generation=collection_checkpoints.c.generation + 1,
                                publisher_cursor=None,
                                page_offset=0,
                                generation_recognized_count=0,
                                generation_unknown_count=0,
                                traversal_complete=False,
                                updated_at=func.clock_timestamp(),
                            )
                            .returning(collection_checkpoints)
                        )
                    )
                    .mappings()
                    .one()
                )
            retry_boundary = False
            if not rolled_generation:
                prior_attempt = (
                    (
                        await connection.execute(
                            select(
                                collection_attempts.c.status,
                                collection_attempts.c.checkpoint_cursor,
                                collection_attempts.c.checkpoint_page_offset,
                            )
                            .where(
                                collection_attempts.c.checkpoint_id == checkpoint.id,
                                collection_attempts.c.generation == checkpoint.generation,
                            )
                            .order_by(
                                collection_attempts.c.started_at.desc(),
                                collection_attempts.c.id.desc(),
                            )
                            .limit(1)
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                retry_boundary = bool(
                    prior_attempt is not None
                    and prior_attempt.status in {"running", "failed"}
                    and prior_attempt.checkpoint_cursor == checkpoint.publisher_cursor
                    and int(prior_attempt.checkpoint_page_offset) == int(checkpoint.page_offset)
                )
            await connection.execute(
                update(collection_attempts)
                .where(
                    collection_attempts.c.checkpoint_id == checkpoint.id,
                    collection_attempts.c.status == "running",
                )
                .values(
                    status="failed",
                    finished_at=func.clock_timestamp(),
                    error_code="stale_collection_attempt",
                    error_message=(
                        "A prior collection attempt ended without a durable finish record"
                    ),
                )
            )
            attempt = (
                await connection.execute(
                    pg_insert(collection_attempts)
                    .values(
                        checkpoint_id=checkpoint.id,
                        generation=checkpoint.generation,
                        start_cursor=checkpoint.publisher_cursor,
                        start_page_offset=checkpoint.page_offset,
                        checkpoint_cursor=checkpoint.publisher_cursor,
                        checkpoint_page_offset=checkpoint.page_offset,
                        charged_bytes=0,
                    )
                    .returning(collection_attempts.c.id)
                )
            ).scalar_one()
            return CollectionAttempt(
                attempt_id=attempt,
                checkpoint_id=checkpoint.id,
                generation=int(checkpoint.generation),
                cursor=checkpoint.publisher_cursor,
                page_offset=int(checkpoint.page_offset),
                generation_recognized_count=int(checkpoint.generation_recognized_count),
                generation_unknown_count=int(checkpoint.generation_unknown_count),
                retry_boundary=retry_boundary,
            )

    async def advance_attempt(
        self,
        attempt_id: UUID,
        *,
        expected_cursor: str | None,
        expected_page_offset: int,
        cursor: str | None,
        page_offset: int,
        discovered_delta: int = 0,
        recognized_delta: int = 0,
        unknown_delta: int = 0,
        processed_delta: int = 0,
        duplicate_delta: int = 0,
    ) -> CollectionAttempt:
        _validate_progress(
            cursor=cursor,
            page_offset=page_offset,
            deltas=(
                discovered_delta,
                recognized_delta,
                unknown_delta,
                processed_delta,
                duplicate_delta,
            ),
        )
        async with self._engine.begin() as connection:
            attempt, checkpoint = await self._locked_attempt(connection, attempt_id)
            if attempt.status != "running":
                raise PersistenceConflictError("Collection attempt is no longer running")
            if (
                attempt.checkpoint_cursor != expected_cursor
                or int(attempt.checkpoint_page_offset) != expected_page_offset
                or checkpoint.publisher_cursor != expected_cursor
                or int(checkpoint.page_offset) != expected_page_offset
                or int(attempt.generation) != int(checkpoint.generation)
            ):
                raise PersistenceConflictError(
                    "Collection checkpoint changed before the expected boundary committed"
                )
            updated_checkpoint = (
                (
                    await connection.execute(
                        update(collection_checkpoints)
                        .where(collection_checkpoints.c.id == checkpoint.id)
                        .values(
                            publisher_cursor=cursor,
                            page_offset=page_offset,
                            generation_recognized_count=(
                                collection_checkpoints.c.generation_recognized_count
                                + recognized_delta
                            ),
                            generation_unknown_count=(
                                collection_checkpoints.c.generation_unknown_count + unknown_delta
                            ),
                            updated_at=func.clock_timestamp(),
                        )
                        .returning(collection_checkpoints)
                    )
                )
                .mappings()
                .one()
            )
            await connection.execute(
                update(collection_attempts)
                .where(collection_attempts.c.id == attempt_id)
                .values(
                    checkpoint_cursor=cursor,
                    checkpoint_page_offset=page_offset,
                    discovered_count=collection_attempts.c.discovered_count + discovered_delta,
                    processed_count=collection_attempts.c.processed_count + processed_delta,
                    duplicate_count=collection_attempts.c.duplicate_count + duplicate_delta,
                    skipped_unknown_count=(
                        collection_attempts.c.skipped_unknown_count + unknown_delta
                    ),
                    warning_count=collection_attempts.c.warning_count + unknown_delta,
                )
            )
            return CollectionAttempt(
                attempt_id=attempt_id,
                checkpoint_id=checkpoint.id,
                generation=int(checkpoint.generation),
                cursor=cursor,
                page_offset=page_offset,
                generation_recognized_count=int(updated_checkpoint.generation_recognized_count),
                generation_unknown_count=int(updated_checkpoint.generation_unknown_count),
            )

    async def finish_attempt(
        self,
        attempt_id: UUID,
        *,
        status: str,
        truncated: bool,
        traversal_complete: bool,
        truncation_reason: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if status not in {"completed", "bounded", "failed"}:
            raise ValueError("collection finish status is invalid")
        if error_code is not None and not 1 <= len(error_code) <= 128:
            raise ValueError("collection error code is invalid")
        if truncated != (truncation_reason is not None):
            raise ValueError("collection truncation reason must match truncated state")
        if truncation_reason is not None and truncation_reason not in _TRUNCATION_REASONS:
            raise ValueError("collection truncation reason is invalid")
        async with self._engine.begin() as connection:
            attempt, checkpoint = await self._locked_attempt(connection, attempt_id)
            if attempt.status != "running":
                raise PersistenceConflictError("Collection attempt is no longer running")
            await connection.execute(
                update(collection_attempts)
                .where(collection_attempts.c.id == attempt_id)
                .values(
                    status=status,
                    finished_at=func.clock_timestamp(),
                    truncated=truncated,
                    truncation_reason=truncation_reason,
                    error_code=error_code,
                    error_message=error_message,
                )
            )
            await connection.execute(
                update(collection_checkpoints)
                .where(collection_checkpoints.c.id == checkpoint.id)
                .values(
                    traversal_complete=traversal_complete,
                    last_completed_at=(
                        func.clock_timestamp()
                        if traversal_complete and status == "completed"
                        else collection_checkpoints.c.last_completed_at
                    ),
                    updated_at=func.clock_timestamp(),
                )
            )

    async def observe_attempt(
        self,
        attempt_id: UUID,
        *,
        discovered_delta: int,
    ) -> None:
        if discovered_delta < 0:
            raise ValueError("collection attempt observation cannot be negative")
        async with self._engine.begin() as connection:
            observed = (
                await connection.execute(
                    update(collection_attempts)
                    .where(
                        collection_attempts.c.id == attempt_id,
                        collection_attempts.c.status == "running",
                    )
                    .values(
                        discovered_count=(collection_attempts.c.discovered_count + discovered_delta)
                    )
                    .returning(collection_attempts.c.id)
                )
            ).scalar_one_or_none()
        if observed is None:
            raise PersistenceConflictError("Collection attempt is no longer running")

    async def is_terminal(self, remote_file: RemoteFile, *, archive_only: bool) -> bool:
        async with self._engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(
                            source_files.c.status,
                            source_files.c.raw_archive_object_id,
                        )
                        .join(portals, portals.c.id == source_files.c.portal_id)
                        .join(retailers, retailers.c.id == source_files.c.retailer_id)
                        .where(
                            retailers.c.source_key == remote_file.retailer_id,
                            portals.c.source_key == remote_file.portal_id,
                            source_files.c.remote_id == remote_file.remote_id,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return False
        return str(row.status) in _TERMINAL_INGESTION_STATUSES or (
            archive_only and row.raw_archive_object_id is not None
        )

    async def note_terminal(self, remote_file: RemoteFile, *, archive_only: bool) -> None:
        if not await self.is_terminal(remote_file, archive_only=archive_only):
            raise PersistenceConflictError(
                "Collection cannot advance before source-file state is durably terminal"
            )

    async def charge_budget(self, attempt_id: UUID) -> CollectionChargeBudget:
        async with self._engine.begin() as connection:
            attempt, checkpoint = await self._locked_attempt(connection, attempt_id)
            if attempt.status != "running":
                raise PersistenceConflictError("Collection attempt is no longer running")
            budget = await self._locked_charge_budget(
                connection,
                UUID(str(checkpoint.retailer_id)),
            )
            return _charge_budget_value(attempt, budget)

    async def reserve_transfer(
        self,
        attempt_id: UUID,
        source_file_id: UUID,
        remote_file: RemoteFile,
        content_length: int,
    ) -> CollectionChargeBudget:
        if content_length < 0:
            raise ValueError("reserved transfer bytes cannot be negative")
        async with self._engine.begin() as connection:
            attempt, checkpoint = await self._locked_attempt(connection, attempt_id)
            if attempt.status != "running":
                raise PersistenceConflictError("Collection attempt is no longer running")
            retailer_id = UUID(str(checkpoint.retailer_id))
            budget = await self._locked_charge_budget(connection, retailer_id)
            retailer_source_key = (
                await connection.execute(
                    select(retailers.c.source_key).where(retailers.c.id == retailer_id)
                )
            ).scalar_one()
            if remote_file.retailer_id != retailer_source_key:
                raise PersistenceConflictError(
                    "Collection archive charge is outside the attempt source scope"
                )
            charged_at = (await connection.execute(select(func.clock_timestamp()))).scalar_one()
            source = (
                (
                    await connection.execute(
                        select(
                            source_files.c.id,
                        )
                        .join(portals, portals.c.id == source_files.c.portal_id)
                        .where(
                            source_files.c.retailer_id == retailer_id,
                            source_files.c.id == source_file_id,
                            portals.c.source_key == remote_file.portal_id,
                            source_files.c.remote_id == remote_file.remote_id,
                        )
                        .with_for_update(of=source_files)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if source is None:
                raise PersistenceConflictError(
                    "Collection archive charge source file was not registered"
                )
            charged = (
                await connection.execute(
                    pg_insert(collection_transfer_charges)
                    .values(
                        attempt_id=attempt_id,
                        source_file_id=source.id,
                        retailer_id=retailer_id,
                        content_length=content_length,
                        charged_at=charged_at,
                    )
                    .on_conflict_do_nothing(
                        index_elements=[
                            collection_transfer_charges.c.attempt_id,
                            collection_transfer_charges.c.source_file_id,
                        ]
                    )
                    .returning(collection_transfer_charges.c.source_file_id)
                )
            ).scalar_one_or_none()
            if charged is not None:
                identity_inserted = (
                    await connection.execute(
                        pg_insert(collection_identity_observations)
                        .values(
                            source_file_id=source.id,
                            retailer_id=retailer_id,
                            observed_at=charged_at,
                        )
                        .on_conflict_do_nothing(
                            index_elements=[collection_identity_observations.c.source_file_id]
                        )
                        .returning(collection_identity_observations.c.source_file_id)
                    )
                ).scalar_one_or_none()
                attempt = (
                    (
                        await connection.execute(
                            update(collection_attempts)
                            .where(collection_attempts.c.id == attempt_id)
                            .values(
                                charged_bytes=(collection_attempts.c.charged_bytes + content_length)
                            )
                            .returning(collection_attempts)
                        )
                    )
                    .mappings()
                    .one()
                )
                await self._adjust_budget_bucket(
                    connection,
                    retailer_id=retailer_id,
                    charged_at=charged_at,
                    charged_bytes_delta=content_length,
                    identity_delta=int(identity_inserted is not None),
                    attempt_delta=1,
                )
                budget = await self._locked_charge_budget(connection, retailer_id)
            return _charge_budget_value(attempt, budget)

    async def settle_transfer(
        self,
        attempt_id: UUID,
        source_file_id: UUID,
        remote_file: RemoteFile,
        transferred_bytes: int,
    ) -> CollectionChargeBudget:
        if transferred_bytes < 0:
            raise ValueError("transferred bytes cannot be negative")
        async with self._engine.begin() as connection:
            attempt, checkpoint = await self._locked_attempt(connection, attempt_id)
            if attempt.status != "running":
                raise PersistenceConflictError("Collection attempt is no longer running")
            retailer_id = UUID(str(checkpoint.retailer_id))
            budget = await self._locked_charge_budget(connection, retailer_id)
            retailer_source_key = (
                await connection.execute(
                    select(retailers.c.source_key).where(retailers.c.id == retailer_id)
                )
            ).scalar_one()
            if remote_file.retailer_id != retailer_source_key:
                raise PersistenceConflictError(
                    "Collection transfer charge is outside the attempt source scope"
                )
            source = (
                (
                    await connection.execute(
                        select(source_files.c.id, source_files.c.raw_archive_object_id)
                        .join(portals, portals.c.id == source_files.c.portal_id)
                        .where(
                            source_files.c.retailer_id == retailer_id,
                            source_files.c.id == source_file_id,
                            portals.c.source_key == remote_file.portal_id,
                            source_files.c.remote_id == remote_file.remote_id,
                        )
                        .with_for_update(of=source_files)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if source is None:
                raise PersistenceConflictError(
                    "Collection transfer charge source file was not registered"
                )
            reservation = (
                (
                    await connection.execute(
                        select(collection_transfer_charges)
                        .where(
                            collection_transfer_charges.c.attempt_id == attempt_id,
                            collection_transfer_charges.c.source_file_id == source_file_id,
                        )
                        .with_for_update()
                    )
                )
                .mappings()
                .one_or_none()
            )
            if reservation is None:
                raise PersistenceConflictError("Collection transfer was not reserved")
            if not reservation.settled:
                settled_at = (await connection.execute(select(func.clock_timestamp()))).scalar_one()
                archive_length = 0
                archive_charge_inserted = False
                archive_attached = source.raw_archive_object_id is not None
                if archive_attached:
                    archive_length = int(
                        (
                            await connection.execute(
                                select(raw_archive_objects.c.content_length).where(
                                    raw_archive_objects.c.id == source.raw_archive_object_id
                                )
                            )
                        ).scalar_one()
                    )
                    archive_charge_inserted = (
                        await connection.execute(
                            pg_insert(collection_archive_charges)
                            .values(
                                source_file_id=source_file_id,
                                retailer_id=retailer_id,
                                attempt_id=attempt_id,
                                content_length=archive_length,
                                charged_at=settled_at,
                            )
                            .on_conflict_do_nothing(
                                index_elements=[collection_archive_charges.c.source_file_id]
                            )
                            .returning(collection_archive_charges.c.source_file_id)
                        )
                    ).scalar_one_or_none() is not None
                transfer_charge = (
                    max(0, transferred_bytes - archive_length)
                    if archive_attached
                    else transferred_bytes
                )
                exact_attempt_charge = transfer_charge + (
                    archive_length if archive_charge_inserted else 0
                )
                if exact_attempt_charge > int(reservation.content_length):
                    raise PersistenceConflictError(
                        "Collection transfer charge exceeds its reservation"
                    )
                await connection.execute(
                    update(collection_transfer_charges)
                    .where(
                        collection_transfer_charges.c.attempt_id == attempt_id,
                        collection_transfer_charges.c.source_file_id == source_file_id,
                    )
                    .values(
                        content_length=transfer_charge,
                        settled=True,
                        archive_attached=archive_attached,
                        charged_at=settled_at,
                    )
                )
                delta = exact_attempt_charge - int(reservation.content_length)
                attempt = (
                    (
                        await connection.execute(
                            update(collection_attempts)
                            .where(collection_attempts.c.id == attempt_id)
                            .values(charged_bytes=(collection_attempts.c.charged_bytes + delta))
                            .returning(collection_attempts)
                        )
                    )
                    .mappings()
                    .one()
                )
                await self._adjust_budget_bucket(
                    connection,
                    retailer_id=retailer_id,
                    charged_at=reservation.charged_at,
                    charged_bytes_delta=-int(reservation.content_length),
                )
                await self._adjust_budget_bucket(
                    connection,
                    retailer_id=retailer_id,
                    charged_at=settled_at,
                    charged_bytes_delta=exact_attempt_charge,
                    success_delta=int(archive_charge_inserted),
                )
                budget = await self._locked_charge_budget(connection, retailer_id)
            return _charge_budget_value(attempt, budget)

    @staticmethod
    async def _retailer_id(
        connection: AsyncConnection,
        scope: CollectionScope,
    ) -> UUID:
        retailer_id = (
            await connection.execute(
                select(retailers.c.id).where(retailers.c.source_key == scope.source_id)
            )
        ).scalar_one_or_none()
        if retailer_id is None:
            raise PersistenceNotFoundError(f"Collection source {scope.source_id} is not registered")
        return UUID(str(retailer_id))

    @staticmethod
    async def _validate_portals(
        connection: AsyncConnection,
        retailer_id: UUID,
        expected_portals: tuple[str, ...],
    ) -> None:
        registered = tuple(
            (
                await connection.execute(
                    select(portals.c.source_key)
                    .where(
                        portals.c.retailer_id == retailer_id,
                        portals.c.source_key.in_(expected_portals),
                    )
                    .order_by(portals.c.source_key)
                )
            ).scalars()
        )
        if registered != expected_portals:
            raise PersistenceNotFoundError(
                "Collection portal generation contains an unregistered portal"
            )

    @staticmethod
    async def _locked_attempt(
        connection: AsyncConnection,
        attempt_id: UUID,
    ) -> tuple[RowMapping, RowMapping]:
        attempt = (
            (
                await connection.execute(
                    select(collection_attempts)
                    .where(collection_attempts.c.id == attempt_id)
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if attempt is None:
            raise PersistenceNotFoundError(f"Collection attempt {attempt_id} was not found")
        checkpoint = (
            (
                await connection.execute(
                    select(collection_checkpoints)
                    .where(collection_checkpoints.c.id == attempt.checkpoint_id)
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        return attempt, checkpoint

    @staticmethod
    async def _locked_charge_budget(
        connection: AsyncConnection,
        retailer_id: UUID,
    ) -> RowMapping:
        now = (await connection.execute(select(func.clock_timestamp()))).scalar_one()
        await connection.execute(
            pg_insert(collection_charge_budgets)
            .values(retailer_id=retailer_id, window_started_at=now)
            .on_conflict_do_nothing(index_elements=[collection_charge_budgets.c.retailer_id])
        )
        _ = (
            (
                await connection.execute(
                    select(collection_charge_budgets)
                    .where(collection_charge_budgets.c.retailer_id == retailer_id)
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        current_bucket = await _budget_bucket_start(connection, now)
        window_started_at = current_bucket - _ARCHIVE_BUDGET_WINDOW
        await connection.execute(
            delete(collection_budget_buckets).where(
                collection_budget_buckets.c.retailer_id == retailer_id,
                collection_budget_buckets.c.bucket_started_at
                < window_started_at - _BUDGET_BUCKET_WIDTH,
            )
        )
        counts = (
            await connection.execute(
                select(
                    func.coalesce(func.sum(collection_budget_buckets.c.charged_bytes), 0),
                    func.coalesce(func.sum(collection_budget_buckets.c.identity_count), 0),
                    func.coalesce(func.sum(collection_budget_buckets.c.attempt_count), 0),
                    func.coalesce(func.sum(collection_budget_buckets.c.success_count), 0),
                ).where(
                    collection_budget_buckets.c.retailer_id == retailer_id,
                    collection_budget_buckets.c.bucket_started_at >= window_started_at,
                    collection_budget_buckets.c.bucket_started_at <= current_bucket,
                )
            )
        ).one()
        return (
            (
                await connection.execute(
                    update(collection_charge_budgets)
                    .where(collection_charge_budgets.c.retailer_id == retailer_id)
                    .values(
                        window_started_at=window_started_at,
                        charged_bytes=int(counts[0]),
                        identity_count=int(counts[1]),
                        attempt_count=int(counts[2]),
                        success_count=int(counts[3]),
                        updated_at=now,
                    )
                    .returning(collection_charge_budgets)
                )
            )
            .mappings()
            .one()
        )

    @staticmethod
    async def _adjust_budget_bucket(
        connection: AsyncConnection,
        *,
        retailer_id: UUID,
        charged_at: datetime,
        charged_bytes_delta: int = 0,
        identity_delta: int = 0,
        attempt_delta: int = 0,
        success_delta: int = 0,
    ) -> None:
        bucket_started_at = await _budget_bucket_start(connection, charged_at)
        if (
            min(
                charged_bytes_delta,
                identity_delta,
                attempt_delta,
                success_delta,
            )
            < 0
        ):
            # An expired reservation bucket may already have been pruned. In that
            # case its charge is no longer in the rolling window and needs no
            # compensating insert. Existing buckets remain protected by the
            # nonnegative-count constraint.
            await connection.execute(
                update(collection_budget_buckets)
                .where(
                    collection_budget_buckets.c.retailer_id == retailer_id,
                    collection_budget_buckets.c.bucket_started_at == bucket_started_at,
                )
                .values(
                    charged_bytes=(collection_budget_buckets.c.charged_bytes + charged_bytes_delta),
                    identity_count=(collection_budget_buckets.c.identity_count + identity_delta),
                    attempt_count=(collection_budget_buckets.c.attempt_count + attempt_delta),
                    success_count=(collection_budget_buckets.c.success_count + success_delta),
                    updated_at=func.clock_timestamp(),
                )
            )
            return
        statement = pg_insert(collection_budget_buckets).values(
            retailer_id=retailer_id,
            bucket_started_at=bucket_started_at,
            charged_bytes=charged_bytes_delta,
            identity_count=identity_delta,
            attempt_count=attempt_delta,
            success_count=success_delta,
        )
        await connection.execute(
            statement.on_conflict_do_update(
                index_elements=[
                    collection_budget_buckets.c.retailer_id,
                    collection_budget_buckets.c.bucket_started_at,
                ],
                set_={
                    "charged_bytes": (
                        collection_budget_buckets.c.charged_bytes + charged_bytes_delta
                    ),
                    "identity_count": (collection_budget_buckets.c.identity_count + identity_delta),
                    "attempt_count": (collection_budget_buckets.c.attempt_count + attempt_delta),
                    "success_count": (collection_budget_buckets.c.success_count + success_delta),
                    "updated_at": func.clock_timestamp(),
                },
            )
        )


async def _budget_bucket_start(connection: AsyncConnection, value: datetime) -> datetime:
    return cast(
        datetime,
        (
            await connection.execute(
                text(
                    "SELECT date_bin(INTERVAL '5 minutes', "
                    "CAST(:value AS timestamptz), TIMESTAMPTZ '1970-01-01 00:00:00+00')"
                ),
                {"value": value},
            )
        ).scalar_one(),
    )


def _charge_budget_value(attempt: RowMapping, budget: RowMapping) -> CollectionChargeBudget:
    return CollectionChargeBudget(
        run_charged_bytes=int(attempt.charged_bytes),
        day_charged_bytes=int(budget.charged_bytes),
        day_source_identities=int(budget.identity_count),
        day_transfer_attempts=int(budget.attempt_count),
        day_successes=int(budget.success_count),
    )


class PostgresCollectionLeaseManager:
    """Hold a crash-releasing PostgreSQL session lock for an entire source traversal."""

    def __init__(self, database: Database) -> None:
        self._engine = database.create_unpooled_engine(
            application_name="makolet-collection-lock",
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
        if ttl <= timedelta(0) or ttl > _MAXIMUM_LEASE_TTL:
            raise ValueError("ttl must be positive and no longer than 24 hours")
        lock_resource = f"makolet:collection-source:{resource}"
        async with self._engine.connect() as connection:
            acquired = bool(
                (
                    await connection.execute(
                        text("SELECT pg_try_advisory_lock(hashtextextended(:resource, 0))"),
                        {"resource": lock_resource},
                    )
                ).scalar_one()
            )
            try:
                yield acquired
            finally:
                if acquired:
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


def _validate_progress(
    *,
    cursor: str | None,
    page_offset: int,
    deltas: tuple[int, ...],
) -> None:
    if cursor is not None and not 1 <= len(cursor.encode("utf-8")) <= _MAXIMUM_CURSOR_BYTES:
        raise ValueError("publisher cursor is empty or exceeds 8192 bytes")
    if page_offset < 0 or any(delta < 0 for delta in deltas):
        raise ValueError("collection offsets and progress deltas cannot be negative")


__all__ = ["PostgresCollectionLeaseManager", "PostgresCollectionRepository"]
