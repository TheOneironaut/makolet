"""PostgreSQL archive paging and normalized rebuild coordination."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Final, cast
from uuid import UUID

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from makolet.adapters.persistence.schema import (
    normalized_rebuild_control,
    normalized_rebuild_files,
    normalized_rebuild_runs,
    normalized_rebuild_snapshots,
)
from makolet.application.models import (
    ArchivedSourceFile,
    ArchivedSourceFilePage,
    NormalizedRebuildRun,
)
from makolet.domain.errors import (
    DomainValidationError,
    InvalidStateTransitionError,
    MaintenanceModeError,
    NotFoundError,
    QueryLimitError,
)

MAXIMUM_ARCHIVE_PAGE_SIZE: Final = 200
_NORMALIZED_REBUILD_LOCK_KEY: Final = 5570743760429981012
_DERIVED_RESET_TABLES: Final = (
    "promotion_clubs",
    "promotion_stores",
    "promotion_items",
    "promotions",
    "price_history",
    "current_prices",
    "availability_history",
    "current_availability",
    "retailer_identifier_assertions",
    "staged_promotion_clubs",
    "staged_promotion_stores",
    "staged_promotion_items",
    "staged_promotions",
    "staged_prices",
    "staged_stores",
    "staged_documents",
    "applied_source_contents",
    "source_scope_watermarks",
)
_TRUNCATE_DERIVED_STATE = "TRUNCATE TABLE " + ", ".join(_DERIVED_RESET_TABLES)
_SNAPSHOT_ENTITIES: Final = (
    ("stores", "snapshot_row.id::text"),
    ("store_aliases", "snapshot_row.id::text"),
    ("retailer_items", "snapshot_row.id::text"),
    ("canonical_products", "snapshot_row.id::text"),
    ("product_identifiers", "snapshot_row.id::text"),
    ("identifier_match_groups", "snapshot_row.id::text"),
    ("retailer_identifier_assertions", "snapshot_row.id::text"),
    ("product_match_candidates", "snapshot_row.id::text"),
    ("confirmed_product_matches", "snapshot_row.id::text"),
    ("current_prices", "snapshot_row.id::text"),
    ("price_history", "snapshot_row.id::text"),
    ("current_availability", "snapshot_row.id::text"),
    ("availability_history", "snapshot_row.id::text"),
    ("promotions", "snapshot_row.id::text"),
    (
        "promotion_items",
        "jsonb_build_array(snapshot_row.promotion_id, snapshot_row.retailer_item_id)::text",
    ),
    (
        "promotion_stores",
        "jsonb_build_array(snapshot_row.promotion_id, snapshot_row.store_id)::text",
    ),
    (
        "promotion_clubs",
        "jsonb_build_array(snapshot_row.promotion_id, snapshot_row.club_id)::text",
    ),
    ("applied_source_contents", "snapshot_row.id::text"),
    ("source_scope_watermarks", "snapshot_row.id::text"),
)
_EXACT_ID_ENTITIES: Final = (
    "product_identifiers",
    "retailer_identifier_assertions",
    "confirmed_product_matches",
    "applied_source_contents",
)
_OBSERVATION_ID_FIELDS: Final = {
    "current_prices": ("id", "source_file_id", "last_observed_at"),
    "price_history": ("id", "valid_to"),
    "current_availability": ("id", "source_file_id", "last_observed_at"),
    "availability_history": ("id", "valid_to"),
}
_PROMOTION_ID_FIELDS: Final = (
    "id",
    "source_file_id",
    "valid_to",
    "last_observed_at",
)
_SYSTEM_EXACT_ACTORS: Final = (
    "system:exact-gtin",
    "system:exact-gtin-evidence",
)
_DERIVED_IDENTIFIER_METHODS: Final = (
    "retailer_assertion",
    "independent_retailer_corroboration",
)


class PostgresArchiveMaintenanceRepository:
    """Keep destructive rebuild state explicit, durable, and restartable."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def list_archived_files(
        self,
        *,
        since: datetime,
        until: datetime,
        limit: int,
        cursor: str | None,
    ) -> ArchivedSourceFilePage:
        bounded_limit = _bounded_limit(limit)
        cursor_id = _uuid_cursor(cursor)
        async with self._engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        text(
                            """
                        WITH cursor_position AS (
                            SELECT source.download_finished_at AS archived_at,
                                   source.id AS source_file_id
                              FROM source_files source
                             WHERE source.id = :cursor_id
                               AND source.raw_archive_object_id IS NOT NULL
                        )
                        SELECT source.id AS source_file_id,
                               source.download_finished_at AS archived_at
                          FROM source_files source
                         WHERE source.raw_archive_object_id IS NOT NULL
                           AND source.download_finished_at >= :since
                           AND source.download_finished_at < :until
                           AND (
                               CAST(:cursor_id AS uuid) IS NULL
                               OR (source.download_finished_at, source.id) > (
                                    SELECT position.archived_at,
                                           position.source_file_id
                                      FROM cursor_position position
                               )
                           )
                         ORDER BY source.download_finished_at, source.id
                         LIMIT :limit
                        """
                        ),
                        {
                            "since": since,
                            "until": until,
                            "cursor_id": cursor_id,
                            "limit": bounded_limit + 1,
                        },
                    )
                )
                .mappings()
                .all()
            )
        selected = rows[:bounded_limit]
        files = tuple(
            ArchivedSourceFile(
                source_file_id=cast(UUID, row["source_file_id"]),
                archived_at=cast(datetime, row["archived_at"]),
            )
            for row in selected
        )
        return ArchivedSourceFilePage(
            files=files,
            next_cursor=(str(files[-1].source_file_id) if len(rows) > bounded_limit else None),
        )

    @asynccontextmanager
    async def lock_rebuild(self, rebuild_run_id: UUID) -> AsyncIterator[bool]:
        del rebuild_run_id
        async with self._engine.connect() as connection:
            acquired = bool(
                (
                    await connection.execute(
                        text("SELECT pg_try_advisory_lock(:key)"),
                        {"key": _NORMALIZED_REBUILD_LOCK_KEY},
                    )
                ).scalar_one()
            )
            try:
                yield acquired
            finally:
                if acquired:
                    await connection.execute(
                        text("SELECT pg_advisory_unlock(:key)"),
                        {"key": _NORMALIZED_REBUILD_LOCK_KEY},
                    )

    async def begin_rebuild(
        self,
        *,
        requested_by: str,
        parser_version: str,
    ) -> NormalizedRebuildRun:
        async with self._engine.begin() as connection:
            control = await _lock_control(connection)
            if control["active_rebuild_run_id"] is not None:
                raise InvalidStateTransitionError(
                    "A normalized rebuild is already active; resume that run"
                )
            row = (
                (
                    await connection.execute(
                        insert(normalized_rebuild_runs)
                        .values(
                            status="running",
                            requested_by=requested_by,
                            requested_parser_version=parser_version,
                            archive_cutoff_at=func.clock_timestamp(),
                        )
                        .returning(*normalized_rebuild_runs.c)
                    )
                )
                .mappings()
                .one()
            )
            rebuild_run_id = cast(UUID, row["id"])
            await connection.execute(
                text(
                    """
                    INSERT INTO normalized_rebuild_files (
                        rebuild_run_id, sequence, source_file_id,
                        archived_at, original_applied_at, original_parser_version,
                        effective_source_timestamp, status
                    )
                    WITH metadata_times AS (
                        SELECT document.source_file_id,
                               max(document.source_updated_at)
                                   AS source_updated_at
                          FROM staged_documents document
                         GROUP BY document.source_file_id
                    ),
                    eligible AS (
                        SELECT source.id AS source_file_id,
                               source.download_finished_at AS archived_at,
                               applied.applied_at AS original_applied_at,
                               source.parser_version AS original_parser_version,
                               COALESCE(
                                   metadata.source_updated_at,
                                   source.source_timestamp,
                                   source.download_finished_at,
                                   source.discovered_at
                               ) AS effective_source_timestamp
                          FROM source_files source
                          JOIN raw_archive_objects archive
                            ON archive.id = source.raw_archive_object_id
                          LEFT JOIN applied_source_contents applied
                            ON applied.source_file_id = source.id
                          LEFT JOIN metadata_times metadata
                            ON metadata.source_file_id = source.id
                         WHERE source.download_finished_at <= :archive_cutoff_at
                           AND source.status IN ('completed', 'archived')
                    )
                    SELECT :rebuild_run_id,
                           row_number() OVER (
                               ORDER BY COALESCE(
                                            original_applied_at,
                                            effective_source_timestamp
                                        ),
                                        source_file_id
                           ),
                           source_file_id,
                           archived_at,
                           original_applied_at,
                           original_parser_version,
                           effective_source_timestamp,
                           'pending'
                       FROM eligible
                      ORDER BY COALESCE(original_applied_at, effective_source_timestamp),
                               source_file_id
                    """
                ),
                {
                    "rebuild_run_id": rebuild_run_id,
                    "archive_cutoff_at": row["archive_cutoff_at"],
                },
            )
            row = (
                (
                    await connection.execute(
                        update(normalized_rebuild_runs)
                        .where(normalized_rebuild_runs.c.id == rebuild_run_id)
                        .values(
                            source_files_total=select(func.count())
                            .select_from(normalized_rebuild_files)
                            .where(normalized_rebuild_files.c.rebuild_run_id == rebuild_run_id)
                            .scalar_subquery(),
                            updated_at=func.clock_timestamp(),
                        )
                        .returning(*normalized_rebuild_runs.c)
                    )
                )
                .mappings()
                .one()
            )
            await connection.execute(
                update(normalized_rebuild_control)
                .where(normalized_rebuild_control.c.singleton_id == 1)
                .values(
                    active_rebuild_run_id=rebuild_run_id,
                    updated_at=func.clock_timestamp(),
                )
            )
            await _capture_snapshots(
                connection,
                rebuild_run_id=rebuild_run_id,
                phase="original",
            )
            await connection.execute(text(_TRUNCATE_DERIVED_STATE))
            await connection.execute(
                text(
                    """
                    DELETE FROM confirmed_product_matches
                     WHERE confirmed_by = ANY(CAST(:actors AS text[]))
                    """
                ),
                {"actors": list(_SYSTEM_EXACT_ACTORS)},
            )
            await connection.execute(
                text(
                    """
                    DELETE FROM product_identifiers
                     WHERE validation_method = ANY(CAST(:methods AS text[]))
                    """
                ),
                {"methods": list(_DERIVED_IDENTIFIER_METHODS)},
            )
        return _rebuild_run(row)

    async def resume_rebuild(self, rebuild_run_id: UUID) -> NormalizedRebuildRun:
        async with self._engine.begin() as connection:
            control = await _lock_control(connection)
            if control["active_rebuild_run_id"] != rebuild_run_id:
                raise InvalidStateTransitionError("The rebuild is not the active maintenance run")
            current = await _lock_rebuild_run(connection, rebuild_run_id)
            if current["status"] == "completed":
                raise InvalidStateTransitionError("A completed rebuild cannot be resumed")
            row = (
                (
                    await connection.execute(
                        update(normalized_rebuild_runs)
                        .where(normalized_rebuild_runs.c.id == rebuild_run_id)
                        .values(
                            status="running",
                            updated_at=func.clock_timestamp(),
                            finished_at=None,
                            error_code=None,
                            error_message=None,
                        )
                        .returning(*normalized_rebuild_runs.c)
                    )
                )
                .mappings()
                .one()
            )
        return _rebuild_run(row)

    async def get_rebuild(self, rebuild_run_id: UUID) -> NormalizedRebuildRun:
        async with self._engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(normalized_rebuild_runs).where(
                            normalized_rebuild_runs.c.id == rebuild_run_id
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise NotFoundError("Normalized rebuild was not found")
        return _rebuild_run(row)

    async def next_rebuild_files(
        self,
        rebuild_run_id: UUID,
        *,
        limit: int,
    ) -> tuple[tuple[int, ArchivedSourceFile], ...]:
        bounded_limit = _bounded_limit(limit)
        async with self._engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(
                            normalized_rebuild_files.c.sequence,
                            normalized_rebuild_files.c.source_file_id,
                            normalized_rebuild_files.c.archived_at,
                        )
                        .where(
                            normalized_rebuild_files.c.rebuild_run_id == rebuild_run_id,
                            normalized_rebuild_files.c.status == "pending",
                        )
                        .order_by(normalized_rebuild_files.c.sequence)
                        .limit(bounded_limit)
                    )
                )
                .mappings()
                .all()
            )
        return tuple(
            (
                int(row["sequence"]),
                ArchivedSourceFile(
                    source_file_id=cast(UUID, row["source_file_id"]),
                    archived_at=cast(datetime, row["archived_at"]),
                ),
            )
            for row in rows
        )

    async def complete_rebuild_file(
        self,
        rebuild_run_id: UUID,
        *,
        sequence: int,
        source_file: ArchivedSourceFile,
    ) -> None:
        async with self._engine.begin() as connection:
            run = await _lock_rebuild_run(connection, rebuild_run_id)
            if run["status"] != "running":
                raise InvalidStateTransitionError("Only a running rebuild can checkpoint files")
            expected_sequence = int(run["last_sequence"] or 0) + 1
            if sequence != expected_sequence:
                raise InvalidStateTransitionError("Rebuild files must checkpoint in order")
            completed = (
                await connection.execute(
                    update(normalized_rebuild_files)
                    .where(
                        normalized_rebuild_files.c.rebuild_run_id == rebuild_run_id,
                        normalized_rebuild_files.c.sequence == sequence,
                        normalized_rebuild_files.c.source_file_id == source_file.source_file_id,
                        normalized_rebuild_files.c.archived_at == source_file.archived_at,
                        normalized_rebuild_files.c.status == "pending",
                    )
                    .values(status="completed", completed_at=func.clock_timestamp())
                    .returning(normalized_rebuild_files.c.sequence)
                )
            ).scalar_one_or_none()
            if completed is None:
                raise InvalidStateTransitionError("Rebuild file is missing or already completed")
            await connection.execute(
                update(normalized_rebuild_runs)
                .where(normalized_rebuild_runs.c.id == rebuild_run_id)
                .values(
                    source_files_completed=normalized_rebuild_runs.c.source_files_completed + 1,
                    last_sequence=sequence,
                    last_source_file_id=source_file.source_file_id,
                    last_archived_at=source_file.archived_at,
                    updated_at=func.clock_timestamp(),
                )
            )

    async def fail_rebuild(
        self,
        rebuild_run_id: UUID,
        *,
        error_code: str,
        error_message: str,
    ) -> None:
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(normalized_rebuild_runs)
                .where(
                    normalized_rebuild_runs.c.id == rebuild_run_id,
                    normalized_rebuild_runs.c.status != "completed",
                )
                .values(
                    status="failed",
                    error_code=error_code,
                    error_message=error_message,
                    updated_at=func.clock_timestamp(),
                )
            )
            if result.rowcount != 1:
                raise InvalidStateTransitionError("Rebuild cannot be marked failed")

    async def finish_rebuild(self, rebuild_run_id: UUID) -> NormalizedRebuildRun:
        async with self._engine.begin() as connection:
            control = await _lock_control(connection)
            if control["active_rebuild_run_id"] != rebuild_run_id:
                raise InvalidStateTransitionError("The rebuild is not the active maintenance run")
            run = await _lock_rebuild_run(connection, rebuild_run_id)
            remaining = int(
                (
                    await connection.execute(
                        select(func.count())
                        .select_from(normalized_rebuild_files)
                        .where(
                            normalized_rebuild_files.c.rebuild_run_id == rebuild_run_id,
                            normalized_rebuild_files.c.status == "pending",
                        )
                    )
                ).scalar_one()
            )
            if remaining or int(run["source_files_completed"]) != int(run["source_files_total"]):
                raise InvalidStateTransitionError("Rebuild still has pending archived files")
            await _capture_snapshots(
                connection,
                rebuild_run_id=rebuild_run_id,
                phase="rebuilt",
            )
            correction, archive_only = await _rebuild_materially_changes_state(
                connection,
                rebuild_run_id=rebuild_run_id,
                requested_parser_version=str(run["requested_parser_version"]),
            )
            exact_equivalence_required = not correction and not archive_only
            if exact_equivalence_required:
                await _assert_replayed_logical_equivalence(
                    connection,
                    rebuild_run_id=rebuild_run_id,
                )
            await _reconcile_stable_rebuild_ids(
                connection,
                rebuild_run_id=rebuild_run_id,
            )
            if exact_equivalence_required:
                await _assert_exact_snapshot_equivalence(
                    connection,
                    rebuild_run_id=rebuild_run_id,
                )
                await connection.execute(
                    normalized_rebuild_snapshots.delete().where(
                        normalized_rebuild_snapshots.c.rebuild_run_id == rebuild_run_id
                    )
                )
            else:
                await _record_snapshot_outcomes(
                    connection,
                    rebuild_run_id=rebuild_run_id,
                )
                await connection.execute(
                    normalized_rebuild_snapshots.delete().where(
                        normalized_rebuild_snapshots.c.rebuild_run_id == rebuild_run_id,
                        normalized_rebuild_snapshots.c.phase == "rebuilt",
                    )
                )
            row = (
                (
                    await connection.execute(
                        update(normalized_rebuild_runs)
                        .where(normalized_rebuild_runs.c.id == rebuild_run_id)
                        .values(
                            status="completed",
                            updated_at=func.clock_timestamp(),
                            finished_at=func.clock_timestamp(),
                            error_code=None,
                            error_message=None,
                        )
                        .returning(*normalized_rebuild_runs.c)
                    )
                )
                .mappings()
                .one()
            )
            await connection.execute(
                update(normalized_rebuild_control)
                .where(normalized_rebuild_control.c.singleton_id == 1)
                .values(active_rebuild_run_id=None, updated_at=func.clock_timestamp())
            )
        return _rebuild_run(row)

    async def maintenance_status(self) -> dict[str, object]:
        async with self._engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(
                            normalized_rebuild_control.c.active_rebuild_run_id,
                            normalized_rebuild_runs.c.status,
                            normalized_rebuild_runs.c.archive_cutoff_at,
                            normalized_rebuild_runs.c.source_files_total,
                            normalized_rebuild_runs.c.source_files_completed,
                            normalized_rebuild_runs.c.last_source_file_id,
                            normalized_rebuild_runs.c.started_at,
                            normalized_rebuild_runs.c.updated_at,
                            normalized_rebuild_runs.c.finished_at,
                        )
                        .select_from(
                            normalized_rebuild_control.outerjoin(
                                normalized_rebuild_runs,
                                normalized_rebuild_runs.c.id
                                == normalized_rebuild_control.c.active_rebuild_run_id,
                            )
                        )
                        .where(normalized_rebuild_control.c.singleton_id == 1)
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None or row["active_rebuild_run_id"] is None:
            return {"active": False, "mode": "normal"}
        return {
            "active": True,
            "mode": "normalized_rebuild",
            "warning": "normalized query state is partial until the rebuild completes",
            "rebuild_run_id": row["active_rebuild_run_id"],
            "status": row["status"],
            "archive_cutoff_at": row["archive_cutoff_at"],
            "source_files_total": row["source_files_total"],
            "source_files_completed": row["source_files_completed"],
            "last_source_file_id": row["last_source_file_id"],
            "started_at": row["started_at"],
            "updated_at": row["updated_at"],
            "finished_at": row["finished_at"],
        }


async def _capture_snapshots(
    connection: AsyncConnection,
    *,
    rebuild_run_id: UUID,
    phase: str,
) -> None:
    for entity, row_key_expression in _SNAPSHOT_ENTITIES:
        await connection.execute(
            text(
                f"""
                INSERT INTO normalized_rebuild_snapshots (
                    rebuild_run_id, phase, entity, row_key, payload, outcome
                )
                SELECT :rebuild_run_id, :phase, :entity,
                       {row_key_expression}, to_jsonb(snapshot_row), NULL
                  FROM {entity} snapshot_row
                ON CONFLICT (rebuild_run_id, phase, entity, row_key)
                DO UPDATE SET payload = excluded.payload,
                              outcome = NULL,
                              captured_at = clock_timestamp()
                """  # noqa: S608 -- identifiers come from the fixed module allowlist.
            ),
            {
                "rebuild_run_id": rebuild_run_id,
                "phase": phase,
                "entity": entity,
            },
        )


async def _rebuild_materially_changes_state(
    connection: AsyncConnection,
    *,
    rebuild_run_id: UUID,
    requested_parser_version: str,
) -> tuple[bool, bool]:
    row = (
        (
            await connection.execute(
                text(
                    """
                    SELECT COALESCE(bool_or(
                               original_applied_at IS NOT NULL
                               AND original_parser_version IS DISTINCT FROM
                                   CAST(:parser_version AS text)
                           ), false) AS parser_correction,
                           COALESCE(bool_or(original_applied_at IS NULL), false)
                               AS archive_only
                      FROM normalized_rebuild_files
                     WHERE rebuild_run_id = :rebuild_run_id
                    """
                ),
                {
                    "rebuild_run_id": rebuild_run_id,
                    "parser_version": requested_parser_version,
                },
            )
        )
        .mappings()
        .one()
    )
    return bool(row["parser_correction"]), bool(row["archive_only"])


async def _assert_replayed_logical_equivalence(
    connection: AsyncConnection,
    *,
    rebuild_run_id: UUID,
) -> None:
    skipped_relations = {"promotion_items", "promotion_stores", "promotion_clubs"}
    for entity, _ in _SNAPSHOT_ENTITIES:
        if entity in skipped_relations:
            continue
        if entity == "confirmed_product_matches":
            normalized = "((payload - 'id') #- '{evidence,assertion_id}')"
        elif entity == "source_scope_watermarks":
            normalized = "(payload - ARRAY['id', 'updated_at'])"
        else:
            normalized = "(payload - 'id')"
        differs = bool(
            (
                await connection.execute(
                    text(
                        f"""
                        WITH original AS (
                            SELECT {normalized} AS value
                              FROM normalized_rebuild_snapshots
                             WHERE rebuild_run_id = :rebuild_run_id
                               AND phase = 'original'
                               AND entity = :entity
                        ), rebuilt AS (
                            SELECT {normalized} AS value
                              FROM normalized_rebuild_snapshots
                             WHERE rebuild_run_id = :rebuild_run_id
                               AND phase = 'rebuilt'
                               AND entity = :entity
                        ), difference AS (
                            (SELECT value FROM original EXCEPT ALL SELECT value FROM rebuilt)
                            UNION ALL
                            (SELECT value FROM rebuilt EXCEPT ALL SELECT value FROM original)
                        )
                        SELECT EXISTS (SELECT 1 FROM difference)
                        """  # noqa: S608 -- expression is selected from fixed cases above.
                    ),
                    {"rebuild_run_id": rebuild_run_id, "entity": entity},
                )
            ).scalar_one()
        )
        if differs:
            raise InvalidStateTransitionError(
                f"Parser-unchanged rebuild changed logical {entity} rows; "
                "the maintenance barrier remains active for inspection"
            )


async def _reconcile_stable_rebuild_ids(
    connection: AsyncConnection,
    *,
    rebuild_run_id: UUID,
) -> None:
    await _reuse_ids_by_payload(
        connection,
        rebuild_run_id=rebuild_run_id,
        entity="retailer_identifier_assertions",
        ignored_fields=("id",),
    )
    await _normalize_confirmed_assertion_evidence(connection)
    for entity in _EXACT_ID_ENTITIES:
        if entity == "retailer_identifier_assertions":
            continue
        await _reuse_ids_by_payload(
            connection,
            rebuild_run_id=rebuild_run_id,
            entity=entity,
            ignored_fields=("id",),
        )
    for entity, ignored_fields in _OBSERVATION_ID_FIELDS.items():
        await _reuse_ids_by_payload(
            connection,
            rebuild_run_id=rebuild_run_id,
            entity=entity,
            ignored_fields=ignored_fields,
        )
    await _reuse_watermark_ids(
        connection,
        rebuild_run_id=rebuild_run_id,
    )
    await _reuse_promotion_ids_and_relations(
        connection,
        rebuild_run_id=rebuild_run_id,
    )


async def _reuse_ids_by_payload(
    connection: AsyncConnection,
    *,
    rebuild_run_id: UUID,
    entity: str,
    ignored_fields: tuple[str, ...],
) -> None:
    ignored = ", ".join(f"'{field}'" for field in ignored_fields)
    await connection.execute(
        text(
            f"""
            WITH candidates AS (
                SELECT DISTINCT ON (live.id)
                       live.id AS current_id,
                       (snapshot.payload ->> 'id')::uuid AS original_id
                  FROM {entity} live
                  JOIN normalized_rebuild_snapshots snapshot
                    ON snapshot.rebuild_run_id = :rebuild_run_id
                   AND snapshot.phase = 'original'
                   AND snapshot.entity = :entity
                   AND (to_jsonb(live) - ARRAY[{ignored}]) =
                       (snapshot.payload - ARRAY[{ignored}])
                 WHERE live.id <> (snapshot.payload ->> 'id')::uuid
                 ORDER BY live.id, snapshot.row_key
            )
            UPDATE {entity} live
               SET id = candidate.original_id
              FROM candidates candidate
             WHERE live.id = candidate.current_id
               AND NOT EXISTS (
                   SELECT 1 FROM {entity} occupied
                    WHERE occupied.id = candidate.original_id
               )
            """  # noqa: S608 -- entity/fields are fixed module-owned identifiers.
        ),
        {"rebuild_run_id": rebuild_run_id, "entity": entity},
    )


async def _normalize_confirmed_assertion_evidence(connection: AsyncConnection) -> None:
    await connection.execute(
        text(
            """
            UPDATE confirmed_product_matches confirmed
               SET evidence = jsonb_set(
                   confirmed.evidence,
                   '{assertion_id}',
                   to_jsonb(assertion.id),
                   false
               )
              FROM retailer_identifier_assertions assertion
             WHERE confirmed.confirmed_by = ANY(CAST(:actors AS text[]))
               AND confirmed.retailer_item_id = assertion.retailer_item_id
               AND assertion.kind = 'gtin'
               AND assertion.superseded_at IS NULL
               AND confirmed.evidence ? 'assertion_id'
            """
        ),
        {"actors": list(_SYSTEM_EXACT_ACTORS)},
    )


async def _reuse_watermark_ids(
    connection: AsyncConnection,
    *,
    rebuild_run_id: UUID,
) -> None:
    await connection.execute(
        text(
            """
            WITH candidates AS (
                SELECT DISTINCT ON (live.id)
                       live.id AS current_id,
                       (snapshot.payload ->> 'id')::uuid AS original_id,
                       (snapshot.payload ->> 'updated_at')::timestamptz
                           AS original_updated_at
                  FROM source_scope_watermarks live
                  JOIN normalized_rebuild_snapshots snapshot
                    ON snapshot.rebuild_run_id = :rebuild_run_id
                   AND snapshot.phase = 'original'
                   AND snapshot.entity = 'source_scope_watermarks'
                   AND (to_jsonb(live) - ARRAY['id', 'updated_at']) =
                       (snapshot.payload - ARRAY['id', 'updated_at'])
                 ORDER BY live.id, snapshot.row_key
            )
            UPDATE source_scope_watermarks live
               SET id = candidate.original_id,
                   updated_at = candidate.original_updated_at
              FROM candidates candidate
             WHERE live.id = candidate.current_id
               AND (
                   live.id = candidate.original_id
                   OR NOT EXISTS (
                       SELECT 1 FROM source_scope_watermarks occupied
                        WHERE occupied.id = candidate.original_id
                   )
               )
            """
        ),
        {"rebuild_run_id": rebuild_run_id},
    )


async def _reuse_promotion_ids_and_relations(
    connection: AsyncConnection,
    *,
    rebuild_run_id: UUID,
) -> None:
    ignored = ", ".join(f"'{field}'" for field in _PROMOTION_ID_FIELDS)
    await connection.execute(
        text(
            f"""
            CREATE TEMP TABLE makolet_rebuild_promotion_id_map
                ON COMMIT DROP AS
            SELECT promotion.id AS rebuilt_id,
                   COALESCE(original.original_id, promotion.id) AS desired_id
              FROM promotions promotion
              LEFT JOIN LATERAL (
                  SELECT (snapshot.payload ->> 'id')::uuid AS original_id
                    FROM normalized_rebuild_snapshots snapshot
                   WHERE snapshot.rebuild_run_id = :rebuild_run_id
                     AND snapshot.phase = 'original'
                     AND snapshot.entity = 'promotions'
                     AND (snapshot.payload - ARRAY[{ignored}]) =
                         (to_jsonb(promotion) - ARRAY[{ignored}])
                   ORDER BY snapshot.row_key
                   LIMIT 1
              ) original ON true
            """  # noqa: S608 -- ignored fields are a fixed module tuple.
        ),
        {"rebuild_run_id": rebuild_run_id},
    )
    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_rebuild_promotion_items ON COMMIT DROP AS
            SELECT * FROM promotion_items
            """
        )
    )
    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_rebuild_promotion_stores ON COMMIT DROP AS
            SELECT * FROM promotion_stores
            """
        )
    )
    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_rebuild_promotion_clubs ON COMMIT DROP AS
            SELECT * FROM promotion_clubs
            """
        )
    )
    await connection.execute(text("DELETE FROM promotion_items"))
    await connection.execute(text("DELETE FROM promotion_stores"))
    await connection.execute(text("DELETE FROM promotion_clubs"))
    await connection.execute(
        text(
            """
            UPDATE promotions promotion
               SET id = mapping.desired_id
              FROM makolet_rebuild_promotion_id_map mapping
             WHERE promotion.id = mapping.rebuilt_id
               AND promotion.id <> mapping.desired_id
            """
        )
    )
    await connection.execute(
        text(
            """
            INSERT INTO promotion_items (
                promotion_id, retailer_item_id, item_type, is_gift
            )
            SELECT mapping.desired_id, relation.retailer_item_id,
                   relation.item_type, relation.is_gift
              FROM makolet_rebuild_promotion_items relation
              JOIN makolet_rebuild_promotion_id_map mapping
                ON mapping.rebuilt_id = relation.promotion_id
            """
        )
    )
    await connection.execute(
        text(
            """
            INSERT INTO promotion_stores (promotion_id, store_id)
            SELECT mapping.desired_id, relation.store_id
              FROM makolet_rebuild_promotion_stores relation
              JOIN makolet_rebuild_promotion_id_map mapping
                ON mapping.rebuilt_id = relation.promotion_id
            """
        )
    )
    await connection.execute(
        text(
            """
            INSERT INTO promotion_clubs (promotion_id, club_id)
            SELECT mapping.desired_id, relation.club_id
              FROM makolet_rebuild_promotion_clubs relation
              JOIN makolet_rebuild_promotion_id_map mapping
                ON mapping.rebuilt_id = relation.promotion_id
            """
        )
    )


async def _assert_exact_snapshot_equivalence(
    connection: AsyncConnection,
    *,
    rebuild_run_id: UUID,
) -> None:
    for entity, _ in _SNAPSHOT_ENTITIES:
        differs = bool(
            (
                await connection.execute(
                    text(
                        f"""
                        WITH original AS (
                            SELECT payload AS value
                              FROM normalized_rebuild_snapshots
                             WHERE rebuild_run_id = :rebuild_run_id
                               AND phase = 'original'
                               AND entity = :entity
                        ), materialized AS (
                            SELECT to_jsonb(materialized_row) AS value
                              FROM {entity} materialized_row
                        ), difference AS (
                            (SELECT value FROM original
                             EXCEPT ALL SELECT value FROM materialized)
                            UNION ALL
                            (SELECT value FROM materialized
                             EXCEPT ALL SELECT value FROM original)
                        )
                        SELECT EXISTS (SELECT 1 FROM difference)
                        """  # noqa: S608 -- entity comes from the fixed allowlist.
                    ),
                    {"rebuild_run_id": rebuild_run_id, "entity": entity},
                )
            ).scalar_one()
        )
        if differs:
            raise InvalidStateTransitionError(
                f"Stable rebuild could not preserve exact {entity} rows; "
                "the maintenance barrier remains active for inspection"
            )


async def _record_snapshot_outcomes(
    connection: AsyncConnection,
    *,
    rebuild_run_id: UUID,
) -> None:
    for entity, _ in _SNAPSHOT_ENTITIES:
        await connection.execute(
            text(
                f"""
                UPDATE normalized_rebuild_snapshots snapshot
                   SET outcome = CASE WHEN EXISTS (
                           SELECT 1
                             FROM {entity} materialized_row
                            WHERE to_jsonb(materialized_row) = snapshot.payload
                       ) THEN 'preserved' ELSE 'superseded' END
                 WHERE snapshot.rebuild_run_id = :rebuild_run_id
                   AND snapshot.phase = 'original'
                   AND snapshot.entity = :entity
                """  # noqa: S608 -- entity comes from the fixed allowlist.
            ),
            {"rebuild_run_id": rebuild_run_id, "entity": entity},
        )


async def assert_ingestion_allowed(
    connection: AsyncConnection,
    *,
    rebuild_run_id: UUID | None,
) -> None:
    """Lock the maintenance barrier for one apply or early ingestion check."""

    control = await _lock_control(connection, shared=True)
    active = cast(UUID | None, control["active_rebuild_run_id"])
    if active is None:
        if rebuild_run_id is not None:
            raise MaintenanceModeError("The requested normalized rebuild is no longer active")
        return
    if rebuild_run_id != active:
        raise MaintenanceModeError(
            f"Normalized rebuild {active} is active; ordinary ingestion is temporarily disabled"
        )
    status = (
        await connection.execute(
            select(normalized_rebuild_runs.c.status).where(
                normalized_rebuild_runs.c.id == rebuild_run_id
            )
        )
    ).scalar_one_or_none()
    if status != "running":
        raise MaintenanceModeError("The active normalized rebuild must be resumed before replay")


async def _lock_control(
    connection: AsyncConnection,
    *,
    shared: bool = False,
) -> RowMapping:
    await connection.execute(
        insert(normalized_rebuild_control)
        .values(singleton_id=1, active_rebuild_run_id=None)
        .on_conflict_do_nothing(index_elements=[normalized_rebuild_control.c.singleton_id])
    )
    statement = select(normalized_rebuild_control).where(
        normalized_rebuild_control.c.singleton_id == 1
    )
    statement = statement.with_for_update(read=shared)
    return (await connection.execute(statement)).mappings().one()


async def _lock_rebuild_run(
    connection: AsyncConnection,
    rebuild_run_id: UUID,
) -> RowMapping:
    row = (
        (
            await connection.execute(
                select(normalized_rebuild_runs)
                .where(normalized_rebuild_runs.c.id == rebuild_run_id)
                .with_for_update()
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise NotFoundError("Normalized rebuild was not found")
    return row


def _bounded_limit(limit: int) -> int:
    if limit <= 0 or limit > MAXIMUM_ARCHIVE_PAGE_SIZE:
        raise QueryLimitError(f"limit must be between 1 and {MAXIMUM_ARCHIVE_PAGE_SIZE}")
    return limit


def _uuid_cursor(cursor: str | None) -> UUID | None:
    if cursor is None:
        return None
    if len(cursor) > 128 or any(ord(character) < 32 for character in cursor):
        raise DomainValidationError("Archive cursor is invalid")
    try:
        return UUID(cursor)
    except ValueError as error:
        raise DomainValidationError("Archive cursor is invalid") from error


def _rebuild_run(row: RowMapping) -> NormalizedRebuildRun:
    return NormalizedRebuildRun(
        rebuild_run_id=cast(UUID, row["id"]),
        status=str(row["status"]),
        archive_cutoff_at=cast(datetime, row["archive_cutoff_at"]),
        source_files_total=int(row["source_files_total"]),
        source_files_completed=int(row["source_files_completed"]),
        last_sequence=(int(row["last_sequence"]) if row["last_sequence"] is not None else None),
        last_source_file_id=cast(UUID | None, row["last_source_file_id"]),
        last_archived_at=cast(datetime | None, row["last_archived_at"]),
        started_at=cast(datetime, row["started_at"]),
        updated_at=cast(datetime, row["updated_at"]),
        finished_at=cast(datetime | None, row["finished_at"]),
        error_code=cast(str | None, row["error_code"]),
        error_message=cast(str | None, row["error_message"]),
    )
