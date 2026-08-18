"""Bounded operational reads and durable stale-ingestion recovery."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from makolet.adapters.persistence.errors import PersistenceConflictError
from makolet.application.models import Page
from makolet.application.ports import IngestionRepository, LeaseManager
from makolet.domain.enums import IngestionStatus
from makolet.domain.errors import NotFoundError

_ACTIVE_STATUSES = (
    IngestionStatus.DOWNLOADING,
    IngestionStatus.ARCHIVED,
    IngestionStatus.PARSING,
    IngestionStatus.STAGED,
    IngestionStatus.VALIDATING,
    IngestionStatus.APPLYING,
)
_FAILURE_STATUSES = (
    IngestionStatus.FAILED_RETRYABLE.value,
    IngestionStatus.FAILED_TERMINAL.value,
)
_RECOVERY_LEASE_TTL = timedelta(minutes=5)


class PostgresOperationalRepository:
    """Expose failures/quarantine and recover abandoned lifecycle states."""

    def __init__(
        self,
        engine: AsyncEngine,
        ingestion: IngestionRepository,
        leases: LeaseManager,
    ) -> None:
        self._engine = engine
        self._ingestion = ingestion
        self._leases = leases

    async def failures(self, *, limit: int, cursor: str | None) -> Page:
        bounded_limit = _bounded_limit(limit)
        cursor_id = _uuid_cursor(cursor)
        rows = await self._rows(
            """
            SELECT source.id, retailer.source_key AS source_id,
                   portal.source_key AS portal_id, source.original_filename,
                   source.document_type, source.status, source.error_code,
                   source.error_message, source.discovered_at,
                   source.source_timestamp, source.updated_at
              FROM source_files source
              JOIN retailers retailer ON retailer.id = source.retailer_id
              JOIN portals portal ON portal.id = source.portal_id
             WHERE source.status = ANY(CAST(:statuses AS text[]))
               AND (CAST(:cursor_id AS uuid) IS NULL OR source.id > :cursor_id)
             ORDER BY source.id
             LIMIT :limit
            """,
            {
                "statuses": list(_FAILURE_STATUSES),
                "cursor_id": cursor_id,
                "limit": bounded_limit + 1,
            },
        )
        return _page(rows, bounded_limit)

    async def list_quarantine(self, *, limit: int, cursor: str | None) -> Page:
        bounded_limit = _bounded_limit(limit)
        cursor_id = _uuid_cursor(cursor)
        rows = await self._rows(
            """
            SELECT source.id, retailer.source_key AS source_id,
                   portal.source_key AS portal_id, source.original_filename,
                   source.document_type, source.error_code, source.error_message,
                   source.discovered_at, source.source_timestamp, source.updated_at,
                   count(issue.id) AS issue_count
              FROM source_files source
              JOIN retailers retailer ON retailer.id = source.retailer_id
              JOIN portals portal ON portal.id = source.portal_id
              LEFT JOIN validation_issues issue ON issue.source_file_id = source.id
             WHERE source.status = :status
               AND (CAST(:cursor_id AS uuid) IS NULL OR source.id > :cursor_id)
             GROUP BY source.id, retailer.source_key, portal.source_key
             ORDER BY source.id
             LIMIT :limit
            """,
            {
                "status": IngestionStatus.QUARANTINED.value,
                "cursor_id": cursor_id,
                "limit": bounded_limit + 1,
            },
        )
        return _page(rows, bounded_limit)

    async def inspect_quarantine(self, quarantine_id: UUID) -> dict[str, object]:
        rows = await self._rows(
            """
            SELECT source.id, retailer.source_key AS source_id,
                   portal.source_key AS portal_id, source.original_filename,
                   source.document_type, source.compression, source.status,
                   source.error_code, source.error_message, source.discovered_at,
                   source.source_timestamp, source.parser_version,
                   archive.object_key, archive.content_sha256,
                   archive.content_length, source.updated_at
              FROM source_files source
              JOIN retailers retailer ON retailer.id = source.retailer_id
              JOIN portals portal ON portal.id = source.portal_id
              LEFT JOIN raw_archive_objects archive
                ON archive.id = source.raw_archive_object_id
             WHERE source.id = :source_file_id
               AND source.status = :status
            """,
            {
                "source_file_id": quarantine_id,
                "status": IngestionStatus.QUARANTINED.value,
            },
        )
        if not rows:
            raise NotFoundError("Quarantined source file was not found")
        issues = await self._rows(
            """
            SELECT id, severity, code, message, record_index,
                   field_name, rejected_value, created_at
              FROM validation_issues
             WHERE source_file_id = :source_file_id
             ORDER BY record_index NULLS FIRST, id
             LIMIT 1001
            """,
            {"source_file_id": quarantine_id},
        )
        truncated = len(issues) > 1_000
        return {
            **rows[0],
            "issues": issues[:1_000],
            "issues_truncated": truncated,
        }

    async def recover_stale_jobs(self, *, stale_after: timedelta) -> int:
        if stale_after <= timedelta(0) or stale_after > timedelta(days=7):
            raise ValueError("stale_after must be positive and no longer than seven days")
        rows = await self._rows(
            """
            SELECT id, status
              FROM source_files
             WHERE status = ANY(CAST(:statuses AS text[]))
               AND updated_at < clock_timestamp() - :stale_seconds * interval '1 second'
             ORDER BY updated_at, id
             LIMIT 1000
            """,
            {
                "statuses": [status.value for status in _ACTIVE_STATUSES],
                "stale_seconds": stale_after.total_seconds(),
            },
        )
        recovered = 0
        for row in rows:
            current = IngestionStatus(str(row["status"]))
            source_file_id = UUID(str(row["id"]))
            async with self._leases.acquire(
                f"source-file:{source_file_id}",
                "stale-ingestion-recovery",
                _RECOVERY_LEASE_TTL,
            ) as acquired:
                if not acquired:
                    continue
                try:
                    await self._ingestion.transition(
                        source_file_id,
                        (current,),
                        IngestionStatus.FAILED_RETRYABLE,
                        error_code="stale_job_recovered",
                        error_message=(
                            "Worker recovered an ingestion whose heartbeat became stale"
                        ),
                    )
                except PersistenceConflictError:
                    continue
                recovered += 1
        return recovered

    async def _rows(
        self,
        statement: str,
        parameters: dict[str, object],
    ) -> tuple[dict[str, object], ...]:
        async with self._engine.connect() as connection:
            values = (await connection.execute(text(statement), parameters)).mappings().all()
        return tuple(dict(value) for value in values)


def _bounded_limit(limit: int) -> int:
    if not 1 <= limit <= 200:
        raise ValueError("limit must be between 1 and 200")
    return limit


def _uuid_cursor(cursor: str | None) -> UUID | None:
    if cursor is None:
        return None
    if len(cursor) > 64:
        raise ValueError("cursor is too long")
    try:
        return UUID(cursor)
    except ValueError as error:
        raise ValueError("cursor is not a valid UUID") from error


def _page(rows: tuple[dict[str, object], ...], limit: int) -> Page:
    items = rows[:limit]
    next_cursor = str(items[-1]["id"]) if len(rows) > limit and items else None
    return Page(items=items, next_cursor=next_cursor)


__all__ = ["PostgresOperationalRepository"]
