"""PostgreSQL implementation of transactional ingestion state and apply semantics."""

# ruff: noqa: S608
# Every interpolated SQL fragment in this module is a module-owned constant. Source
# values remain bound parameters; no retailer or caller text is interpolated.

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol, cast
from urllib.parse import SplitResult, parse_qsl, urlsplit
from uuid import UUID

from sqlalchemy import delete, func, insert, literal, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.sql.schema import Table

from makolet.adapters.persistence.errors import (
    PersistenceConflictError,
    PersistenceNotFoundError,
    SnapshotValidationError,
)
from makolet.adapters.persistence.leases import current_ingestion_lock
from makolet.adapters.persistence.maintenance import (
    assert_ingestion_allowed as assert_maintenance_ingestion_allowed,
)
from makolet.adapters.persistence.schema import (
    applied_source_contents,
    ingestion_runs,
    normalized_rebuild_files,
    portals,
    raw_archive_objects,
    replay_runs,
    retailers,
    source_file_events,
    source_files,
    staged_documents,
    staged_prices,
    staged_promotion_clubs,
    staged_promotion_items,
    staged_promotion_stores,
    staged_promotions,
    staged_stores,
    validation_issues,
)
from makolet.application.models import (
    DEFAULT_MAXIMUM_VALIDATION_ISSUE_BYTES_PER_ATTEMPT,
    DEFAULT_MAXIMUM_VALIDATION_ISSUE_EVIDENCE_PER_ATTEMPT,
    DEFAULT_MAXIMUM_VALIDATION_ISSUES_PER_ATTEMPT,
    ApplySummary,
    ArchivedDownload,
    RegisteredSourceFile,
    ReplayAttempt,
    StageSummary,
    validation_issue_charge,
)
from makolet.domain.enums import (
    CompressionFormat,
    DocumentType,
    IngestionStatus,
    IssueSeverity,
    SourceProtocol,
    ensure_ingestion_transition,
)
from makolet.domain.errors import QuarantinedFileError
from makolet.domain.models import (
    DocumentMetadata,
    ParsedEvent,
    PriceRecord,
    PromotionRecord,
    RemoteFile,
    StoreRecord,
    ValidationIssue,
    effective_promotion_store_ids,
)
from makolet.domain.normalization import is_valid_gtin

COPY_BATCH_SIZE = 5_000
MAXIMUM_FUTURE_SOURCE_SKEW = timedelta(hours=24)
SOURCE_STALENESS_WARNING_AGE = timedelta(days=30)
_DUPLICATE_ISSUE_CODE = "duplicate_logical_record"
_DUPLICATE_ISSUE_MESSAGE = "A later record in this file supersedes the same logical key"
_DUPLICATE_ISSUE_FIELD = "logical_identity"
_DUPLICATE_ISSUE_FIXED_BYTES = 64 + sum(
    len(value.encode("utf-8"))
    for value in (
        _DUPLICATE_ISSUE_CODE,
        _DUPLICATE_ISSUE_MESSAGE,
        _DUPLICATE_ISSUE_FIELD,
    )
)
_SIGNED_QUERY_KEYS = frozenset(
    {
        "access_token",
        "expires",
        "expiry",
        "se",
        "sig",
        "signature",
        "ske",
        "skoid",
        "sks",
        "skt",
        "sktid",
        "skv",
        "sp",
        "sr",
        "st",
        "sv",
        "token",
    }
)


class _CopyConnection(Protocol):
    async def copy_records_to_table(
        self,
        table_name: str,
        *,
        records: list[tuple[object, ...]],
        columns: list[str],
    ) -> str: ...


class PostgresIngestionRepository:
    """Bulk-stage hostile source records, then apply one file atomically."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        maximum_validation_issues: int = DEFAULT_MAXIMUM_VALIDATION_ISSUES_PER_ATTEMPT,
        maximum_validation_issue_bytes: int = (DEFAULT_MAXIMUM_VALIDATION_ISSUE_BYTES_PER_ATTEMPT),
        maximum_validation_issue_evidence: int = (
            DEFAULT_MAXIMUM_VALIDATION_ISSUE_EVIDENCE_PER_ATTEMPT
        ),
    ) -> None:
        if (
            min(
                maximum_validation_issues,
                maximum_validation_issue_bytes,
                maximum_validation_issue_evidence,
            )
            <= 0
        ):
            raise ValueError("Validation issue persistence limits must be positive")
        if maximum_validation_issue_evidence > maximum_validation_issues:
            raise ValueError("Validation evidence limit cannot exceed the issue-count limit")
        self._engine = engine
        self._maximum_validation_issues = maximum_validation_issues
        self._maximum_validation_issue_bytes = maximum_validation_issue_bytes
        self._maximum_validation_issue_evidence = maximum_validation_issue_evidence

    @asynccontextmanager
    async def _transaction(
        self,
        source_file_id: UUID | None = None,
        *,
        require_owned: bool = False,
    ) -> AsyncIterator[AsyncConnection]:
        lock = current_ingestion_lock()
        if require_owned and lock is None:
            raise PersistenceConflictError(
                "Mutable rediscovery refresh requires the per-file ingestion lock"
            )
        if lock is None:
            async with self._engine.begin() as connection:
                yield connection
            return
        if source_file_id is not None and lock.resource != f"source-file:{source_file_id}":
            raise PersistenceConflictError("Ingestion lock does not own the requested source file")
        async with lock.connection.begin():
            yield lock.connection

    async def assert_ingestion_allowed(
        self,
        *,
        rebuild_run_id: UUID | None = None,
    ) -> None:
        async with self._engine.begin() as connection:
            await assert_maintenance_ingestion_allowed(
                connection,
                rebuild_run_id=rebuild_run_id,
            )

    async def register_discovery(
        self,
        remote_file: RemoteFile,
        *,
        owned_refresh: bool = False,
    ) -> RegisteredSourceFile:
        """Register identity; mutate retry metadata only for the per-file lock owner."""

        async with self._transaction(require_owned=owned_refresh) as connection:
            retailer_id = await self._ensure_retailer(connection, remote_file.retailer_id)
            portal_id = await self._ensure_portal(connection, retailer_id, remote_file)
            source_file_id = (
                await connection.execute(
                    pg_insert(source_files)
                    .values(
                        retailer_id=retailer_id,
                        portal_id=portal_id,
                        remote_id=remote_file.remote_id,
                        download_url=remote_file.download_url,
                        original_filename=remote_file.original_filename,
                        document_type=remote_file.document_type.value,
                        compression=remote_file.compression.value,
                        protocol=remote_file.protocol.value,
                        status=IngestionStatus.DISCOVERED.value,
                        discovered_at=remote_file.discovered_at,
                        source_timestamp=remote_file.source_timestamp,
                        declared_content_length=remote_file.content_length,
                        media_type=remote_file.media_type,
                        etag=remote_file.etag,
                        last_modified=remote_file.last_modified,
                        response_metadata=list(remote_file.response_metadata),
                    )
                    .on_conflict_do_nothing(
                        index_elements=[source_files.c.portal_id, source_files.c.remote_id]
                    )
                    .returning(source_files.c.id)
                )
            ).scalar_one_or_none()
            already_registered = source_file_id is None
            if source_file_id is None:
                existing = (
                    (
                        await connection.execute(
                            select(source_files)
                            .where(
                                source_files.c.portal_id == portal_id,
                                source_files.c.remote_id == remote_file.remote_id,
                            )
                            .with_for_update()
                        )
                    )
                    .mappings()
                    .one()
                )
                source_file_id = cast(UUID, existing.id)
                if existing.status != IngestionStatus.COMPLETED.value:
                    rediscovery_updates = _rediscovery_updates(
                        existing,
                        remote_file,
                        owned_refresh=owned_refresh,
                    )
                    if rediscovery_updates:
                        await connection.execute(
                            update(source_files)
                            .where(source_files.c.id == source_file_id)
                            .values(
                                **rediscovery_updates,
                                updated_at=func.clock_timestamp(),
                            )
                        )
            else:
                run_id = (
                    await connection.execute(
                        insert(ingestion_runs)
                        .values(
                            source_file_id=source_file_id,
                            attempt=1,
                            status=IngestionStatus.DISCOVERED.value,
                        )
                        .returning(ingestion_runs.c.id)
                    )
                ).scalar_one()
                await connection.execute(
                    insert(source_file_events).values(
                        source_file_id=source_file_id,
                        ingestion_run_id=run_id,
                        from_status=None,
                        to_status=IngestionStatus.DISCOVERED.value,
                    )
                )
            if owned_refresh:
                owned_lock = current_ingestion_lock()
                if owned_lock is None or owned_lock.resource != f"source-file:{source_file_id}":
                    raise PersistenceConflictError(
                        "Ingestion lock does not own the rediscovered source file"
                    )
            row = await self._source_row(connection, source_file_id)
            return _registered_source_file(row, already_registered=already_registered)

    async def get(self, source_file_id: UUID) -> RegisteredSourceFile:
        async with self._transaction(source_file_id) as connection:
            row = await self._source_row(connection, source_file_id)
        return _registered_source_file(row, already_registered=True)

    async def transition(
        self,
        source_file_id: UUID,
        expected: Sequence[IngestionStatus],
        target: IngestionStatus,
        *,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if not expected:
            raise ValueError("At least one expected ingestion status is required")
        if error_code is not None and len(error_code) > 128:
            raise ValueError("error_code exceeds 128 characters")
        async with self._transaction(source_file_id) as connection:
            current_value = (
                await connection.execute(
                    select(source_files.c.status)
                    .where(source_files.c.id == source_file_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if current_value is None:
                raise PersistenceNotFoundError(f"Source file {source_file_id} does not exist")
            current = IngestionStatus(current_value)
            if current not in expected:
                expected_values = ", ".join(status.value for status in expected)
                raise PersistenceConflictError(
                    f"Source file {source_file_id} is {current.value}; expected {expected_values}"
                )
            ensure_ingestion_transition(current, target)
            run_id = await self._current_run_id(connection, source_file_id)
            if current is IngestionStatus.FAILED_RETRYABLE:
                attempt = (
                    await connection.execute(
                        select(func.max(ingestion_runs.c.attempt)).where(
                            ingestion_runs.c.source_file_id == source_file_id
                        )
                    )
                ).scalar_one()
                run_id = (
                    await connection.execute(
                        insert(ingestion_runs)
                        .values(
                            source_file_id=source_file_id,
                            attempt=int(attempt or 0) + 1,
                            status=target.value,
                        )
                        .returning(ingestion_runs.c.id)
                    )
                ).scalar_one()
            else:
                await connection.execute(
                    update(ingestion_runs)
                    .where(ingestion_runs.c.id == run_id)
                    .values(
                        status=target.value,
                        error_code=error_code,
                        error_message=error_message,
                        finished_at=(
                            func.clock_timestamp()
                            if target
                            in {
                                IngestionStatus.COMPLETED,
                                IngestionStatus.QUARANTINED,
                                IngestionStatus.FAILED_RETRYABLE,
                                IngestionStatus.FAILED_TERMINAL,
                            }
                            else None
                        ),
                    )
                )
            await connection.execute(
                update(source_files)
                .where(source_files.c.id == source_file_id)
                .values(
                    status=target.value,
                    error_code=error_code,
                    error_message=error_message,
                    updated_at=func.clock_timestamp(),
                )
            )
            await connection.execute(
                insert(source_file_events).values(
                    source_file_id=source_file_id,
                    ingestion_run_id=run_id,
                    from_status=current.value,
                    to_status=target.value,
                    error_code=error_code,
                    error_message=error_message,
                )
            )

    async def record_archive(
        self,
        source_file_id: UUID,
        archived: ArchivedDownload,
        *,
        parser_version: str,
    ) -> bool:
        if not parser_version or len(parser_version) > 128:
            raise ValueError("parser_version must contain at most 128 characters")
        if archived.evidence.content_length != archived.archive.content_length:
            raise PersistenceConflictError(
                "Download evidence byte length differs from the archived object"
            )
        if archived.evidence.finished_at < archived.evidence.started_at:
            raise ValueError("Download evidence finishes before it starts")
        for field_name, value in (
            ("started_at", archived.evidence.started_at),
            ("finished_at", archived.evidence.finished_at),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"Download evidence {field_name} must include a timezone")
        async with self._transaction(source_file_id) as connection:
            source_row = (
                await connection.execute(
                    select(
                        source_files.c.status,
                    )
                    .where(source_files.c.id == source_file_id)
                    .with_for_update()
                )
            ).one_or_none()
            if source_row is None:
                raise PersistenceNotFoundError(f"Source file {source_file_id} does not exist")
            current = IngestionStatus(source_row.status)
            if current is not IngestionStatus.DOWNLOADING:
                raise PersistenceConflictError(
                    f"Source file {source_file_id} is {current.value}; expected downloading"
                )
            ensure_ingestion_transition(current, IngestionStatus.ARCHIVED)
            run_id = await self._current_run_id(connection, source_file_id)
            archive_object_id = (
                await connection.execute(
                    pg_insert(raw_archive_objects)
                    .values(
                        content_sha256=archived.archive.content_sha256,
                        object_key=archived.archive.object_key,
                        content_length=archived.archive.content_length,
                        archived_at=archived.archive.archived_at,
                    )
                    .on_conflict_do_nothing(index_elements=[raw_archive_objects.c.content_sha256])
                    .returning(raw_archive_objects.c.id)
                )
            ).scalar_one_or_none()
            archive_object_reused = not archived.archive.created or archive_object_id is None
            if archive_object_id is None:
                archive_row = (
                    await connection.execute(
                        select(
                            raw_archive_objects.c.id,
                            raw_archive_objects.c.content_length,
                            raw_archive_objects.c.object_key,
                        ).where(
                            raw_archive_objects.c.content_sha256 == archived.archive.content_sha256
                        )
                    )
                ).one()
                if int(archive_row.content_length) != archived.archive.content_length:
                    raise PersistenceConflictError(
                        "An archive hash already exists with a different byte length"
                    )
                if str(archive_row.object_key) != archived.archive.object_key:
                    raise PersistenceConflictError(
                        "An archive hash already exists under a different object key"
                    )
                archive_object_id = archive_row.id
            await connection.execute(
                update(source_files)
                .where(source_files.c.id == source_file_id)
                .values(
                    raw_archive_object_id=archive_object_id,
                    parser_version=parser_version,
                    download_started_at=archived.evidence.started_at,
                    download_finished_at=archived.evidence.finished_at,
                    download_status_code=archived.evidence.status_code,
                    download_content_length=archived.evidence.content_length,
                    media_type=func.coalesce(
                        archived.evidence.media_type,
                        source_files.c.media_type,
                    ),
                    etag=func.coalesce(archived.evidence.etag, source_files.c.etag),
                    last_modified=func.coalesce(
                        archived.evidence.last_modified,
                        source_files.c.last_modified,
                    ),
                    download_response_metadata=list(archived.evidence.response_metadata),
                    status=IngestionStatus.ARCHIVED.value,
                    updated_at=func.clock_timestamp(),
                )
            )
            await connection.execute(
                update(ingestion_runs)
                .where(ingestion_runs.c.id == run_id)
                .values(status=IngestionStatus.ARCHIVED.value)
            )
            await connection.execute(
                insert(source_file_events).values(
                    source_file_id=source_file_id,
                    ingestion_run_id=run_id,
                    from_status=current.value,
                    to_status=IngestionStatus.ARCHIVED.value,
                )
            )
            return archive_object_reused

    async def clear_staging(self, source_file_id: UUID) -> None:
        async with self._transaction(source_file_id) as connection:
            await self._clear_staging(connection, source_file_id)

    async def reuse_validated_staging(
        self,
        source_file_id: UUID,
        *,
        parser_version: str,
        document_type: DocumentType,
        compression: CompressionFormat,
    ) -> StageSummary | None:
        """Clone a compatible completed parse, leaving empty staging on a cache miss."""

        if not parser_version or len(parser_version) > 128:
            raise ValueError("parser_version must contain at most 128 characters")
        async with self._transaction(source_file_id) as connection:
            target = (
                (
                    await connection.execute(
                        select(
                            source_files.c.retailer_id,
                            source_files.c.portal_id,
                            source_files.c.document_type,
                            source_files.c.compression,
                            source_files.c.status,
                            raw_archive_objects.c.content_sha256,
                        )
                        .join(
                            raw_archive_objects,
                            source_files.c.raw_archive_object_id == raw_archive_objects.c.id,
                        )
                        .where(source_files.c.id == source_file_id)
                        .with_for_update()
                    )
                )
                .mappings()
                .one_or_none()
            )
            if target is None:
                raise PersistenceNotFoundError(
                    f"Source file {source_file_id} is missing or has not been archived"
                )
            if target.document_type != document_type.value:
                raise PersistenceConflictError("Requested document type differs from discovery")
            if target.compression != compression.value:
                raise PersistenceConflictError("Requested compression differs from discovery")
            if target.status != IngestionStatus.PARSING.value:
                replay_active = (
                    await connection.execute(
                        select(replay_runs.c.id)
                        .where(
                            replay_runs.c.source_file_id == source_file_id,
                            replay_runs.c.finished_at.is_(None),
                        )
                        .limit(1)
                    )
                ).scalar_one_or_none()
                if replay_active is None:
                    raise PersistenceConflictError(
                        "Staging reuse requires an active parsing lifecycle or replay"
                    )
            await connection.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:resource, 0))"),
                {
                    "resource": (
                        "makolet:validated-staging:"
                        f"{target.content_sha256}:{parser_version}:"
                        f"{document_type.value}:{compression.value}"
                    )
                },
            )
            await self._clear_staging(connection, source_file_id)
            candidate = (
                (
                    await connection.execute(
                        text(
                            """
                            SELECT candidate.id AS source_file_id,
                                   candidate_run.id AS ingestion_run_id,
                                   candidate_run.metadata_records,
                                   candidate_run.store_records,
                                   candidate_run.price_records,
                                   candidate_run.promotion_records,
                                   candidate_run.warnings,
                                   candidate_run.rejected_records,
                                   candidate_run.file_quarantine_issues,
                                   candidate_run.validation_issue_bytes,
                                   candidate_run.validation_issue_samples
                              FROM source_files candidate
                              JOIN raw_archive_objects candidate_archive
                                ON candidate_archive.id = candidate.raw_archive_object_id
                              JOIN applied_source_contents applied
                                ON applied.source_file_id = candidate.id
                              JOIN LATERAL (
                                  SELECT run.id, run.metadata_records,
                                         run.store_records, run.price_records,
                                         run.promotion_records, run.warnings,
                                         run.rejected_records,
                                         run.file_quarantine_issues,
                                         run.validation_issue_bytes,
                                         run.validation_issue_samples
                                    FROM ingestion_runs run
                                   WHERE run.source_file_id = candidate.id
                                     AND run.status = 'completed'
                                   ORDER BY run.attempt DESC
                                   LIMIT 1
                              ) candidate_run ON true
                             WHERE candidate.id <> :source_file_id
                               AND candidate.retailer_id = :retailer_id
                               AND candidate.portal_id = :portal_id
                               AND candidate.document_type = :document_type
                               AND candidate.compression = :compression
                               AND candidate.parser_version = :parser_version
                               AND candidate.status = 'completed'
                               AND candidate_archive.content_sha256 = :content_sha256
                               AND NOT EXISTS (
                                   SELECT 1 FROM replay_runs replay
                                    WHERE replay.source_file_id = candidate.id
                               )
                             ORDER BY applied.applied_at DESC, candidate.id
                             LIMIT 1
                             FOR SHARE OF candidate
                            """
                        ),
                        {
                            "source_file_id": source_file_id,
                            "retailer_id": target.retailer_id,
                            "portal_id": target.portal_id,
                            "document_type": document_type.value,
                            "compression": compression.value,
                            "parser_version": parser_version,
                            "content_sha256": target.content_sha256,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if candidate is None:
                return None
            candidate_source_file_id = cast(UUID, candidate["source_file_id"])
            candidate_run_id = cast(UUID, candidate["ingestion_run_id"])
            actual = await _staging_summary_for_attempt(
                connection,
                source_file_id=candidate_source_file_id,
                ingestion_run_id=candidate_run_id,
                replay_run_id=None,
            )
            expected = StageSummary(
                metadata_records=int(candidate["metadata_records"]),
                store_records=int(candidate["store_records"]),
                price_records=int(candidate["price_records"]),
                promotion_records=int(candidate["promotion_records"]),
                warnings=int(candidate["warnings"]),
                rejected_records=int(candidate["rejected_records"]),
                file_quarantines=int(candidate["file_quarantine_issues"]),
                validation_issue_bytes=int(candidate["validation_issue_bytes"]),
                sampled_validation_issues=int(candidate["validation_issue_samples"]),
            )
            if actual != expected:
                return None
            if expected.file_quarantines:
                return None
            ingestion_run_id, replay_run_id = await self._current_attempt_ids(
                connection, source_file_id
            )
            for staging_table in (
                staged_documents,
                staged_stores,
                staged_prices,
                staged_promotions,
                staged_promotion_items,
                staged_promotion_stores,
                staged_promotion_clubs,
            ):
                await _clone_staging_table(
                    connection,
                    staging_table,
                    source_file_id=source_file_id,
                    candidate_source_file_id=candidate_source_file_id,
                )
            await connection.execute(
                insert(validation_issues).from_select(
                    [
                        "source_file_id",
                        "ingestion_run_id",
                        "replay_run_id",
                        "severity",
                        "code",
                        "message",
                        "record_index",
                        "field_name",
                        "rejected_value",
                    ],
                    select(
                        literal(source_file_id),
                        literal(
                            ingestion_run_id,
                            type_=validation_issues.c.ingestion_run_id.type,
                        ),
                        literal(
                            replay_run_id,
                            type_=validation_issues.c.replay_run_id.type,
                        ),
                        validation_issues.c.severity,
                        validation_issues.c.code,
                        validation_issues.c.message,
                        validation_issues.c.record_index,
                        validation_issues.c.field_name,
                        validation_issues.c.rejected_value,
                    ).where(
                        validation_issues.c.source_file_id == candidate_source_file_id,
                        validation_issues.c.ingestion_run_id == candidate_run_id,
                        validation_issues.c.record_index.is_not(None),
                        validation_issues.c.severity.in_(
                            (
                                IssueSeverity.WARNING.value,
                                IssueSeverity.RECORD_REJECTION.value,
                            )
                        ),
                    ),
                )
            )
            if ingestion_run_id is not None:
                await connection.execute(
                    update(ingestion_runs)
                    .where(ingestion_runs.c.id == ingestion_run_id)
                    .values(
                        warnings=actual.warnings,
                        rejected_records=actual.rejected_records,
                        file_quarantine_issues=actual.file_quarantines,
                        validation_issue_bytes=actual.validation_issue_bytes,
                        validation_issue_samples=actual.sampled_validation_issues,
                    )
                )
            elif replay_run_id is not None:
                payload = {
                    "stage": {
                        "metadata_records": actual.metadata_records,
                        "store_records": actual.store_records,
                        "price_records": actual.price_records,
                        "promotion_records": actual.promotion_records,
                        "warnings": actual.warnings,
                        "rejected_records": actual.rejected_records,
                        "file_quarantines": actual.file_quarantines,
                        "validation_issue_bytes": actual.validation_issue_bytes,
                        "sampled_validation_issues": actual.sampled_validation_issues,
                    }
                }
                await connection.execute(
                    update(replay_runs)
                    .where(replay_runs.c.id == replay_run_id)
                    .values(result_summary=payload)
                )
            return actual

    async def stage(
        self,
        source_file_id: UUID,
        events: Iterable[ParsedEvent],
    ) -> StageSummary:
        async with self._transaction(source_file_id) as connection:
            ingestion_run_id, replay_run_id = await self._current_attempt_ids(
                connection, source_file_id
            )
            current_issues = await _attempt_issue_summary(
                connection,
                ingestion_run_id=ingestion_run_id,
                replay_run_id=replay_run_id,
                lock=True,
            )
            metadata_index = (
                int(
                    (
                        await connection.execute(
                            select(
                                func.coalesce(func.max(staged_documents.c.metadata_index), -1)
                            ).where(staged_documents.c.source_file_id == source_file_id)
                        )
                    ).scalar_one()
                )
                + 1
            )
            raw_connection = await connection.get_raw_connection()
            driver = cast(_CopyConnection, raw_connection.driver_connection)
            summary = await self._copy_events(
                driver,
                source_file_id=source_file_id,
                ingestion_run_id=ingestion_run_id,
                replay_run_id=replay_run_id,
                metadata_index=metadata_index,
                events=events,
                evidence_limit=max(
                    0,
                    self._maximum_validation_issue_evidence
                    - current_issues.sampled_validation_issues,
                ),
            )
            cumulative_issue_count = (
                current_issues.warnings
                + current_issues.rejected_records
                + current_issues.file_quarantines
                + summary.warnings
                + summary.rejected_records
                + summary.file_quarantines
            )
            cumulative_issue_bytes = (
                current_issues.validation_issue_bytes + summary.validation_issue_bytes
            )
            if (
                cumulative_issue_count > self._maximum_validation_issues
                or cumulative_issue_bytes > self._maximum_validation_issue_bytes
            ):
                raise QuarantinedFileError(
                    "Validation issue evidence exceeds the configured persistence limit"
                )
            if ingestion_run_id is not None:
                await connection.execute(
                    update(ingestion_runs)
                    .where(ingestion_runs.c.id == ingestion_run_id)
                    .values(
                        metadata_records=ingestion_runs.c.metadata_records
                        + summary.metadata_records,
                        store_records=ingestion_runs.c.store_records + summary.store_records,
                        price_records=ingestion_runs.c.price_records + summary.price_records,
                        promotion_records=ingestion_runs.c.promotion_records
                        + summary.promotion_records,
                        warnings=ingestion_runs.c.warnings + summary.warnings,
                        rejected_records=ingestion_runs.c.rejected_records
                        + summary.rejected_records,
                        file_quarantine_issues=(
                            ingestion_runs.c.file_quarantine_issues + summary.file_quarantines
                        ),
                        validation_issue_bytes=(
                            ingestion_runs.c.validation_issue_bytes + summary.validation_issue_bytes
                        ),
                        validation_issue_samples=(
                            ingestion_runs.c.validation_issue_samples
                            + summary.sampled_validation_issues
                        ),
                    )
                )
            elif replay_run_id is not None:
                await self._add_replay_stage_summary(connection, replay_run_id, summary)
            return summary

    async def finalize_staging(
        self,
        source_file_id: UUID,
        document_type: DocumentType,
    ) -> StageSummary:
        """Reject superseded logical duplicates after all parser batches are staged."""

        async with self._transaction(source_file_id) as connection:
            ingestion_run_id, replay_run_id = await self._current_attempt_ids(
                connection,
                source_file_id,
            )
            if document_type is DocumentType.STORES:
                table_name = "staged_stores"
                partition = "subchain_id, source_store_id"
                key_expression = "concat_ws('/', subchain_id, source_store_id)"
            elif document_type.is_price:
                table_name = "staged_prices"
                partition = "subchain_id, source_store_id, source_item_code"
                key_expression = "concat_ws('/', subchain_id, source_store_id, source_item_code)"
            elif document_type.is_promotion:
                table_name = "staged_promotions"
                partition = "subchain_id, source_promotion_id, source_scope_store_code"
                key_expression = (
                    "concat_ws('/', subchain_id, source_scope_store_code, source_promotion_id)"
                )
            else:
                table_name = ""
                partition = ""
                key_expression = ""
            if table_name:
                current_issues = await _attempt_issue_summary(
                    connection,
                    ingestion_run_id=ingestion_run_id,
                    replay_run_id=replay_run_id,
                    lock=True,
                )
                duplicate_select = f"""
                    SELECT source_file_id, record_index,
                           {key_expression} AS logical_key,
                           row_number() OVER (
                               PARTITION BY {partition}
                               ORDER BY record_index DESC
                           ) AS occurrence
                      FROM {table_name}
                     WHERE source_file_id = :source_file_id
                """
                duplicate_row = (
                    (
                        await connection.execute(
                            text(
                                f"""
                                WITH ranked AS ({duplicate_select})
                                SELECT count(*) AS issue_count,
                                       COALESCE(sum(
                                           :fixed_bytes
                                           + octet_length(left(ranked.logical_key, 512))
                                       ), 0) AS issue_bytes
                                  FROM ranked
                                 WHERE ranked.occurrence > 1
                                """
                            ),
                            {
                                "source_file_id": source_file_id,
                                "fixed_bytes": _DUPLICATE_ISSUE_FIXED_BYTES,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                duplicate_count = int(duplicate_row["issue_count"])
                duplicate_bytes = int(duplicate_row["issue_bytes"])
                existing_issue_count = (
                    current_issues.warnings
                    + current_issues.rejected_records
                    + current_issues.file_quarantines
                )
                if (
                    existing_issue_count + duplicate_count > self._maximum_validation_issues
                    or current_issues.validation_issue_bytes + duplicate_bytes
                    > self._maximum_validation_issue_bytes
                ):
                    raise QuarantinedFileError(
                        "Validation issue evidence exceeds the configured persistence limit"
                    )
                evidence_limit = min(
                    duplicate_count,
                    self._maximum_validation_issue_evidence
                    - current_issues.sampled_validation_issues,
                )
                await connection.execute(
                    text(
                        f"""
                        WITH ranked AS ({duplicate_select})
                        INSERT INTO validation_issues (
                            source_file_id, ingestion_run_id, replay_run_id,
                            severity, code, message, record_index,
                            field_name, rejected_value
                        )
                        SELECT :source_file_id, CAST(:ingestion_run_id AS uuid),
                               CAST(:replay_run_id AS uuid),
                                'record_rejection', :issue_code,
                                :issue_message,
                                ranked.record_index, :issue_field,
                                left(ranked.logical_key, 512)
                           FROM ranked
                          WHERE ranked.occurrence > 1
                          ORDER BY ranked.record_index
                          LIMIT :evidence_limit
                        """
                    ),
                    {
                        "source_file_id": source_file_id,
                        "ingestion_run_id": ingestion_run_id,
                        "replay_run_id": replay_run_id,
                        "issue_code": _DUPLICATE_ISSUE_CODE,
                        "issue_message": _DUPLICATE_ISSUE_MESSAGE,
                        "issue_field": _DUPLICATE_ISSUE_FIELD,
                        "evidence_limit": evidence_limit,
                    },
                )
                duplicate_summary = StageSummary(
                    rejected_records=duplicate_count,
                    validation_issue_bytes=duplicate_bytes,
                    sampled_validation_issues=evidence_limit,
                )
                if ingestion_run_id is not None:
                    await connection.execute(
                        update(ingestion_runs)
                        .where(ingestion_runs.c.id == ingestion_run_id)
                        .values(
                            rejected_records=(ingestion_runs.c.rejected_records + duplicate_count),
                            validation_issue_bytes=(
                                ingestion_runs.c.validation_issue_bytes + duplicate_bytes
                            ),
                            validation_issue_samples=(
                                ingestion_runs.c.validation_issue_samples + evidence_limit
                            ),
                        )
                    )
                elif replay_run_id is not None:
                    await self._add_replay_stage_summary(
                        connection,
                        replay_run_id,
                        duplicate_summary,
                    )
                await connection.execute(
                    text(
                        f"""
                        WITH ranked AS ({duplicate_select})
                        DELETE FROM {table_name} staged
                         USING ranked
                         WHERE ranked.occurrence > 1
                           AND staged.source_file_id = ranked.source_file_id
                           AND staged.record_index = ranked.record_index
                        """
                    ),
                    {"source_file_id": source_file_id},
                )
            summary = await _staging_summary_for_attempt(
                connection,
                source_file_id=source_file_id,
                ingestion_run_id=ingestion_run_id,
                replay_run_id=replay_run_id,
            )
            if ingestion_run_id is not None:
                await connection.execute(
                    update(ingestion_runs)
                    .where(ingestion_runs.c.id == ingestion_run_id)
                    .values(
                        metadata_records=summary.metadata_records,
                        store_records=summary.store_records,
                        price_records=summary.price_records,
                        promotion_records=summary.promotion_records,
                        warnings=summary.warnings,
                        rejected_records=summary.rejected_records,
                        file_quarantine_issues=summary.file_quarantines,
                        validation_issue_bytes=summary.validation_issue_bytes,
                        validation_issue_samples=summary.sampled_validation_issues,
                    )
                )
            elif replay_run_id is not None:
                await connection.execute(
                    update(replay_runs)
                    .where(replay_runs.c.id == replay_run_id)
                    .values(
                        result_summary={
                            "stage": {
                                "metadata_records": summary.metadata_records,
                                "store_records": summary.store_records,
                                "price_records": summary.price_records,
                                "promotion_records": summary.promotion_records,
                                "warnings": summary.warnings,
                                "rejected_records": summary.rejected_records,
                                "file_quarantines": summary.file_quarantines,
                                "validation_issue_bytes": summary.validation_issue_bytes,
                                "sampled_validation_issues": (summary.sampled_validation_issues),
                            }
                        }
                    )
                )
            return summary

    async def has_file_quarantine_issue(self, source_file_id: UUID) -> bool:
        async with self._transaction(source_file_id) as connection:
            ingestion_run_id, replay_run_id = await self._current_attempt_ids(
                connection, source_file_id
            )
            if ingestion_run_id is not None:
                statement = text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM ingestion_runs run
                         WHERE run.id = :run_id
                           AND run.file_quarantine_issues > 0
                    )
                    """
                )
                parameters: dict[str, object] = {"run_id": ingestion_run_id}
            else:
                statement = text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM replay_runs run
                         WHERE run.id = :run_id
                           AND COALESCE(
                               (run.result_summary->'stage'->>'file_quarantines')::bigint,
                               0
                           ) > 0
                    )
                    """
                )
                parameters = {"run_id": replay_run_id}
            return bool((await connection.execute(statement, parameters)).scalar_one())

    async def apply(
        self,
        source_file_id: UUID,
        document_type: DocumentType,
        *,
        minimum_full_records: int,
        maximum_drop_fraction: float,
    ) -> ApplySummary:
        if minimum_full_records < 0:
            raise ValueError("minimum_full_records cannot be negative")
        if not 0 <= maximum_drop_fraction <= 1:
            raise ValueError("maximum_drop_fraction must be between zero and one")
        quarantine: tuple[str, str] | None = None
        summary: ApplySummary | None = None
        async with self._transaction(source_file_id) as connection:
            source_row = await self._lock_source_for_apply(
                connection,
                source_file_id,
                document_type,
            )
            ingestion_run_id, replay_run_id = await self._current_attempt_ids(
                connection, source_file_id
            )
            replay_rebuild_run_id = await self._replay_rebuild_run_id(
                connection,
                replay_run_id,
            )
            await assert_maintenance_ingestion_allowed(
                connection,
                rebuild_run_id=replay_rebuild_run_id,
            )
            await _analyze_staging_for_apply(connection, document_type)
            wall_clock_at = (await connection.execute(select(func.clock_timestamp()))).scalar_one()
            effective_timestamp, quarantine = await _validate_source_ordering(
                connection,
                source_file_id=source_file_id,
                source_row=source_row,
                document_type=document_type,
                observed_at=cast(
                    datetime,
                    source_row.download_finished_at or source_row.discovered_at,
                ),
                ingestion_run_id=ingestion_run_id,
                replay_run_id=replay_run_id,
                maximum_issues=self._maximum_validation_issues,
                maximum_issue_bytes=self._maximum_validation_issue_bytes,
                maximum_evidence=self._maximum_validation_issue_evidence,
            )
            if quarantine is None:
                applied_at = await self._claim_content(
                    connection,
                    source_file_id=source_file_id,
                    retailer_id=source_row.retailer_id,
                    portal_id=source_row.portal_id,
                    document_type=document_type,
                    content_sha256=source_row.content_sha256,
                    default_applied_at=wall_clock_at,
                    archive_applied_at=effective_timestamp,
                    replay_run_id=replay_run_id,
                    rebuild_run_id=replay_rebuild_run_id,
                )
                if document_type is DocumentType.STORES:
                    summary = await self._apply_stores(
                        connection,
                        source_file_id=source_file_id,
                        retailer_id=source_row.retailer_id,
                        portal_id=source_row.portal_id,
                        applied_at=applied_at,
                        minimum_full_records=minimum_full_records,
                        maximum_drop_fraction=maximum_drop_fraction,
                    )
                elif document_type.is_price:
                    summary = await self._apply_prices(
                        connection,
                        source_file_id=source_file_id,
                        retailer_id=source_row.retailer_id,
                        portal_id=source_row.portal_id,
                        applied_at=applied_at,
                        is_full=document_type.is_full_snapshot,
                        minimum_full_records=minimum_full_records,
                        maximum_drop_fraction=maximum_drop_fraction,
                    )
                elif document_type.is_promotion:
                    summary = await self._apply_promotions(
                        connection,
                        source_file_id=source_file_id,
                        retailer_id=source_row.retailer_id,
                        portal_id=source_row.portal_id,
                        applied_at=applied_at,
                        is_full=document_type.is_full_snapshot,
                        minimum_full_records=minimum_full_records,
                        maximum_drop_fraction=maximum_drop_fraction,
                    )
                else:
                    raise SnapshotValidationError("Unknown document types cannot be applied")
                await _advance_source_watermarks(
                    connection,
                    source_file_id=source_file_id,
                    retailer_id=source_row.retailer_id,
                    portal_id=source_row.portal_id,
                    document_family=(
                        "prices"
                        if document_type.is_price
                        else "promotions"
                        if document_type.is_promotion
                        else "stores"
                    ),
                    effective_timestamp=effective_timestamp,
                    content_sha256=str(source_row.content_sha256),
                )
                if ingestion_run_id is not None:
                    await connection.execute(
                        update(ingestion_runs)
                        .where(ingestion_runs.c.id == ingestion_run_id)
                        .values(
                            inserted_records=summary.inserted,
                            updated_records=summary.updated,
                            unchanged_records=summary.unchanged,
                            unavailable_records=summary.unavailable,
                            history_events=summary.history_events,
                        )
                    )
                elif replay_run_id is not None:
                    await connection.execute(
                        update(replay_runs)
                        .where(replay_runs.c.id == replay_run_id)
                        .values(
                            result_summary=replay_runs.c.result_summary.op("||")(
                                {
                                    "apply": {
                                        "inserted": summary.inserted,
                                        "updated": summary.updated,
                                        "unchanged": summary.unchanged,
                                        "unavailable": summary.unavailable,
                                        "history_events": summary.history_events,
                                    }
                                }
                            )
                        )
                    )
        if quarantine is not None:
            raise QuarantinedFileError(quarantine[1])
        if summary is None:
            raise AssertionError("Successful apply must produce a summary")
        return summary

    async def archive_key(self, source_file_id: UUID) -> str:
        row = await self._archive_row(source_file_id)
        return str(row.object_key)

    async def archive_sha256(self, source_file_id: UUID) -> str:
        row = await self._archive_row(source_file_id)
        return str(row.content_sha256)

    async def begin_replay(
        self,
        source_file_id: UUID,
        *,
        parser_version: str,
        rebuild_run_id: UUID | None = None,
    ) -> ReplayAttempt:
        if not parser_version or len(parser_version) > 128:
            raise ValueError("parser_version must contain at most 128 characters")
        async with self._transaction(source_file_id) as connection:
            await assert_maintenance_ingestion_allowed(
                connection,
                rebuild_run_id=rebuild_run_id,
            )
            source_row = (
                await connection.execute(
                    select(
                        source_files.c.parser_version,
                        source_files.c.raw_archive_object_id,
                    )
                    .where(source_files.c.id == source_file_id)
                    .with_for_update()
                )
            ).one_or_none()
            if source_row is None:
                raise PersistenceNotFoundError(f"Source file {source_file_id} does not exist")
            if source_row.raw_archive_object_id is None:
                raise PersistenceConflictError("Only archived source files can be replayed")
            # The application holds the source-file session lock here. Any open row
            # therefore belongs to an interrupted process and must be closed before
            # replay staging is rebuilt from the immutable archive.
            await connection.execute(
                update(replay_runs)
                .where(
                    replay_runs.c.source_file_id == source_file_id,
                    replay_runs.c.finished_at.is_(None),
                )
                .values(
                    status=IngestionStatus.FAILED_RETRYABLE.value,
                    finished_at=func.clock_timestamp(),
                    error_message="Replay owner exited before completing the attempt",
                    result_summary=replay_runs.c.result_summary.op("||")(
                        {
                            "succeeded": False,
                            "error_code": "interrupted_replay_recovered",
                        }
                    ),
                )
            )
            row = (
                await connection.execute(
                    insert(replay_runs)
                    .values(
                        source_file_id=source_file_id,
                        rebuild_run_id=rebuild_run_id,
                        requested_parser_version=parser_version,
                        previous_parser_version=source_row.parser_version,
                        status=IngestionStatus.PARSING.value,
                        result_summary={"stage": _empty_stage_counts()},
                    )
                    .returning(replay_runs.c.id, replay_runs.c.started_at)
                )
            ).one()
        return ReplayAttempt(
            replay_id=cast(UUID, row.id),
            source_file_id=source_file_id,
            started_at=row.started_at,
        )

    async def finish_replay(
        self,
        replay_id: UUID,
        *,
        succeeded: bool,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if error_code is not None and len(error_code) > 128:
            raise ValueError("error_code exceeds 128 characters")
        if succeeded and (error_code is not None or error_message is not None):
            raise ValueError("A successful replay cannot include an error")
        target = IngestionStatus.COMPLETED if succeeded else IngestionStatus.FAILED_TERMINAL
        result_patch: dict[str, object] = {"succeeded": succeeded}
        if error_code is not None:
            result_patch["error_code"] = error_code
        async with self._transaction() as connection:
            lock = current_ingestion_lock()
            if lock is not None:
                replay_source_file_id = (
                    await connection.execute(
                        select(replay_runs.c.source_file_id).where(replay_runs.c.id == replay_id)
                    )
                ).scalar_one_or_none()
                if (
                    replay_source_file_id is None
                    or lock.resource != f"source-file:{replay_source_file_id}"
                ):
                    raise PersistenceConflictError(
                        "Ingestion lock does not own the requested replay"
                    )
            updated = (
                await connection.execute(
                    update(replay_runs)
                    .where(
                        replay_runs.c.id == replay_id,
                        replay_runs.c.finished_at.is_(None),
                    )
                    .values(
                        status=target.value,
                        finished_at=func.clock_timestamp(),
                        error_message=error_message,
                        result_summary=replay_runs.c.result_summary.op("||")(result_patch),
                    )
                    .returning(replay_runs.c.id)
                )
            ).scalar_one_or_none()
        if updated is None:
            raise PersistenceConflictError(
                f"Replay {replay_id} does not exist or is already finished"
            )

    async def _ensure_retailer(self, connection: AsyncConnection, source_key: str) -> UUID:
        retailer_id = (
            await connection.execute(
                pg_insert(retailers)
                .values(source_key=source_key, display_name=source_key)
                .on_conflict_do_update(
                    index_elements=[retailers.c.source_key],
                    set_={"updated_at": func.clock_timestamp()},
                )
                .returning(retailers.c.id)
            )
        ).scalar_one()
        return cast(UUID, retailer_id)

    async def _ensure_portal(
        self,
        connection: AsyncConnection,
        retailer_id: UUID,
        remote_file: RemoteFile,
    ) -> UUID:
        portal_id = (
            await connection.execute(
                pg_insert(portals)
                .values(
                    retailer_id=retailer_id,
                    source_key=remote_file.portal_id,
                    protocol=remote_file.protocol.value,
                )
                .on_conflict_do_update(
                    index_elements=[portals.c.retailer_id, portals.c.source_key],
                    set_={
                        "protocol": remote_file.protocol.value,
                        "updated_at": func.clock_timestamp(),
                    },
                )
                .returning(portals.c.id)
            )
        ).scalar_one()
        return cast(UUID, portal_id)

    async def _source_row(
        self,
        connection: AsyncConnection,
        source_file_id: UUID,
    ) -> RowMapping:
        row = (
            (
                await connection.execute(
                    select(
                        source_files,
                        retailers.c.source_key.label("retailer_source_key"),
                        portals.c.source_key.label("portal_source_key"),
                        raw_archive_objects.c.content_sha256,
                        raw_archive_objects.c.object_key,
                        raw_archive_objects.c.content_length.label("archive_content_length"),
                    )
                    .join(retailers, source_files.c.retailer_id == retailers.c.id)
                    .join(portals, source_files.c.portal_id == portals.c.id)
                    .outerjoin(
                        raw_archive_objects,
                        source_files.c.raw_archive_object_id == raw_archive_objects.c.id,
                    )
                    .where(source_files.c.id == source_file_id)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise PersistenceNotFoundError(f"Source file {source_file_id} does not exist")
        return row

    async def _current_run_id(
        self,
        connection: AsyncConnection,
        source_file_id: UUID,
    ) -> UUID:
        run_id = (
            await connection.execute(
                select(ingestion_runs.c.id)
                .where(ingestion_runs.c.source_file_id == source_file_id)
                .order_by(ingestion_runs.c.attempt.desc())
                .limit(1)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if run_id is None:
            raise PersistenceNotFoundError(
                f"No ingestion run exists for source file {source_file_id}"
            )
        return cast(UUID, run_id)

    async def _current_attempt_ids(
        self,
        connection: AsyncConnection,
        source_file_id: UUID,
    ) -> tuple[UUID | None, UUID | None]:
        replay_id = (
            await connection.execute(
                select(replay_runs.c.id)
                .where(
                    replay_runs.c.source_file_id == source_file_id,
                    replay_runs.c.finished_at.is_(None),
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if replay_id is not None:
            return None, cast(UUID, replay_id)
        return await self._current_run_id(connection, source_file_id), None

    async def _replay_rebuild_run_id(
        self,
        connection: AsyncConnection,
        replay_run_id: UUID | None,
    ) -> UUID | None:
        if replay_run_id is None:
            return None
        value = (
            await connection.execute(
                select(replay_runs.c.rebuild_run_id).where(replay_runs.c.id == replay_run_id)
            )
        ).scalar_one()
        return cast(UUID | None, value)

    async def _add_replay_stage_summary(
        self,
        connection: AsyncConnection,
        replay_id: UUID,
        summary: StageSummary,
    ) -> None:
        current_payload = (
            await connection.execute(
                select(replay_runs.c.result_summary)
                .where(replay_runs.c.id == replay_id)
                .with_for_update()
            )
        ).scalar_one()
        payload = dict(current_payload or {})
        stage = dict(payload.get("stage") or {})
        additions = {
            "metadata_records": summary.metadata_records,
            "store_records": summary.store_records,
            "price_records": summary.price_records,
            "promotion_records": summary.promotion_records,
            "warnings": summary.warnings,
            "rejected_records": summary.rejected_records,
            "file_quarantines": summary.file_quarantines,
            "validation_issue_bytes": summary.validation_issue_bytes,
            "sampled_validation_issues": summary.sampled_validation_issues,
        }
        payload["stage"] = {key: int(stage.get(key, 0)) + value for key, value in additions.items()}
        await connection.execute(
            update(replay_runs).where(replay_runs.c.id == replay_id).values(result_summary=payload)
        )

    async def _clear_staging(
        self,
        connection: AsyncConnection,
        source_file_id: UUID,
    ) -> None:
        await connection.execute(
            delete(staged_documents).where(staged_documents.c.source_file_id == source_file_id)
        )
        await connection.execute(
            delete(staged_stores).where(staged_stores.c.source_file_id == source_file_id)
        )
        await connection.execute(
            delete(staged_prices).where(staged_prices.c.source_file_id == source_file_id)
        )
        await connection.execute(
            delete(staged_promotions).where(staged_promotions.c.source_file_id == source_file_id)
        )
        ingestion_run_id, replay_run_id = await self._current_attempt_ids(
            connection, source_file_id
        )
        if ingestion_run_id is not None:
            await connection.execute(
                delete(validation_issues).where(
                    validation_issues.c.ingestion_run_id == ingestion_run_id
                )
            )
            await connection.execute(
                update(ingestion_runs)
                .where(ingestion_runs.c.id == ingestion_run_id)
                .values(
                    metadata_records=0,
                    store_records=0,
                    price_records=0,
                    promotion_records=0,
                    warnings=0,
                    rejected_records=0,
                    file_quarantine_issues=0,
                    validation_issue_bytes=0,
                    validation_issue_samples=0,
                )
            )
        elif replay_run_id is not None:
            await connection.execute(
                delete(validation_issues).where(validation_issues.c.replay_run_id == replay_run_id)
            )
            await connection.execute(
                update(replay_runs)
                .where(replay_runs.c.id == replay_run_id)
                .values(result_summary={"stage": _empty_stage_counts()})
            )

    async def _copy_events(
        self,
        driver: _CopyConnection,
        *,
        source_file_id: UUID,
        ingestion_run_id: UUID | None,
        replay_run_id: UUID | None,
        metadata_index: int,
        events: Iterable[ParsedEvent],
        evidence_limit: int,
    ) -> StageSummary:
        buffers: dict[Table, list[tuple[object, ...]]] = {
            staged_documents: [],
            staged_stores: [],
            staged_prices: [],
            staged_promotions: [],
            staged_promotion_items: [],
            staged_promotion_stores: [],
            staged_promotion_clubs: [],
            validation_issues: [],
        }
        counts = {
            "metadata": 0,
            "stores": 0,
            "prices": 0,
            "promotions": 0,
            "warnings": 0,
            "rejected": 0,
            "quarantines": 0,
            "issue_bytes": 0,
            "sampled_issues": 0,
        }
        for event in events:
            if event.source_file_id != source_file_id:
                raise PersistenceConflictError("A parsed event belongs to a different source file")
            table, rows = _event_rows(
                event,
                source_file_id=source_file_id,
                ingestion_run_id=ingestion_run_id,
                replay_run_id=replay_run_id,
                metadata_index=metadata_index + counts["metadata"],
            )
            if isinstance(event, DocumentMetadata):
                counts["metadata"] += 1
            elif isinstance(event, StoreRecord):
                counts["stores"] += 1
            elif isinstance(event, PriceRecord):
                counts["prices"] += 1
            elif isinstance(event, PromotionRecord):
                counts["promotions"] += 1
            elif event.severity is IssueSeverity.WARNING:
                counts["warnings"] += 1
            elif event.severity is IssueSeverity.RECORD_REJECTION:
                counts["rejected"] += 1
            elif event.severity is IssueSeverity.FILE_QUARANTINE:
                counts["quarantines"] += 1
            if isinstance(event, ValidationIssue):
                counts["issue_bytes"] += validation_issue_charge(event)
                if counts["sampled_issues"] >= evidence_limit:
                    rows = []
                else:
                    counts["sampled_issues"] += 1
            buffers[table].extend(rows)
            if isinstance(event, PromotionRecord):
                child_rows = _promotion_child_rows(event)
                for child_table, child_values in child_rows.items():
                    buffers[child_table].extend(child_values)
            for buffered_table, buffered_rows in buffers.items():
                if len(buffered_rows) >= COPY_BATCH_SIZE:
                    await _copy_records(driver, buffered_table, buffered_rows)
                    buffered_rows.clear()
        for buffered_table, buffered_rows in buffers.items():
            if buffered_rows:
                await _copy_records(driver, buffered_table, buffered_rows)
        return StageSummary(
            metadata_records=counts["metadata"],
            store_records=counts["stores"],
            price_records=counts["prices"],
            promotion_records=counts["promotions"],
            warnings=counts["warnings"],
            rejected_records=counts["rejected"],
            file_quarantines=counts["quarantines"],
            validation_issue_bytes=counts["issue_bytes"],
            sampled_validation_issues=counts["sampled_issues"],
        )

    async def _lock_source_for_apply(
        self,
        connection: AsyncConnection,
        source_file_id: UUID,
        document_type: DocumentType,
    ) -> RowMapping:
        await connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:resource, 0))"),
            {"resource": f"makolet:source-file:{source_file_id}"},
        )
        row = (
            (
                await connection.execute(
                    select(
                        source_files.c.retailer_id,
                        source_files.c.portal_id,
                        source_files.c.document_type,
                        source_files.c.status,
                        source_files.c.source_timestamp,
                        source_files.c.download_finished_at,
                        source_files.c.discovered_at,
                        raw_archive_objects.c.content_sha256,
                    )
                    .join(
                        raw_archive_objects,
                        source_files.c.raw_archive_object_id == raw_archive_objects.c.id,
                    )
                    .where(source_files.c.id == source_file_id)
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise PersistenceNotFoundError(
                f"Source file {source_file_id} is missing or has not been archived"
            )
        if row.document_type != document_type.value:
            raise PersistenceConflictError("Requested document type differs from discovery")
        replay_active = (
            await connection.execute(
                select(replay_runs.c.id)
                .where(
                    replay_runs.c.source_file_id == source_file_id,
                    replay_runs.c.finished_at.is_(None),
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if (
            row.status
            not in {
                IngestionStatus.APPLYING.value,
                IngestionStatus.COMPLETED.value,
            }
            and replay_active is None
        ):
            raise PersistenceConflictError(
                "Source file must be applying or have an active replay before apply; "
                f"current status is {row.status}"
            )
        family = (
            "price"
            if document_type.is_price
            else "promotion"
            if document_type.is_promotion
            else "stores"
        )
        await connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:resource, 0))"),
            {"resource": (f"makolet:retailer:{row.retailer_id}:portal:{row.portal_id}:{family}")},
        )
        return row

    async def _claim_content(
        self,
        connection: AsyncConnection,
        *,
        source_file_id: UUID,
        retailer_id: UUID,
        portal_id: UUID,
        document_type: DocumentType,
        content_sha256: str,
        default_applied_at: datetime,
        archive_applied_at: datetime,
        replay_run_id: UUID | None,
        rebuild_run_id: UUID | None,
    ) -> datetime:
        existing = (
            await connection.execute(
                select(applied_source_contents.c.applied_at).where(
                    applied_source_contents.c.source_file_id == source_file_id
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            return cast(datetime, existing)

        rebuild_applied_at: datetime | None = None
        if rebuild_run_id is not None:
            rebuild_applied_at = cast(
                datetime | None,
                (
                    await connection.execute(
                        select(
                            func.coalesce(
                                normalized_rebuild_files.c.original_applied_at,
                                normalized_rebuild_files.c.effective_source_timestamp,
                            )
                        ).where(
                            normalized_rebuild_files.c.rebuild_run_id == rebuild_run_id,
                            normalized_rebuild_files.c.source_file_id == source_file_id,
                        )
                    )
                ).scalar_one_or_none(),
            )
        applied_at = rebuild_applied_at or (
            archive_applied_at if replay_run_id is not None else default_applied_at
        )
        inserted_at = (
            await connection.execute(
                pg_insert(applied_source_contents)
                .values(
                    retailer_id=retailer_id,
                    portal_id=portal_id,
                    document_type=document_type.value,
                    content_sha256=content_sha256,
                    source_file_id=source_file_id,
                    applied_at=applied_at,
                )
                .on_conflict_do_nothing(index_elements=[applied_source_contents.c.source_file_id])
                .returning(applied_source_contents.c.applied_at)
            )
        ).scalar_one_or_none()
        if inserted_at is not None:
            return cast(datetime, inserted_at)
        concurrent_applied_at = (
            await connection.execute(
                select(applied_source_contents.c.applied_at).where(
                    applied_source_contents.c.source_file_id == source_file_id
                )
            )
        ).scalar_one()
        return cast(datetime, concurrent_applied_at)

    async def _prior_apply_summary(
        self,
        connection: AsyncConnection,
        prior_source_file_id: UUID,
    ) -> ApplySummary:
        row = (
            await connection.execute(
                select(
                    ingestion_runs.c.inserted_records,
                    ingestion_runs.c.updated_records,
                    ingestion_runs.c.unchanged_records,
                    ingestion_runs.c.unavailable_records,
                    ingestion_runs.c.history_events,
                )
                .where(ingestion_runs.c.source_file_id == prior_source_file_id)
                .order_by(ingestion_runs.c.attempt.desc())
                .limit(1)
            )
        ).one()
        return ApplySummary(
            inserted=int(row.inserted_records),
            updated=int(row.updated_records),
            unchanged=int(row.unchanged_records),
            unavailable=int(row.unavailable_records),
            history_events=int(row.history_events),
        )

    async def _archive_row(self, source_file_id: UUID) -> RowMapping:
        async with self._transaction(source_file_id) as connection:
            row = (
                (
                    await connection.execute(
                        select(
                            raw_archive_objects.c.object_key,
                            raw_archive_objects.c.content_sha256,
                        )
                        .join(
                            source_files,
                            source_files.c.raw_archive_object_id == raw_archive_objects.c.id,
                        )
                        .where(source_files.c.id == source_file_id)
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise PersistenceNotFoundError(f"Source file {source_file_id} has no archived object")
        return row

    async def _apply_stores(
        self,
        connection: AsyncConnection,
        *,
        source_file_id: UUID,
        retailer_id: UUID,
        portal_id: UUID,
        applied_at: datetime,
        minimum_full_records: int,
        maximum_drop_fraction: float,
    ) -> ApplySummary:
        return await _apply_stores(
            connection,
            source_file_id=source_file_id,
            retailer_id=retailer_id,
            portal_id=portal_id,
            applied_at=applied_at,
            minimum_full_records=minimum_full_records,
            maximum_drop_fraction=maximum_drop_fraction,
        )

    async def _apply_prices(
        self,
        connection: AsyncConnection,
        *,
        source_file_id: UUID,
        retailer_id: UUID,
        portal_id: UUID,
        applied_at: datetime,
        is_full: bool,
        minimum_full_records: int,
        maximum_drop_fraction: float,
    ) -> ApplySummary:
        return await _apply_prices(
            connection,
            source_file_id=source_file_id,
            retailer_id=retailer_id,
            portal_id=portal_id,
            applied_at=applied_at,
            is_full=is_full,
            minimum_full_records=minimum_full_records,
            maximum_drop_fraction=maximum_drop_fraction,
        )

    async def _apply_promotions(
        self,
        connection: AsyncConnection,
        *,
        source_file_id: UUID,
        retailer_id: UUID,
        portal_id: UUID,
        applied_at: datetime,
        is_full: bool,
        minimum_full_records: int,
        maximum_drop_fraction: float,
    ) -> ApplySummary:
        return await _apply_promotions(
            connection,
            source_file_id=source_file_id,
            retailer_id=retailer_id,
            portal_id=portal_id,
            applied_at=applied_at,
            is_full=is_full,
            minimum_full_records=minimum_full_records,
            maximum_drop_fraction=maximum_drop_fraction,
        )


def _rediscovery_updates(
    row: RowMapping,
    remote_file: RemoteFile,
    *,
    owned_refresh: bool,
) -> dict[str, object]:
    immutable_identity = (
        ("original_filename", str(row.original_filename), remote_file.original_filename),
        ("document_type", str(row.document_type), remote_file.document_type.value),
        ("compression", str(row.compression), remote_file.compression.value),
        ("protocol", str(row.protocol), remote_file.protocol.value),
    )
    for field_name, existing, candidate in immutable_identity:
        if existing != candidate:
            raise PersistenceConflictError(
                f"Rediscovered source conflicts with its original {field_name}"
            )

    updates: dict[str, object] = {}
    archive_attached = row.raw_archive_object_id is not None
    existing_url = str(row.download_url)
    if existing_url != remote_file.download_url:
        if remote_file.protocol is not SourceProtocol.HTTPS or not _is_signed_url_rotation(
            existing_url,
            remote_file.download_url,
        ):
            raise PersistenceConflictError(
                "Rediscovered source conflicts with its original download_url"
            )
        if owned_refresh and not archive_attached:
            updates["download_url"] = remote_file.download_url

    candidate_evidence = (
        ("source_timestamp", row.source_timestamp, remote_file.source_timestamp),
        ("declared_content_length", row.declared_content_length, remote_file.content_length),
        ("media_type", row.media_type, remote_file.media_type),
        ("etag", row.etag, remote_file.etag),
        ("last_modified", row.last_modified, remote_file.last_modified),
    )
    for field_name, existing_evidence, candidate_evidence_value in candidate_evidence:
        if (
            existing_evidence is not None
            and candidate_evidence_value is not None
            and existing_evidence != candidate_evidence_value
        ):
            raise PersistenceConflictError(
                f"Rediscovered source conflicts with its original {field_name}"
            )
        if (
            owned_refresh
            and not archive_attached
            and existing_evidence is None
            and candidate_evidence_value is not None
        ):
            updates[field_name] = candidate_evidence_value

    existing_metadata = tuple(tuple(pair) for pair in row.response_metadata)
    candidate_metadata = remote_file.response_metadata
    if existing_metadata and candidate_metadata and existing_metadata != candidate_metadata:
        raise PersistenceConflictError(
            "Rediscovered source conflicts with its original response_metadata"
        )
    if owned_refresh and not archive_attached and not existing_metadata and candidate_metadata:
        updates["response_metadata"] = list(candidate_metadata)
    return updates


def _is_signed_url_rotation(existing: str, candidate: str) -> bool:
    existing_url = urlsplit(existing)
    candidate_url = urlsplit(candidate)
    if existing_url.scheme.casefold() != "https" or candidate_url.scheme.casefold() != "https":
        return False
    if _url_identity(existing_url) != _url_identity(candidate_url):
        return False
    existing_query = tuple(
        (key.casefold(), value)
        for key, value in parse_qsl(existing_url.query, keep_blank_values=True)
    )
    candidate_query = tuple(
        (key.casefold(), value)
        for key, value in parse_qsl(candidate_url.query, keep_blank_values=True)
    )
    existing_stable = sorted(pair for pair in existing_query if not _is_signed_query_key(pair[0]))
    candidate_stable = sorted(pair for pair in candidate_query if not _is_signed_query_key(pair[0]))
    existing_signed = sorted(pair for pair in existing_query if _is_signed_query_key(pair[0]))
    candidate_signed = sorted(pair for pair in candidate_query if _is_signed_query_key(pair[0]))
    return (
        existing_stable == candidate_stable
        and bool(existing_signed)
        and bool(candidate_signed)
        and existing_signed != candidate_signed
    )


def _url_identity(value: SplitResult) -> tuple[str | None, int | None, str, str]:
    hostname = value.hostname
    return (
        hostname.casefold() if hostname is not None else None,
        value.port,
        value.path,
        value.fragment,
    )


def _is_signed_query_key(key: str) -> bool:
    return key in _SIGNED_QUERY_KEYS or key.startswith(("x-amz-", "x-goog-"))


def _registered_source_file(
    row: RowMapping,
    *,
    already_registered: bool,
) -> RegisteredSourceFile:
    response_metadata = tuple(tuple(pair) for pair in row.response_metadata)
    remote_file = RemoteFile(
        retailer_id=str(row.retailer_source_key),
        portal_id=str(row.portal_source_key),
        protocol=SourceProtocol(row.protocol),
        remote_id=str(row.remote_id),
        download_url=str(row.download_url),
        original_filename=str(row.original_filename),
        document_type=DocumentType(row.document_type),
        compression=CompressionFormat(row.compression),
        discovered_at=row.discovered_at,
        source_timestamp=row.source_timestamp,
        content_length=row.declared_content_length,
        media_type=row.media_type,
        etag=row.etag,
        last_modified=row.last_modified,
        response_metadata=response_metadata,
    )
    return RegisteredSourceFile(
        source_file_id=row.id,
        remote_file=remote_file,
        status=IngestionStatus(row.status),
        already_registered=already_registered,
        completed_content_sha256=(
            str(row.content_sha256)
            if row.status == IngestionStatus.COMPLETED.value and row.content_sha256
            else None
        ),
        archive_object_key=(str(row.object_key) if row.object_key is not None else None),
        content_sha256=(str(row.content_sha256) if row.content_sha256 is not None else None),
        archive_content_length=(
            int(row.archive_content_length) if row.archive_content_length is not None else None
        ),
    )


def _event_rows(
    event: ParsedEvent,
    *,
    source_file_id: UUID,
    ingestion_run_id: UUID | None,
    replay_run_id: UUID | None,
    metadata_index: int,
) -> tuple[Table, list[tuple[object, ...]]]:
    if isinstance(event, DocumentMetadata):
        return staged_documents, [
            (
                source_file_id,
                metadata_index,
                event.document_type.value,
                event.chain_id,
                event.subchain_id,
                event.store_id,
                event.audit_number,
                event.source_updated_at,
            )
        ]
    if isinstance(event, StoreRecord):
        return staged_stores, [
            (
                source_file_id,
                event.record_index,
                event.chain_id,
                event.subchain_id,
                event.store_id,
                event.audit_number,
                event.store_type,
                event.chain_name,
                event.subchain_name,
                event.store_name,
                event.address,
                event.city,
                event.postal_code,
            )
        ]
    if isinstance(event, PriceRecord):
        return staged_prices, [
            (
                source_file_id,
                event.record_index,
                event.chain_id,
                event.subchain_id,
                event.store_id,
                event.item_code,
                event.item_code if is_valid_gtin(event.item_code) else None,
                event.item_type,
                event.item_name,
                event.manufacturer_name,
                event.manufacturer_country,
                event.manufacturer_description,
                event.unit_quantity,
                event.quantity,
                event.unit_of_measure,
                event.is_weighted,
                event.quantity_in_package,
                event.item_price,
                event.unit_of_measure_price,
                event.allow_discount,
                event.item_status,
                event.price_updated_at,
                event.last_sale_at,
                event.audit_number,
            )
        ]
    if isinstance(event, PromotionRecord):
        return staged_promotions, [
            (
                source_file_id,
                event.record_index,
                event.chain_id,
                event.subchain_id,
                event.source_store_id or "",
                event.promotion_id,
                event.description,
                event.discount_kind.value,
                event.starts_at,
                event.ends_at,
                event.reward_type,
                event.allows_multiple_discounts,
                event.minimum_quantity,
                event.maximum_quantity,
                event.discount_rate,
                event.minimum_purchase,
                event.discounted_price,
                event.discounted_unit_price,
                event.minimum_items_offered,
                event.additional_restrictions,
                event.remarks,
                event.is_active,
                _promotion_fingerprint(event),
            )
        ]
    return validation_issues, [
        (
            source_file_id,
            ingestion_run_id,
            replay_run_id,
            event.severity.value,
            event.code,
            event.message,
            event.record_index,
            event.field_name,
            event.rejected_value,
        )
    ]


def _promotion_child_rows(
    event: PromotionRecord,
) -> dict[Table, list[tuple[object, ...]]]:
    store_ids = effective_promotion_store_ids(event)
    return {
        staged_promotion_items: [
            (
                event.source_file_id,
                event.record_index,
                index,
                item.item_code,
                item.item_type,
                item.is_gift,
            )
            for index, item in enumerate(event.items)
        ],
        staged_promotion_stores: [
            (event.source_file_id, event.record_index, index, store_id)
            for index, store_id in enumerate(store_ids)
        ],
        staged_promotion_clubs: [
            (event.source_file_id, event.record_index, index, club_id)
            for index, club_id in enumerate(event.club_ids)
        ],
    }


def _promotion_fingerprint(event: PromotionRecord) -> str:
    payload = {
        "description": event.description,
        "discount_kind": event.discount_kind.value,
        "starts_at": _json_value(event.starts_at),
        "ends_at": _json_value(event.ends_at),
        "items": sorted((item.item_code, item.item_type, item.is_gift) for item in event.items),
        "stores": sorted(event.store_ids),
        "clubs": sorted(event.club_ids),
        "reward_type": event.reward_type,
        "allows_multiple_discounts": event.allows_multiple_discounts,
        "minimum_quantity": _json_value(event.minimum_quantity),
        "maximum_quantity": _json_value(event.maximum_quantity),
        "discount_rate": _json_value(event.discount_rate),
        "minimum_purchase": _json_value(event.minimum_purchase),
        "discounted_price": _json_value(event.discounted_price),
        "discounted_unit_price": _json_value(event.discounted_unit_price),
        "minimum_items_offered": event.minimum_items_offered,
        "additional_restrictions": event.additional_restrictions,
        "remarks": event.remarks,
        "is_active": event.is_active,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _json_value(value: datetime | Decimal | None) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value) if value is not None else None


def _empty_stage_counts() -> dict[str, int]:
    return {
        "metadata_records": 0,
        "store_records": 0,
        "price_records": 0,
        "promotion_records": 0,
        "warnings": 0,
        "rejected_records": 0,
        "file_quarantines": 0,
        "validation_issue_bytes": 0,
        "sampled_validation_issues": 0,
    }


async def _attempt_issue_summary(
    connection: AsyncConnection,
    *,
    ingestion_run_id: UUID | None,
    replay_run_id: UUID | None,
    lock: bool,
) -> StageSummary:
    if ingestion_run_id is not None:
        statement = select(
            ingestion_runs.c.warnings,
            ingestion_runs.c.rejected_records,
            ingestion_runs.c.file_quarantine_issues,
            ingestion_runs.c.validation_issue_bytes,
            ingestion_runs.c.validation_issue_samples,
        ).where(ingestion_runs.c.id == ingestion_run_id)
        if lock:
            statement = statement.with_for_update()
        row = (await connection.execute(statement)).mappings().one()
        return StageSummary(
            warnings=int(row["warnings"]),
            rejected_records=int(row["rejected_records"]),
            file_quarantines=int(row["file_quarantine_issues"]),
            validation_issue_bytes=int(row["validation_issue_bytes"]),
            sampled_validation_issues=int(row["validation_issue_samples"]),
        )
    if replay_run_id is None:
        raise PersistenceConflictError("No active ingestion or replay attempt exists")
    statement = select(replay_runs.c.result_summary).where(replay_runs.c.id == replay_run_id)
    if lock:
        statement = statement.with_for_update()
    payload = (await connection.execute(statement)).scalar_one() or {}
    stage = dict(payload.get("stage") or {})
    return StageSummary(
        warnings=int(stage.get("warnings", 0)),
        rejected_records=int(stage.get("rejected_records", 0)),
        file_quarantines=int(stage.get("file_quarantines", 0)),
        validation_issue_bytes=int(stage.get("validation_issue_bytes", 0)),
        sampled_validation_issues=int(stage.get("sampled_validation_issues", 0)),
    )


async def _staging_summary_for_attempt(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    ingestion_run_id: UUID | None,
    replay_run_id: UUID | None,
) -> StageSummary:
    counts = (
        await connection.execute(
            select(
                select(func.count())
                .select_from(staged_documents)
                .where(staged_documents.c.source_file_id == source_file_id)
                .scalar_subquery(),
                select(func.count())
                .select_from(staged_stores)
                .where(staged_stores.c.source_file_id == source_file_id)
                .scalar_subquery(),
                select(func.count())
                .select_from(staged_prices)
                .where(staged_prices.c.source_file_id == source_file_id)
                .scalar_subquery(),
                select(func.count())
                .select_from(staged_promotions)
                .where(staged_promotions.c.source_file_id == source_file_id)
                .scalar_subquery(),
            )
        )
    ).one()
    issues = await _attempt_issue_summary(
        connection,
        ingestion_run_id=ingestion_run_id,
        replay_run_id=replay_run_id,
        lock=False,
    )
    return StageSummary(
        metadata_records=int(counts[0]),
        store_records=int(counts[1]),
        price_records=int(counts[2]),
        promotion_records=int(counts[3]),
        warnings=issues.warnings,
        rejected_records=issues.rejected_records,
        file_quarantines=issues.file_quarantines,
        validation_issue_bytes=issues.validation_issue_bytes,
        sampled_validation_issues=issues.sampled_validation_issues,
    )


async def _clone_staging_table(
    connection: AsyncConnection,
    staging_table: Table,
    *,
    source_file_id: UUID,
    candidate_source_file_id: UUID,
) -> None:
    column_names = [column.name for column in staging_table.columns]
    selected_columns = [
        (
            literal(source_file_id, type_=staging_table.c.source_file_id.type)
            if column.name == "source_file_id"
            else column
        )
        for column in staging_table.columns
    ]
    await connection.execute(
        insert(staging_table).from_select(
            column_names,
            select(*selected_columns).where(
                staging_table.c.source_file_id == candidate_source_file_id
            ),
        )
    )


async def _copy_records(
    driver: _CopyConnection,
    table: Table,
    records: list[tuple[object, ...]],
) -> None:
    columns = [
        column.name
        for column in table.columns
        if column.name != "id" and column.server_default is None
    ]
    await driver.copy_records_to_table(
        table.name,
        records=records,
        columns=columns,
    )


async def _record_generated_issue(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    ingestion_run_id: UUID | None,
    replay_run_id: UUID | None,
    severity: IssueSeverity,
    code: str,
    message: str,
    maximum_issues: int,
    maximum_issue_bytes: int,
    maximum_evidence: int,
) -> None:
    current = await _attempt_issue_summary(
        connection,
        ingestion_run_id=ingestion_run_id,
        replay_run_id=replay_run_id,
        lock=True,
    )
    issue = ValidationIssue(
        source_file_id=source_file_id,
        severity=severity,
        code=code,
        message=message,
    )
    issue_bytes = validation_issue_charge(issue)
    issue_count = current.warnings + current.rejected_records + current.file_quarantines
    if issue_count + 1 > maximum_issues or (
        current.validation_issue_bytes + issue_bytes > maximum_issue_bytes
    ):
        raise QuarantinedFileError(
            "Validation issue evidence exceeds the configured persistence limit"
        )
    sampled = current.sampled_validation_issues < maximum_evidence
    if sampled:
        await connection.execute(
            insert(validation_issues).values(
                source_file_id=source_file_id,
                ingestion_run_id=ingestion_run_id,
                replay_run_id=replay_run_id,
                severity=severity.value,
                code=code,
                message=message,
            )
        )
    summary = StageSummary(
        warnings=int(severity is IssueSeverity.WARNING),
        rejected_records=int(severity is IssueSeverity.RECORD_REJECTION),
        file_quarantines=int(severity is IssueSeverity.FILE_QUARANTINE),
        validation_issue_bytes=issue_bytes,
        sampled_validation_issues=int(sampled),
    )
    if ingestion_run_id is not None:
        await connection.execute(
            update(ingestion_runs)
            .where(ingestion_runs.c.id == ingestion_run_id)
            .values(
                warnings=ingestion_runs.c.warnings + summary.warnings,
                rejected_records=(ingestion_runs.c.rejected_records + summary.rejected_records),
                file_quarantine_issues=(
                    ingestion_runs.c.file_quarantine_issues + summary.file_quarantines
                ),
                validation_issue_bytes=(ingestion_runs.c.validation_issue_bytes + issue_bytes),
                validation_issue_samples=(ingestion_runs.c.validation_issue_samples + int(sampled)),
            )
        )
    elif replay_run_id is not None:
        payload = (
            await connection.execute(
                select(replay_runs.c.result_summary).where(replay_runs.c.id == replay_run_id)
            )
        ).scalar_one() or {}
        updated_payload = dict(payload)
        stage = dict(updated_payload.get("stage") or {})
        for key, value in (
            ("warnings", summary.warnings),
            ("rejected_records", summary.rejected_records),
            ("file_quarantines", summary.file_quarantines),
            ("validation_issue_bytes", summary.validation_issue_bytes),
            ("sampled_validation_issues", summary.sampled_validation_issues),
        ):
            stage[key] = int(stage.get(key, 0)) + value
        updated_payload["stage"] = stage
        await connection.execute(
            update(replay_runs)
            .where(replay_runs.c.id == replay_run_id)
            .values(result_summary=updated_payload)
        )


async def _prepare_source_scopes(
    connection: AsyncConnection,
    source_file_id: UUID,
    document_type: DocumentType,
) -> str:
    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_incoming_source_scopes (
                subchain_code text NOT NULL,
                source_scope_code text NOT NULL,
                PRIMARY KEY (subchain_code, source_scope_code)
            ) ON COMMIT DROP
            """
        )
    )
    if document_type is DocumentType.STORES:
        family = "stores"
        await connection.execute(
            text(
                """
                INSERT INTO makolet_incoming_source_scopes
                    (subchain_code, source_scope_code)
                SELECT DISTINCT subchain_id, ''
                  FROM staged_stores
                 WHERE source_file_id = :source_file_id
                """
            ),
            {"source_file_id": source_file_id},
        )
    elif document_type.is_price:
        family = "prices"
        await connection.execute(
            text(
                """
                INSERT INTO makolet_incoming_source_scopes
                    (subchain_code, source_scope_code)
                SELECT DISTINCT subchain_id, source_store_id
                  FROM staged_prices
                 WHERE source_file_id = :source_file_id
                """
            ),
            {"source_file_id": source_file_id},
        )
    elif document_type.is_promotion:
        family = "promotions"
        await connection.execute(
            text(
                """
                INSERT INTO makolet_incoming_source_scopes
                    (subchain_code, source_scope_code)
                SELECT DISTINCT subchain_id, source_scope_store_code
                  FROM staged_promotions
                 WHERE source_file_id = :source_file_id
                """
            ),
            {"source_file_id": source_file_id},
        )
    else:
        raise SnapshotValidationError("Unknown document types do not have source scopes")
    await connection.execute(
        text(
            """
            INSERT INTO makolet_incoming_source_scopes
                (subchain_code, source_scope_code)
            SELECT COALESCE(document.subchain_id, ''),
                   CASE
                       WHEN :family = 'stores' THEN ''
                       ELSE COALESCE(document.store_id, '')
                   END
              FROM staged_documents document
             WHERE document.source_file_id = :source_file_id
               AND NOT EXISTS (
                   SELECT 1 FROM makolet_incoming_source_scopes
               )
            ON CONFLICT DO NOTHING
            """
        ),
        {"source_file_id": source_file_id, "family": family},
    )
    await connection.execute(
        text(
            """
            INSERT INTO makolet_incoming_source_scopes
                (subchain_code, source_scope_code)
            SELECT '', ''
             WHERE NOT EXISTS (
                 SELECT 1 FROM makolet_incoming_source_scopes
             )
            """
        )
    )
    await connection.execute(text("ANALYZE makolet_incoming_source_scopes"))
    return family


async def _validate_source_ordering(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    source_row: RowMapping,
    document_type: DocumentType,
    observed_at: datetime,
    ingestion_run_id: UUID | None,
    replay_run_id: UUID | None,
    maximum_issues: int,
    maximum_issue_bytes: int,
    maximum_evidence: int,
) -> tuple[datetime, tuple[str, str] | None]:
    family = await _prepare_source_scopes(connection, source_file_id, document_type)
    metadata_row = (
        await connection.execute(
            select(
                func.min(staged_documents.c.source_updated_at),
                func.max(staged_documents.c.source_updated_at),
                func.count(func.distinct(staged_documents.c.source_updated_at)),
            ).where(
                staged_documents.c.source_file_id == source_file_id,
                staged_documents.c.source_updated_at.is_not(None),
            )
        )
    ).one()
    metadata_min, metadata_max, metadata_count = metadata_row
    if metadata_max is None and source_row.source_timestamp is None:
        await _record_generated_issue(
            connection,
            source_file_id=source_file_id,
            ingestion_run_id=ingestion_run_id,
            replay_run_id=replay_run_id,
            severity=IssueSeverity.WARNING,
            code="source_timestamp_missing",
            message=(
                "Source has no declared timestamp; archive attachment time is used for ordering"
            ),
            maximum_issues=maximum_issues,
            maximum_issue_bytes=maximum_issue_bytes,
            maximum_evidence=maximum_evidence,
        )
    declared_timestamps = tuple(
        cast(datetime, value)
        for value in (metadata_max, source_row.source_timestamp)
        if value is not None
    )
    effective_timestamp = (
        max(declared_timestamps)
        if declared_timestamps
        else cast(datetime, source_row.download_finished_at or source_row.discovered_at)
    )
    quarantine: tuple[str, str] | None = None
    if int(metadata_count) > 1 and metadata_min != metadata_max:
        quarantine = (
            "source_timestamp_conflict",
            "One source file declares conflicting effective timestamps",
        )
    elif effective_timestamp > observed_at + MAXIMUM_FUTURE_SOURCE_SKEW:
        quarantine = (
            "source_timestamp_future",
            "Source timestamp is implausibly far in the future",
        )
    existing_rows = (
        (
            await connection.execute(
                text(
                    """
                    SELECT watermark.subchain_code,
                           watermark.source_scope_code,
                           watermark.effective_source_timestamp,
                           watermark.source_content_sha256
                      FROM source_scope_watermarks watermark
                      JOIN makolet_incoming_source_scopes incoming
                        ON incoming.subchain_code = watermark.subchain_code
                       AND incoming.source_scope_code = watermark.source_scope_code
                     WHERE watermark.retailer_id = :retailer_id
                       AND watermark.portal_id = :portal_id
                       AND watermark.document_family = :family
                     FOR UPDATE OF watermark
                    """
                ),
                {
                    "retailer_id": source_row.retailer_id,
                    "portal_id": source_row.portal_id,
                    "family": family,
                },
            )
        )
        .mappings()
        .all()
    )
    if quarantine is None:
        for existing in existing_rows:
            previous = cast(datetime, existing["effective_source_timestamp"])
            if effective_timestamp < previous:
                quarantine = (
                    "source_timestamp_regression",
                    "Source timestamp precedes current normalized state; archive it with "
                    "--archive-only and run a normalized rebuild",
                )
                break
            if effective_timestamp == previous and str(existing["source_content_sha256"]) != str(
                source_row.content_sha256
            ):
                quarantine = (
                    "source_timestamp_conflict",
                    "Different source content claims the same scope timestamp",
                )
                break
    if effective_timestamp < observed_at - SOURCE_STALENESS_WARNING_AGE:
        await _record_generated_issue(
            connection,
            source_file_id=source_file_id,
            ingestion_run_id=ingestion_run_id,
            replay_run_id=replay_run_id,
            severity=IssueSeverity.WARNING,
            code="source_timestamp_stale",
            message="Source timestamp is older than the configured freshness warning age",
            maximum_issues=maximum_issues,
            maximum_issue_bytes=maximum_issue_bytes,
            maximum_evidence=maximum_evidence,
        )
    if quarantine is not None:
        await _record_generated_issue(
            connection,
            source_file_id=source_file_id,
            ingestion_run_id=ingestion_run_id,
            replay_run_id=replay_run_id,
            severity=IssueSeverity.FILE_QUARANTINE,
            code=quarantine[0],
            message=quarantine[1],
            maximum_issues=maximum_issues,
            maximum_issue_bytes=maximum_issue_bytes,
            maximum_evidence=maximum_evidence,
        )
    return effective_timestamp, quarantine


async def _advance_source_watermarks(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    retailer_id: UUID,
    portal_id: UUID,
    document_family: str,
    effective_timestamp: datetime,
    content_sha256: str,
) -> None:
    await connection.execute(
        text(
            """
            INSERT INTO source_scope_watermarks (
                retailer_id, portal_id, document_family, subchain_code,
                source_scope_code, effective_source_timestamp,
                source_content_sha256, source_file_id
            )
            SELECT :retailer_id, :portal_id, :document_family,
                   incoming.subchain_code, incoming.source_scope_code,
                   :effective_timestamp, :content_sha256, :source_file_id
              FROM makolet_incoming_source_scopes incoming
            ON CONFLICT (
                retailer_id, portal_id, document_family,
                subchain_code, source_scope_code
            ) DO UPDATE
                  SET effective_source_timestamp = excluded.effective_source_timestamp,
                      source_content_sha256 = excluded.source_content_sha256,
                      source_file_id = excluded.source_file_id,
                      updated_at = clock_timestamp()
            """
        ),
        {
            "retailer_id": retailer_id,
            "portal_id": portal_id,
            "document_family": document_family,
            "effective_timestamp": effective_timestamp,
            "content_sha256": content_sha256,
            "source_file_id": source_file_id,
        },
    )


async def _analyze_staging_for_apply(
    connection: AsyncConnection,
    document_type: DocumentType,
) -> None:
    """Refresh planner statistics once after all COPY batches have committed."""
    if document_type is DocumentType.STORES:
        table_names = "staged_documents, staged_stores"
    elif document_type.is_price:
        table_names = "staged_documents, staged_prices"
    elif document_type.is_promotion:
        table_names = (
            "staged_documents, staged_promotions, staged_promotion_items, "
            "staged_promotion_stores, staged_promotion_clubs"
        )
    else:
        return
    await connection.execute(text(f"ANALYZE {table_names}"))


_STORE_INCOMING = """
SELECT DISTINCT ON (subchain_id, source_store_id)
       chain_id, subchain_id, source_store_id, audit_number, store_type,
       chain_name, subchain_name, store_name, address, city, postal_code
  FROM staged_stores
 WHERE source_file_id = :source_file_id
 ORDER BY subchain_id, source_store_id, record_index DESC
"""


async def _apply_stores(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    retailer_id: UUID,
    portal_id: UUID,
    applied_at: datetime,
    minimum_full_records: int,
    maximum_drop_fraction: float,
) -> ApplySummary:
    roster_counts = (
        await connection.execute(
            text(
                f"""
                WITH incoming AS ({_STORE_INCOMING})
                SELECT (SELECT count(*) FROM incoming) AS incoming_count,
                       count(store.id) FILTER (WHERE store.is_active) AS prior_count
                  FROM stores store
                 WHERE store.retailer_id = :retailer_id
                   AND store.portal_id = :portal_id
                """
            ),
            {
                "source_file_id": source_file_id,
                "retailer_id": retailer_id,
                "portal_id": portal_id,
            },
        )
    ).one()
    _validate_full_snapshot(
        incoming_count=int(roster_counts.incoming_count),
        prior_count=int(roster_counts.prior_count),
        minimum_full_records=minimum_full_records,
        maximum_drop_fraction=maximum_drop_fraction,
        scope="store portal roster",
    )
    counts = (
        await connection.execute(
            text(
                f"""
                WITH incoming AS ({_STORE_INCOMING})
                SELECT count(*) FILTER (WHERE existing.id IS NULL) AS inserted,
                       count(*) FILTER (
                           WHERE existing.id IS NOT NULL AND
                                 ROW(existing.chain_code, existing.audit_number,
                                     existing.store_type, existing.chain_name,
                                     existing.subchain_name, existing.name,
                                     existing.address, existing.city,
                                     existing.postal_code, existing.is_active)
                                 IS DISTINCT FROM
                                 ROW(incoming.chain_id, incoming.audit_number,
                                     incoming.store_type, incoming.chain_name,
                                     incoming.subchain_name, incoming.store_name,
                                     incoming.address, incoming.city,
                                     incoming.postal_code, true)
                       ) AS updated,
                       count(*) FILTER (
                           WHERE existing.id IS NOT NULL AND
                                 ROW(existing.chain_code, existing.audit_number,
                                     existing.store_type, existing.chain_name,
                                     existing.subchain_name, existing.name,
                                     existing.address, existing.city,
                                     existing.postal_code, existing.is_active)
                                 IS NOT DISTINCT FROM
                                 ROW(incoming.chain_id, incoming.audit_number,
                                     incoming.store_type, incoming.chain_name,
                                     incoming.subchain_name, incoming.store_name,
                                     incoming.address, incoming.city,
                                     incoming.postal_code, true)
                       ) AS unchanged
                  FROM incoming
                  LEFT JOIN stores existing
                    ON existing.retailer_id = :retailer_id
                   AND existing.portal_id = :portal_id
                   AND existing.subchain_code = incoming.subchain_id
                   AND existing.source_store_code = incoming.source_store_id
                """
            ),
            {
                "source_file_id": source_file_id,
                "retailer_id": retailer_id,
                "portal_id": portal_id,
            },
        )
    ).one()
    await connection.execute(
        text(
            f"""
            INSERT INTO stores (
                id, retailer_id, portal_id, chain_code, subchain_code, source_store_code,
                audit_number, store_type, chain_name, subchain_name, name,
                address, city, postal_code, is_active, first_seen_at,
                last_seen_at, last_source_file_id
            )
            SELECT uuidv7(), :retailer_id, :portal_id,
                   chain_id, subchain_id, source_store_id,
                   audit_number, store_type, chain_name, subchain_name, store_name,
                   address, city, postal_code, true, :applied_at, :applied_at,
                   :source_file_id
              FROM ({_STORE_INCOMING}) incoming
            ON CONFLICT (
                retailer_id, portal_id, subchain_code, source_store_code
            ) DO UPDATE
                SET chain_code = excluded.chain_code,
                    audit_number = excluded.audit_number,
                    store_type = excluded.store_type,
                    chain_name = excluded.chain_name,
                    subchain_name = excluded.subchain_name,
                    name = excluded.name,
                    address = excluded.address,
                    city = excluded.city,
                    postal_code = excluded.postal_code,
                    is_active = true,
                    last_seen_at = excluded.last_seen_at,
                    last_source_file_id = excluded.last_source_file_id
            """
        ),
        {
            "source_file_id": source_file_id,
            "retailer_id": retailer_id,
            "portal_id": portal_id,
            "applied_at": applied_at,
        },
    )
    await connection.execute(
        text(
            f"""
            WITH incoming AS ({_STORE_INCOMING})
            INSERT INTO store_aliases (
                id, store_id, retailer_id, portal_id,
                alias_kind, alias_value, created_at
            )
            SELECT uuidv7(), store.id, :retailer_id, :portal_id,
                   'source_store_code',
                   incoming.source_store_id, :applied_at
              FROM incoming
              JOIN stores store
                ON store.retailer_id = :retailer_id
               AND store.portal_id = :portal_id
               AND store.subchain_code = incoming.subchain_id
               AND store.source_store_code = incoming.source_store_id
            ON CONFLICT (
                retailer_id, portal_id, alias_kind, alias_value
            ) DO NOTHING
            """
        ),
        {
            "source_file_id": source_file_id,
            "retailer_id": retailer_id,
            "portal_id": portal_id,
            "applied_at": applied_at,
        },
    )
    deactivated = int(
        (
            await connection.execute(
                text(
                    f"""
                    WITH incoming AS ({_STORE_INCOMING})
                    UPDATE stores store
                       SET is_active = false,
                           last_source_file_id = :source_file_id
                     WHERE store.retailer_id = :retailer_id
                       AND store.portal_id = :portal_id
                       AND store.is_active
                       AND NOT EXISTS (
                           SELECT 1 FROM incoming
                            WHERE incoming.subchain_id = store.subchain_code
                              AND incoming.source_store_id = store.source_store_code
                       )
                    RETURNING store.id
                    """
                ),
                {
                    "source_file_id": source_file_id,
                    "retailer_id": retailer_id,
                    "portal_id": portal_id,
                },
            )
        ).rowcount
    )
    return ApplySummary(
        inserted=int(counts.inserted),
        updated=int(counts.updated),
        unchanged=int(counts.unchanged),
        unavailable=deactivated,
        history_events=0,
    )


_PRICE_INCOMING = """
SELECT DISTINCT ON (subchain_id, source_store_id, source_item_code)
       chain_id, subchain_id, source_store_id, source_item_code, gtin,
       item_type, item_name, manufacturer_name, manufacturer_country,
       manufacturer_description, unit_quantity, quantity, unit_of_measure,
       is_weighted, quantity_in_package, item_price, unit_of_measure_price,
       allow_discount, item_status, price_updated_at, last_sale_at, audit_number
  FROM staged_prices
 WHERE source_file_id = :source_file_id
 ORDER BY subchain_id, source_store_id, source_item_code, record_index DESC
"""

_MAPPED_PRICE_INCOMING = f"""
SELECT incoming.*, item.retailer_item_id, store.id AS store_id,
       COALESCE(incoming.item_status <> 0, true) AS is_available
  FROM ({_PRICE_INCOMING}) incoming
  JOIN makolet_exact_gtin_items item
    ON item.source_item_code = incoming.source_item_code
  JOIN makolet_price_stores store
    ON store.subchain_code = incoming.subchain_id
   AND store.source_store_code = incoming.source_store_id
"""

_MAPPED_PRICE_INCOMING_TABLE = "pg_temp.makolet_mapped_price_incoming"

_MISSING_PRICE_KEYS_SELECT = f"""
WITH incoming AS MATERIALIZED (
    SELECT mapped.retailer_item_id, mapped.store_id
      FROM {_MAPPED_PRICE_INCOMING_TABLE} mapped
),
scopes AS MATERIALIZED (
    SELECT DISTINCT store_id FROM incoming
    UNION
    SELECT store.id
      FROM staged_documents document
      JOIN stores store
        ON store.retailer_id = :retailer_id
       AND store.portal_id = :portal_id
       AND store.subchain_code = COALESCE(document.subchain_id, '')
       AND store.source_store_code = document.store_id
     WHERE document.source_file_id = :source_file_id
       AND document.store_id IS NOT NULL
)
SELECT current.retailer_item_id, current.store_id
  FROM current_availability current
  JOIN scopes ON scopes.store_id = current.store_id
 WHERE current.is_available
EXCEPT
SELECT incoming.retailer_item_id, incoming.store_id
  FROM incoming
"""


async def _apply_prices(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    retailer_id: UUID,
    portal_id: UUID,
    applied_at: datetime,
    is_full: bool,
    minimum_full_records: int,
    maximum_drop_fraction: float,
) -> ApplySummary:
    if is_full:
        await _validate_full_price_snapshot(
            connection,
            source_file_id=source_file_id,
            retailer_id=retailer_id,
            portal_id=portal_id,
            minimum_full_records=minimum_full_records,
            maximum_drop_fraction=maximum_drop_fraction,
        )
    await _upsert_price_stores(
        connection,
        source_file_id=source_file_id,
        retailer_id=retailer_id,
        portal_id=portal_id,
        applied_at=applied_at,
    )
    await _upsert_retailer_items(
        connection,
        source_file_id=source_file_id,
        retailer_id=retailer_id,
        portal_id=portal_id,
        applied_at=applied_at,
    )
    await _materialize_price_maps(
        connection,
        source_file_id=source_file_id,
        retailer_id=retailer_id,
        portal_id=portal_id,
    )
    await _materialize_mapped_price_incoming(
        connection,
        parameters={"source_file_id": source_file_id},
    )
    await _match_exact_gtins(
        connection,
        source_file_id=source_file_id,
        retailer_id=retailer_id,
        portal_id=portal_id,
        applied_at=applied_at,
    )
    parameters = {
        "source_file_id": source_file_id,
        "retailer_id": retailer_id,
        "portal_id": portal_id,
        "applied_at": applied_at,
    }
    counts = (
        await connection.execute(
            text(
                """
                SELECT count(*) FILTER (WHERE current.id IS NULL) AS inserted,
                       count(*) FILTER (
                           WHERE current.id IS NOT NULL AND
                                 ROW(current.item_price,
                                     current.unit_of_measure_price,
                                     current.allow_discount)
                                 IS DISTINCT FROM
                                 ROW(incoming.item_price,
                                     incoming.unit_of_measure_price,
                                     incoming.allow_discount)
                       ) AS updated,
                       count(*) FILTER (
                           WHERE current.id IS NOT NULL AND
                                 ROW(current.item_price,
                                     current.unit_of_measure_price,
                                     current.allow_discount)
                                 IS NOT DISTINCT FROM
                                 ROW(incoming.item_price,
                                     incoming.unit_of_measure_price,
                                     incoming.allow_discount)
                       ) AS unchanged
                  FROM pg_temp.makolet_mapped_price_incoming incoming
                  LEFT JOIN current_prices current
                    ON current.retailer_item_id = incoming.retailer_item_id
                   AND current.store_id = incoming.store_id
                """
            ),
            parameters,
        )
    ).one()
    await connection.execute(
        text(
            """
            WITH changed AS MATERIALIZED (
                SELECT incoming.*
                  FROM pg_temp.makolet_mapped_price_incoming incoming
                  JOIN current_prices current
                    ON current.retailer_item_id = incoming.retailer_item_id
                   AND current.store_id = incoming.store_id
                 WHERE ROW(current.item_price,
                           current.unit_of_measure_price,
                           current.allow_discount)
                       IS DISTINCT FROM
                       ROW(incoming.item_price,
                           incoming.unit_of_measure_price,
                           incoming.allow_discount)
            )
            UPDATE price_history history
               SET valid_to = :applied_at
              FROM changed
             WHERE history.retailer_item_id = changed.retailer_item_id
               AND history.store_id = changed.store_id
               AND history.valid_to IS NULL
            """
        ),
        parameters,
    )
    price_history_created = int(
        (
            await connection.execute(
                text(
                    """
                    INSERT INTO price_history (
                        id, retailer_item_id, store_id, item_price,
                        unit_of_measure_price, allow_discount, source_updated_at,
                        last_sale_at, audit_number, source_file_id, valid_from,
                        valid_to
                    )
                    SELECT uuidv7(), incoming.retailer_item_id, incoming.store_id,
                           incoming.item_price, incoming.unit_of_measure_price,
                           incoming.allow_discount, incoming.price_updated_at,
                           incoming.last_sale_at, incoming.audit_number,
                           :source_file_id, :applied_at, NULL
                      FROM pg_temp.makolet_mapped_price_incoming incoming
                      LEFT JOIN current_prices current
                        ON current.retailer_item_id = incoming.retailer_item_id
                       AND current.store_id = incoming.store_id
                     WHERE current.id IS NULL
                        OR ROW(current.item_price,
                               current.unit_of_measure_price,
                               current.allow_discount)
                           IS DISTINCT FROM
                           ROW(incoming.item_price,
                               incoming.unit_of_measure_price,
                               incoming.allow_discount)
                    RETURNING id
                    """
                ),
                parameters,
            )
        ).rowcount
    )
    await connection.execute(
        text(
            """
            INSERT INTO current_prices (
                id, retailer_item_id, store_id, item_price,
                unit_of_measure_price, allow_discount, source_updated_at,
                last_sale_at, audit_number, source_file_id,
                first_observed_at, last_observed_at
            )
            SELECT uuidv7(), retailer_item_id, store_id, item_price,
                   unit_of_measure_price, allow_discount, price_updated_at,
                   last_sale_at, audit_number, :source_file_id,
                   :applied_at, :applied_at
              FROM pg_temp.makolet_mapped_price_incoming incoming
            ON CONFLICT (retailer_item_id, store_id) DO UPDATE
                SET item_price = excluded.item_price,
                    unit_of_measure_price = excluded.unit_of_measure_price,
                    allow_discount = excluded.allow_discount,
                    source_updated_at = excluded.source_updated_at,
                    last_sale_at = excluded.last_sale_at,
                    audit_number = excluded.audit_number,
                    source_file_id = excluded.source_file_id,
                    last_observed_at = excluded.last_observed_at
            """
        ),
        parameters,
    )
    availability = await _apply_incoming_availability(connection, parameters)
    absent = 0
    if is_full:
        absent = await _reconcile_absent_prices(connection, parameters)
    return ApplySummary(
        inserted=int(counts.inserted),
        updated=int(counts.updated),
        unchanged=int(counts.unchanged),
        unavailable=availability[1] + absent,
        history_events=price_history_created + availability[0] + absent,
    )


async def _validate_full_price_snapshot(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    retailer_id: UUID,
    portal_id: UUID,
    minimum_full_records: int,
    maximum_drop_fraction: float,
) -> None:
    rows = (
        await connection.execute(
            text(
                f"""
                WITH incoming AS ({_PRICE_INCOMING}),
                incoming_counts AS (
                    SELECT subchain_id, source_store_id, count(*) AS incoming_count
                      FROM incoming
                     GROUP BY subchain_id, source_store_id
                ),
                scopes AS (
                    SELECT subchain_id, source_store_id, incoming_count
                      FROM incoming_counts
                    UNION
                    SELECT COALESCE(document.subchain_id, ''), document.store_id, 0
                      FROM staged_documents document
                     WHERE document.source_file_id = :source_file_id
                       AND document.store_id IS NOT NULL
                       AND NOT EXISTS (
                           SELECT 1 FROM incoming_counts count_row
                            WHERE count_row.subchain_id =
                                  COALESCE(document.subchain_id, '')
                              AND count_row.source_store_id = document.store_id
                       )
                )
                SELECT scope.subchain_id, scope.source_store_id,
                       scope.incoming_count,
                       count(availability.id) FILTER (
                           WHERE availability.is_available
                       ) AS prior_count
                  FROM scopes scope
                  LEFT JOIN stores store
                    ON store.retailer_id = :retailer_id
                   AND store.portal_id = :portal_id
                   AND store.subchain_code = scope.subchain_id
                   AND store.source_store_code = scope.source_store_id
                  LEFT JOIN current_availability availability
                    ON availability.store_id = store.id
                 GROUP BY scope.subchain_id, scope.source_store_id,
                          scope.incoming_count
                """
            ),
            {
                "source_file_id": source_file_id,
                "retailer_id": retailer_id,
                "portal_id": portal_id,
            },
        )
    ).all()
    total = sum(int(row.incoming_count) for row in rows)
    if total < minimum_full_records:
        raise SnapshotValidationError(
            f"Full price snapshot has {total} records; minimum is {minimum_full_records}"
        )
    for row in rows:
        if int(row.prior_count) == 0 and int(row.incoming_count) < minimum_full_records:
            raise SnapshotValidationError(
                f"First full price snapshot for store "
                f"{row.subchain_id}/{row.source_store_id} has "
                f"{row.incoming_count} records; minimum is {minimum_full_records}"
            )
        _validate_drop(
            incoming_count=int(row.incoming_count),
            prior_count=int(row.prior_count),
            maximum_drop_fraction=maximum_drop_fraction,
            scope=f"store {row.subchain_id}/{row.source_store_id}",
        )


async def _upsert_price_stores(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    retailer_id: UUID,
    portal_id: UUID,
    applied_at: datetime,
) -> None:
    await connection.execute(
        text(
            f"""
            WITH incoming AS ({_PRICE_INCOMING}),
            distinct_stores AS (
                SELECT DISTINCT ON (subchain_id, source_store_id)
                       chain_id, subchain_id, source_store_id
                  FROM incoming
                 ORDER BY subchain_id, source_store_id
            )
            INSERT INTO stores (
                id, retailer_id, portal_id, chain_code, subchain_code,
                source_store_code,
                name, is_active, first_seen_at, last_seen_at, last_source_file_id
            )
            SELECT uuidv7(), :retailer_id, :portal_id, chain_id, subchain_id,
                   source_store_id, 'Store ' || source_store_id, true,
                   :applied_at, :applied_at, :source_file_id
              FROM distinct_stores
            ON CONFLICT (
                retailer_id, portal_id, subchain_code, source_store_code
            ) DO UPDATE
                SET chain_code = excluded.chain_code,
                    is_active = true,
                    last_seen_at = excluded.last_seen_at,
                    last_source_file_id = excluded.last_source_file_id
            """
        ),
        {
            "source_file_id": source_file_id,
            "retailer_id": retailer_id,
            "portal_id": portal_id,
            "applied_at": applied_at,
        },
    )


async def _upsert_retailer_items(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    retailer_id: UUID,
    portal_id: UUID,
    applied_at: datetime,
) -> None:
    await connection.execute(
        text(
            f"""
            WITH incoming AS ({_PRICE_INCOMING}),
            distinct_items AS (
                SELECT DISTINCT ON (source_item_code) *
                  FROM incoming
                 ORDER BY source_item_code, source_store_id
            )
            INSERT INTO retailer_items (
                id, retailer_id, portal_id, source_item_code,
                gtin, item_type, name,
                manufacturer_name, manufacturer_country,
                manufacturer_description, unit_quantity, quantity,
                unit_of_measure, is_weighted, quantity_in_package,
                first_seen_at, last_seen_at, last_source_file_id
            )
            SELECT uuidv7(), :retailer_id, :portal_id,
                   source_item_code, gtin, item_type,
                   item_name, manufacturer_name, manufacturer_country,
                   manufacturer_description, unit_quantity, quantity,
                   unit_of_measure, is_weighted, quantity_in_package,
                   :applied_at, :applied_at, :source_file_id
              FROM distinct_items
            ON CONFLICT (retailer_id, portal_id, source_item_code) DO UPDATE
                SET gtin = excluded.gtin,
                    item_type = excluded.item_type,
                    name = excluded.name,
                    manufacturer_name = excluded.manufacturer_name,
                    manufacturer_country = excluded.manufacturer_country,
                    manufacturer_description = excluded.manufacturer_description,
                    unit_quantity = excluded.unit_quantity,
                    quantity = excluded.quantity,
                    unit_of_measure = excluded.unit_of_measure,
                    is_weighted = excluded.is_weighted,
                    quantity_in_package = excluded.quantity_in_package,
                    last_seen_at = excluded.last_seen_at,
                    last_source_file_id = excluded.last_source_file_id
            """
        ),
        {
            "source_file_id": source_file_id,
            "retailer_id": retailer_id,
            "portal_id": portal_id,
            "applied_at": applied_at,
        },
    )


async def _materialize_price_maps(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    retailer_id: UUID,
    portal_id: UUID,
) -> None:
    """Materialize this file's source-distinct item and store maps with local stats."""

    parameters = {
        "source_file_id": source_file_id,
        "retailer_id": retailer_id,
        "portal_id": portal_id,
    }
    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_exact_gtin_items ON COMMIT DROP AS
            SELECT item.id AS retailer_item_id, item.retailer_id, item.portal_id,
                   item.source_item_code, item.gtin, item.name AS item_name,
                   item.manufacturer_name, item.quantity, item.unit_of_measure
              FROM (
                    SELECT DISTINCT price.source_item_code
                      FROM staged_prices price
                     WHERE price.source_file_id = :source_file_id
                   ) incoming
              JOIN retailer_items item
                ON item.source_item_code = incoming.source_item_code
             WHERE item.retailer_id = :retailer_id
               AND item.portal_id = :portal_id
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            CREATE UNIQUE INDEX makolet_exact_gtin_items_item_idx
                ON makolet_exact_gtin_items (retailer_item_id)
            """
        )
    )
    await connection.execute(
        text(
            """
            CREATE UNIQUE INDEX makolet_exact_gtin_items_source_code_idx
                ON makolet_exact_gtin_items (source_item_code)
            """
        )
    )
    await connection.execute(
        text(
            """
            CREATE INDEX makolet_exact_gtin_items_gtin_idx
                ON makolet_exact_gtin_items (gtin)
            """
        )
    )
    await connection.execute(text("ANALYZE makolet_exact_gtin_items"))
    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_price_stores ON COMMIT DROP AS
            SELECT store.id, store.subchain_code, store.source_store_code
              FROM (
                    SELECT DISTINCT price.subchain_id, price.source_store_id
                      FROM staged_prices price
                     WHERE price.source_file_id = :source_file_id
                   ) incoming
              JOIN stores store
                ON store.retailer_id = :retailer_id
               AND store.portal_id = :portal_id
               AND store.subchain_code = incoming.subchain_id
               AND store.source_store_code = incoming.source_store_id
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            ALTER TABLE pg_temp.makolet_price_stores
                ADD PRIMARY KEY (subchain_code, source_store_code)
            """
        )
    )
    await connection.execute(text("ANALYZE makolet_price_stores"))


async def _materialize_mapped_price_incoming(
    connection: AsyncConnection,
    parameters: dict[str, object],
) -> None:
    """Map one staged price file once and publish accurate transaction-local stats."""

    # Stores and retailer items can be created earlier in this same transaction. Their
    # shared pg_class statistics therefore cannot describe the rows visible to this
    # apply. Materializing this stable projection avoids repeatedly asking PostgreSQL
    # to plan the same fresh-table joins with one-row estimates at large cardinality.
    await connection.execute(
        text(
            f"""
            CREATE TEMP TABLE makolet_mapped_price_incoming ON COMMIT DROP AS
            {_MAPPED_PRICE_INCOMING}
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            ALTER TABLE pg_temp.makolet_mapped_price_incoming
                ADD PRIMARY KEY (retailer_item_id, store_id)
            """
        )
    )
    await connection.execute(text("ANALYZE pg_temp.makolet_mapped_price_incoming"))


async def _match_exact_gtins(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    retailer_id: UUID,
    portal_id: UUID,
    applied_at: datetime,
) -> None:
    parameters = {
        "source_file_id": source_file_id,
        "retailer_id": retailer_id,
        "portal_id": portal_id,
        "applied_at": applied_at,
    }
    # Work from the source-distinct, analyzed retailer-item projection materialized
    # immediately above rather than the store-multiplying staged price rows.
    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_affected_gtins ON COMMIT DROP AS
            SELECT item.gtin
              FROM makolet_exact_gtin_items item
             WHERE item.gtin IS NOT NULL
            UNION
            SELECT assertion.normalized_value AS gtin
              FROM makolet_exact_gtin_items item
              JOIN retailer_identifier_assertions assertion
                ON assertion.retailer_item_id = item.retailer_item_id
               AND assertion.kind = 'gtin'
               AND assertion.superseded_at IS NULL
            """
        )
    )
    await connection.execute(
        text(
            """
            CREATE UNIQUE INDEX makolet_affected_gtins_idx
                ON makolet_affected_gtins (gtin)
            """
        )
    )
    await connection.execute(text("ANALYZE makolet_affected_gtins"))

    # A new file supersedes the prior assertion even when the value is unchanged;
    # that keeps source-file lineage without generating duplicates during replay.
    await connection.execute(
        text(
            """
            UPDATE retailer_identifier_assertions assertion
               SET superseded_at = :applied_at
              FROM makolet_exact_gtin_items item
             WHERE assertion.retailer_item_id = item.retailer_item_id
               AND assertion.kind = 'gtin'
               AND assertion.superseded_at IS NULL
               AND assertion.source_file_id <> :source_file_id
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            INSERT INTO retailer_identifier_assertions (
                id, retailer_item_id, kind, value, normalized_value,
                source_file_id, validation_method, asserted_at, superseded_at
            )
            SELECT uuidv7(), item.retailer_item_id, 'gtin', item.gtin,
                   item.gtin, :source_file_id, 'gtin_checksum', :applied_at, NULL
              FROM makolet_exact_gtin_items item
             WHERE item.gtin IS NOT NULL
            ON CONFLICT (retailer_item_id, kind, source_file_id) DO NOTHING
            """
        ),
        parameters,
    )

    # Automatic decisions are replaceable evidence projections. Operator decisions
    # are deliberately left intact and become review conflicts below.
    await connection.execute(
        text(
            """
            DELETE FROM confirmed_product_matches match
             USING makolet_exact_gtin_items item
             WHERE match.retailer_item_id = item.retailer_item_id
               AND match.method IN (
                   'exact_gtin',
                   'exact_provisional_gtin',
                   'exact_validated_gtin'
               )
               AND match.confirmed_by IN (
                   'system:exact-gtin',
                   'system:exact-gtin-evidence'
               )
            """
        )
    )

    # Match groups are internal concurrency arbiters, not public identifiers. The
    # first retailer therefore gets only an issuer-scoped provisional identifier.
    # A unique group row lets concurrent independent retailers converge without a
    # transaction-wide advisory lock or one candidate per store observation.
    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_exact_gtin_group_candidates ON COMMIT DROP AS
            SELECT DISTINCT ON (item.gtin)
                   uuidv7() AS group_id,
                   COALESCE(validated.product_id, uuidv7()) AS product_id,
                   validated.product_id IS NULL AS creates_product,
                   item.gtin, item.item_name, item.manufacturer_name,
                   item.quantity, item.unit_of_measure
              FROM makolet_exact_gtin_items item
              LEFT JOIN identifier_match_groups group_row
                ON group_row.kind = 'gtin'
               AND group_row.normalized_value = item.gtin
              LEFT JOIN product_identifiers validated
                ON validated.kind = 'gtin'
               AND validated.normalized_value = item.gtin
               AND validated.issuer_retailer_id IS NULL
               AND validated.is_validated
             WHERE item.gtin IS NOT NULL
               AND group_row.id IS NULL
             ORDER BY item.gtin, item.source_item_code
            """
        )
    )
    await connection.execute(
        text(
            """
            INSERT INTO canonical_products (
                id, name, brand, manufacturer, quantity,
                unit_of_measure, status, created_at, updated_at
            )
            SELECT product_id, item_name, manufacturer_name,
                   manufacturer_name, quantity, unit_of_measure,
                   'active', :applied_at, :applied_at
              FROM makolet_exact_gtin_group_candidates
             WHERE creates_product
             ORDER BY gtin
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            INSERT INTO identifier_match_groups (
                id, kind, normalized_value, canonical_product_id,
                created_at, updated_at
            )
            SELECT group_id, 'gtin', gtin, product_id, :applied_at, :applied_at
              FROM makolet_exact_gtin_group_candidates
             ORDER BY gtin
            ON CONFLICT (kind, normalized_value) DO NOTHING
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            DELETE FROM canonical_products product
             USING makolet_exact_gtin_group_candidates candidate
             WHERE product.id = candidate.product_id
               AND candidate.creates_product
               AND NOT EXISTS (
                   SELECT 1
                     FROM identifier_match_groups group_row
                    WHERE group_row.canonical_product_id = product.id
               )
            """
        )
    )

    await connection.execute(
        text(
            """
            INSERT INTO product_identifiers (
                id, product_id, kind, value, normalized_value,
                issuer_retailer_id, issuer_portal_id,
                is_validated, validation_method,
                validation_evidence, created_at
            )
            SELECT uuidv7(), group_row.canonical_product_id, 'gtin', item.gtin,
                   item.gtin, item.retailer_id, item.portal_id,
                   false, 'retailer_assertion',
                   jsonb_build_object(
                       'scope', 'retailer',
                       'retailer_id', item.retailer_id,
                       'source_file_id', CAST(:source_file_id AS uuid),
                       'validation', 'gtin_checksum'
                   ),
                   :applied_at
              FROM makolet_exact_gtin_items item
              JOIN identifier_match_groups group_row
                ON group_row.kind = 'gtin'
               AND group_row.normalized_value = item.gtin
             WHERE item.gtin IS NOT NULL
            GROUP BY group_row.canonical_product_id, item.gtin,
                     item.retailer_id, item.portal_id
            ON CONFLICT (
                kind, normalized_value, issuer_retailer_id, issuer_portal_id
            ) DO UPDATE
                SET value = excluded.value,
                    validation_evidence = excluded.validation_evidence
              WHERE product_identifiers.product_id = excluded.product_id
                AND product_identifiers.validation_method = 'retailer_assertion'
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_live_gtin_retailers ON COMMIT DROP AS
            SELECT assertion.normalized_value AS gtin,
                   evidence_item.retailer_id, evidence_item.portal_id
              FROM retailer_identifier_assertions assertion
              JOIN retailer_items evidence_item
                ON evidence_item.id = assertion.retailer_item_id
              JOIN makolet_affected_gtins affected
                ON affected.gtin = assertion.normalized_value
             WHERE assertion.kind = 'gtin'
               AND assertion.superseded_at IS NULL
             GROUP BY assertion.normalized_value, evidence_item.retailer_id,
                      evidence_item.portal_id
            """
        )
    )
    await connection.execute(
        text(
            """
            ALTER TABLE makolet_live_gtin_retailers
                ADD PRIMARY KEY (gtin, retailer_id, portal_id)
            """
        )
    )
    await connection.execute(text("ANALYZE makolet_live_gtin_retailers"))
    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_stale_scoped_identifier_ids
                ON COMMIT DROP AS
            SELECT identifier.id
              FROM product_identifiers identifier
              JOIN makolet_affected_gtins affected
                ON affected.gtin = identifier.normalized_value
              LEFT JOIN makolet_live_gtin_retailers live
               ON live.gtin = identifier.normalized_value
               AND live.retailer_id = identifier.issuer_retailer_id
               AND live.portal_id = identifier.issuer_portal_id
             WHERE identifier.kind = 'gtin'
               AND identifier.issuer_retailer_id IS NOT NULL
               AND identifier.validation_method = 'retailer_assertion'
               AND live.gtin IS NULL
            """
        )
    )
    await connection.execute(
        text(
            """
            ALTER TABLE makolet_stale_scoped_identifier_ids
                ADD PRIMARY KEY (id)
            """
        )
    )
    await connection.execute(text("ANALYZE makolet_stale_scoped_identifier_ids"))
    await connection.execute(
        text(
            """
            DELETE FROM product_identifiers identifier
             USING makolet_stale_scoped_identifier_ids stale
             WHERE identifier.id = stale.id
            """
        )
    )

    await connection.execute(
        text(
            """
            CREATE TEMP TABLE makolet_gtin_corroboration ON COMMIT DROP AS
            SELECT affected.gtin, group_row.canonical_product_id,
                   count(DISTINCT live.retailer_id) AS retailer_count
              FROM makolet_affected_gtins affected
              JOIN identifier_match_groups group_row
                ON group_row.kind = 'gtin'
               AND group_row.normalized_value = affected.gtin
              LEFT JOIN makolet_live_gtin_retailers live
                ON live.gtin = affected.gtin
             GROUP BY affected.gtin, group_row.canonical_product_id
            """
        )
    )
    await connection.execute(
        text(
            """
            CREATE UNIQUE INDEX makolet_gtin_corroboration_idx
                ON makolet_gtin_corroboration (gtin)
            """
        )
    )
    await connection.execute(text("ANALYZE makolet_gtin_corroboration"))
    await connection.execute(
        text(
            """
            DELETE FROM product_identifiers identifier
             USING makolet_gtin_corroboration corroboration
             WHERE identifier.kind = 'gtin'
               AND identifier.normalized_value = corroboration.gtin
               AND identifier.issuer_retailer_id IS NULL
               AND identifier.validation_method =
                   'independent_retailer_corroboration'
               AND corroboration.retailer_count < 2
            """
        )
    )
    await connection.execute(
        text(
            """
            INSERT INTO product_identifiers (
                id, product_id, kind, value, normalized_value,
                issuer_retailer_id, issuer_portal_id,
                is_validated, validation_method,
                validation_evidence, created_at
            )
            SELECT uuidv7(), corroboration.canonical_product_id, 'gtin',
                   corroboration.gtin, corroboration.gtin, NULL, NULL, true,
                   'independent_retailer_corroboration',
                   jsonb_build_object(
                       'retailer_count', corroboration.retailer_count,
                       'validated_at', CAST(:applied_at AS timestamptz),
                       'latest_source_file_id', CAST(:source_file_id AS uuid)
                   ),
                   CAST(:applied_at AS timestamptz)
              FROM makolet_gtin_corroboration corroboration
             WHERE corroboration.retailer_count >= 2
            ON CONFLICT (
                kind, normalized_value, issuer_retailer_id, issuer_portal_id
            ) DO UPDATE
                SET is_validated = true,
                    validation_method = excluded.validation_method,
                    validation_evidence = excluded.validation_evidence
              WHERE product_identifiers.product_id = excluded.product_id
                AND product_identifiers.validation_method =
                    'independent_retailer_corroboration'
            """
        ),
        parameters,
    )

    # A manual association or a disagreeing pre-existing identifier is never
    # overwritten. Surface the exact deterministic disagreement for review.
    await connection.execute(
        text(
            """
            WITH conflicts AS (
                SELECT item.retailer_item_id,
                       group_row.canonical_product_id AS candidate_product_id,
                       item.gtin, 'manual_match_disagrees' AS reason
                  FROM makolet_exact_gtin_items item
                  JOIN identifier_match_groups group_row
                    ON group_row.kind = 'gtin'
                   AND group_row.normalized_value = item.gtin
                  JOIN confirmed_product_matches existing
                    ON existing.retailer_item_id = item.retailer_item_id
                 WHERE existing.canonical_product_id <>
                       group_row.canonical_product_id
                UNION
                SELECT item.retailer_item_id,
                       group_row.canonical_product_id,
                       item.gtin, 'retailer_scope_disagrees'
                  FROM makolet_exact_gtin_items item
                  JOIN identifier_match_groups group_row
                    ON group_row.kind = 'gtin'
                   AND group_row.normalized_value = item.gtin
                  JOIN product_identifiers scoped
                    ON scoped.kind = 'gtin'
                   AND scoped.normalized_value = item.gtin
                   AND scoped.issuer_retailer_id = item.retailer_id
                   AND scoped.issuer_portal_id = item.portal_id
                 WHERE scoped.product_id <> group_row.canonical_product_id
                UNION
                SELECT item.retailer_item_id, validated.product_id,
                       item.gtin, 'validated_global_disagrees'
                  FROM makolet_exact_gtin_items item
                  JOIN identifier_match_groups group_row
                    ON group_row.kind = 'gtin'
                   AND group_row.normalized_value = item.gtin
                  JOIN product_identifiers validated
                    ON validated.kind = 'gtin'
                   AND validated.normalized_value = item.gtin
                   AND validated.issuer_retailer_id IS NULL
                   AND validated.is_validated
                 WHERE validated.product_id <> group_row.canonical_product_id
            )
            INSERT INTO product_match_candidates (
                id, retailer_item_id, canonical_product_id, method,
                score, status, evidence, created_at
            )
            SELECT uuidv7(), conflict.retailer_item_id,
                   conflict.candidate_product_id, 'exact_identifier_conflict',
                   1.0, 'pending',
                   jsonb_build_object(
                       'gtin', conflict.gtin,
                       'reason', conflict.reason,
                       'source_file_id', CAST(:source_file_id AS uuid)
                   ),
                   :applied_at
              FROM conflicts conflict
             WHERE conflict.candidate_product_id IS NOT NULL
            ON CONFLICT (retailer_item_id, canonical_product_id, method)
                DO NOTHING
            """
        ),
        parameters,
    )

    await connection.execute(
        text(
            """
            INSERT INTO confirmed_product_matches (
                id, retailer_item_id, canonical_product_id, method,
                evidence, confirmed_at, confirmed_by
            )
            SELECT uuidv7(), item.retailer_item_id,
                   group_row.canonical_product_id,
                   CASE WHEN validated.id IS NULL
                        THEN 'exact_provisional_gtin'
                        ELSE 'exact_validated_gtin'
                   END,
                   jsonb_build_object(
                       'assertion_id', assertion.id,
                       'evidence_scope', 'retailer_assertion',
                       'gtin', item.gtin,
                       'retailer_id', item.retailer_id,
                       'source_file_id', assertion.source_file_id,
                       'validation', CASE WHEN validated.id IS NULL
                            THEN 'gtin_checksum'
                            ELSE validated.validation_method
                       END
                   ),
                   :applied_at, 'system:exact-gtin-evidence'
              FROM makolet_exact_gtin_items item
              JOIN retailer_identifier_assertions assertion
                ON assertion.retailer_item_id = item.retailer_item_id
               AND assertion.kind = 'gtin'
               AND assertion.normalized_value = item.gtin
               AND assertion.superseded_at IS NULL
              JOIN identifier_match_groups group_row
                ON group_row.kind = 'gtin'
               AND group_row.normalized_value = item.gtin
              JOIN product_identifiers scoped
                ON scoped.kind = 'gtin'
               AND scoped.normalized_value = item.gtin
               AND scoped.issuer_retailer_id = item.retailer_id
               AND scoped.issuer_portal_id = item.portal_id
               AND scoped.product_id = group_row.canonical_product_id
              LEFT JOIN product_identifiers validated
                ON validated.kind = 'gtin'
               AND validated.normalized_value = item.gtin
               AND validated.issuer_retailer_id IS NULL
               AND validated.is_validated
               AND validated.product_id = group_row.canonical_product_id
              LEFT JOIN confirmed_product_matches existing
                ON existing.retailer_item_id = item.retailer_item_id
             WHERE item.gtin IS NOT NULL
               AND existing.id IS NULL
               AND NOT EXISTS (
                   SELECT 1
                     FROM product_identifiers conflicting_global
                    WHERE conflicting_global.kind = 'gtin'
                      AND conflicting_global.normalized_value = item.gtin
                      AND conflicting_global.issuer_retailer_id IS NULL
                      AND conflicting_global.is_validated
                      AND conflicting_global.product_id <>
                          group_row.canonical_product_id
               )
            ON CONFLICT (retailer_item_id) DO NOTHING
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            UPDATE confirmed_product_matches match
               SET method = CASE WHEN validated.id IS NULL
                                 THEN 'exact_provisional_gtin'
                                 ELSE 'exact_validated_gtin'
                            END,
                   evidence = jsonb_build_object(
                       'assertion_id', assertion.id,
                       'evidence_scope', 'retailer_assertion',
                       'gtin', assertion.normalized_value,
                       'retailer_id', item.retailer_id,
                       'source_file_id', assertion.source_file_id,
                       'validation', CASE WHEN validated.id IS NULL
                            THEN 'gtin_checksum'
                            ELSE validated.validation_method
                       END
                   ),
                   confirmed_at = :applied_at,
                   confirmed_by = 'system:exact-gtin-evidence'
              FROM retailer_identifier_assertions assertion
              JOIN retailer_items item
                ON item.id = assertion.retailer_item_id
              JOIN makolet_affected_gtins affected
                ON affected.gtin = assertion.normalized_value
              JOIN identifier_match_groups group_row
                ON group_row.kind = assertion.kind
               AND group_row.normalized_value = assertion.normalized_value
              LEFT JOIN product_identifiers validated
                ON validated.kind = assertion.kind
               AND validated.normalized_value = assertion.normalized_value
               AND validated.issuer_retailer_id IS NULL
               AND validated.is_validated
               AND validated.product_id = group_row.canonical_product_id
             WHERE assertion.kind = 'gtin'
               AND assertion.superseded_at IS NULL
               AND match.retailer_item_id = assertion.retailer_item_id
               AND match.canonical_product_id = group_row.canonical_product_id
               AND match.method IN (
                   'exact_gtin',
                   'exact_provisional_gtin',
                   'exact_validated_gtin'
               )
               AND match.confirmed_by IN (
                   'system:exact-gtin',
                   'system:exact-gtin-evidence'
               )
            """
        ),
        parameters,
    )


async def _apply_incoming_availability(
    connection: AsyncConnection,
    parameters: dict[str, object],
) -> tuple[int, int]:
    counts = (
        await connection.execute(
            text(
                """
                SELECT count(*) FILTER (
                           WHERE current.id IS NULL
                              OR current.is_available IS DISTINCT FROM incoming.is_available
                              OR current.item_status IS DISTINCT FROM incoming.item_status
                       ) AS history_events,
                       count(*) FILTER (
                           WHERE NOT incoming.is_available
                             AND (current.id IS NULL OR current.is_available)
                       ) AS unavailable
                  FROM pg_temp.makolet_mapped_price_incoming incoming
                  LEFT JOIN current_availability current
                    ON current.retailer_item_id = incoming.retailer_item_id
                   AND current.store_id = incoming.store_id
                """
            ),
            parameters,
        )
    ).one()
    await connection.execute(
        text(
            """
            WITH changed AS MATERIALIZED (
                SELECT incoming.*
                  FROM pg_temp.makolet_mapped_price_incoming incoming
                  JOIN current_availability current
                    ON current.retailer_item_id = incoming.retailer_item_id
                   AND current.store_id = incoming.store_id
                 WHERE current.is_available IS DISTINCT FROM incoming.is_available
                    OR current.item_status IS DISTINCT FROM incoming.item_status
            )
            UPDATE availability_history history
               SET valid_to = :applied_at
              FROM changed
             WHERE history.retailer_item_id = changed.retailer_item_id
               AND history.store_id = changed.store_id
               AND history.valid_to IS NULL
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            INSERT INTO availability_history (
                id, retailer_item_id, store_id, is_available, item_status,
                source_file_id, valid_from, valid_to
            )
            SELECT uuidv7(), incoming.retailer_item_id, incoming.store_id,
                   incoming.is_available, incoming.item_status,
                   :source_file_id, :applied_at, NULL
              FROM pg_temp.makolet_mapped_price_incoming incoming
              LEFT JOIN current_availability current
                ON current.retailer_item_id = incoming.retailer_item_id
               AND current.store_id = incoming.store_id
             WHERE current.id IS NULL
                OR current.is_available IS DISTINCT FROM incoming.is_available
                OR current.item_status IS DISTINCT FROM incoming.item_status
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            INSERT INTO current_availability (
                id, retailer_item_id, store_id, is_available, item_status,
                source_file_id, first_observed_at, last_observed_at
            )
            SELECT uuidv7(), retailer_item_id, store_id, is_available,
                   item_status, :source_file_id, :applied_at, :applied_at
              FROM pg_temp.makolet_mapped_price_incoming incoming
            ON CONFLICT (retailer_item_id, store_id) DO UPDATE
                SET is_available = excluded.is_available,
                    item_status = excluded.item_status,
                    source_file_id = excluded.source_file_id,
                    last_observed_at = excluded.last_observed_at
            """
        ),
        parameters,
    )
    return int(counts.history_events), int(counts.unavailable)


async def _reconcile_absent_prices(
    connection: AsyncConnection,
    parameters: dict[str, object],
) -> int:
    # Materialize and index the composite missing key once. EXCEPT compares both key
    # columns as a set operation; this avoids a partial store_id merge followed by a
    # quadratic retailer_item_id join filter within large stores.
    await connection.execute(
        text(
            f"""
            CREATE TEMP TABLE makolet_missing_price_keys ON COMMIT DROP AS
            {_MISSING_PRICE_KEYS_SELECT}
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            ALTER TABLE pg_temp.makolet_missing_price_keys
                ADD PRIMARY KEY (retailer_item_id, store_id)
            """
        )
    )
    await connection.execute(text("ANALYZE pg_temp.makolet_missing_price_keys"))
    # PostgreSQL data-modifying CTEs share a snapshot and do not promise execution
    # order. Keep these as separate statements inside the surrounding transaction so
    # the partial unique index sees the old open history row closed before insertion.
    await connection.execute(
        text(
            """
            UPDATE availability_history history
               SET valid_to = :applied_at
              FROM pg_temp.makolet_missing_price_keys missing
             WHERE history.retailer_item_id = missing.retailer_item_id
               AND history.store_id = missing.store_id
               AND history.valid_to IS NULL
            """
        ),
        parameters,
    )
    inserted = await connection.execute(
        text(
            """
            INSERT INTO availability_history (
                id, retailer_item_id, store_id, is_available, item_status,
                source_file_id, valid_from, valid_to
            )
            SELECT uuidv7(), retailer_item_id, store_id, false, NULL,
                   :source_file_id, :applied_at, NULL
              FROM pg_temp.makolet_missing_price_keys
            RETURNING id
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            UPDATE current_availability current
               SET is_available = false,
                   item_status = NULL,
                   source_file_id = :source_file_id,
                   last_observed_at = :applied_at
              FROM pg_temp.makolet_missing_price_keys missing
             WHERE current.retailer_item_id = missing.retailer_item_id
               AND current.store_id = missing.store_id
            """
        ),
        parameters,
    )
    return int(inserted.rowcount)


_PROMOTION_INCOMING = """
SELECT DISTINCT ON (subchain_id, source_promotion_id, source_scope_store_code)
       *
  FROM staged_promotions
 WHERE source_file_id = :source_file_id
 ORDER BY subchain_id, source_promotion_id, source_scope_store_code,
          record_index DESC
"""


async def _apply_promotions(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    retailer_id: UUID,
    portal_id: UUID,
    applied_at: datetime,
    is_full: bool,
    minimum_full_records: int,
    maximum_drop_fraction: float,
) -> ApplySummary:
    if is_full:
        await _validate_full_promotion_snapshot(
            connection,
            source_file_id=source_file_id,
            retailer_id=retailer_id,
            portal_id=portal_id,
            minimum_full_records=minimum_full_records,
            maximum_drop_fraction=maximum_drop_fraction,
        )
    parameters = {
        "source_file_id": source_file_id,
        "retailer_id": retailer_id,
        "portal_id": portal_id,
        "applied_at": applied_at,
    }
    await _ensure_promotion_items_and_stores(connection, parameters)
    counts = (
        await connection.execute(
            text(
                f"""
                WITH incoming AS ({_PROMOTION_INCOMING})
                SELECT count(*) FILTER (WHERE current.id IS NULL) AS inserted,
                       count(*) FILTER (
                           WHERE current.id IS NOT NULL
                             AND current.fingerprint_sha256 <> incoming.fingerprint_sha256
                       ) AS updated,
                       count(*) FILTER (
                           WHERE current.id IS NOT NULL
                             AND current.fingerprint_sha256 = incoming.fingerprint_sha256
                       ) AS unchanged
                  FROM incoming
                  LEFT JOIN promotions current
                    ON current.retailer_id = :retailer_id
                   AND current.portal_id = :portal_id
                   AND current.subchain_code = incoming.subchain_id
                   AND current.source_promotion_id = incoming.source_promotion_id
                   AND current.source_scope_store_code = incoming.source_scope_store_code
                   AND current.valid_to IS NULL
                """
            ),
            parameters,
        )
    ).one()
    await connection.execute(
        text(
            f"""
            WITH incoming AS ({_PROMOTION_INCOMING})
            UPDATE promotions current
               SET valid_to = :applied_at
              FROM incoming
             WHERE current.retailer_id = :retailer_id
               AND current.portal_id = :portal_id
               AND current.subchain_code = incoming.subchain_id
               AND current.source_promotion_id = incoming.source_promotion_id
               AND current.source_scope_store_code = incoming.source_scope_store_code
               AND current.valid_to IS NULL
               AND current.fingerprint_sha256 <> incoming.fingerprint_sha256
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            f"""
            WITH incoming AS ({_PROMOTION_INCOMING})
            UPDATE promotions current
               SET last_observed_at = :applied_at,
                   source_file_id = :source_file_id
              FROM incoming
             WHERE current.retailer_id = :retailer_id
               AND current.portal_id = :portal_id
               AND current.subchain_code = incoming.subchain_id
               AND current.source_promotion_id = incoming.source_promotion_id
               AND current.source_scope_store_code = incoming.source_scope_store_code
               AND current.valid_to IS NULL
               AND current.fingerprint_sha256 = incoming.fingerprint_sha256
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            f"""
            WITH incoming AS ({_PROMOTION_INCOMING})
            INSERT INTO promotions (
                id, retailer_id, portal_id, subchain_code,
                source_promotion_id, source_scope_store_code,
                description, discount_kind, starts_at, ends_at, reward_type,
                allows_multiple_discounts, minimum_quantity, maximum_quantity,
                discount_rate, minimum_purchase, discounted_price,
                discounted_unit_price, minimum_items_offered,
                additional_restrictions, remarks, is_active,
                fingerprint_sha256, source_file_id, valid_from, valid_to,
                last_observed_at
            )
            SELECT uuidv7(), :retailer_id, :portal_id, incoming.subchain_id,
                   incoming.source_promotion_id,
                   incoming.source_scope_store_code, incoming.description,
                   incoming.discount_kind, incoming.starts_at, incoming.ends_at,
                   incoming.reward_type, incoming.allows_multiple_discounts,
                   incoming.minimum_quantity, incoming.maximum_quantity,
                   incoming.discount_rate, incoming.minimum_purchase,
                   incoming.discounted_price, incoming.discounted_unit_price,
                   incoming.minimum_items_offered,
                   incoming.additional_restrictions, incoming.remarks,
                   incoming.is_active, incoming.fingerprint_sha256,
                   :source_file_id, :applied_at, NULL, :applied_at
              FROM incoming
              LEFT JOIN promotions current
                ON current.retailer_id = :retailer_id
               AND current.portal_id = :portal_id
               AND current.subchain_code = incoming.subchain_id
               AND current.source_promotion_id = incoming.source_promotion_id
               AND current.source_scope_store_code = incoming.source_scope_store_code
               AND current.valid_to IS NULL
             WHERE current.id IS NULL
            """
        ),
        parameters,
    )
    await _attach_promotion_relations(connection, parameters)
    absent = 0
    if is_full:
        absent = int(
            (
                await connection.execute(
                    text(
                        f"""
                        WITH incoming AS ({_PROMOTION_INCOMING}),
                        scopes AS (
                            SELECT DISTINCT subchain_id,
                                            source_scope_store_code
                              FROM incoming
                            UNION
                            SELECT DISTINCT COALESCE(subchain_id, ''),
                                            COALESCE(store_id, '')
                              FROM staged_documents
                             WHERE source_file_id = :source_file_id
                        )
                        UPDATE promotions current
                           SET valid_to = :applied_at,
                               last_observed_at = :applied_at,
                               source_file_id = :source_file_id
                         WHERE current.retailer_id = :retailer_id
                           AND current.portal_id = :portal_id
                           AND current.valid_to IS NULL
                           AND (current.subchain_code,
                                current.source_scope_store_code) IN (
                               SELECT subchain_id, source_scope_store_code
                                 FROM scopes
                           )
                           AND NOT EXISTS (
                               SELECT 1 FROM incoming
                                WHERE incoming.subchain_id = current.subchain_code
                                  AND incoming.source_promotion_id =
                                      current.source_promotion_id
                                  AND incoming.source_scope_store_code =
                                      current.source_scope_store_code
                           )
                        RETURNING current.id
                        """
                    ),
                    parameters,
                )
            ).rowcount
        )
    return ApplySummary(
        inserted=int(counts.inserted),
        updated=int(counts.updated),
        unchanged=int(counts.unchanged),
        unavailable=absent,
        history_events=int(counts.inserted) + int(counts.updated) + absent,
    )


async def _validate_full_promotion_snapshot(
    connection: AsyncConnection,
    *,
    source_file_id: UUID,
    retailer_id: UUID,
    portal_id: UUID,
    minimum_full_records: int,
    maximum_drop_fraction: float,
) -> None:
    rows = (
        await connection.execute(
            text(
                f"""
                WITH incoming AS ({_PROMOTION_INCOMING}),
                incoming_counts AS (
                    SELECT subchain_id, source_scope_store_code,
                           count(*) AS incoming_count
                      FROM incoming
                     GROUP BY subchain_id, source_scope_store_code
                ),
                scopes AS (
                    SELECT subchain_id, source_scope_store_code,
                           incoming_count
                      FROM incoming_counts
                    UNION
                    SELECT COALESCE(document.subchain_id, ''),
                           COALESCE(document.store_id, ''), 0
                      FROM staged_documents document
                     WHERE document.source_file_id = :source_file_id
                       AND NOT EXISTS (
                           SELECT 1 FROM incoming_counts count_row
                            WHERE count_row.subchain_id =
                                  COALESCE(document.subchain_id, '')
                              AND count_row.source_scope_store_code =
                                  COALESCE(document.store_id, '')
                       )
                )
                SELECT scope.subchain_id, scope.source_scope_store_code,
                       scope.incoming_count,
                       count(current.id) AS prior_count
                  FROM scopes scope
                  LEFT JOIN promotions current
                    ON current.retailer_id = :retailer_id
                   AND current.portal_id = :portal_id
                   AND current.subchain_code = scope.subchain_id
                   AND current.source_scope_store_code =
                       scope.source_scope_store_code
                   AND current.valid_to IS NULL
                 GROUP BY scope.subchain_id,
                          scope.source_scope_store_code, scope.incoming_count
                """
            ),
            {
                "source_file_id": source_file_id,
                "retailer_id": retailer_id,
                "portal_id": portal_id,
            },
        )
    ).all()
    total = sum(int(row.incoming_count) for row in rows)
    if total < minimum_full_records:
        raise SnapshotValidationError(
            f"Full promotion snapshot has {total} records; minimum is {minimum_full_records}"
        )
    for row in rows:
        _validate_drop(
            incoming_count=int(row.incoming_count),
            prior_count=int(row.prior_count),
            maximum_drop_fraction=maximum_drop_fraction,
            scope=(
                f"promotion scope {row.subchain_id or '<default>'}/"
                f"{row.source_scope_store_code or '<all stores>'}"
            ),
        )


async def _ensure_promotion_items_and_stores(
    connection: AsyncConnection,
    parameters: dict[str, object],
) -> None:
    await connection.execute(
        text(
            """
            INSERT INTO retailer_items (
                id, retailer_id, portal_id, source_item_code,
                name, first_seen_at,
                last_seen_at, last_source_file_id
            )
            SELECT uuidv7(), :retailer_id, :portal_id, child.source_item_code,
                   child.source_item_code, :applied_at, :applied_at,
                   :source_file_id
              FROM staged_promotion_items child
             WHERE child.source_file_id = :source_file_id
             GROUP BY child.source_item_code
            ON CONFLICT (retailer_id, portal_id, source_item_code) DO UPDATE
                SET last_seen_at = excluded.last_seen_at,
                    last_source_file_id = excluded.last_source_file_id
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            """
            INSERT INTO stores (
                id, retailer_id, portal_id, chain_code,
                subchain_code, source_store_code,
                name, is_active, first_seen_at, last_seen_at,
                last_source_file_id
            )
            SELECT uuidv7(), :retailer_id, :portal_id, parent.chain_id,
                   parent.subchain_id, child.source_store_code,
                   'Store ' || child.source_store_code, true,
                   :applied_at, :applied_at, :source_file_id
              FROM staged_promotion_stores child
              JOIN staged_promotions parent
                ON parent.source_file_id = child.source_file_id
               AND parent.record_index = child.record_index
             WHERE child.source_file_id = :source_file_id
             GROUP BY parent.chain_id, parent.subchain_id,
                      child.source_store_code
            ON CONFLICT (
                retailer_id, portal_id, subchain_code, source_store_code
            ) DO UPDATE
                SET is_active = true,
                    last_seen_at = excluded.last_seen_at,
                    last_source_file_id = excluded.last_source_file_id
            """
        ),
        parameters,
    )


async def _attach_promotion_relations(
    connection: AsyncConnection,
    parameters: dict[str, object],
) -> None:
    await connection.execute(
        text(
            f"""
            WITH parent AS ({_PROMOTION_INCOMING})
            INSERT INTO promotion_items (
                promotion_id, retailer_item_id, item_type, is_gift
            )
            SELECT promotion.id, item.id, child.item_type, child.is_gift
              FROM staged_promotion_items child
              JOIN parent
                ON parent.source_file_id = child.source_file_id
               AND parent.record_index = child.record_index
              JOIN promotions promotion
                ON promotion.retailer_id = :retailer_id
               AND promotion.portal_id = :portal_id
               AND promotion.subchain_code = parent.subchain_id
               AND promotion.source_promotion_id = parent.source_promotion_id
               AND promotion.source_scope_store_code =
                   parent.source_scope_store_code
               AND promotion.valid_to IS NULL
              JOIN retailer_items item
                ON item.retailer_id = :retailer_id
               AND item.portal_id = :portal_id
               AND item.source_item_code = child.source_item_code
             WHERE child.source_file_id = :source_file_id
            ON CONFLICT (promotion_id, retailer_item_id) DO NOTHING
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            f"""
            WITH parent AS ({_PROMOTION_INCOMING})
            INSERT INTO promotion_stores (promotion_id, store_id)
            SELECT promotion.id, store.id
              FROM staged_promotion_stores child
              JOIN parent
                ON parent.source_file_id = child.source_file_id
               AND parent.record_index = child.record_index
              JOIN promotions promotion
                ON promotion.retailer_id = :retailer_id
               AND promotion.portal_id = :portal_id
               AND promotion.subchain_code = parent.subchain_id
               AND promotion.source_promotion_id = parent.source_promotion_id
               AND promotion.source_scope_store_code =
                   parent.source_scope_store_code
               AND promotion.valid_to IS NULL
              JOIN stores store
                ON store.retailer_id = :retailer_id
               AND store.portal_id = :portal_id
               AND store.subchain_code = parent.subchain_id
               AND store.source_store_code = child.source_store_code
             WHERE child.source_file_id = :source_file_id
            ON CONFLICT (promotion_id, store_id) DO NOTHING
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            f"""
            WITH parent AS ({_PROMOTION_INCOMING})
            INSERT INTO promotion_clubs (promotion_id, club_id)
            SELECT promotion.id, child.club_id
              FROM staged_promotion_clubs child
              JOIN parent
                ON parent.source_file_id = child.source_file_id
               AND parent.record_index = child.record_index
              JOIN promotions promotion
                ON promotion.retailer_id = :retailer_id
               AND promotion.portal_id = :portal_id
               AND promotion.subchain_code = parent.subchain_id
               AND promotion.source_promotion_id = parent.source_promotion_id
               AND promotion.source_scope_store_code =
                   parent.source_scope_store_code
               AND promotion.valid_to IS NULL
             WHERE child.source_file_id = :source_file_id
            ON CONFLICT (promotion_id, club_id) DO NOTHING
            """
        ),
        parameters,
    )


def _validate_full_snapshot(
    *,
    incoming_count: int,
    prior_count: int,
    minimum_full_records: int,
    maximum_drop_fraction: float,
    scope: str,
) -> None:
    if incoming_count < minimum_full_records:
        raise SnapshotValidationError(
            f"Full snapshot for {scope} has {incoming_count} records; "
            f"minimum is {minimum_full_records}"
        )
    _validate_drop(
        incoming_count=incoming_count,
        prior_count=prior_count,
        maximum_drop_fraction=maximum_drop_fraction,
        scope=scope,
    )


def _validate_drop(
    *,
    incoming_count: int,
    prior_count: int,
    maximum_drop_fraction: float,
    scope: str,
) -> None:
    if prior_count <= 0 or incoming_count >= prior_count:
        return
    drop_fraction = (prior_count - incoming_count) / prior_count
    if drop_fraction > maximum_drop_fraction:
        raise SnapshotValidationError(
            f"Full snapshot for {scope} drops {drop_fraction:.1%}; "
            f"maximum is {maximum_drop_fraction:.1%}"
        )
