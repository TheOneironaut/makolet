"""Reproducible PostgreSQL-to-partitioned-Parquet export operations."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from threading import Lock
from typing import Any, BinaryIO, cast
from uuid import UUID, uuid4

import anyio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from makolet.adapters.archive._process import (
    ProcessDeadlineError,
    ProcessWorkerError,
    run_in_spawn_process,
    run_in_spawn_process_with_input,
)
from makolet.adapters.export.dataset import (
    PartitionedParquetExporter,
    recover_export_publication,
)
from makolet.adapters.export.models import (
    ExportConflictError,
    ExportField,
    ExportLimits,
    ExportOperationBudget,
    ExportPartialPublicationError,
    ExportPartition,
    ExportResult,
    ExportSchema,
    ExportType,
    ExportValidationError,
)

_MAXIMUM_PARTITIONS = 1_000
_SPOOL_INPUT_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class _EntityExport:
    name: str
    timestamp_column: str
    partition_sql: str
    count_sql: str
    rows_sql: str
    schema: ExportSchema


@dataclass(frozen=True, slots=True)
class _PlannedPartition:
    retailer_id: str
    partition_date: date
    row_count: int


@dataclass(slots=True)
class _PublicationLedger:
    _items: list[dict[str, object]] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock)

    def record(self, result: ExportResult) -> None:
        item = _result_dict(result)
        with self._lock:
            self._items.append(item)

    def snapshot(self) -> tuple[dict[str, object], ...]:
        with self._lock:
            return tuple(self._items)


@dataclass(frozen=True, slots=True)
class _ExportProcessResponse:
    result: ExportResult | None
    publication: ExportResult | None
    error_type: str | None
    error_message: str | None
    consumed_files: int
    consumed_output_bytes: int


@dataclass(frozen=True, slots=True)
class _ExportRecoveryResponse:
    publication: ExportResult | None
    error_type: str | None
    error_message: str | None


@dataclass(frozen=True, slots=True)
class _ExportLocation:
    destination: Path
    spool_root: Path


@dataclass(frozen=True, slots=True)
class _ExportLocationResponse:
    location: _ExportLocation | None
    error_type: str | None
    error_message: str | None


@dataclass(frozen=True, slots=True)
class _SpoolProcessResponse:
    byte_length: int
    error_type: str | None
    error_message: str | None


_CURRENT_PRICES = _EntityExport(
    name="current_prices",
    timestamp_column="price.last_observed_at",
    partition_sql="""
        SELECT retailer.source_key AS retailer_id,
               (price.last_observed_at AT TIME ZONE 'UTC')::date AS partition_date,
               count(*) AS row_count
          FROM current_prices price
          JOIN retailer_items item ON item.id = price.retailer_item_id
          JOIN retailers retailer ON retailer.id = item.retailer_id
         WHERE (
             CAST(:since AS timestamptz) IS NULL
             OR price.last_observed_at >= CAST(:since AS timestamptz)
         )
           AND (
             CAST(:until AS timestamptz) IS NULL
             OR price.last_observed_at <= CAST(:until AS timestamptz)
           )
         GROUP BY retailer.source_key,
                  (price.last_observed_at AT TIME ZONE 'UTC')::date
         ORDER BY retailer.source_key, partition_date
         LIMIT :partition_limit
    """,
    count_sql="""
        SELECT count(*) AS row_count
          FROM current_prices price
         WHERE (
             CAST(:since AS timestamptz) IS NULL
             OR price.last_observed_at >= CAST(:since AS timestamptz)
         )
           AND (
             CAST(:until AS timestamptz) IS NULL
             OR price.last_observed_at <= CAST(:until AS timestamptz)
           )
    """,
    rows_sql="""
        SELECT retailer.source_key AS retailer_id,
               price.retailer_item_id, item.source_item_code, item.gtin,
               item.name AS item_name, price.store_id,
               store.source_store_code, price.item_price,
               price.unit_of_measure_price, price.allow_discount,
               price.source_updated_at, price.first_observed_at,
               price.last_observed_at, price.source_file_id,
               source.portal_id AS source_portal_id,
               portal.source_key AS source_portal_key,
               source.document_type AS source_document_type,
               source.source_timestamp,
               source.download_finished_at AS source_download_finished_at,
               archive.content_sha256 AS archive_content_sha256
          FROM current_prices price
          JOIN retailer_items item ON item.id = price.retailer_item_id
          JOIN retailers retailer ON retailer.id = item.retailer_id
          JOIN stores store ON store.id = price.store_id
          JOIN source_files source ON source.id = price.source_file_id
          JOIN portals portal ON portal.id = source.portal_id
          JOIN raw_archive_objects archive ON archive.id = source.raw_archive_object_id
         WHERE retailer.source_key = :retailer_id
           AND (price.last_observed_at AT TIME ZONE 'UTC')::date = :partition_date
           AND (
               CAST(:since AS timestamptz) IS NULL
               OR price.last_observed_at >= CAST(:since AS timestamptz)
           )
           AND (
               CAST(:until AS timestamptz) IS NULL
               OR price.last_observed_at <= CAST(:until AS timestamptz)
           )
         ORDER BY price.id
    """,
    schema=ExportSchema(
        entity="current_prices",
        version="2",
        fields=(
            ExportField("retailer_id", ExportType.STRING, False),
            ExportField("retailer_item_id", ExportType.STRING, False),
            ExportField("source_item_code", ExportType.STRING, False),
            ExportField("gtin", ExportType.STRING),
            ExportField("item_name", ExportType.STRING, False),
            ExportField("store_id", ExportType.STRING, False),
            ExportField("source_store_code", ExportType.STRING, False),
            ExportField("item_price", ExportType.DECIMAL_STRING, False),
            ExportField("unit_of_measure_price", ExportType.DECIMAL_STRING),
            ExportField("allow_discount", ExportType.BOOLEAN),
            ExportField("source_updated_at", ExportType.TIMESTAMP_ISO8601),
            ExportField("first_observed_at", ExportType.TIMESTAMP_ISO8601, False),
            ExportField("last_observed_at", ExportType.TIMESTAMP_ISO8601, False),
            ExportField("source_file_id", ExportType.STRING, False),
            ExportField("source_portal_id", ExportType.STRING, False),
            ExportField("source_portal_key", ExportType.STRING, False),
            ExportField("source_document_type", ExportType.STRING, False),
            ExportField("source_timestamp", ExportType.TIMESTAMP_ISO8601),
            ExportField("source_download_finished_at", ExportType.TIMESTAMP_ISO8601, False),
            ExportField("archive_content_sha256", ExportType.STRING, False),
        ),
    ),
)

_PRICE_HISTORY = _EntityExport(
    name="price_history",
    timestamp_column="history.valid_from",
    partition_sql="""
        SELECT retailer.source_key AS retailer_id,
               (history.valid_from AT TIME ZONE 'UTC')::date AS partition_date,
               count(*) AS row_count
          FROM price_history history
          JOIN retailer_items item ON item.id = history.retailer_item_id
          JOIN retailers retailer ON retailer.id = item.retailer_id
         WHERE (
             CAST(:since AS timestamptz) IS NULL
             OR COALESCE(history.valid_to, 'infinity') > CAST(:since AS timestamptz)
         )
           AND (
             CAST(:until AS timestamptz) IS NULL
             OR history.valid_from <= CAST(:until AS timestamptz)
           )
         GROUP BY retailer.source_key,
                  (history.valid_from AT TIME ZONE 'UTC')::date
         ORDER BY retailer.source_key, partition_date
         LIMIT :partition_limit
    """,
    count_sql="""
        SELECT count(*) AS row_count
          FROM price_history history
         WHERE (
             CAST(:since AS timestamptz) IS NULL
             OR COALESCE(history.valid_to, 'infinity') > CAST(:since AS timestamptz)
         )
           AND (
             CAST(:until AS timestamptz) IS NULL
             OR history.valid_from <= CAST(:until AS timestamptz)
           )
    """,
    rows_sql="""
        SELECT retailer.source_key AS retailer_id,
               history.retailer_item_id, item.source_item_code, item.gtin,
               item.name AS item_name, history.store_id,
               store.source_store_code, history.item_price,
               history.unit_of_measure_price, history.allow_discount,
               history.source_updated_at, history.valid_from,
               history.valid_to, history.source_file_id,
               source.portal_id AS source_portal_id,
               portal.source_key AS source_portal_key,
               source.document_type AS source_document_type,
               source.source_timestamp,
               source.download_finished_at AS source_download_finished_at,
               archive.content_sha256 AS archive_content_sha256
          FROM price_history history
          JOIN retailer_items item ON item.id = history.retailer_item_id
          JOIN retailers retailer ON retailer.id = item.retailer_id
          JOIN stores store ON store.id = history.store_id
          JOIN source_files source ON source.id = history.source_file_id
          JOIN portals portal ON portal.id = source.portal_id
          JOIN raw_archive_objects archive ON archive.id = source.raw_archive_object_id
         WHERE retailer.source_key = :retailer_id
           AND (history.valid_from AT TIME ZONE 'UTC')::date = :partition_date
           AND (
               CAST(:since AS timestamptz) IS NULL
               OR COALESCE(history.valid_to, 'infinity') > CAST(:since AS timestamptz)
           )
           AND (
               CAST(:until AS timestamptz) IS NULL
               OR history.valid_from <= CAST(:until AS timestamptz)
           )
         ORDER BY history.id
    """,
    schema=ExportSchema(
        entity="price_history",
        version="2",
        fields=(
            ExportField("retailer_id", ExportType.STRING, False),
            ExportField("retailer_item_id", ExportType.STRING, False),
            ExportField("source_item_code", ExportType.STRING, False),
            ExportField("gtin", ExportType.STRING),
            ExportField("item_name", ExportType.STRING, False),
            ExportField("store_id", ExportType.STRING, False),
            ExportField("source_store_code", ExportType.STRING, False),
            ExportField("item_price", ExportType.DECIMAL_STRING, False),
            ExportField("unit_of_measure_price", ExportType.DECIMAL_STRING),
            ExportField("allow_discount", ExportType.BOOLEAN),
            ExportField("source_updated_at", ExportType.TIMESTAMP_ISO8601),
            ExportField("valid_from", ExportType.TIMESTAMP_ISO8601, False),
            ExportField("valid_to", ExportType.TIMESTAMP_ISO8601),
            ExportField("source_file_id", ExportType.STRING, False),
            ExportField("source_portal_id", ExportType.STRING, False),
            ExportField("source_portal_key", ExportType.STRING, False),
            ExportField("source_document_type", ExportType.STRING, False),
            ExportField("source_timestamp", ExportType.TIMESTAMP_ISO8601),
            ExportField("source_download_finished_at", ExportType.TIMESTAMP_ISO8601, False),
            ExportField("archive_content_sha256", ExportType.STRING, False),
        ),
    ),
)

_ENTITIES = (_CURRENT_PRICES, _PRICE_HISTORY)


def _resolve_export_location_in_process(
    output: Path,
    configured_spool_directory: Path | None,
) -> _ExportLocationResponse:
    try:
        destination = output.resolve(strict=False)
        spool_root = (
            configured_spool_directory.resolve(strict=False)
            if configured_spool_directory is not None
            else destination / ".makolet-spool"
        )
    except BaseException as error:
        return _ExportLocationResponse(
            location=None,
            error_type=type(error).__name__,
            error_message=str(error)[:512],
        )
    return _ExportLocationResponse(
        location=_ExportLocation(destination=destination, spool_root=spool_root),
        error_type=None,
        error_message=None,
    )


def _write_spool_in_process(
    chunks: Iterator[bytes],
    spool: Path,
    maximum_bytes: int,
) -> _SpoolProcessResponse:
    byte_length = 0
    error_type: str | None = None
    error_message: str | None = None
    handle: BinaryIO | None = None
    try:
        descriptor = _open_spool_file(spool)
        try:
            handle = os.fdopen(descriptor, "wb")
        except BaseException:
            os.close(descriptor)
            raise
        for chunk in chunks:
            byte_length = _charge_spool_chunk(chunk, byte_length, maximum_bytes)
            _write_spool_chunk(handle, chunk)
        _flush_spool_file(handle)
    except BaseException as error:
        error_type = type(error).__name__
        error_message = str(error)[:512]
    finally:
        if handle is not None:
            try:
                _close_spool_file(handle)
            except BaseException as error:
                error_type = type(error).__name__
                error_message = str(error)[:512]
    return _SpoolProcessResponse(
        byte_length=byte_length,
        error_type=error_type,
        error_message=error_message,
    )


def _open_spool_file(spool: Path) -> int:
    spool.parent.mkdir(parents=True, exist_ok=True)
    parent_status = spool.parent.lstat()
    if not stat.S_ISDIR(parent_status.st_mode) or stat.S_ISLNK(parent_status.st_mode):
        raise ExportValidationError("export spool parent is not a real directory")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    return os.open(spool, flags, 0o600)


def _charge_spool_chunk(chunk: object, byte_length: int, maximum_bytes: int) -> int:
    if not isinstance(chunk, bytes):
        raise TypeError("export spool child received a non-byte chunk")
    prospective = byte_length + len(chunk)
    if prospective > maximum_bytes:
        raise ExportValidationError("database export exceeds max_spool_bytes")
    return prospective


def _write_spool_chunk(handle: BinaryIO, chunk: bytes) -> None:
    written = handle.write(chunk)
    if written != len(chunk):
        raise OSError("export spool write was incomplete")


def _flush_spool_file(handle: BinaryIO) -> None:
    handle.flush()
    os.fsync(handle.fileno())


def _close_spool_file(handle: BinaryIO) -> None:
    handle.close()


def _export_partition_in_process(
    output: Path,
    limits: ExportLimits,
    partition: ExportPartition,
    schema: ExportSchema,
    spool: Path,
    operation_budget: ExportOperationBudget,
    operation_identity: str,
    publication_journal: Path,
) -> _ExportProcessResponse:
    publications: list[ExportResult] = []
    result: ExportResult | None = None
    error_type: str | None = None
    error_message: str | None = None
    try:
        result = PartitionedParquetExporter(output, limits=limits).export(
            partition=partition,
            schema=schema,
            rows=_spooled_rows(spool, schema),
            operation_budget=operation_budget,
            publication_recorder=publications.append,
            operation_identity=operation_identity,
            publication_journal=publication_journal,
        )
    except BaseException as error:
        error_type = type(error).__name__
        error_message = str(error)[:512]
    return _ExportProcessResponse(
        result=result,
        publication=publications[-1] if publications else None,
        error_type=error_type,
        error_message=error_message,
        consumed_files=operation_budget.consumed_files,
        consumed_output_bytes=operation_budget.consumed_output_bytes,
    )


def _recover_partition_in_process(
    output: Path,
    limits: ExportLimits,
    partition: ExportPartition,
    schema: ExportSchema,
    spool: Path,
    operation_identity: str,
    publication_journal: Path,
) -> _ExportRecoveryResponse:
    try:
        publication = recover_export_publication(
            output,
            partition=partition,
            schema=schema,
            limits=limits,
            operation_identity=operation_identity,
            publication_journal=publication_journal,
        )
        _remove_spool_file(spool)
    except BaseException as error:
        return _ExportRecoveryResponse(
            publication=None,
            error_type=type(error).__name__,
            error_message=str(error)[:512],
        )
    return _ExportRecoveryResponse(
        publication=publication,
        error_type=None,
        error_message=None,
    )


class PostgresParquetExportOperations:
    """Stream database rows through a bounded disk spool into Parquet generations."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        limits: ExportLimits | None = None,
        spool_directory: Path | None = None,
        location_target: Callable[..., _ExportLocationResponse] = (
            _resolve_export_location_in_process
        ),
        spool_target: Callable[..., _SpoolProcessResponse] = _write_spool_in_process,
        process_target: Callable[..., _ExportProcessResponse] = _export_partition_in_process,
        recovery_target: Callable[..., _ExportRecoveryResponse] = _recover_partition_in_process,
    ) -> None:
        self._engine = engine
        self._limits = limits or ExportLimits()
        self._spool_directory = spool_directory
        self._location_target = location_target
        self._spool_target = spool_target
        self._process_target = process_target
        self._recovery_target = recovery_target

    async def export_parquet(
        self,
        output: Path,
        *,
        since: datetime | None,
        until: datetime | None,
    ) -> dict[str, object]:
        _validate_range(since, until)
        publication_ledger = _PublicationLedger()
        operation_budget = ExportOperationBudget.start(self._limits)
        try:
            location = await self._resolve_export_location(output, operation_budget)
            destination = location.destination
            with anyio.fail_after(operation_budget.remaining_seconds()):
                async with self._engine.connect() as connection, connection.begin():
                    await connection.execute(
                        text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    )
                    statement_timeout_ms = max(
                        1,
                        int(operation_budget.remaining_seconds() * 1_000),
                    )
                    await connection.execute(
                        text("SELECT set_config('statement_timeout', :statement_timeout, true)"),
                        {"statement_timeout": f"{statement_timeout_ms}ms"},
                    )
                    snapshot = (
                        (
                            await connection.execute(
                                text(
                                    """
                                    SELECT pg_current_snapshot()::text AS snapshot_id,
                                           transaction_timestamp() AS started_at
                                    """
                                )
                            )
                        )
                        .mappings()
                        .one()
                    )
                    planned_partitions: list[
                        tuple[_EntityExport, tuple[_PlannedPartition, ...]]
                    ] = []
                    planned_rows = 0
                    for entity in _ENTITIES:
                        partitions = await self._partitions(
                            connection,
                            entity,
                            since=since,
                            until=until,
                        )
                        planned_partitions.append((entity, partitions))
                        entity_row_count = await self._row_count(
                            connection,
                            entity,
                            since=since,
                            until=until,
                        )
                        _validate_partition_row_count_total(partitions, entity_row_count)
                        planned_rows += entity_row_count
                    partition_count = sum(
                        len(partitions) for _entity, partitions in planned_partitions
                    )
                    _validate_partition_count(partition_count)
                    operation_budget.preflight(
                        partition_row_counts=tuple(
                            partition.row_count
                            for _entity, partitions in planned_partitions
                            for partition in partitions
                        ),
                        row_count=planned_rows,
                    )
                    for entity, partitions in planned_partitions:
                        for partition in partitions:
                            await self._export_partition(
                                connection,
                                destination,
                                entity,
                                retailer_id=partition.retailer_id,
                                partition_date=partition.partition_date,
                                since=since,
                                until=until,
                                operation_budget=operation_budget,
                                publication_recorder=publication_ledger.record,
                                resolved_spool_root=location.spool_root,
                            )
                    operation_budget.finish()
        except TimeoutError as error:
            failure = ExportValidationError("export operation exceeds max_operation_seconds")
            _raise_export_failure(publication_ledger.snapshot(), failure, error)
        except Exception as error:
            _raise_export_failure(publication_ledger.snapshot(), error)
        manifests = publication_ledger.snapshot()
        return {
            "status": "completed",
            "output": destination,
            "since": since,
            "until": until,
            "partition_count": len(manifests),
            "row_count": sum(cast(int, item["row_count"]) for item in manifests),
            "database_snapshot": str(snapshot["snapshot_id"]),
            "snapshot_started_at": cast(datetime, snapshot["started_at"]),
            "manifests": tuple(manifests),
        }

    async def _resolve_export_location(
        self,
        output: Path,
        operation_budget: ExportOperationBudget,
    ) -> _ExportLocation:
        remaining = operation_budget.remaining_seconds()
        if remaining <= 0:
            raise ExportValidationError("export operation exceeds max_operation_seconds")
        cleanup_reserve = operation_budget.cleanup_deadline - operation_budget.work_deadline
        response = await run_in_spawn_process(
            self._location_target,
            output,
            self._spool_directory,
            timeout_seconds=remaining,
            termination_timeout_seconds=cleanup_reserve / 2,
        )
        if response.error_type is not None or response.location is None:
            raise ExportValidationError(
                "export location resolution failed: "
                f"{response.error_type or 'unknown'}: {response.error_message or 'no detail'}"
            )
        return response.location

    async def _row_count(
        self,
        connection: AsyncConnection,
        entity: _EntityExport,
        *,
        since: datetime | None,
        until: datetime | None,
    ) -> int:
        value = await connection.scalar(
            text(entity.count_sql),
            {"since": since, "until": until},
        )
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ExportValidationError("database export preflight returned an invalid row count")
        return value

    async def _partitions(
        self,
        connection: AsyncConnection,
        entity: _EntityExport,
        *,
        since: datetime | None,
        until: datetime | None,
    ) -> tuple[_PlannedPartition, ...]:
        rows = (
            await connection.execute(
                text(entity.partition_sql),
                {
                    "since": since,
                    "until": until,
                    "partition_limit": _MAXIMUM_PARTITIONS + 1,
                },
            )
        ).all()
        if len(rows) > _MAXIMUM_PARTITIONS:
            raise ValueError(
                f"{entity.name} export exceeds the {_MAXIMUM_PARTITIONS}-partition bound"
            )
        partitions: list[_PlannedPartition] = []
        for row in rows:
            if (
                type(row.partition_date) is not date
                or type(row.row_count) is not int
                or row.row_count <= 0
            ):
                raise ExportValidationError(
                    "database export preflight returned an invalid partition count"
                )
            partitions.append(
                _PlannedPartition(
                    retailer_id=str(row.retailer_id),
                    partition_date=row.partition_date,
                    row_count=row.row_count,
                )
            )
        return tuple(partitions)

    async def _export_partition(
        self,
        connection: AsyncConnection,
        output: Path,
        entity: _EntityExport,
        *,
        retailer_id: str,
        partition_date: date,
        since: datetime | None,
        until: datetime | None,
        operation_budget: ExportOperationBudget | None = None,
        publication_recorder: Callable[[ExportResult], None] | None = None,
        resolved_spool_root: Path | None = None,
    ) -> ExportResult:
        active_budget = operation_budget or ExportOperationBudget.start(self._limits)
        active_budget.checkpoint()
        partition = ExportPartition(entity.name, retailer_id, partition_date)
        operation_identity = uuid4().hex
        if resolved_spool_root is None:
            location = await self._resolve_export_location(output, active_budget)
            output = location.destination
            spool_root = location.spool_root
        else:
            spool_root = resolved_spool_root
        spool = spool_root / f"makolet-export-{operation_identity}.jsonl"
        publication_journal = spool.with_name(
            f".{spool.name}.{operation_identity}.publication.json"
        )
        recovery_attempted = False
        try:
            row_count = 0
            spool_bytes = 0

            async def serialized_chunks() -> AsyncIterator[bytes]:
                nonlocal row_count, spool_bytes
                result = await connection.stream(
                    text(entity.rows_sql),
                    {
                        "retailer_id": retailer_id,
                        "partition_date": partition_date,
                        "since": since,
                        "until": until,
                    },
                )
                async for row in result.mappings():
                    row_count += 1
                    if row_count > self._limits.max_dataset_rows:
                        raise ExportValidationError(
                            "database export partition exceeds max_dataset_rows"
                        )
                    serialized = json.dumps(
                        dict(row),
                        ensure_ascii=False,
                        separators=(",", ":"),
                        default=_json_value,
                    ).encode("utf-8")
                    serialized_line = serialized + b"\n"
                    if spool_bytes + len(serialized_line) > self._limits.max_spool_bytes:
                        raise ExportValidationError(
                            "database export partition exceeds max_spool_bytes"
                        )
                    active_budget.charge_row(len(serialized_line))
                    spool_bytes += len(serialized_line)
                    for offset in range(0, len(serialized_line), _SPOOL_INPUT_CHUNK_BYTES):
                        yield serialized_line[offset : offset + _SPOOL_INPUT_CHUNK_BYTES]

            cleanup_reserve = active_budget.cleanup_deadline - active_budget.work_deadline
            spool_response = await run_in_spawn_process_with_input(
                self._spool_target,
                serialized_chunks(),
                spool,
                self._limits.max_spool_bytes,
                timeout_seconds=active_budget.remaining_seconds(),
                termination_timeout_seconds=cleanup_reserve / 2,
            )
            if spool_response.error_type is not None:
                raise _process_spool_error(spool_response)
            if spool_response.byte_length != spool_bytes:
                raise ExportValidationError("export spool child returned invalid byte accounting")
            process_response: _ExportProcessResponse | None = None
            process_failure: BaseException | None = None
            try:
                process_response = await run_in_spawn_process(
                    self._process_target,
                    output,
                    self._limits,
                    partition,
                    entity.schema,
                    spool,
                    active_budget,
                    operation_identity,
                    publication_journal,
                    timeout_seconds=active_budget.remaining_seconds(),
                    termination_timeout_seconds=cleanup_reserve / 2,
                )
            except BaseException as error:
                process_failure = error

            if process_response is not None:
                try:
                    _merge_process_budget(active_budget, process_response)
                except Exception as error:
                    if process_failure is None:
                        process_failure = error
            publication = process_response.publication if process_response is not None else None
            if publication is not None and publication_recorder is not None:
                publication_recorder(publication)
            with anyio.CancelScope(shield=True):
                recovery_attempted = True
                recovered = await self._recover_partition_publication(
                    output,
                    partition=partition,
                    schema=entity.schema,
                    spool=spool,
                    operation_identity=operation_identity,
                    publication_journal=publication_journal,
                    operation_budget=active_budget,
                )
            if (
                recovered is not None
                and recovered != publication
                and publication_recorder is not None
            ):
                publication_recorder(recovered)
            if process_failure is not None:
                raise process_failure
            if process_response is None:
                raise AssertionError("export publisher returned no response")
            if process_response.error_type is not None:
                raise _process_export_error(process_response)
            if process_response.result is None:
                raise ExportValidationError("export publisher returned no result")
            return process_response.result
        finally:
            if not recovery_attempted:
                with anyio.CancelScope(shield=True):
                    recovered = await self._recover_partition_publication(
                        output,
                        partition=partition,
                        schema=entity.schema,
                        spool=spool,
                        operation_identity=operation_identity,
                        publication_journal=publication_journal,
                        operation_budget=active_budget,
                    )
                    if recovered is not None and publication_recorder is not None:
                        publication_recorder(recovered)

    async def _recover_partition_publication(
        self,
        output: Path,
        *,
        partition: ExportPartition,
        schema: ExportSchema,
        spool: Path,
        operation_identity: str,
        publication_journal: Path,
        operation_budget: ExportOperationBudget,
    ) -> ExportResult | None:
        remaining = operation_budget.remaining_seconds(cleanup=True)
        if remaining <= 0:
            raise ExportValidationError("export publication cleanup exceeds max_operation_seconds")
        cleanup_work = remaining * 0.75
        cleanup_termination = remaining - cleanup_work
        try:
            response = await run_in_spawn_process(
                self._recovery_target,
                output,
                self._limits,
                partition,
                schema,
                spool,
                operation_identity,
                publication_journal,
                timeout_seconds=cleanup_work,
                termination_timeout_seconds=cleanup_termination,
            )
        except (ProcessDeadlineError, ProcessWorkerError, RuntimeError) as error:
            raise ExportValidationError(
                "export publication cleanup subprocess exceeded its bounded deadline"
            ) from error
        if response.error_type is not None:
            raise ExportValidationError(
                "export publication cleanup failed: "
                f"{response.error_type}: {response.error_message or 'no detail'}"
            )
        return response.publication


def _remove_spool_file(spool: Path) -> None:
    try:
        status = spool.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(status.st_mode) or stat.S_ISLNK(status.st_mode):
        raise ExportValidationError("export spool cleanup target is not a regular file")
    spool.unlink()


def _spooled_rows(path: Path, schema: ExportSchema) -> Iterator[Mapping[str, object]]:
    field_types = {field.name: field.value_type for field in schema.fields}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise TypeError("Export spool contains a non-object row")
            yield {
                str(key): _restore_spooled_value(item, field_types.get(str(key)))
                for key, item in value.items()
            }


def _restore_spooled_value(value: object, value_type: ExportType | None) -> object:
    if value is None or value_type is None:
        return value
    if value_type is ExportType.DECIMAL_STRING:
        if not isinstance(value, str):
            raise ExportValidationError("decimal export spool value is not text")
        try:
            return Decimal(value)
        except InvalidOperation as error:
            raise ExportValidationError("decimal export spool value is invalid") from error
    if value_type is ExportType.TIMESTAMP_ISO8601:
        if not isinstance(value, str):
            raise ExportValidationError("timestamp export spool value is not text")
        try:
            return datetime.fromisoformat(value)
        except ValueError as error:
            raise ExportValidationError("timestamp export spool value is invalid") from error
    return value


def _json_value(value: Any) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID | Decimal | date):
        return str(value)
    raise TypeError(f"Database export value {type(value).__name__} is not serializable")


def _result_dict(result: ExportResult) -> dict[str, object]:
    return {
        "manifest_path": result.manifest_path,
        "dataset_id": result.manifest.dataset_id,
        "entity": result.manifest.partition.entity,
        "retailer_id": result.manifest.partition.retailer_id,
        "partition_date": result.manifest.partition.partition_date,
        "row_count": result.manifest.row_count,
        "file_count": len(result.manifest.files),
        "created": result.created,
    }


def _merge_process_budget(
    operation_budget: ExportOperationBudget,
    response: _ExportProcessResponse,
) -> None:
    if (
        response.consumed_files < operation_budget.consumed_files
        or response.consumed_files > operation_budget.limits.max_files
        or response.consumed_output_bytes < operation_budget.consumed_output_bytes
        or response.consumed_output_bytes > operation_budget.limits.max_output_bytes
    ):
        raise ExportValidationError("export publisher returned invalid budget accounting")
    operation_budget.consumed_files = response.consumed_files
    operation_budget.consumed_output_bytes = response.consumed_output_bytes


def _process_export_error(response: _ExportProcessResponse) -> Exception:
    message = response.error_message or "export publisher failed without detail"
    if response.error_type == ExportConflictError.__name__:
        return ExportConflictError(message)
    if response.error_type == ExportValidationError.__name__:
        return ExportValidationError(message)
    if response.error_type in {"FileExistsError", "OSError"}:
        return OSError(message)
    if response.error_type in {"TypeError", "ValueError"}:
        return ExportValidationError(message)
    return ExportValidationError(
        f"export publisher failed with {response.error_type or 'an unknown error'}: {message}"
    )


def _process_spool_error(response: _SpoolProcessResponse) -> Exception:
    message = response.error_message or "export spool child failed without detail"
    if response.error_type == ExportValidationError.__name__:
        return ExportValidationError(message)
    if response.error_type in {"FileExistsError", "OSError"}:
        return OSError(message)
    if response.error_type in {"TypeError", "ValueError"}:
        return ExportValidationError(message)
    return ExportValidationError(
        f"export spool child failed with {response.error_type or 'an unknown error'}: {message}"
    )


def _validate_partition_count(partition_count: int) -> None:
    if partition_count > _MAXIMUM_PARTITIONS:
        raise ExportValidationError("database export exceeds the aggregate partition bound")


def _validate_partition_row_count_total(
    partitions: tuple[_PlannedPartition, ...],
    row_count: int,
) -> None:
    if sum(partition.row_count for partition in partitions) != row_count:
        raise ExportValidationError(
            "database export preflight partition counts do not match its row count"
        )


def _raise_export_failure(
    manifests: Sequence[Mapping[str, object]],
    failure: Exception,
    cause: Exception | None = None,
) -> None:
    published = tuple(
        cast(Path, item["manifest_path"]) for item in manifests if cast(bool, item["created"])
    )
    if published:
        partial = ExportPartialPublicationError(published, failure)
        if cause is not None:
            raise partial from cause
        raise partial from failure
    if cause is not None:
        raise failure from cause
    raise failure


def _validate_range(since: datetime | None, until: datetime | None) -> None:
    for name, value in (("since", since), ("until", until)):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError(f"{name} must include a timezone")
    if since is not None and until is not None and since > until:
        raise ValueError("since must not be after until")


__all__ = ["PostgresParquetExportOperations"]
