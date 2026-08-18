from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch
from uuid import UUID

import anyio
import psutil  # type: ignore[import-untyped]
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from makolet.adapters.export import postgres as postgres_export
from makolet.adapters.export.models import (
    ExportLimits,
    ExportOperationBudget,
    ExportPartialPublicationError,
    ExportValidationError,
)

_FAST_WATCHDOG_CEILING_SECONDS = 3.0 if os.name == "nt" else 0.2
_PROCESS_WARMUP_SECONDS = 30.0
_REACHED_WATCHDOG_SECONDS = 8.0


def _export_then_stall(*args: Any) -> Any:
    response = postgres_export._export_partition_in_process(*args)
    time.sleep(10)
    return response


def _stall_parquet_fsync(*args: Any) -> Any:
    with patch(
        "makolet.adapters.export.dataset.os.fsync",
        side_effect=lambda _descriptor: time.sleep(10),
    ):
        return postgres_export._export_partition_in_process(*args)


def _stall_publication_recovery(*_args: Any) -> Any:
    time.sleep(10)
    raise AssertionError("stalled recovery unexpectedly resumed")


def _stall_location_resolution(
    output: Path,
    configured_spool_directory: Path | None,
) -> Any:
    (output.parent / "resolve-reached.probe").write_text(str(os.getpid()), encoding="ascii")
    with patch.object(Path, "resolve", side_effect=lambda *_args, **_kwargs: time.sleep(10)):
        return postgres_export._resolve_export_location_in_process(
            output,
            configured_spool_directory,
        )


def _stall_spool_create(chunks: Any, spool: Path, maximum_bytes: int) -> Any:
    return _stall_spool_phase(chunks, spool, maximum_bytes, "create", "_open_spool_file")


def _stall_spool_write(chunks: Any, spool: Path, maximum_bytes: int) -> Any:
    return _stall_spool_phase(chunks, spool, maximum_bytes, "write", "_write_spool_chunk")


def _stall_spool_flush(chunks: Any, spool: Path, maximum_bytes: int) -> Any:
    return _stall_spool_phase(chunks, spool, maximum_bytes, "flush", "_flush_spool_file")


def _stall_spool_close(chunks: Any, spool: Path, maximum_bytes: int) -> Any:
    return _stall_spool_phase(chunks, spool, maximum_bytes, "close", "_close_spool_file")


def _mark_spool_stream_started(chunks: Any, spool: Path, maximum_bytes: int) -> Any:
    original_write = postgres_export._write_spool_chunk

    def mark_and_write(handle: Any, chunk: bytes) -> None:
        original_write(handle, chunk)
        (spool.parent.parent / "spool-stream-started.probe").write_text(
            str(os.getpid()),
            encoding="ascii",
        )

    with patch.object(postgres_export, "_write_spool_chunk", side_effect=mark_and_write):
        return postgres_export._write_spool_in_process(chunks, spool, maximum_bytes)


def _stall_spool_phase(
    chunks: Any,
    spool: Path,
    maximum_bytes: int,
    phase: str,
    target_name: str,
) -> Any:
    (spool.parent.parent / f"spool-{phase}-reached.probe").write_text(
        str(os.getpid()),
        encoding="ascii",
    )
    with patch.object(
        postgres_export,
        target_name,
        side_effect=lambda *_args, **_kwargs: time.sleep(10),
    ):
        return postgres_export._write_spool_in_process(chunks, spool, maximum_bytes)


def _row(index: int) -> dict[str, object]:
    return {
        "retailer_id": "retailer",
        "retailer_item_id": UUID(f"10000000-0000-0000-0000-{index:012d}"),
        "source_item_code": f"item-{index}",
        "gtin": "1234567890128",
        "item_name": "Bounded coffee",
        "store_id": UUID(f"20000000-0000-0000-0000-{index:012d}"),
        "source_store_code": "store-1",
        "item_price": Decimal("12.30"),
        "unit_of_measure_price": Decimal("12.30"),
        "allow_discount": True,
        "source_updated_at": datetime(2026, 8, 12, tzinfo=UTC),
        "first_observed_at": datetime(2026, 8, 12, tzinfo=UTC),
        "last_observed_at": datetime(2026, 8, 12, tzinfo=UTC),
        "source_file_id": UUID(f"30000000-0000-0000-0000-{index:012d}"),
        "source_portal_id": UUID("40000000-0000-0000-0000-000000000001"),
        "source_portal_key": "portal",
        "source_document_type": "price_full",
        "source_timestamp": datetime(2026, 8, 12, tzinfo=UTC),
        "source_download_finished_at": datetime(2026, 8, 12, tzinfo=UTC),
        "archive_content_sha256": "a" * 64,
    }


class _FakeStream:
    def __init__(self, rows: tuple[Mapping[str, object], ...]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeStream:
        return self

    async def __aiter__(self) -> AsyncIterator[Mapping[str, object]]:
        for row in self._rows:
            yield row


class _FakeConnection:
    def __init__(self, rows: tuple[Mapping[str, object], ...]) -> None:
        self._rows = rows

    async def __aenter__(self) -> _FakeConnection:
        return self

    async def __aexit__(self, *args: object) -> None:
        del args

    async def stream(self, *args: object, **kwargs: object) -> _FakeStream:
        del args, kwargs
        return _FakeStream(self._rows)


class _FakeEngine:
    def __init__(self, rows: tuple[Mapping[str, object], ...]) -> None:
        self._rows = rows

    def connect(self) -> _FakeConnection:
        return _FakeConnection(self._rows)


class _FakeTransaction:
    async def __aenter__(self) -> _FakeTransaction:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _FakeExecuteResult:
    def __init__(
        self,
        *,
        rows: tuple[SimpleNamespace, ...] = (),
        mapping: Mapping[str, object] | None = None,
    ) -> None:
        self._rows = rows
        self._mapping = mapping

    def all(self) -> tuple[SimpleNamespace, ...]:
        return self._rows

    def mappings(self) -> _FakeExecuteResult:
        return self

    def one(self) -> Mapping[str, object]:
        assert self._mapping is not None
        return self._mapping


class _FakePublicConnection:
    def __init__(self, *, row_count: int, include_partitions: bool) -> None:
        self.row_count = row_count
        self.include_partitions = include_partitions
        self.statement_timeout: str | None = None

    async def __aenter__(self) -> _FakePublicConnection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def begin(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def execute(
        self,
        statement: object,
        parameters: Mapping[str, object] | None = None,
    ) -> _FakeExecuteResult:
        sql = str(statement)
        if "set_config('statement_timeout'" in sql:
            assert parameters is not None
            self.statement_timeout = str(parameters["statement_timeout"])
            return _FakeExecuteResult()
        if "pg_current_snapshot" in sql:
            return _FakeExecuteResult(
                mapping={
                    "snapshot_id": "1:2:",
                    "started_at": datetime(2026, 8, 17, tzinfo=UTC),
                }
            )
        if "GROUP BY retailer.source_key" in sql:
            partitions = (
                SimpleNamespace(
                    retailer_id="retailer",
                    partition_date=datetime(2026, 8, 17, tzinfo=UTC).date(),
                    row_count=self.row_count,
                ),
            )
            return _FakeExecuteResult(rows=partitions if self.include_partitions else ())
        return _FakeExecuteResult()

    async def scalar(
        self,
        _statement: object,
        _parameters: Mapping[str, object],
    ) -> int:
        return self.row_count

    async def stream(self, *_args: object, **_kwargs: object) -> _FakeStream:
        return _FakeStream((_row(1),))


class _FakePublicEngine:
    def __init__(self, connection: _FakePublicConnection) -> None:
        self.connection = connection

    def connect(self) -> _FakePublicConnection:
        return self.connection


def _operations(
    rows: tuple[Mapping[str, object], ...],
    *,
    limits: ExportLimits,
    spool_directory: Path,
    location_target: Any = postgres_export._resolve_export_location_in_process,
    spool_target: Any = postgres_export._write_spool_in_process,
) -> postgres_export.PostgresParquetExportOperations:
    engine = cast(AsyncEngine, cast(Any, _FakeEngine(rows)))
    return postgres_export.PostgresParquetExportOperations(
        engine,
        limits=limits,
        spool_directory=spool_directory,
        location_target=location_target,
        spool_target=spool_target,
    )


def _public_operations(
    connection: _FakePublicConnection,
    *,
    limits: ExportLimits,
    spool_directory: Path,
    location_target: Any = postgres_export._resolve_export_location_in_process,
    spool_target: Any = postgres_export._write_spool_in_process,
    process_target: Any = postgres_export._export_partition_in_process,
    recovery_target: Any = postgres_export._recover_partition_in_process,
) -> postgres_export.PostgresParquetExportOperations:
    engine = cast(AsyncEngine, cast(Any, _FakePublicEngine(connection)))
    return postgres_export.PostgresParquetExportOperations(
        engine,
        limits=limits,
        spool_directory=spool_directory,
        location_target=location_target,
        spool_target=spool_target,
        process_target=process_target,
        recovery_target=recovery_target,
    )


@pytest.mark.asyncio
async def test_database_export_enforces_row_limit_during_first_pass(tmp_path: Path) -> None:
    spool = tmp_path / "configured-spool"
    operations = _operations(
        (_row(1), _row(2)),
        limits=ExportLimits(max_rows_per_file=1, max_dataset_rows=1),
        spool_directory=spool,
    )

    with pytest.raises(ExportValidationError, match="max_dataset_rows"):
        await operations._export_partition(
            cast(Any, _FakeConnection((_row(1), _row(2)))),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
            since=None,
            until=None,
        )

    assert spool.is_dir()
    assert list(spool.iterdir()) == []


@pytest.mark.asyncio
async def test_database_export_enforces_spool_byte_limit_during_first_pass(
    tmp_path: Path,
) -> None:
    spool = tmp_path / "configured-spool"
    operations = _operations(
        (_row(1),),
        limits=ExportLimits(max_spool_bytes=32),
        spool_directory=spool,
    )

    with pytest.raises(ExportValidationError, match="max_spool_bytes"):
        await operations._export_partition(
            cast(Any, _FakeConnection((_row(1),))),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
            since=None,
            until=None,
        )

    assert spool.is_dir()
    assert list(spool.iterdir()) == []


@pytest.mark.asyncio
async def test_database_export_uses_configured_spool_and_cleans_it(tmp_path: Path) -> None:
    spool = tmp_path / "persistent-spool"
    operations = _operations(
        (_row(1),),
        limits=ExportLimits(),
        spool_directory=spool,
    )

    result = await operations._export_partition(
        cast(Any, _FakeConnection((_row(1),))),
        tmp_path / "output",
        postgres_export._CURRENT_PRICES,
        retailer_id="retailer",
        partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
        since=None,
        until=None,
    )

    assert result.manifest.row_count == 1
    assert result.created
    assert spool.is_dir()
    assert list(spool.iterdir()) == []


def _serialized_row_bytes(index: int) -> int:
    return (
        len(
            json.dumps(
                _row(index),
                ensure_ascii=False,
                separators=(",", ":"),
                default=postgres_export._json_value,
            ).encode("utf-8")
        )
        + 1
    )


def test_database_export_preflights_the_operation_wide_row_and_file_bounds() -> None:
    row_limited = ExportOperationBudget.start(ExportLimits(max_rows_per_file=1, max_dataset_rows=2))
    file_limited = ExportOperationBudget.start(
        ExportLimits(max_rows_per_file=2, max_files=1, max_dataset_rows=2)
    )

    with pytest.raises(ExportValidationError, match="max_dataset_rows"):
        row_limited.preflight(partition_row_counts=(1, 2), row_count=3)
    with pytest.raises(ExportValidationError, match="max_files"):
        file_limited.preflight(partition_row_counts=(1, 1), row_count=2)

    assert row_limited.consumed_rows == 0
    assert file_limited.consumed_files == 0


def test_database_export_preflight_sums_minimum_files_per_partition() -> None:
    budget = ExportOperationBudget.start(
        ExportLimits(max_rows_per_file=10, max_files=3, max_dataset_rows=22)
    )

    with pytest.raises(ExportValidationError, match="max_files"):
        budget.preflight(partition_row_counts=(11, 11), row_count=22)

    assert budget.consumed_files == 0


def test_database_export_reserves_a_recovery_process_window() -> None:
    budget = ExportOperationBudget.start(ExportLimits(max_operation_seconds=6))

    assert budget.cleanup_deadline - budget.work_deadline == pytest.approx(3)


@pytest.mark.asyncio
async def test_database_export_public_preflight_sets_timeout_before_any_publication(
    tmp_path: Path,
) -> None:
    connection = _FakePublicConnection(row_count=2, include_partitions=True)
    operations = _public_operations(
        connection,
        limits=ExportLimits(max_rows_per_file=3, max_dataset_rows=3),
        spool_directory=tmp_path / "spool",
    )
    output = tmp_path / "output"

    with pytest.raises(ExportValidationError, match="max_dataset_rows"):
        await operations.export_parquet(output, since=None, until=None)

    assert connection.statement_timeout is not None
    assert connection.statement_timeout.endswith("ms")
    assert not list(output.rglob("_manifest.json"))
    assert not (tmp_path / "spool").exists()


@pytest.mark.asyncio
async def test_database_export_empty_snapshot_preserves_legitimate_completion(
    tmp_path: Path,
) -> None:
    connection = _FakePublicConnection(row_count=0, include_partitions=False)
    operations = _public_operations(
        connection,
        limits=ExportLimits(),
        spool_directory=tmp_path / "spool",
    )

    result = await operations.export_parquet(tmp_path / "output", since=None, until=None)

    assert result["status"] == "completed"
    assert result["partition_count"] == 0
    assert result["row_count"] == 0
    assert result["database_snapshot"] == "1:2:"
    assert result["manifests"] == ()


@pytest.mark.asyncio
async def test_database_export_spool_budget_is_shared_across_partitions(
    tmp_path: Path,
) -> None:
    row_bytes = _serialized_row_bytes(1)
    limits = ExportLimits(
        max_rows_per_file=1,
        max_files=2,
        max_dataset_rows=2,
        max_spool_bytes=row_bytes,
    )
    operations = _operations(
        (_row(1),),
        limits=limits,
        spool_directory=tmp_path / "spool",
    )
    operation_budget = ExportOperationBudget.start(limits)

    first = await operations._export_partition(
        cast(Any, _FakeConnection((_row(1),))),
        tmp_path / "output",
        postgres_export._CURRENT_PRICES,
        retailer_id="retailer",
        partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
        since=None,
        until=None,
        operation_budget=operation_budget,
    )
    with pytest.raises(ExportValidationError, match="max_spool_bytes"):
        await operations._export_partition(
            cast(Any, _FakeConnection((_row(1),))),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 13, tzinfo=UTC).date(),
            since=None,
            until=None,
            operation_budget=operation_budget,
        )

    assert first.created
    assert operation_budget.consumed_rows == 1
    assert operation_budget.consumed_spool_bytes == row_bytes
    assert list((tmp_path / "spool").iterdir()) == []
    assert not list((tmp_path / "output").rglob(".stage-*"))


@pytest.mark.asyncio
async def test_database_export_file_budget_is_shared_across_partitions(
    tmp_path: Path,
) -> None:
    limits = ExportLimits(max_rows_per_file=1, max_files=1, max_dataset_rows=2)
    operations = _operations(
        (_row(1),),
        limits=limits,
        spool_directory=tmp_path / "spool",
    )
    operation_budget = ExportOperationBudget.start(limits)

    first = await operations._export_partition(
        cast(Any, _FakeConnection((_row(1),))),
        tmp_path / "output",
        postgres_export._CURRENT_PRICES,
        retailer_id="retailer",
        partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
        since=None,
        until=None,
        operation_budget=operation_budget,
    )
    with pytest.raises(ExportValidationError, match="max_files"):
        await operations._export_partition(
            cast(Any, _FakeConnection((_row(2),))),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 13, tzinfo=UTC).date(),
            since=None,
            until=None,
            operation_budget=operation_budget,
        )

    assert first.created
    assert operation_budget.consumed_files == 1
    assert not list((tmp_path / "output").rglob(".stage-*"))
    assert len(list((tmp_path / "output").rglob("_manifest.json"))) == 1


@pytest.mark.asyncio
async def test_database_export_output_budget_fails_before_partition_publication(
    tmp_path: Path,
) -> None:
    baseline = await _operations(
        (_row(1),),
        limits=ExportLimits(),
        spool_directory=tmp_path / "baseline-spool",
    )._export_partition(
        cast(Any, _FakeConnection((_row(1),))),
        tmp_path / "baseline",
        postgres_export._CURRENT_PRICES,
        retailer_id="retailer",
        partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
        since=None,
        until=None,
    )
    output_bytes = baseline.manifest.files[0].byte_length
    limits = ExportLimits(max_output_bytes=output_bytes - 1)
    operations = _operations(
        (_row(1),),
        limits=limits,
        spool_directory=tmp_path / "spool",
    )

    with pytest.raises(ExportValidationError, match="max_output_bytes"):
        await operations._export_partition(
            cast(Any, _FakeConnection((_row(1),))),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
            since=None,
            until=None,
        )

    assert not list((tmp_path / "output").rglob("_manifest.json"))
    assert not list((tmp_path / "output").rglob("dataset=*"))
    assert not list((tmp_path / "output").rglob(".stage-*"))


@pytest.mark.asyncio
async def test_database_export_expired_budget_fails_before_spooling(tmp_path: Path) -> None:
    limits = ExportLimits()
    operations = _operations(
        (_row(1),),
        limits=limits,
        spool_directory=tmp_path / "spool",
    )
    expired = ExportOperationBudget(limits, work_deadline=0.0, cleanup_deadline=0.0)

    with pytest.raises(ExportValidationError, match="max_operation_seconds"):
        await operations._export_partition(
            cast(Any, _FakeConnection((_row(1),))),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
            since=None,
            until=None,
            operation_budget=expired,
        )

    assert not (tmp_path / "spool").exists()


@pytest.mark.asyncio
async def test_database_export_shields_spool_cleanup_from_outer_cancellation(
    tmp_path: Path,
) -> None:
    class BlockingStream:
        def mappings(self) -> BlockingStream:
            return self

        async def __aiter__(self) -> AsyncIterator[Mapping[str, object]]:
            await anyio.sleep_forever()
            yield _row(1)

    class BlockingConnection:
        async def stream(self, *_args: object, **_kwargs: object) -> BlockingStream:
            return BlockingStream()

    spool = tmp_path / "spool"
    operations = _operations(
        (),
        limits=ExportLimits(),
        spool_directory=spool,
    )

    with pytest.raises(TimeoutError):
        with anyio.fail_after(0.05):
            await operations._export_partition(
                cast(Any, BlockingConnection()),
                tmp_path / "output",
                postgres_export._CURRENT_PRICES,
                retailer_id="retailer",
                partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
                since=None,
                until=None,
            )

    assert not spool.exists() or list(spool.iterdir()) == []


def test_partial_publication_error_names_only_new_immutable_manifests(
    tmp_path: Path,
) -> None:
    published = tmp_path / "new" / "_manifest.json"
    existing = tmp_path / "existing" / "_manifest.json"
    manifests: list[dict[str, object]] = [
        {"manifest_path": published, "created": True},
        {"manifest_path": existing, "created": False},
    ]

    with pytest.raises(ExportPartialPublicationError) as caught:
        postgres_export._raise_export_failure(
            manifests,
            ExportValidationError("later partition failed"),
        )

    assert caught.value.manifests == (published,)
    assert isinstance(caught.value.original_error, ExportValidationError)


@pytest.mark.asyncio
async def test_database_export_records_publication_before_outer_timeout_is_delivered(
    tmp_path: Path,
) -> None:
    connection = _FakePublicConnection(row_count=1, include_partitions=True)
    operations = _public_operations(
        connection,
        limits=ExportLimits(
            max_rows_per_file=1,
            max_files=2,
            max_dataset_rows=2,
            max_operation_seconds=10,
        ),
        spool_directory=tmp_path / "spool",
        process_target=_export_then_stall,
    )

    started_at = time.monotonic()
    with pytest.raises(ExportPartialPublicationError) as caught:
        await operations.export_parquet(tmp_path / "output", since=None, until=None)

    assert time.monotonic() - started_at < 11
    assert len(caught.value.manifests) == 1
    assert caught.value.manifests[0].is_file()


@pytest.mark.asyncio
async def test_database_export_kills_stalled_fsync_and_cleans_exact_staging(
    tmp_path: Path,
) -> None:
    limits = ExportLimits(max_operation_seconds=6)
    operations = postgres_export.PostgresParquetExportOperations(
        cast(AsyncEngine, cast(Any, _FakeEngine((_row(1),)))),
        limits=limits,
        spool_directory=tmp_path / "spool",
        process_target=_stall_parquet_fsync,
    )
    started_at = time.monotonic()

    with pytest.raises(TimeoutError):
        await operations._export_partition(
            cast(Any, _FakeConnection((_row(1),))),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
            since=None,
            until=None,
        )

    assert time.monotonic() - started_at < 7
    assert not list((tmp_path / "output").rglob(".stage-*"))
    assert not list((tmp_path / "output").rglob("dataset=*"))
    assert not list((tmp_path / "output").rglob("_manifest.json"))
    assert list((tmp_path / "spool").iterdir()) == []


@pytest.mark.asyncio
async def test_database_export_bounds_a_stalled_cleanup_subprocess(tmp_path: Path) -> None:
    limits = ExportLimits(max_operation_seconds=6)
    operations = postgres_export.PostgresParquetExportOperations(
        cast(AsyncEngine, cast(Any, _FakeEngine((_row(1),)))),
        limits=limits,
        spool_directory=tmp_path / "spool",
        process_target=_stall_parquet_fsync,
        recovery_target=_stall_publication_recovery,
    )
    started_at = time.monotonic()

    with pytest.raises(ExportValidationError, match="cleanup subprocess"):
        await operations._export_partition(
            cast(Any, _FakeConnection((_row(1),))),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
            since=None,
            until=None,
        )

    assert time.monotonic() - started_at < 7


@pytest.mark.asyncio
async def test_database_export_ledgers_publication_before_cleanup_subprocess_stalls(
    tmp_path: Path,
) -> None:
    connection = _FakePublicConnection(row_count=1, include_partitions=True)
    operations = _public_operations(
        connection,
        limits=ExportLimits(
            max_rows_per_file=1,
            max_files=2,
            max_dataset_rows=2,
            max_operation_seconds=6,
        ),
        spool_directory=tmp_path / "spool",
        recovery_target=_stall_publication_recovery,
    )
    started_at = time.monotonic()

    with pytest.raises(ExportPartialPublicationError) as caught:
        await operations.export_parquet(tmp_path / "output", since=None, until=None)

    assert time.monotonic() - started_at < 7
    assert len(caught.value.manifests) == 1
    assert caught.value.manifests[0].is_file()


@pytest.mark.asyncio
async def test_database_export_bounds_stalled_output_resolution_at_point_one_seconds(
    tmp_path: Path,
) -> None:
    warmup = _operations(
        (),
        limits=ExportLimits(max_operation_seconds=_PROCESS_WARMUP_SECONDS),
        spool_directory=tmp_path / "warmup-spool",
    )
    await warmup._resolve_export_location(
        tmp_path / "warmup-output",
        ExportOperationBudget.start(ExportLimits(max_operation_seconds=_PROCESS_WARMUP_SECONDS)),
    )
    await anyio.sleep(0.05)
    current_process = psutil.Process()
    baseline_children = {child.pid for child in current_process.children(recursive=True)}
    baseline_handles = (
        current_process.num_handles() if os.name == "nt" else current_process.num_fds()
    )
    operations = _public_operations(
        _FakePublicConnection(row_count=1, include_partitions=True),
        limits=ExportLimits(max_operation_seconds=0.1),
        spool_directory=tmp_path / "spool",
        location_target=_stall_location_resolution,
    )
    started_at = time.monotonic()

    with pytest.raises((ExportValidationError, TimeoutError, RuntimeError)):
        await operations.export_parquet(tmp_path / "output", since=None, until=None)

    assert time.monotonic() - started_at < _FAST_WATCHDOG_CEILING_SECONDS
    await anyio.sleep(0.1)
    assert {child.pid for child in current_process.children(recursive=True)} <= baseline_children
    assert not any(
        thread.is_alive() and thread.name == "makolet-archive-bootstrap"
        for thread in threading.enumerate()
    )
    handles = current_process.num_handles() if os.name == "nt" else current_process.num_fds()
    assert handles <= baseline_handles
    assert not (tmp_path / "output").exists()
    assert not (tmp_path / "spool").exists()


@pytest.mark.asyncio
async def test_database_export_kills_a_reached_stalled_output_resolution(
    tmp_path: Path,
) -> None:
    operations = _public_operations(
        _FakePublicConnection(row_count=1, include_partitions=True),
        limits=ExportLimits(max_operation_seconds=_REACHED_WATCHDOG_SECONDS),
        spool_directory=tmp_path / "spool",
        location_target=_stall_location_resolution,
    )
    started_at = time.monotonic()

    with pytest.raises((ExportValidationError, TimeoutError, RuntimeError)):
        await operations.export_parquet(tmp_path / "output", since=None, until=None)

    assert time.monotonic() - started_at < _REACHED_WATCHDOG_SECONDS + 1
    assert (tmp_path / "resolve-reached.probe").is_file()
    assert not (tmp_path / "output").exists()
    assert not (tmp_path / "spool").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spool_target",
    [_stall_spool_create, _stall_spool_write, _stall_spool_flush, _stall_spool_close],
)
async def test_database_export_bounds_stalled_spool_filesystem_phases_at_point_one_seconds(
    tmp_path: Path,
    spool_target: Any,
) -> None:
    warmup = _operations(
        (),
        limits=ExportLimits(max_operation_seconds=_PROCESS_WARMUP_SECONDS),
        spool_directory=tmp_path / "warmup-spool",
    )
    await warmup._resolve_export_location(
        tmp_path / "warmup-output",
        ExportOperationBudget.start(ExportLimits(max_operation_seconds=_PROCESS_WARMUP_SECONDS)),
    )
    await anyio.sleep(0.05)
    current_process = psutil.Process()
    baseline_children = {child.pid for child in current_process.children(recursive=True)}
    baseline_handles = (
        current_process.num_handles() if os.name == "nt" else current_process.num_fds()
    )
    spool_root = tmp_path / "spool"
    operations = _operations(
        (_row(1),),
        limits=ExportLimits(max_operation_seconds=0.1),
        spool_directory=spool_root,
        spool_target=spool_target,
    )
    started_at = time.monotonic()

    with pytest.raises((ExportValidationError, TimeoutError, OSError, RuntimeError)):
        await operations._export_partition(
            cast(Any, _FakeConnection((_row(1),))),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
            since=None,
            until=None,
            resolved_spool_root=spool_root,
        )

    assert time.monotonic() - started_at < _FAST_WATCHDOG_CEILING_SECONDS
    await anyio.sleep(0.1)
    assert {child.pid for child in current_process.children(recursive=True)} <= baseline_children
    assert not any(
        thread.is_alive() and thread.name == "makolet-archive-bootstrap"
        for thread in threading.enumerate()
    )
    handles = current_process.num_handles() if os.name == "nt" else current_process.num_fds()
    assert handles <= baseline_handles
    assert not list(spool_root.glob("makolet-export-*.jsonl"))
    assert not list(spool_root.glob("*.publication.json"))
    assert not list((tmp_path / "output").rglob(".stage-*"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "spool_target"),
    [
        ("create", _stall_spool_create),
        ("write", _stall_spool_write),
        ("flush", _stall_spool_flush),
        ("close", _stall_spool_close),
    ],
)
async def test_database_export_kills_each_reached_spool_filesystem_phase(
    tmp_path: Path,
    phase: str,
    spool_target: Any,
) -> None:
    spool_root = tmp_path / "spool"
    operations = _operations(
        (_row(1),),
        limits=ExportLimits(max_operation_seconds=_REACHED_WATCHDOG_SECONDS),
        spool_directory=spool_root,
        spool_target=spool_target,
    )
    started_at = time.monotonic()

    with pytest.raises((ExportValidationError, TimeoutError, OSError, RuntimeError)):
        await operations._export_partition(
            cast(Any, _FakeConnection((_row(1),))),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
            since=None,
            until=None,
            resolved_spool_root=spool_root,
        )

    assert time.monotonic() - started_at < _REACHED_WATCHDOG_SECONDS + 1
    assert (tmp_path / f"spool-{phase}-reached.probe").is_file()
    assert not list(spool_root.glob("makolet-export-*.jsonl"))
    assert not list(spool_root.glob("*.publication.json"))


@pytest.mark.asyncio
async def test_database_export_streams_multiple_rows_through_the_spool_child(
    tmp_path: Path,
) -> None:
    result = await _operations(
        (_row(1), _row(2), _row(3)),
        limits=ExportLimits(max_rows_per_file=3, max_dataset_rows=3),
        spool_directory=tmp_path / "spool",
    )._export_partition(
        cast(Any, _FakeConnection((_row(1), _row(2), _row(3)))),
        tmp_path / "output",
        postgres_export._CURRENT_PRICES,
        retailer_id="retailer",
        partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
        since=None,
        until=None,
    )

    assert result.manifest.row_count == 3
    assert not list((tmp_path / "spool").glob("makolet-export-*.jsonl"))


@pytest.mark.asyncio
async def test_outer_cancellation_kills_an_active_spool_writer_and_cleans_exact_file(
    tmp_path: Path,
) -> None:
    class BlockingAfterFirstRow:
        def mappings(self) -> BlockingAfterFirstRow:
            return self

        async def __aiter__(self) -> AsyncIterator[Mapping[str, object]]:
            yield _row(1)
            await anyio.sleep_forever()

    class BlockingConnection:
        async def stream(self, *_args: object, **_kwargs: object) -> BlockingAfterFirstRow:
            return BlockingAfterFirstRow()

    warmup = _operations(
        (),
        limits=ExportLimits(max_operation_seconds=_PROCESS_WARMUP_SECONDS),
        spool_directory=tmp_path / "warmup-spool",
    )
    await warmup._resolve_export_location(
        tmp_path / "warmup-output",
        ExportOperationBudget.start(ExportLimits(max_operation_seconds=_PROCESS_WARMUP_SECONDS)),
    )
    await anyio.sleep(0.05)
    current_process = psutil.Process()
    baseline_children = {child.pid for child in current_process.children(recursive=True)}
    baseline_handles = (
        current_process.num_handles() if os.name == "nt" else current_process.num_fds()
    )
    spool_root = tmp_path / "spool"
    operations = _operations(
        (),
        limits=ExportLimits(max_operation_seconds=5),
        spool_directory=spool_root,
        spool_target=_mark_spool_stream_started,
    )

    async def run_export() -> None:
        await operations._export_partition(
            cast(Any, BlockingConnection()),
            tmp_path / "output",
            postgres_export._CURRENT_PRICES,
            retailer_id="retailer",
            partition_date=datetime(2026, 8, 12, tzinfo=UTC).date(),
            since=None,
            until=None,
            resolved_spool_root=spool_root,
        )

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(run_export)
        with anyio.fail_after(3):
            while not (tmp_path / "spool-stream-started.probe").exists():  # noqa: ASYNC110
                await anyio.sleep(0.01)
        tasks.cancel_scope.cancel()

    await anyio.sleep(0.1)
    assert {child.pid for child in current_process.children(recursive=True)} <= baseline_children
    assert not any(
        thread.is_alive() and thread.name == "makolet-archive-bootstrap"
        for thread in threading.enumerate()
    )
    handles = current_process.num_handles() if os.name == "nt" else current_process.num_fds()
    assert handles <= baseline_handles
    assert not list(spool_root.glob("makolet-export-*.jsonl"))
    assert not list(spool_root.glob("*.publication.json"))
