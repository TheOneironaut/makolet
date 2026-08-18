from __future__ import annotations

import json
import os
import stat
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, BinaryIO, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from makolet.adapters.export import postgres as postgres_export
from makolet.adapters.export.models import (
    ExportConflictError,
    ExportField,
    ExportLimits,
    ExportManifest,
    ExportOperationBudget,
    ExportPartialPublicationError,
    ExportPartition,
    ExportResult,
    ExportSchema,
    ExportType,
    ExportValidationError,
    ManifestFile,
)


def _schema() -> ExportSchema:
    return ExportSchema(
        entity="current_prices",
        version="test-1",
        fields=(
            ExportField("amount", ExportType.DECIMAL_STRING, False),
            ExportField("observed_at", ExportType.TIMESTAMP_ISO8601, False),
            ExportField("label", ExportType.STRING),
        ),
    )


def _partition() -> ExportPartition:
    return ExportPartition("current_prices", "retailer", date(2026, 8, 17))


def _result(root: Path, *, created: bool = True) -> ExportResult:
    partition = _partition()
    schema = _schema()
    manifest = ExportManifest(
        dataset_id="dataset-test",
        partition=partition,
        schema=schema,
        row_count=1,
        files=(
            ManifestFile(
                path="part-00000.parquet",
                sha256="a" * 64,
                byte_length=7,
                row_count=1,
            ),
        ),
    )
    return ExportResult(root / "_manifest.json", manifest, created)


def test_location_child_resolves_default_and_configured_spools_and_reports_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    default = postgres_export._resolve_export_location_in_process(output, None)
    configured = postgres_export._resolve_export_location_in_process(
        output,
        tmp_path / "configured-spool",
    )

    assert default.location == postgres_export._ExportLocation(
        destination=output.resolve(),
        spool_root=output.resolve() / ".makolet-spool",
    )
    assert default.error_type is None
    assert configured.location == postgres_export._ExportLocation(
        destination=output.resolve(),
        spool_root=(tmp_path / "configured-spool").resolve(),
    )

    def fail_resolve(_path: Path, *, strict: bool = False) -> Path:
        del strict
        raise OSError("controlled resolution failure")

    monkeypatch.setattr(Path, "resolve", fail_resolve)
    failed = postgres_export._resolve_export_location_in_process(output, None)
    assert failed.location is None
    assert failed.error_type == "OSError"
    assert failed.error_message == "controlled resolution failure"


def test_spool_child_writes_exact_chunks_and_reports_input_limits(tmp_path: Path) -> None:
    spool = tmp_path / "spool" / "rows.jsonl"
    response = postgres_export._write_spool_in_process(iter((b"one", b"two")), spool, 6)
    assert response == postgres_export._SpoolProcessResponse(
        byte_length=6,
        error_type=None,
        error_message=None,
    )
    assert spool.read_bytes() == b"onetwo"
    if os.name != "nt":
        assert stat.S_IMODE(spool.stat().st_mode) == 0o600

    duplicate = postgres_export._write_spool_in_process(iter((b"new",)), spool, 6)
    assert duplicate.error_type == "FileExistsError"
    assert spool.read_bytes() == b"onetwo"

    non_bytes = postgres_export._write_spool_in_process(
        cast(Iterator[bytes], iter(("not-bytes",))),
        tmp_path / "non-bytes" / "rows.jsonl",
        20,
    )
    assert non_bytes.error_type == "TypeError"

    over_limit = postgres_export._write_spool_in_process(
        iter((b"1234", b"5")),
        tmp_path / "over-limit" / "rows.jsonl",
        4,
    )
    assert over_limit.byte_length == 4
    assert over_limit.error_type == "ExportValidationError"


def test_spool_child_closes_descriptors_when_open_or_close_wrapping_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_fdopen(*_args: object, **_kwargs: object) -> BinaryIO:
        raise OSError("controlled fdopen failure")

    with monkeypatch.context() as context:
        context.setattr(os, "fdopen", fail_fdopen)
        failed_open = postgres_export._write_spool_in_process(
            iter((b"unused",)),
            tmp_path / "fdopen" / "rows.jsonl",
            20,
        )
    assert failed_open.error_type == "OSError"
    assert failed_open.error_message == "controlled fdopen failure"

    def close_then_fail(handle: BinaryIO) -> None:
        handle.close()
        raise OSError("controlled close failure")

    with monkeypatch.context() as context:
        context.setattr(postgres_export, "_close_spool_file", close_then_fail)
        failed_close = postgres_export._write_spool_in_process(
            iter((b"written",)),
            tmp_path / "close" / "rows.jsonl",
            20,
        )
    assert failed_close.byte_length == 7
    assert failed_close.error_type == "OSError"
    assert failed_close.error_message == "controlled close failure"


class _ShortWriter:
    def write(self, value: bytes) -> int:
        return len(value) - 1


def test_spool_helpers_reject_non_directories_nonbytes_and_short_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = tmp_path / "spool" / "rows.jsonl"

    def symlink_status(_path: Path) -> os.stat_result:
        values = [0] * 10
        values[stat.ST_MODE] = stat.S_IFLNK | 0o777
        return os.stat_result(values)

    with monkeypatch.context() as context:
        context.setattr(Path, "lstat", symlink_status)
        with pytest.raises(ExportValidationError, match="real directory"):
            postgres_export._open_spool_file(spool)

    with pytest.raises(TypeError, match="non-byte"):
        postgres_export._charge_spool_chunk("text", 0, 20)
    with pytest.raises(ExportValidationError, match="max_spool_bytes"):
        postgres_export._charge_spool_chunk(b"too-large", 0, 2)
    assert postgres_export._charge_spool_chunk(b"exact", 2, 7) == 7

    with pytest.raises(OSError, match="incomplete"):
        postgres_export._write_spool_chunk(cast(BinaryIO, _ShortWriter()), b"chunk")


class _CapturingExporter:
    result: ExportResult
    restored_rows: list[Mapping[str, object]]

    def __init__(self, _output: Path, *, limits: ExportLimits) -> None:
        self.limits = limits

    def export(
        self,
        *,
        partition: ExportPartition,
        schema: ExportSchema,
        rows: Iterator[Mapping[str, object]],
        operation_budget: ExportOperationBudget,
        publication_recorder: Any,
        operation_identity: str,
        publication_journal: Path,
    ) -> ExportResult:
        del partition, schema, operation_identity, publication_journal
        type(self).restored_rows = list(rows)
        operation_budget.charge_file(7)
        publication_recorder(type(self).result)
        return type(self).result


class _FailingExporter:
    def __init__(self, _output: Path, *, limits: ExportLimits) -> None:
        del limits

    def export(self, **_kwargs: object) -> ExportResult:
        raise ExportValidationError("controlled child export failure")


def test_partition_child_restores_rows_records_publication_and_reports_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _schema()
    result = _result(tmp_path / "output")
    _CapturingExporter.result = result
    spool = tmp_path / "rows.jsonl"
    spool.write_text(
        json.dumps(
            {
                "amount": "12.30",
                "observed_at": "2026-08-17T10:00:00+00:00",
                "label": None,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    limits = ExportLimits()
    budget = ExportOperationBudget.start(limits)

    monkeypatch.setattr(postgres_export, "PartitionedParquetExporter", _CapturingExporter)
    response = postgres_export._export_partition_in_process(
        tmp_path / "output",
        limits,
        _partition(),
        schema,
        spool,
        budget,
        "operation",
        tmp_path / "publication.json",
    )

    assert response.result is result
    assert response.publication is result
    assert response.error_type is None
    assert response.consumed_files == 1
    assert response.consumed_output_bytes == 7
    assert _CapturingExporter.restored_rows == [
        {
            "amount": Decimal("12.30"),
            "observed_at": datetime(2026, 8, 17, 10, tzinfo=UTC),
            "label": None,
        }
    ]

    monkeypatch.setattr(postgres_export, "PartitionedParquetExporter", _FailingExporter)
    failed = postgres_export._export_partition_in_process(
        tmp_path / "output",
        limits,
        _partition(),
        schema,
        spool,
        ExportOperationBudget.start(limits),
        "operation",
        tmp_path / "publication.json",
    )
    assert failed.result is None
    assert failed.publication is None
    assert failed.error_type == "ExportValidationError"
    assert failed.error_message == "controlled child export failure"


def test_recovery_child_returns_publication_and_removes_only_regular_spools(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publication = _result(tmp_path / "output")

    def recover(*_args: object, **_kwargs: object) -> ExportResult:
        return publication

    monkeypatch.setattr(postgres_export, "recover_export_publication", recover)
    spool = tmp_path / "spool.jsonl"
    spool.write_bytes(b"row\n")
    response = postgres_export._recover_partition_in_process(
        tmp_path / "output",
        ExportLimits(),
        _partition(),
        _schema(),
        spool,
        "operation",
        tmp_path / "publication.json",
    )
    assert response == postgres_export._ExportRecoveryResponse(publication, None, None)
    assert not spool.exists()

    missing = tmp_path / "missing.jsonl"
    postgres_export._remove_spool_file(missing)
    assert not missing.exists()

    directory = tmp_path / "directory-spool"
    directory.mkdir()
    failed = postgres_export._recover_partition_in_process(
        tmp_path / "output",
        ExportLimits(),
        _partition(),
        _schema(),
        directory,
        "operation",
        tmp_path / "publication.json",
    )
    assert failed.publication is None
    assert failed.error_type == "ExportValidationError"
    assert directory.is_dir()


@pytest.mark.parametrize(
    ("value", "value_type", "message"),
    [
        (3, ExportType.DECIMAL_STRING, "decimal export spool value is not text"),
        ("invalid", ExportType.DECIMAL_STRING, "decimal export spool value is invalid"),
        (3, ExportType.TIMESTAMP_ISO8601, "timestamp export spool value is not text"),
        ("invalid", ExportType.TIMESTAMP_ISO8601, "timestamp export spool value is invalid"),
    ],
)
def test_spooled_value_rejects_malformed_typed_values(
    value: object,
    value_type: ExportType,
    message: str,
) -> None:
    with pytest.raises(ExportValidationError, match=message):
        postgres_export._restore_spooled_value(value, value_type)


def test_spooled_rows_reject_nonobjects_and_preserve_untyped_values(tmp_path: Path) -> None:
    invalid = tmp_path / "invalid.jsonl"
    invalid.write_text("[]\n", encoding="utf-8")
    with pytest.raises(TypeError, match="non-object"):
        list(postgres_export._spooled_rows(invalid, _schema()))

    assert postgres_export._restore_spooled_value(None, ExportType.STRING) is None
    assert postgres_export._restore_spooled_value("unchanged", None) == "unchanged"
    assert postgres_export._restore_spooled_value(7, ExportType.INT32) == 7
    with pytest.raises(TypeError, match="not serializable"):
        postgres_export._json_value(object())


def _process_response(
    *,
    error_type: str | None = None,
    error_message: str | None = "detail",
    consumed_files: int = 0,
    consumed_output_bytes: int = 0,
) -> postgres_export._ExportProcessResponse:
    return postgres_export._ExportProcessResponse(
        result=None,
        publication=None,
        error_type=error_type,
        error_message=error_message,
        consumed_files=consumed_files,
        consumed_output_bytes=consumed_output_bytes,
    )


@pytest.mark.parametrize(
    ("error_type", "expected_type"),
    [
        ("ExportConflictError", ExportConflictError),
        ("ExportValidationError", ExportValidationError),
        ("FileExistsError", OSError),
        ("OSError", OSError),
        ("TypeError", ExportValidationError),
        ("ValueError", ExportValidationError),
        ("UnexpectedError", ExportValidationError),
        (None, ExportValidationError),
    ],
)
def test_export_child_error_mapping_preserves_supported_public_types(
    error_type: str | None,
    expected_type: type[Exception],
) -> None:
    error = postgres_export._process_export_error(_process_response(error_type=error_type))
    assert isinstance(error, expected_type)
    assert "detail" in str(error)


@pytest.mark.parametrize(
    ("error_type", "expected_type"),
    [
        ("ExportValidationError", ExportValidationError),
        ("FileExistsError", OSError),
        ("OSError", OSError),
        ("TypeError", ExportValidationError),
        ("ValueError", ExportValidationError),
        ("UnexpectedError", ExportValidationError),
        (None, ExportValidationError),
    ],
)
def test_spool_child_error_mapping_preserves_supported_public_types(
    error_type: str | None,
    expected_type: type[Exception],
) -> None:
    response = postgres_export._SpoolProcessResponse(0, error_type, "detail")
    error = postgres_export._process_spool_error(response)
    assert isinstance(error, expected_type)
    assert "detail" in str(error)


@pytest.mark.parametrize(
    ("consumed_files", "consumed_output_bytes"),
    [(-1, 0), (1_001, 0), (0, -1), (0, 68_719_476_737)],
)
def test_process_budget_merge_rejects_impossible_child_accounting(
    consumed_files: int,
    consumed_output_bytes: int,
) -> None:
    budget = ExportOperationBudget.start(ExportLimits())
    with pytest.raises(ExportValidationError, match="budget accounting"):
        postgres_export._merge_process_budget(
            budget,
            _process_response(
                consumed_files=consumed_files,
                consumed_output_bytes=consumed_output_bytes,
            ),
        )


def test_process_budget_merge_accepts_monotonic_bounded_accounting() -> None:
    budget = ExportOperationBudget.start(ExportLimits())
    postgres_export._merge_process_budget(
        budget,
        _process_response(consumed_files=2, consumed_output_bytes=11),
    )
    assert budget.consumed_files == 2
    assert budget.consumed_output_bytes == 11


def test_preflight_and_range_validators_reject_inconsistent_inputs() -> None:
    with pytest.raises(ExportValidationError, match="partition bound"):
        postgres_export._validate_partition_count(1_001)
    with pytest.raises(ExportValidationError, match="do not match"):
        postgres_export._validate_partition_row_count_total(
            (postgres_export._PlannedPartition("retailer", date(2026, 8, 17), 2),),
            1,
        )
    with pytest.raises(ValueError, match="since must include a timezone"):
        postgres_export._validate_range(
            datetime(2026, 8, 17, tzinfo=UTC).replace(tzinfo=None),
            None,
        )
    with pytest.raises(ValueError, match="until must include a timezone"):
        postgres_export._validate_range(
            None,
            datetime(2026, 8, 17, tzinfo=UTC).replace(tzinfo=None),
        )
    with pytest.raises(ValueError, match="since must not be after until"):
        postgres_export._validate_range(
            datetime(2026, 8, 18, tzinfo=UTC),
            datetime(2026, 8, 17, tzinfo=UTC),
        )


def test_failure_translation_retains_publication_and_exception_causality(tmp_path: Path) -> None:
    manifest = tmp_path / "_manifest.json"
    failure = ExportValidationError("controlled failure")
    cause = RuntimeError("controlled cause")

    with pytest.raises(ExportPartialPublicationError) as partial:
        postgres_export._raise_export_failure(
            ({"manifest_path": manifest, "created": True},),
            failure,
            cause,
        )
    assert partial.value.manifests == (manifest,)
    assert partial.value.__cause__ is cause

    with pytest.raises(ExportValidationError) as direct:
        postgres_export._raise_export_failure((), failure, cause)
    assert direct.value is failure
    assert direct.value.__cause__ is cause


class _ScalarConnection:
    def __init__(self, value: object) -> None:
        self.value = value

    async def scalar(self, *_args: object, **_kwargs: object) -> object:
        return self.value


class _PartitionRows:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def all(self) -> list[object]:
        return self.rows


class _PartitionConnection:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    async def execute(self, *_args: object, **_kwargs: object) -> _PartitionRows:
        return _PartitionRows(self.rows)


def _operations() -> postgres_export.PostgresParquetExportOperations:
    return postgres_export.PostgresParquetExportOperations(cast(AsyncEngine, object()))


@pytest.mark.asyncio
async def test_async_preflight_helpers_reject_expiry_bad_counts_and_cleanup_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations = _operations()
    expired = ExportOperationBudget(ExportLimits(), work_deadline=0, cleanup_deadline=0)
    with pytest.raises(ExportValidationError, match="max_operation_seconds"):
        await operations._resolve_export_location(tmp_path, expired)

    async def failed_location(*_args: object, **_kwargs: object) -> object:
        return postgres_export._ExportLocationResponse(None, "OSError", "controlled")

    monkeypatch.setattr(postgres_export, "run_in_spawn_process", failed_location)
    with pytest.raises(ExportValidationError, match="location resolution failed"):
        await operations._resolve_export_location(
            tmp_path,
            ExportOperationBudget.start(ExportLimits()),
        )

    with pytest.raises(ExportValidationError, match="invalid row count"):
        await operations._row_count(
            cast(AsyncConnection, _ScalarConnection(True)),
            postgres_export._CURRENT_PRICES,
            since=None,
            until=None,
        )

    too_many: list[object] = [
        SimpleNamespace(
            retailer_id="retailer",
            partition_date=date(2026, 8, 17),
            row_count=1,
        )
        for _ in range(1_001)
    ]
    with pytest.raises(ValueError, match="partition bound"):
        await operations._partitions(
            cast(AsyncConnection, _PartitionConnection(too_many)),
            postgres_export._CURRENT_PRICES,
            since=None,
            until=None,
        )

    invalid: list[object] = [
        SimpleNamespace(retailer_id="retailer", partition_date="not-a-date", row_count=0)
    ]
    with pytest.raises(ExportValidationError, match="invalid partition count"):
        await operations._partitions(
            cast(AsyncConnection, _PartitionConnection(invalid)),
            postgres_export._CURRENT_PRICES,
            since=None,
            until=None,
        )

    with pytest.raises(ExportValidationError, match="cleanup exceeds"):
        await operations._recover_partition_publication(
            tmp_path,
            partition=_partition(),
            schema=_schema(),
            spool=tmp_path / "spool.jsonl",
            operation_identity="operation",
            publication_journal=tmp_path / "publication.json",
            operation_budget=expired,
        )

    async def failed_recovery(*_args: object, **_kwargs: object) -> object:
        return postgres_export._ExportRecoveryResponse(None, "OSError", "controlled")

    monkeypatch.setattr(postgres_export, "run_in_spawn_process", failed_recovery)
    with pytest.raises(ExportValidationError, match="cleanup failed"):
        await operations._recover_partition_publication(
            tmp_path,
            partition=_partition(),
            schema=_schema(),
            spool=tmp_path / "spool.jsonl",
            operation_identity="operation",
            publication_journal=tmp_path / "publication.json",
            operation_budget=ExportOperationBudget.start(ExportLimits()),
        )
