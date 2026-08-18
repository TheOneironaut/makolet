from __future__ import annotations

import hashlib
import json
import tracemalloc
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pytest

from makolet.adapters.export import (
    ExistingFilePolicy,
    ExportConflictError,
    ExportField,
    ExportLimits,
    ExportPartition,
    ExportResult,
    ExportSchema,
    ExportType,
    ExportValidationError,
    PartitionedParquetExporter,
)
from makolet.adapters.export import dataset as dataset_module
from makolet.adapters.export import parquet as parquet_module
from tests.unit.export.parquet_reader import rows


def _schema() -> ExportSchema:
    return ExportSchema(
        "prices",
        "1",
        (
            ExportField("item_code", ExportType.STRING, nullable=False),
            ExportField("price", ExportType.DECIMAL_STRING),
        ),
    )


def _partition() -> ExportPartition:
    return ExportPartition("prices", "רשת/א %", date(2026, 8, 11))


def _export_rows(count: int) -> list[dict[str, object]]:
    from decimal import Decimal

    return [
        {"item_code": f"item-{index:05d}", "price": Decimal(f"{index}.25")}
        for index in range(count)
    ]


def test_partitions_chunks_and_publishes_checksum_manifest(tmp_path: Path) -> None:
    exporter = PartitionedParquetExporter(
        tmp_path,
        limits=ExportLimits(max_rows_per_file=2, max_files=3, max_dataset_rows=6),
    )

    result = exporter.export(partition=_partition(), schema=_schema(), rows=_export_rows(5))

    assert result.created is True
    assert "entity=prices" in result.manifest_path.parts
    assert "retailer_id=%D7%A8%D7%A9%D7%AA%2F%D7%90%20%25" in result.manifest_path.parts
    assert "date=2026-08-11" in result.manifest_path.parts
    payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert payload == result.manifest.as_dict()
    assert payload["row_count"] == 5
    assert [file["row_count"] for file in payload["files"]] == [2, 2, 1]
    decoded_rows: list[dict[str, object | None]] = []
    for manifest_file in result.manifest.files:
        path = result.manifest_path.parent / manifest_file.path
        assert path.stat().st_size == manifest_file.byte_length
        assert hashlib.sha256(path.read_bytes()).hexdigest() == manifest_file.sha256
        decoded_rows.extend(rows(path))
    assert [row["item_code"] for row in decoded_rows] == [
        f"item-{index:05d}".encode() for index in range(5)
    ]
    assert not list(result.manifest_path.parent.glob(".stage-*"))


def test_partition_publication_is_idempotent_and_conflict_safe(tmp_path: Path) -> None:
    exporter = PartitionedParquetExporter(tmp_path)
    first = exporter.export(partition=_partition(), schema=_schema(), rows=_export_rows(2))
    manifest_before = first.manifest_path.read_bytes()

    repeated = exporter.export(partition=_partition(), schema=_schema(), rows=_export_rows(2))

    assert repeated.created is False
    assert repeated.manifest.dataset_id == first.manifest.dataset_id
    with pytest.raises(ExportConflictError, match="different dataset"):
        exporter.export(partition=_partition(), schema=_schema(), rows=_export_rows(3))
    assert first.manifest_path.read_bytes() == manifest_before
    assert not list(first.manifest_path.parent.glob(".stage-*"))


def test_replace_publishes_new_manifest_but_preserves_immutable_generation(
    tmp_path: Path,
) -> None:
    exporter = PartitionedParquetExporter(tmp_path)
    first = exporter.export(partition=_partition(), schema=_schema(), rows=_export_rows(1))
    first_generation = first.manifest_path.parent / f"dataset={first.manifest.dataset_id}"

    second = exporter.export(
        partition=_partition(),
        schema=_schema(),
        rows=_export_rows(3),
        policy=ExistingFilePolicy.REPLACE,
    )

    assert second.created is True
    assert second.manifest.dataset_id != first.manifest.dataset_id
    assert first_generation.is_dir()
    assert json.loads(second.manifest_path.read_text(encoding="utf-8"))["dataset_id"] == (
        second.manifest.dataset_id
    )


def test_idempotency_verifies_the_referenced_generation(tmp_path: Path) -> None:
    corrupt_exporter = PartitionedParquetExporter(tmp_path / "corrupt")
    corrupt = corrupt_exporter.export(
        partition=_partition(), schema=_schema(), rows=_export_rows(1)
    )
    corrupt_part = corrupt.manifest_path.parent / corrupt.manifest.files[0].path
    corrupt_part.write_bytes(b"not parquet")

    with pytest.raises(ExportConflictError, match="corrupt"):
        corrupt_exporter.export(partition=_partition(), schema=_schema(), rows=_export_rows(1))

    missing_exporter = PartitionedParquetExporter(tmp_path / "missing")
    missing = missing_exporter.export(
        partition=_partition(), schema=_schema(), rows=_export_rows(1)
    )
    generation = missing.manifest_path.parent / f"dataset={missing.manifest.dataset_id}"
    moved_generation = generation.with_name(f"unused-{missing.manifest.dataset_id}")
    generation.rename(moved_generation)

    with pytest.raises(ExportConflictError, match="missing dataset generation"):
        missing_exporter.export(partition=_partition(), schema=_schema(), rows=_export_rows(1))


def test_empty_partition_has_a_schema_bearing_empty_part(tmp_path: Path) -> None:
    result = PartitionedParquetExporter(tmp_path).export(
        partition=_partition(), schema=_schema(), rows=[]
    )

    assert result.manifest.row_count == 0
    assert len(result.manifest.files) == 1
    assert result.manifest.files[0].row_count == 0
    assert rows(result.manifest_path.parent / result.manifest.files[0].path) == []


def test_large_input_is_consumed_in_bounded_files(tmp_path: Path) -> None:
    exporter = PartitionedParquetExporter(
        tmp_path,
        limits=ExportLimits(
            max_rows_per_file=1_024,
            max_files=5,
            max_dataset_rows=5_000,
        ),
    )

    result = exporter.export(partition=_partition(), schema=_schema(), rows=_export_rows(4_097))

    assert [file.row_count for file in result.manifest.files] == [1_024] * 4 + [1]
    assert result.manifest.row_count == 4_097
    decoded_parts = [
        rows(result.manifest_path.parent / file.path) for file in result.manifest.files
    ]
    assert sum(len(part) for part in decoded_parts) == 4_097
    assert decoded_parts[0][0]["item_code"] == b"item-00000"
    assert decoded_parts[-1][-1]["item_code"] == b"item-04096"


def test_generator_rows_split_before_a_non_bmp_page_limit_is_exceeded(
    tmp_path: Path,
) -> None:
    schema = ExportSchema(
        "items",
        "1",
        (ExportField("value", ExportType.STRING, nullable=False),),
    )
    exporter = PartitionedParquetExporter(
        tmp_path,
        limits=ExportLimits(
            max_rows_per_file=10,
            max_files=3,
            max_dataset_rows=10,
            max_page_bytes=50,
            max_file_bytes=512,
            max_working_set_bytes=100_000,
        ),
    )

    result = exporter.export(
        partition=ExportPartition("items", "retailer", date(2026, 8, 16)),
        schema=schema,
        rows=({"value": "\U0001f600" * 8} for _ in range(3)),
    )

    assert [file.row_count for file in result.manifest.files] == [1, 1, 1]
    assert [
        rows(result.manifest_path.parent / file.path)[0]["value"] for file in result.manifest.files
    ] == [("\U0001f600" * 8).encode("utf-8")] * 3
    repeated = exporter.export(
        partition=ExportPartition("items", "retailer", date(2026, 8, 16)),
        schema=schema,
        rows=({"value": "\U0001f600" * 8} for _ in range(3)),
    )
    assert repeated.created is False
    assert repeated.manifest.dataset_id == result.manifest.dataset_id


def test_generator_rows_split_before_the_complete_file_limit_is_exceeded(
    tmp_path: Path,
) -> None:
    schema = ExportSchema(
        "items",
        "1",
        (
            ExportField("first", ExportType.STRING, nullable=False),
            ExportField("second", ExportType.STRING, nullable=False),
        ),
    )
    result = PartitionedParquetExporter(
        tmp_path,
        limits=ExportLimits(
            max_rows_per_file=10,
            max_files=2,
            max_dataset_rows=10,
            max_page_bytes=450,
            max_file_bytes=450,
            max_working_set_bytes=100_000,
        ),
    ).export(
        partition=ExportPartition("items", "retailer", date(2026, 8, 16)),
        schema=schema,
        rows=({"first": "abcdefgh", "second": "ijklmnop"} for _ in range(2)),
    )

    assert [file.row_count for file in result.manifest.files] == [1, 1]
    assert all(file.byte_length <= 450 for file in result.manifest.files)


def test_generator_rows_split_on_the_prospective_working_set_charge(
    tmp_path: Path,
) -> None:
    schema = ExportSchema(
        "items",
        "1",
        (ExportField("value", ExportType.STRING, nullable=False),),
    )
    limits = ExportLimits(
        max_rows_per_file=100,
        max_files=10,
        max_dataset_rows=100,
        max_page_bytes=1_000_000,
        max_file_bytes=1_000_000,
        max_working_set_bytes=1_000_000,
    )
    generated = 0

    def source() -> Iterator[dict[str, object]]:
        nonlocal generated
        for index in range(100):
            generated += 1
            yield {"value": chr(0x10000 + index) * 1_000}

    tracemalloc.start()
    try:
        result = PartitionedParquetExporter(tmp_path, limits=limits).export(
            partition=ExportPartition("items", "retailer", date(2026, 8, 16)),
            schema=schema,
            rows=source(),
        )
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert generated == 100
    assert 1 < len(result.manifest.files) <= limits.max_files
    assert sum(file.row_count for file in result.manifest.files) == 100
    assert peak_bytes <= limits.max_working_set_bytes


def test_dataset_rejects_one_oversized_row_before_plain_encoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = ExportSchema(
        "items",
        "1",
        (ExportField("value", ExportType.STRING, nullable=False),),
    )

    def fail_if_encoding_starts(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("oversized source row reached the PLAIN encoder")

    monkeypatch.setattr(parquet_module, "_encode_plain", fail_if_encoding_starts)

    with pytest.raises(ExportValidationError, match="max_page_bytes"):
        PartitionedParquetExporter(
            tmp_path,
            limits=ExportLimits(
                max_rows_per_file=10,
                max_dataset_rows=10,
                max_page_bytes=50,
                max_file_bytes=512,
                max_working_set_bytes=100_000,
            ),
        ).export(
            partition=ExportPartition("items", "retailer", date(2026, 8, 16)),
            schema=schema,
            rows=({"value": "\U0001f600" * 20} for _ in range(1)),
        )


def test_dataset_limits_and_entity_match_are_enforced(tmp_path: Path) -> None:
    file_limited = PartitionedParquetExporter(
        tmp_path / "files",
        limits=ExportLimits(max_rows_per_file=1, max_files=2, max_dataset_rows=3),
    )
    row_limited = PartitionedParquetExporter(
        tmp_path / "rows",
        limits=ExportLimits(max_rows_per_file=1, max_files=3, max_dataset_rows=2),
    )

    with pytest.raises(ExportValidationError, match="max_files"):
        file_limited.export(partition=_partition(), schema=_schema(), rows=_export_rows(3))
    with pytest.raises(ExportValidationError, match="max_dataset_rows"):
        row_limited.export(partition=_partition(), schema=_schema(), rows=_export_rows(3))
    wrong_schema = ExportSchema("stores", "1", (ExportField("id", ExportType.STRING),))
    with pytest.raises(ExportValidationError, match="must match"):
        PartitionedParquetExporter(tmp_path).export(
            partition=_partition(), schema=wrong_schema, rows=[]
        )


def test_failed_manifest_commit_removes_the_unpublished_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_commit(*_args: object, **_kwargs: object) -> bool:
        raise OSError("simulated manifest publication failure")

    monkeypatch.setattr(dataset_module, "_commit_manifest", fail_commit)

    with pytest.raises(OSError, match="publication failure"):
        PartitionedParquetExporter(tmp_path).export(
            partition=_partition(),
            schema=_schema(),
            rows=_export_rows(2),
        )

    assert not list(tmp_path.rglob("dataset=*"))
    assert not list(tmp_path.rglob(".stage-*"))
    assert not list(tmp_path.rglob("_manifest.json"))


def test_committed_manifest_survives_temporary_unlink_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_unlink = Path.unlink

    def fail_manifest_temporary_unlink(
        path: Path,
        missing_ok: bool = False,
    ) -> None:
        if path.name.startswith("._manifest.json.") and path.name.endswith(".tmp"):
            raise OSError("simulated post-publication temporary cleanup failure")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_manifest_temporary_unlink)

    publications: list[object] = []
    with pytest.raises(ExportValidationError, match="temporary cleanup failed"):
        PartitionedParquetExporter(tmp_path).export(
            partition=_partition(),
            schema=_schema(),
            rows=_export_rows(2),
            publication_recorder=publications.append,
        )

    assert len(publications) == 1
    result = publications[0]
    assert isinstance(result, ExportResult)
    assert result.created
    assert result.manifest_path.is_file()
    assert all(
        (result.manifest_path.parent / file.path).is_file() for file in result.manifest.files
    )


def test_recovery_removes_only_an_owned_unpublished_generation(tmp_path: Path) -> None:
    limits = ExportLimits()
    baseline = PartitionedParquetExporter(tmp_path / "baseline", limits=limits).export(
        partition=_partition(),
        schema=_schema(),
        rows=_export_rows(2),
    )
    exporter = PartitionedParquetExporter(tmp_path / "output", limits=limits)
    partition_directory = exporter._partition_directory(_partition())
    partition_directory.mkdir(parents=True)
    generation = partition_directory / f"dataset={baseline.manifest.dataset_id}"
    baseline_generation = baseline.manifest_path.parent / f"dataset={baseline.manifest.dataset_id}"
    baseline_generation.rename(generation)
    operation_identity = "a" * 32
    journal = tmp_path / "publication.json"
    dataset_module._write_publication_journal(
        journal,
        operation_identity=operation_identity,
        manifest=baseline.manifest,
        generation_preexisting=False,
        operation_budget=None,
    )

    with pytest.raises(ExportValidationError, match="cannot prove ownership"):
        dataset_module.recover_export_publication(
            tmp_path / "output",
            partition=_partition(),
            schema=_schema(),
            limits=limits,
            operation_identity=operation_identity,
            publication_journal=journal,
        )

    assert generation.is_dir()
    assert journal.is_file()
    dataset_module._write_generation_ownership_marker(generation, operation_identity)
    assert (
        dataset_module.recover_export_publication(
            tmp_path / "output",
            partition=_partition(),
            schema=_schema(),
            limits=limits,
            operation_identity=operation_identity,
            publication_journal=journal,
        )
        is None
    )
    assert not generation.exists()
    assert not journal.exists()
