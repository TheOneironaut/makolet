"""Explicit values and limits for analytical exports."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path

_IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SCHEMA_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_MINIMUM_CLEANUP_RESERVE_SECONDS = 4.0


class ExportValidationError(ValueError):
    """Raised when an export cannot be represented by the supported subset."""


class ExportConflictError(FileExistsError):
    """Raised when immutable export output conflicts with existing content."""


class ExportPartialPublicationError(ExportValidationError):
    """An operation stopped after publishing one or more immutable partitions."""

    def __init__(self, manifests: tuple[Path, ...], cause: Exception) -> None:
        self.manifests = manifests
        super().__init__(
            "export stopped after publishing "
            f"{len(manifests)} immutable partition manifest(s); "
            "published generations were retained and in-progress staging cleanup was bounded"
        )
        self.original_error = cause


class ExportType(StrEnum):
    """Logical field types supported by the deliberately small writer."""

    STRING = "string"
    BINARY = "binary"
    INT32 = "int32"
    INT64 = "int64"
    BOOLEAN = "boolean"
    DECIMAL_STRING = "decimal_string"
    TIMESTAMP_ISO8601 = "timestamp_iso8601"


class ExistingFilePolicy(StrEnum):
    """How a writer handles an existing path with different bytes."""

    REQUIRE_IDENTICAL = "require_identical"
    REPLACE = "replace"


@dataclass(frozen=True, slots=True)
class ExportField:
    name: str
    value_type: ExportType
    nullable: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.value_type, ExportType) or type(self.nullable) is not bool:
            raise ExportValidationError("field type and nullability must be explicit")
        encoded_name = self.name.encode("utf-8")
        if not self.name or "\x00" in self.name or len(encoded_name) > 255:
            raise ExportValidationError("field names must be non-empty UTF-8 names up to 255 bytes")


@dataclass(frozen=True, slots=True)
class ExportSchema:
    """Versioned flat schema used both in Parquet metadata and the manifest."""

    entity: str
    version: str
    fields: tuple[ExportField, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.fields, tuple) or not all(
            isinstance(field, ExportField) for field in self.fields
        ):
            raise ExportValidationError("schema fields must be an immutable tuple of ExportField")
        if _IDENTIFIER_PATTERN.fullmatch(self.entity) is None:
            raise ExportValidationError(
                "entity must match [a-z][a-z0-9_]{0,63} for stable partition paths"
            )
        if _SCHEMA_VERSION_PATTERN.fullmatch(self.version) is None:
            raise ExportValidationError("schema version must be a short stable identifier")
        if not self.fields:
            raise ExportValidationError("an export schema must contain at least one field")
        names = tuple(field.name for field in self.fields)
        if len(names) != len(set(names)):
            raise ExportValidationError("export field names must be unique")


@dataclass(frozen=True, slots=True)
class ExportPartition:
    entity: str
    retailer_id: str
    partition_date: date

    def __post_init__(self) -> None:
        if type(self.partition_date) is not date:
            raise ExportValidationError("partition_date must be a date without a time component")
        if _IDENTIFIER_PATTERN.fullmatch(self.entity) is None:
            raise ExportValidationError("partition entity is not a stable identifier")
        encoded_retailer = self.retailer_id.encode("utf-8")
        if not self.retailer_id or "\x00" in self.retailer_id or len(encoded_retailer) > 255:
            raise ExportValidationError(
                "retailer_id must be non-empty UTF-8 text up to 255 bytes without NUL"
            )


@dataclass(frozen=True, slots=True)
class ExportLimits:
    """Hard bounds that prevent one export from consuming unbounded resources."""

    max_columns: int = 128
    max_rows_per_file: int = 50_000
    max_files: int = 1_000
    max_dataset_rows: int = 10_000_000
    max_string_bytes: int = 1_048_576
    max_binary_bytes: int = 16_777_216
    max_page_bytes: int = 67_108_864
    max_file_bytes: int = 67_108_864
    max_working_set_bytes: int = 268_435_456
    max_spool_bytes: int = 2_147_483_648
    max_output_bytes: int = 68_719_476_736
    max_operation_seconds: float = 3_600.0

    def __post_init__(self) -> None:
        values = (
            self.max_columns,
            self.max_rows_per_file,
            self.max_files,
            self.max_dataset_rows,
            self.max_string_bytes,
            self.max_binary_bytes,
            self.max_page_bytes,
            self.max_file_bytes,
            self.max_working_set_bytes,
            self.max_spool_bytes,
            self.max_output_bytes,
        )
        if any(type(value) is not int or value <= 0 for value in values):
            raise ExportValidationError("all export limits must be positive")
        if self.max_dataset_rows < self.max_rows_per_file:
            raise ExportValidationError("max_dataset_rows cannot be smaller than max_rows_per_file")
        if (
            self.max_rows_per_file >= 2**31
            or self.max_page_bytes >= 2**31
            or self.max_file_bytes >= 2**31
        ):
            raise ExportValidationError(
                "row, page, and file limits must fit signed Parquet i32 fields"
            )
        if self.max_string_bytes >= 2**32 or self.max_binary_bytes >= 2**32:
            raise ExportValidationError("byte-array limits must fit PLAIN uint32 lengths")
        if self.max_page_bytes > self.max_file_bytes:
            raise ExportValidationError("max_page_bytes cannot exceed max_file_bytes")
        if self.max_file_bytes > self.max_working_set_bytes:
            raise ExportValidationError("max_file_bytes cannot exceed max_working_set_bytes")
        if self.max_spool_bytes >= 2**63 or self.max_output_bytes >= 2**63:
            raise ExportValidationError(
                "spool and output byte limits must fit signed 64-bit counters"
            )
        if (
            isinstance(self.max_operation_seconds, bool)
            or not isinstance(self.max_operation_seconds, int | float)
            or not 0 < self.max_operation_seconds <= 86_400
        ):
            raise ExportValidationError("max_operation_seconds must be positive and at most 86400")


@dataclass(slots=True)
class ExportOperationBudget:
    """One prospective budget shared by every partition in one invocation."""

    limits: ExportLimits
    work_deadline: float
    cleanup_deadline: float
    planned_rows: int | None = None
    consumed_rows: int = 0
    consumed_files: int = 0
    consumed_spool_bytes: int = 0
    consumed_output_bytes: int = 0

    @classmethod
    def start(cls, limits: ExportLimits) -> ExportOperationBudget:
        started_at = time.monotonic()
        cleanup_reserve = min(
            30.0,
            max(_MINIMUM_CLEANUP_RESERVE_SECONDS, limits.max_operation_seconds * 0.10),
            limits.max_operation_seconds / 2,
        )
        return cls(
            limits=limits,
            work_deadline=started_at + limits.max_operation_seconds - cleanup_reserve,
            cleanup_deadline=started_at + limits.max_operation_seconds,
        )

    def remaining_seconds(self, *, cleanup: bool = False) -> float:
        deadline = self.cleanup_deadline if cleanup else self.work_deadline
        return max(0.0, deadline - time.monotonic())

    def checkpoint(self, *, cleanup: bool = False) -> None:
        if self.remaining_seconds(cleanup=cleanup) <= 0:
            phase = "cleanup" if cleanup else "operation"
            raise ExportValidationError(f"export {phase} exceeds max_operation_seconds")

    def preflight(self, *, partition_row_counts: tuple[int, ...], row_count: int) -> None:
        self.checkpoint()
        if (
            type(row_count) is not int
            or row_count < 0
            or any(
                type(partition_rows) is not int or partition_rows < 0
                for partition_rows in partition_row_counts
            )
        ):
            raise ExportValidationError("database export preflight returned invalid counts")
        if sum(partition_row_counts) != row_count:
            raise ExportValidationError(
                "database export preflight partition counts do not match its row count"
            )
        if row_count > self.limits.max_dataset_rows:
            raise ExportValidationError("database export exceeds max_dataset_rows")
        minimum_files = sum(
            (partition_rows + self.limits.max_rows_per_file - 1) // self.limits.max_rows_per_file
            for partition_rows in partition_row_counts
        )
        if minimum_files > self.limits.max_files:
            raise ExportValidationError("database export exceeds max_files")
        self.planned_rows = row_count

    def charge_row(self, spool_bytes: int) -> None:
        self.checkpoint()
        if spool_bytes < 0:
            raise ExportValidationError("database export spool accounting is invalid")
        prospective_rows = self.consumed_rows + 1
        prospective_spool = self.consumed_spool_bytes + spool_bytes
        if prospective_rows > self.limits.max_dataset_rows:
            raise ExportValidationError("database export exceeds max_dataset_rows")
        if self.planned_rows is not None and prospective_rows > self.planned_rows:
            raise ExportValidationError(
                "database export changed within its repeatable-read snapshot"
            )
        if prospective_spool > self.limits.max_spool_bytes:
            raise ExportValidationError("database export exceeds max_spool_bytes")
        self.consumed_rows = prospective_rows
        self.consumed_spool_bytes = prospective_spool

    def charge_file(self, output_bytes: int) -> None:
        self.checkpoint()
        if output_bytes < 0:
            raise ExportValidationError("database export output accounting is invalid")
        prospective_files = self.consumed_files + 1
        prospective_output = self.consumed_output_bytes + output_bytes
        if prospective_files > self.limits.max_files:
            raise ExportValidationError("database export exceeds max_files")
        if prospective_output > self.limits.max_output_bytes:
            raise ExportValidationError("database export exceeds max_output_bytes")
        self.consumed_files = prospective_files
        self.consumed_output_bytes = prospective_output

    def finish(self) -> None:
        self.checkpoint()
        if self.planned_rows is not None and self.consumed_rows != self.planned_rows:
            raise ExportValidationError(
                "database export changed within its repeatable-read snapshot"
            )


@dataclass(frozen=True, slots=True)
class ParquetWriteResult:
    path: Path
    sha256: str
    byte_length: int
    row_count: int
    created: bool


@dataclass(frozen=True, slots=True)
class ManifestFile:
    path: str
    sha256: str
    byte_length: int
    row_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "byte_length": self.byte_length,
            "path": self.path,
            "row_count": self.row_count,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class ExportManifest:
    """Canonical manifest for one entity/retailer/date partition generation."""

    dataset_id: str
    partition: ExportPartition
    schema: ExportSchema
    row_count: int
    files: tuple[ManifestFile, ...]

    def as_dict(self) -> dict[str, object]:
        schema_fields: list[dict[str, object]] = [
            {
                "name": field.name,
                "nullable": field.nullable,
                "type": field.value_type.value,
            }
            for field in self.schema.fields
        ]
        return {
            "dataset_id": self.dataset_id,
            "entity": self.partition.entity,
            "files": [file.as_dict() for file in self.files],
            "format": "makolet-partition-manifest",
            "manifest_version": 1,
            "partition_date": self.partition.partition_date.isoformat(),
            "retailer_id": self.partition.retailer_id,
            "row_count": self.row_count,
            "schema": schema_fields,
            "schema_version": self.schema.version,
        }


@dataclass(frozen=True, slots=True)
class ExportResult:
    manifest_path: Path
    manifest: ExportManifest
    created: bool
