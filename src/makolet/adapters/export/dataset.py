"""Safe generation and publication of partitioned Parquet datasets."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

from makolet.adapters.export.models import (
    ExistingFilePolicy,
    ExportConflictError,
    ExportLimits,
    ExportManifest,
    ExportOperationBudget,
    ExportPartition,
    ExportResult,
    ExportSchema,
    ExportValidationError,
    ManifestFile,
)
from makolet.adapters.export.parquet import (
    ParquetBatchBudget,
    PreparedParquetRow,
    write_prepared_parquet,
)

_MANIFEST_NAME = "_manifest.json"
_PUBLICATION_JOURNAL_FORMAT = "makolet-export-publication-intent-v1"
_MAXIMUM_PUBLICATION_JOURNAL_BYTES = 1_048_576
_OPERATION_IDENTITY_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_GENERATION_OWNERSHIP_MARKER = "._makolet-publication-owner"


@dataclass(frozen=True, slots=True)
class _ManifestCommit:
    created: bool
    cleanup_error: OSError | None = None


def _canonical_json(payload: Mapping[str, object]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode("utf-8")


def _partition_component(name: str, value: str) -> str:
    return f"{name}={quote(value, safe='-_.~')}"


def _remove_flat_staging_directory(
    staging: Path,
    partition_directory: Path,
    *,
    maximum_files: int,
    operation_budget: ExportOperationBudget | None,
) -> None:
    _remove_bounded_flat_directory(
        staging,
        partition_directory,
        required_prefix=".stage-",
        description="staging",
        maximum_files=maximum_files,
        operation_budget=operation_budget,
    )


def _remove_flat_generation_directory(
    generation: Path,
    partition_directory: Path,
    *,
    maximum_files: int,
    operation_budget: ExportOperationBudget | None,
) -> None:
    _remove_bounded_flat_directory(
        generation,
        partition_directory,
        required_prefix="dataset=",
        description="unpublished generation",
        maximum_files=maximum_files,
        operation_budget=operation_budget,
    )


def _remove_bounded_flat_directory(
    directory: Path,
    parent: Path,
    *,
    required_prefix: str,
    description: str,
    maximum_files: int,
    operation_budget: ExportOperationBudget | None,
) -> None:
    if directory.parent != parent or not directory.name.startswith(required_prefix):
        raise ExportValidationError(f"refusing to remove an unexpected {description} path")
    try:
        directory_status = directory.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(directory_status.st_mode) or stat.S_ISLNK(directory_status.st_mode):
        raise ExportValidationError(f"export {description} path must be a real directory")
    for index, child in enumerate(directory.iterdir(), start=1):
        if index > maximum_files:
            raise ExportValidationError(f"export {description} cleanup exceeds max_files")
        if operation_budget is not None:
            operation_budget.checkpoint(cleanup=True)
        child_status = child.lstat()
        if not stat.S_ISREG(child_status.st_mode) or stat.S_ISLNK(child_status.st_mode):
            raise ExportValidationError(f"export {description} may contain only files")
        child.unlink()
    directory.rmdir()


def _file_sha256(path: Path, operation_budget: ExportOperationBudget | None) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            if operation_budget is not None:
                operation_budget.checkpoint()
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_descriptor(
    *, partition: ExportPartition, schema: ExportSchema, files: Sequence[ManifestFile]
) -> dict[str, object]:
    schema_fields: list[dict[str, object]] = [
        {
            "name": field.name,
            "nullable": field.nullable,
            "type": field.value_type.value,
        }
        for field in schema.fields
    ]
    file_descriptors = [
        {
            "byte_length": file.byte_length,
            "name": Path(file.path).name,
            "row_count": file.row_count,
            "sha256": file.sha256,
        }
        for file in files
    ]
    return {
        "entity": partition.entity,
        "files": file_descriptors,
        "partition_date": partition.partition_date.isoformat(),
        "retailer_id": partition.retailer_id,
        "schema": schema_fields,
        "schema_version": schema.version,
    }


def _validate_existing_generation(
    generation: Path,
    staged_files: Sequence[ManifestFile],
    operation_budget: ExportOperationBudget | None,
    *,
    allowed_names: frozenset[str] = frozenset(),
) -> None:
    try:
        generation_status = generation.lstat()
    except FileNotFoundError as error:
        raise ExportConflictError(
            f"existing dataset generation is missing: {generation}"
        ) from error
    if not stat.S_ISDIR(generation_status.st_mode) or stat.S_ISLNK(generation_status.st_mode):
        raise ExportConflictError(
            f"existing dataset generation is not a real directory: {generation}"
        )
    expected_names = {Path(file.path).name for file in staged_files}
    actual_names = {path.name for path in generation.iterdir()}
    if actual_names != expected_names | allowed_names:
        raise ExportConflictError(f"existing dataset generation has unexpected files: {generation}")
    for file in staged_files:
        existing = generation / Path(file.path).name
        if (
            not existing.is_file()
            or existing.stat().st_size != file.byte_length
            or _file_sha256(existing, operation_budget) != file.sha256
        ):
            raise ExportConflictError(f"existing dataset generation is corrupt: {existing}")


def _commit_manifest(
    path: Path,
    content: bytes,
    policy: ExistingFilePolicy,
    operation_budget: ExportOperationBudget | None,
    *,
    operation_identity: str | None = None,
) -> _ManifestCommit:
    temporary_identity = operation_identity or uuid4().hex
    temporary = path.parent / f".{path.name}.{temporary_identity}.tmp"
    created: bool | None = None
    cleanup_error: OSError | None = None
    try:
        if operation_budget is not None:
            operation_budget.checkpoint()
        with temporary.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if policy is ExistingFilePolicy.REPLACE:
            temporary.replace(path)
            created = True
        else:
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() != content:
                    raise ExportConflictError(
                        f"partition already has a different manifest: {path}"
                    ) from None
                created = False
            else:
                created = True
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError as error:
            if created is None:
                raise
            cleanup_error = error
    if created is None:
        raise AssertionError("manifest commit returned without a publication result")
    return _ManifestCommit(created=created, cleanup_error=cleanup_error)


def _journal_temporary_path(path: Path, operation_identity: str) -> Path:
    return path.parent / f".{path.name}.{operation_identity}.tmp"


def _write_publication_journal(
    path: Path,
    *,
    operation_identity: str,
    manifest: ExportManifest,
    generation_preexisting: bool,
    operation_budget: ExportOperationBudget | None,
) -> None:
    if operation_budget is not None:
        operation_budget.checkpoint()
    content = _canonical_json(
        {
            "format": _PUBLICATION_JOURNAL_FORMAT,
            "generation_preexisting": generation_preexisting,
            "manifest": manifest.as_dict(),
            "operation_identity": operation_identity,
        }
    )
    if len(content) > _MAXIMUM_PUBLICATION_JOURNAL_BYTES:
        raise ExportValidationError("export publication journal exceeds its size limit")
    temporary = _journal_temporary_path(path, operation_identity)
    try:
        with temporary.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    if operation_budget is not None:
        operation_budget.checkpoint()


def _write_generation_ownership_marker(staging: Path, operation_identity: str) -> Path:
    marker = staging / _GENERATION_OWNERSHIP_MARKER
    with marker.open("xb") as handle:
        handle.write(f"{operation_identity}\n".encode("ascii"))
        handle.flush()
        os.fsync(handle.fileno())
    return marker


def _generation_ownership_identity(generation: Path) -> str | None:
    try:
        generation_status = generation.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISDIR(generation_status.st_mode) or stat.S_ISLNK(generation_status.st_mode):
        raise ExportValidationError("export generation path is not a real directory")
    marker = generation / _GENERATION_OWNERSHIP_MARKER
    payload = _read_bounded_regular_file(marker, 33)
    if payload is None:
        return None
    try:
        identity = payload.decode("ascii").removesuffix("\n")
    except UnicodeDecodeError as error:
        raise ExportValidationError("export generation ownership marker is invalid") from error
    if (
        payload != f"{identity}\n".encode("ascii")
        or _OPERATION_IDENTITY_PATTERN.fullmatch(identity) is None
    ):
        raise ExportValidationError("export generation ownership marker is invalid")
    return identity


def recover_export_publication(
    root: Path,
    *,
    partition: ExportPartition,
    schema: ExportSchema,
    limits: ExportLimits,
    operation_identity: str,
    publication_journal: Path,
) -> ExportResult | None:
    """Reconcile exact artifacts after terminating an export publisher process."""

    if _OPERATION_IDENTITY_PATTERN.fullmatch(operation_identity) is None:
        raise ExportValidationError("export recovery identity is invalid")
    exporter = PartitionedParquetExporter(root, limits=limits)
    partition_directory = exporter._partition_directory(partition)
    staging = partition_directory / f".stage-{operation_identity}"
    manifest_path = partition_directory / _MANIFEST_NAME
    manifest_temporary = partition_directory / f".{_MANIFEST_NAME}.{operation_identity}.tmp"
    journal_temporary = _journal_temporary_path(publication_journal, operation_identity)
    journal = _load_publication_journal(
        publication_journal,
        operation_identity=operation_identity,
        partition=partition,
        schema=schema,
        limits=limits,
    )
    publication: ExportResult | None = None
    generation: Path | None = None
    generation_preexisting = True
    if journal is not None:
        manifest, generation_preexisting = journal
        generation = partition_directory / f"dataset={manifest.dataset_id}"
        expected_content = _canonical_json(manifest.as_dict())
        if _regular_file_has_exact_content(manifest_path, expected_content):
            ownership_identity = _generation_ownership_identity(generation)
            allowed_names = (
                frozenset({_GENERATION_OWNERSHIP_MARKER})
                if ownership_identity is not None
                else frozenset()
            )
            _validate_existing_generation(
                generation,
                manifest.files,
                None,
                allowed_names=allowed_names,
            )
            if ownership_identity == operation_identity:
                _remove_known_file(
                    generation / _GENERATION_OWNERSHIP_MARKER,
                    "generation ownership marker",
                )
            publication = ExportResult(
                manifest_path=manifest_path,
                manifest=manifest,
                created=True,
            )
        elif not generation_preexisting:
            ownership_identity = _generation_ownership_identity(generation)
            if ownership_identity is None:
                try:
                    generation.lstat()
                except FileNotFoundError:
                    pass
                else:
                    raise ExportValidationError(
                        "export cannot prove ownership of an unpublished generation"
                    )
            elif ownership_identity != operation_identity:
                raise ExportValidationError(
                    "export cannot prove ownership of an unpublished generation"
                )
            _remove_flat_generation_directory(
                generation,
                partition_directory,
                maximum_files=limits.max_files + 1,
                operation_budget=None,
            )
    _remove_flat_staging_directory(
        staging,
        partition_directory,
        maximum_files=limits.max_files + 1,
        operation_budget=None,
    )
    _remove_known_file(manifest_temporary, "manifest temporary")
    _remove_known_file(journal_temporary, "publication journal temporary")
    _remove_known_file(publication_journal, "publication journal")
    return publication


def _load_publication_journal(
    path: Path,
    *,
    operation_identity: str,
    partition: ExportPartition,
    schema: ExportSchema,
    limits: ExportLimits,
) -> tuple[ExportManifest, bool] | None:
    payload = _read_bounded_regular_file(path, _MAXIMUM_PUBLICATION_JOURNAL_BYTES)
    if payload is None:
        return None
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ExportValidationError("export publication journal is invalid") from error
    if (
        not isinstance(value, dict)
        or value.get("format") != _PUBLICATION_JOURNAL_FORMAT
        or value.get("operation_identity") != operation_identity
        or type(value.get("generation_preexisting")) is not bool
    ):
        raise ExportValidationError("export publication journal is invalid")
    manifest = _manifest_from_dict(value.get("manifest"), partition, schema, limits)
    return manifest, value["generation_preexisting"]


def _manifest_from_dict(
    value: object,
    partition: ExportPartition,
    schema: ExportSchema,
    limits: ExportLimits,
) -> ExportManifest:
    if not isinstance(value, dict):
        raise ExportValidationError("export publication journal manifest is invalid")
    dataset_id = value.get("dataset_id")
    row_count = value.get("row_count")
    raw_files = value.get("files")
    expected_schema = [
        {"name": field.name, "nullable": field.nullable, "type": field.value_type.value}
        for field in schema.fields
    ]
    if (
        not isinstance(dataset_id, str)
        or len(dataset_id) != 64
        or any(character not in "0123456789abcdef" for character in dataset_id)
        or type(row_count) is not int
        or row_count < 0
        or row_count > limits.max_dataset_rows
        or not isinstance(raw_files, list)
        or len(raw_files) > limits.max_files
        or value.get("format") != "makolet-partition-manifest"
        or value.get("manifest_version") != 1
        or value.get("entity") != partition.entity
        or value.get("retailer_id") != partition.retailer_id
        or value.get("partition_date") != partition.partition_date.isoformat()
        or value.get("schema_version") != schema.version
        or value.get("schema") != expected_schema
    ):
        raise ExportValidationError("export publication journal manifest is invalid")
    generation_name = f"dataset={dataset_id}"
    files: list[ManifestFile] = []
    for index, raw_file in enumerate(raw_files):
        if not isinstance(raw_file, dict):
            raise ExportValidationError("export publication journal file is invalid")
        path = raw_file.get("path")
        digest = raw_file.get("sha256")
        byte_length = raw_file.get("byte_length")
        file_rows = raw_file.get("row_count")
        if (
            path != f"{generation_name}/part-{index:05d}.parquet"
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or type(byte_length) is not int
            or byte_length < 0
            or byte_length > limits.max_file_bytes
            or type(file_rows) is not int
            or file_rows < 0
            or file_rows > limits.max_rows_per_file
        ):
            raise ExportValidationError("export publication journal file is invalid")
        files.append(
            ManifestFile(
                path=path,
                sha256=digest,
                byte_length=byte_length,
                row_count=file_rows,
            )
        )
    if not files or sum(file.row_count for file in files) != row_count:
        raise ExportValidationError("export publication journal row accounting is invalid")
    manifest = ExportManifest(
        dataset_id=dataset_id,
        partition=partition,
        schema=schema,
        row_count=row_count,
        files=tuple(files),
    )
    descriptor = _manifest_descriptor(partition=partition, schema=schema, files=manifest.files)
    if hashlib.sha256(_canonical_json(descriptor)).hexdigest() != dataset_id:
        raise ExportValidationError("export publication journal dataset identity is invalid")
    if manifest.as_dict() != value:
        raise ExportValidationError("export publication journal manifest is not canonical")
    return manifest


def _read_bounded_regular_file(path: Path, maximum_bytes: int) -> bytes | None:
    try:
        before = path.lstat()
    except FileNotFoundError:
        return None
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or before.st_size > maximum_bytes
    ):
        raise ExportValidationError("export control file is not a bounded regular file")
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        after = os.fstat(descriptor)
        if (
            not stat.S_ISREG(after.st_mode)
            or before.st_dev != after.st_dev
            or before.st_ino != after.st_ino
            or after.st_size > maximum_bytes
        ):
            raise ExportValidationError("export control file changed while it was opened")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            payload = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(payload) > maximum_bytes or len(payload) != after.st_size:
        raise ExportValidationError("export control file exceeds its size limit")
    return payload


def _regular_file_has_exact_content(path: Path, expected: bytes) -> bool:
    payload = _read_bounded_regular_file(path, len(expected))
    return payload == expected if payload is not None else False


def _remove_known_file(path: Path, description: str) -> None:
    try:
        status = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(status.st_mode) or stat.S_ISLNK(status.st_mode):
        raise ExportValidationError(f"export {description} is not a regular file")
    path.unlink()


class PartitionedParquetExporter:
    """Publish immutable generations beneath Hive-style partition directories."""

    def __init__(self, root: Path, *, limits: ExportLimits | None = None) -> None:
        self._root = root.resolve(strict=False)
        self._limits = limits or ExportLimits()

    def _partition_directory(self, partition: ExportPartition) -> Path:
        candidate = (
            self._root
            / _partition_component("entity", partition.entity)
            / _partition_component("retailer_id", partition.retailer_id)
            / _partition_component("date", partition.partition_date.isoformat())
        )
        resolved = candidate.resolve(strict=False)
        if not resolved.is_relative_to(self._root):
            raise ExportValidationError("partition path escaped the export root")
        return resolved

    def export(
        self,
        *,
        partition: ExportPartition,
        schema: ExportSchema,
        rows: Iterable[Mapping[str, object]],
        policy: ExistingFilePolicy = ExistingFilePolicy.REQUIRE_IDENTICAL,
        operation_budget: ExportOperationBudget | None = None,
        publication_recorder: Callable[[ExportResult], None] | None = None,
        operation_identity: str | None = None,
        publication_journal: Path | None = None,
    ) -> ExportResult:
        """Write bounded part files and atomically publish their canonical manifest."""

        if partition.entity != schema.entity:
            raise ExportValidationError("partition entity must match the export schema")
        if len(schema.fields) > self._limits.max_columns:
            raise ExportValidationError("schema exceeds max_columns")
        if (operation_identity is None) != (publication_journal is None):
            raise ExportValidationError(
                "operation_identity and publication_journal must be configured together"
            )
        if (
            operation_identity is not None
            and _OPERATION_IDENTITY_PATTERN.fullmatch(operation_identity) is None
        ):
            raise ExportValidationError("export operation identity is invalid")
        if operation_budget is not None:
            operation_budget.checkpoint()

        partition_directory = self._partition_directory(partition)
        partition_directory.mkdir(parents=True, exist_ok=True)
        staging = partition_directory / f".stage-{operation_identity or uuid4().hex}"
        staging.mkdir()
        staged_files: list[ManifestFile] = []
        chunk: list[PreparedParquetRow] = []
        budget = ParquetBatchBudget(schema, self._limits)
        row_count = 0
        created_generation: Path | None = None
        generation_ownership_marker: Path | None = None
        manifest_committed = False
        try:
            for row in rows:
                if operation_budget is not None:
                    operation_budget.checkpoint()
                row_count += 1
                if row_count > self._limits.max_dataset_rows:
                    raise ExportValidationError("dataset exceeds max_dataset_rows")
                prepared = budget.prepare_row(row, row_index=row_count - 1)
                rejection = budget.add_if_within_limits(prepared)
                if rejection is not None and chunk:
                    self._write_chunk(
                        staging,
                        chunk,
                        schema,
                        staged_files,
                        operation_budget,
                    )
                    chunk = []
                    budget = ParquetBatchBudget(schema, self._limits)
                    rejection = budget.add_if_within_limits(prepared)
                if rejection is not None:
                    raise ExportValidationError(rejection)
                chunk.append(prepared)
            if chunk or not staged_files:
                self._write_chunk(
                    staging,
                    chunk,
                    schema,
                    staged_files,
                    operation_budget,
                )

            descriptor = _manifest_descriptor(
                partition=partition, schema=schema, files=staged_files
            )
            dataset_id = hashlib.sha256(_canonical_json(descriptor)).hexdigest()
            generation_name = f"dataset={dataset_id}"
            manifest_files = tuple(
                ManifestFile(
                    path=f"{generation_name}/{Path(file.path).name}",
                    sha256=file.sha256,
                    byte_length=file.byte_length,
                    row_count=file.row_count,
                )
                for file in staged_files
            )
            manifest = ExportManifest(
                dataset_id=dataset_id,
                partition=partition,
                schema=schema,
                row_count=row_count,
                files=manifest_files,
            )
            manifest_content = _canonical_json(manifest.as_dict())
            manifest_path = partition_directory / _MANIFEST_NAME
            generation = partition_directory / generation_name

            if manifest_path.exists():
                current = manifest_path.read_bytes()
                if current == manifest_content:
                    if not generation.is_dir():
                        raise ExportConflictError(
                            f"manifest references a missing dataset generation: {generation}"
                        )
                    _validate_existing_generation(generation, staged_files, operation_budget)
                    _remove_flat_staging_directory(
                        staging,
                        partition_directory,
                        maximum_files=self._limits.max_files,
                        operation_budget=operation_budget,
                    )
                    result = ExportResult(
                        manifest_path=manifest_path, manifest=manifest, created=False
                    )
                    if publication_recorder is not None:
                        publication_recorder(result)
                    return result
                if policy is ExistingFilePolicy.REQUIRE_IDENTICAL:
                    raise ExportConflictError(
                        f"partition already contains a different dataset: {manifest_path}"
                    )
            generation_preexisting = generation.exists()
            if generation_preexisting:
                if not generation.is_dir():
                    raise ExportConflictError(
                        f"dataset generation path is not a directory: {generation}"
                    )
                _validate_existing_generation(generation, staged_files, operation_budget)
            if publication_journal is not None and operation_identity is not None:
                _write_publication_journal(
                    publication_journal,
                    operation_identity=operation_identity,
                    manifest=manifest,
                    generation_preexisting=generation_preexisting,
                    operation_budget=operation_budget,
                )
                if not generation_preexisting:
                    generation_ownership_marker = _write_generation_ownership_marker(
                        staging,
                        operation_identity,
                    )
            if generation_preexisting:
                _remove_flat_staging_directory(
                    staging,
                    partition_directory,
                    maximum_files=self._limits.max_files,
                    operation_budget=operation_budget,
                )
            else:
                staging.rename(generation)
                created_generation = generation

            commit = _commit_manifest(
                manifest_path,
                manifest_content,
                policy,
                operation_budget,
                operation_identity=operation_identity,
            )
            manifest_committed = True
            if generation_ownership_marker is not None:
                _remove_known_file(
                    generation / _GENERATION_OWNERSHIP_MARKER,
                    "generation ownership marker",
                )
                generation_ownership_marker = None
            result = ExportResult(
                manifest_path=manifest_path,
                manifest=manifest,
                created=commit.created,
            )
            if publication_recorder is not None:
                publication_recorder(result)
            if commit.cleanup_error is not None:
                raise ExportValidationError(
                    "manifest published but temporary cleanup failed"
                ) from commit.cleanup_error
            return result
        finally:
            if created_generation is not None and not manifest_committed:
                _remove_flat_generation_directory(
                    created_generation,
                    partition_directory,
                    maximum_files=self._limits.max_files + 1,
                    operation_budget=operation_budget,
                )
            if staging.exists():
                _remove_flat_staging_directory(
                    staging,
                    partition_directory,
                    maximum_files=self._limits.max_files + 1,
                    operation_budget=operation_budget,
                )

    def _write_chunk(
        self,
        staging: Path,
        rows: Sequence[PreparedParquetRow],
        schema: ExportSchema,
        staged_files: list[ManifestFile],
        operation_budget: ExportOperationBudget | None,
    ) -> None:
        if len(staged_files) >= self._limits.max_files:
            raise ExportValidationError("dataset exceeds max_files")
        name = f"part-{len(staged_files):05d}.parquet"
        result = write_prepared_parquet(
            staging / name,
            rows,
            schema,
            limits=self._limits,
        )
        if operation_budget is not None:
            operation_budget.charge_file(result.byte_length)
        staged_files.append(
            ManifestFile(
                path=name,
                sha256=result.sha256,
                byte_length=result.byte_length,
                row_count=result.row_count,
            )
        )
