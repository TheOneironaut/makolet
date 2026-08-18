"""A small Apache Parquet writer for Makolet's explicit flat export schemas.

The implementation intentionally supports only format version 1, Data Page V1,
uncompressed PLAIN values, RLE definition levels, and flat primitive columns. It is
original code derived from the Apache Parquet format specification, not from another
writer implementation.
"""

from __future__ import annotations

import hashlib
import json
import os
import struct
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import IntEnum
from pathlib import Path
from uuid import uuid4

from makolet.adapters.export.models import (
    ExistingFilePolicy,
    ExportConflictError,
    ExportLimits,
    ExportSchema,
    ExportType,
    ExportValidationError,
    ParquetWriteResult,
)

_PARQUET_MAGIC = b"PAR1"
_CREATED_BY = "makolet version 0.1.0 (dependency-free parquet writer)"
_ENCODER_OUTPUT_COPY_FACTOR = 5
_BATCH_FIXED_OVERHEAD_BYTES = 65_536
_POINTER_BYTES = struct.calcsize("P")
_LIST_BASE_BYTES = sys.getsizeof([])


class _CompactType(IntEnum):
    STOP = 0
    BOOLEAN_TRUE = 1
    BOOLEAN_FALSE = 2
    BYTE = 3
    I16 = 4
    I32 = 5
    I64 = 6
    DOUBLE = 7
    BINARY = 8
    LIST = 9
    STRUCT = 12


class _PhysicalType(IntEnum):
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    BYTE_ARRAY = 6


class _Encoding(IntEnum):
    PLAIN = 0
    RLE = 3


class _CompactStruct:
    """Minimal TCompactProtocol serializer for the metadata structs we emit."""

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._last_field_id = 0

    def _field_header(self, field_id: int, field_type: _CompactType) -> None:
        delta = field_id - self._last_field_id
        if 1 <= delta <= 15:
            self._buffer.append((delta << 4) | field_type)
        else:
            self._buffer.append(field_type)
            self._buffer.extend(_signed_varint(field_id))
        self._last_field_id = field_id

    def i16(self, field_id: int, value: int) -> None:
        self._field_header(field_id, _CompactType.I16)
        self._buffer.extend(_signed_varint(value))

    def i32(self, field_id: int, value: int) -> None:
        self._field_header(field_id, _CompactType.I32)
        self._buffer.extend(_signed_varint(value))

    def i64(self, field_id: int, value: int) -> None:
        self._field_header(field_id, _CompactType.I64)
        self._buffer.extend(_signed_varint(value))

    def binary(self, field_id: int, value: bytes) -> None:
        self._field_header(field_id, _CompactType.BINARY)
        self._buffer.extend(_unsigned_varint(len(value)))
        self._buffer.extend(value)

    def string(self, field_id: int, value: str) -> None:
        self.binary(field_id, value.encode("utf-8"))

    def struct(self, field_id: int, value: bytes) -> None:
        self._field_header(field_id, _CompactType.STRUCT)
        self._buffer.extend(value)

    def list_i32(self, field_id: int, values: Sequence[int]) -> None:
        self._field_header(field_id, _CompactType.LIST)
        self._list_header(_CompactType.I32, len(values))
        for value in values:
            self._buffer.extend(_signed_varint(value))

    def list_binary(self, field_id: int, values: Sequence[bytes]) -> None:
        self._field_header(field_id, _CompactType.LIST)
        self._list_header(_CompactType.BINARY, len(values))
        for value in values:
            self._buffer.extend(_unsigned_varint(len(value)))
            self._buffer.extend(value)

    def list_struct(self, field_id: int, values: Sequence[bytes]) -> None:
        self._field_header(field_id, _CompactType.LIST)
        self._list_header(_CompactType.STRUCT, len(values))
        for value in values:
            self._buffer.extend(value)

    def _list_header(self, element_type: _CompactType, size: int) -> None:
        if size <= 14:
            self._buffer.append((size << 4) | element_type)
            return
        self._buffer.append(0xF0 | element_type)
        self._buffer.extend(_unsigned_varint(size))

    def finish(self) -> bytes:
        self._buffer.append(_CompactType.STOP)
        return bytes(self._buffer)


def _unsigned_varint(value: int) -> bytes:
    if value < 0:
        raise ExportValidationError("unsigned varints cannot encode negative values")
    encoded = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value == 0:
            encoded.append(byte)
            return bytes(encoded)
        encoded.append(byte | 0x80)


def _signed_varint(value: int) -> bytes:
    return _unsigned_varint((value << 1) ^ (value >> 63))


def _physical_type(value_type: ExportType) -> _PhysicalType:
    if value_type is ExportType.BOOLEAN:
        return _PhysicalType.BOOLEAN
    if value_type is ExportType.INT32:
        return _PhysicalType.INT32
    if value_type is ExportType.INT64:
        return _PhysicalType.INT64
    return _PhysicalType.BYTE_ARRAY


def _is_string_type(value_type: ExportType) -> bool:
    return value_type in {
        ExportType.STRING,
        ExportType.DECIMAL_STRING,
        ExportType.TIMESTAMP_ISO8601,
    }


def _schema_element(
    *, name: str, physical_type: _PhysicalType | None, nullable: bool | None, children: int | None
) -> bytes:
    thrift = _CompactStruct()
    if physical_type is not None:
        thrift.i32(1, physical_type)
    if nullable is not None:
        thrift.i32(3, 1 if nullable else 0)
    thrift.string(4, name)
    if children is not None:
        thrift.i32(5, children)
    return thrift.finish()


def _string_schema_element(*, name: str, nullable: bool) -> bytes:
    thrift = _CompactStruct()
    thrift.i32(1, _PhysicalType.BYTE_ARRAY)
    thrift.i32(3, 1 if nullable else 0)
    thrift.string(4, name)
    thrift.i32(6, 0)  # ConvertedType.UTF8 for older readers.
    logical_type = _CompactStruct()
    logical_type.struct(1, _CompactStruct().finish())  # LogicalType.STRING.
    thrift.struct(10, logical_type.finish())
    return thrift.finish()


def _data_page_header(row_count: int, page_size: int) -> bytes:
    data_header = _CompactStruct()
    data_header.i32(1, row_count)
    data_header.i32(2, _Encoding.PLAIN)
    data_header.i32(3, _Encoding.RLE)
    data_header.i32(4, _Encoding.RLE)

    page_header = _CompactStruct()
    page_header.i32(1, 0)  # PageType.DATA_PAGE.
    page_header.i32(2, page_size)
    page_header.i32(3, page_size)
    page_header.struct(5, data_header.finish())
    return page_header.finish()


def _key_value(key: str, value: str) -> bytes:
    thrift = _CompactStruct()
    thrift.string(1, key)
    thrift.string(2, value)
    return thrift.finish()


def _column_metadata(
    *,
    physical_type: _PhysicalType,
    field_name: str,
    row_count: int,
    total_size: int,
    data_page_offset: int,
) -> bytes:
    thrift = _CompactStruct()
    thrift.i32(1, physical_type)
    thrift.list_i32(2, (_Encoding.PLAIN, _Encoding.RLE))
    thrift.list_binary(3, (field_name.encode("utf-8"),))
    thrift.i32(4, 0)  # CompressionCodec.UNCOMPRESSED.
    thrift.i64(5, row_count)
    thrift.i64(6, total_size)
    thrift.i64(7, total_size)
    thrift.i64(9, data_page_offset)
    return thrift.finish()


def _column_chunk(metadata: bytes) -> bytes:
    thrift = _CompactStruct()
    thrift.i64(2, 0)  # No external column metadata is written.
    thrift.struct(3, metadata)
    return thrift.finish()


def _row_group(
    *, column_chunks: Sequence[bytes], total_size: int, row_count: int, file_offset: int
) -> bytes:
    thrift = _CompactStruct()
    thrift.list_struct(1, column_chunks)
    thrift.i64(2, total_size)
    thrift.i64(3, row_count)
    thrift.i64(5, file_offset)
    thrift.i64(6, total_size)
    thrift.i16(7, 0)
    return thrift.finish()


def _logical_schema_json(schema: ExportSchema) -> str:
    fields = [
        {
            "name": field.name,
            "nullable": field.nullable,
            "type": field.value_type.value,
        }
        for field in schema.fields
    ]
    return json.dumps(fields, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _file_metadata(*, schema: ExportSchema, row_count: int, row_groups: Sequence[bytes]) -> bytes:
    schema_elements = [
        _schema_element(
            name="schema", physical_type=None, nullable=None, children=len(schema.fields)
        )
    ]
    for field in schema.fields:
        if _is_string_type(field.value_type):
            element = _string_schema_element(name=field.name, nullable=field.nullable)
        else:
            element = _schema_element(
                name=field.name,
                physical_type=_physical_type(field.value_type),
                nullable=field.nullable,
                children=None,
            )
        schema_elements.append(element)

    metadata = {
        "makolet.entity": schema.entity,
        "makolet.logical_schema": _logical_schema_json(schema),
        "makolet.schema_version": schema.version,
    }
    key_values = [_key_value(key, metadata[key]) for key in sorted(metadata)]

    thrift = _CompactStruct()
    thrift.i32(1, 1)
    thrift.list_struct(2, schema_elements)
    thrift.i64(3, row_count)
    thrift.list_struct(4, row_groups)
    thrift.list_struct(5, key_values)
    thrift.string(6, _CREATED_BY)
    return thrift.finish()


def _normalize_value(value: object, value_type: ExportType, limits: ExportLimits) -> object:
    if value_type is ExportType.STRING:
        if not isinstance(value, str):
            raise ExportValidationError("string fields accept only str values")
        if len(value) > limits.max_string_bytes:
            raise ExportValidationError("string value exceeds max_string_bytes")
        encoded = str.encode(value, "utf-8")
        if len(encoded) > limits.max_string_bytes:
            raise ExportValidationError("string value exceeds max_string_bytes")
        return encoded
    if value_type is ExportType.BINARY:
        if not isinstance(value, bytes):
            raise ExportValidationError("binary fields accept only bytes values")
        if len(value) > limits.max_binary_bytes:
            raise ExportValidationError("binary value exceeds max_binary_bytes")
        return value if type(value) is bytes else memoryview(value).tobytes()
    if value_type is ExportType.INT32:
        if type(value) is not int or not -(2**31) <= value < 2**31:
            raise ExportValidationError("int32 value is not a signed 32-bit integer")
        return value
    if value_type is ExportType.INT64:
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise ExportValidationError("int64 value is not a signed 64-bit integer")
        return value
    if value_type is ExportType.BOOLEAN:
        if type(value) is not bool:
            raise ExportValidationError("boolean fields accept only bool values")
        return value
    if value_type is ExportType.DECIMAL_STRING:
        if not isinstance(value, Decimal) or not value.is_finite():
            raise ExportValidationError("decimal_string fields require a finite Decimal")
        sign, digits, exponent = value.as_tuple()
        if not isinstance(exponent, int):
            raise ExportValidationError("decimal_string fields require a finite Decimal")
        if value.is_zero() and exponent >= 0:
            formatted_length = sign + 1
        elif exponent >= 0:
            formatted_length = sign + len(digits) + exponent
        elif -exponent < len(digits):
            formatted_length = sign + len(digits) + 1
        else:
            formatted_length = sign + 2 - exponent
        if formatted_length > limits.max_string_bytes:
            raise ExportValidationError("decimal value exceeds max_string_bytes")
        encoded = format(value, "f").encode("utf-8")
        if len(encoded) > limits.max_string_bytes:
            raise ExportValidationError("decimal value exceeds max_string_bytes")
        return encoded
    if not isinstance(value, datetime):
        raise ExportValidationError("timestamp_iso8601 fields require datetime values")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ExportValidationError("timestamp values must include a timezone")
    timestamp = value.astimezone(UTC).isoformat(timespec="microseconds")
    return timestamp.replace("+00:00", "Z").encode("ascii")


def _encode_definition_levels(present: Sequence[bool]) -> bytes:
    encoded = bytearray()
    index = 0
    while index < len(present):
        value = present[index]
        run_end = index + 1
        while run_end < len(present) and present[run_end] is value:
            run_end += 1
        encoded.extend(_unsigned_varint((run_end - index) << 1))
        encoded.append(1 if value else 0)
        index = run_end
    return struct.pack("<I", len(encoded)) + encoded


def _encode_plain(values: Sequence[object], value_type: ExportType) -> bytes:
    encoded = bytearray()
    if value_type is ExportType.BOOLEAN:
        for offset in range(0, len(values), 8):
            packed = 0
            for bit, value in enumerate(values[offset : offset + 8]):
                if value is True:
                    packed |= 1 << bit
            encoded.append(packed)
        return bytes(encoded)
    if value_type is ExportType.INT32:
        for value in values:
            if type(value) is not int:
                raise ExportValidationError("normalized int32 value is not an integer")
            encoded.extend(struct.pack("<i", value))
        return bytes(encoded)
    if value_type is ExportType.INT64:
        for value in values:
            if type(value) is not int:
                raise ExportValidationError("normalized int64 value is not an integer")
            encoded.extend(struct.pack("<q", value))
        return bytes(encoded)
    for value in values:
        if not isinstance(value, bytes):
            raise ExportValidationError("normalized byte-array value is not bytes")
        binary = value
        encoded.extend(struct.pack("<I", len(binary)))
        encoded.extend(binary)
    return bytes(encoded)


def _plain_value_size(value: object, value_type: ExportType) -> int:
    if value_type is ExportType.BOOLEAN:
        return 1
    if value_type is ExportType.INT32:
        return 4
    if value_type is ExportType.INT64:
        return 8
    if not isinstance(value, bytes):
        raise ExportValidationError("normalized byte-array value is not bytes")
    return 4 + len(value)


@dataclass(frozen=True, slots=True)
class PreparedParquetRow:
    """Validated normalized values that do not retain an untrusted source mapping."""

    values: tuple[object | None, ...]


@dataclass(frozen=True, slots=True)
class _BatchUsage:
    page_sizes: tuple[int, ...]
    total_page_bytes: int
    retained_value_bytes: int
    file_bytes: int
    working_set_bytes: int


def _bounded_list_bytes(item_count: int) -> int:
    # CPython's append growth remains below 2x. Charging two pointer slots per
    # item also covers the transient source and present/value reference lists.
    return _LIST_BASE_BYTES + item_count * _POINTER_BYTES * 2


def _parquet_file_size_upper_bound(
    *, row_count: int, page_sizes: Sequence[int], schema: ExportSchema
) -> int:
    if row_count == 0:
        footer = _file_metadata(schema=schema, row_count=0, row_groups=())
        return len(_PARQUET_MAGIC) + len(footer) + 4 + len(_PARQUET_MAGIC)

    offset = len(_PARQUET_MAGIC)
    column_chunks: list[bytes] = []
    total_row_group_size = 0
    row_group_offset = offset
    for field, page_size in zip(schema.fields, page_sizes, strict=True):
        header_size = len(_data_page_header(row_count, page_size))
        total_size = header_size + page_size
        metadata = _column_metadata(
            physical_type=_physical_type(field.value_type),
            field_name=field.name,
            row_count=row_count,
            total_size=total_size,
            data_page_offset=offset,
        )
        column_chunks.append(_column_chunk(metadata))
        offset += total_size
        total_row_group_size += total_size
    row_group = _row_group(
        column_chunks=column_chunks,
        total_size=total_row_group_size,
        row_count=row_count,
        file_offset=row_group_offset,
    )
    footer = _file_metadata(schema=schema, row_count=row_count, row_groups=(row_group,))
    return offset + len(footer) + 4 + len(_PARQUET_MAGIC)


def _prepared_row_retained_bytes(row: PreparedParquetRow) -> int:
    # Prepared values are only exact built-in bytes, int, bool, or None leaves,
    # so shallow sizes cover their complete retained Python allocations.
    return (
        sys.getsizeof(row)
        + sys.getsizeof(row.values)
        + sum(sys.getsizeof(value) for value in row.values)
    )


def _working_set_upper_bound(*, row_count: int, retained_value_bytes: int, file_bytes: int) -> int:
    batch_references = _bounded_list_bytes(row_count)
    active_column_references = _bounded_list_bytes(row_count) * 2
    # The factor covers simultaneous PLAIN bytearray/bytes, concatenated page,
    # growing output (including realloc), and final immutable output copies.
    return (
        _BATCH_FIXED_OVERHEAD_BYTES
        + retained_value_bytes
        + batch_references
        + active_column_references
        + file_bytes * _ENCODER_OUTPUT_COPY_FACTOR
    )


class ParquetBatchBudget:
    """Prospectively validate rows and account a writer batch before retention."""

    def __init__(self, schema: ExportSchema, limits: ExportLimits) -> None:
        if len(schema.fields) > limits.max_columns:
            raise ExportValidationError("schema exceeds max_columns")
        self._schema = schema
        self._limits = limits
        self._field_names = frozenset(field.name for field in schema.fields)
        page_sizes = tuple(4 if field.nullable else 0 for field in schema.fields)
        maximum_page_sizes = (limits.max_page_bytes,) * len(schema.fields)
        self._file_overhead_bytes = _parquet_file_size_upper_bound(
            row_count=limits.max_rows_per_file,
            page_sizes=maximum_page_sizes,
            schema=schema,
        ) - sum(maximum_page_sizes)
        file_bytes = _parquet_file_size_upper_bound(
            row_count=0, page_sizes=page_sizes, schema=schema
        )
        self._usage = _BatchUsage(
            page_sizes=page_sizes,
            total_page_bytes=sum(page_sizes),
            retained_value_bytes=0,
            file_bytes=file_bytes,
            working_set_bytes=_working_set_upper_bound(
                row_count=0,
                retained_value_bytes=0,
                file_bytes=file_bytes,
            ),
        )
        self._row_count = 0

    @property
    def row_count(self) -> int:
        return self._row_count

    @property
    def estimated_working_set_bytes(self) -> int:
        return self._usage.working_set_bytes

    def prepare_row(self, row: Mapping[str, object], *, row_index: int) -> PreparedParquetRow:
        if not isinstance(row, Mapping):
            raise ExportValidationError(f"row {row_index} must be a mapping")
        unknown = set(row) - self._field_names
        if unknown:
            names = ", ".join(sorted(unknown))
            raise ExportValidationError(f"row {row_index} has unknown fields: {names}")
        values: list[object | None] = []
        page_sizes = [4 if field.nullable else 0 for field in self._schema.fields]
        total_page_bytes = sum(page_sizes)
        for field_index, field in enumerate(self._schema.fields):
            value = row.get(field.name)
            if value is None:
                if not field.nullable:
                    raise ExportValidationError(
                        f"row {row_index} is missing required field {field.name!r}"
                    )
                values.append(None)
            else:
                normalized = _normalize_value(value, field.value_type, self._limits)
                values.append(normalized)
                value_bytes = _plain_value_size(normalized, field.value_type)
                page_sizes[field_index] += value_bytes
                total_page_bytes += value_bytes
            if field.nullable:
                page_sizes[field_index] += 2
                total_page_bytes += 2
            if page_sizes[field_index] > self._limits.max_page_bytes:
                raise ExportValidationError(f"column {field.name!r} exceeds max_page_bytes")
            if total_page_bytes > self._limits.max_file_bytes:
                raise ExportValidationError("Parquet row batch exceeds max_file_bytes")
        return PreparedParquetRow(tuple(values))

    def add_if_within_limits(self, row: PreparedParquetRow) -> str | None:
        usage, reason = self._prospective_usage(row)
        if reason is None:
            self._usage = usage
            self._row_count += 1
        return reason

    def add(self, row: PreparedParquetRow) -> None:
        reason = self.add_if_within_limits(row)
        if reason is not None:
            raise ExportValidationError(reason)

    def _prospective_usage(self, row: PreparedParquetRow) -> tuple[_BatchUsage, str | None]:
        if len(row.values) != len(self._schema.fields):
            raise ExportValidationError("prepared row does not match export schema")
        row_count = self._row_count + 1
        if row_count > self._limits.max_rows_per_file:
            return self._usage, "row batch exceeds max_rows_per_file"

        page_sizes = list(self._usage.page_sizes)
        total_page_bytes = self._usage.total_page_bytes
        for field_index, (field, value) in enumerate(
            zip(self._schema.fields, row.values, strict=True)
        ):
            if value is not None:
                value_bytes = _plain_value_size(value, field.value_type)
                page_sizes[field_index] += value_bytes
                total_page_bytes += value_bytes
            if field.nullable:
                # One two-byte RLE run per row is the hostile worst case.
                page_sizes[field_index] += 2
                total_page_bytes += 2
            if page_sizes[field_index] > self._limits.max_page_bytes:
                return self._usage, f"column {field.name!r} exceeds max_page_bytes"

        file_bytes = total_page_bytes + self._file_overhead_bytes
        if file_bytes > self._limits.max_file_bytes:
            return self._usage, "Parquet row batch exceeds max_file_bytes"
        retained_value_bytes = self._usage.retained_value_bytes + _prepared_row_retained_bytes(row)
        working_set_bytes = _working_set_upper_bound(
            row_count=row_count,
            retained_value_bytes=retained_value_bytes,
            file_bytes=file_bytes,
        )
        if working_set_bytes > self._limits.max_working_set_bytes:
            return self._usage, "Parquet row batch exceeds max_working_set_bytes"
        return (
            _BatchUsage(
                page_sizes=tuple(page_sizes),
                total_page_bytes=total_page_bytes,
                retained_value_bytes=retained_value_bytes,
                file_bytes=file_bytes,
                working_set_bytes=working_set_bytes,
            ),
            None,
        )


def _prepared_column(
    rows: Sequence[PreparedParquetRow], field_index: int
) -> tuple[list[bool], list[object]]:
    present: list[bool] = []
    values: list[object] = []
    for row in rows:
        value = row.values[field_index]
        is_present = value is not None
        present.append(is_present)
        if is_present:
            values.append(value)
    return present, values


def _build_parquet_bytes(
    rows: Sequence[PreparedParquetRow], schema: ExportSchema, limits: ExportLimits
) -> bytes:
    budget = ParquetBatchBudget(schema, limits)
    for row in rows:
        budget.add(row)
    output = bytearray(_PARQUET_MAGIC)
    column_chunks: list[bytes] = []
    total_row_group_size = 0
    row_group_offset = len(output)

    for field_index, field in enumerate(schema.fields):
        present, non_null_values = _prepared_column(rows, field_index)
        definition_levels = _encode_definition_levels(present) if field.nullable else b""
        page_body = definition_levels + _encode_plain(non_null_values, field.value_type)
        if len(page_body) > limits.max_page_bytes:
            raise ExportValidationError(f"column {field.name!r} exceeds max_page_bytes")
        page_header = _data_page_header(len(rows), len(page_body))
        if len(output) + len(page_header) + len(page_body) > limits.max_file_bytes:
            raise ExportValidationError("Parquet output exceeds max_file_bytes")
        data_page_offset = len(output)
        output.extend(page_header)
        output.extend(page_body)
        total_size = len(page_header) + len(page_body)
        total_row_group_size += total_size
        metadata = _column_metadata(
            physical_type=_physical_type(field.value_type),
            field_name=field.name,
            row_count=len(rows),
            total_size=total_size,
            data_page_offset=data_page_offset,
        )
        column_chunks.append(_column_chunk(metadata))

    row_groups: tuple[bytes, ...]
    if rows:
        row_groups = (
            _row_group(
                column_chunks=column_chunks,
                total_size=total_row_group_size,
                row_count=len(rows),
                file_offset=row_group_offset,
            ),
        )
    else:
        output = bytearray(_PARQUET_MAGIC)
        row_groups = ()
    footer = _file_metadata(schema=schema, row_count=len(rows), row_groups=row_groups)
    if len(footer) >= 2**32:
        raise ExportValidationError("Parquet footer exceeds the unsigned 32-bit length field")
    final_size = len(output) + len(footer) + 4 + len(_PARQUET_MAGIC)
    if final_size > limits.max_file_bytes:
        raise ExportValidationError("Parquet output exceeds max_file_bytes")
    output.extend(footer)
    output.extend(struct.pack("<I", len(footer)))
    output.extend(_PARQUET_MAGIC)
    return bytes(output)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _commit_bytes(target: Path, content: bytes, policy: ExistingFilePolicy) -> tuple[str, bool]:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.{uuid4().hex}.tmp"
    digest = hashlib.sha256(content).hexdigest()
    try:
        with temporary.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if policy is ExistingFilePolicy.REPLACE:
            temporary.replace(target)
            return digest, True
        try:
            os.link(temporary, target)
        except FileExistsError:
            if not target.is_file() or _hash_file(target) != digest:
                raise ExportConflictError(
                    f"existing export differs from deterministic output: {target}"
                ) from None
            return digest, False
        else:
            return digest, True
    finally:
        temporary.unlink(missing_ok=True)


def write_parquet(
    target: Path,
    rows: Sequence[Mapping[str, object]],
    schema: ExportSchema,
    *,
    limits: ExportLimits | None = None,
    policy: ExistingFilePolicy = ExistingFilePolicy.REQUIRE_IDENTICAL,
) -> ParquetWriteResult:
    """Write one bounded, deterministic Parquet file using an atomic commit."""

    if target.suffix.lower() != ".parquet":
        raise ExportValidationError("Parquet output paths must end in .parquet")
    effective_limits = limits or ExportLimits()
    budget = ParquetBatchBudget(schema, effective_limits)
    prepared_rows: list[PreparedParquetRow] = []
    for row_index, row in enumerate(rows):
        prepared = budget.prepare_row(row, row_index=row_index)
        budget.add(prepared)
        prepared_rows.append(prepared)
    return write_prepared_parquet(
        target,
        prepared_rows,
        schema,
        limits=effective_limits,
        policy=policy,
    )


def write_prepared_parquet(
    target: Path,
    rows: Sequence[PreparedParquetRow],
    schema: ExportSchema,
    *,
    limits: ExportLimits,
    policy: ExistingFilePolicy = ExistingFilePolicy.REQUIRE_IDENTICAL,
) -> ParquetWriteResult:
    """Write rows already validated by :class:`ParquetBatchBudget`."""

    if target.suffix.lower() != ".parquet":
        raise ExportValidationError("Parquet output paths must end in .parquet")
    content = _build_parquet_bytes(rows, schema, limits)
    digest, created = _commit_bytes(target, content, policy)
    return ParquetWriteResult(
        path=target,
        sha256=digest,
        byte_length=len(content),
        row_count=len(rows),
        created=created,
    )
