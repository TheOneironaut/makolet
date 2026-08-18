"""Independent reader for the deliberately small Parquet subset used in tests."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class CompactReader:
    data: bytes
    position: int = 0

    def unsigned_varint(self) -> int:
        result = 0
        shift = 0
        while True:
            byte = self.data[self.position]
            self.position += 1
            result |= (byte & 0x7F) << shift
            if byte & 0x80 == 0:
                return result
            shift += 7

    def signed_varint(self) -> int:
        value = self.unsigned_varint()
        return (value >> 1) ^ -(value & 1)

    def struct(self) -> dict[int, object]:
        fields: dict[int, object] = {}
        last_field_id = 0
        while True:
            header = self.data[self.position]
            self.position += 1
            if header == 0:
                return fields
            field_type = header & 0x0F
            delta = header >> 4
            field_id = last_field_id + delta if delta else self.signed_varint()
            last_field_id = field_id
            fields[field_id] = self.value(field_type)

    def value(self, field_type: int) -> object:
        if field_type == 1:
            return True
        if field_type == 2:
            return False
        if field_type in {3, 4, 5, 6}:
            return self.signed_varint()
        if field_type == 7:
            value = struct.unpack_from("<d", self.data, self.position)[0]
            self.position += 8
            return value
        if field_type == 8:
            length = self.unsigned_varint()
            value = self.data[self.position : self.position + length]
            self.position += length
            return value
        if field_type == 9:
            return self.list_value()
        if field_type == 12:
            return self.struct()
        raise AssertionError(f"unsupported compact type in test reader: {field_type}")

    def list_value(self) -> list[object]:
        header = self.data[self.position]
        self.position += 1
        size = header >> 4
        element_type = header & 0x0F
        if size == 15:
            size = self.unsigned_varint()
        return [self.value(element_type) for _ in range(size)]


def footer(path: Path) -> dict[int, object]:
    content = path.read_bytes()
    assert content[:4] == b"PAR1"
    assert content[-4:] == b"PAR1"
    footer_length = struct.unpack_from("<I", content, len(content) - 8)[0]
    footer_start = len(content) - 8 - footer_length
    reader = CompactReader(content, footer_start)
    metadata = reader.struct()
    assert reader.position == len(content) - 8
    return metadata


def _definition_levels(body: bytes, row_count: int) -> tuple[list[int], int]:
    encoded_length = struct.unpack_from("<I", body)[0]
    encoded = CompactReader(body[4 : 4 + encoded_length])
    levels: list[int] = []
    while encoded.position < len(encoded.data):
        header = encoded.unsigned_varint()
        assert header & 1 == 0
        run_length = header >> 1
        value = encoded.data[encoded.position]
        encoded.position += 1
        levels.extend([value] * run_length)
    assert len(levels) == row_count
    return levels, 4 + encoded_length


def _plain_values(body: bytes, physical_type: int, count: int) -> list[object]:
    values: list[object] = []
    position = 0
    if physical_type == 0:
        for index in range(count):
            if index % 8 == 0:
                packed = body[position]
                position += 1
            values.append(bool(packed & (1 << (index % 8))))
        return values
    if physical_type == 1:
        for _ in range(count):
            values.append(struct.unpack_from("<i", body, position)[0])
            position += 4
        return values
    if physical_type == 2:
        for _ in range(count):
            values.append(struct.unpack_from("<q", body, position)[0])
            position += 8
        return values
    for _ in range(count):
        length = struct.unpack_from("<I", body, position)[0]
        position += 4
        values.append(body[position : position + length])
        position += length
    assert position == len(body)
    return values


def rows(path: Path) -> list[dict[str, object | None]]:
    content = path.read_bytes()
    metadata = footer(path)
    row_count_value = metadata[3]
    assert isinstance(row_count_value, int)
    row_count = row_count_value
    schema_elements = metadata[2]
    assert isinstance(schema_elements, list)
    leaf_schemas = schema_elements[1:]
    row_groups = metadata[4]
    assert isinstance(row_groups, list)
    if not row_groups:
        return []
    row_group = row_groups[0]
    assert isinstance(row_group, dict)
    column_chunks = row_group[1]
    assert isinstance(column_chunks, list)
    columns: list[list[object | None]] = []
    names: list[str] = []
    for leaf_schema, column_chunk in zip(leaf_schemas, column_chunks, strict=True):
        assert isinstance(leaf_schema, dict)
        assert isinstance(column_chunk, dict)
        physical_type = int(leaf_schema[1])
        nullable = int(leaf_schema[3]) == 1
        name_value = leaf_schema[4]
        assert isinstance(name_value, bytes)
        names.append(name_value.decode("utf-8"))
        column_metadata = column_chunk[3]
        assert isinstance(column_metadata, dict)
        offset = int(column_metadata[9])
        page_reader = CompactReader(content, offset)
        page_header = page_reader.struct()
        page_size_value = page_header[3]
        assert isinstance(page_size_value, int)
        page_size = page_size_value
        body = content[page_reader.position : page_reader.position + page_size]
        if nullable:
            levels, values_offset = _definition_levels(body, row_count)
        else:
            levels = [1] * row_count
            values_offset = 0
        plain_values = iter(_plain_values(body[values_offset:], physical_type, sum(levels)))
        column = [next(plain_values) if level else None for level in levels]
        columns.append(column)

    return [
        {name: columns[column_index][row_index] for column_index, name in enumerate(names)}
        for row_index in range(row_count)
    ]
