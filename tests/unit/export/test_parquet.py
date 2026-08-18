from __future__ import annotations

import hashlib
import json
import struct
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from makolet.adapters.export import (
    ExistingFilePolicy,
    ExportConflictError,
    ExportField,
    ExportLimits,
    ExportSchema,
    ExportType,
    ExportValidationError,
    write_parquet,
)
from makolet.adapters.export import parquet as parquet_module
from tests.unit.export.parquet_reader import footer, rows


def _schema() -> ExportSchema:
    return ExportSchema(
        entity="prices",
        version="2026-08-11.1",
        fields=(
            ExportField("name", ExportType.STRING),
            ExportField("payload", ExportType.BINARY),
            ExportField("small", ExportType.INT32, nullable=False),
            ExportField("large", ExportType.INT64),
            ExportField("active", ExportType.BOOLEAN),
            ExportField("price", ExportType.DECIMAL_STRING),
            ExportField("observed_at", ExportType.TIMESTAMP_ISO8601),
        ),
    )


def _sample_rows() -> list[dict[str, object]]:
    return [
        {
            "name": '\u05d7\u05dc\u05d1, "\u05de\u05d9\u05d5\u05d7\u05d3"\n'
            "\u05e9\u05d5\u05e8\u05d4\x00",
            "payload": b"\x00\xffPAR1",
            "small": -(2**31),
            "large": 2**63 - 1,
            "active": True,
            "price": Decimal("006.200"),
            "observed_at": datetime(
                2026, 8, 11, 15, 34, 56, 789, tzinfo=timezone(timedelta(hours=3))
            ),
        },
        {
            "name": None,
            "payload": b"",
            "small": 2**31 - 1,
            "large": None,
            "active": False,
            "price": Decimal("0.00"),
            "observed_at": None,
        },
        {
            "name": "",
            "payload": None,
            "small": 0,
            "large": -1,
            "active": None,
            "price": None,
            "observed_at": datetime(2026, 8, 11, tzinfo=UTC),
        },
    ]


def test_writes_deterministic_parquet_and_round_trips_supported_values(tmp_path: Path) -> None:
    target = tmp_path / "values.parquet"

    first = write_parquet(target, _sample_rows(), _schema())
    second = write_parquet(target, _sample_rows(), _schema())

    content = target.read_bytes()
    assert content[:4] == b"PAR1"
    assert content[-4:] == b"PAR1"
    footer_length = struct.unpack_from("<I", content, len(content) - 8)[0]
    assert 0 < footer_length < len(content) - 8
    assert first.sha256 == hashlib.sha256(content).hexdigest()
    assert first.created is True
    assert second.created is False
    assert first.sha256 == second.sha256
    assert rows(target) == [
        {
            "name": (
                '\u05d7\u05dc\u05d1, "\u05de\u05d9\u05d5\u05d7\u05d3"\n\u05e9\u05d5\u05e8\u05d4\x00'
            ).encode(),
            "payload": b"\x00\xffPAR1",
            "small": -(2**31),
            "large": 2**63 - 1,
            "active": True,
            "price": b"6.200",
            "observed_at": b"2026-08-11T12:34:56.000789Z",
        },
        {
            "name": None,
            "payload": b"",
            "small": 2**31 - 1,
            "large": None,
            "active": False,
            "price": b"0.00",
            "observed_at": None,
        },
        {
            "name": b"",
            "payload": None,
            "small": 0,
            "large": -1,
            "active": None,
            "price": None,
            "observed_at": b"2026-08-11T00:00:00.000000Z",
        },
    ]


def test_footer_records_version_schema_annotations_and_stable_metadata(tmp_path: Path) -> None:
    target = tmp_path / "metadata.parquet"
    result = write_parquet(target, _sample_rows()[:1], _schema())

    metadata = footer(target)
    schema_elements = metadata[2]
    assert isinstance(schema_elements, list)
    assert metadata[1] == 1
    assert metadata[3] == 1
    assert schema_elements[0][4] == b"schema"
    assert schema_elements[0][5] == len(_schema().fields)
    assert schema_elements[1][6] == 0  # ConvertedType.UTF8.
    assert schema_elements[1][10] == {1: {}}
    assert metadata[6] == b"makolet version 0.1.0 (dependency-free parquet writer)"
    key_values = metadata[5]
    assert isinstance(key_values, list)
    decoded_key_values = {
        item[1].decode(): item[2].decode() for item in key_values if isinstance(item, dict)
    }
    assert decoded_key_values["makolet.entity"] == "prices"
    assert decoded_key_values["makolet.schema_version"] == "2026-08-11.1"
    assert json.loads(decoded_key_values["makolet.logical_schema"])[5] == {
        "name": "price",
        "nullable": True,
        "type": "decimal_string",
    }
    assert result.sha256 == "bb95b6d9d67af9c96491061608a2d818b5a7ee315d0af1e8853d6265428e8b3f"


def test_empty_file_preserves_schema_and_has_no_row_groups(tmp_path: Path) -> None:
    target = tmp_path / "empty.parquet"

    result = write_parquet(target, [], _schema())

    metadata = footer(target)
    assert result.row_count == 0
    assert metadata[3] == 0
    assert metadata[4] == []
    assert rows(target) == []


def test_existing_file_policy_never_silently_overwrites(tmp_path: Path) -> None:
    target = tmp_path / "safe.parquet"
    write_parquet(target, _sample_rows()[:1], _schema())
    original = target.read_bytes()

    with pytest.raises(ExportConflictError, match="differs"):
        write_parquet(target, _sample_rows()[1:], _schema())

    assert target.read_bytes() == original
    replaced = write_parquet(
        target,
        _sample_rows()[1:],
        _schema(),
        policy=ExistingFilePolicy.REPLACE,
    )
    assert replaced.created is True
    assert rows(target)[0]["small"] == 2**31 - 1


@pytest.mark.parametrize(
    ("rows_value", "schema", "message"),
    [
        (
            [{"value": "אבג"}],
            ExportSchema("items", "1", (ExportField("value", ExportType.STRING),)),
            "max_string_bytes",
        ),
        (
            [{"value": b"12345"}],
            ExportSchema("items", "1", (ExportField("value", ExportType.BINARY),)),
            "max_binary_bytes",
        ),
        (
            [{"value": 2**31}],
            ExportSchema("items", "1", (ExportField("value", ExportType.INT32),)),
            "signed 32-bit",
        ),
        (
            [{"value": True}],
            ExportSchema("items", "1", (ExportField("value", ExportType.INT64),)),
            "signed 64-bit",
        ),
        (
            [{"value": Decimal("NaN")}],
            ExportSchema("items", "1", (ExportField("value", ExportType.DECIMAL_STRING),)),
            "finite Decimal",
        ),
        (
            [{"value": datetime(2026, 8, 11, tzinfo=UTC).replace(tzinfo=None)}],
            ExportSchema("items", "1", (ExportField("value", ExportType.TIMESTAMP_ISO8601),)),
            "timezone",
        ),
    ],
)
def test_rejects_values_outside_the_explicit_subset(
    tmp_path: Path,
    rows_value: list[dict[str, object]],
    schema: ExportSchema,
    message: str,
) -> None:
    limits = ExportLimits(max_string_bytes=4, max_binary_bytes=4)

    with pytest.raises(ExportValidationError, match=message):
        write_parquet(tmp_path / "invalid.parquet", rows_value, schema, limits=limits)


def test_rejects_unknown_missing_and_unbounded_rows(tmp_path: Path) -> None:
    schema = ExportSchema(
        "items", "1", (ExportField("item_id", ExportType.STRING, nullable=False),)
    )
    limits = ExportLimits(max_rows_per_file=1, max_dataset_rows=1)

    with pytest.raises(ExportValidationError, match="unknown fields"):
        write_parquet(tmp_path / "unknown.parquet", [{"other": "x"}], schema)
    with pytest.raises(ExportValidationError, match="required field"):
        write_parquet(tmp_path / "missing.parquet", [{}], schema)
    with pytest.raises(ExportValidationError, match="max_rows_per_file"):
        write_parquet(
            tmp_path / "many.parquet",
            [{"item_id": "1"}, {"item_id": "2"}],
            schema,
            limits=limits,
        )
    with pytest.raises(ExportValidationError, match=r"end in \.parquet"):
        write_parquet(tmp_path / "wrong.csv", [{"item_id": "1"}], schema)


def test_schema_and_wire_size_limits_are_validated() -> None:
    with pytest.raises(ExportValidationError, match="unique"):
        ExportSchema(
            "items",
            "1",
            (
                ExportField("id", ExportType.STRING),
                ExportField("id", ExportType.STRING),
            ),
        )
    with pytest.raises(ExportValidationError, match="signed Parquet i32"):
        ExportLimits(max_rows_per_file=2**31, max_dataset_rows=2**31)
    with pytest.raises(ExportValidationError, match="PLAIN uint32"):
        ExportLimits(max_binary_bytes=2**32)


def test_rejects_oversized_page_before_plain_encoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = ExportSchema(
        "items",
        "1",
        (ExportField("name", ExportType.STRING, nullable=False),),
    )
    limits = ExportLimits(max_page_bytes=16)

    def fail_if_encoding_starts(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("oversized page reached materializing PLAIN encoder")

    monkeypatch.setattr(parquet_module, "_encode_plain", fail_if_encoding_starts)

    with pytest.raises(ExportValidationError, match="max_page_bytes"):
        write_parquet(
            tmp_path / "oversized.parquet",
            [{"name": "abcdefgh"}, {"name": "ijklmnop"}],
            schema,
            limits=limits,
        )


def test_rejects_aggregate_file_and_working_set_before_encoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strings = ExportSchema(
        "items",
        "1",
        (
            ExportField("first", ExportType.STRING, nullable=False),
            ExportField("second", ExportType.STRING, nullable=False),
        ),
    )
    integers = ExportSchema(
        "items",
        "1",
        (
            ExportField("first", ExportType.INT32, nullable=False),
            ExportField("second", ExportType.INT32, nullable=False),
        ),
    )

    def fail_if_encoding_starts(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("unsafe row batch reached materializing PLAIN encoder")

    monkeypatch.setattr(parquet_module, "_encode_plain", fail_if_encoding_starts)

    with pytest.raises(ExportValidationError, match="max_file_bytes"):
        write_parquet(
            tmp_path / "large-file.parquet",
            [
                {"first": "abcdefgh", "second": "ijklmnop"},
                {"first": "qrstuvwx", "second": "yzabcdef"},
            ],
            strings,
            limits=ExportLimits(
                max_page_bytes=32,
                max_file_bytes=40,
                max_working_set_bytes=1_000,
            ),
        )

    with pytest.raises(ExportValidationError, match="max_working_set_bytes"):
        write_parquet(
            tmp_path / "large-working-set.parquet",
            [{"first": 1, "second": 2}],
            integers,
            limits=ExportLimits(
                max_page_bytes=1_000,
                max_file_bytes=1_000,
                max_working_set_bytes=1_000,
            ),
        )


def test_file_and_working_set_limits_are_consistent() -> None:
    with pytest.raises(ExportValidationError, match="max_page_bytes"):
        ExportLimits(max_page_bytes=2, max_file_bytes=1)
    with pytest.raises(ExportValidationError, match="max_file_bytes"):
        ExportLimits(max_file_bytes=2, max_working_set_bytes=1)
