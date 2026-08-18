# Dependency-free Parquet interoperability

Date verified: 2026-08-11

## Scope and clean-room basis

Makolet writes a deliberately small Parquet subset without importing a Parquet or
Thrift package. The implementation is original Apache-2.0 project code based only on
the public Apache Parquet specification; no implementation source was copied or
translated. Apache's format repository and specification are themselves published
under Apache-2.0.

The primary references used were:

- Apache's [file layout](https://parquet.apache.org/docs/file-format/), including the
  leading/trailing `PAR1` magic, little-endian footer length, and footer placement.
- Apache's [metadata specification](https://parquet.apache.org/docs/file-format/metadata/)
  and canonical [`parquet.thrift`](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift),
  which require TCompactProtocol for page and file metadata and define every field ID.
- Apache's [Data Page V1 layout](https://parquet.apache.org/docs/file-format/data-pages/),
  [PLAIN and RLE encodings](https://parquet.apache.org/docs/file-format/data-pages/encodings/),
  and [null encoding](https://parquet.apache.org/docs/file-format/nulls/).
- Apache's [logical type rules](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md),
  including `BYTE_ARRAY` plus both `LogicalType.STRING` and legacy
  `ConvertedType.UTF8` for interoperable UTF-8 strings.
- Apache's [format-version guidance](https://parquet.apache.org/docs/file-format/versions/),
  which identifies Data Page V1, PLAIN, RLE, and uncompressed pages as version-1
  features and recommends file metadata version `1` for compatibility.

The supported surface is flat schemas containing nullable or required UTF-8 strings,
binary values, signed 32/64-bit integers, and booleans. Decimal values are finite
`Decimal` instances serialized as non-exponent strings; aware timestamps are converted
to UTC ISO-8601 strings with six fractional digits and `Z`. Each bounded part file has
at most one row group and one uncompressed Data Page V1 per column. Nested values,
floating point, native Parquet DECIMAL/TIMESTAMP, compression, dictionaries, page
indexes, and bloom filters are intentionally unsupported.

## Independent reader verification

The validation reader is DuckDB 1.5.4 in an isolated uv environment. It is not present
in `pyproject.toml`, `uv.lock`, or the SBOM and is not redistributed by Makolet. After
generating `all-types.parquet` and `empty.parquet` with the project writer, the reader
was invoked with:

```powershell
uv run --isolated --no-project --with duckdb==1.5.4 python -
```

The validation script used `DESCRIBE SELECT * FROM read_parquet(?)`, selected every
column, checked `hex(name)` and `hex(payload)`, and counted both files. Exact results:

```text
DuckDB: 1.5.4
all-types.parquet: 1212 bytes, 3 rows
SHA-256: 3b5299d801735bfca946d1089bc4a527bcf28016db9091189ee21e9c6f006654
schema: VARCHAR, BLOB, INTEGER, BIGINT, BOOLEAN, VARCHAR, VARCHAR (all nullable)
Hebrew UTF-8 hex: D797D79CD791
binary hex: 00FF
integer extrema: -2147483648 and 9223372036854775807
decimal/timestamp strings: 6.20 and 2026-08-11T12:34:56.789000Z
empty.parquet: 659 bytes, 0 rows, 7 readable columns
SHA-256: 6b78d3262863a667c21a5f1c3691bad544bbf687016c062a2e431784c6df2dc3
```

Offline tests separately decode TCompactProtocol metadata and PLAIN/RLE pages with a
test-only reader, round-trip nulls, empty strings, NUL, newlines, Hebrew, binary bytes,
integer bounds, booleans, decimals, and timestamps, and lock a small golden file by
SHA-256. Dataset tests cover bounded multi-part output, canonical manifests, checksums,
percent-escaped partition values, idempotent publication, conflict refusal, and safe
generation replacement.
