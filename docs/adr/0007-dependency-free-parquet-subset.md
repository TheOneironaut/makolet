# ADR 0007: Dependency-free bounded Parquet subset

- Status: Accepted; amends ADR 0001
- Date: 2026-08-11

## Context

Makolet needs portable analytical export but not an in-process analytical query
engine. The initially selected PyArrow 25.0.1 wheel had Apache metadata but its exact
artifact audit found bundled non-OSI/proprietary license material and incomplete
notice confidence for this project's strict dependency policy. Evaluated DuckDB and
Fastparquet artifacts/dependency closures also did not clear that policy. Omitting
Parquet would fail the portability requirement; silently accepting a policy exception
would fail the open-source requirement.

## Decision

Implement an original small Apache Parquet 1.x-compatible writer from the public
format specification. Support only flat schemas with required/nullable UTF-8 strings,
binary, signed int32/int64, booleans, decimal strings, and UTC ISO-8601 timestamp
strings. Use uncompressed Data Page V1, PLAIN values, RLE definition levels, one row
group, and at most one page per column.

Publish bounded immutable part files under Hive-style entity/retailer/UTC-date
partitions. A canonical manifest records schema version, row/file counts, SHA-256, and
a content-derived dataset generation. Identical reruns reuse a generation; conflicting
output fails unless an explicit internal replacement policy is selected.

The PostgreSQL export operation currently emits `current_prices` and `price_history`.
It streams query rows to a bounded disk spool before synchronous part creation and
limits columns, rows/file, files, dataset rows, value/page/file bytes, retained
working memory, and partitions. One operation-wide budget spans both entities and all
partitions for rows, files, cumulative spool bytes, output bytes, and elapsed time.
The repeatable-read transaction preflights exact per-partition rows and sums each
partition's rows-per-file ceiling to obtain the minimum possible files before
publication. Each input row is validated and normalized before it is retained.
A prospective batch budget charges exact normalized Python values,
complete Parquet framing, and conservative encoder/output copies; the dataset splits
before the next row would cross a row, page, file, or working-set ceiling. A single
oversized row fails before the PLAIN encoder materializes it. Per-partition manifests
and generations remain immutable. If an unpredictable later partition crosses a
byte/file/time bound, completed generations remain valid and the failure identifies
partial publication. Per-partition synchronous writes and publication execute in a
supervised, killable child process. Before generation rename or manifest commit, the
publisher durably writes a canonical operation journal. The parent always follows
normal exit, failure, cancellation, or forced termination with a separately bounded
reconciliation child. Reconciliation either validates the exact committed manifest
and records its publication receipt, or removes only the operation's deterministic
staging path, unpublished generation, and known temporary files; it removes the
journal last. Publisher termination and reconciliation consume the reserved cleanup
budget, so a stalled filesystem operation cannot outlive the operation as an
unowned thread or silently lose an immutable publication receipt.

## Consequences

- The runtime lock has no third-party Parquet/Thrift/analytical-engine dependency.
- Nested types, native Parquet decimal/timestamp logical types, compression,
  dictionaries, page indexes, bloom filters, and analytical reads are intentionally
  unsupported.
- Format correctness requires byte-level unit tests, golden hashes, and an independent
  external reader check. Expanding the subset requires specification evidence and
  interoperability tests.
- The conservative working-set estimate deliberately permits smaller files than the
  nominal file ceiling for adversarial wide/multibyte rows; predictable memory safety
  takes precedence over maximizing one part's size.
- The export is a portable analytical copy, not the operational database or a complete
  PostgreSQL/raw-archive backup.

## Evidence

- [Parquet interoperability report](../research/parquet-interop.md)
- [Dependency/license audit](../research/dependency-license-audit.md)
- [Apache Parquet format](https://parquet.apache.org/docs/file-format/)
