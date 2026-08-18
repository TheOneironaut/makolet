# ADR 0001: CPython async modular-monolith stack

- Status: Accepted; MCP and Parquet dependency choices amended by ADRs 0006 and 0007
- Date: 2026-08-11

## Context

The platform must stream inconsistent XML and compressed files, collect from HTTPS
and FTP, bulk-load PostgreSQL, expose CLI/HTTP/MCP interfaces, export Parquet, and be
maintainable by a broad contributor base. One language must cover the core unless a
measured need justifies another. Product/build dependencies must be OSI-approved and
compatible with Apache-2.0; the repository policy separately defines the
owner-approved, non-redistributed tooling and platform-component boundaries.

## Decision

Use standard-GIL CPython 3.14.7 (compatible patch releases below 3.15) as the single
implementation language and package with `uv`.
Structure the application as an async modular monolith using:

- standard-library `xml.etree.ElementTree` pull/iterative parsing, `Decimal`, Gzip,
  ZIP, Zstandard, and FTP primitives;
- AnyIO and HTTPX for bounded asynchronous network orchestration;
- Pydantic and pydantic-settings at configuration and interface boundaries;
- SQLAlchemy asyncio, asyncpg, and Alembic for PostgreSQL persistence, COPY-capable
  bulk ingestion, and migrations;
- FastAPI/Starlette and Uvicorn for the versioned OpenAPI HTTP interface;
- Typer and Rich for the coherent CLI;
- a small directly tested MCP protocol core for stdio and stateless Streamable HTTP;
- Boto3 behind an async adapter boundary for S3-compatible object storage;
- an original bounded Apache Parquet subset for partitioned analytical export;
- structlog and prometheus-client for open structured logs and metrics.

Pin every direct dependency and commit the complete `uv.lock`. Protocol revisions,
especially MCP, require direct transport integration tests before upgrades.

XML parsing rejects every DTD/DOCTYPE, never resolves external entities, verifies a
safe bundled Expat version, and enforces input, decompressed-byte, depth, record-size,
record-count, and expansion-ratio bounds. Network and archive streams are stored as
exact bytes before decoding or parsing. Synchronous XML, FTP, Boto3, and CPU-heavy
work runs through explicitly bounded worker threads/processes rather than blocking
the async event loop.

## Alternatives considered

### TypeScript/Node.js

The HTTP, CLI, and MCP ecosystems are capable, but exact-decimal handling, large XML
normalization, PostgreSQL COPY workflows, and Parquet/data tooling require more
third-party surface. It offers no measured advantage for this backend-only product.

### Rust

Rust offers excellent throughput and memory control, but the changing source schemas,
contributor accessibility, data tooling, and iteration cost make it a poor first
language absent a measured parser bottleneck. A later isolated native extension must
be justified by benchmarks and an ADR.

### Synchronous Python throughout

It is simpler locally but wastes capacity across many high-latency listing/download
operations. Async orchestration with bounded synchronous parsing keeps control flow
explicit without making domain code framework-dependent.

### lxml and Psycopg

Both are technically strong. The standard library is sufficient for the required
streaming XML subset and reduces native/license audit surface. asyncpg is Apache-2.0,
supports efficient COPY operations, and avoids introducing an LGPL runtime component.
These decisions may be revisited only with an evidence-backed need.

## Consequences

- Contributors need the pinned Python 3.14 runtime; `uv` and containers make this
  deterministic.
- XML safety and archive limits are application responsibilities and receive focused
  tests and benchmarks.
- Boto3 and FTP calls require bounded thread offload.
- The MCP surface is new enough that protocol-version and transport tests are
  mandatory; see ADR 0006.
- The intentionally small Parquet subset trades feature breadth for a dependency
  boundary that can be fully audited and independently read; see ADR 0007.
- One language serves collection, domain logic, operations, API, CLI, MCP, tests, and
  benchmarks.

## Evidence

- [Python XML security guidance](https://docs.python.org/3/library/xml.html#xml-security)
- [Python 3.14.7 release](https://www.python.org/downloads/release/python-3147/)
- [Python 3.14 `compression` package](https://docs.python.org/3/library/compression.html)
- [HTTPX async streaming](https://www.python-httpx.org/async/)
- [asyncpg usage/API](https://magicstack.github.io/asyncpg/current/)
- [SQLAlchemy asyncio](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html)
- [FastAPI OpenAPI documentation](https://fastapi.tiangolo.com/features/)
- [Model Context Protocol specification](https://modelcontextprotocol.io/specification/)
- [Apache Parquet file format](https://parquet.apache.org/docs/file-format/)
