# ADR 0003: PostgreSQL current state plus validity history

- Status: Accepted
- Date: 2026-08-11

## Context

The operational store must serve tens of millions of current-price rows, retain a
much larger history, distinguish complete snapshots from deltas, and avoid writing an
unchanged historical row on every observation. Query and ingestion correctness need
transactions, uniqueness, locking, and inspectable plans.

## Decision

Use PostgreSQL 18.4 (and later compatible PostgreSQL 18 security/bugfix minors) as the
operational database. Durable UUIDv7 identifiers are primary
keys; retailer codes, store codes, internal item codes, and GTINs are preserved in
separate columns with scoped uniqueness constraints.

Maintain compact current tables keyed by retailer item and store. History tables use
half-open validity (`valid_from`, nullable `valid_to`) plus source-file provenance.
When an observed value changes, atomically close the open history row and insert the
new value; when it is unchanged, update only observation/freshness metadata. Apply
availability independently from price.

Load parsed records into per-file staging tables with COPY, validate counts and
relationships, then apply set-based SQL in one logical transaction. Delta files touch
only records they name. Full-snapshot absence reconciliation runs only after parsing
and staging complete and the configured minimum-record and maximum prior-count-drop
checks pass for the applicable scopes. Per-file crash-releasing session advisory
locks, portal/family apply locks, and a per-source immutable apply ledger serialize
concurrent work. File ownership does not expire during supported long parsing/apply;
stale recovery must obtain the same lock. Raw bytes are content-addressed globally;
distinct source identities with identical bytes still advance chronology, while
unchanged values create no new history event.

Use PostgreSQL-native normalized text, exact identifiers, B-tree/GIN indexes, and
`pg_trgm` for initial search. Enforce stable ordering and limits. Export historical
analytics as partitioned Parquet rather than turning the operational database into an
unbounded warehouse.

## Consequences

- PostgreSQL is required for production behavior and integration tests; SQLite is not
  a semantic substitute.
- Set-based migrations and query-plan regression checks are core tests.
- Partitioning is introduced for measured table sizes/query plans, not pre-created
  speculative topology.
- Parquet is a reproducible export, not a second source of operational truth.

## Evidence

- [PostgreSQL 18 documentation](https://www.postgresql.org/docs/18/)
- [PostgreSQL COPY](https://www.postgresql.org/docs/18/sql-copy.html)
- [PostgreSQL advisory locks](https://www.postgresql.org/docs/18/explicit-locking.html#ADVISORY-LOCKS)
- [`pg_trgm`](https://www.postgresql.org/docs/18/pgtrgm.html)
