# ADR 0008: Stable-identity in-place normalized rebuild

- Status: Accepted; extends ADRs 0003 and 0004
- Date: 2026-08-12

## Context

The immutable archive must support rebuilding normalized state after parser fixes or
operational recovery. A naive truncate-and-replay changes generated UUIDs, invalidates
public keyset cursors, rewrites temporal intervals, and can erase reviewed catalog and
matching decisions. Replaying in download order also changes history when the original
apply chronology differed. An in-memory or temporary mapping cannot survive a process
failure, and a shadow-schema swap would require a second operational schema plus a
read-routing design the application does not implement.

## Decision

Use an explicitly operator-confirmed, no-network, in-place rebuild under a PostgreSQL
maintenance barrier and scheduled downtime. Capture a fixed archive-backed file list
and each source's original parser/apply metadata. Replay previously applied files at
their immutable original apply time; order archive-only files by their effective
source time. Checkpoint a file only after replay and isolated-catalog bootstrap both
succeed.

Preserve curated catalog and matching state in place: stores and aliases, retailer
items, canonical products, identifier groups, reviewed identifiers, match candidates
in every decision state, and manual or isolated confirmed matches. Reset and rederive
only archive-derived observations, temporal rows, retailer assertions, system exact
identifier/match projections, promotion relations, watermarks, staging, and apply
claims. Raw replay must reconnect to the preserved identities and cannot overwrite
reviewed canonical fields.

Migration `0008_stable_rebuild_snapshots` adds durable run-scoped original/rebuilt
snapshots. Its fixed entity allowlist covers the preserved catalog/audit overlay and
every reset public, cursor-bearing, temporal, promotion-relation, watermark, and
apply-ledger table. Each row has an explicit logical key (512-byte maximum) and JSON
object payload (1 MiB maximum). Temporary tables may assist one finishing transaction
but are never the crash-recovery source of truth.

For an unchanged parser with no archive-only expansion, compare regenerated logical
rows, reconcile UUIDs and observation/interval timestamps, and require exact final row
equality before atomically deleting snapshots and clearing the barrier. This preserves
existing cursors and byte/logical Parquet output. A mismatch rolls back completion and
keeps the barrier and original evidence.

A parser correction or archive-only expansion may legitimately change derived state.
It retains every original snapshot row with a `preserved` or `superseded` outcome and
deletes only rebuilt scratch rows. It never erases the audit overlay, and export
equivalence is not promised. Retained correction/expansion evidence has no automatic
expiry; failed runs retain snapshots for same-run resume. Downgrade refuses while any
snapshot rows remain.

## Consequences

- Durable UUIDs, reviewed decisions, public cursor continuity, historical intervals,
  promotion versions, and provenance survive ordinary rebuilds exactly.
- A crash before replay or after a replay commit resumes from the durable run; an
  uncheckpointed replay is idempotently retried.
- The snapshot can temporarily approach the size of the affected normalized state,
  so operators must reserve database capacity and include retained correction audits
  in backups.
- Reads can be partial for the entire maintenance window. The implementation does not
  provide uninterrupted reads, a shadow swap, or concurrent normalized editing.
- A parser correction requires explicit downstream review because superseded IDs and
  byte-equivalent exports are not expected.

## Evidence

- [Normalized-state rebuild runbook](../operations-runbook.md#normalized-state-rebuild)
- [Ingestion lifecycle](../ingestion-lifecycle.md#normalized-state-rebuild)
- [Data dictionary](../data-dictionary.md#normalized_rebuild_snapshots)
