# CLI

`makolet` is the single operational and query command surface. It loads the same
`MAKOLET_*` settings and production composition used by API, MCP, and the worker.

```text
uv run makolet --help
```

Most finite commands accept `--json`. Human output is deliberately compact; JSON is
the stable choice for scripts. Options belong after the selected subcommand, for
example `makolet products search tahini --limit 10 --json`.

## Database

| Command | Behavior |
|---|---|
| `makolet database migrate [--revision head] [--json]` | Run Alembic upgrade using the configured PostgreSQL URL, idempotently synchronize retailer/portal registry metadata, then return status and registry counts. Revisions are limited to `head`, `base`, or a short safe identifier. |
| `makolet database status [--json]` | Read and report PostgreSQL version, `expected_migration_heads`, all `current_migration_revisions`, and `schema_ready`. `schema_ready` is false for an absent, behind, unknown, or divergent revision set and true only for an exact head match. The compatibility `migration_revision` field is null unless exactly one current revision exists. |

Status is read-only. Migrations time out after ten minutes and do not accept arbitrary
shell fragments.

## Sources

| Command | Behavior |
|---|---|
| `makolet sources list [--json]` | List all 28 registry rows in official order with source ID, display name, family, and configured/disabled status. |
| `makolet sources inspect <source-id> [--json]` | Show official entity, observed chain IDs, family, and disabled reason/public lead when applicable. |
| `makolet sources test <source-id> [--json]` | Perform one bounded read-only listing request (`limit=1`) and report discovered metadata. It does not download or ingest a file. |

Source health is external and can change. `sources test` returning an error is not an
empty successful listing; compare it with the dated
[coverage matrix](source-coverage.md).

## Ingestion and worker

| Command | Behavior |
|---|---|
| `makolet ingest source <source-id> [--json]` | Discover all bounded pages for one enabled source and ingest known file/compression types. |
| `makolet ingest retailer <retailer-id> [--json]` | Ingest the source keyed by that official legal-group/retailer ID. |
| `makolet ingest all [--json]` | Ingest the configured batch source set serially. Without `MAKOLET_ENABLED_SOURCES`, this means every non-disabled registry source and can generate many public requests. |
| `makolet ingest backfill <source-id> --since <timestamp> --until <timestamp> [--json]` | Discover the source and ingest only files whose known source timestamp is inside the inclusive range. Files without a source timestamp are excluded. |
| `makolet ingest replay <source-file-uuid> [--json]` | Verify and replay archived bytes without network access. |
| `makolet ingest replay-range --since <timestamp> --until <timestamp> [--limit 50] [--cursor ...] [--json]` | Replay one deterministic page of archived source files in the half-open archive-attachment range `[since, until)`, without discovery or download. The page limit is 1–200. |
| `makolet ingest rebuild-normalized --confirm REBUILD-NORMALIZED-STATE --requested-by <label> [--json]` | Start the explicitly destructive, auditable rebuild of raw-derived normalized state from the existing archive. The acknowledgement must match exactly. |
| `makolet ingest resume-rebuild <rebuild-run-uuid> [--json]` | Resume the same durable rebuild from its first uncompleted source file. |
| `makolet ingest rebuild-status <rebuild-run-uuid> [--json]` | Inspect durable rebuild totals, cursor, status, and safe failure detail. |
| `makolet ingest worker [--source <id> ...] [--once] [--json]` | Run selected sources once with bounded concurrency or continuously at configured intervals. |

`--since`, `--until`, and `--at` require ISO-8601 timezones. A continuous worker needs
an interval for every selected source. Configure JSON-shaped settings, for example:

```powershell
$env:MAKOLET_ENABLED_SOURCES = '["shufersal"]'
$env:MAKOLET_SOURCE_INTERVALS_SECONDS = '{"shufersal":21600}'
uv run makolet ingest worker --source shufersal
```

`--once` recovers stale jobs, runs a stable de-duplicated source set with the worker's
fixed concurrency, and always renders the complete per-source summary. It exits `0`
only when every outcome succeeded and exits `4` (temporary failure) after rendering
when any outcome failed, including a mixed run. Continuous mode runs each source
immediately, adds configured positive jitter to later intervals, emits worker
telemetry, serves worker liveness and Prometheus metrics on the configured worker
metrics address, and drains queued work within one shared shutdown grace after
SIGINT/SIGTERM. A process watchdog remains armed through `asyncio.run` teardown and
forces a temporary-failure exit if cancellation-ignoring work outlives that bound.
The worker command owns these signals outside Uvicorn, so the watchdog is armed
before metrics-server request or lifespan shutdown begins.

Range replay uses each source file's recorded download-finish/archive-attachment time,
then its UUID, as the stable order. If a page fails, rerun it with the same input and
cursor; already replayed files remain idempotent and every attempt remains audited.
The normalized rebuild has a larger operational boundary than ordinary replay; follow
the backup, downtime, partial-query, and recovery procedure in the
[operations runbook](operations-runbook.md#normalized-state-rebuild).

## Status, failures, and quarantine

| Command | Behavior |
|---|---|
| `makolet status [--limit 50] [--cursor ...] [--json]` | Maintenance/rebuild state plus the latest source-file state per persisted portal. |
| `makolet freshness [--limit 50] [--cursor ...] [--json]` | Per-store globally latest availability observation and contributor plus counts capped at `item_probe_limit`; `items_truncated` marks lower-bound counts. |
| `makolet source-status [--limit 50] [--cursor ...] [--json]` | Latest and last-good ingestion state plus the latest durable collection attempt, counts, charged bytes (archive plus failed/retried transfers), categorized truncation, and safe failure per source portal. |
| `makolet failures [--limit 50] [--cursor ...] [--json]` | Retryable and terminal source-file failures. |
| `makolet quarantine list [--limit 50] [--cursor ...] [--json]` | Quarantined source files and issue counts. |
| `makolet quarantine inspect <source-file-uuid> [--json]` | Archive/hash/parser metadata and up to 1,000 structured issues for one quarantined file. |
| `makolet doctor [--json]` | Check PostgreSQL, initialize/check the configured archive, and report production capability wiring. It does not contact every retailer. |

All list limits above are 1–200 and use opaque deterministic cursors. Public query
cursors are versioned, checksummed, and bound to the exact command route plus
normalized filters; changed-filter, cross-command, malformed, and legacy cursors fail
with a nonzero validation result before database work.

## Catalog matching administration

| Command | Behavior |
|---|---|
| `makolet matching generate [--item-limit 50] [--candidate-limit 50] [--review-threshold 0.65] [--cursor ...] [--json]` | Process one UUID-keyset page of isolated retailer items. Candidate products come from bounded indexed exact-identifier and trigram blocks; normalized name, manufacturer, quantity/unit, and packaging contribute explainable scores. The result supplies a continuation cursor when more items remain. |
| `makolet matching list [--status pending|accepted|rejected|superseded] [--retailer-id UUID] [--limit 50] [--cursor ...] [--json]` | List one score-descending, UUID-tiebroken review page including evidence and review metadata. |
| `makolet matching inspect <candidate-uuid> [--json]` | Inspect subject/target features, explanations, current effective mapping, auditor, and timestamps. |
| `makolet matching accept <candidate-uuid> --reviewed-by <label> [--json]` | Transactionally accept a pending candidate, replace only the subject's system-isolated mapping, and supersede other pending choices. A manual/non-isolated conflict fails nonzero without mutation. |
| `makolet matching reject <candidate-uuid> --reviewed-by <label> [--json]` | Reject one pending candidate without reopening it on later generation. |

Completed ingestion and explicit replay automatically bootstrap otherwise unmatched
items as isolated one-item canonical products, so ordinary non-GTIN retailer SKUs are
queryable without waiting for a human merge. Isolation is representation, not an
equivalence claim. Exact normalized non-GTIN identifiers and all structured/fuzzy
scores remain review candidates. Acceptance, rejection, and automatic supersession
record a required bounded auditor label and database timestamp. These mutation
operations are administrative CLI capabilities; the HTTP API and MCP server remain
read-only.

## Retailer, store, product, price, availability, and promotion queries

| Command | Behavior |
|---|---|
| `makolet retailers list [--limit 50] [--cursor ...] [--json]` | List configured retailer legal groups in stable UUID order. |
| `makolet stores find [--query ...] [--retailer-id UUID] [--city ...] [--limit 50] [--cursor ...] [--json]` | Find portal-scoped stores by normalized name/address text, retailer, and/or city. |
| `makolet products search <query> [--limit 50] [--cursor ...] [--json]` | Normalized Hebrew/Latin/mixed canonical and matched-retailer alias search, exact base-unit quantity ranking (`1 L = 1000 ml`, `1 kg = 1000 g`), and exact barcode evidence ranking. The product-text portion remains required and numeric substrings never count as quantity matches. Retailer-scoped provisional barcodes are labeled separately from independently corroborated global identifiers. |
| `makolet products get <product-uuid> [--json]` | Canonical product details and identifiers; not found exits 3. |
| `makolet products find-barcode <barcode> [--json]` | Exact unscoped validated numeric-barcode lookup with issuer provenance; ambiguity is a structured error and not found exits 3. |
| `makolet products find-retailer-item <retailer-uuid> <item-code> [--portal-id UUID] [--json]` | Exact normalized item-code lookup. A retailer-wide collision across source portals fails with `domain_validation_error` until `--portal-id` is supplied; not found exits 3. |
| `makolet prices current <product-uuid> [--retailer-id UUID] [--store-id UUID] [--limit 50] [--cursor ...] [--json]` | Current matched prices, cheapest first, with source-file/archive and portal provenance. |
| `makolet prices compare <product-uuid> [--retailer-id UUID] [--limit 50] [--cursor ...] [--json]` | Compare current prices across stores and, unless filtered, retailers in the same deterministic cheapest-first order. |
| `makolet prices history <product-uuid> [--store-id UUID] [--since ...] [--until ...] [--limit 200] [--cursor ...] [--json]` | Newest-first validity rows overlapping the half-open range; supply both aware bounds or neither, in which case the cursor-pinned trailing 366 days are used; limit 1–1,000 and maximum explicit span 3,660 days. |
| `makolet promotions active [--product-id UUID] [--store-id UUID] [--at ...] [--limit 50] [--cursor ...] [--json]` | Promotion versions active at the selected instant (UTC now by default), including bounded deterministic items/stores/clubs and source provenance. |
| `makolet promotions history [--product-id UUID] [--store-id UUID] [--since ...] [--until ...] [--limit 200] [--cursor ...] [--json]` | Past promotion versions overlapping the same paired-or-default cursor-pinned history window, with each version's own bounded relations and provenance. |
| `makolet availability current <product-uuid> [--store-id UUID] [--limit 50] [--cursor ...] [--json]` | Current per-store availability with source-file, portal, and observation provenance. |

The public commands still accept `--limit 1..200`; price and promotion history still
accept `--limit 1..1000`. Before publisher-controlled fields are fetched, the shared
query service uses the following narrower parent-row materialization caps:

| Command family | Maximum materialized parent rows per page |
|---|---:|
| `retailers list` | 50 |
| `stores find` | 5 |
| `products search` | 5 |
| `prices current` and `prices compare` | 28 |
| `prices history` | 28 |
| `promotions active` and `promotions history` | 1 |
| `availability current` | 28 |
| `freshness` | 50 |
| `source-status` and the source page in `status` | 32 |

A larger accepted limit can therefore produce a shorter non-final page. In JSON
output, a non-null `next_cursor` is the ordinary deterministic continuation and must
be followed even when fewer rows than requested were returned. Values below the
route cap are honored unchanged. Each returned promotion parent includes at most
seven ordered items, seven stores, and seven clubs (21 children total); the
`returned_*_count` values and `*_truncated` flags report the returned cardinality and
whether additional child relations exist. Child lists do not have separate cursors.

Examples against the deterministic demo:

```text
uv run makolet products search 7290000000015 --json
uv run makolet products get 77777777-7777-7777-7777-777777777777 --json
uv run makolet products find-barcode 7290000000015 --json
uv run makolet products find-retailer-item 22222222-2222-2222-2222-222222222222 SKU-1 --portal-id 33333333-3333-3333-3333-333333333333 --json
uv run makolet prices current 77777777-7777-7777-7777-777777777777 --json
uv run makolet prices compare 77777777-7777-7777-7777-777777777777 --json
uv run makolet availability current 77777777-7777-7777-7777-777777777777 --json
```

Passing only one of `--since` or `--until` is a validation error before PostgreSQL
work starts. Current price/comparison, availability, and history commands page exact
canonical-product projections in response order before loading portal and source
provenance; no matched-item/store fan-out is silently truncated.

## Servers

```text
uv run makolet api serve [--host 127.0.0.1] [--port 8000]
uv run makolet mcp serve --transport stdio
uv run makolet mcp serve --transport http [--host 127.0.0.1] [--port 8001]
```

Stdio MCP reserves stdout for newline-delimited JSON-RPC and sends JSON logs to
stderr. HTTP MCP serves `POST /mcp` and applies the configured origin allowlist. See
[HTTP API](api.md) and [MCP](mcp.md).

## Parquet export

```text
uv run makolet export parquet <output-directory> [--since ...] [--until ...] [--json]
```

The command opens one PostgreSQL repeatable-read, read-only snapshot and exports
`current_prices` whose `last_observed_at` is inside the inclusive aware timestamp
range plus `price_history` intervals that overlap it. Output uses Hive-style
`entity=.../retailer_id=.../date=.../dataset=<sha256>/part-*.parquet` generations and
one canonical `_manifest.json` per partition. Identical reruns are idempotent; a
different dataset at an existing partition fails rather than overwriting it. A single
invocation has one aggregate budget across both entities and every partition: at
most 1,000 partitions, 10,000,000 rows, 1,000 files, 2 GiB of cumulative spool
input, 64 GiB of Parquet output, and 3,600 seconds including reserved cleanup time.
The repeatable-read snapshot is preflighted for exact total rows and the minimum file
count before the first partition is published. That minimum is the sum of each
partition's row-count ceiling at the configured rows-per-file bound, so rows in one
partition cannot subsidize files required by another. Prospective spool, file, and
output charges fail before the affected partition's manifest is committed. A later
dynamic limit failure retains any earlier immutable partition manifests and reports
that partial publication explicitly. Each synchronous Parquet write and publication
runs in a supervised, killable child process under the operation deadline rather
than an abandoned thread. Before generation rename or manifest commit, that child
durably records the exact operation identity, manifest, and prior-generation state.
After either a normal child result or termination, a second bounded child reconciles
that journal: it recognizes an exact completed manifest for the publication ledger,
or removes only the operation's staging, unpublished generation, and known temporary
files. The journal is removed last. The operation reserves at least four seconds for
recovery when its total budget permits, capped at half of short budgets and 30
seconds overall. Supervised work, child termination, reconciliation, and cleanup
share that wall-clock budget. Synchronous operating-system process creation cannot
be preempted and may add platform startup latency before the async watchdog regains
control; every wait after launch remains bounded. The command result records the
PostgreSQL snapshot identifier
and transaction start time. Schema version 2 rows carry source-file, portal,
document/timestamp, and exact archive SHA-256 provenance so the dataset remains
auditable without joining back to the live database.

## Benchmarks

```text
MAKOLET_BENCHMARK_DATABASE_CONFIRM=makolet_benchmark
uv run makolet benchmark run --quick
uv run makolet benchmark run --standard
```

The quick profile is a bounded development smoke and is not scale-acceptance
evidence. The standard profile runs the measured acceptance scenarios and may require
`MAKOLET_BENCHMARK_DATABASE_URL` plus exact
`MAKOLET_BENCHMARK_DATABASE_CONFIRM=makolet_benchmark`; it records its result artifact rather than turning
measurement into a normal test. See [testing](testing.md#benchmarks) and
[performance](performance.md).

## Exit codes

| Code | Meaning |
|---:|---|
| 0 | Operation completed (including a page with no results). |
| 1 | Non-retryable/runtime/unexpected failure. |
| 2 | Invalid CLI argument or value. |
| 3 | Requested entity was not found. |
| 4 | Retryable application/source failure. |
| 5 | Invalid configuration, failed `doctor`, or unavailable runtime capability. |
| 130 | Interrupted by the operator. |

JSON errors are written to stderr as
`{"error":{"code":"...","message":"..."}}`. Unexpected failures return a safe
generic message; inspect structured stderr logs without exposing them to an untrusted
caller.

## Configuration boundary

Settings load from environment and optional UTF-8 `.env`, with the `MAKOLET_` prefix.
Invalid settings fail before an operation and are not echoed back. Paths resolve to
absolute paths; S3 URLs reject embedded credentials/query/fragment; worker IDs,
origins, buckets, source IDs, limits, and bind values are validated. The supported
variables and incident implications are summarized in the
[operations runbook](operations-runbook.md#configuration).
