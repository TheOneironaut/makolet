# Operations runbook

This runbook covers runtime diagnosis and safe application actions. Container startup,
resource limits, and network topology are in [deployment](deployment.md); destructive
database/archive restore steps are intentionally isolated in
[backup and recovery](backup-and-recovery.md).

## First checks

For a host process:

```text
uv run makolet database status --json
uv run makolet doctor --json
uv run makolet status --limit 50 --json
uv run makolet freshness --limit 50 --json
uv run makolet failures --limit 50 --json
```

For Compose:

```text
docker compose ps
docker compose logs --since 15m api worker migrate postgres seaweedfs
```

`/healthz` proves only that the API process responds. `/readyz` performs a PostgreSQL
health query and requires the database's complete Alembic revision set to exactly
match the repository head set. `doctor` checks PostgreSQL, initializes/verifies the
configured archive, and confirms runtime capability wiring; it does not contact
publisher portals or prove data freshness. Use `sources test <id>` for one bounded
listing request.

## Configuration

Settings are case-insensitive `MAKOLET_*` environment variables or UTF-8 `.env`
entries. Secret values are Pydantic `SecretStr` and are revealed only at database/S3
client boundaries. Never pass secrets on a CLI command line or embed them in URLs.

### Database and storage

| Variable | Default | Constraint / effect |
|---|---|---|
| `MAKOLET_ENVIRONMENT` | `development` | `development`, `test`, or `production`; production rejects bundled development database/S3 credentials. |
| `MAKOLET_DATABASE_URL` | local PostgreSQL URL | PostgreSQL/asyncpg URL with host and database. Production remote hosts require `sslmode=verify-full` or `ssl=verify-full`; other or missing TLS modes fail closed. |
| `MAKOLET_DATABASE_ALLOW_INSECURE_LOCAL` | `false` (`true` in development Compose) | In production, explicitly permits plaintext only to a literal loopback IP. DNS labels such as `postgres` and `localhost` require verified TLS. |
| `MAKOLET_DATABASE_POOL_SIZE` | `10` | 1–100 connections. |
| `MAKOLET_DATABASE_MAX_OVERFLOW` | `20` | 0–200 overflow connections. |
| `MAKOLET_DATABASE_STATEMENT_TIMEOUT_MS` | `30000` | 100–600000 ms, applied per connection. |
| `MAKOLET_ARCHIVE_BACKEND` | `local` | `local` or `s3`; immutable behavior is shared. |
| `MAKOLET_ARCHIVE_ROOT` | `raw-archive` | Local objects and persistent-volume FTP/S3/parser spools. |
| `MAKOLET_ARCHIVE_MAXIMUM_OBJECT_BYTES` | `2147483648` | 1 KiB–16 GiB exact-object ceiling. |
| `MAKOLET_ARCHIVE_MINIMUM_FREE_BYTES` | `1073741824` | Local archive and FTP/S3/XML/ZIP spool free-space floor. Positive-reserve Makolet processes sharing the archive root serialize each bounded write through `.makolet-capacity.lock`; zero disables only this reserve, not byte quotas. |
| `MAKOLET_S3_ENDPOINT` | `http://127.0.0.1:8333` | Absolute credential-free HTTP(S) URL. An active production S3 backend requires HTTPS except for the explicit exact-local exception below. |
| `MAKOLET_S3_ALLOW_INSECURE_LOCAL` | `false` (`true` in the development Compose example) | In production, explicitly permits HTTP only for a literal loopback IP using path-style addressing and disables ambient HTTP(S) proxies for that connection. DNS labels such as `seaweedfs` and `localhost` require HTTPS; a bucket-prefixed virtual host is never authorized. |
| `MAKOLET_S3_BUCKET` | `makolet-raw` | Valid S3-style bucket. |
| `MAKOLET_S3_REGION` | `us-east-1` | Non-empty region string. |
| `MAKOLET_S3_ACCESS_KEY`, `MAKOLET_S3_SECRET_KEY` | empty | Both required when backend is `s3`. |
| `MAKOLET_S3_KEY_PREFIX` | `raw` | Canonical path prefix; no empty/dot/parent parts. |
| `MAKOLET_S3_PATH_STYLE` | `true` | Path-style S3 addressing when true; required by the production plaintext-local exception. |
| `MAKOLET_EXPORT_ROOT` | `exports` | Dedicated bounded export-spool root; Compose mounts `/var/lib/makolet/exports`, and the CLI output argument selects a dataset destination explicitly. |

Compose-only `MAKOLET_POSTGRES_PORT` and `MAKOLET_S3_PORT` select loopback host ports.
`MAKOLET_BIND_ADDRESS` controls only the published API and optional Prometheus ports;
it cannot widen PostgreSQL or S3 beyond `127.0.0.1`. These are not application
settings.

For a database outside the bundled Compose network, provision a certificate whose
identity matches the URL hostname and a trusted CA chain, then select
`sslmode=verify-full` (the runtime canonicalizes it to asyncpg's `ssl=verify-full`).
`verify-ca` is insufficient because it does not verify the hostname. Do not enable
`MAKOLET_DATABASE_ALLOW_INSECURE_LOCAL` for remote deployments: the validator ignores
that exception unless the URL host is exactly `postgres`, `localhost`, `127.0.0.1`,
or `::1`; URLs using `host`, `service`, or `servicefile` query overrides are never
eligible for the exception.

For an S3-compatible service outside the bundled Compose network, configure an
`https://` endpoint and leave `MAKOLET_S3_ALLOW_INSECURE_LOCAL=false`. Production
rejects remote HTTP even when that flag is true. The same validation runs before the
application runtime, demo seed, or raw-archive backup/restore tool creates an S3
client, so an operational helper cannot bypass the runtime policy. Production local
HTTP also rejects virtual-host addressing because it would change the actual request
authority from the exact approved endpoint to `<bucket>.<host>`.

Runtime S3 reads use a 300-second total operation deadline in addition to SDK
connection/read timeouts. Each parser-facing read is first written to the bounded
archive spool, checked through EOF against the content-addressed SHA-256 key and
S3-declared/actual byte length, and only then exposed through that exact verified
file handle. The parser-facing iterator hashes and counts that same handle again,
drains it if a parser returns early, and cannot leave its context successfully until
EOF still matches the expected digest and length. Deadline expiry or cancellation
closes the response body and removes the partial spool; an equivocated second GET
therefore fails before parsing, staging, or apply.

### Collection, apply, and interfaces

| Variable | Default | Constraint / effect |
|---|---|---|
| `MAKOLET_SOURCE_LISTING_TIMEOUT_SECONDS` | `20` | Positive, at most 300 seconds. |
| `MAKOLET_SOURCE_DOWNLOAD_TIMEOUT_SECONDS` | `600` | Positive, at most 86400 seconds; total wall deadline for HTTP artifacts and FTP/FTPS transfers. |
| `MAKOLET_ALLOW_INSECURE_FTP` | `false` | Plain FTP discovery/download stays blocked unless the operator explicitly accepts unauthenticated transport; FTPS is unaffected. |
| `MAKOLET_INGESTION_DISCOVERY_PAGE_SIZE` | `100` | 1–500 remote files per adapter page. |
| `MAKOLET_INGESTION_MAXIMUM_FILES_PER_SOURCE_RUN` | `10000` | 1–10000 eligible files archived/applied per run. A separate fixed 100,000-record traversal ceiling advances durably across large out-of-range or already-terminal catalog prefixes. |
| `MAKOLET_INGESTION_MINIMUM_FULL_RECORDS` | `1` | 0–10 million; minimum complete snapshot count. |
| `MAKOLET_INGESTION_MAXIMUM_CHARGED_BYTES_PER_SOURCE_RUN` | `8589934592` | Charged-byte ceiling for one source attempt, including immutable archive bytes and failed/retried transfer bytes; must cover the object ceiling plus the FTP/FTPS control, address-attempt, and TLS-framing reservation and 64 KiB final-frame headroom. Compose defaults to 4 GiB. |
| `MAKOLET_INGESTION_MAXIMUM_CHARGED_BYTES_PER_SOURCE_DAY` | `34359738368` | Durable rolling-24-hour charged-byte ceiling per retailer source, including conservative in-flight reservations until they settle or expire; must be at least the run ceiling. Compose defaults to 8 GiB. |
| `MAKOLET_INGESTION_MAXIMUM_SOURCE_IDENTITIES_PER_SOURCE_DAY` | `2000` | Independent rolling source-identity ceiling per retailer. |
| `MAKOLET_INGESTION_MAXIMUM_TRANSFER_ATTEMPTS_PER_SOURCE_DAY` | `4000` | Independent rolling transfer-attempt ceiling; must be at least the identity ceiling. |
| `MAKOLET_INGESTION_MAXIMUM_SUCCESSES_PER_SOURCE_DAY` | `2000` | Independent rolling successful-archive ceiling. |
| `MAKOLET_INGESTION_MAXIMUM_VALIDATION_ISSUES` | `100000` | Exact cumulative warning/rejection/quarantine count ceiling per ingestion or replay attempt. |
| `MAKOLET_INGESTION_MAXIMUM_VALIDATION_ISSUE_BYTES` | `67108864` | Exact cumulative logical UTF-8 validation-issue byte ceiling per attempt. |
| `MAKOLET_INGESTION_MAXIMUM_VALIDATION_ISSUE_EVIDENCE` | `1000` | Maximum persisted evidence samples per attempt; exact counts and bytes remain in the run summary. |
| `MAKOLET_INGESTION_MAXIMUM_FULL_SNAPSHOT_DROP_FRACTION` | `0.50` | `[0,1)` maximum accepted count drop. |
| `MAKOLET_API_HOST`, `MAKOLET_API_PORT` | `127.0.0.1`, `8000` | API bind; CLI flags can override. |
| `MAKOLET_MCP_HOST`, `MAKOLET_MCP_PORT` | `127.0.0.1`, `8001` | HTTP MCP bind; stdio ignores these. |
| `MAKOLET_MCP_ALLOWED_ORIGINS` | `[]` | JSON array of exact HTTP(S) origins; no path/query/credentials. |
| `MAKOLET_MCP_HTTP_BODY_TIMEOUT_SECONDS` | `10` | Total HTTP request-body read deadline, 0–300 seconds exclusive of zero. |
| `MAKOLET_MCP_HTTP_MAXIMUM_CONCURRENCY` | `100` | 1–10000 Uvicorn concurrent connections/tasks for MCP HTTP. |
| `MAKOLET_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR`. |

### Worker

| Variable | Default | Effect |
|---|---|---|
| `MAKOLET_WORKER_ID` | `<hostname>-<pid>` | Owner/telemetry identity, max 200 safe characters. |
| `MAKOLET_WORKER_METRICS_HOST` | `127.0.0.1` | Bind address for the continuous worker's health/metrics server; Compose sets `0.0.0.0`. |
| `MAKOLET_WORKER_METRICS_PORT` | `9100` | Port for the continuous worker's health/metrics server. |
| `MAKOLET_WORKER_CONCURRENCY` | `4` | 1–64 fixed consumers. |
| `MAKOLET_WORKER_QUEUE_CAPACITY` | `64` | 1–10000 bounded jobs. |
| `MAKOLET_WORKER_MAXIMUM_SOURCES` | `1000` | Maximum configured schedules. |
| `MAKOLET_WORKER_HEARTBEAT_SECONDS` | `30` | Metrics heartbeat cadence. |
| `MAKOLET_WORKER_POLL_SECONDS` | `1` | Scheduler resolution. |
| `MAKOLET_WORKER_STALE_AFTER_SECONDS` | `7200` | Active job age before recovery. |
| `MAKOLET_WORKER_STALE_RECOVERY_SECONDS` | `900` | Recovery scan cadence. |
| `MAKOLET_WORKER_SHUTDOWN_GRACE_SECONDS` | `30` | Queue drain before cancellation. |
| `MAKOLET_WORKER_JITTER_RATIO` | `0.10` | Positive scheduling jitter, 0–1 of interval. |
| `MAKOLET_ENABLED_SOURCES` | `[]` | JSON list; if non-empty, every item needs an interval. |
| `MAKOLET_SOURCE_INTERVALS_SECONDS` | `{}` | JSON object from source ID to 1–604800 seconds. |

NCR FTP/FTPS credentials use the per-feed variables documented in the
[source-adapter guide](source-adapter-guide.md#url-and-listing-safety).
Setting `MAKOLET_ALLOW_INSECURE_FTP=true` does not make FTP authentic: archived
transport provenance remains `unauthenticated`, and a network-path attacker could
substitute bytes. Prefer an authenticated publisher endpoint whenever one exists.

## Normal worker operation

Start one configured continuous worker:

```text
uv run makolet ingest worker
```

Run a bounded maintenance cycle without scheduling:

```text
uv run makolet ingest worker --source shufersal --once --json
```

The worker recovers stale jobs before work, de-duplicates selected source IDs, never
queues the same source while it is pending/in-flight, and catches a source failure so
other sources continue. One-shot mode renders every outcome, then exits `4` when one
or more sources failed (including mixed success/failure) and `0` only when all sources
succeeded. Automation may use the process status while retaining the JSON summary for
per-source diagnosis.

SIGINT/SIGTERM stops new scheduling and drains the queue up to the grace period.
Cancelled in-flight work is recorded in process health as `worker_shutdown`; durable
shutdown uses one shared grace budget. A daemon watchdog stays armed until the outer
event loop and runtime resources close; cancellation-ignoring work beyond the grace
plus bounded cleanup allowance forces a temporary-failure process exit.
Worker signal ownership remains outside Uvicorn, so a metrics request or lifespan
hook that stalls during shutdown cannot delay watchdog arming.
active ingestion states are later eligible for stale recovery.

## Metrics and logs

The API process serves `/healthz`, `/readyz`, and `/metrics` on its API port. A
continuous worker serves process liveness at `/healthz` and its own registry at
`/metrics` on `MAKOLET_WORKER_METRICS_HOST:MAKOLET_WORKER_METRICS_PORT`. `--once`
does not start that HTTP server. Compose health-checks the worker endpoint, and the
monitoring profile scrapes `api:8000/metrics` and `worker:9100/metrics` separately.
Compose does not publish port 9100 to the host.

Metric families use the `makolet_` prefix:

- counters: `ingestion_completed_total`, `ingestion_archive_deduplicated_total`,
  `ingestion_replay_completed_total`, `ingestion_failure_total`,
  `ingestion_parser_failure_total`, `ingestion_files_downloaded_total`,
  `ingestion_records_staged_total`, `ingestion_records_rejected_total`, and
  `ingestion_warnings_total`;
- histograms: `ingestion_duration_seconds`, `ingestion_download_duration_seconds`,
  `ingestion_parsing_duration_seconds`, `ingestion_database_apply_duration_seconds`,
  and `ingestion_file_bytes`;
- gauges: `source_freshness_timestamp_seconds`, `worker_heartbeat_timestamp_seconds`,
  `worker_active_sources`, and labeled `source_healthy`.

Metrics have fixed label sets to prevent arbitrary series creation. Registries remain
process-local: scheduled ingestion, worker heartbeat/source-health, and successful
collection freshness samples appear on the worker endpoint; API-process activity
appears on the API endpoint. Persisted `status` and `freshness` remain the durable
operational view across restarts.

Logs are JSON lines on stderr. Stable discovery-through-apply, replay/rebuild, and
worker lifecycle names carry bounded correlation and entity identifiers, counts,
durations, states, and error codes. Lifecycle events exclude URLs, filenames, raw
data, credentials, and exception messages. The shared renderer redacts secret fields,
escapes all ASCII control characters, bounds nested values, and suppresses traceback
text. See [observability](observability.md) for the complete event and field contract.
Never weaken redaction to make a source easier to debug; record safe metadata and reproduce
with a scrubbed fixture.

## Incident playbooks

### API not ready or database unavailable

1. Check `docker compose ps postgres migrate api` and PostgreSQL logs.
2. Run `makolet database status --json`; do not run a migration repeatedly without
   reading the first failure.
3. Confirm the DSN points to the expected database and contains no shell/URL quoting
   mistakes. Do not print it.
4. Inspect `schema_ready`, `expected_migration_heads`, and
   `current_migration_revisions`. If the schema is absent or behind, run the documented
   migration command. Treat an unknown/divergent revision as an incident rather than
   stamping it manually. If data is damaged, stop writers and use
   [backup and recovery](backup-and-recovery.md).

Revision `0011_resource_probe_budgets` installs the trusted `btree_gist` extension,
rewrites the stored range columns on `price_history` and `promotions`, and builds GiST
indexes under ordinary DDL locks. Before upgrading a populated database, verify the
role can create trusted extensions, inspect both table sizes, and reserve a maintenance
window with sufficient WAL and temporary capacity. Validation and collection legacy
backfills are keyset/bucketed in 10,000-row batches, but the generated-column rewrite
and index builds necessarily visit each existing temporal row once.

Collection day accounting reads at most the fixed five-minute bucket window rather
than summing the complete audit ledgers. Boundary buckets conservatively cover up to
five extra minutes; archive/transfer ledgers remain the exact byte and idempotence
record. `identity_day_limit`, `attempt_day_limit`, and `success_day_limit` identify the
independent cardinality ceiling that stopped a traversal.

### Archive unavailable or integrity failure

1. Stop ingestion workers; queries can remain online if PostgreSQL is healthy.
2. Run `makolet doctor --json`. For S3, check endpoint/bucket reachability and scoped
   credentials without listing secrets.
3. Do not overwrite, delete, rename, or hand-edit a SHA-256 object. Use archive backup
   verification procedures. Backup and restore use the same canonical
   `MAKOLET_S3_KEY_PREFIX` as the runtime and fail closed if a manifest key escapes it
   or its digest shards disagree with the content hash.
4. A missing/hash-mismatched object blocks replay. Restore the exact object from a
   verified backup; never substitute re-downloaded bytes under the old key.

### Source failure or staleness

1. Compare `makolet status`, `freshness`, and `failures`; distinguish transport state
   from last good operational data. In `source-status`, inspect the latest collection
   attempt's operation, generation, processed/discovered/warning counts, truncation,
   safe error, and finish time before retrying.
2. Run exactly one `makolet sources test <id> --json`. Avoid repeated probes during a
   publisher outage/rate limit.
3. Check the source coverage limitation and whether credentials are intentionally
   configured. Never bypass access controls or switch to an unreviewed mirror.
4. Retry only retryable failures after the external condition changes. Record a new
   dated live observation in the coverage matrix; never mark a failed smoke green.

### Quarantined or suspicious full snapshot

1. Use `quarantine inspect <UUID> --json`; preserve its archive key/hash and issues.
2. Do not lower minimum-count/drop thresholds globally merely to force apply.
3. Reproduce with archived bytes or a legal scrubbed fixture; fix source-independent
   parsing only when the bytes prove a format variation.
4. After a reviewed parser/config fix, run `ingest replay <UUID> --json`. Confirm
   counts and history; replay does not redownload.

### Worker appears stuck

1. Confirm process/container state and recent structured logs.
2. Inspect active source-file statuses and PostgreSQL advisory-lock sessions through
   supported read-only diagnostics. Per-file ownership is a session lock, not a
   takeover row; do not terminate its database session while its worker may be alive.
3. Stop gracefully and wait for the configured grace. Normal cancellation unlocks;
   process/connection death releases the lock. On restart, stale recovery moves only
   unlocked abandoned states to `failed_retryable` after the configured age.
   The next source collection resumes at its last durable file boundary; do not edit
   collection cursors or legacy lease rows manually.
4. If the same file repeatedly stalls, preserve its archive and diagnose bounded
   parser/database behavior before increasing timeouts or concurrency.

### Duplicate or unexpected history

1. Compare the source file's `(portal, remote_id)`, archive SHA-256, and
   `applied_source_contents` entry.
2. A duplicate result should have no stage/apply summary and no new current/history
   rows. An unchanged observation may advance `last_observed_at` but must not open
   another history interval.
3. Stop writers before manual database investigation. Do not repair by deleting
   idempotence rows; preserve evidence and add a regression test for any code defect.

### Normalized-state rebuild

Use this procedure only when the normalized read model must be regenerated from the
already archived, previously applied source bytes. It is an in-place destructive
maintenance operation, not a backup/restore mechanism or a shadow-schema swap.

1. Schedule ingestion downtime and stop all ordinary workers. Block or warn external
   query consumers that results will be partial during the run. Take and verify a
   coordinated PostgreSQL plus raw-archive backup using the
   [backup and recovery](backup-and-recovery.md) procedure.
2. Confirm `makolet status --json` reports no active rebuild. Record the current
   schema revision and operator/change reference. Do not start if any archived object
   required by an applied or archive-only source is missing or its backup is
   unverified. Verify PostgreSQL has capacity for a durable snapshot approximately
   the size of the affected normalized catalog/read model; every snapshot row is
   bounded to a 512-byte key and 1 MiB JSON object, but total rows scale with the
   database.
3. Start with the literal acknowledgement and a non-secret audit label:

   ```text
   uv run makolet ingest rebuild-normalized --confirm REBUILD-NORMALIZED-STATE --requested-by change-1234 --json
   ```

   Any other confirmation fails before database mutation. In one barrier transaction
   the command snapshots stable identities, exact logical rows, reviewed decisions,
   and the ordered source-file work list; it then resets only archive-derived state
   and synchronously replays the archive. It makes no source-network requests.
4. Monitor `makolet status --json`, `GET /api/v1/status`, or MCP
   `get_source_status`. `maintenance.active=true` means ordinary ingest/apply is
   blocked and public normalized queries may be incomplete. Inspect a specific run
   with `makolet ingest rebuild-status <run-uuid> --json`.
5. If the process stops, keep workers stopped and inspect the durable safe error and
   replay records. Repair the archive/parser/database cause, then resume exactly the
   same run:

   ```text
   uv run makolet ingest resume-rebuild <run-uuid> --json
   ```

   Do not start another rebuild or clear the control row by hand. A failed run keeps
   the barrier active by design; completed files are checkpointed and skipped.
6. Completion runs in one transaction. For an unchanged parser it must reconcile and
   prove exact UUID, value, provenance, observation timestamp, interval, promotion,
   apply-ledger, watermark, and reviewed-decision equality before it removes snapshots
   and the barrier. A conflict rolls that transaction back and leaves maintenance
   active. Validate counts, representative retailer-scoped
   item/barcode/search/price/history queries, saved pre-rebuild cursors, a deterministic
   Parquet comparison when used operationally, and `maintenance.active=false` before
   restarting workers and query consumers.

The reset includes archive-derived price/availability current and history rows,
promotions and memberships, retailer identifier assertions, system-derived exact
identifier/match projections, source-scope watermarks, staging rows, and
`applied_source_contents`. It preserves stores/aliases, retailer items, canonical
products and identifier groups, curated identifiers, accepted/rejected/superseded
candidate decisions, manual/isolated confirmations, retailer/portal registry,
`source_files`, immutable raw objects, ingestion runs/events, validation issues,
replay runs, and rebuild audit rows. Replay reconnects derived evidence to those
stable catalog identities and creates an isolated representation for an archive-only
non-GTIN item before checkpointing that file; reviewed canonical fields are never
replaced from raw replay.

Snapshot retention is deterministic. A parser-unchanged run with no archive-only
expansion deletes its original and rebuilt snapshots only after exact equivalence.
Failed/interrupted runs retain the original snapshot until that same run resumes.
Parser-correction and archive-expansion runs retain original rows indefinitely with
`preserved`/`superseded` outcomes; rebuilt-phase scratch rows are removed. There is no
automatic time-based purge. Treat retained rows as audit evidence in backup sizing and
retention policy; migration `0008_stable_rebuild_snapshots` refuses downgrade while
any remain. Retiring a completed rebuild run and its cascading snapshots is a separate
reviewed database-retention change, never part of rebuild resume or automatic cleanup.

If any file's recorded parser version differs from the current parser, the run is a
parser correction. Exact export equality is then not expected. Review every
superseded outcome and downstream export before reopening reads; the old normalized
evidence remains in the snapshot overlay rather than being erased.

### Catalog candidate review

1. Ordinary completed ingestion creates idempotent isolated mappings automatically.
   Use `makolet products find-retailer-item` to verify an item is queryable before any
   equivalence decision.
2. Run `makolet matching generate --json` page by page, preserving each returned
   cursor. Item and candidate limits are independently capped at 200; lowering them is
   appropriate during database pressure.
3. Inspect every proposed merge. Treat exact retailer codes as issuer-scoped even
   when their normalized text matches across retailers. Check explanation/features
   against source provenance; do not accept because a fuzzy score merely looks high.
4. Accept or reject with a non-secret change/ticket/auditor label. An accept that now
   conflicts with a manual or non-isolated mapping fails closed; re-inspect rather
   than editing match rows directly.
5. Audit completed work with `matching list --status accepted|rejected|superseded`.
   Later generation updates pending evidence only and never silently reopens a final
   decision.

## Routine maintenance

- Run verified PostgreSQL and archive backups together; raw objects without metadata
  and metadata without objects are not a complete recovery point.
- Verify checksums and restore into a staging database/bucket before swapping service.
- Run a bounded Parquet export for portable analysis; it is not a backup replacement.
- Review failure/quarantine/freshness trends and scheduled live-smoke results.
- Apply PostgreSQL/SeaweedFS/Python dependency security updates through the lock,
  image-pin, license, migration, and full test process.
