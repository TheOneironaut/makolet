# Observability

Makolet emits one JSON object per stderr line and exposes bounded Prometheus metrics.
The logging pipeline is shared by CLI, API, MCP, and worker runtimes: application
lifecycle events use structlog, and standard-library log records pass through the same
JSON renderer and safety processors. Configure the minimum level with
`MAKOLET_LOG_LEVEL` (`DEBUG`, `INFO`, `WARNING`, or `ERROR`).

## Lifecycle events

The `event` field is a stable machine-readable name. Event names and their normal
order are:

| Boundary | Events |
|---|---|
| Source discovery | `discovery.started`, `discovery.page_completed`, `discovery.completed`, `discovery.failed` |
| Download and archive | `download.started`, `download.completed`, `download.failed`, `archive.stored`, `archive.deduplicated` |
| Parse, stage, and apply | `parse.started`, `parse.completed`, `parse.failed`, `stage.started`, `stage.reused`, `stage.completed`, `stage.failed`, `apply.started`, `apply.completed`, `apply.failed` |
| File outcome | `ingestion.completed`, `ingestion.quarantined`, `ingestion.failed` |
| Replay | `replay.started`, `replay.completed`, `replay.failed`, `replay.range_started`, `replay.range_completed`, `replay.range_failed` |
| Normalized rebuild | `rebuild.started`, `rebuild.progress`, `rebuild.completed`, `rebuild.failed` |
| Worker | `worker.run_started`, `worker.run_completed`, `worker.run_failed`, `worker.source_started`, `worker.source_completed`, `worker.source_failed`, `worker.source_stopped`, `worker.heartbeat`, `worker.recovery_completed`, `worker.recovery_failed`, `worker.shutdown_started`, `worker.shutdown_completed` |

Every record also has `timestamp`, `level`, and `logger`. Applicable lifecycle records
carry bounded identifiers such as `correlation_id`, `run_id`, `worker_run_id`,
`source_run_id`, `source_id`, `retailer_id`, `portal_id`, `source_file_id`,
`replay_id`, and `rebuild_run_id`. Counts, durations, state tokens, and boolean flags
are the only other lifecycle fields. Examples include `page_file_count`,
`content_length`, `accepted_records`, `rejected_records`, `inserted_count`,
`queue_depth`, `duration_seconds`, `status`, and `error_code`.

Collection creates a run context and propagates it through download, archive, parse,
stage, and apply. An existing API request correlation ID is retained. Worker source
runs retain their worker-run identifier while receiving an isolated source-run
correlation. Replay and rebuild events retain source-file and maintenance-run
identities. Context managers restore their prior values on exit, so identifiers do
not leak into later requests or source runs.

`archive.deduplicated` covers both content-addressed create-if-absent reuse and an
existing archive object verified for retry or replay. The `created` and `duplicate`
flags distinguish these cases. Phase completion events contain measured counts and
durations; a start without its matching completion should be followed by a classified
failure, quarantine, worker-stop, or process termination.

## Logging safety boundary

Lifecycle logging uses an explicit field allowlist. It never accepts or emits source
URLs, filenames, object keys, content hashes, credentials, raw records, downloaded
bytes, rejected source values, operator text, or exception messages. Failures expose
only a stable `error_code` and lifecycle `status`; inspect durable quarantine,
failure, replay, or rebuild audit records for approved diagnostics.

The shared renderer additionally:

- redacts credential, authorization, cookie, password, secret, and token fields;
- redacts URL user information and common secret query parameters, including Azure
  SAS `sig`, signature, token, key, password, secret, credential, and authorization
  variants, in non-lifecycle diagnostic messages. Query-key classification is
  case-insensitive and recognizes percent-encoded names without decoding or
  rewriting unrelated values; malformed or overlong query keys fail closed;
- replaces byte values with their byte count and bounds strings, collections, and
  nesting;
- escapes ASCII/C1 controls, surrogates, Unicode format controls, and line/paragraph
  separators, including CR, LF, NUL, tab, escape, and delete, so one event remains
  exactly one physical JSON line; and
- suppresses traceback and stack text rather than serializing exception strings.

Reconfiguration replaces the one process-local root handler and structlog loggers are
not cached against an old output stream. This prevents duplicate lines and stale
handler leakage in long-lived processes and tests. Do not bypass the lifecycle facade
or bind unreviewed source metadata merely to make debugging more convenient.

## Metrics and operational use

API and continuous-worker Prometheus registries are deliberately process-local. The
API serves `/metrics` on its API port; the worker serves its own `/metrics` and
`/healthz` on the configured worker metrics address. Persisted status and freshness
queries remain the durable view across restarts. Source status includes the latest
durable collection attempt and its committed boundary/counts beside latest and
last-good file state. Metric names, labels, and incident
procedures are listed in the [operations runbook](operations-runbook.md#metrics-and-logs).

Typical local inspection keeps each JSON line intact:

```text
docker compose logs --since 15m worker
uv run makolet failures --limit 50 --json
uv run makolet quarantine inspect <source-file-uuid> --json
```

Filter on `correlation_id` or a durable UUID instead of searching for publisher URLs
or source values. A failed worker run can still contain completed source runs, while
a bounded one-shot worker reports each isolated source outcome explicitly.
