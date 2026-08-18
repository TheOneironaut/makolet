# Makolet documentation

Start with the path that matches the work you are doing.

## Use and operate

- [README](../README.md) — bootstrap and project scope.
- [CLI](cli.md) — ingestion, diagnostics, queries, servers, and export commands.
- [HTTP API](api.md) — versioned routes, pagination, errors, and examples.
- [MCP](mcp.md) — tools and stdio/HTTP transports.
- [Deployment](deployment.md) — local container topology and startup.
- [Operations runbook](operations-runbook.md) — health, worker, failures, quarantine,
  metrics, and incident response.
- [Observability](observability.md) — stable lifecycle events, correlation, log safety,
  and process-local metrics.
- [Backup and recovery](backup-and-recovery.md) — PostgreSQL and raw-archive procedures.

## Understand and extend

- [Architecture](architecture.md) — boundaries, composition, and data flow.
- [Data model](data-model.md) — identity and lifecycle semantics.
- [Data dictionary](data-dictionary.md) — PostgreSQL tables and key fields.
- [Ingestion lifecycle](ingestion-lifecycle.md) — states, idempotence, replay, and
  full-versus-delta behavior.
- [Source adapter guide](source-adapter-guide.md) — clean-room connector workflow.
- [Source coverage](source-coverage.md) — retailer-by-retailer verified status.
- [Testing](testing.md) — test layers and local commands.
- [Performance](performance.md) — workloads and measured baselines.
- [Contributing](../CONTRIBUTING.md) and [agent guide](../AGENTS.md).

## Evidence and decisions

- [Source research](research/source-research.md)
- [Open-source review](research/open-source-review.md)
- [Dependency/license audit](research/dependency-license-audit.md)
- [Parquet interoperability](research/parquet-interop.md)
- [Architecture decisions](adr/)
- [Living implementation plan](../.agent/execplans/platform.md)

Documentation reports implementation and dated verification evidence separately.
External publisher health can change without a code change; the coverage matrix is
the authoritative place for those observations.
