# Self-hosted deployment

Makolet's default self-hosted stack uses Docker Compose, PostgreSQL 18.4, and
SeaweedFS 4.40. It starts a migration job before the API and continuous ingestion
worker. The application image runs as UID/GID `10001:10001` with a read-only root
filesystem; PostgreSQL data, SeaweedFS data, local parser/archive spools, analytical
exports, and Prometheus data use durable named volumes.

## Prerequisites and configuration

Install Docker Engine with the Compose v2 plugin. Copy `.env.example` to `.env`,
then edit it before starting the stack:

```bash
cp .env.example .env
```

Compose has no credential fallbacks: the copied development file supplies every
required value explicitly. Every value containing `development` is local-only. For
any non-local deployment, replace `POSTGRES_PASSWORD`, update the same password in
`MAKOLET_DATABASE_URL`, and replace both `MAKOLET_S3_ACCESS_KEY` and
`MAKOLET_S3_SECRET_KEY`. Production settings reject the bundled development database
or S3 credentials. Keep replacements in a protected environment file or secret
manager, never in version control. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
are derived inside Compose for SeaweedFS and backup client compatibility; the
application reads the validated `MAKOLET_S3_*` settings.

The bundled PostgreSQL service does not terminate TLS. Compose therefore sets
`MAKOLET_DATABASE_ALLOW_INSECURE_LOCAL=true` for development. In production, that
explicit exception is accepted only for a literal loopback IP and only when the URL
has no `host`, `service`, or `servicefile` override; DNS names such as `postgres` and
`localhost` are not proof that the peer is local. When `MAKOLET_DATABASE_URL` points
outside literal loopback, remove the exception and append
`sslmode=verify-full` (or the asyncpg spelling `ssl=verify-full`) to the URL. Production
rejects a missing mode and `disable`, `allow`, `prefer`, `require`, or `verify-ca` for
remote hosts. The verified mode requires a trusted CA and checks that the certificate
matches the database hostname; an unavailable or invalid trust chain fails closed.

The bundled SeaweedFS S3 endpoint is likewise plaintext only in development on
Compose's private backend network. The example environment explicitly sets
`MAKOLET_S3_ALLOW_INSECURE_LOCAL=true`; production accepts that exception only when
the active S3 endpoint uses a literal loopback IP and
`MAKOLET_S3_PATH_STYLE=true`. DNS names such as `seaweedfs` and `localhost` require
HTTPS. Path-style is mandatory for plaintext so botocore cannot replace the approved
authority with a bucket-prefixed virtual host. Makolet also disables ambient HTTP(S)
proxy routing for this literal-loopback plaintext exception, so the signed request
cannot leave the host through a configured proxy. For every other object store, use an
`https://` endpoint and set the exception to `false`; the runtime, demo seed, and
raw-archive backup/restore tool all fail before client creation when a production
endpoint is plaintext. HTTPS certificate and hostname verification remains enabled
through botocore's configured CA trust.

Database backup operations additionally require
`MAKOLET_DATABASE_BACKUP_AUTH_KEY_FILE`, pointing to a protected raw 32-byte key kept
outside the repository and backup storage. This is a host-side operational secret,
not an application or Compose setting, and its value is never passed into PostgreSQL.
Provision, protect, rotate, and recover it using the exact contract in
[Backup and recovery](backup-and-recovery.md) before relying on database backups.

PostgreSQL and S3 host ports are fixed to `127.0.0.1`; changing
`MAKOLET_BIND_ADDRESS` cannot expose either state service. That variable controls the
API and enabled monitoring ports only, so pair any non-loopback API/monitoring bind
with host firewall, TLS proxy, and authentication rules.

The API has a process-local Uvicorn concurrency ceiling of 100 connections/tasks by
default. Direct deployments may set `MAKOLET_API_HTTP_MAXIMUM_CONCURRENCY` from 1
through 10,000; the validated setting is passed unchanged to Uvicorn. Keep any
reverse-proxy concurrency limit at or below that value so excess work is rejected
before it queues behind the application database pool.

The example schedules `shufersal` every six hours, discovers 25 entries per page,
and processes at most 100 files per source run. Change
`MAKOLET_ENABLED_SOURCES`, `MAKOLET_SOURCE_INTERVALS_SECONDS`, and the file/archive
byte bounds before first start when a different collection policy is required. An
enabled source is attempted immediately when the worker starts. The example permits
4 GiB per source run and 8 GiB in an exact rolling 24-hour window.
`MAKOLET_ARCHIVE_MINIMUM_FREE_BYTES` separately reserves local archive and parser,
FTP, and S3 spool capacity. Cooperating Makolet processes sharing the archive root
serialize each capacity check, flushed bounded write, and post-write check with the
persistent `.makolet-capacity.lock`; unrelated host writers are outside this advisory
lock. External S3 service capacity still requires provider monitoring and alerts.

Validate the fully interpolated configuration without starting containers:

```bash
docker compose --env-file .env config --quiet
```

The application image name is fixed to `makolet:local` and its Compose
`pull_policy: build` forces repository-source construction instead of accepting an
ambient image override. `scripts/check_container_images.py` inventories the syntax
frontend, every Dockerfile stage and external build source, every context-relative
Compose Dockerfile, and every runtime image bidirectionally against the reviewed
digest/license lock. Remote, home-expanded, variable, flow-style, tagged, inline,
additional-context, build-argument, and default `compose.override.*` forms fail
closed rather than escaping the inventory. Use explicit `--file` arguments for any
separately reviewed deployment composition.

## Start and inspect the stack

Build and start PostgreSQL, SeaweedFS, migrations, API, and worker:

```bash
docker compose --env-file .env up --build --detach --wait
docker compose --env-file .env ps
```

The default endpoints are:

| Service | Loopback endpoint | Purpose |
| --- | --- | --- |
| API | `http://127.0.0.1:8000` | `/healthz`, `/readyz`, `/metrics`, API and OpenAPI |
| SeaweedFS S3 | `http://127.0.0.1:8333` | S3-compatible immutable raw archive |
| PostgreSQL | `127.0.0.1:5432` | Operational database |

The worker metrics server listens only on the Compose network at
`worker:9100`. Its `/healthz` endpoint drives the container health check and its
`/metrics` endpoint contains worker-local ingestion and heartbeat metrics. It is
intentionally not published to the host.

Inspect structured logs or stop gracefully with:

```bash
docker compose --env-file .env logs --follow api worker
docker compose --env-file .env stop
```

`docker compose down` removes containers and networks but retains named volumes.
`docker compose down --volumes` permanently removes the stack data and must only be
used for a disposable environment.

## Migrations and demo data

The `migrate` job is a dependency of both API and worker. Run migration and status
checks explicitly with the local operational helpers:

```bash
MAKOLET_COMPOSE_ENV_FILE=.env bash scripts/database-migrate.sh
MAKOLET_COMPOSE_ENV_FILE=.env bash scripts/database-status.sh
```

PowerShell equivalents are:

```powershell
$env:MAKOLET_COMPOSE_ENV_FILE = ".env"
.\scripts\operations.ps1 database-migrate
.\scripts\operations.ps1 database-status
```

`database status` is read-only and reports both the expected migration head set and
all current database revisions. `schema_ready` becomes true only for an exact match;
the API `/readyz` endpoint applies the same test in addition to PostgreSQL health.
An empty, behind, unknown, or divergent schema therefore keeps the API out of service
without changing the database.

Revision `0010_bounded_query_paths` adds stored query projections, trigger
maintenance, and ordered indexes. Its existing-row transforms advance in 10,000-row
UUID batches, but the generated-city column and index builds are lock-heavy. Stop API
and worker writers, take and verify a backup, and run it in a maintenance window. If
the migration is interrupted, let PostgreSQL roll the transaction back completely;
do not stamp the revision or edit `0009`. Restore capacity and rerun the normal
migration command, then require exact-head readiness before restarting writers.

Seed the deterministic clean-room demo object and database rows with the `demo`
profile. The operation is idempotent:

```bash
MAKOLET_COMPOSE_ENV_FILE=.env bash scripts/seed-demo.sh
```

## Monitoring profile

Enable the optional local Prometheus service with:

```bash
docker compose --env-file .env --profile monitoring up --detach --wait prometheus
```

Prometheus is available at `http://127.0.0.1:9090` by default and retains 15 days,
bounded to 2 GB. Its configuration has separate scrape jobs for the API at
`api:8000/metrics`, the worker at `worker:9100/metrics`, and SeaweedFS at
`seaweedfs:9327`. Check target health at `/targets`. The profile is optional; no
hosted monitoring service is required. Remote Prometheus lifecycle management is
disabled, so the HTTP reload and shutdown handlers are not available.

## Deployment checks

The Linux container smoke script creates an isolated Compose project with
ephemeral loopback ports, builds the image, migrates, seeds, queries the API, runs
real PostgreSQL and SeaweedFS integration tests plus the complete clean-room E2E
workflow in `tests/e2e`, verifies the container security settings, and exercises
database and archive backup and restore. The restore drill waits for the restarted
API and re-queries the deterministic demo barcode and current price. The E2E workflow proves ordered discovery,
download, immutable archive, parse, stage, and apply boundaries; CLI/HTTP/MCP queries;
duplicate suppression; archive replay; price history; and active promotion retrieval:

```bash
bash scripts/container-smoke.sh
```

The smoke ignores ambient `COMPOSE_FILE`, `COMPOSE_PROJECT_NAME`, profiles, env-file,
database, S3 endpoint, bucket, prefix, credentials, and image selectors. It renders
only the repository `compose.yaml` with `.env.example`, forces the bundled loopback
PostgreSQL and SeaweedFS identities, and requires the exact
`makolet_test_coverage` confirmation before any schema reset. Cleanup removes only
the generated project, database, buckets/prefixes, keys, containers, and volumes.

The smoke script creates a one-run 256-bit database-backup authentication key under
its isolated temporary root, outside the backup directory, and removes it with the
rest of that disposable root. This proves the protected-key interface without
creating or using an operator key.

The command removes only its own disposable containers and volumes on exit. See
[Backup and recovery](backup-and-recovery.md) before operating on persistent data.
