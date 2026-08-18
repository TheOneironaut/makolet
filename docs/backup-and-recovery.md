# Backup and recovery

Back up both PostgreSQL and the S3-compatible raw archive. A database dump alone
does not contain the immutable source bytes, while an archive backup alone does
not contain normalized records, history, leases, or ingestion status.

The commands below assume the stack was started with `.env`. On Linux and macOS,
export the environment-file selector once:

```bash
export MAKOLET_COMPOSE_ENV_FILE=.env
```

On Windows, use PowerShell 7 or newer (`pwsh`); Windows PowerShell 5.1 lacks the
bounded process-supervision APIs required by the wrapper. Set the environment file
with:

```powershell
$env:MAKOLET_COMPOSE_ENV_FILE = ".env"
```

The operational wrappers resolve that explicit file to a regular, non-link path,
pin the repository's absolute `compose.yaml` and project directory, use project name
`makolet`, and reject ambient `COMPOSE_FILE`, `COMPOSE_ENV_FILES`,
`COMPOSE_PATH_SEPARATOR`, `COMPOSE_PROFILES`, `COMPOSE_DISABLE_ENV_FILE`,
`DOCKER_CONFIG`, `DOCKER_CONTEXT`, and `DOCKER_HOST` before invoking Docker. An ambient
`COMPOSE_PROJECT_NAME` is also rejected except for the container smoke's narrow
generated `makolet-smoke-...-<process-id>` form when its exact development,
`.env.example`, and test-database markers are present; that value is removed from
the environment and passed as an explicit argument. Clear all such variables rather
than relying on precedence. This prevents a
shell profile or parent process from substituting another service definition,
project, or daemon when a wrapper mounts an authentication key, reads S3
credentials, or signs PostgreSQL output. `MAKOLET_COMPOSE_ENV_FILE` is the sole
supported operator-selected Compose environment file for these commands.

Store backup directories on storage separate from the Compose host. Protect them
as production data even though the scripts never write credentials into their
manifests.

Archive backup and restore inherit `MAKOLET_ENVIRONMENT`,
`MAKOLET_S3_ENDPOINT`, and `MAKOLET_S3_ALLOW_INSECURE_LOCAL` from the selected
Compose environment file. In production, the archive tool refuses remote plaintext
HTTP before constructing an S3 client. HTTPS is required for every DNS hostname;
local HTTP is limited to a literal loopback IP and additionally requires the explicit
flag and path-style addressing. The archive tool disables ambient HTTP(S) proxies for
that literal-loopback plaintext connection. Production backup and restore also require an
authenticated AWS access-key/secret-key pair; the tool never silently selects
anonymous S3 requests. Local backup verification does not contact S3. Use the same
endpoint policy described in [deployment](deployment.md).

Before Botocore can materialize a response body, the archive tool intercepts every
S3 operation it uses: listing, object read and metadata, conditional staging upload,
conditional publication copy, and staging deletion. Error bodies and non-streaming
success bodies share one 8 MiB byte budget across all retries of a request. For a
body-bearing response, declared or streamed length above the remaining budget fails
closed and the raw body is closed. HTTP `HEAD` and no-content statuses preserve their
representation headers, which do not describe a response body, while still charging
and capping any raw bytes that arrive. A successful `GetObject` is deliberately not
buffered by this control; it remains a stream governed by the configured per-object
length, total operation deadline, digest/HMAC verification, and filesystem-capacity
reserve.
Runtime immutable-archive clients apply the same pre-parse boundary to bucket checks,
conditional writes, metadata checks, and object-read errors. This transport limit is
not an object-size setting and should not be increased to accommodate archive data.

## Database backup authentication key

Database backup and restore require a separate 256-bit authentication key. The
environment contains only its file path; the key itself must be a raw 32-byte secret
kept in an operating-system secret store, mounted secret, or protected directory
outside the repository and outside all backup storage. Do not copy it into a recovery
set or place it beside a dump. Keep a separately protected recovery copy: losing the
key makes its backups unverifiable. Use a distinct key for each deployment/database;
never share one between development, staging, and production.

Provision a new key once at an operator-selected protected path whose parent already
exists, then retain the same environment setting for backup and restore:

```bash
export MAKOLET_DATABASE_BACKUP_AUTH_KEY_FILE=/absolute/protected/path/database-backup-auth.key
uv run python -m makolet.interfaces.database_backup_auth \
  generate-key "$MAKOLET_DATABASE_BACKUP_AUTH_KEY_FILE"
```

The generator refuses to overwrite any existing path. On POSIX, it creates mode
`0600` and backup/restore also reject a key readable or writable by group or other
users. On Windows, put the key in an administrator-approved user/service directory
with an ACL limited to the account running the operation; inherited Windows ACLs
remain the operator's responsibility. Signing and verification on both platforms
reject a key that is not exactly 32 bytes, is a symlink/reparse point or hard link,
or resolves beside, above, or below the dump's directory tree.

PowerShell uses the same helper and contract:

```powershell
$env:MAKOLET_DATABASE_BACKUP_AUTH_KEY_FILE = `
    "C:\absolute\protected\path\database-backup-auth.key"
uv run python -m makolet.interfaces.database_backup_auth `
    generate-key $env:MAKOLET_DATABASE_BACKUP_AUTH_KEY_FILE
```

The verifier defaults to a 128 GiB maximum dump and keeps at least 1 GiB free on
the filesystem holding its authenticated temporary copy. Set
`MAKOLET_DATABASE_BACKUP_MAXIMUM_BYTES` and
`MAKOLET_DATABASE_BACKUP_MINIMUM_FREE_BYTES` to deployment-specific integer byte
limits before both backup signing and restore verification. The maximum must cover a
legitimate dump; neither setting weakens HMAC authentication. Oversized, sparse, or
capacity-exhausting inputs fail before any copy is retained or passed to
`pg_restore`.
The Bash and PowerShell restore entry points pass their shared system-temporary root
as the capacity coordination directory, so even though each restore uses a distinct
private subdirectory, concurrent Makolet verifiers serialize each checked and flushed
1 MiB chunk through one persistent per-user
`.makolet-capacity-<identity>/.makolet-capacity.lock`. On POSIX the helper requires
that private directory to be owned by the current user with mode `0700`, and rejects
a writable non-sticky parent, preventing another local user from replacing the lock.
It rechecks the floor before releasing the lock and rejects a coordination directory
on another filesystem.
The lock is advisory: unrelated host writers require operational volume isolation or
their own capacity controls.

Do not rerun key generation for an existing path. During rotation, retain each old
key in protected custody for as long as any backup authenticated with it remains a
recovery point, and record the key-to-recovery-set association in the operator's
secret inventory without storing the key in that inventory.

Authentication proves that the exact bytes were produced by an operator holding the
key; it does not select a recovery point or make an old authentic backup current.
Choose the expected dated recovery set from a separately protected inventory before
starting a restore.

## Raw archive backup authentication key

Raw archive backup, verification, and restore require their own 256-bit
authentication key. It must be distinct from the database backup key and from every
application or object-store credential. Keep the raw 32-byte key in protected
storage outside the repository and outside every archive backup tree, and retain a
separately protected recovery copy. The archive scripts mount the selected file
read-only at a fixed path in the short-lived operations container; the key is never
copied into the backup or written to the manifest.

The existing exclusive key-file generator can create this second, dedicated key.
Select a new path rather than rerunning it against the database key or any existing
file:

```bash
export MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE=/absolute/protected/path/archive-backup-auth.key
uv run python -m makolet.interfaces.database_backup_auth \
  generate-key "$MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"
```

PowerShell uses the same raw key format:

```powershell
$env:MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE = `
    "C:\absolute\protected\path\archive-backup-auth.key"
uv run python -m makolet.interfaces.database_backup_auth `
    generate-key $env:MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE
```

On POSIX, archive operations require the key to be owner-only and reject symbolic
links, reparse points, additional hard links, and any value other than exactly 32
bytes. On Windows, place it under an ACL limited to the operator account; host ACL
management remains an operator responsibility. All three archive commands reject a
key located inside the selected backup tree. Retain old archive keys for as long as
their authenticated recovery sets are retained, and record that association in the
protected recovery inventory.

The POSIX Bash wrappers, and PowerShell when run on POSIX, pass the invoking numeric
user and primary group to the operations container. This lets the same identity read
an owner-only `0600` key and create files in the operator-owned backup directory;
generated archive artifacts consequently remain owned by the invoking operator.
The wrappers reject root, a root primary group, and numeric identities outside their
supported range rather than override the image's non-root invariant.

This identity mapping targets an ordinary local rootful Docker daemon. Rootless
Docker, `userns-remap`, remote Docker contexts, Git Bash over an NTFS path, and an
SELinux policy without an approved container-readable label do not preserve the same
bind-mount ownership contract and therefore fail closed. Use PowerShell on Windows.
For a remapped or remote daemon, stage recovery material on daemon-local storage with
an operator-approved ownership/label design before using these wrappers; do not run
the archive container as root to bypass the check.

Docker Desktop does not expose Windows ACLs as meaningful POSIX mode bits inside its
Linux container. On Windows, the PowerShell wrapper therefore validates that the
host path is a non-reparse, exact-32-byte file outside the backup tree, mounts it
read-only at a fixed staging path, and enables a narrow launcher mode for that one
container run under the image's numeric `10001:10001` identity. The wrapper keeps an
exclusive-against-write read handle open and binds its signal to the SHA-256 digest
of the exact 32 host bytes. The launcher accepts only that fixed source path, exact
digest, identity, Docker Desktop `0777` bind mode, and a read-only mount. It then
performs a bounded no-follow read, copies the key to a unique `0600` file in the
container's existing `/tmp` tmpfs, points the unchanged core verifier at that private
copy, and removes it after success or failure. The direct archive tool never waives
its owner-only POSIX key check. The internal
`MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_WINDOWS_BIND` launcher signal is created by
`operations.ps1`; do not configure it in `.env`, Compose, or the operator shell.

## Consistent backup procedure

Stop the worker gracefully so no ingestion crosses the database and object-store
backup boundary. The API is read-only and may remain available:

```bash
compose=(docker compose --file "$PWD/compose.yaml" --project-directory "$PWD" \
  --project-name makolet --env-file "$MAKOLET_COMPOSE_ENV_FILE")
"${compose[@]}" stop worker
mkdir -p backups/2026-08-11/database backups/2026-08-11/archive
bash scripts/database-backup.sh backups/2026-08-11/database/makolet.dump
bash scripts/archive-backup.sh backups/2026-08-11/archive
bash scripts/archive-verify.sh backups/2026-08-11/archive
"${compose[@]}" up --detach --wait worker
```

Use a current UTC date in place of the example. The database helper refuses to
overwrite an existing dump, checksum sidecar, or authentication sidecar. It creates
a PostgreSQL custom-format dump without ownership or ACL statements, validates it
by streaming the bounded host file directly to `pg_restore --list` on stdin without
creating a second container-side dump, writes the corruption-check sidecar
`makolet.dump.sha256`, and authenticates the exact dump bytes into
`makolet.dump.hmac-sha256` using versioned, domain-separated HMAC-SHA-256. It
creates both sidecars under the same capacity lock/free-space policy and restricts
all three files to the calling user where the host supports POSIX permissions. Keep
those three non-secret recovery artifacts together; the protected key never joins
them.

The raw-archive helper streams only canonical content-addressed objects below the
configured `MAKOLET_S3_KEY_PREFIX` into files under `objects/<sha256>`. Its versioned
manifest records that exact prefix and each complete service key. Backup,
verification, and restore all reject a prefix mismatch, a non-canonical
`<prefix>/sha256/<first-two>/<next-two>/<64-hex-digest>` key, or disagreement between
the path digest and downloaded bytes. The tool writes `manifest.json`, the corruption
check `manifest.json.sha256`, and the mandatory authentication sidecar
`manifest.json.hmac-sha256`. The sidecar is a versioned, domain-separated
HMAC-SHA-256 over the exact canonical manifest bytes. Verification authenticates
those bytes before parsing JSON or trusting the object inventory; recomputing the
unkeyed checksum after deleting inventory entries does not make a changed manifest
valid.

Backup membership comes from the bounded, deterministically ordered PostgreSQL
`raw_archive_objects` inventory at the stopped-worker recovery point, not from S3
listing alone. The helper converts every database object key, SHA-256 digest, and
byte length to its exact service key, then requires the complete paginated S3
listing to match that authoritative sequence and count before downloading or
publishing `manifest.json` or either sidecar. A missing, extra, reordered, duplicate,
wrong-length, or non-canonical listing entry fails closed. Before touching the
worker, each archive-backup wrapper atomically acquires a project-scoped Docker
volume lock. The Bash and PowerShell entry points use the same lock, so concurrent
processes and separate repository clones targeting the same Compose project cannot
overlap backups. While holding it, the wrapper treats both `running` and
`restarting` as active worker states, stops the worker, and queries those states
again; the snapshot starts only after the worker is proven nonrunning.

Stop and raw-archive Compose invocations are supervised as well as the restart: the
outer archive watchdog is the configured operation budget plus its cleanup phase,
and each operations container receives a unique, validated name. The lock remains
held through exact-container cleanup and any required worker restart, so one backup
cannot restart the worker while another backup is active. After a failed or timed-
out client, the wrapper force-removes only that exact container under the cleanup
watchdog before considering a restart. If container cleanup, worker quiescence, or
lock ownership cannot be proved, the command fails closed, reports the exact
recovery names, suppresses the worker restart, and retains the lock. A lock-release
failure after restart triggers a bounded attempt to stop the worker again. Do not
remove a retained `makolet_archive_backup_lock_<project>` volume until an operator
has proved the named archive container absent and the worker nonrunning; remove the
lock only as part of that explicit recovery, then rerun the backup or restart the
worker.

Both Bash and PowerShell supervise an allowed restart with a 120-second watchdog and
return failure if the worker does not become healthy; lower or raise the bound from
1 through 3,600 seconds with
`MAKOLET_ARCHIVE_WORKER_RESTART_TIMEOUT_SECONDS`. The watchdog is validated before
the wrapper stops the worker. Verification and restore Compose invocations use the
same named-container supervision without stopping the worker. When taking the
coordinated database-plus-archive set above, keep the worker stopped across both
commands so both artifacts describe the same recovery interval.

Backup creation enforces the 128 GiB aggregate default across every object plus the
exact canonical manifest, checksum, and HMAC sidecars. It computes each pretty-JSON
entry's exact UTF-8 contribution before retaining that inventory row, stops listing
as soon as the 16 MiB manifest or aggregate ceiling would be crossed, and verifies
the final canonical length before downloading any object. Object and metadata writes
share one destination-scoped filesystem-capacity guard and the 1 GiB default
free-space floor. Existing manifest or sidecar paths are never overwritten.

Listing itself has independent response-byte, page/request, no-progress, and
wall-clock ceilings. Each signed `ListObjectsV2` response is streamed through an
8 MiB pre-parse cap before botocore may materialize its XML. Defaults are 10,000
pages, three consecutive pages without an object, and 300 seconds. Operators may
lower the configurable aggregate bounds with
`MAKOLET_ARCHIVE_BACKUP_MAX_LIST_PAGES`,
`MAKOLET_ARCHIVE_BACKUP_MAX_NO_PROGRESS_PAGES`, and
`MAKOLET_ARCHIVE_BACKUP_LIST_TIMEOUT_SECONDS`; choose values large enough for the
authoritative object count. SDK connection/read timeouts and retries remain active
inside those aggregate bounds.

Backup, local verification, and restore also share one 3,600-second monotonic
operation budget across PostgreSQL inventory, listing, every S3 body, HEAD, PUT,
COPY, DELETE, checksum pass, and object. Set
`MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS` to a positive value no larger
than 86,400 seconds for a deployment-specific recovery set. The tool configures the
inventory transaction's PostgreSQL `statement_timeout` from the remaining budget,
cancels the transaction on expiry, and closes active S3 clients and bodies. A
separate, bounded 30-second abort/cleanup phase closes resources and removes restore
staging; configure it from 1 through 300 seconds with
`MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS`. An expiry is a failed operation,
never a successful partial verification. Choose the work bound from measured object
count and storage throughput rather than disabling it.

Manifest reads are capped at 16 MiB. The manifest, checksum, and authentication
sidecar must each be a no-follow regular file whose declared size fits its limit;
the reader then performs a bounded `limit + 1` read and rejects malformed, sparse
oversized, replaced, or changing metadata before JSON allocation. Keep all four
non-secret archive components together: the manifest, both sidecars, and the
`objects` directory. The authentication key remains separate.

PowerShell performs the same operations:

```powershell
.\scripts\operations.ps1 database-backup backups\2026-08-11\database\makolet.dump
.\scripts\operations.ps1 archive-backup backups\2026-08-11\archive
.\scripts\operations.ps1 archive-verify backups\2026-08-11\archive
```

## Verification and restore drill

Verify the raw backup again after copying it to off-host storage and as part of
every restore drill:

```bash
bash scripts/archive-verify.sh backups/2026-08-11/archive
```

Database restore first requires the adjacent `.hmac-sha256` sidecar and protected
key. It authenticates while copying into a newly created private temporary file, then
checks the existing adjacent SHA-256 sidecar against that exact authenticated copy.
The checksum sidecar reader accepts only one regular, at-most-4-KiB line before
extracting its lowercase digest, so unauthenticated recovery metadata cannot force an
unbounded pre-authentication read.
Only this copy reaches `pg_restore --list` or the staged restore, so changing the
original dump after authentication cannot substitute unauthenticated bytes. Plan for
temporary host space equal to the dump size. The copy is removed on success and
failure.

After authentication, restore validates the custom dump before creating or changing
a database. It restores first into a new staging database and runs current migrations
against that isolated target. It then requires the staging database's complete
Alembic revision set to exactly equal the migration heads shipped in the application
image. Only after those preflight steps succeed does it inspect and stop a running API
and worker, transactionally rename the active database, promote the already-current
staging database, and restart the services that were previously running.

An authentication, checksum, restore, migration, or exact-head preflight failure
drops any predictable `makolet_restore_<UTC timestamp>_<process id>` staging database
it created and leaves the active database plus API/worker state unchanged. Missing or
wrong key errors do not reveal the key, its bytes, or internal exception details and
occur before `pg_restore`. The helper never prints the derived staging DSN. A failure
after the atomic swap is operationally distinct: the displaced database is retained
under its reported `makolet_previous_*` name for an explicit recovery decision.

Legacy database dumps without `.hmac-sha256` are deliberately rejected. Prefer a new
backup from the trusted running database. Authenticating an old dump is an explicit
incident-recovery decision and is safe only after its provenance has been established
outside the potentially compromised backup store; recomputing `.sha256` alone never
establishes authenticity.

The final argument is an exact, case-sensitive confirmation of `POSTGRES_DB`:

```bash
bash scripts/database-restore.sh \
  backups/2026-08-11/database/makolet.dump \
  makolet
bash scripts/database-status.sh
```

The displaced database is preserved as
`makolet_previous_<UTC timestamp>_<process id>` for rollback and inspection. It
continues consuming PostgreSQL volume space. Remove it only after application,
row-count, migration, and restore-point checks have passed and a separate verified
backup exists.

PowerShell equivalent:

```powershell
.\scripts\operations.ps1 database-restore `
  backups\2026-08-11\database\makolet.dump `
  makolet
.\scripts\operations.ps1 database-status
```

## Raw archive restore

Archive restore first authenticates the exact manifest snapshot, then verifies its
canonical inventory and every local object. It uses a conditional S3 write, so it
does not overwrite an existing raw object, and then streams every destination object
back to verify its SHA-256 digest and length.
Each verified local descriptor is uploaded only to a deterministic noncanonical
staging key. Restore verifies that exact staging digest, length, ETag, and VersionId
when supplied; conditionally copies it to an absent canonical key; and removes the
staging object through a bounded idempotent cleanup path even after an ambiguous
upload or transient delete response. A destination precondition failure is accepted
as an idempotent existing object only after the canonical remote bytes are reverified.
The final argument must exactly match `MAKOLET_S3_BUCKET`; a wrong confirmation is
rejected before the restore container starts. The runtime key prefix must also match
the prefix recorded in the backup; changing it requires an explicit, separately
audited migration rather than rewriting manifest keys.

```bash
bash scripts/archive-restore.sh backups/2026-08-11/archive makolet-raw
```

PowerShell equivalent:

```powershell
.\scripts\operations.ps1 archive-restore backups\2026-08-11\archive makolet-raw
```

An idempotent rerun reports zero newly created objects and still verifies the
remote content. Restore the raw archive before enabling ingestion workers, then
restore PostgreSQL and confirm readiness:

```bash
curl --fail http://127.0.0.1:8000/readyz
docker compose --env-file .env --profile monitoring up --detach --wait prometheus
```

After recovery, check API queries, worker heartbeat metrics, Prometheus targets,
source freshness, and object counts before resuming normal collection cadence.

Legacy raw archive recovery sets without `manifest.json.hmac-sha256` are
deliberately rejected. Prefer creating a new backup from the trusted object store.
Adding an unkeyed checksum to an old manifest does not establish its provenance;
authenticating a historical set is an explicit incident-recovery decision that is
safe only after its provenance and completeness have been established outside the
potentially compromised backup store.

The automated restore drill does not restore over the source bucket. Running
`bash scripts/container-smoke.sh` creates a uniquely named Compose project and a
brand-new `makolet-restore-verification-<run>-<process>` bucket inside it, restores
the backup into that bucket, and requires `objects_created` to equal the complete
manifest count.
The restore implementation then reads every created object back and checks its exact
SHA-256 digest and byte length. This proves creation and content recovery rather than
an idempotent no-op against the bucket that produced the backup. The drill generates
separate temporary database and archive authentication keys and removes both with
the rest of its temporary tree.
