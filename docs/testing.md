# Testing

Makolet separates deterministic offline checks, real-service integration, container
smoke, opt-in live publisher probes, and measured benchmarks. External access is
never required for ordinary unit/contract tests.

## Locked setup and static checks

```text
uv python install 3.14.7
uv sync --all-groups --frozen
uv lock --check
uv run ruff format --check .
uv run ruff check .
uv run mypy src tests benchmarks
uv run python scripts/check_docs.py
bash -n scripts/*.sh
```

Use `uv run ruff format .` to apply formatting. Mypy runs in strict mode. Do not hide
setup or checks in CI-only scripts. Run the shell-syntax check from Linux, WSL, or
Git Bash on Windows.

## Offline unit and contract tests

```text
uv run pytest --no-cov -m "not integration and not live and not benchmark"
```

The suite covers domain normalization/identity/state transitions, matching, query
limits, worker scheduling, configuration, exact local/S3 archive behavior, HTTP
download policies, compression and XML security/encoding/record contracts, source
family listings/cursors, PostgreSQL-independent ingestion orchestration, Parquet
format/dataset semantics, logs/metrics, and API/CLI/MCP transport contracts.
Focused database-backup tests also prove that a recomputed plain SHA-256 sidecar
cannot authorize modified dump bytes, that wrong keys/sidecars fail without retaining
an unauthenticated temporary copy, and that both shell front ends authenticate before
any restore-time `pg_restore` invocation. They also reject an oversized checksum
sidecar through the same bounded helper before reading attacker-controlled lines.

Parser/source fixtures are independently authored minimal inputs. Source listing
fixtures document their clean-room origin in
`tests/unit/sources/fixtures/README.md`; no retailer dump or active signed URL belongs
in the repository.

Offline-only execution does not claim repository coverage: the production
PostgreSQL adapters are intentionally exercised against PostgreSQL rather than
mocked or omitted. Use `--no-cov` for offline and narrow iteration commands. The
coverage gate runs every non-live, non-benchmark test in one process against the
dedicated test services and enforces 85% branch-aware repository coverage:

```powershell
$env:MAKOLET_TEST_DATABASE_URL = "postgresql://makolet:<test-password>@127.0.0.1:5432/makolet_test_coverage"
$env:MAKOLET_TEST_DATABASE_CONFIRM = "makolet_test_coverage"
$env:MAKOLET_TEST_S3_ENDPOINT = "http://127.0.0.1:8333"
$env:MAKOLET_TEST_S3_BUCKET = "makolet-raw"
$env:MAKOLET_TEST_S3_ACCESS_KEY = "<test-access-key>"
$env:MAKOLET_TEST_S3_SECRET_KEY = "<test-secret-key>"
uv run pytest --cov=makolet --cov-branch --cov-report=term-missing --cov-fail-under=85 -m "not live and not benchmark"
```

The database must be disposable, use a loopback authority, have no driver query or
fragment override, match `makolet_test_[a-z0-9_]{1,48}`, and equal the explicit
`MAKOLET_TEST_DATABASE_CONFIRM`. This combined
invocation is important: separate pytest processes do not, by themselves, prove one
coherent repository-wide threshold. For focused iteration:

```text
uv run pytest tests/unit/test_xml_parser.py --no-cov -q
uv run pytest tests/unit/sources -m "not live" --no-cov -q
```

Coverage is evidence only when important domain/ingestion failure paths are tested;
framework-line coverage is not a substitute for correctness assertions.

## Real PostgreSQL and object storage

Integration tests use a real PostgreSQL 18 database and S3-compatible service. They
skip unless explicit variables are supplied:

```powershell
$env:MAKOLET_TEST_DATABASE_URL = "postgresql://makolet:<test-password>@127.0.0.1:5432/makolet_test_integration"
$env:MAKOLET_TEST_DATABASE_CONFIRM = "makolet_test_integration"
$env:MAKOLET_TEST_S3_ENDPOINT = "http://127.0.0.1:8333"
$env:MAKOLET_TEST_S3_BUCKET = "makolet-raw"
$env:MAKOLET_TEST_S3_ACCESS_KEY = "<test-access-key>"
$env:MAKOLET_TEST_S3_SECRET_KEY = "<test-secret-key>"
uv run pytest --no-cov -m "integration or e2e" tests/integration tests/e2e
```

Safety rules are enforced by the fixtures:

- the database uses PostgreSQL on exact loopback, has no query/fragment overrides,
  matches the strict `makolet_test_...` allowlist, and equals its confirmation;
- an existing Alembic-managed test schema is downgraded to base, then upgraded;
- a database with unrelated unmanaged tables is refused;
- tables are truncated only inside that dedicated test database;
- S3 tests use a random prefix and verify concurrent conditional creation and exact
  read-back bytes.

Current real-service tests exercise empty migration/up/down, constraints, repository
staging/apply/idempotence/full-versus-delta/history, leases/replay, and immutable S3
archive behavior. The clean-room E2E fixture records and asserts discovery before
separate download, archive, parse, stage, and apply boundaries. It then uses the real
PostgreSQL rows through CLI, HTTP, and MCP, verifies same-identity idempotence and
unchanged-value history suppression,
replays archived bytes without downloading, applies a later delta, and observes the
two-row price history through all three interfaces. The same pipeline ingests the
shared clean-room PromoFull fixture and proves the active product/store promotion
through the query service, CLI, HTTP, and MCP.

Revision 0011 coverage additionally upgrades a populated 0010 schema through a
10,005-issue backfill, proves exact issue bytes/counts with a 1,000-row evidence
sample, reconstructs fixed collection buckets, checks `btree_gist` availability and
permission, verifies empty downgrade, and runs schema drift. Query-plan tests seed
12,000 sparse temporal and unrelated relationship rows, require the price/promotion
GiST and reverse relationship indexes, preserve an overlapping predecessor, and prove
fail-closed probe overflow. Collection and MCP exploit tests cover tiny-identity floods,
fixed-bucket plans, oversized declared bodies, trickled bodies, and concurrency wiring.

## Container smoke

On Linux, CI, WSL, or Git Bash:

```text
bash scripts/container-smoke.sh
```

The script creates an isolated Compose project and temporary directory, validates
image pins, builds the non-root/read-only application image, starts PostgreSQL and
SeaweedFS, migrates an empty `makolet_test_coverage` database, and seeds the
deterministic archive/database demo. With publisher collection disabled, it starts
the API, continuous worker, and monitoring profile; proves that both application
containers are non-root with read-only root filesystems; requires Prometheus to
report the API, worker, and SeaweedFS targets healthy; and queries the seeded Hebrew
product through HTTP. It stops the long-lived database clients before running the
offline, integration, and E2E suites together against the real services, excludes
only explicit live and measured-benchmark tests, and enforces 85% branch-aware
package coverage. Because the real-database fixtures deliberately reset their
dedicated schema, the smoke re-seeds the idempotent deterministic demo after
coverage and before backup. It backs up and verifies the raw archive, restores it
into a newly created isolated bucket, and re-verifies the complete target inventory
instead of trusting only the restore summary. It then restores PostgreSQL, restarts
the API, worker, and Prometheus, and re-queries the deterministic demo barcode and
current price through the public API after the database swap. It removes the
temporary project containers, volumes, and networks on exit. Its database
backup/restore drill generates a disposable protected authentication key outside
the backup directory; this test key and its recovery artifacts remain under the
script's uniquely named temporary root and are removed on exit.

When the smoke is launched from Git Bash, MSYS, or Cygwin on Windows, it routes
raw-archive backup, verification, and restore through `scripts/operations.ps1` so
the Docker Desktop key-mount protections are preserved and requires PowerShell 7's
`pwsh.exe`; it fails before the archive operation if only Windows PowerShell 5.1 is
available. Other hosts retain the POSIX Bash wrappers. Compose-only API and database
settings remain exported for later
operations, but the host pytest coverage subprocess explicitly unsets the complete
`MAKOLET_*` namespace before supplying only its dedicated `MAKOLET_TEST_*` values, so
tests exercise their own configuration contracts.

This smoke test is destructive only to its uniquely named Compose project and
`makolet_test_coverage` database. Do not point its environment at production
services.

Latest completed container evidence, reconciled at 2026-08-17T17:04:58Z immediately
after that successful rerun:
`C:\Program Files\Git\bin\bash.exe scripts/container-smoke.sh` exited 0 against
Docker Desktop under isolated Compose project `makolet-smoke-local-1298`. The
disposable PostgreSQL 18.4 database migrated to
`0011_resource_probe_budgets`. The combined process collected 1,215 cases and
selected 1,212: 1,206 passed, six expected capability cases skipped, three live
cases were deselected, and eight known warnings were emitted in 311.81 seconds.
Branch coverage was 85.77%, above the enforced 85% floor. HMAC-authenticated
PostgreSQL backup and restore passed, and the restored database reported migration
head 0011. Raw-archive HMAC backup, source verification, restore into a
confirmed-clean isolated bucket, and a second exact verification against that target
passed for all 18 objects. The post-restore public API check passed. Cleanup left
zero containers, volumes, or networks for the exact smoke project.

This smoke result records container, coverage, and recovery behavior only.
It predates the seven fixes discovered by Standard scan
`dcd9f5e6-8410-49d0-90f9-533dffb6e20b`, including runtime HTTP/S3/database and
public-query changes, so it is now historical rather than final-tree acceptance. A
new combined container/coverage/recovery run is required after the source tree is
frozen. Distribution and runtime-SBOM reconciliation can also rebuild
`makolet:local`, so this result is not an image/runtime-SBOM fixed-point claim.

Historical attempts failed safely and did not count as acceptance: the builder did
not yet receive the `migrations/` input required by the package manifest, the random
host API port changed when Compose recreated the API after database restore, and a
later Windows run selected Windows PowerShell 5.1 after coverage and database restore.
The Dockerfile now supplies the declared input, the smoke rediscovers the port after
restore, and Windows POSIX shells require `pwsh.exe` before archive operations. The
current successful run above supersedes that earlier evidence.

## Live source smoke

Publisher-facing listing smokes are read-only, explicit, and excluded from ordinary
runs. One invocation selects one source and normally asks for at most one discovered
file. The Maayan 2000
case asks for one file from each of its five configured type partitions, proving that
every bounded partition remains below the publisher cap without downloading source bytes:

```powershell
$env:MAKOLET_LIVE_SOURCE = "shufersal"
uv run pytest --no-cov -m live tests/unit/sources/test_live_sources.py -vv
Remove-Item Env:MAKOLET_LIVE_SOURCE
```

Unknown IDs fail rather than skip. Network/publisher failures fail the selected test;
they must be triaged as external state versus a code regression and recorded honestly
in `docs/source-coverage.md`. Never add `continue-on-error` or treat a skip as a live
pass.

`.github/workflows/live-sources.yml` provides a manual source choice and a weekly,
max-parallel-one matrix for seven credential-free HTTPS sources. Credentialed FTP and
blocked/unresolved sources are intentionally absent.

Latest recorded listing evidence: on 2026-08-18, after the Matrix empty-wrapper
fix at source digest
`0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805`,
each documented source was selected in its own sequential invocation of
`uv run pytest --no-cov -m live tests/unit/sources/test_live_sources.py -vv`.
All seven credential-free HTTPS lanes passed: `shufersal`, `king-store`,
`maayan-2000`, `global-retail`, `hyper-cohen`, `hazi-hinam`, and `city-market`.
`hyper-cohen` had failed closed earlier the same morning on a publisher `[]`
catalog (`EXTERNAL_SOURCE_INVALID_RESPONSE: Matrix catalog has no expected record
collection`); the later window returned a nonempty catalog instead. Empty named
wrappers such as `{"files": []}` still fail closed and are not treated as a
successful empty catalog. The checks were read-only, ran one at a time, and
downloaded no source object.

A separately selected NCR plain-FTP check also requires
`MAKOLET_ALLOW_INSECURE_FTP=true`; setting it is an explicit acceptance of a publisher
transport that cannot authenticate received bytes. Do not enable it in the scheduled
HTTPS matrix, and never treat an FTP result as equivalent to certificate-verified
HTTPS/FTPS evidence.

### Representative live-ingestion acceptance

The listing smokes above do not prove archival, parsing, PostgreSQL application,
catalog matching, replay, or public-query provenance. A separate explicit acceptance
test sends one Maayan 2000 Price-delta file for store `001` through those production
paths. It is never scheduled and is excluded from ordinary, container, and coverage
runs. Run it only against local disposable PostgreSQL 18 and S3-compatible services:

```powershell
$env:MAKOLET_LIVE_INGESTION_ACCEPT = "maayan-2000-price-store-001"
$env:MAKOLET_LIVE_ACCEPTANCE_ADMIN_DATABASE_URL = "postgresql://<test-admin>:<test-password>@127.0.0.1:5432/postgres"
$env:MAKOLET_LIVE_ACCEPTANCE_S3_ENDPOINT = "http://127.0.0.1:8333"
$env:MAKOLET_LIVE_ACCEPTANCE_S3_BUCKET = "makolet-raw"
$env:MAKOLET_LIVE_ACCEPTANCE_S3_REGION = "us-east-1"
$env:MAKOLET_LIVE_ACCEPTANCE_S3_ACCESS_KEY = "<test-access-key>"
$env:MAKOLET_LIVE_ACCEPTANCE_S3_SECRET_KEY = "<test-secret-key>"
uv run pytest --no-cov -m live tests/live/test_representative_ingestion.py -vv
Remove-Item Env:MAKOLET_LIVE_INGESTION_ACCEPT
Remove-Item Env:MAKOLET_LIVE_ACCEPTANCE_ADMIN_DATABASE_URL
Remove-Item Env:MAKOLET_LIVE_ACCEPTANCE_S3_ENDPOINT
Remove-Item Env:MAKOLET_LIVE_ACCEPTANCE_S3_BUCKET
Remove-Item Env:MAKOLET_LIVE_ACCEPTANCE_S3_REGION
Remove-Item Env:MAKOLET_LIVE_ACCEPTANCE_S3_ACCESS_KEY
Remove-Item Env:MAKOLET_LIVE_ACCEPTANCE_S3_SECRET_KEY
```

The acceptance harness has a fixed credential-free HTTPS source and refuses
non-loopback service URLs. Its PostgreSQL administrative URL must contain no query
parameters or fragment, preventing driver-level host or service overrides from
bypassing the loopback authority check. It creates a uniquely named database containing `test`,
uses a random `live-acceptance/` object prefix, and attempts both cleanup paths after
database creation is confirmed, even when later setup or assertions fail. An
ambiguous database-create response fails closed: the harness reports the exact
generated name for inspection and does not risk dropping a pre-existing database.
An initial S3 listing failure or non-empty-prefix collision behaves the same way: the
exact generated prefix is reported and is not purged unless its initial emptiness was
confirmed.
The one HTTPS publisher object is capped at 4 MiB. The run/day charged-byte settings
cover the production-wide worst case: the object, the bounded FTP/FTPS control,
address-attempt, and TLS-framing reservation, and 64 KiB final-frame headroom.
This HTTPS lane consumes only
its object and final-frame portion because it has no FTP control channel.
The publisher request counter permits at most 12 requests, which is the fixed
listing, optional resolver, and download sequence including each production redirect
ceiling. Discovery is terminated after the first selected file.

The test streams the exact S3 object back under the same 4 MiB cap and verifies its
nonzero length and SHA-256 against both ingestion output and database provenance. It
also verifies the exact scoped object count, accepted price rows,
automatic isolated-catalog matching, archive replay while publisher HTTP is patched
to fail, zero new history from unchanged replay, and the same provenance fields
through the query service, CLI, HTTP API, and MCP. It does not retain publisher bytes,
signed URLs, the temporary database, or objects under its prefix. An interrupted
test process can prevent `finally` cleanup; before rerunning, an operator must remove
only an orphan whose database name starts `makolet_live_acceptance_test_` and whose
S3 prefix starts `live-acceptance/`. A code review or skipped test is not live
evidence: record the exact successful command and date in the living ExecPlan and
source-coverage matrix.

Latest verified live evidence, reconciled on 2026-08-18 after the Matrix
empty-wrapper fix: the exact command above passed both tests on source digest
`0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805`
in 12.28 seconds against PostgreSQL 18.4 and SeaweedFS 4.40 on loopback
`127.0.0.1:55432` / `127.0.0.1:58333`. It newly completed exactly one bounded
Price-delta file, archived one non-empty object no larger than 4 MiB, accepted a
nonzero number of price rows, and stayed within the asserted 2-to-12 publisher
request envelope. Isolated matching succeeded; publisher-disabled replay emitted
zero history events; and QueryService, CLI, HTTP API, and MCP returned identical
provenance. A separate read-only post-test check found zero `makolet_live%`
databases afterward. Earlier same-day runs at `96180312...` remain historical and
are not relabelled as this digest.

## Benchmarks

Fast deterministic generator tests run offline under `tests/benchmark/`; they are not
performance claims. Measured workloads run separately:

```text
MAKOLET_BENCHMARK_DATABASE_CONFIRM=makolet_benchmark
uv run makolet benchmark run --quick
uv run makolet benchmark run --standard
```

From a source checkout, the underlying developer entry points are
`uv run python -m benchmarks.run --profile quick` and `--profile standard`; the
canonical CLI wrapper delegates to those profiles and is the command used in release
evidence.

Database scenarios require a dedicated URL in
`MAKOLET_BENCHMARK_DATABASE_URL` and exact
`MAKOLET_BENCHMARK_DATABASE_CONFIRM=makolet_benchmark`. The URL must be PostgreSQL
on loopback with no driver query/fragment override and the exact database name
`makolet_benchmark`. The standard profile is the only scale-acceptance
profile. It owns the fixed `makolet_benchmark` schema and records JSON evidence; see
[performance](performance.md) and `benchmarks/README.md` before running it.

Current quiet-host standard acceptance at source digest
`0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805`
is `benchmarks/results/20260819-standard-all-0daaa40f.json`. It completed the exact
one-million/ten-million `scenario=all` workload, passed the 70% floors against the
`b9aababf` baseline, and passed the enforced plan gate with zero failures. The
446,434-byte artifact has SHA-256
`ec4a0e17b8edf0562a70315535cb1ab940e166b13b4f32b6289fef4ea161eade`.
See [performance](performance.md#current-quiet-host-standard-measurement).

The earlier complete artifact at historical digest
`c53ec893cb53dcf21e7e336a70cff0226ca3652c91d975a4e33920489b7fd11d`
remains historical evidence only. The quick artifact
`benchmarks/results/20260816-quick-all-c53ec893.json` is a 23.298273-second diagnostic
only; its SHA-256 is
`e36c6e2df19debec7898569b3405fed8862e4d05275d0661a1e92bfbcc479920`.
The exact standard artifact
`benchmarks/results/20260816-standard-all-c53ec893-e-drive.json` completed the full
workload and passed plans, but failed three performance floors: initial staging
65.56%, reconciliation apply 63.80%, and amplification 23.75%. Do not treat those
complete-scenario flags as a waiver. See
[performance](performance.md#historical-standard-measurement-scale-and-plans-pass-performance-fails).

## Dependency, license, SBOM, and image checks

The canonical vulnerability gate audits an exact frozen export. Running
`pip-audit` directly against the environment tries to resolve the unpublished
editable Makolet project and is not equivalent.

```powershell
$auditRequirements = New-TemporaryFile
uv export --frozen --all-groups --no-emit-project --format requirements-txt --output-file $auditRequirements
uv run pip-audit --requirement $auditRequirements --no-deps --disable-pip --progress-spinner off --strict
uv run pip-audit --requirement build-constraints.txt --no-deps --disable-pip --progress-spinner off --strict
Remove-Item -LiteralPath $auditRequirements

uv run python scripts/check_secrets.py
uv run python scripts/check_licenses.py
uv run python scripts/check_build_constraints.py
uv export --preview-features sbom-export --frozen --all-groups --format cyclonedx1.5 --output-file .ci-sbom.json
uv run python scripts/check_sbom.py sbom.cdx.json .ci-sbom.json
uv run python scripts/check_container_images.py
uv run python scripts/check_distribution.py --reproducible
```

The reproducibility gate performs two isolated builds from the same tree with
`SOURCE_DATE_EPOCH=946684800` (2000-01-01 UTC), UTC, a fixed hash seed, disabled
network/Python downloads, and the committed hash-constrained PEP 517 closure. It
requires exhaustive allowlisted wheel and sdist inventories, exact source bytes, and
a bounded text scan that rejects credentials, personal host paths, benchmark results,
repository-only state, and caches. It also checks the exact `Requires-Python`, complete
runtime `Requires-Dist`, `benchmark` extra, console entry point, license metadata and
files, Alembic configuration, and migration graph. It then requires byte-for-byte and
SHA-256 equality between the two builds. Finally it builds a wheel from the generated
sdist, requires that wheel to equal the first wheel, installs it and its exactly pinned
runtime dependencies from the offline uv cache into a temporary virtual environment,
verifies all 59 advertised CLI help paths plus the installed metadata, migration,
benchmark, and license evidence outside the source tree. All subprocesses,
artifact/member counts and sizes, text inputs, output, runtime, and temporary paths are
bounded; the temporary tree is removed on success or failure.

The default gate remains offline and does not require a database. After a caller has
created a uniquely named, empty PostgreSQL 18 database on a literal loopback address,
the installed-wheel migration proof can be added explicitly:

```powershell
$env:MAKOLET_DISTRIBUTION_TEST_DATABASE_URL = '<caller-provided-loopback-url>'
$env:MAKOLET_DISTRIBUTION_TEST_DATABASE_CONFIRM = '<exact-database-name>'
uv run python scripts/check_distribution.py --reproducible --verify-installed-postgres
```

The exact database name and confirmation must match
`makolet_test_distribution_<8-to-32-lowercase-hex-digits>`. The gate first proves that
the database is empty, then runs only the installed wheel's `database status`,
`database migrate`, and `database status` commands and requires PostgreSQL 18 and head
`0011_resource_probe_budgets`. It never creates or drops a database; the caller owns
both actions and must remove the disposable database after the command returns.

Historical recorded evidence: on 2026-08-16 the then-current gate produced two byte-identical
102-member wheels with SHA-256
`88fc3755a49c008364632089670b38e193d72686c19ef7a3d727b8df435778d5`
and two byte-identical 275-member sdists with SHA-256
`030d3053435d57067fb4d1e0aa9721a69efb86472c4cbb1521bfab29534f129f`.
Rebuilding from the sdist produced the same wheel. The isolated offline installation
loaded the packaged benchmark with exact `psutil==7.2.2`, selected the packaged
Alembic configuration, then migrated an empty PostgreSQL database to the then-current
`0009_collection_charge_budgets`, reported `schema_ready=true`, and passed the
license/notice and installed CLI checks. The disposable database and both temporary
distribution trees were removed. The benchmark source digest remained unchanged.
Later migrations and the seven `dcd9f5e6-...` remediations changed the package and
runtime inputs, so repeat reproducibility, isolated installation, migration inventory,
dependency/license/SBOM, and image-pin checks after the final source freeze.

Current release-candidate evidence on 2026-08-18 used a caller-created, uniquely named
loopback PostgreSQL 18 database `makolet_test_distribution_0daaa40f` and the exact
command above. Two builds, the wheel rebuilt from the sdist, and the isolated install
were byte-identical: the 107-member wheel SHA-256 was
`9dc8e81892dfe74b7dbd77f48d34491798ed3907465cb4e50e6b51d9c0ec6440`
and the 291-member sdist SHA-256 was
`e81db8ff2976931a95249368e26b737146422d7b326186874b53568a30901b45`.
The installed 59-command CLI and benchmark extra passed, and the installed wheel moved
the empty database through status/migrate/status to exact head
`0011_resource_probe_budgets`. The caller then dropped only that database and confirmed
it absent. This evidence is bound to source digest
`0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805`.

The content policy requires the wheel to contain the packaged Alembic graph, both
license/notice files, and the explicit `benchmark` extra. The sdist must also carry
the root security policy and private-reporting instructions. The gate rejects
generated benchmark results, repository agent instructions, personal or absolute
local-host paths, credentials, caches, unsafe archive members, and incomplete
migration inventories in both artifacts. The sdist deliberately retains the test
suite, documentation, deployment helpers, and audit artifacts needed for downstream
source verification; generated benchmark results and local agent state are not
distribution inputs. To validate already-built artifacts without rebuilding them,
pass their sole output directory to `scripts/check_distribution.py`.

The license allowlist is a reviewed repository artifact, not permission to add a new
dependency without checking its complete transitive set and updating
`THIRD_PARTY_NOTICES.md`, the applicable SBOM, and constraints. Package metadata
is not sufficient evidence for a native executable: the detailed audit records
the non-distributed Ruff tooling exception and embedded component review.

`build-constraints.txt` is the complete isolated PEP 517 closure, not a runtime or
development group. To propose a Hatchling update, generate a candidate in a temporary
path first:

```powershell
$candidate = Join-Path $env:TEMP ('makolet-build-constraints-' + [guid]::NewGuid() + '.txt')
'hatchling==1.32.0' | uv pip compile - --python-version 3.14 --universal --generate-hashes --no-header --output-file $candidate
```

Audit every candidate artifact/license, update the exact pins in `[tool.uv]` and
`sbom.build.cdx.json`, and pass the isolated `uv build --require-hashes` gate before
replacing the committed constraints. Do not treat generated hashes as approval.

After building `makolet:local`, reproduce the exact Linux runtime inventory from
inside that image:

```powershell
docker run --rm --user 10001:10001 --volume "${PWD}:/audit" --entrypoint python makolet:local /audit/scripts/generate_runtime_sbom.py --output /audit/.ci-runtime-sbom.json
uv run python scripts/check_sbom.py --runtime-semantic sbom.runtime-linux.cdx.json .ci-runtime-sbom.json
```

The runtime SBOM includes the 41 installed Python distributions, CPython, all 97
Debian packages, machine-readable license labels, license/copyright evidence hashes,
source-package versions, and native dynamic-link evidence for the pinned image. The
runtime-semantic comparison checks every stable CycloneDX field, including component
types, purls, bom-refs, license and evidence values, base-image identity, and dependency
edges. It treats array order as non-semantic and ignores only the documented
host-kernel-dependent value of `makolet:platform`; an absent/duplicate platform field
or any other difference fails. `check_secrets.py` scans tracked
and untracked repository files using bounded, deterministic patterns and self-checks
positive and negative samples; CI invokes the same local command. It complements, but
does not replace, dependency auditing or an approved security review.

Latest completed runtime evidence on 2026-08-18 at source digest
`0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805`:
image `makolet:local` ID
`sha256:1cdf91b26475121d1c34edf3797187a81c8172e76b8a4d5eb38e7efb5dcc64f9`
produced a 155,406-byte, 139-component runtime SBOM. The committed and freshly
generated documents were byte-identical at SHA-256
`d6a06d38bd3bfdd868299add1473de06a160aa1d216a02b0b375bb4c39b66508`,
the runtime-semantic gate passed, and all six immutable image pins passed. Isolated
container smoke `makolet-smoke-local-720` then passed 1,355 selected tests with six
capability skips, measured 85.94% branch coverage, restored PostgreSQL to head
`0011_resource_probe_budgets`, and left zero smoke project containers, volumes, or
networks. Quiet-host standard performance later passed on the same digest with
`benchmarks/results/20260819-standard-all-0daaa40f.json`; do not relabel the
historical `c53ec893...` failure as current acceptance.

## CI mapping

`.github/workflows/ci.yml` runs locked setup, format/lint/type checks, shell syntax,
dependency vulnerability/license/SBOM/image checks, offline tests without a false
global coverage claim, and the container smoke with the combined 85% real-service
coverage gate. The live workflow is separate by design. All CI commands shown above
can be run locally with the same repository files.

## Required evidence before a release

Record exact commands, environment/service versions, pass/skip/failure counts,
coverage, migration revision, container digest/build result, backup/restore result,
and benchmark result paths in the living ExecPlan. A missing service that causes an
integration skip is not a passing integration gate, and a live external outage is not
a product success.
