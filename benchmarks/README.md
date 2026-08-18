# Makolet benchmarks

These opt-in scenarios generate deterministic, independently authored data. They use
bounded generators and a fixed disposable PostgreSQL schema named
`makolet_benchmark`; application tables in `public` are not truncated or measured.
Source checkouts obtain the benchmark dependency through the development group. A
wheel installation must select the explicit `makolet[benchmark]` extra before using
the packaged `makolet benchmark run` command.

Set a test-only PostgreSQL URL, then run the diagnostic profile:

```powershell
$env:MAKOLET_BENCHMARK_DATABASE_URL = '<postgresql+asyncpg test URL>'
$env:MAKOLET_BENCHMARK_DATABASE_CONFIRM = 'makolet_benchmark'
uv run python -m benchmarks.run --profile quick
```

The release/scale run retains the same two variables:

```powershell
uv run python -m benchmarks.run --profile standard
```

`standard` streams one million PriceFull records, stages and applies one million
normalized price records (100,000 products across 10 stores) through the production
PostgreSQL repository, reconciles a later full snapshot, extends the dataset to 100
stores and exactly ten million current-price rows, and measures search, barcode,
comparison, and history queries with analyzed buffer-aware plans.
Only that profile is scale-acceptance evidence. `smoke` and `quick` exist for code and
environment diagnostics.

The standard parser and database scenarios may be measured separately with
`--scenario parser` and `--scenario database` to avoid resource contention. Such a
result sets whole-result `acceptance_evidence` to false and records the completed
workload under `scenario_acceptance_evidence`; only `--scenario all` claims that both
ran in one invocation. Release notes may combine the two separately measured standard
scenario artifacts, but must name both files and must not imply the unrun scenario is
present in either one.

The schema is dropped in a `finally` block. If the process or machine is terminated,
recover with the following exact command after confirming no benchmark is running:

```sql
DROP SCHEMA IF EXISTS makolet_benchmark CASCADE;
```

Pass `--keep-schema` only for manual plan inspection, then perform the same cleanup.
Result JSON redacts the database password and records the full inputs, environment,
durations, process RSS, row rates, query distributions, database size, and plans. It
also records a Git revision when one exists and a deterministic SHA-256 over the
source, migrations, benchmark code, lock, and build constraints so an uncommitted
measurement still identifies its exact benchmark-relevant inputs.
