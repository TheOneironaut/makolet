# Complete and verify the Makolet data platform

## Purpose and outcome

Deliver the self-hosted Israeli supermarket price-data platform in the initiating
goal: clean-room collection, immutable raw archival, normalized PostgreSQL current
state and history, canonical product matching/search, CLI, HTTP API, MCP, scheduled
worker, Parquet export, operations, security, tests, benchmarks, and honest retailer
coverage. A consumer website is out of scope. Publisher access controls, data rights,
bounded resource use, exact provenance, and no-copy clean-room rules remain hard
boundaries.

## Current status

2026-08-19T02:16:09+03:00 - Phase 8 verification is terminal on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` and first local `main` commit `54ffb79666753aab42986c8fabc53d16c5449d56`. Quiet-host standard `scenario=all` passed with `benchmarks/results/20260819-standard-all-0daaa40f.json` (SHA-256 `ec4a0e17b8edf0562a70315535cb1ab940e166b13b4f32b6289fef4ea161eade`). A clean local clone of that commit on C reproduced frozen setup, advertised CLI help, static checks, offline pytest 1,281 passed / 6 skipped / 77 deselected, and core-service acceptance: disposable `makolet_test_clone_54ffb79` on loopback PostgreSQL 18.4 `127.0.0.1:55434` migrated to head `0011_resource_probe_budgets` with `schema_ready=true` and `makolet doctor` `ok=true`. The clone, test database, and credential file were removed. No remote exists. Phase 8 and overall completion are now marked complete. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-19T01:57:00+03:00 - First main commit 54ffb79666753aab42986c8fabc53d16c5449d56 exists locally with no remote. Quiet-host standard scenario=all passed on digest 0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805 via benchmarks/results/20260819-standard-all-0daaa40f.json. A clean local clone of that commit on C reproduced frozen uv sync --all-groups --frozen, uv lock --check, Ruff format/lint, mypy 176 files, docs, secrets, advertised CLI help, and offline pytest 1281 passed / 6 skipped / 77 deselected. makolet doctor without a database correctly reported database_unavailable while archive/sources/ingestion/operations/worker/export were ok. The disposable clone was deleted. Phase 8 remains open for remaining documented core-service acceptance that needs isolated PostgreSQL from a clean clone, then the final ExecPlan close. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-19T00:39:27+02:00 - Remote quiet-host standard `scenario=all` on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` completed and passed every measured floor. Artifact `benchmarks/results/20260819-standard-all-0daaa40f.json` (SHA-256 `ec4a0e17b8edf0562a70315535cb1ab940e166b13b4f32b6289fef4ea161eade`) records staging 34,767.74 rows/s (279.7% of 12,427.93), initial apply 3,958.71 rows/s (250.7% of 1,579.00), amplification 2,584.90 rows/s for 10,000,000 rows (178.4% of 1,449.14), reconciliation apply 9,400.38 rows/s, plan_gate passed with zero failures, and `makolet_benchmark` dropped after capture. Isolated remote resources `makolet-bench-0daaa40f`, volume, tree, and port 55518 are gone. Historical `b9aababf` / `c53ec893` artifacts were not overwritten. First commit and clean-clone proof remain; Phase 8 is not complete. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-19T00:10:19+02:00 - Remote quiet-host standard `scenario=all` on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` is still in current-price amplification: `current_prices` n_live_tup = 7,390,433 / 10,000,000, database 9,352 MB. Recent store copies have slowed to about 60k-80k rows/min after autovacuum/`DataFileRead`, still above the 70% amplification floor of 1,449.14. Isolated PostgreSQL remains `makolet-bench-0daaa40f` on `127.0.0.1:55518`. Client pid 1255967 / python 1255971 still alive (~41 min). No result JSON yet; queries, plan gate, and cleanup remain after amplification. First commit and Phase 8 remain deferred. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-18T23:56:24+02:00 - Remote quiet-host standard `scenario=all` on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` is still in current-price amplification: `current_prices` = 6,100,000 / 10,000,000, database ~8.4 GB. Store copies have slowed from the early 700k/min burst to about 100k-200k rows/min but remain above the 70% amplification floor of 1,449.14. Isolated PostgreSQL remains `makolet-bench-0daaa40f` on `127.0.0.1:55518`. Client pid 1255967 / python 1255971 still alive. No result JSON yet; queries, plan gate, and cleanup remain after amplification. First commit and Phase 8 remain deferred. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-18T23:49:15+02:00 - Remote quiet-host standard `scenario=all` on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` is halfway through current-price amplification: `current_prices` = 5,000,000 / 10,000,000 at 23:49:02, database ~7.1 GB. Recent store copies are 200,000-700,000 rows/min and remain far above the 70% amplification floor of 1,449.14. Isolated PostgreSQL remains `makolet-bench-0daaa40f` on `127.0.0.1:55518`. Client pid 1255967 / python 1255971 still alive. No result JSON yet; queries, plan gate, and cleanup remain after amplification. First commit and Phase 8 remain deferred. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-18T23:44:02+02:00 - Remote quiet-host standard `scenario=all` on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` is in current-price amplification. `current_prices` grew from 1,600,000 at 23:41:57 to 3,000,000 at 23:43:58, about 700,000 additional rows/min (~11,667 rows/s), well above the 70% amplification floor of 1,449.14. Isolated PostgreSQL remains `makolet-bench-0daaa40f` on `127.0.0.1:55518`. Client pid 1255967 / python 1255971 still alive. No result JSON yet; queries, plan gate, and cleanup remain after amplification. First commit and Phase 8 remain deferred. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-18T23:40:54+02:00 - Remote quiet-host standard `scenario=all` on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` passed reconciliation apply. `full_snapshot_reconciliation.apply_completed` measured 105.314863 s / 9,400.38 rows/s for 990,000 records (10,000 unavailable, 990,000 unchanged), 818.4% of the `b9aababf` reconciliation-apply baseline 1,148.61. Staging 34,767.74 and initial apply 3,958.71 remain above their 70% floors. Isolated PostgreSQL remains `makolet-bench-0daaa40f` on `127.0.0.1:55518`. Client pid 1255967 / python 1255971 still alive. No result JSON yet; amplification, queries, plan gate, and cleanup remain. First commit and Phase 8 remain deferred. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-18T23:38:48+02:00 - Remote quiet-host standard `scenario=all` on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` passed initial apply. `initial_full_snapshot.apply_completed` measured 252.607498 s / 3,958.71 rows/s for 1,000,000 inserted rows (2,000,000 history events; 0 unavailable/unchanged/updated), 175.5% of the `b9aababf` apply baseline 2,255.72 and 250.7% of the 70% floor 1,579.00. Staging remains 34,767.74 rows/s. Duplicate archive CAS detection completed in 0.008307 s. Isolated PostgreSQL remains `makolet-bench-0daaa40f` on `127.0.0.1:55518`. Client pid 1255967 / python 1255971 still alive. No result JSON yet; amplification/reconciliation/query/plan/cleanup remain. First commit and Phase 8 remain deferred. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-18T23:35:00+02:00 - Remote quiet-host standard `scenario=all` on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` passed parser and initial staging. `initial_full_snapshot.stage_completed` measured 28.762291 s / 34,767.74 rows/s for 1,000,000 staged rows, 279.7% of the 70% floor (12,427.93) and 195.8% of the `b9aababf` baseline 17,754.18 rows/s. Isolated PostgreSQL 18.4 remains `makolet-bench-0daaa40f` on `127.0.0.1:55518`. Apply is now inserting `product_identifiers` (database ~965 MB). Detached client pid 1255967 / python 1255971 still alive. No result JSON yet. First commit and Phase 8 remain deferred until the complete artifact, remaining floors, cleanup, commit, and clean clone are terminal. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-18T23:29:00+02:00 - Quiet-host standard `scenario=all` is now running on the owner Codex remote (`amitay@159.195.61.71`, hostname `v2202512314232412002`) instead of the contended laptop. Digest still `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805`. Isolated disposable tree is `/home/amitay/tmp/makolet-standard-0daaa40f`. Isolated PostgreSQL 18.4-bookworm `@sha256:882236b897e39051d2368c5ccc6cda944904723506b2dfc97f2a8f5bc9afa382` is container `makolet-bench-0daaa40f` on loopback `127.0.0.1:55518` only; owner DBs on 5432/55432/55434 were left untouched. Command: `uv run makolet benchmark run --standard --output benchmarks/results/20260819-standard-all-0daaa40f.json`, detached as remote pid 1255967. Local D/E remain unused. Laptop RAM is still too contended for a local standard start. Phase 8 remains open until this artifact is terminal with all floors, then first commit and clean clone. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-19T00:10:00Z - Isolated PostgreSQL 18.4 now exists on C only, inside the existing Ubuntu WSL VHD at ~/.makolet-local (user-space 18.4 packages, loopback 127.0.0.1:55434, empty makolet_benchmark, leftover schema=0). D/E still have no Makolet leftovers and remain out of scope. The previous E-drive standard leftover is gone. Host RAM is not quiet enough for a new standard start: about 1.18 GiB free, vmmemWSL 3.68 GiB, plus an unrelated 1.0 GiB transcription process. Do not start scenario=all until that contention clears. Phase 8 remains open. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-18T20:55:00Z - Owner forbade further Makolet work on D: or E:. The in-flight standard run on MakoletBenchmark/E was stopped, the distro was unregistered, and leftover Makolet paths on E were deleted. D had none. Future isolated PostgreSQL/benchmark/container work stays on C and must be deleted after use. The 371 GiB Docker path is one VHDX, not loose files. Unused Makolet images and the Docker build cache were pruned; Appwrite/n8n/qdrant were left untouched; no global volume prune. The VHDX is still 371.11 GiB because compact needs administrator rights. Phase 8 remains open: no passing quiet-host standard artifact on digest 0daaa40f. This statement supersedes older current-status entries below without rewriting their dated evidence.

2026-08-18T20:00:00Z - Quiet-host standard `scenario=all` is running detached on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805`. Command: `uv run makolet benchmark run --standard --output benchmarks/results/20260818-standard-all-0daaa40f.json`. Windows client PIDs wrapper 10060 / uv 29076 / python 9144 remain alive, unlike the previous vanished session. Parser finished and PostgreSQL reached `database_sandbox_ready` then `initial_full_snapshot.stage_completed` at 83.399489 s / 11,990.48 rows/s for 1,000,000 staged rows. That staging rate is 67.54% of the `b9aababf` baseline 17,754.18 rows/s and is already below the documented 70% floor (12,427.93). The run is continuing for a complete artifact rather than being aborted. Apply is still inserting `price_history` on backend pid 870 (`WALWrite`, interruptible sleep, LSN advancing through 13/67..., database ~1.53 GiB). No result JSON yet. First commit and Phase 8 remain deferred. This statement supersedes the older current-status entries below without rewriting their dated evidence.

2026-08-18T19:40:00Z - Host is quiet enough to retry the exact standard `scenario=all` on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805`. The leftover apply backend pid 173 is gone, schema `makolet_benchmark` was dropped (database now 9,639,615 bytes, only `public`, zero client backends, zero advisory locks), and the role credential was rotated without being printed. Docker Desktop is stopped, the previous 6 GiB bun hog is gone (`bun` ~0.45 GiB), free RAM is 2.54 GiB of 15.86 GiB, C has ~36 GiB free and E has 1.52 TiB. Dedicated PostgreSQL 18.4 remains on loopback `127.0.0.1:55434`. Command: `uv run makolet benchmark run --standard --output benchmarks/results/20260818-standard-all-0daaa40f.json`, launched detached so the Windows client cannot vanish with the Codex session. Historical artifacts are not overwritten. First commit and Phase 8 remain deferred until this run is terminal with all floors. This statement supersedes the older current-status entries below without rewriting their dated evidence.

2026-08-18T16:25:00Z - The quiet-host standard `scenario=all` start on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` is not terminal. The Windows client finished the parser scenario and reported `initial_full_snapshot.stage_completed` at 75.711956 seconds / 13,207.95 rows/s for 1,000,000 staged rows, then disappeared with empty stderr and no result artifact `benchmarks/results/20260818-standard-all-0daaa40f.json`. Dedicated PostgreSQL 18.4 on `127.0.0.1:55434` remains up. Database size is 1,856,231,103 bytes with schema `makolet_benchmark` still present. Orphan backend pid 173 (`makolet-scale-benchmark`) is still executing `INSERT INTO price_history` in uninterruptible disk sleep (`State: D`, wait `WalSync`) after about 14,012 seconds. `SIGTERM`/`SIGKILL` did not reap it. This is incomplete apply leftover, not acceptance. Do not start another standard run until that backend exits or `MakoletBenchmark` is crash-recovered and the schema is dropped. First commit and Phase 8 remain deferred. This statement supersedes the older current-status entries below without rewriting their dated evidence.

2026-08-18T12:15:00Z - Quiet-host standard `scenario=all` started on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805`. The previous 6 GiB `bun`/OpenCodex hog is gone (`bun` now 0.32 GiB). Docker Desktop and the unrelated Appwrite/n8n/qdrant stack are stopped. Dedicated `MakoletBenchmark` PostgreSQL 18.4 is running on loopback `127.0.0.1:55434` after creating `/var/run/postgresql` and removing a stale pidfile. Preflight on `makolet_benchmark` showed only `public`, one backend, 9,254,591 bytes, and zero advisory locks. The role credential was rotated without being printed. Host memory at start is 1.14 GiB free of 15.86 GiB after WSL/PostgreSQL came up; C has 36.69 GiB free and E has 1.52 TiB. Command: `uv run makolet benchmark run --standard --output benchmarks/results/20260818-standard-all-0daaa40f.json`. Historical artifacts are not overwritten. First commit, clean clone, and Phase 8 completion remain deferred until this run is terminal. This statement supersedes the older current-status entries below without rewriting their dated evidence.

2026-08-18T03:12:00Z - Phase 8 is blocked on an external quiet-host requirement. Digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` is unchanged and still unfrozen. The same host-contention blocker has now been confirmed on three consecutive goal turns after the owner restart: `bun` still holds about 6.07 GiB, free RAM is 1.41 GiB of 15.86 GiB, Docker Desktop plus Appwrite/n8n/qdrant/mission-control remain up, and `MakoletBenchmark` is stopped with port 55434 closed. Starting standard `scenario=all` here would reproduce the historical contended failure, not accept it. First commit and clean-clone proof remain deferred because they require a passing quiet-host standard artifact on this digest. Overall completion is not claimed. This statement supersedes the older current-status entries below without rewriting their dated evidence.

2026-08-18T03:05:00Z - Phase 8 remains blocked on a quiet-host standard `scenario=all` rerun. Digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` is unchanged. Canonical offline pytest on this digest passed 1,281 selected tests with six documented capability skips, 77 deselections, and one known Starlette/httpx warning in 175.94 seconds. Cheap current-tree gates remain green, including frozen sync/lock, Ruff, mypy, docs, secrets, build-constraints, image pins, license coverage, locked/runtime SBOM comparison, and shell/PowerShell syntax. This host is still not quiet (`bun` ~6.07 GiB, under 2 GiB free RAM, unrelated Compose stack still up; `MakoletBenchmark` stopped, port 55434 closed). First commit and clean-clone proof remain deferred. Overall completion is not claimed. This statement supersedes the older current-status entries below without rewriting their dated evidence.

2026-08-18T02:55:00Z - Phase 8 remains blocked on a quiet-host standard `scenario=all` rerun. Digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` is unchanged. This host is still not quiet after the owner restart: 0.37-1.72 GiB free RAM of 15.86 GiB, `bun` holding 6.08 GiB, Docker Desktop plus Appwrite/n8n/qdrant/mission-control still up, and `MakoletBenchmark` stopped with port 55434 closed. Cheap current-tree gates remain green: `uv sync --all-groups --frozen`, `uv lock --check`, Ruff format/lint, mypy 176 files, docs, secrets, build-constraints, image pins, license coverage, locked/runtime SBOM comparison, `bash -n scripts/*.sh`, and `scripts/operations.ps1` parse. A disposable 2,000-row amplification-plan diagnostic on isolated `makolet_test_ampplan_20260818` showed the statement trigger `trg_current_prices_project_insert` costing 60.548 ms versus 20.488 ms for the INSERT itself; that write-amplification exists in the passing `b9aababf` baseline too and cannot explain the historical last-10-store cliff of 810-1790 s/store. The diagnostic database was dropped and only `makolet_test` remains. First commit and clean-clone proof remain deferred. Overall completion is not claimed. This statement supersedes the older current-status entries below without rewriting their dated evidence.

2026-08-18T02:42:00Z - Phase 8 verification remains blocked on quiet-host standard performance. Digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` is unchanged. After the owner-reported restart, this host is still not quiet: 0.76 GiB free RAM of 15.86 GiB, `bun` holding 6.08 GiB, Docker Desktop WSL plus Appwrite/n8n/qdrant/mission-control still running, and `MakoletBenchmark` stopped with port 55434 closed. Cheap current-tree gates were rechecked without services: `uv lock --check`, Ruff format/lint, mypy `src tests benchmarks` (176 files), docs (43 Markdown / 100 links), and secrets (317 files). `git diff --check` on a dry-run index was not commit-clean because historical `benchmarks/results/*.json` keep CR bytes and a few non-digest files had an extra blank line at EOF; `.gitattributes` now ignores those historical whitespace checks, and extra EOF blanks were stripped only outside the digest. First commit and clean-clone proof remain deferred. Overall completion is not claimed. This statement supersedes the older current-status entries below without rewriting their dated evidence.

2026-08-18T02:20:00Z - Phase 8 verification continued on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` after the Matrix empty-wrapper fix. Isolated PostgreSQL/S3 integration and e2e passed 74 tests against disposable `makolet_test_phase8_matrix_20260818`, which was then dropped while `makolet_test` remained. All seven credential-free HTTPS listing smokes passed, including a now-nonempty `hyper-cohen` catalog. Representative live ingestion passed both tests in 12.28s with exact disposable cleanup. Reproducible distribution plus installed PostgreSQL proof passed on `makolet_test_distribution_0daaa40f` (107-member wheel `9dc8e81892dfe74b7dbd77f48d34491798ed3907465cb4e50e6b51d9c0ec6440`, 291-member sdist `e81db8ff2976931a95249368e26b737146422d7b326186874b53568a30901b45`). License, build-constraint, image-pin, secret, docs, pip-audit, and locked SBOM gates passed. Image `sha256:1cdf91b26475121d1c34edf3797187a81c8172e76b8a4d5eb38e7efb5dcc64f9` reproduced the committed 139-component runtime SBOM `d6a06d38bd3bfdd868299add1473de06a160aa1d216a02b0b375bb4c39b66508`. Isolated container smoke `makolet-smoke-local-720` passed with 1,355 tests, six capability skips, 85.94% branch coverage, authenticated database/archive backup-restore to head `0011_resource_probe_budgets`, and exact project cleanup. Final Standard scan `3f46a47c-b8ce-45ef-8f60-fcd0e10ede7f` sealed zero findings on snapshot `codex-security-snapshot/v1:sha256:8198c379e4231ac72e81541e575f62ad45ef1558573e52091976f4f813a1af11` with partial coverage of the six assigned threat surfaces. TAC advisory failed `USER_NOT_LOGGED_IN`. Quiet-host standard performance, first commit, and clean-clone proof remain pending. Overall completion is not claimed. This statement supersedes the older current-status entries below without rewriting their dated evidence.

2026-08-18T01:50:00Z - Standard scan `4edfe7af-5754-47e1-b37e-18f15b8659d6` sealed one Medium finding on the pre-fix tree: Matrix accepted empty named wrappers such as `{"files": []}` as a complete empty catalog. That path now fails closed with the same expected-collection error as a direct `[]`, and focused Matrix tests pass 10 cases. The post-fix source digest is `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` and is not frozen. Combined container/coverage, quiet-host standard performance, a final scan of this digest, first commit, and clean-clone proof remain pending. Overall completion is not claimed. This statement supersedes the older current-status entries below without rewriting their dated evidence.

2026-08-18T01:10:00Z — Phase 8 verification continued on the working tree after a
BINA empty-partition discovery fix. Canonical offline pytest now passes 1,276
selected tests with six documented capability skips and 77 deselections. Isolated
PostgreSQL/S3 integration and e2e passed 74 tests against
`makolet_test_phase8_svc_20260818`. Reproducible distribution plus installed
PostgreSQL proof passed on `makolet_test_distribution_a18c4e72` with the same
wheel/sdist hashes as the earlier packaging run. Representative live ingestion
passed twice, including after the BINA change, at source digest
`961803129bfc690679c18f9d1526757dc2dedc4d0c25e5dae3cfeca67c0978cb`. Six of seven
credential-free HTTPS listing lanes passed; `hyper-cohen` failed closed on a
publisher-empty Matrix catalog. Combined container/coverage, quiet-host standard
performance, final security scan, first commit, and clean-clone proof remain
pending. Overall completion is not claimed. This statement supersedes the older
current-status entries below without rewriting their dated evidence.

2026-08-18T00:21:35Z — FTP/FTPS security remediations are implemented on the
working tree and verified by focused offline gates. Public FTP listing and
download now reject more than four unique public DNS answers. Listing charges
each vetted-address connect as its own request. FTPS meters TLS ciphertext,
including handshake records, below protocol parsing. Durable FTP/FTPS
reservations now cover the 256 KiB control channel, four address attempts, and
TLS 1.3 framing through the 16 GiB object ceiling. The current source digest is
`0628bb68adb9ddba916c86a9167097029b660633a26722313963faaf2b67b641`; it is
recorded but not frozen. Focused verification passed 190 unit tests, Ruff and
mypy on the touched files, the repository secret scan (317 files), and the
documentation link check (43 Markdown files / 100 local links). The
`3fc095d6-...` sealed scan and earlier container/live/distribution/benchmark
artifacts predate these remediations. Remaining Phase 8 frozen-tree service,
container, live, supply-chain, quiet-host standard performance, final security
rescan, initial-commit, and clean-clone gates are pending; overall completion
is not claimed. This statement supersedes the older current-status entries
below without rewriting their dated evidence.

2026-08-17T20:13:19Z — Standard scan
`dcd9f5e6-8410-49d0-90f9-533dffb6e20b` sealed the authoritative 312-file pre-fix
snapshot
`codex-security-snapshot/v1:sha256:8d66bc4e0949546c8a6e2098254eca69e8b2d58bcfcb7c9901ff77294f735dc1`
with complete coverage and seven findings (five Medium, two Low). All seven are now
remediated on the working tree: MCP diagnostic fan-out, public-page materialization,
lease-engine statement-timeout propagation, HTTP DNS/control accounting for listing
and artifacts, and runtime/recovery S3 response pre-parsing. The current no-service
selection passed 1,164 tests with six documented capability skips, 77 deselections,
and one known warning in 165.60 seconds; whole-tree Ruff format (238 files), lint,
and mypy (173 files) are green. The seven-test PostgreSQL 18.4 public-query contract
also passes. The source digest is not yet frozen. The
`984d43e4...` container/live evidence and earlier distribution/image/benchmark
artifacts predate these fixes and are historical. Frozen-tree PostgreSQL/S3,
container/coverage/recovery, applicable live, supply-chain/image, standard
performance, post-fix scan, initial-commit, and clean-clone gates remain pending;
overall completion is not claimed. This statement supersedes the older current-status
entries below without rewriting their dated evidence.

2026-08-17T17:04:58Z — at that checkpoint, the benchmark-relevant digest was
`984d43e4fbe44a03bd3a2fab5cf6acbaffd13f16bd1cf9a312bb9eed8be953da`.
The then-current container smoke exited 0 under isolated Compose project
`makolet-smoke-local-1298` and migrated PostgreSQL 18.4 to
`0011_resource_probe_budgets`. Its combined process collected 1,215 cases, selected
1,212, passed 1,206, skipped six expected capability cases, deselected three live
cases, emitted eight known warnings, and measured 85.77% branch coverage in 311.81
seconds. HMAC-authenticated PostgreSQL backup/restore returned the database to head;
raw-archive HMAC backup, source verification, clean isolated-target restore, and
target re-verification passed for all 18 objects. The post-restore API check passed,
and exact-project cleanup left zero containers, volumes, or networks. Current
image/runtime-SBOM, reproducible-distribution, dependency/license/SBOM,
standard-performance, final post-fix scan, initial-commit, and clean-clone evidence
remain pending; overall completion is not claimed.

2026-08-17T11:08:39Z — the bounded process, archive-backup, worker-shutdown, and
Parquet-export follow-up is complete at benchmark-relevant digest
`b0e6d4b80440387df669727fe48334d13d7b39cd3598a90cda186360870c6c49`.
Independent exploit review found no blocker on `_process.py`
`38998fa4605ac96b85121736d979ed8b4430d5f0e103165787c88b75fcb93762`
and PostgreSQL export
`28de1148dc65ed76acfba38a37642797a452f6312e4f0f62c060c5ae5fb9503e`.
The final no-service command selected 983 cases and passed 977 with six explicit
Windows/POSIX capability skips, 75 deselections, and one known Starlette/httpx
warning. Ruff formatted 234 files, Ruff lint passed, mypy passed 169 source files,
documentation passed 43 Markdown files and 99 links, and the secret scan passed 309
repository files. A final hostile-order PostgreSQL/S3 slice passed six tests and
proved its exact disposable database absent afterward. The broader performance,
sealed security-rescan, initial-commit, and clean-clone work remains deliberately
pending and is not claimed by this checkpoint.

2026-08-17T01:52:35Z — all current-tree live, container, reproducible-distribution,
image/runtime-SBOM, and functional scale/plan evidence is recorded at benchmark digest
`c53ec893cb53dcf21e7e336a70cff0226ca3652c91d975a4e33920489b7fd11d`.
The current 102-member wheel is
`88fc3755a49c008364632089670b38e193d72686c19ef7a3d727b8df435778d5`,
the 275-member sdist is
`030d3053435d57067fb4d1e0aa9721a69efb86472c4cbb1521bfab29534f129f`,
and image
`sha256:f8079873d7f7807ba988c48ca99d76301589b82254cfa646f4028ac9c04e1aab`
contains the exact 139-component runtime SBOM at
`d6a06d38bd3bfdd868299add1473de06a160aa1d216a02b0b375bb4c39b66508`.
The standard artifact is complete, has exact scale counts, and passes the enforced
eight-query/four-apply plan inventory, but performance acceptance fails at 65.56% for
initial staging, 63.80% for reconciliation apply, and 23.75% for amplification. A
quiet-host rerun remains required; the final sealed read-only security scan and first
intentional commit/clean-clone proof also remain. Phase 8 and overall completion are
therefore not claimed.

2026-08-16T18:27:50Z — final current-tree live acceptance is complete at
benchmark-relevant digest
`c53ec893cb53dcf21e7e336a70cff0226ca3652c91d975a4e33920489b7fd11d`.
Read-only preflight found PostgreSQL 18.4 and SeaweedFS 4.40 healthy on loopback,
database-create privilege available, and zero existing live-acceptance databases or
prefix objects. The Maayan 2000 five-partition listing preflight passed. The exact
representative command passed both tests in 5.75 seconds through one newly completed
Price/store-001 file, bounded immutable S3 archival, `retail-xml/10` parsing and
PostgreSQL apply, isolated matching, publisher-forbidden unchanged replay with zero
history events, and QueryService/CLI/API/MCP provenance parity. A separate post-run
query returned zero matching databases and zero `live-acceptance/` objects. The six
other documented credential-free HTTPS listing smokes then passed sequentially, so
all seven current listing lanes are green without retaining source bytes. Completion
was not claimed at that checkpoint; the current status above supersedes its remaining
gate list.

2026-08-16T18:05:17Z — evidence reconciled immediately after the current-tree
container smoke exited 0 under isolated Compose project
`makolet-smoke-local-1745`. It migrated PostgreSQL 18.4 to
`0009_collection_charge_budgets`, proved the API and worker non-root/read-only plus
all API/worker/SeaweedFS Prometheus targets healthy, passed the combined 859-test
gate with five host-capability skips, three live deselections, four known warnings,
and 86.88% branch coverage, then completed authenticated database and raw-archive
backup/verify/restore. The archive target was verified again after restore; the
restored API queries and restarted API/worker/Prometheus stack were healthy. Cleanup
left zero containers, volumes, or networks for the exact project. The observed
`makolet:local` ID was
`sha256:f8079873d7f7807ba988c48ca99d76301589b82254cfa646f4028ac9c04e1aab`;
at this checkpoint it was smoke evidence only. The later byte-for-byte runtime-SBOM
confirmation promoted the same immutable image ID to the current fixed point.

2026-08-16T17:22:56Z — sealed release scan
`853a74cc-8dd6-4f44-9419-3a027f060d82` reported eleven findings (ten Medium, one
Low); every named source-to-sink path is now remediated. The independent closure read
confirmed ten and exposed one remaining nested-SubChain identity fallback; parser
`retail-xml/10` now rejects that omission at file scope before accepting records.
Current no-service/static checks are green. Benchmark-relevant source is now
`c53ec893cb53dcf21e7e336a70cff0226ca3652c91d975a4e33920489b7fd11d`.
The canonical standard artifact at `b9aababf...` and the then-current live,
distribution, and image/runtime-SBOM fixed points below predated these security
changes. This dated statement is superseded by the current evidence above. Completion
is still not claimed because performance acceptance, the final sealed scan, and the
initial-commit clean clone remain pending.

PostgreSQL 18.4 at migration head `0009_collection_charge_budgets` now has real
`EXPLAIN (ANALYZE, BUFFERS)` proof for unfiltered promotion-history and freshness
first/cursor pages. Candidate CTE output was exactly `limit + 1`; the freshness CTE
returned at most 1,001 probe rows per selected store, used the intended count and
latest-contributor indexes, capped 1,005-item stores at 1,000 with
`items_truncated=true`, and retained the globally latest provenance even when that row
was outside the count sample. One calibration bitmap scan visited 1,005 matching
tuples before its `Limit` emitted 1,001, so the proof is an output-cardinality and
index-use bound, not a universal physical tuple-visit ceiling.

The last pre-remediation representative Maayan 2000 live acceptance completed on its
then-current publisher data:
one 28,705-byte ZIP-wrapped Price object yielded 719 accepted records in three
publisher requests, archived bytes/digest matched PostgreSQL and S3, replay made no
publisher request and created zero history events, and QueryService, CLI, HTTP, and
MCP returned the same provenance. Both disposable database and S3 scopes were empty
after cleanup. At that time XML parser `retail-xml/8` and the BINA partition
configuration recorded only the observed compression and direct-field variations that
the run proved. The current `/10` live acceptance above supersedes this historical
publisher observation.

The current isolated container smoke completed under
`makolet-smoke-local-1745`. Its observed `makolet:local` ID
`sha256:f8079873d7f7807ba988c48ca99d76301589b82254cfa646f4028ac9c04e1aab`
passed migration through `0009`, health/readiness and demo queries,
non-root/read-only API and worker execution, three healthy Prometheus targets, the
combined 859-test/86.88%-coverage gate, authenticated PostgreSQL backup/atomic
restore, authenticated S3 archive backup/verify/restore with target re-verification,
and post-restore public queries after the full stack restarted healthy. Zero exact
project containers, volumes, or networks remained. Later distribution/runtime checks
confirmed the same image ID as the final byte-for-byte runtime-SBOM fixed point.

The last pre-remediation distribution reproducibility gate completed after documentation
reconciliation:
two independent offline/hash-constrained builds and the sdist-derived rebuild
produced the identical 100-member wheel
`926cd2e76e450d9a57272b4db62f32f5d0039edbd1ee68c5624fa46e65c65570`
and 271-member sdist
`8422e6fe73605c24bb56536bbf580f83e62e74db1306866269630ec9b8900ecc`.
The rebuilt wheel installed and passed packaged migration/license/notice/CLI checks;
temporary paths were absent. This ExecPlan is excluded from the sdist inventory, so
recording those hashes here does not alter either artifact.

The last pre-remediation image `makolet:local` ID
`sha256:b8d9c27fb20d66e319354e25d5b2b313c5ad7f754795322a7c580cc6e2e112d5`
embedded the exact committed license, notices, and three SBOMs. Its freshly generated
139-component runtime SBOM was byte-identical to the committed document at SHA-256
`d6a06d38bd3bfdd868299add1473de06a160aa1d216a02b0b375bb4c39b66508`;
all 97 Debian components carried machine-readable licenses and the semantic gate
ignored only the documented host-platform value.

The last pre-remediation recovered PostgreSQL environment completed the exact standard
parser/database run. The artifact has `acceptance_evidence=true`, both scenario flags
true, 1,000,000 parsed and normalized rows, 10,000,000 final current-price rows, and a
passing enforced plan gate with zero failures. Bounded monitoring showed objective
LSN/database/VHD progress through every WAL-heavy phase. Post-run cleanup left zero
schema, benchmark sessions, advisory locks, prepared transactions, invalid indexes,
or runners; the credential was rotated without retention, the database returned to
9,287,359 bytes, and offline checksums found zero bad blocks across 1,260 files/3,998
blocks. PostgreSQL shut down cleanly at `9/431B6B00`; only the dedicated distro
stopped, port 55434 closed, and the separate 55432 test service remained available.

The owner-approved `SECURITY.md` publishes `amitibr19@gmail.com` as the private
reporting channel. The latest sealed scan before the runtime-SBOM checker changes,
`bcc531cc-b949-4dde-982e-93e00da343e6`, reviewed
the exact 292-file snapshot
`codex-security-snapshot/v1:sha256:220682500829d7efb1d841a5dad82400c2f1e680cfa72aab982b4b639a1dcbf8`
and found zero reportable findings and no validated Medium-or-higher vulnerability or
regression. Its one Low defense-in-depth residual is the absence of a dedicated
current-tree PostgreSQL index-plan assertion for fuzzy store search; no Medium+
impact was demonstrated. This documentation reconciliation postdates that read-only
snapshot. A final rescan of the frozen post-SBOM tree remains the next gate.

## Milestones

- [x] Phase 1: establish official-source/open-source research, licensing/SBOM
  evidence, the source matrix, architecture decisions, and this living plan.
- [x] Phase 2: implement the locked Python/PostgreSQL/SeaweedFS foundation,
  migrations, domain values, repository layout, quality configuration, and CI;
  a clean local clone of `54ffb79` reproduced frozen setup and static/offline
  checks.
- [x] Phase 3: implement a discovery → exact-byte archive → parse → stage/apply →
  query/replay vertical slice shared by CLI, HTTP, and MCP, including one
  real-PostgreSQL cross-interface end-to-end proof.
- [x] Phase 4: implement the observed BINA, Matrix/Laib, NCR FTP/FTPS, static-daily,
  Hazi Hinam, City Market, and Shufersal source/parser families with bounded listing
  and hostile-input contracts, including current live BINA variation proof.
- [x] Phase 5: represent all 28 official-list entities as enabled/configured or
  explicitly disabled records, retain dated low-volume live evidence, and keep the
  source-coverage/research summaries aligned with implementation.
- [x] Phase 6: implement retailer-item versus canonical-product identity, staged
  matching, normalized multilingual/barcode search, current price/availability,
  comparison, history, and promotion queries with current-head real-service and plan
  verification.
- [x] Phase 7: implement worker leases/retries/recovery, quarantine, structured logs,
  API/worker Prometheus endpoints, immutable local/S3 archives, Parquet export,
  Compose, backup/restore tools, and benchmark scenarios. Image/SBOM, container
  smoke, quiet-host standard performance, and clean-clone verification are recorded
  on digest `0daaa40f...`.
- [x] Phase 8: complete a clean-clone setup, all offline/real-service/container/live
  gates, representative ingest/replay/history proof through CLI/HTTP/MCP, measured
  scale and performance acceptance, documentation command/link checks, and the final
  report. Digest `0daaa40f...` now has current offline, isolated service, live,
  distribution, image/SBOM, container/coverage/backup-restore, quiet-host standard
  performance, first `main` commit `54ffb79`, and clean-clone core-service evidence.
  The final scan sealed zero findings with partial coverage.

## Decisions

- 2026-08-11 — Use Apache-2.0 for original code and only compatible OSI-approved
  dependencies. Restricted, non-commercial, source-available, or unlicensed projects
  inform observable behavior only; no code, tests, fixtures, or prose is derived.
- 2026-08-11 — Use a modular monolith with inward dependencies and one composition
  root. Domain and application services stay independent of transport/storage
  frameworks; CLI, API, MCP, and worker share those services. See ADR 0002.
- 2026-08-11 — Use CPython 3.14.7, PostgreSQL 18, SQLAlchemy Core/asyncpg, HTTPX,
  bounded stdlib XML/FTP/compression, FastAPI, and Typer. Pin `tzdata==2026.3` for
  deterministic `Asia/Jerusalem` handling on hosts without system zone data.
- 2026-08-11 — Implement the small required read-only MCP subset directly rather
  than adding an SDK dependency. Support current protocol `2026-07-28` and the two
  implemented legacy versions with strict bounded transport validation. See ADR 0006.
- 2026-08-11 — Store exact raw bytes under SHA-256 keys, locally or through an
  S3-compatible service with conditional creation and read-back verification. See
  ADR 0004.
- 2026-08-11 — Emit the required primitive, uncompressed Parquet subset with the
  repository's independently implemented writer rather than add PyArrow; constrain
  schema evolution and verify interoperability independently. See ADR 0007.
- 2026-08-11 — Keep Prometheus registries process-local. API and continuous worker
  expose separate `/metrics` endpoints; persisted status/freshness is the durable
  cross-restart operational view.
- 2026-08-12 — Use an explicitly operator-confirmed in-place normalized rebuild. It
  snapshots the previously applied archive sequence and immutable observation times,
  activates a database maintenance barrier, and replays without network access.
  Durable store, retailer-item, canonical-product, promotion, identifier, confirmed
  match, and reviewed candidate identities/decisions must survive. Raw-derived
  observations, assertions, relations, watermarks, and apply state are rederived into
  those stable identities. A shadow-schema swap is outside the current architecture;
  partial reads during the in-place rebuild are exposed honestly through status.
- 2026-08-12 — Persist bounded per-run original/rebuilt mappings in PostgreSQL rather
  than memory or temporary tables. Parser-unchanged completion reconciles UUIDs and
  times and proves exact rows before deleting snapshots; parser corrections and
  archive-only expansions retain the original audit overlay with per-row outcomes.
  Ordinary rebuilds preserve curated catalog and reviewed match rows in place. See
  ADR 0008.
- 2026-08-12 — Page archive-range replay by each source file's own
  download-finish/archive-attachment timestamp and UUID. The raw object's first
  archive timestamp is not suitable because later source identities can deduplicate
  to older content.
- 2026-08-12 — Represent every otherwise unmatched retailer item with a system-owned
  isolated canonical product immediately after successful ingestion/replay. This
  makes non-GTIN SKUs queryable without claiming cross-item equivalence. Only an
  audited candidate acceptance may replace that isolated link; normalized non-GTIN
  and structured/fuzzy matches remain review-only, while any manual/non-isolated
  conflict fails closed.
- 2026-08-12 — Treat raw content-addressed reuse and source-file idempotence as
  separate boundaries. A new stable source identity retains its own immutable apply
  ledger and advances normalized provenance; exact compatible validated staging may
  be cloned under a serialized cache key, while unchanged values alone suppress new
  history intervals. Replays reuse their immutable ledger time, and a first replay
  after quarantine/failure uses the source's effective archive timestamp.

- 2026-08-12 — Persist ordinary and range/archive-only collection traversal at a
  retailer plus exact portal-generation scope. Advance the publisher cursor/in-page
  offset only after a terminal or retry-safe file boundary, count the run cap only
  against eligible processed files, and roll the traversal generation after complete
  enumeration. A crash-releasing PostgreSQL session advisory lock prevents source
  leapfrog without relying on an expiring TTL during long bounded runs.
- 2026-08-12 — Treat a full Stores file as the portal roster for absence
  reconciliation. Completeness/drop validation compares the whole incoming roster
  with all active portal stores, including omitted subchains at zero, before
  atomically deactivating portal-scoped absences. Per-subchain validation made any
  omitted subchain either invisible or an unconditional 100% drop and was rejected.

- 2026-08-12 - Bound public-query decoration by selecting the exact ordered
  `limit + 1` candidate IDs first. Promotion history uses an inline filtered-ID
  relation plus a materialized candidate page before lateral relationship
  aggregation. Freshness additionally probes at most 1,001 deterministic
  availability rows per selected store, returns counts capped at 1,000 with an
  explicit truncation flag, and uses an independent indexed top-one lookup for the
  globally latest contributing observation. Applying only the response-store limit
  was rejected because one high-cardinality store could still amplify work; deriving
  freshness provenance from the truncated count sample was rejected because it could
  report a stale observation.

- 2026-08-17 — Preserve the public request contract (1–200 normally and 1–1,000 for
  history), but pass the smaller route-specific materialization cap to the repository
  before publisher-controlled text is fetched. A shorter non-final page retains the
  ordinary deterministic cursor. The fixed caps are 50 retailers; 5 stores/products;
  28 prices/comparison/price-history/availability rows; 1 active or historical
  promotion; 50 freshness rows; and 32 source/platform-status rows. One promotion
  exposes at most seven items, seven stores, and seven clubs. Rejecting formerly
  valid larger limits was rejected as an avoidable compatibility break; relying only
  on the late 1 MiB serializer check was rejected because it permits pre-response
  database/driver/application amplification.

- 2026-08-12 — Authenticate every PostgreSQL backup with a versioned,
  domain-separated HMAC-SHA-256 made from a raw 256-bit key stored outside the
  repository and backup tree. Preserve the adjacent SHA-256 sidecar as an independent
  corruption check, but restore only from a private copy populated and retained after
  successful HMAC verification. A standard-library helper shared by Bash and
  PowerShell was selected over an added cryptography dependency; unsigned legacy
  dumps now fail closed before any restore-time `pg_restore`.

## Discoveries and risks

- The official 2025 large-retailer list, corrected 2026-07-23, contains 28 named
  entities and excludes retailers qualifying only under the alternative in section
  14. This scope nuance belongs in the coverage matrix.
- Current publisher behavior spans HTTPS listings, public/static indexes, FTP/FTPS,
  expiring signed download URLs, multiple compression types, UTF-8, UTF-16, and
  Windows-1255 including incorrect XML declarations. Signed URLs are provenance, not
  stable source identity.
- Two official rows remain explicitly disabled because current public access is
  unresolved/blocked. A disabled row is honest coverage, not a verified connector.
- The packaged `makolet benchmark run --quick|--standard` wrapper exists; the Docker
  build context must retain benchmark source while excluding generated results.
- A sealed read-only Standard security scan of the original 210-file snapshot found
  2 high, 11 medium, and 3 low issues. Review of that snapshot is complete: 15
  findings have repository-native remediations, while operator-enabled plain FTP
  remains an explicitly documented unauthenticated-transport residual. The original
  report remains pre-fix evidence. Final post-fix scan
  `bcc531cc-b949-4dde-982e-93e00da343e6` later closed all 12 review rows over its
  292-file authoritative snapshot with zero reportable findings and no validated
  Medium-or-higher regression. Its one Low plan-assertion residual is recorded in
  `docs/research/security-remediation.md`.
- The 1,000,000-observation database run no longer exhausts advisory locks and the
  store-multiplicity matcher was collapsed to source-file retailer items. A later
  standard run staged all one million rows in 82.465426 seconds, then exposed a new
  CPU-bound cleanup of retailer-asserted identifier evidence. The cleanup now uses
  materialized composite live evidence; a complete standard rerun remains required.
- Publisher health, licenses, and source formats are external state. Fixture passes
  cannot be promoted to live evidence, and a publisher failure cannot be recorded as
  a successful empty ingestion.
- Portal-scoped normalized identity is required for legal groups with multiple
  independent feeds (for example Paz/Freshmarket and Rami Levy/Super Cofix). A
  migration must fail before DDL when legacy rows lack one unambiguous portal, and a
  downgrade must refuse valid cross-portal collisions rather than merge them.
- Distinct source identities may carry identical bytes. Raw CAS deduplicates those
  bytes, while each source identity must retain its own immutable apply chronology;
  unchanged values suppress history events, not parsing/apply provenance. Safe
  parser-versioned staging reuse is implemented so this correctness does not repeat
  expensive parsing. Focused unit evidence is 21 passing tests; focused PostgreSQL
  evidence is five passing cases covering safe fallback after replay mutation,
  A-to-B-to-A history restoration, concurrent identities, and first replay after
  terminal failure/quarantine.
- Parquet selection previously filtered only partition discovery, then streamed a
  whole UTC date through independent read-committed connections. It now applies the
  requested inclusive-current/overlapping-history predicate to rows, with
  `valid_to > since` at both layers so a half-open interval ending exactly at the
  lower boundary is excluded, and uses one repeatable-read, read-only PostgreSQL
  snapshot for both enumeration and all streams.
  Schema version 2 embeds portal, document, source-time, download-time, and raw
  SHA-256 provenance. Focused export evidence is 22 unit tests plus two real-PG tests,
  including a concurrent commit after partition discovery that remains outside the
  exported snapshot.

- The prior collection loop discarded an in-page position at its per-run file cap,
  so every later run restarted at catalog row one and could permanently starve later
  files/backfill ranges. Migration `0007_collection_traversal` now retains scoped
  checkpoints and audited attempts. Unknown mixed entries produce durable warnings;
  all-unknown non-empty generations fail closed while genuinely empty listings
  complete.
- A truncate-and-replay rebuild cannot preserve generated UUIDs, signed public
  cursors, temporal interval IDs, or reviewed decisions by replay alone. Migration
  `0008_stable_rebuild_snapshots` adds a fixed 19-entity durable snapshot allowlist
  with a 512-byte logical-key limit and 1 MiB JSON-object row limit. The final
  transaction now fails closed on any parser-unchanged mismatch.

- The promotion-history and freshness SQL has an offline-enforced candidate-first
  shape and current-head PostgreSQL 18.4 result/plan proof. The observed CTE output
  and index paths satisfy the public work contract, but a planner bitmap scan can
  visit more matching tuples than its enclosing `Limit` emits. Treat the evidence as
  a logical output-cardinality/index-use bound; a hard physical tuple-visit bound
  would require a different architecture and is not claimed.
- Valid publisher fields can individually reach 10,000 characters, and MCP duplicates
  each successful value as text plus structured content. Count-only request limits do
  not bound pre-serialization allocation. The route-specific parent caps and the
  seven-per-kind promotion child cap keep the maximum regression response below 1 MiB
  while retaining cursor-complete parent pagination.

## Benchmark results

- The fixed standard parser profile processed 1,000,000 records / 576,817,086 bytes
  in 377.042696 seconds (2,652.22 rows/s) with zero rejects. Peak RSS was 72.062 MiB,
  only 9.062 MiB above baseline, in
  `benchmarks/results/20260811-standard-parser-fixed.json`.
- The quick database grid produced 10,000 normalized and 100,000 current rows in
  34.058378 seconds. Initial apply was 10.991326 seconds, reconciliation apply
  14.349744 seconds, and measured p95 search/barcode/comparison/history latencies
  were 17.754/9.501/22.017/32.038 ms. Evidence is
  `benchmarks/results/quick-grid.json`.
- The first standard database attempt was stopped after the confirmed-match insert
  remained CPU-bound for 694.985 seconds over 1,000,000 observations. It held only
  the two intended family/document locks and had no wait event, isolating the
  store-multiplicity query shape. Evidence is
  `benchmarks/results/20260811-standard-database-confirmed-match-cutoff.json`.
- A later standard database attempt staged 1,000,000 observations in 82.465426
  seconds, then was stopped after retailer-assertion identifier cleanup remained
  CPU-bound for more than 611 seconds. It rolled back and removed only the benchmark
  schema. Evidence is
  `benchmarks/results/20260812-standard-database-identifier-cleanup-cutoff.json`.
- After the identifier cleanup rewrite, a focused diagnostic staged 100,000
  normalized rows representing exactly 100,000 unique GTINs in 5.507595 seconds
  (18,156.74 rows/s) and passed the earlier identifier/confirmed-match phases. The
  first availability-history insert then stayed active for 803.606 seconds at 93.7%
  backend CPU, 248,788 KiB RSS, and no wait event. The transaction had statistics for
  `staged_prices` but `reltuples = -1` and no analyze timestamp for the newly inserted
  `retailer_items` and `stores`. Targeted backend termination rolled back the apply;
  cleanup left zero benchmark schemas, sessions, advisory locks, or runner processes.
  Evidence, bound to source-tree SHA-256
  `e4b421d69deefda484d40bd3477e8af5ba393506d20bf6698deccaf6fb88ca4e`, is
  `benchmarks/results/20260812-availability-history-cutoff.json` and is explicitly
  non-acceptance evidence.
- After mapped incoming item/store/price projections were materialized, indexed, and
  analyzed, the repeated 100,000-unique-GTIN diagnostic completed initial apply in
  109.238689 seconds and reconciliation apply in 251.797673 seconds. The former
  availability-history blocker took at most 2.540645 seconds and the report-only
  plan gate had zero failures. Evidence is
  `benchmarks/results/20260812-gtin-cleanup-diagnostic-fixed.json`.
- At source-tree SHA-256
  `371b37c1caeefdbff40be5cdeb36ec643a75c3156f917b586d0251eb29b10c55`, the final
  standard database attempt staged 1,000,000 observations in 120.373811 seconds
  (8,307.45 rows/s). The initial `current_prices` upsert then remained active for
  9,090.892 seconds in `LWLock/WALWrite`; the checkpointer waited on `WALWrite`, the
  background writer on `WALInsert`, no backend blocked it, and a bounded 15-second
  sample showed zero database/VHD-size and physical-disk-I/O progress. Only the exact
  runner and isolated distro were terminated. Runner count is zero, but WSL control
  remained unresponsive after bounded recovery, preventing PostgreSQL log inspection,
  crash recovery, schema/session/lock and baseline-size verification, and credential
  rotation. Docker Desktop was stopped during safeguarded recovery and remains
  stopped under the no-further-WSL/Docker instruction. This is non-acceptance
  infrastructure-stall evidence with no established root cause in
  `benchmarks/results/20260812-standard-database-wal-stall-cutoff.json`.
- 2026-08-16T11:37:14Z — bounded recovery of the dedicated historical WAL-stall
  environment completed without a benchmark run or repository edit. PostgreSQL 18.4
  replayed from `3/DEF6E0D8` through `4/1B3FFF58` in 33.52 seconds and reached ready.
  The truncated WAL tail logged `unexpected pageaddr`, but `pg_amcheck` passed 258
  relations/956 pages and offline `pg_checksums` found zero bad checksums across
  1,260 files/3,996 blocks. Only `makolet_benchmark` was dropped; database size fell
  from 1,430,173,375 to 9,270,975 bytes, and counts for sessions, benchmark sessions,
  advisory locks, prepared transactions, invalid indexes, and runners were all zero.
  The `makolet` role credential was rotated and verified over TCP/SCRAM without being
  printed or retained. PostgreSQL shut down cleanly at checkpoint `4/1B720958`; only
  the `MakoletBenchmark` distro stopped, port 55434 closed, and the separate 55432
  test services stayed healthy. The final VHD size was 5,425,332,224 bytes. This
  proves recovery integrity and isolation, not the cause of the old stall or scale
  acceptance.
- The post-security standard parser observation processed 1,000,000 rows in
  549.262447 seconds (1,820.62 rows/s) with a 9.074 MiB RSS delta. That is 68.65% of
  the accepted 2,652.22 rows/s baseline, below the provisional 70% regression floor.
  It was not accepted; evidence is
  `benchmarks/results/20260812-standard-parser-postsecurity.json`.
- The last accepted quiet-host standard parser repeat at historical source-tree SHA-256
  `371b37c1caeefdbff40be5cdeb36ec643a75c3156f917b586d0251eb29b10c55` processed
  exactly 1,000,000 records plus one metadata event from 576,817,086 bytes in 8,802
  chunks, with zero rejects. It took 470.919928 seconds (2,123.50 rows/s), or 80.065%
  of the 2,652.22 rows/s baseline, passing the 70% floor by 266.946 rows/s. Peak RSS
  was 73.430 MiB and its 8.555 MiB delta passed the 45.593 MiB ceiling. Parser
  scenario acceptance is true for that historical digest; whole-result acceptance
  remains false because this invocation intentionally omitted the database scenario.
  Later benchmark-relevant changes invalidate any claim that it is final-tree
  evidence. Evidence is
  `benchmarks/results/20260812-standard-parser-final.json`.
- The 2026-08-16 current-tree quiet-host standard parser repeat used
  `uv run python -u -m benchmarks.run --profile standard --scenario parser --output
  benchmarks/results/20260816-standard-parser-final.json`. At source-tree
  SHA-256 `40ddc6be7a3340b85b34422e0571385c42b6a64c32c091246b27ff009bc9631d`
  processed exactly 1,000,000 records plus one metadata event from 576,817,086 bytes
  in 8,802 chunks with zero rejects. It took 384.737072 seconds (2,599.18 rows/s,
  98.00% of the 2,652.22 rows/s baseline) with an 8.258 MiB peak RSS delta, below
  the 45.593 MiB ceiling. Parser scenario acceptance is true; whole-result acceptance
  is false solely because the database scenario was not run. Evidence is
  `benchmarks/results/20260816-standard-parser-final.json`.

- 2026-08-16T12:37:03Z–14:25:04Z — the canonical
  `uv run makolet benchmark run --standard --output
  benchmarks/results/20260816-standard-all-b9aababf.json` completed the exact
  `scenario=all` profile at source-tree SHA-256
  `b9aababfca8f689e2d5dd8eae4ef5bfdce07b54d5ebb613a82e04f4204855114`.
  Whole-result `acceptance_evidence` and both scenario flags are true. The parser
  produced 1,000,000 records plus one metadata event from 576,817,086 bytes/8,802
  chunks with zero rejects in 344.893442 seconds (2,899.45 rows/s, 1.595 MiB/s); its
  RSS delta was 8.863 MiB. Database staging/apply took 56.324763/443.318121 seconds,
  inserted 1,000,000 rows, and created 2,000,000 history events. Reconciliation
  staging/apply took 108.748465/861.911061 seconds and produced exactly 990,000
  unchanged rows, 10,000 unavailable rows, and 10,000 history events. CAS duplicate
  detection took 27.896 ms. Amplification added 9,000,000 rows in 4,347.414955
  seconds (2,070.2 rows/s) and exact final counts were 100,000 canonical products,
  100,000 retailer items, 10,000,000 current prices, and 1,009,999 price-history
  rows; relation size was 5,567,291,392 bytes. Warm p95 latencies for
  search/barcode/comparison/history were 60.849/3.211/8.405/40.351 ms. The enforced
  six-query/four-apply plan gate passed with zero missing plans, important-relation
  sequential scans, pathological nested loops, or other failures. Database work took
  6,020.573608 seconds; total wall time was 6,368.791861 seconds. The 300,532-byte
  artifact SHA-256 is
  `b2187deb192011d403fbf84d91fcadd88e66f2760d6ae25d2c0f48e57974cafc`.

- 2026-08-16T18:26:34Z–18:26:57Z — current-digest quick diagnostic
  `uv run makolet benchmark run --quick --output
  benchmarks/results/20260816-quick-all-c53ec893.json` completed 10,000 parser and
  normalized rows plus 100,000 current prices in 23.298273 seconds. It correctly has
  `acceptance_evidence=false`; its unenforced plan check reports the expected
  small-table sequential-scan diagnostic. The 377,258-byte artifact SHA-256 is
  `e36c6e2df19debec7898569b3405fed8862e4d05275d0661a1e92bfbcc479920`.
  Schema/session/lock counts returned to zero and the credential was rotated. This is
  diagnostic evidence only.

- 2026-08-16T18:28:32Z — the first current-digest standard start overlapped the live
  acceptance workload for about 37 seconds. It was deliberately terminated before an
  artifact was written. Cleanup found zero schema, benchmark sessions, advisory locks,
  prepared transactions, invalid indexes, or runners; the database returned to
  9,279,167 bytes and the credential was rotated. This aborted run is explicitly
  non-evidence.

- 2026-08-16T18:33:49Z — a clean C-drive standard restart completed parser, initial
  staging/apply, reconciliation, and plan capture, then reached 5.1 million current
  prices before the production 2 GiB headroom guard stopped it. C had fallen from
  14.23 GiB to 0.74 GiB while an unrelated Docker VHD grew. No JSON artifact was
  written. Terminal schema/session/lock/prepared/invalid-index/runner counts were all
  zero, the database returned to 9,221,823 bytes, and the credential was rotated at
  2026-08-16T19:37:44.4849883Z. PostgreSQL logged no SQL, backend, filesystem, kernel,
  or OOM error. This capacity-floor attempt is also explicitly non-evidence.

- 2026-08-16T19:50:39Z–2026-08-17T01:39:47Z — the complete current-tree standard
  rerun loaded the byte-identical source and benchmark modules from a temporary
  E-drive copy, with `TEMP`/`TMP` also on E, so the unchanged production headroom
  guard measured the drive with 1,630,174,445,568 bytes free. It used Windows 11,
  CPython 3.14.7, 6 physical/12 logical CPUs, 17,032,929,280 bytes physical memory,
  and dedicated loopback PostgreSQL 18.4. Available host memory at start was only
  1,427,705,856 bytes. The command remained the canonical installed CLI and completed
  `scenario=all` at source digest
  `c53ec893cb53dcf21e7e336a70cff0226ca3652c91d975a4e33920489b7fd11d`.
  Total wall/database durations were 20,946.975967/20,574.241603 seconds.

  The parser emitted 1,000,000 rows and one metadata event with zero rejects in
  369.606381 seconds (2,705.58 rows/s) and peaked at 129.328 MiB RSS with a 9.871 MiB
  delta. Initial stage/apply took 85.918813/508.585069 seconds at
  11,638.89/1,966.24 rows/s and created 1,000,000 current plus 2,000,000 history rows;
  the highest measured client RSS was 252.543 MiB. Reconciliation stage/apply took
  79.204741/1,350.908374 seconds at 12,499.25/732.84 rows/s and produced exactly
  990,000 unchanged, 10,000 unavailable, and 10,000 history events. Amplification
  added 9,000,000 rows in 18,301.374909 seconds (491.77 rows/s), with a
  1,790.029033-second maximum 100,000-row store batch. Final counts were 100,000
  canonical products, 100,000 retailer items, 10,000,000 current prices, and
  1,009,999 price-history rows; relation size was 5,418,352,640 bytes.

  Warm p95 search/barcode/comparison/history latencies were
  57.578/3.437/6.612/44.910 ms, all within the two-times-baseline ceilings. The
  enforced inventory covered eight query and four apply plans with zero failures.
  Whole-result `acceptance_evidence` and both scenario flags are true because the
  complete functional scale workload ran, but the independent performance policy
  **fails**: initial staging was 65.56%, reconciliation apply 63.80%, and amplification
  23.75% of their historical baseline rates. Parser, initial apply, reconciliation
  staging, query p95, and plans passed. An unrelated user process held about 5.5 GiB
  RAM, making host contention plausible but not proven and not a waiver. A quiet-host
  standard rerun remains required.

  The 378,834-byte result was copied byte-for-byte into
  `benchmarks/results/20260816-standard-all-c53ec893-e-drive.json`, SHA-256
  `a2eb2163ecc7c6522f4f9fd1942f9c40c2f55e0eaee0a3acc4acfd44fbe01414`.
  Cleanup left zero schemas, sessions, advisory locks, prepared transactions, invalid
  indexes, public tables, or benchmark processes; the database returned to 9,650,703
  bytes. The credential was rotated at 2026-08-17T01:39:51.3467693Z. The exact
  temporary E-drive copy was removed after path validation, `MakoletBenchmark` is
  stopped, port 55434 is closed, and the source digest remained unchanged.

- 2026-08-16 — the focused current-head public-query plan contract measured the
  production promotion-history first/cursor statements at 2.714/0.642 ms and the
  freshness first/cursor statements at 1.017/1.204 ms on PostgreSQL 18.4. Candidate
  output, index use, capped counts, independent provenance, and cursor order passed as
  described in Current status. These small focused fixtures are correctness/cost-shape
  evidence, not a substitute for the standard scale benchmark plan registry, which
  still requires `promotion_history_bounded_page` and `freshness_bounded_page`.

## Verification evidence

- 2026-08-19T00:39:27+02:00 — remote quiet-host `uv run makolet benchmark run --standard --output benchmarks/results/20260819-standard-all-0daaa40f.json` completed on digest `0daaa40f0db95f674e61340e66ce30bfdffab5062670b2ed2e40690ab8870805` with acceptance_evidence=true, staging 34,767.74, apply 3,958.71, amplification 2,584.90 rows/s, plan_gate passed, and schema dropped after capture. Artifact SHA-256 `ec4a0e17b8edf0562a70315535cb1ab940e166b13b4f32b6289fef4ea161eade`.
- 2026-08-19T01:40:00+03:00 — first local `main` commit `54ffb79666753aab42986c8fabc53d16c5449d56` created after secrets and `git diff --cached --check`. No remote.
- 2026-08-19T01:56:00+03:00 — clean clone of `54ffb79` on C passed frozen sync/lock, Ruff, mypy 176 files, docs, secrets, `makolet --help`, and offline pytest 1,281 passed / 6 skipped / 77 deselected.
- 2026-08-19T02:10:00+03:00 — the same clone migrated disposable `makolet_test_clone_54ffb79` on PostgreSQL 18.4 `127.0.0.1:55434` to `0011_resource_probe_budgets` with `schema_ready=true` and `makolet doctor` `ok=true`. The clone, archive temp, credential file, and database were removed.
- 2026-08-11 — `uv run makolet --help`, `uv run makolet ingest --help`,
  `uv run makolet products search --help`, `uv run makolet mcp serve --help`, and
  `uv run makolet export parquet --help` each exited 0 in the locked workspace.
- 2026-08-11T20:08:33Z — `uv run python scripts/check_secrets.py` exited 0 and
  reported `Secret scan passed for 200 repository files.` This is one check, not a
  complete security review.
- 2026-08-11 — The real-PostgreSQL cross-interface suite passed four end-to-end tests
  in 4.59 seconds, covering exact-byte local CAS, Stores + PriceFull + Price delta,
  duplicate identity, replay idempotence, price history, API, default-runtime CLI,
  and MCP HTTP over the same canonical product.
- 2026-08-11T21:30:29Z — Codex Security scan
  `946fe79f-4fc5-4661-9d95-990ed9dbe1fd` sealed with complete coverage, 16 findings,
  and 1,771,188 measured tokens across five review threads. This is pre-remediation
  evidence; focused regression tests and post-fix gates are still required.
- 2026-08-12 — 63 focused application/query/CLI/API/MCP unit tests passed for
  retailer-scoped item lookup, bounded date-range replay, operator confirmation,
  durable rebuild checkpoint/failure handling, and maintenance status.
- 2026-08-12 — 14 focused real-PostgreSQL persistence/maintenance tests passed in
  7.57 seconds, including two retailer scopes sharing one exact raw object, no-network
  range replay, a post-replay/pre-checkpoint interruption, same-run resume, barrier
  enforcement, and complete normalized regeneration.
- 2026-08-12 — A fresh Alembic chain through `0003_normalized_rebuilds`, downgrade,
  and migration-versus-SQLAlchemy metadata drift gate passed with the archive
  maintenance integration test (2 passed in 4.89 seconds; only the known computed
  column comparison warnings).
- 2026-08-12T00:06:09Z — `uv run pytest --no-cov -q
  tests/unit/test_cli.py tests/unit/test_catalog_matching.py
  tests/unit/test_catalog_matching_service.py` passed 26 tests in 6.24 seconds;
  focused Ruff and mypy checks passed for the same catalog/CLI slice.
- 2026-08-12T00:06:09Z — `uv run pytest --no-cov -q
  tests/unit/test_collection.py` passed 5 tests in 0.36 seconds, including automatic
  post-ingestion/replay bootstrap and the archive-only exclusion.
- 2026-08-12T00:06:09Z — against the dedicated PostgreSQL 18 test service,
  `uv run pytest --no-cov -q tests/integration/test_catalog_matching.py` passed 3
  tests in 4.71 seconds. It proved idempotent non-GTIN isolation/queryability,
  bounded explainable generation, transactional accept/reject/supersede, conflict
  preservation, and use of `ix_retailer_items_last_source_file_id_id` in the
  post-ingestion plan.
- 2026-08-12 — after the final exact normalized retailer-code and corroborated-GTIN
  corrections, the focused matching/collection/CLI/maintenance unit suite passed 47
  tests in 5.00 seconds. Ruff format and lint checks and mypy passed for the matching
  implementation and its integration surface; `scripts/check_docs.py` passed for 41
  Markdown files and 88 local links.
- 2026-08-12 — the final real-PostgreSQL catalog suite passed 3 tests in 1.98
  seconds, including isolated non-GTIN lookup before review, normalized identifier
  candidates across retailer scopes, auditable decisions, manual-match conflict
  refusal, and all three bounded-plan index assertions. The migration-versus-runtime
  metadata drift test also passed in 1.33 seconds with only the three known computed
  column comparison warnings, and an explicit `0005_catalog_matching` downgrade to
  `0004_temporal_quality` followed by upgrade to the then-current
  `0005_catalog_matching` head succeeded.
- 2026-08-12 — the public-query parity slice passed 48 focused application/CLI/API/MCP
  unit tests in 5.08 seconds and the complete unit suite passed 345 tests with only
  the explicitly opted-out live listing test skipped. Focused Ruff format/lint and
  mypy checks passed for 11 query/interface/test files; the documentation checker
  passed 41 Markdown files and 91 local links.
- 2026-08-12 — a final fresh dedicated PostgreSQL 18 database passed the real
  public-query contract in 1.85 seconds. The test exercises default-runtime CLI,
  FastAPI, and MCP
  against the same rows; proves explicit portal disambiguation, issuer provenance,
  current/price-history source-file and archive evidence, deterministic keyset pages,
  structured range/cursor failures, two distinct promotion versions, and active and
  historical normalized promotion fields. It also seeds 205 items/stores/clubs,
  proves each response collection stops at 200 with explicit truncation metadata,
  and verifies the primary-key bounded child-selection plans.
- 2026-08-12 — repeating the portal-collision contract exposed that the initial
  `0006_portal_scoped_identity` downgrade could not recreate legacy retailer-only item
  uniqueness after valid cross-portal collisions existed. The migration now has an
  explicit fail-closed precondition before destructive DDL; the query contract uses
  isolated fresh databases and never weakens portal identity for downgrade symmetry.
- 2026-08-12 — six independently selected, credential-free HTTPS live listing tests
  (`shufersal`, `king-store`, `global-retail`, `hyper-cohen`, `hazi-hinam`, and
  `city-market`) each passed once with bounded discovery and no retained retailer
  dump. This is dated endpoint evidence, not a blanket production-health guarantee.
- 2026-08-12 — portal migration safety passed three PostgreSQL 18 cases twice in
  fresh sessions: active rebuild, ambiguous legacy evidence, and lossy cross-portal
  downgrade all fail before schema/data mutation. The portal-scoped catalog suite
  passed four cases; targeted Ruff and mypy were clean.
- 2026-08-12 — after closing the audited CLI gaps, `tests/unit/test_cli.py` passed
  17 tests in 5.46 seconds and focused Ruff/mypy checks passed. The CLI now exposes
  bounded retailer listing, store filtering, unscoped barcode lookup, portal-aware
  retailer-item lookup, current/comparison/history prices, active/historical
  promotions, item availability, freshness, and source status in addition to its
  existing operational commands; every leaf is discoverable through `--help` and
  delegates to the shared query service.
- 2026-08-12 — a fresh PostgreSQL 18 database then passed the expanded public-query
  contract in 3.20 seconds. The one test invokes the complete required FastAPI read
  surface, all 14 MCP tools, and the complete CLI public-read set against the same
  portal-collision, provenance, promotion-version, and 205-child fan-out rows. This
  run also found and fixed missing explicit PostgreSQL text casts for nullable store
  filters under asyncpg prepared statements.
- 2026-08-12 — the complete unit suite after the CLI expansion passed 346 tests in
  8.43 seconds; the single live-source listing test remained explicitly opt-in and
  skipped. The final 11-file query/interface slice passed Ruff format/lint and mypy,
  48 focused units passed in 4.57 seconds, and the documentation checker passed 41
  Markdown files and 91 local links.
- 2026-08-12 — a dedicated PostgreSQL 18 database passed nine durable collection
  cases and two Stores reconciliation cases. Evidence covers disjoint
  100/100/50 processing of a 250-row catalog, a fourth no-repeat generation, later
  new-identity collection, retryable-error and cancellation boundary resume,
  retryable catalog-postprocess replay at only its uncommitted boundary, independent
  ordinary/backfill checkpoints with a late in-range cap, mixed/all-unknown and empty
  listings, stale-cursor fail-closed behavior, concurrent source lease exclusion,
  and accepted/rejected A+B-to-A roster drops with atomic preservation. Stable-ID
  rediscovery preserves original URL/listing metadata/timestamps without exposing
  original or rotated signed-URL secrets, while latest-attempt and last-good file IDs
  remain distinct. The combined nine discovery, two Stores, four chronology, and
  metadata-drift command passed 17 tests in 56.07 seconds against PostgreSQL 18; only
  the three known generated-column warnings remained. Focused format/Ruff/mypy, 18
  units, and the documentation checker passed.
- 2026-08-12 — `0007_collection_traversal` upgraded from an empty PostgreSQL 18
  database, downgraded to `0006_portal_scoped_identity`, and upgraded again. A later
  dedicated database upgraded through `0008_stable_rebuild_snapshots`; the
  migration-versus-runtime metadata drift comparison remained empty.
- 2026-08-12T09:13:20Z — against a dedicated PostgreSQL 18 test database,
  `uv run pytest --no-cov -q tests/integration/test_archive_maintenance.py
  tests/integration/test_postgres_persistence.py::test_migration_matches_runtime_metadata`
  passed 6 tests in 18.44 seconds with only the three known generated-column warnings.
  The five rebuild cases prove no-network replay; interruption/resume before replay
  and after replay commit but before checkpoint; exact curated canonical fields,
  manual confirmation, accepted/rejected/superseded decisions, public signed-history
  cursor continuity, price/availability/promotion UUIDs, interval times, provenance,
  and byte-identical Parquet artifacts; archive-only isolated bootstrap before its
  checkpoint; correction supersession audit; and conflict rollback with the barrier
  and original snapshot retained. Metadata drift was empty.
- 2026-08-12T09:13:20Z — explicit Alembic downgrade
  `0008_stable_rebuild_snapshots -> 0007_collection_traversal` and upgrade back to
  the then-current `0008_stable_rebuild_snapshots` head both succeeded on the empty
  dedicated test database. Full `ruff check` passed;
  `ruff format --check` reported 203 files formatted; mypy reported no issues in 143
  source files; 13 archive-maintenance units passed; and the documentation checker
  passed 42 Markdown files and 95 local links.
- 2026-08-12 — the public interface contract was closed over endpoint/tool-specific
  Pydantic models: OpenAPI exposes concrete UUID/date-time/fixed-decimal/provenance
  fields and bounded pages/children, while all fourteen MCP tools advertise and
  validate distinct closed success-or-error output schemas. Focused Ruff, mypy, and
  24 API/MCP unit tests passed, the full offline suite passed 422 tests with 54
  service/live tests explicitly skipped. Repository-wide Ruff formatting/linting and
  mypy passed across 143 typed source files, and the documentation checker passed 42
  Markdown files with 95 local links. A fresh PostgreSQL 18 `0008` public-query contract
  also passed in 7.98 seconds through the shared CLI, HTTP, and MCP surfaces,
  including quantity equivalence and deterministic freshness provenance.
- 2026-08-12T09:52:21Z — the post-audit boundary-hardening gate ran focused Ruff
  format/lint and mypy over 11 implementation/test files with no findings. The full
  non-live unit suite passed 444 tests with one live test deselected and the known
  Starlette/httpx deprecation warning. `scripts/check_docs.py` passed 42 Markdown
  files and 95 local links; after renaming a scanner-triggering local variable,
  `scripts/check_secrets.py` passed all 265 repository files. Focused model tests
  then passed 25 cases with Ruff and mypy clean.
- 2026-08-12 — XML parser/security verification passed 64 focused tests. The
  PostgreSQL/Parquet unit slice passed 22 tests, and the two-test real-PostgreSQL
  export contract passed in 1.29 seconds against disposable database
  `makolet_test_acceptance_gap_20260812_a7f32`. Its two exact-lower-bound rows share
  and do not share an otherwise selected UTC partition, proving both streamed-row
  and partition discovery use `valid_to > since`; repeatable-read snapshot evidence
  remains covered. The database was force-dropped and a catalog query returned zero
  remaining rows. Focused Ruff format/lint and mypy passed over the five changed
  implementation/test files; the documentation checker passed 42 Markdown files
  and 95 local links, and the secret scanner passed 266 repository files.
- 2026-08-12T15:08Z — at that intermediate parser/export/benchmark snapshot,
  `uv sync --all-groups --frozen` and `uv lock --check` passed on
  CPython 3.14.7 with uv 0.12.3. `ruff format --check .` reported all 203 files
  formatted, `ruff check .` passed, and strict `mypy src tests benchmarks` reported
  no issues in 143 source files. The documentation checker passed 42 Markdown files
  and 95 local links; the secret scanner passed 269 repository files; build
  constraints verified six hash-pinned distributions and their SBOM; and the image
  checker verified six immutable OSI-licensed image pins. The lightweight
  dependency reconciliation found all 85 installed packages compatible, verified
  81 installed distributions against the license allowlist, rejected no
  GPL/AGPL/LGPL distribution, and matched the committed SBOM to all 84 locked
  components; its temporary reports were removed.
- 2026-08-12T15:09Z — the then-current documented offline command
  `uv run pytest --no-cov -m "not integration and not live and not benchmark"`
  collected 520 tests, selected and passed 466, deselected 54 service/live cases,
  skipped none, and failed none in 12.97 seconds. Its only warning was the already
  recorded Starlette/httpx deprecation warning. Later entries supersede this count.
- 2026-08-12 — the final combined real-PostgreSQL/S3 coverage command was not run
  and produced no coverage percentage: the standard database WAL stall left WSL
  unresponsive and Docker Desktop stopped, so PostgreSQL 18 and SeaweedFS were not
  available. This is an externally blocked gate, not a pass or a pytest skip. The
  container smoke, final backup/restore, and final live-ingestion gates are likewise
  externally blocked pending administrator WSL/Docker recovery. Before that outage,
  `bash -n scripts/container-smoke.sh` passed after the then-current shell change.
  The contemporaneous claim that a full syntax recheck could not run was superseded:
  the later Git-for-Windows Bash run recorded below parsed every `scripts/*.sh`.

- 2026-08-12T15:55:42Z — an HTTPX-shaped stdlib log regression first failed because
  an Azure SAS `sig` query value remained in structured JSON, then passed after the
  shared query-secret sanitizer recognized that exact alias. The final
  `tests/unit/test_observability.py` run passed 12 tests with the known
  Starlette/httpx deprecation warning; it also statically proves the Compose
  Prometheus listener remains configured while remote lifecycle management is
  absent. Focused Ruff format/lint and mypy passed for the logging implementation and
  tests, and the immutable-image checker still verified all six pinned images. No
  Docker, WSL, network, or service-backed check was run for this change.

- 2026-08-12T16:00:32Z - candidate-first promotion-history/freshness remediation:
  `.\.venv\Scripts\python.exe -m pytest tests\unit\test_public_query_sql_shape.py
  tests\unit\test_query_service.py tests\benchmark\test_synthetic.py -q` passed 41
  tests in 0.95 seconds. Focused `ruff check` passed, `ruff format --check` reported
  all four implementation/test files formatted, and focused mypy reported no issues
  in four source files. `scripts/check_docs.py` passed 42 Markdown files and 95 local
  links. No real PostgreSQL, Docker, WSL, network, or application process was used.

- 2026-08-12T17:39:19Z - freshness per-store amplification follow-up: the public contract now
  returns `item_probe_limit = 1000` and `items_truncated`; truncated item counts are
  documented lower bounds. SQL-shape regression tests require a deterministic
  1,001-row count probe and a separate global latest-contributor lookup, and runtime
  metadata tests require the exact supporting count and contributor indexes. The
  focused public-query/API/MCP/CLI/benchmark slice passed 89 tests in 8.76 seconds;
  Ruff formatting/linting and mypy passed for all nine touched Python files; the
  documentation checker passed 42 Markdown files and 96 links; and the secret scan
  passed 275 repository files. Real PostgreSQL execution and plan evidence remains
  blocked on WSL/Docker recovery and was not inferred from static SQL.

- 2026-08-12T16:07:52Z — the production-credential regression first produced three
  expected failures: production accepted the bundled database password, production
  accepted the bundled S3 pair, and Compose supplied known fallbacks while sharing
  the externally configurable bind with both state services. The final focused
  configuration/observability slice passed 28 tests with only the known
  Starlette/httpx warning. It covers plain and URI-encoded database sentinels,
  bundled S3 values, accepted operator-supplied production credentials, the copied
  development environment, required Compose substitutions, loopback-only PostgreSQL
  and S3 host ports, the independent API bind, and the previously disabled
  Prometheus lifecycle handlers. Focused Ruff format/lint and mypy passed, and the
  immutable-image checker still verified all six pinned images. Docker, WSL,
  network, and service-backed execution remained intentionally unrun.

- 2026-08-12T16:17:49Z — the sealed database-backup authenticity regression passed
  15 focused tests in 0.30 seconds. It proves a dump modified alongside a recomputed
  `.sha256` fails HMAC verification, wrong keys and forged authentication sidecars
  fail without retaining a private copy, key material inside the backup tree or with
  an additional hard link is rejected, and both Bash and PowerShell authenticate
  before any restore-time `pg_restore` while preserving staging-before-stop/swap.
  Focused Ruff format/lint and strict mypy passed; the PowerShell AST parser accepted
  1,821 tokens; Git-for-Windows Bash parsed every `scripts/*.sh`; the documentation
  checker passed 42 Markdown files and 96 local links; and the secret scanner passed
  273 repository files. The then-current exact offline suite passed 488 tests with 54
  service/live/benchmark cases deselected and one known Starlette/httpx warning.
  Whole-tree Ruff lint and strict mypy passed; whole-tree formatting remained blocked
  only by concurrent unformatted changes in `src/makolet/config.py` and
  `src/makolet/interfaces/api.py`. No WSL, Docker, PostgreSQL, network, or application
  process was used. The authenticated container backup/restore drill remains pending
  WSL/Docker recovery. Later no-service entries supersede this test count.
- 2026-08-12T16:18Z — a subsequent concurrent-tree retry reported all 207 files formatted.
  Whole-tree Ruff lint and mypy were no longer green because the separate active
  archive-quota lane left unreachable code at
  `src/makolet/adapters/persistence/collection.py:619` and unresolved annotations or
  exports at `tests/unit/test_collection.py:223`, `:241`, and `:484`. Those files are
  outside this remediation's ownership; focused authentication lint, typing, and
  tests remain green, and the parent was notified for owner reconciliation.

- 2026-08-12T18:34:25Z - the HTTPX-shaped encoded-query regression first failed
  because `S%69G=<secret>` passed through structured JSON, then passed after the
  bounded query scanner percent-decoded keys only for case-insensitive secret-name
  classification. Mixed-case and encoded signature/token/key/password/secret/
  credential/auth variants redact their raw values; encoded delimiters and unrelated
  malformed value escapes remain byte-for-byte unchanged, while malformed or
  overlong keys fail closed. The final observability unit slice passed 14 tests with
  the known Starlette/httpx deprecation warning. Focused Ruff format/lint and strict
  mypy passed, documentation checking passed 42 Markdown files and 96 links, and
  secret scanning passed 275 repository files. No service, network, Docker, WSL, or
  quota/migration path was used or changed.

- 2026-08-15T23:41:24Z — documentation reconciliation corrected the Hazi adapter
  status, charged-byte reservation/retry semantics, migration-head/public-query
  evidence boundary, parser digest evidence, Bash syntax status, and superseded test
  counts. The current-tree parser result was copied exactly from
  `benchmarks/results/20260816-standard-parser-final.json`. `uv run python
  scripts/check_docs.py` passed 42 Markdown files and 96 local links; `uv run python
  scripts/check_secrets.py` passed 278 repository files. No code, service, container,
  or network state was changed.

- 2026-08-16 — added the explicit representative live-ingestion acceptance harness
  under `tests/live` and its operator procedure in `docs/testing.md`. The fixed
  Maayan 2000 Price/store-001 lane accepts exactly one credential-free HTTPS file;
  caps publisher attempts at 12, the archive object at 4 MiB, and charged bytes at
  the object cap plus 64 KiB; creates and drops only a confirmed-owned generated
  loopback PostgreSQL test database; and writes and purges only a confirmed-empty
  unique loopback S3 prefix. It streams and hashes the sole stored object under the
  same cap, forbids publisher HTTP during replay, requires zero replay history, and
  compares source-file, digest, portal, and retailer provenance across QueryService,
  CLI, API, and MCP. Ambiguous database creation or initial S3 ownership fails closed
  and reports the exact generated scope. Focused Ruff format/lint and strict mypy
  passed; the no-environment pytest path collected and safely skipped both live
  tests; documentation passed 42 files/97 links and secret scanning passed 281 files.
  No network or service was used, so live execution and observed cleanup remain
  pending after WSL/Docker recovery.

- 2026-08-16 — sealed Standard security follow-up scan
  `bc53afd6-d4a9-4790-9343-507924e392b8` over snapshot
  `codex-security-snapshot/v1:sha256:f882f8224841a00578854dedc992da8d40d69000307e82c0ac084ef80f39c9b6`
  produced five findings. All five now have repository-native remediations:
  every production runtime, collection-lock, demo-seed, and online-migration engine
  requires remote `verify-full` and receives an explicit hostname-verifying asyncpg
  SSL context; static-daily embedded JSON receives one-value
  byte/depth/token/string/scalar preflight before decoding; Zstandard decoder history
  is capped at 128 MiB; raw-archive manifests require a distinct-key,
  domain-separated HMAC before JSON/inventory trust; and manifest/checksum/HMAC/key
  reads are regular-file, no-follow, identity-checked, and bounded. A post-fix audit
  reproduced two sibling TLS bypasses and the POSIX container key-permission failure;
  the final patch centralizes every product engine and makes POSIX wrappers retain
  the invoking non-root identity. Windows binds are held against replacement,
  digest-bound, and copied into a private `0600` tmpfs key before unchanged core
  validation. Agent-owned final evidence was 102 PostgreSQL/config/engine tests, 67
  source tests with one live opt-in skip, 36 parser/security tests, and 32 archive
  tests with five explicit host/service capability skips. Ruff, canonical mypy,
  documentation, secret, Bash syntax, and PowerShell AST checks passed in the owning
  lanes. No network, WSL, Docker, PostgreSQL, or S3 service was used; live TLS
  negotiation and the S3 recovery drill remain explicit post-recovery gates rather
  than claimed passes.

- 2026-08-16T00:53:20Z — final no-service confirmation after the sealed follow-up
  remediations used `uv run pytest --no-cov -m "not integration and not live and not
  benchmark"` and passed 608 tests with four explicit host-capability skips, 56
  integration/live/benchmark cases deselected, and the existing Starlette/httpx
  warning. The security-owning selection passed 131 tests with four of the same
  skips. Whole-tree Ruff formatting passed 218 files, Ruff lint passed, canonical
  mypy passed 157 source files, `uv lock --check` and frozen synchronization checked
  85 packages, documentation passed 42 Markdown files/97 links, secret scanning
  passed 285 repository files, build constraints verified six distributions, and
  container pin validation verified six images. Git-for-Windows Bash parsed every
  shell script and the PowerShell parser accepted 2,485 tokens. No WSL, Docker,
  PostgreSQL, S3, publisher network, or live source was used. The benchmark-relevant
  source digest is now
  `a22c9e5a9d43946dc2fec06b722b9a3521083722e71ff2d5bca51d05518be19a`;
  the previously accepted parser artifact is therefore historical and must be rerun
  later rather than relabelled.

- 2026-08-16T01:10:49Z — a read-only bypass review found that the representative
  live-ingestion harness checked only the PostgreSQL URL authority while asyncpg
  honored a query-level `host` override. The harness now rejects every administrative
  database query parameter and fragment before constructing either create or drop
  engines and revalidates the same invariant at `_asyncpg_url`. Fourteen offline
  regressions cover direct IPv4/IPv6/localhost authorities, remote authorities,
  literal/case/percent-encoded/duplicate host overrides, service/servicefile,
  fragment parsing, non-PostgreSQL URLs, the S3 loopback boundary, and the exact
  one-file/object-plus-frame runtime limits. The exact no-service command
  `uv run pytest --no-cov -m "not integration and not live and not benchmark"`
  then passed 622 tests with four host-capability skips and 56 cases deselected;
  whole-tree Ruff format/lint passed 219 files, canonical mypy passed 158 source
  files, documentation passed 42 files/97 links, and secret scanning passed 286
  files. A frozen 85-package export and the six-package build closure had no known
  vulnerabilities under strict `pip-audit`; license allowlist and copyleft checks,
  the 84-component locked SBOM comparison, and the hash-constrained sdist/wheel build
  passed. All uniquely named supply-chain temporary files were removed.

- 2026-08-16T01:10:49Z — the bounded runtime health probe remained negative:
  `WSLService` and its VM footprint existed, but both `wsl --status` and
  `wsl --list --verbose` hung without output and their exact probe process trees were
  removed. Docker Desktop was stopped with no daemon pipe; PostgreSQL, S3, API, and
  Prometheus ports had no listeners; and no runtime `.env` existed. No service state
  was changed. Service, container, live-ingestion, current-head migration/EXPLAIN,
  coverage, and benchmark gates therefore remain pending external WSL/Docker
  recovery rather than being run against a known-broken environment.

- 2026-08-16T01:15:18Z — the unborn local branch was renamed from `master` to
  `main`, matching the CI push trigger; no file was staged or committed and no remote
  exists yet. A fresh hash-constrained sdist/wheel build was installed into a unique
  CPython 3.14.7 virtual environment after synchronizing the frozen runtime-only
  requirements with `--require-hashes`. An isolated `python -I` import reported
  Makolet `0.1.0`, `Apache-2.0`, and a `site-packages` origin; the installed
  `makolet --help` and `makolet database --help` entry points rendered the expected
  command surface. The exact temporary build, requirements, and virtual-environment
  root was removed afterward. This proves package construction and installation, not
  the still-pending initial commit, remote clone, Linux image, or service behavior.

## Additional progress

- 2026-08-16 - closed the final free-space-floor TOCTOU between concurrent local
  archive, FTP/S3 spool, XML/ZIP parser spool, and database-backup-copy writers.
  Positive-reserve Makolet processes sharing a root now use a stdlib-only,
  cross-platform advisory file lock plus an in-process lock. The critical section
  covers the capacity check, one flushed bounded write, and a post-write check; each
  spool's final flush/fsync and post-sync floor check use the same guard so delayed
  allocation cannot appear after unlock. The bounded local async chunk and release
  are cancellation-shielded so buffered data cannot flush after unlock, while lock
  acquisition remains bounded. Bash and
  PowerShell pass their shared temporary root so unique private restore-copy
  directories coordinate with one another through a stable per-user directory;
  POSIX requires user ownership, mode `0700`, and a non-writable or sticky parent,
  preventing shared-`/tmp` lock replacement. A deterministic two-process regression
  proves that, from 100 available bytes and a
  40-byte floor, only one competing 60-byte writer can enter and the second is
  rejected after observing the first write. The lock is deliberately advisory for
  application processes; unrelated host writers remain an operator volume-isolation
  responsibility. The owning capacity/archive/download/parser/backup/config slice
  passed 119 tests; the exact no-service selection then passed 539 tests with 54
  integration/live/benchmark cases deselected and the existing Starlette/httpx
  warning. Whole-tree Ruff formatting/lint passed 211 files, strict mypy passed 150
  source files, documentation passed 42 files/96 links, and the secret scanner
  passed 277 repository files. Final source-tree digest:
  `40ddc6be7a3340b85b34422e0571385c42b6a64c32c091246b27ff009bc9631d`.

- 2026-08-12T16:32Z - superseded initial aggregate archive-exhaustion remediation added append-only
  migration `0009_collection_charge_budgets`. Collection now serializes each
  retailer's exact trailing-24-hour sum under a locked budget row, records one
  idempotent actual-length charge per source identity, stops with a durable run/day
  truncation reason, and narrows HTTP/FTP response bounds to the remaining budget.
  Archived rejected evidence is charged before a retry boundary advances. Local
  archives and S3 upload spools enforce a configurable free-space reserve. The final
  focused offline slice passed 103 tests (collection, ingestion, HTTP/FTP, local/S3
  archive, config, API, and MCP), with no network/service use. PostgreSQL migration,
  concurrency, rolling-window, source-status, and metadata-drift proof remains
  pending WSL/Docker recovery.
- 2026-08-12T18:05Z - independent closure review replaced after-the-fact archive
  accounting with durable pre-I/O charged-byte reservations and exact settlement.
  The contract now counts immutable archive objects plus failed/retried transfer
  overhead, supports multiple `(attempt, source_file)` settlements, derives archive
  attachment from locked PostgreSQL state, and retains conservative reservations
  across process death or ambiguous post-CAS failures. Internal retries reduce their
  next transport cap by cumulative bytes; HTTP and FTP deadline errors expose bytes
  already received. Migration `0009_collection_charge_budgets` captures one
  materialized trailing-24-hour cutoff, fails closed above 100,000 seed candidates,
  prevents a charged rollout reset/downgrade, and adds the exact freshness-contributor
  lookup index. Whole-tree Ruff format/lint and strict mypy passed; the focused
  quota/transport/interface slice passed 121 tests. The then-current documented offline
  command passed 523 tests with 54 integration/live/benchmark cases deselected and
  one known Starlette/httpx warning. Documentation checking passed 42 Markdown files
  and 96 links, and secret scanning passed 275 files. Later entries supersede this
  no-service count. PostgreSQL
  migration/concurrency proof remains pending WSL/Docker recovery.
- 2026-08-12T16:43Z - independent quota review found and closed two pre-service
  defects: the Alembic revision identifier was shortened to fit the standard
  32-character version column, and HTTP/FTP length evidence now fails at stream EOF
  before a CAS sink can commit mismatched publisher bytes. Focused downloader tests
  prove both transports leave no local archive file on an EOF length mismatch. FTP
  download spooling now enforces the same configured free-space reserve before each
  filesystem write, closing the pre-archive capacity gap found by the same review.
- 2026-08-12T18:42:38Z - final independent quota reread closed five remaining edge
  cases. FTP now charges its complete pre-spooled body on downstream archive failure
  and preserves partial bytes on permission errors. HTTP/FTP final frames are capped
  at 64 KiB; collection reserves that headroom before I/O and narrows the logical
  object cap, so exact failure settlement cannot cross the run/day reservation.
  Permanent object-limit errors remain terminal and advance the checkpoint instead
  of being converted into a retryable quota boundary. Migration 0009 pre-limits its
  indexed download-time and archive-time fallback branches before their bounded
  merge, and legacy attempts retain `charged_bytes = NULL` rather than a false zero.
  The focused quota/transport/migration/config gate passed 90 tests. Whole-tree Ruff
  formatting and lint passed for 209 files, strict mypy passed 148 source files, and
  the exact no-service pytest selection passed 533 tests with 54 deselected and one
  known Starlette/httpx warning. Documentation passed 42 files/96 links and secret
  scanning passed 275 files. The later 539-test no-service gate supersedes this
  intermediate count. Real PostgreSQL migration/concurrency proof remains
  blocked on the already-recorded WSL/Docker outage.
- 2026-08-12T16:52Z - post-fix backup review found the authenticated temporary-copy
  path could consume unbounded host storage before HMAC rejection. The helper now
  enforces a 128-GiB default/operator-configurable input ceiling and a 1-GiB
  default/operator-configurable destination reserve, checks sparse declared size
  before streaming, rechecks the reserve per chunk, and deletes every failed copy.
- 2026-08-12T16:36:04Z - this historical intermediate no-service gate passed after the
  six security remediations then present and archive-quota review fixes; later
  reservation, retry-accounting, length, and filesystem-lock changes supersede its
  current-tree claim. The then-current documented
  offline command passed 498 tests with 54 integration/live/benchmark cases
  deselected and one known Starlette/httpx warning; Ruff reported all 208 files
  formatted and no lint findings; mypy reported no issues in 147 source files; the
  documentation checker passed 42 Markdown files and 96 local links; the secret
  scanner passed 274 repository files; and all six immutable container-image pins
  passed. No WSL, Docker, PostgreSQL, S3, or network service was accessed. This was
  the then-current digest; the later `a22c9e5a...` entry supersedes it.

- 2026-08-16T01:36:35Z — production S3 transport now fails closed before any
  boto3 client is created: an active production S3 backend requires HTTPS, while
  plaintext requires explicit `MAKOLET_S3_ALLOW_INSECURE_LOCAL=true`, an exact
  `seaweedfs`/loopback hostname, and path-style addressing. The runtime, demo seed,
  and raw-archive backup/restore client paths share that policy; development and test
  retain remote-HTTP compatibility. A pre-fix real-settings reproducer accepted
  `http://objects.example.test:9000`. The first independent reread then found that
  virtual addressing could change an approved `seaweedfs` authority into
  `<bucket>.seaweedfs`; the final validator rejects that combination, and runtime and
  seed tests prove path-style reaches botocore. The owning no-service slice passed 97
  tests with four host-capability and two unconfigured-service skips. The exact
  no-service selection passed 648 tests with four host-capability skips and 56 cases
  deselected. Whole-tree Ruff formatting passed 220 files, Ruff lint passed, canonical
  mypy passed 158 source files, documentation passed 42 Markdown files/98 links, and
  secret scanning passed 288 repository files. No network, S3, Docker, WSL, or other
  service was used; actual HTTPS negotiation and the Compose S3 drill remain pending
  service recovery. The benchmark-relevant source digest at this combined tree is
  `bd038a1e5b15f09594589b7cc0f6221a937236ada5e77065b939dfec328cab9f`.

- 2026-08-16T01:41:32Z — repaired the standalone Python distribution contract.
  Wheels now package `_alembic.ini` plus the exact migration environment, template,
  and nine-version graph under `makolet/_migrations`; database status resolved head
  `0009_collection_charge_budgets` from an isolated wheel installation. The packaged
  benchmark command declares `makolet[benchmark]` with pinned `psutil==7.2.2`, and
  both Apache-2.0 license evidence files are wheel metadata. Explicit Hatch wheel and
  sdist inventories omit generated benchmark results, `AGENTS.md`, `.agent`, and
  caches while deliberately retaining source-verification tests/docs in the sdist.
  The new `scripts/check_distribution.py` CI gate first failed a direct build because
  broad migration inclusion still copied `migrations/AGENTS.md`; narrowing the build
  inputs to the environment, template, and versions turned the same gate green. The
  final hash-constrained build produced a 307,166-byte wheel
  (`eaf3bde54d5e38e6e8c3a3776b96ab5a1979243ca971bce8f68b512061317295`)
  and 649,137-byte sdist
  (`b124ad1a6ea5aa510ad9ef9cde909c32d6d106454fb66c37689f9cca2b8c4e90`);
  the gate verified 100 wheel and 266 sdist members. A fresh CPython 3.14.7
  environment installed the wheel with its benchmark extra, imported Makolet only
  from `site-packages`, resolved the packaged migration head, exposed database and
  benchmark help, and ran the packaged quick parser over 10,000 records with zero
  rejects at 2,610.49 rows/s. After exact temporary-build cleanup, whole-tree Ruff
  formatting passed 220 files, lint passed, canonical mypy passed 158 source files,
  and the exact no-service selection passed 648 tests with four host-capability skips
  and 56 cases deselected; docs passed 42 files/98 links, secrets passed 288 files,
  Git-for-Windows Bash parsed every shell script, the 85-package lock remained exact,
  and the six-package build closure matched its SBOM. No database, S3, Docker, WSL,
  or publisher network was used; this is distribution behavior proof, not
  current-tree scale acceptance.

- 2026-08-16T10:25:39Z — published the owner-approved root `SECURITY.md`.
  It makes `main` the supported pre-release line, publishes the private reporting
  address, defines the full product/deployment/package boundary, treats publisher and
  read-interface inputs as hostile, records fifteen fail-closed invariants, and
  distinguishes reportable bypasses from explicitly accepted plain-FTP,
  authentication/TLS-boundary, at-rest-encryption, backup-freshness, and cooperative
  filesystem-lock limitations. The Codex Security resolver found exactly one
  applicable root policy; its SHA-256 is
  `ac8e0bf6c9e456826740ff03b2ec2cea958a858d3d90acb189ea5ae270be60e5`.
  The explicit sdist inventory and distribution gate now require `SECURITY.md`; a
  hash-constrained build verified 100 wheel and 267 sdist members, and the packaged
  10,456-byte policy was byte-identical to the approved source. The wheel remained
  307,166 bytes with SHA-256
  `eaf3bde54d5e38e6e8c3a3776b96ab5a1979243ca971bce8f68b512061317295`;
  the policy-bearing sdist was 653,673 bytes with SHA-256
  `29b4b4c59fa7c18e003ee9643072a847fa8e2359c5ca36b45715544bd4541c96`.
  Whole-tree Ruff formatting passed 221 files and lint passed without cache; canonical
  mypy passed 158 source files; the exact no-service selection passed 648 tests with
  four host-capability skips and 56 deselections; docs passed 43 files/98 links,
  secrets passed 289 files, the 85-package lock remained exact, and the six-package
  build closure matched its SBOM. Exact temporary build artifacts were removed. No
  staging, commit, push, network, WSL, Docker, PostgreSQL, S3, or publisher request
  occurred.

- 2026-08-16T10:50:55Z — recovered the existing dedicated application test
  services without restarting or removing their containers or volumes: PostgreSQL
  18.4 remained on `127.0.0.1:55432` and SeaweedFS 4.40 on
  `127.0.0.1:58333`. A uniquely named disposable database migrated to
  `0009_collection_charge_budgets`. The clean-room E2E fixture now derives its query
  instant from the promotion's durable system-valid row, uses a non-expiring
  independently authored promotion fixture, and correctly proves that a distinct
  source identity reusing immutable bytes still parses/applies while emitting zero
  unchanged history. Integration setup now scopes `MAKOLET_DATABASE_URL` only to
  migration commands instead of leaking it into later configuration tests; migration
  safety resets and restores the rebuild-control singleton; the authenticated raw
  archive tamper test writes the exact LF-only checksum sidecar on Windows. The exact
  command
  `uv run pytest --cov=makolet --cov-branch --cov-report=term-missing --cov-fail-under=85 -m "not live and not benchmark"`
  collected 709 cases, deselected the three explicit live cases, and passed 702 with
  four host-capability skips in 85.13 seconds. Branch coverage was 86.87%, above the
  85% gate. This run included current-head migration/downgrade safety, metadata drift,
  collection concurrency/accounting, PostgreSQL export and public-query contracts,
  exact S3 archive behavior, authenticated backup verification, and the five
  CLI/API/MCP E2E cases. The only warnings were the known Starlette/httpx deprecation
  and three PostgreSQL generated-column comparison warnings. The disposable database
  was force-dropped by exact validated name and `pg_database` reported zero remaining
  rows; shared containers and volumes were left running and untouched. The resulting
  benchmark-relevant digest is
  `d5357f5c15682532f682b4d34396fe7e43645051d37af5df04184bc3dd5d78ab`.
  On the final recorded bytes, Ruff formatting passed 221 files, Ruff lint passed
  without cache, canonical mypy passed 158 source files, documentation passed 43
  Markdown files/98 links, secret scanning passed 290 repository files, and
  `git diff --check` passed. No publisher request, representative live ingestion,
  container-smoke run, or standard benchmark was started.

- 2026-08-16 — `uv run pytest --no-cov -m live
  tests/live/test_representative_ingestion.py -vv` passed both representative-live
  cases in 5.01 seconds against the selected Maayan 2000 Price/store-001 partition.
  Three publisher requests downloaded one 28,705-byte immutable object and produced
  719 accepted price rows. The object length/SHA-256 agreed across ingestion, S3, and
  PostgreSQL; replay was publisher-free and emitted zero new history events; isolated
  matching and QueryService/CLI/HTTP/MCP provenance agreed. Cleanup queries returned
  `live_databases_remaining=0` and `live_prefix_objects_remaining=0`.

- 2026-08-16T11:17:05Z — the current-head PostgreSQL plan contract passed on
  PostgreSQL 18.4 after migrating to `0009_collection_charge_budgets`. Promotion
  first/cursor candidate rows were 2/2 and used
  `ix_promotions_history_from_id`. Freshness first/cursor candidate rows were 2/2;
  their bounded CTEs emitted 2,002/2,001 rows, with the count-probe and independent
  contributor indexes present. Exact 1,005-item stores returned 1,000 plus
  `items_truncated=true`; the latest source/hash outside the count sample remained
  correct, and service cursor order matched the full page. EXPLAIN execution times
  were promotion 2.714/0.642 ms and freshness 1.017/1.204 ms, all shared-buffer hits
  and zero reads. Migration safety plus public query passed 5 tests in 8.11 seconds;
  SQL shape plus benchmark registry passed 21 in 0.59 seconds. The regression passed
  three repetitions plus the final run. Disposable database
  `makolet_test_pgplan_acceptance_b731` was force-dropped and confirmed absent; the
  shared container remained healthy. One calibration bitmap scan visited all 1,005
  matches before its Limit output 1,001, so physical tuple visits are not claimed
  bounded by the logical output limit.

- 2026-08-16T11:28:45Z — `uv run python scripts/check_distribution.py
  --reproducible` completed two byte-identical controlled offline/hash-constrained
  builds and a byte-identical sdist-derived wheel rebuild. The 100-member wheel hash
  was `926cd2e76e450d9a57272b4db62f32f5d0039edbd1ee68c5624fa46e65c65570`;
  the 270-member sdist hash was
  `7bf8fbd0f9edee353655015b4801a1f878e509f687a8d0d19ede9301a97be5a8`.
  Isolated installation, packaged migration/policy/license/notice validation, and
  installed CLI help passed; zero matching temporary paths remained. Four focused
  tests, Ruff, focused mypy, documentation (43 files/98 links), secrets (292 files),
  and the six-entry build closure passed. These artifact hashes identify the exact
  pre-documentation-reconciliation input tree and are not relabelled as post-edit
  hashes.

- 2026-08-16T11:29:00Z — the final `bash scripts/container-smoke.sh` run completed
  with `{"status":"passed","api_url":"http://127.0.0.1:32780"}`. Image
  `makolet:local` was
  `sha256:e1d70d9f6da965d67c8c33f60c73c45a8fd44b17a931cc4beba03514fb4e22aa`.
  PostgreSQL 18.4 migrated to head `0009`; registry counts were 28 retailers/30
  portals; health/readiness and demo reads passed. The combined smoke selection
  collected 723 tests and passed 716, skipped four host-capability cases, deselected
  three live cases, and measured 86.87% branch coverage in 95.71 seconds. Database
  HMAC backup and atomic restore passed. Raw-archive HMAC backup, verification, and
  restore into a new empty bucket passed, followed by the post-restore product/price
  API query. Final counts were `smoke_projects=0`, `smoke_containers=0`, and
  `smoke_volumes=0`; the separate shared PostgreSQL/S3 services stayed healthy. Two
  earlier safe failures exposed missing builder `migrations/` input and a changed
  random API host port after restore; both were fixed and are not counted as passes.

- 2026-08-16T11:41:43Z — final read-only Codex Security scan
  `bcc531cc-b949-4dde-982e-93e00da343e6` sealed over the 292-file repository-root
  snapshot
  `codex-security-snapshot/v1:sha256:220682500829d7efb1d841a5dad82400c2f1e680cfa72aab982b4b639a1dcbf8`.
  All 12 review rows closed with zero reportable findings and no validated
  Medium-or-higher vulnerability/regression. The Low fuzzy-store-search plan-assertion
  residual and all verified non-findings are recorded in
  `docs/research/security-remediation.md`. The owner-approved reporting channel is
  `amitibr19@gmail.com`. No files, network, WSL/Docker, services, live sources,
  secrets, dependency advisory state, or heavy test state were changed by the scan.

- 2026-08-16 — strengthened the Linux runtime SBOM release gate after the final
  audit showed that its prior comparison accepted any document with the same 139
  name/version pairs. `scripts/check_sbom.py --runtime-semantic` now validates the
  runtime profile and compares every stable CycloneDX value after order-only
  canonicalization: licenses, Python and Debian evidence hashes, Debian source
  metadata, native links, component types/purls/bom-refs, generator/base-image
  metadata, and dependency edges. Only the value of the mandatory
  Docker-host-dependent `makolet:platform` property is ignored. The runtime
  generator now records machine-readable license names for all 97 Debian packages:
  exact DEP-5/scoped `License` labels where present and explicit reviewed legacy
  labels for 12 package documents, each bound to its pinned copyright SHA-256 so a
  text change fails pending review. Two separate runs inside image
  `sha256:e1d70d9f6da965d67c8c33f60c73c45a8fd44b17a931cc4beba03514fb4e22aa`
  produced byte-identical temporary documents with 139 components, 97/97 Debian
  license arrays, 547 Debian license entries, generator version 2, and SHA-256
  `d6a06d38bd3bfdd868299add1473de06a160aa1d216a02b0b375bb4c39b66508`.
  The committed artifact has those exact bytes. The semantic checker and ordinary
  84-component main-SBOM inventory mode both passed; 22 focused tests cover every
  important field mutation, platform/order normalization, Debian extraction and
  hash-bound legacy review, and exact CI/docs wiring. Focused Ruff, mypy, and the
  43-file/98-link documentation check passed. A final image rebuild remains required
  so the image itself contains this updated committed SBOM before post-build parity.

- 2026-08-16 — the owner explicitly approved the repository's narrow licensing
  boundary: Ruff may remain solely as a non-redistributed external development/CI
  executable; unmodified MPL-2.0 artifacts may be used with prominent notice,
  source-availability, and covered-file obligations; and separately licensed
  base-OS/service-image programs and libraries, including GPL/LGPL components, may
  be distributed as platform components only when they remain outside Makolet's
  Apache-2.0 source, are fully inventoried, and their own obligations are met.
  Product/build dependencies otherwise remain OSI-approved and Apache-compatible;
  any further exception still requires explicit owner approval. This resolves the
  previously open policy decision without changing the benchmark-relevant source
  digest.

- 2026-08-16T14:25:04Z — terminal standard-benchmark cleanup verified schema,
  benchmark-session, advisory-lock, prepared-transaction, invalid-index, and runner
  counts of zero; the database returned to 9,287,359 bytes. The in-memory credential
  was replaced by a new unretained SCRAM secret and its environment variable was
  absent. PostgreSQL completed an explicit checkpoint and shut down cleanly at
  `9/431B6B00`. Offline `pg_checksums` checked 1,260 files/3,998 blocks with zero bad
  checksums. Only `MakoletBenchmark` stopped, port 55434 had zero listeners, the
  separate 55432 test listener remained, and the final VHD size was 7,740,588,032
  bytes. During the long run, transient WAL waits and highly variable per-store
  amplification times (maximum 690.123463 seconds) were accepted only while bounded
  LSN/database/VHD or commit progress remained observable; no zero-progress stall or
  corruption was inferred.

- 2026-08-16 — final image/runtime-SBOM fixed point passed. Canonical Compose build
  produced `makolet:local` image ID
  `sha256:b8d9c27fb20d66e319354e25d5b2b313c5ad7f754795322a7c580cc6e2e112d5`.
  A non-root generator run inside that image produced 41 PyPI, 97 Debian, and one
  CPython component; all Debian components had machine-readable license evidence.
  `scripts/check_sbom.py --runtime-semantic` passed, and the generated document was
  byte-identical to `sbom.runtime-linux.cdx.json` at SHA-256
  `d6a06d38bd3bfdd868299add1473de06a160aa1d216a02b0b375bb4c39b66508`.
  The image's license, notices, main/build/runtime SBOM bytes matched the repository;
  non-root help and read-only-root checks passed. Twenty-seven focused tests, Ruff,
  mypy, docs, secrets, and all six image pins passed; temporary files/containers were
  absent and shared services retained their IDs and healthy state.

- 2026-08-16 — final reproducible-distribution gate
  `uv run python scripts/check_distribution.py --reproducible` produced two
  byte-identical 100-member, 307,343-byte wheels at SHA-256
  `926cd2e76e450d9a57272b4db62f32f5d0039edbd1ee68c5624fa46e65c65570`
  and two byte-identical 271-member, 680,034-byte sdists at SHA-256
  `8422e6fe73605c24bb56536bbf580f83e62e74db1306866269630ec9b8900ecc`.
  Rebuilding from the sdist yielded the same wheel; isolated offline installation,
  packaged migration inventory through `0009`, license/notice/security evidence,
  benchmark extra, and installed CLI help passed. Lock, compatibility, license,
  build-SBOM, main-SBOM, image-pin, secret, and four distribution regression checks
  passed. All temporary distribution/artifact paths were absent.

- 2026-08-16 — the final frozen-tree static/service inventory contained 295 files at
  SHA-256 `001d48323c87f830d6af21694e14538dae2902fc96fccec7cdae01a0764a48a0`;
  benchmark digest remained `b9aababf...5114`. Frozen sync/lock covered 85 packages;
  Ruff formatted 224 files and linted cleanly; mypy passed 161 source files; docs
  passed 43 files/98 links; secrets passed 295 files; Bash syntax, the six-package
  build closure, six image pins, and 84-component ordinary SBOM matched. Exact
  no-service pytest passed 684 with four host-capability skips and 57 deselections.
  The exact combined PostgreSQL/S3 branch-coverage command passed 738 with four
  host-capability skips, three live deselections, and 86.87% coverage. Its unique
  database ended with zero sessions/prepared transactions/locks and was absent after
  drop; its isolated S3 object/multipart/bucket counts returned to zero after bounded
  asynchronous-cleaner retry. Shared PostgreSQL 18.4 and SeaweedFS stayed healthy.

- 2026-08-16T17:22:56Z — completed remediation of sealed release scan
  `853a74cc-8dd6-4f44-9419-3a027f060d82` (ten Medium findings and one Low finding).
  The fixes now fail closed at the destructive database target, ZIP decoder,
  container-smoke environment, nested Stores identity, fuzzy-search candidate,
  backup creation/restore, staging admission, Parquet allocation, public-response,
  and image-inventory boundaries. Red regressions reproduced each vulnerable class.
  An independent end-to-end read initially closed ten findings and isolated the last
  nested SubChain fallback; `retail-xml/10` now requires a direct, non-empty
  `SubChainID` in every nested scope containing Stores and raises a file-level error
  before accepting that scope. Its focused XML suite passed 53 tests. Focused image
  inventory passed 35 tests and verified six exact immutable pins; Compose config was
  valid. Fix-specific real-service checks exercised three PostgreSQL public-query
  contracts plus isolated S3 and PostgreSQL backup paths, but are not substituted for
  the deferred full service fixed point.

  On final benchmark-relevant digest
  `c53ec893cb53dcf21e7e336a70cff0226ca3652c91d975a4e33920489b7fd11d`, the exact
  no-service command
  `uv run pytest --no-cov -q -m "not integration and not live and not benchmark"`
  passed 792 tests, skipped five host-capability cases, deselected 58, and emitted only
  the known Starlette/httpx warning. Frozen sync/lock covered 85 packages; Ruff
  formatted 228 files and linted cleanly; canonical mypy passed 165 source files;
  documentation passed 43 files/98 links; secrets passed 299 files; the six-package
  build closure and six image pins matched. Git-for-Windows Bash parsed all nine shell
  scripts, PowerShell parsed 2,531 tokens with zero errors, and canonical Compose
  configuration passed. No container smoke, combined coverage run, standard benchmark,
  publisher request, live ingestion, distribution rebuild, image/runtime-SBOM rebuild,
  staging, or commit was performed in this final pass. Per the owner's instruction,
  those fixed-point gates continue in the next session rather than broadening this one.

- 2026-08-16T18:05:17Z — immediate after-run evidence reconciliation:
  `C:\Program Files\Git\bin\bash.exe scripts/container-smoke.sh` exited 0 against
  Docker Desktop under exact Compose project `makolet-smoke-local-1745`. The run
  built and observed `makolet:local` ID
  `sha256:f8079873d7f7807ba988c48ca99d76301589b82254cfa646f4028ac9c04e1aab`,
  migrated its disposable PostgreSQL 18.4 database to
  `0009_collection_charge_budgets`, queried the seeded Hebrew product, verified the
  API and worker as UID/GID `10001:10001` with read-only roots, and observed healthy
  Prometheus targets for `makolet-api`, `makolet-worker`, and `seaweedfs`. The
  combined real-service process collected 867 cases: 859 passed, five
  host-capability cases skipped, three live cases deselected, four known warnings,
  and branch coverage was 86.88%. Authenticated PostgreSQL backup and atomic restore
  passed. Raw-archive HMAC backup and source verification passed; restore into a
  newly created empty target bucket then passed a second exact archive verification
  against that target. The restored barcode/current-price API assertions passed
  after API, worker, and Prometheus restarted healthy. Terminal JSON reported a
  newly discovered ephemeral loopback API URL. The process exited 0, and read-only
  follow-up checks found zero containers, volumes, or networks for the exact project.
  At this checkpoint the ID recorded container behavior only; the later final runtime
  comparison below confirmed the same immutable image ID.

- 2026-08-16T18:27:50Z — read-only service preflight against the retained isolated
  test stack reported PostgreSQL `18.4 (Debian 18.4-1.pgdg12+1)`, database-create
  privilege true, `orphan_databases=0`, and `orphan_prefix_objects=0`; SeaweedFS
  4.40 was healthy. With `MAKOLET_LIVE_SOURCE=maayan-2000`, the documented bounded
  listing test passed once in 0.90 seconds and verified all five configured BINA
  partitions without downloading a source object. The exact representative command
  `uv run pytest --no-cov -m live tests/live/test_representative_ingestion.py -vv`
  then collected and passed two tests in 5.75 seconds. Its assertions proved exactly
  one newly completed credential-free HTTPS Price/store-001 file, a non-empty object
  no larger than 4 MiB, a nonzero accepted-price count, 2-to-12 publisher requests,
  digest equality across ingestion/S3/PostgreSQL, automatic isolated matching,
  publisher-free replay with zero new history, and QueryService/CLI/API/MCP
  provenance equality. Independent cleanup returned `databases=0` and
  `prefix_objects=0`. Six further one-at-a-time invocations for `shufersal`,
  `king-store`, `global-retail`, `hyper-cohen`, `hazi-hinam`, and `city-market`
  each collected and passed one listing test in 1.55/0.30/0.96/0.31/1.97/2.36
  seconds. No credential, signed URL, source payload, product name, or price was
  emitted or retained. Recomputing the benchmark digest after all live reads still
  returned `c53ec893cb53dcf21e7e336a70cff0226ca3652c91d975a4e33920489b7fd11d`.

- 2026-08-16T18:18:07Z — the current reproducible-distribution gate
  `uv run python scripts/check_distribution.py --reproducible` produced two
  byte-identical 102-member wheels at SHA-256
  `88fc3755a49c008364632089670b38e193d72686c19ef7a3d727b8df435778d5`
  and two byte-identical 275-member sdists at SHA-256
  `030d3053435d57067fb4d1e0aa9721a69efb86472c4cbb1521bfab29534f129f`.
  The sdist-derived wheel was byte-identical. Its isolated offline installation used
  the frozen hashed runtime plus benchmark closure, imported exact `psutil==7.2.2`,
  selected packaged Alembic assets, migrated an empty PostgreSQL database through
  `0009_collection_charge_budgets`, reported `schema_ready=true`, and passed installed
  CLI/license/notice checks. Sixteen focused tests, Ruff, mypy, lock check, and the
  all-groups frozen dry run passed. The disposable database and both temporary
  distribution trees were absent afterward; source digest remained `c53ec893...`.

- 2026-08-16T18:20:50Z — final runtime fixed-point confirmation used existing image
  `makolet:local` ID
  `sha256:f8079873d7f7807ba988c48ca99d76301589b82254cfa646f4028ac9c04e1aab`.
  The committed, freshly generated, and image-embedded 155,406-byte runtime SBOMs
  were byte-identical at SHA-256
  `d6a06d38bd3bfdd868299add1473de06a160aa1d216a02b0b375bb4c39b66508`.
  The runtime-semantic gate passed all 139 components, and all six immutable image
  pins passed. Both owned temporary directories and the accidental repository-root
  `-` path were absent afterward. No image or repository byte was changed by this
  confirmation.

- 2026-08-17T01:57:26Z — after final evidence reconciliation,
  `uv run python scripts/check_docs.py` passed 43 Markdown files and 99 local links;
  `uv run python scripts/check_secrets.py` passed 301 repository files. A direct
  `_source_tree_digest(WORKSPACE)` recomputation returned
  `c53ec893cb53dcf21e7e336a70cff0226ca3652c91d975a4e33920489b7fd11d`.

- 2026-08-17T03:46:11Z — remediated the twelve findings from authoritative Standard
  scan `21ee984a-c5b1-43fc-834c-576917ff83d9` over snapshot
  `codex-security-snapshot/v1:sha256:a94fe54aa8b3c5caf0f43e7fbbd69cac5480e3ef64b418170eeb41f33196a1a1`.
  S3 parser-facing reads now spool and verify one deadline-bounded object; archive
  backup derives exact signed membership from PostgreSQL and a complete bounded S3
  comparison; S3 runtime/backup work has total deadlines and pagination budgets;
  production archive tooling requires credentials; and backup/restore wrappers pin
  their Compose target and reject ambient selectors. The red archive selection failed
  six regressions as expected, the exploit rerun passed 13, focused archive coverage
  passed 139 with seven explicit capability/service skips, and its offline suite
  passed 850 with five skips and 66 deselections. Ruff, mypy, Bash, PowerShell, docs,
  and secrets passed; no service was used in that lane.

  Discovery chronology/evidence and archive-attached provenance are now immutable;
  lock-owned pre-archive signed-URL rotation is the only refresh path. Collection
  carries one 256-request/8-MiB/300-second budget through all adapters and transports,
  with Matrix catalog reuse. Ingestion/replay now uses crash-releasing PostgreSQL
  session ownership and one owning connection for every mutation. The focused green
  gate passed 189 tests with one skip plus Ruff, mypy, docs, and secrets. Seven exact
  PostgreSQL regression cases collected but were unrun without the guarded service
  variables.

  Migration `0010_bounded_query_paths` adds 10,000-row keyset projection backfills,
  transactional maintenance, normalized city search, and response-order indexes.
  Current price/comparison, availability, and price history select exact `limit + 1`
  candidates before decoration. History requires paired aware bounds or a
  cursor-pinned trailing 366-day half-open window. The query red gate failed nine of
  39 cases; the green focused gate passed 120 and its offline suite passed 851 with
  five skips and 68 deselections. Alembic reports exactly one head at 0010. Real
  migration execution and adversarial PostgreSQL EXPLAIN remain unrun pending the
  guarded dedicated database.

  MCP decoding now preflights JSON depth 64, 100,000 structural tokens, decoded
  strings of 65,536 characters, and scalars of 4,096 characters before `json.loads`.
  Recursion, Unicode, JSON, and numeric-decoder failures return JSON-RPC -32700 on
  stdio and HTTP without terminating the stream. Its red regressions reproduced both
  the direct exception and HTTP 500; the final MCP file passed 17 tests with Ruff,
  mypy, and docs green. The final combined static/offline, PostgreSQL/S3/container,
  coverage, security-rescan, performance, and clean-clone gates remain pending and
  must bind to the later frozen digest.

- 2026-08-17T11:08:39Z — closed the archive/process/export findings after two
  independent bypass-review rounds. Explicit HTTPS port zero now fails before DNS.
  Local verification and every production S3 operation use a private inherited
  channel and a killable child whose parent guardian starts before payload decoding;
  blocked bootstrap, stalled unpickle, descriptor transfer, cancellation, and hard
  parent exit leave zero handles, threads, or descendants. The worker owns signals
  outside Uvicorn and keeps its process watchdog armed through lifespan and outer
  `asyncio.run` cleanup. Exact exploit evidence included 16 process-boundary tests,
  two real signal/Uvicorn subprocess tests, and repeated zero-residue probes.

  Archive backup now holds one ownership-checked Docker-volume lock per exact Compose
  project, treats `running` and `restarting` workers as active, proves a nonrunning
  state before backup, and restarts only while it still owns the lock and exact child
  cleanup is proven. Concurrent Bash and PowerShell backup probes, restart failure,
  stale ownership, and exact-container cleanup passed without touching unrelated
  services.

  Export destination resolution and row spooling now run in bounded killable children;
  rows remain streamed and retain the preflight row/byte/file/output ceilings. Cold
  That host's 0.100-second stalls at resolve, create, write, flush/fsync, and close
  returned below 0.2 seconds with no child, thread, handle, spool, journal, or staging
  residue. A later post-reboot measurement disproved that as a portable Windows
  ceiling: native `CreateProcess` alone varied from 0.13 to 1.32 seconds.
  Publication receipt, recovery, cancellation, partial-publication ledgers, and
  multirow controls passed independent review. The final exact commands passed 58
  export tests, 16 process tests, repository-wide Ruff/mypy, and the canonical
  no-service selection with `977 passed, 6 skipped, 75 deselected`. PostgreSQL 18.4
  and SeaweedFS then passed the hostile-order six-test export/S3/backup slice; the
  exact database `makolet_test_export_final_20260817_a2` was force-dropped and its
  post-cleanup catalog count was zero. No publisher network access was used.

- 2026-08-17T12:50:11Z — sealed Standard scan
  `4db0500b-f557-409b-a2ec-1fa2f69c2f5d` over 309 files/eight surfaces reported
  three bounded-resource findings. FTP/FTPS control replies now have cumulative
  byte/line ceilings and settle control plus data bytes across retry and success;
  the HTTP API carries a validated Uvicorn concurrency ceiling of 100 by default;
  and product detail probes at most 201 deterministically ordered identifiers before
  JSON aggregation, preserving exact output through 200 and failing stably above it.
  Focused FTP/ingestion passed 76 tests; focused API/query passed 173; whole-tree
  Ruff format checked 237 files, Ruff lint passed, mypy checked 172 modules, docs
  checked 43 Markdown files/99 links, secrets checked 312 files, and the final
  no-service command selected 1,057 cases with 1,051 passed, six host-capability
  skips, 75 deselections, and one known warning.

  The first post-reboot container attempt safely stopped after coverage because its
  Windows POSIX routing selected PowerShell 5.1; the exact project had zero remaining
  containers, volumes, or networks. Routing now requires PowerShell 7 `pwsh.exe`,
  with 11 focused wrapper tests, Git Bash syntax, PowerShell AST, and docs green.
  The exact rerun `C:\Program Files\Git\bin\bash.exe scripts/container-smoke.sh`
  then passed under isolated project `makolet-smoke-local-644`: PostgreSQL 18.4 at
  migration `0011_resource_probe_budgets`; 1,123 passed, six capability skips,
  three live deselections, eight known warnings, and 85.51% branch coverage;
  authenticated database backup/atomic restore; HMAC-authenticated 25-object archive
  backup, verification, isolated-bucket restore, and second verification; healthy
  non-root/read-only API/worker/SeaweedFS/Prometheus; restored public product/price
  queries; terminal API status; and zero exact-project containers, volumes, or
  networks afterward. Current benchmark-relevant source digest is
  `3c0fa7368e2b0c8be54ea8d2cf3953bc933477c3db23badcea8fbaa14b5457ab`.

- 2026-08-17T13:33:13Z — sealed post-remediation Standard scan
  `852eca2f-109a-407f-91eb-99c51ec76b41` over the 312-file snapshot
  `codex-security-snapshot/v1:sha256:515c3c5a48bbcdb2bb03566b7fd5ce5f1b18c00ab84a59df776f3ee64bf55e00`
  reported one low finding, `csf_c0f790ac0d9c89a6eb04e7ec`: a full FTP/FTPS
  object reservation omitted bounded control-channel overhead and the full-budget
  branch removed the cumulative retry limit. Collection now reserves protocol-aware
  payload, 256-KiB FTP control, and 64-KiB final-frame ceilings and supplies a finite
  cumulative limit to every ordinary HTTP/FTP download. Permanent object oversize
  remains terminal, while only an explicitly budget-limited transfer becomes a
  retryable charge-boundary event. Memory and PostgreSQL settlement fail before
  mutation when an exact charge exceeds its reservation. The final focused unit gate
  passed 178 tests; the dedicated PostgreSQL rollback test passed against
  `makolet_test_ftp_budget_20260817_a1`, after which the database was dropped and its
  catalog count was verified as zero. Whole-tree Ruff format (237 files), lint, mypy
  (172 modules), docs (43 files/99 links), and secrets (312 files) passed. The final
  no-service attempt passed 1,056 selected tests with six host-capability skips but
  did not close the repository gate: four unrelated Windows export watchdog tests
  exceeded their 0.1/0.2-second timing envelopes during spawned-process startup or
  cleanup. Those export timing failures remain deferred; the finding-specific fix
  report records the exact exception. The resulting benchmark-relevant source digest
  is `fde5010e85f0ca406537903c9b735c4b025c2d8b1d7c9f0f2e3b290c520a8b0a`; all earlier
  benchmark artifacts remain historical rather than current-tree acceptance.

- 2026-08-17 — corrected the Windows export watchdog evidence after the complete
  no-service gate exposed native process-start jitter. Eight direct launch samples
  took 0.14–1.20 seconds while bounded child cleanup took 0.001–0.052 seconds; a
  separate capture reached 1.32 seconds inside `_winapi.CreateProcess`. No failed
  fast-path probe reached its filesystem target and no child/spool/output residue
  remained. Short export budgets now reserve at least four seconds for recovery when
  the total permits (three seconds of a six-second budget). Tests use a generous
  healthy warm-up, retain a three-second Windows-only ceiling for a 0.1-second
  deadline that expires during native startup, and separately require every reached
  resolve/create/write/flush/fsync/close stall to be killed and cleaned. Strict
  subsecond preemption of synchronous Windows process creation would require a
  persistent launcher broker and is neither claimed nor emulated with an abandoned
  thread. The complete export file then passed 30 tests, the archive
  process/protocol slice passed 32, and the canonical no-service command passed
  1,062 tests with six documented capability skips, 76 deselections, and one known
  warning. Locked sync/lock, whole-tree Ruff format (237 files), Ruff lint, mypy (172
  modules), Bash syntax, docs (43 files/99 links), and secrets (312 files) passed.
  The resulting benchmark-relevant digest is
  `80435dee05a99a7305d21dd356004e3a823e93334f4e305f3a3bea0920f28004`.

- 2026-08-17 — after the owner rebooted Windows, bounded read-only probes confirmed
  WSL2 and Docker responsive again. `docker-desktop` was running, PostgreSQL 18.4 on
  `127.0.0.1:55432` and SeaweedFS on `127.0.0.1:58333` were healthy, and the stopped
  `MakoletBenchmark` distro remained isolated. An exact unique PostgreSQL database
  then migrated from empty to `0011_resource_probe_budgets`; status and runtime
  metadata matched before and after ten focused real-service migration, public-query,
  candidate-first/freshness-plan, charged-byte rollback, and FTP-success settlement
  tests. All ten passed with only the seven known generated-column comparison
  warnings. The exact database was force-dropped, with zero catalog rows or sessions.
  A unique-prefix SeaweedFS archive backup/restore test also passed and left the
  complete before/after inventory unchanged.

- 2026-08-17 — Standard scan `88fc05da-2dcb-4a09-a061-d627bcbac877` sealed the
  authoritative 312-file snapshot
  `codex-security-snapshot/v1:sha256:919dea9bda21f79383105fe935a20d6421026c8bac1b0f46d797d4eee8319dd4`
  with complete coverage and seven high-confidence findings (four medium, three low).
  Remediation is complete on the post-snapshot tree: raw S3 listing bytes are bounded
  before Botocore XML parsing; source HTTP cookies are rejected; HTML/JSON discovery
  fails closed on incomplete or ambiguous structures; production plaintext
  exceptions require literal loopback; PowerShell watchdogs reject command scripts;
  and live S3 cleanup is deadline/response bounded with independent DB
  cleanup. The scan-local `artifacts/fix_report.md` records red/green evidence. This
  remediation also closes the independent audit's ambient-proxy, blocked in-flight
  cleanup, pre-parse live listing, plain-text/lexical/generic HTML, JSON-mode, and
  StaticDaily terminal-empty bypasses. A final independent reread found no blocker in
  any of the seven findings. The canonical no-service gate then passed 1,133 tests
  with six documented host-capability skips, 76 deselections, and one known warning;
  whole-tree Ruff format (237 files), Ruff lint, mypy (172 modules), Bash syntax,
  PowerShell AST, docs (43 files/99 links), and secrets (312 files) passed. All seven
  bounded HTTPS listing lanes passed sequentially, and the representative Maayan 2000
  ingestion passed both tests on benchmark-relevant digest
  `984d43e4fbe44a03bd3a2fab5cf6acbaffd13f16bd1cf9a312bb9eed8be953da`, with exact
  database/prefix and global acceptance residue zero. This entry does not claim the
  required new post-fix scan or combined container rerun; those remain pending.

- 2026-08-17T17:04:58Z — the exact current-tree command
  `C:\Program Files\Git\bin\bash.exe scripts/container-smoke.sh` exited 0 under
  isolated Compose project `makolet-smoke-local-1298`. PostgreSQL 18.4 migrated to
  `0011_resource_probe_budgets`. The combined process collected 1,215 cases and
  selected 1,212: 1,206 passed, six expected capability cases skipped, three live
  cases deselected, and eight known warnings were emitted in 311.81 seconds. Branch
  coverage was 85.77%, above the enforced 85% floor. HMAC-authenticated PostgreSQL
  backup and restore passed and the restored database reported migration head 0011.
  Raw-archive HMAC backup, source verification, restore into a confirmed-clean
  isolated target, and target re-verification passed for all 18 objects. The
  post-restore API check passed. Read-only follow-up found zero containers, volumes,
  or networks for the exact project. This closes the current-tree combined
  container/coverage/recovery gate on benchmark-relevant digest `984d43e4...`; it
  does not claim the separate image/runtime-SBOM fixed point.

- 2026-08-17T18:15:30Z — two findings from sealed Standard scan
  `dcd9f5e6-8410-49d0-90f9-533dffb6e20b` are remediated on the working tree:
  `occ_185d269c8adc55758d4b9fd2` and `occ_b6db931376ac9931b6a034cf`.
  MCP now rejects unknown argument keys with one bounded protocol issue before
  Pydantic validation and checks the validation error count before materializing at
  most 16 typed diagnostics. `Database.from_url` retains its validated statement
  timeout and supplies it to both unpooled collection and ingestion lease engines;
  TLS propagation, UTC, distinct application names, `NullPool`, advisory-lock
  ownership, and rollback semantics are unchanged.

  The pre-fix three-test contract reproduced both paths with two failures and one
  legitimate diagnostic control pass. The same contract passed all three cases after
  the patch; the complete focused MCP/database files passed 34 tests, and 90 related
  configuration, migration-environment, ingestion-security, and collection unit
  tests passed. A real PostgreSQL 18.4 check used a uniquely named disposable
  database on the existing test service: a one-second `pg_sleep` on the lock-owning
  connection was canceled by the configured 100 ms statement timeout, its transaction
  rolled back, a subsequent `SELECT 1` succeeded on the same connection, and the
  database was force-dropped; a post-cleanup catalog query returned zero databases
  matching `makolet_test_timeout_%`. The first harness invocation was rejected before
  the test because its URL included a forbidden driver query parameter; that disposable
  database was also cleaned before the corrected no-query invocation passed. Focused
  Ruff format/lint and mypy passed five files; documentation checked 43 Markdown
  files/99 links and the secret scan checked 313 repository files. This entry claims
  only these two occurrences, not the other findings or the final post-fix fixed
  point.

- 2026-08-17T18:23:06Z — sealed-scan S3 response occurrences
  `occ_5f461e91ab2a33a0be772829` and `occ_7f4fa70f203a6fff789249a1`
  are remediated on the working tree. One shared operation-specific Botocore
  `before-send` transport now caps every runtime and recovery error/non-streaming
  success body under a cumulative 8 MiB per-request retry budget while preserving
  successful `GetObject` streaming. Runtime covers `HeadBucket`, `PutObject`,
  `HeadObject`, and `GetObject`; backup/restore additionally covers listing,
  conditional staging upload/copy, and staging deletion. The exact pre-fix Botocore
  contract produced eight failures and one legitimate streaming pass. After the
  patch, its nine security/control cases plus three signed-listing compatibility
  checks passed; the complete owning files passed 108 tests with four documented
  host-capability skips. Focused Ruff format/lint and mypy passed. No external service
  was used for this change, so a frozen-tree S3/container rerun remains required and
  this entry does not close the final release gate.

- 2026-08-17T18:27:51Z — supersedes the preceding entry's statement that no external
  service was used. The first safe UUID-prefix SeaweedFS backup/restore integration
  run failed before staging upload because a successful HTTP `HEAD` may legitimately
  advertise the selected object's `Content-Length`/`Content-Encoding` while carrying
  no response body. The transport now treats `HEAD`, 204, and 304 with their correct
  no-body semantics: representation headers pass through to Botocore, while every raw
  byte that nevertheless arrives is still charged against the same cumulative cap.
  The new exact regression passed together with the ten-case security/control slice.
  Rerunning
  `pytest --no-cov -q tests/integration/test_archive_backup.py::test_nondefault_prefix_backup_restore_and_manifest_key_validation`
  against the already-running local SeaweedFS service passed in 0.98 seconds, covering
  signed listing/get/head/put/copy/delete, conditional clean-target restore, remote
  verification, and exact-key `finally` cleanup. The complete owning unit files then
  passed 109 tests with four host-capability skips in 47.00 seconds. Container/runtime-
  child fixed-point verification remains a separate final-tree gate.

- 2026-08-17T18:29:37Z — sealed-scan HTTP occurrences
  `occ_fbc409368fdcecfda48535fe` and `occ_733f203540ad295ecedfa55f`
  are remediated on the working tree. The shared resolver rejects more than four
  unique public addresses before send; every physical failover/redirect attempt now
  commits a 128 KiB control floor, and discovery consumes one request unit per
  attempt. The exact pinned production
  `httpx`/`httpcore` network stream charges cleartext raw reads and writes before HTTP
  parsing; the follow-up below records the corrected TLS boundary. Session payload
  accounting preserves exact archived bytes. This first-stage patch reserved 9 MiB
  across the four configured ingestion retries for control work; the later
  2026-08-17T21:11:34Z entry corrects that incomplete headroom calculation to include
  normal TLS framing. Cumulative control/payload charges settle through the existing
  durable boundary.

  The pre-fix focused contract failed at import because the control/accounting
  boundary was absent. After the patch, seven exact exploit/control cases passed,
  including excess DNS rejection before send, legitimate two-address failover,
  header-only redirect charging, an installed-`httpcore` informational-response flood
  stopped before final-header parsing, transport-failure charging, and retry charge
  preservation. The broader owning selection passed 260 tests with one documented
  live opt-in skip; focused Ruff format/lint and mypy passed nine files. No external
  network or service was used. Documentation checked 43 Markdown files/99 links and
  the secret scan checked 313 repository files. Frozen-tree container, benchmark, and
  security reruns remain parent release gates.

- 2026-08-17T20:37:33Z — an independent post-fix audit found that the first HTTP
  transport wrapper delegated `start_tls` before attaching its meter, so TLS handshake
  ciphertext was uncharged and later reads/writes were observed at the decrypted
  plaintext boundary. The pinned AnyIO raw byte stream is now wrapped before
  `TLSStream.wrap`; handshake and encrypted records are charged exactly, and the
  returned decrypted stream is not wrapped again. Backends that cannot expose that
  pinned boundary fail closed before TLS. A 64 KiB charge is also committed before
  each DNS lookup, closing the remaining zero-accounting resolution path. The maximum
  per-open control portion was consequently calculated as 2.25 MiB and this
  intermediate patch reserved 9 MiB across four ingestion retries. The later
  2026-08-17T21:11:34Z entry records the final 24.25 MiB/open and 97 MiB/four-open
  reservation after adding normal TLS framing headroom.

  The production-shaped TLS/DNS contract first failed three tests: handshake bytes
  remained zero and artifact/listing private-resolution failures remained uncharged.
  It now passes, together with a full-stack unsupported-backend fail-closed control.
  The complete HTTP/listing files pass 54 tests; the broader source/downloader/
  collection/ingestion selection passes 265 tests with one documented live opt-in
  skip; focused Ruff format/lint and mypy pass nine files. Behavior-level edge tests
  raised focused branch coverage of `adapters/download/http.py` from 83% to 87%. No
  external network or service was used. Frozen-tree container, coverage, benchmark,
  and security reruns remain parent release gates.

- 2026-08-17T21:11:34Z — final HTTP reservation review corrected the preceding
  historical 9 MiB headroom calculation. The 2.25 MiB figure covers DNS and physical
  address-attempt control work for one open but not normal TLS record framing. The
  shared application contract now additionally reserves 22 MiB for TLS 1.3 framing
  through the supported 16 GiB archive-object ceiling, for 24.25 MiB per independently
  metered open and 97 MiB across four downloader attempts. Pathologically padded or
  fragmented records remain fail-closed at the raw-wire meter. Unsupported or
  non-AnyIO TLS streams now close the delegated stream inside a cancellation-shielded
  scope before returning the generic accounting error; close failure cannot replace
  that security error. The latest focused downloader/listing/collection/configuration
  selection passed 147 tests. A separate focused branch run passed 59 tests at 88.36%
  combined branch coverage (`adapters/download/http.py` 87%, source listing HTTP 94%);
  focused Ruff format/lint and mypy passed. No network or service was used. The final
  independent reread and frozen-tree service gates remain pending.

- 2026-08-17T20:13:19Z — authoritative pre-fix Standard scan
  `dcd9f5e6-8410-49d0-90f9-533dffb6e20b` sealed exactly once at
  2026-08-17T18:05:00.894718Z over 312 files, snapshot
  `codex-security-snapshot/v1:sha256:8d66bc4e0949546c8a6e2098254eca69e8b2d58bcfcb7c9901ff77294f735dc1`.
  Coverage was complete. It reported five Medium and two Low findings: MCP diagnostic
  fan-out (`occ_185d269c8adc55758d4b9fd2`), public-page materialization
  (`occ_e965ad22dffd892e1f110676`), lease-engine timeout propagation
  (`occ_b6db931376ac9931b6a034cf`), artifact/listing HTTP accounting
  (`occ_fbc409368fdcecfda48535fe`, `occ_733f203540ad295ecedfa55f`), and
  runtime/recovery S3 response pre-parsing (`occ_5f461e91ab2a33a0be772829`,
  `occ_7f4fa70f203a6fff789249a1`). The dated entries immediately above record the
  MCP/database, S3, and HTTP red/green proof.

  Public requests still accept 1–200 rows normally and 1–1,000 for history. Before
  repository materialization, current code caps retailers at 50; stores and product
  search at 5; prices, comparison, price history, and availability at 28; active and
  historical promotions at 1; freshness at 50; and source/platform status at 32.
  Non-final short pages retain the normal cursor. A promotion returns at most seven
  items, seven stores, and seven clubs, with explicit returned counts and truncation
  flags. Eight maximum-four-byte-publisher MCP regressions failed before the final cap
  correction and now pass. Their maximum complete JSON-RPC response was 1,011,955
  bytes against the 1,048,576-byte ceiling and the duplicated tool result remained
  below its 1,044,480-byte working budget. The focused query/API/MCP/SQL selection
  passed 104 tests and the benchmark synthetic contract passed 22 tests.

  The complete seven-test `tests/integration/test_public_query_contract.py` file then
  passed against PostgreSQL 18.4 after its stale pre-cap expectations were aligned
  with the intentional short-page contract. The current no-service selection passed
  1,164 tests with six documented host-capability skips, 77 deselections, and one
  known Starlette/httpx warning in 165.60 seconds. Whole-tree Ruff format checked 238
  files, Ruff lint passed, and mypy checked 173 files. Documentation checked 43
  Markdown files/99 links and the secret scan checked 313 repository files. These
  results close working-tree remediation behavior, not the required frozen-tree
  post-fix scan, combined
  container/recovery gate, applicable live rerun, standard benchmark, or release
  fixed point.

- 2026-08-17T20:45:05Z — closed the Python dependency license/notices inventory
  automation gap without adding a dependency or license exception. The pre-fix
  contract produced ten expected failures because the checker had no frozen-tree
  coverage API or reviewed policy and the historical 81-row `pip-licenses` report
  omitted its own distribution plus `pip`, `prettytable`, and `wcwidth`. The new
  `docs/research/dependency-license-policy.toml` is compared fail-closed with all 84
  third-party `uv.lock` identities, the lock's build-constraint manifest, six exact
  hash-constrained build identities, and the committed main/build SBOMs. Three
  overlaps yield exactly 87 reviewed Python identities. Every identity has one
  approved classification and explicit license/notice obligations. The sole Ruff
  external-tool boundary, the two exact unmodified MPL-2.0 artifacts and source
  hashes, and the digest-pinned 97-package Debian base-image inventory and
  corresponding-source obligations are fixed assertions; no broader exception can
  enter through policy data.

  Fourteen final regression tests passed, covering each omitted tooling package,
  stale and partial Python/build/runtime inventories, weakened pip/MPL obligations,
  a broadened external-tool exception, the base-image boundary, and exact CI wiring.
  The combined license/SBOM unit slice passed 47 tests. Whole-tree Ruff format
  checked 240 files, Ruff lint passed, canonical mypy passed 175 source files,
  documentation checked 43 Markdown files/100 links, and secrets checked 316 files.
  Frozen lock/sync reported 85 packages with no changes; the build checker verified
  six hash-pinned distributions; an independently exported all-groups SBOM matched
  all 84 locked components and its exact temporary file was removed. CI now invokes
  the repository gate directly rather than generating a partial installed-tool
  report. No Docker, build, service, benchmark, network, dependency, source-runtime,
  migration, or distribution-script operation was used.

- 2026-08-17T21:13:10Z — independent distribution/license release-gate review
  reproduced three fail-open boundaries before heavy builds: a safe first credential
  candidate masked a later unsafe assignment or URI in both scanners; dynamically
  expected distribution files allowed unclassified payloads and skipped `Dockerfile`;
  and `THIRD_PARTY_NOTICES.md` invoked the retired positional
  `check_licenses.py .ci-licenses.json` interface. Remediation now checks every
  credential candidate, rejects every unreviewed payload type, explicitly scans
  `Dockerfile`, compares the installed Typer command tree bidirectionally with all 59
  advertised help paths, and documents only the canonical frozen license-policy
  command. The focused distribution/license/secret/SBOM selection passed 88 tests;
  Ruff format/lint and focused strict mypy passed five files; docs passed 43 files/100
  links; secrets passed 317 files; license coverage passed 84 locked, six build, 87
  unique Python, and 97 Debian identities; six build constraints and six image pins
  passed. No service, network, distribution build, container build, or migration was
  run; reproducible build, installed PostgreSQL proof, and runtime-image SBOM remain
  parent fixed-point gates.

- 2026-08-17T21:48Z — the post-reboot isolated container fixed-point gate passed on
  the current tree. Git Bash `scripts/container-smoke.sh` built the pinned non-root
  image, migrated PostgreSQL 18.4 to exact head `0011_resource_probe_budgets`, started
  healthy API/worker/SeaweedFS/Prometheus services, and ran 1,342 tests with six
  host-capability skips and three marker deselections. Combined real-service branch
  coverage was 86.39%, above the required 85%. The authenticated database backup and
  restore preserved the deterministic demo at head 0011. Raw-archive backup,
  authentication, verification, create-only restore to a separate bucket, and
  re-verification covered 17 objects; the restored API query passed. Terminal output
  was `status=passed`. The project-scoped cleanup left zero containers, volumes, or
  networks. No unrelated Docker resource was stopped or removed.

- 2026-08-17T21:50Z — reproducible installed-distribution proof first failed safely
  on an E-drive environment because offline dependency installation exceeded the
  bounded 300-second command timeout; its exact database and temporary tree were
  removed. The same gate on the faster C drive exposed a harness defect:
  `MAKOLET_LOG_LEVEL=CRITICAL` was outside the documented settings literal and caused
  installed empty-status to fail before migration. The harness now uses supported
  `ERROR`, with a regression proving the isolated environment strips unrelated
  `MAKOLET_*` values while retaining the exact loopback target and local exception.
  The focused distribution/secret/license selection passed 56 tests, focused Ruff and
  mypy passed, and the rerun then completed: two builds and the sdist-derived wheel
  were identical; the 107-member wheel SHA-256 is
  `02ab42d932767ab1f8fa07a8b7cb101f6066909c5234f94618217922c987fb19`;
  the 291-member sdist SHA-256 is
  `4c506adead180059819b40bf872649c87e375f295674fd7f26f313609049c837`.
  The isolated install verified all 59 CLI help paths, exact metadata/licenses,
  benchmark extra, packaged Alembic graph, PostgreSQL 18 empty status, migration, and
  exact ready status at 0011. Cleanup proved database count zero and the exact
  temporary tree absent.

- 2026-08-17T21:52Z — image and supply-chain fixed-point gates passed. Image
  `makolet:local` was `sha256:5ac28a9a4e53f1f5bb044372323b901aed9a00305e529f7cd003eeba23645cd7`.
  The freshly generated 155,406-byte, 139-component Linux runtime SBOM, its committed
  document, and the image-embedded document were byte-identical at SHA-256
  `d6a06d38bd3bfdd868299add1473de06a160aa1d216a02b0b375bb4c39b66508`;
  semantic comparison ignored only the documented host-kernel platform value. Exact
  frozen all-groups and build-constraint `pip-audit` runs reported no known
  vulnerabilities, the all-groups SBOM matched all 84 locked components, and the
  license/build/image gates retained 84 locked, six build, 87 unique Python, 97
  Debian, six hash-pinned build distributions, and six immutable image pins. Both
  temporary audit trees and generated SBOM files were removed. The benchmark-relevant
  digest after the HTTP closure is
  `9908911b3856dbd424ec86c8496d2796a7dc73ea229e6f27610963674517fe85`.

## Recovery instructions

The repository now has local `main` at `54ffb79` and no remote. Inspect
`git status --short` before later edits. Restore the locked environment with
`uv sync --all-groups --frozen`. Discover active test services with
`docker compose ps -a` and `docker ps --format '{{.Names}}\t{{.Ports}}'`; do not stop,
remove, or reuse a database/container owned by another workstream. Real-database
tests must use a database whose name contains `test`; benchmark work must use its
dedicated URL/schema. Never paste credentials, signed URLs, or raw retailer files
into the plan. D/E remain out of scope. Isolated remote standard leftovers are gone.
A later disposable clone database `makolet_test_clone_54ffb79` was dropped; the
user-space PostgreSQL 18.4 install on loopback `127.0.0.1:55434` may still be
running and should be stopped after use. Historical artifacts `b9aababf` and
`c53ec893` must not be overwritten.

## Remaining work

None. Phase 8 is complete on digest `0daaa40f...` and first local `main` commit
`54ffb79666753aab42986c8fabc53d16c5449d56`. No Git remote exists, so remote
push/clone evidence is not claimed. Standard scan `3f46a47c-...` remains the sealed
zero-finding review with partial coverage; TAC advisory failed `USER_NOT_LOGGED_IN`
and is not a product finding. Do not start another scan unless `src/` changes.
