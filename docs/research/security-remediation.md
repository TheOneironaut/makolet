# Security remediation evidence

Originally verified 2026-08-12 against Codex Security scan
`946fe79f-4fc5-4661-9d95-990ed9dbe1fd`. The sealed scan describes the
pre-remediation directory snapshot
`codex-security-snapshot/v1:sha256:ec07278732a1424779896c890a8a902d7ce1fa104ee39adb90fb767e36f63fd4`.
This document is the clean-clone-visible record of the later remediation reviews;
it does not alter or supersede that evidence. Dated post-fix scans are recorded
below, but the newest `dcd9f5e6-...` snapshot is direct pre-fix evidence for seven
later remediations. One new final point-in-time scan remains required.

## Outcome

Fifteen of the original sixteen validated findings are fixed at their original security
boundary. The remaining high-severity plain-FTP finding is mitigated and recorded
as a deliberate operator exception, not marked fixed: discovery and download fail
closed by default, but an operator who sets `MAKOLET_ALLOW_INSECURE_FTP=true`
accepts a channel that still cannot authenticate the publisher. Such artifacts are
explicitly recorded with `transport-security=unauthenticated`.

An earlier sealed repository scan on 2026-08-16 found zero reportable findings in
its exact 292-file snapshot. The later 295-file release scan found eleven additional
paths, all addressed in the remediation section below; therefore the earlier result
is historical rather than current-tree closure evidence. No security scan substitutes
for external dependency-advisory freshness, publisher availability, or performance
acceptance.

## Finding ledger

### `csf_9db05bc9a18e3416f8d14b7c` - plain FTP substitution (high)

- **Outcome:** mitigated with a documented residual risk; not fixed when the
  explicit insecure-FTP opt-in is enabled.
- **Control:** `Settings.allow_insecure_ftp` defaults to false and both
  `StdlibFtpCatalogClient` and `FtpDownloader` reject plain FTP before credentials
  or bytes are exchanged. Configuration, composition, Compose, and the example
  environment preserve that default. When explicitly enabled, download provenance
  says `unauthenticated` rather than implying that a local SHA-256 authenticates the
  publisher.
- **Proof:** `tests/unit/test_config.py`, `tests/unit/sources/test_ncr.py`, and
  `tests/unit/test_ftp_downloader.py` exercise default rejection, explicit opt-in,
  and provenance. The pre-scan automatic/default path no longer reproduces.
- **Residual risk:** plain FTP has no peer authentication or transport integrity.
  Makolet does not have a publisher signature or an independently authenticated
  digest for these feeds, so a network-path attacker can still substitute bytes
  after an operator knowingly opts in. Prefer HTTPS or verified FTPS and keep the
  opt-in false unless that risk is explicitly accepted.

### `csf_cf54c92664470c7625a50709` - irreversible global GTIN match (high)

- **Outcome:** fixed.
- **Control:** migration `0002_identifier_evidence` and the ingestion repository
  separate retailer assertions from global identity. A first assertion is
  issuer-scoped, provisional, unvalidated, and tied to its source file. Independent
  retailer corroboration is required for a globally validated GTIN. Later
  corrections supersede old assertion lineage and replace only system-created
  automatic matches; disagreement with an operator decision becomes a reviewable
  `exact_identifier_conflict` candidate.
- **Proof:** real-PostgreSQL tests cover concurrent corroboration, a single scoped
  assertion, atomic correction/supersession, and preservation of a conflicting
  manual match. The migration-to-runtime metadata test also passed after an empty
  database migration to head.
- **Residual risk:** corroboration establishes independent-source agreement, not
  manufacturer-issued cryptographic proof. Operator-reviewed matches remain an
  intentional trusted decision and are never silently overwritten.

### `csf_03fec1d94d2647af6fb365d5` - FTPS certificate validation (medium)

- **Outcome:** fixed.
- **Control:** both FTPS catalog and artifact clients use
  `ssl.create_default_context()`, preserve the configured logical publisher host for
  SNI and hostname checks, and protect the data channel with `PROT P`. Python's
  verified default context requires a trusted CA and hostname match.
- **Proof:** the catalog and downloader tests assert an `SSLContext` with
  `check_hostname` enabled and `CERT_REQUIRED`, logical-host preservation, and
  protected data channels.
- **Residual risk:** trust follows the runtime CA store. The focused suite validates
  the exact stdlib verification contract with test doubles rather than running a
  separate local CA/mismatch handshake.

### `csf_e0078ec3ee1341bf9bb5c17a` - DNS validation/connection race (medium)

- **Outcome:** fixed.
- **Control:** HTTP resolves once, rejects non-public answers, connects to a vetted
  numeric address, and retains the logical host for `Host` and TLS SNI; each
  redirect repeats the process. FTP and FTPS likewise connect only to vetted
  numeric addresses, restore the logical hostname before TLS authentication, and
  ignore a server-supplied passive IPv4 address.
- **Proof:** downloader and listing tests assert numeric-address connection plus
  logical host/SNI, reject private DNS answers before connection or authentication,
  and validate redirects independently. FTP tests assert the vetted peer and no
  authentication after a private resolution.
- **Residual risk:** the allowlist and public-address classification must remain
  part of every new connector; the current shared connectors enforce it.

### `csf_9c8e6f840816fcd609a74cc6` - missing HTTP total deadline (medium)

- **Outcome:** fixed.
- **Control:** one monotonic AnyIO deadline encloses resolution, connection,
  redirects, response streaming, and cleanup. Response close is shielded so
  cancellation cannot strand the pool resource.
- **Proof:** focused tests use a slow response and a slow resolver, require the
  stable total-deadline error, and prove the live stream is closed.
- **Residual risk:** the configured duration remains an operator capacity choice;
  it is positive and bounded by validation.

### `csf_a9ea630aeb209ab0bb16dcdc` - abandoned FTP worker (medium)

- **Outcome:** fixed.
- **Control:** the blocking bridge owns a cancellation handle, closes the live FTP
  socket at deadline, shields the join, and returns only after the worker has
  terminated. Temporary-file cleanup therefore cannot race a live writer.
- **Proof:** separate catalog and artifact timeout tests assert socket release,
  cleared worker activity, and an empty temporary directory before return.
- **Residual risk:** a platform socket implementation that ignores `close()` would
  still be an operating-system defect outside the tested stdlib behavior.

### `csf_f667310399f8d891cc33b054` - unclosed rejected HTTP response (medium)

- **Outcome:** fixed.
- **Control:** response ownership is exception-safe: redirects, rejected status,
  invalid or excessive length, malformed redirect, and unexpected post-send errors
  close any response that is not returned. The accepted response is closed by its
  session context even on cancellation.
- **Proof:** invalid-header and encoded-listing tests track `aclose`; redirect,
  status, declared-size, and malformed-response branches are exercised by the HTTP
  download and listing suites.
- **Residual risk:** new post-send branches must retain the same single-owner
  `finally` pattern.

### `csf_053cab3432f81528c6484a92` - listing materialization (medium)

- **Outcome:** fixed.
- **Control:** listing requests ask for identity encoding and reject any non-identity
  `Content-Encoding` before iteration. They consume raw bytes, compare the
  prospective size before extending the bounded buffer, and then apply bounded
  HTML link/text/tag and JSON depth/string/scalar/structural-token preflights before
  object materialization.
- **Proof:** tests reject encoded bodies without iteration, declared and streamed
  overflow, oversized HTML tags/link counts, retained-text growth, and a JSON
  structural bomb before `json.loads` is called. Static-daily HTML applies the same
  preflight to its one embedded `const files` value before `raw_decode`; depth,
  token, plain/escaped-string, and scalar bombs never reach the decoder, while
  decoder JSON/recursion errors become a categorized source failure.
- **Residual risk:** a listing is still buffered up to its configured byte ceiling;
  the security invariant is that allocation and parser structure are bounded before
  attacker-controlled growth continues.

### `csf_34ffaf8aedb3e5b1de0c5946` - late gzip/Zstandard ratio check (medium)

- **Outcome:** fixed for the validated late-enforcement path.
- **Control:** gzip and Zstandard output is emitted in bounded chunks and cumulative
  decompressed-byte and expansion-ratio limits are checked before each chunk is
  yielded. Concatenated/trailing gzip data and multiple/trailing Zstandard frames
  are rejected, so an attacker cannot reset the accounting with another frame.
  Zstandard additionally sets `DecompressionParameter.window_log_max = 27`, bounding
  decoder history to 128 MiB before the first output byte exists.
- **Proof:** parameterized gzip and Zstandard regressions show that emitted bytes
  never exceed the configured ratio before rejection. A genuine nine-byte frame
  header declaring a 2 GiB history window is rejected by the bounded Python 3.14
  decoder, and a constructor-spy regression proves the production path supplies the
  configured option while ordinary supported frames still decode.
- **Residual risk:** byte and ratio work is bounded, but decompression does not yet
  have a separate parser-only wall-clock budget. Operational worker supervision and
  the absolute byte ceilings remain the CPU-containment controls.

### `csf_181fc627d01936dfbc8b622f` - ZIP central-directory allocation (medium)

- **Outcome:** fixed.
- **Control:** a bounded-tail EOCD preflight runs before `zipfile.ZipFile`. It
  rejects excessive entry counts or central-directory bytes, invalid offsets and
  counts, multi-disk archives, ZIP64 sentinels/extra fields, truncated metadata,
  and entries pointing outside the payload area.
- **Proof:** tests monkeypatch `ZipFile` to fail if constructed and then prove that
  excessive entries, inconsistent counts, ZIP64, and central-directory size are
  rejected first.
- **Residual risk:** ZIP64 and multi-disk inputs are deliberately unsupported rather
  than partially interpreted.

### `csf_8a70722e72cc77142ba7fd92` - XML token/text accumulation (medium)

- **Outcome:** fixed.
- **Control:** a public XML parser target and lexical token guard enforce cumulative
  text, CDATA, attribute-value, unfinished-token, element, depth, record, and record
  byte limits before ElementTree can retain unbounded attacker text. One shared
  configured spool directory contains both XML and ZIP spill files and cleans them
  after parsing.
- **Proof:** tests cover unknown-element text, comment-split text, chunk-split
  attributes, one-small-chunk unfinished markup, unfinished attributes, CDATA, and
  spool cleanup.
- **Residual risk:** increasing parser limits increases the permitted bounded work
  and should be treated as an operational capacity change.

### `csf_9d03a2d0f2a86f67f6d2ccf0` - wrong-root/HTML empty delta (low)

- **Outcome:** fixed.
- **Control:** the parser requires the regulated `Root`, the container for the
  declared document type, direct record children, one non-nested container, and a
  completely closed document. HTML is rejected even when preceded by an XML
  declaration. A zero-record delta is therefore a no-op only when it has the
  explicitly accepted regulated root and correct empty container; full snapshots
  remain subject to completeness thresholds before reconciliation.
- **Proof:** tests reject declaration-prefixed HTML, a wrong root, a stores
  container for a price document, records outside the container, and a missing
  expected container.
- **Residual risk:** the known strict empty-delta shape is accepted by policy; source
  adapters must not add alternate empty schemas without fixture evidence.

### `csf_e659a2fec9795e99e9501e4c` - one-character public search (medium)

- **Outcome:** fixed.
- **Control:** the shared query service requires at least three normalized
  searchable characters before repository work; the repository independently
  enforces the same floor. HTTP, CLI, and MCP return stable structured limit errors,
  while exact numeric barcode lookup remains separate. Valid search uses indexed
  prefix/trigram candidates capped at 10,000 before ranking.
- **Proof:** service and all three public interfaces prove short input never reaches
  the repository and barcode lookup remains available. A real-PostgreSQL test
  confirms repository-level rejection.
- **Residual risk:** route-level rate limiting remains the deploying reverse
  proxy's responsibility when the loopback/private read service is exposed.

### `csf_c8e6e909d047386f1bbbd4b2` - source-status history scan (medium)

- **Outcome:** fixed.
- **Control:** source status paginates portals first and uses one lateral
  latest-row lookup per portal, backed by
  `ix_source_files_portal_latest (portal_id, discovered_at DESC, id DESC)`. It no
  longer ranks the complete ingestion history before applying the public limit.
- **Proof:** a real-PostgreSQL test inserts 2,000 historical rows, disables
  sequential scans for a deterministic assertion, and confirms the latest-row plan
  uses `ix_source_files_portal_latest`.
- **Residual risk:** public concurrency/rate policy belongs at the deployment edge;
  per-request database work is bounded by requested portal count.

### `csf_1bfb5f76fc44936a7d23e26b` - public raw exception disclosure (low)

- **Outcome:** fixed.
- **Control:** ingestion persists a stable error code plus one of a small set of
  fixed public-safe messages, never `str(error)`. Source status selects only the
  error code and projects a constant public explanation. Operator logs suppress
  arbitrary non-domain exception detail, redact URI userinfo and secret-shaped
  assignments/query values, escape controls, and cap length.
- **Proof:** unit tests inject credential-shaped values and prove neither persisted
  status nor logs contain them; the real-PostgreSQL public-status test seeds a raw
  secret-bearing stored value and proves the query response replaces it with the
  constant message. The repository secret scan also passed.
- **Residual risk:** authorized operator logs intentionally retain bounded,
  redacted classified-domain diagnostics; they are not a public API.

### `csf_21efd037698365fda4e7c03f` - unbounded PostgreSQL export spool (low)

- **Outcome:** fixed for the local storage-exhaustion sink.
- **Control:** the first database-streaming pass counts rows and serialized bytes
  before each write, enforcing `max_dataset_rows` and `max_spool_bytes`. Spools live
  under the configured export root rather than the system temporary directory and
  are removed in `finally` on success and failure.
- **Proof:** focused export tests abort at row-plus-one, abort before exceeding a
  tiny byte budget, assert the configured spool location, and verify cleanup.
- **Residual risk:** the SQL does not add a separate `LIMIT max+1`; cancellation at
  the application cap bounds local disk growth, while a large ordered partition can
  still consume database work until the stream is closed.

### Bundled production credentials and shared state-service bind (medium, CWE-1392)

- **Outcome:** fixed.
- **Control:** production settings reject the bundled database password and either
  bundled S3 credential, including a URI-encoded database password. Compose requires
  all database/S3 secrets to be supplied explicitly instead of interpolating known
  fallbacks. Its PostgreSQL and S3 host ports are fixed to loopback, so widening the
  API/monitoring bind cannot expose either state service.
- **Proof:** focused settings tests cover plain and URI-encoded database passwords,
  S3 credentials, accepted operator-supplied production values, the copied local
  development environment, required Compose substitutions, fixed state-service
  binds, and the independently configurable API bind.
- **Residual risk:** the documented local environment intentionally uses known
  development credentials on loopback. Other local users remain inside that stated
  development trust boundary; non-local operators must replace every development
  value before selecting production.

### Aggregate source archive exhaustion (medium, CWE-400)

- **Outcome:** fixed and verified in application/storage code and PostgreSQL 18.4.
- **Control:** collection enforces a configurable charged-byte limit per attempt and
  an exact durable trailing-24-hour limit per retailer source. Before network I/O it
  durably reserves, under the locked retailer budget row, the permitted object bytes
  plus one 64-KiB transfer-frame allowance and, for FTP/FTPS, the bounded 256-KiB
  control channel. Every ordinary HTTP/FTP download receives a finite cumulative
  limit shared by all of its internal retries. Settlement replaces that reservation
  with one idempotent immutable-archive charge per source identity plus the actual
  failed/retried transfer overhead; internal retries reduce their next transport cap
  by bytes already received. Unsettled reservations remain conservatively charged
  after process death or an ambiguous post-CAS failure. Archived rejected evidence
  is settled before the traversal boundary advances. The remaining run/day budget
  narrows the downloader's declared and streamed byte cap before another object is
  committed. HTTP and FTP validate declared/listing length at iterator EOF, before
  the content-addressed archive sink commits, so invalid publisher evidence cannot
  leave an unlinked object outside the charge ledger. FTP download spooling, local
  archive writes, S3 upload spooling, XML/ZIP parser spills, and authenticated
  database-backup copies also fail retryably before retaining bytes across a
  configurable free-space reserve. Every positive-reserve Makolet writer sharing a
  root serializes the capacity check, one flushed bounded write, and its post-write
  check with a cross-process advisory lock.
- **Proof:** focused collection tests cover multi-file run truncation, later safe
  resumption, cross-attempt day exhaustion, pre-I/O reservation, successful retry
  overhead, conservative ambiguous-failure accounting, CAS/checkpoint retry
  idempotence, final-frame headroom, and a terminal rejected file. HTTP and FTP tests
  cover narrowed per-retry transport caps and prove length mismatches leave no
  committed local CAS object; FTP and local/S3
  archive tests cover the free-space circuit breaker. A deterministic two-process
  regression holds one writer between check and write and proves the second cannot
  enter or jointly cross the floor. Migration/runtime
  metadata and the SQL implementation preserve actual lengths and serialize budget
  updates under a locked retailer row. Memory and PostgreSQL settlement regressions
  prove that an exact charge above its reservation fails before state changes; the
  real-PostgreSQL proof also verifies rollback retains the original reservation.
- **Residual risk:** S3 has no portable API for total provider free capacity; the
  circuit breaker protects the local upload spool, while operators must retain
  provider-side storage quotas/alerts. The filesystem lock coordinates Makolet
  processes using the same root, not unrelated host applications; deployment must
  isolate the volume or govern those writers separately. The final combined
  PostgreSQL/S3 coverage run exercised current-head migration, concurrency, and
  accounting behavior; the container smoke repeated the database/archive recovery
  path.

## 2026-08-16 sealed follow-up scan

The sealed Standard follow-up scan
`bc53afd6-d4a9-4790-9343-507924e392b8` examined snapshot
`codex-security-snapshot/v1:sha256:f882f8224841a00578854dedc992da8d40d69000307e82c0ac084ef80f39c9b6`.
Its five findings were remediated and verified without service or network access;
the service-backed checks that were then pending were later separated into the
verified local PostgreSQL/S3/container drills and the still-distinct remote
production-like TLS negotiation noted below.

### `csf_0d920e4efd8e13b45d1023d5` - production PostgreSQL transport (medium)

- **Outcome:** fixed in configuration and engine construction. Local PostgreSQL
  service execution is verified; a separate remote production-like TLS negotiation
  was not performed.
- **Control:** a production remote database URL must select exactly one
  `sslmode=verify-full` or `ssl=verify-full` mode. Missing, disabled, opportunistic,
  encryption-only, and CA-only modes fail closed. The only plaintext exception is
  explicit and authority-host scoped to a literal loopback IP; DNS labels such as
  `postgres` and `localhost` require verified TLS. It also rejects `host`, `service`,
  or `servicefile` query overrides. The persistence
  boundary removes URL TLS switches and supplies asyncpg an `SSLContext` with
  certificate and hostname verification. Primary runtime, derived collection-lock,
  demo-seed, and online Alembic engines all use that centralized constructor; the
  trusted-local exception is propagated explicitly as `ssl=False`.
- **Proof:** regression tests cover every rejected mode, percent-encoded and
  duplicate query keys, remote misuse of the local exception, host/service override
  attempts, canonicalization, every sibling engine, and the exact SSL context passed
  to each engine. Red regressions reproduced both the stripped-URL collection-lock
  bypass and raw-environment demo/migration paths; the final related selection passed
  102 tests.
- **Residual risk:** certificate-chain and hostname negotiation against a real
  production-like PostgreSQL endpoint remains an operator/deployment acceptance
  check. The bundled private Compose database remains plaintext only in the
  documented development configuration; a production Compose deployment must supply
  verified TLS or a literal-loopback topology.

### `csf_ee055de024c21580c756f5f6` - embedded catalog JSON preflight (medium)

- **Outcome:** fixed.
- **Control:** static-daily `const files` catalogs preflight exactly one embedded JSON
  value before `JSONDecoder.raw_decode`, enforcing the shared 8 MiB byte, depth 64,
  500,000 structural-token, 65,536 decoded-character string, and 4,096-character
  scalar limits. Escaped string characters count toward the decoded bound;
  JSON/recursion failures become a categorized source response error. Inert trailing
  publisher JavaScript remains supported.
- **Proof:** depth, token, escaped-string, and scalar bombs all failed before the raw
  decoder after reproducing the vulnerable path; focused source tests passed 67
  cases with only the explicit live opt-in skipped.
- **Residual risk:** the bounded catalog body is still publisher-controlled and can
  consume work up to the documented limits; source scheduling and HTTP response
  limits remain the outer concurrency controls.

### `csf_529c4bb44414502fe6d0e08c` - Zstandard decoder window (medium)

- **Outcome:** fixed.
- **Control:** Zstandard construction sets `window_log_max=27`, capping decoder
  history at 128 MiB before output, expansion-ratio, or total decompressed-byte
  enforcement begins. Configuration is validated against the installed decoder's
  supported bounds.
- **Proof:** a genuine nine-byte frame requesting a 2 GiB window is rejected by the
  bounded decoder, the constructor-option spy proves the production setting, and a
  supported frame still decompresses. The focused parser/security selection passed
  36 tests.
- **Residual risk:** legitimate inputs may still consume up to the separately bounded
  compressed, decompressed, ratio, spool, and 128 MiB window ceilings.

### `csf_cdcea367ac2b215506e36017` - raw archive manifest authenticity (medium)

- **Outcome:** fixed in the backup/verify/restore implementation and verified by the
  real S3-compatible container drill.
- **Control:** every raw-archive recovery set now carries mandatory
  `manifest.json.hmac-sha256`, a versioned, domain-separated HMAC-SHA-256 over the
  exact canonical manifest bytes. Its distinct raw 32-byte key must be a protected,
  single-link regular file outside the backup tree. Authentication precedes checksum,
  JSON parsing, inventory validation, and restore. Bash and PowerShell mount only the
  selected key read-only into the one-shot operations container.
- **Proof:** the pre-fix subset attack rewrote the inventory and recomputed its plain
  checksum; the final regression rejects it before `json.loads`. Focused fake-client
  backup, verification, restore, script propagation, and single-snapshot tests passed.
  Wrapper regressions also prove the invoking POSIX identity reaches Compose and the
  Windows read-only bind is digest-bound into a private `0600` tmpfs copy while a
  direct `0644` key remains rejected.
- **Residual risk:** operators must retain the dedicated key separately from both the
  backup store and database-backup key. Losing it makes recovery deliberately fail
  closed; compromising it permits authenticating a malicious inventory. HMAC proves
  authenticity, not freshness: recovery operators must select the expected dated set
  from a separately protected inventory because an older legitimately signed set is
  still cryptographically valid. The 2026-08-16 container smoke authenticated,
  verified, and restored the archive into a confirmed-empty isolated bucket, then
  verified each restored object; this proves recovery behavior, not backup freshness.

### `csf_5ff6e705ca629c686cab7371` - raw archive metadata reads (low)

- **Outcome:** fixed.
- **Control:** manifest, checksum, HMAC sidecar, and authentication key are inspected
  with `lstat`, opened no-follow, revalidated with `fstat`, size-checked before
  allocation, and read with a strict `maximum + 1` bound. Fixed-format sidecars and
  the key require exact lengths; changing or replaced metadata fails closed.
- **Proof:** sparse oversized metadata, malformed fixed-format sidecars, hard-linked
  keys, and supported-host symlinks/reparse points are rejected. The combined focused
  security selection passed 108 tests before the wrapper follow-up; the owning final
  archive slice passed 32 tests with five explicit host/service capability skips.
- **Residual risk:** the authenticated object payload set remains governed by its
  existing per-object/count/digest limits. The full S3-compatible restore drill passed
  on 2026-08-16 and remains a release gate for future artifact changes.

## 2026-08-16 earlier post-fix freeze scan (superseded)

Codex Security scan `bcc531cc-b949-4dde-982e-93e00da343e6` completed and sealed over
the repository-root, 292-file snapshot
`codex-security-snapshot/v1:sha256:220682500829d7efb1d841a5dad82400c2f1e680cfa72aab982b4b639a1dcbf8`.
Its 12-row review worklist closed with zero reportable findings, an empty severity
inventory, and no validated Medium-or-higher vulnerability or regression. The review
covered hostile publisher/listing and BINA alias handling, SSRF/DNS/redirect/TLS,
FTP/FTPS, compression/XML/JSON limits, atomic ingestion and quota accounting, CAS/S3,
read-only API/MCP and SQL bounds, authenticated backup/restore, live-harness cleanup,
containers/Compose, CI/supply chain, export, logging, subprocess, migration, and
packaging paths. The accepted risks matched the owner-approved `SECURITY.md`, whose
private reporting channel is `amitibr19@gmail.com`.

One Low residual was recorded as defense-in-depth, not a validated vulnerability:
the fuzzy store query's parameterized, input/output-bounded
`LIKE ... OR similarity(...)` path has a trigram index and statement timeout but no
current-tree PostgreSQL plan assertion dedicated to proving index-bounded execution.
No Medium-or-higher availability impact was demonstrated. Add that focused plan
assertion if this query becomes release-critical or its dataset/request exposure
changes.

This is point-in-time evidence for the exact sealed snapshot. The scan itself was
read-only and used no network, Docker/WSL, services, publisher data, dependency
advisory lookup, or secrets. The documentation reconciliation that records the result
occurred afterward and therefore is not silently claimed as part of the sealed digest;
it changed no product, migration, deployment, test, or dependency input.

## 2026-08-16 sealed release-scan remediation

The later, broader Codex Security scan
`853a74cc-8dd6-4f44-9419-3a027f060d82` sealed over the then-current 295-file
unversioned snapshot and reported eleven high-confidence findings: ten Medium and
one Low. That result supersedes the earlier zero-finding snapshot above rather than
rewriting it. The owner-approved private reporting channel remained
`amitibr19@gmail.com`.

The eleven source-to-sink paths were remediated as follows:

1. Destructive integration/E2E and benchmark entry points share one validator that
   requires PostgreSQL, exact loopback authority, no query/fragment override, strict
   `makolet_test_...` or exact `makolet_benchmark` naming, and a matching explicit
   confirmation immediately before mutation.
2. ZIP members accept only STORE and DEFLATE before `archive.open`; decoder methods
   with independent dictionary/window allocation are rejected.
3. Container smoke forces the repository Compose file and example environment plus
   its own development database/S3/image selectors, so ambient Compose/S3 state
   cannot reach its mutation path.
4. XML Stores parsing restores Chain/SubChain values lexically when leaving nested
   wrappers and requires every nested SubChain containing Stores to declare its own
   direct, non-empty `SubChainID`. Root defaults and previous siblings cannot satisfy
   that scope, omission is a file-level error, and parser version `retail-xml/10`
   prevents stale staging reuse.
5. Fuzzy store search evaluates a 10,001-row UUID window and uses distinct first and
   cursor SQL, with an unconditional cursor index condition under generic plans.
6. Archive backup accounts exact canonical manifest/checksum/HMAC bytes before
   download and stops inventory iteration before retaining an over-16-MiB manifest;
   PostgreSQL dump capture/validation/sidecars share bounded capacity handling.
7. Staging charges promotion parents and every persisted relationship row plus a
   conservative 64-MiB retained-memory budget; a single oversized event is
   quarantined before admission.
8. Archive restore reads verified descriptors into deterministic noncanonical S3
   staging, verifies digest/length/ETag/version, conditionally publishes, and cleans
   ambiguous staging writes without touching a pre-existing canonical object.
9. Parquet validates and normalizes each row before retention, uses a conservative
   actual-value/encoder/output working-set charge, splits prospectively, and rejects
   one oversized row before PLAIN encoding.
10. At that snapshot, promotion queries decorated only returned parents under one
    1,200-child page budget. The later `dcd9f5e6-...` remediation supersedes this
    with one parent and seven children per relation kind. API and MCP share
    byte-accurate compact UTF-8 JSON preflight; the complete representation must fit
    1 MiB.
11. The image gate parses Dockerfile directives/stages/external sources and
    context-relative Compose builds/images bidirectionally, forbids default override
    files and unsupported YAML/build forms, and forces `makolet:local` to rebuild.

Red regressions reproduced the vulnerable boundaries before their respective
patches. Focused implementation gates and independent bypass reviews are recorded in
the living ExecPlan. Real PostgreSQL public-query plans and isolated raw S3 plus
PostgreSQL backup paths were exercised; final whole-tree/container/benchmark/live
fixed-point reruns remain separate release gates because these patches changed their
source digest.

## 2026-08-17 sealed release-scan remediation

The authoritative Standard scan `21ee984a-c5b1-43fc-834c-576917ff83d9`
reviewed sealed snapshot
`codex-security-snapshot/v1:sha256:a94fe54aa8b3c5caf0f43e7fbbd69cac5480e3ef64b418170eeb41f33196a1a1`
and reported twelve findings: three High, six Medium, and three Low. It covered the
product network, archive, parser, ingestion, persistence, public-interface,
deployment, backup, and recovery boundaries; supporting low-risk documentation and
fixture coverage was partial. The owner-approved private reporting address remains
`amitibr19@gmail.com`.

The current workspace remediates those paths as follows:

1. S3 parsing consumes one capacity-guarded, total-deadline spool and verifies the
   expected digest and length before exposure, then rehashes the exact parser-visible
   handle through EOF before apply.
2. Backup and restore wrappers pin the repository Compose file/project directory and
   reject ambient Compose/Docker selection. Archive backup pauses a running worker
   while it compares a bounded PostgreSQL raw-object inventory with a complete,
   bounded S3 listing before signing or downloading anything.
3. Runtime S3 reads and backup pagination enforce total request, progress, byte, and
   elapsed-time limits; production archive tooling no longer permits anonymous S3.
4. Existing discovery chronology and evidence are immutable. Only the owning
   pre-archive file lock may rotate a verified signed HTTPS URL or fill missing
   evidence, and archive-attached provenance cannot be relabeled.
5. One collection-run budget of 256 requests, 8 MiB, and 300 seconds flows through
   every production listing adapter and transport. Matrix fetches its catalog once
   per traversal.
6. Per-file ingestion and replay use crash-releasing PostgreSQL session advisory
   ownership and the owning connection for every mutation, so stale inherited tasks
   cannot stage, apply, replay, or recover after takeover.
7. Migration `0010_bounded_query_paths` maintains candidate projections and ordered
   indexes for current price, comparison, availability, history, and normalized city
   search. Public reads select exact `limit + 1` candidates before decoration.
8. Price and promotion history require paired aware bounds or use a first-page
   trailing 366-day half-open window whose exact bounds are pinned in the cursor;
   explicit spans remain capped at 3,660 days.
9. MCP request decoding preflights depth, token, decoded-string, and scalar limits
   before JSON materialization and converts JSON, recursion, Unicode, or numeric
   decoder failures to the stable JSON-RPC parse error on stdio and HTTP.

The archive/backup exploit rerun passed thirteen focused tests and its complete
no-service selection passed 850 tests with five host-capability skips and 66
deselections. The query lane passed 851 offline tests with five skips and 68
deselections; the ingestion lane passed 189 focused tests with one skip; the final
MCP unit file passed all seventeen tests. Ruff, mypy, documentation, secret, shell,
and PowerShell checks passed in their owning lanes. These are offline results, not a
substitute for the pending PostgreSQL 18 migration/locking/query-plan proofs, real S3
backup drill, combined coverage/container run, or final scan over the post-fix bytes.

## 2026-08-17 resource-bound scan closure

Standard scan `4db0500b-f557-409b-a2ec-1fa2f69c2f5d` sealed snapshot
`codex-security-snapshot/v1:sha256:50954d036284e19da95ac7ca32837042ea3e0731db0478ae52b644c8dd28cf3b`.
It completely reviewed 309 files across eight attack surfaces and reported three
resource-exhaustion findings: unbounded FTP multiline control replies, an unset HTTP
API concurrency ceiling, and product identifiers aggregated without a pre-aggregate
cardinality bound.

The FTP and FTPS clients now share a bounded linear control-reply reader with
per-reply and per-operation byte/line ceilings. Control and data bytes are settled
once across failure, retry, and successful archive attachment. The API publishes a
validated `1..10,000` Uvicorn ceiling with default 100. Product detail selects a
deterministically ordered 201-row probe before `jsonb_agg`, preserves exact output
through 200 identifiers, and returns the stable query-limit error on overflow.

Red regressions reproduced each missing control. The final focused FTP/ingestion
slice passed 76 tests; the API/query lane passed 173 focused tests and 1,015 unit
tests with seven expected skips. The final no-service selection passed 1,051 tests
with six capability skips and 75 deselections. PostgreSQL 18.4 then exercised the
current migration and public-query SQL in the combined container gate; that gate
passed 1,123 tests with six capability skips, three live deselections, eight known
warnings, and 85.51% branch coverage. The authenticated database and 25-object raw
archive backup/verify/restore drills, restored API queries, and exact isolated
project cleanup all passed. A fresh post-remediation scan over the final bytes remains
the last security fixed-point check; this section does not relabel the pre-fix
snapshot as post-fix evidence.

Standard scan `852eca2f-109a-407f-91eb-99c51ec76b41` then sealed the 312-file
snapshot
`codex-security-snapshot/v1:sha256:515c3c5a48bbcdb2bb03566b7fd5ce5f1b18c00ab84a59df776f3ee64bf55e00`
and found one low charged-byte reservation mismatch. Full HTTP reservations had
discarded their cumulative retry limit, while FTP/FTPS could add its bounded control
channel beyond payload and frame headroom. Collection now reserves a finite
protocol-aware maximum before every ordinary transfer, all retries share it, and
only caller-budget-limited oversize is a retryable boundary; a permanently oversized
publisher object remains terminal. Memory and PostgreSQL settlement compute the
prospective exact archive-plus-transfer charge and reject over-reservation before
state mutation. The focused lane passed 178 tests, the dedicated PostgreSQL rollback
test passed and its database was removed, and an independent bypass review returned
clean.

The subsequent Windows export-watchdog correction is not a security-finding waiver.
Native synchronous `CreateProcess` startup was measured at 0.13–1.32 seconds and
cannot be preempted safely without a persistent broker; reached child work and every
post-launch wait remain bounded and residue-checked. Short export budgets now reserve
at least four seconds for recovery when possible. The complete export file passed 30
tests, process/protocol passed 32, and the canonical no-service gate passed 1,062
tests with six documented capability skips. A new final scan is still required after
the remaining current-tree runtime and release gates.

## 2026-08-17 publisher, operations, and S3-boundary scan closure

Standard scan `88fc05da-2dcb-4a09-a061-d627bcbac877` sealed the authoritative
312-file snapshot
`codex-security-snapshot/v1:sha256:919dea9bda21f79383105fe935a20d6421026c8bac1b0f46d797d4eee8319dd4`
with complete coverage. It reported seven high-confidence findings: four medium
resource/transport/operations findings and three low discovery/live-harness findings.

The post-scan implementation now enforces these boundaries:

1. Every raw-archive `ListObjectsV2` response is streamed through an 8 MiB cap before
   Botocore XML materialization, while signed retries and valid pagination remain.
2. Production plaintext PostgreSQL/S3 exceptions accept only literal loopback IPs.
   Plaintext S3 additionally requires path-style addressing and bypasses ambient
   HTTP(S) proxies across runtime, demo, backup, and live clients.
3. The one shared publisher HTTP client uses a reject-all cookie jar, so publisher
   responses cannot accumulate or replay cookie state across requests or siblings.
4. The PowerShell watchdog refuses `.cmd`/`.bat` Docker resolutions and invokes only
   `docker.exe` through a structured argument list on Windows.
5. HTML discovery rejects plain-text/error bodies, lexical EOF, nested anchors,
   unclosed relevant containers, and pages with no recognized source files. The sole
   optional final HTML-root close is evidence-backed and allowed only after all other
   structure is complete.
6. Live S3 cleanup bounds response bytes before parsing and places all finalization-
   gating S3 work in a killable 30-second process boundary. Database cleanup is then
   attempted independently after success, error, or timeout.
7. JSON discovery requires either one exact unambiguous documented wrapper or a
   source-specific nonempty direct-array mode. Empty/error/ambiguous schema drift no
   longer becomes terminal empty discovery; the distinct embedded-index explicit
   empty-array contract remains supported.

Red regressions reproduced each original boundary and the independent audit's proxy,
in-flight deadline, plain-text HTML, lexical EOF, generic-container, direct-array, and
StaticDaily error-page bypasses before their respective fixes. The final source suite
passed 128 tests with one explicit live opt-in skip; the archive backup unit file
passed 80 tests with four host-capability skips; focused S3/config/live/watchdog
selections passed. PostgreSQL 18.4 accepted migration head
`0011_resource_probe_budgets` and ten real-service contract tests, a unique-prefix
SeaweedFS backup/restore test passed with zero residue, and all seven bounded live
listing lanes passed on the fixed source bytes. The scan-local
`artifacts/fix_report.md` contains the ordered red/green commands and remaining pinned-
Botocore upgrade caveat. A final independent current-byte reread found no blocker in
any of the seven findings. This is remediation evidence for the vulnerable snapshot;
one new final fixed-point scan over the release candidate remains required and must
not be inferred from this section.

At 2026-08-17T17:04:58Z, the then-current combined gate used
`C:\Program Files\Git\bin\bash.exe scripts/container-smoke.sh` and exited 0 under
isolated Compose project `makolet-smoke-local-1298`. PostgreSQL 18.4 migrated to
`0011_resource_probe_budgets`; the process collected 1,215 cases, selected 1,212,
passed 1,206, skipped six expected capability cases, deselected three live cases,
emitted eight known warnings, and measured 85.77% branch coverage in 311.81 seconds.
HMAC-authenticated database backup/restore returned the database to migration head;
raw-archive HMAC backup, source verification, clean isolated-target restore, and
target re-verification passed for all 18 objects. The post-restore API check passed,
and cleanup left zero exact-project containers, volumes, or networks. This
superseded the earlier container counts at that checkpoint. It predates the later
`dcd9f5e6-...` fixes and is now historical operational/recovery evidence; a new
combined gate and the final post-fix security scan remain required.

## 2026-08-17 sealed seven-finding pre-fix scan

Standard scan `dcd9f5e6-8410-49d0-90f9-533dffb6e20b` started at
2026-08-17T17:04:22.401013Z and sealed exactly once at
2026-08-17T18:05:00.894718Z. Its authoritative 312-file snapshot is
`codex-security-snapshot/v1:sha256:8d66bc4e0949546c8a6e2098254eca69e8b2d58bcfcb7c9901ff77294f735dc1`.
Coverage was complete across source, migrations, deployment, scripts, tests,
Compose/Docker/CI, policy, and operations/security documentation. The scan reported
five Medium and two Low resource-exhaustion findings:

1. Medium `occ_185d269c8adc55758d4b9fd2`: MCP validation-diagnostic fan-out.
2. Medium `occ_e965ad22dffd892e1f110676`: public page materialization before
   the response-byte limit.
3. Medium `occ_b6db931376ac9931b6a034cf`: loss of the configured PostgreSQL
   statement timeout on lease-owned ingestion connections.
4. Medium `occ_fbc409368fdcecfda48535fe`: artifact-download DNS fan-out and HTTP
   control-byte accounting.
5. Low `occ_733f203540ad295ecedfa55f`: the equivalent listing-accounting gap.
6. Medium `occ_5f461e91ab2a33a0be772829`: runtime S3 response materialization
   before Botocore-side application limits.
7. Low `occ_7f4fa70f203a6fff789249a1`: the sibling backup/restore S3 gap.

All seven occurrences are remediated on the later working tree. This scan remains
the direct pre-fix evidence and is not relabelled as a post-fix result. A new sealed
repository-wide scan over the frozen release candidate is still required.

## 2026-08-17 MCP, lease-timeout, and public-page closure

MCP now rejects unknown argument keys before Pydantic with one fixed diagnostic and
uses the validation error count before materializing at most 16 typed issues. The
three-case pre-fix contract produced two failures and one legitimate typed-diagnostic
control pass; the exact contract passed all three cases after the fix. The complete
MCP/database unit files passed 34 tests, and 90 related configuration,
migration-environment, ingestion-security, and collection tests passed.

`Database.from_url` now retains its validated statement timeout and passes it to both
derived `NullPool` lock engines. A real PostgreSQL 18.4 proof on a uniquely named
disposable database canceled `pg_sleep(1)` at the configured 100 ms timeout on the
lease-owning connection, rolled back, then successfully ran `SELECT 1` on that same
connection. The database was force-dropped and the matching catalog count returned
zero. TLS, UTC session state, distinct application names, advisory-lock ownership,
and rollback behavior remained unchanged.

Public request validation remains 1..200 for normal pages and 1..1,000 for history,
but candidate materialization now stops at route-specific parent caps before the
repository fetch: retailers 50; stores and product search 5; current prices,
comparison, price history, and availability 28; active promotion and promotion
history 1; freshness 50; and source/platform status 32. A shorter non-final page
uses the ordinary deterministic cursor. Each promotion parent exposes at most seven
ordered items, seven stores, and seven clubs (21 children total), with returned-count
and per-collection truncation fields.

Eight maximum-four-byte-publisher MCP page regressions failed before the final cap
correction and pass on the current tree. The largest complete JSON-RPC response in
that proof was 1,011,955 bytes, below the 1,048,576-byte protocol ceiling; the
duplicated tool-result representation also remained below its 1,044,480-byte working
budget. The focused query/API/MCP/SQL selection passed 104 tests, and all 22
synthetic benchmark-contract tests passed. The final no-service selection passed
1,164 tests with six documented host-capability skips, 77 deselections, and the one
known Starlette/httpx warning in 165.60 seconds. Whole-tree Ruff format checked 238
files, Ruff lint passed, and mypy checked 173 source files. Documentation checked 43
Markdown files/99 links, and the secret scan checked 313 repository files.

The complete seven-test `tests/integration/test_public_query_contract.py` file then
passed against PostgreSQL 18.4 after stale pre-cap expectations were aligned with the
intentional short-page contract. This evidence does not substitute for the remaining
frozen-tree PostgreSQL/S3 service slice, combined container gate, standard benchmark,
or post-fix security scan.

## 2026-08-17 runtime and recovery S3 pre-parse response closure

Standard scan `dcd9f5e6-8410-49d0-90f9-533dffb6e20b` identified two related
resource-exhaustion occurrences on its sealed snapshot:
`occ_5f461e91ab2a33a0be772829` covered runtime CAS operations, and
`occ_7f4fa70f203a6fff789249a1` covered backup/restore operations other than the
already bounded listing path. In both paths, default Botocore behavior could join an
attacker-controlled error or non-streaming success body before application object,
spool, or deadline checks regained control.

The remediation extracts the proven signed `before-send` listing interceptor into
one shared S3 transport. Runtime registers `HeadBucket`, `PutObject`, `HeadObject`,
and `GetObject`; the recovery tool registers `ListObjectsV2`, `GetObject`,
`HeadObject`, `PutObject`, `CopyObject`, and `DeleteObject`. Each Botocore request
context owns one 8 MiB control/error response budget shared by all retry attempts.
Declared length is checked before reading, chunked bytes are charged before retention,
encoded, malformed, inconsistent-length, and non-byte responses fail closed, and the
underlying raw body is closed. A successful `GetObject` remains untouched and streamed
through the existing object-length, digest, HMAC, deadline, and capacity boundaries;
conditional create/copy/delete and staging cleanup semantics are unchanged.

Deterministic pre-fix tests used Botocore 1.43.68 with a synthetic HTTP session at the
real `before-send`/parser boundary. Eight exploit cases failed while the legitimate
successful-`GetObject` streaming control passed: oversized runtime `PutObject`
success, oversized runtime `GetObject` error, all five recovery sibling operations,
and two individually sub-limit listing responses whose retry aggregate exceeded 8
MiB. After the patch, that exact nine-case contract passed. A first unique-prefix
SeaweedFS backup/restore run then exposed a legitimate HTTP `HEAD` distinction:
`Content-Length` and `Content-Encoding` describe the selected representation, not a
HEAD response body. The shared transport was corrected to ignore representation
headers for `HEAD`, 204, and 304 while still byte-capping any raw content that arrives;
an exact oversized-representation-header regression was added. The resulting ten-case
security/control selection and three signed-listing compatibility controls passed, and
the complete owning unit files passed 109 tests with four host-capability skips.
Focused Ruff format/lint and mypy passed. The same real SeaweedFS unique-prefix test
then passed backup, local verification, clean-target conditional restore, remote digest
verification, and `finally` cleanup. The final combined release gate must still
re-exercise runtime child-process and container recovery paths on frozen bytes.

## 2026-08-17 HTTP DNS fanout and wire-accounting closure

Standard scan `dcd9f5e6-8410-49d0-90f9-533dffb6e20b` identified artifact occurrence
`occ_fbc409368fdcecfda48535fe` and listing occurrence
`occ_733f203540ad295ecedfa55f`. Both shared the same boundary: DNS could return an
attacker-sized set of public addresses, the pinned sender could attempt every address,
and application budgets saw only retained response-body chunks after HTTP control work
had already been parsed.

Before invoking the OS resolver, the shared meter commits a 64 KiB DNS control charge;
resolution failures therefore no longer consume network/CPU time while the durable or
discovery byte counter remains at zero. The resolver accepts at most four unique public
addresses and still vets every accepted value before any connection. Remote policies
retain the existing three-redirect ceiling. Each physical address attempt commits a
128 KiB conservative control floor before send, and discovery also consumes one
request unit per attempt. On the pinned
production `httpx==0.28.1` / `httpcore==1.0.9` AnyIO transport, the raw byte stream is
wrapped below TLS before `TLSStream.wrap`. TLS handshakes and encrypted records are
therefore charged at the ciphertext boundary; the returned decrypted stream is not
wrapped again, avoiding plaintext double counting. Cleartext HTTP is charged at the
same raw boundary before parsing, covering informational responses, final headers,
chunk framing, trailers, and payload. A production-shaped backend that cannot expose
the pinned raw AnyIO boundary fails closed before TLS. Mock/custom transports retain
the DNS and attempt floors plus materialized-header and exact-payload accounting.
`Host`, TLS SNI, public-address pinning, redirect revalidation, identity encoding,
response deadlines, and exact archived body bytes are unchanged.

Artifact sessions report the larger of exact raw I/O and the conservative
control-plus-payload estimate through `transferred_bytes`; all failure branches carry
that value into existing retry accumulation and durable settlement. One open reserves
2.25 MiB for four DNS lookups and four vetted-address attempts across each of four
logical hops, plus 22 MiB for normal TLS 1.3 record framing through the supported
16 GiB object ceiling. Collection therefore reserves 24.25 MiB per independently
metered open and 97 MiB across four downloader retries, in addition to the existing
final-frame headroom. Pathologically padded or fragmented TLS is still bounded by the
raw-wire meter. Discovery uses the same per-open meter directly against its cumulative
request/byte/time budget, so a failed resolution/address, header-only redirect, or
high-control response cannot advance the cursor or continue outside the run ceiling.

The original pre-fix focused collection failed because the accounting boundary did not
exist. After the first patch, an independent production-shaped TLS regression exposed
that the delegate performed its handshake before the returned plaintext stream was
wrapped: the meter remained at zero for handshake ciphertext. Two companion DNS tests
also proved that resolution failures carried zero bytes. All three failed before the
follow-up patch and pass now. The full 54-test HTTP/listing contract additionally proves
excess-answer rejection before send, legitimate two-address failover, DNS charging,
header-only redirect charging, a four-frame 103 Early Hints flood stopped before final-
header materialization, exact TLS handshake/record accounting without plaintext double
counting, unsupported-TLS-backend fail-closed behavior, transport-failure charging, and
retry charge preservation. The broader source/downloader/collection/ingestion selection
passed 265 tests with one explicit live-source opt-in skip. A final reservation and
unsupported-backend closure then passed 147 focused downloader, listing, collection,
and configuration tests. The final focused branch run passed 59 tests with 88.36%
combined branch coverage across the downloader and listing adapters (87% and 94%
respectively). Ruff format/lint and mypy passed the changed files. No external network
or service was used.

## Verification performed

The earlier 2026-08-16 follow-up remediations were confirmed on benchmark-relevant source
digest `a22c9e5a9d43946dc2fec06b722b9a3521083722e71ff2d5bca51d05518be19a`.
The security-owning selection passed 131 tests with four host-capability skips. The
canonical no-service selection passed 608 tests with four skips and 56
integration/live/benchmark cases deselected. Whole-tree Ruff format/lint and mypy
passed; documentation checked 42 files/97 links, secret scanning checked 285 files,
and Git-for-Windows Bash plus the PowerShell parser accepted the operational scripts.
No service, container, WSL, publisher network, or live source was used in that
earlier check. Later current-head PostgreSQL/S3 coverage and the authenticated
container archive backup/verify/restore drill passed. A remote production-like
PostgreSQL TLS handshake remains distinct from the verified engine-construction and
bundled trusted-local contracts.

The older evidence below records the original remediation scan and is retained for
provenance; its smaller file/test counts are not current-tree totals.

The focused regression selection intentionally used `--no-cov`: the repository's
85% threshold applies to the complete suite, whereas this selection exercises only
the security-owning modules. The first identical selection without `--no-cov`
executed all 139 tests successfully but exited 1 because aggregate repository
coverage was 64.43%. It was not treated as a passing command or as a full-suite
coverage result.

```text
uv run pytest --no-cov -q tests/unit/test_config.py tests/unit/test_ftp_downloader.py tests/unit/test_http_downloader.py tests/unit/sources/test_ncr.py tests/unit/sources/test_http_listing.py tests/unit/sources/test_common.py tests/unit/parsers/test_streams_security.py tests/unit/parsers/test_xml_security.py tests/unit/test_query_service.py tests/unit/test_api.py tests/unit/test_cli.py tests/unit/test_mcp.py tests/unit/test_ingestion_service.py tests/unit/export/test_postgres.py
139 passed in 10.47s

MAKOLET_TEST_DATABASE_URL=<dedicated-local-test-url> uv run pytest --no-cov -q tests/integration/test_postgres_persistence.py::test_concurrent_equal_gtin_uses_one_canonical_identity_without_orphans tests/integration/test_postgres_persistence.py::test_single_retailer_gtin_is_scoped_provisional_evidence tests/integration/test_postgres_persistence.py::test_gtin_correction_supersedes_lineage_and_replaces_automatic_match tests/integration/test_postgres_persistence.py::test_exact_identifier_conflict_preserves_manual_match_for_review tests/integration/test_postgres_persistence.py::test_public_queries_bound_short_search_and_sanitize_latest_source_error tests/integration/test_postgres_persistence.py::test_latest_source_status_plan_uses_bounded_portal_index
6 passed in 5.93s

MAKOLET_TEST_DATABASE_URL=<dedicated-local-test-url> uv run pytest --no-cov -q tests/integration/test_postgres_persistence.py::test_migration_matches_runtime_metadata
1 passed, 3 known computed-default comparison warnings in 1.72s

uv run ruff format --check <41 focused source, migration, and test files>
41 files already formatted

uv run ruff check <41 focused source, migration, and test files>
All checks passed

uv run mypy <41 focused source, migration, and test files>
Success: no issues found in 41 source files

uv run python scripts/check_secrets.py
Secret scan passed for 235 repository files

uv run python scripts/check_docs.py
Documentation link check passed for 40 Markdown files and 87 local links
```

The exact 41-file static-analysis selection comprises the remediation-owning
configuration/composition, ingestion/query services, network/listing/parser,
persistence/schema/migration, export and public-interface modules plus the focused
test files named above and `tests/integration/test_postgres_persistence.py`.

The required scan-local companion report is written to the existing scan's
`artifacts/fix_report.md`. Only that new remediation report was added under the
scan directory. The sealed manifest, canonical findings, coverage, and generated
report were not rewritten.
