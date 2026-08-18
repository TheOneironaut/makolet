# Changelog

All notable changes are documented here. The project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and will use semantic
versioning for public releases.

## [Unreleased]

### Fixed

- Matrix whole-catalog discovery now fails closed on empty named wrappers such as `{"files": []}` or `{"data": []}`, matching the existing direct-`[]` reject. A nonempty named collection still succeeds.
- BINA discovery now treats an empty direct JSON array as an empty partition rather than a broken catalog. An unpublished Stores family therefore no longer blocks later Price/Promo partitions on the same date.
- FTP/FTPS listing and download now reject more than four unique public DNS answers
  and count every vetted-address connect as its own listing request. FTPS meters
  TLS ciphertext, including handshake records, below protocol parsing so padded or
  fragmented records cannot hide behind plaintext control/payload ceilings. Durable
  FTP/FTPS reservations therefore cover the 256 KiB control channel, four address
  attempts, and TLS 1.3 framing through the 16 GiB object ceiling.
- Public query request limits remain compatible at 1–200 normally and 1–1,000 for
  history, while route-specific candidate caps now stop publisher-controlled text
  before repository materialization. Short non-final pages carry their ordinary
  cursor. Promotion pages materialize one parent with at most seven items, seven
  stores, and seven clubs, with explicit returned counts and truncation flags.
- HTTP publisher access now charges a 64 KiB control allowance before each DNS lookup,
  rejects more than four unique vetted answers, and charges every physical address
  attempt. The pinned production transport meters cleartext HTTP and TLS ciphertext,
  including handshake records, below protocol parsing and fails closed when that raw
  boundary is unavailable. Listing budgets therefore include redirects, failover,
  informational responses, headers, chunk framing, trailers, and payload. Normal
  TLS 1.3 record framing through the supported 16 GiB object ceiling is reserved as
  well: one independently metered open has 24.25 MiB of bounded transport headroom,
  and artifact downloads settle the same accounting through four retries under a
  97 MiB durable transport reservation. Pathologically padded or fragmented TLS is
  still stopped by the raw-wire meter.
- MCP rejects unknown tool-argument members before Pydantic can allocate one
  diagnostic per member and caps remaining typed diagnostics before materialization.
  PostgreSQL collection and ingestion lease engines now inherit the configured
  statement timeout together with TLS, UTC session state, application names, and
  unpooled lock semantics.
- Source discovery no longer retains publisher cookies across the shared HTTP client,
  rejects lexically or structurally incomplete HTML at EOF, and requires each JSON
  adapter's exact documented wrapper or explicit direct-array mode. This prevents
  partial/error catalogs from becoming successful terminal empty discovery.
- Runtime raw-archive and backup/restore S3 clients now cap every error and
  non-streaming success before Botocore materialization under one 8 MiB budget shared
  by retries; successful `GetObject` bodies remain streamed into the existing
  deadline, length, digest, and filesystem-capacity controls. Production plaintext
  PostgreSQL/S3 exceptions accept only literal
  loopback IPs and direct S3 connections bypass ambient proxies, PowerShell watchdogs
  reject command-script Docker shims, and the live acceptance harness pre-bounds S3
  listing bodies and process time while attempting database cleanup independently.
- Collection now reserves a finite protocol-aware transfer ceiling before every
  ordinary HTTP, HTTPS, FTP, or FTPS download. FTP reservations include the bounded
  control channel as well as payload and final-frame headroom, retries share that
  ceiling, and both persistence implementations reject exact settlement above the
  durable reservation without consuming or releasing it.
- The read-only HTTP API now applies a validated Uvicorn concurrency ceiling, product
  detail bounds identifiers before JSON aggregation and fails stably above 200, and
  FTP/FTPS listing and download paths bound multiline control replies and settle
  control-channel bytes together with payload bytes across retries and successes.
- Export operations now reserve enough of short wall-clock budgets for killable
  publication recovery. Their Windows process tests allow bounded native
  `CreateProcess` startup latency while separately proving that every reached
  filesystem stall is killed, cleaned, and reaped; they also wait for complete PID
  signals and discount only CPython's measured one-time process-runtime handles.
- Container smoke now requires PowerShell 7 when launched from a Windows POSIX shell,
  avoiding an unsafe late failure in the authenticated archive backup/restore drill
  under Windows PowerShell 5.1.
- Archive reads and writes now run behind authenticated, parent-bound, killable child
  processes with bounded bootstrap, termination, and reaping. Exact HTTPS scheme/port
  validation rejects port zero before DNS, and the continuous worker owns shutdown
  signals outside Uvicorn so its watchdog also bounds lifespan and outer event-loop
  teardown without leaving credential-bearing children.
- Raw-archive backup wrappers now serialize each Compose project through an
  ownership-checked lock, treat both running and restarting workers as active, and
  suppress restart unless worker quiescence and exact operation-container cleanup are
  proven. Bash and PowerShell supervise every inspect, stop, run, cleanup, and restart
  command with bounded watchdogs.
- PostgreSQL Parquet export now resolves destinations and streams bounded rows through
  a killable spool/publisher process, so create, write, flush, fsync, close, link,
  rename, cleanup, and recovery all honor one wall-clock budget. Durable publication
  journals preserve already committed immutable manifests while removing only the
  interrupted operation's staging and temporary artifacts.
- Security hardening now freezes existing source chronology and archive-attached
  provenance, carries cumulative request/byte/time budgets across an entire listing
  traversal, and replaces expiring per-file ingestion leases with crash-releasing
  PostgreSQL session ownership that fences staging, apply, replay, and recovery.
- Public product-state and history reads now use transactionally maintained
  canonical-product projections, candidate-first keyset plans, paired and
  cursor-pinned history windows, and normalized city-leading indexes. Migration
  `0010_bounded_query_paths` adds the required projections and indexes without
  silently truncating public results.
- S3 parser reads now verify and parse the same deadline-bounded spool; raw-archive
  backup signs an exact PostgreSQL inventory only after a complete bounded S3
  comparison. Runtime and backup S3 operations have total budgets, production tools
  require credentials, and backup wrappers reject ambient Compose target selection.
- MCP JSON decoding now rejects excessive nesting, structural tokens, decoded
  strings, and scalar lengths before `json.loads`; malformed or recursion-triggering
  bodies return the JSON-RPC parse error and do not terminate stdio or produce an
  HTTP 500 response.
- Repository licensing policy now records the owner's narrow approval for
  non-redistributed Ruff tooling, compliant unmodified MPL-2.0 artifacts, and
  separately licensed base-OS/service components with complete SBOM and distribution
  obligations; product and build dependencies remain OSI-approved and
  Apache-compatible.
- Runtime SBOM parity now compares the complete stable CycloneDX document rather
  than only component names and versions. All Debian components carry exact DEP-5
  license labels or hash-bound reviewed legacy labels; CI rejects changes to
  licenses, evidence hashes, source metadata, native links, package identities,
  base-image metadata, or dependency edges while ignoring only the Docker-host
  kernel value in `makolet:platform`.
- Container smoke now supplies every forced wheel input to the image builder,
  isolates host-side pytest from Compose-only runtime variables, uses the hardened
  PowerShell archive operations on Windows POSIX shells, and rediscovers the random
  published API port after database restore. With publisher collection disabled, it
  exercises non-root/read-only API and worker containers plus the Prometheus
  API/worker/SeaweedFS targets, stops long-lived database clients during the
  destructive coverage reset, and restarts the full stack after recovery. The
  complete migration, coverage, authenticated database/archive backup-and-restore,
  restored-bucket re-verification, and post-restore API drill now run successfully
  from Git Bash while retaining the native POSIX-host path.
- The Maayan 2000 BINA Price partition now records the publisher's observed ZIP
  wrapper even when the object is named and typed as Gzip. XML parser
  `retail-xml/10` accepts the observed direct `ItemNm`, `ManufactureCountry`, and
  `bIsWeighted` aliases while retaining fail-closed structural parsing for every
  unobserved path.
- Built wheels now carry the complete Alembic graph and notice files, while the
  packaged benchmark command declares its pinned `benchmark` extra. Explicit wheel
  and sdist inventories exclude generated benchmark evidence, agent instructions,
  and caches, and a distribution-content gate prevents those package regressions in
  CI.
- Distribution and secret gates now inspect every assignment and credentialed-URI
  candidate on a line, reject every unclassified package payload, scan `Dockerfile`
  explicitly, and compare the installed Typer tree bidirectionally with all 59
  advertised commands. The isolated installed-package database proof also uses a
  supported `ERROR` log level, so its empty-status/migrate/status sequence exercises
  runtime configuration instead of failing in the test harness.
- Promotion-history and freshness reads now select their ordered `limit + 1`
  promotion/store candidates before expanding related rows. Each freshness store
  counts at most 1,000 deterministically probed availability rows, reports
  `items_truncated` when more exist, and obtains its globally latest contributing
  observation through a separate indexed top-one lookup. A high-cardinality store
  therefore cannot turn a small public page into an unbounded current-state scan.
- FastAPI/OpenAPI and all fourteen MCP tools now expose endpoint-specific closed
  output contracts with exact UUID, aware-datetime, fixed-precision string, source
  provenance, status, page, and promotion-child bounds. Both interfaces validate
  service output before return and convert malformed mappings or request shapes to
  non-echoing, secret-safe structured errors.
- Product search now separates product text from a fixed-precision structured
  quantity, normalizes English/Hebrew mass and volume aliases to base units, and
  prevents numeric substring matches such as `1` matching `10`. Freshness reads now
  identify their deterministic contributing source file and archive hash.
- XML parser `retail-xml/10` accepts business values only from documented direct and
  relationship paths, so known leaves under additive wrappers cannot shadow direct
  fields. Additive wrappers, leaves, and attributes emit durable bounded warnings;
  unparsed promotion conditions are retained in restrictions, while renamed required
  fields fail the record. Amount and conditional promotion kinds have explicit parser
  evidence.
- Naive publisher times in Jerusalem's DST gap or fold are now rejected unless an
  explicit offset disambiguates them, preventing silent one-hour chronology shifts;
  invalid optional listing modification metadata is omitted rather than poisoning an
  otherwise valid discovery row.
- Public query cursors are now versioned, checksummed envelopes bound to each route
  and normalized filter set; active-promotion pagination also fixes the first-page
  instant across subsequent pages.
- Raw-archive backup format v2 binds manifests and every service key to the
  configured S3 key prefix, rejects non-canonical digest shards and key/content hash
  disagreement, and has a non-default-prefix S3 restore regression.
- Repeated raw bytes under a new source identity now create a new provenance/apply
  observation, with exact parser-context staging reuse and unchanged-history
  suppression instead of a content-level apply short circuit.
- PostgreSQL Parquet exports now enforce row-level timestamp bounds, exclude
  half-open history intervals ending exactly at the lower boundary during partition
  discovery and row streaming, use one repeatable-read snapshot, and include
  portal/document/raw-archive provenance.
- Promotion XML records no longer silently omit malformed item, store, or club
  relationship children; the whole promotion becomes a durable record rejection.
- One-shot workers now render their complete source summary and return temporary
  failure when any source outcome failed.
- Database status and API readiness now fail closed unless the current Alembic
  revision set exactly matches the repository heads.
- Unexpected HTTP exceptions now return a generic request-correlated 500 response
  and emit no exception message or credential-bearing traceback.
- Database restore now migrates and validates its isolated staging database before
  stopping services or promoting it.
- Bounded discovery no longer restarts at the catalog head after reaching the file
  cap: portal/range/generation-scoped checkpoints resume at retry-safe boundaries,
  and full Stores rosters now reconcile omitted subchains portal-wide under the
  configured suspicious-drop guard.
- Completed stable source identities now retain their original URL, listing metadata,
  and discovery/source timestamps when rediscovered, so rotated signed URLs cannot
  rewrite immutable download provenance or leak through public source status.
- Parser-unchanged normalized rebuilds now preserve durable catalog identities,
  reviewed matching decisions, observation/history/promotion UUIDs and timestamps,
  public cursor continuity, exact provenance, and deterministic Parquet bytes.
  Durable bounded snapshots make interruption resumable and completion fail closed;
  parser corrections retain preserved/superseded audit evidence instead of erasing
  the old normalized rows.

### Added

- Deterministic process-protocol and PostgreSQL-export helper tests raise combined
  real-service branch coverage above the required 85% threshold without weakening
  the coverage configuration or excluding production modules.
- Current-tree release evidence now retains the historical `c53ec893` standard
  failure and the later quiet-host pass on digest `0daaa40f...`. The passing artifact
  is `benchmarks/results/20260819-standard-all-0daaa40f.json` (SHA-256
  `ec4a0e17b8edf0562a70315535cb1ab940e166b13b4f32b6289fef4ea161eade`): staging
  34,767.74, apply 3,958.71, and amplification 2,584.90 rows/s all clear the 70%
  floors; plan_gate passed with zero failures. The earlier
  `20260816-standard-all-c53ec893-e-drive.json` remains historical evidence of the
  contended failure (65.56% / 63.80% / 23.75%) and was not overwritten.
- Final current-tree evidence records the bounded representative ingestion and all
  seven credential-free HTTPS listing smokes, a reproducible 107-member wheel and
  291-member sdist, and a byte-identical 139-component runtime SBOM in the rebuilt
  image on digest `0daaa40f...`. Disposable live, distribution, runtime, and
  container-smoke resources were removed or returned to their verified baselines; no
  source payload or credential was retained.
- The distribution gate now proves reproducibility with two controlled offline,
  hash-constrained builds, rebuilds the wheel from the generated sdist, requires
  byte-identical artifacts, installs the rebuilt wheel with pinned dependencies,
  and exercises packaged migrations, license/notice evidence, and the CLI outside
  the source tree.
- A repository-wide security policy now defines the supported-version window,
  private reporting channel, trust boundaries, mandatory security invariants,
  reportability criteria, accepted risks, and compensating controls. The policy is
  included in source distributions and enforced by the distribution-content gate.
- An explicit opt-in representative live-ingestion harness sends one bounded Maayan
  2000 price-delta file through production download, immutable S3 archive, XML
  parsing, PostgreSQL apply, matching, replay, and QueryService/CLI/HTTP/MCP
  provenance checks in disposable loopback-only scopes. Offline safety regressions
  reject database driver host/service overrides before any connection.
- Apache-2.0 licensing, compatible dependency inventory, CycloneDX SBOM, and
  clean-room research ledgers.
- A 28-row official-retailer registry and discovery adapters for BINA, Matrix/Laib,
  NCR FTP/FTPS, static daily indexes, Hazi Hinam, City Market, and Shufersal, with
  explicit disabled records for unresolved or externally blocked sources.
- Bounded exact-byte HTTP/FTP download routing and immutable local/S3-compatible
  SHA-256 archives.
- Streaming XML/compression handling for Stores, Price/PriceFull, and Promo/PromoFull,
  including UTF-8, Windows-1255, UTF-16, record rejection, and hostile-input limits.
- PostgreSQL 18 schema, Alembic migration, COPY staging, transactional apply,
  idempotence, leases, replay, quarantine, full-snapshot reconciliation, current
  state, validity history, product matching, bounded query repositories, and
  idempotent retailer/portal registry synchronization during migration.
- Read-only CLI, FastAPI/OpenAPI API, and typed MCP stdio/HTTP interfaces over shared
  application services.
- Explicit portal-aware item-code lookup across CLI, HTTP, and MCP with structured
  cross-portal ambiguity errors; source-file/archive provenance on price reads; and
  complete, bounded promotion-version relations and conditions.
- A complete bounded CLI public-read surface for retailers, filtered stores, barcode
  and retailer-item lookup, current/comparison/history prices, active/historical
  promotions, availability, freshness, and per-portal source status.
- Bounded timezone-aware archive-range replay and operator-confirmed, restartable
  normalized rebuilds with durable audit state and a fail-closed ingestion barrier.
- Automatic one-item canonical bootstrap for otherwise unmatched retailer SKUs,
  bounded exact/structured candidate generation, and transactional CLI review with
  explanations, auditor/time, rejection/supersession audit, and manual-match conflict
  protection.
- Bounded worker scheduling, stale-job recovery, correlated secret-safe JSON lifecycle
  events from discovery through apply/replay/rebuild and worker shutdown, collection
  freshness metrics, and separate Prometheus/health endpoints for API and
  continuous-worker processes.
- Durable source collection attempts record safe errors, discovery/processing/
  warning counts, truncation, and committed publisher checkpoints; source status
  exposes the latest attempt beside latest and last-good file state.
- Dependency-free Parquet writer and immutable partitioned exports for current prices
  and price history, independently verified with DuckDB.
- Deterministic container build and Compose deployment with PostgreSQL, SeaweedFS,
  demo data, optional Prometheus, and backup/restore tooling.
- Layered unit, parser/source contract, integration, live-smoke, and synthetic
  benchmark foundations.
- A local/CI repository secret scanner with built-in detection self-checks.

### Security

- The final release security review's eleven sealed findings now fail closed at their
  actual allocation or mutation boundaries. Destructive test and benchmark targets
  require exact loopback database names and confirmations; container smoke discards
  ambient Compose/S3 selectors; ZIP accepts only STORE/DEFLATE; nested Stores context
  is lexical and every nested SubChain declares its own identifier; and staging counts
  promotion relationship rows plus a conservative
  retained-memory charge before admitting each event.
- Public fuzzy-store searches use distinct first/cursor SQL shapes and a 10,001-row
  indexed candidate window. Promotion expansion decorates one returned parent and
  caps its ordered item/store/club collections at seven each (21 total). HTTP and MCP both preflight their complete
  compact UTF-8 JSON representations against a 1 MiB ceiling.
- Raw-archive and PostgreSQL backup creation enforce object, aggregate, metadata,
  manifest, and free-space ceilings before writes. Archive manifests stop listing
  prospectively at their exact canonical 16 MiB ceiling; restore uploads a verified
  snapshot through deterministic, conditionally published, version-aware staging
  and removes ambiguous staging objects. PostgreSQL dump validation streams the
  bounded host file directly to `pg_restore --list` and capacity-guards both
  sidecars.
- Parquet export validates and normalizes each row before retention, splits before
  page/file/working-set ceilings, and rejects one oversized row before PLAIN
  encoding. Container-image inventory is bidirectional across Dockerfile frontend,
  stages, external build sources, Compose build contexts and images; unsupported
  flow/override/ARG forms fail closed, and the local application image is rebuilt
  under `pull_policy: build` rather than selected by ambient image configuration.
- Active production S3 connections now require HTTPS before boto3 client creation.
  The only plaintext exception requires explicit
  `MAKOLET_S3_ALLOW_INSECURE_LOCAL=true` and a literal loopback IP with path-style
  addressing; DNS labels cannot authorize a remote endpoint or
  replace the approved authority with a bucket-prefixed virtual host. Runtime
  composition, the demo seed, and raw-archive backup/restore share the same
  fail-closed policy, while development and test transports retain their existing
  compatibility.
- Every production runtime, collection-lock, demo-seed, and online-migration
  PostgreSQL engine now requires authenticated, hostname-verified `verify-full` TLS
  for remote hosts. The explicit plaintext exception is limited to literal loopback
  authority hosts and rejects host/service overrides;
  centralized asyncpg construction supplies the verified SSL context or explicit
  trusted-local `ssl=False` consistently.
- Raw-archive backup manifests now require a versioned, domain-separated HMAC made
  with a distinct protected 256-bit key outside the backup tree. Verification and
  restore authenticate the exact canonical manifest before parsing or trusting its
  inventory, while manifest, checksum, authentication sidecar, and key reads are
  no-follow regular-file reads with strict declared-size and `limit + 1` bounds.
  POSIX wrappers retain the invoking non-root identity so owner-only host keys and
  backup directories remain usable; Windows materializes the exact digest-bound
  read-only bind into a private `0600` tmpfs key without weakening core validation.
- Static-daily embedded `const files` catalogs now receive the shared byte, depth,
  structural-token, decoded-string, and scalar preflight before JSON materialization;
  decoder recursion is categorized and inert trailing publisher JavaScript remains
  supported.
- Zstandard decompression now caps decoder history at 128 MiB before processing
  frame output. This closes the gap where a tiny allowlisted `.zst` frame could
  request the format's 2 GiB history window ahead of decompressed-byte and expansion
  checks; focused regressions cover the real large-window header and legitimate
  frames.
- Source collection now reserves charged bytes before network I/O and enforces
  archive-plus-failed/retried-transfer ceilings per attempt and per exact rolling
  24-hour retailer window. Composite attempt/source settlements support multiple
  files, crashes retain conservative reservations, and archive attachments remain
  idempotent by source identity. Categorized checkpoint truncation, a narrowed
  transport cap with bounded 64-KiB final-frame reservation headroom, and FTP-download,
  local-archive, S3-spool, parser-spool, and backup-copy free-space reserves. Writers
  sharing a root now serialize each check, flushed bounded chunk, and post-write
  check through a cross-process advisory lock, closing the concurrent check/write
  race. Rejected immutable evidence counts toward the quota;
  HTTP and FTP length evidence is checked at stream EOF before the content-addressed
  archive commit, so publisher length mismatches cannot create uncharged objects.
  Legacy attempts expose unknown charged bytes as null, and the migration's indexed,
  per-timestamp backfill branches remain bounded before their global merge.
- PostgreSQL backups now carry a versioned HMAC-SHA-256 authentication sidecar made
  with a separately stored, protected 256-bit key. Bash and PowerShell restore only
  an authenticated private copy, before any `pg_restore`, while retaining the plain
  SHA-256 corruption check and isolated staging-before-swap recovery semantics.
  Authentication streaming also enforces operator-configurable dump-byte and host
  free-space limits before retaining the verified copy, and its checksum sidecar is
  parsed as one bounded regular-file line instead of by unbounded shell readers.
- Structured stdlib/HTTPX logs now redact secret query values even when a query key
  uses mixed case or percent encoding. Raw safe values remain unchanged, while
  malformed key escapes fail closed; the optional Prometheus profile no longer
  enables unauthenticated HTTP lifecycle reload or shutdown handlers.
- Production settings now reject bundled development database/S3 credentials;
  Compose requires explicit secret substitutions and keeps PostgreSQL and S3 host
  ports on loopback when the API/monitoring bind is widened.
- Source filenames now enforce decoded-basename and 255-byte/character boundaries
  and reject C0, C1, surrogate, bidirectional-control, and line/paragraph separator
  characters. Remote metadata also bounds stable IDs and URLs and rejects URL
  scheme/network-protocol mismatch, user information, or a missing network host.
  Human CLI and JSON output visibly encode terminal/control characters returned by
  persisted data without hiding ordinary Hebrew text.
- Structured JSON logging escapes C0/C1 controls, surrogates, Unicode format
  controls, and line/paragraph separators so one event cannot create multiple
  physical log lines.
- Added SSRF allowlists and public-address checks, redirect and response limits, XML
  DTD/entity rejection, archive traversal/bomb defenses, parameterized queries,
  bounded API/MCP inputs, lifecycle-field allowlisting, structured-log secret and
  control-character redaction, and traceback suppression.
