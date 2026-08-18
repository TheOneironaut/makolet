# Security Policy

## Supported Versions

Makolet has not yet published a stable release. Until the first release, only
the current `main` branch receives security fixes.

After the first release, security fixes are provided for the current `main`
branch and the latest published release. Older releases are unsupported unless
a release notice explicitly states otherwise.

## Reporting a Vulnerability

Report suspected vulnerabilities privately by email to
[amitibr19@gmail.com](mailto:amitibr19@gmail.com).

Do not open a public issue, discussion, or pull request for a suspected
vulnerability before coordinated disclosure.

Include, when available:

- the affected revision or release;
- the deployment mode and exposed interface;
- the security impact and realistic preconditions;
- minimal reproduction steps or a proof of concept;
- relevant logs with credentials, signed URLs, retailer data, and personal data
  removed; and
- any suggested remediation.

Do not send credentials, private keys, raw retailer files, database dumps, or
other sensitive production data by email. Send an initial description and
coordinate a safer transfer method if additional evidence is required.

## System and Scope

Makolet is a self-hosted platform for discovering, downloading, immutably
archiving, normalizing, querying, exporting, and replaying Israeli supermarket
price-transparency data.

This policy covers:

- domain and application code under `src/makolet`;
- publisher-source adapters, listing clients, downloaders, parsers, and archive
  implementations;
- PostgreSQL schema, migrations, ingestion, history, matching, maintenance, and
  public-query repositories;
- the CLI, read-only HTTP API, read-only MCP server, worker, scheduler, metrics,
  and export paths;
- container, Compose, backup, restore, demo-seed, and deployment tooling;
- security-, license-, SBOM-, and CI-verification scripts; and
- the packaged wheel, source distribution, and optional benchmark command.

The HTTP API and MCP server are intentionally unauthenticated and read-only.
They bind to loopback by default and require an operator-managed TLS and
authentication boundary before exposure to an untrusted network. Loopback
reachability does not make their inputs trusted.

## Threat Model and Trust Boundaries

Treat the following as attacker-controlled or potentially hostile:

- publisher listings, URLs, redirects, filenames, timestamps, response headers,
  compressed archives, XML, JSON, and every decoded business field;
- API query parameters, cursors, Host headers, and request bodies;
- MCP arguments, origins, and transport bodies;
- database and raw-archive backup sets obtained from recovery storage;
- S3-compatible service responses and object metadata;
- filenames and scalar values rendered in logs or operator output; and
- deployment configuration supplied outside the reviewed local defaults.

Important protected assets include:

- the exact immutable raw publisher bytes and their content hashes;
- source identity, portal, retailer, chronology, and provenance evidence;
- normalized current state and append-only validity history;
- database, S3, source, and backup-authentication credentials;
- reviewed catalog matches and maintenance checkpoints;
- backup and restore integrity; and
- service and host resource availability.

The self-hosting operator and local administrative CLI caller are trusted to
perform the operations they explicitly request. That trust does not exclude
confused-deputy behavior, privilege expansion, unsafe default exposure, or an
untrusted input escaping an operator-approved boundary from being reportable.

## Security Invariants

The following properties must hold:

1. Publisher bytes are archived exactly before business parsing. Raw objects
   remain immutable and content-addressed, and replay performs no publisher
   network access.
2. Publisher-source traffic obeys source-specific scheme, host, port, redirect,
   DNS, public-address, timeout, and byte allowlists. Source-controlled input
   must not provide SSRF access to local, private, link-local, or metadata
   services.
3. Compression and parser work is bounded before materialization by byte,
   expansion-ratio, member, depth, token, string, record, and filesystem
   ceilings. Truncated, malformed, suspiciously small, error-page, or
   structurally incomplete input fails closed.
4. Deltas affect only mentioned records. Absence reconciliation is
   portal-scoped and occurs only after a complete, sufficiently large, fully
   validated snapshot commits atomically. Truncated, partial, or error-bearing
   snapshots never become successful empty snapshots.
5. Stable source identity, content hashes, chronology, provenance, and reviewed
   matching decisions cannot be silently rewritten by rediscovery, replay, or
   maintenance.
6. SQL values remain parameterized. Every public query has deterministic
   ordering, bounded output, validated cursors, and bounded database work
   proportional to the documented request ceiling.
7. The HTTP API and MCP server expose no ingestion, replay, maintenance,
   filesystem, arbitrary SQL, or other mutation capability. Unexpected errors
   remain generic, correlated, and secret-safe.
8. Credentials, signed query values, authentication material, control
   characters, and hostile directionality characters are redacted or escaped
   in logs and human-readable output.
9. Remote production PostgreSQL and S3 connections require authenticated,
   hostname-verifying TLS. Plaintext local exceptions are explicit,
   host-scoped, and cannot authorize a remote or virtual-hosted endpoint.
10. FTPS requires verified TLS. Plain FTP remains disabled unless the operator
    explicitly accepts its unauthenticated transport risk.
11. Download, archive, parser-spool, backup-copy, run, and rolling-source
    resource budgets fail closed. Failed and retried transfer bytes are
    accounted for, and cooperating Makolet filesystem writers coordinate their
    free-space checks and writes.
12. Database and archive restore authenticate bounded metadata before parsing
    or applying it. Authentication keys remain separate from backup storage,
    and restore does not expose unverified data to PostgreSQL or S3.
13. The global normalized rebuild requires exact operator confirmation, durable
    checkpoints, a maintenance barrier, preservation of curated state, and
    exact reconciliation before completion.
14. Retailer and portal identities remain isolated throughout discovery,
    ingestion, matching, reconciliation, querying, and provenance reporting.
15. Production containers retain their documented non-root identity,
    read-only filesystem, dropped capabilities, resource limits, and private
    state-service binds.

## Reportable Findings and Severity Context

A finding is reportable when it can realistically violate an invariant in a
supported configuration or bypass a documented boundary. Examples include:

- remote code execution, SQL injection, or unintended mutation;
- SSRF, DNS-rebinding, redirect, TLS, or credential-boundary bypasses;
- raw-archive, provenance, history, matching, backup, or restore corruption;
- secret, signed-URL, sensitive metadata, or raw-source disclosure;
- portal or retailer scope confusion;
- parser, decompression, query, transfer, or storage amplification that bypasses
  an enforced ceiling;
- unauthenticated access to an administrative operation;
- unsafe production defaults that expose state services or bundled credentials;
  and
- a package, migration, container, or recovery path that defeats a documented
  security control.

Default loopback binding, read-only intent, a statement timeout, or a nominal
result limit is not by itself sufficient reason to dismiss a reachable
amplification or confused-deputy finding.

Assess severity using realistic reachability and impact. Code execution,
credential compromise, private-network access, destructive reconciliation, or
trusted-backup substitution generally warrants higher severity than bounded
metadata exposure or defense-in-depth hardening.

## Out of Scope and Accepted Risk

The following are not vulnerabilities by themselves:

- the absence of built-in user authentication or TLS on the intentionally
  read-only API and MCP transports, when they remain private or are protected by
  the documented operator-managed boundary;
- transport substitution inherent to plain FTP after the operator explicitly
  enables the insecure-FTP exception;
- the absence of application-level encryption at rest when database, archive,
  backup, and host storage are protected by operator-managed volume or platform
  encryption;
- vulnerabilities solely in an operator-managed reverse proxy, operating
  system, PostgreSQL, SeaweedFS, or publisher service, unless Makolet exposes,
  misconfigures, or unsafely composes the affected component; and
- resource consumption caused only by a trusted administrator deliberately
  replacing reviewed limits with unsafe values, unless untrusted input can use
  that configuration to escape another Makolet boundary.

These exclusions do not suppress adjacent bypasses. For example, enabling plain
FTP does not excuse credential logging, private-network SSRF, or a failure to
honor the explicit opt-in.

## Known Limitations and Compensating Controls

- The read-only API, MCP transport, and metrics endpoints rely on loopback or an
  operator-managed reverse proxy for authentication and TLS.
- Plain FTP cannot authenticate the publisher or protect transport integrity;
  it is disabled by default and requires explicit configuration.
- Makolet does not provide application-level encryption at rest. Operators must
  protect PostgreSQL, archive, backup, and authentication-key storage.
- Backup HMACs provide authenticity and integrity, not confidentiality or
  freshness. Without an external recovery checkpoint, an older legitimately
  authenticated recovery set can be replayed.
- Filesystem-capacity locks coordinate cooperating Makolet writers. Unrelated
  host processes require volume isolation and host-level quotas.
- Windows backup-key confidentiality depends on operator-managed ACLs in
  addition to the wrapper's bounded, digest-bound staging checks.
- Live publisher availability, legality, schemas, and rate limits are external
  conditions. Makolet's bounded live checks do not authorize bypassing access
  controls or publisher restrictions.
