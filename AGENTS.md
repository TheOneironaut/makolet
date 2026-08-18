# Makolet agent guide

## Purpose

Makolet is a self-hosted, open-source platform for collecting, immutably archiving,
normalizing, querying, and replaying Israeli supermarket price-transparency data.
Correctness, provenance, bounded resource use, and honest source coverage are core
product requirements.

## Repository map

- `src/makolet/domain/`: framework-free domain types and invariants.
- `src/makolet/application/`: use cases and storage/source ports.
- `src/makolet/adapters/`: network, parser, archive, and database implementations.
- `src/makolet/interfaces/`: CLI, HTTP API, MCP, and worker entry points.
- `migrations/`: append-only Alembic migrations; see its scoped guide.
- `tests/`: unit, contract, integration, end-to-end, and live suites.
- `tests/fixtures/`: small legal or independently authored source documents.
- `benchmarks/`: deterministic generators and measured scenarios.
- `docs/`: implemented architecture, operations, interfaces, research, and ADRs.
- `.agent/execplans/`: living plans for substantial multi-file work.

## Canonical commands

```text
uv python install 3.14.7
uv sync --all-groups --frozen
uv run ruff format --check .
uv run ruff check .
uv run mypy src tests benchmarks
uv run pytest
uv run makolet database migrate
uv run makolet database status
uv run makolet doctor
uv run makolet benchmark run --quick
uv run makolet benchmark run --standard
uv run python scripts/check_secrets.py
uv run python scripts/check_docs.py
docker compose up --build
```

Use `uv run makolet --help` for the discoverable runtime command surface. CI must
call these same local commands rather than hide setup in workflow-only scripts. See
`docs/testing.md` for real-service, live-source, benchmark, license, and SBOM gates.

## Dependency and architecture rules

Dependencies point inward: interfaces and adapters may depend on application and
domain modules; application may depend on domain and declared ports; domain code
must not import database, HTTP, CLI, MCP, worker, or source frameworks. API, CLI,
MCP, and worker entry points reuse application services. Prefer frozen domain
values and explicit ports over globals, deep inheritance, or generic helper bins.

Downloading never parses business records. Parsers never perform network access.
Shared normalizers never import retailer-specific code. Add a source-specific quirk
under `adapters/sources/<family>/` only after a real response or legal fixture proves
the variation exists.

## Adding a portal or retailer

1. Record primary-source evidence and license/clean-room notes in `docs/research/`.
2. Add or extend a portal-family adapter; use retailer configuration for identifiers
   and endpoints when behavior is otherwise identical.
3. Add minimal legal fixtures and parser/source contract tests, including malformed
   and truncated cases.
4. Add the retailer to `docs/source-coverage.md` with an honest live verification.
5. Run offline checks and the explicitly selected, low-volume live smoke test.
6. Update adapter, operations, and coverage documentation in the same change.

Never bypass authentication, CAPTCHAs, access controls, rate limits, or anti-bot
measures. Public credentials may be configured only when the publisher explicitly
intends them for public price-transparency access; never log credentials.

## Database and correctness invariants

- Add a new migration; never edit an applied migration. Test upgrade from empty and
  downgrade only where the migration explicitly supports it.
- Retailer items and canonical products are different entities. Source identifiers
  and GTINs are not database primary keys.
- Money is fixed-precision decimal. Preserve source values and provenance.
- Archive exact bytes before parsing; raw objects are immutable and hash-addressed.
- A source file is idempotent by stable source identity and content hash.
- Unchanged observations do not create new history events.
- Apply deltas only to mentioned records. Reconcile absence only after a complete,
  sufficiently large, fully validated snapshot commits atomically.
- Failed, truncated, suspiciously small, or error-page downloads never become a
  successful empty ingestion.
- Every public query has a deterministic order and enforced result bound.

## Clean-room, licensing, security, and fixtures

Original code is Apache-2.0. Product and build dependencies must be OSI-approved
and Apache-compatible. The owner has approved only these narrow adjacent cases:
unmodified MPL-2.0 artifacts with their notice, source-availability, and
covered-file obligations; Ruff solely as a non-redistributed external development
and CI executable; and separately licensed base-OS or service-image components,
including GPL/LGPL programs and libraries, when they remain outside Makolet's
Apache-2.0 source, are fully inventoried, and their distribution obligations are
met. Any other exception requires explicit owner approval. Update
`THIRD_PARTY_NOTICES.md` after dependency or image changes. Restricted,
non-commercial, source-available, or unlicensed projects may inform observable
behavior only; do not copy, adapt, translate, or derive their code.

Treat listings, filenames, archives, and XML as hostile. Preserve SSRF allowlists,
redirect bounds, timeouts, archive limits, XML entity protections, parameterized SQL,
query limits, and secret redaction. Do not commit retailer dumps: fixtures must be
minimal, legal, scrubbed, and documented with origin/license or authored clean-room.

## Documentation and definition of done

Documentation describes verified implementation, not aspirations. Update relevant
docs, coverage, ADRs, runbooks, changelog, and the active ExecPlan alongside code.
A change is done only when formatting, linting, typing, relevant tests, migrations,
documented commands, security/license checks, and behavior-level verification pass;
core paths may not end as stubs or TODOs.

Use an ExecPlan following `.agent/PLANS.md` for substantial multi-file work. Keep the
plan current with progress, decisions, discoveries, measurements, evidence, and
recovery steps before handing work to another agent.
