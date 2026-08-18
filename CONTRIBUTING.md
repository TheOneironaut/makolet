# Contributing to Makolet

Thank you for helping maintain Makolet. Correct source provenance and safe behavior
matter as much as feature breadth: do not turn an external failure into apparently
valid empty data, and do not claim retailer support that was not verified.

## Development setup

Install the pinned runtime and locked dependency set:

```text
uv python install 3.14.7
uv sync --all-groups --frozen
uv run makolet --help
```

For database or object-storage work, use the open-source local services documented in
[deployment](docs/deployment.md). Never substitute SQLite for PostgreSQL behavior in
integration tests.

## Before changing code

- Read [AGENTS.md](AGENTS.md), the relevant nested guide, and the applicable ADRs.
- For substantial multi-file work, create or update an ExecPlan using
  [.agent/PLANS.md](.agent/PLANS.md).
- Check `git status`; preserve unrelated work in a shared or dirty checkout.
- For source work, update primary-source evidence and the clean-room/license ledger
  before implementing a new variation.

Dependencies point inward: `domain` has no framework dependencies, `application`
depends on domain and explicit ports, and adapters/interfaces depend on both. Network
discovery, exact-byte download, archive, parse, normalize, apply, and query are
separate responsibilities.

## Required checks

Run the narrow tests while iterating, then the applicable repository checks:

```text
uv run ruff format .
uv run ruff format --check .
uv run ruff check .
uv run mypy src tests benchmarks
uv run pytest --no-cov -m "not integration and not live and not benchmark"
```

Changes to persistence, archive, API, CLI, or MCP also require the real-service and
behavior tests described in [docs/testing.md](docs/testing.md). Dependency changes
require regeneration and verification of the lock, notices, license inventory, and
CycloneDX SBOM; use the exact commands in that guide.

## Source adapters and fixtures

Follow [the source-adapter guide](docs/source-adapter-guide.md). In particular:

1. Record official/public evidence and the exact verification date.
2. Reuse a portal-family adapter when behavior is the same; configuration is not a
   reason to duplicate code.
3. Keep credentials outside source, URLs, fixtures, test output, and logs.
4. Add independently authored or clearly licensed minimal fixtures, including failure
   cases. Never commit retailer dumps.
5. Run a single explicitly selected low-volume live smoke only when legal and useful.
6. Update `docs/source-coverage.md` without converting a fixture pass into a live pass.

Restricted, non-commercial, source-available, and unlicensed repositories may only
inform publicly observable behavior. Do not copy, adapt, translate, or derive their
code, tests, fixtures, or prose.

## Database changes

Add a successor migration; never edit an applied migration. The frozen DDL snapshot
under `migrations/versions/` must not import mutable application metadata. Verify an
upgrade from an empty PostgreSQL database and only promise downgrade support when the
migration explicitly implements it. Preserve UUID internal identities, decimal money,
source provenance, current/history consistency, and full-versus-delta semantics.

## Pull request checklist

- The change has a focused purpose and no unrelated generated/cache files.
- Public and expensive queries remain bounded and deterministically ordered.
- Hostile-input and secret-redaction boundaries are preserved.
- Tests cover behavior and failure paths, not just framework wiring.
- Documentation and the active ExecPlan describe what was actually verified.
- New product/build dependencies have compatible OSI-approved licenses and updated
  notices/SBOM; any use of the repository's narrow owner-approved external-tool or
  platform-component cases remains within their documented boundaries.
- No credentials, retailer dumps, personal data, or expiring signed URLs are added.
- `CHANGELOG.md` contains a concise user-visible entry when appropriate.

Security reports must follow the repository's `SECURITY.md`; do not open a public
issue for a suspected vulnerability.
