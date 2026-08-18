# Makolet

Makolet is a self-hosted, Apache-2.0 data platform for Israel's supermarket
price-transparency publications. It discovers public retailer files, archives the
exact bytes immutably, parses and normalizes stores, items, prices, availability, and
promotions, retains change history in PostgreSQL, and exposes the same read model
through a CLI, HTTP API, and MCP server.

The project is a pre-release backend platform, not a consumer shopping application.
Retailer support is reported row by row in the
[source coverage matrix](docs/source-coverage.md); a configured adapter or fixture is
not presented as a currently healthy publisher.

## What is implemented

- Clean-room discovery adapters for the observed BINA, Matrix/Laib, NCR FTP/FTPS,
  static daily index, Hazi Hinam, City Market, and Shufersal portal families.
- Bounded HTTP and certificate-verified explicit-FTPS downloading with per-portal
  policies, socket-bound SSRF defenses, and total operation deadlines. Legacy plain
  FTP is fail-closed unless an operator explicitly accepts its authenticity risk.
- immutable SHA-256-addressed local and S3-compatible raw archives;
- streaming Stores, Price/PriceFull, and Promo/PromoFull XML parsing with UTF-8,
  Windows-1255, and UTF-16 handling plus hostile-input limits;
- transactional PostgreSQL staging, full-versus-delta apply semantics, current state,
  validity history, one-file/date-range replay, restartable archive rebuilds, leases,
  quarantine, portal-scoped source identity, and deterministic bounded queries with
  immutable source provenance;
- automatic isolated canonical representation for ordinary non-GTIN retailer items,
  plus bounded explainable match candidates and auditable CLI accept/reject review;
- one Typer CLI, a versioned read-only FastAPI API, and a bounded read-only MCP server,
  all reusing the same application-layer services for retailer/store discovery,
  product and barcode lookup, price comparison/history, promotion versions,
  availability, freshness, and source status;
- an in-process scheduled worker, Prometheus-format metrics, correlated secret-safe
  JSON lifecycle logs, and partitioned Parquet export;
- a Docker Compose development deployment using PostgreSQL 18 and SeaweedFS.

Research evidence and external blockers remain distinct from implementation status.
See [source coverage](docs/source-coverage.md) before relying on a particular retailer.

## Quick start with the clean-room demo

Requirements are Docker with Compose v2 and enough local resources for PostgreSQL and
SeaweedFS. The committed example credentials are development-only.

```powershell
Copy-Item .env.example .env
docker compose up --build -d postgres seaweedfs
docker compose run --rm migrate
docker compose --profile demo run --rm demo-seed
docker compose up -d api
```

Check the service and query the deterministic demo product:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/readyz
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/products/search?query=7290000000015"
```

The demo uses independently authored synthetic data, including canonical product ID
`77777777-7777-7777-7777-777777777777` and barcode `7290000000015`. It also writes a
small immutable object to SeaweedFS. Re-running the seed is idempotent.

The Compose stack's worker makes an immediate low-volume request for each enabled
public source. Review `MAKOLET_ENABLED_SOURCES` and
`MAKOLET_SOURCE_INTERVALS_SECONDS` in `.env` before starting `worker` or the complete
stack. Full deployment, health, backup, and restore procedures are in
[deployment](docs/deployment.md) and [backup and recovery](docs/backup-and-recovery.md).

## Host development

The locked toolchain is CPython 3.14.7 with `uv` 0.12.3. Start PostgreSQL and
SeaweedFS with Compose, then point host processes at their loopback ports:

```powershell
uv python install 3.14.7
uv sync --all-groups --frozen
$env:MAKOLET_DATABASE_URL = "postgresql://makolet:makolet-development-only-change-me@127.0.0.1:5432/makolet"
$env:MAKOLET_ARCHIVE_BACKEND = "local"
uv run makolet database migrate
uv run makolet doctor
uv run makolet api serve
```

Run `uv run makolet --help` for the complete command tree. The main references are:

- [documentation index](docs/index.md)
- [architecture](docs/architecture.md)
- [CLI](docs/cli.md), [HTTP API](docs/api.md), and [MCP](docs/mcp.md)
- [operations runbook](docs/operations-runbook.md)
- [observability](docs/observability.md)
- [testing](docs/testing.md) and [performance](docs/performance.md)
- [contributor guide](CONTRIBUTING.md)

## Quality checks

These are the same primary checks used by CI:

```text
uv sync --all-groups --frozen
uv run ruff format --check .
uv run ruff check .
uv run mypy src tests benchmarks
uv run pytest --no-cov -m "not integration and not live and not benchmark"
uv run python scripts/check_secrets.py
uv run python scripts/check_build_constraints.py
uv run python scripts/check_container_images.py
```

Integration tests require real PostgreSQL and S3-compatible services. Live publisher
tests are opt-in and never part of ordinary offline tests; see
[testing](docs/testing.md). That guide includes the exact frozen-export vulnerability,
license, three-SBOM, isolated-build, and container-runtime audit commands used by CI.

## License and data rights

Makolet's original software is licensed under [Apache-2.0](LICENSE). The software
license does not grant rights to retailer publications, product images, names,
trademarks, or external datasets. Raw retailer files are runtime inputs and are not
committed to this repository. Dependency notices are in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md), and the clean-room decisions are in
the [open-source review](docs/research/open-source-review.md).
