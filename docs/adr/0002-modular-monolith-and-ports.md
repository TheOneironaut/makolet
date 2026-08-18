# ADR 0002: Modular monolith with explicit ports

- Status: Accepted
- Date: 2026-08-11

## Context

Collection, parsing, normalization, storage, query, HTTP, CLI, MCP, and worker behavior
must evolve together without becoming a giant scraper class or a set of unrelated
scripts. No measured scale requirement currently justifies distributed messaging,
microservices, or a workflow engine.

## Decision

Deploy one modular application with four inward-pointing layers:

1. `domain`: frozen values, entities, policies, state transitions, and errors; no
   framework imports.
2. `application`: ingestion/query use cases and narrow protocols for sources,
   archives, transactions, clocks, locks, and repositories.
3. `adapters`: portal families, secure parsers, local/S3 archive, and PostgreSQL.
4. `interfaces`: CLI, HTTP, MCP, and worker composition roots.

Downloading and parsing remain separate application steps. Parsing accepts an
archived byte stream and never performs network access. Portal-family adapters own
discovery behavior; retailer configuration supplies identifiers/endpoints where the
behavior is identical. API, CLI, MCP, and worker entry points call the same services.

Use composition and structural protocols, not an inheritance tree rooted in a base
scraper. Add an abstraction only after two observed sources demonstrate the variation
or a port is needed to test a correctness boundary.

## Consequences

- One deployable image and database simplify local operation and atomic ingestion.
- Adapters can be replaced by fakes in tests without global monkeypatching.
- Source expansion remains configuration-heavy only when behavior is truly shared.
- Kafka, Redis, Elasticsearch, ClickHouse, Kubernetes, and workflow engines are not
  dependencies. A future addition requires a measured bottleneck/reliability need and
  a new ADR.
