# ADR 0005: Database-native product search first

- Status: Accepted
- Date: 2026-08-11

## Context

Product queries need Hebrew, English, mixed-script text, punctuation and whitespace
variation, quantity/unit forms, exact barcode lookup, pagination, and stable ordering.
A separate search service would add operational state and synchronization failure
without current benchmark evidence.

## Decision

Normalize searchable text deterministically with Unicode NFKC, case folding where
applicable, punctuation-to-space mapping, whitespace collapse, and documented unit
aliases. Preserve original text. Resolve exact validated barcodes and scoped retailer
item codes before text search.

Extract the first explicit quantity expression from a product query before text
ranking. Normalize kilograms/grams to grams, litres/millilitres to millilitres, and
the documented Hebrew and English aliases to the same base unit. Compare that
structured decimal value to normalized retailer-item quantity evidence; never use
substring matching (`1` must not match `10`). The remaining product text still drives
the ordinary prefix/trigram rank, so equivalent forms such as `1 L` and `1000 ml`
share the same text candidates.

Use PostgreSQL expression/generated columns with B-tree and `pg_trgm` GIN indexes.
Combine exact, prefix, token, and trigram signals into an explainable deterministic
rank, followed by stable internal identifier ordering. Every query has maximum input,
result, offset/cursor, store, retailer, and date-range bounds.

Do not use fuzzy text rank to merge canonical products. Search ranking and product
matching are separate concerns; matching creates reviewable candidates unless an
exact deterministic identity rule applies.

## Consequences

- One transactional store provides current data and search freshness.
- Hebrew behavior receives seeded integration tests and measured query plans.
- Elasticsearch/OpenSearch or another engine may be added only if the ten-million-row
  benchmark misses a recorded latency/reliability target and PostgreSQL tuning cannot
  meet it.
