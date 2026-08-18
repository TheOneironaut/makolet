# HTTP API

Makolet exposes a versioned, read-only FastAPI interface over the same `QueryService`
used by CLI and MCP. It has no public ingestion or administrative mutation routes.

Start it directly:

```text
uv run makolet api serve
```

Defaults are `127.0.0.1:8000`; override with `--host`/`--port` or
`MAKOLET_API_HOST`/`MAKOLET_API_PORT`. Interactive OpenAPI is at `/docs`, ReDoc at
`/redoc`, and the schema at `/openapi.json`. The server admits at most
`MAKOLET_API_HTTP_MAXIMUM_CONCURRENCY` concurrent connections/tasks (100 by default,
validated from 1 through 10,000); Uvicorn rejects excess work with `503` before it
reaches the application or database pool.

## Health and metrics

| Method and path | Result |
|---|---|
| `GET /healthz` | Process liveness: `{"status":"ok"}`. It does not prove database readiness. |
| `GET /readyz` | `200 {"status":"ready"}` only when PostgreSQL is reachable and every current Alembic revision exactly matches the repository head set; otherwise `503 {"status":"not_ready"}`. |
| `GET /metrics` | Prometheus text exposition. It is deliberately omitted from OpenAPI. |

Every HTTP response receives `X-Request-ID`. A caller-supplied `X-Request-ID` is
accepted only when it is 1–128 ASCII letters/digits or `._:-`; otherwise the server
generates a UUID. The same value is bound as the structured-log correlation ID.

## Version 1 routes

| Method and path | Query parameters | Order / purpose |
|---|---|---|
| `GET /api/v1/retailers` | `limit`, `cursor` | Retailers by UUID. |
| `GET /api/v1/stores` | `query`, `retailer_id`, `city`, `limit`, `cursor` | Stores by UUID; normalized name and exact normalized city filters. |
| `GET /api/v1/products/search` | required `query`, `limit`, `cursor` | Validated exact barcode/name rank plus exact normalized quantity/unit evidence, descending then UUID. |
| `GET /api/v1/barcodes/{barcode}` | none | One active canonical product for an exact validated numeric identifier. |
| `GET /api/v1/retailer-items/lookup` | required `retailer_id`, optional `portal_id`, required `item_code` | One active canonical product for an exact normalized item code. A retailer-wide collision across portals returns a structured ambiguity error until `portal_id` is supplied. |
| `GET /api/v1/products/{product_id}` | none | Canonical product and at most 200 deterministically ordered identifiers. |
| `GET /api/v1/products/{product_id}/prices` | `retailer_id`, `store_id`, `limit`, `cursor` | Current matched prices, cheapest first then UUID. |
| `GET /api/v1/products/{product_id}/compare` | `retailer_id`, `limit`, `cursor` | Current cross-store comparison (same ordered read as prices without a store filter). |
| `GET /api/v1/products/{product_id}/history` | `store_id`, `since`, `until`, `limit`, `cursor` | Validity rows overlapping the resolved half-open window, newest first. |
| `GET /api/v1/products/{product_id}/availability` | `store_id`, `limit`, `cursor` | Current matched availability by UUID. |
| `GET /api/v1/promotions` | `product_id`, `store_id`, `at`, `limit`, `cursor` | Promotion versions active at `at` (UTC now when omitted). |
| `GET /api/v1/promotions/history` | `product_id`, `store_id`, `since`, `until`, `limit`, `cursor` | Observed promotion versions overlapping the half-open interval, newest first. |
| `GET /api/v1/freshness` | `limit`, `cursor` | Per-store globally latest observation and capped available/observed item-count lower bounds. |
| `GET /api/v1/source-status` | `limit`, `cursor` | Latest and last-good source-file state plus the latest durable collection attempt, charged bytes (archive plus failed/retried transfers), and categorized truncation per portal. |
| `GET /api/v1/status` | `limit`, `cursor` | Maintenance/rebuild state and a page of latest source-file state per portal. |

UUID parameters use canonical UUID parsing. Timestamps must be ISO 8601 with a time
zone, for example `2026-08-11T12:00:00Z`. Search inputs are normalized with Unicode
NFKC, punctuation/whitespace folding, and have a 200-character application limit.
The first explicit English or Hebrew amount/unit expression is removed from the text
term and compared as a fixed-precision base quantity (`kg`/`g`, `L`/`ml`, each, or
metres). For example, `1 L` equals `1000 ml`; numeric substrings never count as a
quantity match. A quantity-only query is rejected because searchable product text is
still required.
Retailer item codes are NFKC-normalized, have whitespace removed, and are limited to
128 characters. They are never resolved without the explicit retailer UUID. Because
one legal retailer can publish independent feeds, a code that matches more than one
portal returns `domain_validation_error`; repeat the lookup with the `portal_id`
reported by product/store/source results. The server never silently chooses the first
portal match.

## Pagination and bounds

Normal list routes default to 50 and accept `limit=1..200`. History accepts up to
1,000 rows and a maximum explicit span of ten 366-day years. `since` and `until`
must either both be present or both be absent; a one-sided range returns
`domain_validation_error`. When both are absent, the first page uses the exact
half-open interval `[request UTC time - 366 days, request UTC time)`. The effective
timestamps are carried inside the opaque cursor, so later pages do not drift as the
clock advances. A page is:

Those accepted request limits are unchanged, but they are ceilings rather than a
promise that one response will contain that many rows. Before PostgreSQL materializes
publisher-controlled text, the shared query service applies these narrower
route-specific candidate caps:

| Route | Maximum materialized parent rows per page |
|---|---:|
| `/api/v1/retailers` | 50 |
| `/api/v1/stores` | 5 |
| `/api/v1/products/search` | 5 |
| `/api/v1/products/{product_id}/prices` and `/compare` | 28 |
| `/api/v1/products/{product_id}/history` | 28 |
| `/api/v1/promotions` and `/api/v1/promotions/history` | 1 |
| `/api/v1/products/{product_id}/availability` | 28 |
| `/api/v1/freshness` | 50 |
| `/api/v1/source-status` and the source page inside `/api/v1/status` | 32 |

A request such as `limit=200` therefore remains valid even when the route returns
only its smaller materialization cap. If more ordered rows exist, that shorter page
has an ordinary non-null `next_cursor`; it is not a final page and the caller should
continue with that cursor. A requested limit below the route cap is honored as-is.

```json
{
  "items": [{"id": "77777777-7777-7777-7777-777777777777", "name": "Demo product"}],
  "next_cursor": null
}
```

`next_cursor` is null on the final page. Otherwise pass it back unchanged; it is an
opaque keyset cursor tied to the route's deterministic ordering. Do not synthesize a
cursor or reuse one for another route/filter set. The application wraps the database
position in a bounded, versioned envelope with a checksum and a fingerprint of the
route plus normalized filters. Corrupt, legacy, cross-route, or changed-filter
cursors return the same structured validation error before a repository query. For
active promotions with no explicit `at`, the envelope also preserves the first
page's UTC instant so later pages cannot drift as the clock advances. A cursor is a
pagination integrity mechanism, not an authorization token.

An exact normalized city lookup uses a stored normalized city value and a dedicated
`(city_search, id)` or `(retailer_id, city_search, id)` keyset index; it does not pass
through the nullable-filter store scan. Fuzzy store-name search evaluates similarity only inside an ID-ordered window of at
most 10,000 stores. Its first-page and cursor-page SQL are distinct; cursor pages use
an unconditional `store.id > cursor` index condition. A window with no matches may
therefore return an empty `items` array and a non-null `next_cursor`; follow that
cursor to continue the bounded search.

Promotion history applies its filters and keyset position to an ordered effective
page-size-plus-one ID candidate page before loading bounded item/store/club
relationships. The effective size is the smaller of the accepted request limit and
the route cap above.
Current prices, cross-store comparisons, current availability, and price history use
transactionally maintained canonical-product projection columns with filter/order
matching indexes. They select the exact ordered effective-page-size-plus-one
candidate IDs before
joining retailer, portal, store, source-file, archive, or availability decoration;
the projection is updated on state insertion and confirmed-match remapping, so this
is an exact resumable page rather than a partially complete fan-out.
Freshness similarly pages stores with current availability first. For each selected
store, it deterministically probes at most 1,001 rows to return counts capped at
`item_probe_limit = 1000` and to set `items_truncated`; a separate indexed top-one
lookup preserves the globally latest contributing observation even when the count
probe truncates. These query shapes bound relationship and current-state work before
serialization; the database statement timeout remains a separate fail-safe.

Decimal database values are JSON strings to preserve precision. UUIDs and datetimes
are strings. A source/store/product may legitimately return an empty `items` array;
that does not alter ingestion state.

Every route has its own closed OpenAPI response component. The schema names and
documents the concrete retailer, store, product, identifier, price, availability,
promotion, freshness, source-status, provenance, and maintenance fields rather than
an open-ended JSON object. Normal pages declare at most 200 items, history pages at
most 1,000, and each promotion child collection retains a compatibility ceiling of
200; the production materialization caps above and the seven-child promotion cap
below are stricter. Response mappings are
validated before serialization; missing, extra, or incorrectly typed service fields
fail as a generic request-correlated 500 instead of silently changing the public
contract. Decimal fields serialize as JSON strings, UUID fields use `format: uuid`,
and aware timestamps use `format: date-time`. After validation and before FastAPI
serializes a public query mapping, the server preflights its compact UTF-8 JSON size.
The body ceiling is 1 MiB, including the byte cost of Hebrew and JSON escaping; a
larger valid page returns a bounded 422 `query_limit_exceeded` error and must be
retried with a smaller page size.

Product detail probes at most 201 identifier rows before JSON aggregation. Up to 200
identifiers are returned unchanged; the 201st is an overflow sentinel and produces a
stable 422 `query_limit_exceeded` response instead of silently truncating or
materializing an unbounded identifier collection.

## Examples

After running the Compose demo seed from the [README](../README.md):

```powershell
$base = "http://127.0.0.1:8000"
Invoke-RestMethod "$base/api/v1/products/search?query=7290000000015&limit=10"
Invoke-RestMethod "$base/api/v1/barcodes/7290000000015"
Invoke-RestMethod "$base/api/v1/retailer-items/lookup?retailer_id=22222222-2222-2222-2222-222222222222&portal_id=33333333-3333-3333-3333-333333333333&item_code=SKU-1"
Invoke-RestMethod "$base/api/v1/products/77777777-7777-7777-7777-777777777777/prices"
```

An illustrative barcode response has the exact envelope and field types below (the
seed's product names are Hebrew and are omitted here only for readability):

```json
{
  "data": {
    "id": "77777777-7777-7777-7777-777777777777",
    "name": "<seeded Hebrew product name>",
    "brand": "<seeded Hebrew brand>",
    "manufacturer": "<seeded Hebrew manufacturer>",
    "quantity": "500.000000",
    "unit_of_measure": "<seeded unit>",
    "barcode": "7290000000015"
  }
}
```

Freshness rows identify the deterministic globally latest contributing availability
observation with `source_file_id`, document/source timestamps, portal identity, and
archive `content_sha256`. `available_items` and `observed_items` are capped at the
published `item_probe_limit` of 1,000. When `items_truncated` is true, those counts
are conservative lower bounds rather than exact store totals; freshness timestamp
and provenance still cover all current availability rows for the store.

Product identifiers include validation method/evidence plus issuer retailer and issuer
portal IDs/keys. Current-price rows include `source_file_id`, archive content hash,
source document/timestamps, price/discount/observation time, availability/status,
retailer-item code/name, portal ID/key, store UUID/name, and retailer UUID/key/name.
Price-history and availability rows carry the same source provenance; history also
includes `valid_from` and `valid_to`.

Promotion rows include every normalized condition/reward field, source file/archive
provenance, and deterministic `items`, `stores`, and `clubs` child collections.
The public service materializes at most one promotion parent per page. That parent
contains at most seven UUID/key-ordered items, seven UUID-ordered stores, and seven
ordered clubs: 21 children in total. The pagination sentinel never loads children.
`returned_item_count`,
`returned_store_count`, and `returned_club_count` describe the returned entries;
`items_truncated`, `stores_truncated`, and `clubs_truncated` explicitly report whether
at least one additional relation exists in the corresponding collection. Child
collections have no separate cursor; the flags make their intentional truncation
explicit while the parent promotion cursor continues only the ordered promotion
page. These per-collection and response-wide bounds ensure a
malformed publisher fan-out cannot make a response unbounded. Historical
queries return the relations and source evidence attached to each past version, not
the current version's children.

During an operator-started normalized rebuild, `/api/v1/status` returns
`maintenance.active=true`, the rebuild run/counters, and a warning that normalized
query results are partial. Read routes remain available; callers that require a
consistent complete snapshot must wait for maintenance to return to normal.

## Errors

Application/domain errors use:

```json
{
  "error": {
    "code": "not_found",
    "message": "Product was not found",
    "request_id": "24e35979-585c-4ea4-8070-1a29fc9bba2d"
  }
}
```

`not_found` maps to 404; domain/query validation to 422; retryable application errors
to 503; other application errors to 400. Malformed path/query values use the same
closed envelope with code `request_validation_error`, the generic message `Request
validation failed`, and no echoed input or framework validation detail. Version 1
has no public request-body routes; a future read route that accepts a body inherits
the same non-echoing handler. An unexpected `Exception` or invalid internal response
mapping becomes a generic `internal_server_error` response with status 500 and the
same request ID in the response header, body, and secret-safe error log. Internal
exception messages, response validation details, stack traces, credentials, and raw
request values are never returned or added to that log event. OpenAPI references
`ErrorResponse` for every documented 400/404/422/500/503 application response.

## Exposure guidance

The server binds to loopback by default and provides no authentication or TLS. Keep it
private or put it behind a separately managed reverse proxy/authentication boundary.
Any proxy-side concurrency policy must be no weaker than the configured application
cap; rate limiting remains a separate deployment-edge responsibility.
Do not publish `/metrics` or detailed source-status/errors to an untrusted network
without evaluating the metadata exposure. Configure process/database/query bounds in
the [operations runbook](operations-runbook.md), not through public request input.
