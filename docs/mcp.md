# MCP server

Makolet provides fourteen bounded, read-only tools over the same query service as its
HTTP API and CLI. It does not expose SQL, ingestion, replay, filesystem access, or
administrative mutation.

## Start a transport

Local stdio is the default:

```text
uv run makolet mcp serve
```

It reads one UTF-8 JSON-RPC object per input line and writes one compact JSON-RPC
response per line. Logs stay on stderr so stdout remains valid protocol data. One
message is limited to 1 MiB. Before decoding, each frame is also limited to 64 JSON
nesting levels, 100,000 structural tokens, 65,536 characters per string, and 4,096
characters per scalar. A malformed or structurally excessive frame returns `-32700`;
the stdio server remains available for the next line.

Example client process configuration:

```json
{
  "command": "uv",
  "args": ["run", "makolet", "mcp", "serve"],
  "cwd": "/absolute/path/to/makolet",
  "env": {
    "MAKOLET_DATABASE_URL": "<PostgreSQL URL>"
  }
}
```

Stateless HTTP is optional:

```text
uv run makolet mcp serve --transport http --host 127.0.0.1 --port 8001
```

The only endpoint is `POST /mcp`; GET and DELETE return 405. The server returns 202
for notifications and JSON for requests. It does not implement persistent sessions or
server-initiated SSE events.

## Tools

| Tool | Required arguments | Optional bounded arguments | Result |
|---|---|---|---|
| `search_products` | `query` (1–200 chars) | `limit` 1–200, `cursor` | Ranked canonical products with exact normalized English/Hebrew quantity-unit evidence. |
| `get_product` | `product_id` UUID | none | One canonical product and identifiers. |
| `find_product_by_barcode` | numeric `barcode` (1–32 digits) | none | One active product for an exact validated identifier. |
| `find_product_by_retailer_item_code` | `retailer_id` UUID, `item_code` (1–128 chars) | `portal_id` UUID | One active product for an exact normalized code; an unscoped cross-portal collision is an error. |
| `list_retailers` | none | `limit`, `cursor` | Persisted retailers. |
| `find_stores` | none | `query`, `retailer_id`, `city`, `limit`, `cursor` | Filtered stores. |
| `get_current_prices` | `product_id` | `retailer_id`, `store_id`, `limit`, `cursor` | Current matched store prices. |
| `compare_product_prices` | `product_id` | `retailer_id`, `limit`, `cursor` | Cheapest-first cross-store comparison. |
| `get_price_history` | `product_id` | `store_id`, aware `since`/`until`, `limit` 1–1,000, `cursor` | Newest-first price-validity rows overlapping an exact half-open window. `since` and `until` must be supplied together; when omitted, the first page uses `[request UTC time - 366 days, request UTC time)` and the cursor pins those bounds. Maximum explicit span is 3,660 days. |
| `get_active_promotions` | none | `product_id`, `store_id`, aware `at`, `limit`, `cursor` | Promotion versions active at the instant. |
| `get_promotion_history` | none | `product_id`, `store_id`, aware `since`/`until`, `limit` 1–1,000, `cursor` | Newest-first observed promotion versions overlapping the same paired-or-omitted, cursor-pinned half-open window contract. |
| `get_item_availability` | `product_id` | `store_id`, `limit`, `cursor` | Current store availability. |
| `get_data_freshness` | none | `limit`, `cursor` | Per-store globally latest observation/contributor plus counts capped at `item_probe_limit`; `items_truncated` marks lower-bound counts. |
| `get_source_status` | none | `limit`, `cursor` | Maintenance/rebuild state plus latest and last-good file state and the latest durable collection attempt, charged bytes (archive plus failed/retried transfers), and categorized truncation per portal. |

Every paginated read returns a bounded opaque cursor. Pass it back only to the same
tool with the same normalized filters. The versioned checksummed envelope rejects
changed-filter, cross-tool, malformed, and legacy cursor reuse as a structured
`domain_validation_error`; it is not an authorization credential.

Normal tool arguments continue to accept `limit=1..200`, and price/promotion history
continues to accept `limit=1..1,000`. Those values are request ceilings. Before
publisher-controlled text is materialized, the query service applies the following
parent-row caps:

| Tool | Maximum materialized parent rows per result page |
|---|---:|
| `list_retailers` | 50 |
| `find_stores` | 5 |
| `search_products` | 5 |
| `get_current_prices` and `compare_product_prices` | 28 |
| `get_price_history` | 28 |
| `get_active_promotions` and `get_promotion_history` | 1 |
| `get_item_availability` | 28 |
| `get_data_freshness` | 50 |
| `get_source_status` | 32 |

A valid larger request can therefore return a shorter page. When more ordered rows
exist, `nextCursor` is non-null and is the ordinary deterministic continuation; the
short page is not final. A requested value below the tool's cap is honored unchanged.

Price and promotion history first obtain an indexed overlap/relationship candidate
projection capped at 20,000 physical candidates. A scope above that ceiling fails
closed as `query_limit_exceeded`; lowering the requested response page does not bypass
the probe. This keeps sparse windows and arbitrary promotion instants bounded while
preserving the documented order and cursor semantics.

All argument models forbid unknown fields. Cursors are at most 512 characters and
must be passed back unchanged. Each `tools/list` entry advertises a distinct closed
`outputSchema` for that tool's structured success value or the closed structured
error value; there is no generic free-form object schema. Those schemas include
concrete UUID/date-time/decimal representations, page bounds (200 normally, 1,000
for history), source and archive provenance, maintenance/source-health fields, and
compatibility ceilings of 200 entries for promotion child collections. Production
uses the stricter parent caps above and the seven-entry child caps below. Tool annotations mark every tool
read-only, non-destructive, idempotent, and closed-world.

## Tool call result

A successful call returns both human-compatible text and the same structured object:

```json
{
  "content": [
    {
      "type": "text",
      "text": "{\"items\":[],\"nextCursor\":null}"
    }
  ],
  "structuredContent": {
    "items": [],
    "nextCursor": null
  },
  "isError": false
}
```

List tools return `items` and `nextCursor`; single-value tools return `data`. UUID,
aware datetime, and decimal values are serialized as strings. A domain/query failure
is a tool result with `isError=true` and a bounded safe
`structuredContent.error`; malformed protocol or arguments use JSON-RPC errors.
Unknown tool-argument names are rejected before field-model validation with one
bounded issue, and remaining typed validation diagnostics are capped at 16 before
their details are materialized. Thus a request within the 1 MiB frame ceiling cannot
turn a large set of unknown members into an attacker-sized diagnostic response.
The server validates service output against the advertised success model before it
builds text or structured content. Missing, extra, oversized, or incorrectly typed
service fields become the generic `internal_error` / `Tool execution failed` tool
result. Validation details, internal stack traces, unexpected values, and credentials
are not returned. A valid result is also preflighted with the same compact UTF-8 JSON
measurement as the HTTP API. Because MCP carries both JSON text and
`structuredContent`, the combined response must fit the 1 MiB message ceiling; an
oversized valid page becomes a bounded `query_limit_exceeded` tool result. Hebrew is
kept as UTF-8 and quotes, backslashes, and control characters are charged after JSON
escaping.

Retailer item lookup always requires the retailer UUID; item codes are not assumed to
be globally unique. When a retailer-wide code collides across portals the tool returns
`domain_validation_error`; pass the issuer `portal_id` to disambiguate. Product,
price, availability, and promotion results expose issuer/source portal and immutable
source provenance where relevant. Promotion children are deterministic and bounded
to seven items, seven stores, and seven clubs for the single materialized promotion
parent, for at most 21 children in one result page. `returned_*_count` reports the
entries actually returned, while the corresponding `*_truncated` flag is true when
additional relations exist. Children have no independent cursor; truncation is
explicit and the parent `nextCursor` continues only promotion versions. Historical
versions retain their
own child relations and source evidence. While a normalized rebuild is active,
`get_source_status`
includes `maintenance.active=true`, durable progress, and the partial-query warning.
Outside rebuilds it also distinguishes the latest file from the last successfully
completed file and reports the latest collection attempt's generation, bounded
counts, truncation state, and sanitized failure details.

Fuzzy `find_stores` evaluates at most 10,000 ID-ordered candidates per call. Cursor
pages use an unconditional indexed `store.id > cursor` predicate, and an empty
matching page can still carry `nextCursor` when another bounded candidate window
remains.

## Protocol negotiation

The protocol core supports current MCP `2026-07-28` plus legacy initialization for
`2025-11-25` and `2025-06-18`:

- legacy clients call `initialize`, then `tools/list`/`tools/call`;
- current clients use `server/discover` and include
  `_meta["io.modelcontextprotocol/protocolVersion"]="2026-07-28"` plus a client
  capabilities object in every modern request;
- discovery and tool-list results declare a five-minute public cache and
  `listChanged=false`.

Unknown methods return JSON-RPC `-32601`. Invalid requests/arguments use standard
`-32600`/`-32602`; current-version transport problems use `-32020` and unsupported
versions use `-32022`.

## HTTP requirements

Every HTTP request must use:

```text
Content-Type: application/json
Accept: application/json, text/event-stream
```

The dual Accept value follows Streamable HTTP negotiation even though this stateless
server currently returns JSON only. Bodies over 1 MiB return 413; wrong content type
returns 415; incomplete Accept returns 406. Bodies within the byte ceiling still pass
the same bounded JSON preflight as stdio, so decoder depth or numeric-conversion
failures become a JSON-RPC `-32700` response rather than an HTTP 500.

The transport rejects an invalid or negative `Content-Length` with 400 and a declared
length above 1 MiB with 413 before reading the stream. Incremental reads share one
total deadline, `MAKOLET_MCP_HTTP_BODY_TIMEOUT_SECONDS` (10 seconds by default), and
deadline expiry returns 408. Uvicorn admits at most
`MAKOLET_MCP_HTTP_MAXIMUM_CONCURRENCY` concurrent connections/tasks (100 by default).
A reverse proxy must apply limits no weaker than the same declared/body-byte ceiling,
total read deadline, and concurrency cap; an idle/read-gap timeout alone is not a
total-body deadline.

For modern requests, HTTP metadata must agree with the JSON body:

- `MCP-Protocol-Version: 2026-07-28`;
- `Mcp-Method` exactly equals the JSON-RPC method;
- `Mcp-Name` exactly equals the tool name for `tools/call` (printable ASCII or the
  MCP base64 sentinel form).

Missing/mismatched metadata returns HTTP 400 with `-32020`; an unknown modern method
returns HTTP 404. Legacy calls do not require those modern metadata headers.

If an HTTP `Origin` header is present, it must exactly match a configured
`MAKOLET_MCP_ALLOWED_ORIGINS` value (scheme and authority only, no path/query). An
absent origin is accepted for non-browser clients. The transport has no TLS or user
authentication; keep it on loopback/private networks or place it behind a separately
managed secure boundary.

## Minimal legacy example

With HTTP transport running:

```powershell
$headers = @{
  Accept = "application/json, text/event-stream"
  "Content-Type" = "application/json"
}
$body = '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search_products","arguments":{"query":"7290000000015","limit":10}}}'
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8001/mcp -Headers $headers -Body $body
```

Use a conforming MCP client for current protocol metadata and discovery. The direct
protocol implementation decision and compatibility boundary are recorded in
[ADR 0006](adr/0006-direct-mcp-protocol-core.md).
