# ADR 0006: Direct bounded MCP protocol core

- Status: Accepted; amends ADR 0001
- Date: 2026-08-11

## Context

The platform requires read-only typed MCP tools over local stdio and a documented
network transport. The initially evaluated `mcp==2.0.0` dependency was unused by the
implementation and selected `pywin32==312` on Windows. The audited wheel included an
LGPL `adodbapi` payload, conflicting with the project's literal OSI-and-
Apache-compatible dependency policy. Pulling a complete server SDK for twelve query
tools also widened the transport/runtime surface materially.

MCP protocol behavior changes over time, so a direct implementation is acceptable
only if its surface remains small, versioned, bounded, and tested against the public
specification rather than approximated from another project's code.

## Decision

Implement an original protocol core on JSON-RPC 2.0 with Pydantic argument schemas:

- current `2026-07-28` `server/discover`, `tools/list`, and `tools/call` behavior;
- legacy `initialize` negotiation for `2025-11-25` and `2025-06-18`;
- newline-delimited stdio with stdout reserved for protocol data;
- stateless FastAPI `POST /mcp` with exact content negotiation, origin checking,
  current-version metadata/header agreement, a 1 MiB body bound, and no sessions;
- fourteen read-only, non-destructive, idempotent tools calling `QueryService`;
- bounded page/search/history/cursor inputs, safe errors, and no arbitrary SQL.

Keep protocol framing in `interfaces/mcp.py`; business queries remain in the
application service. Add an SDK only after an exact artifact/license audit and a
measured maintenance or interoperability benefit. Do not copy an external MCP server
implementation.

## Consequences

- The runtime graph has no MCP SDK or `pywin32` dependency.
- Makolet owns compatibility tests for both protocol generations and HTTP metadata,
  status, origin, content-type, Accept, body-size, notification, and tool-error paths.
- The server intentionally omits sessions and server-initiated SSE. A future transport
  feature requires a new decision and tests, not an undocumented partial extension.
- Public-spec changes must update the version constants, schemas, documentation, and
  automated transport tests together.

## Evidence

- [MCP specification](https://modelcontextprotocol.io/specification/)
- [Dependency/license audit](../research/dependency-license-audit.md)
- [MCP interface documentation](../mcp.md)
