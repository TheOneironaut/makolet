# Source adapter guide

A source adapter discovers public files and returns stable metadata. It does not
download a file, open the archive, parse XML, or write PostgreSQL. Those boundaries
are intentional: a new portal variation should not weaken shared download/parser
safety or duplicate business logic.

## Implemented families

| Family key | Module | Observed behavior |
|---|---|---|
| `bina_https_json` | `adapters/sources/bina/` | Date/type-partitioned ASP.NET JSON arrays plus a download-path resolver; empty `[]` is an empty partition, later families still enumerate, and a 1,000-result partition still fails closed. |
| `matrix_https_json` | `adapters/sources/matrix/` | Bounded nonempty whole-catalog Laib/Matrix JSON arrays keyed by EDI. Empty `[]` and empty named wrappers fail closed. |
| `ncr_ftp_ftps` | `adapters/sources/ncr/` | Credentialed FTP or explicit FTPS directory listing, including multiple feeds for a legal group. |
| `static_daily_https` | `adapters/sources/static_daily/` | Dated HTML/embedded indexes, currently configured for Carrefour and Wolt. |
| `hazi_html_azure` | `adapters/sources/hazi/` | Hazi Hinam HTML pagination and public Azure object URLs. |
| `city_html_uuid` | `adapters/sources/city/` | City Market HTML metadata with same-origin opaque UUID downloads. |
| `shufersal_html_azure` | `adapters/sources/shufersal/` | ASP.NET pagination and expiring public Azure read URLs. |
| disabled statuses | `adapters/sources/disabled.py` | Explicit `unresolved` or `externally_blocked` failure; never an empty-success adapter. |

Retailer/legal-group configuration and official ordering live in
`adapters/sources/registry.py`. The current matrix and dated live evidence are in
[source coverage](source-coverage.md).

## Discovery contract

Every adapter implements:

```python
class SourceAdapter(Protocol):
    source_id: str

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: DiscoveryRunBudget | None = None,
    ) -> DiscoveryPage: ...
```

`DiscoveryPage.files` contains `RemoteFile` values with retailer/portal IDs,
protocol, stable `remote_id`, current download URL, safe original filename, inferred
document/compression type, aware discovery time, and optional source/size/media/ETag/
last-modified metadata. `next_cursor` is opaque outside that adapter. `complete=true`
is valid only on the terminal page.

Collection supplies one mutable `DiscoveryRunBudget` for the whole source traversal;
direct diagnostic calls receive the same safe defaults when they omit it. Every
network listing/redirect/resolver request must call `begin_request()`, and every
retained listing body contribution must call `consume_bytes()`. Adapters must pass
the same object to nested transport calls. Collection also enforces its elapsed
deadline around adapter parsing/materialization, so valid terminal or unknown pages
cannot multiply work outside the request, byte, or time ceiling. Whole-catalog
families such as Matrix cache the catalog only for continuation offsets within one
adapter traversal; a new cursor-less traversal fetches fresh publisher state.

The safe original filename is a decoded basename of at most 255 characters and 255
UTF-8 bytes. C0/C1 controls, surrogates, and Unicode directional overrides/isolates
or line/paragraph separators are rejected at the domain boundary; path-bearing or
still-percent-encoded values are not valid `RemoteFile` filenames. Stable remote IDs
are limited to 4,096 characters. Download URLs are limited to 8,192 characters, must
match the declared network protocol, contain the required host for network protocols,
and cannot carry URL user information. The non-network `fixture` protocol may retain
an HTTP-shaped test locator without authorizing a network request. Do not replace
these shared checks with source-family-specific sanitization.

Required behavior:

- accept limits only from 1 through 500 and never return more;
- use a versioned, source-bound cursor; reject malformed, oversized, repeated, stale,
  or cross-source cursors rather than restarting silently;
- return deterministic order and deduplicate stable remote identities;
- use source timestamps from reliable listing metadata/filename evidence only;
- treat malformed, nonexistent, or ambiguous optional listing `last_modified`
  metadata as absent. It is an advisory transport hint and must not turn an otherwise
  valid catalog row into a source failure; an explicit numeric offset remains
  authoritative;
- use `canonical_remote_id()` so volatile query signatures do not define identity;
- mark unknown document/compression types honestly; collection skips rather than
  guessing how to ingest them;
- distinguish empty terminal pages from broken pagination. Collection rejects an
  empty page with a continuation cursor or an incomplete page without one.

## URL and listing safety

Each family declares exact listing/download host, scheme, port, redirect, and byte
policies. `SafeHttpListingClient` resolves every hostname, connects to the selected
validated public address while preserving the logical HTTPS hostname/SNI, and rejects
userinfo, disallowed ports, unexpected redirects, non-identity listing encodings,
oversized listings, and non-200/error statuses. HTML is parsed as inert text; scripts
are not executed. Plain-text/error bodies, lexical EOF, nested anchors, and unclosed
containers fail closed; a source may allow only an evidence-backed optional root end
when every other structure is complete and recognized files are present. JSON callers
select either an exact unambiguous wrapper or an evidence-backed nonempty direct-array
mode, never an arbitrary list or terminal empty fallback. HTML and JSON bytes, tokens,
depth, strings, attributes, retained
text, and collection sizes are bounded before full materialization. Static-daily
date-page URLs pass the same host and scheme/default-port contract before a request;
HTTPS always means port 443, including an explicitly written port, and can never be
combined with an independently allowlisted port 80. Static-daily
`const files` catalogs preflight exactly one embedded JSON value under the same
8 MiB byte, depth 64, 500,000 structural-token, 65,536-character string, and
4,096-character scalar ceilings before `raw_decode`; legal trailing publisher
JavaScript remains inert and supported.

The per-response 8 MiB ceiling is not reset into unbounded source-run work. Production
collection additionally shares default cumulative ceilings of 256 requests, 8 MiB,
and 300 seconds across every page, redirect, resolver, and FTP listing in that run.

The HTTP downloader independently validates the final file URL against the registry's
portal-keyed policy. Do not pass a newly discovered host through because it appeared
in a response. Add it only after public evidence and an SSRF review.

NCR feeds are registry-declared. FTP URLs cannot contain credentials, queries,
fragments, alternate hosts/ports/directories, or a filename different from discovered
metadata. The control/data socket connects to a validated public address; FTPS uses
the logical hostname for certificate verification, and passive-mode server addresses
cannot redirect the data socket. Plain FTP is blocked unless
`MAKOLET_ALLOW_INSECURE_FTP=true`. That opt-in accepts an inherent lack of publisher
authenticity and is recorded as `transport-security=unauthenticated`; FTPS records
`transport-security=tls`. DNS resolution, connection, listing/download, and protocol
cleanup consume one total operation deadline. MLSD and NLST use an owned incremental
data channel rather than `ftplib`'s materialized MLSD list: every wire byte and line,
including directories and unused facts, is charged before decoding or filtering;
raw bytes, line length/count, and MLSD fact tokens have hard ceilings. Timeout or
caller cancellation closes both the active data socket and control client, and the
blocking bridge waits only for a bounded cleanup interval. Credentials are injected
by key:

```text
MAKOLET_SOURCE_<NORMALIZED_CREDENTIAL_KEY>_USERNAME
MAKOLET_SOURCE_<NORMALIZED_CREDENTIAL_KEY>_PASSWORD
```

For example, credential key `dor-alon_primary` becomes
`MAKOLET_SOURCE_DOR_ALON_PRIMARY_USERNAME` and `_PASSWORD`. Only use credentials the
publisher intends for public transparency access. Never add their values to examples,
fixtures, error messages, or coverage notes.

## Adding a retailer to an existing family

1. Add primary-source observations and clean-room notes to
   `docs/research/source-research.md`, including date, protocol, listing/file forms,
   auth, encoding/compression evidence, and any blocker.
2. Confirm the legal entity/row and choose a stable lowercase source key. Do not merge
   separate official entities just because they share a brand or portal.
3. Add a `RetailerSourceDefinition` using an existing family config. Keep endpoint and
   identifier differences in immutable configuration.
4. Add/extend independently authored fixture records proving the observed field or
   filename variation. Record fixture origin/license in the fixture README.
5. Add registry and family contract tests: normal pages, pagination, malformed and
   truncated responses, unsafe links, duplicate entries, bounds, cursor misuse, and
   source timestamp/document/compression inference.
6. Update `docs/source-coverage.md` with implementation, fixture, live result, and
   exact limitation as separate columns.

If the adapter needs a new conditional for one retailer, first determine whether the
response proves a new family behavior. A retailer-specific module is appropriate only
when the observable protocol/listing behavior cannot be expressed safely through the
existing config.

## Adding a portal family

Start with the narrowest adapter that matches the observed listing. Use shared helpers
from `sources/common.py` for safe filenames, encoding, link/JSON parsing, timestamps,
cursor framing, URL validation, and deduplication. Do not create a generic scraper
base class.

Add all of the following in the same change:

- immutable config and adapter module under `adapters/sources/<family>/`;
- a registry construction branch plus file-download policy/feed entry;
- legal minimal fixtures and source contract tests;
- parser contract fixtures only if the actual document variation requires parser
  work (parsers remain source-independent);
- primary evidence, license/clean-room decision, coverage row(s), operations notes,
  and any new configuration variables;
- a low-volume opt-in live test path.

Do not copy code, tests, fixture bytes, prose, or configuration from a restricted,
non-commercial, source-available, or unlicensed repository. Public behavior and
official file formats may be reimplemented independently.

## Testing

Offline source contracts:

```text
uv run pytest tests/unit/sources -m "not live" --no-cov -q
```

One explicit read-only live listing (PowerShell):

```powershell
$env:MAKOLET_LIVE_SOURCE = "shufersal"
uv run pytest tests/unit/sources/test_live_sources.py -m live --no-cov -q
Remove-Item Env:MAKOLET_LIVE_SOURCE
```

The live test performs at most one adapter discovery result and no ingestion/write.
Run only one selected source, respect publisher limits, and record external failures as
external failures. A fixture pass is never a live green result.

For operator diagnosis, `makolet sources test <source-id>` likewise makes one bounded
listing request and does not ingest. `sources inspect` is configuration-only and safe
offline once the runtime database/archive dependencies are available.
