# Data dictionary

This is the semantic dictionary for migrations `0001_initial` through
`0010_bounded_query_paths`. The append-only migrations are the authority for exact
PostgreSQL types, defaults, foreign keys, checks, and index names; this document
explains how application code uses the resulting 44 tables.

## Conventions

| Convention | Meaning |
|---|---|
| `id` | Internal UUID primary key, generated with PostgreSQL `uuidv7()` unless a join/staging key is used. |
| `*_id` UUID | Internal foreign key unless explicitly described as a publisher/source identifier. |
| `source_*`, `chain_*`, `subchain_*` | Preserved publisher values, never global primary keys. |
| `*_at` | Timezone-aware PostgreSQL timestamp. |
| money | `numeric(14,4)`; represented as Python `Decimal` and JSON strings. |
| quantity | `numeric(18,6)`; discount rates use `numeric(10,6)`. |
| `name_search`, `city_search` | Stored generated NFKC/lower/punctuation-and-space-normalized text used by indexed query paths. |
| `valid_from`, `valid_to` | Half-open validity interval; null `valid_to` means the open/current historical value. |
| `source_file_id` | Provenance of the observation or state change. |

Enumerated checks are:

- document type: `stores`, `price_full`, `price_delta`, `promotion_full`,
  `promotion_delta`, `unknown`;
- compression: `none`, `gzip`, `zip`, `zstandard`, `unknown`;
- protocol: `https`, `http`, `ftp`, `ftps`, `fixture`;
- ingestion status: `discovered`, `downloading`, `archived`, `parsing`, `staged`,
  `validating`, `applying`, `completed`, `quarantined`, `failed_retryable`,
  `failed_terminal`;
- issue severity: `warning`, `record_rejection`, `file_quarantine`,
  `source_failure`, `system_failure`;
- discount kind: `fixed_price`, `percentage`, `amount`, `quantity`, `second_item`,
  `mix_and_match`, `conditional`, `unknown`.

## Publishers, stores, and source provenance

### `retailers`

One legal/operational retailer identity. `source_key` is the stable application key;
`legal_name`, `display_name`, optional `edi`, and `is_active` are metadata. Both
`source_key` and non-null `edi` are unique.

### `portals`

One discovery feed for a retailer. Important fields are `retailer_id`, scoped unique
`source_key`, `family`, `protocol`, optional `base_url`, and `is_active`. A retailer
may have multiple portals/feeds.

### `collection_checkpoints`

One durable traversal boundary per retailer, exact portal-generation fingerprint,
ordinary/backfill operation, optional inclusive range, and archive-only mode. It
stores the publisher cursor plus in-page offset, positive traversal generation,
generation recognized/unknown counts, completion time, and the exact sorted portal
set as bounded JSON. Null-distinct uniqueness keeps ordinary and independently ranged
backfill checkpoints separate; cursor and portal-generation checks fail closed on
oversized or malformed persistent state.

### `collection_attempts`

Audited executions of a collection checkpoint. Each row retains generation,
start/finish state, start and last committed cursor/offset, discovered/processed/
duplicate/unknown/warning counts, actual collection charged bytes, a categorized
truncation reason, and safe error fields. At most one
running attempt exists per checkpoint. A source traversal lease prevents concurrent
workers from advancing the same source out of order.

`charged_bytes` is the truthful collection resource total: immutable archive bytes,
failed/retried transfer overhead, and any conservative in-flight reservation that
could not be safely settled after a crash or ambiguous archive commit.
It is `null` for attempts created before migration 0009, because those legacy rows
have no exact collection charge ledger; attempts created in the accounting era start
at zero and retain an exact non-negative value.

### `collection_charge_budgets`

One locked summary row per retailer source. It caches charged bytes plus independent
source-identity, transfer-attempt, and success counts for the rolling fixed-bucket
window. Collection recomputes it from at most 289 five-minute buckets while holding
this row, so simultaneous attempts cannot oversubscribe a durable source/day ceiling.

### `collection_budget_buckets`

One row per retailer and five-minute UTC bucket with exact charged bytes and the three
cardinality counters. The bounded rolling read is conservative at its boundary; the
immutable charge ledgers remain the exact accounting source.

### `collection_identity_observations`

One immutable first observation per `source_file_id`, retaining retailer and time.
Its primary key makes repeated attempts for the same remote identity idempotent.

### `collection_archive_charges`

One immutable accounting charge per `source_file_id`, containing the actual archived
object length, retailer source, originating collection attempt, and charge time. The
source-file primary key makes CAS reuse, retries, and checkpoint interruptions
idempotent. Terminal rejected/quarantined files remain immutable evidence and still
consume the quota.

### `collection_transfer_charges`

One durable reservation and settlement per `(attempt_id, source_file_id)`. Collection
creates the maximum permitted reservation before network I/O, then settles it to
failed/retried transfer overhead after the source row has transactionally revealed
whether immutable archive bytes attached. Multiple files in one attempt therefore
remain independently idempotent. The attempt foreign key is restrictive so deleting
audit state cannot erase a live rolling-window charge. An interrupted or ambiguous
commit deliberately retains its conservative reservation until the 24-hour window
expires.

### `stores`

Current retailer store identity and display/location data. The natural scope is
unique `(retailer_id, portal_id, subchain_code, source_store_code)`. It retains portal,
chain/subchain,
audit/type, name/address/city/postal code, active flag, first/last seen times, and
`last_source_file_id`. `city_search` plus city-first and retailer/city/UUID indexes
serve exact normalized keyset pages; the name trigram index serves bounded fuzzy
windows.

### `store_aliases`

Alternate source identities for a store. `alias_kind` and `alias_value` are unique
within a retailer portal and point to one `store_id`.

### `raw_archive_objects`

Content-deduplicated exact source bytes: unique `content_sha256`, unique canonical
`object_key`, `content_length`, `archived_at`, and optional `verified_at`. The object
itself lives in the local or S3-compatible archive.

### `source_files`

One stable remote identity, unique by `(portal_id, remote_id)`. It records retailer,
portal, URL, original filename, document/compression/protocol, lifecycle status,
source/discovery metadata, declared length/media/ETag/last-modified/safe response
metadata, archive link, parser version, download evidence, and safe error fields.
First discovery chronology is preserved. Optional evidence may be filled only by the
per-file lock owner before archive attachment; after attachment it is immutable.
Only a same-origin/path HTTPS signed-query rotation is eligible before attachment,
and conflicting rediscovery fails closed.
Indexes cover status work queues, retailer/document/source time, archive lookup, and
deterministic archive paging by download-finish time then UUID.

### `source_scope_watermarks`

Atomic latest-source guard per `(retailer, portal, document family, subchain, source
scope)`. It stores the effective source timestamp, exact content hash, and source
file. Apply locks and advances it in the same transaction as normalized mutation;
older/conflicting input is quarantined instead of rewinding current state.

### `applied_source_contents`

Immutable per-source successful-apply chronology. `source_file_id` is unique; the
row also retains retailer, portal, document type, exact content hash, and the
deterministic `applied_at` used for observation/history intervals. Different source
identities may share bytes while keeping distinct chronology.

## Runs, lifecycle, and quality

### `ingestion_runs`

One numbered attempt per source file. It stores lifecycle status and start/finish
times; parsed metadata/store/price/promotion counts; warnings/rejections; apply
insert/update/unchanged/unavailable/history counts; exact file-quarantine count and
logical UTF-8 issue bytes; bounded persisted issue-sample count; and safe error code/message.
`(source_file_id, attempt)` is unique and every count is non-negative.

### `source_file_events`

Append-only state transitions for a source file and ingestion run. It stores optional
`from_status`, required `to_status`, safe error detail, and `occurred_at`, ordered by
file/time/UUID.

### `validation_issues`

Structured warning/rejection/quarantine/source/system issue with code, message,
optional record/field/rejected value, and creation time. Exactly one of
`ingestion_run_id` or `replay_run_id` must be present. At most 1,000 evidence rows are
retained per attempt by default; run/replay summaries remain exact and source status
reads those summaries rather than scanning this evidence table.

### `replay_runs`

One deterministic archive replay attempt with requested/previous parser version,
status, time window, JSON result summary, and safe error message. Optional
`rebuild_run_id` links attempts performed by a normalized rebuild. A partial unique
index allows only one unfinished replay per source file.

### `normalized_rebuild_runs`

One operator-requested in-place normalized rebuild. It stores the non-secret operator
label, parser version, fixed archive cutoff, running/failed/completed status, total and
completed file counts, last durable sequence/source/archive timestamp, start/update/
finish times, and bounded safe error detail.

### `normalized_rebuild_files`

The immutable source-file work list captured before a rebuild reset. Each run has one
row per eligible archive-backed completed or archive-only source file. Rows are
sequenced by original apply time when present, otherwise effective source time, then
source UUID. A row records archive-attachment time, original apply time and parser
version when available, effective source timestamp, pending/completed state, and
completion timestamp. The run/file pair is unique.

### `normalized_rebuild_snapshots`

Durable, run-scoped identity and equivalence evidence captured inside the rebuild
barrier transaction. The fixed entity allowlist covers durable catalog and reviewed
matching rows plus all reset observation, history, promotion-relation, watermark, and
apply-ledger rows. Each original/rebuilt row stores an exact logical key and JSON
object payload; keys are limited to 512 bytes and each payload to 1 MiB. An unchanged
rebuild reconciles the regenerated UUIDs and timestamps, proves exact equality, and
deletes both phases. A parser correction or archive-only expansion retains the
original phase with `preserved` or `superseded` outcomes as audit evidence; failed
runs retain their original phase for same-run resume and investigation.

### `normalized_rebuild_control`

A checked singleton row whose optional `active_rebuild_run_id` is the database
maintenance barrier. Ordinary apply takes a shared lock and fails closed when a run
is active; only replay attempts linked to that active running rebuild are admitted.

### `leases`

Legacy crash-expiring coordination rows keyed by `resource`, with owner, unguessable
`lease_token`, acquisition, and expiry. Current source traversal and per-file
ingestion/replay ownership use crash-releasing PostgreSQL session advisory locks and
do not depend on this table or a takeover TTL. Existing rows remain schema/audit
compatibility data and should not be edited to influence current ownership.

## Product identity and matching

### `retailer_items`

The publisher-specific catalog entity. `(retailer_id, portal_id, source_item_code)`
is unique.
It preserves optional GTIN/item type, name and generated search text, manufacturer
fields, unit/quantity/weight/package fields, first/last seen, and latest source file.
GTIN and trigram name indexes support deterministic candidates and reads. The
`(last_source_file_id, id)` index makes post-ingestion isolated-catalog bootstrap a
bounded keyset operation rather than a catalog-wide scan.
Public search normalizes explicit English/Hebrew mass and volume aliases to base
grams/millilitres before exact numeric comparison with these item fields; free-text
substring matches are not treated as quantity evidence.

### `canonical_products`

Cross-retailer product identity: name/search text, brand, manufacturer, quantity,
unit, lifecycle status (`active`, `merged`, `retired`), and timestamps. GIN plus
active-product prefix/GiST trigram indexes support public search and bounded candidate
blocks. A system-created active product may initially represent exactly one retailer
item; that isolated representation makes a non-GTIN SKU queryable but does not assert
equivalence to any other item. When review merges its only item into another product,
the now-unreferenced isolated product is retired transactionally.

### `product_identifiers`

Identifiers attached to a canonical product. `kind` is `gtin`, `retailer_item`,
`manufacturer`, or `unknown`; original and normalized values are both preserved.
Optional `issuer_retailer_id` and `issuer_portal_id` scope non-global identifiers.
Retailer-item identifiers require both. Identity uniqueness uses `NULLS NOT DISTINCT`;
`is_validated`, `validation_method`, and JSON
`validation_evidence` control and explain exact barcode resolution.

### `identifier_match_groups`

The stable canonical product selected for one normalized identifier value. The
`(kind, normalized_value)` pair is unique and prevents ingestion order from creating
different canonical products for later independent retailer assertions.

### `retailer_identifier_assertions`

Source-proven identifier evidence for one retailer item, including original and
normalized values, source file, validation method, assertion time, and optional
supersession. Only one current assertion per item/kind is allowed. Global validation
can therefore require independent retailer corroboration without treating a checksum
alone as global identity proof.

### `product_match_candidates`

Reviewable proposed retailer-item/canonical-product link. It records method, bounded
score `[0,1]`, JSON evidence, status (`pending`, `accepted`, `rejected`,
`superseded`), and review metadata. Candidate tuple `(item, product, method)` is
unique. Evidence contains the normalized rule, explanation list, generation time,
and bounded subject/candidate feature snapshots. Regeneration updates pending rows
but never silently reopens a rejected, accepted, or superseded decision.

### `confirmed_product_matches`

The effective one-to-one assignment from a retailer item to a canonical product,
including method, JSON evidence, confirmation time, and actor. A retailer item can
have only one confirmed match; a canonical product may have many items.
`isolated_retailer_item` is a system-owned one-item representation, not a global
identity assertion. An accepted review may replace only that system-owned link;
manual and other non-isolated links fail closed on conflict.

## Prices and availability

### `current_prices`

One current price per `(retailer_item_id, store_id)`: item and unit price,
discount flag, source update/last-sale/audit fields, source provenance, and first/last
observation times. Nullable internal `canonical_product_id` and
`query_retailer_id` projections mirror the effective confirmed match and item owner.
Product/price, product/retailer/price, product/store/price, and combined filter
indexes support exact cheapest-first candidate pages before response decoration.

### `price_history`

Historical price validity rows with the same source values and provenance plus
`valid_from`/`valid_to`. A partial unique index permits one open row per item/store;
internal `canonical_product_id` plus product/time and product/store/time covering
indexes support newest-first pages. Stored `valid_period` and composite GiST indexes
obtain a bounded overlap candidate projection, including the predecessor whose
validity crosses the requested half-open window boundary.

### `current_availability`

One current availability row per `(retailer_item_id, store_id)`, preserving the
publisher `item_status`, source file, and first/last observation times.
Its internal `canonical_product_id` projection has product/UUID and
product/store/UUID indexes for exact candidate-first availability pages.
The `(store_id, is_available, retailer_item_id)` index supports the deterministic
bounded freshness count probe. The
`(store_id, last_observed_at DESC, source_file_id DESC, id DESC)` index independently
supports the globally latest freshness contributor, so a truncated count probe
cannot report stale observation time or provenance.

Migration 0010 backfills these three product projections in 10,000-row UUID batches.
Statement-level insert triggers populate bulk writes, rekey triggers cover an item
identity change, and set-wise confirmed-match triggers apply inserts and remaps.
Deletion is deferred until commit and clears a projection only when no replacement
match exists, avoiding transient delete/reinsert churn. Thus isolated bootstrap, audited remapping,
ordinary ingestion, replay, and normalized rebuilds share one database-enforced
projection contract.

### `availability_history`

Historical availability intervals. A partial unique index permits one open row per
item/store; the history index orders intervals newest first.

## Promotions

### `promotions`

Versioned promotion state. The scoped publisher identity is `(retailer_id, portal_id,
subchain_code, source_promotion_id, source_scope_store_code)` for its one open row.
Fields preserve
description, discount kind, source window, reward/multiple-discount flags,
quantity/rate/purchase/price/item conditions, restrictions, remarks, active flag,
fingerprint, source file, validity, and last observation. Checks reject negative
numeric conditions and invalid ranges. Stored `valid_period` and `active_period` GiST
indexes bound history windows and arbitrary instants before page decoration.

### `promotion_items`

Many-to-many promotion membership for retailer items, with optional source
`item_type` and `is_gift`.

### `promotion_stores`

Stores explicitly eligible for a promotion. No rows means the query layer treats the
promotion as not store-restricted.

### `promotion_clubs`

Publisher club/member identifiers required by a promotion.

## Per-file staging

Staging rows are keyed by `source_file_id` and source record indexes. They are cleared
before parse/replay, populated in bounded COPY batches, validated, applied in one
transaction, and retained until the next staging clear for that file.

### `staged_documents`

Document header metadata: type, chain, subchain, store, audit number, and source update
time, keyed by `(source_file_id, metadata_index)`.

### `staged_stores`

Normalized store records: record index, chain/subchain/source store identity,
audit/type/names, address, city, and postal code.

### `staged_prices`

Normalized price records: store/item identity; optional GTIN/type/manufacturer and
quantity metadata; decimal item/unit price; discount/status/source times/audit data.
Prices and quantities have non-negative checks.

### `staged_promotions`

Normalized promotion header/conditions plus a deterministic `fingerprint_sha256`.
Child item/store/club arrays are stored in the three tables below.

### `staged_promotion_items`

Ordered source item links `(source_file_id, record_index, item_index)` with source item
code, type, and gift flag. Its deferrable foreign key targets the staged promotion.

### `staged_promotion_stores`

Ordered source store links `(source_file_id, record_index, store_index)`.

### `staged_promotion_clubs`

Ordered club IDs `(source_file_id, record_index, club_index)`.

## Analytical export mapping

The Parquet command exports `current_prices` partitioned by `last_observed_at` UTC
date and `price_history` partitioned by `valid_from` UTC date. Each output row includes
retailer/item/store identifiers, source values, time bounds, `source_file_id`, source
portal UUID/key, document type/source timestamp/download-finish time, and exact raw
archive SHA-256. Current rows obey the requested inclusive observation range. History
rows overlap the requested range when `valid_to > since` (or remains open) and
`valid_from <= until`: a half-open history interval ending exactly at `since` is
excluded, while the requested upper endpoint remains inclusive. Partition discovery
and every streamed row use one repeatable-read, read-only PostgreSQL snapshot. The
command result records that snapshot, while schema and checksums are published in
each partition's `_manifest.json`; see
[the CLI guide](cli.md#parquet-export) and
[Parquet interoperability evidence](research/parquet-interop.md).
