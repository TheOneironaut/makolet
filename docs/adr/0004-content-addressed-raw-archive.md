# ADR 0004: Content-addressed immutable raw archive

- Status: Accepted
- Date: 2026-08-11

## Context

Every public source file must be archived exactly as received before parsing and must
remain replayable after signed URLs expire or portals change. Development needs no
external service; production-style deployments need S3-compatible storage without a
specific cloud dependency.

## Decision

Hash the exact streamed response with SHA-256 while writing to a private staging
object. Commit it atomically under a content-derived key only after byte count and hash
are final. Existing objects at that key are immutable and must match; never overwrite.

Provide two implementations behind one archive port:

- local filesystem, with an atomic same-filesystem hard-link create and read-only
  committed files. Reads accept only a stable, no-follow regular-file identity within
  the configured object-size ceiling, hash it completely into a private bounded
  spool held open by the parent process, and expose only that exact verified spool to
  the parser. The bounded copy runs in a killable child process. `verify()` consumes
  this same path, so a pathname replacement cannot separate verification from parse;
- S3-compatible storage, using conditional create where supported and explicit
  head/hash verification for idempotency. Conditional upload and its read-back
  verification share one total operation deadline. Every SDK operation runs in an
  isolated spawn process, and verified reads write into a parent-owned private file
  descriptor transferred only after an authenticated post-start acknowledgement.
  The child monitors a parent-liveness channel and exits if the parent hard-exits.
  Deadline expiry or caller cancellation terminates and, if necessary, kills that
  child, so an SDK call that ignores `close()` cannot hold process exit.

Store discovery/download/HTTP-or-FTP metadata, original filename and remote identity,
retailer/portal/type/timestamps, hash, byte length, media/compression type, parser
version, and lifecycle status in PostgreSQL. The database points to the content key;
an expiring signed URL is provenance, never the durable identity.

Parsing reads only from the committed archive. Archive and decompression limits are
separate: the exact object may be accepted for audit but quarantined before parsing
when expansion, format, or structure is unsafe.

## Consequences

- A failure after archival can replay without network access.
- Duplicate bytes across remote identities share storage but retain separate source
  discovery metadata.
- Backup includes both PostgreSQL metadata and raw objects, verified by hashes.
- Retailer data rights are distinct from the Apache-2.0 software license; raw data is
  never committed to this repository or represented as project-licensed content.
