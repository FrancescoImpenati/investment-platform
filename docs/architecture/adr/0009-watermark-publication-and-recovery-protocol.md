# ADR 0009: Retention-aware watermark, publication, and recovery protocol

- **Status:** Accepted
- **Date:** 2026-08-31
- **Phase:** 2 — Living Data Ingestion
- **Implementation status:** Planned for Phase 2

## Context

Parquet publication and SQLite state changes do not share one atomic transaction. Advancing a
watermark before canonical files are durable can hide missing history. Publishing files and then
losing the job-state update can cause retries to produce arbitrary copies. Overlapping backfill,
incremental, and repair windows also make `MAX(timestamp)` an unsafe definition of progress.

Provider calls and process attempts may repeat after timeouts or crashes. Phase 2 therefore cannot
honestly promise end-to-end exactly-once delivery.

## Decision

Use at-least-once request attempts with artifact and semantic idempotency. Filesystem publication is
completed first; one SQLite transaction then makes a verified batch visible and updates coverage,
gaps, and its watermark.

### Identities and idempotency

The following identities have distinct purposes:

- `run_id` identifies one user-visible ingestion run.
- `request_spec_hash` is a deterministic digest of the canonical bounded request: provider, exact
  dataset, stream dimensions, half-open interval, and request-affecting parameters.
- `request_id` identifies one planned request instance. Repair may deliberately create another
  instance with the same specification hash; `attempt_id` identifies each retryable execution.
  Pagination creates deterministic child identities rather than changing the specification.
- `raw_artifact_id` binds the request specification, stable page relation, canonical media
  type/content encoding, byte length, and content checksum. Attempt-varying provider request IDs
  and response metadata remain provenance but do not change artifact identity. Repeating identical
  request/page/representation/bytes confirms or reuses the immutable artifact; different content
  never overwrites it.
- `batch_id` is a deterministic digest of `request_spec_hash`, ordered raw-artifact identities,
  schema, normalizer and validator versions, any semantically relevant calendar snapshot, and
  canonical-output dimensions. It excludes the request instance and retry attempt. An existing
  destination is reused only after its manifest and content verify identically.
- `observation_id` identifies one canonical fact by provider, dataset, stable instrument UUID,
  timeframe, session, adjustment mode, interval/event time, and every other dimension that changes
  stream semantics. A semantic value/processing fingerprint distinguishes an identical replay from
  a provider correction; additional raw provenance is linked separately.

Artifact idempotency means identical immutable input or output artifacts converge on the same
identity. Semantic idempotency means retrying the same canonical facts does not create another
visible copy. A changed value for the same observation identity is preserved as a new version with
its batch and retrieval provenance; it is not silently overwritten. This protocol does not select a
winner between different providers.

Once raw acquisition is complete, a durable batch context fixes the batch ID, ordered inputs,
processing versions, and the `ingested_at`/manifest creation timestamp. Every replay reuses that
context. Attempt-varying run and retention-policy provenance remains in SQLite, outside immutable
content identity, so the same batch ID cannot produce different bytes after a crash.

Likewise, the first raw publisher fixes one immutable artifact manifest. Later identical attempts
verify its identity-bearing request/page/content fields and add attempt/retrieval/provider-request
provenance through SQLite links; they do not require volatile metadata to match or rewrite the
manifest.

Overlapping verified batches are checked by observation identity and value/provenance fingerprint
during publication or reconciliation. Identical facts are reused or excluded from a second visible
version. Conflicting facts remain explicit revisions for comparison. SQLite stores batch metadata
and state, not bar copies.

### Publication protocol

The durable state machine is:

1. Create the ingestion run and acquire the single-writer lease.
2. Plan and persist canonical, bounded logical requests before attempting them.
3. Receive through bounded transient transport, reject/erase response bytes outside policy
   age/bounds before downstream use, then persist every permitted raw artifact immutably and verify
   its checksum. Unauthorized bytes never enter raw or quarantine.
4. Persist or reuse the fixed batch context after raw pagination completes.
5. Normalize from those artifacts with the context's recorded code, schema, and timestamps.
6. Validate schema, provenance, interval bounds, observation identities, and quality findings per
   stream. Recoverable findings remain flagged; a fatal finding blocks that stream's whole bounded
   interval rather than silently dropping rows.
7. Write the complete canonical batch for independently publishable streams, checksums, and
   blocked stream outcomes to `staging/<batch_id>/`. Write the manifest last as the staging
   completion record, then flush and close all files.
8. Atomically rename that new directory to its immutable final batch path. Never replace a
   non-identical destination.
9. Reopen and verify the published files, manifest, checksums, row/identity summaries, policy
   status, and expected interval.
10. In one `BEGIN IMMEDIATE` SQLite transaction, insert or adopt the verified batch, update only
   independently verified stream coverage and all gap/blocking outcomes, advance the watermark
   only over verified contiguous coverage, and commit the request's terminal SUCCESS, PARTIAL, or
   FAILED outcome.
11. Commit that transaction, making the batch queryable through the catalog, request outcome, and
    watermark durable together.
12. Derive and persist any now-terminal aggregate run summary. If this bookkeeping is interrupted,
    recovery derives it from the committed request and batch state.

If validation leaves no publishable stream, publication is skipped. A short SQLite transaction
records the FAILED request and blocking outcomes without adding coverage or changing the
watermark.

Canonical query code obtains paths from SQLite and reads only batches in `VERIFIED` state. A file
that exists on disk but is not cataloged as verified is not visible to DuckDB queries.

### Watermark invariant

A durable watermark may exist only while the observations supporting it are durably retained and
present. It is the exclusive end of verified, contiguous coverage for one exact stream key, not the
maximum timestamp observed. The stream key includes provider, dataset, instrument, timeframe,
session, adjustment mode, and all other semantics-changing dimensions. Coverage retains explicit
gaps and verification state.

Contiguity is measured over eligible sessions and bar slots in the referenced calendar snapshot,
not every wall-clock instant. Exchange-closed overnight periods, weekends, and holidays are
`NOT_APPLICABLE`, so an RTH frontier may cross them without inventing coverage. An eligible slot
with no returned bar advances only after successful bounded-request completion, acquisition and
verification of every page, valid pagination termination, and sufficiently demonstrated provider
aggregation/omission semantics support a durable `VERIFIED_EMPTY` fact. An empty response or absent
bar alone is insufficient. Unclassified missing eligible slots remain blocking, and calendar
changes make affected proofs stale.

Retention modes constrain it as follows:

- `EPHEMERAL` creates no durable historical watermark;
- `TTL` permits a watermark only while all supporting data remains unexpired and present;
- `SUBSCRIPTION_BOUND` permits it only while the dataset and entitlement remain active and retained;
- `DURABLE_AUTHORIZED` permits durable advancement; and
- `SYNTHETIC_UNRESTRICTED` permits durable test watermarks.

`PROHIBITED` cannot enter the protocol. Expiration, purge, missing files, failed re-verification, or
subscription termination invalidates or rolls back affected coverage and watermarks in the same
coordinated state update. A watermark never advances across a known or expected unresolved gap.

### Crash recovery

Recovery runs before new work while holding the writer lease:

- Before raw persistence, retry the bounded request under a new attempt.
- A crash-left transient transport spool is never adopted as raw evidence; validate its managed
  target, delete it under policy, and retry only if still eligible.
- After raw persistence but before publication, create or reuse the fixed batch context and replay
  from verified immutable raw artifacts.
- An incomplete staging directory is never queried; discard it after validation or rebuild it from
  raw artifacts.
- After atomic rename but before SQLite commit, the deterministic final directory is an orphan.
  Reopen and verify it, then adopt it through the normal SQLite transaction; otherwise quarantine
  it and replay. The watermark has not advanced.
- A crash during the SQLite transaction leaves either all or none of batch visibility, coverage,
  gaps, and watermark committed.
- After that commit but before run-summary completion, recovery recognizes the terminal request
  outcome and completes aggregate bookkeeping without republishing data.
- A final destination with the expected ID but different bytes is a conflict, not a retry success;
  quarantine and investigation precede replay.

These rules provide idempotent durable effects under retry, not exactly-once provider delivery.

## Consequences

- A visible watermark always has verified, currently retained canonical evidence behind it.
- Crash recovery has deterministic inputs and decisions at every filesystem/SQLite boundary.
- Canonical batches are immutable, so provider corrections are auditable rather than destructive.
- Query services must use the verified catalog instead of globbing every Parquet file under the
  private root.
- Checksums, manifests, overlap checks, and recovery add work, but they make no-op retry and repair
  behavior testable.

## Alternatives considered

- **Advance `MAX(timestamp)` after download:** rejected because maximum time says nothing about
  gaps, publication durability, stream dimensions, or retention.
- **Commit SQLite before Parquet publication:** rejected because a committed watermark could point
  to missing canonical data.
- **Treat every published file as queryable:** rejected because an orphan or partially verified
  batch must not affect analytical results.
- **Claim exactly-once ingestion:** rejected because provider calls, filesystem publication, and
  SQLite cannot participate in one end-to-end transaction.
- **Use a distributed transaction or message broker:** rejected as disproportionate for a local,
  single-writer Phase 2 system.
