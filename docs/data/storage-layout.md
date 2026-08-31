# Data storage layout

- **Status:** Phase 0–1 layout implemented; Phase 2 private-runtime layout designed
- **Last review:** 2026-08-31

This document owns the physical and logical storage contract. It distinguishes existing storage
primitives from the Phase 2 design so a path shown here is never mistaken for implemented runtime
behavior or permission to retain a dataset.

## 1. Status by responsibility

| Responsibility | Authority / format | Status |
| --- | --- | --- |
| Provider-native raw evidence | Payload plus JSON manifest | Implemented primitive |
| Canonical price bars | Parquet | Implemented primitive |
| Analytical queries | In-memory DuckDB over Parquet | Implemented |
| Durable live workflow using those stores | Raw plus verified canonical batches | Designed for Phase 2 |
| Operational state | SQLite | Designed for Phase 2 |
| Retention catalog/enforcement | Exact provider-by-dataset policy | Designed for Phase 2 |
| Curated/adjusted datasets | Parquet | Future |
| Deterministic feature history | Parquet | Future |

The current RawBatchStore and ParquetBarStore accept caller-supplied roots. The Phase 1 live runner
used them only under an external temporary directory that was deleted. No persistent live-data
root, SQLite database, canonical batch catalog, or watermark exists in the current implementation.

Parquet is authoritative for analytical observations. SQLite will coordinate mutable ingestion
state without copying OHLCV values. DuckDB will continue to query Parquet in-process.

## 2. Public/private boundary

### Public Git repository

May contain:

- source, tests, schemas, migration source, and documentation;
- non-secret policy/configuration catalogs;
- synthetic fixtures and explicitly redistributable samples;
- permitted non-substitutive aggregate reports;
- redacted governance records.

Must not contain:

- provider credentials or account data;
- licensed raw, normalized, curated, or feature values;
- operational SQLite files or journals;
- private logs or quarantine;
- complete support correspondence or other private evidence.

### Private runtime

Owns:

- licensed artifacts permitted by exact dataset policy;
- operational state;
- staging and quarantine;
- sanitized local logs;
- full private governance evidence;
- future personal portfolio/broker data only after separate design.

Repository ignore rules remain a secondary accident barrier. They are not the primary separation.

## 3. External root contract

The Phase 2 private_research profile requires INVESTMENT_PLATFORM_DATA_ROOT to identify a
dedicated absolute local path outside the repository.

Before any write or deletion, the implementation will:

- resolve root and repository paths;
- reject the repository, its ancestors and descendants, filesystem/drive root, home/profile,
  general temporary/system locations, UNC/network roots, and link/reparse escapes;
- validate the direct-child sentinel schema, purpose, immutable root UUID, and canonical path;
- verify that the exact target resolves beneath an owned namespace in the same root;
- fail closed when configuration or identity is absent, moved, malformed, or ambiguous.

Initialization accepts only a new or empty dedicated directory and creates:

~~~text
.investment-platform-root.json
~~~

The sentinel contains no credential. Destructive actions may remove only exact cataloged relative
artifacts after a second validation. They never recursively delete the root.

See [ADR 0006](../architecture/adr/0006-external-private-data-root.md).

## 4. Phase 2 logical root

Namespaces are created lazily:

~~~text
<PRIVATE_DATA_ROOT>/
├── .investment-platform-root.json
├── raw/
├── normalized/
├── curated/                 # future
├── features/                # future
├── staging/
├── operational/
│   ├── ingestion.sqlite3
│   ├── ingestion.sqlite3-wal
│   ├── ingestion.sqlite3-shm
│   └── locks/
├── logs/
├── quarantine/
└── governance/
    └── evidence/
~~~

Differences from the initial proposal:

- staging is explicit and shares a filesystem with normalized publication targets, allowing one
  atomic directory rename per complete canonical batch;
- the platform sentinel is a direct child of the root and identifies the directory before any
  mutation;
- SQLite journal sidecars are shown because they are part of operational state while present;
- no backup directory is added beside the primary data because that would not protect against loss
  of the same filesystem;
- no portfolio subtree is created before portfolio behavior is designed.

curated and features remain reserved logical destinations only. Phase 2 does not create or write
them merely to make the tree look complete.

## 5. Immutable raw artifacts

### 5.1 Implemented foundation layout

~~~text
raw/
└── provider=<provider>/
    └── dataset=<dataset>/
        └── retrieval_date=YYYY-MM-DD/
            └── batch_id=<uuid>/
                ├── payload.<provider-native-extension>
                └── manifest.json
~~~

provider, dataset, and extension are validated path segments. No complete URL, token, or arbitrary
caller path is interpolated.

The implemented manifest records:

- caller-owned batch ID and source/dataset;
- retrieval timestamp in UTC;
- logical endpoint, media type, safe provider request ID, and sanitized request metadata;
- payload filename, byte count, and SHA-256 checksum;
- provenance/license classification.

The implemented writer:

1. consumes a reopenable payload in bounded reads;
2. writes payload and manifest to a sibling temporary directory;
3. atomically renames the complete directory;
4. treats the same batch ID and identical metadata/bytes as a no-op;
5. treats the same batch ID with different data as a collision;
6. never overwrites raw evidence.

### 5.2 Phase 2 refinement

Provider adapters currently generate new UUIDs for ordinary attempts. Phase 2 will add a durable
raw artifact identity binding:

- deterministic request specification;
- stable page ordinal/normalized relation;
- canonical media type/content encoding, payload checksum, and byte count.

The exact request/page/content replay therefore converges across distinct process attempts. Changed
provider bytes become a new immutable artifact rather than colliding with a request-only ID.
Provider request IDs and other attempt-varying metadata remain sanitized provenance and do not
participate in artifact identity.

The first publisher writes an immutable raw manifest with identity-bearing request/page/content
fields and fixed first-persistence provenance. A later identical attempt verifies the stable fields
and payload, reuses the directory, and records its own attempt/retrieval/provider-request metadata
in SQLite artifact-observation links. It neither rewrites the manifest nor requires volatile
attempt metadata to match.

Large durable responses will use file-backed/spooled payloads where needed so the raw boundary's
bounded-memory contract is exercised by live transport.

Before immutable raw publication, bounded transient transport verifies that response observations
fit the active policy age and request bounds. Unauthorized overfetch is erased from transient
storage and never enters raw, quarantine, normalization, or coverage.

Raw persistence remains earlier than normalization. A request is not raw-complete until pagination
terminates correctly and every page is present and verified.

## 6. Canonical price bars

### 6.1 Implemented Phase 0–1 layout

~~~text
normalized/price_bars/
└── timeframe=<tf>/
    └── year=YYYY/
        └── month=MM/
            └── part-<batch_id>-<ordinal>.parquet
~~~

The implementation:

- uses an explicit canonical Polars schema and Zstandard compression;
- retains instrument UUID as a column instead of a high-cardinality partition;
- writes deterministic batch part names;
- rejects repeated target paths;
- queries discovered parts through an in-memory DuckDB connection.

This provides in-process rollback if one append call raises, but publication of multiple files is
not a single crash-atomic filesystem event. Direct glob discovery can also see an orphan after an
abrupt process crash.

### 6.2 Designed Phase 2 batch layout

Living ingestion changes publication, not the authority of Parquet:

~~~text
normalized/price_bars/
└── provider=<provider>/
    └── dataset=<dataset>/
        └── batches/
            └── batch_id=<canonical-batch-id>/
                ├── manifest.json
                └── timeframe=<tf>/
                    └── year=YYYY/
                        └── month=MM/
                            └── part-0000.parquet
~~~

One canonical batch may span months and multiple physical parts, but all parts and the manifest
live below one immutable batch directory. The full directory is written and verified under:

~~~text
staging/canonical-batches/<canonical-batch-id>.tmp/
~~~

and published with one same-filesystem atomic rename.

The canonical manifest records:

- request specification hash and canonical batch identity;
- ordered raw artifacts and checksums;
- schema, normalizer, validator, and calendar versions;
- ordered relative file paths, sizes, and checksums;
- per-stream row counts and half-open bounds;
- PUBLISHABLE/BLOCKED outcomes plus validation, duplicate, and revision summaries;
- the fixed batch-context creation time.

All Parquet parts are closed and hashed before `manifest.json` is written last as the staging
completion record. It is not a catalog visibility marker.

After raw completion and before normalization, SQLite persists a batch context that fixes the
batch ID, ordered inputs, processing versions, and `ingested_at`/manifest creation time. Replays
reuse it. The policy snapshot that authorized a run is catalog provenance in SQLite, not volatile
immutable content.

Request-instance, attempt, and run links also remain in SQLite so a later repair can reuse identical
content without changing the manifest.

An existing final batch path is a no-op only after manifest and content verify identically.
Different content at the same identity is an integrity conflict.

A multi-stream batch may publish only independently PUBLISHABLE streams, but its manifest records
the outcome of every requested stream. Fatal validation blocks the affected stream's entire
bounded interval, creates no coverage for it, and makes the request PARTIAL or FAILED; recoverable
observations remain with quality flags. VERIFIED means the published contents are internally
valid, not that every requested stream succeeded.

If no stream is publishable, no canonical directory is created and SQLite records only the failed
request and blocking outcomes; coverage and watermark remain unchanged.

## 7. Canonical schema and version semantics

The existing price-bar columns remain the implemented foundation:

| Column | Representation | Nullable | Meaning |
| --- | --- | --- | --- |
| instrument_id | UTF-8 UUID | No | Stable internal identity |
| timeframe | UTF-8 enum | No | 1d or 5m |
| timestamp_start | UTC timestamp | No | Inclusive observation start |
| timestamp_end | UTC timestamp | No | Exclusive observation end |
| open, high, low, close | Float64 | No | Finite canonical OHLC |
| volume, vwap | Float64 | Yes | Provider values when available |
| currency | UTF-8 | Yes | Price currency when known |
| session | UTF-8 enum | No | Session classification |
| adjustment_state | UTF-8 enum | No | Explicit adjustment semantics |
| source_id | UTF-8 UUID | No | Data source |
| raw_batch_id | UTF-8 UUID | No | Raw provenance in the current schema |
| provider_record_id | UTF-8 | Yes | Provider record identity when supplied |
| available_at | UTC timestamp | Yes | Earliest known availability |
| retrieved_at | UTC timestamp | No | Provider retrieval |
| ingested_at | UTC timestamp | No | Local ingestion |
| quality_flags | List of UTF-8 | No | Non-destructive findings |

Phase 2 may deliberately evolve the schema to support batch/revision identity. Any change requires:

- a versioned schema;
- compatibility tests over existing synthetic Parquet;
- updated documentation;
- explicit migration or read compatibility;
- no silent reinterpretation of old files.

Canonical observation identity is stream plus timestamp_start/timestamp_end. Exact semantic replay
does not create a second visible observation. Changed same-provider values create immutable
revisions; no winner is chosen across providers.

## 8. Catalog-driven visibility

Phase 2 DuckDB queries must not glob all filesystem files. Query planning asks SQLite for explicit
paths satisfying:

- canonical batch status VERIFIED;
- manifest/integrity status current;
- dataset policy currently permits use;
- retention has not expired;
- requested stream and interval.

DuckDB reads that path list and returns Polars results. The Parquet values remain authoritative;
SQLite decides only whether an immutable publication is verified and visible.

A directory published before a crash but not cataloged is invisible. Recovery verifies and adopts
it or quarantines it.

## 9. Operational state

Designed location:

~~~text
operational/ingestion.sqlite3
~~~

The SQLite schema will own:

- migrations and writer lease;
- ingestion runs, bounded requests, attempts, retries, errors, and progress;
- raw artifacts and attempt-observation links, fixed batch contexts, and canonical
  batch/file/stream-outcome catalogs;
- exact stream keys;
- coverage segments, gaps, and materialized watermarks;
- provider budget observations;
- dataset-policy snapshots/status;
- retention and purge runs.

It stores sanitized metadata, hashes, counts, UTC bounds, statuses, and paths relative to the
private root. It never stores credentials, payload bytes, or canonical OHLCV copies.

Connections use foreign keys, WAL mode, synchronous FULL, a bounded busy timeout, and short write
transactions. WAL requires a local same-host filesystem. SQLite-aware backup is required; copying
only the main database while live WAL state exists is not a backup protocol.

See [ADR 0008](../architecture/adr/0008-sqlite-operational-state-store.md).

## 10. Coverage and watermark references

Coverage links exact streams and half-open intervals to:

- verified canonical batch and raw evidence;
- policy snapshot/current retention validity;
- calendar snapshot;
- row/artifact counts;
- verification/gap state.

The watermark is derived from retained contiguous VERIFIED or VERIFIED_EMPTY coverage and stores
an exclusive frontier. Contiguity follows eligible sessions/bar slots in the recorded calendar,
not every wall-clock instant: overnight closures, weekends, and holidays are NOT_APPLICABLE.
VERIFIED_EMPTY requires a complete bounded request plus provider semantics supporting a no-trade
omission. An unclassified missing eligible slot blocks the frontier. The watermark is never
inferred from a maximum Parquet timestamp.

If a raw/canonical file is absent, corrupt, expired, quarantined, or no longer permitted, readers
lose visibility and the supporting coverage/watermark is invalidated before use.

EPHEMERAL data creates no historical durable watermark. TTL and SUBSCRIPTION_BOUND watermarks are
valid only while supporting data exists and its rights remain active. DURABLE_AUTHORIZED and
SYNTHETIC_UNRESTRICTED may support durable watermarks in their permitted environments.

See [ADR 0009](../architecture/adr/0009-watermark-publication-and-recovery-protocol.md).

## 11. Quarantine and staging lifecycle

staging:

- contains only incomplete or not-yet-published platform-owned artifacts;
- is never query-visible;
- inherits dataset retention;
- may hold bounded attempt-scoped transport spools that are never raw evidence and are deleted on
  rejection or restart before other work;
- is inspected first during recovery;
- is removed only by exact validated target.

quarantine:

- contains conflicting, corrupt, or fatally invalid artifacts requiring investigation;
- preserves allowed provenance without making it canonical;
- inherits the original dataset expiry/termination rules;
- cannot retain PROHIBITED or expired data;
- remains private and never becomes a fixture source.

## 12. Policy-driven deletion

Expiration/termination is state-first:

1. mark dataset and affected canonical batches unavailable;
2. invalidate coverage and watermarks;
3. catalog exact root-relative purge targets;
4. commit SQLite;
5. revalidate root/sentinel and delete targets idempotently;
6. verify absence and complete the purge record.

A crash cannot leave a watermark valid merely because physical deletion is incomplete. The
platform never deletes by provider/user-supplied glob or recursive root removal.

## 13. Private governance evidence

The intended locator for the complete Alpaca support evidence is:

~~~text
governance/evidence/alpaca/ticket-342496/
~~~

It is private, outside Git, and may contain the original correspondence and provenance metadata.
The repository contains only
[the redacted record](../governance/data-rights/alpaca-historical-sip.md).

## 14. Repository data policy

Legacy repository-local data/raw, data/normalized, data/curated, and data/features paths remain
ignored as a secondary defense and for backwards-compatible tests. They are not approved as the
private_research root.

Only small synthetic or explicitly redistributable content may be committed under data/sample,
with origin and rights documentation. Provider-shaped test fixtures must remain hand-authored
synthetic data.

No current document or directory creates permission to persist real data. Permission comes from an
active exact dataset policy enforced by future Phase 2 code.
