# Data storage layout

This document defines the Phase 0 filesystem contract and the boundary between immutable provider
evidence, analytical datasets, and future mutable application state. Paths are relative to a
caller-supplied data root; storage code must not assume the repository's `data/` directory.

## Storage responsibilities

| Layer | Format | Purpose | Phase 0 status |
| --- | --- | --- | --- |
| Raw | Provider-native payload + JSON manifest | Immutable evidence and provenance | Implemented foundation |
| Normalized | Parquet | Canonical provider-neutral analytical records | Implemented for price bars |
| Curated / adjusted | Parquet | Reconciled or transformed analytical views | Planned |
| Features | Parquet | Observed deterministic feature history | Planned |
| Operational state | Future transactional store | Watermarks, jobs, retries, alerts, configuration, portfolios, orders | Deliberately absent |

Parquet is authoritative for analytical datasets. It is not an application-wide source of truth.
DuckDB is an in-process query engine over Parquet and does not persist a competing copy in Phase 0.

## Immutable raw artifacts

```text
data/raw/
└── provider=<provider>/
    └── dataset=<dataset>/
        └── retrieval_date=YYYY-MM-DD/
            └── batch_id=<uuid>/
                ├── payload.<provider-native-extension>
                └── manifest.json
```

`provider`, `dataset`, and the extension are validated safe path segments; no caller-supplied path
or complete URL is interpolated into the filesystem path.

The manifest records:

- stable batch ID and source/dataset identity;
- retrieval timestamp in UTC;
- logical endpoint, media type, provider request ID when available, and sanitized request metadata;
- payload filename, byte count, and SHA-256 checksum;
- data-license classification.

Authorization headers, cookies, credentials, API keys, tokens, passwords, secrets, and complete
authenticated/query URLs must never enter a manifest.

The store consumes an openable binary payload in bounded reads. It writes payload and manifest to a
temporary sibling directory, then publishes the completed batch directory atomically. A raw
artifact is never overwritten:

- same batch ID and identical relevant metadata/checksum: return the existing artifact as a no-op;
- same batch ID with different metadata or checksum: raise a collision error;
- provider correction: write a new batch, preserving the previous evidence.

This is artifact-level idempotency. Phase 0 does not provide an atomic commit spanning raw data,
normalization, multiple analytical partitions, and a future watermark.

## Canonical normalized price bars

```text
data/normalized/price_bars/
└── timeframe=<tf>/
    └── year=YYYY/
        └── month=MM/
            └── part-<batch_id>-<ordinal>.parquet
```

Partitioning by timeframe/year/month supports pruning without creating one directory per
instrument. `instrument_id` remains a column. Part names are deterministic for a batch; writers
must preflight all targets and must not overwrite or invent suffixed duplicates.

Files use Zstandard compression and one explicit canonical schema:

| Column | Parquet/Polars representation | Nullable | Meaning |
| --- | --- | --- | --- |
| `instrument_id` | UTF-8 UUID | No | Stable internal identity |
| `timeframe` | UTF-8 enum | No | `1d` or `5m` in Phase 0 |
| `timestamp_start` | UTC timestamp | No | Inclusive observation start |
| `timestamp_end` | UTC timestamp | No | Exclusive observation end |
| `open`, `high`, `low`, `close` | Float64 | No | Finite canonical OHLC values |
| `volume` | Float64 | Yes | Provider volume when available |
| `vwap` | Float64 | Yes | Provider VWAP when available |
| `session` | UTF-8 enum | No | Regular, pre/post-market, overnight, or unknown |
| `currency` | UTF-8 | Yes | Price currency when known |
| `source_id` | UTF-8 UUID | No | Data source identity |
| `raw_batch_id` | UTF-8 UUID | No | Link to immutable raw provenance |
| `provider_record_id` | UTF-8 | Yes | Provider observation identifier |
| `adjustment_state` | UTF-8 enum | No | Explicit price-adjustment semantics |
| `available_at` | UTC timestamp | Yes | Earliest known availability, when known |
| `retrieved_at` | UTC timestamp | No | Provider retrieval time |
| `ingested_at` | UTC timestamp | No | Local ingestion/persistence time |
| `quality_flags` | List of UTF-8 strings | No | Non-destructive quality annotations |

The code-level schema is authoritative if a future migration deliberately changes these fields;
such a change must update this document and preserve an explicit compatibility/migration policy.

## Query and version semantics

DuckDB opens in-memory, discovers Parquet files with schema-union support, applies parameterized
filters, and returns a Polars frame. An empty dataset yields an empty canonical frame rather than a
persistent empty database.

The logical duplicate key for initial quality checks includes instrument, source, timeframe,
session, adjustment state, and bar start. A repeated interval under a different batch ID may
represent an accidental duplicate, delayed record, or provider correction. Phase 0 flags competing
observations but does not choose a winner. Cross-batch reconciliation, as-of/current views,
compaction, schema migration, and multi-writer coordination are later concerns.

## Future operational state

Mutable state such as ingestion watermarks, job attempts, retry state, scheduler configuration,
alerts, checkpoints, portfolios, orders, executions, and users should not be forced into analytical
Parquet files. A later phase may introduce a transactional relational store—likely PostgreSQL—when
those requirements exist. That store will coordinate operation and reference analytical artifacts;
it will not duplicate the full historical bar dataset by default.

## Repository data policy

`data/raw/`, `data/normalized/`, `data/curated/`, and `data/features/` are ignored by Git. Only small
synthetic or explicitly redistributable material may be committed under `data/sample/`, with source
and license documentation. See `data/sample/README.md`.
