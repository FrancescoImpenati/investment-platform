# ADR 0002: Provider-native raw data, Parquet analytics, and DuckDB queries

- **Status:** Accepted
- **Date:** 2026-08-14
- **Phase:** 0 — Foundation
- **Refined by:** ADR 0008 for the Phase 2 operational-store technology

## Context

Provider evidence must remain auditable, while normalized bars and future features need efficient
columnar scans. Mutable workflow state has different transactional requirements from historical
analytical data.

## Decision

- Persist each provider response as an immutable provider-native raw artifact with a sanitized
  manifest, stable batch ID, byte count, and SHA-256 checksum.
- Use partitioned Parquet as canonical analytical storage for normalized/curated market datasets
  and future observed feature history.
- Use DuckDB in-memory to query Parquet; do not persist a competing DuckDB copy of the same bars.
- Keep a future operational store separate. A relational database, likely PostgreSQL, may later own
  watermarks, job state, retries, alerts, application configuration, portfolios, orders, or users.

ADR 0008 retains this separation and selects SQLite, rather than PostgreSQL, for the local,
single-user Phase 2 ingestion state. PostgreSQL remains deferred until topology or concurrency
requirements justify a server.

Parquet is not declared the source of truth for the entire application.

## Consequences

- Raw evidence can be reprocessed after normalization rules change.
- Analytical storage remains portable and efficient without a server.
- Phase 0 provides artifact-level replay protection, not an atomic transaction across raw,
  normalization, Parquet partitions, and a future watermark.
- Corrections may create competing observation versions under different batch IDs until a later
  reconciliation policy selects a current/as-of view.

## Alternatives considered

- **PostgreSQL for all data now:** deferred; it adds operations and is not needed for local
  analytical scans in Phase 0.
- **Persistent DuckDB as a second canonical copy:** rejected because two authoritative copies of the
  same bars would create synchronization ambiguity.
- **Convert every raw payload to Parquet:** rejected because conversion would no longer preserve the
  exact provider response.
- **One Parquet file per instrument:** rejected because a large universe would create excessive
  small files and high-cardinality partitions.
