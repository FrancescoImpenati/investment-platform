# ADR 0008: SQLite as the Phase 2 operational state store

- **Status:** Accepted
- **Date:** 2026-08-31
- **Phase:** 2 — Living Data Ingestion
- **Implementation status:** Implemented in Phase 2

## Context

Living ingestion must remember runs, bounded requests, retry state, stream coverage, gaps,
watermarks, provider budgets, errors, repair status, and retention lifecycle after a process or
machine restart. Parquet is authoritative for analytical datasets and DuckDB scans those files; they
do not supply the transactional workflow state required by ingestion.

The platform remains local, single-user, single-machine, and non-multi-tenant. Requiring a database
server would add deployment and recovery work without a demonstrated Phase 2 benefit.

## Decision

Use SQLite through Python's standard-library `sqlite3` module as the Phase 2 operational state
store. Its files live under the validated private root, initially at
`operational/ingestion.sqlite3`, with journals and sidecar files in the same directory.

The schema will track at least:

- schema migrations and the active writer lease;
- ingestion runs, logical requests, attempts, retries, errors, and progress;
- raw artifact catalogs with attempt/retrieval provenance links;
- fixed batch contexts, stream keys, published/verified canonical batches, per-stream outcomes,
  contiguous coverage, gaps, and watermarks;
- provider rate/budget observations needed for safe resume; and
- dataset-policy versions, entitlement/lifecycle state, purge, expiration, and repair status.

It will not contain copies of market-data bars, raw payload bytes, or a second analytical dataset.
Immutable raw artifacts and canonical Parquet remain on the filesystem. DuckDB continues to query
only the verified Parquet paths selected through the operational catalog.

Every connection will enable foreign keys, configure a bounded busy timeout, and use WAL journaling
with `synchronous=FULL`. Mutating workflows use explicit transactions and `BEGIN IMMEDIATE` when
claiming work or updating coordinated state. Constraints and stable identities enforce replay and
state-transition invariants rather than relying only on application checks.

Phase 2 supports one active ingestion writer per private root. A transactional lease row contains a
unique owner/run identity, acquisition and heartbeat times, and a bounded expiry. A conflicting
writer fails clearly. Recovery may reclaim an expired lease after recording the stale owner; it
must not steal a live lease. Read-only status and analytical queries may run concurrently when they
do not mutate workflow state.

Numbered, forward-only schema migrations will be implemented before the first durable live state is
accepted. Startup verifies the schema version and refuses unknown newer or partially applied
migrations. Migration and backup procedures must preserve the database together with its WAL state.

SQLite transactions coordinate only operational rows. They are not presented as atomic with
filesystem writes; ADR 0009 defines filesystem-first publication, verification, catalog commit, and
recovery across that boundary.

## Consequences

- Ingestion can resume after restart without operating a database server.
- Foreign keys, uniqueness constraints, and transactions give stronger crash behavior than loose
  manifests while keeping deployment local.
- WAL permits readers during the single writer's work, but the application still serializes
  ingestion mutations through its lease.
- A damaged or deleted operational database requires restore or deliberate reconstruction from
  verified artifacts; Parquet alone is not silently treated as current workflow state.
- File permissions and backups for `operational/` are part of private-root operations. Secrets are
  not stored in SQLite.

## Alternatives considered

- **PostgreSQL:** deferred until concurrent writers, remote access, multi-user operation, or another
  measured requirement justifies a server. None is in Phase 2.
- **JSON or manifest files as the state store:** rejected because multi-record transitions,
  uniqueness, crash recovery, and concurrent status reads would require rebuilding database
  behavior.
- **Persistent DuckDB for operational state:** rejected because it would mix transactional workflow
  ownership with its established role as an in-process analytical query engine over Parquet.
- **Parquet-only state:** rejected because immutable analytical files are unsuitable for leases,
  retries, state transitions, and small transactional updates.
