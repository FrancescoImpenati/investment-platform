# Phase 0 Implementation Plan — Investment Intelligence Platform

**Status:** Phase 0 implemented and verified
**Target version:** 0.1.0
**Last review:** 2026-08-17

## 1. Outcome

Build a Python-first, local-first modular-monolith foundation that demonstrates:

```text
provider contract
→ immutable raw artifact
→ canonical representation
→ quality flags
→ Parquet analytical storage
→ DuckDB query
```

Phase 0 prepares for a living, periodically updated system without implementing production
ingestion. Provider requests must support bounded intervals so future backfill, incremental update,
and repair/reconciliation do not require a redesign.

Parquet is authoritative for canonical analytical datasets, not for all future application state.
DuckDB is an in-memory analytical engine over Parquet. Mutable watermarks, jobs, retries,
checkpoints, portfolio state, and similar operational concerns may use a transactional store in a
later roadmap phase; no operational database is introduced now.

## 2. Frozen decisions

- Python 3.13 baseline; `.python-version` pins 3.13.14 and `requires-python` is
  `>=3.13,<3.14`.
- `uv` with a committed `uv.lock`; Hatchling build backend.
- Runtime: Pydantic 2, `polars[pyarrow]`, DuckDB 1, and `tzdata`.
- Development: pytest, Ruff, and mypy strict.
- GitHub Actions CI on push and pull request.
- Internal instrument IDs are UUIDs; ticker is never a primary key.
- Stored timestamps are timezone-aware UTC; America/New_York interprets US sessions.
- Price-bar intervals, provider requests, and membership validity are half-open `[start, end)`.
- Raw provider payloads are immutable and provider-native.
- No code license is added in Phase 0; the README records that licensing is undecided.
- No branch, commit, push, remote, or default-branch change is part of the implementation.

## 3. Repository foundation

Create only purposeful modules:

```text
src/investment_platform/
├── instruments/
│   └── models.py
├── data/
│   ├── market_time.py
│   ├── models.py
│   ├── provenance.py
│   ├── providers/base.py
│   ├── storage/raw.py
│   ├── storage/market_bars.py
│   └── validation/bars.py
└── features/
    └── models.py

tests/
├── unit/
└── integration/
```

Required package `__init__.py` files contain a concise docstring and intentional exports. Do not
create `analytics`, `agents`, `api`, `checkpoints`, `events`, `ingestion`, `normalization`,
`strategies`, `scheduler`, or `dashboard` placeholders.

Root and documentation artifacts:

- `README.md`, `AGENTS.md`, `pyproject.toml`, `uv.lock`, `.python-version`, `.env.example`, and
  `.gitignore`;
- Design Document v0.1;
- five concise ADRs covering local-first modular monolith, analytical storage, provider boundary,
  identity/time/point-in-time semantics, and deterministic analytics before AI;
- canonical storage layout documentation;
- a clearly unexecuted `docs/research/provider_quality_report.md` template;
- `data/sample/README.md`; no external or binary dataset.

`README.md` must distinguish `Implemented` from `Planned`, explain installation and verification,
state that market data and secrets are not public code, include the investment-advice disclaimer,
and describe the repository's undecided code license.

`AGENTS.md` must remain concise and include mission, invariants, layout, exact quality commands,
dependency policy, data/security/licensing rules, and Definition of Done. Detailed design remains
under `docs/`.

## 4. Toolchain and CI

Configure:

- project version `0.1.0` and Hatchling with the `src` layout;
- dependency major ranges in `pyproject.toml`, with exact transitive resolution in `uv.lock`;
- Ruff format plus a focused lint set (`E`, `F`, `I`, `UP`, `B`, `SIM`, `RUF`), target `py313`,
  line length 100;
- mypy strict for `src` and `tests`, without global `ignore_missing_imports`;
- pytest strict configuration and separate unit/integration markers;
- no Black, isort, Flake8, pre-commit, tox, nox, Pandas, NumPy, SciPy, or Plotly.

Create one read-only GitHub Actions job on Ubuntu:

- `actions/checkout` v7.0.1 pinned to
  `3d3c42e5aac5ba805825da76410c181273ba90b1`, with credentials persistence disabled;
- `astral-sh/setup-uv` v8.1.0 pinned to
  `08807647e7069bb48b6ef5acd8ec9567f424441b`;
- uv 0.11.32, Python 3.13.14, cache enabled;
- locked sync, lock check, Ruff format/check, mypy, pytest, and package build;
- no matrix, publishing, deployment, or secrets.

## 5. Domain contracts

Use frozen Pydantic models with `extra="forbid"` for serializable domain and metadata contracts.
Datetime validators reject naive values and normalize aware values to UTC.

### Instruments and universes

- `Instrument`: UUID, asset class, name, optional primary currency and MIC, and temporal external
  identifiers.
- `InstrumentIdentifier`: type/namespace, value, optional provider, `valid_from`, and exclusive
  `valid_to`.
- `Universe`: UUID, name, optional description/source.
- `UniverseMembership`: universe UUID, instrument UUID, `[valid_from, valid_to)`, optional
  `available_at`, and `ingested_at`; include `is_active_on(date)`.

Tests prove that two instruments may share a ticker while retaining distinct identities and that a
ticker change does not change `instrument_id`.

### Provenance and data source

- `DataSource`: stable source ID, provider, dataset, sanitized logical endpoint, and license
  classification (`private`, `redistributable`, `sample`, `synthetic`). Unknown classification
  defaults to `private`.
- `RawBatchMetadata`: caller-supplied batch UUID, `DataSource`, `retrieved_at`, media type, safe file
  extension, optional provider request ID, and sanitized request metadata.
- Request metadata rejects sensitive key names such as authorization, token, key, password, or
  secret; raw headers and complete query URLs are never written to the manifest.

### Raw payload boundary

Do not use `RawBatch.payload: bytes`. Separate serializable metadata from the payload resource:

```python
class RawPayload(Protocol):
    def open_binary(self) -> ContextManager[BinaryIO]: ...


@dataclass(frozen=True, slots=True)
class RawBatch:
    metadata: RawBatchMetadata
    payload: RawPayload
```

Provide a simple `BytesRawPayload` for fixtures and small mock pages. `RawBatchStore` consumes the
reader with bounded-size reads; callers are not required to materialize arbitrary payloads in
memory. Do not implement HTTP streaming, async readers, resumable downloads, backpressure, or
multipart handling.

### Market data and corporate actions

- `Timeframe`: `1d`, `5m`; later values are additive.
- `TradingSession`: regular, pre-market, post-market, overnight, unknown.
- `AdjustmentState`: unadjusted, split-adjusted, split-and-dividend-adjusted,
  provider-adjusted-unknown, unknown.
- `PriceBar`: instrument UUID, timeframe, start/end, required finite OHLC, nullable volume/VWAP/
  currency, session, adjustment state, source ID, source batch UUID, optional provider record ID,
  nullable `available_at`, `retrieved_at`, `ingested_at`, and quality flags.
- Price bars use Float64 in the columnar schema. Cash dividends and split ratios use `Decimal` in
  domain models.
- Corporate actions are a discriminated union of `SplitAction`, `DividendAction`, and
  `TickerChangeAction`, with common instrument, effective date, availability, ingestion, and
  provenance fields.

### Features

- `FeatureDefinition`: name, version, input timeframe, description, and JSON-scalar parameters such
  as `{"lookback": 20}`.
- Do not implement `FeatureValue`, feature execution, forecasts, or signals.

## 6. Provider abstraction and incremental compatibility

Define a synchronous, runtime-checkable protocol:

```python
class MarketDataProvider(Protocol):
    @property
    def provider_name(self) -> str: ...

    def get_instruments(self, *, as_of: date | None = None) -> Iterable[RawBatch]: ...
    def get_bars(self, request: BarRequest) -> Iterable[RawBatch]: ...
    def get_corporate_actions(self, request: CorporateActionRequest) -> Iterable[RawBatch]: ...
```

`ProviderInstrumentRef` pairs an internal UUID with the provider identifier. `BarRequest` includes
one or more refs, timeframe, session, requested adjustment state, and an aware UTC `[start, end)`
interval. `CorporateActionRequest` is likewise bounded by instrument refs and dates.

Do not add an `IngestionMode` enum: backfill, incremental, and repair are future update-planner
intents that all map to bounded provider requests. The protocol returns `Iterable`, not a fully
materialized `Sequence`, so provider pages can later be consumed progressively.

The fake provider lives in tests. Do not add a vendor SDK, real normalizer, async API, pagination
state machine, retry, caching, or rate limiting.

## 7. Raw storage and artifact idempotency

`RawBatchStore.write(batch) -> RawArtifact` uses this layout:

```text
data/raw/provider=<provider>/dataset=<dataset>/retrieval_date=YYYY-MM-DD/
└── batch_id=<uuid>/
    ├── payload.<provider-native-extension>
    └── manifest.json
```

Implementation behavior:

1. Read the payload in bounded chunks into a sibling temporary directory while calculating SHA-256
   and byte count.
2. Create a deterministic, sanitized manifest containing metadata, checksum, and size.
3. For a new batch ID, atomically rename the complete temporary directory into place.
4. For the same batch ID with identical source/request identity, metadata, and checksum, remove the
   temporary candidate and return the existing `RawArtifact` with `created=False`.
5. For the same batch ID with different relevant metadata or checksum, remove the temporary
   candidate and raise `BatchCollisionError`.
6. Never overwrite an existing raw artifact.

This is artifact-level idempotency only. Phase 0 does not guarantee exactly-once processing across
raw, normalization, multiple Parquet partitions, and watermark advancement.

## 8. Canonical Parquet storage and DuckDB

Define one explicit Polars schema for canonical price bars. UUIDs and enums are UTF-8 strings,
timestamps are UTC logical timestamps, prices/volume are Float64, and flags are lists of strings.

`ParquetBarStore.append(frame, batch_id)`:

- rejects an empty input;
- validates/coerces the canonical schema;
- groups rows by timeframe/year/month;
- sorts partition groups deterministically;
- writes Zstd files to temporary paths and publishes deterministic
  `part-<batch_id>-<ordinal>.parquet` files;
- preflights all target paths and refuses a repeated batch target without adding a suffixed copy or
  overwriting data;
- keeps `instrument_id` as a column rather than a high-cardinality filesystem partition.

Layout:

```text
data/normalized/price_bars/
└── timeframe=<tf>/year=YYYY/month=MM/part-<batch_id>-<ordinal>.parquet
```

`ParquetBarStore.query(BarQuery)` opens an in-memory DuckDB connection, reads the relevant Parquet
files with schema-union support, applies parameterized filters, and returns a Polars DataFrame.
An empty store returns an empty canonical frame. No `.duckdb` database is persisted.

A repeated logical interval under a different batch ID may still create competing observation
versions. Cross-batch reconciliation, winner/as-of policy, compaction, multi-writer transactions,
and atomic watermark commit are deliberately deferred.

## 9. Validation pattern

Pydantic handles structural validity at record boundaries. Vectorized Polars rules annotate data
quality without dropping or reordering observations.

`validate_bars(frame, policy) -> BarValidationResult` returns:

- the same row count and order with a `quality_flags` list column;
- structured issues containing row/key, code, severity, and message;
- counts by flag.

Initial rules:

- OHLC consistency;
- negative volume;
- duplicates keyed by instrument, source, timeframe, session, adjustment state, and bar start;
- non-positive prices under a configurable equity-default policy;
- incorrect duration for a `5m` bar.

The module does not implement a winner policy, calendar-aware missing bars, holidays, or early-close
logic. Raw input remains recoverable when a record cannot be normalized structurally.

## 10. Time and trading-calendar boundary

Provide focused helpers for aware-to-UTC normalization and **nominal** US RTH timezone/DST
conversion using `zoneinfo`/`tzdata`. Test ordinary full-session bounds in winter and summer. Name
and document the helper so it cannot be mistaken for a source of completeness expectations. Do not
assume every session is a normal 09:30–16:00 day when deciding data completeness.

The provider bake-off and later ingestion layer must evaluate exchange holidays, DST, early closes,
expected sessions, and calendar-aware missing-bar detection. No trading-calendar package is added
in Phase 0.

## 11. Documentation of future operation

The Design Document, README roadmap, and relevant ADRs must preserve these future requirements
without creating code placeholders:

- dataset-level backfill, incremental update, and repair/reconciliation;
- a dataset-specific watermark key containing provider/dataset/instrument/timeframe plus every
  dimension that changes the logical stream—for price bars, session scope and adjustment state
  where distinct; it represents contiguous coverage through an exclusive boundary and advances
  only after successful canonical persistence;
- scheduler/manual trigger → update planner → provider → raw → normalization → validation →
  analytical storage → affected features → Market State, which feeds the updateable dashboard and,
  independently, checkpoint/threshold evaluation;
- dataset-specific cadence;
- an updateable daily/5-minute dashboard rather than static manual snapshots;
- deterministic threshold evaluation before optional AI interpretation;
- a future operational store, likely PostgreSQL, only when mutable transactional state exists.

Do not create `IngestionState`, watermark, scheduler, planner, operational-store, Market State,
checkpoint, dashboard, or AI implementation in Phase 0.

## 12. Data and security rules

`.gitignore` excludes `.env` files except `.env.example`, virtual environments, caches, coverage,
build outputs, DuckDB files, and real/private `data/raw`, `data/normalized`, `data/curated`, and
`data/features`. `data/sample` remains publishable only for synthetic or explicitly redistributable
content.

`.env.example` states that Phase 0 requires no real credentials and contains only empty future
provider placeholders. Do not add a dotenv/settings dependency before a runtime consumer exists.

## 13. Tests

Unit tests cover:

- stable instrument identity, temporal identifiers, and half-open universe membership;
- timezone rejection/normalization and US DST;
- nullable point-in-time fields and distinct availability/retrieval/ingestion times;
- price-bar and corporate-action structural contracts;
- parametric `FeatureDefinition`;
- bounded provider requests and a paged fake conforming to the protocol;
- in-memory payload source plus a test source that rejects unbounded `read()` calls;
- OHLC, volume, duplicate, duration, and non-positive-price flags with row preservation.

Integration tests cover:

- raw write, checksum, manifest sanitization, and atomic layout;
- identical raw replay as a no-op and conflicting replay as `BatchCollisionError`;
- deterministic/no-overwrite normalized batch behavior;
- Polars → Parquet → DuckDB → Polars round-trip, including UTC, nulls, lists, filters, and empty
  store behavior;
- no network access.

## 14. Acceptance and review

Run:

```text
uv lock --check
uv sync --locked --all-groups
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked mypy src tests
uv run --locked pytest
uv build
git diff --check
```

Use `git check-ignore` to prove that secrets and real data are ignored while `.env.example` and
`data/sample` are visible. Because the repository has no base commit, inventory and review every
created file in addition to checking Git status.

The implementation handoff must contain:

A. files and components created;
B. architectural decisions and alternatives;
C. deliberately unimplemented capabilities;
D. commands and results;
E. concise repository tree;
F. technical debt/open issues;
G. unchanged Phase 1 provider bake-off proposal;
H. 5–10 learning notes.

## 15. Explicitly out of scope

- real provider, S&P 500 download, provider-specific normalization;
- persistent watermark or `IngestionState`;
- scheduler, update planner, orchestrator, retry, rate limiter, or job recovery;
- PostgreSQL or an operational-store abstraction;
- HTTP streaming engine, resumable transfer, or backpressure;
- complete trading calendar and calendar-aware missing bars;
- cross-batch deduplication/winner policy and complete workflow idempotency;
- feature executor, Market State, checkpoints, alerts, dashboard, news, or AI agents;
- forecasting, strategies, backtesting, broker integration, paper/live trading;
- Docker, cloud deployment, microservices, queues, or Kubernetes.

The next roadmap step remains the provider bake-off on 10–20 securities using Massive, one second
provider selected at that time, and yfinance only as a sanity check. This review does not add or
renumber phases.
