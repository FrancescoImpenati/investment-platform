# Investment Intelligence Platform

Investment Intelligence Platform is a Python-first, local-first foundation for collecting,
validating, storing, and analyzing market data. The long-term goal is a daily research system that
combines deterministic quantitative analytics with later, clearly separated AI interpretation.

This repository is currently at **Phase 0 — Foundation**. It is not an investment product, a live
trading system, or a complete market-data pipeline.

## Current status

### Implemented in Phase 0

- a typed domain foundation for instruments, universes, price bars, corporate actions, provenance,
  and parameterized feature definitions;
- a vendor-neutral provider contract with bounded time requests and immutable raw-payload
  boundaries;
- append-safe raw artifacts with manifests, checksums, and batch collision protection;
- a canonical Polars representation, vectorized quality flags, partitioned Parquet storage, and
  in-memory DuckDB queries;
- tests, locked Python tooling, CI, architecture documentation, and durable repository guidance.

Phase 0 uses synthetic fixtures only. It does not connect to a production data provider.

### Planned

- a provider bake-off on 10–20 securities using Massive, a second provider selected during the
  study, and yfinance only as a sanity check;
- provider-specific normalization, backfill, incremental update, repair/reconciliation, and
  persistent ingestion watermarks;
- calendar-aware completeness checks for holidays, DST, and early closes;
- deterministic feature computation, Market State, checkpoints, alerts, and an updateable
  dashboard;
- forecasting, strategies, backtesting, AI-assisted interpretation, and—much later—paper trading
  and broker integration.

PostgreSQL, schedulers, real providers, dashboards, agents, and trading integrations are not part
of Phase 0.

## Architecture

```text
Provider
  -> immutable provider-native raw artifact
  -> provider-specific normalization
  -> validation and quality flags
  -> canonical analytical Parquet datasets
  -> in-memory DuckDB queries
  -> deterministic analytics
  -> Market State
  -> optional AI interpretation
```

Parquet is authoritative for analytical datasets such as normalized market bars and future feature
history; it is not the source of truth for every future kind of application state. Mutable state
such as watermarks, job attempts, alerts, portfolios, or orders may later belong in a transactional
store. DuckDB remains an in-process analytical engine over Parquet in Phase 0.

See the [Design Document](docs/architecture/design-v0.1.md),
[architecture decisions](docs/architecture/adr/), and
[storage layout](docs/data/storage-layout.md) for the durable design.

## Quickstart

Requirements:

- Python 3.13.14;
- [uv](https://docs.astral.sh/uv/).

Create the locked environment:

```bash
uv sync --locked --all-groups
```

Run the complete local verification suite:

```bash
uv lock --check
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked mypy src tests
uv run --locked pytest
uv build
```

Phase 0 requires no API credentials. `.env.example` contains empty placeholders for later provider
work; the application does not load it yet.

## Repository layout

```text
src/investment_platform/   Python package and domain/data foundations
tests/                     Unit and integration tests
data/sample/               Synthetic or explicitly redistributable examples only
docs/architecture/         Design document and ADRs
docs/data/                 Storage and data-contract documentation
docs/research/             Provider bake-off template and later research
PLAN.md                    Phase 0 implementation contract
AGENTS.md                  Root-wide repository instructions for Codex
```

Real market data is stored only in ignored local paths such as `data/raw/` and
`data/normalized/`.

## Data, secrets, and licensing

- Never commit API keys, `.env` files, credentials, complete authenticated URLs, or authorization
  headers.
- Treat external market data as private unless redistribution rights are explicitly documented.
- Only synthetic or explicitly redistributable data may be committed under `data/sample/`, with
  provenance and licensing notes.
- Public source code and private/licensed market data are separate concerns.

No open-source license has been selected for the repository. Until a license is added, the code
should not be assumed to be open for reuse or redistribution.

## Roadmap

Phase 0 establishes testable contracts and local analytical storage. The next step is the provider
bake-off described by the unexecuted
[provider quality report template](docs/research/provider_quality_report.md). Later phases add
incremental ingestion and trading-calendar semantics, deterministic analytics and Market State,
then checkpoints/dashboard capabilities, and only afterward AI interpretation and strategy work.

## Disclaimer

This project is for software-engineering, data-research, and educational purposes. It does not
provide investment advice, recommendations, or assurances of market performance. Market data may
be delayed, incomplete, corrected, or subject to provider licensing restrictions.
