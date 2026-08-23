# Investment Intelligence Platform

Investment Intelligence Platform is a Python-first, local-first foundation for collecting,
validating, storing, and analyzing market data. The long-term goal is a daily research system that
combines deterministic quantitative analytics with later, clearly separated AI interpretation.

The approved Phase 0 foundation is frozen. **Phase 1 — Provider Bake-off** is in progress on its
feature branch. It is not an investment product, a live trading system, or a complete market-data
pipeline.

## Current status

### Approved Phase 0 foundation

- a typed domain foundation for instruments, universes, price bars, corporate actions, provenance,
  and parameterized feature definitions;
- a vendor-neutral provider contract with bounded time requests and immutable raw-payload
  boundaries;
- append-safe raw artifacts with manifests, checksums, and batch collision protection;
- a canonical Polars representation, vectorized quality flags, partitioned Parquet storage, and
  in-memory DuckDB queries;
- tests, locked Python tooling, CI, architecture documentation, and durable repository guidance.

### Phase 1 work in progress

- bounded standard-library HTTP adapters for Massive and Alpaca, with no provider SDKs;
- explicit Alpaca SIP/IEX source identity and no implicit feed fallback;
- provider-specific normalization with finite session oracles and explicit semantic-gap findings;
- a pairwise quality harness that preserves raw-batch provenance and does not average or select
  conflicting provider values;
- an intentional 16-security experiment design, sanitized synthetic fixtures, and fully offline
  deterministic tests.

Minimal transient access preflights succeeded for Alpaca historical SIP and Massive, without
persisting real payloads. The full 16-security dataset has not been downloaded. Alpaca remains the
approved comparison provider, not a canonical provider. Massive is a technically validated
candidate whose full real bake-off is blocked by standard Individual market-data licensing; its
implementation and synthetic evidence remain intact. Twelve Data Basic was considered as the
replacement Provider 1 but failed the no-purchase documentary gate because split and dividend
endpoints require Grow or above, so no Twelve Data adapter or live call was added.

### Remaining Phase 1 work

- approval of an eligible Provider 1 path, then a bounded full empirical bake-off against Alpaca
  SIP;
- measured provider-quality, technical-capability, economics, and licensing evidence;
- yfinance only as a bounded sanity check, never as a canonical production source;
- the final provider recommendation, Phase 1 report, CI-verified pull request, and human review.

Later phases may add:

- backfill, incremental update, repair/reconciliation, and persistent ingestion watermarks;
- calendar-aware completeness checks for holidays, DST, and early closes;
- deterministic feature computation, Market State, checkpoints, alerts, and an updateable
  dashboard;
- forecasting, strategies, backtesting, AI-assisted interpretation, and—much later—paper trading
  and broker integration.

PostgreSQL, schedulers, production ingestion orchestration, dashboards, agents, and trading
integrations are outside Phase 1.

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

Normal tests and CI require no API credentials or network. Opt-in live Phase 1 work reads
`MASSIVE_API_KEY`, `APCA_API_KEY_ID`, and `APCA_API_SECRET_KEY` from the process environment.
`.env.example` contains empty names for local setup, but the application does not load `.env`
files; secrets must never be committed or printed.

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

The foundation provides ignored local paths such as `data/raw/` and `data/normalized/`, but a
technical storage path is not permission to retain licensed data. When durable retention is
ambiguous but temporary private processing is permitted, Phase 1 may use an external temporary
data root for the complete raw-to-query pipeline and delete it at run end. A provider agreement
that restricts the required non-display processing still stops that provider's run. The bake-off
report evaluates a physically external private data root and durable provider-specific retention
rules as Phase 2 prerequisites.

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

Phase 0 establishes testable contracts and local analytical storage. Phase 1 follows the bounded
[provider bake-off design](docs/research/provider_bakeoff_design.md) and records progress in the
[provider quality report](docs/research/provider_quality_report.md). Later phases add incremental
ingestion and trading-calendar semantics, deterministic analytics and Market State, then
checkpoints/dashboard capabilities, and only afterward AI interpretation and strategy work.

## Disclaimer

This project is for software-engineering, data-research, and educational purposes. It does not
provide investment advice, recommendations, or assurances of market performance. Market data may
be delayed, incomplete, corrected, or subject to provider licensing restrictions.
