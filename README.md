# Investment Intelligence Platform

Investment Intelligence Platform is a Python-first, local-first foundation for collecting,
validating, storing, and analyzing market data. The long-term goal is a daily research system that
combines deterministic quantitative analytics with later, clearly separated AI interpretation.

Phase 0 and **Phase 1 — Provider Bake-off** are implemented and approved. Phase 2 living ingestion
is designed and planned, but no Phase 2 operational database, watermark, CLI, scheduler, backfill,
or durable live workflow is implemented yet. This is not an investment product or live trading
system.

## Current status

### Implemented and approved

- a typed domain foundation for instruments, universes, price bars, corporate actions, provenance,
  and parameterized feature definitions;
- a vendor-neutral provider contract with bounded time requests and immutable raw-payload
  boundaries;
- append-safe raw artifacts with manifests, checksums, and batch collision protection;
- a canonical Polars representation, vectorized quality flags, partitioned Parquet storage, and
  in-memory DuckDB queries;
- tests, locked Python tooling, CI, architecture documentation, and durable repository guidance;
- bounded standard-library HTTP adapters for Massive, Alpaca, and Twelve Data, with no provider
  SDKs;
- explicit Alpaca SIP/IEX source identity and no implicit feed fallback;
- provider-specific normalization with finite session oracles and explicit semantic-gap findings;
- a pairwise quality harness that preserves raw-batch provenance and does not average or select
  conflicting provider values;
- an intentional 16-security experiment design, sanitized synthetic fixtures, fully offline
  deterministic tests, and an opt-in ephemeral live runner;
- approved post-Phase 1 Databento research, with no adapter or persistent Databento data.

Minimal transient access preflights succeeded for Alpaca historical SIP, Twelve Data Basic, and
Massive. The complete bounded bar pipeline and comparison ran ephemerally for Alpaca SIP versus
Twelve Data Basic; yfinance remained an isolated sanity source. Real payloads and analytical data
were deleted after the runs and never entered Git. Massive remains a **technically validated
candidate — full real bake-off blocked by standard Individual market-data licensing**; its
implementation and synthetic evidence remain intact. Twelve Data corporate actions were not
upgraded: `/splits` and `/dividends` are unavailable on Basic, so actions were assessed
asymmetrically. See the [provider quality report](docs/research/provider_quality_report.md) for the
measured evidence and dataset-specific recommendation.

### Designed for Phase 2, not implemented

- physically external private data root with a sentinel and fail-closed path validation;
- explicit test, ci, development, private_research, and demo profiles;
- retention-aware exact provider/dataset policy;
- SQLite operational state for runs, requests, coverage, gaps, errors, and watermarks;
- crash-safe canonical batch publication and recovery;
- backfill, incremental update, repair, status, and verify commands;
- a maintained US exchange calendar and eventual external scheduler invocation.

See the [Phase 2 design](docs/architecture/phase-2-living-ingestion.md) and
[implementation plan](PLAN_PHASE_2.md). Feature execution, Market State, dashboards, agents,
strategies, backtesting engines, brokers, and trading integrations remain future work.

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
history; it is not the source of truth for every kind of application state. The accepted Phase 2
design assigns mutable ingestion state to local SQLite while DuckDB remains an in-process query
engine over verified Parquet. This separation is designed, not yet implemented.

See the [Phase 0 Design Document](docs/architecture/design-v0.1.md),
[Phase 2 design](docs/architecture/phase-2-living-ingestion.md),
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
uv sync --locked --all-groups
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked mypy src tests
uv run --locked pytest
uv build
```

Normal tests and CI require no API credentials or network. The existing opt-in live Phase 1 runner
reads `MASSIVE_API_KEY`, `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`, and `TWELVE_DATA_API_KEY` from
the process environment. `.env.example` contains empty names for local setup, but the application
does not load `.env` files; secrets must never be committed or printed.

## Repository layout

```text
src/investment_platform/   Python package and domain/data foundations
tests/                     Unit and integration tests
data/sample/               Synthetic or explicitly redistributable examples only
docs/architecture/         Design document and ADRs
docs/data/                 Storage and data-contract documentation
docs/governance/           Redacted public governance records
docs/research/             Provider selection, frozen design, and empirical evidence
PLAN.md                    Phase 0 implementation contract
PLAN_PHASE_2.md            Phase 2 implementation contract; implementation not started
AGENTS.md                  Root-wide repository instructions for Codex
```

The existing foundation accepts caller-supplied roots and the Phase 1 runner proved an external
temporary root with cleanup. Phase 2 now designs a mandatory, absolute, dedicated private root
outside Git with a platform sentinel, but that guard is not implemented yet. The full Alpaca
support evidence will live only under that private root; the repository contains only the
[redacted rights record](docs/governance/data-rights/alpaca-historical-sip.md).

Ignored repository-local paths remain a secondary accident barrier. They are not the Phase 2
security boundary and do not grant permission to retain data.

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

Phase 0 established testable contracts and local analytical storage. Phase 1 completed the bounded
[provider bake-off](docs/research/provider_quality_report.md), and the approved
[Databento evaluation](docs/research/databento-evaluation.md) remains a separate research
checkpoint. Phase 2 is the designed next implementation: a retention-aware living historical
store. Deterministic analytics and Market State follow only after Phase 2; checkpoints/dashboard,
AI interpretation, and strategy work remain later.

## Disclaimer

This project is for software-engineering, data-research, and educational purposes. It does not
provide investment advice, recommendations, or assurances of market performance. Market data may
be delayed, incomplete, corrected, or subject to provider licensing restrictions.
