# Investment Intelligence Platform

Investment Intelligence Platform is a Python-first, local-first foundation for collecting,
validating, storing, and analyzing market data. The long-term goal is a daily research system that
combines deterministic quantitative analytics with later, clearly separated AI interpretation.

Phase 0 and **Phase 1 — Provider Bake-off** are implemented and approved. The approved Phase 2
design now has an offline implementation checkpoint: its private-root guard, retention-aware
control plane, SQLite state, calendar, ingestion modes, recovery protocol, and CLI are implemented
and exercised with synthetic data. Controlled Alpaca persistence and the Phase 2 pull-request gate
are still pending, so Phase 2 is not complete or approved as a whole. This is not an investment
product or live trading system.

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

### Implemented in the Phase 2 offline checkpoint

- physically external private data root with a sentinel and fail-closed path validation;
- explicit test, ci, development, private_research, and demo profiles;
- machine-readable, exact provider/dataset retention policy enforced across acquisition,
  persistence, query, quarantine, watermark, export, and purge boundaries;
- standard-library SQLite operational state for runs, requests, attempts, artifacts, coverage,
  gaps, errors, budgets, leases, retention, and watermarks;
- maintained XNYS calendar snapshots, deterministic request planning, bounded backfill/update/repair,
  atomic canonical publication, catalog-driven DuckDB queries, and restart recovery;
- non-interactive data-root, backfill, update, repair, resume, status, verify, and retention commands;
- synthetic end-to-end and fault-injection coverage, including restart, incremental no-op, repair,
  publication recovery, and retention invalidation.

### Designed and pending within Phase 2

- controlled live acceptance for Alpaca historical SIP US stock bars at 1d and 5m;
- AAPL restart, incremental-extension, and repair evidence followed by the bounded Phase 1 sample;
- activation of an external scheduler only after manual live acceptance;
- final full review, Phase 2 pull request, and CI confirmation.

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
history; it is not the source of truth for every kind of application state. The Phase 2 offline
implementation assigns mutable ingestion state to local SQLite while DuckDB remains an in-process
query engine over cataloged, verified Parquet. Controlled live-data acceptance of this separation
is still pending.

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

The Phase 2 control plane is available through `uv run --locked investment-platform --help`.
Private commands require an explicit `private_research` profile and a separately initialized data
root. See the [living-ingestion operator guide](docs/operations/living-ingestion.md); do not run its
live examples until the documented gate is satisfied.

## Repository layout

```text
src/investment_platform/   Python package and domain/data foundations
tests/                     Unit and integration tests
data/sample/               Synthetic or explicitly redistributable examples only
docs/architecture/         Design document and ADRs
docs/data/                 Storage and data-contract documentation
docs/governance/           Redacted public governance records
docs/operations/           Private-runtime procedures and sanitized acceptance status
docs/research/             Provider selection, frozen design, and empirical evidence
PLAN.md                    Phase 0 implementation contract
PLAN_PHASE_2.md            Phase 2 implementation contract; offline checkpoint implemented
AGENTS.md                  Root-wide repository instructions for Codex
```

The Phase 1 runner proved an external temporary root with cleanup. Phase 2 now implements a
mandatory, absolute, dedicated private root outside Git with a platform sentinel and exact
managed-path validation. No root is selected or initialized implicitly. The full Alpaca support
evidence must live only under that private root; the repository contains only the
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
checkpoint. Phase 2 is the active retention-aware living historical-store implementation. Its
offline control plane is implemented, while controlled live acceptance, final review,
and the Phase 2 pull request remain open. Deterministic analytics and Market State follow only
after Phase 2; checkpoints/dashboard, AI interpretation, and strategy work remain later.

## Disclaimer

This project is for software-engineering, data-research, and educational purposes. It does not
provide investment advice, recommendations, or assurances of market performance. Market data may
be delayed, incomplete, corrected, or subject to provider licensing restrictions.
