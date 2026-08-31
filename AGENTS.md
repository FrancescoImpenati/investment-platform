# Repository instructions

## Mission and current boundary

- Build a Python-first, local-first Investment Intelligence Platform for reproducible market-data
  research.
- Preserve the dependency direction: data -> deterministic analytics -> Market State -> optional
  AI interpretation -> strategy -> execution.
- Phase 0 and Phase 1 are implemented and approved. Phase 2 living ingestion is designed in
  `docs/architecture/phase-2-living-ingestion.md` but is not implemented yet. Never describe
  Designed or Planned behavior as Implemented.
- Phase 2 implementation is limited to the contract in `PLAN_PHASE_2.md`, initially Alpaca
  historical SIP US stock bars at `1d` and `5m` after the dataset's strict historical-age gate. Do
  not add new provider adapters, internal schedulers, feature execution, dashboards, agents,
  strategy/backtesting engines, or trading integrations unless a later task explicitly changes
  scope.
- Read `docs/architecture/design-v0.1.md`, the Phase 2 design, and the relevant ADR before changing
  architecture. `PLAN.md` remains the historical Phase 0 contract; `PLAN_PHASE_2.md` is the next
  implementation contract.

## Architectural invariants

- Keep a modular monolith with domain logic separated from provider, filesystem, and query I/O.
- Use stable internal UUIDs for instruments. Tickers and provider identifiers are external and may
  change over time.
- Store timestamps as timezone-aware UTC. Use `America/New_York` to interpret US sessions, not as
  the persistence timezone. Treat intervals as half-open `[start, end)`.
- Keep event/observation time, `available_at`, `retrieved_at`, and `ingested_at` distinct. Never
  invent `available_at` from ingestion time.
- Persist provider-native raw payloads immutably before downstream use. Raw payload APIs must not
  require arbitrarily large content to live entirely in memory.
- Parquet is authoritative for analytical datasets; DuckDB queries those datasets in-process. Do
  not use either as the operational-state database. Phase 2 assigns local operational state to
  SQLite without copying canonical bars into it.
- Durable private research data belongs under an absolute, validated, sentinel-marked root outside
  Git. `.gitignore` is only a secondary defense.
- Enforce retention with an exact provider-by-dataset policy, separate from
  `LicenseClassification`. Unknown datasets fail closed, and a watermark is valid only while its
  verified supporting observations remain retained and present.
- Provider-specific behavior stays behind the provider boundary. Business and analytics code must
  not import vendor SDKs.
- Flag questionable observations without silently dropping or rewriting recoverable input.
- Compute returns, volatility, correlations, ranks, thresholds, and similar facts with deterministic
  testable code, never with an LLM.

## Layout

- `src/investment_platform/instruments/`: instrument and universe domain contracts.
- `src/investment_platform/data/`: time, provenance, provider, validation, and storage boundaries.
- `src/investment_platform/features/`: feature definitions only until execution is authorized.
- `tests/unit/` and `tests/integration/`: meaningful behavior tests; no network access.
- `docs/architecture/`: long-lived design and decisions.
- `docs/data/`: canonical data/storage rules.
- `docs/governance/`: redacted public governance records; full evidence stays private.
- `docs/research/`: provider evaluation and research artifacts.
- `data/sample/`: small synthetic or explicitly redistributable data only.

Do not create empty packages for future subsystems.

## Development policy

- Baseline Python is 3.13.14; use `uv` and keep `uv.lock` committed.
- Prefer standard-library functionality before adding dependencies. Every production dependency
  needs a concrete runtime use and documented rationale; ask before adding one when scope is not
  explicit.
- Use Polars for columnar transformations, Parquet for analytical persistence, DuckDB for local
  analytical queries, and Pydantic for serializable boundary/domain contracts.
- Do not add overlapping tools or libraries without evidence: no parallel formatter/linter stack,
  Pandas, database server, framework, or provider SDK by default. The planned exchange-calendar
  dependency must pass the explicit Phase 2 dependency gate before it is added.
- Preserve public contracts deliberately. Update tests and documentation when behavior or schema
  changes.

## Required checks

Run from the repository root:

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

Use focused tests while iterating, then run the complete suite before handoff.

## Data, security, and licensing

- Never commit secrets, API keys, `.env`, authorization headers, authenticated URLs, account data,
  or provider credentials. Request metadata and manifests must be sanitized.
- Never commit real/private/licensed payloads under `data/raw`, `data/normalized`, `data/curated`,
  or `data/features`; these paths are ignored by Git. Phase 2 real data must instead be physically
  outside the repository under the validated private root.
- Never commit operational state, private logs, quarantine content, or full governance evidence.
- Treat unclassified external data as private. Commit samples only with synthetic origin or explicit
  redistribution rights and provenance.
- Do not add a code license without an explicit project decision.

## Definition of Done

- The implementation matches the current phase and relevant ADRs without speculative
  infrastructure.
- New behavior has meaningful tests, including failure and time/provenance edge cases where
  applicable.
- Formatting, lint, typing, tests, build, and `git diff --check` pass.
- No secret or real market-data file is tracked; ignored/tracked data paths are verified when they
  change.
- README, design/data docs, and ADRs reflect any changed public behavior or architectural decision.
- The final diff is reviewed for accidental scope growth, generated artifacts, and unrelated edits.
