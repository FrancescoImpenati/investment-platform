# ADR 0004: Stable identity and explicit point-in-time semantics

- **Status:** Accepted
- **Date:** 2026-08-14
- **Phase:** 0 — Foundation

## Context

Tickers change, providers use different identifiers, universe membership evolves, and information
may become available after the market event it describes. Conflating these concepts would create
survivorship and look-ahead bias in later research and backtests.

## Decision

- Give each `Instrument` a stable internal UUID. Store ticker, FIGI, provider IDs, and similar values
  as separate temporal identifiers.
- Represent `UniverseMembership` and market-data/request intervals as half-open `[start, end)`.
- Persist aware timestamps in UTC and use `America/New_York` only to interpret US trading sessions.
- Keep event/observation time, `available_at`, `retrieved_at`, and `ingested_at` separate. Leave
  `available_at` null when unknown.
- Preserve session and adjustment state explicitly; model split, dividend, and ticker change as
  corporate actions rather than relying on one adjusted-close field.
- Defer calendar-aware completeness rules to a later phase with a real exchange calendar.

## Consequences

- A ticker change does not break an instrument's history.
- Point-in-time universe queries and future as-of analysis remain representable.
- UTC storage avoids ambiguous instants while explicit session metadata preserves market meaning.
- Consumers must choose a policy for missing availability timestamps instead of receiving a
  fabricated value.

## Alternatives considered

- **Ticker as primary key:** rejected because tickers are neither globally unique nor permanent.
- **Naive or provider-local timestamps:** rejected because DST and cross-provider comparison become
  ambiguous.
- **Use ingestion time as availability time:** rejected because it manufactures false point-in-time
  knowledge.
- **Hard-code 09:30–16:00 completeness:** rejected because holidays and early closes require an
  exchange calendar.
