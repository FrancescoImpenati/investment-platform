# ADR 0005: Deterministic analytics before AI interpretation

- **Status:** Accepted
- **Date:** 2026-08-14
- **Phase:** 0 — Foundation

## Context

Returns, volatility, correlations, rankings, breadth, and thresholds are numerical facts that must
be reproducible and testable. Continuously asking an LLM to calculate or scan every instrument
would be expensive, difficult to audit, and less reliable than deterministic code.

## Decision

Preserve this dependency direction:

```text
data -> deterministic analytics -> Market State -> optional AI interpretation -> strategy -> execution
```

Feature definitions are parameterized and distinct from forecasts. Future checkpoint and threshold
logic evaluates deterministic Market State changes before any optional AI layer is invoked. AI may
explain, summarize, connect narratives, or assist research; it does not replace quantitative
calculation.

## Consequences

- Numerical outputs can be unit-tested, versioned, reproduced, and audited.
- Later AI calls receive smaller, higher-signal context selected by deterministic change detection.
- Phase 0 needs only feature-definition contracts and architectural boundaries, not a feature
  executor, Market State, checkpoints, or agents.

## Alternatives considered

- **LLM-computed metrics:** rejected because results would be harder to reproduce and validate.
- **Continuous LLM monitoring of the full universe:** rejected because deterministic thresholds are
  cheaper and more reliable.
- **Agent framework in Phase 0:** deferred until stable data, analytics, and Market State exist.
