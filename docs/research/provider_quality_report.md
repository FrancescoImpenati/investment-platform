# Provider quality report — Phase 1 intermediate state

> **Status: FULL OFFLINE QUALITY GATE PASSED; LIVE BAKE-OFF PENDING**
>
> Alpaca Basic is approved as Provider 2. The Massive and Alpaca adapters, provider-specific
> normalizers, comparison harness, and synthetic offline fixtures are implemented. At this
> intermediate checkpoint no provider API has been called, no credential has been used, and no real
> or licensed market-data payload has been downloaded. Therefore this report makes **no final
> primary-provider recommendation**.

The approved research basis is recorded in
[Provider 2 selection](provider-2-selection.md). The frozen sample, half-open windows, adjustment
matrix, request budget, calendar oracle, and stop rules are in the
[provider bake-off design](provider_bakeoff_design.md).

The four sections below deliberately separate what this repository has measured, what providers
state in official documentation, project interpretation, and questions that remain open.

## Observed evidence

### What was observed offline

The following evidence comes only from deterministic tests against hand-authored synthetic,
provider-shaped JSON fixtures:

- On 2026-08-20 the complete Phase 0 + Phase 1 offline suite collected and passed **157 tests** on
  Python 3.13.14. `uv lock --check`, locked dependency sync, Ruff formatting and lint, strict mypy,
  the complete pytest suite, package build, and `git diff --check` all passed.

- Both adapters construct bounded daily and 5-minute bar requests and corporate-action requests,
  emit unchanged paginated `RawBatch` payloads, retain sanitized request metadata, and expose
  typed authentication, entitlement, rate-limit, HTTP, transport, and malformed-response failures.
- Massive pagination rejects a `next_url` that changes origin, endpoint, request semantics, or
  carries a credential parameter. Alpaca pagination preserves page tokens and the requested feed
  identity. The shared transport does not follow redirects and bounds both success and error
  response bodies.
- Alpaca defaults explicitly to SIP, has an explicit old-data SIP entitlement preflight, and does
  not silently fall back to IEX.
- Provider-specific normalizers map synthetic Massive and Alpaca bars, splits, dividends, and
  ticker/name changes into Phase 0 canonical contracts. Malformed or canonically
  unrepresentable records remain available in the immutable raw artifact and produce diagnostics
  rather than being silently rewritten or discarded.
- Phase 1 strengthens request/response validation and provenance binding at the provider boundary.
  Those boundary checks do not change the Phase 0 canonical schemas or their invariants, and their
  fixture coverage is not evidence that every live provider case is representable.
- Synthetic Massive and Alpaca SIP integration paths both exercise the required flow: provider
  response -> immutable raw artifact -> integrity-checked replay -> provider-specific
  normalization -> canonical bars -> validation -> Parquet -> DuckDB query. The stored manifests
  do not contain the synthetic test credentials, and a replayed artifact is reverified before its
  bytes are exposed to normalization.
- The deterministic comparison harness measures observation availability, expected missing keys,
  duplicates, interval boundaries, session and adjustment state, currency, OHLC, volume, VWAP,
  splits, dividends, and ticker changes. Each discrepancy preserves raw-batch provenance and can
  be classified as definitional, adjustment, timing/session, missing observation, likely provider
  issue, or unresolved; ambiguous matches remain unresolved rather than being selected.
- A runtime summary can recover sanitized response status, page count, latency, and rate-limit
  fields from raw manifests when a live run eventually provides them.

The fixtures in `tests/fixtures/providers/` are synthetic. Their symbols, identifiers, values,
actions, request IDs, and pagination tokens were invented for tests; they are not observations
from Massive, Alpaca, Yahoo, an exchange, or an issuer.

### What has not been observed

No empirical statement can yet be made about either provider's:

- daily or 5-minute coverage, missing bars, duplicates, OHLC, volume, or VWAP accuracy;
- actual timestamp, DST, early-close, holiday, session, or daily-bar semantics;
- split, dividend, or adjusted-versus-unadjusted values;
- identifier continuity or the SQ-to-XYZ ticker transition;
- pagination size, batching behavior, latency, rate-limit headers, corrections, or service
  stability under live conditions;
- entitlement, historical depth, or permission to retain immutable raw artifacts for this use.

There are consequently no live raw batch IDs, row counts, discrepancy counts, latency samples, or
observed rate-limit measurements to report.

### Intermediate multidimensional evidence grid

| Dimension | Massive Basic | Alpaca Basic | Current evidence status |
| --- | --- | --- | --- |
| Data quality | Adapter and normalizer behavior verified with synthetic Massive-shaped pages | Adapter and normalizer behavior verified with synthetic Alpaca-shaped pages | **Live quality unknown**; no provider observation has been compared |
| Technical fit | Bounded requests, pagination, immutable raw batches, actions, failures, and normalization implemented | Multi-symbol bars, page tokens, feed provenance, SIP preflight implementation, assets/actions, failures, and normalization implemented | **Full offline repository gate passed**; live endpoint compatibility remains pending |
| Economics and scalability | Request budget can be estimated from documented Basic limits | Request budget can be estimated from documented Basic limits | **Documentation only**; no billing, throttling, or observed throughput evidence |
| Licensing and governance | Public material does not resolve durable immutable-raw retention | Public material does not resolve durable immutable-raw retention | **Blocking ambiguity**; no live retention until permission is confirmed |

This grid is a readiness assessment, not a provider score and not a winner selection.

## Provider documentation

Official sources were reviewed on 2026-08-19 and are summarized more fully, with links, in
[Provider 2 selection](provider-2-selection.md). These are provider claims, not observations from
the implemented adapters.

### Massive Basic

- Massive advertises a free Stocks Basic tier with five API calls per minute, two years of history,
  US stocks, end-of-day and minute aggregates, reference data, and corporate actions.
- Its custom-bars documentation supports daily bars and custom five-minute aggregates, labels an
  aggregate at interval start, omits a bar when there is no qualifying trade, and documents a
  maximum of 50,000 base aggregates per query.
- Aggregates are documented as split-adjusted by default, with an explicit unadjusted option;
  dividends are not part of that adjustment.
- Ticker reference, split, dividend, and ticker-event documentation exposes useful identifier and
  corporate-action fields, including CIK and FIGI when available.
- The public pricing language describes a rolling two-year availability window. It does not settle
  whether a response retrieved legitimately during that window may be retained indefinitely as a
  private immutable raw artifact after it ages out or after access ends.
- The public market-data terms impose personal/non-commercial, display, derived-use, and
  redistribution constraints unless another agreement applies. The exact fit for long-lived
  private raw retention in this platform remains ambiguous.

### Alpaca Basic

- Alpaca documents Basic as free, with US stock and ETF historical data since 2016, a restriction
  around the latest 15 minutes, and 200 historical API calls per minute.
- Historical-bars documentation supports daily and five-minute bars, multiple symbols, pagination,
  `raw`, `split`, `dividend`, `spin-off`, and `all` adjustment values, an `asof` mapping, and an
  explicit feed parameter.
- Corporate-actions and assets documentation exposes action families and provider identifiers,
  including an Alpaca UUID and other identifiers when present.
- Alpaca's market-data FAQ says historical SIP queries ending at least 15 minutes ago can be made
  without a paid subscription. A separate official historical-stock-data page describes IEX as the
  only feed usable without a subscription. The documents therefore do not establish that this
  particular Basic account will authorize the intended historical SIP request.
- “History since 2016” describes advertised data availability, not necessarily a contractual right
  to retain complete raw responses indefinitely. The reviewed agreements do not remove the
  platform's uncertainty about durable private raw retention after account or agreement changes.

### SIP versus IEX

SIP and IEX are not interchangeable experimental inputs. SIP is intended to represent consolidated
US market activity, while IEX is a single-venue feed. Substituting IEX after an SIP entitlement
failure would change coverage, volume, bar construction, and therefore the question being tested.
The approved adapter behavior is to preserve the requested feed in provenance and stop on SIP
denial rather than silently continue with IEX.

## Interpretation

### Provider 2 and current decision state

Alpaca Basic is the approved second bake-off provider. It was selected because the documented free
tier can plausibly exercise the required daily, five-minute, corporate-action, identifier,
adjustment, pagination, and error paths without a purchase, while providing a materially different
API and feed model from Massive.

The offline implementation uses the existing `MarketDataProvider` and canonical contracts while
adding stricter provider-boundary validation and provenance checks; it does not change the Phase 0
canonical model. Synthetic fixture success does not yet prove that the boundary is sufficient for
every live response shape or semantic case, nor that either provider's live data is more complete,
correct, economically sustainable, or legally suitable. Massive remains the required primary
*candidate*, not the recommended canonical provider. No primary or fallback recommendation is
justified before the live comparison and retention review.

### Interpretation by decision dimension

| Dimension | Current interpretation | What is needed for a decision |
| --- | --- | --- |
| Data quality | The harness is capable of preserving disagreement without automatically declaring a winner | Live paired raw batches over the preregistered symbols/windows, session oracle, and investigated discrepancies |
| Technical architecture | The implemented adapters use the Phase 0 boundary, with Phase 1 validation and provenance hardening confined to that boundary | Live confirmation that response shapes, pagination, timestamps, action date bases, and error behavior remain representable without a canonical-model change |
| Economics/scalability | The bounded experiment appears to fit documented free-tier request budgets; this is an estimate, not measured throughput | Rechecked current tiers, observed calls/pages/latency/limits, and a later Phase 2 scale model |
| Licensing/governance | Redistribution is out of scope and raw data must remain private; durable retention permission is not established | Account-specific agreement review or written provider clarification before retaining live raw data |
| International expansion | Neither implemented adapter currently proves a broad international cash-equity path | A later separately scoped provider evaluation; it must not distort the US-equity bake-off |

### Phase 2 recommendation: private data root

Phase 1 does not relocate the data root or change the Phase 0 storage contract. Based on the
licensing ambiguity exposed here, Phase 2 should evaluate formally requiring a configurable
**private data root outside the Git working tree**, on storage with access controls,
backup/retention rules, and an explicit deletion procedure. The existing
[storage-layout rule](../data/storage-layout.md) already says storage code must not assume the
repository's `data/` directory; using a caller-supplied private runtime path for any authorized
Phase 1 artifact is operator configuration, not a foundation mutation. The repository should retain
only sanitized manifests or aggregated results that the applicable agreement permits; provider
payloads must never be added as fixtures. No hard-coded external path or Phase 2 implementation is
introduced in this phase.

### Residual role of yfinance

yfinance remains only an optional sanity/reference check. It is not a production provider, a
canonical candidate, or an oracle. It has deliberately not been implemented in this stage:

- adding it would introduce Pandas/NumPy and overlap with the approved Polars-first dependency
  policy for a component that cannot become the production provider; and
- spending implementation effort on an unofficial reference path before the two approved live
  providers run would not resolve the current credential or retention blockers.

If used later, yfinance results must be labeled separately, must not bypass immutable-raw and
licensing rules, and agreement among three sources still must not be treated as proof of truth.

### Authorized stop

The Phase 1 live run is stopped before substantive provider download. Once credentials are supplied
through the approved environment variables, one bounded historical SIP entitlement preflight may
run even while retention rights remain ambiguous: its response body is discarded and no raw
artifact is persisted. A successful sanitized result is observed entitlement evidence only. The
later corporate-action access check is a separate endpoint check, and contractual retention review
is a separate governance gate. If retention is forbidden or remains unresolved, the substantive
bake-off and raw persistence remain stopped. No purchase, paid-plan activation, IEX substitution,
or architectural change is authorized by this intermediate report.

## Unresolved questions

1. **Credentials:** live access still requires `MASSIVE_API_KEY`, `APCA_API_KEY_ID`, and
   `APCA_API_SECRET_KEY`. Values must remain outside code, tests, logs, manifests, documentation,
   and Git.
2. **Massive retention:** does the applicable Basic agreement permit indefinite private retention
   of immutable raw payloads retrieved while they are within the rolling two-year access window?
3. **Alpaca retention:** does the applicable account agreement permit indefinite private retention
   of immutable historical SIP, assets, and corporate-action responses, including after an account
   or plan change?
4. **Alpaca SIP entitlement:** will the configured Basic account authorize one historical SIP query
   ending more than 15 minutes ago? If the preflight returns authentication or entitlement denial,
   the bake-off must stop; it must not retry the experiment on IEX.
5. **Alpaca corporate-action access:** after the SIP entitlement result, does a separate minimal
   check show that the configured account and region authorize the corporate-actions endpoint? SIP
   success does not answer this question, and neither endpoint result answers the contractual
   retention question.
6. **Retention-window gate:** immediately before retrieval, do all Massive request starts still fall
   inside the then-current advertised two-year window, and do Alpaca endpoints authorize the same
   fixed windows? A failed gate requires redesign/approval, not silent date movement.
7. **Live data quality:** what are the paired row counts, expected missing counts, duplicates,
   OHLC/volume/VWAP discrepancies, and their defensible classifications for the frozen sample?
8. **Time semantics:** how do both providers actually label daily bars, regular sessions, DST
   transitions, holidays, and the 2025 early closes? Is a full trading-calendar dependency required
   for Phase 2?
9. **Adjustment and actions:** do raw/split-adjusted bars, split ratios, dividends, and event dates
   agree definitionally, and which action-date fields cannot be represented canonically without
   losing necessary meaning?
10. **Identifiers:** can both providers resolve stable provider identifiers and reproduce the
   SQ-to-XYZ history without treating ticker text as instrument identity?
11. **Operational behavior:** what pagination, latency, error, rate-limit, and correction behavior
    is observed under the bounded run?
12. **Economics at Phase 2 scale:** after empirical page sizes and throughput are known, what plan
    would be required for bounded backfill, incremental ingestion, and repair? No such plan should
    be inferred from headline price alone.
13. **Final recommendation:** which provider, if either, should be primary for US equities, which
    should be fallback/cross-check, and what residual role should yfinance retain? This decision is
    explicitly deferred until the live evidence and governance gates are complete.
