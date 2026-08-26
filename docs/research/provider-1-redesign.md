# Phase 1 Provider 1 redesign — Twelve Data documentary preflight and amendment

> **Status: ORIGINAL GATE SUPERSEDED; BARS-FIRST BASIC RUN APPROVED AND EXECUTED**
>
> On 2026-08-23 the Phase 1 Provider 1 redesign was approved after Massive's standard Individual
> market-data license blocked the full non-display bake-off. Twelve Data was evaluated as the
> proposed operational replacement. Its free Basic tier passes the licensing, US-bars, history,
> and request-budget checks, but it cannot supply the required corporate-action dataset without a
> paid Grow-or-higher entitlement. That finding stopped implementation under the original
> symmetric corporate-action gate.

On 2026-08-24 the user approved a bars-first amendment without purchase or upgrade. The full
empirical bar comparison became Alpaca SIP versus Twelve Data Basic, while corporate actions
became a provider-capability assessment rather than a parity requirement. Twelve Data Basic
`/splits` and `/dividends` remained unavailable and were not called; no equivalent official
ticker-change-history endpoint was identified. The adapter, synthetic fixtures, offline tests,
minimal preflight, and bounded 16-security run were then completed. Final evidence belongs in the
[provider quality report](provider_quality_report.md); the gate analysis below is preserved as the
chronological decision record.

## Decision context

Massive remains implemented and documented as:

> **technically validated candidate — full real bake-off blocked by standard Individual
> market-data licensing**

Its adapter, normalizer, synthetic fixtures, offline tests, transient preflight evidence,
engineering findings, and licensing assessment remain part of Phase 1. No further Massive
market-data processing is authorized without explicit additional rights.

Alpaca SIP remained the approved comparison provider. At the original documentary gate, Twelve
Data was only a proposal. The subsequent approved amendment made it the operational bars provider
for Phase 1 without making it canonical or treating its partial-volume US feed as SIP-equivalent.

## Evidence method

Official Twelve Data sources were accessed on **2026-08-23**. The classifications below distinguish:

- **[DOC]** facts stated by Twelve Data;
- **[EST]** calculations from documented limits and the frozen experiment;
- **[INTERPRETATION]** project conclusions;
- **[UNRESOLVED]** questions the documentation does not answer.

At this documentary-gate stage, no Twelve Data account, API key, endpoint, payload, or trial symbol
was used, and no provider code, fixture, dependency, or environment-variable contract had yet been
added. The later approved execution is documented separately in the final report.

## Twelve Data documentary gate

| Requirement | Documentary result | Evidence and consequence |
| --- | --- | --- |
| Individual personal/internal/non-commercial use | **PASS [DOC]** | Individual plans expressly cover personal, internal, non-commercial projects, education, research, development, and testing. |
| Internal non-display processing | **PASS [DOC]** | Basic pricing explicitly includes internal non-display usage. The Terms permit processing and storage for authorized Internal Use. |
| Temporary raw and analytical processing | **PASS [INTERPRETATION]** | A private temporary raw -> normalization -> Parquet/DuckDB -> comparison -> cleanup run fits the documented internal processing grant. |
| Durable storage/retention | **CONDITIONAL [DOC]** | Storage is allowed only for the subscription/documentation-permitted duration; no concrete durable US-feed period was found. Termination requires deletion within 30 days. |
| US listed-equity universe | **PASS [DOC]** | The default feed covers all listed US equities; OTC is excluded. |
| Five-minute bars | **PASS [DOC]** | `/time_series` supports `5min`; documented US intraday history begins 2022-12-15. |
| Daily bars | **PASS [DOC]** | `/time_series` supports `1day`; the US market page advertises daily history from 1980. |
| Frozen 2025 windows | **PASS [INTERPRETATION]** | Every designed daily and intraday bound lies inside the documented history. |
| Split and dividend events | **FAIL [DOC]** | `/splits` and `/dividends` require Grow individual or above and cost 20 credits per symbol. Their calendar endpoints are also Grow-or-higher. |
| Ticker-change history | **NOT DOCUMENTED [UNRESOLVED]** | Current reference endpoints expose present symbols, but no official endpoint was found for reconstructing the SQ-to-XYZ change history. |
| Adjustment semantics | **PASS WITH LIVE QUESTION [DOC]** | `/time_series` documents `none`, `splits`, `dividends`, and `all`, defaulting to splits. Separate guidance says daily data are split-adjusted and intraday data unadjusted; the interaction needs empirical verification. |
| Basic request budget | **PASS FOR BARS [EST]** | Basic provides eight API credits per minute and 800 per day; the frozen bar/reference plan is estimated at 161 credits. |
| Original symmetric no-purchase bake-off | **FAIL [INTERPRETATION]** | Corporate-action endpoint entitlement is paid, so Basic could not support the then-required symmetric daily + 5-minute + action comparison. The later bars-first amendment superseded this parity requirement. |

Official sources:

- [Terms of Use](https://twelvedata.com/terms), last updated 2026-01-01;
- [Individual pricing](https://twelvedata.com/pricing);
- [commercial and personal usage](https://support.twelvedata.com/en/articles/5332349-commercial-and-personal-usage);
- [US equities market data](https://support.twelvedata.com/en/articles/9935903-us-equities-market-data);
- [API documentation](https://twelvedata.com/docs);
- [historical prices](https://support.twelvedata.com/en/articles/5656039-how-to-get-historical-prices);
- [batch requests](https://support.twelvedata.com/en/articles/5203360-batch-api-requests);
- [adjustment semantics](https://support.twelvedata.com/en/articles/5179064-are-the-prices-adjusted).

## Licensing and retention assessment

This is an engineering assessment, not legal advice.

| Use | Classification | Phase 1 treatment |
| --- | --- | --- |
| API access on an active Individual tier | **CLEARLY PERMITTED** | Subject to endpoint/tier entitlement |
| Personal/internal/non-commercial research | **CLEARLY PERMITTED** | Matches the private educational project |
| Basic internal non-display analytics | **CLEARLY PERMITTED** | Explicit Basic pricing feature |
| Temporary raw, normalization, Parquet, and DuckDB | **CLEARLY PERMITTED for authorized data** | Ephemeral external data root remains the conservative mode |
| Durable raw/reversible normalized bars | **AMBIGUOUS / NEEDS CLARIFICATION on duration** | Treat reversible normalization as provider Data, not freely derived data |
| Non-reversible aggregate Derived Data | **CLEARLY PERMITTED during subscription** | Repository evidence must remain non-substitutive and attributed |
| Redistribution/resale | **CLEARLY RESTRICTED without add-on/agreement** | Out of scope |
| Public display | **RESTRICTED / ATTRIBUTION REQUIRED** | Do not publish underlying values |
| Termination/expiration | **DELETE DATA WITHIN 30 DAYS** | Durable operational history cannot rely on the default terms alone |

The licensing gate passes only for datasets included in the tier. It does not convert paid
corporate-action endpoints into Basic entitlements.

## US feed coverage is an experimental dimension

Twelve Data explicitly states that its default US equities feed:

- covers **all listed US equity symbols**;
- represents approximately **5% of total US trading volume** for the default real-time/intraday
  feed; and
- is sourced from venues that do not require an additional exchange license.

The same official guide separately describes next-day historical/EOD data as consolidated across
all exchanges and covering 100% of US volume. It does not establish that old intraday bars become
consolidated. Therefore:

- Twelve Data five-minute data must not be represented as SIP-equivalent;
- volume and VWAP differences versus Alpaca SIP would first be classified as a **venue/feed
  difference**, not automatically a provider-quality error;
- daily and intraday coverage require separate interpretation; and
- `/vwap` is a separate technical-indicator endpoint and cannot be assumed definitionally equal to
  Alpaca's native per-bar SIP VWAP.

## Frozen bar-only request estimate

Basic charges `/time_series` by symbol even when symbols are batched. Eight-symbol batches fit one
eight-credit quota window.

| Operation | Credits [EST] | HTTP requests [EST] |
| --- | ---: | ---: |
| Sixteen reference resolutions | 16 | 16 |
| Daily core, sixteen symbols x two adjustment states | 32 | 4 |
| Six calendar probes x sixteen symbols | 96 | 12 |
| Split-boundary bar probes | 12 | 12 |
| SQ/XYZ continuity segments | 2 | 2 |
| KO adjustment probes | 2 | 2 |
| Minimal live preflight | 1 | 1 |
| **Total bars/reference** | **161** | **49** |

This is within the 800-credit daily cap, leaving 639 credits. At eight credits per minute it needs
at least 21 quota windows, or approximately 22-25 minutes with conservative pacing. The expected
12,197 observations remain below each 5,000-point per-symbol response limit and are estimated at
roughly 2-4 MB raw.

The unavailable full-sample action calls would add at least:

- sixteen `/splits` calls x 20 credits = **320 credits**;
- sixteen `/dividends` calls x 20 credits = **320 credits**;
- at least **640 paid-tier corporate-action credits**, excluding calendar pagination; and
- no documented solution for ticker-change history.

## Historical gate conclusion

At the time of this gate, Twelve Data Basic was sufficient for a controlled bars-only experiment,
but not for the then-approved symmetric corporate-action bake-off. Implementing an adapter would
have crossed the user's explicit gate. Accordingly, at that checkpoint:

- no adapter, normalizer, fixture, or live runner was implemented;
- no Twelve Data credential was requested or inspected;
- no API call or purchase was made; and
- Massive remains unchanged and preserved.

The later bars-first decision did authorize Twelve Data implementation. It uses the environment
variable `TWELVE_DATA_API_KEY`, read from the process environment without revealing or persisting
its value.

## Alternatives considered at the historical gate

1. **Authorize Twelve Data Grow or a provider-granted temporary research entitlement.** Confirm
   access for all sixteen symbols, not only trial symbols. This enables splits/dividends but still
   requires a decision for the undocumented SQ-to-XYZ ticker-history case.
2. **Approve a bars-only Twelve Data vs Alpaca SIP experiment.** This was selected on 2026-08-24:
   actions were assessed through Alpaca entitlement plus Twelve Data adjustment behavior and
   documented Basic limitations, without claiming head-to-head parity.
3. **Select another Provider 1.** A fresh official-source gate must prove no-purchase daily,
   five-minute, splits, dividends, suitable identifier/ticker history, and non-display processing.
   The already-reviewed shortlist currently has paid-access or licensing blockers.
4. **Request written Twelve Data clarification.** Ask whether the frozen sixteen-security
   corporate-action sample can receive a temporary entitlement and whether a ticker-event/history
   endpoint exists.

That approved path produced the full real-data bars pipeline and empirical comparison without a
Grow purchase. Completion evidence and the final recommendation are in the provider quality
report.
