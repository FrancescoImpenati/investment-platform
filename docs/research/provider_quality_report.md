# Provider quality report — Phase 1 final empirical evidence

> **Status: PHASE 1 COMPLETE AND APPROVED**
>
> The full empirical comparison is **Alpaca historical SIP versus Twelve Data Basic**. Corporate
> actions are an asymmetric capability assessment. Massive remains a technically evaluated but
> licensing-blocked candidate; yfinance remains a sanity source only. No real market-data payload,
> raw artifact, canonical observation, Parquet part, credential, or provider response is retained
> in Git.

> **Post-report governance update — 2026-08-29:** Written Alpaca support ticket 342496 resolves the
> durable-retention gate only for historical SIP US stock bars older than 15 minutes in the private
> use scope recorded in
> [the redacted Alpaca rights record](../governance/data-rights/alpaca-historical-sip.md).
> Historical statements below that call Alpaca retention ambiguous remain accurate as of the
> 2026-08-24 bake-off and are intentionally not rewritten. The new evidence is an input to the
> now-implemented Phase 2 retention policy. Phase 2 implementation and acceptance status is owned
> by its plan and sanitized acceptance record, not by this historical Phase 1 report.

## Scope and methodology

Phase 1 tested whether the Phase 0 boundary can carry real US-equity data through the complete
pipeline and what provider combination is technically, economically, and contractually plausible.
The frozen design used 16 securities and bounded 2025 windows for daily bars, five-minute bars,
split adjustment, ticker continuity, DST, holidays, and early closes. The sample and dates are in
[provider_bakeoff_design.md](provider_bakeoff_design.md).

The approved execution amendment replaced the licensing-blocked Massive substantive run with
Twelve Data Basic and made corporate actions asymmetric. The principal empirical comparison was:

```text
Alpaca historical SIP
vs
Twelve Data Basic standard partial-volume intraday feed and separately documented consolidated EOD
```

The substantive run took place on **2026-08-24** using the account's Alpaca historical SIP access
and Twelve Data Basic entitlement. Execution started from implementation checkpoint `740d671`;
provider-boundary differences discovered live were converted into synthetic regressions and are
included in the final Phase 1 commit.

| Evidence window | Securities | Effective request |
| --- | ---: | --- |
| Daily core, raw and split-adjusted | 16 | `[2025-05-27, 2025-12-06)`; 135 expected sessions each |
| Calendar/DST five-minute probes | 16 | 2025-03-07, 03-10, 07-02, 07-03, 10-31, and 11-03 regular-session bounds |
| Split-boundary five-minute probes | ORLY, IBKR, NFLX | Sessions immediately before/on their 2025 adjusted-trading dates, raw and split-adjusted |
| Ticker continuity | SQ/XYZ | `[2025-01-13, 2025-01-29)`; 11 expected sessions |
| Dividend/all adjustment evidence | KO | `[2025-06-09, 2025-06-21)`; 9 expected sessions |
| Corporate-action capability | 16 | Alpaca 2025 `process_date` query; Twelve action endpoints documented but not Basic-entitled |

Every substantive provider page followed this flow inside a private Windows temporary directory
physically outside the repository:

```text
HTTP response
-> immutable RawArtifact
-> checksum/replay
-> provider-specific normalization
-> canonical validation
-> Parquet
-> DuckDB query
-> pairwise comparison
-> aggregate non-substitutive evidence
-> cleanup
```

The comparison never averages discordant values and never selects a winner per observation.
Twelve Data has no native per-bar VWAP in `/time_series`, so VWAP was excluded rather than compared
to its separate technical-indicator endpoint. Values from raw or canonical live datasets were not
copied into this report.

Evidence labels used below are:

- **DOCUMENTED:** stated in current official documentation or applicable public terms;
- **OBSERVED:** measured directly in the Phase 1 run;
- **INTERPRETATION:** a project conclusion from documented and observed evidence;
- **UNRESOLVED:** not established by this experiment.

## Providers considered

### Full empirical bake-off

- **Alpaca historical SIP:** consolidated US market feed for the tested historical requests.
- **Twelve Data Basic:** standard US intraday feed, explicitly not SIP-equivalent.

### Technically evaluated but licensing-blocked

- **Massive:** **technically validated candidate — full real bake-off blocked by standard
  Individual market-data licensing**. The adapter, normalizer, synthetic fixtures, tests, minimal
  access preflight, and engineering/licensing findings remain intact. No further Massive
  market-data processing was performed after the approved stop.

### Sanity/reference

- **yfinance 1.6.0:** isolated research-only sanity source. It is not a `MarketDataProvider`, was
  not added to project dependencies, and is not a production or canonical-provider candidate.

### Alternatives considered

Tiingo, Alpha Vantage, EODHD, Finnhub, and other candidates remain background to the provider
selection. They were not expanded into third or fourth empirical bake-offs. The prior research is
preserved in [provider-2-selection.md](provider-2-selection.md).

## Documented capabilities

### Alpaca

- **DOCUMENTED:** [About Market Data API](https://docs.alpaca.markets/us/docs/about-market-data-api)
  describes historical US stocks/ETFs since 2016 and 200 historical requests per minute on Basic.
- **DOCUMENTED:** the [Market Data FAQ](https://docs.alpaca.markets/us/docs/market-data-faq) says an
  historical SIP request ending at least 15 minutes in the past can be made without the paid market
  data subscription. SIP consolidates US-exchange activity; IEX is a distinct partial feed.
- **DOCUMENTED:** [historical bars](https://docs.alpaca.markets/us/reference/stockbars) support
  multi-symbol queries, `5Min`/daily data, paging, adjustments, explicit feed selection, and `asof`
  ticker mapping.
- **DOCUMENTED:** [corporate actions](https://docs.alpaca.markets/us/reference/corporateactions-1)
  expose multiple event families and paginate using a query bounded by `process_date`.

### Twelve Data

- **DOCUMENTED:** [Individual pricing](https://twelvedata.com/pricing) lists Basic at eight API
  credits/minute and 800/day, with internal non-display usage, US equities/ETFs, batch requests,
  and reference data.
- **DOCUMENTED:** [`/time_series`](https://twelvedata.com/docs#time-series) supports `5min`, `1day`,
  date bounds, up to 5,000 points, and `none`, `splits`, `dividends`, and `all` adjustment modes.
- **DOCUMENTED:** the [historical-data guide](https://support.twelvedata.com/en/articles/5214728-getting-historical-data)
  says combined `start_date`/`end_date` returns the bounded period and that adding `outputsize`
  would restrict it. The live daily boundary behavior is recorded separately below.
- **DOCUMENTED:** the [US equities guide](https://support.twelvedata.com/en/articles/9935903-us-equities-market-data)
  says the default feed covers all listed US symbols but represents approximately **5% of total US
  trading volume**. It separately describes next-day EOD as consolidated full-market data.
- **DOCUMENTED:** `/splits` and `/dividends` require Grow or above. No equivalent official
  ticker-change-history endpoint was identified. Basic was used without purchase or upgrade.

### Massive

- **DOCUMENTED:** [Stocks pricing](https://massive.com/pricing?product=stocks) and
  [custom bars](https://massive.com/docs/rest/stocks/aggregates/custom-bars) describe Basic US
  aggregates, minute/daily support, split adjustment, reference data, corporate actions, and
  pagination.
- **DOCUMENTED:** the default [Individual terms](https://massive.com/legal/individuals-terms-of-service)
  and [Market Data terms](https://massive.com/legal/market-data-terms-of-service) do not grant the
  non-display processing and derived-use rights required by this bake-off without a separate
  license.

### yfinance

- **DOCUMENTED:** [PyPI](https://pypi.org/project/yfinance/) describes yfinance as unaffiliated
  with Yahoo, intended for research/education, and reminds users that Yahoo data is for personal
  use and subject to Yahoo terms.
- **DOCUMENTED:** [`yfinance.download`](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html)
  documents an exclusive `end`, explicit adjustment controls, actions, and the limitation that
  intraday data cannot extend beyond the last 60 days.

## Observed evidence

### Access and entitlement preflights

- **OBSERVED — Alpaca:** one old AAPL `5Min` request with explicit `feed=sip` returned HTTP 200 and
  SIP data. Historical SIP entitlement was confirmed for that request; there was no IEX fallback.
- **OBSERVED — Massive:** one tiny AAPL aggregate request returned HTTP 200 with a compatible
  envelope and approximately 519 ms latency. This proved technical access only, before the
  licensing stop.
- **OBSERVED — Twelve Data:** a one-bar AAPL `5min` preflight returned HTTP 200, one canonical row,
  and passed raw artifact, replay, normalization, validation, Parquet, and DuckDB. The provider
  returned the wire `end` observation; the canonical half-open guard correctly excluded it.
- Two Twelve Data preflight calls were made because the first local result renderer referenced an
  obsolete metric name. The duplicate was minimal and no response survived cleanup.

### Frozen sample and request budget

The sample was AAPL, MSFT, NVDA, AMZN, NFLX, JPM, IBKR, BRK.B, XYZ, XOM, JNJ, KO, BA, ORLY, NEE,
and SPY. Provider identifiers remained external to the stable internal UUIDs. BRK.B/BRK-B and
SQ/XYZ exercised punctuation and temporal identity.

The successful substantive run used:

| Provider | Reference | Canonical bars | Auxiliary evidence | Successful-run total |
| --- | ---: | ---: | ---: | ---: |
| Alpaca | 16 asset lookups | 20 frozen requests + 1 mapped ticker request | 1 unmapped ticker control + 1 action page + 2 adjustment probes | **41 calls** |
| Twelve Data | 16 symbol searches | 30 batched requests | 2 adjustment probes | **48 calls / 160 reserved credits** |

Twelve Data pacing held each rolling minute to eight credits. Provider headers exposed current
window usage and remaining capacity; `api-credits-used` was observed to be cumulative within the
window, not a per-call cost. A live regression changed the aggregate from an invalid sum to the
observed maximum of eight. The successful run plus preflight, one early interrupted reference
pass, and bounded corrective diagnostics consumed an estimated **210 Twelve Data credits**, safely
below the 800/day Basic allowance.

The successful main run created approximately **2.96 MB** of raw HTTP bodies across both providers
before cleanup. The main analytical pipelines wrote 35 Alpaca and 58 Twelve Data Parquet parts.
Corrective reruns were bounded and ephemeral.

### Live differences from synthetic fixtures

Three provider-boundary differences were found and converted to offline regressions:

1. Alpaca's live Asset Object used official field `class=us_equity`; the synthetic fixture used
   `asset_class`. Reference verification now accepts either official wire spelling.
2. Twelve Data daily `end_date` behaved as an **exclusive exchange-date bound**. The adapter now
   sends the exclusive date after the last potentially admissible UTC day, while the normalizer
   retains strict `[start, end)` filtering. A targeted rerun restored the missing 2025-12-05 daily
   bars and the 2025-01-28 XYZ bar; a synthetic non-midnight regression protects the generic
   boundary.
3. Twelve Data's `api-credits-used` header is a cumulative current-window counter. It is no longer
   summed as though it were the cost of each request.

No canonical model or Phase 0 architectural invariant changed.

## Data-quality results

### Availability, missing bars, and duplicates

The frozen design expected 12,179 canonical rows per provider across distinct adjustment segments.
After the daily-boundary regression and targeted real-data rerun:

| Segment | Expected | Alpaca SIP | Twelve Data Basic | Finding |
| --- | ---: | ---: | ---: | --- |
| Daily unadjusted | 2,160 | 2,160 | 2,160 | Complete on both |
| Daily split-adjusted | 2,160 | 2,160 | 2,160 | Complete on both |
| Six calendar `5m` sessions | 6,912 | 6,906 | 6,912 | Six Alpaca missing observations, all ORLY |
| Split-boundary `5m` unadjusted | 468 | 460 | 468 | Eight Alpaca ORLY observations missing on 2025-06-09 |
| Split-boundary `5m` split-adjusted | 468 | 460 | 468 | Same eight wire observations absent in the separate adjustment segment |
| SQ/XYZ ticker continuity | 11 | 11 | 11 | Complete after Twelve daily-boundary fix |
| **Total** | **12,179** | **12,157** | **12,179** | No duplicates on either provider |

The six calendar gaps were ORLY at 2025-03-07 19:35/19:40 UTC and 2025-03-10
15:50/16:45/18:30/18:45 UTC. These remain **missing observations**, consistent with a bar service
that emits intervals only when eligible trades exist; Phase 1 does not label them a provider error.

### OHLC and volume discrepancies

Strict numeric tolerances deliberately count small rounding differences. Counts below are metric
findings, not numbers of bad rows.

| Segment | Classification | Key observed result |
| --- | --- | --- |
| Daily unadjusted | unresolved discrepancy | 5,920 metric differences; maximum OHLC absolute difference about 0.00503; mean relative volume difference about 0.0269%, maximum about 23.94% |
| Daily split-adjusted | unresolved discrepancy | 5,990 metric differences; similarly small maximum OHLC difference; daily volume outlier remains unresolved |
| Calendar `5m` | volume: venue/feed; OHLC: unresolved | 6,701 volume and 12,427 OHLC metric differences; mean relative volume difference about 15.27%, maximum about 99.05% |
| Split-boundary `5m` raw | volume: venue/feed; OHLC: unresolved | 454 volume and 787 OHLC metric differences; mean relative volume difference about 19.71%, maximum about 98.38% |
| Split-boundary `5m` split-adjusted | volume: venue/feed; OHLC: unresolved | 454 volume and 1,006 OHLC metric differences; mean relative volume difference about 19.71%, maximum about 98.38% |
| Ticker continuity daily | unresolved discrepancy | 13 small metric differences; no availability gap after correction |

Intraday OHLC outliers were materially larger than daily rounding differences: across the six
calendar sessions the maximum absolute differences were approximately 10.39 open, 5.27 high,
2.22 low, and 4.08 close in provider price units. The known feed coverage can affect which trades
set OHLC as well as volume, but it does not prove the cause of every outlier. OHLC remains an
**unresolved discrepancy**; the partial venue/feed coverage is the leading interpretation, not an
automatic provider-error label or a demonstrated causal classification.

The documented “approximately 5%” feed statement does not imply that every Twelve Data bar must
show exactly 5% of SIP volume; observed relative differences were heterogeneous. Future volume,
VWAP, liquidity, breadth, and trade-intensity features must not treat the standard Twelve Data US
intraday feed as interchangeable with SIP.

### VWAP and adjustments

- **OBSERVED:** Alpaca bars exposed native per-bar VWAP; Twelve Data `/time_series` did not.
  Cross-provider VWAP was therefore **not semantically comparable**.
- **OBSERVED:** raw versus split-adjusted daily values differed on 147 matched rows for each
  provider.
- **OBSERVED:** around the three split cases, 234/468 Twelve Data five-minute rows changed under
  split adjustment. Alpaca changed 226/460 because the eight missing ORLY pre-split bars were not
  present in either adjustment segment.
- **OBSERVED:** for KO over `[2025-06-09, 2025-06-21)`, Alpaca `dividend` and `all` each differed
  from unadjusted on all 9 matched daily rows. After the daily-boundary fix, Twelve Data
  `dividends` and `all` likewise differed on all 9 matched rows.
- **INTERPRETATION:** dividend-only and provider `all` modes remain provider-native evidence; they
  were not forced into a canonical adjustment enum with stronger semantics.

## API and engineering results

### Real pipeline acceptance

| Stage | Alpaca SIP | Twelve Data Basic |
| --- | --- | --- |
| Provider authentication/access | **PASS** | **PASS** |
| Exact US-equity reference resolution (16/16) | **PASS** | **PASS** |
| Requested feed/dataset | **PASS — SIP** | **PASS — standard US feed** |
| Immutable raw artifact before inspection | **PASS** | **PASS** |
| Checksum/replay | **PASS** | **PASS** |
| Provider-specific normalization | **PASS** | **PASS** |
| Canonical validation | **PASS; zero flags** | **PASS; zero flags** |
| Analytical Parquet | **PASS** | **PASS** |
| DuckDB replay/query | **PASS** | **PASS** |
| Pairwise comparison | **PASS** | **PASS** |
| Temporary-root cleanup | **PASS** | **PASS** |

Comparison `PASS` means that every designed segment exercised the harness with nonempty provider
observations and no duplicates. It does not mean perfect completeness: missing expected keys and
numeric discrepancies remain explicit quality evidence below that structural acceptance result.

The successful main pipelines processed 21 Alpaca raw batches/12,157 rows and 30 Twelve Data raw
batches/12,146 rows before the end-boundary correction. The affected targeted rerun processed all
4,331 daily/ticker rows per provider and passed every pipeline stage, producing the corrected
12,179-row Twelve total stated above.

Main-run response latency was indicative, not a benchmark: Alpaca averaged about 806 ms across 21
bar pages (maximum 1,260 ms); Twelve Data averaged about 580 ms across 30 bar pages (maximum
1,729 ms). No bar request paginated in the bounded sample. Alpaca corporate actions returned one
page. No authentication, entitlement, rate-limit, redirect, or malformed-response error occurred
during the successful run. Offline tests cover those failures and forbid silent retries/fallbacks.

### Corporate-action capability assessment

Alpaca corporate-action entitlement passed for the current account and one 2025 `process_date`
query over the sample.

| Family | Live records | Canonical result |
| --- | ---: | --- |
| Forward splits | 3 | 3 canonical split actions |
| Cash dividends | 33 | Not canonicalized: live records lacked currency and USD was not invented |
| Name changes | 1 | Not canonicalized: no reliable effective date beyond process-date semantics |
| Cash mergers | 1 | Retained in raw; unsupported by the Phase 0 action union |
| Stock mergers | 1 | Retained in raw; unsupported by the Phase 0 action union |

This produced 34 `incomplete_corporate_action` and two `unsupported_corporate_action` findings,
plus one warning that the query is bounded by process date. The canonical model correctly refuses
to invent dividend currency or event effective dates. Phase 1 does not require a model expansion
for merger types.

Twelve Data Basic `/splits` and `/dividends` were **not called** because they are not entitled.
No official equivalent ticker-change-history endpoint was identified. Higher-tier capabilities
remain **DOCUMENTED**, not empirically tested.

Ticker continuity was nevertheless tested through bars: Alpaca `asof` mapping returned all 11
expected SQ/XYZ sessions, while a mapping-disabled control returned only 6 current-symbol rows.
Twelve Data returned 5 SQ plus 6 XYZ rows after the end-boundary fix. yfinance returned all 11
under XYZ and zero under SQ, demonstrating a third, distinct identifier mapping policy.

## Trading-calendar findings

- **OBSERVED:** full sessions contained 78 five-minute intervals per symbol. Twelve Data returned
  all expected intervals in every calendar window. Alpaca's only calendar gaps were six ORLY bars.
- **OBSERVED:** the 2025-07-03 early close contained 42 intervals per symbol, or 672 rows/provider.
- **OBSERVED:** 2025-03-07/10 and 2025-10-31/11-03 used the correct UTC shift across US DST; no
  normalization issue or out-of-session bar was produced.
- **OBSERVED:** daily completeness was consistent with the five frozen closed sessions in scope,
  including exchange holidays; no holiday bar was introduced.
- **INTERPRETATION:** Phase 2 needs a maintained exchange-calendar source for scalable expected-row
  checks and early closes. The 148-session frozen lookup remains experiment-only and is not a
  production calendar engine.

## yfinance sanity/reference result

Using yfinance 1.6.0 in an isolated temporary environment with `auto_adjust=False`:

- all 16 symbols returned 135 daily rows, or **2,160 total**, for the core daily window;
- the core window exposed one split each for IBKR, NFLX, and ORLY and 25 dividend events across ten
  sample securities;
- the old AAPL 2025-07-02 five-minute query returned **zero rows**, consistent with the documented
  60-day intraday limitation;
- SQ returned zero continuity rows while XYZ returned all 11, reflecting Yahoo's mapping policy;
- cache and downloaded data were deleted; no dependency or lockfile change was made.

This is useful sanity evidence for daily availability and the three split cases, not an independent
production feed or a substitute for licensed provider data.

## Licensing and data governance

This is an engineering assessment, not legal advice. Technical access, feed entitlement, data
semantics, and retention/reuse rights remain separate.

| Provider | Temporary private processing | Durable raw/reversible retention | Public display/redistribution | Termination consequence |
| --- | --- | --- | --- | --- |
| Alpaca | **CLEARLY PERMITTED at personal/non-commercial access/use level** | **AMBIGUOUS / NEEDS CLARIFICATION** | **RESTRICTED without authorization** | Retained-data rights unresolved |
| Twelve Data | **CLEARLY PERMITTED for entitled internal use** | **AMBIGUOUS on exact duration/operational use** | **RESTRICTED; underlying values not published** | Public terms require deletion within 30 days after expiration/termination |
| Massive Individual | **CLEARLY RESTRICTED for this non-display bake-off absent separate license** | **CLEARLY RESTRICTED for intended use** | **RESTRICTED** | Cease use/delete under applicable terms |
| yfinance/Yahoo | Research/personal sanity use only | Not assessed as a durable project source | Not authorized by this experiment | Subject to Yahoo terms |

The approved **ephemeral/private mode** was applied to Alpaca, Twelve Data, and yfinance. One
interrupted live run left a temporary directory because the process received an external interrupt;
the exact verified root was immediately removed. The successful run and every corrective diagnostic
reported cleanup `PASS`. Repository and environment scans found no credential or real/private
market-data file.

The public/private boundary emerging from Phase 1 is:

```text
Public repository: code, architecture, tests, schemas, synthetic fixtures, aggregate findings
Private runtime: credentials, licensed raw, canonical real data, restricted derivatives
```

**durable provider-compatible private market-data retention must be solved before a persistent
historical ingestion system is deployed.** Ephemeral mode proves the pipeline but sacrifices raw
replay, re-normalization, correction investigation, historical audit, reproducibility, and robust
repair/reconciliation.

Reversible normalized bars remain close enough to provider data that Phase 1 does not assume they
are freely retainable derived data. The retained report metrics are deliberately aggregate,
non-substitutive analytical evidence; that distinction does not replace provider-specific legal
clarification for a durable history.

## Cost and scalability

- **Alpaca:** the 16-security successful run required 41 calls and stayed well inside the observed
  historical allowance. Multi-symbol requests are efficient, though approximately 500 symbols
  will increase points, pages, and request partitioning materially.
- **Twelve Data Basic:** the successful run required 48 calls/160 credits and roughly 20 minutes at
  eight credits/minute. The experiment fit within 800/day without purchase. At 500 symbols, even
  one batched request dimension approaches 500 credits; the full multi-window design cannot scale
  on Basic's daily allowance. Paid economics must be evaluated before operational ingestion.
- **Massive:** the one-ticker aggregate surface is technically capable but scales request count more
  steeply and remains blocked by default Individual licensing, regardless of API price.
- **Storage:** the bounded raw run was under 3 MB, but 500-stock backfill, corrections, and replay
  would create a materially different durable storage and licensing problem.

## Recommendation

Phase 1 does not support one global winner for all datasets.

1. **Primary US historical consolidated-bars candidate: Alpaca SIP.** It is the strongest tested
   fit for five-minute US bars where consolidated volume, per-bar VWAP, and trade coverage matter.
   It also remains a future broker/paper-trading candidate. This recommendation is conditional on
   resolving durable retention and long-term non-display rights before Phase 2 persists history.
2. **Secondary/reference and future international-bars candidate: Twelve Data.** Daily US bars
   showed strong OHLC agreement and complete corrected availability, and the internal-use terms
   fit the research workflow better. Its standard US intraday feed must not back volume, VWAP,
   liquidity, breadth, or similar consolidated-market features. International coverage was
   documented but not empirically tested in Phase 1.
3. **Corporate actions: use a dataset-specific source policy.** Alpaca is technically useful for
   splits and raw event discovery, but current cash-dividend and name-change fields were
   insufficient for the canonical contract. Twelve Data Basic actions are unavailable. A dedicated
   or separately licensed source may be required.
4. **Massive:** retain the exact status **technically validated candidate — full real bake-off
   blocked by standard Individual market-data licensing**. Reconsider only with explicit written
   rights or a different agreement.
5. **yfinance:** retain solely as a bounded sanity/reference source; never make it canonical.

No multi-provider orchestration, curated winner policy, or automatic reconciliation is implemented
in Phase 1.

## Unresolved questions

- What durable raw, normalized, and derived-data retention rights apply to the exact Alpaca account
  and SIP use after subscription/account termination?
- What Twelve Data retention duration and operational-history rights apply beyond the public
  internal-use/deletion language?
- Can Alpaca supply an authoritative currency for cash dividends and an effective date for name
  changes, or must another action source be used?
- What explains the largest daily-volume and intraday-OHLC outliers after feed-definition effects?
- Which provider/tier is economically viable for approximately 500 symbols, corrections, and
  recurrent updates?
- How does Twelve Data perform on the international markets that motivate its future role?

## Phase 2 implications

Before operational historical ingestion, Phase 2 must explicitly design and approve:

- a physically external durable private data root and provider-specific retention policy;
- long-term licensing assumptions and deletion behavior;
- a dataset/provider selection and reconciliation/winner policy;
- backfill partitioning and request budgeting;
- incremental updates, persistent watermarks, and correction windows;
- repair/replay from retained raw evidence;
- scheduler, retry, pacing, and failure-state behavior;
- a maintained exchange calendar for holidays, DST, early closes, and expected counts;
- a corporate-action source capable of currency and reliable effective-date semantics.

For Phase 3/4 feature work, consolidated volume, provider-native VWAP, breadth, liquidity, and
trade-intensity inputs must come from a consolidated or otherwise explicitly fit-for-purpose feed.
Twelve Data's standard partial-volume US intraday feed cannot be substituted silently for SIP,
although its international coverage remains a separate future hypothesis to test.

None of those Phase 2 capabilities is implemented by this report or the finite Phase 1 runner.

## Learning notes

Real data validated the Phase 0 separation of concerns. Raw-first persistence made it possible to
discover provider wire behavior without contaminating the canonical contract. The strict half-open
boundary caught an end-date mismatch rather than silently losing or duplicating a trading day.
Stable UUIDs allowed SQ/XYZ to expose three different ticker-history policies without changing
instrument identity. Explicit adjustment states prevented provider `all` and dividend-only modes
from being mislabeled as a stronger canonical guarantee.

The bake-off also showed that “data quality” is multidimensional. Twelve Data was complete after a
wire fix yet definitionally unsuitable for consolidated intraday volume. Alpaca SIP had six
legitimate-looking ORLY gaps while providing the richer feed. Corporate-action access did not imply
canonical completeness because currency and effective date were absent. Finally, API access did
not settle retention rights. Those distinctions are precisely why provider boundaries, provenance,
validation, and data governance must stay separate in the platform architecture.
