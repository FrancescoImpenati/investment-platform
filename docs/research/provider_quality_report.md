# Provider quality report — Phase 1 Provider 1 redesign gate checkpoint

> **Status: MASSIVE LICENSING STOP APPROVED; TWELVE DATA BASIC DOCUMENTARY GATE FAILED;
> FULL EMPIRICAL BAKE-OFF STILL PENDING**
>
> This is an evidence-bearing Phase 1 checkpoint, not the final provider recommendation. The
> approved 16-security experiment was not downloaded or compared. No real provider payload,
> price, volume, corporate action, raw artifact, normalized dataset, or Parquet file is retained in
> the repository.

## Scope and methodology

Phase 1 asks which provider combination can feed the Phase 0 market-data foundation reliably and
sustainably. The preregistered sample, windows, adjustment matrix, and session oracle remain frozen
in the [provider bake-off design](provider_bakeoff_design.md). Alpaca Basic is the approved Provider
2; the decision record is [Provider 2 selection](provider-2-selection.md). After Massive's
licensing stop, Twelve Data was approved for a documentary Provider 1 preflight. Its result is in
[Provider 1 redesign](provider-1-redesign.md).

Evidence is separated as follows:

- **Documented:** a current official provider or exchange statement.
- **Observed:** something measured directly in an offline test or a deliberately minimal live
  preflight.
- **Interpretation:** a project conclusion drawn from documented or observed evidence.
- **Unresolved:** a question this checkpoint cannot answer.

The operating policy approved after preregistration permits an external private temporary data
root when durable retention is ambiguous but temporary private processing is not clearly
restricted. Such a run must still exercise raw artifact, replay, normalization, validation,
Parquet, DuckDB, and comparison before cleanup. That policy does not override a provider term that
restricts the non-display processing itself.

## Providers considered

- **Full empirical bake-off:** no Provider 1 is currently eligible; Alpaca SIP remains ready as the
  approved comparison provider.
- **Massive Stocks Basic:** **technically validated candidate — full real bake-off blocked by
  standard Individual market-data licensing**. Its implementation and evidence remain intact.
- **Twelve Data Basic:** proposed operational Provider 1, stopped before implementation because
  corporate-action endpoints require a paid tier.
- **yfinance:** optional sanity/reference source only; it was not called at this checkpoint and is
  not a canonical-provider candidate.
- Tiingo, Alpha Vantage, EODHD, Finnhub, and other candidates remain alternatives considered in the
  Provider 2 research. None currently has an approved no-purchase full-bake-off path.

## Documented capabilities

Official sources were rechecked on **2026-08-23**. These are provider claims, not measured quality.

### Massive

- [Stocks pricing](https://massive.com/pricing?product=stocks) advertises Basic at $0, five API
  calls per minute, a rolling two years of history, end-of-day recency, US tickers, minute and
  daily aggregates, reference data, and corporate actions.
- [Custom Bars](https://massive.com/docs/rest/stocks/aggregates/custom-bars) supports custom
  five-minute and daily aggregates, one ticker per path, split adjustment, `next_url`, and up to
  50,000 base aggregates per request.
- Official split, dividend, and ticker-event endpoints expose the corporate-action families needed
  by the design, subject to the same rolling Basic history and account entitlement.

### Alpaca

- [About Market Data API](https://docs.alpaca.markets/us/docs/about-market-data-api) documents Basic
  as free, with historical US stock/ETF data since 2016 and 200 historical requests per minute.
- [Market Data FAQ](https://docs.alpaca.markets/us/docs/market-data-faq) states that a historical
  SIP request whose `end` is at least 15 minutes old can be made without the paid subscription.
  Recent/latest SIP and historical SIP are therefore different entitlement cases.
- The FAQ defines SIP as consolidated US-exchange activity and IEX as a single exchange. No
  automatic SIP-to-IEX fallback is methodologically valid.
- Historical bars support `5Min`, daily bars, multi-symbol batching, a 10,000-point page limit,
  `next_page_token`, explicit feed selection, adjustment choices, and `asof`. The corporate-action
  endpoint documents split, dividend, merger, spin-off, name-change, and other action families.

### Twelve Data documentary preflight

- [Individual pricing](https://twelvedata.com/pricing) lists Basic as free with internal
  non-display use, eight API credits per minute, 800 per day, US equities/ETFs, reference data, and
  batch requests.
- `/time_series` supports `5min`, `1day`, bounded dates, up to 5,000 points per symbol, and
  adjustment values `none`, `splits`, `dividends`, and `all`. Every frozen 2025 bar window is inside
  the documented US daily and intraday history.
- The official [US equities guide](https://support.twelvedata.com/en/articles/9935903-us-equities-market-data)
  states that the default feed covers all listed symbols but represents approximately **5% of
  total US trading volume**. It separately describes next-day EOD as consolidated full-market
  data. Twelve Data five-minute data is therefore not SIP-equivalent.
- `/splits` and `/dividends` each cost 20 credits per symbol and require Grow individual or above;
  their calendar variants are also paid-tier-only. No official ticker-change-history endpoint was
  found.
- The no-purchase full-bake-off gate therefore failed before adapter implementation, credential
  configuration, or API access.

## Observed evidence

### Recovery and offline checkpoint

- Branch, local `HEAD`, and `origin/phase-1-provider-bakeoff` were aligned at
  `0dc037e Implement Phase 1 offline provider bake-off`; `main` remained at
  `0355d3f Complete Phase 0 foundation`; the working tree was clean before live preflight.
- The GitHub Actions push run for `0dc037e` completed successfully:
  [CI run 32399788050](https://github.com/FrancescoImpenati/investment-platform/actions/runs/32399788050).
- The Phase 0 + Phase 1 deterministic suite had passed 157 tests with lock check, locked sync,
  Ruff format/lint, strict mypy, pytest, package build, and `git diff --check` at the offline
  checkpoint.
- The finite session schedule is an explicit lookup limited to the preregistered dates and early
  closes. It is not a production trading-calendar engine.
- All three required environment variables were present in the live process. Only presence was
  inspected; values, prefixes, suffixes, and fingerprints were not printed or persisted.

### Provider 1 redesign recovery

- The redesign resumed with local and remote `phase-1-provider-bakeoff` aligned at
  `ce2b63a Document live preflights and licensing stop`; `main` remained frozen at `0355d3f` and
  the working tree was clean.
- Twelve Data research used only current official public documentation. No account, credential,
  endpoint, demo key, trial symbol, or response was used.
- The documentary gate stopped before adapter implementation because Basic lacks split/dividend
  endpoint entitlement. This is a tier finding, not a live provider-quality observation.

### Alpaca historical SIP preflight

On **2026-08-23**, the existing opt-in preflight requested one AAPL historical `5Min` interval,
`[2025-07-02T13:30:00Z, 2025-07-02T13:35:00Z)`, with explicit `feed=sip`.

- Authentication: **PASS**
- HTTP result: **200**
- Requested/served feed: **SIP / SIP**
- Historical SIP entitlement for the tested old interval: **CONFIRMED**
- Essential response shape: a non-empty bars collection with the expected symbol/feed context
- Observed historical rate-limit header: capacity 200; 199 remaining after the request
- Permanent raw persistence: **NO**

This supports only the statement: **Alpaca historical SIP entitlement confirmed for the tested
request.** It does not establish corporate-action access, data quality, or retention rights.

### Massive minimal access preflight

On **2026-08-23**, a transient request used the implemented adapter for the same AAPL five-minute
interval with `adjusted=false`.

- Authentication: **PASS**
- HTTP result: **200**
- Endpoint/timeframe access: **PASS** for the tested historical US-equity aggregate
- Essential response shape: status `OK`, matching ticker, non-empty result array, adjustment flag,
  and no continuation URL for this tiny request
- Adapter compatibility with the observed envelope: **PASS**
- Indicative single-call latency: approximately **519 ms**
- Provider request identifier present: **YES**, not retained
- Rate-limit headers on this response: **NOT OBSERVED**
- Permanent raw persistence: **NO**

This preflight proves technical access and basic response-envelope compatibility only. It did not
run the provider response through raw storage, normalization, Parquet, DuckDB, or comparison.

## Data-quality results

No live data-quality comparison was legally authorized after the licensing gate. Consequently,
there are no observed paired metrics for availability, expected count, missing bars, duplicates,
OHLC, volume, VWAP, adjustment behavior, splits, dividends, ticker changes, DST, holidays, or early
closes. No discrepancy is classified as definitional, adjustment, venue/feed, timing/session,
missing observation, likely provider issue, or unresolved because no paired core dataset exists.

Synthetic fixture results remain useful engineering evidence but are not market-data-quality
evidence. The repository must not present fixture agreement as a live provider finding.

## API and engineering results

The offline checkpoint verifies both adapters, paginated raw batches, redirect rejection,
credential-safe metadata, provider-specific normalizers, canonical validation, Parquet/DuckDB
integration, corporate-action mapping, malformed responses, and discrepancy provenance with
synthetic provider-shaped fixtures. The live preflights add only access and response-envelope
evidence.

| Real-data pipeline stage | Massive | Alpaca SIP | Evidence |
| --- | --- | --- | --- |
| Provider authentication/access | **PASS** | **PASS** | Minimal transient requests |
| Requested feed entitlement | Not a separate feed parameter | **PASS** | Historical SIP explicitly served |
| Immutable raw artifact | **NOT RUN** | **NOT RUN** | Substantive run stopped at licensing gate |
| Checksum/replay | **NOT RUN** | **NOT RUN** | No real raw artifact created |
| Provider normalization | **NOT RUN** | **NOT RUN** | No live value mapping performed |
| Canonical validation | **NOT RUN** | **NOT RUN** | No live canonical records created |
| Analytical Parquet | **NOT RUN** | **NOT RUN** | No real analytical dataset created |
| DuckDB query | **NOT RUN** | **NOT RUN** | No real Parquet dataset existed |
| Cross-provider comparison | **NOT RUN** | **NOT RUN** | Massive non-display license missing |

The full real-data pipeline acceptance criterion is therefore **not met**. No canonical-model or
Phase 0 invariant change was requested or made.

## Licensing and data governance

This is a pragmatic project assessment, not legal advice. Technical access, feed entitlement,
observed semantics, and retention/reuse rights are deliberately separate.

### Massive default Individual terms

The [Massive for Individuals Terms](https://massive.com/legal/individuals-terms-of-service) cover
the API and incorporate the
[Market Data Terms](https://massive.com/legal/market-data-terms-of-service). The latter were last
updated 2025-08-28. They default Market Data to display-only use, restrict non-display use and
creation of derivative works without a separate license, broadly restrict third-party transfer of
analytics/research based on Market Data, and require deletion of all Market Data after account
termination, restriction, or suspension. No public subsequent agreement granting an Individual
Stocks account the required non-display rights was found.

| Use | Classification under the default public terms | Phase 1 consequence |
| --- | --- | --- |
| API access | **CLEARLY PERMITTED**, subject to account/tier | Minimal access preflight was technically successful |
| Private personal/non-commercial display use | **CLEARLY PERMITTED, BUT LIMITED** | Not a general analytics license |
| Transient buffering solely incident to receipt/display | **AMBIGUOUS / NEEDS CLARIFICATION** | Does not authorize the bake-off pipeline |
| Ephemeral raw -> normalization -> Parquet/DuckDB -> comparison | **CLEARLY RESTRICTED absent separate license** | Material stop condition triggered |
| Durable raw archive for replay/re-normalization | **CLEARLY RESTRICTED for this intended non-display use** | No real raw artifact created |
| Normalized/derived private storage | **CLEARLY RESTRICTED absent separate license** | No real dataset created |
| Public display / redistribution | **CLEARLY RESTRICTED absent consent/license** | Out of scope and not attempted |
| Sanitized data-quality findings in Git/GitHub | **RESTRICTED OR NEEDS WRITTEN CLARIFICATION** | No Massive quality findings were produced or pushed |
| Account termination/restriction/suspension | **CLEAR OBLIGATION TO CEASE USE AND DELETE** | Phase 2 cannot assume durable history |

Temporary lifetime does not change the requested pipeline's non-display character. The approved
ephemeral policy therefore cannot cure the Massive restriction. An account-specific order form or
written authorization could change this conclusion; possession of a working API key cannot.

### Alpaca terms

Alpaca's current [Terms and Conditions](https://files.alpaca.markets/disclosures/library/TermsAndConditions.pdf)
cover the API and Market Data as Service/Content and permit personal non-commercial use. The
applicable [Customer Agreement](https://files.alpaca.markets/disclosures/library/AcctAppMarginAndCustAgmt.pdf)
and incorporated exchange agreements restrict reproduction and furnishing data to others and
define derived/processed market data broadly.

| Use | Classification | Phase 1 consequence |
| --- | --- | --- |
| API access | **CLEARLY PERMITTED**, subject to account/tier | Preflight passed |
| Historical SIP ending at least 15 minutes ago | **CLEARLY PERMITTED technically** | Tested entitlement passed; not a retention grant |
| Qualifying private personal/non-commercial processing | **CLEARLY PERMITTED at access/use level** | Ephemeral mode is appropriate |
| Durable raw retention | **AMBIGUOUS / NEEDS CLARIFICATION** | Do not retain durable raw |
| Normalized/derived durable storage | **AMBIGUOUS / NEEDS CLARIFICATION** | Do not assume normalization frees the data |
| Public display / redistribution | **CLEARLY RESTRICTED without suitable authorization** | Out of scope and not attempted |
| Post-termination retained-data use | **AMBIGUOUS / NEEDS CLARIFICATION** | Requires provider-specific Phase 2 policy |

Alpaca passes the approved ephemeral-mode gate, but it still lacks an eligible Provider 1 for the
mandated comparison.

### Twelve Data terms and tier entitlement

The [Twelve Data Terms](https://twelvedata.com/terms) grant authorized Internal Use access,
processing, storage, and non-reversible Derived Data. Basic pricing explicitly includes internal
non-display usage, and official personal-use guidance includes education, research, development,
and testing. The default US feed is described as requiring no additional exchange license for
internal use.

| Use | Classification | Phase 1 consequence |
| --- | --- | --- |
| Basic personal/internal/non-commercial use | **CLEARLY PERMITTED** | Licensing scope matches this project |
| Basic internal non-display processing | **CLEARLY PERMITTED** | Ephemeral pipeline would be authorized for entitled datasets |
| Temporary raw and analytical storage | **CLEARLY PERMITTED for authorized data** | External temporary root and cleanup remain appropriate |
| Durable raw/reversible normalized storage | **AMBIGUOUS on exact duration** | Treat normalized bars as provider Data |
| Non-reversible derived aggregates | **CLEARLY PERMITTED during subscription** | Keep any future report non-substitutive and attributed |
| Redistribution/public underlying values | **RESTRICTED without add-on/agreement** | Out of scope |
| Termination/expiration | **DELETE DATA WITHIN 30 DAYS** | Durable operational history still requires Phase 2 resolution |
| Basic split/dividend endpoints | **NOT ENTITLED** | Technical tier gate fails despite licensing pass |

Licensing and endpoint entitlement are separate. The former passes; the paid corporate-action
entitlement prevents Twelve Data Basic from entering the full empirical bake-off.

## Cost and scalability

The frozen experiment remains bounded and no download estimate changed:

- Massive: **191 nominal requests**, plus any pagination; at five calls/minute this implies at
  least 38.2 minutes and roughly 40-45 minutes with conservative pacing.
- Alpaca: **41 nominal requests**, plus the separate completed SIP preflight and any pagination;
  the historical-bars portion fits well within the documented 200 requests/minute.
- Twelve Data Basic bars/reference plan: **161 credits across approximately 49 HTTP requests**,
  within its 800-credit daily cap and requiring roughly 22-25 minutes at eight credits/minute.
  Full-sample splits and dividends would add at least 640 credits but are unavailable on Basic.
- Estimated raw responses remain below approximately 5 MB/provider and 10 MB combined, with
  roughly 0.5-2 MB/provider of analytical Parquet. These remain estimates, not observed byte
  counts.

At approximately 500 stocks, Massive's one-ticker aggregate path scales request count much more
steeply than Alpaca's multi-symbol endpoint; Alpaca's total-point page limit still makes page count
and ordering important. No throughput or cost conclusion may be upgraded from estimate to evidence
until an authorized run measures pages, latency, throttling, and correction behavior.

No purchase, trial, account creation, API key use, or plan activation was attempted. Twelve Data
Grow could expose split/dividend endpoints, but purchase is not authorized and no documented
ticker-change-history endpoint has been identified.

## Recommendation

No canonical US-equity provider recommendation is justified at this checkpoint.

- **Massive** is a **technically validated candidate — full real bake-off blocked by standard
  Individual market-data licensing**. Its adapter, normalizer, tests, fixtures, preflight, and
  findings remain retained.
- **Alpaca SIP** has confirmed historical entitlement for the tested old request and remains the
  approved comparison provider. Its durable retention and derived-storage rights still need
  clarification before an operational historical database.
- **Twelve Data Basic** is licensing-compatible for internal non-display bars and has a useful
  deliberately partial intraday feed, but cannot be the approved full Provider 1 without paid
  corporate actions or an explicit bars-only methodology change.
- **yfinance** remains an unexecuted sanity/reference option, never a production or canonical
  provider.

The next decision is whether to authorize Twelve Data Grow/temporary research entitlement, approve
a bars-only methodological exception, or select another Provider 1. No empirical winner can be
chosen before one of those paths is approved and executed.

## Phase 2 implications

**durable private market-data retention remains a Phase 2 prerequisite.** In particular, durable
private market-data retention/data root must be solved before relying on Phase 2 as a persistent
historical market database.

Before Phase 2 can maintain history, the project must define:

- a physically external, configurable private data root with access control and deletion policy;
- provider-specific rights for raw retention, normalized/derived storage, non-display use, and
  post-termination handling;
- long-term licensing and cost assumptions;
- a reconciliation/winner policy that preserves provenance without averaging disagreements;
- backfill, incremental update, repair, watermarks, scheduler, retry, and correction behavior;
- a trading-calendar solution if an authorized live bake-off demonstrates that the finite Phase 1
  oracle is insufficient.

Loss of raw artifacts after an ephemeral run reduces re-normalization, auditability, correction
investigation, reproducibility, reconstruction after bugs, and repair/reconciliation reliability.
Ephemeral processing is therefore a Phase 1 risk-control mechanism, not the target architecture.
None of these Phase 2 capabilities is implemented here.

## Unresolved questions and exact resume point

1. Will a paid Twelve Data Grow plan or a temporary research entitlement be explicitly authorized?
2. If Twelve Data is retained, can its support identify a ticker-event/history endpoint for the
   SQ-to-XYZ case?
3. Alternatively, is a bars-only Twelve Data vs Alpaca SIP experiment acceptable even though it is
   not a complete corporate-action head-to-head comparison?
4. Should another Provider 1 be selected under the original no-purchase requirements?
5. Does Alpaca corporate-action access work for this account, and how do its action dates behave?
6. What live paired row counts, discrepancies, action semantics, and calendar behavior occur over
   the frozen sample?
7. Does any live case require a canonical-model change? No such evidence exists yet.

Resume only after one of the Provider 1 alternatives in
[Provider 1 redesign](provider-1-redesign.md) is explicitly approved. No Twelve Data adapter or
runner was implemented because Basic cannot complete the authorized experiment.
