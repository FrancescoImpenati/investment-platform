# Phase 1 Provider 2 selection

> **Status: APPROVED AS PHASE 1 BAKE-OFF PROVIDER 2**
>
> At the 2026-08-19 provider-selection checkpoint, no provider API had been called, no credentials
> had been used, and no market-data payload had been downloaded. This document records the
> candidate selection for the Phase 1 bake-off; it does not select the production canonical
> provider. Subsequent implementation status is recorded below.

## Decision recorded

On 2026-08-19, **Alpaca Basic** was approved as Provider 2 alongside the required primary
candidate, Massive. The approval authorizes an empirical comparison only. It does not select
Alpaca or Massive as the production-canonical provider. yfinance remains only a sanity/reference
check and never a production-canonical provider.

The approval establishes three separate checks rather than one combined licensing/entitlement
gate:

1. one transient historical SIP API entitlement preflight determines whether the configured Alpaca
   account authorizes `feed=sip` for data older than 15 minutes; it may run while retention remains
   ambiguous, discards the response body, and persists no raw artifact;
2. a later, separate corporate-action endpoint access check determines that endpoint's account and
   regional entitlement; SIP success does not prove corporate-action access; and
3. review of the applicable account and market-data agreements determines whether substantive
   responses may be downloaded and retained as immutable private raw artifacts for this research
   use. This contractual review is not answered by either API result.

No paid plan is proposed. The adapter must not silently substitute the IEX feed if SIP is not
entitled, because that would materially change the intended comparison.

## Scope and method

The comparison used current official product documentation, pricing pages, support material, and
terms available on 2026-08-19. It considered:

- daily and 5-minute US-equity bars;
- historical depth and feed composition;
- corporate actions, identifiers, and reference data;
- adjustment, timestamp, session, pagination, and rate-limit semantics;
- no-cost or already-authorized accessibility for a small bake-off;
- licensing, raw-data retention, and redistribution constraints;
- future international-equity expansion.

The categories below deliberately separate provider claims from direct observations and project
judgment. Terms are summarized for engineering evaluation, not as legal advice.

## Comparison summary

| Candidate | Daily and 5-minute US equities | Corporate actions / identifiers | Bake-off access | Licensing fit | International path | Assessment |
| --- | --- | --- | --- | --- | --- | --- |
| Massive | Yes; daily and custom 5-minute aggregates | Splits, dividends, point-in-time ticker reference, CIK and FIGI | Basic is free but limited to 5 calls/minute and two years | Private/internal use only unless separately licensed; retention needs confirmation | No established international cash-equity path | Required primary candidate |
| Alpaca Basic | Yes; bars support `1Day` and `5Min`; historical SIP older than 15 minutes is documented, subject to account entitlement | Rich corporate-actions endpoint; Alpaca UUID, ticker, exchange, CUSIP lookup, and ticker-change `asof` | Free, documented 200 historical requests/minute, history from 2016 | **Unresolved**; reviewed material does not establish durable private raw retention or all intended non-display uses | Global corporate actions exist, but broad international price history is not established | **Recommended Provider 2 for the bake-off, subject to the separate gates above** |
| Twelve Data Basic | Yes, but the standard free US intraday feed represents about 5% of total volume; daily is consolidated EOD | Broad global identifiers; dividend/split endpoints require Grow or Venture | Free tier is rate-limited; required corporate-action coverage is paid | Internal non-display use; termination/deletion obligations reduce long-term fit | Strongest documented international trajectory of the shortlist | Retain as future international candidate |
| Tiingo Starter | EOD plus 5-minute IEX history | Split/dividend data and Tiingo symbology | Technically accessible on Starter | Current terms prohibit persistent storage on Starter, constrain benchmark analysis, and prohibit reconstructable normalization | Limited evidence of broad multi-exchange equity coverage | Exclude absent written permission |
| Alpha Vantage free | Daily is available; historical 5-minute and adjusted daily capabilities needed here are premium | Listing-status and split endpoints; weaker stable-identifier story | Free limit is 25 requests/day; core comparison requires premium access | Not evaluated far enough to justify live use | Global-symbol coverage is documented | Reject for this no-purchase bake-off |
| EODHD | Daily and 5-minute bars, but intraday requires a paid plan | Detailed actions and mappings for CUSIP, ISIN, FIGI, LEI, and CIK | No-purchase tier does not provide the needed intraday comparison | Standard terms require deletion after subscription termination | Broad documented exchange coverage | Exclude absent a different retention agreement |
| Finnhub | US candles are available, but the useful historical tier is paid | FIGI/MIC reference is strong; split/dividend access is premium | No suitable free full bake-off path | Terms require deletion at termination and tightly constrain shared derived results | Paid international daily coverage exists | Exclude on access, licensing, and semantics gaps |

## Provider documentation

### Massive — required primary candidate

- The [Stocks pricing page](https://massive.com/pricing?product=stocks) lists a free Basic tier with
  five API calls per minute, two years of history, US stock coverage, reference data, corporate
  actions, end-of-day data, and minute aggregates. Deeper history and higher limits are paid.
- The [custom bars documentation](https://massive.com/docs/rest/stocks/aggregates/custom-bars)
  supports day and minute timespans, so five-minute bars can be requested with a multiplier. It
  labels aggregates at interval start, emits no bar when no qualifying trade exists, and documents
  a 50,000-base-aggregate query limit.
- Massive documents adjusted aggregates as split-adjusted by default; requesting unadjusted data
  is explicit. Dividends are not included in that bar adjustment.
- The official [ticker reference](https://massive.com/docs/rest/stocks/tickers/all-tickers),
  [split](https://massive.com/docs/rest/stocks/corporate-actions/splits), and
  [dividend](https://massive.com/docs/rest/stocks/corporate-actions/dividends) endpoints expose
  point-in-time reference and corporate-action data. Ticker records include identifiers such as
  CIK and FIGI where available.
- The [market-data terms](https://massive.com/terms/market_data_terms.pdf) impose important
  personal/non-commercial, display, derived-use, and redistribution constraints unless a separate
  agreement applies. The right to retain immutable raw artifacts for the complete intended project
  lifetime is not explicit in the public material reviewed.

### Alpaca Basic — recommended Provider 2; licensing fit unresolved

- Alpaca's [market-data overview](https://docs.alpaca.markets/us/docs/about-market-data-api)
  documents a free Basic plan for US stocks and ETFs, history since 2016, and 200 historical
  requests per minute. The paid Algo Trader Plus tier is not needed for the proposed bounded test.
- The [market-data FAQ](https://docs.alpaca.markets/us/docs/market-data-faq) says historical SIP
  data is accessible without a subscription when the request ends at least 15 minutes in the past.
  The same documentation distinguishes the Basic real-time IEX feed from SIP.
- The [historical bars endpoint](https://docs.alpaca.markets/us/reference/stockbars) supports
  `5Min` and `1Day`, multiple symbols, up to 10,000 observations per page, `next_page_token`, and
  explicit `sip`, `iex`, `boats`, and `otc` feeds. It documents `raw`, `split`, `dividend`,
  `spin-off`, `all`, and combined adjustment choices, plus point-in-time ticker mapping through
  `asof`.
- Alpaca's API ranges are inclusive. The adapter will therefore need an explicit, tested mapping to
  the platform's half-open UTC requests. Minute bars are labeled at interval start, while daily
  bars use a New York calendar date.
- The [asset endpoint](https://docs.alpaca.markets/us/reference/get-v2-assets-1) returns Alpaca UUID,
  symbol, exchange, status, and name; individual lookup supports asset UUID, ticker, or CUSIP.
- The [corporate-actions endpoint](https://docs.alpaca.markets/us/reference/corporateactions-1)
  documents pagination and 15 action types, including dividends, splits, mergers, spin-offs, name
  changes, rights, and reorganizations. It also warns that event creation time is not guaranteed.
- Since June 2026 Alpaca documents ISIN, currency, and non-US regions for corporate actions in its
  [official changelog](https://docs.alpaca.markets/us/changelog/2026-06-03-market-data-9dddd18).
  This does not establish equivalent international historical price-bar coverage.
- The [paper-trading documentation](https://docs.alpaca.markets/us/docs/paper-trading) says Paper
  Only accounts receive IEX market data. This creates an entitlement ambiguity with the historical
  SIP FAQ and makes the live SIP preflight mandatory.
- The public [customer agreement](https://files.alpaca.markets/disclosures/library/TermsAndConditions.pdf)
  restricts reproduction, distribution, and commercial exploitation without consent. The material
  reviewed did not establish an unconditional right to retain raw SIP payloads indefinitely.

### Twelve Data — strongest future international candidate

- The [market-data API documentation](https://twelvedata.com/docs/market-data) supports `5min` and
  `1day`, returns at most 5,000 points per time-series response, and documents explicit adjustment
  choices. Intraday and daily timezone semantics differ and would need provider-specific mapping.
- Twelve Data states that its standard [US intraday equities feed](https://support.twelvedata.com/en/articles/9935903-us-equities-market-data)
  represents about 5% of total market volume, while next-day EOD data aggregates all exchanges and
  covers full reported US volume.
- The [pricing page](https://twelvedata.com/pricing) lists a Basic free tier with eight API credits
  per minute and 800 per day. The dividend and split endpoints require at least a paid Grow or
  Venture entitlement, making a complete no-purchase corporate-action bake-off impossible.
- Twelve Data documents a broad international catalogue and identifiers such as FIGI, MIC, CFI,
  CIK, ISIN, and CUSIP, with some fields or markets dependent on tier or add-on.
- The [terms](https://twelvedata.com/terms) permit internal processing and storage subject to plan
  limits, restrict redistribution, and impose deletion obligations after termination. This is a
  weaker long-term raw-retention fit than the project needs.

### Tiingo — excluded on current terms

- Tiingo documents long EOD history with raw and adjusted fields and a 5-minute IEX feed. Its
  technical surface would otherwise be relevant.
- The current [Tiingo Terms of Use](https://api.tiingo.com/tos/) were reviewed in the version marked
  last updated 2026-08-05. They prohibit persistent or durable storage for Starter and trial plans,
  require deletion after a paid plan ends or is downgraded unless separately agreed, restrict use
  for benchmarking or similar competitive analysis, and prohibit reconstructable transformations
  such as normalization, schema changes, resampling, or timezone shifting.
- Those terms conflict directly with immutable raw retention, provider-specific normalization, and
  the comparative quality harness. Tiingo must not be queried or implemented for this bake-off
  without written permission covering those uses.

### Alpha Vantage — not selected

- The official [API documentation](https://www.alphavantage.co/documentation/) documents daily and
  global-equity functions, but the historical intraday and adjusted-daily capabilities relevant to
  this experiment are premium features.
- The [premium page](https://www.alphavantage.co/premium/) documents a free allowance of 25
  requests per day. This is less accessible for the intended daily, 5-minute, corporate-action,
  and pagination comparison than Alpaca Basic.

### EODHD and Finnhub — not selected

- EODHD documents daily data across more than 70 exchanges and five-minute history from 2020, but
  the relevant [intraday access](https://eodhd.com/financial-apis/intraday-historical-data-api) is
  paid. Its [corporate-action API](https://eodhd.com/financial-apis/api-splits-dividends) and
  identifier mappings are technically relevant.
- EODHD's [standard terms](https://eodhd.com/financial-apis/terms-conditions) require stored copies
  to be deleted after the subscription ends. That conflicts with long-lived immutable raw
  artifacts unless a separate agreement changes the retention right.
- Finnhub documents strong FIGI/MIC reference fields and paid US/international history, but splits
  and dividends are premium and its public candle-adjustment semantics are not sufficiently precise
  for this experiment. The [market-data pricing](https://www.finnhub.io/pricing-stock-api-market-data)
  does not provide a no-cost like-for-like path.
- Finnhub's [terms](https://finnhub.io/terms-of-service) require deletion of data when the service
  ends and restrict sharing data or derived results without approval. These constraints are not a
  fit for the present reproducibility model.

## Direct observations

- Git baseline `0355d3f Complete Phase 0 foundation`, local `main`, and `origin/main` were aligned
  before the feature branch was created.
- The existing CI runs on both `push` and `pull_request`; it is offline and does not expose provider
  credentials. No CI change is needed for this selection stage.
- At the selection checkpoint the repository had no live adapter or opt-in preflight mechanism.
  Subsequently, Massive and Alpaca adapters, provider-specific normalizers, and a transient Alpaca
  SIP preflight were implemented and tested offline without a vendor SDK. That implementation does
  not constitute a live provider observation.
- None of `MASSIVE_API_KEY`, `APCA_API_KEY_ID`, or `APCA_API_SECRET_KEY` was configured in the
  current process when checked. Only presence was tested; no value was read or logged.
- No provider endpoint was called. There is therefore no observed claim yet about coverage,
  correctness, latency, error behavior, or actual entitlement.

## Interpretation

Alpaca is the best *technical experiment candidate* for the bounded Phase 1 comparison because it
offers, without a proposed purchase, the closest documented match to all three required datasets:
daily bars, historical 5-minute bars, and corporate actions. It also exposes adjustment controls,
pagination, a provider UUID, and point-in-time ticker behavior that exercise the Phase 0 boundary
meaningfully. This assessment does not establish licensing suitability; durable private raw
retention and the intended non-display uses remain unresolved.

Massive and Alpaca may both derive US stock data from SIP. Their comparison is consequently not a
fully independent validation of the underlying tape. It is still useful for testing aggregation,
trade eligibility, adjustment behavior, availability timing, paging, error semantics, identifier
mapping, and provider-specific normalization.

Twelve Data has the most promising documented international-equity trajectory of this shortlist,
but its partial standard US intraday feed and paid corporate-action access would weaken this
specific no-purchase experiment. Tiingo's current contract conflicts with the experiment itself.
Alpha Vantage, EODHD, and Finnhub require paid capabilities for a like-for-like intraday comparison;
EODHD and Finnhub also have incompatible standard retention obligations.

## Unresolved questions and mandatory preflight

The following are not provider-quality findings; they must be resolved before or during the first
minimal live calls:

1. Does the actual Alpaca account authorize `feed=sip` for requests ending more than 15 minutes in
   the past, or only IEX?
2. Is the corporate-actions endpoint enabled for that account and region?
3. Which Alpaca and SIP subscriber terms apply to the account, and do they authorize immutable
   private raw retention for this research workflow?
4. Which Massive product/tier is actually configured, and do its applicable terms authorize the
   same retention and normalization?
5. Do corporate-action fields from either provider expose cases that cannot be represented by the
   Phase 0 canonical model without losing a required date or action type?
6. Do early-close daily bars require a small experiment-specific session table, or will the
   Phase 0 daily interval model require an approved change?
7. Can yfinance be kept isolated as a reference tool without adding overlapping Pandas/NumPy
   dependencies to the production environment?

If the one SIP entitlement preflight denies SIP, or the later corporate-action access check denies
that endpoint, stop and request a new decision. If the applicable license forbids the required raw
retention **or retention remains unresolved**, stop before substantive download or persistence. Do
not substitute a different feed, buy a plan, run substantive ingestion without the required
raw-artifact path, or weaken the canonical model. The entitlement-only SIP preflight is deliberately
transient, may precede retention resolution, and persists no payload.

## Approval scope and resume point

The approval authorizes the Alpaca adapter and bounded bake-off work under these conditions:

1. keep working on `phase-1-provider-bakeoff`;
2. define the 10–20-instrument sample and bounded date windows;
3. estimate requests and response size against the configured tiers;
4. implement offline-tested Massive and Alpaca provider boundaries and normalizers without vendor
   SDKs unless a concrete dependency is separately justified;
5. require `MASSIVE_API_KEY`, `APCA_API_KEY_ID`, and `APCA_API_SECRET_KEY` only through the process
   environment;
6. make exactly the bounded transient SIP entitlement preflight first, then treat any later
   corporate-action endpoint check and the contractual retention review as separate gates;
7. preserve substantive raw artifacts privately only after retention rights are affirmatively
   established; forbidden or unresolved retention stops substantive download and persistence; and
   keep every ordinary test offline and deterministic.

The adapters, transient SIP preflight, normalizers, and offline harness are now implemented on the
same feature branch. The next live step remains the one transient historical-SIP entitlement
preflight after local credentials are available. A later corporate-action endpoint access check and
the contractual retention review remain distinct open items; no IEX substitution, paid upgrade, or
raw persistence is authorized by the Provider 2 approval.
This stage changes no Phase 0 invariant and makes no recommendation yet about the final canonical
provider.
