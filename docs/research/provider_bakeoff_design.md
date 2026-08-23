# Provider bake-off design

> **Status: DESIGNED, NOT EXECUTED**
>
> This document preregisters a bounded Phase 1 experiment. At preregistration no provider had been
> called, no credential had been used, and no market-data result in this document was observed
> evidence. Later observations must be recorded in the provider quality report. Purchases,
> paid-plan activation, and automatic fallback to a non-comparable feed are not authorized.

> **Provider 1 redesign note (2026-08-23):** Massive is retained as a technically validated
> candidate whose full real bake-off is blocked by its standard Individual market-data licensing.
> A user-approved documentary preflight considered Twelve Data as the operational replacement.
> Twelve Data Basic passed the bars, history, request-budget, and internal non-display gates but
> failed the mandatory no-purchase corporate-actions gate because `/splits` and `/dividends`
> require Grow or above. No Twelve Data adapter or revised execution matrix was implemented. See
> [Provider 1 redesign](provider-1-redesign.md). The frozen sample and time windows below remain
> reusable after a new Provider 1 decision.

## 1. Objective and evidence labels

The experiment compares Massive Stocks Basic and Alpaca Trading API Basic for the first real
market-data path through the Phase 0 foundation. It uses yfinance only as a later sanity check,
never as a candidate canonical provider.

Every statement in the design and later report must use one of these labels:

- **[DOC] Provider documentation:** a claim made by an official provider, exchange, issuer, or
  regulator source, with the source and access date recorded.
- **[OBS] Observed evidence:** a result measured from a stored raw batch, a sanitized failed-call
  record, or the sanitized outcome of the explicitly transient Alpaca SIP entitlement preflight.
  That preflight is observed API evidence even on success, but deliberately has no `RawBatch`, raw
  checksum, or persisted response body.
- **[DESIGN] Evaluation or decision:** an intentional experimental choice, tolerance, or scope
  boundary. It is not a provider fact.
- **[EST] Inference or estimate:** a calculation based on documented limits and the designed
  sample. It must be replaced or qualified by observed values after execution.

The study must not treat agreement between providers as proof of correctness or disagreement as
proof that either provider is wrong.

## 2. Official documentation snapshot

Sources were accessed on **2026-08-19**. Current access and plan terms must be rechecked at the
retention and tier gate immediately before a live run.

| Subject | Label | Preregistered fact | Official source |
| --- | --- | --- | --- |
| Massive Stocks Basic | [DOC] | Free, 5 API calls/minute, two years of history, end-of-day data, reference data, corporate actions, and minute aggregates | [Massive pricing](https://massive.com/pricing?product=stocks) |
| Massive custom bars | [DOC] | One ticker per path; `adjusted=true` adjusts for splits; limit defaults to 5,000 base aggregates and has a maximum of 50,000 | [Custom Bars](https://massive.com/docs/rest/stocks/aggregates/custom-bars) |
| Massive splits | [DOC] | Split execution date, old/new share ratio, adjustment type, and pagination are exposed | [Splits](https://massive.com/docs/rest/stocks/corporate-actions/splits) |
| Massive dividends | [DOC] | Ex-, declaration-, record-, and pay-date fields and cash amounts are exposed | [Dividends](https://massive.com/docs/rest/stocks/corporate-actions/dividends) |
| Massive ticker events | [DOC] | The experimental endpoint currently exposes ticker-change events; Basic advertises two years of event history | [Ticker Events](https://massive.com/docs/rest/stocks/corporate-actions/ticker-events) |
| Alpaca Trading API Basic | [DOC] | Free, historical data since 2016, latest-15-minute restriction, and 200 historical API calls/minute | [Market Data plans](https://docs.alpaca.markets/us/docs/about-market-data-api) |
| Alpaca historical bars | [DOC] | Multi-symbol request, limit 10,000 points/page, page tokens, `raw`, `split`, `dividend`, `spin-off`, and `all` adjustments, plus symbol `asof` mapping | [Historical Bars](https://docs.alpaca.markets/us/v1.4.2/reference/stockbars) |
| Alpaca corporate actions | [DOC] | Multi-symbol query, limit 1,000/page, page tokens, multiple action types, and filtering by `process_date` | [Corporate Actions](https://docs.alpaca.markets/us/reference/corporateactions-1) |
| Alpaca historical SIP access | [DOC] | The FAQ says historical SIP queries ending at least 15 minutes ago can be queried without a paid subscription | [Market Data FAQ](https://docs.alpaca.markets/us/docs/market-data-faq) |
| Alpaca feed description | [DOC] | A separate page describes IEX as the only feed usable without a subscription | [Historical Stock Data](https://docs.alpaca.markets/us/docs/historical-stock-data-1) |
| 2025 US sessions | [DOC] | Exchange holidays and 13:00 ET early closes are published by NYSE | [NYSE 2025 calendar](https://www.nyse.com/publicdocs/ICE_NYSE_2025_Yearly_Trading_Calendar.pdf) |

The two Alpaca feed statements are not silently reconciled. They create the SIP preflight stop
rule in Section 10.

## 3. Frozen sample

The sample has 15 common stocks and one ETF. UUIDs are assigned once and are not derived from
tickers. Provider identifiers are separate temporal external identifiers and must be resolved from
each provider's reference response before bar retrieval. In particular, the identifiers for
`BRK.B` must not be assumed to use the same punctuation across providers.

| Current symbol | Internal UUID | Design role and motivation |
| --- | --- | --- |
| AAPL | `1923431d-8907-4f63-ba11-68182c11f778` | Mega-cap technology baseline; Nasdaq; highly liquid; recurring dividend |
| MSFT | `b0793b0a-644e-4739-a75e-f20a244b478b` | Mega-cap technology; highly liquid; recurring dividend |
| NVDA | `b8c8923d-0c66-4cb9-ac35-85072ecbadc1` | Mega-cap semiconductor; high volume and volatility |
| AMZN | `84349b31-9a65-48f6-b84f-ed451a756af2` | Consumer discretionary; liquid negative control for cash dividends |
| NFLX | `78795cab-5e96-4617-b6cb-a452d443b2f8` | Communication services; liquid; 10-for-1 split in the core window |
| JPM | `20951dc1-e959-4bb3-b21e-2725c6e7d30d` | Mega-cap bank; NYSE; recurring dividend |
| IBKR | `ad2c358c-f495-4267-b6d3-d4e9f75c6ce9` | Brokerage; liquid; 4-for-1 split in the core window |
| BRK.B | `73c80583-2161-412b-8843-2d8fbc51353f` | Large financial; share-class and punctuation identifier edge case |
| XYZ | `eaef544a-6b12-4077-b866-2206b2a64832` | Financial technology; same issuer changed NYSE ticker from SQ to XYZ |
| XOM | `d4937c21-210c-474c-bc55-47dd2c415b7e` | Mega-cap energy; NYSE; recurring dividend |
| JNJ | `d8dcd9b0-96fe-4c6c-b2cd-2a5616239791` | Large healthcare; NYSE; recurring dividend |
| KO | `93f71c2d-6377-489f-99b0-1989fe7009e4` | Consumer staples; recurring cash-dividend validation target |
| BA | `1fdcf62d-840a-4b10-a799-ef8929ab06b8` | Industrial; liquid and volatile |
| ORLY | `53a26cfb-760f-4fb6-ba2f-a5ba314ee794` | Consumer discretionary; 15-for-1 split in the core window |
| NEE | `dd2d973e-0a0d-47b1-a250-7aab0b43ce71` | Utility; NYSE; recurring dividend |
| SPY | `7d1a5577-94da-4267-a1fb-e9bd3aa2555c` | Highly liquid ETF benchmark and asset-class control |

Issuer evidence for the intentional event cases:

- **[DOC]** Block's NYSE symbol changed from SQ to XYZ effective 2025-01-21, without a CUSIP
  change: [Block investor relations](https://investors.block.xyz/investor-news/news-details/2025/Block-Announces-Ticker-Symbol-Change-to-XYZ-To-Report-Fourth-Quarter-Results/default.aspx).
- **[DOC]** O'Reilly trading reflected a 15-for-1 split on 2025-06-10:
  [O'Reilly split history](https://corporate.oreillyauto.com/investor-relations-information/stock-split-history/).
- **[DOC]** Interactive Brokers expected split-adjusted trading to begin on 2025-06-18:
  [SEC-filed issuer release](https://www.sec.gov/Archives/edgar/data/1381197/000138119725000043/ikr-8k_20250331er.htm).
- **[DOC]** Netflix expected split-adjusted trading to begin on 2025-11-17:
  [Netflix investor relations](https://ir.netflix.net/investor-news-and-events/financial-releases/press-release-details/2025/Netflix-Announces-Ten-For-One-Stock-Split/default.aspx).

These issuer facts select test cases; they are not evidence about either provider's payload.

## 4. Half-open experiment windows

All internal bar requests use aware UTC half-open intervals. Provider endpoints with inclusive end
parameters may receive a provider-specific inclusive bound, but normalization must retain only
records whose canonical start is in `[start, end)`. The raw response is preserved unchanged before
that filter.

### 4.1 Daily core

| Property | Preregistered value |
| --- | --- |
| Internal request | `[2025-05-27T00:00:00Z, 2025-12-06T00:00:00Z)` |
| Provider date range | 2025-05-27 through 2025-12-05 inclusive |
| Instruments | All 16 |
| Adjustment requests | Unadjusted and split-adjusted |
| Expected sessions | 135 per instrument |
| Expected canonical rows | 2,160 per adjustment; 4,320 total |

**[EST]** The session count is the weekday count in the range less the four closed weekdays in
the limited oracle below. It is an expectation to test, not observed provider coverage.

The window contains the ORLY, IBKR, and NFLX splits; Juneteenth; Independence Day and its early
close; Labor Day; the autumn DST transition; Thanksgiving and its early close; and multiple cash
dividends.

### 4.2 Ticker continuity

| Property | Preregistered value |
| --- | --- |
| Internal request | `[2025-01-13T00:00:00Z, 2025-01-29T00:00:00Z)` |
| Instrument | Block internal UUID above |
| Temporal identifiers | SQ before 2025-01-21; XYZ on and after 2025-01-21 |
| Adjustment | Unadjusted |
| Expected sessions | 11 total; 2025-01-20 is closed |

Massive receives bounded requests for the applicable temporal symbol plus a separate ticker-event
request. Alpaca receives a current-symbol request with an explicit post-change `asof` date and a
symbol-mapping-disabled negative control. Only the preregistered continuity path is eligible for
canonical persistence; the negative control is comparison evidence.

### 4.3 Corporate actions

| Property | Preregistered value |
| --- | --- |
| Canonical effective-date target | `[2025-01-01, 2025-12-06)` |
| Instruments | All 16 |
| Target types | Forward/reverse split, cash dividend, and ticker/name change where available |
| Pagination | Consume until no provider next-page token/URL remains |

Provider query dates are not automatically claimed to have effective-date semantics. Alpaca's
documented `process_date` filter is recorded separately from the action-specific effective or
ex-date used for canonical filtering. Querying the target effective-date bounds as `process_date`
cannot establish complete action coverage: an action effective inside the target window may have
been processed outside it. No retrieval halo is preregistered or authorized in Phase 1, so Alpaca
corporate-action completeness must remain explicitly unresolved unless official semantics or live
evidence close this gap.

### 4.4 Dividend-adjustment evidence probe

| Property | Preregistered value |
| --- | --- |
| Internal request | `[2025-06-09T00:00:00Z, 2025-06-21T00:00:00Z)` |
| Instrument | KO |
| Expected sessions | 9; 2025-06-19 is closed |
| Alpaca variants | `dividend` and `all` |
| Persistence | Raw evidence and comparison metrics only unless Section 7 permits mapping |

Massive does not receive a dividend-adjusted bar request because its custom-bar documentation
defines the adjusted flag as split adjustment, not dividend adjustment.

## 5. Limited session oracle and 5-minute windows

This oracle exists only for the fixed experiment. It is not a production trading calendar. Normal
session bounds are derived with `America/New_York`/`zoneinfo`; the listed closures and early closes
are explicit exchange facts.

### 5.1 Daily-core exceptions

| Session date | Expected state | Canonical UTC bounds when open |
| --- | --- | --- |
| 2025-06-19 | Closed | None |
| 2025-07-03 | Early close | `[2025-07-03T13:30:00Z, 2025-07-03T17:00:00Z)` |
| 2025-07-04 | Closed | None |
| 2025-09-01 | Closed | None |
| 2025-11-27 | Closed | None |
| 2025-11-28 | Early close | `[2025-11-28T14:30:00Z, 2025-11-28T18:00:00Z)` |

All other weekdays in the daily core are expected to use nominal regular-session bounds. This
expectation applies only to the named sample and window.

### 5.2 Calendar/DST 5-minute probes

Each row is a separate unadjusted request for all 16 instruments.

| Session | Purpose | Canonical UTC interval | Expected bars/instrument |
| --- | --- | --- | ---: |
| 2025-03-07 | Before spring DST | `[2025-03-07T14:30:00Z, 2025-03-07T21:00:00Z)` | 78 |
| 2025-03-10 | After spring DST | `[2025-03-10T13:30:00Z, 2025-03-10T20:00:00Z)` | 78 |
| 2025-07-02 | Ordinary EDT session | `[2025-07-02T13:30:00Z, 2025-07-02T20:00:00Z)` | 78 |
| 2025-07-03 | Early close | `[2025-07-03T13:30:00Z, 2025-07-03T17:00:00Z)` | 42 |
| 2025-10-31 | Before autumn DST | `[2025-10-31T13:30:00Z, 2025-10-31T20:00:00Z)` | 78 |
| 2025-11-03 | After autumn DST | `[2025-11-03T14:30:00Z, 2025-11-03T21:00:00Z)` | 78 |

Expected total: **432 bars/instrument and 6,912 bars across the sample**. A missing bar is first a
coverage observation. It is not automatically a provider error because both providers document
trade-qualification rules that can suppress empty aggregates.

### 5.3 Split-boundary 5-minute probes

The regular sessions immediately before and on the split-adjusted trading date are queried in both
unadjusted and split-adjusted form:

| Instrument | Pre-event session | Post-event session | Ratio expected from issuer evidence |
| --- | --- | --- | ---: |
| ORLY | 2025-06-09 | 2025-06-10 | 15:1 |
| IBKR | 2025-06-17 | 2025-06-18 | 4:1 |
| NFLX | 2025-11-14 | 2025-11-17 | 10:1 |

All six sessions are nominal RTH sessions. Expected total: 468 rows per adjustment and 936 rows
across the two adjustment states.

## 6. Provider request mapping

### 6.1 Massive

- Use custom aggregates with ascending sort.
- Use `1/day` for daily and `5/minute` for five-minute data.
- Set `limit=50000`; every designed request is far below that base-aggregate limit.
- Use `adjusted=false` and `adjusted=true` as separate immutable raw batches.
- Use one provider ticker per aggregates call because the symbol is a path parameter.
- Never persist the authenticated request URL or API key. Persist only the logical endpoint and
  sanitized dimensions such as symbol, interval, adjustment, page number, and elapsed milliseconds.

### 6.2 Alpaca

- Use the multi-symbol historical-bars endpoint with `limit=10000` and ascending sort.
- Request `feed=sip` only after the SIP preflight in Section 10 passes.
- Use `raw` and `split` as the persistable core adjustment requests.
- Follow every `next_page_token`; never assume that a short page is the last page.
- Record the requested `asof` behavior for XYZ/SQ explicitly.
- Treat the corporate-action request's `process_date` bounds as provider query semantics, then
  retain the action-specific dates separately during comparison.

## 7. Adjustment matrix

| Provider/use | Wire option | Documented meaning | Canonical state | Canonical persistence |
| --- | --- | --- | --- | --- |
| Massive core | `adjusted=false` | No split adjustment | `unadjusted` | Yes |
| Massive core | `adjusted=true` | Split-adjusted bars | `split_adjusted` | Yes, subject to payload verification |
| Alpaca core | `adjustment=raw` | No adjustment | `unadjusted` | Yes |
| Alpaca core | `adjustment=split` | Forward/reverse split adjustment | `split_adjusted` | Yes, subject to payload verification |
| Alpaca evidence probe | `adjustment=dividend` | Cash-dividend price adjustment without necessarily applying splits | No exact Phase 0 enum | No; raw evidence only |
| Alpaca evidence probe | `adjustment=all` | Split, dividend, spin-off, and other documented adjustments | Not exactly `split_and_dividend_adjusted` | No; raw evidence or `provider_adjusted_unknown` only after review |
| yfinance sanity check | Version/configuration TBD | Unofficial reference behavior | Never canonical | No |

No normalizer may label a series more specifically than provider documentation and observed fields
support. If a required series has no exact Phase 0 adjustment state, stop before changing the enum.

## 8. Request and rate-limit budget

Counts assume one page where the documented limit is sufficient, no retries, and no failed
preflights. Any next page adds one request and one raw batch.

### 8.1 Massive Stocks Basic

| Operation | Nominal requests |
| --- | ---: |
| One reference detail per instrument | 16 |
| Daily core: 16 instruments x 2 adjustment states | 32 |
| Six calendar/DST sessions x 16 instruments | 96 |
| Six split session-instrument pairs x 2 adjustment states | 12 |
| SQ and XYZ temporal bar segments | 2 |
| One split lookup per instrument | 16 |
| One dividend lookup per instrument | 16 |
| XYZ ticker-event lookup | 1 |
| **Total** | **191** |

**[EST]** At the documented 5 calls/minute, the theoretical lower bound is 38.2 minutes. The live
run must budget 40-45 minutes and obey a conservative 12-second spacing or an equivalently safe
windowed throttle. This is measurement control, not a production retry/rate-limit subsystem.

### 8.2 Alpaca Trading API Basic

| Operation | Nominal requests |
| --- | ---: |
| One asset detail per instrument | 16 |
| Daily multi-symbol raw and split | 2 |
| Six multi-symbol calendar/DST sessions | 6 |
| Six split session-instrument pairs x 2 adjustment states | 12 |
| XYZ mapped query and symbol-mapping negative control | 2 |
| One multi-symbol corporate-action page | 1 |
| KO `dividend` and `all` evidence probes | 2 |
| **Total** | **41** |

**[EST]** The documented 200 historical-API-calls/minute figure is applied only to the 24 planned
historical-bars calls; the mandatory successful SIP preflight would add one more historical-bars
call. It must not be assumed to govern the 16 asset-detail calls or the corporate-actions endpoint,
whose current endpoint/account limits must be checked separately. The overall 41-call design count
therefore is not one homogeneous 200-calls/minute bucket. Execution should remain sequential or
conservatively paced so latency and endpoint-specific rate-limit headers remain interpretable. A
corporate-action page token increases the count.

### 8.3 yfinance

yfinance is excluded from provider request-rate estimates because it has no provider contract in
this study and its internal HTTP-call count is not a stable experimental input. Its version,
configuration, exact symbols, and bounded windows must be recorded before the sanity check. It
must not broaden the live dataset.

## 9. Estimated data volume

| Component | Expected canonical rows |
| --- | ---: |
| Daily core, raw and split | 4,320 |
| Calendar/DST intraday | 6,912 |
| Split-boundary intraday, raw and split | 936 |
| Canonical ticker-continuity path | 11 |
| **Core total/provider** | **12,179** |
| Alpaca dividend evidence variants | 18 additional, not canonical |

**[EST]** At approximately 150-300 bytes per compact JSON bar plus response wrappers, bar payloads
should be about 1.8-3.7 MB/provider. Reference data, corporate actions, and manifests are expected
to remain below roughly 1 MB/provider. The conservative gate is **less than 5 MB raw/provider and
less than 10 MB combined**. Expected Zstandard Parquet output is approximately 0.5-2 MB/provider.

These are planning estimates. Actual raw artifact byte counts and compressed output sizes become
[OBS] values only after immutable persistence. If a preflight indicates a response or page plan
would exceed these bounds materially, stop and revise the budget before downloading.

## 10. Mandatory preflight and stop rules

Run these checks in order before the first substantive download.

### 10.1 Tier and credential gate

1. Confirm that only Massive Stocks Basic and Alpaca Trading API Basic are active.
2. Confirm the required environment variables exist without printing their values.
3. Do not activate, trial, or purchase a paid plan.
4. Recheck current rate limits, history, endpoint access, and licensing from official sources.

A credential, tier, or access failure stops the affected provider before the core dataset. A clear
contractual restriction on the temporary non-display processing required by this experiment also
stops the affected provider. Ambiguity limited to *durable* retention does not by itself stop the
run: if temporary private processing is not clearly restricted, use a private temporary data root
outside the repository for the complete pipeline, then delete raw and analytical artifacts after
retaining only permitted sanitized aggregate evidence. This later Phase 1 operating policy
supersedes the stricter retention-ambiguity stop recorded at preregistration.

The single transient SIP entitlement preflight in Section 10.3 always discards its response body
and persists no raw payload. It establishes technical entitlement only; it does not establish
temporary-processing or durable-retention rights.

### 10.2 Massive rolling-retention gate

Massive Basic advertises two years of history. The earliest designed corporate-action bound is
2025-01-01, the earliest bar bound is 2025-01-13, and the earliest intraday probe is 2025-03-07.
Immediately before execution:

1. calculate the earliest date advertised as accessible for the authenticated Basic account;
2. verify every designed start lies inside that boundary;
3. perform only a minimal one-symbol/one-day availability probe after that documentary check.

If any required window is outside the accessible tier, **stop**. Do not silently move dates, omit
an event, activate a paid plan, or substitute already adjusted third-party data. Report the gate
failure and request approval for a redesigned sample/window.

Because the limit is rolling, future reproduction from Basic may cease to be possible even though
the design is fixed. Raw batch IDs, manifests, retrieval date, tier, and code revision therefore
belong in the final reproducibility record.

### 10.3 Alpaca historical SIP gate

Before the multi-symbol run, issue at most one authenticated, old, one-symbol, one-five-minute-bar
historical-bars probe with explicit `feed=sip`, ending well over 15 minutes in the past.

- If it succeeds, discard the response body after counting the returned observations and retain
  only a sanitized, non-payload entitlement result. A successful HTTP call proves technical
  access; it does not prove a contractual right to retain raw Alpaca market data. Classify the
  sanitized result as **[OBS] observed evidence**, recording only safe request dimensions, feed,
  status/outcome, observation count, latency, sanitized request ID, and safe rate-limit scalars.
- If it returns a permission/subscription error, **stop the cross-provider core run**. Preserve a
  sanitized failure observation, report the official-documentation conflict, and request a user
  decision.
- Do not automatically fall back to `feed=iex`. An IEX-only experiment is definitionally different
  from Massive's consolidated coverage and requires explicit approval as a separate experiment.
- Do not purchase or activate Algo Trader Plus.

The successful Alpaca preflight body is never persisted. For the substantive run, durable
retention ambiguity permits only a private external temporary root when temporary personal
processing is not clearly restricted. The same documentary assessment applies independently to
Massive. A provider whose applicable terms clearly restrict this experiment's non-display pipeline
cannot enter the substantive run without a separate license or written authorization.

The successful transient preflight is an intentional exception to ordinary successful-page
evidence rules: it has no `RawBatch`, batch ID, payload checksum, persisted page, or replay artifact.
The later corporate-action endpoint access check is a separate entitlement check; SIP success does
not prove it, and neither API result resolves the contractual retention review.

### 10.4 Pagination and size gate

After each authorized **substantive** response, record status, elapsed time, sanitized provider
request ID, row count, page number, byte count, checksum, and whether a next page exists. Durable
artifacts require affirmative retention rights; otherwise these records and the complete
raw-to-query flow live only in the external temporary root and are deleted at run end. Never log a
next URL, page token, authenticated URL, or credential. The transient SIP preflight follows the
sanitized evidence rule in Section 10.3 instead and is exempt from raw-page/checksum requirements.
Stop if pagination or bytes materially exceed the preregistered budget before continuing to later
symbols.

## 11. Canonical-model and foundation stop risks

These are known representational risks, not approved architecture changes.

1. **Daily interval semantics.** Provider daily labels may identify a market date or midnight,
   while `PriceBar` requires the actual session interval. Normalization must use the limited oracle
   for this experiment. It must not invent a 16:00 ET close on an early-close day.
2. **Corporate-action query semantics.** Alpaca filters by `process_date`, while
   `CorporateActionRequest` promises effective-date bounds. No retrieval halo is preregistered in
   this experiment, so complete effective-date coverage cannot be claimed. Adding a bounded halo
   would itself require an explicit design update and still could not be represented as a
   production completeness guarantee without provider semantics that justify it.
3. **Dividend dates.** Providers expose ex-, declaration-, record-, pay-, and processing dates;
   `DividendAction` retains only one `effective_date`. The preregistered canonical candidate is the
   ex-date, but losing other dates must be reported before adopting it as sufficient.
4. **Stock dividend versus split.** ORLY and IBKR describe splits effected as stock dividends.
   Providers may classify these differently; the normalizer must not silently collapse a distinct
   provider action into `SplitAction` without documented equivalence.
5. **Adjustment states.** Dividend-only adjustment and Alpaca `all` do not map exactly to Phase 0
   enums. Evidence-only handling is required unless a model change is approved.
6. **Instrument discovery.** `MarketDataProvider.get_instruments(as_of=...)` is not sample-bounded.
   Per-symbol provider reference calls are part of this experiment, but changing the public
   contract requires approval.
7. **Unknown identifier validity start.** Do not fabricate `valid_from` for SQ. Ticker continuity
   can be represented by `TickerChangeAction` and bounded provider references, but a complete
   identifier history may expose a model gap.
8. **Corporate-action analytical storage.** Phase 0 implements Parquet/DuckDB storage only for
   price bars. Adding an authoritative corporate-action or reference-data store is not implicit in
   this design.
9. **Availability time.** If providers do not expose earliest availability, `available_at` remains
   null. Retrieval or ingestion time must never be substituted.

If live evidence makes any of these changes necessary to complete the bake-off correctly, stop
after preserving the raw evidence and submit the observed case, proposed change, alternatives, and
foundation impact for approval.

## 12. Results record to complete after execution

The final provider quality report must link this design and record:

- code revision, study date, provider tier, documentation access date, and yfinance version;
- provider identifiers resolved for every internal UUID;
- raw batch IDs and manifests for every successfully persisted substantive page, plus a separately
  labeled sanitized SIP-preflight observation with no raw batch, checksum, or payload;
- sanitized failed-call evidence and observed latency/rate-limit headers;
- requested and returned intervals, page counts, row counts, and bytes;
- expected keys, missing keys, duplicates, and out-of-window records;
- OHLC and volume absolute/relative differences without assuming a winner;
- timestamp label, session, DST, early-close, daily, and adjustment behavior;
- corporate-action date/type mapping and every structurally unnormalizable raw record;
- classification of each discrepancy as definitional, adjustment, timing/session, missing,
  likely-provider, or unresolved;
- separate sections for **Observed evidence**, **Provider documentation**, **Interpretation**, and
  **Unresolved questions**.

Aside from the explicitly transient entitlement observation, until those fields are backed by
stored raw evidence no primary-provider recommendation is authorized.
