# Databento evaluation — post-Phase 1 research

> **Status: RESEARCH CHECKPOINT COMPLETE — NO ADAPTER OR PHASE 2 IMPLEMENTATION**
>
> This evaluation is separate from the completed Phase 1 provider bake-off. It does not amend
> `provider_quality_report.md`, does not select a replacement canonical provider, and does not
> change the frozen Phase 0/1 architecture. Real market data was processed in memory only and was
> not retained in the repository or on disk.

## Executive conclusion

Databento is technically credible for two distinct roles, but the account and evidence do not
support adopting it as the Investment Platform's global US-equity provider today:

- **Investment Platform:** `EQUS.SUMMARY` is a promising consolidated end-of-day supplement, with
  a specific final-summary/post-market semantic. `XNAS.BASIC` is useful Nasdaq-centered intraday
  data but is not a consolidated US SIP. The account did not expose `EQUS.SIP`. Databento's
  reference products document strong corporate-action, adjustment-factor, stable-identifier, and
  point-in-time capabilities, but all three tested data endpoints returned HTTP 403 for this
  account. Durable raw and reversible-normalized retention rights remain unresolved, and the
  public User Agreement expressly ends further use of third-party data after termination.
- **Options research:** `OPRA.PILLAR` is a strong specialized candidate. The evaluation resolved an
  SPXW option definition, observed separate consolidated bid/ask prices and sizes, identified the
  contributing venues, and observed venue-attributed trades. Parent metadata also confirmed
  `SPX.OPT`. This is enough to justify a future, separately authorized options design experiment,
  not a full chain loader or volatility-surface implementation.

The summed pre-download estimate for all six market-data downloads was **$0.000109992922**. Every
download was preceded by record-count, billable-size, and cost checks. No request approached the
approved $0.25 per-request or $1.00 total gates. Databento documents that estimates can overstate
short ranges that are not multiples of ten minutes; definition estimates are accurate on full-day
boundaries, as used here. The exact posted promotional-credit debit was not exposed
programmatically during this evaluation; the Databento portal is the authoritative place to
confirm posted usage.

## Evidence taxonomy

Statements use these labels:

- **DOCUMENTED:** stated in current official Databento or publisher material linked below.
- **ACCOUNT VISIBLE:** returned by free authenticated metadata; this is not by itself proof of every
  subscription or product entitlement.
- **ACCOUNT ENTITLED:** a requested data operation succeeded for this account.
- **OBSERVED:** measured directly in the bounded evaluation.
- **INTERPRETATION:** a project conclusion derived from documented and observed evidence.
- **UNRESOLVED:** not established by the available contract, documentation, account, or microtest.

## Scope, safety, and method

The evaluation ran on **2026-08-26** from `research/databento-evaluation`, based on the Phase 1
merge commit `c825537`. The official Python client was used in an isolated environment at observed
version `0.85.0`; Databento was not added to `pyproject.toml` or `uv.lock`. Authentication came only
from `DATABENTO_API_KEY` in process scope. Its value, prefix, suffix, and fingerprint were never
printed or persisted.

The experiment deliberately did not:

- implement a `MarketDataProvider` adapter;
- alter the canonical bar or corporate-action models;
- download a complete option chain;
- resample one-minute bars to five minutes;
- construct an options surface, backtest, ingestion scheduler, or persistent history;
- make a cash purchase, activate a subscription, upgrade, or change any account entitlement;
- use Reference data after the account returned an authorization failure;
- save a real DBN, CSV, JSON, Parquet, DuckDB, raw, or normalized artifact.

For each billable request, the order was:

```text
authenticated metadata
-> record count
-> billable bytes
-> estimated cost
-> cost-gate decision
-> bounded in-memory time-series request
-> structural inspection
-> object disposal
```

Official documentation confirms that metadata, symbology, and account-management access is free,
and that `metadata.get_cost` is the supported pre-download estimator. Historical data is metered by
uncompressed DBN size. See the [Historical API](https://databento.com/docs/api-reference-historical),
[pricing page](https://databento.com/pricing/), and
[usage/credits FAQ](https://databento.com/docs/faqs/usage-pricing-and-data-credits).

## Authenticated discovery

### Dataset visibility

**ACCOUNT VISIBLE:** authentication succeeded and `metadata.list_datasets` returned 29 dataset
IDs:

```text
ARCX.PILLAR
BATS.PITCH
BATY.PITCH
DBEQ.BASIC
EDGA.PITCH
EDGX.PITCH
EPRL.DOM
EQUS.MINI
EQUS.SUMMARY
GLBX.MDP3
IEXG.TOPS
IFEU.IMPACT
IFLL.IMPACT
IFUS.IMPACT
MEMX.MEMOIR
NDEX.IMPACT
OCEA.MEMOIR
OPRA.PILLAR
XASE.PILLAR
XBOS.ITCH
XCBF.PITCH
XCHI.PILLAR
XCIS.TRADESBBO
XEEE.EOBI
XEUR.EOBI
XNAS.BASIC
XNAS.ITCH
XNYS.PILLAR
XPSX.ITCH
```

`EQUS.SUMMARY`, `XNAS.BASIC`, and `OPRA.PILLAR` were present. `EQUS.SIP` was not present.
Successful microdownloads subsequently established historical time-series entitlement only for
the exact datasets, schemas, symbols, dates, and limits tested. Visibility must not be generalized
to paid Reference access or to unlisted `EQUS.SIP`.

### Dataset metadata observed through the official API

| Dataset | Schemas relevant to this evaluation | Observed range | Observed unit prices |
| --- | --- | --- | --- |
| `EQUS.SUMMARY` | `ohlcv-1d`, `definition`, `statistics` | 2024-07-01 through the 2026-08-26 metadata boundary | `ohlcv-1d` $30/GB; `definition` and `statistics` $16/GB |
| `XNAS.BASIC` | `cmbp-1`, `tcbbo`, `trades`, `cbbo-1s`, `cbbo-1m`, `ohlcv-1s`, `ohlcv-1m`, `ohlcv-1h`, `ohlcv-1d`, `definition`, `statistics`, `status` | 2024-07-01 through the 2026-08-26 metadata boundary | `ohlcv-1m` $12/GB; `trades`/`tcbbo` $6/GB; `cbbo` $4/GB; `cmbp-1` $1.20/GB |
| `OPRA.PILLAR` | `cmbp-1`, `cbbo-1s`, `cbbo-1m`, `tcbbo`, `trades`, `ohlcv-1s`, `ohlcv-1m`, `ohlcv-1h`, `ohlcv-1d`, `statistics`, `status`, `definition` | overall from 2013-04-01; schema-specific starts vary | schema-specific official API prices used by `get_cost` |

Ranges and unit prices in this table are **OBSERVED via official authenticated metadata**, not
contractual promises. The OPRA metadata observed `cmbp-1` and `tcbbo` from 2023-03-28 and
`cbbo-1s` from 2025-02-20. Public OPRA materials describe different schema-history boundaries in
some places, so exact production requests should continue to use dataset-condition metadata.

No evaluated dataset exposed native `ohlcv-5m`. A future canonical five-minute Databento path
would therefore need explicit, deterministic `ohlcv-1m -> ohlcv-5m` resampling, including empty
interval, session-boundary, and calendar rules. That work was not implemented.

## Official dataset semantics

### `EQUS.SUMMARY`

**DOCUMENTED:** Databento receives Nasdaq NLS+ and exposes only `ohlcv-1d`, `statistics`, and
`definition`. Consolidated US volume is normalized into `statistics`; the NLS+ end-of-day summary
is normalized into `ohlcv-1d`. Databento publishes the final summary, around 20:15 ET, which
includes post-market volume and can differ from providers using the earlier 16:15 or 17:00 ET
summary. See the official [EQUS.SUMMARY specification](https://databento.com/docs/venues-and-datasets/equs-summary).

**INTERPRETATION:** this is a specific consolidated end-of-day summary product, not an ordinary
regular-session-only daily bar and not a full consolidated tick feed. Its semantic provenance is a
benefit, but downstream comparison must not label expected close/volume differences as errors.

### `XNAS.BASIC`

**DOCUMENTED:** Nasdaq Basic combines QBBO and NLS/NLS+. Trades include Nasdaq, Nasdaq PSX,
Nasdaq Texas, and two FINRA/Nasdaq trade-reporting facilities; quotes are Nasdaq-only. BBO and
trade events may be out of order because they originate from separate feeds. Trade aggressor side
is unavailable and normalizes to `N`. See the official
[XNAS.BASIC specification](https://databento.com/docs/venues-and-datasets/xnas-basic).

A current marketing description mentions trade aggressor side, while the technical feed
specification says the source does not provide it and `side` is always `N`. This evaluation
privileges the technical specification; the cross-page inconsistency is **UNRESOLVED**.

**INTERPRETATION:** `XNAS.BASIC` is broader than one lit venue for trade volume, but it is not the
US consolidated SIP. It is potentially useful for Nasdaq-centered intraday research; it is not a
like-for-like substitute for Alpaca SIP breadth, NBBO, or consolidated volume.

### `OPRA.PILLAR`

**DOCUMENTED:** OPRA distributes consolidated top-of-book, last-sale, and daily-statistics data
for US equity-options venues. Databento publisher 30 represents the OPRA consolidated BBO; the
`bid_pb_00` and `ask_pb_00` fields identify the contributing best-bid and best-ask venues. Trade
records retain the execution-venue publisher ID. `ts_event` is OPRA's consolidator-processed block
timestamp and `ts_recv` is Databento receipt time. See the official
[OPRA specification](https://databento.com/docs/venues-and-datasets/opra-pillar).

OPRA raw symbols use 21-character OCC/OSI symbology: parent, expiration, call/put, and strike.
Parent symbology uses `[ROOT].OPT`; SPXW is an explicit case where the option parent differs from a
simple underlying ticker. Definition `expiration` has date granularity and is represented at UTC
midnight even though the contract expires during or after a trading session. See
[Databento symbology](https://databento.com/docs/standards-and-conventions/symbology).

The current OPRA specification documents that data before 2023-02-28 was derived from subsampled
history: `cmbp-1`, `cbbo-1s`, and `tcbbo` are unavailable there; `ts_recv` is copied from
`ts_event` and marked `F_BAD_TS_RECV`; best-bid/offer contributor publisher IDs are unavailable;
and historical statistics are limited to open interest. A separate official history announcement
and authenticated schema metadata expose different later boundaries for some schemas. This
official-source mismatch reinforces the need to query dataset conditions for every planned range.

## API and data-call ledger

### Free metadata, documentation, and symbology calls

The following calls did not download billable market-data records:

| Call | Sanitized purpose | Result |
| --- | --- | --- |
| Historical `metadata.list_datasets` | confirm account-visible datasets | 29 IDs; target datasets present; `EQUS.SIP` absent |
| Historical dataset/schema/range/unit-price metadata | enumerate exact candidate capabilities and price basis | succeeded |
| Historical symbology, `AAPL`, `EQUS.SUMMARY` | verify equity mapping | instrument ID 38 for 2025-07-02 |
| Historical symbology, parent candidates | learn OPRA parent output constraints | `SPXW` without `.OPT` rejected as invalid format; parent-to-`raw_symbol` rejected because parent output must be `instrument_id` |
| OPRA definition metadata, `SPX.OPT`, 2025-07-02, `limit=1` | confirm SPX parent availability without downloading a definition | count 1; 360 estimated billable bytes; hypothetical download estimate $0.000001676381; no download performed |
| Reference `corporate_actions.list_events` | inspect documented event vocabulary | succeeded; 60 event definitions observed |
| Reference `corporate_actions.list_enums` | inspect enum vocabulary | succeeded; 235 enum groups observed |

The failed parent-symbology attempts are useful engineering evidence: parent input requires the
`.OPT` form, and the symbology resolver cannot emit raw child symbols directly from a parent. A
bounded `definition` request is the practical chain-discovery boundary.

For every executed download, the exact official method sequence was
`metadata.get_record_count`, `metadata.get_billable_size`, `metadata.get_cost`, then
`timeseries.get_range`. Discovery also used the official dataset/schema/range/unit-price metadata
methods and `symbology.resolve`; Reference enumeration used only the two methods named in the
table. No undocumented endpoint or vendor SDK fallback was used.

### Pre-download cost gate and executed time-series calls

All intervals are half-open UTC ranges. Costs were returned by official `metadata.get_cost` before
each corresponding download.

| # | Dataset/schema | Symbol and interval | Limit | Count | Billable bytes | Estimated USD | Observed rows |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | `EQUS.SUMMARY` / `ohlcv-1d` | `AAPL`, `[2025-07-02 00:00, 2025-07-03 00:00)` | 10 | 1 | 56 | 0.000001564622 | 1 |
| 2 | `XNAS.BASIC` / `ohlcv-1m` | `AAPL`, `[2025-07-02 13:30, 13:35)` | 10 | 5 | 280 | 0.000003129244 | 5 |
| 3 | `OPRA.PILLAR` / `definition` | parent `SPXW.OPT`, `[2025-07-02, 2025-07-03)` | 25 | 25 | 9,000 | 0.000041909516 | 25 |
| 4 | `OPRA.PILLAR` / `cbbo-1s` | one selected SPXW contract, `[13:30, 13:35)` | 5 | 5 | 400 | 0.000000745058 | 5 |
| 5 | `OPRA.PILLAR` / `trades` | same contract, `[13:30, 20:00)` | 5 | 5 | 240 | 0.000062584877 | 5 |
| 6 | `OPRA.PILLAR` / `cmbp-1` | same contract, `[13:30, 13:31)` | 5 | 5 | 400 | 0.000000059605 | 5 |
| **Total** |  |  |  | **46** | **10,376** | **0.000109992922** | **46** |

The initial five-minute OPRA trade probe estimated zero records and zero cost. The interval was
widened only after a new count/size/cost estimate, then remained capped at five records. Repeated
estimator calls are free; only the six rows above were market-data downloads.

The twelve-decimal total is an arithmetic sum of official **estimates**, not a claim that an equal
debit posted. In particular, the sub-ten-minute quote/bar ranges can be overestimates under
Databento's estimator rules; the full-day definition range used the documented accurate boundary.

**UNRESOLVED:** the exact posted promotional-credit debit was not available through the client
surface used here. The values above are official pre-download estimates, not a claim about the
eventual portal ledger. Databento documents that streaming is charged for bytes actually sent and
that the portal's Data usage page exposes posted cost and size by dataset, key, and mode. See the
[data-usage guide](https://databento.com/docs/portal/data-usage).

## Observed Investment Platform evidence

### Daily `EQUS.SUMMARY` microtest

**ACCOUNT ENTITLED / OBSERVED:** the bounded AAPL request returned one `ohlcv-1d` record. The
decoded structure contained `publisher_id`, `instrument_id`, `open`, `high`, `low`, `close`,
`volume`, and resolved `symbol`. Its event index was `2025-07-02 00:00:00+00:00`.

**INTERPRETATION:** the midnight UTC event key is an aggregation-date/interval-start key, not the
publication time or data-availability time, and it must not be described as a regular-session-only
boundary. A future adapter would need to preserve separate retrieval/ingestion provenance and
must not invent `available_at`; it would also need to declare that the bar is the final NLS+
summary including post-market activity.

**UNRESOLVED:** the experiment did not compare this bar against a consolidated reference across a
larger sample, validate correction behavior, or test adjustment semantics. Official Databento
examples apply Reference adjustment factors separately to original OHLCV, so these bars must not
be assumed split/dividend adjusted.

### Intraday `XNAS.BASIC` microtest

**ACCOUNT ENTITLED / OBSERVED:** five AAPL one-minute bars were returned with event indices from
13:30 through 13:34 UTC and the same structural OHLCV/provenance fields as the daily record.

**INTERPRETATION:** parsing is straightforward, but canonical five-minute support would require
deterministic resampling. Any future comparison against Alpaca SIP must classify systematic
volume, quote, and possible price differences as a likely **venue/feed coverage difference** before
considering provider error.

**UNRESOLVED:** this microtest did not evaluate DST, holidays, early closes, missing-minute rules,
corrections, pagination, or a multi-symbol request. Phase 1 already covers those general provider
boundary concerns with other providers; this research does not silently extend that bake-off.

## Reference and corporate-action assessment

### Documented capability

**DOCUMENTED:** Databento's Reference API describes listing-level point-in-time corporate actions,
adjustment factors, and security-master histories. Corporate-action events include dividends,
forward/reverse splits, mergers, issuer-name changes, and local-code changes, with event-specific
effective, ex, record, payment, announcement/declaration, due-bill, completion, and other dates.
With `pit=True`, all historical records for an `event_unique_id` are retained rather than only the
latest state. See the [Reference API](https://databento.com/docs/api-reference-reference) and
[corporate-actions specification](https://databento.com/docs/venues-and-datasets/corporate-actions).

The [security-master specification](https://databento.com/docs/venues-and-datasets/security-master)
documents `security_id` and `listing_id` continuity across specified name changes, mergers, and
demergers, plus external identifiers such as ISIN, CUSIP/`us_code`, and FIGI. It also documents
cases where symbol normalization is insufficient and stable external identifiers are the reliable
join key. The [adjustment-factor specification](https://databento.com/docs/venues-and-datasets/adjustment-factors)
describes a separate adjustment layer rather than silently mutating historical bars.

### Account result

**OBSERVED:** documentation enumeration succeeded, returning 60 event definitions and 235 enum
groups. The current public product page advertises 61 supported event types; whether the one-item
difference is a version, rollout, or counting distinction is **UNRESOLVED**. Minimal PIT data calls
for the known Phase 1 cases `ORLY`, `IBKR`, and `NFLX` were attempted over a bounded June-December
2025 effective-date window:

| Reference endpoint | Result | Classification |
| --- | --- | --- |
| `corporate_actions.get_range` | HTTP 403 | documented capability; account not entitled; no records obtained |
| `adjustment_factors.get_range` | HTTP 403 | documented capability; account not entitled; no records obtained |
| `security_master.get_range` | HTTP 403 | documented capability; account not entitled; no records obtained |

No retry, upgrade, subscription, or workaround was attempted. This means split ratios, currency,
effective/ex/record/payment dates, ticker/name changes, mergers, PIT revisions, adjustment factors,
and stable IDs are **not empirically validated for this account**, even though their field and
event models are documented. The public corporate-actions page states that personal and commercial
users of that product may use it internally for display and non-display purposes, while external
file/API redistribution is prohibited; product entitlement and its contract remain separate from
historical market-data credits. See [Databento corporate actions](https://databento.com/corporate-actions).

## Observed options evidence

### Definition and symbology

**ACCOUNT ENTITLED / OBSERVED:** a parent request for `SPXW.OPT` returned 25 bounded definitions.
One contract was selected for the quote/trade probes:

```text
raw symbol:  SPXW  250930P06275000
expiration:  2025-09-30
type:        put
strike:      6275
parent:      SPXW
currency:    USD
```

The request did not download the full chain. Separately, free metadata returned a positive count
for `SPX.OPT`, so SPX parent availability is **ACCOUNT VISIBLE** but SPX child fields and market
records were not empirically inspected.

### Consolidated quote observations

**ACCOUNT ENTITLED / OBSERVED:** five `cbbo-1s` records were returned. All had Databento publisher
30, the documented OPRA consolidated publisher. Separate `bid_px_00`, `ask_px_00`, `bid_sz_00`,
and `ask_sz_00` values were populated, as were `bid_pb_00` and `ask_pb_00`, which identify the
venues contributing the best bid and offer.

The aggregate records were indexed by `ts_recv` at approximately 13:30:03-13:30:07 UTC. A
`ts_event` column existed but was undefined (`NaT`) in these five `cbbo-1s` rows. This is consistent
with Databento's aggregate schema semantics: the interval receive timestamp exists while an event
timestamp associated with a last trade can be undefined. A future implementation must preserve
that distinction rather than synthesize event time.

**ACCOUNT ENTITLED / OBSERVED:** five `cmbp-1` records from the same contract carried publisher 30,
both `ts_event` and `ts_recv`, consolidated bid/ask prices and sizes, and bid/ask contributing
publisher identifiers. This schema is a better fit when exact quote-update time matters, but broad
CMBP requests can be very large; Databento explicitly recommends batch delivery for large
historical requests.

### Trade observations

**ACCOUNT ENTITLED / OBSERVED:** five trades were obtained only after widening the estimated
window, still under `limit=5`. They occurred around 19:29:40.299-19:29:40.358 UTC and carried
publisher 22, which official publisher metadata maps to Cboe Options (`XCBO`). Fields included
`price`, `size`, `action`, `side`, `flags`, `sequence`, and `ts_event`. OPRA does not disseminate
trade aggressor side, so the documented normalized side is `N`.

Publisher 22 and publisher 30 are complementary provenance, not a discrepancy: 22 identifies the
trade execution venue; 30 identifies the consolidated OPRA BBO.

### Options-project interpretation

Databento/OPRA is a **strong candidate for a future bounded options research prototype** because
the tested boundary can provide:

- parent-based SPX/SPXW definition discovery;
- expiry, strike, call/put, parent, currency, and raw OCC/OSI identity;
- consolidated best bid and ask with independent sizes and contributing venues;
- exact quote-update and receipt timestamps through `cmbp-1`;
- venue-attributed trades and sequence/flag fields;
- a documented consolidated options feed rather than a single-venue proxy.

This evaluation did **not** establish a production-ready chain loader, 0DTE completeness, quote
staleness policy, crossed/locked-market handling, contract adjustment handling, trading calendar,
surface construction, Greeks, or replay architecture. It selected a non-0DTE contract and must not
be cited as an empirical 0DTE validation. Parent expansion and OPRA message volume can make costs
grow rapidly; count/size/cost estimates and hard limits must remain mandatory.

## Licensing and retention assessment

This is an engineering classification, not legal advice. Technical access, feed entitlement,
observed data semantics, and retention/reuse rights are deliberately kept separate.

| Topic | Official evidence | Classification for this evaluation |
| --- | --- | --- |
| Historical API access | usage-based historical data; account successfully downloaded the six bounded requests | **ACCOUNT ENTITLED** for tested requests |
| Historical exchange license | Databento states that users generally do not need a market-data license for data at least 24 hours old, with exceptions including redistribution, certain OTC data, and ICE PCAP | **DOCUMENTED** access rule; not a retention grant |
| Personal/internal research | portal definitions describe personal use as an individual's own research/education and internal use as a private environment | **DOCUMENTED terminology**; exact dataset agreement still governs |
| Non-display processing | portal definitions include internal analytics in non-display use; venue rules vary | **DOCUMENTED terminology / UNRESOLVED exact grant** for these historical datasets |
| Temporary private processing | the public User Agreement grants access and use for internal purposes while the account is in good standing | **APPLIED EPHEMERALLY** as a bounded interpretation of that positive grant; not a durable-storage conclusion |
| Durable raw retention | no reviewed public dataset term established how long privately downloaded raw `EQUS.SUMMARY`, `XNAS.BASIC`, or `OPRA.PILLAR` records may be retained | **UNRESOLVED — provider/License Manager confirmation required** |
| Reversible normalized storage | no reviewed term expressly classified a losslessly reversible normalization | **UNRESOLVED** |
| Internal derived analytics | internal analytics is described as non-display use, but exact historical-dataset derived-data and reversibility boundaries were not established | **UNRESOLVED for durable implementation** |
| Corporate actions/security master | product page permits internal display/non-display use and prohibits external file/API redistribution; account returned 403 | **DOCUMENTED product rule / NOT ENTITLED** |
| OPRA-specific obligations | the OPRA fee schedule distinguishes historical data and defines non-display categories/fees; reviewed public material did not establish the exact declaration or fee consequence for this account's personal use of old historical records | **UNRESOLVED — account License Manager or provider confirmation required** |
| Public display | venue- and product-specific licensing applies; not part of this private evaluation | **NOT AUTHORIZED OR TESTED** |
| Redistribution | Databento says rights depend on the source dataset and License Manager; corporate-actions external file/API redistribution is expressly prohibited | **RESTRICTED / DATASET-SPECIFIC; not authorized** |
| Use after account/agreement termination | User Agreement section 9.3 says the customer's rights terminate and the customer may no longer download, access, or use third-party data | **CLEARLY RESTRICTED** under the public base agreement |
| Deletion/destruction after termination | the public base agreement reviewed does not state a general destruction procedure; publisher agreements may add one | **UNRESOLVED** |
| Databento batch re-download | Databento documents a 30-day no-additional-charge re-download window for a billed batch request | **DOCUMENTED delivery behavior**, not a local-retention rule |

Sources: [licensing introduction](https://databento.com/blog/introduction-market-data-licensing),
[portal licensing definitions](https://databento.com/docs/portal),
[pricing and redistribution FAQ](https://databento.com/pricing/), and
[OPRA fee schedule](https://api0.databento.com/v0/licensing/documents/opra/OPRA_Fee_Schedule.pdf).
The official [Databento User Agreement](https://databento.com/legal/04-00-00-user-agreement.html)
(document effective date: 2024-01-31; reviewed 2026-08-26) grants non-transferable internal access
while the account is in good standing, states that ownership of third-party data does not transfer
to the customer, subjects all use to publisher-specific terms, restricts redistribution including
furnishing derived information to third parties, and ends download, access, and use rights on
termination.

The absence of a separate exchange license for historical access does **not** prove that durable
raw storage, reversible normalized storage, or derived-data reuse is permitted. Separately, section
9.3 clearly bars further use after termination; whether already-held copies must be destroyed is
unresolved in the public base agreement. Before any persistent ingestion, the project needs
dataset-specific License Manager terms or written Databento/publisher confirmation covering:

1. private raw retention duration while the account remains active;
2. persistent normalized and reversibly transformed records;
3. internal derived analytics and model inputs;
4. backups and disaster-recovery copies;
5. deletion obligations and any separately negotiated surviving rights after termination;
6. display, sharing, and redistribution boundaries.

Therefore Databento is **not yet a verified durable-retention solution** for the Investment
Platform. It may become one after dataset-specific confirmation; technical download capability
alone is insufficient.

## Recommendations

### Investment Platform

| Potential role | Recommendation | Basis |
| --- | --- | --- |
| Primary US daily provider | **Do not select yet** | `EQUS.SUMMARY` is technically promising and consolidated, but has final 20:15 ET/post-market semantics, observed history only from July 2024, no broad empirical comparison, and unresolved durable retention |
| Specialized EOD supplement | **Worth a future terms/retention-cleared bake-off** | clear NLS+ summary provenance, one-record-per-date structure, consolidated volume documentation |
| Primary US intraday provider | **Do not select `XNAS.BASIC` as SIP-equivalent** | Nasdaq-centered quote/trade coverage, no native 5m, only a five-minute AAPL microtest, `EQUS.SIP` not account-visible |
| Nasdaq-focused intraday research | **Technically plausible** | successful one-minute bars and well-documented feed provenance |
| Corporate actions / adjustments | **Promising product, unavailable to this account** | rich documented PIT model; all bounded data endpoints returned 403 |
| Security master | **Promising product, unavailable to this account** | documented stable IDs and listing continuity; data access returned 403 |
| Durable retention provider | **Unresolved** | no confirmed raw/reversible-normalized/post-termination rights |

Databento should remain a **specialized alternate candidate**, not a silent replacement for the
Phase 1 recommendation. A future decision should compare `EQUS.SUMMARY` against the chosen daily
semantics, identify a genuinely consolidated intraday entitlement if required, price Reference
access, and obtain written retention terms before implementation.

### Options project

Proceed only with a separately approved, cost-bounded prototype. `OPRA.PILLAR` is technically well
matched to SPX/SPXW chain discovery, consolidated bid/ask research, and trade/quote provenance.
Recommended gates before implementation are:

1. confirm OPRA historical retention and derived-analytics rights in the account's License Manager
   or through written Databento/publisher confirmation;
2. estimate a realistic single-session SPXW definition + quote + trade workload;
3. choose `cmbp-1`, `tcbbo`, or sampled `cbbo` based on timestamp and replay requirements;
4. define contract identity across daily `instrument_id` remapping and corporate-action adjustments;
5. define chain completeness, 0DTE selection, session calendar, and quote-staleness rules;
6. keep hard record/byte/cost limits before every parent-expanded request.

## Cleanup and repository verification

**OBSERVED:** every time-series result remained in process memory. No output path was supplied, no
batch job was created, and no real response was converted into a fixture. After the SDK processes
ended:

- no `.dbn`, market-data CSV/JSON, real Parquet, or DuckDB file remained;
- no `data/raw`, `data/normalized`, `data/curated`, or `data/features` record was tracked;
- no API key or authorization header was written to source, docs, logs, manifests, or commands;
- no project dependency or lockfile change was made;
- the only intended repository change is this research report.

Cleanup is **PASS**. Because raw data was deliberately not retained, exact replay and independent
re-normalization of these microtests are no longer possible; the report preserves only sanitized
request boundaries, structural observations, and aggregate counts/costs.

## Unresolved questions

1. What dataset-specific License Manager or contractual language governs durable private raw,
   reversible-normalized, backup, and derived storage for each evaluated dataset while the account
   is active, and is any surviving right available by separate agreement despite the public base
   agreement's post-termination restriction?
2. Would a current account/plan expose a truly consolidated US equity intraday dataset such as
   `EQUS.SIP`, and at what historical depth and cost?
3. Is the short observed July-2024 history for `EQUS.SUMMARY` and `XNAS.BASIC` sufficient for any
   planned feature horizon?
4. How do `EQUS.SUMMARY` corrections and its 20:15 ET summary compare empirically with the Phase 1
   providers across splits, dividends, early closes, and post-market-heavy sessions?
5. What paid Reference plan and symbol allocation would cover the intended universe, and would it
   empirically resolve the ORLY, IBKR, NFLX, ticker-change, currency, and PIT cases?
6. Which OPRA schema provides the best cost/fidelity balance for SPXW 0DTE work, and how large is a
   realistic whole-session, bounded-strike chain?
7. Why do some current official OPRA materials expose different schema-history start boundaries?
   Production planning should rely on authenticated metadata until clarified.
8. How quickly does the portal post exact promotional-credit usage, and can that ledger be exported
   for automated research cost governance?

## Official sources

- [Historical API](https://databento.com/docs/api-reference-historical)
- [Reference API](https://databento.com/docs/api-reference-reference)
- [Pricing](https://databento.com/pricing/)
- [Usage, pricing, and credits FAQ](https://databento.com/docs/faqs/usage-pricing-and-data-credits)
- [Portal and licensing terminology](https://databento.com/docs/portal)
- [Data usage](https://databento.com/docs/portal/data-usage)
- [Introduction to market-data licensing](https://databento.com/blog/introduction-market-data-licensing)
- [Databento User Agreement](https://databento.com/legal/04-00-00-user-agreement.html)
- [EQUS.SUMMARY specification](https://databento.com/docs/venues-and-datasets/equs-summary)
- [XNAS.BASIC specification](https://databento.com/docs/venues-and-datasets/xnas-basic)
- [XNAS.BASIC dataset page](https://databento.com/datasets/XNAS.BASIC)
- [OPRA.PILLAR specification](https://databento.com/docs/venues-and-datasets/opra-pillar)
- [OPRA history/schema announcement](https://databento.com/blog/opra-improvements-coming-soon)
- [Symbology](https://databento.com/docs/standards-and-conventions/symbology)
- [Common fields, timestamps, and identifiers](https://databento.com/docs/standards-and-conventions/common-fields-enums-types)
- [Corporate actions](https://databento.com/docs/venues-and-datasets/corporate-actions)
- [Adjustment factors](https://databento.com/docs/venues-and-datasets/adjustment-factors)
- [Security master](https://databento.com/docs/venues-and-datasets/security-master)
- [Corporate-actions product and usage rules](https://databento.com/corporate-actions)
- [Security-master product and usage rules](https://databento.com/security-master)
- [OPRA fee schedule](https://api0.databento.com/v0/licensing/documents/opra/OPRA_Fee_Schedule.pdf)
