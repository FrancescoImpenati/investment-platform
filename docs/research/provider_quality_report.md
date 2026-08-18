# Provider quality report — bake-off template

> **Status: NOT EXECUTED — TEMPLATE ONLY**
>
> No provider was queried and no result, score, or recommendation below should be interpreted as an
> observed finding. This study belongs to the next roadmap step, not Phase 0.

## Objective

Compare Massive with one second provider selected at study time. Use yfinance only as a low-cost
sanity check, never as the canonical source. The study should cover approximately 10–20 securities;
it must not download the complete S&P 500.

## Reproducibility record

| Item | Value |
| --- | --- |
| Study date | Not run |
| Code revision | Not run |
| Instrument set and internal UUIDs | Not selected |
| Date ranges | Not selected |
| Massive product/tier | Not recorded |
| Second provider and product/tier | Not selected |
| yfinance version | Not recorded |
| Trading-calendar reference | Not selected |
| Raw batch IDs / manifests | None |

Record plan limits, endpoint names, requested adjustment/session options, request timestamps, and
provider documentation versions. Never record API keys, authorization headers, cookies, or
authenticated URLs.

## Proposed sample

Select liquid and less-liquid US equities plus at least one ETF, with examples that exercise:

- a split and a cash dividend;
- a ticker change when a practical historical case is available;
- normal sessions, a DST boundary, an exchange holiday, and an early close;
- daily and 5-minute bars;
- both recent and older intervals allowed by each provider tier.

The final sample and dates remain **TBD** until provider access and licensing terms are known.

## Evaluation dimensions

### Coverage and correctness

- daily and 5-minute interval coverage;
- missing and duplicate bars;
- OHLC, volume, and VWAP differences;
- timestamp label, timezone, DST, session, and early-close behavior;
- split, dividend, ticker-change, and adjusted/unadjusted semantics;
- provider instrument identifiers and symbol-history handling;
- delayed data and corrections discovered by repeat retrieval.

### Operational behavior

- authentication and endpoint ergonomics;
- pagination, rate limits, retry guidance, and observed latency;
- maximum lookback and intraday retention;
- bulk request support and expected S&P 500-scale cost;
- error clarity, service stability, and reproducibility.

### Legal and data governance

- license classification and redistribution limits;
- whether raw, normalized, or derived samples may be published;
- attribution and retention requirements;
- restrictions on display, derived data, or commercial/portfolio use.

## Results template

| Dimension | Massive | Second provider | yfinance sanity check | Evidence / raw batch IDs |
| --- | --- | --- | --- | --- |
| Daily coverage | Not run | Not run | Not run | None |
| 5-minute coverage | Not run | Not run | Not run | None |
| Timestamp/session semantics | Not run | Not run | Not run | None |
| Missing/duplicate bars | Not run | Not run | Not run | None |
| Corporate actions | Not run | Not run | Not run | None |
| Adjustment behavior | Not run | Not run | Not run | None |
| Rate limits and latency | Not run | Not run | Not run | None |
| Cost/scalability | Not run | Not run | Not run | None |
| Licensing/redistribution | Not run | Not run | Not run | None |

For numerical comparisons, report the requested interval, expected trading sessions, row counts,
missing-key counts, duplicate-key counts, and clearly defined tolerances. Separate verified facts
from inferences.

## Decision record

- **Recommended primary provider:** Not decided
- **Recommended fallback/cross-check provider:** Not decided
- **Canonical adjustment policy:** Not decided
- **Known limitations:** Not evaluated
- **Required architecture changes:** None identified because the study has not run

Do not select a primary provider until the evidence table, licensing review, and cost/scalability
assessment are complete.
