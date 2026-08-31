# Phase 2 acceptance record

- **Offline implementation:** IMPLEMENTED
- **Controlled live acceptance:** PENDING
- **Phase 2 completion / approval:** PENDING
- **Phase 3:** NOT STARTED
- **Last review:** 2026-09-01

This is a sanitized public status record. It contains no provider payloads, OHLCV rows, credentials,
private-root path, support correspondence, or reproducible licensed dataset.

## Offline checkpoint scope

The current implementation and synthetic test suite cover the offline control plane required
before live persistence:

| Acceptance area | Offline status | Evidence boundary |
| --- | --- | --- |
| Runtime isolation | Implemented | All five profiles, capability denial, credential/root isolation |
| External root | Implemented | Unsafe-path rejection, sentinel identity, managed paths, revalidation |
| Retention | Implemented | Exact lookup, fail-closed unknowns, age gate, layer/query/quarantine/watermark/export/purge gates |
| Operational state | Implemented | Forward-only SQLite migrations, WAL/FULL contract, constraints, restart, lease |
| Calendar | Implemented | XNYS ordinary/holiday/early-close/DST cases, 1d/5m RTH slots, versioned checksum |
| Planning | Implemented | Deterministic half-open requests, coverage subtraction, partitioning, budgets, no-op |
| Synthetic backfill | Implemented | Raw-first persistence, canonical Parquet, catalog, coverage, initial watermark |
| Synthetic incremental update | Implemented | Frontier-derived extension and provider-call-free second no-op |
| Synthetic repair | Implemented | Gap detection, missing-only repair, raw replay, retained provenance |
| Publication/recovery | Implemented | Manifest-last staging, atomic rename, orphan adoption, transaction rollback/restart |
| Coverage/watermark | Implemented | Calendar-contiguous retained VERIFIED proof; no `MAX(timestamp)` shortcut |
| `VERIFIED_EMPTY` | Implemented | Requires complete request, verified complete pagination, bounds, and demonstrated provider semantics |
| Status/verify CLI | Implemented | Sanitized control-plane output and read-only integrity findings |
| Fault injection | Implemented | Raw, staging, manifest, rename, reopen, SQLite/watermark, completion, pacing boundaries |

Repository tests remain offline and synthetic. The normal suite blocks network access and requires
neither API credentials nor a private data root. The definitive revision-level quality result is
the complete command gate in `AGENTS.md`; test counts are not copied here because they change as
coverage grows.

## Retention decisions exercised offline

- `alpaca / price_bars_sip` is the only active Alpaca
  `DURABLE_AUTHORIZED` entry and is restricted to private historical SIP US stock bars strictly
  older than 15 minutes plus the configured finalization buffer.
- Alpaca options and crypto remain `UNVERIFIED_PENDING` without active policies.
- Alpaca real-time, news, and unlisted datasets have no active entry and fail closed.
- Pending or suspended Twelve Data/Databento entries cannot silently become active durable streams.
- Massive Individual entries are `PROHIBITED` for the intended workflow.
- synthetic/sample entries exercise `SYNTHETIC_UNRESTRICTED` without creating licensing claims for
  provider data.

`VERIFIED_EMPTY` is not inferred from an empty response or absent bar. The offline contracts require
a successfully completed bounded request, complete verified pagination, correct interval, no
incomplete rate-limit/error state, and sufficiently demonstrated provider omission semantics.

## Live acceptance still pending

No real Alpaca payload was persisted for this offline record. The following required results are
therefore deliberately not claimed:

- AAPL 1d and 5m durable backfill under the validated private root;
- post-process restart with unchanged SQLite coverage/watermark and readable Parquet;
- live incremental extension followed by a provider-call-free identical update;
- live bounded gap repair with provenance preservation;
- progression to the frozen Phase 1 sample of 16 securities;
- activation of an external scheduler;
- final Phase 2 pull request and CI review.

The live gate requires an explicit `private_research` process, a safe initialized external root,
the actual ticket evidence in its private locator, and Alpaca credentials present without printing
their values. Until that gate and the controlled sequence pass, Phase 2 is incomplete and no Phase
3 work is authorized.

## Offline-to-live handoff

Use the [living-ingestion operator guide](living-ingestion.md) to initialize the root and place the
real private evidence. Then execute the approved AAPL progression with small bounded request
budgets, running `status` and `verify` at each step. Record only aggregate, non-substitutive results
in this public document; licensed artifacts and complete evidence remain private.
