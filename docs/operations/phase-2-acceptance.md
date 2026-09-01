# Phase 2 acceptance record

- **Offline implementation:** IMPLEMENTED
- **Controlled AAPL live acceptance (M6):** COMPLETE
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

## Controlled AAPL live acceptance (M6)

The controlled live run used only Alpaca historical SIP US stock bars for AAPL, RTH, unadjusted,
under `private_research`. All requested sessions were closed, beyond the strict historical-age gate
and finalization buffer. Provider payloads and canonical observations remain exclusively under the
validated external private root.

| Check | Sanitized result |
| --- | --- |
| Initial backfill | 1d session on 2026-08-24; complete 5m session on 2026-08-24 |
| Incremental extension | 1d coverage extended to 2026-08-25 without reacquiring prior coverage |
| Identical update | Meaningful no-op; zero provider calls and zero new raw/Parquet artifacts |
| Controlled discontinuity | Disjoint 1d targets exposed the missing 2026-08-26 session as one blocking gap |
| Repair | One bounded `MISSING_ONLY` request for 2026-08-26; gap resolved with provenance retained |
| Final 1d state | Five sessions, coverage `[2026-08-24T13:30Z, 2026-08-28T20:00Z)`, VERIFIED watermark at the exclusive end |
| Final 5m state | One complete session, 78 current rows, coverage and VERIFIED watermark ending 2026-08-24T20:00Z |
| Durable artifacts | Six raw artifacts, six canonical batches and six Parquet parts |
| Current analytical view | 83 rows total: five 1d and 78 5m |
| Provider budget used | Six provider dispatches, below the approved ceiling of 20 |
| Restart | New process validated the sentinel, reopened SQLite and read all current Parquet rows through DuckDB without a provider call |
| Final status/verify | SQLite schema 11 healthy; zero open gaps, zero quarantine findings, zero unexpected orphans; every verification check passed |

The live sequence found and fixed two real boundary defects. Disjoint verified batches now
materialize a calendar-eligible internal gap without moving the watermark across it, including
restart-safe handling of exchange-closed `NOT_APPLICABLE` intervals. Verification also accepts the
approved last-changing-batch provenance when a repair batch fills an internal gap and unlocks
already-persisted later coverage.

The private Alpaca evidence locator exists, but its archive status remains
`pending_manual_archive`; no email or support evidence was fabricated. This did not block the
explicitly approved small M6 acceptance.

## Remaining Phase 2 work

M6 does not complete Phase 2. The controlled Phase 1 sample rollout, final hardening/quality gate,
documentation review, pull request and CI review remain pending in M7. No 16-security rollout,
pull request, merge, scheduler activation or Phase 3 work was performed here.
