# Phase 2 acceptance record

- **Offline implementation:** IMPLEMENTED
- **Controlled AAPL live acceptance (M6):** COMPLETE / APPROVED
- **Capped multi-instrument live exercise (M7):** COMPLETE WITH CONTAINED PARTIAL FAILURE
- **Final local quality:** COMPLETE
- **Final security/data/diff audit:** COMPLETE
- **Pull request / Linux CI:** PENDING
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

## Capped multi-instrument live exercise (M7)

M7 deliberately did not reuse the complete historical Phase 1 sample of 16. Its total instrument
scope was capped at AAPL, MSFT, ORLY, and NEE. Daily requests covered only a few completed sessions;
five-minute requests were limited to at most one completed RTH session for MSFT and ORLY. Every
request used historical SIP data beyond the strict age gate and finalization buffer.

| Check | Sanitized result |
| --- | --- |
| Planned instrument scope | Four total instruments including AAPL; no 16-security or broader rollout |
| Bounded intervals | Daily `[2026-08-24T13:30Z, 2026-08-26T20:00Z)`; MSFT/ORLY 5m `[2026-08-24T13:30Z, 2026-08-24T20:00Z)` |
| New successful streams | MSFT 1d: 3 aggregate observations; MSFT 5m: 78; NEE 1d: 3; ORLY 5m: 78 |
| ORLY 1d | Incomplete after the failed three-symbol run plus three isolated bounded runs (nine related dispatches); no raw artifact, canonical batch, Parquet part, coverage, or watermark |
| Empty-data semantics | ORLY 1d was never promoted to `VERIFIED_EMPTY`; four gap records remain `OPEN` |
| M7 provider budget | 16 new dispatches, below the hard stop of 20 |
| Final operational inventory | Seven streams, four distinct instrument UUIDs, 10 raw artifacts, 10 canonical batches, 10 Parquet parts, and 245 aggregate canonical observations |
| Independent progress | Six stream watermarks; the failed ORLY 1d stream has none and did not alter successful streams |
| Gap state | Four open records, all attributable to the unverified ORLY daily interval |
| Isolation | Distinct stream keys and UUID mappings; no cross-instrument canonical, coverage, watermark, or artifact contamination found |
| Final status | SQLite schema 11 healthy; all seven streams shown with their own dimensions, coverage, watermark, and gap state |
| Final verify | PASS for every integrity check; zero staging entries, quarantine findings, and unexpected orphans |

The first multi-symbol attempt failed at transport before any stream acquired durable truth.
Subsequent independently bounded runs succeeded for MSFT, NEE, and ORLY 5m. This demonstrated that
a transport failure does not falsely advance another stream and that later independent success does
not repair or hide the failed ORLY daily stream. M7 did not inject a destructive fault into the
live database; destructive partial-publication and transaction failures remain covered by the
existing synthetic tests.

### Real M7 transport defect

The rollout exposed one real compatibility defect on the tested Windows route: default TLS
negotiation repeatedly timed out, while capping the client at TLS 1.2 restored successful requests.
The bounded transport was corrected and one focused regression test was added. MSFT, NEE, and the
requested five-minute streams succeeded after that correction, but intermittent transport failure
remained observable on ORLY daily and is an open host-route issue. No additional architecture or
provider behavior was introduced.

## Private evidence status

The validated private evidence locator still contains no manually archived correspondence and no
checksum manifest. Its public redacted state remains `pending_manual_archive`; no email, personal
address, attachment, checksum, or support content was fabricated or copied into the repository.
This is the same explicitly approved scoped exception used for M6 and M7, not authorization for a
larger rollout or scheduler.

## Local symlink-test limitation and Linux CI gate

The Windows host cannot create ordinary test symlinks without the required privilege and reports
WinError 1314. Exactly these five security tests are therefore skipped locally rather than counted
as passes:

1. `test_sentinel_symlink_to_external_file_is_rejected_when_supported`;
2. `test_managed_path_rejects_symlink_escape_when_supported`;
3. `test_raw_verification_rejects_a_symlinked_payload`;
4. `test_recovery_rejects_symlinked_files_and_intermediate_directories`;
5. `test_database_cannot_be_redirected_outside_the_validated_root`.

Their security intent has not been weakened and the tests were not changed to bypass the host
restriction. Execution through GitHub Actions on Linux, including an exact explanation of any
Linux skips, is still pending and must be recorded only after the push and pull-request workflows
actually complete.

## Final local quality gate

The complete local gate is green: lock and locked synchronization, Ruff formatting and lint,
mypy, pytest, package build, and `git diff --check` all passed. Pytest collected 723 items and
reported 718 passed plus the five Windows symlink skips above in 353.98 seconds. The gate first
identified formatting and import/export ordering left by earlier checkpoints; Ruff corrected only
those mechanical issues. The first isolated package-build attempt then hit transient PyPI DNS
resolution, and the unchanged `uv build` command passed once name resolution recovered.

The one final security/data/diff audit found no credential values, private-root literal, private
evidence, operational database, licensed payload, or non-sample market-data artifact in tracked or
untracked repository files. The root remains absolute, sentinel-valid, and physically separate
from Git. SQLite integrity, raw/canonical checksums, retention consistency, watermarks, and catalog
files passed; staging, quarantine, and unexpected-orphan counts were zero. The diff against `main`
contains no CI change, Phase 3 subsystem, or forbidden infrastructure.

## Remaining Phase 2 work and Phase 3 readiness

The Phase 2 implementation now provides the private-root boundary, exact retention enforcement,
raw-first immutable persistence, verified Parquet publication, SQLite control plane, independent
coverage/watermarks, backfill/update/no-op/repair/restart, and sanitized status/verification needed
for a later Phase 3 decision. Coherent commits, push, pull request, and both CI trigger results
remain pending at this record revision.

The private evidence archive and intermittent transport on the tested host route remain open
issues. No scheduler is activated, no merge has occurred, and Phase 3 has not started.
