# Phase 2 Implementation Plan — Living Data Ingestion

- **Status:** Implementation contract complete; implementation not started
- **Date:** 2026-08-31
- **Design:** [Phase 2 — Living Data Ingestion](docs/architecture/phase-2-living-ingestion.md)
- **Baseline:** Phase 0–1 approved; Databento research present in main at 76e3ffd

## 1. Outcome

Implement a local, restartable historical market-data store that demonstrates:

~~~text
safe private runtime
-> retention-authorized bounded request
-> immutable raw evidence
-> verified atomic canonical batch
-> durable coverage and watermark
-> backfill / update / repair
~~~

The first live implementation is Alpaca historical SIP US stock bars for 1d and 5m. All normal
tests and CI remain synthetic, offline, credential-free, and isolated from private data.

This plan is the Phase 2 implementation contract. PLAN.md remains the historical Phase 0 contract.

## 2. Frozen design decisions

- Keep the Python-first local-first modular monolith.
- Keep Parquet authoritative for analytical observations.
- Keep DuckDB in-process and query only cataloged VERIFIED Parquet files.
- Use SQLite through the standard library for mutable operational state.
- Use a dedicated absolute private data root outside Git with a direct-child sentinel.
- Permit durable real-data persistence only in the private_research profile.
- Keep provider credentials in the existing process environment variables.
- Do not require or load dotenv files.
- Add a separate machine-readable provider-by-dataset retention policy; do not repurpose
  LicenseClassification.
- Deny an unknown provider/dataset by default.
- Select Alpaca historical price_bars_sip as the initial DURABLE_AUTHORIZED stream.
- Enforce its strict >15-minute minimum observation age, plus finalization buffer, in every request
  mode.
- Use batch-directory staging and atomic same-filesystem rename for canonical publication.
- Make SQLite catalog visibility, coverage, gaps, and watermark one post-publication transaction.
- Promise at-least-once attempts with idempotent effects, not exactly-once execution.
- Derive watermark from retained VERIFIED contiguous coverage, never MAX(timestamp).
- Keep backfill, incremental update, and repair as planner intents over bounded requests.
- Use one ingestion writer/lease and conservative sequential provider execution initially.
- Introduce no internal scheduler; make a reliable manual command callable by external schedulers.
- Adopt a maintained XNYS calendar behind a provider-neutral boundary only after its dependency
  gate passes.
- Define no automatic winner between different providers.

The accepted ADRs are:

- [ADR 0006](docs/architecture/adr/0006-external-private-data-root.md);
- [ADR 0007](docs/architecture/adr/0007-retention-aware-dataset-policy.md);
- [ADR 0008](docs/architecture/adr/0008-sqlite-operational-state-store.md);
- [ADR 0009](docs/architecture/adr/0009-watermark-publication-and-recovery-protocol.md).

## 3. Delivery rules

Every milestone must:

- implement only purposeful modules with real behavior;
- include meaningful failure and restart tests;
- preserve existing public contracts deliberately;
- avoid network access in normal tests;
- use synthetic provider pages and injected clocks/faults;
- update design, data docs, ADRs, and README when behavior differs;
- verify no real/private/licensed data or secret is tracked;
- keep migrations and policy schemas reviewable in the public repository;
- avoid empty packages for later phases.

Use focused tests while iterating. Run the complete required checks before every checkpoint handoff.

## 4. Milestone 1 — Runtime profiles and capability isolation

### Deliverables

- A typed environment enum for test, ci, development, private_research, and demo.
- One capability matrix controlling network, provider construction, credentials, durable storage,
  and private-root access.
- A minimal settings resolver for INVESTMENT_PLATFORM_ENV and
  INVESTMENT_PLATFORM_DATA_ROOT.
- Dependency injection for test roots and clocks.
- Explicit failures for unsupported profile/capability combinations.

### Required behavior

- private_research requires a configured data root.
- test, ci, and demo never resolve or open the private root.
- ci cannot opt into network or credentials.
- development defaults to synthetic/sample behavior.
- A development preflight is explicit, bounded, and non-durable.
- No credential value enters a model representation, log, error, path, or manifest.

### Tests

- Every profile capability row.
- Missing/unknown profile.
- Stale private-root environment variable under test/demo is never opened.
- CI rejects private-root/network configuration.
- Development cannot persist a real provider batch.
- private_research without a root fails before provider construction.

## 5. Milestone 2 — External private root and sentinel

### Deliverables

- A data-root initializer for a new or empty dedicated directory.
- A versioned root sentinel contract.
- Root and managed-path validation shared by all storage components.
- A safe relative-path resolver for cataloged artifacts.
- Lazy creation of raw, normalized, staging, operational, logs, quarantine, and governance paths.
- Destructive-operation guardrails.
- Secondary `.gitignore` hardening for accidental repository-root private-runtime namespaces and
  SQLite sidecars, without ignoring public `docs/governance/` records.

### Required behavior

- Input path is absolute and resolves to a local filesystem location.
- Reject drive/filesystem root, home/profile, repository, repository ancestor or descendant,
  system/temp roots, Windows UNC/network roots, and symlink/junction escapes.
- A nonempty directory without the exact sentinel is not adopted.
- Sentinel purpose, schema, root ID, and canonical path must match on every mutation.
- Staging and canonical targets must share the same filesystem.
- Never recursively delete the private root.
- Only exact cataloged relative targets may be removed.

### Tests

- Safe absolute external path succeeds in a temporary filesystem.
- Relative, root, home, repository-contained, repository-parent, and UNC-style paths fail.
- Missing, corrupt, wrong-purpose, wrong-root-ID, and moved sentinels fail.
- Symlink/reparse escape attempts fail on supported platforms.
- A deletion target outside the root fails even when reached through traversal or a link.
- Revalidation catches a sentinel replacement between planning and mutation.
- `git check-ignore` covers accidental raw/normalized/operational/staging/logs/quarantine runtime
  paths while the redacted governance documentation remains trackable.

## 6. Milestone 3 — Retention policy model and enforcement

### Deliverables

- A serializable DatasetRetentionPolicy contract.
- Retention rules for raw, normalized/reversible, and derived layers.
- Request-eligibility rules, including minimum observation age.
- A versioned committed policy catalog with exact provider/dataset keys.
- Policy validation, hashing, lookup, and immutable run snapshots.
- Enforcement gates callable before network, processing, persistence, query, watermark, and purge.
- Dataset status and policy-restriction handling.

### Initial entries

- Alpaca historical price_bars_sip: DURABLE_AUTHORIZED only after its strict >15-minute age gate.
- Alpaca historical options: UNVERIFIED / PENDING; evidence does not cover the dataset and no active
  policy entry exists.
- Alpaca crypto: UNVERIFIED / PENDING; evidence does not cover historical or real-time crypto and
  no active policy entry exists.
- Alpaca real-time, news, and other unlisted datasets: no active entry, therefore fail closed.
- Twelve Data price_bars_us_daily: SUBSCRIPTION_BOUND and inactive until status is recorded.
- Twelve Data price_bars_standard_us_intraday: SUBSCRIPTION_BOUND and inactive until status is
  recorded.
- Databento OPRA.PILLAR: EPHEMERAL.
- Massive Individual price_bars and corporate_actions: exact PROHIBITED entries; other datasets
  fail closed by absence.
- Reserved synthetic/sample price-bar datasets: SYNTHETIC_UNRESTRICTED.

### Required behavior

- Unknown provider/dataset denies by default.
- Runtime configuration may restrict but never expand the committed policy.
- A run records policy ID, revision, hash, and verification date.
- PROHIBITED fails before provider construction/network.
- EPHEMERAL cannot create durable artifacts, coverage, or historical watermark.
- TTL requires an expiry and makes expired data unavailable before physical deletion.
- SUBSCRIPTION_BOUND requires active exact-dataset status.
- DURABLE_AUTHORIZED does not permit public display or redistribution unless separately true.
- Backfill, update, and repair all apply dataset age/finalization ceilings before network access.
- Unexpected younger or out-of-bound Alpaca data is not persisted or quarantined.
- Quarantine inherits the originating rule.

### Tests

- All modes and every enforcement boundary.
- Unknown/misspelled dataset.
- Restrictive policy change during a run.
- TTL boundary exactly at expiry.
- Subscription becomes inactive during an incomplete run.
- Policy hash/revision persists across restart.
- Alpaca observations exactly at, before, and after the strict 15-minute boundary.
- Unexpected provider overfetch produces no raw, canonical, quarantine, coverage, or watermark.
- Existing LicenseClassification remains unchanged and independent.

## 7. Milestone 4 — SQLite operational state

### Deliverables

- A stdlib sqlite3 connection factory under the validated operational root.
- Versioned forward-only schema migrations.
- Repositories for runs, requests, attempts, raw artifacts and attempt-observation links, fixed batch
  contexts, canonical batches/files and stream outcomes, streams, coverage, gaps, watermarks,
  retries/errors, budgets, policy status, purge runs, and leases.
- Foreign keys, uniqueness constraints, indexes, and status transition validation.
- Writer lease acquisition, heartbeat, expiry, and safe takeover.
- Integrity/status diagnostics.

### Connection contract

- foreign_keys=ON.
- journal_mode=WAL and verify the returned value.
- synchronous=FULL.
- bounded busy timeout.
- short explicit transactions.
- BEGIN IMMEDIATE for write transitions.
- no provider I/O, sleep, checksum scan, Polars work, or Parquet write inside a transaction.

### Required behavior

- Database values contain only sanitized metadata and root-relative paths.
- Canonical bar values remain in Parquet.
- Unique identities turn repeated repository operations into no-ops or explicit collisions.
- Invalid state transitions fail.
- One writer may mutate ingestion state; read-only status can run concurrently.
- Restart recovers committed rows without reconstructing them from process memory.

### Tests

- Fresh database migration and idempotent reopen.
- Upgrade from every schema version introduced in Phase 2.
- Foreign-key and uniqueness failures.
- Interrupted transaction rolls back all related state.
- Competing writer lease and stale takeover with injected clock.
- WAL/synchronous configuration verification.
- Database path outside the validated root is refused.

## 8. Milestone 5 — Maintained trading calendar

### Dependency gate

Before adding a dependency:

1. resolve the current exchange-calendars release under Python 3.13 and uv;
2. inventory its transitive dependencies and licenses;
3. record the justified calendar-only Pandas/NumPy exception;
4. prove the frozen Phase 1 XNYS cases;
5. reject the dependency and return for design review if the gate fails.

### Deliverables

- A small provider-neutral TradingCalendar protocol.
- XNYS implementation behind that boundary.
- Immutable CalendarSnapshot and Session contracts.
- Operational persistence of snapshot version/checksum and sessions.
- A diff/verification path for calendar upgrades.
- Expected 1d sessions and 5m RTH slot generation.

### Required behavior

- Store UTC open/close and America/New_York session date separately.
- Version library, calendar ID, tzdata, covered range, and schedule checksum.
- Coverage references the snapshot used.
- Calendar changes mark affected verification stale; they do not rewrite data.
- An eligible slot is not automatically an expected trade.

### Tests

- Known Phase 1 holidays.
- 2025-07-03 early close with 42 five-minute slots.
- Ordinary session with 78 five-minute slots.
- Spring and autumn DST UTC shifts.
- Half-open boundary at close.
- Calendar snapshot checksum stability and upgrade diff.

## 9. Milestone 6 — Stable identities and bounded planner

### Deliverables

- Stable stream-key serialization and hash.
- Request specification, request instance, and attempt identities.
- Raw artifact and canonical batch identities.
- Canonical observation key and value-hash functions.
- Version identifiers for provider request mapping, canonical schema, normalizer, validator, and
  calendar snapshot.
- A planner returning bounded requests and budget estimates.

### Required behavior

- Stream key contains provider, exact dataset/feed, instrument UUID, timeframe, session,
  adjustment, and every series-defining dimension.
- Ticker/provider identifier is temporal provenance, not the stable stream identity.
- Request specification includes UUID/provider mappings sorted by a canonical key and a half-open
  interval; caller order is excluded.
- Credentials, page tokens, authenticated URLs, run IDs, retries, and retrieval times are excluded.
- Canonical batch identity changes when raw content or processing semantics change.
- Attempt-varying provider request IDs do not change raw artifact identity.
- Canonical media type/content encoding does change raw artifact identity.
- A persisted batch context fixes `ingested_at` and manifest creation time across replays.
- Same observation key/value/processing signature is a semantic no-op.
- Changed same-provider values form a revision.

### Planner behavior

- desired interval minus retained VERIFIED coverage;
- intersect every mode with policy request eligibility before partitioning;
- partition by sessions, point/page constraints, maximum bytes, calls, and optional cost;
- persist all bounded requests before dispatch;
- preflight hard ceilings;
- return no work when coverage is already complete;
- no provider-specific backfill/update/repair enum in the adapter.

### Tests

- Order-independent multi-instrument request fingerprint.
- Any stream-defining dimension changes stream identity.
- Retry metadata does not change request specification.
- Caller permutation of the same instrument mapping does not change request specification.
- Provider request-ID changes do not defeat raw artifact adoption.
- Same bytes under a different canonical representation type do not collide.
- Crash replay reuses byte-stable batch timestamps and content.
- Changed raw content changes canonical batch identity.
- Validator version and a semantically relevant calendar snapshot change canonical batch identity.
- Exact semantic replay is a no-op.
- Provider correction becomes a revision.
- Boundary partition union equals the requested half-open interval without overlaps.

## 10. Milestone 7 — Atomic canonical batches and recovery

### Deliverables

- File-backed/spooled provider payload support for durable ingestion where page size requires it.
- Attempt-scoped transient spool cleanup and crash recovery before immutable raw adoption.
- Raw artifact identity/adoption across distinct attempts.
- Immutable raw manifests with separate SQLite attempt-observation provenance links.
- Durable batch context created after raw completion and reused by replay.
- Batch-oriented canonical staging and manifest.
- Atomic same-filesystem directory publication.
- Post-publication verification.
- SQLite catalog-driven DuckDB file selection.
- Recovery coordinator and fault-injection hooks.

### Canonical manifest

Record:

- request specification hash plus batch, raw artifact, schema, normalizer, validator, and calendar
  identities;
- ordered relative Parquet file paths;
- file size and checksum;
- row/stream counts and temporal bounds;
- validation findings;
- semantic duplicate and revision counts;
- every requested stream's PUBLISHABLE/BLOCKED outcome;
- fixed batch-context creation time.

The operational catalog, not immutable content identity, links request instances, attempts, runs,
and the authorizing policy snapshot.

Never record credentials, authenticated URLs, page tokens, response bodies, or private values in
operational logs.

### Required behavior

- Persist raw before normalization.
- Inspect response bounds/age through bounded transient transport before raw persistence;
  unauthorized bytes are cleaned up and never enter raw, quarantine, or downstream processing.
- A provider page is never considered acquired before immutable raw publication.
- A request is raw-complete only after correct page termination.
- Write/verify the whole canonical batch under staging.
- Publish the entire batch with one atomic rename.
- Keep recoverable findings as flags; a fatal validation blocks the affected stream's full bounded
  interval rather than silently dropping rows.
- Publish independently PUBLISHABLE streams atomically, record all blocked outcomes, and commit
  coverage only for publishable streams.
- DuckDB receives only files from VERIFIED, policy-valid catalog entries.
- Adopt a published uncataloged batch after verification.
- Do not invent suffixed copies.
- Do not advance coverage/watermark before the final SQLite transaction.
- Commit the terminal SUCCESS/PARTIAL/FAILED request outcome in that transaction; reconcile only
  the aggregate run summary afterward.

### Fault-injection tests

Inject a process failure:

- after run creation;
- after request planning;
- during provider spooling;
- after raw publication and before catalog link;
- after each raw page;
- after batch-context persistence;
- during normalization;
- after validation;
- during each staged Parquet part;
- after staged manifest verification;
- after final rename;
- during SQLite catalog/coverage/watermark transaction;
- after commit and before run-summary completion.

Every restart must converge to one verified effect, an explicit retryable/partial state, or
policy-permitted quarantine.

## 11. Milestone 8 — Coverage, gaps, and watermark

### Deliverables

- Verified coverage-segment model and repository.
- Gap taxonomy and lifecycle.
- Reconstructible materialized watermark.
- Artifact-presence/integrity verifier.
- Retention invalidation integration.

### Required behavior

- Coverage is authoritative; watermark is a cached contiguous frontier.
- Contiguity is over calendar-eligible sessions/slots, not wall-clock time; exchange-closed periods
  are NOT_APPLICABLE.
- Watermark contains coverage start, exclusive frontier, verification state, generation, calendar,
  policy, last-changing run/batch, and blocking-gap count.
- Never infer progress from MAX(timestamp).
- An acquisition or integrity gap blocks the frontier.
- A sparse/no-trade slot requires explicit durable classification; no fabricated bar.
- VERIFIED_EMPTY requires successful bounded-request completion, complete verified pagination, and
  sufficiently demonstrated provider aggregation/omission semantics; an absent bar or empty
  response alone is insufficient.
- Missing/corrupt/expired/unauthorized artifacts invalidate coverage before use.
- EPHEMERAL cannot create a historical durable watermark.
- TTL and SUBSCRIPTION_BOUND frontiers remain usable only while retained rights/data remain valid.

### Tests

- Contiguous segment union and first-gap stop.
- Friday close to Monday open, intervening holiday, and daily multi-session traversal.
- Early-close slot domain and an eligible unclassified missing slot.
- Out-of-order completed requests.
- Multiple instruments from one provider request update independently.
- No-trade finding versus acquisition gap.
- Empty response with incomplete pagination cannot become VERIFIED_EMPTY.
- Complete request/pagination without demonstrated omission semantics remains unresolved.
- Deleted or corrupted raw/canonical file.
- Policy expiry and calendar snapshot change.
- Watermark reconstruction equals the materialized row.

## 12. Milestone 9 — Backfill command

### Deliverables

- A stdlib CLI entry point.
- data-root init, backfill, status, and verify commands.
- Durable backfill orchestration over the planner and commit protocol.
- Sanitized human-readable and optional structured status.

### Required behavior

- Explicit provider, dataset, instruments, timeframe, session, adjustment, and half-open bounds.
- Cap the requested end at the dataset-policy age/finalization ceiling.
- Estimate and require hard request/byte/call/cost limits before dispatch.
- Resume partial success after restart.
- Persist raw/canonical only under private_research and active policy.
- Exit nonzero for incomplete/failure; distinguish no-op and success in output.
- A multi-symbol failure commits only independently verified stream coverage.

### Acceptance progression

1. Synthetic fake provider.
2. One Alpaca security with a small, approved historical interval.
3. Restart and verify.
4. Frozen Phase 1 sample of 16 only after one-security acceptance.

No automated full-universe expansion.

## 13. Milestone 10 — Incremental update

### Deliverables

- update command.
- Safe-end calculation from calendar and provider historical/finalization semantics.
- Provider-call-free no-op.
- Durable retry/budget behavior.

### Required behavior

- Require a valid retained watermark.
- start equals the contiguous exclusive frontier.
- End includes only closed/finalizable data.
- End also satisfies the active dataset-policy minimum observation age.
- Request only uncovered intervals.
- A second identical run is a meaningful no-op with no new raw, canonical, or revision state.
- Correction windows are not silently folded into ordinary update.
- Command is suitable for later Task Scheduler or cron invocation without code changes.

### Tests

- No watermark directs to backfill/verify.
- Watermark with blocking gap refuses unsafe advancement.
- safe_end before/equal start is provider-call-free.
- One new daily session and one new 5m interval.
- Duplicate attempt and restart.
- Provider delay/finalization buffer boundary.
- The same age boundary used by backfill and repair.

## 14. Milestone 11 — Repair, revisions, and retention enforcement

### Deliverables

- repair and retention enforce commands.
- Bounded repair planner.
- Raw-only canonical replay path before provider reacquisition.
- Same-provider revision comparison/current view.
- Gap transition and coverage recalculation.
- State-first TTL/subscription purge workflow.

### Required behavior

- Repair retains any newly fetched raw evidence and preserves old permitted evidence.
- Repair applies the same policy age/finalization ceiling as every other mode.
- Complete verified policy-valid raw is replayed without a provider call when canonical output is
  missing or stale; provider fetch is reserved for missing raw or explicit correction refresh.
- Same observation/value/processing signature is a no-op.
- Changed values create a new immutable revision.
- Fatal revision uses policy-permitted quarantine and does not become current; data outside the
  age/use grant is not quarantined.
- Current-view order is trusted provider revision metadata when available, otherwise fixed raw
  `retrieved_at`, raw artifact ID, and batch ID; only VERIFIED policy-valid revisions participate.
- No cross-provider winner.
- Policy invalidation makes data query-invisible and watermark-invalid before deletion.
- Purge deletes only exact cataloged targets after root/sentinel revalidation.
- Purge is restartable at every step.

### Tests

- Simulated gap detection and repair.
- Delete/quarantine canonical output, replay retained raw, and restore coverage with no network.
- Missing/expired raw cannot be replayed and remains invalid until a permitted bounded fetch.
- Same bytes, changed pagination.
- Provider correction to one value.
- Old and new provenance remain queryable as versions.
- Current same-provider view is deterministic.
- Equal/absent provider revision timestamps resolve by the documented stable tie-breakers.
- TTL expiry and subscription termination.
- Crash before, during, and after physical deletion.

## 15. Milestone 12 — Progressive live acceptance and external scheduling

### Live gates

- Confirm private root and sentinel.
- Confirm full Alpaca evidence is present under the private governance locator.
- Confirm policy hash and exact historical SIP dataset.
- Confirm every requested bar is beyond the strict >15-minute policy boundary and finalization
  buffer.
- Confirm credentials only by presence; never print values.
- Confirm request estimates and bounded interval.
- Confirm repository/worktree contains no real data before and after.

### Progression

1. One security.
2. Frozen 16-security sample.
3. Deliberately selected larger subset.
4. Full current S&P 500 only after a separate acceptance decision based on cost, storage,
   performance, gaps, and repair evidence.

### Scheduler handoff

After manual update acceptance, document examples for:

- Windows Task Scheduler;
- cron;
- another external local scheduler.

The scheduler invokes the same idempotent update command. Do not implement Celery, Redis, Kafka,
a daemon, cloud orchestration, or an internal scheduling service.

## 16. End-to-end acceptance criteria

### Environment isolation

- test/demo never see private data.
- CI has no network, credentials, or private-root dependency.
- private_research requires an absolute initialized external root.
- Real data cannot be written inside Git even if ignored.

### Backfill

- Small Alpaca historical raw and Parquet artifacts survive process/computer restart.
- No accepted interval crosses the strict >15-minute/finalization ceiling.
- Coverage and watermark initialize only after verification.
- Resume does not repeat verified requests.

### Incremental update

- Only the new interval is requested.
- No duplicates are visible.
- Identical second run is a no-op without provider access.

### Crash safety

- Pre-publication crash cannot advance watermark.
- Post-publication/pre-SQL crash is adopted.
- SQL transaction interruption leaves catalog, request outcome, coverage, and watermark
  consistent.
- Retry produces no arbitrary copies.

### Repair

- Simulated gap is detected.
- Bounded interval is repaired.
- Canonical loss can be repaired from retained raw without a provider call.
- Coverage becomes contiguous.
- Raw and canonical provenance remain available.

### Retention

- PROHIBITED refuses before network/processing.
- EPHEMERAL creates no durable historical watermark.
- DURABLE_AUTHORIZED enables scoped private storage.
- Alpaca data at or inside the prohibited recent-time window never becomes durable or quarantined.
- TTL/subscription invalidation precedes purge.
- Unknown datasets fail closed.

### Restart

After restart the system knows:

- provider/dataset and policy status;
- runs, requests, attempts, and errors;
- raw/canonical artifact status;
- coverage and gaps;
- watermark/frontier;
- retry eligibility and provider budgets;
- pending recovery or purge work.

### Quality

- All normal tests remain offline.
- CI requires no secrets.
- No real data or private evidence is tracked.
- Full required checks pass.
- Final diff contains no feature, dashboard, agent, strategy, broker, or Phase 3+ implementation.

## 17. Exact repository checks

Run from the repository root:

~~~text
uv lock --check
uv sync --locked --all-groups
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked mypy src tests
uv run --locked pytest
uv build
git diff --check
~~~

Additionally verify:

- Git ignore behavior for secrets and legacy repository-local private data paths;
- no tracked file under raw, normalized, curated, features, operational, logs, quarantine, or
  private governance evidence;
- no credential names paired with populated values;
- no real provider payload or market-data value in fixtures;
- private-root and destructive-path tests on Windows and CI platforms;
- generated build artifacts are not part of the final diff.

## 18. Explicitly out of scope

- Feature Engine or observed feature execution.
- Returns, volatility, correlations, ranks, or Market State.
- Dashboard, checkpoints, alerts, news, or events.
- AI agents or interpretation.
- Strategy, backtesting engine, portfolio analytics, or broker execution.
- Options surface/chain ingestion or multi-asset ingestion.
- Complete corporate-action/reference master.
- Cross-provider curated winner policy.
- PostgreSQL, cloud deployment, microservices, queues, or multi-user SaaS.
- Real-time market data, public display, or redistribution.
- Full S&P 500 before progressive acceptance.

Phase 2 is complete when the project has a living, reliable historical market-data store for the
approved narrow scope—not when the entire investment platform roadmap is complete.

## 19. Definition of Done

- Every accepted Phase 2 design invariant is implemented or explicitly returned for decision.
- All three ingestion modes are represented: backfill, update, and repair; verification is an
  explicit supporting operation.
- Retention policy controls real pipeline behavior.
- Root safety and environment isolation fail closed.
- Operational state survives restart and is transactionally consistent.
- Raw and canonical storage are immutable, verified, recoverable, and semantically idempotent.
- Coverage and watermark satisfy retention and contiguity invariants.
- Manual commands pass synthetic and controlled live acceptance.
- External scheduling requires only invoking the accepted update command.
- Full quality, data, security, and diff checks pass.
- Documentation distinguishes Implemented, Designed, Planned, and Future behavior.
