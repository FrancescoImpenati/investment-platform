# Phase 2 — Living Data Ingestion

- **Status:** Design approved; offline implementation checkpoint present; live acceptance pending
- **Date:** 2026-09-01
- **Baseline:** main at 76e3ffd, including approved Phase 0, Phase 1, and Databento research
- **Implementation contract:** [PLAN_PHASE_2.md](../../PLAN_PHASE_2.md)
- **Scope:** local persistent historical market-data ingestion for private research

## 1. Status vocabulary

This document uses the following terms literally:

- **Implemented:** behavior present in source code and covered by the current test suite.
- **Designed:** an architectural decision recorded here or in an accepted ADR, but not yet built.
- **Planned:** implementation work ordered in PLAN_PHASE_2.md.
- **Future:** work outside Phase 2.

An accepted ADR records a design decision. It does not mean that its implementation exists.

## 2. Outcome and boundary

Phase 2 turns the validated Phase 0–1 pipeline from:

~~~text
manual research run
-> temporary data
-> cleanup
~~~

into:

~~~text
persistent private historical store
-> backfill
-> incremental update
-> repair
-> durable operational state
~~~

The result is a living, restartable historical market-data store on one local machine. It is not
a distributed system, a cloud service, a multi-user application, or a real-time trading engine.

The first durable live stream is intentionally narrow:

- provider: Alpaca;
- dataset: historical SIP US stock bars;
- timeframes: 1d and 5m;
- retention eligibility: only observations strictly older than 15 minutes, plus any conservative
  provider-finalization buffer;
- session: US regular trading hours;
- initial adjustment modes: unadjusted and provider split-adjusted only where already represented
  by the canonical contract;
- use: individual, non-professional, private, educational, non-commercial research.

The complete corporate-action source remains unresolved. That does not block unadjusted or
explicitly provider-adjusted historical bar ingestion. Phase 2 must not imply that provider
adjustment solves point-in-time corporate actions.

## 3. Implementation state

### 3.1 Baseline implemented through Phase 1 at design approval

The repository already implements:

- stable internal instrument UUIDs and temporal external identifiers;
- aware UTC timestamps, America/New_York session interpretation, and half-open intervals;
- canonical 1d and 5m PriceBar contracts with explicit provenance and adjustment state;
- a synchronous provider-neutral boundary for bounded bars and corporate actions;
- standard-library HTTP adapters for Alpaca, Twelve Data, and Massive;
- provider-specific normalization and non-destructive validation;
- immutable raw artifacts with sanitized manifests, SHA-256 checksums, bounded reads, atomic
  directory publication, replay verification, and same-batch collision protection;
- canonical Parquet price bars, partitioning, in-memory DuckDB queries, and Polars results;
- a pairwise quality harness with no automatic cross-provider winner;
- a finite, sequential, external-temporary-root Phase 1 live runner;
- a 16-security empirical Alpaca SIP versus Twelve Data Basic bake-off;
- fully offline normal tests and CI with synthetic provider-shaped fixtures.

The provider quality report records the empirical result. The Databento report is approved
research only and adds no adapter or persistence behavior.

### 3.2 Designed previously, but not implemented at design approval

The Phase 0 design already anticipated:

- backfill, incremental update, and repair intents;
- a multidimensional contiguous watermark rather than MAX(timestamp);
- artifact idempotency distinct from workflow and semantic idempotency;
- a transactional operational store separate from Parquet and DuckDB;
- a maintainable exchange calendar.

Those passages were forward-looking and must not be reported as current capabilities.

### 3.3 Gaps identified at design approval

At design approval the repository did not contain:

- runtime environment profiles;
- a durable external private data-root contract with a sentinel;
- a retention-aware provider-by-dataset policy;
- SQLite operational state, schema migrations, or repositories;
- durable run, request, attempt, retry, budget, coverage, gap, or watermark state;
- a backfill/update/repair planner;
- semantic idempotency across distinct provider batch IDs;
- canonical batch manifests or crash recovery across Parquet and state;
- a maintained exchange calendar;
- a user-facing ingestion CLI or scheduler integration.

At that baseline the provider response transport was bounded but materialized each HTTP response
in memory. The raw payload boundary permitted file-backed payloads, but the live transport did not
spool large pages directly to the private root.

At that baseline the Parquet store published the files of a multi-part batch individually and
discovered files by glob. An abrupt process crash could therefore leave a partial batch visible.

### 3.4 Implemented in the current offline checkpoint

The Phase 2 source and synthetic test suite now implement:

- the five runtime profiles and their fail-closed capability matrix;
- explicit initialization and repeated validation of an external sentinel-owned private root;
- an exact machine-readable retention catalog and enforcement at request, storage, query,
  quarantine, watermark, export, and purge boundaries;
- standard-library SQLite migrations, operational repositories, short transactions, diagnostics,
  and a durable single-writer lease;
- a provider-neutral XNYS calendar adapter with immutable snapshots, expected 1d/5m RTH slots,
  calendar-diff reconciliation, and deterministic checksums;
- deterministic stream/request/artifact/batch/observation identities, bounded planning, budgets,
  and persisted progress for backfill, update, repair, and resume;
- bounded file-backed response spooling, raw-first immutable publication, atomic canonical batch
  publication, catalog-driven DuckDB visibility, and crash recovery;
- retention-aware coverage, gap episodes, reconstructed watermarks, semantic no-op handling,
  same-provider revision provenance, and state-first invalidation/purge;
- a non-interactive CLI for root initialization, ingestion, resume, status, verify, and retention;
- offline synthetic integration and fault-injection tests covering restart, incremental no-op,
  gap repair, raw replay, orphan adoption, pacing, publication failures, and integrity loss.

No real Alpaca payload has been persisted by this checkpoint. The private root/evidence gate,
controlled AAPL and 16-security acceptance, external scheduler activation, final Phase 2 pull
request, and Phase 2 approval remain pending. Phase 3 has not started.

## 4. Architectural invariants

Phase 2 preserves the dependency direction:

~~~text
provider
-> immutable provider-native raw evidence
-> provider-specific deterministic normalization
-> deterministic validation
-> immutable canonical batch publication
-> verified coverage
-> watermark
~~~

The following invariants are mandatory:

1. Retention permission is checked before network access, processing, persistence, query
   visibility, and watermark advancement.
2. Provider-native raw evidence is durable before durable downstream use.
3. Parquet remains authoritative for analytical observations.
4. DuckDB remains an in-process analytical query engine over verified Parquet files.
5. SQLite coordinates mutable operational state and never becomes a second OHLCV store.
6. A ticker is never a stream identity; the stable internal instrument UUID is.
7. All intervals remain half-open and persisted timestamps remain aware UTC.
8. Missing available_at remains null; retrieval or ingestion time is never substituted.
9. Questionable records are flagged or quarantined, not silently repaired.
10. No cross-provider winner or averaging policy is introduced.
11. At-least-once attempts are expected; observable effects must be idempotent.
12. A watermark advances only after canonical publication, verification, catalog commit, and
    retention checks.

## 5. Component model

~~~mermaid
flowchart TD
    A["Manual CLI now; external scheduler later"] --> B["Environment and root guard"]
    B --> C["Dataset retention gate"]
    C --> D["Planner: backfill, update, or repair"]
    D --> E["SQLite run/request ledger and budgets"]
    E --> F["Provider adapter"]
    F --> G["Immutable raw artifact"]
    G --> H["Normalization and validation"]
    H --> I["Canonical batch staging"]
    I --> J["Atomic directory publication"]
    J --> K["Post-publication verification"]
    K --> L["SQLite atomic catalog/coverage/gap/watermark commit"]
    L --> M["DuckDB reads only VERIFIED catalog entries"]
    L --> N["Status, verify, and future external scheduling"]
~~~

### 5.1 Environment and root guard

Resolves the runtime capability profile, validates the external root and sentinel, and grants only
the filesystem and network capabilities permitted by that profile.

### 5.2 Retention policy catalog

Loads a versioned, machine-readable, exact provider-by-dataset catalog. It produces an immutable
policy snapshot for each run. Unknown datasets are denied.

### 5.3 Planner

Converts a desired stream interval into bounded requests after subtracting verified, retained
coverage. Backfill, update, and repair are planner intents; provider adapters continue to receive
bounded requests rather than mode-specific commands.

### 5.4 Operational state repositories

Use SQLite transactions for runs, requests, attempts, raw and canonical artifact catalogs, stream
keys, coverage, gaps, watermarks, retry eligibility, provider budgets, errors, policy status,
purge state, and a single-writer lease.

### 5.5 Storage publication boundary

Persists raw artifacts and canonical batch directories immutably. A canonical batch is visible to
queries only after post-publication verification and a SQLite catalog commit.

### 5.6 Coverage verifier

Combines the request result, canonical observations, provider aggregation semantics, and a
versioned trading-calendar snapshot. It distinguishes acquisition, integrity, expected-slot, and
legitimate sparse/no-trade findings.

### 5.7 Recovery coordinator

Inspects incomplete operational rows, staging directories, raw manifests, and published canonical
manifests after restart. It adopts verified work or safely replays the smallest missing step.

## 6. Public repository and private runtime

### 6.1 Public repository

The Git repository may contain only:

- source code;
- tests;
- schemas and migrations as source text;
- architecture, ADRs, plans, and governance summaries;
- synthetic fixtures;
- small samples with explicit redistribution rights;
- non-secret policy/configuration catalogs;
- non-substitutive aggregate reports permitted by the applicable rights.

It must never contain private evidence, credentials, account data, licensed payloads, real
canonical observations, quarantine content, operational state, or private logs.

### 6.2 Private runtime

The private environment owns:

- credentials read only from process environment variables;
- licensed raw, normalized, and later curated data;
- later reversible features where permitted;
- SQLite operational state and its journal files;
- sanitized local logs;
- staging and quarantine;
- full private governance evidence;
- future personal portfolio or broker data, when separately designed.

The private root is a security boundary, not a convenience path. Repository ignore rules are only
a secondary defense.

## 7. Runtime environments

Profiles are separate explicit values even when they share implementation:

| Profile | Network | Credentials | Storage | Private root |
| --- | --- | --- | --- | --- |
| test | Denied | Never read | Injected temporary synthetic fixture root | Never opened |
| ci | Denied and non-overridable | Not required or injected | Temporary synthetic fixture root | Configuration rejected |
| development | Denied by default; bounded preflight opt-in | Read only for the explicit preflight | Synthetic/sample by default; real response ephemeral | No durable real data |
| private_research | Allowed through provider boundary | Existing provider environment names | Durable licensed storage under policy | Required and validated |
| demo | Denied | Never read | Public sample/synthetic input and temporary output | Never opened |

test and ci remain separate profile names. They share one offline capability base, but ci is
non-overridable and rejects a configured private root. This makes a hidden CI dependency fail
immediately while avoiding duplicated policy logic.

test and demo do not resolve or touch INVESTMENT_PLATFORM_DATA_ROOT even if the host environment
contains a stale value. Provider constructors are unavailable through those profiles.

development permits only an explicitly requested, bounded live preflight. It does not turn into a
durable data mode merely because a path or credential happens to exist. Durable real-data work
requires private_research.

private_research is the only Phase 2 profile authorized to persist real market data.

## 8. Minimal configuration and precedence

Initial configuration uses:

~~~text
INVESTMENT_PLATFORM_ENV
INVESTMENT_PLATFORM_DATA_ROOT
~~~

Rules:

1. INVESTMENT_PLATFORM_ENV selects one profile and is required for mutating commands. There is no
   implicit fallback from private_research to development.
2. INVESTMENT_PLATFORM_DATA_ROOT is required only for private_research and must be an absolute
   path. Tests receive temporary roots through dependency injection, not this variable.
3. Provider secrets keep their existing environment-variable names:
   APCA_API_KEY_ID, APCA_API_SECRET_KEY, TWELVE_DATA_API_KEY, and MASSIVE_API_KEY.
4. A committed retention catalog supplies dataset rights. Runtime configuration may restrict that
   catalog but may never expand it.
5. CLI arguments express operation intent, interval, streams, and safety limits. They do not carry
   credentials or override retention rights.
6. No dotenv loader is required. A local .env file may be used by a human shell tool, but the
   application neither requires nor loads it.
7. No value is duplicated across environment variables, a settings file, and code. One concept has
   one authoritative source.

## 9. Private data-root contract

ADR 0006 defines the decision. The root must:

- be supplied as an absolute path;
- resolve physically outside the repository;
- be a dedicated local filesystem directory;
- not equal or contain the repository, and not be contained by it;
- not be a filesystem/drive root, home/profile directory, repository ancestor, system directory,
  or general temporary directory;
- not be a Windows UNC/network path in Phase 2;
- be newly initialized or already contain the expected sentinel;
- reject symlink, junction, or reparse-point escapes for managed targets;
- be revalidated before every mutating or destructive operation.

Initialization is permitted only for a nonexistent path or an empty dedicated directory. It writes
a direct-child sentinel such as:

~~~json
{
  "schema_version": 1,
  "purpose": "investment_platform_private_research",
  "root_id": "stable-uuid",
  "canonical_path": "absolute-resolved-path",
  "created_at": "aware-utc-timestamp"
}
~~~

Subsequent commands require a valid schema, purpose, root ID, and canonical path. A missing,
corrupt, moved, or mismatched sentinel fails closed.

Deletion is inventory-driven. The system never recursively deletes the private root, a caller
supplied glob, or an unverified path. Every target must be cataloged, resolve below the same root,
inherit the originating dataset policy, and pass sentinel validation immediately before removal.

### 9.1 Logical layout

Directories are created lazily only when implemented behavior owns them:

~~~text
<PRIVATE_DATA_ROOT>/
├── .investment-platform-root.json
├── raw/
├── normalized/
├── curated/                 # future; not written in Phase 2
├── features/                # future; not written in Phase 2
├── staging/                 # same filesystem as publication targets
├── operational/
│   ├── ingestion.sqlite3
│   ├── ingestion.sqlite3-wal
│   ├── ingestion.sqlite3-shm
│   └── locks/
├── logs/
├── quarantine/
└── governance/
    └── evidence/
~~~

staging is the deliberate addition to the proposed layout. Keeping it under the same verified root
and filesystem permits atomic rename of a completed canonical batch. It also gives recovery one
bounded place to inspect and clean.

SQLite journal sidecars are shown because they are part of operational state while present. A
backup must use SQLite-aware backup/checkpoint behavior rather than copying only the main file.
Backup and disaster-recovery policy are outside the initial Phase 2 implementation.

quarantine inherits the source dataset retention. It is never a way to retain forbidden or expired
data. logs contain sanitized operational metadata, not response bodies, credentials, complete
authenticated URLs, or market-data values.

The full Alpaca ticket evidence belongs at:

~~~text
governance/evidence/alpaca/ticket-342496/
~~~

inside the private root. Only the redacted governance record is public.

## 10. Retention-aware dataset policy

ADR 0007 defines a new first-class policy axis. The existing LicenseClassification remains a
redistribution/privacy classification and is not reused as a retention engine.

### 10.1 Modes

| Mode | Processing and persistence behavior | Watermark behavior |
| --- | --- | --- |
| PROHIBITED | Refuse before provider market-data access, processing, or persistence | Forbidden |
| EPHEMERAL | Private run-scoped processing; verified cleanup; no durable licensed artifacts | No historical durable watermark |
| TTL | Private persistence until an explicit expiry | Valid only while every referenced artifact remains present and unexpired |
| SUBSCRIPTION_BOUND | Use only while exact entitlement/license status is active | Invalid immediately when status becomes inactive or unknown |
| DURABLE_AUTHORIZED | Durable private raw, normalized, and permitted derived retention | Durable watermark permitted |
| SYNTHETIC_UNRESTRICTED | Synthetic/sample behavior under its public provenance terms | Test/demo watermark permitted |

PROHIBITED ingestion policy does not prevent documentary research or a separately approved,
payload-free entitlement check. It prevents the living ingestion pipeline from requesting or
retaining the prohibited market dataset.

### 10.2 Minimal machine-readable model

Each exact provider/dataset entry contains:

- stable policy ID, revision, and content hash;
- provider and exact dataset/feed identifier;
- permitted runtime profiles and use scope;
- processing permission;
- raw retention rule;
- normalized/reversible retention rule;
- derived retention rule;
- TTL, grace period, or expiry where applicable;
- request-eligibility constraints such as minimum observation age;
- delete-on-termination behavior;
- public-display permission;
- redistribution permission;
- evidence reference;
- verification date and optional review date;
- ACTIVE, SUSPENDED, EXPIRED, or PENDING status;
- concise notes.

Booleans default to denial. An absent provider/dataset entry is denied. Wildcards do not grant
rights to provider datasets. A run stores the policy revision and hash it used.

Policy enforcement occurs:

1. before request planning or provider construction;
2. before each provider request;
3. before raw persistence;
4. before normalization;
5. before canonical publication;
6. before query visibility;
7. before coverage/watermark commit;
8. on process startup and verify;
9. before export, purge, or deletion.

A policy becoming more restrictive during a run blocks publication and watermark advancement.
Local configuration may narrow behavior but cannot turn PENDING or denied rights into permission.

### 10.3 Initial catalog decisions

| Provider / exact dataset scope | Initial mode/status | Phase 2 use |
| --- | --- | --- |
| Alpaca historical price_bars_sip | DURABLE_AUTHORIZED | Primary 1d/5m scope after the >15-minute age gate |
| Alpaca historical options | UNVERIFIED / PENDING | Evidence does not cover this dataset; no active policy entry |
| Alpaca crypto | UNVERIFIED / PENDING | Evidence does not cover historical or real-time crypto; no active policy entry |
| Alpaca real-time/streaming, news, and other unlisted datasets | No active catalog entry | Fail closed before request or persistence |
| Twelve Data price_bars_us_daily | SUBSCRIPTION_BOUND | Secondary/reference; not primary Phase 2 live stream |
| Twelve Data price_bars_standard_us_intraday | SUBSCRIPTION_BOUND | Not a SIP-equivalent volume source |
| Databento OPRA.PILLAR | EPHEMERAL | Future options candidate pending exact clarification |
| Massive Individual price_bars | PROHIBITED | Existing adapter dataset; no intended non-display ingestion |
| Massive Individual corporate_actions | PROHIBITED | Existing adapter dataset; no intended non-display ingestion |
| Reserved synthetic/sample price-bar datasets | SYNTHETIC_UNRESTRICTED | Offline tests and demo |
| yfinance | Denied by absence | Sanity/reference only; never canonical living ingestion |

The Alpaca historical SIP stock-bar decision is backed by
[the redacted rights record](../governance/data-rights/alpaca-historical-sip.md). It is specific to
the described private use. It grants no public display, redistribution, resale, or real-time
retention.

Historical options and crypto are `UNVERIFIED / PENDING`: the evidence does not cover them, and no
active software policy exists. Real-time data, news, and every other unlisted Alpaca dataset also
have no active entry and fail closed. Activation requires new dataset-specific evidence, an exact
stable dataset/feed key, and a request-eligibility rule.

For this entry, `minimum_observation_age` is `PT15M` and eligibility is conservative: an
observation is durable-request eligible only when its exclusive end is strictly earlier than the
evaluation time minus 15 minutes, and after any larger finalization buffer. The planner applies
that ceiling to backfill, update, and repair. If a provider unexpectedly returns younger or
out-of-bound market data, the attempt records only a sanitized policy failure. The response is not
published as durable raw, passed to normalization, moved to quarantine, or used for coverage.

Bounded transport may use an attempt-scoped file under `staging/transport-attempts/` to avoid
unbounded memory. It is never evidence or query-visible. Policy-permitted content is published to
raw before downstream use; rejected content is deleted immediately, and recovery deletes any
unadopted transient attempt before other work.

Twelve Data entries require dataset-specific entitlement status and termination behavior. They do
not become active merely because a credential exists. Databento technical entitlement does not
prove durable retention.

### 10.4 Expiry, termination, and purge

Purge is state-first:

1. acquire the writer lease and revalidate the root/sentinel;
2. atomically mark the dataset unavailable to readers;
3. invalidate affected coverage and watermarks;
4. catalog exact relative deletion targets and a purge run;
5. commit SQLite state;
6. remove only verified catalog targets idempotently;
7. verify absence and mark purge complete.

A crash before deletion leaves the data inaccessible and the purge resumable. A crash after file
deletion but before final status is resolved by verifying absence. Sanitized policy/audit rows may
remain only when their retention is permitted.

## 11. Operational state store

ADR 0008 selects SQLite under operational/ingestion.sqlite3.

### 11.1 Recommendation

SQLite fits the Phase 2 deployment:

- embedded in Python through the standard library;
- no server, service account, network port, or separate lifecycle;
- transactional updates across related operational rows;
- constraints, indexes, foreign keys, migrations, and restart recovery;
- one local machine and one ingestion writer with concurrent status readers.

Implemented connection policy:

- foreign_keys=ON;
- journal_mode=WAL, with failure if the local VFS cannot enable it;
- synchronous=FULL for state guarding coverage and watermarks;
- a bounded busy timeout;
- BEGIN IMMEDIATE for short write transactions;
- no HTTP, sleeps, Polars transforms, Parquet writes, or checksum scans inside a SQL transaction;
- one expiring writer lease with heartbeat and safe stale-lease takeover.

SQLite WAL requires same-host local filesystem semantics. This reinforces, rather than replaces,
the private-root guard. The official SQLite documentation describes
[atomic commit](https://www.sqlite.org/atomiccommit.html) and
[WAL behavior and constraints](https://www.sqlite.org/wal.html).

### 11.2 Conceptual schema ownership

The initial schema owns:

- schema migrations;
- ingestion runs and terminal status;
- bounded request specifications and attempts;
- raw artifact catalog plus request/page and attempt-observation provenance links;
- fixed batch contexts, canonical batches/files, and per-stream publication outcomes;
- stream keys;
- coverage segments and gaps;
- materialized watermarks;
- retry/error state;
- provider budget windows and observations;
- policy snapshots and current dataset-policy status;
- retention/purge runs;
- writer leases.

SQLite stores identifiers, paths relative to the private root, hashes, bounds, counts, statuses,
and sanitized errors. It does not store provider response bodies or canonical bar values.

### 11.3 Alternatives

- **PostgreSQL:** excellent for remote access, multiple writers, users, and services, but adds a
  database server, credentials, upgrades, backup operations, and deployment work with no Phase 2
  benefit. Reconsider only when the topology changes.
- **Persistent DuckDB:** remains inappropriate as a second authority for mutable workflow state
  and canonical bars. DuckDB continues to query verified Parquet.
- **JSON files or an append-only custom log:** would require custom locking, indexes, foreign-key
  checks, compaction, and multi-record atomicity.
- **LMDB/RocksDB-style key-value stores:** add dependencies and fit relational run/request/coverage
  state less naturally.

## 12. Trading calendar

Phase 2 introduces a small provider-neutral TradingCalendar boundary returning immutable sessions:

~~~text
sessions(start_date, end_date)
-> (session_date, open_utc, close_utc, calendar_snapshot_id)
~~~

The offline implementation adopts `exchange-calendars` with calendar XNYS behind that boundary.
It provides maintained sessions and regular-trading open/close semantics for NYSE. The dependency
gate was resolved in the locked project environment as follows:

| Gate fact | Verified resolution |
| --- | --- |
| Project Python | 3.13.14 |
| `exchange-calendars` | 4.13.2, constrained to `>=4.13.2,<5` and locked by uv |
| Declared Python range | `>=3.10,<4` |
| Direct calendar dependencies in the lock | korean-lunar-calendar, NumPy, Pandas, python-dateutil, pyluach, toolz, and tzdata |
| Boundary proof | XNYS is imported only by the calendar adapter; adapter tests cover ordinary sessions, the 2025-07-03 early close, DST, holidays, half-open bounds, counts, and checksums |

The resolved installed-package metadata records Apache-2.0 for `exchange-calendars` and `tzdata`,
MIT for `korean-lunar-calendar`, BSD-3-Clause for Pandas and toolz, a dual-license marker for
python-dateutil, and a permissive license expression for NumPy. The installed pyluach metadata does
not declare a license field, so this record does not infer one. This inventory records the
dependency-gate observation; it is not a legal opinion or a substitute for upstream license files.

Package and API references used by the architectural decision remain:

- [exchange_calendars package and examples](https://pypi.org/project/exchange-calendars/);
- [current dependency metadata](https://github.com/gerrymanoim/exchange_calendars/blob/master/pyproject.toml).

pandas_market_calendars is not preferred because it depends on exchange-calendars and Pandas,
adding another wrapper without a Phase 2 need. Annual exchange PDFs remain authoritative
regression evidence, not a manually copied production calendar. A custom multi-year holiday table
is rejected.

`exchange-calendars` brings Pandas and NumPy transitively. The accepted dependency is a deliberate,
calendar-only exception to the default dependency policy, not permission to introduce Pandas into
domain or analytical transformations. Both libraries remain confined behind the adapter from the
platform's point of view; the rest of the project continues to use Polars for columnar work.

Each persisted calendar snapshot records:

- library and version;
- calendar code;
- tzdata version;
- covered date range;
- generated-at UTC time;
- ordered sessions with UTC open/close;
- deterministic snapshot checksum.

Requests and coverage reference the snapshot used for verification. A dependency/calendar upgrade
must diff affected sessions. Changed expectations mark coverage STALE and schedule verification;
they never silently rewrite bars.

Offline tests retain focused official cases rather than years of hard-coded dates:

- ordinary 78-bar 5m regular sessions;
- the 42-bar 2025-07-03 early close;
- spring and autumn US DST boundaries;
- known exchange holidays from the Phase 1 window;
- an interval ending exactly at session close.

A session grid establishes eligible intervals, not proof that every instrument traded. Alpaca
bars may legitimately omit intervals with no eligible trades.

## 13. Identity and idempotency

Phase 2 does not claim exactly-once delivery or execution.

### 13.1 Request identity

A request specification hash is deterministic canonical JSON over:

- provider and exact dataset/feed;
- internal UUID/provider-identifier mappings sorted by a canonical key; caller order is excluded;
- timeframe, session, adjustment, and every provider stream dimension;
- half-open start and end;
- provider request-mapping semantic version.

It excludes credentials, authenticated URLs, retrieval time, run ID, retry number, page token, and
request batching accidents.

A request instance is one planned bounded unit. An attempt is one execution of that instance.
Repair may deliberately create a new request instance with the same specification hash and a
different reason.

### 13.2 Raw artifact identity

A raw artifact is one immutable provider response page. Artifact identity binds:

- request specification;
- stable page ordinal or normalized page relation;
- canonical media type/content encoding, exact payload checksum, and byte count.

Safe provider request IDs and other attempt-varying response metadata remain provenance linked to
the artifact. They do not participate in its identity, so an identical request/page/content replay
can converge even when the provider assigns a new request ID.

The immutable raw manifest written by the first successful publisher separates identity-bearing
fields (request specification, stable page relation, payload hash/size/media type) from its fixed
first-persistence provenance. Later attempts with the same artifact ID verify only the stable
identity/content fields, reuse the artifact, and append their own `attempt_id`, `retrieved_at`, and
safe provider request ID as SQLite artifact-observation links. They never rewrite the manifest or
treat changed attempt metadata as a content collision.

An identical request/page/content replay resolves to the existing artifact. Changed content is a
new artifact and may represent a provider correction. Raw page tokens and authenticated next URLs
are never persisted.

### 13.3 Canonical batch identity

A canonical batch is the atomic publication unit. Its identity binds:

- request specification hash;
- ordered raw artifact identities/checksums;
- canonical schema version;
- normalizer and validator versions;
- calendar snapshot where interval construction depends on it.

It is not derived only from the request: the same request may legitimately return corrected bytes.

After raw acquisition completes, SQLite persists one batch context before normalization. It fixes
the deterministic batch identity, ordered inputs, processing versions, and the single
`ingested_at`/manifest creation timestamp used by every replay. Attempt time, current run status,
and retention-policy snapshot stay in operational provenance rather than content identity. A
replay must reuse the context; it may not regenerate volatile timestamps under the same batch ID.

### 13.4 Canonical observation identity

Within one stream:

~~~text
(stream_id, timestamp_start, timestamp_end)
~~~

is the canonical observation key. stream_id already includes provider, dataset, instrument UUID,
timeframe, session, adjustment, and other series-defining dimensions.

Ticker, provider page, retrieval timestamp, ingestion timestamp, and raw batch are provenance, not
observation identity.

An observation value hash covers normalized semantic values and the processing signature while
excluding volatile retrieval/ingestion provenance:

- same key and same value hash: semantic no-op, with additional raw provenance link where useful;
- same key and changed value hash: immutable same-provider revision;
- different provider: different stream; no winner is inferred.

The current same-provider view considers only VERIFIED, policy-valid revisions. Its total order is
trusted provider revision sequence/time when supplied, otherwise the raw artifact's fixed
`retrieved_at`, then raw artifact ID and canonical batch ID as stable tie-breakers. The tie-breakers
provide determinism, not a claim that an arbitrary conflicting value is economically truer. Replay
commit order is never the selector. All revisions and provenance remain available; cross-provider
comparison stays explicit and non-substitutive.

## 14. Canonical batch publication

The planned normalized layout is batch-oriented:

~~~text
normalized/price_bars/
└── provider=alpaca/
    └── dataset=price_bars_sip/
        └── batches/
            └── batch_id=<canonical-batch-id>/
                ├── manifest.json
                └── timeframe=5m/
                    └── year=YYYY/
                        └── month=MM/
                            └── part-0000.parquet
~~~

The batch directory is prepared under top-level staging on the same filesystem. Its manifest
contains:

- batch ID and request specification hash;
- raw artifact identities and checksums;
- code/schema/normalizer/validator/calendar versions;
- ordered file paths, sizes, and checksums;
- row counts and temporal bounds by stream;
- every requested stream's PUBLISHABLE or BLOCKED outcome, validation summary, and semantic
  duplicate/revision counts;
- the fixed batch-context creation time.

Parquet parts are closed and hashed first; `manifest.json` is written last and is the staging
completion record. A manifest's presence does not make a batch query-visible: publication,
reopening verification, and the SQLite commit are still required.

The SQLite catalog links each use/publication to the policy snapshot that authorized it. That
attempt-varying authorization provenance is not embedded in content identity or allowed to change
immutable bytes under an existing batch ID.

Originating request instances, attempts, and runs are also SQLite links; the immutable manifest
contains the stable request specification, not a particular replay's IDs.

Readers never glob every Parquet file. SQLite supplies only the explicit file list of canonical
batches in VERIFIED state whose policy remains valid.

VERIFIED describes the integrity of published batch contents, not success for every stream in a
multi-stream request. Recoverable observations remain present with quality flags. A fatal
validation blocks the affected stream for the entire bounded request interval; its rows are not
silently dropped into a partial coverage claim. A batch may atomically publish the independently
PUBLISHABLE streams while recording blocked stream outcomes. SQLite commits coverage only for
those publishable streams and marks the request SUCCESS, PARTIAL, or FAILED accordingly. If no
stream is publishable, no canonical batch or coverage is published: publication is skipped and
SQLite commits the FAILED request plus blocking findings with the watermark unchanged. Permitted
raw evidence and sanitized validation findings remain replayable; quarantine is used only when the
active policy permits it.

## 15. Commit protocol and crash recovery

ADR 0009 defines a filesystem-first protocol because Parquet and SQLite cannot share one atomic
transaction.

### 15.1 Protocol

1. Validate profile, root, sentinel, policy, writer lease, and request budget.
2. Create an ingestion run and planned bounded requests in SQLite.
3. Start a request attempt, receive each page through bounded transient transport, and verify its
   response bounds/age against policy before downstream use. Persist every policy-permitted page as
   immutable raw evidence. Unexpected unauthorized bytes are erased from transient storage and
   never enter raw or quarantine.
4. Mark raw acquisition complete only after correct pagination termination.
5. Create or reuse the durable batch context with fixed replay timestamps.
6. Replay raw artifacts into deterministic normalization.
7. Validate per stream without dropping recoverable observations; mark fatal stream outcomes
   BLOCKED and quarantine only as policy permits.
8. Write the complete canonical batch and manifest for publishable streams in staging.
9. Verify staged schema, identities, uniqueness, counts, hashes, bounds, and all stream outcomes.
10. Atomically rename the entire batch directory to its final immutable location.
11. Reopen and verify the published directory.
12. In one short SQLite transaction:
    - catalog the canonical batch and files as VERIFIED;
    - link raw provenance;
    - add coverage only for PUBLISHABLE streams and gap/blocking findings for the others;
    - recalculate the contiguous frontier;
    - advance or invalidate the watermark as policy permits;
    - commit the terminal SUCCESS, PARTIAL, or FAILED request outcome.
13. Commit SQLite, derive any now-terminal run summary idempotently, release the lease, and emit
    sanitized status.

The filesystem rename is not the watermark commit. A published but uncataloged batch is invisible
to analytical readers until recovery verifies and adopts it.

### 15.2 Recovery matrix

| Last durable evidence | Recovery |
| --- | --- |
| No run row | No operation existed |
| Run/request planned, no raw artifact | Dispatch or retry within durable limits |
| Partial provider attempt | No coverage; restart the bounded request unless a stable resumable provider cursor is explicitly supported |
| Transient transport spool | Never adopt as evidence; delete after root/policy validation, then retry if eligible |
| Raw artifact exists, DB link missing | Verify manifest/checksum and adopt by deterministic identity |
| Complete raw request, no batch context | Create the context once, then replay without redownload |
| Batch context exists | Reuse its timestamps and processing identity; never regenerate them |
| Incomplete staging directory | Validate target, remove only that staging entry, and rebuild |
| Complete verified staging directory | Publish by atomic rename |
| Final batch directory exists, SQLite not committed | Reopen, verify, and adopt; do not create a suffixed copy |
| SQLite transaction interrupted | SQLite rolls back; batch remains adoptable and watermark unchanged |
| Batch/request terminal, run summary not terminal | Reconcile the run summary idempotently |
| Watermark references missing/corrupt files | Invalidate before use and create repair work |
| Purge pending | Resume exact cataloged targets |

A crash before publication cannot advance the watermark. A crash after publication but before
SQLite commit leaves an invisible, recoverable batch. A retry after the SQLite commit is a
meaningful no-op.

Process-crash behavior is tested with fault injection at every numbered boundary. Power-loss
durability additionally depends on SQLite FULL synchronous mode, filesystem semantics, flush
behavior, and post-restart verification; the platform does not promise more than those components
can guarantee.

## 16. Coverage, gaps, and watermarks

### 16.1 Stream key

A price-bar stream includes:

- provider;
- exact dataset/feed;
- data kind;
- instrument UUID;
- timeframe;
- session scope;
- adjustment state;
- bar/currency semantics where series-defining;
- any additional provider dimension that changes the logical series.

Provider ticker, request page size, retry number, batch ID, normalizer version, and calendar
version do not define a stream. They remain provenance or verification metadata.

### 16.2 Coverage is authoritative

Coverage segments record:

- stream and half-open interval;
- request and verified canonical batch;
- policy snapshot and current retention validity;
- calendar snapshot;
- row and artifact counts;
- verification status;
- discovered gaps/findings.

“Contiguous” is evaluated over the ordered eligible domain defined by the referenced
CalendarSnapshot and stream session scope, not over every wall-clock instant. For an RTH stream:

- exchange-closed overnight periods, weekends, and holidays are NOT_APPLICABLE and do not create
  gaps;
- daily coverage advances by verified expected sessions;
- 5m coverage advances by eligible slots between each session's actual open and close, including
  early closes;
- a slot with no provider observation advances only after the bounded request completed
  successfully, every page was acquired and verified, pagination reached its valid terminal state,
  and sufficiently demonstrated provider aggregation/omission semantics justify a durable
  VERIFIED_EMPTY/no-eligible-trade fact;
- an unclassified missing eligible slot remains blocking.

An empty provider response or an absent bar is not, by itself, sufficient for `VERIFIED_EMPTY`.

The exclusive watermark may therefore cross Friday close to the next eligible Monday slot, or a
holiday, only because the recorded calendar snapshot proves the intervening time is closed. It may
not skip an eligible session or bar. Calendar changes make the affected proof stale.

The watermark is a materialized frontier reconstructible from coverage, not a separate source of
truth:

> The watermark is the greatest exclusive boundary reachable across the calendar-defined eligible
> domain from retained VERIFIED or VERIFIED_EMPTY coverage without a blocking gap.

It stores at least:

- coverage start;
- contiguous-through exclusive boundary;
- last verified session;
- verification status and timestamp;
- coverage generation;
- calendar and policy snapshot;
- blocking-gap count;
- run and canonical batch that last changed it.

MAX(timestamp) is insufficient because it ignores missing pages, sessions, artifacts, corrections,
policy invalidation, and gaps.

### 16.3 Retention invariant

> A durable watermark may exist only when the observations to which it refers were durably
> retained and are still present.

Consequences:

~~~text
EPHEMERAL
-> no durable historical watermark

TTL
-> valid only over unexpired, present coverage

SUBSCRIPTION_BOUND
-> valid only while exact dataset rights remain active
-> invalidate before purge on termination

DURABLE_AUTHORIZED
-> durable watermark allowed

SYNTHETIC_UNRESTRICTED
-> watermark allowed in isolated tests/demo
~~~

If an artifact disappears, expires, is quarantined, or loses policy permission, coverage and the
watermark become invalid before any query or update uses them.

### 16.4 Gap taxonomy

- **Acquisition gap:** a bounded interval/page was never fully acquired; blocks the frontier.
- **Integrity gap:** a referenced raw/canonical artifact is missing or fails checksum; blocks and
  invalidates coverage.
- **Expected-observation finding:** an eligible calendar slot lacks a bar; initially unresolved.
- **Verified sparse/no-trade slot:** only after complete request and pagination plus sufficiently
  demonstrated provider semantics support omission when no eligible trade exists; represented as a
  durable coverage fact, not a fabricated bar. An absent bar alone remains unresolved.
- **Correction/revision finding:** repeated provider evidence changes a canonical value; creates
  same-provider revision and reconciliation state.
- **Calendar-stale coverage:** a calendar version changed expectations; requires verification.

An exchange calendar describes eligible slots. It does not prove that a security traded. The ORLY
gaps observed in Phase 1 demonstrate why every absent 5m bar must not automatically be labeled
provider failure.

## 17. Ingestion modes

All three modes first intersect desired work with the active dataset-policy eligibility interval.
For Alpaca historical SIP, no bounded request may include an observation whose exclusive end has
not passed the strict >15-minute gate and finalization buffer.

### 17.1 Backfill

Backfill computes:

~~~text
desired bounded interval
- retained VERIFIED coverage
= missing request intervals
~~~

It:

- partitions by provider point/page limits, sessions, instruments, and hard byte/call/cost budgets;
- estimates work before network access;
- caps every partition at the policy-eligible end, including historical windows near the present;
- records every bounded request before execution;
- preserves successful segments after partial failure;
- resumes only missing or retry-eligible requests;
- reports progress as verified requests/coverage, not merely received rows;
- stops the watermark at the first blocking gap.

One request may contain multiple instruments, but coverage is verified and committed per stream.

### 17.2 Incremental update

Update requires a valid retention-aware watermark or directs the operator to backfill/verify.

It:

- starts at the contiguous exclusive frontier;
- derives safe end from the maintained calendar, completed bar/session semantics, provider
  historical-delay rules, and a conservative finalization buffer;
- requests only missing closed intervals;
- returns a successful no-op before provider construction when safe_end is not after start;
- does not redownload the full history;
- does not perform broad correction scans implicitly.

A second identical update must create no new raw artifact, canonical batch, revision, or watermark
change and should ordinarily make no provider call.

### 17.3 Repair and reconciliation

Repair is triggered by:

- acquisition or integrity gaps;
- unresolved expected-slot findings;
- explicit correction windows;
- calendar-version changes;
- manual operator request;
- missing/corrupt artifact verification.

It:

- plans a bounded replacement/reconciliation window;
- applies the same policy age ceiling as backfill and update;
- first replays complete, checksum-verified, policy-valid retained raw pages when canonical output
  or verification is missing/stale, without a provider call;
- fetches new provider evidence only when raw acquisition is incomplete/missing or an explicit
  correction refresh requests a new version;
- retains any newly fetched raw provider evidence;
- compares observation identities and value hashes;
- treats an exact semantic replay as a no-op;
- publishes changed same-provider revisions immutably;
- preserves old and new provenance;
- reclassifies gaps and recalculates coverage/frontier.

Expired, purged, corrupt, or no-longer-permitted raw cannot be replayed. Coverage stays invalid and
a provider repair is planned only if the current policy independently permits a new request.

No automatic winner is chosen between providers. A future curated cross-provider policy is outside
Phase 2.

## 18. Rate limits, budgets, retries, and progress

Provider budgets are first-class request-planning constraints:

- maximum calls and points per bounded request;
- rolling/minute and daily limits;
- estimated bytes and optional cost;
- observed safe rate-limit headers;
- hard per-run and per-request ceilings.

Budget reservation and observations are durable in SQLite so restart does not forget recent local
use. Provider claims and account headers remain dataset/endpoint-specific.

Retry policy is explicit:

- retry only transport failures, selected 5xx responses, and 429 responses;
- honor a valid Retry-After;
- use bounded exponential backoff with jitter and a durable next-eligible time;
- never retry policy denial, bad credentials, entitlement denial, malformed request, integrity
  collision, or fatal validation automatically;
- never sleep while holding a SQLite transaction;
- stop at configured attempt/budget limits and retain a sanitized error.

The initial implementation remains sequential or conservatively single-writer. Concurrency is not
required to make the store living.

## 19. Manual CLI and external scheduling

The offline checkpoint implements a small standard-library command-line entry point with these
command families:

~~~text
investment-platform data-root init
investment-platform backfill
investment-platform update
investment-platform repair
investment-platform status
investment-platform verify
investment-platform retention enforce
~~~

Commands are bounded, idempotent, restartable, and return stable nonzero exit codes for invalid
configuration, incomplete work, failed work, and failed verification. status and verify expose
sanitized counts, intervals, gaps, state, and IDs without market-data values or credentials.

update accepts all configuration from the explicit profile, process environment, policy catalog,
and non-secret arguments. Scheduler command templates are documented in the
[operator guide](../operations/living-ingestion.md), but no schedule is activated. Only after
manual live acceptance may the same command be invoked by Windows Task Scheduler, cron, or another
external scheduler.

Phase 2 does not introduce Celery, Redis, Kafka, a daemon, a queue, a cloud orchestrator, or an
internal scheduler.

## 20. Provider and rollout scope

The provider architecture remains dataset-specific, not one global winner:

- Alpaca historical SIP is primary for Phase 2 US 1d/5m bars.
- Twelve Data remains secondary/reference and a future international candidate. Its standard US
  intraday feed is not a consolidated-volume source.
- Databento remains an options and reference/corporate-action candidate; OPRA is ephemeral pending
  dataset-specific retention clarification, and Reference is not entitled.
- Massive remains prohibited for the intended Individual non-display workflow.
- yfinance remains sanity/reference only.

The controlled live progression is:

1. synthetic offline tests;
2. one security and a small historical window;
3. the frozen Phase 1 sample of 16 securities;
4. a deliberately bounded larger subset;
5. full current S&P 500 only after all prior acceptance gates.

No real download occurred in the offline checkpoint. Controlled live acceptance must estimate and
confirm each request before expanding scope.

## 21. Acceptance design

The [sanitized acceptance record](../operations/phase-2-acceptance.md) distinguishes the implemented
offline checkpoint from the still-pending controlled live progression.

### 21.1 Environment isolation

- test, ci, and demo cannot construct a live provider or open the private root;
- CI requires no network or credentials;
- development real-data persistence fails;
- private_research without a safe initialized root fails;
- repository scans prove no real/private/licensed file is tracked.

### 21.2 Backfill and restart

- a small Alpaca SIP history produces durable raw and verified canonical batch directories;
- SQLite records dataset, request, artifacts, coverage, watermark, run, and errors;
- process/computer restart preserves and verifies all state;
- request partition progress resumes without repeating verified work.

### 21.3 Incremental update

- only the interval after the valid frontier is requested;
- a second identical update is a provider-call-free meaningful no-op;
- exact semantic observations do not duplicate;
- a changed provider value becomes an explicit same-provider revision.

### 21.4 Crash safety

Fault injection proves:

- crash before raw publication creates no durable coverage;
- crash before canonical publication never advances a watermark;
- partial staging is never query-visible;
- crash after final directory rename and before SQLite commit is adopted;
- crash during SQLite commit leaves coverage/watermark atomic;
- crash after commit completes by status reconciliation without a duplicate batch.

### 21.5 Repair

- a simulated acquisition/integrity gap is detected;
- a bounded repair is planned;
- a raw-only canonical loss is rebuilt from retained verified raw without network access;
- new raw provenance is retained;
- unchanged observations are no-ops and changed observations are revisions;
- blocking gaps close and contiguous coverage/frontier return.

### 21.6 Retention

- unknown and PROHIBITED datasets fail before network access;
- the Alpaca >15-minute eligibility boundary is enforced for backfill, update, repair, and
  unexpected provider overfetch;
- EPHEMERAL creates no durable historical watermark;
- DURABLE_AUTHORIZED permits only private scoped persistence;
- TTL expiry and subscription termination invalidate query visibility, coverage, and watermark
  before purge;
- purge resumes safely after an injected crash.

### 21.7 Calendar and quality

- holidays, early close, and DST cases match frozen official expectations;
- Friday-to-Monday, holiday, and daily multi-session coverage cross only calendar-closed time;
- eligible empty 5m slots are neither automatically provider errors nor `VERIFIED_EMPTY`; the
  complete-request, complete-pagination, and provider-semantics prerequisites are enforced;
- all normal tests remain offline and deterministic;
- formatting, lint, typing, tests, build, diff, Git-ignore, tracked-data, and secret scans pass.

## 22. Implementation milestones and current status

Milestones are internal ordering within Phase 2, not new official phases. Items 1–10 are
implemented and exercised offline with synthetic data; item 11 is pending, and item 12 is
documented but deliberately not activated:

1. environment capability matrix and configuration hierarchy;
2. external root initialization, sentinel, path guards, and safe staging;
3. machine-readable retention catalog and enforcement boundary;
4. SQLite schema/versioning, repositories, leases, and restart tests;
5. maintained trading-calendar adapter and versioned snapshots;
6. stream/request/artifact/batch/observation identities and bounded planner;
7. atomic canonical batch publication, catalog-driven queries, recovery, and fault injection;
8. manual synthetic and one-security backfill;
9. incremental update, no-op, status, and verify;
10. repair/revision handling and retention invalidation/purge;
11. **Pending:** progressive 1 -> 16 -> larger-subset live acceptance;
12. **Documentation only:** external scheduler invocation after manual reliability.

PLAN_PHASE_2.md owns the detailed deliverables and gates.

## 23. Open questions

There are no unresolved architectural questions that block starting the narrow Alpaca SIP
implementation.

The following are pre-live gates, not design blockers:

- choose and initialize the actual external private-root path;
- place the complete ticket evidence at the documented private location;
- calibrate Alpaca request partition size and conservative finalization buffer with the
  one-security acceptance;
- activate no Twelve Data durable stream until its exact dataset policy and subscription status
  are recorded.

The corporate-action provider, cross-provider winner policy, options, full S&P 500 rollout, and
future feature inputs are deliberately non-blocking because they are outside the initial Phase 2
acceptance.

## 24. Scope guard

Phase 2 does not implement:

- feature calculation, return/volatility analytics, or Market State;
- dashboard, checkpoints, alerts, or news/events;
- AI agents or interpretation;
- strategies, backtesting engine, or broker execution;
- options surfaces, multi-asset ingestion, or corporate-action master;
- PostgreSQL, cloud deployment, microservices, queues, or multi-user SaaS;
- data redistribution or public market-data display.

The only backtesting reference in the Alpaca rights record describes an authorized use of retained
data. It does not authorize a backtesting engine in Phase 2.

## 25. Readiness verdict

**OFFLINE IMPLEMENTATION READY FOR CONTROLLED LIVE ACCEPTANCE**

The approved design has been implemented through its synthetic/offline gates without expanding
into later analytical or product phases. The dependency gate and offline safety contracts are
resolved. Phase 2 is not complete: a safe initialized external root with the actual private
evidence, controlled Alpaca SIP acceptance, final quality review, pull request, and CI confirmation
are still required. This is not authorization to start Phase 3.

## 26. Learning notes

1. **Parquet and SQLite solve different problems.** Parquet is efficient immutable analytical
   history; SQLite coordinates small mutable facts such as what ran and what is verified.
2. **A database transaction cannot include a filesystem rename.** Safety comes from ordering,
   immutable artifacts, verification, catalog visibility, and recovery, not from pretending the
   two stores share one commit.
3. **A watermark is a verified claim, not the last timestamp seen.** It says every required step
   before an exclusive frontier is accounted for and still retained.
4. **Coverage is richer than a watermark.** Coverage records holes, stale verification, policy
   validity, and the evidence supporting each interval. The watermark is only a cached frontier.
5. **At-least-once execution is normal.** Networks and process crashes repeat attempts. Stable
   identities and idempotent effects turn repetitions into no-ops instead of corrupt data.
6. **Artifact and semantic idempotency differ.** Identical bytes are one artifact; equivalent bars
   can still arrive in differently paged responses and need observation-level comparison.
7. **A correction is not a duplicate.** Preserve immutable versions and same-provider provenance,
   then expose an explicit current view.
8. **A calendar predicts eligible sessions, not trades.** An empty intraday slot may be legitimate
   for a trade-aggregated feed, but absence alone proves nothing. `VERIFIED_EMPTY` additionally
   requires a complete request, complete valid pagination, and demonstrated provider semantics.
9. **Retention controls state truth.** A watermark pointing to expired or deleted data is worse
   than no watermark because it tells the updater to skip history it no longer owns.
10. **External scheduling should invoke a reliable command.** Once update is manual, bounded,
    restartable, and idempotent, Task Scheduler or cron is a thin trigger rather than a new
    architecture.
