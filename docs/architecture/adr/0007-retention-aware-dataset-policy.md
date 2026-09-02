# ADR 0007: Retention-aware policy for each provider dataset

- **Status:** Accepted
- **Date:** 2026-08-31
- **Phase:** 2 — Living Data Ingestion
- **Implementation status:** Implemented in Phase 2

## Context

`LicenseClassification` describes the provenance class carried by data, such as private or
synthetic. It does not answer whether a precise provider dataset may be requested, transformed,
retained in raw or normalized form, displayed, redistributed, or kept after subscription
termination. Treating a provider name or successful API call as permission would make the pipeline
unsafe and would let durable state outlive the data it purports to describe.

Phase 2 needs executable policy without becoming a contract-management system. The safe unit is an
exact `provider × dataset` entry backed by dated evidence, not a provider-wide assumption.

## Decision

Add a versioned, machine-readable dataset-policy catalog that is independent of
`LicenseClassification`. Lookup uses an exact, stable provider and dataset identifier. Aliases,
wildcards, and inheritance cannot broaden permission. An absent, ambiguous, expired, or
insufficiently specific entry is denied.

The supported retention modes are:

- `PROHIBITED`: deny substantive request, processing, persistence, and downstream use;
- `EPHEMERAL`: permit only the authorized private temporary processing, followed by verified
  cleanup; no durable historical coverage or watermark is created;
- `TTL`: permit configured layers only until a mandatory expiry time or grace period;
- `SUBSCRIPTION_BOUND`: permit configured layers only while the named entitlement and license state
  remain active, with termination and grace-period rules applied;
- `DURABLE_AUTHORIZED`: permit the configured private layers without a subscription-termination
  deletion requirement, subject to the recorded restrictions; and
- `SYNTHETIC_UNRESTRICTED`: permit synthetic or explicitly redistributable fixtures within their
  recorded provenance and public-use scope.

Each entry records, at minimum:

- provider, exact dataset, policy version, effective status, and processing permission;
- separate raw, normalized, and derived retention modes, including any TTL or grace period;
- request-eligibility constraints, including minimum observation age when the evidence requires
  one;
- whether termination requires deletion and which retained layers it affects;
- public-display and redistribution permissions, both false unless affirmatively granted;
- an evidence reference, verification date, scope/caveats, and non-secret notes; and
- the dataset or entitlement condition required for the entry to remain effective.

One top-level label may summarize an entry only when all relevant layers share that mode. Layer
rules remain authoritative. A grant for normalized OHLCV does not imply permission for raw payloads,
and a derived-data grant does not imply public display or redistribution.

The initial catalog will encode these narrow decisions rather than provider-wide defaults:

- **Alpaca historical SIP US stock bars — `DURABLE_AUTHORIZED`:** private historical API
  responses, normalized OHLCV/Parquet, and authorized private research uses, only for observations
  strictly older than 15 minutes.
- **Twelve Data entitled internal datasets — `SUBSCRIPTION_BOUND`:** exact dataset and entitlement
  rules; no general provider grant.
- **Databento OPRA — `EPHEMERAL`:** temporary authorized evaluation pending dataset-specific
  retention clarification.
- **Massive Individual `price_bars` and `corporate_actions` — `PROHIBITED`:** the two current
  adapter dataset IDs are explicitly denied for the intended non-display processing/persistence
  workflow; other unlisted Massive datasets fail closed by absence.
- **Synthetic and approved sample data — `SYNTHETIC_UNRESTRICTED`:** only declared synthetic or
  redistributable artifacts.

Historical Alpaca options and crypto are `UNVERIFIED / PENDING` because the recorded evidence does
not cover them; neither has an active policy entry. `UNVERIFIED` describes the evidence state and
`PENDING` the inactive policy state—neither is a retention mode or grant. Real-time data, news, and
every other unlisted Alpaca dataset also have no active entry and therefore fail closed. Permission
is never inherited from historical SIP stock bars.

The pipeline will enforce policy before a provider request, raw or canonical write, replay,
publication, query, export, and destructive lifecycle action. A run records the policy version it
used. Runtime entitlement checks cannot silently relax the catalog; the stricter result wins.

For Alpaca historical SIP bars, the catalog records `minimum_observation_age=PT15M`. Every
backfill, update, and repair request is capped so all possible bar ends are strictly older than that
threshold, plus any conservative finalization buffer. Unexpected younger or out-of-bound data is
not durably persisted or quarantined and produces only a sanitized policy failure.

Expiration, termination, revocation, or a restrictive policy change schedules or requires purge of
the affected layers. Until purge completes, affected artifacts are not queryable. The same action
invalidates or recomputes verified batches, coverage, gaps, and watermarks whose supporting data is
removed. Purge outcome and policy status are durable operational records.

## Consequences

- Retention and use restrictions become testable pipeline behavior rather than comments.
- A dataset may be technically reachable while remaining unavailable to ingestion.
- Policy updates are auditable and cannot retroactively masquerade as the policy used by an older
  run.
- Layer-specific rules add configuration and lifecycle tests, but avoid both excessive deletion and
  unauthorized retention.
- The catalog records engineering decisions and evidence references; it is not legal advice or a
  replacement for reviewing provider-specific terms.

## Alternatives considered

- **Reuse `LicenseClassification`:** rejected because provenance visibility and retention rights are
  independent dimensions.
- **Provider-wide allow/deny flags:** rejected because rights vary by dataset, entitlement, layer,
  and time.
- **Documentation-only policy:** rejected because it cannot gate requests, writes, queries, or
  purge-driven state invalidation.
- **A general legal-rules engine:** rejected as unnecessary for a local single-user platform; a
  small versioned catalog and explicit lifecycle rules are sufficient.
