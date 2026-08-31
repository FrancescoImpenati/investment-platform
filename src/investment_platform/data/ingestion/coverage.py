"""Pure Phase 2 coverage, gap, and retention-aware watermark domain.

Coverage is the source of truth.  This module deliberately has no filesystem or
SQLite dependencies: operational repositories may project verified catalog
rows into these immutable values, run the deterministic calculation, and then
persist its result in the same post-publication transaction.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.calendar import CalendarSnapshot, ExpectedCalendarSlot
from investment_platform.data.ingestion.identity import (
    BarSemantics,
    DataKind,
    IdentityDimension,
    StreamKey,
)
from investment_platform.data.ingestion.planner import (
    CoverageClassification,
    CoverageVerificationState,
)
from investment_platform.data.market_time import to_utc
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.retention import (
    DatasetPolicyStatus,
    DatasetRetentionPolicy,
    DatasetRuntimeStatus,
    RetentionMode,
)

_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,191}$"
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_PLATFORM_SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"


class CoverageDomainError(RuntimeError):
    """Raised when inputs cannot support an unambiguous frontier claim."""


class GapTransitionError(CoverageDomainError):
    """Raised when a gap lifecycle transition is invalid."""


class GapType(StrEnum):
    """Phase 2 gap/finding taxonomy, aligned with the operational schema."""

    ACQUISITION = "ACQUISITION"
    INTEGRITY = "INTEGRITY"
    EXPECTED_OBSERVATION = "EXPECTED_OBSERVATION"
    CORRECTION = "CORRECTION"
    CALENDAR_STALE = "CALENDAR_STALE"


class GapStatus(StrEnum):
    """Durable gap lifecycle states."""

    OPEN = "OPEN"
    REPAIRING = "REPAIRING"
    RESOLVED = "RESOLVED"
    INVALIDATED = "INVALIDATED"


class CoverageInvalidationReason(StrEnum):
    """Reasons a previously verified coverage claim may no longer be used."""

    ARTIFACT_MISSING = "ARTIFACT_MISSING"
    ARTIFACT_CORRUPT = "ARTIFACT_CORRUPT"
    CALENDAR_CHANGED = "CALENDAR_CHANGED"
    EXPIRED = "EXPIRED"
    POLICY_REVOKED = "POLICY_REVOKED"
    PURGED = "PURGED"
    QUARANTINED = "QUARANTINED"


class WatermarkPolicyReason(StrEnum):
    """Fail-closed durable-watermark policy assessment."""

    ELIGIBLE = "ELIGIBLE"
    POLICY_KEY_MISMATCH = "POLICY_KEY_MISMATCH"
    POLICY_INACTIVE = "POLICY_INACTIVE"
    PROCESSING_PROHIBITED = "PROCESSING_PROHIBITED"
    DATASET_PROHIBITED = "DATASET_PROHIBITED"
    DATASET_EPHEMERAL = "DATASET_EPHEMERAL"
    NORMALIZED_NOT_DURABLE = "NORMALIZED_NOT_DURABLE"
    RUNTIME_STATUS_REQUIRED = "RUNTIME_STATUS_REQUIRED"
    RUNTIME_STATUS_MISMATCH = "RUNTIME_STATUS_MISMATCH"
    RUNTIME_STATUS_DISABLED = "RUNTIME_STATUS_DISABLED"
    RUNTIME_STATUS_EXPIRED = "RUNTIME_STATUS_EXPIRED"
    TTL_STATUS_INCOMPLETE = "TTL_STATUS_INCOMPLETE"
    TTL_START_IN_FUTURE = "TTL_START_IN_FUTURE"
    TTL_EXPANDS_POLICY = "TTL_EXPANDS_POLICY"
    SUBSCRIPTION_INACTIVE = "SUBSCRIPTION_INACTIVE"


class MissingSlotReason(StrEnum):
    """Why an eligible slot could not extend the contiguous frontier."""

    NO_VALID_COVERAGE = "NO_VALID_COVERAGE"
    BLOCKING_GAP = "BLOCKING_GAP"


class CoverageRequestTerminalState(StrEnum):
    """Terminal request outcomes relevant to independently verified coverage."""

    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"


class CoverageStreamOutcome(StrEnum):
    """Per-stream outcome used when a multi-stream request is PARTIAL."""

    PUBLISHABLE = "PUBLISHABLE"
    BLOCKED = "BLOCKED"


class _FrozenCoverageModel(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class VerifiedEmptySemantics(_FrozenCoverageModel):
    """One exact provider omission semantic approved for VERIFIED_EMPTY."""

    provider: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,63}$")]
    dataset: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")]
    data_kind: DataKind
    timeframe: Timeframe
    session: TradingSession
    adjustment: AdjustmentState
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
    bar_semantics: BarSemantics
    additional_dimensions: tuple[IdentityDimension, ...] = ()
    version: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")]


class VerifiedEmptySemanticsRegistry(_FrozenCoverageModel):
    """Exact, deliberately small allow-list for durable empty-slot claims."""

    entries: tuple[VerifiedEmptySemantics, ...] = ()

    @model_validator(mode="after")
    def require_unique_entries(self) -> Self:
        keys = tuple(
            (
                entry.provider,
                entry.dataset,
                entry.data_kind,
                entry.timeframe,
                entry.session,
                entry.adjustment,
                entry.currency,
                entry.bar_semantics,
                tuple(
                    (dimension.name, dimension.value) for dimension in entry.additional_dimensions
                ),
                entry.version,
            )
            for entry in self.entries
        )
        if len(keys) != len(set(keys)):
            raise ValueError("VERIFIED_EMPTY semantics registry contains duplicate entries")
        return self

    def approves(self, stream: StreamKey, version: str | None) -> bool:
        """Return whether one exact stream dataset/version has demonstrated semantics."""

        return version is not None and any(
            (
                entry.provider,
                entry.dataset,
                entry.data_kind,
                entry.timeframe,
                entry.session,
                entry.adjustment,
                entry.currency,
                entry.bar_semantics,
                entry.additional_dimensions,
                entry.version,
            )
            == (
                stream.provider,
                stream.dataset,
                stream.data_kind,
                stream.timeframe,
                stream.session,
                stream.adjustment,
                stream.currency,
                stream.bar_semantics,
                stream.additional_dimensions,
                version,
            )
            for entry in self.entries
        )


PHASE2_VERIFIED_EMPTY_SEMANTICS = VerifiedEmptySemanticsRegistry(
    entries=tuple(
        VerifiedEmptySemantics(
            provider="synthetic",
            dataset="price_bars",
            data_kind=DataKind.PRICE_BAR,
            timeframe=timeframe,
            session=TradingSession.REGULAR,
            adjustment=AdjustmentState.UNADJUSTED,
            currency="USD",
            bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
            version="synthetic-complete-pagination-v1",
        )
        for timeframe in (Timeframe.ONE_DAY, Timeframe.FIVE_MINUTES)
    )
)


class CoverageSegment(_FrozenCoverageModel):
    """A catalog-projected coverage fact over complete calendar slots.

    ``OBSERVED`` segments contain exactly one canonical observation per covered
    eligible slot.  Empty slots are represented separately as ``VERIFIED_EMPTY``
    facts and require complete acquisition plus an approved provider semantic.
    """

    coverage_id: str = Field(pattern=_ID_PATTERN)
    stream_id: str = Field(pattern=_ID_PATTERN)
    canonical_batch_id: str = Field(pattern=_ID_PATTERN)
    calendar_snapshot_id: str = Field(pattern=_ID_PATTERN)
    calendar_snapshot_checksum: str = Field(pattern=_PLATFORM_SHA256_PATTERN)
    policy_snapshot_id: str = Field(pattern=_ID_PATTERN)
    policy_id: str = Field(pattern=_ID_PATTERN)
    policy_revision: Annotated[int, Field(gt=0)]
    policy_hash: str = Field(pattern=_SHA256_PATTERN)
    coverage_start: datetime
    start: datetime
    end: datetime
    classification: CoverageClassification
    verification_state: CoverageVerificationState
    retained: bool
    row_count: Annotated[int, Field(ge=0)]
    artifact_count: Annotated[int, Field(gt=0)]
    artifacts_present: bool
    artifact_integrity_verified: bool
    interval_verified: bool
    request_completed: bool
    request_terminal_state: CoverageRequestTerminalState
    stream_outcome: CoverageStreamOutcome
    pagination_verified: bool
    terminal_page_verified: bool
    canonical_batch_verified: bool
    canonical_file_count: Annotated[int, Field(ge=0)]
    raw_artifact_count: Annotated[int, Field(ge=0)]
    relational_provenance_verified: bool
    provider_semantics_version: str | None = None
    generation: Annotated[int, Field(gt=0)]
    verified_at: datetime
    invalidated_at: datetime | None = None

    @field_validator(
        "coverage_start",
        "start",
        "end",
        "verified_at",
        "invalidated_at",
        mode="after",
    )
    @classmethod
    def normalize_datetimes(cls, value: datetime | None) -> datetime | None:
        return None if value is None else to_utc(value)

    @model_validator(mode="after")
    def validate_fact(self) -> Self:
        if self.end <= self.start:
            raise ValueError("coverage end must be later than start")
        if self.coverage_start > self.start:
            raise ValueError("coverage_start cannot be later than segment start")
        if self.verified_at < self.end:
            raise ValueError("coverage cannot be verified before its interval completes")
        if self.classification is CoverageClassification.OBSERVED:
            if self.row_count <= 0:
                raise ValueError("OBSERVED coverage requires canonical rows")
            if self.provider_semantics_version is not None:
                raise ValueError("provider omission semantics apply only to VERIFIED_EMPTY")
        else:
            if self.row_count != 0:
                raise ValueError("VERIFIED_EMPTY coverage cannot contain a canonical row")
            if not (
                self.request_completed
                and self.request_terminal_state
                in {CoverageRequestTerminalState.SUCCESS, CoverageRequestTerminalState.PARTIAL}
                and self.stream_outcome is CoverageStreamOutcome.PUBLISHABLE
                and self.pagination_verified
                and self.terminal_page_verified
                and self.interval_verified
                and self.canonical_batch_verified
                and self.canonical_file_count > 0
                and self.raw_artifact_count > 0
                and self.relational_provenance_verified
                and self.provider_semantics_version
            ):
                raise ValueError(
                    "VERIFIED_EMPTY requires complete request, pagination, interval, and "
                    "provider semantics proof"
                )
        if self.verification_state is CoverageVerificationState.INVALID:
            if self.invalidated_at is None:
                raise ValueError("INVALID coverage requires invalidated_at")
        elif self.verification_state is CoverageVerificationState.STALE:
            if self.invalidated_at is None:
                raise ValueError("STALE coverage requires invalidated_at")
        elif self.invalidated_at is not None:
            raise ValueError("VERIFIED coverage cannot have invalidated_at")
        if self.invalidated_at is not None and self.invalidated_at < self.verified_at:
            raise ValueError("coverage invalidation cannot precede verification")
        return self


class GapFinding(_FrozenCoverageModel):
    """One durable gap/finding and its lifecycle state."""

    gap_id: str = Field(pattern=_ID_PATTERN)
    stream_id: str = Field(pattern=_ID_PATTERN)
    start: datetime
    end: datetime
    gap_type: GapType
    status: GapStatus
    blocking: bool
    detected_at: datetime
    resolved_at: datetime | None = None
    request_instance_id: str | None = Field(default=None, pattern=_ID_PATTERN)
    canonical_batch_id: str | None = Field(default=None, pattern=_ID_PATTERN)

    @field_validator("start", "end", "detected_at", "resolved_at", mode="after")
    @classmethod
    def normalize_datetimes(cls, value: datetime | None) -> datetime | None:
        return None if value is None else to_utc(value)

    @model_validator(mode="after")
    def validate_finding(self) -> Self:
        if self.end <= self.start:
            raise ValueError("gap end must be later than start")
        terminal = self.status in {GapStatus.RESOLVED, GapStatus.INVALIDATED}
        if terminal != (self.resolved_at is not None):
            raise ValueError("terminal gap state and resolved_at must be set together")
        if self.resolved_at is not None and self.resolved_at < self.detected_at:
            raise ValueError("resolved_at cannot precede detected_at")
        if (
            self.gap_type
            in {
                GapType.ACQUISITION,
                GapType.INTEGRITY,
                GapType.EXPECTED_OBSERVATION,
                GapType.CALENDAR_STALE,
            }
            and not self.blocking
        ):
            raise ValueError(f"{self.gap_type.value} gaps must block the frontier")
        return self

    @property
    def actively_blocks(self) -> bool:
        return self.blocking and self.status in {GapStatus.OPEN, GapStatus.REPAIRING}


class CoverageInvalidation(_FrozenCoverageModel):
    """Explicit invalidation request applied before query/update use or purge."""

    coverage_ids: Annotated[tuple[str, ...], Field(min_length=1)]
    reason: CoverageInvalidationReason
    invalidated_at: datetime

    @field_validator("coverage_ids", mode="after")
    @classmethod
    def unique_coverage_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("coverage invalidation contains duplicate IDs")
        return tuple(sorted(value))

    @field_validator("invalidated_at", mode="after")
    @classmethod
    def normalize_invalidated_at(cls, value: datetime) -> datetime:
        return to_utc(value)


class WatermarkPolicyEligibility(_FrozenCoverageModel):
    """Result of the pure durable-retention policy assessment."""

    eligible: bool
    reason: WatermarkPolicyReason
    valid_until: datetime | None = None
    observation_eligible_before: datetime | None = None

    @field_validator("valid_until", "observation_eligible_before", mode="after")
    @classmethod
    def normalize_valid_until(cls, value: datetime | None) -> datetime | None:
        return None if value is None else to_utc(value)

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        if self.eligible != (self.reason is WatermarkPolicyReason.ELIGIBLE):
            raise ValueError("watermark eligibility and reason disagree")
        if self.eligible != (self.observation_eligible_before is not None):
            raise ValueError("eligible policy must expose its strict observation-age boundary")
        return self


class NotApplicableInterval(_FrozenCoverageModel):
    """A wall-clock interval proven exchange-closed by the snapshot."""

    start: datetime
    end: datetime

    @field_validator("start", "end", mode="after")
    @classmethod
    def normalize_bounds(cls, value: datetime) -> datetime:
        return to_utc(value)

    @model_validator(mode="after")
    def validate_interval(self) -> Self:
        if self.end <= self.start:
            raise ValueError("NOT_APPLICABLE interval end must be later than start")
        return self


class UncoveredEligibleSlot(_FrozenCoverageModel):
    """An eligible calendar slot that cannot currently support the frontier."""

    slot: ExpectedCalendarSlot
    reason: MissingSlotReason
    blocking_gap_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_reason(self) -> Self:
        if self.reason is MissingSlotReason.BLOCKING_GAP and not self.blocking_gap_ids:
            raise ValueError("blocking slot requires at least one gap identity")
        if self.reason is MissingSlotReason.NO_VALID_COVERAGE and self.blocking_gap_ids:
            raise ValueError("uncovered slot without a known gap cannot name gap identities")
        return self


class WatermarkCandidate(_FrozenCoverageModel):
    """Reconstructed watermark fields before operational generation context is added."""

    stream_id: str = Field(pattern=_ID_PATTERN)
    coverage_start: datetime
    exclusive_frontier: datetime
    verification_state: CoverageVerificationState
    calendar_snapshot_id: str = Field(pattern=_ID_PATTERN)
    policy_snapshot_id: str = Field(pattern=_ID_PATTERN)
    last_verified_session: date
    blocking_gap_count: Annotated[int, Field(ge=0)]
    supporting_coverage_ids: Annotated[tuple[str, ...], Field(min_length=1)]
    supporting_batch_ids: Annotated[tuple[str, ...], Field(min_length=1)]
    evidence_verified_at: datetime
    policy_evaluated_at: datetime
    policy_valid_until: datetime | None = None
    observation_eligible_before: datetime

    @field_validator(
        "coverage_start",
        "exclusive_frontier",
        "evidence_verified_at",
        "policy_evaluated_at",
        "policy_valid_until",
        "observation_eligible_before",
        mode="after",
    )
    @classmethod
    def normalize_bounds(cls, value: datetime | None) -> datetime | None:
        return None if value is None else to_utc(value)

    @model_validator(mode="after")
    def validate_candidate(self) -> Self:
        if self.exclusive_frontier <= self.coverage_start:
            raise ValueError("watermark frontier must advance beyond coverage_start")
        if self.verification_state is not CoverageVerificationState.VERIFIED:
            raise ValueError("only a VERIFIED frontier may be materialized")
        if len(self.supporting_coverage_ids) != len(set(self.supporting_coverage_ids)):
            raise ValueError("watermark supporting coverage IDs must be unique")
        if len(self.supporting_batch_ids) != len(set(self.supporting_batch_ids)):
            raise ValueError("watermark supporting batch IDs must be unique")
        if self.evidence_verified_at > self.policy_evaluated_at:
            raise ValueError("watermark policy assessment predates supporting evidence")
        if (
            self.policy_valid_until is not None
            and self.policy_evaluated_at >= self.policy_valid_until
        ):
            raise ValueError("watermark candidate policy is already expired")
        return self


class WatermarkChangeContext(_FrozenCoverageModel):
    """Operational provenance attached only after a candidate is reconstructed."""

    generation: Annotated[int, Field(gt=0)]
    last_run_id: str = Field(pattern=_ID_PATTERN)
    last_batch_id: str = Field(pattern=_ID_PATTERN)
    computed_at: datetime

    @field_validator("computed_at", mode="after")
    @classmethod
    def normalize_computed_at(cls, value: datetime) -> datetime:
        return to_utc(value)


class MaterializedWatermark(_FrozenCoverageModel):
    """Complete durable watermark DTO matching the approved operational contract."""

    stream_id: str = Field(pattern=_ID_PATTERN)
    coverage_start: datetime
    exclusive_frontier: datetime
    verification_state: CoverageVerificationState
    generation: Annotated[int, Field(gt=0)]
    calendar_snapshot_id: str = Field(pattern=_ID_PATTERN)
    policy_snapshot_id: str = Field(pattern=_ID_PATTERN)
    last_run_id: str = Field(pattern=_ID_PATTERN)
    last_batch_id: str = Field(pattern=_ID_PATTERN)
    last_verified_session: date
    blocking_gap_count: Annotated[int, Field(ge=0)]
    computed_at: datetime
    invalidated_at: datetime | None = None

    @field_validator(
        "coverage_start",
        "exclusive_frontier",
        "computed_at",
        "invalidated_at",
        mode="after",
    )
    @classmethod
    def normalize_datetimes(cls, value: datetime | None) -> datetime | None:
        return None if value is None else to_utc(value)

    @model_validator(mode="after")
    def validate_watermark(self) -> Self:
        if self.exclusive_frontier <= self.coverage_start:
            raise ValueError("watermark frontier must advance beyond coverage_start")
        if self.verification_state is CoverageVerificationState.VERIFIED:
            if self.invalidated_at is not None:
                raise ValueError("a VERIFIED watermark cannot have invalidated_at")
        elif self.invalidated_at is None:
            raise ValueError("a STALE or INVALID watermark requires invalidated_at")
        return self


class FrontierEvaluation(_FrozenCoverageModel):
    """Deterministic diagnostic and candidate result for one exact stream."""

    stream_id: str = Field(pattern=_ID_PATTERN)
    coverage_start: datetime
    domain_end: datetime
    policy_eligibility: WatermarkPolicyEligibility
    eligible_slot_count: Annotated[int, Field(ge=0)]
    contiguous_slot_count: Annotated[int, Field(ge=0)]
    exclusive_frontier: datetime
    last_verified_session: date | None
    not_applicable: tuple[NotApplicableInterval, ...]
    uncovered_slots: tuple[UncoveredEligibleSlot, ...]
    active_blocking_gaps: tuple[GapFinding, ...]
    invalid_coverage_ids: tuple[str, ...]
    supporting_coverage_ids: tuple[str, ...]
    candidate: WatermarkCandidate | None

    @field_validator("coverage_start", "domain_end", "exclusive_frontier", mode="after")
    @classmethod
    def normalize_datetimes(cls, value: datetime) -> datetime:
        return to_utc(value)

    @model_validator(mode="after")
    def validate_evaluation(self) -> Self:
        if self.domain_end <= self.coverage_start:
            raise ValueError("frontier domain end must be later than start")
        if not self.coverage_start <= self.exclusive_frontier <= self.domain_end:
            raise ValueError("exclusive frontier is outside the evaluated domain")
        if self.contiguous_slot_count > self.eligible_slot_count:
            raise ValueError("contiguous slot count exceeds eligible domain")
        if self.candidate is not None:
            if not self.policy_eligibility.eligible or self.contiguous_slot_count == 0:
                raise ValueError("watermark candidate lacks durable contiguous coverage")
            if self.candidate.exclusive_frontier != self.exclusive_frontier:
                raise ValueError("candidate and evaluation frontiers disagree")
        return self


def assess_watermark_policy(
    stream: StreamKey,
    policy: DatasetRetentionPolicy,
    *,
    runtime_status: DatasetRuntimeStatus | None,
    evaluated_at: datetime,
) -> WatermarkPolicyEligibility:
    """Assess current durable retention without expanding policy permissions."""

    now = to_utc(evaluated_at)
    observation_eligible_before = now - timedelta(
        seconds=(policy.minimum_observation_age_seconds + policy.finalization_buffer_seconds)
    )
    if (stream.provider, stream.dataset) != (policy.provider, policy.dataset):
        return _ineligible(WatermarkPolicyReason.POLICY_KEY_MISMATCH)
    if policy.status is not DatasetPolicyStatus.ACTIVE:
        return _ineligible(WatermarkPolicyReason.POLICY_INACTIVE)
    if policy.mode is RetentionMode.PROHIBITED:
        return _ineligible(WatermarkPolicyReason.DATASET_PROHIBITED)
    if not policy.processing_allowed:
        return _ineligible(WatermarkPolicyReason.PROCESSING_PROHIBITED)
    if policy.mode is RetentionMode.EPHEMERAL:
        return _ineligible(WatermarkPolicyReason.DATASET_EPHEMERAL)
    if policy.normalized.mode in {RetentionMode.PROHIBITED, RetentionMode.EPHEMERAL}:
        return _ineligible(WatermarkPolicyReason.NORMALIZED_NOT_DURABLE)

    if runtime_status is None:
        if policy.mode in {RetentionMode.TTL, RetentionMode.SUBSCRIPTION_BOUND}:
            return _ineligible(WatermarkPolicyReason.RUNTIME_STATUS_REQUIRED)
        return _eligible(observation_eligible_before=observation_eligible_before)

    expected_identity = (
        policy.provider,
        policy.dataset,
        policy.policy_id,
        policy.revision,
        policy.content_hash,
    )
    actual_identity = (
        runtime_status.provider,
        runtime_status.dataset,
        runtime_status.policy_id,
        runtime_status.policy_revision,
        runtime_status.policy_hash,
    )
    if actual_identity != expected_identity:
        return _ineligible(WatermarkPolicyReason.RUNTIME_STATUS_MISMATCH)
    if not runtime_status.enabled:
        return _ineligible(WatermarkPolicyReason.RUNTIME_STATUS_DISABLED)
    if runtime_status.expires_at is not None and now >= runtime_status.expires_at:
        return _ineligible(WatermarkPolicyReason.RUNTIME_STATUS_EXPIRED)

    if policy.mode is RetentionMode.TTL:
        if runtime_status.retention_started_at is None or runtime_status.expires_at is None:
            return _ineligible(WatermarkPolicyReason.TTL_STATUS_INCOMPLETE)
        if runtime_status.retention_started_at > now:
            return _ineligible(WatermarkPolicyReason.TTL_START_IN_FUTURE)
        ttl_values = tuple(
            rule.ttl_seconds
            for rule in (policy.raw, policy.normalized, policy.derived)
            if rule.mode is RetentionMode.TTL and rule.ttl_seconds is not None
        )
        maximum_expiry = runtime_status.retention_started_at + timedelta(seconds=min(ttl_values))
        if runtime_status.expires_at > maximum_expiry:
            return _ineligible(WatermarkPolicyReason.TTL_EXPANDS_POLICY)
    if (
        policy.mode is RetentionMode.SUBSCRIPTION_BOUND
        and runtime_status.entitlement_active is not True
    ):
        return _ineligible(WatermarkPolicyReason.SUBSCRIPTION_INACTIVE)
    return _eligible(
        valid_until=runtime_status.expires_at,
        observation_eligible_before=observation_eligible_before,
    )


def reconstruct_frontier(
    *,
    stream: StreamKey,
    calendar_snapshot: CalendarSnapshot,
    calendar_snapshot_id: str,
    policy: DatasetRetentionPolicy,
    policy_snapshot_id: str,
    runtime_status: DatasetRuntimeStatus | None,
    coverage_start: datetime,
    domain_end: datetime,
    coverage: Sequence[CoverageSegment],
    gaps: Sequence[GapFinding] = (),
    evaluated_at: datetime,
    verified_empty_semantics: VerifiedEmptySemanticsRegistry = (PHASE2_VERIFIED_EMPTY_SEMANTICS),
) -> FrontierEvaluation:
    """Reconstruct the contiguous frontier over complete eligible XNYS RTH slots.

    Exchange-closed wall-clock intervals are returned as ``NOT_APPLICABLE`` and
    never become gaps.  A candidate exists only after at least one eligible slot
    is backed by currently retained, verified, policy-valid evidence.
    """

    start = to_utc(coverage_start)
    end = to_utc(domain_end)
    now = to_utc(evaluated_at)
    if end <= start:
        raise CoverageDomainError("frontier domain end must be later than start")
    _validate_supported_stream(stream, calendar_snapshot)
    _validate_snapshot_range(calendar_snapshot, start, end)

    slots = _domain_slots(calendar_snapshot, stream.timeframe, start, end)
    not_applicable = _not_applicable_intervals(start, end, slots)
    policy_eligibility = assess_watermark_policy(
        stream,
        policy,
        runtime_status=runtime_status,
        evaluated_at=now,
    )

    stream_id = stream.stream_id
    coverage_ids = tuple(segment.coverage_id for segment in coverage)
    if len(coverage_ids) != len(set(coverage_ids)):
        raise CoverageDomainError("coverage projection contains duplicate identities")
    gap_ids = tuple(gap.gap_id for gap in gaps)
    if len(gap_ids) != len(set(gap_ids)):
        raise CoverageDomainError("gap projection contains duplicate identities")
    for segment in coverage:
        if segment.stream_id != stream_id:
            raise CoverageDomainError("coverage contains a different stream identity")
        if segment.coverage_start != start:
            raise CoverageDomainError(
                "coverage projection rebases the authoritative stream coverage_start"
            )
    for gap in gaps:
        if gap.stream_id != stream_id:
            raise CoverageDomainError("gaps contain a different stream identity")

    intersecting_segments = tuple(
        segment for segment in coverage if _intersects(segment.start, segment.end, start, end)
    )
    valid_segments: list[CoverageSegment] = []
    invalid_ids: set[str] = set()
    for segment in intersecting_segments:
        if _segment_is_valid(
            segment,
            stream=stream,
            calendar_snapshot=calendar_snapshot,
            calendar_snapshot_id=calendar_snapshot_id,
            policy=policy,
            policy_snapshot_id=policy_snapshot_id,
            policy_eligible=policy_eligibility.eligible,
            semantics=verified_empty_semantics,
            evaluated_at=now,
        ):
            _validate_segment_slot_claim(segment, calendar_snapshot, stream.timeframe)
            valid_segments.append(segment)
        else:
            invalid_ids.add(segment.coverage_id)

    active_gaps = tuple(
        sorted(
            (
                gap
                for gap in gaps
                if gap.actively_blocks
                and any(
                    _intersects(gap.start, gap.end, slot.start_utc, slot.end_utc) for slot in slots
                )
            ),
            key=lambda gap: (gap.start, gap.end, gap.gap_id),
        )
    )

    uncovered: list[UncoveredEligibleSlot] = []
    covered_support: list[tuple[ExpectedCalendarSlot, tuple[CoverageSegment, ...]]] = []
    first_missing_index: int | None = None
    for index, slot in enumerate(slots):
        blocking_ids = tuple(
            gap.gap_id
            for gap in active_gaps
            if _intersects(gap.start, gap.end, slot.start_utc, slot.end_utc)
        )
        supports = tuple(
            sorted(
                (
                    segment
                    for segment in valid_segments
                    if segment.start <= slot.start_utc and segment.end >= slot.end_utc
                ),
                key=lambda segment: (segment.verified_at, segment.coverage_id),
            )
        )
        classifications = {segment.classification for segment in supports}
        if len(classifications) > 1:
            raise CoverageDomainError(
                "one eligible slot has conflicting OBSERVED and VERIFIED_EMPTY coverage"
            )
        if blocking_ids:
            uncovered.append(
                UncoveredEligibleSlot(
                    slot=slot,
                    reason=MissingSlotReason.BLOCKING_GAP,
                    blocking_gap_ids=blocking_ids,
                )
            )
            if first_missing_index is None:
                first_missing_index = index
        elif not supports:
            uncovered.append(
                UncoveredEligibleSlot(
                    slot=slot,
                    reason=MissingSlotReason.NO_VALID_COVERAGE,
                )
            )
            if first_missing_index is None:
                first_missing_index = index
        else:
            covered_support.append((slot, supports))

    contiguous_count = len(slots) if first_missing_index is None else first_missing_index
    contiguous_support = covered_support[:contiguous_count]
    if contiguous_count == 0:
        frontier = slots[0].start_utc if slots else end
        last_session: date | None = None
    else:
        last_slot = slots[contiguous_count - 1]
        last_session = last_slot.session_date
        frontier = slots[contiguous_count].start_utc if contiguous_count < len(slots) else end

    supporting_coverage_ids = tuple(
        sorted({segment.coverage_id for _, supports in contiguous_support for segment in supports})
    )
    supporting_batch_ids = tuple(
        sorted(
            {
                segment.canonical_batch_id
                for _, supports in contiguous_support
                for segment in supports
            }
        )
    )
    candidate = None
    if policy_eligibility.eligible and contiguous_count > 0 and last_session is not None:
        eligible_before = policy_eligibility.observation_eligible_before
        if eligible_before is None:
            raise CoverageDomainError("eligible policy lacks an observation-age boundary")
        candidate = WatermarkCandidate(
            stream_id=stream_id,
            coverage_start=start,
            exclusive_frontier=frontier,
            verification_state=CoverageVerificationState.VERIFIED,
            calendar_snapshot_id=calendar_snapshot_id,
            policy_snapshot_id=policy_snapshot_id,
            last_verified_session=last_session,
            blocking_gap_count=len(active_gaps),
            supporting_coverage_ids=supporting_coverage_ids,
            supporting_batch_ids=supporting_batch_ids,
            evidence_verified_at=max(
                segment.verified_at for _, supports in contiguous_support for segment in supports
            ),
            policy_evaluated_at=now,
            policy_valid_until=policy_eligibility.valid_until,
            observation_eligible_before=eligible_before,
        )

    return FrontierEvaluation(
        stream_id=stream_id,
        coverage_start=start,
        domain_end=end,
        policy_eligibility=policy_eligibility,
        eligible_slot_count=len(slots),
        contiguous_slot_count=contiguous_count,
        exclusive_frontier=frontier,
        last_verified_session=last_session,
        not_applicable=not_applicable,
        uncovered_slots=tuple(uncovered),
        active_blocking_gaps=active_gaps,
        invalid_coverage_ids=tuple(sorted(invalid_ids)),
        supporting_coverage_ids=supporting_coverage_ids,
        candidate=candidate,
    )


def materialize_watermark(
    candidate: WatermarkCandidate,
    context: WatermarkChangeContext,
) -> MaterializedWatermark:
    """Attach transaction provenance to an already reconstructed candidate."""

    if context.last_batch_id not in candidate.supporting_batch_ids:
        raise CoverageDomainError("last-changing batch does not support the reconstructed frontier")
    if context.computed_at < candidate.policy_evaluated_at:
        raise CoverageDomainError("watermark materialization predates frontier verification")
    if (
        candidate.policy_valid_until is not None
        and context.computed_at >= candidate.policy_valid_until
    ):
        raise CoverageDomainError("watermark policy expired before materialization")
    return MaterializedWatermark(
        stream_id=candidate.stream_id,
        coverage_start=candidate.coverage_start,
        exclusive_frontier=candidate.exclusive_frontier,
        verification_state=candidate.verification_state,
        generation=context.generation,
        calendar_snapshot_id=candidate.calendar_snapshot_id,
        policy_snapshot_id=candidate.policy_snapshot_id,
        last_run_id=context.last_run_id,
        last_batch_id=context.last_batch_id,
        last_verified_session=candidate.last_verified_session,
        blocking_gap_count=candidate.blocking_gap_count,
        computed_at=context.computed_at,
    )


def invalidate_materialized_watermark(
    watermark: MaterializedWatermark,
    *,
    reason: CoverageInvalidationReason,
    generation: int,
    last_run_id: str,
    invalidated_at: datetime,
) -> MaterializedWatermark:
    """Make a retained watermark unusable before purge/deletion or re-verification."""

    changed_at = to_utc(invalidated_at)
    if generation <= watermark.generation:
        raise CoverageDomainError("watermark invalidation generation must advance")
    if changed_at < watermark.computed_at:
        raise CoverageDomainError("watermark invalidation predates its materialization")
    state = CoverageVerificationState.INVALID
    if (
        watermark.verification_state is not CoverageVerificationState.INVALID
        and reason is CoverageInvalidationReason.CALENDAR_CHANGED
    ):
        state = CoverageVerificationState.STALE
    return MaterializedWatermark(
        stream_id=watermark.stream_id,
        coverage_start=watermark.coverage_start,
        exclusive_frontier=watermark.exclusive_frontier,
        verification_state=state,
        generation=generation,
        calendar_snapshot_id=watermark.calendar_snapshot_id,
        policy_snapshot_id=watermark.policy_snapshot_id,
        last_run_id=last_run_id,
        last_batch_id=watermark.last_batch_id,
        last_verified_session=watermark.last_verified_session,
        blocking_gap_count=watermark.blocking_gap_count,
        computed_at=changed_at,
        invalidated_at=watermark.invalidated_at or changed_at,
    )


def transition_gap(
    gap: GapFinding,
    target: GapStatus,
    *,
    transitioned_at: datetime,
) -> GapFinding:
    """Apply an explicit, validated gap lifecycle transition."""

    if target is gap.status:
        return gap
    allowed = {
        GapStatus.OPEN: {GapStatus.REPAIRING, GapStatus.RESOLVED, GapStatus.INVALIDATED},
        GapStatus.REPAIRING: {GapStatus.OPEN, GapStatus.RESOLVED, GapStatus.INVALIDATED},
        GapStatus.RESOLVED: set(),
        GapStatus.INVALIDATED: set(),
    }
    if target not in allowed[gap.status]:
        raise GapTransitionError(f"gap cannot transition from {gap.status} to {target}")
    changed_at = to_utc(transitioned_at)
    if changed_at < gap.detected_at:
        raise GapTransitionError("gap transition cannot precede detection")
    terminal = target in {GapStatus.RESOLVED, GapStatus.INVALIDATED}
    return GapFinding(
        gap_id=gap.gap_id,
        stream_id=gap.stream_id,
        start=gap.start,
        end=gap.end,
        gap_type=gap.gap_type,
        status=target,
        blocking=gap.blocking,
        detected_at=gap.detected_at,
        resolved_at=changed_at if terminal else None,
        request_instance_id=gap.request_instance_id,
        canonical_batch_id=gap.canonical_batch_id,
    )


def invalidate_coverage(
    coverage: Sequence[CoverageSegment],
    invalidation: CoverageInvalidation,
) -> tuple[CoverageSegment, ...]:
    """Invalidate exact coverage IDs before purge, expiry, or unsafe reuse."""

    requested = set(invalidation.coverage_ids)
    present = {segment.coverage_id for segment in coverage}
    missing = requested - present
    if missing:
        raise CoverageDomainError(
            f"coverage invalidation references unknown IDs: {', '.join(sorted(missing))}"
        )
    stale = invalidation.reason is CoverageInvalidationReason.CALENDAR_CHANGED
    # State invalidation precedes physical purge/quarantine.  Only an integrity
    # observation that the artifact is already missing may change presence here.
    removes_artifact = invalidation.reason is CoverageInvalidationReason.ARTIFACT_MISSING
    corrupts_artifact = invalidation.reason is CoverageInvalidationReason.ARTIFACT_CORRUPT
    invalidates_retention = invalidation.reason in {
        CoverageInvalidationReason.EXPIRED,
        CoverageInvalidationReason.POLICY_REVOKED,
        CoverageInvalidationReason.PURGED,
        CoverageInvalidationReason.QUARANTINED,
    }
    result: list[CoverageSegment] = []
    for segment in coverage:
        if segment.coverage_id not in requested:
            result.append(segment)
            continue
        if invalidation.invalidated_at < segment.verified_at or (
            segment.invalidated_at is not None
            and invalidation.invalidated_at < segment.invalidated_at
        ):
            raise CoverageDomainError("coverage invalidation timestamp regresses audit history")
        result.append(
            CoverageSegment(
                coverage_id=segment.coverage_id,
                stream_id=segment.stream_id,
                canonical_batch_id=segment.canonical_batch_id,
                calendar_snapshot_id=segment.calendar_snapshot_id,
                calendar_snapshot_checksum=segment.calendar_snapshot_checksum,
                policy_snapshot_id=segment.policy_snapshot_id,
                policy_id=segment.policy_id,
                policy_revision=segment.policy_revision,
                policy_hash=segment.policy_hash,
                coverage_start=segment.coverage_start,
                start=segment.start,
                end=segment.end,
                classification=segment.classification,
                verification_state=(
                    segment.verification_state
                    if segment.verification_state is CoverageVerificationState.INVALID
                    else (
                        CoverageVerificationState.STALE
                        if stale
                        else CoverageVerificationState.INVALID
                    )
                ),
                retained=segment.retained and not invalidates_retention,
                row_count=segment.row_count,
                artifact_count=segment.artifact_count,
                artifacts_present=segment.artifacts_present and not removes_artifact,
                artifact_integrity_verified=(
                    segment.artifact_integrity_verified
                    and not removes_artifact
                    and not corrupts_artifact
                ),
                interval_verified=segment.interval_verified,
                request_completed=segment.request_completed,
                request_terminal_state=segment.request_terminal_state,
                stream_outcome=segment.stream_outcome,
                pagination_verified=segment.pagination_verified,
                terminal_page_verified=segment.terminal_page_verified,
                canonical_batch_verified=segment.canonical_batch_verified,
                canonical_file_count=segment.canonical_file_count,
                raw_artifact_count=segment.raw_artifact_count,
                relational_provenance_verified=segment.relational_provenance_verified,
                provider_semantics_version=segment.provider_semantics_version,
                generation=segment.generation,
                verified_at=segment.verified_at,
                invalidated_at=segment.invalidated_at or invalidation.invalidated_at,
            )
        )
    return tuple(result)


def _validate_supported_stream(stream: StreamKey, snapshot: CalendarSnapshot) -> None:
    if stream.timeframe not in {Timeframe.ONE_DAY, Timeframe.FIVE_MINUTES}:
        raise CoverageDomainError("Phase 2 coverage supports only 1d and 5m streams")
    if stream.session is not TradingSession.REGULAR:
        raise CoverageDomainError("Phase 2 coverage supports only regular-session streams")
    if snapshot.calendar_name != "XNYS":
        raise CoverageDomainError("Phase 2 US-equity coverage requires an XNYS snapshot")


def _validate_snapshot_range(snapshot: CalendarSnapshot, start: datetime, end: datetime) -> None:
    try:
        timezone = ZoneInfo(snapshot.timezone_name)
    except ZoneInfoNotFoundError as error:
        raise CoverageDomainError("calendar snapshot timezone is unavailable") from error
    first_date = start.astimezone(timezone).date()
    last_date = (end - timedelta(microseconds=1)).astimezone(timezone).date()
    if first_date < snapshot.range_start or last_date >= snapshot.range_end:
        raise CoverageDomainError("calendar snapshot does not cover the complete frontier domain")


def _domain_slots(
    snapshot: CalendarSnapshot,
    timeframe: Timeframe,
    start: datetime,
    end: datetime,
) -> tuple[ExpectedCalendarSlot, ...]:
    result: list[ExpectedCalendarSlot] = []
    for slot in snapshot.expected_slots(timeframe):
        if not _intersects(slot.start_utc, slot.end_utc, start, end):
            continue
        if slot.start_utc < start or slot.end_utc > end:
            raise CoverageDomainError("frontier bounds cut through an eligible calendar slot")
        result.append(slot)
    return tuple(result)


def _not_applicable_intervals(
    start: datetime,
    end: datetime,
    slots: Sequence[ExpectedCalendarSlot],
) -> tuple[NotApplicableInterval, ...]:
    result: list[NotApplicableInterval] = []
    cursor = start
    for slot in slots:
        if slot.start_utc > cursor:
            result.append(NotApplicableInterval(start=cursor, end=slot.start_utc))
        cursor = slot.end_utc
    if cursor < end:
        result.append(NotApplicableInterval(start=cursor, end=end))
    return tuple(result)


def _segment_is_valid(
    segment: CoverageSegment,
    *,
    stream: StreamKey,
    calendar_snapshot: CalendarSnapshot,
    calendar_snapshot_id: str,
    policy: DatasetRetentionPolicy,
    policy_snapshot_id: str,
    policy_eligible: bool,
    semantics: VerifiedEmptySemanticsRegistry,
    evaluated_at: datetime,
) -> bool:
    eligible_before = evaluated_at - timedelta(
        seconds=(policy.minimum_observation_age_seconds + policy.finalization_buffer_seconds)
    )
    return (
        policy_eligible
        and segment.verification_state is CoverageVerificationState.VERIFIED
        and segment.retained
        and segment.artifacts_present
        and segment.artifact_integrity_verified
        and segment.interval_verified
        and segment.request_completed
        and segment.request_terminal_state
        in {CoverageRequestTerminalState.SUCCESS, CoverageRequestTerminalState.PARTIAL}
        and segment.stream_outcome is CoverageStreamOutcome.PUBLISHABLE
        and segment.pagination_verified
        and segment.terminal_page_verified
        and segment.canonical_batch_verified
        and segment.canonical_file_count > 0
        and segment.raw_artifact_count > 0
        and segment.relational_provenance_verified
        and segment.verified_at <= evaluated_at
        and segment.end < eligible_before
        and segment.calendar_snapshot_id == calendar_snapshot_id
        and segment.calendar_snapshot_checksum == calendar_snapshot.checksum
        and segment.policy_snapshot_id == policy_snapshot_id
        and segment.policy_id == policy.policy_id
        and segment.policy_revision == policy.revision
        and segment.policy_hash == policy.content_hash
        and (
            segment.classification is not CoverageClassification.VERIFIED_EMPTY
            or semantics.approves(stream, segment.provider_semantics_version)
        )
    )


def _validate_segment_slot_claim(
    segment: CoverageSegment,
    snapshot: CalendarSnapshot,
    timeframe: Timeframe,
) -> None:
    slots = tuple(
        slot
        for slot in snapshot.expected_slots(timeframe)
        if _intersects(segment.start, segment.end, slot.start_utc, slot.end_utc)
    )
    if not slots:
        raise CoverageDomainError("coverage segment contains no eligible calendar slot")
    if segment.start != slots[0].start_utc or segment.end != slots[-1].end_utc:
        raise CoverageDomainError("coverage segment bounds must align to complete eligible slots")
    if segment.classification is CoverageClassification.OBSERVED and segment.row_count != len(
        slots
    ):
        raise CoverageDomainError(
            "OBSERVED coverage must contain exactly one canonical row per eligible slot"
        )


def _ineligible(reason: WatermarkPolicyReason) -> WatermarkPolicyEligibility:
    return WatermarkPolicyEligibility(eligible=False, reason=reason)


def _eligible(
    *,
    observation_eligible_before: datetime,
    valid_until: datetime | None = None,
) -> WatermarkPolicyEligibility:
    return WatermarkPolicyEligibility(
        eligible=True,
        reason=WatermarkPolicyReason.ELIGIBLE,
        valid_until=valid_until,
        observation_eligible_before=observation_eligible_before,
    )


def _intersects(
    left_start: datetime,
    left_end: datetime,
    right_start: datetime,
    right_end: datetime,
) -> bool:
    return left_start < right_end and left_end > right_start


__all__ = [
    "PHASE2_VERIFIED_EMPTY_SEMANTICS",
    "CoverageDomainError",
    "CoverageInvalidation",
    "CoverageInvalidationReason",
    "CoverageRequestTerminalState",
    "CoverageSegment",
    "CoverageStreamOutcome",
    "FrontierEvaluation",
    "GapFinding",
    "GapStatus",
    "GapTransitionError",
    "GapType",
    "MaterializedWatermark",
    "MissingSlotReason",
    "NotApplicableInterval",
    "UncoveredEligibleSlot",
    "VerifiedEmptySemantics",
    "VerifiedEmptySemanticsRegistry",
    "WatermarkCandidate",
    "WatermarkChangeContext",
    "WatermarkPolicyEligibility",
    "WatermarkPolicyReason",
    "assess_watermark_policy",
    "invalidate_coverage",
    "invalidate_materialized_watermark",
    "materialize_watermark",
    "reconstruct_frontier",
    "transition_gap",
]
