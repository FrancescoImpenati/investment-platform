"""Deterministic, retention-aware planning for bounded ingestion requests.

The planner is intentionally provider-neutral and side-effect free.  It turns a
calendar-defined eligible domain into stable :class:`RequestSpecification`
values; persistence, attempts, retries, and provider dispatch remain separate
operational concerns.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Final, Self
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.calendar import CalendarSnapshot, ExpectedCalendarSlot
from investment_platform.data.ingestion.identity import (
    ProviderInstrumentMapping,
    RequestSpecification,
    StreamKey,
)
from investment_platform.data.market_time import to_utc
from investment_platform.data.models import Timeframe
from investment_platform.data.retention import (
    DatasetRuntimeStatus,
    PlanningPolicyAuthorization,
    RequestPolicyAuthorization,
    RetentionPolicyEnforcer,
)
from investment_platform.runtime import RuntimeEnvironment

_PLATFORM_SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_DURABLE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,191}$"
_SENSITIVE_TEXT = re.compile(
    r"(?:[a-z][a-z0-9+.-]*://|(?:authorization|api[_-]?key|password|secret|token)\s*[:=])",
    re.IGNORECASE,
)

# An empty provider response is not evidence by itself.  This deliberately small,
# exact registry represents semantics proved by offline fixtures only.  Alpaca has
# no approved VERIFIED_EMPTY semantics entry in Phase 2.
_VERIFIED_EMPTY_SEMANTICS: Final[dict[tuple[str, str], frozenset[str]]] = {
    ("synthetic", "price_bars"): frozenset({"synthetic-complete-pagination-v1"}),
}


class PlanningError(RuntimeError):
    """Raised when safe, deterministic bounded work cannot be formulated."""


class BudgetExceeded(PlanningError):
    """Raised before dispatch when a complete pending plan exceeds a hard ceiling."""

    def __init__(self, ceiling: str, estimated: int | Decimal, allowed: int | Decimal) -> None:
        self.ceiling = ceiling
        self.estimated = estimated
        self.allowed = allowed
        super().__init__(f"planning budget exceeded for {ceiling}: {estimated} > {allowed}")


class IngestionIntent(StrEnum):
    """Provider-neutral reasons for planning ingestion work."""

    BACKFILL = "BACKFILL"
    UPDATE = "UPDATE"
    REPAIR = "REPAIR"


class AcquisitionStrategy(StrEnum):
    """How a plan obtains evidence; only bounded network acquisition exists here."""

    NETWORK = "NETWORK"


class RepairStrategy(StrEnum):
    """Provider-neutral semantics for an explicitly bounded repair."""

    MISSING_ONLY = "MISSING_ONLY"
    PROVIDER_REFRESH = "PROVIDER_REFRESH"
    RAW_REPLAY = "RAW_REPLAY"


class CoverageClassification(StrEnum):
    """Facts that may satisfy a calendar-eligible observation slot."""

    OBSERVED = "OBSERVED"
    VERIFIED_EMPTY = "VERIFIED_EMPTY"


class CoverageVerificationState(StrEnum):
    """Verification state of a coverage fact."""

    VERIFIED = "VERIFIED"
    STALE = "STALE"
    INVALID = "INVALID"


class _FrozenPlannerModel(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class VerifiedCoverageProjection(_FrozenPlannerModel):
    """A repository-verified projection of durable coverage provenance.

    The object is a projection of relational joins and file-presence checks, not a
    cryptographic token.  The operational repository is the production trust
    boundary.  All proof fields are explicit so a stale policy, absent artifact,
    incomplete request, or unverifiable empty interval remains missing work.
    """

    coverage_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    request_instance_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    canonical_batch_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    policy_snapshot_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    active_policy_snapshot_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    calendar_snapshot_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    stream: StreamKey
    start: datetime
    end: datetime
    classification: CoverageClassification
    verification_state: CoverageVerificationState
    retained: bool
    policy_valid: bool
    policy_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    policy_revision: Annotated[int, Field(gt=0)]
    policy_hash: str = Field(pattern=_SHA256_PATTERN)
    active_policy_hash: str = Field(pattern=_SHA256_PATTERN)
    calendar_snapshot_checksum: str = Field(pattern=_PLATFORM_SHA256_PATTERN)
    relational_provenance_verified: bool
    interval_verified: bool
    request_completed: bool
    pagination_verified: bool
    canonical_batch_verified: bool
    canonical_file_count: Annotated[int, Field(ge=0)]
    raw_artifact_count: Annotated[int, Field(ge=0)]
    artifacts_present: bool
    provider_semantics_version: str | None = None

    @field_validator("start", "end", mode="after")
    @classmethod
    def normalize_bounds(cls, value: datetime) -> datetime:
        return to_utc(value)

    @model_validator(mode="after")
    def validate_coverage_proof(self) -> Self:
        if self.end <= self.start:
            raise ValueError("coverage end must be later than start")
        if self.classification is CoverageClassification.VERIFIED_EMPTY:
            if not (
                self.request_completed
                and self.pagination_verified
                and self.interval_verified
                and self.relational_provenance_verified
            ):
                raise ValueError(
                    "VERIFIED_EMPTY requires a completed request, verified pagination, "
                    "interval, and relational provenance"
                )
            approved = _VERIFIED_EMPTY_SEMANTICS.get(
                (self.stream.provider, self.stream.dataset), frozenset()
            )
            if self.provider_semantics_version not in approved:
                raise ValueError(
                    "VERIFIED_EMPTY requires an exact provider/dataset semantics registry entry"
                )
        elif self.provider_semantics_version is not None:
            raise ValueError("provider semantics version is valid only for VERIFIED_EMPTY")
        return self

    def is_subtractable(
        self,
        snapshot: CalendarSnapshot,
        authorization: PlanningPolicyAuthorization,
    ) -> bool:
        """Return whether this fact is eligible to remove planned work."""

        policy = authorization.policy_snapshot
        return (
            self.retained
            and self.policy_valid
            and self.verification_state is CoverageVerificationState.VERIFIED
            and (self.stream.provider, self.stream.dataset) == (policy.provider, policy.dataset)
            and self.policy_id == policy.policy_id
            and self.policy_revision == policy.policy_revision
            and self.policy_hash == policy.policy_hash
            and self.active_policy_snapshot_id == self.policy_snapshot_id
            and self.active_policy_hash == self.policy_hash
            and self.calendar_snapshot_checksum == snapshot.checksum
            and self.relational_provenance_verified
            and self.interval_verified
            and self.request_completed
            and self.pagination_verified
            and self.canonical_batch_verified
            and self.canonical_file_count > 0
            and self.raw_artifact_count > 0
            and self.artifacts_present
            and (
                self.classification is not CoverageClassification.VERIFIED_EMPTY
                or self.provider_semantics_version
                in _VERIFIED_EMPTY_SEMANTICS.get(
                    (self.stream.provider, self.stream.dataset), frozenset()
                )
            )
        )


class PlannerLimits(_FrozenPlannerModel):
    """Provider/endpoint limits used to formulate each bounded request."""

    max_instruments_per_request: Annotated[int, Field(gt=0)]
    max_expected_observations_per_request: Annotated[int, Field(gt=0)]
    max_observations_per_page: Annotated[int, Field(gt=0)]
    max_pages_per_request: Annotated[int, Field(gt=0)]
    max_calls_per_request: Annotated[int, Field(gt=0)]
    max_estimated_bytes_per_request: Annotated[int, Field(gt=0)]
    estimated_bytes_per_observation: Annotated[int, Field(gt=0)]
    estimated_bytes_per_page: Annotated[int, Field(ge=0)] = 0
    estimated_cost_per_call: Annotated[Decimal, Field(ge=0)] = Decimal(0)
    max_estimated_cost_per_request: Annotated[Decimal, Field(ge=0)] | None = None


class PlannerBudget(_FrozenPlannerModel):
    """Hard ceilings for all pending network work in one plan."""

    max_calls: Annotated[int, Field(ge=0)]
    max_expected_observations: Annotated[int, Field(ge=0)]
    max_pages: Annotated[int, Field(ge=0)]
    max_estimated_bytes: Annotated[int, Field(ge=0)]
    max_estimated_cost: Annotated[Decimal, Field(ge=0)] | None = None


class PlannedRequest(_FrozenPlannerModel):
    """One policy-authorized request ready to be persisted before dispatch."""

    specification: RequestSpecification
    authorization: RequestPolicyAuthorization
    expected_slots: tuple[ExpectedCalendarSlot, ...]
    expected_observations: Annotated[int, Field(gt=0)]
    estimated_pages: Annotated[int, Field(gt=0)]
    estimated_calls: Annotated[int, Field(gt=0)]
    estimated_bytes: Annotated[int, Field(gt=0)]
    estimated_cost: Annotated[Decimal, Field(ge=0)]

    @model_validator(mode="after")
    def validate_estimate(self) -> Self:
        expected = len(self.expected_slots) * len(self.specification.instrument_mappings)
        if self.expected_observations != expected:
            raise ValueError("expected_observations does not match slots x instruments")
        if not self.expected_slots:
            raise ValueError("a planned request requires at least one expected slot")
        slot_bounds = tuple((slot.start_utc, slot.end_utc) for slot in self.expected_slots)
        if slot_bounds != tuple(sorted(set(slot_bounds))):
            raise ValueError("expected slots must be unique and canonically ordered")
        if any(slot.timeframe is not self.specification.timeframe for slot in self.expected_slots):
            raise ValueError("expected slot timeframe does not match request specification")
        if self.specification.start != self.expected_slots[0].start_utc:
            raise ValueError("request start does not match its first expected slot")
        if self.specification.end != self.expected_slots[-1].end_utc:
            raise ValueError("request end does not match its last expected slot")
        if self.authorization.request_start != self.specification.start or (
            self.authorization.request_end != self.specification.end
        ):
            raise ValueError("request authorization does not match the specification bounds")
        if self.authorization.request_spec_hash != self.specification.request_spec_hash:
            raise ValueError("request authorization does not match the specification identity")
        return self

    @property
    def request_spec_hash(self) -> str:
        return self.specification.request_spec_hash


class IngestionPlan(_FrozenPlannerModel):
    """A complete, deterministic planning result with preflighted pending work."""

    intent: IngestionIntent
    provider: str
    dataset: str
    environment: RuntimeEnvironment
    policy_authorization: PlanningPolicyAuthorization
    acquisition_strategy: AcquisitionStrategy
    repair_strategy: RepairStrategy | None = None
    repair_reason: Annotated[str, Field(min_length=1, max_length=256)] | None = None
    desired_start: datetime
    desired_end: datetime
    safe_end: datetime
    calendar_snapshot_checksum: str = Field(pattern=_PLATFORM_SHA256_PATTERN)
    streams: tuple[StreamKey, ...]
    stream_ids: tuple[str, ...]
    eligible_slot_count: Annotated[int, Field(ge=0)]
    eligible_observation_count: Annotated[int, Field(ge=0)]
    missing_observation_count: Annotated[int, Field(ge=0)]
    pending_observation_count: Annotated[int, Field(ge=0)]
    requests: tuple[PlannedRequest, ...]
    estimated_pages: Annotated[int, Field(ge=0)]
    estimated_calls: Annotated[int, Field(ge=0)]
    estimated_bytes: Annotated[int, Field(ge=0)]
    estimated_cost: Annotated[Decimal, Field(ge=0)]

    @field_validator("desired_start", "desired_end", "safe_end", mode="after")
    @classmethod
    def normalize_bounds(cls, value: datetime) -> datetime:
        return to_utc(value)

    @field_validator("repair_reason", mode="after")
    @classmethod
    def reject_sensitive_repair_reason(cls, value: str | None) -> str | None:
        if value is not None and ("\r" in value or "\n" in value or _SENSITIVE_TEXT.search(value)):
            raise ValueError("repair reason contains a URL, secret-shaped text, or line break")
        return value

    @model_validator(mode="after")
    def validate_totals(self) -> Self:
        if self.desired_end <= self.desired_start:
            raise ValueError("desired end must be later than start")
        if self.intent is IngestionIntent.REPAIR:
            if self.repair_strategy is None or self.repair_reason is None:
                raise ValueError("repair plans require an explicit strategy and reason")
            if self.repair_strategy is RepairStrategy.RAW_REPLAY:
                raise ValueError("raw replay is not a network ingestion plan")
        elif self.repair_strategy is not None or self.repair_reason is not None:
            raise ValueError("repair strategy and reason are valid only for REPAIR")
        if self.acquisition_strategy is not AcquisitionStrategy.NETWORK:
            raise ValueError("this planner currently persists only bounded network acquisition")
        expected_stream_ids = tuple(stream.stream_id for stream in self.streams)
        if not self.streams or expected_stream_ids != self.stream_ids:
            raise ValueError("streams and ordered stream_ids must match exactly")
        if self.stream_ids != tuple(sorted(set(self.stream_ids))):
            raise ValueError("plan streams must have unique deterministic order")
        if any(
            (stream.provider, stream.dataset) != (self.provider, self.dataset)
            for stream in self.streams
        ):
            raise ValueError("every plan stream must match the exact provider/dataset")
        policy_snapshot = self.policy_authorization.policy_snapshot
        if (
            policy_snapshot.provider,
            policy_snapshot.dataset,
            self.policy_authorization.environment,
        ) != (self.provider, self.dataset, self.environment):
            raise ValueError("planning policy authorization does not match the plan scope")
        if self.safe_end != self.policy_authorization.eligible_before:
            raise ValueError("safe_end must equal the frozen planning policy frontier")
        if any(
            request.authorization.policy_snapshot != policy_snapshot
            or request.authorization.environment is not self.environment
            or request.authorization.eligible_before != self.safe_end
            or request.authorization.authorized_at != self.policy_authorization.authorized_at
            for request in self.requests
        ):
            raise ValueError("every bounded request must share the frozen planning authorization")
        request_hashes = tuple(request.request_spec_hash for request in self.requests)
        if len(request_hashes) != len(set(request_hashes)):
            raise ValueError("bounded request specifications must be unique within a plan")
        if self.requests != tuple(sorted(self.requests, key=_planned_request_sort_key)):
            raise ValueError("bounded requests must use canonical deterministic order")
        stream_by_id = {stream.stream_id: stream for stream in self.streams}
        planned_stream_slots: set[tuple[str, datetime, datetime]] = set()
        for request in self.requests:
            specification = request.specification
            if (specification.provider, specification.dataset) != (
                self.provider,
                self.dataset,
            ):
                raise ValueError("bounded request provider/dataset differs from the plan")
            request_streams = specification.stream_keys()
            if any(
                stream.stream_id not in stream_by_id or stream_by_id[stream.stream_id] != stream
                for stream in request_streams
            ):
                raise ValueError("bounded request stream dimensions differ from plan streams")
            if (
                specification.start < self.desired_start
                or specification.end > self.desired_end
                or specification.end >= self.safe_end
            ):
                raise ValueError("bounded request lies outside desired or policy-safe bounds")
            for stream in request_streams:
                for slot in request.expected_slots:
                    key = (stream.stream_id, slot.start_utc, slot.end_utc)
                    if key in planned_stream_slots:
                        raise ValueError("same stream/slot appears in multiple bounded requests")
                    planned_stream_slots.add(key)
        if self.pending_observation_count != sum(
            request.expected_observations for request in self.requests
        ):
            raise ValueError("pending observation total does not match planned requests")
        if self.pending_observation_count != self.missing_observation_count:
            raise ValueError("every missing observation must remain explicit pending work")
        if self.estimated_pages != sum(request.estimated_pages for request in self.requests):
            raise ValueError("page estimate does not match planned requests")
        if self.estimated_calls != sum(request.estimated_calls for request in self.requests):
            raise ValueError("call estimate does not match planned requests")
        if self.estimated_bytes != sum(request.estimated_bytes for request in self.requests):
            raise ValueError("byte estimate does not match planned requests")
        if self.estimated_cost != sum(
            (request.estimated_cost for request in self.requests), start=Decimal(0)
        ):
            raise ValueError("cost estimate does not match planned requests")
        return self

    @property
    def is_no_op(self) -> bool:
        """A no-op has no provider work because verified coverage already satisfies demand."""

        return not self.requests


class IngestionPlanner:
    """Plan bounded requests from policy, calendar, coverage, and hard limits."""

    def __init__(self, policy_enforcer: RetentionPolicyEnforcer) -> None:
        self._policy_enforcer = policy_enforcer

    def plan(
        self,
        *,
        intent: IngestionIntent,
        streams: Sequence[StreamKey],
        instrument_mappings: Sequence[ProviderInstrumentMapping],
        desired_start: datetime,
        desired_end: datetime,
        calendar_snapshot: CalendarSnapshot,
        coverage: Sequence[VerifiedCoverageProjection],
        limits: PlannerLimits,
        budget: PlannerBudget,
        environment: RuntimeEnvironment,
        mapping_semantic_version: str,
        runtime_status: DatasetRuntimeStatus | None = None,
        repair_strategy: RepairStrategy | None = None,
        repair_reason: str | None = None,
    ) -> IngestionPlan:
        """Return a complete plan without persistence or provider construction.

        Desired bounds may surround closed time, but may not cut through an
        expected slot. This makes the selected domain the complete-slot
        intersection without requesting observations outside caller bounds.
        """

        desired_start = _aware_utc(desired_start, name="desired_start")
        desired_end = _aware_utc(desired_end, name="desired_end")
        if desired_end <= desired_start:
            raise PlanningError("desired_end must be later than desired_start")
        if intent is IngestionIntent.REPAIR:
            if repair_strategy is None or repair_reason is None or not repair_reason.strip():
                raise PlanningError("REPAIR requires an explicit strategy and non-empty reason")
            if repair_strategy is RepairStrategy.RAW_REPLAY:
                raise PlanningError(
                    "RAW_REPLAY requires retained-raw catalog orchestration and creates no "
                    "provider request; it is not implemented by this bounded network planner"
                )
        elif repair_strategy is not None or repair_reason is not None:
            raise PlanningError("repair strategy/reason may be supplied only for REPAIR")

        ordered_streams, exemplar = _validate_streams(streams)
        mappings_by_instrument = _validate_mappings(ordered_streams, instrument_mappings)
        _validate_snapshot_range(calendar_snapshot, desired_start, desired_end)

        # This public gate is deliberately called even when the outcome is a no-op.
        # Unknown, inactive, prohibited, or environment-ineligible datasets fail closed.
        planning_authorization = self._policy_enforcer.authorize_planning(
            exemplar.provider,
            exemplar.dataset,
            environment=environment,
            runtime_status=runtime_status,
        )
        policy_snapshot = planning_authorization.policy_snapshot
        if (exemplar.provider, exemplar.dataset) != (
            policy_snapshot.provider,
            policy_snapshot.dataset,
        ):
            raise PlanningError("stream provider/dataset keys must use catalog-canonical spelling")
        safe_end = planning_authorization.eligible_before

        all_slots = calendar_snapshot.expected_slots(exemplar.timeframe)
        _validate_ordered_slots(all_slots)
        eligible_slots = _complete_eligible_slots(
            all_slots,
            desired_start=desired_start,
            desired_end=desired_end,
            safe_end=safe_end,
        )
        slot_index = {
            (slot.start_utc, slot.end_utc): index for index, slot in enumerate(eligible_slots)
        }

        valid_coverage = _merge_coverage(
            coverage,
            ordered_streams,
            calendar_snapshot,
            planning_authorization,
        )
        subtract_coverage = not (
            intent is IngestionIntent.REPAIR and repair_strategy is RepairStrategy.PROVIDER_REFRESH
        )
        missing_by_stream: dict[str, tuple[ExpectedCalendarSlot, ...]] = {}
        for stream in ordered_streams:
            merged = valid_coverage.get(stream.stream_id, ()) if subtract_coverage else ()
            missing_by_stream[stream.stream_id] = tuple(
                slot for slot in eligible_slots if not _slot_is_covered(slot, merged)
            )

        pending: list[PlannedRequest] = []
        missing_observations = sum(len(slots) for slots in missing_by_stream.values())

        grouped: dict[tuple[tuple[datetime, datetime], ...], list[StreamKey]] = defaultdict(list)
        for stream in ordered_streams:
            missing = missing_by_stream[stream.stream_id]
            if missing:
                grouped[tuple((slot.start_utc, slot.end_utc) for slot in missing)].append(stream)

        for shape in sorted(grouped, key=_shape_sort_key):
            shape_slots = tuple(
                eligible_slots[slot_index[bounds]] for bounds in shape if bounds in slot_index
            )
            if len(shape_slots) != len(shape):
                raise PlanningError(
                    "missing-work shape is not part of the eligible calendar domain"
                )
            shape_streams = tuple(sorted(grouped[shape], key=lambda stream: stream.stream_id))
            instrument_chunk_size = _maximum_instrument_chunk(len(shape_streams), limits)
            slot_runs = _slot_runs(shape_slots, slot_index)
            # Use the largest chunk to define identical request bounds for every
            # instrument chunk that shares this missing-work shape.
            slots_per_request = _maximum_slots_per_request(instrument_chunk_size, limits)
            partitions = tuple(
                run[offset : offset + slots_per_request]
                for run in slot_runs
                for offset in range(0, len(run), slots_per_request)
            )

            for stream_offset in range(0, len(shape_streams), instrument_chunk_size):
                stream_chunk = shape_streams[stream_offset : stream_offset + instrument_chunk_size]
                request_mappings = tuple(
                    mappings_by_instrument[stream.instrument_id] for stream in stream_chunk
                )
                for slots in partitions:
                    specification = RequestSpecification(
                        provider=exemplar.provider,
                        dataset=exemplar.dataset,
                        data_kind=exemplar.data_kind,
                        instrument_mappings=request_mappings,
                        timeframe=exemplar.timeframe,
                        session=exemplar.session,
                        adjustment=exemplar.adjustment,
                        currency=exemplar.currency,
                        bar_semantics=exemplar.bar_semantics,
                        additional_dimensions=exemplar.additional_dimensions,
                        start=slots[0].start_utc,
                        end=slots[-1].end_utc,
                        mapping_semantic_version=mapping_semantic_version,
                    )
                    estimate = _estimate(
                        len(slots) * len(request_mappings),
                        limits,
                    )
                    authorization = self._policy_enforcer.authorize_request(
                        specification.provider,
                        specification.dataset,
                        environment=environment,
                        start=specification.start,
                        end=specification.end,
                        request_spec_hash=specification.request_spec_hash,
                        runtime_status=runtime_status,
                        planning_authorization=planning_authorization,
                    )
                    pending.append(
                        PlannedRequest(
                            specification=specification,
                            authorization=authorization,
                            expected_slots=slots,
                            expected_observations=estimate.observations,
                            estimated_pages=estimate.pages,
                            estimated_calls=estimate.calls,
                            estimated_bytes=estimate.bytes,
                            estimated_cost=estimate.cost,
                        )
                    )

        pending.sort(key=_planned_request_sort_key)
        totals = _Estimate(
            observations=sum(request.expected_observations for request in pending),
            pages=sum(request.estimated_pages for request in pending),
            calls=sum(request.estimated_calls for request in pending),
            bytes=sum(request.estimated_bytes for request in pending),
            cost=sum((request.estimated_cost for request in pending), start=Decimal(0)),
        )
        _enforce_budget(totals, budget)

        return IngestionPlan(
            intent=intent,
            provider=exemplar.provider,
            dataset=exemplar.dataset,
            environment=environment,
            policy_authorization=planning_authorization,
            acquisition_strategy=AcquisitionStrategy.NETWORK,
            repair_strategy=repair_strategy,
            repair_reason=repair_reason.strip() if repair_reason is not None else None,
            desired_start=desired_start,
            desired_end=desired_end,
            safe_end=safe_end,
            calendar_snapshot_checksum=calendar_snapshot.checksum,
            streams=ordered_streams,
            stream_ids=tuple(stream.stream_id for stream in ordered_streams),
            eligible_slot_count=len(eligible_slots),
            eligible_observation_count=len(eligible_slots) * len(ordered_streams),
            missing_observation_count=missing_observations,
            pending_observation_count=totals.observations,
            requests=tuple(pending),
            estimated_pages=totals.pages,
            estimated_calls=totals.calls,
            estimated_bytes=totals.bytes,
            estimated_cost=totals.cost,
        )


class _Estimate(_FrozenPlannerModel):
    observations: Annotated[int, Field(ge=0)]
    pages: Annotated[int, Field(ge=0)]
    calls: Annotated[int, Field(ge=0)]
    bytes: Annotated[int, Field(ge=0)]
    cost: Annotated[Decimal, Field(ge=0)]


def _aware_utc(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise PlanningError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _validate_streams(streams: Sequence[StreamKey]) -> tuple[tuple[StreamKey, ...], StreamKey]:
    if not streams:
        raise PlanningError("at least one stream is required")
    ordered = tuple(sorted(streams, key=lambda stream: stream.stream_id))
    if len({stream.stream_id for stream in ordered}) != len(ordered):
        raise PlanningError("streams contain duplicate identities")
    exemplar = ordered[0]
    comparable_fields = (
        "provider",
        "dataset",
        "data_kind",
        "timeframe",
        "session",
        "adjustment",
        "currency",
        "bar_semantics",
        "additional_dimensions",
    )
    for stream in ordered[1:]:
        if any(getattr(stream, field) != getattr(exemplar, field) for field in comparable_fields):
            raise PlanningError("one plan may group only streams with identical series dimensions")
    return ordered, exemplar


def _validate_mappings(
    streams: tuple[StreamKey, ...],
    mappings: Sequence[ProviderInstrumentMapping],
) -> dict[UUID, ProviderInstrumentMapping]:
    by_instrument: dict[UUID, ProviderInstrumentMapping] = {}
    for mapping in mappings:
        if mapping.instrument_id in by_instrument:
            raise PlanningError("instrument mappings contain duplicate instrument IDs")
        by_instrument[mapping.instrument_id] = mapping
    provider_identifiers = [mapping.provider_identifier for mapping in mappings]
    if len(provider_identifiers) != len(set(provider_identifiers)):
        raise PlanningError("provider identifiers must be unique across the complete plan")
    expected = {stream.instrument_id for stream in streams}
    if set(by_instrument) != expected:
        raise PlanningError("instrument mappings must exactly match planned stream instruments")
    return by_instrument


def _validate_snapshot_range(
    snapshot: CalendarSnapshot,
    desired_start: datetime,
    desired_end: datetime,
) -> None:
    try:
        timezone = ZoneInfo(snapshot.timezone_name)
    except ZoneInfoNotFoundError as error:
        raise PlanningError("calendar snapshot timezone is unavailable") from error
    first_date = desired_start.astimezone(timezone).date()
    last_date = (desired_end - timedelta(microseconds=1)).astimezone(timezone).date()
    if first_date < snapshot.range_start or last_date >= snapshot.range_end:
        raise PlanningError("calendar snapshot does not cover the complete desired interval")


def _validate_ordered_slots(slots: tuple[ExpectedCalendarSlot, ...]) -> None:
    previous_end: datetime | None = None
    for slot in slots:
        if previous_end is not None and slot.start_utc < previous_end:
            raise PlanningError("calendar expected slots overlap or are out of order")
        previous_end = slot.end_utc


def _complete_eligible_slots(
    slots: tuple[ExpectedCalendarSlot, ...],
    *,
    desired_start: datetime,
    desired_end: datetime,
    safe_end: datetime,
) -> tuple[ExpectedCalendarSlot, ...]:
    selected: list[ExpectedCalendarSlot] = []
    for slot in slots:
        # Filter non-finalized slots before checking whether user bounds cut
        # through them.  A scheduler's default ``end=now`` commonly lands in
        # the current 5m slot or daily session; that slot is outside the strict
        # historical-age grant and therefore cannot make an otherwise safe
        # update invalid.
        if slot.end_utc >= safe_end:
            continue
        intersects = slot.start_utc < desired_end and slot.end_utc > desired_start
        if not intersects:
            continue
        if slot.start_utc < desired_start or slot.end_utc > desired_end:
            raise PlanningError("desired bounds cut through a calendar-eligible slot")
        selected.append(slot)
    return tuple(selected)


def _merge_coverage(
    coverage: Sequence[VerifiedCoverageProjection],
    streams: tuple[StreamKey, ...],
    snapshot: CalendarSnapshot,
    authorization: PlanningPolicyAuthorization,
) -> dict[str, tuple[tuple[datetime, datetime], ...]]:
    stream_ids = {stream.stream_id for stream in streams}
    intervals: dict[str, list[tuple[datetime, datetime]]] = defaultdict(list)
    for window in coverage:
        if window.stream.stream_id not in stream_ids or not window.is_subtractable(
            snapshot, authorization
        ):
            continue
        intervals[window.stream.stream_id].append((window.start, window.end))

    merged: dict[str, tuple[tuple[datetime, datetime], ...]] = {}
    for stream_id, values in intervals.items():
        ordered = sorted(values)
        combined: list[tuple[datetime, datetime]] = []
        for start, end in ordered:
            if not combined or start > combined[-1][1]:
                combined.append((start, end))
                continue
            previous_start, previous_end = combined[-1]
            combined[-1] = (previous_start, max(previous_end, end))
        merged[stream_id] = tuple(combined)
    return merged


def _slot_is_covered(
    slot: ExpectedCalendarSlot,
    coverage: tuple[tuple[datetime, datetime], ...],
) -> bool:
    return any(start <= slot.start_utc and end >= slot.end_utc for start, end in coverage)


def _shape_sort_key(
    shape: tuple[tuple[datetime, datetime], ...],
) -> tuple[datetime, datetime, int, tuple[tuple[datetime, datetime], ...]]:
    return shape[0][0], shape[-1][1], len(shape), shape


def _planned_request_sort_key(
    request: PlannedRequest,
) -> tuple[datetime, datetime, tuple[str, ...], str]:
    return (
        request.specification.start,
        request.specification.end,
        tuple(str(mapping.instrument_id) for mapping in request.specification.instrument_mappings),
        request.request_spec_hash,
    )


def _slot_runs(
    slots: tuple[ExpectedCalendarSlot, ...],
    slot_index: dict[tuple[datetime, datetime], int],
) -> tuple[tuple[ExpectedCalendarSlot, ...], ...]:
    runs: list[list[ExpectedCalendarSlot]] = []
    previous_index: int | None = None
    previous_session_date: date | None = None
    for slot in slots:
        index = slot_index[(slot.start_utc, slot.end_utc)]
        crosses_intraday_session = (
            slot.timeframe is Timeframe.FIVE_MINUTES
            and previous_session_date is not None
            and slot.session_date != previous_session_date
        )
        if previous_index is None or index != previous_index + 1 or crosses_intraday_session:
            runs.append([])
        runs[-1].append(slot)
        previous_index = index
        previous_session_date = slot.session_date
    return tuple(tuple(run) for run in runs)


def _maximum_instrument_chunk(stream_count: int, limits: PlannerLimits) -> int:
    upper = min(stream_count, limits.max_instruments_per_request)
    candidate = _largest_fitting_count(upper, lambda value: _fits(value, limits))
    if candidate is None:
        raise PlanningError("provider limits cannot fit one instrument-slot observation")
    return candidate


def _maximum_slots_per_request(instrument_count: int, limits: PlannerLimits) -> int:
    upper = limits.max_expected_observations_per_request // instrument_count
    candidate = _largest_fitting_count(
        upper,
        lambda value: _fits(value * instrument_count, limits),
    )
    if candidate is None:
        raise PlanningError("provider limits cannot fit one calendar slot")
    return candidate


def _largest_fitting_count(
    upper: int,
    predicate: Callable[[int], bool],
) -> int | None:
    """Return the largest positive value accepted by a monotonic predicate."""

    lower = 1
    best: int | None = None
    while lower <= upper:
        middle = (lower + upper) // 2
        if predicate(middle):
            best = middle
            lower = middle + 1
        else:
            upper = middle - 1
    return best


def _fits(observations: int, limits: PlannerLimits) -> bool:
    estimate = _estimate(observations, limits)
    return (
        observations <= limits.max_expected_observations_per_request
        and estimate.pages <= limits.max_pages_per_request
        and estimate.calls <= limits.max_calls_per_request
        and estimate.bytes <= limits.max_estimated_bytes_per_request
        and (
            limits.max_estimated_cost_per_request is None
            or estimate.cost <= limits.max_estimated_cost_per_request
        )
    )


def _estimate(observations: int, limits: PlannerLimits) -> _Estimate:
    if observations <= 0:
        return _Estimate(observations=0, pages=0, calls=0, bytes=0, cost=Decimal(0))
    pages = (observations + limits.max_observations_per_page - 1) // (
        limits.max_observations_per_page
    )
    calls = pages
    estimated_bytes = (
        observations * limits.estimated_bytes_per_observation
        + pages * limits.estimated_bytes_per_page
    )
    return _Estimate(
        observations=observations,
        pages=pages,
        calls=calls,
        bytes=estimated_bytes,
        cost=limits.estimated_cost_per_call * calls,
    )


def _enforce_budget(estimate: _Estimate, budget: PlannerBudget) -> None:
    ceilings: tuple[tuple[str, int | Decimal, int | Decimal], ...] = (
        ("calls", estimate.calls, budget.max_calls),
        ("expected_observations", estimate.observations, budget.max_expected_observations),
        ("pages", estimate.pages, budget.max_pages),
        ("estimated_bytes", estimate.bytes, budget.max_estimated_bytes),
    )
    for name, value, ceiling in ceilings:
        if value > ceiling:
            raise BudgetExceeded(name, value, ceiling)
    if budget.max_estimated_cost is not None and estimate.cost > budget.max_estimated_cost:
        raise BudgetExceeded("estimated_cost", estimate.cost, budget.max_estimated_cost)


__all__ = [
    "AcquisitionStrategy",
    "BudgetExceeded",
    "CoverageClassification",
    "CoverageVerificationState",
    "IngestionIntent",
    "IngestionPlan",
    "IngestionPlanner",
    "PlannedRequest",
    "PlannerBudget",
    "PlannerLimits",
    "PlanningError",
    "RepairStrategy",
    "VerifiedCoverageProjection",
]
