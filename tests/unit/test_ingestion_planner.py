"""Offline tests for retention-aware deterministic ingestion planning."""

import inspect
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from investment_platform.data.calendar import CalendarSession, CalendarSnapshot
from investment_platform.data.ingestion.identity import (
    BarSemantics,
    DataKind,
    ProviderInstrumentMapping,
    StreamKey,
)
from investment_platform.data.ingestion.planner import (
    AcquisitionStrategy,
    BudgetExceeded,
    CoverageClassification,
    CoverageVerificationState,
    IngestionIntent,
    IngestionPlan,
    IngestionPlanner,
    PlannerBudget,
    PlannerLimits,
    PlanningError,
    RepairStrategy,
    VerifiedCoverageProjection,
)
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.retention import (
    DatasetPolicyDenied,
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.runtime import RuntimeEnvironment

_INSTRUMENT_A = UUID("00000000-0000-4000-8000-000000000001")
_INSTRUMENT_B = UUID("00000000-0000-4000-8000-000000000002")
_INSTRUMENT_C = UUID("00000000-0000-4000-8000-000000000003")
_GENERATED_AT = datetime(2026, 8, 31, 12, tzinfo=UTC)
_CLOCK = datetime(2026, 8, 31, 12, tzinfo=UTC)


def _snapshot() -> CalendarSnapshot:
    return CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="1",
        tzdata_version="test",
        calendar_name="SYNTHETIC_US_RTH",
        timezone_name="America/New_York",
        range_start=date(2025, 7, 2),
        range_end=date(2025, 7, 8),
        generated_at=_GENERATED_AT,
        sessions=(
            CalendarSession(
                session_date=date(2025, 7, 2),
                open_utc=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
                close_utc=datetime(2025, 7, 2, 20, tzinfo=UTC),
            ),
            CalendarSession(
                session_date=date(2025, 7, 3),
                open_utc=datetime(2025, 7, 3, 13, 30, tzinfo=UTC),
                close_utc=datetime(2025, 7, 3, 17, tzinfo=UTC),
                is_early_close=True,
            ),
            # July 4 is intentionally absent: the calendar proves it is closed.
            CalendarSession(
                session_date=date(2025, 7, 7),
                open_utc=datetime(2025, 7, 7, 13, 30, tzinfo=UTC),
                close_utc=datetime(2025, 7, 7, 20, tzinfo=UTC),
            ),
        ),
    )


def _stream(
    instrument_id: UUID = _INSTRUMENT_A,
    *,
    provider: str = "alpaca",
    dataset: str = "price_bars_sip",
    timeframe: Timeframe = Timeframe.FIVE_MINUTES,
) -> StreamKey:
    return StreamKey(
        provider=provider,
        dataset=dataset,
        data_kind=DataKind.PRICE_BAR,
        instrument_id=instrument_id,
        timeframe=timeframe,
        session=TradingSession.REGULAR,
        adjustment=AdjustmentState.UNADJUSTED,
        currency="USD",
        bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
    )


def _synthetic_stream(
    instrument_id: UUID = _INSTRUMENT_A,
    *,
    timeframe: Timeframe = Timeframe.FIVE_MINUTES,
) -> StreamKey:
    return _stream(
        instrument_id,
        provider="synthetic",
        dataset="price_bars",
        timeframe=timeframe,
    )


def _mapping(instrument_id: UUID, identifier: str) -> ProviderInstrumentMapping:
    return ProviderInstrumentMapping(
        instrument_id=instrument_id,
        provider_identifier=identifier,
    )


def _limits(**overrides: object) -> PlannerLimits:
    values: dict[str, object] = {
        "max_instruments_per_request": 10,
        "max_expected_observations_per_request": 500,
        "max_observations_per_page": 100,
        "max_pages_per_request": 10,
        "max_calls_per_request": 10,
        "max_estimated_bytes_per_request": 1_000_000,
        "estimated_bytes_per_observation": 200,
        "estimated_bytes_per_page": 1_000,
        "estimated_cost_per_call": Decimal("0.01"),
    }
    values.update(overrides)
    return PlannerLimits.model_validate(values)


def _budget(**overrides: object) -> PlannerBudget:
    values: dict[str, object] = {
        "max_calls": 100,
        "max_expected_observations": 10_000,
        "max_pages": 100,
        "max_estimated_bytes": 100_000_000,
        "max_estimated_cost": Decimal("10"),
    }
    values.update(overrides)
    return PlannerBudget.model_validate(values)


def _planner(*, clock: datetime = _CLOCK) -> IngestionPlanner:
    catalog = RetentionPolicyCatalog.load_default()
    return IngestionPlanner(RetentionPolicyEnforcer(catalog, clock=lambda: clock))


def _coverage(
    stream: StreamKey,
    start: datetime,
    end: datetime,
    *,
    snapshot: CalendarSnapshot,
    classification: CoverageClassification = CoverageClassification.OBSERVED,
    overrides: dict[str, object] | None = None,
) -> VerifiedCoverageProjection:
    policy = RetentionPolicyCatalog.load_default().snapshot(
        stream.provider,
        stream.dataset,
        captured_at=_CLOCK,
    )
    values: dict[str, object] = {
        "coverage_id": f"coverage_{start:%Y%m%dT%H%M%S}_{end:%Y%m%dT%H%M%S}",
        "request_instance_id": "request_instance_test_1",
        "canonical_batch_id": f"batch_v1_{'b' * 64}",
        "policy_snapshot_id": "policy_snapshot_test_1",
        "active_policy_snapshot_id": "policy_snapshot_test_1",
        "calendar_snapshot_id": "calendar_snapshot_test_1",
        "stream": stream,
        "start": start,
        "end": end,
        "classification": classification,
        "verification_state": CoverageVerificationState.VERIFIED,
        "retained": True,
        "policy_valid": True,
        "policy_id": policy.policy_id,
        "policy_revision": policy.policy_revision,
        "policy_hash": policy.policy_hash,
        "active_policy_hash": policy.policy_hash,
        "calendar_snapshot_checksum": snapshot.checksum,
        "relational_provenance_verified": True,
        "interval_verified": True,
        "request_completed": True,
        "pagination_verified": True,
        "canonical_batch_verified": True,
        "canonical_file_count": 1,
        "raw_artifact_count": 1,
        "artifacts_present": True,
    }
    if classification is CoverageClassification.VERIFIED_EMPTY:
        values["provider_semantics_version"] = "synthetic-complete-pagination-v1"
    if overrides is not None:
        values.update(overrides)
    return VerifiedCoverageProjection.model_validate(values)


def _plan(
    *,
    planner: IngestionPlanner | None = None,
    streams: tuple[StreamKey, ...] = (_stream(),),
    mappings: tuple[ProviderInstrumentMapping, ...] | None = None,
    coverage: tuple[VerifiedCoverageProjection, ...] = (),
    limits: PlannerLimits | None = None,
    budget: PlannerBudget | None = None,
    start: datetime = datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
    end: datetime = datetime(2025, 7, 7, 20, tzinfo=UTC),
    intent: IngestionIntent = IngestionIntent.BACKFILL,
    repair_strategy: RepairStrategy | None = None,
    repair_reason: str | None = None,
    environment: RuntimeEnvironment | None = None,
) -> IngestionPlan:
    if mappings is None:
        names = {
            _INSTRUMENT_A: "AAPL",
            _INSTRUMENT_B: "MSFT",
            _INSTRUMENT_C: "NVDA",
        }
        mappings = tuple(
            _mapping(stream.instrument_id, names[stream.instrument_id]) for stream in streams
        )
    if environment is None:
        environment = (
            RuntimeEnvironment.TEST
            if all(stream.provider == "synthetic" for stream in streams)
            else RuntimeEnvironment.PRIVATE_RESEARCH
        )
    return (planner or _planner()).plan(
        intent=intent,
        streams=streams,
        instrument_mappings=mappings,
        desired_start=start,
        desired_end=end,
        calendar_snapshot=_snapshot(),
        coverage=coverage,
        limits=limits or _limits(),
        budget=budget or _budget(),
        environment=environment,
        mapping_semantic_version=f"{streams[0].provider}-bars-request-v1",
        repair_strategy=repair_strategy,
        repair_reason=repair_reason,
    )


@pytest.mark.unit
def test_out_of_order_overlapping_verified_coverage_is_merged_and_subtracted() -> None:
    snapshot = _snapshot()
    stream = _synthetic_stream()
    session = snapshot.sessions[0]
    covered = (
        _coverage(
            stream,
            session.open_utc + timedelta(minutes=25),
            session.open_utc + timedelta(minutes=60),
            snapshot=snapshot,
        ),
        _coverage(
            stream,
            session.open_utc,
            session.open_utc + timedelta(minutes=30),
            snapshot=snapshot,
        ),
    )

    plan = _plan(
        streams=(stream,),
        coverage=covered,
        start=session.open_utc,
        end=session.close_utc,
    )

    assert plan.eligible_observation_count == 78
    assert plan.missing_observation_count == 66
    assert plan.pending_observation_count == 66
    assert all(
        slot.start_utc >= session.open_utc + timedelta(minutes=60)
        for request in plan.requests
        for slot in request.expected_slots
    )


@pytest.mark.unit
def test_complete_coverage_returns_policy_checked_no_op() -> None:
    snapshot = _snapshot()
    stream = _synthetic_stream()
    coverage = (
        _coverage(
            stream,
            snapshot.sessions[0].open_utc,
            snapshot.sessions[-1].close_utc,
            snapshot=snapshot,
        ),
    )

    plan = _plan(streams=(stream,), coverage=coverage)

    assert plan.is_no_op
    assert plan.requests == ()
    assert plan.missing_observation_count == 0
    assert plan.estimated_calls == 0


@pytest.mark.unit
def test_partition_union_equals_missing_expected_slots_without_overlap() -> None:
    snapshot = _snapshot()
    stream = _stream()
    plan = _plan(
        streams=(stream,),
        limits=_limits(
            max_expected_observations_per_request=25,
            max_observations_per_page=10,
            max_pages_per_request=3,
            max_calls_per_request=3,
        ),
    )

    planned_bounds = [
        (slot.start_utc, slot.end_utc)
        for request in plan.requests
        for slot in request.expected_slots
    ]
    expected_bounds = [
        (slot.start_utc, slot.end_utc) for slot in snapshot.expected_slots(Timeframe.FIVE_MINUTES)
    ]
    assert planned_bounds == expected_bounds
    assert len(planned_bounds) == len(set(planned_bounds)) == 198
    assert all(request.expected_observations <= 25 for request in plan.requests)
    assert all(request.estimated_pages <= 3 for request in plan.requests)


@pytest.mark.unit
def test_synthetic_calendar_holiday_and_early_close_drive_expected_counts() -> None:
    snapshot = _snapshot()
    plan = _plan()

    assert snapshot.early_close_dates == (date(2025, 7, 3),)
    assert plan.eligible_slot_count == 78 + 42 + 78
    assert {slot.session_date for request in plan.requests for slot in request.expected_slots} == {
        date(2025, 7, 2),
        date(2025, 7, 3),
        date(2025, 7, 7),
    }


@pytest.mark.unit
def test_multi_instrument_grouping_and_caller_order_are_deterministic() -> None:
    streams = (_stream(_INSTRUMENT_A), _stream(_INSTRUMENT_B), _stream(_INSTRUMENT_C))
    mappings = (
        _mapping(_INSTRUMENT_A, "AAPL"),
        _mapping(_INSTRUMENT_B, "MSFT"),
        _mapping(_INSTRUMENT_C, "NVDA"),
    )

    first = _plan(streams=streams, mappings=mappings)
    second = _plan(streams=tuple(reversed(streams)), mappings=tuple(reversed(mappings)))

    assert tuple(request.request_spec_hash for request in first.requests) == tuple(
        request.request_spec_hash for request in second.requests
    )
    assert all(len(request.specification.instrument_mappings) == 3 for request in first.requests)
    assert first.stream_ids == tuple(sorted(first.stream_ids))


@pytest.mark.unit
def test_streams_with_different_missing_shapes_are_not_grouped() -> None:
    snapshot = _snapshot()
    first = _synthetic_stream(_INSTRUMENT_A)
    second = _synthetic_stream(_INSTRUMENT_B)
    session = snapshot.sessions[0]
    first_coverage = (
        _coverage(
            first,
            session.open_utc,
            session.open_utc + timedelta(hours=1),
            snapshot=snapshot,
        ),
    )

    plan = _plan(streams=(first, second), coverage=first_coverage)

    assert all(len(request.specification.instrument_mappings) == 1 for request in plan.requests)
    assert plan.missing_observation_count == 198 * 2 - 12


@pytest.mark.unit
def test_provider_limits_partition_and_run_budget_fails_before_dispatch() -> None:
    with pytest.raises(BudgetExceeded, match="expected_observations") as captured:
        _plan(
            limits=_limits(max_expected_observations_per_request=20),
            budget=_budget(max_expected_observations=197),
        )

    assert captured.value.ceiling == "expected_observations"
    assert captured.value.estimated == 198
    assert captured.value.allowed == 197


@pytest.mark.unit
def test_missing_coverage_is_replanned_and_resume_state_is_not_a_planner_shortcut() -> None:
    first = _plan(limits=_limits(max_expected_observations_per_request=50))
    repeated = _plan(limits=_limits(max_expected_observations_per_request=50))

    assert (
        "completed_request_spec_hashes" not in inspect.signature(IngestionPlanner.plan).parameters
    )
    assert repeated.pending_observation_count == repeated.missing_observation_count
    assert tuple(request.request_spec_hash for request in repeated.requests) == tuple(
        request.request_spec_hash for request in first.requests
    )


@pytest.mark.unit
def test_strict_safe_end_excludes_slot_ending_at_exact_policy_frontier() -> None:
    # Alpaca policy is 15 minutes plus a 60-second finalization buffer.
    safe_end = datetime(2025, 7, 2, 20, tzinfo=UTC)
    planner = _planner(clock=safe_end + timedelta(minutes=16))
    session = _snapshot().sessions[0]

    plan = _plan(
        planner=planner,
        start=session.open_utc,
        end=session.close_utc,
    )

    assert plan.safe_end == safe_end
    assert plan.eligible_slot_count == 77
    assert all(
        slot.end_utc < safe_end for request in plan.requests for slot in request.expected_slots
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("provider", "dataset"),
    (("alpaca", "historical_options"), ("massive", "price_bars")),
)
def test_unknown_pending_and_prohibited_exact_dataset_policies_fail_closed(
    provider: str,
    dataset: str,
) -> None:
    stream = _stream(provider=provider, dataset=dataset)

    with pytest.raises(DatasetPolicyDenied):
        _plan(streams=(stream,))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("override", "expected"),
    (
        ({"request_completed": False}, "completed request"),
        ({"pagination_verified": False}, "verified pagination"),
        ({"interval_verified": False}, "verified pagination, interval"),
        ({"relational_provenance_verified": False}, "relational provenance"),
        ({"provider_semantics_version": None}, "semantics registry entry"),
    ),
)
def test_verified_empty_requires_complete_durable_proof(
    override: dict[str, object], expected: str
) -> None:
    snapshot = _snapshot()
    session = snapshot.sessions[0]

    with pytest.raises(ValidationError, match=expected):
        _coverage(
            _synthetic_stream(),
            session.open_utc,
            session.close_utc,
            snapshot=snapshot,
            classification=CoverageClassification.VERIFIED_EMPTY,
            overrides=override,
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "override",
    (
        {"verification_state": CoverageVerificationState.STALE},
        {"retained": False},
        {"policy_valid": False},
        {"interval_verified": False},
        {"request_completed": False},
        {"pagination_verified": False},
        {"calendar_snapshot_checksum": f"sha256:{'0' * 64}"},
        {"relational_provenance_verified": False},
        {"canonical_batch_verified": False},
        {"canonical_file_count": 0},
        {"raw_artifact_count": 0},
        {"artifacts_present": False},
        {"active_policy_snapshot_id": "different_policy_snapshot"},
        {"active_policy_hash": "0" * 64},
    ),
)
def test_any_missing_observed_coverage_prerequisite_keeps_work_missing(
    override: dict[str, object],
) -> None:
    snapshot = _snapshot()
    stream = _synthetic_stream()
    session = snapshot.sessions[0]
    windows = (
        _coverage(
            stream,
            session.open_utc,
            session.close_utc,
            snapshot=snapshot,
            overrides=override,
        ),
    )

    plan = _plan(
        streams=(stream,),
        coverage=windows,
        start=session.open_utc,
        end=session.close_utc,
    )

    assert plan.missing_observation_count == 78


@pytest.mark.unit
@pytest.mark.parametrize(
    "identity_field",
    (
        "coverage_id",
        "request_instance_id",
        "canonical_batch_id",
        "policy_snapshot_id",
        "active_policy_snapshot_id",
        "calendar_snapshot_id",
    ),
)
def test_coverage_requires_every_durable_proof_identity(identity_field: str) -> None:
    snapshot = _snapshot()
    session = snapshot.sessions[0]

    with pytest.raises(ValidationError):
        _coverage(
            _synthetic_stream(),
            session.open_utc,
            session.close_utc,
            snapshot=snapshot,
            overrides={identity_field: ""},
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "required_field",
    (
        "coverage_id",
        "request_instance_id",
        "canonical_batch_id",
        "policy_snapshot_id",
        "calendar_snapshot_id",
        "calendar_snapshot_checksum",
        "verification_state",
        "retained",
        "policy_valid",
        "policy_id",
        "policy_revision",
        "policy_hash",
        "active_policy_hash",
        "relational_provenance_verified",
        "interval_verified",
        "request_completed",
        "pagination_verified",
        "canonical_batch_verified",
        "canonical_file_count",
        "raw_artifact_count",
        "artifacts_present",
        "classification",
    ),
)
def test_coverage_proof_fields_have_no_implicit_trusting_defaults(required_field: str) -> None:
    snapshot = _snapshot()
    session = snapshot.sessions[0]
    complete = _coverage(
        _synthetic_stream(),
        session.open_utc,
        session.close_utc,
        snapshot=snapshot,
    ).model_dump(mode="python")
    del complete[required_field]

    with pytest.raises(ValidationError, match=required_field):
        VerifiedCoverageProjection.model_validate(complete)


@pytest.mark.unit
def test_alpaca_has_no_verified_empty_semantics_registry_entry() -> None:
    snapshot = _snapshot()
    session = snapshot.sessions[0]

    with pytest.raises(ValidationError, match="semantics registry entry"):
        _coverage(
            _stream(),
            session.open_utc,
            session.close_utc,
            snapshot=snapshot,
            classification=CoverageClassification.VERIFIED_EMPTY,
        )


@pytest.mark.unit
def test_provider_identifier_must_be_unique_across_single_instrument_chunks() -> None:
    streams = (_stream(_INSTRUMENT_A), _stream(_INSTRUMENT_B))
    mappings = (
        _mapping(_INSTRUMENT_A, "DUPLICATE"),
        _mapping(_INSTRUMENT_B, "DUPLICATE"),
    )

    with pytest.raises(PlanningError, match="unique across the complete plan"):
        _plan(
            streams=streams,
            mappings=mappings,
            limits=_limits(max_instruments_per_request=1),
        )


@pytest.mark.unit
def test_per_request_cost_ceiling_partitions_at_exact_boundary() -> None:
    plan = _plan(
        limits=_limits(
            max_expected_observations_per_request=500,
            max_observations_per_page=10,
            estimated_cost_per_call=Decimal("0.25"),
            max_estimated_cost_per_request=Decimal("0.50"),
        )
    )

    assert [request.expected_observations for request in plan.requests[:-1]] == [20] * 9
    assert plan.requests[-1].expected_observations == 18
    assert all(request.estimated_cost <= Decimal("0.50") for request in plan.requests)


@pytest.mark.unit
def test_desired_bound_that_cuts_a_slot_is_rejected_without_overacquisition() -> None:
    session = _snapshot().sessions[0]

    with pytest.raises(PlanningError, match="cut through"):
        _plan(start=session.open_utc + timedelta(minutes=1), end=session.close_utc)


@pytest.mark.unit
def test_all_ingestion_intents_share_the_same_provider_neutral_request_contract() -> None:
    results = (
        _plan(intent=IngestionIntent.BACKFILL),
        _plan(intent=IngestionIntent.UPDATE),
        _plan(
            intent=IngestionIntent.REPAIR,
            repair_strategy=RepairStrategy.MISSING_ONLY,
            repair_reason="fill verified acquisition gap",
        ),
    )

    assert all(result.acquisition_strategy is AcquisitionStrategy.NETWORK for result in results)
    assert (
        tuple(request.request_spec_hash for request in results[0].requests)
        == tuple(request.request_spec_hash for request in results[1].requests)
        == tuple(request.request_spec_hash for request in results[2].requests)
    )


@pytest.mark.unit
def test_provider_refresh_reacquires_explicit_window_despite_verified_coverage() -> None:
    snapshot = _snapshot()
    stream = _synthetic_stream()
    session = snapshot.sessions[0]
    complete = (
        _coverage(
            stream,
            session.open_utc,
            session.close_utc,
            snapshot=snapshot,
        ),
    )

    missing_only = _plan(
        streams=(stream,),
        coverage=complete,
        start=session.open_utc,
        end=session.close_utc,
        intent=IngestionIntent.REPAIR,
        repair_strategy=RepairStrategy.MISSING_ONLY,
        repair_reason="repair only cataloged gaps",
    )
    refresh = _plan(
        streams=(stream,),
        coverage=complete,
        start=session.open_utc,
        end=session.close_utc,
        intent=IngestionIntent.REPAIR,
        repair_strategy=RepairStrategy.PROVIDER_REFRESH,
        repair_reason="verify provider correction",
    )

    assert missing_only.is_no_op
    assert refresh.missing_observation_count == 78
    assert refresh.pending_observation_count == 78
    assert refresh.repair_strategy is RepairStrategy.PROVIDER_REFRESH


@pytest.mark.unit
def test_raw_replay_is_explicitly_deferred_from_network_planning() -> None:
    with pytest.raises(PlanningError, match="retained-raw catalog orchestration"):
        _plan(
            intent=IngestionIntent.REPAIR,
            repair_strategy=RepairStrategy.RAW_REPLAY,
            repair_reason="replay retained immutable raw pages",
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "unsafe_reason",
    ("secret=must-not-persist", "https://authenticated.invalid/private", "line\nbreak"),
)
def test_repair_reason_rejects_sensitive_persisted_text(unsafe_reason: str) -> None:
    with pytest.raises(ValidationError, match="repair reason"):
        _plan(
            intent=IngestionIntent.REPAIR,
            repair_strategy=RepairStrategy.MISSING_ONLY,
            repair_reason=unsafe_reason,
        )
