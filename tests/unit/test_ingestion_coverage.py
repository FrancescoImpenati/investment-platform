"""Deterministic tests for coverage, gaps, and watermark reconstruction."""

from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from investment_platform.data.calendar import CalendarSession, CalendarSnapshot
from investment_platform.data.ingestion.coverage import (
    CoverageDomainError,
    CoverageInvalidation,
    CoverageInvalidationReason,
    CoverageRequestTerminalState,
    CoverageSegment,
    CoverageStreamOutcome,
    FrontierEvaluation,
    GapFinding,
    GapStatus,
    GapTransitionError,
    GapType,
    MissingSlotReason,
    WatermarkCandidate,
    WatermarkChangeContext,
    WatermarkPolicyReason,
    assess_watermark_policy,
    invalidate_coverage,
    invalidate_materialized_watermark,
    materialize_watermark,
    reconstruct_frontier,
    transition_gap,
)
from investment_platform.data.ingestion.identity import BarSemantics, DataKind, StreamKey
from investment_platform.data.ingestion.planner import (
    CoverageClassification,
    CoverageVerificationState,
)
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.retention import (
    DatasetPolicyStatus,
    DatasetRetentionPolicy,
    DatasetRuntimeStatus,
    LayerRetentionPolicy,
    RetentionMode,
    RetentionPolicyCatalog,
)
from investment_platform.runtime import RuntimeEnvironment

_INSTRUMENT = UUID("00000000-0000-4000-8000-000000000001")
_NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
_CALENDAR_ID = "calendar-snapshot-xnys-test"
_POLICY_SNAPSHOT_ID = "policy-snapshot-synthetic-test"


def _snapshot() -> CalendarSnapshot:
    return CalendarSnapshot.create(
        library_name="exchange_calendars",
        library_version="test",
        tzdata_version="test",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=date(2025, 7, 2),
        range_end=date(2025, 7, 8),
        generated_at=_NOW,
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
            # 2025-07-04 is an exchange holiday; the weekend follows it.
            CalendarSession(
                session_date=date(2025, 7, 7),
                open_utc=datetime(2025, 7, 7, 13, 30, tzinfo=UTC),
                close_utc=datetime(2025, 7, 7, 20, tzinfo=UTC),
            ),
        ),
    )


def _stream(
    *,
    timeframe: Timeframe = Timeframe.FIVE_MINUTES,
    provider: str = "synthetic",
    dataset: str = "price_bars",
) -> StreamKey:
    return StreamKey(
        provider=provider,
        dataset=dataset,
        data_kind=DataKind.PRICE_BAR,
        instrument_id=_INSTRUMENT,
        timeframe=timeframe,
        session=TradingSession.REGULAR,
        adjustment=AdjustmentState.UNADJUSTED,
        currency="USD",
        bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
    )


def _policy(provider: str = "synthetic", dataset: str = "price_bars") -> DatasetRetentionPolicy:
    return RetentionPolicyCatalog.load_default().lookup(provider, dataset)


def _segment(
    stream: StreamKey,
    snapshot: CalendarSnapshot,
    start: datetime,
    end: datetime,
    *,
    suffix: str,
    classification: CoverageClassification = CoverageClassification.OBSERVED,
    row_count: int | None = None,
    coverage_start: datetime | None = None,
    policy: DatasetRetentionPolicy | None = None,
    **overrides: object,
) -> CoverageSegment:
    selected_policy = policy or _policy(stream.provider, stream.dataset)
    slots = tuple(
        slot
        for slot in snapshot.expected_slots(stream.timeframe)
        if start <= slot.start_utc and end >= slot.end_utc
    )
    values: dict[str, object] = {
        "coverage_id": f"coverage-{suffix}",
        "stream_id": stream.stream_id,
        "canonical_batch_id": f"batch-{suffix}",
        "calendar_snapshot_id": _CALENDAR_ID,
        "calendar_snapshot_checksum": snapshot.checksum,
        "policy_snapshot_id": _POLICY_SNAPSHOT_ID,
        "policy_id": selected_policy.policy_id,
        "policy_revision": selected_policy.revision,
        "policy_hash": selected_policy.content_hash,
        "coverage_start": coverage_start or start,
        "start": start,
        "end": end,
        "classification": classification,
        "verification_state": CoverageVerificationState.VERIFIED,
        "retained": True,
        "row_count": (
            len(slots)
            if row_count is None and classification is CoverageClassification.OBSERVED
            else (row_count or 0)
        ),
        "artifact_count": 2,
        "artifacts_present": True,
        "artifact_integrity_verified": True,
        "interval_verified": True,
        "request_completed": True,
        "request_terminal_state": CoverageRequestTerminalState.SUCCESS,
        "stream_outcome": CoverageStreamOutcome.PUBLISHABLE,
        "pagination_verified": True,
        "terminal_page_verified": True,
        "canonical_batch_verified": True,
        "canonical_file_count": 1,
        "raw_artifact_count": 2,
        "relational_provenance_verified": True,
        "provider_semantics_version": (
            "synthetic-complete-pagination-v1"
            if classification is CoverageClassification.VERIFIED_EMPTY
            else None
        ),
        "generation": 1,
        "verified_at": _NOW,
    }
    values.update(overrides)
    return CoverageSegment.model_validate(values)


def _evaluate(
    *,
    stream: StreamKey,
    snapshot: CalendarSnapshot,
    start: datetime,
    end: datetime,
    coverage: tuple[CoverageSegment, ...],
    gaps: tuple[GapFinding, ...] = (),
    policy: DatasetRetentionPolicy | None = None,
    runtime_status: DatasetRuntimeStatus | None = None,
) -> FrontierEvaluation:
    return reconstruct_frontier(
        stream=stream,
        calendar_snapshot=snapshot,
        calendar_snapshot_id=_CALENDAR_ID,
        policy=policy or _policy(stream.provider, stream.dataset),
        policy_snapshot_id=_POLICY_SNAPSHOT_ID,
        runtime_status=runtime_status,
        coverage_start=start,
        domain_end=end,
        coverage=coverage,
        gaps=gaps,
        evaluated_at=_NOW,
    )


def test_out_of_order_daily_segments_form_contiguous_union_across_holiday() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slots = snapshot.expected_slots(Timeframe.ONE_DAY)
    coverage = tuple(
        reversed(
            tuple(
                _segment(
                    stream,
                    snapshot,
                    slot.start_utc,
                    slot.end_utc,
                    suffix=f"daily-{index}",
                    coverage_start=slots[0].start_utc,
                )
                for index, slot in enumerate(slots)
            )
        )
    )

    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slots[0].start_utc,
        end=slots[-1].end_utc,
        coverage=coverage,
    )

    assert result.contiguous_slot_count == 3
    assert result.exclusive_frontier == slots[-1].end_utc
    assert result.last_verified_session == date(2025, 7, 7)
    assert result.candidate is not None
    assert any(
        interval.start == slots[1].end_utc and interval.end == slots[2].start_utc
        for interval in result.not_applicable
    )


def test_frontier_crosses_closed_friday_to_monday_but_stops_at_monday_gap() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    thursday_early_close, monday = snapshot.expected_slots(Timeframe.ONE_DAY)[1:]
    coverage = (
        _segment(
            stream,
            snapshot,
            thursday_early_close.start_utc,
            thursday_early_close.end_utc,
            suffix="friday-analogue",
        ),
    )

    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=thursday_early_close.start_utc,
        end=monday.end_utc,
        coverage=coverage,
    )

    assert result.contiguous_slot_count == 1
    assert result.exclusive_frontier == monday.start_utc
    assert result.uncovered_slots[0].slot == monday
    assert result.uncovered_slots[0].reason is MissingSlotReason.NO_VALID_COVERAGE
    assert result.candidate is not None


def test_early_close_has_42_slots_and_first_missing_slot_stops_frontier() -> None:
    snapshot = _snapshot()
    stream = _stream()
    early_slots = tuple(
        slot
        for slot in snapshot.expected_slots(Timeframe.FIVE_MINUTES)
        if slot.session_date == date(2025, 7, 3)
    )
    missing = early_slots[11]
    before = _segment(
        stream,
        snapshot,
        early_slots[0].start_utc,
        early_slots[10].end_utc,
        suffix="before-gap",
    )
    after = _segment(
        stream,
        snapshot,
        early_slots[12].start_utc,
        early_slots[-1].end_utc,
        suffix="after-gap",
        coverage_start=early_slots[0].start_utc,
    )

    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=early_slots[0].start_utc,
        end=early_slots[-1].end_utc,
        coverage=(after, before),
    )

    assert len(early_slots) == 42
    assert result.contiguous_slot_count == 11
    assert result.exclusive_frontier == missing.start_utc
    assert result.uncovered_slots == (result.uncovered_slots[0],)
    assert result.uncovered_slots[0].slot == missing


def test_open_blocking_gap_overrides_otherwise_valid_coverage() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slots = snapshot.expected_slots(Timeframe.ONE_DAY)
    segment = _segment(
        stream,
        snapshot,
        slots[0].start_utc,
        slots[-1].end_utc,
        suffix="all-days",
    )
    gap = GapFinding(
        gap_id="gap-integrity",
        stream_id=stream.stream_id,
        start=slots[1].start_utc,
        end=slots[1].end_utc,
        gap_type=GapType.INTEGRITY,
        status=GapStatus.OPEN,
        blocking=True,
        detected_at=_NOW,
        canonical_batch_id=segment.canonical_batch_id,
    )

    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slots[0].start_utc,
        end=slots[-1].end_utc,
        coverage=(segment,),
        gaps=(gap,),
    )

    assert result.contiguous_slot_count == 1
    assert result.exclusive_frontier == slots[1].start_utc
    assert result.uncovered_slots[0].reason is MissingSlotReason.BLOCKING_GAP
    assert result.uncovered_slots[0].blocking_gap_ids == (gap.gap_id,)
    assert result.candidate is not None
    assert result.candidate.blocking_gap_count == 1


def test_later_subwindow_cannot_rebase_coverage_start_and_skip_prior_gap() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slots = snapshot.expected_slots(Timeframe.ONE_DAY)
    first = _segment(
        stream,
        snapshot,
        slots[0].start_utc,
        slots[0].end_utc,
        suffix="anti-rebase-first",
        coverage_start=slots[0].start_utc,
    )
    last = _segment(
        stream,
        snapshot,
        slots[2].start_utc,
        slots[2].end_utc,
        suffix="anti-rebase-last",
        coverage_start=slots[0].start_utc,
    )
    gap = GapFinding(
        gap_id="gap-anti-rebase",
        stream_id=stream.stream_id,
        start=slots[1].start_utc,
        end=slots[1].end_utc,
        gap_type=GapType.ACQUISITION,
        status=GapStatus.OPEN,
        blocking=True,
        detected_at=_NOW,
    )
    full = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slots[0].start_utc,
        end=slots[2].end_utc,
        coverage=(first, last),
        gaps=(gap,),
    )
    assert full.exclusive_frontier == slots[1].start_utc

    with pytest.raises(CoverageDomainError, match="rebases"):
        _evaluate(
            stream=stream,
            snapshot=snapshot,
            start=slots[2].start_utc,
            end=slots[2].end_utc,
            coverage=(last,),
            gaps=(gap,),
        )


def test_resolved_or_closed_time_gap_does_not_block_eligible_frontier() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slots = snapshot.expected_slots(Timeframe.ONE_DAY)
    segment = _segment(
        stream,
        snapshot,
        slots[0].start_utc,
        slots[-1].end_utc,
        suffix="all-days-resolved",
    )
    resolved = GapFinding(
        gap_id="gap-resolved",
        stream_id=stream.stream_id,
        start=slots[1].start_utc,
        end=slots[1].end_utc,
        gap_type=GapType.ACQUISITION,
        status=GapStatus.RESOLVED,
        blocking=True,
        detected_at=_NOW - timedelta(hours=2),
        resolved_at=_NOW - timedelta(hours=1),
    )
    closed_only = GapFinding(
        gap_id="gap-closed-only",
        stream_id=stream.stream_id,
        start=slots[1].end_utc,
        end=slots[2].start_utc,
        gap_type=GapType.ACQUISITION,
        status=GapStatus.OPEN,
        blocking=True,
        detected_at=_NOW,
    )

    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slots[0].start_utc,
        end=slots[-1].end_utc,
        coverage=(segment,),
        gaps=(closed_only, resolved),
    )

    assert result.contiguous_slot_count == len(slots)
    assert result.active_blocking_gaps == ()


def test_verified_empty_advances_only_for_approved_complete_synthetic_semantics() -> None:
    snapshot = _snapshot()
    stream = _stream()
    slot = snapshot.expected_slots(Timeframe.FIVE_MINUTES)[0]
    empty = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="verified-empty",
        classification=CoverageClassification.VERIFIED_EMPTY,
        request_terminal_state=CoverageRequestTerminalState.PARTIAL,
    )

    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slot.start_utc,
        end=slot.end_utc,
        coverage=(empty,),
    )

    assert result.contiguous_slot_count == 1
    assert result.candidate is not None


def test_absent_bar_or_incomplete_pagination_cannot_create_verified_empty() -> None:
    snapshot = _snapshot()
    stream = _stream()
    slot = snapshot.expected_slots(Timeframe.FIVE_MINUTES)[0]

    with pytest.raises(ValidationError, match="complete request, pagination"):
        _segment(
            stream,
            snapshot,
            slot.start_utc,
            slot.end_utc,
            suffix="incomplete-empty",
            classification=CoverageClassification.VERIFIED_EMPTY,
            pagination_verified=False,
        )


def test_alpaca_has_no_verified_empty_semantics_entry() -> None:
    snapshot = _snapshot()
    stream = _stream(provider="alpaca", dataset="price_bars_sip")
    policy = _policy("alpaca", "price_bars_sip")
    slot = snapshot.expected_slots(Timeframe.FIVE_MINUTES)[0]
    empty = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="alpaca-empty",
        classification=CoverageClassification.VERIFIED_EMPTY,
        policy=policy,
        provider_semantics_version="unapproved-alpaca-omission",
    )

    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slot.start_utc,
        end=slot.end_utc,
        coverage=(empty,),
        policy=policy,
    )

    assert result.contiguous_slot_count == 0
    assert result.candidate is None
    assert result.invalid_coverage_ids == (empty.coverage_id,)


def test_conflicting_observed_and_verified_empty_claims_fail_closed() -> None:
    snapshot = _snapshot()
    stream = _stream()
    slot = snapshot.expected_slots(Timeframe.FIVE_MINUTES)[0]
    observed = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="observed-conflict",
    )
    empty = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="empty-conflict",
        classification=CoverageClassification.VERIFIED_EMPTY,
    )

    with pytest.raises(CoverageDomainError, match="conflicting"):
        _evaluate(
            stream=stream,
            snapshot=snapshot,
            start=slot.start_utc,
            end=slot.end_utc,
            coverage=(observed, empty),
        )


def test_ephemeral_policy_cannot_create_durable_historical_watermark() -> None:
    snapshot = _snapshot()
    policy = DatasetRetentionPolicy.model_validate(
        {
            **_policy("databento", "opra.pillar").model_dump(),
            "status": DatasetPolicyStatus.ACTIVE,
        }
    )
    slot = snapshot.expected_slots(Timeframe.ONE_DAY)[0]
    stream = _stream(
        timeframe=Timeframe.ONE_DAY,
        provider="databento",
        dataset="opra.pillar",
    )
    segment = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="ephemeral",
        policy=policy,
    )

    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slot.start_utc,
        end=slot.end_utc,
        coverage=(segment,),
        policy=policy,
    )

    assert result.policy_eligibility.reason is WatermarkPolicyReason.DATASET_EPHEMERAL
    assert result.contiguous_slot_count == 0
    assert result.candidate is None
    assert result.invalid_coverage_ids == (segment.coverage_id,)


def test_alpaca_frontier_enforces_strict_age_plus_finalization_boundary() -> None:
    evaluated_at = datetime(2026, 8, 31, 20, 16, tzinfo=UTC)
    snapshot = CalendarSnapshot.create(
        library_name="exchange_calendars",
        library_version="test",
        tzdata_version="test",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=date(2026, 8, 31),
        range_end=date(2026, 9, 1),
        generated_at=evaluated_at,
        sessions=(
            CalendarSession(
                session_date=date(2026, 8, 31),
                open_utc=datetime(2026, 8, 31, 13, 30, tzinfo=UTC),
                close_utc=datetime(2026, 8, 31, 20, 0, tzinfo=UTC),
            ),
        ),
    )
    stream = _stream(provider="alpaca", dataset="price_bars_sip")
    policy = _policy("alpaca", "price_bars_sip")
    slot = snapshot.expected_slots(Timeframe.FIVE_MINUTES)[-1]
    segment = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="alpaca-strict-age",
        policy=policy,
        verified_at=evaluated_at - timedelta(seconds=1),
    )

    at_boundary = reconstruct_frontier(
        stream=stream,
        calendar_snapshot=snapshot,
        calendar_snapshot_id=_CALENDAR_ID,
        policy=policy,
        policy_snapshot_id=_POLICY_SNAPSHOT_ID,
        runtime_status=None,
        coverage_start=slot.start_utc,
        domain_end=slot.end_utc,
        coverage=(segment,),
        evaluated_at=evaluated_at,
    )
    after_boundary = reconstruct_frontier(
        stream=stream,
        calendar_snapshot=snapshot,
        calendar_snapshot_id=_CALENDAR_ID,
        policy=policy,
        policy_snapshot_id=_POLICY_SNAPSHOT_ID,
        runtime_status=None,
        coverage_start=slot.start_utc,
        domain_end=slot.end_utc,
        coverage=(segment,),
        evaluated_at=evaluated_at + timedelta(seconds=1),
    )

    # Alpaca requires >15m plus its 60s finalization buffer: equality is denied.
    assert at_boundary.candidate is None
    assert after_boundary.candidate is not None


def _ttl_policy() -> DatasetRetentionPolicy:
    layer = LayerRetentionPolicy(mode=RetentionMode.TTL, ttl_seconds=3600)
    return DatasetRetentionPolicy(
        policy_id="ttl-test-policy",
        revision=1,
        provider="synthetic_ttl",
        dataset="price_bars",
        mode=RetentionMode.TTL,
        status=DatasetPolicyStatus.ACTIVE,
        permitted_environments=(RuntimeEnvironment.TEST,),
        use_scope="Deterministic TTL test.",
        processing_allowed=True,
        raw=layer,
        normalized=layer,
        derived=layer,
        evidence_reference="synthetic-test",
        verified_on=date(2026, 8, 31),
        notes="Synthetic test policy.",
    )


def test_ttl_and_subscription_policy_state_is_fail_closed() -> None:
    ttl_policy = _ttl_policy()
    ttl_stream = _stream(provider="synthetic_ttl", dataset="price_bars")
    missing = assess_watermark_policy(
        ttl_stream,
        ttl_policy,
        runtime_status=None,
        evaluated_at=_NOW,
    )
    expired_status = DatasetRuntimeStatus.for_policy(
        ttl_policy,
        retention_started_at=_NOW - timedelta(hours=1),
        expires_at=_NOW,
    )
    expired = assess_watermark_policy(
        ttl_stream,
        ttl_policy,
        runtime_status=expired_status,
        evaluated_at=_NOW,
    )

    subscription = DatasetRetentionPolicy.model_validate(
        {
            **_policy("twelve_data", "price_bars_us_daily").model_dump(),
            "status": DatasetPolicyStatus.ACTIVE,
        }
    )
    subscription_stream = _stream(
        provider="twelve_data",
        dataset="price_bars_us_daily",
    )
    inactive = assess_watermark_policy(
        subscription_stream,
        subscription,
        runtime_status=DatasetRuntimeStatus.for_policy(
            subscription,
            entitlement_active=False,
        ),
        evaluated_at=_NOW,
    )

    assert missing.reason is WatermarkPolicyReason.RUNTIME_STATUS_REQUIRED
    assert expired.reason is WatermarkPolicyReason.RUNTIME_STATUS_EXPIRED
    assert inactive.reason is WatermarkPolicyReason.SUBSCRIPTION_INACTIVE


def test_purge_invalidation_removes_frontier_before_artifact_deletion() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slot = snapshot.expected_slots(Timeframe.ONE_DAY)[0]
    segment = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="purge",
    )
    before = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slot.start_utc,
        end=slot.end_utc,
        coverage=(segment,),
    )
    invalidated = invalidate_coverage(
        (segment,),
        CoverageInvalidation(
            coverage_ids=(segment.coverage_id,),
            reason=CoverageInvalidationReason.PURGED,
            invalidated_at=_NOW + timedelta(seconds=1),
        ),
    )
    after = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slot.start_utc,
        end=slot.end_utc,
        coverage=invalidated,
    )

    assert before.candidate is not None
    assert invalidated[0].verification_state is CoverageVerificationState.INVALID
    assert invalidated[0].retained is False
    # State becomes unusable first; physical deletion and its presence update follow.
    assert invalidated[0].artifacts_present is True
    assert after.candidate is None
    assert after.invalid_coverage_ids == (segment.coverage_id,)


def test_calendar_change_marks_coverage_stale_and_prevents_reuse() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slot = snapshot.expected_slots(Timeframe.ONE_DAY)[0]
    segment = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="calendar-stale",
    )
    stale = invalidate_coverage(
        (segment,),
        CoverageInvalidation(
            coverage_ids=(segment.coverage_id,),
            reason=CoverageInvalidationReason.CALENDAR_CHANGED,
            invalidated_at=_NOW + timedelta(seconds=1),
        ),
    )
    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slot.start_utc,
        end=slot.end_utc,
        coverage=stale,
    )

    assert stale[0].verification_state is CoverageVerificationState.STALE
    assert stale[0].retained is True
    assert stale[0].invalidated_at == _NOW + timedelta(seconds=1)
    assert result.candidate is None


def test_materialized_watermark_uses_explicit_generation_and_supporting_batch() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slot = snapshot.expected_slots(Timeframe.ONE_DAY)[0]
    segment = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="materialize",
    )
    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slot.start_utc,
        end=slot.end_utc,
        coverage=(segment,),
    )
    assert result.candidate is not None
    watermark = materialize_watermark(
        result.candidate,
        WatermarkChangeContext(
            generation=4,
            last_run_id="run-materialize",
            last_batch_id=segment.canonical_batch_id,
            computed_at=_NOW,
        ),
    )

    assert watermark.exclusive_frontier == slot.end_utc
    assert watermark.generation == 4
    assert watermark.last_verified_session == slot.session_date

    invalid = invalidate_materialized_watermark(
        watermark,
        reason=CoverageInvalidationReason.PURGED,
        generation=5,
        last_run_id="run-purge",
        invalidated_at=_NOW + timedelta(seconds=1),
    )
    assert invalid.verification_state is CoverageVerificationState.INVALID
    assert invalid.invalidated_at == _NOW + timedelta(seconds=1)
    assert invalid.exclusive_frontier == watermark.exclusive_frontier

    with pytest.raises(CoverageDomainError, match="does not support"):
        materialize_watermark(
            result.candidate,
            WatermarkChangeContext(
                generation=5,
                last_run_id="run-other",
                last_batch_id="batch-unrelated",
                computed_at=_NOW,
            ),
        )


def test_gap_lifecycle_is_explicit_and_terminal_states_do_not_reopen() -> None:
    stream = _stream()
    gap = GapFinding(
        gap_id="gap-lifecycle",
        stream_id=stream.stream_id,
        start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        end=datetime(2025, 7, 2, 13, 35, tzinfo=UTC),
        gap_type=GapType.EXPECTED_OBSERVATION,
        status=GapStatus.OPEN,
        blocking=True,
        detected_at=_NOW,
    )
    repairing = transition_gap(
        gap,
        GapStatus.REPAIRING,
        transitioned_at=_NOW + timedelta(seconds=1),
    )
    resolved = transition_gap(
        repairing,
        GapStatus.RESOLVED,
        transitioned_at=_NOW + timedelta(seconds=2),
    )

    assert repairing.resolved_at is None
    assert resolved.resolved_at == _NOW + timedelta(seconds=2)
    assert resolved.actively_blocks is False
    with pytest.raises(GapTransitionError, match="cannot transition"):
        transition_gap(
            resolved,
            GapStatus.OPEN,
            transitioned_at=_NOW + timedelta(seconds=3),
        )


def test_acquisition_and_integrity_gaps_cannot_be_declared_nonblocking() -> None:
    stream = _stream()
    values = {
        "gap_id": "gap-must-block",
        "stream_id": stream.stream_id,
        "start": datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        "end": datetime(2025, 7, 2, 13, 35, tzinfo=UTC),
        "status": GapStatus.OPEN,
        "blocking": False,
        "detected_at": _NOW,
    }

    for gap_type in (GapType.ACQUISITION, GapType.INTEGRITY):
        with pytest.raises(ValidationError, match="must block"):
            GapFinding.model_validate({**values, "gap_type": gap_type})


def test_segment_cannot_claim_many_slots_with_one_observation() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slots = snapshot.expected_slots(Timeframe.ONE_DAY)
    segment = _segment(
        stream,
        snapshot,
        slots[0].start_utc,
        slots[1].end_utc,
        suffix="under-counted",
        row_count=1,
    )

    with pytest.raises(CoverageDomainError, match="exactly one canonical row"):
        _evaluate(
            stream=stream,
            snapshot=snapshot,
            start=slots[0].start_utc,
            end=slots[1].end_utc,
            coverage=(segment,),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("stream_outcome", CoverageStreamOutcome.BLOCKED),
        ("terminal_page_verified", False),
        ("canonical_batch_verified", False),
        ("canonical_file_count", 0),
        ("raw_artifact_count", 0),
        ("relational_provenance_verified", False),
        ("artifact_integrity_verified", False),
    ),
)
def test_incomplete_raw_canonical_or_relational_proof_cannot_advance(
    field: str,
    value: object,
) -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slot = snapshot.expected_slots(Timeframe.ONE_DAY)[0]
    baseline = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix=f"weak-proof-{field}",
    )
    segment = CoverageSegment.model_validate({**baseline.model_dump(), field: value})

    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slot.start_utc,
        end=slot.end_utc,
        coverage=(segment,),
    )

    assert result.candidate is None
    assert result.invalid_coverage_ids == (segment.coverage_id,)


def test_future_verification_evidence_is_not_usable() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slot = snapshot.expected_slots(Timeframe.ONE_DAY)[0]
    segment = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="future-proof",
        verified_at=_NOW + timedelta(seconds=1),
    )

    result = _evaluate(
        stream=stream,
        snapshot=snapshot,
        start=slot.start_utc,
        end=slot.end_utc,
        coverage=(segment,),
    )

    assert result.candidate is None
    assert result.invalid_coverage_ids == (segment.coverage_id,)


def test_ttl_candidate_cannot_be_materialized_at_expiry_boundary() -> None:
    candidate = WatermarkCandidate(
        stream_id=_stream().stream_id,
        coverage_start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        exclusive_frontier=datetime(2025, 7, 2, 13, 35, tzinfo=UTC),
        verification_state=CoverageVerificationState.VERIFIED,
        calendar_snapshot_id=_CALENDAR_ID,
        policy_snapshot_id=_POLICY_SNAPSHOT_ID,
        last_verified_session=date(2025, 7, 2),
        blocking_gap_count=0,
        supporting_coverage_ids=("coverage-ttl",),
        supporting_batch_ids=("batch-ttl",),
        evidence_verified_at=_NOW,
        policy_evaluated_at=_NOW,
        policy_valid_until=_NOW + timedelta(seconds=5),
        observation_eligible_before=_NOW,
    )

    with pytest.raises(CoverageDomainError, match="expired before materialization"):
        materialize_watermark(
            candidate,
            WatermarkChangeContext(
                generation=1,
                last_run_id="run-ttl-expired",
                last_batch_id="batch-ttl",
                computed_at=_NOW + timedelta(seconds=5),
            ),
        )


def test_invalid_coverage_never_regresses_to_stale() -> None:
    snapshot = _snapshot()
    stream = _stream(timeframe=Timeframe.ONE_DAY)
    slot = snapshot.expected_slots(Timeframe.ONE_DAY)[0]
    segment = _segment(
        stream,
        snapshot,
        slot.start_utc,
        slot.end_utc,
        suffix="monotonic-invalid",
    )
    invalid = invalidate_coverage(
        (segment,),
        CoverageInvalidation(
            coverage_ids=(segment.coverage_id,),
            reason=CoverageInvalidationReason.PURGED,
            invalidated_at=_NOW + timedelta(seconds=1),
        ),
    )
    still_invalid = invalidate_coverage(
        invalid,
        CoverageInvalidation(
            coverage_ids=(segment.coverage_id,),
            reason=CoverageInvalidationReason.CALENDAR_CHANGED,
            invalidated_at=_NOW + timedelta(seconds=2),
        ),
    )

    assert still_invalid[0].verification_state is CoverageVerificationState.INVALID
    assert still_invalid[0].invalidated_at == _NOW + timedelta(seconds=1)
