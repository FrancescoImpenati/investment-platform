"""Offline tests for deterministic, provenance-preserving provider comparisons."""

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from investment_platform.data.models import (
    AdjustmentState,
    DividendAction,
    PriceBar,
    SplitAction,
    TickerChangeAction,
    Timeframe,
    TradingSession,
)
from investment_platform.data.provenance import (
    BytesRawPayload,
    DataSource,
    LicenseClassification,
    RawBatch,
    RawBatchMetadata,
)
from investment_platform.data.quality import (
    ActionComparisonTolerance,
    ComparisonMetric,
    ComparisonTolerance,
    DiscrepancyClassification,
    ObservationKey,
    compare_provider_actions,
    compare_provider_bars,
    summarize_provider_runtime,
)

pytestmark = pytest.mark.unit

_INSTRUMENT_ID = UUID("10000000-0000-4000-8000-000000000001")
_LEFT_SOURCE_ID = UUID("20000000-0000-4000-8000-000000000001")
_RIGHT_SOURCE_ID = UUID("30000000-0000-4000-8000-000000000001")
_LEFT_BATCH_ID = UUID("40000000-0000-4000-8000-000000000001")
_RIGHT_BATCH_ID = UUID("50000000-0000-4000-8000-000000000001")
_START = datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
_RETRIEVED = datetime(2025, 7, 3, 12, tzinfo=UTC)


def _bar(*, side: str, **overrides: object) -> PriceBar:
    values: dict[str, object] = {
        "instrument_id": _INSTRUMENT_ID,
        "timeframe": Timeframe.FIVE_MINUTES,
        "timestamp_start": _START,
        "timestamp_end": _START + timedelta(minutes=5),
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1_000.0,
        "vwap": 100.25,
        "currency": "USD",
        "session": TradingSession.REGULAR,
        "adjustment_state": AdjustmentState.UNADJUSTED,
        "source_id": _LEFT_SOURCE_ID if side == "left" else _RIGHT_SOURCE_ID,
        "raw_batch_id": _LEFT_BATCH_ID if side == "left" else _RIGHT_BATCH_ID,
        "retrieved_at": _RETRIEVED,
        "ingested_at": _RETRIEVED + timedelta(seconds=1),
    }
    values.update(overrides)
    return PriceBar.model_validate(values)


def _split(*, side: str, **overrides: object) -> SplitAction:
    values: dict[str, object] = {
        "instrument_id": _INSTRUMENT_ID,
        "effective_date": date(2025, 8, 28),
        "split_ratio": Decimal("4"),
        "source_id": _LEFT_SOURCE_ID if side == "left" else _RIGHT_SOURCE_ID,
        "raw_batch_id": _LEFT_BATCH_ID if side == "left" else _RIGHT_BATCH_ID,
        "retrieved_at": _RETRIEVED,
        "ingested_at": _RETRIEVED + timedelta(seconds=1),
    }
    values.update(overrides)
    return SplitAction.model_validate(values)


def _dividend(*, side: str, **overrides: object) -> DividendAction:
    values: dict[str, object] = {
        "instrument_id": _INSTRUMENT_ID,
        "effective_date": date(2025, 8, 8),
        "amount": Decimal("0.25"),
        "currency": "USD",
        "source_id": _LEFT_SOURCE_ID if side == "left" else _RIGHT_SOURCE_ID,
        "raw_batch_id": _LEFT_BATCH_ID if side == "left" else _RIGHT_BATCH_ID,
        "retrieved_at": _RETRIEVED,
        "ingested_at": _RETRIEVED + timedelta(seconds=1),
    }
    values.update(overrides)
    return DividendAction.model_validate(values)


def _ticker_change(*, side: str, **overrides: object) -> TickerChangeAction:
    values: dict[str, object] = {
        "instrument_id": _INSTRUMENT_ID,
        "effective_date": date(2025, 1, 2),
        "old_ticker": "OLD",
        "new_ticker": "NEW",
        "source_id": _LEFT_SOURCE_ID if side == "left" else _RIGHT_SOURCE_ID,
        "raw_batch_id": _LEFT_BATCH_ID if side == "left" else _RIGHT_BATCH_ID,
        "retrieved_at": _RETRIEVED,
        "ingested_at": _RETRIEVED + timedelta(seconds=1),
    }
    values.update(overrides)
    return TickerChangeAction.model_validate(values)


def test_pairwise_comparison_preserves_unresolved_values_and_raw_provenance() -> None:
    left = _bar(side="left")
    right = _bar(side="right", close=100.75, volume=1_010.0)

    report = compare_provider_bars("massive", (left,), "alpaca_sip", (right,))

    assert report.left.observation_count == 1
    assert report.right.unique_key_count == 1
    assert [(finding.metric, finding.classification) for finding in report.discrepancies] == [
        (ComparisonMetric.CLOSE, DiscrepancyClassification.UNRESOLVED_DISCREPANCY),
        (ComparisonMetric.VOLUME, DiscrepancyClassification.UNRESOLVED_DISCREPANCY),
    ]
    assert report.discrepancies[0].left_raw_batch_ids == (_LEFT_BATCH_ID,)
    assert report.discrepancies[0].right_raw_batch_ids == (_RIGHT_BATCH_ID,)


def test_adjustment_and_timing_differences_qualify_numeric_discrepancies() -> None:
    left = _bar(side="left")
    adjusted = _bar(
        side="right",
        adjustment_state=AdjustmentState.SPLIT_ADJUSTED,
        close=10.05,
    )
    adjusted_report = compare_provider_bars("massive", (left,), "alpaca_sip", (adjusted,))

    assert any(
        finding.metric is ComparisonMetric.ADJUSTMENT
        and finding.classification is DiscrepancyClassification.ADJUSTMENT_DIFFERENCE
        for finding in adjusted_report.discrepancies
    )
    assert (
        next(
            finding
            for finding in adjusted_report.discrepancies
            if finding.metric is ComparisonMetric.CLOSE
        ).classification
        is DiscrepancyClassification.ADJUSTMENT_DIFFERENCE
    )

    shifted = _bar(
        side="right",
        timestamp_end=_START + timedelta(minutes=6),
        session=TradingSession.UNKNOWN,
        close=100.75,
    )
    timing_report = compare_provider_bars("massive", (left,), "alpaca_sip", (shifted,))
    assert {finding.metric: finding.classification for finding in timing_report.discrepancies}[
        ComparisonMetric.CLOSE
    ] is DiscrepancyClassification.TIMING_SESSION_DIFFERENCE


def test_shifted_five_minute_boundary_is_paired_conservatively_with_provenance() -> None:
    left = _bar(side="left")
    right = _bar(
        side="right",
        timestamp_start=_START + timedelta(minutes=1),
        timestamp_end=_START + timedelta(minutes=6),
    )
    expected = ObservationKey(_INSTRUMENT_ID, Timeframe.FIVE_MINUTES, _START)

    report = compare_provider_bars(
        "massive",
        (left,),
        "alpaca_sip",
        (right,),
        expected_keys=(expected,),
    )

    assert [(item.metric, item.classification) for item in report.discrepancies] == [
        (
            ComparisonMetric.INTERVAL_BOUNDARY,
            DiscrepancyClassification.TIMING_SESSION_DIFFERENCE,
        )
    ]
    finding = report.discrepancies[0]
    assert finding.left_raw_batch_ids == (_LEFT_BATCH_ID,)
    assert finding.right_raw_batch_ids == (_RIGHT_BATCH_ID,)
    assert report.left.missing_expected_count == 0
    assert report.right.missing_expected_count == 0


def test_timestamp_alignment_does_not_pair_ambiguous_or_distant_bars() -> None:
    left_at_1330 = _bar(side="left")
    left_at_1332 = _bar(
        side="left",
        timestamp_start=_START + timedelta(minutes=2),
        timestamp_end=_START + timedelta(minutes=7),
        raw_batch_id=UUID("40000000-0000-4000-8000-000000000002"),
    )
    right_at_1331 = _bar(
        side="right",
        timestamp_start=_START + timedelta(minutes=1),
        timestamp_end=_START + timedelta(minutes=6),
    )

    ambiguous = compare_provider_bars(
        "massive",
        (left_at_1330, left_at_1332),
        "alpaca_sip",
        (right_at_1331,),
    )
    assert len(ambiguous.discrepancies) == 3
    assert all(
        item.classification is DiscrepancyClassification.MISSING_OBSERVATION
        for item in ambiguous.discrepancies
    )

    right_at_1332 = _bar(
        side="right",
        timestamp_start=_START + timedelta(minutes=2),
        timestamp_end=_START + timedelta(minutes=7),
    )
    distant = compare_provider_bars("massive", (left_at_1330,), "alpaca_sip", (right_at_1332,))
    assert len(distant.discrepancies) == 2
    assert {item.metric for item in distant.discrepancies} == {ComparisonMetric.AVAILABILITY}


def test_observation_key_rejects_naive_time_and_normalizes_aware_time_to_utc() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        ObservationKey(
            _INSTRUMENT_ID,
            Timeframe.FIVE_MINUTES,
            datetime(2025, 7, 2, 15, 30),
        )

    key = ObservationKey(
        _INSTRUMENT_ID,
        Timeframe.FIVE_MINUTES,
        datetime(2025, 7, 2, 15, 30, tzinfo=timezone(timedelta(hours=2))),
    )
    assert key.timestamp_start == _START
    assert key.timestamp_start.tzinfo is UTC


def test_availability_and_duplicates_are_reported_without_selecting_a_row() -> None:
    left = _bar(side="left")
    duplicate = _bar(side="left", raw_batch_id=UUID("40000000-0000-4000-8000-000000000002"))
    expected_missing = ObservationKey(
        instrument_id=_INSTRUMENT_ID,
        timeframe=Timeframe.FIVE_MINUTES,
        timestamp_start=_START + timedelta(minutes=5),
    )

    report = compare_provider_bars(
        "massive",
        (left, duplicate),
        "alpaca_sip",
        (),
        expected_keys=(
            ObservationKey(_INSTRUMENT_ID, Timeframe.FIVE_MINUTES, _START),
            expected_missing,
        ),
    )

    assert report.left.duplicate_observation_count == 1
    assert report.left.missing_expected_count == 1
    assert report.right.missing_expected_count == 2
    assert [finding.metric for finding in report.discrepancies] == [
        ComparisonMetric.DUPLICATE_BAR,
        ComparisonMetric.AVAILABILITY,
    ]
    assert report.discrepancies[0].classification is DiscrepancyClassification.LIKELY_PROVIDER_ISSUE
    assert report.discrepancies[0].left_raw_batch_ids == (
        _LEFT_BATCH_ID,
        UUID("40000000-0000-4000-8000-000000000002"),
    )


def test_explicit_tolerances_control_numeric_findings() -> None:
    left = _bar(side="left")
    right = _bar(side="right", close=100.5001, volume=1_000.5)

    report = compare_provider_bars(
        "massive",
        (left,),
        "alpaca_sip",
        (right,),
        tolerance=ComparisonTolerance(
            price_absolute=0.001,
            price_relative=0,
            volume_absolute=1,
            volume_relative=0,
        ),
    )

    assert report.discrepancies == ()
    with pytest.raises(ValueError, match="finite and non-negative"):
        ComparisonTolerance(price_absolute=float("nan"))
    with pytest.raises(ValueError, match="must not exceed one minute"):
        ComparisonTolerance(five_minute_start_alignment=timedelta(seconds=61))


def test_split_comparison_reports_ratio_and_bounded_date_semantics_with_provenance() -> None:
    left = _split(side="left")
    right = _split(
        side="right",
        effective_date=date(2025, 8, 29),
        split_ratio=Decimal("5"),
    )

    report = compare_provider_actions("massive", (left,), "alpaca_sip", (right,))

    assert [(item.metric, item.classification) for item in report.discrepancies] == [
        (
            ComparisonMetric.EFFECTIVE_DATE,
            DiscrepancyClassification.UNRESOLVED_DISCREPANCY,
        ),
        (
            ComparisonMetric.SPLIT_RATIO,
            DiscrepancyClassification.UNRESOLVED_DISCREPANCY,
        ),
    ]
    assert all(item.left_raw_batch_ids == (_LEFT_BATCH_ID,) for item in report.discrepancies)
    assert all(item.right_raw_batch_ids == (_RIGHT_BATCH_ID,) for item in report.discrepancies)
    assert report.left.observation_count == report.right.observation_count == 1


def test_dividend_comparison_distinguishes_ex_date_amount_and_currency() -> None:
    left = _dividend(side="left")
    right = _dividend(
        side="right",
        effective_date=date(2025, 8, 11),
        amount=Decimal("0.30"),
        currency="EUR",
    )

    report = compare_provider_actions("massive", (left,), "alpaca_sip", (right,))

    assert {item.metric: item.classification for item in report.discrepancies} == {
        ComparisonMetric.EFFECTIVE_DATE: DiscrepancyClassification.UNRESOLVED_DISCREPANCY,
        ComparisonMetric.DIVIDEND_AMOUNT: DiscrepancyClassification.UNRESOLVED_DISCREPANCY,
        ComparisonMetric.CURRENCY: DiscrepancyClassification.UNRESOLVED_DISCREPANCY,
    }


def test_action_comparison_reports_missing_and_duplicate_evidence_without_selection() -> None:
    duplicate_batch_id = UUID("40000000-0000-4000-8000-000000000002")
    split = _split(side="left")
    duplicate = _split(side="left", raw_batch_id=duplicate_batch_id)
    right_dividend = _dividend(side="right")

    report = compare_provider_actions(
        "massive",
        (split, duplicate),
        "alpaca_sip",
        (right_dividend,),
    )

    assert report.left.duplicate_observation_count == 1
    assert {item.metric for item in report.discrepancies} == {
        ComparisonMetric.DUPLICATE_ACTION,
        ComparisonMetric.AVAILABILITY,
    }
    duplicate_finding = next(
        item for item in report.discrepancies if item.metric is ComparisonMetric.DUPLICATE_ACTION
    )
    assert duplicate_finding.classification is DiscrepancyClassification.LIKELY_PROVIDER_ISSUE
    assert duplicate_finding.left_raw_batch_ids == (_LEFT_BATCH_ID, duplicate_batch_id)
    missing = next(
        item for item in report.discrepancies if item.metric is ComparisonMetric.AVAILABILITY
    )
    assert missing.classification is DiscrepancyClassification.MISSING_OBSERVATION
    assert missing.right_raw_batch_ids == (_RIGHT_BATCH_ID,)


def test_ticker_change_comparison_keeps_identifier_disagreement_and_provenance() -> None:
    left = _ticker_change(side="left")
    right = _ticker_change(side="right", old_ticker="LEGACY", new_ticker="CURRENT")

    report = compare_provider_actions("massive", (left,), "alpaca_sip", (right,))

    assert [(item.metric, item.left_value, item.right_value) for item in report.discrepancies] == [
        (ComparisonMetric.OLD_TICKER, "OLD", "LEGACY"),
        (ComparisonMetric.NEW_TICKER, "NEW", "CURRENT"),
    ]
    assert all(
        item.classification is DiscrepancyClassification.DEFINITIONAL_DIFFERENCE
        for item in report.discrepancies
    )
    assert report.discrepancies[0].left_raw_batch_ids == (_LEFT_BATCH_ID,)
    assert report.discrepancies[0].right_raw_batch_ids == (_RIGHT_BATCH_ID,)


def test_action_date_alignment_is_bounded_and_ambiguous_matches_remain_missing() -> None:
    with pytest.raises(ValueError, match="between zero and seven days"):
        ActionComparisonTolerance(effective_date_alignment_days=8)

    left = _dividend(side="left")
    distant = _dividend(side="right", effective_date=date(2025, 8, 18))
    report = compare_provider_actions("massive", (left,), "alpaca_sip", (distant,))
    assert len(report.discrepancies) == 2
    assert all(
        item.classification is DiscrepancyClassification.MISSING_OBSERVATION
        for item in report.discrepancies
    )

    second_left = _dividend(
        side="left",
        effective_date=date(2025, 8, 10),
        raw_batch_id=UUID("40000000-0000-4000-8000-000000000002"),
    )
    middle_right = _dividend(side="right", effective_date=date(2025, 8, 9))
    ambiguous = compare_provider_actions(
        "massive",
        (left, second_left),
        "alpaca_sip",
        (middle_right,),
    )
    assert len(ambiguous.discrepancies) == 3
    assert all(
        item.classification is DiscrepancyClassification.MISSING_OBSERVATION
        for item in ambiguous.discrepancies
    )


def test_runtime_summary_uses_only_sanitized_manifest_metrics() -> None:
    source = DataSource(
        source_id=_LEFT_SOURCE_ID,
        provider="massive",
        dataset="price_bars",
        logical_endpoint="v2/aggs/ticker/range",
        license_classification=LicenseClassification.SYNTHETIC,
    )
    batches = tuple(
        RawBatch(
            metadata=RawBatchMetadata(
                batch_id=batch_id,
                source=source,
                retrieved_at=_RETRIEVED,
                media_type="application/json",
                file_extension="json",
                request_metadata={
                    "response_status": 200,
                    "latency_ms": latency,
                    "page_number": page,
                    "rate_limit_capacity": 5,
                    "rate_limit_remaining": 5 - page,
                },
            ),
            payload=BytesRawPayload(b"{}"),
        )
        for page, (batch_id, latency) in enumerate(
            ((_LEFT_BATCH_ID, 12.5), (UUID("40000000-0000-4000-8000-000000000002"), 7.5)),
            start=1,
        )
    )

    summary = summarize_provider_runtime("massive", batches)

    assert summary.batch_count == 2
    assert summary.datasets == ("price_bars",)
    assert summary.observed_status_codes == (200,)
    assert summary.latency_sample_count == 2
    assert summary.total_latency_ms == 20.0
    assert summary.maximum_latency_ms == 12.5
    assert summary.observed_rate_limit_capacities == (5.0,)
    assert summary.minimum_rate_limit_remaining == 3.0

    with pytest.raises(ValueError, match="does not match"):
        summarize_provider_runtime("alpaca", batches)
