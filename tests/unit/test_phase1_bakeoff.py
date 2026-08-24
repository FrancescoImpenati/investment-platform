"""Deterministic tests for the finite Phase 1 bake-off runner."""

from __future__ import annotations

from dataclasses import fields
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from uuid import UUID

import pytest

from investment_platform.data.models import Timeframe
from investment_platform.data.phase1_bakeoff import (
    PHASE1_SECURITIES,
    TWELVE_DATA_BASIC_BUDGET,
    Phase1BarSegment,
    SanitizedBatchPipelineMetrics,
    TwelveDataBasicPacer,
    aggregate_pipeline_metrics,
    assert_external_private_data_root,
    build_phase1_bar_windows,
    build_phase1_session_schedule,
    build_provider_bar_request_plan,
    expected_keys_by_segment,
    phase1_temporary_data_root,
    sanitize_comparison_report,
)
from investment_platform.data.quality import (
    BarComparisonReport,
    BarDiscrepancy,
    ComparisonMetric,
    DiscrepancyClassification,
    ObservationKey,
    ProviderBarMetrics,
)

pytestmark = pytest.mark.unit


def test_frozen_sample_has_sixteen_stable_identifiers_and_yahoo_punctuation() -> None:
    assert len(PHASE1_SECURITIES) == 16
    assert len({item.instrument_id for item in PHASE1_SECURITIES}) == 16
    assert len({item.symbol for item in PHASE1_SECURITIES}) == 16
    berkshire = next(item for item in PHASE1_SECURITIES if item.symbol == "BRK.B")
    assert berkshire.provider_identifier("alpaca_sip") == "BRK.B"
    assert berkshire.provider_identifier("twelve-data") == "BRK.B"
    assert berkshire.provider_identifier("yfinance") == "BRK-B"
    with pytest.raises(ValueError, match="unsupported Phase 1 provider"):
        berkshire.provider_identifier("unknown")


def test_finite_schedule_contains_only_frozen_sessions_dst_and_early_closes() -> None:
    schedule = build_phase1_session_schedule()

    assert len(schedule.sessions) == 148
    assert schedule.bounds_for(date(2025, 1, 20)) is None
    assert schedule.bounds_for(date(2025, 7, 4)) is None
    assert schedule.bounds_for(date(2024, 7, 3)) is None
    july_early_close = schedule.bounds_for(date(2025, 7, 3))
    november_early_close = schedule.bounds_for(date(2025, 11, 28))
    before_dst = schedule.bounds_for(date(2025, 3, 7))
    after_dst = schedule.bounds_for(date(2025, 3, 10))
    assert july_early_close is not None
    assert november_early_close is not None
    assert before_dst is not None
    assert after_dst is not None
    assert july_early_close.end == datetime(2025, 7, 3, 17, tzinfo=UTC)
    assert november_early_close.end == datetime(2025, 11, 28, 18, tzinfo=UTC)
    assert before_dst.start.hour == 14
    assert after_dst.start.hour == 13


def test_frozen_windows_and_expected_keys_match_preregistered_cardinalities() -> None:
    schedule = build_phase1_session_schedule()
    windows = build_phase1_bar_windows(schedule)
    expected = expected_keys_by_segment(windows, schedule)

    assert len(windows) == 22
    assert {segment: len(keys) for segment, keys in expected.items()} == {
        Phase1BarSegment.DAILY_UNADJUSTED: 2_160,
        Phase1BarSegment.DAILY_SPLIT_ADJUSTED: 2_160,
        Phase1BarSegment.CALENDAR_FIVE_MINUTE: 6_912,
        Phase1BarSegment.SPLIT_BOUNDARY_UNADJUSTED: 468,
        Phase1BarSegment.SPLIT_BOUNDARY_SPLIT_ADJUSTED: 468,
        Phase1BarSegment.TICKER_CONTINUITY: 11,
    }
    assert sum(len(keys) for keys in expected.values()) == 12_179


def test_provider_request_plans_separate_adjustments_and_basic_credit_costs() -> None:
    alpaca = build_provider_bar_request_plan("alpaca_sip")
    twelve_data = build_provider_bar_request_plan("twelve_data")

    assert len(alpaca) == 22
    assert len(twelve_data) == 30
    assert sum(item.credit_cost for item in twelve_data) == 142
    assert all(len(item.request.instruments) <= 8 for item in twelve_data)
    assert all(len(item.request.instruments) <= 16 for item in alpaca)
    assert TWELVE_DATA_BASIC_BUDGET.canonical_bar_requests == len(twelve_data)

    for provider_plan in (alpaca, twelve_data):
        by_segment: dict[Phase1BarSegment, set[object]] = {}
        for item in provider_plan:
            by_segment.setdefault(item.segment, set()).add(item.request.adjustment_state)
        assert len(by_segment[Phase1BarSegment.DAILY_UNADJUSTED]) == 1
        assert len(by_segment[Phase1BarSegment.DAILY_SPLIT_ADJUSTED]) == 1
        assert (
            by_segment[Phase1BarSegment.DAILY_UNADJUSTED]
            != by_segment[Phase1BarSegment.DAILY_SPLIT_ADJUSTED]
        )


class _FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_twelve_basic_pacer_is_a_finite_credit_gate_without_retries() -> None:
    fake = _FakeTime()
    pacer = TwelveDataBasicPacer(
        clock=fake.clock,
        sleeper=fake.sleep,
        safety_seconds=0.1,
    )

    pacer.before_request(8)
    pacer.before_request(1)

    assert pacer.credits_used == 9
    assert pacer.request_count == 2
    assert fake.sleeps == [pytest.approx(60.1)]

    budget_time = _FakeTime()
    budget = TwelveDataBasicPacer(
        clock=budget_time.clock,
        sleeper=budget_time.sleep,
        safety_seconds=0,
    )
    for _ in range(20):
        budget.before_request(8)
    with pytest.raises(RuntimeError, match="161-credit"):
        budget.before_request(2)
    with pytest.raises(ValueError, match="between one and eight"):
        budget.before_request(9)


def test_temporary_data_root_is_external_and_deleted(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    private_parent = tmp_path / "private"
    repository.mkdir()
    private_parent.mkdir()

    with phase1_temporary_data_root(
        repository,
        parent_directory=private_parent,
    ) as data_root:
        assert not data_root.is_relative_to(repository)
        (data_root / "ephemeral-marker").write_text("private", encoding="utf-8")
        retained_path = data_root

    assert not retained_path.exists()
    with pytest.raises(ValueError, match="outside the repository"):
        assert_external_private_data_root(repository / "data" / "raw", repository)


def _batch_metrics(*, latency_ms: float, rows: int) -> SanitizedBatchPipelineMetrics:
    return SanitizedBatchPipelineMetrics(
        provider="twelve_data",
        dataset="price_bars_standard_us",
        page_number=1,
        response_status=200,
        latency_ms=latency_ms,
        rate_limit_capacity=8,
        rate_limit_remaining=4,
        api_credits_used=1,
        raw_size_bytes=100,
        normalized_row_count=rows,
        normalization_issue_counts=(("provider_definition", 1),),
        validation_flag_counts=(),
        parquet_part_count=1,
        duckdb_row_count=rows,
        raw_artifact_pass=True,
        checksum_replay_pass=True,
        normalization_pass=True,
        canonical_validation_pass=True,
        parquet_pass=True,
        duckdb_query_pass=True,
    )


def test_pipeline_aggregation_is_sanitized_and_contains_no_provenance_fields() -> None:
    summary = aggregate_pipeline_metrics(
        "twelve_data",
        (_batch_metrics(latency_ms=10, rows=2), _batch_metrics(latency_ms=15, rows=3)),
    )

    assert summary.batch_count == 2
    assert summary.canonical_row_count == summary.duckdb_row_count == 5
    assert summary.normalization_issue_counts == (("provider_definition", 2),)
    assert summary.total_latency_ms == 25
    assert summary.maximum_latency_ms == 15
    assert summary.api_credit_usage_sample_count == 2
    assert summary.total_api_credits_used == 2
    assert summary.pipeline_pass is True
    names = {item.name for item in fields(summary)}
    assert not names.intersection({"batch_id", "sha256", "path", "payload", "url"})
    assert aggregate_pipeline_metrics("twelve_data", ()).pipeline_pass is False


def test_comparison_summary_drops_values_and_raw_batch_provenance() -> None:
    raw_batch_id = UUID("40000000-0000-4000-8000-000000000001")
    key = ObservationKey(
        UUID("10000000-0000-4000-8000-000000000001"),
        Timeframe.FIVE_MINUTES,
        datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
    )
    left = ProviderBarMetrics("twelve_data", 1, 1, 0, 1, 0)
    right = ProviderBarMetrics("alpaca_sip", 1, 1, 0, 1, 0)
    discrepancy = BarDiscrepancy(
        key=key,
        metric=ComparisonMetric.VOLUME,
        classification=DiscrepancyClassification.VENUE_FEED_DIFFERENCE,
        left_provider=left.provider,
        right_provider=right.provider,
        left_value=50.0,
        right_value=1_000.0,
        left_raw_batch_ids=(raw_batch_id,),
        right_raw_batch_ids=(),
    )
    report = BarComparisonReport(
        left=left,
        right=right,
        discrepancies=(discrepancy,),
        counts_by_classification=MappingProxyType(
            {DiscrepancyClassification.VENUE_FEED_DIFFERENCE: 1}
        ),
    )

    summary = sanitize_comparison_report(Phase1BarSegment.CALENDAR_FIVE_MINUTE, report)

    assert summary.discrepancy_counts_by_metric == (("volume", 1),)
    assert summary.discrepancy_counts_by_classification == (("venue_feed_difference", 1),)
    assert str(raw_batch_id) not in repr(summary)
    assert "1000.0" not in repr(summary)
    names = {item.name for item in fields(summary)}
    assert not names.intersection({"left_value", "right_value", "raw_batch_ids"})
