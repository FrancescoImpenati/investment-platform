"""Offline safety and evidence tests for the opt-in live Phase 1 entry point."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from investment_platform.data import phase1_bakeoff_live
from investment_platform.data.models import AdjustmentState, PriceBar, Timeframe, TradingSession
from investment_platform.data.phase1_bakeoff import Phase1BarSegment

pytestmark = pytest.mark.unit


def test_live_entry_point_does_not_emit_unexpected_exception_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_marker = "synthetic-secret-that-must-not-be-rendered"

    def fail(repository_root: Path, **kwargs: object) -> dict[str, object]:
        del repository_root, kwargs
        raise RuntimeError(secret_marker)

    monkeypatch.setattr(phase1_bakeoff_live, "run_live_bakeoff", fail)

    assert phase1_bakeoff_live.main() == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert secret_marker not in captured.err
    assert "RuntimeError" in captured.err
    assert "no exception details emitted" in captured.err


def test_live_entry_point_returns_failure_when_pipeline_acceptance_fails(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def failed_result(repository_root: Path, **kwargs: object) -> dict[str, object]:
        del repository_root, kwargs
        return {
            "pipeline_acceptance": {
                "alpaca_sip": "PASS",
                "twelve_data_basic": "FAIL",
                "comparison": "PASS",
            }
        }

    monkeypatch.setattr(phase1_bakeoff_live, "run_live_bakeoff", failed_result)

    assert phase1_bakeoff_live.main() == 1
    assert '"twelve_data_basic": "FAIL"' in capsys.readouterr().out


def test_reference_matching_rejects_cross_market_symbol_collisions() -> None:
    assert phase1_bakeoff_live._is_alpaca_us_equity(
        {"symbol": "XPH1", "asset_class": "us_equity"}, "XPH1"
    )
    assert not phase1_bakeoff_live._is_alpaca_us_equity(
        {"symbol": "XPH1", "asset_class": "crypto"}, "XPH1"
    )
    assert phase1_bakeoff_live._is_twelve_data_us_equity(
        {
            "symbol": "XPH1",
            "country": "United States",
            "instrument_type": "Common Stock",
        },
        "XPH1",
    )
    assert not phase1_bakeoff_live._is_twelve_data_us_equity(
        {
            "symbol": "XPH1",
            "country": "Canada",
            "instrument_type": "Common Stock",
        },
        "XPH1",
    )


def test_adjustment_summary_does_not_treat_two_missing_volumes_as_different() -> None:
    start = datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
    bar = PriceBar(
        instrument_id=UUID("10000000-0000-4000-8000-000000000001"),
        timeframe=Timeframe.FIVE_MINUTES,
        timestamp_start=start,
        timestamp_end=start + timedelta(minutes=5),
        open=100,
        high=101,
        low=99,
        close=100.5,
        volume=None,
        session=TradingSession.REGULAR,
        adjustment_state=AdjustmentState.UNADJUSTED,
        source_id=UUID("20000000-0000-4000-8000-000000000001"),
        raw_batch_id=UUID("30000000-0000-4000-8000-000000000001"),
        retrieved_at=datetime(2026, 8, 24, 12, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 24, 12, tzinfo=UTC),
    )
    adjusted = bar.model_copy(update={"adjustment_state": AdjustmentState.SPLIT_ADJUSTED})

    summary = phase1_bakeoff_live._adjustment_behavior(
        {
            Phase1BarSegment.SPLIT_BOUNDARY_UNADJUSTED: (bar,),
            Phase1BarSegment.SPLIT_BOUNDARY_SPLIT_ADJUSTED: (adjusted,),
        }
    )

    assert summary["split_boundary_5m"] == {"matched_rows": 1, "different_rows": 0}
