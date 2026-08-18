"""Unit tests for non-destructive, vectorized bar validation."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import polars as pl
import pytest

from investment_platform.data.models import BarQualityFlag
from investment_platform.data.validation.bars import BarValidationPolicy, validate_bars


def _quality_frame() -> pl.DataFrame:
    start = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
    instrument = str(uuid4())
    source = str(uuid4())
    common = {
        "instrument_id": instrument,
        "timeframe": "5m",
        "timestamp_start": start,
        "timestamp_end": start + timedelta(minutes=5),
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "vwap": 10.2,
        "volume": 100.0,
        "session": "regular",
        "adjustment_state": "unadjusted",
        "source_id": source,
        "quality_flags": [],
        "sequence": 0,
    }
    duplicate_bad = {
        **common,
        "high": 9.0,
        "low": -1.0,
        "close": 8.0,
        "volume": -2.0,
        "sequence": 1,
    }
    wrong_duration = {
        **common,
        "instrument_id": str(uuid4()),
        "timestamp_start": start + timedelta(minutes=5),
        "timestamp_end": start + timedelta(minutes=9),
        "vwap": 0.0,
        "quality_flags": ["upstream_flag"],
        "sequence": 2,
    }
    return pl.DataFrame(
        [common, duplicate_bad, wrong_duration],
        schema_overrides={
            "timestamp_start": pl.Datetime("us", "UTC"),
            "timestamp_end": pl.Datetime("us", "UTC"),
            "quality_flags": pl.List(pl.String),
        },
    )


@pytest.mark.unit
def test_validation_flags_rows_without_dropping_or_reordering() -> None:
    frame = _quality_frame()

    result = validate_bars(frame)

    assert result.frame.height == frame.height
    assert result.frame.get_column("sequence").to_list() == [0, 1, 2]
    flags = result.frame.get_column("quality_flags").to_list()
    assert flags[0] == [BarQualityFlag.DUPLICATE_BAR.value]
    assert flags[1] == [
        BarQualityFlag.OHLC_INCONSISTENT.value,
        BarQualityFlag.NEGATIVE_VOLUME.value,
        BarQualityFlag.DUPLICATE_BAR.value,
        BarQualityFlag.NON_POSITIVE_PRICE.value,
    ]
    assert flags[2] == [
        "upstream_flag",
        BarQualityFlag.NON_POSITIVE_PRICE.value,
        BarQualityFlag.UNEXPECTED_DURATION.value,
    ]
    assert result.counts_by_flag[BarQualityFlag.DUPLICATE_BAR] == 2
    assert result.counts_by_flag[BarQualityFlag.UNEXPECTED_DURATION] == 1
    assert result.counts_by_flag[BarQualityFlag.NON_POSITIVE_PRICE] == 2
    assert [issue.row_index for issue in result.issues] == [0, 1, 1, 1, 1, 2, 2]


@pytest.mark.unit
def test_validation_is_idempotent_and_policy_can_disable_price_rule() -> None:
    first = validate_bars(_quality_frame())
    second = validate_bars(first.frame)

    assert (
        second.frame.get_column("quality_flags").to_list()
        == first.frame.get_column("quality_flags").to_list()
    )
    assert second.issues == first.issues

    without_price_rule = validate_bars(
        _quality_frame(),
        BarValidationPolicy(check_non_positive_prices=False),
    )
    assert without_price_rule.counts_by_flag[BarQualityFlag.NON_POSITIVE_PRICE] == 0
    assert (
        BarQualityFlag.NON_POSITIVE_PRICE.value
        not in without_price_rule.frame.row(1, named=True)["quality_flags"]
    )


@pytest.mark.unit
def test_validation_rejects_a_noncanonical_input_shape() -> None:
    with pytest.raises(ValueError, match="requires columns"):
        validate_bars(pl.DataFrame({"instrument_id": [str(uuid4())]}))

    frame = _quality_frame().with_columns(
        pl.Series(
            "quality_flags",
            [["  "], [], []],
            dtype=pl.List(pl.String),
        )
    )
    with pytest.raises(ValueError, match="non-empty"):
        validate_bars(frame)
