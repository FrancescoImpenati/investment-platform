"""Tests for explicit timezone handling without a trading calendar."""

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from investment_platform.data.market_time import nominal_us_rth_bounds, to_utc

pytestmark = pytest.mark.unit


def test_to_utc_normalizes_aware_datetime() -> None:
    source = datetime(2026, 8, 14, 16, 45, tzinfo=timezone(timedelta(hours=2)))

    assert to_utc(source) == datetime(2026, 8, 14, 14, 45, tzinfo=UTC)


def test_to_utc_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        to_utc(datetime(2026, 8, 14, 14, 45))


@pytest.mark.parametrize(
    ("session_date", "expected_open", "expected_close"),
    [
        (
            date(2026, 1, 15),
            datetime(2026, 1, 15, 14, 30, tzinfo=UTC),
            datetime(2026, 1, 15, 21, 0, tzinfo=UTC),
        ),
        (
            date(2026, 7, 15),
            datetime(2026, 7, 15, 13, 30, tzinfo=UTC),
            datetime(2026, 7, 15, 20, 0, tzinfo=UTC),
        ),
    ],
)
def test_nominal_us_rth_bounds_follow_new_york_dst(
    session_date: date,
    expected_open: datetime,
    expected_close: datetime,
) -> None:
    assert nominal_us_rth_bounds(session_date) == (expected_open, expected_close)


def test_nominal_bounds_are_clock_conversion_not_calendar_validation() -> None:
    weekend_open, weekend_close = nominal_us_rth_bounds(date(2026, 7, 18))

    assert weekend_open.tzinfo is UTC
    assert weekend_close - weekend_open == timedelta(hours=6, minutes=30)
