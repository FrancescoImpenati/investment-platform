"""Offline regressions for the maintained XNYS calendar boundary."""

from datetime import UTC, date, datetime, timedelta

import pytest
from pydantic import ValidationError

from investment_platform.data.calendar import (
    CalendarSession,
    CalendarSnapshot,
    TradingCalendar,
    XNYSCalendar,
)
from investment_platform.data.models import Timeframe

_GENERATED_AT = datetime(2026, 8, 31, 12, tzinfo=UTC)


def _snapshot(
    start: date, end: date, *, generated_at: datetime = _GENERATED_AT
) -> CalendarSnapshot:
    calendar: TradingCalendar = XNYSCalendar()
    return calendar.snapshot(start, end, generated_at=generated_at)


@pytest.mark.unit
def test_snapshot_range_is_half_open_and_closed_dates_have_no_sessions() -> None:
    snapshot = _snapshot(date(2025, 5, 27), date(2025, 12, 6))

    session_dates = {session.session_date for session in snapshot.sessions}
    assert len(session_dates) == 135
    assert {
        date(2025, 6, 19),
        date(2025, 7, 4),
        date(2025, 9, 1),
        date(2025, 11, 27),
        date(2025, 12, 6),
    }.isdisjoint(session_dates)
    assert snapshot.range_start == date(2025, 5, 27)
    assert snapshot.range_end == date(2025, 12, 6)


@pytest.mark.unit
def test_snapshot_can_represent_a_range_with_no_sessions() -> None:
    snapshot = _snapshot(date(2025, 7, 4), date(2025, 7, 5))

    assert snapshot.sessions == ()
    assert snapshot.expected_slots(Timeframe.ONE_DAY) == ()
    assert snapshot.expected_slots(Timeframe.FIVE_MINUTES) == ()


@pytest.mark.unit
def test_early_close_has_42_half_open_five_minute_slots() -> None:
    snapshot = _snapshot(date(2025, 7, 3), date(2025, 7, 4))
    (session,) = snapshot.sessions
    slots = snapshot.expected_slots(Timeframe.FIVE_MINUTES)

    assert snapshot.early_close_dates == (date(2025, 7, 3),)
    assert session.open_utc == datetime(2025, 7, 3, 13, 30, tzinfo=UTC)
    assert session.close_utc == datetime(2025, 7, 3, 17, 0, tzinfo=UTC)
    assert len(slots) == 42
    assert slots[0].start_utc == session.open_utc
    assert slots[-1].end_utc == session.close_utc
    assert all(slot.start_utc != session.close_utc for slot in slots)
    assert slots[-1].contains(session.close_utc - timedelta(microseconds=1))
    assert not slots[-1].contains(session.close_utc)
    assert session.contains(session.open_utc)
    assert not session.contains(session.close_utc)


@pytest.mark.unit
def test_ordinary_session_has_78_five_minute_slots_and_one_daily_slot() -> None:
    snapshot = _snapshot(date(2025, 7, 2), date(2025, 7, 3))
    (session,) = snapshot.sessions
    five_minute = snapshot.expected_slots(Timeframe.FIVE_MINUTES)
    daily = snapshot.expected_slots(Timeframe.ONE_DAY)

    assert session.is_early_close is False
    assert len(five_minute) == 78
    assert len(daily) == 1
    assert (daily[0].start_utc, daily[0].end_utc) == (
        session.open_utc,
        session.close_utc,
    )


@pytest.mark.unit
def test_xnys_session_bounds_follow_spring_and_autumn_dst() -> None:
    spring = _snapshot(date(2025, 3, 7), date(2025, 3, 11))
    autumn = _snapshot(date(2025, 10, 31), date(2025, 11, 4))

    spring_opens = {session.session_date: session.open_utc for session in spring.sessions}
    autumn_opens = {session.session_date: session.open_utc for session in autumn.sessions}
    assert spring_opens[date(2025, 3, 7)] == datetime(2025, 3, 7, 14, 30, tzinfo=UTC)
    assert spring_opens[date(2025, 3, 10)] == datetime(2025, 3, 10, 13, 30, tzinfo=UTC)
    assert autumn_opens[date(2025, 10, 31)] == datetime(2025, 10, 31, 13, 30, tzinfo=UTC)
    assert autumn_opens[date(2025, 11, 3)] == datetime(2025, 11, 3, 14, 30, tzinfo=UTC)


@pytest.mark.unit
def test_checksum_is_stable_across_generation_times_and_records_versions() -> None:
    first = _snapshot(date(2025, 7, 2), date(2025, 7, 4))
    second = _snapshot(
        date(2025, 7, 2),
        date(2025, 7, 4),
        generated_at=_GENERATED_AT + timedelta(days=1),
    )

    assert first.checksum == second.checksum
    assert first.diff(second).has_changes is False
    assert first.library_name == "exchange_calendars"
    assert first.library_version == "4.13.2"
    assert first.tzdata_version
    assert first.timezone_name == "America/New_York"


@pytest.mark.unit
def test_snapshot_diff_identifies_schedule_and_metadata_changes() -> None:
    source = _snapshot(date(2025, 7, 2), date(2025, 7, 3))
    (session,) = source.sessions
    revised_session = CalendarSession(
        session_date=session.session_date,
        open_utc=session.open_utc,
        close_utc=session.close_utc - timedelta(minutes=5),
        is_early_close=True,
    )
    target = CalendarSnapshot.create(
        library_name=source.library_name,
        library_version=f"{source.library_version}.revision",
        tzdata_version=source.tzdata_version,
        calendar_name=source.calendar_name,
        timezone_name=source.timezone_name,
        range_start=source.range_start,
        range_end=source.range_end,
        generated_at=source.generated_at,
        sessions=(revised_session,),
    )

    change = source.diff(target)
    assert change.has_changes is True
    assert change.metadata_changes == ("library_version",)
    assert change.affected_session_dates == (date(2025, 7, 2),)
    assert len(change.changed_sessions) == 1
    assert change.source_checksum != change.target_checksum


@pytest.mark.unit
def test_version_only_diff_does_not_claim_an_affected_session() -> None:
    source = _snapshot(date(2025, 7, 2), date(2025, 7, 3))
    target = CalendarSnapshot.create(
        library_name=source.library_name,
        library_version=f"{source.library_version}.revision",
        tzdata_version=source.tzdata_version,
        calendar_name=source.calendar_name,
        timezone_name=source.timezone_name,
        range_start=source.range_start,
        range_end=source.range_end,
        generated_at=source.generated_at,
        sessions=source.sessions,
    )

    change = source.diff(target)
    assert change.has_changes is True
    assert change.metadata_changes == ("library_version",)
    assert change.affected_session_dates == ()
    assert change.source_checksum == change.target_checksum


@pytest.mark.unit
def test_snapshot_and_session_contracts_reject_unsafe_boundaries() -> None:
    calendar = XNYSCalendar()
    with pytest.raises(ValueError, match="end_date must be later"):
        calendar.snapshot(date(2025, 7, 3), date(2025, 7, 3))

    with pytest.raises(ValidationError, match="timezone-aware"):
        CalendarSession(
            session_date=date(2025, 7, 2),
            open_utc=datetime(2025, 7, 2, 13, 30),
            close_utc=datetime(2025, 7, 2, 20),
        )
