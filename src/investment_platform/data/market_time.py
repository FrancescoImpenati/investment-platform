"""Timezone helpers that deliberately stop short of a trading calendar."""

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

US_EASTERN = ZoneInfo("America/New_York")
_NOMINAL_RTH_OPEN = time(hour=9, minute=30)
_NOMINAL_RTH_CLOSE = time(hour=16)


def to_utc(value: datetime) -> datetime:
    """Normalize an aware datetime to UTC and reject naive values."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def nominal_us_rth_bounds(session_date: date) -> tuple[datetime, datetime]:
    """Return nominal 09:30-16:00 US/Eastern bounds in UTC.

    This helper performs timezone and DST conversion only. It is not an exchange
    calendar and must not be used to infer whether a session exists or whether it
    closes early.
    """

    start_local = datetime.combine(session_date, _NOMINAL_RTH_OPEN, tzinfo=US_EASTERN)
    end_local = datetime.combine(session_date, _NOMINAL_RTH_CLOSE, tzinfo=US_EASTERN)
    return to_utc(start_local), to_utc(end_local)
