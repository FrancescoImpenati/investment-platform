"""Provider-neutral trading-calendar contracts and the Phase 2 XNYS adapter.

``exchange_calendars`` and its Pandas values are deliberately confined to this
module.  Public contracts expose only standard-library values, canonical
``Timeframe`` members, and immutable Pydantic models.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version
from typing import Any, Protocol, Self

import exchange_calendars as exchange_calendars  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.market_time import US_EASTERN, to_utc
from investment_platform.data.models import Timeframe

_FIVE_MINUTES = timedelta(minutes=5)
_CHECKSUM_PATTERN = r"^sha256:[0-9a-f]{64}$"


class _FrozenCalendarModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class CalendarSession(_FrozenCalendarModel):
    """One exchange session with a New York session date and UTC bounds."""

    session_date: date
    open_utc: datetime
    close_utc: datetime
    is_early_close: bool = False

    @field_validator("open_utc", "close_utc", mode="after")
    @classmethod
    def normalize_bounds(cls, value: datetime) -> datetime:
        return to_utc(value)

    @model_validator(mode="after")
    def validate_session(self) -> Self:
        if self.close_utc <= self.open_utc:
            raise ValueError("close_utc must be later than open_utc")
        if self.open_utc.astimezone(US_EASTERN).date() != self.session_date:
            raise ValueError("open_utc does not belong to session_date in America/New_York")
        if self.close_utc.astimezone(US_EASTERN).date() != self.session_date:
            raise ValueError("close_utc does not belong to session_date in America/New_York")
        return self

    def contains(self, value: datetime) -> bool:
        """Return whether ``value`` is inside the half-open session interval."""

        instant = to_utc(value)
        return self.open_utc <= instant < self.close_utc


class ExpectedCalendarSlot(_FrozenCalendarModel):
    """A calendar-eligible interval; it is not proof that an instrument traded."""

    session_date: date
    timeframe: Timeframe
    start_utc: datetime
    end_utc: datetime

    @field_validator("start_utc", "end_utc", mode="after")
    @classmethod
    def normalize_bounds(cls, value: datetime) -> datetime:
        return to_utc(value)

    @model_validator(mode="after")
    def validate_slot(self) -> Self:
        if self.end_utc <= self.start_utc:
            raise ValueError("end_utc must be later than start_utc")
        if self.start_utc.astimezone(US_EASTERN).date() != self.session_date:
            raise ValueError("start_utc does not belong to session_date in America/New_York")
        if self.end_utc.astimezone(US_EASTERN).date() != self.session_date:
            raise ValueError("end_utc does not belong to session_date in America/New_York")
        if (
            self.timeframe is Timeframe.FIVE_MINUTES
            and self.end_utc - self.start_utc != _FIVE_MINUTES
        ):
            raise ValueError("a 5m expected slot must be exactly five minutes")
        return self

    def contains(self, value: datetime) -> bool:
        """Return whether ``value`` is inside the half-open slot interval."""

        instant = to_utc(value)
        return self.start_utc <= instant < self.end_utc


class CalendarSessionChange(_FrozenCalendarModel):
    """A changed session shared by two calendar snapshots."""

    session_date: date
    before: CalendarSession
    after: CalendarSession


class CalendarSnapshotDiff(_FrozenCalendarModel):
    """Structural and version metadata changes between two snapshots."""

    source_checksum: str = Field(pattern=_CHECKSUM_PATTERN)
    target_checksum: str = Field(pattern=_CHECKSUM_PATTERN)
    metadata_changes: tuple[str, ...] = ()
    added_sessions: tuple[CalendarSession, ...] = ()
    removed_sessions: tuple[CalendarSession, ...] = ()
    changed_sessions: tuple[CalendarSessionChange, ...] = ()

    @property
    def affected_session_dates(self) -> tuple[date, ...]:
        """Return session dates whose expected schedule changed."""

        dates = {
            *(session.session_date for session in self.added_sessions),
            *(session.session_date for session in self.removed_sessions),
            *(change.session_date for change in self.changed_sessions),
        }
        return tuple(sorted(dates))

    @property
    def has_changes(self) -> bool:
        """Return whether schedule or versioned metadata differs."""

        return bool(
            self.metadata_changes
            or self.added_sessions
            or self.removed_sessions
            or self.changed_sessions
        )


class CalendarSnapshot(_FrozenCalendarModel):
    """Immutable, versioned schedule used to establish calendar eligibility."""

    library_name: str = Field(min_length=1)
    library_version: str = Field(min_length=1)
    tzdata_version: str = Field(min_length=1)
    calendar_name: str = Field(min_length=1)
    timezone_name: str = Field(min_length=1)
    range_start: date
    range_end: date
    generated_at: datetime
    sessions: tuple[CalendarSession, ...]
    checksum: str = Field(pattern=_CHECKSUM_PATTERN)

    @field_validator("generated_at", mode="after")
    @classmethod
    def normalize_generated_at(cls, value: datetime) -> datetime:
        return to_utc(value)

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        if self.range_end <= self.range_start:
            raise ValueError("range_end must be later than range_start")

        session_dates = tuple(session.session_date for session in self.sessions)
        if session_dates != tuple(sorted(set(session_dates))):
            raise ValueError("sessions must be unique and ordered by session_date")
        if any(not self.range_start <= value < self.range_end for value in session_dates):
            raise ValueError("every session must fall inside [range_start, range_end)")

        expected = _schedule_checksum(
            calendar_name=self.calendar_name,
            timezone_name=self.timezone_name,
            range_start=self.range_start,
            range_end=self.range_end,
            sessions=self.sessions,
        )
        if self.checksum != expected:
            raise ValueError("checksum does not match the canonical schedule")
        return self

    @classmethod
    def create(
        cls,
        *,
        library_name: str,
        library_version: str,
        tzdata_version: str,
        calendar_name: str,
        timezone_name: str,
        range_start: date,
        range_end: date,
        generated_at: datetime,
        sessions: Iterable[CalendarSession],
    ) -> Self:
        """Create a snapshot and derive its deterministic schedule checksum."""

        ordered_sessions = tuple(sessions)
        checksum = _schedule_checksum(
            calendar_name=calendar_name,
            timezone_name=timezone_name,
            range_start=range_start,
            range_end=range_end,
            sessions=ordered_sessions,
        )
        return cls(
            library_name=library_name,
            library_version=library_version,
            tzdata_version=tzdata_version,
            calendar_name=calendar_name,
            timezone_name=timezone_name,
            range_start=range_start,
            range_end=range_end,
            generated_at=generated_at,
            sessions=ordered_sessions,
            checksum=checksum,
        )

    @property
    def early_close_dates(self) -> tuple[date, ...]:
        """Return the early-close dates recorded in this snapshot."""

        return tuple(session.session_date for session in self.sessions if session.is_early_close)

    def expected_slots(self, timeframe: Timeframe) -> tuple[ExpectedCalendarSlot, ...]:
        """Return ordered eligible 1d sessions or complete 5m RTH intervals."""

        if timeframe is Timeframe.ONE_DAY:
            return tuple(
                ExpectedCalendarSlot(
                    session_date=session.session_date,
                    timeframe=timeframe,
                    start_utc=session.open_utc,
                    end_utc=session.close_utc,
                )
                for session in self.sessions
            )

        if timeframe is not Timeframe.FIVE_MINUTES:
            raise ValueError(f"unsupported calendar timeframe: {timeframe}")

        slots: list[ExpectedCalendarSlot] = []
        for session in self.sessions:
            duration = session.close_utc - session.open_utc
            if duration % _FIVE_MINUTES:
                raise ValueError(
                    f"session {session.session_date.isoformat()} cannot be divided into 5m slots"
                )
            cursor = session.open_utc
            while cursor < session.close_utc:
                end = cursor + _FIVE_MINUTES
                slots.append(
                    ExpectedCalendarSlot(
                        session_date=session.session_date,
                        timeframe=timeframe,
                        start_utc=cursor,
                        end_utc=end,
                    )
                )
                cursor = end
        return tuple(slots)

    def diff(self, target: CalendarSnapshot) -> CalendarSnapshotDiff:
        """Compare this snapshot with a prospective replacement."""

        metadata_fields = (
            "library_name",
            "library_version",
            "tzdata_version",
            "calendar_name",
            "timezone_name",
            "range_start",
            "range_end",
        )
        metadata_changes = tuple(
            field for field in metadata_fields if getattr(self, field) != getattr(target, field)
        )

        source_by_date = {session.session_date: session for session in self.sessions}
        target_by_date = {session.session_date: session for session in target.sessions}
        added = tuple(
            target_by_date[value] for value in sorted(target_by_date.keys() - source_by_date.keys())
        )
        removed = tuple(
            source_by_date[value] for value in sorted(source_by_date.keys() - target_by_date.keys())
        )
        changed = tuple(
            CalendarSessionChange(
                session_date=value,
                before=source_by_date[value],
                after=target_by_date[value],
            )
            for value in sorted(source_by_date.keys() & target_by_date.keys())
            if source_by_date[value] != target_by_date[value]
        )
        return CalendarSnapshotDiff(
            source_checksum=self.checksum,
            target_checksum=target.checksum,
            metadata_changes=metadata_changes,
            added_sessions=added,
            removed_sessions=removed,
            changed_sessions=changed,
        )


class TradingCalendar(Protocol):
    """Provider-neutral boundary for immutable exchange-calendar snapshots."""

    calendar_name: str
    timezone_name: str

    def snapshot(
        self,
        start_date: date,
        end_date: date,
        *,
        generated_at: datetime | None = None,
    ) -> CalendarSnapshot:
        """Return a snapshot for the half-open date range ``[start_date, end_date)``."""


class XNYSCalendar:
    """XNYS regular-trading-hours adapter backed by ``exchange_calendars``."""

    calendar_name = "XNYS"
    timezone_name = "America/New_York"

    def snapshot(
        self,
        start_date: date,
        end_date: date,
        *,
        generated_at: datetime | None = None,
    ) -> CalendarSnapshot:
        """Return XNYS RTH sessions for ``[start_date, end_date)``."""

        if end_date <= start_date:
            raise ValueError("end_date must be later than start_date")

        sessions: tuple[CalendarSession, ...]
        try:
            # The upstream date range is inclusive. Requesting ``end_date`` and
            # filtering it below makes our public boundary explicitly half-open.
            calendar: Any = exchange_calendars.get_calendar(
                self.calendar_name,
                start=start_date,
                end=end_date,
                side="left",
            )
        except exchange_calendars.errors.NoSessionsError:
            sessions = ()
        else:
            early_close_dates = {
                _as_date(value)
                for value in calendar.early_closes
                if start_date <= _as_date(value) < end_date
            }
            collected: list[CalendarSession] = []
            for label in calendar.schedule.index:
                session_date = _as_date(label)
                if not start_date <= session_date < end_date:
                    continue
                collected.append(
                    CalendarSession(
                        session_date=session_date,
                        open_utc=_as_utc_datetime(calendar.schedule.at[label, "open"]),
                        close_utc=_as_utc_datetime(calendar.schedule.at[label, "close"]),
                        is_early_close=session_date in early_close_dates,
                    )
                )
            sessions = tuple(collected)

        return CalendarSnapshot.create(
            library_name="exchange_calendars",
            library_version=version("exchange-calendars"),
            tzdata_version=version("tzdata"),
            calendar_name=self.calendar_name,
            timezone_name=self.timezone_name,
            range_start=start_date,
            range_end=end_date,
            generated_at=generated_at or datetime.now(UTC),
            sessions=sessions,
        )


def _as_date(value: object) -> date:
    converter = getattr(value, "date", None)
    if not callable(converter):
        raise TypeError("calendar session label cannot be converted to date")
    result = converter()
    if not isinstance(result, date):
        raise TypeError("calendar session label did not produce a date")
    return result


def _as_utc_datetime(value: object) -> datetime:
    converter = getattr(value, "to_pydatetime", None)
    if not callable(converter):
        raise TypeError("calendar bound cannot be converted to datetime")
    result = converter()
    if not isinstance(result, datetime):
        raise TypeError("calendar bound did not produce a datetime")
    return to_utc(result)


def _schedule_checksum(
    *,
    calendar_name: str,
    timezone_name: str,
    range_start: date,
    range_end: date,
    sessions: tuple[CalendarSession, ...],
) -> str:
    payload = {
        "calendar_name": calendar_name,
        "range_end": range_end.isoformat(),
        "range_start": range_start.isoformat(),
        "sessions": [
            {
                "close_utc": _canonical_utc(session.close_utc),
                "is_early_close": session.is_early_close,
                "open_utc": _canonical_utc(session.open_utc),
                "session_date": session.session_date.isoformat(),
            }
            for session in sessions
        ],
        "timezone_name": timezone_name,
    }
    canonical = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _canonical_utc(value: datetime) -> str:
    return to_utc(value).isoformat().replace("+00:00", "Z")


__all__ = [
    "CalendarSession",
    "CalendarSessionChange",
    "CalendarSnapshot",
    "CalendarSnapshotDiff",
    "ExpectedCalendarSlot",
    "TradingCalendar",
    "XNYSCalendar",
]
