"""Typed, lease-fenced persistence for immutable calendar snapshots."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date
from typing import Protocol

from investment_platform.data.calendar import CalendarSession, CalendarSnapshot
from investment_platform.data.operational.store import (
    OperationalStateError,
    OperationalStateStore,
    WriterLease,
    _format_utc,
    _parse_utc,
)


class CalendarSnapshotRepositoryError(OperationalStateError):
    """Raised when persisted calendar metadata violates its immutable contract."""


class CalendarSnapshotIdentityCollisionError(CalendarSnapshotRepositoryError):
    """Raised when one snapshot ID resolves to different immutable metadata."""


class CalendarSnapshotIdFactory(Protocol):
    def __call__(self, snapshot: CalendarSnapshot) -> str: ...


def deterministic_calendar_snapshot_id(snapshot: CalendarSnapshot) -> str:
    """Return an identity over schedule and semantic versions, excluding generation time."""

    identity = {
        "calendar_name": snapshot.calendar_name,
        "checksum": snapshot.checksum,
        "library_name": snapshot.library_name,
        "library_version": snapshot.library_version,
        "range_end": snapshot.range_end.isoformat(),
        "range_start": snapshot.range_start.isoformat(),
        "timezone_name": snapshot.timezone_name,
        "tzdata_version": snapshot.tzdata_version,
    }
    canonical = json.dumps(identity, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return f"calendar-{hashlib.sha256(canonical.encode()).hexdigest()}"


class CalendarSnapshotRepository:
    """Persist and verify versioned calendar schedules under the one writer lease."""

    def __init__(
        self,
        store: OperationalStateStore,
        *,
        snapshot_id_factory: CalendarSnapshotIdFactory = deterministic_calendar_snapshot_id,
    ) -> None:
        self._store = store
        self._snapshot_id_factory = snapshot_id_factory

    def persist(self, lease: WriterLease, snapshot: CalendarSnapshot) -> str:
        """Persist one snapshot; an exact semantic replay is a durable no-op."""

        snapshot_id = self._snapshot_id_factory(snapshot)
        if not snapshot_id or len(snapshot_id) > 128:
            raise CalendarSnapshotRepositoryError("calendar snapshot ID is invalid")
        session_rows = tuple(
            self._calendar_session_row(snapshot_id, value) for value in snapshot.sessions
        )
        with self._store._leased_transaction(lease) as connection:
            existing = connection.execute(
                "SELECT 1 FROM calendar_snapshots WHERE calendar_snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
            if existing is not None:
                persisted = self._load_validated(connection, snapshot_id)
                if not self._same_identity(persisted, snapshot):
                    raise CalendarSnapshotIdentityCollisionError(
                        "calendar snapshot ID collides with different immutable metadata"
                    )
                return snapshot_id
            connection.execute(
                """
                INSERT INTO calendar_snapshots(
                    calendar_snapshot_id, calendar_name, timezone_name,
                    package_name, package_version, tzdata_version,
                    session_start_date, session_end_date, schedule_checksum,
                    generated_at, created_at, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'CURRENT')
                """,
                (
                    snapshot_id,
                    snapshot.calendar_name,
                    snapshot.timezone_name,
                    snapshot.library_name,
                    snapshot.library_version,
                    snapshot.tzdata_version,
                    snapshot.range_start.isoformat(),
                    snapshot.range_end.isoformat(),
                    snapshot.checksum,
                    _format_utc(snapshot.generated_at),
                    _format_utc(self._store._now()),
                ),
            )
            connection.executemany(
                """
                INSERT INTO calendar_sessions(
                    calendar_snapshot_id, session_date, open_at, close_at,
                    is_early_close, expected_1d_count, expected_5m_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                session_rows,
            )
        return snapshot_id

    def load(self, snapshot_id: str) -> CalendarSnapshot:
        """Load and checksum-verify a calendar snapshot after restart."""

        with self._store.read_only_connection() as connection:
            return self._load_validated(connection, snapshot_id)

    @staticmethod
    def _calendar_session_row(
        snapshot_id: str,
        session: CalendarSession,
    ) -> tuple[object, ...]:
        duration_seconds = int((session.close_utc - session.open_utc).total_seconds())
        if duration_seconds <= 0 or duration_seconds % 300:
            raise CalendarSnapshotRepositoryError(
                "calendar session cannot be represented as complete five-minute slots"
            )
        return (
            snapshot_id,
            session.session_date.isoformat(),
            _format_utc(session.open_utc),
            _format_utc(session.close_utc),
            int(session.is_early_close),
            1,
            duration_seconds // 300,
        )

    @staticmethod
    def _same_identity(left: CalendarSnapshot, right: CalendarSnapshot) -> bool:
        return left.model_dump(exclude={"generated_at"}) == right.model_dump(
            exclude={"generated_at"}
        )

    def _load_validated(
        self,
        connection: sqlite3.Connection,
        snapshot_id: str,
    ) -> CalendarSnapshot:
        snapshot = self._load(connection, snapshot_id)
        if self._snapshot_id_factory(snapshot) != snapshot_id:
            raise CalendarSnapshotIdentityCollisionError(
                "calendar snapshot ID does not match its immutable metadata"
            )
        return snapshot

    @staticmethod
    def _load(connection: sqlite3.Connection, snapshot_id: str) -> CalendarSnapshot:
        row: sqlite3.Row | None = connection.execute(
            "SELECT * FROM calendar_snapshots WHERE calendar_snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        if row is None:
            raise CalendarSnapshotRepositoryError("calendar snapshot is not cataloged")
        session_rows = connection.execute(
            """
            SELECT * FROM calendar_sessions
            WHERE calendar_snapshot_id = ?
            ORDER BY session_date
            """,
            (snapshot_id,),
        ).fetchall()
        sessions: list[CalendarSession] = []
        for session_row in session_rows:
            session = CalendarSession(
                session_date=date.fromisoformat(str(session_row["session_date"])),
                open_utc=_parse_utc(str(session_row["open_at"])),
                close_utc=_parse_utc(str(session_row["close_at"])),
                is_early_close=bool(session_row["is_early_close"]),
            )
            expected_five_minutes = int(
                (session.close_utc - session.open_utc).total_seconds() // 300
            )
            if (
                int(session_row["expected_1d_count"]) != 1
                or int(session_row["expected_5m_count"]) != expected_five_minutes
            ):
                raise CalendarSnapshotRepositoryError(
                    "persisted calendar session counts fail deterministic verification"
                )
            sessions.append(session)
        try:
            return CalendarSnapshot(
                library_name=str(row["package_name"]),
                library_version=str(row["package_version"]),
                tzdata_version=str(row["tzdata_version"]),
                calendar_name=str(row["calendar_name"]),
                timezone_name=str(row["timezone_name"]),
                range_start=date.fromisoformat(str(row["session_start_date"])),
                range_end=date.fromisoformat(str(row["session_end_date"])),
                generated_at=_parse_utc(str(row["generated_at"])),
                sessions=tuple(sessions),
                checksum=str(row["schedule_checksum"]),
            )
        except (ValueError, TypeError) as error:
            raise CalendarSnapshotRepositoryError(
                "persisted calendar snapshot fails deterministic verification"
            ) from error


__all__ = [
    "CalendarSnapshotIdentityCollisionError",
    "CalendarSnapshotRepository",
    "CalendarSnapshotRepositoryError",
    "deterministic_calendar_snapshot_id",
]
