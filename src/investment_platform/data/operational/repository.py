"""Typed, lease-fenced persistence for immutable calendar snapshots."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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


@dataclass(frozen=True, slots=True)
class CalendarReconciliationResult:
    """Auditable effects of persisting one current calendar snapshot."""

    snapshot_id: str
    stale_snapshot_ids: tuple[str, ...] = ()
    affected_session_dates: tuple[date, ...] = ()
    stale_coverage_ids: tuple[str, ...] = ()
    rebound_coverage_ids: tuple[str, ...] = ()
    stale_watermark_stream_ids: tuple[str, ...] = ()
    rebound_watermark_stream_ids: tuple[str, ...] = ()
    calendar_stale_gap_ids: tuple[str, ...] = ()


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
        """Persist and reconcile one snapshot; an exact replay is a durable no-op."""

        return self.persist_reconciling(lease, snapshot).snapshot_id

    def persist_reconciling(
        self,
        lease: WriterLease,
        snapshot: CalendarSnapshot,
    ) -> CalendarReconciliationResult:
        """Persist a snapshot and atomically reconcile compatible predecessors.

        A predecessor is compatible when it has the same calendar/timezone and its
        complete date range is contained by the target.  Coverage over unchanged
        schedule is rebound with an explicit ledger; only facts crossing a changed
        session become stale.  This also keeps range expansion from stranding a
        previously verified watermark behind a different snapshot identity.
        """

        snapshot_id = self._snapshot_id_factory(snapshot)
        if not snapshot_id or len(snapshot_id) > 128:
            raise CalendarSnapshotRepositoryError("calendar snapshot ID is invalid")
        session_rows = tuple(
            self._calendar_session_row(snapshot_id, value) for value in snapshot.sessions
        )
        reconciled_at = self._store._now()
        with self._store._leased_transaction(lease) as connection:
            existing = connection.execute(
                "SELECT * FROM calendar_snapshots WHERE calendar_snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
            if existing is not None:
                persisted = self._load_validated(connection, snapshot_id)
                if not self._same_identity(persisted, snapshot):
                    raise CalendarSnapshotIdentityCollisionError(
                        "calendar snapshot ID collides with different immutable metadata"
                    )
                if str(existing["state"]) != "CURRENT":
                    raise CalendarSnapshotRepositoryError(
                        "a stale calendar snapshot cannot be restored as current implicitly"
                    )
            else:
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
                        _format_utc(reconciled_at),
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
                existing = connection.execute(
                    "SELECT * FROM calendar_snapshots WHERE calendar_snapshot_id = ?",
                    (snapshot_id,),
                ).fetchone()
                if existing is None:
                    raise CalendarSnapshotRepositoryError(
                        "calendar snapshot insert was not visible inside its transaction"
                    )
            return self._reconcile_predecessors(
                connection,
                target_snapshot_id=snapshot_id,
                target=snapshot,
                target_created_at=str(existing["created_at"]),
                reconciled_at=reconciled_at,
            )

    def _reconcile_predecessors(
        self,
        connection: sqlite3.Connection,
        *,
        target_snapshot_id: str,
        target: CalendarSnapshot,
        target_created_at: str,
        reconciled_at: datetime,
    ) -> CalendarReconciliationResult:
        predecessor_rows = connection.execute(
            """
            SELECT calendar_snapshot_id
            FROM calendar_snapshots
            WHERE calendar_snapshot_id <> ?
              AND state = 'CURRENT'
              AND calendar_name = ?
              AND timezone_name = ?
              AND session_start_date >= ?
              AND session_end_date <= ?
              AND created_at <= ?
            ORDER BY session_start_date, session_end_date, calendar_snapshot_id
            """,
            (
                target_snapshot_id,
                target.calendar_name,
                target.timezone_name,
                target.range_start.isoformat(),
                target.range_end.isoformat(),
                target_created_at,
            ),
        ).fetchall()
        stale_snapshot_ids: set[str] = set()
        affected_session_dates: set[date] = set()
        stale_coverage_ids: set[str] = set()
        rebound_coverage_ids: set[str] = set()
        stale_watermark_stream_ids: set[str] = set()
        rebound_watermark_stream_ids: set[str] = set()
        calendar_stale_gap_ids: set[str] = set()
        for row in predecessor_rows:
            source_snapshot_id = str(row["calendar_snapshot_id"])
            source = self._load_validated(connection, source_snapshot_id)
            result = self._reconcile_predecessor(
                connection,
                source_snapshot_id=source_snapshot_id,
                source=source,
                target_snapshot_id=target_snapshot_id,
                target=target,
                reconciled_at=reconciled_at,
            )
            stale_snapshot_ids.update(result.stale_snapshot_ids)
            affected_session_dates.update(result.affected_session_dates)
            stale_coverage_ids.update(result.stale_coverage_ids)
            rebound_coverage_ids.update(result.rebound_coverage_ids)
            stale_watermark_stream_ids.update(result.stale_watermark_stream_ids)
            rebound_watermark_stream_ids.update(result.rebound_watermark_stream_ids)
            calendar_stale_gap_ids.update(result.calendar_stale_gap_ids)
        return CalendarReconciliationResult(
            snapshot_id=target_snapshot_id,
            stale_snapshot_ids=tuple(sorted(stale_snapshot_ids)),
            affected_session_dates=tuple(sorted(affected_session_dates)),
            stale_coverage_ids=tuple(sorted(stale_coverage_ids)),
            rebound_coverage_ids=tuple(sorted(rebound_coverage_ids)),
            stale_watermark_stream_ids=tuple(sorted(stale_watermark_stream_ids)),
            rebound_watermark_stream_ids=tuple(sorted(rebound_watermark_stream_ids)),
            calendar_stale_gap_ids=tuple(sorted(calendar_stale_gap_ids)),
        )

    def _reconcile_predecessor(
        self,
        connection: sqlite3.Connection,
        *,
        source_snapshot_id: str,
        source: CalendarSnapshot,
        target_snapshot_id: str,
        target: CalendarSnapshot,
        reconciled_at: datetime,
    ) -> CalendarReconciliationResult:
        if (
            source.calendar_name != target.calendar_name
            or source.timezone_name != target.timezone_name
            or target.range_start > source.range_start
            or target.range_end < source.range_end
        ):
            raise CalendarSnapshotRepositoryError(
                "calendar reconciliation requires a same-calendar containing target"
            )
        changes = self._changed_sessions(source, target)
        change_document = tuple(
            {
                "after": self._session_document(after),
                "before": self._session_document(before),
                "session_date": session_date.isoformat(),
            }
            for session_date, before, after in changes
        )
        evidence = {
            "affected_sessions": change_document,
            "source_calendar_snapshot_id": source_snapshot_id,
            "source_range": [source.range_start.isoformat(), source.range_end.isoformat()],
            "target_calendar_snapshot_id": target_snapshot_id,
            "target_range": [target.range_start.isoformat(), target.range_end.isoformat()],
            "version": 1,
        }
        evidence_json = self._canonical_json(evidence)
        evidence_hash = hashlib.sha256(evidence_json.encode()).hexdigest()
        reconciliation_id = (
            "calendar-reconciliation-"
            + hashlib.sha256(
                self._canonical_json(
                    {
                        "diff_hash": evidence_hash,
                        "source": source_snapshot_id,
                        "target": target_snapshot_id,
                        "version": 1,
                    }
                ).encode()
            ).hexdigest()
        )
        reconciled_text = _format_utc(reconciled_at)
        existing_reconciliation = connection.execute(
            """
            SELECT * FROM calendar_snapshot_reconciliations
            WHERE source_calendar_snapshot_id = ? AND target_calendar_snapshot_id = ?
            """,
            (source_snapshot_id, target_snapshot_id),
        ).fetchone()
        expected_reconciliation = (
            reconciliation_id,
            source_snapshot_id,
            target_snapshot_id,
            evidence_json,
            evidence_hash,
        )
        if existing_reconciliation is None:
            connection.execute(
                """
                INSERT INTO calendar_snapshot_reconciliations(
                    reconciliation_id, source_calendar_snapshot_id,
                    target_calendar_snapshot_id, affected_sessions_json,
                    diff_hash, reconciled_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (*expected_reconciliation, reconciled_text),
            )
        elif (
            tuple(
                str(existing_reconciliation[column])
                for column in (
                    "reconciliation_id",
                    "source_calendar_snapshot_id",
                    "target_calendar_snapshot_id",
                    "affected_sessions_json",
                    "diff_hash",
                )
            )
            != expected_reconciliation
        ):
            raise CalendarSnapshotIdentityCollisionError(
                "calendar reconciliation identity collides with different evidence"
            )

        coverage_rows = connection.execute(
            """
            SELECT coverage.*, proof.request_instance_id
            FROM coverage_segments AS coverage
            JOIN coverage_request_proofs AS proof
              ON proof.coverage_id = coverage.coverage_id
            WHERE coverage.calendar_snapshot_id = ?
              AND coverage.verification_state = 'VERIFIED'
              AND coverage.invalidated_at IS NULL
            ORDER BY coverage.stream_id, coverage.interval_start, coverage.coverage_id
            """,
            (source_snapshot_id,),
        ).fetchall()
        stale_coverage: list[sqlite3.Row] = []
        rebound_coverage: list[sqlite3.Row] = []
        for coverage in coverage_rows:
            if _parse_utc(str(coverage["verified_at"])) > reconciled_at:
                raise CalendarSnapshotRepositoryError(
                    "calendar reconciliation predates coverage verification"
                )
            start = _parse_utc(str(coverage["interval_start"]))
            end = _parse_utc(str(coverage["interval_end"]))
            if not self._snapshot_covers_interval(target, start=start, end=end):
                raise CalendarSnapshotRepositoryError(
                    "target calendar does not cover a predecessor coverage segment"
                )
            if self._sessions_equivalent(source, target, start=start, end=end):
                rebound_coverage.append(coverage)
            else:
                stale_coverage.append(coverage)

        changed_intervals = self._changed_session_intervals(changes)
        stale_coverage_by_id = {str(row["coverage_id"]): row for row in stale_coverage}
        for coverage in stale_coverage:
            cursor = connection.execute(
                """
                UPDATE coverage_segments
                SET verification_state = 'STALE', invalidated_at = ?
                WHERE coverage_id = ? AND calendar_snapshot_id = ?
                  AND verification_state = 'VERIFIED' AND invalidated_at IS NULL
                """,
                (reconciled_text, str(coverage["coverage_id"]), source_snapshot_id),
            )
            if cursor.rowcount != 1:
                raise CalendarSnapshotRepositoryError(
                    "coverage changed during calendar reconciliation"
                )
        for coverage in rebound_coverage:
            coverage_id = str(coverage["coverage_id"])
            connection.execute(
                """
                INSERT INTO calendar_coverage_rebindings(
                    reconciliation_id, coverage_id, source_calendar_snapshot_id,
                    target_calendar_snapshot_id, rebound_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    reconciliation_id,
                    coverage_id,
                    source_snapshot_id,
                    target_snapshot_id,
                    reconciled_text,
                ),
            )
            cursor = connection.execute(
                """
                UPDATE coverage_segments SET calendar_snapshot_id = ?
                WHERE coverage_id = ? AND calendar_snapshot_id = ?
                  AND verification_state = 'VERIFIED' AND invalidated_at IS NULL
                """,
                (target_snapshot_id, coverage_id, source_snapshot_id),
            )
            if cursor.rowcount != 1:
                raise CalendarSnapshotRepositoryError(
                    "coverage changed during calendar eligibility rebind"
                )

        watermark_rows = connection.execute(
            """
            SELECT * FROM watermarks
            WHERE calendar_snapshot_id = ? AND verification_state = 'VERIFIED'
              AND invalidated_at IS NULL
            ORDER BY stream_id
            """,
            (source_snapshot_id,),
        ).fetchall()
        stale_watermarks: list[sqlite3.Row] = []
        rebound_watermarks: list[sqlite3.Row] = []
        for watermark in watermark_rows:
            if _parse_utc(str(watermark["computed_at"])) > reconciled_at:
                raise CalendarSnapshotRepositoryError(
                    "calendar reconciliation predates watermark computation"
                )
            start = _parse_utc(str(watermark["coverage_start"]))
            end = _parse_utc(str(watermark["exclusive_frontier"]))
            if end <= start or not self._snapshot_covers_interval(target, start=start, end=end):
                raise CalendarSnapshotRepositoryError(
                    "target calendar does not cover a predecessor watermark"
                )
            if self._sessions_equivalent(source, target, start=start, end=end):
                rebound_watermarks.append(watermark)
            else:
                stale_watermarks.append(watermark)
        stale_streams_from_coverage = {str(row["stream_id"]) for row in stale_coverage}
        if any(
            str(row["stream_id"]) not in stale_streams_from_coverage for row in stale_watermarks
        ):
            raise CalendarSnapshotRepositoryError(
                "calendar-affected watermark lacks affected verified coverage"
            )
        for watermark in stale_watermarks:
            cursor = connection.execute(
                """
                UPDATE watermarks
                SET verification_state = 'STALE', generation = generation + 1,
                    computed_at = ?, invalidated_at = ?
                WHERE stream_id = ? AND calendar_snapshot_id = ?
                  AND verification_state = 'VERIFIED' AND invalidated_at IS NULL
                """,
                (
                    reconciled_text,
                    reconciled_text,
                    str(watermark["stream_id"]),
                    source_snapshot_id,
                ),
            )
            if cursor.rowcount != 1:
                raise CalendarSnapshotRepositoryError(
                    "watermark changed during calendar reconciliation"
                )
        for watermark in rebound_watermarks:
            stream_id = str(watermark["stream_id"])
            connection.execute(
                """
                INSERT INTO calendar_watermark_rebindings(
                    reconciliation_id, stream_id, source_calendar_snapshot_id,
                    target_calendar_snapshot_id, rebound_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    reconciliation_id,
                    stream_id,
                    source_snapshot_id,
                    target_snapshot_id,
                    reconciled_text,
                ),
            )
            cursor = connection.execute(
                """
                UPDATE watermarks
                SET calendar_snapshot_id = ?, generation = generation + 1, computed_at = ?
                WHERE stream_id = ? AND calendar_snapshot_id = ?
                  AND verification_state = 'VERIFIED' AND invalidated_at IS NULL
                """,
                (
                    target_snapshot_id,
                    reconciled_text,
                    stream_id,
                    source_snapshot_id,
                ),
            )
            if cursor.rowcount != 1:
                raise CalendarSnapshotRepositoryError(
                    "watermark changed during calendar eligibility rebind"
                )

        gap_ids = self._persist_calendar_stale_gaps(
            connection,
            reconciliation_id=reconciliation_id,
            source_snapshot_id=source_snapshot_id,
            target_snapshot_id=target_snapshot_id,
            changed_intervals=changed_intervals,
            stale_coverage=tuple(stale_coverage_by_id.values()),
            detected_at=reconciled_at,
        )
        cursor = connection.execute(
            """
            UPDATE calendar_snapshots SET state = 'STALE'
            WHERE calendar_snapshot_id = ? AND state = 'CURRENT'
            """,
            (source_snapshot_id,),
        )
        if cursor.rowcount != 1:
            raise CalendarSnapshotRepositoryError(
                "calendar predecessor changed during reconciliation"
            )
        return CalendarReconciliationResult(
            snapshot_id=target_snapshot_id,
            stale_snapshot_ids=(source_snapshot_id,),
            affected_session_dates=tuple(value[0] for value in changes),
            stale_coverage_ids=tuple(sorted(stale_coverage_by_id)),
            rebound_coverage_ids=tuple(
                sorted(str(value["coverage_id"]) for value in rebound_coverage)
            ),
            stale_watermark_stream_ids=tuple(
                sorted(str(value["stream_id"]) for value in stale_watermarks)
            ),
            rebound_watermark_stream_ids=tuple(
                sorted(str(value["stream_id"]) for value in rebound_watermarks)
            ),
            calendar_stale_gap_ids=gap_ids,
        )

    def _persist_calendar_stale_gaps(
        self,
        connection: sqlite3.Connection,
        *,
        reconciliation_id: str,
        source_snapshot_id: str,
        target_snapshot_id: str,
        changed_intervals: tuple[tuple[date, datetime, datetime], ...],
        stale_coverage: tuple[sqlite3.Row, ...],
        detected_at: datetime,
    ) -> tuple[str, ...]:
        detected_text = _format_utc(detected_at)
        gap_ids: list[str] = []
        for session_date, start, end in changed_intervals:
            overlapping = tuple(
                row
                for row in stale_coverage
                if _parse_utc(str(row["interval_start"])) < end
                and _parse_utc(str(row["interval_end"])) > start
            )
            by_stream: dict[str, list[sqlite3.Row]] = {}
            for row in overlapping:
                by_stream.setdefault(str(row["stream_id"]), []).append(row)
            for stream_id, rows in sorted(by_stream.items()):
                request_ids = tuple(sorted({str(value["request_instance_id"]) for value in rows}))
                if not request_ids:
                    raise CalendarSnapshotRepositoryError(
                        "calendar-stale gap lacks coverage request provenance"
                    )
                request_instance_id = request_ids[0]
                request_scope = connection.execute(
                    """
                    SELECT 1 FROM request_instances AS instance
                    JOIN request_spec_streams AS stream
                      ON stream.request_spec_id = instance.request_spec_id
                    WHERE instance.request_instance_id = ? AND stream.stream_id = ?
                    """,
                    (request_instance_id, stream_id),
                ).fetchone()
                if request_scope is None:
                    raise CalendarSnapshotRepositoryError(
                        "calendar-stale gap request provenance is outside the stream"
                    )
                gap_id = (
                    "gap-calendar-stale-"
                    + hashlib.sha256(
                        self._canonical_json(
                            {
                                "end": _format_utc(end),
                                "session_date": session_date.isoformat(),
                                "source_calendar_snapshot_id": source_snapshot_id,
                                "start": _format_utc(start),
                                "stream_id": stream_id,
                                "target_calendar_snapshot_id": target_snapshot_id,
                                "type": "CALENDAR_STALE",
                                "version": 1,
                            }
                        ).encode()
                    ).hexdigest()
                )
                expected_gap = (
                    gap_id,
                    stream_id,
                    _format_utc(start),
                    _format_utc(end),
                    "CALENDAR_STALE",
                    "OPEN",
                    1,
                    detected_text,
                    None,
                    request_instance_id,
                    None,
                )
                columns = (
                    "gap_id",
                    "stream_id",
                    "interval_start",
                    "interval_end",
                    "gap_type",
                    "status",
                    "blocking",
                    "detected_at",
                    "resolved_at",
                    "request_instance_id",
                    "canonical_batch_id",
                )
                existing = connection.execute(
                    "SELECT * FROM gaps WHERE gap_id = ?",
                    (gap_id,),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        f"INSERT INTO gaps({', '.join(columns)}) "
                        f"VALUES ({', '.join('?' for _ in columns)})",
                        expected_gap,
                    )
                elif tuple(existing[column] for column in columns) != expected_gap:
                    raise CalendarSnapshotIdentityCollisionError(
                        "calendar-stale gap identity collides with different evidence"
                    )
                existing_provenance = connection.execute(
                    "SELECT * FROM calendar_stale_gap_provenance WHERE gap_id = ?",
                    (gap_id,),
                ).fetchone()
                expected_provenance = (
                    gap_id,
                    reconciliation_id,
                    session_date.isoformat(),
                )
                if existing_provenance is None:
                    connection.execute(
                        """
                        INSERT INTO calendar_stale_gap_provenance(
                            gap_id, reconciliation_id, session_date
                        ) VALUES (?, ?, ?)
                        """,
                        expected_provenance,
                    )
                elif (
                    tuple(
                        existing_provenance[column]
                        for column in ("gap_id", "reconciliation_id", "session_date")
                    )
                    != expected_provenance
                ):
                    raise CalendarSnapshotIdentityCollisionError(
                        "calendar-stale gap provenance collides with different evidence"
                    )
                for row in sorted(rows, key=lambda value: str(value["coverage_id"])):
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO calendar_stale_gap_coverage(gap_id, coverage_id)
                        VALUES (?, ?)
                        """,
                        (gap_id, str(row["coverage_id"])),
                    )
                gap_ids.append(gap_id)
        if stale_coverage and not gap_ids:
            raise CalendarSnapshotRepositoryError(
                "stale calendar coverage has no changed-session gap evidence"
            )
        return tuple(sorted(set(gap_ids)))

    @staticmethod
    def _changed_sessions(
        source: CalendarSnapshot,
        target: CalendarSnapshot,
    ) -> tuple[
        tuple[date, CalendarSession | None, CalendarSession | None],
        ...,
    ]:
        source_by_date = {value.session_date: value for value in source.sessions}
        target_by_date = {
            value.session_date: value
            for value in target.sessions
            if source.range_start <= value.session_date < source.range_end
        }
        dates = sorted(set(source_by_date) | set(target_by_date))
        return tuple(
            (value, source_by_date.get(value), target_by_date.get(value))
            for value in dates
            if source_by_date.get(value) != target_by_date.get(value)
        )

    @staticmethod
    def _changed_session_intervals(
        changes: tuple[
            tuple[date, CalendarSession | None, CalendarSession | None],
            ...,
        ],
    ) -> tuple[tuple[date, datetime, datetime], ...]:
        intervals: list[tuple[date, datetime, datetime]] = []
        for session_date, before, after in changes:
            sessions = tuple(value for value in (before, after) if value is not None)
            if not sessions:
                raise CalendarSnapshotRepositoryError(
                    "calendar change lacks both source and target session"
                )
            intervals.append(
                (
                    session_date,
                    min(value.open_utc for value in sessions),
                    max(value.close_utc for value in sessions),
                )
            )
        return tuple(intervals)

    @staticmethod
    def _session_document(session: CalendarSession | None) -> dict[str, object] | None:
        if session is None:
            return None
        return {
            "close_utc": _format_utc(session.close_utc),
            "is_early_close": session.is_early_close,
            "open_utc": _format_utc(session.open_utc),
        }

    @staticmethod
    def _snapshot_covers_interval(
        snapshot: CalendarSnapshot,
        *,
        start: datetime,
        end: datetime,
    ) -> bool:
        if end <= start:
            return False
        try:
            timezone = ZoneInfo(snapshot.timezone_name)
        except (ZoneInfoNotFoundError, ValueError):
            return False
        first_date = start.astimezone(timezone).date()
        last_date = (end - timedelta(microseconds=1)).astimezone(timezone).date()
        return snapshot.range_start <= first_date and last_date < snapshot.range_end

    @staticmethod
    def _sessions_equivalent(
        source: CalendarSnapshot,
        target: CalendarSnapshot,
        *,
        start: datetime,
        end: datetime,
    ) -> bool:
        def overlapping(snapshot: CalendarSnapshot) -> tuple[CalendarSession, ...]:
            return tuple(
                session
                for session in snapshot.sessions
                if session.open_utc < end and session.close_utc > start
            )

        return overlapping(source) == overlapping(target)

    @staticmethod
    def _canonical_json(value: object) -> str:
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)

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
    "CalendarReconciliationResult",
    "CalendarSnapshotIdentityCollisionError",
    "CalendarSnapshotRepository",
    "CalendarSnapshotRepositoryError",
    "deterministic_calendar_snapshot_id",
]
