"""Typed calendar-repository and writer-fencing tests."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from investment_platform.data.calendar import CalendarSession, CalendarSnapshot
from investment_platform.data.operational import (
    CalendarSnapshotIdentityCollisionError,
    CalendarSnapshotRepository,
    CalendarSnapshotRepositoryError,
    OperationalStateStore,
    WriterLease,
    WriterLeaseError,
    WriterLeaseLostError,
)
from investment_platform.data_root import PrivateDataRoot

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
_OPEN = datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
_CLOSE = datetime(2025, 7, 2, 20, 0, tzinfo=UTC)


@dataclass
class MutableClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value

    def advance(self, delta: timedelta) -> None:
        self.value += delta


@pytest.fixture
def repository_root() -> Path:
    return Path(__file__).parents[2].resolve(strict=True)


@pytest.fixture
def private_root(tmp_path: Path, repository_root: Path) -> PrivateDataRoot:
    root = PrivateDataRoot(
        tmp_path / "dedicated-calendar-state-root",
        repository_root,
        allow_temporary_for_tests=True,
    )
    root.initialize(created_at=_NOW)
    return root


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(_NOW)


@pytest.fixture
def store(
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> Generator[OperationalStateStore]:
    opened = OperationalStateStore.open(private_root, clock=clock)
    yield opened
    opened.close()


@pytest.fixture
def lease(store: OperationalStateStore) -> WriterLease:
    return store.acquire_writer_lease("calendar-owner", timedelta(minutes=5))


def _snapshot(*, generated_at: datetime = _NOW, calendar_name: str = "XNYS") -> CalendarSnapshot:
    return CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="1.0",
        tzdata_version="2026a",
        calendar_name=calendar_name,
        timezone_name="America/New_York",
        range_start=date(2025, 7, 2),
        range_end=date(2025, 7, 3),
        generated_at=generated_at,
        sessions=(
            CalendarSession(
                session_date=date(2025, 7, 2),
                open_utc=_OPEN,
                close_utc=_CLOSE,
            ),
        ),
    )


def test_calendar_snapshot_round_trip_exact_replay_and_restart(
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    snapshot = _snapshot()
    with OperationalStateStore.open(private_root, clock=clock) as first:
        lease = first.acquire_writer_lease("calendar-writer", timedelta(minutes=5))
        repository = CalendarSnapshotRepository(first)
        snapshot_id = repository.persist(lease, snapshot)
        replay = _snapshot(generated_at=_NOW + timedelta(days=1))

        assert repository.persist(lease, replay) == snapshot_id
        assert repository.load(snapshot_id) == snapshot
        with first.read_only_connection() as connection:
            assert (
                int(connection.execute("SELECT count(*) FROM calendar_snapshots").fetchone()[0])
                == 1
            )
            assert (
                int(connection.execute("SELECT count(*) FROM calendar_sessions").fetchone()[0]) == 1
            )

    with OperationalStateStore.open(private_root, clock=clock) as reopened:
        loaded = CalendarSnapshotRepository(reopened).load(snapshot_id)
    assert loaded == snapshot


def test_calendar_snapshot_collision_fails_without_overwrite(
    store: OperationalStateStore,
    lease: WriterLease,
) -> None:
    repository = CalendarSnapshotRepository(
        store,
        snapshot_id_factory=lambda _: "forced-calendar-id",
    )
    first = _snapshot()
    repository.persist(lease, first)

    with pytest.raises(CalendarSnapshotIdentityCollisionError, match="collides"):
        repository.persist(lease, _snapshot(calendar_name="OTHER"))
    assert repository.load("forced-calendar-id") == first


def test_calendar_load_detects_identity_or_session_count_tampering(
    store: OperationalStateStore,
    lease: WriterLease,
) -> None:
    repository = CalendarSnapshotRepository(store)
    snapshot_id = repository.persist(lease, _snapshot())
    with store._transaction() as connection:
        connection.execute(
            """
            UPDATE calendar_sessions SET expected_5m_count = 77
            WHERE calendar_snapshot_id = ?
            """,
            (snapshot_id,),
        )

    with pytest.raises(CalendarSnapshotRepositoryError, match="counts"):
        repository.load(snapshot_id)


def test_typed_calendar_mutation_is_fenced_by_owner_expiry_and_takeover(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
) -> None:
    repository = CalendarSnapshotRepository(store)
    non_owner = replace(lease, owner_id="not-the-owner")
    with pytest.raises(WriterLeaseLostError, match="does not authorize"):
        repository.persist(non_owner, _snapshot())

    clock.advance(timedelta(minutes=5))
    with pytest.raises(WriterLeaseLostError, match="does not authorize"):
        repository.persist(lease, _snapshot())

    takeover = store.acquire_writer_lease("replacement-owner", timedelta(minutes=5))
    with pytest.raises(WriterLeaseLostError, match="does not authorize"):
        repository.persist(lease, _snapshot())
    assert repository.persist(takeover, _snapshot()).startswith("calendar-")


def test_phase2_rejects_every_alternative_writer_lease_name(
    store: OperationalStateStore,
    lease: WriterLease,
) -> None:
    forged = replace(lease, lease_name="alternate-writer")
    repository = CalendarSnapshotRepository(store)

    with pytest.raises(WriterLeaseError, match="only the single"):
        store.renew_writer_lease(forged, timedelta(minutes=1))
    with pytest.raises(WriterLeaseError, match="only the single"):
        store.release_writer_lease(forged)
    with pytest.raises(WriterLeaseError, match="only the single"):
        repository.persist(forged, _snapshot())


def test_operational_public_api_has_no_unfenced_generic_mutator() -> None:
    assert not hasattr(OperationalStateStore, "transaction")


def test_narrow_or_disjoint_snapshot_does_not_stale_a_broader_current_domain(
    store: OperationalStateStore,
    lease: WriterLease,
) -> None:
    broad = CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="broad",
        tzdata_version="2026a",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=date(2025, 7, 2),
        range_end=date(2026, 1, 3),
        generated_at=_NOW,
        sessions=(
            _snapshot().sessions[0],
            CalendarSession(
                session_date=date(2026, 1, 2),
                open_utc=datetime(2026, 1, 2, 14, 30, tzinfo=UTC),
                close_utc=datetime(2026, 1, 2, 21, 0, tzinfo=UTC),
            ),
        ),
    )
    narrow = _snapshot(generated_at=_NOW + timedelta(seconds=1))
    disjoint = CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="disjoint",
        tzdata_version="2026a",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=date(2026, 1, 2),
        range_end=date(2026, 1, 3),
        generated_at=_NOW + timedelta(seconds=2),
        sessions=(broad.sessions[1],),
    )
    repository = CalendarSnapshotRepository(store)
    broad_id = repository.persist(lease, broad)

    narrow_result = repository.persist_reconciling(lease, narrow)
    disjoint_result = repository.persist_reconciling(lease, disjoint)

    assert narrow_result.stale_snapshot_ids == ()
    assert disjoint_result.stale_snapshot_ids == ()
    with store.read_only_connection() as connection:
        states = connection.execute(
            """
            SELECT calendar_snapshot_id, state FROM calendar_snapshots
            WHERE calendar_snapshot_id IN (?, ?, ?)
            ORDER BY calendar_snapshot_id
            """,
            (broad_id, narrow_result.snapshot_id, disjoint_result.snapshot_id),
        ).fetchall()
    assert len(states) == 3
    assert {str(row["state"]) for row in states} == {"CURRENT"}
