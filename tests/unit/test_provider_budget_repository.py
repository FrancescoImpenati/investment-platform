"""Restart-safe provider budget reservations remain conservative around network I/O."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from investment_platform.data.operational.budget import (
    BudgetReservationState,
    ProviderBudgetExceededError,
    ProviderBudgetRepository,
    ProviderBudgetReservationRequest,
    ProviderBudgetStateConflictError,
    ProviderBudgetWindow,
)
from investment_platform.data.operational.store import OperationalStateStore, _format_utc
from investment_platform.data_root import PrivateDataRoot

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
_RUN_ID = UUID("10000000-0000-4000-8000-000000000010")
_REQUEST_ID = UUID("20000000-0000-4000-8000-000000000010")
_ATTEMPT_ID = UUID("30000000-0000-4000-8000-000000000010")
_SECOND_ATTEMPT_ID = UUID("30000000-0000-4000-8000-000000000011")


@dataclass
class MutableClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value


def _private_root(tmp_path: Path) -> PrivateDataRoot:
    repository_root = Path(__file__).parents[2].resolve(strict=True)
    root = PrivateDataRoot(
        tmp_path.parent / f"p2-budget-{uuid4().hex[:8]}",
        repository_root,
        allow_temporary_for_tests=True,
    )
    root.initialize(created_at=_NOW)
    return root


def _insert_running_attempts(store: OperationalStateStore) -> None:
    timestamp = _format_utc(_NOW)
    with store._transaction() as connection:
        connection.execute(
            """
            INSERT INTO policy_snapshots(
                policy_snapshot_id, policy_id, revision, policy_hash, provider, dataset,
                retention_mode, verified_at, captured_at
            ) VALUES ('policy', 'policy', 1, ?, 'synthetic', 'price_bars',
                      'SYNTHETIC_UNRESTRICTED', ?, ?)
            """,
            ("a" * 64, timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO ingestion_runs(
                run_id, mode, environment, provider, dataset, status,
                policy_snapshot_id, created_at, started_at, planned_request_count
            ) VALUES (?, 'BACKFILL', 'test', 'synthetic', 'price_bars', 'RUNNING',
                      'policy', ?, ?, 1)
            """,
            (str(_RUN_ID), timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO request_specs(
                request_spec_id, request_spec_hash, provider, dataset, interval_start,
                interval_end, mapping_version, specification_json, created_at
            ) VALUES ('spec', ?, 'synthetic', 'price_bars', ?, ?, 'v1', '{}', ?)
            """,
            (
                "b" * 64,
                _format_utc(_NOW - timedelta(minutes=10)),
                _format_utc(_NOW + timedelta(minutes=10)),
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO request_instances(
                request_instance_id, run_id, request_spec_id, intent, reason,
                plan_ordinal, status, created_at
            ) VALUES (?, ?, 'spec', 'BACKFILL', 'synthetic budget test', 0,
                      'ACQUIRING', ?)
            """,
            (str(_REQUEST_ID), str(_RUN_ID), timestamp),
        )
        for ordinal, attempt_id in enumerate((_ATTEMPT_ID, _SECOND_ATTEMPT_ID), start=1):
            connection.execute(
                """
                INSERT INTO request_attempts(
                    attempt_id, request_instance_id, attempt_number, status, started_at
                ) VALUES (?, ?, ?, 'RUNNING', ?)
                """,
                (str(attempt_id), str(_REQUEST_ID), ordinal, timestamp),
            )


def _window(*, limit: int = 2) -> ProviderBudgetWindow:
    return ProviderBudgetWindow(
        provider="synthetic",
        dataset="price_bars",
        budget_key="calls-per-minute",
        window_start=_NOW - timedelta(seconds=30),
        window_end=_NOW + timedelta(seconds=30),
        limit_count=limit,
    )


def _request(
    attempt_id: UUID = _ATTEMPT_ID,
    *,
    limit: int = 2,
    dispatch_ordinal: int = 1,
) -> ProviderBudgetReservationRequest:
    return ProviderBudgetReservationRequest(
        request_instance_id=_REQUEST_ID,
        attempt_id=attempt_id,
        dispatch_ordinal=dispatch_ordinal,
        window=_window(limit=limit),
    )


def test_reservation_and_pre_dispatch_consumption_are_idempotent_across_restart(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        _insert_running_attempts(store)
        lease = store.acquire_writer_lease("budget-test", timedelta(minutes=5))
        repository = ProviderBudgetRepository(store)
        first = repository.reserve(lease, _request())
        replay = repository.reserve(lease, _request())
        consumed = repository.consume_before_dispatch(lease, first.reservation_id)
        consumed_replay = repository.consume_before_dispatch(lease, first.reservation_id)

        assert first.state == BudgetReservationState.RESERVED
        assert first.reserved_count == 1
        assert not first.replayed
        assert replay.replayed
        assert consumed.state == BudgetReservationState.CONSUMED
        assert consumed.used_count == 1
        assert consumed.reserved_count == 0
        assert consumed_replay.replayed

    with OperationalStateStore.open(root, clock=clock) as reopened:
        snapshot = ProviderBudgetRepository(reopened).snapshot(_window())

    assert snapshot is not None
    assert snapshot.used_count == 1
    assert snapshot.reserved_count == 0
    assert snapshot.available_count == 1


def test_budget_exhaustion_happens_before_dispatch_and_release_restores_capacity(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        _insert_running_attempts(store)
        lease = store.acquire_writer_lease("budget-test", timedelta(minutes=5))
        repository = ProviderBudgetRepository(store)
        reserved = repository.reserve(lease, _request(limit=1))

        with pytest.raises(ProviderBudgetExceededError, match="exhausted"):
            repository.reserve(lease, _request(_SECOND_ATTEMPT_ID, limit=1))

        released = repository.release_before_dispatch(lease, reserved.reservation_id)
        second = repository.reserve(lease, _request(_SECOND_ATTEMPT_ID, limit=1))

        assert released.state == BudgetReservationState.RELEASED
        assert released.used_count == 0
        assert second.state == BudgetReservationState.RESERVED

        with pytest.raises(ProviderBudgetStateConflictError, match="cannot change outcome"):
            repository.consume_before_dispatch(lease, released.reservation_id)


def test_provider_header_observation_is_conservative_and_requires_consumed_call(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        _insert_running_attempts(store)
        lease = store.acquire_writer_lease("budget-test", timedelta(minutes=5))
        repository = ProviderBudgetRepository(store)
        reserved = repository.reserve(lease, _request(limit=10))

        with pytest.raises(ProviderBudgetStateConflictError, match="consumed"):
            repository.observe_remaining(
                lease,
                reserved.reservation_id,
                capacity=10,
                remaining=8,
            )

        repository.consume_before_dispatch(lease, reserved.reservation_id)
        observed = repository.observe_remaining(
            lease,
            reserved.reservation_id,
            capacity=10,
            remaining=8,
        )

        assert observed.used_count == 2
        assert observed.available_count == 8

        with pytest.raises(ProviderBudgetStateConflictError, match="commitments"):
            repository.observe_remaining(
                lease,
                reserved.reservation_id,
                capacity=1,
                remaining=1,
            )


def test_repeated_possible_dispatch_for_same_attempt_gets_new_durable_budget_identity(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        _insert_running_attempts(store)
        lease = store.acquire_writer_lease("budget-test", timedelta(minutes=5))
        repository = ProviderBudgetRepository(store)
        first = repository.reserve(lease, _request(limit=2, dispatch_ordinal=1))
        repository.consume_before_dispatch(lease, first.reservation_id)

        latest = repository.latest_for_attempt(
            _ATTEMPT_ID,
            budget_key="calls-per-minute",
        )
        second = repository.reserve(lease, _request(limit=2, dispatch_ordinal=2))
        consumed = repository.consume_before_dispatch(lease, second.reservation_id)

        assert latest is not None
        assert latest.dispatch_ordinal == 1
        assert latest.state is BudgetReservationState.CONSUMED
        assert second.reservation_id != first.reservation_id
        assert consumed.dispatch_ordinal == 2
        assert consumed.used_count == 2
