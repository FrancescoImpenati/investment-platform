"""Fail-closed read projections used to restart living ingestion."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from investment_platform.data.ingestion.identity import AttemptIdentity
from investment_platform.data.operational import (
    CalendarSnapshotRepository,
    IngestionExecutionRepository,
    IngestionPlanRepository,
    PlanPersistenceRequest,
    RequestInstanceStatus,
)
from investment_platform.data.operational.restart import (
    PublicationRecoveryState,
    RestartAction,
    RestartProjectionIntegrityError,
    RestartProjectionReader,
)
from investment_platform.data.operational.store import (
    OperationalStateStore,
    WriterLease,
    _format_utc,
)
from investment_platform.data.retention import RetentionPolicyCatalog, RetentionPolicyEnforcer
from tests.unit.test_ingestion_execution_repository import (
    _ATTEMPT_ID,
    _NOW,
    _RUN_ID,
    MutableClock,
    _calendar,
    _plan,
    _prepare_scenario,
    _private_root,
)

pytestmark = pytest.mark.unit


def _persist_plan(
    store: OperationalStateStore,
    clock: MutableClock,
    *,
    max_attempts: int = 3,
) -> tuple[PlanPersistenceRequest, WriterLease]:
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    plan = _plan(enforcer).model_copy(update={"max_attempts": max_attempts})
    lease = store.acquire_writer_lease("restart-projection", timedelta(minutes=30))
    CalendarSnapshotRepository(store).persist(lease, _calendar())
    IngestionPlanRepository(store).persist(lease, plan)
    return plan, lease


def _seed_active_gap(
    store: OperationalStateStore,
    lease: WriterLease,
    *,
    gap_id: str,
    stream_id: str,
    request_id: str,
    start: datetime,
    end: datetime,
    detected_at: datetime,
) -> None:
    with store._leased_transaction(lease) as connection:
        connection.execute(
            """
            INSERT INTO gaps(
                gap_id, stream_id, interval_start, interval_end, gap_type,
                status, blocking, detected_at, resolved_at,
                request_instance_id, canonical_batch_id
            ) VALUES (?, ?, ?, ?, 'EXPECTED_OBSERVATION', 'OPEN', 1, ?, NULL, ?, NULL)
            """,
            (
                gap_id,
                stream_id,
                _format_utc(start),
                _format_utc(end),
                _format_utc(detected_at),
                request_id,
            ),
        )
        connection.execute(
            "UPDATE watermarks SET blocking_gap_count = 1 WHERE stream_id = ?",
            (stream_id,),
        )


def test_reconstructs_exact_plan_and_ordered_dispatch_state(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        persisted, lease = _persist_plan(store, clock)
        before = store.path.stat().st_size

        projection = RestartProjectionReader(store).load_run(_RUN_ID)

        assert projection.plan == persisted.plan
        assert projection.plan_hash
        assert projection.calendar_current
        assert projection.policy_current
        assert len(projection.requests) == 1
        request = projection.requests[0]
        assert request.plan_ordinal == 0
        assert request.specification == persisted.plan.requests[0].specification
        assert request.authorization == persisted.plan.requests[0].authorization
        assert request.latest_attempt is None
        assert request.action is RestartAction.DISPATCH
        assert store.path.stat().st_size == before
        store.release_writer_lease(lease)


def test_running_attempt_reconstructs_exact_authorization_and_resume_action(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        persisted, lease = _persist_plan(store, clock)
        request_id = (
            IngestionPlanRepository(store).load_progress(_RUN_ID).requests[0].request_instance_id
        )
        planned = persisted.plan.requests[0]
        identity = AttemptIdentity(
            attempt_id=_ATTEMPT_ID,
            request_instance_id=request_id,
            attempt_number=1,
        )
        IngestionExecutionRepository(store).begin_attempt(
            lease,
            identity,
            planned.authorization,
        )

        request = RestartProjectionReader(store).load_run(_RUN_ID).requests[0]

        assert request.action is RestartAction.RESUME_ACQUISITION
        assert request.latest_attempt is not None
        assert request.latest_attempt.request_authorization == planned.authorization
        assert request.latest_attempt.acquisition_authorization is None
        store.release_writer_lease(lease)


def test_retry_wait_becomes_retry_dispatch_at_durable_threshold_after_reopen(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        persisted, lease = _persist_plan(store, clock)
        request_id = (
            IngestionPlanRepository(store).load_progress(_RUN_ID).requests[0].request_instance_id
        )
        identity = AttemptIdentity(
            attempt_id=_ATTEMPT_ID,
            request_instance_id=request_id,
            attempt_number=1,
        )
        repository = IngestionExecutionRepository(store)
        repository.begin_attempt(lease, identity, persisted.plan.requests[0].authorization)
        clock.advance()
        eligible_at = clock.value + timedelta(minutes=5)
        repository.record_attempt_failure(
            lease,
            identity,
            retryable=True,
            category="PROVIDER_TRANSIENT",
            code="RATE_LIMITED",
            sanitized_message="Provider requested a bounded retry delay.",
            failed_at=clock.value,
            next_eligible_at=eligible_at,
        )
        before_restart = RestartProjectionReader(store).load_run(_RUN_ID).requests[0]
        assert before_restart.action is RestartAction.WAIT_RETRY
        assert before_restart.next_eligible_at == eligible_at
        store.release_writer_lease(lease)

    with OperationalStateStore.open(root, clock=clock) as reopened:
        still_waiting = RestartProjectionReader(reopened).load_run(_RUN_ID).requests[0]
        assert still_waiting.action is RestartAction.WAIT_RETRY

    clock.advance(timedelta(minutes=5))
    with OperationalStateStore.open(root, clock=clock) as reopened:
        eligible = RestartProjectionReader(reopened).load_run(_RUN_ID).requests[0]
        assert eligible.action is RestartAction.RETRY_DISPATCH
        assert eligible.retry_count == 1
        assert eligible.max_attempts == 3
        assert eligible.latest_attempt is not None
        assert eligible.latest_attempt.next_eligible_at == eligible_at


def test_retryable_failure_at_max_attempts_projects_terminal_request(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        persisted, lease = _persist_plan(store, clock, max_attempts=1)
        request_id = (
            IngestionPlanRepository(store).load_progress(_RUN_ID).requests[0].request_instance_id
        )
        identity = AttemptIdentity(
            attempt_id=_ATTEMPT_ID,
            request_instance_id=request_id,
            attempt_number=1,
        )
        repository = IngestionExecutionRepository(store)
        repository.begin_attempt(lease, identity, persisted.plan.requests[0].authorization)
        clock.advance()
        repository.record_attempt_failure(
            lease,
            identity,
            retryable=True,
            category="PROVIDER_TRANSIENT",
            code="TEMPORARILY_UNAVAILABLE",
            sanitized_message="Provider endpoint is temporarily unavailable.",
            failed_at=clock.value,
            next_eligible_at=clock.value + timedelta(minutes=1),
        )

        terminal = RestartProjectionReader(store).load_run(_RUN_ID).requests[0]

        assert terminal.status is RequestInstanceStatus.FAILED
        assert terminal.action is RestartAction.RECONCILE_RUN
        assert terminal.retry_count == terminal.max_attempts == 1
        assert terminal.next_eligible_at is None
        assert terminal.latest_attempt is not None
        assert terminal.latest_attempt.next_eligible_at is None
        store.release_writer_lease(lease)


def test_invalid_authorization_json_fails_closed_without_echoing_content(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        persisted, lease = _persist_plan(store, clock)
        request_id = (
            IngestionPlanRepository(store).load_progress(_RUN_ID).requests[0].request_instance_id
        )
        IngestionExecutionRepository(store).begin_attempt(
            lease,
            AttemptIdentity(
                attempt_id=_ATTEMPT_ID,
                request_instance_id=request_id,
                attempt_number=1,
            ),
            persisted.plan.requests[0].authorization,
        )
        marker = "api_key=must-not-be-reported"
        with store._transaction(write=True) as connection:
            connection.execute(
                "UPDATE attempt_request_authorizations SET authorization_json = ?",
                (f'{{"unsafe":"{marker}"}}',),
            )

        with pytest.raises(RestartProjectionIntegrityError) as captured:
            RestartProjectionReader(store).load_run(_RUN_ID)

        assert marker not in str(captured.value)
        store.release_writer_lease(lease)


def test_prepared_and_committed_publication_choose_adopt_then_reconcile(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        reader = RestartProjectionReader(store)

        prepared = reader.load_run(_RUN_ID).requests[0]
        assert prepared.action is RestartAction.ADOPT_PUBLICATION
        assert prepared.latest_attempt is not None
        assert prepared.latest_attempt.acquisition_authorization == scenario.acquisition
        assert prepared.publication is not None
        assert prepared.publication.state is PublicationRecoveryState.PREPARED
        assert not prepared.publication.publication_committed

        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        committed = reader.load_run(_RUN_ID).requests[0]
        proofs = reader.load_stream_proofs((scenario.coverage.segments[0].stream_id,))

        assert committed.action is RestartAction.RECONCILE_RUN
        assert committed.publication is not None
        assert committed.publication.state is PublicationRecoveryState.CATALOGED
        assert committed.publication.publication_committed
        assert proofs.coverage == scenario.coverage.segments
        assert proofs.gaps == scenario.coverage.gaps
        assert proofs.watermarks == scenario.coverage.watermarks
        store.release_writer_lease(lease)


def test_tampered_coverage_proof_hash_is_rejected(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        with store._transaction(write=True) as connection:
            connection.execute(
                "UPDATE coverage_request_proofs SET proof_hash = ?",
                ("0" * 64,),
            )

        with pytest.raises(RestartProjectionIntegrityError):
            RestartProjectionReader(store).load_stream_proofs(
                (scenario.coverage.segments[0].stream_id,)
            )
        store.release_writer_lease(lease)


def test_active_gap_starting_exactly_at_frontier_is_counted_but_does_not_invalidate_prefix(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        stream_id = scenario.coverage.segments[0].stream_id
        frontier = scenario.coverage.watermarks[0].exclusive_frontier
        _seed_active_gap(
            store,
            lease,
            gap_id="gap-exactly-at-frontier",
            stream_id=stream_id,
            request_id=str(scenario.request_id),
            start=frontier,
            end=frontier + timedelta(minutes=5),
            detected_at=clock.value,
        )

        proofs = RestartProjectionReader(store).load_stream_proofs((stream_id,))

        assert proofs.watermarks[0].exclusive_frontier == frontier
        assert proofs.watermarks[0].blocking_gap_count == 1
        assert any(gap.gap_id == "gap-exactly-at-frontier" for gap in proofs.gaps)
        store.release_writer_lease(lease)


def test_active_gap_inside_watermark_prefix_is_rejected(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        stream_id = scenario.coverage.segments[0].stream_id
        _seed_active_gap(
            store,
            lease,
            gap_id="gap-inside-prefix",
            stream_id=stream_id,
            request_id=str(scenario.request_id),
            start=scenario.coverage.watermarks[0].coverage_start,
            end=scenario.coverage.watermarks[0].coverage_start + timedelta(minutes=5),
            detected_at=clock.value,
        )

        with pytest.raises(RestartProjectionIntegrityError):
            RestartProjectionReader(store).load_stream_proofs((stream_id,))
        store.release_writer_lease(lease)


def test_all_blocked_request_projects_abandoned_publication_for_run_reconciliation(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        repository = IngestionExecutionRepository(store)
        repository.fail_processing_request(
            lease,
            AttemptIdentity(
                attempt_id=scenario.attempt_id,
                request_instance_id=scenario.request_id,
                attempt_number=1,
            ),
            terminal_status=RequestInstanceStatus.FAILED,
            category="VALIDATION",
            code="ALL_STREAMS_BLOCKED",
            sanitized_message="Every requested stream failed deterministic validation.",
        )

        request = RestartProjectionReader(store).load_run(_RUN_ID).requests[0]

        assert request.action is RestartAction.RECONCILE_RUN
        assert request.publication is not None
        assert request.publication.state is PublicationRecoveryState.ABANDONED
        assert not request.publication.publication_committed
        store.release_writer_lease(lease)
