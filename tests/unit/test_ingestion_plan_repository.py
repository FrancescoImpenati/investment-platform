"""Offline persistence, replay, resume, and fencing tests for ingestion plans."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Generator
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from investment_platform.data.calendar import CalendarSession, CalendarSnapshot
from investment_platform.data.ingestion import (
    BarSemantics,
    CoverageClassification,
    CoverageVerificationState,
    DataKind,
    IngestionIntent,
    IngestionPlan,
    IngestionPlanner,
    PlannerBudget,
    PlannerLimits,
    ProviderInstrumentMapping,
    RepairStrategy,
    StreamKey,
    VerifiedCoverageProjection,
)
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.operational import (
    CalendarSnapshotRepository,
    IngestionPlanRepository,
    PlanCalendarMismatchError,
    PlanIdentityCollisionError,
    PlanPersistenceRequest,
    PlanPolicyMismatchError,
    PlanRepositoryError,
    RequestInstanceStatus,
    RequestResumeAction,
    WriterLease,
    WriterLeaseLostError,
    deterministic_calendar_snapshot_id,
)
from investment_platform.data.operational.store import OperationalStateStore
from investment_platform.data.retention import RetentionPolicyCatalog, RetentionPolicyEnforcer
from investment_platform.data_root import PrivateDataRoot
from investment_platform.runtime import RuntimeEnvironment

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
_OPEN = datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
_CLOSE = datetime(2025, 7, 2, 14, tzinfo=UTC)
_INSTRUMENT = UUID("00000000-0000-4000-8000-000000000001")
_RUN_ID = UUID("10000000-0000-4000-8000-000000000001")


def _ordered_artifact_ids_hash(*artifact_ids: str) -> str:
    canonical = json.dumps(
        {
            "canonicalization_version": 1,
            "kind": "ordered-raw-artifacts",
            "payload": {"artifact_ids": list(artifact_ids)},
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


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
        tmp_path / "dedicated-plan-state-root",
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
    return store.acquire_writer_lease("plan-writer", timedelta(minutes=5))


def _snapshot() -> CalendarSnapshot:
    return CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="1",
        tzdata_version="test",
        calendar_name="SYNTHETIC_US_RTH",
        timezone_name="America/New_York",
        range_start=date(2025, 7, 2),
        range_end=date(2025, 7, 3),
        generated_at=_NOW,
        sessions=(
            CalendarSession(
                session_date=date(2025, 7, 2),
                open_utc=_OPEN,
                close_utc=_CLOSE,
            ),
        ),
    )


def _stream() -> StreamKey:
    return StreamKey(
        provider="synthetic",
        dataset="price_bars",
        data_kind=DataKind.PRICE_BAR,
        instrument_id=_INSTRUMENT,
        timeframe=Timeframe.FIVE_MINUTES,
        session=TradingSession.REGULAR,
        adjustment=AdjustmentState.UNADJUSTED,
        currency="USD",
        bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
    )


def _planner(clock: MutableClock) -> IngestionPlanner:
    return IngestionPlanner(
        RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    )


def _plan(
    clock: MutableClock,
    *,
    coverage: tuple[VerifiedCoverageProjection, ...] = (),
    intent: IngestionIntent = IngestionIntent.BACKFILL,
    repair_strategy: RepairStrategy | None = None,
    repair_reason: str | None = None,
    max_expected_observations_per_request: int = 2,
    max_run_dispatches: int = 3,
) -> IngestionPlan:
    return _planner(clock).plan(
        intent=intent,
        streams=(_stream(),),
        instrument_mappings=(
            ProviderInstrumentMapping(
                instrument_id=_INSTRUMENT,
                provider_identifier="SYNTHETIC-A",
            ),
        ),
        desired_start=_OPEN,
        desired_end=_CLOSE,
        calendar_snapshot=_snapshot(),
        coverage=coverage,
        limits=PlannerLimits(
            max_instruments_per_request=1,
            max_expected_observations_per_request=max_expected_observations_per_request,
            max_observations_per_page=2,
            max_pages_per_request=1,
            max_calls_per_request=1,
            max_estimated_bytes_per_request=1_000,
            estimated_bytes_per_observation=100,
            estimated_bytes_per_page=10,
            estimated_cost_per_call=Decimal("0.01"),
            max_estimated_cost_per_request=Decimal("0.01"),
        ),
        budget=PlannerBudget(
            max_calls=max_run_dispatches,
            max_expected_observations=6,
            max_pages=max_run_dispatches,
            max_estimated_bytes=1_000,
            max_estimated_cost=Decimal("0.01") * max_run_dispatches,
        ),
        environment=RuntimeEnvironment.TEST,
        mapping_semantic_version="synthetic-bars-request-v1",
        repair_strategy=repair_strategy,
        repair_reason=repair_reason,
    )


def _coverage(plan: IngestionPlan) -> VerifiedCoverageProjection:
    policy = plan.policy_authorization.policy_snapshot
    return VerifiedCoverageProjection(
        coverage_id="coverage-synthetic-complete",
        request_instance_id="request-synthetic-complete",
        canonical_batch_id="batch-synthetic-complete",
        policy_snapshot_id="policy-synthetic-current",
        active_policy_snapshot_id="policy-synthetic-current",
        calendar_snapshot_id="calendar-synthetic-current",
        stream=plan.streams[0],
        start=_OPEN,
        end=_CLOSE,
        classification=CoverageClassification.OBSERVED,
        verification_state=CoverageVerificationState.VERIFIED,
        retained=True,
        policy_valid=True,
        policy_id=policy.policy_id,
        policy_revision=policy.policy_revision,
        policy_hash=policy.policy_hash,
        active_policy_hash=policy.policy_hash,
        calendar_snapshot_checksum=plan.calendar_snapshot_checksum,
        relational_provenance_verified=True,
        interval_verified=True,
        request_completed=True,
        pagination_verified=True,
        canonical_batch_verified=True,
        canonical_file_count=1,
        raw_artifact_count=1,
        artifacts_present=True,
    )


def _persistence_request(
    clock: MutableClock,
    *,
    run_id: UUID = _RUN_ID,
    no_op: bool = False,
) -> PlanPersistenceRequest:
    plan = _plan(clock)
    if no_op:
        plan = _plan(clock, coverage=(_coverage(plan),))
    return PlanPersistenceRequest(
        run_id=run_id,
        calendar_snapshot_id=deterministic_calendar_snapshot_id(_snapshot()),
        reason="synthetic deterministic acceptance",
        max_attempts=3,
        plan=plan,
    )


def _persist_calendar(store: OperationalStateStore, lease: WriterLease) -> str:
    return CalendarSnapshotRepository(store).persist(lease, _snapshot())


def _write_private_file(root: PrivateDataRoot, relative_path: str, content: bytes) -> None:
    path = root.managed_path(Path(relative_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _advance_request_to_terminal(
    repository: IngestionPlanRepository,
    lease: WriterLease,
    *,
    run_id: UUID,
    terminal: RequestInstanceStatus,
) -> UUID:
    progress = repository.load_progress(run_id)
    request_id = progress.requests[0].request_instance_id
    expected = RequestInstanceStatus.PLANNED
    for new in (
        RequestInstanceStatus.DISPATCHING,
        RequestInstanceStatus.ACQUIRING,
        RequestInstanceStatus.RAW_COMPLETE,
        RequestInstanceStatus.PROCESSING,
        terminal,
    ):
        repository.transition_request_status(
            lease,
            run_id=run_id,
            request_instance_id=request_id,
            expected=expected,
            new=new,
        )
        expected = new
    return request_id


def _insert_verified_coverage_graph(
    store: OperationalStateStore,
    private_root: PrivateDataRoot,
    request: PlanPersistenceRequest,
    request_ids: tuple[UUID, ...],
) -> tuple[str, str, str]:
    planned = request.plan.requests[0]
    specification = planned.specification
    exact_raw_path = "raw/synthetic/exact.bin"
    exact_raw_manifest = "raw/synthetic/exact.manifest.json"
    alien_raw_path = "raw/synthetic/alien.bin"
    alien_raw_manifest = "raw/synthetic/alien.manifest.json"
    canonical_path = "normalized/synthetic/batch/part-00000.parquet"
    canonical_manifest = "normalized/synthetic/batch/manifest.json"
    exact_content = b"exact batch-context raw bytes"
    alien_content = b"alien correction bytes"
    canonical_content = b"previously reopened synthetic parquet bytes"
    for path, content in (
        (exact_raw_path, exact_content),
        (exact_raw_manifest, b"{}"),
        (alien_raw_path, alien_content),
        (alien_raw_manifest, b"{}"),
        (canonical_path, canonical_content),
        (canonical_manifest, b"{}"),
    ):
        _write_private_file(private_root, path, content)

    policy_id = request.plan.policy_authorization.policy_snapshot.policy_id
    with store._transaction() as connection:
        policy_row = connection.execute(
            "SELECT policy_snapshot_id FROM policy_snapshots WHERE policy_id = ?",
            (policy_id,),
        ).fetchone()
        assert policy_row is not None
        policy_snapshot_id = str(policy_row["policy_snapshot_id"])
        for ordinal, request_id in enumerate(request_ids, start=1):
            connection.execute(
                """
                INSERT INTO request_attempts(
                    attempt_id, request_instance_id, attempt_number, status,
                    started_at, completed_at, page_count,
                    pagination_complete, terminal_page_verified
                ) VALUES (?, ?, 1, 'SUCCESS', ?, ?, 1, 1, 1)
                """,
                (f"attempt-{ordinal}", str(request_id), _NOW.isoformat(), _NOW.isoformat()),
            )
        for artifact_id, path, manifest, content, relation_hash in (
            (
                "artifact-exact",
                exact_raw_path,
                exact_raw_manifest,
                exact_content,
                "1" * 64,
            ),
            (
                "artifact-alien",
                alien_raw_path,
                alien_raw_manifest,
                alien_content,
                "2" * 64,
            ),
        ):
            connection.execute(
                """
                INSERT INTO raw_artifacts(
                    artifact_id, request_spec_id, page_ordinal, page_relation_hash,
                    content_sha256, byte_count, media_type, content_encoding,
                    relative_path, manifest_relative_path, first_persisted_at,
                    verified_at, state
                ) VALUES (?, ?, 0, ?, ?, ?, 'application/json', 'identity', ?, ?, ?, ?, 'VERIFIED')
                """,
                (
                    artifact_id,
                    specification.request_spec_id,
                    relation_hash,
                    hashlib.sha256(content).hexdigest(),
                    len(content),
                    path,
                    manifest,
                    _NOW.isoformat(),
                    _NOW.isoformat(),
                ),
            )
        connection.execute(
            """
            INSERT INTO batch_contexts(
                batch_context_id, canonical_batch_id, request_spec_id,
                ordered_artifacts_hash, canonical_schema_version,
                normalizer_version, validator_version, calendar_snapshot_id,
                fixed_ingested_at, manifest_created_at, created_at
            ) VALUES (
                'context-exact', 'batch-exact', ?, ?, 'bars-v1',
                'normalizer-v1', 'validator-v1', ?, ?, ?, ?
            )
            """,
            (
                specification.request_spec_id,
                _ordered_artifact_ids_hash("artifact-exact"),
                request.calendar_snapshot_id,
                _NOW.isoformat(),
                _NOW.isoformat(),
                _NOW.isoformat(),
            ),
        )
        connection.execute(
            "INSERT INTO batch_context_artifacts(batch_context_id, artifact_id, ordinal) "
            "VALUES ('context-exact', 'artifact-exact', 0)"
        )
        for request_id in request_ids:
            connection.execute(
                """
                INSERT INTO batch_context_requests(
                    batch_context_id, request_instance_id, linked_at
                ) VALUES ('context-exact', ?, ?)
                """,
                (str(request_id), _NOW.isoformat()),
            )
        connection.execute(
            """
            INSERT INTO canonical_batches(
                canonical_batch_id, batch_context_id, policy_snapshot_id,
                relative_path, manifest_relative_path, state, row_count,
                published_at, verified_at
            ) VALUES (
                'batch-exact', 'context-exact', ?, 'normalized/synthetic/batch', ?,
                'VERIFIED', 2, ?, ?
            )
            """,
            (policy_snapshot_id, canonical_manifest, _NOW.isoformat(), _NOW.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO canonical_files(
                canonical_batch_id, file_ordinal, relative_path,
                content_sha256, byte_count, row_count, interval_start,
                interval_end, schema_fingerprint
            ) VALUES ('batch-exact', 0, ?, ?, ?, 2, ?, ?, 'schema-v1')
            """,
            (
                canonical_path,
                hashlib.sha256(canonical_content).hexdigest(),
                len(canonical_content),
                specification.start.isoformat(),
                specification.end.isoformat(),
            ),
        )
        connection.execute(
            """
            INSERT INTO canonical_batch_streams(
                canonical_batch_id, stream_id, outcome, row_count,
                interval_start, interval_end, validation_summary_json
            ) VALUES ('batch-exact', ?, 'PUBLISHABLE', 2, ?, ?, '{}')
            """,
            (
                request.plan.stream_ids[0],
                specification.start.isoformat(),
                specification.end.isoformat(),
            ),
        )
        for request_id in request_ids:
            connection.execute(
                """
                INSERT INTO canonical_batch_requests(
                    canonical_batch_id, request_instance_id, policy_snapshot_id, linked_at
                ) VALUES ('batch-exact', ?, ?, ?)
                """,
                (str(request_id), policy_snapshot_id, _NOW.isoformat()),
            )
        connection.execute(
            """
            INSERT INTO coverage_segments(
                coverage_id, stream_id, canonical_batch_id, calendar_snapshot_id,
                policy_snapshot_id, coverage_start, interval_start, interval_end,
                classification, verification_state, retained, row_count,
                artifact_count, request_completed, pagination_verified,
                generation, verified_at
            ) VALUES (
                'coverage-exact', ?, 'batch-exact', ?, ?, ?, ?, ?,
                'OBSERVED', 'VERIFIED', 1, 2, 1, 1, 1, 1, ?
            )
            """,
            (
                request.plan.stream_ids[0],
                request.calendar_snapshot_id,
                policy_snapshot_id,
                specification.start.isoformat(),
                specification.start.isoformat(),
                specification.end.isoformat(),
                _NOW.isoformat(),
            ),
        )
    return exact_raw_path, alien_raw_path, canonical_path


def test_plan_round_trip_restart_and_exact_replay_preserve_order_and_state(
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    request = _persistence_request(clock)
    with OperationalStateStore.open(private_root, clock=clock) as first:
        lease = first.acquire_writer_lease("plan-writer", timedelta(minutes=5))
        assert _persist_calendar(first, lease) == request.calendar_snapshot_id
        repository = IngestionPlanRepository(first)
        created = repository.persist(lease, request)
        progress = repository.load_progress(request.run_id)

        assert not created.replayed
        assert created.request_count == 3
        assert [value.plan_ordinal for value in progress.requests] == [0, 1, 2]
        assert [value.resume_action for value in progress.requests] == [
            RequestResumeAction.DISPATCH,
            RequestResumeAction.DISPATCH,
            RequestResumeAction.DISPATCH,
        ]

    with OperationalStateStore.open(private_root, clock=clock) as reopened:
        repository = IngestionPlanRepository(reopened)
        replay = repository.persist(lease, request)
        progress = repository.load_progress(request.run_id)

        assert replay.replayed
        assert replay.plan_hash == created.plan_hash == progress.plan_hash
        with reopened.read_only_connection() as connection:
            assert (
                int(
                    connection.execute(
                        "SELECT count(*) FROM ingestion_plan_streams WHERE run_id = ?",
                        (str(request.run_id),),
                    ).fetchone()[0]
                )
                == 1
            )
            assert (
                int(connection.execute("SELECT count(*) FROM request_plan_estimates").fetchone()[0])
                == 3
            )


def test_partial_request_states_are_restart_safe_and_replay_ignores_mutable_progress(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
) -> None:
    _persist_calendar(store, lease)
    request = _persistence_request(clock)
    repository = IngestionPlanRepository(store)
    repository.persist(lease, request)
    initial = repository.load_progress(request.run_id)

    repository.transition_request_status(
        lease,
        run_id=request.run_id,
        request_instance_id=initial.requests[0].request_instance_id,
        expected=RequestInstanceStatus.PLANNED,
        new=RequestInstanceStatus.DISPATCHING,
    )
    repository.transition_request_status(
        lease,
        run_id=request.run_id,
        request_instance_id=initial.requests[1].request_instance_id,
        expected=RequestInstanceStatus.PLANNED,
        new=RequestInstanceStatus.FAILED,
    )

    assert repository.persist(lease, request).replayed
    progress = repository.load_progress(request.run_id)
    assert [value.status for value in progress.requests] == [
        RequestInstanceStatus.DISPATCHING,
        RequestInstanceStatus.FAILED,
        RequestInstanceStatus.PLANNED,
    ]
    assert [value.resume_action for value in progress.requests] == [
        RequestResumeAction.RECONCILE_ACQUISITION,
        RequestResumeAction.NONE,
        RequestResumeAction.DISPATCH,
    ]


def test_no_op_persists_policy_calendar_and_target_stream_without_request_rows(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
) -> None:
    _persist_calendar(store, lease)
    request = _persistence_request(clock, run_id=UUID(int=2), no_op=True)

    persisted = IngestionPlanRepository(store).persist(lease, request)
    progress = IngestionPlanRepository(store).load_progress(request.run_id)

    assert persisted.no_op and persisted.request_count == 0
    assert progress.no_op and progress.status.value == "SUCCESS"
    with store.read_only_connection() as connection:
        assert (
            int(
                connection.execute(
                    "SELECT count(*) FROM ingestion_plan_streams WHERE run_id = ?",
                    (str(request.run_id),),
                ).fetchone()[0]
            )
            == 1
        )
        assert int(connection.execute("SELECT count(*) FROM request_instances").fetchone()[0]) == 0


def test_same_run_identity_with_different_plan_metadata_fails_without_overwrite(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
) -> None:
    _persist_calendar(store, lease)
    request = _persistence_request(clock)
    repository = IngestionPlanRepository(store)
    original = repository.persist(lease, request)
    collision = request.model_copy(update={"reason": "different deterministic reason"})

    with pytest.raises(PlanIdentityCollisionError, match="different ingestion plan"):
        repository.persist(lease, collision)

    assert repository.load_progress(request.run_id).plan_hash == original.plan_hash


def test_request_dispatch_headroom_is_allocated_within_the_run_ceiling(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
) -> None:
    _persist_calendar(store, lease)
    plan = _plan(
        clock,
        max_expected_observations_per_request=1,
        max_run_dispatches=7,
    )
    assert len(plan.requests) == 6
    request = PlanPersistenceRequest(
        run_id=UUID(int=7),
        calendar_snapshot_id=deterministic_calendar_snapshot_id(_snapshot()),
        reason="bounded multi-request dispatch allocation",
        max_attempts=3,
        max_pages=7,
        max_calls=7,
        max_pages_per_request=2,
        max_calls_per_request=2,
        plan=plan,
    )

    IngestionPlanRepository(store).persist(lease, request)

    with store.read_only_connection() as connection:
        rows = connection.execute(
            """
            SELECT limits.max_pages, limits.max_calls
            FROM request_instances AS instance
            JOIN request_execution_limits AS limits
              ON limits.request_instance_id = instance.request_instance_id
            WHERE instance.run_id = ? ORDER BY instance.plan_ordinal
            """,
            (str(request.run_id),),
        ).fetchall()
    allocated = [tuple(int(value) for value in row) for row in rows]
    assert allocated == [(2, 2), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)]
    assert sum(value[0] for value in allocated) == request.max_pages
    assert sum(value[1] for value in allocated) == request.max_calls


def test_repair_strategy_and_reason_are_immutable_plan_provenance(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
) -> None:
    _persist_calendar(store, lease)
    repair_plan = _plan(
        clock,
        intent=IngestionIntent.REPAIR,
        repair_strategy=RepairStrategy.PROVIDER_REFRESH,
        repair_reason="verify deterministic provider correction window",
    )
    request = PlanPersistenceRequest(
        run_id=UUID(int=4),
        calendar_snapshot_id=deterministic_calendar_snapshot_id(_snapshot()),
        reason="synthetic correction reconciliation",
        max_attempts=2,
        plan=repair_plan,
    )

    IngestionPlanRepository(store).persist(lease, request)
    with store.read_only_connection() as connection:
        row = connection.execute(
            "SELECT acquisition_strategy, repair_strategy, repair_reason, max_attempts "
            "FROM ingestion_plan_records WHERE run_id = ?",
            (str(request.run_id),),
        ).fetchone()

    assert row is not None
    assert tuple(row) == (
        "NETWORK",
        "PROVIDER_REFRESH",
        "verify deterministic provider correction window",
        2,
    )


def test_calendar_and_exact_active_policy_mismatches_fail_closed(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
) -> None:
    request = _persistence_request(clock)
    repository = IngestionPlanRepository(store)
    with pytest.raises(PlanCalendarMismatchError, match="persisted before"):
        repository.persist(lease, request)

    _persist_calendar(store, lease)
    wrong_scope = request.model_copy(
        update={"plan": request.plan.model_copy(update={"dataset": "different_dataset"})}
    )
    with pytest.raises(PlanPolicyMismatchError, match="exact policy dataset"):
        repository.persist(lease, wrong_scope)

    repository.persist(lease, request)
    with store._transaction() as connection:
        connection.execute(
            "UPDATE dataset_policy_status SET status = 'SUSPENDED' "
            "WHERE provider = 'synthetic' AND dataset = 'price_bars'"
        )
    with pytest.raises(PlanPolicyMismatchError, match="active dataset policy"):
        repository.persist(lease, _persistence_request(clock, run_id=UUID(int=3)))


def test_mid_transaction_lease_expiry_rolls_back_the_complete_plan(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _persist_calendar(store, lease)
    request = _persistence_request(clock)
    repository = IngestionPlanRepository(store)
    original = repository._persist_stream

    def expire_after_stream(*args: object, **kwargs: object) -> None:
        original(*args, **kwargs)  # type: ignore[arg-type]
        clock.advance(timedelta(minutes=6))

    monkeypatch.setattr(repository, "_persist_stream", expire_after_stream)
    with pytest.raises(WriterLeaseLostError, match="before operational commit"):
        repository.persist(lease, request)

    with store.read_only_connection() as connection:
        assert int(connection.execute("SELECT count(*) FROM ingestion_runs").fetchone()[0]) == 0
        assert int(connection.execute("SELECT count(*) FROM policy_snapshots").fetchone()[0]) == 0
        assert int(connection.execute("SELECT count(*) FROM stream_keys").fetchone()[0]) == 0


def test_forged_lease_is_rejected_before_any_plan_metadata(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
) -> None:
    _persist_calendar(store, lease)
    forged = replace(lease, generation=lease.generation + 1)
    with pytest.raises(WriterLeaseLostError, match="does not authorize"):
        IngestionPlanRepository(store).persist(forged, _persistence_request(clock))

    with store.read_only_connection() as connection:
        assert int(connection.execute("SELECT count(*) FROM ingestion_runs").fetchone()[0]) == 0


def test_coverage_reader_is_read_only_and_does_not_invent_verified_empty(
    store: OperationalStateStore,
) -> None:
    before = store._connection.total_changes
    result = IngestionPlanRepository(store).load_planner_coverage(
        (_stream(),),
        calendar_snapshot_id=deterministic_calendar_snapshot_id(_snapshot()),
    )

    assert result.windows == ()
    assert result.rejected_count == 0
    assert store._connection.total_changes == before


def test_coverage_uses_exact_batch_context_raw_and_current_file_integrity(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
    private_root: PrivateDataRoot,
) -> None:
    _persist_calendar(store, lease)
    first = _persistence_request(clock, run_id=UUID(int=10))
    second = _persistence_request(clock, run_id=UUID(int=11))
    repository = IngestionPlanRepository(store)
    repository.persist(lease, first)
    repository.persist(lease, second)
    first_id = _advance_request_to_terminal(
        repository,
        lease,
        run_id=first.run_id,
        terminal=RequestInstanceStatus.SUCCESS,
    )
    second_id = _advance_request_to_terminal(
        repository,
        lease,
        run_id=second.run_id,
        terminal=RequestInstanceStatus.SUCCESS,
    )
    exact_raw, alien_raw, canonical = _insert_verified_coverage_graph(
        store,
        private_root,
        first,
        (first_id, second_id),
    )

    initial = repository.load_planner_coverage(
        first.plan.streams,
        calendar_snapshot_id=first.calendar_snapshot_id,
    )
    assert len(initial.windows) == 1
    assert initial.windows[0].request_instance_id == str(min(first_id, second_id))

    # Repointing the relation to another intact version with the same request
    # spec must break the batch context's immutable ordered-artifact commitment.
    with store._transaction() as connection:
        connection.execute(
            "UPDATE batch_context_artifacts SET artifact_id = 'artifact-alien' "
            "WHERE batch_context_id = 'context-exact' AND ordinal = 0"
        )
    relation_tampering = repository.load_planner_coverage(
        first.plan.streams,
        calendar_snapshot_id=first.calendar_snapshot_id,
    )
    assert relation_tampering.windows == () and relation_tampering.rejected_count == 1
    with store._transaction() as connection:
        connection.execute(
            "UPDATE batch_context_artifacts SET artifact_id = 'artifact-exact' "
            "WHERE batch_context_id = 'context-exact' AND ordinal = 0"
        )

    # The batch-context relation and the immutable artifact must agree on the
    # exact page position; cardinality alone is not sufficient provenance.
    with store._transaction() as connection:
        connection.execute(
            "UPDATE raw_artifacts SET page_ordinal = 1 WHERE artifact_id = 'artifact-exact'"
        )
    ordinal_mismatch = repository.load_planner_coverage(
        first.plan.streams,
        calendar_snapshot_id=first.calendar_snapshot_id,
    )
    assert ordinal_mismatch.windows == () and ordinal_mismatch.rejected_count == 1
    with store._transaction() as connection:
        connection.execute(
            "UPDATE raw_artifacts SET page_ordinal = 0 WHERE artifact_id = 'artifact-exact'"
        )

    # A second raw correction for the same request spec is not part of this
    # batch context and therefore cannot affect its exact proof.
    _write_private_file(private_root, alien_raw, b"corrupted unrelated correction")
    unrelated_corruption = repository.load_planner_coverage(
        first.plan.streams,
        calendar_snapshot_id=first.calendar_snapshot_id,
    )
    assert len(unrelated_corruption.windows) == 1

    _write_private_file(private_root, exact_raw, b"corrupted exact raw artifact")
    raw_corruption = repository.load_planner_coverage(
        first.plan.streams,
        calendar_snapshot_id=first.calendar_snapshot_id,
    )
    assert raw_corruption.windows == () and raw_corruption.rejected_count == 1

    _write_private_file(private_root, exact_raw, b"exact batch-context raw bytes")
    private_root.managed_path(Path(canonical)).unlink()
    canonical_missing = repository.load_planner_coverage(
        first.plan.streams,
        calendar_snapshot_id=first.calendar_snapshot_id,
    )
    assert canonical_missing.windows == () and canonical_missing.rejected_count == 1


def test_publishable_coverage_accepts_complete_partial_request_proof(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
    private_root: PrivateDataRoot,
) -> None:
    _persist_calendar(store, lease)
    request = _persistence_request(clock, run_id=UUID(int=12))
    repository = IngestionPlanRepository(store)
    repository.persist(lease, request)
    request_id = _advance_request_to_terminal(
        repository,
        lease,
        run_id=request.run_id,
        terminal=RequestInstanceStatus.PARTIAL,
    )
    _insert_verified_coverage_graph(store, private_root, request, (request_id,))

    result = repository.load_planner_coverage(
        request.plan.streams,
        calendar_snapshot_id=request.calendar_snapshot_id,
    )

    assert len(result.windows) == 1
    assert result.windows[0].request_instance_id == str(request_id)


def test_replay_rechecks_complete_stream_row_and_rejects_tampering(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
) -> None:
    _persist_calendar(store, lease)
    request = _persistence_request(clock, run_id=UUID(int=13))
    repository = IngestionPlanRepository(store)
    repository.persist(lease, request)
    with store._transaction() as connection:
        connection.execute(
            "UPDATE stream_keys SET dimensions_json = '{}' WHERE stream_id = ?",
            (request.plan.stream_ids[0],),
        )

    with pytest.raises(PlanIdentityCollisionError, match="stream ID collides"):
        repository.persist(lease, request)


@pytest.mark.parametrize(
    ("field", "unsafe"),
    (
        ("reason", "secret=must-not-persist"),
        ("repair_reason", "https://authenticated.invalid/private"),
    ),
)
def test_repository_rejects_sensitive_reason_even_after_model_copy_bypass(
    store: OperationalStateStore,
    lease: WriterLease,
    clock: MutableClock,
    field: str,
    unsafe: str,
) -> None:
    repair_plan = _plan(
        clock,
        intent=IngestionIntent.REPAIR,
        repair_strategy=RepairStrategy.MISSING_ONLY,
        repair_reason="repair verified missing interval",
    )
    request = PlanPersistenceRequest(
        run_id=UUID(int=14),
        calendar_snapshot_id=deterministic_calendar_snapshot_id(_snapshot()),
        reason="safe repair reason",
        max_attempts=2,
        plan=repair_plan,
    )
    if field == "reason":
        request = request.model_copy(update={"reason": unsafe})
    else:
        request = request.model_copy(
            update={"plan": request.plan.model_copy(update={"repair_reason": unsafe})}
        )

    with pytest.raises(PlanRepositoryError, match="unsafe persisted text"):
        IngestionPlanRepository(store).persist(lease, request)
