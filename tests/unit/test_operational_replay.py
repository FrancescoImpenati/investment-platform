"""Cross-run raw adoption and state-first canonical-loss recovery."""

from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import UUID

import pytest

from investment_platform.data.ingestion.identity import AttemptIdentity
from investment_platform.data.operational.execution import IngestionExecutionRepository
from investment_platform.data.operational.planning import (
    IngestionPlanRepository,
)
from investment_platform.data.operational.replay import (
    CanonicalLossCondition,
    CanonicalLossState,
    OperationalReplayRepository,
    RawReplayEligibility,
    RawReplayReason,
    ReplayIdentityCollisionError,
    ReplayIntegrityError,
    ReplayStateConflictError,
)
from investment_platform.data.operational.store import OperationalStateStore
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.data_root import PrivateDataRoot
from tests.unit.test_ingestion_execution_repository import (
    _END,
    _NOW,
    _SECOND_ATTEMPT_ID,
    _SECOND_RUN_ID,
    _START,
    MutableClock,
    Scenario,
    _plan,
    _prepare_scenario,
    _private_root,
)

pytestmark = pytest.mark.unit


def _commit_and_invalidate(
    private_root: PrivateDataRoot,
    store: OperationalStateStore,
    clock: MutableClock,
) -> tuple[Scenario, OperationalReplayRepository, RawReplayEligibility]:
    scenario = _prepare_scenario(private_root, store, clock)
    lease = store.get_writer_lease()
    assert lease is not None
    IngestionExecutionRepository(store).commit_published_batch(
        lease,
        scenario.commit_request(),
    )
    canonical_file = (
        private_root.root
        / scenario.published.relative_directory
        / scenario.manifest.files[0].relative_path
    )
    canonical_file.unlink()
    reconciliation = OperationalReplayRepository(store).reconcile_canonical_loss(
        lease,
        scenario.manifest.canonical_batch_id,
    )
    assert reconciliation.state is CanonicalLossState.INVALIDATED
    assert reconciliation.replay_eligibility is not None
    return scenario, OperationalReplayRepository(store), reconciliation.replay_eligibility


def _new_running_attempt(
    store: OperationalStateStore,
    clock: MutableClock,
    scenario: Scenario,
) -> tuple[AttemptIdentity, AcquisitionPolicyAuthorization]:
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    request = _plan(enforcer).model_copy(update={"run_id": _SECOND_RUN_ID})
    lease = store.get_writer_lease()
    assert lease is not None
    IngestionPlanRepository(store).persist(lease, request)
    progress = IngestionPlanRepository(store).load_progress(_SECOND_RUN_ID)
    request_id = progress.requests[0].request_instance_id
    identity = AttemptIdentity(
        attempt_id=_SECOND_ATTEMPT_ID,
        request_instance_id=request_id,
        attempt_number=1,
    )
    planned = request.plan.requests[0]
    IngestionExecutionRepository(store).begin_attempt(lease, identity, planned.authorization)
    descriptor = scenario.acquisition.ordered_artifacts[0]
    page = enforcer.authorize_response_page(
        planned.authorization,
        page_ordinal=descriptor.page_ordinal,
        page_relation=descriptor.page_relation,
        payload_sha256=descriptor.content_sha256,
        payload_size_bytes=descriptor.byte_count,
        canonical_media_type=descriptor.media_type,
        content_encoding=descriptor.content_encoding,
        observed_start=_START,
        observed_end=_END,
    )
    acquisition = enforcer.authorize_completed_acquisition(
        planned.authorization,
        (page,),
        pagination_complete=True,
        terminal_page_verified=True,
    )
    return identity, acquisition


def test_raw_replay_requires_exact_durable_canonical_evidence(tmp_path: Path) -> None:
    private_root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(private_root, clock=clock) as store:
        scenario = _prepare_scenario(private_root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        fake = RawReplayEligibility(
            reason=RawReplayReason.CANONICAL_LOSS,
            canonical_batch_id=scenario.manifest.canonical_batch_id,
            evidence_gap_ids=("gap-not-durable",),
        )

        with pytest.raises(ReplayStateConflictError, match="INVALID canonical batch"):
            OperationalReplayRepository(store).find_latest_replayable_acquisition(
                scenario.expectation.specification,
                fake,
            )


def test_complete_raw_is_adopted_cross_run_and_survives_restart(tmp_path: Path) -> None:
    private_root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(private_root, clock=clock) as store:
        scenario, replay_repository, eligibility = _commit_and_invalidate(
            private_root,
            store,
            clock,
        )
        replay = replay_repository.find_latest_replayable_acquisition(
            scenario.expectation.specification,
            eligibility,
        )
        assert replay is not None
        assert replay.eligibility == eligibility
        durable_context = replay_repository.load_batch_preparation(
            scenario.expectation.batch_context.batch_context_id
        )
        assert durable_context is not None
        assert durable_context.batch_context == scenario.expectation.batch_context
        assert durable_context.provenance == scenario.expectation.provenance
        assert (
            replay_repository.find_replay_eligibility(scenario.expectation.specification)
            == eligibility
        )
        identity, authorization = _new_running_attempt(store, clock, scenario)
        lease = store.get_writer_lease()
        assert lease is not None

        adopted = replay_repository.adopt_acquisition(
            lease,
            identity,
            authorization,
            replay,
        )
        idempotent = replay_repository.adopt_acquisition(
            lease,
            identity,
            authorization,
            replay,
        )

        assert not adopted.replayed
        assert idempotent.replayed
        assert adopted.ordered_artifact_ids == (scenario.raw_identity.artifact_id,)
        with store.read_only_connection() as connection:
            assert (
                connection.execute(
                    "SELECT status FROM request_attempts WHERE attempt_id = ?",
                    (str(identity.attempt_id),),
                ).fetchone()[0]
                == "RAW_COMPLETE"
            )
            assert (
                connection.execute(
                    "SELECT status FROM request_instances WHERE request_instance_id = ?",
                    (str(identity.request_instance_id),),
                ).fetchone()[0]
                == "RAW_COMPLETE"
            )
            adoption_row = connection.execute(
                """
                SELECT source_attempt_id, canonical_batch_id, replay_reason
                FROM raw_acquisition_adoptions WHERE attempt_id = ?
                """,
                (str(identity.attempt_id),),
            ).fetchone()
            assert adoption_row is not None
            assert tuple(adoption_row) == (
                str(scenario.attempt_id),
                eligibility.canonical_batch_id,
                eligibility.reason.value,
            )
            assert connection.execute("SELECT count(*) FROM raw_artifacts").fetchone()[0] == 1
            assert (
                connection.execute("SELECT count(*) FROM raw_replay_provenance").fetchone()[0] == 2
            )

    with OperationalStateStore.open(private_root, clock=clock) as reopened:
        restarted_repository = OperationalReplayRepository(reopened)
        recovered = restarted_repository.load_adopted_replay(_SECOND_ATTEMPT_ID)
        assert recovered is not None
        assert recovered.ordered_artifact_ids == (scenario.raw_identity.artifact_id,)
        restarted_context = restarted_repository.load_batch_preparation(
            scenario.expectation.batch_context.batch_context_id
        )
        assert restarted_context is not None
        assert restarted_context.batch_context == scenario.expectation.batch_context


def test_adoption_fails_closed_when_authorization_bytes_collide(tmp_path: Path) -> None:
    private_root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(private_root, clock=clock) as store:
        scenario, repository, eligibility = _commit_and_invalidate(
            private_root,
            store,
            clock,
        )
        replay = repository.find_latest_replayable_acquisition(
            scenario.expectation.specification,
            eligibility,
        )
        assert replay is not None
        identity, authorization = _new_running_attempt(store, clock, scenario)
        lease = store.get_writer_lease()
        assert lease is not None
        descriptor = authorization.ordered_artifacts[0]
        changed_descriptor = descriptor.model_copy(
            update={"content_sha256": hashlib.sha256(b"different synthetic page").hexdigest()}
        )
        changed = authorization.model_copy(update={"ordered_artifacts": (changed_descriptor,)})

        with pytest.raises(ReplayIdentityCollisionError, match="retained raw identity"):
            repository.adopt_acquisition(lease, identity, changed, replay)


def test_canonical_loss_is_state_first_and_exact_republication_reactivates(
    tmp_path: Path,
) -> None:
    private_root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(private_root, clock=clock) as store:
        scenario = _prepare_scenario(private_root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        file_path = (
            private_root.root
            / scenario.published.relative_directory
            / scenario.manifest.files[0].relative_path
        )
        original_file = file_path.read_bytes()
        file_path.unlink()
        repository = OperationalReplayRepository(store)

        invalidated = repository.reconcile_canonical_loss(
            lease,
            scenario.manifest.canonical_batch_id,
        )
        repeated = repository.reconcile_canonical_loss(
            lease,
            scenario.manifest.canonical_batch_id,
        )

        assert invalidated.state is CanonicalLossState.INVALIDATED
        assert repeated.state is CanonicalLossState.ALREADY_INVALID
        assert invalidated.targets[0].condition is CanonicalLossCondition.ABSENT
        assert invalidated.replay_eligibility is not None
        replay = repository.find_latest_replayable_acquisition(
            scenario.expectation.specification,
            invalidated.replay_eligibility,
        )
        assert replay is not None
        identity, authorization = _new_running_attempt(store, clock, scenario)
        repository.adopt_acquisition(lease, identity, authorization, replay)
        durable_context = repository.load_batch_preparation(
            scenario.expectation.batch_context.batch_context_id
        )
        assert durable_context is not None
        execution = IngestionExecutionRepository(store)
        execution.record_batch_context(
            lease,
            identity,
            scenario.expectation.specification,
            durable_context.batch_context,
            calendar_snapshot_id=durable_context.calendar_snapshot_id,
            provenance=durable_context.provenance,
        )
        execution.prepare_publication(lease, identity, scenario.expectation)
        with store.read_only_connection() as connection:
            assert (
                connection.execute(
                    "SELECT state FROM canonical_batches WHERE canonical_batch_id = ?",
                    (scenario.manifest.canonical_batch_id,),
                ).fetchone()[0]
                == "INVALID"
            )
            assert (
                connection.execute("SELECT verification_state FROM coverage_segments").fetchone()[0]
                == "INVALID"
            )
            assert (
                connection.execute("SELECT verification_state FROM watermarks").fetchone()[0]
                == "INVALID"
            )
            gap_row = connection.execute(
                """
                SELECT gap_id, gap_type, status, detected_at FROM gaps
                WHERE canonical_batch_id = ? AND gap_type = 'INTEGRITY'
                """,
                (scenario.manifest.canonical_batch_id,),
            ).fetchone()
            assert gap_row is not None
            first_gap_id = str(gap_row["gap_id"])
            first_detected_at = str(gap_row["detected_at"])
            assert tuple(gap_row)[1:3] == ("INTEGRITY", "OPEN")

        file_path.write_bytes(original_file)
        restored = repository.reactivate_identical_canonical_batch(
            lease,
            identity,
            scenario.expectation,
            scenario.manifest,
            scenario.published,
        )

        assert restored.state == "VERIFIED"
        assert restored.restored_coverage_count == 1
        assert restored.restored_watermark_count == 1
        assert restored.resolved_gap_count == 1
        with store.read_only_connection() as connection:
            assert (
                connection.execute(
                    "SELECT state FROM canonical_batches WHERE canonical_batch_id = ?",
                    (scenario.manifest.canonical_batch_id,),
                ).fetchone()[0]
                == "VERIFIED"
            )
            assert (
                connection.execute(
                    "SELECT status FROM request_attempts WHERE attempt_id = ?",
                    (str(identity.attempt_id),),
                ).fetchone()[0]
                == "SUCCESS"
            )
            assert (
                connection.execute(
                    "SELECT status FROM request_instances WHERE request_instance_id = ?",
                    (str(identity.request_instance_id),),
                ).fetchone()[0]
                == "SUCCESS"
            )
            assert (
                connection.execute(
                    "SELECT commit_source FROM publication_commits WHERE request_instance_id = ?",
                    (str(identity.request_instance_id),),
                ).fetchone()[0]
                == "RECOVERY_ADOPTION"
            )

        file_path.unlink()
        clock.advance()
        lost_again = repository.reconcile_canonical_loss(
            lease,
            scenario.manifest.canonical_batch_id,
        )
        assert lost_again.state is CanonicalLossState.INVALIDATED
        with store.read_only_connection() as connection:
            recurring = connection.execute(
                """
                SELECT gap_id, status, detected_at FROM gaps
                WHERE canonical_batch_id = ? AND gap_type = 'INTEGRITY'
                """,
                (scenario.manifest.canonical_batch_id,),
            ).fetchall()
        assert len(recurring) == 1
        assert str(recurring[0]["gap_id"]) == first_gap_id
        assert str(recurring[0]["status"]) == "OPEN"
        assert str(recurring[0]["detected_at"]) > first_detected_at


def test_canonical_reactivation_rejects_nonidentical_bytes(tmp_path: Path) -> None:
    private_root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(private_root, clock=clock) as store:
        scenario = _prepare_scenario(private_root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        file_path = (
            private_root.root
            / scenario.published.relative_directory
            / scenario.manifest.files[0].relative_path
        )
        file_path.write_bytes(b"corrupt synthetic canonical bytes")
        repository = OperationalReplayRepository(store)
        invalidated = repository.reconcile_canonical_loss(
            lease,
            scenario.manifest.canonical_batch_id,
        )
        assert invalidated.targets[0].condition is CanonicalLossCondition.CORRUPT

        with pytest.raises(ReplayIntegrityError, match="failed exact verification"):
            repository.reactivate_identical_canonical_batch(
                lease,
                AttemptIdentity(
                    attempt_id=_SECOND_ATTEMPT_ID,
                    request_instance_id=scenario.request_id,
                    attempt_number=1,
                ),
                scenario.expectation,
                scenario.manifest,
                scenario.published,
            )


def test_replay_eligibility_rejects_unsorted_or_duplicate_evidence() -> None:
    batch_id = f"batch_v1_{'a' * 64}"
    with pytest.raises(ValueError, match="unique and sorted"):
        RawReplayEligibility(
            reason=RawReplayReason.CANONICAL_LOSS,
            canonical_batch_id=batch_id,
            evidence_gap_ids=("gap-b", "gap-a", "gap-a"),
        )


def test_dedicated_raw_replay_uses_repair_run_without_network_attempt(tmp_path: Path) -> None:
    private_root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    operation_id = UUID("40000000-0000-4000-8000-000000000001")
    with OperationalStateStore.open(private_root, clock=clock) as store:
        scenario = _prepare_scenario(private_root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        file_path = (
            private_root.root
            / scenario.published.relative_directory
            / scenario.manifest.files[0].relative_path
        )
        original = file_path.read_bytes()
        file_path.unlink()
        repository = OperationalReplayRepository(store)
        repository.reconcile_canonical_loss(lease, scenario.manifest.canonical_batch_id)

        planned = repository.plan_raw_replay_operation(
            lease,
            operation_id,
            scenario.expectation.specification,
        )
        running = repository.start_raw_replay_operation(lease, operation_id)
        replay = repository.find_latest_replayable_acquisition(
            running.specification,
            running.eligibility,
        )
        assert planned.status.value == "PLANNED"
        assert running.status.value == "RUNNING"
        assert replay is not None
        file_path.write_bytes(original)
        result = repository.complete_raw_replay_operation(
            lease,
            operation_id,
            scenario.expectation,
            scenario.manifest,
            scenario.published,
        )
        repeated = repository.complete_raw_replay_operation(
            lease,
            operation_id,
            scenario.expectation,
            scenario.manifest,
            scenario.published,
        )

        assert not result.replayed
        assert repeated.replayed
        assert result.restored_coverage_count == 1
        with store.read_only_connection() as connection:
            assert connection.execute(
                "SELECT mode, status FROM ingestion_runs WHERE run_id = ?",
                (str(operation_id),),
            ).fetchone()[0:2] == ("REPAIR", "SUCCESS")
            assert connection.execute("SELECT count(*) FROM request_attempts").fetchone()[0] == 1

    with OperationalStateStore.open(private_root, clock=clock) as reopened:
        operation = OperationalReplayRepository(reopened).load_raw_replay_operation(operation_id)
        assert operation is not None
        assert operation.status.value == "SUCCESS"
