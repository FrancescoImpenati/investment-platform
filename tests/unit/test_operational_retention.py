"""Offline acceptance tests for state-first retention invalidation and purge recovery."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from uuid import UUID, uuid4

import pytest

from investment_platform.data.operational.retention import (
    PurgeDeletionStatus,
    PurgePlan,
    PurgeRunStatus,
    PurgeTargetType,
    RetentionFaultPoint,
    RetentionLifecycleRepository,
    RetentionLifecycleStateError,
    RetentionPurgeSafetyError,
)
from investment_platform.data.operational.store import (
    OperationalStateStore,
    WriterLease,
    _format_utc,
)
from investment_platform.data.retention import (
    DatasetPolicyDenied,
    DatasetPolicyStatus,
    DatasetRetentionPolicy,
    LayerRetentionPolicy,
    RetentionCatalogDocument,
    RetentionMode,
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage.living_raw import raw_artifact_relative_directory
from investment_platform.data_root import PrivateDataRoot, PrivateDataRootError
from investment_platform.runtime import RuntimeEnvironment

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
_RAW_ID = f"raw_v1_{'a' * 64}"
_BATCH_ID = f"batch_v1_{'b' * 64}"
_POLICY_SNAPSHOT_ID = "policy_snapshot_test_retention_v1"
_RUN_ID = "run-test-retention"
_STREAM_ID = "stream_v1_test_retention"
_REQUEST_SPEC_ID = "request_v1_test_retention"
_CALENDAR_ID = "calendar_v1_test_retention"
_BATCH_CONTEXT_ID = f"batch_context_v1_{'b' * 64}"
_QUARANTINE_ID = "quarantine-test-1"


class SimulatedCrash(RuntimeError):
    pass


@dataclass(slots=True)
class Scenario:
    root: PrivateDataRoot
    store: OperationalStateStore
    lease: WriterLease
    repository: RetentionLifecycleRepository
    policy: DatasetRetentionPolicy
    raw_directory: Path
    canonical_directory: Path
    canonical_file: Path
    quarantine_directory: Path


def _clock() -> datetime:
    return _NOW


def _policy(mode: RetentionMode) -> DatasetRetentionPolicy:
    layer = LayerRetentionPolicy(
        mode=mode,
        ttl_seconds=3600 if mode is RetentionMode.TTL else None,
        quarantine_allowed=True,
    )
    provider = "test_ttl" if mode is RetentionMode.TTL else "test_subscription"
    return DatasetRetentionPolicy(
        policy_id=f"policy-{provider}-price-bars",
        revision=1,
        provider=provider,
        dataset="price_bars",
        mode=mode,
        status=DatasetPolicyStatus.ACTIVE,
        permitted_environments=(RuntimeEnvironment.PRIVATE_RESEARCH,),
        use_scope="Private synthetic retention lifecycle tests.",
        processing_allowed=True,
        raw=layer,
        normalized=layer,
        derived=LayerRetentionPolicy(mode=RetentionMode.PROHIBITED),
        delete_on_termination=True,
        evidence_reference="tests/unit/test_operational_retention.py",
        verified_on=date(2026, 8, 31),
        notes="Synthetic exact-key policy used only by offline unit tests.",
    )


def _catalog(policy: DatasetRetentionPolicy) -> RetentionPolicyCatalog:
    return RetentionPolicyCatalog(
        RetentionCatalogDocument(
            schema_version=1,
            catalog_id="test-operational-retention",
            revision=1,
            policies=(policy,),
        )
    )


def _private_root(tmp_path: Path) -> PrivateDataRoot:
    repository_root = Path(__file__).parents[2].resolve(strict=True)
    root = PrivateDataRoot(
        tmp_path.parent / f"p2-ret-{uuid4().hex[:8]}",
        repository_root,
        allow_temporary_for_tests=True,
    )
    root.initialize(created_at=_NOW)
    return root


def _canonical_directory(policy: DatasetRetentionPolicy) -> PurePosixPath:
    return PurePosixPath(
        "normalized",
        "price_bars",
        f"provider={policy.provider}",
        f"dataset={policy.dataset}",
        "batches",
        f"batch={_BATCH_ID.removeprefix('batch_v1_')[:32]}",
    )


def _quarantine_directory(policy: DatasetRetentionPolicy) -> PurePosixPath:
    return PurePosixPath(
        "quarantine",
        f"provider={policy.provider}",
        f"dataset={policy.dataset}",
        "artifacts",
        f"artifact={_QUARANTINE_ID}",
    )


def _write_managed(
    root: PrivateDataRoot,
    root_id: UUID,
    relative: PurePosixPath,
    payload: bytes,
) -> Path:
    parent = root.ensure_directory(
        Path(*relative.parent.parts),
        expected_root_id=root_id,
    )
    path = parent / relative.name
    path.write_bytes(payload)
    return path


def _seed_operational_state(
    scenario: Scenario,
    *,
    expires_at: datetime | None,
) -> None:
    policy = scenario.policy
    root_id = UUID(scenario.store.root_id)
    raw_relative = raw_artifact_relative_directory(policy.provider, policy.dataset, _RAW_ID)
    canonical_relative = _canonical_directory(policy)
    canonical_file_relative = (
        canonical_relative / "timeframe=5m" / "year=2025" / "month=07" / "part-0000.parquet"
    )
    raw_payload = b'{"synthetic":true}\n'
    parquet_payload = b"synthetic-parquet-placeholder"
    _write_managed(scenario.root, root_id, raw_relative / "payload.bin", raw_payload)
    _write_managed(scenario.root, root_id, raw_relative / "manifest.json", b"{}\n")
    _write_managed(scenario.root, root_id, canonical_relative / "manifest.json", b"{}\n")
    _write_managed(scenario.root, root_id, canonical_file_relative, parquet_payload)
    _write_managed(
        scenario.root,
        root_id,
        _quarantine_directory(policy) / "manifest.json",
        b"{}\n",
    )

    timestamp = _format_utc(_NOW)
    start = _format_utc(datetime(2025, 7, 2, 13, 30, tzinfo=UTC))
    end = _format_utc(datetime(2025, 7, 2, 13, 35, tzinfo=UTC))
    with scenario.store._leased_transaction(scenario.lease) as connection:
        connection.execute(
            """
            INSERT INTO policy_snapshots(
                policy_snapshot_id, policy_id, revision, policy_hash,
                provider, dataset, retention_mode, verified_at, captured_at,
                expires_at, entitlement_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _POLICY_SNAPSHOT_ID,
                policy.policy_id,
                policy.revision,
                policy.content_hash,
                policy.provider,
                policy.dataset,
                policy.mode.value,
                policy.verified_on.isoformat(),
                timestamp,
                None if expires_at is None else _format_utc(expires_at),
                1,
            ),
        )
        connection.execute(
            """
            INSERT INTO dataset_policy_status(
                provider, dataset, status, retention_mode, policy_snapshot_id,
                effective_at, expires_at, unavailable_at, last_checked_at
            ) VALUES (?, ?, 'ACTIVE', ?, ?, ?, ?, NULL, ?)
            """,
            (
                policy.provider,
                policy.dataset,
                policy.mode.value,
                _POLICY_SNAPSHOT_ID,
                timestamp,
                None if expires_at is None else _format_utc(expires_at),
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO ingestion_runs(
                run_id, mode, environment, provider, dataset, status,
                policy_snapshot_id, created_at, started_at, completed_at,
                planned_request_count, succeeded_request_count, failed_request_count
            ) VALUES (?, 'BACKFILL', 'private_research', ?, ?, 'SUCCESS', ?, ?, ?, ?, 1, 1, 0)
            """,
            (
                _RUN_ID,
                policy.provider,
                policy.dataset,
                _POLICY_SNAPSHOT_ID,
                timestamp,
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO stream_keys(
                stream_id, stream_hash, provider, dataset, instrument_id,
                timeframe, session, adjustment, dimensions_json, created_at
            ) VALUES (?, ?, ?, ?, ?, '5m', 'regular', 'unadjusted', '{}', ?)
            """,
            (
                _STREAM_ID,
                "1" * 64,
                policy.provider,
                policy.dataset,
                "00000000-0000-0000-0000-000000000001",
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO request_specs(
                request_spec_id, request_spec_hash, provider, dataset,
                interval_start, interval_end, mapping_version,
                specification_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'test-v1', '{}', ?)
            """,
            (
                _REQUEST_SPEC_ID,
                "2" * 64,
                policy.provider,
                policy.dataset,
                start,
                end,
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO request_instances(
                request_instance_id, run_id, request_spec_id, intent, reason,
                plan_ordinal, status, created_at, completed_at
            ) VALUES ('request-instance-retention', ?, ?, 'BACKFILL',
                      'synthetic retention test', 0, 'SUCCESS', ?, ?)
            """,
            (_RUN_ID, _REQUEST_SPEC_ID, timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO request_attempts(
                attempt_id, request_instance_id, attempt_number, status,
                started_at, completed_at, page_count, pagination_complete,
                terminal_page_verified
            ) VALUES ('attempt-retention', 'request-instance-retention', 1, 'SUCCESS',
                      ?, ?, 1, 1, 1)
            """,
            (timestamp, timestamp),
        )
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
                _RAW_ID,
                _REQUEST_SPEC_ID,
                "3" * 64,
                hashlib.sha256(raw_payload).hexdigest(),
                len(raw_payload),
                (raw_relative / "payload.bin").as_posix(),
                (raw_relative / "manifest.json").as_posix(),
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO calendar_snapshots(
                calendar_snapshot_id, calendar_name, timezone_name,
                package_name, package_version, tzdata_version,
                session_start_date, session_end_date, schedule_checksum,
                generated_at, created_at, state
            ) VALUES (?, 'XNYS', 'America/New_York', 'synthetic-calendar', '1',
                      'synthetic', '2025-07-02', '2025-07-03', ?, ?, ?, 'CURRENT')
            """,
            (_CALENDAR_ID, f"sha256:{'4' * 64}", timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO batch_contexts(
                batch_context_id, canonical_batch_id, request_spec_id,
                ordered_artifacts_hash, canonical_schema_version,
                normalizer_version, validator_version, calendar_snapshot_id,
                fixed_ingested_at, manifest_created_at, created_at
            ) VALUES (?, ?, ?, ?, '1', '1', '1', ?, ?, ?, ?)
            """,
            (
                _BATCH_CONTEXT_ID,
                _BATCH_ID,
                _REQUEST_SPEC_ID,
                "5" * 64,
                _CALENDAR_ID,
                timestamp,
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO batch_context_artifacts(
                batch_context_id, artifact_id, ordinal
            ) VALUES (?, ?, 0)
            """,
            (_BATCH_CONTEXT_ID, _RAW_ID),
        )
        connection.execute(
            """
            INSERT INTO batch_context_requests(
                batch_context_id, request_instance_id, linked_at
            ) VALUES (?, 'request-instance-retention', ?)
            """,
            (_BATCH_CONTEXT_ID, timestamp),
        )
        connection.execute(
            """
            INSERT INTO quarantine_artifacts(
                quarantine_artifact_id, provider, dataset, request_spec_id,
                batch_context_id, policy_snapshot_id, request_instance_id, attempt_id,
                relative_path, manifest_content_sha256, manifest_byte_count,
                validation_summary_json, state, created_at, invalidated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'request-instance-retention', 'attempt-retention',
                      ?, ?, 3, '{}', 'VERIFIED', ?, NULL)
            """,
            (
                _QUARANTINE_ID,
                policy.provider,
                policy.dataset,
                _REQUEST_SPEC_ID,
                _BATCH_CONTEXT_ID,
                _POLICY_SNAPSHOT_ID,
                _quarantine_directory(policy).as_posix(),
                hashlib.sha256(b"{}\n").hexdigest(),
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO canonical_batches(
                canonical_batch_id, batch_context_id, policy_snapshot_id,
                relative_path, manifest_relative_path, state, row_count,
                published_at, verified_at, invalidated_at
            ) VALUES (?, ?, ?, ?, ?, 'VERIFIED', 1, ?, ?, NULL)
            """,
            (
                _BATCH_ID,
                _BATCH_CONTEXT_ID,
                _POLICY_SNAPSHOT_ID,
                canonical_relative.as_posix(),
                (canonical_relative / "manifest.json").as_posix(),
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO canonical_files(
                canonical_batch_id, file_ordinal, relative_path,
                content_sha256, byte_count, row_count, interval_start,
                interval_end, schema_fingerprint
            ) VALUES (?, 0, ?, ?, ?, 1, ?, ?, 'synthetic-schema-v1')
            """,
            (
                _BATCH_ID,
                canonical_file_relative.as_posix(),
                hashlib.sha256(parquet_payload).hexdigest(),
                len(parquet_payload),
                start,
                end,
            ),
        )
        connection.execute(
            """
            INSERT INTO canonical_batch_streams(
                canonical_batch_id, stream_id, outcome, row_count,
                interval_start, interval_end, validation_summary_json,
                semantic_duplicate_count, revision_count
            ) VALUES (?, ?, 'PUBLISHABLE', 1, ?, ?, '{}', 0, 0)
            """,
            (_BATCH_ID, _STREAM_ID, start, end),
        )
        connection.execute(
            """
            INSERT INTO coverage_segments(
                coverage_id, stream_id, canonical_batch_id, calendar_snapshot_id,
                policy_snapshot_id, coverage_start, interval_start, interval_end,
                classification, verification_state, retained, row_count,
                artifact_count, request_completed, pagination_verified,
                provider_semantics_version, generation, verified_at, invalidated_at
            ) VALUES ('coverage-test-retention', ?, ?, ?, ?, ?, ?, ?, 'OBSERVED',
                      'VERIFIED', 1, 1, 1, 1, 1, 'synthetic-v1', 1, ?, NULL)
            """,
            (
                _STREAM_ID,
                _BATCH_ID,
                _CALENDAR_ID,
                _POLICY_SNAPSHOT_ID,
                start,
                start,
                end,
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO gaps(
                gap_id, stream_id, interval_start, interval_end, gap_type,
                status, blocking, detected_at, resolved_at,
                request_instance_id, canonical_batch_id
            ) VALUES ('gap-test-retention', ?, ?, ?, 'INTEGRITY', 'OPEN', 1, ?, NULL, NULL, ?)
            """,
            (_STREAM_ID, start, end, timestamp, _BATCH_ID),
        )
        connection.execute(
            """
            INSERT INTO watermarks(
                stream_id, coverage_start, exclusive_frontier, verification_state,
                generation, calendar_snapshot_id, policy_snapshot_id,
                last_run_id, last_batch_id, last_verified_session,
                blocking_gap_count, computed_at, invalidated_at
            ) VALUES (?, ?, ?, 'VERIFIED', 1, ?, ?, ?, ?, '2025-07-02', 0, ?, NULL)
            """,
            (
                _STREAM_ID,
                start,
                end,
                _CALENDAR_ID,
                _POLICY_SNAPSHOT_ID,
                _RUN_ID,
                _BATCH_ID,
                timestamp,
            ),
        )


def _scenario(
    tmp_path: Path,
    *,
    mode: RetentionMode = RetentionMode.TTL,
    expires_at: datetime | None = _NOW,
) -> Scenario:
    policy = _policy(mode)
    root = _private_root(tmp_path)
    store = OperationalStateStore.open(root, clock=_clock)
    lease = store.acquire_writer_lease("retention-tests", timedelta(minutes=30))
    repository = RetentionLifecycleRepository(
        store,
        root,
        RetentionPolicyEnforcer(_catalog(policy), clock=_clock),
    )
    raw_relative = raw_artifact_relative_directory(policy.provider, policy.dataset, _RAW_ID)
    canonical_relative = _canonical_directory(policy)
    scenario = Scenario(
        root=root,
        store=store,
        lease=lease,
        repository=repository,
        policy=policy,
        raw_directory=root.root.joinpath(*raw_relative.parts),
        canonical_directory=root.root.joinpath(*canonical_relative.parts),
        canonical_file=root.root.joinpath(
            *(
                canonical_relative / "timeframe=5m" / "year=2025" / "month=07" / "part-0000.parquet"
            ).parts
        ),
        quarantine_directory=root.root.joinpath(*_quarantine_directory(policy).parts),
    )
    _seed_operational_state(scenario, expires_at=expires_at)
    return scenario


def _begin_ttl(
    scenario: Scenario,
    *,
    fault_injector: Callable[[RetentionFaultPoint], None] | None = None,
) -> PurgePlan:
    return scenario.repository.begin_ttl_expiry_purge(
        scenario.lease,
        scenario.policy.provider,
        scenario.policy.dataset,
        fault_injector=fault_injector,
    )


def test_ttl_purge_is_state_first_exact_and_idempotent(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path)
    try:
        plan = _begin_ttl(scenario)

        assert plan.status is PurgeRunStatus.UNAVAILABLE
        assert {target.target_type for target in plan.targets} == set(PurgeTargetType)
        assert scenario.raw_directory.is_dir()
        assert scenario.canonical_directory.is_dir()
        assert scenario.quarantine_directory.is_dir()
        with scenario.store.read_only_connection() as connection:
            policy = connection.execute("SELECT * FROM dataset_policy_status").fetchone()
            raw = connection.execute("SELECT state FROM raw_artifacts").fetchone()
            batch = connection.execute("SELECT state FROM canonical_batches").fetchone()
            quarantine = connection.execute("SELECT state FROM quarantine_artifacts").fetchone()
            coverage = connection.execute("SELECT * FROM coverage_segments").fetchone()
            watermark = connection.execute("SELECT * FROM watermarks").fetchone()
            gap = connection.execute("SELECT * FROM gaps").fetchone()
        assert policy is not None and (policy["status"], policy["unavailable_at"] is not None) == (
            "EXPIRED",
            True,
        )
        assert raw is not None and raw["state"] == "INVALID"
        assert batch is not None and batch["state"] == "INVALID"
        assert quarantine is not None and quarantine["state"] == "INVALID"
        assert coverage is not None and (
            coverage["verification_state"],
            coverage["retained"],
            coverage["generation"],
        ) == ("INVALID", 0, 2)
        assert watermark is not None and (
            watermark["verification_state"],
            watermark["generation"],
        ) == ("INVALID", 2)
        assert gap is not None and gap["status"] == "INVALIDATED"

        replay = _begin_ttl(scenario)
        assert replay.replayed
        assert replay.purge_run_id == plan.purge_run_id

        result = scenario.repository.execute_purge(scenario.lease, plan.purge_run_id)
        assert result.status is PurgeRunStatus.SUCCESS
        assert result.deleted_count == 4
        assert not scenario.raw_directory.exists()
        assert not scenario.canonical_directory.exists()
        assert not scenario.quarantine_directory.exists()
        with scenario.store.read_only_connection() as connection:
            assert connection.execute("SELECT state FROM raw_artifacts").fetchone()[0] == "PURGED"
            assert (
                connection.execute("SELECT state FROM canonical_batches").fetchone()[0] == "PURGED"
            )
            assert (
                connection.execute("SELECT state FROM quarantine_artifacts").fetchone()[0]
                == "PURGED"
            )

        replayed_result = scenario.repository.execute_purge(
            scenario.lease,
            plan.purge_run_id,
        )
        assert replayed_result.replayed
        assert replayed_result.status is PurgeRunStatus.SUCCESS
    finally:
        scenario.store.close()


def test_crash_before_invalidation_rolls_back_everything(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path)

    def crash(point: RetentionFaultPoint) -> None:
        if point is RetentionFaultPoint.BEFORE_INVALIDATION:
            raise SimulatedCrash

    try:
        with pytest.raises(SimulatedCrash):
            _begin_ttl(scenario, fault_injector=crash)
        with scenario.store.read_only_connection() as connection:
            assert connection.execute("SELECT status FROM dataset_policy_status").fetchone()[0] == (
                "ACTIVE"
            )
            assert connection.execute("SELECT COUNT(*) FROM purge_runs").fetchone()[0] == 0
            assert connection.execute("SELECT state FROM raw_artifacts").fetchone()[0] == (
                "VERIFIED"
            )
        assert scenario.raw_directory.is_dir()
        assert scenario.canonical_directory.is_dir()
    finally:
        scenario.store.close()


def test_crash_after_invalidation_resumes_without_revalidation_gap(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path)

    def crash(point: RetentionFaultPoint) -> None:
        if point is RetentionFaultPoint.AFTER_INVALIDATION:
            raise SimulatedCrash

    try:
        with pytest.raises(SimulatedCrash):
            _begin_ttl(scenario, fault_injector=crash)
        with scenario.store.read_only_connection() as connection:
            assert connection.execute("SELECT status FROM dataset_policy_status").fetchone()[0] == (
                "EXPIRED"
            )
            assert connection.execute("SELECT state FROM raw_artifacts").fetchone()[0] == "INVALID"
            purge_run_id = connection.execute("SELECT purge_run_id FROM purge_runs").fetchone()[0]
        assert scenario.raw_directory.is_dir()

        replay = _begin_ttl(scenario)
        assert replay.replayed and replay.purge_run_id == purge_run_id
        assert (
            scenario.repository.execute_purge(
                scenario.lease,
                purge_run_id,
            ).status
            is PurgeRunStatus.SUCCESS
        )
    finally:
        scenario.store.close()


def test_crash_before_deletion_leaves_every_catalog_target_present(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path)

    def crash(point: RetentionFaultPoint) -> None:
        if point is RetentionFaultPoint.BEFORE_DELETION:
            raise SimulatedCrash

    try:
        plan = _begin_ttl(scenario)
        with pytest.raises(SimulatedCrash):
            scenario.repository.execute_purge(
                scenario.lease,
                plan.purge_run_id,
                fault_injector=crash,
            )
        interrupted = scenario.repository.load_plan(plan.purge_run_id)
        assert interrupted.status is PurgeRunStatus.DELETING
        assert all(
            target.deletion_status is PurgeDeletionStatus.PLANNED for target in interrupted.targets
        )
        assert scenario.raw_directory.is_dir()
        assert scenario.canonical_file.is_file()
        assert scenario.quarantine_directory.is_dir()
        assert (
            scenario.repository.execute_purge(
                scenario.lease,
                plan.purge_run_id,
            ).status
            is PurgeRunStatus.SUCCESS
        )
    finally:
        scenario.store.close()


@pytest.mark.parametrize(
    "fault_point",
    [RetentionFaultPoint.AFTER_DELETION, RetentionFaultPoint.BEFORE_FINALIZE],
)
def test_purge_recovers_after_physical_delete_boundaries(
    tmp_path: Path,
    fault_point: RetentionFaultPoint,
) -> None:
    scenario = _scenario(tmp_path)
    raised = False

    def crash(point: RetentionFaultPoint) -> None:
        nonlocal raised
        if point is fault_point and not raised:
            raised = True
            raise SimulatedCrash

    try:
        plan = _begin_ttl(scenario)
        with pytest.raises(SimulatedCrash):
            scenario.repository.execute_purge(
                scenario.lease,
                plan.purge_run_id,
                fault_injector=crash,
            )
        interrupted = scenario.repository.load_plan(plan.purge_run_id)
        assert interrupted.status is PurgeRunStatus.DELETING
        assert (
            scenario.repository.execute_purge(
                scenario.lease,
                plan.purge_run_id,
            ).status
            is PurgeRunStatus.SUCCESS
        )
        final = scenario.repository.load_plan(plan.purge_run_id)
        assert all(
            target.deletion_status in {PurgeDeletionStatus.DELETED, PurgeDeletionStatus.ABSENT}
            for target in final.targets
        )
    finally:
        scenario.store.close()


def test_crash_after_finalize_replays_committed_success(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path)

    def crash(point: RetentionFaultPoint) -> None:
        if point is RetentionFaultPoint.AFTER_FINALIZE:
            raise SimulatedCrash

    try:
        plan = _begin_ttl(scenario)
        with pytest.raises(SimulatedCrash):
            scenario.repository.execute_purge(
                scenario.lease,
                plan.purge_run_id,
                fault_injector=crash,
            )
        committed = scenario.repository.load_plan(plan.purge_run_id)
        assert committed.status is PurgeRunStatus.SUCCESS
        result = scenario.repository.execute_purge(scenario.lease, plan.purge_run_id)
        assert result.status is PurgeRunStatus.SUCCESS
        assert result.replayed
    finally:
        scenario.store.close()


def test_subscription_termination_has_an_explicit_state_first_entry(tmp_path: Path) -> None:
    scenario = _scenario(
        tmp_path,
        mode=RetentionMode.SUBSCRIPTION_BOUND,
        expires_at=None,
    )
    try:
        plan = scenario.repository.begin_subscription_termination_purge(
            scenario.lease,
            scenario.policy.provider,
            scenario.policy.dataset,
        )
        with scenario.store.read_only_connection() as connection:
            assert connection.execute("SELECT status FROM dataset_policy_status").fetchone()[0] == (
                "TERMINATED"
            )
        assert (
            scenario.repository.execute_purge(
                scenario.lease,
                plan.purge_run_id,
            ).status
            is PurgeRunStatus.SUCCESS
        )
    finally:
        scenario.store.close()


def test_future_ttl_and_unknown_or_noncanonical_keys_fail_closed(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path, expires_at=_NOW + timedelta(seconds=1))
    try:
        with pytest.raises(RetentionLifecycleStateError, match="not expired"):
            _begin_ttl(scenario)
        with pytest.raises(DatasetPolicyDenied):
            scenario.repository.begin_ttl_expiry_purge(
                scenario.lease,
                "unknown",
                "price_bars",
            )
        with pytest.raises(RetentionLifecycleStateError, match="exact canonical"):
            scenario.repository.begin_ttl_expiry_purge(
                scenario.lease,
                scenario.policy.provider.upper(),
                scenario.policy.dataset,
            )
        with scenario.store.read_only_connection() as connection:
            assert connection.execute("SELECT status FROM dataset_policy_status").fetchone()[0] == (
                "ACTIVE"
            )
            assert connection.execute("SELECT COUNT(*) FROM purge_runs").fetchone()[0] == 0
    finally:
        scenario.store.close()


def test_unexpected_file_fails_safe_and_purge_remains_recoverable(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path)
    rogue = scenario.raw_directory / "not-cataloged.bin"
    rogue.write_bytes(b"synthetic unowned entry")
    try:
        plan = _begin_ttl(scenario)
        with pytest.raises(RetentionPurgeSafetyError, match="unexpected file"):
            scenario.repository.execute_purge(scenario.lease, plan.purge_run_id)
        failed = scenario.repository.load_plan(plan.purge_run_id)
        assert failed.status is PurgeRunStatus.FAILED
        raw_target = next(
            target
            for target in failed.targets
            if target.target_type is PurgeTargetType.RAW_ARTIFACT
        )
        assert raw_target.deletion_status is PurgeDeletionStatus.FAILED
        assert rogue.read_bytes() == b"synthetic unowned entry"
        with scenario.store.read_only_connection() as connection:
            assert connection.execute("SELECT state FROM raw_artifacts").fetchone()[0] == "INVALID"
            assert connection.execute("SELECT status FROM dataset_policy_status").fetchone()[0] == (
                "EXPIRED"
            )

        rogue.unlink()
        assert (
            scenario.repository.execute_purge(
                scenario.lease,
                plan.purge_run_id,
            ).status
            is PurgeRunStatus.SUCCESS
        )
    finally:
        scenario.store.close()


def test_physical_deletion_requires_the_original_live_sentinel(tmp_path: Path) -> None:
    scenario = _scenario(tmp_path)
    sentinel = scenario.root.sentinel_path
    held = sentinel.with_name("sentinel-held-for-synthetic-crash-test")
    try:
        plan = _begin_ttl(scenario)
        sentinel.rename(held)
        with pytest.raises(PrivateDataRootError):
            scenario.repository.execute_purge(scenario.lease, plan.purge_run_id)
        assert scenario.raw_directory.is_dir()
        assert scenario.canonical_file.is_file()
        assert scenario.quarantine_directory.is_dir()

        held.rename(sentinel)
        assert (
            scenario.repository.execute_purge(
                scenario.lease,
                plan.purge_run_id,
            ).status
            is PurgeRunStatus.SUCCESS
        )
    finally:
        if held.exists() and not sentinel.exists():
            held.rename(sentinel)
        scenario.store.close()
