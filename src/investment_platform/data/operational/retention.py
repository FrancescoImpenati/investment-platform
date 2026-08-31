"""State-first retention invalidation and exact-target private-data purge.

The operational transaction deliberately makes retained data unavailable before any filesystem
mutation.  Physical deletion is then restartable: every target is an exact, durable catalog row,
and absence is a successful replay outcome.  This module never accepts a caller-supplied path or
glob.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Final
from uuid import UUID

from investment_platform.data.operational.store import (
    OperationalStateError,
    OperationalStateStore,
    WriterLease,
    _format_utc,
    _parse_utc,
)
from investment_platform.data.retention import (
    DatasetRetentionPolicy,
    RetentionMode,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage._publication import safe_partition_value
from investment_platform.data.storage.living_raw import raw_artifact_relative_directory
from investment_platform.data_root import PrivateDataRoot, PrivateDataRootError

_RAW_ID: Final = re.compile(r"raw_v1_[0-9a-f]{64}\Z")
_BATCH_ID: Final = re.compile(r"batch_v1_[0-9a-f]{64}\Z")
_QUARANTINE_ID: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_PURGE_ID_VERSION: Final = "purge_v1"


class RetentionLifecycleError(OperationalStateError):
    """Base error for durable expiration, termination, and purge state."""


class RetentionLifecycleStateError(RetentionLifecycleError):
    """The catalog and operational state cannot prove one safe lifecycle transition."""


class RetentionPurgeSafetyError(RetentionLifecycleError):
    """An exact purge target is unsafe, unexpected, or cannot be removed safely."""


class RetentionInvalidationTrigger(StrEnum):
    TTL_EXPIRY = "TTL_EXPIRY"
    SUBSCRIPTION_TERMINATION = "SUBSCRIPTION_TERMINATION"


class PurgeRunStatus(StrEnum):
    PLANNED = "PLANNED"
    UNAVAILABLE = "UNAVAILABLE"
    DELETING = "DELETING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class PurgeTargetType(StrEnum):
    RAW_ARTIFACT = "RAW_ARTIFACT"
    CANONICAL_BATCH = "CANONICAL_BATCH"
    CANONICAL_FILE = "CANONICAL_FILE"
    QUARANTINE = "QUARANTINE"


class PurgeDeletionStatus(StrEnum):
    PLANNED = "PLANNED"
    DELETED = "DELETED"
    ABSENT = "ABSENT"
    FAILED = "FAILED"


class RetentionFaultPoint(StrEnum):
    """Stable crash boundaries for offline recovery tests."""

    BEFORE_INVALIDATION = "before_invalidation"
    AFTER_INVALIDATION = "after_invalidation"
    BEFORE_DELETION = "before_deletion"
    AFTER_DELETION = "after_deletion"
    BEFORE_FINALIZE = "before_finalize"
    AFTER_FINALIZE = "after_finalize"


type RetentionFaultInjector = Callable[[RetentionFaultPoint], None]


@dataclass(frozen=True, slots=True)
class QuarantinePurgeTarget:
    """One exact platform-owned quarantine artifact identity, never an arbitrary path."""

    target_id: str

    def __post_init__(self) -> None:
        if not _QUARANTINE_ID.fullmatch(self.target_id):
            raise ValueError("quarantine target ID is not a safe durable identifier")


@dataclass(frozen=True, slots=True)
class PurgeTargetRecord:
    target_type: PurgeTargetType
    target_id: str
    relative_path: str
    deletion_status: PurgeDeletionStatus
    deleted_at: datetime | None


@dataclass(frozen=True, slots=True)
class PurgePlan:
    purge_run_id: str
    provider: str
    dataset: str
    policy_snapshot_id: str
    trigger: RetentionInvalidationTrigger
    status: PurgeRunStatus
    created_at: datetime
    completed_at: datetime | None
    targets: tuple[PurgeTargetRecord, ...]
    replayed: bool


@dataclass(frozen=True, slots=True)
class PurgeExecutionResult:
    purge_run_id: str
    status: PurgeRunStatus
    target_count: int
    deleted_count: int
    absent_count: int
    replayed: bool


def _invoke_fault(
    injector: RetentionFaultInjector | None,
    point: RetentionFaultPoint,
) -> None:
    if injector is not None:
        injector(point)


def _canonical_relative_directory(
    provider: str,
    dataset: str,
    canonical_batch_id: str,
) -> PurePosixPath:
    """Mirror the canonical publisher's fixed physical identity derivation."""

    provider = safe_partition_value(provider, label="provider")
    dataset = safe_partition_value(dataset, label="dataset")
    if not _BATCH_ID.fullmatch(canonical_batch_id):
        raise RetentionLifecycleStateError("canonical batch has an invalid durable identity")
    return PurePosixPath(
        "normalized",
        "price_bars",
        f"provider={provider}",
        f"dataset={dataset}",
        "batches",
        f"batch={canonical_batch_id.removeprefix('batch_v1_')[:32]}",
    )


def _quarantine_relative_directory(
    provider: str,
    dataset: str,
    target_id: str,
) -> PurePosixPath:
    provider = safe_partition_value(provider, label="provider")
    dataset = safe_partition_value(dataset, label="dataset")
    if not _QUARANTINE_ID.fullmatch(target_id):
        raise RetentionLifecycleStateError("quarantine target has an invalid durable identity")
    return PurePosixPath(
        "quarantine",
        f"provider={provider}",
        f"dataset={dataset}",
        "artifacts",
        f"artifact={target_id}",
    )


def _purge_run_id(
    *,
    root_id: UUID,
    provider: str,
    dataset: str,
    policy_snapshot_id: str,
    trigger: RetentionInvalidationTrigger,
) -> str:
    encoded = json.dumps(
        {
            "dataset": dataset,
            "policy_snapshot_id": policy_snapshot_id,
            "provider": provider,
            "root_id": str(root_id),
            "trigger": trigger.value,
            "version": 1,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"{_PURGE_ID_VERSION}_{hashlib.sha256(encoded).hexdigest()}"


def _reason(trigger: RetentionInvalidationTrigger) -> str:
    return f"retention:{trigger.value}:v1"


def _trigger_from_reason(value: str) -> RetentionInvalidationTrigger:
    for trigger in RetentionInvalidationTrigger:
        if value == _reason(trigger):
            return trigger
    raise RetentionLifecycleStateError("purge run has an unknown retention reason")


def _is_reparse_or_link(path: Path, details: os.stat_result) -> bool:
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    attributes = getattr(details, "st_file_attributes", 0)
    try:
        is_junction = path.is_junction()
    except (AttributeError, OSError):
        is_junction = False
    return bool(path.is_symlink() or is_junction or (reparse_flag and attributes & reparse_flag))


def _file_identity(details: os.stat_result) -> tuple[int, int, int, int]:
    return (details.st_dev, details.st_ino, details.st_size, details.st_mtime_ns)


class RetentionLifecycleRepository:
    """Lease-fenced state-first invalidation followed by exact idempotent deletion."""

    def __init__(
        self,
        store: OperationalStateStore,
        data_root: PrivateDataRoot,
        enforcer: RetentionPolicyEnforcer,
    ) -> None:
        sentinel = data_root.validate()
        if str(sentinel.root_id) != store.root_id:
            raise RetentionLifecycleStateError(
                "operational store and retention lifecycle use different private roots"
            )
        self._store = store
        self._data_root = data_root
        self._enforcer = enforcer
        self._root_id = sentinel.root_id

    def begin_ttl_expiry_purge(
        self,
        lease: WriterLease,
        provider: str,
        dataset: str,
        *,
        quarantine_targets: Sequence[QuarantinePurgeTarget] = (),
        fault_injector: RetentionFaultInjector | None = None,
    ) -> PurgePlan:
        """Expire one due TTL dataset and durably catalog all exact purge targets."""

        return self._begin_purge(
            lease,
            provider,
            dataset,
            trigger=RetentionInvalidationTrigger.TTL_EXPIRY,
            quarantine_targets=quarantine_targets,
            fault_injector=fault_injector,
        )

    def begin_subscription_termination_purge(
        self,
        lease: WriterLease,
        provider: str,
        dataset: str,
        *,
        quarantine_targets: Sequence[QuarantinePurgeTarget] = (),
        fault_injector: RetentionFaultInjector | None = None,
    ) -> PurgePlan:
        """Terminate one subscription-bound dataset before deleting its retained bytes."""

        return self._begin_purge(
            lease,
            provider,
            dataset,
            trigger=RetentionInvalidationTrigger.SUBSCRIPTION_TERMINATION,
            quarantine_targets=quarantine_targets,
            fault_injector=fault_injector,
        )

    def _begin_purge(
        self,
        lease: WriterLease,
        provider: str,
        dataset: str,
        *,
        trigger: RetentionInvalidationTrigger,
        quarantine_targets: Sequence[QuarantinePurgeTarget],
        fault_injector: RetentionFaultInjector | None,
    ) -> PurgePlan:
        policy = self._exact_purge_policy(provider, dataset)
        self._validate_trigger_policy(policy, trigger)
        supplied_quarantine_ids = tuple(sorted({target.target_id for target in quarantine_targets}))
        if len(supplied_quarantine_ids) != len(quarantine_targets):
            raise RetentionLifecycleStateError("quarantine purge targets contain duplicates")
        self._data_root.validate(expected_root_id=self._root_id)
        _invoke_fault(fault_injector, RetentionFaultPoint.BEFORE_INVALIDATION)

        replayed = False
        with self._store._leased_transaction(lease) as connection:
            now = self._store._now()
            quarantine_ids = tuple(
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT quarantine_artifact_id FROM quarantine_artifacts
                    WHERE provider = ? AND dataset = ?
                    ORDER BY quarantine_artifact_id
                    """,
                    (policy.provider, policy.dataset),
                ).fetchall()
            )
            if supplied_quarantine_ids and supplied_quarantine_ids != quarantine_ids:
                raise RetentionLifecycleStateError(
                    "caller quarantine targets differ from the exact operational catalog"
                )
            status = connection.execute(
                """
                SELECT status.*, snapshot.policy_id, snapshot.revision, snapshot.policy_hash,
                       snapshot.provider AS snapshot_provider,
                       snapshot.dataset AS snapshot_dataset,
                       snapshot.retention_mode AS snapshot_retention_mode
                FROM dataset_policy_status AS status
                LEFT JOIN policy_snapshots AS snapshot
                  ON snapshot.policy_snapshot_id = status.policy_snapshot_id
                WHERE status.provider = ? AND status.dataset = ?
                """,
                (policy.provider, policy.dataset),
            ).fetchone()
            if status is None or status["policy_snapshot_id"] is None:
                raise RetentionLifecycleStateError(
                    "exact dataset has no durable operational policy snapshot"
                )
            policy_snapshot_id = str(status["policy_snapshot_id"])
            self._validate_operational_policy(status, policy, trigger, now=now)
            purge_run_id = _purge_run_id(
                root_id=self._root_id,
                provider=policy.provider,
                dataset=policy.dataset,
                policy_snapshot_id=policy_snapshot_id,
                trigger=trigger,
            )
            existing = connection.execute(
                "SELECT * FROM purge_runs WHERE purge_run_id = ?",
                (purge_run_id,),
            ).fetchone()
            if existing is not None:
                self._validate_existing_run(
                    existing,
                    provider=policy.provider,
                    dataset=policy.dataset,
                    policy_snapshot_id=policy_snapshot_id,
                    trigger=trigger,
                )
                if quarantine_ids:
                    stored_ids = {
                        str(row["target_id"])
                        for row in connection.execute(
                            """
                            SELECT target_id FROM purge_targets
                            WHERE purge_run_id = ? AND target_type = 'QUARANTINE'
                            """,
                            (purge_run_id,),
                        ).fetchall()
                    }
                    if stored_ids != set(quarantine_ids):
                        raise RetentionLifecycleStateError(
                            "retry supplied a different quarantine target set"
                        )
                replayed = True
            else:
                terminal_status = (
                    "EXPIRED"
                    if trigger is RetentionInvalidationTrigger.TTL_EXPIRY
                    else "TERMINATED"
                )
                if str(status["status"]) == terminal_status:
                    raise RetentionLifecycleStateError(
                        "terminal dataset state has no matching atomic purge run"
                    )
                targets = self._collect_exact_targets(
                    connection,
                    provider=policy.provider,
                    dataset=policy.dataset,
                    quarantine_ids=quarantine_ids,
                )
                recorded_at = _format_utc(now)
                connection.execute(
                    """
                    UPDATE dataset_policy_status
                    SET status = ?, unavailable_at = COALESCE(unavailable_at, ?),
                        last_checked_at = ?
                    WHERE provider = ? AND dataset = ?
                    """,
                    (
                        terminal_status,
                        recorded_at,
                        recorded_at,
                        policy.provider,
                        policy.dataset,
                    ),
                )
                connection.execute(
                    """
                    UPDATE raw_artifacts
                    SET state = 'INVALID'
                    WHERE state <> 'PURGED' AND request_spec_id IN (
                        SELECT request_spec_id FROM request_specs
                        WHERE provider = ? AND dataset = ?
                    )
                    """,
                    (policy.provider, policy.dataset),
                )
                connection.execute(
                    """
                    UPDATE canonical_batches
                    SET state = 'INVALID', invalidated_at = COALESCE(invalidated_at, ?)
                    WHERE state <> 'PURGED' AND policy_snapshot_id IN (
                        SELECT policy_snapshot_id FROM policy_snapshots
                        WHERE provider = ? AND dataset = ?
                    )
                    """,
                    (recorded_at, policy.provider, policy.dataset),
                )
                connection.execute(
                    """
                    UPDATE quarantine_artifacts
                    SET state = 'INVALID', invalidated_at = COALESCE(invalidated_at, ?)
                    WHERE provider = ? AND dataset = ? AND state <> 'PURGED'
                    """,
                    (recorded_at, policy.provider, policy.dataset),
                )
                connection.execute(
                    """
                    UPDATE coverage_segments
                    SET verification_state = 'INVALID', retained = 0,
                        generation = CASE
                            WHEN verification_state = 'INVALID' THEN generation
                            ELSE generation + 1
                        END,
                        invalidated_at = COALESCE(invalidated_at, ?)
                    WHERE canonical_batch_id IN (
                        SELECT batch.canonical_batch_id
                        FROM canonical_batches AS batch
                        JOIN policy_snapshots AS snapshot
                          ON snapshot.policy_snapshot_id = batch.policy_snapshot_id
                        WHERE snapshot.provider = ? AND snapshot.dataset = ?
                    )
                    """,
                    (recorded_at, policy.provider, policy.dataset),
                )
                connection.execute(
                    """
                    UPDATE watermarks
                    SET verification_state = 'INVALID',
                        generation = CASE
                            WHEN verification_state = 'INVALID' THEN generation
                            ELSE generation + 1
                        END,
                        invalidated_at = COALESCE(invalidated_at, ?)
                    WHERE stream_id IN (
                        SELECT stream_id FROM stream_keys
                        WHERE provider = ? AND dataset = ?
                    )
                    """,
                    (recorded_at, policy.provider, policy.dataset),
                )
                connection.execute(
                    """
                    UPDATE gaps
                    SET status = 'INVALIDATED', resolved_at = COALESCE(resolved_at, ?)
                    WHERE status <> 'INVALIDATED' AND stream_id IN (
                        SELECT stream_id FROM stream_keys
                        WHERE provider = ? AND dataset = ?
                    )
                    """,
                    (recorded_at, policy.provider, policy.dataset),
                )
                connection.execute(
                    """
                    INSERT INTO purge_runs(
                        purge_run_id, provider, dataset, policy_snapshot_id,
                        reason, status, created_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, 'UNAVAILABLE', ?, NULL)
                    """,
                    (
                        purge_run_id,
                        policy.provider,
                        policy.dataset,
                        policy_snapshot_id,
                        _reason(trigger),
                        recorded_at,
                    ),
                )
                connection.executemany(
                    """
                    INSERT INTO purge_targets(
                        purge_run_id, target_type, target_id, relative_path,
                        deletion_status, deleted_at
                    ) VALUES (?, ?, ?, ?, 'PLANNED', NULL)
                    """,
                    tuple(
                        (
                            purge_run_id,
                            target.target_type.value,
                            target.target_id,
                            target.relative_path,
                        )
                        for target in targets
                    ),
                )
            self._data_root.validate(expected_root_id=self._root_id)
            plan = self._load_plan_from_connection(
                connection,
                purge_run_id,
                replayed=replayed,
            )
            if replayed:
                if plan.status is PurgeRunStatus.PLANNED:
                    raise RetentionLifecycleStateError(
                        "existing purge run never completed state-first invalidation"
                    )
                self._assert_state_is_unavailable(connection, plan)

        _invoke_fault(fault_injector, RetentionFaultPoint.AFTER_INVALIDATION)
        return plan

    def execute_purge(
        self,
        lease: WriterLease,
        purge_run_id: str,
        *,
        fault_injector: RetentionFaultInjector | None = None,
    ) -> PurgeExecutionResult:
        """Delete one durable plan idempotently and finalize catalog state after absence."""

        initial = self.load_plan(purge_run_id)
        self._exact_purge_policy(initial.provider, initial.dataset)
        if initial.status is PurgeRunStatus.SUCCESS:
            self._validate_successful_replay(initial)
            return self._result(initial, replayed=True)

        with self._store._leased_transaction(lease) as connection:
            current = self._load_plan_from_connection(connection, purge_run_id, replayed=False)
            if current.status is PurgeRunStatus.SUCCESS:
                return self._result(current, replayed=True)
            if current.status not in {
                PurgeRunStatus.UNAVAILABLE,
                PurgeRunStatus.DELETING,
                PurgeRunStatus.FAILED,
            }:
                raise RetentionLifecycleStateError(
                    "purge cannot delete before state-first invalidation"
                )
            self._assert_plan_catalog_complete(connection, current)
            self._assert_state_is_unavailable(connection, current)
            connection.execute(
                """
                UPDATE purge_runs SET status = 'DELETING', completed_at = NULL
                WHERE purge_run_id = ?
                """,
                (purge_run_id,),
            )

        self._data_root.validate(expected_root_id=self._root_id)
        _invoke_fault(fault_injector, RetentionFaultPoint.BEFORE_DELETION)
        order = {
            PurgeTargetType.CANONICAL_FILE: 0,
            PurgeTargetType.RAW_ARTIFACT: 1,
            PurgeTargetType.QUARANTINE: 2,
            PurgeTargetType.CANONICAL_BATCH: 3,
        }
        for target in sorted(
            initial.targets, key=lambda value: (order[value.target_type], value.target_id)
        ):
            if target.deletion_status in {
                PurgeDeletionStatus.DELETED,
                PurgeDeletionStatus.ABSENT,
            }:
                if not self._target_is_absent(target.relative_path):
                    error = RetentionPurgeSafetyError(
                        "a completed purge target reappeared under the private root"
                    )
                    self._record_target_failure(lease, purge_run_id, target, error)
                    raise error
                continue
            try:
                outcome = self._delete_target(purge_run_id, target)
            except RetentionPurgeSafetyError as error:
                self._record_target_failure(lease, purge_run_id, target, error)
                raise
            _invoke_fault(fault_injector, RetentionFaultPoint.AFTER_DELETION)
            with self._store._leased_transaction(lease) as connection:
                changed = connection.execute(
                    """
                    UPDATE purge_targets
                    SET deletion_status = ?, deleted_at = ?
                    WHERE purge_run_id = ? AND target_type = ? AND target_id = ?
                      AND deletion_status IN ('PLANNED', 'FAILED')
                    """,
                    (
                        outcome.value,
                        _format_utc(self._store._now()),
                        purge_run_id,
                        target.target_type.value,
                        target.target_id,
                    ),
                ).rowcount
                if changed != 1:
                    raise RetentionLifecycleStateError(
                        "purge target status changed outside the active writer generation"
                    )

        _invoke_fault(fault_injector, RetentionFaultPoint.BEFORE_FINALIZE)
        refreshed = self.load_plan(purge_run_id)
        if any(not self._target_is_absent(target.relative_path) for target in refreshed.targets):
            raise RetentionPurgeSafetyError("purge targets are not all absent at finalization")

        with self._store._leased_transaction(lease) as connection:
            finalizing = self._load_plan_from_connection(connection, purge_run_id, replayed=False)
            self._assert_state_is_unavailable(connection, finalizing)
            if any(
                target.deletion_status
                not in {PurgeDeletionStatus.DELETED, PurgeDeletionStatus.ABSENT}
                for target in finalizing.targets
            ):
                raise RetentionLifecycleStateError(
                    "purge target state is incomplete despite physical absence"
                )
            self._mark_catalog_content_purged(connection, finalizing)
            completed_at = _format_utc(self._store._now())
            connection.execute(
                """
                UPDATE purge_runs SET status = 'SUCCESS', completed_at = ?
                WHERE purge_run_id = ? AND status = 'DELETING'
                """,
                (completed_at, purge_run_id),
            )
            completed = self._load_plan_from_connection(
                connection,
                purge_run_id,
                replayed=False,
            )
            if completed.status is not PurgeRunStatus.SUCCESS:
                raise RetentionLifecycleStateError("purge finalization did not commit success")

        _invoke_fault(fault_injector, RetentionFaultPoint.AFTER_FINALIZE)
        return self._result(completed, replayed=False)

    def load_plan(self, purge_run_id: str) -> PurgePlan:
        with self._store.read_only_connection() as connection:
            return self._load_plan_from_connection(connection, purge_run_id, replayed=False)

    def _exact_purge_policy(
        self,
        provider: str,
        dataset: str,
    ) -> DatasetRetentionPolicy:
        policy = self._enforcer.authorize_purge(provider, dataset)
        if (provider, dataset) != (policy.provider, policy.dataset):
            raise RetentionLifecycleStateError(
                "retention lifecycle requires the exact canonical provider/dataset key"
            )
        return policy

    @staticmethod
    def _validate_trigger_policy(
        policy: DatasetRetentionPolicy,
        trigger: RetentionInvalidationTrigger,
    ) -> None:
        if trigger is RetentionInvalidationTrigger.TTL_EXPIRY:
            if policy.mode is not RetentionMode.TTL:
                raise RetentionLifecycleStateError("TTL expiry requires an exact TTL policy")
            return
        if policy.mode is not RetentionMode.SUBSCRIPTION_BOUND or not policy.delete_on_termination:
            raise RetentionLifecycleStateError(
                "subscription termination requires delete-on-termination policy"
            )

    @staticmethod
    def _validate_operational_policy(
        status: sqlite3.Row,
        policy: DatasetRetentionPolicy,
        trigger: RetentionInvalidationTrigger,
        *,
        now: datetime,
    ) -> None:
        expected = (policy.provider, policy.dataset, policy.mode.value)
        actual = (
            str(status["snapshot_provider"]),
            str(status["snapshot_dataset"]),
            str(status["snapshot_retention_mode"]),
        )
        if actual != expected or str(status["retention_mode"]) != policy.mode.value:
            raise RetentionLifecycleStateError(
                "operational policy snapshot differs from the exact retention policy"
            )
        state = str(status["status"])
        terminal = "EXPIRED" if trigger is RetentionInvalidationTrigger.TTL_EXPIRY else "TERMINATED"
        if state not in {"ACTIVE", "SUSPENDED", terminal}:
            raise RetentionLifecycleStateError(
                "dataset operational status cannot enter this purge lifecycle"
            )
        if state == terminal:
            return
        if status["unavailable_at"] is not None:
            raise RetentionLifecycleStateError(
                "non-terminal dataset is already unavailable without a purge run"
            )
        if trigger is RetentionInvalidationTrigger.TTL_EXPIRY:
            if status["expires_at"] is None:
                raise RetentionLifecycleStateError("TTL dataset has no operational expiry")
            if _parse_utc(str(status["expires_at"])) > now:
                raise RetentionLifecycleStateError("TTL dataset has not expired yet")

    @staticmethod
    def _validate_existing_run(
        row: sqlite3.Row,
        *,
        provider: str,
        dataset: str,
        policy_snapshot_id: str,
        trigger: RetentionInvalidationTrigger,
    ) -> None:
        actual = (
            str(row["provider"]),
            str(row["dataset"]),
            str(row["policy_snapshot_id"]),
            str(row["reason"]),
        )
        expected = (provider, dataset, policy_snapshot_id, _reason(trigger))
        if actual != expected:
            raise RetentionLifecycleStateError(
                "deterministic purge identity collides with different lifecycle state"
            )

    def _collect_exact_targets(
        self,
        connection: sqlite3.Connection,
        *,
        provider: str,
        dataset: str,
        quarantine_ids: tuple[str, ...],
    ) -> tuple[PurgeTargetRecord, ...]:
        targets: list[PurgeTargetRecord] = []
        raw_rows = connection.execute(
            """
            SELECT artifact.*
            FROM raw_artifacts AS artifact
            JOIN request_specs AS request
              ON request.request_spec_id = artifact.request_spec_id
            WHERE request.provider = ? AND request.dataset = ?
              AND artifact.state <> 'PURGED'
            ORDER BY artifact.artifact_id
            """,
            (provider, dataset),
        ).fetchall()
        for row in raw_rows:
            artifact_id = str(row["artifact_id"])
            if not _RAW_ID.fullmatch(artifact_id):
                raise RetentionLifecycleStateError("raw artifact has an invalid durable identity")
            expected = raw_artifact_relative_directory(provider, dataset, artifact_id)
            if (
                PurePosixPath(str(row["relative_path"])) != expected / "payload.bin"
                or PurePosixPath(str(row["manifest_relative_path"])) != expected / "manifest.json"
            ):
                raise RetentionLifecycleStateError(
                    "raw artifact catalog paths differ from their deterministic identity"
                )
            targets.append(
                self._planned_target(PurgeTargetType.RAW_ARTIFACT, artifact_id, expected)
            )

        batch_rows = connection.execute(
            """
            SELECT batch.*, request.provider AS request_provider,
                   request.dataset AS request_dataset
            FROM canonical_batches AS batch
            JOIN policy_snapshots AS snapshot
              ON snapshot.policy_snapshot_id = batch.policy_snapshot_id
            JOIN batch_contexts AS context
              ON context.batch_context_id = batch.batch_context_id
            JOIN request_specs AS request
              ON request.request_spec_id = context.request_spec_id
            WHERE snapshot.provider = ? AND snapshot.dataset = ?
              AND batch.state <> 'PURGED'
            ORDER BY batch.canonical_batch_id
            """,
            (provider, dataset),
        ).fetchall()
        for row in batch_rows:
            if (str(row["request_provider"]), str(row["request_dataset"])) != (
                provider,
                dataset,
            ):
                raise RetentionLifecycleStateError(
                    "canonical batch policy and request datasets disagree"
                )
            batch_id = str(row["canonical_batch_id"])
            expected = _canonical_relative_directory(provider, dataset, batch_id)
            if (
                PurePosixPath(str(row["relative_path"])) != expected
                or PurePosixPath(str(row["manifest_relative_path"])) != expected / "manifest.json"
            ):
                raise RetentionLifecycleStateError(
                    "canonical batch paths differ from their deterministic identity"
                )
            streams = connection.execute(
                """
                SELECT stream.provider, stream.dataset
                FROM canonical_batch_streams AS binding
                JOIN stream_keys AS stream ON stream.stream_id = binding.stream_id
                WHERE binding.canonical_batch_id = ?
                """,
                (batch_id,),
            ).fetchall()
            if not streams or any(
                (str(stream["provider"]), str(stream["dataset"])) != (provider, dataset)
                for stream in streams
            ):
                raise RetentionLifecycleStateError(
                    "canonical batch streams disagree with the exact purge dataset"
                )
            files = connection.execute(
                """
                SELECT file_ordinal, relative_path FROM canonical_files
                WHERE canonical_batch_id = ? ORDER BY file_ordinal
                """,
                (batch_id,),
            ).fetchall()
            for file in files:
                relative = PurePosixPath(str(file["relative_path"]))
                if (
                    not relative.is_relative_to(expected)
                    or relative == expected / "manifest.json"
                    or relative.suffix.casefold() != ".parquet"
                ):
                    raise RetentionLifecycleStateError(
                        "canonical file lies outside its deterministic batch directory"
                    )
                target_id = f"{batch_id}:{int(file['file_ordinal'])}"
                targets.append(
                    self._planned_target(PurgeTargetType.CANONICAL_FILE, target_id, relative)
                )
            targets.append(
                self._planned_target(PurgeTargetType.CANONICAL_BATCH, batch_id, expected)
            )

        for target_id in quarantine_ids:
            relative = _quarantine_relative_directory(provider, dataset, target_id)
            row = connection.execute(
                """
                SELECT relative_path FROM quarantine_artifacts
                WHERE quarantine_artifact_id = ? AND provider = ? AND dataset = ?
                """,
                (target_id, provider, dataset),
            ).fetchone()
            if row is None or PurePosixPath(str(row["relative_path"])) != relative:
                raise RetentionLifecycleStateError(
                    "quarantine catalog path differs from deterministic identity"
                )
            targets.append(self._planned_target(PurgeTargetType.QUARANTINE, target_id, relative))
        paths = [target.relative_path for target in targets]
        if len(paths) != len(set(paths)):
            raise RetentionLifecycleStateError("purge target paths collide")
        return tuple(targets)

    def _planned_target(
        self,
        target_type: PurgeTargetType,
        target_id: str,
        relative: PurePosixPath,
    ) -> PurgeTargetRecord:
        self._data_root.managed_path(
            Path(*relative.parts),
            expected_root_id=self._root_id,
        )
        return PurgeTargetRecord(
            target_type=target_type,
            target_id=target_id,
            relative_path=relative.as_posix(),
            deletion_status=PurgeDeletionStatus.PLANNED,
            deleted_at=None,
        )

    def _load_plan_from_connection(
        self,
        connection: sqlite3.Connection,
        purge_run_id: str,
        *,
        replayed: bool,
    ) -> PurgePlan:
        row = connection.execute(
            "SELECT * FROM purge_runs WHERE purge_run_id = ?",
            (purge_run_id,),
        ).fetchone()
        if row is None:
            raise RetentionLifecycleStateError("purge run is not cataloged")
        trigger = _trigger_from_reason(str(row["reason"]))
        targets = tuple(
            PurgeTargetRecord(
                target_type=PurgeTargetType(str(target["target_type"])),
                target_id=str(target["target_id"]),
                relative_path=str(target["relative_path"]),
                deletion_status=PurgeDeletionStatus(str(target["deletion_status"])),
                deleted_at=(
                    None if target["deleted_at"] is None else _parse_utc(str(target["deleted_at"]))
                ),
            )
            for target in connection.execute(
                """
                SELECT * FROM purge_targets WHERE purge_run_id = ?
                ORDER BY target_type, target_id
                """,
                (purge_run_id,),
            ).fetchall()
        )
        return PurgePlan(
            purge_run_id=str(row["purge_run_id"]),
            provider=str(row["provider"]),
            dataset=str(row["dataset"]),
            policy_snapshot_id=str(row["policy_snapshot_id"]),
            trigger=trigger,
            status=PurgeRunStatus(str(row["status"])),
            created_at=_parse_utc(str(row["created_at"])),
            completed_at=(
                None if row["completed_at"] is None else _parse_utc(str(row["completed_at"]))
            ),
            targets=targets,
            replayed=replayed,
        )

    def _assert_plan_catalog_complete(
        self,
        connection: sqlite3.Connection,
        plan: PurgePlan,
    ) -> None:
        quarantine_ids = tuple(
            sorted(
                target.target_id
                for target in plan.targets
                if target.target_type is PurgeTargetType.QUARANTINE
            )
        )
        expected = self._collect_exact_targets(
            connection,
            provider=plan.provider,
            dataset=plan.dataset,
            quarantine_ids=quarantine_ids,
        )
        expected_identity = {
            (target.target_type, target.target_id, target.relative_path) for target in expected
        }
        actual_identity = {
            (target.target_type, target.target_id, target.relative_path) for target in plan.targets
        }
        if expected_identity != actual_identity or len(actual_identity) != len(plan.targets):
            raise RetentionLifecycleStateError(
                "purge plan is incomplete or differs from the exact operational catalog"
            )

    def _validate_successful_replay(self, plan: PurgePlan) -> None:
        if plan.completed_at is None or any(
            target.deletion_status not in {PurgeDeletionStatus.DELETED, PurgeDeletionStatus.ABSENT}
            for target in plan.targets
        ):
            raise RetentionLifecycleStateError(
                "successful purge lacks complete durable target state"
            )
        with self._store.read_only_connection() as connection:
            self._assert_state_is_unavailable(connection, plan)
            for target in plan.targets:
                self._validate_target_and_expected_files(connection, plan, target)
                if target.target_type is PurgeTargetType.RAW_ARTIFACT:
                    row = connection.execute(
                        "SELECT state FROM raw_artifacts WHERE artifact_id = ?",
                        (target.target_id,),
                    ).fetchone()
                    if row is None or str(row["state"]) != "PURGED":
                        raise RetentionLifecycleStateError(
                            "successful purge has a non-purged raw catalog entry"
                        )
                elif target.target_type is PurgeTargetType.CANONICAL_BATCH:
                    row = connection.execute(
                        "SELECT state FROM canonical_batches WHERE canonical_batch_id = ?",
                        (target.target_id,),
                    ).fetchone()
                    if row is None or str(row["state"]) != "PURGED":
                        raise RetentionLifecycleStateError(
                            "successful purge has a non-purged canonical catalog entry"
                        )
                elif target.target_type is PurgeTargetType.QUARANTINE:
                    row = connection.execute(
                        """
                        SELECT state FROM quarantine_artifacts
                        WHERE quarantine_artifact_id = ?
                        """,
                        (target.target_id,),
                    ).fetchone()
                    if row is None or str(row["state"]) != "PURGED":
                        raise RetentionLifecycleStateError(
                            "successful purge has a non-purged quarantine catalog entry"
                        )
        if any(not self._target_is_absent(target.relative_path) for target in plan.targets):
            raise RetentionPurgeSafetyError(
                "successful purge target reappeared under the private root"
            )

    @staticmethod
    def _assert_state_is_unavailable(
        connection: sqlite3.Connection,
        plan: PurgePlan,
    ) -> None:
        terminal = (
            "EXPIRED" if plan.trigger is RetentionInvalidationTrigger.TTL_EXPIRY else "TERMINATED"
        )
        status = connection.execute(
            """
            SELECT status, policy_snapshot_id, unavailable_at
            FROM dataset_policy_status WHERE provider = ? AND dataset = ?
            """,
            (plan.provider, plan.dataset),
        ).fetchone()
        if (
            status is None
            or str(status["status"]) != terminal
            or str(status["policy_snapshot_id"]) != plan.policy_snapshot_id
            or status["unavailable_at"] is None
        ):
            raise RetentionLifecycleStateError(
                "dataset is not durably unavailable under the purge policy snapshot"
            )
        visible_coverage = connection.execute(
            """
            SELECT COUNT(*)
            FROM coverage_segments AS coverage
            JOIN canonical_batches AS batch
              ON batch.canonical_batch_id = coverage.canonical_batch_id
            JOIN policy_snapshots AS snapshot
              ON snapshot.policy_snapshot_id = batch.policy_snapshot_id
            WHERE snapshot.provider = ? AND snapshot.dataset = ?
              AND (coverage.verification_state <> 'INVALID' OR coverage.retained <> 0)
            """,
            (plan.provider, plan.dataset),
        ).fetchone()
        valid_watermarks = connection.execute(
            """
            SELECT COUNT(*) FROM watermarks
            WHERE verification_state <> 'INVALID' AND stream_id IN (
                SELECT stream_id FROM stream_keys WHERE provider = ? AND dataset = ?
            )
            """,
            (plan.provider, plan.dataset),
        ).fetchone()
        if (
            visible_coverage is None
            or int(visible_coverage[0]) != 0
            or valid_watermarks is None
            or int(valid_watermarks[0]) != 0
        ):
            raise RetentionLifecycleStateError(
                "coverage or watermark remained valid after policy invalidation"
            )

    def _delete_target(
        self,
        purge_run_id: str,
        target: PurgeTargetRecord,
    ) -> PurgeDeletionStatus:
        with self._store.read_only_connection() as connection:
            run = self._load_plan_from_connection(connection, purge_run_id, replayed=False)
            expected_files = self._validate_target_and_expected_files(connection, run, target)
        if target.target_type is PurgeTargetType.CANONICAL_FILE:
            return self._delete_regular_file(target.relative_path)
        return self._delete_directory(target.relative_path, expected_files=expected_files)

    def _validate_target_and_expected_files(
        self,
        connection: sqlite3.Connection,
        plan: PurgePlan,
        target: PurgeTargetRecord,
    ) -> frozenset[str] | None:
        relative = PurePosixPath(target.relative_path)
        if target.target_type is PurgeTargetType.RAW_ARTIFACT:
            row = connection.execute(
                """
                SELECT artifact.relative_path, artifact.manifest_relative_path,
                       request.provider, request.dataset
                FROM raw_artifacts AS artifact
                JOIN request_specs AS request
                  ON request.request_spec_id = artifact.request_spec_id
                WHERE artifact.artifact_id = ?
                """,
                (target.target_id,),
            ).fetchone()
            expected = raw_artifact_relative_directory(
                plan.provider,
                plan.dataset,
                target.target_id,
            )
            if (
                row is None
                or (str(row["provider"]), str(row["dataset"])) != (plan.provider, plan.dataset)
                or relative != expected
                or PurePosixPath(str(row["relative_path"])) != expected / "payload.bin"
                or PurePosixPath(str(row["manifest_relative_path"])) != expected / "manifest.json"
            ):
                raise RetentionPurgeSafetyError("raw purge target is not the exact catalog target")
            return frozenset(
                {
                    (expected / "payload.bin").as_posix(),
                    (expected / "manifest.json").as_posix(),
                }
            )
        if target.target_type is PurgeTargetType.CANONICAL_FILE:
            batch_id, separator, ordinal = target.target_id.rpartition(":")
            if not separator or not ordinal.isdecimal():
                raise RetentionPurgeSafetyError("canonical file target ID is malformed")
            row = connection.execute(
                """
                SELECT file.relative_path, snapshot.provider, snapshot.dataset
                FROM canonical_files AS file
                JOIN canonical_batches AS batch
                  ON batch.canonical_batch_id = file.canonical_batch_id
                JOIN policy_snapshots AS snapshot
                  ON snapshot.policy_snapshot_id = batch.policy_snapshot_id
                WHERE file.canonical_batch_id = ? AND file.file_ordinal = ?
                """,
                (batch_id, int(ordinal)),
            ).fetchone()
            if (
                row is None
                or (str(row["provider"]), str(row["dataset"])) != (plan.provider, plan.dataset)
                or relative != PurePosixPath(str(row["relative_path"]))
            ):
                raise RetentionPurgeSafetyError(
                    "canonical file purge target is not the exact catalog target"
                )
            return None
        if target.target_type is PurgeTargetType.CANONICAL_BATCH:
            row = connection.execute(
                """
                SELECT batch.manifest_relative_path, snapshot.provider, snapshot.dataset
                FROM canonical_batches AS batch
                JOIN policy_snapshots AS snapshot
                  ON snapshot.policy_snapshot_id = batch.policy_snapshot_id
                WHERE batch.canonical_batch_id = ?
                """,
                (target.target_id,),
            ).fetchone()
            expected = _canonical_relative_directory(
                plan.provider,
                plan.dataset,
                target.target_id,
            )
            if (
                row is None
                or (str(row["provider"]), str(row["dataset"])) != (plan.provider, plan.dataset)
                or relative != expected
                or PurePosixPath(str(row["manifest_relative_path"])) != expected / "manifest.json"
            ):
                raise RetentionPurgeSafetyError(
                    "canonical batch purge target is not the exact catalog target"
                )
            files = connection.execute(
                """
                SELECT relative_path FROM canonical_files
                WHERE canonical_batch_id = ?
                """,
                (target.target_id,),
            ).fetchall()
            return frozenset(
                {
                    (expected / "manifest.json").as_posix(),
                    *(str(file["relative_path"]) for file in files),
                }
            )
        expected = _quarantine_relative_directory(
            plan.provider,
            plan.dataset,
            target.target_id,
        )
        row = connection.execute(
            """
            SELECT relative_path, manifest_content_sha256, manifest_byte_count,
                   provider, dataset
            FROM quarantine_artifacts WHERE quarantine_artifact_id = ?
            """,
            (target.target_id,),
        ).fetchone()
        if (
            row is None
            or (str(row["provider"]), str(row["dataset"])) != (plan.provider, plan.dataset)
            or relative != expected
            or PurePosixPath(str(row["relative_path"])) != expected
        ):
            raise RetentionPurgeSafetyError(
                "quarantine purge target is not the exact cataloged artifact directory"
            )
        return frozenset({(expected / "manifest.json").as_posix()})

    def _managed_target(self, relative_path: str) -> Path:
        try:
            return self._data_root.managed_path(
                Path(*PurePosixPath(relative_path).parts),
                expected_root_id=self._root_id,
            )
        except PrivateDataRootError as error:
            raise RetentionPurgeSafetyError(
                "purge target failed private-root validation"
            ) from error

    def _delete_regular_file(self, relative_path: str) -> PurgeDeletionStatus:
        path = self._managed_target(relative_path)
        if not path.exists() and not path.is_symlink():
            self._data_root.validate(expected_root_id=self._root_id)
            return PurgeDeletionStatus.ABSENT
        try:
            before = path.lstat()
        except OSError as error:
            raise RetentionPurgeSafetyError("cannot inspect exact purge file") from error
        if (
            _is_reparse_or_link(path, before)
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
        ):
            raise RetentionPurgeSafetyError(
                "purge file must be a direct regular non-link with one hard link"
            )
        self._data_root.validate(expected_root_id=self._root_id)
        try:
            immediately_before = path.lstat()
            if _file_identity(immediately_before) != _file_identity(before):
                raise RetentionPurgeSafetyError("purge file changed before deletion")
            path.unlink()
        except RetentionPurgeSafetyError:
            raise
        except OSError as error:
            raise RetentionPurgeSafetyError("failed to delete exact purge file") from error
        self._data_root.validate(expected_root_id=self._root_id)
        if path.exists() or path.is_symlink():
            raise RetentionPurgeSafetyError("purge file remained after deletion")
        return PurgeDeletionStatus.DELETED

    def _delete_directory(
        self,
        relative_path: str,
        *,
        expected_files: frozenset[str] | None,
    ) -> PurgeDeletionStatus:
        target = self._managed_target(relative_path)
        if not target.exists() and not target.is_symlink():
            self._data_root.validate(expected_root_id=self._root_id)
            return PurgeDeletionStatus.ABSENT
        files, directories = self._inventory_directory(target)
        if expected_files is not None:
            actual = {path.relative_to(self._data_root.root).as_posix() for path in files}
            unexpected = actual - expected_files
            if unexpected:
                raise RetentionPurgeSafetyError(
                    "cataloged purge directory contains an unexpected file"
                )
            allowed_directories = {
                parent.as_posix()
                for file in expected_files
                for parent in PurePosixPath(file).parents
                if parent != PurePosixPath(".")
            }
            actual_directories = {
                path.relative_to(self._data_root.root).as_posix() for path in directories
            }
            if not actual_directories <= allowed_directories:
                raise RetentionPurgeSafetyError(
                    "cataloged purge directory contains an unexpected subdirectory"
                )
        for file in sorted(files, key=lambda value: value.as_posix()):
            relative = file.relative_to(self._data_root.root).as_posix()
            self._delete_regular_file(relative)
        for directory in sorted(
            directories,
            key=lambda value: (len(value.parts), value.as_posix()),
            reverse=True,
        ):
            self._remove_empty_directory(directory)
        self._remove_empty_directory(target)
        if target.exists() or target.is_symlink():
            raise RetentionPurgeSafetyError("purge directory remained after deletion")
        return PurgeDeletionStatus.DELETED

    def _inventory_directory(self, target: Path) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
        try:
            root_details = target.lstat()
        except OSError as error:
            raise RetentionPurgeSafetyError("cannot inspect exact purge directory") from error
        if _is_reparse_or_link(target, root_details) or not stat.S_ISDIR(root_details.st_mode):
            raise RetentionPurgeSafetyError("purge directory must be a direct non-link directory")
        files: list[Path] = []
        directories: list[Path] = []
        pending = [target]
        while pending:
            directory = pending.pop()
            try:
                entries = tuple(os.scandir(directory))
            except OSError as error:
                raise RetentionPurgeSafetyError("cannot enumerate exact purge directory") from error
            for entry in entries:
                path = Path(entry.path)
                try:
                    # ``DirEntry.stat`` reports ``st_nlink == 0`` for ordinary files on
                    # some Windows/Python combinations.  ``Path.lstat`` uses the reliable
                    # handle-based result already used by the private-root boundary.
                    details = path.lstat()
                except OSError as error:
                    raise RetentionPurgeSafetyError(
                        "cannot inspect purge directory entry"
                    ) from error
                if _is_reparse_or_link(path, details):
                    raise RetentionPurgeSafetyError(
                        "purge directory contains a link, junction, or reparse point"
                    )
                if stat.S_ISDIR(details.st_mode):
                    directories.append(path)
                    pending.append(path)
                elif stat.S_ISREG(details.st_mode) and details.st_nlink == 1:
                    files.append(path)
                else:
                    raise RetentionPurgeSafetyError(
                        "purge directory contains a special or multiply-linked file"
                    )
        self._data_root.validate(expected_root_id=self._root_id)
        return tuple(files), tuple(directories)

    def _remove_empty_directory(self, directory: Path) -> None:
        relative = directory.relative_to(self._data_root.root)
        checked = self._managed_target(relative.as_posix())
        if checked != directory:
            raise RetentionPurgeSafetyError("purge directory identity changed")
        try:
            details = directory.lstat()
        except FileNotFoundError:
            return
        except OSError as error:
            raise RetentionPurgeSafetyError("cannot re-inspect purge directory") from error
        if _is_reparse_or_link(directory, details) or not stat.S_ISDIR(details.st_mode):
            raise RetentionPurgeSafetyError("purge directory changed before removal")
        self._data_root.validate(expected_root_id=self._root_id)
        try:
            directory.rmdir()
        except OSError as error:
            raise RetentionPurgeSafetyError(
                "purge directory is not empty or cannot be removed safely"
            ) from error
        self._data_root.validate(expected_root_id=self._root_id)

    def _target_is_absent(self, relative_path: str) -> bool:
        path = self._managed_target(relative_path)
        absent = not path.exists() and not path.is_symlink()
        self._data_root.validate(expected_root_id=self._root_id)
        return absent

    def _record_target_failure(
        self,
        lease: WriterLease,
        purge_run_id: str,
        target: PurgeTargetRecord,
        _error: RetentionPurgeSafetyError,
    ) -> None:
        with self._store._leased_transaction(lease) as connection:
            now = _format_utc(self._store._now())
            connection.execute(
                """
                UPDATE purge_targets SET deletion_status = 'FAILED', deleted_at = NULL
                WHERE purge_run_id = ? AND target_type = ? AND target_id = ?
                  AND deletion_status NOT IN ('DELETED', 'ABSENT')
                """,
                (purge_run_id, target.target_type.value, target.target_id),
            )
            connection.execute(
                """
                UPDATE purge_runs SET status = 'FAILED', completed_at = ?
                WHERE purge_run_id = ? AND status <> 'SUCCESS'
                """,
                (now, purge_run_id),
            )

    @staticmethod
    def _mark_catalog_content_purged(
        connection: sqlite3.Connection,
        plan: PurgePlan,
    ) -> None:
        for target in plan.targets:
            if target.target_type is PurgeTargetType.RAW_ARTIFACT:
                row = connection.execute(
                    "SELECT state FROM raw_artifacts WHERE artifact_id = ?",
                    (target.target_id,),
                ).fetchone()
                if row is None or str(row["state"]) not in {"INVALID", "PURGED"}:
                    raise RetentionLifecycleStateError(
                        "raw artifact was not invalid before purge finalization"
                    )
                connection.execute(
                    "UPDATE raw_artifacts SET state = 'PURGED' WHERE artifact_id = ?",
                    (target.target_id,),
                )
            elif target.target_type is PurgeTargetType.CANONICAL_BATCH:
                row = connection.execute(
                    "SELECT state FROM canonical_batches WHERE canonical_batch_id = ?",
                    (target.target_id,),
                ).fetchone()
                if row is None or str(row["state"]) not in {"INVALID", "PURGED"}:
                    raise RetentionLifecycleStateError(
                        "canonical batch was not invalid before purge finalization"
                    )
                connection.execute(
                    """
                    UPDATE canonical_batches SET state = 'PURGED'
                    WHERE canonical_batch_id = ?
                    """,
                    (target.target_id,),
                )
            elif target.target_type is PurgeTargetType.QUARANTINE:
                row = connection.execute(
                    """
                    SELECT state FROM quarantine_artifacts
                    WHERE quarantine_artifact_id = ?
                    """,
                    (target.target_id,),
                ).fetchone()
                if row is None or str(row["state"]) not in {"INVALID", "PURGED"}:
                    raise RetentionLifecycleStateError(
                        "quarantine artifact was not invalid before purge finalization"
                    )
                connection.execute(
                    """
                    UPDATE quarantine_artifacts SET state = 'PURGED'
                    WHERE quarantine_artifact_id = ?
                    """,
                    (target.target_id,),
                )

    @staticmethod
    def _result(plan: PurgePlan, *, replayed: bool) -> PurgeExecutionResult:
        return PurgeExecutionResult(
            purge_run_id=plan.purge_run_id,
            status=plan.status,
            target_count=len(plan.targets),
            deleted_count=sum(
                target.deletion_status is PurgeDeletionStatus.DELETED for target in plan.targets
            ),
            absent_count=sum(
                target.deletion_status is PurgeDeletionStatus.ABSENT for target in plan.targets
            ),
            replayed=replayed,
        )


__all__ = [
    "PurgeDeletionStatus",
    "PurgeExecutionResult",
    "PurgePlan",
    "PurgeRunStatus",
    "PurgeTargetRecord",
    "PurgeTargetType",
    "QuarantinePurgeTarget",
    "RetentionFaultInjector",
    "RetentionFaultPoint",
    "RetentionInvalidationTrigger",
    "RetentionLifecycleError",
    "RetentionLifecycleRepository",
    "RetentionLifecycleStateError",
    "RetentionPurgeSafetyError",
]
