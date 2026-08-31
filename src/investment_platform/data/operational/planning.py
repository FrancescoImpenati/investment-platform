"""Typed, lease-fenced SQLite persistence for deterministic ingestion plans.

This repository persists metadata only.  It does not dispatch providers, write
artifacts, publish canonical data, or mutate coverage, gaps, or watermarks.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Self, cast
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.ingestion.identity import (
    IDENTITY_CANONICALIZATION_VERSION,
    RequestInstanceIdentity,
    RequestSpecification,
    StreamKey,
)
from investment_platform.data.ingestion.planner import (
    AcquisitionStrategy,
    CoverageClassification,
    CoverageVerificationState,
    IngestionPlan,
    PlannedRequest,
    RepairStrategy,
    VerifiedCoverageProjection,
)
from investment_platform.data.operational.store import (
    OperationalStateError,
    OperationalStateStore,
    WriterLease,
    _format_utc,
    _parse_utc,
)
from investment_platform.data.retention import (
    DatasetPolicySnapshot,
    DatasetPolicyStatus,
    RetentionMode,
)

PLANNER_PERSISTENCE_CONTRACT_VERSION = 1
_MAX_DURABLE_REQUEST_DISPATCHES = 1000
_REQUEST_INSTANCE_NAMESPACE = UUID("f1680721-6906-47d0-a762-4e9fd3d771d5")
_DURABLE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,191}$"
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_SECRET_TEXT = re.compile(
    r"(?:[a-z][a-z0-9+.-]*://|(?:authorization|api[_-]?key|password|secret|token)\s*[:=])",
    re.IGNORECASE,
)


class PlanRepositoryError(OperationalStateError):
    """Base error for durable plan metadata."""


class PlanNotFoundError(PlanRepositoryError):
    """Raised when a typed plan/run identity is not cataloged."""


class PlanIdentityCollisionError(PlanRepositoryError):
    """Raised when a durable identity resolves to different immutable metadata."""


class PlanPolicyMismatchError(PlanRepositoryError):
    """Raised when plan policy provenance is missing, stale, or inexact."""


class PlanCalendarMismatchError(PlanRepositoryError):
    """Raised when the referenced calendar is absent, stale, or different."""


class RequestStateConflictError(PlanRepositoryError):
    """Raised when a compare-and-set request transition cannot be applied."""


class IngestionRunStatus(StrEnum):
    PLANNED = "PLANNED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class RequestInstanceStatus(StrEnum):
    PLANNED = "PLANNED"
    DISPATCHING = "DISPATCHING"
    ACQUIRING = "ACQUIRING"
    RAW_COMPLETE = "RAW_COMPLETE"
    PROCESSING = "PROCESSING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"


class RequestResumeAction(StrEnum):
    DISPATCH = "DISPATCH"
    RECONCILE_ACQUISITION = "RECONCILE_ACQUISITION"
    RESUME_PROCESSING = "RESUME_PROCESSING"
    WAIT_RETRY = "WAIT_RETRY"
    NONE = "NONE"


_TERMINAL_REQUEST_STATES = frozenset(
    {
        RequestInstanceStatus.SUCCESS,
        RequestInstanceStatus.PARTIAL,
        RequestInstanceStatus.FAILED,
        RequestInstanceStatus.BLOCKED,
        RequestInstanceStatus.CANCELLED,
    }
)


class _FrozenOperationalModel(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class PlanPersistenceRequest(_FrozenOperationalModel):
    """Caller-owned run identity plus one already authorized deterministic plan."""

    run_id: UUID
    calendar_snapshot_id: str = Field(pattern=_DURABLE_ID_PATTERN, max_length=128)
    reason: Annotated[str, Field(min_length=1, max_length=256)]
    max_attempts: Annotated[int, Field(gt=0)]
    max_pages: Annotated[
        int,
        Field(gt=0, le=_MAX_DURABLE_REQUEST_DISPATCHES),
    ] = _MAX_DURABLE_REQUEST_DISPATCHES
    max_calls: Annotated[
        int,
        Field(gt=0, le=_MAX_DURABLE_REQUEST_DISPATCHES),
    ] = _MAX_DURABLE_REQUEST_DISPATCHES
    max_pages_per_request: Annotated[
        int,
        Field(gt=0, le=_MAX_DURABLE_REQUEST_DISPATCHES),
    ] = _MAX_DURABLE_REQUEST_DISPATCHES
    max_calls_per_request: Annotated[
        int,
        Field(gt=0, le=_MAX_DURABLE_REQUEST_DISPATCHES),
    ] = _MAX_DURABLE_REQUEST_DISPATCHES
    plan: IngestionPlan

    @field_validator("reason", mode="after")
    @classmethod
    def reject_sensitive_reason(cls, value: str) -> str:
        if _is_sensitive_text(value):
            raise ValueError("plan reason contains a URL, secret-shaped text, or line break")
        return value

    @model_validator(mode="after")
    def validate_execution_limits(self) -> Self:
        if self.max_pages < self.plan.estimated_pages:
            raise ValueError("run page ceiling is below the deterministic plan estimate")
        if self.max_calls < self.plan.estimated_calls:
            raise ValueError("run call ceiling is below the deterministic plan estimate")
        return self


class PersistedIngestionPlan(_FrozenOperationalModel):
    run_id: UUID
    plan_hash: str = Field(pattern=_SHA256_PATTERN)
    policy_snapshot_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    catalog_snapshot_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    calendar_snapshot_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    status: IngestionRunStatus
    request_count: Annotated[int, Field(ge=0)]
    no_op: bool
    recorded_at: datetime
    replayed: bool

    @field_validator("recorded_at", mode="after")
    @classmethod
    def normalize_recorded_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("recorded_at must be timezone-aware")
        return value.astimezone(UTC)


class RequestProgress(_FrozenOperationalModel):
    request_instance_id: UUID
    request_spec_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    request_spec_hash: str = Field(pattern=_SHA256_PATTERN)
    plan_ordinal: Annotated[int, Field(ge=0)]
    status: RequestInstanceStatus
    interval_start: datetime
    interval_end: datetime
    attempt_count: Annotated[int, Field(ge=0)]
    latest_attempt_status: str | None = None
    next_eligible_at: datetime | None = None

    @field_validator("interval_start", "interval_end", "next_eligible_at", mode="after")
    @classmethod
    def normalize_times(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("request progress timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.interval_end <= self.interval_start:
            raise ValueError("request progress interval is invalid")
        return self

    @property
    def terminal(self) -> bool:
        return self.status in _TERMINAL_REQUEST_STATES

    @property
    def resume_action(self) -> RequestResumeAction:
        return {
            RequestInstanceStatus.PLANNED: RequestResumeAction.DISPATCH,
            RequestInstanceStatus.DISPATCHING: RequestResumeAction.RECONCILE_ACQUISITION,
            RequestInstanceStatus.ACQUIRING: RequestResumeAction.RECONCILE_ACQUISITION,
            RequestInstanceStatus.RAW_COMPLETE: RequestResumeAction.RESUME_PROCESSING,
            RequestInstanceStatus.PROCESSING: RequestResumeAction.RESUME_PROCESSING,
            RequestInstanceStatus.RETRY_WAIT: RequestResumeAction.WAIT_RETRY,
            RequestInstanceStatus.SUCCESS: RequestResumeAction.NONE,
            RequestInstanceStatus.PARTIAL: RequestResumeAction.NONE,
            RequestInstanceStatus.FAILED: RequestResumeAction.NONE,
            RequestInstanceStatus.BLOCKED: RequestResumeAction.NONE,
            RequestInstanceStatus.CANCELLED: RequestResumeAction.NONE,
        }[self.status]


class IngestionRunProgress(_FrozenOperationalModel):
    run_id: UUID
    plan_hash: str = Field(pattern=_SHA256_PATTERN)
    status: IngestionRunStatus
    provider: str
    dataset: str
    intent: str
    environment: str
    calendar_snapshot_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    policy_snapshot_id: str = Field(pattern=_DURABLE_ID_PATTERN)
    planned_request_count: Annotated[int, Field(ge=0)]
    terminal_request_count: Annotated[int, Field(ge=0)]
    requests: tuple[RequestProgress, ...]

    @model_validator(mode="after")
    def validate_request_count(self) -> Self:
        if self.planned_request_count != len(self.requests):
            raise ValueError("run request count does not match durable request instances")
        if self.terminal_request_count != sum(request.terminal for request in self.requests):
            raise ValueError("terminal request count does not match request states")
        return self

    @property
    def no_op(self) -> bool:
        return self.planned_request_count == 0

    @property
    def resumable_requests(self) -> tuple[RequestProgress, ...]:
        return tuple(
            request
            for request in self.requests
            if request.resume_action is not RequestResumeAction.NONE
        )


class CoverageReadResult(_FrozenOperationalModel):
    """Coverage facts safe to pass to the planner plus fail-closed rejection count."""

    windows: tuple[VerifiedCoverageProjection, ...]
    rejected_count: Annotated[int, Field(ge=0)]


def deterministic_policy_snapshot_id(snapshot: DatasetPolicySnapshot) -> str:
    """Identify the semantic policy version, excluding per-run capture time/catalog use."""

    payload = {
        "dataset": snapshot.dataset,
        "mode": snapshot.mode.value,
        "policy_hash": snapshot.policy_hash,
        "policy_id": snapshot.policy_id,
        "policy_revision": snapshot.policy_revision,
        "provider": snapshot.provider,
        "status": snapshot.status.value,
        "verified_on": snapshot.verified_on.isoformat(),
    }
    return f"policy-{_hash_json(payload)}"


def deterministic_catalog_snapshot_id(snapshot: DatasetPolicySnapshot) -> str:
    """Identify the exact committed catalog used by one planning authorization."""

    payload = {
        "catalog_hash": snapshot.catalog_hash,
        "catalog_id": snapshot.catalog_id,
        "catalog_revision": snapshot.catalog_revision,
    }
    return f"catalog-{_hash_json(payload)}"


def deterministic_request_instance_identity(
    run_id: UUID,
    plan_ordinal: int,
    specification: RequestSpecification,
) -> RequestInstanceIdentity:
    """Create the stable request-instance UUID for one run/ordinal/specification."""

    value = f"request-instance-v1/{run_id}/{plan_ordinal}/{specification.request_spec_hash}"
    return RequestInstanceIdentity(
        request_instance_id=uuid5(_REQUEST_INSTANCE_NAMESPACE, value),
        request_spec_hash=specification.request_spec_hash,
    )


class IngestionPlanRepository:
    """Persist plans atomically and expose read-only restart/progress state."""

    def __init__(self, store: OperationalStateStore) -> None:
        self._store = store

    def persist(
        self,
        lease: WriterLease,
        request: PlanPersistenceRequest,
    ) -> PersistedIngestionPlan:
        """Persist policy, streams, run, and ordered requests in one fenced transaction."""

        self._validate_plan(request)
        plan_hash = _plan_hash(request)
        policy_snapshot = request.plan.policy_authorization.policy_snapshot
        policy_snapshot_id = deterministic_policy_snapshot_id(policy_snapshot)
        catalog_snapshot_id = deterministic_catalog_snapshot_id(policy_snapshot)
        try:
            with self._store._leased_transaction(lease) as connection:
                recorded_at = self._store._now()
                self._require_calendar(
                    connection,
                    request.calendar_snapshot_id,
                    request.plan.calendar_snapshot_checksum,
                )
                self._persist_policy(
                    connection,
                    policy_snapshot,
                    policy_snapshot_id=policy_snapshot_id,
                    catalog_snapshot_id=catalog_snapshot_id,
                    recorded_at=recorded_at,
                )
                # Adopt-or-compare every complete stream row on both first persist
                # and replay. Stream IDs/ordinals alone cannot detect tampering.
                for stream in request.plan.streams:
                    self._persist_stream(connection, stream, recorded_at)
                existing = connection.execute(
                    "SELECT 1 FROM ingestion_runs WHERE run_id = ?",
                    (str(request.run_id),),
                ).fetchone()
                if existing is not None:
                    self._validate_replay(
                        connection,
                        request,
                        plan_hash=plan_hash,
                        policy_snapshot_id=policy_snapshot_id,
                        catalog_snapshot_id=catalog_snapshot_id,
                    )
                    row = self._load_plan_row(connection, str(request.run_id))
                    return self._persisted_result(row, replayed=True)

                no_op = not request.plan.requests
                run_status = IngestionRunStatus.SUCCESS if no_op else IngestionRunStatus.PLANNED
                connection.execute(
                    """
                    INSERT INTO ingestion_runs(
                        run_id, mode, environment, provider, dataset, status,
                        policy_snapshot_id, created_at, started_at, completed_at,
                        planned_request_count, succeeded_request_count, failed_request_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
                    """,
                    (
                        str(request.run_id),
                        request.plan.intent.value,
                        request.plan.environment.value,
                        request.plan.provider,
                        request.plan.dataset,
                        run_status.value,
                        policy_snapshot_id,
                        _format_utc(recorded_at),
                        _format_utc(recorded_at) if no_op else None,
                        _format_utc(recorded_at) if no_op else None,
                        len(request.plan.requests),
                    ),
                )
                limits_hash = _execution_limits_hash(request)
                request_limits = _allocate_request_execution_limits(request)
                connection.execute(
                    """
                    INSERT INTO ingestion_execution_limits(
                        run_id, max_pages, max_calls, max_pages_per_request,
                        max_calls_per_request, limits_hash
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(request.run_id),
                        request.max_pages,
                        request.max_calls,
                        request.max_pages_per_request,
                        request.max_calls_per_request,
                        limits_hash,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO ingestion_plan_records(
                        run_id, plan_hash, planner_contract_version,
                        calendar_snapshot_id, calendar_snapshot_checksum,
                        policy_snapshot_id, catalog_snapshot_id, reason,
                        acquisition_strategy, repair_strategy, repair_reason, max_attempts,
                        desired_start, desired_end, safe_end, authorized_at,
                        eligible_slot_count, eligible_observation_count,
                        missing_observation_count, pending_observation_count,
                        estimated_pages, estimated_calls, estimated_bytes, estimated_cost,
                        lease_owner_id, lease_generation, recorded_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    self._plan_record_values(
                        request,
                        plan_hash=plan_hash,
                        policy_snapshot_id=policy_snapshot_id,
                        catalog_snapshot_id=catalog_snapshot_id,
                        lease=lease,
                        recorded_at=recorded_at,
                    ),
                )
                connection.executemany(
                    """
                    INSERT INTO ingestion_plan_streams(run_id, stream_id, ordinal)
                    VALUES (?, ?, ?)
                    """,
                    tuple(
                        (str(request.run_id), stream.stream_id, ordinal)
                        for ordinal, stream in enumerate(request.plan.streams)
                    ),
                )
                for ordinal, planned in enumerate(request.plan.requests):
                    self._persist_planned_request(
                        connection,
                        request,
                        planned,
                        ordinal=ordinal,
                        execution_limit=request_limits[ordinal],
                        policy_snapshot_id=policy_snapshot_id,
                        recorded_at=recorded_at,
                    )
                row = self._load_plan_row(connection, str(request.run_id))
                return self._persisted_result(row, replayed=False)
        except sqlite3.IntegrityError as error:
            raise PlanIdentityCollisionError(
                "SQLite rejected plan metadata; no partial plan was committed"
            ) from error

    def load_progress(self, run_id: UUID) -> IngestionRunProgress:
        """Load restart-safe ordered request status without mutating the run."""

        with self._store.read_only_connection() as connection:
            row = connection.execute(
                """
                SELECT r.*, p.plan_hash, p.calendar_snapshot_id
                FROM ingestion_runs AS r
                JOIN ingestion_plan_records AS p ON p.run_id = r.run_id
                WHERE r.run_id = ?
                """,
                (str(run_id),),
            ).fetchone()
            if row is None:
                raise PlanNotFoundError("ingestion plan is not cataloged")
            request_rows = connection.execute(
                """
                SELECT
                    i.request_instance_id, i.plan_ordinal, i.status,
                    s.request_spec_id, s.request_spec_hash,
                    s.interval_start, s.interval_end,
                    (SELECT count(*) FROM request_attempts AS a
                     WHERE a.request_instance_id = i.request_instance_id) AS attempt_count,
                    (SELECT a.status FROM request_attempts AS a
                     WHERE a.request_instance_id = i.request_instance_id
                     ORDER BY a.attempt_number DESC LIMIT 1) AS latest_attempt_status,
                    (SELECT retry.next_eligible_at FROM retry_state AS retry
                     WHERE retry.request_instance_id = i.request_instance_id) AS next_eligible_at
                FROM request_instances AS i
                JOIN request_specs AS s ON s.request_spec_id = i.request_spec_id
                WHERE i.run_id = ?
                ORDER BY i.plan_ordinal
                """,
                (str(run_id),),
            ).fetchall()
        requests = tuple(self._request_progress(value) for value in request_rows)
        return IngestionRunProgress(
            run_id=run_id,
            plan_hash=str(row["plan_hash"]),
            status=IngestionRunStatus(str(row["status"])),
            provider=str(row["provider"]),
            dataset=str(row["dataset"]),
            intent=str(row["mode"]),
            environment=str(row["environment"]),
            calendar_snapshot_id=str(row["calendar_snapshot_id"]),
            policy_snapshot_id=str(row["policy_snapshot_id"]),
            planned_request_count=int(row["planned_request_count"]),
            terminal_request_count=sum(request.terminal for request in requests),
            requests=requests,
        )

    def transition_request_status(
        self,
        lease: WriterLease,
        *,
        run_id: UUID,
        request_instance_id: UUID,
        expected: RequestInstanceStatus,
        new: RequestInstanceStatus,
    ) -> RequestProgress:
        """Apply one trigger-validated compare-and-set request transition."""

        if new is expected:
            raise RequestStateConflictError("request transition must change status")
        try:
            with self._store._leased_transaction(lease) as connection:
                row = connection.execute(
                    """
                    SELECT status FROM request_instances
                    WHERE request_instance_id = ? AND run_id = ?
                    """,
                    (str(request_instance_id), str(run_id)),
                ).fetchone()
                if row is None:
                    raise PlanNotFoundError("request instance is not part of the run")
                if str(row["status"]) != expected.value:
                    raise RequestStateConflictError("request state changed before compare-and-set")
                connection.execute(
                    """
                    UPDATE request_instances
                    SET status = ?, completed_at = ?
                    WHERE request_instance_id = ? AND run_id = ? AND status = ?
                    """,
                    (
                        new.value,
                        _format_utc(self._store._now())
                        if new in _TERMINAL_REQUEST_STATES
                        else None,
                        str(request_instance_id),
                        str(run_id),
                        expected.value,
                    ),
                )
        except sqlite3.IntegrityError as error:
            raise RequestStateConflictError(
                "request transition violates the durable state machine"
            ) from error
        progress = self.load_progress(run_id)
        for request in progress.requests:
            if request.request_instance_id == request_instance_id:
                return request
        raise PlanNotFoundError("request disappeared after its state transition")

    def load_planner_coverage(
        self,
        streams: Sequence[StreamKey],
        *,
        calendar_snapshot_id: str,
    ) -> CoverageReadResult:
        """Project verified joins and file presence into read-only planner evidence."""

        if not streams:
            return CoverageReadResult(windows=(), rejected_count=0)
        stream_by_id = {stream.stream_id: stream for stream in streams}
        if len(stream_by_id) != len(streams):
            raise PlanRepositoryError("coverage read streams contain duplicates")
        placeholders = ",".join("?" for _ in stream_by_id)
        with self._store.read_only_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    c.*, cal.schedule_checksum, cal.state AS calendar_state,
                    batch.state AS batch_state, batch.verified_at AS batch_verified_at,
                    batch.manifest_relative_path AS batch_manifest_relative_path,
                    context.batch_context_id AS batch_context_id,
                    context.request_spec_id AS context_request_spec_id,
                    context.ordered_artifacts_hash AS context_ordered_artifacts_hash,
                    batch_stream.outcome AS batch_stream_outcome,
                    batch_stream.interval_start AS batch_stream_start,
                    batch_stream.interval_end AS batch_stream_end,
                    policy.provider AS policy_provider, policy.dataset AS policy_dataset,
                    policy.policy_id, policy.revision AS policy_revision,
                    policy.policy_hash, policy.retention_mode,
                    active.status AS active_policy_status,
                    active.policy_snapshot_id AS active_policy_snapshot_id,
                    active.retention_mode AS active_retention_mode,
                    active.expires_at AS active_expires_at,
                    active.unavailable_at AS active_unavailable_at,
                    active_policy.policy_hash AS active_policy_hash
                FROM coverage_segments AS c
                JOIN calendar_snapshots AS cal
                    ON cal.calendar_snapshot_id = c.calendar_snapshot_id
                JOIN canonical_batches AS batch
                    ON batch.canonical_batch_id = c.canonical_batch_id
                JOIN batch_contexts AS context
                    ON context.batch_context_id = batch.batch_context_id
                JOIN canonical_batch_streams AS batch_stream
                    ON batch_stream.canonical_batch_id = c.canonical_batch_id
                   AND batch_stream.stream_id = c.stream_id
                JOIN policy_snapshots AS policy
                    ON policy.policy_snapshot_id = c.policy_snapshot_id
                LEFT JOIN dataset_policy_status AS active
                    ON active.provider = policy.provider AND active.dataset = policy.dataset
                LEFT JOIN policy_snapshots AS active_policy
                    ON active_policy.policy_snapshot_id = active.policy_snapshot_id
                WHERE c.calendar_snapshot_id = ?
                  AND c.stream_id IN ({placeholders})
                ORDER BY c.stream_id, c.interval_start, c.interval_end, c.coverage_id
                """,
                (calendar_snapshot_id, *stream_by_id),
            ).fetchall()
            now = self._store._now()
            windows: list[VerifiedCoverageProjection] = []
            rejected = 0
            for row in rows:
                provenance = connection.execute(
                    """
                    SELECT
                        link.request_instance_id,
                        instance.status AS request_status,
                        instance.request_spec_id,
                        EXISTS(
                            SELECT 1 FROM request_attempts AS attempt
                            WHERE attempt.request_instance_id = link.request_instance_id
                              AND attempt.status IN ('RAW_COMPLETE', 'SUCCESS')
                              AND attempt.page_count > 0
                              AND attempt.pagination_complete = 1
                              AND attempt.terminal_page_verified = 1
                        ) AS pagination_complete
                    FROM canonical_batch_requests AS link
                    JOIN batch_context_requests AS context_link
                      ON context_link.request_instance_id = link.request_instance_id
                    JOIN request_instances AS instance
                      ON instance.request_instance_id = link.request_instance_id
                    WHERE link.canonical_batch_id = ? AND link.policy_snapshot_id = ?
                      AND context_link.batch_context_id = ?
                      AND instance.request_spec_id = ?
                      AND instance.status IN ('SUCCESS', 'PARTIAL')
                    ORDER BY link.request_instance_id
                    """,
                    (
                        str(row["canonical_batch_id"]),
                        str(row["policy_snapshot_id"]),
                        str(row["batch_context_id"]),
                        str(row["context_request_spec_id"]),
                    ),
                ).fetchall()
                proof = next(
                    (value for value in provenance if bool(value["pagination_complete"])),
                    None,
                )
                if proof is None:
                    rejected += 1
                    continue
                canonical_files = connection.execute(
                    """
                    SELECT relative_path, content_sha256, byte_count
                    FROM canonical_files
                    WHERE canonical_batch_id = ? ORDER BY file_ordinal
                    """,
                    (str(row["canonical_batch_id"]),),
                ).fetchall()
                raw_artifacts = connection.execute(
                    """
                    SELECT
                        relation.ordinal, artifact.artifact_id,
                        artifact.request_spec_id, artifact.page_ordinal,
                        artifact.state,
                        artifact.relative_path, artifact.manifest_relative_path,
                        artifact.content_sha256, artifact.byte_count
                    FROM batch_context_artifacts AS relation
                    JOIN raw_artifacts AS artifact
                      ON artifact.artifact_id = relation.artifact_id
                    WHERE relation.batch_context_id = ?
                    ORDER BY relation.ordinal
                    """,
                    (str(row["batch_context_id"]),),
                ).fetchall()
                raw_relation_verified = (
                    tuple(int(value["ordinal"]) for value in raw_artifacts)
                    == tuple(range(len(raw_artifacts)))
                    and len(raw_artifacts) == int(row["artifact_count"])
                    and _ordered_raw_artifact_ids_hash(
                        tuple(str(value["artifact_id"]) for value in raw_artifacts)
                    )
                    == str(row["context_ordered_artifacts_hash"])
                    and all(
                        int(value["ordinal"]) == int(value["page_ordinal"])
                        and str(value["request_spec_id"]) == str(row["context_request_spec_id"])
                        and str(value["state"]) == "VERIFIED"
                        for value in raw_artifacts
                    )
                )
                artifacts_present = (
                    bool(canonical_files)
                    and bool(raw_artifacts)
                    and self._store._managed_regular_file_is_present(
                        str(row["batch_manifest_relative_path"])
                    )
                    and all(
                        self._store._managed_file_matches_catalog(
                            str(value["relative_path"]),
                            expected_sha256=str(value["content_sha256"]),
                            expected_bytes=int(value["byte_count"]),
                        )
                        for value in (*canonical_files, *raw_artifacts)
                    )
                    and all(
                        self._store._managed_regular_file_is_present(
                            str(value["manifest_relative_path"])
                        )
                        for value in raw_artifacts
                    )
                )
                policy_valid = self._coverage_policy_valid(row, now=now)
                interval_verified = (
                    str(row["verification_state"]) == CoverageVerificationState.VERIFIED.value
                    and row["invalidated_at"] is None
                    and str(row["batch_state"]) == "VERIFIED"
                    and row["batch_verified_at"] is not None
                    and str(row["calendar_state"]) == "CURRENT"
                )
                relational_provenance_verified = (
                    str(proof["request_spec_id"]) == str(row["context_request_spec_id"])
                    and str(row["batch_stream_outcome"]) == "PUBLISHABLE"
                    and _parse_utc(str(row["batch_stream_start"]))
                    <= _parse_utc(str(row["interval_start"]))
                    and _parse_utc(str(row["batch_stream_end"]))
                    >= _parse_utc(str(row["interval_end"]))
                    and str(row["policy_provider"]) == stream_by_id[str(row["stream_id"])].provider
                    and str(row["policy_dataset"]) == stream_by_id[str(row["stream_id"])].dataset
                    and raw_relation_verified
                )
                request_completed = bool(row["request_completed"]) and str(
                    proof["request_status"]
                ) in {
                    RequestInstanceStatus.SUCCESS.value,
                    RequestInstanceStatus.PARTIAL.value,
                }
                pagination_verified = bool(row["pagination_verified"]) and bool(
                    proof["pagination_complete"]
                )
                canonical_batch_verified = (
                    str(row["batch_state"]) == "VERIFIED" and row["batch_verified_at"] is not None
                )
                if not (
                    bool(row["retained"])
                    and policy_valid
                    and interval_verified
                    and relational_provenance_verified
                    and request_completed
                    and pagination_verified
                    and canonical_batch_verified
                    and artifacts_present
                ):
                    rejected += 1
                    continue
                try:
                    windows.append(
                        VerifiedCoverageProjection(
                            coverage_id=str(row["coverage_id"]),
                            request_instance_id=str(proof["request_instance_id"]),
                            canonical_batch_id=str(row["canonical_batch_id"]),
                            policy_snapshot_id=str(row["policy_snapshot_id"]),
                            active_policy_snapshot_id=str(row["active_policy_snapshot_id"]),
                            calendar_snapshot_id=str(row["calendar_snapshot_id"]),
                            stream=stream_by_id[str(row["stream_id"])],
                            start=_parse_utc(str(row["interval_start"])),
                            end=_parse_utc(str(row["interval_end"])),
                            classification=CoverageClassification(str(row["classification"])),
                            verification_state=CoverageVerificationState(
                                str(row["verification_state"])
                            ),
                            retained=bool(row["retained"]),
                            policy_valid=policy_valid,
                            policy_id=str(row["policy_id"]),
                            policy_revision=int(row["policy_revision"]),
                            policy_hash=str(row["policy_hash"]),
                            active_policy_hash=str(row["active_policy_hash"]),
                            calendar_snapshot_checksum=str(row["schedule_checksum"]),
                            relational_provenance_verified=relational_provenance_verified,
                            interval_verified=interval_verified,
                            request_completed=request_completed,
                            pagination_verified=pagination_verified,
                            canonical_batch_verified=canonical_batch_verified,
                            canonical_file_count=len(canonical_files),
                            raw_artifact_count=len(raw_artifacts),
                            artifacts_present=artifacts_present,
                            provider_semantics_version=(
                                str(row["provider_semantics_version"])
                                if row["provider_semantics_version"] is not None
                                else None
                            ),
                        )
                    )
                except (TypeError, ValueError):
                    rejected += 1
        return CoverageReadResult(windows=tuple(windows), rejected_count=rejected)

    @staticmethod
    def _coverage_policy_valid(row: sqlite3.Row, *, now: datetime) -> bool:
        if (
            str(row["active_policy_status"]) != "ACTIVE"
            or str(row["active_policy_snapshot_id"]) != str(row["policy_snapshot_id"])
            or str(row["active_policy_hash"]) != str(row["policy_hash"])
            or str(row["active_retention_mode"]) != str(row["retention_mode"])
            or str(row["policy_provider"]) == ""
            or str(row["policy_dataset"]) == ""
        ):
            return False
        expires_at = row["active_expires_at"]
        unavailable_at = row["active_unavailable_at"]
        return not (
            (expires_at is not None and _parse_utc(str(expires_at)) <= now)
            or (unavailable_at is not None and _parse_utc(str(unavailable_at)) <= now)
        )

    @staticmethod
    def _validate_plan(request: PlanPersistenceRequest) -> None:
        plan = request.plan
        if _is_sensitive_text(request.reason):
            raise PlanRepositoryError("plan reason contains unsafe persisted text")
        if plan.repair_reason is not None and _is_sensitive_text(plan.repair_reason):
            raise PlanRepositoryError("repair reason contains unsafe persisted text")
        snapshot = plan.policy_authorization.policy_snapshot
        if snapshot.status is not DatasetPolicyStatus.ACTIVE:
            raise PlanPolicyMismatchError("planning policy snapshot is not active")
        if snapshot.mode is RetentionMode.PROHIBITED:
            raise PlanPolicyMismatchError("prohibited policy cannot authorize an ingestion plan")
        if (snapshot.provider, snapshot.dataset) != (plan.provider, plan.dataset):
            raise PlanPolicyMismatchError("plan does not match its exact policy dataset")
        if plan.acquisition_strategy is not AcquisitionStrategy.NETWORK:
            raise PlanIdentityCollisionError("only bounded network acquisition may be persisted")
        if plan.intent.value == "REPAIR":
            if (
                plan.repair_strategy
                not in {
                    RepairStrategy.MISSING_ONLY,
                    RepairStrategy.PROVIDER_REFRESH,
                }
                or plan.repair_reason is None
            ):
                raise PlanIdentityCollisionError(
                    "repair persistence requires a supported explicit strategy and reason"
                )
        elif plan.repair_strategy is not None or plan.repair_reason is not None:
            raise PlanIdentityCollisionError("non-repair plans cannot persist repair provenance")
        request_spec_ids = [item.specification.request_spec_id for item in plan.requests]
        if len(request_spec_ids) != len(set(request_spec_ids)):
            raise PlanIdentityCollisionError("plan contains duplicate bounded request identities")
        stream_ids = set(plan.stream_ids)
        if any(
            stream.stream_id not in stream_ids
            for item in plan.requests
            for stream in item.specification.stream_keys()
        ):
            raise PlanIdentityCollisionError("bounded request references a stream outside the plan")
        if any(
            item.authorization.request_spec_hash != item.specification.request_spec_hash
            for item in plan.requests
        ):
            raise PlanIdentityCollisionError(
                "bounded request authorization differs from its specification identity"
            )

    @staticmethod
    def _require_calendar(
        connection: sqlite3.Connection,
        calendar_snapshot_id: str,
        checksum: str,
    ) -> None:
        row = connection.execute(
            """
            SELECT schedule_checksum, state FROM calendar_snapshots
            WHERE calendar_snapshot_id = ?
            """,
            (calendar_snapshot_id,),
        ).fetchone()
        if row is None:
            raise PlanCalendarMismatchError("calendar snapshot must be persisted before the plan")
        if str(row["schedule_checksum"]) != checksum or str(row["state"]) != "CURRENT":
            raise PlanCalendarMismatchError(
                "calendar snapshot is stale or has a different checksum"
            )

    @staticmethod
    def _persist_policy(
        connection: sqlite3.Connection,
        snapshot: DatasetPolicySnapshot,
        *,
        policy_snapshot_id: str,
        catalog_snapshot_id: str,
        recorded_at: datetime,
    ) -> None:
        existing = connection.execute(
            "SELECT * FROM policy_snapshots WHERE policy_snapshot_id = ?",
            (policy_snapshot_id,),
        ).fetchone()
        expected_policy = (
            snapshot.policy_id,
            snapshot.policy_revision,
            snapshot.policy_hash,
            snapshot.provider,
            snapshot.dataset,
            snapshot.mode.value,
            snapshot.verified_on.isoformat(),
        )
        if existing is None:
            connection.execute(
                """
                INSERT INTO policy_snapshots(
                    policy_snapshot_id, policy_id, revision, policy_hash,
                    provider, dataset, retention_mode, verified_at,
                    captured_at, expires_at, entitlement_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                """,
                (
                    policy_snapshot_id,
                    *expected_policy,
                    _format_utc(snapshot.captured_at),
                ),
            )
        else:
            actual_policy = (
                str(existing["policy_id"]),
                int(existing["revision"]),
                str(existing["policy_hash"]),
                str(existing["provider"]),
                str(existing["dataset"]),
                str(existing["retention_mode"]),
                str(existing["verified_at"]),
            )
            if actual_policy != expected_policy:
                raise PlanIdentityCollisionError(
                    "policy snapshot ID collides with different semantic policy metadata"
                )

        provenance = connection.execute(
            "SELECT * FROM policy_snapshot_provenance WHERE policy_snapshot_id = ?",
            (policy_snapshot_id,),
        ).fetchone()
        expected_provenance = (
            snapshot.status.value,
            snapshot.verified_on.isoformat(),
        )
        if provenance is None:
            connection.execute(
                """
                INSERT INTO policy_snapshot_provenance(
                    policy_snapshot_id, policy_status, verified_on, recorded_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    policy_snapshot_id,
                    *expected_provenance,
                    _format_utc(recorded_at),
                ),
            )
        elif (
            str(provenance["policy_status"]),
            str(provenance["verified_on"]),
        ) != expected_provenance:
            raise PlanIdentityCollisionError("policy provenance collides with existing metadata")

        catalog = connection.execute(
            "SELECT * FROM policy_catalog_snapshots WHERE catalog_snapshot_id = ?",
            (catalog_snapshot_id,),
        ).fetchone()
        expected_catalog = (
            snapshot.catalog_id,
            snapshot.catalog_revision,
            snapshot.catalog_hash,
        )
        if catalog is None:
            connection.execute(
                """
                INSERT INTO policy_catalog_snapshots(
                    catalog_snapshot_id, catalog_id, catalog_revision,
                    catalog_hash, captured_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    catalog_snapshot_id,
                    *expected_catalog,
                    _format_utc(snapshot.captured_at),
                ),
            )
        elif (
            str(catalog["catalog_id"]),
            int(catalog["catalog_revision"]),
            str(catalog["catalog_hash"]),
        ) != expected_catalog:
            raise PlanIdentityCollisionError("catalog snapshot ID collides with existing metadata")

        status = connection.execute(
            "SELECT * FROM dataset_policy_status WHERE provider = ? AND dataset = ?",
            (snapshot.provider, snapshot.dataset),
        ).fetchone()
        if status is None:
            connection.execute(
                """
                INSERT INTO dataset_policy_status(
                    provider, dataset, status, retention_mode, policy_snapshot_id,
                    effective_at, expires_at, unavailable_at, last_checked_at
                ) VALUES (?, ?, 'ACTIVE', ?, ?, ?, NULL, NULL, ?)
                """,
                (
                    snapshot.provider,
                    snapshot.dataset,
                    snapshot.mode.value,
                    policy_snapshot_id,
                    _format_utc(snapshot.captured_at),
                    _format_utc(snapshot.captured_at),
                ),
            )
        elif (
            str(status["status"]),
            str(status["retention_mode"]),
            str(status["policy_snapshot_id"]),
            status["unavailable_at"],
        ) != ("ACTIVE", snapshot.mode.value, policy_snapshot_id, None):
            raise PlanPolicyMismatchError(
                "active dataset policy state does not match the planning snapshot"
            )

    @staticmethod
    def _persist_stream(
        connection: sqlite3.Connection,
        stream: StreamKey,
        recorded_at: datetime,
    ) -> None:
        dimensions_json = stream.canonical_json
        expected = (
            stream.stream_hash,
            stream.provider,
            stream.dataset,
            str(stream.instrument_id),
            stream.timeframe.value,
            stream.session.value,
            stream.adjustment.value,
            dimensions_json,
        )
        row = connection.execute(
            "SELECT * FROM stream_keys WHERE stream_id = ?",
            (stream.stream_id,),
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO stream_keys(
                    stream_id, stream_hash, provider, dataset, instrument_id,
                    timeframe, session, adjustment, dimensions_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (stream.stream_id, *expected, _format_utc(recorded_at)),
            )
            return
        actual = (
            str(row["stream_hash"]),
            str(row["provider"]),
            str(row["dataset"]),
            str(row["instrument_id"]),
            str(row["timeframe"]),
            str(row["session"]),
            str(row["adjustment"]),
            str(row["dimensions_json"]),
        )
        if actual != expected:
            raise PlanIdentityCollisionError(
                "stream ID collides with different immutable dimensions"
            )

    def _persist_planned_request(
        self,
        connection: sqlite3.Connection,
        request: PlanPersistenceRequest,
        planned: PlannedRequest,
        *,
        ordinal: int,
        execution_limit: tuple[int, int],
        policy_snapshot_id: str,
        recorded_at: datetime,
    ) -> None:
        specification = planned.specification
        self._persist_request_spec(connection, specification, recorded_at)
        identity = deterministic_request_instance_identity(
            request.run_id,
            ordinal,
            specification,
        )
        request_instance_id = str(identity.request_instance_id)
        connection.execute(
            """
            INSERT INTO request_instances(
                request_instance_id, run_id, request_spec_id, intent,
                reason, plan_ordinal, status, created_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'PLANNED', ?, NULL)
            """,
            (
                request_instance_id,
                str(request.run_id),
                specification.request_spec_id,
                request.plan.intent.value,
                request.reason,
                ordinal,
                _format_utc(recorded_at),
            ),
        )
        max_pages, max_calls = execution_limit
        connection.execute(
            """
            INSERT INTO request_execution_limits(
                request_instance_id, run_id, max_pages, max_calls, limits_hash
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                request_instance_id,
                str(request.run_id),
                max_pages,
                max_calls,
                _request_execution_limits_hash(
                    request.run_id,
                    identity.request_instance_id,
                    max_pages=max_pages,
                    max_calls=max_calls,
                ),
            ),
        )
        connection.execute(
            """
            INSERT INTO request_plan_estimates(
                request_instance_id, policy_snapshot_id, calendar_snapshot_id,
                expected_slot_count, expected_observation_count,
                estimated_pages, estimated_calls, estimated_bytes, estimated_cost,
                first_slot_start, last_slot_end,
                authorization_eligible_before, authorized_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._request_estimate_values(
                request_instance_id,
                request.calendar_snapshot_id,
                policy_snapshot_id,
                planned,
            ),
        )
        connection.execute(
            """
            INSERT INTO retry_state(
                request_instance_id, retry_count, max_attempts,
                next_eligible_at, last_error_id, updated_at
            ) VALUES (?, 0, ?, NULL, NULL, ?)
            """,
            (
                request_instance_id,
                request.max_attempts,
                _format_utc(recorded_at),
            ),
        )

    @staticmethod
    def _persist_request_spec(
        connection: sqlite3.Connection,
        specification: RequestSpecification,
        recorded_at: datetime,
    ) -> None:
        expected = (
            specification.request_spec_hash,
            specification.provider,
            specification.dataset,
            _format_utc(specification.start),
            _format_utc(specification.end),
            specification.mapping_semantic_version,
            specification.canonical_json,
        )
        row = connection.execute(
            "SELECT * FROM request_specs WHERE request_spec_id = ?",
            (specification.request_spec_id,),
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO request_specs(
                    request_spec_id, request_spec_hash, provider, dataset,
                    interval_start, interval_end, mapping_version,
                    specification_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    specification.request_spec_id,
                    *expected,
                    _format_utc(recorded_at),
                ),
            )
            connection.executemany(
                """
                INSERT INTO request_spec_streams(
                    request_spec_id, stream_id, provider_identifier, ordinal
                ) VALUES (?, ?, ?, ?)
                """,
                tuple(
                    (
                        specification.request_spec_id,
                        stream.stream_id,
                        mapping.provider_identifier,
                        ordinal,
                    )
                    for ordinal, (stream, mapping) in enumerate(
                        zip(
                            specification.stream_keys(),
                            specification.instrument_mappings,
                            strict=True,
                        )
                    )
                ),
            )
            return
        actual = (
            str(row["request_spec_hash"]),
            str(row["provider"]),
            str(row["dataset"]),
            str(row["interval_start"]),
            str(row["interval_end"]),
            str(row["mapping_version"]),
            str(row["specification_json"]),
        )
        if actual != expected:
            raise PlanIdentityCollisionError(
                "request specification ID collides with different canonical metadata"
            )
        mappings = connection.execute(
            """
            SELECT stream_id, provider_identifier, ordinal
            FROM request_spec_streams WHERE request_spec_id = ? ORDER BY ordinal
            """,
            (specification.request_spec_id,),
        ).fetchall()
        expected_mappings = tuple(
            (stream.stream_id, mapping.provider_identifier, ordinal)
            for ordinal, (stream, mapping) in enumerate(
                zip(specification.stream_keys(), specification.instrument_mappings, strict=True)
            )
        )
        if (
            tuple(
                (str(value["stream_id"]), str(value["provider_identifier"]), int(value["ordinal"]))
                for value in mappings
            )
            != expected_mappings
        ):
            raise PlanIdentityCollisionError("request-to-stream mapping collides with plan")

    def _validate_replay(
        self,
        connection: sqlite3.Connection,
        request: PlanPersistenceRequest,
        *,
        plan_hash: str,
        policy_snapshot_id: str,
        catalog_snapshot_id: str,
    ) -> None:
        row = self._load_plan_row(connection, str(request.run_id))
        expected_record = self._plan_record_immutable_values(
            request,
            plan_hash=plan_hash,
            policy_snapshot_id=policy_snapshot_id,
            catalog_snapshot_id=catalog_snapshot_id,
        )
        actual_record = tuple(row[name] for name in _PLAN_IMMUTABLE_COLUMNS)
        if actual_record != expected_record:
            raise PlanIdentityCollisionError("run ID collides with a different ingestion plan")
        run_expected = (
            request.plan.intent.value,
            request.plan.environment.value,
            request.plan.provider,
            request.plan.dataset,
            policy_snapshot_id,
            len(request.plan.requests),
        )
        run_actual = (
            str(row["mode"]),
            str(row["environment"]),
            str(row["provider"]),
            str(row["dataset"]),
            str(row["run_policy_snapshot_id"]),
            int(row["planned_request_count"]),
        )
        if run_actual != run_expected:
            raise PlanIdentityCollisionError("run row collides with immutable plan scope")
        limits = connection.execute(
            "SELECT * FROM ingestion_execution_limits WHERE run_id = ?",
            (str(request.run_id),),
        ).fetchone()
        expected_limits = (
            request.max_pages,
            request.max_calls,
            request.max_pages_per_request,
            request.max_calls_per_request,
            _execution_limits_hash(request),
        )
        if limits is None or tuple(
            limits[column]
            for column in (
                "max_pages",
                "max_calls",
                "max_pages_per_request",
                "max_calls_per_request",
                "limits_hash",
            )
        ) != expected_limits:
            raise PlanIdentityCollisionError("request execution limits collide with replay")

        streams = connection.execute(
            """
            SELECT stream_id, ordinal FROM ingestion_plan_streams
            WHERE run_id = ? ORDER BY ordinal
            """,
            (str(request.run_id),),
        ).fetchall()
        if tuple((str(row["stream_id"]), int(row["ordinal"])) for row in streams) != tuple(
            (stream.stream_id, ordinal) for ordinal, stream in enumerate(request.plan.streams)
        ):
            raise PlanIdentityCollisionError("persisted plan stream order collides with replay")

        instances = connection.execute(
            """
            SELECT * FROM request_instances WHERE run_id = ? ORDER BY plan_ordinal
            """,
            (str(request.run_id),),
        ).fetchall()
        if len(instances) != len(request.plan.requests):
            raise PlanIdentityCollisionError("persisted request count collides with replay")
        allocated_limits = _allocate_request_execution_limits(request)
        for ordinal, (instance, planned) in enumerate(
            zip(instances, request.plan.requests, strict=True)
        ):
            identity = deterministic_request_instance_identity(
                request.run_id,
                ordinal,
                planned.specification,
            )
            expected_instance = (
                str(identity.request_instance_id),
                planned.specification.request_spec_id,
                request.plan.intent.value,
                request.reason,
                ordinal,
            )
            actual_instance = (
                str(instance["request_instance_id"]),
                str(instance["request_spec_id"]),
                str(instance["intent"]),
                str(instance["reason"]),
                int(instance["plan_ordinal"]),
            )
            if actual_instance != expected_instance:
                raise PlanIdentityCollisionError("request instance collides with replay")
            request_limit = connection.execute(
                "SELECT * FROM request_execution_limits WHERE request_instance_id = ?",
                (str(identity.request_instance_id),),
            ).fetchone()
            max_pages, max_calls = allocated_limits[ordinal]
            expected_request_limit = (
                str(request.run_id),
                max_pages,
                max_calls,
                _request_execution_limits_hash(
                    request.run_id,
                    identity.request_instance_id,
                    max_pages=max_pages,
                    max_calls=max_calls,
                ),
            )
            if request_limit is None or tuple(
                request_limit[column]
                for column in ("run_id", "max_pages", "max_calls", "limits_hash")
            ) != expected_request_limit:
                raise PlanIdentityCollisionError("request dispatch limits collide with replay")
            self._persist_request_spec(
                connection,
                planned.specification,
                planned.authorization.authorized_at,
            )
            estimate = connection.execute(
                "SELECT * FROM request_plan_estimates WHERE request_instance_id = ?",
                (str(identity.request_instance_id),),
            ).fetchone()
            if estimate is None or tuple(
                estimate[name] for name in _REQUEST_ESTIMATE_COLUMNS
            ) != self._request_estimate_values(
                str(identity.request_instance_id),
                request.calendar_snapshot_id,
                policy_snapshot_id,
                planned,
            ):
                raise PlanIdentityCollisionError("request estimate collides with replay")
            retry = connection.execute(
                "SELECT max_attempts FROM retry_state WHERE request_instance_id = ?",
                (str(identity.request_instance_id),),
            ).fetchone()
            if retry is None or int(retry["max_attempts"]) != request.max_attempts:
                raise PlanIdentityCollisionError("request retry policy collides with replay")

    @staticmethod
    def _load_plan_row(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT p.*, r.mode, r.environment, r.provider, r.dataset, r.status,
                   r.policy_snapshot_id AS run_policy_snapshot_id,
                   r.planned_request_count
            FROM ingestion_plan_records AS p
            JOIN ingestion_runs AS r ON r.run_id = p.run_id
            WHERE p.run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if row is None:
            raise PlanIdentityCollisionError("run exists without complete plan provenance")
        return cast(sqlite3.Row, row)

    @staticmethod
    def _plan_record_values(
        request: PlanPersistenceRequest,
        *,
        plan_hash: str,
        policy_snapshot_id: str,
        catalog_snapshot_id: str,
        lease: WriterLease,
        recorded_at: datetime,
    ) -> tuple[object, ...]:
        return (
            str(request.run_id),
            *IngestionPlanRepository._plan_record_immutable_values(
                request,
                plan_hash=plan_hash,
                policy_snapshot_id=policy_snapshot_id,
                catalog_snapshot_id=catalog_snapshot_id,
            ),
            lease.owner_id,
            lease.generation,
            _format_utc(recorded_at),
        )

    @staticmethod
    def _plan_record_immutable_values(
        request: PlanPersistenceRequest,
        *,
        plan_hash: str,
        policy_snapshot_id: str,
        catalog_snapshot_id: str,
    ) -> tuple[object, ...]:
        plan = request.plan
        return (
            plan_hash,
            PLANNER_PERSISTENCE_CONTRACT_VERSION,
            request.calendar_snapshot_id,
            plan.calendar_snapshot_checksum,
            policy_snapshot_id,
            catalog_snapshot_id,
            request.reason,
            plan.acquisition_strategy.value,
            plan.repair_strategy.value if plan.repair_strategy is not None else None,
            plan.repair_reason,
            request.max_attempts,
            _format_utc(plan.desired_start),
            _format_utc(plan.desired_end),
            _format_utc(plan.safe_end),
            _format_utc(plan.policy_authorization.authorized_at),
            plan.eligible_slot_count,
            plan.eligible_observation_count,
            plan.missing_observation_count,
            plan.pending_observation_count,
            plan.estimated_pages,
            plan.estimated_calls,
            plan.estimated_bytes,
            _canonical_decimal(plan.estimated_cost),
        )

    @staticmethod
    def _request_estimate_values(
        request_instance_id: str,
        calendar_snapshot_id: str,
        policy_snapshot_id: str,
        planned: PlannedRequest,
    ) -> tuple[object, ...]:
        return (
            request_instance_id,
            policy_snapshot_id,
            calendar_snapshot_id,
            len(planned.expected_slots),
            planned.expected_observations,
            planned.estimated_pages,
            planned.estimated_calls,
            planned.estimated_bytes,
            _canonical_decimal(planned.estimated_cost),
            _format_utc(planned.expected_slots[0].start_utc),
            _format_utc(planned.expected_slots[-1].end_utc),
            _format_utc(planned.authorization.eligible_before),
            _format_utc(planned.authorization.authorized_at),
        )

    @staticmethod
    def _persisted_result(row: sqlite3.Row, *, replayed: bool) -> PersistedIngestionPlan:
        count = int(row["planned_request_count"])
        return PersistedIngestionPlan(
            run_id=UUID(str(row["run_id"])),
            plan_hash=str(row["plan_hash"]),
            policy_snapshot_id=str(row["policy_snapshot_id"]),
            catalog_snapshot_id=str(row["catalog_snapshot_id"]),
            calendar_snapshot_id=str(row["calendar_snapshot_id"]),
            status=IngestionRunStatus(str(row["status"])),
            request_count=count,
            no_op=count == 0,
            recorded_at=_parse_utc(str(row["recorded_at"])),
            replayed=replayed,
        )

    @staticmethod
    def _request_progress(row: sqlite3.Row) -> RequestProgress:
        return RequestProgress(
            request_instance_id=UUID(str(row["request_instance_id"])),
            request_spec_id=str(row["request_spec_id"]),
            request_spec_hash=str(row["request_spec_hash"]),
            plan_ordinal=int(row["plan_ordinal"]),
            status=RequestInstanceStatus(str(row["status"])),
            interval_start=_parse_utc(str(row["interval_start"])),
            interval_end=_parse_utc(str(row["interval_end"])),
            attempt_count=int(row["attempt_count"]),
            latest_attempt_status=(
                str(row["latest_attempt_status"])
                if row["latest_attempt_status"] is not None
                else None
            ),
            next_eligible_at=(
                _parse_utc(str(row["next_eligible_at"]))
                if row["next_eligible_at"] is not None
                else None
            ),
        )


_PLAN_IMMUTABLE_COLUMNS = (
    "plan_hash",
    "planner_contract_version",
    "calendar_snapshot_id",
    "calendar_snapshot_checksum",
    "policy_snapshot_id",
    "catalog_snapshot_id",
    "reason",
    "acquisition_strategy",
    "repair_strategy",
    "repair_reason",
    "max_attempts",
    "desired_start",
    "desired_end",
    "safe_end",
    "authorized_at",
    "eligible_slot_count",
    "eligible_observation_count",
    "missing_observation_count",
    "pending_observation_count",
    "estimated_pages",
    "estimated_calls",
    "estimated_bytes",
    "estimated_cost",
)

_REQUEST_ESTIMATE_COLUMNS = (
    "request_instance_id",
    "policy_snapshot_id",
    "calendar_snapshot_id",
    "expected_slot_count",
    "expected_observation_count",
    "estimated_pages",
    "estimated_calls",
    "estimated_bytes",
    "estimated_cost",
    "first_slot_start",
    "last_slot_end",
    "authorization_eligible_before",
    "authorized_at",
)


def _plan_hash(request: PlanPersistenceRequest) -> str:
    plan = request.plan
    snapshot = plan.policy_authorization.policy_snapshot
    payload = {
        "calendar_snapshot_id": request.calendar_snapshot_id,
        "calendar_snapshot_checksum": plan.calendar_snapshot_checksum,
        "catalog_snapshot_id": deterministic_catalog_snapshot_id(snapshot),
        "contract_version": PLANNER_PERSISTENCE_CONTRACT_VERSION,
        "dataset": plan.dataset,
        "desired_end": _format_utc(plan.desired_end),
        "desired_start": _format_utc(plan.desired_start),
        "environment": plan.environment.value,
        "intent": plan.intent.value,
        "acquisition_strategy": plan.acquisition_strategy.value,
        "repair_strategy": (
            plan.repair_strategy.value if plan.repair_strategy is not None else None
        ),
        "repair_reason": plan.repair_reason,
        "max_attempts": request.max_attempts,
        "max_pages": request.max_pages,
        "max_calls": request.max_calls,
        "max_pages_per_request": request.max_pages_per_request,
        "max_calls_per_request": request.max_calls_per_request,
        "policy_snapshot_id": deterministic_policy_snapshot_id(snapshot),
        "planning_authorized_at": _format_utc(plan.policy_authorization.authorized_at),
        "provider": plan.provider,
        "reason": request.reason,
        "requests": [
            {
                "authorized_at": _format_utc(value.authorization.authorized_at),
                "eligible_before": _format_utc(value.authorization.eligible_before),
                "estimated_bytes": value.estimated_bytes,
                "estimated_calls": value.estimated_calls,
                "estimated_cost": _canonical_decimal(value.estimated_cost),
                "estimated_pages": value.estimated_pages,
                "expected_observations": value.expected_observations,
                "expected_slots": [
                    {
                        "end": _format_utc(slot.end_utc),
                        "session_date": slot.session_date.isoformat(),
                        "start": _format_utc(slot.start_utc),
                        "timeframe": slot.timeframe.value,
                    }
                    for slot in value.expected_slots
                ],
                "specification": value.specification.canonical_json,
            }
            for value in plan.requests
        ],
        "safe_end": _format_utc(plan.safe_end),
        "streams": [stream.canonical_json for stream in plan.streams],
        "totals": {
            "eligible_observations": plan.eligible_observation_count,
            "eligible_slots": plan.eligible_slot_count,
            "estimated_bytes": plan.estimated_bytes,
            "estimated_calls": plan.estimated_calls,
            "estimated_cost": _canonical_decimal(plan.estimated_cost),
            "estimated_pages": plan.estimated_pages,
            "missing_observations": plan.missing_observation_count,
            "pending_observations": plan.pending_observation_count,
        },
    }
    return _hash_json(payload)


def _hash_json(value: object) -> str:
    canonical = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _execution_limits_hash(request: PlanPersistenceRequest) -> str:
    return _hash_json(
        {
            "max_calls": request.max_calls,
            "max_calls_per_request": request.max_calls_per_request,
            "max_pages": request.max_pages,
            "max_pages_per_request": request.max_pages_per_request,
            "run_id": str(request.run_id),
            "version": 1,
        }
    )


def _request_execution_limits_hash(
    run_id: UUID,
    request_instance_id: UUID,
    *,
    max_pages: int,
    max_calls: int,
) -> str:
    return _hash_json(
        {
            "max_calls": max_calls,
            "max_pages": max_pages,
            "request_instance_id": str(request_instance_id),
            "run_id": str(run_id),
            "version": 1,
        }
    )


def _allocate_request_execution_limits(
    request: PlanPersistenceRequest,
) -> tuple[tuple[int, int], ...]:
    """Allocate bounded headroom without multiplying the run ceilings.

    The deterministic estimate is the minimum allocation.  Any remaining run
    headroom is distributed in plan order one dispatch at a time, capped by the
    per-request ceiling.  Consequently a short provider page can continue, while
    the sum of every request's possible dispatches never exceeds the caller's
    durable run budget.
    """

    page_limits = _allocate_one_execution_limit(
        tuple(planned.estimated_pages for planned in request.plan.requests),
        run_ceiling=request.max_pages,
        per_request_ceiling=request.max_pages_per_request,
    )
    call_limits = _allocate_one_execution_limit(
        tuple(planned.estimated_calls for planned in request.plan.requests),
        run_ceiling=request.max_calls,
        per_request_ceiling=request.max_calls_per_request,
    )
    return tuple(zip(page_limits, call_limits, strict=True))


def _allocate_one_execution_limit(
    estimates: tuple[int, ...],
    *,
    run_ceiling: int,
    per_request_ceiling: int,
) -> tuple[int, ...]:
    if not estimates:
        return ()
    if any(estimate <= 0 or estimate > per_request_ceiling for estimate in estimates):
        raise ValueError("request estimate exceeds its durable dispatch ceiling")
    remaining = run_ceiling - sum(estimates)
    if remaining < 0:
        raise ValueError("request estimates exceed the durable run dispatch ceiling")
    allocated = list(estimates)
    while remaining:
        progressed = False
        for ordinal, current in enumerate(allocated):
            if current >= per_request_ceiling:
                continue
            allocated[ordinal] += 1
            remaining -= 1
            progressed = True
            if remaining == 0:
                break
        if not progressed:
            break
    return tuple(allocated)


def _ordered_raw_artifact_ids_hash(artifact_ids: tuple[str, ...]) -> str:
    """Recompute the immutable batch-context page sequence commitment."""

    return _hash_json(
        {
            "canonicalization_version": IDENTITY_CANONICALIZATION_VERSION,
            "kind": "ordered-raw-artifacts",
            "payload": {"artifact_ids": list(artifact_ids)},
        }
    )


def _is_sensitive_text(value: str) -> bool:
    return "\r" in value or "\n" in value or _SECRET_TEXT.search(value) is not None


def _canonical_decimal(value: Decimal) -> str:
    if value.is_zero():
        return "0"
    return format(value.normalize(), "f")


__all__ = [
    "PLANNER_PERSISTENCE_CONTRACT_VERSION",
    "CoverageReadResult",
    "IngestionPlanRepository",
    "IngestionRunProgress",
    "IngestionRunStatus",
    "PersistedIngestionPlan",
    "PlanCalendarMismatchError",
    "PlanIdentityCollisionError",
    "PlanNotFoundError",
    "PlanPersistenceRequest",
    "PlanPolicyMismatchError",
    "PlanRepositoryError",
    "RequestInstanceStatus",
    "RequestProgress",
    "RequestResumeAction",
    "RequestStateConflictError",
    "deterministic_catalog_snapshot_id",
    "deterministic_policy_snapshot_id",
    "deterministic_request_instance_identity",
]
