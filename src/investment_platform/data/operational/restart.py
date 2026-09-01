"""Read-only, fail-closed restart projections for Phase 2 ingestion.

The writer repositories deliberately persist normalized relational facts.  This
module is the inverse boundary used after a process restart: it reconstructs
validated domain models, checks their content identities and proof graph, and
returns only the state required to make the next recovery decision.  It never
mutates SQLite and never returns provider error messages or request secrets.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from itertools import pairwise
from typing import Annotated, Any, Self, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from investment_platform.data.calendar import (
    CalendarSession,
    CalendarSnapshot,
    ExpectedCalendarSlot,
)
from investment_platform.data.ingestion.coverage import (
    CoverageRequestTerminalState,
    CoverageSegment,
    CoverageStreamOutcome,
    GapFinding,
    GapStatus,
    GapType,
    MaterializedWatermark,
)
from investment_platform.data.ingestion.identity import (
    IDENTITY_CANONICALIZATION_VERSION,
    RawArtifactIdentity,
    RequestSpecification,
    StreamKey,
)
from investment_platform.data.ingestion.planner import (
    AcquisitionStrategy,
    CoverageClassification,
    CoverageVerificationState,
    IngestionIntent,
    IngestionPlan,
    PlannedRequest,
    RepairStrategy,
)
from investment_platform.data.operational.execution import SemanticNoOpObservationProof
from investment_platform.data.operational.planning import (
    PLANNER_PERSISTENCE_CONTRACT_VERSION,
    IngestionRunStatus,
    PlanPersistenceRequest,
    RequestInstanceStatus,
    _allocate_request_execution_limits,
    _execution_limits_hash,
    _plan_hash,
    _request_execution_limits_hash,
    deterministic_catalog_snapshot_id,
    deterministic_policy_snapshot_id,
    deterministic_request_instance_identity,
)
from investment_platform.data.operational.repository import deterministic_calendar_snapshot_id
from investment_platform.data.operational.store import (
    OperationalStateError,
    OperationalStateStore,
    _format_utc,
    _parse_utc,
)
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    DatasetPolicySnapshot,
    DatasetPolicyStatus,
    PlanningPolicyAuthorization,
    RequestPolicyAuthorization,
    RetentionMode,
)
from investment_platform.data.storage.canonical_batches import CanonicalBatchExpectation
from investment_platform.runtime import RuntimeEnvironment

_DURABLE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,191}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_PLATFORM_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_TERMINAL_REQUESTS = frozenset(
    {
        RequestInstanceStatus.SUCCESS,
        RequestInstanceStatus.PARTIAL,
        RequestInstanceStatus.FAILED,
        RequestInstanceStatus.BLOCKED,
        RequestInstanceStatus.CANCELLED,
    }
)
_TERMINAL_RUNS = frozenset(
    {
        IngestionRunStatus.SUCCESS,
        IngestionRunStatus.PARTIAL,
        IngestionRunStatus.FAILED,
        IngestionRunStatus.CANCELLED,
    }
)


class RestartProjectionError(OperationalStateError):
    """Base error for a sanitized restart-state read."""


class RestartRunNotFoundError(RestartProjectionError):
    """Raised when the requested durable run is absent."""


class RestartProjectionIntegrityError(RestartProjectionError):
    """Raised when persisted restart metadata cannot prove its own identity."""


class RestartAction(StrEnum):
    """Safe next action derived only from committed durable state."""

    DISPATCH = "DISPATCH"
    RESUME_ACQUISITION = "RESUME_ACQUISITION"
    REPLAY_RAW = "REPLAY_RAW"
    RESUME_PROCESSING = "RESUME_PROCESSING"
    ADOPT_PUBLICATION = "ADOPT_PUBLICATION"
    WAIT_RETRY = "WAIT_RETRY"
    RETRY_DISPATCH = "RETRY_DISPATCH"
    RECONCILE_RUN = "RECONCILE_RUN"
    POLICY_BLOCKED = "POLICY_BLOCKED"
    CALENDAR_BLOCKED = "CALENDAR_BLOCKED"
    NONE = "NONE"


class AttemptStatus(StrEnum):
    PLANNED = "PLANNED"
    RUNNING = "RUNNING"
    RAW_COMPLETE = "RAW_COMPLETE"
    SUCCESS = "SUCCESS"
    RETRYABLE_FAILED = "RETRYABLE_FAILED"
    FATAL_FAILED = "FATAL_FAILED"
    ABORTED = "ABORTED"


class PublicationRecoveryState(StrEnum):
    CONTEXT_ONLY = "CONTEXT_ONLY"
    PREPARED = "PREPARED"
    CATALOGED = "CATALOGED"
    ABANDONED = "ABANDONED"


class _FrozenRestartModel(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class RestartAttemptProjection(_FrozenRestartModel):
    attempt_id: UUID
    request_instance_id: UUID
    attempt_number: Annotated[int, Field(gt=0)]
    status: AttemptStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    next_eligible_at: datetime | None = None
    page_count: Annotated[int, Field(ge=0)]
    pagination_complete: bool
    terminal_page_verified: bool
    request_authorization: RequestPolicyAuthorization
    acquisition_authorization: AcquisitionPolicyAuthorization | None = None
    ordered_artifact_ids: tuple[str, ...] = ()

    @field_validator("started_at", "completed_at", "next_eligible_at", mode="after")
    @classmethod
    def normalize_times(cls, value: datetime | None) -> datetime | None:
        return None if value is None else value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_completion(self) -> Self:
        has_acquisition = self.acquisition_authorization is not None
        if has_acquisition != bool(self.ordered_artifact_ids):
            raise ValueError("acquisition proof and artifact order must be present together")
        if has_acquisition and not (
            self.pagination_complete
            and self.terminal_page_verified
            and self.page_count == len(self.ordered_artifact_ids)
        ):
            raise ValueError("completed acquisition flags do not match its artifacts")
        return self


class RestartPublicationProjection(_FrozenRestartModel):
    batch_context_id: str = Field(pattern=r"^batch_context_v1_[0-9a-f]{64}$")
    canonical_batch_id: str = Field(pattern=r"^batch_v1_[0-9a-f]{64}$")
    state: PublicationRecoveryState
    expectation: CanonicalBatchExpectation | None = None
    prepared_at: datetime | None = None
    publication_committed: bool = False

    @field_validator("prepared_at", mode="after")
    @classmethod
    def normalize_prepared_at(cls, value: datetime | None) -> datetime | None:
        return None if value is None else value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if self.state is PublicationRecoveryState.CONTEXT_ONLY:
            if self.expectation is not None or self.prepared_at is not None:
                raise ValueError("context-only state cannot contain a publication expectation")
        elif self.expectation is None or self.prepared_at is None:
            raise ValueError("publication state requires its durable expectation")
        if self.publication_committed != (self.state is PublicationRecoveryState.CATALOGED):
            raise ValueError("publication commit presence disagrees with expectation state")
        return self


class RestartRequestProjection(_FrozenRestartModel):
    request_instance_id: UUID
    plan_ordinal: Annotated[int, Field(ge=0)]
    status: RequestInstanceStatus
    specification: RequestSpecification
    authorization: RequestPolicyAuthorization
    expected_slot_count: Annotated[int, Field(gt=0)]
    expected_observation_count: Annotated[int, Field(gt=0)]
    max_pages: Annotated[int, Field(gt=0, le=1000)]
    max_calls: Annotated[int, Field(gt=0, le=1000)]
    max_attempts: Annotated[int, Field(gt=0)]
    retry_count: Annotated[int, Field(ge=0)]
    next_eligible_at: datetime | None = None
    latest_attempt: RestartAttemptProjection | None = None
    publication: RestartPublicationProjection | None = None
    semantic_noop_committed: bool = False
    nonpublication_partial_committed: bool = False
    action: RestartAction

    @field_validator("next_eligible_at", mode="after")
    @classmethod
    def normalize_next_eligible_at(cls, value: datetime | None) -> datetime | None:
        return None if value is None else value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_retry_state(self) -> Self:
        if self.retry_count > self.max_attempts:
            raise ValueError("retry count exceeds the durable attempt limit")
        if (self.status is RequestInstanceStatus.RETRY_WAIT) != (self.next_eligible_at is not None):
            raise ValueError("retry-wait state and eligibility timestamp disagree")
        return self


class RestartRunContext(_FrozenRestartModel):
    run_id: UUID
    status: IngestionRunStatus
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    calendar_snapshot_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,191}$")
    policy_snapshot_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,191}$")
    catalog_snapshot_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,191}$")
    calendar_current: bool
    policy_current: bool
    plan: IngestionPlan
    requests: tuple[RestartRequestProjection, ...]

    @model_validator(mode="after")
    def validate_request_order(self) -> Self:
        if tuple(request.plan_ordinal for request in self.requests) != tuple(
            range(len(self.requests))
        ):
            raise ValueError("restart requests are not in complete plan order")
        if tuple(request.specification for request in self.requests) != tuple(
            request.specification for request in self.plan.requests
        ):
            raise ValueError("restart requests disagree with the reconstructed plan")
        return self


class StreamProofProjection(_FrozenRestartModel):
    """Proof-backed durable frontier inputs for exact selected streams."""

    stream_ids: tuple[str, ...]
    coverage: tuple[CoverageSegment, ...]
    gaps: tuple[GapFinding, ...]
    watermarks: tuple[MaterializedWatermark, ...]


def _canonical_json(value: object) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _hash_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _strict_json(value: str) -> Any:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate JSON member")
            result[key] = item
        return result

    def reject_constant(_: str) -> None:
        raise ValueError("non-finite JSON number")

    return json.loads(value, object_pairs_hook=object_pairs, parse_constant=reject_constant)


def _identity_payload(value: str, *, kind: str) -> dict[str, Any]:
    parsed = _strict_json(value)
    if not isinstance(parsed, dict) or set(parsed) != {
        "canonicalization_version",
        "kind",
        "payload",
    }:
        raise ValueError("identity envelope is invalid")
    if (
        parsed["canonicalization_version"] != IDENTITY_CANONICALIZATION_VERSION
        or parsed["kind"] != kind
        or not isinstance(parsed["payload"], dict)
    ):
        raise ValueError("identity envelope is inconsistent")
    return cast(dict[str, Any], parsed["payload"])


def _parse_specification(value: str) -> RequestSpecification:
    specification = RequestSpecification.model_validate(
        _identity_payload(value, kind="request-specification")
    )
    if specification.canonical_json != value:
        raise ValueError("request specification is not canonical")
    return specification


def _parse_stream(value: str) -> StreamKey:
    stream = StreamKey.model_validate(_identity_payload(value, kind="stream-key"))
    if stream.canonical_json != value:
        raise ValueError("stream key is not canonical")
    return stream


def _parse_model_json[T: BaseModel](model: type[T], value: str) -> T:
    parsed = _strict_json(value)
    instance = model.model_validate(parsed)
    if _canonical_json(instance) != value:
        raise ValueError("stored model JSON is not canonical")
    return instance


def _parse_optional_utc(value: object) -> datetime | None:
    return None if value is None else _parse_utc(str(value))


def _as_bool(value: object) -> bool:
    if value not in (0, 1):
        raise ValueError("stored boolean is invalid")
    return bool(value)


def _validate_sha256(value: object, *, platform: bool = False) -> str:
    rendered = str(value)
    pattern = _PLATFORM_SHA256 if platform else _SHA256
    if pattern.fullmatch(rendered) is None:
        raise ValueError("stored digest is invalid")
    return rendered


class RestartProjectionReader:
    """Reconstruct restart and coverage state through independent read-only snapshots."""

    def __init__(self, store: OperationalStateStore) -> None:
        self._store = store

    def load_run(self, run_id: UUID) -> RestartRunContext:
        """Load one complete plan/run projection or fail without partial output."""

        try:
            with self._store.read_only_connection() as connection:
                connection.execute("PRAGMA query_only = ON")
                connection.execute("BEGIN")
                try:
                    result = self._load_run(connection, run_id)
                finally:
                    connection.rollback()
            return result
        except RestartRunNotFoundError:
            raise
        except (RestartProjectionIntegrityError, sqlite3.DatabaseError):
            raise RestartProjectionIntegrityError(
                "durable restart state failed its integrity checks"
            ) from None
        except (KeyError, TypeError, ValueError, ValidationError, InvalidOperation):
            raise RestartProjectionIntegrityError(
                "durable restart state contains invalid typed metadata"
            ) from None

    def load_stream_proofs(self, stream_ids: Sequence[str]) -> StreamProofProjection:
        """Load exact coverage/gap/watermark facts only after proving their graph."""

        selected = tuple(stream_ids)
        if (
            not selected
            or selected != tuple(sorted(set(selected)))
            or any(_DURABLE_ID.fullmatch(value) is None for value in selected)
        ):
            raise ValueError("stream selection must be non-empty, unique, and ordered")
        try:
            with self._store.read_only_connection() as connection:
                connection.execute("PRAGMA query_only = ON")
                connection.execute("BEGIN")
                try:
                    result = self._load_stream_proofs(connection, selected)
                finally:
                    connection.rollback()
            return result
        except (RestartProjectionIntegrityError, sqlite3.DatabaseError):
            raise RestartProjectionIntegrityError(
                "durable stream proofs failed their integrity checks"
            ) from None

    def load_optional_stream_proofs(
        self,
        stream_ids: Sequence[str],
    ) -> StreamProofProjection | None:
        """Load selected proofs, returning ``None`` only when no stream is cataloged.

        A partially cataloged selection remains an integrity error.  This narrow
        bootstrap API lets a first bounded backfill build its calendar before the
        deterministic plan catalogs the stream keys without weakening normal
        restart proof validation.
        """

        selected = tuple(stream_ids)
        if (
            not selected
            or selected != tuple(sorted(set(selected)))
            or any(_DURABLE_ID.fullmatch(value) is None for value in selected)
        ):
            raise ValueError("stream selection must be non-empty, unique, and ordered")
        try:
            with self._store.read_only_connection() as connection:
                connection.execute("PRAGMA query_only = ON")
                connection.execute("BEGIN")
                try:
                    placeholders = ",".join("?" for _ in selected)
                    cataloged = tuple(
                        str(row[0])
                        for row in connection.execute(
                            f"""
                            SELECT stream_id FROM stream_keys
                            WHERE stream_id IN ({placeholders}) ORDER BY stream_id
                            """,
                            selected,
                        ).fetchall()
                    )
                    if not cataloged:
                        result = None
                    elif cataloged != selected:
                        raise RestartProjectionIntegrityError(
                            "selected streams are only partially cataloged"
                        )
                    else:
                        result = self._load_stream_proofs(connection, selected)
                finally:
                    connection.rollback()
            return result
        except (RestartProjectionIntegrityError, sqlite3.DatabaseError):
            raise RestartProjectionIntegrityError(
                "durable stream proofs failed their integrity checks"
            ) from None
        except (KeyError, TypeError, ValueError, ValidationError):
            raise RestartProjectionIntegrityError(
                "durable stream proofs contain invalid typed metadata"
            ) from None

    def _load_run(self, connection: sqlite3.Connection, run_id: UUID) -> RestartRunContext:
        evaluated_at = self._store._now()
        row = connection.execute(
            """
            SELECT run.*, plan.*,
                   limits.max_pages,
                   limits.max_calls,
                   limits.max_pages_per_request,
                   limits.max_calls_per_request,
                   limits.limits_hash,
                   run.policy_snapshot_id AS run_policy_snapshot_id,
                   run.status AS run_status
            FROM ingestion_runs AS run
            JOIN ingestion_plan_records AS plan ON plan.run_id = run.run_id
            JOIN ingestion_execution_limits AS limits ON limits.run_id = run.run_id
            WHERE run.run_id = ?
            """,
            (str(run_id),),
        ).fetchone()
        if row is None:
            raise RestartRunNotFoundError("ingestion run is not cataloged")
        if int(row["planner_contract_version"]) != PLANNER_PERSISTENCE_CONTRACT_VERSION:
            raise RestartProjectionIntegrityError("planner contract version is unsupported")

        run_status = IngestionRunStatus(str(row["run_status"]))
        calendar, calendar_current = self._load_calendar(
            connection,
            str(row["calendar_snapshot_id"]),
        )
        if calendar.checksum != str(row["calendar_snapshot_checksum"]):
            raise RestartProjectionIntegrityError("calendar checksum does not match the plan")
        policy, policy_current = self._load_policy(
            connection,
            policy_snapshot_id=str(row["policy_snapshot_id"]),
            catalog_snapshot_id=str(row["catalog_snapshot_id"]),
        )
        if (
            str(row["run_policy_snapshot_id"]) != deterministic_policy_snapshot_id(policy)
            or str(row["provider"]) != policy.provider
            or str(row["dataset"]) != policy.dataset
        ):
            raise RestartProjectionIntegrityError("run policy scope is inconsistent")

        environment = RuntimeEnvironment(str(row["environment"]))
        authorized_at = _parse_utc(str(row["authorized_at"]))
        # ``policy_snapshot_id`` identifies the semantic policy revision and is
        # intentionally reused across runs.  The authorization snapshot carried
        # by a reconstructed plan is nevertheless captured at that run's exact
        # authorization time, not at the first use of the semantic policy row.
        policy = policy.model_copy(update={"captured_at": authorized_at})
        planning_authorization = PlanningPolicyAuthorization(
            policy_snapshot=policy,
            environment=environment,
            eligible_before=_parse_utc(str(row["safe_end"])),
            authorized_at=authorized_at,
        )
        streams = self._load_plan_streams(connection, run_id)
        request_rows = connection.execute(
            """
            SELECT instance.*, spec.*, estimate.*,
                   request_limit.max_pages AS request_max_pages,
                   request_limit.max_calls AS request_max_calls,
                   request_limit.limits_hash AS request_limits_hash,
                   instance.request_instance_id AS instance_id,
                   instance.status AS instance_status,
                   retry.retry_count, retry.max_attempts,
                   retry.next_eligible_at AS retry_next_eligible_at
            FROM request_instances AS instance
            JOIN request_specs AS spec ON spec.request_spec_id = instance.request_spec_id
            JOIN request_plan_estimates AS estimate
              ON estimate.request_instance_id = instance.request_instance_id
            JOIN request_execution_limits AS request_limit
              ON request_limit.request_instance_id = instance.request_instance_id
             AND request_limit.run_id = instance.run_id
            JOIN retry_state AS retry
              ON retry.request_instance_id = instance.request_instance_id
            WHERE instance.run_id = ?
            ORDER BY instance.plan_ordinal
            """,
            (str(run_id),),
        ).fetchall()
        if len(request_rows) != int(row["planned_request_count"]):
            raise RestartProjectionIntegrityError("planned request cardinality is inconsistent")
        if tuple(int(value["plan_ordinal"]) for value in request_rows) != tuple(
            range(len(request_rows))
        ):
            raise RestartProjectionIntegrityError("planned request ordinals are incomplete")

        planned_requests: list[PlannedRequest] = []
        specifications: list[RequestSpecification] = []
        authorizations: list[RequestPolicyAuthorization] = []
        for request_row in request_rows:
            specification = self._validate_specification_row(connection, request_row)
            slots = self._request_slots(calendar, specification)
            authorization = RequestPolicyAuthorization(
                policy_snapshot=policy,
                request_spec_hash=specification.request_spec_hash,
                environment=environment,
                request_start=specification.start,
                request_end=specification.end,
                eligible_before=_parse_utc(str(request_row["authorization_eligible_before"])),
                authorized_at=_parse_utc(str(request_row["authorized_at"])),
            )
            if (
                str(request_row["policy_snapshot_id"]) != deterministic_policy_snapshot_id(policy)
                or str(request_row["calendar_snapshot_id"]) != str(row["calendar_snapshot_id"])
                or len(slots) != int(request_row["expected_slot_count"])
                or len(slots) * len(specification.instrument_mappings)
                != int(request_row["expected_observation_count"])
                or slots[0].start_utc != _parse_utc(str(request_row["first_slot_start"]))
                or slots[-1].end_utc != _parse_utc(str(request_row["last_slot_end"]))
                or int(request_row["max_attempts"]) != int(row["max_attempts"])
            ):
                raise RestartProjectionIntegrityError("request planning proof is inconsistent")
            planned_requests.append(
                PlannedRequest(
                    specification=specification,
                    authorization=authorization,
                    expected_slots=slots,
                    expected_observations=int(request_row["expected_observation_count"]),
                    estimated_pages=int(request_row["estimated_pages"]),
                    estimated_calls=int(request_row["estimated_calls"]),
                    estimated_bytes=int(request_row["estimated_bytes"]),
                    estimated_cost=Decimal(str(request_row["estimated_cost"])),
                )
            )
            specifications.append(specification)
            authorizations.append(authorization)

        plan = IngestionPlan(
            intent=IngestionIntent(str(row["mode"])),
            provider=str(row["provider"]),
            dataset=str(row["dataset"]),
            environment=environment,
            policy_authorization=planning_authorization,
            acquisition_strategy=AcquisitionStrategy(str(row["acquisition_strategy"])),
            repair_strategy=(
                None
                if row["repair_strategy"] is None
                else RepairStrategy(str(row["repair_strategy"]))
            ),
            repair_reason=(None if row["repair_reason"] is None else str(row["repair_reason"])),
            desired_start=_parse_utc(str(row["desired_start"])),
            desired_end=_parse_utc(str(row["desired_end"])),
            safe_end=_parse_utc(str(row["safe_end"])),
            calendar_snapshot_checksum=calendar.checksum,
            streams=streams,
            stream_ids=tuple(stream.stream_id for stream in streams),
            eligible_slot_count=int(row["eligible_slot_count"]),
            eligible_observation_count=int(row["eligible_observation_count"]),
            missing_observation_count=int(row["missing_observation_count"]),
            pending_observation_count=int(row["pending_observation_count"]),
            requests=tuple(planned_requests),
            estimated_pages=int(row["estimated_pages"]),
            estimated_calls=int(row["estimated_calls"]),
            estimated_bytes=int(row["estimated_bytes"]),
            estimated_cost=Decimal(str(row["estimated_cost"])),
        )
        persistence = PlanPersistenceRequest(
            run_id=run_id,
            calendar_snapshot_id=str(row["calendar_snapshot_id"]),
            reason=str(row["reason"]),
            max_attempts=int(row["max_attempts"]),
            max_pages=int(row["max_pages"]),
            max_calls=int(row["max_calls"]),
            max_pages_per_request=int(row["max_pages_per_request"]),
            max_calls_per_request=int(row["max_calls_per_request"]),
            plan=plan,
        )
        if (
            _plan_hash(persistence) != str(row["plan_hash"])
            or _execution_limits_hash(persistence) != str(row["limits_hash"])
            or deterministic_catalog_snapshot_id(policy) != str(row["catalog_snapshot_id"])
            or deterministic_calendar_snapshot_id(calendar) != str(row["calendar_snapshot_id"])
        ):
            raise RestartProjectionIntegrityError("plan content identity is inconsistent")

        projections: list[RestartRequestProjection] = []
        allocated_limits = _allocate_request_execution_limits(persistence)
        for request_row, specification, authorization in zip(
            request_rows, specifications, authorizations, strict=True
        ):
            identity = deterministic_request_instance_identity(
                run_id,
                int(request_row["plan_ordinal"]),
                specification,
            )
            if (
                str(identity.request_instance_id) != str(request_row["instance_id"])
                or str(request_row["request_spec_id"]) != specification.request_spec_id
                or str(request_row["intent"]) != plan.intent.value
            ):
                raise RestartProjectionIntegrityError("request instance identity is inconsistent")
            allocated_pages, allocated_calls = allocated_limits[int(request_row["plan_ordinal"])]
            if (
                int(request_row["request_max_pages"]) != allocated_pages
                or int(request_row["request_max_calls"]) != allocated_calls
                or str(request_row["request_limits_hash"])
                != _request_execution_limits_hash(
                    run_id,
                    identity.request_instance_id,
                    max_pages=allocated_pages,
                    max_calls=allocated_calls,
                )
            ):
                raise RestartProjectionIntegrityError("request dispatch limits are inconsistent")
            status = RequestInstanceStatus(str(request_row["instance_status"]))
            attempts = self._load_attempts(
                connection,
                identity.request_instance_id,
                authorization,
            )
            latest = attempts[-1] if attempts else None
            publication = self._load_publication(
                connection,
                request_instance_id=identity.request_instance_id,
                specification=specification,
                calendar=calendar,
                attempts=attempts,
            )
            semantic_noop_committed = self._load_semantic_noop(
                connection,
                request_instance_id=identity.request_instance_id,
                specification=specification,
                latest=latest,
                policy_snapshot_id=str(row["policy_snapshot_id"]),
                evaluated_at=evaluated_at,
            )
            nonpublication_partial_committed = self._load_nonpublication_partial(
                connection,
                request_instance_id=identity.request_instance_id,
                specification=specification,
                latest=latest,
                policy_snapshot_id=str(row["policy_snapshot_id"]),
            )
            retry_count = int(request_row["retry_count"])
            max_attempts = int(request_row["max_attempts"])
            retry_next_eligible_at = _parse_optional_utc(request_row["retry_next_eligible_at"])
            action = self._derive_action(
                run_status=run_status,
                request_status=status,
                latest=latest,
                publication=publication,
                semantic_noop_committed=semantic_noop_committed,
                nonpublication_partial_committed=nonpublication_partial_committed,
                policy_current=policy_current,
                calendar_current=calendar_current,
                retry_count=retry_count,
                max_attempts=max_attempts,
                next_eligible_at=retry_next_eligible_at,
                evaluated_at=evaluated_at,
            )
            projections.append(
                RestartRequestProjection(
                    request_instance_id=identity.request_instance_id,
                    plan_ordinal=int(request_row["plan_ordinal"]),
                    status=status,
                    specification=specification,
                    authorization=authorization,
                    expected_slot_count=int(request_row["expected_slot_count"]),
                    expected_observation_count=int(request_row["expected_observation_count"]),
                    max_pages=allocated_pages,
                    max_calls=allocated_calls,
                    max_attempts=max_attempts,
                    retry_count=retry_count,
                    next_eligible_at=retry_next_eligible_at,
                    latest_attempt=latest,
                    publication=publication,
                    semantic_noop_committed=semantic_noop_committed,
                    nonpublication_partial_committed=nonpublication_partial_committed,
                    action=action,
                )
            )

        if run_status in _TERMINAL_RUNS and any(
            request.status not in _TERMINAL_REQUESTS for request in projections
        ):
            raise RestartProjectionIntegrityError("terminal run contains unfinished requests")
        return RestartRunContext(
            run_id=run_id,
            status=run_status,
            plan_hash=str(row["plan_hash"]),
            calendar_snapshot_id=str(row["calendar_snapshot_id"]),
            policy_snapshot_id=str(row["policy_snapshot_id"]),
            catalog_snapshot_id=str(row["catalog_snapshot_id"]),
            calendar_current=calendar_current,
            policy_current=policy_current,
            plan=plan,
            requests=tuple(projections),
        )

    def _load_calendar(
        self,
        connection: sqlite3.Connection,
        calendar_snapshot_id: str,
    ) -> tuple[CalendarSnapshot, bool]:
        row = connection.execute(
            "SELECT * FROM calendar_snapshots WHERE calendar_snapshot_id = ?",
            (calendar_snapshot_id,),
        ).fetchone()
        if row is None:
            raise RestartProjectionIntegrityError("calendar snapshot is absent")
        sessions = connection.execute(
            """
            SELECT * FROM calendar_sessions
            WHERE calendar_snapshot_id = ? ORDER BY session_date
            """,
            (calendar_snapshot_id,),
        ).fetchall()
        snapshot = CalendarSnapshot.create(
            library_name=str(row["package_name"]),
            library_version=str(row["package_version"]),
            tzdata_version=str(row["tzdata_version"]),
            calendar_name=str(row["calendar_name"]),
            timezone_name=str(row["timezone_name"]),
            range_start=date.fromisoformat(str(row["session_start_date"])),
            range_end=date.fromisoformat(str(row["session_end_date"])),
            generated_at=_parse_utc(str(row["generated_at"])),
            sessions=tuple(
                CalendarSession(
                    session_date=date.fromisoformat(str(value["session_date"])),
                    open_utc=_parse_utc(str(value["open_at"])),
                    close_utc=_parse_utc(str(value["close_at"])),
                    is_early_close=_as_bool(value["is_early_close"]),
                )
                for value in sessions
            ),
        )
        if snapshot.checksum != _validate_sha256(row["schedule_checksum"], platform=True):
            raise RestartProjectionIntegrityError("calendar schedule proof is inconsistent")
        for value, session in zip(sessions, snapshot.sessions, strict=True):
            expected_5m = int((session.close_utc - session.open_utc).total_seconds() // 300)
            if (
                int(value["expected_1d_count"]) != 1
                or int(value["expected_5m_count"]) != expected_5m
            ):
                raise RestartProjectionIntegrityError("calendar expected counts are inconsistent")
        return snapshot, str(row["state"]) == "CURRENT"

    def _load_policy(
        self,
        connection: sqlite3.Connection,
        *,
        policy_snapshot_id: str,
        catalog_snapshot_id: str,
    ) -> tuple[DatasetPolicySnapshot, bool]:
        row = connection.execute(
            """
            SELECT policy.*, provenance.policy_status, provenance.verified_on,
                   catalog.catalog_id, catalog.catalog_revision, catalog.catalog_hash,
                   catalog.captured_at AS catalog_captured_at
            FROM policy_snapshots AS policy
            JOIN policy_snapshot_provenance AS provenance
              ON provenance.policy_snapshot_id = policy.policy_snapshot_id
            JOIN policy_catalog_snapshots AS catalog
              ON catalog.catalog_snapshot_id = ?
            WHERE policy.policy_snapshot_id = ?
            """,
            (catalog_snapshot_id, policy_snapshot_id),
        ).fetchone()
        if row is None:
            raise RestartProjectionIntegrityError("policy provenance is absent")
        policy_hash = _validate_sha256(row["policy_hash"])
        catalog_hash = _validate_sha256(row["catalog_hash"])
        if str(row["verified_at"]) != str(row["verified_on"]) or str(row["captured_at"]) != str(
            row["catalog_captured_at"]
        ):
            raise RestartProjectionIntegrityError("policy dates are inconsistent")
        snapshot = DatasetPolicySnapshot(
            catalog_id=str(row["catalog_id"]),
            catalog_revision=int(row["catalog_revision"]),
            catalog_hash=catalog_hash,
            policy_id=str(row["policy_id"]),
            policy_revision=int(row["revision"]),
            policy_hash=policy_hash,
            provider=str(row["provider"]),
            dataset=str(row["dataset"]),
            mode=RetentionMode(str(row["retention_mode"])),
            status=DatasetPolicyStatus(str(row["policy_status"])),
            verified_on=date.fromisoformat(str(row["verified_on"])),
            captured_at=_parse_utc(str(row["captured_at"])),
        )
        if (
            deterministic_policy_snapshot_id(snapshot) != policy_snapshot_id
            or deterministic_catalog_snapshot_id(snapshot) != catalog_snapshot_id
        ):
            raise RestartProjectionIntegrityError("policy identity is inconsistent")
        current = connection.execute(
            """
            SELECT * FROM dataset_policy_status WHERE provider = ? AND dataset = ?
            """,
            (snapshot.provider, snapshot.dataset),
        ).fetchone()
        if current is None:
            raise RestartProjectionIntegrityError("dataset policy status is absent")
        current_status = str(current["status"])
        if current_status not in {
            "ACTIVE",
            "PENDING",
            "SUSPENDED",
            "EXPIRED",
            "TERMINATED",
            "PROHIBITED",
        }:
            raise RestartProjectionIntegrityError("dataset policy status is invalid")
        policy_current = current_status == DatasetPolicyStatus.ACTIVE.value
        if policy_current and (
            str(current["policy_snapshot_id"]) != policy_snapshot_id
            or str(current["retention_mode"]) != snapshot.mode.value
            or current["unavailable_at"] is not None
        ):
            raise RestartProjectionIntegrityError("active dataset policy is inconsistent")
        return snapshot, policy_current

    def _load_plan_streams(
        self,
        connection: sqlite3.Connection,
        run_id: UUID,
    ) -> tuple[StreamKey, ...]:
        rows = connection.execute(
            """
            SELECT link.ordinal, stream.* FROM ingestion_plan_streams AS link
            JOIN stream_keys AS stream ON stream.stream_id = link.stream_id
            WHERE link.run_id = ? ORDER BY link.ordinal
            """,
            (str(run_id),),
        ).fetchall()
        if tuple(int(row["ordinal"]) for row in rows) != tuple(range(len(rows))):
            raise RestartProjectionIntegrityError("plan stream ordinals are incomplete")
        streams = tuple(_parse_stream(str(row["dimensions_json"])) for row in rows)
        for row, stream in zip(rows, streams, strict=True):
            if (
                str(row["stream_id"]) != stream.stream_id
                or str(row["stream_hash"]) != stream.stream_hash
                or str(row["provider"]) != stream.provider
                or str(row["dataset"]) != stream.dataset
                or str(row["instrument_id"]) != str(stream.instrument_id)
                or str(row["timeframe"]) != stream.timeframe.value
                or str(row["session"]) != stream.session.value
                or str(row["adjustment"]) != stream.adjustment.value
            ):
                raise RestartProjectionIntegrityError("plan stream identity is inconsistent")
        return streams

    def _validate_specification_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> RequestSpecification:
        specification = _parse_specification(str(row["specification_json"]))
        if (
            str(row["request_spec_id"]) != specification.request_spec_id
            or str(row["request_spec_hash"]) != specification.request_spec_hash
            or str(row["provider"]) != specification.provider
            or str(row["dataset"]) != specification.dataset
            or _parse_utc(str(row["interval_start"])) != specification.start
            or _parse_utc(str(row["interval_end"])) != specification.end
            or str(row["mapping_version"]) != specification.mapping_semantic_version
        ):
            raise RestartProjectionIntegrityError("request specification identity is inconsistent")
        links = connection.execute(
            """
            SELECT link.*, stream.dimensions_json FROM request_spec_streams AS link
            JOIN stream_keys AS stream ON stream.stream_id = link.stream_id
            WHERE link.request_spec_id = ? ORDER BY link.ordinal
            """,
            (specification.request_spec_id,),
        ).fetchall()
        expected = tuple(
            (stream.stream_id, mapping.provider_identifier, ordinal)
            for ordinal, (stream, mapping) in enumerate(
                zip(specification.stream_keys(), specification.instrument_mappings, strict=True)
            )
        )
        actual = tuple(
            (str(value["stream_id"]), str(value["provider_identifier"]), int(value["ordinal"]))
            for value in links
        )
        if actual != expected or any(
            _parse_stream(str(value["dimensions_json"])) != stream
            for value, stream in zip(links, specification.stream_keys(), strict=True)
        ):
            raise RestartProjectionIntegrityError("request-to-stream proof is inconsistent")
        return specification

    @staticmethod
    def _request_slots(
        calendar: CalendarSnapshot,
        specification: RequestSpecification,
    ) -> tuple[ExpectedCalendarSlot, ...]:
        slots = tuple(
            slot
            for slot in calendar.expected_slots(specification.timeframe)
            if specification.start <= slot.start_utc and slot.end_utc <= specification.end
        )
        if (
            not slots
            or slots[0].start_utc != specification.start
            or slots[-1].end_utc != specification.end
        ):
            raise RestartProjectionIntegrityError("request/calendar interval proof is incomplete")
        return slots

    def _load_attempts(
        self,
        connection: sqlite3.Connection,
        request_instance_id: UUID,
        authorization: RequestPolicyAuthorization,
    ) -> tuple[RestartAttemptProjection, ...]:
        rows = connection.execute(
            """
            SELECT attempt.*, auth.request_instance_id AS auth_request_instance_id,
                   auth.request_spec_id AS auth_request_spec_id,
                   auth.policy_snapshot_id AS auth_policy_snapshot_id,
                   auth.authorization_hash, auth.authorization_json,
                   auth.eligible_before, auth.authorized_at
            FROM request_attempts AS attempt
            LEFT JOIN attempt_request_authorizations AS auth
              ON auth.attempt_id = attempt.attempt_id
            WHERE attempt.request_instance_id = ? ORDER BY attempt.attempt_number
            """,
            (str(request_instance_id),),
        ).fetchall()
        if tuple(int(row["attempt_number"]) for row in rows) != tuple(range(1, len(rows) + 1)):
            raise RestartProjectionIntegrityError("attempt sequence is incomplete")
        projections: list[RestartAttemptProjection] = []
        for row in rows:
            if row["authorization_json"] is None:
                raise RestartProjectionIntegrityError("attempt authorization proof is absent")
            persisted = _parse_model_json(
                RequestPolicyAuthorization,
                str(row["authorization_json"]),
            )
            if (
                persisted != authorization
                or str(row["authorization_hash"])
                != _hash_json(authorization.model_dump(mode="json"))
                or str(row["auth_request_instance_id"]) != str(request_instance_id)
                or str(row["auth_request_spec_id"])
                != authorization.request_spec_hash.join(("request_spec_v1_", ""))
                or str(row["auth_policy_snapshot_id"])
                != deterministic_policy_snapshot_id(authorization.policy_snapshot)
                or _parse_utc(str(row["eligible_before"])) != authorization.eligible_before
                or _parse_utc(str(row["authorized_at"])) != authorization.authorized_at
            ):
                raise RestartProjectionIntegrityError("attempt authorization proof is inconsistent")
            attempt_id = UUID(str(row["attempt_id"]))
            acquisition, artifact_ids = self._load_acquisition(
                connection,
                attempt_id=attempt_id,
                request_instance_id=request_instance_id,
                request_authorization=authorization,
            )
            projections.append(
                RestartAttemptProjection(
                    attempt_id=attempt_id,
                    request_instance_id=request_instance_id,
                    attempt_number=int(row["attempt_number"]),
                    status=AttemptStatus(str(row["status"])),
                    started_at=_parse_optional_utc(row["started_at"]),
                    completed_at=_parse_optional_utc(row["completed_at"]),
                    next_eligible_at=_parse_optional_utc(row["next_eligible_at"]),
                    page_count=int(row["page_count"]),
                    pagination_complete=_as_bool(row["pagination_complete"]),
                    terminal_page_verified=_as_bool(row["terminal_page_verified"]),
                    request_authorization=persisted,
                    acquisition_authorization=acquisition,
                    ordered_artifact_ids=artifact_ids,
                )
            )
        return tuple(projections)

    def _load_acquisition(
        self,
        connection: sqlite3.Connection,
        *,
        attempt_id: UUID,
        request_instance_id: UUID,
        request_authorization: RequestPolicyAuthorization,
    ) -> tuple[AcquisitionPolicyAuthorization | None, tuple[str, ...]]:
        row = connection.execute(
            "SELECT * FROM attempt_acquisition_records WHERE attempt_id = ?",
            (str(attempt_id),),
        ).fetchone()
        if row is None:
            return None, ()
        authorization = _parse_model_json(
            AcquisitionPolicyAuthorization,
            str(row["authorization_json"]),
        )
        if (
            authorization.request != request_authorization
            or str(row["request_instance_id"]) != str(request_instance_id)
            or str(row["request_spec_id"])
            != f"request_spec_v1_{request_authorization.request_spec_hash}"
            or str(row["policy_snapshot_id"])
            != deterministic_policy_snapshot_id(request_authorization.policy_snapshot)
            or str(row["authorization_hash"]) != _hash_json(authorization.model_dump(mode="json"))
            or not _as_bool(row["pagination_complete"])
            or not _as_bool(row["terminal_page_verified"])
        ):
            raise RestartProjectionIntegrityError("completed acquisition proof is inconsistent")
        links = connection.execute(
            """
            SELECT link.*, raw.*, manifest.manifest_content_sha256,
                   manifest.manifest_byte_count,
                   observation.attempt_id AS observation_attempt_id,
                   replay.raw_batch_id
            FROM acquisition_artifacts AS link
            JOIN raw_artifacts AS raw ON raw.artifact_id = link.artifact_id
            JOIN raw_artifact_manifests AS manifest ON manifest.artifact_id = raw.artifact_id
            JOIN attempt_artifact_observations AS observation
              ON observation.attempt_id = link.attempt_id
             AND observation.artifact_id = link.artifact_id
            JOIN raw_replay_provenance AS replay
              ON replay.attempt_id = link.attempt_id
             AND replay.artifact_id = link.artifact_id
            WHERE link.attempt_id = ? ORDER BY link.ordinal
            """,
            (str(attempt_id),),
        ).fetchall()
        identities = tuple(
            RawArtifactIdentity.from_digest(
                request_spec_hash=value.request_spec_hash,
                page_ordinal=value.page_ordinal,
                page_relation=value.page_relation,
                media_type=value.media_type,
                content_encoding=value.content_encoding,
                content_sha256=value.content_sha256,
                byte_count=value.byte_count,
            )
            for value in authorization.ordered_artifacts
        )
        if len(links) != len(identities) or int(row["page_count"]) != len(identities):
            raise RestartProjectionIntegrityError("acquisition artifact count is inconsistent")
        for ordinal, (link, descriptor, identity) in enumerate(
            zip(links, authorization.ordered_artifacts, identities, strict=True)
        ):
            if (
                int(link["ordinal"]) != ordinal
                or str(link["artifact_id"]) != identity.artifact_id
                or str(link["descriptor_hash"]) != _hash_json(descriptor.model_dump(mode="json"))
                or str(link["request_spec_id"]) != f"request_spec_v1_{identity.request_spec_hash}"
                or int(link["page_ordinal"]) != identity.page_ordinal
                or str(link["page_relation_hash"]) != identity.page_relation_hash
                or str(link["content_sha256"]) != identity.content_sha256
                or int(link["byte_count"]) != identity.byte_count
                or str(link["media_type"]) != identity.media_type
                or str(link["content_encoding"]) != identity.content_encoding
                or str(link["state"]) != "VERIFIED"
                or str(link["observation_attempt_id"]) != str(attempt_id)
                or not self._store._managed_file_matches_catalog(
                    str(link["relative_path"]),
                    expected_sha256=str(link["content_sha256"]),
                    expected_bytes=int(link["byte_count"]),
                )
                or not self._store._managed_file_matches_catalog(
                    str(link["manifest_relative_path"]),
                    expected_sha256=str(link["manifest_content_sha256"]),
                    expected_bytes=int(link["manifest_byte_count"]),
                )
            ):
                raise RestartProjectionIntegrityError("acquisition raw proof is inconsistent")
        artifact_ids = tuple(identity.artifact_id for identity in identities)
        ordered_hash = _hash_json(
            {
                "canonicalization_version": IDENTITY_CANONICALIZATION_VERSION,
                "kind": "ordered-raw-artifacts",
                "payload": {"artifact_ids": list(artifact_ids)},
            }
        )
        if str(row["ordered_artifacts_hash"]) != ordered_hash:
            raise RestartProjectionIntegrityError("acquisition artifact order is inconsistent")
        return authorization, artifact_ids

    def _load_publication(
        self,
        connection: sqlite3.Connection,
        *,
        request_instance_id: UUID,
        specification: RequestSpecification,
        calendar: CalendarSnapshot,
        attempts: tuple[RestartAttemptProjection, ...],
    ) -> RestartPublicationProjection | None:
        rows = connection.execute(
            """
            SELECT context.*, expectation.expectation_hash, expectation.expectation_json,
                   request_expectation.state AS expectation_state,
                   request_expectation.prepared_at,
                   contract.processing_signature_hash, contract.processing_signature_json,
                   contract.provenance_json, contract.provenance_hash
            FROM batch_context_requests AS link
            JOIN batch_contexts AS context ON context.batch_context_id = link.batch_context_id
            JOIN batch_context_processing_contracts AS contract
              ON contract.batch_context_id = context.batch_context_id
            LEFT JOIN batch_publication_expectations AS expectation
              ON expectation.batch_context_id = context.batch_context_id
            LEFT JOIN batch_publication_expectation_requests AS request_expectation
              ON request_expectation.batch_context_id = context.batch_context_id
             AND request_expectation.request_instance_id = link.request_instance_id
            WHERE link.request_instance_id = ?
            ORDER BY context.created_at, context.batch_context_id
            """,
            (str(request_instance_id),),
        ).fetchall()
        if not rows:
            return None
        active = [row for row in rows if row["expectation_state"] in (None, "PREPARED")]
        if len(active) > 1:
            raise RestartProjectionIntegrityError("publication recovery state is ambiguous")
        selected = active[0] if active else rows[-1]
        context_artifacts = tuple(
            str(value["artifact_id"])
            for value in connection.execute(
                """
                SELECT artifact_id FROM batch_context_artifacts
                WHERE batch_context_id = ? ORDER BY ordinal
                """,
                (str(selected["batch_context_id"]),),
            ).fetchall()
        )
        if not attempts or context_artifacts != attempts[-1].ordered_artifact_ids:
            raise RestartProjectionIntegrityError("batch context lacks current acquisition proof")
        if selected["expectation_state"] is None:
            return RestartPublicationProjection(
                batch_context_id=str(selected["batch_context_id"]),
                canonical_batch_id=str(selected["canonical_batch_id"]),
                state=PublicationRecoveryState.CONTEXT_ONLY,
            )
        expectation = _parse_model_json(
            CanonicalBatchExpectation,
            str(selected["expectation_json"]),
        )
        if (
            str(selected["expectation_hash"]) != _hash_json(expectation.model_dump(mode="json"))
            or expectation.specification != specification
            or expectation.calendar_snapshot != calendar
            or expectation.batch_context.batch_context_id != str(selected["batch_context_id"])
            or expectation.batch_context.canonical_batch_id != str(selected["canonical_batch_id"])
            or expectation.batch_context.batch_identity.artifact_ids != context_artifacts
            or _hash_json(
                expectation.batch_context.batch_identity.processing_signature.model_dump(
                    mode="json"
                )
            )
            != str(selected["processing_signature_hash"])
            or _canonical_json(expectation.batch_context.batch_identity.processing_signature)
            != str(selected["processing_signature_json"])
            or _hash_json(expectation.provenance.model_dump(mode="json"))
            != str(selected["provenance_hash"])
            or _canonical_json(expectation.provenance) != str(selected["provenance_json"])
        ):
            raise RestartProjectionIntegrityError("publication expectation is inconsistent")
        state = PublicationRecoveryState(str(selected["expectation_state"]))
        commit = connection.execute(
            """
            SELECT publication.*, terminal.terminal_status,
                   terminal.coverage_commit_hash AS terminal_coverage_hash
            FROM publication_commits AS publication
            LEFT JOIN request_terminal_proofs AS terminal
              ON terminal.request_instance_id = publication.request_instance_id
            WHERE publication.request_instance_id = ? AND publication.canonical_batch_id = ?
            """,
            (str(request_instance_id), expectation.batch_context.canonical_batch_id),
        ).fetchone()
        if state is PublicationRecoveryState.CATALOGED:
            if (
                commit is None
                or str(commit["terminal_coverage_hash"]) != str(commit["coverage_commit_hash"])
                or str(commit["attempt_id"]) != str(attempts[-1].attempt_id)
            ):
                raise RestartProjectionIntegrityError("publication commit proof is inconsistent")
        elif commit is not None:
            raise RestartProjectionIntegrityError("uncommitted expectation has a commit proof")
        return RestartPublicationProjection(
            batch_context_id=expectation.batch_context.batch_context_id,
            canonical_batch_id=expectation.batch_context.canonical_batch_id,
            state=state,
            expectation=expectation,
            prepared_at=_parse_utc(str(selected["prepared_at"])),
            publication_committed=state is PublicationRecoveryState.CATALOGED,
        )

    def _load_semantic_noop(
        self,
        connection: sqlite3.Connection,
        *,
        request_instance_id: UUID,
        specification: RequestSpecification,
        latest: RestartAttemptProjection | None,
        policy_snapshot_id: str,
        evaluated_at: datetime,
    ) -> bool:
        row = connection.execute(
            """
            SELECT noop.*, instance.status AS request_status,
                   attempt.status AS attempt_status,
                   acquisition.authorization_hash,
                   context_request.batch_context_id AS linked_batch_context_id,
                   contract.processing_signature_hash,
                    active.status AS active_policy_status,
                    active.policy_snapshot_id AS active_policy_snapshot_id,
                    active.retention_mode AS active_retention_mode,
                    active.expires_at AS active_policy_expires_at,
                    active.unavailable_at AS active_policy_unavailable_at
            FROM semantic_noop_commits AS noop
            JOIN request_instances AS instance
              ON instance.request_instance_id = noop.request_instance_id
            JOIN request_attempts AS attempt ON attempt.attempt_id = noop.attempt_id
            JOIN attempt_acquisition_records AS acquisition
              ON acquisition.attempt_id = noop.attempt_id
            JOIN batch_context_requests AS context_request
              ON context_request.batch_context_id = noop.batch_context_id
             AND context_request.request_instance_id = noop.request_instance_id
            JOIN batch_context_processing_contracts AS contract
              ON contract.batch_context_id = noop.batch_context_id
            LEFT JOIN dataset_policy_status AS active
              ON active.provider = ? AND active.dataset = ?
            WHERE noop.request_instance_id = ?
            """,
            (
                specification.provider,
                specification.dataset,
                str(request_instance_id),
            ),
        ).fetchone()
        if row is None:
            return False
        if latest is None or latest.acquisition_authorization is None:
            raise RestartProjectionIntegrityError(
                "semantic no-op lacks a completed acquisition attempt"
            )
        try:
            observation_payload = _strict_json(str(row["duplicate_observations_json"]))
            if not isinstance(observation_payload, list):
                raise ValueError("semantic no-op observations are not a list")
            observations = tuple(
                SemanticNoOpObservationProof.model_validate(value) for value in observation_payload
            )
        except (ValueError, ValidationError, json.JSONDecodeError) as error:
            raise RestartProjectionIntegrityError(
                "semantic no-op observation proof is invalid"
            ) from error
        canonical_observations = [
            {
                "end": _format_utc(value.end),
                "matching_supporting_batch_ids": list(value.matching_supporting_batch_ids),
                "observation_id": value.observation_id,
                "start": _format_utc(value.start),
                "stream_id": value.stream_id,
                "value_fingerprint": value.value_fingerprint,
            }
            for value in observations
        ]
        expected_order = tuple(
            sorted(
                observations,
                key=lambda value: (value.start, value.stream_id, value.observation_id),
            )
        )
        duplicate_count = len(observations)
        observation_support = tuple(
            sorted(
                {
                    batch_id
                    for observation in observations
                    for batch_id in observation.matching_supporting_batch_ids
                }
            )
        )
        supporting = tuple(
            str(value["canonical_batch_id"])
            for value in connection.execute(
                """
                SELECT canonical_batch_id FROM semantic_noop_supporting_batches
                WHERE request_instance_id = ? ORDER BY canonical_batch_id
                """,
                (str(request_instance_id),),
            ).fetchall()
        )
        proof_payload = {
            "acquisition_authorization_hash": str(row["acquisition_authorization_hash"]),
            "attempt_id": str(row["attempt_id"]),
            "batch_context_id": str(row["batch_context_id"]),
            "duplicate_observations_hash": str(row["duplicate_observations_hash"]),
            "processing_signature_hash": str(row["processing_signature_hash"]),
            "request_instance_id": str(request_instance_id),
            "request_spec_id": specification.request_spec_id,
            "semantic_duplicate_count": duplicate_count,
            "supporting_batch_ids": list(supporting),
            "version": 1,
        }
        if (
            str(row["request_spec_id"]) != specification.request_spec_id
            or str(row["batch_context_id"]) != str(row["linked_batch_context_id"])
            or str(row["policy_snapshot_id"]) != policy_snapshot_id
            or str(row["request_status"]) != RequestInstanceStatus.SUCCESS.value
            or str(row["attempt_status"]) != AttemptStatus.SUCCESS.value
            or str(row["attempt_id"]) != str(latest.attempt_id)
            or str(row["authorization_hash"]) != str(row["acquisition_authorization_hash"])
            or _hash_json(latest.acquisition_authorization.model_dump(mode="json"))
            != str(row["acquisition_authorization_hash"])
            or _canonical_json(canonical_observations) != str(row["duplicate_observations_json"])
            or _hash_json(canonical_observations) != str(row["duplicate_observations_hash"])
            or duplicate_count != int(row["semantic_duplicate_count"])
            or not observations
            or observations != expected_order
            or len({value.observation_id for value in observations}) != duplicate_count
            or {value.stream_id for value in observations}
            != {stream.stream_id for stream in specification.stream_keys()}
            or any(
                value.start < specification.start or value.end > specification.end
                for value in observations
            )
            or observation_support != supporting
            or not supporting
            or _hash_json(proof_payload) != str(row["proof_hash"])
            or str(row["active_policy_status"]) != DatasetPolicyStatus.ACTIVE.value
            or str(row["active_policy_snapshot_id"]) != policy_snapshot_id
            or str(row["active_retention_mode"])
            in {RetentionMode.PROHIBITED.value, RetentionMode.EPHEMERAL.value}
            or (
                row["active_policy_expires_at"] is not None
                and _parse_utc(str(row["active_policy_expires_at"])) <= evaluated_at
            )
            or row["active_policy_unavailable_at"] is not None
        ):
            raise RestartProjectionIntegrityError("semantic no-op terminal proof is inconsistent")
        support_rows = connection.execute(
            """
            SELECT link.canonical_batch_id, batch.state, batch.policy_snapshot_id,
                   spec.provider, spec.dataset,
                   contract.processing_signature_hash,
                   count(file.relative_path) AS file_count
            FROM semantic_noop_supporting_batches AS link
            JOIN canonical_batches AS batch
              ON batch.canonical_batch_id = link.canonical_batch_id
            JOIN batch_contexts AS context
              ON context.batch_context_id = batch.batch_context_id
            JOIN request_specs AS spec ON spec.request_spec_id = context.request_spec_id
            JOIN batch_context_processing_contracts AS contract
              ON contract.batch_context_id = context.batch_context_id
            LEFT JOIN canonical_files AS file
              ON file.canonical_batch_id = batch.canonical_batch_id
            WHERE link.request_instance_id = ?
            GROUP BY link.canonical_batch_id
            ORDER BY link.canonical_batch_id
            """,
            (str(request_instance_id),),
        ).fetchall()
        if len(support_rows) != len(supporting) or any(
            str(value["state"]) != "VERIFIED"
            or str(value["policy_snapshot_id"]) != policy_snapshot_id
            or (str(value["provider"]), str(value["dataset"]))
            != (specification.provider, specification.dataset)
            or str(value["processing_signature_hash"]) != str(row["processing_signature_hash"])
            or int(value["file_count"]) <= 0
            for value in support_rows
        ):
            raise RestartProjectionIntegrityError(
                "semantic no-op supporting catalog proof is no longer valid"
            )
        return True

    def _load_nonpublication_partial(
        self,
        connection: sqlite3.Connection,
        *,
        request_instance_id: UUID,
        specification: RequestSpecification,
        latest: RestartAttemptProjection | None,
        policy_snapshot_id: str,
    ) -> bool:
        rows = connection.execute(
            """
            SELECT quarantine.batch_context_id, quarantine.policy_snapshot_id,
                   quarantine.validation_summary_json, quarantine.state,
                   error.attempt_id, error.category, error.code,
                   error.sanitized_message, error.retryable
            FROM batch_context_requests AS context_request
            JOIN quarantine_artifacts AS quarantine
              ON quarantine.batch_context_id = context_request.batch_context_id
            JOIN errors AS error
              ON error.request_instance_id = context_request.request_instance_id
            WHERE context_request.request_instance_id = ?
              AND error.code = 'PARTIAL_STREAMS_BLOCKED'
            ORDER BY quarantine.quarantine_artifact_id, error.error_id
            """,
            (str(request_instance_id),),
        ).fetchall()
        if not rows:
            return False
        if len(rows) != 1 or latest is None:
            raise RestartProjectionIntegrityError(
                "partial non-publication outcome has ambiguous durable evidence"
            )
        row = rows[0]
        try:
            summary = _strict_json(str(row["validation_summary_json"]))
        except (ValueError, json.JSONDecodeError) as error:
            raise RestartProjectionIntegrityError(
                "partial non-publication quarantine proof is invalid"
            ) from error
        if (
            not isinstance(summary, dict)
            or set(summary) != {"blocked_streams", "schema_version"}
            or summary["schema_version"] != 1
            or not isinstance(summary["blocked_streams"], list)
            or not summary["blocked_streams"]
            or _canonical_json(summary) != str(row["validation_summary_json"])
        ):
            raise RestartProjectionIntegrityError(
                "partial non-publication quarantine proof is malformed"
            )
        blocked: list[tuple[str, datetime, datetime]] = []
        for value in summary["blocked_streams"]:
            if (
                not isinstance(value, dict)
                or set(value) != {"request_end", "request_start", "stream_id", "validation_codes"}
                or not isinstance(value["stream_id"], str)
                or not isinstance(value["request_start"], str)
                or not isinstance(value["request_end"], str)
                or not isinstance(value["validation_codes"], list)
                or not value["validation_codes"]
                or any(not isinstance(code, str) for code in value["validation_codes"])
            ):
                raise RestartProjectionIntegrityError(
                    "partial non-publication blocked-stream proof is malformed"
                )
            blocked.append(
                (
                    value["stream_id"],
                    _parse_utc(value["request_start"]),
                    _parse_utc(value["request_end"]),
                )
            )
        gap_rows = connection.execute(
            """
            SELECT stream_id, interval_start, interval_end, gap_type, status,
                   blocking, resolved_at, canonical_batch_id
            FROM gaps
            WHERE request_instance_id = ?
            ORDER BY stream_id, interval_start, interval_end
            """,
            (str(request_instance_id),),
        ).fetchall()
        gaps = tuple(
            (
                str(value["stream_id"]),
                _parse_utc(str(value["interval_start"])),
                _parse_utc(str(value["interval_end"])),
            )
            for value in gap_rows
        )
        expected_streams = {stream.stream_id for stream in specification.stream_keys()}
        if (
            str(row["state"]) != "VERIFIED"
            or str(row["policy_snapshot_id"]) != policy_snapshot_id
            or str(row["attempt_id"]) != str(latest.attempt_id)
            or latest.status is not AttemptStatus.SUCCESS
            or str(row["category"]) != "VALIDATION"
            or int(row["retryable"]) != 0
            or not str(row["sanitized_message"])
            or len(set(blocked)) != len(blocked)
            or any(
                stream_id not in expected_streams
                or start != specification.start
                or end != specification.end
                for stream_id, start, end in blocked
            )
            or tuple(sorted(blocked)) != gaps
            or any(
                str(value["gap_type"]) != "INTEGRITY"
                or str(value["status"]) != "OPEN"
                or not _as_bool(value["blocking"])
                or value["resolved_at"] is not None
                or value["canonical_batch_id"] is not None
                for value in gap_rows
            )
        ):
            raise RestartProjectionIntegrityError(
                "partial non-publication terminal proof is inconsistent"
            )
        return True

    @staticmethod
    def _derive_action(
        *,
        run_status: IngestionRunStatus,
        request_status: RequestInstanceStatus,
        latest: RestartAttemptProjection | None,
        publication: RestartPublicationProjection | None,
        semantic_noop_committed: bool,
        nonpublication_partial_committed: bool,
        policy_current: bool,
        calendar_current: bool,
        retry_count: int,
        max_attempts: int,
        next_eligible_at: datetime | None,
        evaluated_at: datetime,
    ) -> RestartAction:
        if request_status in _TERMINAL_REQUESTS:
            if request_status in {
                RequestInstanceStatus.SUCCESS,
                RequestInstanceStatus.PARTIAL,
            } and (
                latest is None
                or latest.status is not AttemptStatus.SUCCESS
                or (
                    not semantic_noop_committed
                    and not (
                        request_status is RequestInstanceStatus.PARTIAL
                        and nonpublication_partial_committed
                    )
                    and (
                        publication is None
                        or publication.state is not PublicationRecoveryState.CATALOGED
                    )
                )
            ):
                raise RestartProjectionIntegrityError(
                    "successful request lacks terminal publication proof"
                )
            return (
                RestartAction.RECONCILE_RUN
                if run_status not in _TERMINAL_RUNS
                else RestartAction.NONE
            )
        if not policy_current:
            return RestartAction.POLICY_BLOCKED
        if not calendar_current:
            return RestartAction.CALENDAR_BLOCKED
        if request_status is RequestInstanceStatus.RETRY_WAIT:
            if latest is None or latest.status is not AttemptStatus.RETRYABLE_FAILED:
                raise RestartProjectionIntegrityError("retry state lacks a retryable attempt")
            if (
                next_eligible_at is None
                or latest.next_eligible_at != next_eligible_at
                or retry_count != latest.attempt_number
                or retry_count >= max_attempts
            ):
                raise RestartProjectionIntegrityError(
                    "retry state is inconsistent with attempt history or limits"
                )
            return (
                RestartAction.RETRY_DISPATCH
                if next_eligible_at <= evaluated_at
                else RestartAction.WAIT_RETRY
            )
        if request_status is RequestInstanceStatus.PLANNED:
            if latest is not None:
                raise RestartProjectionIntegrityError("planned request unexpectedly has an attempt")
            return RestartAction.DISPATCH
        if request_status in {
            RequestInstanceStatus.DISPATCHING,
            RequestInstanceStatus.ACQUIRING,
        }:
            if (
                latest is None
                or latest.status is not AttemptStatus.RUNNING
                or latest.acquisition_authorization is not None
            ):
                raise RestartProjectionIntegrityError("acquisition restart state is inconsistent")
            return RestartAction.RESUME_ACQUISITION
        if request_status in {RequestInstanceStatus.RAW_COMPLETE, RequestInstanceStatus.PROCESSING}:
            if (
                latest is None
                or latest.status is not AttemptStatus.RAW_COMPLETE
                or latest.acquisition_authorization is None
            ):
                raise RestartProjectionIntegrityError("raw replay state lacks acquisition proof")
            if publication is None:
                return RestartAction.REPLAY_RAW
            if publication.state is PublicationRecoveryState.CONTEXT_ONLY:
                return RestartAction.RESUME_PROCESSING
            if publication.state is PublicationRecoveryState.PREPARED:
                return RestartAction.ADOPT_PUBLICATION
            raise RestartProjectionIntegrityError(
                "unfinished request has terminal publication state"
            )
        raise RestartProjectionIntegrityError("request state has no safe restart action")

    def _load_stream_proofs(
        self,
        connection: sqlite3.Connection,
        stream_ids: tuple[str, ...],
    ) -> StreamProofProjection:
        placeholders = ",".join("?" for _ in stream_ids)
        stream_rows = connection.execute(
            f"SELECT * FROM stream_keys WHERE stream_id IN ({placeholders}) ORDER BY stream_id",
            stream_ids,
        ).fetchall()
        if tuple(str(row["stream_id"]) for row in stream_rows) != stream_ids:
            raise RestartProjectionIntegrityError("selected stream is not cataloged")
        streams = {
            str(row["stream_id"]): _parse_stream(str(row["dimensions_json"])) for row in stream_rows
        }
        if any(stream.stream_id != key for key, stream in streams.items()):
            raise RestartProjectionIntegrityError("selected stream identity is inconsistent")

        coverage_rows = connection.execute(
            f"""
            SELECT * FROM coverage_segments
            WHERE stream_id IN ({placeholders})
            ORDER BY stream_id, interval_start, interval_end, coverage_id
            """,
            stream_ids,
        ).fetchall()
        coverage = tuple(self._load_coverage_segment(connection, row) for row in coverage_rows)
        coverage_groups: dict[tuple[str, str], list[CoverageSegment]] = {}
        for segment in coverage:
            coverage_groups.setdefault((segment.canonical_batch_id, segment.stream_id), []).append(
                segment
            )
        for (batch_id, stream_id), segments in coverage_groups.items():
            ordered = sorted(segments, key=lambda value: (value.start, value.end))
            if any(left.end > right.start for left, right in pairwise(ordered)):
                raise RestartProjectionIntegrityError("coverage proof intervals overlap")
            row_count = connection.execute(
                """
                SELECT row_count FROM canonical_batch_streams
                WHERE canonical_batch_id = ? AND stream_id = ?
                """,
                (batch_id, stream_id),
            ).fetchone()
            if row_count is None or sum(value.row_count for value in segments) != int(row_count[0]):
                raise RestartProjectionIntegrityError("coverage row proof is incomplete")
        gap_rows = connection.execute(
            f"""
            SELECT * FROM gaps WHERE stream_id IN ({placeholders})
            ORDER BY stream_id, interval_start, interval_end, gap_id
            """,
            stream_ids,
        ).fetchall()
        gaps = tuple(self._load_gap(connection, row) for row in gap_rows)
        watermark_rows = connection.execute(
            f"""
            SELECT * FROM watermarks WHERE stream_id IN ({placeholders}) ORDER BY stream_id
            """,
            stream_ids,
        ).fetchall()
        watermarks = tuple(
            self._load_watermark(
                connection,
                row,
                stream=streams[str(row["stream_id"])],
                coverage=coverage,
                gaps=gaps,
            )
            for row in watermark_rows
        )
        return StreamProofProjection(
            stream_ids=stream_ids,
            coverage=coverage,
            gaps=gaps,
            watermarks=watermarks,
        )

    def _load_coverage_segment(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> CoverageSegment:
        proof = connection.execute(
            """
            SELECT proof.*, acquisition.page_count AS acquisition_page_count,
                   acquisition.pagination_complete AS acquisition_pagination_complete,
                   acquisition.terminal_page_verified AS acquisition_terminal_verified,
                   acquisition.authorization_hash AS acquisition_authorization_hash,
                   instance.status AS request_status, terminal.terminal_status,
                   terminal.canonical_batch_id AS terminal_batch_id,
                   terminal.coverage_commit_hash,
                   publication.coverage_commit_hash AS publication_coverage_hash,
                   batch.state AS batch_state, batch.row_count AS batch_row_count,
                   batch_stream.outcome, batch_stream.row_count AS stream_row_count,
                   batch_stream.interval_start AS stream_start,
                   batch_stream.interval_end AS stream_end,
                   calendar.schedule_checksum, calendar.state AS calendar_state,
                   policy.policy_id, policy.revision, policy.policy_hash,
                   plan.catalog_snapshot_id,
                   active.status AS active_policy_status,
                   active.policy_snapshot_id AS active_policy_snapshot_id
            FROM coverage_request_proofs AS proof
            JOIN attempt_acquisition_records AS acquisition
              ON acquisition.attempt_id = proof.attempt_id
            JOIN request_instances AS instance
              ON instance.request_instance_id = proof.request_instance_id
            JOIN ingestion_plan_records AS plan ON plan.run_id = instance.run_id
            JOIN request_terminal_proofs AS terminal
              ON terminal.request_instance_id = proof.request_instance_id
            JOIN publication_commits AS publication
              ON publication.request_instance_id = proof.request_instance_id
             AND publication.canonical_batch_id = terminal.canonical_batch_id
            JOIN canonical_batches AS batch
              ON batch.canonical_batch_id = terminal.canonical_batch_id
            JOIN canonical_batch_streams AS batch_stream
              ON batch_stream.canonical_batch_id = batch.canonical_batch_id
             AND batch_stream.stream_id = ?
            JOIN canonical_batch_manifests AS manifest
              ON manifest.canonical_batch_id = batch.canonical_batch_id
            JOIN calendar_snapshots AS calendar
              ON calendar.calendar_snapshot_id = ?
            JOIN policy_snapshots AS policy
              ON policy.policy_snapshot_id = ?
            LEFT JOIN dataset_policy_status AS active
              ON active.provider = policy.provider AND active.dataset = policy.dataset
            WHERE proof.coverage_id = ?
            """,
            (
                str(row["stream_id"]),
                str(row["calendar_snapshot_id"]),
                str(row["policy_snapshot_id"]),
                str(row["coverage_id"]),
            ),
        ).fetchone()
        if proof is None:
            raise RestartProjectionIntegrityError("coverage proof graph is incomplete")
        proof_payload = {
            "coverage_id": str(proof["coverage_id"]),
            "request_instance_id": str(proof["request_instance_id"]),
            "attempt_id": str(proof["attempt_id"]),
            "authorization_hash": str(proof["authorization_hash"]),
            "request_terminal_state": str(proof["request_terminal_state"]),
            "stream_outcome": str(proof["stream_outcome"]),
            "terminal_page_verified": _as_bool(proof["terminal_page_verified"]),
            "canonical_batch_verified": _as_bool(proof["canonical_batch_verified"]),
            "canonical_file_count": int(proof["canonical_file_count"]),
            "raw_artifact_count": int(proof["raw_artifact_count"]),
            "relational_provenance_verified": _as_bool(proof["relational_provenance_verified"]),
            "provider_semantics_version": proof["provider_semantics_version"],
        }
        canonical_file_count = int(
            connection.execute(
                "SELECT count(*) FROM canonical_files WHERE canonical_batch_id = ?",
                (str(row["canonical_batch_id"]),),
            ).fetchone()[0]
        )
        raw_count = int(
            connection.execute(
                "SELECT count(*) FROM acquisition_artifacts WHERE attempt_id = ?",
                (str(proof["attempt_id"]),),
            ).fetchone()[0]
        )
        raw_files = connection.execute(
            """
            SELECT raw.relative_path, raw.content_sha256, raw.byte_count, raw.state,
                   raw.manifest_relative_path, manifest.manifest_content_sha256,
                   manifest.manifest_byte_count
            FROM acquisition_artifacts AS link
            JOIN raw_artifacts AS raw ON raw.artifact_id = link.artifact_id
            JOIN raw_artifact_manifests AS manifest ON manifest.artifact_id = raw.artifact_id
            JOIN raw_replay_provenance AS replay
              ON replay.attempt_id = link.attempt_id AND replay.artifact_id = link.artifact_id
            WHERE link.attempt_id = ? ORDER BY link.ordinal
            """,
            (str(proof["attempt_id"]),),
        ).fetchall()
        raw_files_present = len(raw_files) == raw_count and all(
            str(value["state"]) == "VERIFIED"
            and self._store._managed_file_matches_catalog(
                str(value["relative_path"]),
                expected_sha256=str(value["content_sha256"]),
                expected_bytes=int(value["byte_count"]),
            )
            and self._store._managed_file_matches_catalog(
                str(value["manifest_relative_path"]),
                expected_sha256=str(value["manifest_content_sha256"]),
                expected_bytes=int(value["manifest_byte_count"]),
            )
            for value in raw_files
        )
        canonical_files = connection.execute(
            """
            SELECT file.relative_path, file.content_sha256, file.byte_count,
                   batch.manifest_relative_path, manifest.manifest_content_sha256,
                   manifest.manifest_byte_count
            FROM canonical_files AS file
            JOIN canonical_batches AS batch
              ON batch.canonical_batch_id = file.canonical_batch_id
            JOIN canonical_batch_manifests AS manifest
              ON manifest.canonical_batch_id = batch.canonical_batch_id
            WHERE file.canonical_batch_id = ? ORDER BY file.file_ordinal
            """,
            (str(row["canonical_batch_id"]),),
        ).fetchall()
        canonical_files_present = len(canonical_files) == canonical_file_count and all(
            self._store._managed_file_matches_catalog(
                str(value["relative_path"]),
                expected_sha256=str(value["content_sha256"]),
                expected_bytes=int(value["byte_count"]),
            )
            and self._store._managed_file_matches_catalog(
                str(value["manifest_relative_path"]),
                expected_sha256=str(value["manifest_content_sha256"]),
                expected_bytes=int(value["manifest_byte_count"]),
            )
            for value in canonical_files
        )
        policy, policy_current = self._load_policy(
            connection,
            policy_snapshot_id=str(row["policy_snapshot_id"]),
            catalog_snapshot_id=str(proof["catalog_snapshot_id"]),
        )
        calendar, calendar_current = self._load_calendar(
            connection,
            str(row["calendar_snapshot_id"]),
        )
        if (
            _hash_json(proof_payload) != str(proof["proof_hash"])
            or str(row["canonical_batch_id"]) != str(proof["terminal_batch_id"])
            or str(proof["coverage_commit_hash"]) != str(proof["publication_coverage_hash"])
            or str(proof["authorization_hash"]) != str(proof["acquisition_authorization_hash"])
            or str(proof["request_status"]) != str(proof["request_terminal_state"])
            or str(proof["terminal_status"]) != str(proof["request_terminal_state"])
            or str(proof["batch_state"]) != "VERIFIED"
            or str(proof["outcome"]) != "PUBLISHABLE"
            or not _as_bool(proof["acquisition_pagination_complete"])
            or not _as_bool(proof["acquisition_terminal_verified"])
            or int(proof["acquisition_page_count"]) != raw_count
            or canonical_file_count != int(proof["canonical_file_count"])
            or raw_count != int(proof["raw_artifact_count"])
            or int(row["row_count"]) > int(proof["stream_row_count"])
            or proof["provider_semantics_version"] != row["provider_semantics_version"]
            or policy.policy_id != str(proof["policy_id"])
            or policy.policy_revision != int(proof["revision"])
            or policy.policy_hash != str(proof["policy_hash"])
            or calendar.checksum != str(proof["schedule_checksum"])
            or _parse_utc(str(row["interval_start"])) < _parse_utc(str(proof["stream_start"]))
            or _parse_utc(str(row["interval_end"])) > _parse_utc(str(proof["stream_end"]))
        ):
            raise RestartProjectionIntegrityError("coverage proof graph is inconsistent")
        verified = str(row["verification_state"]) == CoverageVerificationState.VERIFIED.value
        retained = _as_bool(row["retained"])
        artifacts_present = (
            raw_files_present and raw_count > 0 and int(row["artifact_count"]) == raw_count
        )
        if (
            verified
            and retained
            and (
                not calendar_current
                or not policy_current
                or str(proof["calendar_state"]) != "CURRENT"
                or str(proof["active_policy_status"]) != DatasetPolicyStatus.ACTIVE.value
                or str(proof["active_policy_snapshot_id"]) != str(row["policy_snapshot_id"])
                or not artifacts_present
                or not canonical_files_present
            )
        ):
            raise RestartProjectionIntegrityError("verified coverage uses stale policy or calendar")
        return CoverageSegment(
            coverage_id=str(row["coverage_id"]),
            stream_id=str(row["stream_id"]),
            canonical_batch_id=str(row["canonical_batch_id"]),
            calendar_snapshot_id=str(row["calendar_snapshot_id"]),
            calendar_snapshot_checksum=_validate_sha256(proof["schedule_checksum"], platform=True),
            policy_snapshot_id=str(row["policy_snapshot_id"]),
            policy_id=str(proof["policy_id"]),
            policy_revision=int(proof["revision"]),
            policy_hash=_validate_sha256(proof["policy_hash"]),
            coverage_start=_parse_utc(str(row["coverage_start"])),
            start=_parse_utc(str(row["interval_start"])),
            end=_parse_utc(str(row["interval_end"])),
            classification=CoverageClassification(str(row["classification"])),
            verification_state=CoverageVerificationState(str(row["verification_state"])),
            retained=retained,
            row_count=int(row["row_count"]),
            artifact_count=int(row["artifact_count"]),
            artifacts_present=artifacts_present,
            artifact_integrity_verified=artifacts_present,
            interval_verified=True,
            request_completed=_as_bool(row["request_completed"]),
            request_terminal_state=CoverageRequestTerminalState(
                str(proof["request_terminal_state"])
            ),
            stream_outcome=CoverageStreamOutcome(str(proof["stream_outcome"])),
            pagination_verified=_as_bool(row["pagination_verified"]),
            terminal_page_verified=_as_bool(proof["terminal_page_verified"]),
            canonical_batch_verified=(
                _as_bool(proof["canonical_batch_verified"]) and canonical_files_present
            ),
            canonical_file_count=canonical_file_count,
            raw_artifact_count=raw_count,
            relational_provenance_verified=_as_bool(proof["relational_provenance_verified"]),
            provider_semantics_version=(
                None
                if row["provider_semantics_version"] is None
                else str(row["provider_semantics_version"])
            ),
            generation=int(row["generation"]),
            verified_at=_parse_utc(str(row["verified_at"])),
            invalidated_at=_parse_optional_utc(row["invalidated_at"]),
        )

    @staticmethod
    def _load_gap(connection: sqlite3.Connection, row: sqlite3.Row) -> GapFinding:
        request_id = row["request_instance_id"]
        if request_id is None:
            raise RestartProjectionIntegrityError("gap lacks request provenance")
        request_scope = connection.execute(
            """
            SELECT 1 FROM request_instances AS instance
            JOIN request_spec_streams AS link ON link.request_spec_id = instance.request_spec_id
            WHERE instance.request_instance_id = ? AND link.stream_id = ?
            """,
            (str(request_id), str(row["stream_id"])),
        ).fetchone()
        if request_scope is None:
            raise RestartProjectionIntegrityError("gap request provenance is inconsistent")
        batch_id = row["canonical_batch_id"]
        if batch_id is not None:
            batch_scope = connection.execute(
                """
                SELECT 1 FROM canonical_batch_streams
                WHERE canonical_batch_id = ? AND stream_id = ?
                """,
                (str(batch_id), str(row["stream_id"])),
            ).fetchone()
            if batch_scope is None:
                raise RestartProjectionIntegrityError("gap batch provenance is inconsistent")
        return GapFinding(
            gap_id=str(row["gap_id"]),
            stream_id=str(row["stream_id"]),
            start=_parse_utc(str(row["interval_start"])),
            end=_parse_utc(str(row["interval_end"])),
            gap_type=GapType(str(row["gap_type"])),
            status=GapStatus(str(row["status"])),
            blocking=_as_bool(row["blocking"]),
            detected_at=_parse_utc(str(row["detected_at"])),
            resolved_at=_parse_optional_utc(row["resolved_at"]),
            request_instance_id=str(request_id),
            canonical_batch_id=None if batch_id is None else str(batch_id),
        )

    def _load_watermark(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        stream: StreamKey,
        coverage: tuple[CoverageSegment, ...],
        gaps: tuple[GapFinding, ...],
    ) -> MaterializedWatermark:
        watermark = MaterializedWatermark(
            stream_id=str(row["stream_id"]),
            coverage_start=_parse_utc(str(row["coverage_start"])),
            exclusive_frontier=_parse_utc(str(row["exclusive_frontier"])),
            verification_state=CoverageVerificationState(str(row["verification_state"])),
            generation=int(row["generation"]),
            calendar_snapshot_id=str(row["calendar_snapshot_id"]),
            policy_snapshot_id=str(row["policy_snapshot_id"]),
            last_run_id=str(row["last_run_id"]),
            last_batch_id=str(row["last_batch_id"]),
            last_verified_session=date.fromisoformat(str(row["last_verified_session"])),
            blocking_gap_count=int(row["blocking_gap_count"]),
            computed_at=_parse_utc(str(row["computed_at"])),
            invalidated_at=_parse_optional_utc(row["invalidated_at"]),
        )
        relational = connection.execute(
            """
            SELECT 1 FROM publication_commits AS publication
            JOIN request_instances AS instance
              ON instance.request_instance_id = publication.request_instance_id
            JOIN canonical_batches AS batch
              ON batch.canonical_batch_id = publication.canonical_batch_id
            WHERE instance.run_id = ? AND publication.canonical_batch_id = ?
              AND batch.state IN ('VERIFIED', 'INVALID', 'PURGED')
            """,
            (watermark.last_run_id, watermark.last_batch_id),
        ).fetchone()
        if relational is None:
            raise RestartProjectionIntegrityError("watermark publication provenance is absent")
        if watermark.verification_state is not CoverageVerificationState.VERIFIED:
            return watermark
        calendar, current = self._load_calendar(connection, watermark.calendar_snapshot_id)
        policy, policy_current = self._load_policy(
            connection,
            policy_snapshot_id=watermark.policy_snapshot_id,
            catalog_snapshot_id=str(
                connection.execute(
                    """
                    SELECT catalog_snapshot_id FROM ingestion_plan_records
                    WHERE run_id = ?
                    """,
                    (watermark.last_run_id,),
                ).fetchone()[0]
            ),
        )
        if (
            not current
            or not policy_current
            or (
                policy.provider,
                policy.dataset,
            )
            != (stream.provider, stream.dataset)
        ):
            raise RestartProjectionIntegrityError(
                "verified watermark uses stale policy or calendar"
            )
        supporting = tuple(
            segment
            for segment in coverage
            if segment.stream_id == watermark.stream_id
            and segment.calendar_snapshot_id == watermark.calendar_snapshot_id
            and segment.policy_snapshot_id == watermark.policy_snapshot_id
            and segment.verification_state is CoverageVerificationState.VERIFIED
            and segment.retained
            and segment.start < watermark.exclusive_frontier
            and segment.end > watermark.coverage_start
        )
        eligible_slots = calendar.expected_slots(stream.timeframe)
        slots = tuple(
            slot
            for slot in eligible_slots
            if watermark.coverage_start <= slot.start_utc
            and slot.end_utc <= watermark.exclusive_frontier
        )
        frontier_is_eligible_boundary = bool(slots) and (
            slots[-1].end_utc == watermark.exclusive_frontier
            or any(slot.start_utc == watermark.exclusive_frontier for slot in eligible_slots)
        )
        if (
            not supporting
            or not slots
            or slots[0].start_utc != watermark.coverage_start
            or not frontier_is_eligible_boundary
            or watermark.last_batch_id not in {segment.canonical_batch_id for segment in supporting}
            or any(
                not any(
                    segment.start <= slot.start_utc and segment.end >= slot.end_utc
                    for segment in supporting
                )
                for slot in slots
            )
            or slots[-1].session_date != watermark.last_verified_session
        ):
            raise RestartProjectionIntegrityError("watermark lacks contiguous calendar coverage")
        active_gaps = tuple(
            gap for gap in gaps if gap.stream_id == watermark.stream_id and gap.actively_blocks
        )
        prefix_gaps = tuple(
            gap
            for gap in active_gaps
            if gap.start < watermark.exclusive_frontier and gap.end > watermark.coverage_start
        )
        if len(active_gaps) != watermark.blocking_gap_count or prefix_gaps:
            raise RestartProjectionIntegrityError("verified watermark disagrees with blocking gaps")
        return watermark


__all__ = [
    "AttemptStatus",
    "PublicationRecoveryState",
    "RestartAction",
    "RestartAttemptProjection",
    "RestartProjectionError",
    "RestartProjectionIntegrityError",
    "RestartProjectionReader",
    "RestartPublicationProjection",
    "RestartRequestProjection",
    "RestartRunContext",
    "RestartRunNotFoundError",
    "StreamProofProjection",
]
