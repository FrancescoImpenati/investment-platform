"""Typed cross-run raw replay and state-first canonical-loss reconciliation.

This repository owns operational metadata only.  It never calls a provider and never removes or
rewrites a filesystem object.  Raw reuse is permitted only after the immutable payload and
manifest still match their catalog; canonical reactivation is permitted only after the exact
original bytes and semantics have been republished and verified.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Annotated, Any, Self, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.ingestion.identity import (
    IDENTITY_CANONICALIZATION_VERSION,
    AttemptIdentity,
    BatchContext,
    CanonicalBatchIdentity,
    ProcessingSignature,
    RawArtifactIdentity,
    RequestSpecification,
)
from investment_platform.data.operational.execution import RawReplayRecord
from investment_platform.data.operational.planning import (
    RequestInstanceStatus,
    deterministic_policy_snapshot_id,
)
from investment_platform.data.operational.store import (
    OperationalStateError,
    OperationalStateStore,
    WriterLease,
    _format_utc,
    _parse_utc,
)
from investment_platform.data.provenance import RawBatchMetadata
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    DatasetPolicyStatus,
    RetentionMode,
)
from investment_platform.data.storage.canonical_batches import (
    CanonicalBatchExpectation,
    CanonicalBatchManifest,
    CanonicalPublicationProvenance,
    PublishedCanonicalBatch,
)

_SHA256 = r"^[0-9a-f]{64}$"
_DURABLE_ID = r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,191}$"
_RAW_ID = r"^raw_v1_[0-9a-f]{64}$"
_BATCH_ID = r"^batch_v1_[0-9a-f]{64}$"


class OperationalReplayError(OperationalStateError):
    """Base error for cross-run raw adoption and canonical-loss recovery."""


class ReplayStateConflictError(OperationalReplayError):
    """Durable workflow state cannot accept the requested replay transition."""


class ReplayIdentityCollisionError(OperationalReplayError):
    """A durable identity is already bound to different replay semantics."""


class ReplayIntegrityError(OperationalReplayError):
    """Retained bytes, provenance, policy, or canonical metadata failed verification."""


class CanonicalLossState(StrEnum):
    HEALTHY = "HEALTHY"
    INVALIDATED = "INVALIDATED"
    ALREADY_INVALID = "ALREADY_INVALID"


class CanonicalLossTargetType(StrEnum):
    MANIFEST = "MANIFEST"
    PARQUET_FILE = "PARQUET_FILE"
    RAW_PAYLOAD = "RAW_PAYLOAD"
    RAW_MANIFEST = "RAW_MANIFEST"


class CanonicalLossCondition(StrEnum):
    ABSENT = "ABSENT"
    CORRUPT = "CORRUPT"


class RawReplayReason(StrEnum):
    """Durable operational reason that can justify a zero-network raw replay."""

    CANONICAL_LOSS = "CANONICAL_LOSS"
    CANONICAL_STALE = "CANONICAL_STALE"


class RawReplayOperationStatus(StrEnum):
    PLANNED = "PLANNED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class _FrozenReplayModel(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class RawReplayEligibility(_FrozenReplayModel):
    """Caller-supplied selector whose complete evidence is re-proven from SQLite."""

    reason: RawReplayReason
    canonical_batch_id: str = Field(pattern=_BATCH_ID)
    evidence_gap_ids: Annotated[tuple[str, ...], Field(min_length=1)]

    @field_validator("evidence_gap_ids", mode="after")
    @classmethod
    def require_canonical_gap_order(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item.strip() for item in value):
            raise ValueError("evidence gap ids cannot be blank")
        if value != tuple(sorted(set(value))):
            raise ValueError("evidence gap ids must be unique and sorted")
        return value


class ReplayableAcquisition(_FrozenReplayModel):
    """One complete retained acquisition safe to adopt without a provider call."""

    source_attempt_id: UUID
    source_request_instance_id: UUID
    specification: RequestSpecification
    eligibility: RawReplayEligibility
    source_authorization: AcquisitionPolicyAuthorization
    ordered_raw: Annotated[tuple[RawReplayRecord, ...], Field(min_length=1)]
    completed_at: datetime

    @field_validator("completed_at", mode="after")
    @classmethod
    def normalize_completed_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="completed_at")

    @model_validator(mode="after")
    def validate_exact_acquisition(self) -> Self:
        authorization = self.source_authorization
        if authorization.request.request_spec_hash != self.specification.request_spec_hash:
            raise ValueError("source acquisition belongs to another request specification")
        identities = _raw_identities(authorization)
        if tuple(value.artifact_id for value in identities) != tuple(
            value.artifact_id for value in self.ordered_raw
        ):
            raise ValueError("source raw replay order differs from acquisition authorization")
        if any(value.attempt_id != self.source_attempt_id for value in self.ordered_raw):
            raise ValueError("raw replay records belong to another source attempt")
        return self

    @property
    def ordered_artifact_ids(self) -> tuple[str, ...]:
        return tuple(value.artifact_id for value in self.ordered_raw)


class AdoptedAcquisition(_FrozenReplayModel):
    attempt_id: UUID
    request_instance_id: UUID
    source_attempt_id: UUID
    eligibility: RawReplayEligibility
    authorization_hash: str = Field(pattern=_SHA256)
    ordered_artifact_ids: tuple[str, ...]
    adopted_at: datetime
    replayed: bool

    @field_validator("adopted_at", mode="after")
    @classmethod
    def normalize_adopted_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="adopted_at")


class CanonicalLossTarget(_FrozenReplayModel):
    target_type: CanonicalLossTargetType
    relative_path: str
    condition: CanonicalLossCondition


class CanonicalLossReconciliation(_FrozenReplayModel):
    canonical_batch_id: str = Field(pattern=_BATCH_ID)
    batch_context_id: str = Field(pattern=r"^batch_context_v1_[0-9a-f]{64}$")
    request_specification: RequestSpecification
    expectation: CanonicalBatchExpectation
    state: CanonicalLossState
    targets: tuple[CanonicalLossTarget, ...]
    affected_stream_ids: tuple[str, ...]
    integrity_gap_ids: tuple[str, ...]
    replay_eligibility: RawReplayEligibility | None
    invalidated_at: datetime | None
    replayed: bool

    @field_validator("invalidated_at", mode="after")
    @classmethod
    def normalize_invalidated_at(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _as_utc(value, label="invalidated_at")

    @model_validator(mode="after")
    def require_loss_evidence(self) -> Self:
        if self.state is CanonicalLossState.HEALTHY:
            if self.replay_eligibility is not None:
                raise ValueError("healthy canonical state cannot authorize raw replay")
        elif self.replay_eligibility is None:
            raise ValueError("invalid canonical state requires durable replay eligibility")
        return self


class CanonicalReactivationResult(_FrozenReplayModel):
    canonical_batch_id: str = Field(pattern=_BATCH_ID)
    attempt_id: UUID
    request_instance_id: UUID
    state: str
    restored_coverage_count: Annotated[int, Field(ge=0)]
    restored_watermark_count: Annotated[int, Field(ge=0)]
    resolved_gap_count: Annotated[int, Field(ge=0)]
    verified_at: datetime
    replayed: bool

    @field_validator("verified_at", mode="after")
    @classmethod
    def normalize_verified_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="verified_at")


class PersistedBatchPreparation(_FrozenReplayModel):
    """Exact batch identity, timestamps, and provenance reloaded after durable recording."""

    specification: RequestSpecification
    batch_context: BatchContext
    provenance: CanonicalPublicationProvenance
    calendar_snapshot_id: str = Field(pattern=_DURABLE_ID)
    recorded_at: datetime

    @field_validator("recorded_at", mode="after")
    @classmethod
    def normalize_recorded_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="recorded_at")


class RawReplayOperation(_FrozenReplayModel):
    operation_id: UUID
    specification: RequestSpecification
    eligibility: RawReplayEligibility
    status: RawReplayOperationStatus
    requested_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result_hash: str | None = Field(default=None, pattern=_SHA256)
    error_code: str | None = None
    sanitized_error: str | None = None

    @field_validator("requested_at", "started_at", "completed_at", mode="after")
    @classmethod
    def normalize_operation_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _as_utc(value, label="operation timestamp")


class RawReplayOperationResult(_FrozenReplayModel):
    operation_id: UUID
    canonical_batch_id: str = Field(pattern=_BATCH_ID)
    restored_coverage_count: Annotated[int, Field(ge=0)]
    restored_watermark_count: Annotated[int, Field(ge=0)]
    resolved_gap_count: Annotated[int, Field(ge=0)]
    completed_at: datetime
    originating_coverage_commit_hash: str = Field(pattern=_SHA256)
    result_hash: str = Field(pattern=_SHA256)
    replayed: bool

    @field_validator("completed_at", mode="after")
    @classmethod
    def normalize_completed_operation_time(cls, value: datetime) -> datetime:
        return _as_utc(value, label="completed_at")


def _as_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _canonical_json(value: object) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


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


def _parse_request_specification(value: str) -> RequestSpecification:
    envelope = _strict_json(value)
    if not isinstance(envelope, dict) or set(envelope) != {
        "canonicalization_version",
        "kind",
        "payload",
    }:
        raise ValueError("request specification envelope is invalid")
    if (
        envelope["canonicalization_version"] != IDENTITY_CANONICALIZATION_VERSION
        or envelope["kind"] != "request-specification"
        or not isinstance(envelope["payload"], dict)
    ):
        raise ValueError("request specification envelope is inconsistent")
    specification = RequestSpecification.model_validate(envelope["payload"])
    if specification.canonical_json != value:
        raise ValueError("request specification JSON is not canonical")
    return specification


def _hash_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _ordered_artifacts_hash(artifact_ids: tuple[str, ...]) -> str:
    return _hash_json(
        {
            "canonicalization_version": 1,
            "kind": "ordered-raw-artifacts",
            "payload": {"artifact_ids": artifact_ids},
        }
    )


def _raw_identities(
    authorization: AcquisitionPolicyAuthorization,
) -> tuple[RawArtifactIdentity, ...]:
    return tuple(
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


def _gap_id(
    *,
    canonical_batch_id: str,
    stream_id: str,
    start: str,
    end: str,
) -> str:
    digest = _hash_json(
        {
            "canonical_batch_id": canonical_batch_id,
            "end": end,
            "kind": "canonical-loss-integrity-gap",
            "start": start,
            "stream_id": stream_id,
            "version": 1,
        }
    )
    return f"gap-integrity-{digest}"


class OperationalReplayRepository:
    """Lease-fenced raw adoption and exact canonical-loss state transitions."""

    def __init__(self, store: OperationalStateStore) -> None:
        self._store = store

    def find_replay_eligibility(
        self,
        specification: RequestSpecification,
    ) -> RawReplayEligibility | None:
        """Derive one unambiguous canonical-only repair proof for an exact work item."""

        with self._store.read_only_connection() as connection:
            self._require_exact_active_specification(connection, specification)
            batch_rows = connection.execute(
                """
                SELECT batch.canonical_batch_id, batch.state
                FROM canonical_batches AS batch
                JOIN batch_contexts AS context
                  ON context.batch_context_id = batch.batch_context_id
                WHERE context.request_spec_id = ? AND batch.state <> 'PURGED'
                ORDER BY batch.canonical_batch_id
                """,
                (specification.request_spec_id,),
            ).fetchall()
            candidates: list[RawReplayEligibility] = []
            for row in batch_rows:
                canonical_batch_id = str(row["canonical_batch_id"])
                gap_rows = connection.execute(
                    """
                    SELECT gap_id, gap_type FROM gaps
                    WHERE canonical_batch_id = ? AND blocking = 1
                      AND status IN ('OPEN', 'REPAIRING')
                      AND gap_type IN ('INTEGRITY', 'CALENDAR_STALE')
                    ORDER BY gap_type, gap_id
                    """,
                    (canonical_batch_id,),
                ).fetchall()
                by_type = {
                    gap_type: tuple(
                        sorted(
                            str(gap["gap_id"])
                            for gap in gap_rows
                            if str(gap["gap_type"]) == gap_type
                        )
                    )
                    for gap_type in ("INTEGRITY", "CALENDAR_STALE")
                }
                if str(row["state"]) == "INVALID" and by_type["INTEGRITY"]:
                    candidates.append(
                        RawReplayEligibility(
                            reason=RawReplayReason.CANONICAL_LOSS,
                            canonical_batch_id=canonical_batch_id,
                            evidence_gap_ids=by_type["INTEGRITY"],
                        )
                    )
                stale_count = int(
                    connection.execute(
                        """
                        SELECT count(*) FROM coverage_segments
                        WHERE canonical_batch_id = ? AND verification_state = 'STALE'
                          AND retained = 1
                        """,
                        (canonical_batch_id,),
                    ).fetchone()[0]
                )
                if stale_count > 0 and by_type["CALENDAR_STALE"]:
                    candidates.append(
                        RawReplayEligibility(
                            reason=RawReplayReason.CANONICAL_STALE,
                            canonical_batch_id=canonical_batch_id,
                            evidence_gap_ids=by_type["CALENDAR_STALE"],
                        )
                    )
            if not candidates:
                return None
            if len(candidates) != 1:
                raise ReplayStateConflictError(
                    "exact request has ambiguous canonical-only replay evidence"
                )
            eligibility = candidates[0]
            self._require_replay_eligibility(connection, specification, eligibility)
            return eligibility

    def load_adopted_replay(self, attempt_id: UUID) -> ReplayableAcquisition | None:
        """Reload a durable adoption and its historical WHY for restart processing."""

        with self._store.read_only_connection() as connection:
            row = connection.execute(
                """
                SELECT adoption.*, spec.specification_json
                FROM raw_acquisition_adoptions AS adoption
                JOIN attempt_acquisition_records AS target
                  ON target.attempt_id = adoption.attempt_id
                JOIN request_specs AS spec ON spec.request_spec_id = target.request_spec_id
                WHERE adoption.attempt_id = ?
                """,
                (str(attempt_id),),
            ).fetchone()
            if row is None:
                return None
            try:
                eligibility = RawReplayEligibility.model_validate(
                    _strict_json(str(row["evidence_json"]))
                )
                specification = _parse_request_specification(str(row["specification_json"]))
            except ValueError as error:
                raise ReplayIntegrityError("durable raw adoption evidence is corrupt") from error
            if (
                _hash_json(eligibility.model_dump(mode="json")) != str(row["evidence_hash"])
                or eligibility.canonical_batch_id != str(row["canonical_batch_id"])
                or eligibility.reason.value != str(row["replay_reason"])
            ):
                raise ReplayIntegrityError("durable raw adoption evidence hash differs")
        return self._load_replay_attempt(
            UUID(str(row["source_attempt_id"])),
            specification,
            eligibility,
            require_active_eligibility=False,
        )

    def load_batch_preparation(
        self,
        batch_context_id: str,
    ) -> PersistedBatchPreparation | None:
        """Reload and verify the context contract persisted before normalization."""

        with self._store.read_only_connection() as connection:
            row = connection.execute(
                """
                SELECT context.*, spec.request_spec_hash, spec.specification_json,
                       contract.processing_signature_hash,
                       contract.processing_signature_json, contract.source_id,
                       contract.provenance_json, contract.provenance_hash,
                       contract.recorded_at
                FROM batch_contexts AS context
                JOIN request_specs AS spec ON spec.request_spec_id = context.request_spec_id
                JOIN batch_context_processing_contracts AS contract
                  ON contract.batch_context_id = context.batch_context_id
                WHERE context.batch_context_id = ?
                """,
                (batch_context_id,),
            ).fetchone()
            if row is None:
                return None
            artifacts = connection.execute(
                """
                SELECT link.ordinal, raw.* FROM batch_context_artifacts AS link
                JOIN raw_artifacts AS raw ON raw.artifact_id = link.artifact_id
                WHERE link.batch_context_id = ? ORDER BY link.ordinal
                """,
                (batch_context_id,),
            ).fetchall()
        try:
            specification = _parse_request_specification(str(row["specification_json"]))
            signature = ProcessingSignature.model_validate(
                _strict_json(str(row["processing_signature_json"]))
            )
            provenance = CanonicalPublicationProvenance.model_validate(
                _strict_json(str(row["provenance_json"]))
            )
            ordered_artifacts = tuple(
                RawArtifactIdentity.from_digest(
                    request_spec_hash=specification.request_spec_hash,
                    page_ordinal=int(artifact["page_ordinal"]),
                    media_type=str(artifact["media_type"]),
                    content_encoding=str(artifact["content_encoding"]),
                    content_sha256=str(artifact["content_sha256"]),
                    byte_count=int(artifact["byte_count"]),
                )
                for artifact in artifacts
            )
            context = BatchContext(
                batch_identity=CanonicalBatchIdentity(
                    request_spec_hash=specification.request_spec_hash,
                    ordered_artifacts=ordered_artifacts,
                    processing_signature=signature,
                ),
                fixed_ingested_at=_parse_utc(str(row["fixed_ingested_at"])),
                manifest_created_at=_parse_utc(str(row["manifest_created_at"])),
            )
        except ValueError as error:
            raise ReplayIntegrityError("persisted batch preparation is corrupt") from error
        if tuple(int(value["ordinal"]) for value in artifacts) != tuple(range(len(artifacts))):
            raise ReplayIntegrityError("persisted batch preparation artifact order is incomplete")
        actual = (
            specification.request_spec_id,
            specification.request_spec_hash,
            context.batch_context_id,
            context.canonical_batch_id,
            context.batch_identity.ordered_artifacts_hash,
            signature.canonical_schema_version,
            signature.normalizer_version,
            signature.validator_version,
            _hash_json(signature.model_dump(mode="json")),
            _canonical_json(signature),
            _hash_json(provenance.model_dump(mode="json")),
            _canonical_json(provenance),
            str(provenance.source_id),
            tuple(value.artifact_id for value in ordered_artifacts),
        )
        expected = (
            str(row["request_spec_id"]),
            str(row["request_spec_hash"]),
            str(row["batch_context_id"]),
            str(row["canonical_batch_id"]),
            str(row["ordered_artifacts_hash"]),
            str(row["canonical_schema_version"]),
            str(row["normalizer_version"]),
            str(row["validator_version"]),
            str(row["processing_signature_hash"]),
            str(row["processing_signature_json"]),
            str(row["provenance_hash"]),
            str(row["provenance_json"]),
            str(row["source_id"]),
            tuple(str(value["artifact_id"]) for value in artifacts),
        )
        if (
            actual != expected
            or not artifacts
            or any(str(value["state"]) != "VERIFIED" for value in artifacts)
            or any(
                value.artifact_id != str(row_value["artifact_id"])
                or value.page_relation_hash != str(row_value["page_relation_hash"])
                for value, row_value in zip(ordered_artifacts, artifacts, strict=True)
            )
        ):
            raise ReplayIdentityCollisionError(
                "persisted batch preparation differs from its durable identity"
            )
        return PersistedBatchPreparation(
            specification=specification,
            batch_context=context,
            provenance=provenance,
            calendar_snapshot_id=str(row["calendar_snapshot_id"]),
            recorded_at=_parse_utc(str(row["recorded_at"])),
        )

    def plan_raw_replay_operation(
        self,
        lease: WriterLease,
        operation_id: UUID,
        specification: RequestSpecification,
        *,
        requested_at: datetime | None = None,
    ) -> RawReplayOperation:
        """Create an exact zero-network canonical-loss work item, idempotently."""

        requested = (
            self._store._now()
            if requested_at is None
            else _as_utc(requested_at, label="requested_at")
        )
        eligibility = self.find_replay_eligibility(specification)
        if eligibility is None or eligibility.reason is not RawReplayReason.CANONICAL_LOSS:
            raise ReplayStateConflictError(
                "exact request lacks unambiguous canonical-loss replay evidence"
            )
        evidence_json = _canonical_json(eligibility)
        evidence_hash = _hash_json(eligibility.model_dump(mode="json"))
        with self._store._leased_transaction(lease) as connection:
            self._require_replay_eligibility(connection, specification, eligibility)
            existing = self._load_operation_row(connection, operation_id)
            origin_rows = connection.execute(
                """
                SELECT DISTINCT batch.policy_snapshot_id, run.environment
                FROM canonical_batches AS batch
                JOIN canonical_batch_requests AS link
                  ON link.canonical_batch_id = batch.canonical_batch_id
                JOIN request_instances AS request
                  ON request.request_instance_id = link.request_instance_id
                JOIN ingestion_runs AS run ON run.run_id = request.run_id
                WHERE batch.canonical_batch_id = ?
                """,
                (eligibility.canonical_batch_id,),
            ).fetchall()
            origins = {
                (str(value["policy_snapshot_id"]), str(value["environment"]))
                for value in origin_rows
            }
            if len(origins) != 1:
                raise ReplayIntegrityError(
                    "raw replay operation lacks one exact originating runtime/policy"
                )
            policy_snapshot_id, environment = next(iter(origins))
            values = (
                eligibility.canonical_batch_id,
                specification.request_spec_id,
                specification.provider,
                specification.dataset,
                _format_utc(specification.start),
                _format_utc(specification.end),
                eligibility.reason.value,
                evidence_json,
                evidence_hash,
            )
            if existing is None:
                prior_run = connection.execute(
                    "SELECT 1 FROM ingestion_runs WHERE run_id = ?",
                    (str(operation_id),),
                ).fetchone()
                if prior_run is not None:
                    raise ReplayIdentityCollisionError(
                        "raw replay operation UUID collides with another ingestion run"
                    )
                connection.execute(
                    """
                    INSERT INTO ingestion_runs(
                        run_id, mode, environment, provider, dataset, status,
                        policy_snapshot_id, created_at, planned_request_count,
                        succeeded_request_count, failed_request_count
                    ) VALUES (?, 'REPAIR', ?, ?, ?, 'PLANNED', ?, ?, 1, 0, 0)
                    """,
                    (
                        str(operation_id),
                        environment,
                        specification.provider,
                        specification.dataset,
                        policy_snapshot_id,
                        _format_utc(requested),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO raw_replay_operations(
                        operation_id, canonical_batch_id, request_spec_id, provider, dataset,
                        interval_start, interval_end, replay_reason, evidence_json,
                        evidence_hash, status, requested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PLANNED', ?)
                    """,
                    (str(operation_id), *values, _format_utc(requested)),
                )
            elif (
                tuple(
                    existing[column]
                    for column in (
                        "canonical_batch_id",
                        "request_spec_id",
                        "provider",
                        "dataset",
                        "interval_start",
                        "interval_end",
                        "replay_reason",
                        "evidence_json",
                        "evidence_hash",
                    )
                )
                != values
            ):
                raise ReplayIdentityCollisionError("raw replay operation identity collides")
        operation = self.load_raw_replay_operation(operation_id)
        if operation is None:
            raise ReplayIntegrityError("planned raw replay operation was not durable")
        return operation

    def load_raw_replay_operation(self, operation_id: UUID) -> RawReplayOperation | None:
        with self._store.read_only_connection() as connection:
            row = self._load_operation_row(connection, operation_id)
        return None if row is None else self._operation_from_row(row)

    def start_raw_replay_operation(
        self,
        lease: WriterLease,
        operation_id: UUID,
        *,
        started_at: datetime | None = None,
    ) -> RawReplayOperation:
        started = (
            self._store._now() if started_at is None else _as_utc(started_at, label="started_at")
        )
        with self._store._leased_transaction(lease) as connection:
            row = self._load_operation_row(connection, operation_id)
            if row is None:
                raise ReplayStateConflictError("raw replay operation does not exist")
            operation = self._operation_from_row(row)
            if operation.status is RawReplayOperationStatus.PLANNED:
                self._require_exact_active_specification(connection, operation.specification)
                self._require_replay_eligibility(
                    connection,
                    operation.specification,
                    operation.eligibility,
                )
                connection.execute(
                    """
                    UPDATE raw_replay_operations SET status = 'RUNNING', started_at = ?
                    WHERE operation_id = ? AND status = 'PLANNED'
                    """,
                    (_format_utc(started), str(operation_id)),
                )
                connection.execute(
                    """
                    UPDATE ingestion_runs SET status = 'RUNNING', started_at = ?
                    WHERE run_id = ? AND status = 'PLANNED'
                    """,
                    (_format_utc(started), str(operation_id)),
                )
            elif operation.status not in {
                RawReplayOperationStatus.RUNNING,
                RawReplayOperationStatus.SUCCESS,
            }:
                raise ReplayStateConflictError("failed raw replay operation cannot restart")
        durable = self.load_raw_replay_operation(operation_id)
        if durable is None:
            raise ReplayIntegrityError("started raw replay operation disappeared")
        return durable

    def complete_raw_replay_operation(
        self,
        lease: WriterLease,
        operation_id: UUID,
        expectation: CanonicalBatchExpectation,
        manifest: CanonicalBatchManifest,
        published: PublishedCanonicalBatch,
        *,
        completed_at: datetime | None = None,
    ) -> RawReplayOperationResult:
        """Atomically finish an identical no-attempt canonical replay."""

        completed = (
            self._store._now()
            if completed_at is None
            else _as_utc(completed_at, label="completed_at")
        )
        self._verify_identical_republication(expectation, manifest, published)
        try:
            with self._store._leased_transaction(lease) as connection:
                operation_row = self._load_operation_row(connection, operation_id)
                if operation_row is None:
                    raise ReplayStateConflictError("raw replay operation does not exist")
                operation = self._operation_from_row(operation_row)
                if (
                    operation.specification != expectation.specification
                    or operation.eligibility.canonical_batch_id
                    != expectation.batch_context.canonical_batch_id
                ):
                    raise ReplayIdentityCollisionError(
                        "raw replay result differs from planned exact work item"
                    )
                coverage_rows = connection.execute(
                    """
                    SELECT DISTINCT coverage_commit_hash FROM publication_commits
                    WHERE canonical_batch_id = ? ORDER BY coverage_commit_hash
                    """,
                    (operation.eligibility.canonical_batch_id,),
                ).fetchall()
                coverage_hashes = tuple(
                    str(value["coverage_commit_hash"]) for value in coverage_rows
                )
                if len(coverage_hashes) != 1:
                    raise ReplayIntegrityError(
                        "dedicated replay lacks one exact originating terminal proof"
                    )
                result_hash = _hash_json(
                    {
                        "canonical_batch_id": expectation.batch_context.canonical_batch_id,
                        "kind": "dedicated-raw-replay-result",
                        "operation_id": str(operation_id),
                        "originating_coverage_commit_hash": coverage_hashes[0],
                        "request_spec_id": expectation.specification.request_spec_id,
                        "version": 1,
                    }
                )
                if operation.status is RawReplayOperationStatus.SUCCESS:
                    if operation.result_hash != result_hash or operation.completed_at is None:
                        raise ReplayIdentityCollisionError("raw replay terminal result differs")
                    return RawReplayOperationResult(
                        operation_id=operation_id,
                        canonical_batch_id=operation.eligibility.canonical_batch_id,
                        restored_coverage_count=0,
                        restored_watermark_count=0,
                        resolved_gap_count=0,
                        completed_at=operation.completed_at,
                        originating_coverage_commit_hash=coverage_hashes[0],
                        result_hash=result_hash,
                        replayed=True,
                    )
                if operation.status is not RawReplayOperationStatus.RUNNING:
                    raise ReplayStateConflictError("raw replay operation is not RUNNING")
                row = self._canonical_context_row(
                    connection,
                    operation.eligibility.canonical_batch_id,
                )
                self._assert_reconcilable_policy(row)
                (
                    effective_completed,
                    restored_coverage,
                    restored_watermarks,
                    resolved_gaps,
                    replayed,
                ) = self._restore_canonical_state(
                    connection,
                    row,
                    canonical_batch_id=operation.eligibility.canonical_batch_id,
                    verified=completed,
                )
                connection.execute(
                    """
                    UPDATE raw_replay_operations
                    SET status = 'SUCCESS', completed_at = ?, result_hash = ?
                    WHERE operation_id = ? AND status = 'RUNNING'
                    """,
                    (_format_utc(effective_completed), result_hash, str(operation_id)),
                )
                connection.execute(
                    """
                    UPDATE ingestion_runs
                    SET status = 'SUCCESS', completed_at = ?, succeeded_request_count = 1
                    WHERE run_id = ? AND status = 'RUNNING'
                    """,
                    (_format_utc(effective_completed), str(operation_id)),
                )
                result = RawReplayOperationResult(
                    operation_id=operation_id,
                    canonical_batch_id=operation.eligibility.canonical_batch_id,
                    restored_coverage_count=restored_coverage,
                    restored_watermark_count=restored_watermarks,
                    resolved_gap_count=resolved_gaps,
                    completed_at=effective_completed,
                    originating_coverage_commit_hash=coverage_hashes[0],
                    result_hash=result_hash,
                    replayed=replayed,
                )
        except sqlite3.IntegrityError as error:
            raise ReplayIdentityCollisionError(
                "SQLite rejected dedicated raw replay atomically"
            ) from error
        return result

    def fail_raw_replay_operation(
        self,
        lease: WriterLease,
        operation_id: UUID,
        *,
        error_code: str,
        sanitized_error: str,
        failed_at: datetime | None = None,
    ) -> RawReplayOperation:
        failed = self._store._now() if failed_at is None else _as_utc(failed_at, label="failed_at")
        if (
            not error_code
            or len(error_code) > 64
            or not error_code.replace("_", "").isalnum()
            or not sanitized_error
            or len(sanitized_error) > 512
            or any(ord(value) < 32 for value in sanitized_error)
        ):
            raise ValueError("raw replay failure metadata is not safely sanitized")
        with self._store._leased_transaction(lease) as connection:
            row = self._load_operation_row(connection, operation_id)
            if row is None:
                raise ReplayStateConflictError("raw replay operation does not exist")
            status = RawReplayOperationStatus(str(row["status"]))
            if status in {RawReplayOperationStatus.PLANNED, RawReplayOperationStatus.RUNNING}:
                connection.execute(
                    """
                    UPDATE raw_replay_operations
                    SET status = 'FAILED', completed_at = ?, error_code = ?, sanitized_error = ?
                    WHERE operation_id = ? AND status IN ('PLANNED', 'RUNNING')
                    """,
                    (_format_utc(failed), error_code, sanitized_error, str(operation_id)),
                )
                connection.execute(
                    """
                    UPDATE ingestion_runs
                    SET status = 'FAILED', completed_at = ?, failed_request_count = 1
                    WHERE run_id = ? AND status IN ('PLANNED', 'RUNNING')
                    """,
                    (_format_utc(failed), str(operation_id)),
                )
            elif status is RawReplayOperationStatus.FAILED:
                if (
                    str(row["error_code"]) != error_code
                    or str(row["sanitized_error"]) != sanitized_error
                ):
                    raise ReplayIdentityCollisionError(
                        "raw replay failure replay differs from durable error"
                    )
            else:
                raise ReplayStateConflictError("successful raw replay cannot be failed")
        durable = self.load_raw_replay_operation(operation_id)
        if durable is None:
            raise ReplayIntegrityError("failed raw replay operation disappeared")
        return durable

    @staticmethod
    def _load_operation_row(
        connection: sqlite3.Connection,
        operation_id: UUID,
    ) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            connection.execute(
                """
                SELECT operation.*, spec.specification_json,
                       run.mode AS run_mode, run.environment AS run_environment,
                       run.provider AS run_provider, run.dataset AS run_dataset,
                       run.status AS run_status, run.created_at AS run_created_at,
                       run.started_at AS run_started_at, run.completed_at AS run_completed_at,
                       run.planned_request_count, run.succeeded_request_count,
                       run.failed_request_count
                FROM raw_replay_operations AS operation
                JOIN request_specs AS spec
                  ON spec.request_spec_id = operation.request_spec_id
                JOIN ingestion_runs AS run ON run.run_id = operation.operation_id
                WHERE operation.operation_id = ?
                """,
                (str(operation_id),),
            ).fetchone(),
        )

    @staticmethod
    def _operation_from_row(row: sqlite3.Row) -> RawReplayOperation:
        try:
            specification = _parse_request_specification(str(row["specification_json"]))
            eligibility = RawReplayEligibility.model_validate(
                _strict_json(str(row["evidence_json"]))
            )
            operation = RawReplayOperation(
                operation_id=UUID(str(row["operation_id"])),
                specification=specification,
                eligibility=eligibility,
                status=RawReplayOperationStatus(str(row["status"])),
                requested_at=_parse_utc(str(row["requested_at"])),
                started_at=(
                    None if row["started_at"] is None else _parse_utc(str(row["started_at"]))
                ),
                completed_at=(
                    None if row["completed_at"] is None else _parse_utc(str(row["completed_at"]))
                ),
                result_hash=(None if row["result_hash"] is None else str(row["result_hash"])),
                error_code=(None if row["error_code"] is None else str(row["error_code"])),
                sanitized_error=(
                    None if row["sanitized_error"] is None else str(row["sanitized_error"])
                ),
            )
        except ValueError as error:
            raise ReplayIntegrityError("raw replay operation ledger is corrupt") from error
        expected_run_counts = {
            RawReplayOperationStatus.PLANNED: ("PLANNED", 0, 0),
            RawReplayOperationStatus.RUNNING: ("RUNNING", 0, 0),
            RawReplayOperationStatus.SUCCESS: ("SUCCESS", 1, 0),
            RawReplayOperationStatus.FAILED: ("FAILED", 0, 1),
        }[operation.status]
        if (
            eligibility.canonical_batch_id != str(row["canonical_batch_id"])
            or eligibility.reason.value != str(row["replay_reason"])
            or _canonical_json(eligibility) != str(row["evidence_json"])
            or _hash_json(eligibility.model_dump(mode="json")) != str(row["evidence_hash"])
            or specification.request_spec_id != str(row["request_spec_id"])
            or specification.provider != str(row["provider"])
            or specification.dataset != str(row["dataset"])
            or _format_utc(specification.start) != str(row["interval_start"])
            or _format_utc(specification.end) != str(row["interval_end"])
            or str(row["run_mode"]) != "REPAIR"
            or str(row["run_provider"]) != specification.provider
            or str(row["run_dataset"]) != specification.dataset
            or str(row["run_status"]) != expected_run_counts[0]
            or int(row["planned_request_count"]) != 1
            or int(row["succeeded_request_count"]) != expected_run_counts[1]
            or int(row["failed_request_count"]) != expected_run_counts[2]
            or str(row["run_created_at"]) != str(row["requested_at"])
            or row["run_started_at"] != row["started_at"]
            or row["run_completed_at"] != row["completed_at"]
        ):
            raise ReplayIdentityCollisionError("raw replay operation/run ledger differs")
        return operation

    def find_latest_replayable_acquisition(
        self,
        specification: RequestSpecification,
        eligibility: RawReplayEligibility,
    ) -> ReplayableAcquisition | None:
        """Return retained raw only when a canonical-only repair is durably proven.

        An exact specification alone is intentionally insufficient: acquisition and expected-
        observation gaps may require a provider refresh even when an old raw acquisition is
        complete.  The typed selector is always revalidated against the operational ledger.
        """

        with self._store.read_only_connection() as connection:
            self._require_exact_active_specification(connection, specification)
            source_request_ids = self._require_replay_eligibility(
                connection,
                specification,
                eligibility,
            )
            candidates = connection.execute(
                """
                SELECT acquisition.attempt_id, acquisition.request_instance_id,
                       acquisition.authorization_hash, acquisition.authorization_json,
                       acquisition.ordered_artifacts_hash, acquisition.page_count,
                       acquisition.completed_at, attempt.status AS attempt_status,
                       policy.provider, policy.dataset, policy.retention_mode
                FROM attempt_acquisition_records AS acquisition
                JOIN request_attempts AS attempt ON attempt.attempt_id = acquisition.attempt_id
                JOIN policy_snapshots AS policy
                  ON policy.policy_snapshot_id = acquisition.policy_snapshot_id
                WHERE acquisition.request_spec_id = ?
                  AND acquisition.request_instance_id IN (
                      SELECT request_instance_id FROM canonical_batch_requests
                      WHERE canonical_batch_id = ?
                  )
                  AND acquisition.pagination_complete = 1
                  AND acquisition.terminal_page_verified = 1
                  AND attempt.status IN ('RAW_COMPLETE', 'SUCCESS')
                ORDER BY acquisition.completed_at DESC, acquisition.attempt_id DESC
                """,
                (specification.request_spec_id, eligibility.canonical_batch_id),
            ).fetchall()
            for candidate in candidates:
                if str(candidate["request_instance_id"]) not in source_request_ids:
                    raise ReplayIntegrityError(
                        "canonical request provenance changed during replay selection"
                    )
                replay = self._candidate_replay(
                    connection,
                    specification,
                    eligibility,
                    candidate,
                )
                if replay is not None:
                    return replay
        return None

    def adopt_acquisition(
        self,
        lease: WriterLease,
        identity: AttemptIdentity,
        authorization: AcquisitionPolicyAuthorization,
        replay: ReplayableAcquisition,
        *,
        adopted_at: datetime | None = None,
    ) -> AdoptedAcquisition:
        """Adopt verified raw on one new exact-spec RUNNING attempt, atomically and idempotently."""

        adopted = (
            self._store._now() if adopted_at is None else _as_utc(adopted_at, label="adopted_at")
        )
        if identity.attempt_id == replay.source_attempt_id:
            raise ReplayStateConflictError("raw adoption requires a different target attempt")
        current_identities = _raw_identities(authorization)
        current_artifact_ids = tuple(value.artifact_id for value in current_identities)
        if current_artifact_ids != replay.ordered_artifact_ids:
            raise ReplayIdentityCollisionError(
                "current acquisition authorization differs from retained raw identity"
            )
        if authorization.request.request_spec_hash != replay.specification.request_spec_hash:
            raise ReplayIdentityCollisionError("current authorization is for another exact spec")
        durable_adoption = self.load_adopted_replay(identity.attempt_id)
        verified_replay = (
            self._load_replay_attempt(
                replay.source_attempt_id,
                replay.specification,
                replay.eligibility,
            )
            if durable_adoption is None
            else durable_adoption
        )
        if verified_replay != replay:
            raise ReplayIdentityCollisionError("source replay changed since it was selected")
        authorization_json = _canonical_json(authorization)
        authorization_hash = _hash_json(authorization.model_dump(mode="json"))
        request_authorization_hash = _hash_json(authorization.request.model_dump(mode="json"))
        policy_snapshot_id = deterministic_policy_snapshot_id(authorization.request.policy_snapshot)
        if adopted < authorization.authorized_at:
            raise ReplayStateConflictError("adoption cannot predate current authorization")

        try:
            with self._store._leased_transaction(lease) as connection:
                target = self._require_adoption_target(
                    connection,
                    identity,
                    replay,
                    authorization,
                    request_authorization_hash=request_authorization_hash,
                    policy_snapshot_id=policy_snapshot_id,
                )
                existing_adoption = connection.execute(
                    "SELECT * FROM raw_acquisition_adoptions WHERE attempt_id = ?",
                    (str(identity.attempt_id),),
                ).fetchone()
                eligible_source_requests = (
                    self._require_replay_eligibility(
                        connection,
                        replay.specification,
                        replay.eligibility,
                    )
                    if existing_adoption is None
                    else self._require_historical_replay_binding(
                        connection,
                        replay.specification,
                        replay.eligibility,
                    )
                )
                if str(replay.source_request_instance_id) not in eligible_source_requests:
                    raise ReplayIntegrityError(
                        "source acquisition is not linked to replay evidence"
                    )
                source = self._source_rows(
                    connection,
                    replay.source_attempt_id,
                    replay.specification,
                )
                existing_acquisition = connection.execute(
                    "SELECT * FROM attempt_acquisition_records WHERE attempt_id = ?",
                    (str(identity.attempt_id),),
                ).fetchone()
                replayed = existing_adoption is not None or existing_acquisition is not None
                if (existing_adoption is None) != (existing_acquisition is None):
                    raise ReplayIntegrityError("raw adoption is only partially durable")
                if existing_adoption is None:
                    self._insert_adoption(
                        connection,
                        identity=identity,
                        replay=replay,
                        authorization=authorization,
                        authorization_json=authorization_json,
                        authorization_hash=authorization_hash,
                        policy_snapshot_id=policy_snapshot_id,
                        adopted=adopted,
                        source_rows=source,
                    )
                else:
                    evidence_json = _canonical_json(replay.eligibility)
                    if (
                        str(existing_adoption["source_attempt_id"]) != str(replay.source_attempt_id)
                        or str(existing_adoption["canonical_batch_id"])
                        != replay.eligibility.canonical_batch_id
                        or str(existing_adoption["replay_reason"])
                        != replay.eligibility.reason.value
                        or str(existing_adoption["evidence_json"]) != evidence_json
                        or str(existing_adoption["evidence_hash"])
                        != _hash_json(replay.eligibility.model_dump(mode="json"))
                    ):
                        raise ReplayIdentityCollisionError(
                            "target attempt was adopted with different source or evidence"
                        )
                    self._assert_existing_adoption(
                        connection,
                        identity=identity,
                        replay=replay,
                        authorization=authorization,
                        authorization_json=authorization_json,
                        authorization_hash=authorization_hash,
                        policy_snapshot_id=policy_snapshot_id,
                        source_rows=source,
                    )
                    adopted = _parse_utc(str(existing_adoption["adopted_at"]))
                if str(target["attempt_status"]) == "RUNNING":
                    connection.execute(
                        """
                        UPDATE request_attempts
                        SET status = 'RAW_COMPLETE', completed_at = ?, page_count = ?,
                            pagination_complete = 1, terminal_page_verified = 1
                        WHERE attempt_id = ? AND status = 'RUNNING'
                        """,
                        (
                            _format_utc(adopted),
                            len(current_artifact_ids),
                            str(identity.attempt_id),
                        ),
                    )
                if str(target["request_status"]) == RequestInstanceStatus.ACQUIRING.value:
                    connection.execute(
                        """
                        UPDATE request_instances SET status = 'RAW_COMPLETE'
                        WHERE request_instance_id = ? AND status = 'ACQUIRING'
                        """,
                        (str(identity.request_instance_id),),
                    )
        except sqlite3.IntegrityError as error:
            raise ReplayIdentityCollisionError(
                "SQLite rejected raw acquisition adoption atomically"
            ) from error
        return AdoptedAcquisition(
            attempt_id=identity.attempt_id,
            request_instance_id=identity.request_instance_id,
            source_attempt_id=replay.source_attempt_id,
            eligibility=replay.eligibility,
            authorization_hash=authorization_hash,
            ordered_artifact_ids=current_artifact_ids,
            adopted_at=adopted,
            replayed=replayed,
        )

    def reconcile_canonical_loss(
        self,
        lease: WriterLease,
        canonical_batch_id: str,
        *,
        detected_at: datetime | None = None,
    ) -> CanonicalLossReconciliation:
        """Invalidate one verified batch and its proofs after exact catalog loss is observed."""

        observed = (
            self._store._now() if detected_at is None else _as_utc(detected_at, label="detected_at")
        )
        context = self._load_canonical_context(canonical_batch_id)
        losses = self._canonical_losses(context)
        if context["batch_state"] == "VERIFIED" and not losses:
            return self._canonical_result(
                context,
                state=CanonicalLossState.HEALTHY,
                targets=(),
                invalidated_at=None,
                replayed=True,
            )
        try:
            with self._store._leased_transaction(lease) as connection:
                durable = self._canonical_context_row(connection, canonical_batch_id)
                self._assert_reconcilable_policy(durable)
                state = str(durable["batch_state"])
                if state == "PURGED":
                    raise ReplayStateConflictError("purged canonical data cannot be repaired")
                if state == "INVALID":
                    gaps = self._canonical_integrity_gaps(connection, canonical_batch_id)
                    if not gaps:
                        raise ReplayStateConflictError(
                            "canonical batch is invalid for an unrelated or unproven reason"
                        )
                    invalidated_at = _parse_utc(str(durable["invalidated_at"]))
                    return self._canonical_result(
                        context,
                        state=CanonicalLossState.ALREADY_INVALID,
                        targets=losses,
                        invalidated_at=invalidated_at,
                        replayed=True,
                        gaps=gaps,
                    )
                if state != "VERIFIED" or not losses:
                    raise ReplayStateConflictError(
                        "canonical-loss invalidation requires a verified batch with proven loss"
                    )
                timestamp = _format_utc(observed)
                connection.execute(
                    """
                    UPDATE canonical_batches
                    SET state = 'INVALID', invalidated_at = ?
                    WHERE canonical_batch_id = ? AND state = 'VERIFIED'
                    """,
                    (timestamp, canonical_batch_id),
                )
                connection.execute(
                    """
                    UPDATE coverage_segments
                    SET verification_state = 'INVALID', retained = 0,
                        generation = generation + 1, invalidated_at = ?
                    WHERE canonical_batch_id = ? AND verification_state = 'VERIFIED'
                    """,
                    (timestamp, canonical_batch_id),
                )
                streams = connection.execute(
                    """
                    SELECT stream_id, interval_start, interval_end
                    FROM canonical_batch_streams
                    WHERE canonical_batch_id = ? ORDER BY stream_id
                    """,
                    (canonical_batch_id,),
                ).fetchall()
                if not streams:
                    raise ReplayIntegrityError("canonical batch has no durable stream scope")
                connection.execute(
                    """
                    UPDATE watermarks
                    SET verification_state = 'INVALID', generation = generation + 1,
                        invalidated_at = ?
                    WHERE verification_state = 'VERIFIED' AND stream_id IN (
                        SELECT stream_id FROM canonical_batch_streams
                        WHERE canonical_batch_id = ?
                    )
                    """,
                    (timestamp, canonical_batch_id),
                )
                request_id = self._originating_request_id(connection, canonical_batch_id)
                for stream in streams:
                    self._upsert_canonical_loss_gap(
                        connection,
                        canonical_batch_id=canonical_batch_id,
                        request_instance_id=request_id,
                        stream_id=str(stream["stream_id"]),
                        start=str(stream["interval_start"]),
                        end=str(stream["interval_end"]),
                        detected_at=timestamp,
                    )
                gaps = self._canonical_integrity_gaps(connection, canonical_batch_id)
                result = self._canonical_result(
                    context,
                    state=CanonicalLossState.INVALIDATED,
                    targets=losses,
                    invalidated_at=observed,
                    replayed=False,
                    gaps=gaps,
                )
        except sqlite3.IntegrityError as error:
            raise ReplayIdentityCollisionError(
                "SQLite rejected canonical-loss invalidation atomically"
            ) from error
        return result

    def reconcile_stream_integrity(
        self,
        lease: WriterLease,
        stream_ids: tuple[str, ...],
        *,
        detected_at: datetime | None = None,
    ) -> tuple[CanonicalLossReconciliation, ...]:
        """Recheck every verified batch supporting selected exact streams."""

        if not stream_ids or tuple(sorted(set(stream_ids))) != stream_ids:
            raise ValueError("integrity reconciliation stream IDs must be unique and sorted")
        with self._store.read_only_connection() as connection:
            placeholders = ", ".join("?" for _ in stream_ids)
            rows = connection.execute(
                f"""
                SELECT DISTINCT coverage.canonical_batch_id
                FROM coverage_segments AS coverage
                JOIN canonical_batches AS batch
                  ON batch.canonical_batch_id = coverage.canonical_batch_id
                WHERE coverage.stream_id IN ({placeholders})
                  AND coverage.verification_state = 'VERIFIED'
                  AND coverage.retained = 1
                  AND coverage.invalidated_at IS NULL
                  AND batch.state = 'VERIFIED'
                ORDER BY coverage.canonical_batch_id
                """,
                stream_ids,
            ).fetchall()
        return tuple(
            self.reconcile_canonical_loss(
                lease,
                str(row["canonical_batch_id"]),
                detected_at=detected_at,
            )
            for row in rows
        )

    def reactivate_identical_canonical_batch(
        self,
        lease: WriterLease,
        identity: AttemptIdentity,
        expectation: CanonicalBatchExpectation,
        manifest: CanonicalBatchManifest,
        published: PublishedCanonicalBatch,
        *,
        verified_at: datetime | None = None,
    ) -> CanonicalReactivationResult:
        """Restore only an exact republish of a loss-invalidated canonical identity."""

        verified = (
            self._store._now() if verified_at is None else _as_utc(verified_at, label="verified_at")
        )
        canonical_batch_id = expectation.batch_context.canonical_batch_id
        self._verify_identical_republication(expectation, manifest, published)
        try:
            with self._store._leased_transaction(lease) as connection:
                row = self._canonical_context_row(connection, canonical_batch_id)
                self._assert_reconcilable_policy(row)
                target = self._require_reactivation_target(
                    connection,
                    identity,
                    expectation,
                )
                (
                    effective_verified,
                    restored_coverage,
                    restored_watermarks,
                    resolved_gaps,
                    replayed,
                ) = self._restore_canonical_state(
                    connection,
                    row,
                    canonical_batch_id=canonical_batch_id,
                    verified=verified,
                )
                self._finalize_replay_request(
                    connection,
                    lease,
                    identity,
                    expectation,
                    target,
                    completed_at=_format_utc(effective_verified),
                )
                result = CanonicalReactivationResult(
                    canonical_batch_id=canonical_batch_id,
                    attempt_id=identity.attempt_id,
                    request_instance_id=identity.request_instance_id,
                    state="VERIFIED",
                    restored_coverage_count=restored_coverage,
                    restored_watermark_count=restored_watermarks,
                    resolved_gap_count=resolved_gaps,
                    verified_at=effective_verified,
                    replayed=replayed,
                )
        except sqlite3.IntegrityError as error:
            raise ReplayIdentityCollisionError(
                "SQLite rejected canonical reactivation atomically"
            ) from error
        return result

    def _restore_canonical_state(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        canonical_batch_id: str,
        verified: datetime,
    ) -> tuple[datetime, int, int, int, bool]:
        state = str(row["batch_state"])
        if state == "VERIFIED":
            return _parse_utc(str(row["batch_verified_at"])), 0, 0, 0, True
        if state != "INVALID" or row["invalidated_at"] is None:
            raise ReplayStateConflictError(
                "only a loss-invalidated canonical batch can be reactivated"
            )
        invalidated_at = str(row["invalidated_at"])
        gaps = self._canonical_integrity_gaps(connection, canonical_batch_id)
        streams = {
            str(value["stream_id"])
            for value in connection.execute(
                """
                SELECT stream_id FROM canonical_batch_streams
                WHERE canonical_batch_id = ?
                """,
                (canonical_batch_id,),
            ).fetchall()
        }
        if not streams or {str(value["stream_id"]) for value in gaps} != streams:
            raise ReplayIntegrityError(
                "canonical reactivation lacks exact integrity gaps for every stream"
            )
        timestamp = _format_utc(verified)
        restored_coverage = connection.execute(
            """
            UPDATE coverage_segments
            SET verification_state = 'VERIFIED', retained = 1,
                generation = generation + 1, verified_at = ?, invalidated_at = NULL
            WHERE canonical_batch_id = ? AND verification_state = 'INVALID'
              AND invalidated_at = ?
            """,
            (timestamp, canonical_batch_id, invalidated_at),
        ).rowcount
        resolved_gaps = connection.execute(
            """
            UPDATE gaps SET status = 'RESOLVED', resolved_at = ?
            WHERE canonical_batch_id = ? AND gap_type = 'INTEGRITY'
              AND status IN ('OPEN', 'REPAIRING') AND detected_at = ?
            """,
            (timestamp, canonical_batch_id, invalidated_at),
        ).rowcount
        connection.execute(
            """
            UPDATE canonical_batches
            SET state = 'VERIFIED', verified_at = ?, invalidated_at = NULL
            WHERE canonical_batch_id = ? AND state = 'INVALID' AND invalidated_at = ?
            """,
            (timestamp, canonical_batch_id, invalidated_at),
        )
        restored_watermarks = 0
        for stream_id in sorted(streams):
            blocking = connection.execute(
                """
                SELECT 1 FROM gaps WHERE stream_id = ? AND blocking = 1
                  AND status IN ('OPEN', 'REPAIRING') LIMIT 1
                """,
                (stream_id,),
            ).fetchone()
            invalid_coverage = connection.execute(
                """
                SELECT 1 FROM coverage_segments WHERE stream_id = ?
                  AND (verification_state <> 'VERIFIED' OR retained <> 1) LIMIT 1
                """,
                (stream_id,),
            ).fetchone()
            if blocking is not None or invalid_coverage is not None:
                continue
            restored_watermarks += connection.execute(
                """
                UPDATE watermarks SET verification_state = 'VERIFIED',
                    generation = generation + 1, computed_at = ?, invalidated_at = NULL
                WHERE stream_id = ? AND verification_state = 'INVALID'
                  AND invalidated_at = ?
                """,
                (timestamp, stream_id, invalidated_at),
            ).rowcount
        return verified, restored_coverage, restored_watermarks, resolved_gaps, False

    @staticmethod
    def _require_reactivation_target(
        connection: sqlite3.Connection,
        identity: AttemptIdentity,
        expectation: CanonicalBatchExpectation,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT attempt.status AS attempt_status, attempt.attempt_number,
                   attempt.request_instance_id, request.status AS request_status,
                   request.request_spec_id, run.status AS run_status,
                   run.policy_snapshot_id AS run_policy_snapshot_id,
                   acquisition.policy_snapshot_id AS acquisition_policy_snapshot_id,
                   adoption.canonical_batch_id, adoption.replay_reason,
                   adoption.evidence_json, adoption.evidence_hash,
                   batch.policy_snapshot_id AS batch_policy_snapshot_id,
                   context_link.request_instance_id AS context_request_id,
                   prepared.state AS preparation_state
            FROM request_attempts AS attempt
            JOIN request_instances AS request
              ON request.request_instance_id = attempt.request_instance_id
            JOIN ingestion_runs AS run ON run.run_id = request.run_id
            JOIN attempt_acquisition_records AS acquisition
              ON acquisition.attempt_id = attempt.attempt_id
            JOIN raw_acquisition_adoptions AS adoption
              ON adoption.attempt_id = attempt.attempt_id
            JOIN canonical_batches AS batch
              ON batch.canonical_batch_id = adoption.canonical_batch_id
            LEFT JOIN batch_context_requests AS context_link
              ON context_link.batch_context_id = batch.batch_context_id
             AND context_link.request_instance_id = request.request_instance_id
            LEFT JOIN batch_publication_expectation_requests AS prepared
              ON prepared.batch_context_id = batch.batch_context_id
             AND prepared.request_instance_id = request.request_instance_id
            WHERE attempt.attempt_id = ?
            """,
            (str(identity.attempt_id),),
        ).fetchone()
        if row is None:
            raise ReplayStateConflictError("canonical reactivation requires durable raw adoption")
        try:
            evidence = RawReplayEligibility.model_validate(_strict_json(str(row["evidence_json"])))
        except ValueError as error:
            raise ReplayIntegrityError(
                "canonical reactivation adoption evidence is corrupt"
            ) from error
        expected_batch = expectation.batch_context.canonical_batch_id
        if (
            str(row["request_instance_id"]) != str(identity.request_instance_id)
            or int(row["attempt_number"]) != identity.attempt_number
            or str(row["request_spec_id"]) != expectation.specification.request_spec_id
            or str(row["canonical_batch_id"]) != expected_batch
            or evidence.canonical_batch_id != expected_batch
            or evidence.reason is not RawReplayReason.CANONICAL_LOSS
            or str(row["replay_reason"]) != evidence.reason.value
            or _hash_json(evidence.model_dump(mode="json")) != str(row["evidence_hash"])
            or _canonical_json(evidence) != str(row["evidence_json"])
            or str(row["run_policy_snapshot_id"]) != str(row["acquisition_policy_snapshot_id"])
            or str(row["run_policy_snapshot_id"]) != str(row["batch_policy_snapshot_id"])
        ):
            raise ReplayIdentityCollisionError("canonical reactivation target identity differs")
        if str(row["attempt_status"]) not in {"RAW_COMPLETE", "SUCCESS"}:
            raise ReplayStateConflictError("replay attempt is not ready for canonical reactivation")
        if str(row["request_status"]) not in {"PROCESSING", "SUCCESS"}:
            raise ReplayStateConflictError("replay request is not processing or complete")
        if str(row["run_status"]) not in {"RUNNING", "SUCCESS", "PARTIAL"}:
            raise ReplayStateConflictError("replay run cannot accept canonical completion")
        if row["context_request_id"] is None or str(row["preparation_state"]) not in {
            "PREPARED",
            "CATALOGED",
        }:
            raise ReplayStateConflictError(
                "replay request lacks durable context/publication preparation"
            )
        return cast(sqlite3.Row, row)

    @staticmethod
    def _finalize_replay_request(
        connection: sqlite3.Connection,
        lease: WriterLease,
        identity: AttemptIdentity,
        expectation: CanonicalBatchExpectation,
        target: sqlite3.Row,
        *,
        completed_at: str,
    ) -> None:
        canonical_batch_id = expectation.batch_context.canonical_batch_id
        policy_snapshot_id = str(target["batch_policy_snapshot_id"])
        request_id = str(identity.request_instance_id)
        existing_link = connection.execute(
            """
            SELECT policy_snapshot_id FROM canonical_batch_requests
            WHERE canonical_batch_id = ? AND request_instance_id = ?
            """,
            (canonical_batch_id, request_id),
        ).fetchone()
        if existing_link is None:
            connection.execute(
                """
                INSERT INTO canonical_batch_requests(
                    canonical_batch_id, request_instance_id, policy_snapshot_id, linked_at
                ) VALUES (?, ?, ?, ?)
                """,
                (canonical_batch_id, request_id, policy_snapshot_id, completed_at),
            )
        elif str(existing_link["policy_snapshot_id"]) != policy_snapshot_id:
            raise ReplayIdentityCollisionError("canonical replay request policy link differs")

        coverage_hash_rows = connection.execute(
            """
            SELECT DISTINCT coverage_commit_hash FROM publication_commits
            WHERE canonical_batch_id = ? ORDER BY coverage_commit_hash
            """,
            (canonical_batch_id,),
        ).fetchall()
        coverage_hashes = tuple(str(value["coverage_commit_hash"]) for value in coverage_hash_rows)
        if len(coverage_hashes) != 1:
            raise ReplayIntegrityError("canonical replay lacks one exact prior coverage proof")
        coverage_hash = coverage_hashes[0]
        commit = connection.execute(
            """
            SELECT * FROM publication_commits
            WHERE canonical_batch_id = ? AND request_instance_id = ?
            """,
            (canonical_batch_id, request_id),
        ).fetchone()
        if commit is None:
            connection.execute(
                """
                INSERT INTO publication_commits(
                    canonical_batch_id, request_instance_id, attempt_id,
                    coverage_commit_hash, commit_source, lease_owner_id,
                    lease_generation, committed_at
                ) VALUES (?, ?, ?, ?, 'RECOVERY_ADOPTION', ?, ?, ?)
                """,
                (
                    canonical_batch_id,
                    request_id,
                    str(identity.attempt_id),
                    coverage_hash,
                    lease.owner_id,
                    lease.generation,
                    completed_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO request_terminal_proofs(
                    request_instance_id, attempt_id, canonical_batch_id,
                    coverage_commit_hash, terminal_status, completed_at
                ) VALUES (?, ?, ?, ?, 'SUCCESS', ?)
                """,
                (
                    request_id,
                    str(identity.attempt_id),
                    canonical_batch_id,
                    coverage_hash,
                    completed_at,
                ),
            )
        elif (
            str(commit["attempt_id"]),
            str(commit["coverage_commit_hash"]),
            str(commit["commit_source"]),
        ) != (str(identity.attempt_id), coverage_hash, "RECOVERY_ADOPTION"):
            raise ReplayIdentityCollisionError("canonical replay terminal commit differs")
        terminal = connection.execute(
            "SELECT * FROM request_terminal_proofs WHERE request_instance_id = ?",
            (request_id,),
        ).fetchone()
        if terminal is None or (
            str(terminal["attempt_id"]),
            str(terminal["canonical_batch_id"]),
            str(terminal["coverage_commit_hash"]),
            str(terminal["terminal_status"]),
        ) != (str(identity.attempt_id), canonical_batch_id, coverage_hash, "SUCCESS"):
            raise ReplayIdentityCollisionError("canonical replay terminal proof differs")
        connection.execute(
            """
            UPDATE batch_publication_expectation_requests
            SET state = 'CATALOGED', cataloged_at = ?
            WHERE batch_context_id = ? AND request_instance_id = ? AND state = 'PREPARED'
            """,
            (completed_at, expectation.batch_context.batch_context_id, request_id),
        )
        connection.execute(
            """
            UPDATE request_attempts SET status = 'SUCCESS', completed_at = ?
            WHERE attempt_id = ? AND status = 'RAW_COMPLETE'
            """,
            (completed_at, str(identity.attempt_id)),
        )
        connection.execute(
            """
            UPDATE request_instances SET status = 'SUCCESS', completed_at = ?
            WHERE request_instance_id = ? AND status = 'PROCESSING'
            """,
            (completed_at, request_id),
        )

    def _require_exact_active_specification(
        self,
        connection: sqlite3.Connection,
        specification: RequestSpecification,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT spec.*, active.status AS active_status,
                   active.policy_snapshot_id AS active_policy_snapshot_id,
                   active.retention_mode AS active_retention_mode,
                   active.expires_at AS active_expires_at,
                   active.unavailable_at AS active_unavailable_at,
                   policy.provider AS policy_provider, policy.dataset AS policy_dataset,
                   policy.retention_mode AS policy_retention_mode
            FROM request_specs AS spec
            LEFT JOIN dataset_policy_status AS active
              ON active.provider = spec.provider AND active.dataset = spec.dataset
            LEFT JOIN policy_snapshots AS policy
              ON policy.policy_snapshot_id = active.policy_snapshot_id
            WHERE spec.request_spec_id = ?
            """,
            (specification.request_spec_id,),
        ).fetchone()
        if row is None:
            raise ReplayStateConflictError("exact request specification is not durable")
        expected = (
            specification.request_spec_hash,
            specification.provider,
            specification.dataset,
            specification.canonical_json,
        )
        actual = (
            str(row["request_spec_hash"]),
            str(row["provider"]),
            str(row["dataset"]),
            str(row["specification_json"]),
        )
        if actual != expected:
            raise ReplayIdentityCollisionError(
                "request specification identity differs from durable exact metadata"
            )
        if (
            str(row["active_status"]) != DatasetPolicyStatus.ACTIVE.value
            or row["active_unavailable_at"] is not None
            or row["active_policy_snapshot_id"] is None
            or (str(row["policy_provider"]), str(row["policy_dataset"]))
            != (specification.provider, specification.dataset)
            or str(row["active_retention_mode"]) != str(row["policy_retention_mode"])
            or str(row["active_retention_mode"])
            in {RetentionMode.PROHIBITED.value, RetentionMode.EPHEMERAL.value}
        ):
            raise ReplayIntegrityError("exact dataset policy cannot support retained raw replay")
        if (
            row["active_expires_at"] is not None
            and _parse_utc(str(row["active_expires_at"])) <= self._store._now()
        ):
            raise ReplayIntegrityError("exact dataset policy expired before raw replay")
        return cast(sqlite3.Row, row)

    @staticmethod
    def _require_replay_eligibility(
        connection: sqlite3.Connection,
        specification: RequestSpecification,
        eligibility: RawReplayEligibility,
    ) -> frozenset[str]:
        row = connection.execute(
            """
            SELECT batch.state, context.request_spec_id, expectation.expectation_json
            FROM canonical_batches AS batch
            JOIN batch_contexts AS context
              ON context.batch_context_id = batch.batch_context_id
            JOIN batch_publication_expectations AS expectation
              ON expectation.batch_context_id = context.batch_context_id
            WHERE batch.canonical_batch_id = ?
            """,
            (eligibility.canonical_batch_id,),
        ).fetchone()
        if row is None:
            raise ReplayStateConflictError("replay evidence canonical batch does not exist")
        if str(row["request_spec_id"]) != specification.request_spec_id:
            raise ReplayIdentityCollisionError(
                "replay evidence belongs to another request specification"
            )
        try:
            expectation = CanonicalBatchExpectation.model_validate_json(
                str(row["expectation_json"])
            )
        except ValueError as error:
            raise ReplayIntegrityError("replay evidence expectation is corrupt") from error
        if (
            expectation.specification.request_spec_hash != specification.request_spec_hash
            or expectation.batch_context.canonical_batch_id != eligibility.canonical_batch_id
        ):
            raise ReplayIdentityCollisionError(
                "replay evidence expectation differs from the exact request"
            )

        if eligibility.reason is RawReplayReason.CANONICAL_LOSS:
            required_gap_type = "INTEGRITY"
            if str(row["state"]) != "INVALID":
                raise ReplayStateConflictError(
                    "canonical-loss replay requires an INVALID canonical batch"
                )
        else:
            required_gap_type = "CALENDAR_STALE"
            if str(row["state"]) not in {"PUBLISHED", "VERIFIED"}:
                raise ReplayStateConflictError(
                    "canonical-stale replay requires retained canonical state"
                )
            stale_count = int(
                connection.execute(
                    """
                    SELECT count(*) FROM coverage_segments
                    WHERE canonical_batch_id = ? AND verification_state = 'STALE'
                      AND retained = 1
                    """,
                    (eligibility.canonical_batch_id,),
                ).fetchone()[0]
            )
            if stale_count == 0:
                raise ReplayStateConflictError(
                    "canonical-stale replay lacks retained STALE coverage"
                )

        evidence_rows = connection.execute(
            """
            SELECT gap_id FROM gaps
            WHERE canonical_batch_id = ? AND gap_type = ?
              AND status IN ('OPEN', 'REPAIRING') AND blocking = 1
            ORDER BY gap_id
            """,
            (eligibility.canonical_batch_id, required_gap_type),
        ).fetchall()
        durable_gap_ids = tuple(str(value["gap_id"]) for value in evidence_rows)
        if durable_gap_ids != eligibility.evidence_gap_ids:
            raise ReplayStateConflictError(
                "typed replay eligibility does not exactly match durable blocking evidence"
            )
        request_rows = connection.execute(
            """
            SELECT request_instance_id FROM canonical_batch_requests
            WHERE canonical_batch_id = ? ORDER BY request_instance_id
            """,
            (eligibility.canonical_batch_id,),
        ).fetchall()
        request_ids = frozenset(str(value["request_instance_id"]) for value in request_rows)
        if not request_ids:
            raise ReplayIntegrityError("replay evidence lacks canonical request provenance")
        return request_ids

    @staticmethod
    def _require_historical_replay_binding(
        connection: sqlite3.Connection,
        specification: RequestSpecification,
        eligibility: RawReplayEligibility,
    ) -> frozenset[str]:
        row = connection.execute(
            """
            SELECT batch.state, context.request_spec_id, expectation.expectation_json
            FROM canonical_batches AS batch
            JOIN batch_contexts AS context
              ON context.batch_context_id = batch.batch_context_id
            JOIN batch_publication_expectations AS expectation
              ON expectation.batch_context_id = context.batch_context_id
            WHERE batch.canonical_batch_id = ?
            """,
            (eligibility.canonical_batch_id,),
        ).fetchone()
        if row is None or str(row["state"]) == "PURGED":
            raise ReplayStateConflictError("adopted canonical replay source is unavailable")
        try:
            expectation = CanonicalBatchExpectation.model_validate_json(
                str(row["expectation_json"])
            )
        except ValueError as error:
            raise ReplayIntegrityError("adopted replay expectation is corrupt") from error
        if (
            str(row["request_spec_id"]) != specification.request_spec_id
            or expectation.specification.request_spec_hash != specification.request_spec_hash
            or expectation.batch_context.canonical_batch_id != eligibility.canonical_batch_id
        ):
            raise ReplayIdentityCollisionError("adopted replay source identity differs")
        rows = connection.execute(
            """
            SELECT request_instance_id FROM canonical_batch_requests
            WHERE canonical_batch_id = ? ORDER BY request_instance_id
            """,
            (eligibility.canonical_batch_id,),
        ).fetchall()
        request_ids = frozenset(str(value["request_instance_id"]) for value in rows)
        if not request_ids:
            raise ReplayIntegrityError("adopted replay source lacks request provenance")
        return request_ids

    def _candidate_replay(
        self,
        connection: sqlite3.Connection,
        specification: RequestSpecification,
        eligibility: RawReplayEligibility,
        candidate: sqlite3.Row,
    ) -> ReplayableAcquisition | None:
        if (str(candidate["provider"]), str(candidate["dataset"])) != (
            specification.provider,
            specification.dataset,
        ):
            raise ReplayIntegrityError("source acquisition policy differs from exact dataset")
        if str(candidate["retention_mode"]) in {
            RetentionMode.PROHIBITED.value,
            RetentionMode.EPHEMERAL.value,
        }:
            return None
        try:
            authorization = AcquisitionPolicyAuthorization.model_validate_json(
                str(candidate["authorization_json"])
            )
        except ValueError as error:
            raise ReplayIntegrityError("source acquisition authorization is corrupt") from error
        if (
            _hash_json(authorization.model_dump(mode="json"))
            != str(candidate["authorization_hash"])
            or authorization.request.request_spec_hash != specification.request_spec_hash
        ):
            raise ReplayIntegrityError("source acquisition authorization hash differs")
        rows = self._source_rows(
            connection,
            UUID(str(candidate["attempt_id"])),
            specification,
        )
        identities = _raw_identities(authorization)
        if len(rows) != len(identities) or len(rows) != int(candidate["page_count"]):
            raise ReplayIntegrityError("source acquisition page cardinality differs")
        raw_records: list[RawReplayRecord] = []
        for descriptor, identity, row in zip(
            authorization.ordered_artifacts,
            identities,
            rows,
            strict=True,
        ):
            self._assert_source_row(row, descriptor, identity, specification)
            if not self._source_files_present(row):
                return None
            raw_records.append(self._raw_replay_record(UUID(str(candidate["attempt_id"])), row))
        artifact_ids = tuple(value.artifact_id for value in identities)
        if _ordered_artifacts_hash(artifact_ids) != str(candidate["ordered_artifacts_hash"]):
            raise ReplayIntegrityError("source acquisition ordered artifact hash differs")
        return ReplayableAcquisition(
            source_attempt_id=UUID(str(candidate["attempt_id"])),
            source_request_instance_id=UUID(str(candidate["request_instance_id"])),
            specification=specification,
            eligibility=eligibility,
            source_authorization=authorization,
            ordered_raw=tuple(raw_records),
            completed_at=_parse_utc(str(candidate["completed_at"])),
        )

    def _load_replay_attempt(
        self,
        attempt_id: UUID,
        specification: RequestSpecification,
        eligibility: RawReplayEligibility,
        *,
        require_active_eligibility: bool = True,
    ) -> ReplayableAcquisition:
        with self._store.read_only_connection() as connection:
            self._require_exact_active_specification(connection, specification)
            source_request_ids = (
                self._require_replay_eligibility(
                    connection,
                    specification,
                    eligibility,
                )
                if require_active_eligibility
                else self._require_historical_replay_binding(
                    connection,
                    specification,
                    eligibility,
                )
            )
            row = connection.execute(
                """
                SELECT acquisition.attempt_id, acquisition.request_instance_id,
                       acquisition.authorization_hash, acquisition.authorization_json,
                       acquisition.ordered_artifacts_hash, acquisition.page_count,
                       acquisition.completed_at, attempt.status AS attempt_status,
                       policy.provider, policy.dataset, policy.retention_mode
                FROM attempt_acquisition_records AS acquisition
                JOIN request_attempts AS attempt ON attempt.attempt_id = acquisition.attempt_id
                JOIN policy_snapshots AS policy
                  ON policy.policy_snapshot_id = acquisition.policy_snapshot_id
                WHERE acquisition.attempt_id = ?
                  AND acquisition.request_spec_id = ?
                  AND acquisition.pagination_complete = 1
                  AND acquisition.terminal_page_verified = 1
                  AND attempt.status IN ('RAW_COMPLETE', 'SUCCESS')
                """,
                (str(attempt_id), specification.request_spec_id),
            ).fetchone()
            if row is None:
                raise ReplayStateConflictError("source acquisition is no longer replayable")
            if str(row["request_instance_id"]) not in source_request_ids:
                raise ReplayIntegrityError("source acquisition is not linked to replay evidence")
            replay = self._candidate_replay(connection, specification, eligibility, row)
        if replay is None:
            raise ReplayIntegrityError("source raw bytes are absent or corrupt")
        return replay

    @staticmethod
    def _source_rows(
        connection: sqlite3.Connection,
        attempt_id: UUID,
        specification: RequestSpecification,
    ) -> tuple[sqlite3.Row, ...]:
        return tuple(
            connection.execute(
                """
                SELECT link.ordinal, link.descriptor_hash,
                       artifact.*, manifest.manifest_content_sha256,
                       manifest.manifest_byte_count,
                       observation.retrieved_at AS observation_retrieved_at,
                       observation.observed_at, observation.safe_provider_request_id,
                       replay.raw_batch_id, replay.source_id, replay.source_provider,
                       replay.source_dataset, replay.logical_endpoint,
                       replay.license_classification, replay.retrieved_at AS replay_retrieved_at,
                       replay.media_type AS replay_media_type,
                       replay.file_extension, replay.safe_provider_request_id AS replay_request_id,
                       replay.request_metadata_json, replay.metadata_hash,
                       replay.recorded_at AS replay_recorded_at
                FROM acquisition_artifacts AS link
                JOIN raw_artifacts AS artifact ON artifact.artifact_id = link.artifact_id
                JOIN raw_artifact_manifests AS manifest
                  ON manifest.artifact_id = artifact.artifact_id
                JOIN attempt_artifact_observations AS observation
                  ON observation.attempt_id = link.attempt_id
                 AND observation.artifact_id = link.artifact_id
                JOIN raw_replay_provenance AS replay
                  ON replay.attempt_id = link.attempt_id
                 AND replay.artifact_id = link.artifact_id
                WHERE link.attempt_id = ? AND artifact.request_spec_id = ?
                ORDER BY link.ordinal
                """,
                (str(attempt_id), specification.request_spec_id),
            ).fetchall()
        )

    def _assert_source_row(
        self,
        row: sqlite3.Row,
        descriptor: object,
        identity: RawArtifactIdentity,
        specification: RequestSpecification,
    ) -> None:
        descriptor_hash = _hash_json(cast(BaseModel, descriptor).model_dump(mode="json"))
        actual = (
            int(row["ordinal"]),
            str(row["descriptor_hash"]),
            str(row["artifact_id"]),
            str(row["request_spec_id"]),
            int(row["page_ordinal"]),
            str(row["page_relation_hash"]),
            str(row["content_sha256"]),
            int(row["byte_count"]),
            str(row["media_type"]),
            str(row["content_encoding"]),
            str(row["state"]),
        )
        expected = (
            identity.page_ordinal,
            descriptor_hash,
            identity.artifact_id,
            specification.request_spec_id,
            identity.page_ordinal,
            identity.page_relation_hash,
            identity.content_sha256,
            identity.byte_count,
            identity.media_type,
            identity.content_encoding,
            "VERIFIED",
        )
        if actual != expected:
            raise ReplayIdentityCollisionError(
                "source raw artifact differs from authorized immutable identity"
            )
        if (
            str(row["source_provider"]),
            str(row["source_dataset"]),
            str(row["observation_retrieved_at"]),
        ) != (
            specification.provider,
            specification.dataset,
            str(row["replay_retrieved_at"]),
        ):
            raise ReplayIntegrityError("source raw observation provenance differs")

    def _source_files_present(self, row: sqlite3.Row) -> bool:
        return self._store._managed_file_matches_catalog(
            str(row["relative_path"]),
            expected_sha256=str(row["content_sha256"]),
            expected_bytes=int(row["byte_count"]),
        ) and self._store._managed_file_matches_catalog(
            str(row["manifest_relative_path"]),
            expected_sha256=str(row["manifest_content_sha256"]),
            expected_bytes=int(row["manifest_byte_count"]),
        )

    @staticmethod
    def _raw_replay_record(attempt_id: UUID, row: sqlite3.Row) -> RawReplayRecord:
        try:
            request_metadata = json.loads(str(row["request_metadata_json"]))
            metadata = RawBatchMetadata.model_validate(
                {
                    "batch_id": str(row["raw_batch_id"]),
                    "source": {
                        "source_id": str(row["source_id"]),
                        "provider": str(row["source_provider"]),
                        "dataset": str(row["source_dataset"]),
                        "logical_endpoint": str(row["logical_endpoint"]),
                        "license_classification": str(row["license_classification"]),
                    },
                    "retrieved_at": str(row["replay_retrieved_at"]),
                    "media_type": str(row["replay_media_type"]),
                    "file_extension": str(row["file_extension"]),
                    "provider_request_id": (
                        None if row["replay_request_id"] is None else str(row["replay_request_id"])
                    ),
                    "request_metadata": request_metadata,
                }
            )
        except (ValueError, json.JSONDecodeError) as error:
            raise ReplayIntegrityError("raw replay metadata is corrupt") from error
        if _hash_json(metadata.model_dump(mode="json")) != str(row["metadata_hash"]):
            raise ReplayIntegrityError("raw replay metadata hash differs")
        return RawReplayRecord(
            attempt_id=attempt_id,
            artifact_id=str(row["artifact_id"]),
            payload_relative_path=str(row["relative_path"]),
            manifest_relative_path=str(row["manifest_relative_path"]),
            content_sha256=str(row["content_sha256"]),
            byte_count=int(row["byte_count"]),
            metadata=metadata,
        )

    def _require_adoption_target(
        self,
        connection: sqlite3.Connection,
        identity: AttemptIdentity,
        replay: ReplayableAcquisition,
        authorization: AcquisitionPolicyAuthorization,
        *,
        request_authorization_hash: str,
        policy_snapshot_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT attempt.status AS attempt_status, attempt.attempt_number,
                   attempt.request_instance_id, auth.authorization_hash,
                   auth.authorization_json, auth.request_spec_id, auth.policy_snapshot_id,
                   instance.status AS request_status, instance.request_spec_id AS instance_spec_id,
                   run.status AS run_status, run.policy_snapshot_id AS run_policy_snapshot_id,
                   spec.request_spec_hash, spec.specification_json, spec.provider, spec.dataset,
                   active.status AS active_status,
                   active.policy_snapshot_id AS active_policy_snapshot_id,
                   active.retention_mode AS active_retention_mode,
                   active.expires_at AS active_expires_at,
                   active.unavailable_at AS active_unavailable_at,
                   policy.retention_mode AS policy_retention_mode
            FROM request_attempts AS attempt
            JOIN attempt_request_authorizations AS auth ON auth.attempt_id = attempt.attempt_id
            JOIN request_instances AS instance
              ON instance.request_instance_id = attempt.request_instance_id
            JOIN ingestion_runs AS run ON run.run_id = instance.run_id
            JOIN request_specs AS spec ON spec.request_spec_id = instance.request_spec_id
            JOIN policy_snapshots AS policy ON policy.policy_snapshot_id = run.policy_snapshot_id
            LEFT JOIN dataset_policy_status AS active
              ON active.provider = spec.provider AND active.dataset = spec.dataset
            WHERE attempt.attempt_id = ?
            """,
            (str(identity.attempt_id),),
        ).fetchone()
        if row is None:
            raise ReplayStateConflictError("target attempt is not durable")
        request_authorization_json = _canonical_json(authorization.request)
        if (
            str(row["request_instance_id"]) != str(identity.request_instance_id)
            or int(row["attempt_number"]) != identity.attempt_number
            or str(row["authorization_hash"]) != request_authorization_hash
            or str(row["authorization_json"]) != request_authorization_json
            or str(row["request_spec_id"]) != replay.specification.request_spec_id
            or str(row["instance_spec_id"]) != replay.specification.request_spec_id
            or str(row["request_spec_hash"]) != replay.specification.request_spec_hash
            or str(row["specification_json"]) != replay.specification.canonical_json
            or (str(row["provider"]), str(row["dataset"]))
            != (replay.specification.provider, replay.specification.dataset)
            or str(row["policy_snapshot_id"]) != policy_snapshot_id
            or str(row["run_policy_snapshot_id"]) != policy_snapshot_id
        ):
            raise ReplayIdentityCollisionError(
                "target attempt differs from current exact replay authorization"
            )
        if str(row["attempt_status"]) not in {"RUNNING", "RAW_COMPLETE", "SUCCESS"}:
            raise ReplayStateConflictError("target attempt cannot accept raw adoption")
        if str(row["request_status"]) not in {
            RequestInstanceStatus.ACQUIRING.value,
            RequestInstanceStatus.RAW_COMPLETE.value,
            RequestInstanceStatus.PROCESSING.value,
            RequestInstanceStatus.SUCCESS.value,
            RequestInstanceStatus.PARTIAL.value,
        }:
            raise ReplayStateConflictError("target request cannot accept raw adoption")
        if (
            str(row["active_status"]) != DatasetPolicyStatus.ACTIVE.value
            or str(row["active_policy_snapshot_id"]) != policy_snapshot_id
            or str(row["active_retention_mode"]) != str(row["policy_retention_mode"])
            or row["active_unavailable_at"] is not None
            or str(row["active_retention_mode"])
            in {RetentionMode.PROHIBITED.value, RetentionMode.EPHEMERAL.value}
        ):
            raise ReplayIntegrityError("exact dataset policy cannot adopt retained raw")
        if (
            row["active_expires_at"] is not None
            and _parse_utc(str(row["active_expires_at"])) <= self._store._now()
        ):
            raise ReplayIntegrityError("exact dataset policy expired before raw adoption")
        return cast(sqlite3.Row, row)

    def _insert_adoption(
        self,
        connection: sqlite3.Connection,
        *,
        identity: AttemptIdentity,
        replay: ReplayableAcquisition,
        authorization: AcquisitionPolicyAuthorization,
        authorization_json: str,
        authorization_hash: str,
        policy_snapshot_id: str,
        adopted: datetime,
        source_rows: tuple[sqlite3.Row, ...],
    ) -> None:
        artifact_ids = replay.ordered_artifact_ids
        timestamp = _format_utc(adopted)
        connection.execute(
            """
            INSERT INTO attempt_acquisition_records(
                attempt_id, request_instance_id, request_spec_id, policy_snapshot_id,
                authorization_hash, authorization_json, ordered_artifacts_hash,
                page_count, pagination_complete, terminal_page_verified,
                eligible_before, authorized_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?)
            """,
            (
                str(identity.attempt_id),
                str(identity.request_instance_id),
                replay.specification.request_spec_id,
                policy_snapshot_id,
                authorization_hash,
                authorization_json,
                _ordered_artifacts_hash(artifact_ids),
                len(artifact_ids),
                _format_utc(authorization.request.eligible_before),
                _format_utc(authorization.authorized_at),
                timestamp,
            ),
        )
        for ordinal, (descriptor, row) in enumerate(
            zip(authorization.ordered_artifacts, source_rows, strict=True)
        ):
            artifact_id = str(row["artifact_id"])
            connection.execute(
                """
                INSERT INTO attempt_artifact_observations(
                    attempt_id, artifact_id, retrieved_at, observed_at,
                    safe_provider_request_id
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(identity.attempt_id),
                    artifact_id,
                    str(row["observation_retrieved_at"]),
                    str(row["observed_at"]),
                    row["safe_provider_request_id"],
                ),
            )
            connection.execute(
                """
                INSERT INTO raw_replay_provenance(
                    attempt_id, artifact_id, raw_batch_id, source_id,
                    source_provider, source_dataset, logical_endpoint,
                    license_classification, retrieved_at, media_type,
                    file_extension, safe_provider_request_id,
                    request_metadata_json, metadata_hash, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(identity.attempt_id),
                    artifact_id,
                    str(row["raw_batch_id"]),
                    str(row["source_id"]),
                    str(row["source_provider"]),
                    str(row["source_dataset"]),
                    str(row["logical_endpoint"]),
                    str(row["license_classification"]),
                    str(row["replay_retrieved_at"]),
                    str(row["replay_media_type"]),
                    str(row["file_extension"]),
                    row["replay_request_id"],
                    str(row["request_metadata_json"]),
                    str(row["metadata_hash"]),
                    str(row["replay_recorded_at"]),
                ),
            )
            connection.execute(
                """
                INSERT INTO acquisition_artifacts(
                    attempt_id, artifact_id, ordinal, descriptor_hash
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    str(identity.attempt_id),
                    artifact_id,
                    ordinal,
                    _hash_json(descriptor.model_dump(mode="json")),
                ),
            )
        connection.execute(
            """
            INSERT INTO raw_acquisition_adoptions(
                attempt_id, source_attempt_id, canonical_batch_id, replay_reason,
                evidence_json, evidence_hash, adopted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(identity.attempt_id),
                str(replay.source_attempt_id),
                replay.eligibility.canonical_batch_id,
                replay.eligibility.reason.value,
                _canonical_json(replay.eligibility),
                _hash_json(replay.eligibility.model_dump(mode="json")),
                timestamp,
            ),
        )

    def _assert_existing_adoption(
        self,
        connection: sqlite3.Connection,
        *,
        identity: AttemptIdentity,
        replay: ReplayableAcquisition,
        authorization: AcquisitionPolicyAuthorization,
        authorization_json: str,
        authorization_hash: str,
        policy_snapshot_id: str,
        source_rows: tuple[sqlite3.Row, ...],
    ) -> None:
        acquisition = connection.execute(
            "SELECT * FROM attempt_acquisition_records WHERE attempt_id = ?",
            (str(identity.attempt_id),),
        ).fetchone()
        if acquisition is None:
            raise ReplayIntegrityError("adoption lacks completed acquisition proof")
        expected = (
            str(identity.request_instance_id),
            replay.specification.request_spec_id,
            policy_snapshot_id,
            authorization_hash,
            authorization_json,
            _ordered_artifacts_hash(replay.ordered_artifact_ids),
            len(replay.ordered_artifact_ids),
            1,
            1,
            _format_utc(authorization.request.eligible_before),
            _format_utc(authorization.authorized_at),
        )
        columns = (
            "request_instance_id",
            "request_spec_id",
            "policy_snapshot_id",
            "authorization_hash",
            "authorization_json",
            "ordered_artifacts_hash",
            "page_count",
            "pagination_complete",
            "terminal_page_verified",
            "eligible_before",
            "authorized_at",
        )
        if tuple(acquisition[column] for column in columns) != expected:
            raise ReplayIdentityCollisionError("existing acquisition adoption differs")
        links = connection.execute(
            """
            SELECT acquisition.artifact_id, acquisition.ordinal,
                   acquisition.descriptor_hash, observation.retrieved_at,
                   observation.observed_at, observation.safe_provider_request_id,
                   replay.raw_batch_id, replay.metadata_hash
            FROM acquisition_artifacts AS acquisition
            JOIN attempt_artifact_observations AS observation
              ON observation.attempt_id = acquisition.attempt_id
             AND observation.artifact_id = acquisition.artifact_id
            JOIN raw_replay_provenance AS replay
              ON replay.attempt_id = acquisition.attempt_id
             AND replay.artifact_id = acquisition.artifact_id
            WHERE acquisition.attempt_id = ? ORDER BY acquisition.ordinal
            """,
            (str(identity.attempt_id),),
        ).fetchall()
        expected_links = tuple(
            (
                str(source["artifact_id"]),
                ordinal,
                _hash_json(descriptor.model_dump(mode="json")),
                str(source["observation_retrieved_at"]),
                str(source["observed_at"]),
                source["safe_provider_request_id"],
                str(source["raw_batch_id"]),
                str(source["metadata_hash"]),
            )
            for ordinal, (descriptor, source) in enumerate(
                zip(authorization.ordered_artifacts, source_rows, strict=True)
            )
        )
        actual_links = tuple(
            (
                str(row["artifact_id"]),
                int(row["ordinal"]),
                str(row["descriptor_hash"]),
                str(row["retrieved_at"]),
                str(row["observed_at"]),
                row["safe_provider_request_id"],
                str(row["raw_batch_id"]),
                str(row["metadata_hash"]),
            )
            for row in links
        )
        if actual_links != expected_links:
            raise ReplayIdentityCollisionError("existing adoption provenance differs")

    def _load_canonical_context(self, canonical_batch_id: str) -> dict[str, object]:
        with self._store.read_only_connection() as connection:
            row = self._canonical_context_row(connection, canonical_batch_id)
            self._assert_reconcilable_policy(row)
            expectation = self._expectation(connection, row)
            streams = tuple(
                str(value["stream_id"])
                for value in connection.execute(
                    """
                    SELECT stream_id FROM canonical_batch_streams
                    WHERE canonical_batch_id = ? ORDER BY stream_id
                    """,
                    (canonical_batch_id,),
                ).fetchall()
            )
            gaps = self._canonical_integrity_gaps(connection, canonical_batch_id)
            files = tuple(
                dict(value)
                for value in connection.execute(
                    """
                    SELECT * FROM canonical_files
                    WHERE canonical_batch_id = ? ORDER BY file_ordinal
                    """,
                    (canonical_batch_id,),
                ).fetchall()
            )
            raw_files = tuple(
                dict(value)
                for value in connection.execute(
                    """
                    SELECT artifact.artifact_id, artifact.relative_path,
                           artifact.manifest_relative_path,
                           artifact.content_sha256, artifact.byte_count,
                           manifest.manifest_content_sha256,
                           manifest.manifest_byte_count
                    FROM batch_context_artifacts AS link
                    JOIN raw_artifacts AS artifact
                      ON artifact.artifact_id = link.artifact_id
                    JOIN raw_artifact_manifests AS manifest
                      ON manifest.artifact_id = artifact.artifact_id
                    WHERE link.batch_context_id = ? ORDER BY link.ordinal
                    """,
                    (str(row["batch_context_id"]),),
                ).fetchall()
            )
        return {
            **dict(row),
            "expectation": expectation,
            "streams": streams,
            "gaps": tuple(dict(value) for value in gaps),
            "files": files,
            "raw_files": raw_files,
        }

    @staticmethod
    def _canonical_context_row(
        connection: sqlite3.Connection,
        canonical_batch_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT batch.canonical_batch_id, batch.batch_context_id,
                   batch.policy_snapshot_id, batch.relative_path,
                   batch.manifest_relative_path, batch.state AS batch_state,
                   batch.row_count, batch.published_at,
                   batch.verified_at AS batch_verified_at, batch.invalidated_at,
                   manifest.manifest_content_sha256, manifest.manifest_byte_count,
                   context.request_spec_id, request.request_spec_hash,
                   request.specification_json, request.provider, request.dataset,
                   policy.retention_mode, active.status AS active_status,
                   active.policy_snapshot_id AS active_policy_snapshot_id,
                   active.retention_mode AS active_retention_mode,
                   active.expires_at AS active_expires_at,
                   active.unavailable_at AS active_unavailable_at,
                   expectation.expectation_hash, expectation.expectation_json
            FROM canonical_batches AS batch
            JOIN canonical_batch_manifests AS manifest
              ON manifest.canonical_batch_id = batch.canonical_batch_id
            JOIN batch_contexts AS context ON context.batch_context_id = batch.batch_context_id
            JOIN request_specs AS request ON request.request_spec_id = context.request_spec_id
            JOIN policy_snapshots AS policy
              ON policy.policy_snapshot_id = batch.policy_snapshot_id
            JOIN batch_publication_expectations AS expectation
              ON expectation.batch_context_id = context.batch_context_id
            LEFT JOIN dataset_policy_status AS active
              ON active.provider = request.provider AND active.dataset = request.dataset
            WHERE batch.canonical_batch_id = ?
            """,
            (canonical_batch_id,),
        ).fetchone()
        if row is None:
            raise ReplayStateConflictError("canonical batch recovery context is not durable")
        return cast(sqlite3.Row, row)

    def _assert_reconcilable_policy(self, row: sqlite3.Row | dict[str, object]) -> None:
        if (
            str(row["active_status"]) != DatasetPolicyStatus.ACTIVE.value
            or str(row["active_policy_snapshot_id"]) != str(row["policy_snapshot_id"])
            or str(row["active_retention_mode"]) != str(row["retention_mode"])
            or row["active_unavailable_at"] is not None
            or str(row["retention_mode"])
            in {RetentionMode.PROHIBITED.value, RetentionMode.EPHEMERAL.value}
        ):
            raise ReplayIntegrityError(
                "exact dataset policy cannot support canonical-loss recovery"
            )
        if (
            row["active_expires_at"] is not None
            and _parse_utc(str(row["active_expires_at"])) <= self._store._now()
        ):
            raise ReplayIntegrityError("dataset policy expired before canonical recovery")

    @staticmethod
    def _expectation(
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> CanonicalBatchExpectation:
        try:
            expectation = CanonicalBatchExpectation.model_validate_json(
                str(row["expectation_json"])
            )
            specification = _parse_request_specification(str(row["specification_json"]))
        except ValueError as error:
            raise ReplayIntegrityError("canonical recovery context is corrupt") from error
        if (
            _hash_json(expectation.model_dump(mode="json")) != str(row["expectation_hash"])
            or expectation.batch_context.canonical_batch_id != str(row["canonical_batch_id"])
            or expectation.batch_context.batch_context_id != str(row["batch_context_id"])
            or expectation.specification != specification
            or specification.request_spec_id != str(row["request_spec_id"])
            or specification.request_spec_hash != str(row["request_spec_hash"])
        ):
            raise ReplayIntegrityError("canonical expectation identity or hash differs")
        return expectation

    def _canonical_losses(
        self,
        context: dict[str, object],
    ) -> tuple[CanonicalLossTarget, ...]:
        targets: list[CanonicalLossTarget] = []
        manifest_path = str(context["manifest_relative_path"])
        if not self._store._managed_file_matches_catalog(
            manifest_path,
            expected_sha256=str(context["manifest_content_sha256"]),
            expected_bytes=int(cast(int, context["manifest_byte_count"])),
        ):
            condition = (
                CanonicalLossCondition.CORRUPT
                if self._store._managed_regular_file_is_present(manifest_path)
                else CanonicalLossCondition.ABSENT
            )
            targets.append(
                CanonicalLossTarget(
                    target_type=CanonicalLossTargetType.MANIFEST,
                    relative_path=manifest_path,
                    condition=condition,
                )
            )
        for file in cast(tuple[dict[str, object], ...], context["files"]):
            relative_path = str(file["relative_path"])
            if self._store._managed_file_matches_catalog(
                relative_path,
                expected_sha256=str(file["content_sha256"]),
                expected_bytes=int(cast(int, file["byte_count"])),
            ):
                continue
            condition = (
                CanonicalLossCondition.CORRUPT
                if self._store._managed_regular_file_is_present(relative_path)
                else CanonicalLossCondition.ABSENT
            )
            targets.append(
                CanonicalLossTarget(
                    target_type=CanonicalLossTargetType.PARQUET_FILE,
                    relative_path=relative_path,
                    condition=condition,
                )
            )
        for artifact in cast(tuple[dict[str, object], ...], context["raw_files"]):
            for target_type, path_key, sha_key, bytes_key in (
                (
                    CanonicalLossTargetType.RAW_PAYLOAD,
                    "relative_path",
                    "content_sha256",
                    "byte_count",
                ),
                (
                    CanonicalLossTargetType.RAW_MANIFEST,
                    "manifest_relative_path",
                    "manifest_content_sha256",
                    "manifest_byte_count",
                ),
            ):
                relative_path = str(artifact[path_key])
                if self._store._managed_file_matches_catalog(
                    relative_path,
                    expected_sha256=str(artifact[sha_key]),
                    expected_bytes=int(cast(int, artifact[bytes_key])),
                ):
                    continue
                condition = (
                    CanonicalLossCondition.CORRUPT
                    if self._store._managed_regular_file_is_present(relative_path)
                    else CanonicalLossCondition.ABSENT
                )
                targets.append(
                    CanonicalLossTarget(
                        target_type=target_type,
                        relative_path=relative_path,
                        condition=condition,
                    )
                )
        return tuple(targets)

    @staticmethod
    def _canonical_integrity_gaps(
        connection: sqlite3.Connection,
        canonical_batch_id: str,
    ) -> tuple[sqlite3.Row, ...]:
        return tuple(
            connection.execute(
                """
                SELECT * FROM gaps
                WHERE canonical_batch_id = ? AND gap_type = 'INTEGRITY'
                  AND status IN ('OPEN', 'REPAIRING') AND blocking = 1
                ORDER BY stream_id, interval_start, interval_end
                """,
                (canonical_batch_id,),
            ).fetchall()
        )

    @staticmethod
    def _originating_request_id(
        connection: sqlite3.Connection,
        canonical_batch_id: str,
    ) -> str:
        row = connection.execute(
            """
            SELECT request_instance_id FROM canonical_batch_requests
            WHERE canonical_batch_id = ? ORDER BY linked_at, request_instance_id LIMIT 1
            """,
            (canonical_batch_id,),
        ).fetchone()
        if row is None:
            raise ReplayIntegrityError("canonical batch lacks request provenance")
        return str(row["request_instance_id"])

    @staticmethod
    def _upsert_canonical_loss_gap(
        connection: sqlite3.Connection,
        *,
        canonical_batch_id: str,
        request_instance_id: str,
        stream_id: str,
        start: str,
        end: str,
        detected_at: str,
    ) -> str:
        gap_id = _gap_id(
            canonical_batch_id=canonical_batch_id,
            stream_id=stream_id,
            start=start,
            end=end,
        )
        existing = connection.execute(
            "SELECT * FROM gaps WHERE gap_id = ?",
            (gap_id,),
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO gaps(
                    gap_id, stream_id, interval_start, interval_end, gap_type,
                    status, blocking, detected_at, resolved_at,
                    request_instance_id, canonical_batch_id
                ) VALUES (?, ?, ?, ?, 'INTEGRITY', 'OPEN', 1, ?, NULL, ?, ?)
                """,
                (
                    gap_id,
                    stream_id,
                    start,
                    end,
                    detected_at,
                    request_instance_id,
                    canonical_batch_id,
                ),
            )
            return gap_id
        if (
            str(existing["gap_id"]) != gap_id
            or int(existing["blocking"]) != 1
            or (
                existing["canonical_batch_id"] is not None
                and str(existing["canonical_batch_id"]) != canonical_batch_id
            )
        ):
            raise ReplayIdentityCollisionError(
                "canonical-loss gap collides with different integrity provenance"
            )
        connection.execute(
            """
            UPDATE gaps SET status = 'OPEN', resolved_at = NULL,
                request_instance_id = ?, canonical_batch_id = ?, detected_at = ?
            WHERE gap_id = ?
            """,
            (request_instance_id, canonical_batch_id, detected_at, gap_id),
        )
        return gap_id

    def _canonical_result(
        self,
        context: dict[str, object],
        *,
        state: CanonicalLossState,
        targets: tuple[CanonicalLossTarget, ...],
        invalidated_at: datetime | None,
        replayed: bool,
        gaps: tuple[sqlite3.Row, ...] | None = None,
    ) -> CanonicalLossReconciliation:
        expectation = cast(CanonicalBatchExpectation, context["expectation"])
        durable_gaps = (
            cast(tuple[dict[str, object], ...], context["gaps"])
            if gaps is None
            else tuple(dict(value) for value in gaps)
        )
        gap_ids = tuple(sorted(str(value["gap_id"]) for value in durable_gaps))
        replay_eligibility = (
            None
            if state is CanonicalLossState.HEALTHY
            else RawReplayEligibility(
                reason=RawReplayReason.CANONICAL_LOSS,
                canonical_batch_id=str(context["canonical_batch_id"]),
                evidence_gap_ids=gap_ids,
            )
        )
        return CanonicalLossReconciliation(
            canonical_batch_id=str(context["canonical_batch_id"]),
            batch_context_id=str(context["batch_context_id"]),
            request_specification=expectation.specification,
            expectation=expectation,
            state=state,
            targets=targets,
            affected_stream_ids=cast(tuple[str, ...], context["streams"]),
            integrity_gap_ids=gap_ids,
            replay_eligibility=replay_eligibility,
            invalidated_at=invalidated_at,
            replayed=replayed,
        )

    def _verify_identical_republication(
        self,
        expectation: CanonicalBatchExpectation,
        manifest: CanonicalBatchManifest,
        published: PublishedCanonicalBatch,
    ) -> None:
        canonical_batch_id = expectation.batch_context.canonical_batch_id
        if (
            manifest.canonical_batch_id != canonical_batch_id
            or published.canonical_batch_id != canonical_batch_id
            or manifest.batch_context_id != expectation.batch_context.batch_context_id
        ):
            raise ReplayIdentityCollisionError(
                "republished canonical identity differs from the durable expectation"
            )
        context = self._load_canonical_context(canonical_batch_id)
        if cast(CanonicalBatchExpectation, context["expectation"]) != expectation:
            raise ReplayIdentityCollisionError("republished canonical semantics differ")
        if (
            str(context["relative_path"]) != published.relative_directory
            or str(context["manifest_relative_path"]) != published.manifest_relative_path
        ):
            raise ReplayIdentityCollisionError("republished canonical paths differ")
        manifest_bytes = self._store._read_managed_file_bounded(
            published.manifest_relative_path,
            max_bytes=16 * 1024 * 1024,
        )
        try:
            persisted = CanonicalBatchManifest.model_validate_json(manifest_bytes)
        except ValueError as error:
            raise ReplayIntegrityError("republished canonical manifest is invalid") from error
        if persisted != manifest or (
            hashlib.sha256(manifest_bytes).hexdigest(),
            len(manifest_bytes),
        ) != (
            str(context["manifest_content_sha256"]),
            int(cast(int, context["manifest_byte_count"])),
        ):
            raise ReplayIdentityCollisionError(
                "republished canonical manifest differs from original verified bytes"
            )
        catalog_files = cast(tuple[dict[str, object], ...], context["files"])
        if len(catalog_files) != len(manifest.files) or len(published.files) != len(manifest.files):
            raise ReplayIdentityCollisionError("republished canonical file cardinality differs")
        directory = PurePosixPath(published.relative_directory)
        for ordinal, (catalog, file, published_file) in enumerate(
            zip(catalog_files, manifest.files, published.files, strict=True)
        ):
            relative = (directory / PurePosixPath(file.relative_path)).as_posix()
            actual = (
                int(cast(int, catalog["file_ordinal"])),
                str(catalog["relative_path"]),
                str(catalog["content_sha256"]),
                int(cast(int, catalog["byte_count"])),
                int(cast(int, catalog["row_count"])),
                str(catalog["schema_fingerprint"]),
            )
            expected = (
                ordinal,
                relative,
                file.sha256,
                file.byte_count,
                file.row_count,
                file.schema_sha256,
            )
            if actual != expected or published_file != file:
                raise ReplayIdentityCollisionError("republished canonical file semantics differ")
            if not self._store._managed_file_matches_catalog(
                relative,
                expected_sha256=file.sha256,
                expected_bytes=file.byte_count,
            ):
                raise ReplayIntegrityError("republished canonical file failed exact verification")


__all__ = [
    "AdoptedAcquisition",
    "CanonicalLossCondition",
    "CanonicalLossReconciliation",
    "CanonicalLossState",
    "CanonicalLossTarget",
    "CanonicalLossTargetType",
    "CanonicalReactivationResult",
    "OperationalReplayError",
    "OperationalReplayRepository",
    "PersistedBatchPreparation",
    "RawReplayEligibility",
    "RawReplayOperation",
    "RawReplayOperationResult",
    "RawReplayOperationStatus",
    "RawReplayReason",
    "ReplayIdentityCollisionError",
    "ReplayIntegrityError",
    "ReplayStateConflictError",
    "ReplayableAcquisition",
]
