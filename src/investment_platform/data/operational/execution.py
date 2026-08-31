"""Lease-fenced execution metadata and the filesystem-before-watermark commit.

Provider I/O, normalization, Parquet writes, and expensive file reopening happen outside SQLite.
This repository records only sanitized provenance and verified file catalog metadata.  Its final
publication method is deliberately one short transaction: either catalog, coverage, gaps,
watermarks, and terminal request proof all become durable, or none of them do.  Aggregate run
reconciliation is a separate recoverable transaction.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from itertools import pairwise
from pathlib import PurePosixPath
from typing import Annotated, Self, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.ingestion.coverage import (
    CoverageSegment,
    GapFinding,
    MaterializedWatermark,
)
from investment_platform.data.ingestion.identity import (
    AttemptIdentity,
    BatchContext,
    RawArtifactIdentity,
    RequestSpecification,
)
from investment_platform.data.ingestion.planner import (
    CoverageClassification,
    CoverageVerificationState,
)
from investment_platform.data.operational.planning import (
    IngestionRunStatus,
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
    RequestPolicyAuthorization,
    RetentionMode,
)
from investment_platform.data.storage.canonical_batches import (
    CanonicalBatchExpectation,
    CanonicalBatchManifest,
    CanonicalPublicationProvenance,
    PublishedCanonicalBatch,
    StreamPublicationOutcome,
)
from investment_platform.data.storage.living_raw import (
    PublishedRawArtifact,
    RawArtifactManifest,
)

_SHA256 = r"^[0-9a-f]{64}$"
_DURABLE_ID = r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,191}$"
_SAFE_CODE = r"^[A-Z][A-Z0-9_]{0,63}$"
_SECRET_TEXT = re.compile(
    r"(?:[a-z][a-z0-9+.-]*://|(?:authorization|api[_-]?key|password|secret|token)\s*[:=])",
    re.IGNORECASE,
)


class ExecutionRepositoryError(OperationalStateError):
    """Base error for durable acquisition and publication metadata."""


class ExecutionIdentityCollisionError(ExecutionRepositoryError):
    """An existing durable identity has different immutable metadata."""


class ExecutionStateConflictError(ExecutionRepositoryError):
    """The requested operation is inconsistent with the durable state machine."""


class ExecutionIntegrityError(ExecutionRepositoryError):
    """Published files or their relational provenance are incomplete or inconsistent."""


class PublicationCommitSource(StrEnum):
    NORMAL = "NORMAL"
    RECOVERY_ADOPTION = "RECOVERY_ADOPTION"


class ExecutionFaultPoint(StrEnum):
    SQLITE_TRANSACTION = "SQLITE_TRANSACTION"
    WATERMARK_UPDATE = "WATERMARK_UPDATE"
    RUN_COMPLETION = "RUN_COMPLETION"


class _FrozenExecutionModel(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class DurableAttempt(_FrozenExecutionModel):
    attempt_id: UUID
    request_instance_id: UUID
    attempt_number: Annotated[int, Field(gt=0)]
    status: str
    request_authorization_hash: str = Field(pattern=_SHA256)
    started_at: datetime
    replayed: bool

    @field_validator("started_at", mode="after")
    @classmethod
    def normalize_started_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="started_at")


class CatalogedRawArtifact(_FrozenExecutionModel):
    attempt_id: UUID
    artifact_id: str = Field(pattern=r"^raw_v1_[0-9a-f]{64}$")
    raw_batch_id: UUID
    payload_relative_path: str
    manifest_relative_path: str
    content_sha256: str = Field(pattern=_SHA256)
    byte_count: Annotated[int, Field(ge=0)]
    manifest_content_sha256: str = Field(pattern=_SHA256)
    manifest_byte_count: Annotated[int, Field(gt=0)]
    replayed: bool


class RawReplayRecord(_FrozenExecutionModel):
    attempt_id: UUID
    artifact_id: str = Field(pattern=r"^raw_v1_[0-9a-f]{64}$")
    payload_relative_path: str
    manifest_relative_path: str
    content_sha256: str = Field(pattern=_SHA256)
    byte_count: Annotated[int, Field(ge=0)]
    metadata: RawBatchMetadata


class DurableAcquisition(_FrozenExecutionModel):
    attempt_id: UUID
    request_instance_id: UUID
    authorization_hash: str = Field(pattern=_SHA256)
    ordered_artifact_ids: tuple[str, ...]
    completed_at: datetime
    replayed: bool

    @field_validator("completed_at", mode="after")
    @classmethod
    def normalize_completed_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="completed_at")


class PreparedBatch(_FrozenExecutionModel):
    request_instance_id: UUID
    attempt_id: UUID
    canonical_batch_id: str = Field(pattern=r"^batch_v1_[0-9a-f]{64}$")
    batch_context_id: str = Field(pattern=r"^batch_context_v1_[0-9a-f]{64}$")
    expectation_hash: str = Field(pattern=_SHA256)
    state: str
    prepared_at: datetime
    replayed: bool

    @field_validator("prepared_at", mode="after")
    @classmethod
    def normalize_prepared_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="prepared_at")


class DurableBatchContext(_FrozenExecutionModel):
    request_instance_id: UUID
    attempt_id: UUID
    canonical_batch_id: str = Field(pattern=r"^batch_v1_[0-9a-f]{64}$")
    batch_context_id: str = Field(pattern=r"^batch_context_v1_[0-9a-f]{64}$")
    recorded_at: datetime
    replayed: bool

    @field_validator("recorded_at", mode="after")
    @classmethod
    def normalize_recorded_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="recorded_at")


class CoverageCommit(_FrozenExecutionModel):
    """Pure coverage/frontier result consumed atomically by the operational repository."""

    calendar_snapshot_id: str = Field(pattern=_DURABLE_ID)
    policy_snapshot_id: str = Field(pattern=_DURABLE_ID)
    segments: Annotated[tuple[CoverageSegment, ...], Field(min_length=1)]
    gaps: tuple[GapFinding, ...] = ()
    watermarks: tuple[MaterializedWatermark, ...] = ()

    @model_validator(mode="after")
    def canonicalize_and_validate(self) -> Self:
        segment_keys = tuple(value.coverage_id for value in self.segments)
        gap_keys = tuple(value.gap_id for value in self.gaps)
        watermark_keys = tuple(value.stream_id for value in self.watermarks)
        if segment_keys != tuple(sorted(set(segment_keys))):
            raise ValueError("coverage segments must be unique and ordered by coverage ID")
        if gap_keys != tuple(sorted(set(gap_keys))):
            raise ValueError("gaps must be unique and ordered by gap ID")
        if watermark_keys != tuple(sorted(set(watermark_keys))):
            raise ValueError("watermarks must be unique and ordered by stream ID")
        if any(
            value.calendar_snapshot_id != self.calendar_snapshot_id
            or value.policy_snapshot_id != self.policy_snapshot_id
            for value in self.segments
        ):
            raise ValueError("coverage segments disagree with the commit snapshots")
        if any(
            value.calendar_snapshot_id != self.calendar_snapshot_id
            or value.policy_snapshot_id != self.policy_snapshot_id
            for value in self.watermarks
        ):
            raise ValueError("watermarks disagree with the commit snapshots")
        return self

    @property
    def commit_hash(self) -> str:
        return _hash_model(self)


class PublicationCommitRequest(_FrozenExecutionModel):
    request_instance_id: UUID
    attempt_id: UUID
    acquisition_authorization: AcquisitionPolicyAuthorization
    expectation: CanonicalBatchExpectation
    manifest: CanonicalBatchManifest
    published: PublishedCanonicalBatch
    coverage: CoverageCommit
    terminal_status: RequestInstanceStatus
    source: PublicationCommitSource = PublicationCommitSource.NORMAL

    @field_validator("terminal_status", mode="after")
    @classmethod
    def validate_terminal_status(cls, value: RequestInstanceStatus) -> RequestInstanceStatus:
        if value not in {RequestInstanceStatus.SUCCESS, RequestInstanceStatus.PARTIAL}:
            raise ValueError("published requests may finish only SUCCESS or PARTIAL")
        return value


class PublicationCommitResult(_FrozenExecutionModel):
    run_id: UUID
    request_instance_id: UUID
    canonical_batch_id: str = Field(pattern=r"^batch_v1_[0-9a-f]{64}$")
    coverage_commit_hash: str = Field(pattern=_SHA256)
    request_status: RequestInstanceStatus
    run_status: IngestionRunStatus
    committed_at: datetime
    replayed: bool

    @field_validator("committed_at", mode="after")
    @classmethod
    def normalize_committed_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="committed_at")


class TerminalFailureResult(_FrozenExecutionModel):
    run_id: UUID
    request_instance_id: UUID
    attempt_id: UUID
    request_status: RequestInstanceStatus
    error_id: str = Field(pattern=_DURABLE_ID)
    completed_at: datetime
    replayed: bool

    @field_validator("completed_at", mode="after")
    @classmethod
    def normalize_completed_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="completed_at")


class PreparedPublicationRecord(_FrozenExecutionModel):
    request_instance_id: UUID
    canonical_batch_id: str = Field(pattern=r"^batch_v1_[0-9a-f]{64}$")
    state: str
    expectation: CanonicalBatchExpectation
    prepared_at: datetime

    @field_validator("prepared_at", mode="after")
    @classmethod
    def normalize_prepared_at(cls, value: datetime) -> datetime:
        return _as_utc(value, label="prepared_at")


FaultInjector = Callable[[ExecutionFaultPoint], None]


def _as_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _canonical_json(value: object) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _hash_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _hash_model(value: BaseModel) -> str:
    return _hash_json(value.model_dump(mode="json"))


def _raw_identity_from_authorization(
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


def _invoke_fault(injector: FaultInjector | None, point: ExecutionFaultPoint) -> None:
    if injector is not None:
        injector(point)


def _safe_metadata(metadata: RawBatchMetadata) -> RawBatchMetadata:
    candidate = RawBatchMetadata.model_validate(metadata.model_dump(mode="python"))
    rendered = _canonical_json(candidate)
    if _SECRET_TEXT.search(rendered) or "\r" in rendered or "\n" in rendered:
        raise ExecutionIntegrityError("raw replay metadata contains unsafe text")
    return candidate


def _row_tuple(row: sqlite3.Row, columns: tuple[str, ...]) -> tuple[object, ...]:
    return tuple(row[column] for column in columns)


class IngestionExecutionRepository:
    """Durable attempt/acquisition/catalog coordinator under the single writer lease."""

    def __init__(self, store: OperationalStateStore) -> None:
        self._store = store

    def begin_attempt(
        self,
        lease: WriterLease,
        identity: AttemptIdentity,
        authorization: RequestPolicyAuthorization,
        *,
        started_at: datetime | None = None,
    ) -> DurableAttempt:
        """Durably bind one provider attempt to an exact sanitized request authorization."""

        authorization_json = _canonical_json(authorization)
        authorization_hash = _hash_json(authorization.model_dump(mode="json"))
        started = (
            self._store._now() if started_at is None else _as_utc(started_at, label="started_at")
        )
        if started < authorization.authorized_at:
            raise ExecutionStateConflictError("attempt cannot start before request authorization")
        policy_snapshot_id = deterministic_policy_snapshot_id(authorization.policy_snapshot)
        try:
            with self._store._leased_transaction(lease) as connection:
                request = self._require_request_scope(
                    connection,
                    identity.request_instance_id,
                    authorization,
                    policy_snapshot_id=policy_snapshot_id,
                )
                existing = connection.execute(
                    "SELECT * FROM request_attempts WHERE attempt_id = ?",
                    (str(identity.attempt_id),),
                ).fetchone()
                if existing is not None:
                    expected_attempt = (
                        str(identity.request_instance_id),
                        identity.attempt_number,
                    )
                    if (
                        _row_tuple(existing, ("request_instance_id", "attempt_number"))
                        != expected_attempt
                    ):
                        raise ExecutionIdentityCollisionError(
                            "attempt ID collides with another request or ordinal"
                        )
                    auth_row = connection.execute(
                        "SELECT * FROM attempt_request_authorizations WHERE attempt_id = ?",
                        (str(identity.attempt_id),),
                    ).fetchone()
                    expected_auth = (
                        str(identity.request_instance_id),
                        str(request["request_spec_id"]),
                        policy_snapshot_id,
                        authorization_hash,
                        authorization_json,
                        _format_utc(authorization.eligible_before),
                        _format_utc(authorization.authorized_at),
                    )
                    if (
                        auth_row is None
                        or _row_tuple(
                            auth_row,
                            (
                                "request_instance_id",
                                "request_spec_id",
                                "policy_snapshot_id",
                                "authorization_hash",
                                "authorization_json",
                                "eligible_before",
                                "authorized_at",
                            ),
                        )
                        != expected_auth
                    ):
                        raise ExecutionIdentityCollisionError(
                            "attempt authorization collides with durable metadata"
                        )
                    return DurableAttempt(
                        attempt_id=identity.attempt_id,
                        request_instance_id=identity.request_instance_id,
                        attempt_number=identity.attempt_number,
                        status=str(existing["status"]),
                        request_authorization_hash=authorization_hash,
                        started_at=_parse_utc(str(existing["started_at"])),
                        replayed=True,
                    )

                request_status = RequestInstanceStatus(str(request["request_status"]))
                if request_status not in {
                    RequestInstanceStatus.PLANNED,
                    RequestInstanceStatus.RETRY_WAIT,
                }:
                    raise ExecutionStateConflictError(
                        "new attempt requires a PLANNED or RETRY_WAIT request"
                    )
                latest = connection.execute(
                    """
                    SELECT max(attempt_number) AS latest
                    FROM request_attempts WHERE request_instance_id = ?
                    """,
                    (str(identity.request_instance_id),),
                ).fetchone()
                expected_number = (
                    1 if latest is None or latest["latest"] is None else int(latest["latest"]) + 1
                )
                if identity.attempt_number != expected_number:
                    raise ExecutionStateConflictError(
                        "attempt number must continue the durable 1-based sequence"
                    )
                if str(request["run_status"]) == IngestionRunStatus.PLANNED.value:
                    connection.execute(
                        """
                        UPDATE ingestion_runs
                        SET status = 'RUNNING', started_at = COALESCE(started_at, ?)
                        WHERE run_id = ? AND status = 'PLANNED'
                        """,
                        (_format_utc(started), str(request["run_id"])),
                    )
                elif str(request["run_status"]) != IngestionRunStatus.RUNNING.value:
                    raise ExecutionStateConflictError("terminal run cannot start another attempt")

                connection.execute(
                    """
                    UPDATE request_instances SET status = 'DISPATCHING', completed_at = NULL
                    WHERE request_instance_id = ? AND status = ?
                    """,
                    (str(identity.request_instance_id), request_status.value),
                )
                connection.execute(
                    """
                    UPDATE request_instances SET status = 'ACQUIRING'
                    WHERE request_instance_id = ? AND status = 'DISPATCHING'
                    """,
                    (str(identity.request_instance_id),),
                )
                connection.execute(
                    """
                    INSERT INTO request_attempts(
                        attempt_id, request_instance_id, attempt_number, status,
                        started_at, completed_at, next_eligible_at, safe_provider_request_id,
                        page_count, pagination_complete, terminal_page_verified
                    ) VALUES (?, ?, ?, 'RUNNING', ?, NULL, NULL, NULL, 0, 0, 0)
                    """,
                    (
                        str(identity.attempt_id),
                        str(identity.request_instance_id),
                        identity.attempt_number,
                        _format_utc(started),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO attempt_request_authorizations(
                        attempt_id, request_instance_id, request_spec_id,
                        policy_snapshot_id, authorization_hash, authorization_json,
                        eligible_before, authorized_at, recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(identity.attempt_id),
                        str(identity.request_instance_id),
                        str(request["request_spec_id"]),
                        policy_snapshot_id,
                        authorization_hash,
                        authorization_json,
                        _format_utc(authorization.eligible_before),
                        _format_utc(authorization.authorized_at),
                        _format_utc(started),
                    ),
                )
        except sqlite3.IntegrityError as error:
            raise ExecutionIdentityCollisionError(
                "SQLite rejected attempt identity; no partial attempt was committed"
            ) from error
        return DurableAttempt(
            attempt_id=identity.attempt_id,
            request_instance_id=identity.request_instance_id,
            attempt_number=identity.attempt_number,
            status="RUNNING",
            request_authorization_hash=authorization_hash,
            started_at=started,
            replayed=False,
        )

    def record_raw_artifact(
        self,
        lease: WriterLease,
        identity: AttemptIdentity,
        artifact_identity: RawArtifactIdentity,
        published: PublishedRawArtifact,
        metadata: RawBatchMetadata,
        *,
        observed_at: datetime,
    ) -> CatalogedRawArtifact:
        """Catalog one already published and verified raw page plus replay provenance."""

        observed_at = _as_utc(observed_at, label="observed_at")
        metadata = _safe_metadata(metadata)
        if str(published.root_id) != self._store.root_id:
            raise ExecutionIntegrityError("raw publication belongs to another private root")
        if (
            published.artifact_id != artifact_identity.artifact_id
            or published.content_sha256 != artifact_identity.content_sha256
            or published.byte_count != artifact_identity.byte_count
        ):
            raise ExecutionIntegrityError("raw publication disagrees with its content identity")
        if metadata.media_type.casefold() != artifact_identity.media_type:
            raise ExecutionIntegrityError(
                "raw replay media type differs from stored representation"
            )
        if not self._store._managed_file_matches_catalog(
            published.payload_relative_path,
            expected_sha256=published.content_sha256,
            expected_bytes=published.byte_count,
        ):
            raise ExecutionIntegrityError("raw payload is absent or failed integrity verification")
        raw_manifest_bytes = self._store._read_managed_file_bounded(
            published.manifest_relative_path,
            max_bytes=1024 * 1024,
        )
        try:
            raw_manifest = RawArtifactManifest.model_validate_json(raw_manifest_bytes)
        except ValueError as error:
            raise ExecutionIntegrityError("raw publication manifest is invalid") from error
        if (
            raw_manifest.identity != artifact_identity
            or raw_manifest.artifact_id != published.artifact_id
            or raw_manifest.payload.sha256 != published.content_sha256
            or raw_manifest.payload.byte_count != published.byte_count
            or raw_manifest.first_persisted_at != published.first_persisted_at
        ):
            raise ExecutionIntegrityError(
                "raw publication result differs from its persisted completion manifest"
            )
        manifest_sha256 = hashlib.sha256(raw_manifest_bytes).hexdigest()
        manifest_bytes = len(raw_manifest_bytes)
        metadata_hash = _hash_json(metadata.model_dump(mode="json"))
        request_metadata_json = _canonical_json(dict(metadata.request_metadata))
        try:
            with self._store._leased_transaction(lease) as connection:
                attempt = self._require_running_attempt(connection, identity)
                auth = RequestPolicyAuthorization.model_validate_json(
                    str(attempt["authorization_json"])
                )
                if artifact_identity.request_spec_hash != auth.request_spec_hash:
                    raise ExecutionIntegrityError("raw page belongs to another request")
                if (metadata.source.provider, metadata.source.dataset) != (
                    auth.policy_snapshot.provider,
                    auth.policy_snapshot.dataset,
                ):
                    raise ExecutionIntegrityError("raw replay source differs from exact dataset")
                replayed = self._persist_raw_artifact_row(
                    connection,
                    artifact_identity,
                    published,
                    manifest_sha256=manifest_sha256,
                    manifest_bytes=manifest_bytes,
                    verified_at=observed_at,
                )
                observation_values = (
                    str(identity.attempt_id),
                    artifact_identity.artifact_id,
                    _format_utc(metadata.retrieved_at),
                    _format_utc(observed_at),
                    metadata.provider_request_id,
                )
                row = connection.execute(
                    """
                    SELECT * FROM attempt_artifact_observations
                    WHERE attempt_id = ? AND artifact_id = ?
                    """,
                    (str(identity.attempt_id), artifact_identity.artifact_id),
                ).fetchone()
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO attempt_artifact_observations(
                            attempt_id, artifact_id, retrieved_at, observed_at,
                            safe_provider_request_id
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        observation_values,
                    )
                elif _row_tuple(
                    row,
                    (
                        "attempt_id",
                        "artifact_id",
                        "retrieved_at",
                        "safe_provider_request_id",
                    ),
                ) != (
                    observation_values[0],
                    observation_values[1],
                    observation_values[2],
                    observation_values[4],
                ):
                    raise ExecutionIdentityCollisionError(
                        "attempt/raw observation collides with durable provenance"
                    )
                provenance_values = (
                    str(identity.attempt_id),
                    artifact_identity.artifact_id,
                    str(metadata.batch_id),
                    str(metadata.source.source_id),
                    metadata.source.provider,
                    metadata.source.dataset,
                    metadata.source.logical_endpoint,
                    metadata.source.license_classification.value,
                    _format_utc(metadata.retrieved_at),
                    metadata.media_type,
                    metadata.file_extension,
                    metadata.provider_request_id,
                    request_metadata_json,
                    metadata_hash,
                    _format_utc(observed_at),
                )
                provenance = connection.execute(
                    """
                    SELECT * FROM raw_replay_provenance
                    WHERE attempt_id = ? AND artifact_id = ?
                    """,
                    (str(identity.attempt_id), artifact_identity.artifact_id),
                ).fetchone()
                columns = (
                    "attempt_id",
                    "artifact_id",
                    "raw_batch_id",
                    "source_id",
                    "source_provider",
                    "source_dataset",
                    "logical_endpoint",
                    "license_classification",
                    "retrieved_at",
                    "media_type",
                    "file_extension",
                    "safe_provider_request_id",
                    "request_metadata_json",
                    "metadata_hash",
                    "recorded_at",
                )
                if provenance is None:
                    connection.execute(
                        f"INSERT INTO raw_replay_provenance({', '.join(columns)}) "
                        f"VALUES ({', '.join('?' for _ in columns)})",
                        provenance_values,
                    )
                elif _row_tuple(provenance, columns[:-1]) != provenance_values[:-1]:
                    raise ExecutionIdentityCollisionError(
                        "raw replay metadata collides for the same attempt artifact"
                    )
                page_count = int(
                    connection.execute(
                        """
                        SELECT count(*) FROM attempt_artifact_observations
                        WHERE attempt_id = ?
                        """,
                        (str(identity.attempt_id),),
                    ).fetchone()[0]
                )
                connection.execute(
                    "UPDATE request_attempts SET page_count = ? WHERE attempt_id = ?",
                    (page_count, str(identity.attempt_id)),
                )
                replayed = replayed and row is not None and provenance is not None
        except sqlite3.IntegrityError as error:
            raise ExecutionIdentityCollisionError(
                "SQLite rejected raw catalog metadata; the transaction was rolled back"
            ) from error
        return CatalogedRawArtifact(
            attempt_id=identity.attempt_id,
            artifact_id=artifact_identity.artifact_id,
            raw_batch_id=metadata.batch_id,
            payload_relative_path=published.payload_relative_path,
            manifest_relative_path=published.manifest_relative_path,
            content_sha256=published.content_sha256,
            byte_count=published.byte_count,
            manifest_content_sha256=manifest_sha256,
            manifest_byte_count=manifest_bytes,
            replayed=replayed,
        )

    def complete_acquisition(
        self,
        lease: WriterLease,
        identity: AttemptIdentity,
        authorization: AcquisitionPolicyAuthorization,
        *,
        completed_at: datetime | None = None,
    ) -> DurableAcquisition:
        """Freeze verified complete pagination and move request/attempt to RAW_COMPLETE."""

        completed = (
            self._store._now()
            if completed_at is None
            else _as_utc(completed_at, label="completed_at")
        )
        authorization_json = _canonical_json(authorization)
        authorization_hash = _hash_json(authorization.model_dump(mode="json"))
        request_authorization_hash = _hash_json(authorization.request.model_dump(mode="json"))
        identities = _raw_identity_from_authorization(authorization)
        artifact_ids = tuple(value.artifact_id for value in identities)
        ordered_hash = _hash_json(
            {
                "canonicalization_version": 1,
                "kind": "ordered-raw-artifacts",
                "payload": {"artifact_ids": artifact_ids},
            }
        )
        if completed < authorization.authorized_at:
            raise ExecutionStateConflictError(
                "acquisition cannot complete before its authorization"
            )
        try:
            with self._store._leased_transaction(lease) as connection:
                attempt = self._require_attempt_authorization(
                    connection,
                    identity,
                    request_authorization_hash=request_authorization_hash,
                )
                status = str(attempt["status"])
                if status not in {"RUNNING", "RAW_COMPLETE", "SUCCESS"}:
                    raise ExecutionStateConflictError(
                        "only a running or completed attempt can freeze acquisition proof"
                    )
                request_status = str(attempt["request_status"])
                if request_status not in {
                    RequestInstanceStatus.ACQUIRING.value,
                    RequestInstanceStatus.RAW_COMPLETE.value,
                    RequestInstanceStatus.PROCESSING.value,
                    RequestInstanceStatus.SUCCESS.value,
                    RequestInstanceStatus.PARTIAL.value,
                }:
                    raise ExecutionStateConflictError(
                        "request state cannot accept completed acquisition proof"
                    )
                rows = connection.execute(
                    """
                    SELECT a.*, m.manifest_content_sha256, m.manifest_byte_count
                    FROM attempt_artifact_observations AS observation
                    JOIN raw_artifacts AS a ON a.artifact_id = observation.artifact_id
                    JOIN raw_artifact_manifests AS m ON m.artifact_id = a.artifact_id
                    WHERE observation.attempt_id = ?
                    ORDER BY a.page_ordinal
                    """,
                    (str(identity.attempt_id),),
                ).fetchall()
                if len(rows) != len(identities):
                    raise ExecutionIntegrityError(
                        "complete acquisition does not match cataloged response pages"
                    )
                for expected, row in zip(identities, rows, strict=True):
                    self._assert_raw_row_matches_identity(row, expected)
                    if str(row["state"]) != "VERIFIED":
                        raise ExecutionIntegrityError("acquisition contains unverified raw data")
                    if not self._store._managed_file_matches_catalog(
                        str(row["relative_path"]),
                        expected_sha256=str(row["content_sha256"]),
                        expected_bytes=int(row["byte_count"]),
                    ):
                        raise ExecutionIntegrityError(
                            "cataloged raw payload disappeared before acquisition completion"
                        )
                    if not self._store._managed_file_matches_catalog(
                        str(row["manifest_relative_path"]),
                        expected_sha256=str(row["manifest_content_sha256"]),
                        expected_bytes=int(row["manifest_byte_count"]),
                    ):
                        raise ExecutionIntegrityError(
                            "cataloged raw manifest disappeared before acquisition completion"
                        )

                record_values = (
                    str(identity.attempt_id),
                    str(identity.request_instance_id),
                    str(attempt["request_spec_id"]),
                    str(attempt["policy_snapshot_id"]),
                    authorization_hash,
                    authorization_json,
                    ordered_hash,
                    len(identities),
                    1,
                    1,
                    _format_utc(authorization.request.eligible_before),
                    _format_utc(authorization.authorized_at),
                    _format_utc(completed),
                )
                record = connection.execute(
                    "SELECT * FROM attempt_acquisition_records WHERE attempt_id = ?",
                    (str(identity.attempt_id),),
                ).fetchone()
                record_columns = (
                    "attempt_id",
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
                    "completed_at",
                )
                replayed = record is not None
                if record is None:
                    connection.execute(
                        f"INSERT INTO attempt_acquisition_records({', '.join(record_columns)}) "
                        f"VALUES ({', '.join('?' for _ in record_columns)})",
                        record_values,
                    )
                    for ordinal, (descriptor, raw_identity) in enumerate(
                        zip(authorization.ordered_artifacts, identities, strict=True)
                    ):
                        connection.execute(
                            """
                            INSERT INTO acquisition_artifacts(
                                attempt_id, artifact_id, ordinal, descriptor_hash
                            ) VALUES (?, ?, ?, ?)
                            """,
                            (
                                str(identity.attempt_id),
                                raw_identity.artifact_id,
                                ordinal,
                                _hash_json(descriptor.model_dump(mode="json")),
                            ),
                        )
                elif _row_tuple(record, record_columns[:-1]) != record_values[:-1]:
                    raise ExecutionIdentityCollisionError(
                        "completed acquisition collides with durable authorization proof"
                    )
                else:
                    completed = _parse_utc(str(record["completed_at"]))
                    links = connection.execute(
                        """
                        SELECT artifact_id, ordinal, descriptor_hash
                        FROM acquisition_artifacts WHERE attempt_id = ? ORDER BY ordinal
                        """,
                        (str(identity.attempt_id),),
                    ).fetchall()
                    expected_links = tuple(
                        (
                            raw_identity.artifact_id,
                            ordinal,
                            _hash_json(descriptor.model_dump(mode="json")),
                        )
                        for ordinal, (descriptor, raw_identity) in enumerate(
                            zip(authorization.ordered_artifacts, identities, strict=True)
                        )
                    )
                    if (
                        tuple(
                            (
                                str(row["artifact_id"]),
                                int(row["ordinal"]),
                                str(row["descriptor_hash"]),
                            )
                            for row in links
                        )
                        != expected_links
                    ):
                        raise ExecutionIdentityCollisionError(
                            "acquisition artifact ordering collides with durable proof"
                        )
                if status == "RUNNING":
                    connection.execute(
                        """
                        UPDATE request_attempts
                        SET status = 'RAW_COMPLETE', completed_at = ?, page_count = ?,
                            pagination_complete = 1, terminal_page_verified = 1
                        WHERE attempt_id = ? AND status = 'RUNNING'
                        """,
                        (_format_utc(completed), len(identities), str(identity.attempt_id)),
                    )
                if request_status == RequestInstanceStatus.ACQUIRING.value:
                    connection.execute(
                        """
                        UPDATE request_instances SET status = 'RAW_COMPLETE'
                        WHERE request_instance_id = ? AND status = 'ACQUIRING'
                        """,
                        (str(identity.request_instance_id),),
                    )
        except sqlite3.IntegrityError as error:
            raise ExecutionIdentityCollisionError(
                "SQLite rejected acquisition proof; no partial proof was committed"
            ) from error
        return DurableAcquisition(
            attempt_id=identity.attempt_id,
            request_instance_id=identity.request_instance_id,
            authorization_hash=authorization_hash,
            ordered_artifact_ids=artifact_ids,
            completed_at=completed,
            replayed=replayed,
        )

    def load_raw_replay(
        self,
        attempt_id: UUID,
        artifact_id: str,
    ) -> RawReplayRecord:
        """Load sanitized metadata needed to reconstruct a RawBatch after restart."""

        with self._store.read_only_connection() as connection:
            row = connection.execute(
                """
                SELECT raw.*, artifact.relative_path, artifact.manifest_relative_path,
                       artifact.content_sha256, artifact.byte_count, artifact.state,
                       manifest.manifest_content_sha256, manifest.manifest_byte_count
                FROM raw_replay_provenance AS raw
                JOIN raw_artifacts AS artifact ON artifact.artifact_id = raw.artifact_id
                JOIN raw_artifact_manifests AS manifest
                  ON manifest.artifact_id = raw.artifact_id
                WHERE raw.attempt_id = ? AND raw.artifact_id = ?
                """,
                (str(attempt_id), artifact_id),
            ).fetchone()
        if row is None:
            raise ExecutionStateConflictError("raw replay provenance is not cataloged")
        if str(row["state"]) != "VERIFIED" or not self._store._managed_file_matches_catalog(
            str(row["relative_path"]),
            expected_sha256=str(row["content_sha256"]),
            expected_bytes=int(row["byte_count"]),
        ):
            raise ExecutionIntegrityError("raw replay payload is no longer verified and present")
        if not self._store._managed_file_matches_catalog(
            str(row["manifest_relative_path"]),
            expected_sha256=str(row["manifest_content_sha256"]),
            expected_bytes=int(row["manifest_byte_count"]),
        ):
            raise ExecutionIntegrityError("raw replay manifest is no longer verified and present")
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
                "retrieved_at": str(row["retrieved_at"]),
                "media_type": str(row["media_type"]),
                "file_extension": str(row["file_extension"]),
                "provider_request_id": (
                    str(row["safe_provider_request_id"])
                    if row["safe_provider_request_id"] is not None
                    else None
                ),
                "request_metadata": json.loads(str(row["request_metadata_json"])),
            }
        )
        if _hash_json(metadata.model_dump(mode="json")) != str(row["metadata_hash"]):
            raise ExecutionIntegrityError("raw replay metadata hash no longer matches")
        return RawReplayRecord(
            attempt_id=attempt_id,
            artifact_id=artifact_id,
            payload_relative_path=str(row["relative_path"]),
            manifest_relative_path=str(row["manifest_relative_path"]),
            content_sha256=str(row["content_sha256"]),
            byte_count=int(row["byte_count"]),
            metadata=metadata,
        )

    def record_batch_context(
        self,
        lease: WriterLease,
        identity: AttemptIdentity,
        specification: RequestSpecification,
        context: BatchContext,
        *,
        calendar_snapshot_id: str,
        provenance: CanonicalPublicationProvenance,
        recorded_at: datetime | None = None,
    ) -> DurableBatchContext:
        """Freeze replay semantics and timestamps before canonical staging begins."""

        recorded = (
            self._store._now() if recorded_at is None else _as_utc(recorded_at, label="recorded_at")
        )
        processing = context.batch_identity.processing_signature
        processing_json = _canonical_json(processing)
        processing_hash = _hash_json(processing.model_dump(mode="json"))
        provenance_json = _canonical_json(provenance)
        provenance_hash = _hash_json(provenance.model_dump(mode="json"))
        artifact_ids = context.batch_identity.artifact_ids
        if context.batch_identity.request_spec_hash != specification.request_spec_hash:
            raise ExecutionIntegrityError("batch context belongs to another request")
        if tuple(value.artifact_id for value in provenance.raw_bindings) != artifact_ids:
            raise ExecutionIntegrityError("batch context raw provenance order is inconsistent")
        try:
            with self._store._leased_transaction(lease) as connection:
                acquisition = self._require_completed_acquisition(connection, identity)
                request = connection.execute(
                    """
                    SELECT i.*, s.request_spec_hash, s.specification_json
                    FROM request_instances AS i
                    JOIN request_specs AS s ON s.request_spec_id = i.request_spec_id
                    WHERE i.request_instance_id = ?
                    """,
                    (str(identity.request_instance_id),),
                ).fetchone()
                if request is None:
                    raise ExecutionStateConflictError("batch request is not cataloged")
                if (
                    str(request["request_spec_hash"]) != specification.request_spec_hash
                    or str(request["specification_json"]) != specification.canonical_json
                ):
                    raise ExecutionIdentityCollisionError(
                        "batch request specification differs from durable plan"
                    )
                calendar = connection.execute(
                    """
                    SELECT schedule_checksum, state FROM calendar_snapshots
                    WHERE calendar_snapshot_id = ?
                    """,
                    (calendar_snapshot_id,),
                ).fetchone()
                if (
                    calendar is None
                    or str(calendar["state"]) != "CURRENT"
                    or str(calendar["schedule_checksum"]) != processing.calendar_snapshot_checksum
                ):
                    raise ExecutionIntegrityError(
                        "batch processing signature lacks the exact current calendar snapshot"
                    )
                existing = connection.execute(
                    "SELECT * FROM batch_contexts WHERE batch_context_id = ?",
                    (context.batch_context_id,),
                ).fetchone()
                acquisition_links = connection.execute(
                    """
                    SELECT link.artifact_id, link.ordinal, raw.raw_batch_id
                    FROM acquisition_artifacts AS link
                    JOIN raw_replay_provenance AS raw
                      ON raw.attempt_id = link.attempt_id
                     AND raw.artifact_id = link.artifact_id
                    WHERE link.attempt_id = ? ORDER BY link.ordinal
                    """,
                    (str(identity.attempt_id),),
                ).fetchall()
                actual_artifacts = tuple(str(row["artifact_id"]) for row in acquisition_links)
                actual_raw_batches = tuple(str(row["raw_batch_id"]) for row in acquisition_links)
                if actual_artifacts != artifact_ids:
                    raise ExecutionIntegrityError(
                        "batch context differs from the complete acquisition artifact sequence"
                    )
                if existing is None and actual_raw_batches != tuple(
                    str(value.raw_batch_id) for value in provenance.raw_bindings
                ):
                    raise ExecutionIntegrityError(
                        "new batch context differs from attempt-specific raw provenance"
                    )

                context_values = (
                    context.batch_context_id,
                    context.canonical_batch_id,
                    specification.request_spec_id,
                    context.batch_identity.ordered_artifacts_hash,
                    processing.canonical_schema_version,
                    processing.normalizer_version,
                    processing.validator_version,
                    calendar_snapshot_id,
                    _format_utc(context.fixed_ingested_at),
                    _format_utc(context.manifest_created_at),
                    _format_utc(recorded),
                )
                columns = (
                    "batch_context_id",
                    "canonical_batch_id",
                    "request_spec_id",
                    "ordered_artifacts_hash",
                    "canonical_schema_version",
                    "normalizer_version",
                    "validator_version",
                    "calendar_snapshot_id",
                    "fixed_ingested_at",
                    "manifest_created_at",
                    "created_at",
                )
                replayed = existing is not None
                if existing is None:
                    connection.execute(
                        f"INSERT INTO batch_contexts({', '.join(columns)}) "
                        f"VALUES ({', '.join('?' for _ in columns)})",
                        context_values,
                    )
                    for ordinal, artifact_id in enumerate(artifact_ids):
                        connection.execute(
                            """
                            INSERT INTO batch_context_artifacts(
                                batch_context_id, artifact_id, ordinal
                            ) VALUES (?, ?, ?)
                            """,
                            (context.batch_context_id, artifact_id, ordinal),
                        )
                elif _row_tuple(existing, columns[:-1]) != context_values[:-1]:
                    raise ExecutionIdentityCollisionError(
                        "batch context identity collides with different fixed semantics"
                    )
                else:
                    recorded = _parse_utc(str(existing["created_at"]))
                    links = connection.execute(
                        """
                        SELECT artifact_id, ordinal FROM batch_context_artifacts
                        WHERE batch_context_id = ? ORDER BY ordinal
                        """,
                        (context.batch_context_id,),
                    ).fetchall()
                    if tuple(
                        (str(row["artifact_id"]), int(row["ordinal"])) for row in links
                    ) != tuple((value, ordinal) for ordinal, value in enumerate(artifact_ids)):
                        raise ExecutionIdentityCollisionError(
                            "batch context artifact order collides with durable metadata"
                        )
                contract_values = (
                    context.batch_context_id,
                    processing_hash,
                    processing_json,
                    str(provenance.source_id),
                    provenance_json,
                    provenance_hash,
                    _format_utc(recorded),
                )
                contract_columns = (
                    "batch_context_id",
                    "processing_signature_hash",
                    "processing_signature_json",
                    "source_id",
                    "provenance_json",
                    "provenance_hash",
                    "recorded_at",
                )
                contract = connection.execute(
                    """
                    SELECT * FROM batch_context_processing_contracts
                    WHERE batch_context_id = ?
                    """,
                    (context.batch_context_id,),
                ).fetchone()
                if contract is None:
                    connection.execute(
                        f"INSERT INTO batch_context_processing_contracts"
                        f"({', '.join(contract_columns)}) VALUES "
                        f"({', '.join('?' for _ in contract_columns)})",
                        contract_values,
                    )
                elif _row_tuple(contract, contract_columns[:-1]) != contract_values[:-1]:
                    raise ExecutionIdentityCollisionError(
                        "batch processing/provenance contract collides with durable metadata"
                    )
                link = connection.execute(
                    """
                    SELECT linked_at FROM batch_context_requests
                    WHERE batch_context_id = ? AND request_instance_id = ?
                    """,
                    (context.batch_context_id, str(identity.request_instance_id)),
                ).fetchone()
                if link is None:
                    connection.execute(
                        """
                        INSERT INTO batch_context_requests(
                            batch_context_id, request_instance_id, linked_at
                        ) VALUES (?, ?, ?)
                        """,
                        (
                            context.batch_context_id,
                            str(identity.request_instance_id),
                            _format_utc(recorded),
                        ),
                    )
                if str(request["status"]) == RequestInstanceStatus.RAW_COMPLETE.value:
                    connection.execute(
                        """
                        UPDATE request_instances SET status = 'PROCESSING'
                        WHERE request_instance_id = ? AND status = 'RAW_COMPLETE'
                        """,
                        (str(identity.request_instance_id),),
                    )
                elif str(request["status"]) not in {
                    RequestInstanceStatus.PROCESSING.value,
                    RequestInstanceStatus.SUCCESS.value,
                    RequestInstanceStatus.PARTIAL.value,
                }:
                    raise ExecutionStateConflictError(
                        "request is not ready for durable batch processing"
                    )
                if str(acquisition["request_instance_id"]) != str(identity.request_instance_id):
                    raise ExecutionIntegrityError("batch context acquisition/request mismatch")
        except sqlite3.IntegrityError as error:
            raise ExecutionIdentityCollisionError(
                "SQLite rejected batch context; no partial context was committed"
            ) from error
        return DurableBatchContext(
            request_instance_id=identity.request_instance_id,
            attempt_id=identity.attempt_id,
            canonical_batch_id=context.canonical_batch_id,
            batch_context_id=context.batch_context_id,
            recorded_at=recorded,
            replayed=replayed,
        )

    def prepare_publication(
        self,
        lease: WriterLease,
        identity: AttemptIdentity,
        expectation: CanonicalBatchExpectation,
        *,
        prepared_at: datetime | None = None,
    ) -> PreparedBatch:
        """Persist the semantic expectation required to adopt an orphan after restart."""

        prepared = (
            self._store._now() if prepared_at is None else _as_utc(prepared_at, label="prepared_at")
        )
        expectation_json = _canonical_json(expectation)
        expectation_hash = _hash_json(expectation.model_dump(mode="json"))
        context = expectation.batch_context
        try:
            with self._store._leased_transaction(lease) as connection:
                self._require_completed_acquisition(connection, identity)
                self._assert_durable_batch_context(
                    connection,
                    identity,
                    expectation,
                )
                row = connection.execute(
                    """
                    SELECT * FROM batch_publication_expectations
                    WHERE batch_context_id = ?
                    """,
                    (context.batch_context_id,),
                ).fetchone()
                semantic_values = (
                    context.batch_context_id,
                    context.canonical_batch_id,
                    expectation_hash,
                    expectation_json,
                    _format_utc(prepared),
                )
                semantic_columns = (
                    "batch_context_id",
                    "canonical_batch_id",
                    "expectation_hash",
                    "expectation_json",
                    "first_prepared_at",
                )
                if row is None:
                    connection.execute(
                        "INSERT INTO batch_publication_expectations"
                        f"({', '.join(semantic_columns)}) VALUES "
                        f"({', '.join('?' for _ in semantic_columns)})",
                        semantic_values,
                    )
                elif _row_tuple(row, semantic_columns[:-1]) != semantic_values[:-1]:
                    raise ExecutionIdentityCollisionError(
                        "publication expectation collides with durable semantics"
                    )
                request_row = connection.execute(
                    """
                    SELECT * FROM batch_publication_expectation_requests
                    WHERE batch_context_id = ? AND request_instance_id = ?
                    """,
                    (context.batch_context_id, str(identity.request_instance_id)),
                ).fetchone()
                replayed = request_row is not None
                if request_row is None:
                    connection.execute(
                        """
                        INSERT INTO batch_publication_expectation_requests(
                            batch_context_id, request_instance_id, state,
                            prepared_at, cataloged_at, abandoned_at
                        ) VALUES (?, ?, 'PREPARED', ?, NULL, NULL)
                        """,
                        (
                            context.batch_context_id,
                            str(identity.request_instance_id),
                            _format_utc(prepared),
                        ),
                    )
                    state = "PREPARED"
                else:
                    state = str(request_row["state"])
                    prepared = _parse_utc(str(request_row["prepared_at"]))
                    if state == "ABANDONED":
                        raise ExecutionStateConflictError(
                            "abandoned publication expectation cannot be reused"
                        )
        except sqlite3.IntegrityError as error:
            raise ExecutionIdentityCollisionError(
                "SQLite rejected publication expectation; no partial record was committed"
            ) from error
        return PreparedBatch(
            request_instance_id=identity.request_instance_id,
            attempt_id=identity.attempt_id,
            canonical_batch_id=context.canonical_batch_id,
            batch_context_id=context.batch_context_id,
            expectation_hash=expectation_hash,
            state=state,
            prepared_at=prepared,
            replayed=replayed,
        )

    def load_prepared_publication(
        self,
        canonical_batch_id: str,
        request_instance_id: UUID | None = None,
    ) -> PreparedPublicationRecord:
        """Reconstruct the exact persisted semantic expectation for recovery/adoption."""

        with self._store.read_only_connection() as connection:
            parameters: tuple[str, ...]
            predicate = ""
            parameters = (canonical_batch_id,)
            if request_instance_id is not None:
                predicate = " AND link.request_instance_id = ?"
                parameters = (canonical_batch_id, str(request_instance_id))
            rows = connection.execute(
                """
                SELECT expectation.*, link.request_instance_id, link.state,
                       link.prepared_at, link.cataloged_at, link.abandoned_at
                FROM batch_publication_expectations AS expectation
                JOIN batch_publication_expectation_requests AS link
                  ON link.batch_context_id = expectation.batch_context_id
                WHERE expectation.canonical_batch_id = ?
                """
                + predicate
                + " ORDER BY link.prepared_at, link.request_instance_id",
                parameters,
            ).fetchall()
        if not rows:
            raise ExecutionStateConflictError("publication expectation is not cataloged")
        if len(rows) != 1:
            raise ExecutionStateConflictError(
                "canonical batch has multiple request-specific publication states"
            )
        row = rows[0]
        try:
            expectation = CanonicalBatchExpectation.model_validate_json(
                str(row["expectation_json"])
            )
        except ValueError as error:
            raise ExecutionIntegrityError(
                "persisted publication expectation failed schema validation"
            ) from error
        if expectation.batch_context.canonical_batch_id != canonical_batch_id or _hash_json(
            expectation.model_dump(mode="json")
        ) != str(row["expectation_hash"]):
            raise ExecutionIntegrityError("publication expectation hash no longer matches")
        return PreparedPublicationRecord(
            request_instance_id=UUID(str(row["request_instance_id"])),
            canonical_batch_id=canonical_batch_id,
            state=str(row["state"]),
            expectation=expectation,
            prepared_at=_parse_utc(str(row["prepared_at"])),
        )

    def list_uncataloged_publications(self) -> tuple[PreparedPublicationRecord, ...]:
        """List prepared contexts that recovery should inspect on the filesystem."""

        with self._store.read_only_connection() as connection:
            rows = connection.execute(
                """
                SELECT expectation.canonical_batch_id, link.request_instance_id
                FROM batch_publication_expectations AS expectation
                JOIN batch_publication_expectation_requests AS link
                  ON link.batch_context_id = expectation.batch_context_id
                WHERE link.state = 'PREPARED'
                ORDER BY link.prepared_at, expectation.canonical_batch_id,
                         link.request_instance_id
                """
            ).fetchall()
        return tuple(
            self.load_prepared_publication(
                str(row["canonical_batch_id"]),
                UUID(str(row["request_instance_id"])),
            )
            for row in rows
        )

    def commit_published_batch(
        self,
        lease: WriterLease,
        request: PublicationCommitRequest,
        *,
        fault_injector: FaultInjector | None = None,
    ) -> PublicationCommitResult:
        """Atomically catalog a verified publication and advance only proven state.

        All filesystem bytes are verified before ``BEGIN IMMEDIATE``.  SQLite is then the single
        atomic boundary for catalog rows, coverage/gaps, watermark, and terminal request proof.
        Aggregate run reconciliation follows in its own recoverable transaction. Retrying the
        same request is an idempotent replay; any semantic difference for the same IDs fails
        closed.
        """

        manifest_sha256, manifest_bytes = self._verify_publication_files(request)
        coverage_hash = request.coverage.commit_hash
        policy_snapshot_id = deterministic_policy_snapshot_id(
            request.acquisition_authorization.request.policy_snapshot
        )
        if request.coverage.policy_snapshot_id != policy_snapshot_id:
            raise ExecutionIntegrityError("coverage uses a different retention policy snapshot")
        committed = self._store._now()
        try:
            with self._store._leased_transaction(lease) as connection:
                existing_commit = connection.execute(
                    """
                    SELECT publication.*, proof.terminal_status,
                           instance.status AS request_status,
                           run.status AS run_status, instance.run_id
                    FROM publication_commits AS publication
                    JOIN request_terminal_proofs AS proof
                      ON proof.request_instance_id = publication.request_instance_id
                    JOIN request_instances AS instance
                      ON instance.request_instance_id = publication.request_instance_id
                    JOIN ingestion_runs AS run ON run.run_id = instance.run_id
                    WHERE publication.canonical_batch_id = ?
                      AND publication.request_instance_id = ?
                    """,
                    (
                        request.manifest.canonical_batch_id,
                        str(request.request_instance_id),
                    ),
                ).fetchone()
                if existing_commit is not None:
                    self._validate_commit_replay(
                        existing_commit,
                        request,
                        coverage_hash=coverage_hash,
                    )
                    return PublicationCommitResult(
                        run_id=UUID(str(existing_commit["run_id"])),
                        request_instance_id=request.request_instance_id,
                        canonical_batch_id=request.manifest.canonical_batch_id,
                        coverage_commit_hash=coverage_hash,
                        request_status=RequestInstanceStatus(
                            str(existing_commit["request_status"])
                        ),
                        run_status=IngestionRunStatus(str(existing_commit["run_status"])),
                        committed_at=_parse_utc(str(existing_commit["committed_at"])),
                        replayed=True,
                    )

                durable = self._require_publication_commit_inputs(
                    connection,
                    request,
                    policy_snapshot_id=policy_snapshot_id,
                )
                self._persist_canonical_catalog(
                    connection,
                    request,
                    policy_snapshot_id=policy_snapshot_id,
                    manifest_sha256=manifest_sha256,
                    manifest_bytes=manifest_bytes,
                    verified_at=committed,
                )
                _invoke_fault(fault_injector, ExecutionFaultPoint.SQLITE_TRANSACTION)
                authorization_hash = str(durable["authorization_hash"])
                self._persist_coverage(
                    connection,
                    request,
                    policy_snapshot_id=policy_snapshot_id,
                    authorization_hash=authorization_hash,
                )
                self._persist_gaps(connection, request)
                self._invalidate_watermarks_blocked_by_gaps(
                    connection,
                    request,
                    invalidated_at=committed,
                )
                self._persist_watermarks(
                    connection,
                    request,
                    policy_snapshot_id=policy_snapshot_id,
                )
                _invoke_fault(fault_injector, ExecutionFaultPoint.WATERMARK_UPDATE)
                connection.execute(
                    """
                    INSERT INTO publication_commits(
                        canonical_batch_id, request_instance_id, attempt_id,
                        coverage_commit_hash, commit_source, lease_owner_id,
                        lease_generation, committed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request.manifest.canonical_batch_id,
                        str(request.request_instance_id),
                        str(request.attempt_id),
                        coverage_hash,
                        request.source.value,
                        lease.owner_id,
                        lease.generation,
                        _format_utc(committed),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO request_terminal_proofs(
                        request_instance_id, attempt_id, canonical_batch_id,
                        coverage_commit_hash, terminal_status, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(request.request_instance_id),
                        str(request.attempt_id),
                        request.manifest.canonical_batch_id,
                        coverage_hash,
                        request.terminal_status.value,
                        _format_utc(committed),
                    ),
                )
                connection.execute(
                    """
                    UPDATE batch_publication_expectation_requests
                    SET state = 'CATALOGED', cataloged_at = ?
                    WHERE batch_context_id = ? AND request_instance_id = ?
                      AND state = 'PREPARED'
                    """,
                    (
                        _format_utc(committed),
                        request.manifest.batch_context_id,
                        str(request.request_instance_id),
                    ),
                )
                attempt_status = str(durable["attempt_status"])
                if attempt_status == "RAW_COMPLETE":
                    connection.execute(
                        """
                        UPDATE request_attempts SET status = 'SUCCESS', completed_at = ?
                        WHERE attempt_id = ? AND status = 'RAW_COMPLETE'
                        """,
                        (_format_utc(committed), str(request.attempt_id)),
                    )
                elif attempt_status != "SUCCESS":
                    raise ExecutionStateConflictError(
                        "publication commit requires a RAW_COMPLETE attempt"
                    )
                request_status = str(durable["request_status"])
                if request_status == RequestInstanceStatus.PROCESSING.value:
                    connection.execute(
                        """
                        UPDATE request_instances SET status = ?, completed_at = ?
                        WHERE request_instance_id = ? AND status = 'PROCESSING'
                        """,
                        (
                            request.terminal_status.value,
                            _format_utc(committed),
                            str(request.request_instance_id),
                        ),
                    )
                elif request_status != request.terminal_status.value:
                    raise ExecutionStateConflictError(
                        "publication request is not PROCESSING or the same terminal state"
                    )
                run_id = str(durable["run_id"])
                run_status = IngestionRunStatus(str(durable["run_status"]))
        except sqlite3.IntegrityError as error:
            raise ExecutionIdentityCollisionError(
                "SQLite rejected publication commit; all operational effects were rolled back"
            ) from error
        return PublicationCommitResult(
            run_id=UUID(run_id),
            request_instance_id=request.request_instance_id,
            canonical_batch_id=request.manifest.canonical_batch_id,
            coverage_commit_hash=coverage_hash,
            request_status=request.terminal_status,
            run_status=run_status,
            committed_at=committed,
            replayed=False,
        )

    def adopt_published_batch(
        self,
        lease: WriterLease,
        request: PublicationCommitRequest,
        *,
        fault_injector: FaultInjector | None = None,
    ) -> PublicationCommitResult:
        """Catalog a content-verified orphan only against its persisted expectation."""

        if request.source is not PublicationCommitSource.RECOVERY_ADOPTION:
            raise ExecutionStateConflictError(
                "orphan adoption must be explicitly marked RECOVERY_ADOPTION"
            )
        prepared = self.load_prepared_publication(
            request.manifest.canonical_batch_id,
            request.request_instance_id,
        )
        if prepared.state not in {"PREPARED", "CATALOGED"} or (
            prepared.expectation != request.expectation
        ):
            raise ExecutionIntegrityError(
                "orphan publication differs from its pre-publication durable expectation"
            )
        return self.commit_published_batch(
            lease,
            request,
            fault_injector=fault_injector,
        )

    def reconcile_run(
        self,
        lease: WriterLease,
        run_id: UUID,
        *,
        completed_at: datetime | None = None,
        fault_injector: FaultInjector | None = None,
    ) -> IngestionRunStatus:
        """Repair a missing run summary after request terminal state survived a restart."""

        completed = (
            self._store._now()
            if completed_at is None
            else _as_utc(completed_at, label="completed_at")
        )
        with self._store._leased_transaction(lease) as connection:
            _, status = self._reconcile_run(connection, str(run_id), completed_at=completed)
            _invoke_fault(fault_injector, ExecutionFaultPoint.RUN_COMPLETION)
            return status

    def fail_processing_request(
        self,
        lease: WriterLease,
        identity: AttemptIdentity,
        *,
        terminal_status: RequestInstanceStatus,
        category: str,
        code: str,
        sanitized_message: str,
        completed_at: datetime | None = None,
    ) -> TerminalFailureResult:
        """Terminate an all-blocked or failed processing path without publishing a batch."""

        if terminal_status not in {
            RequestInstanceStatus.FAILED,
            RequestInstanceStatus.BLOCKED,
        }:
            raise ValueError("non-publication processing outcome must be FAILED or BLOCKED")
        if not re.fullmatch(_SAFE_CODE, category) or not re.fullmatch(_SAFE_CODE, code):
            raise ValueError("failure category/code must be sanitized stable identifiers")
        if (
            not 1 <= len(sanitized_message) <= 512
            or "\r" in sanitized_message
            or "\n" in sanitized_message
            or _SECRET_TEXT.search(sanitized_message)
        ):
            raise ValueError("failure message contains unsafe or secret-shaped text")
        completed = (
            self._store._now()
            if completed_at is None
            else _as_utc(completed_at, label="completed_at")
        )
        error_id = "error_v1_" + _hash_json(
            {
                "attempt_id": str(identity.attempt_id),
                "request_instance_id": str(identity.request_instance_id),
                "terminal_status": terminal_status.value,
                "category": category,
                "code": code,
            }
        )
        with self._store._leased_transaction(lease) as connection:
            acquisition = self._require_completed_acquisition(connection, identity)
            request = connection.execute(
                "SELECT * FROM request_instances WHERE request_instance_id = ?",
                (str(identity.request_instance_id),),
            ).fetchone()
            if request is None:
                raise ExecutionStateConflictError("processing request is not cataloged")
            run_id = str(request["run_id"])
            existing_error = connection.execute(
                "SELECT * FROM errors WHERE error_id = ?",
                (error_id,),
            ).fetchone()
            values = (
                error_id,
                run_id,
                str(identity.request_instance_id),
                str(identity.attempt_id),
                category,
                code,
                sanitized_message,
                0,
                _format_utc(completed),
            )
            columns = (
                "error_id",
                "run_id",
                "request_instance_id",
                "attempt_id",
                "category",
                "code",
                "sanitized_message",
                "retryable",
                "occurred_at",
            )
            replayed = existing_error is not None
            if existing_error is None:
                connection.execute(
                    f"INSERT INTO errors({', '.join(columns)}) "
                    f"VALUES ({', '.join('?' for _ in columns)})",
                    values,
                )
            else:
                immutable_columns = columns[:-1]
                if _row_tuple(existing_error, immutable_columns) != values[:-1]:
                    raise ExecutionIdentityCollisionError(
                        "failure identity collides with different sanitized facts"
                    )
                completed = _parse_utc(str(existing_error["occurred_at"]))
            attempt_status = str(acquisition["attempt_status"])
            if attempt_status == "RAW_COMPLETE":
                connection.execute(
                    """
                    UPDATE request_attempts SET status = 'SUCCESS', completed_at = ?
                    WHERE attempt_id = ? AND status = 'RAW_COMPLETE'
                    """,
                    (_format_utc(completed), str(identity.attempt_id)),
                )
            elif attempt_status != "SUCCESS":
                raise ExecutionStateConflictError(
                    "processing failure lacks successful raw acquisition"
                )
            current = RequestInstanceStatus(str(request["status"]))
            if current is RequestInstanceStatus.RAW_COMPLETE:
                connection.execute(
                    """
                    UPDATE request_instances SET status = 'PROCESSING'
                    WHERE request_instance_id = ? AND status = 'RAW_COMPLETE'
                    """,
                    (str(identity.request_instance_id),),
                )
                current = RequestInstanceStatus.PROCESSING
            if current is RequestInstanceStatus.PROCESSING:
                connection.execute(
                    """
                    UPDATE request_instances SET status = ?, completed_at = ?
                    WHERE request_instance_id = ? AND status = 'PROCESSING'
                    """,
                    (
                        terminal_status.value,
                        _format_utc(completed),
                        str(identity.request_instance_id),
                    ),
                )
            elif current is not terminal_status:
                raise ExecutionStateConflictError(
                    "request already has a different terminal processing outcome"
                )
            connection.execute(
                """
                UPDATE batch_publication_expectation_requests
                SET state = 'ABANDONED', abandoned_at = ?
                WHERE request_instance_id = ? AND state = 'PREPARED'
                """,
                (_format_utc(completed), str(identity.request_instance_id)),
            )
        return TerminalFailureResult(
            run_id=UUID(run_id),
            request_instance_id=identity.request_instance_id,
            attempt_id=identity.attempt_id,
            request_status=terminal_status,
            error_id=error_id,
            completed_at=completed,
            replayed=replayed,
        )

    def _verify_publication_files(
        self,
        request: PublicationCommitRequest,
    ) -> tuple[str, int]:
        manifest = request.manifest
        expectation = request.expectation
        published = request.published
        if str(published.root_id) != self._store.root_id:
            raise ExecutionIntegrityError("canonical publication belongs to another private root")
        if (
            published.canonical_batch_id != manifest.canonical_batch_id
            or published.row_count != manifest.row_count
            or published.files != manifest.files
            or expectation.batch_context.canonical_batch_id != manifest.canonical_batch_id
            or expectation.batch_context.batch_context_id != manifest.batch_context_id
            or expectation.specification.request_spec_hash != manifest.request_spec_hash
            or expectation.specification.provider != manifest.provider
            or expectation.specification.dataset != manifest.dataset
            or expectation.batch_context.batch_identity.ordered_artifacts
            != manifest.ordered_raw_artifacts
            or expectation.batch_context.batch_identity.processing_signature
            != manifest.processing_signature
            or expectation.calendar_snapshot != manifest.calendar_snapshot
            or expectation.provenance != manifest.provenance
            or tuple(sorted(expectation.streams, key=lambda value: value.stream_id))
            != manifest.streams
            or expectation.batch_context.fixed_ingested_at != manifest.fixed_ingested_at
            or expectation.batch_context.manifest_created_at != manifest.manifest_created_at
        ):
            raise ExecutionIntegrityError(
                "published batch differs from its exact semantic expectation"
            )
        authorized_raw = _raw_identity_from_authorization(request.acquisition_authorization)
        if authorized_raw != manifest.ordered_raw_artifacts:
            raise ExecutionIntegrityError(
                "published batch does not derive from the authorized complete acquisition"
            )
        directory = PurePosixPath(published.relative_directory)
        expected_manifest_path = (directory / "manifest.json").as_posix()
        if published.manifest_relative_path != expected_manifest_path:
            raise ExecutionIntegrityError("canonical manifest path is inconsistent")
        manifest_content = self._store._read_managed_file_bounded(
            published.manifest_relative_path,
            max_bytes=16 * 1024 * 1024,
        )
        try:
            persisted_manifest = CanonicalBatchManifest.model_validate_json(manifest_content)
        except ValueError as error:
            raise ExecutionIntegrityError("canonical completion manifest is invalid") from error
        if persisted_manifest != manifest:
            raise ExecutionIntegrityError(
                "caller manifest differs from the persisted canonical completion manifest"
            )
        manifest_sha256 = hashlib.sha256(manifest_content).hexdigest()
        manifest_bytes = len(manifest_content)
        for file in manifest.files:
            full_path = (directory / PurePosixPath(file.relative_path)).as_posix()
            if not self._store._managed_file_matches_catalog(
                full_path,
                expected_sha256=file.sha256,
                expected_bytes=file.byte_count,
            ):
                raise ExecutionIntegrityError(
                    "canonical Parquet file is absent or changed after publication verification"
                )
        return manifest_sha256, manifest_bytes

    def _require_publication_commit_inputs(
        self,
        connection: sqlite3.Connection,
        request: PublicationCommitRequest,
        *,
        policy_snapshot_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT instance.run_id, instance.request_spec_id,
                   instance.status AS request_status,
                   spec.request_spec_hash, spec.specification_json,
                   run.status AS run_status, run.policy_snapshot_id,
                   attempt.status AS attempt_status,
                   acquisition.authorization_hash,
                   acquisition.authorization_json,
                   acquisition.ordered_artifacts_hash,
                   acquisition.page_count,
                   expectation.expectation_hash,
                   expectation.expectation_json,
                   expectation_request.state AS expectation_state,
                   policy.provider, policy.dataset, policy.retention_mode,
                   active.status AS active_policy_status,
                   active.policy_snapshot_id AS active_policy_snapshot_id,
                   active.retention_mode AS active_retention_mode,
                   active.expires_at AS active_expires_at,
                   active.unavailable_at AS active_unavailable_at
            FROM request_instances AS instance
            JOIN request_specs AS spec ON spec.request_spec_id = instance.request_spec_id
            JOIN ingestion_runs AS run ON run.run_id = instance.run_id
            JOIN request_attempts AS attempt
              ON attempt.request_instance_id = instance.request_instance_id
            JOIN attempt_acquisition_records AS acquisition
              ON acquisition.attempt_id = attempt.attempt_id
            JOIN batch_context_requests AS context_request
              ON context_request.request_instance_id = instance.request_instance_id
            JOIN batch_publication_expectations AS expectation
              ON expectation.batch_context_id = context_request.batch_context_id
            JOIN batch_publication_expectation_requests AS expectation_request
              ON expectation_request.batch_context_id = expectation.batch_context_id
             AND expectation_request.request_instance_id = instance.request_instance_id
            JOIN policy_snapshots AS policy
              ON policy.policy_snapshot_id = run.policy_snapshot_id
            LEFT JOIN dataset_policy_status AS active
              ON active.provider = policy.provider AND active.dataset = policy.dataset
            WHERE instance.request_instance_id = ? AND attempt.attempt_id = ?
              AND expectation.canonical_batch_id = ?
            """,
            (
                str(request.request_instance_id),
                str(request.attempt_id),
                request.manifest.canonical_batch_id,
            ),
        ).fetchone()
        if row is None:
            raise ExecutionStateConflictError(
                "publication lacks durable request, acquisition, or expectation state"
            )
        authorization = request.acquisition_authorization
        expectation_hash = _hash_json(request.expectation.model_dump(mode="json"))
        authorization_hash = _hash_json(authorization.model_dump(mode="json"))
        if (
            str(row["request_spec_hash"]) != request.expectation.specification.request_spec_hash
            or str(row["specification_json"]) != request.expectation.specification.canonical_json
            or str(row["policy_snapshot_id"]) != policy_snapshot_id
            or str(row["authorization_hash"]) != authorization_hash
            or str(row["authorization_json"]) != _canonical_json(authorization)
            or int(row["page_count"]) != len(authorization.ordered_artifacts)
            or str(row["ordered_artifacts_hash"])
            != request.expectation.batch_context.batch_identity.ordered_artifacts_hash
            or str(row["expectation_hash"]) != expectation_hash
            or str(row["expectation_json"]) != _canonical_json(request.expectation)
            or str(row["expectation_state"]) != "PREPARED"
            or str(row["provider"]) != request.manifest.provider
            or str(row["dataset"]) != request.manifest.dataset
        ):
            raise ExecutionIdentityCollisionError(
                "publication inputs differ from their durable acquisition/context proof"
            )
        if (
            str(row["active_policy_status"]) != DatasetPolicyStatus.ACTIVE.value
            or str(row["active_policy_snapshot_id"]) != policy_snapshot_id
            or str(row["active_retention_mode"]) != str(row["retention_mode"])
            or row["active_unavailable_at"] is not None
            or str(row["retention_mode"])
            in {RetentionMode.PROHIBITED.value, RetentionMode.EPHEMERAL.value}
        ):
            raise ExecutionIntegrityError(
                "current exact dataset policy cannot support durable publication"
            )
        if (
            row["active_expires_at"] is not None
            and _parse_utc(str(row["active_expires_at"])) <= self._store._now()
        ):
            raise ExecutionIntegrityError("dataset policy state expired before publication commit")
        if str(row["request_status"]) != RequestInstanceStatus.PROCESSING.value:
            raise ExecutionStateConflictError("request is not ready for publication commit")
        if str(row["attempt_status"]) != "RAW_COMPLETE":
            raise ExecutionStateConflictError("attempt lacks complete raw acquisition proof")
        self._assert_durable_batch_context(
            connection,
            AttemptIdentity(
                attempt_id=request.attempt_id,
                request_instance_id=request.request_instance_id,
                attempt_number=int(
                    connection.execute(
                        "SELECT attempt_number FROM request_attempts WHERE attempt_id = ?",
                        (str(request.attempt_id),),
                    ).fetchone()[0]
                ),
            ),
            request.expectation,
        )
        return cast(sqlite3.Row, row)

    def _persist_canonical_catalog(
        self,
        connection: sqlite3.Connection,
        request: PublicationCommitRequest,
        *,
        policy_snapshot_id: str,
        manifest_sha256: str,
        manifest_bytes: int,
        verified_at: datetime,
    ) -> None:
        manifest = request.manifest
        published = request.published
        batch_values = (
            manifest.canonical_batch_id,
            manifest.batch_context_id,
            policy_snapshot_id,
            published.relative_directory,
            published.manifest_relative_path,
            "VERIFIED",
            manifest.row_count,
            _format_utc(manifest.manifest_created_at),
            _format_utc(verified_at),
            None,
        )
        batch_columns = (
            "canonical_batch_id",
            "batch_context_id",
            "policy_snapshot_id",
            "relative_path",
            "manifest_relative_path",
            "state",
            "row_count",
            "published_at",
            "verified_at",
            "invalidated_at",
        )
        existing = connection.execute(
            "SELECT * FROM canonical_batches WHERE canonical_batch_id = ?",
            (manifest.canonical_batch_id,),
        ).fetchone()
        if existing is None:
            connection.execute(
                f"INSERT INTO canonical_batches({', '.join(batch_columns)}) "
                f"VALUES ({', '.join('?' for _ in batch_columns)})",
                batch_values,
            )
        else:
            immutable_columns = (*batch_columns[:8], "invalidated_at")
            immutable_values = (*batch_values[:8], None)
            if _row_tuple(existing, immutable_columns) != immutable_values:
                raise ExecutionIdentityCollisionError(
                    "canonical batch ID collides with different catalog metadata"
                )
        manifest_values = (
            manifest.canonical_batch_id,
            manifest_sha256,
            manifest_bytes,
            manifest.schema_version,
            _format_utc(verified_at),
        )
        cataloged_manifest = connection.execute(
            "SELECT * FROM canonical_batch_manifests WHERE canonical_batch_id = ?",
            (manifest.canonical_batch_id,),
        ).fetchone()
        manifest_columns = (
            "canonical_batch_id",
            "manifest_content_sha256",
            "manifest_byte_count",
            "manifest_schema_version",
            "verified_at",
        )
        if cataloged_manifest is None:
            connection.execute(
                f"INSERT INTO canonical_batch_manifests({', '.join(manifest_columns)}) "
                f"VALUES ({', '.join('?' for _ in manifest_columns)})",
                manifest_values,
            )
        elif _row_tuple(cataloged_manifest, manifest_columns[:-1]) != manifest_values[:-1]:
            raise ExecutionIdentityCollisionError("canonical manifest catalog collision")

        directory = PurePosixPath(published.relative_directory)
        for ordinal, file in enumerate(manifest.files):
            file_values = (
                manifest.canonical_batch_id,
                ordinal,
                (directory / PurePosixPath(file.relative_path)).as_posix(),
                file.sha256,
                file.byte_count,
                file.row_count,
                _format_utc(file.timestamp_start_min),
                _format_utc(file.timestamp_end_max),
                file.schema_sha256,
            )
            columns = (
                "canonical_batch_id",
                "file_ordinal",
                "relative_path",
                "content_sha256",
                "byte_count",
                "row_count",
                "interval_start",
                "interval_end",
                "schema_fingerprint",
            )
            existing_file = connection.execute(
                """
                SELECT * FROM canonical_files
                WHERE canonical_batch_id = ? AND file_ordinal = ?
                """,
                (manifest.canonical_batch_id, ordinal),
            ).fetchone()
            if existing_file is None:
                connection.execute(
                    f"INSERT INTO canonical_files({', '.join(columns)}) "
                    f"VALUES ({', '.join('?' for _ in columns)})",
                    file_values,
                )
            elif _row_tuple(existing_file, columns) != file_values:
                raise ExecutionIdentityCollisionError(
                    "canonical file catalog differs for reused batch identity"
                )
        file_count = int(
            connection.execute(
                "SELECT count(*) FROM canonical_files WHERE canonical_batch_id = ?",
                (manifest.canonical_batch_id,),
            ).fetchone()[0]
        )
        if file_count != len(manifest.files):
            raise ExecutionIdentityCollisionError(
                "canonical batch reuse found a different file cardinality"
            )
        for outcome in manifest.streams:
            stream_values = (
                manifest.canonical_batch_id,
                outcome.stream_id,
                outcome.outcome.value,
                outcome.row_count,
                _format_utc(outcome.request_start),
                _format_utc(outcome.request_end),
                _canonical_json({"codes": list(outcome.validation_codes)}),
                outcome.semantic_duplicate_count,
                outcome.revision_count,
            )
            columns = (
                "canonical_batch_id",
                "stream_id",
                "outcome",
                "row_count",
                "interval_start",
                "interval_end",
                "validation_summary_json",
                "semantic_duplicate_count",
                "revision_count",
            )
            existing_stream = connection.execute(
                """
                SELECT * FROM canonical_batch_streams
                WHERE canonical_batch_id = ? AND stream_id = ?
                """,
                (manifest.canonical_batch_id, outcome.stream_id),
            ).fetchone()
            if existing_stream is None:
                connection.execute(
                    f"INSERT INTO canonical_batch_streams({', '.join(columns)}) "
                    f"VALUES ({', '.join('?' for _ in columns)})",
                    stream_values,
                )
            elif _row_tuple(existing_stream, columns) != stream_values:
                raise ExecutionIdentityCollisionError(
                    "canonical stream catalog differs for reused batch identity"
                )
        stream_count = int(
            connection.execute(
                "SELECT count(*) FROM canonical_batch_streams WHERE canonical_batch_id = ?",
                (manifest.canonical_batch_id,),
            ).fetchone()[0]
        )
        if stream_count != len(manifest.streams):
            raise ExecutionIdentityCollisionError(
                "canonical batch reuse found a different stream cardinality"
            )
        request_link = connection.execute(
            """
            SELECT * FROM canonical_batch_requests
            WHERE canonical_batch_id = ? AND request_instance_id = ?
            """,
            (manifest.canonical_batch_id, str(request.request_instance_id)),
        ).fetchone()
        link_values = (
            manifest.canonical_batch_id,
            str(request.request_instance_id),
            policy_snapshot_id,
            _format_utc(verified_at),
        )
        link_columns = (
            "canonical_batch_id",
            "request_instance_id",
            "policy_snapshot_id",
            "linked_at",
        )
        if request_link is None:
            connection.execute(
                f"INSERT INTO canonical_batch_requests({', '.join(link_columns)}) "
                f"VALUES ({', '.join('?' for _ in link_columns)})",
                link_values,
            )
        elif _row_tuple(request_link, link_columns[:-1]) != link_values[:-1]:
            raise ExecutionIdentityCollisionError(
                "canonical batch request link collides with another policy"
            )

    def _persist_coverage(
        self,
        connection: sqlite3.Connection,
        request: PublicationCommitRequest,
        *,
        policy_snapshot_id: str,
        authorization_hash: str,
    ) -> None:
        manifest = request.manifest
        outcomes = {value.stream_id: value for value in manifest.streams}
        publishable_streams = {
            stream_id
            for stream_id, outcome in outcomes.items()
            if outcome.outcome is StreamPublicationOutcome.PUBLISHABLE
        }
        segment_streams = {segment.stream_id for segment in request.coverage.segments}
        if segment_streams != publishable_streams:
            raise ExecutionIntegrityError(
                "coverage must account for every and only publishable batch stream"
            )
        policy = connection.execute(
            "SELECT * FROM policy_snapshots WHERE policy_snapshot_id = ?",
            (policy_snapshot_id,),
        ).fetchone()
        if policy is None:
            raise ExecutionIntegrityError("coverage policy snapshot is absent")
        acquisition_count = int(
            connection.execute(
                """
                SELECT page_count FROM attempt_acquisition_records WHERE attempt_id = ?
                """,
                (str(request.attempt_id),),
            ).fetchone()[0]
        )
        for stream_id in sorted(segment_streams):
            stream_segments = sorted(
                (
                    segment
                    for segment in request.coverage.segments
                    if segment.stream_id == stream_id
                ),
                key=lambda segment: (segment.start, segment.end, segment.coverage_id),
            )
            if any(left.end > right.start for left, right in pairwise(stream_segments)):
                raise ExecutionIntegrityError(
                    "one publication cannot persist overlapping coverage for a stream"
                )
            stream_outcome = outcomes[stream_id]
            if sum(segment.row_count for segment in stream_segments) != stream_outcome.row_count:
                raise ExecutionIntegrityError(
                    "coverage row proof differs from the canonical stream row count"
                )
            origins = {segment.coverage_start for segment in stream_segments}
            if len(origins) != 1:
                raise ExecutionIntegrityError(
                    "one stream publication cannot introduce multiple coverage origins"
                )
            durable = connection.execute(
                """
                SELECT min(coverage_start) AS first_origin,
                       max(coverage_start) AS last_origin,
                       max(generation) AS latest_generation
                FROM coverage_segments WHERE stream_id = ?
                """,
                (stream_id,),
            ).fetchone()
            if durable is None:
                raise ExecutionIntegrityError("durable coverage projection is unavailable")
            first_origin = durable["first_origin"]
            last_origin = durable["last_origin"]
            if first_origin is not None and (
                str(first_origin) != str(last_origin)
                or _parse_utc(str(first_origin)) != next(iter(origins))
            ):
                raise ExecutionIntegrityError(
                    "coverage_start is authoritative and cannot be rebased"
                )
            expected_generation = (
                1 if durable["latest_generation"] is None else int(durable["latest_generation"]) + 1
            )
            for segment in stream_segments:
                durable_segment = connection.execute(
                    "SELECT generation FROM coverage_segments WHERE coverage_id = ?",
                    (segment.coverage_id,),
                ).fetchone()
                required_generation = (
                    expected_generation
                    if durable_segment is None
                    else int(durable_segment["generation"])
                )
                if segment.generation != required_generation:
                    raise ExecutionIntegrityError(
                        "coverage generation must advance once, or replay its exact fact"
                    )
        for segment in request.coverage.segments:
            outcome = outcomes.get(segment.stream_id)
            if segment.canonical_batch_id != manifest.canonical_batch_id or outcome is None:
                raise ExecutionIntegrityError(
                    "coverage must be produced by a stream in the committed batch"
                )
            if (
                segment.policy_snapshot_id != policy_snapshot_id
                or segment.policy_id != str(policy["policy_id"])
                or segment.policy_revision != int(policy["revision"])
                or segment.policy_hash != str(policy["policy_hash"])
                or segment.calendar_snapshot_id != request.coverage.calendar_snapshot_id
                or segment.calendar_snapshot_checksum != manifest.calendar_snapshot.checksum
                or segment.verification_state is not CoverageVerificationState.VERIFIED
                or not segment.retained
                or segment.invalidated_at is not None
                or segment.artifact_count != acquisition_count
                or not segment.artifacts_present
                or not segment.artifact_integrity_verified
                or not segment.interval_verified
                or not segment.request_completed
                or not segment.pagination_verified
                or not segment.terminal_page_verified
                or not segment.canonical_batch_verified
                or segment.canonical_file_count != len(manifest.files)
                or segment.raw_artifact_count != acquisition_count
                or not segment.relational_provenance_verified
                or segment.request_terminal_state.value != request.terminal_status.value
                or segment.stream_outcome.value != "PUBLISHABLE"
            ):
                raise ExecutionIntegrityError(
                    "coverage lacks exact calendar, policy, artifact, or request proof"
                )
            claimed_slots = tuple(
                slot
                for slot in manifest.eligible_slots
                if slot.start_utc >= segment.start and slot.end_utc <= segment.end
            )
            if (
                not claimed_slots
                or claimed_slots[0].start_utc != segment.start
                or claimed_slots[-1].end_utc != segment.end
                or (
                    segment.classification is CoverageClassification.OBSERVED
                    and len(claimed_slots) != segment.row_count
                )
            ):
                raise ExecutionIntegrityError(
                    "coverage interval/row proof is not an exact eligible-slot run"
                )
            if (
                segment.verified_at < manifest.manifest_created_at
                or segment.verified_at > self._store._now()
            ):
                raise ExecutionIntegrityError(
                    "coverage verification time is outside the durable publication timeline"
                )
            if segment.start < outcome.request_start or segment.end > outcome.request_end:
                raise ExecutionIntegrityError("coverage exceeds the bounded stream request")
            if segment.classification is CoverageClassification.OBSERVED:
                if outcome.outcome is not StreamPublicationOutcome.PUBLISHABLE:
                    raise ExecutionIntegrityError(
                        "observed coverage requires a publishable canonical stream"
                    )
            else:
                approved = (
                    manifest.provider,
                    manifest.dataset,
                    segment.provider_semantics_version,
                ) == (
                    "synthetic",
                    "price_bars",
                    "synthetic-complete-pagination-v1",
                )
                if not approved:
                    raise ExecutionIntegrityError(
                        "VERIFIED_EMPTY is not approved for this exact provider dataset"
                    )
                if outcome.row_count != 0:
                    raise ExecutionIntegrityError(
                        "VERIFIED_EMPTY cannot be attached to a stream containing rows"
                    )
            coverage_start = segment.coverage_start
            if coverage_start > segment.start:
                raise ExecutionIntegrityError("coverage start cannot follow segment start")
            values = (
                segment.coverage_id,
                segment.stream_id,
                manifest.canonical_batch_id,
                request.coverage.calendar_snapshot_id,
                policy_snapshot_id,
                _format_utc(coverage_start),
                _format_utc(segment.start),
                _format_utc(segment.end),
                segment.classification.value,
                segment.verification_state.value,
                int(segment.retained),
                segment.row_count,
                segment.artifact_count,
                int(segment.request_completed),
                int(segment.pagination_verified),
                segment.provider_semantics_version,
                segment.generation,
                _format_utc(segment.verified_at),
                _format_utc(segment.invalidated_at) if segment.invalidated_at is not None else None,
            )
            columns = (
                "coverage_id",
                "stream_id",
                "canonical_batch_id",
                "calendar_snapshot_id",
                "policy_snapshot_id",
                "coverage_start",
                "interval_start",
                "interval_end",
                "classification",
                "verification_state",
                "retained",
                "row_count",
                "artifact_count",
                "request_completed",
                "pagination_verified",
                "provider_semantics_version",
                "generation",
                "verified_at",
                "invalidated_at",
            )
            existing = connection.execute(
                "SELECT * FROM coverage_segments WHERE coverage_id = ?",
                (segment.coverage_id,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    f"INSERT INTO coverage_segments({', '.join(columns)}) "
                    f"VALUES ({', '.join('?' for _ in columns)})",
                    values,
                )
            elif _row_tuple(existing, columns) != values:
                raise ExecutionIdentityCollisionError(
                    "coverage ID collides with different verified facts"
                )
            proof_hash = _hash_json(
                {
                    "coverage_id": segment.coverage_id,
                    "request_instance_id": str(request.request_instance_id),
                    "attempt_id": str(request.attempt_id),
                    "authorization_hash": authorization_hash,
                    "request_terminal_state": segment.request_terminal_state.value,
                    "stream_outcome": segment.stream_outcome.value,
                    "terminal_page_verified": segment.terminal_page_verified,
                    "canonical_batch_verified": segment.canonical_batch_verified,
                    "canonical_file_count": segment.canonical_file_count,
                    "raw_artifact_count": segment.raw_artifact_count,
                    "relational_provenance_verified": (segment.relational_provenance_verified),
                    "provider_semantics_version": segment.provider_semantics_version,
                }
            )
            proof_values = (
                segment.coverage_id,
                str(request.request_instance_id),
                str(request.attempt_id),
                authorization_hash,
                segment.request_terminal_state.value,
                segment.stream_outcome.value,
                int(segment.terminal_page_verified),
                int(segment.canonical_batch_verified),
                segment.canonical_file_count,
                segment.raw_artifact_count,
                int(segment.relational_provenance_verified),
                segment.provider_semantics_version,
                proof_hash,
            )
            proof = connection.execute(
                "SELECT * FROM coverage_request_proofs WHERE coverage_id = ?",
                (segment.coverage_id,),
            ).fetchone()
            proof_columns = (
                "coverage_id",
                "request_instance_id",
                "attempt_id",
                "authorization_hash",
                "request_terminal_state",
                "stream_outcome",
                "terminal_page_verified",
                "canonical_batch_verified",
                "canonical_file_count",
                "raw_artifact_count",
                "relational_provenance_verified",
                "provider_semantics_version",
                "proof_hash",
            )
            if proof is None:
                if existing is not None:
                    raise ExecutionIntegrityError(
                        "reused coverage segment is missing its original request proof"
                    )
                connection.execute(
                    f"INSERT INTO coverage_request_proofs({', '.join(proof_columns)}) "
                    f"VALUES ({', '.join('?' for _ in proof_columns)})",
                    proof_values,
                )
            elif existing is None:
                raise ExecutionIdentityCollisionError(
                    "new coverage identity already has an unrelated request proof"
                )
            else:
                reusable_proof_columns = proof_columns[4:12]
                reusable_proof_values = proof_values[4:12]
                if _row_tuple(proof, reusable_proof_columns) != reusable_proof_values:
                    raise ExecutionIdentityCollisionError(
                        "reused coverage fact has different immutable proof semantics"
                    )

    def _persist_gaps(
        self,
        connection: sqlite3.Connection,
        request: PublicationCommitRequest,
    ) -> None:
        outcomes = {outcome.stream_id: outcome for outcome in request.manifest.streams}
        current_request_id = str(request.request_instance_id)
        for gap in request.coverage.gaps:
            outcome = outcomes.get(gap.stream_id)
            if outcome is None:
                raise ExecutionIntegrityError(
                    "publication gap belongs to a stream outside the bounded request"
                )
            existing = connection.execute(
                "SELECT * FROM gaps WHERE gap_id = ?",
                (gap.gap_id,),
            ).fetchone()
            if existing is None:
                if gap.request_instance_id not in {None, current_request_id}:
                    raise ExecutionIntegrityError("new gap points at a different request instance")
                if gap.start < outcome.request_start or gap.end > outcome.request_end:
                    raise ExecutionIntegrityError(
                        "new gap exceeds the current bounded stream request"
                    )
                if gap.status.value in {"RESOLVED", "INVALIDATED"}:
                    raise ExecutionIntegrityError(
                        "publication cannot create a previously unknown terminal gap"
                    )
                if gap.canonical_batch_id is not None:
                    raise ExecutionIntegrityError(
                        "new active gap cannot claim a canonical resolution batch"
                    )
            elif gap.status.value in {"RESOLVED", "INVALIDATED"}:
                if gap.status.value != "RESOLVED":
                    raise ExecutionIntegrityError(
                        "publication commit may resolve, but not invalidate, an existing gap"
                    )
                if outcome.outcome is not StreamPublicationOutcome.PUBLISHABLE or not any(
                    segment.stream_id == gap.stream_id
                    and segment.classification is CoverageClassification.OBSERVED
                    and segment.start < gap.end
                    and segment.end > gap.start
                    for segment in request.coverage.segments
                ):
                    raise ExecutionIntegrityError(
                        "gap resolution lacks intersecting observed coverage from this batch"
                    )

            request_instance_id = gap.request_instance_id or current_request_id
            canonical_batch_id = gap.canonical_batch_id
            if canonical_batch_id is None and gap.status.value in {"RESOLVED", "INVALIDATED"}:
                canonical_batch_id = request.manifest.canonical_batch_id
            if (
                gap.status.value == "RESOLVED"
                and canonical_batch_id != request.manifest.canonical_batch_id
            ):
                raise ExecutionIntegrityError(
                    "gap resolution must point at the current verified canonical batch"
                )
            values = (
                gap.gap_id,
                gap.stream_id,
                _format_utc(gap.start),
                _format_utc(gap.end),
                gap.gap_type.value,
                gap.status.value,
                int(gap.blocking),
                _format_utc(gap.detected_at),
                _format_utc(gap.resolved_at) if gap.resolved_at is not None else None,
                request_instance_id,
                canonical_batch_id,
            )
            columns = (
                "gap_id",
                "stream_id",
                "interval_start",
                "interval_end",
                "gap_type",
                "status",
                "blocking",
                "detected_at",
                "resolved_at",
                "request_instance_id",
                "canonical_batch_id",
            )
            if existing is None:
                connection.execute(
                    f"INSERT INTO gaps({', '.join(columns)}) "
                    f"VALUES ({', '.join('?' for _ in columns)})",
                    values,
                )
                continue
            immutable = (
                "gap_id",
                "stream_id",
                "interval_start",
                "interval_end",
                "gap_type",
                "blocking",
                "detected_at",
            )
            if _row_tuple(existing, immutable) != tuple(
                values[columns.index(column)] for column in immutable
            ):
                raise ExecutionIdentityCollisionError(
                    "gap ID collides with different immutable finding metadata"
                )
            current_status = str(existing["status"])
            target_status = gap.status.value
            allowed = {
                "OPEN": {"OPEN", "REPAIRING", "RESOLVED", "INVALIDATED"},
                "REPAIRING": {"REPAIRING", "OPEN", "RESOLVED", "INVALIDATED"},
                "RESOLVED": {"RESOLVED"},
                "INVALIDATED": {"INVALIDATED"},
            }
            if target_status not in allowed[current_status]:
                raise ExecutionStateConflictError("invalid durable gap lifecycle transition")
            existing_request = (
                str(existing["request_instance_id"])
                if existing["request_instance_id"] is not None
                else None
            )
            existing_batch = (
                str(existing["canonical_batch_id"])
                if existing["canonical_batch_id"] is not None
                else None
            )
            if (
                gap.request_instance_id is not None
                and existing_request is not None
                and gap.request_instance_id != existing_request
            ):
                raise ExecutionIdentityCollisionError(
                    "gap transition points at a different originating request"
                )
            if existing_request is not None:
                request_instance_id = existing_request
            if existing_batch not in {None, canonical_batch_id}:
                raise ExecutionIdentityCollisionError(
                    "gap resolution points at a different canonical batch"
                )
            if current_status in {"RESOLVED", "INVALIDATED"}:
                expected_resolved = (
                    _format_utc(gap.resolved_at) if gap.resolved_at is not None else None
                )
                if str(existing["resolved_at"]) != expected_resolved or (
                    existing_batch is not None and existing_batch != canonical_batch_id
                ):
                    raise ExecutionIdentityCollisionError(
                        "terminal gap replay differs from its fixed resolution proof"
                    )
                continue
            connection.execute(
                """
                UPDATE gaps SET status = ?, resolved_at = ?,
                    request_instance_id = COALESCE(request_instance_id, ?),
                    canonical_batch_id = COALESCE(canonical_batch_id, ?)
                WHERE gap_id = ?
                """,
                (
                    target_status,
                    _format_utc(gap.resolved_at) if gap.resolved_at is not None else None,
                    request_instance_id,
                    canonical_batch_id,
                    gap.gap_id,
                ),
            )

    def _invalidate_watermarks_blocked_by_gaps(
        self,
        connection: sqlite3.Connection,
        request: PublicationCommitRequest,
        *,
        invalidated_at: datetime,
    ) -> None:
        """Invalidate any prior frontier crossed by a newly durable active gap."""

        incoming_streams = {watermark.stream_id for watermark in request.coverage.watermarks}
        changed_streams = sorted({gap.stream_id for gap in request.coverage.gaps})
        run_id = str(
            connection.execute(
                "SELECT run_id FROM request_instances WHERE request_instance_id = ?",
                (str(request.request_instance_id),),
            ).fetchone()[0]
        )
        for stream_id in changed_streams:
            watermark = connection.execute(
                "SELECT * FROM watermarks WHERE stream_id = ?",
                (stream_id,),
            ).fetchone()
            if watermark is None or str(watermark["verification_state"]) != "VERIFIED":
                continue
            blocking = connection.execute(
                """
                SELECT 1 FROM gaps
                WHERE stream_id = ? AND blocking = 1
                  AND status IN ('OPEN', 'REPAIRING')
                  AND interval_start < ? AND interval_end > ?
                LIMIT 1
                """,
                (
                    stream_id,
                    str(watermark["exclusive_frontier"]),
                    str(watermark["coverage_start"]),
                ),
            ).fetchone()
            if blocking is None:
                continue
            if stream_id in incoming_streams:
                raise ExecutionIntegrityError(
                    "one commit cannot verify a watermark across an active blocking gap"
                )
            connection.execute(
                """
                UPDATE watermarks
                SET verification_state = 'INVALID', generation = generation + 1,
                    last_run_id = ?, last_batch_id = ?, invalidated_at = ?
                WHERE stream_id = ? AND verification_state = 'VERIFIED'
                """,
                (
                    run_id,
                    request.manifest.canonical_batch_id,
                    _format_utc(invalidated_at),
                    stream_id,
                ),
            )

    def _persist_watermarks(
        self,
        connection: sqlite3.Connection,
        request: PublicationCommitRequest,
        *,
        policy_snapshot_id: str,
    ) -> None:
        for watermark in request.coverage.watermarks:
            outcome = next(
                (
                    candidate
                    for candidate in request.manifest.streams
                    if candidate.stream_id == watermark.stream_id
                ),
                None,
            )
            if outcome is None or outcome.outcome is not StreamPublicationOutcome.PUBLISHABLE:
                raise ExecutionIntegrityError(
                    "watermark belongs to a non-publishable or unrequested stream"
                )
            if (
                watermark.verification_state is not CoverageVerificationState.VERIFIED
                or watermark.invalidated_at is not None
            ):
                raise ExecutionIntegrityError(
                    "publication commit can advance only a currently VERIFIED watermark"
                )
            if watermark.computed_at > self._store._now():
                raise ExecutionIntegrityError("watermark computation time cannot be in the future")
            stream_segments = tuple(
                segment
                for segment in request.coverage.segments
                if segment.stream_id == watermark.stream_id
            )
            if not stream_segments or watermark.computed_at < max(
                segment.verified_at for segment in stream_segments
            ):
                raise ExecutionIntegrityError(
                    "watermark predates the changing batch coverage proof"
                )
            origins = connection.execute(
                """
                SELECT DISTINCT coverage_start FROM coverage_segments
                WHERE stream_id = ?
                """,
                (watermark.stream_id,),
            ).fetchall()
            if len(origins) != 1 or _parse_utc(str(origins[0][0])) != watermark.coverage_start:
                raise ExecutionIntegrityError(
                    "watermark rebases or lacks the authoritative coverage origin"
                )
            if (
                watermark.last_run_id
                != str(
                    connection.execute(
                        """
                        SELECT run_id FROM request_instances WHERE request_instance_id = ?
                        """,
                        (str(request.request_instance_id),),
                    ).fetchone()[0]
                )
                or watermark.last_batch_id != request.manifest.canonical_batch_id
                or watermark.policy_snapshot_id != policy_snapshot_id
                or watermark.calendar_snapshot_id != request.coverage.calendar_snapshot_id
            ):
                raise ExecutionIntegrityError(
                    "watermark provenance differs from the current publication transaction"
                )
            blocking = connection.execute(
                """
                SELECT 1 FROM gaps
                WHERE stream_id = ? AND blocking = 1
                  AND status IN ('OPEN', 'REPAIRING')
                  AND interval_start < ? AND interval_end > ?
                LIMIT 1
                """,
                (
                    watermark.stream_id,
                    _format_utc(watermark.exclusive_frontier),
                    _format_utc(watermark.coverage_start),
                ),
            ).fetchone()
            if blocking is not None:
                raise ExecutionIntegrityError("watermark frontier crosses an active blocking gap")
            supporting = connection.execute(
                """
                SELECT 1 FROM coverage_segments
                WHERE stream_id = ? AND canonical_batch_id = ?
                  AND verification_state = 'VERIFIED' AND retained = 1
                  AND interval_start < ? AND interval_end > ?
                LIMIT 1
                """,
                (
                    watermark.stream_id,
                    request.manifest.canonical_batch_id,
                    _format_utc(watermark.exclusive_frontier),
                    _format_utc(watermark.coverage_start),
                ),
            ).fetchone()
            if supporting is None:
                raise ExecutionIntegrityError(
                    "watermark lacks retained verified coverage from the changing batch"
                )
            values = (
                watermark.stream_id,
                _format_utc(watermark.coverage_start),
                _format_utc(watermark.exclusive_frontier),
                watermark.verification_state.value,
                watermark.generation,
                watermark.calendar_snapshot_id,
                watermark.policy_snapshot_id,
                watermark.last_run_id,
                watermark.last_batch_id,
                watermark.last_verified_session.isoformat(),
                watermark.blocking_gap_count,
                _format_utc(watermark.computed_at),
                None,
            )
            columns = (
                "stream_id",
                "coverage_start",
                "exclusive_frontier",
                "verification_state",
                "generation",
                "calendar_snapshot_id",
                "policy_snapshot_id",
                "last_run_id",
                "last_batch_id",
                "last_verified_session",
                "blocking_gap_count",
                "computed_at",
                "invalidated_at",
            )
            existing = connection.execute(
                "SELECT * FROM watermarks WHERE stream_id = ?",
                (watermark.stream_id,),
            ).fetchone()
            if existing is None:
                if watermark.generation != 1:
                    raise ExecutionStateConflictError("first watermark generation must be one")
                connection.execute(
                    f"INSERT INTO watermarks({', '.join(columns)}) "
                    f"VALUES ({', '.join('?' for _ in columns)})",
                    values,
                )
                continue
            if watermark.generation != int(existing["generation"]) + 1:
                raise ExecutionStateConflictError(
                    "watermark generation must advance exactly once per changing commit"
                )
            if _parse_utc(str(existing["exclusive_frontier"])) > watermark.exclusive_frontier:
                raise ExecutionStateConflictError(
                    "verified publication cannot move a watermark frontier backward"
                )
            connection.execute(
                """
                UPDATE watermarks SET coverage_start = ?, exclusive_frontier = ?,
                    verification_state = ?, generation = ?, calendar_snapshot_id = ?,
                    policy_snapshot_id = ?, last_run_id = ?, last_batch_id = ?,
                    last_verified_session = ?, blocking_gap_count = ?, computed_at = ?,
                    invalidated_at = ? WHERE stream_id = ?
                """,
                (*values[1:], watermark.stream_id),
            )

    @staticmethod
    def _reconcile_run(
        connection: sqlite3.Connection,
        run_id: str,
        *,
        completed_at: datetime,
    ) -> tuple[str, IngestionRunStatus]:
        run = connection.execute(
            "SELECT * FROM ingestion_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if run is None:
            raise ExecutionStateConflictError("run is not cataloged")
        counts = connection.execute(
            """
            SELECT count(*) AS total,
                   sum(CASE WHEN status IN ('SUCCESS', 'PARTIAL') THEN 1 ELSE 0 END) AS succeeded,
                   sum(CASE WHEN status IN ('FAILED', 'BLOCKED', 'CANCELLED') THEN 1 ELSE 0 END)
                       AS failed,
                   sum(CASE WHEN status IN (
                       'SUCCESS', 'PARTIAL', 'FAILED', 'BLOCKED', 'CANCELLED'
                   ) THEN 1 ELSE 0 END) AS terminal,
                   sum(CASE WHEN status = 'PARTIAL' THEN 1 ELSE 0 END) AS partial
            FROM request_instances WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if counts is None:
            raise ExecutionStateConflictError("run request summary is unavailable")
        total = int(counts["total"] or 0)
        succeeded = int(counts["succeeded"] or 0)
        failed = int(counts["failed"] or 0)
        terminal = int(counts["terminal"] or 0)
        partial = int(counts["partial"] or 0)
        planned = int(run["planned_request_count"])
        if total != planned:
            raise ExecutionIntegrityError("run request count differs from its durable plan")
        if terminal < total:
            status = IngestionRunStatus.RUNNING
            if str(run["status"]) not in {
                IngestionRunStatus.RUNNING.value,
                IngestionRunStatus.PLANNED.value,
            }:
                raise ExecutionStateConflictError("terminal run has non-terminal request instances")
            connection.execute(
                """
                UPDATE ingestion_runs SET succeeded_request_count = ?,
                    failed_request_count = ? WHERE run_id = ?
                """,
                (succeeded, failed, run_id),
            )
            return run_id, status
        if failed == total:
            target = IngestionRunStatus.FAILED
        elif failed or partial:
            target = IngestionRunStatus.PARTIAL
        else:
            target = IngestionRunStatus.SUCCESS
        current = IngestionRunStatus(str(run["status"]))
        if (
            current
            in {
                IngestionRunStatus.SUCCESS,
                IngestionRunStatus.PARTIAL,
                IngestionRunStatus.FAILED,
                IngestionRunStatus.CANCELLED,
            }
            and current is not target
        ):
            raise ExecutionIdentityCollisionError(
                "run summary conflicts with terminal request facts"
            )
        if current is target and run["completed_at"] is not None:
            if (
                int(run["succeeded_request_count"]) != succeeded
                or int(run["failed_request_count"]) != failed
            ):
                raise ExecutionIdentityCollisionError(
                    "terminal run counters conflict with terminal request facts"
                )
            return run_id, target
        connection.execute(
            """
            UPDATE ingestion_runs SET status = ?, completed_at = ?,
                succeeded_request_count = ?, failed_request_count = ?
            WHERE run_id = ?
            """,
            (target.value, _format_utc(completed_at), succeeded, failed, run_id),
        )
        return run_id, target

    @staticmethod
    def _validate_commit_replay(
        row: sqlite3.Row,
        request: PublicationCommitRequest,
        *,
        coverage_hash: str,
    ) -> None:
        actual = (
            str(row["attempt_id"]),
            str(row["coverage_commit_hash"]),
            str(row["commit_source"]),
            str(row["terminal_status"]),
            str(row["request_status"]),
        )
        expected = (
            str(request.attempt_id),
            coverage_hash,
            request.source.value,
            request.terminal_status.value,
            request.terminal_status.value,
        )
        if actual != expected:
            raise ExecutionIdentityCollisionError(
                "publication replay differs from the committed idempotent effects"
            )

    def _require_request_scope(
        self,
        connection: sqlite3.Connection,
        request_instance_id: UUID,
        authorization: RequestPolicyAuthorization,
        *,
        policy_snapshot_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT instance.run_id, instance.status AS request_status,
                   instance.request_spec_id, spec.request_spec_hash,
                   spec.provider, spec.dataset, spec.interval_start, spec.interval_end,
                   run.status AS run_status, run.environment, run.policy_snapshot_id,
                   policy.policy_id, policy.revision, policy.policy_hash,
                   policy.retention_mode, policy.verified_at,
                   provenance.policy_status, plan.catalog_snapshot_id,
                   estimate.authorized_at AS request_authorized_at,
                   estimate.authorization_eligible_before,
                   catalog.catalog_id, catalog.catalog_revision, catalog.catalog_hash,
                   active.status AS active_status,
                   active.policy_snapshot_id AS active_policy_snapshot_id,
                   active.retention_mode AS active_retention_mode,
                   active.unavailable_at
            FROM request_instances AS instance
            JOIN request_specs AS spec ON spec.request_spec_id = instance.request_spec_id
            JOIN ingestion_runs AS run ON run.run_id = instance.run_id
            JOIN ingestion_plan_records AS plan ON plan.run_id = run.run_id
            JOIN request_plan_estimates AS estimate
              ON estimate.request_instance_id = instance.request_instance_id
            JOIN policy_snapshots AS policy
              ON policy.policy_snapshot_id = run.policy_snapshot_id
            JOIN policy_snapshot_provenance AS provenance
              ON provenance.policy_snapshot_id = policy.policy_snapshot_id
            JOIN policy_catalog_snapshots AS catalog
              ON catalog.catalog_snapshot_id = plan.catalog_snapshot_id
            LEFT JOIN dataset_policy_status AS active
              ON active.provider = policy.provider AND active.dataset = policy.dataset
            WHERE instance.request_instance_id = ?
            """,
            (str(request_instance_id),),
        ).fetchone()
        if row is None:
            raise ExecutionStateConflictError("request instance is not cataloged")
        snapshot = authorization.policy_snapshot
        expected = (
            authorization.request_spec_hash,
            snapshot.provider,
            snapshot.dataset,
            _format_utc(authorization.request_start),
            _format_utc(authorization.request_end),
            authorization.environment.value,
            policy_snapshot_id,
            snapshot.policy_id,
            snapshot.policy_revision,
            snapshot.policy_hash,
            snapshot.mode.value,
            snapshot.verified_on.isoformat(),
            _format_utc(authorization.authorized_at),
            _format_utc(authorization.eligible_before),
            snapshot.status.value,
            snapshot.catalog_id,
            snapshot.catalog_revision,
            snapshot.catalog_hash,
        )
        actual = (
            str(row["request_spec_hash"]),
            str(row["provider"]),
            str(row["dataset"]),
            str(row["interval_start"]),
            str(row["interval_end"]),
            str(row["environment"]),
            str(row["policy_snapshot_id"]),
            str(row["policy_id"]),
            int(row["revision"]),
            str(row["policy_hash"]),
            str(row["retention_mode"]),
            str(row["verified_at"]),
            str(row["request_authorized_at"]),
            str(row["authorization_eligible_before"]),
            str(row["policy_status"]),
            str(row["catalog_id"]),
            int(row["catalog_revision"]),
            str(row["catalog_hash"]),
        )
        if actual != expected:
            raise ExecutionIdentityCollisionError(
                "request authorization differs from durable plan/policy scope"
            )
        if (
            str(row["active_status"]) != DatasetPolicyStatus.ACTIVE.value
            or str(row["active_policy_snapshot_id"]) != policy_snapshot_id
            or str(row["active_retention_mode"]) != snapshot.mode.value
            or row["unavailable_at"] is not None
        ):
            raise ExecutionIntegrityError("exact dataset policy is no longer active")
        return cast(sqlite3.Row, row)

    def _require_running_attempt(
        self,
        connection: sqlite3.Connection,
        identity: AttemptIdentity,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT attempt.*, auth.authorization_json, auth.authorization_hash,
                   auth.request_spec_id, auth.policy_snapshot_id,
                   instance.status AS request_status
            FROM request_attempts AS attempt
            JOIN attempt_request_authorizations AS auth ON auth.attempt_id = attempt.attempt_id
            JOIN request_instances AS instance
              ON instance.request_instance_id = attempt.request_instance_id
            WHERE attempt.attempt_id = ?
            """,
            (str(identity.attempt_id),),
        ).fetchone()
        if row is None:
            raise ExecutionStateConflictError("attempt is not durable")
        if (
            str(row["request_instance_id"]) != str(identity.request_instance_id)
            or int(row["attempt_number"]) != identity.attempt_number
        ):
            raise ExecutionIdentityCollisionError("attempt identity does not match durable state")
        if str(row["status"]) != "RUNNING" or str(row["request_status"]) != "ACQUIRING":
            raise ExecutionStateConflictError("raw pages require an actively acquiring attempt")
        return cast(sqlite3.Row, row)

    def _require_attempt_authorization(
        self,
        connection: sqlite3.Connection,
        identity: AttemptIdentity,
        *,
        request_authorization_hash: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT attempt.*, auth.authorization_json, auth.authorization_hash,
                   auth.request_spec_id, auth.policy_snapshot_id,
                   instance.status AS request_status
            FROM request_attempts AS attempt
            JOIN attempt_request_authorizations AS auth ON auth.attempt_id = attempt.attempt_id
            JOIN request_instances AS instance
              ON instance.request_instance_id = attempt.request_instance_id
            WHERE attempt.attempt_id = ?
            """,
            (str(identity.attempt_id),),
        ).fetchone()
        if row is None:
            raise ExecutionStateConflictError("attempt authorization is not durable")
        if (
            str(row["request_instance_id"]) != str(identity.request_instance_id)
            or int(row["attempt_number"]) != identity.attempt_number
            or str(row["authorization_hash"]) != request_authorization_hash
        ):
            raise ExecutionIdentityCollisionError(
                "attempt/request authorization identity differs from durable state"
            )
        return cast(sqlite3.Row, row)

    def _require_completed_acquisition(
        self,
        connection: sqlite3.Connection,
        identity: AttemptIdentity,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT acquisition.*, attempt.status AS attempt_status,
                   attempt.attempt_number, instance.status AS request_status
            FROM attempt_acquisition_records AS acquisition
            JOIN request_attempts AS attempt ON attempt.attempt_id = acquisition.attempt_id
            JOIN request_instances AS instance
              ON instance.request_instance_id = acquisition.request_instance_id
            WHERE acquisition.attempt_id = ?
            """,
            (str(identity.attempt_id),),
        ).fetchone()
        if row is None:
            raise ExecutionStateConflictError("complete acquisition proof is absent")
        if (
            str(row["request_instance_id"]) != str(identity.request_instance_id)
            or int(row["attempt_number"]) != identity.attempt_number
        ):
            raise ExecutionIdentityCollisionError(
                "acquisition proof belongs to a different attempt identity"
            )
        return cast(sqlite3.Row, row)

    def _persist_raw_artifact_row(
        self,
        connection: sqlite3.Connection,
        identity: RawArtifactIdentity,
        published: PublishedRawArtifact,
        *,
        manifest_sha256: str,
        manifest_bytes: int,
        verified_at: datetime,
    ) -> bool:
        values = (
            identity.artifact_id,
            f"request_spec_v1_{identity.request_spec_hash}",
            identity.page_ordinal,
            identity.page_relation_hash,
            identity.content_sha256,
            identity.byte_count,
            identity.media_type,
            identity.content_encoding,
            published.payload_relative_path,
            published.manifest_relative_path,
            _format_utc(published.first_persisted_at),
            _format_utc(verified_at),
            "VERIFIED",
        )
        columns = (
            "artifact_id",
            "request_spec_id",
            "page_ordinal",
            "page_relation_hash",
            "content_sha256",
            "byte_count",
            "media_type",
            "content_encoding",
            "relative_path",
            "manifest_relative_path",
            "first_persisted_at",
            "verified_at",
            "state",
        )
        row = connection.execute(
            "SELECT * FROM raw_artifacts WHERE artifact_id = ?",
            (identity.artifact_id,),
        ).fetchone()
        replayed = row is not None
        if row is None:
            connection.execute(
                f"INSERT INTO raw_artifacts({', '.join(columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)})",
                values,
            )
        elif _row_tuple(row, (*columns[:11], "state")) != (*values[:11], "VERIFIED"):
            raise ExecutionIdentityCollisionError(
                "raw artifact ID collides with different immutable catalog metadata"
            )
        manifest_values = (
            identity.artifact_id,
            manifest_sha256,
            manifest_bytes,
            1,
            _format_utc(verified_at),
        )
        manifest_columns = (
            "artifact_id",
            "manifest_content_sha256",
            "manifest_byte_count",
            "manifest_schema_version",
            "verified_at",
        )
        manifest = connection.execute(
            "SELECT * FROM raw_artifact_manifests WHERE artifact_id = ?",
            (identity.artifact_id,),
        ).fetchone()
        if manifest is None:
            connection.execute(
                f"INSERT INTO raw_artifact_manifests({', '.join(manifest_columns)}) "
                f"VALUES ({', '.join('?' for _ in manifest_columns)})",
                manifest_values,
            )
        elif _row_tuple(manifest, manifest_columns[:-1]) != manifest_values[:-1]:
            raise ExecutionIdentityCollisionError("raw manifest catalog collision")
        return replayed

    @staticmethod
    def _assert_raw_row_matches_identity(
        row: sqlite3.Row,
        identity: RawArtifactIdentity,
    ) -> None:
        actual = (
            str(row["artifact_id"]),
            str(row["request_spec_id"]),
            int(row["page_ordinal"]),
            str(row["page_relation_hash"]),
            str(row["content_sha256"]),
            int(row["byte_count"]),
            str(row["media_type"]),
            str(row["content_encoding"]),
        )
        expected = (
            identity.artifact_id,
            f"request_spec_v1_{identity.request_spec_hash}",
            identity.page_ordinal,
            identity.page_relation_hash,
            identity.content_sha256,
            identity.byte_count,
            identity.media_type,
            identity.content_encoding,
        )
        if actual != expected:
            raise ExecutionIdentityCollisionError(
                "raw catalog row differs from authorized artifact identity"
            )

    @staticmethod
    def _assert_durable_batch_context(
        connection: sqlite3.Connection,
        identity: AttemptIdentity,
        expectation: CanonicalBatchExpectation,
    ) -> None:
        context = expectation.batch_context
        processing = context.batch_identity.processing_signature
        row = connection.execute(
            """
            SELECT context.*, contract.processing_signature_hash,
                   contract.processing_signature_json, contract.source_id,
                   contract.provenance_json, contract.provenance_hash,
                   calendar.schedule_checksum, request.request_spec_hash,
                   request.specification_json
            FROM batch_contexts AS context
            JOIN batch_context_processing_contracts AS contract
              ON contract.batch_context_id = context.batch_context_id
            JOIN calendar_snapshots AS calendar
              ON calendar.calendar_snapshot_id = context.calendar_snapshot_id
            JOIN request_specs AS request
              ON request.request_spec_id = context.request_spec_id
            JOIN batch_context_requests AS link
              ON link.batch_context_id = context.batch_context_id
            WHERE context.batch_context_id = ? AND link.request_instance_id = ?
            """,
            (context.batch_context_id, str(identity.request_instance_id)),
        ).fetchone()
        if row is None:
            raise ExecutionStateConflictError("durable batch context is absent")
        expected = (
            context.canonical_batch_id,
            expectation.specification.request_spec_id,
            context.batch_identity.ordered_artifacts_hash,
            processing.canonical_schema_version,
            processing.normalizer_version,
            processing.validator_version,
            processing.calendar_snapshot_checksum,
            _format_utc(context.fixed_ingested_at),
            _format_utc(context.manifest_created_at),
            _hash_json(processing.model_dump(mode="json")),
            _canonical_json(processing),
            str(expectation.provenance.source_id),
            _canonical_json(expectation.provenance),
            _hash_json(expectation.provenance.model_dump(mode="json")),
            expectation.specification.request_spec_hash,
            expectation.specification.canonical_json,
        )
        actual = (
            str(row["canonical_batch_id"]),
            str(row["request_spec_id"]),
            str(row["ordered_artifacts_hash"]),
            str(row["canonical_schema_version"]),
            str(row["normalizer_version"]),
            str(row["validator_version"]),
            str(row["schedule_checksum"]),
            str(row["fixed_ingested_at"]),
            str(row["manifest_created_at"]),
            str(row["processing_signature_hash"]),
            str(row["processing_signature_json"]),
            str(row["source_id"]),
            str(row["provenance_json"]),
            str(row["provenance_hash"]),
            str(row["request_spec_hash"]),
            str(row["specification_json"]),
        )
        if actual != expected:
            raise ExecutionIdentityCollisionError(
                "batch context differs from persisted replay semantics"
            )
        artifacts = connection.execute(
            """
            SELECT artifact_id, ordinal FROM batch_context_artifacts
            WHERE batch_context_id = ? ORDER BY ordinal
            """,
            (context.batch_context_id,),
        ).fetchall()
        if tuple((str(row["artifact_id"]), int(row["ordinal"])) for row in artifacts) != tuple(
            (artifact_id, ordinal)
            for ordinal, artifact_id in enumerate(context.batch_identity.artifact_ids)
        ):
            raise ExecutionIdentityCollisionError(
                "batch context artifact sequence differs from durable state"
            )


__all__ = [
    "CatalogedRawArtifact",
    "CoverageCommit",
    "DurableAcquisition",
    "DurableAttempt",
    "DurableBatchContext",
    "ExecutionFaultPoint",
    "ExecutionIdentityCollisionError",
    "ExecutionIntegrityError",
    "ExecutionRepositoryError",
    "ExecutionStateConflictError",
    "FaultInjector",
    "IngestionExecutionRepository",
    "PreparedBatch",
    "PreparedPublicationRecord",
    "PublicationCommitRequest",
    "PublicationCommitResult",
    "PublicationCommitSource",
    "RawReplayRecord",
    "TerminalFailureResult",
]
