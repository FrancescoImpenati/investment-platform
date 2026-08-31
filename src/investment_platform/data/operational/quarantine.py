"""Durable catalog for policy-authorized, metadata-only quarantine findings."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from investment_platform.data.ingestion.identity import AttemptIdentity, RequestSpecification
from investment_platform.data.operational.planning import deterministic_policy_snapshot_id
from investment_platform.data.operational.store import (
    OperationalStateError,
    OperationalStateStore,
    WriterLease,
    _format_utc,
)
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    DatasetRuntimeStatus,
    RetentionLayer,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage._publication import file_integrity, json_bytes
from investment_platform.data.storage.quarantine import (
    PublishedQuarantineArtifact,
    QuarantineArtifactManifest,
    quarantine_artifact_relative_directory,
    verify_quarantine_artifact_directory,
)
from investment_platform.data_root import PrivateDataRoot
from investment_platform.runtime import RuntimeEnvironment


class QuarantineRepositoryError(OperationalStateError):
    """Base error for quarantine catalog persistence."""


class QuarantineCatalogCollisionError(QuarantineRepositoryError):
    """A deterministic quarantine identity already carries different metadata."""


class QuarantineCatalogIntegrityError(QuarantineRepositoryError):
    """Filesystem, acquisition, batch, policy, and catalog provenance do not agree."""


class _FrozenCatalogModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class CatalogedQuarantineArtifact(_FrozenCatalogModel):
    quarantine_artifact_id: str = Field(pattern=r"^quarantine_v1_[0-9a-f]{64}$")
    request_instance_id: UUID
    attempt_id: UUID
    batch_context_id: str = Field(pattern=r"^batch_context_v1_[0-9a-f]{64}$")
    policy_snapshot_id: str
    relative_directory: str
    manifest_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_byte_count: Annotated[int, Field(gt=0)]
    created_at: datetime
    replayed: bool

    @field_validator("created_at", mode="after")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("cataloged quarantine time must be timezone-aware")
        return value.astimezone(UTC)


def _canonical_json(value: object) -> str:
    return json_bytes(value).decode("utf-8").rstrip("\n")


def _authorization_hash(value: AcquisitionPolicyAuthorization) -> str:
    encoded = json.dumps(
        value.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validation_summary(manifest: QuarantineArtifactManifest) -> str:
    return _canonical_json(
        {
            "blocked_streams": [
                {
                    "request_end": value.request_end.isoformat().replace("+00:00", "Z"),
                    "request_start": value.request_start.isoformat().replace("+00:00", "Z"),
                    "stream_id": value.stream_id,
                    "validation_codes": list(value.validation_codes),
                }
                for value in manifest.blocked_streams
            ],
            "schema_version": 1,
        }
    )


class QuarantineArtifactRepository:
    """Catalog only exact, reopened findings backed by complete authorized raw acquisition."""

    def __init__(
        self,
        store: OperationalStateStore,
        data_root: PrivateDataRoot,
        enforcer: RetentionPolicyEnforcer,
    ) -> None:
        sentinel = data_root.validate()
        if str(sentinel.root_id) != store.root_id:
            raise QuarantineCatalogIntegrityError(
                "operational store and quarantine catalog use different private roots"
            )
        self._store = store
        self._data_root = data_root
        self._root_id = sentinel.root_id
        self._enforcer = enforcer

    def catalog(
        self,
        lease: WriterLease,
        identity: AttemptIdentity,
        specification: RequestSpecification,
        authorization: AcquisitionPolicyAuthorization,
        manifest: QuarantineArtifactManifest,
        published: PublishedQuarantineArtifact,
        *,
        environment: RuntimeEnvironment,
        runtime_status: DatasetRuntimeStatus | None = None,
    ) -> CatalogedQuarantineArtifact:
        """Atomically bind one verified filesystem finding to its exact attempt and policy."""

        if published.root_id != self._root_id:
            raise QuarantineCatalogIntegrityError("published quarantine belongs to another root")
        if manifest.request_specification != specification:
            raise QuarantineCatalogIntegrityError("quarantine manifest has another exact request")
        policy_snapshot_id = deterministic_policy_snapshot_id(authorization.request.policy_snapshot)
        # Re-run the central gate at the catalog boundary; filesystem publication alone cannot
        # make an otherwise denied artifact operationally visible.
        self._enforcer.authorize_quarantine(
            specification.provider,
            specification.dataset,
            environment=environment,
            layer=RetentionLayer.NORMALIZED,
            runtime_status=runtime_status,
            acquisition_authorization=authorization,
            input_artifacts=authorization.ordered_artifacts,
            input_page_sha256=authorization.ordered_page_sha256,
        )
        expected_relative = quarantine_artifact_relative_directory(
            specification.provider,
            specification.dataset,
            manifest.quarantine_artifact_id,
        )
        if (
            published.quarantine_artifact_id != manifest.quarantine_artifact_id
            or PurePosixPath(published.relative_directory) != expected_relative
            or PurePosixPath(published.manifest_relative_path)
            != expected_relative / "manifest.json"
        ):
            raise QuarantineCatalogIntegrityError(
                "published quarantine paths differ from deterministic identity"
            )
        directory = self._data_root.managed_path(
            Path(*expected_relative.parts), expected_root_id=self._root_id
        )
        persisted = verify_quarantine_artifact_directory(
            directory,
            data_root=self._data_root,
            root_id=self._root_id,
            expected_manifest=manifest,
        )
        digest, byte_count = file_integrity(directory / "manifest.json")
        if (
            persisted != manifest
            or digest != published.manifest_content_sha256
            or byte_count != published.manifest_byte_count
        ):
            raise QuarantineCatalogIntegrityError(
                "reopened quarantine manifest differs from publication result"
            )
        validation_summary = _validation_summary(manifest)
        with self._store._leased_transaction(lease) as connection:
            self._validate_operational_provenance(
                connection,
                identity=identity,
                specification=specification,
                authorization=authorization,
                manifest=manifest,
                policy_snapshot_id=policy_snapshot_id,
            )
            existing = connection.execute(
                "SELECT * FROM quarantine_artifacts WHERE quarantine_artifact_id = ?",
                (manifest.quarantine_artifact_id,),
            ).fetchone()
            values = {
                "quarantine_artifact_id": manifest.quarantine_artifact_id,
                "provider": specification.provider,
                "dataset": specification.dataset,
                "request_spec_id": specification.request_spec_id,
                "batch_context_id": manifest.batch_context.batch_context_id,
                "policy_snapshot_id": policy_snapshot_id,
                "request_instance_id": str(identity.request_instance_id),
                "attempt_id": str(identity.attempt_id),
                "relative_path": expected_relative.as_posix(),
                "manifest_content_sha256": digest,
                "manifest_byte_count": byte_count,
                "validation_summary_json": validation_summary,
                "state": "VERIFIED",
                "created_at": _format_utc(manifest.created_at),
                "invalidated_at": None,
            }
            replayed = existing is not None
            catalog_request_instance_id = identity.request_instance_id
            catalog_attempt_id = identity.attempt_id
            catalog_policy_snapshot_id = policy_snapshot_id
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO quarantine_artifacts(
                        quarantine_artifact_id, provider, dataset, request_spec_id,
                        batch_context_id, policy_snapshot_id, request_instance_id, attempt_id,
                        relative_path, manifest_content_sha256, manifest_byte_count,
                        validation_summary_json, state, created_at, invalidated_at
                    ) VALUES (
                        :quarantine_artifact_id, :provider, :dataset, :request_spec_id,
                        :batch_context_id, :policy_snapshot_id, :request_instance_id, :attempt_id,
                        :relative_path, :manifest_content_sha256, :manifest_byte_count,
                        :validation_summary_json, :state, :created_at, :invalidated_at
                    )
                    """,
                    values,
                )
            else:
                self._assert_existing(existing, values)
                catalog_request_instance_id = UUID(str(existing["request_instance_id"]))
                catalog_attempt_id = UUID(str(existing["attempt_id"]))
                catalog_policy_snapshot_id = str(existing["policy_snapshot_id"])
        return CatalogedQuarantineArtifact(
            quarantine_artifact_id=manifest.quarantine_artifact_id,
            request_instance_id=catalog_request_instance_id,
            attempt_id=catalog_attempt_id,
            batch_context_id=manifest.batch_context.batch_context_id,
            policy_snapshot_id=catalog_policy_snapshot_id,
            relative_directory=expected_relative.as_posix(),
            manifest_content_sha256=digest,
            manifest_byte_count=byte_count,
            created_at=manifest.created_at,
            replayed=replayed,
        )

    @staticmethod
    def _assert_existing(existing: sqlite3.Row, expected: dict[str, object]) -> None:
        # The filesystem identity is semantic, while policy/request/attempt are operational links
        # to the first authorized observation.  A later exact replay under a newer active policy
        # may reuse the same immutable finding after its own acquisition provenance was validated.
        immutable_columns = (
            "quarantine_artifact_id",
            "provider",
            "dataset",
            "request_spec_id",
            "batch_context_id",
            "relative_path",
            "manifest_content_sha256",
            "manifest_byte_count",
            "validation_summary_json",
            "state",
            "created_at",
            "invalidated_at",
        )
        for column in immutable_columns:
            value = expected[column]
            actual = existing[column]
            if actual != value:
                raise QuarantineCatalogCollisionError(
                    "quarantine identity collides with different durable metadata"
                )

    @staticmethod
    def _validate_operational_provenance(
        connection: sqlite3.Connection,
        *,
        identity: AttemptIdentity,
        specification: RequestSpecification,
        authorization: AcquisitionPolicyAuthorization,
        manifest: QuarantineArtifactManifest,
        policy_snapshot_id: str,
    ) -> None:
        row = connection.execute(
            """
            SELECT attempt.request_instance_id, instance.request_spec_id,
                   request.specification_json,
                   acquisition.policy_snapshot_id,
                   acquisition.authorization_hash,
                   acquisition.authorization_json,
                   context.request_spec_id AS context_request_spec_id,
                   context_request.request_instance_id AS context_request_instance_id,
                   contract.processing_signature_json
            FROM request_attempts AS attempt
            JOIN request_instances AS instance
              ON instance.request_instance_id = attempt.request_instance_id
            JOIN request_specs AS request ON request.request_spec_id = instance.request_spec_id
            JOIN attempt_acquisition_records AS acquisition
              ON acquisition.attempt_id = attempt.attempt_id
            JOIN batch_contexts AS context ON context.batch_context_id = ?
            JOIN batch_context_requests AS context_request
              ON context_request.batch_context_id = context.batch_context_id
             AND context_request.request_instance_id = instance.request_instance_id
            JOIN batch_context_processing_contracts AS contract
              ON contract.batch_context_id = context.batch_context_id
            WHERE attempt.attempt_id = ?
            """,
            (manifest.batch_context.batch_context_id, str(identity.attempt_id)),
        ).fetchone()
        if row is None:
            raise QuarantineCatalogIntegrityError(
                "quarantine lacks complete durable attempt/acquisition/batch provenance"
            )
        expected = (
            str(identity.request_instance_id),
            specification.request_spec_id,
            None,
            policy_snapshot_id,
            _authorization_hash(authorization),
            None,
            specification.request_spec_id,
            str(identity.request_instance_id),
            None,
        )
        actual = tuple(row)
        # JSON in durable repositories is canonical but not necessarily rendered through the same
        # Pydantic convenience method.  Compare parsed objects for those three columns.
        scalar_actual = (actual[0], actual[1], actual[3], actual[4], actual[6], actual[7])
        scalar_expected = (
            expected[0],
            expected[1],
            expected[3],
            expected[4],
            expected[6],
            expected[7],
        )
        if scalar_actual != scalar_expected:
            raise QuarantineCatalogIntegrityError(
                "quarantine durable identities disagree with the exact attempt"
            )
        try:
            stored_authorization = AcquisitionPolicyAuthorization.model_validate_json(
                str(actual[5])
            )
            stored_signature = json.loads(str(actual[8]))
        except (ValueError, json.JSONDecodeError) as error:
            raise QuarantineCatalogIntegrityError(
                "quarantine durable provenance JSON is invalid"
            ) from error
        if (
            str(actual[2]) != specification.canonical_json
            or stored_authorization != authorization
            or stored_signature != manifest.processing_signature.model_dump(mode="json")
        ):
            raise QuarantineCatalogIntegrityError(
                "quarantine durable provenance differs from the supplied exact evidence"
            )
        artifact_rows = connection.execute(
            """
            SELECT artifact_id FROM acquisition_artifacts
            WHERE attempt_id = ? ORDER BY ordinal
            """,
            (str(identity.attempt_id),),
        ).fetchall()
        actual_artifacts = tuple(str(value[0]) for value in artifact_rows)
        expected_artifacts = tuple(value.artifact_id for value in manifest.ordered_raw_artifacts)
        if actual_artifacts != expected_artifacts:
            raise QuarantineCatalogIntegrityError(
                "quarantine ordered raw artifacts differ from durable acquisition"
            )


__all__ = [
    "CatalogedQuarantineArtifact",
    "QuarantineArtifactRepository",
    "QuarantineCatalogCollisionError",
    "QuarantineCatalogIntegrityError",
    "QuarantineRepositoryError",
]
