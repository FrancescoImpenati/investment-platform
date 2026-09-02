"""Policy-authorized, metadata-only quarantine findings for living ingestion.

Quarantine never becomes an escape hatch for provider bytes.  The immutable raw pages remain the
replay source; this artifact records only the exact request, frozen processing context, ordered raw
content identities, and sanitized blocking validation codes.  Publication follows the same
manifest-last, no-replace directory protocol used by raw and canonical storage.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Annotated, Final, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.ingestion.identity import (
    BatchContext,
    ProcessingSignature,
    RawArtifactIdentity,
    RequestSpecification,
)
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    AuthorizedRawArtifactDescriptor,
    DatasetRuntimeStatus,
    RetentionLayer,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage._publication import (
    FaultInjector,
    PublicationCollisionError,
    PublicationError,
    PublicationFaultPoint,
    PublicationIntegrityError,
    assert_direct_owned_directory,
    assert_owned_staging_candidate,
    atomic_rename_directory,
    ensure_directory,
    file_integrity,
    fsync_directory,
    invoke_fault,
    iter_safe_regular_files,
    json_bytes,
    remove_owned_staging_directory,
    safe_partition_value,
    write_file_durably,
)
from investment_platform.data.storage.canonical_batches import (
    CanonicalStreamOutcome,
    StreamPublicationOutcome,
)
from investment_platform.data_root import PrivateDataRoot
from investment_platform.runtime import RuntimeEnvironment

_MANIFEST_NAME: Final = "manifest.json"
_STAGING_PARENT: Final = PurePosixPath("staging/quarantine-artifacts")
_QUARANTINE_ID_PREFIX: Final = "quarantine_v1_"
Sha256Hex = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class _FrozenQuarantineModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


def _descriptor(identity: RawArtifactIdentity) -> AuthorizedRawArtifactDescriptor:
    return AuthorizedRawArtifactDescriptor(
        request_spec_hash=identity.request_spec_hash,
        page_ordinal=identity.page_ordinal,
        page_relation=identity.page_relation,
        content_sha256=identity.content_sha256,
        byte_count=identity.byte_count,
        media_type=identity.media_type,
        content_encoding=identity.content_encoding,
    )


def _identity_payload(
    *,
    specification: RequestSpecification,
    batch_context: BatchContext,
    blocked_streams: tuple[CanonicalStreamOutcome, ...],
) -> dict[str, object]:
    return {
        "batch_context_id": batch_context.batch_context_id,
        "blocked_streams": [
            value.model_dump(mode="json", exclude_none=True) for value in blocked_streams
        ],
        "kind": "quarantine-finding",
        "ordered_raw_artifacts": [
            value.model_dump(mode="json")
            for value in batch_context.batch_identity.ordered_artifacts
        ],
        "processing_signature": batch_context.batch_identity.processing_signature.model_dump(
            mode="json"
        ),
        "request_spec_hash": specification.request_spec_hash,
        "schema_version": 1,
    }


def deterministic_quarantine_artifact_id(
    *,
    specification: RequestSpecification,
    batch_context: BatchContext,
    blocked_streams: tuple[CanonicalStreamOutcome, ...],
) -> str:
    """Derive one stable identity from exact raw, processing, request, and findings."""

    ordered = tuple(sorted(blocked_streams, key=lambda value: value.stream_id))
    payload = json_bytes(
        _identity_payload(
            specification=specification,
            batch_context=batch_context,
            blocked_streams=ordered,
        )
    )
    return f"{_QUARANTINE_ID_PREFIX}{hashlib.sha256(payload).hexdigest()}"


class QuarantineArtifactManifest(_FrozenQuarantineModel):
    """Redacted deterministic completion record; it contains no market values."""

    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    quarantine_artifact_id: str = Field(pattern=r"^quarantine_v1_[0-9a-f]{64}$")
    provider: str
    dataset: str
    request_specification: RequestSpecification
    batch_context: BatchContext
    ordered_raw_artifacts: Annotated[tuple[RawArtifactIdentity, ...], Field(min_length=1)]
    processing_signature: ProcessingSignature
    blocked_streams: Annotated[tuple[CanonicalStreamOutcome, ...], Field(min_length=1)]
    created_at: datetime

    @field_validator("created_at", mode="after")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("quarantine creation time must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        safe_partition_value(self.provider, label="provider")
        safe_partition_value(self.dataset, label="dataset")
        specification = self.request_specification
        if (self.provider, self.dataset) != (specification.provider, specification.dataset):
            raise ValueError("quarantine dataset differs from its exact request")
        identity = self.batch_context.batch_identity
        if identity.request_spec_hash != specification.request_spec_hash:
            raise ValueError("quarantine batch context belongs to another exact request")
        if self.ordered_raw_artifacts != identity.ordered_artifacts:
            raise ValueError("quarantine raw artifacts differ from the frozen batch context")
        if self.processing_signature != identity.processing_signature:
            raise ValueError("quarantine processing signature differs from the batch context")
        stream_ids = tuple(value.stream_id for value in self.blocked_streams)
        if stream_ids != tuple(sorted(set(stream_ids))):
            raise ValueError("blocked quarantine streams must be unique and sorted")
        requested = {value.stream_id for value in specification.stream_keys()}
        if not set(stream_ids).issubset(requested):
            raise ValueError("quarantine finding contains an unrequested stream")
        if any(
            value.outcome is not StreamPublicationOutcome.BLOCKED
            or value.request_start != specification.start
            or value.request_end != specification.end
            for value in self.blocked_streams
        ):
            raise ValueError("quarantine findings must be blocked within exact request bounds")
        expected_id = deterministic_quarantine_artifact_id(
            specification=specification,
            batch_context=self.batch_context,
            blocked_streams=self.blocked_streams,
        )
        if self.quarantine_artifact_id != expected_id:
            raise ValueError("quarantine artifact ID differs from its deterministic finding")
        return self


class PublishedQuarantineArtifact(_FrozenQuarantineModel):
    quarantine_artifact_id: str = Field(pattern=r"^quarantine_v1_[0-9a-f]{64}$")
    root_id: UUID
    relative_directory: str
    manifest_relative_path: str
    manifest_content_sha256: Sha256Hex
    manifest_byte_count: Annotated[int, Field(gt=0)]
    created: bool


def quarantine_artifact_relative_directory(
    provider: str,
    dataset: str,
    quarantine_artifact_id: str,
) -> PurePosixPath:
    provider = safe_partition_value(provider, label="provider")
    dataset = safe_partition_value(dataset, label="dataset")
    if not (
        quarantine_artifact_id.startswith(_QUARANTINE_ID_PREFIX)
        and len(quarantine_artifact_id) == len(_QUARANTINE_ID_PREFIX) + 64
        and all(value in "0123456789abcdef" for value in quarantine_artifact_id[-64:])
    ):
        raise PublicationError("quarantine artifact identity is invalid")
    return PurePosixPath(
        "quarantine",
        f"provider={provider}",
        f"dataset={dataset}",
        "artifacts",
        f"artifact={quarantine_artifact_id}",
    )


def _manifest_bytes(manifest: QuarantineArtifactManifest) -> bytes:
    return json_bytes(manifest.model_dump(mode="json", exclude_none=True))


def _verify_directory(
    directory: Path,
    *,
    data_root: PrivateDataRoot,
    root_id: UUID,
    expected_manifest: QuarantineArtifactManifest | None,
) -> tuple[QuarantineArtifactManifest, str, int]:
    data_root.validate(expected_root_id=root_id)
    assert_direct_owned_directory(directory, parent=directory.parent)
    files = tuple(iter_safe_regular_files(directory))
    manifest_path = directory / _MANIFEST_NAME
    if files != (manifest_path,):
        raise PublicationIntegrityError(
            "quarantine artifact must contain exactly one manifest and no market values"
        )
    try:
        manifest = QuarantineArtifactManifest.model_validate_json(manifest_path.read_bytes())
    except (OSError, ValueError) as error:
        raise PublicationIntegrityError("quarantine manifest is missing or invalid") from error
    if expected_manifest is not None and manifest != expected_manifest:
        raise PublicationCollisionError("quarantine identity has different finding metadata")
    digest, byte_count = file_integrity(manifest_path)
    if _manifest_bytes(manifest) != manifest_path.read_bytes():
        raise PublicationIntegrityError("quarantine manifest is not canonically serialized")
    data_root.validate(expected_root_id=root_id)
    return manifest, digest, byte_count


def verify_quarantine_artifact_directory(
    directory: Path,
    *,
    data_root: PrivateDataRoot,
    root_id: UUID,
    expected_manifest: QuarantineArtifactManifest | None = None,
) -> QuarantineArtifactManifest:
    """Reopen one published finding and verify structure, identity, and exact bytes."""

    manifest, _, _ = _verify_directory(
        directory,
        data_root=data_root,
        root_id=root_id,
        expected_manifest=expected_manifest,
    )
    return manifest


class QuarantineArtifactPublisher:
    """Publish immutable validation findings only when the exact policy permits quarantine."""

    def __init__(
        self,
        data_root: PrivateDataRoot,
        enforcer: RetentionPolicyEnforcer,
    ) -> None:
        sentinel = data_root.validate()
        self._data_root = data_root
        self._root_id = sentinel.root_id
        self._enforcer = enforcer

    @property
    def root_id(self) -> UUID:
        return self._root_id

    @staticmethod
    def _staging_prefix(artifact_id: str) -> str:
        return f"{artifact_id}."

    def _cleanup_candidates(self, staging_parent: Path, artifact_id: str) -> None:
        prefix = self._staging_prefix(artifact_id)
        for candidate in sorted(staging_parent.iterdir(), key=lambda value: value.name):
            if candidate.name.startswith(prefix):
                remove_owned_staging_directory(
                    self._data_root,
                    self._root_id,
                    candidate,
                    staging_parent=staging_parent,
                )

    def _publish_candidate(
        self,
        candidate: Path,
        target: Path,
        *,
        staging_parent: Path,
        manifest: QuarantineArtifactManifest,
        fault_injector: FaultInjector | None,
    ) -> PublishedQuarantineArtifact:
        assert_owned_staging_candidate(
            self._data_root,
            self._root_id,
            candidate,
            staging_parent=staging_parent,
        )
        try:
            atomic_rename_directory(candidate, target)
            fsync_directory(target.parent)
        except FileExistsError:
            self._cleanup_candidates(staging_parent, manifest.quarantine_artifact_id)
            return self._existing(target, manifest)
        except OSError as error:
            raise PublicationError("quarantine atomic publication failed") from error
        invoke_fault(fault_injector, PublicationFaultPoint.RENAME)
        invoke_fault(fault_injector, PublicationFaultPoint.REOPEN)
        _, digest, byte_count = _verify_directory(
            target,
            data_root=self._data_root,
            root_id=self._root_id,
            expected_manifest=manifest,
        )
        self._cleanup_candidates(staging_parent, manifest.quarantine_artifact_id)
        relative = target.relative_to(self._data_root.root).as_posix()
        return PublishedQuarantineArtifact(
            quarantine_artifact_id=manifest.quarantine_artifact_id,
            root_id=self._root_id,
            relative_directory=relative,
            manifest_relative_path=f"{relative}/{_MANIFEST_NAME}",
            manifest_content_sha256=digest,
            manifest_byte_count=byte_count,
            created=True,
        )

    def _existing(
        self,
        target: Path,
        manifest: QuarantineArtifactManifest,
    ) -> PublishedQuarantineArtifact:
        _, digest, byte_count = _verify_directory(
            target,
            data_root=self._data_root,
            root_id=self._root_id,
            expected_manifest=manifest,
        )
        relative = target.relative_to(self._data_root.root).as_posix()
        return PublishedQuarantineArtifact(
            quarantine_artifact_id=manifest.quarantine_artifact_id,
            root_id=self._root_id,
            relative_directory=relative,
            manifest_relative_path=f"{relative}/{_MANIFEST_NAME}",
            manifest_content_sha256=digest,
            manifest_byte_count=byte_count,
            created=False,
        )

    def publish(
        self,
        specification: RequestSpecification,
        batch_context: BatchContext,
        stream_outcomes: tuple[CanonicalStreamOutcome, ...],
        *,
        authorization: AcquisitionPolicyAuthorization,
        environment: RuntimeEnvironment,
        runtime_status: DatasetRuntimeStatus | None = None,
        fault_injector: FaultInjector | None = None,
    ) -> PublishedQuarantineArtifact | None:
        """Publish sanitized BLOCKED findings; return ``None`` when no stream is blocked."""

        blocked = tuple(
            sorted(
                (
                    value
                    for value in stream_outcomes
                    if value.outcome is StreamPublicationOutcome.BLOCKED
                ),
                key=lambda value: value.stream_id,
            )
        )
        if not blocked:
            return None
        ordered_artifacts = batch_context.batch_identity.ordered_artifacts
        input_artifacts = tuple(_descriptor(value) for value in ordered_artifacts)
        self._enforcer.authorize_quarantine(
            specification.provider,
            specification.dataset,
            environment=environment,
            layer=RetentionLayer.NORMALIZED,
            runtime_status=runtime_status,
            acquisition_authorization=authorization,
            input_artifacts=input_artifacts,
            input_page_sha256=tuple(value.content_sha256 for value in input_artifacts),
        )
        if input_artifacts != authorization.ordered_artifacts:
            raise PublicationIntegrityError(
                "quarantine batch context differs from acquisition authorization"
            )
        artifact_id = deterministic_quarantine_artifact_id(
            specification=specification,
            batch_context=batch_context,
            blocked_streams=blocked,
        )
        manifest = QuarantineArtifactManifest(
            quarantine_artifact_id=artifact_id,
            provider=specification.provider,
            dataset=specification.dataset,
            request_specification=specification,
            batch_context=batch_context,
            ordered_raw_artifacts=ordered_artifacts,
            processing_signature=batch_context.batch_identity.processing_signature,
            blocked_streams=blocked,
            created_at=batch_context.manifest_created_at,
        )
        relative = quarantine_artifact_relative_directory(
            specification.provider,
            specification.dataset,
            artifact_id,
        )
        target_parent = ensure_directory(
            self._data_root,
            self._root_id,
            relative.parent,
        )
        target = target_parent / relative.name
        if target.exists() or target.is_symlink():
            return self._existing(target, manifest)
        staging_parent = ensure_directory(
            self._data_root,
            self._root_id,
            _STAGING_PARENT,
        )
        prefix = self._staging_prefix(artifact_id)
        for candidate in sorted(staging_parent.iterdir(), key=lambda value: value.name):
            if not candidate.name.startswith(prefix):
                continue
            try:
                staged, _, _ = _verify_directory(
                    candidate,
                    data_root=self._data_root,
                    root_id=self._root_id,
                    expected_manifest=manifest,
                )
            except PublicationIntegrityError:
                remove_owned_staging_directory(
                    self._data_root,
                    self._root_id,
                    candidate,
                    staging_parent=staging_parent,
                )
                continue
            if staged != manifest:
                raise PublicationCollisionError("quarantine staging identity collision")
            invoke_fault(fault_injector, PublicationFaultPoint.STAGED_MANIFEST_VERIFIED)
            return self._publish_candidate(
                candidate,
                target,
                staging_parent=staging_parent,
                manifest=manifest,
                fault_injector=fault_injector,
            )

        candidate = staging_parent / f"{prefix}{uuid4().hex[:16]}.tmp"
        try:
            candidate.mkdir()
            fsync_directory(staging_parent)
        except OSError as error:
            raise PublicationError("failed to create quarantine staging directory") from error
        invoke_fault(fault_injector, PublicationFaultPoint.STAGING)
        assert_owned_staging_candidate(
            self._data_root,
            self._root_id,
            candidate,
            staging_parent=staging_parent,
        )
        # There are deliberately no provider values or normalized payload files.  The completion
        # manifest is therefore the only file and, by definition, is written last.
        write_file_durably(candidate / _MANIFEST_NAME, _manifest_bytes(manifest))
        fsync_directory(candidate)
        invoke_fault(fault_injector, PublicationFaultPoint.MANIFEST)
        assert_owned_staging_candidate(
            self._data_root,
            self._root_id,
            candidate,
            staging_parent=staging_parent,
        )
        _verify_directory(
            candidate,
            data_root=self._data_root,
            root_id=self._root_id,
            expected_manifest=manifest,
        )
        invoke_fault(fault_injector, PublicationFaultPoint.STAGED_MANIFEST_VERIFIED)
        return self._publish_candidate(
            candidate,
            target,
            staging_parent=staging_parent,
            manifest=manifest,
            fault_injector=fault_injector,
        )


__all__ = [
    "PublishedQuarantineArtifact",
    "QuarantineArtifactManifest",
    "QuarantineArtifactPublisher",
    "deterministic_quarantine_artifact_id",
    "quarantine_artifact_relative_directory",
    "verify_quarantine_artifact_directory",
]
