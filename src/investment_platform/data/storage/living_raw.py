"""Retention-authorized, content-identified raw artifact publication."""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Annotated, BinaryIO, Final, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.ingestion.identity import (
    RawArtifactIdentity,
    RequestSpecification,
)
from investment_platform.data.retention import (
    DatasetPolicyDenied,
    DatasetRuntimeStatus,
    ResponsePageAuthorization,
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
    managed_path,
    remove_owned_staging_directory,
    safe_partition_value,
    write_file_durably,
)
from investment_platform.data_root import PrivateDataRoot

_MANIFEST_NAME: Final = "manifest.json"
_PAYLOAD_NAME: Final = "payload.bin"
_RAW_STAGING_PARENT: Final = PurePosixPath("staging/raw-artifacts")
_DEFAULT_CHUNK_SIZE: Final = 1024 * 1024
Sha256Hex = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class _FrozenManifestModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class RawPayloadManifest(_FrozenManifestModel):
    relative_path: str = Field(default="payload.bin", pattern=r"^payload\.bin$")
    sha256: Sha256Hex
    byte_count: Annotated[int, Field(ge=0)]


class RawArtifactManifest(_FrozenManifestModel):
    """Immutable raw identity plus fixed first-persistence provenance."""

    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    artifact_id: str = Field(pattern=r"^raw_v1_[0-9a-f]{64}$")
    provider: str
    dataset: str
    identity: RawArtifactIdentity
    payload: RawPayloadManifest
    first_persisted_at: datetime

    @field_validator("first_persisted_at", mode="after")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("first_persisted_at must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.artifact_id != self.identity.artifact_id:
            raise ValueError("artifact_id does not match the raw identity")
        if (
            self.payload.sha256 != self.identity.content_sha256
            or self.payload.byte_count != self.identity.byte_count
        ):
            raise ValueError("payload metadata does not match the raw identity")
        safe_partition_value(self.provider, label="provider")
        safe_partition_value(self.dataset, label="dataset")
        return self


class PublishedRawArtifact(_FrozenManifestModel):
    """Verified immutable raw artifact returned to the operational coordinator."""

    root_id: UUID
    artifact_id: str
    relative_directory: str
    payload_relative_path: str
    manifest_relative_path: str
    content_sha256: Sha256Hex
    byte_count: Annotated[int, Field(ge=0)]
    first_persisted_at: datetime
    created: bool

    @field_validator("first_persisted_at", mode="after")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("first_persisted_at must be timezone-aware")
        return value.astimezone(UTC)


def raw_artifact_relative_directory(
    provider: str,
    dataset: str,
    artifact_id: str,
) -> PurePosixPath:
    provider = safe_partition_value(provider, label="provider")
    dataset = safe_partition_value(dataset, label="dataset")
    return PurePosixPath(
        "raw",
        f"provider={provider}",
        f"dataset={dataset}",
        "artifacts",
        # The full content identity remains in the immutable manifest/catalog. A
        # 128-bit physical key keeps deeply nested Windows private roots usable;
        # any truncation collision is detected by full-identity verification.
        f"artifact={artifact_id.removeprefix('raw_v1_')[:32]}",
    )


def _assert_authorization_shape(
    specification: RequestSpecification,
    authorization: ResponsePageAuthorization,
    *,
    page_ordinal: int,
    canonical_media_type: str,
    content_encoding: str,
) -> None:
    snapshot = authorization.request.policy_snapshot
    if (snapshot.provider, snapshot.dataset) != (
        specification.provider,
        specification.dataset,
    ):
        raise DatasetPolicyDenied("raw authorization is for a different exact dataset")
    if (
        authorization.request.request_start != specification.start
        or authorization.request.request_end != specification.end
    ):
        raise DatasetPolicyDenied("raw authorization is for different request bounds")
    if authorization.request.request_spec_hash != specification.request_spec_hash:
        raise DatasetPolicyDenied("raw authorization is for a different request specification")
    if authorization.page_ordinal != page_ordinal:
        raise DatasetPolicyDenied("raw authorization page ordinal is inconsistent")
    if authorization.canonical_media_type != canonical_media_type:
        raise DatasetPolicyDenied("raw authorization media type is inconsistent")
    if authorization.content_encoding != content_encoding:
        raise DatasetPolicyDenied("raw authorization content encoding is inconsistent")


def _authorized_identity(
    specification: RequestSpecification,
    authorization: ResponsePageAuthorization,
    *,
    page_ordinal: int,
    media_type: str,
    content_encoding: str,
) -> RawArtifactIdentity:
    identity = RawArtifactIdentity.from_digest(
        request_spec_hash=specification.request_spec_hash,
        page_ordinal=page_ordinal,
        page_relation=authorization.page_relation,
        media_type=media_type,
        content_encoding=content_encoding,
        content_sha256=authorization.payload_sha256,
        byte_count=authorization.payload_size_bytes,
    )
    _assert_authorization_shape(
        specification,
        authorization,
        page_ordinal=page_ordinal,
        canonical_media_type=identity.media_type,
        content_encoding=identity.content_encoding,
    )
    return identity


def verify_raw_artifact_directory(
    directory: Path,
    *,
    expected_identity: RawArtifactIdentity | None = None,
    expected_provider: str | None = None,
    expected_dataset: str | None = None,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
) -> RawArtifactManifest:
    """Share strict tree, manifest, and checksum verification with recovery."""

    try:
        relative_files = {
            path.relative_to(directory).as_posix() for path in iter_safe_regular_files(directory)
        }
        if relative_files != {_MANIFEST_NAME, _PAYLOAD_NAME}:
            raise PublicationIntegrityError("raw artifact has missing or unexpected files")
        manifest = RawArtifactManifest.model_validate_json(
            (directory / _MANIFEST_NAME).read_bytes()
        )
    except (OSError, ValueError) as error:
        raise PublicationIntegrityError("raw artifact manifest is missing or invalid") from error
    if expected_identity is not None and manifest.identity != expected_identity:
        raise PublicationCollisionError(
            f"raw artifact {expected_identity.artifact_id} has conflicting immutable metadata"
        )
    if expected_provider is not None and manifest.provider != expected_provider:
        raise PublicationCollisionError("raw artifact provider conflicts with its exact path")
    if expected_dataset is not None and manifest.dataset != expected_dataset:
        raise PublicationCollisionError("raw artifact dataset conflicts with its exact path")
    sha256, byte_count = file_integrity(directory / _PAYLOAD_NAME, chunk_size=chunk_size)
    if sha256 != manifest.payload.sha256 or byte_count != manifest.payload.byte_count:
        raise PublicationIntegrityError("raw artifact payload failed content verification")
    return manifest


class RawArtifactPublisher:
    """Stream exact authorized bytes to staging, then atomically publish one raw page."""

    def __init__(
        self,
        data_root: PrivateDataRoot,
        policy_enforcer: RetentionPolicyEnforcer,
        *,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self._data_root = data_root
        self._root_id = data_root.validate().root_id
        self._policy_enforcer = policy_enforcer
        self._chunk_size = chunk_size

    @property
    def root_id(self) -> UUID:
        return self._root_id

    def _verified_existing(
        self,
        relative_directory: PurePosixPath,
        expected_identity: RawArtifactIdentity,
        *,
        provider: str,
        dataset: str,
    ) -> PublishedRawArtifact:
        target = managed_path(self._data_root, self._root_id, relative_directory)
        assert_direct_owned_directory(target, parent=target.parent)
        try:
            manifest = verify_raw_artifact_directory(
                target,
                expected_identity=expected_identity,
                expected_provider=provider,
                expected_dataset=dataset,
                chunk_size=self._chunk_size,
            )
        except PublicationIntegrityError as error:
            raise PublicationCollisionError(
                f"raw artifact {expected_identity.artifact_id} is incomplete or invalid"
            ) from error
        return PublishedRawArtifact(
            root_id=self._root_id,
            artifact_id=expected_identity.artifact_id,
            relative_directory=relative_directory.as_posix(),
            payload_relative_path=(relative_directory / _PAYLOAD_NAME).as_posix(),
            manifest_relative_path=(relative_directory / _MANIFEST_NAME).as_posix(),
            content_sha256=manifest.payload.sha256,
            byte_count=manifest.payload.byte_count,
            first_persisted_at=manifest.first_persisted_at,
            created=False,
        )

    def verify_published(
        self,
        specification: RequestSpecification,
        identity: RawArtifactIdentity,
    ) -> PublishedRawArtifact:
        """Reopen one expected raw page without mutating or adopting it."""

        if identity.request_spec_hash != specification.request_spec_hash:
            raise PublicationIntegrityError(
                "raw artifact belongs to a different request specification"
            )
        relative = raw_artifact_relative_directory(
            specification.provider,
            specification.dataset,
            identity.artifact_id,
        )
        return self._verified_existing(
            relative,
            identity,
            provider=specification.provider,
            dataset=specification.dataset,
        )

    @staticmethod
    def _staging_prefix(identity: RawArtifactIdentity) -> str:
        return f"artifact={identity.artifact_hash}."

    def _cleanup_artifact_staging(
        self,
        staging_parent: Path,
        identity: RawArtifactIdentity,
    ) -> None:
        prefix = self._staging_prefix(identity)
        for candidate in sorted(staging_parent.iterdir(), key=lambda value: value.name):
            if candidate.name.startswith(prefix):
                remove_owned_staging_directory(
                    self._data_root,
                    self._root_id,
                    candidate,
                    staging_parent=staging_parent,
                )

    def _recover_staging_candidate(
        self,
        staging_parent: Path,
        identity: RawArtifactIdentity,
        *,
        provider: str,
        dataset: str,
    ) -> tuple[Path, RawArtifactManifest] | None:
        prefix = self._staging_prefix(identity)
        matching: list[tuple[Path, RawArtifactManifest]] = []
        for candidate in sorted(staging_parent.iterdir(), key=lambda value: value.name):
            if not candidate.name.startswith(prefix):
                continue
            checked = managed_path(
                self._data_root,
                self._root_id,
                candidate.relative_to(self._data_root.root),
            )
            if checked != candidate or not checked.is_dir():
                raise PublicationIntegrityError("raw staging candidate is unsafe")
            try:
                manifest = verify_raw_artifact_directory(
                    candidate,
                    expected_identity=identity,
                    expected_provider=provider,
                    expected_dataset=dataset,
                    chunk_size=self._chunk_size,
                )
            except PublicationCollisionError:
                raise
            except PublicationIntegrityError as error:
                try:
                    files = {
                        path.relative_to(candidate).as_posix()
                        for path in iter_safe_regular_files(candidate)
                    }
                except PublicationIntegrityError:
                    raise
                complete_manifest = False
                if _MANIFEST_NAME in files:
                    try:
                        RawArtifactManifest.model_validate_json(
                            (candidate / _MANIFEST_NAME).read_bytes()
                        )
                    except (OSError, ValueError):
                        pass
                    else:
                        complete_manifest = True
                if complete_manifest:
                    raise PublicationCollisionError(
                        "complete raw staging candidate failed integrity verification"
                    ) from error
                remove_owned_staging_directory(
                    self._data_root,
                    self._root_id,
                    candidate,
                    staging_parent=staging_parent,
                )
                continue
            matching.append((candidate, manifest))
        if not matching:
            return None
        selected = matching[0]
        for candidate, _ in matching[1:]:
            remove_owned_staging_directory(
                self._data_root,
                self._root_id,
                candidate,
                staging_parent=staging_parent,
            )
        return selected

    def _publish_verified_candidate(
        self,
        candidate: Path,
        manifest: RawArtifactManifest,
        *,
        staging_parent: Path,
        fault_injector: FaultInjector | None,
    ) -> PublishedRawArtifact:
        identity = manifest.identity
        relative_directory = raw_artifact_relative_directory(
            manifest.provider,
            manifest.dataset,
            identity.artifact_id,
        )
        target_parent = ensure_directory(
            self._data_root,
            self._root_id,
            relative_directory.parent,
        )
        target = managed_path(self._data_root, self._root_id, relative_directory)
        if target.exists() or target.is_symlink():
            existing = self._verified_existing(
                relative_directory,
                identity,
                provider=manifest.provider,
                dataset=manifest.dataset,
            )
            self._cleanup_artifact_staging(staging_parent, identity)
            return existing

        assert_owned_staging_candidate(
            self._data_root,
            self._root_id,
            candidate,
            staging_parent=staging_parent,
        )
        self._data_root.validate(expected_root_id=self._root_id)
        try:
            atomic_rename_directory(candidate, target)
        except OSError as error:
            if target.exists() and target.is_dir():
                existing = self._verified_existing(
                    relative_directory,
                    identity,
                    provider=manifest.provider,
                    dataset=manifest.dataset,
                )
                self._cleanup_artifact_staging(staging_parent, identity)
                return existing
            raise PublicationError("raw artifact atomic publication failed") from error
        fsync_directory(target_parent)
        invoke_fault(fault_injector, PublicationFaultPoint.RENAME)
        invoke_fault(fault_injector, PublicationFaultPoint.REOPEN)
        verified = self._verified_existing(
            relative_directory,
            identity,
            provider=manifest.provider,
            dataset=manifest.dataset,
        )
        self._cleanup_artifact_staging(staging_parent, identity)
        return verified.model_copy(update={"created": True})

    def publish(
        self,
        specification: RequestSpecification,
        payload: BinaryIO,
        *,
        page_ordinal: int,
        media_type: str,
        content_encoding: str,
        authorization: ResponsePageAuthorization,
        first_persisted_at: datetime,
        runtime_status: DatasetRuntimeStatus | None = None,
        fault_injector: FaultInjector | None = None,
    ) -> PublishedRawArtifact:
        """Publish one page with at-least-once execution and idempotent filesystem effects."""

        # Bind the request, representation, page relation, and exact inspected
        # bytes before transient persistence starts.
        identity = _authorized_identity(
            specification,
            authorization,
            page_ordinal=page_ordinal,
            media_type=media_type,
            content_encoding=content_encoding,
        )
        if first_persisted_at.tzinfo is None or first_persisted_at.utcoffset() is None:
            raise ValueError("first_persisted_at must be timezone-aware")
        first_persisted_at = first_persisted_at.astimezone(UTC)
        if first_persisted_at < authorization.authorized_at:
            raise ValueError("first_persisted_at cannot precede response authorization")
        self._policy_enforcer.authorize_persistence(
            specification.provider,
            specification.dataset,
            environment=authorization.request.environment,
            layer=RetentionLayer.RAW,
            runtime_status=runtime_status,
            response_authorization=authorization,
            payload_sha256=identity.content_sha256,
            payload_size_bytes=identity.byte_count,
            canonical_media_type=identity.media_type,
            content_encoding=identity.content_encoding,
            request_spec_hash=identity.request_spec_hash,
            page_ordinal=identity.page_ordinal,
            page_relation=identity.page_relation,
        )
        self._data_root.validate(expected_root_id=self._root_id)
        relative_directory = raw_artifact_relative_directory(
            specification.provider,
            specification.dataset,
            identity.artifact_id,
        )
        target = managed_path(self._data_root, self._root_id, relative_directory)
        if target.exists() or target.is_symlink():
            existing = self._verified_existing(
                relative_directory,
                identity,
                provider=specification.provider,
                dataset=specification.dataset,
            )
            staging_parent = managed_path(
                self._data_root,
                self._root_id,
                _RAW_STAGING_PARENT,
            )
            if staging_parent.exists():
                self._cleanup_artifact_staging(staging_parent, identity)
            return existing
        staging_parent = ensure_directory(
            self._data_root,
            self._root_id,
            _RAW_STAGING_PARENT,
        )
        recovered = self._recover_staging_candidate(
            staging_parent,
            identity,
            provider=specification.provider,
            dataset=specification.dataset,
        )
        if recovered is not None:
            candidate, recovered_manifest = recovered
            invoke_fault(fault_injector, PublicationFaultPoint.STAGED_MANIFEST_VERIFIED)
            return self._publish_verified_candidate(
                candidate,
                recovered_manifest,
                staging_parent=staging_parent,
                fault_injector=fault_injector,
            )
        candidate = staging_parent / (f"{self._staging_prefix(identity)}{uuid4().hex[:16]}.tmp")
        try:
            candidate.mkdir(exist_ok=False)
            fsync_directory(staging_parent)
        except OSError as error:
            raise PublicationError("failed to create raw artifact staging directory") from error
        invoke_fault(fault_injector, PublicationFaultPoint.STAGING)
        assert_owned_staging_candidate(
            self._data_root,
            self._root_id,
            candidate,
            staging_parent=staging_parent,
        )

        staged_payload = candidate / _PAYLOAD_NAME
        digest = hashlib.sha256()
        byte_count = 0
        try:
            with staged_payload.open("xb") as writer:
                while chunk := payload.read(self._chunk_size):
                    if not isinstance(chunk, bytes):
                        raise PublicationError("raw payload reader must return bytes")
                    if len(chunk) > self._chunk_size:
                        raise PublicationError("raw payload reader exceeded the bounded read size")
                    writer.write(chunk)
                    digest.update(chunk)
                    byte_count += len(chunk)
                writer.flush()
                os.fsync(writer.fileno())
        except OSError as error:
            raise PublicationError("failed while streaming raw bytes to staging") from error
        invoke_fault(fault_injector, PublicationFaultPoint.RAW_WRITE)
        assert_owned_staging_candidate(
            self._data_root,
            self._root_id,
            candidate,
            staging_parent=staging_parent,
        )

        if digest.hexdigest() != identity.content_sha256 or byte_count != identity.byte_count:
            remove_owned_staging_directory(
                self._data_root,
                self._root_id,
                candidate,
                staging_parent=staging_parent,
            )
            raise DatasetPolicyDenied("raw bytes differ from the inspected response authorization")

        manifest = RawArtifactManifest(
            artifact_id=identity.artifact_id,
            provider=specification.provider,
            dataset=specification.dataset,
            identity=identity,
            payload=RawPayloadManifest(
                sha256=identity.content_sha256,
                byte_count=identity.byte_count,
            ),
            first_persisted_at=first_persisted_at,
        )
        write_file_durably(candidate / _MANIFEST_NAME, json_bytes(manifest.model_dump(mode="json")))
        fsync_directory(candidate)
        invoke_fault(fault_injector, PublicationFaultPoint.MANIFEST)
        assert_owned_staging_candidate(
            self._data_root,
            self._root_id,
            candidate,
            staging_parent=staging_parent,
        )
        verify_raw_artifact_directory(
            candidate,
            expected_identity=identity,
            expected_provider=specification.provider,
            expected_dataset=specification.dataset,
            chunk_size=self._chunk_size,
        )
        invoke_fault(fault_injector, PublicationFaultPoint.STAGED_MANIFEST_VERIFIED)
        assert_owned_staging_candidate(
            self._data_root,
            self._root_id,
            candidate,
            staging_parent=staging_parent,
        )
        return self._publish_verified_candidate(
            candidate,
            manifest,
            staging_parent=staging_parent,
            fault_injector=fault_injector,
        )


__all__ = [
    "PublishedRawArtifact",
    "RawArtifactManifest",
    "RawArtifactPublisher",
    "RawPayloadManifest",
    "raw_artifact_relative_directory",
    "verify_raw_artifact_directory",
]
