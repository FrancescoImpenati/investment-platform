"""Read-only recovery inspection for incomplete staging and uncataloged batches."""

from __future__ import annotations

import re
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Annotated, Final
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from investment_platform.data.storage._publication import (
    PublicationError,
    iter_safe_regular_files,
    managed_path,
    safe_partition_value,
)
from investment_platform.data.storage.canonical_batches import (
    CanonicalBatchExpectation,
    CanonicalBatchManifest,
    verify_canonical_batch_directory,
)
from investment_platform.data.storage.living_raw import verify_raw_artifact_directory
from investment_platform.data_root import PrivateDataRoot, PrivateDataRootError

_RAW_STAGING_PARENT: Final = PurePosixPath("staging/raw-artifacts")
_CANONICAL_STAGING_PARENT: Final = PurePosixPath("staging/canonical-batches")
_MANIFEST_NAME: Final = "manifest.json"


class RecoveryArtifactKind(StrEnum):
    RAW_ARTIFACT = "RAW_ARTIFACT"
    CANONICAL_BATCH = "CANONICAL_BATCH"


class RecoveryInspectionState(StrEnum):
    ABSENT = "ABSENT"
    INCOMPLETE = "INCOMPLETE"
    COMPLETE = "COMPLETE"
    CONTENT_VERIFIED_PUBLISHED = "CONTENT_VERIFIED_PUBLISHED"
    INVALID = "INVALID"


class RecoveryInspection(BaseModel):
    """Sanitized content result; retention/query eligibility is a separate policy gate."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    root_id: UUID
    kind: RecoveryArtifactKind
    state: RecoveryInspectionState
    relative_directory: str
    identity: str | None = None
    error_code: str | None = None
    file_count: Annotated[int, Field(ge=0)] = 0


def _inspect_raw(directory: Path, relative: str, root_id: UUID) -> RecoveryInspection:
    try:
        relative_files = {
            path.relative_to(directory).as_posix() for path in iter_safe_regular_files(directory)
        }
    except PublicationError:
        return RecoveryInspection(
            root_id=root_id,
            kind=RecoveryArtifactKind.RAW_ARTIFACT,
            state=RecoveryInspectionState.INVALID,
            relative_directory=relative,
            error_code="UNSAFE_PUBLICATION_TREE",
        )
    if _MANIFEST_NAME not in relative_files:
        return RecoveryInspection(
            root_id=root_id,
            kind=RecoveryArtifactKind.RAW_ARTIFACT,
            state=RecoveryInspectionState.INCOMPLETE,
            relative_directory=relative,
            error_code="MANIFEST_MISSING",
            file_count=len(relative_files),
        )
    try:
        manifest = verify_raw_artifact_directory(directory)
    except (OSError, ValueError, PublicationError):
        return RecoveryInspection(
            root_id=root_id,
            kind=RecoveryArtifactKind.RAW_ARTIFACT,
            state=RecoveryInspectionState.INVALID,
            relative_directory=relative,
            error_code="INTEGRITY_FAILED",
        )
    return RecoveryInspection(
        root_id=root_id,
        kind=RecoveryArtifactKind.RAW_ARTIFACT,
        state=RecoveryInspectionState.COMPLETE,
        relative_directory=relative,
        identity=manifest.artifact_id,
        file_count=2,
    )


def _inspect_canonical(
    directory: Path,
    relative: str,
    root_id: UUID,
    *,
    data_root: PrivateDataRoot,
    published: bool,
    expected_semantics: CanonicalBatchExpectation | None = None,
    expected_manifest: CanonicalBatchManifest | None = None,
    expected_provider: str | None = None,
    expected_dataset: str | None = None,
    expected_batch_id: str | None = None,
) -> RecoveryInspection:
    try:
        relative_files = {
            path.relative_to(directory).as_posix() for path in iter_safe_regular_files(directory)
        }
    except PublicationError:
        return RecoveryInspection(
            root_id=root_id,
            kind=RecoveryArtifactKind.CANONICAL_BATCH,
            state=RecoveryInspectionState.INVALID,
            relative_directory=relative,
            error_code="UNSAFE_PUBLICATION_TREE",
        )
    if _MANIFEST_NAME not in relative_files:
        return RecoveryInspection(
            root_id=root_id,
            kind=RecoveryArtifactKind.CANONICAL_BATCH,
            state=(
                RecoveryInspectionState.INVALID if published else RecoveryInspectionState.INCOMPLETE
            ),
            relative_directory=relative,
            error_code="MANIFEST_MISSING",
            file_count=len(relative_files),
        )
    try:
        manifest = verify_canonical_batch_directory(
            directory,
            data_root=data_root,
            root_id=root_id,
            expected_manifest=expected_manifest,
            expected_semantics=expected_semantics,
            expected_provider=expected_provider,
            expected_dataset=expected_dataset,
            expected_batch_id=expected_batch_id,
        )
    except (OSError, ValueError, PublicationError):
        return RecoveryInspection(
            root_id=root_id,
            kind=RecoveryArtifactKind.CANONICAL_BATCH,
            state=RecoveryInspectionState.INVALID,
            relative_directory=relative,
            error_code="INTEGRITY_FAILED",
        )
    return RecoveryInspection(
        root_id=root_id,
        kind=RecoveryArtifactKind.CANONICAL_BATCH,
        state=(
            RecoveryInspectionState.CONTENT_VERIFIED_PUBLISHED
            if published and expected_manifest is not None
            else RecoveryInspectionState.COMPLETE
        ),
        relative_directory=relative,
        identity=manifest.canonical_batch_id,
        file_count=len(manifest.files) + 1,
    )


class PublicationRecoveryInspector:
    """Inspect only platform-owned paths; never mutates or catalogs filesystem state."""

    def __init__(self, data_root: PrivateDataRoot) -> None:
        self._data_root = data_root
        self._root_id = data_root.validate().root_id

    @property
    def root_id(self) -> UUID:
        return self._root_id

    def inspect_staging(self) -> tuple[RecoveryInspection, ...]:
        self._data_root.validate(expected_root_id=self._root_id)
        results: list[RecoveryInspection] = []
        for kind, relative_parent in (
            (RecoveryArtifactKind.RAW_ARTIFACT, _RAW_STAGING_PARENT),
            (RecoveryArtifactKind.CANONICAL_BATCH, _CANONICAL_STAGING_PARENT),
        ):
            parent = managed_path(self._data_root, self._root_id, relative_parent)
            if not parent.exists():
                continue
            for candidate in sorted(parent.iterdir(), key=lambda path: path.name):
                relative = candidate.relative_to(self._data_root.root).as_posix()
                try:
                    checked = managed_path(self._data_root, self._root_id, Path(relative))
                    if checked != candidate or not checked.is_dir():
                        raise PublicationError("staging candidate is not a direct directory")
                    result = (
                        _inspect_raw(candidate, relative, self._root_id)
                        if kind is RecoveryArtifactKind.RAW_ARTIFACT
                        else _inspect_canonical(
                            candidate,
                            relative,
                            self._root_id,
                            data_root=self._data_root,
                            published=False,
                        )
                    )
                except (OSError, PrivateDataRootError, PublicationError):
                    result = RecoveryInspection(
                        root_id=self._root_id,
                        kind=kind,
                        state=RecoveryInspectionState.INVALID,
                        relative_directory=relative,
                        error_code="UNSAFE_STAGING_ENTRY",
                    )
                results.append(result)
        self._data_root.validate(expected_root_id=self._root_id)
        return tuple(results)

    def inspect_published_batch(
        self,
        provider: str,
        dataset: str,
        canonical_batch_id: str,
        *,
        expected_semantics: CanonicalBatchExpectation | None = None,
        expected_manifest: CanonicalBatchManifest | None = None,
    ) -> RecoveryInspection:
        provider = safe_partition_value(provider, label="provider")
        dataset = safe_partition_value(dataset, label="dataset")
        if re.fullmatch(r"batch_v1_[0-9a-f]{64}", canonical_batch_id) is None:
            raise PublicationError("canonical batch ID is invalid")
        relative = PurePosixPath(
            "normalized",
            "price_bars",
            f"provider={provider}",
            f"dataset={dataset}",
            "batches",
            f"batch={canonical_batch_id.removeprefix('batch_v1_')[:32]}",
        )
        try:
            target = managed_path(self._data_root, self._root_id, relative)
        except PrivateDataRootError:
            return RecoveryInspection(
                root_id=self._root_id,
                kind=RecoveryArtifactKind.CANONICAL_BATCH,
                state=RecoveryInspectionState.INVALID,
                relative_directory=relative.as_posix(),
                error_code="UNSAFE_PUBLISHED_PATH",
            )
        if not target.exists():
            return RecoveryInspection(
                root_id=self._root_id,
                kind=RecoveryArtifactKind.CANONICAL_BATCH,
                state=RecoveryInspectionState.ABSENT,
                relative_directory=relative.as_posix(),
            )
        result = _inspect_canonical(
            target,
            relative.as_posix(),
            self._root_id,
            data_root=self._data_root,
            published=True,
            expected_semantics=expected_semantics,
            expected_manifest=expected_manifest,
            expected_provider=provider,
            expected_dataset=dataset,
            expected_batch_id=canonical_batch_id,
        )
        return result


__all__ = [
    "PublicationRecoveryInspector",
    "RecoveryArtifactKind",
    "RecoveryInspection",
    "RecoveryInspectionState",
]
