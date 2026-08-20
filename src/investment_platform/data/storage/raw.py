"""Write-once storage for exact provider-native payloads."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import unicodedata
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Final, cast
from uuid import UUID

from pydantic import ValidationError

from investment_platform.data.provenance import RawBatch, RawBatchMetadata

_MANIFEST_NAME: Final = "manifest.json"
_DEFAULT_CHUNK_SIZE: Final = 1024 * 1024
_SAFE_EXTENSION: Final = re.compile(r"^[a-z0-9][a-z0-9._-]{0,31}$")
_UNSAFE_METADATA_KEY: Final = re.compile(
    r"(?:authorization|credential|password|secret|token|api[_-]?key|cookie)",
    re.IGNORECASE,
)
_WINDOWS_RESERVED_NAMES: Final = {
    "aux",
    "com1",
    "com2",
    "com3",
    "com4",
    "com5",
    "com6",
    "com7",
    "com8",
    "com9",
    "con",
    "lpt1",
    "lpt2",
    "lpt3",
    "lpt4",
    "lpt5",
    "lpt6",
    "lpt7",
    "lpt8",
    "lpt9",
    "nul",
    "prn",
}


class RawStorageError(RuntimeError):
    """Base error for immutable raw storage."""


class BatchCollisionError(RawStorageError):
    """Raised when an existing batch ID is replayed with different content or metadata."""


@dataclass(frozen=True, slots=True)
class RawArtifact:
    """Published raw payload plus its integrity metadata."""

    batch_id: UUID
    directory: Path
    payload_path: Path
    manifest_path: Path
    sha256: str
    size_bytes: int
    created: bool


@dataclass(frozen=True, slots=True)
class _IntegrityCheckedFilePayload:
    """Snapshot and verify persisted bytes before exposing them to normalization."""

    path: Path
    sha256: str
    size_bytes: int
    chunk_size: int

    @contextmanager
    def _open(self) -> Iterator[BinaryIO]:
        digest = hashlib.sha256()
        size = 0
        with tempfile.TemporaryFile(mode="w+b") as temporary_snapshot:
            snapshot = cast(BinaryIO, temporary_snapshot)
            try:
                with self.path.open("rb") as reader:
                    while chunk := reader.read(self.chunk_size):
                        digest.update(chunk)
                        size += len(chunk)
                        snapshot.write(chunk)
            except OSError as error:
                raise RawStorageError("raw artifact payload is missing or unreadable") from error
            if digest.hexdigest() != self.sha256 or size != self.size_bytes:
                raise RawStorageError("raw artifact payload failed its integrity check")
            snapshot.seek(0)
            yield snapshot

    def open_binary(self) -> AbstractContextManager[BinaryIO]:
        return self._open()


def replay_raw_artifact(
    artifact: RawArtifact,
    *,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
) -> RawBatch:
    """Integrity-check a published artifact and reopen it for downstream normalization."""

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if artifact.manifest_path != artifact.directory / _MANIFEST_NAME:
        raise RawStorageError("raw artifact manifest path is inconsistent with its directory")
    if artifact.payload_path.parent != artifact.directory:
        raise RawStorageError("raw artifact payload path is inconsistent with its directory")
    try:
        manifest_value = json.loads(artifact.manifest_path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RawStorageError("raw artifact manifest is missing or invalid") from error
    if not isinstance(manifest_value, dict):
        raise RawStorageError("raw artifact manifest must be a JSON object")
    payload_value = manifest_value.get("payload")
    if not isinstance(payload_value, dict):
        raise RawStorageError("raw artifact manifest payload metadata is invalid")
    payload_name = payload_value.get("filename")
    manifest_sha256 = payload_value.get("sha256")
    manifest_size = payload_value.get("size_bytes")
    if (
        payload_name != artifact.payload_path.name
        or manifest_sha256 != artifact.sha256
        or manifest_size != artifact.size_bytes
    ):
        raise RawStorageError("raw artifact fields disagree with the persisted manifest")
    metadata_fields = {
        key: manifest_value.get(key)
        for key in (
            "batch_id",
            "source",
            "retrieved_at",
            "media_type",
            "file_extension",
            "provider_request_id",
            "request_metadata",
        )
    }
    try:
        metadata = RawBatchMetadata.model_validate(metadata_fields)
    except ValidationError as error:
        raise RawStorageError("raw artifact metadata failed validation") from error
    if metadata.batch_id != artifact.batch_id:
        raise RawStorageError("raw artifact batch ID disagrees with the manifest")
    try:
        actual_sha256, actual_size = _file_integrity(
            artifact.payload_path,
            chunk_size=chunk_size,
        )
    except OSError as error:
        raise RawStorageError("raw artifact payload is missing or unreadable") from error
    if actual_sha256 != artifact.sha256 or actual_size != artifact.size_bytes:
        raise RawStorageError("raw artifact payload failed its integrity check")
    return RawBatch(
        metadata=metadata,
        payload=_IntegrityCheckedFilePayload(
            path=artifact.payload_path,
            sha256=artifact.sha256,
            size_bytes=artifact.size_bytes,
            chunk_size=chunk_size,
        ),
    )


def _path_slug(value: str, *, field_name: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", normalized).strip("-_").lower()
    if not slug:
        raise RawStorageError(f"{field_name} cannot produce a safe path segment")
    if slug in _WINDOWS_RESERVED_NAMES:
        slug = f"_{slug}"
    return slug


def _json_bytes(value: object) -> bytes:
    rendered = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"{rendered}\n".encode()


def _assert_sanitized_metadata(metadata: RawBatchMetadata) -> None:
    unsafe_keys = sorted(
        key for key in metadata.request_metadata if _UNSAFE_METADATA_KEY.search(key)
    )
    if unsafe_keys:
        raise RawStorageError(f"request metadata contains sensitive keys: {unsafe_keys}")
    endpoint = metadata.source.logical_endpoint
    if "://" in endpoint or "?" in endpoint or "#" in endpoint:
        raise RawStorageError("logical_endpoint must not be a complete or query-bearing URL")


def _manifest(
    metadata: RawBatchMetadata,
    *,
    payload_name: str,
    sha256: str,
    size_bytes: int,
) -> dict[str, object]:
    dumped = metadata.model_dump(mode="json")
    return {
        "schema_version": 1,
        "batch_id": dumped["batch_id"],
        "source": dumped["source"],
        "retrieved_at": dumped["retrieved_at"],
        "media_type": dumped["media_type"],
        "file_extension": dumped["file_extension"],
        "provider_request_id": dumped["provider_request_id"],
        "request_metadata": dumped["request_metadata"],
        "payload": {
            "filename": payload_name,
            "sha256": sha256,
            "size_bytes": size_bytes,
        },
    }


def _file_integrity(path: Path, *, chunk_size: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as reader:
        while chunk := reader.read(chunk_size):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


class RawBatchStore:
    """Persist complete raw batches atomically without overwriting existing evidence."""

    def __init__(self, root: Path, *, chunk_size: int = _DEFAULT_CHUNK_SIZE) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self._root = Path(root)
        self._chunk_size = chunk_size

    @property
    def root(self) -> Path:
        return self._root

    def _target_directory(self, metadata: RawBatchMetadata) -> Path:
        source = metadata.source
        return (
            self._root
            / f"provider={_path_slug(source.provider, field_name='provider')}"
            / f"dataset={_path_slug(source.dataset, field_name='dataset')}"
            / f"retrieval_date={metadata.retrieved_at.date().isoformat()}"
            / f"batch_id={metadata.batch_id}"
        )

    def _existing_artifact(
        self,
        *,
        metadata: RawBatchMetadata,
        target: Path,
        payload_name: str,
        candidate_manifest: bytes,
        candidate_sha256: str,
        candidate_size: int,
    ) -> RawArtifact:
        manifest_path = target / _MANIFEST_NAME
        payload_path = target / payload_name
        try:
            existing_manifest = manifest_path.read_bytes()
            existing_sha256, existing_size = _file_integrity(
                payload_path,
                chunk_size=self._chunk_size,
            )
        except OSError as error:
            raise BatchCollisionError(
                f"raw batch {metadata.batch_id} exists but is incomplete or unreadable"
            ) from error

        if (
            existing_manifest != candidate_manifest
            or existing_sha256 != candidate_sha256
            or existing_size != candidate_size
        ):
            raise BatchCollisionError(
                f"raw batch {metadata.batch_id} already exists with different metadata or content"
            )

        return RawArtifact(
            batch_id=metadata.batch_id,
            directory=target,
            payload_path=payload_path,
            manifest_path=manifest_path,
            sha256=existing_sha256,
            size_bytes=existing_size,
            created=False,
        )

    def write(self, batch: RawBatch) -> RawArtifact:
        """Consume a bounded-read payload, then atomically publish or verify its replay."""

        try:
            metadata = RawBatchMetadata.model_validate(batch.metadata.model_dump(mode="python"))
        except ValidationError as error:
            raise RawStorageError("raw batch metadata failed safety revalidation") from error
        _assert_sanitized_metadata(metadata)
        extension = metadata.file_extension.lower()
        if not _SAFE_EXTENSION.fullmatch(extension) or ".." in extension:
            raise RawStorageError("file_extension is not a safe provider-native extension")

        target = self._target_directory(metadata)
        if self._root.exists():
            existing_locations = [
                path for path in self._root.rglob(f"batch_id={metadata.batch_id}") if path.is_dir()
            ]
            foreign_locations = [path for path in existing_locations if path != target]
            if foreign_locations:
                raise BatchCollisionError(
                    f"raw batch {metadata.batch_id} already exists under different metadata"
                )
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(
            tempfile.mkdtemp(
                prefix=f".batch_id={metadata.batch_id}-",
                dir=target.parent,
            )
        )
        payload_name = f"payload.{extension}"
        temporary_payload = temporary / payload_name
        digest = hashlib.sha256()
        size = 0

        try:
            with batch.payload.open_binary() as reader, temporary_payload.open("xb") as writer:
                while chunk := reader.read(self._chunk_size):
                    if not isinstance(chunk, bytes):
                        raise RawStorageError("raw payload readers must return bytes")
                    if len(chunk) > self._chunk_size:
                        raise RawStorageError(
                            "raw payload reader exceeded the requested chunk size"
                        )
                    writer.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)

            sha256 = digest.hexdigest()
            manifest = _manifest(
                metadata,
                payload_name=payload_name,
                sha256=sha256,
                size_bytes=size,
            )
            manifest_bytes = _json_bytes(manifest)
            (temporary / _MANIFEST_NAME).write_bytes(manifest_bytes)

            if target.exists():
                return self._existing_artifact(
                    metadata=metadata,
                    target=target,
                    payload_name=payload_name,
                    candidate_manifest=manifest_bytes,
                    candidate_sha256=sha256,
                    candidate_size=size,
                )

            try:
                os.rename(temporary, target)
            except OSError:
                if not target.exists():
                    raise
                return self._existing_artifact(
                    metadata=metadata,
                    target=target,
                    payload_name=payload_name,
                    candidate_manifest=manifest_bytes,
                    candidate_sha256=sha256,
                    candidate_size=size,
                )

            return RawArtifact(
                batch_id=metadata.batch_id,
                directory=target,
                payload_path=target / payload_name,
                manifest_path=target / _MANIFEST_NAME,
                sha256=sha256,
                size_bytes=size,
                created=True,
            )
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)


__all__ = [
    "BatchCollisionError",
    "RawArtifact",
    "RawBatchStore",
    "RawStorageError",
    "replay_raw_artifact",
]
