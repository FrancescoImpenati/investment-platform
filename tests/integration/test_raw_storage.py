"""Integration tests for immutable, chunked raw artifact persistence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

import pytest

from investment_platform.data.provenance import (
    BytesRawPayload,
    DataSource,
    LicenseClassification,
    RawBatch,
    RawBatchMetadata,
)
from investment_platform.data.storage.raw import (
    BatchCollisionError,
    RawBatchStore,
    RawStorageError,
    replay_raw_artifact,
)


def _metadata() -> RawBatchMetadata:
    return RawBatchMetadata(
        batch_id=uuid4(),
        source=DataSource(
            source_id=uuid4(),
            provider="Example Provider",
            dataset="Daily Bars",
            logical_endpoint="aggregates/daily",
            license_classification=LicenseClassification.SYNTHETIC,
        ),
        retrieved_at=datetime(2026, 1, 5, 22, 0, tzinfo=UTC),
        media_type="application/json",
        file_extension="json.gz",
        provider_request_id="request-17",
        request_metadata={"symbol_count": 2, "adjusted": False},
    )


class _BoundedReader(BytesIO):
    def __init__(self, content: bytes, maximum: int) -> None:
        super().__init__(content)
        self._maximum = maximum

    def read(self, size: int | None = -1) -> bytes:
        if size is None or size < 0 or size > self._maximum:
            raise AssertionError("storage attempted an unbounded read")
        return super().read(size)


class _BoundedPayload:
    def __init__(self, content: bytes, maximum: int) -> None:
        self._content = content
        self._maximum = maximum

    @contextmanager
    def _open(self) -> Iterator[BinaryIO]:
        reader = _BoundedReader(self._content, self._maximum)
        try:
            yield reader
        finally:
            reader.close()

    def open_binary(self) -> AbstractContextManager[BinaryIO]:
        return self._open()


@pytest.mark.integration
def test_raw_store_streams_checksums_and_publishes_a_sanitized_manifest(tmp_path: Path) -> None:
    content = b'{"bars":[1,2,3]}'
    metadata = _metadata()
    store = RawBatchStore(tmp_path / "raw", chunk_size=4)

    artifact = store.write(RawBatch(metadata=metadata, payload=_BoundedPayload(content, maximum=4)))

    assert artifact.created is True
    assert artifact.payload_path.read_bytes() == content
    assert artifact.sha256 == hashlib.sha256(content).hexdigest()
    assert artifact.size_bytes == len(content)
    assert artifact.directory == (
        tmp_path
        / "raw"
        / "provider=example-provider"
        / "dataset=daily-bars"
        / "retrieval_date=2026-01-05"
        / f"batch_id={metadata.batch_id}"
    )
    manifest = json.loads(artifact.manifest_path.read_text(encoding="utf-8"))
    assert manifest["payload"] == {
        "filename": "payload.json.gz",
        "sha256": artifact.sha256,
        "size_bytes": len(content),
    }
    assert manifest["source"]["logical_endpoint"] == "aggregates/daily"
    assert "authorization" not in artifact.manifest_path.read_text(encoding="utf-8").lower()
    assert not list(artifact.directory.parent.glob(".batch_id=*-*"))


@pytest.mark.integration
def test_published_raw_artifact_can_be_integrity_checked_and_reopened(tmp_path: Path) -> None:
    content = b'{"provider_native":true}'
    metadata = _metadata()
    store = RawBatchStore(tmp_path / "raw", chunk_size=4)
    artifact = store.write(RawBatch(metadata=metadata, payload=BytesRawPayload(content)))

    replayed = replay_raw_artifact(artifact, chunk_size=3)

    assert replayed.metadata == metadata
    with replayed.payload.open_binary() as reader:
        assert reader.read() == content

    artifact.payload_path.write_bytes(b"tampered")
    with (
        pytest.raises(RawStorageError, match="integrity check"),
        replayed.payload.open_binary() as reader,
    ):
        reader.read()
    with pytest.raises(RawStorageError, match="integrity check"):
        replay_raw_artifact(artifact, chunk_size=3)


@pytest.mark.integration
def test_identical_raw_replay_is_a_noop_but_different_content_collides(tmp_path: Path) -> None:
    metadata = _metadata()
    content = b"same provider payload"
    store = RawBatchStore(tmp_path / "raw", chunk_size=5)

    created = store.write(RawBatch(metadata=metadata, payload=BytesRawPayload(content)))
    replayed = store.write(RawBatch(metadata=metadata, payload=BytesRawPayload(content)))

    assert created.created is True
    assert replayed.created is False
    assert replayed.directory == created.directory
    assert replayed.sha256 == created.sha256
    assert len(list((tmp_path / "raw").rglob("payload.*"))) == 1

    with pytest.raises(BatchCollisionError, match="different metadata or content"):
        store.write(RawBatch(metadata=metadata, payload=BytesRawPayload(b"changed")))

    assert created.payload_path.read_bytes() == content
    assert not list(created.directory.parent.glob(".batch_id=*-*"))


@pytest.mark.integration
def test_same_raw_batch_id_with_changed_metadata_is_a_collision(tmp_path: Path) -> None:
    metadata = _metadata()
    content = b"provider payload"
    store = RawBatchStore(tmp_path / "raw")
    store.write(RawBatch(metadata=metadata, payload=BytesRawPayload(content)))
    changed = metadata.model_copy(update={"provider_request_id": "different-request"})

    with pytest.raises(BatchCollisionError, match="different metadata or content"):
        store.write(RawBatch(metadata=changed, payload=BytesRawPayload(content)))


@pytest.mark.integration
@pytest.mark.parametrize(
    "unsafe_request_metadata",
    [
        {"apiKey": "must-not-persist"},
        {"cursor": "prefix Bearer must-not-persist"},
        {"cursor": "next=https://provider.test/page?token=must-not-persist"},
    ],
)
def test_raw_store_revalidates_metadata_before_manifest_persistence(
    tmp_path: Path,
    unsafe_request_metadata: dict[str, object],
) -> None:
    metadata = _metadata()
    unsafe_metadata = metadata.model_copy(update={"request_metadata": unsafe_request_metadata})

    with pytest.raises(RawStorageError, match="safety revalidation"):
        RawBatchStore(tmp_path / "raw").write(
            RawBatch(metadata=unsafe_metadata, payload=BytesRawPayload(b"payload"))
        )

    assert not list((tmp_path / "raw").rglob("manifest.json"))


@pytest.mark.integration
def test_same_raw_batch_id_cannot_escape_collision_by_changing_its_layout_metadata(
    tmp_path: Path,
) -> None:
    metadata = _metadata()
    content = b"provider payload"
    store = RawBatchStore(tmp_path / "raw")
    created = store.write(RawBatch(metadata=metadata, payload=BytesRawPayload(content)))
    changed_source = metadata.model_copy(
        update={"source": metadata.source.model_copy(update={"dataset": "Other Dataset"})}
    )
    changed_date = metadata.model_copy(
        update={"retrieved_at": datetime(2026, 1, 6, 22, 0, tzinfo=UTC)}
    )

    for changed in (changed_source, changed_date):
        with pytest.raises(BatchCollisionError, match="different metadata"):
            store.write(RawBatch(metadata=changed, payload=BytesRawPayload(content)))

    assert len(list((tmp_path / "raw").rglob(f"batch_id={metadata.batch_id}"))) == 1
    assert created.payload_path.read_bytes() == content
