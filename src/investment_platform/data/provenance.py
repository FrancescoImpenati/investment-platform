"""Data-source, immutable raw-batch metadata, and payload boundaries."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from io import BytesIO
from typing import Annotated, BinaryIO, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from investment_platform._immutable import FrozenMapping
from investment_platform.data.market_time import to_utc

type JsonScalar = str | int | float | bool | None
NonEmptyStr = Annotated[str, Field(min_length=1)]

_SAFE_EXTENSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")
_SENSITIVE_KEY_PARTS = frozenset(
    {
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "header",
        "headers",
        "key",
        "password",
        "secret",
        "token",
        "uri",
        "url",
    }
)
_SENSITIVE_KEY_FRAGMENTS = (
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "header",
    "password",
    "secret",
    "token",
    "uri",
    "url",
)
_COMPLETE_URL = re.compile(r"[a-z][a-z0-9+.-]*://", re.IGNORECASE)
_SECRET_VALUE = re.compile(
    r"(?:\b(?:basic|bearer)\s+|(?:authorization|api[_-]?key|access[_-]?token|token|password|secret)\s*[:=])",
    re.IGNORECASE,
)


class _FrozenMetadata(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class LicenseClassification(StrEnum):
    """Redistribution classification for data obtained from a source."""

    PRIVATE = "private"
    REDISTRIBUTABLE = "redistributable"
    SAMPLE = "sample"
    SYNTHETIC = "synthetic"


class DataSource(_FrozenMetadata):
    """Stable identity and safe descriptive metadata for a provider dataset."""

    source_id: UUID
    provider: NonEmptyStr
    dataset: NonEmptyStr
    logical_endpoint: NonEmptyStr
    license_classification: LicenseClassification = LicenseClassification.PRIVATE

    @field_validator("logical_endpoint", mode="after")
    @classmethod
    def reject_query_strings(cls, value: str) -> str:
        if "://" in value or "?" in value or "#" in value:
            raise ValueError(
                "logical_endpoint must be a logical path, not a complete or query-bearing URL"
            )
        return value


class RawBatchMetadata(_FrozenMetadata):
    """Serializable metadata for a captured provider-native payload."""

    batch_id: UUID
    source: DataSource
    retrieved_at: datetime
    media_type: NonEmptyStr
    file_extension: NonEmptyStr
    provider_request_id: NonEmptyStr | None = None
    request_metadata: Mapping[str, JsonScalar] = Field(default_factory=dict)

    @field_validator("retrieved_at", mode="after")
    @classmethod
    def normalize_retrieved_at(cls, value: datetime) -> datetime:
        return to_utc(value)

    @field_validator("file_extension", mode="after")
    @classmethod
    def validate_file_extension(cls, value: str) -> str:
        if not _SAFE_EXTENSION.fullmatch(value) or ".." in value:
            raise ValueError("file_extension must be a safe extension without path components")
        return value.lower()

    @field_validator("provider_request_id", mode="after")
    @classmethod
    def reject_unsafe_provider_request_id(cls, value: str | None) -> str | None:
        if value is not None and (
            _COMPLETE_URL.search(value)
            or _SECRET_VALUE.search(value)
            or "?" in value
            or "#" in value
            or "\n" in value
            or "\r" in value
        ):
            raise ValueError("provider_request_id must be an opaque identifier, not a URL")
        return value

    @field_validator("request_metadata", mode="after")
    @classmethod
    def reject_sensitive_metadata(cls, value: Mapping[str, JsonScalar]) -> Mapping[str, JsonScalar]:
        for key, metadata_value in value.items():
            key_parts = set(re.findall(r"[a-z0-9]+", key.lower()))
            collapsed_key = "".join(re.findall(r"[a-z0-9]+", key.lower()))
            if key_parts & _SENSITIVE_KEY_PARTS or any(
                fragment in collapsed_key for fragment in _SENSITIVE_KEY_FRAGMENTS
            ):
                raise ValueError(f"request metadata contains a sensitive key: {key!r}")
            if isinstance(metadata_value, str) and (
                _COMPLETE_URL.search(metadata_value) or _SECRET_VALUE.search(metadata_value)
            ):
                raise ValueError("request metadata must not contain complete URLs or secrets")
        return FrozenMapping(value)

    @field_serializer("request_metadata")
    def serialize_request_metadata(self, value: Mapping[str, JsonScalar]) -> dict[str, JsonScalar]:
        return dict(value)


@runtime_checkable
class RawPayload(Protocol):
    """A reopenable provider-native payload resource."""

    def open_binary(self) -> AbstractContextManager[BinaryIO]:
        """Open a binary reader owned by the returned context manager."""

        ...


@contextmanager
def _open_bytes(payload: bytes) -> Iterator[BinaryIO]:
    reader = BytesIO(payload)
    try:
        yield reader
    finally:
        reader.close()


@dataclass(frozen=True, slots=True)
class BytesRawPayload:
    """Small in-memory payload adapter for fixtures and mock provider pages."""

    content: bytes

    def open_binary(self) -> AbstractContextManager[BinaryIO]:
        return _open_bytes(self.content)


@dataclass(frozen=True, slots=True)
class RawBatch:
    """A provider page combining serializable metadata with a payload resource."""

    metadata: RawBatchMetadata
    payload: RawPayload

    def __post_init__(self) -> None:
        if not isinstance(self.payload, RawPayload):
            raise TypeError("payload must implement RawPayload")
