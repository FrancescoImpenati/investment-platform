"""Stable Phase 2 ingestion identities and semantic replay classification.

Identity-bearing values are serialized through one versioned canonical JSON
envelope. Execution IDs and volatile provenance deliberately live outside the
content digests.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.market_time import to_utc
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession

IDENTITY_CANONICALIZATION_VERSION = 1

Sha256Hex = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
PlatformSha256 = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
SafeVersion = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")]
ExactName = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")]
DimensionName = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")]
type DimensionScalar = str | int | bool

_COMPLETE_URL = re.compile(r"[a-z][a-z0-9+.-]*://", re.IGNORECASE)
_SECRET_VALUE = re.compile(
    r"(?:\b(?:basic|bearer)\s+|(?:authorization|api[_-]?key|password|secret|token)\s*[:=])",
    re.IGNORECASE,
)
_UNSAFE_NAME_FRAGMENTS = (
    "authorization",
    "credential",
    "header",
    "password",
    "secret",
    "token",
    "uri",
    "url",
)
_RESERVED_DIMENSION_NAMES = frozenset(
    {
        "adjustment",
        "attempt_id",
        "bar_semantics",
        "batch_id",
        "calendar_snapshot",
        "currency",
        "data_kind",
        "dataset",
        "end",
        "ingested_at",
        "instrument_id",
        "normalizer_version",
        "page_size",
        "provider",
        "provider_identifier",
        "request_id",
        "retrieved_at",
        "retry",
        "run_id",
        "session",
        "start",
        "symbol",
        "ticker",
        "timeframe",
        "validator_version",
    }
)
_MEDIA_TYPE = re.compile(r"^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$")
_CONTENT_ENCODING = re.compile(r"^[a-z0-9][a-z0-9._+-]{0,31}$")
_PAGE_RELATION = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$")


class _FrozenIdentity(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class DataKind(StrEnum):
    """Data kinds implemented by Phase 2 identity contracts."""

    PRICE_BAR = "price_bar"


class BarSemantics(StrEnum):
    """Canonical interpretation of a provider-native OHLCV aggregation."""

    PROVIDER_AGGREGATED_OHLCV = "provider_aggregated_ohlcv"
    CANONICAL_SESSION_OHLCV = "canonical_session_ohlcv"


class ObservationComparison(StrEnum):
    """Result of comparing two canonical observation versions."""

    DIFFERENT_OBSERVATION = "different_observation"
    SEMANTIC_NO_OP = "semantic_no_op"
    REVISION = "revision"


class IdentityDimension(_FrozenIdentity):
    """One safe, immutable additional semantic dimension."""

    name: DimensionName
    value: DimensionScalar

    @model_validator(mode="after")
    def reject_unsafe_dimension(self) -> Self:
        collapsed_name = self.name.replace("_", "")
        if self.name in _RESERVED_DIMENSION_NAMES or any(
            fragment in collapsed_name for fragment in _UNSAFE_NAME_FRAGMENTS
        ):
            raise ValueError(f"unsafe or reserved identity dimension: {self.name!r}")
        if isinstance(self.value, str):
            if not self.value or len(self.value) > 256:
                raise ValueError("string dimension values must contain 1..256 characters")
            _reject_unsafe_text(self.value, field="dimension value")
        elif isinstance(self.value, int) and not -(2**63) <= self.value < 2**63:
            raise ValueError("integer dimension values must fit in signed 64 bits")
        return self


class _SeriesDimensions(_FrozenIdentity):
    provider: ExactName
    dataset: ExactName
    data_kind: DataKind
    timeframe: Timeframe
    session: TradingSession
    adjustment: AdjustmentState
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
    bar_semantics: BarSemantics
    additional_dimensions: tuple[IdentityDimension, ...] = ()

    @field_validator("provider", "dataset", mode="before")
    @classmethod
    def require_canonical_exact_key(cls, value: object) -> object:
        if isinstance(value, str) and (
            value != value.casefold() or any(character.isspace() for character in value)
        ):
            raise ValueError(
                "provider and dataset keys must be lowercase and contain no whitespace"
            )
        return value

    @field_validator("provider", "dataset", mode="after")
    @classmethod
    def validate_exact_names(cls, value: str) -> str:
        _reject_unsafe_text(value, field="provider or dataset")
        return value

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    @field_validator("additional_dimensions", mode="after")
    @classmethod
    def canonicalize_dimensions(
        cls, value: tuple[IdentityDimension, ...]
    ) -> tuple[IdentityDimension, ...]:
        names = [dimension.name for dimension in value]
        if len(names) != len(set(names)):
            raise ValueError("additional dimensions contain duplicate names")
        return tuple(sorted(value, key=lambda dimension: dimension.name))

    def _series_payload(self) -> dict[str, Any]:
        return {
            "additional_dimensions": [
                {"name": dimension.name, "value": dimension.value}
                for dimension in self.additional_dimensions
            ],
            "adjustment": self.adjustment.value,
            "bar_semantics": self.bar_semantics.value,
            "currency": self.currency,
            "data_kind": self.data_kind.value,
            "dataset": self.dataset,
            "provider": self.provider,
            "session": self.session.value,
            "timeframe": self.timeframe.value,
        }


class StreamKey(_SeriesDimensions):
    """Provider-specific logical series keyed by stable instrument UUID."""

    instrument_id: UUID

    @property
    def canonical_json(self) -> str:
        payload = {**self._series_payload(), "instrument_id": str(self.instrument_id)}
        return _canonical_json("stream-key", payload)

    @property
    def stream_hash(self) -> str:
        return _digest(self.canonical_json)

    @property
    def stream_id(self) -> str:
        return f"stream_v1_{self.stream_hash}"


class ProviderInstrumentMapping(_FrozenIdentity):
    """Temporal provider identifier used only to formulate a bounded request."""

    instrument_id: UUID
    provider_identifier: Annotated[str, Field(min_length=1, max_length=128)]

    @field_validator("provider_identifier", mode="after")
    @classmethod
    def validate_provider_identifier(cls, value: str) -> str:
        _reject_unsafe_text(value, field="provider identifier")
        if any(character in value for character in "?#\\\r\n"):
            raise ValueError("provider identifier must be opaque and path/query free")
        return value


class RequestSpecification(_SeriesDimensions):
    """Deterministic logical request over a half-open UTC interval."""

    instrument_mappings: Annotated[tuple[ProviderInstrumentMapping, ...], Field(min_length=1)]
    start: datetime
    end: datetime
    mapping_semantic_version: SafeVersion

    @field_validator("start", "end", mode="after")
    @classmethod
    def normalize_bounds(cls, value: datetime) -> datetime:
        return to_utc(value)

    @field_validator("instrument_mappings", mode="after")
    @classmethod
    def canonicalize_mappings(
        cls, value: tuple[ProviderInstrumentMapping, ...]
    ) -> tuple[ProviderInstrumentMapping, ...]:
        return tuple(
            sorted(
                value,
                key=lambda mapping: (str(mapping.instrument_id), mapping.provider_identifier),
            )
        )

    @model_validator(mode="after")
    def validate_request(self) -> Self:
        if self.end <= self.start:
            raise ValueError("end must be later than start")
        instrument_ids = [mapping.instrument_id for mapping in self.instrument_mappings]
        provider_identifiers = [mapping.provider_identifier for mapping in self.instrument_mappings]
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("instrument mappings contain duplicate instrument IDs")
        if len(provider_identifiers) != len(set(provider_identifiers)):
            raise ValueError("instrument mappings contain duplicate provider identifiers")
        return self

    @property
    def canonical_json(self) -> str:
        payload = {
            **self._series_payload(),
            "end": _canonical_utc(self.end),
            "instrument_mappings": [
                {
                    "instrument_id": str(mapping.instrument_id),
                    "provider_identifier": mapping.provider_identifier,
                }
                for mapping in self.instrument_mappings
            ],
            "mapping_semantic_version": self.mapping_semantic_version,
            "start": _canonical_utc(self.start),
        }
        return _canonical_json("request-specification", payload)

    @property
    def request_spec_hash(self) -> str:
        return _digest(self.canonical_json)

    @property
    def request_spec_id(self) -> str:
        return f"request_spec_v1_{self.request_spec_hash}"

    def stream_keys(self) -> tuple[StreamKey, ...]:
        """Materialize one stable stream key per temporal provider mapping."""

        return tuple(
            StreamKey(
                provider=self.provider,
                dataset=self.dataset,
                data_kind=self.data_kind,
                instrument_id=mapping.instrument_id,
                timeframe=self.timeframe,
                session=self.session,
                adjustment=self.adjustment,
                currency=self.currency,
                bar_semantics=self.bar_semantics,
                additional_dimensions=self.additional_dimensions,
            )
            for mapping in self.instrument_mappings
        )


class RequestInstanceIdentity(_FrozenIdentity):
    """One planned use of a logical request specification."""

    request_instance_id: UUID
    request_spec_hash: Sha256Hex

    @classmethod
    def create(
        cls,
        specification: RequestSpecification,
        *,
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> Self:
        return cls(
            request_instance_id=uuid_factory(),
            request_spec_hash=specification.request_spec_hash,
        )


class AttemptIdentity(_FrozenIdentity):
    """One retryable execution of a request instance."""

    attempt_id: UUID
    request_instance_id: UUID
    attempt_number: Annotated[int, Field(gt=0)]

    @classmethod
    def create(
        cls,
        request_instance: RequestInstanceIdentity,
        *,
        attempt_number: int,
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> Self:
        return cls(
            attempt_id=uuid_factory(),
            request_instance_id=request_instance.request_instance_id,
            attempt_number=attempt_number,
        )


def canonical_page_relation(page_ordinal: int) -> str:
    """Return the deterministic, non-token relation for one response page."""

    if page_ordinal < 0:
        raise ValueError("page_ordinal must be non-negative")
    if page_ordinal == 0:
        return "root"
    return f"after:{page_ordinal - 1}"


class RawArtifactIdentity(_FrozenIdentity):
    """Identity-bearing fields for one immutable provider response page."""

    request_spec_hash: Sha256Hex
    page_ordinal: Annotated[int, Field(ge=0)]
    page_relation: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9._:-]{0,127}$")]
    media_type: str
    content_encoding: str
    content_sha256: Sha256Hex
    byte_count: Annotated[int, Field(ge=0)]

    @field_validator("page_relation", mode="after")
    @classmethod
    def validate_page_relation(cls, value: str) -> str:
        if not _PAGE_RELATION.fullmatch(value):
            raise ValueError("page_relation is not canonical")
        _reject_unsafe_text(value, field="page relation")
        return value

    @model_validator(mode="after")
    def validate_page_chain(self) -> Self:
        expected_relation = canonical_page_relation(self.page_ordinal)
        if self.page_relation != expected_relation:
            raise ValueError(
                "page_relation must be the deterministic relation "
                f"{expected_relation!r} for page_ordinal {self.page_ordinal}"
            )
        return self

    @field_validator("media_type", mode="before")
    @classmethod
    def canonicalize_media_type(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        canonical = value.strip().lower()
        if not _MEDIA_TYPE.fullmatch(canonical):
            raise ValueError("media_type must be a canonical type/subtype without parameters")
        return canonical

    @field_validator("content_encoding", mode="before")
    @classmethod
    def canonicalize_content_encoding(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        canonical = value.strip().lower()
        if not _CONTENT_ENCODING.fullmatch(canonical):
            raise ValueError("content_encoding is not canonical")
        return canonical

    @classmethod
    def from_bytes(
        cls,
        specification: RequestSpecification,
        *,
        page_ordinal: int,
        media_type: str,
        content_encoding: str,
        payload: bytes,
        page_relation: str | None = None,
    ) -> Self:
        """Convenience constructor for an in-memory stored representation."""

        return cls.from_digest(
            request_spec_hash=specification.request_spec_hash,
            page_ordinal=page_ordinal,
            page_relation=page_relation,
            media_type=media_type,
            content_encoding=content_encoding,
            content_sha256=hashlib.sha256(payload).hexdigest(),
            byte_count=len(payload),
        )

    @classmethod
    def from_digest(
        cls,
        *,
        request_spec_hash: str,
        page_ordinal: int,
        media_type: str,
        content_encoding: str,
        content_sha256: str,
        byte_count: int,
        page_relation: str | None = None,
    ) -> Self:
        """Build from streamed-write metadata without retaining payload bytes."""

        return cls(
            request_spec_hash=request_spec_hash,
            page_ordinal=page_ordinal,
            page_relation=(
                canonical_page_relation(page_ordinal) if page_relation is None else page_relation
            ),
            media_type=media_type,
            content_encoding=content_encoding,
            content_sha256=content_sha256,
            byte_count=byte_count,
        )

    @property
    def canonical_json(self) -> str:
        return _canonical_json(
            "raw-artifact",
            {
                "byte_count": self.byte_count,
                "content_encoding": self.content_encoding,
                "content_sha256": self.content_sha256,
                "media_type": self.media_type,
                "page_ordinal": self.page_ordinal,
                "page_relation": self.page_relation,
                "request_spec_hash": self.request_spec_hash,
            },
        )

    @property
    def artifact_hash(self) -> str:
        return _digest(self.canonical_json)

    @property
    def page_relation_hash(self) -> str:
        canonical = _canonical_json(
            "raw-page-relation",
            {"page_relation": self.page_relation},
        )
        return _digest(canonical)

    @property
    def artifact_id(self) -> str:
        return f"raw_v1_{self.artifact_hash}"


class ProcessingSignature(_FrozenIdentity):
    """Versions and dimensions that determine canonical processing semantics."""

    canonical_schema_version: SafeVersion
    normalizer_version: SafeVersion
    validator_version: SafeVersion
    calendar_snapshot_checksum: PlatformSha256
    process_semantics: tuple[IdentityDimension, ...] = ()

    @field_validator("process_semantics", mode="after")
    @classmethod
    def canonicalize_semantics(
        cls, value: tuple[IdentityDimension, ...]
    ) -> tuple[IdentityDimension, ...]:
        names = [dimension.name for dimension in value]
        if len(names) != len(set(names)):
            raise ValueError("process semantics contain duplicate names")
        return tuple(sorted(value, key=lambda dimension: dimension.name))

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "calendar_snapshot_checksum": self.calendar_snapshot_checksum,
            "canonical_schema_version": self.canonical_schema_version,
            "normalizer_version": self.normalizer_version,
            "process_semantics": [
                {"name": dimension.name, "value": dimension.value}
                for dimension in self.process_semantics
            ],
            "validator_version": self.validator_version,
        }


class CanonicalBatchIdentity(_FrozenIdentity):
    """Content-derived identity of one atomic canonical publication unit."""

    request_spec_hash: Sha256Hex
    ordered_artifacts: Annotated[tuple[RawArtifactIdentity, ...], Field(min_length=1)]
    processing_signature: ProcessingSignature

    @model_validator(mode="after")
    def validate_artifacts(self) -> Self:
        if any(
            artifact.request_spec_hash != self.request_spec_hash
            for artifact in self.ordered_artifacts
        ):
            raise ValueError("all raw artifacts must belong to request_spec_hash")
        artifact_ids = [artifact.artifact_id for artifact in self.ordered_artifacts]
        if len(artifact_ids) != len(set(artifact_ids)):
            raise ValueError("ordered_artifacts contain duplicate identities")
        ordinals = tuple(artifact.page_ordinal for artifact in self.ordered_artifacts)
        expected_ordinals = tuple(range(len(self.ordered_artifacts)))
        if ordinals != expected_ordinals:
            raise ValueError(
                "ordered_artifacts must have contiguous page ordinals starting at zero"
            )
        if any(
            artifact.page_relation != canonical_page_relation(artifact.page_ordinal)
            for artifact in self.ordered_artifacts
        ):
            raise ValueError("ordered_artifacts contain an incoherent page relation")
        return self

    @property
    def artifact_ids(self) -> tuple[str, ...]:
        return tuple(artifact.artifact_id for artifact in self.ordered_artifacts)

    @property
    def ordered_artifacts_hash(self) -> str:
        canonical = _canonical_json("ordered-raw-artifacts", {"artifact_ids": self.artifact_ids})
        return _digest(canonical)

    @property
    def canonical_json(self) -> str:
        return _canonical_json(
            "canonical-batch",
            {
                "ordered_artifact_ids": self.artifact_ids,
                "processing_signature": self.processing_signature.canonical_payload(),
                "request_spec_hash": self.request_spec_hash,
            },
        )

    @property
    def batch_hash(self) -> str:
        return _digest(self.canonical_json)

    @property
    def canonical_batch_id(self) -> str:
        return f"batch_v1_{self.batch_hash}"


class BatchContext(_FrozenIdentity):
    """Durable replay context with fixed manifest and ingestion timestamps."""

    batch_identity: CanonicalBatchIdentity
    fixed_ingested_at: datetime
    manifest_created_at: datetime

    @field_validator("fixed_ingested_at", "manifest_created_at", mode="after")
    @classmethod
    def normalize_timestamps(cls, value: datetime) -> datetime:
        return to_utc(value)

    @model_validator(mode="after")
    def validate_timestamp_order(self) -> Self:
        if self.manifest_created_at < self.fixed_ingested_at:
            raise ValueError("manifest_created_at must not precede fixed_ingested_at")
        return self

    @property
    def canonical_batch_id(self) -> str:
        return self.batch_identity.canonical_batch_id

    @property
    def batch_context_id(self) -> str:
        return f"batch_context_v1_{self.batch_identity.batch_hash}"

    def validate_replay(self, candidate: BatchContext) -> None:
        """Reject replay input that would change content or fixed timestamps."""

        if candidate.batch_identity != self.batch_identity:
            raise ValueError("replay batch identity differs from the persisted context")
        if candidate.fixed_ingested_at != self.fixed_ingested_at:
            raise ValueError("replay must reuse fixed_ingested_at")
        if candidate.manifest_created_at != self.manifest_created_at:
            raise ValueError("replay must reuse manifest_created_at")


class ObservationIdentity(_FrozenIdentity):
    """Canonical observation key within one exact stream."""

    stream: StreamKey
    start: datetime
    end: datetime

    @field_validator("start", "end", mode="after")
    @classmethod
    def normalize_bounds(cls, value: datetime) -> datetime:
        return to_utc(value)

    @model_validator(mode="after")
    def validate_interval(self) -> Self:
        if self.end <= self.start:
            raise ValueError("end must be later than start")
        return self

    @property
    def canonical_json(self) -> str:
        return _canonical_json(
            "observation",
            {
                "end": _canonical_utc(self.end),
                "start": _canonical_utc(self.start),
                "stream_id": self.stream.stream_id,
            },
        )

    @property
    def observation_hash(self) -> str:
        return _digest(self.canonical_json)

    @property
    def observation_id(self) -> str:
        return f"observation_v1_{self.observation_hash}"


class PriceBarSemanticValue(_FrozenIdentity):
    """Normalized bar values retained in the semantic value fingerprint."""

    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    vwap: float | None = None
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")] | None = None
    available_at: datetime | None = None
    quality_flags: tuple[Annotated[str, Field(min_length=1, max_length=64)], ...] = ()

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    @field_validator("available_at", mode="after")
    @classmethod
    def normalize_available_at(cls, value: datetime | None) -> datetime | None:
        return to_utc(value) if value is not None else None

    @field_validator("quality_flags", mode="after")
    @classmethod
    def canonicalize_quality_flags(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("quality_flags contain duplicates")
        return tuple(sorted(value))

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "available_at": (
                _canonical_utc(self.available_at) if self.available_at is not None else None
            ),
            "close": _canonical_number(self.close),
            "currency": self.currency,
            "high": _canonical_number(self.high),
            "low": _canonical_number(self.low),
            "open": _canonical_number(self.open),
            "quality_flags": self.quality_flags,
            "volume": _canonical_number(self.volume) if self.volume is not None else None,
            "vwap": _canonical_number(self.vwap) if self.vwap is not None else None,
        }


def semantic_value_fingerprint(
    value: PriceBarSemanticValue,
    processing_signature: ProcessingSignature,
) -> str:
    """Hash semantic values and code semantics, excluding volatile provenance."""

    canonical = _canonical_json(
        "observation-value",
        {
            "processing_signature": processing_signature.canonical_payload(),
            "semantic_value": value.canonical_payload(),
        },
    )
    return _digest(canonical)


class SemanticObservation(_FrozenIdentity):
    """Observation key paired with its validated semantic value fingerprint."""

    identity: ObservationIdentity
    value_fingerprint: Sha256Hex

    @classmethod
    def create(
        cls,
        identity: ObservationIdentity,
        value: PriceBarSemanticValue,
        processing_signature: ProcessingSignature,
    ) -> Self:
        return cls(
            identity=identity,
            value_fingerprint=semantic_value_fingerprint(value, processing_signature),
        )

    def compare(self, candidate: SemanticObservation) -> ObservationComparison:
        """Classify a candidate as unrelated, an exact no-op, or a revision."""

        if candidate.identity != self.identity:
            return ObservationComparison.DIFFERENT_OBSERVATION
        if candidate.value_fingerprint == self.value_fingerprint:
            return ObservationComparison.SEMANTIC_NO_OP
        return ObservationComparison.REVISION


def _canonical_json(kind: str, payload: dict[str, Any]) -> str:
    envelope = {
        "canonicalization_version": IDENTITY_CANONICALIZATION_VERSION,
        "kind": kind,
        "payload": payload,
    }
    return json.dumps(envelope, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _digest(canonical_json: str) -> str:
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _canonical_utc(value: datetime) -> str:
    return to_utc(value).isoformat().replace("+00:00", "Z")


def _canonical_number(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("semantic numbers must be finite")
    decimal = Decimal(str(value))
    if decimal.is_zero():
        return "0"
    normalized = decimal.normalize()
    return format(normalized, "f")


def _reject_unsafe_text(value: str, *, field: str) -> None:
    if _COMPLETE_URL.search(value) or _SECRET_VALUE.search(value) or "\r" in value or "\n" in value:
        raise ValueError(f"{field} contains a URL, secret, or volatile metadata")


__all__ = [
    "IDENTITY_CANONICALIZATION_VERSION",
    "AttemptIdentity",
    "BarSemantics",
    "BatchContext",
    "CanonicalBatchIdentity",
    "DataKind",
    "IdentityDimension",
    "ObservationComparison",
    "ObservationIdentity",
    "PriceBarSemanticValue",
    "ProcessingSignature",
    "ProviderInstrumentMapping",
    "RawArtifactIdentity",
    "RequestInstanceIdentity",
    "RequestSpecification",
    "SemanticObservation",
    "StreamKey",
    "canonical_page_relation",
    "semantic_value_fingerprint",
]
