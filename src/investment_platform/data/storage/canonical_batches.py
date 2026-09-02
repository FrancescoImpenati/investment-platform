"""Atomic batch-oriented canonical Parquet publication for living ingestion."""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Annotated, Final, Self
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.calendar import CalendarSnapshot, ExpectedCalendarSlot
from investment_platform.data.ingestion.identity import (
    BatchContext,
    CanonicalBatchIdentity,
    ProcessingSignature,
    RawArtifactIdentity,
    RequestSpecification,
    StreamKey,
)
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    AuthorizedRawArtifactDescriptor,
    DatasetPolicyDenied,
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
    ensure_direct_subdirectory,
    ensure_directory,
    file_integrity,
    fsync_directory,
    invoke_fault,
    iter_safe_regular_files,
    json_bytes,
    managed_path,
    remove_owned_staging_directory,
    safe_partition_value,
    safe_relative_file,
    write_file_durably,
)
from investment_platform.data.storage.living_raw import (
    raw_artifact_relative_directory,
    verify_raw_artifact_directory,
)
from investment_platform.data.storage.market_bars import PRICE_BAR_SCHEMA
from investment_platform.data.validation.bars import validate_bars
from investment_platform.data_root import PrivateDataRoot

_MANIFEST_NAME: Final = "manifest.json"
_CANONICAL_STAGING_PARENT: Final = PurePosixPath("staging/canonical-batches")
_TIMESTAMP_START: Final = "timestamp_start"
_TIMESTAMP_END: Final = "timestamp_end"
_INGESTED_AT: Final = "ingested_at"
_PARTITIONED_PARQUET = re.compile(
    r"^timeframe=(?P<timeframe>[a-z0-9]+)/(?:year=)(?P<year>[0-9]{4})/"
    r"(?:month=)(?P<month>0[1-9]|1[0-2])/part-(?P<part>[0-9]{4,})\.parquet$"
)
Sha256Hex = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class _FrozenPublicationModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class StreamPublicationOutcome(StrEnum):
    PUBLISHABLE = "PUBLISHABLE"
    BLOCKED = "BLOCKED"


class CanonicalStreamOutcome(_FrozenPublicationModel):
    """Sanitized per-stream publication result embedded in the immutable manifest."""

    stream: StreamKey
    outcome: StreamPublicationOutcome
    request_start: datetime
    request_end: datetime
    row_count: Annotated[int, Field(ge=0)]
    observed_start: datetime | None = None
    observed_end: datetime | None = None
    validation_codes: tuple[str, ...] = ()
    semantic_duplicate_count: Annotated[int, Field(ge=0)] = 0
    revision_count: Annotated[int, Field(ge=0)] = 0

    @property
    def stream_id(self) -> str:
        return self.stream.stream_id

    @field_validator(
        "request_start",
        "request_end",
        "observed_start",
        "observed_end",
        mode="after",
    )
    @classmethod
    def normalize_times(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("stream outcome timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("validation_codes", mode="after")
    @classmethod
    def validate_codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("validation codes must be unique and sorted")
        if any(
            not code
            or len(code) > 128
            or any(
                character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:-" for character in code
            )
            for code in value
        ):
            raise ValueError("validation codes must be sanitized stable identifiers")
        return value

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.request_end <= self.request_start:
            raise ValueError("stream request bounds must be half-open and non-empty")
        if (self.observed_start is None) != (self.observed_end is None):
            raise ValueError("observed bounds must both be present or absent")
        if self.outcome is StreamPublicationOutcome.PUBLISHABLE:
            if self.row_count <= 0 or self.observed_start is None or self.observed_end is None:
                raise ValueError("a publishable stream requires rows and observed bounds")
            if (
                self.observed_end <= self.observed_start
                or self.observed_start < self.request_start
                or self.observed_end > self.request_end
            ):
                raise ValueError("publishable stream bounds exceed the bounded request")
        else:
            if self.row_count != 0 or self.observed_start is not None:
                raise ValueError("a blocked stream cannot contribute canonical rows")
            if not self.validation_codes:
                raise ValueError("a blocked stream requires a sanitized validation code")
        return self


class RawProvenanceBinding(_FrozenPublicationModel):
    """Current-schema UUID provenance bound to one content-addressed raw page."""

    artifact_id: str = Field(pattern=r"^raw_v1_[0-9a-f]{64}$")
    raw_batch_id: UUID


class CanonicalPublicationProvenance(_FrozenPublicationModel):
    """Exact source and raw UUIDs expected in canonical rows."""

    source_id: UUID
    raw_bindings: Annotated[tuple[RawProvenanceBinding, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def validate_bindings(self) -> Self:
        artifact_ids = tuple(binding.artifact_id for binding in self.raw_bindings)
        raw_batch_ids = tuple(binding.raw_batch_id for binding in self.raw_bindings)
        if len(set(artifact_ids)) != len(artifact_ids):
            raise ValueError("raw provenance contains duplicate artifact identities")
        if len(set(raw_batch_ids)) != len(raw_batch_ids):
            raise ValueError("raw provenance UUIDs must be unique")
        return self


class CanonicalBatchExpectation(_FrozenPublicationModel):
    """Persisted semantic context required before an orphan can be adopted."""

    specification: RequestSpecification
    batch_context: BatchContext
    calendar_snapshot: CalendarSnapshot
    provenance: CanonicalPublicationProvenance
    streams: tuple[CanonicalStreamOutcome, ...]

    @model_validator(mode="after")
    def validate_expectation(self) -> Self:
        if self.batch_context.batch_identity.request_spec_hash != (
            self.specification.request_spec_hash
        ):
            raise ValueError("batch expectation belongs to a different request")
        if (
            self.batch_context.batch_identity.processing_signature.calendar_snapshot_checksum
            != self.calendar_snapshot.checksum
        ):
            raise ValueError("batch expectation has a different calendar snapshot")
        expected_streams = tuple(
            sorted(self.specification.stream_keys(), key=lambda value: value.stream_id)
        )
        supplied_streams = tuple(
            outcome.stream for outcome in sorted(self.streams, key=lambda value: value.stream_id)
        )
        if supplied_streams != expected_streams:
            raise ValueError("batch expectation does not cover every requested stream")
        if any(
            outcome.request_start != self.specification.start
            or outcome.request_end != self.specification.end
            for outcome in self.streams
        ):
            raise ValueError("batch expectation has different request bounds")
        if not _calendar_covers_bounds(
            self.calendar_snapshot,
            self.specification.start,
            self.specification.end,
        ):
            raise ValueError("batch expectation calendar does not cover the request")
        expected_artifacts = self.batch_context.batch_identity.artifact_ids
        if tuple(binding.artifact_id for binding in self.provenance.raw_bindings) != (
            expected_artifacts
        ):
            raise ValueError("batch expectation has different raw provenance")
        return self


@dataclass(frozen=True, slots=True)
class CanonicalParquetPart:
    """One already-normalized in-memory frame and its batch-relative target."""

    relative_path: str
    frame: pl.DataFrame


class CanonicalFileManifest(_FrozenPublicationModel):
    relative_path: str
    sha256: Sha256Hex
    byte_count: Annotated[int, Field(gt=0)]
    row_count: Annotated[int, Field(gt=0)]
    schema_sha256: Sha256Hex
    timestamp_start_min: datetime
    timestamp_end_max: datetime

    @field_validator("relative_path", mode="after")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return safe_relative_file(value, suffix=".parquet").as_posix()

    @field_validator("timestamp_start_min", "timestamp_end_max", mode="after")
    @classmethod
    def normalize_times(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical file bounds must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.timestamp_end_max <= self.timestamp_start_min:
            raise ValueError("canonical file temporal bounds are invalid")
        return self


class CanonicalBatchManifest(_FrozenPublicationModel):
    """Deterministic completion record written last in canonical staging."""

    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    canonical_batch_id: str = Field(pattern=r"^batch_v1_[0-9a-f]{64}$")
    batch_context_id: str = Field(pattern=r"^batch_context_v1_[0-9a-f]{64}$")
    provider: str
    dataset: str
    request_spec_hash: Sha256Hex
    ordered_raw_artifacts: tuple[RawArtifactIdentity, ...]
    processing_signature: ProcessingSignature
    provenance: CanonicalPublicationProvenance
    calendar_snapshot: CalendarSnapshot
    eligible_slots: tuple[ExpectedCalendarSlot, ...]
    fixed_ingested_at: datetime
    manifest_created_at: datetime
    files: tuple[CanonicalFileManifest, ...]
    streams: tuple[CanonicalStreamOutcome, ...]
    row_count: Annotated[int, Field(gt=0)]

    @field_validator("fixed_ingested_at", "manifest_created_at", mode="after")
    @classmethod
    def normalize_times(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("batch manifest timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        safe_partition_value(self.provider, label="provider")
        safe_partition_value(self.dataset, label="dataset")
        if not self.files:
            raise ValueError("canonical batch must contain at least one Parquet file")
        file_paths = tuple(file.relative_path for file in self.files)
        if file_paths != tuple(sorted(set(file_paths))):
            raise ValueError("canonical files must be unique and ordered by relative path")
        stream_ids = tuple(stream.stream_id for stream in self.streams)
        if stream_ids != tuple(sorted(set(stream_ids))):
            raise ValueError("canonical stream outcomes must be unique and ordered")
        if self.row_count != sum(file.row_count for file in self.files):
            raise ValueError("batch row_count does not match its files")
        if self.row_count != sum(stream.row_count for stream in self.streams):
            raise ValueError("batch row_count does not match publishable stream outcomes")
        if self.manifest_created_at < self.fixed_ingested_at:
            raise ValueError("manifest timestamp precedes the fixed ingestion timestamp")
        identity = CanonicalBatchIdentity(
            request_spec_hash=self.request_spec_hash,
            ordered_artifacts=self.ordered_raw_artifacts,
            processing_signature=self.processing_signature,
        )
        if self.canonical_batch_id != identity.canonical_batch_id:
            raise ValueError("canonical batch ID does not match its content identity")
        if self.batch_context_id != f"batch_context_v1_{identity.batch_hash}":
            raise ValueError("batch context ID does not match the canonical identity")
        if tuple(binding.artifact_id for binding in self.provenance.raw_bindings) != tuple(
            artifact.artifact_id for artifact in self.ordered_raw_artifacts
        ):
            raise ValueError("raw provenance bindings do not match ordered artifacts")
        if self.processing_signature.calendar_snapshot_checksum != self.calendar_snapshot.checksum:
            raise ValueError("calendar snapshot does not match the processing signature")
        if not self.eligible_slots:
            raise ValueError("canonical rows require calendar-eligible slots")
        slot_keys = tuple((slot.start_utc, slot.end_utc) for slot in self.eligible_slots)
        if slot_keys != tuple(sorted(set(slot_keys))):
            raise ValueError("calendar slots must be unique and ordered")
        if any(
            slot.timeframe is not stream.stream.timeframe
            for stream in self.streams
            for slot in self.eligible_slots
        ):
            raise ValueError("calendar slots disagree with stream timeframe")
        if any(
            (stream.stream.provider, stream.stream.dataset) != (self.provider, self.dataset)
            for stream in self.streams
        ):
            raise ValueError("manifest streams disagree with exact provider/dataset")
        request_bounds = {(stream.request_start, stream.request_end) for stream in self.streams}
        if len(request_bounds) != 1:
            raise ValueError("manifest streams must share one bounded request interval")
        request_start, request_end = next(iter(request_bounds))
        if not _calendar_covers_bounds(
            self.calendar_snapshot,
            request_start,
            request_end,
        ):
            raise ValueError("calendar snapshot does not cover the whole bounded request")
        timeframe = self.streams[0].stream.timeframe
        expected_slots = tuple(
            slot
            for slot in self.calendar_snapshot.expected_slots(timeframe)
            if slot.start_utc >= request_start and slot.end_utc <= request_end
        )
        if self.eligible_slots != expected_slots:
            raise ValueError("calendar-eligible slots do not replay from the immutable snapshot")
        if any(stream.stream.timeframe is not timeframe for stream in self.streams):
            raise ValueError("manifest streams must share one exact timeframe")
        return self


class PublishedCanonicalBatch(_FrozenPublicationModel):
    root_id: UUID
    canonical_batch_id: str
    relative_directory: str
    manifest_relative_path: str
    files: tuple[CanonicalFileManifest, ...]
    row_count: Annotated[int, Field(gt=0)]
    created: bool


def _assert_manifest_expectation(
    manifest: CanonicalBatchManifest,
    expectation: CanonicalBatchExpectation,
) -> None:
    specification = expectation.specification
    context = expectation.batch_context
    actual = (
        manifest.canonical_batch_id,
        manifest.batch_context_id,
        manifest.provider,
        manifest.dataset,
        manifest.request_spec_hash,
        manifest.ordered_raw_artifacts,
        manifest.processing_signature,
        manifest.provenance,
        manifest.calendar_snapshot,
        manifest.fixed_ingested_at,
        manifest.manifest_created_at,
        manifest.streams,
    )
    expected = (
        context.canonical_batch_id,
        context.batch_context_id,
        specification.provider,
        specification.dataset,
        specification.request_spec_hash,
        context.batch_identity.ordered_artifacts,
        context.batch_identity.processing_signature,
        expectation.provenance,
        expectation.calendar_snapshot,
        context.fixed_ingested_at,
        context.manifest_created_at,
        tuple(sorted(expectation.streams, key=lambda value: value.stream_id)),
    )
    if actual != expected:
        raise PublicationCollisionError(
            "canonical batch conflicts with its persisted semantic expectation"
        )


def _canonical_relative_directory(
    provider: str,
    dataset: str,
    canonical_batch_id: str,
) -> PurePosixPath:
    provider = safe_partition_value(provider, label="provider")
    dataset = safe_partition_value(dataset, label="dataset")
    return PurePosixPath(
        "normalized",
        "price_bars",
        f"provider={provider}",
        f"dataset={dataset}",
        "batches",
        # Full identity is stored in the manifest. The compact physical key
        # avoids legacy Windows MAX_PATH failures and is collision-checked.
        f"batch={canonical_batch_id.removeprefix('batch_v1_')[:32]}",
    )


def _schema_descriptor(schema: pl.Schema) -> tuple[tuple[str, str], ...]:
    return tuple((name, str(dtype)) for name, dtype in schema.items())


def _schema_sha256(schema: pl.Schema) -> str:
    return hashlib.sha256(json_bytes(_schema_descriptor(schema))).hexdigest()


def _calendar_covers_bounds(
    snapshot: CalendarSnapshot,
    start: datetime,
    end: datetime,
) -> bool:
    try:
        calendar_timezone = ZoneInfo(snapshot.timezone_name)
    except (KeyError, ValueError) as error:
        raise PublicationIntegrityError("calendar snapshot timezone is unavailable") from error
    first_request_date = start.astimezone(calendar_timezone).date()
    last_request_date = (end - timedelta(microseconds=1)).astimezone(calendar_timezone).date()
    return snapshot.range_start <= first_request_date and snapshot.range_end > last_request_date


def _aware_utc(value: object, *, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise PublicationIntegrityError(f"{label} must contain timezone-aware datetimes")
    return value.astimezone(UTC)


def _frame_bounds(frame: pl.DataFrame) -> tuple[datetime, datetime]:
    if _TIMESTAMP_START not in frame.columns or _TIMESTAMP_END not in frame.columns:
        raise PublicationIntegrityError(
            "canonical frame lacks timestamp_start/timestamp_end columns"
        )
    start = _aware_utc(frame.get_column(_TIMESTAMP_START).min(), label=_TIMESTAMP_START)
    end = _aware_utc(frame.get_column(_TIMESTAMP_END).max(), label=_TIMESTAMP_END)
    if end <= start:
        raise PublicationIntegrityError("canonical frame has invalid temporal bounds")
    return start, end


def _validate_fixed_ingested_at(frame: pl.DataFrame, expected: datetime) -> None:
    if _INGESTED_AT not in frame.columns:
        raise PublicationIntegrityError("canonical frame lacks fixed ingested_at provenance")
    values = frame.get_column(_INGESTED_AT).unique().to_list()
    if len(values) != 1 or _aware_utc(values[0], label=_INGESTED_AT) != expected:
        raise PublicationIntegrityError(
            "canonical frame does not reuse BatchContext.fixed_ingested_at"
        )


def _validate_canonical_schema(frame: pl.DataFrame) -> None:
    if frame.schema != PRICE_BAR_SCHEMA:
        raise PublicationIntegrityError(
            "canonical frame does not match the implemented price-bar schema"
        )


def _validate_part_partition(relative_path: str, frame: pl.DataFrame) -> None:
    match = _PARTITIONED_PARQUET.fullmatch(relative_path)
    if match is None:
        raise PublicationIntegrityError(
            "canonical Parquet path must use timeframe/year/month partitioning"
        )
    timeframes = frame.get_column("timeframe").unique().to_list()
    starts = frame.get_column(_TIMESTAMP_START).to_list()
    if len(timeframes) != 1 or str(timeframes[0]) != match.group("timeframe"):
        raise PublicationIntegrityError("canonical timeframe partition disagrees with its rows")
    partitions = {
        (
            _aware_utc(value, label=_TIMESTAMP_START).year,
            _aware_utc(value, label=_TIMESTAMP_START).month,
        )
        for value in starts
    }
    expected = (int(match.group("year")), int(match.group("month")))
    if partitions != {expected}:
        raise PublicationIntegrityError("canonical year/month partition disagrees with its rows")


def _assert_acquisition_shape(
    specification: RequestSpecification,
    batch_context: BatchContext,
    authorization: AcquisitionPolicyAuthorization,
) -> None:
    identity = batch_context.batch_identity
    request = authorization.request
    snapshot = request.policy_snapshot
    if identity.request_spec_hash != specification.request_spec_hash:
        raise DatasetPolicyDenied("batch context is for a different request specification")
    if (snapshot.provider, snapshot.dataset) != (
        specification.provider,
        specification.dataset,
    ):
        raise DatasetPolicyDenied("acquisition authorization is for a different exact dataset")
    if request.request_start != specification.start or request.request_end != specification.end:
        raise DatasetPolicyDenied("acquisition authorization is for different request bounds")
    input_artifacts = tuple(
        AuthorizedRawArtifactDescriptor(
            request_spec_hash=artifact.request_spec_hash,
            page_ordinal=artifact.page_ordinal,
            page_relation=artifact.page_relation,
            content_sha256=artifact.content_sha256,
            byte_count=artifact.byte_count,
            media_type=artifact.media_type,
            content_encoding=artifact.content_encoding,
        )
        for artifact in identity.ordered_artifacts
    )
    if authorization.ordered_artifacts != input_artifacts:
        raise DatasetPolicyDenied("batch raw inputs differ from the authorized page sequence")
    if batch_context.fixed_ingested_at < authorization.authorized_at:
        raise DatasetPolicyDenied("batch ingestion time precedes acquisition authorization")


def _validate_canonical_rows(
    frame: pl.DataFrame,
    *,
    outcomes: tuple[CanonicalStreamOutcome, ...],
    request_start: datetime,
    request_end: datetime,
    eligible_slots: tuple[ExpectedCalendarSlot, ...],
    fixed_ingested_at: datetime,
    provenance: CanonicalPublicationProvenance,
) -> None:
    """Reject any row set that cannot support an exact immutable manifest claim."""

    _validate_canonical_schema(frame)
    _validate_fixed_ingested_at(frame, fixed_ingested_at)
    required = tuple(
        column
        for column in PRICE_BAR_SCHEMA
        if column not in {"volume", "vwap", "available_at", "provider_record_id"}
    )
    null_counts = frame.select(pl.col(required).null_count()).row(0)
    if any(int(count) != 0 for count in null_counts):
        raise PublicationIntegrityError("canonical required columns contain nulls")
    for column in ("open", "high", "low", "close", "volume", "vwap"):
        if not frame.get_column(column).drop_nulls().is_finite().all():
            raise PublicationIntegrityError("canonical numeric values must be finite")

    streams_by_instrument = {str(outcome.stream.instrument_id): outcome for outcome in outcomes}
    if len(streams_by_instrument) != len(outcomes):
        raise PublicationIntegrityError("manifest outcomes contain duplicate instruments")
    allowed_raw_batch_ids = {str(binding.raw_batch_id) for binding in provenance.raw_bindings}
    eligible = {(slot.start_utc, slot.end_utc) for slot in eligible_slots}
    observed_keys: set[tuple[str, datetime, datetime]] = set()
    counts = {outcome.stream_id: 0 for outcome in outcomes}
    starts: dict[str, datetime] = {}
    ends: dict[str, datetime] = {}

    for row in frame.iter_rows(named=True):
        instrument_id = str(row["instrument_id"])
        outcome = streams_by_instrument.get(instrument_id)
        if outcome is None:
            raise PublicationIntegrityError(
                "canonical row instrument is outside the bounded request"
            )
        stream = outcome.stream
        start = _aware_utc(row[_TIMESTAMP_START], label=_TIMESTAMP_START)
        end = _aware_utc(row[_TIMESTAMP_END], label=_TIMESTAMP_END)
        key = (stream.stream_id, start, end)
        if key in observed_keys:
            raise PublicationIntegrityError(
                "canonical observation identity is duplicated across batch parts"
            )
        observed_keys.add(key)
        if end <= start or start < request_start or end > request_end:
            raise PublicationIntegrityError("canonical observation exceeds bounded request")
        if (start, end) not in eligible:
            raise PublicationIntegrityError(
                "canonical observation is not an exact calendar-eligible slot"
            )
        if (
            row["timeframe"] != stream.timeframe.value
            or row["session"] != stream.session.value
            or row["adjustment_state"] != stream.adjustment.value
            or row["currency"] != stream.currency
        ):
            raise PublicationIntegrityError(
                "canonical row dimensions disagree with its exact stream"
            )
        if str(row["source_id"]) != str(provenance.source_id):
            raise PublicationIntegrityError("canonical source provenance is inconsistent")
        if str(row["raw_batch_id"]) not in allowed_raw_batch_ids:
            raise PublicationIntegrityError("canonical raw provenance is not authorized")
        retrieved_at = _aware_utc(row["retrieved_at"], label="retrieved_at")
        if end > retrieved_at:
            raise PublicationIntegrityError("canonical retrieval precedes observation completion")
        if retrieved_at > fixed_ingested_at:
            raise PublicationIntegrityError("canonical retrieval time follows ingestion time")
        available_at = row["available_at"]
        if available_at is not None and (
            _aware_utc(available_at, label="available_at") > retrieved_at
        ):
            raise PublicationIntegrityError("canonical availability time follows retrieval time")
        counts[stream.stream_id] += 1
        starts[stream.stream_id] = min(starts.get(stream.stream_id, start), start)
        ends[stream.stream_id] = max(ends.get(stream.stream_id, end), end)

    validation = validate_bars(frame)
    if (
        validation.frame.get_column("quality_flags").to_list()
        != frame.get_column("quality_flags").to_list()
    ):
        raise PublicationIntegrityError(
            "canonical rows were not annotated by the canonical validator"
        )

    for outcome in outcomes:
        if outcome.request_start != request_start or outcome.request_end != request_end:
            raise PublicationIntegrityError("stream outcome bounds differ from request bounds")
        if outcome.outcome is StreamPublicationOutcome.BLOCKED:
            if counts[outcome.stream_id] != 0:
                raise PublicationIntegrityError("blocked stream contributed canonical rows")
            continue
        if (
            outcome.row_count != counts[outcome.stream_id]
            or outcome.observed_start != starts.get(outcome.stream_id)
            or outcome.observed_end != ends.get(outcome.stream_id)
        ):
            raise PublicationIntegrityError(
                "publishable stream summary disagrees with canonical rows"
            )


def verify_canonical_batch_directory(
    directory: Path,
    *,
    data_root: PrivateDataRoot,
    root_id: UUID,
    expected_manifest: CanonicalBatchManifest | None = None,
    expected_semantics: CanonicalBatchExpectation | None = None,
    expected_provider: str | None = None,
    expected_dataset: str | None = None,
    expected_batch_id: str | None = None,
) -> CanonicalBatchManifest:
    """Share exact tree/content/provenance verification with publisher and recovery."""

    try:
        actual_files = {
            path.relative_to(directory).as_posix() for path in iter_safe_regular_files(directory)
        }
        manifest = CanonicalBatchManifest.model_validate_json(
            (directory / _MANIFEST_NAME).read_bytes()
        )
    except (OSError, ValueError) as error:
        raise PublicationIntegrityError("canonical manifest is missing or invalid") from error
    if expected_manifest is not None and manifest != expected_manifest:
        raise PublicationCollisionError(
            f"canonical batch {expected_manifest.canonical_batch_id} has conflicting metadata"
        )
    if expected_provider is not None and manifest.provider != expected_provider:
        raise PublicationCollisionError("canonical provider conflicts with its exact path")
    if expected_dataset is not None and manifest.dataset != expected_dataset:
        raise PublicationCollisionError("canonical dataset conflicts with its exact path")
    if expected_batch_id is not None and manifest.canonical_batch_id != expected_batch_id:
        raise PublicationCollisionError("canonical batch identity conflicts with its exact path")
    if expected_semantics is not None:
        _assert_manifest_expectation(manifest, expected_semantics)
    expected_files = {_MANIFEST_NAME, *(file.relative_path for file in manifest.files)}
    if actual_files != expected_files:
        raise PublicationIntegrityError("canonical batch contains missing or unexpected files")

    frames: list[pl.DataFrame] = []
    for expected in manifest.files:
        relative = safe_relative_file(expected.relative_path, suffix=".parquet")
        path = directory.joinpath(*relative.parts)
        sha256, byte_count = file_integrity(path)
        if sha256 != expected.sha256 or byte_count != expected.byte_count:
            raise PublicationIntegrityError(
                f"canonical Parquet checksum failed for {expected.relative_path}"
            )
        try:
            frame = pl.read_parquet(path)
        except (OSError, pl.exceptions.PolarsError) as error:
            raise PublicationIntegrityError(
                f"canonical Parquet cannot be reopened: {expected.relative_path}"
            ) from error
        start, end = _frame_bounds(frame)
        _validate_canonical_schema(frame)
        _validate_part_partition(expected.relative_path, frame)
        if (
            frame.height != expected.row_count
            or _schema_sha256(frame.schema) != expected.schema_sha256
            or start != expected.timestamp_start_min
            or end != expected.timestamp_end_max
        ):
            raise PublicationIntegrityError(
                f"canonical Parquet metadata disagrees with manifest: {expected.relative_path}"
            )
        frames.append(frame)

    combined = pl.concat(frames, how="vertical", rechunk=False)
    request_start = manifest.streams[0].request_start
    request_end = manifest.streams[0].request_end
    _validate_canonical_rows(
        combined,
        outcomes=manifest.streams,
        request_start=request_start,
        request_end=request_end,
        eligible_slots=manifest.eligible_slots,
        fixed_ingested_at=manifest.fixed_ingested_at,
        provenance=manifest.provenance,
    )
    if combined.height != manifest.row_count:
        raise PublicationIntegrityError("canonical row count disagrees with manifest")

    data_root.validate(expected_root_id=root_id)
    for artifact in manifest.ordered_raw_artifacts:
        relative = raw_artifact_relative_directory(
            manifest.provider,
            manifest.dataset,
            artifact.artifact_id,
        )
        raw_directory = managed_path(data_root, root_id, relative)
        verify_raw_artifact_directory(
            raw_directory,
            expected_identity=artifact,
            expected_provider=manifest.provider,
            expected_dataset=manifest.dataset,
        )
    data_root.validate(expected_root_id=root_id)
    return manifest


class CanonicalBatchPublisher:
    """Write, verify, and atomically rename a complete immutable Parquet batch."""

    def __init__(
        self,
        data_root: PrivateDataRoot,
        policy_enforcer: RetentionPolicyEnforcer,
    ) -> None:
        self._data_root = data_root
        self._root_id = data_root.validate().root_id
        self._policy_enforcer = policy_enforcer

    @property
    def root_id(self) -> UUID:
        return self._root_id

    def _verify_directory(
        self,
        directory: Path,
        expected_manifest: CanonicalBatchManifest,
    ) -> CanonicalBatchManifest:
        return verify_canonical_batch_directory(
            directory,
            data_root=self._data_root,
            root_id=self._root_id,
            expected_manifest=expected_manifest,
        )

    def _verified_existing(
        self,
        relative_directory: PurePosixPath,
        expected_manifest: CanonicalBatchManifest,
    ) -> PublishedCanonicalBatch:
        target = managed_path(self._data_root, self._root_id, relative_directory)
        assert_direct_owned_directory(target, parent=target.parent)
        self._verify_directory(target, expected_manifest)
        return PublishedCanonicalBatch(
            root_id=self._root_id,
            canonical_batch_id=expected_manifest.canonical_batch_id,
            relative_directory=relative_directory.as_posix(),
            manifest_relative_path=(relative_directory / _MANIFEST_NAME).as_posix(),
            files=expected_manifest.files,
            row_count=expected_manifest.row_count,
            created=False,
        )

    def _recover_staging_candidate(
        self,
        staging_parent: Path,
        *,
        specification: RequestSpecification,
        batch_context: BatchContext,
        parts: tuple[CanonicalParquetPart, ...],
        relative_paths: tuple[PurePosixPath, ...],
        outcomes: tuple[CanonicalStreamOutcome, ...],
        calendar_snapshot: CalendarSnapshot,
        eligible_slots: tuple[ExpectedCalendarSlot, ...],
        provenance: CanonicalPublicationProvenance,
    ) -> tuple[Path, CanonicalBatchManifest] | None:
        prefix = f"batch={batch_context.batch_identity.batch_hash}."
        matching: list[tuple[Path, CanonicalBatchManifest]] = []
        for candidate in sorted(staging_parent.iterdir(), key=lambda value: value.name):
            if not candidate.name.startswith(prefix):
                continue
            checked = managed_path(
                self._data_root,
                self._root_id,
                candidate.relative_to(self._data_root.root),
            )
            if checked != candidate or not checked.is_dir():
                raise PublicationIntegrityError("canonical staging candidate is unsafe")
            try:
                manifest = verify_canonical_batch_directory(
                    candidate,
                    data_root=self._data_root,
                    root_id=self._root_id,
                )
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
                        CanonicalBatchManifest.model_validate_json(
                            (candidate / _MANIFEST_NAME).read_bytes()
                        )
                    except (OSError, ValueError):
                        pass
                    else:
                        complete_manifest = True
                if complete_manifest:
                    raise PublicationCollisionError(
                        "complete canonical staging failed integrity verification"
                    ) from error
                remove_owned_staging_directory(
                    self._data_root,
                    self._root_id,
                    candidate,
                    staging_parent=staging_parent,
                )
                continue
            stable_identity = (
                manifest.canonical_batch_id,
                manifest.batch_context_id,
                manifest.provider,
                manifest.dataset,
                manifest.request_spec_hash,
                manifest.ordered_raw_artifacts,
                manifest.processing_signature,
                manifest.calendar_snapshot,
                manifest.fixed_ingested_at,
                manifest.manifest_created_at,
                manifest.streams,
                manifest.eligible_slots,
                manifest.provenance,
            )
            expected_identity = (
                batch_context.canonical_batch_id,
                batch_context.batch_context_id,
                specification.provider,
                specification.dataset,
                specification.request_spec_hash,
                batch_context.batch_identity.ordered_artifacts,
                batch_context.batch_identity.processing_signature,
                calendar_snapshot,
                batch_context.fixed_ingested_at,
                batch_context.manifest_created_at,
                outcomes,
                eligible_slots,
                provenance,
            )
            if stable_identity != expected_identity:
                raise PublicationCollisionError(
                    "complete canonical staging conflicts with the fixed batch context"
                )
            if tuple(file.relative_path for file in manifest.files) != tuple(
                path.as_posix() for path in relative_paths
            ):
                raise PublicationCollisionError(
                    "complete canonical staging has different Parquet part paths"
                )
            for part, file in zip(parts, manifest.files, strict=True):
                staged = pl.read_parquet(candidate / file.relative_path)
                if not staged.equals(part.frame):
                    raise PublicationCollisionError(
                        "complete canonical staging has different normalized values"
                    )
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

    def _cleanup_batch_staging(
        self,
        staging_parent: Path,
        batch_context: BatchContext,
    ) -> None:
        prefix = f"batch={batch_context.batch_identity.batch_hash}."
        for candidate in sorted(staging_parent.iterdir(), key=lambda value: value.name):
            if candidate.name.startswith(prefix):
                remove_owned_staging_directory(
                    self._data_root,
                    self._root_id,
                    candidate,
                    staging_parent=staging_parent,
                )

    def _all_blocked_has_conflict(
        self,
        specification: RequestSpecification,
        batch_context: BatchContext,
    ) -> bool:
        final_relative = _canonical_relative_directory(
            specification.provider,
            specification.dataset,
            batch_context.canonical_batch_id,
        )
        final = managed_path(self._data_root, self._root_id, final_relative)
        if final.exists() or final.is_symlink():
            return True
        staging_parent = managed_path(
            self._data_root,
            self._root_id,
            _CANONICAL_STAGING_PARENT,
        )
        if not staging_parent.exists():
            return False
        prefix = f"batch={batch_context.batch_identity.batch_hash}."
        return any(candidate.name.startswith(prefix) for candidate in staging_parent.iterdir())

    def _publish_verified_candidate(
        self,
        candidate: Path,
        manifest: CanonicalBatchManifest,
        *,
        staging_parent: Path,
        batch_context: BatchContext,
        fault_injector: FaultInjector | None,
    ) -> PublishedCanonicalBatch:
        relative_directory = _canonical_relative_directory(
            manifest.provider,
            manifest.dataset,
            manifest.canonical_batch_id,
        )
        target_parent = ensure_directory(
            self._data_root,
            self._root_id,
            relative_directory.parent,
        )
        target = managed_path(self._data_root, self._root_id, relative_directory)
        if target.exists() or target.is_symlink():
            existing = self._verified_existing(relative_directory, manifest)
            self._cleanup_batch_staging(staging_parent, batch_context)
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
                existing = self._verified_existing(relative_directory, manifest)
                self._cleanup_batch_staging(staging_parent, batch_context)
                return existing
            raise PublicationError("canonical batch atomic publication failed") from error
        fsync_directory(target_parent)
        invoke_fault(fault_injector, PublicationFaultPoint.RENAME)
        invoke_fault(fault_injector, PublicationFaultPoint.REOPEN)
        verified = self._verified_existing(relative_directory, manifest)
        self._cleanup_batch_staging(staging_parent, batch_context)
        return verified.model_copy(update={"created": True})

    def publish(
        self,
        specification: RequestSpecification,
        batch_context: BatchContext,
        parts: tuple[CanonicalParquetPart, ...],
        stream_outcomes: tuple[CanonicalStreamOutcome, ...],
        *,
        authorization: AcquisitionPolicyAuthorization,
        calendar_snapshot: CalendarSnapshot,
        provenance: CanonicalPublicationProvenance | None,
        runtime_status: DatasetRuntimeStatus | None = None,
        fault_injector: FaultInjector | None = None,
    ) -> PublishedCanonicalBatch | None:
        """Publish a complete batch; exact replay verifies and returns a filesystem no-op."""

        _assert_acquisition_shape(specification, batch_context, authorization)
        expected_streams = tuple(
            sorted(specification.stream_keys(), key=lambda value: value.stream_id)
        )
        supplied_streams = tuple(
            outcome.stream for outcome in sorted(stream_outcomes, key=lambda value: value.stream_id)
        )
        if supplied_streams != expected_streams:
            raise PublicationError("stream outcomes do not cover the exact bounded request")
        if any(
            outcome.request_start != specification.start or outcome.request_end != specification.end
            for outcome in stream_outcomes
        ):
            raise PublicationIntegrityError("stream outcome bounds differ from the bounded request")
        if (
            batch_context.batch_identity.processing_signature.calendar_snapshot_checksum
            != calendar_snapshot.checksum
        ):
            raise PublicationIntegrityError("calendar snapshot differs from batch identity")
        if not _calendar_covers_bounds(
            calendar_snapshot,
            specification.start,
            specification.end,
        ):
            raise PublicationIntegrityError(
                "calendar snapshot does not cover the whole bounded request"
            )
        eligible_slots = tuple(
            slot
            for slot in calendar_snapshot.expected_slots(specification.timeframe)
            if slot.start_utc >= specification.start and slot.end_utc <= specification.end
        )
        if not eligible_slots:
            raise PublicationIntegrityError("bounded request has no calendar-eligible slots")
        all_blocked = all(
            outcome.outcome is StreamPublicationOutcome.BLOCKED for outcome in stream_outcomes
        )
        if all_blocked:
            if parts:
                raise PublicationIntegrityError("all-blocked request cannot publish Parquet rows")
        else:
            if not parts:
                raise PublicationError("publishable streams require at least one Parquet part")
            if provenance is None:
                raise PublicationIntegrityError("publishable streams require exact raw provenance")
            expected_artifact_ids = tuple(
                artifact.artifact_id for artifact in batch_context.batch_identity.ordered_artifacts
            )
            if (
                tuple(binding.artifact_id for binding in provenance.raw_bindings)
                != expected_artifact_ids
            ):
                raise PublicationIntegrityError("raw provenance does not match batch inputs")
        for part in parts:
            if part.frame.is_empty():
                raise PublicationError("canonical Parquet parts cannot be empty")
            _validate_canonical_schema(part.frame)
            _validate_fixed_ingested_at(part.frame, batch_context.fixed_ingested_at)
            _validate_part_partition(part.relative_path, part.frame)
        if parts:
            if provenance is None:
                raise PublicationIntegrityError("canonical rows require exact raw provenance")
            combined_input = pl.concat(
                [part.frame for part in parts],
                how="vertical",
                rechunk=False,
            )
            _validate_canonical_rows(
                combined_input,
                outcomes=stream_outcomes,
                request_start=specification.start,
                request_end=specification.end,
                eligible_slots=eligible_slots,
                fixed_ingested_at=batch_context.fixed_ingested_at,
                provenance=provenance,
            )
        input_artifacts = tuple(
            AuthorizedRawArtifactDescriptor(
                request_spec_hash=artifact.request_spec_hash,
                page_ordinal=artifact.page_ordinal,
                page_relation=artifact.page_relation,
                content_sha256=artifact.content_sha256,
                byte_count=artifact.byte_count,
                media_type=artifact.media_type,
                content_encoding=artifact.content_encoding,
            )
            for artifact in batch_context.batch_identity.ordered_artifacts
        )
        if all_blocked:
            self._policy_enforcer.authorize_processing(
                specification.provider,
                specification.dataset,
                environment=authorization.request.environment,
                runtime_status=runtime_status,
            )
        else:
            self._policy_enforcer.authorize_persistence(
                specification.provider,
                specification.dataset,
                environment=authorization.request.environment,
                layer=RetentionLayer.NORMALIZED,
                runtime_status=runtime_status,
                acquisition_authorization=authorization,
                input_artifacts=input_artifacts,
            )
        for artifact in batch_context.batch_identity.ordered_artifacts:
            raw_directory = managed_path(
                self._data_root,
                self._root_id,
                raw_artifact_relative_directory(
                    specification.provider,
                    specification.dataset,
                    artifact.artifact_id,
                ),
            )
            verify_raw_artifact_directory(
                raw_directory,
                expected_identity=artifact,
                expected_provider=specification.provider,
                expected_dataset=specification.dataset,
            )
        if all_blocked:
            if self._all_blocked_has_conflict(specification, batch_context):
                raise PublicationCollisionError(
                    "all-blocked result conflicts with an existing canonical batch effect"
                )
            return None
        if provenance is None:
            raise PublicationIntegrityError("canonical publication lacks raw provenance")

        relative_paths = tuple(
            safe_relative_file(part.relative_path, suffix=".parquet") for part in parts
        )
        if tuple(path.as_posix() for path in relative_paths) != tuple(
            sorted({path.as_posix() for path in relative_paths})
        ):
            raise PublicationError("canonical part paths must be unique and sorted")

        ordered_outcomes = tuple(sorted(stream_outcomes, key=lambda outcome: outcome.stream_id))

        staging_parent = ensure_directory(
            self._data_root,
            self._root_id,
            _CANONICAL_STAGING_PARENT,
        )
        recovered = self._recover_staging_candidate(
            staging_parent,
            specification=specification,
            batch_context=batch_context,
            parts=parts,
            relative_paths=relative_paths,
            outcomes=ordered_outcomes,
            calendar_snapshot=calendar_snapshot,
            eligible_slots=eligible_slots,
            provenance=provenance,
        )
        if recovered is not None:
            candidate, recovered_manifest = recovered
            invoke_fault(
                fault_injector,
                PublicationFaultPoint.STAGED_MANIFEST_VERIFIED,
            )
            return self._publish_verified_candidate(
                candidate,
                recovered_manifest,
                staging_parent=staging_parent,
                batch_context=batch_context,
                fault_injector=fault_injector,
            )
        candidate = staging_parent / (
            f"batch={batch_context.batch_identity.batch_hash}.{uuid4().hex[:16]}.tmp"
        )
        try:
            candidate.mkdir(exist_ok=False)
            fsync_directory(staging_parent)
        except OSError as error:
            raise PublicationError("failed to create canonical staging directory") from error
        invoke_fault(fault_injector, PublicationFaultPoint.STAGING)
        assert_owned_staging_candidate(
            self._data_root,
            self._root_id,
            candidate,
            staging_parent=staging_parent,
        )

        file_manifests: list[CanonicalFileManifest] = []
        expected_schema_hash: str | None = None
        for part, relative_path in zip(parts, relative_paths, strict=True):
            assert_owned_staging_candidate(
                self._data_root,
                self._root_id,
                candidate,
                staging_parent=staging_parent,
            )
            frame = part.frame
            if frame.is_empty():
                raise PublicationError("canonical Parquet parts cannot be empty")
            _validate_canonical_schema(frame)
            _validate_fixed_ingested_at(frame, batch_context.fixed_ingested_at)
            _validate_part_partition(relative_path.as_posix(), frame)
            start, end = _frame_bounds(frame)
            if start < specification.start or end > specification.end:
                raise PublicationIntegrityError(
                    "canonical part contains observations outside the bounded request"
                )
            schema_hash = _schema_sha256(frame.schema)
            if expected_schema_hash is None:
                expected_schema_hash = schema_hash
            elif schema_hash != expected_schema_hash:
                raise PublicationError("all canonical parts must use one exact schema")

            target = candidate.joinpath(*relative_path.parts)
            try:
                parent = ensure_direct_subdirectory(
                    candidate,
                    PurePosixPath(*relative_path.parts[:-1]),
                )
                target = parent / relative_path.name
                with target.open("xb") as writer:
                    frame.write_parquet(writer, compression="zstd", statistics=True)
                    writer.flush()
                    os.fsync(writer.fileno())
                fsync_directory(target.parent)
            except (OSError, pl.exceptions.PolarsError) as error:
                raise PublicationError(
                    f"failed to write canonical Parquet part {relative_path.as_posix()}"
                ) from error
            sha256, byte_count = file_integrity(target)
            candidate_file = CanonicalFileManifest(
                relative_path=relative_path.as_posix(),
                sha256=sha256,
                byte_count=byte_count,
                row_count=frame.height,
                schema_sha256=schema_hash,
                timestamp_start_min=start,
                timestamp_end_max=end,
            )
            # Reopen staged output before the completion manifest exists.
            reopened = pl.read_parquet(target)
            reopened_start, reopened_end = _frame_bounds(reopened)
            if (
                reopened.height != candidate_file.row_count
                or _schema_sha256(reopened.schema) != candidate_file.schema_sha256
                or reopened_start != candidate_file.timestamp_start_min
                or reopened_end != candidate_file.timestamp_end_max
            ):
                raise PublicationIntegrityError("staged canonical Parquet failed verification")
            _validate_fixed_ingested_at(reopened, batch_context.fixed_ingested_at)
            file_manifests.append(candidate_file)
            invoke_fault(fault_injector, PublicationFaultPoint.STAGING)

        assert_owned_staging_candidate(
            self._data_root,
            self._root_id,
            candidate,
            staging_parent=staging_parent,
        )
        row_count = sum(file.row_count for file in file_manifests)
        manifest = CanonicalBatchManifest(
            canonical_batch_id=batch_context.canonical_batch_id,
            batch_context_id=batch_context.batch_context_id,
            provider=specification.provider,
            dataset=specification.dataset,
            request_spec_hash=specification.request_spec_hash,
            ordered_raw_artifacts=batch_context.batch_identity.ordered_artifacts,
            processing_signature=batch_context.batch_identity.processing_signature,
            provenance=provenance,
            calendar_snapshot=calendar_snapshot,
            eligible_slots=eligible_slots,
            fixed_ingested_at=batch_context.fixed_ingested_at,
            manifest_created_at=batch_context.manifest_created_at,
            files=tuple(file_manifests),
            streams=ordered_outcomes,
            row_count=row_count,
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
        self._verify_directory(candidate, manifest)
        invoke_fault(
            fault_injector,
            PublicationFaultPoint.STAGED_MANIFEST_VERIFIED,
        )
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
            batch_context=batch_context,
            fault_injector=fault_injector,
        )


__all__ = [
    "CanonicalBatchExpectation",
    "CanonicalBatchManifest",
    "CanonicalBatchPublisher",
    "CanonicalFileManifest",
    "CanonicalParquetPart",
    "CanonicalPublicationProvenance",
    "CanonicalStreamOutcome",
    "PublishedCanonicalBatch",
    "RawProvenanceBinding",
    "StreamPublicationOutcome",
    "verify_canonical_batch_directory",
]
