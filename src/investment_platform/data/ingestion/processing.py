"""Deterministic Alpaca SIP normalization and canonical-batch preparation."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Final

import polars as pl

from investment_platform.data.calendar import CalendarSnapshot
from investment_platform.data.ingestion.acquisition import (
    CompletedRawAcquisition,
    specification_to_bar_request,
)
from investment_platform.data.ingestion.identity import (
    BatchContext,
    CanonicalBatchIdentity,
    IdentityDimension,
    ProcessingSignature,
    RawArtifactIdentity,
    RequestSpecification,
)
from investment_platform.data.models import BarQualityFlag, PriceBar
from investment_platform.data.normalization import (
    DailyBarSemantics,
    NormalizationError,
    NormalizationIssue,
    NormalizationIssueCode,
    NormalizationSeverity,
    SessionBounds,
    StaticSessionSchedule,
    normalize_alpaca_bars,
)
from investment_platform.data.provenance import RawBatch
from investment_platform.data.providers.alpaca import ALPACA_SIP_BAR_SOURCE
from investment_platform.data.retention import AcquisitionPolicyAuthorization
from investment_platform.data.storage import (
    CanonicalBatchExpectation,
    CanonicalParquetPart,
    CanonicalPublicationProvenance,
    CanonicalStreamOutcome,
    RawProvenanceBinding,
    StreamPublicationOutcome,
    price_bars_to_frame,
)
from investment_platform.data.validation import validate_bars

CANONICAL_PRICE_BAR_SCHEMA_VERSION: Final = "price-bar-v1"
ALPACA_SIP_NORMALIZER_VERSION: Final = "alpaca-sip-bars-v1"
PRICE_BAR_VALIDATOR_VERSION: Final = "price-bar-validator-v1"
_NON_FATAL_NORMALIZATION_CODES: Final = frozenset(
    {NormalizationIssueCode.OUTSIDE_REQUESTED_INTERVAL}
)
_BLOCKING_QUALITY_FLAGS: Final = frozenset({BarQualityFlag.DUPLICATE_BAR})
_RAW_VERIFY_CHUNK_BYTES: Final = 1024 * 1024


class CanonicalProcessingError(RuntimeError):
    """Raw evidence cannot be deterministically prepared for canonical publication."""


@dataclass(frozen=True, slots=True)
class RawProcessingPage:
    """One replayable raw page paired with its stable content identity."""

    identity: RawArtifactIdentity
    batch: RawBatch


@dataclass(frozen=True, slots=True)
class PreparedBatchContext:
    """Raw-verified identity/provenance that must be durable before normalization."""

    specification: RequestSpecification
    acquisition_authorization: AcquisitionPolicyAuthorization
    batch_context: BatchContext
    provenance: CanonicalPublicationProvenance


@dataclass(frozen=True, slots=True)
class PreparedCanonicalBatch:
    """Complete deterministic input for ``CanonicalBatchPublisher``."""

    specification: RequestSpecification
    acquisition_authorization: AcquisitionPolicyAuthorization
    batch_context: BatchContext
    expectation: CanonicalBatchExpectation
    parts: tuple[CanonicalParquetPart, ...]
    stream_outcomes: tuple[CanonicalStreamOutcome, ...]
    provenance: CanonicalPublicationProvenance
    normalization_issue_codes: tuple[str, ...]
    quality_issue_codes: tuple[str, ...]

    @property
    def publishable_stream_count(self) -> int:
        return sum(
            outcome.outcome is StreamPublicationOutcome.PUBLISHABLE
            for outcome in self.stream_outcomes
        )

    @property
    def blocked_stream_count(self) -> int:
        return len(self.stream_outcomes) - self.publishable_stream_count

    @property
    def all_blocked(self) -> bool:
        return self.publishable_stream_count == 0


def processing_pages_from_acquisition(
    acquisition: CompletedRawAcquisition,
) -> tuple[RawProcessingPage, ...]:
    """Create processing pages only for a wholly new, just-persisted acquisition.

    If any artifact was adopted, its stable RawBatch provenance must first be
    reloaded from the operational catalog.  Reusing the new attempt's UUID or
    retrieval time under an existing canonical batch identity would make a
    retry produce different Parquet bytes.
    """

    if any(not page.published.created for page in acquisition.pages):
        raise CanonicalProcessingError(
            "adopted raw artifacts require stable operational-catalog replay provenance"
        )

    return tuple(
        RawProcessingPage(identity=page.identity, batch=page.raw_batch)
        for page in acquisition.pages
    )


def session_schedule_from_snapshot(snapshot: CalendarSnapshot) -> StaticSessionSchedule:
    """Adapt the maintained Phase 2 calendar to the existing normalizer boundary."""

    return StaticSessionSchedule(
        tuple(
            SessionBounds(
                session_date=session.session_date,
                start=session.open_utc,
                end=session.close_utc,
                source=f"calendar:{snapshot.checksum}",
            )
            for session in snapshot.sessions
        )
    )


def _validate_page_chain(
    specification: RequestSpecification,
    pages: tuple[RawProcessingPage, ...],
    authorization: AcquisitionPolicyAuthorization,
) -> None:
    if not pages:
        raise CanonicalProcessingError("canonical processing requires at least one raw page")
    if (
        authorization.request.policy_snapshot.provider,
        authorization.request.policy_snapshot.dataset,
    ) != (
        specification.provider,
        specification.dataset,
    ):
        raise CanonicalProcessingError("acquisition authorization is for another exact dataset")
    identities = tuple(page.identity for page in pages)
    if tuple(identity.page_ordinal for identity in identities) != tuple(range(len(identities))):
        raise CanonicalProcessingError("raw processing pages are not a complete 0-based chain")
    if any(
        identity.request_spec_hash != specification.request_spec_hash for identity in identities
    ):
        raise CanonicalProcessingError("raw processing page belongs to another request")
    authorized = authorization.ordered_artifacts
    actual = tuple(
        (
            value.request_spec_hash,
            value.page_ordinal,
            value.page_relation,
            value.content_sha256,
            value.byte_count,
            value.media_type,
            value.content_encoding,
        )
        for value in identities
    )
    expected = tuple(
        (
            value.request_spec_hash,
            value.page_ordinal,
            value.page_relation,
            value.content_sha256,
            value.byte_count,
            value.media_type,
            value.content_encoding,
        )
        for value in authorized
    )
    if actual != expected:
        raise CanonicalProcessingError("raw processing pages differ from acquisition authorization")
    for page in pages:
        if page.batch.metadata.source != ALPACA_SIP_BAR_SOURCE:
            raise CanonicalProcessingError("raw processing page is not historical Alpaca SIP data")
        if page.batch.metadata.media_type.strip().casefold() != page.identity.media_type:
            raise CanonicalProcessingError("raw processing media type differs from its identity")
        digest = hashlib.sha256()
        byte_count = 0
        try:
            with page.batch.payload.open_binary() as reader:
                while True:
                    chunk = reader.read(_RAW_VERIFY_CHUNK_BYTES)
                    if not isinstance(chunk, bytes) or len(chunk) > _RAW_VERIFY_CHUNK_BYTES:
                        raise CanonicalProcessingError(
                            "raw processing payload violated the bounded binary-reader contract"
                        )
                    if not chunk:
                        break
                    digest.update(chunk)
                    byte_count += len(chunk)
        except OSError as error:
            raise CanonicalProcessingError(
                "raw processing payload could not be reopened"
            ) from error
        if (
            digest.hexdigest() != page.identity.content_sha256
            or byte_count != page.identity.byte_count
        ):
            raise CanonicalProcessingError("raw processing payload differs from its identity")
    batch_ids = tuple(page.batch.metadata.batch_id for page in pages)
    if len(batch_ids) != len(set(batch_ids)):
        raise CanonicalProcessingError("raw processing provenance reuses a batch UUID")


def _processing_signature(calendar_snapshot: CalendarSnapshot) -> ProcessingSignature:
    return ProcessingSignature(
        canonical_schema_version=CANONICAL_PRICE_BAR_SCHEMA_VERSION,
        normalizer_version=ALPACA_SIP_NORMALIZER_VERSION,
        validator_version=PRICE_BAR_VALIDATOR_VERSION,
        calendar_snapshot_checksum=calendar_snapshot.checksum,
        process_semantics=(
            IdentityDimension(name="calendar_slot_membership", value="exact"),
            IdentityDimension(name="daily_bar_semantics", value="regular_session"),
            IdentityDimension(
                name="validation_retention", value="flag_recoverable_block_duplicate"
            ),
        ),
    )


def _validate_processing_scope(
    specification: RequestSpecification,
    calendar_snapshot: CalendarSnapshot,
) -> None:
    if (specification.provider, specification.dataset) != ("alpaca", "price_bars_sip"):
        raise CanonicalProcessingError("processing requires exact historical Alpaca SIP bars")
    if (
        calendar_snapshot.calendar_name != "XNYS"
        or calendar_snapshot.timezone_name != "America/New_York"
    ):
        raise CanonicalProcessingError("Alpaca SIP RTH processing requires the XNYS calendar")


def prepare_alpaca_sip_batch_context(
    *,
    specification: RequestSpecification,
    pages: tuple[RawProcessingPage, ...],
    acquisition_authorization: AcquisitionPolicyAuthorization,
    calendar_snapshot: CalendarSnapshot,
    fixed_ingested_at: datetime,
    manifest_created_at: datetime,
) -> PreparedBatchContext:
    """Verify raw evidence and freeze identity/provenance before normalization starts."""

    _validate_processing_scope(specification, calendar_snapshot)
    specification_to_bar_request(specification)
    _validate_page_chain(specification, pages, acquisition_authorization)
    if acquisition_authorization.request.request_spec_hash != specification.request_spec_hash:
        raise CanonicalProcessingError("acquisition authorization belongs to another request")
    if fixed_ingested_at.tzinfo is None or fixed_ingested_at.utcoffset() is None:
        raise CanonicalProcessingError("fixed_ingested_at must be timezone-aware")
    if manifest_created_at.tzinfo is None or manifest_created_at.utcoffset() is None:
        raise CanonicalProcessingError("manifest_created_at must be timezone-aware")
    if manifest_created_at < fixed_ingested_at:
        raise CanonicalProcessingError("manifest creation time precedes fixed ingestion time")
    latest_retrieval = max(page.batch.metadata.retrieved_at for page in pages)
    if fixed_ingested_at < latest_retrieval:
        raise CanonicalProcessingError("fixed ingestion time precedes raw retrieval")

    context = BatchContext(
        batch_identity=CanonicalBatchIdentity(
            request_spec_hash=specification.request_spec_hash,
            ordered_artifacts=tuple(page.identity for page in pages),
            processing_signature=_processing_signature(calendar_snapshot),
        ),
        fixed_ingested_at=fixed_ingested_at,
        manifest_created_at=manifest_created_at,
    )
    provenance = CanonicalPublicationProvenance(
        source_id=pages[0].batch.metadata.source.source_id,
        raw_bindings=tuple(
            RawProvenanceBinding(
                artifact_id=page.identity.artifact_id,
                raw_batch_id=page.batch.metadata.batch_id,
            )
            for page in pages
        ),
    )
    if any(page.batch.metadata.source.source_id != provenance.source_id for page in pages):
        raise CanonicalProcessingError("raw page chain contains different source identities")
    return PreparedBatchContext(
        specification=specification,
        acquisition_authorization=acquisition_authorization,
        batch_context=context,
        provenance=provenance,
    )


def _validate_durable_batch_context(
    *,
    specification: RequestSpecification,
    pages: tuple[RawProcessingPage, ...],
    acquisition_authorization: AcquisitionPolicyAuthorization,
    calendar_snapshot: CalendarSnapshot,
    batch_context: BatchContext,
    provenance: CanonicalPublicationProvenance,
) -> None:
    _validate_processing_scope(specification, calendar_snapshot)
    _validate_page_chain(specification, pages, acquisition_authorization)
    expected_identity = CanonicalBatchIdentity(
        request_spec_hash=specification.request_spec_hash,
        ordered_artifacts=tuple(page.identity for page in pages),
        processing_signature=_processing_signature(calendar_snapshot),
    )
    if batch_context.batch_identity != expected_identity:
        raise CanonicalProcessingError(
            "durable batch context differs from exact raw/processing identity"
        )
    if batch_context.manifest_created_at < batch_context.fixed_ingested_at:
        raise CanonicalProcessingError("durable manifest time precedes fixed ingestion time")
    if batch_context.fixed_ingested_at < max(page.batch.metadata.retrieved_at for page in pages):
        raise CanonicalProcessingError("durable ingestion time precedes raw retrieval")
    expected_provenance = CanonicalPublicationProvenance(
        source_id=pages[0].batch.metadata.source.source_id,
        raw_bindings=tuple(
            RawProvenanceBinding(
                artifact_id=page.identity.artifact_id,
                raw_batch_id=page.batch.metadata.batch_id,
            )
            for page in pages
        ),
    )
    if provenance != expected_provenance:
        raise CanonicalProcessingError("durable provenance differs from exact raw chain")


def _canonical_partitions(frame: pl.DataFrame) -> tuple[CanonicalParquetPart, ...]:
    if frame.is_empty():
        return ()
    ordered = frame.sort(
        [
            "timeframe",
            "timestamp_start",
            "timestamp_end",
            "instrument_id",
            "source_id",
            "raw_batch_id",
        ]
    ).with_columns(
        pl.col("timestamp_start").dt.year().alias("__year"),
        pl.col("timestamp_start").dt.month().alias("__month"),
    )
    parts: list[CanonicalParquetPart] = []
    groups = ordered.partition_by(
        ["timeframe", "__year", "__month"],
        maintain_order=True,
        include_key=True,
    )
    for group in groups:
        timeframe = str(group.get_column("timeframe")[0])
        year = int(group.get_column("__year")[0])
        month = int(group.get_column("__month")[0])
        parts.append(
            CanonicalParquetPart(
                relative_path=(
                    f"timeframe={timeframe}/year={year:04d}/month={month:02d}/part-0000.parquet"
                ),
                frame=group.drop("__year", "__month"),
            )
        )
    return tuple(parts)


def prepare_alpaca_sip_canonical_batch_from_context(
    *,
    specification: RequestSpecification,
    pages: tuple[RawProcessingPage, ...],
    acquisition_authorization: AcquisitionPolicyAuthorization,
    calendar_snapshot: CalendarSnapshot,
    batch_context: BatchContext,
    provenance: CanonicalPublicationProvenance,
) -> PreparedCanonicalBatch:
    """Normalize using the exact context/provenance already committed to SQLite."""

    request = specification_to_bar_request(specification)
    _validate_durable_batch_context(
        specification=specification,
        pages=pages,
        acquisition_authorization=acquisition_authorization,
        calendar_snapshot=calendar_snapshot,
        batch_context=batch_context,
        provenance=provenance,
    )
    context = batch_context
    schedule = session_schedule_from_snapshot(calendar_snapshot)
    bars: list[PriceBar] = []
    normalization_issues: list[NormalizationIssue] = []
    for page in pages:
        try:
            result = normalize_alpaca_bars(
                page.batch,
                request,
                ingested_at=context.fixed_ingested_at,
                session_schedule=schedule,
                daily_semantics=DailyBarSemantics.REGULAR_SESSION,
            )
        except (NormalizationError, ValueError) as error:
            raise CanonicalProcessingError(
                "raw Alpaca page failed deterministic normalization"
            ) from error
        bars.extend(result.bars)
        normalization_issues.extend(result.issues)

    fatal_normalization = tuple(
        issue
        for issue in normalization_issues
        if issue.severity is NormalizationSeverity.ERROR
        or issue.code not in _NON_FATAL_NORMALIZATION_CODES
    )
    frame = price_bars_to_frame(bars)
    validated = validate_bars(frame) if not frame.is_empty() else None
    validated_frame = frame if validated is None else validated.frame
    blocking_instruments: dict[str, set[str]] = defaultdict(set)
    quality_codes: set[str] = set()
    if validated is not None:
        for issue in validated.issues:
            code = f"QUALITY:{issue.code.value.upper()}"
            quality_codes.add(code)
            if issue.code in _BLOCKING_QUALITY_FLAGS:
                blocking_instruments[issue.instrument_id].add(code)

    expected_intervals = {
        (slot.start_utc, slot.end_utc)
        for slot in calendar_snapshot.expected_slots(specification.timeframe)
    }
    for instrument_id, timestamp_start, timestamp_end in validated_frame.select(
        "instrument_id", "timestamp_start", "timestamp_end"
    ).iter_rows():
        if (timestamp_start, timestamp_end) not in expected_intervals:
            blocking_instruments[str(instrument_id)].add("CALENDAR:UNEXPECTED_SLOT")

    global_blocking_codes = {
        f"NORMALIZATION:{issue.code.value.upper()}" for issue in fatal_normalization
    }
    outcomes: list[CanonicalStreamOutcome] = []
    publishable_frames: list[pl.DataFrame] = []
    for stream in sorted(specification.stream_keys(), key=lambda value: value.stream_id):
        instrument_text = str(stream.instrument_id)
        stream_frame = validated_frame.filter(pl.col("instrument_id") == instrument_text)
        blocking_codes = set(global_blocking_codes) | blocking_instruments.get(
            instrument_text, set()
        )
        if blocking_codes or stream_frame.is_empty():
            if not blocking_codes:
                blocking_codes.add("NO_CANONICAL_ROWS")
            outcomes.append(
                CanonicalStreamOutcome(
                    stream=stream,
                    outcome=StreamPublicationOutcome.BLOCKED,
                    request_start=specification.start,
                    request_end=specification.end,
                    row_count=0,
                    validation_codes=tuple(sorted(blocking_codes)),
                )
            )
            continue
        publishable_frames.append(stream_frame)
        observed_start = stream_frame.get_column("timestamp_start").min()
        observed_end = stream_frame.get_column("timestamp_end").max()
        if not isinstance(observed_start, datetime) or not isinstance(observed_end, datetime):
            raise CanonicalProcessingError("canonical stream bounds are not datetimes")
        stream_quality = tuple(
            sorted(
                {
                    f"QUALITY:{flag.upper()}"
                    for flags in stream_frame.get_column("quality_flags").to_list()
                    for flag in flags
                }
            )
        )
        outcomes.append(
            CanonicalStreamOutcome(
                stream=stream,
                outcome=StreamPublicationOutcome.PUBLISHABLE,
                request_start=specification.start,
                request_end=specification.end,
                row_count=stream_frame.height,
                observed_start=observed_start,
                observed_end=observed_end,
                validation_codes=stream_quality,
            )
        )

    publishable = (
        pl.concat(publishable_frames, how="vertical")
        if publishable_frames
        else validated_frame.clear()
    )
    parts = _canonical_partitions(publishable)
    ordered_outcomes = tuple(sorted(outcomes, key=lambda value: value.stream_id))
    expectation = CanonicalBatchExpectation(
        specification=specification,
        batch_context=context,
        calendar_snapshot=calendar_snapshot,
        provenance=provenance,
        streams=ordered_outcomes,
    )
    return PreparedCanonicalBatch(
        specification=specification,
        acquisition_authorization=acquisition_authorization,
        batch_context=context,
        expectation=expectation,
        parts=parts,
        stream_outcomes=ordered_outcomes,
        provenance=provenance,
        normalization_issue_codes=tuple(
            sorted({issue.code.value for issue in normalization_issues})
        ),
        quality_issue_codes=tuple(sorted(quality_codes)),
    )


def prepare_alpaca_sip_canonical_batch(
    *,
    specification: RequestSpecification,
    pages: tuple[RawProcessingPage, ...],
    acquisition_authorization: AcquisitionPolicyAuthorization,
    calendar_snapshot: CalendarSnapshot,
    fixed_ingested_at: datetime,
    manifest_created_at: datetime,
) -> PreparedCanonicalBatch:
    """Compatibility helper; durable services must persist between its two explicit phases."""

    prepared_context = prepare_alpaca_sip_batch_context(
        specification=specification,
        pages=pages,
        acquisition_authorization=acquisition_authorization,
        calendar_snapshot=calendar_snapshot,
        fixed_ingested_at=fixed_ingested_at,
        manifest_created_at=manifest_created_at,
    )
    return prepare_alpaca_sip_canonical_batch_from_context(
        specification=prepared_context.specification,
        pages=pages,
        acquisition_authorization=prepared_context.acquisition_authorization,
        calendar_snapshot=calendar_snapshot,
        batch_context=prepared_context.batch_context,
        provenance=prepared_context.provenance,
    )


__all__ = [
    "ALPACA_SIP_NORMALIZER_VERSION",
    "CANONICAL_PRICE_BAR_SCHEMA_VERSION",
    "PRICE_BAR_VALIDATOR_VERSION",
    "CanonicalProcessingError",
    "PreparedBatchContext",
    "PreparedCanonicalBatch",
    "RawProcessingPage",
    "prepare_alpaca_sip_batch_context",
    "prepare_alpaca_sip_canonical_batch",
    "prepare_alpaca_sip_canonical_batch_from_context",
    "processing_pages_from_acquisition",
    "session_schedule_from_snapshot",
]
