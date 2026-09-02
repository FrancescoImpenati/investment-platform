"""Offline tests for Phase 2 raw and canonical filesystem publication."""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import polars as pl
import pytest

from investment_platform.data.calendar import CalendarSession, CalendarSnapshot
from investment_platform.data.ingestion.identity import (
    BarSemantics,
    BatchContext,
    CanonicalBatchIdentity,
    DataKind,
    ProcessingSignature,
    ProviderInstrumentMapping,
    RawArtifactIdentity,
    RequestSpecification,
)
from investment_platform.data.models import (
    AdjustmentState,
    PriceBar,
    Timeframe,
    TradingSession,
)
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    DatasetPolicyDenied,
    ResponsePageAuthorization,
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage import (
    CanonicalBatchExpectation,
    CanonicalBatchManifest,
    CanonicalBatchPublisher,
    CanonicalParquetPart,
    CanonicalPublicationProvenance,
    CanonicalStreamOutcome,
    FaultInjector,
    PublicationCollisionError,
    PublicationFaultPoint,
    PublicationIntegrityError,
    PublicationRecoveryInspector,
    PublishedCanonicalBatch,
    PublishedRawArtifact,
    RawArtifactPublisher,
    RawProvenanceBinding,
    RecoveryInspectionState,
    StreamPublicationOutcome,
    price_bars_to_frame,
)
from investment_platform.data_root import (
    PrivateDataRoot,
    PrivateDataRootSentinelError,
    PrivateRootSentinel,
)
from investment_platform.runtime import RuntimeEnvironment

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
_START = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
_END = datetime(2026, 8, 28, 14, 0, tzinfo=UTC)
_INGESTED_AT = datetime(2026, 8, 31, 12, 1, tzinfo=UTC)
_MANIFEST_AT = datetime(2026, 8, 31, 12, 2, tzinfo=UTC)
_PAYLOAD = b'{"bars":[],"fixture":"synthetic"}'
_INSTRUMENT_ID = UUID("00000000-0000-4000-8000-000000000001")
_SOURCE_ID = UUID("00000000-0000-4000-8000-000000000002")
_RAW_BATCH_ID = UUID("00000000-0000-4000-8000-000000000003")


class InjectedCrash(RuntimeError):
    pass


@pytest.fixture
def private_root(
    tmp_path: Path,
) -> tuple[PrivateDataRoot, PrivateRootSentinel]:
    repository_root = Path(__file__).parents[2].resolve(strict=True)
    root = PrivateDataRoot(
        tmp_path.parent / f"p2-private-{uuid4().hex[:8]}",
        repository_root,
        allow_temporary_for_tests=True,
    )
    sentinel = root.initialize(created_at=_NOW)
    return root, sentinel


def _enforcer() -> RetentionPolicyEnforcer:
    return RetentionPolicyEnforcer(
        RetentionPolicyCatalog.load_default(),
        clock=lambda: _NOW,
    )


def _request() -> RequestSpecification:
    return RequestSpecification(
        provider="synthetic",
        dataset="price_bars",
        data_kind=DataKind.PRICE_BAR,
        instrument_mappings=(
            ProviderInstrumentMapping(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="SYNTHETIC",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        session=TradingSession.REGULAR,
        adjustment=AdjustmentState.UNADJUSTED,
        currency="USD",
        bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
        start=_START,
        end=_END,
        mapping_semantic_version="synthetic-bars-v1",
    )


def _authorizations(
    enforcer: RetentionPolicyEnforcer,
    specification: RequestSpecification,
    payload: bytes = _PAYLOAD,
) -> tuple[ResponsePageAuthorization, AcquisitionPolicyAuthorization]:
    request = enforcer.authorize_request(
        specification.provider,
        specification.dataset,
        environment=RuntimeEnvironment.TEST,
        start=specification.start,
        end=specification.end,
        request_spec_hash=specification.request_spec_hash,
    )
    page = enforcer.authorize_response_page(
        request,
        page_ordinal=0,
        page_relation="root",
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        payload_size_bytes=len(payload),
        canonical_media_type="application/json",
        content_encoding="identity",
        observed_start=specification.start,
        observed_end=specification.end,
    )
    acquisition = enforcer.authorize_completed_acquisition(
        request,
        (page,),
        pagination_complete=True,
        terminal_page_verified=True,
    )
    return page, acquisition


def _raw_identity(specification: RequestSpecification) -> RawArtifactIdentity:
    return RawArtifactIdentity.from_bytes(
        specification,
        page_ordinal=0,
        media_type="application/json",
        content_encoding="identity",
        payload=_PAYLOAD,
    )


def _calendar() -> CalendarSnapshot:
    return CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="1",
        tzdata_version="synthetic",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=_START.date(),
        range_end=_START.date() + timedelta(days=1),
        generated_at=_NOW,
        sessions=(
            CalendarSession(
                session_date=_START.date(),
                open_utc=_START,
                close_utc=datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
            ),
        ),
    )


def _batch_context(specification: RequestSpecification) -> BatchContext:
    return BatchContext(
        batch_identity=CanonicalBatchIdentity(
            request_spec_hash=specification.request_spec_hash,
            ordered_artifacts=(_raw_identity(specification),),
            processing_signature=ProcessingSignature(
                canonical_schema_version="price-bar-v1",
                normalizer_version="synthetic-normalizer-v1",
                validator_version="bar-validator-v1",
                calendar_snapshot_checksum=_calendar().checksum,
            ),
        ),
        fixed_ingested_at=_INGESTED_AT,
        manifest_created_at=_MANIFEST_AT,
    )


def _frame(*, close_delta: float = 0.0) -> pl.DataFrame:
    bars = []
    for ordinal in range(2):
        start = _START + timedelta(minutes=5 * ordinal)
        bars.append(
            PriceBar(
                instrument_id=_INSTRUMENT_ID,
                timeframe=Timeframe.FIVE_MINUTES,
                timestamp_start=start,
                timestamp_end=start + timedelta(minutes=5),
                open=100.0 + ordinal,
                high=101.0 + ordinal,
                low=99.0 + ordinal,
                close=100.5 + ordinal + close_delta,
                volume=1_000.0 + ordinal,
                vwap=100.25 + ordinal,
                currency="USD",
                session=TradingSession.REGULAR,
                adjustment_state=AdjustmentState.UNADJUSTED,
                source_id=_SOURCE_ID,
                raw_batch_id=_RAW_BATCH_ID,
                provider_record_id=f"synthetic-{ordinal}",
                retrieved_at=_INGESTED_AT - timedelta(minutes=1),
                ingested_at=_INGESTED_AT,
                quality_flags=(),
            )
        )
    return price_bars_to_frame(bars)


def _parts(*, close_delta: float = 0.0) -> tuple[CanonicalParquetPart, ...]:
    frame = _frame(close_delta=close_delta)
    return (
        CanonicalParquetPart(
            relative_path="timeframe=5m/year=2026/month=08/part-0000.parquet",
            frame=frame.slice(0, 1),
        ),
        CanonicalParquetPart(
            relative_path="timeframe=5m/year=2026/month=08/part-0001.parquet",
            frame=frame.slice(1, 1),
        ),
    )


def _part_path(ordinal: int) -> str:
    return f"timeframe=5m/year=2026/month=08/part-{ordinal:04d}.parquet"


def _outcomes(specification: RequestSpecification) -> tuple[CanonicalStreamOutcome, ...]:
    stream = specification.stream_keys()[0]
    return (
        CanonicalStreamOutcome(
            stream=stream,
            outcome=StreamPublicationOutcome.PUBLISHABLE,
            request_start=specification.start,
            request_end=specification.end,
            row_count=2,
            observed_start=_START,
            observed_end=_START + timedelta(minutes=10),
        ),
    )


def _provenance(context: BatchContext) -> CanonicalPublicationProvenance:
    return CanonicalPublicationProvenance(
        source_id=_SOURCE_ID,
        raw_bindings=tuple(
            RawProvenanceBinding(
                artifact_id=artifact.artifact_id,
                raw_batch_id=_RAW_BATCH_ID,
            )
            for artifact in context.batch_identity.ordered_artifacts
        ),
    )


def _expectation(
    specification: RequestSpecification,
    context: BatchContext,
) -> CanonicalBatchExpectation:
    return CanonicalBatchExpectation(
        specification=specification,
        batch_context=context,
        calendar_snapshot=_calendar(),
        provenance=_provenance(context),
        streams=_outcomes(specification),
    )


def _manifest(root: PrivateDataRoot, published: PublishedCanonicalBatch) -> CanonicalBatchManifest:
    return CanonicalBatchManifest.model_validate_json(
        (root.root / published.manifest_relative_path).read_bytes()
    )


def _publish_canonical(
    publisher: CanonicalBatchPublisher,
    specification: RequestSpecification,
    context: BatchContext,
    parts: tuple[CanonicalParquetPart, ...],
    outcomes: tuple[CanonicalStreamOutcome, ...],
    acquisition: AcquisitionPolicyAuthorization,
    *,
    fault_injector: FaultInjector | None = None,
) -> PublishedCanonicalBatch | None:
    return publisher.publish(
        specification,
        context,
        parts,
        outcomes,
        authorization=acquisition,
        calendar_snapshot=_calendar(),
        provenance=_provenance(context),
        fault_injector=fault_injector,
    )


def _publish_raw(
    root: PrivateDataRoot,
    enforcer: RetentionPolicyEnforcer,
    specification: RequestSpecification,
    authorization: ResponsePageAuthorization,
    *,
    fault_injector: FaultInjector | None = None,
) -> PublishedRawArtifact:
    publisher = RawArtifactPublisher(root, enforcer, chunk_size=7)
    return publisher.publish(
        specification,
        io.BytesIO(_PAYLOAD),
        page_ordinal=0,
        media_type="application/json",
        content_encoding="identity",
        authorization=authorization,
        first_persisted_at=_INGESTED_AT,
        fault_injector=fault_injector,
    )


def test_raw_streaming_publication_is_content_identified_and_exact_replay_is_noop(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, sentinel = private_root
    enforcer = _enforcer()
    specification = _request()
    page, _ = _authorizations(enforcer, specification)
    publisher = RawArtifactPublisher(root, enforcer, chunk_size=7)

    first = publisher.publish(
        specification,
        io.BytesIO(_PAYLOAD),
        page_ordinal=0,
        media_type="application/json",
        content_encoding="identity",
        authorization=page,
        first_persisted_at=_INGESTED_AT,
    )
    second = publisher.publish(
        specification,
        io.BytesIO(_PAYLOAD),
        page_ordinal=0,
        media_type="application/json",
        content_encoding="identity",
        authorization=page,
        first_persisted_at=_INGESTED_AT + timedelta(hours=1),
    )

    assert first.created is True
    assert second.created is False
    assert first.artifact_id == _raw_identity(specification).artifact_id
    assert second.first_persisted_at == _INGESTED_AT
    assert first.root_id == sentinel.root_id
    assert first.content_sha256 == hashlib.sha256(_PAYLOAD).hexdigest()
    assert (root.root / first.payload_relative_path).read_bytes() == _PAYLOAD
    assert first.relative_directory.startswith("raw/provider=synthetic/dataset=price_bars/")
    manifest = json.loads((root.root / first.manifest_relative_path).read_text(encoding="utf-8"))
    assert manifest["identity"] == {
        "byte_count": len(_PAYLOAD),
        "content_encoding": "identity",
        "content_sha256": hashlib.sha256(_PAYLOAD).hexdigest(),
        "media_type": "application/json",
        "page_ordinal": 0,
        "page_relation": "root",
        "request_spec_hash": specification.request_spec_hash,
    }
    assert not any((root.root / "operational").iterdir())
    assert PublicationRecoveryInspector(root).inspect_staging() == ()


def test_raw_authorization_must_match_exact_streamed_bytes_and_denial_cleans_candidate(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer = _enforcer()
    specification = _request()
    page, _ = _authorizations(enforcer, specification)

    with pytest.raises(DatasetPolicyDenied, match="differ"):
        RawArtifactPublisher(root, enforcer).publish(
            specification,
            io.BytesIO(b"different synthetic bytes"),
            page_ordinal=0,
            media_type="application/json",
            content_encoding="identity",
            authorization=page,
            first_persisted_at=_INGESTED_AT,
        )

    assert not any(path.is_file() for path in (root.root / "raw").rglob("*"))
    assert PublicationRecoveryInspector(root).inspect_staging() == ()


def test_corrupt_existing_raw_artifact_fails_closed_as_collision(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer = _enforcer()
    specification = _request()
    page, _ = _authorizations(enforcer, specification)
    first = _publish_raw(root, enforcer, specification, page)
    payload_path = root.root / first.payload_relative_path
    payload_path.write_bytes(b"corrupt")

    with pytest.raises(PublicationCollisionError, match="incomplete or invalid"):
        _publish_raw(root, enforcer, specification, page)


@pytest.mark.parametrize(
    "point",
    [
        PublicationFaultPoint.STAGING,
        PublicationFaultPoint.RAW_WRITE,
        PublicationFaultPoint.MANIFEST,
        PublicationFaultPoint.STAGED_MANIFEST_VERIFIED,
        PublicationFaultPoint.RENAME,
        PublicationFaultPoint.REOPEN,
    ],
)
def test_raw_fault_boundaries_leave_recoverable_state_and_retry_converges(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
    point: PublicationFaultPoint,
) -> None:
    root, _ = private_root
    enforcer = _enforcer()
    specification = _request()
    page, _ = _authorizations(enforcer, specification)

    def fail(selected: PublicationFaultPoint) -> None:
        if selected is point:
            raise InjectedCrash(point.value)

    with pytest.raises(InjectedCrash, match=point.value):
        _publish_raw(root, enforcer, specification, page, fault_injector=fail)

    before_retry = PublicationRecoveryInspector(root).inspect_staging()
    if point in {
        PublicationFaultPoint.STAGING,
        PublicationFaultPoint.RAW_WRITE,
        PublicationFaultPoint.MANIFEST,
        PublicationFaultPoint.STAGED_MANIFEST_VERIFIED,
    }:
        assert len(before_retry) == 1
        expected_state = (
            RecoveryInspectionState.COMPLETE
            if point
            in {
                PublicationFaultPoint.MANIFEST,
                PublicationFaultPoint.STAGED_MANIFEST_VERIFIED,
            }
            else RecoveryInspectionState.INCOMPLETE
        )
        assert before_retry[0].state is expected_state
    else:
        assert before_retry == ()

    recovered = _publish_raw(root, enforcer, specification, page)
    assert recovered.created is (
        point not in {PublicationFaultPoint.RENAME, PublicationFaultPoint.REOPEN}
    )
    assert PublicationRecoveryInspector(root).inspect_staging() == ()


def test_sentinel_replacement_after_open_blocks_raw_mutation(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer = _enforcer()
    specification = _request()
    page, _ = _authorizations(enforcer, specification)
    publisher = RawArtifactPublisher(root, enforcer)
    value = json.loads(root.sentinel_path.read_text(encoding="utf-8"))
    value["root_id"] = str(uuid4())
    root.sentinel_path.write_text(
        json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(PrivateDataRootSentinelError, match="root ID changed"):
        publisher.publish(
            specification,
            io.BytesIO(_PAYLOAD),
            page_ordinal=0,
            media_type="application/json",
            content_encoding="identity",
            authorization=page,
            first_persisted_at=_INGESTED_AT,
        )


def _prepared_canonical(
    root: PrivateDataRoot,
) -> tuple[
    RetentionPolicyEnforcer,
    RequestSpecification,
    AcquisitionPolicyAuthorization,
    BatchContext,
]:
    enforcer = _enforcer()
    specification = _request()
    page, acquisition = _authorizations(enforcer, specification)
    _publish_raw(root, enforcer, specification, page)
    return enforcer, specification, acquisition, _batch_context(specification)


def test_canonical_batch_is_manifest_last_atomic_verified_and_exact_replay_is_noop(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, sentinel = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    publisher = CanonicalBatchPublisher(root, enforcer)

    first = _publish_canonical(
        publisher,
        specification,
        context,
        _parts(),
        _outcomes(specification),
        acquisition,
    )
    second = _publish_canonical(
        publisher,
        specification,
        context,
        _parts(),
        _outcomes(specification),
        acquisition,
    )

    assert first is not None
    assert second is not None
    assert first.created is True
    assert second.created is False
    assert first.root_id == sentinel.root_id
    assert first.row_count == 2
    assert len(first.files) == 2
    assert first.relative_directory.startswith(
        "normalized/price_bars/provider=synthetic/dataset=price_bars/batches/"
    )
    manifest = json.loads((root.root / first.manifest_relative_path).read_text(encoding="utf-8"))
    assert manifest["fixed_ingested_at"] == _INGESTED_AT.isoformat().replace("+00:00", "Z")
    assert manifest["manifest_created_at"] == _MANIFEST_AT.isoformat().replace("+00:00", "Z")
    assert manifest["row_count"] == 2
    assert not any((root.root / "operational").iterdir())
    inspection = PublicationRecoveryInspector(root).inspect_published_batch(
        specification.provider,
        specification.dataset,
        context.canonical_batch_id,
        expected_semantics=_expectation(specification, context),
        expected_manifest=_manifest(root, first),
    )
    assert inspection.state is RecoveryInspectionState.CONTENT_VERIFIED_PUBLISHED
    assert inspection.identity == context.canonical_batch_id


def test_same_batch_identity_with_different_canonical_values_is_collision(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    publisher = CanonicalBatchPublisher(root, enforcer)
    _publish_canonical(
        publisher,
        specification,
        context,
        _parts(),
        _outcomes(specification),
        acquisition,
    )

    with pytest.raises(PublicationCollisionError, match="conflicting metadata"):
        _publish_canonical(
            publisher,
            specification,
            context,
            _parts(close_delta=0.25),
            _outcomes(specification),
            acquisition,
        )


def test_canonical_requires_immutable_raw_input_first(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer = _enforcer()
    specification = _request()
    _, acquisition = _authorizations(enforcer, specification)

    with pytest.raises(PublicationIntegrityError, match="missing"):
        context = _batch_context(specification)
        _publish_canonical(
            CanonicalBatchPublisher(root, enforcer),
            specification,
            context,
            _parts(),
            _outcomes(specification),
            acquisition,
        )

    assert not any(path.is_file() for path in (root.root / "normalized").rglob("*"))


def test_canonical_rejects_wrong_schema_and_volatile_ingested_at(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    publisher = CanonicalBatchPublisher(root, enforcer)
    wrong_schema = _frame().select("timestamp_start", "timestamp_end", "ingested_at")
    wrong_time = _frame().with_columns(
        pl.lit(_INGESTED_AT + timedelta(seconds=1)).alias("ingested_at")
    )

    with pytest.raises(PublicationIntegrityError, match="schema"):
        _publish_canonical(
            publisher,
            specification,
            context,
            (CanonicalParquetPart(relative_path="part-0000.parquet", frame=wrong_schema),),
            _outcomes(specification),
            acquisition,
        )
    with pytest.raises(PublicationIntegrityError, match="BatchContext"):
        _publish_canonical(
            publisher,
            specification,
            context,
            (CanonicalParquetPart(relative_path="part-0000.parquet", frame=wrong_time),),
            _outcomes(specification),
            acquisition,
        )


@pytest.mark.parametrize(
    "point",
    [
        PublicationFaultPoint.STAGING,
        PublicationFaultPoint.MANIFEST,
        PublicationFaultPoint.STAGED_MANIFEST_VERIFIED,
        PublicationFaultPoint.RENAME,
        PublicationFaultPoint.REOPEN,
    ],
)
def test_canonical_fault_boundaries_are_inspectable_and_retry_is_idempotent(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
    point: PublicationFaultPoint,
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    publisher = CanonicalBatchPublisher(root, enforcer)

    def fail(selected: PublicationFaultPoint) -> None:
        if selected is point:
            raise InjectedCrash(point.value)

    with pytest.raises(InjectedCrash, match=point.value):
        _publish_canonical(
            publisher,
            specification,
            context,
            _parts(),
            _outcomes(specification),
            acquisition,
            fault_injector=fail,
        )

    staged = PublicationRecoveryInspector(root).inspect_staging()
    canonical_staged = [item for item in staged if "canonical-batches" in item.relative_directory]
    if point in {
        PublicationFaultPoint.STAGING,
        PublicationFaultPoint.MANIFEST,
        PublicationFaultPoint.STAGED_MANIFEST_VERIFIED,
    }:
        assert len(canonical_staged) == 1
        expected = (
            RecoveryInspectionState.COMPLETE
            if point
            in {
                PublicationFaultPoint.MANIFEST,
                PublicationFaultPoint.STAGED_MANIFEST_VERIFIED,
            }
            else RecoveryInspectionState.INCOMPLETE
        )
        assert canonical_staged[0].state is expected
    else:
        assert canonical_staged == []

    recovered = _publish_canonical(
        publisher,
        specification,
        context,
        _parts(),
        _outcomes(specification),
        acquisition,
    )
    assert recovered is not None
    assert recovered.created is (
        point not in {PublicationFaultPoint.RENAME, PublicationFaultPoint.REOPEN}
    )
    published = PublicationRecoveryInspector(root).inspect_published_batch(
        specification.provider,
        specification.dataset,
        context.canonical_batch_id,
        expected_semantics=_expectation(specification, context),
        expected_manifest=_manifest(root, recovered),
    )
    assert published.state is RecoveryInspectionState.CONTENT_VERIFIED_PUBLISHED
    remaining = PublicationRecoveryInspector(root).inspect_staging()
    assert not [item for item in remaining if "canonical-batches" in item.relative_directory]


@pytest.mark.parametrize(
    ("crash_on_staging_call", "expected_part_count"),
    [(2, 1), (3, 2)],
)
def test_fault_during_staged_part_proves_manifest_is_written_last(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
    crash_on_staging_call: int,
    expected_part_count: int,
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    staging_calls = 0

    def fail_after_first_part(point: PublicationFaultPoint) -> None:
        nonlocal staging_calls
        if point is PublicationFaultPoint.STAGING:
            staging_calls += 1
            if staging_calls == crash_on_staging_call:
                raise InjectedCrash("after-part")

    with pytest.raises(InjectedCrash, match="after-part"):
        _publish_canonical(
            CanonicalBatchPublisher(root, enforcer),
            specification,
            context,
            _parts(),
            _outcomes(specification),
            acquisition,
            fault_injector=fail_after_first_part,
        )

    staged = [
        item
        for item in PublicationRecoveryInspector(root).inspect_staging()
        if "canonical-batches" in item.relative_directory
    ]
    assert len(staged) == 1
    assert staged[0].state is RecoveryInspectionState.INCOMPLETE
    assert staged[0].error_code == "MANIFEST_MISSING"
    candidate = root.root / staged[0].relative_directory
    assert len(tuple(candidate.rglob("*.parquet"))) == expected_part_count


def test_recovery_marks_tampered_published_batch_invalid(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    published = _publish_canonical(
        CanonicalBatchPublisher(root, enforcer),
        specification,
        context,
        _parts(),
        _outcomes(specification),
        acquisition,
    )
    assert published is not None
    batch_directory = root.root / published.relative_directory
    first_part = batch_directory / published.files[0].relative_path
    first_part.write_bytes(b"not parquet")

    inspection = PublicationRecoveryInspector(root).inspect_published_batch(
        specification.provider,
        specification.dataset,
        context.canonical_batch_id,
    )
    assert inspection.state is RecoveryInspectionState.INVALID
    assert inspection.error_code == "INTEGRITY_FAILED"


@pytest.mark.parametrize(
    ("media_type", "content_encoding", "message"),
    [
        ("text/plain", "identity", "media type"),
        ("application/json", "gzip", "content encoding"),
    ],
)
def test_raw_authorization_binds_the_exact_stored_representation(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
    media_type: str,
    content_encoding: str,
    message: str,
) -> None:
    root, _ = private_root
    enforcer = _enforcer()
    specification = _request()
    page, _ = _authorizations(enforcer, specification)

    with pytest.raises(DatasetPolicyDenied, match=message):
        RawArtifactPublisher(root, enforcer).publish(
            specification,
            io.BytesIO(_PAYLOAD),
            page_ordinal=0,
            media_type=media_type,
            content_encoding=content_encoding,
            authorization=page,
            first_persisted_at=_INGESTED_AT,
        )

    assert not any(path.is_file() for path in (root.root / "raw").rglob("*"))
    assert not (root.root / "staging" / "raw-artifacts").exists()


def test_canonical_authorization_binds_full_ordered_raw_identities(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    changed = acquisition.ordered_artifacts[0].model_copy(update={"content_encoding": "gzip"})
    mismatched = acquisition.model_copy(update={"ordered_artifacts": (changed,)})

    with pytest.raises(DatasetPolicyDenied, match="raw inputs differ"):
        _publish_canonical(
            CanonicalBatchPublisher(root, enforcer),
            specification,
            context,
            _parts(),
            _outcomes(specification),
            mismatched,
        )

    assert not any(path.is_file() for path in (root.root / "normalized").rglob("*"))


def test_all_blocked_streams_require_codes_and_skip_canonical_publication(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    stream = specification.stream_keys()[0]
    blocked = CanonicalStreamOutcome(
        stream=stream,
        outcome=StreamPublicationOutcome.BLOCKED,
        request_start=specification.start,
        request_end=specification.end,
        row_count=0,
        validation_codes=("FATAL_PROVIDER_SHAPE",),
    )

    publisher = CanonicalBatchPublisher(root, enforcer)
    result = publisher.publish(
        specification,
        context,
        (),
        (blocked,),
        authorization=acquisition,
        calendar_snapshot=_calendar(),
        provenance=None,
    )

    assert result is None
    assert not any(path.is_file() for path in (root.root / "normalized").rglob("*"))
    with pytest.raises(ValueError, match="validation code"):
        CanonicalStreamOutcome(
            stream=stream,
            outcome=StreamPublicationOutcome.BLOCKED,
            request_start=specification.start,
            request_end=specification.end,
            row_count=0,
        )


@pytest.mark.parametrize("existing_effect", ["published", "staging"])
def test_all_blocked_result_rejects_same_identity_filesystem_effects(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
    existing_effect: str,
) -> None:
    root, sentinel = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    publisher = CanonicalBatchPublisher(root, enforcer)
    if existing_effect == "published":
        result = _publish_canonical(
            publisher,
            specification,
            context,
            _parts(),
            _outcomes(specification),
            acquisition,
        )
        assert result is not None
    else:
        staging = root.ensure_directory(
            "staging/canonical-batches",
            expected_root_id=sentinel.root_id,
        )
        (staging / f"batch={context.batch_identity.batch_hash}.manual.tmp").mkdir()
    blocked = CanonicalStreamOutcome(
        stream=specification.stream_keys()[0],
        outcome=StreamPublicationOutcome.BLOCKED,
        request_start=specification.start,
        request_end=specification.end,
        row_count=0,
        validation_codes=("FATAL_PROVIDER_SHAPE",),
    )

    with pytest.raises(PublicationCollisionError, match="all-blocked result conflicts"):
        publisher.publish(
            specification,
            context,
            (),
            (blocked,),
            authorization=acquisition,
            calendar_snapshot=_calendar(),
            provenance=None,
        )
    mismatched_bounds = blocked.model_copy(
        update={"request_start": specification.start + timedelta(minutes=5)}
    )
    with pytest.raises(ValueError, match="different request bounds"):
        CanonicalBatchExpectation(
            specification=specification,
            batch_context=context,
            calendar_snapshot=_calendar(),
            provenance=_provenance(context),
            streams=(mismatched_bounds,),
        )
    with pytest.raises(PublicationIntegrityError, match="bounds differ"):
        publisher.publish(
            specification,
            context,
            (),
            (mismatched_bounds,),
            authorization=acquisition,
            calendar_snapshot=_calendar(),
            provenance=None,
        )


def test_canonical_rejects_duplicate_observation_identity_across_parts(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    duplicated = _frame().slice(0, 1)
    parts = (
        CanonicalParquetPart(relative_path=_part_path(0), frame=duplicated),
        CanonicalParquetPart(relative_path=_part_path(1), frame=duplicated),
    )

    with pytest.raises(PublicationIntegrityError, match="duplicated across batch parts"):
        _publish_canonical(
            CanonicalBatchPublisher(root, enforcer),
            specification,
            context,
            parts,
            _outcomes(specification),
            acquisition,
        )


def test_canonical_rejects_nonfinite_null_dimension_time_and_provenance_mismatches(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    base = _frame()
    cases = (
        (base.with_columns(pl.lit(float("nan")).alias("close")), "finite"),
        (
            base.with_columns(pl.lit(None, dtype=pl.Float64).alias("open")),
            "nulls",
        ),
        (base.with_columns(pl.lit("1d").alias("timeframe")), "timeframe partition"),
        (base.with_columns(pl.lit("ALL").alias("session")), "dimensions"),
        (
            base.with_columns(pl.lit("split_adjusted").alias("adjustment_state")),
            "dimensions",
        ),
        (base.with_columns(pl.lit("EUR").alias("currency")), "dimensions"),
        (base.with_columns(pl.lit(str(uuid4())).alias("source_id")), "source provenance"),
        (base.with_columns(pl.lit(str(uuid4())).alias("raw_batch_id")), "raw provenance"),
        (
            base.with_columns(
                (pl.col("timestamp_end") + timedelta(minutes=1)).alias("timestamp_end")
            ),
            "calendar-eligible slot",
        ),
        (
            base.with_columns(pl.lit(_INGESTED_AT + timedelta(seconds=1)).alias("available_at")),
            "availability time",
        ),
        (
            base.with_columns(pl.lit(_START).alias("retrieved_at")),
            "retrieval precedes",
        ),
        (base.with_columns(pl.lit(-1.0).alias("close")), "canonical validator"),
    )
    publisher = CanonicalBatchPublisher(root, enforcer)
    for ordinal, (frame, message) in enumerate(cases):
        with pytest.raises(PublicationIntegrityError, match=message):
            _publish_canonical(
                publisher,
                specification,
                context,
                (
                    CanonicalParquetPart(
                        relative_path=_part_path(100 + ordinal),
                        frame=frame,
                    ),
                ),
                _outcomes(specification),
                acquisition,
            )


def test_invalid_complete_canonical_stage_is_rebuilt_and_removed_on_retry(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    publisher = CanonicalBatchPublisher(root, enforcer)

    def crash_after_manifest(point: PublicationFaultPoint) -> None:
        if point is PublicationFaultPoint.MANIFEST:
            raise InjectedCrash(point.value)

    with pytest.raises(InjectedCrash, match="manifest"):
        _publish_canonical(
            publisher,
            specification,
            context,
            _parts(),
            _outcomes(specification),
            acquisition,
            fault_injector=crash_after_manifest,
        )
    staged = PublicationRecoveryInspector(root).inspect_staging()
    candidate = root.root / staged[0].relative_directory
    (candidate / "manifest.json").write_text("{}\n", encoding="utf-8")

    recovered = _publish_canonical(
        publisher,
        specification,
        context,
        _parts(),
        _outcomes(specification),
        acquisition,
    )

    assert recovered is not None and recovered.created is True
    assert PublicationRecoveryInspector(root).inspect_staging() == ()


def _replace_with_symlink(path: Path, target: Path, *, directory: bool = False) -> None:
    if path.exists():
        path.unlink()
    try:
        path.symlink_to(target, target_is_directory=directory)
    except OSError as error:
        pytest.skip(f"host cannot create a test symlink: {error}")


def _create_windows_junction(link: Path, target: Path) -> None:
    if os.name != "nt" or not hasattr(Path, "is_junction"):
        pytest.skip("Windows junction semantics are unavailable")
    command = (
        "& { param([string]$Link,[string]$Target) "
        "$ErrorActionPreference='Stop'; "
        "New-Item -ItemType Junction -Path $Link -Target $Target | Out-Null }"
    )
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            command,
            str(link),
            str(target),
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stdout or result.stderr).strip().replace("\r", " ").replace("\n", " ")
        pytest.skip(f"directory junction creation is unavailable: {detail}")
    assert link.is_junction()


def test_raw_verification_rejects_a_symlinked_payload(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer = _enforcer()
    specification = _request()
    page, _ = _authorizations(enforcer, specification)
    published = _publish_raw(root, enforcer, specification, page)
    target = root.root / "governance" / "evidence" / "synthetic-target.bin"
    target.write_bytes(_PAYLOAD)
    _replace_with_symlink(root.root / published.payload_relative_path, target)

    with pytest.raises(PublicationCollisionError, match="incomplete or invalid"):
        RawArtifactPublisher(root, enforcer).verify_published(
            specification,
            _raw_identity(specification),
        )


def test_recovery_rejects_symlinked_files_and_intermediate_directories(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, sentinel = private_root
    target_file = root.root / "governance" / "evidence" / "synthetic-target.bin"
    target_file.write_bytes(_PAYLOAD)
    target_directory = target_file.parent
    raw_staging = root.ensure_directory(
        "staging/raw-artifacts",
        expected_root_id=sentinel.root_id,
    )
    raw_candidate = raw_staging / "artifact=unsafe-file.tmp"
    raw_candidate.mkdir()
    _replace_with_symlink(raw_candidate / "payload.bin", target_file)
    canonical_staging = root.ensure_directory(
        "staging/canonical-batches",
        expected_root_id=sentinel.root_id,
    )
    canonical_candidate = canonical_staging / "batch=unsafe-directory.tmp"
    canonical_candidate.mkdir()
    _replace_with_symlink(
        canonical_candidate / "parts",
        target_directory,
        directory=True,
    )

    inspections = PublicationRecoveryInspector(root).inspect_staging()

    assert len(inspections) == 2
    assert all(item.state is RecoveryInspectionState.INVALID for item in inspections)
    assert all(item.error_code == "UNSAFE_PUBLICATION_TREE" for item in inspections)


@pytest.mark.skipif(os.name != "nt", reason="Windows junction semantics")
@pytest.mark.parametrize("swap_level", ["candidate", "intermediate"])
@pytest.mark.parametrize(
    ("publication_kind", "fault_point"),
    [
        ("raw", PublicationFaultPoint.STAGING),
        ("raw", PublicationFaultPoint.RAW_WRITE),
        ("raw", PublicationFaultPoint.STAGED_MANIFEST_VERIFIED),
        ("canonical", PublicationFaultPoint.STAGING),
        ("canonical", PublicationFaultPoint.STAGED_MANIFEST_VERIFIED),
    ],
)
def test_candidate_junction_swap_is_rejected_before_any_external_write(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
    publication_kind: str,
    swap_level: str,
    fault_point: PublicationFaultPoint,
) -> None:
    root, _ = private_root
    enforcer = _enforcer()
    specification = _request()
    page, acquisition = _authorizations(enforcer, specification)
    if publication_kind == "canonical":
        _publish_raw(root, enforcer, specification, page)
    outside = root.root.parent / f"outside-publication-{uuid4().hex[:8]}"
    outside.mkdir()
    swapped: tuple[Path, Path] | None = None
    outside_snapshot: tuple[tuple[str, str], ...] | None = None

    def snapshot_outside() -> tuple[tuple[str, str], ...]:
        values: list[tuple[str, str]] = []
        for path in sorted(outside.rglob("*")):
            relative = path.relative_to(outside).as_posix()
            values.append(
                (relative, hashlib.sha256(path.read_bytes()).hexdigest())
                if path.is_file()
                else (relative, "DIRECTORY")
            )
        return tuple(values)

    def replace_candidate(point: PublicationFaultPoint) -> None:
        nonlocal outside_snapshot, swapped
        if point is not fault_point or swapped is not None:
            return
        namespace = "raw-artifacts" if publication_kind == "raw" else "canonical-batches"
        parent = root.root / "staging" / namespace
        candidate = next(parent.iterdir())
        if swap_level == "candidate":
            link = candidate
            held = parent / "held-candidate.tmp"
            candidate.rename(held)
            junction_target = outside
        else:
            link = parent
            held = outside / "held-staging-parent"
            parent.rename(held)
            junction_target = held
        swapped = (link, held)
        outside_snapshot = snapshot_outside()
        _create_windows_junction(link, junction_target)

    try:
        with pytest.raises(
            PublicationIntegrityError,
            match=r"direct directory|junction|reparse",
        ):
            if publication_kind == "raw":
                _publish_raw(
                    root,
                    enforcer,
                    specification,
                    page,
                    fault_injector=replace_candidate,
                )
            else:
                _publish_canonical(
                    CanonicalBatchPublisher(root, enforcer),
                    specification,
                    _batch_context(specification),
                    _parts(),
                    _outcomes(specification),
                    acquisition,
                    fault_injector=replace_candidate,
                )
        assert outside_snapshot is not None
        assert snapshot_outside() == outside_snapshot
    finally:
        if swapped is not None:
            junction, held = swapped
            if junction.exists() or junction.is_symlink():
                junction.rmdir()
            if held.exists():
                held.rename(junction)
        outside.rmdir()


def test_shared_recovery_verifier_requires_raw_presence_and_fixed_batch_time(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    published = _publish_canonical(
        CanonicalBatchPublisher(root, enforcer),
        specification,
        context,
        _parts(),
        _outcomes(specification),
        acquisition,
    )
    assert published is not None
    manifest_path = root.root / published.manifest_relative_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["fixed_ingested_at"] = (_INGESTED_AT + timedelta(seconds=1)).isoformat()
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )

    inspection = PublicationRecoveryInspector(root).inspect_published_batch(
        specification.provider,
        specification.dataset,
        context.canonical_batch_id,
    )
    assert inspection.state is RecoveryInspectionState.INVALID

    manifest["fixed_ingested_at"] = _INGESTED_AT.isoformat()
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )
    raw = _raw_identity(specification)
    raw_directory = root.root / (
        f"raw/provider=synthetic/dataset=price_bars/artifacts/artifact={raw.artifact_hash[:32]}"
    )
    (raw_directory / "payload.bin").unlink()

    inspection = PublicationRecoveryInspector(root).inspect_published_batch(
        specification.provider,
        specification.dataset,
        context.canonical_batch_id,
    )
    assert inspection.state is RecoveryInspectionState.INVALID


def test_recovery_requires_persisted_semantics_before_calling_an_orphan_verified(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    published = _publish_canonical(
        CanonicalBatchPublisher(root, enforcer),
        specification,
        context,
        _parts(),
        _outcomes(specification),
        acquisition,
    )
    assert published is not None
    manifest_path = root.root / published.manifest_relative_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["manifest_created_at"] = (_MANIFEST_AT + timedelta(seconds=1)).isoformat()
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )

    internally_complete = PublicationRecoveryInspector(root).inspect_published_batch(
        specification.provider,
        specification.dataset,
        context.canonical_batch_id,
    )
    with_persisted_context = PublicationRecoveryInspector(root).inspect_published_batch(
        specification.provider,
        specification.dataset,
        context.canonical_batch_id,
        expected_semantics=_expectation(specification, context),
    )

    assert internally_complete.state is RecoveryInspectionState.COMPLETE
    assert with_persisted_context.state is RecoveryInspectionState.INVALID


def test_recovery_requires_expected_file_identity_for_content_verified_state(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    published = _publish_canonical(
        CanonicalBatchPublisher(root, enforcer),
        specification,
        context,
        _parts(),
        _outcomes(specification),
        acquisition,
    )
    assert published is not None
    expected_manifest = _manifest(root, published)
    manifest_path = root.root / published.manifest_relative_path
    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    part_path = root.root / published.relative_directory / published.files[0].relative_path
    frame = pl.read_parquet(part_path).with_columns((pl.col("close") + 0.1).alias("close"))
    frame.write_parquet(part_path, compression="zstd", statistics=True)
    replacement = part_path.read_bytes()
    document["files"][0]["sha256"] = hashlib.sha256(replacement).hexdigest()
    document["files"][0]["byte_count"] = len(replacement)
    manifest_path.write_text(
        json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )

    internally_complete = PublicationRecoveryInspector(root).inspect_published_batch(
        specification.provider,
        specification.dataset,
        context.canonical_batch_id,
        expected_semantics=_expectation(specification, context),
    )
    exact_expected = PublicationRecoveryInspector(root).inspect_published_batch(
        specification.provider,
        specification.dataset,
        context.canonical_batch_id,
        expected_semantics=_expectation(specification, context),
        expected_manifest=expected_manifest,
    )

    assert internally_complete.state is RecoveryInspectionState.COMPLETE
    assert exact_expected.state is RecoveryInspectionState.INVALID


def test_calendar_snapshot_must_cover_the_whole_request(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    next_day = _START + timedelta(days=1)
    truncated = CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="1",
        tzdata_version="synthetic",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=next_day.date(),
        range_end=next_day.date() + timedelta(days=1),
        generated_at=_NOW,
        sessions=(
            CalendarSession(
                session_date=next_day.date(),
                open_utc=next_day,
                close_utc=next_day + timedelta(hours=6, minutes=30),
            ),
        ),
    )
    truncated_context = BatchContext(
        batch_identity=CanonicalBatchIdentity(
            request_spec_hash=specification.request_spec_hash,
            ordered_artifacts=context.batch_identity.ordered_artifacts,
            processing_signature=ProcessingSignature(
                canonical_schema_version="price-bar-v1",
                normalizer_version="synthetic-normalizer-v1",
                validator_version="bar-validator-v1",
                calendar_snapshot_checksum=truncated.checksum,
            ),
        ),
        fixed_ingested_at=context.fixed_ingested_at,
        manifest_created_at=context.manifest_created_at,
    )

    with pytest.raises(ValueError, match="calendar does not cover"):
        CanonicalBatchExpectation(
            specification=specification,
            batch_context=truncated_context,
            calendar_snapshot=truncated,
            provenance=_provenance(truncated_context),
            streams=_outcomes(specification),
        )
    with pytest.raises(PublicationIntegrityError, match="whole bounded request"):
        CanonicalBatchPublisher(root, enforcer).publish(
            specification,
            truncated_context,
            _parts(),
            _outcomes(specification),
            authorization=acquisition,
            calendar_snapshot=truncated,
            provenance=_provenance(truncated_context),
        )


def test_canonical_part_path_must_match_its_timeframe_and_utc_month(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    publisher = CanonicalBatchPublisher(root, enforcer)
    invalid_parts = (CanonicalParquetPart(relative_path="part-0000.parquet", frame=_frame()),)
    wrong_month = (
        CanonicalParquetPart(
            relative_path="timeframe=5m/year=2026/month=07/part-0000.parquet",
            frame=_frame(),
        ),
    )

    with pytest.raises(PublicationIntegrityError, match="timeframe/year/month"):
        _publish_canonical(
            publisher,
            specification,
            context,
            invalid_parts,
            _outcomes(specification),
            acquisition,
        )
    with pytest.raises(PublicationIntegrityError, match="year/month partition"):
        _publish_canonical(
            publisher,
            specification,
            context,
            wrong_month,
            _outcomes(specification),
            acquisition,
        )


def test_checksum_corrupt_complete_stage_fails_closed_instead_of_being_adopted(
    private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    root, _ = private_root
    enforcer, specification, acquisition, context = _prepared_canonical(root)
    publisher = CanonicalBatchPublisher(root, enforcer)

    def crash_after_manifest(point: PublicationFaultPoint) -> None:
        if point is PublicationFaultPoint.STAGED_MANIFEST_VERIFIED:
            raise InjectedCrash(point.value)

    with pytest.raises(InjectedCrash, match="staged_manifest_verified"):
        _publish_canonical(
            publisher,
            specification,
            context,
            _parts(),
            _outcomes(specification),
            acquisition,
            fault_injector=crash_after_manifest,
        )
    staged = PublicationRecoveryInspector(root).inspect_staging()
    candidate = root.root / staged[0].relative_directory
    manifest = json.loads((candidate / "manifest.json").read_text(encoding="utf-8"))
    (candidate / manifest["files"][0]["relative_path"]).write_bytes(b"corrupt")

    with pytest.raises(PublicationCollisionError, match="complete canonical staging"):
        _publish_canonical(
            publisher,
            specification,
            context,
            _parts(),
            _outcomes(specification),
            acquisition,
        )
