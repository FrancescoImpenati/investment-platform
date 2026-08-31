"""Deterministic identity and semantic replay tests for Phase 2 ingestion."""

import hashlib
import json
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from investment_platform.data.ingestion.identity import (
    IDENTITY_CANONICALIZATION_VERSION,
    AttemptIdentity,
    BarSemantics,
    BatchContext,
    CanonicalBatchIdentity,
    DataKind,
    IdentityDimension,
    ObservationComparison,
    ObservationIdentity,
    PriceBarSemanticValue,
    ProcessingSignature,
    ProviderInstrumentMapping,
    RawArtifactIdentity,
    RequestInstanceIdentity,
    RequestSpecification,
    SemanticObservation,
    StreamKey,
    canonical_page_relation,
    semantic_value_fingerprint,
)
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession

_INSTRUMENT_A = UUID("00000000-0000-4000-8000-000000000001")
_INSTRUMENT_B = UUID("00000000-0000-4000-8000-000000000002")
_START = datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
_END = datetime(2025, 7, 2, 20, 0, tzinfo=UTC)
_CALENDAR_CHECKSUM = f"sha256:{'c' * 64}"


def _dimensions() -> tuple[IdentityDimension, ...]:
    return (
        IdentityDimension(name="feed_scope", value="consolidated"),
        IdentityDimension(name="trade_condition_policy", value="provider_default"),
    )


def _stream(**overrides: object) -> StreamKey:
    payload: dict[str, object] = {
        "provider": "alpaca",
        "dataset": "price_bars_sip",
        "data_kind": DataKind.PRICE_BAR,
        "instrument_id": _INSTRUMENT_A,
        "timeframe": Timeframe.FIVE_MINUTES,
        "session": TradingSession.REGULAR,
        "adjustment": AdjustmentState.UNADJUSTED,
        "currency": "USD",
        "bar_semantics": BarSemantics.PROVIDER_AGGREGATED_OHLCV,
        "additional_dimensions": _dimensions(),
    }
    payload.update(overrides)
    return StreamKey.model_validate(payload)


def _mapping_a(identifier: str = "AAPL") -> ProviderInstrumentMapping:
    return ProviderInstrumentMapping(
        instrument_id=_INSTRUMENT_A,
        provider_identifier=identifier,
    )


def _mapping_b(identifier: str = "MSFT") -> ProviderInstrumentMapping:
    return ProviderInstrumentMapping(
        instrument_id=_INSTRUMENT_B,
        provider_identifier=identifier,
    )


def _request(**overrides: object) -> RequestSpecification:
    payload: dict[str, object] = {
        "provider": "alpaca",
        "dataset": "price_bars_sip",
        "data_kind": DataKind.PRICE_BAR,
        "instrument_mappings": (_mapping_a(), _mapping_b()),
        "timeframe": Timeframe.FIVE_MINUTES,
        "session": TradingSession.REGULAR,
        "adjustment": AdjustmentState.UNADJUSTED,
        "currency": "USD",
        "bar_semantics": BarSemantics.PROVIDER_AGGREGATED_OHLCV,
        "additional_dimensions": _dimensions(),
        "start": _START,
        "end": _END,
        "mapping_semantic_version": "alpaca-bars-v1",
    }
    payload.update(overrides)
    return RequestSpecification.model_validate(payload)


def _artifact(
    request: RequestSpecification,
    payload: bytes = b'{"bars":[]}',
    *,
    page_ordinal: int = 0,
    page_relation: str | None = None,
    media_type: str = "application/json",
    content_encoding: str = "identity",
) -> RawArtifactIdentity:
    return RawArtifactIdentity.from_bytes(
        request,
        page_ordinal=page_ordinal,
        media_type=media_type,
        content_encoding=content_encoding,
        payload=payload,
        page_relation=page_relation,
    )


def _processing(**overrides: object) -> ProcessingSignature:
    payload: dict[str, object] = {
        "canonical_schema_version": "price-bar-v1",
        "normalizer_version": "alpaca-normalizer-v1",
        "validator_version": "bar-validator-v1",
        "calendar_snapshot_checksum": _CALENDAR_CHECKSUM,
        "process_semantics": (IdentityDimension(name="interval_boundary", value="half_open"),),
    }
    payload.update(overrides)
    return ProcessingSignature.model_validate(payload)


def _batch(
    request: RequestSpecification,
    *,
    artifacts: tuple[RawArtifactIdentity, ...] | None = None,
    processing: ProcessingSignature | None = None,
) -> CanonicalBatchIdentity:
    return CanonicalBatchIdentity(
        request_spec_hash=request.request_spec_hash,
        ordered_artifacts=artifacts or (_artifact(request),),
        processing_signature=processing or _processing(),
    )


def _observation(stream: StreamKey | None = None) -> ObservationIdentity:
    return ObservationIdentity(
        stream=stream or _stream(),
        start=_START,
        end=_START + timedelta(minutes=5),
    )


def _value(**overrides: object) -> PriceBarSemanticValue:
    payload: dict[str, object] = {
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1_000.0,
        "vwap": 100.25,
        "currency": "USD",
        "available_at": None,
        "quality_flags": (),
    }
    payload.update(overrides)
    return PriceBarSemanticValue.model_validate(payload)


@pytest.mark.unit
def test_stream_key_uses_versioned_canonical_json_and_excludes_ticker() -> None:
    stream = _stream()
    manifest = json.loads(stream.canonical_json)

    assert manifest["canonicalization_version"] == IDENTITY_CANONICALIZATION_VERSION
    assert manifest["kind"] == "stream-key"
    assert manifest["payload"]["instrument_id"] == str(_INSTRUMENT_A)
    assert manifest["payload"]["data_kind"] == "price_bar"
    assert "ticker" not in stream.canonical_json
    assert "provider_identifier" not in stream.canonical_json
    assert stream.stream_id == f"stream_v1_{stream.stream_hash}"

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        StreamKey.model_validate({**stream.model_dump(), "ticker": "AAPL"})


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("provider", "Alpaca"),
        ("provider", "alpaca "),
        ("dataset", "PRICE_BARS_SIP"),
        ("dataset", "price bars sip"),
    ],
)
def test_provider_and_dataset_require_exact_canonical_catalog_keys(
    field: str,
    value: str,
) -> None:
    with pytest.raises(ValidationError, match="lowercase and contain no whitespace"):
        _stream(**{field: value})


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("provider", "other_provider"),
        ("dataset", "historical_other_feed_bars"),
        ("instrument_id", _INSTRUMENT_B),
        ("timeframe", Timeframe.ONE_DAY),
        ("session", TradingSession.PRE_MARKET),
        ("adjustment", AdjustmentState.SPLIT_ADJUSTED),
        ("currency", "EUR"),
        ("bar_semantics", BarSemantics.CANONICAL_SESSION_OHLCV),
        (
            "additional_dimensions",
            (IdentityDimension(name="feed_scope", value="venue_only"),),
        ),
    ],
)
def test_every_stream_defining_dimension_changes_stream_identity(
    field: str,
    changed_value: object,
) -> None:
    assert _stream().stream_id != _stream(**{field: changed_value}).stream_id


@pytest.mark.unit
def test_dimension_order_is_canonical_and_unsafe_or_unknown_fields_fail_closed() -> None:
    forward = _stream(additional_dimensions=_dimensions())
    reversed_order = _stream(additional_dimensions=tuple(reversed(_dimensions())))

    assert forward == reversed_order
    assert forward.stream_hash == reversed_order.stream_hash

    with pytest.raises(ValidationError, match="unsafe or reserved"):
        IdentityDimension(name="ticker", value="AAPL")
    with pytest.raises(ValidationError, match="unsafe or reserved"):
        IdentityDimension(name="api_token", value="redacted")
    with pytest.raises(ValidationError, match="URL, secret"):
        IdentityDimension(name="venue", value="https://example.invalid/feed")
    with pytest.raises(ValidationError, match="duplicate names"):
        _stream(
            additional_dimensions=(
                IdentityDimension(name="venue", value="a"),
                IdentityDimension(name="venue", value="b"),
            )
        )
    with pytest.raises(ValidationError, match="Input should be 'price_bar'"):
        _stream(data_kind="unknown")
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        StreamKey.model_validate({**forward.model_dump(), "unknown_dimension": "x"})


@pytest.mark.unit
def test_request_mapping_and_timezone_order_are_canonical() -> None:
    forward = _request(instrument_mappings=(_mapping_a(), _mapping_b()))
    reversed_order = _request(instrument_mappings=(_mapping_b(), _mapping_a()))
    offset = timezone(timedelta(hours=2))
    equivalent_timezone = _request(
        start=_START.astimezone(offset),
        end=_END.astimezone(offset),
    )

    assert forward.instrument_mappings == (_mapping_a(), _mapping_b())
    assert forward.request_spec_hash == reversed_order.request_spec_hash
    assert forward.request_spec_hash == equivalent_timezone.request_spec_hash
    assert forward.request_spec_id == f"request_spec_v1_{forward.request_spec_hash}"


@pytest.mark.unit
def test_provider_mapping_is_request_identity_but_not_stream_identity() -> None:
    old_mapping = _request(instrument_mappings=(_mapping_a("OLD"),))
    new_mapping = _request(instrument_mappings=(_mapping_a("NEW"),))

    assert old_mapping.request_spec_hash != new_mapping.request_spec_hash
    assert old_mapping.stream_keys()[0].stream_id == new_mapping.stream_keys()[0].stream_id


@pytest.mark.unit
def test_request_instances_and_attempts_are_distinct_from_request_specification() -> None:
    specification = _request()
    first_instance = RequestInstanceIdentity.create(
        specification,
        uuid_factory=lambda: UUID("10000000-0000-4000-8000-000000000001"),
    )
    second_instance = RequestInstanceIdentity.create(
        specification,
        uuid_factory=lambda: UUID("10000000-0000-4000-8000-000000000002"),
    )
    first_attempt = AttemptIdentity.create(
        first_instance,
        attempt_number=1,
        uuid_factory=lambda: UUID("20000000-0000-4000-8000-000000000001"),
    )
    retry_attempt = AttemptIdentity.create(
        first_instance,
        attempt_number=2,
        uuid_factory=lambda: UUID("20000000-0000-4000-8000-000000000002"),
    )

    assert first_instance.request_spec_hash == second_instance.request_spec_hash
    assert first_instance.request_instance_id != second_instance.request_instance_id
    assert first_attempt.request_instance_id == retry_attempt.request_instance_id
    assert first_attempt.attempt_id != retry_attempt.attempt_id
    assert specification.request_spec_hash == _request().request_spec_hash

    for forbidden in ("retry", "run_id", "page_token", "credential", "authenticated_url"):
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            RequestSpecification.model_validate(
                {**specification.model_dump(), forbidden: "must-not-enter-identity"}
            )


@pytest.mark.unit
def test_mapping_semantic_version_and_half_open_bounds_are_identity_bearing() -> None:
    specification = _request()

    assert (
        specification.request_spec_hash
        != _request(mapping_semantic_version="alpaca-bars-v2").request_spec_hash
    )
    assert (
        specification.request_spec_hash
        != _request(end=_END + timedelta(minutes=5)).request_spec_hash
    )
    with pytest.raises(ValidationError, match="end must be later"):
        _request(end=_START)
    with pytest.raises(ValidationError, match="timezone-aware"):
        _request(start=datetime(2025, 7, 2, 13, 30))


@pytest.mark.unit
def test_raw_identity_excludes_attempt_metadata_and_binds_representation() -> None:
    request = _request()
    first = _artifact(request)
    replay_under_another_provider_request_id = _artifact(request)
    different_media = _artifact(request, media_type="application/vnd.alpaca+json")
    different_encoding = _artifact(request, content_encoding="gzip")

    assert first == replay_under_another_provider_request_id
    assert first.artifact_id == replay_under_another_provider_request_id.artifact_id
    assert first.artifact_id != different_media.artifact_id
    assert first.artifact_id != different_encoding.artifact_id
    assert "provider_request_id" not in first.canonical_json

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        RawArtifactIdentity.model_validate(
            {**first.model_dump(), "provider_request_id": "attempt-specific-id"}
        )


@pytest.mark.unit
def test_raw_identity_changes_with_content_page_and_rejects_bad_hashes() -> None:
    request = _request()
    first = _artifact(request)

    assert first.artifact_id != _artifact(request, payload=b'{"bars":[1]}').artifact_id
    assert (
        first.artifact_id
        != _artifact(
            request,
            page_ordinal=1,
            page_relation="after:0",
        ).artifact_id
    )
    assert len(first.page_relation_hash) == 64

    with pytest.raises(ValidationError, match="String should match pattern"):
        RawArtifactIdentity.model_validate({**first.model_dump(), "content_sha256": "sha256:bad"})
    with pytest.raises(ValidationError, match="URL, secret"):
        _artifact(request, page_relation="next_token:secret")
    with pytest.raises(ValidationError, match="deterministic relation"):
        _artifact(request, page_relation="cursor:abc")
    with pytest.raises(ValidationError, match="deterministic relation"):
        _artifact(request, page_ordinal=1, page_relation="root")
    with pytest.raises(ValidationError):
        _artifact(request, page_relation="https://example.invalid/page")


@pytest.mark.unit
def test_raw_identity_can_be_built_from_streamed_digest_metadata() -> None:
    request = _request()
    payload = b'{"bars":[1]}'
    from_bytes = _artifact(request, payload=payload, page_ordinal=1)
    from_digest = RawArtifactIdentity.from_digest(
        request_spec_hash=request.request_spec_hash,
        page_ordinal=1,
        media_type="application/json",
        content_encoding="identity",
        content_sha256=hashlib.sha256(payload).hexdigest(),
        byte_count=len(payload),
    )

    assert from_digest == from_bytes
    assert from_digest.page_relation == canonical_page_relation(1) == "after:0"


@pytest.mark.unit
def test_batch_identity_binds_raw_content_processing_versions_and_calendar() -> None:
    request = _request()
    original_artifact = _artifact(request)
    original = _batch(request, artifacts=(original_artifact,))
    corrected = _batch(
        request,
        artifacts=(_artifact(request, payload=b'{"bars":[{"revision":2}]}'),),
    )
    new_validator = _batch(
        request,
        artifacts=(original_artifact,),
        processing=_processing(validator_version="bar-validator-v2"),
    )
    new_calendar = _batch(
        request,
        artifacts=(original_artifact,),
        processing=_processing(calendar_snapshot_checksum=f"sha256:{'d' * 64}"),
    )

    assert original.canonical_batch_id != corrected.canonical_batch_id
    assert original.canonical_batch_id != new_validator.canonical_batch_id
    assert original.canonical_batch_id != new_calendar.canonical_batch_id
    assert len(original.ordered_artifacts_hash) == 64

    with pytest.raises(ValidationError, match="String should match pattern"):
        _processing(calendar_snapshot_checksum="c" * 64)


@pytest.mark.unit
def test_batch_artifacts_are_unique_ordered_and_belong_to_the_request() -> None:
    request = _request()
    first = _artifact(request)
    second = _artifact(
        request,
        payload=b'{"bars":[],"page":2}',
        page_ordinal=1,
        page_relation="after:0",
    )

    assert _batch(request, artifacts=(first, second)).artifact_ids == (
        first.artifact_id,
        second.artifact_id,
    )
    with pytest.raises(ValidationError, match="contiguous page ordinals"):
        _batch(request, artifacts=(second, first))
    with pytest.raises(ValidationError, match="duplicate identities"):
        _batch(request, artifacts=(first, first))
    with pytest.raises(ValidationError, match="belong to request_spec_hash"):
        _batch(request, artifacts=(_artifact(_request(dataset="other_dataset")),))


@pytest.mark.unit
def test_batch_rejects_incomplete_or_non_rooted_page_chains() -> None:
    request = _request()
    root = _artifact(request, page_ordinal=0)
    page_one = _artifact(request, page_ordinal=1)
    page_two = _artifact(request, page_ordinal=2)

    with pytest.raises(ValidationError, match="contiguous page ordinals"):
        _batch(request, artifacts=(root, page_two))
    with pytest.raises(ValidationError, match="contiguous page ordinals"):
        _batch(request, artifacts=(page_one,))


@pytest.mark.unit
def test_batch_context_replay_reuses_fixed_timestamps_excluded_from_batch_digest() -> None:
    batch = _batch(_request())
    ingested_at = datetime(2026, 8, 31, 10, tzinfo=UTC)
    manifest_created_at = ingested_at + timedelta(seconds=5)
    persisted = BatchContext(
        batch_identity=batch,
        fixed_ingested_at=ingested_at,
        manifest_created_at=manifest_created_at,
    )
    exact_replay = BatchContext(
        batch_identity=batch,
        fixed_ingested_at=ingested_at,
        manifest_created_at=manifest_created_at,
    )
    regenerated_timestamps = BatchContext(
        batch_identity=batch,
        fixed_ingested_at=ingested_at + timedelta(seconds=1),
        manifest_created_at=manifest_created_at + timedelta(seconds=1),
    )

    persisted.validate_replay(exact_replay)
    assert persisted.canonical_batch_id == regenerated_timestamps.canonical_batch_id
    assert persisted.batch_context_id == regenerated_timestamps.batch_context_id
    with pytest.raises(ValueError, match="fixed_ingested_at"):
        persisted.validate_replay(regenerated_timestamps)


@pytest.mark.unit
def test_observation_identity_and_value_fingerprint_classify_noop_and_revision() -> None:
    identity = _observation()
    processing = _processing()
    value = _value(quality_flags=("flag_b", "flag_a"))
    existing = SemanticObservation.create(identity, value, processing)
    replay = SemanticObservation.create(
        _observation(),
        _value(quality_flags=("flag_a", "flag_b")),
        processing,
    )
    correction = SemanticObservation.create(identity, _value(close=100.75), processing)
    reprocessed = SemanticObservation.create(
        identity,
        value,
        _processing(normalizer_version="alpaca-normalizer-v2"),
    )
    other_interval = SemanticObservation.create(
        ObservationIdentity(
            stream=_stream(),
            start=_START + timedelta(minutes=5),
            end=_START + timedelta(minutes=10),
        ),
        value,
        processing,
    )

    assert existing.compare(replay) is ObservationComparison.SEMANTIC_NO_OP
    assert existing.compare(correction) is ObservationComparison.REVISION
    assert existing.compare(reprocessed) is ObservationComparison.REVISION
    assert existing.compare(other_interval) is ObservationComparison.DIFFERENT_OBSERVATION
    assert "retrieved_at" not in identity.canonical_json
    assert "ingested_at" not in identity.canonical_json


@pytest.mark.unit
def test_semantic_fingerprint_normalizes_numeric_spelling_and_rejects_volatile_fields() -> None:
    processing = _processing()
    assert semantic_value_fingerprint(_value(open=1), processing) == semantic_value_fingerprint(
        _value(open=1.0), processing
    )

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        PriceBarSemanticValue.model_validate(
            {**_value().model_dump(), "retrieved_at": datetime.now(UTC)}
        )
    with pytest.raises(ValidationError, match="String should match pattern"):
        SemanticObservation(identity=_observation(), value_fingerprint="not-a-sha256")
