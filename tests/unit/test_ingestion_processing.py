"""Deterministic canonical preparation tests over offline Alpaca-shaped pages."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import pytest

from investment_platform.data.calendar import CalendarSession, CalendarSnapshot
from investment_platform.data.ingestion.acquisition import (
    inspect_alpaca_sip_bar_page,
    specification_to_bar_request,
)
from investment_platform.data.ingestion.identity import (
    BarSemantics,
    DataKind,
    ProviderInstrumentMapping,
    RawArtifactIdentity,
    RequestSpecification,
)
from investment_platform.data.ingestion.processing import (
    CanonicalProcessingError,
    PreparedCanonicalBatch,
    RawProcessingPage,
    prepare_alpaca_sip_batch_context,
    prepare_alpaca_sip_canonical_batch,
    prepare_alpaca_sip_canonical_batch_from_context,
)
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.provenance import BytesRawPayload, RawBatch
from investment_platform.data.providers import AlpacaCredentials, AlpacaFeed, AlpacaProvider
from investment_platform.data.providers.alpaca import ALPACA_IEX_BAR_SOURCE
from investment_platform.data.providers.http import HttpResponse
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage import StreamPublicationOutcome
from investment_platform.runtime import RuntimeEnvironment
from tests.provider_fakes import QueueHttpTransport

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
_SESSION_DATE = date(2025, 7, 2)
_OPEN = datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
_CLOSE = datetime(2025, 7, 2, 20, tzinfo=UTC)
_A = UUID("10000000-0000-4000-8000-000000000001")
_B = UUID("10000000-0000-4000-8000-000000000002")


def _calendar() -> CalendarSnapshot:
    return CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="1",
        tzdata_version="synthetic",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=_SESSION_DATE,
        range_end=_SESSION_DATE + timedelta(days=1),
        generated_at=_NOW,
        sessions=(
            CalendarSession(
                session_date=_SESSION_DATE,
                open_utc=_OPEN,
                close_utc=_CLOSE,
            ),
        ),
    )


def _specification(
    *,
    timeframe: Timeframe = Timeframe.FIVE_MINUTES,
    mappings: tuple[tuple[UUID, str], ...] = ((_A, "XPH1"),),
) -> RequestSpecification:
    start, end = (
        (_OPEN, _CLOSE)
        if timeframe is Timeframe.ONE_DAY
        else (
            _OPEN,
            _OPEN + timedelta(minutes=10),
        )
    )
    return RequestSpecification(
        provider="alpaca",
        dataset="price_bars_sip",
        data_kind=DataKind.PRICE_BAR,
        instrument_mappings=tuple(
            ProviderInstrumentMapping(instrument_id=instrument_id, provider_identifier=symbol)
            for instrument_id, symbol in mappings
        ),
        timeframe=timeframe,
        session=TradingSession.REGULAR,
        adjustment=AdjustmentState.UNADJUSTED,
        currency="USD",
        bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
        start=start,
        end=end,
        mapping_semantic_version="alpaca-sip-bars-v1",
    )


def _bar(timestamp: str, *, close: float = 100.5) -> dict[str, object]:
    return {
        "t": timestamp,
        "o": 100.0,
        "h": 101.0,
        "l": 99.0,
        "c": close,
        "v": 1000,
        "vw": 100.25,
    }


def _processing_input(
    specification: RequestSpecification,
    bars: dict[str, list[dict[str, object]]],
) -> tuple[tuple[RawProcessingPage, ...], AcquisitionPolicyAuthorization]:
    payload = json.dumps(
        {"bars": bars, "next_page_token": None},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    provider = AlpacaProvider(
        AlpacaCredentials("synthetic-id", "synthetic-secret"),
        feed=AlpacaFeed.SIP,
        transport=QueueHttpTransport([HttpResponse(200, payload)]),
        clock=lambda: _NOW,
        batch_id_factory=lambda: UUID("30000000-0000-4000-8000-000000000001"),
    )
    batch = next(provider.get_bars(specification_to_bar_request(specification)))
    enforcer = RetentionPolicyEnforcer(
        RetentionPolicyCatalog.load_default(),
        clock=lambda: _NOW,
    )
    request = enforcer.authorize_request(
        specification.provider,
        specification.dataset,
        environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        start=specification.start,
        end=specification.end,
        request_spec_hash=specification.request_spec_hash,
    )
    inspected = inspect_alpaca_sip_bar_page(batch, specification, _calendar())
    page_authorization = enforcer.authorize_response_page(
        request,
        page_ordinal=0,
        page_relation="root",
        payload_sha256=inspected.payload_sha256,
        payload_size_bytes=inspected.payload_size_bytes,
        canonical_media_type=inspected.canonical_media_type,
        content_encoding=inspected.content_encoding,
        observed_start=inspected.observed_start,
        observed_end=inspected.observed_end,
    )
    acquisition = enforcer.authorize_completed_acquisition(
        request,
        (page_authorization,),
        pagination_complete=True,
        terminal_page_verified=True,
    )
    identity = RawArtifactIdentity.from_digest(
        request_spec_hash=specification.request_spec_hash,
        page_ordinal=0,
        media_type="application/json",
        content_encoding="identity",
        content_sha256=hashlib.sha256(payload).hexdigest(),
        byte_count=len(payload),
    )
    return (RawProcessingPage(identity=identity, batch=batch),), acquisition


def _prepare(
    specification: RequestSpecification,
    bars: dict[str, list[dict[str, object]]],
) -> PreparedCanonicalBatch:
    pages, authorization = _processing_input(specification, bars)
    return prepare_alpaca_sip_canonical_batch(
        specification=specification,
        pages=pages,
        acquisition_authorization=authorization,
        calendar_snapshot=_calendar(),
        fixed_ingested_at=_NOW + timedelta(minutes=1),
        manifest_created_at=_NOW + timedelta(minutes=2),
    )


def test_five_minute_preparation_is_deterministic_and_partitioned() -> None:
    specification = _specification()
    bars = {
        "XPH1": [
            _bar("2025-07-02T13:30:00Z"),
            _bar("2025-07-02T13:35:00Z", close=100.75),
        ]
    }

    first = _prepare(specification, bars)
    replay = _prepare(specification, bars)

    assert first.batch_context == replay.batch_context
    assert first.expectation == replay.expectation
    assert first.publishable_stream_count == 1
    assert first.blocked_stream_count == 0
    assert first.parts[0].relative_path == "timeframe=5m/year=2025/month=07/part-0000.parquet"
    assert first.parts[0].frame.height == 2
    assert first.stream_outcomes[0].row_count == 2


def test_context_is_frozen_before_normalization_and_reused_after_fault(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    specification = _specification()
    pages, authorization = _processing_input(
        specification,
        {"XPH1": [_bar("2025-07-02T13:30:00Z")]},
    )
    frozen = prepare_alpaca_sip_batch_context(
        specification=specification,
        pages=pages,
        acquisition_authorization=authorization,
        calendar_snapshot=_calendar(),
        fixed_ingested_at=_NOW + timedelta(minutes=1),
        manifest_created_at=_NOW + timedelta(minutes=2),
    )

    def fail_normalization(*args: object, **kwargs: object) -> object:
        raise RuntimeError("synthetic normalization crash")

    monkeypatch.setattr(
        "investment_platform.data.ingestion.processing.normalize_alpaca_bars",
        fail_normalization,
    )
    with pytest.raises(RuntimeError, match="synthetic normalization crash"):
        prepare_alpaca_sip_canonical_batch_from_context(
            specification=specification,
            pages=pages,
            acquisition_authorization=authorization,
            calendar_snapshot=_calendar(),
            batch_context=frozen.batch_context,
            provenance=frozen.provenance,
        )

    monkeypatch.undo()
    recovered = prepare_alpaca_sip_canonical_batch_from_context(
        specification=specification,
        pages=pages,
        acquisition_authorization=authorization,
        calendar_snapshot=_calendar(),
        batch_context=frozen.batch_context,
        provenance=frozen.provenance,
    )

    assert recovered.batch_context is frozen.batch_context
    assert recovered.batch_context.fixed_ingested_at == _NOW + timedelta(minutes=1)
    assert recovered.batch_context.manifest_created_at == _NOW + timedelta(minutes=2)


def test_multi_stream_request_publishes_independent_nonempty_stream() -> None:
    specification = _specification(mappings=((_A, "XPH1"), (_B, "XPH2")))

    prepared = _prepare(
        specification,
        {"XPH1": [_bar("2025-07-02T13:30:00Z")], "XPH2": []},
    )

    by_instrument = {outcome.stream.instrument_id: outcome for outcome in prepared.stream_outcomes}
    assert by_instrument[_A].outcome is StreamPublicationOutcome.PUBLISHABLE
    assert by_instrument[_B].outcome is StreamPublicationOutcome.BLOCKED
    assert by_instrument[_B].validation_codes == ("NO_CANONICAL_ROWS",)
    assert prepared.parts[0].frame.get_column("instrument_id").unique().to_list() == [str(_A)]


def test_daily_timestamp_is_canonicalized_to_xnys_session_bounds() -> None:
    specification = _specification(timeframe=Timeframe.ONE_DAY)

    prepared = _prepare(
        specification,
        {"XPH1": [_bar("2025-07-02T04:00:00Z")]},
    )

    outcome = prepared.stream_outcomes[0]
    assert outcome.observed_start == _OPEN
    assert outcome.observed_end == _CLOSE
    assert prepared.parts[0].relative_path.startswith("timeframe=1d/")


def test_duplicate_observation_blocks_entire_affected_stream() -> None:
    specification = _specification()

    prepared = _prepare(
        specification,
        {
            "XPH1": [
                _bar("2025-07-02T13:30:00Z"),
                _bar("2025-07-02T13:30:00Z", close=99.75),
            ]
        },
    )

    assert prepared.all_blocked is True
    assert prepared.parts == ()
    assert prepared.stream_outcomes[0].validation_codes == ("QUALITY:DUPLICATE_BAR",)


def test_fixed_ingestion_time_cannot_precede_raw_retrieval() -> None:
    specification = _specification()
    pages, authorization = _processing_input(
        specification,
        {"XPH1": [_bar("2025-07-02T13:30:00Z")]},
    )

    with pytest.raises(CanonicalProcessingError, match="precedes raw retrieval"):
        prepare_alpaca_sip_canonical_batch(
            specification=specification,
            pages=pages,
            acquisition_authorization=authorization,
            calendar_snapshot=_calendar(),
            fixed_ingested_at=_NOW - timedelta(seconds=1),
            manifest_created_at=_NOW,
        )


def test_processing_revalidates_raw_payload_content_identity() -> None:
    specification = _specification()
    pages, authorization = _processing_input(
        specification,
        {"XPH1": [_bar("2025-07-02T13:30:00Z")]},
    )
    original = pages[0]
    tampered = RawProcessingPage(
        identity=original.identity,
        batch=RawBatch(
            metadata=original.batch.metadata,
            payload=BytesRawPayload(b'{"bars":{},"next_page_token":null}'),
        ),
    )

    with pytest.raises(CanonicalProcessingError, match="payload differs from its identity"):
        prepare_alpaca_sip_canonical_batch(
            specification=specification,
            pages=(tampered,),
            acquisition_authorization=authorization,
            calendar_snapshot=_calendar(),
            fixed_ingested_at=_NOW + timedelta(minutes=1),
            manifest_created_at=_NOW + timedelta(minutes=2),
        )


def test_processing_rejects_non_sip_source_even_with_authorized_identity() -> None:
    specification = _specification()
    pages, authorization = _processing_input(
        specification,
        {"XPH1": [_bar("2025-07-02T13:30:00Z")]},
    )
    original = pages[0]
    wrong_source = RawProcessingPage(
        identity=original.identity,
        batch=RawBatch(
            metadata=original.batch.metadata.model_copy(update={"source": ALPACA_IEX_BAR_SOURCE}),
            payload=original.batch.payload,
        ),
    )

    with pytest.raises(CanonicalProcessingError, match="not historical Alpaca SIP"):
        prepare_alpaca_sip_canonical_batch(
            specification=specification,
            pages=(wrong_source,),
            acquisition_authorization=authorization,
            calendar_snapshot=_calendar(),
            fixed_ingested_at=_NOW + timedelta(minutes=1),
            manifest_created_at=_NOW + timedelta(minutes=2),
        )


def test_off_grid_five_minute_observation_blocks_affected_stream() -> None:
    specification = _specification()

    prepared = _prepare(
        specification,
        {"XPH1": [_bar("2025-07-02T13:31:00Z")]},
    )

    assert prepared.all_blocked is True
    assert prepared.parts == ()
    assert prepared.stream_outcomes[0].validation_codes == ("CALENDAR:UNEXPECTED_SLOT",)
