"""Tests for bounded, paginable provider contracts and raw payload resources."""

from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta, timezone
from typing import cast
from uuid import uuid4

import pytest
from pydantic import ValidationError

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.provenance import (
    BytesRawPayload,
    DataSource,
    RawBatch,
    RawBatchMetadata,
    RawPayload,
)
from investment_platform.data.providers import (
    BarRequest,
    CorporateActionRequest,
    MarketDataProvider,
    ProviderInstrumentRef,
)

pytestmark = pytest.mark.unit


def _raw_batch(content: bytes) -> RawBatch:
    metadata = RawBatchMetadata(
        batch_id=uuid4(),
        source=DataSource(
            source_id=uuid4(),
            provider="fake",
            dataset="bars",
            logical_endpoint="fixtures/bars",
        ),
        retrieved_at=datetime(2026, 8, 14, 14, tzinfo=UTC),
        media_type="application/json",
        file_extension="json",
    )
    return RawBatch(metadata=metadata, payload=BytesRawPayload(content))


class _PagedFakeProvider:
    def __init__(self, pages: tuple[RawBatch, ...]) -> None:
        self._pages = pages

    @property
    def provider_name(self) -> str:
        return "fake"

    def get_instruments(self, *, as_of: date | None = None) -> Iterable[RawBatch]:
        del as_of
        return iter(self._pages)

    def get_bars(self, request: BarRequest) -> Iterable[RawBatch]:
        del request
        return iter(self._pages)

    def get_corporate_actions(self, request: CorporateActionRequest) -> Iterable[RawBatch]:
        del request
        return iter(self._pages)


def test_raw_payload_is_reopenable_and_runtime_checkable() -> None:
    payload = BytesRawPayload(b'{"page": 1}')

    assert isinstance(payload, RawPayload)
    with payload.open_binary() as first_reader:
        assert first_reader.read() == b'{"page": 1}'
    with payload.open_binary() as second_reader:
        assert second_reader.read() == b'{"page": 1}'


def test_raw_batch_rejects_objects_without_payload_contract() -> None:
    batch = _raw_batch(b"valid")

    with pytest.raises(TypeError, match="RawPayload"):
        RawBatch(metadata=batch.metadata, payload=cast(RawPayload, object()))


def test_bar_request_is_bounded_half_open_and_normalized_to_utc() -> None:
    ref = ProviderInstrumentRef(instrument_id=uuid4(), provider_identifier="AAPL")
    offset = timezone(timedelta(hours=2))
    request = BarRequest(
        instruments=(ref,),
        timeframe=Timeframe.FIVE_MINUTES,
        session=TradingSession.REGULAR,
        adjustment_state=AdjustmentState.UNADJUSTED,
        start=datetime(2026, 8, 14, 15, 30, tzinfo=offset),
        end=datetime(2026, 8, 14, 16, 30, tzinfo=offset),
    )

    assert request.start == datetime(2026, 8, 14, 13, 30, tzinfo=UTC)
    assert request.end == datetime(2026, 8, 14, 14, 30, tzinfo=UTC)


def test_bar_request_rejects_empty_duplicate_naive_or_reversed_inputs() -> None:
    ref = ProviderInstrumentRef(instrument_id=uuid4(), provider_identifier="AAPL")
    start = datetime(2026, 8, 14, 13, 30, tzinfo=UTC)

    with pytest.raises(ValidationError):
        BarRequest(
            instruments=(),
            timeframe=Timeframe.FIVE_MINUTES,
            start=start,
            end=start + timedelta(minutes=5),
        )
    with pytest.raises(ValidationError, match="duplicate"):
        BarRequest(
            instruments=(ref, ref),
            timeframe=Timeframe.FIVE_MINUTES,
            start=start,
            end=start + timedelta(minutes=5),
        )
    with pytest.raises(ValidationError, match="timezone-aware"):
        BarRequest(
            instruments=(ref,),
            timeframe=Timeframe.FIVE_MINUTES,
            start=datetime(2026, 8, 14, 13, 30),
            end=start + timedelta(minutes=5),
        )
    with pytest.raises(ValidationError, match="end must be later"):
        BarRequest(
            instruments=(ref,),
            timeframe=Timeframe.FIVE_MINUTES,
            start=start,
            end=start,
        )


def test_corporate_action_request_uses_half_open_dates() -> None:
    request = CorporateActionRequest(
        instruments=(ProviderInstrumentRef(instrument_id=uuid4(), provider_identifier="NVDA"),),
        start=date(2026, 1, 1),
        end=date(2026, 2, 1),
    )

    assert request.start == date(2026, 1, 1)
    assert request.end == date(2026, 2, 1)

    with pytest.raises(ValidationError, match="end must be later"):
        CorporateActionRequest(
            instruments=request.instruments,
            start=date(2026, 2, 1),
            end=date(2026, 2, 1),
        )


def test_paged_fake_provider_conforms_at_runtime_without_vendor_code() -> None:
    pages = (_raw_batch(b"page-one"), _raw_batch(b"page-two"))
    provider = _PagedFakeProvider(pages)
    request = BarRequest(
        instruments=(ProviderInstrumentRef(instrument_id=uuid4(), provider_identifier="MSFT"),),
        timeframe=Timeframe.ONE_DAY,
        start=datetime(2026, 8, 1, tzinfo=UTC),
        end=datetime(2026, 8, 2, tzinfo=UTC),
    )

    assert isinstance(provider, MarketDataProvider)
    assert tuple(provider.get_bars(request)) == pages
    assert tuple(provider.get_instruments(as_of=date(2026, 8, 1))) == pages
