"""Offline Twelve Data normalization tests for feed, time, and partial-batch semantics."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pytest

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.normalization import (
    DailyBarSemantics,
    NormalizationError,
    NormalizationIssueCode,
    SessionBounds,
    StaticSessionSchedule,
    normalize_twelve_data_bars,
)
from investment_platform.data.providers import (
    BarRequest,
    ProviderInstrumentRef,
    TwelveDataCredentials,
    TwelveDataProvider,
)
from investment_platform.data.providers.http import HttpResponse
from tests.provider_fakes import QueueHttpTransport

pytestmark = pytest.mark.unit

_FIXTURES = Path(__file__).parents[1] / "fixtures" / "providers" / "twelve_data"
_RETRIEVED = datetime(2026, 8, 24, 12, tzinfo=UTC)
_INGESTED = datetime(2026, 8, 24, 12, 1, tzinfo=UTC)
_BATCH_ID = UUID("41000000-0000-4000-8000-000000000001")
_REFS = (
    ProviderInstrumentRef(
        instrument_id=UUID("10000000-0000-4000-8000-000000000001"),
        provider_identifier="XPH1",
    ),
    ProviderInstrumentRef(
        instrument_id=UUID("10000000-0000-4000-8000-000000000002"),
        provider_identifier="XPH2",
    ),
)
_SUMMER = SessionBounds(
    session_date=date(2025, 7, 2),
    start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
    end=datetime(2025, 7, 2, 20, tzinfo=UTC),
    source="synthetic fixed summer session",
)
_WINTER = SessionBounds(
    session_date=date(2025, 11, 3),
    start=datetime(2025, 11, 3, 14, 30, tzinfo=UTC),
    end=datetime(2025, 11, 3, 21, tzinfo=UTC),
    source="synthetic fixed winter session",
)
_EARLY_CLOSE = SessionBounds(
    session_date=date(2025, 7, 3),
    start=datetime(2025, 7, 3, 13, 30, tzinfo=UTC),
    end=datetime(2025, 7, 3, 17, tzinfo=UTC),
    source="synthetic fixed early close",
)
_SCHEDULE = StaticSessionSchedule((_SUMMER, _WINTER, _EARLY_CLOSE))


def _fixture(name: str) -> bytes:
    return (_FIXTURES / name).read_bytes()


def _request(
    *,
    refs: tuple[ProviderInstrumentRef, ...] = _REFS,
    timeframe: Timeframe = Timeframe.FIVE_MINUTES,
    adjustment: AdjustmentState = AdjustmentState.UNADJUSTED,
    start: datetime = datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
    end: datetime = datetime(2025, 7, 2, 20, tzinfo=UTC),
) -> BarRequest:
    return BarRequest(
        instruments=refs,
        timeframe=timeframe,
        start=start,
        end=end,
        session=TradingSession.REGULAR,
        adjustment_state=adjustment,
    )


def _batch(payload: bytes, request: BarRequest):  # type: ignore[no-untyped-def]
    provider = TwelveDataProvider(
        TwelveDataCredentials(api_key="synthetic-test-secret"),
        transport=QueueHttpTransport([HttpResponse(200, payload, elapsed_ms=4.0)]),
        clock=lambda: _RETRIEVED,
        batch_id_factory=lambda: _BATCH_ID,
    )
    (batch,) = tuple(provider.get_bars(request))
    return batch


def test_twelve_data_batch_maps_exact_uuid_utc_ohlcv_and_missing_vwap() -> None:
    request = _request()
    batch = _batch(_fixture("bars_5m_standard_batch.json"), request)

    result = normalize_twelve_data_bars(
        batch,
        request,
        ingested_at=_INGESTED,
        session_schedule=_SCHEDULE,
    )

    assert result.issues == ()
    assert [bar.instrument_id for bar in result.bars] == [
        _REFS[0].instrument_id,
        _REFS[1].instrument_id,
    ]
    assert all(bar.timestamp_start == _SUMMER.start for bar in result.bars)
    assert all(bar.timestamp_end == datetime(2025, 7, 2, 13, 35, tzinfo=UTC) for bar in result.bars)
    assert all(bar.vwap is None for bar in result.bars)
    assert [bar.volume for bar in result.bars] == [1000.0, 2000.0]
    assert all(bar.currency == "USD" for bar in result.bars)
    assert all(bar.adjustment_state is AdjustmentState.UNADJUSTED for bar in result.bars)


def test_twelve_data_intraday_naive_wire_timestamp_uses_requested_utc_across_dst() -> None:
    payload = {
        "meta": {
            "symbol": "XPH1",
            "interval": "5min",
            "currency": "USD",
            "exchange_timezone": "America/New_York",
        },
        "values": [
            {
                "datetime": "2025-11-03 14:30:00",
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100.5",
                "volume": "1000",
            }
        ],
        "status": "ok",
    }
    request = _request(
        refs=(_REFS[0],),
        start=_WINTER.start,
        end=_WINTER.end,
    )

    result = normalize_twelve_data_bars(
        _batch(json.dumps(payload).encode(), request),
        request,
        ingested_at=_INGESTED,
        session_schedule=_SCHEDULE,
    )

    assert result.issues == ()
    assert result.bars[0].timestamp_start == _WINTER.start


def test_twelve_data_daily_market_date_requires_verified_finite_session_semantics() -> None:
    request = _request(
        refs=(_REFS[0],),
        timeframe=Timeframe.ONE_DAY,
        start=datetime(2025, 7, 2, tzinfo=UTC),
        end=datetime(2025, 7, 3, tzinfo=UTC),
    )
    batch = _batch(_fixture("bars_daily_standard.json"), request)

    unverified = normalize_twelve_data_bars(
        batch,
        request,
        ingested_at=_INGESTED,
        session_schedule=_SCHEDULE,
    )
    verified = normalize_twelve_data_bars(
        batch,
        request,
        ingested_at=_INGESTED,
        session_schedule=_SCHEDULE,
        daily_semantics=DailyBarSemantics.REGULAR_SESSION,
    )

    assert unverified.bars == ()
    assert [issue.code for issue in unverified.issues] == [
        NormalizationIssueCode.DAILY_SEMANTICS_UNVERIFIED
    ]
    assert len(verified.bars) == 1
    assert verified.bars[0].timestamp_start == _SUMMER.start
    assert verified.bars[0].timestamp_end == _SUMMER.end


def test_twelve_data_daily_early_close_comes_only_from_the_finite_schedule() -> None:
    payload = json.loads(_fixture("bars_daily_standard.json"))
    payload["values"][0]["datetime"] = "2025-07-03"
    request = _request(
        refs=(_REFS[0],),
        timeframe=Timeframe.ONE_DAY,
        start=datetime(2025, 7, 3, tzinfo=UTC),
        end=datetime(2025, 7, 4, tzinfo=UTC),
    )

    result = normalize_twelve_data_bars(
        _batch(json.dumps(payload).encode(), request),
        request,
        ingested_at=_INGESTED,
        session_schedule=_SCHEDULE,
        daily_semantics=DailyBarSemantics.REGULAR_SESSION,
    )

    assert result.issues == ()
    assert result.bars[0].timestamp_end == _EARLY_CLOSE.end


def test_twelve_data_daily_timestamp_must_be_an_exact_market_date() -> None:
    payload = json.loads(_fixture("bars_daily_standard.json"))
    payload["values"][0]["datetime"] = "2025-07-02garbage"
    request = _request(
        refs=(_REFS[0],),
        timeframe=Timeframe.ONE_DAY,
        start=datetime(2025, 7, 2, tzinfo=UTC),
        end=datetime(2025, 7, 3, tzinfo=UTC),
    )

    result = normalize_twelve_data_bars(
        _batch(json.dumps(payload).encode(), request),
        request,
        ingested_at=_INGESTED,
        session_schedule=_SCHEDULE,
        daily_semantics=DailyBarSemantics.REGULAR_SESSION,
    )

    assert result.bars == ()
    assert [issue.code for issue in result.issues] == [NormalizationIssueCode.MALFORMED_RECORD]


def test_twelve_data_partial_batch_errors_and_missing_symbols_remain_diagnostic() -> None:
    payload = json.dumps(
        {"XPH1": {"status": "error", "code": 404, "message": "synthetic no data"}}
    ).encode()
    request = _request()

    result = normalize_twelve_data_bars(
        _batch(payload, request),
        request,
        ingested_at=_INGESTED,
        session_schedule=_SCHEDULE,
    )

    assert result.bars == ()
    assert [issue.code for issue in result.issues] == [
        NormalizationIssueCode.PROVIDER_RESPONSE_ERROR,
        NormalizationIssueCode.PROVIDER_RESPONSE_ERROR,
    ]
    assert all("synthetic no data" not in issue.message for issue in result.issues)


def test_twelve_data_inclusive_wire_end_is_filtered_by_canonical_half_open_interval() -> None:
    payload = json.loads(_fixture("bars_daily_standard.json"))
    payload["meta"]["interval"] = "5min"
    payload["values"] = [
        {
            "datetime": timestamp,
            "open": "100",
            "high": "101",
            "low": "99",
            "close": "100.5",
            "volume": "1000",
        }
        for timestamp in ("2025-07-02 13:30:00", "2025-07-02 13:35:00")
    ]
    request = _request(
        refs=(_REFS[0],),
        end=datetime(2025, 7, 2, 13, 35, tzinfo=UTC),
    )

    result = normalize_twelve_data_bars(
        _batch(json.dumps(payload).encode(), request),
        request,
        ingested_at=_INGESTED,
        session_schedule=_SCHEDULE,
    )

    assert len(result.bars) == 1
    assert result.bars[0].timestamp_start == request.start
    assert [issue.code for issue in result.issues] == [
        NormalizationIssueCode.OUTSIDE_REQUESTED_INTERVAL
    ]


def test_twelve_data_malformed_and_out_of_session_records_are_not_silently_rewritten() -> None:
    payload = json.loads(_fixture("bars_daily_standard.json"))
    payload["meta"]["interval"] = "5min"
    payload["values"] = [
        {"datetime": "2025-07-02 13:30:00", "open": "bad"},
        {
            "datetime": "2025-07-02 20:00:00",
            "open": "100",
            "high": "101",
            "low": "99",
            "close": "100.5",
            "volume": "1000",
        },
    ]
    request = _request(refs=(_REFS[0],))

    result = normalize_twelve_data_bars(
        _batch(json.dumps(payload).encode(), request),
        request,
        ingested_at=_INGESTED,
        session_schedule=_SCHEDULE,
    )

    assert result.bars == ()
    assert [issue.code for issue in result.issues] == [
        NormalizationIssueCode.MALFORMED_RECORD,
        NormalizationIssueCode.OUTSIDE_REQUESTED_SESSION,
    ]


def test_twelve_data_normalizer_rejects_source_and_request_metadata_mismatch() -> None:
    intraday_request = _request(refs=(_REFS[0],))
    batch = _batch(_fixture("bars_daily_standard.json"), intraday_request)
    daily_request = _request(
        refs=(_REFS[0],),
        timeframe=Timeframe.ONE_DAY,
        start=datetime(2025, 7, 2, tzinfo=UTC),
        end=datetime(2025, 7, 3, tzinfo=UTC),
    )

    with pytest.raises(NormalizationError, match="unexpected source identity"):
        normalize_twelve_data_bars(
            batch,
            daily_request,
            ingested_at=_INGESTED,
            session_schedule=_SCHEDULE,
        )

    altered_request = intraday_request.model_copy(
        update={"adjustment_state": AdjustmentState.SPLIT_ADJUSTED}
    )
    with pytest.raises(NormalizationError, match="does not match"):
        normalize_twelve_data_bars(
            batch,
            altered_request,
            ingested_at=_INGESTED,
            session_schedule=_SCHEDULE,
        )
