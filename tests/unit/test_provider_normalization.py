"""Offline canonical-normalization tests for the Phase 1 provider evidence."""

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from investment_platform.data.models import (
    DividendAction,
    SplitAction,
    TickerChangeAction,
    Timeframe,
    TradingSession,
)
from investment_platform.data.normalization import (
    DailyBarSemantics,
    NormalizationError,
    NormalizationIssueCode,
    SessionBounds,
    StaticSessionSchedule,
    normalize_alpaca_bars,
    normalize_alpaca_corporate_actions,
    normalize_massive_bars,
    normalize_massive_corporate_actions,
)
from investment_platform.data.provenance import (
    BytesRawPayload,
    DataSource,
    FileRawPayload,
    JsonScalar,
    LicenseClassification,
    RawBatch,
    RawBatchMetadata,
    RawPayload,
)
from investment_platform.data.providers import (
    BarRequest,
    CorporateActionRequest,
    ProviderInstrumentRef,
)
from investment_platform.data.providers.alpaca import (
    ALPACA_CORPORATE_ACTION_SOURCE,
    ALPACA_SIP_BAR_SOURCE,
)
from investment_platform.data.providers.base import (
    instrument_refs_fingerprint,
    instrument_refs_manifest,
)
from investment_platform.data.providers.massive import (
    MASSIVE_BAR_SOURCE,
    MASSIVE_DIVIDEND_SOURCE,
    MASSIVE_SPLIT_SOURCE,
    MASSIVE_TICKER_EVENT_SOURCE,
)

pytestmark = pytest.mark.unit

_FIXTURES = Path(__file__).parents[1] / "fixtures" / "providers"
_INSTRUMENT_ID = UUID("10000000-0000-4000-8000-000000000001")
_RETRIEVED_AT = datetime(2026, 8, 19, 12, tzinfo=UTC)
_INGESTED_AT = datetime(2026, 8, 19, 12, 5, tzinfo=UTC)
_FIVE_MINUTES = timedelta(minutes=5)

_SUMMER_SESSION = SessionBounds(
    session_date=date(2025, 7, 2),
    start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
    end=datetime(2025, 7, 2, 20, 0, tzinfo=UTC),
    source="synthetic verified schedule",
)
_WINTER_SESSION = SessionBounds(
    session_date=date(2025, 11, 3),
    start=datetime(2025, 11, 3, 14, 30, tzinfo=UTC),
    end=datetime(2025, 11, 3, 21, 0, tzinfo=UTC),
    source="synthetic verified schedule",
)
_SESSION_SCHEDULE = StaticSessionSchedule((_SUMMER_SESSION, _WINTER_SESSION))


def test_session_bounds_require_a_coherent_new_york_session_date() -> None:
    with pytest.raises(ValueError, match="session start"):
        SessionBounds(
            session_date=date(2025, 7, 3),
            start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
            end=datetime(2025, 7, 2, 20, 0, tzinfo=UTC),
            source="synthetic mismatch",
        )

    with pytest.raises(ValueError, match="same New York session date"):
        SessionBounds(
            session_date=date(2025, 7, 2),
            start=datetime(2025, 7, 2, 23, 30, tzinfo=UTC),
            end=datetime(2025, 7, 3, 4, 30, tzinfo=UTC),
            source="synthetic cross-date interval",
        )


def _synthetic_source(provider: str, dataset: str, logical_endpoint: str) -> DataSource:
    if provider == "massive" and dataset.startswith("price_bars"):
        source = MASSIVE_BAR_SOURCE
    elif provider == "alpaca" and dataset.startswith("price_bars"):
        source = ALPACA_SIP_BAR_SOURCE
    elif provider == "massive":
        sources = {
            "stocks/v1/splits": MASSIVE_SPLIT_SOURCE,
            "stocks/v1/dividends": MASSIVE_DIVIDEND_SOURCE,
            "vX/reference/tickers/events": MASSIVE_TICKER_EVENT_SOURCE,
        }
        source = sources[logical_endpoint]
    elif provider == "alpaca" and logical_endpoint == "v1/corporate-actions":
        source = ALPACA_CORPORATE_ACTION_SOURCE
    else:
        raise ValueError("unsupported synthetic provider source")
    return source.model_copy(update={"license_classification": LicenseClassification.SYNTHETIC})


def _raw_batch(
    *,
    provider: str,
    dataset: str,
    logical_endpoint: str,
    label: str,
    payload: RawPayload,
    request_metadata: dict[str, JsonScalar] | None = None,
) -> RawBatch:
    if request_metadata is None:
        request_metadata = (
            _bar_request_metadata(_bar_request())
            if dataset.startswith("price_bars")
            else _corporate_action_request_metadata(_corporate_action_request())
        )
    return RawBatch(
        metadata=RawBatchMetadata(
            batch_id=uuid5(NAMESPACE_URL, f"test-batch:{provider}:{label}"),
            source=_synthetic_source(provider, dataset, logical_endpoint),
            retrieved_at=_RETRIEVED_AT,
            media_type="application/json",
            file_extension="json",
            request_metadata=request_metadata,
        ),
        payload=payload,
    )


def _fixture_batch(
    provider: str,
    fixture_name: str,
    *,
    dataset: str,
    logical_endpoint: str,
    request_metadata: dict[str, JsonScalar] | None = None,
) -> RawBatch:
    return _raw_batch(
        provider=provider,
        dataset=dataset,
        logical_endpoint=logical_endpoint,
        label=fixture_name,
        payload=FileRawPayload(_FIXTURES / provider / fixture_name),
        request_metadata=request_metadata,
    )


def _content_batch(
    provider: str,
    content: bytes,
    *,
    label: str,
    dataset: str = "price_bars",
    logical_endpoint: str | None = None,
    request_metadata: dict[str, JsonScalar] | None = None,
) -> RawBatch:
    if logical_endpoint is None:
        logical_endpoint = (
            MASSIVE_BAR_SOURCE.logical_endpoint
            if provider == "massive"
            else ALPACA_SIP_BAR_SOURCE.logical_endpoint
        )
    return _raw_batch(
        provider=provider,
        dataset=dataset,
        logical_endpoint=logical_endpoint,
        label=label,
        payload=BytesRawPayload(content),
        request_metadata=request_metadata,
    )


def _bar_request(
    bounds: SessionBounds = _SUMMER_SESSION,
    *,
    timeframe: Timeframe = Timeframe.FIVE_MINUTES,
) -> BarRequest:
    return BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="XPH1",
            ),
        ),
        timeframe=timeframe,
        start=bounds.start,
        end=bounds.end,
        session=TradingSession.REGULAR,
    )


def _corporate_action_request(identifier: str = "XPH1") -> CorporateActionRequest:
    return CorporateActionRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier=identifier,
            ),
        ),
        start=date(2025, 1, 1),
        end=date(2026, 1, 1),
    )


def _bar_request_metadata(request: BarRequest) -> dict[str, JsonScalar]:
    return {
        "instrument_id": str(request.instruments[0].instrument_id),
        "provider_identifier": request.instruments[0].provider_identifier,
        "timeframe": request.timeframe.value,
        "start": request.start.isoformat(),
        "end_exclusive": request.end.isoformat(),
        "session": request.session.value,
        "adjustment_state": request.adjustment_state.value,
        "provider_adjustment": "raw",
        "canonical_persistence_eligible": True,
        "feed": "sip",
        "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
        "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
    }


def _corporate_action_request_metadata(
    request: CorporateActionRequest,
) -> dict[str, JsonScalar]:
    return {
        "instrument_id": str(request.instruments[0].instrument_id),
        "provider_identifier": request.instruments[0].provider_identifier,
        "start": request.start.isoformat(),
        "end_exclusive": request.end.isoformat(),
        "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
        "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
    }


def _single_bar_payload(provider: str, timestamp: datetime) -> bytes:
    record: dict[str, object] = {
        "o": 100.0,
        "h": 101.0,
        "l": 99.5,
        "c": 100.5,
        "v": 1200,
        "vw": 100.25,
    }
    if provider == "alpaca":
        record["t"] = timestamp.isoformat().replace("+00:00", "Z")
        payload: object = {"bars": {"XPH1": [record]}, "next_page_token": None}
    elif provider == "massive":
        record["t"] = int(timestamp.timestamp() * 1000)
        payload = {
            "ticker": "XPH1",
            "status": "OK",
            "adjusted": False,
            "results": [record],
        }
    else:
        raise ValueError(f"unsupported test provider {provider!r}")
    return json.dumps(payload, separators=(",", ":")).encode()


def test_alpaca_5m_bounds_keep_rth_bars_and_report_inclusive_end_extra() -> None:
    first_batch = _fixture_batch(
        "alpaca",
        "bars_5m_sip_page_1.json",
        dataset="price_bars_sip",
        logical_endpoint="v2/stocks/bars",
    )
    second_batch = _fixture_batch(
        "alpaca",
        "bars_5m_sip_page_2.json",
        dataset="price_bars_sip",
        logical_endpoint="v2/stocks/bars",
    )
    request = _bar_request()

    first = normalize_alpaca_bars(
        first_batch,
        request,
        ingested_at=_INGESTED_AT,
        session_schedule=_SESSION_SCHEDULE,
    )
    second = normalize_alpaca_bars(
        second_batch,
        request,
        ingested_at=_INGESTED_AT,
        session_schedule=_SESSION_SCHEDULE,
    )

    bars = first.bars + second.bars
    assert [bar.timestamp_start for bar in bars] == [
        datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        datetime(2025, 7, 2, 13, 35, tzinfo=UTC),
    ]
    assert all(bar.timestamp_end - bar.timestamp_start == _FIVE_MINUTES for bar in bars)
    assert all(bar.session is TradingSession.REGULAR for bar in bars)
    assert second.issues[0].code is NormalizationIssueCode.OUTSIDE_REQUESTED_SESSION
    assert second.issues[0].record_index == 1


def test_alpaca_multi_symbol_pages_preserve_exact_uuid_and_raw_provenance() -> None:
    second_instrument_id = UUID("10000000-0000-4000-8000-000000000002")
    request = BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="XPH1",
            ),
            ProviderInstrumentRef(
                instrument_id=second_instrument_id,
                provider_identifier="XPH2",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        start=_SUMMER_SESSION.start,
        end=_SUMMER_SESSION.end,
        session=TradingSession.REGULAR,
    )
    payload = json.dumps(
        {
            "bars": {
                "XPH1": [
                    {
                        "t": "2025-07-02T13:30:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99,
                        "c": 100.5,
                        "v": 1000,
                    }
                ],
                "XPH2": [
                    {
                        "t": "2025-07-02T13:35:00Z",
                        "o": 200,
                        "h": 201,
                        "l": 199,
                        "c": 200.5,
                        "v": 2000,
                    }
                ],
                "UNEXPECTED": [
                    {
                        "t": "2025-07-02T13:40:00Z",
                        "o": 300,
                        "h": 301,
                        "l": 299,
                        "c": 300.5,
                        "v": 3000,
                    }
                ],
            }
        }
    ).encode()
    batch = _content_batch(
        "alpaca",
        payload,
        label="multi-symbol",
        request_metadata=_bar_request_metadata(request),
    )

    result = normalize_alpaca_bars(
        batch,
        request,
        ingested_at=_INGESTED_AT,
        session_schedule=_SESSION_SCHEDULE,
    )

    assert [bar.instrument_id for bar in result.bars] == [
        _INSTRUMENT_ID,
        second_instrument_id,
    ]
    assert all(bar.raw_batch_id == batch.metadata.batch_id for bar in result.bars)
    assert all(bar.source_id == ALPACA_SIP_BAR_SOURCE.source_id for bar in result.bars)
    assert [issue.code for issue in result.issues] == [NormalizationIssueCode.UNMAPPED_INSTRUMENT]


def test_alpaca_normalizer_binds_feed_metadata_to_the_source_identity() -> None:
    request = _bar_request()
    batch = _content_batch(
        "alpaca",
        _single_bar_payload("alpaca", _SUMMER_SESSION.start),
        label="feed-mismatch",
        request_metadata={**_bar_request_metadata(request), "feed": "iex"},
    )

    with pytest.raises(NormalizationError, match="does not match"):
        normalize_alpaca_bars(
            batch,
            request,
            ingested_at=_INGESTED_AT,
            session_schedule=_SESSION_SCHEDULE,
        )


def test_massive_5m_uses_explicit_summer_and_winter_dst_session_bounds() -> None:
    for bounds in (_SUMMER_SESSION, _WINTER_SESSION):
        batch = _content_batch(
            "massive",
            _single_bar_payload("massive", bounds.start),
            label=f"dst-{bounds.session_date}",
            request_metadata=_bar_request_metadata(_bar_request(bounds)),
        )

        result = normalize_massive_bars(
            batch,
            _bar_request(bounds),
            ingested_at=_INGESTED_AT,
            session_schedule=_SESSION_SCHEDULE,
        )

        assert not result.issues
        assert len(result.bars) == 1
        assert result.bars[0].timestamp_start == bounds.start
        assert result.bars[0].timestamp_end == bounds.start + _FIVE_MINUTES

    assert _SUMMER_SESSION.start.hour == 13
    assert _WINTER_SESSION.start.hour == 14


def test_finite_schedule_enforces_an_early_close_without_calendar_inference() -> None:
    early_close = SessionBounds(
        session_date=date(2025, 7, 3),
        start=datetime(2025, 7, 3, 13, 30, tzinfo=UTC),
        end=datetime(2025, 7, 3, 17, 0, tzinfo=UTC),
        source="synthetic early-close oracle",
    )
    schedule = StaticSessionSchedule((early_close,))
    request = BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="XPH1",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        start=early_close.start,
        end=early_close.end + _FIVE_MINUTES,
        session=TradingSession.REGULAR,
    )
    payload = json.dumps(
        {
            "ticker": "XPH1",
            "status": "OK",
            "adjusted": False,
            "results": [
                {
                    "t": int((early_close.end - _FIVE_MINUTES).timestamp() * 1000),
                    "o": 100,
                    "h": 101,
                    "l": 99,
                    "c": 100.5,
                    "v": 1000,
                },
                {
                    "t": int(early_close.end.timestamp() * 1000),
                    "o": 100.5,
                    "h": 101,
                    "l": 100,
                    "c": 100.75,
                    "v": 500,
                },
            ],
        }
    ).encode()
    batch = _content_batch(
        "massive",
        payload,
        label="early-close",
        request_metadata=_bar_request_metadata(request),
    )

    result = normalize_massive_bars(
        batch,
        request,
        ingested_at=_INGESTED_AT,
        session_schedule=schedule,
    )

    assert [bar.timestamp_start for bar in result.bars] == [early_close.end - _FIVE_MINUTES]
    assert [issue.code for issue in result.issues] == [
        NormalizationIssueCode.OUTSIDE_REQUESTED_SESSION
    ]


def test_closed_date_without_registered_bounds_is_reported_not_inferred() -> None:
    closed_start = datetime(2025, 7, 4, 13, 30, tzinfo=UTC)
    request = BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="XPH1",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        start=closed_start,
        end=closed_start + _FIVE_MINUTES,
        session=TradingSession.REGULAR,
    )
    batch = _content_batch(
        "massive",
        _single_bar_payload("massive", closed_start),
        label="closed-date",
        request_metadata=_bar_request_metadata(request),
    )

    result = normalize_massive_bars(
        batch,
        request,
        ingested_at=_INGESTED_AT,
        session_schedule=StaticSessionSchedule(()),
    )

    assert not result.bars
    assert [issue.code for issue in result.issues] == [
        NormalizationIssueCode.SESSION_BOUNDS_MISSING
    ]


def test_daily_bars_require_explicit_semantics_before_using_session_bounds() -> None:
    request = _bar_request(timeframe=Timeframe.ONE_DAY)
    alpaca_batch = _fixture_batch(
        "alpaca",
        "bars_daily_sip.json",
        dataset="price_bars_sip",
        logical_endpoint="v2/stocks/bars",
        request_metadata=_bar_request_metadata(request),
    )
    massive_batch = _fixture_batch(
        "massive",
        "bars_daily.json",
        dataset="price_bars",
        logical_endpoint="v2/aggs",
        request_metadata=_bar_request_metadata(request),
    )

    alpaca_blocked = normalize_alpaca_bars(
        alpaca_batch,
        request,
        ingested_at=_INGESTED_AT,
        session_schedule=_SESSION_SCHEDULE,
    )
    massive_blocked = normalize_massive_bars(
        massive_batch,
        request,
        ingested_at=_INGESTED_AT,
        session_schedule=_SESSION_SCHEDULE,
    )

    for result in (alpaca_blocked, massive_blocked):
        assert not result.bars
        assert [issue.code for issue in result.issues] == [
            NormalizationIssueCode.DAILY_SEMANTICS_UNVERIFIED
        ]

    alpaca_verified = normalize_alpaca_bars(
        alpaca_batch,
        request,
        ingested_at=_INGESTED_AT,
        session_schedule=_SESSION_SCHEDULE,
        daily_semantics=DailyBarSemantics.REGULAR_SESSION,
    )
    massive_verified = normalize_massive_bars(
        massive_batch,
        request,
        ingested_at=_INGESTED_AT,
        session_schedule=_SESSION_SCHEDULE,
        daily_semantics=DailyBarSemantics.REGULAR_SESSION,
    )

    for result in (alpaca_verified, massive_verified):
        assert not result.issues
        assert len(result.bars) == 1
        assert result.bars[0].timestamp_start == _SUMMER_SESSION.start
        assert result.bars[0].timestamp_end == _SUMMER_SESSION.end


def test_massive_maps_split_and_cash_dividend_without_collapsing_stock_dividend() -> None:
    split_batch = _fixture_batch(
        "massive",
        "splits_mixed.json",
        dataset="corporate_actions",
        logical_endpoint="stocks/v1/splits",
    )
    dividend_batch = _fixture_batch(
        "massive",
        "dividends.json",
        dataset="corporate_actions",
        logical_endpoint="stocks/v1/dividends",
    )
    request = _corporate_action_request()

    splits = normalize_massive_corporate_actions(
        split_batch,
        request,
        ingested_at=_INGESTED_AT,
    )
    dividends = normalize_massive_corporate_actions(
        dividend_batch,
        request,
        ingested_at=_INGESTED_AT,
    )

    assert len(splits.actions) == 1
    split = splits.actions[0]
    assert isinstance(split, SplitAction)
    assert split.split_ratio == Decimal("15")
    assert split.effective_date == date(2025, 6, 10)
    assert [issue.code for issue in splits.issues] == [
        NormalizationIssueCode.UNSUPPORTED_CORPORATE_ACTION
    ]
    assert splits.issues[0].provider_record_id == "SYNTHETIC-STOCK-DIVIDEND-1"

    assert len(dividends.actions) == 1
    dividend = dividends.actions[0]
    assert isinstance(dividend, DividendAction)
    assert dividend.amount == Decimal("0.25")
    assert dividend.currency == "USD"
    assert dividend.effective_date == date(2025, 6, 16)
    assert not dividends.issues


def test_massive_unknown_split_adjustment_is_not_mislabeled_as_a_stock_dividend() -> None:
    payload = json.dumps(
        {
            "status": "OK",
            "results": [
                {
                    "execution_date": "2025-06-10",
                    "split_from": 1,
                    "split_to": 2,
                    "adjustment_type": "synthetic_unknown_type",
                    "ticker": "XPH1",
                    "id": "SYNTHETIC-UNKNOWN-SPLIT",
                }
            ],
        }
    ).encode()
    request = _corporate_action_request()
    batch = _content_batch(
        "massive",
        payload,
        label="unknown-split-type",
        dataset="corporate_actions",
        logical_endpoint="stocks/v1/splits",
        request_metadata=_corporate_action_request_metadata(request),
    )

    result = normalize_massive_corporate_actions(batch, request, ingested_at=_INGESTED_AT)

    assert not result.actions
    assert [issue.code for issue in result.issues] == [
        NormalizationIssueCode.UNSUPPORTED_CORPORATE_ACTION
    ]
    assert "stock-dividend" not in result.issues[0].message


def test_massive_ticker_timeline_marks_provider_definition_inference() -> None:
    request = _corporate_action_request("XNEW")
    batch = _fixture_batch(
        "massive",
        "ticker_events.json",
        dataset="corporate_actions",
        logical_endpoint="vX/reference/tickers/events",
        request_metadata=_corporate_action_request_metadata(request),
    )

    result = normalize_massive_corporate_actions(
        batch,
        request,
        ingested_at=_INGESTED_AT,
    )

    assert len(result.actions) == 1
    change = result.actions[0]
    assert isinstance(change, TickerChangeAction)
    assert (change.old_ticker, change.new_ticker) == ("XOLD", "XNEW")
    assert [issue.code for issue in result.issues] == [NormalizationIssueCode.PROVIDER_DEFINITION]


def test_alpaca_actions_map_supported_records_and_surface_semantic_gaps() -> None:
    batch = _fixture_batch(
        "alpaca",
        "corporate_actions_mixed.json",
        dataset="corporate_actions",
        logical_endpoint="v1/corporate-actions",
    )

    result = normalize_alpaca_corporate_actions(
        batch,
        _corporate_action_request(),
        ingested_at=_INGESTED_AT,
    )

    assert len(result.actions) == 2
    split, dividend = result.actions
    assert isinstance(split, SplitAction)
    assert split.split_ratio == Decimal("15")
    assert isinstance(dividend, DividendAction)
    assert dividend.amount == Decimal("0.25")
    assert dividend.currency == "USD"

    assert [issue.code for issue in result.issues] == [
        NormalizationIssueCode.CORPORATE_ACTION_DATE_BASIS,
        NormalizationIssueCode.INCOMPLETE_CORPORATE_ACTION,
        NormalizationIssueCode.INCOMPLETE_CORPORATE_ACTION,
        NormalizationIssueCode.UNSUPPORTED_CORPORATE_ACTION,
    ]
    issues_by_id = {issue.provider_record_id: issue for issue in result.issues}
    missing_currency = issues_by_id["SYNTHETIC-DIVIDEND-NO-CURRENCY"]
    assert "must not be inferred as USD" in missing_currency.message
    name_change = issues_by_id["SYNTHETIC-NAME-CHANGE"]
    assert "process_date" in name_change.message
    assert "stock_dividends" in result.issues[-1].message
    assert "process_date" in result.issues[0].message


def test_invalid_json_payloads_raise_normalization_error_for_both_providers() -> None:
    request = _bar_request()
    alpaca_batch = _content_batch("alpaca", b'{"bars":', label="invalid-json")
    massive_batch = _content_batch("massive", b'{"results":', label="invalid-json")

    with pytest.raises(NormalizationError, match="not valid JSON"):
        normalize_alpaca_bars(
            alpaca_batch,
            request,
            ingested_at=_INGESTED_AT,
            session_schedule=_SESSION_SCHEDULE,
        )
    with pytest.raises(NormalizationError, match="not valid JSON"):
        normalize_massive_bars(
            massive_batch,
            request,
            ingested_at=_INGESTED_AT,
            session_schedule=_SESSION_SCHEDULE,
        )


def test_malformed_bar_records_are_reported_without_partial_canonical_rows() -> None:
    timestamp = _SUMMER_SESSION.start
    alpaca_payload = json.dumps(
        {"bars": {"XPH1": [{"t": timestamp.isoformat(), "o": 100.0}]}}
    ).encode()
    massive_payload = json.dumps(
        {
            "ticker": "XPH1",
            "status": "OK",
            "adjusted": False,
            "results": [{"t": int(timestamp.timestamp() * 1000), "o": 100.0}],
        }
    ).encode()

    alpaca = normalize_alpaca_bars(
        _content_batch("alpaca", alpaca_payload, label="malformed-record"),
        _bar_request(),
        ingested_at=_INGESTED_AT,
        session_schedule=_SESSION_SCHEDULE,
    )
    massive = normalize_massive_bars(
        _content_batch("massive", massive_payload, label="malformed-record"),
        _bar_request(),
        ingested_at=_INGESTED_AT,
        session_schedule=_SESSION_SCHEDULE,
    )

    for result in (alpaca, massive):
        assert not result.bars
        assert [issue.code for issue in result.issues] == [NormalizationIssueCode.MALFORMED_RECORD]


def test_massive_out_of_range_epoch_is_reported_as_a_malformed_record() -> None:
    payload = json.dumps(
        {
            "ticker": "XPH1",
            "status": "OK",
            "adjusted": False,
            "results": [
                {
                    "t": 10**30,
                    "o": 100.0,
                    "h": 101.0,
                    "l": 99.5,
                    "c": 100.5,
                    "v": 1200,
                }
            ],
        }
    ).encode()

    result = normalize_massive_bars(
        _content_batch("massive", payload, label="epoch-overflow"),
        _bar_request(),
        ingested_at=_INGESTED_AT,
        session_schedule=_SESSION_SCHEDULE,
    )

    assert not result.bars
    assert [issue.code for issue in result.issues] == [NormalizationIssueCode.MALFORMED_RECORD]


def test_massive_requires_explicit_success_status_for_bar_payloads() -> None:
    payload = _single_bar_payload("massive", _SUMMER_SESSION.start).replace(b'"status":"OK",', b"")

    with pytest.raises(NormalizationError, match="status was not OK"):
        normalize_massive_bars(
            _content_batch("massive", payload, label="missing-status"),
            _bar_request(),
            ingested_at=_INGESTED_AT,
            session_schedule=_SESSION_SCHEDULE,
        )


@pytest.mark.parametrize("provider", ["massive", "alpaca"])
def test_normalizers_reject_raw_metadata_from_a_different_canonical_request(
    provider: str,
) -> None:
    batch = _content_batch(
        provider,
        _single_bar_payload(provider, _SUMMER_SESSION.start),
        label="request-mismatch",
    )
    mismatched = RawBatch(
        metadata=batch.metadata.model_copy(
            update={"request_metadata": {"timeframe": Timeframe.ONE_DAY.value}}
        ),
        payload=batch.payload,
    )
    normalizer = normalize_massive_bars if provider == "massive" else normalize_alpaca_bars

    with pytest.raises(NormalizationError, match="does not match"):
        normalizer(
            mismatched,
            _bar_request(),
            ingested_at=_INGESTED_AT,
            session_schedule=_SESSION_SCHEDULE,
        )


@pytest.mark.parametrize("provider", ["massive", "alpaca"])
def test_normalizers_bind_provider_identifiers_to_the_persisted_internal_uuid(
    provider: str,
) -> None:
    batch = _content_batch(
        provider,
        _single_bar_payload(provider, _SUMMER_SESSION.start),
        label="uuid-binding",
    )
    request = _bar_request().model_copy(
        update={
            "instruments": (
                ProviderInstrumentRef(
                    instrument_id=UUID("10000000-0000-4000-8000-000000000099"),
                    provider_identifier="XPH1",
                ),
            )
        }
    )
    normalizer = normalize_massive_bars if provider == "massive" else normalize_alpaca_bars

    with pytest.raises(NormalizationError, match="does not match"):
        normalizer(
            batch,
            request,
            ingested_at=_INGESTED_AT,
            session_schedule=_SESSION_SCHEDULE,
        )


def test_massive_normalizer_rejects_unexpected_adjustment_and_ticker_event_status() -> None:
    adjusted_payload = _single_bar_payload("massive", _SUMMER_SESSION.start).replace(
        b'"adjusted":false', b'"adjusted":true'
    )
    with pytest.raises(NormalizationError, match="adjusted flag disagrees"):
        normalize_massive_bars(
            _content_batch("massive", adjusted_payload, label="adjustment-mismatch"),
            _bar_request(),
            ingested_at=_INGESTED_AT,
            session_schedule=_SESSION_SCHEDULE,
        )

    request = _corporate_action_request("XNEW")
    error_batch = _content_batch(
        "massive",
        b'{"status":"ERROR","results":{"events":[]}}',
        label="ticker-event-status",
        dataset="corporate_actions",
        logical_endpoint="vX/reference/tickers/events",
        request_metadata=_corporate_action_request_metadata(request),
    )
    with pytest.raises(NormalizationError, match="status was not OK"):
        normalize_massive_corporate_actions(
            error_batch,
            request,
            ingested_at=_INGESTED_AT,
        )


@pytest.mark.parametrize(
    ("provider", "wrong_source"),
    [
        ("massive", MASSIVE_SPLIT_SOURCE),
        ("alpaca", ALPACA_CORPORATE_ACTION_SOURCE),
    ],
)
def test_bar_normalizers_reject_payloads_with_the_wrong_dataset_identity(
    provider: str,
    wrong_source: DataSource,
) -> None:
    batch = _content_batch(
        provider,
        _single_bar_payload(provider, _SUMMER_SESSION.start),
        label="wrong-source",
    )
    mismatched = RawBatch(
        metadata=batch.metadata.model_copy(update={"source": wrong_source}),
        payload=batch.payload,
    )
    normalizer = normalize_massive_bars if provider == "massive" else normalize_alpaca_bars

    with pytest.raises(NormalizationError, match="unexpected source identity"):
        normalizer(
            mismatched,
            _bar_request(),
            ingested_at=_INGESTED_AT,
            session_schedule=_SESSION_SCHEDULE,
        )


def test_massive_empty_ticker_timeline_is_valid_availability_evidence() -> None:
    request = _corporate_action_request("XNEW")
    batch = _content_batch(
        "massive",
        b'{"status":"OK","results":{"events":[]}}',
        label="empty-ticker-timeline",
        dataset="corporate_actions",
        logical_endpoint="vX/reference/tickers/events",
        request_metadata=_corporate_action_request_metadata(request),
    )

    result = normalize_massive_corporate_actions(batch, request, ingested_at=_INGESTED_AT)

    assert not result.actions
    assert [issue.code for issue in result.issues] == [NormalizationIssueCode.PROVIDER_DEFINITION]
    assert "no usable events" in result.issues[0].message


def test_massive_ticker_timeline_must_be_anchored_to_the_requested_identifier() -> None:
    request = _corporate_action_request("XREQUESTED")
    payload = json.dumps(
        {
            "status": "OK",
            "results": {
                "events": [
                    {
                        "date": "2025-01-01",
                        "type": "ticker_change",
                        "ticker_change": {"ticker": "XOTHER"},
                    }
                ]
            },
        }
    ).encode()
    batch = _content_batch(
        "massive",
        payload,
        label="unanchored-ticker-timeline",
        dataset="corporate_actions",
        logical_endpoint="vX/reference/tickers/events",
        request_metadata=_corporate_action_request_metadata(request),
    )

    with pytest.raises(NormalizationError, match="not anchored"):
        normalize_massive_corporate_actions(batch, request, ingested_at=_INGESTED_AT)


def test_massive_ticker_timeline_rejects_ambiguous_same_date_changes() -> None:
    request = _corporate_action_request("XNEW")
    payload = json.dumps(
        {
            "status": "OK",
            "results": {
                "events": [
                    {
                        "date": "2025-01-01",
                        "type": "ticker_change",
                        "ticker_change": {"ticker": "XNEW"},
                    },
                    {
                        "date": "2025-01-01",
                        "type": "ticker_change",
                        "ticker_change": {"ticker": "XOTHER"},
                    },
                ]
            },
        }
    ).encode()
    batch = _content_batch(
        "massive",
        payload,
        label="ambiguous-ticker-timeline",
        dataset="corporate_actions",
        logical_endpoint="vX/reference/tickers/events",
        request_metadata=_corporate_action_request_metadata(request),
    )

    with pytest.raises(NormalizationError, match="ambiguous same-date"):
        normalize_massive_corporate_actions(batch, request, ingested_at=_INGESTED_AT)
