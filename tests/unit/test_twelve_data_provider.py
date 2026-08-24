"""Offline request, batching, entitlement, and failure tests for Twelve Data Basic."""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.providers import (
    BarRequest,
    CorporateActionRequest,
    MarketDataProvider,
    ProviderAuthenticationError,
    ProviderCapabilityError,
    ProviderConfigurationError,
    ProviderEntitlementError,
    ProviderHttpError,
    ProviderInstrumentRef,
    ProviderRateLimitError,
    ProviderResponseError,
    TwelveDataCredentials,
    TwelveDataEvidenceAdjustment,
    TwelveDataProvider,
)
from investment_platform.data.providers.http import HttpResponse
from investment_platform.data.providers.twelve_data import (
    TWELVE_DATA_DAILY_BAR_SOURCE,
    TWELVE_DATA_INTRADAY_BAR_SOURCE,
)
from tests.provider_fakes import QueueHttpTransport

pytestmark = pytest.mark.unit

_FIXTURES = Path(__file__).parents[1] / "fixtures" / "providers" / "twelve_data"
_NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
_BATCH_IDS = (
    UUID("40000000-0000-4000-8000-000000000001"),
    UUID("40000000-0000-4000-8000-000000000002"),
    UUID("40000000-0000-4000-8000-000000000003"),
)


def _fixture(name: str) -> bytes:
    return (_FIXTURES / name).read_bytes()


def _references(count: int = 1) -> tuple[ProviderInstrumentRef, ...]:
    return tuple(
        ProviderInstrumentRef(
            instrument_id=UUID(f"10000000-0000-4000-8000-{index:012d}"),
            provider_identifier=f"XPH{index}",
        )
        for index in range(1, count + 1)
    )


def _bar_request(
    *,
    instruments: tuple[ProviderInstrumentRef, ...] | None = None,
    timeframe: Timeframe = Timeframe.FIVE_MINUTES,
    adjustment: AdjustmentState = AdjustmentState.UNADJUSTED,
    session: TradingSession = TradingSession.REGULAR,
    start: datetime | None = None,
    end: datetime | None = None,
) -> BarRequest:
    request_start = start or datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
    request_end = end or datetime(2025, 7, 2, 20, 0, tzinfo=UTC)
    return BarRequest(
        instruments=instruments or _references(),
        timeframe=timeframe,
        start=request_start,
        end=request_end,
        session=session,
        adjustment_state=adjustment,
    )


def _provider(
    transport: QueueHttpTransport,
    *,
    batch_ids: tuple[UUID, ...] = _BATCH_IDS,
) -> TwelveDataProvider:
    identifiers = iter(batch_ids)
    return TwelveDataProvider(
        TwelveDataCredentials(api_key="twelve-data-test-secret"),
        transport=transport,
        clock=lambda: _NOW,
        batch_id_factory=identifiers.__next__,
    )


def test_twelve_data_credentials_are_redacted_required_and_origin_bound() -> None:
    credentials = TwelveDataCredentials(api_key="twelve-data-test-secret")

    assert "twelve-data-test-secret" not in repr(credentials)
    assert TwelveDataCredentials.from_environment(
        {"TWELVE_DATA_API_KEY": "environment-test-secret"}
    ) == TwelveDataCredentials(api_key="environment-test-secret")
    with pytest.raises(ProviderConfigurationError, match="API key is missing"):
        TwelveDataCredentials.from_environment({})
    with pytest.raises(ValueError, match="official API origin"):
        TwelveDataProvider(credentials, base_url="https://attacker.invalid")


def test_twelve_data_batches_at_eight_credits_and_preserves_exact_raw_payloads() -> None:
    payload = _fixture("bars_5m_standard_batch.json")
    transport = QueueHttpTransport(
        [
            HttpResponse(
                200,
                payload,
                {"api-credits-used": "8", "api-credits-left": "0"},
                12.5,
            ),
            HttpResponse(200, payload, {}, 10.0),
        ]
    )
    provider = _provider(transport, batch_ids=_BATCH_IDS[:2])
    request = _bar_request(instruments=_references(10))

    batches = tuple(provider.get_bars(request))

    assert isinstance(provider, MarketDataProvider)
    assert len(batches) == 2
    first_query = dict(transport.requests[0].query)
    second_query = dict(transport.requests[1].query)
    assert first_query == {
        "symbol": "XPH1,XPH2,XPH3,XPH4,XPH5,XPH6,XPH7,XPH8",
        "interval": "5min",
        "start_date": "2025-07-02 13:30:00",
        "end_date": "2025-07-02 20:00:00",
        "order": "asc",
        "timezone": "UTC",
        "prepost": "false",
        "adjust": "none",
        "format": "JSON",
    }
    assert second_query["symbol"] == "XPH9,XPH10"
    assert all(
        captured.headers == {"Authorization": "apikey twelve-data-test-secret"}
        for captured in transport.requests
    )
    assert all("apikey" not in dict(captured.query) for captured in transport.requests)
    assert [batch.metadata.request_metadata["api_credit_cost"] for batch in batches] == [8, 2]
    assert [batch.metadata.request_metadata["chunk_number"] for batch in batches] == [1, 2]
    assert batches[0].metadata.request_metadata["rate_limit_remaining"] == 0
    assert batches[0].metadata.request_metadata["api_credits_used"] == 8
    assert batches[0].metadata.source == TWELVE_DATA_INTRADAY_BAR_SOURCE
    with batches[0].payload.open_binary() as reader:
        assert reader.read() == payload
    metadata = str(batches[0].metadata.model_dump(mode="json"))
    assert "twelve-data-test-secret" not in metadata
    assert "Authorization" not in metadata


def test_twelve_data_daily_bounds_source_and_split_adjustment_are_explicit() -> None:
    transport = QueueHttpTransport(
        [HttpResponse(200, _fixture("bars_daily_standard.json"), elapsed_ms=8.0)]
    )
    provider = _provider(transport, batch_ids=_BATCH_IDS[:1])
    request = _bar_request(
        timeframe=Timeframe.ONE_DAY,
        adjustment=AdjustmentState.SPLIT_ADJUSTED,
        start=datetime(2025, 5, 27, tzinfo=UTC),
        end=datetime(2025, 12, 6, tzinfo=UTC),
    )

    (batch,) = tuple(provider.get_bars(request))

    query = dict(transport.requests[0].query)
    assert query["interval"] == "1day"
    assert query["start_date"] == "2025-05-27"
    assert query["end_date"] == "2025-12-06"
    assert query["adjust"] == "splits"
    assert "outputsize" not in query
    assert "dp" not in query
    assert batch.metadata.source == TWELVE_DATA_DAILY_BAR_SOURCE
    assert batch.metadata.request_metadata["adjustment_state"] == "split_adjusted"
    assert batch.metadata.request_metadata["provider_adjustment"] == "splits"


def test_twelve_data_daily_wire_bound_includes_last_admissible_non_midnight_date() -> None:
    transport = QueueHttpTransport(
        [HttpResponse(200, _fixture("bars_daily_standard.json"), elapsed_ms=8.0)]
    )
    provider = _provider(transport, batch_ids=_BATCH_IDS[:1])
    request = _bar_request(
        timeframe=Timeframe.ONE_DAY,
        start=datetime(2025, 12, 5, tzinfo=UTC),
        end=datetime(2025, 12, 5, 22, tzinfo=UTC),
    )

    tuple(provider.get_bars(request))

    query = dict(transport.requests[0].query)
    assert query["start_date"] == "2025-12-05"
    assert query["end_date"] == "2025-12-06"


def test_twelve_data_wire_bounds_expand_subsecond_intervals_without_omitting_data() -> None:
    transport = QueueHttpTransport([HttpResponse(200, _fixture("bars_5m_standard_batch.json"))])
    provider = _provider(transport, batch_ids=_BATCH_IDS[:1])
    request = _bar_request(
        start=datetime(2025, 7, 2, 13, 30, 0, 500000, tzinfo=UTC),
        end=datetime(2025, 7, 2, 13, 35, 0, 500000, tzinfo=UTC),
    )

    tuple(provider.get_bars(request))

    query = dict(transport.requests[0].query)
    assert query["start_date"] == "2025-07-02 13:30:00"
    assert query["end_date"] == "2025-07-02 13:35:01"


@pytest.mark.parametrize(
    "adjustment",
    [TwelveDataEvidenceAdjustment.DIVIDENDS, TwelveDataEvidenceAdjustment.ALL],
)
def test_twelve_data_provider_adjustment_evidence_is_not_canonical(
    adjustment: TwelveDataEvidenceAdjustment,
) -> None:
    transport = QueueHttpTransport(
        [HttpResponse(200, _fixture("bars_daily_standard.json"), elapsed_ms=8.0)]
    )
    provider = _provider(transport, batch_ids=_BATCH_IDS[:1])

    (batch,) = tuple(provider.get_adjustment_evidence(_bar_request(), adjustment=adjustment))

    assert dict(transport.requests[0].query)["adjust"] == adjustment.value
    assert batch.metadata.request_metadata["adjustment_state"] == "provider_evidence_only"
    assert batch.metadata.request_metadata["canonical_persistence_eligible"] is False


def test_twelve_data_rejects_unrepresentable_adjustment_session_and_oversized_range() -> None:
    transport = QueueHttpTransport([])
    provider = _provider(transport)

    with pytest.raises(ProviderCapabilityError, match="cannot map canonical adjustment"):
        tuple(
            provider.get_bars(_bar_request(adjustment=AdjustmentState.SPLIT_AND_DIVIDEND_ADJUSTED))
        )
    with pytest.raises(ProviderCapabilityError, match="pre_market"):
        tuple(provider.get_bars(_bar_request(session=TradingSession.PRE_MARKET)))
    start = datetime(2025, 1, 1, tzinfo=UTC)
    with pytest.raises(ProviderCapabilityError, match="5000-point"):
        tuple(provider.get_bars(_bar_request(start=start, end=start + timedelta(minutes=5 * 5000))))
    with pytest.raises(ValueError, match="unadjusted base request"):
        tuple(
            provider.get_adjustment_evidence(
                _bar_request(adjustment=AdjustmentState.SPLIT_ADJUSTED),
                adjustment=TwelveDataEvidenceAdjustment.ALL,
            )
        )

    assert not transport.requests


@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [
        (401, ProviderAuthenticationError),
        (403, ProviderEntitlementError),
        (429, ProviderRateLimitError),
        (500, ProviderHttpError),
    ],
)
def test_twelve_data_http_failures_are_typed_sanitized_and_not_retried(
    status_code: int,
    error_type: type[ProviderHttpError],
) -> None:
    transport = QueueHttpTransport(
        [HttpResponse(status_code, b'{"status":"error"}', {"Retry-After": "12"})]
    )
    provider = _provider(transport)

    with pytest.raises(error_type) as captured:
        tuple(provider.get_bars(_bar_request()))

    assert captured.value.status_code == status_code
    assert captured.value.retry_after_seconds == 12
    assert "twelve-data-test-secret" not in str(captured.value)
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    ("provider_code", "error_type"),
    [
        (401, ProviderAuthenticationError),
        (403, ProviderEntitlementError),
        (429, ProviderRateLimitError),
        (400, ProviderHttpError),
    ],
)
def test_twelve_data_http_200_body_errors_are_typed_without_exposing_messages(
    provider_code: int,
    error_type: type[ProviderHttpError],
) -> None:
    body = (
        '{"status":"error","code":'
        f'{provider_code},"message":"synthetic user input and secret must not escape"}}'
    ).encode()
    transport = QueueHttpTransport([HttpResponse(200, body)])
    provider = _provider(transport)

    with pytest.raises(error_type) as captured:
        tuple(provider.get_bars(_bar_request()))

    assert captured.value.status_code == provider_code
    assert "synthetic user input" not in str(captured.value)
    assert len(transport.requests) == 1


def test_twelve_data_preserves_per_symbol_batch_errors_for_normalization() -> None:
    payload = b'{"XPH1":{"status":"error","code":404,"message":"not found"}}'
    provider = _provider(
        QueueHttpTransport([HttpResponse(200, payload)]),
        batch_ids=_BATCH_IDS[:1],
    )

    (batch,) = tuple(provider.get_bars(_bar_request()))

    with batch.payload.open_binary() as reader:
        assert reader.read() == payload


def test_twelve_data_yields_top_level_body_error_before_safe_classification() -> None:
    payload = b'{"status":"error","code":429,"message":"synthetic quota detail"}'
    provider = _provider(QueueHttpTransport([HttpResponse(200, payload)]))
    pages = iter(provider.get_bars(_bar_request()))

    batch = next(pages)

    with batch.payload.open_binary() as reader:
        assert reader.read() == payload
    with pytest.raises(ProviderRateLimitError):
        next(pages)


@pytest.mark.parametrize(
    "payload",
    [
        b"not-json",
        b'{"status":"unexpected"}',
        b'{"status":"error","code":"429"}',
    ],
)
def test_twelve_data_rejects_malformed_top_level_responses(payload: bytes) -> None:
    provider = _provider(QueueHttpTransport([HttpResponse(200, payload)]))

    with pytest.raises(ProviderResponseError):
        tuple(provider.get_bars(_bar_request()))


def test_twelve_data_bounded_symbol_search_and_basic_capability_failures() -> None:
    transport = QueueHttpTransport([HttpResponse(200, _fixture("symbol_search.json"))])
    provider = _provider(transport, batch_ids=_BATCH_IDS[:1])

    (batch,) = tuple(provider.get_instrument(" XPH1 "))

    assert transport.requests[0].path == "/symbol_search"
    assert dict(transport.requests[0].query) == {"symbol": "XPH1", "outputsize": "30"}
    assert batch.metadata.request_metadata["lookup_scope"] == "single_instrument_search"
    with pytest.raises(ProviderCapabilityError, match="unbounded instrument inventory"):
        tuple(provider.get_instruments())
    with pytest.raises(ProviderCapabilityError, match="does not entitle"):
        tuple(
            provider.get_corporate_actions(
                CorporateActionRequest(
                    instruments=_references(),
                    start=date(2025, 1, 1),
                    end=date(2026, 1, 1),
                )
            )
        )
    with pytest.raises(ValueError, match="must not be blank"):
        tuple(provider.get_instrument("  "))

    assert len(transport.requests) == 1


@pytest.mark.parametrize("identifier", ["XPH1,XPH2", "XPH1\x1fXPH2"])
def test_twelve_data_rejects_identifier_expansion_before_network(identifier: str) -> None:
    transport = QueueHttpTransport([])
    provider = _provider(transport)
    reference = ProviderInstrumentRef(
        instrument_id=_references()[0].instrument_id,
        provider_identifier=identifier,
    )

    with pytest.raises(ProviderCapabilityError, match="commas or control"):
        tuple(provider.get_bars(_bar_request(instruments=(reference,))))

    assert not transport.requests
