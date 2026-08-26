"""Offline request, pagination, and failure tests for the Massive adapter."""

from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pytest

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.providers import (
    BarRequest,
    CorporateActionRequest,
    MarketDataProvider,
    MassiveCredentials,
    MassiveProvider,
    ProviderAccessDeniedError,
    ProviderAuthenticationError,
    ProviderCapabilityError,
    ProviderConfigurationError,
    ProviderHttpError,
    ProviderInstrumentRef,
    ProviderRateLimitError,
    ProviderResponseError,
)
from investment_platform.data.providers.http import HttpResponse
from tests.provider_fakes import QueueHttpTransport

pytestmark = pytest.mark.unit

_FIXTURES = Path(__file__).parents[1] / "fixtures" / "providers" / "massive"
_INSTRUMENT_ID = UUID("10000000-0000-4000-8000-000000000001")
_BATCH_IDS = (
    UUID("20000000-0000-4000-8000-000000000001"),
    UUID("20000000-0000-4000-8000-000000000002"),
    UUID("20000000-0000-4000-8000-000000000003"),
)
_NOW = datetime(2026, 8, 19, 12, tzinfo=UTC)


def _fixture(name: str) -> bytes:
    return (_FIXTURES / name).read_bytes()


def _bar_request(
    *,
    adjustment: AdjustmentState = AdjustmentState.UNADJUSTED,
    session: TradingSession = TradingSession.REGULAR,
) -> BarRequest:
    return BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="XPH1",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        end=datetime(2025, 7, 2, 20, 0, tzinfo=UTC),
        session=session,
        adjustment_state=adjustment,
    )


def _provider(
    transport: QueueHttpTransport,
    *,
    batch_ids: tuple[UUID, ...] = _BATCH_IDS,
) -> MassiveProvider:
    identifiers = iter(batch_ids)
    return MassiveProvider(
        MassiveCredentials(api_key="massive-test-secret"),
        transport=transport,
        clock=lambda: _NOW,
        batch_id_factory=identifiers.__next__,
    )


def test_massive_credentials_are_redacted_and_required() -> None:
    credentials = MassiveCredentials(api_key="massive-test-secret")

    assert "massive-test-secret" not in repr(credentials)
    with pytest.raises(ProviderConfigurationError, match="API key is missing"):
        MassiveCredentials(api_key="  ")
    with pytest.raises(ValueError, match="official API origin"):
        MassiveProvider(credentials, base_url="https://attacker.invalid")
    assert MassiveCredentials.from_environment(
        {"MASSIVE_API_KEY": "environment-test-secret"}
    ) == MassiveCredentials(api_key="environment-test-secret")
    with pytest.raises(ProviderConfigurationError, match="API key is missing"):
        MassiveCredentials.from_environment({})


def test_massive_bars_emit_exact_paginated_raw_pages_with_bearer_auth() -> None:
    first = _fixture("bars_5m_page_1.json")
    second = _fixture("bars_5m_page_2.json")
    transport = QueueHttpTransport(
        [
            HttpResponse(200, first, {"X-Request-ID": "SYNTHETIC-REQUEST-1"}, 12.5),
            HttpResponse(200, second, {}, 10.0),
        ]
    )
    provider = _provider(transport)

    batches = tuple(provider.get_bars(_bar_request()))

    assert isinstance(provider, MarketDataProvider)
    assert len(batches) == 2
    with batches[0].payload.open_binary() as reader:
        assert reader.read() == first
    assert batches[0].metadata.provider_request_id == "SYNTHETIC-REQUEST-1"
    assert batches[0].metadata.request_metadata["page_number"] == 1
    assert batches[1].metadata.request_metadata["page_number"] == 2
    assert "cursor" not in batches[1].metadata.request_metadata
    assert len(transport.requests) == 2
    assert transport.requests[0].headers == {"Authorization": "Bearer massive-test-secret"}
    assert "apiKey" not in dict(transport.requests[0].query)
    assert dict(transport.requests[1].query)["cursor"] == "SYNTHETIC_PAGE_2"
    assert "massive-test-secret" not in str(batches[0].metadata.model_dump(mode="json"))


def test_massive_rejects_unrepresentable_adjustment_and_session() -> None:
    provider = _provider(QueueHttpTransport([]))

    with pytest.raises(ProviderCapabilityError, match="adjustment state"):
        tuple(
            provider.get_bars(_bar_request(adjustment=AdjustmentState.SPLIT_AND_DIVIDEND_ADJUSTED))
        )
    with pytest.raises(ProviderCapabilityError, match="pre_market"):
        tuple(provider.get_bars(_bar_request(session=TradingSession.PRE_MARKET)))


def test_massive_rate_limit_failure_is_sanitized_and_not_retried() -> None:
    transport = QueueHttpTransport(
        [HttpResponse(429, b'{"status":"ERROR"}', {"Retry-After": "12"}, 3.0)]
    )
    provider = _provider(transport)

    with pytest.raises(ProviderRateLimitError) as captured:
        tuple(provider.get_bars(_bar_request()))

    assert captured.value.status_code == 429
    assert captured.value.retry_after_seconds == 12
    assert "massive-test-secret" not in str(captured.value)
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [
        (401, ProviderAuthenticationError),
        (403, ProviderAccessDeniedError),
        (500, ProviderHttpError),
    ],
)
def test_massive_http_failures_are_typed_and_never_retried(
    status_code: int,
    error_type: type[ProviderHttpError],
) -> None:
    transport = QueueHttpTransport([HttpResponse(status_code, b'{"status":"ERROR"}')])
    provider = _provider(transport)

    with pytest.raises(error_type) as captured:
        tuple(provider.get_bars(_bar_request()))

    assert captured.value.status_code == status_code
    assert len(transport.requests) == 1
    assert "massive-test-secret" not in str(captured.value)


def test_massive_rejects_repeated_pagination_urls() -> None:
    payload = (
        b'{"status":"OK","results":[],"next_url":"https://api.massive.com/v2/aggs/'
        b'ticker/XPH1/range/5/minute/1751463000000/1751486399999?cursor=REPEATED"}'
    )
    provider = _provider(
        QueueHttpTransport([HttpResponse(200, payload), HttpResponse(200, payload)]),
        batch_ids=_BATCH_IDS[:2],
    )

    with pytest.raises(ProviderResponseError, match="repeated next_url"):
        tuple(provider.get_bars(_bar_request()))


def test_massive_rejects_same_origin_cross_endpoint_pagination() -> None:
    payload = (
        b'{"status":"OK","results":[],"next_url":'
        b'"https://api.massive.com/v3/reference/tickers?cursor=SYNTHETIC"}'
    )
    provider = _provider(QueueHttpTransport([HttpResponse(200, payload)]))

    with pytest.raises(ProviderResponseError, match="changed the expected"):
        tuple(provider.get_bars(_bar_request()))


@pytest.mark.parametrize(
    "query",
    [
        "cursor=SYNTHETIC&adjusted=true",
        "cursor=SYNTHETIC&apiKey=must-not-follow",
        "cursor=",
    ],
)
def test_massive_rejects_pagination_query_semantic_drift(query: str) -> None:
    payload = (
        b'{"status":"OK","results":[],"next_url":"https://api.massive.com/v2/aggs/'
        b"ticker/XPH1/range/5/minute/1751463000000/1751486399999?" + query.encode() + b'"}'
    )
    transport = QueueHttpTransport([HttpResponse(200, payload)])
    provider = _provider(transport)

    with pytest.raises(ProviderResponseError, match="next_url"):
        tuple(provider.get_bars(_bar_request()))

    assert len(transport.requests) == 1


def test_massive_rejects_cross_origin_pagination() -> None:
    payload = (
        b'{"status":"OK","results":[],"next_url":"https://malicious.invalid/page?cursor=SYNTHETIC"}'
    )
    provider = _provider(QueueHttpTransport([HttpResponse(200, payload)]))

    with pytest.raises(ProviderResponseError, match="configured Massive HTTPS origin"):
        tuple(provider.get_bars(_bar_request()))


def test_massive_corporate_action_requests_are_effective_date_bounded() -> None:
    transport = QueueHttpTransport(
        [
            HttpResponse(200, _fixture("splits_mixed.json")),
            HttpResponse(200, _fixture("dividends.json")),
        ]
    )
    provider = _provider(transport, batch_ids=_BATCH_IDS[:2])
    request = CorporateActionRequest(
        instruments=(
            ProviderInstrumentRef(instrument_id=_INSTRUMENT_ID, provider_identifier="XPH1"),
        ),
        start=date(2025, 1, 1),
        end=date(2026, 1, 1),
    )

    batches = tuple(provider.get_corporate_actions(request))

    assert len(batches) == 2
    split_query = dict(transport.requests[0].query)
    dividend_query = dict(transport.requests[1].query)
    assert split_query["execution_date.gte"] == "2025-01-01"
    assert split_query["execution_date.lt"] == "2026-01-01"
    assert dividend_query["ex_dividend_date.lt"] == "2026-01-01"
    assert batches[0].metadata.request_metadata["date_basis"] == "effective_date"


def test_massive_single_instrument_lookup_avoids_full_inventory() -> None:
    transport = QueueHttpTransport(
        [
            HttpResponse(
                200,
                b'{"status":"OK","request_id":"SYNTHETIC-BODY-REQUEST","results":{}}',
            )
        ]
    )
    provider = _provider(transport, batch_ids=_BATCH_IDS[:1])

    (batch,) = tuple(provider.get_instrument("BRK.B", as_of=date(2025, 7, 2)))

    assert transport.requests[0].path == "/v3/reference/tickers/BRK.B"
    assert dict(transport.requests[0].query) == {"date": "2025-07-02"}
    assert batch.metadata.request_metadata["lookup_scope"] == "single_instrument"
    assert batch.metadata.request_metadata["provider_identifier"] == "BRK.B"
    assert batch.metadata.provider_request_id == "SYNTHETIC-BODY-REQUEST"

    with pytest.raises(ValueError, match="must not be blank"):
        tuple(provider.get_instrument("  "))


def test_massive_ticker_events_are_an_explicit_experimental_superset() -> None:
    transport = QueueHttpTransport([HttpResponse(200, _fixture("ticker_events.json"))])
    provider = _provider(transport, batch_ids=_BATCH_IDS[:1])
    request = CorporateActionRequest(
        instruments=(
            ProviderInstrumentRef(instrument_id=_INSTRUMENT_ID, provider_identifier="XNEW"),
        ),
        start=date(2025, 1, 1),
        end=date(2026, 1, 1),
    )

    (batch,) = tuple(provider.get_ticker_events(request))

    assert transport.requests[0].path.endswith("/XNEW/events")
    assert batch.metadata.request_metadata["date_basis"] == "timeline_superset"
    assert batch.metadata.request_metadata["experimental_endpoint"] is True
