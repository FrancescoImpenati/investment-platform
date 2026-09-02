"""Offline request, feed-entitlement, and failure tests for the Alpaca adapter."""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.providers import (
    AlpacaCredentials,
    AlpacaEvidenceAdjustment,
    AlpacaFeed,
    AlpacaProvider,
    AlpacaSipPreflightOutcome,
    BarRequest,
    CorporateActionRequest,
    MarketDataProvider,
    ProviderAccessDeniedError,
    ProviderAuthenticationError,
    ProviderCapabilityError,
    ProviderConfigurationError,
    ProviderEntitlementError,
    ProviderHttpError,
    ProviderInstrumentRef,
    ProviderRateLimitError,
    ProviderResponseError,
)
from investment_platform.data.providers.http import HttpResponse
from tests.provider_fakes import QueueHttpTransport

pytestmark = pytest.mark.unit

_FIXTURES = Path(__file__).parents[1] / "fixtures" / "providers" / "alpaca"
_INSTRUMENT_ID = UUID("10000000-0000-4000-8000-000000000001")
_BATCH_IDS = (
    UUID("30000000-0000-4000-8000-000000000001"),
    UUID("30000000-0000-4000-8000-000000000002"),
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
            ProviderInstrumentRef(instrument_id=_INSTRUMENT_ID, provider_identifier="XPH1"),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        end=datetime(2025, 7, 2, 20, 0, tzinfo=UTC),
        session=session,
        adjustment_state=adjustment,
    )


def _preflight_request() -> BarRequest:
    return BarRequest(
        instruments=(
            ProviderInstrumentRef(instrument_id=_INSTRUMENT_ID, provider_identifier="XPH1"),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        end=datetime(2025, 7, 2, 13, 35, tzinfo=UTC),
        session=TradingSession.REGULAR,
        adjustment_state=AdjustmentState.UNADJUSTED,
    )


def _provider(
    transport: QueueHttpTransport,
    *,
    feed: AlpacaFeed = AlpacaFeed.SIP,
) -> AlpacaProvider:
    identifiers = iter(_BATCH_IDS)
    return AlpacaProvider(
        AlpacaCredentials(key_id="alpaca-test-id", secret_key="alpaca-test-secret"),
        feed=feed,
        transport=transport,
        clock=lambda: _NOW,
        batch_id_factory=identifiers.__next__,
    )


def test_alpaca_credentials_are_redacted_and_required() -> None:
    credentials = AlpacaCredentials(key_id="alpaca-test-id", secret_key="alpaca-test-secret")

    assert "alpaca-test-id" not in repr(credentials)
    assert "alpaca-test-secret" not in repr(credentials)
    with pytest.raises(ProviderConfigurationError, match="both required"):
        AlpacaCredentials(key_id="", secret_key="")
    with pytest.raises(ValueError, match="official data origin"):
        AlpacaProvider(credentials, data_base_url="https://attacker.invalid")
    with pytest.raises(ValueError, match="official paper API origin"):
        AlpacaProvider(credentials, trading_base_url="https://attacker.invalid")
    assert AlpacaCredentials.from_environment(
        {
            "APCA_API_KEY_ID": "environment-test-id",
            "APCA_API_SECRET_KEY": "environment-test-secret",
        }
    ) == AlpacaCredentials(
        key_id="environment-test-id",
        secret_key="environment-test-secret",
    )
    with pytest.raises(ProviderConfigurationError, match="both required"):
        AlpacaCredentials.from_environment({})


def test_alpaca_sip_bars_are_paginated_without_feed_fallback() -> None:
    first = _fixture("bars_5m_sip_page_1.json")
    second = _fixture("bars_5m_sip_page_2.json")
    transport = QueueHttpTransport(
        [
            HttpResponse(
                200,
                first,
                {"X-RateLimit-Limit": "200", "X-RateLimit-Remaining": "199"},
            ),
            HttpResponse(200, second),
        ]
    )
    provider = _provider(transport)

    batches = tuple(provider.get_bars(_bar_request()))

    assert isinstance(provider, MarketDataProvider)
    assert len(batches) == 2
    first_query = dict(transport.requests[0].query)
    second_query = dict(transport.requests[1].query)
    assert first_query["feed"] == "sip"
    assert first_query["asof"] == "-"
    assert first_query["currency"] == "USD"
    assert "page_token" not in first_query
    assert second_query["page_token"] == "SYNTHETIC_PAGE_2"
    assert second_query["feed"] == "sip"
    assert batches[0].metadata.source.dataset == "price_bars_sip"
    assert batches[0].metadata.request_metadata["rate_limit_capacity"] == 200
    assert batches[0].metadata.request_metadata["rate_limit_remaining"] == 199
    assert "SYNTHETIC_PAGE_2" not in str(batches[1].metadata.model_dump(mode="json"))
    assert transport.requests[0].headers == {
        "APCA-API-KEY-ID": "alpaca-test-id",
        "APCA-API-SECRET-KEY": "alpaca-test-secret",
    }


@pytest.mark.parametrize(
    ("start", "end", "wire_start", "wire_end"),
    (
        (
            datetime(2025, 1, 2, 14, 30, tzinfo=UTC),
            datetime(2025, 1, 2, 21, 0, tzinfo=UTC),
            "2025-01-02T05:00:00.000000Z",
            "2025-01-03T04:59:59.999999Z",
        ),
        (
            datetime(2025, 3, 7, 14, 30, tzinfo=UTC),
            datetime(2025, 3, 10, 20, 0, tzinfo=UTC),
            "2025-03-07T05:00:00.000000Z",
            "2025-03-11T03:59:59.999999Z",
        ),
    ),
)
def test_alpaca_daily_wire_bounds_cover_first_and_last_session_midnights_across_dst(
    start: datetime,
    end: datetime,
    wire_start: str,
    wire_end: str,
) -> None:
    transport = QueueHttpTransport([HttpResponse(200, _fixture("bars_daily_sip.json"))])
    provider = _provider(transport)
    request = BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="XPH1",
            ),
        ),
        timeframe=Timeframe.ONE_DAY,
        start=start,
        end=end,
        session=TradingSession.REGULAR,
        adjustment_state=AdjustmentState.UNADJUSTED,
    )

    (batch,) = tuple(provider.get_bars(request))

    query = dict(transport.requests[0].query)
    assert query["start"] == wire_start
    assert query["end"] == wire_end
    assert batch.metadata.request_metadata["start"] == start.isoformat()
    assert batch.metadata.request_metadata["end_exclusive"] == end.isoformat()


def test_alpaca_iex_is_a_separately_labeled_source() -> None:
    transport = QueueHttpTransport([HttpResponse(200, _fixture("bars_5m_sip_page_2.json"))])
    provider = _provider(transport, feed=AlpacaFeed.IEX)

    (batch,) = tuple(provider.get_bars(_bar_request()))

    assert dict(transport.requests[0].query)["feed"] == "iex"
    assert batch.metadata.source.dataset == "price_bars_iex"


@pytest.mark.parametrize(
    ("adjustment", "wire_value"),
    [
        (AdjustmentState.SPLIT_ADJUSTED, "split"),
    ],
)
def test_alpaca_maps_only_explicit_canonical_adjustment_states(
    adjustment: AdjustmentState,
    wire_value: str,
) -> None:
    transport = QueueHttpTransport([HttpResponse(200, _fixture("bars_5m_sip_page_2.json"))])
    provider = _provider(transport)

    tuple(provider.get_bars(_bar_request(adjustment=adjustment)))

    assert dict(transport.requests[0].query)["adjustment"] == wire_value


def test_alpaca_rejects_a_canonical_adjustment_without_an_exact_wire_semantic() -> None:
    transport = QueueHttpTransport([])
    provider = _provider(transport)

    with pytest.raises(ProviderCapabilityError, match="cannot map canonical adjustment"):
        tuple(
            provider.get_bars(_bar_request(adjustment=AdjustmentState.SPLIT_AND_DIVIDEND_ADJUSTED))
        )

    assert not transport.requests


def test_alpaca_point_in_time_symbol_mapping_is_an_explicit_evidence_call() -> None:
    transport = QueueHttpTransport([HttpResponse(200, _fixture("bars_5m_sip_page_2.json"))])
    provider = _provider(transport)

    (batch,) = tuple(provider.get_bars_as_of(_bar_request(), as_of=date(2025, 1, 21)))

    assert dict(transport.requests[0].query)["asof"] == "2025-01-21"
    assert batch.metadata.request_metadata["symbol_mapping_as_of"] == "2025-01-21"


@pytest.mark.parametrize(
    "evidence_adjustment",
    [AlpacaEvidenceAdjustment.DIVIDEND, AlpacaEvidenceAdjustment.ALL],
)
def test_alpaca_provider_only_adjustment_probes_are_not_canonical_series(
    evidence_adjustment: AlpacaEvidenceAdjustment,
) -> None:
    transport = QueueHttpTransport([HttpResponse(200, _fixture("bars_5m_sip_page_2.json"))])
    provider = _provider(transport)

    (batch,) = tuple(
        provider.get_adjustment_evidence(
            _bar_request(),
            adjustment=evidence_adjustment,
        )
    )

    assert dict(transport.requests[0].query)["adjustment"] == evidence_adjustment.value
    assert batch.metadata.request_metadata["provider_adjustment"] == evidence_adjustment.value
    assert batch.metadata.request_metadata["canonical_persistence_eligible"] is False
    assert batch.metadata.request_metadata["adjustment_state"] == "provider_evidence_only"


def test_alpaca_sip_422_entitlement_error_never_retries_as_iex() -> None:
    transport = QueueHttpTransport([HttpResponse(422, _fixture("error_sip_entitlement.json"))])
    provider = _provider(transport)

    with pytest.raises(ProviderEntitlementError) as captured:
        tuple(provider.get_bars(_bar_request()))

    assert captured.value.status_code == 422
    assert len(transport.requests) == 1
    assert dict(transport.requests[0].query)["feed"] == "sip"


@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [
        (401, ProviderAuthenticationError),
        (403, ProviderAccessDeniedError),
        (429, ProviderRateLimitError),
        (500, ProviderHttpError),
    ],
)
def test_alpaca_http_failures_are_typed_without_retry_or_iex_fallback(
    status_code: int,
    error_type: type[ProviderHttpError],
) -> None:
    transport = QueueHttpTransport([HttpResponse(status_code, b'{"message":"synthetic"}')])
    provider = _provider(transport)

    with pytest.raises(error_type) as captured:
        tuple(provider.get_bars(_bar_request()))

    assert captured.value.status_code == status_code
    assert len(transport.requests) == 1
    assert dict(transport.requests[0].query)["feed"] == "sip"
    assert "alpaca-test-secret" not in str(captured.value)


def test_alpaca_rejects_repeated_page_tokens_and_malformed_success_json() -> None:
    repeated = b'{"bars":{},"next_page_token":"REPEATED"}'
    provider = _provider(
        QueueHttpTransport([HttpResponse(200, repeated), HttpResponse(200, repeated)])
    )
    with pytest.raises(ProviderResponseError, match="repeated next_page_token"):
        tuple(provider.get_bars(_bar_request()))

    malformed_provider = _provider(QueueHttpTransport([HttpResponse(200, b"not-json")]))
    with pytest.raises(ProviderResponseError, match="valid JSON"):
        tuple(malformed_provider.get_bars(_bar_request()))


def test_alpaca_preflight_is_transient_and_classifies_entitlement() -> None:
    transport = QueueHttpTransport([HttpResponse(422, _fixture("error_sip_entitlement.json"))])
    provider = _provider(transport)

    result = provider.preflight_sip_entitlement(_preflight_request())

    assert result.outcome is AlpacaSipPreflightOutcome.ENTITLEMENT_DENIED
    assert result.requested_feed is AlpacaFeed.SIP
    assert result.observation_count is None
    assert len(transport.requests) == 1
    assert dict(transport.requests[0].query)["limit"] == "1"
    assert dict(transport.requests[0].query)["feed"] == "sip"


def test_alpaca_preflight_authorized_result_contains_only_counts() -> None:
    transport = QueueHttpTransport(
        [
            HttpResponse(
                200,
                _fixture("bars_5m_sip_page_1.json"),
                {
                    "X-RateLimit-Limit": "200",
                    "X-RateLimit-Remaining": "198",
                    "X-RateLimit-Reset": "1787140800",
                },
            )
        ]
    )
    provider = _provider(transport)

    result = provider.preflight_sip_entitlement(_preflight_request())

    assert result.outcome is AlpacaSipPreflightOutcome.AUTHORIZED
    assert result.observation_count == 1
    assert result.checked_at == _NOW
    assert result.rate_limit_capacity == 200
    assert result.rate_limit_remaining == 198
    assert result.rate_limit_reset == 1787140800
    assert result.raw_retention_authorized is None
    assert not hasattr(result, "body")


def test_alpaca_preflight_treats_403_as_ambiguous_auth_or_permission_denial() -> None:
    provider = _provider(QueueHttpTransport([HttpResponse(403, b'{"message":"synthetic"}')]))

    result = provider.preflight_sip_entitlement(_preflight_request())

    assert result.outcome is AlpacaSipPreflightOutcome.AUTH_OR_PERMISSION_DENIED
    assert result.observation_count is None


@pytest.mark.parametrize(
    ("status_code", "outcome"),
    [
        (401, AlpacaSipPreflightOutcome.AUTHENTICATION_FAILED),
        (429, AlpacaSipPreflightOutcome.RATE_LIMITED),
        (500, AlpacaSipPreflightOutcome.HTTP_ERROR),
    ],
)
def test_alpaca_preflight_classifies_non_entitlement_http_outcomes(
    status_code: int,
    outcome: AlpacaSipPreflightOutcome,
) -> None:
    provider = _provider(
        QueueHttpTransport([HttpResponse(status_code, b'{"message":"synthetic"}')])
    )

    result = provider.preflight_sip_entitlement(_preflight_request())

    assert result.outcome is outcome
    assert result.observation_count is None


def test_alpaca_preflight_rejects_malformed_symbol_entries() -> None:
    provider = _provider(QueueHttpTransport([HttpResponse(200, b'{"bars":{"XPH1":{}}}')]))

    result = provider.preflight_sip_entitlement(_preflight_request())

    assert result.outcome is AlpacaSipPreflightOutcome.MALFORMED_RESPONSE
    assert result.observation_count is None


def test_alpaca_preflight_enforces_one_old_regular_unadjusted_bar() -> None:
    transport = QueueHttpTransport([])
    provider = _provider(transport)
    base = _preflight_request()
    invalid_requests = (
        base.model_copy(update={"end": base.start + timedelta(minutes=10)}),
        base.model_copy(update={"end": _NOW}),
        base.model_copy(update={"session": TradingSession.UNKNOWN}),
        base.model_copy(update={"adjustment_state": AdjustmentState.SPLIT_ADJUSTED}),
    )

    for request in invalid_requests:
        with pytest.raises(ValueError, match="SIP preflight"):
            provider.preflight_sip_entitlement(request)

    assert not transport.requests


def test_alpaca_assets_reject_point_in_time_and_preserve_array_payload() -> None:
    provider = _provider(QueueHttpTransport([]))
    with pytest.raises(ProviderCapabilityError, match="no point-in-time"):
        tuple(provider.get_instruments(as_of=date(2025, 1, 1)))

    transport = QueueHttpTransport([HttpResponse(200, _fixture("assets_us_equity.json"))])
    provider = _provider(transport)
    (batch,) = tuple(provider.get_instruments())
    with batch.payload.open_binary() as reader:
        assert reader.read() == _fixture("assets_us_equity.json")


def test_alpaca_single_asset_lookup_avoids_full_inventory() -> None:
    payload = b'{"id":"00000000-0000-4000-8000-000000000001","symbol":"BRK.B"}'
    transport = QueueHttpTransport([HttpResponse(200, payload)])
    provider = _provider(transport)

    (batch,) = tuple(provider.get_instrument("BRK.B"))

    assert transport.requests[0].path == "/v2/assets/BRK.B"
    assert transport.requests[0].query == ()
    assert batch.metadata.request_metadata["lookup_scope"] == "single_instrument"
    assert batch.metadata.request_metadata["as_of"] is None

    with pytest.raises(ValueError, match="must not be blank"):
        tuple(provider.get_instrument("  "))


def test_alpaca_corporate_action_query_records_process_date_limit() -> None:
    transport = QueueHttpTransport([HttpResponse(200, _fixture("corporate_actions_mixed.json"))])
    provider = _provider(transport)
    request = CorporateActionRequest(
        instruments=(
            ProviderInstrumentRef(instrument_id=_INSTRUMENT_ID, provider_identifier="XPH1"),
        ),
        start=date(2025, 1, 1),
        end=date(2026, 1, 1),
    )

    (batch,) = tuple(provider.get_corporate_actions(request))

    query = dict(transport.requests[0].query)
    assert query["end"] == "2025-12-31"
    assert "types" not in query
    assert batch.metadata.request_metadata["date_basis"] == "process_date"
    assert batch.metadata.request_metadata["effective_date_filter_complete"] is False


def test_alpaca_rejects_unsupported_sessions() -> None:
    provider = _provider(QueueHttpTransport([]))

    with pytest.raises(ProviderCapabilityError, match="overnight"):
        tuple(provider.get_bars(_bar_request(session=TradingSession.OVERNIGHT)))


@pytest.mark.parametrize("identifier", ["XPH1,XPH2", "XPH1\x1fXPH2"])
def test_alpaca_rejects_identifier_delimiters_before_any_bar_request(
    identifier: str,
) -> None:
    transport = QueueHttpTransport([])
    provider = _provider(transport)
    request = _bar_request().model_copy(
        update={
            "instruments": (
                ProviderInstrumentRef(
                    instrument_id=_INSTRUMENT_ID,
                    provider_identifier=identifier,
                ),
            )
        }
    )

    with pytest.raises(ProviderCapabilityError, match="commas or control"):
        tuple(provider.get_bars(request))

    assert not transport.requests


def test_alpaca_rejects_identifier_expansion_for_actions_and_preflight() -> None:
    transport = QueueHttpTransport([])
    provider = _provider(transport)
    reference = ProviderInstrumentRef(
        instrument_id=_INSTRUMENT_ID,
        provider_identifier="XPH1,XPH2",
    )
    action_request = CorporateActionRequest(
        instruments=(reference,),
        start=date(2025, 1, 1),
        end=date(2026, 1, 1),
    )
    bar_request = _preflight_request().model_copy(update={"instruments": (reference,)})

    with pytest.raises(ProviderCapabilityError, match="commas or control"):
        tuple(provider.get_corporate_actions(action_request))
    with pytest.raises(ProviderCapabilityError, match="commas or control"):
        provider.preflight_sip_entitlement(bar_request)

    assert not transport.requests
