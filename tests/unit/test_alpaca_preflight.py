"""Tests for the non-persistent Alpaca SIP preflight rendering."""

from datetime import UTC, datetime

import pytest

from investment_platform.data.models import Timeframe
from investment_platform.data.providers import AlpacaFeed, AlpacaSipPreflightOutcome
from investment_platform.data.providers.alpaca import AlpacaSipPreflightResult
from investment_platform.data.providers.alpaca_preflight import (
    fixed_historical_sip_request,
    sanitized_preflight_record,
)

pytestmark = pytest.mark.unit


def test_fixed_preflight_is_one_old_explicitly_bounded_bar() -> None:
    request = fixed_historical_sip_request()

    assert len(request.instruments) == 1
    assert request.instruments[0].provider_identifier == "AAPL"
    assert request.timeframe is Timeframe.FIVE_MINUTES
    assert request.start == datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
    assert request.end == datetime(2025, 7, 2, 13, 35, tzinfo=UTC)


def test_preflight_record_is_sanitized_and_marks_retention_unknown() -> None:
    result = AlpacaSipPreflightResult(
        outcome=AlpacaSipPreflightOutcome.AUTHORIZED,
        status_code=200,
        requested_feed=AlpacaFeed.SIP,
        timeframe=Timeframe.FIVE_MINUTES,
        start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        end_exclusive=datetime(2025, 7, 2, 13, 35, tzinfo=UTC),
        instrument_count=1,
        observation_count=1,
        checked_at=datetime(2026, 8, 19, 12, tzinfo=UTC),
        rate_limit_capacity=200,
        rate_limit_remaining=199,
        rate_limit_reset=1787140800,
        raw_retention_authorized=None,
    )

    record = sanitized_preflight_record(result)

    assert record["served_explicit_feed"] == "sip"
    assert record["raw_payload_persisted"] is False
    assert record["raw_retention_authorized"] is None
    rendered = repr(record).casefold()
    assert "secret" not in rendered
    assert "authorization" not in rendered
    assert "payload" in rendered  # The only payload field is the false persistence declaration.


def test_denied_preflight_never_claims_that_sip_was_served() -> None:
    result = AlpacaSipPreflightResult(
        outcome=AlpacaSipPreflightOutcome.AUTH_OR_PERMISSION_DENIED,
        status_code=403,
        requested_feed=AlpacaFeed.SIP,
        timeframe=Timeframe.FIVE_MINUTES,
        start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        end_exclusive=datetime(2025, 7, 2, 13, 35, tzinfo=UTC),
        instrument_count=1,
        observation_count=None,
        checked_at=datetime(2026, 8, 20, 12, tzinfo=UTC),
        rate_limit_capacity=None,
        rate_limit_remaining=None,
        rate_limit_reset=None,
        raw_retention_authorized=None,
    )

    record = sanitized_preflight_record(result)

    assert record["requested_feed"] == "sip"
    assert record["served_explicit_feed"] is None
    assert record["raw_payload_persisted"] is False
