"""Opt-in, non-persistent Alpaca historical-SIP entitlement preflight."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.providers.alpaca import (
    AlpacaFeed,
    AlpacaProvider,
    AlpacaSipPreflightOutcome,
    AlpacaSipPreflightResult,
)
from investment_platform.data.providers.base import BarRequest, ProviderInstrumentRef
from investment_platform.data.providers.errors import ProviderError

_AAPL_INSTRUMENT_ID = UUID("1923431d-8907-4f63-ba11-68182c11f778")


def fixed_historical_sip_request() -> BarRequest:
    """Return the frozen one-symbol, one-bar preflight request from the Phase 1 design."""

    return BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_AAPL_INSTRUMENT_ID,
                provider_identifier="AAPL",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        end=datetime(2025, 7, 2, 13, 35, tzinfo=UTC),
        session=TradingSession.REGULAR,
        adjustment_state=AdjustmentState.UNADJUSTED,
    )


def sanitized_preflight_record(result: AlpacaSipPreflightResult) -> dict[str, Any]:
    """Render the entitlement result without provider payloads, URLs, or credentials."""

    served_feed = (
        result.requested_feed.value
        if result.outcome is AlpacaSipPreflightOutcome.AUTHORIZED
        else None
    )
    return {
        "provider": "alpaca",
        "capability": "historical_us_equities_5m",
        "outcome": result.outcome.value,
        "http_status": result.status_code,
        "requested_feed": result.requested_feed.value,
        "served_explicit_feed": served_feed,
        "instrument": "AAPL",
        "timeframe": result.timeframe.value,
        "start": result.start.isoformat(),
        "end_exclusive": result.end_exclusive.isoformat(),
        "instrument_count": result.instrument_count,
        "observation_count": result.observation_count,
        "checked_at": result.checked_at.isoformat(),
        "rate_limit_capacity": result.rate_limit_capacity,
        "rate_limit_remaining": result.rate_limit_remaining,
        "rate_limit_reset": result.rate_limit_reset,
        "raw_payload_persisted": False,
        "raw_retention_authorized": result.raw_retention_authorized,
    }


def main() -> int:
    """Run exactly one explicit-SIP call; this command never creates a raw artifact."""

    try:
        provider = AlpacaProvider.from_environment(feed=AlpacaFeed.SIP)
        result = provider.preflight_sip_entitlement(fixed_historical_sip_request())
    except ProviderError as error:
        print(f"Alpaca SIP preflight not completed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(sanitized_preflight_record(result), sort_keys=True))
    return 0 if result.outcome is AlpacaSipPreflightOutcome.AUTHORIZED else 3


if __name__ == "__main__":  # pragma: no cover - exercised only by the opt-in live command
    raise SystemExit(main())


__all__ = ["fixed_historical_sip_request", "main", "sanitized_preflight_record"]
