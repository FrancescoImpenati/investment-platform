"""Tests for stable instrument identity and point-in-time membership."""

from datetime import UTC, date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from investment_platform.instruments import (
    AssetClass,
    Instrument,
    InstrumentIdentifier,
    Universe,
    UniverseMembership,
)

pytestmark = pytest.mark.unit


def test_ticker_is_temporal_and_never_the_instrument_identity() -> None:
    first_id = uuid4()
    second_id = uuid4()
    reused_ticker = InstrumentIdentifier(
        namespace="ticker",
        value="ABC",
        valid_from=date(2024, 1, 1),
    )

    first = Instrument(
        instrument_id=first_id,
        asset_class=AssetClass.EQUITY,
        name="First issuer",
        primary_currency="usd",
        mic="xnas",
        identifiers=(reused_ticker,),
    )
    second = Instrument(
        instrument_id=second_id,
        asset_class=AssetClass.EQUITY,
        name="Second issuer",
        identifiers=(reused_ticker,),
    )

    assert first.instrument_id != second.instrument_id
    assert first.identifiers[0].value == second.identifiers[0].value
    assert first.primary_currency == "USD"
    assert first.mic == "XNAS"


def test_ticker_change_preserves_instrument_id() -> None:
    instrument_id = uuid4()
    instrument = Instrument(
        instrument_id=instrument_id,
        asset_class=AssetClass.EQUITY,
        name="Renamed issuer",
        identifiers=(
            InstrumentIdentifier(
                namespace="ticker",
                value="OLD",
                valid_from=date(2020, 1, 1),
                valid_to=date(2024, 6, 1),
            ),
            InstrumentIdentifier(
                namespace="ticker",
                value="NEW",
                valid_from=date(2024, 6, 1),
            ),
        ),
    )

    assert instrument.instrument_id == instrument_id
    assert instrument.identifiers[0].is_valid_on(date(2024, 5, 31))
    assert not instrument.identifiers[0].is_valid_on(date(2024, 6, 1))
    assert instrument.identifiers[1].is_valid_on(date(2024, 6, 1))


def test_identifier_rejects_empty_or_reversed_validity_interval() -> None:
    with pytest.raises(ValidationError):
        InstrumentIdentifier(
            namespace="ticker",
            value="ABC",
            valid_from=date(2024, 1, 2),
            valid_to=date(2024, 1, 2),
        )


def test_universe_membership_is_half_open_and_normalizes_timestamps() -> None:
    offset = timezone(timedelta(hours=2))
    membership = UniverseMembership(
        membership_id=uuid4(),
        universe_id=uuid4(),
        instrument_id=uuid4(),
        valid_from=date(2024, 1, 1),
        valid_to=date(2024, 2, 1),
        available_at=None,
        ingested_at=datetime(2024, 1, 2, 10, tzinfo=offset),
    )

    assert membership.is_active_on(date(2024, 1, 1))
    assert membership.is_active_on(date(2024, 1, 31))
    assert not membership.is_active_on(date(2024, 2, 1))
    assert membership.available_at is None
    assert membership.ingested_at == datetime(2024, 1, 2, 8, tzinfo=UTC)


def test_universe_contracts_are_frozen_and_forbid_extra_fields() -> None:
    universe = Universe(universe_id=uuid4(), name="S&P 500", source="index committee")
    name_field = "name"

    with pytest.raises(ValidationError):
        setattr(universe, name_field, "changed")

    with pytest.raises(ValidationError):
        Universe.model_validate(
            {
                "universe_id": str(uuid4()),
                "name": "S&P 500",
                "unexpected": True,
            }
        )


def test_membership_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        UniverseMembership(
            membership_id=uuid4(),
            universe_id=uuid4(),
            instrument_id=uuid4(),
            valid_from=date(2024, 1, 1),
            ingested_at=datetime(2024, 1, 1, 12),
        )
