"""Tests for canonical market-data, provenance, and corporate-action contracts."""

from copy import deepcopy
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from typing import cast
from uuid import uuid4

import pytest
from pydantic import TypeAdapter, ValidationError

from investment_platform.data.models import (
    AdjustmentState,
    BarQualityFlag,
    CorporateAction,
    DividendAction,
    PriceBar,
    SplitAction,
    TickerChangeAction,
    Timeframe,
    TradingSession,
)
from investment_platform.data.provenance import (
    DataSource,
    LicenseClassification,
    RawBatchMetadata,
)

pytestmark = pytest.mark.unit


def _source() -> DataSource:
    return DataSource(
        source_id=uuid4(),
        provider="example",
        dataset="price_bars",
        logical_endpoint="v2/aggs",
    )


def _bar(**overrides: object) -> PriceBar:
    values: dict[str, object] = {
        "instrument_id": uuid4(),
        "timeframe": Timeframe.FIVE_MINUTES,
        "timestamp_start": datetime(2026, 8, 14, 13, 30, tzinfo=UTC),
        "timestamp_end": datetime(2026, 8, 14, 13, 35, tzinfo=UTC),
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "source_id": uuid4(),
        "raw_batch_id": uuid4(),
        "retrieved_at": datetime(2026, 8, 14, 13, 36, tzinfo=UTC),
        "ingested_at": datetime(2026, 8, 14, 13, 37, tzinfo=UTC),
        "session": TradingSession.REGULAR,
        "adjustment_state": AdjustmentState.UNADJUSTED,
    }
    values.update(overrides)
    return PriceBar.model_validate(values)


def _corporate_action_common() -> dict[str, object]:
    return {
        "instrument_id": uuid4(),
        "effective_date": date(2026, 8, 14),
        "source_id": uuid4(),
        "raw_batch_id": uuid4(),
        "retrieved_at": datetime(2026, 8, 14, 20, tzinfo=UTC),
        "ingested_at": datetime(2026, 8, 14, 20, 1, tzinfo=UTC),
    }


def test_data_source_defaults_to_private_and_serializes_safely() -> None:
    source = _source()
    dumped = source.model_dump(mode="json")

    assert source.license_classification is LicenseClassification.PRIVATE
    assert dumped["source_id"] == str(source.source_id)
    assert dumped["license_classification"] == "private"


@pytest.mark.parametrize(
    "logical_endpoint",
    ["v2/aggs?apiKey=secret", "v2/aggs#fragment", "https://provider.test/v2/aggs"],
)
def test_data_source_rejects_complete_or_query_bearing_endpoints(logical_endpoint: str) -> None:
    with pytest.raises(ValidationError):
        DataSource(
            source_id=uuid4(),
            provider="example",
            dataset="bars",
            logical_endpoint=logical_endpoint,
        )


def test_raw_batch_metadata_normalizes_time_and_rejects_sensitive_keys() -> None:
    source = _source()
    offset = timezone(timedelta(hours=2))
    metadata = RawBatchMetadata(
        batch_id=uuid4(),
        source=source,
        retrieved_at=datetime(2026, 8, 14, 16, tzinfo=offset),
        media_type="application/json",
        file_extension="JSON",
        provider_request_id="request-1",
        request_metadata={"cursor": "next", "page": 2},
    )

    assert metadata.retrieved_at == datetime(2026, 8, 14, 14, tzinfo=UTC)
    assert metadata.file_extension == "json"
    assert metadata.model_dump(mode="json")["request_metadata"] == {
        "cursor": "next",
        "page": 2,
    }
    with pytest.raises(TypeError):
        cast(dict[str, object], metadata.request_metadata)["cursor"] = "changed"
    assert deepcopy(metadata).model_dump(mode="json") == metadata.model_dump(mode="json")
    assert metadata.model_copy(deep=True).model_dump(mode="json") == metadata.model_dump(
        mode="json"
    )

    with pytest.raises(ValidationError, match="sensitive key"):
        RawBatchMetadata(
            batch_id=uuid4(),
            source=source,
            retrieved_at=datetime(2026, 8, 14, 14, tzinfo=UTC),
            media_type="application/json",
            file_extension="json",
            request_metadata={"api_key": "must-not-persist"},
        )


@pytest.mark.parametrize("sensitive_key", ["apiKey", "accessToken", "requestHeaders", "url"])
def test_raw_batch_metadata_rejects_compact_sensitive_key_names(sensitive_key: str) -> None:
    with pytest.raises(ValidationError, match="sensitive key"):
        RawBatchMetadata(
            batch_id=uuid4(),
            source=_source(),
            retrieved_at=datetime(2026, 8, 14, 14, tzinfo=UTC),
            media_type="application/json",
            file_extension="json",
            request_metadata={sensitive_key: "must-not-persist"},
        )


@pytest.mark.parametrize(
    "unsafe_value",
    [
        "https://provider.test/page?token=secret",
        "next=https://provider.test/page?token=secret",
        "Bearer secret",
        "prefix Bearer secret",
        "cursor=Basic secret",
        "token=secret",
    ],
)
def test_raw_batch_metadata_rejects_unsafe_values_under_a_benign_key(
    unsafe_value: str,
) -> None:
    with pytest.raises(ValidationError, match="complete URLs or secrets"):
        RawBatchMetadata(
            batch_id=uuid4(),
            source=_source(),
            retrieved_at=datetime(2026, 8, 14, 14, tzinfo=UTC),
            media_type="application/json",
            file_extension="json",
            request_metadata={"cursor": unsafe_value},
        )


@pytest.mark.parametrize(
    "provider_request_id",
    [
        "https://provider.test/request?token=secret",
        "request=https://provider.test/request?token=secret",
    ],
)
def test_raw_batch_metadata_rejects_a_url_as_provider_request_id(
    provider_request_id: str,
) -> None:
    with pytest.raises(ValidationError, match="opaque identifier"):
        RawBatchMetadata(
            batch_id=uuid4(),
            source=_source(),
            retrieved_at=datetime(2026, 8, 14, 14, tzinfo=UTC),
            media_type="application/json",
            file_extension="json",
            provider_request_id=provider_request_id,
        )


def test_raw_batch_metadata_rejects_naive_time_and_unsafe_extension() -> None:
    source = _source()
    with pytest.raises(ValidationError, match="timezone-aware"):
        RawBatchMetadata(
            batch_id=uuid4(),
            source=source,
            retrieved_at=datetime(2026, 8, 14, 14),
            media_type="application/json",
            file_extension="json",
        )

    with pytest.raises(ValidationError, match="safe extension"):
        RawBatchMetadata(
            batch_id=uuid4(),
            source=source,
            retrieved_at=datetime(2026, 8, 14, 14, tzinfo=UTC),
            media_type="application/json",
            file_extension="../json",
        )


def test_price_bar_preserves_nullability_and_point_in_time_distinctions() -> None:
    offset = timezone(timedelta(hours=2))
    bar = _bar(
        timestamp_start=datetime(2026, 8, 14, 15, 30, tzinfo=offset),
        timestamp_end=datetime(2026, 8, 14, 15, 35, tzinfo=offset),
        retrieved_at=datetime(2026, 8, 14, 15, 36, tzinfo=offset),
        ingested_at=datetime(2026, 8, 14, 15, 37, tzinfo=offset),
        available_at=None,
        volume=None,
        vwap=None,
        currency=None,
    )

    assert bar.timestamp_start == datetime(2026, 8, 14, 13, 30, tzinfo=UTC)
    assert bar.timestamp_end == datetime(2026, 8, 14, 13, 35, tzinfo=UTC)
    assert bar.available_at is None
    assert bar.retrieved_at != bar.ingested_at
    assert bar.volume is None
    assert bar.vwap is None


def test_price_bar_structural_validation_does_not_hide_quality_anomalies() -> None:
    flagged = _bar(
        high=98.0,
        volume=-1.0,
        quality_flags=(
            BarQualityFlag.OHLC_INCONSISTENT,
            BarQualityFlag.NEGATIVE_VOLUME,
            "provider_specific_flag",
        ),
    )

    assert flagged.high == 98.0
    assert flagged.volume == -1.0
    assert flagged.quality_flags == (
        BarQualityFlag.OHLC_INCONSISTENT.value,
        BarQualityFlag.NEGATIVE_VOLUME.value,
        "provider_specific_flag",
    )

    with pytest.raises(ValidationError):
        _bar(open=float("inf"))
    with pytest.raises(ValidationError):
        _bar(quality_flags=("  ",))


def test_price_bar_rejects_naive_or_non_half_open_interval() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        _bar(timestamp_start=datetime(2026, 8, 14, 13, 30))

    instant = datetime(2026, 8, 14, 13, 30, tzinfo=UTC)
    with pytest.raises(ValidationError, match="timestamp_end"):
        _bar(timestamp_start=instant, timestamp_end=instant)


def test_corporate_actions_are_discriminated_and_use_decimal() -> None:
    adapter: TypeAdapter[CorporateAction] = TypeAdapter(CorporateAction)
    common = _corporate_action_common()

    split = adapter.validate_python({**common, "action_type": "split", "split_ratio": "4"})
    dividend = adapter.validate_python(
        {**common, "action_type": "dividend", "amount": "0.25", "currency": "usd"}
    )
    ticker_change = adapter.validate_python(
        {**common, "action_type": "ticker_change", "old_ticker": "FB", "new_ticker": "META"}
    )

    assert isinstance(split, SplitAction)
    assert split.split_ratio == Decimal("4")
    assert isinstance(dividend, DividendAction)
    assert dividend.amount == Decimal("0.25")
    assert dividend.currency == "USD"
    assert isinstance(ticker_change, TickerChangeAction)
    assert ticker_change.instrument_id == common["instrument_id"]


def test_corporate_action_constraints_reject_impossible_values() -> None:
    common = _corporate_action_common()

    with pytest.raises(ValidationError):
        SplitAction(**common, split_ratio=Decimal("0"))
    with pytest.raises(ValidationError):
        DividendAction(**common, amount=Decimal("-0.01"), currency="USD")
    with pytest.raises(ValidationError, match="must differ"):
        TickerChangeAction(**common, old_ticker="abc", new_ticker="ABC")


def test_corporate_action_type_is_required_for_union_parsing() -> None:
    adapter: TypeAdapter[CorporateAction] = TypeAdapter(CorporateAction)
    payload = _corporate_action_common()
    payload["split_ratio"] = "2"

    with pytest.raises(ValidationError):
        adapter.validate_python(payload)
