"""Canonical market-data and corporate-action domain contracts."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.market_time import to_utc

NonEmptyStr = Annotated[str, Field(min_length=1)]


class _FrozenModel(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class Timeframe(StrEnum):
    """Timeframes implemented by the Phase 0 canonical schema."""

    ONE_DAY = "1d"
    FIVE_MINUTES = "5m"


class TradingSession(StrEnum):
    """Session classification retained independently from bar timestamps."""

    REGULAR = "regular"
    PRE_MARKET = "pre_market"
    POST_MARKET = "post_market"
    OVERNIGHT = "overnight"
    UNKNOWN = "unknown"


class AdjustmentState(StrEnum):
    """How provider or curated price fields have been adjusted."""

    UNADJUSTED = "unadjusted"
    SPLIT_ADJUSTED = "split_adjusted"
    SPLIT_AND_DIVIDEND_ADJUSTED = "split_and_dividend_adjusted"
    PROVIDER_ADJUSTED_UNKNOWN = "provider_adjusted_unknown"
    UNKNOWN = "unknown"


class BarQualityFlag(StrEnum):
    """Phase 0 quality annotations applied without dropping observations."""

    OHLC_INCONSISTENT = "ohlc_inconsistent"
    NEGATIVE_VOLUME = "negative_volume"
    DUPLICATE_BAR = "duplicate_bar"
    NON_POSITIVE_PRICE = "non_positive_price"
    UNEXPECTED_DURATION = "unexpected_duration"


class PriceBar(_FrozenModel):
    """A canonical price observation over a half-open UTC interval."""

    instrument_id: UUID
    timeframe: Timeframe
    timestamp_start: datetime
    timestamp_end: datetime
    open: float
    high: float
    low: float
    close: float
    source_id: UUID
    raw_batch_id: UUID
    retrieved_at: datetime
    ingested_at: datetime
    volume: float | None = None
    vwap: float | None = None
    currency: NonEmptyStr | None = None
    session: TradingSession = TradingSession.UNKNOWN
    adjustment_state: AdjustmentState = AdjustmentState.UNKNOWN
    provider_record_id: NonEmptyStr | None = None
    available_at: datetime | None = None
    quality_flags: tuple[NonEmptyStr, ...] = Field(default_factory=tuple)

    @field_validator(
        "timestamp_start",
        "timestamp_end",
        "retrieved_at",
        "ingested_at",
        "available_at",
        mode="after",
    )
    @classmethod
    def normalize_datetimes(cls, value: datetime | None) -> datetime | None:
        return to_utc(value) if value is not None else None

    @field_validator("currency", mode="after")
    @classmethod
    def normalize_currency(cls, value: str | None) -> str | None:
        return value.upper() if value is not None else None

    @model_validator(mode="after")
    def validate_interval(self) -> Self:
        if self.timestamp_end <= self.timestamp_start:
            raise ValueError("timestamp_end must be later than timestamp_start")
        if self.ingested_at < self.retrieved_at:
            raise ValueError("ingested_at must not be earlier than retrieved_at")
        return self


class _CorporateActionBase(_FrozenModel):
    instrument_id: UUID
    effective_date: date
    source_id: UUID
    raw_batch_id: UUID
    retrieved_at: datetime
    ingested_at: datetime
    available_at: datetime | None = None
    provider_record_id: NonEmptyStr | None = None

    @field_validator("retrieved_at", "ingested_at", "available_at", mode="after")
    @classmethod
    def normalize_datetimes(cls, value: datetime | None) -> datetime | None:
        return to_utc(value) if value is not None else None

    @model_validator(mode="after")
    def validate_ingestion_order(self) -> Self:
        if self.ingested_at < self.retrieved_at:
            raise ValueError("ingested_at must not be earlier than retrieved_at")
        return self


class SplitAction(_CorporateActionBase):
    """A split represented by new shares received per old share."""

    action_type: Literal["split"] = "split"
    split_ratio: Decimal = Field(gt=Decimal(0))


class DividendAction(_CorporateActionBase):
    """A positive cash distribution in an explicit currency."""

    action_type: Literal["dividend"] = "dividend"
    amount: Decimal = Field(gt=Decimal(0))
    currency: Annotated[str, Field(min_length=3, max_length=3)]

    @field_validator("currency", mode="after")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()


class TickerChangeAction(_CorporateActionBase):
    """A ticker change that preserves the stable internal instrument identity."""

    action_type: Literal["ticker_change"] = "ticker_change"
    old_ticker: NonEmptyStr
    new_ticker: NonEmptyStr

    @model_validator(mode="after")
    def validate_ticker_change(self) -> Self:
        if self.old_ticker.casefold() == self.new_ticker.casefold():
            raise ValueError("old_ticker and new_ticker must differ")
        return self


type CorporateAction = Annotated[
    SplitAction | DividendAction | TickerChangeAction,
    Field(discriminator="action_type"),
]
