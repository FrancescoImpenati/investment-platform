"""Synchronous, paginable provider protocol and bounded request contracts."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from typing import Annotated, Protocol, Self, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.market_time import to_utc
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.provenance import RawBatch

NonEmptyStr = Annotated[str, Field(min_length=1)]


class _FrozenRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class ProviderInstrumentRef(_FrozenRequest):
    """Link an internal instrument UUID to one provider identifier."""

    instrument_id: UUID
    provider_identifier: NonEmptyStr


class BarRequest(_FrozenRequest):
    """A bounded, mode-agnostic request for half-open UTC bar intervals."""

    instruments: Annotated[tuple[ProviderInstrumentRef, ...], Field(min_length=1)]
    timeframe: Timeframe
    start: datetime
    end: datetime
    session: TradingSession = TradingSession.REGULAR
    adjustment_state: AdjustmentState = AdjustmentState.UNADJUSTED

    @field_validator("start", "end", mode="after")
    @classmethod
    def normalize_datetimes(cls, value: datetime) -> datetime:
        return to_utc(value)

    @model_validator(mode="after")
    def validate_request(self) -> Self:
        if self.end <= self.start:
            raise ValueError("end must be later than start")
        keys = {(ref.instrument_id, ref.provider_identifier) for ref in self.instruments}
        if len(keys) != len(self.instruments):
            raise ValueError("instruments must not contain duplicate provider references")
        return self


class CorporateActionRequest(_FrozenRequest):
    """A bounded request for corporate actions effective on ``[start, end)``."""

    instruments: Annotated[tuple[ProviderInstrumentRef, ...], Field(min_length=1)]
    start: date
    end: date

    @model_validator(mode="after")
    def validate_request(self) -> Self:
        if self.end <= self.start:
            raise ValueError("end must be later than start")
        keys = {(ref.instrument_id, ref.provider_identifier) for ref in self.instruments}
        if len(keys) != len(self.instruments):
            raise ValueError("instruments must not contain duplicate provider references")
        return self


@runtime_checkable
class MarketDataProvider(Protocol):
    """Vendor-neutral synchronous provider contract whose iterables may represent pages."""

    @property
    def provider_name(self) -> str:
        """Return the stable provider name represented by this adapter."""

        ...

    def get_instruments(self, *, as_of: date | None = None) -> Iterable[RawBatch]:
        """Return provider-native instrument pages, optionally as known on a date."""

        ...

    def get_bars(self, request: BarRequest) -> Iterable[RawBatch]:
        """Return provider-native pages for a bounded bar request."""

        ...

    def get_corporate_actions(self, request: CorporateActionRequest) -> Iterable[RawBatch]:
        """Return provider-native pages for a bounded corporate-action request."""

        ...
