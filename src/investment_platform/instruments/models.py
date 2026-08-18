"""Domain models for stable instruments and temporal universe membership."""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Annotated, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.market_time import to_utc

NonEmptyStr = Annotated[str, Field(min_length=1)]


class _FrozenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class AssetClass(StrEnum):
    """Broad instrument classes; ticker symbols are deliberately not identities."""

    EQUITY = "equity"
    ETF = "etf"
    INDEX = "index"
    BOND = "bond"
    FUTURE = "future"
    OTHER = "other"


class InstrumentIdentifier(_FrozenModel):
    """An external identifier valid on a half-open date interval."""

    namespace: NonEmptyStr
    value: NonEmptyStr
    provider: NonEmptyStr | None = None
    valid_from: date
    valid_to: date | None = None

    @model_validator(mode="after")
    def validate_validity_interval(self) -> Self:
        if self.valid_to is not None and self.valid_to <= self.valid_from:
            raise ValueError("valid_to must be later than valid_from")
        return self

    def is_valid_on(self, on_date: date) -> bool:
        """Return whether this identifier is valid on ``on_date``."""

        return self.valid_from <= on_date and (self.valid_to is None or on_date < self.valid_to)


class Instrument(_FrozenModel):
    """A stable internal identity independent from temporal provider identifiers."""

    instrument_id: UUID
    asset_class: AssetClass
    name: NonEmptyStr
    primary_currency: Annotated[str, Field(min_length=3, max_length=3)] | None = None
    mic: Annotated[str, Field(min_length=4, max_length=4)] | None = None
    identifiers: tuple[InstrumentIdentifier, ...] = Field(default_factory=tuple)

    @field_validator("primary_currency", "mic", mode="after")
    @classmethod
    def normalize_codes(cls, value: str | None) -> str | None:
        return value.upper() if value is not None else None


class Universe(_FrozenModel):
    """A named, provider-independent collection of instruments."""

    universe_id: UUID
    name: NonEmptyStr
    description: NonEmptyStr | None = None
    source: NonEmptyStr | None = None


class UniverseMembership(_FrozenModel):
    """Point-in-time universe membership over a half-open date interval."""

    membership_id: UUID
    universe_id: UUID
    instrument_id: UUID
    valid_from: date
    ingested_at: datetime
    valid_to: date | None = None
    available_at: datetime | None = None

    @field_validator("ingested_at", "available_at", mode="after")
    @classmethod
    def normalize_datetimes(cls, value: datetime | None) -> datetime | None:
        return to_utc(value) if value is not None else None

    @model_validator(mode="after")
    def validate_validity_interval(self) -> Self:
        if self.valid_to is not None and self.valid_to <= self.valid_from:
            raise ValueError("valid_to must be later than valid_from")
        return self

    def is_active_on(self, on_date: date) -> bool:
        """Return whether the membership is active on ``on_date``."""

        return self.valid_from <= on_date and (self.valid_to is None or on_date < self.valid_to)
