"""Definitions for deterministic, parameterized observed features."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from investment_platform._immutable import FrozenMapping
from investment_platform.data.models import Timeframe

type JsonScalar = str | int | float | bool | None
NonEmptyStr = Annotated[str, Field(min_length=1)]


class FeatureDefinition(BaseModel):
    """A versioned feature recipe, separate from values and predictive models."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )

    name: NonEmptyStr
    version: NonEmptyStr
    input_timeframe: Timeframe
    description: NonEmptyStr | None = None
    parameters: Mapping[str, JsonScalar] = Field(default_factory=dict)

    @field_validator("parameters", mode="after")
    @classmethod
    def freeze_parameters(cls, value: Mapping[str, JsonScalar]) -> Mapping[str, JsonScalar]:
        return FrozenMapping(value)

    @field_serializer("parameters")
    def serialize_parameters(self, value: Mapping[str, JsonScalar]) -> dict[str, JsonScalar]:
        return dict(value)
