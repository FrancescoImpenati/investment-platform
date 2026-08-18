"""Tests for parametric feature definitions without an execution engine."""

from copy import deepcopy
from typing import cast

import pytest
from pydantic import ValidationError

from investment_platform.data.models import Timeframe
from investment_platform.features import FeatureDefinition

pytestmark = pytest.mark.unit


def test_feature_definition_is_parametric() -> None:
    short = FeatureDefinition(
        name="realized_volatility",
        version="1",
        input_timeframe=Timeframe.ONE_DAY,
        description="Observed rolling volatility",
        parameters={"lookback": 20, "annualize": True},
    )
    long = FeatureDefinition(
        name="realized_volatility",
        version="1",
        input_timeframe=Timeframe.ONE_DAY,
        parameters={"lookback": 63, "annualize": True},
    )

    assert short.name == long.name
    assert short.parameters["lookback"] == 20
    assert long.parameters["lookback"] == 63


def test_feature_parameters_are_limited_to_json_scalars() -> None:
    with pytest.raises(ValidationError):
        FeatureDefinition.model_validate(
            {
                "name": "bad_feature",
                "version": "1",
                "input_timeframe": "1d",
                "parameters": {"lookbacks": [20, 63]},
            }
        )

    with pytest.raises(ValidationError):
        FeatureDefinition(
            name="bad_feature",
            version="1",
            input_timeframe=Timeframe.ONE_DAY,
            parameters={"threshold": float("nan")},
        )


def test_feature_definition_is_frozen_and_forbids_extra_fields() -> None:
    definition = FeatureDefinition(
        name="momentum",
        version="1",
        input_timeframe=Timeframe.ONE_DAY,
        parameters={"lookback": 20},
    )
    version_field = "version"

    assert definition.model_dump(mode="json")["parameters"] == {"lookback": 20}
    assert deepcopy(definition).model_dump(mode="json") == definition.model_dump(mode="json")
    assert definition.model_copy(deep=True).model_dump(mode="json") == definition.model_dump(
        mode="json"
    )
    with pytest.raises(ValidationError):
        setattr(definition, version_field, "2")

    with pytest.raises(ValidationError):
        FeatureDefinition.model_validate(
            {
                "name": "momentum",
                "version": "1",
                "input_timeframe": "1d",
                "parameters": {},
                "executor": "not-in-phase-zero",
            }
        )

    with pytest.raises(TypeError):
        cast(dict[str, object], definition.parameters)["lookback"] = 63
