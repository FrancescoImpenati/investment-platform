"""Vectorized, non-destructive quality checks for canonical price bars."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Final

import polars as pl

from investment_platform.data.models import BarQualityFlag

_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "instrument_id",
        "timeframe",
        "timestamp_start",
        "timestamp_end",
        "open",
        "high",
        "low",
        "close",
        "vwap",
        "volume",
        "session",
        "adjustment_state",
        "source_id",
        "quality_flags",
    }
)
_DUPLICATE_KEY: Final[tuple[str, ...]] = (
    "instrument_id",
    "source_id",
    "timeframe",
    "session",
    "adjustment_state",
    "timestamp_start",
)
_RULE_ORDER: Final[tuple[BarQualityFlag, ...]] = (
    BarQualityFlag.OHLC_INCONSISTENT,
    BarQualityFlag.NEGATIVE_VOLUME,
    BarQualityFlag.DUPLICATE_BAR,
    BarQualityFlag.NON_POSITIVE_PRICE,
    BarQualityFlag.UNEXPECTED_DURATION,
)
_MESSAGES: Final[Mapping[BarQualityFlag, str]] = {
    BarQualityFlag.OHLC_INCONSISTENT: "OHLC values violate the bar range invariants",
    BarQualityFlag.NEGATIVE_VOLUME: "volume is negative",
    BarQualityFlag.DUPLICATE_BAR: "another row has the same canonical observation key",
    BarQualityFlag.NON_POSITIVE_PRICE: "one or more OHLC/VWAP prices are not positive",
    BarQualityFlag.UNEXPECTED_DURATION: "a 5m bar does not span exactly five minutes",
}


class QualitySeverity(StrEnum):
    """Finding severity; neither level causes an observation to be removed."""

    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class BarValidationPolicy:
    """Configurable rules, using the positive-price check as the equity-oriented default."""

    check_non_positive_prices: bool = True


@dataclass(frozen=True, slots=True)
class BarValidationIssue:
    """One deterministic quality finding tied to the original row position and key."""

    row_index: int
    instrument_id: str
    timestamp_start: datetime
    code: BarQualityFlag
    severity: QualitySeverity
    message: str


@dataclass(frozen=True, slots=True)
class BarValidationResult:
    """Annotated frame plus structured findings and final row counts by managed flag."""

    frame: pl.DataFrame
    issues: tuple[BarValidationIssue, ...]
    counts_by_flag: Mapping[BarQualityFlag, int]

    @property
    def has_issues(self) -> bool:
        return bool(self.issues)


def _quality_flag_values(flags: object) -> list[str]:
    if flags is None:
        return []
    if not isinstance(flags, list):
        raise ValueError("quality_flags must contain lists of strings")
    values: list[str] = []
    for flag in flags:
        if not isinstance(flag, str):
            raise ValueError("quality_flags must contain lists of strings")
        normalized = flag.strip()
        if not normalized:
            raise ValueError("quality_flags must contain non-empty strings")
        if normalized not in values:
            values.append(normalized)
    return values


def _rule_masks(
    frame: pl.DataFrame, policy: BarValidationPolicy
) -> dict[BarQualityFlag, list[bool]]:
    priced_columns = ("open", "high", "low", "close", "vwap")
    ohlc_inconsistent = (
        (pl.col("high") < pl.col("low"))
        | (pl.col("high") < pl.col("open"))
        | (pl.col("high") < pl.col("close"))
        | (pl.col("low") > pl.col("open"))
        | (pl.col("low") > pl.col("close"))
    )
    negative_volume = (pl.col("volume").is_not_null() & (pl.col("volume") < 0)).fill_null(False)
    non_positive = pl.any_horizontal(*(pl.col(column) <= 0 for column in priced_columns)).fill_null(
        False
    )
    if not policy.check_non_positive_prices:
        non_positive = pl.lit(False)
    unexpected_duration = (
        (pl.col("timeframe") == "5m")
        & ((pl.col("timestamp_end") - pl.col("timestamp_start")) != timedelta(minutes=5))
    ).fill_null(False)

    evaluated = frame.with_columns(
        ohlc_inconsistent.fill_null(False).alias("__ohlc_inconsistent"),
        negative_volume.alias("__negative_volume"),
        pl.struct(_DUPLICATE_KEY).is_duplicated().alias("__duplicate_bar"),
        non_positive.alias("__non_positive_price"),
        unexpected_duration.alias("__unexpected_duration"),
    )
    names = {
        BarQualityFlag.OHLC_INCONSISTENT: "__ohlc_inconsistent",
        BarQualityFlag.NEGATIVE_VOLUME: "__negative_volume",
        BarQualityFlag.DUPLICATE_BAR: "__duplicate_bar",
        BarQualityFlag.NON_POSITIVE_PRICE: "__non_positive_price",
        BarQualityFlag.UNEXPECTED_DURATION: "__unexpected_duration",
    }
    return {
        flag: [bool(value) for value in evaluated.get_column(name).to_list()]
        for flag, name in names.items()
    }


def validate_bars(
    frame: pl.DataFrame,
    policy: BarValidationPolicy | None = None,
) -> BarValidationResult:
    """Annotate canonical bars while preserving cardinality and original row order."""

    missing = sorted(_REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"bar validation requires columns: {missing}")

    policy = policy or BarValidationPolicy()
    masks = _rule_masks(frame, policy)
    managed_values = {flag.value for flag in _RULE_ORDER}
    existing_flags = frame.get_column("quality_flags").to_list()
    updated_flags: list[list[str]] = []
    issues: list[BarValidationIssue] = []

    instrument_ids = frame.get_column("instrument_id").cast(pl.String).to_list()
    timestamps = frame.get_column("timestamp_start").to_list()
    for row_index in range(frame.height):
        existing = _quality_flag_values(existing_flags[row_index])
        row_flags = [flag for flag in existing if flag not in managed_values]
        for flag in _RULE_ORDER:
            if not masks[flag][row_index]:
                continue
            row_flags.append(flag.value)
            issues.append(
                BarValidationIssue(
                    row_index=row_index,
                    instrument_id=instrument_ids[row_index],
                    timestamp_start=timestamps[row_index],
                    code=flag,
                    severity=QualitySeverity.ERROR,
                    message=_MESSAGES[flag],
                )
            )
        updated_flags.append(row_flags)

    annotated = frame.with_columns(
        pl.Series("quality_flags", updated_flags, dtype=pl.List(pl.String))
    )
    counts = {
        flag: sum(flag.value in row_flags for row_flags in updated_flags) for flag in _RULE_ORDER
    }
    return BarValidationResult(
        frame=annotated,
        issues=tuple(issues),
        counts_by_flag=MappingProxyType(counts),
    )


__all__ = [
    "BarValidationIssue",
    "BarValidationPolicy",
    "BarValidationResult",
    "QualitySeverity",
    "validate_bars",
]
