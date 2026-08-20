"""Deterministic pairwise evidence for the Phase 1 provider bake-off."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

from investment_platform.data.market_time import to_utc
from investment_platform.data.models import (
    CorporateAction,
    DividendAction,
    PriceBar,
    SplitAction,
    TickerChangeAction,
    Timeframe,
)
from investment_platform.data.provenance import RawBatch

type _ActionValue = SplitAction | DividendAction | TickerChangeAction


class DiscrepancyClassification(StrEnum):
    """Interpretive categories that never select or blend a winning value."""

    DEFINITIONAL_DIFFERENCE = "definitional_difference"
    ADJUSTMENT_DIFFERENCE = "adjustment_difference"
    TIMING_SESSION_DIFFERENCE = "timing_session_difference"
    MISSING_OBSERVATION = "missing_observation"
    LIKELY_PROVIDER_ISSUE = "likely_provider_issue"
    UNRESOLVED_DISCREPANCY = "unresolved_discrepancy"


class ComparisonMetric(StrEnum):
    """Dimensions compared directly from canonical provider observations."""

    AVAILABILITY = "availability"
    DUPLICATE_BAR = "duplicate_bar"
    INTERVAL_BOUNDARY = "interval_boundary"
    SESSION = "session"
    ADJUSTMENT = "adjustment"
    CURRENCY = "currency"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"
    VWAP = "vwap"
    DUPLICATE_ACTION = "duplicate_action"
    EFFECTIVE_DATE = "effective_date"
    SPLIT_RATIO = "split_ratio"
    DIVIDEND_AMOUNT = "dividend_amount"
    OLD_TICKER = "old_ticker"
    NEW_TICKER = "new_ticker"


@dataclass(frozen=True, slots=True, order=True)
class ObservationKey:
    """Provider-neutral key used to align observations without their source identity."""

    instrument_id: UUID
    timeframe: Timeframe
    timestamp_start: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp_start", to_utc(self.timestamp_start))


@dataclass(frozen=True, slots=True)
class ComparisonTolerance:
    """Explicit numeric tolerances; defaults remain intentionally strict."""

    price_absolute: float = 1e-9
    price_relative: float = 1e-8
    volume_absolute: float = 0.0
    volume_relative: float = 1e-8
    five_minute_start_alignment: timedelta = timedelta(minutes=1)

    def __post_init__(self) -> None:
        values = (
            self.price_absolute,
            self.price_relative,
            self.volume_absolute,
            self.volume_relative,
        )
        if any(not math.isfinite(value) or value < 0 for value in values):
            raise ValueError("comparison tolerances must be finite and non-negative")
        if self.five_minute_start_alignment < timedelta(0):
            raise ValueError("timestamp alignment tolerance must be non-negative")
        if self.five_minute_start_alignment > timedelta(minutes=1):
            raise ValueError("5-minute timestamp alignment tolerance must not exceed one minute")


@dataclass(frozen=True, slots=True)
class ActionComparisonTolerance:
    """Conservative tolerance for matching differently-labelled action dates."""

    effective_date_alignment_days: int = 7

    def __post_init__(self) -> None:
        if not 0 <= self.effective_date_alignment_days <= 7:
            raise ValueError("action date alignment tolerance must be between zero and seven days")


@dataclass(frozen=True, slots=True)
class ProviderBarMetrics:
    """Cardinality evidence for one provider side of a comparison."""

    provider: str
    observation_count: int
    unique_key_count: int
    duplicate_observation_count: int
    expected_key_count: int | None
    missing_expected_count: int | None


@dataclass(frozen=True, slots=True)
class BarDiscrepancy:
    """One difference with direct raw-batch provenance from both sides."""

    key: ObservationKey
    metric: ComparisonMetric
    classification: DiscrepancyClassification
    left_provider: str
    right_provider: str
    left_value: str | float | None
    right_value: str | float | None
    left_raw_batch_ids: tuple[UUID, ...]
    right_raw_batch_ids: tuple[UUID, ...]


@dataclass(frozen=True, slots=True)
class BarComparisonReport:
    """Pairwise results; no averaging, winner policy, or mutation is performed."""

    left: ProviderBarMetrics
    right: ProviderBarMetrics
    discrepancies: tuple[BarDiscrepancy, ...]
    counts_by_classification: Mapping[DiscrepancyClassification, int]


@dataclass(frozen=True, slots=True, order=True)
class CorporateActionKey:
    """Provider-neutral action identity used for exact and conservative date alignment."""

    instrument_id: UUID
    action_type: str
    effective_date: date


@dataclass(frozen=True, slots=True)
class ProviderActionMetrics:
    """Cardinality evidence for one provider's canonical corporate actions."""

    provider: str
    observation_count: int
    unique_key_count: int
    duplicate_observation_count: int


@dataclass(frozen=True, slots=True)
class CorporateActionDiscrepancy:
    """One action difference retaining raw provenance from both provider sides."""

    key: CorporateActionKey
    metric: ComparisonMetric
    classification: DiscrepancyClassification
    left_provider: str
    right_provider: str
    left_value: str | float | None
    right_value: str | float | None
    left_raw_batch_ids: tuple[UUID, ...]
    right_raw_batch_ids: tuple[UUID, ...]


@dataclass(frozen=True, slots=True)
class CorporateActionComparisonReport:
    """Pairwise action results without winner selection or value reconciliation."""

    left: ProviderActionMetrics
    right: ProviderActionMetrics
    discrepancies: tuple[CorporateActionDiscrepancy, ...]
    counts_by_classification: Mapping[DiscrepancyClassification, int]


@dataclass(frozen=True, slots=True)
class ProviderRuntimeSummary:
    """Sanitized response-page and latency evidence recovered from raw manifests."""

    provider: str
    batch_count: int
    datasets: tuple[str, ...]
    observed_status_codes: tuple[int, ...]
    latency_sample_count: int
    total_latency_ms: float
    maximum_latency_ms: float | None
    observed_rate_limit_capacities: tuple[float, ...]
    minimum_rate_limit_remaining: float | None


def _observation_key(bar: PriceBar) -> ObservationKey:
    return ObservationKey(bar.instrument_id, bar.timeframe, bar.timestamp_start)


def _group_bars(bars: Iterable[PriceBar]) -> dict[ObservationKey, tuple[PriceBar, ...]]:
    grouped: defaultdict[ObservationKey, list[PriceBar]] = defaultdict(list)
    for bar in bars:
        grouped[_observation_key(bar)].append(bar)
    return {key: tuple(values) for key, values in grouped.items()}


def _bar_raw_ids(values: tuple[PriceBar, ...]) -> tuple[UUID, ...]:
    return tuple(sorted({bar.raw_batch_id for bar in values}, key=str))


def _action_raw_ids(values: tuple[_ActionValue, ...]) -> tuple[UUID, ...]:
    return tuple(sorted({action.raw_batch_id for action in values}, key=str))


def _render(value: object) -> str | float | None:
    if value is None:
        return None
    if isinstance(value, float):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    enum_value = getattr(value, "value", None)
    return str(enum_value if enum_value is not None else value)


def _numeric_equal(
    left: float | None,
    right: float | None,
    *,
    absolute: float,
    relative: float,
) -> bool:
    if left is None or right is None:
        return left is right
    return math.isclose(left, right, abs_tol=absolute, rel_tol=relative)


def _value_classification(left: PriceBar, right: PriceBar) -> DiscrepancyClassification:
    if left.adjustment_state is not right.adjustment_state:
        return DiscrepancyClassification.ADJUSTMENT_DIFFERENCE
    if (
        left.session is not right.session
        or left.timestamp_start != right.timestamp_start
        or left.timestamp_end != right.timestamp_end
    ):
        return DiscrepancyClassification.TIMING_SESSION_DIFFERENCE
    return DiscrepancyClassification.UNRESOLVED_DISCREPANCY


def _fuzzy_bar_pairs(
    left_keys: Iterable[ObservationKey],
    right_keys: Iterable[ObservationKey],
    alignment: timedelta,
) -> dict[ObservationKey, ObservationKey]:
    """Return only mutually unique, non-exact 5-minute timestamp matches."""

    left_values = tuple(left_keys)
    right_values = tuple(right_keys)

    def candidates(
        key: ObservationKey, choices: tuple[ObservationKey, ...]
    ) -> tuple[ObservationKey, ...]:
        return tuple(
            choice
            for choice in choices
            if key.instrument_id == choice.instrument_id
            and key.timeframe is Timeframe.FIVE_MINUTES
            and choice.timeframe is Timeframe.FIVE_MINUTES
            and timedelta(0) < abs(key.timestamp_start - choice.timestamp_start) <= alignment
        )

    left_candidates = {key: candidates(key, right_values) for key in left_values}
    right_candidates = {key: candidates(key, left_values) for key in right_values}
    return {
        left_key: right_candidates_for_left[0]
        for left_key, right_candidates_for_left in left_candidates.items()
        if len(right_candidates_for_left) == 1
        and len(right_candidates[right_candidates_for_left[0]]) == 1
    }


def _missing_expected_count(
    grouped: Mapping[ObservationKey, tuple[PriceBar, ...]],
    expected: frozenset[ObservationKey],
    alignment: timedelta,
) -> int:
    exact = expected & grouped.keys()
    unmatched_expected = expected - exact
    unmatched_actual = grouped.keys() - exact
    aligned = _fuzzy_bar_pairs(unmatched_expected, unmatched_actual, alignment)
    return len(unmatched_expected) - len(aligned)


def _metrics(
    provider: str,
    grouped: Mapping[ObservationKey, tuple[PriceBar, ...]],
    expected: frozenset[ObservationKey] | None,
    alignment: timedelta,
) -> ProviderBarMetrics:
    observation_count = sum(len(values) for values in grouped.values())
    duplicate_count = sum(max(0, len(values) - 1) for values in grouped.values())
    return ProviderBarMetrics(
        provider=provider,
        observation_count=observation_count,
        unique_key_count=len(grouped),
        duplicate_observation_count=duplicate_count,
        expected_key_count=len(expected) if expected is not None else None,
        missing_expected_count=(
            _missing_expected_count(grouped, expected, alignment) if expected is not None else None
        ),
    )


def compare_provider_bars(
    left_provider: str,
    left_bars: Iterable[PriceBar],
    right_provider: str,
    right_bars: Iterable[PriceBar],
    *,
    expected_keys: Iterable[ObservationKey] | None = None,
    tolerance: ComparisonTolerance | None = None,
) -> BarComparisonReport:
    """Compare two canonical datasets while retaining ambiguity and raw provenance."""

    left_provider = left_provider.strip()
    right_provider = right_provider.strip()
    if not left_provider or not right_provider:
        raise ValueError("provider names must not be blank")
    if left_provider.casefold() == right_provider.casefold():
        raise ValueError("provider names must identify two different datasets")
    tolerance = tolerance or ComparisonTolerance()
    expected = frozenset(expected_keys) if expected_keys is not None else None
    left = _group_bars(left_bars)
    right = _group_bars(right_bars)
    findings: list[BarDiscrepancy] = []

    left_unmatched = {key for key in left.keys() - right.keys() if len(left[key]) == 1}
    right_unmatched = {key for key in right.keys() - left.keys() if len(right[key]) == 1}
    shifted_pairs = _fuzzy_bar_pairs(
        left_unmatched,
        right_unmatched,
        tolerance.five_minute_start_alignment,
    )
    shifted_right_keys = frozenset(shifted_pairs.values())

    def add(
        key: ObservationKey,
        metric: ComparisonMetric,
        classification: DiscrepancyClassification,
        left_value: object,
        right_value: object,
        left_values: tuple[PriceBar, ...],
        right_values: tuple[PriceBar, ...],
    ) -> None:
        findings.append(
            BarDiscrepancy(
                key=key,
                metric=metric,
                classification=classification,
                left_provider=left_provider,
                right_provider=right_provider,
                left_value=_render(left_value),
                right_value=_render(right_value),
                left_raw_batch_ids=_bar_raw_ids(left_values),
                right_raw_batch_ids=_bar_raw_ids(right_values),
            )
        )

    all_keys = set(left) | set(right)
    if expected is not None:
        all_keys |= set(expected)
    for key in sorted(all_keys):
        if key in shifted_right_keys and key not in left:
            continue
        left_values = left.get(key, ())
        right_key = shifted_pairs.get(key, key)
        right_values = right.get(right_key, ())
        if len(left_values) > 1 or len(right_values) > 1:
            add(
                key,
                ComparisonMetric.DUPLICATE_BAR,
                DiscrepancyClassification.LIKELY_PROVIDER_ISSUE,
                len(left_values),
                len(right_values),
                left_values,
                right_values,
            )
            # Selecting a row from duplicate evidence would itself be a winner policy.
            continue
        if not left_values or not right_values:
            add(
                key,
                ComparisonMetric.AVAILABILITY,
                DiscrepancyClassification.MISSING_OBSERVATION,
                len(left_values),
                len(right_values),
                left_values,
                right_values,
            )
            continue

        left_bar = left_values[0]
        right_bar = right_values[0]
        if (
            left_bar.timestamp_start != right_bar.timestamp_start
            or left_bar.timestamp_end != right_bar.timestamp_end
        ):
            add(
                key,
                ComparisonMetric.INTERVAL_BOUNDARY,
                DiscrepancyClassification.TIMING_SESSION_DIFFERENCE,
                f"[{left_bar.timestamp_start.isoformat()}, {left_bar.timestamp_end.isoformat()})",
                f"[{right_bar.timestamp_start.isoformat()}, {right_bar.timestamp_end.isoformat()})",
                left_values,
                right_values,
            )
        if left_bar.session is not right_bar.session:
            add(
                key,
                ComparisonMetric.SESSION,
                DiscrepancyClassification.TIMING_SESSION_DIFFERENCE,
                left_bar.session,
                right_bar.session,
                left_values,
                right_values,
            )
        if left_bar.adjustment_state is not right_bar.adjustment_state:
            add(
                key,
                ComparisonMetric.ADJUSTMENT,
                DiscrepancyClassification.ADJUSTMENT_DIFFERENCE,
                left_bar.adjustment_state,
                right_bar.adjustment_state,
                left_values,
                right_values,
            )
        if left_bar.currency != right_bar.currency:
            add(
                key,
                ComparisonMetric.CURRENCY,
                DiscrepancyClassification.DEFINITIONAL_DIFFERENCE,
                left_bar.currency,
                right_bar.currency,
                left_values,
                right_values,
            )

        value_classification = _value_classification(left_bar, right_bar)
        for metric, field_name in (
            (ComparisonMetric.OPEN, "open"),
            (ComparisonMetric.HIGH, "high"),
            (ComparisonMetric.LOW, "low"),
            (ComparisonMetric.CLOSE, "close"),
            (ComparisonMetric.VWAP, "vwap"),
        ):
            left_value = getattr(left_bar, field_name)
            right_value = getattr(right_bar, field_name)
            if not _numeric_equal(
                left_value,
                right_value,
                absolute=tolerance.price_absolute,
                relative=tolerance.price_relative,
            ):
                add(
                    key,
                    metric,
                    value_classification,
                    left_value,
                    right_value,
                    left_values,
                    right_values,
                )
        if not _numeric_equal(
            left_bar.volume,
            right_bar.volume,
            absolute=tolerance.volume_absolute,
            relative=tolerance.volume_relative,
        ):
            add(
                key,
                ComparisonMetric.VOLUME,
                value_classification,
                left_bar.volume,
                right_bar.volume,
                left_values,
                right_values,
            )

    counts = {
        classification: sum(finding.classification is classification for finding in findings)
        for classification in DiscrepancyClassification
    }
    return BarComparisonReport(
        left=_metrics(
            left_provider,
            left,
            expected,
            tolerance.five_minute_start_alignment,
        ),
        right=_metrics(
            right_provider,
            right,
            expected,
            tolerance.five_minute_start_alignment,
        ),
        discrepancies=tuple(findings),
        counts_by_classification=MappingProxyType(counts),
    )


def _action_key(action: _ActionValue) -> CorporateActionKey:
    return CorporateActionKey(
        instrument_id=action.instrument_id,
        action_type=action.action_type,
        effective_date=action.effective_date,
    )


def _group_actions(
    actions: Iterable[CorporateAction],
) -> dict[CorporateActionKey, tuple[_ActionValue, ...]]:
    grouped: defaultdict[CorporateActionKey, list[_ActionValue]] = defaultdict(list)
    for action in actions:
        grouped[_action_key(action)].append(action)
    return {key: tuple(values) for key, values in grouped.items()}


def _fuzzy_action_pairs(
    left_keys: Iterable[CorporateActionKey],
    right_keys: Iterable[CorporateActionKey],
    alignment_days: int,
) -> dict[CorporateActionKey, CorporateActionKey]:
    """Return mutually unique action matches within a bounded date-label difference."""

    left_values = tuple(left_keys)
    right_values = tuple(right_keys)

    def candidates(
        key: CorporateActionKey,
        choices: tuple[CorporateActionKey, ...],
    ) -> tuple[CorporateActionKey, ...]:
        return tuple(
            choice
            for choice in choices
            if key.instrument_id == choice.instrument_id
            and key.action_type == choice.action_type
            and 0 < abs((key.effective_date - choice.effective_date).days) <= alignment_days
        )

    left_candidates = {key: candidates(key, right_values) for key in left_values}
    right_candidates = {key: candidates(key, left_values) for key in right_values}
    return {
        left_key: right_candidates_for_left[0]
        for left_key, right_candidates_for_left in left_candidates.items()
        if len(right_candidates_for_left) == 1
        and len(right_candidates[right_candidates_for_left[0]]) == 1
    }


def _action_metrics(
    provider: str,
    grouped: Mapping[CorporateActionKey, tuple[_ActionValue, ...]],
) -> ProviderActionMetrics:
    return ProviderActionMetrics(
        provider=provider,
        observation_count=sum(len(values) for values in grouped.values()),
        unique_key_count=len(grouped),
        duplicate_observation_count=sum(max(0, len(values) - 1) for values in grouped.values()),
    )


def compare_provider_actions(
    left_provider: str,
    left_actions: Iterable[CorporateAction],
    right_provider: str,
    right_actions: Iterable[CorporateAction],
    *,
    tolerance: ActionComparisonTolerance | None = None,
) -> CorporateActionComparisonReport:
    """Compare canonical actions pairwise without selecting, averaging, or rewriting values."""

    left_provider = left_provider.strip()
    right_provider = right_provider.strip()
    if not left_provider or not right_provider:
        raise ValueError("provider names must not be blank")
    if left_provider.casefold() == right_provider.casefold():
        raise ValueError("provider names must identify two different datasets")
    tolerance = tolerance or ActionComparisonTolerance()
    left = _group_actions(left_actions)
    right = _group_actions(right_actions)
    findings: list[CorporateActionDiscrepancy] = []

    left_unmatched = {key for key in left.keys() - right.keys() if len(left[key]) == 1}
    right_unmatched = {key for key in right.keys() - left.keys() if len(right[key]) == 1}
    shifted_pairs = _fuzzy_action_pairs(
        left_unmatched,
        right_unmatched,
        tolerance.effective_date_alignment_days,
    )
    shifted_right_keys = frozenset(shifted_pairs.values())

    def add(
        key: CorporateActionKey,
        metric: ComparisonMetric,
        classification: DiscrepancyClassification,
        left_value: object,
        right_value: object,
        left_values: tuple[_ActionValue, ...],
        right_values: tuple[_ActionValue, ...],
    ) -> None:
        findings.append(
            CorporateActionDiscrepancy(
                key=key,
                metric=metric,
                classification=classification,
                left_provider=left_provider,
                right_provider=right_provider,
                left_value=_render(left_value),
                right_value=_render(right_value),
                left_raw_batch_ids=_action_raw_ids(left_values),
                right_raw_batch_ids=_action_raw_ids(right_values),
            )
        )

    for key in sorted(set(left) | set(right)):
        if key in shifted_right_keys and key not in left:
            continue
        left_values = left.get(key, ())
        right_values = right.get(shifted_pairs.get(key, key), ())
        if len(left_values) > 1 or len(right_values) > 1:
            add(
                key,
                ComparisonMetric.DUPLICATE_ACTION,
                DiscrepancyClassification.LIKELY_PROVIDER_ISSUE,
                len(left_values),
                len(right_values),
                left_values,
                right_values,
            )
            continue
        if not left_values or not right_values:
            add(
                key,
                ComparisonMetric.AVAILABILITY,
                DiscrepancyClassification.MISSING_OBSERVATION,
                len(left_values),
                len(right_values),
                left_values,
                right_values,
            )
            continue

        left_action = left_values[0]
        right_action = right_values[0]
        if left_action.effective_date != right_action.effective_date:
            add(
                key,
                ComparisonMetric.EFFECTIVE_DATE,
                DiscrepancyClassification.UNRESOLVED_DISCREPANCY,
                left_action.effective_date,
                right_action.effective_date,
                left_values,
                right_values,
            )
        if isinstance(left_action, SplitAction) and isinstance(right_action, SplitAction):
            if left_action.split_ratio != right_action.split_ratio:
                add(
                    key,
                    ComparisonMetric.SPLIT_RATIO,
                    DiscrepancyClassification.UNRESOLVED_DISCREPANCY,
                    left_action.split_ratio,
                    right_action.split_ratio,
                    left_values,
                    right_values,
                )
        elif isinstance(left_action, DividendAction) and isinstance(right_action, DividendAction):
            if left_action.amount != right_action.amount:
                add(
                    key,
                    ComparisonMetric.DIVIDEND_AMOUNT,
                    DiscrepancyClassification.UNRESOLVED_DISCREPANCY,
                    left_action.amount,
                    right_action.amount,
                    left_values,
                    right_values,
                )
            if left_action.currency != right_action.currency:
                add(
                    key,
                    ComparisonMetric.CURRENCY,
                    DiscrepancyClassification.UNRESOLVED_DISCREPANCY,
                    left_action.currency,
                    right_action.currency,
                    left_values,
                    right_values,
                )
        elif isinstance(left_action, TickerChangeAction) and isinstance(
            right_action, TickerChangeAction
        ):
            for metric, left_value, right_value in (
                (ComparisonMetric.OLD_TICKER, left_action.old_ticker, right_action.old_ticker),
                (ComparisonMetric.NEW_TICKER, left_action.new_ticker, right_action.new_ticker),
            ):
                if left_value != right_value:
                    add(
                        key,
                        metric,
                        DiscrepancyClassification.DEFINITIONAL_DIFFERENCE,
                        left_value,
                        right_value,
                        left_values,
                        right_values,
                    )
        else:
            # The key includes action_type, so reaching this branch indicates malformed input.
            add(
                key,
                ComparisonMetric.AVAILABILITY,
                DiscrepancyClassification.UNRESOLVED_DISCREPANCY,
                type(left_action).__name__,
                type(right_action).__name__,
                left_values,
                right_values,
            )

    counts = {
        classification: sum(finding.classification is classification for finding in findings)
        for classification in DiscrepancyClassification
    }
    return CorporateActionComparisonReport(
        left=_action_metrics(left_provider, left),
        right=_action_metrics(right_provider, right),
        discrepancies=tuple(findings),
        counts_by_classification=MappingProxyType(counts),
    )


def summarize_provider_runtime(
    provider: str,
    batches: Iterable[RawBatch],
) -> ProviderRuntimeSummary:
    """Summarize only sanitized manifest fields; error attempts remain separate evidence."""

    provider = provider.strip()
    if not provider:
        raise ValueError("provider must not be blank")
    batch_values = tuple(batches)
    latency_values: list[float] = []
    status_values: set[int] = set()
    capacity_values: set[float] = set()
    remaining_values: list[float] = []
    datasets: set[str] = set()
    for batch in batch_values:
        if batch.metadata.source.provider.casefold() != provider.casefold():
            raise ValueError("raw batch provider does not match the requested runtime summary")
        datasets.add(batch.metadata.source.dataset)
        latency = batch.metadata.request_metadata.get("latency_ms")
        if isinstance(latency, (int, float)) and not isinstance(latency, bool):
            latency_values.append(float(latency))
        status = batch.metadata.request_metadata.get("response_status")
        if isinstance(status, int) and not isinstance(status, bool):
            status_values.add(status)
        capacity = batch.metadata.request_metadata.get("rate_limit_capacity")
        if isinstance(capacity, (int, float)) and not isinstance(capacity, bool):
            capacity_values.add(float(capacity))
        remaining = batch.metadata.request_metadata.get("rate_limit_remaining")
        if isinstance(remaining, (int, float)) and not isinstance(remaining, bool):
            remaining_values.append(float(remaining))
    return ProviderRuntimeSummary(
        provider=provider,
        batch_count=len(batch_values),
        datasets=tuple(sorted(datasets)),
        observed_status_codes=tuple(sorted(status_values)),
        latency_sample_count=len(latency_values),
        total_latency_ms=sum(latency_values),
        maximum_latency_ms=max(latency_values, default=None),
        observed_rate_limit_capacities=tuple(sorted(capacity_values)),
        minimum_rate_limit_remaining=min(remaining_values, default=None),
    )


__all__ = [
    "ActionComparisonTolerance",
    "BarComparisonReport",
    "BarDiscrepancy",
    "ComparisonMetric",
    "ComparisonTolerance",
    "CorporateActionComparisonReport",
    "CorporateActionDiscrepancy",
    "CorporateActionKey",
    "DiscrepancyClassification",
    "ObservationKey",
    "ProviderActionMetrics",
    "ProviderBarMetrics",
    "ProviderRuntimeSummary",
    "compare_provider_actions",
    "compare_provider_bars",
    "summarize_provider_runtime",
]
