"""Finite, opt-in orchestration primitives for the Phase 1 provider bake-off.

This module is deliberately a bounded research runner, not a scheduler, retry
engine, trading calendar, or persistent ingestion service.  Real provider data
must be processed under :func:`phase1_temporary_data_root` and only the
sanitized summaries defined here may outlive that context.
"""

from __future__ import annotations

import math
import tempfile
import time
from collections import Counter, deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Protocol
from uuid import UUID

import polars as pl

from investment_platform.data.market_time import nominal_us_rth_bounds
from investment_platform.data.models import AdjustmentState, PriceBar, Timeframe, TradingSession
from investment_platform.data.normalization import (
    BarNormalizationResult,
    DailyBarSemantics,
    NormalizationSeverity,
    SessionBounds,
    StaticSessionSchedule,
)
from investment_platform.data.provenance import RawBatch
from investment_platform.data.providers import BarRequest, ProviderInstrumentRef
from investment_platform.data.quality import BarComparisonReport, ObservationKey
from investment_platform.data.storage import (
    ParquetBarStore,
    RawBatchStore,
    price_bars_to_frame,
    replay_raw_artifact,
)
from investment_platform.data.validation import QualitySeverity, validate_bars


@dataclass(frozen=True, slots=True)
class Phase1Security:
    """One frozen sample member with provider identifiers kept external."""

    symbol: str
    instrument_id: UUID
    alpaca_identifier: str
    twelve_data_identifier: str
    yfinance_identifier: str

    def provider_identifier(self, provider: str) -> str:
        normalized = provider.strip().casefold().replace("-", "_")
        identifiers = {
            "alpaca": self.alpaca_identifier,
            "alpaca_sip": self.alpaca_identifier,
            "twelve_data": self.twelve_data_identifier,
            "yfinance": self.yfinance_identifier,
        }
        try:
            return identifiers[normalized]
        except KeyError as error:
            raise ValueError(
                f"unsupported Phase 1 provider identifier namespace: {provider!r}"
            ) from error


PHASE1_SECURITIES: tuple[Phase1Security, ...] = (
    Phase1Security(
        "AAPL",
        UUID("1923431d-8907-4f63-ba11-68182c11f778"),
        "AAPL",
        "AAPL",
        "AAPL",
    ),
    Phase1Security(
        "MSFT",
        UUID("b0793b0a-644e-4739-a75e-f20a244b478b"),
        "MSFT",
        "MSFT",
        "MSFT",
    ),
    Phase1Security(
        "NVDA",
        UUID("b8c8923d-0c66-4cb9-ac35-85072ecbadc1"),
        "NVDA",
        "NVDA",
        "NVDA",
    ),
    Phase1Security(
        "AMZN",
        UUID("84349b31-9a65-48f6-b84f-ed451a756af2"),
        "AMZN",
        "AMZN",
        "AMZN",
    ),
    Phase1Security(
        "NFLX",
        UUID("78795cab-5e96-4617-b6cb-a452d443b2f8"),
        "NFLX",
        "NFLX",
        "NFLX",
    ),
    Phase1Security(
        "JPM",
        UUID("20951dc1-e959-4bb3-b21e-2725c6e7d30d"),
        "JPM",
        "JPM",
        "JPM",
    ),
    Phase1Security(
        "IBKR",
        UUID("ad2c358c-f495-4267-b6d3-d4e9f75c6ce9"),
        "IBKR",
        "IBKR",
        "IBKR",
    ),
    Phase1Security(
        "BRK.B",
        UUID("73c80583-2161-412b-8843-2d8fbc51353f"),
        "BRK.B",
        "BRK.B",
        "BRK-B",
    ),
    Phase1Security(
        "XYZ",
        UUID("eaef544a-6b12-4077-b866-2206b2a64832"),
        "XYZ",
        "XYZ",
        "XYZ",
    ),
    Phase1Security(
        "XOM",
        UUID("d4937c21-210c-474c-bc55-47dd2c415b7e"),
        "XOM",
        "XOM",
        "XOM",
    ),
    Phase1Security(
        "JNJ",
        UUID("d8dcd9b0-96fe-4c6c-b2cd-2a5616239791"),
        "JNJ",
        "JNJ",
        "JNJ",
    ),
    Phase1Security(
        "KO",
        UUID("93f71c2d-6377-489f-99b0-1989fe7009e4"),
        "KO",
        "KO",
        "KO",
    ),
    Phase1Security(
        "BA",
        UUID("1fdcf62d-840a-4b10-a799-ef8929ab06b8"),
        "BA",
        "BA",
        "BA",
    ),
    Phase1Security(
        "ORLY",
        UUID("53a26cfb-760f-4fb6-ba2f-a5ba314ee794"),
        "ORLY",
        "ORLY",
        "ORLY",
    ),
    Phase1Security(
        "NEE",
        UUID("dd2d973e-0a0d-47b1-a250-7aab0b43ce71"),
        "NEE",
        "NEE",
        "NEE",
    ),
    Phase1Security(
        "SPY",
        UUID("7d1a5577-94da-4267-a1fb-e9bd3aa2555c"),
        "SPY",
        "SPY",
        "SPY",
    ),
)

_SECURITY_BY_SYMBOL = MappingProxyType(
    {security.symbol: security for security in PHASE1_SECURITIES}
)

DAILY_CORE_START = datetime(2025, 5, 27, tzinfo=UTC)
DAILY_CORE_END = datetime(2025, 12, 6, tzinfo=UTC)
TICKER_CONTINUITY_START = datetime(2025, 1, 13, tzinfo=UTC)
TICKER_CHANGE_DATE = datetime(2025, 1, 21, tzinfo=UTC)
TICKER_CONTINUITY_END = datetime(2025, 1, 29, tzinfo=UTC)
CORPORATE_ACTION_START = date(2025, 1, 1)
CORPORATE_ACTION_END = date(2025, 12, 6)
KO_ADJUSTMENT_START = datetime(2025, 6, 9, tzinfo=UTC)
KO_ADJUSTMENT_END = datetime(2025, 6, 21, tzinfo=UTC)

PHASE1_CLOSED_SESSIONS: frozenset[date] = frozenset(
    {
        date(2025, 1, 20),
        date(2025, 6, 19),
        date(2025, 7, 4),
        date(2025, 9, 1),
        date(2025, 11, 27),
    }
)
PHASE1_EARLY_CLOSES: Mapping[date, datetime] = MappingProxyType(
    {
        date(2025, 7, 3): datetime(2025, 7, 3, 17, tzinfo=UTC),
        date(2025, 11, 28): datetime(2025, 11, 28, 18, tzinfo=UTC),
    }
)


def _weekdays(start: date, end: date) -> Iterator[date]:
    current = start
    while current < end:
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)


def build_phase1_session_schedule() -> StaticSessionSchedule:
    """Build only the 148 explicitly bounded sessions required by the frozen study."""

    dates = {
        session_date
        for session_date in _weekdays(date(2025, 5, 27), date(2025, 12, 6))
        if session_date not in PHASE1_CLOSED_SESSIONS
    }
    dates.update(
        session_date
        for session_date in _weekdays(date(2025, 1, 13), date(2025, 1, 29))
        if session_date not in PHASE1_CLOSED_SESSIONS
    )
    dates.update({date(2025, 3, 7), date(2025, 3, 10)})
    sessions: list[SessionBounds] = []
    for session_date in sorted(dates):
        start, nominal_end = nominal_us_rth_bounds(session_date)
        end = PHASE1_EARLY_CLOSES.get(session_date, nominal_end)
        sessions.append(
            SessionBounds(
                session_date=session_date,
                start=start,
                end=end,
                source="frozen Phase 1 NYSE 2025 session oracle",
            )
        )
    if len(sessions) != 148:
        raise RuntimeError("frozen Phase 1 session schedule cardinality changed")
    return StaticSessionSchedule(tuple(sessions))


class Phase1BarSegment(StrEnum):
    DAILY_UNADJUSTED = "daily_unadjusted"
    DAILY_SPLIT_ADJUSTED = "daily_split_adjusted"
    CALENDAR_FIVE_MINUTE = "calendar_5m_unadjusted"
    SPLIT_BOUNDARY_UNADJUSTED = "split_boundary_5m_unadjusted"
    SPLIT_BOUNDARY_SPLIT_ADJUSTED = "split_boundary_5m_split_adjusted"
    TICKER_CONTINUITY = "ticker_continuity_daily_unadjusted"


@dataclass(frozen=True, slots=True)
class Phase1BarWindow:
    """One canonical request window before provider-specific batching."""

    label: str
    segment: Phase1BarSegment
    targets: tuple[Phase1Security, ...]
    timeframe: Timeframe
    start: datetime
    end: datetime
    adjustment_state: AdjustmentState


@dataclass(frozen=True, slots=True)
class PlannedBarRequest:
    """One provider request with its finite-study credit weight."""

    label: str
    segment: Phase1BarSegment
    request: BarRequest
    credit_cost: int


def _window(
    *,
    label: str,
    segment: Phase1BarSegment,
    targets: tuple[Phase1Security, ...],
    timeframe: Timeframe,
    start: datetime,
    end: datetime,
    adjustment_state: AdjustmentState,
) -> Phase1BarWindow:
    return Phase1BarWindow(
        label=label,
        segment=segment,
        targets=targets,
        timeframe=timeframe,
        start=start,
        end=end,
        adjustment_state=adjustment_state,
    )


def _session_window(
    schedule: StaticSessionSchedule,
    session_date: date,
) -> tuple[datetime, datetime]:
    bounds = schedule.bounds_for(session_date)
    if bounds is None:
        raise RuntimeError(f"frozen Phase 1 schedule lacks {session_date.isoformat()}")
    return bounds.start, bounds.end


def _temporal_security(symbol: str) -> Phase1Security:
    current = _SECURITY_BY_SYMBOL["XYZ"]
    return Phase1Security(
        symbol=symbol,
        instrument_id=current.instrument_id,
        alpaca_identifier=symbol,
        twelve_data_identifier=symbol,
        yfinance_identifier=symbol,
    )


def build_phase1_bar_windows(
    schedule: StaticSessionSchedule | None = None,
) -> tuple[Phase1BarWindow, ...]:
    """Return the 22 canonical windows preregistered for the bars-first bake-off."""

    schedule = schedule or build_phase1_session_schedule()
    windows: list[Phase1BarWindow] = [
        _window(
            label="daily-core-raw",
            segment=Phase1BarSegment.DAILY_UNADJUSTED,
            targets=PHASE1_SECURITIES,
            timeframe=Timeframe.ONE_DAY,
            start=DAILY_CORE_START,
            end=DAILY_CORE_END,
            adjustment_state=AdjustmentState.UNADJUSTED,
        ),
        _window(
            label="daily-core-split",
            segment=Phase1BarSegment.DAILY_SPLIT_ADJUSTED,
            targets=PHASE1_SECURITIES,
            timeframe=Timeframe.ONE_DAY,
            start=DAILY_CORE_START,
            end=DAILY_CORE_END,
            adjustment_state=AdjustmentState.SPLIT_ADJUSTED,
        ),
    ]
    calendar_dates = (
        date(2025, 3, 7),
        date(2025, 3, 10),
        date(2025, 7, 2),
        date(2025, 7, 3),
        date(2025, 10, 31),
        date(2025, 11, 3),
    )
    for session_date in calendar_dates:
        start, end = _session_window(schedule, session_date)
        windows.append(
            _window(
                label=f"calendar-{session_date.isoformat()}",
                segment=Phase1BarSegment.CALENDAR_FIVE_MINUTE,
                targets=PHASE1_SECURITIES,
                timeframe=Timeframe.FIVE_MINUTES,
                start=start,
                end=end,
                adjustment_state=AdjustmentState.UNADJUSTED,
            )
        )

    split_dates = {
        "ORLY": (date(2025, 6, 9), date(2025, 6, 10)),
        "IBKR": (date(2025, 6, 17), date(2025, 6, 18)),
        "NFLX": (date(2025, 11, 14), date(2025, 11, 17)),
    }
    for symbol, dates in split_dates.items():
        for session_date in dates:
            start, end = _session_window(schedule, session_date)
            for adjustment, segment, suffix in (
                (
                    AdjustmentState.UNADJUSTED,
                    Phase1BarSegment.SPLIT_BOUNDARY_UNADJUSTED,
                    "raw",
                ),
                (
                    AdjustmentState.SPLIT_ADJUSTED,
                    Phase1BarSegment.SPLIT_BOUNDARY_SPLIT_ADJUSTED,
                    "split",
                ),
            ):
                windows.append(
                    _window(
                        label=f"split-{symbol}-{session_date.isoformat()}-{suffix}",
                        segment=segment,
                        targets=(_SECURITY_BY_SYMBOL[symbol],),
                        timeframe=Timeframe.FIVE_MINUTES,
                        start=start,
                        end=end,
                        adjustment_state=adjustment,
                    )
                )

    windows.extend(
        (
            _window(
                label="ticker-continuity-sq",
                segment=Phase1BarSegment.TICKER_CONTINUITY,
                targets=(_temporal_security("SQ"),),
                timeframe=Timeframe.ONE_DAY,
                start=TICKER_CONTINUITY_START,
                end=TICKER_CHANGE_DATE,
                adjustment_state=AdjustmentState.UNADJUSTED,
            ),
            _window(
                label="ticker-continuity-xyz",
                segment=Phase1BarSegment.TICKER_CONTINUITY,
                targets=(_temporal_security("XYZ"),),
                timeframe=Timeframe.ONE_DAY,
                start=TICKER_CHANGE_DATE,
                end=TICKER_CONTINUITY_END,
                adjustment_state=AdjustmentState.UNADJUSTED,
            ),
        )
    )
    if len(windows) != 22:
        raise RuntimeError("frozen Phase 1 bar-window count changed")
    return tuple(windows)


def expected_keys_for_window(
    window: Phase1BarWindow,
    schedule: StaticSessionSchedule,
) -> tuple[ObservationKey, ...]:
    """Generate expected canonical starts without inferring any unregistered session."""

    keys: list[ObservationKey] = []
    for target in window.targets:
        for bounds in schedule.sessions:
            if window.timeframe is Timeframe.ONE_DAY:
                if window.start <= bounds.start < window.end and bounds.end <= window.end:
                    keys.append(
                        ObservationKey(target.instrument_id, window.timeframe, bounds.start)
                    )
                continue
            overlap_start = max(window.start, bounds.start)
            overlap_end = min(window.end, bounds.end)
            if overlap_end <= overlap_start:
                continue
            if (overlap_start - bounds.start) % timedelta(minutes=5):
                raise ValueError("5-minute window start is not aligned to the frozen session")
            current = overlap_start
            while current + timedelta(minutes=5) <= overlap_end:
                keys.append(ObservationKey(target.instrument_id, window.timeframe, current))
                current += timedelta(minutes=5)
    return tuple(sorted(keys))


def expected_keys_by_segment(
    windows: Sequence[Phase1BarWindow] | None = None,
    schedule: StaticSessionSchedule | None = None,
) -> Mapping[Phase1BarSegment, tuple[ObservationKey, ...]]:
    """Return disjoint comparison keys so adjustment states are never mixed."""

    schedule = schedule or build_phase1_session_schedule()
    windows = tuple(windows) if windows is not None else build_phase1_bar_windows(schedule)
    grouped: dict[Phase1BarSegment, list[ObservationKey]] = {
        segment: [] for segment in Phase1BarSegment
    }
    for window in windows:
        grouped[window.segment].extend(expected_keys_for_window(window, schedule))
    result: dict[Phase1BarSegment, tuple[ObservationKey, ...]] = {}
    for segment, values in grouped.items():
        if len(values) != len(set(values)):
            raise RuntimeError(f"frozen Phase 1 expected keys overlap in {segment.value}")
        result[segment] = tuple(sorted(values))
    return MappingProxyType(result)


def build_provider_bar_request_plan(
    provider: str,
    *,
    schedule: StaticSessionSchedule | None = None,
) -> tuple[PlannedBarRequest, ...]:
    """Batch frozen windows for Alpaca SIP or Twelve Data Basic without making calls."""

    normalized_provider = provider.strip().casefold().replace("-", "_")
    batch_sizes = {"alpaca": 16, "alpaca_sip": 16, "twelve_data": 8}
    try:
        batch_size = batch_sizes[normalized_provider]
    except KeyError as error:
        raise ValueError(f"unsupported Phase 1 bar provider: {provider!r}") from error
    schedule = schedule or build_phase1_session_schedule()
    planned: list[PlannedBarRequest] = []
    for window in build_phase1_bar_windows(schedule):
        for batch_number, offset in enumerate(range(0, len(window.targets), batch_size), start=1):
            targets = window.targets[offset : offset + batch_size]
            refs = tuple(
                ProviderInstrumentRef(
                    instrument_id=target.instrument_id,
                    provider_identifier=target.provider_identifier(normalized_provider),
                )
                for target in targets
            )
            request = BarRequest(
                instruments=refs,
                timeframe=window.timeframe,
                start=window.start,
                end=window.end,
                session=TradingSession.REGULAR,
                adjustment_state=window.adjustment_state,
            )
            planned.append(
                PlannedBarRequest(
                    label=f"{window.label}-batch-{batch_number}",
                    segment=window.segment,
                    request=request,
                    credit_cost=(len(refs) if normalized_provider == "twelve_data" else 1),
                )
            )
    return tuple(planned)


@dataclass(frozen=True, slots=True)
class Phase1RequestBudget:
    provider: str
    reference_requests: int
    canonical_bar_requests: int
    evidence_bar_requests: int
    corporate_action_requests: int
    preflight_requests: int
    estimated_http_requests: int
    estimated_credits: int | None


TWELVE_DATA_BASIC_BUDGET = Phase1RequestBudget(
    provider="twelve_data_basic",
    reference_requests=16,
    canonical_bar_requests=30,
    evidence_bar_requests=2,
    corporate_action_requests=0,
    preflight_requests=1,
    estimated_http_requests=49,
    estimated_credits=161,
)
ALPACA_SIP_BUDGET = Phase1RequestBudget(
    provider="alpaca_sip",
    reference_requests=16,
    canonical_bar_requests=22,
    evidence_bar_requests=2,
    corporate_action_requests=1,
    preflight_requests=0,
    estimated_http_requests=41,
    estimated_credits=None,
)


class PacingHook(Protocol):
    """One finite-study call hook; it does not retry failed provider requests."""

    def before_request(self, credit_cost: int) -> None: ...


class TwelveDataBasicPacer:
    """Bound only the approved 161-credit study to Basic's eight-credit minute."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        safety_seconds: float = 0.25,
    ) -> None:
        if not math.isfinite(safety_seconds) or safety_seconds < 0:
            raise ValueError("safety_seconds must be finite and non-negative")
        self._clock = clock
        self._sleeper = sleeper
        self._safety_seconds = safety_seconds
        self._events: deque[tuple[float, int]] = deque()
        self._credits_used = 0
        self._request_count = 0

    @property
    def credits_used(self) -> int:
        return self._credits_used

    @property
    def request_count(self) -> int:
        return self._request_count

    def before_request(self, credit_cost: int) -> None:
        if not 1 <= credit_cost <= 8:
            raise ValueError(
                "one Twelve Data Basic request must cost between one and eight credits"
            )
        approved_credits = TWELVE_DATA_BASIC_BUDGET.estimated_credits
        if approved_credits is None:
            raise RuntimeError("Twelve Data Basic credit budget is not configured")
        if self._credits_used + credit_cost > approved_credits:
            raise RuntimeError("Twelve Data request would exceed the approved 161-credit study")
        if self._request_count + 1 > TWELVE_DATA_BASIC_BUDGET.estimated_http_requests:
            raise RuntimeError("Twelve Data request would exceed the approved 49-request study")

        while True:
            now = self._clock()
            while self._events and now - self._events[0][0] >= 60:
                self._events.popleft()
            used_in_window = sum(cost for _, cost in self._events)
            if used_in_window + credit_cost <= 8:
                self._events.append((now, credit_cost))
                self._credits_used += credit_cost
                self._request_count += 1
                return
            wait_seconds = self._events[0][0] + 60 - now + self._safety_seconds
            self._sleeper(max(wait_seconds, self._safety_seconds))
            if self._clock() <= now:
                raise RuntimeError("pacing sleeper did not advance the monotonic clock")


def assert_external_private_data_root(data_root: Path, repository_root: Path) -> None:
    """Fail before writing when a live data root resolves inside the repository."""

    resolved_data_root = Path(data_root).resolve()
    resolved_repository_root = Path(repository_root).resolve()
    if resolved_data_root == resolved_repository_root or resolved_data_root.is_relative_to(
        resolved_repository_root
    ):
        raise ValueError("Phase 1 real-data root must be physically outside the repository")


@contextmanager
def phase1_temporary_data_root(
    repository_root: Path,
    *,
    parent_directory: Path | None = None,
) -> Iterator[Path]:
    """Yield an external private root and verify deletion when the run ends."""

    parent = Path(parent_directory) if parent_directory is not None else None
    with tempfile.TemporaryDirectory(prefix="ip-phase1-", dir=parent) as name:
        path = Path(name).resolve()
        assert_external_private_data_root(path, repository_root)
        yield path
    if path.exists():
        raise RuntimeError("Phase 1 temporary real-data root was not deleted")


class BarNormalizer(Protocol):
    def __call__(
        self,
        batch: RawBatch,
        request: BarRequest,
        *,
        ingested_at: datetime,
        session_schedule: StaticSessionSchedule,
        daily_semantics: DailyBarSemantics,
    ) -> BarNormalizationResult: ...


@dataclass(frozen=True, slots=True)
class SanitizedBatchPipelineMetrics:
    """Safe page-level facts; no values, credentials, URLs, IDs, hashes, or paths."""

    provider: str
    dataset: str
    page_number: int | None
    response_status: int | None
    latency_ms: float | None
    rate_limit_capacity: float | None
    rate_limit_remaining: float | None
    api_credits_used: float | None
    raw_size_bytes: int
    normalized_row_count: int
    normalization_issue_counts: tuple[tuple[str, int], ...]
    validation_flag_counts: tuple[tuple[str, int], ...]
    parquet_part_count: int
    duckdb_row_count: int
    raw_artifact_pass: bool
    checksum_replay_pass: bool
    normalization_pass: bool
    canonical_validation_pass: bool
    parquet_pass: bool
    duckdb_query_pass: bool


@dataclass(frozen=True, slots=True)
class ProcessedBarBatch:
    """Keep live values private while exposing only sanitized metrics in representations."""

    metrics: SanitizedBatchPipelineMetrics
    bars: tuple[PriceBar, ...] = field(repr=False)


@dataclass(frozen=True, slots=True)
class SanitizedProviderPipelineSummary:
    provider: str
    batch_count: int
    raw_size_bytes: int
    canonical_row_count: int
    parquet_part_count: int
    duckdb_row_count: int
    normalization_issue_counts: tuple[tuple[str, int], ...]
    validation_flag_counts: tuple[tuple[str, int], ...]
    latency_sample_count: int
    total_latency_ms: float
    maximum_latency_ms: float | None
    minimum_rate_limit_remaining: float | None
    api_credit_usage_sample_count: int
    total_api_credits_used: float
    pipeline_pass: bool


@dataclass(frozen=True, slots=True)
class SanitizedComparisonSummary:
    segment: str
    left_provider: str
    right_provider: str
    left_observation_count: int
    right_observation_count: int
    left_duplicate_count: int
    right_duplicate_count: int
    left_missing_expected_count: int | None
    right_missing_expected_count: int | None
    discrepancy_counts_by_metric: tuple[tuple[str, int], ...]
    discrepancy_counts_by_classification: tuple[tuple[str, int], ...]
    comparison_pass: bool


def _optional_number(value: object) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        rendered = float(value)
        return rendered if math.isfinite(rendered) and rendered >= 0 else None
    return None


def _optional_int(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


_FRAME_SORT_COLUMNS = (
    "instrument_id",
    "timeframe",
    "timestamp_start",
    "timestamp_end",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "currency",
    "session",
    "adjustment_state",
    "source_id",
    "raw_batch_id",
    "provider_record_id",
    "available_at",
    "retrieved_at",
    "ingested_at",
)


def _frames_match(left: pl.DataFrame, right: pl.DataFrame) -> bool:
    if left.schema != right.schema or left.height != right.height:
        return False
    return (
        left.sort(_FRAME_SORT_COLUMNS, nulls_last=True).to_dicts()
        == right.sort(_FRAME_SORT_COLUMNS, nulls_last=True).to_dicts()
    )


class EphemeralBarPipeline:
    """Exercise raw-to-DuckDB mechanics under one already-approved temporary root."""

    def __init__(self, data_root: Path, *, repository_root: Path) -> None:
        assert_external_private_data_root(data_root, repository_root)
        root = Path(data_root)
        self._raw_store = RawBatchStore(root / "raw")
        self._bar_store = ParquetBarStore(root / "analytical" / "price_bars")

    def process(
        self,
        batch: RawBatch,
        request: BarRequest,
        *,
        normalizer: BarNormalizer,
        ingested_at: datetime,
        session_schedule: StaticSessionSchedule,
        daily_semantics: DailyBarSemantics,
    ) -> ProcessedBarBatch:
        artifact = self._raw_store.write(batch)
        replayed = replay_raw_artifact(artifact)
        normalized = normalizer(
            replayed,
            request,
            ingested_at=ingested_at,
            session_schedule=session_schedule,
            daily_semantics=daily_semantics,
        )
        frame = price_bars_to_frame(normalized.bars)
        validated = validate_bars(frame)

        stored_paths: tuple[Path, ...] = ()
        if not validated.frame.is_empty():
            stored_paths = self._bar_store.append(validated.frame, batch.metadata.batch_id)
        queried = self._bar_store.query()
        queried_batch = queried.filter(pl.col("raw_batch_id") == str(batch.metadata.batch_id))
        if not _frames_match(validated.frame, queried_batch):
            raise RuntimeError("DuckDB replay does not match the validated canonical batch")

        replayed_bars = tuple(PriceBar.model_validate(row) for row in queried_batch.to_dicts())

        normalization_counts = Counter(issue.code.value for issue in normalized.issues)
        validation_counts = {
            flag.value: count for flag, count in validated.counts_by_flag.items() if count
        }
        metadata = batch.metadata.request_metadata
        metrics = SanitizedBatchPipelineMetrics(
            provider=batch.metadata.source.provider,
            dataset=batch.metadata.source.dataset,
            page_number=_optional_int(metadata.get("page_number")),
            response_status=_optional_int(metadata.get("response_status")),
            latency_ms=_optional_number(metadata.get("latency_ms")),
            rate_limit_capacity=_optional_number(metadata.get("rate_limit_capacity")),
            rate_limit_remaining=_optional_number(metadata.get("rate_limit_remaining")),
            api_credits_used=_optional_number(metadata.get("api_credits_used")),
            raw_size_bytes=artifact.size_bytes,
            normalized_row_count=validated.frame.height,
            normalization_issue_counts=tuple(sorted(normalization_counts.items())),
            validation_flag_counts=tuple(sorted(validation_counts.items())),
            parquet_part_count=len(stored_paths),
            duckdb_row_count=queried_batch.height,
            raw_artifact_pass=True,
            checksum_replay_pass=True,
            normalization_pass=not any(
                issue.severity is NormalizationSeverity.ERROR for issue in normalized.issues
            ),
            canonical_validation_pass=not any(
                issue.severity is QualitySeverity.ERROR for issue in validated.issues
            ),
            parquet_pass=validated.frame.is_empty() or bool(stored_paths),
            duckdb_query_pass=True,
        )
        return ProcessedBarBatch(metrics=metrics, bars=replayed_bars)

    def query(self) -> pl.DataFrame:
        """Return live canonical values only to the in-process comparison caller."""

        return self._bar_store.query()


def aggregate_pipeline_metrics(
    provider: str,
    batches: Sequence[SanitizedBatchPipelineMetrics],
) -> SanitizedProviderPipelineSummary:
    """Collapse page metrics without retaining individual observations or provenance IDs."""

    normalized_provider = provider.strip()
    if not normalized_provider:
        raise ValueError("provider must not be blank")
    if any(batch.provider.casefold() != normalized_provider.casefold() for batch in batches):
        raise ValueError("pipeline metrics contain a different provider")
    normalization_counts: Counter[str] = Counter()
    validation_counts: Counter[str] = Counter()
    latencies: list[float] = []
    remaining: list[float] = []
    api_credits_used: list[float] = []
    for batch in batches:
        normalization_counts.update(dict(batch.normalization_issue_counts))
        validation_counts.update(dict(batch.validation_flag_counts))
        if batch.latency_ms is not None:
            latencies.append(batch.latency_ms)
        if batch.rate_limit_remaining is not None:
            remaining.append(batch.rate_limit_remaining)
        if batch.api_credits_used is not None:
            api_credits_used.append(batch.api_credits_used)
    flags = (
        batch.raw_artifact_pass
        and batch.checksum_replay_pass
        and batch.normalization_pass
        and batch.canonical_validation_pass
        and batch.parquet_pass
        and batch.duckdb_query_pass
        for batch in batches
    )
    return SanitizedProviderPipelineSummary(
        provider=normalized_provider,
        batch_count=len(batches),
        raw_size_bytes=sum(batch.raw_size_bytes for batch in batches),
        canonical_row_count=sum(batch.normalized_row_count for batch in batches),
        parquet_part_count=sum(batch.parquet_part_count for batch in batches),
        duckdb_row_count=sum(batch.duckdb_row_count for batch in batches),
        normalization_issue_counts=tuple(sorted(normalization_counts.items())),
        validation_flag_counts=tuple(sorted(validation_counts.items())),
        latency_sample_count=len(latencies),
        total_latency_ms=sum(latencies),
        maximum_latency_ms=max(latencies, default=None),
        minimum_rate_limit_remaining=min(remaining, default=None),
        api_credit_usage_sample_count=len(api_credits_used),
        total_api_credits_used=sum(api_credits_used),
        pipeline_pass=bool(batches) and all(flags),
    )


def sanitize_comparison_report(
    segment: Phase1BarSegment | str,
    report: BarComparisonReport,
) -> SanitizedComparisonSummary:
    """Remove values and raw IDs while keeping reproducible discrepancy cardinalities."""

    metric_counts = Counter(finding.metric.value for finding in report.discrepancies)
    classification_counts = {
        classification.value: count
        for classification, count in report.counts_by_classification.items()
        if count
    }
    segment_value = segment.value if isinstance(segment, Phase1BarSegment) else segment.strip()
    if not segment_value:
        raise ValueError("comparison segment must not be blank")
    return SanitizedComparisonSummary(
        segment=segment_value,
        left_provider=report.left.provider,
        right_provider=report.right.provider,
        left_observation_count=report.left.observation_count,
        right_observation_count=report.right.observation_count,
        left_duplicate_count=report.left.duplicate_observation_count,
        right_duplicate_count=report.right.duplicate_observation_count,
        left_missing_expected_count=report.left.missing_expected_count,
        right_missing_expected_count=report.right.missing_expected_count,
        discrepancy_counts_by_metric=tuple(sorted(metric_counts.items())),
        discrepancy_counts_by_classification=tuple(sorted(classification_counts.items())),
        comparison_pass=True,
    )


__all__ = [
    "ALPACA_SIP_BUDGET",
    "CORPORATE_ACTION_END",
    "CORPORATE_ACTION_START",
    "DAILY_CORE_END",
    "DAILY_CORE_START",
    "KO_ADJUSTMENT_END",
    "KO_ADJUSTMENT_START",
    "PHASE1_CLOSED_SESSIONS",
    "PHASE1_EARLY_CLOSES",
    "PHASE1_SECURITIES",
    "TICKER_CHANGE_DATE",
    "TICKER_CONTINUITY_END",
    "TICKER_CONTINUITY_START",
    "TWELVE_DATA_BASIC_BUDGET",
    "EphemeralBarPipeline",
    "Phase1BarSegment",
    "Phase1BarWindow",
    "Phase1RequestBudget",
    "Phase1Security",
    "PlannedBarRequest",
    "ProcessedBarBatch",
    "SanitizedBatchPipelineMetrics",
    "SanitizedComparisonSummary",
    "SanitizedProviderPipelineSummary",
    "TwelveDataBasicPacer",
    "aggregate_pipeline_metrics",
    "assert_external_private_data_root",
    "build_phase1_bar_windows",
    "build_phase1_session_schedule",
    "build_provider_bar_request_plan",
    "expected_keys_by_segment",
    "expected_keys_for_window",
    "phase1_temporary_data_root",
    "sanitize_comparison_report",
]
