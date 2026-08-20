"""Normalize persisted Massive JSON pages without hiding provider-only semantics."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from investment_platform.data.market_time import US_EASTERN, to_utc
from investment_platform.data.models import (
    AdjustmentState,
    DividendAction,
    PriceBar,
    SplitAction,
    TickerChangeAction,
    Timeframe,
    TradingSession,
)
from investment_platform.data.normalization.common import (
    BarNormalizationResult,
    CorporateActionNormalizationResult,
    DailyBarSemantics,
    NormalizationError,
    NormalizationIssue,
    NormalizationIssueCode,
    NormalizationSeverity,
    StaticSessionSchedule,
    VendorJsonValue,
    read_provider_json,
    require_json_object,
    require_matching_request_metadata,
)
from investment_platform.data.provenance import RawBatch
from investment_platform.data.providers.base import (
    BarRequest,
    CorporateActionRequest,
    ProviderInstrumentRef,
    instrument_refs_fingerprint,
    instrument_refs_manifest,
)
from investment_platform.data.providers.massive import (
    MASSIVE_BAR_SOURCE,
    MASSIVE_DIVIDEND_SOURCE,
    MASSIVE_SPLIT_SOURCE,
    MASSIVE_TICKER_EVENT_SOURCE,
)

_PROVIDER = "massive"
PositiveDecimal = Annotated[Decimal, Field(gt=Decimal(0))]


class _VendorModel(BaseModel):
    model_config = ConfigDict(extra="ignore", allow_inf_nan=False)


class _MassiveBar(_VendorModel):
    t: int
    o: float
    h: float
    low: float = Field(alias="l")
    c: float
    v: float | None = None
    vw: float | None = None


class _MassiveSplit(_VendorModel):
    execution_date: date
    split_from: PositiveDecimal
    split_to: PositiveDecimal
    adjustment_type: str
    ticker: str
    id: str | None = None


class _MassiveDividend(_VendorModel):
    ex_dividend_date: date
    cash_amount: PositiveDecimal
    currency: str
    ticker: str
    id: str | None = None


class _TickerEvent(_VendorModel):
    date: date
    type: str
    ticker_change: dict[str, str]


def _issue(
    batch: RawBatch,
    *,
    dataset: str,
    code: NormalizationIssueCode,
    severity: NormalizationSeverity,
    message: str,
    record_index: int | None = None,
    provider_record_id: str | None = None,
) -> NormalizationIssue:
    return NormalizationIssue(
        provider=_PROVIDER,
        dataset=dataset,
        raw_batch_id=batch.metadata.batch_id,
        code=code,
        severity=severity,
        message=message,
        record_index=record_index,
        provider_record_id=provider_record_id,
    )


def _batch_instrument_ref(
    batch: RawBatch,
    instruments: tuple[ProviderInstrumentRef, ...],
) -> ProviderInstrumentRef:
    instrument_id = batch.metadata.request_metadata.get("instrument_id")
    provider_identifier = batch.metadata.request_metadata.get("provider_identifier")
    matches = [
        reference
        for reference in instruments
        if str(reference.instrument_id) == instrument_id
        and reference.provider_identifier == provider_identifier
    ]
    if len(matches) != 1:
        raise NormalizationError(
            "Massive raw metadata does not identify exactly one requested instrument reference"
        )
    return matches[0]


def _successful_results(batch: RawBatch, *, dataset: str) -> list[VendorJsonValue]:
    root = require_json_object(
        read_provider_json(batch, provider=_PROVIDER, dataset=dataset),
        provider=_PROVIDER,
        dataset=dataset,
    )
    status = root.get("status")
    if status != "OK":
        raise NormalizationError(f"{_PROVIDER} {dataset} response status was not OK")
    results = root.get("results", [])
    if not isinstance(results, list):
        raise NormalizationError(f"{_PROVIDER} {dataset} results must be an array")
    return results


def _bar_interval(
    *,
    timestamp_start: datetime,
    request: BarRequest,
    schedule: StaticSessionSchedule,
    daily_semantics: DailyBarSemantics,
) -> tuple[datetime, datetime, TradingSession] | NormalizationIssueCode:
    session_date = timestamp_start.astimezone(US_EASTERN).date()
    if request.timeframe is Timeframe.ONE_DAY:
        if daily_semantics is not DailyBarSemantics.REGULAR_SESSION:
            return NormalizationIssueCode.DAILY_SEMANTICS_UNVERIFIED
        bounds = schedule.bounds_for(session_date)
        if bounds is None:
            return NormalizationIssueCode.SESSION_BOUNDS_MISSING
        return bounds.start, bounds.end, TradingSession.REGULAR

    timestamp_end = timestamp_start + timedelta(minutes=5)
    if request.session is TradingSession.UNKNOWN:
        return timestamp_start, timestamp_end, TradingSession.UNKNOWN
    bounds = schedule.bounds_for(session_date)
    if bounds is None:
        return NormalizationIssueCode.SESSION_BOUNDS_MISSING
    if timestamp_start < bounds.start or timestamp_end > bounds.end:
        return NormalizationIssueCode.OUTSIDE_REQUESTED_SESSION
    return timestamp_start, timestamp_end, TradingSession.REGULAR


def normalize_massive_bars(
    batch: RawBatch,
    request: BarRequest,
    *,
    ingested_at: datetime,
    session_schedule: StaticSessionSchedule,
    daily_semantics: DailyBarSemantics = DailyBarSemantics.UNVERIFIED,
) -> BarNormalizationResult:
    """Map one persisted Massive aggregates page into canonical bars plus diagnostics."""

    if batch.metadata.source.provider != _PROVIDER:
        raise ValueError("raw batch is not a Massive response")
    if batch.metadata.source.source_id != MASSIVE_BAR_SOURCE.source_id:
        raise NormalizationError("Massive price_bars raw batch has an unexpected source identity")
    ingested_at = to_utc(ingested_at)
    require_matching_request_metadata(
        batch,
        {
            "timeframe": request.timeframe.value,
            "start": request.start.isoformat(),
            "end_exclusive": request.end.isoformat(),
            "session": request.session.value,
            "adjustment_state": request.adjustment_state.value,
            "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
            "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
        },
        provider=_PROVIDER,
        dataset="price_bars",
    )
    batch_reference = _batch_instrument_ref(batch, request.instruments)
    bars: list[PriceBar] = []
    issues: list[NormalizationIssue] = []

    root = require_json_object(
        read_provider_json(batch, provider=_PROVIDER, dataset="price_bars"),
        provider=_PROVIDER,
        dataset="price_bars",
    )
    status = root.get("status")
    if status != "OK":
        raise NormalizationError("Massive price_bars response status was not OK")
    adjusted = root.get("adjusted")
    if not isinstance(adjusted, bool):
        raise NormalizationError("Massive price_bars response must declare adjusted as a boolean")
    expected_adjusted = request.adjustment_state is AdjustmentState.SPLIT_ADJUSTED
    if (
        request.adjustment_state
        not in {
            AdjustmentState.UNADJUSTED,
            AdjustmentState.SPLIT_ADJUSTED,
        }
        or adjusted is not expected_adjusted
    ):
        raise NormalizationError(
            "Massive price_bars adjusted flag disagrees with the canonical request"
        )
    response_identifier = root.get("ticker")
    if response_identifier != batch_reference.provider_identifier:
        raise NormalizationError(
            "Massive price_bars ticker disagrees with the per-batch instrument reference"
        )
    results = root.get("results", [])
    if not isinstance(results, list):
        raise NormalizationError("Massive price_bars results must be an array")
    instrument_id = batch_reference.instrument_id

    for index, value in enumerate(results):
        try:
            record = _MassiveBar.model_validate(value)
            timestamp_start = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=record.t)
        except (OverflowError, ValidationError):
            issues.append(
                _issue(
                    batch,
                    dataset="price_bars",
                    code=NormalizationIssueCode.MALFORMED_RECORD,
                    severity=NormalizationSeverity.ERROR,
                    message="bar record is missing a required field or has an invalid value",
                    record_index=index,
                )
            )
            continue
        interval = _bar_interval(
            timestamp_start=timestamp_start,
            request=request,
            schedule=session_schedule,
            daily_semantics=daily_semantics,
        )
        if isinstance(interval, NormalizationIssueCode):
            messages = {
                NormalizationIssueCode.DAILY_SEMANTICS_UNVERIFIED: (
                    "daily provider semantics were not verified as a regular-session aggregate"
                ),
                NormalizationIssueCode.SESSION_BOUNDS_MISSING: (
                    "finite bake-off schedule has no bounds for the provider trading date"
                ),
                NormalizationIssueCode.OUTSIDE_REQUESTED_SESSION: (
                    "provider bar falls outside the registered regular session"
                ),
            }
            issues.append(
                _issue(
                    batch,
                    dataset="price_bars",
                    code=interval,
                    severity=NormalizationSeverity.WARNING,
                    message=messages[interval],
                    record_index=index,
                )
            )
            continue
        canonical_start, canonical_end, session = interval
        if canonical_start < request.start or canonical_end > request.end:
            issues.append(
                _issue(
                    batch,
                    dataset="price_bars",
                    code=NormalizationIssueCode.OUTSIDE_REQUESTED_INTERVAL,
                    severity=NormalizationSeverity.INFO,
                    message="snapped provider bar is not fully contained in the canonical interval",
                    record_index=index,
                )
            )
            continue
        bars.append(
            PriceBar(
                instrument_id=instrument_id,
                timeframe=request.timeframe,
                timestamp_start=canonical_start,
                timestamp_end=canonical_end,
                open=record.o,
                high=record.h,
                low=record.low,
                close=record.c,
                volume=record.v,
                vwap=record.vw,
                currency=None,
                session=session,
                adjustment_state=request.adjustment_state,
                source_id=batch.metadata.source.source_id,
                raw_batch_id=batch.metadata.batch_id,
                provider_record_id=None,
                available_at=None,
                retrieved_at=batch.metadata.retrieved_at,
                ingested_at=ingested_at,
            )
        )
    return BarNormalizationResult(bars=tuple(bars), issues=tuple(issues))


def _action_in_range(effective_date: date, request: CorporateActionRequest) -> bool:
    return request.start <= effective_date < request.end


def _normalize_splits(
    batch: RawBatch,
    request: CorporateActionRequest,
    ingested_at: datetime,
    batch_reference: ProviderInstrumentRef,
) -> CorporateActionNormalizationResult:
    actions: list[SplitAction] = []
    issues: list[NormalizationIssue] = []
    for index, value in enumerate(_successful_results(batch, dataset="corporate_actions")):
        try:
            record = _MassiveSplit.model_validate(value)
        except ValidationError:
            issues.append(
                _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.MALFORMED_RECORD,
                    severity=NormalizationSeverity.ERROR,
                    message="split record is missing required canonical fields",
                    record_index=index,
                )
            )
            continue
        if record.adjustment_type not in {"forward_split", "reverse_split"}:
            message = (
                "stock-dividend semantics cannot be collapsed into canonical split"
                if record.adjustment_type == "stock_dividend"
                else "provider split adjustment type is not represented canonically"
            )
            issues.append(
                _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.UNSUPPORTED_CORPORATE_ACTION,
                    severity=NormalizationSeverity.WARNING,
                    message=message,
                    record_index=index,
                    provider_record_id=record.id,
                )
            )
            continue
        if record.ticker != batch_reference.provider_identifier:
            issues.append(
                _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.UNMAPPED_INSTRUMENT,
                    severity=NormalizationSeverity.ERROR,
                    message="split ticker has no exact ProviderInstrumentRef mapping",
                    record_index=index,
                    provider_record_id=record.id,
                )
            )
            continue
        instrument_id = batch_reference.instrument_id
        if not _action_in_range(record.execution_date, request):
            issues.append(
                _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.OUTSIDE_REQUESTED_INTERVAL,
                    severity=NormalizationSeverity.INFO,
                    message="split effective date is outside the canonical request",
                    record_index=index,
                    provider_record_id=record.id,
                )
            )
            continue
        actions.append(
            SplitAction(
                instrument_id=instrument_id,
                effective_date=record.execution_date,
                split_ratio=record.split_to / record.split_from,
                source_id=batch.metadata.source.source_id,
                raw_batch_id=batch.metadata.batch_id,
                retrieved_at=batch.metadata.retrieved_at,
                ingested_at=ingested_at,
                available_at=None,
                provider_record_id=record.id,
            )
        )
    return CorporateActionNormalizationResult(tuple(actions), tuple(issues))


def _normalize_dividends(
    batch: RawBatch,
    request: CorporateActionRequest,
    ingested_at: datetime,
    batch_reference: ProviderInstrumentRef,
) -> CorporateActionNormalizationResult:
    actions: list[DividendAction] = []
    issues: list[NormalizationIssue] = []
    for index, value in enumerate(_successful_results(batch, dataset="corporate_actions")):
        try:
            record = _MassiveDividend.model_validate(value)
        except ValidationError:
            issues.append(
                _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.INCOMPLETE_CORPORATE_ACTION,
                    severity=NormalizationSeverity.ERROR,
                    message="dividend lacks ex-date, positive cash amount, currency, or ticker",
                    record_index=index,
                )
            )
            continue
        if record.ticker != batch_reference.provider_identifier:
            issues.append(
                _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.UNMAPPED_INSTRUMENT,
                    severity=NormalizationSeverity.ERROR,
                    message="dividend ticker has no exact ProviderInstrumentRef mapping",
                    record_index=index,
                    provider_record_id=record.id,
                )
            )
            continue
        instrument_id = batch_reference.instrument_id
        if not _action_in_range(record.ex_dividend_date, request):
            issues.append(
                _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.OUTSIDE_REQUESTED_INTERVAL,
                    severity=NormalizationSeverity.INFO,
                    message="dividend ex-date is outside the canonical request",
                    record_index=index,
                    provider_record_id=record.id,
                )
            )
            continue
        try:
            actions.append(
                DividendAction(
                    instrument_id=instrument_id,
                    effective_date=record.ex_dividend_date,
                    amount=record.cash_amount,
                    currency=record.currency,
                    source_id=batch.metadata.source.source_id,
                    raw_batch_id=batch.metadata.batch_id,
                    retrieved_at=batch.metadata.retrieved_at,
                    ingested_at=ingested_at,
                    available_at=None,
                    provider_record_id=record.id,
                )
            )
        except ValidationError:
            issues.append(
                _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.INCOMPLETE_CORPORATE_ACTION,
                    severity=NormalizationSeverity.ERROR,
                    message="dividend currency is not a canonical three-letter code",
                    record_index=index,
                    provider_record_id=record.id,
                )
            )
    return CorporateActionNormalizationResult(tuple(actions), tuple(issues))


def _normalize_ticker_events(
    batch: RawBatch,
    request: CorporateActionRequest,
    ingested_at: datetime,
    batch_reference: ProviderInstrumentRef,
) -> CorporateActionNormalizationResult:
    root = require_json_object(
        read_provider_json(batch, provider=_PROVIDER, dataset="corporate_actions"),
        provider=_PROVIDER,
        dataset="corporate_actions",
    )
    status = root.get("status")
    if status != "OK":
        raise NormalizationError("Massive ticker-event response status was not OK")
    result_value = root.get("results")
    if not isinstance(result_value, dict):
        raise NormalizationError("Massive ticker-event results must be an object")
    event_values = result_value.get("events", [])
    if not isinstance(event_values, list):
        raise NormalizationError("Massive ticker-event events must be an array")
    parsed_events: list[_TickerEvent] = []
    issues: list[NormalizationIssue] = []
    for index, value in enumerate(event_values):
        try:
            event = _TickerEvent.model_validate(value)
            if event.type != "ticker_change" or not event.ticker_change.get("ticker"):
                raise ValueError
        except (ValidationError, ValueError):
            issues.append(
                _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.MALFORMED_RECORD,
                    severity=NormalizationSeverity.ERROR,
                    message="ticker event is not a usable ticker_change record",
                    record_index=index,
                )
            )
            continue
        parsed_events.append(event)
    if not parsed_events:
        issues.append(
            _issue(
                batch,
                dataset="corporate_actions",
                code=NormalizationIssueCode.PROVIDER_DEFINITION,
                severity=NormalizationSeverity.INFO,
                message="ticker-event timeline contained no usable events",
            )
        )
        return CorporateActionNormalizationResult((), tuple(issues))
    timeline_tickers = {event.ticker_change["ticker"] for event in parsed_events}
    if batch_reference.provider_identifier not in timeline_tickers:
        raise NormalizationError(
            "Massive ticker-event timeline is not anchored to the per-batch identifier"
        )
    tickers_by_date: dict[date, set[str]] = {}
    for event in parsed_events:
        tickers_by_date.setdefault(event.date, set()).add(event.ticker_change["ticker"])
    if any(len(tickers) > 1 for tickers in tickers_by_date.values()):
        raise NormalizationError("Massive ticker-event timeline has an ambiguous same-date change")
    parsed_events.sort(key=lambda item: (item.date, item.ticker_change["ticker"]))
    actions: list[TickerChangeAction] = []
    for previous, current in pairwise(parsed_events):
        old_ticker = previous.ticker_change["ticker"]
        new_ticker = current.ticker_change["ticker"]
        instrument_id = batch_reference.instrument_id
        if old_ticker.casefold() == new_ticker.casefold() or not _action_in_range(
            current.date, request
        ):
            continue
        actions.append(
            TickerChangeAction(
                instrument_id=instrument_id,
                effective_date=current.date,
                old_ticker=old_ticker,
                new_ticker=new_ticker,
                source_id=batch.metadata.source.source_id,
                raw_batch_id=batch.metadata.batch_id,
                retrieved_at=batch.metadata.retrieved_at,
                ingested_at=ingested_at,
                available_at=None,
                provider_record_id=None,
            )
        )
        issues.append(
            _issue(
                batch,
                dataset="corporate_actions",
                code=NormalizationIssueCode.PROVIDER_DEFINITION,
                severity=NormalizationSeverity.INFO,
                message="ticker change was inferred from consecutive experimental timeline events",
            )
        )
    return CorporateActionNormalizationResult(tuple(actions), tuple(issues))


def normalize_massive_corporate_actions(
    batch: RawBatch,
    request: CorporateActionRequest,
    *,
    ingested_at: datetime,
) -> CorporateActionNormalizationResult:
    """Normalize the supported family identified by the raw batch source endpoint."""

    if batch.metadata.source.provider != _PROVIDER:
        raise ValueError("raw batch is not a Massive response")
    ingested_at = to_utc(ingested_at)
    require_matching_request_metadata(
        batch,
        {
            "start": request.start.isoformat(),
            "end_exclusive": request.end.isoformat(),
            "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
            "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
        },
        provider=_PROVIDER,
        dataset="corporate_actions",
    )
    batch_reference = _batch_instrument_ref(batch, request.instruments)
    endpoint = batch.metadata.source.logical_endpoint
    expected_sources = {
        "stocks/v1/splits": MASSIVE_SPLIT_SOURCE.source_id,
        "stocks/v1/dividends": MASSIVE_DIVIDEND_SOURCE.source_id,
        "vX/reference/tickers/events": MASSIVE_TICKER_EVENT_SOURCE.source_id,
    }
    if batch.metadata.source.source_id != expected_sources.get(endpoint):
        raise NormalizationError(
            "Massive corporate_actions raw batch has an unexpected source identity"
        )
    if endpoint == "stocks/v1/splits":
        return _normalize_splits(batch, request, ingested_at, batch_reference)
    if endpoint == "stocks/v1/dividends":
        return _normalize_dividends(batch, request, ingested_at, batch_reference)
    if endpoint == "vX/reference/tickers/events":
        return _normalize_ticker_events(batch, request, ingested_at, batch_reference)
    raise ValueError(f"unsupported Massive corporate-action endpoint {endpoint!r}")


__all__ = ["normalize_massive_bars", "normalize_massive_corporate_actions"]
