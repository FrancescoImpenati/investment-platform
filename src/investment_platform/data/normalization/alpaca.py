"""Normalize persisted Alpaca pages with explicit feed and date-basis diagnostics."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Annotated
from uuid import UUID

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
from investment_platform.data.providers.alpaca import (
    ALPACA_CORPORATE_ACTION_SOURCE,
    ALPACA_IEX_BAR_SOURCE,
    ALPACA_SIP_BAR_SOURCE,
)
from investment_platform.data.providers.base import (
    BarRequest,
    CorporateActionRequest,
    instrument_refs_fingerprint,
    instrument_refs_manifest,
)

_PROVIDER = "alpaca"
PositiveDecimal = Annotated[Decimal, Field(gt=Decimal(0))]


class _VendorModel(BaseModel):
    model_config = ConfigDict(extra="ignore", allow_inf_nan=False)


class _AlpacaBar(_VendorModel):
    t: datetime
    o: float
    h: float
    low: float = Field(alias="l")
    c: float
    v: float | None = None
    vw: float | None = None


class _AlpacaSplit(_VendorModel):
    symbol: str
    new_rate: PositiveDecimal
    old_rate: PositiveDecimal
    ex_date: date
    id: str | None = None


class _AlpacaCashDividend(_VendorModel):
    symbol: str
    rate: PositiveDecimal
    ex_date: date
    currency: str | None = None
    id: str | None = None


class _AlpacaNameChange(_VendorModel):
    old_symbol: str
    new_symbol: str
    process_date: date
    effective_date: date | None = None
    id: str | None = None


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


def normalize_alpaca_bars(
    batch: RawBatch,
    request: BarRequest,
    *,
    ingested_at: datetime,
    session_schedule: StaticSessionSchedule,
    daily_semantics: DailyBarSemantics = DailyBarSemantics.UNVERIFIED,
) -> BarNormalizationResult:
    """Map one persisted Alpaca bars page while retaining every rejected record in raw."""

    if batch.metadata.source.provider != _PROVIDER:
        raise ValueError("raw batch is not an Alpaca response")
    if batch.metadata.source.source_id not in {
        ALPACA_SIP_BAR_SOURCE.source_id,
        ALPACA_IEX_BAR_SOURCE.source_id,
    }:
        raise NormalizationError("Alpaca price_bars raw batch has an unexpected source identity")
    expected_feed = (
        "sip" if batch.metadata.source.source_id == ALPACA_SIP_BAR_SOURCE.source_id else "iex"
    )
    provider_adjustments = {
        AdjustmentState.UNADJUSTED: "raw",
        AdjustmentState.SPLIT_ADJUSTED: "split",
    }
    expected_provider_adjustment = provider_adjustments.get(request.adjustment_state)
    if expected_provider_adjustment is None:
        raise NormalizationError(
            "Alpaca price_bars request has no exact canonical provider adjustment"
        )
    ingested_at = to_utc(ingested_at)
    require_matching_request_metadata(
        batch,
        {
            "timeframe": request.timeframe.value,
            "start": request.start.isoformat(),
            "end_exclusive": request.end.isoformat(),
            "session": request.session.value,
            "adjustment_state": request.adjustment_state.value,
            "provider_adjustment": expected_provider_adjustment,
            "canonical_persistence_eligible": True,
            "feed": expected_feed,
            "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
            "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
        },
        provider=_PROVIDER,
        dataset="price_bars",
    )
    refs = {
        reference.provider_identifier: reference.instrument_id for reference in request.instruments
    }
    root = require_json_object(
        read_provider_json(batch, provider=_PROVIDER, dataset="price_bars"),
        provider=_PROVIDER,
        dataset="price_bars",
    )
    bars_value = root.get("bars")
    if not isinstance(bars_value, dict):
        raise NormalizationError("Alpaca price_bars bars must be an object")
    bars: list[PriceBar] = []
    issues: list[NormalizationIssue] = []
    record_index = 0
    for identifier, values in bars_value.items():
        if not isinstance(identifier, str) or not isinstance(values, list):
            raise NormalizationError("Alpaca bars entries must map symbols to arrays")
        instrument_id = refs.get(identifier)
        for value in values:
            current_index = record_index
            record_index += 1
            if instrument_id is None:
                issues.append(
                    _issue(
                        batch,
                        dataset="price_bars",
                        code=NormalizationIssueCode.UNMAPPED_INSTRUMENT,
                        severity=NormalizationSeverity.ERROR,
                        message="bar symbol has no exact ProviderInstrumentRef mapping",
                        record_index=current_index,
                    )
                )
                continue
            try:
                record = _AlpacaBar.model_validate(value)
                timestamp_start = to_utc(record.t)
            except (ValidationError, ValueError):
                issues.append(
                    _issue(
                        batch,
                        dataset="price_bars",
                        code=NormalizationIssueCode.MALFORMED_RECORD,
                        severity=NormalizationSeverity.ERROR,
                        message="bar record is missing a required field or aware timestamp",
                        record_index=current_index,
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
                        "daily Alpaca aggregation was not verified as regular-session only"
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
                        record_index=current_index,
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
                        message="inclusive provider response falls outside the canonical interval",
                        record_index=current_index,
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
                    currency="USD",
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
    return BarNormalizationResult(tuple(bars), tuple(issues))


def _instrument_for_symbol(
    symbol: str,
    request: CorporateActionRequest,
) -> UUID | None:
    return next(
        (
            reference.instrument_id
            for reference in request.instruments
            if reference.provider_identifier == symbol
        ),
        None,
    )


def _action_in_range(effective_date: date, request: CorporateActionRequest) -> bool:
    return request.start <= effective_date < request.end


def _supported_action(
    *,
    family: str,
    value: VendorJsonValue,
    batch: RawBatch,
    request: CorporateActionRequest,
    ingested_at: datetime,
    record_index: int,
) -> tuple[SplitAction | DividendAction | TickerChangeAction | None, NormalizationIssue | None]:
    try:
        if family in {"forward_splits", "reverse_splits"}:
            split = _AlpacaSplit.model_validate(value)
            instrument_id = _instrument_for_symbol(split.symbol, request)
            if instrument_id is None:
                return None, _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.UNMAPPED_INSTRUMENT,
                    severity=NormalizationSeverity.ERROR,
                    message="split symbol has no exact ProviderInstrumentRef mapping",
                    record_index=record_index,
                    provider_record_id=split.id,
                )
            if not _action_in_range(split.ex_date, request):
                return None, _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.OUTSIDE_REQUESTED_INTERVAL,
                    severity=NormalizationSeverity.INFO,
                    message="split ex-date is outside the canonical request",
                    record_index=record_index,
                    provider_record_id=split.id,
                )
            return (
                SplitAction(
                    instrument_id=instrument_id,
                    effective_date=split.ex_date,
                    split_ratio=split.new_rate / split.old_rate,
                    source_id=batch.metadata.source.source_id,
                    raw_batch_id=batch.metadata.batch_id,
                    retrieved_at=batch.metadata.retrieved_at,
                    ingested_at=ingested_at,
                    available_at=None,
                    provider_record_id=split.id,
                ),
                None,
            )
        if family == "cash_dividends":
            dividend = _AlpacaCashDividend.model_validate(value)
            instrument_id = _instrument_for_symbol(dividend.symbol, request)
            if instrument_id is None:
                return None, _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.UNMAPPED_INSTRUMENT,
                    severity=NormalizationSeverity.ERROR,
                    message="dividend symbol has no exact ProviderInstrumentRef mapping",
                    record_index=record_index,
                    provider_record_id=dividend.id,
                )
            if dividend.currency is None:
                return None, _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.INCOMPLETE_CORPORATE_ACTION,
                    severity=NormalizationSeverity.WARNING,
                    message="dividend currency is absent and must not be inferred as USD",
                    record_index=record_index,
                    provider_record_id=dividend.id,
                )
            if not _action_in_range(dividend.ex_date, request):
                return None, _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.OUTSIDE_REQUESTED_INTERVAL,
                    severity=NormalizationSeverity.INFO,
                    message="dividend ex-date is outside the canonical request",
                    record_index=record_index,
                    provider_record_id=dividend.id,
                )
            return (
                DividendAction(
                    instrument_id=instrument_id,
                    effective_date=dividend.ex_date,
                    amount=dividend.rate,
                    currency=dividend.currency,
                    source_id=batch.metadata.source.source_id,
                    raw_batch_id=batch.metadata.batch_id,
                    retrieved_at=batch.metadata.retrieved_at,
                    ingested_at=ingested_at,
                    available_at=None,
                    provider_record_id=dividend.id,
                ),
                None,
            )
        if family == "name_changes":
            change = _AlpacaNameChange.model_validate(value)
            if change.effective_date is None:
                return None, _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.INCOMPLETE_CORPORATE_ACTION,
                    severity=NormalizationSeverity.WARNING,
                    message="name change exposes process_date but no reliable effective date",
                    record_index=record_index,
                    provider_record_id=change.id,
                )
            instrument_id = _instrument_for_symbol(
                change.new_symbol, request
            ) or _instrument_for_symbol(change.old_symbol, request)
            if instrument_id is None:
                return None, _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.UNMAPPED_INSTRUMENT,
                    severity=NormalizationSeverity.ERROR,
                    message="name-change symbols have no ProviderInstrumentRef mapping",
                    record_index=record_index,
                    provider_record_id=change.id,
                )
            if not _action_in_range(change.effective_date, request):
                return None, _issue(
                    batch,
                    dataset="corporate_actions",
                    code=NormalizationIssueCode.OUTSIDE_REQUESTED_INTERVAL,
                    severity=NormalizationSeverity.INFO,
                    message="ticker-change effective date is outside the canonical request",
                    record_index=record_index,
                    provider_record_id=change.id,
                )
            return (
                TickerChangeAction(
                    instrument_id=instrument_id,
                    effective_date=change.effective_date,
                    old_ticker=change.old_symbol,
                    new_ticker=change.new_symbol,
                    source_id=batch.metadata.source.source_id,
                    raw_batch_id=batch.metadata.batch_id,
                    retrieved_at=batch.metadata.retrieved_at,
                    ingested_at=ingested_at,
                    available_at=None,
                    provider_record_id=change.id,
                ),
                None,
            )
    except ValidationError:
        return None, _issue(
            batch,
            dataset="corporate_actions",
            code=NormalizationIssueCode.INCOMPLETE_CORPORATE_ACTION,
            severity=NormalizationSeverity.ERROR,
            message="corporate-action record lacks required canonical fields",
            record_index=record_index,
        )
    return None, _issue(
        batch,
        dataset="corporate_actions",
        code=NormalizationIssueCode.UNSUPPORTED_CORPORATE_ACTION,
        severity=NormalizationSeverity.INFO,
        message=f"Alpaca action family {family!r} is not represented canonically",
        record_index=record_index,
    )


def normalize_alpaca_corporate_actions(
    batch: RawBatch,
    request: CorporateActionRequest,
    *,
    ingested_at: datetime,
) -> CorporateActionNormalizationResult:
    """Normalize representable actions and report the process-date completeness limitation."""

    if batch.metadata.source.provider != _PROVIDER:
        raise ValueError("raw batch is not an Alpaca response")
    if batch.metadata.source.source_id != ALPACA_CORPORATE_ACTION_SOURCE.source_id:
        raise NormalizationError(
            "Alpaca corporate_actions raw batch has an unexpected source identity"
        )
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
    root = require_json_object(
        read_provider_json(batch, provider=_PROVIDER, dataset="corporate_actions"),
        provider=_PROVIDER,
        dataset="corporate_actions",
    )
    action_value = root.get("corporate_actions", root)
    if not isinstance(action_value, dict):
        raise NormalizationError("Alpaca corporate_actions must be an object")
    actions: list[SplitAction | DividendAction | TickerChangeAction] = []
    issues: list[NormalizationIssue] = [
        _issue(
            batch,
            dataset="corporate_actions",
            code=NormalizationIssueCode.CORPORATE_ACTION_DATE_BASIS,
            severity=NormalizationSeverity.WARNING,
            message=(
                "provider query is bounded by process_date, so effective-date completeness is "
                "not guaranteed"
            ),
        )
    ]
    record_index = 0
    ignored_top_level = {"next_page_token", "corporate_actions"}
    for family, values in action_value.items():
        if family in ignored_top_level:
            continue
        if not isinstance(family, str) or not isinstance(values, list):
            raise NormalizationError("Alpaca corporate-action families must be arrays")
        for value in values:
            action, issue = _supported_action(
                family=family,
                value=value,
                batch=batch,
                request=request,
                ingested_at=ingested_at,
                record_index=record_index,
            )
            record_index += 1
            if action is not None:
                actions.append(action)
            if issue is not None:
                issues.append(issue)
    return CorporateActionNormalizationResult(tuple(actions), tuple(issues))


__all__ = ["normalize_alpaca_bars", "normalize_alpaca_corporate_actions"]
