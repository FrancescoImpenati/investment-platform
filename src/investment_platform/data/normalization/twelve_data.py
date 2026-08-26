"""Normalize persisted Twelve Data bars without hiding feed or adjustment semantics."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError

from investment_platform.data.market_time import to_utc
from investment_platform.data.models import AdjustmentState, PriceBar, Timeframe, TradingSession
from investment_platform.data.normalization.common import (
    BarNormalizationResult,
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
    instrument_refs_fingerprint,
    instrument_refs_manifest,
)
from investment_platform.data.providers.twelve_data import (
    TWELVE_DATA_DAILY_BAR_SOURCE,
    TWELVE_DATA_INTRADAY_BAR_SOURCE,
)

_PROVIDER = "twelve_data"


class _VendorModel(BaseModel):
    model_config = ConfigDict(extra="ignore", allow_inf_nan=False)


class _TwelveDataMeta(_VendorModel):
    symbol: str
    interval: str
    currency: str | None = None
    exchange_timezone: str | None = None
    exchange: str | None = None
    mic_code: str | None = None
    type: str | None = None


class _TwelveDataBar(_VendorModel):
    datetime: str
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None


def _issue(
    batch: RawBatch,
    *,
    code: NormalizationIssueCode,
    severity: NormalizationSeverity,
    message: str,
    record_index: int | None = None,
) -> NormalizationIssue:
    return NormalizationIssue(
        provider=_PROVIDER,
        dataset="price_bars",
        raw_batch_id=batch.metadata.batch_id,
        code=code,
        severity=severity,
        message=message,
        record_index=record_index,
    )


def _wire_interval(timeframe: Timeframe) -> str:
    if timeframe is Timeframe.ONE_DAY:
        return "1day"
    if timeframe is Timeframe.FIVE_MINUTES:
        return "5min"
    raise NormalizationError(f"unsupported Twelve Data timeframe {timeframe.value}")


def _provider_adjustment(adjustment: AdjustmentState) -> str:
    if adjustment is AdjustmentState.UNADJUSTED:
        return "none"
    if adjustment is AdjustmentState.SPLIT_ADJUSTED:
        return "splits"
    raise NormalizationError(
        "Twelve Data price_bars request has no exact canonical provider adjustment"
    )


def _chunk_identifiers(batch: RawBatch) -> tuple[str, ...]:
    raw_manifest = batch.metadata.request_metadata.get("chunk_instrument_refs_manifest")
    if not isinstance(raw_manifest, str):
        raise NormalizationError("Twelve Data raw metadata lacks a chunk instrument manifest")
    try:
        parsed: Any = json.loads(raw_manifest)
    except json.JSONDecodeError as error:
        raise NormalizationError(
            "Twelve Data chunk instrument manifest is not valid JSON"
        ) from error
    if not isinstance(parsed, dict) or parsed.get("version") != 1:
        raise NormalizationError("Twelve Data chunk instrument manifest has an invalid version")
    references = parsed.get("references")
    if not isinstance(references, list):
        raise NormalizationError("Twelve Data chunk instrument manifest lacks references")
    identifiers: list[str] = []
    for reference in references:
        if not isinstance(reference, dict):
            raise NormalizationError("Twelve Data chunk instrument reference is malformed")
        identifier = reference.get("provider_identifier")
        if not isinstance(identifier, str) or not identifier:
            raise NormalizationError("Twelve Data chunk provider identifier is malformed")
        identifiers.append(identifier)
    if not identifiers or len(identifiers) != len(set(identifiers)):
        raise NormalizationError("Twelve Data chunk identifiers must be non-empty and unique")
    return tuple(identifiers)


def _series_by_symbol(
    root: dict[str, VendorJsonValue],
) -> tuple[dict[str, dict[str, VendorJsonValue]], bool]:
    """Return symbol-keyed series and whether the wire envelope was a single response."""

    if any(key in root for key in ("meta", "values", "status", "code", "message")):
        meta = root.get("meta")
        symbol = meta.get("symbol") if isinstance(meta, dict) else None
        if not isinstance(symbol, str) or not symbol:
            return {}, True
        return {symbol: root}, True

    series: dict[str, dict[str, VendorJsonValue]] = {}
    for symbol, value in root.items():
        if not isinstance(symbol, str) or not isinstance(value, dict):
            raise NormalizationError("Twelve Data batch entries must map symbols to objects")
        series[symbol] = value
    return series, False


def _timestamp_start(raw_value: str, timeframe: Timeframe) -> datetime | date:
    if timeframe is Timeframe.ONE_DAY:
        try:
            return date.fromisoformat(raw_value)
        except ValueError as error:
            raise ValueError("daily timestamp is not an exact ISO market date") from error
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("intraday timestamp is not ISO-compatible") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return to_utc(parsed)


def _bar_interval(
    *,
    timestamp: datetime | date,
    request: BarRequest,
    schedule: StaticSessionSchedule,
    daily_semantics: DailyBarSemantics,
) -> tuple[datetime, datetime, TradingSession] | NormalizationIssueCode:
    if request.timeframe is Timeframe.ONE_DAY:
        if not isinstance(timestamp, date) or isinstance(timestamp, datetime):
            return NormalizationIssueCode.MALFORMED_RECORD
        if daily_semantics is not DailyBarSemantics.REGULAR_SESSION:
            return NormalizationIssueCode.DAILY_SEMANTICS_UNVERIFIED
        bounds = schedule.bounds_for(timestamp)
        if bounds is None:
            return NormalizationIssueCode.SESSION_BOUNDS_MISSING
        return bounds.start, bounds.end, TradingSession.REGULAR

    if not isinstance(timestamp, datetime):
        return NormalizationIssueCode.MALFORMED_RECORD
    timestamp_end = timestamp + timedelta(minutes=5)
    if request.session is TradingSession.UNKNOWN:
        return timestamp, timestamp_end, TradingSession.UNKNOWN
    bounds = schedule.bounds_for(timestamp.date())
    if bounds is None:
        return NormalizationIssueCode.SESSION_BOUNDS_MISSING
    if timestamp < bounds.start or timestamp_end > bounds.end:
        return NormalizationIssueCode.OUTSIDE_REQUESTED_SESSION
    return timestamp, timestamp_end, TradingSession.REGULAR


def normalize_twelve_data_bars(
    batch: RawBatch,
    request: BarRequest,
    *,
    ingested_at: datetime,
    session_schedule: StaticSessionSchedule,
    daily_semantics: DailyBarSemantics = DailyBarSemantics.UNVERIFIED,
) -> BarNormalizationResult:
    """Map one persisted Twelve Data chunk while retaining rejected records in raw."""

    if batch.metadata.source.provider != _PROVIDER:
        raise ValueError("raw batch is not a Twelve Data response")
    expected_source = (
        TWELVE_DATA_DAILY_BAR_SOURCE
        if request.timeframe is Timeframe.ONE_DAY
        else TWELVE_DATA_INTRADAY_BAR_SOURCE
    )
    if batch.metadata.source.source_id != expected_source.source_id:
        raise NormalizationError(
            "Twelve Data price_bars raw batch has an unexpected source identity"
        )
    ingested_at = to_utc(ingested_at)
    expected_adjustment = _provider_adjustment(request.adjustment_state)
    require_matching_request_metadata(
        batch,
        {
            "timeframe": request.timeframe.value,
            "start": request.start.isoformat(),
            "end_exclusive": request.end.isoformat(),
            "session": request.session.value,
            "adjustment_state": request.adjustment_state.value,
            "provider_adjustment": expected_adjustment,
            "canonical_persistence_eligible": True,
            "requested_output_timezone": "UTC",
            "provider_time_basis": (
                "exchange_market_date" if request.timeframe is Timeframe.ONE_DAY else "UTC"
            ),
            "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
            "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
        },
        provider=_PROVIDER,
        dataset="price_bars",
    )
    refs = {
        reference.provider_identifier: reference.instrument_id for reference in request.instruments
    }
    chunk_identifiers = _chunk_identifiers(batch)
    if any(identifier not in refs for identifier in chunk_identifiers):
        raise NormalizationError("Twelve Data chunk metadata contains an unrequested instrument")

    root = require_json_object(
        read_provider_json(batch, provider=_PROVIDER, dataset="price_bars"),
        provider=_PROVIDER,
        dataset="price_bars",
    )
    series_by_symbol, single_response = _series_by_symbol(root)
    bars: list[PriceBar] = []
    issues: list[NormalizationIssue] = []
    if single_response and not series_by_symbol:
        issues.append(
            _issue(
                batch,
                code=NormalizationIssueCode.PROVIDER_RESPONSE_ERROR,
                severity=NormalizationSeverity.ERROR,
                message="single-symbol response lacks a usable meta.symbol",
            )
        )
    for identifier in chunk_identifiers:
        if identifier not in series_by_symbol:
            issues.append(
                _issue(
                    batch,
                    code=NormalizationIssueCode.PROVIDER_RESPONSE_ERROR,
                    severity=NormalizationSeverity.ERROR,
                    message="requested symbol is absent from the provider response envelope",
                )
            )

    expected_interval = _wire_interval(request.timeframe)
    record_index = 0
    for envelope_symbol, value in series_by_symbol.items():
        instrument_id: UUID | None = refs.get(envelope_symbol)
        if instrument_id is None or envelope_symbol not in chunk_identifiers:
            issues.append(
                _issue(
                    batch,
                    code=NormalizationIssueCode.UNMAPPED_INSTRUMENT,
                    severity=NormalizationSeverity.ERROR,
                    message="response symbol has no exact requested chunk mapping",
                )
            )
            continue
        status = value.get("status")
        if status != "ok":
            issues.append(
                _issue(
                    batch,
                    code=NormalizationIssueCode.PROVIDER_RESPONSE_ERROR,
                    severity=NormalizationSeverity.ERROR,
                    message="provider returned a non-success status for the requested symbol",
                )
            )
            continue
        meta_value = value.get("meta")
        values = value.get("values")
        if not isinstance(meta_value, dict) or not isinstance(values, list):
            issues.append(
                _issue(
                    batch,
                    code=NormalizationIssueCode.PROVIDER_RESPONSE_ERROR,
                    severity=NormalizationSeverity.ERROR,
                    message="successful series lacks a meta object or values array",
                )
            )
            continue
        try:
            meta = _TwelveDataMeta.model_validate(meta_value)
        except ValidationError:
            issues.append(
                _issue(
                    batch,
                    code=NormalizationIssueCode.PROVIDER_RESPONSE_ERROR,
                    severity=NormalizationSeverity.ERROR,
                    message="series metadata lacks required symbol or interval fields",
                )
            )
            continue
        if meta.symbol != envelope_symbol or meta.interval != expected_interval:
            issues.append(
                _issue(
                    batch,
                    code=NormalizationIssueCode.PROVIDER_RESPONSE_ERROR,
                    severity=NormalizationSeverity.ERROR,
                    message="series metadata disagrees with its symbol key or requested interval",
                )
            )
            continue
        currency = meta.currency.strip() if meta.currency is not None else None
        if currency == "":
            currency = None
        for raw_record in values:
            current_index = record_index
            record_index += 1
            try:
                record = _TwelveDataBar.model_validate(raw_record)
                timestamp = _timestamp_start(record.datetime, request.timeframe)
            except (ValidationError, ValueError):
                issues.append(
                    _issue(
                        batch,
                        code=NormalizationIssueCode.MALFORMED_RECORD,
                        severity=NormalizationSeverity.ERROR,
                        message="bar record is missing valid OHLCV or timestamp fields",
                        record_index=current_index,
                    )
                )
                continue
            interval = _bar_interval(
                timestamp=timestamp,
                request=request,
                schedule=session_schedule,
                daily_semantics=daily_semantics,
            )
            if isinstance(interval, NormalizationIssueCode):
                messages = {
                    NormalizationIssueCode.MALFORMED_RECORD: "bar timestamp type is invalid",
                    NormalizationIssueCode.DAILY_SEMANTICS_UNVERIFIED: (
                        "daily Twelve Data aggregation was not verified as regular-session only"
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
                        code=NormalizationIssueCode.OUTSIDE_REQUESTED_INTERVAL,
                        severity=NormalizationSeverity.INFO,
                        message="provider response falls outside the canonical half-open interval",
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
                    open=record.open,
                    high=record.high,
                    low=record.low,
                    close=record.close,
                    volume=record.volume,
                    vwap=None,
                    currency=currency,
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


__all__ = ["normalize_twelve_data_bars"]
