"""Opt-in live execution of the frozen Phase 1 bars-first provider bake-off.

The runner is deliberately finite, sequential, ephemeral, and credential-from-environment only.
It is not a scheduler, retry engine, watermark manager, or production ingestion workflow.
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from investment_platform.data.market_time import US_EASTERN
from investment_platform.data.models import AdjustmentState, CorporateAction, PriceBar, Timeframe
from investment_platform.data.normalization import (
    DailyBarSemantics,
    normalize_alpaca_bars,
    normalize_alpaca_corporate_actions,
    normalize_twelve_data_bars,
)
from investment_platform.data.normalization.common import read_provider_json, require_json_object
from investment_platform.data.phase1_bakeoff import (
    CORPORATE_ACTION_END,
    CORPORATE_ACTION_START,
    KO_ADJUSTMENT_END,
    KO_ADJUSTMENT_START,
    PHASE1_SECURITIES,
    TICKER_CONTINUITY_END,
    TICKER_CONTINUITY_START,
    EphemeralBarPipeline,
    Phase1BarSegment,
    PlannedBarRequest,
    SanitizedBatchPipelineMetrics,
    TwelveDataBasicPacer,
    aggregate_pipeline_metrics,
    build_phase1_session_schedule,
    build_provider_bar_request_plan,
    expected_keys_by_segment,
    phase1_temporary_data_root,
    sanitize_comparison_report,
)
from investment_platform.data.provenance import RawBatch
from investment_platform.data.providers import (
    AlpacaCredentials,
    AlpacaEvidenceAdjustment,
    AlpacaFeed,
    AlpacaProvider,
    BarRequest,
    CorporateActionRequest,
    ProviderError,
    ProviderInstrumentRef,
    TwelveDataCredentials,
    TwelveDataEvidenceAdjustment,
    TwelveDataProvider,
)
from investment_platform.data.quality import (
    ComparisonMetric,
    compare_provider_bars,
)
from investment_platform.data.storage import RawBatchStore, replay_raw_artifact

type ProgressEmitter = Callable[[Mapping[str, object]], None]
type PrivateOhlcSeries = dict[str, tuple[float, float, float, float]]


def _silent_progress(event: Mapping[str, object]) -> None:
    del event


def _json_progress(event: Mapping[str, object]) -> None:
    print(json.dumps({"kind": "progress", **event}, sort_keys=True), flush=True)


def _metadata_number(batch: RawBatch, key: str) -> float | int | None:
    value = batch.metadata.request_metadata.get(key)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    return None


def _required_non_negative_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RuntimeError(f"sanitized pipeline metric {field} is not a non-negative integer")
    return value


def _provider_float(value: object) -> float:
    if not isinstance(value, (str, int, float, Decimal)) or isinstance(value, bool):
        raise TypeError("provider value is not numeric")
    return float(value)


def _persist_aux_pages(
    pages: Iterable[RawBatch],
    store: RawBatchStore,
) -> Iterator[tuple[RawBatch, dict[str, object]]]:
    """Persist and replay each page before asking a provider generator for the next one."""

    for page in pages:
        artifact = store.write(page)
        replayed = replay_raw_artifact(artifact)
        yield (
            replayed,
            {
                "dataset": page.metadata.source.dataset,
                "status": _metadata_number(page, "response_status"),
                "latency_ms": _metadata_number(page, "latency_ms"),
                "rate_limit_remaining": _metadata_number(page, "rate_limit_remaining"),
                "api_credits_used": _metadata_number(page, "api_credits_used"),
                "raw_size_bytes": artifact.size_bytes,
                "raw_artifact": "PASS",
                "checksum_replay": "PASS",
            },
        )


def _is_alpaca_us_equity(root: Mapping[str, object], expected_symbol: str) -> bool:
    asset_class = root.get("asset_class", root.get("class"))
    return root.get("symbol") == expected_symbol and asset_class == "us_equity"


def _is_twelve_data_us_equity(value: Mapping[str, object], expected_symbol: str) -> bool:
    country = value.get("country")
    instrument_type = value.get("instrument_type")
    return (
        value.get("symbol") == expected_symbol
        and isinstance(country, str)
        and country.strip().casefold() in {"united states", "united states of america", "us", "usa"}
        and isinstance(instrument_type, str)
        and instrument_type.strip().casefold()
        in {"common stock", "preferred stock", "stock", "etf"}
    )


def _resolve_alpaca_references(
    provider: AlpacaProvider,
    store: RawBatchStore,
    emit: ProgressEmitter,
) -> dict[str, object]:
    resolved = 0
    pages = 0
    total_bytes = 0
    latencies: list[float] = []
    for index, security in enumerate(PHASE1_SECURITIES, start=1):
        matched = False
        for batch, metrics in _persist_aux_pages(
            provider.get_instrument(security.alpaca_identifier), store
        ):
            pages += 1
            total_bytes += _required_non_negative_int(metrics["raw_size_bytes"], "raw_size_bytes")
            latency = metrics["latency_ms"]
            if isinstance(latency, (int, float)):
                latencies.append(float(latency))
            root = require_json_object(
                read_provider_json(batch, provider="alpaca", dataset="instruments"),
                provider="alpaca",
                dataset="instruments",
            )
            matched = _is_alpaca_us_equity(root, security.alpaca_identifier)
            emit(
                {
                    "stage": "reference",
                    "provider": "alpaca_sip",
                    "completed": index,
                    "total": len(PHASE1_SECURITIES),
                    "exact_match": matched,
                    **metrics,
                }
            )
        resolved += int(matched)
    return {
        "requested": len(PHASE1_SECURITIES),
        "exactly_resolved": resolved,
        "response_pages": pages,
        "raw_size_bytes": total_bytes,
        "maximum_latency_ms": max(latencies, default=None),
    }


def _resolve_twelve_references(
    provider: TwelveDataProvider,
    store: RawBatchStore,
    pacer: TwelveDataBasicPacer,
    emit: ProgressEmitter,
) -> dict[str, object]:
    resolved = 0
    pages = 0
    total_bytes = 0
    latencies: list[float] = []
    for index, security in enumerate(PHASE1_SECURITIES, start=1):
        pacer.before_request(1)
        matched = False
        for batch, metrics in _persist_aux_pages(
            provider.get_instrument(security.twelve_data_identifier), store
        ):
            pages += 1
            total_bytes += _required_non_negative_int(metrics["raw_size_bytes"], "raw_size_bytes")
            latency = metrics["latency_ms"]
            if isinstance(latency, (int, float)):
                latencies.append(float(latency))
            root = require_json_object(
                read_provider_json(batch, provider="twelve_data", dataset="instruments"),
                provider="twelve_data",
                dataset="instruments",
            )
            values = root.get("data")
            if isinstance(values, list):
                matched = any(
                    isinstance(value, dict)
                    and _is_twelve_data_us_equity(value, security.twelve_data_identifier)
                    for value in values
                )
            emit(
                {
                    "stage": "reference",
                    "provider": "twelve_data_basic",
                    "completed": index,
                    "total": len(PHASE1_SECURITIES),
                    "exact_match": matched,
                    **metrics,
                }
            )
        resolved += int(matched)
    return {
        "requested": len(PHASE1_SECURITIES),
        "exactly_resolved": resolved,
        "response_pages": pages,
        "raw_size_bytes": total_bytes,
        "maximum_latency_ms": max(latencies, default=None),
    }


def _run_planned_bar_requests(
    *,
    provider_name: str,
    provider: AlpacaProvider | TwelveDataProvider,
    plan: Sequence[PlannedBarRequest],
    pipeline: EphemeralBarPipeline,
    schedule: Any,
    normalizer: Any,
    emit: ProgressEmitter,
    pacer: TwelveDataBasicPacer | None = None,
) -> tuple[
    dict[Phase1BarSegment, list[PriceBar]],
    list[SanitizedBatchPipelineMetrics],
]:
    grouped: dict[Phase1BarSegment, list[PriceBar]] = defaultdict(list)
    metrics: list[SanitizedBatchPipelineMetrics] = []
    for request_number, planned in enumerate(plan, start=1):
        if pacer is not None:
            pacer.before_request(planned.credit_cost)
        for processed in (
            pipeline.process(
                batch,
                planned.request,
                normalizer=normalizer,
                ingested_at=datetime.now(UTC),
                session_schedule=schedule,
                daily_semantics=DailyBarSemantics.REGULAR_SESSION,
            )
            for batch in provider.get_bars(planned.request)
        ):
            grouped[planned.segment].extend(processed.bars)
            metrics.append(processed.metrics)
            emit(
                {
                    "stage": "canonical_bars",
                    "provider": provider_name,
                    "request": request_number,
                    "request_total": len(plan),
                    "segment": planned.segment.value,
                    "rows": processed.metrics.normalized_row_count,
                    "normalization_issues": sum(
                        count for _, count in processed.metrics.normalization_issue_counts
                    ),
                    "validation_flags": sum(
                        count for _, count in processed.metrics.validation_flag_counts
                    ),
                    "latency_ms": processed.metrics.latency_ms,
                    "rate_limit_remaining": processed.metrics.rate_limit_remaining,
                    "api_credits_used": processed.metrics.api_credits_used,
                    "raw_size_bytes": processed.metrics.raw_size_bytes,
                }
            )
    return grouped, metrics


def _alpaca_ticker_continuity(
    provider: AlpacaProvider,
    pipeline: EphemeralBarPipeline,
    aux_store: RawBatchStore,
    schedule: Any,
    emit: ProgressEmitter,
) -> tuple[list[PriceBar], list[SanitizedBatchPipelineMetrics], dict[str, object]]:
    security = next(value for value in PHASE1_SECURITIES if value.symbol == "XYZ")
    request = BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=security.instrument_id,
                provider_identifier=security.alpaca_identifier,
            ),
        ),
        timeframe=Timeframe.ONE_DAY,
        start=TICKER_CONTINUITY_START,
        end=TICKER_CONTINUITY_END,
        adjustment_state=AdjustmentState.UNADJUSTED,
    )
    bars: list[PriceBar] = []
    metrics: list[SanitizedBatchPipelineMetrics] = []
    for batch in provider.get_bars_as_of(request, as_of=date(2025, 1, 29)):
        processed = pipeline.process(
            batch,
            request,
            normalizer=normalize_alpaca_bars,
            ingested_at=datetime.now(UTC),
            session_schedule=schedule,
            daily_semantics=DailyBarSemantics.REGULAR_SESSION,
        )
        bars.extend(processed.bars)
        metrics.append(processed.metrics)
        emit(
            {
                "stage": "ticker_continuity",
                "provider": "alpaca_sip",
                "mode": "mapped_canonical",
                "rows": len(processed.bars),
                "latency_ms": processed.metrics.latency_ms,
            }
        )

    negative_rows = 0
    negative_pages = 0
    for batch, page_metrics in _persist_aux_pages(provider.get_bars(request), aux_store):
        negative_pages += 1
        root = require_json_object(
            read_provider_json(batch, provider="alpaca", dataset="price_bars"),
            provider="alpaca",
            dataset="price_bars",
        )
        values = root.get("bars")
        if isinstance(values, dict):
            symbol_values = values.get(security.alpaca_identifier)
            if isinstance(symbol_values, list):
                negative_rows += len(symbol_values)
        emit(
            {
                "stage": "ticker_continuity",
                "provider": "alpaca_sip",
                "mode": "mapping_disabled_evidence",
                "rows": negative_rows,
                **page_metrics,
            }
        )
    return (
        bars,
        metrics,
        {
            "mapped_canonical_rows": len(bars),
            "mapping_disabled_rows": negative_rows,
            "mapping_disabled_pages": negative_pages,
        },
    )


def _private_ohlc_series(batch: RawBatch, provider: str, symbol: str) -> PrivateOhlcSeries:
    root = require_json_object(
        read_provider_json(batch, provider=provider, dataset="price_bars"),
        provider=provider,
        dataset="price_bars",
    )
    if provider == "alpaca":
        bars_value = root.get("bars")
        values = bars_value.get(symbol) if isinstance(bars_value, dict) else None
        timestamp_key = "t"
        field_names = ("o", "h", "l", "c")
    else:
        series = root
        if "values" not in series:
            nested = root.get(symbol)
            series = nested if isinstance(nested, dict) else {}
        values = series.get("values") if isinstance(series, dict) else None
        timestamp_key = "datetime"
        field_names = ("open", "high", "low", "close")
    result: PrivateOhlcSeries = {}
    if not isinstance(values, list):
        return result
    for value in values:
        if not isinstance(value, dict):
            continue
        raw_timestamp = value.get(timestamp_key)
        raw_fields = tuple(value.get(name) for name in field_names)
        if not isinstance(raw_timestamp, str):
            continue
        try:
            fields = tuple(_provider_float(field) for field in raw_fields)
        except (TypeError, ValueError):
            continue
        if len(fields) == 4:
            result[raw_timestamp[:10]] = (fields[0], fields[1], fields[2], fields[3])
    return result


def _canonical_ohlc_by_date(bars: Iterable[PriceBar], instrument_id: object) -> PrivateOhlcSeries:
    result: PrivateOhlcSeries = {}
    for bar in bars:
        if bar.instrument_id != instrument_id:
            continue
        local_date = bar.timestamp_start.astimezone(US_EASTERN).date()
        if KO_ADJUSTMENT_START.date() <= local_date < KO_ADJUSTMENT_END.date():
            result[local_date.isoformat()] = (bar.open, bar.high, bar.low, bar.close)
    return result


def _difference_count(left: PrivateOhlcSeries, right: PrivateOhlcSeries) -> tuple[int, int]:
    common = set(left) & set(right)
    return len(common), sum(left[key] != right[key] for key in common)


def _adjustment_evidence(
    *,
    alpaca: AlpacaProvider,
    twelve_data: TwelveDataProvider,
    store: RawBatchStore,
    pacer: TwelveDataBasicPacer,
    alpaca_daily_raw: Sequence[PriceBar],
    twelve_daily_raw: Sequence[PriceBar],
    emit: ProgressEmitter,
) -> dict[str, object]:
    security = next(value for value in PHASE1_SECURITIES if value.symbol == "KO")

    def request(provider_identifier: str) -> BarRequest:
        return BarRequest(
            instruments=(
                ProviderInstrumentRef(
                    instrument_id=security.instrument_id,
                    provider_identifier=provider_identifier,
                ),
            ),
            timeframe=Timeframe.ONE_DAY,
            start=KO_ADJUSTMENT_START,
            end=KO_ADJUSTMENT_END,
            adjustment_state=AdjustmentState.UNADJUSTED,
        )

    baselines = {
        "alpaca": _canonical_ohlc_by_date(alpaca_daily_raw, security.instrument_id),
        "twelve_data": _canonical_ohlc_by_date(twelve_daily_raw, security.instrument_id),
    }
    output: dict[str, object] = {}
    variants: tuple[
        tuple[
            str,
            str,
            Iterable[RawBatch],
        ],
        ...,
    ] = (
        (
            "alpaca",
            "dividend",
            alpaca.get_adjustment_evidence(
                request(security.alpaca_identifier),
                adjustment=AlpacaEvidenceAdjustment.DIVIDEND,
            ),
        ),
        (
            "alpaca",
            "all",
            alpaca.get_adjustment_evidence(
                request(security.alpaca_identifier),
                adjustment=AlpacaEvidenceAdjustment.ALL,
            ),
        ),
    )
    for provider_name, adjustment, pages in variants:
        series: PrivateOhlcSeries = {}
        page_count = 0
        for batch, page_metrics in _persist_aux_pages(pages, store):
            page_count += 1
            series.update(_private_ohlc_series(batch, provider_name, security.symbol))
            emit(
                {
                    "stage": "adjustment_evidence",
                    "provider": provider_name,
                    "adjustment": adjustment,
                    "row_count": len(series),
                    **page_metrics,
                }
            )
        common, differences = _difference_count(series, baselines[provider_name])
        output[f"{provider_name}_{adjustment}"] = {
            "pages": page_count,
            "rows": len(series),
            "rows_compared_to_unadjusted": common,
            "different_ohlc_rows_vs_unadjusted": differences,
            "canonical_persistence": False,
        }

    for adjustment in (
        TwelveDataEvidenceAdjustment.DIVIDENDS,
        TwelveDataEvidenceAdjustment.ALL,
    ):
        pacer.before_request(1)
        series = {}
        page_count = 0
        pages = twelve_data.get_adjustment_evidence(
            request(security.twelve_data_identifier),
            adjustment=adjustment,
        )
        for batch, page_metrics in _persist_aux_pages(pages, store):
            page_count += 1
            series.update(_private_ohlc_series(batch, "twelve_data", security.symbol))
            emit(
                {
                    "stage": "adjustment_evidence",
                    "provider": "twelve_data",
                    "adjustment": adjustment.value,
                    "row_count": len(series),
                    **page_metrics,
                }
            )
        common, differences = _difference_count(series, baselines["twelve_data"])
        output[f"twelve_data_{adjustment.value}"] = {
            "pages": page_count,
            "rows": len(series),
            "rows_compared_to_unadjusted": common,
            "different_ohlc_rows_vs_unadjusted": differences,
            "canonical_persistence": False,
        }
    return output


def _alpaca_corporate_actions(
    provider: AlpacaProvider,
    store: RawBatchStore,
    emit: ProgressEmitter,
) -> dict[str, object]:
    request = CorporateActionRequest(
        instruments=tuple(
            ProviderInstrumentRef(
                instrument_id=security.instrument_id,
                provider_identifier=security.alpaca_identifier,
            )
            for security in PHASE1_SECURITIES
        ),
        start=CORPORATE_ACTION_START,
        end=CORPORATE_ACTION_END,
    )
    actions: list[CorporateAction] = []
    issue_counts: Counter[str] = Counter()
    pages = 0
    raw_bytes = 0
    latencies: list[float] = []
    for batch, page_metrics in _persist_aux_pages(provider.get_corporate_actions(request), store):
        pages += 1
        raw_bytes += _required_non_negative_int(page_metrics["raw_size_bytes"], "raw_size_bytes")
        latency = page_metrics["latency_ms"]
        if isinstance(latency, (int, float)):
            latencies.append(float(latency))
        normalized = normalize_alpaca_corporate_actions(
            batch,
            request,
            ingested_at=datetime.now(UTC),
        )
        actions.extend(normalized.actions)
        issue_counts.update(issue.code.value for issue in normalized.issues)
        emit(
            {
                "stage": "corporate_actions",
                "provider": "alpaca",
                "page": pages,
                "canonical_actions": len(normalized.actions),
                "normalization_issues": len(normalized.issues),
                **page_metrics,
            }
        )
    action_counts = Counter(action.action_type for action in actions)
    return {
        "entitlement": "PASS",
        "query_date_basis": "process_date",
        "effective_date_completeness": "UNRESOLVED",
        "pages": pages,
        "raw_size_bytes": raw_bytes,
        "canonical_action_count": len(actions),
        "counts_by_type": tuple(sorted((str(key), value) for key, value in action_counts.items())),
        "normalization_issue_counts": tuple(sorted(issue_counts.items())),
        "maximum_latency_ms": max(latencies, default=None),
    }


def _numeric_difference_summary(report: Any) -> dict[str, object]:
    output: dict[str, object] = {}
    for metric in (
        ComparisonMetric.OPEN,
        ComparisonMetric.HIGH,
        ComparisonMetric.LOW,
        ComparisonMetric.CLOSE,
        ComparisonMetric.VOLUME,
    ):
        absolute: list[float] = []
        relative: list[float] = []
        for finding in report.discrepancies:
            if finding.metric is not metric:
                continue
            if not isinstance(finding.left_value, float) or not isinstance(
                finding.right_value, float
            ):
                continue
            difference = abs(finding.left_value - finding.right_value)
            absolute.append(difference)
            denominator = max(abs(finding.left_value), abs(finding.right_value))
            if denominator:
                relative.append(difference / denominator)
        output[metric.value] = {
            "numeric_difference_count": len(absolute),
            "mean_absolute_difference": (sum(absolute) / len(absolute) if absolute else None),
            "maximum_absolute_difference": max(absolute, default=None),
            "mean_relative_difference": (sum(relative) / len(relative) if relative else None),
            "maximum_relative_difference": max(relative, default=None),
        }
    return output


def _compare_segments(
    alpaca: Mapping[Phase1BarSegment, Sequence[PriceBar]],
    twelve_data: Mapping[Phase1BarSegment, Sequence[PriceBar]],
) -> tuple[list[dict[str, object]], bool]:
    expected = expected_keys_by_segment()
    output: list[dict[str, object]] = []
    all_pass = True
    for segment in Phase1BarSegment:
        intraday = segment in {
            Phase1BarSegment.CALENDAR_FIVE_MINUTE,
            Phase1BarSegment.SPLIT_BOUNDARY_UNADJUSTED,
            Phase1BarSegment.SPLIT_BOUNDARY_SPLIT_ADJUSTED,
        }
        report = compare_provider_bars(
            "alpaca_sip",
            alpaca.get(segment, ()),
            "twelve_data_basic",
            twelve_data.get(segment, ()),
            expected_keys=expected[segment],
            venue_feed_metrics=(ComparisonMetric.VOLUME,) if intraday else (),
            excluded_metrics=(ComparisonMetric.VWAP,),
        )
        sanitized = sanitize_comparison_report(segment, report)
        segment_pass = (
            sanitized.left_observation_count > 0
            and sanitized.right_observation_count > 0
            and sanitized.left_duplicate_count == 0
            and sanitized.right_duplicate_count == 0
        )
        all_pass = all_pass and segment_pass
        output.append(
            {
                **asdict(sanitized),
                "comparison_pass": segment_pass,
                "numeric_differences": _numeric_difference_summary(report),
                "vwap_comparison": "NOT_SEMANTICALLY_COMPARABLE",
                "volume_classification": (
                    "venue_feed_difference" if intraday else "unresolved_discrepancy"
                ),
                "ohlc_classification": "unresolved_discrepancy",
            }
        )
    return output, all_pass


def _calendar_counts(
    grouped: Mapping[Phase1BarSegment, Sequence[PriceBar]],
) -> tuple[tuple[str, int], ...]:
    counts: Counter[str] = Counter()
    for bar in grouped.get(Phase1BarSegment.CALENDAR_FIVE_MINUTE, ()):
        counts[bar.timestamp_start.astimezone(US_EASTERN).date().isoformat()] += 1
    return tuple(sorted(counts.items()))


def _adjustment_behavior(
    grouped: Mapping[Phase1BarSegment, Sequence[PriceBar]],
) -> dict[str, object]:
    def values(
        segment: Phase1BarSegment,
    ) -> dict[tuple[object, datetime], tuple[float | None, ...]]:
        return {
            (bar.instrument_id, bar.timestamp_start): (
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                float(bar.volume) if bar.volume is not None else None,
            )
            for bar in grouped.get(segment, ())
        }

    output: dict[str, object] = {}
    for label, raw_segment, adjusted_segment in (
        (
            "daily",
            Phase1BarSegment.DAILY_UNADJUSTED,
            Phase1BarSegment.DAILY_SPLIT_ADJUSTED,
        ),
        (
            "split_boundary_5m",
            Phase1BarSegment.SPLIT_BOUNDARY_UNADJUSTED,
            Phase1BarSegment.SPLIT_BOUNDARY_SPLIT_ADJUSTED,
        ),
    ):
        raw = values(raw_segment)
        adjusted = values(adjusted_segment)
        common = set(raw) & set(adjusted)
        output[label] = {
            "matched_rows": len(common),
            "different_rows": sum(raw[key] != adjusted[key] for key in common),
        }
    return output


def run_live_bakeoff(
    repository_root: Path,
    *,
    emit: ProgressEmitter = _silent_progress,
) -> dict[str, object]:
    """Run the approved study once and return only non-substitutive aggregate evidence."""

    alpaca = AlpacaProvider(
        AlpacaCredentials.from_environment(),
        feed=AlpacaFeed.SIP,
    )
    twelve_data = TwelveDataProvider(TwelveDataCredentials.from_environment())
    schedule = build_phase1_session_schedule()
    pacer = TwelveDataBasicPacer(safety_seconds=0.5)
    result: dict[str, object]

    with phase1_temporary_data_root(repository_root) as data_root:
        auxiliary_store = RawBatchStore(data_root / "auxiliary_raw")
        alpaca_references = _resolve_alpaca_references(alpaca, auxiliary_store, emit)
        twelve_references = _resolve_twelve_references(twelve_data, auxiliary_store, pacer, emit)

        alpaca_pipeline = EphemeralBarPipeline(
            data_root / "alpaca_pipeline", repository_root=repository_root
        )
        twelve_pipeline = EphemeralBarPipeline(
            data_root / "twelve_pipeline", repository_root=repository_root
        )
        alpaca_plan = tuple(
            planned
            for planned in build_provider_bar_request_plan("alpaca_sip", schedule=schedule)
            if planned.segment is not Phase1BarSegment.TICKER_CONTINUITY
        )
        twelve_plan = build_provider_bar_request_plan("twelve_data", schedule=schedule)
        alpaca_bars, alpaca_metrics = _run_planned_bar_requests(
            provider_name="alpaca_sip",
            provider=alpaca,
            plan=alpaca_plan,
            pipeline=alpaca_pipeline,
            schedule=schedule,
            normalizer=normalize_alpaca_bars,
            emit=emit,
        )
        ticker_bars, ticker_metrics, ticker_evidence = _alpaca_ticker_continuity(
            alpaca,
            alpaca_pipeline,
            auxiliary_store,
            schedule,
            emit,
        )
        alpaca_bars[Phase1BarSegment.TICKER_CONTINUITY].extend(ticker_bars)
        alpaca_metrics.extend(ticker_metrics)

        twelve_bars, twelve_metrics = _run_planned_bar_requests(
            provider_name="twelve_data_basic",
            provider=twelve_data,
            plan=twelve_plan,
            pipeline=twelve_pipeline,
            schedule=schedule,
            normalizer=normalize_twelve_data_bars,
            emit=emit,
            pacer=pacer,
        )
        comparisons, comparison_pass = _compare_segments(alpaca_bars, twelve_bars)
        actions = _alpaca_corporate_actions(alpaca, auxiliary_store, emit)
        adjustment_evidence = _adjustment_evidence(
            alpaca=alpaca,
            twelve_data=twelve_data,
            store=auxiliary_store,
            pacer=pacer,
            alpaca_daily_raw=alpaca_bars[Phase1BarSegment.DAILY_UNADJUSTED],
            twelve_daily_raw=twelve_bars[Phase1BarSegment.DAILY_UNADJUSTED],
            emit=emit,
        )

        alpaca_summary = aggregate_pipeline_metrics("alpaca", alpaca_metrics)
        twelve_summary = aggregate_pipeline_metrics("twelve_data", twelve_metrics)
        result = {
            "study_date": datetime.now(UTC).date().isoformat(),
            "sample_size": len(PHASE1_SECURITIES),
            "providers": {
                "alpaca_sip": {
                    "reference": alpaca_references,
                    "pipeline": asdict(alpaca_summary),
                    "calendar_rows_by_session": _calendar_counts(alpaca_bars),
                    "adjustment_behavior": _adjustment_behavior(alpaca_bars),
                    "ticker_continuity": ticker_evidence,
                },
                "twelve_data_basic": {
                    "reference": twelve_references,
                    "pipeline": asdict(twelve_summary),
                    "calendar_rows_by_session": _calendar_counts(twelve_bars),
                    "adjustment_behavior": _adjustment_behavior(twelve_bars),
                    "pacer_reserved_request_count": pacer.request_count,
                    "pacer_reserved_credit_cost": pacer.credits_used,
                },
            },
            "comparisons": comparisons,
            "comparison_pass": comparison_pass,
            "corporate_actions": {
                "alpaca": actions,
                "twelve_data_basic": {
                    "splits_endpoint": "UNAVAILABLE_ON_BASIC_DOCUMENTED",
                    "dividends_endpoint": "UNAVAILABLE_ON_BASIC_DOCUMENTED",
                    "ticker_change_history": "NO_EQUIVALENT_OFFICIAL_ENDPOINT_IDENTIFIED",
                    "empirically_tested": False,
                },
            },
            "adjustment_evidence": adjustment_evidence,
            "pipeline_acceptance": {
                "alpaca_reference": (
                    "PASS"
                    if alpaca_references["exactly_resolved"] == len(PHASE1_SECURITIES)
                    else "FAIL"
                ),
                "twelve_data_reference": (
                    "PASS"
                    if twelve_references["exactly_resolved"] == len(PHASE1_SECURITIES)
                    else "FAIL"
                ),
                "alpaca_sip": "PASS" if alpaca_summary.pipeline_pass else "FAIL",
                "twelve_data_basic": "PASS" if twelve_summary.pipeline_pass else "FAIL",
                "comparison": "PASS" if comparison_pass else "FAIL",
            },
            "retention_mode": "EPHEMERAL_PRIVATE",
        }
    result["cleanup"] = "PASS"
    return result


def main() -> int:
    try:
        result = run_live_bakeoff(Path.cwd(), emit=_json_progress)
    except ProviderError as error:
        print(
            json.dumps(
                {
                    "kind": "failure",
                    "provider": error.provider,
                    "dataset": error.dataset,
                    "error_type": type(error).__name__,
                    "message": error.safe_message,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    except Exception as error:
        print(
            json.dumps(
                {
                    "kind": "failure",
                    "error_type": type(error).__name__,
                    "message": "unexpected local bake-off failure; no exception details emitted",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps({"kind": "result", **result}, sort_keys=True))
    acceptance = result.get("pipeline_acceptance")
    if not isinstance(acceptance, dict) or any(value != "PASS" for value in acceptance.values()):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main", "run_live_bakeoff"]
