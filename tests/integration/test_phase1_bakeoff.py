"""Offline proof that the Phase 1 runner exercises the complete ephemeral pipeline."""

from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pytest

from investment_platform.data.models import (
    AdjustmentState,
    BarQualityFlag,
    Timeframe,
    TradingSession,
)
from investment_platform.data.normalization import (
    BarNormalizationResult,
    DailyBarSemantics,
    NormalizationIssue,
    NormalizationIssueCode,
    NormalizationSeverity,
    SessionBounds,
    StaticSessionSchedule,
    normalize_alpaca_bars,
)
from investment_platform.data.phase1_bakeoff import (
    EphemeralBarPipeline,
    aggregate_pipeline_metrics,
    phase1_temporary_data_root,
)
from investment_platform.data.provenance import RawBatch
from investment_platform.data.providers import (
    AlpacaCredentials,
    AlpacaProvider,
    BarRequest,
    ProviderInstrumentRef,
)
from investment_platform.data.providers.http import HttpResponse
from tests.provider_fakes import QueueHttpTransport

pytestmark = pytest.mark.integration

_FIXTURE = (
    Path(__file__).parents[1] / "fixtures" / "providers" / "alpaca" / "bars_5m_sip_page_2.json"
)
_INSTRUMENT_ID = UUID("10000000-0000-4000-8000-000000000001")
_BATCH_ID = UUID("30000000-0000-4000-8000-000000000099")
_RETRIEVED_AT = datetime(2026, 8, 24, 12, tzinfo=UTC)


def test_ephemeral_runner_raw_replay_normalize_validate_parquet_duckdb_cleanup(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    provider = AlpacaProvider(
        AlpacaCredentials("synthetic-id", "synthetic-secret"),
        transport=QueueHttpTransport([HttpResponse(200, _FIXTURE.read_bytes(), elapsed_ms=7.5)]),
        clock=lambda: _RETRIEVED_AT,
        batch_id_factory=lambda: _BATCH_ID,
    )
    request = BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="XPH1",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        end=datetime(2025, 7, 2, 20, tzinfo=UTC),
        session=TradingSession.REGULAR,
        adjustment_state=AdjustmentState.UNADJUSTED,
    )
    schedule = StaticSessionSchedule(
        (
            SessionBounds(
                session_date=date(2025, 7, 2),
                start=request.start,
                end=request.end,
                source="synthetic finite runner schedule",
            ),
        )
    )
    (batch,) = tuple(provider.get_bars(request))

    with phase1_temporary_data_root(repository) as data_root:
        pipeline = EphemeralBarPipeline(data_root, repository_root=repository)
        processed = pipeline.process(
            batch,
            request,
            normalizer=normalize_alpaca_bars,
            ingested_at=_RETRIEVED_AT,
            session_schedule=schedule,
            daily_semantics=DailyBarSemantics.UNVERIFIED,
        )
        queried = pipeline.query()
        summary = aggregate_pipeline_metrics("alpaca", (processed.metrics,))
        retained_path = data_root

        assert len(processed.bars) == 1
        assert processed.metrics.raw_size_bytes == len(_FIXTURE.read_bytes())
        assert processed.metrics.raw_artifact_pass is True
        assert processed.metrics.checksum_replay_pass is True
        assert processed.metrics.normalization_pass is True
        assert processed.metrics.canonical_validation_pass is True
        assert processed.metrics.parquet_pass is True
        assert processed.metrics.duckdb_query_pass is True
        assert processed.metrics.normalized_row_count == 1
        assert processed.metrics.duckdb_row_count == 1
        assert queried.height == 1
        assert summary.pipeline_pass is True
        assert "synthetic-secret" not in repr(processed)
        assert "100.5" not in repr(processed)

    assert not retained_path.exists()


def test_ephemeral_runner_returns_duckdb_replayed_validation_flags(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    provider = AlpacaProvider(
        AlpacaCredentials("synthetic-id", "synthetic-secret"),
        transport=QueueHttpTransport([HttpResponse(200, _FIXTURE.read_bytes())]),
        clock=lambda: _RETRIEVED_AT,
        batch_id_factory=lambda: _BATCH_ID,
    )
    request = BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="XPH1",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        end=datetime(2025, 7, 2, 20, 0, tzinfo=UTC),
        session=TradingSession.REGULAR,
        adjustment_state=AdjustmentState.UNADJUSTED,
    )
    schedule = StaticSessionSchedule(
        (
            SessionBounds(
                session_date=date(2025, 7, 2),
                start=request.start,
                end=request.end,
                source="synthetic finite runner schedule",
            ),
        )
    )
    (batch,) = tuple(provider.get_bars(request))

    def invalid_volume_normalizer(
        batch: RawBatch,
        request: BarRequest,
        *,
        ingested_at: datetime,
        session_schedule: StaticSessionSchedule,
        daily_semantics: DailyBarSemantics,
    ) -> BarNormalizationResult:
        normalized = normalize_alpaca_bars(
            batch,
            request,
            ingested_at=ingested_at,
            session_schedule=session_schedule,
            daily_semantics=daily_semantics,
        )
        return BarNormalizationResult(
            bars=(normalized.bars[0].model_copy(update={"volume": -1.0}),),
            issues=(
                *normalized.issues,
                NormalizationIssue(
                    provider="alpaca",
                    dataset="price_bars",
                    raw_batch_id=batch.metadata.batch_id,
                    code=NormalizationIssueCode.MALFORMED_RECORD,
                    severity=NormalizationSeverity.ERROR,
                    message="synthetic error used to verify pipeline acceptance",
                ),
            ),
        )

    with phase1_temporary_data_root(repository) as data_root:
        processed = EphemeralBarPipeline(data_root, repository_root=repository).process(
            batch,
            request,
            normalizer=invalid_volume_normalizer,
            ingested_at=_RETRIEVED_AT,
            session_schedule=schedule,
            daily_semantics=DailyBarSemantics.UNVERIFIED,
        )

        assert processed.metrics.normalization_pass is False
        assert processed.metrics.canonical_validation_pass is False
        assert processed.metrics.validation_flag_counts == (("negative_volume", 1),)
        assert processed.bars[0].quality_flags == (BarQualityFlag.NEGATIVE_VOLUME.value,)
        assert aggregate_pipeline_metrics("alpaca", (processed.metrics,)).pipeline_pass is False
