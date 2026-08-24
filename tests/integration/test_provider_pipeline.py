"""Offline integration of the complete provider-to-DuckDB Phase 1 flow."""

from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pytest

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.normalization import (
    NormalizationIssueCode,
    SessionBounds,
    StaticSessionSchedule,
    normalize_alpaca_bars,
    normalize_massive_bars,
    normalize_twelve_data_bars,
)
from investment_platform.data.providers import (
    AlpacaCredentials,
    AlpacaProvider,
    BarRequest,
    MassiveCredentials,
    MassiveProvider,
    ProviderInstrumentRef,
    TwelveDataCredentials,
    TwelveDataProvider,
)
from investment_platform.data.providers.alpaca import ALPACA_SIP_BAR_SOURCE
from investment_platform.data.providers.http import HttpResponse
from investment_platform.data.providers.twelve_data import TWELVE_DATA_INTRADAY_BAR_SOURCE
from investment_platform.data.storage import (
    ParquetBarStore,
    RawBatchStore,
    price_bars_to_frame,
    replay_raw_artifact,
)
from investment_platform.data.validation import validate_bars
from tests.provider_fakes import QueueHttpTransport

pytestmark = pytest.mark.integration

_FIXTURE = Path(__file__).parents[1] / "fixtures" / "providers" / "massive" / "bars_5m_page_2.json"
_ALPACA_FIXTURE = (
    Path(__file__).parents[1] / "fixtures" / "providers" / "alpaca" / "bars_5m_sip_page_2.json"
)
_TWELVE_DATA_FIXTURE = (
    Path(__file__).parents[1]
    / "fixtures"
    / "providers"
    / "twelve_data"
    / "bars_5m_standard_batch.json"
)
_INSTRUMENT_ID = UUID("10000000-0000-4000-8000-000000000001")
_SECOND_INSTRUMENT_ID = UUID("10000000-0000-4000-8000-000000000002")
_BATCH_ID = UUID("20000000-0000-4000-8000-000000000010")
_ALPACA_BATCH_ID = UUID("30000000-0000-4000-8000-000000000010")
_TWELVE_DATA_BATCH_ID = UUID("40000000-0000-4000-8000-000000000010")
_RETRIEVED = datetime(2026, 8, 19, 12, tzinfo=UTC)


def test_raw_normalize_validate_parquet_and_duckdb_pipeline(tmp_path: Path) -> None:
    transport = QueueHttpTransport([HttpResponse(200, _FIXTURE.read_bytes(), elapsed_ms=8.5)])
    provider = MassiveProvider(
        MassiveCredentials(api_key="synthetic-test-secret"),
        transport=transport,
        clock=lambda: _RETRIEVED,
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
    (downloaded_batch,) = tuple(provider.get_bars(request))

    raw_artifact = RawBatchStore(tmp_path / "raw").write(downloaded_batch)
    persisted_batch = replay_raw_artifact(raw_artifact)
    schedule = StaticSessionSchedule(
        (
            SessionBounds(
                session_date=date(2025, 7, 2),
                start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
                end=datetime(2025, 7, 2, 20, 0, tzinfo=UTC),
                source="synthetic Phase 1 session fixture",
            ),
        )
    )
    normalized = normalize_massive_bars(
        persisted_batch,
        request,
        ingested_at=_RETRIEVED,
        session_schedule=schedule,
    )

    assert len(normalized.bars) == 1
    assert [issue.code for issue in normalized.issues] == [
        NormalizationIssueCode.OUTSIDE_REQUESTED_SESSION
    ]
    frame = price_bars_to_frame(normalized.bars)
    validated = validate_bars(frame)
    assert validated.issues == ()

    store = ParquetBarStore(tmp_path / "normalized" / "price_bars")
    stored_paths = store.append(validated.frame, _BATCH_ID)
    queried = store.query()

    assert len(stored_paths) == 1
    assert queried.height == 1
    row = queried.row(0, named=True)
    assert row["instrument_id"] == str(_INSTRUMENT_ID)
    assert row["raw_batch_id"] == str(_BATCH_ID)
    assert row["timestamp_start"] == datetime(2025, 7, 2, 13, 35, tzinfo=UTC)
    assert row["session"] == "regular"
    assert raw_artifact.payload_path.exists()
    assert "synthetic-test-secret" not in raw_artifact.manifest_path.read_text(encoding="utf-8")


def test_alpaca_sip_raw_normalize_validate_parquet_and_duckdb_pipeline(
    tmp_path: Path,
) -> None:
    transport = QueueHttpTransport(
        [HttpResponse(200, _ALPACA_FIXTURE.read_bytes(), elapsed_ms=7.5)]
    )
    provider = AlpacaProvider(
        AlpacaCredentials(
            key_id="synthetic-alpaca-id",
            secret_key="synthetic-alpaca-secret",
        ),
        transport=transport,
        clock=lambda: _RETRIEVED,
        batch_id_factory=lambda: _ALPACA_BATCH_ID,
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
    (downloaded_batch,) = tuple(provider.get_bars(request))

    raw_artifact = RawBatchStore(tmp_path / "raw").write(downloaded_batch)
    persisted_batch = replay_raw_artifact(raw_artifact)
    schedule = StaticSessionSchedule(
        (
            SessionBounds(
                session_date=date(2025, 7, 2),
                start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
                end=datetime(2025, 7, 2, 20, 0, tzinfo=UTC),
                source="synthetic Phase 1 session fixture",
            ),
        )
    )
    normalized = normalize_alpaca_bars(
        persisted_batch,
        request,
        ingested_at=_RETRIEVED,
        session_schedule=schedule,
    )

    assert len(normalized.bars) == 1
    assert [issue.code for issue in normalized.issues] == [
        NormalizationIssueCode.OUTSIDE_REQUESTED_SESSION
    ]
    assert normalized.bars[0].source_id == ALPACA_SIP_BAR_SOURCE.source_id
    frame = price_bars_to_frame(normalized.bars)
    validated = validate_bars(frame)
    assert validated.issues == ()

    store = ParquetBarStore(tmp_path / "normalized" / "price_bars")
    stored_paths = store.append(validated.frame, _ALPACA_BATCH_ID)
    queried = store.query()

    assert len(stored_paths) == 1
    assert queried.height == 1
    row = queried.row(0, named=True)
    assert row["instrument_id"] == str(_INSTRUMENT_ID)
    assert row["raw_batch_id"] == str(_ALPACA_BATCH_ID)
    assert row["source_id"] == str(ALPACA_SIP_BAR_SOURCE.source_id)
    assert row["currency"] == "USD"
    assert row["timestamp_start"] == datetime(2025, 7, 2, 13, 35, tzinfo=UTC)
    assert row["session"] == "regular"
    assert downloaded_batch.metadata.source.dataset == "price_bars_sip"
    manifest = raw_artifact.manifest_path.read_text(encoding="utf-8")
    assert "synthetic-alpaca-id" not in manifest
    assert "synthetic-alpaca-secret" not in manifest


def test_twelve_data_raw_normalize_validate_parquet_and_duckdb_pipeline(
    tmp_path: Path,
) -> None:
    transport = QueueHttpTransport(
        [HttpResponse(200, _TWELVE_DATA_FIXTURE.read_bytes(), elapsed_ms=6.5)]
    )
    provider = TwelveDataProvider(
        TwelveDataCredentials(api_key="synthetic-twelve-data-secret"),
        transport=transport,
        clock=lambda: _RETRIEVED,
        batch_id_factory=lambda: _TWELVE_DATA_BATCH_ID,
    )
    request = BarRequest(
        instruments=(
            ProviderInstrumentRef(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="XPH1",
            ),
            ProviderInstrumentRef(
                instrument_id=_SECOND_INSTRUMENT_ID,
                provider_identifier="XPH2",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
        end=datetime(2025, 7, 2, 20, 0, tzinfo=UTC),
        session=TradingSession.REGULAR,
        adjustment_state=AdjustmentState.UNADJUSTED,
    )
    (downloaded_batch,) = tuple(provider.get_bars(request))

    raw_artifact = RawBatchStore(tmp_path / "raw").write(downloaded_batch)
    persisted_batch = replay_raw_artifact(raw_artifact)
    schedule = StaticSessionSchedule(
        (
            SessionBounds(
                session_date=date(2025, 7, 2),
                start=datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
                end=datetime(2025, 7, 2, 20, 0, tzinfo=UTC),
                source="synthetic Phase 1 session fixture",
            ),
        )
    )
    normalized = normalize_twelve_data_bars(
        persisted_batch,
        request,
        ingested_at=_RETRIEVED,
        session_schedule=schedule,
    )

    assert normalized.issues == ()
    assert len(normalized.bars) == 2
    assert all(
        bar.source_id == TWELVE_DATA_INTRADAY_BAR_SOURCE.source_id for bar in normalized.bars
    )
    assert all(bar.vwap is None for bar in normalized.bars)
    frame = price_bars_to_frame(normalized.bars)
    validated = validate_bars(frame)
    assert validated.issues == ()

    store = ParquetBarStore(tmp_path / "normalized" / "price_bars")
    stored_paths = store.append(validated.frame, _TWELVE_DATA_BATCH_ID)
    queried = store.query()

    assert len(stored_paths) == 1
    assert queried.height == 2
    assert set(queried["instrument_id"].to_list()) == {
        str(_INSTRUMENT_ID),
        str(_SECOND_INSTRUMENT_ID),
    }
    assert set(queried["raw_batch_id"].to_list()) == {str(_TWELVE_DATA_BATCH_ID)}
    assert set(queried["source_id"].to_list()) == {str(TWELVE_DATA_INTRADAY_BAR_SOURCE.source_id)}
    manifest = raw_artifact.manifest_path.read_text(encoding="utf-8")
    assert "synthetic-twelve-data-secret" not in manifest
    assert raw_artifact.payload_path.exists()
