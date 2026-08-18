"""Integration tests for deterministic Parquet storage and DuckDB queries."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import polars as pl
import pytest

from investment_platform.data.models import Timeframe
from investment_platform.data.storage.market_bars import (
    PRICE_BAR_SCHEMA,
    BarBatchAlreadyExistsError,
    BarQuery,
    BarSchemaError,
    ParquetBarStore,
)


def _bar_frame(batch_id: UUID) -> pl.DataFrame:
    instrument_one = uuid4()
    instrument_two = uuid4()
    source = uuid4()
    first_start = datetime(2026, 1, 30, 14, 30, tzinfo=UTC)
    second_start = datetime(2026, 2, 2, 14, 30, tzinfo=UTC)
    rows = [
        {
            "instrument_id": str(instrument_two),
            "timeframe": "5m",
            "timestamp_start": second_start,
            "timestamp_end": second_start + timedelta(minutes=5),
            "open": 201.0,
            "high": 202.0,
            "low": 200.0,
            "close": 201.5,
            "volume": None,
            "vwap": None,
            "currency": None,
            "session": "regular",
            "adjustment_state": "unadjusted",
            "source_id": str(source),
            "raw_batch_id": str(batch_id),
            "provider_record_id": None,
            "available_at": None,
            "retrieved_at": second_start + timedelta(minutes=6),
            "ingested_at": second_start + timedelta(minutes=7),
            "quality_flags": ["upstream_flag", "negative_volume"],
        },
        {
            "instrument_id": str(instrument_one),
            "timeframe": "5m",
            "timestamp_start": first_start,
            "timestamp_end": first_start + timedelta(minutes=5),
            "open": 101.0,
            "high": 102.0,
            "low": 100.0,
            "close": 101.5,
            "volume": 1_000.0,
            "vwap": 101.4,
            "currency": "USD",
            "session": "regular",
            "adjustment_state": "unadjusted",
            "source_id": str(source),
            "raw_batch_id": str(batch_id),
            "provider_record_id": "provider-row-1",
            "available_at": first_start + timedelta(minutes=5),
            "retrieved_at": first_start + timedelta(minutes=6),
            "ingested_at": first_start + timedelta(minutes=7),
            "quality_flags": [],
        },
    ]
    return pl.DataFrame(rows, schema=PRICE_BAR_SCHEMA)


@pytest.mark.integration
def test_parquet_duckdb_round_trip_preserves_schema_nulls_utc_lists_and_filters(
    tmp_path: Path,
) -> None:
    batch_id = uuid4()
    store = ParquetBarStore(tmp_path / "normalized" / "price_bars")
    frame = _bar_frame(batch_id)

    paths = store.append(frame, batch_id)

    assert len(paths) == 2
    assert (
        paths[0]
        .as_posix()
        .endswith(f"timeframe=5m/year=2026/month=01/part-{batch_id}-0000.parquet")
    )
    assert (
        paths[1]
        .as_posix()
        .endswith(f"timeframe=5m/year=2026/month=02/part-{batch_id}-0001.parquet")
    )
    result = store.query()
    assert result.schema == PRICE_BAR_SCHEMA
    assert result.height == 2
    assert result.get_column("timestamp_start").dtype == pl.Datetime("us", "UTC")
    assert result.get_column("volume").to_list() == [1_000.0, None]
    assert result.get_column("vwap").to_list() == [101.4, None]
    assert result.get_column("quality_flags").to_list() == [
        [],
        ["negative_volume", "upstream_flag"],
    ]

    first = result.row(0, named=True)
    filtered = store.query(
        BarQuery(
            instrument_ids=(first["instrument_id"].upper(),),
            timeframe=Timeframe.FIVE_MINUTES,
            source_ids=(first["source_id"].upper(),),
            start=first["timestamp_start"],
            end=first["timestamp_end"],
        )
    )
    assert filtered.height == 1
    assert filtered.row(0, named=True)["instrument_id"] == first["instrument_id"]


@pytest.mark.integration
def test_parquet_append_is_fail_closed_for_empty_mismatched_or_replayed_batches(
    tmp_path: Path,
) -> None:
    batch_id = uuid4()
    store = ParquetBarStore(tmp_path / "price_bars")
    frame = _bar_frame(batch_id)

    with pytest.raises(ValueError, match="empty"):
        store.append(pl.DataFrame(schema=PRICE_BAR_SCHEMA), batch_id)

    wrong_batch_id = uuid4()
    with pytest.raises(BarSchemaError, match="raw_batch_id"):
        store.append(frame, wrong_batch_id)

    unsafe_timeframe = frame.with_columns(pl.lit("../5m").alias("timeframe"))
    with pytest.raises(BarSchemaError, match="unsupported timeframe"):
        store.append(unsafe_timeframe, batch_id)

    paths = store.append(frame, batch_id)
    before = {path: path.read_bytes() for path in paths}
    with pytest.raises(BarBatchAlreadyExistsError, match="already exists"):
        store.append(frame, batch_id)
    assert {path: path.read_bytes() for path in paths} == before
    assert len(list(store.root.rglob(f"part-{batch_id}-*.parquet"))) == len(paths)


@pytest.mark.integration
def test_parquet_append_validates_canonical_values_and_normalizes_uuids(tmp_path: Path) -> None:
    batch_id = uuid4()
    store = ParquetBarStore(tmp_path / "price_bars")
    frame = _bar_frame(batch_id)

    with pytest.raises(BarSchemaError, match="invalid UUID"):
        store.append(frame.with_columns(pl.lit("not-a-uuid").alias("instrument_id")), batch_id)
    with pytest.raises(BarSchemaError, match="unsupported session"):
        store.append(frame.with_columns(pl.lit("auction").alias("session")), batch_id)
    with pytest.raises(BarSchemaError, match="non-finite"):
        store.append(frame.with_columns(pl.lit(float("inf")).alias("open")), batch_id)
    with pytest.raises(BarSchemaError, match="quality_flags"):
        store.append(
            frame.with_columns(
                pl.Series(
                    "quality_flags",
                    [["existing", None], ["existing"]],
                    dtype=pl.List(pl.String),
                )
            ),
            batch_id,
        )
    with pytest.raises(BarSchemaError, match="non-empty"):
        store.append(
            frame.with_columns(
                pl.Series(
                    "quality_flags",
                    [["  "], ["existing"]],
                    dtype=pl.List(pl.String),
                )
            ),
            batch_id,
        )
    with pytest.raises(BarSchemaError, match="lists of strings"):
        store.append(
            frame.with_columns(
                pl.Series(
                    "quality_flags",
                    [[123], [456]],
                    dtype=pl.List(pl.Int64),
                )
            ),
            batch_id,
        )

    upper_uuid_frame = frame.with_columns(
        pl.col("instrument_id").str.to_uppercase(),
        pl.col("source_id").str.to_uppercase(),
        pl.col("raw_batch_id").str.to_uppercase(),
    )
    store.append(upper_uuid_frame, batch_id)
    result = store.query()
    assert result.get_column("raw_batch_id").unique().to_list() == [str(batch_id)]
    assert all(value == str(UUID(value)) for value in result.get_column("instrument_id"))


@pytest.mark.integration
def test_parquet_append_normalizes_inferred_empty_quality_flag_lists(tmp_path: Path) -> None:
    batch_id = uuid4()
    store = ParquetBarStore(tmp_path / "price_bars")
    frame = _bar_frame(batch_id).with_columns(
        pl.Series("quality_flags", [[], []], dtype=pl.List(pl.Null))
    )

    store.append(frame, batch_id)

    assert store.query().get_column("quality_flags").to_list() == [[], []]


@pytest.mark.integration
def test_querying_an_empty_store_returns_the_canonical_empty_frame(tmp_path: Path) -> None:
    result = ParquetBarStore(tmp_path / "missing").query()

    assert result.is_empty()
    assert result.schema == PRICE_BAR_SCHEMA


@pytest.mark.integration
def test_bar_query_rejects_naive_bounds_and_unknown_enum_values() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        BarQuery(start=datetime(2026, 1, 1))

    with pytest.raises(ValueError):
        BarQuery(timeframe="15m")
