"""Canonical Parquet persistence and in-process DuckDB queries for price bars."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from uuid import UUID, uuid4

import duckdb
import polars as pl

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession

PRICE_BAR_SCHEMA: Final[pl.Schema] = pl.Schema(
    {
        "instrument_id": pl.String(),
        "timeframe": pl.String(),
        "timestamp_start": pl.Datetime("us", "UTC"),
        "timestamp_end": pl.Datetime("us", "UTC"),
        "open": pl.Float64(),
        "high": pl.Float64(),
        "low": pl.Float64(),
        "close": pl.Float64(),
        "volume": pl.Float64(),
        "vwap": pl.Float64(),
        "currency": pl.String(),
        "session": pl.String(),
        "adjustment_state": pl.String(),
        "source_id": pl.String(),
        "raw_batch_id": pl.String(),
        "provider_record_id": pl.String(),
        "available_at": pl.Datetime("us", "UTC"),
        "retrieved_at": pl.Datetime("us", "UTC"),
        "ingested_at": pl.Datetime("us", "UTC"),
        "quality_flags": pl.List(pl.String()),
    }
)

_COLUMNS: Final[tuple[str, ...]] = tuple(PRICE_BAR_SCHEMA)
_TIMESTAMP_COLUMNS: Final[tuple[str, ...]] = (
    "timestamp_start",
    "timestamp_end",
    "available_at",
    "retrieved_at",
    "ingested_at",
)
_NULLABLE_COLUMNS: Final[frozenset[str]] = frozenset(
    {"volume", "vwap", "currency", "provider_record_id", "available_at"}
)
_FLOAT_COLUMNS: Final[tuple[str, ...]] = ("open", "high", "low", "close", "volume", "vwap")
_UUID_COLUMNS: Final[tuple[str, ...]] = ("instrument_id", "source_id", "raw_batch_id")
_ENUM_VALUES: Final[dict[str, frozenset[str]]] = {
    "timeframe": frozenset(member.value for member in Timeframe),
    "session": frozenset(member.value for member in TradingSession),
    "adjustment_state": frozenset(member.value for member in AdjustmentState),
}
_ROW_SORT_COLUMNS: Final[tuple[str, ...]] = tuple(
    column for column in _COLUMNS if column != "quality_flags"
)


class BarStorageError(RuntimeError):
    """Base error for canonical bar persistence."""


class BarSchemaError(BarStorageError, ValueError):
    """Raised when a frame cannot satisfy the canonical schema."""


class BarBatchAlreadyExistsError(BarStorageError):
    """Raised when a canonical batch identifier has already been published."""


def _as_filter_value(value: AdjustmentState | Timeframe | TradingSession | str) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _normalize_bound(value: datetime | None, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _normalize_uuid_filters(values: tuple[UUID | str, ...], *, field_name: str) -> tuple[str, ...]:
    normalized: list[str] = []
    for value in values:
        try:
            normalized.append(str(UUID(str(value))))
        except ValueError as error:
            raise ValueError(f"{field_name} contains an invalid UUID: {value!r}") from error
    return tuple(normalized)


@dataclass(frozen=True, slots=True)
class BarQuery:
    """Filters over canonical bars; temporal bounds are half-open on bar start."""

    instrument_ids: tuple[UUID | str, ...] = ()
    timeframe: Timeframe | str | None = None
    source_ids: tuple[UUID | str, ...] = ()
    session: TradingSession | str | None = None
    adjustment_state: AdjustmentState | str | None = None
    start: datetime | None = None
    end: datetime | None = None

    def __post_init__(self) -> None:
        instrument_ids = _normalize_uuid_filters(
            self.instrument_ids,
            field_name="instrument_ids",
        )
        source_ids = _normalize_uuid_filters(self.source_ids, field_name="source_ids")
        start = _normalize_bound(self.start, field_name="start")
        end = _normalize_bound(self.end, field_name="end")
        if start is not None and end is not None and end <= start:
            raise ValueError("end must be after start")
        if self.timeframe is not None:
            object.__setattr__(self, "timeframe", Timeframe(self.timeframe))
        if self.session is not None:
            object.__setattr__(self, "session", TradingSession(self.session))
        if self.adjustment_state is not None:
            object.__setattr__(
                self,
                "adjustment_state",
                AdjustmentState(self.adjustment_state),
            )
        object.__setattr__(self, "instrument_ids", instrument_ids)
        object.__setattr__(self, "source_ids", source_ids)
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)


def empty_price_bar_frame() -> pl.DataFrame:
    """Return an empty frame with the exact canonical schema."""

    return pl.DataFrame(schema=PRICE_BAR_SCHEMA)


def _is_aware_datetime(dtype: pl.DataType) -> bool:
    return isinstance(dtype, pl.Datetime) and dtype.time_zone is not None


def _coerce_canonical_frame(frame: pl.DataFrame) -> pl.DataFrame:
    actual = set(frame.columns)
    expected = set(_COLUMNS)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing={missing}")
        if extra:
            details.append(f"extra={extra}")
        raise BarSchemaError("canonical price-bar columns do not match: " + ", ".join(details))

    for column in _TIMESTAMP_COLUMNS:
        dtype = frame.schema[column]
        if column == "available_at" and dtype == pl.Null:
            continue
        if not _is_aware_datetime(dtype):
            raise BarSchemaError(f"{column} must be a timezone-aware datetime column")

    quality_flags_dtype = frame.schema["quality_flags"]
    if quality_flags_dtype == pl.Null:
        frame = frame.with_columns(pl.lit([], dtype=pl.List(pl.String)).alias("quality_flags"))
    elif isinstance(quality_flags_dtype, pl.List) and quality_flags_dtype.inner == pl.Null:
        has_nested_nulls = frame.select(
            pl.col("quality_flags").list.len().fill_null(0).gt(0).any()
        ).item()
        if has_nested_nulls:
            raise BarSchemaError("quality_flags must contain only strings")
        frame = frame.with_columns(pl.lit([], dtype=pl.List(pl.String)).alias("quality_flags"))
    elif not (isinstance(quality_flags_dtype, pl.List) and quality_flags_dtype.inner == pl.String):
        raise BarSchemaError("quality_flags must be lists of strings")

    expressions: list[pl.Expr] = []
    for column, dtype in PRICE_BAR_SCHEMA.items():
        expression = pl.col(column)
        if column == "quality_flags":
            expression = expression.fill_null(pl.lit([], dtype=pl.List(pl.String)))
        expressions.append(expression.cast(dtype, strict=True).alias(column))

    try:
        canonical = frame.select(expressions)
    except (pl.exceptions.ComputeError, pl.exceptions.InvalidOperationError) as error:
        raise BarSchemaError("frame values cannot be cast to the canonical schema") from error

    canonical = canonical.with_columns(
        pl.col("quality_flags").list.eval(pl.element().str.strip_chars())
    )

    required = [column for column in _COLUMNS if column not in _NULLABLE_COLUMNS]
    null_counts = canonical.select(pl.col(required).null_count()).row(0, named=True)
    columns_with_nulls = sorted(name for name, count in null_counts.items() if count)
    if columns_with_nulls:
        raise BarSchemaError(f"required columns contain nulls: {columns_with_nulls}")

    non_finite_columns = [
        column
        for column in _FLOAT_COLUMNS
        if not canonical.get_column(column).drop_nulls().is_finite().all()
    ]
    if non_finite_columns:
        raise BarSchemaError(f"numeric columns contain non-finite values: {non_finite_columns}")
    has_null_flag = canonical.select(
        pl.col("quality_flags").list.eval(pl.element().is_null()).list.any().any()
    ).item()
    if has_null_flag:
        raise BarSchemaError("quality_flags must contain only strings")
    has_blank_flag = canonical.select(
        pl.col("quality_flags").list.eval(pl.element() == "").list.any().any()
    ).item()
    if has_blank_flag:
        raise BarSchemaError("quality_flags must contain non-empty strings")

    uuid_replacements: dict[str, dict[str, str]] = {}
    for column in _UUID_COLUMNS:
        replacements: dict[str, str] = {}
        for value in canonical.get_column(column).unique().to_list():
            try:
                replacements[value] = str(UUID(value))
            except (AttributeError, TypeError, ValueError) as error:
                raise BarSchemaError(f"{column} contains an invalid UUID: {value!r}") from error
        uuid_replacements[column] = replacements
    canonical = canonical.with_columns(
        pl.col(column).replace_strict(replacements, return_dtype=pl.String).alias(column)
        for column, replacements in uuid_replacements.items()
    )

    if canonical.filter(pl.col("timestamp_end") <= pl.col("timestamp_start")).height:
        raise BarSchemaError("every timestamp_end must be after timestamp_start")
    if canonical.filter(pl.col("ingested_at") < pl.col("retrieved_at")).height:
        raise BarSchemaError("ingested_at must not be earlier than retrieved_at")

    for column, allowed_values in _ENUM_VALUES.items():
        unsupported_values = sorted(
            set(canonical.get_column(column).unique().to_list()) - allowed_values
        )
        if unsupported_values:
            raise BarSchemaError(f"unsupported {column} values: {unsupported_values}")

    return canonical.with_columns(pl.col("quality_flags").list.sort())


class ParquetBarStore:
    """Append-safe Parquet dataset queried through an in-memory DuckDB connection."""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)

    @property
    def root(self) -> Path:
        return self._root

    def append(self, frame: pl.DataFrame, batch_id: UUID) -> tuple[Path, ...]:
        """Append one normalized batch without overwriting or suffixing existing targets."""

        if frame.is_empty():
            raise ValueError("cannot append an empty price-bar frame")

        canonical = _coerce_canonical_frame(frame)
        batch_text = str(batch_id)
        stored_batch_ids = canonical.get_column("raw_batch_id").unique().to_list()
        if stored_batch_ids != [batch_text]:
            raise BarSchemaError("raw_batch_id must equal the append batch_id for every row")

        if self._root.exists() and any(self._root.rglob(f"part-{batch_text}-*.parquet")):
            raise BarBatchAlreadyExistsError(f"canonical batch {batch_text} already exists")

        partitioned = canonical.with_columns(
            pl.col("timestamp_start").dt.year().alias("_year"),
            pl.col("timestamp_start").dt.month().alias("_month"),
        )
        partition_keys = (
            partitioned.select("timeframe", "_year", "_month")
            .unique()
            .sort("timeframe", "_year", "_month")
            .iter_rows()
        )

        groups: list[tuple[Path, pl.DataFrame]] = []
        for ordinal, (timeframe, year, month) in enumerate(partition_keys):
            target = (
                self._root
                / f"timeframe={timeframe}"
                / f"year={year:04d}"
                / f"month={month:02d}"
                / f"part-{batch_text}-{ordinal:04d}.parquet"
            )
            group = (
                partitioned.filter(
                    (pl.col("timeframe") == timeframe)
                    & (pl.col("_year") == year)
                    & (pl.col("_month") == month)
                )
                .drop("_year", "_month")
                .sort(_ROW_SORT_COLUMNS, nulls_last=True)
            )
            groups.append((target, group))

        existing = [target for target, _ in groups if target.exists()]
        if existing:
            raise BarBatchAlreadyExistsError(
                f"refusing to overwrite canonical target {existing[0]}"
            )

        staged: list[tuple[Path, Path]] = []
        published: list[Path] = []
        try:
            for target, group in groups:
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
                group.write_parquet(
                    temporary,
                    compression="zstd",
                    statistics=True,
                )
                staged.append((temporary, target))

            for temporary, target in staged:
                os.link(temporary, target)
                published.append(target)
        except Exception:
            for target in published:
                target.unlink(missing_ok=True)
            raise
        finally:
            for temporary, _ in staged:
                temporary.unlink(missing_ok=True)

        return tuple(published)

    def query(self, query: BarQuery | None = None) -> pl.DataFrame:
        """Query all matching Parquet parts and return a canonical Polars frame."""

        files = sorted(self._root.rglob("part-*.parquet")) if self._root.exists() else []
        if not files:
            return empty_price_bar_frame()

        query = query or BarQuery()
        predicates: list[str] = []
        parameters: list[object] = []

        if query.instrument_ids:
            placeholders = ", ".join("?" for _ in query.instrument_ids)
            predicates.append(f"instrument_id IN ({placeholders})")
            parameters.extend(str(value) for value in query.instrument_ids)
        if query.timeframe is not None:
            predicates.append("timeframe = ?")
            parameters.append(_as_filter_value(query.timeframe))
        if query.source_ids:
            placeholders = ", ".join("?" for _ in query.source_ids)
            predicates.append(f"source_id IN ({placeholders})")
            parameters.extend(str(value) for value in query.source_ids)
        if query.session is not None:
            predicates.append("session = ?")
            parameters.append(_as_filter_value(query.session))
        if query.adjustment_state is not None:
            predicates.append("adjustment_state = ?")
            parameters.append(_as_filter_value(query.adjustment_state))
        if query.start is not None:
            predicates.append("timestamp_start >= ?")
            parameters.append(query.start)
        if query.end is not None:
            predicates.append("timestamp_start < ?")
            parameters.append(query.end)

        where_clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        selected_columns = ", ".join(f'"{column}"' for column in _COLUMNS)
        sql = (
            f"SELECT {selected_columns} FROM _canonical_price_bars{where_clause} "
            "ORDER BY timestamp_start, instrument_id, source_id, adjustment_state, session"
        )

        with duckdb.connect(":memory:") as connection:
            connection.execute("SET TimeZone = 'UTC'")
            relation = connection.read_parquet(
                [str(path) for path in files],
                union_by_name=True,
            )
            relation.create_view("_canonical_price_bars", replace=True)
            result = connection.execute(sql, parameters).pl()

        return _coerce_canonical_frame(result)


__all__ = [
    "PRICE_BAR_SCHEMA",
    "BarBatchAlreadyExistsError",
    "BarQuery",
    "BarSchemaError",
    "ParquetBarStore",
    "empty_price_bar_frame",
]
