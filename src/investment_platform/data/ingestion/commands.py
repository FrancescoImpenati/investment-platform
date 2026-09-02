"""Neutral typed contract between the Phase 2 CLI and ingestion service.

The command DTO contains only non-secret intent, exact stream selectors, and hard safety budgets.
It deliberately carries no credentials and exposes no provider payload or market-data values.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Protocol, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.ingestion.planner import IngestionIntent, RepairStrategy
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.runtime import RuntimeSettings

_SAFE_CODE = r"^[A-Z][A-Z0-9_]{0,63}$"
_SAFE_DURABLE_ID = r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,191}$"
_SAFE_PROVIDER_INSTRUMENT = r"^[A-Z0-9][A-Z0-9.-]{0,31}$"
_SENSITIVE_TEXT = re.compile(
    r"(?:[a-z][a-z0-9+.-]*://|(?:authorization|api[_-]?key|password|secret|token)\s*[:=])",
    re.IGNORECASE,
)


class IngestionCommandOutcome(StrEnum):
    """Stable scheduler-facing outcome; incomplete work is not reported as success."""

    SUCCESS = "SUCCESS"
    NO_OP = "NO_OP"
    INCOMPLETE = "INCOMPLETE"
    FAILED = "FAILED"


class _FrozenCommandModel(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class IngestionCommandRequest(_FrozenCommandModel):
    """One explicit, bounded CLI intent ready for service-side resolution and planning."""

    intent: IngestionIntent
    provider: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")]
    dataset: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")]
    instruments: Annotated[tuple[str, ...], Field(min_length=1, max_length=16)]
    timeframe: Timeframe
    session: TradingSession
    adjustment: AdjustmentState
    start: datetime | None = None
    end: datetime | None = None
    repair_strategy: RepairStrategy | None = None
    repair_reason: Annotated[str, Field(min_length=1, max_length=256)] | None = None
    max_calls: Annotated[int, Field(gt=0)]
    max_pages: Annotated[int, Field(gt=0)]
    max_expected_observations: Annotated[int, Field(gt=0)]
    max_estimated_bytes: Annotated[int, Field(gt=0)]
    max_estimated_cost: Annotated[Decimal, Field(ge=0)]

    @field_validator("provider", "dataset", mode="before")
    @classmethod
    def require_exact_lowercase_key(cls, value: object) -> object:
        if isinstance(value, str) and value != value.casefold():
            raise ValueError("provider and dataset keys must use exact lowercase spelling")
        return value

    @field_validator("instruments", mode="before")
    @classmethod
    def normalize_instruments(cls, value: object) -> object:
        if isinstance(value, (list, tuple)):
            return tuple(item.upper() if isinstance(item, str) else item for item in value)
        return value

    @field_validator("instruments", mode="after")
    @classmethod
    def validate_instruments(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(re.fullmatch(_SAFE_PROVIDER_INSTRUMENT, item) is None for item in value):
            raise ValueError("instrument selectors must be safe provider symbols")
        ordered = tuple(sorted(set(value)))
        if len(ordered) != len(value):
            raise ValueError("instrument selectors must be unique")
        return ordered

    @field_validator("start", "end", mode="after")
    @classmethod
    def normalize_bound(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("command bounds must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("repair_reason", mode="after")
    @classmethod
    def reject_sensitive_reason(cls, value: str | None) -> str | None:
        if value is not None and ("\r" in value or "\n" in value or _SENSITIVE_TEXT.search(value)):
            raise ValueError("repair reason must not contain sensitive or multiline text")
        return value

    @model_validator(mode="after")
    def validate_intent_shape(self) -> Self:
        if self.intent is IngestionIntent.UPDATE:
            if self.start is not None:
                raise ValueError("update derives its start from durable coverage")
            if self.repair_strategy is not None or self.repair_reason is not None:
                raise ValueError("update cannot carry repair settings")
            return self

        if self.start is None or self.end is None:
            raise ValueError("backfill and repair require explicit half-open bounds")
        if self.end <= self.start:
            raise ValueError("command end must be later than start")
        if self.intent is IngestionIntent.REPAIR:
            if self.repair_strategy is None or self.repair_reason is None:
                raise ValueError("repair requires an explicit strategy and reason")
        elif self.repair_strategy is not None or self.repair_reason is not None:
            raise ValueError("repair settings are valid only for repair")
        return self


class IngestionCommandResult(_FrozenCommandModel):
    """Sanitized aggregate service result safe for human or JSON CLI output."""

    outcome: IngestionCommandOutcome
    code: str = Field(pattern=_SAFE_CODE)
    run_id: str | None = Field(default=None, pattern=_SAFE_DURABLE_ID)
    planned_request_count: Annotated[int, Field(ge=0)] = 0
    completed_request_count: Annotated[int, Field(ge=0)] = 0
    raw_artifact_count: Annotated[int, Field(ge=0)] = 0
    canonical_batch_count: Annotated[int, Field(ge=0)] = 0
    open_gap_count: Annotated[int, Field(ge=0)] = 0

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if self.completed_request_count > self.planned_request_count:
            raise ValueError("completed request count cannot exceed planned request count")
        if self.outcome is IngestionCommandOutcome.NO_OP and any(
            (
                self.planned_request_count,
                self.completed_request_count,
                self.raw_artifact_count,
                self.canonical_batch_count,
            )
        ):
            raise ValueError("NO_OP cannot report newly planned or persisted work")
        return self


class IngestionCommandRunner(Protocol):
    """Production or injected command executor; argparse remains outside the service."""

    def run(self, request: IngestionCommandRequest) -> IngestionCommandResult:
        """Execute one bounded command and return a sanitized aggregate outcome."""

    def resume(self, run_id: UUID) -> IngestionCommandResult:
        """Resume one exact durable run without reconstructing its intent from CLI input."""


class IngestionCommandRunnerFactory(Protocol):
    """Construct a runner only after the CLI has resolved the explicit runtime profile."""

    def __call__(
        self,
        settings: RuntimeSettings,
        repository_root: Path,
    ) -> IngestionCommandRunner: ...


__all__ = [
    "IngestionCommandOutcome",
    "IngestionCommandRequest",
    "IngestionCommandResult",
    "IngestionCommandRunner",
    "IngestionCommandRunnerFactory",
]
