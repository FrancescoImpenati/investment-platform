"""Provider-neutral normalization results and finite Phase 1 session schedules."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import cast
from uuid import UUID

from investment_platform.data.market_time import US_EASTERN, to_utc
from investment_platform.data.models import CorporateAction, PriceBar
from investment_platform.data.provenance import JsonScalar, RawBatch

type VendorJsonScalar = str | int | float | Decimal | bool | None
type VendorJsonValue = VendorJsonScalar | list[VendorJsonValue] | dict[str, VendorJsonValue]


class NormalizationError(ValueError):
    """The provider payload cannot be parsed as the expected top-level dataset."""


class NormalizationIssueCode(StrEnum):
    """Recoverable reasons why provider evidence was not mapped canonically."""

    MALFORMED_RECORD = "malformed_record"
    UNMAPPED_INSTRUMENT = "unmapped_instrument"
    OUTSIDE_REQUESTED_INTERVAL = "outside_requested_interval"
    OUTSIDE_REQUESTED_SESSION = "outside_requested_session"
    SESSION_BOUNDS_MISSING = "session_bounds_missing"
    DAILY_SEMANTICS_UNVERIFIED = "daily_semantics_unverified"
    UNSUPPORTED_CORPORATE_ACTION = "unsupported_corporate_action"
    INCOMPLETE_CORPORATE_ACTION = "incomplete_corporate_action"
    CORPORATE_ACTION_DATE_BASIS = "corporate_action_date_basis"
    PROVIDER_DEFINITION = "provider_definition"
    PROVIDER_RESPONSE_ERROR = "provider_response_error"


class NormalizationSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class DailyBarSemantics(StrEnum):
    """Only a verified regular-session daily definition is canonically representable today."""

    UNVERIFIED = "unverified"
    REGULAR_SESSION = "regular_session"


@dataclass(frozen=True, slots=True)
class NormalizationIssue:
    provider: str
    dataset: str
    raw_batch_id: UUID
    code: NormalizationIssueCode
    severity: NormalizationSeverity
    message: str
    record_index: int | None = None
    provider_record_id: str | None = None


@dataclass(frozen=True, slots=True)
class BarNormalizationResult:
    bars: tuple[PriceBar, ...]
    issues: tuple[NormalizationIssue, ...]


@dataclass(frozen=True, slots=True)
class CorporateActionNormalizationResult:
    actions: tuple[CorporateAction, ...]
    issues: tuple[NormalizationIssue, ...]


@dataclass(frozen=True, slots=True)
class SessionBounds:
    """One externally verified exchange session; no holiday inference is performed."""

    session_date: date
    start: datetime
    end: datetime
    source: str

    def __post_init__(self) -> None:
        start = to_utc(self.start)
        end = to_utc(self.end)
        if end <= start:
            raise ValueError("session end must be later than session start")
        if not self.source.strip():
            raise ValueError("session source must not be blank")
        if start.astimezone(US_EASTERN).date() != self.session_date:
            raise ValueError("session_date must match the session start in America/New_York")
        if (end - timedelta(microseconds=1)).astimezone(US_EASTERN).date() != self.session_date:
            raise ValueError("session end must remain within the same New York session date")
        if end - start > timedelta(days=1):
            raise ValueError("session bounds must not exceed one day")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "source", self.source.strip())


@dataclass(frozen=True, slots=True)
class StaticSessionSchedule:
    """Finite bake-off oracle that deliberately is not a general trading calendar."""

    sessions: tuple[SessionBounds, ...]

    def __post_init__(self) -> None:
        dates = [session.session_date for session in self.sessions]
        if len(dates) != len(set(dates)):
            raise ValueError("session schedule must not contain duplicate dates")

    def bounds_for(self, session_date: date) -> SessionBounds | None:
        return next(
            (session for session in self.sessions if session.session_date == session_date),
            None,
        )


def read_provider_json(batch: RawBatch, *, provider: str, dataset: str) -> VendorJsonValue:
    try:
        with batch.payload.open_binary() as reader:
            parsed = json.load(reader, parse_float=Decimal)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise NormalizationError(f"{provider} {dataset} payload is not valid JSON") from error
    return cast(VendorJsonValue, parsed)


def require_json_object(
    value: VendorJsonValue,
    *,
    provider: str,
    dataset: str,
) -> dict[str, VendorJsonValue]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise NormalizationError(f"{provider} {dataset} payload must be a JSON object")
    return value


def require_matching_request_metadata(
    batch: RawBatch,
    expected: Mapping[str, JsonScalar],
    *,
    provider: str,
    dataset: str,
) -> None:
    """Require persisted request dimensions to agree with the canonical request."""

    missing = sorted(set(expected) - set(batch.metadata.request_metadata))
    mismatches = sorted(
        key
        for key, expected_value in expected.items()
        if key in batch.metadata.request_metadata
        and batch.metadata.request_metadata[key] != expected_value
    )
    if missing or mismatches:
        raise NormalizationError(
            f"{provider} {dataset} raw metadata does not match the canonical request; "
            f"missing={missing}, mismatched={mismatches}"
        )


__all__ = [
    "BarNormalizationResult",
    "CorporateActionNormalizationResult",
    "DailyBarSemantics",
    "NormalizationError",
    "NormalizationIssue",
    "NormalizationIssueCode",
    "NormalizationSeverity",
    "SessionBounds",
    "StaticSessionSchedule",
    "VendorJsonValue",
    "read_provider_json",
    "require_json_object",
    "require_matching_request_metadata",
]
