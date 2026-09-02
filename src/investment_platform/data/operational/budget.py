"""Lease-fenced, restart-safe provider request budgets.

Reservations are durable and idempotent per attempt/window.  A caller consumes a reservation
*before* provider I/O, so a crash can over-count a call but can never make the next process forget
capacity that may already have reached the provider.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Self
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.data.operational.store import (
    OperationalStateError,
    OperationalStateStore,
    WriterLease,
    _format_utc,
    _parse_utc,
)

_RESERVATION_NAMESPACE = UUID("68dc7c52-9df1-4dc5-98c6-e135fe7137d4")
_SAFE_SCOPE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")


class ProviderBudgetError(OperationalStateError):
    """Base error for durable provider-budget coordination."""


class ProviderBudgetExceededError(ProviderBudgetError):
    """Raised before dispatch when the durable window has no remaining capacity."""


class ProviderBudgetCollisionError(ProviderBudgetError):
    """Raised when a stable budget identity resolves to different metadata."""


class ProviderBudgetStateConflictError(ProviderBudgetError):
    """Raised for an invalid reservation state transition or attempt scope."""


class BudgetReservationState(StrEnum):
    RESERVED = "RESERVED"
    CONSUMED = "CONSUMED"
    RELEASED = "RELEASED"


class _FrozenBudgetModel(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class ProviderBudgetWindow(_FrozenBudgetModel):
    """One provider/dataset-specific fixed capacity window."""

    provider: str = Field(min_length=1, max_length=128)
    dataset: str = Field(min_length=1, max_length=128)
    budget_key: str = Field(min_length=1, max_length=128)
    window_start: datetime
    window_end: datetime
    limit_count: Annotated[int, Field(gt=0)]

    @field_validator("provider", "dataset", "budget_key", mode="after")
    @classmethod
    def validate_safe_scope(cls, value: str) -> str:
        if not _SAFE_SCOPE.fullmatch(value) or "://" in value:
            raise ValueError("budget scope must be a bounded non-secret identifier")
        return value

    @field_validator("window_start", "window_end", mode="after")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("budget window timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if self.window_end <= self.window_start:
            raise ValueError("budget window must be a non-empty half-open interval")
        return self


class ProviderBudgetReservationRequest(_FrozenBudgetModel):
    request_instance_id: UUID
    attempt_id: UUID
    dispatch_ordinal: Annotated[int, Field(gt=0)] = 1
    window: ProviderBudgetWindow
    amount: Annotated[int, Field(gt=0)] = 1


class ProviderBudgetReservation(_FrozenBudgetModel):
    reservation_id: UUID
    request_instance_id: UUID
    attempt_id: UUID
    dispatch_ordinal: Annotated[int, Field(gt=0)]
    window: ProviderBudgetWindow
    amount: Annotated[int, Field(gt=0)]
    state: BudgetReservationState
    used_count: Annotated[int, Field(ge=0)]
    reserved_count: Annotated[int, Field(ge=0)]
    reserved_at: datetime
    finalized_at: datetime | None
    replayed: bool

    @field_validator("reserved_at", "finalized_at", mode="after")
    @classmethod
    def normalize_times(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("reservation timestamps must be timezone-aware")
        return value.astimezone(UTC)


class ProviderBudgetSnapshot(_FrozenBudgetModel):
    window: ProviderBudgetWindow
    used_count: Annotated[int, Field(ge=0)]
    reserved_count: Annotated[int, Field(ge=0)]
    observed_at: datetime
    updated_at: datetime

    @property
    def available_count(self) -> int:
        return self.window.limit_count - self.used_count - self.reserved_count

    @field_validator("observed_at", "updated_at", mode="after")
    @classmethod
    def normalize_times(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("budget timestamps must be timezone-aware")
        return value.astimezone(UTC)


def deterministic_budget_reservation_id(
    attempt_id: UUID,
    budget_key: str,
    window_start: datetime,
    dispatch_ordinal: int = 1,
) -> UUID:
    """Return the stable identity used to make reserve retries idempotent."""

    if not _SAFE_SCOPE.fullmatch(budget_key) or "://" in budget_key:
        raise ValueError("budget_key must be a bounded non-secret identifier")
    if window_start.tzinfo is None or window_start.utcoffset() is None:
        raise ValueError("window_start must be timezone-aware")
    if dispatch_ordinal <= 0:
        raise ValueError("dispatch_ordinal must be positive")
    identity = (
        f"{attempt_id}|{budget_key}|{dispatch_ordinal}|{_format_utc(window_start.astimezone(UTC))}"
    )
    return uuid5(_RESERVATION_NAMESPACE, identity)


class ProviderBudgetRepository:
    """Typed persistence for fixed-window call reservations and observations."""

    def __init__(self, store: OperationalStateStore) -> None:
        self._store = store

    def reserve(
        self,
        lease: WriterLease,
        request: ProviderBudgetReservationRequest,
    ) -> ProviderBudgetReservation:
        now = self._store._now()
        window = request.window
        if not window.window_start <= now < window.window_end:
            raise ProviderBudgetStateConflictError("budget reservation requires the active window")
        reservation_id = deterministic_budget_reservation_id(
            request.attempt_id,
            window.budget_key,
            window.window_start,
            request.dispatch_ordinal,
        )
        try:
            with self._store._leased_transaction(lease) as connection:
                self._require_attempt_scope(connection, request)
                budget = connection.execute(
                    """
                    SELECT * FROM provider_budget_state
                    WHERE provider = ? AND dataset = ? AND budget_key = ? AND window_start = ?
                    """,
                    self._window_key(window),
                ).fetchone()
                if budget is None:
                    connection.execute(
                        """
                        INSERT INTO provider_budget_state(
                            provider, dataset, budget_key, window_start, window_end,
                            limit_count, used_count, reserved_count, observed_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
                        """,
                        (
                            *self._window_key(window),
                            _format_utc(window.window_end),
                            window.limit_count,
                            _format_utc(now),
                            _format_utc(now),
                        ),
                    )
                    budget = connection.execute(
                        """
                        SELECT * FROM provider_budget_state
                        WHERE provider = ? AND dataset = ? AND budget_key = ? AND window_start = ?
                        """,
                        self._window_key(window),
                    ).fetchone()
                if budget is None:
                    raise ProviderBudgetStateConflictError("budget window was not persisted")
                self._require_window_match(budget, window)
                existing = connection.execute(
                    "SELECT * FROM provider_budget_reservations WHERE reservation_id = ?",
                    (str(reservation_id),),
                ).fetchone()
                if existing is not None:
                    self._require_reservation_match(existing, request)
                    return self._reservation_from_rows(existing, budget, replayed=True)
                used = int(budget["used_count"])
                reserved = int(budget["reserved_count"])
                if used + reserved + request.amount > window.limit_count:
                    raise ProviderBudgetExceededError("provider request budget is exhausted")
                connection.execute(
                    """
                    UPDATE provider_budget_state
                    SET reserved_count = reserved_count + ?, updated_at = ?
                    WHERE provider = ? AND dataset = ? AND budget_key = ? AND window_start = ?
                    """,
                    (
                        request.amount,
                        _format_utc(now),
                        *self._window_key(window),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO provider_budget_reservations(
                        reservation_id, provider, dataset, budget_key, window_start,
                        request_instance_id, attempt_id, dispatch_ordinal, amount,
                        state, reserved_at, finalized_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'RESERVED', ?, NULL)
                    """,
                    (
                        str(reservation_id),
                        *self._window_key(window),
                        str(request.request_instance_id),
                        str(request.attempt_id),
                        request.dispatch_ordinal,
                        request.amount,
                        _format_utc(now),
                    ),
                )
                updated = connection.execute(
                    """
                    SELECT * FROM provider_budget_state
                    WHERE provider = ? AND dataset = ? AND budget_key = ? AND window_start = ?
                    """,
                    self._window_key(window),
                ).fetchone()
                reservation = connection.execute(
                    "SELECT * FROM provider_budget_reservations WHERE reservation_id = ?",
                    (str(reservation_id),),
                ).fetchone()
                if updated is None or reservation is None:
                    raise ProviderBudgetStateConflictError("budget reservation disappeared")
                return self._reservation_from_rows(reservation, updated, replayed=False)
        except sqlite3.IntegrityError as error:
            raise ProviderBudgetCollisionError(
                "SQLite rejected provider-budget identity; no partial reservation was committed"
            ) from error

    def latest_for_attempt(
        self,
        attempt_id: UUID,
        *,
        budget_key: str,
    ) -> ProviderBudgetReservation | None:
        """Return the latest durable dispatch reservation for restart decisions."""

        if not _SAFE_SCOPE.fullmatch(budget_key) or "://" in budget_key:
            raise ValueError("budget_key must be a bounded non-secret identifier")
        with self._store.read_only_connection() as connection:
            reservation = connection.execute(
                """
                SELECT * FROM provider_budget_reservations
                WHERE attempt_id = ? AND budget_key = ?
                ORDER BY dispatch_ordinal DESC LIMIT 1
                """,
                (str(attempt_id), budget_key),
            ).fetchone()
            if reservation is None:
                return None
            _, budget = self._load_reservation_and_budget(
                connection,
                UUID(str(reservation["reservation_id"])),
            )
        return self._reservation_from_rows(reservation, budget, replayed=True)

    def consume_before_dispatch(
        self,
        lease: WriterLease,
        reservation_id: UUID,
    ) -> ProviderBudgetReservation:
        """Count the possible provider call before any network side effect."""

        return self._finalize(lease, reservation_id, consume=True)

    def release_before_dispatch(
        self,
        lease: WriterLease,
        reservation_id: UUID,
    ) -> ProviderBudgetReservation:
        """Release capacity only when the caller proves dispatch never began."""

        return self._finalize(lease, reservation_id, consume=False)

    def observe_remaining(
        self,
        lease: WriterLease,
        reservation_id: UUID,
        *,
        capacity: int,
        remaining: int,
        observed_at: datetime | None = None,
    ) -> ProviderBudgetSnapshot:
        """Reconcile safe provider headers without reducing already counted local use."""

        if capacity <= 0 or remaining < 0 or remaining > capacity:
            raise ValueError("provider budget observation is outside its valid range")
        if observed_at is not None and (
            observed_at.tzinfo is None or observed_at.utcoffset() is None
        ):
            raise ValueError("provider budget observation timestamp must be timezone-aware")
        observed = self._store._now() if observed_at is None else observed_at.astimezone(UTC)
        with self._store._leased_transaction(lease) as connection:
            reservation, budget = self._load_reservation_and_budget(connection, reservation_id)
            if str(reservation["state"]) != BudgetReservationState.CONSUMED.value:
                raise ProviderBudgetStateConflictError(
                    "provider headers require a consumed dispatch reservation"
                )
            current_used = int(budget["used_count"])
            reserved = int(budget["reserved_count"])
            observed_used = capacity - remaining
            reconciled_used = max(current_used, observed_used)
            if reconciled_used + reserved > capacity:
                raise ProviderBudgetStateConflictError(
                    "provider observation conflicts with durable commitments"
                )
            connection.execute(
                """
                UPDATE provider_budget_state
                SET limit_count = ?, used_count = ?, observed_at = ?, updated_at = ?
                WHERE provider = ? AND dataset = ? AND budget_key = ? AND window_start = ?
                """,
                (
                    capacity,
                    reconciled_used,
                    _format_utc(observed),
                    _format_utc(self._store._now()),
                    str(budget["provider"]),
                    str(budget["dataset"]),
                    str(budget["budget_key"]),
                    str(budget["window_start"]),
                ),
            )
            updated = connection.execute(
                """
                SELECT * FROM provider_budget_state
                WHERE provider = ? AND dataset = ? AND budget_key = ? AND window_start = ?
                """,
                (
                    str(budget["provider"]),
                    str(budget["dataset"]),
                    str(budget["budget_key"]),
                    str(budget["window_start"]),
                ),
            ).fetchone()
            if updated is None:
                raise ProviderBudgetStateConflictError("budget window disappeared")
            return self._snapshot_from_row(updated)

    def snapshot(self, window: ProviderBudgetWindow) -> ProviderBudgetSnapshot | None:
        with self._store.read_only_connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM provider_budget_state
                WHERE provider = ? AND dataset = ? AND budget_key = ? AND window_start = ?
                """,
                self._window_key(window),
            ).fetchone()
        if row is None:
            return None
        self._require_window_match(row, window)
        return self._snapshot_from_row(row)

    def active_snapshot(
        self,
        *,
        provider: str,
        dataset: str,
        budget_key: str,
        at: datetime,
    ) -> ProviderBudgetSnapshot | None:
        """Load the effective observed capacity for one active exact window."""

        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("budget lookup timestamp must be timezone-aware")
        when = at.astimezone(UTC)
        with self._store.read_only_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM provider_budget_state
                WHERE provider = ? AND dataset = ? AND budget_key = ?
                  AND window_start <= ? AND window_end > ?
                ORDER BY window_start DESC
                """,
                (
                    provider,
                    dataset,
                    budget_key,
                    _format_utc(when),
                    _format_utc(when),
                ),
            ).fetchall()
        if len(rows) > 1:
            raise ProviderBudgetStateConflictError(
                "multiple durable provider windows overlap the lookup timestamp"
            )
        return None if not rows else self._snapshot_from_row(rows[0])

    def _finalize(
        self,
        lease: WriterLease,
        reservation_id: UUID,
        *,
        consume: bool,
    ) -> ProviderBudgetReservation:
        target = BudgetReservationState.CONSUMED if consume else BudgetReservationState.RELEASED
        with self._store._leased_transaction(lease) as connection:
            reservation, budget = self._load_reservation_and_budget(connection, reservation_id)
            state = BudgetReservationState(str(reservation["state"]))
            if state == target:
                return self._reservation_from_rows(reservation, budget, replayed=True)
            if state != BudgetReservationState.RESERVED:
                raise ProviderBudgetStateConflictError(
                    "a finalized budget reservation cannot change outcome"
                )
            amount = int(reservation["amount"])
            if int(budget["reserved_count"]) < amount:
                raise ProviderBudgetStateConflictError("budget reservation accounting is corrupt")
            used_increment = amount if consume else 0
            now = self._store._now()
            connection.execute(
                """
                UPDATE provider_budget_state
                SET reserved_count = reserved_count - ?, used_count = used_count + ?, updated_at = ?
                WHERE provider = ? AND dataset = ? AND budget_key = ? AND window_start = ?
                """,
                (
                    amount,
                    used_increment,
                    _format_utc(now),
                    str(reservation["provider"]),
                    str(reservation["dataset"]),
                    str(reservation["budget_key"]),
                    str(reservation["window_start"]),
                ),
            )
            connection.execute(
                """
                UPDATE provider_budget_reservations
                SET state = ?, finalized_at = ?
                WHERE reservation_id = ? AND state = 'RESERVED'
                """,
                (target.value, _format_utc(now), str(reservation_id)),
            )
            updated_reservation, updated_budget = self._load_reservation_and_budget(
                connection, reservation_id
            )
            return self._reservation_from_rows(
                updated_reservation,
                updated_budget,
                replayed=False,
            )

    @staticmethod
    def _window_key(window: ProviderBudgetWindow) -> tuple[str, str, str, str]:
        return (
            window.provider,
            window.dataset,
            window.budget_key,
            _format_utc(window.window_start),
        )

    @staticmethod
    def _require_attempt_scope(
        connection: sqlite3.Connection,
        request: ProviderBudgetReservationRequest,
    ) -> None:
        row = connection.execute(
            """
            SELECT attempt.request_instance_id, attempt.status, run.provider, run.dataset
            FROM request_attempts AS attempt
            JOIN request_instances AS instance
              ON instance.request_instance_id = attempt.request_instance_id
            JOIN ingestion_runs AS run ON run.run_id = instance.run_id
            WHERE attempt.attempt_id = ?
            """,
            (str(request.attempt_id),),
        ).fetchone()
        if row is None:
            raise ProviderBudgetStateConflictError("budget reservation attempt is not durable")
        if (
            str(row["request_instance_id"]) != str(request.request_instance_id)
            or str(row["provider"]) != request.window.provider
            or str(row["dataset"]) != request.window.dataset
        ):
            raise ProviderBudgetStateConflictError(
                "budget reservation does not match the durable request scope"
            )
        if str(row["status"]) != "RUNNING":
            raise ProviderBudgetStateConflictError(
                "budget reservation requires a running provider attempt"
            )

    @staticmethod
    def _require_window_match(row: sqlite3.Row, window: ProviderBudgetWindow) -> None:
        if (
            str(row["window_end"]) != _format_utc(window.window_end)
            or int(row["limit_count"]) != window.limit_count
        ):
            raise ProviderBudgetCollisionError(
                "provider budget window identity collides with different bounds or capacity"
            )

    @staticmethod
    def _require_reservation_match(
        row: sqlite3.Row,
        request: ProviderBudgetReservationRequest,
    ) -> None:
        expected = (
            request.window.provider,
            request.window.dataset,
            request.window.budget_key,
            _format_utc(request.window.window_start),
            str(request.request_instance_id),
            str(request.attempt_id),
            request.dispatch_ordinal,
            request.amount,
        )
        actual = tuple(
            row[column]
            for column in (
                "provider",
                "dataset",
                "budget_key",
                "window_start",
                "request_instance_id",
                "attempt_id",
                "dispatch_ordinal",
                "amount",
            )
        )
        if actual != expected:
            raise ProviderBudgetCollisionError(
                "provider budget reservation identity collides with different metadata"
            )

    @staticmethod
    def _load_reservation_and_budget(
        connection: sqlite3.Connection,
        reservation_id: UUID,
    ) -> tuple[sqlite3.Row, sqlite3.Row]:
        reservation = connection.execute(
            "SELECT * FROM provider_budget_reservations WHERE reservation_id = ?",
            (str(reservation_id),),
        ).fetchone()
        if reservation is None:
            raise ProviderBudgetStateConflictError("provider budget reservation is not durable")
        budget = connection.execute(
            """
            SELECT * FROM provider_budget_state
            WHERE provider = ? AND dataset = ? AND budget_key = ? AND window_start = ?
            """,
            (
                str(reservation["provider"]),
                str(reservation["dataset"]),
                str(reservation["budget_key"]),
                str(reservation["window_start"]),
            ),
        ).fetchone()
        if budget is None:
            raise ProviderBudgetStateConflictError("provider budget window is missing")
        return reservation, budget

    @staticmethod
    def _window_from_row(row: sqlite3.Row) -> ProviderBudgetWindow:
        return ProviderBudgetWindow(
            provider=str(row["provider"]),
            dataset=str(row["dataset"]),
            budget_key=str(row["budget_key"]),
            window_start=_parse_utc(str(row["window_start"])),
            window_end=_parse_utc(str(row["window_end"])),
            limit_count=int(row["limit_count"]),
        )

    @classmethod
    def _snapshot_from_row(cls, row: sqlite3.Row) -> ProviderBudgetSnapshot:
        return ProviderBudgetSnapshot(
            window=cls._window_from_row(row),
            used_count=int(row["used_count"]),
            reserved_count=int(row["reserved_count"]),
            observed_at=_parse_utc(str(row["observed_at"])),
            updated_at=_parse_utc(str(row["updated_at"])),
        )

    @classmethod
    def _reservation_from_rows(
        cls,
        reservation: sqlite3.Row,
        budget: sqlite3.Row,
        *,
        replayed: bool,
    ) -> ProviderBudgetReservation:
        return ProviderBudgetReservation(
            reservation_id=UUID(str(reservation["reservation_id"])),
            request_instance_id=UUID(str(reservation["request_instance_id"])),
            attempt_id=UUID(str(reservation["attempt_id"])),
            dispatch_ordinal=int(reservation["dispatch_ordinal"]),
            window=cls._window_from_row(budget),
            amount=int(reservation["amount"]),
            state=BudgetReservationState(str(reservation["state"])),
            used_count=int(budget["used_count"]),
            reserved_count=int(budget["reserved_count"]),
            reserved_at=_parse_utc(str(reservation["reserved_at"])),
            finalized_at=(
                None
                if reservation["finalized_at"] is None
                else _parse_utc(str(reservation["finalized_at"]))
            ),
            replayed=replayed,
        )


__all__ = [
    "BudgetReservationState",
    "ProviderBudgetCollisionError",
    "ProviderBudgetError",
    "ProviderBudgetExceededError",
    "ProviderBudgetRepository",
    "ProviderBudgetReservation",
    "ProviderBudgetReservationRequest",
    "ProviderBudgetSnapshot",
    "ProviderBudgetStateConflictError",
    "ProviderBudgetWindow",
    "deterministic_budget_reservation_id",
]
