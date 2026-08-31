"""Durable metadata-only operational state for living ingestion."""

from investment_platform.data.operational.repository import (
    CalendarSnapshotIdentityCollisionError,
    CalendarSnapshotRepository,
    CalendarSnapshotRepositoryError,
    deterministic_calendar_snapshot_id,
)
from investment_platform.data.operational.schema import LATEST_SCHEMA_VERSION, MIGRATIONS, Migration
from investment_platform.data.operational.store import (
    DATABASE_RELATIVE_PATH,
    DEFAULT_BUSY_TIMEOUT_MS,
    DEFAULT_WRITER_LEASE_NAME,
    OperationalDiagnostics,
    OperationalSchemaError,
    OperationalSchemaTooNewError,
    OperationalStateError,
    OperationalStateStore,
    OperationalTransactionError,
    WriterLease,
    WriterLeaseBusyError,
    WriterLeaseError,
    WriterLeaseLostError,
)

__all__ = [
    "DATABASE_RELATIVE_PATH",
    "DEFAULT_BUSY_TIMEOUT_MS",
    "DEFAULT_WRITER_LEASE_NAME",
    "LATEST_SCHEMA_VERSION",
    "MIGRATIONS",
    "CalendarSnapshotIdentityCollisionError",
    "CalendarSnapshotRepository",
    "CalendarSnapshotRepositoryError",
    "Migration",
    "OperationalDiagnostics",
    "OperationalSchemaError",
    "OperationalSchemaTooNewError",
    "OperationalStateError",
    "OperationalStateStore",
    "OperationalTransactionError",
    "WriterLease",
    "WriterLeaseBusyError",
    "WriterLeaseError",
    "WriterLeaseLostError",
    "deterministic_calendar_snapshot_id",
]
