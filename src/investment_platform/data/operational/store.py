"""SQLite operational-state foundation for restartable ingestion."""

from __future__ import annotations

import hashlib
import importlib
import os
import re
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from typing import Final, Self, cast

from investment_platform.data.operational.schema import LATEST_SCHEMA_VERSION, MIGRATIONS
from investment_platform.data_root import PrivateDataRoot

DATABASE_RELATIVE_PATH: Final = Path("operational/ingestion.sqlite3")
DEFAULT_BUSY_TIMEOUT_MS: Final = 5_000
DEFAULT_WRITER_LEASE_NAME: Final = "ingestion-writer"
_APPLICATION_ID: Final = 0x49504C54  # ASCII "IPLT".
_IDENTIFIER_PATTERN: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}\Z")
_MINIMUM_LEASE_TTL: Final = timedelta(seconds=1)
_MAXIMUM_LEASE_TTL: Final = timedelta(days=1)
_SQLITE_HEADER_PREFIX: Final = b"SQLite format 3\x00"
_LIFECYCLE_LOCK_OFFSET: Final = 1 << 20


class OperationalStateError(RuntimeError):
    """Base error for the durable operational-state boundary."""


class OperationalSchemaError(OperationalStateError):
    """Raised for an unknown, inconsistent, or partially applied schema."""


class OperationalSchemaTooNewError(OperationalSchemaError):
    """Raised when the database was written by newer application code."""


class OperationalTransactionError(OperationalStateError):
    """Raised when a caller attempts an unsafe transaction operation."""


class WriterLeaseError(OperationalStateError):
    """Base error for writer-lease ownership failures."""


class WriterLeaseBusyError(WriterLeaseError):
    """Raised when another live writer owns the lease."""


class WriterLeaseLostError(WriterLeaseError):
    """Raised when a lease handle is no longer the active generation."""


@dataclass(frozen=True, slots=True)
class WriterLease:
    """Generation-bound handle for the single operational writer."""

    lease_name: str
    owner_id: str
    generation: int
    acquired_at: datetime
    heartbeat_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class OperationalDiagnostics:
    """Sanitized integrity and connection status for the operational database."""

    database_path: Path
    schema_version: int
    journal_mode: str
    synchronous: int
    busy_timeout_ms: int
    integrity_messages: tuple[str, ...]
    foreign_key_violations: int

    @property
    def healthy(self) -> bool:
        return self.integrity_messages == ("ok",) and self.foreign_key_violations == 0


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise OperationalStateError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _format_utc(value: datetime) -> str:
    return (
        _as_utc(value, label="timestamp").isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _as_utc(parsed, label="stored timestamp")


def _validate_identifier(value: str, *, label: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise WriterLeaseError(
            f"{label} must be 1-128 safe identifier characters and contain no whitespace"
        )
    return value


def _validate_lease_ttl(ttl: timedelta) -> timedelta:
    if not _MINIMUM_LEASE_TTL <= ttl <= _MAXIMUM_LEASE_TTL:
        raise WriterLeaseError("writer lease TTL must be between one second and one day")
    return ttl


def _validate_phase2_lease_name(value: str) -> str:
    _validate_identifier(value, label="lease_name")
    if value != DEFAULT_WRITER_LEASE_NAME:
        raise WriterLeaseError("Phase 2 supports only the single ingestion-writer lease")
    return value


def _validate_database_file(path: Path, *, require_existing: bool = False) -> None:
    if path.is_symlink():
        raise OperationalStateError("operational database must not be a symlink")
    try:
        details = path.lstat()
    except FileNotFoundError:
        if require_existing:
            raise OperationalStateError("operational database does not exist") from None
        return
    except OSError as error:
        raise OperationalStateError("cannot inspect the operational database file") from error
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    attributes = getattr(details, "st_file_attributes", 0)
    if (
        not stat.S_ISREG(details.st_mode)
        or details.st_nlink != 1
        or bool(reparse_flag and attributes & reparse_flag)
    ):
        raise OperationalStateError(
            "operational database must be a direct regular non-link file with one hard link"
        )


type _WalSidecarIdentity = tuple[tuple[int, int], tuple[int, int]]


def _wal_sidecar_identity(path: Path) -> _WalSidecarIdentity | None:
    identities: list[tuple[int, int] | None] = []
    for sidecar in (Path(f"{path}-wal"), Path(f"{path}-shm")):
        try:
            details = sidecar.lstat()
        except FileNotFoundError:
            identities.append(None)
            continue
        except OSError as error:
            raise OperationalStateError("cannot inspect an operational WAL sidecar") from error
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        attributes = getattr(details, "st_file_attributes", 0)
        if (
            not stat.S_ISREG(details.st_mode)
            or details.st_nlink != 1
            or bool(reparse_flag and attributes & reparse_flag)
        ):
            raise OperationalStateError(
                "operational WAL sidecars must be direct regular non-link files"
            )
        identities.append((details.st_dev, details.st_ino))
    if (identities[0] is None) != (identities[1] is None):
        raise OperationalStateError("operational WAL sidecar state is incomplete")
    if identities[0] is None:
        return None
    first, second = identities
    assert first is not None and second is not None
    return (first, second)


@contextmanager
def _database_lifecycle_lock(
    sentinel_path: Path,
    *,
    shared: bool,
    timeout_ms: int,
) -> Iterator[None]:
    """Serialize diagnostic opening with cooperative writer close without new files."""

    try:
        descriptor = os.open(sentinel_path, os.O_RDONLY)
    except OSError as error:
        raise OperationalStateError(
            "cannot open the operational database lifecycle lock"
        ) from error
    acquired = False
    deadline = monotonic() + timeout_ms / 1_000
    platform_lock: object | None = None
    try:
        if os.name == "nt":
            platform_lock = importlib.import_module("msvcrt")
            mode = getattr(
                platform_lock,
                "LK_NBRLCK" if shared else "LK_NBLCK",
            )
            while True:
                try:
                    os.lseek(descriptor, _LIFECYCLE_LOCK_OFFSET, os.SEEK_SET)
                    platform_lock.locking(descriptor, mode, 1)
                    acquired = True
                    break
                except OSError as error:
                    if monotonic() >= deadline:
                        raise OperationalStateError(
                            "operational database lifecycle lock is busy"
                        ) from error
                    sleep(0.01)
        else:
            platform_lock = importlib.import_module("fcntl")
            mode = platform_lock.LOCK_SH if shared else platform_lock.LOCK_EX
            while True:
                try:
                    platform_lock.flock(
                        descriptor,
                        mode | platform_lock.LOCK_NB,
                    )
                    acquired = True
                    break
                except OSError as error:
                    if monotonic() >= deadline:
                        raise OperationalStateError(
                            "operational database lifecycle lock is busy"
                        ) from error
                    sleep(0.01)
        yield
    finally:
        if acquired and platform_lock is not None:
            if os.name == "nt":
                os.lseek(descriptor, _LIFECYCLE_LOCK_OFFSET, os.SEEK_SET)
                platform_lock.locking(  # type: ignore[attr-defined]
                    descriptor,
                    platform_lock.LK_UNLCK,  # type: ignore[attr-defined]
                    1,
                )
            else:
                platform_lock.flock(  # type: ignore[attr-defined]
                    descriptor,
                    platform_lock.LOCK_UN,  # type: ignore[attr-defined]
                )
        os.close(descriptor)


def _database_header_uses_wal(path: Path) -> bool:
    try:
        with path.open("rb") as source:
            header = source.read(20)
    except OSError as error:
        raise OperationalStateError("cannot read the operational database header") from error
    return (
        len(header) == 20
        and header.startswith(_SQLITE_HEADER_PREFIX)
        and header[18:20] == b"\x02\x02"
    )


def _migration_table_sql() -> str:
    return """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY CHECK (version > 0),
            name TEXT NOT NULL UNIQUE,
            checksum_sha256 TEXT NOT NULL CHECK (length(checksum_sha256) = 64),
            applied_at TEXT NOT NULL
        )
    """


class OperationalStateStore:
    """One validated SQLite database under a platform-owned private root.

    Connections run in autocommit mode. Bootstrap and migration code may use the private
    ``_transaction`` helper; production mutations must go through typed repositories and the
    private, lease-fenced ``_leased_transaction`` helper. Provider I/O and filesystem work belong
    outside these short transactions.
    """

    def __init__(
        self,
        data_root: PrivateDataRoot,
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not 1 <= busy_timeout_ms <= 60_000:
            raise ValueError("busy_timeout_ms must be between 1 and 60000")
        self._data_root = data_root
        self._busy_timeout_ms = busy_timeout_ms
        self._clock = clock
        self._diagnostic_immutable = False
        self._diagnostic_read_only = False
        self._diagnostic_sidecar_identity: _WalSidecarIdentity | None = None
        sentinel = data_root.validate()
        self._root_id = sentinel.root_id
        operational_directory = data_root.ensure_directory(
            "operational", expected_root_id=sentinel.root_id
        )
        expected_path = data_root.managed_path(
            DATABASE_RELATIVE_PATH, expected_root_id=sentinel.root_id
        )
        if expected_path != operational_directory / DATABASE_RELATIVE_PATH.name:
            raise OperationalStateError(
                "operational database location is not the fixed managed path"
            )
        _validate_database_file(expected_path)
        self._path = expected_path
        self._connection = self._connect(expected_path)
        try:
            _validate_database_file(expected_path)
            data_root.validate(expected_root_id=self._root_id)
            self._migrate(root_id=str(sentinel.root_id))
        except BaseException:
            self.close()
            raise

    @classmethod
    def open(
        cls,
        data_root: PrivateDataRoot,
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
        clock: Callable[[], datetime] = _utc_now,
    ) -> Self:
        """Open, configure, migrate, and bind the fixed database to ``data_root``."""

        return cls(data_root, busy_timeout_ms=busy_timeout_ms, clock=clock)

    @classmethod
    def open_read_only(
        cls,
        data_root: PrivateDataRoot,
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
        clock: Callable[[], datetime] = _utc_now,
    ) -> Self:
        """Open an existing current-schema database without bootstrap, WAL setup, or migration."""

        if not 1 <= busy_timeout_ms <= 60_000:
            raise ValueError("busy_timeout_ms must be between 1 and 60000")
        opened = cls.__new__(cls)
        opened._data_root = data_root
        opened._busy_timeout_ms = busy_timeout_ms
        opened._clock = clock
        opened._diagnostic_read_only = True
        sentinel = data_root.validate()
        opened._root_id = sentinel.root_id
        expected_path = data_root.managed_path(
            DATABASE_RELATIVE_PATH, expected_root_id=sentinel.root_id
        )
        if expected_path != data_root.root / DATABASE_RELATIVE_PATH:
            raise OperationalStateError(
                "operational database location is not the fixed managed path"
            )
        _validate_database_file(expected_path, require_existing=True)
        opened._path = expected_path
        with _database_lifecycle_lock(
            data_root.sentinel_path,
            shared=True,
            timeout_ms=busy_timeout_ms,
        ):
            sidecar_identity = _wal_sidecar_identity(expected_path)
            immutable = sidecar_identity is None
            if immutable and not _database_header_uses_wal(expected_path):
                raise OperationalStateError("operational database header is not in WAL mode")
            opened._diagnostic_immutable = immutable
            opened._diagnostic_sidecar_identity = sidecar_identity
            opened._connection = opened._connect_read_only(expected_path, immutable=immutable)
            try:
                current_sidecars = _wal_sidecar_identity(expected_path)
                if immutable and current_sidecars is not None:
                    opened._connection.close()
                    opened._diagnostic_immutable = False
                    opened._diagnostic_sidecar_identity = current_sidecars
                    opened._connection = opened._connect_read_only(
                        expected_path,
                        immutable=False,
                    )
                elif not immutable and current_sidecars != sidecar_identity:
                    raise OperationalStateError(
                        "operational WAL sidecars changed during diagnostic opening"
                    )
                _validate_database_file(expected_path, require_existing=True)
                data_root.validate(expected_root_id=opened._root_id)
                opened._verify_read_only_schema(root_id=str(sentinel.root_id))
                opened._assert_diagnostic_snapshot_stable()
            except BaseException:
                opened._connection.close()
                raise
        return opened

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def root_id(self) -> str:
        """Return the sentinel identity to bind verified filesystem publication results."""

        return str(self._root_id)

    @property
    def schema_version(self) -> int:
        self._data_root.validate(expected_root_id=self._root_id)
        row = self._connection.execute("PRAGMA user_version").fetchone()
        if row is None:
            raise OperationalSchemaError("SQLite did not report a schema version")
        return int(row[0])

    @property
    def busy_timeout_ms(self) -> int:
        return self._busy_timeout_ms

    def close(self) -> None:
        if self._diagnostic_read_only:
            self._connection.close()
            return
        with _database_lifecycle_lock(
            self._data_root.sentinel_path,
            shared=False,
            timeout_ms=self._busy_timeout_ms,
        ):
            self._connection.close()

    def _now(self) -> datetime:
        return _as_utc(self._clock(), label="clock value")

    def _assert_diagnostic_snapshot_stable(self) -> None:
        current_sidecars = _wal_sidecar_identity(self._path)
        if self._diagnostic_immutable and current_sidecars is not None:
            raise OperationalStateError("operational state changed during diagnostic access")
        if (
            self._diagnostic_read_only
            and not self._diagnostic_immutable
            and current_sidecars != self._diagnostic_sidecar_identity
        ):
            raise OperationalStateError("operational WAL sidecars changed during diagnostic access")

    def _managed_regular_file_is_present(self, relative_path: str) -> bool:
        """Fail closed when cataloged private-root content is absent or link-like."""

        path = self._data_root.managed_path(
            Path(relative_path),
            expected_root_id=self._root_id,
        )
        try:
            details = path.lstat()
        except FileNotFoundError:
            return False
        except OSError:
            return False
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        attributes = getattr(details, "st_file_attributes", 0)
        return (
            stat.S_ISREG(details.st_mode)
            and not path.is_symlink()
            and not bool(reparse_flag and attributes & reparse_flag)
            and details.st_nlink == 1
        )

    def _managed_file_matches_catalog(
        self,
        relative_path: str,
        *,
        expected_sha256: str,
        expected_bytes: int,
    ) -> bool:
        """Stream-check current bytes against their already verified catalog entry."""

        if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256) or expected_bytes < 0:
            return False
        try:
            digest, byte_count = self._managed_file_integrity(relative_path)
        except OperationalStateError:
            return False
        # A matching digest proves the bytes are unchanged since the catalog's
        # publication-time Parquet reopen/schema verification.
        return digest == expected_sha256 and byte_count == expected_bytes

    def _managed_file_integrity(self, relative_path: str) -> tuple[str, int]:
        """Return a safe managed file's streamed digest and size.

        Typed repositories use this only after storage-layer semantic verification.  It binds the
        current immutable file bytes into SQLite without trusting caller-supplied manifest hashes.
        """

        if not self._managed_regular_file_is_present(relative_path):
            raise OperationalStateError("managed catalog file is absent or unsafe")
        path = self._data_root.managed_path(
            Path(relative_path),
            expected_root_id=self._root_id,
        )
        digest = hashlib.sha256()
        byte_count = 0
        try:
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
                    byte_count += len(chunk)
        except OSError as error:
            raise OperationalStateError("managed catalog file cannot be read safely") from error
        self._data_root.validate(expected_root_id=self._root_id)
        if not self._managed_regular_file_is_present(relative_path):
            raise OperationalStateError("managed catalog file changed during verification")
        return digest.hexdigest(), byte_count

    def _read_managed_file_bounded(
        self,
        relative_path: str,
        *,
        max_bytes: int,
    ) -> bytes:
        """Read one small verified metadata file without offering a payload-loading API."""

        if not 1 <= max_bytes <= 16 * 1024 * 1024:
            raise ValueError("managed metadata read limit must be between 1 byte and 16 MiB")
        if not self._managed_regular_file_is_present(relative_path):
            raise OperationalStateError("managed metadata file is absent or unsafe")
        path = self._data_root.managed_path(
            Path(relative_path),
            expected_root_id=self._root_id,
        )
        try:
            size = path.stat().st_size
            if size > max_bytes:
                raise OperationalStateError("managed metadata file exceeds its bounded limit")
            content = path.read_bytes()
        except OSError as error:
            raise OperationalStateError("managed metadata file cannot be read safely") from error
        self._data_root.validate(expected_root_id=self._root_id)
        if len(content) != size or not self._managed_regular_file_is_present(relative_path):
            raise OperationalStateError("managed metadata file changed during bounded read")
        return content

    def _connect(self, path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(
            path,
            timeout=self._busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
            journal_row = self._enable_wal_mode(connection)
            if journal_row is None or str(journal_row[0]).casefold() != "wal":
                raise OperationalStateError("SQLite WAL mode could not be enabled")
            connection.execute("PRAGMA synchronous = FULL")
            self._verify_connection_contract(connection)
        except BaseException:
            connection.close()
            raise
        return connection

    def _connect_read_only(
        self,
        path: Path,
        *,
        immutable: bool,
    ) -> sqlite3.Connection:
        immutable_parameter = "&immutable=1" if immutable else ""
        try:
            connection = sqlite3.connect(
                f"{path.as_uri()}?mode=ro{immutable_parameter}",
                uri=True,
                timeout=self._busy_timeout_ms / 1_000,
                isolation_level=None,
            )
        except sqlite3.DatabaseError as error:
            raise OperationalStateError(
                "operational database could not be opened read-only"
            ) from error
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
            connection.execute("PRAGMA query_only = ON")
            self._verify_connection_contract(connection, immutable=immutable)
        except BaseException:
            connection.close()
            raise
        return connection

    def _enable_wal_mode(self, connection: sqlite3.Connection) -> sqlite3.Row | None:
        deadline = monotonic() + (self._busy_timeout_ms / 1_000)
        while True:
            try:
                return cast(
                    sqlite3.Row | None,
                    connection.execute("PRAGMA journal_mode = WAL").fetchone(),
                )
            except sqlite3.OperationalError as error:
                error_code = getattr(error, "sqlite_errorcode", None)
                primary_code = error_code & 0xFF if isinstance(error_code, int) else None
                if primary_code not in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                    raise
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise
                sleep(min(0.01, remaining))

    def _verify_connection_contract(
        self,
        connection: sqlite3.Connection,
        *,
        immutable: bool = False,
    ) -> None:
        foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()
        synchronous = connection.execute("PRAGMA synchronous").fetchone()
        busy_timeout = connection.execute("PRAGMA busy_timeout").fetchone()
        if foreign_keys is None or int(foreign_keys[0]) != 1:
            raise OperationalStateError("SQLite foreign-key enforcement is not active")
        expected_journal_mode = "delete" if immutable else "wal"
        if journal_mode is None or str(journal_mode[0]).casefold() != expected_journal_mode:
            raise OperationalStateError("SQLite journal mode is not WAL")
        if synchronous is None or int(synchronous[0]) != 2:
            raise OperationalStateError("SQLite synchronous mode is not FULL")
        if busy_timeout is None or int(busy_timeout[0]) != self._busy_timeout_ms:
            raise OperationalStateError("SQLite busy timeout differs from the configured limit")

    def _migrate(self, *, root_id: str) -> None:
        try:
            # The authoritative schema reads belong after BEGIN IMMEDIATE. A concurrent first
            # opener may have completed migration while this connection waited for the lock.
            with self._transaction(write=True) as connection:
                user_version_row = connection.execute("PRAGMA user_version").fetchone()
                application_row = connection.execute("PRAGMA application_id").fetchone()
                if user_version_row is None:
                    raise OperationalSchemaError("SQLite did not report user_version")
                if application_row is None:
                    raise OperationalSchemaError("SQLite did not report application_id")
                user_version = int(user_version_row[0])
                application_id = int(application_row[0])
                if user_version > LATEST_SCHEMA_VERSION:
                    raise OperationalSchemaTooNewError(
                        f"operational schema {user_version} is newer than supported "
                        f"{LATEST_SCHEMA_VERSION}"
                    )
                if application_id not in (0, _APPLICATION_ID):
                    raise OperationalSchemaError(
                        "database belongs to a different SQLite application"
                    )
                if application_id == 0 and user_version > 0:
                    raise OperationalSchemaError(
                        "versioned operational database is missing its application identity"
                    )
                connection.execute(_migration_table_sql())
                rows = connection.execute(
                    """
                    SELECT version, name, checksum_sha256
                    FROM schema_migrations
                    ORDER BY version
                    """
                ).fetchall()
                applied_versions = [int(row["version"]) for row in rows]
                expected_prefix = list(range(1, len(applied_versions) + 1))
                if applied_versions != expected_prefix:
                    raise OperationalSchemaError(
                        "schema migration history is not a contiguous prefix"
                    )
                if applied_versions and applied_versions[-1] > LATEST_SCHEMA_VERSION:
                    raise OperationalSchemaTooNewError(
                        "schema migration history contains a newer application version"
                    )
                if user_version != (applied_versions[-1] if applied_versions else 0):
                    raise OperationalSchemaError(
                        "SQLite user_version and migration history disagree"
                    )
                was_fresh = not applied_versions

                for row, expected in zip(rows, MIGRATIONS[: len(applied_versions)], strict=True):
                    if (
                        str(row["name"]) != expected.name
                        or str(row["checksum_sha256"]) != expected.checksum_sha256
                    ):
                        raise OperationalSchemaError(
                            f"schema migration {expected.version} identity does not match source"
                        )

                if application_id == 0:
                    connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
                for migration in MIGRATIONS[len(applied_versions) :]:
                    for statement in migration.statements:
                        connection.execute(statement)
                    connection.execute(
                        """
                        INSERT INTO schema_migrations(
                            version, name, checksum_sha256, applied_at
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (
                            migration.version,
                            migration.name,
                            migration.checksum_sha256,
                            _format_utc(self._now()),
                        ),
                    )
                    connection.execute(f"PRAGMA user_version = {migration.version}")

                metadata = connection.execute(
                    """
                    SELECT root_id, schema_version
                    FROM store_metadata
                    WHERE singleton = 1
                    """
                ).fetchone()
                if metadata is None:
                    if not was_fresh:
                        raise OperationalSchemaError(
                            "versioned operational database is missing root metadata"
                        )
                    connection.execute(
                        """
                        INSERT INTO store_metadata(singleton, root_id, created_at, schema_version)
                        VALUES (1, ?, ?, ?)
                        """,
                        (root_id, _format_utc(self._now()), LATEST_SCHEMA_VERSION),
                    )
                elif str(metadata["root_id"]) != root_id:
                    raise OperationalSchemaError(
                        "operational database is bound to a different private-root ID"
                    )
                else:
                    if int(metadata["schema_version"]) != user_version:
                        raise OperationalSchemaError(
                            "store metadata and migration history disagree"
                        )
                    connection.execute(
                        "UPDATE store_metadata SET schema_version = ? WHERE singleton = 1",
                        (LATEST_SCHEMA_VERSION,),
                    )
        except sqlite3.DatabaseError as error:
            raise OperationalSchemaError(
                "operational schema migration failed atomically"
            ) from error

    def _verify_read_only_schema(self, *, root_id: str) -> None:
        try:
            user_version_row = self._connection.execute("PRAGMA user_version").fetchone()
            application_row = self._connection.execute("PRAGMA application_id").fetchone()
            if user_version_row is None or application_row is None:
                raise OperationalSchemaError("SQLite did not report schema identity")
            user_version = int(user_version_row[0])
            if user_version > LATEST_SCHEMA_VERSION:
                raise OperationalSchemaTooNewError(
                    f"operational schema {user_version} is newer than supported "
                    f"{LATEST_SCHEMA_VERSION}"
                )
            if user_version != LATEST_SCHEMA_VERSION:
                raise OperationalSchemaError(
                    "operational schema requires migration; diagnostics are read-only"
                )
            if int(application_row[0]) != _APPLICATION_ID:
                raise OperationalSchemaError("database has an invalid SQLite application identity")

            rows = self._connection.execute(
                """
                SELECT version, name, checksum_sha256
                FROM schema_migrations
                ORDER BY version
                """
            ).fetchall()
            if len(rows) != len(MIGRATIONS):
                raise OperationalSchemaError("schema migration history is incomplete")
            for row, expected in zip(rows, MIGRATIONS, strict=True):
                if (
                    int(row["version"]) != expected.version
                    or str(row["name"]) != expected.name
                    or str(row["checksum_sha256"]) != expected.checksum_sha256
                ):
                    raise OperationalSchemaError(
                        f"schema migration {expected.version} identity does not match source"
                    )

            metadata = self._connection.execute(
                """
                SELECT root_id, schema_version
                FROM store_metadata
                WHERE singleton = 1
                """
            ).fetchone()
            if metadata is None:
                raise OperationalSchemaError("operational database is missing root metadata")
            if str(metadata["root_id"]) != root_id:
                raise OperationalSchemaError(
                    "operational database is bound to a different private-root ID"
                )
            if int(metadata["schema_version"]) != LATEST_SCHEMA_VERSION:
                raise OperationalSchemaError("store metadata and migration history disagree")
        except OperationalSchemaError:
            raise
        except sqlite3.DatabaseError as error:
            raise OperationalSchemaError("operational schema verification failed") from error

    @contextmanager
    def _transaction(self, *, write: bool = True) -> Iterator[sqlite3.Connection]:
        """Internal transaction primitive; public mutations are typed and lease-fenced."""

        if self._connection.in_transaction:
            raise OperationalTransactionError("nested operational transactions are not supported")
        self._data_root.validate(expected_root_id=self._root_id)
        self._connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield self._connection
        except BaseException:
            self._connection.rollback()
            raise
        else:
            try:
                self._connection.commit()
            except BaseException:
                self._connection.rollback()
                raise

    @contextmanager
    def _leased_transaction(self, lease: WriterLease) -> Iterator[sqlite3.Connection]:
        """Fence one typed mutation by owner/generation inside the same write transaction."""

        _validate_phase2_lease_name(lease.lease_name)
        _validate_identifier(lease.owner_id, label="owner_id")
        with self._transaction(write=True) as connection:
            now = self._now()
            row = connection.execute(
                """
                SELECT owner_id, generation, state, heartbeat_at, expires_at
                FROM writer_leases
                WHERE lease_name = ?
                """,
                (lease.lease_name,),
            ).fetchone()
            if (
                row is None
                or str(row["state"]) != "ACTIVE"
                or str(row["owner_id"]) != lease.owner_id
                or int(row["generation"]) != lease.generation
                or _parse_utc(str(row["expires_at"])) <= now
                or now < _parse_utc(str(row["heartbeat_at"]))
            ):
                raise WriterLeaseLostError(
                    "writer lease does not authorize this operational mutation"
                )
            yield connection
            # Re-fence immediately before the enclosing transaction commits. A
            # mutation that outlives its generation or TTL must roll back in full.
            final_now = self._now()
            final_row = connection.execute(
                """
                SELECT owner_id, generation, state, heartbeat_at, expires_at
                FROM writer_leases
                WHERE lease_name = ?
                """,
                (lease.lease_name,),
            ).fetchone()
            if (
                final_row is None
                or str(final_row["state"]) != "ACTIVE"
                or str(final_row["owner_id"]) != lease.owner_id
                or int(final_row["generation"]) != lease.generation
                or _parse_utc(str(final_row["expires_at"])) <= final_now
                or final_now < _parse_utc(str(final_row["heartbeat_at"]))
                or final_now < now
            ):
                raise WriterLeaseLostError(
                    "writer lease expired or changed before operational commit"
                )

    @contextmanager
    def read_only_connection(self) -> Iterator[sqlite3.Connection]:
        """Open an independent read-only status connection while the writer remains active."""

        self._data_root.validate(expected_root_id=self._root_id)
        _validate_database_file(self._path)
        self._assert_diagnostic_snapshot_stable()
        immutable = self._diagnostic_immutable
        connection = self._connect_read_only(
            self._path,
            immutable=immutable,
        )
        try:
            yield connection
        finally:
            connection.close()
        self._assert_diagnostic_snapshot_stable()

    def diagnostics(self) -> OperationalDiagnostics:
        """Run SQLite integrity and foreign-key checks without exposing private values."""

        self._data_root.validate(expected_root_id=self._root_id)
        self._assert_diagnostic_snapshot_stable()
        integrity_messages = tuple(
            str(row[0]) for row in self._connection.execute("PRAGMA integrity_check").fetchall()
        )
        violations = self._connection.execute("PRAGMA foreign_key_check").fetchall()
        journal_mode = self._connection.execute("PRAGMA journal_mode").fetchone()
        synchronous = self._connection.execute("PRAGMA synchronous").fetchone()
        busy_timeout = self._connection.execute("PRAGMA busy_timeout").fetchone()
        if journal_mode is None or synchronous is None or busy_timeout is None:
            raise OperationalStateError("SQLite did not return connection diagnostics")
        self._assert_diagnostic_snapshot_stable()
        return OperationalDiagnostics(
            database_path=self._path,
            schema_version=self.schema_version,
            journal_mode=("wal" if self._diagnostic_immutable else str(journal_mode[0]).casefold()),
            synchronous=int(synchronous[0]),
            busy_timeout_ms=int(busy_timeout[0]),
            integrity_messages=integrity_messages,
            foreign_key_violations=len(violations),
        )

    def get_writer_lease(self) -> WriterLease | None:
        """Return the active lease handle, or ``None`` when no writer is active."""

        lease_name = DEFAULT_WRITER_LEASE_NAME
        self._data_root.validate(expected_root_id=self._root_id)
        self._assert_diagnostic_snapshot_stable()
        row = self._connection.execute(
            "SELECT * FROM writer_leases WHERE lease_name = ?",
            (lease_name,),
        ).fetchone()
        self._assert_diagnostic_snapshot_stable()
        if (
            row is None
            or str(row["state"]) != "ACTIVE"
            or _parse_utc(str(row["expires_at"])) <= self._now()
        ):
            return None
        return self._lease_from_row(row)

    def acquire_writer_lease(
        self,
        owner_id: str,
        ttl: timedelta,
    ) -> WriterLease:
        """Acquire the one writer lease, safely taking over only an expired generation."""

        owner_id = _validate_identifier(owner_id, label="owner_id")
        lease_name = DEFAULT_WRITER_LEASE_NAME
        ttl = _validate_lease_ttl(ttl)
        with self._transaction(write=True) as connection:
            now = self._now()
            expires_at = now + ttl
            row = connection.execute(
                "SELECT * FROM writer_leases WHERE lease_name = ?",
                (lease_name,),
            ).fetchone()
            event_type = "ACQUIRED"
            previous_owner_id: str | None = None
            if row is None:
                generation = 1
                connection.execute(
                    """
                    INSERT INTO writer_leases(
                        lease_name, owner_id, generation, state, acquired_at,
                        heartbeat_at, expires_at, released_at, previous_owner_id
                    ) VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?, NULL, NULL)
                    """,
                    (
                        lease_name,
                        owner_id,
                        generation,
                        _format_utc(now),
                        _format_utc(now),
                        _format_utc(expires_at),
                    ),
                )
            else:
                existing_owner = str(row["owner_id"])
                existing_generation = int(row["generation"])
                existing_active = str(row["state"]) == "ACTIVE"
                existing_expiry = _parse_utc(str(row["expires_at"]))
                if existing_active and existing_expiry > now:
                    if existing_owner == owner_id:
                        return self._lease_from_row(row)
                    raise WriterLeaseBusyError(
                        f"writer lease {lease_name!r} is held by another live owner"
                    )
                generation = existing_generation + 1
                previous_owner_id = existing_owner
                if existing_active:
                    event_type = "STALE_TAKEOVER"
                connection.execute(
                    """
                    UPDATE writer_leases
                    SET owner_id = ?, generation = ?, state = 'ACTIVE', acquired_at = ?,
                        heartbeat_at = ?, expires_at = ?, released_at = NULL,
                        previous_owner_id = ?
                    WHERE lease_name = ?
                    """,
                    (
                        owner_id,
                        generation,
                        _format_utc(now),
                        _format_utc(now),
                        _format_utc(expires_at),
                        previous_owner_id,
                        lease_name,
                    ),
                )
            connection.execute(
                """
                INSERT INTO writer_lease_events(
                    lease_name, event_type, owner_id, previous_owner_id, generation, occurred_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    lease_name,
                    event_type,
                    owner_id,
                    previous_owner_id,
                    generation,
                    _format_utc(now),
                ),
            )
            updated = connection.execute(
                "SELECT * FROM writer_leases WHERE lease_name = ?",
                (lease_name,),
            ).fetchone()
            if updated is None:
                raise WriterLeaseError("writer lease disappeared during acquisition")
            return self._lease_from_row(updated)

    def renew_writer_lease(self, lease: WriterLease, ttl: timedelta) -> WriterLease:
        """Heartbeat a generation-bound lease without allowing resurrection after takeover."""

        _validate_phase2_lease_name(lease.lease_name)
        _validate_identifier(lease.owner_id, label="owner_id")
        ttl = _validate_lease_ttl(ttl)
        with self._transaction(write=True) as connection:
            now = self._now()
            expires_at = now + ttl
            row = connection.execute(
                "SELECT * FROM writer_leases WHERE lease_name = ?",
                (lease.lease_name,),
            ).fetchone()
            if (
                row is None
                or str(row["state"]) != "ACTIVE"
                or str(row["owner_id"]) != lease.owner_id
                or int(row["generation"]) != lease.generation
                or _parse_utc(str(row["expires_at"])) <= now
            ):
                raise WriterLeaseLostError("writer lease generation is no longer active")
            if now < _parse_utc(str(row["heartbeat_at"])):
                raise WriterLeaseError("writer lease clock moved backwards")
            connection.execute(
                """
                UPDATE writer_leases
                SET heartbeat_at = ?, expires_at = ?
                WHERE lease_name = ? AND owner_id = ? AND generation = ? AND state = 'ACTIVE'
                """,
                (
                    _format_utc(now),
                    _format_utc(expires_at),
                    lease.lease_name,
                    lease.owner_id,
                    lease.generation,
                ),
            )
            connection.execute(
                """
                INSERT INTO writer_lease_events(
                    lease_name, event_type, owner_id, previous_owner_id, generation, occurred_at
                ) VALUES (?, 'RENEWED', ?, NULL, ?, ?)
                """,
                (lease.lease_name, lease.owner_id, lease.generation, _format_utc(now)),
            )
            updated = connection.execute(
                "SELECT * FROM writer_leases WHERE lease_name = ?",
                (lease.lease_name,),
            ).fetchone()
            if updated is None:
                raise WriterLeaseLostError("writer lease disappeared during renewal")
            return self._lease_from_row(updated)

    def release_writer_lease(self, lease: WriterLease) -> bool:
        """Release a matching lease generation; repeated release is an idempotent no-op."""

        _validate_phase2_lease_name(lease.lease_name)
        _validate_identifier(lease.owner_id, label="owner_id")
        with self._transaction(write=True) as connection:
            now = self._now()
            row = connection.execute(
                "SELECT * FROM writer_leases WHERE lease_name = ?",
                (lease.lease_name,),
            ).fetchone()
            if row is None:
                raise WriterLeaseLostError("writer lease does not exist")
            matches = (
                str(row["owner_id"]) == lease.owner_id
                and int(row["generation"]) == lease.generation
            )
            if not matches:
                raise WriterLeaseLostError("writer lease generation is no longer active")
            if str(row["state"]) == "RELEASED":
                return False
            if _parse_utc(str(row["expires_at"])) <= now:
                raise WriterLeaseLostError("writer lease generation has expired")
            if now < _parse_utc(str(row["heartbeat_at"])):
                raise WriterLeaseError("writer lease clock moved backwards")
            connection.execute(
                """
                UPDATE writer_leases
                SET state = 'RELEASED', released_at = ?, expires_at = ?
                WHERE lease_name = ? AND owner_id = ? AND generation = ? AND state = 'ACTIVE'
                """,
                (
                    _format_utc(now),
                    _format_utc(now),
                    lease.lease_name,
                    lease.owner_id,
                    lease.generation,
                ),
            )
            connection.execute(
                """
                INSERT INTO writer_lease_events(
                    lease_name, event_type, owner_id, previous_owner_id, generation, occurred_at
                ) VALUES (?, 'RELEASED', ?, NULL, ?, ?)
                """,
                (lease.lease_name, lease.owner_id, lease.generation, _format_utc(now)),
            )
            return True

    @staticmethod
    def _lease_from_row(row: sqlite3.Row) -> WriterLease:
        _validate_phase2_lease_name(str(row["lease_name"]))
        return WriterLease(
            lease_name=str(row["lease_name"]),
            owner_id=str(row["owner_id"]),
            generation=int(row["generation"]),
            acquired_at=_parse_utc(str(row["acquired_at"])),
            heartbeat_at=_parse_utc(str(row["heartbeat_at"])),
            expires_at=_parse_utc(str(row["expires_at"])),
        )


__all__ = [
    "DATABASE_RELATIVE_PATH",
    "DEFAULT_BUSY_TIMEOUT_MS",
    "DEFAULT_WRITER_LEASE_NAME",
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
]
