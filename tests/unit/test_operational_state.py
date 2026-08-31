"""Offline tests for the Phase 2 SQLite operational-state boundary."""

from __future__ import annotations

import inspect
import json
import sqlite3
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest

import investment_platform.data.operational.store as store_module
from investment_platform.data.operational import (
    DATABASE_RELATIVE_PATH,
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    Migration,
    OperationalSchemaError,
    OperationalSchemaTooNewError,
    OperationalStateError,
    OperationalStateStore,
    OperationalTransactionError,
    WriterLeaseBusyError,
    WriterLeaseError,
    WriterLeaseLostError,
)
from investment_platform.data_root import (
    PrivateDataRoot,
    PrivateDataRootSentinelError,
    UnsafePrivateDataRootError,
)

pytestmark = pytest.mark.unit

_START = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
_HASH_A = "a" * 64
_HASH_B = "b" * 64


@dataclass
class MutableClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value

    def advance(self, delta: timedelta) -> None:
        self.value += delta


@pytest.fixture
def repository_root() -> Path:
    return Path(__file__).parents[2].resolve(strict=True)


@pytest.fixture
def private_root(tmp_path: Path, repository_root: Path) -> PrivateDataRoot:
    root = PrivateDataRoot(
        tmp_path / "dedicated-operational-state-root",
        repository_root,
        allow_temporary_for_tests=True,
    )
    root.initialize(created_at=_START)
    return root


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(_START)


@pytest.fixture
def store(
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> Generator[OperationalStateStore]:
    opened = OperationalStateStore.open(private_root, clock=clock)
    yield opened
    opened.close()


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _insert_policy_snapshot(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT INTO policy_snapshots(
            policy_snapshot_id, policy_id, revision, policy_hash, provider, dataset,
            retention_mode, verified_at, captured_at
        ) VALUES (
            'policy-snapshot', 'policy', 1, ?, 'synthetic', 'price_bars',
            'SYNTHETIC_UNRESTRICTED', ?, ?
        )
        """,
        (_HASH_A, _START.isoformat(), _START.isoformat()),
    )


def _insert_run(connection: sqlite3.Connection) -> None:
    _insert_policy_snapshot(connection)
    connection.execute(
        """
        INSERT INTO ingestion_runs(
            run_id, mode, environment, provider, dataset, status,
            policy_snapshot_id, created_at
        ) VALUES (
            'run-1', 'BACKFILL', 'test', 'synthetic', 'price_bars', 'PLANNED',
            'policy-snapshot', ?
        )
        """,
        (_START.isoformat(),),
    )


def test_fresh_database_uses_exact_managed_location_and_complete_schema(
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    with OperationalStateStore.open(private_root, clock=clock) as opened:
        assert opened.path == private_root.root / DATABASE_RELATIVE_PATH
        assert opened.path.is_file()
        assert opened.schema_version == LATEST_SCHEMA_VERSION == 1
        with opened.read_only_connection() as connection:
            tables = _table_names(connection)

    assert {
        "schema_migrations",
        "store_metadata",
        "ingestion_runs",
        "request_specs",
        "request_instances",
        "request_attempts",
        "stream_keys",
        "batch_contexts",
        "raw_artifacts",
        "attempt_artifact_observations",
        "canonical_batches",
        "canonical_files",
        "canonical_batch_streams",
        "calendar_snapshots",
        "calendar_sessions",
        "coverage_segments",
        "gaps",
        "watermarks",
        "retry_state",
        "errors",
        "provider_budget_state",
        "dataset_policy_status",
        "policy_snapshots",
        "purge_runs",
        "purge_targets",
        "writer_leases",
        "writer_lease_events",
    } <= tables


def test_reopen_is_idempotent_and_preserves_committed_state(
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    with OperationalStateStore.open(private_root, clock=clock) as first:
        with first._transaction() as connection:
            connection.execute(
                """
                INSERT INTO stream_keys(
                    stream_id, stream_hash, provider, dataset, instrument_id,
                    timeframe, session, adjustment, created_at
                ) VALUES ('stream-1', ?, 'synthetic', 'price_bars', 'instrument-1',
                          '5m', 'rth', 'raw', ?)
                """,
                (_HASH_A, _START.isoformat()),
            )
        migration_rows = first._connection.execute(
            "SELECT version, checksum_sha256 FROM schema_migrations"
        ).fetchall()

    clock.advance(timedelta(hours=1))
    with OperationalStateStore.open(private_root, clock=clock) as second:
        assert second.schema_version == 1
        count = second._connection.execute(
            "SELECT count(*) FROM stream_keys WHERE stream_id = 'stream-1'"
        ).fetchone()
        reopened_migrations = second._connection.execute(
            "SELECT version, checksum_sha256 FROM schema_migrations"
        ).fetchall()

    assert count is not None and int(count[0]) == 1
    assert [tuple(row) for row in reopened_migrations] == [tuple(row) for row in migration_rows]


def test_two_first_openers_serialize_authoritative_migration_reads(
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    barrier = Barrier(2)

    def open_once() -> tuple[int, int]:
        barrier.wait(timeout=5)
        with OperationalStateStore.open(private_root, clock=clock) as opened:
            with opened.read_only_connection() as connection:
                migration_count = int(
                    connection.execute("SELECT count(*) FROM schema_migrations").fetchone()[0]
                )
            return opened.schema_version, migration_count

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(lambda _: open_once(), range(2)))

    assert results == ((1, 1), (1, 1))


def test_connection_contract_and_integrity_diagnostics(store: OperationalStateStore) -> None:
    diagnostics = store.diagnostics()

    assert diagnostics.healthy
    assert diagnostics.database_path == store.path
    assert diagnostics.schema_version == 1
    assert diagnostics.journal_mode == "wal"
    assert diagnostics.synchronous == 2
    assert diagnostics.busy_timeout_ms == 5_000
    assert diagnostics.integrity_messages == ("ok",)
    assert diagnostics.foreign_key_violations == 0
    with store.read_only_connection() as connection:
        assert int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) == 1
        assert str(connection.execute("PRAGMA journal_mode").fetchone()[0]).casefold() == "wal"


def test_invalid_busy_timeout_and_naive_clock_fail_closed(
    private_root: PrivateDataRoot,
) -> None:
    with pytest.raises(ValueError, match="busy_timeout"):
        OperationalStateStore.open(private_root, busy_timeout_ms=0)

    naive_clock = MutableClock(datetime(2026, 8, 31, 12, 0))
    with pytest.raises(OperationalStateError, match="timezone-aware"):
        OperationalStateStore.open(private_root, clock=naive_clock)


def test_future_schema_is_refused_without_mutation(
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    with OperationalStateStore.open(private_root, clock=clock) as opened:
        database_path = opened.path
    with sqlite3.connect(database_path, isolation_level=None) as connection:
        connection.execute(f"PRAGMA user_version = {LATEST_SCHEMA_VERSION + 1}")

    with pytest.raises(OperationalSchemaTooNewError, match="newer than supported"):
        OperationalStateStore.open(private_root, clock=clock)

    with sqlite3.connect(database_path) as connection:
        assert int(connection.execute("PRAGMA user_version").fetchone()[0]) == 2


def test_migration_identity_tampering_is_refused(
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    with OperationalStateStore.open(private_root, clock=clock) as opened:
        database_path = opened.path
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE schema_migrations SET checksum_sha256 = ? WHERE version = 1",
            (_HASH_B,),
        )

    with pytest.raises(OperationalSchemaError, match="identity does not match"):
        OperationalStateStore.open(private_root, clock=clock)


def test_failed_forward_migration_rolls_back_ddl_and_version(
    monkeypatch: pytest.MonkeyPatch,
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    with OperationalStateStore.open(private_root, clock=clock) as opened:
        database_path = opened.path
    failing = Migration(
        version=2,
        name="synthetic_failing_migration",
        statements=(
            "CREATE TABLE must_rollback(value TEXT)",
            "THIS IS NOT VALID SQL",
        ),
    )
    monkeypatch.setattr(store_module, "LATEST_SCHEMA_VERSION", 2)
    monkeypatch.setattr(store_module, "MIGRATIONS", (*MIGRATIONS, failing))

    with pytest.raises(OperationalSchemaError, match="failed atomically"):
        OperationalStateStore.open(private_root, clock=clock)

    with sqlite3.connect(database_path) as connection:
        assert int(connection.execute("PRAGMA user_version").fetchone()[0]) == 1
        assert "must_rollback" not in _table_names(connection)
        rows = connection.execute("SELECT version FROM schema_migrations").fetchall()
        assert rows == [(1,)]


def test_database_cannot_be_redirected_outside_the_validated_root(
    tmp_path: Path,
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    operational = private_root.ensure_directory("operational")
    outside = tmp_path / "outside-operational.sqlite3"
    sqlite3.connect(outside).close()
    redirected = operational / DATABASE_RELATIVE_PATH.name
    try:
        redirected.symlink_to(outside)
    except OSError as error:
        pytest.skip(f"file symlinks are unavailable: {error}")

    with pytest.raises((OperationalStateError, UnsafePrivateDataRootError), match="symlink"):
        OperationalStateStore.open(private_root, clock=clock)


def test_store_refuses_any_managed_path_resolution_outside_operational_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    outside = tmp_path / "outside-operational.sqlite3"

    def redirected_path(
        _relative_path: str | Path,
        *,
        expected_root_id: object | None = None,
    ) -> Path:
        del expected_root_id
        return outside

    monkeypatch.setattr(private_root, "managed_path", redirected_path)

    with pytest.raises(OperationalStateError, match="fixed managed path"):
        OperationalStateStore.open(private_root, clock=clock)


def test_schema_contains_metadata_not_market_values_or_response_bytes(
    store: OperationalStateStore,
) -> None:
    forbidden_columns = {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
        "payload",
        "response_body",
        "authorization",
        "api_key",
        "secret",
    }
    with store.read_only_connection() as connection:
        tables = _table_names(connection) - {"sqlite_sequence"}
        all_columns = {
            str(row[1]).casefold()
            for table in tables
            for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
        }
        schema_text = "\n".join(
            str(row[0])
            for row in connection.execute(
                "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL"
            ).fetchall()
        ).casefold()

    assert forbidden_columns.isdisjoint(all_columns)
    assert "payload" not in schema_text
    assert "response_body" not in schema_text
    assert "api_secret" not in schema_text
    assert "ohlcv" not in schema_text


def test_foreign_key_unique_check_and_relative_path_constraints(
    store: OperationalStateStore,
) -> None:
    with (
        pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"),
        store._transaction() as connection,
    ):
        connection.execute(
            """
            INSERT INTO request_spec_streams(
                request_spec_id, stream_id, provider_identifier, ordinal
            ) VALUES ('missing-request', 'missing-stream', 'SYNTHETIC', 0)
            """
        )

    with store._transaction() as connection:
        connection.execute(
            """
            INSERT INTO stream_keys(
                stream_id, stream_hash, provider, dataset, instrument_id,
                timeframe, session, adjustment, created_at
            ) VALUES ('stream-1', ?, 'synthetic', 'price_bars', 'instrument-1',
                      '5m', 'rth', 'raw', ?)
            """,
            (_HASH_A, _START.isoformat()),
        )
    with (
        pytest.raises(sqlite3.IntegrityError, match="UNIQUE"),
        store._transaction() as connection,
    ):
        connection.execute(
            """
            INSERT INTO stream_keys(
                stream_id, stream_hash, provider, dataset, instrument_id,
                timeframe, session, adjustment, created_at
            ) VALUES ('stream-2', ?, 'synthetic', 'other', 'instrument-2',
                      '1d', 'rth', 'raw', ?)
            """,
            (_HASH_A, _START.isoformat()),
        )

    with (
        pytest.raises(sqlite3.IntegrityError, match="CHECK"),
        store._transaction() as connection,
    ):
        connection.execute(
            """
            INSERT INTO request_specs(
                request_spec_id, request_spec_hash, provider, dataset,
                interval_start, interval_end, mapping_version,
                specification_json, created_at
            ) VALUES ('request', ?, 'synthetic', 'price_bars',
                      '2026-01-02', '2026-01-01', 'v1', '{}', ?)
            """,
            (_HASH_B, _START.isoformat()),
        )


def test_verified_empty_requires_complete_request_pagination_and_provider_semantics(
    store: OperationalStateStore,
) -> None:
    ddl = store._connection.execute(
        "SELECT sql FROM sqlite_master WHERE name = 'coverage_segments'"
    ).fetchone()

    assert ddl is not None
    normalized = " ".join(str(ddl[0]).split())
    assert "classification <> 'VERIFIED_EMPTY'" in normalized
    assert "request_completed = 1" in normalized
    assert "pagination_verified = 1" in normalized
    assert "provider_semantics_version IS NOT NULL" in normalized
    assert "row_count" in normalized
    assert "artifact_count" in normalized


def test_attempt_cannot_be_raw_complete_without_verified_pagination(
    store: OperationalStateStore,
) -> None:
    with store._transaction() as connection:
        _insert_run(connection)
        connection.execute(
            """
            INSERT INTO request_specs(
                request_spec_id, request_spec_hash, provider, dataset,
                interval_start, interval_end, mapping_version,
                specification_json, created_at
            ) VALUES ('request-spec', ?, 'synthetic', 'price_bars',
                      '2026-01-01', '2026-01-02', 'v1', '{}', ?)
            """,
            (_HASH_B, _START.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO request_instances(
                request_instance_id, run_id, request_spec_id, intent,
                reason, plan_ordinal, status, created_at
            ) VALUES ('request-instance', 'run-1', 'request-spec', 'BACKFILL',
                      'synthetic acceptance', 0, 'PLANNED', ?)
            """,
            (_START.isoformat(),),
        )
        connection.execute(
            """
            INSERT INTO request_attempts(
                attempt_id, request_instance_id, attempt_number, status
            ) VALUES ('attempt-1', 'request-instance', 1, 'PLANNED')
            """
        )
        connection.execute(
            "UPDATE request_attempts SET status = 'RUNNING' WHERE attempt_id = 'attempt-1'"
        )

    with (
        pytest.raises(sqlite3.IntegrityError, match="CHECK"),
        store._transaction() as connection,
    ):
        connection.execute(
            "UPDATE request_attempts SET status = 'RAW_COMPLETE' WHERE attempt_id = 'attempt-1'"
        )

    with store._transaction() as connection:
        connection.execute(
            """
            UPDATE request_attempts
            SET status = 'RAW_COMPLETE', page_count = 1,
                pagination_complete = 1, terminal_page_verified = 1
            WHERE attempt_id = 'attempt-1'
            """
        )


def test_calendar_checksum_and_watermark_session_schema_match_adapter_contract(
    store: OperationalStateStore,
) -> None:
    with store.read_only_connection() as connection:
        calendar_ddl = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'calendar_snapshots'"
        ).fetchone()
        watermark_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(watermarks)").fetchall()
        }

    assert calendar_ddl is not None
    normalized = " ".join(str(calendar_ddl[0]).split())
    assert "length(schedule_checksum) = 71" in normalized
    assert "substr(schedule_checksum, 1, 7) = 'sha256:'" in normalized
    assert "substr(schedule_checksum, 8) NOT GLOB '*[^0-9a-f]*'" in normalized
    assert "last_verified_session" in watermark_columns

    with (
        pytest.raises(sqlite3.IntegrityError, match="CHECK"),
        store._transaction() as write_connection,
    ):
        write_connection.execute(
            """
            INSERT INTO calendar_snapshots(
                calendar_snapshot_id, calendar_name, timezone_name,
                package_name, package_version, tzdata_version,
                session_start_date, session_end_date, schedule_checksum,
                generated_at, created_at, state
            ) VALUES ('bad-calendar', 'XNYS', 'America/New_York',
                      'synthetic', '1', '2026a', '2025-01-01', '2025-01-02', ?,
                      ?, ?, 'CURRENT')
            """,
            ("sha256:" + "g" * 64, _START.isoformat(), _START.isoformat()),
        )


def test_status_transition_trigger_rejects_terminal_reopen(
    store: OperationalStateStore,
) -> None:
    with store._transaction() as connection:
        _insert_run(connection)
        connection.execute(
            "UPDATE ingestion_runs SET status = 'RUNNING', started_at = ? WHERE run_id = 'run-1'",
            (_START.isoformat(),),
        )
        connection.execute(
            """
            UPDATE ingestion_runs
            SET status = 'SUCCESS', completed_at = ?
            WHERE run_id = 'run-1'
            """,
            (_START.isoformat(),),
        )

    with (
        pytest.raises(sqlite3.IntegrityError, match="invalid ingestion run"),
        store._transaction() as connection,
    ):
        connection.execute("UPDATE ingestion_runs SET status = 'RUNNING' WHERE run_id = 'run-1'")


def test_transaction_rolls_back_all_rows_and_rejects_nesting(store: OperationalStateStore) -> None:
    with (
        pytest.raises(RuntimeError, match="synthetic crash"),
        store._transaction() as connection,
    ):
        connection.execute(
            """
            INSERT INTO stream_keys(
                stream_id, stream_hash, provider, dataset, instrument_id,
                timeframe, session, adjustment, created_at
            ) VALUES ('rolled-back', ?, 'synthetic', 'price_bars', 'instrument-1',
                      '5m', 'rth', 'raw', ?)
            """,
            (_HASH_A, _START.isoformat()),
        )
        raise RuntimeError("synthetic crash")

    with store.read_only_connection() as connection:
        count = int(
            connection.execute(
                "SELECT count(*) FROM stream_keys WHERE stream_id = 'rolled-back'"
            ).fetchone()[0]
        )
    assert count == 0

    with (
        store._transaction(),
        pytest.raises(OperationalTransactionError, match="nested"),
        store._transaction(),
    ):
        pass


def test_open_store_rejects_mutation_after_sentinel_root_id_replacement(
    store: OperationalStateStore,
    private_root: PrivateDataRoot,
) -> None:
    document = json.loads(private_root.sentinel_path.read_text(encoding="utf-8"))
    document["root_id"] = str(uuid4())
    private_root.sentinel_path.write_text(
        json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(PrivateDataRootSentinelError, match="ID changed"), store._transaction():
        pass


def test_competing_writer_lease_and_same_owner_acquire_are_safe(
    private_root: PrivateDataRoot,
    clock: MutableClock,
) -> None:
    with (
        OperationalStateStore.open(private_root, clock=clock) as first_store,
        OperationalStateStore.open(private_root, clock=clock) as second_store,
    ):
        first = first_store.acquire_writer_lease("writer-a", timedelta(seconds=30))
        same = first_store.acquire_writer_lease("writer-a", timedelta(minutes=5))
        assert same == first

        with pytest.raises(WriterLeaseBusyError, match="another live owner"):
            second_store.acquire_writer_lease("writer-b", timedelta(seconds=30))

        assert second_store.get_writer_lease() == first


def test_writer_lease_renew_release_and_reacquire(
    store: OperationalStateStore,
    clock: MutableClock,
) -> None:
    first = store.acquire_writer_lease("writer-a", timedelta(seconds=10))
    clock.advance(timedelta(seconds=5))
    renewed = store.renew_writer_lease(first, timedelta(seconds=20))

    assert renewed.generation == first.generation == 1
    assert renewed.heartbeat_at == clock.value
    assert renewed.expires_at == clock.value + timedelta(seconds=20)
    assert store.release_writer_lease(renewed)
    assert not store.release_writer_lease(renewed)
    assert store.get_writer_lease() is None

    second = store.acquire_writer_lease("writer-b", timedelta(seconds=10))
    assert second.generation == 2
    assert second.owner_id == "writer-b"


def test_expired_lease_takeover_records_stale_owner_and_invalidates_old_handle(
    store: OperationalStateStore,
    clock: MutableClock,
) -> None:
    first = store.acquire_writer_lease("writer-a", timedelta(seconds=10))
    clock.advance(timedelta(seconds=10))

    assert store.get_writer_lease() is None
    with pytest.raises(WriterLeaseLostError, match="no longer active"):
        store.renew_writer_lease(first, timedelta(seconds=20))
    with pytest.raises(WriterLeaseLostError, match="expired"):
        store.release_writer_lease(first)

    second = store.acquire_writer_lease("writer-b", timedelta(seconds=20))

    assert second.generation == first.generation + 1
    assert second.owner_id == "writer-b"
    with pytest.raises(WriterLeaseLostError, match="no longer active"):
        store.renew_writer_lease(first, timedelta(seconds=20))
    with pytest.raises(WriterLeaseLostError, match="no longer active"):
        store.release_writer_lease(first)

    row = store._connection.execute(
        """
        SELECT event_type, owner_id, previous_owner_id, generation
        FROM writer_lease_events
        ORDER BY event_id DESC LIMIT 1
        """
    ).fetchone()
    assert row is not None
    assert tuple(row) == ("STALE_TAKEOVER", "writer-b", "writer-a", 2)


@pytest.mark.parametrize(
    "ttl",
    [timedelta(0), timedelta(milliseconds=999), timedelta(days=1, seconds=1)],
)
def test_writer_lease_rejects_unbounded_ttl(
    store: OperationalStateStore,
    ttl: timedelta,
) -> None:
    with pytest.raises(WriterLeaseError, match="between one second and one day"):
        store.acquire_writer_lease("writer-a", ttl)


@pytest.mark.parametrize("owner_id", ["", "contains spaces", "line\nbreak", "x" * 129])
def test_writer_lease_rejects_unsafe_owner_identifier(
    store: OperationalStateStore,
    owner_id: str,
) -> None:
    with pytest.raises(WriterLeaseError, match="safe identifier"):
        store.acquire_writer_lease(owner_id, timedelta(seconds=10))


def test_writer_lease_rejects_clock_rollback(
    store: OperationalStateStore,
    clock: MutableClock,
) -> None:
    lease = store.acquire_writer_lease(str(uuid4()), timedelta(seconds=10))
    clock.value -= timedelta(seconds=1)

    with pytest.raises(WriterLeaseError, match="clock moved backwards"):
        store.renew_writer_lease(lease, timedelta(seconds=10))


def test_public_open_api_does_not_accept_an_arbitrary_database_path() -> None:
    signature = inspect.signature(OperationalStateStore.open)

    assert "database_path" not in signature.parameters
    assert "path" not in signature.parameters
    assert not hasattr(OperationalStateStore, "transaction")
