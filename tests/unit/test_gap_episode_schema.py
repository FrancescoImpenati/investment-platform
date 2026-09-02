"""Operational persistence for recurring gap episodes."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import investment_platform.data.operational.store as store_module
from investment_platform.data.operational import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    OperationalStateStore,
)
from investment_platform.data_root import PrivateDataRoot

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
_START = datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
_END = _START + timedelta(minutes=5)


def _private_root(tmp_path: Path, name: str) -> PrivateDataRoot:
    repository_root = Path(__file__).parents[2].resolve(strict=True)
    root = PrivateDataRoot(
        tmp_path / name,
        repository_root,
        allow_temporary_for_tests=True,
    )
    root.initialize(created_at=_NOW)
    return root


def _insert_stream(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT INTO stream_keys(
            stream_id, stream_hash, provider, dataset, instrument_id,
            timeframe, session, adjustment, dimensions_json, created_at
        ) VALUES (
            'stream-gap-episode', ?, 'synthetic', 'price_bars',
            '00000000-0000-4000-8000-000000000001',
            '5m', 'rth', 'raw', '{}', ?
        )
        """,
        ("a" * 64, _NOW.isoformat()),
    )


def test_resolved_gap_tuple_can_recur_as_a_distinct_durable_episode(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path, "gap-episode-root")
    with OperationalStateStore.open(root, clock=lambda: _NOW) as store:
        lease = store.acquire_writer_lease("gap-episode-test", timedelta(minutes=5))
        with store._leased_transaction(lease) as connection:
            _insert_stream(connection)
            connection.execute(
                """
                INSERT INTO gaps(
                    gap_id, stream_id, interval_start, interval_end, gap_type,
                    status, blocking, detected_at, resolved_at,
                    request_instance_id, canonical_batch_id
                ) VALUES (?, 'stream-gap-episode', ?, ?, 'EXPECTED_OBSERVATION',
                          'RESOLVED', 1, ?, ?, NULL, NULL)
                """,
                (
                    "gap-episode-resolved",
                    _START.isoformat(),
                    _END.isoformat(),
                    _NOW.isoformat(),
                    (_NOW + timedelta(seconds=1)).isoformat(),
                ),
            )
            connection.execute(
                """
                INSERT INTO gaps(
                    gap_id, stream_id, interval_start, interval_end, gap_type,
                    status, blocking, detected_at, resolved_at,
                    request_instance_id, canonical_batch_id
                ) VALUES (?, 'stream-gap-episode', ?, ?, 'EXPECTED_OBSERVATION',
                          'OPEN', 1, ?, NULL, NULL, NULL)
                """,
                (
                    "gap-episode-recurrence",
                    _START.isoformat(),
                    _END.isoformat(),
                    (_NOW + timedelta(seconds=2)).isoformat(),
                ),
            )
        with store.read_only_connection() as connection:
            rows = connection.execute(
                """
                SELECT gap_id, status FROM gaps
                WHERE stream_id = 'stream-gap-episode'
                ORDER BY detected_at
                """
            ).fetchall()
        assert [tuple(row) for row in rows] == [
            ("gap-episode-resolved", "RESOLVED"),
            ("gap-episode-recurrence", "OPEN"),
        ]

    with (
        OperationalStateStore.open(root, clock=lambda: _NOW) as reopened,
        reopened.read_only_connection() as connection,
    ):
        assert (
            connection.execute(
                "SELECT count(*) FROM gaps WHERE stream_id = 'stream-gap-episode'"
            ).fetchone()[0]
            == 2
        )


def test_migration_nine_preserves_prior_gaps_and_removes_tuple_uniqueness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path, "gap-migration-root")
    pre_episode_migrations = MIGRATIONS[:8]
    monkeypatch.setattr(store_module, "MIGRATIONS", pre_episode_migrations)
    monkeypatch.setattr(store_module, "LATEST_SCHEMA_VERSION", 8)
    with OperationalStateStore.open(root, clock=lambda: _NOW) as old_store:
        lease = old_store.acquire_writer_lease("gap-migration-test", timedelta(minutes=5))
        with old_store._leased_transaction(lease) as connection:
            _insert_stream(connection)
            connection.execute(
                """
                INSERT INTO gaps(
                    gap_id, stream_id, interval_start, interval_end, gap_type,
                    status, blocking, detected_at, resolved_at,
                    request_instance_id, canonical_batch_id
                ) VALUES ('gap-before-m9', 'stream-gap-episode', ?, ?,
                          'EXPECTED_OBSERVATION', 'RESOLVED', 1, ?, ?, NULL, NULL)
                """,
                (
                    _START.isoformat(),
                    _END.isoformat(),
                    _NOW.isoformat(),
                    (_NOW + timedelta(seconds=1)).isoformat(),
                ),
            )
        old_store.release_writer_lease(lease)

    monkeypatch.setattr(store_module, "MIGRATIONS", MIGRATIONS)
    monkeypatch.setattr(store_module, "LATEST_SCHEMA_VERSION", LATEST_SCHEMA_VERSION)
    with OperationalStateStore.open(root, clock=lambda: _NOW) as upgraded:
        assert upgraded.schema_version == LATEST_SCHEMA_VERSION
        lease = upgraded.acquire_writer_lease("gap-migration-upgrade", timedelta(minutes=5))
        with upgraded._leased_transaction(lease) as connection:
            assert (
                connection.execute(
                    "SELECT status FROM gaps WHERE gap_id = 'gap-before-m9'"
                ).fetchone()[0]
                == "RESOLVED"
            )
            connection.execute(
                """
                INSERT INTO gaps(
                    gap_id, stream_id, interval_start, interval_end, gap_type,
                    status, blocking, detected_at, resolved_at,
                    request_instance_id, canonical_batch_id
                ) VALUES ('gap-after-m9', 'stream-gap-episode', ?, ?,
                          'EXPECTED_OBSERVATION', 'OPEN', 1, ?, NULL, NULL, NULL)
                """,
                (
                    _START.isoformat(),
                    _END.isoformat(),
                    (_NOW + timedelta(seconds=2)).isoformat(),
                ),
            )
        with upgraded.read_only_connection() as connection:
            assert (
                connection.execute(
                    "SELECT count(*) FROM gaps WHERE stream_id = 'stream-gap-episode'"
                ).fetchone()[0]
                == 2
            )
