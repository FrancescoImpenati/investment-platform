"""Offline acceptance for catalog-only query visibility and same-provider revisions."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import polars as pl
import pytest

from investment_platform.data.ingestion.planner import IngestionIntent, RepairStrategy
from investment_platform.data.operational.query import (
    CandidateBatchDisposition,
    CatalogBarQueryIntegrityError,
    CatalogBarQueryPolicyError,
    CatalogBarQueryRepository,
    CatalogRevisionView,
)
from investment_platform.data.operational.store import OperationalStateStore
from investment_platform.data.retention import RetentionPolicyCatalog, RetentionPolicyEnforcer
from investment_platform.data.storage.canonical_batches import (
    CanonicalBatchManifest,
    CanonicalParquetPart,
    CanonicalStreamOutcome,
    StreamPublicationOutcome,
)
from investment_platform.data_root import PrivateDataRoot
from investment_platform.runtime import RuntimeEnvironment
from tests.integration.test_living_ingestion_service import (
    _NOW,
    _OPEN,
    _RUN_1,
    MutableClock,
    _payload,
    _private_root,
    _request,
    _service,
)

pytestmark = pytest.mark.integration

_CORRECTION_RUN = UUID("10000000-0000-4000-8000-000000000099")


def _query_repository(
    root: PrivateDataRoot,
    store: OperationalStateStore,
    clock: MutableClock,
) -> CatalogBarQueryRepository:
    return CatalogBarQueryRepository(
        store,
        root,
        RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock),
        environment=RuntimeEnvironment.PRIVATE_RESEARCH,
    )


def _corrected_payload() -> bytes:
    return json.dumps(
        {
            "bars": {
                "AAPL": [
                    {
                        "t": _OPEN.isoformat().replace("+00:00", "Z"),
                        "o": 100.0,
                        "h": 103.0,
                        "l": 99.0,
                        "c": 102.0,
                        "v": 1_000,
                        "vw": 100.25,
                    }
                ]
            },
            "next_page_token": None,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _published_candidate(
    root: PrivateDataRoot,
    store: OperationalStateStore,
) -> tuple[tuple[CanonicalParquetPart, ...], CanonicalBatchManifest]:
    with store.read_only_connection() as connection:
        row = connection.execute(
            """
            SELECT batch.manifest_relative_path
            FROM canonical_batches AS batch
            WHERE batch.state = 'VERIFIED'
            ORDER BY batch.published_at LIMIT 1
            """
        ).fetchone()
    assert row is not None
    manifest_path = root.managed_path(str(row["manifest_relative_path"]))
    manifest = CanonicalBatchManifest.model_validate_json(manifest_path.read_bytes())
    batch_directory = manifest_path.parent
    parts = tuple(
        CanonicalParquetPart(
            relative_path=value.relative_path,
            frame=pl.read_parquet(batch_directory / value.relative_path),
        )
        for value in manifest.files
    )
    return parts, manifest


def test_query_uses_only_verified_catalog_files_and_semantic_noop_classifier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        service, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        service.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))
        repository = _query_repository(root, store, clock)

        def reject_glob(*_: object, **__: object) -> object:
            raise AssertionError("Phase 2 query must not glob the filesystem")

        monkeypatch.setattr(Path, "rglob", reject_glob)
        result = repository.query("alpaca", "price_bars_sip")

        assert result.canonical_file_count == 1
        assert len(result.revisions) == 2
        assert len(result.current) == 2
        assert result.frame().height == 2
        assert result.frame(CatalogRevisionView.ALL_VERSIONS).height == 2
        assert all(len(value.provenances) == 1 for value in result.revisions)

        parts, manifest = _published_candidate(root, store)
        comparison = repository.classify_candidate(
            provider="alpaca",
            dataset="price_bars_sip",
            parts=parts,
            stream_outcomes=manifest.streams,
            processing_signature=manifest.processing_signature,
        )

        assert comparison.disposition is CandidateBatchDisposition.SEMANTIC_NO_OP
        assert comparison.semantic_duplicate_count == 2
        assert comparison.new_observation_count == 0
        assert comparison.revision_count == 0
        assert comparison.matching_canonical_batch_ids == (manifest.canonical_batch_id,)
        assert len(comparison.semantic_duplicate_slots) == 2
        assert {value.value_fingerprint for value in comparison.semantic_duplicate_slots} == {
            value.value_fingerprint for value in result.revisions
        }
        assert {
            (value.stream_id, value.start, value.end, value.observation_id)
            for value in comparison.semantic_duplicate_slots
        } == {
            (
                manifest.streams[0].stream_id,
                _OPEN,
                _OPEN + timedelta(minutes=5),
                result.revisions[0].observation_id,
            ),
            (
                manifest.streams[0].stream_id,
                _OPEN + timedelta(minutes=5),
                _OPEN + timedelta(minutes=10),
                result.revisions[1].observation_id,
            ),
        }
        assert not comparison.has_blocked_streams
        assert not comparison.publication_required
        updated = comparison.apply_stream_counts(manifest.streams)
        assert updated[0].semantic_duplicate_count == 2
        assert updated[0].revision_count == 0

        blocked_stream = manifest.streams[0].stream.model_copy(
            update={"instrument_id": UUID("1923431d-8907-4f63-ba11-68182c11f799")}
        )
        blocked = CanonicalStreamOutcome(
            stream=blocked_stream,
            outcome=StreamPublicationOutcome.BLOCKED,
            request_start=manifest.streams[0].request_start,
            request_end=manifest.streams[0].request_end,
            row_count=0,
            validation_codes=("QUALITY:TEST_BLOCKED",),
        )
        blocked_comparison = repository.classify_candidate(
            provider="alpaca",
            dataset="price_bars_sip",
            parts=parts,
            stream_outcomes=(*manifest.streams, blocked),
            processing_signature=manifest.processing_signature,
        )
        assert blocked_comparison.disposition is CandidateBatchDisposition.BLOCKED
        assert blocked_comparison.has_blocked_streams
        assert not blocked_comparison.publication_required


def test_provider_correction_preserves_versions_and_selects_current_deterministically(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(root, store, clock, [_payload(_OPEN)])
        initial.run(
            _request(
                run_id=_RUN_1,
                intent=IngestionIntent.BACKFILL,
                end=_OPEN + timedelta(minutes=5),
            )
        )
        correction, _ = _service(root, store, clock, [_corrected_payload()])
        correction.run(
            _request(
                run_id=_CORRECTION_RUN,
                intent=IngestionIntent.REPAIR,
                end=_OPEN + timedelta(minutes=5),
                repair_strategy=RepairStrategy.PROVIDER_REFRESH,
                repair_reason="synthetic provider correction",
            )
        )

        result = _query_repository(root, store, clock).query("alpaca", "price_bars_sip")

        assert len(result.revisions) == 2
        assert [value.revision_number for value in result.revisions] == [1, 2]
        assert sum(value.is_current for value in result.revisions) == 1
        assert len({value.value_fingerprint for value in result.revisions}) == 2
        expected_current = max(
            result.revisions,
            key=lambda value: value.provenances[-1].order_key,
        )
        assert result.current == (expected_current,)
        assert {value.bar.close for value in result.revisions} == {100.5, 102.0}
        assert {value.provenances[0].retrieved_at for value in result.revisions} == {clock.value}
        assert all(len(value.provenances) == 1 for value in result.revisions)


def test_query_fails_closed_for_policy_invalidation_and_checksum_change(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        service, _ = _service(root, store, clock, [_payload(_OPEN)])
        service.run(
            _request(
                run_id=_RUN_1,
                intent=IngestionIntent.BACKFILL,
                end=_OPEN + timedelta(minutes=5),
            )
        )
        repository = _query_repository(root, store, clock)
        assert repository.query("alpaca", "price_bars_sip").current
        for provider, dataset in (
            ("Alpaca", "price_bars_sip"),
            ("alpaca", "price_bars_sip "),
            ("alpaca", "price_bar_sip"),
        ):
            with pytest.raises(CatalogBarQueryPolicyError):
                repository.query(provider, dataset)

        with store.read_only_connection() as connection:
            relative_path = str(
                connection.execute("SELECT relative_path FROM canonical_files LIMIT 1").fetchone()[
                    0
                ]
            )
        with root.managed_path(relative_path).open("ab") as writer:
            writer.write(b"corrupt")
        with pytest.raises(CatalogBarQueryIntegrityError, match="checksum"):
            repository.query("alpaca", "price_bars_sip")

        # Policy invalidation denies before any stale canonical bytes can be inspected.
        with store._transaction() as connection:
            connection.execute(
                """
                UPDATE dataset_policy_status
                SET status = 'SUSPENDED', unavailable_at = ?, last_checked_at = ?
                WHERE provider = 'alpaca' AND dataset = 'price_bars_sip'
                """,
                (
                    clock.value.isoformat().replace("+00:00", "Z"),
                    clock.value.isoformat().replace("+00:00", "Z"),
                ),
            )
        with pytest.raises(CatalogBarQueryPolicyError, match="not active"):
            repository.query("alpaca", "price_bars_sip")


def test_invalid_batch_is_never_query_visible(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        service, _ = _service(root, store, clock, [_payload(_OPEN)])
        service.run(
            _request(
                run_id=_RUN_1,
                intent=IngestionIntent.BACKFILL,
                end=_OPEN + timedelta(minutes=5),
            )
        )
        with store._transaction() as connection:
            connection.execute(
                """
                UPDATE canonical_batches
                SET state = 'INVALID', invalidated_at = ?
                WHERE state = 'VERIFIED'
                """,
                (clock.value.isoformat().replace("+00:00", "Z"),),
            )

        result = _query_repository(root, store, clock).query("alpaca", "price_bars_sip")
        assert result.revisions == ()
        assert result.current == ()
        assert result.canonical_file_count == 0
