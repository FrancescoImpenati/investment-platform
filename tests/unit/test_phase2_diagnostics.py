"""Offline tests for sanitized Phase 2 operational diagnostics."""

from __future__ import annotations

import hashlib
import io
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from investment_platform.data.diagnostics import (
    DiagnosticCheck,
    DiagnosticStatus,
    Phase2OperationalDiagnostics,
    Phase2VerificationReport,
)
from investment_platform.data.ingestion.identity import (
    BarSemantics,
    DataKind,
    ProviderInstrumentMapping,
    RequestSpecification,
)
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.operational import (
    LATEST_SCHEMA_VERSION,
    CalendarSnapshotRepository,
    IngestionExecutionRepository,
    IngestionPlanRepository,
    OperationalStateStore,
    RestartProjectionReader,
)
from investment_platform.data.retention import (
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage import RawArtifactPublisher
from investment_platform.data.storage._publication import file_integrity
from investment_platform.data.storage.transport_spool import (
    TransportSpoolFaultPoint,
    TransportSpoolStore,
)
from investment_platform.data_root import PrivateDataRoot
from investment_platform.runtime import RuntimeEnvironment
from tests.unit.test_ingestion_execution_repository import (
    _NOW as _EXECUTION_NOW,
)
from tests.unit.test_ingestion_execution_repository import (
    _START as _EXECUTION_START,
)
from tests.unit.test_ingestion_execution_repository import (
    MutableClock as ExecutionClock,
)
from tests.unit.test_ingestion_execution_repository import (
    _calendar,
    _plan,
    _prepare_scenario,
)
from tests.unit.test_ingestion_execution_repository import (
    _private_root as _execution_private_root,
)

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
_START = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
_END = datetime(2026, 8, 28, 13, 35, tzinfo=UTC)
_PAYLOAD = b'{"bars":[],"origin":"synthetic"}'
_HEX = "a" * 64


class _InjectedTransportCrash(BaseException):
    pass


@pytest.fixture
def diagnostic_runtime(
    tmp_path: Path,
) -> Iterator[tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics]]:
    repository_root = Path(__file__).parents[2].resolve(strict=True)
    root = PrivateDataRoot(
        tmp_path.parent / f"p2-diagnostics-{uuid4().hex[:10]}",
        repository_root,
        allow_temporary_for_tests=True,
    )
    root.initialize(created_at=_NOW)
    store = OperationalStateStore(root, clock=lambda: _NOW)
    diagnostics = Phase2OperationalDiagnostics(root, store, clock=lambda: _NOW)
    try:
        yield root, store, diagnostics
    finally:
        store.close()


def _check(report: Phase2VerificationReport, code: str) -> DiagnosticCheck:
    return next(check for check in report.checks if check.code == code)


def _seed_active_policy(
    store: OperationalStateStore,
    *,
    provider: str = "synthetic",
    dataset: str = "price_bars",
) -> str:
    catalog = RetentionPolicyCatalog.load_default()
    policy = catalog.lookup(provider, dataset)
    snapshot_id = f"policy_snapshot_{uuid4().hex}"
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO policy_snapshots(
                policy_snapshot_id, policy_id, revision, policy_hash, provider, dataset,
                retention_mode, verified_at, captured_at, expires_at, entitlement_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (
                snapshot_id,
                policy.policy_id,
                policy.revision,
                policy.content_hash,
                provider,
                dataset,
                policy.mode.value,
                policy.verified_on.isoformat(),
                _NOW.isoformat(),
            ),
        )
        connection.execute(
            """
            INSERT INTO dataset_policy_status(
                provider, dataset, status, retention_mode, policy_snapshot_id,
                effective_at, expires_at, unavailable_at, last_checked_at
            ) VALUES (?, ?, 'ACTIVE', ?, ?, ?, NULL, NULL, ?)
            """,
            (
                provider,
                dataset,
                policy.mode.value,
                snapshot_id,
                _NOW.isoformat(),
                _NOW.isoformat(),
            ),
        )
    return snapshot_id


def _request() -> RequestSpecification:
    return RequestSpecification(
        provider="synthetic",
        dataset="price_bars",
        data_kind=DataKind.PRICE_BAR,
        instrument_mappings=(
            ProviderInstrumentMapping(
                instrument_id=UUID("00000000-0000-4000-8000-000000000001"),
                provider_identifier="SYNTHETIC",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        session=TradingSession.REGULAR,
        adjustment=AdjustmentState.UNADJUSTED,
        currency="USD",
        bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
        start=_START,
        end=_END,
        mapping_semantic_version="synthetic-diagnostics-v1",
    )


def _publish_and_catalog_raw(
    root: PrivateDataRoot,
    store: OperationalStateStore,
) -> Path:
    specification = _request()
    catalog = RetentionPolicyCatalog.load_default()
    enforcer = RetentionPolicyEnforcer(catalog, clock=lambda: _NOW)
    request_authorization = enforcer.authorize_request(
        specification.provider,
        specification.dataset,
        environment=RuntimeEnvironment.TEST,
        start=specification.start,
        end=specification.end,
        request_spec_hash=specification.request_spec_hash,
    )
    page_authorization = enforcer.authorize_response_page(
        request_authorization,
        page_ordinal=0,
        page_relation="root",
        payload_sha256=hashlib.sha256(_PAYLOAD).hexdigest(),
        payload_size_bytes=len(_PAYLOAD),
        canonical_media_type="application/json",
        content_encoding="identity",
        observed_start=None,
        observed_end=None,
    )
    published = RawArtifactPublisher(root, enforcer).publish(
        specification,
        io.BytesIO(_PAYLOAD),
        page_ordinal=0,
        media_type="application/json",
        content_encoding="identity",
        authorization=page_authorization,
        first_persisted_at=_NOW,
    )
    manifest_path = root.managed_path(published.manifest_relative_path)
    manifest_sha256, manifest_bytes = file_integrity(manifest_path)
    identity = page_authorization.artifact_descriptor
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO request_specs(
                request_spec_id, request_spec_hash, provider, dataset, interval_start,
                interval_end, mapping_version, specification_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                specification.request_spec_id,
                specification.request_spec_hash,
                specification.provider,
                specification.dataset,
                specification.start.isoformat(),
                specification.end.isoformat(),
                specification.mapping_semantic_version,
                specification.canonical_json,
                _NOW.isoformat(),
            ),
        )
        connection.execute(
            """
            INSERT INTO raw_artifacts(
                artifact_id, request_spec_id, page_ordinal, page_relation_hash,
                content_sha256, byte_count, media_type, content_encoding, relative_path,
                manifest_relative_path, first_persisted_at, verified_at, state
            ) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'VERIFIED')
            """,
            (
                published.artifact_id,
                specification.request_spec_id,
                # Reconstructing the identity here makes the catalog comparison independently
                # sensitive to page-relation corruption.
                _raw_page_relation_hash(),
                identity.content_sha256,
                identity.byte_count,
                identity.media_type,
                identity.content_encoding,
                published.payload_relative_path,
                published.manifest_relative_path,
                published.first_persisted_at.isoformat(),
                _NOW.isoformat(),
            ),
        )
        connection.execute(
            """
            INSERT INTO raw_artifact_manifests(
                artifact_id, manifest_content_sha256, manifest_byte_count,
                manifest_schema_version, verified_at
            ) VALUES (?, ?, ?, 1, ?)
            """,
            (published.artifact_id, manifest_sha256, manifest_bytes, _NOW.isoformat()),
        )
    return root.managed_path(published.payload_relative_path)


def _raw_page_relation_hash() -> str:
    specification = _request()
    from investment_platform.data.ingestion.identity import RawArtifactIdentity

    return RawArtifactIdentity.from_bytes(
        specification,
        page_ordinal=0,
        media_type="application/json",
        content_encoding="identity",
        payload=_PAYLOAD,
    ).page_relation_hash


def test_empty_store_status_and_verify_are_sanitized_and_healthy(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    root, _, diagnostics = diagnostic_runtime

    status = diagnostics.status()
    report = diagnostics.verify()

    assert status.root_valid
    assert status.sqlite_healthy
    assert status.schema_version == LATEST_SCHEMA_VERSION
    assert status.run_count == status.request_count == status.stream_count == 0
    assert status.canonical_row_count == status.stored_raw_artifact_count == 0
    assert status.quarantine_artifact_count == 0
    assert status.latest_run is None
    assert status.latest_error is None
    assert report.healthy
    assert {check.code for check in report.checks} == {
        "ROOT_SENTINEL",
        "SQLITE_INTEGRITY",
        "RAW_CATALOG_CONTENT",
        "CANONICAL_CATALOG_CONTENT",
        "QUARANTINE_CATALOG_CONTENT",
        "STAGING_STATE",
        "PUBLISHED_ORPHANS",
        "WATERMARK_COVERAGE",
        "RETENTION_POLICY",
    }
    serialized = status.model_dump_json()
    assert str(root.root) not in serialized
    assert "ingestion.sqlite3" not in serialized


def test_status_omits_stored_error_message_and_private_identifiers(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    root, store, diagnostics = diagnostic_runtime
    snapshot_id = _seed_active_policy(store)
    run_id = str(uuid4())
    marker = "PRIVATE_MESSAGE_MUST_NOT_APPEAR"
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO ingestion_runs(
                run_id, mode, environment, provider, dataset, status,
                policy_snapshot_id, created_at
            ) VALUES (?, 'VERIFY', 'test', 'synthetic', 'price_bars', 'FAILED', ?, ?)
            """,
            (run_id, snapshot_id, _NOW.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO errors(
                error_id, run_id, category, code, sanitized_message, retryable, occurred_at
            ) VALUES (?, ?, 'INTEGRITY', 'CHECK_FAILED', ?, 0, ?)
            """,
            (str(uuid4()), run_id, marker, _NOW.isoformat()),
        )

    serialized = diagnostics.status().model_dump_json()

    assert marker not in serialized
    assert run_id not in serialized
    assert str(root.root) not in serialized
    assert "CHECK_FAILED" in serialized


def test_status_boundedly_exposes_non_terminal_run_ids_for_hard_crash_recovery(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, store, diagnostics = diagnostic_runtime
    run_ids = tuple(UUID(int=index + 1) for index in range(21))
    enforcer = RetentionPolicyEnforcer(
        RetentionPolicyCatalog.load_default(),
        clock=lambda: _NOW,
    )
    lease = store.acquire_writer_lease("crashed-process", timedelta(minutes=5))
    CalendarSnapshotRepository(store).persist(lease, _calendar())
    template = _plan(enforcer)
    plans = IngestionPlanRepository(store)
    for run_id in run_ids:
        plans.persist(
            lease,
            template.model_copy(
                update={
                    "run_id": run_id,
                    "reason": f"hard-crash-recovery-{run_id.int}",
                }
            ),
        )

    status = diagnostics.status()

    assert status.active_writer_lease
    assert status.non_terminal_run_count == len(run_ids)
    assert len(status.non_terminal_runs) == 20
    assert status.non_terminal_runs[0].run_id == run_ids[-1]
    assert {run.run_id for run in status.non_terminal_runs} == set(run_ids[1:])
    assert {run.next_action for run in status.non_terminal_runs} == {"WAIT_FOR_WRITER"}
    assert RestartProjectionReader(store).load_run(run_ids[-1]).run_id == run_ids[-1]
    with OperationalStateStore.open_read_only(root, clock=lambda: _NOW) as diagnostic_store:
        reopened = Phase2OperationalDiagnostics(
            root,
            diagnostic_store,
            clock=lambda: _NOW,
        ).status()
    assert reopened.non_terminal_run_count == len(run_ids)
    assert reopened.non_terminal_runs[0].run_id == run_ids[-1]
    assert {run.next_action for run in reopened.non_terminal_runs} == {"WAIT_FOR_WRITER"}

    original_connect = store._connect_read_only
    terminalized_during_status = False
    with sqlite3.connect(store.path) as racing_writer:

        def connect_with_race(path: Path, *, immutable: bool) -> sqlite3.Connection:
            connection = original_connect(path, immutable=immutable)

            def terminalize_latest_run(statement: str) -> None:
                nonlocal terminalized_during_status
                if (
                    terminalized_during_status
                    or "SELECT run_id, environment" not in statement
                    or "FROM ingestion_runs AS recoverable" not in statement
                ):
                    return
                racing_writer.execute(
                    """
                    UPDATE ingestion_runs
                    SET status = 'FAILED', completed_at = ?
                    WHERE run_id = ?
                    """,
                    (_NOW.isoformat(), str(run_ids[-1])),
                )
                racing_writer.commit()
                terminalized_during_status = True

            connection.set_trace_callback(terminalize_latest_run)
            return connection

        monkeypatch.setattr(store, "_connect_read_only", connect_with_race)
        raced = diagnostics.status()

    assert terminalized_during_status
    assert raced.non_terminal_run_count == len(run_ids)
    assert raced.non_terminal_runs[0].run_id == run_ids[-1]
    monkeypatch.setattr(store, "_connect_read_only", original_connect)

    with sqlite3.connect(store.path) as connection:
        connection.execute(
            """
            UPDATE writer_leases
            SET acquired_at = ?, heartbeat_at = ?, expires_at = ?
            WHERE lease_name = ?
            """,
            (
                (_NOW - timedelta(seconds=3)).isoformat(),
                (_NOW - timedelta(seconds=2)).isoformat(),
                (_NOW - timedelta(seconds=1)).isoformat(),
                lease.lease_name,
            ),
        )

    expired = diagnostics.status()

    assert not expired.active_writer_lease
    assert {run.next_action for run in expired.non_terminal_runs} == {"RESUME"}


def test_status_reports_stream_dimensions_and_open_gap_without_observations(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    _, store, diagnostics = diagnostic_runtime
    instrument_id = UUID("00000000-0000-4000-8000-000000000001")
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO stream_keys(
                stream_id, stream_hash, provider, dataset, instrument_id,
                timeframe, session, adjustment, dimensions_json, created_at
            ) VALUES ('stream-safe', ?, 'synthetic', 'price_bars', ?,
                      '5m', 'regular', 'unadjusted', '{}', ?)
            """,
            (_HEX, str(instrument_id), _NOW.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO gaps(
                gap_id, stream_id, interval_start, interval_end, gap_type,
                status, blocking, detected_at
            ) VALUES ('gap-safe', 'stream-safe', ?, ?, 'ACQUISITION',
                      'OPEN', 1, ?)
            """,
            (_START.isoformat(), _END.isoformat(), _NOW.isoformat()),
        )

    status = diagnostics.status()

    assert len(status.streams) == 1
    stream = status.streams[0]
    assert stream.instrument_id == instrument_id
    assert stream.timeframe == "5m"
    assert stream.coverage_start is None
    assert stream.watermark_frontier is None
    assert stream.open_gap_count == 1


def test_verify_reports_recoverable_staging_without_modifying_it(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    root, _, diagnostics = diagnostic_runtime
    candidate = root.ensure_directory("staging/raw-artifacts/recovery-test.tmp")
    partial = candidate / "partial.bin"
    partial.write_bytes(b"partial")
    before = partial.read_bytes()

    report = diagnostics.verify()

    staging = _check(report, "STAGING_STATE")
    assert staging.status is DiagnosticStatus.WARN
    assert staging.issue_codes == ("STAGING_RECOVERY_REQUIRED",)
    assert partial.read_bytes() == before
    assert candidate.exists()


def test_verify_reports_residual_transport_attempt_without_mutating_it(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    root, _, diagnostics = diagnostic_runtime
    attempt_id = UUID("60000000-0000-4000-8000-000000000001")
    spool = TransportSpoolStore(root)

    def crash(point: TransportSpoolFaultPoint) -> None:
        if point is TransportSpoolFaultPoint.ATTEMPT_READY:
            raise _InjectedTransportCrash

    with pytest.raises(_InjectedTransportCrash), spool.attempt(attempt_id, fault_injector=crash):
        pass
    attempt = root.root / "staging" / "transport-attempts" / str(attempt_id)
    before = (attempt / ".attempt-owner.json").read_bytes()

    staging = _check(diagnostics.verify(), "STAGING_STATE")

    assert staging.status is DiagnosticStatus.WARN
    assert "TRANSPORT_STAGING_RECOVERY_REQUIRED" in staging.issue_codes
    assert (attempt / ".attempt-owner.json").read_bytes() == before


def test_verify_fails_for_invalid_transport_attempt_name_without_deleting_it(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    root, _, diagnostics = diagnostic_runtime
    invalid = root.ensure_directory("staging/transport-attempts/not-an-attempt")

    staging = _check(diagnostics.verify(), "STAGING_STATE")

    assert staging.status is DiagnosticStatus.FAIL
    assert "INVALID_TRANSPORT_STAGING_ENTRY" in staging.issue_codes
    assert invalid.is_dir()


def test_verify_fails_for_malformed_transport_owner_without_modifying_it(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    root, _, diagnostics = diagnostic_runtime
    attempt_id = UUID("60000000-0000-4000-8000-000000000002")
    spool = TransportSpoolStore(root)

    def crash(point: TransportSpoolFaultPoint) -> None:
        if point is TransportSpoolFaultPoint.ATTEMPT_READY:
            raise _InjectedTransportCrash

    with pytest.raises(_InjectedTransportCrash), spool.attempt(attempt_id, fault_injector=crash):
        pass
    owner = root.root / "staging" / "transport-attempts" / str(attempt_id) / ".attempt-owner.json"
    malformed = b"{}\n"
    owner.write_bytes(malformed)

    staging = _check(diagnostics.verify(), "STAGING_STATE")

    assert staging.status is DiagnosticStatus.FAIL
    assert "INVALID_TRANSPORT_STAGING_ENTRY" in staging.issue_codes
    assert owner.read_bytes() == malformed


def test_verify_fails_closed_for_invalid_uncataloged_publication(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    root, _, diagnostics = diagnostic_runtime
    orphan = root.ensure_directory(
        "raw/provider=synthetic/dataset=price_bars/artifacts/artifact=" + "b" * 32
    )
    (orphan / "manifest.json").write_text("{}", encoding="utf-8")

    report = diagnostics.verify()

    check = _check(report, "PUBLISHED_ORPHANS")
    assert check.status is DiagnosticStatus.FAIL
    assert "INVALID_UNCATALOGED_RAW" in check.issue_codes
    assert not report.healthy


def test_verify_detects_unmanifested_file_in_managed_data_namespace(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    root, _, diagnostics = diagnostic_runtime
    stray = root.ensure_directory(
        "raw/provider=synthetic/dataset=price_bars/artifacts/artifact=" + "c" * 32
    )
    (stray / "payload.bin").write_bytes(b"unmanifested synthetic bytes")

    report = diagnostics.verify()

    check = _check(report, "PUBLISHED_ORPHANS")
    assert check.status is DiagnosticStatus.FAIL
    assert "UNEXPECTED_PUBLICATION_LAYOUT" in check.issue_codes


def test_raw_catalog_verification_reopens_content_and_detects_corruption(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    root, store, diagnostics = diagnostic_runtime
    _seed_active_policy(store)
    payload_path = _publish_and_catalog_raw(root, store)

    healthy = diagnostics.verify()

    assert _check(healthy, "RAW_CATALOG_CONTENT").status is DiagnosticStatus.PASS
    assert _check(healthy, "PUBLISHED_ORPHANS").status is DiagnosticStatus.PASS
    payload_path.write_bytes(b"corrupted synthetic bytes")

    corrupted = diagnostics.verify()

    raw_check = _check(corrupted, "RAW_CATALOG_CONTENT")
    assert raw_check.status is DiagnosticStatus.FAIL
    assert "RAW_FILE_OR_MANIFEST_INVALID" in raw_check.issue_codes


def test_retention_check_rejects_unknown_active_dataset(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    _, store, diagnostics = diagnostic_runtime
    snapshot_id = f"policy_snapshot_{uuid4().hex}"
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO policy_snapshots(
                policy_snapshot_id, policy_id, revision, policy_hash, provider, dataset,
                retention_mode, verified_at, captured_at, expires_at, entitlement_active
            ) VALUES (?, 'unknown-policy', 1, ?, 'unknown_provider', 'private_data',
                      'DURABLE_AUTHORIZED', '2026-08-31', ?, NULL, NULL)
            """,
            (snapshot_id, _HEX, _NOW.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO dataset_policy_status(
                provider, dataset, status, retention_mode, policy_snapshot_id,
                effective_at, expires_at, unavailable_at, last_checked_at
            ) VALUES ('unknown_provider', 'private_data', 'ACTIVE',
                      'DURABLE_AUTHORIZED', ?, ?, NULL, NULL, ?)
            """,
            (snapshot_id, _NOW.isoformat(), _NOW.isoformat()),
        )

    report = diagnostics.verify()

    retention = _check(report, "RETENTION_POLICY")
    assert retention.status is DiagnosticStatus.FAIL
    assert retention.issue_codes == ("UNKNOWN_DATASET_POLICY",)


def test_watermark_requires_exact_contiguous_coverage_proof(
    diagnostic_runtime: tuple[PrivateDataRoot, OperationalStateStore, Phase2OperationalDiagnostics],
) -> None:
    _, store, diagnostics = diagnostic_runtime
    snapshot_id = _seed_active_policy(store)
    run_id = str(uuid4())
    stream_id = "stream_v1_" + "1" * 64
    request_spec_id = "request_spec_v1_" + "2" * 64
    calendar_id = "calendar_v1_" + "3" * 64
    context_id = "batch_context_v1_" + "4" * 64
    batch_id = "batch_v1_" + "5" * 64
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO ingestion_runs(
                run_id, mode, environment, provider, dataset, status,
                policy_snapshot_id, created_at
            ) VALUES (?, 'BACKFILL', 'test', 'synthetic', 'price_bars', 'SUCCESS', ?, ?)
            """,
            (run_id, snapshot_id, _NOW.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO stream_keys(
                stream_id, stream_hash, provider, dataset, instrument_id, timeframe,
                session, adjustment, dimensions_json, created_at
            ) VALUES (?, ?, 'synthetic', 'price_bars', ?, '5m', 'regular',
                      'unadjusted', '{}', ?)
            """,
            (stream_id, "1" * 64, str(uuid4()), _NOW.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO request_specs(
                request_spec_id, request_spec_hash, provider, dataset, interval_start,
                interval_end, mapping_version, specification_json, created_at
            ) VALUES (?, ?, 'synthetic', 'price_bars', ?, ?, 'test-v1', '{}', ?)
            """,
            (request_spec_id, "2" * 64, _START.isoformat(), _END.isoformat(), _NOW.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO calendar_snapshots(
                calendar_snapshot_id, calendar_name, timezone_name, package_name,
                package_version, tzdata_version, session_start_date, session_end_date,
                schedule_checksum, generated_at, created_at, state
            ) VALUES (?, 'XNYS', 'America/New_York', 'synthetic-calendar', '1',
                      'synthetic', '2026-08-28', '2026-08-29', ?, ?, ?, 'CURRENT')
            """,
            (calendar_id, "sha256:" + "3" * 64, _NOW.isoformat(), _NOW.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO calendar_sessions(
                calendar_snapshot_id, session_date, open_at, close_at, is_early_close,
                expected_1d_count, expected_5m_count
            ) VALUES (?, '2026-08-28', ?, ?, 0, 1, 1)
            """,
            (calendar_id, _START.isoformat(), _END.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO batch_contexts(
                batch_context_id, canonical_batch_id, request_spec_id,
                ordered_artifacts_hash, canonical_schema_version, normalizer_version,
                validator_version, calendar_snapshot_id, fixed_ingested_at,
                manifest_created_at, created_at
            ) VALUES (?, ?, ?, ?, 'price-bar-v1', 'synthetic-v1', 'validator-v1',
                      ?, ?, ?, ?)
            """,
            (
                context_id,
                batch_id,
                request_spec_id,
                "4" * 64,
                calendar_id,
                _NOW.isoformat(),
                _NOW.isoformat(),
                _NOW.isoformat(),
            ),
        )
        relative = (
            "normalized/price_bars/provider=synthetic/dataset=price_bars/batches/batch=" + "5" * 32
        )
        connection.execute(
            """
            INSERT INTO canonical_batches(
                canonical_batch_id, batch_context_id, policy_snapshot_id, relative_path,
                manifest_relative_path, state, row_count, published_at, verified_at
            ) VALUES (?, ?, ?, ?, ?, 'VERIFIED', 1, ?, ?)
            """,
            (
                batch_id,
                context_id,
                snapshot_id,
                relative,
                relative + "/manifest.json",
                _NOW.isoformat(),
                _NOW.isoformat(),
            ),
        )
        connection.execute(
            """
            INSERT INTO canonical_batch_streams(
                canonical_batch_id, stream_id, outcome, row_count, interval_start,
                interval_end, validation_summary_json
            ) VALUES (?, ?, 'PUBLISHABLE', 1, ?, ?, '{}')
            """,
            (batch_id, stream_id, _START.isoformat(), _END.isoformat()),
        )
        connection.execute(
            """
            INSERT INTO watermarks(
                stream_id, coverage_start, exclusive_frontier, verification_state,
                generation, calendar_snapshot_id, policy_snapshot_id, last_run_id,
                last_batch_id, last_verified_session, blocking_gap_count, computed_at
            ) VALUES (?, ?, ?, 'VERIFIED', 1, ?, ?, ?, ?, '2026-08-28', 0, ?)
            """,
            (
                stream_id,
                _START.isoformat(),
                _END.isoformat(),
                calendar_id,
                snapshot_id,
                run_id,
                batch_id,
                _NOW.isoformat(),
            ),
        )

    report = diagnostics.verify()

    watermark = _check(report, "WATERMARK_COVERAGE")
    assert watermark.status is DiagnosticStatus.FAIL
    assert "WATERMARK_FRONTIER_NOT_CONTIGUOUS" in watermark.issue_codes


@pytest.mark.parametrize(
    ("column", "value", "issue_code"),
    [
        (
            "coverage_start",
            (_EXECUTION_START + timedelta(minutes=5)).isoformat(),
            "WATERMARK_COVERAGE_START_MISMATCH",
        ),
        (
            "exclusive_frontier",
            (_EXECUTION_START + timedelta(minutes=5)).isoformat(),
            "WATERMARK_FRONTIER_MISMATCH",
        ),
        ("blocking_gap_count", 1, "BLOCKING_GAP_COUNT_MISMATCH"),
    ],
)
def test_watermark_verification_reconstructs_authoritative_projection(
    tmp_path: Path,
    column: str,
    value: object,
    issue_code: str,
) -> None:
    root = _execution_private_root(tmp_path)
    clock = ExecutionClock(_EXECUTION_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        diagnostics = Phase2OperationalDiagnostics(root, store, clock=clock)
        assert _check(diagnostics.verify(), "WATERMARK_COVERAGE").status is DiagnosticStatus.PASS

        with store._transaction(write=True) as connection:
            connection.execute(
                f"UPDATE watermarks SET {column} = ? WHERE stream_id = ?",
                (value, scenario.coverage.watermarks[0].stream_id),
            )

        watermark = _check(diagnostics.verify(), "WATERMARK_COVERAGE")

        assert watermark.status is DiagnosticStatus.FAIL
        assert issue_code in watermark.issue_codes
        store.release_writer_lease(lease)
