"""Transactional operational integration for living-ingestion publication and recovery."""

from __future__ import annotations

import hashlib
import io
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import polars as pl
import pytest

from investment_platform.data.calendar import CalendarSession, CalendarSnapshot
from investment_platform.data.diagnostics import (
    DiagnosticStatus,
    Phase2OperationalDiagnostics,
)
from investment_platform.data.ingestion import (
    BarSemantics,
    BatchContext,
    CanonicalBatchIdentity,
    CoverageClassification,
    CoverageSegment,
    CoverageVerificationState,
    DataKind,
    IngestionIntent,
    IngestionPlanner,
    MaterializedWatermark,
    PlannerBudget,
    PlannerLimits,
    ProcessingSignature,
    ProviderInstrumentMapping,
    RawArtifactIdentity,
    StreamKey,
)
from investment_platform.data.ingestion.coverage import (
    CoverageRequestTerminalState,
    CoverageStreamOutcome,
    GapFinding,
    GapStatus,
    GapType,
)
from investment_platform.data.ingestion.identity import AttemptIdentity
from investment_platform.data.models import (
    AdjustmentState,
    PriceBar,
    Timeframe,
    TradingSession,
)
from investment_platform.data.operational import (
    AttemptFailureStatus,
    CalendarSnapshotRepository,
    CoverageCommit,
    ExecutionFaultPoint,
    ExecutionIdentityCollisionError,
    ExecutionIntegrityError,
    ExecutionStateConflictError,
    IngestionExecutionRepository,
    IngestionPlanRepository,
    IngestionRunStatus,
    PlanPersistenceRequest,
    PublicationCommitRequest,
    PublicationCommitSource,
    QuarantineArtifactRepository,
    QuarantineCatalogIntegrityError,
    RequestInstanceStatus,
    RestartProjectionReader,
    deterministic_calendar_snapshot_id,
    deterministic_policy_snapshot_id,
)
from investment_platform.data.operational.execution import (
    ProviderDispatchLimitExceededError,
)
from investment_platform.data.operational.store import (
    OperationalStateStore,
    WriterLease,
    _format_utc,
)
from investment_platform.data.provenance import (
    DataSource,
    LicenseClassification,
    RawBatchMetadata,
)
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    DatasetPolicyDenied,
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage import (
    CanonicalBatchExpectation,
    CanonicalBatchManifest,
    CanonicalBatchPublisher,
    CanonicalParquetPart,
    CanonicalPublicationProvenance,
    CanonicalStreamOutcome,
    PublicationFaultPoint,
    PublishedCanonicalBatch,
    PublishedRawArtifact,
    QuarantineArtifactManifest,
    QuarantineArtifactPublisher,
    RawArtifactPublisher,
    RawProvenanceBinding,
    StreamPublicationOutcome,
    deterministic_quarantine_artifact_id,
    price_bars_to_frame,
)
from investment_platform.data_root import PrivateDataRoot
from investment_platform.runtime import RuntimeEnvironment

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, tzinfo=UTC)
_START = datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
_END = datetime(2025, 7, 2, 13, 40, tzinfo=UTC)
_INSTRUMENT_ID = UUID("00000000-0000-4000-8000-000000000001")
_SOURCE_ID = UUID("00000000-0000-4000-8000-000000000002")
_RAW_BATCH_ID = UUID("00000000-0000-4000-8000-000000000003")
_RUN_ID = UUID("10000000-0000-4000-8000-000000000001")
_SECOND_RUN_ID = UUID("10000000-0000-4000-8000-000000000002")
_ATTEMPT_ID = UUID("20000000-0000-4000-8000-000000000001")
_SECOND_ATTEMPT_ID = UUID("20000000-0000-4000-8000-000000000002")
_PAYLOAD = b'{"bars":[{"fixture":"synthetic"}]}'


class InjectedCrash(RuntimeError):
    pass


@dataclass
class MutableClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value

    def advance(self, delta: timedelta = timedelta(seconds=1)) -> None:
        self.value += delta


@dataclass(frozen=True)
class Scenario:
    run_id: UUID
    request_id: UUID
    attempt_id: UUID
    acquisition: AcquisitionPolicyAuthorization
    expectation: CanonicalBatchExpectation
    manifest: CanonicalBatchManifest
    published: PublishedCanonicalBatch
    coverage: CoverageCommit
    raw_identity: RawArtifactIdentity
    raw_published: PublishedRawArtifact
    raw_metadata: RawBatchMetadata

    def commit_request(
        self,
        *,
        source: PublicationCommitSource = PublicationCommitSource.NORMAL,
    ) -> PublicationCommitRequest:
        return PublicationCommitRequest(
            request_instance_id=self.request_id,
            attempt_id=self.attempt_id,
            acquisition_authorization=self.acquisition,
            expectation=self.expectation,
            manifest=self.manifest,
            published=self.published,
            coverage=self.coverage,
            terminal_status=RequestInstanceStatus.SUCCESS,
            source=source,
        )


def _private_root(tmp_path: Path) -> PrivateDataRoot:
    repository_root = Path(__file__).parents[2].resolve(strict=True)
    root = PrivateDataRoot(
        tmp_path.parent / f"p2-execution-{uuid4().hex[:8]}",
        repository_root,
        allow_temporary_for_tests=True,
    )
    root.initialize(created_at=_NOW)
    return root


def _calendar() -> CalendarSnapshot:
    return CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="1",
        tzdata_version="synthetic",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=date(2025, 7, 2),
        range_end=date(2025, 7, 3),
        generated_at=_NOW,
        sessions=(
            CalendarSession(
                session_date=date(2025, 7, 2),
                open_utc=_START,
                close_utc=_END,
            ),
        ),
    )


def _stream() -> StreamKey:
    return StreamKey(
        provider="synthetic",
        dataset="price_bars",
        data_kind=DataKind.PRICE_BAR,
        instrument_id=_INSTRUMENT_ID,
        timeframe=Timeframe.FIVE_MINUTES,
        session=TradingSession.REGULAR,
        adjustment=AdjustmentState.UNADJUSTED,
        currency="USD",
        bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
    )


def _planner(enforcer: RetentionPolicyEnforcer) -> IngestionPlanner:
    return IngestionPlanner(enforcer)


def _plan(enforcer: RetentionPolicyEnforcer) -> PlanPersistenceRequest:
    plan = _planner(enforcer).plan(
        intent=IngestionIntent.BACKFILL,
        streams=(_stream(),),
        instrument_mappings=(
            ProviderInstrumentMapping(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="SYNTHETIC-A",
            ),
        ),
        desired_start=_START,
        desired_end=_END,
        calendar_snapshot=_calendar(),
        coverage=(),
        limits=PlannerLimits(
            max_instruments_per_request=1,
            max_expected_observations_per_request=10,
            max_observations_per_page=10,
            max_pages_per_request=1,
            max_calls_per_request=1,
            max_estimated_bytes_per_request=10_000,
            estimated_bytes_per_observation=100,
            estimated_bytes_per_page=10,
            estimated_cost_per_call=Decimal("0.01"),
            max_estimated_cost_per_request=Decimal("0.01"),
        ),
        budget=PlannerBudget(
            max_calls=1,
            max_expected_observations=10,
            max_pages=1,
            max_estimated_bytes=10_000,
            max_estimated_cost=Decimal("0.01"),
        ),
        environment=RuntimeEnvironment.TEST,
        mapping_semantic_version="synthetic-bars-request-v1",
    )
    assert len(plan.requests) == 1
    return PlanPersistenceRequest(
        run_id=_RUN_ID,
        calendar_snapshot_id=deterministic_calendar_snapshot_id(_calendar()),
        reason="synthetic execution acceptance",
        max_attempts=3,
        plan=plan,
    )


def _bars(ingested_at: datetime) -> pl.DataFrame:
    values = tuple(
        PriceBar(
            instrument_id=_INSTRUMENT_ID,
            timeframe=Timeframe.FIVE_MINUTES,
            timestamp_start=_START + timedelta(minutes=ordinal * 5),
            timestamp_end=_START + timedelta(minutes=(ordinal + 1) * 5),
            open=100.0 + ordinal,
            high=101.0 + ordinal,
            low=99.0 + ordinal,
            close=100.5 + ordinal,
            volume=1_000.0 + ordinal,
            vwap=100.25 + ordinal,
            currency="USD",
            session=TradingSession.REGULAR,
            adjustment_state=AdjustmentState.UNADJUSTED,
            source_id=_SOURCE_ID,
            raw_batch_id=_RAW_BATCH_ID,
            provider_record_id=f"synthetic-{ordinal}",
            retrieved_at=ingested_at - timedelta(seconds=1),
            ingested_at=ingested_at,
            quality_flags=(),
        )
        for ordinal in range(2)
    )
    return price_bars_to_frame(values)


def _prepare_scenario(
    root: PrivateDataRoot,
    store: OperationalStateStore,
    clock: MutableClock,
) -> Scenario:
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    plan_request = _plan(enforcer)
    lease = store.acquire_writer_lease("execution-writer", timedelta(minutes=30))
    CalendarSnapshotRepository(store).persist(lease, _calendar())
    IngestionPlanRepository(store).persist(lease, plan_request)
    request_id = (
        IngestionPlanRepository(store).load_progress(_RUN_ID).requests[0].request_instance_id
    )
    planned = plan_request.plan.requests[0]
    specification = planned.specification
    attempt = AttemptIdentity(
        attempt_id=_ATTEMPT_ID,
        request_instance_id=request_id,
        attempt_number=1,
    )
    execution = IngestionExecutionRepository(store)
    first_attempt = execution.begin_attempt(lease, attempt, planned.authorization)
    clock.advance(timedelta(minutes=1))
    replayed_attempt = execution.begin_attempt(lease, attempt, planned.authorization)
    assert not first_attempt.replayed
    assert replayed_attempt.replayed
    assert replayed_attempt.started_at == first_attempt.started_at

    page = enforcer.authorize_response_page(
        planned.authorization,
        page_ordinal=0,
        page_relation="root",
        payload_sha256=hashlib.sha256(_PAYLOAD).hexdigest(),
        payload_size_bytes=len(_PAYLOAD),
        canonical_media_type="application/json",
        content_encoding="identity",
        observed_start=_START,
        observed_end=_END,
    )
    raw = RawArtifactPublisher(root, enforcer).publish(
        specification,
        io.BytesIO(_PAYLOAD),
        page_ordinal=0,
        media_type="application/json",
        content_encoding="identity",
        authorization=page,
        first_persisted_at=clock.value,
    )
    metadata = RawBatchMetadata(
        batch_id=_RAW_BATCH_ID,
        source=DataSource(
            source_id=_SOURCE_ID,
            provider="synthetic",
            dataset="price_bars",
            logical_endpoint="synthetic/bars",
            license_classification=LicenseClassification.SYNTHETIC,
        ),
        retrieved_at=clock.value,
        media_type="application/json",
        file_extension="json",
        provider_request_id="synthetic-request-1",
        request_metadata={"provider_identifier": "SYNTHETIC-A", "page_number": 1},
    )
    raw_identity = RawArtifactIdentity.from_bytes(
        specification,
        page_ordinal=0,
        media_type="application/json",
        content_encoding="identity",
        payload=_PAYLOAD,
    )
    first_raw = execution.record_raw_artifact(
        lease,
        attempt,
        raw_identity,
        raw,
        metadata,
        observed_at=clock.value,
    )
    clock.advance()
    replayed_raw = execution.record_raw_artifact(
        lease,
        attempt,
        raw_identity,
        raw,
        metadata,
        observed_at=clock.value,
    )
    assert not first_raw.replayed
    assert replayed_raw.replayed
    acquisition = enforcer.authorize_completed_acquisition(
        planned.authorization,
        (page,),
        pagination_complete=True,
        terminal_page_verified=True,
    )
    first_acquisition = execution.complete_acquisition(lease, attempt, acquisition)
    clock.advance()
    replayed_acquisition = execution.complete_acquisition(lease, attempt, acquisition)
    assert not first_acquisition.replayed
    assert replayed_acquisition.replayed
    assert replayed_acquisition.completed_at == first_acquisition.completed_at

    context = BatchContext(
        batch_identity=CanonicalBatchIdentity(
            request_spec_hash=specification.request_spec_hash,
            ordered_artifacts=(raw_identity,),
            processing_signature=ProcessingSignature(
                canonical_schema_version="price-bar-v1",
                normalizer_version="synthetic-normalizer-v1",
                validator_version="bar-validator-v1",
                calendar_snapshot_checksum=_calendar().checksum,
            ),
        ),
        fixed_ingested_at=clock.value + timedelta(seconds=1),
        manifest_created_at=clock.value + timedelta(seconds=2),
    )
    provenance = CanonicalPublicationProvenance(
        source_id=_SOURCE_ID,
        raw_bindings=(
            RawProvenanceBinding(
                artifact_id=raw_identity.artifact_id,
                raw_batch_id=_RAW_BATCH_ID,
            ),
        ),
    )
    outcome = CanonicalStreamOutcome(
        stream=specification.stream_keys()[0],
        outcome=StreamPublicationOutcome.PUBLISHABLE,
        request_start=_START,
        request_end=_END,
        row_count=2,
        observed_start=_START,
        observed_end=_END,
    )
    expectation = CanonicalBatchExpectation(
        specification=specification,
        batch_context=context,
        calendar_snapshot=_calendar(),
        provenance=provenance,
        streams=(outcome,),
    )
    execution.record_batch_context(
        lease,
        attempt,
        specification,
        context,
        calendar_snapshot_id=plan_request.calendar_snapshot_id,
        provenance=provenance,
        recorded_at=clock.value,
    )
    execution.prepare_publication(lease, attempt, expectation, prepared_at=clock.value)
    published = CanonicalBatchPublisher(root, enforcer).publish(
        specification,
        context,
        (
            CanonicalParquetPart(
                relative_path="timeframe=5m/year=2025/month=07/part-0000.parquet",
                frame=_bars(context.fixed_ingested_at),
            ),
        ),
        (outcome,),
        authorization=acquisition,
        calendar_snapshot=_calendar(),
        provenance=provenance,
    )
    assert published is not None
    manifest = CanonicalBatchManifest.model_validate_json(
        (root.root / published.manifest_relative_path).read_bytes()
    )
    if clock.value < manifest.manifest_created_at:
        clock.advance(manifest.manifest_created_at - clock.value)
    policy = plan_request.plan.policy_authorization.policy_snapshot
    policy_snapshot_id = deterministic_policy_snapshot_id(policy)
    segment = CoverageSegment(
        coverage_id="coverage-synthetic-execution-1",
        stream_id=specification.stream_keys()[0].stream_id,
        canonical_batch_id=manifest.canonical_batch_id,
        calendar_snapshot_id=plan_request.calendar_snapshot_id,
        calendar_snapshot_checksum=_calendar().checksum,
        policy_snapshot_id=policy_snapshot_id,
        policy_id=policy.policy_id,
        policy_revision=policy.policy_revision,
        policy_hash=policy.policy_hash,
        coverage_start=_START,
        start=_START,
        end=_END,
        classification=CoverageClassification.OBSERVED,
        verification_state=CoverageVerificationState.VERIFIED,
        retained=True,
        row_count=2,
        artifact_count=1,
        artifacts_present=True,
        artifact_integrity_verified=True,
        interval_verified=True,
        request_completed=True,
        request_terminal_state=CoverageRequestTerminalState.SUCCESS,
        stream_outcome=CoverageStreamOutcome.PUBLISHABLE,
        pagination_verified=True,
        terminal_page_verified=True,
        canonical_batch_verified=True,
        canonical_file_count=1,
        raw_artifact_count=1,
        relational_provenance_verified=True,
        generation=1,
        verified_at=clock.value,
    )
    watermark = MaterializedWatermark(
        stream_id=segment.stream_id,
        coverage_start=_START,
        exclusive_frontier=_END,
        verification_state=CoverageVerificationState.VERIFIED,
        generation=1,
        calendar_snapshot_id=plan_request.calendar_snapshot_id,
        policy_snapshot_id=policy_snapshot_id,
        last_run_id=str(_RUN_ID),
        last_batch_id=manifest.canonical_batch_id,
        last_verified_session=_START.date(),
        blocking_gap_count=0,
        computed_at=clock.value,
    )
    repair_gap = GapFinding(
        gap_id="gap-synthetic-repaired-1",
        stream_id=segment.stream_id,
        start=_START,
        end=_END,
        gap_type=GapType.EXPECTED_OBSERVATION,
        status=GapStatus.RESOLVED,
        blocking=True,
        detected_at=clock.value - timedelta(seconds=1),
        resolved_at=clock.value,
        request_instance_id=str(request_id),
        canonical_batch_id=manifest.canonical_batch_id,
    )
    with store._leased_transaction(lease) as connection:
        connection.execute(
            """
            INSERT INTO gaps(
                gap_id, stream_id, interval_start, interval_end, gap_type,
                status, blocking, detected_at, resolved_at,
                request_instance_id, canonical_batch_id
            ) VALUES (?, ?, ?, ?, ?, 'OPEN', 1, ?, NULL, ?, NULL)
            """,
            (
                repair_gap.gap_id,
                repair_gap.stream_id,
                _format_utc(repair_gap.start),
                _format_utc(repair_gap.end),
                repair_gap.gap_type.value,
                _format_utc(repair_gap.detected_at),
                str(request_id),
            ),
        )
    coverage = CoverageCommit(
        calendar_snapshot_id=plan_request.calendar_snapshot_id,
        policy_snapshot_id=policy_snapshot_id,
        segments=(segment,),
        gaps=(repair_gap,),
        watermarks=(watermark,),
    )
    return Scenario(
        run_id=_RUN_ID,
        request_id=request_id,
        attempt_id=_ATTEMPT_ID,
        acquisition=acquisition,
        expectation=expectation,
        manifest=manifest,
        published=published,
        coverage=coverage,
        raw_identity=raw_identity,
        raw_published=raw,
        raw_metadata=metadata,
    )


def _prepare_running_attempt(
    store: OperationalStateStore,
    clock: MutableClock,
    *,
    max_attempts: int = 3,
) -> tuple[
    IngestionExecutionRepository,
    WriterLease,
    AttemptIdentity,
    PlanPersistenceRequest,
]:
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    plan_request = _plan(enforcer).model_copy(update={"max_attempts": max_attempts})
    lease = store.acquire_writer_lease("attempt-failure-test", timedelta(minutes=30))
    CalendarSnapshotRepository(store).persist(lease, _calendar())
    IngestionPlanRepository(store).persist(lease, plan_request)
    request_id = (
        IngestionPlanRepository(store)
        .load_progress(plan_request.run_id)
        .requests[0]
        .request_instance_id
    )
    identity = AttemptIdentity(
        attempt_id=_ATTEMPT_ID,
        request_instance_id=request_id,
        attempt_number=1,
    )
    repository = IngestionExecutionRepository(store)
    repository.begin_attempt(
        lease,
        identity,
        plan_request.plan.requests[0].authorization,
    )
    return repository, lease, identity, plan_request


def test_publication_commit_is_atomic_restartable_and_replay_safe(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        repository = IngestionExecutionRepository(store)
        committed = repository.commit_published_batch(lease, scenario.commit_request())

        assert not committed.replayed
        assert committed.run_status.value == "RUNNING"
        replay = repository.load_raw_replay(
            scenario.attempt_id, scenario.manifest.ordered_raw_artifacts[0].artifact_id
        )
        assert replay.metadata.batch_id == _RAW_BATCH_ID
        assert replay.metadata.source.source_id == _SOURCE_ID
        with store.read_only_connection() as connection:
            assert connection.execute("SELECT count(*) FROM canonical_batches").fetchone()[0] == 1
            assert connection.execute("SELECT count(*) FROM coverage_segments").fetchone()[0] == 1
            assert connection.execute("SELECT status FROM gaps").fetchone()[0] == "RESOLVED"
            assert connection.execute("SELECT count(*) FROM watermarks").fetchone()[0] == 1

        with store.read_only_connection() as connection:
            assert (
                connection.execute("SELECT status FROM ingestion_runs").fetchone()[0] == "RUNNING"
            )
        store.release_writer_lease(lease)

    clock.advance(timedelta(minutes=10))
    with OperationalStateStore.open(root, clock=clock) as reopened:
        lease = reopened.acquire_writer_lease("restart-writer", timedelta(minutes=30))
        repository = IngestionExecutionRepository(reopened)
        replayed = repository.commit_published_batch(lease, scenario.commit_request())
        assert replayed.replayed
        assert replayed.committed_at == committed.committed_at
        assert repository.reconcile_run(lease, scenario.run_id).value == "SUCCESS"
        second_summary = repository.reconcile_run(lease, scenario.run_id)
        assert second_summary.value == "SUCCESS"


def test_attempt_identity_replays_durable_start_after_process_restart(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    plan_request = _plan(enforcer)
    planned = plan_request.plan.requests[0]

    with OperationalStateStore.open(root, clock=clock) as store:
        lease = store.acquire_writer_lease("first-process", timedelta(minutes=30))
        CalendarSnapshotRepository(store).persist(lease, _calendar())
        IngestionPlanRepository(store).persist(lease, plan_request)
        request_id = (
            IngestionPlanRepository(store)
            .load_progress(plan_request.run_id)
            .requests[0]
            .request_instance_id
        )
        identity = AttemptIdentity(
            attempt_id=_ATTEMPT_ID,
            request_instance_id=request_id,
            attempt_number=1,
        )
        first = IngestionExecutionRepository(store).begin_attempt(
            lease,
            identity,
            planned.authorization,
        )
        store.release_writer_lease(lease)

    clock.advance(timedelta(days=2))
    with OperationalStateStore.open(root, clock=clock) as reopened:
        lease = reopened.acquire_writer_lease("second-process", timedelta(minutes=30))
        repository = IngestionExecutionRepository(reopened)
        replay = repository.begin_attempt(
            lease,
            identity,
            planned.authorization,
        )
        with pytest.raises(
            ExecutionIdentityCollisionError,
            match="attempt ID collides with another request or ordinal",
        ):
            repository.begin_attempt(
                lease,
                identity.model_copy(update={"attempt_number": 2}),
                planned.authorization,
            )

    assert replay.replayed
    assert replay.started_at == first.started_at
    assert replay.started_at != clock.value


def test_identical_canonical_batch_is_reused_across_runs_and_request_instances(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        repository = IngestionExecutionRepository(store)
        first = repository.commit_published_batch(lease, scenario.commit_request())
        assert repository.reconcile_run(lease, scenario.run_id) is IngestionRunStatus.SUCCESS

        # A distinct run may observe the same immutable response bytes.  The
        # request/attempt proof is new, while the content-derived batch,
        # manifest, files, streams, and coverage fact remain singular.
        replay_enforcer = RetentionPolicyEnforcer(
            RetentionPolicyCatalog.load_default(),
            clock=lambda: _NOW,
        )
        second_plan = _plan(replay_enforcer).model_copy(update={"run_id": _SECOND_RUN_ID})
        CalendarSnapshotRepository(store).persist(lease, _calendar())
        IngestionPlanRepository(store).persist(lease, second_plan)
        second_request_id = (
            IngestionPlanRepository(store)
            .load_progress(_SECOND_RUN_ID)
            .requests[0]
            .request_instance_id
        )
        second_attempt = AttemptIdentity(
            attempt_id=_SECOND_ATTEMPT_ID,
            request_instance_id=second_request_id,
            attempt_number=1,
        )
        planned = second_plan.plan.requests[0]
        repository.begin_attempt(lease, second_attempt, planned.authorization)
        clock.advance()
        second_metadata = scenario.raw_metadata.model_copy(
            update={
                "batch_id": UUID("00000000-0000-4000-8000-000000000004"),
                "retrieved_at": clock.value,
                "provider_request_id": "synthetic-request-2",
            }
        )
        repository.record_raw_artifact(
            lease,
            second_attempt,
            scenario.raw_identity,
            scenario.raw_published,
            second_metadata,
            observed_at=clock.value,
        )
        repository.complete_acquisition(
            lease,
            second_attempt,
            scenario.acquisition,
            completed_at=clock.value,
        )
        context = scenario.expectation.batch_context
        repository.record_batch_context(
            lease,
            second_attempt,
            scenario.expectation.specification,
            context,
            calendar_snapshot_id=deterministic_calendar_snapshot_id(_calendar()),
            provenance=scenario.expectation.provenance,
            recorded_at=clock.value,
        )
        prepared = repository.prepare_publication(
            lease,
            second_attempt,
            scenario.expectation,
            prepared_at=clock.value,
        )
        assert not prepared.replayed
        second_coverage = scenario.coverage.model_copy(update={"watermarks": ()})
        second_request = PublicationCommitRequest(
            request_instance_id=second_request_id,
            attempt_id=_SECOND_ATTEMPT_ID,
            acquisition_authorization=scenario.acquisition,
            expectation=scenario.expectation,
            manifest=scenario.manifest,
            published=scenario.published,
            coverage=second_coverage,
            terminal_status=RequestInstanceStatus.SUCCESS,
        )
        second = repository.commit_published_batch(lease, second_request)
        assert not second.replayed
        assert second.canonical_batch_id == first.canonical_batch_id
        assert repository.commit_published_batch(lease, second_request).replayed
        assert repository.reconcile_run(lease, _SECOND_RUN_ID) is IngestionRunStatus.SUCCESS

        with store.read_only_connection() as connection:
            assert connection.execute("SELECT count(*) FROM canonical_batches").fetchone()[0] == 1
            assert connection.execute("SELECT count(*) FROM canonical_files").fetchone()[0] == 1
            assert (
                connection.execute("SELECT count(*) FROM canonical_batch_streams").fetchone()[0]
                == 1
            )
            assert (
                connection.execute(
                    "SELECT count(*) FROM batch_publication_expectations"
                ).fetchone()[0]
                == 1
            )
            assert (
                connection.execute(
                    "SELECT count(*) FROM batch_publication_expectation_requests"
                ).fetchone()[0]
                == 2
            )
            assert (
                connection.execute("SELECT count(*) FROM canonical_batch_requests").fetchone()[0]
                == 2
            )
            assert connection.execute("SELECT count(*) FROM publication_commits").fetchone()[0] == 2
            assert (
                connection.execute("SELECT count(*) FROM request_terminal_proofs").fetchone()[0]
                == 2
            )
            assert connection.execute("SELECT count(*) FROM coverage_segments").fetchone()[0] == 1
            assert (
                connection.execute("SELECT count(*) FROM coverage_request_proofs").fetchone()[0]
                == 1
            )
            assert connection.execute("SELECT count(*) FROM watermarks").fetchone()[0] == 1

        with (
            pytest.raises(sqlite3.IntegrityError),
            store._leased_transaction(lease) as connection,
        ):
            connection.execute(
                "DELETE FROM request_terminal_proofs WHERE request_instance_id = ?",
                (str(second_request_id),),
            )
            connection.execute(
                """
                INSERT INTO request_terminal_proofs(
                    request_instance_id, attempt_id, canonical_batch_id,
                    coverage_commit_hash, terminal_status, completed_at
                ) VALUES (?, ?, ?, ?, 'SUCCESS', ?)
                """,
                (
                    str(second_request_id),
                    str(_ATTEMPT_ID),
                    second.canonical_batch_id,
                    second.coverage_commit_hash,
                    _format_utc(clock.value),
                ),
            )
        with store.read_only_connection() as connection:
            assert (
                connection.execute("SELECT count(*) FROM request_terminal_proofs").fetchone()[0]
                == 2
            )


def test_publication_rejects_manifest_changed_after_atomic_rename(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        manifest_path = root.root / scenario.published.manifest_relative_path
        changed = scenario.manifest.model_copy(
            update={
                "manifest_created_at": scenario.manifest.manifest_created_at + timedelta(seconds=1)
            }
        )
        manifest_path.write_text(changed.model_dump_json(), encoding="utf-8")

        with pytest.raises(
            ExecutionIntegrityError,
            match="caller manifest differs from the persisted canonical completion manifest",
        ):
            IngestionExecutionRepository(store).commit_published_batch(
                lease,
                scenario.commit_request(),
            )

        with store.read_only_connection() as connection:
            assert connection.execute("SELECT count(*) FROM publication_commits").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM watermarks").fetchone()[0] == 0


def test_publication_rejects_watermark_crossing_new_active_gap(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        gap = GapFinding(
            gap_id="gap-synthetic-active-before-frontier",
            stream_id=scenario.coverage.segments[0].stream_id,
            start=_START + timedelta(minutes=5),
            end=_END,
            gap_type=GapType.EXPECTED_OBSERVATION,
            status=GapStatus.OPEN,
            blocking=True,
            detected_at=clock.value,
            request_instance_id=str(scenario.request_id),
        )
        unsafe = scenario.commit_request().model_copy(
            update={"coverage": scenario.coverage.model_copy(update={"gaps": (gap,)})}
        )

        with pytest.raises(
            ExecutionIntegrityError,
            match="watermark frontier crosses an active blocking gap",
        ):
            IngestionExecutionRepository(store).commit_published_batch(lease, unsafe)

        with store.read_only_connection() as connection:
            assert connection.execute("SELECT count(*) FROM canonical_batches").fetchone()[0] == 0
            # ``_prepare_scenario`` seeds one repair target before publication. The unsafe
            # candidate gap is part of the rejected commit and must not survive its rollback.
            assert connection.execute("SELECT count(*) FROM gaps").fetchone()[0] == 1
            assert (
                connection.execute(
                    "SELECT count(*) FROM gaps WHERE gap_id = ?",
                    (gap.gap_id,),
                ).fetchone()[0]
                == 0
            )
            seeded = connection.execute(
                "SELECT status FROM gaps WHERE gap_id = 'gap-synthetic-repaired-1'"
            ).fetchone()
            assert seeded is not None and seeded[0] == "OPEN"
            assert connection.execute("SELECT count(*) FROM watermarks").fetchone()[0] == 0


@pytest.mark.parametrize(
    "point",
    [ExecutionFaultPoint.SQLITE_TRANSACTION, ExecutionFaultPoint.WATERMARK_UPDATE],
)
def test_publication_fault_rolls_back_every_operational_effect(
    tmp_path: Path,
    point: ExecutionFaultPoint,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        repository = IngestionExecutionRepository(store)

        def crash(candidate: ExecutionFaultPoint) -> None:
            if candidate is point:
                raise InjectedCrash(point.value)

        with pytest.raises(InjectedCrash, match=point.value):
            repository.commit_published_batch(
                lease,
                scenario.commit_request(),
                fault_injector=crash,
            )
        with store.read_only_connection() as connection:
            assert connection.execute("SELECT count(*) FROM canonical_batches").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM coverage_segments").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM watermarks").fetchone()[0] == 0
            assert (
                connection.execute("SELECT status FROM request_instances").fetchone()[0]
                == "PROCESSING"
            )

        recovered = repository.adopt_published_batch(
            lease,
            scenario.commit_request(source=PublicationCommitSource.RECOVERY_ADOPTION),
        )
        assert not recovered.replayed
        replayed_recovery = repository.adopt_published_batch(
            lease,
            scenario.commit_request(source=PublicationCommitSource.RECOVERY_ADOPTION),
        )
        assert replayed_recovery.replayed
        assert replayed_recovery.committed_at == recovered.committed_at


def test_run_summary_fault_does_not_rollback_committed_publication(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        repository = IngestionExecutionRepository(store)
        repository.commit_published_batch(lease, scenario.commit_request())

        with pytest.raises(InjectedCrash, match="RUN_COMPLETION"):
            repository.reconcile_run(
                lease,
                scenario.run_id,
                fault_injector=lambda point: (_ for _ in ()).throw(InjectedCrash(point.value)),
            )
        with store.read_only_connection() as connection:
            assert connection.execute("SELECT count(*) FROM publication_commits").fetchone()[0] == 1
            assert connection.execute("SELECT count(*) FROM watermarks").fetchone()[0] == 1
            assert (
                connection.execute("SELECT status FROM ingestion_runs").fetchone()[0] == "RUNNING"
            )
        assert repository.reconcile_run(lease, scenario.run_id).value == "SUCCESS"


def test_all_blocked_processing_can_fail_without_publication(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        repository = IngestionExecutionRepository(store)
        attempt = AttemptIdentity(
            attempt_id=scenario.attempt_id,
            request_instance_id=scenario.request_id,
            attempt_number=1,
        )
        failed = repository.fail_processing_request(
            lease,
            attempt,
            terminal_status=RequestInstanceStatus.FAILED,
            category="VALIDATION",
            code="ALL_STREAMS_BLOCKED",
            sanitized_message="Every requested stream failed deterministic validation.",
        )
        assert failed.request_status is RequestInstanceStatus.FAILED
        with store.read_only_connection() as connection:
            assert connection.execute("SELECT count(*) FROM canonical_batches").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM watermarks").fetchone()[0] == 0
            assert (
                connection.execute(
                    "SELECT state FROM batch_publication_expectation_requests"
                ).fetchone()[0]
                == "ABANDONED"
            )
        assert repository.reconcile_run(lease, scenario.run_id).value == "FAILED"


def test_retryable_attempt_failure_is_atomic_idempotent_and_never_sleeps(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        repository, lease, identity, _ = _prepare_running_attempt(store, clock)
        clock.advance()
        eligible_at = clock.value + timedelta(minutes=5)

        first = repository.record_attempt_failure(
            lease,
            identity,
            retryable=True,
            category="PROVIDER_TRANSIENT",
            code="RATE_LIMITED",
            sanitized_message="Provider requested a bounded retry delay.",
            failed_at=clock.value,
            next_eligible_at=eligible_at,
        )
        replay = repository.record_attempt_failure(
            lease,
            identity,
            retryable=True,
            category="PROVIDER_TRANSIENT",
            code="RATE_LIMITED",
            sanitized_message="Provider requested a bounded retry delay.",
            failed_at=clock.value + timedelta(seconds=1),
            next_eligible_at=eligible_at,
        )

        assert first.attempt_status is AttemptFailureStatus.RETRYABLE_FAILED
        assert first.request_status is RequestInstanceStatus.RETRY_WAIT
        assert first.retry_count == 1
        assert first.max_attempts == 3
        assert first.next_eligible_at == eligible_at
        assert not first.replayed
        assert replay.replayed
        assert replay.failed_at == first.failed_at
        with store.read_only_connection() as connection:
            assert connection.execute("SELECT count(*) FROM errors").fetchone()[0] == 1
            attempt = connection.execute(
                "SELECT status, completed_at, next_eligible_at FROM request_attempts"
            ).fetchone()
            retry = connection.execute("SELECT * FROM retry_state").fetchone()
            request = connection.execute(
                "SELECT status, completed_at FROM request_instances"
            ).fetchone()
        assert attempt is not None and tuple(attempt) == (
            "RETRYABLE_FAILED",
            _format_utc(clock.value),
            _format_utc(eligible_at),
        )
        assert retry is not None
        assert int(retry["retry_count"]) == 1
        assert retry["last_error_id"] == first.error_id
        assert retry["next_eligible_at"] == _format_utc(eligible_at)
        assert request is not None and tuple(request) == ("RETRY_WAIT", None)
        store.release_writer_lease(lease)


def test_fatal_attempt_failure_terminates_request_without_retry_eligibility(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        repository, lease, identity, _ = _prepare_running_attempt(store, clock)
        clock.advance()

        result = repository.record_attempt_failure(
            lease,
            identity,
            retryable=False,
            category="PROVIDER_FATAL",
            code="ENTITLEMENT_DENIED",
            sanitized_message="Provider entitlement does not authorize this request.",
            failed_at=clock.value,
        )

        assert result.attempt_status is AttemptFailureStatus.FATAL_FAILED
        assert result.request_status is RequestInstanceStatus.FAILED
        assert result.next_eligible_at is None
        with store.read_only_connection() as connection:
            assert (
                connection.execute("SELECT status FROM request_attempts").fetchone()[0]
                == "FATAL_FAILED"
            )
            assert (
                connection.execute("SELECT status FROM request_instances").fetchone()[0] == "FAILED"
            )
            assert (
                connection.execute("SELECT next_eligible_at FROM retry_state").fetchone()[0] is None
            )
        store.release_writer_lease(lease)


def test_retry_dispatch_enforces_eligibility_and_durable_max_attempts(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        repository, lease, first_identity, plan_request = _prepare_running_attempt(
            store,
            clock,
            max_attempts=2,
        )
        clock.advance()
        eligible_at = clock.value + timedelta(minutes=1)
        repository.record_attempt_failure(
            lease,
            first_identity,
            retryable=True,
            category="PROVIDER_TRANSIENT",
            code="TEMPORARILY_UNAVAILABLE",
            sanitized_message="Provider endpoint is temporarily unavailable.",
            failed_at=clock.value,
            next_eligible_at=eligible_at,
        )
        second_identity = AttemptIdentity(
            attempt_id=_SECOND_ATTEMPT_ID,
            request_instance_id=first_identity.request_instance_id,
            attempt_number=2,
        )
        with pytest.raises(ExecutionStateConflictError, match="not yet eligible"):
            repository.begin_attempt(
                lease,
                second_identity,
                plan_request.plan.requests[0].authorization,
            )

        clock.advance(timedelta(minutes=1))
        repository.begin_attempt(
            lease,
            second_identity,
            plan_request.plan.requests[0].authorization,
        )
        clock.advance()
        exhausted = repository.record_attempt_failure(
            lease,
            second_identity,
            retryable=True,
            category="PROVIDER_TRANSIENT",
            code="TEMPORARILY_UNAVAILABLE",
            sanitized_message="Provider endpoint is temporarily unavailable.",
            failed_at=clock.value,
            next_eligible_at=clock.value + timedelta(minutes=1),
        )

        assert exhausted.attempt_status is AttemptFailureStatus.RETRYABLE_FAILED
        assert exhausted.request_status is RequestInstanceStatus.FAILED
        assert exhausted.retry_count == exhausted.max_attempts == 2
        assert exhausted.next_eligible_at is None
        with store.read_only_connection() as connection:
            retry = connection.execute("SELECT * FROM retry_state").fetchone()
            assert retry is not None
            assert int(retry["retry_count"]) == 2
            assert retry["next_eligible_at"] is None
            assert (
                connection.execute("SELECT status FROM request_instances").fetchone()[0] == "FAILED"
            )
        store.release_writer_lease(lease)


def test_global_dispatch_ceiling_blocks_retry_after_other_request_consumes_budget(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    plan = _planner(enforcer).plan(
        intent=IngestionIntent.BACKFILL,
        streams=(_stream(),),
        instrument_mappings=(
            ProviderInstrumentMapping(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="SYNTHETIC-A",
            ),
        ),
        desired_start=_START,
        desired_end=_END,
        calendar_snapshot=_calendar(),
        coverage=(),
        limits=PlannerLimits(
            max_instruments_per_request=1,
            max_expected_observations_per_request=1,
            max_observations_per_page=1,
            max_pages_per_request=2,
            max_calls_per_request=2,
            max_estimated_bytes_per_request=10_000,
            estimated_bytes_per_observation=100,
            estimated_bytes_per_page=10,
            estimated_cost_per_call=Decimal("0.01"),
            max_estimated_cost_per_request=Decimal("0.02"),
        ),
        budget=PlannerBudget(
            max_calls=2,
            max_expected_observations=2,
            max_pages=2,
            max_estimated_bytes=10_000,
            max_estimated_cost=Decimal("0.02"),
        ),
        environment=RuntimeEnvironment.TEST,
        mapping_semantic_version="synthetic-bars-request-v1",
    )
    assert len(plan.requests) == 2
    persistence = PlanPersistenceRequest(
        run_id=_RUN_ID,
        calendar_snapshot_id=deterministic_calendar_snapshot_id(_calendar()),
        reason="global dispatch ceiling retry acceptance",
        max_attempts=2,
        max_pages=2,
        max_calls=2,
        max_pages_per_request=2,
        max_calls_per_request=2,
        plan=plan,
    )

    with OperationalStateStore.open(root, clock=clock) as store:
        lease = store.acquire_writer_lease("dispatch-ceiling-test", timedelta(minutes=30))
        CalendarSnapshotRepository(store).persist(lease, _calendar())
        plan_repository = IngestionPlanRepository(store)
        plan_repository.persist(lease, persistence)
        requests = plan_repository.load_progress(_RUN_ID).requests
        execution = IngestionExecutionRepository(store)

        first = AttemptIdentity(
            attempt_id=_ATTEMPT_ID,
            request_instance_id=requests[0].request_instance_id,
            attempt_number=1,
        )
        execution.begin_attempt(lease, first, plan.requests[0].authorization)
        execution.claim_provider_dispatch(lease, first, page_ordinal=0)
        execution.record_attempt_failure(
            lease,
            first,
            retryable=False,
            category="PROVIDER_FATAL",
            code="SYNTHETIC_FATAL",
            sanitized_message="Synthetic first request failed terminally.",
            failed_at=clock.value,
        )

        second = AttemptIdentity(
            attempt_id=_SECOND_ATTEMPT_ID,
            request_instance_id=requests[1].request_instance_id,
            attempt_number=1,
        )
        execution.begin_attempt(lease, second, plan.requests[1].authorization)
        execution.claim_provider_dispatch(lease, second, page_ordinal=0)
        execution.record_attempt_failure(
            lease,
            second,
            retryable=True,
            category="PROVIDER_TRANSIENT",
            code="SYNTHETIC_RETRY",
            sanitized_message="Synthetic second request is retryable.",
            failed_at=clock.value,
            next_eligible_at=clock.value,
        )
        retry = AttemptIdentity(
            attempt_id=UUID("20000000-0000-4000-8000-000000000003"),
            request_instance_id=requests[1].request_instance_id,
            attempt_number=2,
        )
        execution.begin_attempt(lease, retry, plan.requests[1].authorization)

        with pytest.raises(
            ProviderDispatchLimitExceededError,
            match="durable run provider-dispatch ceiling is exhausted",
        ):
            execution.claim_provider_dispatch(lease, retry, page_ordinal=0)

        with store.read_only_connection() as connection:
            claim_count = int(
                connection.execute("SELECT count(*) FROM ingestion_dispatch_claims").fetchone()[0]
            )
            allocated = connection.execute(
                "SELECT sum(max_pages), sum(max_calls) FROM request_execution_limits"
            ).fetchone()
        assert claim_count == 2
        assert allocated is not None and tuple(allocated) == (2, 2)


def _blocked_outcome(scenario: Scenario) -> CanonicalStreamOutcome:
    specification = scenario.expectation.specification
    return CanonicalStreamOutcome(
        stream=specification.stream_keys()[0],
        outcome=StreamPublicationOutcome.BLOCKED,
        request_start=specification.start,
        request_end=specification.end,
        row_count=0,
        validation_codes=("QUALITY:DUPLICATE_BAR",),
    )


@pytest.mark.parametrize(
    "fault_point",
    [
        PublicationFaultPoint.STAGING,
        PublicationFaultPoint.MANIFEST,
        PublicationFaultPoint.STAGED_MANIFEST_VERIFIED,
        PublicationFaultPoint.RENAME,
        PublicationFaultPoint.REOPEN,
    ],
)
def test_quarantine_manifest_publication_is_atomic_and_retryable(
    tmp_path: Path,
    fault_point: PublicationFaultPoint,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
        publisher = QuarantineArtifactPublisher(root, enforcer)
        outcome = _blocked_outcome(scenario)

        def crash(point: PublicationFaultPoint) -> None:
            if point is fault_point:
                raise InjectedCrash(point.value)

        with pytest.raises(InjectedCrash):
            publisher.publish(
                scenario.expectation.specification,
                scenario.expectation.batch_context,
                (outcome,),
                authorization=scenario.acquisition,
                environment=RuntimeEnvironment.TEST,
                fault_injector=crash,
            )
        recovered = publisher.publish(
            scenario.expectation.specification,
            scenario.expectation.batch_context,
            (outcome,),
            authorization=scenario.acquisition,
            environment=RuntimeEnvironment.TEST,
        )
        assert recovered is not None
        manifest_path = root.root / recovered.manifest_relative_path
        manifest = QuarantineArtifactManifest.model_validate_json(manifest_path.read_bytes())
        assert manifest.blocked_streams == (outcome,)
        assert tuple(manifest_path.parent.iterdir()) == (manifest_path,)
        assert b"synthetic-parquet-placeholder" not in manifest_path.read_bytes()
        assert recovered.created is (
            fault_point not in {PublicationFaultPoint.RENAME, PublicationFaultPoint.REOPEN}
        )


def test_quarantine_catalog_is_idempotent_and_bound_to_exact_attempt(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
        outcome = _blocked_outcome(scenario)
        published = QuarantineArtifactPublisher(root, enforcer).publish(
            scenario.expectation.specification,
            scenario.expectation.batch_context,
            (outcome,),
            authorization=scenario.acquisition,
            environment=RuntimeEnvironment.TEST,
        )
        assert published is not None
        manifest = QuarantineArtifactManifest.model_validate_json(
            (root.root / published.manifest_relative_path).read_bytes()
        )
        identity = AttemptIdentity(
            attempt_id=scenario.attempt_id,
            request_instance_id=scenario.request_id,
            attempt_number=1,
        )
        repository = QuarantineArtifactRepository(store, root, enforcer)
        first = repository.catalog(
            lease,
            identity,
            scenario.expectation.specification,
            scenario.acquisition,
            manifest,
            published,
            environment=RuntimeEnvironment.TEST,
        )
        replay = repository.catalog(
            lease,
            identity,
            scenario.expectation.specification,
            scenario.acquisition,
            manifest,
            published,
            environment=RuntimeEnvironment.TEST,
        )
        assert not first.replayed
        assert replay.replayed
        with store.read_only_connection() as connection:
            row = connection.execute("SELECT * FROM quarantine_artifacts").fetchone()
        assert row is not None
        assert row["state"] == "VERIFIED"
        assert row["relative_path"] == published.relative_directory
        with pytest.raises(QuarantineCatalogIntegrityError, match="differs"):
            repository.catalog(
                lease,
                identity,
                scenario.expectation.specification,
                scenario.acquisition,
                manifest,
                published.model_copy(update={"manifest_byte_count": 1}),
                environment=RuntimeEnvironment.TEST,
            )


def test_quarantine_policy_gate_denies_a_layer_without_explicit_permission(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        default = RetentionPolicyCatalog.load_default()
        policy = default.lookup("synthetic", "price_bars")
        denied = policy.model_copy(
            update={
                "normalized": policy.normalized.model_copy(update={"quarantine_allowed": False})
            }
        )
        document = default.document.model_copy(
            update={
                "policies": tuple(
                    denied if value == policy else value for value in default.document.policies
                )
            }
        )
        publisher = QuarantineArtifactPublisher(
            root,
            RetentionPolicyEnforcer(RetentionPolicyCatalog(document), clock=clock),
        )
        with pytest.raises(DatasetPolicyDenied, match="quarantine"):
            publisher.publish(
                scenario.expectation.specification,
                scenario.expectation.batch_context,
                (_blocked_outcome(scenario),),
                authorization=scenario.acquisition,
                environment=RuntimeEnvironment.TEST,
            )


def test_quarantine_filesystem_identity_is_independent_of_policy_revision(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        outcome = _blocked_outcome(scenario)
        artifact_id = deterministic_quarantine_artifact_id(
            specification=scenario.expectation.specification,
            batch_context=scenario.expectation.batch_context,
            blocked_streams=(outcome,),
        )
        revised_snapshot = scenario.acquisition.request.policy_snapshot.model_copy(
            update={
                "policy_revision": (
                    scenario.acquisition.request.policy_snapshot.policy_revision + 1
                ),
                "policy_hash": "f" * 64,
            }
        )
        assert deterministic_policy_snapshot_id(revised_snapshot) != (
            deterministic_policy_snapshot_id(scenario.acquisition.request.policy_snapshot)
        )
        published = QuarantineArtifactPublisher(
            root,
            RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock),
        ).publish(
            scenario.expectation.specification,
            scenario.expectation.batch_context,
            (outcome,),
            authorization=scenario.acquisition,
            environment=RuntimeEnvironment.TEST,
        )
        assert published is not None
        manifest_bytes = (root.root / published.manifest_relative_path).read_bytes()
        assert published.quarantine_artifact_id == artifact_id
        assert b"policy_snapshot" not in manifest_bytes
        assert b"policy_hash" not in manifest_bytes


def test_quarantine_diagnostics_detect_corruption_and_uncataloged_publication(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
        publisher = QuarantineArtifactPublisher(root, enforcer)
        published = publisher.publish(
            scenario.expectation.specification,
            scenario.expectation.batch_context,
            (_blocked_outcome(scenario),),
            authorization=scenario.acquisition,
            environment=RuntimeEnvironment.TEST,
        )
        assert published is not None
        diagnostics = Phase2OperationalDiagnostics(root, store, clock=clock)
        orphan_check = next(
            value for value in diagnostics.verify().checks if value.code == "PUBLISHED_ORPHANS"
        )
        assert "UNCATALOGED_QUARANTINE_PUBLICATION" in orphan_check.issue_codes

        lease = store.get_writer_lease()
        assert lease is not None
        manifest_path = root.root / published.manifest_relative_path
        manifest = QuarantineArtifactManifest.model_validate_json(manifest_path.read_bytes())
        QuarantineArtifactRepository(store, root, enforcer).catalog(
            lease,
            AttemptIdentity(
                attempt_id=scenario.attempt_id,
                request_instance_id=scenario.request_id,
                attempt_number=1,
            ),
            scenario.expectation.specification,
            scenario.acquisition,
            manifest,
            published,
            environment=RuntimeEnvironment.TEST,
        )
        assert diagnostics.status().quarantine_artifact_count == 1
        healthy_check = next(
            value
            for value in diagnostics.verify().checks
            if value.code == "QUARANTINE_CATALOG_CONTENT"
        )
        assert healthy_check.status is DiagnosticStatus.PASS

        manifest_path.write_bytes(b"{}\n")
        corrupted = next(
            value
            for value in diagnostics.verify().checks
            if value.code == "QUARANTINE_CATALOG_CONTENT"
        )
        assert corrupted.status is DiagnosticStatus.FAIL
        assert "QUARANTINE_FILE_OR_MANIFEST_INVALID" in corrupted.issue_codes


def _calendar_replacement(
    *,
    library_version: str,
    first_close: datetime = _END,
    extend_range: bool = False,
) -> CalendarSnapshot:
    sessions = [
        CalendarSession(
            session_date=date(2025, 7, 2),
            open_utc=_START,
            close_utc=first_close,
        )
    ]
    if extend_range:
        sessions.append(
            CalendarSession(
                session_date=date(2026, 1, 2),
                open_utc=datetime(2026, 1, 2, 14, 30, tzinfo=UTC),
                close_utc=datetime(2026, 1, 2, 14, 40, tzinfo=UTC),
            )
        )
    return CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version=library_version,
        tzdata_version="synthetic",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=date(2025, 7, 2),
        range_end=date(2026, 1, 3) if extend_range else date(2025, 7, 3),
        generated_at=_NOW + timedelta(days=1),
        sessions=sessions,
    )


@pytest.mark.parametrize("extend_range", [False, True])
def test_calendar_equivalent_upgrade_rebinds_coverage_and_watermark_with_ledger(
    tmp_path: Path,
    extend_range: bool,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    old_calendar_id = deterministic_calendar_snapshot_id(_calendar())
    target = _calendar_replacement(library_version="2", extend_range=extend_range)
    target_id = deterministic_calendar_snapshot_id(target)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        clock.advance()

        result = CalendarSnapshotRepository(store).persist_reconciling(lease, target)

        assert result.snapshot_id == target_id
        assert result.stale_snapshot_ids == (old_calendar_id,)
        assert result.affected_session_dates == ()
        assert result.stale_coverage_ids == ()
        assert result.rebound_coverage_ids == (scenario.coverage.segments[0].coverage_id,)
        assert result.stale_watermark_stream_ids == ()
        assert result.rebound_watermark_stream_ids == (scenario.coverage.segments[0].stream_id,)
        assert result.calendar_stale_gap_ids == ()
        with store.read_only_connection() as connection:
            assert (
                connection.execute(
                    "SELECT state FROM calendar_snapshots WHERE calendar_snapshot_id = ?",
                    (old_calendar_id,),
                ).fetchone()[0]
                == "STALE"
            )
            coverage = connection.execute(
                """
                SELECT calendar_snapshot_id, verification_state, invalidated_at
                FROM coverage_segments WHERE coverage_id = ?
                """,
                (scenario.coverage.segments[0].coverage_id,),
            ).fetchone()
            assert tuple(coverage) == (target_id, "VERIFIED", None)
            watermark = connection.execute(
                """
                SELECT calendar_snapshot_id, verification_state, invalidated_at
                FROM watermarks WHERE stream_id = ?
                """,
                (scenario.coverage.segments[0].stream_id,),
            ).fetchone()
            assert tuple(watermark) == (target_id, "VERIFIED", None)
            assert (
                connection.execute("SELECT count(*) FROM calendar_coverage_rebindings").fetchone()[
                    0
                ]
                == 1
            )
            assert (
                connection.execute("SELECT count(*) FROM calendar_watermark_rebindings").fetchone()[
                    0
                ]
                == 1
            )
            assert (
                connection.execute(
                    """
                SELECT count(*) FROM coverage_segments
                WHERE calendar_snapshot_id = ? AND verification_state = 'VERIFIED'
                """,
                    (old_calendar_id,),
                ).fetchone()[0]
                == 0
            )

        replay = CalendarSnapshotRepository(store).persist_reconciling(lease, target)
        assert replay.stale_snapshot_ids == ()
        with store.read_only_connection() as connection:
            assert (
                connection.execute(
                    "SELECT count(*) FROM calendar_snapshot_reconciliations"
                ).fetchone()[0]
                == 1
            )
        proofs = RestartProjectionReader(store).load_stream_proofs(
            (scenario.coverage.segments[0].stream_id,)
        )
        assert proofs.coverage[0].calendar_snapshot_id == target_id
        assert proofs.coverage[0].verification_state is CoverageVerificationState.VERIFIED
        assert proofs.watermarks[0].calendar_snapshot_id == target_id
        store.release_writer_lease(lease)

    with OperationalStateStore.open(root, clock=clock) as reopened:
        proofs = RestartProjectionReader(reopened).load_stream_proofs(
            (scenario.coverage.segments[0].stream_id,)
        )
        assert proofs.coverage[0].calendar_snapshot_id == target_id
        assert proofs.watermarks[0].calendar_snapshot_id == target_id


def test_calendar_schedule_change_stales_only_affected_facts_and_creates_linked_gap(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    old_calendar_id = deterministic_calendar_snapshot_id(_calendar())
    target = _calendar_replacement(
        library_version="2",
        first_close=_END + timedelta(minutes=5),
    )
    target_id = deterministic_calendar_snapshot_id(target)
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        clock.advance()

        result = CalendarSnapshotRepository(store).persist_reconciling(lease, target)

        assert result.affected_session_dates == (date(2025, 7, 2),)
        assert result.stale_coverage_ids == (scenario.coverage.segments[0].coverage_id,)
        assert result.rebound_coverage_ids == ()
        assert result.stale_watermark_stream_ids == (scenario.coverage.segments[0].stream_id,)
        assert len(result.calendar_stale_gap_ids) == 1
        with store.read_only_connection() as connection:
            coverage = connection.execute(
                """
                SELECT calendar_snapshot_id, verification_state, invalidated_at
                FROM coverage_segments WHERE coverage_id = ?
                """,
                (scenario.coverage.segments[0].coverage_id,),
            ).fetchone()
            assert coverage[0] == old_calendar_id
            assert coverage[1] == "STALE"
            assert coverage[2] is not None
            watermark = connection.execute(
                """
                SELECT calendar_snapshot_id, verification_state, invalidated_at
                FROM watermarks WHERE stream_id = ?
                """,
                (scenario.coverage.segments[0].stream_id,),
            ).fetchone()
            assert watermark[0] == old_calendar_id
            assert watermark[1] == "STALE"
            assert watermark[2] is not None
            gap = connection.execute(
                """
                SELECT gap.*, provenance.session_date,
                       reconciliation.source_calendar_snapshot_id,
                       reconciliation.target_calendar_snapshot_id
                FROM gaps AS gap
                JOIN calendar_stale_gap_provenance AS provenance USING(gap_id)
                JOIN calendar_snapshot_reconciliations AS reconciliation
                  USING(reconciliation_id)
                WHERE gap.gap_id = ?
                """,
                (result.calendar_stale_gap_ids[0],),
            ).fetchone()
            assert gap["gap_type"] == "CALENDAR_STALE"
            assert gap["request_instance_id"] == str(scenario.request_id)
            assert gap["session_date"] == "2025-07-02"
            assert gap["source_calendar_snapshot_id"] == old_calendar_id
            assert gap["target_calendar_snapshot_id"] == target_id
            assert (
                connection.execute(
                    "SELECT coverage_id FROM calendar_stale_gap_coverage WHERE gap_id = ?",
                    (result.calendar_stale_gap_ids[0],),
                ).fetchone()[0]
                == scenario.coverage.segments[0].coverage_id
            )
            assert (
                connection.execute(
                    """
                SELECT count(*) FROM watermarks
                WHERE calendar_snapshot_id = ? AND verification_state = 'VERIFIED'
                """,
                    (old_calendar_id,),
                ).fetchone()[0]
                == 0
            )
        proofs = RestartProjectionReader(store).load_stream_proofs(
            (scenario.coverage.segments[0].stream_id,)
        )
        assert proofs.coverage[0].verification_state is CoverageVerificationState.STALE
        assert any(gap.gap_id == result.calendar_stale_gap_ids[0] for gap in proofs.gaps)
        assert proofs.watermarks[0].verification_state is CoverageVerificationState.STALE
        store.release_writer_lease(lease)


def test_calendar_change_outside_verified_interval_rebinds_without_false_staleness(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    extended = _calendar_replacement(library_version="2", extend_range=True)
    changed_uncovered_session = CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="3",
        tzdata_version="synthetic",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=extended.range_start,
        range_end=extended.range_end,
        generated_at=_NOW + timedelta(days=2),
        sessions=(
            extended.sessions[0],
            extended.sessions[1].model_copy(
                update={"close_utc": extended.sessions[1].close_utc + timedelta(minutes=5)}
            ),
        ),
    )
    with OperationalStateStore.open(root, clock=clock) as store:
        scenario = _prepare_scenario(root, store, clock)
        lease = store.get_writer_lease()
        assert lease is not None
        IngestionExecutionRepository(store).commit_published_batch(
            lease,
            scenario.commit_request(),
        )
        clock.advance()
        CalendarSnapshotRepository(store).persist_reconciling(lease, extended)
        clock.advance()

        result = CalendarSnapshotRepository(store).persist_reconciling(
            lease,
            changed_uncovered_session,
        )

        assert result.affected_session_dates == (date(2026, 1, 2),)
        assert result.stale_coverage_ids == ()
        assert result.stale_watermark_stream_ids == ()
        assert result.rebound_coverage_ids == (scenario.coverage.segments[0].coverage_id,)
        assert result.rebound_watermark_stream_ids == (scenario.coverage.segments[0].stream_id,)
        assert result.calendar_stale_gap_ids == ()
        proofs = RestartProjectionReader(store).load_stream_proofs(
            (scenario.coverage.segments[0].stream_id,)
        )
        target_id = deterministic_calendar_snapshot_id(changed_uncovered_session)
        assert proofs.coverage[0].calendar_snapshot_id == target_id
        assert proofs.coverage[0].verification_state is CoverageVerificationState.VERIFIED
        assert proofs.watermarks[0].calendar_snapshot_id == target_id
        assert proofs.watermarks[0].verification_state is CoverageVerificationState.VERIFIED
        store.release_writer_lease(lease)
