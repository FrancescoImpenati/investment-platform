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
    CalendarSnapshotRepository,
    CoverageCommit,
    ExecutionFaultPoint,
    ExecutionIdentityCollisionError,
    ExecutionIntegrityError,
    IngestionExecutionRepository,
    IngestionPlanRepository,
    IngestionRunStatus,
    PlanPersistenceRequest,
    PublicationCommitRequest,
    PublicationCommitSource,
    RequestInstanceStatus,
    deterministic_calendar_snapshot_id,
    deterministic_policy_snapshot_id,
)
from investment_platform.data.operational.store import OperationalStateStore, _format_utc
from investment_platform.data.provenance import (
    DataSource,
    LicenseClassification,
    RawBatchMetadata,
)
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
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
    PublishedCanonicalBatch,
    PublishedRawArtifact,
    RawArtifactPublisher,
    RawProvenanceBinding,
    StreamPublicationOutcome,
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
