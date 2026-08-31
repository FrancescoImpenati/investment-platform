"""Application service for restartable Phase 2 living ingestion.

The service is the small orchestration layer between deterministic planning,
provider acquisition, immutable filesystem publication, and the operational
SQLite repositories.  It deliberately owns no provider or storage semantics:
all sensitive decisions are delegated to the retention, calendar, publication,
and operational-state boundaries.

Execution is at least once.  Durable request, artifact, batch, and observation
identities make retries idempotent; this module does not claim exactly-once
provider execution.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from math import isfinite
from pathlib import Path
from typing import Final
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from investment_platform.data.calendar import CalendarSnapshot, XNYSCalendar
from investment_platform.data.ingestion.acquisition import (
    AcquiredRawPage,
    BarPageProvider,
    RawAcquisitionError,
    RawAcquisitionService,
)
from investment_platform.data.ingestion.commands import (
    IngestionCommandOutcome,
    IngestionCommandRequest,
    IngestionCommandResult,
    IngestionCommandRunner,
)
from investment_platform.data.ingestion.coverage import GapType
from investment_platform.data.ingestion.identity import (
    AttemptIdentity,
    BarSemantics,
    DataKind,
    ProviderInstrumentMapping,
    RawArtifactIdentity,
    RequestSpecification,
    StreamKey,
)
from investment_platform.data.ingestion.planner import (
    CoverageVerificationState,
    IngestionIntent,
    IngestionPlan,
    IngestionPlanner,
    PlannerBudget,
    PlannerLimits,
    RepairStrategy,
)
from investment_platform.data.ingestion.processing import (
    PreparedCanonicalBatch,
    RawProcessingPage,
    prepare_alpaca_sip_batch_context,
    prepare_alpaca_sip_canonical_batch_from_context,
)
from investment_platform.data.models import Timeframe, TradingSession
from investment_platform.data.operational.budget import (
    BudgetReservationState,
    ProviderBudgetExceededError,
    ProviderBudgetRepository,
    ProviderBudgetReservation,
    ProviderBudgetReservationRequest,
    ProviderBudgetWindow,
)
from investment_platform.data.operational.coverage_commit import (
    build_blocking_acquisition_gaps,
    build_blocking_integrity_gaps,
    build_publication_coverage_commit,
)
from investment_platform.data.operational.execution import (
    CoverageCommit,
    ExecutionStateConflictError,
    IngestionExecutionRepository,
    ProviderDispatchLimitExceededError,
    PublicationCommitRequest,
    PublicationCommitSource,
    SemanticNoOpCommitRequest,
    SemanticNoOpObservationProof,
)
from investment_platform.data.operational.execution import (
    FaultInjector as ExecutionFaultInjector,
)
from investment_platform.data.operational.planning import (
    IngestionPlanRepository,
    IngestionRunStatus,
    PlanPersistenceRequest,
    RequestInstanceStatus,
    deterministic_policy_snapshot_id,
    deterministic_request_instance_identity,
)
from investment_platform.data.operational.quarantine import QuarantineArtifactRepository
from investment_platform.data.operational.query import (
    CandidateBatchDisposition,
    CatalogBarQueryRepository,
)
from investment_platform.data.operational.replay import (
    CanonicalLossState,
    OperationalReplayRepository,
    RawReplayOperation,
    RawReplayOperationResult,
    RawReplayOperationStatus,
    ReplayableAcquisition,
)
from investment_platform.data.operational.repository import CalendarSnapshotRepository
from investment_platform.data.operational.restart import (
    AttemptStatus,
    PublicationRecoveryState,
    RestartAction,
    RestartAttemptProjection,
    RestartProjectionIntegrityError,
    RestartProjectionReader,
    RestartRequestProjection,
    RestartRunContext,
    StreamProofProjection,
)
from investment_platform.data.operational.store import (
    OperationalStateStore,
    WriterLease,
    WriterLeaseError,
)
from investment_platform.data.phase1_bakeoff import PHASE1_SECURITIES, Phase1Security
from investment_platform.data.provenance import FileRawPayload, RawBatch
from investment_platform.data.providers.alpaca import AlpacaFeed, AlpacaProvider
from investment_platform.data.providers.errors import (
    ProviderError,
    ProviderHttpError,
    ProviderRateLimitError,
    ProviderTransportError,
)
from investment_platform.data.providers.http import SpoolingUrllibHttpTransport
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    DatasetRuntimeStatus,
    RetentionLayer,
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage import (
    CanonicalBatchManifest,
    CanonicalBatchPublisher,
    PublishedCanonicalBatch,
    QuarantineArtifactManifest,
    QuarantineArtifactPublisher,
    RawArtifactPublisher,
    verify_canonical_batch_directory,
)
from investment_platform.data.storage._publication import (
    FaultInjector as PublicationFaultInjector,
)
from investment_platform.data.storage.transport_spool import TransportSpoolFaultInjector
from investment_platform.data_root import PrivateDataRoot
from investment_platform.runtime import RuntimeEnvironment, RuntimeSettings

_LEASE_TTL: Final = timedelta(minutes=30)
_XNYS: Final = ZoneInfo("America/New_York")
_ALPACA_CALL_LIMIT_PER_MINUTE: Final = 200
_RETRY_BASE_DELAY: Final = timedelta(seconds=5)
_RETRY_MAX_DELAY: Final = timedelta(minutes=15)
_RETRYABLE_HTTP_STATUSES: Final = frozenset({500, 502, 503, 504})
_TERMINAL_REQUESTS: Final = frozenset(
    {
        RequestInstanceStatus.SUCCESS,
        RequestInstanceStatus.PARTIAL,
        RequestInstanceStatus.FAILED,
        RequestInstanceStatus.BLOCKED,
        RequestInstanceStatus.CANCELLED,
    }
)
_TERMINAL_RUNS: Final = frozenset(
    {
        IngestionRunStatus.SUCCESS,
        IngestionRunStatus.PARTIAL,
        IngestionRunStatus.FAILED,
        IngestionRunStatus.CANCELLED,
    }
)


class LivingIngestionServiceError(RuntimeError):
    """A safe application-level orchestration invariant failed."""


class LivingIngestionIncomplete(LivingIngestionServiceError):
    """Durable work exists but is not currently eligible to advance."""


class LivingIngestionFaultPoint(StrEnum):
    """Application boundaries not covered by filesystem/SQLite injectors."""

    PLAN_PERSISTED = "plan_persisted"
    PROVIDER_BUDGET_CONSUMED = "provider_budget_consumed"
    RAW_PAGE_CATALOGED = "raw_page_cataloged"
    ACQUISITION_COMPLETED = "acquisition_completed"
    BATCH_CONTEXT_RECORDED = "batch_context_recorded"
    PUBLICATION_PREPARED = "publication_prepared"
    FILESYSTEM_PUBLISHED = "filesystem_published"
    SQLITE_COMMITTED = "sqlite_committed"


type LivingFaultInjector = Callable[[LivingIngestionFaultPoint], None]
type UuidFactory = Callable[[], UUID]
type Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class LivingIngestionFaults:
    """Injected crash boundaries for deterministic offline acceptance tests."""

    service: LivingFaultInjector | None = None
    raw_publication: PublicationFaultInjector | None = None
    canonical_publication: PublicationFaultInjector | None = None
    operational: ExecutionFaultInjector | None = None
    transport_spool: TransportSpoolFaultInjector | None = None


@dataclass(frozen=True, slots=True)
class LivingIngestionRunRequest:
    """Complete non-secret input for planning and executing one durable run."""

    intent: IngestionIntent
    streams: tuple[StreamKey, ...]
    instrument_mappings: tuple[ProviderInstrumentMapping, ...]
    desired_start: datetime
    desired_end: datetime
    calendar_snapshot: CalendarSnapshot
    limits: PlannerLimits
    budget: PlannerBudget
    environment: RuntimeEnvironment
    mapping_semantic_version: str
    reason: str
    max_attempts: int = 3
    repair_strategy: RepairStrategy | None = None
    repair_reason: str | None = None
    runtime_status: DatasetRuntimeStatus | None = None
    run_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class LivingIngestionRunResult:
    """Sanitized durable outcome; contains no provider values or credentials."""

    run_id: UUID
    status: IngestionRunStatus
    no_op: bool
    planned_request_count: int
    completed_request_count: int
    raw_artifact_count: int
    canonical_batch_count: int
    open_gap_count: int


@dataclass(frozen=True, slots=True)
class _FailureDisposition:
    retryable: bool
    category: str
    code: str
    sanitized_message: str
    retry_after_seconds: float | None = None


def _invoke(injector: LivingFaultInjector | None, point: LivingIngestionFaultPoint) -> None:
    if injector is not None:
        injector(point)


def _aware_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise LivingIngestionServiceError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _exact_non_negative_integer(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    candidate = int(value)
    if candidate < 0 or candidate != value:
        return None
    return candidate


def _classify_acquisition_failure(
    error: ProviderError | RawAcquisitionError,
) -> _FailureDisposition:
    if isinstance(error, ProviderRateLimitError):
        return _FailureDisposition(
            retryable=True,
            category="RATE_LIMIT",
            code="PROVIDER_RATE_LIMITED",
            sanitized_message="The provider rate limit deferred this bounded request.",
            retry_after_seconds=error.retry_after_seconds,
        )
    if isinstance(error, ProviderTransportError):
        return _FailureDisposition(
            retryable=True,
            category="TRANSPORT",
            code="PROVIDER_TRANSPORT_FAILED",
            sanitized_message="The provider transport failed before a complete response.",
        )
    if isinstance(error, ProviderHttpError) and error.status_code in _RETRYABLE_HTTP_STATUSES:
        return _FailureDisposition(
            retryable=True,
            category="PROVIDER_HTTP",
            code="PROVIDER_HTTP_RETRYABLE",
            sanitized_message="The provider returned a retryable server response.",
            retry_after_seconds=error.retry_after_seconds,
        )
    if isinstance(error, ProviderError):
        return _FailureDisposition(
            retryable=False,
            category="PROVIDER",
            code="PROVIDER_REQUEST_FATAL",
            sanitized_message="The provider rejected or could not validate the bounded request.",
        )
    return _FailureDisposition(
        retryable=False,
        category="ACQUISITION",
        code="RAW_ACQUISITION_INVALID",
        sanitized_message="The raw response chain failed bounded acquisition validation.",
    )


def _retry_delay(
    identity: AttemptIdentity,
    *,
    retry_after_seconds: float | None,
) -> timedelta:
    exponent = min(identity.attempt_number - 1, 8)
    base_seconds = min(
        _RETRY_MAX_DELAY.total_seconds(),
        _RETRY_BASE_DELAY.total_seconds() * (2**exponent),
    )
    jitter_fraction = int.from_bytes(identity.attempt_id.bytes[-2:]) / 65_535
    local_backoff_seconds = min(
        base_seconds * (1 + 0.2 * jitter_fraction),
        _RETRY_MAX_DELAY.total_seconds(),
    )
    if (
        retry_after_seconds is not None
        and isfinite(retry_after_seconds)
        and retry_after_seconds >= 0
    ):
        return timedelta(seconds=max(local_backoff_seconds, retry_after_seconds))
    return timedelta(seconds=local_backoff_seconds)


class LivingIngestionService:
    """Coordinate planning, execution, publication, and restart recovery."""

    def __init__(
        self,
        *,
        data_root: PrivateDataRoot,
        store: OperationalStateStore,
        provider: BarPageProvider | None,
        policy_enforcer: RetentionPolicyEnforcer,
        clock: Clock | None = None,
        uuid_factory: UuidFactory = uuid4,
        lease_owner_id: str | None = None,
        lease_ttl: timedelta = _LEASE_TTL,
    ) -> None:
        if lease_ttl <= timedelta(0):
            raise ValueError("lease_ttl must be positive")
        self._data_root = data_root
        self._store = store
        self._policy_enforcer = policy_enforcer
        self._clock = clock or (lambda: datetime.now(UTC))
        self._uuid_factory = uuid_factory
        self._lease_owner_id = lease_owner_id or f"living-ingestion-{uuid_factory()}"
        self._lease_ttl = lease_ttl
        self._calendar_repository = CalendarSnapshotRepository(store)
        self._plan_repository = IngestionPlanRepository(store)
        self._execution_repository = IngestionExecutionRepository(store)
        self._budget_repository = ProviderBudgetRepository(store)
        self._replay_repository = OperationalReplayRepository(store)
        self._restart_reader = RestartProjectionReader(store)
        self._planner = IngestionPlanner(policy_enforcer)
        self._raw_publisher = RawArtifactPublisher(data_root, policy_enforcer)
        self._raw_acquisition = (
            None
            if provider is None
            else RawAcquisitionService(
                provider,
                self._raw_publisher,
                policy_enforcer,
                clock=self._clock,
            )
        )
        self._canonical_publisher = CanonicalBatchPublisher(data_root, policy_enforcer)
        self._quarantine_publisher = QuarantineArtifactPublisher(data_root, policy_enforcer)
        self._quarantine_repository = QuarantineArtifactRepository(
            store,
            data_root,
            policy_enforcer,
        )

    def _now(self) -> datetime:
        return _aware_utc(self._clock(), label="service clock")

    def run(
        self,
        request: LivingIngestionRunRequest,
        *,
        faults: LivingIngestionFaults | None = None,
    ) -> LivingIngestionRunResult:
        """Persist a deterministic plan, then execute or resume all bounded requests."""

        faults = faults or LivingIngestionFaults()
        run_id = request.run_id or self._uuid_factory()
        lease = self._store.acquire_writer_lease(self._lease_owner_id, self._lease_ttl)
        try:
            if request.intent is IngestionIntent.UPDATE:
                try:
                    reconciled = self._replay_repository.reconcile_stream_integrity(
                        lease,
                        tuple(sorted(stream.stream_id for stream in request.streams)),
                        detected_at=self._now(),
                    )
                    if any(
                        result.state is not CanonicalLossState.HEALTHY
                        for result in reconciled
                    ):
                        raise LivingIngestionIncomplete("INTEGRITY_REPAIR_REQUIRED")
                    proofs = self._restart_reader.load_optional_stream_proofs(
                        tuple(sorted(stream.stream_id for stream in request.streams))
                    )
                except RestartProjectionIntegrityError as error:
                    raise LivingIngestionIncomplete(
                        "INTEGRITY_REPAIR_REQUIRED"
                    ) from error
                if proofs is None:
                    raise LivingIngestionIncomplete("NO_COVERAGE_ORIGIN")
                if any(
                    gap.gap_type is GapType.INTEGRITY and gap.actively_blocks
                    for gap in proofs.gaps
                ):
                    raise LivingIngestionIncomplete("INTEGRITY_REPAIR_REQUIRED")
            calendar_snapshot_id = self._calendar_repository.persist(
                lease,
                request.calendar_snapshot,
            )
            coverage = self._plan_repository.load_planner_coverage(
                request.streams,
                calendar_snapshot_id=calendar_snapshot_id,
            )
            plan = self._planner.plan(
                intent=request.intent,
                streams=request.streams,
                instrument_mappings=request.instrument_mappings,
                desired_start=request.desired_start,
                desired_end=request.desired_end,
                calendar_snapshot=request.calendar_snapshot,
                coverage=coverage.windows,
                limits=request.limits,
                budget=request.budget,
                environment=request.environment,
                mapping_semantic_version=request.mapping_semantic_version,
                runtime_status=request.runtime_status,
                repair_strategy=request.repair_strategy,
                repair_reason=request.repair_reason,
            )
            self._plan_repository.persist(
                lease,
                PlanPersistenceRequest(
                    run_id=run_id,
                    calendar_snapshot_id=calendar_snapshot_id,
                    reason=request.reason,
                    max_attempts=request.max_attempts,
                    max_pages=min(request.budget.max_pages, 1000),
                    max_calls=min(request.budget.max_calls, 1000),
                    max_pages_per_request=min(
                        request.limits.max_pages_per_request,
                        1000,
                    ),
                    max_calls_per_request=min(
                        request.limits.max_calls_per_request,
                        1000,
                    ),
                    plan=plan,
                ),
            )
            _invoke(faults.service, LivingIngestionFaultPoint.PLAN_PERSISTED)
            return self._execute(
                run_id, lease, faults=faults, runtime_status=request.runtime_status
            )
        finally:
            self._release_lease(lease)

    def reconcile_integrity(self, streams: Sequence[StreamKey]) -> bool:
        """Recheck selected verified support without constructing a provider."""

        stream_ids = tuple(sorted({stream.stream_id for stream in streams}))
        if not stream_ids or len(stream_ids) != len(streams):
            raise ValueError("integrity reconciliation requires unique streams")
        lease = self._store.acquire_writer_lease(self._lease_owner_id, self._lease_ttl)
        try:
            results = self._replay_repository.reconcile_stream_integrity(
                lease,
                stream_ids,
                detected_at=self._now(),
            )
            return any(result.state is not CanonicalLossState.HEALTHY for result in results)
        finally:
            self._release_lease(lease)

    def resume(
        self,
        run_id: UUID,
        *,
        runtime_status: DatasetRuntimeStatus | None = None,
        faults: LivingIngestionFaults | None = None,
    ) -> LivingIngestionRunResult:
        """Resume only from durable plan/provenance state after process restart."""

        lease = self._store.acquire_writer_lease(self._lease_owner_id, self._lease_ttl)
        try:
            return self._execute(
                run_id,
                lease,
                faults=faults or LivingIngestionFaults(),
                runtime_status=runtime_status,
            )
        finally:
            self._release_lease(lease)

    def run_raw_replay(
        self,
        specification: RequestSpecification,
        *,
        operation_id: UUID | None = None,
        runtime_status: DatasetRuntimeStatus | None = None,
        faults: LivingIngestionFaults | None = None,
    ) -> RawReplayOperationResult:
        """Execute one dedicated, durable canonical-loss repair with zero network I/O."""

        identifier = operation_id or self._uuid_factory()
        lease = self._store.acquire_writer_lease(self._lease_owner_id, self._lease_ttl)
        try:
            self._replay_repository.plan_raw_replay_operation(
                lease,
                identifier,
                specification,
                requested_at=self._now(),
            )
            return self._execute_raw_replay(
                identifier,
                lease,
                runtime_status=runtime_status,
                faults=faults or LivingIngestionFaults(),
            )
        finally:
            self._release_lease(lease)

    def resume_raw_replay(
        self,
        operation_id: UUID,
        *,
        runtime_status: DatasetRuntimeStatus | None = None,
        faults: LivingIngestionFaults | None = None,
    ) -> RawReplayOperationResult:
        """Resume a PLANNED/RUNNING zero-network operation from its durable identity."""

        lease = self._store.acquire_writer_lease(self._lease_owner_id, self._lease_ttl)
        try:
            return self._execute_raw_replay(
                operation_id,
                lease,
                runtime_status=runtime_status,
                faults=faults or LivingIngestionFaults(),
            )
        finally:
            self._release_lease(lease)

    def load_raw_replay_operation(self, operation_id: UUID) -> RawReplayOperation | None:
        """Expose sanitized durable operation state for scheduler-safe resume dispatch."""

        return self._replay_repository.load_raw_replay_operation(operation_id)

    def _execute_raw_replay(
        self,
        operation_id: UUID,
        lease: WriterLease,
        *,
        runtime_status: DatasetRuntimeStatus | None,
        faults: LivingIngestionFaults,
    ) -> RawReplayOperationResult:
        operation = self._replay_repository.start_raw_replay_operation(
            lease,
            operation_id,
            started_at=self._now(),
        )
        if operation.status is not RawReplayOperationStatus.RUNNING:
            raise LivingIngestionIncomplete(f"RAW_REPLAY_{operation.status.value}")
        replay = self._replay_repository.find_latest_replayable_acquisition(
            operation.specification,
            operation.eligibility,
        )
        if replay is None:
            raise LivingIngestionIncomplete("RAW_REPLAY_SOURCE_UNAVAILABLE")
        reconciliation = self._replay_repository.reconcile_canonical_loss(
            lease,
            operation.eligibility.canonical_batch_id,
            detected_at=self._now(),
        )
        durable = self._replay_repository.load_batch_preparation(reconciliation.batch_context_id)
        if durable is None or (
            durable.specification != operation.specification
            or durable.batch_context != reconciliation.expectation.batch_context
        ):
            raise LivingIngestionServiceError(
                "raw replay preparation differs from canonical-loss evidence"
            )
        calendar = self._calendar_repository.load(durable.calendar_snapshot_id)
        authorization = replay.source_authorization
        self._policy_enforcer.authorize_persistence(
            operation.specification.provider,
            operation.specification.dataset,
            environment=authorization.request.environment,
            layer=RetentionLayer.NORMALIZED,
            runtime_status=runtime_status,
            acquisition_authorization=authorization,
            input_artifacts=authorization.ordered_artifacts,
            input_page_sha256=authorization.ordered_page_sha256,
        )
        prepared = prepare_alpaca_sip_canonical_batch_from_context(
            specification=operation.specification,
            pages=self._replay_processing_pages(replay),
            acquisition_authorization=authorization,
            calendar_snapshot=calendar,
            batch_context=durable.batch_context,
            provenance=durable.provenance,
        )
        if prepared.expectation != reconciliation.expectation:
            raise LivingIngestionServiceError(
                "raw replay output differs from the immutable canonical expectation"
            )
        published = self._canonical_publisher.publish(
            operation.specification,
            prepared.batch_context,
            prepared.parts,
            prepared.stream_outcomes,
            authorization=authorization,
            calendar_snapshot=calendar,
            provenance=prepared.provenance,
            runtime_status=runtime_status,
            fault_injector=faults.canonical_publication,
        )
        if published is None:
            raise LivingIngestionServiceError("raw replay produced no canonical publication")
        _invoke(faults.service, LivingIngestionFaultPoint.FILESYSTEM_PUBLISHED)
        manifest = self._verify_published(published, prepared)
        result = self._replay_repository.complete_raw_replay_operation(
            lease,
            operation_id,
            prepared.expectation,
            manifest,
            published,
            completed_at=max(self._now(), manifest.manifest_created_at),
        )
        _invoke(faults.service, LivingIngestionFaultPoint.SQLITE_COMMITTED)
        return result

    def _release_lease(self, lease: WriterLease) -> None:
        with suppress(WriterLeaseError):
            self._store.release_writer_lease(lease)

    def _heartbeat(self, lease: WriterLease) -> None:
        self._store.renew_writer_lease(lease, self._lease_ttl)

    def _execute(
        self,
        run_id: UUID,
        lease: WriterLease,
        *,
        faults: LivingIngestionFaults,
        runtime_status: DatasetRuntimeStatus | None,
    ) -> LivingIngestionRunResult:
        iterations = 0
        while True:
            iterations += 1
            if iterations > 10_000:
                raise LivingIngestionServiceError("restart state did not converge")
            self._heartbeat(lease)
            context = self._restart_reader.load_run(run_id)
            unfinished = tuple(
                request for request in context.requests if request.status not in _TERMINAL_REQUESTS
            )
            if not unfinished:
                if context.status not in _TERMINAL_RUNS:
                    self._execution_repository.reconcile_run(
                        lease,
                        run_id,
                        fault_injector=faults.operational,
                    )
                    continue
                return self._result(self._restart_reader.load_run(run_id))

            current = unfinished[0]
            if current.action is RestartAction.DISPATCH:
                identity = AttemptIdentity.create(
                    deterministic_request_instance_identity(
                        context.run_id,
                        current.plan_ordinal,
                        current.specification,
                    ),
                    attempt_number=1,
                    uuid_factory=self._uuid_factory,
                )
                self._execution_repository.begin_attempt(
                    lease,
                    identity,
                    current.authorization,
                )
                self._acquire(
                    context,
                    current,
                    identity,
                    lease,
                    faults=faults,
                    runtime_status=runtime_status,
                    allow_raw_replay=True,
                    recover_raw_orphans=False,
                )
                continue
            if current.action is RestartAction.RETRY_DISPATCH:
                identity = AttemptIdentity.create(
                    deterministic_request_instance_identity(
                        context.run_id,
                        current.plan_ordinal,
                        current.specification,
                    ),
                    attempt_number=current.retry_count + 1,
                    uuid_factory=self._uuid_factory,
                )
                self._execution_repository.begin_attempt(
                    lease,
                    identity,
                    current.authorization,
                )
                self._acquire(
                    context,
                    current,
                    identity,
                    lease,
                    faults=faults,
                    runtime_status=runtime_status,
                    allow_raw_replay=True,
                    recover_raw_orphans=False,
                )
                continue
            if current.action is RestartAction.RESUME_ACQUISITION:
                latest = self._require_latest(current)
                identity = AttemptIdentity(
                    attempt_id=latest.attempt_id,
                    request_instance_id=current.request_instance_id,
                    attempt_number=latest.attempt_number,
                )
                self._acquire(
                    context,
                    current,
                    identity,
                    lease,
                    faults=faults,
                    runtime_status=runtime_status,
                    allow_raw_replay=False,
                    recover_raw_orphans=True,
                )
                continue
            if current.action in {
                RestartAction.REPLAY_RAW,
                RestartAction.RESUME_PROCESSING,
                RestartAction.ADOPT_PUBLICATION,
            }:
                self._process(
                    context,
                    current,
                    lease,
                    faults=faults,
                    runtime_status=runtime_status,
                    adoption=current.action is RestartAction.ADOPT_PUBLICATION,
                )
                continue
            if current.action is RestartAction.WAIT_RETRY:
                # Returning durable RUNNING state keeps the run ID visible to
                # callers and schedulers; the service never sleeps while holding
                # its writer lease.  A later explicit resume dispatches only once
                # the persisted eligibility timestamp has arrived.
                return self._result(context)
            if current.action is RestartAction.POLICY_BLOCKED:
                raise LivingIngestionIncomplete(current.action.value)
            if current.action is RestartAction.CALENDAR_BLOCKED:
                raise LivingIngestionIncomplete("CALENDAR_BLOCKED")
            raise LivingIngestionServiceError(
                f"unfinished request has unsupported restart action {current.action.value}"
            )

    @staticmethod
    def _require_latest(request: RestartRequestProjection) -> RestartAttemptProjection:
        latest = request.latest_attempt
        if latest is None:
            raise LivingIngestionServiceError("restart action lacks an attempt")
        return latest

    def _acquire(
        self,
        context: RestartRunContext,
        request: RestartRequestProjection,
        identity: AttemptIdentity,
        lease: WriterLease,
        *,
        faults: LivingIngestionFaults,
        runtime_status: DatasetRuntimeStatus | None,
        allow_raw_replay: bool,
        recover_raw_orphans: bool,
    ) -> None:
        if allow_raw_replay and self._try_adopt_raw_replay(
            context,
            request,
            identity,
            lease,
            runtime_status=runtime_status,
        ):
            _invoke(faults.service, LivingIngestionFaultPoint.ACQUISITION_COMPLETED)
            return
        if self._raw_acquisition is None:
            raise LivingIngestionServiceError("network acquisition provider is not configured")
        calendar = self._calendar_repository.load(context.calendar_snapshot_id)
        if recover_raw_orphans:
            for artifact_identity, published in self._raw_publisher.list_verified_for_request(
                request.specification
            ):
                self._execution_repository.catalog_raw_orphan(
                    lease,
                    identity,
                    request.specification,
                    artifact_identity,
                    published,
                    verified_at=self._now(),
                )
        reservations: dict[int, ProviderBudgetReservation] = {}

        def before_dispatch(page_ordinal: int) -> None:
            reservation = self._reserve_provider_budget(context, request, identity, lease)
            try:
                self._execution_repository.claim_provider_dispatch(
                    lease,
                    identity,
                    page_ordinal=page_ordinal,
                )
            except Exception:
                # The durable reservation is still RESERVED, which proves the
                # network boundary was not crossed.  Release only this capacity;
                # dispatch claims, once created, remain conservatively counted.
                self._budget_repository.release_before_dispatch(
                    lease,
                    reservation.reservation_id,
                )
                raise
            self._budget_repository.consume_before_dispatch(
                lease,
                reservation.reservation_id,
            )
            reservations[page_ordinal] = reservation
            _invoke(faults.service, LivingIngestionFaultPoint.PROVIDER_BUDGET_CONSUMED)

        def catalog_page(page: AcquiredRawPage) -> None:
            self._heartbeat(lease)
            try:
                self._execution_repository.load_raw_replay(
                    identity.attempt_id,
                    page.identity.artifact_id,
                )
            except ExecutionStateConflictError:
                metadata = page.raw_batch.metadata
                if not page.published.created:
                    try:
                        metadata = self._execution_repository.load_verified_artifact_replay(
                            page.identity.artifact_id
                        ).metadata
                    except ExecutionStateConflictError:
                        # A crash after the raw directory rename/reopen can leave
                        # verified immutable bytes without a catalog row.  The
                        # retry has just re-inspected those exact bytes, so bind
                        # them to the current attempt's real retrieval metadata;
                        # never fabricate provenance for the interrupted process.
                        metadata = page.raw_batch.metadata
                self._execution_repository.record_raw_artifact(
                    lease,
                    identity,
                    page.identity,
                    page.published,
                    metadata,
                    observed_at=max(self._now(), page.authorization.authorized_at),
                )
            reservation = reservations.get(page.page_ordinal)
            if reservation is None:
                raise LivingIngestionServiceError(
                    "raw page lacks its durable pre-dispatch budget proof"
                )
            self._observe_provider_budget(lease, reservation, page)
            _invoke(faults.service, LivingIngestionFaultPoint.RAW_PAGE_CATALOGED)

        try:
            planned = context.plan.requests[request.plan_ordinal]
            if planned.specification != request.specification:
                raise LivingIngestionServiceError(
                    "durable request projection differs from its deterministic plan"
                )
            acquisition = self._raw_acquisition.acquire(
                request.specification,
                request.authorization,
                calendar,
                runtime_status=runtime_status,
                attempt_id=identity.attempt_id,
                max_pages=request.max_pages,
                max_calls=request.max_calls,
                before_dispatch=before_dispatch,
                on_page_persisted=catalog_page,
                fault_injector=faults.raw_publication,
                transport_fault_injector=faults.transport_spool,
            )
        except ProviderBudgetExceededError:
            self._record_budget_wait(lease, request, identity)
            return
        except ProviderDispatchLimitExceededError:
            self._record_dispatch_limit_failure(lease, request, identity)
            return
        except ProviderError as error:
            self._record_acquisition_failure(lease, request, identity, error)
            return
        except RawAcquisitionError as error:
            self._record_acquisition_failure(lease, request, identity, error)
            return
        self._execution_repository.complete_acquisition(
            lease,
            identity,
            acquisition.authorization,
            completed_at=max(self._now(), acquisition.authorization.authorized_at),
        )
        _invoke(faults.service, LivingIngestionFaultPoint.ACQUISITION_COMPLETED)

    def _try_adopt_raw_replay(
        self,
        context: RestartRunContext,
        request: RestartRequestProjection,
        identity: AttemptIdentity,
        lease: WriterLease,
        *,
        runtime_status: DatasetRuntimeStatus | None,
    ) -> bool:
        """Adopt retained raw only for a ledger-proven canonical-only repair."""

        if (
            context.plan.intent is not IngestionIntent.REPAIR
            or context.plan.repair_strategy is not RepairStrategy.MISSING_ONLY
        ):
            return False
        eligibility = self._replay_repository.find_replay_eligibility(request.specification)
        if eligibility is None:
            return False
        replay = self._replay_repository.find_latest_replayable_acquisition(
            request.specification,
            eligibility,
        )
        if replay is None:
            return False
        # The immutable descriptors were inspected and authorized on the source
        # acquisition.  Binding them to this run's current request authorization
        # is checked again against the active exact policy by adopt_acquisition.
        authorization = AcquisitionPolicyAuthorization(
            request=request.authorization,
            ordered_artifacts=replay.source_authorization.ordered_artifacts,
            pagination_complete=True,
            terminal_page_verified=True,
            authorized_at=max(self._now(), request.authorization.authorized_at),
        )
        self._policy_enforcer.authorize_persistence(
            request.specification.provider,
            request.specification.dataset,
            environment=request.authorization.environment,
            layer=RetentionLayer.NORMALIZED,
            runtime_status=runtime_status,
            acquisition_authorization=authorization,
            input_artifacts=authorization.ordered_artifacts,
            input_page_sha256=authorization.ordered_page_sha256,
        )
        self._replay_repository.adopt_acquisition(
            lease,
            identity,
            authorization,
            replay,
            adopted_at=max(self._now(), authorization.authorized_at),
        )
        return True

    def _record_acquisition_failure(
        self,
        lease: WriterLease,
        request: RestartRequestProjection,
        identity: AttemptIdentity,
        error: ProviderError | RawAcquisitionError,
    ) -> None:
        disposition = _classify_acquisition_failure(error)
        failed_at = self._now()
        next_eligible_at = None
        if disposition.retryable:
            next_eligible_at = failed_at + _retry_delay(
                identity,
                retry_after_seconds=disposition.retry_after_seconds,
            )
        terminal = not disposition.retryable or identity.attempt_number >= request.max_attempts
        self._execution_repository.record_attempt_failure(
            lease,
            identity,
            retryable=disposition.retryable,
            category=disposition.category,
            code=disposition.code,
            sanitized_message=disposition.sanitized_message,
            failed_at=failed_at,
            next_eligible_at=next_eligible_at,
            blocking_gaps=(
                build_blocking_acquisition_gaps(
                    streams=request.specification.stream_keys(),
                    start=request.specification.start,
                    end=request.specification.end,
                    request_instance_id=request.request_instance_id,
                    detected_at=failed_at,
                )
                if terminal
                else ()
            ),
        )

    def _record_budget_wait(
        self,
        lease: WriterLease,
        request: RestartRequestProjection,
        identity: AttemptIdentity,
    ) -> None:
        failed_at = self._now()
        active = self._budget_repository.active_snapshot(
            provider=request.specification.provider,
            dataset=request.specification.dataset,
            budget_key="historical_sip_calls",
            at=failed_at,
        )
        next_eligible_at = (
            active.window.window_end
            if active is not None
            else failed_at.replace(second=0, microsecond=0) + timedelta(minutes=1)
        )
        terminal = identity.attempt_number >= request.max_attempts
        self._execution_repository.record_attempt_failure(
            lease,
            identity,
            retryable=True,
            category="RATE_LIMIT",
            code="PROVIDER_BUDGET_EXHAUSTED",
            sanitized_message="The durable provider request budget deferred this bounded request.",
            failed_at=failed_at,
            next_eligible_at=next_eligible_at,
            blocking_gaps=(
                build_blocking_acquisition_gaps(
                    streams=request.specification.stream_keys(),
                    start=request.specification.start,
                    end=request.specification.end,
                    request_instance_id=request.request_instance_id,
                    detected_at=failed_at,
                )
                if terminal
                else ()
            ),
        )

    def _record_dispatch_limit_failure(
        self,
        lease: WriterLease,
        request: RestartRequestProjection,
        identity: AttemptIdentity,
    ) -> None:
        failed_at = self._now()
        self._execution_repository.record_attempt_failure(
            lease,
            identity,
            retryable=False,
            category="BUDGET",
            code="DISPATCH_LIMIT_EXHAUSTED",
            sanitized_message="The durable request or run provider-dispatch ceiling is exhausted.",
            failed_at=failed_at,
            blocking_gaps=build_blocking_acquisition_gaps(
                streams=request.specification.stream_keys(),
                start=request.specification.start,
                end=request.specification.end,
                request_instance_id=request.request_instance_id,
                detected_at=failed_at,
            ),
        )

    def _reserve_provider_budget(
        self,
        context: RestartRunContext,
        request: RestartRequestProjection,
        identity: AttemptIdentity,
        lease: WriterLease,
    ) -> ProviderBudgetReservation:
        budget_key = "historical_sip_calls"
        now = self._now()
        previous = self._budget_repository.latest_for_attempt(
            identity.attempt_id,
            budget_key=budget_key,
        )
        if previous is not None and previous.state is BudgetReservationState.RESERVED:
            if previous.window.window_start <= now < previous.window.window_end:
                # A RESERVED dispatch has provably not crossed the network boundary:
                # every call is counted by consume_before_dispatch first.  Reusing it
                # preserves the idempotent reserve-before-dispatch transition.
                return previous
            self._budget_repository.release_before_dispatch(
                lease,
                previous.reservation_id,
            )

        # A CONSUMED reservation means a call may already have reached Alpaca.
        # Any restart which performs network I/O therefore receives a new durable
        # dispatch ordinal and is counted again, even when the payload effect later
        # proves idempotent.  This intentionally prefers over-counting to forgetting
        # provider load after a crash.
        dispatch_ordinal = 1 if previous is None else previous.dispatch_ordinal + 1
        window_start = now.replace(second=0, microsecond=0)
        active = self._budget_repository.active_snapshot(
            provider=request.specification.provider,
            dataset=request.specification.dataset,
            budget_key=budget_key,
            at=now,
        )
        window = (
            active.window
            if active is not None
            else ProviderBudgetWindow(
                provider=request.specification.provider,
                dataset=request.specification.dataset,
                budget_key=budget_key,
                window_start=window_start,
                window_end=window_start + timedelta(minutes=1),
                limit_count=_ALPACA_CALL_LIMIT_PER_MINUTE,
            )
        )
        return self._budget_repository.reserve(
            lease,
            ProviderBudgetReservationRequest(
                request_instance_id=request.request_instance_id,
                attempt_id=identity.attempt_id,
                dispatch_ordinal=dispatch_ordinal,
                window=window,
                amount=1,
            ),
        )

    def _observe_provider_budget(
        self,
        lease: WriterLease,
        reservation: ProviderBudgetReservation,
        page: AcquiredRawPage,
    ) -> None:
        metadata = page.raw_batch.metadata.request_metadata
        capacity = _exact_non_negative_integer(metadata.get("rate_limit_capacity"))
        remaining = _exact_non_negative_integer(metadata.get("rate_limit_remaining"))
        if capacity is None or remaining is None or capacity <= 0 or remaining > capacity:
            return
        self._budget_repository.observe_remaining(
            lease,
            reservation.reservation_id,
            capacity=capacity,
            remaining=remaining,
            observed_at=max(self._now(), page.raw_batch.metadata.retrieved_at),
        )

    def _process(
        self,
        context: RestartRunContext,
        request: RestartRequestProjection,
        lease: WriterLease,
        *,
        faults: LivingIngestionFaults,
        runtime_status: DatasetRuntimeStatus | None,
        adoption: bool,
    ) -> None:
        latest = self._require_latest(request)
        if latest.status is not AttemptStatus.RAW_COMPLETE:
            raise LivingIngestionServiceError("processing requires durable RAW_COMPLETE evidence")
        authorization = latest.acquisition_authorization
        if authorization is None:
            raise LivingIngestionServiceError("processing lacks acquisition authorization")
        identity = AttemptIdentity(
            attempt_id=latest.attempt_id,
            request_instance_id=request.request_instance_id,
            attempt_number=latest.attempt_number,
        )
        calendar = self._calendar_repository.load(context.calendar_snapshot_id)
        pages = self._raw_processing_pages(identity.attempt_id, authorization)

        fixed_time = authorization.authorized_at
        prepared_context = prepare_alpaca_sip_batch_context(
            specification=request.specification,
            pages=pages,
            acquisition_authorization=authorization,
            calendar_snapshot=calendar,
            fixed_ingested_at=fixed_time,
            manifest_created_at=fixed_time,
        )
        existing_context = self._replay_repository.load_batch_preparation(
            prepared_context.batch_context.batch_context_id
        )
        context_to_record = prepared_context
        if existing_context is not None:
            context_to_record = replace(
                prepared_context,
                batch_context=existing_context.batch_context,
                provenance=existing_context.provenance,
            )
        self._execution_repository.record_batch_context(
            lease,
            identity,
            request.specification,
            context_to_record.batch_context,
            calendar_snapshot_id=context.calendar_snapshot_id,
            provenance=context_to_record.provenance,
            recorded_at=max(self._now(), fixed_time),
        )
        _invoke(faults.service, LivingIngestionFaultPoint.BATCH_CONTEXT_RECORDED)
        durable_context = self._replay_repository.load_batch_preparation(
            context_to_record.batch_context.batch_context_id
        )
        if durable_context is None or (
            durable_context.specification != request.specification
            or durable_context.batch_context != context_to_record.batch_context
            or durable_context.provenance != context_to_record.provenance
            or durable_context.calendar_snapshot_id != context.calendar_snapshot_id
        ):
            raise LivingIngestionServiceError(
                "persisted batch preparation differs from its verified raw context"
            )
        prepared = prepare_alpaca_sip_canonical_batch_from_context(
            specification=request.specification,
            pages=pages,
            acquisition_authorization=authorization,
            calendar_snapshot=calendar,
            batch_context=durable_context.batch_context,
            provenance=durable_context.provenance,
        )
        if (
            request.publication is not None
            and request.publication.expectation is not None
            and prepared.expectation != request.publication.expectation
        ):
            raise LivingIngestionServiceError(
                "replayed processing differs from its durable publication expectation"
            )

        comparison = None
        if prepared.parts:
            comparison = CatalogBarQueryRepository(
                self._store,
                self._data_root,
                self._policy_enforcer,
                environment=context.plan.environment,
            ).classify_candidate(
                provider=request.specification.provider,
                dataset=request.specification.dataset,
                parts=prepared.parts,
                stream_outcomes=prepared.stream_outcomes,
                processing_signature=(prepared.batch_context.batch_identity.processing_signature),
            )
            annotated_outcomes = comparison.apply_stream_counts(prepared.stream_outcomes)
            prepared = replace(
                prepared,
                stream_outcomes=annotated_outcomes,
                expectation=prepared.expectation.model_copy(update={"streams": annotated_outcomes}),
            )
        self._catalog_quarantine(
            lease,
            identity,
            request,
            prepared,
            authorization,
            runtime_status=runtime_status,
            fault_injector=faults.canonical_publication,
        )
        if prepared.all_blocked:
            completed_at = max(self._now(), fixed_time)
            self._execution_repository.fail_processing_request(
                lease,
                identity,
                terminal_status=RequestInstanceStatus.FAILED,
                category="VALIDATION",
                code="ALL_STREAMS_BLOCKED",
                sanitized_message="Every requested stream failed deterministic validation.",
                completed_at=completed_at,
                blocking_gaps=build_blocking_integrity_gaps(
                    stream_outcomes=prepared.stream_outcomes,
                    request_instance_id=request.request_instance_id,
                    detected_at=completed_at,
                ),
            )
            return
        if comparison is not None and comparison.disposition is CandidateBatchDisposition.BLOCKED:
            completed_at = max(self._now(), fixed_time)
            self._execution_repository.fail_processing_request(
                lease,
                identity,
                terminal_status=RequestInstanceStatus.PARTIAL,
                category="VALIDATION",
                code="PARTIAL_STREAMS_BLOCKED",
                sanitized_message=(
                    "Some requested streams failed deterministic validation; "
                    "duplicate observations remain supported by existing canonical batches."
                ),
                completed_at=completed_at,
                blocking_gaps=build_blocking_integrity_gaps(
                    stream_outcomes=prepared.stream_outcomes,
                    request_instance_id=request.request_instance_id,
                    detected_at=completed_at,
                ),
            )
            return
        if (
            comparison is not None
            and comparison.disposition is CandidateBatchDisposition.SEMANTIC_NO_OP
        ):
            self._execution_repository.commit_semantic_noop(
                lease,
                SemanticNoOpCommitRequest(
                    identity=identity,
                    specification=request.specification,
                    acquisition_authorization=authorization,
                    processing_signature=(
                        prepared.batch_context.batch_identity.processing_signature
                    ),
                    batch_context_id=prepared.batch_context.batch_context_id,
                    duplicate_observations=tuple(
                        SemanticNoOpObservationProof(
                            observation_id=value.observation_id,
                            value_fingerprint=value.value_fingerprint,
                            stream_id=value.stream_id,
                            start=value.start,
                            end=value.end,
                            matching_supporting_batch_ids=(value.matching_canonical_batch_ids),
                        )
                        for value in comparison.semantic_duplicate_slots
                    ),
                ),
                committed_at=max(self._now(), fixed_time),
            )
            _invoke(faults.service, LivingIngestionFaultPoint.SQLITE_COMMITTED)
            return

        self._execution_repository.prepare_publication(
            lease,
            identity,
            prepared.expectation,
            prepared_at=max(self._now(), fixed_time),
        )
        _invoke(faults.service, LivingIngestionFaultPoint.PUBLICATION_PREPARED)

        self._heartbeat(lease)
        published = self._canonical_publisher.publish(
            request.specification,
            prepared.batch_context,
            prepared.parts,
            prepared.stream_outcomes,
            authorization=authorization,
            calendar_snapshot=calendar,
            provenance=prepared.provenance,
            runtime_status=runtime_status,
            fault_injector=faults.canonical_publication,
        )
        if published is None:
            raise LivingIngestionServiceError(
                "publishable processing outcome produced no canonical publication"
            )
        _invoke(faults.service, LivingIngestionFaultPoint.FILESYSTEM_PUBLISHED)
        manifest = self._verify_published(published, prepared)
        raw_adoption = self._replay_repository.load_adopted_replay(identity.attempt_id)
        if raw_adoption is not None:
            if (
                raw_adoption.eligibility.canonical_batch_id
                != prepared.batch_context.canonical_batch_id
            ):
                raise LivingIngestionServiceError(
                    "adopted raw replay differs from the canonical recovery target"
                )
            self._replay_repository.reactivate_identical_canonical_batch(
                lease,
                identity,
                prepared.expectation,
                manifest,
                published,
                verified_at=max(self._now(), manifest.manifest_created_at),
            )
            _invoke(faults.service, LivingIngestionFaultPoint.SQLITE_COMMITTED)
            return
        coverage = self._coverage_commit(
            context,
            request,
            prepared,
            manifest,
            runtime_status=runtime_status,
        )
        commit = PublicationCommitRequest(
            request_instance_id=request.request_instance_id,
            attempt_id=identity.attempt_id,
            acquisition_authorization=authorization,
            expectation=prepared.expectation,
            manifest=manifest,
            published=published,
            coverage=coverage,
            terminal_status=RequestInstanceStatus(
                coverage.segments[0].request_terminal_state.value
            ),
            source=(
                PublicationCommitSource.RECOVERY_ADOPTION
                if adoption
                else PublicationCommitSource.NORMAL
            ),
        )
        if adoption:
            self._execution_repository.adopt_published_batch(
                lease,
                commit,
                fault_injector=faults.operational,
            )
        else:
            self._execution_repository.commit_published_batch(
                lease,
                commit,
                fault_injector=faults.operational,
            )
        _invoke(faults.service, LivingIngestionFaultPoint.SQLITE_COMMITTED)

    def _raw_processing_pages(
        self,
        attempt_id: UUID,
        authorization: AcquisitionPolicyAuthorization,
    ) -> tuple[RawProcessingPage, ...]:
        pages: list[RawProcessingPage] = []
        for descriptor in authorization.ordered_artifacts:
            identity = RawArtifactIdentity.from_digest(
                request_spec_hash=descriptor.request_spec_hash,
                page_ordinal=descriptor.page_ordinal,
                page_relation=descriptor.page_relation,
                media_type=descriptor.media_type,
                content_encoding=descriptor.content_encoding,
                content_sha256=descriptor.content_sha256,
                byte_count=descriptor.byte_count,
            )
            replay = self._execution_repository.load_raw_replay(
                attempt_id,
                identity.artifact_id,
            )
            payload_path = self._data_root.managed_path(
                replay.payload_relative_path,
                expected_root_id=self._canonical_publisher.root_id,
            )
            pages.append(
                RawProcessingPage(
                    identity=identity,
                    batch=RawBatch(
                        metadata=replay.metadata,
                        payload=FileRawPayload(payload_path),
                    ),
                )
            )
        return tuple(pages)

    def _catalog_quarantine(
        self,
        lease: WriterLease,
        identity: AttemptIdentity,
        request: RestartRequestProjection,
        prepared: PreparedCanonicalBatch,
        authorization: AcquisitionPolicyAuthorization,
        *,
        runtime_status: DatasetRuntimeStatus | None,
        fault_injector: PublicationFaultInjector | None,
    ) -> None:
        published = self._quarantine_publisher.publish(
            request.specification,
            prepared.batch_context,
            prepared.stream_outcomes,
            authorization=authorization,
            environment=authorization.request.environment,
            runtime_status=runtime_status,
            fault_injector=fault_injector,
        )
        if published is None:
            return
        manifest_path = self._data_root.managed_path(
            published.manifest_relative_path,
            expected_root_id=published.root_id,
        )
        try:
            manifest = QuarantineArtifactManifest.model_validate_json(manifest_path.read_bytes())
        except (OSError, ValueError) as error:
            raise LivingIngestionServiceError(
                "published quarantine manifest cannot be reopened"
            ) from error
        self._quarantine_repository.catalog(
            lease,
            identity,
            request.specification,
            authorization,
            manifest,
            published,
            environment=authorization.request.environment,
            runtime_status=runtime_status,
        )

    def _replay_processing_pages(
        self,
        replay: ReplayableAcquisition,
    ) -> tuple[RawProcessingPage, ...]:
        pages: list[RawProcessingPage] = []
        for descriptor, record in zip(
            replay.source_authorization.ordered_artifacts,
            replay.ordered_raw,
            strict=True,
        ):
            identity = RawArtifactIdentity.from_digest(
                request_spec_hash=descriptor.request_spec_hash,
                page_ordinal=descriptor.page_ordinal,
                page_relation=descriptor.page_relation,
                media_type=descriptor.media_type,
                content_encoding=descriptor.content_encoding,
                content_sha256=descriptor.content_sha256,
                byte_count=descriptor.byte_count,
            )
            if identity.artifact_id != record.artifact_id:
                raise LivingIngestionServiceError(
                    "raw replay record differs from its authorized artifact identity"
                )
            payload_path = self._data_root.managed_path(
                record.payload_relative_path,
                expected_root_id=self._canonical_publisher.root_id,
            )
            pages.append(
                RawProcessingPage(
                    identity=identity,
                    batch=RawBatch(
                        metadata=record.metadata,
                        payload=FileRawPayload(payload_path),
                    ),
                )
            )
        return tuple(pages)

    def _verify_published(
        self,
        published: PublishedCanonicalBatch,
        prepared: PreparedCanonicalBatch,
    ) -> CanonicalBatchManifest:
        directory = self._data_root.managed_path(
            published.relative_directory,
            expected_root_id=published.root_id,
        )
        return verify_canonical_batch_directory(
            directory,
            data_root=self._data_root,
            root_id=published.root_id,
            expected_semantics=prepared.expectation,
            expected_provider=prepared.specification.provider,
            expected_dataset=prepared.specification.dataset,
            expected_batch_id=prepared.batch_context.canonical_batch_id,
        )

    def _coverage_commit(
        self,
        context: RestartRunContext,
        request: RestartRequestProjection,
        prepared: PreparedCanonicalBatch,
        manifest: CanonicalBatchManifest,
        *,
        runtime_status: DatasetRuntimeStatus | None,
    ) -> CoverageCommit:
        stream_ids = tuple(sorted(stream.stream_id for stream in context.plan.streams))
        proofs = self._restart_reader.load_stream_proofs(stream_ids)
        policy = self._policy_enforcer.authorize_processing(
            context.plan.provider,
            context.plan.dataset,
            environment=context.plan.environment,
            runtime_status=runtime_status,
        )
        verified_at = max(self._now(), manifest.manifest_created_at)
        return build_publication_coverage_commit(
            manifest=manifest,
            parts=prepared.parts,
            calendar_snapshot_id=context.calendar_snapshot_id,
            policy_snapshot_id=deterministic_policy_snapshot_id(
                context.plan.policy_authorization.policy_snapshot
            ),
            policy=policy,
            runtime_status=runtime_status,
            run_id=context.run_id,
            request_instance_id=request.request_instance_id,
            coverage_start=self._coverage_start(context.plan, proofs),
            frontier_domain_end=max(
                (
                    request.specification.end,
                    *(segment.end for segment in proofs.coverage),
                    *(watermark.exclusive_frontier for watermark in proofs.watermarks),
                )
            ),
            verified_at=verified_at,
            existing_segments=proofs.coverage,
            existing_gaps=proofs.gaps,
            existing_watermarks=proofs.watermarks,
        )

    @staticmethod
    def _coverage_start(plan: IngestionPlan, proofs: StreamProofProjection) -> datetime:
        origins = {
            *(segment.coverage_start for segment in proofs.coverage),
            *(watermark.coverage_start for watermark in proofs.watermarks),
        }
        if len(origins) > 1:
            raise LivingIngestionServiceError("durable streams disagree on coverage origin")
        slots = tuple(
            sorted(
                (
                    slot
                    for request in plan.requests
                    for slot in request.expected_slots
                    if slot.start_utc >= plan.desired_start
                ),
                key=lambda value: (value.start_utc, value.end_utc),
            )
        )
        if not slots:
            raise LivingIngestionServiceError("plan has no eligible coverage origin")
        planned_origin = slots[0].start_utc
        if origins:
            durable_origin = next(iter(origins))
            if plan.intent is IngestionIntent.BACKFILL:
                return min(planned_origin, durable_origin)
            return durable_origin
        return planned_origin

    def _result(self, context: RestartRunContext) -> LivingIngestionRunResult:
        raw_ids = {
            artifact_id
            for request in context.requests
            if request.latest_attempt is not None
            for artifact_id in request.latest_attempt.ordered_artifact_ids
        }
        canonical_ids = {
            request.publication.canonical_batch_id
            for request in context.requests
            if request.publication is not None
            and request.publication.state is PublicationRecoveryState.CATALOGED
        }
        completed = sum(request.status in _TERMINAL_REQUESTS for request in context.requests)
        if context.plan.is_no_op:
            return LivingIngestionRunResult(
                run_id=context.run_id,
                status=context.status,
                no_op=True,
                planned_request_count=0,
                completed_request_count=0,
                raw_artifact_count=0,
                canonical_batch_count=0,
                open_gap_count=0,
            )
        proofs = self._restart_reader.load_stream_proofs(context.plan.stream_ids)
        return LivingIngestionRunResult(
            run_id=context.run_id,
            status=context.status,
            no_op=False,
            planned_request_count=len(context.requests),
            completed_request_count=completed,
            raw_artifact_count=len(raw_ids),
            canonical_batch_count=len(canonical_ids),
            open_gap_count=sum(gap.actively_blocks for gap in proofs.gaps),
        )


class _ProductionCommandRunner(IngestionCommandRunner):
    """Construct short-lived production services without retaining credentials in DTOs."""

    def __init__(self, settings: RuntimeSettings, repository_root: Path) -> None:
        settings.require_provider_access()
        root_path = settings.require_durable_real_data()
        self._settings = settings
        self._repository_root = repository_root.resolve(strict=True)
        self._data_root = PrivateDataRoot(root_path, self._repository_root)
        self._data_root.validate()

    def run(self, request: IngestionCommandRequest) -> IngestionCommandResult:
        try:
            return self._run(request)
        except LivingIngestionIncomplete as error:
            return IngestionCommandResult(
                outcome=IngestionCommandOutcome.INCOMPLETE,
                code=str(error) if str(error) else "INCOMPLETE",
            )
        except Exception:
            # CLI output is intentionally non-revelatory. Detailed sanitized
            # operational errors are persisted by their owning repositories.
            return IngestionCommandResult(
                outcome=IngestionCommandOutcome.FAILED,
                code="INGESTION_FAILED",
            )

    def resume(self, run_id: UUID) -> IngestionCommandResult:
        """Resume one exact durable run without reconstructing command selectors."""

        try:
            with OperationalStateStore.open(self._data_root) as store:
                enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default())
                replay_operation = OperationalReplayRepository(store).load_raw_replay_operation(
                    run_id
                )
                if replay_operation is not None:
                    if replay_operation.status is RawReplayOperationStatus.SUCCESS:
                        return self._raw_replay_operation_result(replay_operation)
                    if replay_operation.status is RawReplayOperationStatus.FAILED:
                        return IngestionCommandResult(
                            outcome=IngestionCommandOutcome.FAILED,
                            code="RAW_REPLAY_FAILED",
                            run_id=str(run_id),
                        )
                    replay_service = LivingIngestionService(
                        data_root=self._data_root,
                        store=store,
                        provider=None,
                        policy_enforcer=enforcer,
                    )
                    return self._raw_replay_result(replay_service.resume_raw_replay(run_id))
                context = RestartProjectionReader(store).load_run(run_id)
                provider_required = any(
                    request.action
                    in {
                        RestartAction.DISPATCH,
                        RestartAction.RETRY_DISPATCH,
                        RestartAction.RESUME_ACQUISITION,
                    }
                    for request in context.requests
                )
                provider: BarPageProvider | None = None
                if provider_required:
                    provider = AlpacaProvider.from_environment(
                        feed=AlpacaFeed.SIP,
                        transport=SpoolingUrllibHttpTransport(self._data_root),
                    )
                service = LivingIngestionService(
                    data_root=self._data_root,
                    store=store,
                    provider=provider,
                    policy_enforcer=enforcer,
                )
                return self._command_result(service.resume(run_id))
        except LivingIngestionIncomplete as error:
            return IngestionCommandResult(
                outcome=IngestionCommandOutcome.INCOMPLETE,
                code=str(error) if str(error) else "INCOMPLETE",
                run_id=str(run_id),
            )
        except Exception:
            return IngestionCommandResult(
                outcome=IngestionCommandOutcome.FAILED,
                code="INGESTION_FAILED",
                run_id=str(run_id),
            )

    def _run(self, request: IngestionCommandRequest) -> IngestionCommandResult:
        if (
            request.provider != "alpaca"
            or request.dataset != "price_bars_sip"
            or request.timeframe not in {Timeframe.ONE_DAY, Timeframe.FIVE_MINUTES}
            or request.session is not TradingSession.REGULAR
        ):
            raise LivingIngestionServiceError("command lies outside Phase 2 provider scope")
        selected = _resolve_phase1_instruments(request.instruments)
        streams = tuple(
            sorted(
                (
                    StreamKey(
                        provider="alpaca",
                        dataset="price_bars_sip",
                        data_kind=DataKind.PRICE_BAR,
                        instrument_id=security.instrument_id,
                        timeframe=request.timeframe,
                        session=request.session,
                        adjustment=request.adjustment,
                        currency="USD",
                        bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
                    )
                    for security in selected
                ),
                key=lambda value: value.stream_id,
            )
        )
        mappings = tuple(
            ProviderInstrumentMapping(
                instrument_id=security.instrument_id,
                provider_identifier=security.alpaca_identifier,
            )
            for security in selected
        )
        now = datetime.now(UTC)
        with OperationalStateStore.open(self._data_root) as store:
            enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default())
            integrity = LivingIngestionService(
                data_root=self._data_root,
                store=store,
                provider=None,
                policy_enforcer=enforcer,
            )
            try:
                integrity_loss = integrity.reconcile_integrity(streams)
                raw_replay_requested = (
                    request.intent is IngestionIntent.REPAIR
                    and request.repair_strategy is RepairStrategy.RAW_REPLAY
                )
                if integrity_loss and not raw_replay_requested:
                    raise LivingIngestionIncomplete("INTEGRITY_REPAIR_REQUIRED")
                bounds = self._command_bounds(request, store, streams, now=now)
            except RestartProjectionIntegrityError as error:
                raise LivingIngestionIncomplete("INTEGRITY_REPAIR_REQUIRED") from error
            if bounds is None:
                return IngestionCommandResult(
                    outcome=IngestionCommandOutcome.NO_OP,
                    code="NO_NEW_INTERVAL",
                )
            start, end = bounds
            if request.repair_strategy is RepairStrategy.RAW_REPLAY:
                replay_service = LivingIngestionService(
                    data_root=self._data_root,
                    store=store,
                    provider=None,
                    policy_enforcer=enforcer,
                )
                return self._raw_replay_result(
                    replay_service.run_raw_replay(
                        self._command_specification(
                            streams,
                            mappings,
                            start=start,
                            end=end,
                        )
                    )
                )
            proofs = RestartProjectionReader(store).load_optional_stream_proofs(
                tuple(stream.stream_id for stream in streams)
            )
            retained = tuple(
                segment
                for segment in (() if proofs is None else proofs.coverage)
                if segment.retained
                and segment.verification_state is CoverageVerificationState.VERIFIED
                and segment.invalidated_at is None
            )
            verified_watermarks = tuple(
                watermark
                for watermark in (() if proofs is None else proofs.watermarks)
                if watermark.verification_state is CoverageVerificationState.VERIFIED
                and watermark.invalidated_at is None
            )
            calendar_start = min(
                (
                    start,
                    *(segment.coverage_start for segment in retained),
                    *(watermark.coverage_start for watermark in verified_watermarks),
                )
            )
            calendar_end = max(
                (
                    end,
                    *(segment.end for segment in retained),
                    *(watermark.exclusive_frontier for watermark in verified_watermarks),
                )
            )
            snapshot = _calendar_for_bounds(calendar_start, calendar_end, generated_at=now)
            service = LivingIngestionService(
                data_root=self._data_root,
                store=store,
                provider=AlpacaProvider.from_environment(
                    feed=AlpacaFeed.SIP,
                    transport=SpoolingUrllibHttpTransport(self._data_root),
                ),
                policy_enforcer=enforcer,
            )
            result = service.run(
                LivingIngestionRunRequest(
                    intent=request.intent,
                    streams=streams,
                    instrument_mappings=mappings,
                    desired_start=start,
                    desired_end=end,
                    calendar_snapshot=snapshot,
                    limits=_command_limits(request),
                    budget=_command_budget(request),
                    environment=self._settings.environment,
                    mapping_semantic_version="alpaca-sip-bars-v1",
                    reason=f"manual {request.intent.value.lower()} command",
                    repair_strategy=request.repair_strategy,
                    repair_reason=request.repair_reason,
                )
            )
        return self._command_result(result)

    @staticmethod
    def _command_specification(
        streams: tuple[StreamKey, ...],
        mappings: tuple[ProviderInstrumentMapping, ...],
        *,
        start: datetime,
        end: datetime,
    ) -> RequestSpecification:
        exemplar = streams[0]
        return RequestSpecification(
            provider=exemplar.provider,
            dataset=exemplar.dataset,
            data_kind=exemplar.data_kind,
            instrument_mappings=mappings,
            timeframe=exemplar.timeframe,
            session=exemplar.session,
            adjustment=exemplar.adjustment,
            currency=exemplar.currency,
            bar_semantics=exemplar.bar_semantics,
            additional_dimensions=exemplar.additional_dimensions,
            start=start,
            end=end,
            mapping_semantic_version="alpaca-sip-bars-v1",
        )

    @staticmethod
    def _raw_replay_result(result: RawReplayOperationResult) -> IngestionCommandResult:
        return IngestionCommandResult(
            outcome=IngestionCommandOutcome.SUCCESS,
            code="RAW_REPLAY_SUCCESS",
            run_id=str(result.operation_id),
            planned_request_count=1,
            completed_request_count=1,
            canonical_batch_count=1,
        )

    @staticmethod
    def _raw_replay_operation_result(
        operation: RawReplayOperation,
    ) -> IngestionCommandResult:
        if operation.status is not RawReplayOperationStatus.SUCCESS:
            raise LivingIngestionServiceError("raw replay operation is not successful")
        return IngestionCommandResult(
            outcome=IngestionCommandOutcome.SUCCESS,
            code="RAW_REPLAY_SUCCESS",
            run_id=str(operation.operation_id),
            planned_request_count=1,
            completed_request_count=1,
            canonical_batch_count=1,
        )

    @staticmethod
    def _command_result(result: LivingIngestionRunResult) -> IngestionCommandResult:
        if result.no_op:
            outcome = IngestionCommandOutcome.NO_OP
        elif result.status is IngestionRunStatus.SUCCESS:
            outcome = IngestionCommandOutcome.SUCCESS
        elif result.status in {IngestionRunStatus.FAILED, IngestionRunStatus.CANCELLED}:
            outcome = IngestionCommandOutcome.FAILED
        else:
            outcome = IngestionCommandOutcome.INCOMPLETE
        return IngestionCommandResult(
            outcome=outcome,
            code="NO_OP" if result.no_op else result.status.value,
            run_id=str(result.run_id),
            planned_request_count=result.planned_request_count,
            completed_request_count=result.completed_request_count,
            raw_artifact_count=result.raw_artifact_count,
            canonical_batch_count=result.canonical_batch_count,
            open_gap_count=result.open_gap_count,
        )

    @staticmethod
    def _command_bounds(
        request: IngestionCommandRequest,
        store: OperationalStateStore,
        streams: tuple[StreamKey, ...],
        *,
        now: datetime,
    ) -> tuple[datetime, datetime] | None:
        if request.intent is not IngestionIntent.UPDATE:
            if request.start is None or request.end is None:
                raise LivingIngestionServiceError("bounded command is missing bounds")
            return request.start, request.end
        proofs = RestartProjectionReader(store).load_optional_stream_proofs(
            tuple(stream.stream_id for stream in streams)
        )
        if proofs is None:
            raise LivingIngestionIncomplete("NO_COVERAGE_ORIGIN")
        eligible = tuple(
            watermark
            for watermark in proofs.watermarks
            if watermark.verification_state is CoverageVerificationState.VERIFIED
            and watermark.invalidated_at is None
            and watermark.blocking_gap_count == 0
        )
        origins = {watermark.coverage_start for watermark in eligible}
        frontiers = {watermark.exclusive_frontier for watermark in eligible}
        if len(origins) != 1 or len(frontiers) != 1 or len(eligible) != len(streams):
            raise LivingIngestionIncomplete("NO_COVERAGE_ORIGIN")
        end = request.end or now
        start = next(iter(frontiers))
        if end <= start:
            return None
        return start, end


def _resolve_phase1_instruments(symbols: Sequence[str]) -> tuple[Phase1Security, ...]:
    known = {security.symbol: security for security in PHASE1_SECURITIES}
    try:
        return tuple(known[symbol] for symbol in symbols)
    except KeyError as error:
        raise LivingIngestionServiceError(
            "instrument is outside the approved Phase 1 sample"
        ) from error


def _calendar_for_bounds(
    start: datetime,
    end: datetime,
    *,
    generated_at: datetime,
) -> CalendarSnapshot:
    start_date = start.astimezone(_XNYS).date()
    last_date = (end - timedelta(microseconds=1)).astimezone(_XNYS).date()
    range_start = date(start_date.year, 1, 1)
    range_end = date(last_date.year + 1, 1, 1)
    return XNYSCalendar().snapshot(range_start, range_end, generated_at=generated_at)


def _command_limits(request: IngestionCommandRequest) -> PlannerLimits:
    observations_per_page = 10_000
    return PlannerLimits(
        max_instruments_per_request=min(16, len(request.instruments)),
        max_expected_observations_per_request=min(
            request.max_expected_observations,
            observations_per_page * request.max_pages,
        ),
        max_observations_per_page=observations_per_page,
        max_pages_per_request=request.max_pages,
        max_calls_per_request=request.max_calls,
        max_estimated_bytes_per_request=request.max_estimated_bytes,
        estimated_bytes_per_observation=512,
        estimated_bytes_per_page=2048,
        estimated_cost_per_call=Decimal(0),
        max_estimated_cost_per_request=request.max_estimated_cost,
    )


def _command_budget(request: IngestionCommandRequest) -> PlannerBudget:
    return PlannerBudget(
        max_calls=request.max_calls,
        max_expected_observations=request.max_expected_observations,
        max_pages=request.max_pages,
        max_estimated_bytes=request.max_estimated_bytes,
        max_estimated_cost=request.max_estimated_cost,
    )


def create_cli_command_runner(
    settings: RuntimeSettings,
    repository_root: Path,
) -> IngestionCommandRunner:
    """Create the non-argparse production bridge used by the Phase 2 CLI."""

    if settings.environment is not RuntimeEnvironment.PRIVATE_RESEARCH:
        raise LivingIngestionServiceError(
            "living ingestion commands require the private_research profile"
        )
    return _ProductionCommandRunner(settings, repository_root)


__all__ = [
    "LivingIngestionFaultPoint",
    "LivingIngestionFaults",
    "LivingIngestionIncomplete",
    "LivingIngestionRunRequest",
    "LivingIngestionRunResult",
    "LivingIngestionService",
    "LivingIngestionServiceError",
    "create_cli_command_runner",
]
