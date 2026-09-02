"""Offline end-to-end acceptance for restartable living ingestion."""

from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from investment_platform.data.calendar import CalendarSession, CalendarSnapshot
from investment_platform.data.diagnostics import DiagnosticStatus, Phase2OperationalDiagnostics
from investment_platform.data.ingestion.commands import IngestionCommandRequest
from investment_platform.data.ingestion.identity import (
    BarSemantics,
    DataKind,
    ProviderInstrumentMapping,
    StreamKey,
)
from investment_platform.data.ingestion.planner import (
    IngestionIntent,
    PlannerBudget,
    PlannerLimits,
    RepairStrategy,
)
from investment_platform.data.ingestion.service import (
    LivingIngestionFaultPoint,
    LivingIngestionFaults,
    LivingIngestionIncomplete,
    LivingIngestionRunRequest,
    LivingIngestionService,
    create_cli_command_runner,
)
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.operational import IngestionRunStatus, OperationalStateStore
from investment_platform.data.operational.budget import (
    BudgetReservationState,
    ProviderBudgetRepository,
    ProviderBudgetWindow,
)
from investment_platform.data.operational.query import CatalogBarQueryRepository
from investment_platform.data.operational.replay import OperationalReplayRepository
from investment_platform.data.operational.restart import (
    RestartProjectionIntegrityError,
    RestartProjectionReader,
)
from investment_platform.data.providers import AlpacaCredentials, AlpacaFeed, AlpacaProvider
from investment_platform.data.providers.http import HttpResponse
from investment_platform.data.retention import (
    DatasetPolicyDenied,
    DatasetRuntimeStatus,
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage import CanonicalBatchManifest, PublicationFaultPoint
from investment_platform.data_root import PrivateDataRoot
from investment_platform.runtime import RuntimeEnvironment, RuntimeSettings
from tests.provider_fakes import QueueHttpTransport

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
_SESSION_DATE = date(2025, 7, 2)
_OPEN = datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
_CLOSE = _OPEN + timedelta(minutes=15)
_INSTRUMENT_ID = UUID("1923431d-8907-4f63-ba11-68182c11f778")
_SECOND_INSTRUMENT_ID = UUID("1923431d-8907-4f63-ba11-68182c11f799")
_RUN_1 = UUID("10000000-0000-4000-8000-000000000001")
_RUN_2 = UUID("10000000-0000-4000-8000-000000000002")
_RUN_3 = UUID("10000000-0000-4000-8000-000000000003")
_RUN_4 = UUID("10000000-0000-4000-8000-000000000004")


class InjectedCrash(RuntimeError):
    pass


@dataclass
class MutableClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value


def _private_root(tmp_path: Path) -> PrivateDataRoot:
    repository = Path(__file__).parents[2].resolve(strict=True)
    root = PrivateDataRoot(
        tmp_path.parent / f"p2-service-{uuid4().hex[:8]}",
        repository,
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
        range_start=_SESSION_DATE,
        range_end=_SESSION_DATE + timedelta(days=1),
        generated_at=_NOW,
        sessions=(
            CalendarSession(
                session_date=_SESSION_DATE,
                open_utc=_OPEN,
                close_utc=_CLOSE,
            ),
        ),
    )


def _stream(timeframe: Timeframe = Timeframe.FIVE_MINUTES) -> StreamKey:
    return StreamKey(
        provider="alpaca",
        dataset="price_bars_sip",
        data_kind=DataKind.PRICE_BAR,
        instrument_id=_INSTRUMENT_ID,
        timeframe=timeframe,
        session=TradingSession.REGULAR,
        adjustment=AdjustmentState.UNADJUSTED,
        currency="USD",
        bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
    )


def _payload(*timestamps: datetime, next_token: str | None = None) -> bytes:
    bars = [
        {
            "t": timestamp.isoformat().replace("+00:00", "Z"),
            "o": 100.0 + ordinal,
            "h": 101.0 + ordinal,
            "l": 99.0 + ordinal,
            "c": 100.5 + ordinal,
            "v": 1_000 + ordinal,
            "vw": 100.25 + ordinal,
        }
        for ordinal, timestamp in enumerate(timestamps)
    ]
    return json.dumps(
        {"bars": {"AAPL": bars}, "next_page_token": next_token},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _payload_with_revised_close(*timestamps: datetime) -> bytes:
    payload = json.loads(_payload(*timestamps))
    payload["bars"]["AAPL"][0]["c"] = 777.25
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _provider(
    clock: MutableClock,
    responses: list[bytes],
) -> tuple[AlpacaProvider, QueueHttpTransport]:
    transport = QueueHttpTransport(
        [
            HttpResponse(
                200,
                response,
                headers={
                    "x-ratelimit-limit": "200",
                    "x-ratelimit-remaining": str(199 - index),
                },
            )
            for index, response in enumerate(responses)
        ]
    )
    return (
        AlpacaProvider(
            AlpacaCredentials("synthetic-id", "synthetic-secret"),
            feed=AlpacaFeed.SIP,
            transport=transport,
            clock=clock,
            batch_id_factory=uuid4,
        ),
        transport,
    )


def _service(
    root: PrivateDataRoot,
    store: OperationalStateStore,
    clock: MutableClock,
    responses: list[bytes],
) -> tuple[LivingIngestionService, QueueHttpTransport]:
    provider, transport = _provider(clock, responses)
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    return (
        LivingIngestionService(
            data_root=root,
            store=store,
            provider=provider,
            policy_enforcer=enforcer,
            clock=clock,
            lease_owner_id=f"test-service-{uuid4()}",
        ),
        transport,
    )


def _request(
    *,
    run_id: UUID,
    intent: IngestionIntent,
    start: datetime = _OPEN,
    end: datetime = _OPEN + timedelta(minutes=10),
    repair_strategy: RepairStrategy | None = None,
    repair_reason: str | None = None,
    timeframe: Timeframe = Timeframe.FIVE_MINUTES,
    max_pages: int = 1,
    max_calls: int = 1,
) -> LivingIngestionRunRequest:
    return LivingIngestionRunRequest(
        run_id=run_id,
        intent=intent,
        streams=(_stream(timeframe),),
        instrument_mappings=(
            ProviderInstrumentMapping(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="AAPL",
            ),
        ),
        desired_start=start,
        desired_end=end,
        calendar_snapshot=_calendar(),
        limits=PlannerLimits(
            max_instruments_per_request=1,
            max_expected_observations_per_request=10,
            max_observations_per_page=10,
            max_pages_per_request=max_pages,
            max_calls_per_request=max_calls,
            max_estimated_bytes_per_request=100_000,
            estimated_bytes_per_observation=512,
            estimated_bytes_per_page=2_048,
            estimated_cost_per_call=Decimal(0),
            max_estimated_cost_per_request=Decimal(0),
        ),
        budget=PlannerBudget(
            max_calls=max_calls,
            max_expected_observations=10,
            max_pages=max_pages,
            max_estimated_bytes=100_000,
            max_estimated_cost=Decimal(0),
        ),
        environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        mapping_semantic_version="alpaca-sip-bars-v1",
        reason=f"synthetic {intent.value.lower()} acceptance",
        repair_strategy=repair_strategy,
        repair_reason=repair_reason,
    )


def test_backfill_incremental_extension_and_second_update_no_op(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        backfill, backfill_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        first = backfill.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))

        assert first.status is IngestionRunStatus.SUCCESS
        assert first.raw_artifact_count == 1
        assert first.canonical_batch_count == 1
        assert first.open_gap_count == 0
        assert len(backfill_transport.requests) == 1

        update, update_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN + timedelta(minutes=10))],
        )
        extended = update.run(
            _request(
                run_id=_RUN_2,
                intent=IngestionIntent.UPDATE,
                end=_OPEN + timedelta(minutes=15),
            )
        )

        assert extended.status is IngestionRunStatus.SUCCESS
        assert len(update_transport.requests) == 1
        query = dict(update_transport.requests[0].query)
        assert query["start"] == "2025-07-02T13:40:00.000000Z"

        no_op, no_op_transport = _service(root, store, clock, [])
        repeated = no_op.run(
            _request(
                run_id=_RUN_3,
                intent=IngestionIntent.UPDATE,
                end=_OPEN + timedelta(minutes=15),
            )
        )

        assert repeated.no_op
        assert repeated.status is IngestionRunStatus.SUCCESS
        assert no_op_transport.requests == []


def test_empty_response_records_expected_observation_without_verified_empty(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        service, transport = _service(root, store, clock, [_payload()])

        result = service.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))

        assert result.status is IngestionRunStatus.FAILED
        assert len(transport.requests) == 1
        with store.read_only_connection() as connection:
            gap = connection.execute("SELECT gap_type, status, blocking FROM gaps").fetchone()
            verified_empty_count = connection.execute(
                """
                SELECT count(*) FROM coverage_segments
                WHERE classification = 'VERIFIED_EMPTY'
                """
            ).fetchone()[0]
            watermark_count = connection.execute("SELECT count(*) FROM watermarks").fetchone()[0]
        assert gap is not None and tuple(gap) == ("EXPECTED_OBSERVATION", "OPEN", 1)
        assert verified_empty_count == 0
        assert watermark_count == 0


def test_terminal_gap_at_frontier_reconciles_watermark_without_moving_it(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        initial.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))

        update, _ = _service(root, store, clock, [_payload()])
        result = update.run(
            _request(
                run_id=_RUN_2,
                intent=IngestionIntent.UPDATE,
                end=_OPEN + timedelta(minutes=15),
            )
        )

        assert result.status is IngestionRunStatus.FAILED
        with store.read_only_connection() as connection:
            gap = connection.execute(
                "SELECT status, interval_start FROM gaps WHERE status = 'OPEN'"
            ).fetchone()
            watermark = connection.execute(
                "SELECT exclusive_frontier, blocking_gap_count, generation, last_run_id "
                "FROM watermarks"
            ).fetchone()
        assert gap is not None
        assert gap["interval_start"] == (_OPEN + timedelta(minutes=10)).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        assert watermark is not None
        assert tuple(watermark) == (
            (_OPEN + timedelta(minutes=10))
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            1,
            2,
            str(_RUN_2),
        )
        assert RestartProjectionReader(store).load_run(_RUN_2).status is IngestionRunStatus.FAILED
        proofs = RestartProjectionReader(store).load_stream_proofs((_stream().stream_id,))
        assert proofs.watermarks[0].exclusive_frontier == _OPEN + timedelta(minutes=10)
        assert proofs.watermarks[0].blocking_gap_count == 1


def test_provider_short_page_continues_within_persisted_hard_dispatch_ceiling(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        service, transport = _service(
            root,
            store,
            clock,
            [
                _payload(_OPEN, next_token="SYNTHETIC_PAGE_2"),
                _payload(_OPEN + timedelta(minutes=5)),
            ],
        )

        completed = service.run(
            _request(
                run_id=_RUN_1,
                intent=IngestionIntent.BACKFILL,
                max_pages=2,
                max_calls=2,
            )
        )

        assert completed.status is IngestionRunStatus.SUCCESS
        assert len(transport.requests) == 2
        with store.read_only_connection() as connection:
            limits = connection.execute(
                """
                SELECT max_pages, max_calls,
                       max_pages_per_request, max_calls_per_request
                FROM ingestion_execution_limits WHERE run_id = ?
                """,
                (str(_RUN_1),),
            ).fetchone()
            request_limits = connection.execute(
                "SELECT max_pages, max_calls FROM request_execution_limits"
            ).fetchone()
            estimate = connection.execute(
                "SELECT estimated_pages, estimated_calls FROM request_plan_estimates"
            ).fetchone()
            reservations = connection.execute(
                """
                SELECT amount, state FROM provider_budget_reservations
                ORDER BY dispatch_ordinal
                """
            ).fetchall()
            dispatch_claims = connection.execute(
                "SELECT page_ordinal FROM ingestion_dispatch_claims ORDER BY dispatch_ordinal"
            ).fetchall()
        assert limits is not None and tuple(limits) == (2, 2, 2, 2)
        assert request_limits is not None and tuple(request_limits) == (2, 2)
        assert estimate is not None and tuple(estimate) == (1, 1)
        assert [tuple(row) for row in reservations] == [(1, "CONSUMED"), (1, "CONSUMED")]
        assert [int(row[0]) for row in dispatch_claims] == [0, 1]

        reopened, reopened_transport = _service(root, store, clock, [])
        assert reopened.resume(_RUN_1) == completed
        assert reopened_transport.requests == []


def test_daily_backfill_query_watermark_and_reopen_are_durable(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    request = _request(
        run_id=_RUN_1,
        intent=IngestionIntent.BACKFILL,
        start=_OPEN,
        end=_CLOSE,
        timeframe=Timeframe.ONE_DAY,
    )
    with OperationalStateStore.open(root, clock=clock) as first_store:
        service, transport = _service(root, first_store, clock, [_payload(_OPEN)])
        completed = service.run(request)

        assert completed.status is IngestionRunStatus.SUCCESS
        assert completed.canonical_batch_count == 1
        assert len(transport.requests) == 1
        query = CatalogBarQueryRepository(
            first_store,
            root,
            RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock),
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        ).query("alpaca", "price_bars_sip")
        assert len(query.current) == 1
        assert query.frame().get_column("timeframe").to_list() == ["1d"]
        with first_store.read_only_connection() as connection:
            watermark = connection.execute(
                "SELECT coverage_start, exclusive_frontier FROM watermarks"
            ).fetchone()
        assert watermark is not None
        assert watermark[0] == _OPEN.isoformat(timespec="microseconds").replace("+00:00", "Z")
        assert watermark[1] == _CLOSE.isoformat(timespec="microseconds").replace("+00:00", "Z")

    with OperationalStateStore.open(root, clock=clock) as reopened_store:
        restarted = LivingIngestionService(
            data_root=root,
            store=reopened_store,
            provider=None,
            policy_enforcer=RetentionPolicyEnforcer(
                RetentionPolicyCatalog.load_default(),
                clock=clock,
            ),
            clock=clock,
            lease_owner_id=f"daily-reopen-{uuid4()}",
        )
        assert restarted.resume(_RUN_1) == completed


def test_disjoint_daily_publication_materializes_inter_batch_gap(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    session_dates = tuple(date(2026, 8, day) for day in range(24, 28))
    opens = tuple(
        datetime.combine(session_date, datetime.min.time(), tzinfo=UTC)
        + timedelta(hours=13, minutes=30)
        for session_date in session_dates
    )
    closes = tuple(value + timedelta(hours=6, minutes=30) for value in opens)
    provider_timestamps = tuple(
        datetime.combine(session_date, datetime.min.time(), tzinfo=UTC) + timedelta(hours=4)
        for session_date in session_dates
    )
    calendar = CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="1",
        tzdata_version="synthetic",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=session_dates[0],
        range_end=session_dates[-1] + timedelta(days=1),
        generated_at=_NOW,
        sessions=tuple(
            CalendarSession(
                session_date=session_date,
                open_utc=session_open,
                close_utc=session_close,
            )
            for session_date, session_open, session_close in zip(
                session_dates,
                opens,
                closes,
                strict=True,
            )
        ),
    )

    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(root, store, clock, [_payload(*provider_timestamps[:2])])
        first = initial.run(
            replace(
                _request(
                    run_id=_RUN_1,
                    intent=IngestionIntent.BACKFILL,
                    start=opens[0],
                    end=closes[1],
                    timeframe=Timeframe.ONE_DAY,
                ),
                calendar_snapshot=calendar,
            )
        )
        assert first.status is IngestionRunStatus.SUCCESS
        with store.read_only_connection() as connection:
            first_watermark = connection.execute("SELECT generation FROM watermarks").fetchone()
        assert first_watermark is not None

        extension, _ = _service(root, store, clock, [_payload(provider_timestamps[3])])
        partial = extension.run(
            replace(
                _request(
                    run_id=_RUN_2,
                    intent=IngestionIntent.BACKFILL,
                    start=opens[3],
                    end=closes[3],
                    timeframe=Timeframe.ONE_DAY,
                ),
                calendar_snapshot=calendar,
            )
        )

        assert partial.status is IngestionRunStatus.PARTIAL
        assert partial.open_gap_count == 1
        with store.read_only_connection() as connection:
            coverage_end = connection.execute(
                "SELECT max(interval_end) FROM coverage_segments"
            ).fetchone()[0]
            gaps = connection.execute(
                """
                SELECT interval_start, interval_end, gap_type, status, blocking
                FROM gaps WHERE status IN ('OPEN', 'REPAIRING')
                """
            ).fetchall()
            watermark = connection.execute(
                """
                SELECT exclusive_frontier, generation, last_verified_session,
                       blocking_gap_count
                FROM watermarks
                """
            ).fetchone()
        assert coverage_end == closes[3].isoformat(timespec="microseconds").replace("+00:00", "Z")
        assert len(gaps) == 1
        assert tuple(gaps[0]) == (
            opens[2].isoformat(timespec="microseconds").replace("+00:00", "Z"),
            closes[2].isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "EXPECTED_OBSERVATION",
            "OPEN",
            1,
        )
        assert watermark is not None
        assert watermark[0] == opens[2].isoformat(timespec="microseconds").replace("+00:00", "Z")
        assert int(watermark[1]) == int(first_watermark[0]) + 1
        assert watermark[2] == session_dates[1].isoformat()
        assert int(watermark[3]) == 1
        assert Phase2OperationalDiagnostics(root, store, clock=clock).verify().healthy

    with OperationalStateStore.open(root, clock=clock) as reopened_store:
        reopened = LivingIngestionService(
            data_root=root,
            store=reopened_store,
            provider=None,
            policy_enforcer=RetentionPolicyEnforcer(
                RetentionPolicyCatalog.load_default(),
                clock=clock,
            ),
            clock=clock,
            lease_owner_id=f"daily-gap-reopen-{uuid4()}",
        )
        assert reopened.resume(_RUN_2) == partial


def test_earlier_backfill_rebases_origin_without_moving_frontier_backward(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        later, _ = _service(root, store, clock, [_payload(_OPEN + timedelta(minutes=10))])
        later.run(
            _request(
                run_id=_RUN_1,
                intent=IngestionIntent.BACKFILL,
                start=_OPEN + timedelta(minutes=10),
                end=_OPEN + timedelta(minutes=15),
            )
        )

        earlier, transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        result = earlier.run(
            _request(
                run_id=_RUN_2,
                intent=IngestionIntent.BACKFILL,
                start=_OPEN,
                end=_OPEN + timedelta(minutes=10),
            )
        )

        assert result.status is IngestionRunStatus.SUCCESS
        assert len(transport.requests) == 1
        with store.read_only_connection() as connection:
            origins = {
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT coverage_start FROM coverage_segments"
                ).fetchall()
            }
            watermark = connection.execute(
                "SELECT coverage_start, exclusive_frontier FROM watermarks"
            ).fetchone()
            rebindings = connection.execute(
                """
                SELECT source_coverage_start, target_coverage_start,
                       run_id, request_instance_id, canonical_batch_id
                FROM coverage_origin_rebindings
                """
            ).fetchall()
        assert origins == {_OPEN.isoformat(timespec="microseconds").replace("+00:00", "Z")}
        assert watermark is not None
        assert watermark[0] == _OPEN.isoformat(timespec="microseconds").replace("+00:00", "Z")
        assert watermark[1] == (_OPEN + timedelta(minutes=15)).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        assert len(rebindings) == 1
        assert rebindings[0][0] == (_OPEN + timedelta(minutes=10)).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        assert rebindings[0][1] == _OPEN.isoformat(timespec="microseconds").replace("+00:00", "Z")
        assert rebindings[0][2] == str(_RUN_2)

        reopened = earlier.resume(_RUN_2)
        assert reopened.status is IngestionRunStatus.SUCCESS
        with store.read_only_connection() as connection:
            assert (
                connection.execute("SELECT count(*) FROM coverage_origin_rebindings").fetchone()[0]
                == 1
            )


def test_gap_repair_restores_contiguous_coverage(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=10))],
        )
        with_gap = initial.run(
            _request(
                run_id=_RUN_1,
                intent=IngestionIntent.BACKFILL,
                end=_OPEN + timedelta(minutes=15),
            )
        )
        assert with_gap.open_gap_count == 1

        repair, repair_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN + timedelta(minutes=5))],
        )
        repaired = repair.run(
            _request(
                run_id=_RUN_2,
                intent=IngestionIntent.REPAIR,
                start=_OPEN + timedelta(minutes=5),
                end=_OPEN + timedelta(minutes=10),
                repair_strategy=RepairStrategy.PROVIDER_REFRESH,
                repair_reason="synthetic missing-slot reconciliation",
            )
        )

        assert repaired.status is IngestionRunStatus.SUCCESS
        assert repaired.open_gap_count == 0
        assert len(repair_transport.requests) == 1
        query = dict(repair_transport.requests[0].query)
        assert query["start"] == "2025-07-02T13:35:00.000000Z"
        assert Phase2OperationalDiagnostics(root, store, clock=clock).verify().healthy


def test_provider_refresh_identical_values_commits_semantic_noop_and_restarts(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        initial.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))
        original_directories = tuple((root.root / "normalized").rglob("manifest.json"))
        assert len(original_directories) == 1

        refresh, refresh_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        no_op = refresh.run(
            _request(
                run_id=_RUN_2,
                intent=IngestionIntent.REPAIR,
                repair_strategy=RepairStrategy.PROVIDER_REFRESH,
                repair_reason="check unchanged provider values",
            )
        )

        assert no_op.status is IngestionRunStatus.SUCCESS
        assert no_op.raw_artifact_count == 1
        assert no_op.canonical_batch_count == 0
        assert len(refresh_transport.requests) == 1
        assert tuple((root.root / "normalized").rglob("manifest.json")) == original_directories
        with store.read_only_connection() as connection:
            proof = connection.execute(
                "SELECT semantic_duplicate_count FROM semantic_noop_commits"
            ).fetchone()
        assert proof is not None
        assert int(proof["semantic_duplicate_count"]) == 2

        orphan_check = next(
            check
            for check in Phase2OperationalDiagnostics(root, store, clock=clock).verify().checks
            if check.code == "PUBLISHED_ORPHANS"
        )
        assert orphan_check.status is DiagnosticStatus.PASS

        restarted, restarted_transport = _service(root, store, clock, [])
        recovered = restarted.resume(_RUN_2)

        assert recovered == no_op
        assert restarted_transport.requests == []

        with sqlite3.connect(store.path) as connection:
            connection.execute(
                "UPDATE semantic_noop_commits SET duplicate_observations_json = '[]'"
            )
        with pytest.raises(RestartProjectionIntegrityError):
            RestartProjectionReader(store).load_run(_RUN_2)


def test_semantic_duplicate_repair_resolves_proven_gap_and_rebuilds_watermark(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        initial.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))

        blocked, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN)],
        )
        failed = blocked.run(
            _request(
                run_id=_RUN_2,
                intent=IngestionIntent.REPAIR,
                repair_strategy=RepairStrategy.PROVIDER_REFRESH,
                repair_reason="create a bounded integrity finding",
            )
        )
        assert failed.status is IngestionRunStatus.FAILED

        partial, _ = _service(root, store, clock, [_payload(_OPEN)])
        incomplete_proof = partial.run(
            _request(
                run_id=_RUN_3,
                intent=IngestionIntent.REPAIR,
                end=_OPEN + timedelta(minutes=5),
                repair_strategy=RepairStrategy.PROVIDER_REFRESH,
                repair_reason="prove only a strict subset of the finding",
            )
        )
        assert incomplete_proof.status is IngestionRunStatus.SUCCESS
        assert incomplete_proof.canonical_batch_count == 0
        with store.read_only_connection() as connection:
            assert connection.execute("SELECT status FROM gaps").fetchone()[0] == "OPEN"

        repair, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        repaired = repair.run(
            _request(
                run_id=_RUN_4,
                intent=IngestionIntent.REPAIR,
                repair_strategy=RepairStrategy.PROVIDER_REFRESH,
                repair_reason="prove retained values are unchanged",
            )
        )

        assert repaired.status is IngestionRunStatus.SUCCESS
        assert repaired.canonical_batch_count == 0
        with store.read_only_connection() as connection:
            gap = connection.execute("SELECT status FROM gaps").fetchone()
            watermark = connection.execute(
                "SELECT verification_state, exclusive_frontier, generation FROM watermarks"
            ).fetchone()
        assert gap is not None and gap["status"] == "RESOLVED"
        assert watermark is not None
        assert tuple(watermark) == (
            "VERIFIED",
            (_OPEN + timedelta(minutes=10))
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            2,
        )
        proofs = RestartProjectionReader(store).load_stream_proofs((_stream().stream_id,))
        assert proofs.gaps[0].status.value == "RESOLVED"
        assert proofs.watermarks[0].verification_state.value == "VERIFIED"


def test_provider_refresh_mixed_revision_publishes_complete_candidate(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    timestamps = (_OPEN, _OPEN + timedelta(minutes=5))
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(root, store, clock, [_payload(*timestamps)])
        initial.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))

        refresh, transport = _service(
            root,
            store,
            clock,
            [_payload_with_revised_close(*timestamps)],
        )
        revised = refresh.run(
            _request(
                run_id=_RUN_2,
                intent=IngestionIntent.REPAIR,
                repair_strategy=RepairStrategy.PROVIDER_REFRESH,
                repair_reason="verify one provider correction",
            )
        )

        assert revised.status is IngestionRunStatus.SUCCESS
        assert revised.canonical_batch_count == 1
        assert len(transport.requests) == 1
        manifests = tuple(
            CanonicalBatchManifest.model_validate_json(path.read_bytes())
            for path in (root.root / "normalized").rglob("manifest.json")
        )
        assert len(manifests) == 2
        revision = next(manifest for manifest in manifests if manifest.streams[0].revision_count)
        assert revision.row_count == 2
        assert revision.streams[0].semantic_duplicate_count == 1
        assert revision.streams[0].revision_count == 1


def test_duplicate_candidate_with_blocked_stream_finishes_partial_without_publication(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        initial.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))
        original_manifests = tuple((root.root / "normalized").rglob("manifest.json"))

        second_stream = _stream().model_copy(update={"instrument_id": _SECOND_INSTRUMENT_ID})
        base_request = _request(
            run_id=_RUN_2,
            intent=IngestionIntent.REPAIR,
            repair_strategy=RepairStrategy.PROVIDER_REFRESH,
            repair_reason="reconcile a mixed blocked request",
        )
        mixed_request = replace(
            base_request,
            streams=tuple(sorted((_stream(), second_stream), key=lambda value: value.stream_id)),
            instrument_mappings=(
                ProviderInstrumentMapping(
                    instrument_id=_INSTRUMENT_ID,
                    provider_identifier="AAPL",
                ),
                ProviderInstrumentMapping(
                    instrument_id=_SECOND_INSTRUMENT_ID,
                    provider_identifier="MSFT",
                ),
            ),
            limits=base_request.limits.model_copy(update={"max_instruments_per_request": 2}),
        )
        refresh, transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )

        partial = refresh.run(mixed_request)

        assert partial.status is IngestionRunStatus.PARTIAL
        assert partial.canonical_batch_count == 0
        assert partial.open_gap_count == 1
        assert len(transport.requests) == 1
        assert tuple((root.root / "normalized").rglob("manifest.json")) == original_manifests
        assert len(tuple((root.root / "quarantine").rglob("manifest.json"))) == 1
        with store.read_only_connection() as connection:
            assert (
                connection.execute("SELECT count(*) FROM semantic_noop_commits").fetchone()[0] == 0
            )

        restarted, restarted_transport = _service(root, store, clock, [])
        assert restarted.resume(_RUN_2) == partial
        assert restarted_transport.requests == []


def test_canonical_loss_repair_adopts_retained_raw_without_provider_call(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, initial_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        completed = initial.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))
        assert completed.status is IngestionRunStatus.SUCCESS
        assert len(initial_transport.requests) == 1

        publication = RestartProjectionReader(store).load_run(_RUN_1).requests[0].publication
        assert publication is not None
        canonical_directory = next((root.root / "normalized").rglob("manifest.json")).parent
        assert canonical_directory.is_relative_to(root.root / "normalized")
        shutil.rmtree(canonical_directory)
        lease = store.acquire_writer_lease(f"test-reconcile-{uuid4()}", timedelta(minutes=5))
        try:
            reconciled = OperationalReplayRepository(store).reconcile_canonical_loss(
                lease,
                publication.canonical_batch_id,
                detected_at=clock.value,
            )
        finally:
            store.release_writer_lease(lease)
        assert reconciled.replay_eligibility is not None

        repair, repair_transport = _service(root, store, clock, [])
        repaired = repair.run(
            _request(
                run_id=_RUN_2,
                intent=IngestionIntent.REPAIR,
                repair_strategy=RepairStrategy.MISSING_ONLY,
                repair_reason="restore loss-invalidated canonical bytes",
            )
        )

        assert repaired.status is IngestionRunStatus.SUCCESS
        assert repaired.open_gap_count == 0
        assert repair_transport.requests == []
        assert tuple(canonical_directory.rglob("*.parquet"))


def test_dedicated_raw_replay_operation_recovers_after_publication_crash_without_network(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        initial.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))
        projection = RestartProjectionReader(store).load_run(_RUN_1)
        request = projection.requests[0]
        assert request.publication is not None
        canonical_directory = next((root.root / "normalized").rglob("manifest.json")).parent
        assert canonical_directory.is_relative_to(root.root / "normalized")
        shutil.rmtree(canonical_directory)
        lease = store.acquire_writer_lease(f"test-reconcile-{uuid4()}", timedelta(minutes=5))
        try:
            OperationalReplayRepository(store).reconcile_canonical_loss(
                lease,
                request.publication.canonical_batch_id,
                detected_at=clock.value,
            )
        finally:
            store.release_writer_lease(lease)

        crashing, crashing_transport = _service(root, store, clock, [])

        def crash(point: LivingIngestionFaultPoint) -> None:
            if point is LivingIngestionFaultPoint.FILESYSTEM_PUBLISHED:
                raise InjectedCrash

        with pytest.raises(InjectedCrash):
            crashing.run_raw_replay(
                request.specification,
                operation_id=_RUN_2,
                faults=LivingIngestionFaults(service=crash),
            )
        assert crashing_transport.requests == []
        operation = OperationalReplayRepository(store).load_raw_replay_operation(_RUN_2)
        assert operation is not None
        assert operation.status.value == "RUNNING"
        status = Phase2OperationalDiagnostics(root, store, clock=clock).status()
        assert status.non_terminal_run_count == 1
        assert tuple(value.run_id for value in status.non_terminal_runs) == (_RUN_2,)
        assert status.non_terminal_runs[0].next_action.value == "RESUME"

        resumed, resumed_transport = _service(root, store, clock, [])
        result = resumed.resume_raw_replay(_RUN_2)

        assert result.operation_id == _RUN_2
        assert result.canonical_batch_id == request.publication.canonical_batch_id
        assert result.resolved_gap_count == 1
        assert resumed_transport.requests == []


def test_cli_raw_replay_reconciles_and_repairs_canonical_loss_in_one_invocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        initial.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))
        canonical_directory = next((root.root / "normalized").rglob("manifest.json")).parent
        shutil.rmtree(canonical_directory)

    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)
    monkeypatch.setattr(
        "investment_platform.data.ingestion.service.PrivateDataRoot",
        lambda configured_root, repository_root: root,
    )
    runner = create_cli_command_runner(
        RuntimeSettings(
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            data_root=root.root,
            environment_was_explicit=True,
        ),
        Path(__file__).parents[2],
    )
    request = IngestionCommandRequest(
        intent=IngestionIntent.REPAIR,
        provider="alpaca",
        dataset="price_bars_sip",
        instruments=("AAPL",),
        timeframe=Timeframe.FIVE_MINUTES,
        session=TradingSession.REGULAR,
        adjustment=AdjustmentState.UNADJUSTED,
        start=_OPEN,
        end=_OPEN + timedelta(minutes=10),
        repair_strategy=RepairStrategy.RAW_REPLAY,
        repair_reason="restore canonical loss from retained raw",
        max_calls=1,
        max_pages=1,
        max_expected_observations=10,
        max_estimated_bytes=100_000,
        max_estimated_cost=Decimal(0),
    )

    repaired = runner.run(request)

    assert repaired.outcome.value == "SUCCESS"
    assert repaired.code == "RAW_REPLAY_SUCCESS"
    assert tuple(canonical_directory.rglob("*.parquet"))
    with (
        OperationalStateStore.open(root, clock=clock) as store,
        store.read_only_connection() as connection,
    ):
        assert (
            connection.execute(
                "SELECT count(*) FROM raw_replay_operations WHERE status = 'SUCCESS'"
            ).fetchone()[0]
            == 1
        )
        assert connection.execute("SELECT count(*) FROM request_attempts").fetchone()[0] == 1


def test_published_orphan_is_adopted_after_restart_without_provider_call(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        crashing, first_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )

        def crash(point: LivingIngestionFaultPoint) -> None:
            if point is LivingIngestionFaultPoint.FILESYSTEM_PUBLISHED:
                raise InjectedCrash

        with pytest.raises(InjectedCrash):
            crashing.run(
                _request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL),
                faults=LivingIngestionFaults(service=crash),
            )
        assert len(first_transport.requests) == 1

        resumed, resumed_transport = _service(root, store, clock, [])
        result = resumed.resume(_RUN_1)

        assert result.status is IngestionRunStatus.SUCCESS
        assert result.canonical_batch_count == 1
        assert resumed_transport.requests == []


def test_resume_checks_processing_policy_before_loading_retained_raw(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        crashing, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )

        def crash(point: LivingIngestionFaultPoint) -> None:
            if point is LivingIngestionFaultPoint.ACQUISITION_COMPLETED:
                raise InjectedCrash

        with pytest.raises(InjectedCrash):
            crashing.run(
                _request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL),
                faults=LivingIngestionFaults(service=crash),
            )

        resumed, resumed_transport = _service(root, store, clock, [])
        raw_page_loading_attempted = False
        raw_integrity_read_attempted = False
        real_raw_page_loader = resumed._raw_processing_pages
        real_integrity_reader = store._managed_file_matches_catalog

        def fail_raw_page_loading(*_args: object, **_kwargs: object) -> None:
            nonlocal raw_page_loading_attempted
            raw_page_loading_attempted = True
            raise AssertionError("retained raw was loaded before the processing policy gate")

        def fail_raw_integrity_read(*_args: object, **_kwargs: object) -> bool:
            nonlocal raw_integrity_read_attempted
            raw_integrity_read_attempted = True
            raise AssertionError("retained raw was opened before the processing policy gate")

        monkeypatch.setattr(resumed, "_raw_processing_pages", fail_raw_page_loading)
        monkeypatch.setattr(store, "_managed_file_matches_catalog", fail_raw_integrity_read)
        catalog = RetentionPolicyCatalog.load_default()
        runtime_status = DatasetRuntimeStatus.for_policy(
            catalog.lookup("alpaca", "price_bars_sip"),
            enabled=False,
            entitlement_active=True,
        )

        with pytest.raises(DatasetPolicyDenied, match="disabled"):
            resumed.resume(_RUN_1, runtime_status=runtime_status)

        assert not raw_page_loading_attempted
        assert not raw_integrity_read_attempted
        assert resumed_transport.requests == []

        cli_raw_opened = False
        cli_processing_gate_called = False
        real_store_integrity_reader = OperationalStateStore._managed_file_matches_catalog
        real_authorize_processing = RetentionPolicyEnforcer.authorize_processing

        def fail_cli_raw_open(*_args: object, **_kwargs: object) -> bool:
            nonlocal cli_raw_opened
            cli_raw_opened = True
            raise AssertionError("CLI resume opened retained raw before its policy gate")

        def deny_cli_processing(*_args: object, **_kwargs: object) -> None:
            nonlocal cli_processing_gate_called
            cli_processing_gate_called = True
            raise DatasetPolicyDenied("dataset processing is disabled")

        monkeypatch.setattr(
            "investment_platform.data.ingestion.service.PrivateDataRoot",
            lambda _configured_root, _repository_root: root,
        )
        monkeypatch.setattr(
            OperationalStateStore,
            "_managed_file_matches_catalog",
            fail_cli_raw_open,
        )
        monkeypatch.setattr(
            RetentionPolicyEnforcer,
            "authorize_processing",
            deny_cli_processing,
        )
        runner = create_cli_command_runner(
            RuntimeSettings(
                environment=RuntimeEnvironment.PRIVATE_RESEARCH,
                data_root=root.root,
                environment_was_explicit=True,
            ),
            Path(__file__).parents[2],
        )

        cli_result = runner.resume(_RUN_1)

        assert cli_result.code == "INGESTION_FAILED"
        assert cli_processing_gate_called
        assert not cli_raw_opened
        monkeypatch.setattr(
            OperationalStateStore,
            "_managed_file_matches_catalog",
            real_store_integrity_reader,
        )
        monkeypatch.setattr(
            RetentionPolicyEnforcer,
            "authorize_processing",
            real_authorize_processing,
        )
        monkeypatch.setattr(
            "investment_platform.data.ingestion.service.PrivateDataRoot",
            PrivateDataRoot,
        )

        monkeypatch.setattr(resumed, "_raw_processing_pages", real_raw_page_loader)
        monkeypatch.setattr(store, "_managed_file_matches_catalog", real_integrity_reader)
        assert resumed.resume(_RUN_1).status is IngestionRunStatus.SUCCESS

        raw_integrity_read_attempted = False
        monkeypatch.setattr(store, "_managed_file_matches_catalog", fail_raw_integrity_read)
        with pytest.raises(DatasetPolicyDenied, match="disabled"):
            resumed.reconcile_integrity(
                (_stream(),),
                environment=RuntimeEnvironment.PRIVATE_RESEARCH,
                runtime_status=runtime_status,
            )
        assert not raw_integrity_read_attempted

        backfill, backfill_transport = _service(root, store, clock, [])
        backfill_request = replace(
            _request(run_id=_RUN_2, intent=IngestionIntent.BACKFILL),
            runtime_status=runtime_status,
        )
        with pytest.raises(DatasetPolicyDenied, match="disabled"):
            backfill.run(backfill_request)
        assert not raw_integrity_read_attempted
        assert backfill_transport.requests == []

        update, update_transport = _service(root, store, clock, [])
        update_request = replace(
            _request(
                run_id=_RUN_3,
                intent=IngestionIntent.UPDATE,
                end=_OPEN + timedelta(minutes=15),
            ),
            runtime_status=runtime_status,
        )
        with pytest.raises(DatasetPolicyDenied, match="disabled"):
            update.run(update_request)
        assert not raw_integrity_read_attempted
        assert update_transport.requests == []

        repair, repair_transport = _service(root, store, clock, [])
        repair_request = replace(
            _request(
                run_id=_RUN_4,
                intent=IngestionIntent.REPAIR,
                repair_strategy=RepairStrategy.PROVIDER_REFRESH,
                repair_reason="synthetic retention-gate repair",
            ),
            runtime_status=runtime_status,
        )
        with pytest.raises(DatasetPolicyDenied, match="disabled"):
            repair.run(repair_request)
        assert not raw_integrity_read_attempted
        assert repair_transport.requests == []

        monkeypatch.setattr(store, "_managed_file_matches_catalog", real_integrity_reader)
        projection = RestartProjectionReader(store).load_run(_RUN_1)
        publication = projection.requests[0].publication
        assert publication is not None
        canonical_directory = next((root.root / "normalized").rglob("manifest.json")).parent
        shutil.rmtree(canonical_directory)
        loss_lease = store.acquire_writer_lease("test-retention-raw-replay", timedelta(minutes=1))
        try:
            OperationalReplayRepository(store).reconcile_canonical_loss(
                loss_lease,
                publication.canonical_batch_id,
                detected_at=clock.value,
            )
        finally:
            store.release_writer_lease(loss_lease)

        raw_integrity_read_attempted = False
        monkeypatch.setattr(store, "_managed_file_matches_catalog", fail_raw_integrity_read)
        replay, replay_transport = _service(root, store, clock, [])
        with pytest.raises(DatasetPolicyDenied, match="disabled"):
            replay.run_raw_replay(
                projection.requests[0].specification,
                operation_id=uuid4(),
                runtime_status=runtime_status,
            )
        assert not raw_integrity_read_attempted
        assert replay_transport.requests == []


def test_completed_run_survives_sqlite_reopen_without_provider_call(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as first_store:
        service, first_transport = _service(
            root,
            first_store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        completed = service.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))
        assert completed.status is IngestionRunStatus.SUCCESS
        assert len(first_transport.requests) == 1

    with OperationalStateStore.open(root, clock=clock) as reopened_store:
        restarted, restarted_transport = _service(root, reopened_store, clock, [])
        recovered = restarted.resume(_RUN_1)

        assert recovered == completed
        assert restarted_transport.requests == []


def test_cli_resume_of_terminal_run_does_not_read_alpaca_credentials(
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
        completed = service.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))
        assert completed.status is IngestionRunStatus.SUCCESS

    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)
    monkeypatch.setattr(
        "investment_platform.data.ingestion.service.PrivateDataRoot",
        lambda configured_root, repository_root: root,
    )
    runner = create_cli_command_runner(
        RuntimeSettings(
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            data_root=root.root,
            environment_was_explicit=True,
        ),
        Path(__file__).parents[2],
    )

    result = runner.resume(_RUN_1)

    assert result.outcome.value == "SUCCESS"
    assert result.code == "SUCCESS"
    assert result.run_id == str(_RUN_1)


def test_production_runner_extends_calendar_across_earlier_backfill_and_reopens(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    # Adjacent XNYS sessions across the year boundary prove that the calendar
    # union treats the New Year's closure as NOT_APPLICABLE, not as a data gap.
    later_open = datetime(2025, 1, 2, 14, 30, tzinfo=UTC)
    later_close = datetime(2025, 1, 2, 21, 0, tzinfo=UTC)
    earlier_open = datetime(2024, 12, 31, 14, 30, tzinfo=UTC)
    earlier_close = datetime(2024, 12, 31, 21, 0, tzinfo=UTC)
    providers = iter(
        (
            _provider(clock, [_payload(later_open)])[0],
            _provider(clock, [_payload(earlier_open)])[0],
        )
    )

    def provider_from_environment(
        cls: type[AlpacaProvider],
        *,
        feed: AlpacaFeed = AlpacaFeed.SIP,
        transport: object | None = None,
    ) -> AlpacaProvider:
        assert cls is AlpacaProvider
        assert feed is AlpacaFeed.SIP
        assert transport is not None
        return next(providers)

    monkeypatch.setattr(
        "investment_platform.data.ingestion.service.PrivateDataRoot",
        lambda configured_root, repository_root: root,
    )
    monkeypatch.setattr(
        AlpacaProvider,
        "from_environment",
        classmethod(provider_from_environment),
    )
    runner = create_cli_command_runner(
        RuntimeSettings(
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            data_root=root.root,
            environment_was_explicit=True,
        ),
        Path(__file__).parents[2],
    )

    def command(start: datetime, end: datetime) -> IngestionCommandRequest:
        return IngestionCommandRequest(
            intent=IngestionIntent.BACKFILL,
            provider="alpaca",
            dataset="price_bars_sip",
            instruments=("AAPL",),
            timeframe=Timeframe.ONE_DAY,
            session=TradingSession.REGULAR,
            adjustment=AdjustmentState.UNADJUSTED,
            start=start,
            end=end,
            max_calls=1,
            max_pages=1,
            max_expected_observations=1,
            max_estimated_bytes=100_000,
            max_estimated_cost=Decimal(0),
        )

    later = runner.run(command(later_open, later_close))
    earlier = runner.run(command(earlier_open, earlier_close))

    assert later.outcome.value == earlier.outcome.value == "SUCCESS"
    assert earlier.run_id is not None
    with OperationalStateStore.open(root, clock=clock) as store:
        with store.read_only_connection() as connection:
            watermarks = connection.execute(
                """
                SELECT watermarks.coverage_start, watermarks.exclusive_frontier,
                       calendar_snapshots.session_start_date,
                       calendar_snapshots.session_end_date
                FROM watermarks
                JOIN calendar_snapshots USING (calendar_snapshot_id)
                """
            ).fetchall()
            rebindings = connection.execute(
                "SELECT count(*) FROM calendar_coverage_rebindings"
            ).fetchone()[0]
        assert len(watermarks) == 1
        assert watermarks[0][0].startswith("2024-12-31T14:30:00")
        assert watermarks[0][1].startswith("2025-01-02T21:00:00")
        assert tuple(watermarks[0][2:]) == ("2024-01-01", "2026-01-01")
        assert rebindings >= 1

    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)
    reopened = runner.resume(UUID(earlier.run_id))
    assert reopened.outcome.value == "SUCCESS"


def test_crash_after_batch_context_freeze_resumes_processing_without_provider_call(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        crashing, first_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )

        def crash(point: LivingIngestionFaultPoint) -> None:
            if point is LivingIngestionFaultPoint.BATCH_CONTEXT_RECORDED:
                raise InjectedCrash

        with pytest.raises(InjectedCrash):
            crashing.run(
                _request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL),
                faults=LivingIngestionFaults(service=crash),
            )
        assert len(first_transport.requests) == 1

        resumed, resumed_transport = _service(root, store, clock, [])
        completed = resumed.resume(_RUN_1)

        assert completed.status is IngestionRunStatus.SUCCESS
        assert completed.canonical_batch_count == 1
        assert resumed_transport.requests == []


@pytest.mark.parametrize(
    "fault_point",
    [
        LivingIngestionFaultPoint.PLAN_PERSISTED,
        LivingIngestionFaultPoint.ACQUISITION_COMPLETED,
        LivingIngestionFaultPoint.PUBLICATION_PREPARED,
    ],
)
def test_additional_service_fault_boundaries_resume_from_durable_state(
    tmp_path: Path,
    fault_point: LivingIngestionFaultPoint,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    payload = _payload(_OPEN, _OPEN + timedelta(minutes=5))
    before_dispatch = fault_point is LivingIngestionFaultPoint.PLAN_PERSISTED
    with OperationalStateStore.open(root, clock=clock) as store:
        crashing, first_transport = _service(
            root,
            store,
            clock,
            [] if before_dispatch else [payload],
        )

        def crash(point: LivingIngestionFaultPoint) -> None:
            if point is fault_point:
                raise InjectedCrash

        with pytest.raises(InjectedCrash):
            crashing.run(
                _request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL),
                faults=LivingIngestionFaults(service=crash),
            )

        resumed, resumed_transport = _service(
            root,
            store,
            clock,
            [payload] if before_dispatch else [],
        )
        completed = resumed.resume(_RUN_1)

        assert completed.status is IngestionRunStatus.SUCCESS
        assert completed.canonical_batch_count == 1
        assert len(first_transport.requests) == (0 if before_dispatch else 1)
        assert len(resumed_transport.requests) == (1 if before_dispatch else 0)


def test_raw_catalog_and_sqlite_commit_crashes_are_restart_safe(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        crashing, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )

        def raw_crash(point: LivingIngestionFaultPoint) -> None:
            if point is LivingIngestionFaultPoint.RAW_PAGE_CATALOGED:
                raise InjectedCrash

        with pytest.raises(InjectedCrash):
            crashing.run(
                _request(
                    run_id=_RUN_1,
                    intent=IngestionIntent.BACKFILL,
                    max_pages=2,
                    max_calls=2,
                ),
                faults=LivingIngestionFaults(service=raw_crash),
            )

        reacquire, reacquire_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )

        def sqlite_crash(point: LivingIngestionFaultPoint) -> None:
            if point is LivingIngestionFaultPoint.SQLITE_COMMITTED:
                raise InjectedCrash

        with pytest.raises(InjectedCrash):
            reacquire.resume(
                _RUN_1,
                faults=LivingIngestionFaults(service=sqlite_crash),
            )
        assert len(reacquire_transport.requests) == 1

        reconcile, reconcile_transport = _service(root, store, clock, [])
        result = reconcile.resume(_RUN_1)

        assert result.status is IngestionRunStatus.SUCCESS
        assert result.raw_artifact_count == 1
        assert result.canonical_batch_count == 1
        assert reconcile_transport.requests == []


@pytest.mark.parametrize(
    "fault_point",
    [PublicationFaultPoint.RENAME, PublicationFaultPoint.REOPEN],
)
def test_raw_post_rename_crash_adopts_exact_retry_with_current_provenance(
    tmp_path: Path,
    fault_point: PublicationFaultPoint,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    payload = _payload(_OPEN, _OPEN + timedelta(minutes=5))
    with OperationalStateStore.open(root, clock=clock) as store:
        crashing, _ = _service(root, store, clock, [payload])

        def crash(point: PublicationFaultPoint) -> None:
            if point is fault_point:
                raise InjectedCrash

        with pytest.raises(InjectedCrash):
            crashing.run(
                _request(
                    run_id=_RUN_1,
                    intent=IngestionIntent.BACKFILL,
                    max_pages=2,
                    max_calls=2,
                ),
                faults=LivingIngestionFaults(raw_publication=crash),
            )
        with store.read_only_connection() as connection:
            assert connection.execute("SELECT count(*) FROM raw_artifacts").fetchone()[0] == 0

        resumed, transport = _service(root, store, clock, [payload])
        result = resumed.resume(_RUN_1)

        assert result.status is IngestionRunStatus.SUCCESS
        assert len(transport.requests) == 1
        with store.read_only_connection() as connection:
            raw = connection.execute("SELECT state FROM raw_artifacts").fetchall()
            provenance_count = connection.execute(
                "SELECT count(*) FROM raw_replay_provenance"
            ).fetchone()[0]
        assert [row[0] for row in raw] == ["VERIFIED"]
        assert provenance_count == 1


def test_raw_post_rename_crash_catalogs_changed_response_orphan_without_fake_provenance(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    original = _payload(_OPEN, _OPEN + timedelta(minutes=5))
    changed = _payload_with_revised_close(_OPEN, _OPEN + timedelta(minutes=5))
    with OperationalStateStore.open(root, clock=clock) as store:
        crashing, _ = _service(root, store, clock, [original])

        def crash(point: PublicationFaultPoint) -> None:
            if point is PublicationFaultPoint.RENAME:
                raise InjectedCrash

        with pytest.raises(InjectedCrash):
            crashing.run(
                _request(
                    run_id=_RUN_1,
                    intent=IngestionIntent.BACKFILL,
                    max_pages=2,
                    max_calls=2,
                ),
                faults=LivingIngestionFaults(raw_publication=crash),
            )

        resumed, transport = _service(root, store, clock, [changed])
        result = resumed.resume(_RUN_1)

        assert result.status is IngestionRunStatus.SUCCESS
        assert len(transport.requests) == 1
        with store.read_only_connection() as connection:
            raw = connection.execute(
                """
                SELECT artifact_id, state,
                       (SELECT count(*) FROM raw_replay_provenance AS replay
                        WHERE replay.artifact_id = raw_artifacts.artifact_id)
                FROM raw_artifacts ORDER BY state, artifact_id
                """
            ).fetchall()
        assert len(raw) == 2
        assert {row[1] for row in raw} == {"PRESENT", "VERIFIED"}
        assert next(row[2] for row in raw if row[1] == "PRESENT") == 0
        assert next(row[2] for row in raw if row[1] == "VERIFIED") == 1
        verification = Phase2OperationalDiagnostics(root, store).verify()
        raw_check = next(
            check for check in verification.checks if check.code == "RAW_CATALOG_CONTENT"
        )
        orphan_check = next(
            check for check in verification.checks if check.code == "PUBLISHED_ORPHANS"
        )
        assert verification.healthy
        assert raw_check.status is DiagnosticStatus.PASS
        assert "UNCATALOGED_RAW_PUBLICATION" not in orphan_check.issue_codes
        assert Phase2OperationalDiagnostics(root, store).status().stored_raw_artifact_count == 2

        present_artifact_id = next(str(row[0]) for row in raw if row[1] == "PRESENT")
        with store._transaction() as connection:
            attempt_id = str(
                connection.execute(
                    "SELECT attempt_id FROM request_attempts ORDER BY attempt_number DESC LIMIT 1"
                ).fetchone()[0]
            )
            connection.execute(
                """
                INSERT INTO attempt_artifact_observations(
                    attempt_id, artifact_id, retrieved_at, observed_at,
                    safe_provider_request_id
                ) VALUES (?, ?, ?, ?, NULL)
                """,
                (
                    attempt_id,
                    present_artifact_id,
                    _NOW.isoformat(),
                    _NOW.isoformat(),
                ),
            )
        referenced = next(
            check
            for check in Phase2OperationalDiagnostics(root, store).verify().checks
            if check.code == "RAW_CATALOG_CONTENT"
        )
        assert referenced.status is DiagnosticStatus.FAIL
        assert "RAW_PRESENT_REFERENCED" in referenced.issue_codes


def test_crash_after_budget_consumption_counts_restart_dispatch_again(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        crashing, first_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )

        def crash(point: LivingIngestionFaultPoint) -> None:
            if point is LivingIngestionFaultPoint.PROVIDER_BUDGET_CONSUMED:
                raise InjectedCrash

        with pytest.raises(InjectedCrash):
            crashing.run(
                _request(
                    run_id=_RUN_1,
                    intent=IngestionIntent.BACKFILL,
                    max_pages=2,
                    max_calls=2,
                ),
                faults=LivingIngestionFaults(service=crash),
            )
        assert first_transport.requests == []

        projection = RestartProjectionReader(store).load_run(_RUN_1)
        latest_attempt = projection.requests[0].latest_attempt
        assert latest_attempt is not None
        budget = ProviderBudgetRepository(store)
        first_reservation = budget.latest_for_attempt(
            latest_attempt.attempt_id,
            budget_key="historical_sip_calls",
        )
        assert first_reservation is not None
        assert first_reservation.dispatch_ordinal == 1
        assert first_reservation.state is BudgetReservationState.CONSUMED

        resumed, resumed_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        result = resumed.resume(_RUN_1)

        assert result.status is IngestionRunStatus.SUCCESS
        assert len(resumed_transport.requests) == 1
        second_reservation = budget.latest_for_attempt(
            latest_attempt.attempt_id,
            budget_key="historical_sip_calls",
        )
        assert second_reservation is not None
        assert second_reservation.dispatch_ordinal == 2
        assert second_reservation.state is BudgetReservationState.CONSUMED
        snapshot = budget.snapshot(
            ProviderBudgetWindow(
                provider="alpaca",
                dataset="price_bars_sip",
                budget_key="historical_sip_calls",
                window_start=_NOW.replace(second=0, microsecond=0),
                window_end=_NOW.replace(second=0, microsecond=0) + timedelta(minutes=1),
                limit_count=200,
            )
        )
        assert snapshot is not None
        assert snapshot.used_count == 2


def test_retryable_rate_limit_is_durable_and_dispatches_new_attempt(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    transport = QueueHttpTransport(
        [
            HttpResponse(429, b"{}", headers={"retry-after": "3600"}),
            HttpResponse(
                200,
                _payload(_OPEN, _OPEN + timedelta(minutes=5)),
                headers={
                    "x-ratelimit-limit": "200",
                    "x-ratelimit-remaining": "198",
                },
            ),
        ]
    )
    provider = AlpacaProvider(
        AlpacaCredentials("synthetic-id", "synthetic-secret"),
        feed=AlpacaFeed.SIP,
        transport=transport,
        clock=clock,
        batch_id_factory=uuid4,
    )
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    with OperationalStateStore.open(root, clock=clock) as store:
        service = LivingIngestionService(
            data_root=root,
            store=store,
            provider=provider,
            policy_enforcer=enforcer,
            clock=clock,
            lease_owner_id=f"test-service-{uuid4()}",
        )

        waiting = service.run(
            _request(
                run_id=_RUN_1,
                intent=IngestionIntent.BACKFILL,
                max_pages=2,
                max_calls=2,
            )
        )
        assert waiting.run_id == _RUN_1
        assert waiting.status is IngestionRunStatus.RUNNING
        assert len(transport.requests) == 1

        retry_state = RestartProjectionReader(store).load_run(_RUN_1).requests[0]
        assert retry_state.next_eligible_at is not None
        assert retry_state.next_eligible_at >= _NOW + timedelta(hours=1)
        still_waiting = service.resume(_RUN_1)
        assert still_waiting == waiting
        assert len(transport.requests) == 1

        clock.value = retry_state.next_eligible_at
        completed = service.resume(_RUN_1)

        assert completed.status is IngestionRunStatus.SUCCESS
        assert len(transport.requests) == 2
        final_state = RestartProjectionReader(store).load_run(_RUN_1).requests[0]
        assert final_state.latest_attempt is not None
        assert final_state.latest_attempt.attempt_number == 2


def test_exhausted_durable_budget_waits_until_next_window_without_network(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    first_transport = QueueHttpTransport(
        [
            HttpResponse(
                200,
                _payload(_OPEN),
                headers={"x-ratelimit-limit": "1", "x-ratelimit-remaining": "0"},
            )
        ]
    )
    first_provider = AlpacaProvider(
        AlpacaCredentials("synthetic-id", "synthetic-secret"),
        feed=AlpacaFeed.SIP,
        transport=first_transport,
        clock=clock,
        batch_id_factory=uuid4,
    )
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    with OperationalStateStore.open(root, clock=clock) as store:
        first = LivingIngestionService(
            data_root=root,
            store=store,
            provider=first_provider,
            policy_enforcer=enforcer,
            clock=clock,
            lease_owner_id=f"budget-first-{uuid4()}",
        )
        first.run(
            _request(
                run_id=_RUN_1,
                intent=IngestionIntent.BACKFILL,
                end=_OPEN + timedelta(minutes=5),
            )
        )

        deferred, deferred_transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN + timedelta(minutes=5))],
        )
        waiting = deferred.run(
            _request(
                run_id=_RUN_2,
                intent=IngestionIntent.BACKFILL,
                start=_OPEN + timedelta(minutes=5),
                end=_OPEN + timedelta(minutes=10),
            )
        )

        assert waiting.status is IngestionRunStatus.RUNNING
        assert deferred_transport.requests == []
        projection = RestartProjectionReader(store).load_run(_RUN_2).requests[0]
        assert projection.next_eligible_at == _NOW.replace(second=0, microsecond=0) + timedelta(
            minutes=1
        )
        assert deferred.resume(_RUN_2) == waiting
        assert deferred_transport.requests == []
        with store.read_only_connection() as connection:
            error = connection.execute(
                "SELECT code, sanitized_message FROM errors WHERE request_instance_id = ?",
                (str(projection.request_instance_id),),
            ).fetchone()
        assert error is not None
        assert error[0] == "PROVIDER_BUDGET_EXHAUSTED"
        assert "credential" not in str(error[1]).casefold()

        clock.value = projection.next_eligible_at
        completed = deferred.resume(_RUN_2)

        assert completed.status is IngestionRunStatus.SUCCESS
        assert len(deferred_transport.requests) == 1


def test_non_retryable_provider_failure_finishes_request_and_run(tmp_path: Path) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    transport = QueueHttpTransport([HttpResponse(401, b"{}")])
    provider = AlpacaProvider(
        AlpacaCredentials("synthetic-id", "synthetic-secret"),
        feed=AlpacaFeed.SIP,
        transport=transport,
        clock=clock,
        batch_id_factory=uuid4,
    )
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=clock)
    with OperationalStateStore.open(root, clock=clock) as store:
        service = LivingIngestionService(
            data_root=root,
            store=store,
            provider=provider,
            policy_enforcer=enforcer,
            clock=clock,
            lease_owner_id=f"test-service-{uuid4()}",
        )

        failed = service.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))

        assert failed.status is IngestionRunStatus.FAILED
        assert failed.raw_artifact_count == 0
        assert failed.canonical_batch_count == 0
        assert failed.open_gap_count == 1
        assert len(transport.requests) == 1
        with store.read_only_connection() as connection:
            assert connection.execute("SELECT gap_type FROM gaps").fetchone()[0] == "ACQUISITION"


def test_terminal_processing_gap_invalidates_overlapping_verified_watermark(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        initial.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))

        blocked, _ = _service(root, store, clock, [_payload()])
        failed = blocked.run(
            _request(
                run_id=_RUN_2,
                intent=IngestionIntent.REPAIR,
                repair_strategy=RepairStrategy.PROVIDER_REFRESH,
                repair_reason="exercise overlapping validation failure",
            )
        )

        assert failed.status is IngestionRunStatus.FAILED
        assert failed.open_gap_count == 1
        with store.read_only_connection() as connection:
            watermark = connection.execute(
                "SELECT verification_state, invalidated_at FROM watermarks"
            ).fetchone()
        assert watermark is not None
        assert watermark[0] == "INVALID"
        assert watermark[1] is not None


@pytest.mark.parametrize("missing_layer", ["raw", "parquet"])
def test_update_reconciles_missing_support_before_any_provider_dispatch(
    tmp_path: Path,
    missing_layer: str,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        initial, _ = _service(
            root,
            store,
            clock,
            [_payload(_OPEN, _OPEN + timedelta(minutes=5))],
        )
        initial.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))
        with store.read_only_connection() as connection:
            if missing_layer == "raw":
                relative = connection.execute(
                    "SELECT relative_path FROM raw_artifacts WHERE state = 'VERIFIED'"
                ).fetchone()[0]
            else:
                relative = connection.execute(
                    "SELECT relative_path FROM canonical_files"
                ).fetchone()[0]
        (root.root / Path(str(relative))).unlink()

        update, transport = _service(
            root,
            store,
            clock,
            [_payload(_OPEN + timedelta(minutes=10))],
        )
        with pytest.raises(LivingIngestionIncomplete, match="INTEGRITY_REPAIR_REQUIRED"):
            update.run(
                _request(
                    run_id=_RUN_2,
                    intent=IngestionIntent.UPDATE,
                    end=_OPEN + timedelta(minutes=15),
                )
            )

        assert transport.requests == []
        with store.read_only_connection() as connection:
            watermark = connection.execute(
                "SELECT verification_state, invalidated_at FROM watermarks"
            ).fetchone()
            gap = connection.execute(
                "SELECT gap_type, status FROM gaps WHERE canonical_batch_id IS NOT NULL"
            ).fetchone()
        assert watermark is not None and tuple(watermark) == ("INVALID", watermark[1])
        assert watermark[1] is not None
        assert gap is not None and tuple(gap) == ("INTEGRITY", "OPEN")


def test_all_blocked_processing_finishes_failed_without_canonical_publication(
    tmp_path: Path,
) -> None:
    root = _private_root(tmp_path)
    clock = MutableClock(_NOW)
    with OperationalStateStore.open(root, clock=clock) as store:
        service, _ = _service(root, store, clock, [_payload()])

        result = service.run(_request(run_id=_RUN_1, intent=IngestionIntent.BACKFILL))

        assert result.status is IngestionRunStatus.FAILED
        assert result.raw_artifact_count == 1
        assert result.canonical_batch_count == 0
        assert result.open_gap_count == 1
        assert not tuple((root.root / "normalized").rglob("*.parquet"))
        assert len(tuple((root.root / "quarantine").rglob("manifest.json"))) == 1
        with store.read_only_connection() as connection:
            assert (
                connection.execute("SELECT count(*) FROM quarantine_artifacts").fetchone()[0] == 1
            )
