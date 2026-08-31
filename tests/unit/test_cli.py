"""Offline tests for the Phase 2 argparse control plane."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from investment_platform.cli import ExitCode, build_parser, main
from investment_platform.data.ingestion.commands import (
    IngestionCommandOutcome,
    IngestionCommandRequest,
    IngestionCommandResult,
    IngestionCommandRunner,
)
from investment_platform.data.ingestion.planner import IngestionIntent, RepairStrategy
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.operational.store import OperationalStateStore, _format_utc
from investment_platform.data.retention import RetentionPolicyCatalog
from investment_platform.data_root import (
    ALPACA_EVIDENCE_RELATIVE_PATH,
    MANAGED_NAMESPACES,
    PrivateDataRoot,
)
from investment_platform.runtime import RuntimeEnvironment, RuntimeSettings

pytestmark = pytest.mark.unit


def _temporary_root(root: Path, repository_root: Path) -> PrivateDataRoot:
    return PrivateDataRoot(
        root,
        repository_root,
        allow_temporary_for_tests=True,
    )


def _environment(root: Path) -> dict[str, str]:
    return {
        "INVESTMENT_PLATFORM_ENV": "private_research",
        "INVESTMENT_PLATFORM_DATA_ROOT": str(root),
    }


def _ingestion_arguments(command: str) -> list[str]:
    values = [
        command,
        "--provider",
        "alpaca",
        "--dataset",
        "price_bars_sip",
        "--instrument",
        "AAPL",
        "--timeframe",
        "5m",
        "--session",
        "regular",
        "--adjustment",
        "unadjusted",
    ]
    if command in {"backfill", "repair"}:
        values.extend(
            [
                "--start",
                "2026-08-20T13:30:00Z",
                "--end",
                "2026-08-20T14:00:00Z",
            ]
        )
    if command == "repair":
        values.extend(
            [
                "--strategy",
                "MISSING_ONLY",
                "--reason",
                "controlled-gap-repair",
            ]
        )
    values.extend(
        [
            "--max-calls",
            "2",
            "--max-pages",
            "2",
            "--max-expected-observations",
            "100",
            "--max-estimated-bytes",
            "1000000",
            "--max-estimated-cost",
            "0",
        ]
    )
    return values


class _RecordingRunner:
    def __init__(self, result: IngestionCommandResult) -> None:
        self.result = result
        self.requests: list[IngestionCommandRequest] = []
        self.resumed_run_ids: list[UUID] = []

    def run(self, request: IngestionCommandRequest) -> IngestionCommandResult:
        self.requests.append(request)
        return self.result

    def resume(self, run_id: UUID) -> IngestionCommandResult:
        self.resumed_run_ids.append(run_id)
        return self.result


class _RecordingFactory:
    def __init__(self, runner: IngestionCommandRunner) -> None:
        self.runner = runner
        self.calls: list[tuple[RuntimeSettings, Path]] = []

    def __call__(
        self,
        settings: RuntimeSettings,
        repository_root: Path,
    ) -> IngestionCommandRunner:
        self.calls.append((settings, repository_root))
        return self.runner


class _FailingRunner:
    def run(self, request: IngestionCommandRequest) -> IngestionCommandResult:
        del request
        raise RuntimeError("sensitive-provider-detail-do-not-print")

    def resume(self, run_id: UUID) -> IngestionCommandResult:
        del run_id
        raise RuntimeError("sensitive-provider-detail-do-not-print")


@pytest.fixture
def repository_root() -> Path:
    return Path(__file__).parents[2].resolve(strict=True)


@pytest.fixture
def initialized_root(tmp_path: Path, repository_root: Path) -> Path:
    root = tmp_path / "dedicated-cli-private-runtime"
    _temporary_root(root, repository_root).initialize()
    return root


def test_help_lists_the_complete_manual_control_plane() -> None:
    help_text = build_parser().format_help()

    assert "data-root" in help_text
    assert "backfill" in help_text
    assert "update" in help_text
    assert "repair" in help_text
    assert "resume" in help_text
    assert "status" in help_text
    assert "verify" in help_text
    assert "retention" in help_text


@pytest.mark.parametrize(
    ("option", "unsupported"),
    [
        ("--session", "pre_market"),
        ("--adjustment", "split_and_dividend_adjusted"),
    ],
)
def test_parser_exposes_only_the_approved_phase2_stream_modes(
    option: str,
    unsupported: str,
) -> None:
    arguments = _ingestion_arguments("backfill")
    arguments[arguments.index(option) + 1] = unsupported

    with pytest.raises(SystemExit) as exit_info:
        build_parser().parse_args(arguments)

    assert exit_info.value.code == ExitCode.USAGE


def test_data_root_init_requires_explicit_private_research(
    tmp_path: Path,
    repository_root: Path,
) -> None:
    output = StringIO()
    errors = StringIO()

    exit_code = main(
        ["data-root", "init"],
        environ={},
        repository_root=repository_root,
        data_root_factory=_temporary_root,
        stdout=output,
        stderr=errors,
    )

    assert exit_code == ExitCode.CONFIGURATION
    assert output.getvalue() == ""
    assert "RUNTIME_CONFIGURATION_ERROR" in errors.getvalue()
    assert "INVESTMENT_PLATFORM_DATA_ROOT" not in errors.getvalue()
    assert not tuple(tmp_path.iterdir())


def test_data_root_init_is_intentional_idempotent_and_creates_full_layout(
    tmp_path: Path,
    repository_root: Path,
) -> None:
    root = tmp_path / "dedicated-cli-private-runtime"
    first_output = StringIO()
    second_output = StringIO()
    arguments = ["data-root", "init", "--json"]

    first_exit = main(
        arguments,
        environ=_environment(root),
        repository_root=repository_root,
        data_root_factory=_temporary_root,
        stdout=first_output,
    )
    second_exit = main(
        arguments,
        environ=_environment(root),
        repository_root=repository_root,
        data_root_factory=_temporary_root,
        stdout=second_output,
    )

    first = json.loads(first_output.getvalue())
    second = json.loads(second_output.getvalue())
    assert first_exit == second_exit == ExitCode.SUCCESS
    assert first == second
    assert first["outcome"] == "SUCCESS"
    assert first["managed_namespaces"] == sorted(MANAGED_NAMESPACES)
    assert first["evidence_directory"] == ALPACA_EVIDENCE_RELATIVE_PATH.as_posix()
    assert all((root / namespace).is_dir() for namespace in MANAGED_NAMESPACES)
    assert (root / ALPACA_EVIDENCE_RELATIVE_PATH).is_dir()
    assert not tuple((root / ALPACA_EVIDENCE_RELATIVE_PATH).iterdir())


@pytest.mark.parametrize(
    ("command", "intent"),
    [
        ("backfill", IngestionIntent.BACKFILL),
        ("update", IngestionIntent.UPDATE),
        ("repair", IngestionIntent.REPAIR),
    ],
)
def test_ingestion_commands_pass_one_typed_bounded_request_to_injected_runner(
    command: str,
    intent: IngestionIntent,
    initialized_root: Path,
    repository_root: Path,
) -> None:
    result = IngestionCommandResult(
        outcome=IngestionCommandOutcome.SUCCESS,
        code="COMPLETED",
        run_id="run_v1_synthetic",
        planned_request_count=1,
        completed_request_count=1,
        raw_artifact_count=1,
        canonical_batch_count=1,
    )
    runner = _RecordingRunner(result)
    factory = _RecordingFactory(runner)
    output = StringIO()

    exit_code = main(
        [*_ingestion_arguments(command), "--json"],
        environ=_environment(initialized_root),
        repository_root=repository_root,
        runner_factory=factory,
        data_root_factory=_temporary_root,
        stdout=output,
    )

    assert exit_code == ExitCode.SUCCESS
    assert json.loads(output.getvalue())["outcome"] == "SUCCESS"
    assert len(factory.calls) == 1
    assert factory.calls[0][0].environment is RuntimeEnvironment.PRIVATE_RESEARCH
    assert factory.calls[0][1] == repository_root
    assert len(runner.requests) == 1
    request = runner.requests[0]
    assert request.intent is intent
    assert request.provider == "alpaca"
    assert request.dataset == "price_bars_sip"
    assert request.instruments == ("AAPL",)
    assert request.timeframe is Timeframe.FIVE_MINUTES
    assert request.session is TradingSession.REGULAR
    assert request.adjustment is AdjustmentState.UNADJUSTED
    assert request.max_calls == 2
    assert request.max_pages == 2
    assert request.max_expected_observations == 100
    assert request.max_estimated_bytes == 1_000_000
    assert request.max_estimated_cost == 0
    if intent is IngestionIntent.UPDATE:
        assert request.start is None
        assert request.end is None
    else:
        assert request.start is not None and request.start.utcoffset() is not None
        assert request.end is not None and request.end.utcoffset() is not None
    if intent is IngestionIntent.REPAIR:
        assert request.repair_strategy is RepairStrategy.MISSING_ONLY
        assert request.repair_reason == "controlled-gap-repair"


@pytest.mark.parametrize(
    ("outcome", "expected_exit"),
    [
        (IngestionCommandOutcome.NO_OP, ExitCode.SUCCESS),
        (IngestionCommandOutcome.INCOMPLETE, ExitCode.INCOMPLETE),
        (IngestionCommandOutcome.FAILED, ExitCode.FAILURE),
    ],
)
def test_scheduler_exit_codes_distinguish_noop_incomplete_and_failure(
    outcome: IngestionCommandOutcome,
    expected_exit: ExitCode,
    initialized_root: Path,
    repository_root: Path,
) -> None:
    runner = _RecordingRunner(
        IngestionCommandResult(
            outcome=outcome,
            code=f"{outcome.value}_RESULT",
        )
    )

    exit_code = main(
        _ingestion_arguments("update"),
        environ=_environment(initialized_root),
        repository_root=repository_root,
        runner_factory=_RecordingFactory(runner),
        data_root_factory=_temporary_root,
        stdout=StringIO(),
    )

    assert exit_code == expected_exit


def test_resume_passes_only_the_typed_durable_run_identity_to_the_runner(
    initialized_root: Path,
    repository_root: Path,
) -> None:
    run_id = uuid4()
    runner = _RecordingRunner(
        IngestionCommandResult(
            outcome=IngestionCommandOutcome.INCOMPLETE,
            code="WAIT_RETRY",
            run_id=str(run_id),
            planned_request_count=1,
        )
    )
    output = StringIO()

    exit_code = main(
        ["resume", "--run-id", str(run_id), "--json"],
        environ=_environment(initialized_root),
        repository_root=repository_root,
        runner_factory=_RecordingFactory(runner),
        data_root_factory=_temporary_root,
        stdout=output,
    )

    assert exit_code == ExitCode.INCOMPLETE
    assert json.loads(output.getvalue())["run_id"] == str(run_id)
    assert runner.resumed_run_ids == [run_id]
    assert runner.requests == []


def test_resume_rejects_a_non_uuid_run_identity_before_constructing_the_runner(
    initialized_root: Path,
    repository_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runner = _RecordingRunner(
        IngestionCommandResult(outcome=IngestionCommandOutcome.SUCCESS, code="COMPLETED")
    )
    factory = _RecordingFactory(runner)

    with pytest.raises(SystemExit) as exit_info:
        main(
            ["resume", "--run-id", "not-a-uuid"],
            environ=_environment(initialized_root),
            repository_root=repository_root,
            runner_factory=factory,
            data_root_factory=_temporary_root,
            stdout=StringIO(),
            stderr=StringIO(),
        )

    assert exit_info.value.code == ExitCode.USAGE
    assert factory.calls == []
    assert "not-a-uuid" not in capsys.readouterr().err


def test_resume_terminal_result_uses_scheduler_success_exit(
    initialized_root: Path,
    repository_root: Path,
) -> None:
    run_id = uuid4()
    runner = _RecordingRunner(
        IngestionCommandResult(
            outcome=IngestionCommandOutcome.SUCCESS,
            code="SUCCESS",
            run_id=str(run_id),
            planned_request_count=1,
            completed_request_count=1,
            raw_artifact_count=1,
            canonical_batch_count=1,
        )
    )

    exit_code = main(
        ["resume", "--run-id", str(run_id)],
        environ=_environment(initialized_root),
        repository_root=repository_root,
        runner_factory=_RecordingFactory(runner),
        data_root_factory=_temporary_root,
        stdout=StringIO(),
    )

    assert exit_code == ExitCode.SUCCESS
    assert runner.resumed_run_ids == [run_id]


def test_status_and_verify_use_sanitized_operational_diagnostics(
    initialized_root: Path,
    repository_root: Path,
) -> None:
    status_output = StringIO()
    verify_output = StringIO()

    status_exit = main(
        ["status", "--json"],
        environ=_environment(initialized_root),
        repository_root=repository_root,
        data_root_factory=_temporary_root,
        stdout=status_output,
    )
    verify_exit = main(
        ["verify", "--json"],
        environ=_environment(initialized_root),
        repository_root=repository_root,
        data_root_factory=_temporary_root,
        stdout=verify_output,
    )

    status = json.loads(status_output.getvalue())
    verification = json.loads(verify_output.getvalue())
    assert status_exit == ExitCode.SUCCESS
    assert status["environment"] == "private_research"
    assert status["private_root"] == str(initialized_root)
    assert status["private_root_status"] == "VALIDATED"
    assert status["sqlite_healthy"] is True
    assert status["run_count"] == 0
    assert verify_exit == ExitCode.SUCCESS
    assert verification["healthy"] is True
    assert verification["checks"]
    assert str(initialized_root) not in verify_output.getvalue()


def test_retention_enforce_runs_one_exact_catalog_driven_subscription_purge(
    initialized_root: Path,
    repository_root: Path,
) -> None:
    root = _temporary_root(initialized_root, repository_root)
    policy = RetentionPolicyCatalog.load_default().lookup(
        "twelve_data",
        "price_bars_us_daily",
    )
    recorded_at = _format_utc(datetime(2026, 8, 31, 12, 0, tzinfo=UTC))
    snapshot_id = "policy_snapshot_cli_retention_v1"
    with OperationalStateStore.open(root) as store:
        lease = store.acquire_writer_lease("test-cli-retention", timedelta(minutes=1))
        try:
            with store._leased_transaction(lease) as connection:
                connection.execute(
                    """
                    INSERT INTO policy_snapshots(
                        policy_snapshot_id, policy_id, revision, policy_hash,
                        provider, dataset, retention_mode, verified_at, captured_at,
                        expires_at, entitlement_active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
                    """,
                    (
                        snapshot_id,
                        policy.policy_id,
                        policy.revision,
                        policy.content_hash,
                        policy.provider,
                        policy.dataset,
                        policy.mode.value,
                        policy.verified_on.isoformat(),
                        recorded_at,
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
                        policy.provider,
                        policy.dataset,
                        policy.mode.value,
                        snapshot_id,
                        recorded_at,
                        recorded_at,
                    ),
                )
        finally:
            store.release_writer_lease(lease)

    output = StringIO()
    exit_code = main(
        [
            "retention",
            "enforce",
            "--provider",
            policy.provider,
            "--dataset",
            policy.dataset,
            "--trigger",
            "SUBSCRIPTION_TERMINATION",
            "--json",
        ],
        environ=_environment(initialized_root),
        repository_root=repository_root,
        data_root_factory=_temporary_root,
        stdout=output,
    )

    result = json.loads(output.getvalue())
    assert exit_code == ExitCode.SUCCESS
    assert result["outcome"] == "SUCCESS"
    assert result["status"] == "SUCCESS"
    assert result["target_count"] == 0


def test_unexpected_runner_exception_is_redacted(
    initialized_root: Path,
    repository_root: Path,
) -> None:
    output = StringIO()
    errors = StringIO()
    secret = "do-not-print-this-value"

    exit_code = main(
        [*_ingestion_arguments("backfill"), "--json"],
        environ=_environment(initialized_root),
        repository_root=repository_root,
        runner_factory=_RecordingFactory(_FailingRunner()),
        data_root_factory=_temporary_root,
        stdout=output,
        stderr=errors,
    )

    assert exit_code == ExitCode.FAILURE
    assert output.getvalue() == ""
    assert json.loads(errors.getvalue()) == {"code": "COMMAND_FAILED", "outcome": "FAILED"}
    assert secret not in errors.getvalue()


def test_uninitialized_root_fails_closed_without_echoing_configured_path(
    tmp_path: Path,
    repository_root: Path,
) -> None:
    root = tmp_path / "dedicated-uninitialized-private-runtime"
    errors = StringIO()

    exit_code = main(
        ["status"],
        environ=_environment(root),
        repository_root=repository_root,
        data_root_factory=_temporary_root,
        stdout=StringIO(),
        stderr=errors,
    )

    assert exit_code == ExitCode.CONFIGURATION
    assert "PRIVATE_DATA_ROOT_ERROR" in errors.getvalue()
    assert str(root) not in errors.getvalue()
