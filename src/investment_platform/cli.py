"""Small, non-interactive Phase 2 command-line control plane."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import IntEnum
from pathlib import Path
from typing import TextIO, cast
from uuid import UUID

from pydantic import ValidationError

from investment_platform.data.diagnostics import Phase2OperationalDiagnostics
from investment_platform.data.ingestion.commands import (
    IngestionCommandOutcome,
    IngestionCommandRequest,
    IngestionCommandRunner,
    IngestionCommandRunnerFactory,
)
from investment_platform.data.ingestion.planner import IngestionIntent, RepairStrategy
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.operational.retention import (
    RetentionInvalidationTrigger,
    RetentionLifecycleRepository,
)
from investment_platform.data.operational.store import OperationalStateError, OperationalStateStore
from investment_platform.data.retention import RetentionPolicyCatalog, RetentionPolicyEnforcer
from investment_platform.data_root import (
    ALPACA_EVIDENCE_RELATIVE_PATH,
    MANAGED_NAMESPACES,
    PrivateDataRoot,
    PrivateDataRootError,
)
from investment_platform.runtime import (
    RuntimeCapabilityError,
    RuntimeConfigurationError,
    RuntimeEnvironment,
    RuntimeSettings,
    resolve_runtime_settings,
)


class ExitCode(IntEnum):
    """Stable process outcomes suitable for an external scheduler."""

    SUCCESS = 0
    USAGE = 2
    CONFIGURATION = 3
    INCOMPLETE = 4
    FAILURE = 5
    VERIFICATION_FAILED = 6


def _aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "expected an ISO-8601 timestamp with UTC offset"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed.astimezone(UTC)


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return parsed


def _nonnegative_decimal(value: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise argparse.ArgumentTypeError("expected a non-negative decimal") from error
    if not parsed.is_finite() or parsed < 0:
        raise argparse.ArgumentTypeError("expected a non-negative decimal")
    return parsed


def _run_uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("run ID must be a UUID") from error


def _add_json_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        default=argparse.SUPPRESS,
        help="emit sanitized machine-readable JSON",
    )


def _add_stream_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--provider", required=True, help="exact retention-catalog provider key")
    parser.add_argument("--dataset", required=True, help="exact retention-catalog dataset key")
    parser.add_argument(
        "--instrument",
        action="append",
        required=True,
        dest="instruments",
        metavar="SYMBOL",
        help="approved provider symbol; repeat for a bounded group (maximum 16)",
    )
    parser.add_argument(
        "--timeframe",
        required=True,
        choices=tuple(value.value for value in Timeframe),
    )
    parser.add_argument(
        "--session",
        required=True,
        choices=(TradingSession.REGULAR.value,),
    )
    parser.add_argument(
        "--adjustment",
        required=True,
        choices=(
            AdjustmentState.UNADJUSTED.value,
            AdjustmentState.SPLIT_ADJUSTED.value,
        ),
    )


def _add_budget_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--max-calls", required=True, type=_positive_integer)
    parser.add_argument("--max-pages", required=True, type=_positive_integer)
    parser.add_argument(
        "--max-expected-observations",
        required=True,
        type=_positive_integer,
    )
    parser.add_argument("--max-estimated-bytes", required=True, type=_positive_integer)
    parser.add_argument("--max-estimated-cost", required=True, type=_nonnegative_decimal)


def build_parser() -> argparse.ArgumentParser:
    """Build the public parser without resolving settings or touching the filesystem."""

    parser = argparse.ArgumentParser(
        prog="investment-platform",
        description="Phase 2 living market-data ingestion control plane",
    )
    parser.set_defaults(json_output=False)
    _add_json_option(parser)
    commands = parser.add_subparsers(dest="command", required=True)

    data_root = commands.add_parser("data-root", help="manage the external private data root")
    data_root_commands = data_root.add_subparsers(dest="data_root_command", required=True)
    initialize = data_root_commands.add_parser(
        "init",
        help="intentionally initialize the configured dedicated private root",
    )
    _add_json_option(initialize)

    backfill = commands.add_parser("backfill", help="ingest one bounded missing interval")
    _add_stream_arguments(backfill)
    backfill.add_argument("--start", required=True, type=_aware_datetime)
    backfill.add_argument("--end", required=True, type=_aware_datetime)
    _add_budget_arguments(backfill)
    _add_json_option(backfill)

    update = commands.add_parser(
        "update",
        help="extend an existing stream from its durable contiguous watermark",
    )
    _add_stream_arguments(update)
    update.add_argument(
        "--end",
        type=_aware_datetime,
        help="optional target end; policy and finalization gates may cap it further",
    )
    _add_budget_arguments(update)
    _add_json_option(update)

    repair = commands.add_parser("repair", help="repair one explicit bounded interval")
    _add_stream_arguments(repair)
    repair.add_argument("--start", required=True, type=_aware_datetime)
    repair.add_argument("--end", required=True, type=_aware_datetime)
    repair.add_argument(
        "--strategy",
        required=True,
        choices=tuple(value.value for value in RepairStrategy),
    )
    repair.add_argument(
        "--reason",
        required=True,
        help="short non-sensitive repair reason recorded with provenance",
    )
    _add_budget_arguments(repair)
    _add_json_option(repair)

    resume = commands.add_parser(
        "resume",
        help="resume one exact durable run from its persisted operational state",
    )
    resume.add_argument("--run-id", required=True, type=_run_uuid, metavar="UUID")
    _add_json_option(resume)

    status = commands.add_parser("status", help="show sanitized aggregate operational state")
    _add_json_option(status)
    verify = commands.add_parser("verify", help="run read-only Phase 2 integrity diagnostics")
    _add_json_option(verify)

    retention = commands.add_parser(
        "retention",
        help="enforce one exact durable retention lifecycle",
    )
    retention_commands = retention.add_subparsers(dest="retention_command", required=True)
    enforce = retention_commands.add_parser(
        "enforce",
        help="invalidate first, then purge exact cataloged targets for one dataset",
    )
    enforce.add_argument(
        "--provider",
        required=True,
        help="exact retention-catalog provider key",
    )
    enforce.add_argument(
        "--dataset",
        required=True,
        help="exact retention-catalog dataset key",
    )
    enforce.add_argument(
        "--trigger",
        required=True,
        choices=tuple(value.value for value in RetentionInvalidationTrigger),
        help="explicit lifecycle event proved by the caller",
    )
    _add_json_option(enforce)
    return parser


def _discover_repository_root() -> Path:
    candidates = (Path.cwd(), Path(__file__).resolve(strict=True).parent)
    for candidate in candidates:
        for directory in (candidate, *candidate.parents):
            if (directory / "pyproject.toml").is_file() and (directory / ".git").exists():
                return directory.resolve(strict=True)
    raise RuntimeConfigurationError("repository root could not be discovered")


def _production_runner_factory(
    settings: RuntimeSettings,
    repository_root: Path,
) -> IngestionCommandRunner:
    # Kept lazy so --help, data-root init, status, and verify never import provider execution.
    service = importlib.import_module("investment_platform.data.ingestion.service")
    factory = cast(
        IngestionCommandRunnerFactory,
        service.create_cli_command_runner,
    )
    return factory(settings, repository_root)


def _resolve_private_runtime(
    environ: Mapping[str, str],
    repository_root: Path,
    *,
    data_root_factory: Callable[[Path, Path], PrivateDataRoot],
) -> tuple[RuntimeSettings, PrivateDataRoot]:
    settings = resolve_runtime_settings(environ, require_explicit_environment=True)
    if settings.environment is not RuntimeEnvironment.PRIVATE_RESEARCH:
        raise RuntimeCapabilityError("Phase 2 private commands require private_research")
    root = data_root_factory(settings.require_private_root(), repository_root)
    return settings, root


def _command_request(namespace: argparse.Namespace) -> IngestionCommandRequest:
    common = {
        "provider": namespace.provider,
        "dataset": namespace.dataset,
        "instruments": tuple(namespace.instruments),
        "timeframe": Timeframe(namespace.timeframe),
        "session": TradingSession(namespace.session),
        "adjustment": AdjustmentState(namespace.adjustment),
        "max_calls": namespace.max_calls,
        "max_pages": namespace.max_pages,
        "max_expected_observations": namespace.max_expected_observations,
        "max_estimated_bytes": namespace.max_estimated_bytes,
        "max_estimated_cost": namespace.max_estimated_cost,
    }
    if namespace.command == "backfill":
        return IngestionCommandRequest(
            intent=IngestionIntent.BACKFILL,
            start=namespace.start,
            end=namespace.end,
            **common,
        )
    if namespace.command == "update":
        return IngestionCommandRequest(
            intent=IngestionIntent.UPDATE,
            end=namespace.end,
            **common,
        )
    if namespace.command == "repair":
        return IngestionCommandRequest(
            intent=IngestionIntent.REPAIR,
            start=namespace.start,
            end=namespace.end,
            repair_strategy=RepairStrategy(namespace.strategy),
            repair_reason=namespace.reason,
            **common,
        )
    raise ValueError("unsupported ingestion command")


def _write_json(stream: TextIO, payload: object) -> None:
    stream.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True))
    stream.write("\n")


def _write_error(stream: TextIO, *, code: str, message: str, json_output: bool) -> None:
    if json_output:
        _write_json(stream, {"code": code, "outcome": "FAILED"})
    else:
        stream.write(f"FAILED [{code}]: {message}\n")


def _run_data_root_init(
    root: PrivateDataRoot,
    *,
    json_output: bool,
    stdout: TextIO,
) -> int:
    sentinel = root.initialize()
    payload = {
        "code": "DATA_ROOT_READY",
        "evidence_directory": ALPACA_EVIDENCE_RELATIVE_PATH.as_posix(),
        "managed_namespaces": sorted(MANAGED_NAMESPACES),
        "outcome": "SUCCESS",
        "root_id": str(sentinel.root_id),
    }
    if json_output:
        _write_json(stdout, payload)
    else:
        stdout.write("SUCCESS [DATA_ROOT_READY]\n")
        stdout.write(f"Root ID: {sentinel.root_id}\n")
        stdout.write(
            "Private Alpaca evidence location: "
            f"{ALPACA_EVIDENCE_RELATIVE_PATH.as_posix()} (currently not fabricated)\n"
        )
    return ExitCode.SUCCESS


def _run_status_or_verify(
    command: str,
    root: PrivateDataRoot,
    *,
    environment: RuntimeEnvironment,
    json_output: bool,
    stdout: TextIO,
) -> int:
    with OperationalStateStore.open_read_only(root) as store:
        diagnostics = Phase2OperationalDiagnostics(root, store)
        if command == "status":
            snapshot = diagnostics.status()
            payload = {
                "environment": environment.value,
                "private_root": str(root.root),
                "private_root_status": "VALIDATED",
                **snapshot.model_dump(mode="json"),
            }
            if json_output:
                _write_json(stdout, payload)
            else:
                stdout.write(f"Environment: {environment.value}\n")
                stdout.write(f"Private root: {root.root} (VALIDATED)\n")
                stdout.write(f"SQLite: {'HEALTHY' if snapshot.sqlite_healthy else 'UNHEALTHY'}\n")
                stdout.write(f"Schema version: {snapshot.schema_version}\n")
                stdout.write(
                    "Runs/requests/streams: "
                    f"{snapshot.run_count}/{snapshot.request_count}/{snapshot.stream_count}\n"
                )
                stdout.write(f"Open gaps: {snapshot.open_gap_count}\n")
                stdout.write(
                    "Raw batches/canonical batches/Parquet parts/canonical rows: "
                    f"{snapshot.stored_raw_artifact_count}/"
                    f"{snapshot.canonical_batch_count}/{snapshot.parquet_part_count}/"
                    f"{snapshot.canonical_row_count}\n"
                )
                stdout.write(f"Quarantine findings: {snapshot.quarantine_artifact_count}\n")
                for policy in snapshot.dataset_policies:
                    stdout.write(
                        "Policy: "
                        f"{policy.provider}/{policy.dataset} status={policy.status} "
                        f"retention={policy.retention_mode or 'NONE'}\n"
                    )
                for stream in snapshot.streams:
                    coverage = (
                        "NONE"
                        if stream.coverage_start is None or stream.coverage_end is None
                        else (
                            f"[{stream.coverage_start.isoformat()}, "
                            f"{stream.coverage_end.isoformat()})"
                        )
                    )
                    watermark = (
                        "NONE"
                        if stream.watermark_frontier is None
                        else stream.watermark_frontier.isoformat()
                    )
                    stdout.write(
                        "Stream: "
                        f"{stream.provider}/{stream.dataset} instrument={stream.instrument_id} "
                        f"timeframe={stream.timeframe} session={stream.session} "
                        f"adjustment={stream.adjustment} coverage={coverage} "
                        f"watermark={watermark} watermark_state="
                        f"{stream.watermark_state or 'NONE'} gaps={stream.open_gap_count}\n"
                    )
                if snapshot.latest_run is not None:
                    stdout.write(
                        "Last run: "
                        f"{snapshot.latest_run.provider}/{snapshot.latest_run.dataset} "
                        f"{snapshot.latest_run.mode} {snapshot.latest_run.status}\n"
                    )
                if snapshot.non_terminal_run_count:
                    stdout.write(
                        "Non-terminal runs: "
                        f"{snapshot.non_terminal_run_count} "
                        f"(showing {len(snapshot.non_terminal_runs)})\n"
                    )
                for run in snapshot.non_terminal_runs:
                    next_action = (
                        "wait for the active writer"
                        if run.next_action == "WAIT_FOR_WRITER"
                        else f"investment-platform resume --run-id {run.run_id}"
                    )
                    stdout.write(
                        "Non-terminal run: "
                        f"{run.run_id} {run.provider}/{run.dataset} "
                        f"{run.mode} {run.status} next_action={next_action}\n"
                    )
                if snapshot.latest_error is not None:
                    stdout.write(
                        f"Latest error: {snapshot.latest_error.category}/"
                        f"{snapshot.latest_error.code}\n"
                    )
            return ExitCode.SUCCESS if snapshot.sqlite_healthy else ExitCode.FAILURE

        report = diagnostics.verify()
        payload = report.model_dump(mode="json")
        payload["healthy"] = report.healthy
        if json_output:
            _write_json(stdout, payload)
        else:
            stdout.write(f"Verification: {'PASS' if report.healthy else 'FAIL'}\n")
            for check in report.checks:
                suffix = ",".join(check.issue_codes) if check.issue_codes else "none"
                stdout.write(
                    f"{check.code}: {check.status.value} "
                    f"checked={check.checked_count} issues={check.issue_count} codes={suffix}\n"
                )
        return ExitCode.SUCCESS if report.healthy else ExitCode.VERIFICATION_FAILED


def _run_retention_enforce(
    namespace: argparse.Namespace,
    root: PrivateDataRoot,
    *,
    json_output: bool,
    stdout: TextIO,
) -> int:
    """Execute one exact state-first purge without accepting filesystem paths."""

    trigger = RetentionInvalidationTrigger(namespace.trigger)
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default())
    with OperationalStateStore.open(root) as store:
        lease = store.acquire_writer_lease("cli-retention-enforce", timedelta(minutes=2))
        try:
            lifecycle = RetentionLifecycleRepository(store, root, enforcer)
            if trigger is RetentionInvalidationTrigger.TTL_EXPIRY:
                plan = lifecycle.begin_ttl_expiry_purge(
                    lease,
                    namespace.provider,
                    namespace.dataset,
                )
            else:
                plan = lifecycle.begin_subscription_termination_purge(
                    lease,
                    namespace.provider,
                    namespace.dataset,
                )
            result = lifecycle.execute_purge(lease, plan.purge_run_id)
        finally:
            store.release_writer_lease(lease)

    payload = {
        "absent_count": result.absent_count,
        "code": "RETENTION_ENFORCED",
        "deleted_count": result.deleted_count,
        "outcome": "SUCCESS",
        "purge_run_id": result.purge_run_id,
        "replayed": result.replayed,
        "status": result.status.value,
        "target_count": result.target_count,
    }
    if json_output:
        _write_json(stdout, payload)
    else:
        stdout.write("SUCCESS [RETENTION_ENFORCED]\n")
        stdout.write(f"Purge run: {result.purge_run_id}\n")
        stdout.write(
            "Targets/deleted/already absent: "
            f"{result.target_count}/{result.deleted_count}/{result.absent_count}\n"
        )
    return ExitCode.SUCCESS


def _run_ingestion(
    namespace: argparse.Namespace,
    settings: RuntimeSettings,
    repository_root: Path,
    *,
    runner_factory: IngestionCommandRunnerFactory,
    json_output: bool,
    stdout: TextIO,
) -> int:
    runner = runner_factory(settings, repository_root)
    result = (
        runner.resume(namespace.run_id)
        if namespace.command == "resume"
        else runner.run(_command_request(namespace))
    )
    if json_output:
        _write_json(stdout, result.model_dump(mode="json"))
    else:
        stdout.write(f"{result.outcome.value} [{result.code}]\n")
        if result.run_id is not None:
            stdout.write(f"Run: {result.run_id}\n")
        stdout.write(
            "Requests completed/planned: "
            f"{result.completed_request_count}/{result.planned_request_count}\n"
        )
        stdout.write(
            f"Raw artifacts/canonical batches: {result.raw_artifact_count}/"
            f"{result.canonical_batch_count}\n"
        )
        stdout.write(f"Open gaps: {result.open_gap_count}\n")
    if result.outcome in {IngestionCommandOutcome.SUCCESS, IngestionCommandOutcome.NO_OP}:
        return ExitCode.SUCCESS
    if result.outcome is IngestionCommandOutcome.INCOMPLETE:
        return ExitCode.INCOMPLETE
    return ExitCode.FAILURE


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    repository_root: Path | None = None,
    runner_factory: IngestionCommandRunnerFactory = _production_runner_factory,
    data_root_factory: Callable[[Path, Path], PrivateDataRoot] = PrivateDataRoot,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Execute one CLI command and return a stable scheduler-facing exit code."""

    output = sys.stdout if stdout is None else stdout
    errors = sys.stderr if stderr is None else stderr
    namespace = build_parser().parse_args(argv)
    json_output = bool(namespace.json_output)
    values = os.environ if environ is None else environ
    try:
        repo = _discover_repository_root() if repository_root is None else repository_root
        settings, root = _resolve_private_runtime(
            values,
            repo,
            data_root_factory=data_root_factory,
        )
        if namespace.command == "data-root":
            return int(_run_data_root_init(root, json_output=json_output, stdout=output))
        root.validate()
        if namespace.command in {"status", "verify"}:
            return int(
                _run_status_or_verify(
                    namespace.command,
                    root,
                    environment=settings.environment,
                    json_output=json_output,
                    stdout=output,
                )
            )
        if namespace.command == "retention":
            return int(
                _run_retention_enforce(
                    namespace,
                    root,
                    json_output=json_output,
                    stdout=output,
                )
            )
        return int(
            _run_ingestion(
                namespace,
                settings,
                repo,
                runner_factory=runner_factory,
                json_output=json_output,
                stdout=output,
            )
        )
    except (RuntimeConfigurationError, RuntimeCapabilityError):
        _write_error(
            errors,
            code="RUNTIME_CONFIGURATION_ERROR",
            message="the explicit runtime profile is missing or cannot perform this command",
            json_output=json_output,
        )
        return int(ExitCode.CONFIGURATION)
    except PrivateDataRootError:
        _write_error(
            errors,
            code="PRIVATE_DATA_ROOT_ERROR",
            message="the configured private data root is absent, unowned, or unsafe",
            json_output=json_output,
        )
        return int(ExitCode.CONFIGURATION)
    except (ValidationError, ValueError):
        _write_error(
            errors,
            code="COMMAND_REJECTED",
            message="the command does not satisfy the bounded Phase 2 contract",
            json_output=json_output,
        )
        return int(ExitCode.USAGE)
    except OperationalStateError:
        _write_error(
            errors,
            code="OPERATIONAL_STATE_ERROR",
            message="the operational state could not be opened or verified safely",
            json_output=json_output,
        )
        return int(ExitCode.FAILURE)
    except Exception:
        _write_error(
            errors,
            code="COMMAND_FAILED",
            message="the command failed; inspect sanitized operational status",
            json_output=json_output,
        )
        return int(ExitCode.FAILURE)


if __name__ == "__main__":  # pragma: no cover - console-script path
    raise SystemExit(main())


__all__ = ["ExitCode", "build_parser", "main"]
