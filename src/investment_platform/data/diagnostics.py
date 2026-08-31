"""Sanitized read-only status and integrity diagnostics for Phase 2 ingestion.

The diagnostics boundary deliberately returns counts, fixed check codes, and sanitized workflow
metadata only.  It never returns payload bytes, canonical observations, provider request metadata,
error messages, credentials, private paths, or catalog identities.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Final
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from investment_platform.data.operational.schema import LATEST_SCHEMA_VERSION
from investment_platform.data.operational.store import OperationalStateStore
from investment_platform.data.retention import (
    DatasetPolicyDenied,
    DatasetPolicyStatus,
    RetentionLayer,
    RetentionMode,
    RetentionPolicyCatalog,
)
from investment_platform.data.storage._publication import (
    PublicationError,
    file_integrity,
    iter_safe_regular_files,
    safe_partition_value,
)
from investment_platform.data.storage.canonical_batches import (
    CanonicalBatchManifest,
    verify_canonical_batch_directory,
)
from investment_platform.data.storage.living_raw import (
    RawArtifactManifest,
    raw_artifact_relative_directory,
    verify_raw_artifact_directory,
)
from investment_platform.data.storage.quarantine import (
    QuarantineArtifactManifest,
    quarantine_artifact_relative_directory,
    verify_quarantine_artifact_directory,
)
from investment_platform.data.storage.recovery import (
    PublicationRecoveryInspector,
    RecoveryInspectionState,
)
from investment_platform.data.storage.transport_spool import (
    TransportSpoolInspectionState,
    TransportSpoolIntegrityError,
    TransportSpoolStore,
)
from investment_platform.data_root import PrivateDataRoot, PrivateDataRootError

_SAFE_OUTPUT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}\Z")
_RAW_DIRECTORY = re.compile(
    r"raw/provider=[a-z0-9][a-z0-9._-]{0,127}/"
    r"dataset=[a-z0-9][a-z0-9._-]{0,127}/artifacts/artifact=[0-9a-f]{32}\Z"
)
_CANONICAL_DIRECTORY = re.compile(
    r"normalized/price_bars/provider=[a-z0-9][a-z0-9._-]{0,127}/"
    r"dataset=[a-z0-9][a-z0-9._-]{0,127}/batches/batch=[0-9a-f]{32}\Z"
)
_QUARANTINE_DIRECTORY = re.compile(
    r"quarantine/provider=[a-z0-9][a-z0-9._-]{0,127}/"
    r"dataset=[a-z0-9][a-z0-9._-]{0,127}/artifacts/"
    r"artifact=quarantine_v1_[0-9a-f]{64}\Z"
)
_DURABLE_MODES: Final = frozenset(
    {
        RetentionMode.TTL,
        RetentionMode.SUBSCRIPTION_BOUND,
        RetentionMode.DURABLE_AUTHORIZED,
        RetentionMode.SYNTHETIC_UNRESTRICTED,
    }
)


class DiagnosticStatus(StrEnum):
    """Outcome of one stable verification primitive."""

    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class _FrozenDiagnosticModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class DiagnosticCheck(_FrozenDiagnosticModel):
    """Sanitized check result; issue codes never contain filesystem or row values."""

    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")
    status: DiagnosticStatus
    checked_count: int = Field(ge=0)
    issue_count: int = Field(ge=0)
    issue_codes: tuple[str, ...] = ()

    @field_validator("issue_codes", mode="after")
    @classmethod
    def validate_issue_codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("diagnostic issue codes must be unique and sorted")
        if any(re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", code) is None for code in value):
            raise ValueError("diagnostic issue code is not sanitized")
        return value


class LatestRunSummary(_FrozenDiagnosticModel):
    """Non-sensitive fields from the most recent ingestion run."""

    environment: str
    provider: str
    dataset: str
    mode: str
    status: str
    observed_at: datetime


class LatestErrorSummary(_FrozenDiagnosticModel):
    """Only stable error category/code are exposed; the stored message is never read."""

    category: str
    code: str
    occurred_at: datetime


class DatasetPolicySummary(_FrozenDiagnosticModel):
    provider: str
    dataset: str
    status: str
    retention_mode: str | None


class StreamStatusSummary(_FrozenDiagnosticModel):
    """One sanitized stream's durable coverage frontier and open-gap state."""

    stream_id: str
    provider: str
    dataset: str
    instrument_id: UUID
    timeframe: str
    session: str
    adjustment: str
    coverage_start: datetime | None
    coverage_end: datetime | None
    watermark_frontier: datetime | None
    watermark_state: str | None
    open_gap_count: int = Field(ge=0)

    @field_validator(
        "coverage_start",
        "coverage_end",
        "watermark_frontier",
        mode="after",
    )
    @classmethod
    def normalize_times(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("stream status timestamps must be timezone-aware")
        return value.astimezone(UTC)


class OperationalStatusSnapshot(_FrozenDiagnosticModel):
    """Small scheduler/control-panel snapshot without private observations or paths."""

    root_valid: bool
    sqlite_healthy: bool
    schema_version: int = Field(ge=0)
    run_count: int = Field(ge=0)
    request_count: int = Field(ge=0)
    stream_count: int = Field(ge=0)
    open_gap_count: int = Field(ge=0)
    stored_raw_artifact_count: int = Field(ge=0)
    canonical_batch_count: int = Field(ge=0)
    parquet_part_count: int = Field(ge=0)
    canonical_row_count: int = Field(ge=0)
    quarantine_artifact_count: int = Field(ge=0)
    active_writer_lease: bool
    latest_run: LatestRunSummary | None
    latest_error: LatestErrorSummary | None
    dataset_policies: tuple[DatasetPolicySummary, ...]
    streams: tuple[StreamStatusSummary, ...]


class Phase2VerificationReport(_FrozenDiagnosticModel):
    """Full read-only verification report with no user/provider content."""

    checked_at: datetime
    checks: tuple[DiagnosticCheck, ...]

    @property
    def healthy(self) -> bool:
        return bool(self.checks) and all(
            check.status is DiagnosticStatus.PASS for check in self.checks
        )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("stored timestamp is not timezone-aware")
    return parsed.astimezone(UTC)


def _safe_output(value: object, *, fallback: str = "REDACTED") -> str:
    rendered = str(value)
    return rendered if _SAFE_OUTPUT.fullmatch(rendered) else fallback


def _check(
    code: str,
    *,
    checked_count: int,
    issue_count: int = 0,
    issue_codes: Iterable[str] = (),
    warning: bool = False,
) -> DiagnosticCheck:
    unique_codes = tuple(sorted(set(issue_codes)))
    if issue_count == 0:
        status = DiagnosticStatus.PASS
    elif warning:
        status = DiagnosticStatus.WARN
    else:
        status = DiagnosticStatus.FAIL
    return DiagnosticCheck(
        code=code,
        status=status,
        checked_count=checked_count,
        issue_count=issue_count,
        issue_codes=unique_codes,
    )


def _managed_path(data_root: PrivateDataRoot, root_id: UUID, relative: object) -> Path:
    value = PurePosixPath(str(relative))
    return data_root.managed_path(Path(*value.parts), expected_root_id=root_id)


def _canonical_relative_directory(provider: str, dataset: str, batch_id: str) -> PurePosixPath:
    provider = safe_partition_value(provider, label="provider")
    dataset = safe_partition_value(dataset, label="dataset")
    if re.fullmatch(r"batch_v1_[0-9a-f]{64}", batch_id) is None:
        raise PublicationError("canonical batch identity is invalid")
    return PurePosixPath(
        "normalized",
        "price_bars",
        f"provider={provider}",
        f"dataset={dataset}",
        "batches",
        f"batch={batch_id.removeprefix('batch_v1_')[:32]}",
    )


class Phase2OperationalDiagnostics:
    """Read-only diagnostics over one already initialized root and operational store."""

    def __init__(
        self,
        data_root: PrivateDataRoot,
        store: OperationalStateStore,
        *,
        retention_catalog: RetentionPolicyCatalog | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        sentinel = data_root.validate()
        if str(sentinel.root_id) != store.root_id:
            raise ValueError("operational store belongs to a different private root")
        self._data_root = data_root
        self._store = store
        self._root_id = sentinel.root_id
        self._catalog = retention_catalog or RetentionPolicyCatalog.load_default()
        self._clock = clock

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("diagnostic clock must return an aware datetime")
        return value.astimezone(UTC)

    def status(self) -> OperationalStatusSnapshot:
        """Read compact aggregate state without reopening private market-data values."""

        self._data_root.validate(expected_root_id=self._root_id)
        sqlite_status = self._store.diagnostics()
        with self._store.read_only_connection() as connection:
            counts = connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM ingestion_runs) AS run_count,
                    (SELECT count(*) FROM request_instances) AS request_count,
                    (SELECT count(*) FROM stream_keys) AS stream_count,
                    (SELECT count(*) FROM gaps
                     WHERE status IN ('OPEN', 'REPAIRING')) AS open_gap_count,
                    (SELECT count(*) FROM raw_artifacts
                     WHERE state IN ('PRESENT', 'VERIFIED')) AS raw_count,
                    (SELECT count(*) FROM canonical_batches
                     WHERE state IN ('PUBLISHED', 'VERIFIED')) AS batch_count,
                    (SELECT count(*) FROM canonical_files AS file
                     JOIN canonical_batches AS batch
                       ON batch.canonical_batch_id = file.canonical_batch_id
                     WHERE batch.state IN ('PUBLISHED', 'VERIFIED')) AS part_count,
                    (SELECT coalesce(sum(row_count), 0) FROM canonical_batches
                     WHERE state IN ('PUBLISHED', 'VERIFIED')) AS row_count,
                    (SELECT count(*) FROM quarantine_artifacts
                     WHERE state = 'VERIFIED') AS quarantine_count
                """
            ).fetchone()
            if counts is None:
                raise sqlite3.DatabaseError("status aggregate query returned no row")
            latest_run_row = connection.execute(
                """
                SELECT environment, provider, dataset, mode, status,
                       coalesce(completed_at, started_at, created_at) AS observed_at
                FROM ingestion_runs
                ORDER BY observed_at DESC, run_id DESC LIMIT 1
                """
            ).fetchone()
            # Deliberately omit sanitized_message and every request/provider identifier.
            latest_error_row = connection.execute(
                """
                SELECT category, code, occurred_at FROM errors
                ORDER BY occurred_at DESC, error_id DESC LIMIT 1
                """
            ).fetchone()
            policy_rows = connection.execute(
                """
                SELECT provider, dataset, status, retention_mode
                FROM dataset_policy_status ORDER BY provider, dataset
                """
            ).fetchall()
            stream_rows = connection.execute(
                """
                WITH retained_coverage AS (
                    SELECT stream_id, min(interval_start) AS coverage_start,
                           max(interval_end) AS coverage_end
                    FROM coverage_segments
                    WHERE retained = 1 AND verification_state = 'VERIFIED'
                    GROUP BY stream_id
                ), open_gaps AS (
                    SELECT stream_id, count(*) AS gap_count
                    FROM gaps WHERE status IN ('OPEN', 'REPAIRING')
                    GROUP BY stream_id
                )
                SELECT stream.stream_id, stream.provider, stream.dataset,
                       stream.instrument_id, stream.timeframe, stream.session,
                       stream.adjustment, coverage.coverage_start, coverage.coverage_end,
                       watermark.exclusive_frontier, watermark.verification_state,
                       coalesce(gap.gap_count, 0) AS gap_count
                FROM stream_keys AS stream
                LEFT JOIN retained_coverage AS coverage
                  ON coverage.stream_id = stream.stream_id
                LEFT JOIN open_gaps AS gap ON gap.stream_id = stream.stream_id
                LEFT JOIN watermarks AS watermark ON watermark.stream_id = stream.stream_id
                ORDER BY stream.provider, stream.dataset, stream.instrument_id,
                         stream.timeframe, stream.session, stream.adjustment
                """
            ).fetchall()

        latest_run = None
        if latest_run_row is not None:
            latest_run = LatestRunSummary(
                environment=_safe_output(latest_run_row["environment"]),
                provider=_safe_output(latest_run_row["provider"]),
                dataset=_safe_output(latest_run_row["dataset"]),
                mode=_safe_output(latest_run_row["mode"]),
                status=_safe_output(latest_run_row["status"]),
                observed_at=_parse_utc(latest_run_row["observed_at"]),
            )
        latest_error = None
        if latest_error_row is not None:
            latest_error = LatestErrorSummary(
                category=_safe_output(latest_error_row["category"]),
                code=_safe_output(latest_error_row["code"]),
                occurred_at=_parse_utc(latest_error_row["occurred_at"]),
            )
        policies = tuple(
            DatasetPolicySummary(
                provider=_safe_output(row["provider"]),
                dataset=_safe_output(row["dataset"]),
                status=_safe_output(row["status"]),
                retention_mode=(
                    _safe_output(row["retention_mode"])
                    if row["retention_mode"] is not None
                    else None
                ),
            )
            for row in policy_rows
        )
        streams = tuple(
            StreamStatusSummary(
                stream_id=_safe_output(row["stream_id"]),
                provider=_safe_output(row["provider"]),
                dataset=_safe_output(row["dataset"]),
                instrument_id=UUID(str(row["instrument_id"])),
                timeframe=_safe_output(row["timeframe"]),
                session=_safe_output(row["session"]),
                adjustment=_safe_output(row["adjustment"]),
                coverage_start=(
                    None if row["coverage_start"] is None else _parse_utc(row["coverage_start"])
                ),
                coverage_end=(
                    None if row["coverage_end"] is None else _parse_utc(row["coverage_end"])
                ),
                watermark_frontier=(
                    None
                    if row["exclusive_frontier"] is None
                    else _parse_utc(row["exclusive_frontier"])
                ),
                watermark_state=(
                    None
                    if row["verification_state"] is None
                    else _safe_output(row["verification_state"])
                ),
                open_gap_count=int(row["gap_count"]),
            )
            for row in stream_rows
        )
        return OperationalStatusSnapshot(
            root_valid=True,
            sqlite_healthy=sqlite_status.healthy,
            schema_version=sqlite_status.schema_version,
            run_count=int(counts["run_count"]),
            request_count=int(counts["request_count"]),
            stream_count=int(counts["stream_count"]),
            open_gap_count=int(counts["open_gap_count"]),
            stored_raw_artifact_count=int(counts["raw_count"]),
            canonical_batch_count=int(counts["batch_count"]),
            parquet_part_count=int(counts["part_count"]),
            canonical_row_count=int(counts["row_count"]),
            quarantine_artifact_count=int(counts["quarantine_count"]),
            active_writer_lease=self._store.get_writer_lease() is not None,
            latest_run=latest_run,
            latest_error=latest_error,
            dataset_policies=policies,
            streams=streams,
        )

    def verify(self) -> Phase2VerificationReport:
        """Run every offline integrity check without mutating filesystem or SQLite state."""

        checks = (
            self._verify_root(),
            self._verify_sqlite(),
            self._verify_raw_catalog(),
            self._verify_canonical_catalog(),
            self._verify_quarantine_catalog(),
            self._verify_staging(),
            self._verify_orphans(),
            self._verify_watermarks(),
            self._verify_retention(),
        )
        return Phase2VerificationReport(checked_at=self._now(), checks=checks)

    def _verify_root(self) -> DiagnosticCheck:
        try:
            self._data_root.validate(expected_root_id=self._root_id)
        except PrivateDataRootError:
            return _check(
                "ROOT_SENTINEL",
                checked_count=1,
                issue_count=1,
                issue_codes=("ROOT_VALIDATION_FAILED",),
            )
        return _check("ROOT_SENTINEL", checked_count=1)

    def _verify_sqlite(self) -> DiagnosticCheck:
        issues: list[str] = []
        issue_count = 0
        try:
            status = self._store.diagnostics()
        except (PrivateDataRootError, sqlite3.DatabaseError, RuntimeError):
            return _check(
                "SQLITE_INTEGRITY",
                checked_count=1,
                issue_count=1,
                issue_codes=("SQLITE_DIAGNOSTICS_FAILED",),
            )
        if status.integrity_messages != ("ok",):
            issues.append("INTEGRITY_CHECK_FAILED")
            issue_count += len(status.integrity_messages)
        if status.foreign_key_violations:
            issues.append("FOREIGN_KEY_VIOLATION")
            issue_count += status.foreign_key_violations
        if status.journal_mode != "wal":
            issues.append("JOURNAL_MODE_NOT_WAL")
            issue_count += 1
        if status.synchronous != 2:
            issues.append("SYNCHRONOUS_NOT_FULL")
            issue_count += 1
        if status.schema_version != LATEST_SCHEMA_VERSION:
            issues.append("SCHEMA_VERSION_MISMATCH")
            issue_count += 1
        return _check(
            "SQLITE_INTEGRITY",
            checked_count=1,
            issue_count=issue_count,
            issue_codes=issues,
        )

    def _verify_raw_catalog(self) -> DiagnosticCheck:
        issues: list[str] = []
        issue_count = 0
        checked_count = 0
        try:
            with self._store.read_only_connection() as connection:
                rows = connection.execute(
                    """
                    SELECT artifact.*, spec.provider, spec.dataset, spec.request_spec_hash,
                           manifest.manifest_content_sha256,
                           manifest.manifest_byte_count,
                           manifest.manifest_schema_version,
                           (SELECT count(*) FROM attempt_artifact_observations AS observed
                            WHERE observed.artifact_id = artifact.artifact_id)
                               AS attempt_reference_count,
                           (SELECT count(*) FROM acquisition_artifacts AS acquired
                            WHERE acquired.artifact_id = artifact.artifact_id)
                               AS acquisition_reference_count,
                           (SELECT count(*) FROM raw_replay_provenance AS replay
                            WHERE replay.artifact_id = artifact.artifact_id)
                               AS replay_reference_count,
                           (SELECT count(*) FROM batch_context_artifacts AS context_artifact
                            WHERE context_artifact.artifact_id = artifact.artifact_id)
                               AS canonical_context_reference_count
                    FROM raw_artifacts AS artifact
                    JOIN request_specs AS spec
                      ON spec.request_spec_id = artifact.request_spec_id
                    LEFT JOIN raw_artifact_manifests AS manifest
                      ON manifest.artifact_id = artifact.artifact_id
                    WHERE artifact.state <> 'PURGED'
                    ORDER BY artifact.artifact_id
                    """
                ).fetchall()
            checked_count = len(rows)
            for row in rows:
                row_issues = self._verify_one_raw(row)
                if row_issues:
                    issue_count += 1
                    issues.extend(row_issues)
        except (PrivateDataRootError, PublicationError, sqlite3.DatabaseError, ValueError):
            issue_count += 1
            issues.append("RAW_CATALOG_CHECK_FAILED")
        return _check(
            "RAW_CATALOG_CONTENT",
            checked_count=checked_count,
            issue_count=issue_count,
            issue_codes=issues,
        )

    def _verify_one_raw(self, row: sqlite3.Row) -> tuple[str, ...]:
        issues: list[str] = []
        try:
            expected_directory = raw_artifact_relative_directory(
                str(row["provider"]), str(row["dataset"]), str(row["artifact_id"])
            )
            payload_relative = PurePosixPath(str(row["relative_path"]))
            manifest_relative = PurePosixPath(str(row["manifest_relative_path"]))
            if (
                payload_relative != expected_directory / "payload.bin"
                or manifest_relative != expected_directory / "manifest.json"
            ):
                return ("RAW_CATALOG_PATH_MISMATCH",)
            directory = _managed_path(self._data_root, self._root_id, expected_directory)
            manifest = verify_raw_artifact_directory(
                directory,
                expected_provider=str(row["provider"]),
                expected_dataset=str(row["dataset"]),
            )
            if not self._raw_manifest_matches_row(manifest, row):
                issues.append("RAW_MANIFEST_CATALOG_MISMATCH")
            if row["manifest_content_sha256"] is None:
                issues.append("RAW_MANIFEST_CATALOG_MISSING")
            else:
                manifest_hash, manifest_bytes = file_integrity(directory / "manifest.json")
                if (
                    manifest_hash != str(row["manifest_content_sha256"])
                    or manifest_bytes != int(row["manifest_byte_count"])
                    or int(row["manifest_schema_version"]) != manifest.schema_version
                ):
                    issues.append("RAW_MANIFEST_CHECKSUM_MISMATCH")
            state = str(row["state"])
            if state == "PRESENT":
                # A crash after immutable rename but before catalog commit can
                # leave authorized, byte-verified raw evidence without retrieval
                # provenance.  It is retained and purge-visible, but may never
                # support an attempt, acquisition, replay, canonical batch, or
                # coverage proof unless a later exact response adopts it.
                reference_count = sum(
                    int(row[column])
                    for column in (
                        "attempt_reference_count",
                        "acquisition_reference_count",
                        "replay_reference_count",
                        "canonical_context_reference_count",
                    )
                )
                if reference_count:
                    issues.append("RAW_PRESENT_REFERENCED")
            elif state != "VERIFIED":
                issues.append("RAW_NOT_VERIFIED")
        except (
            OSError,
            PrivateDataRootError,
            PublicationError,
            ValueError,
        ):
            issues.append("RAW_FILE_OR_MANIFEST_INVALID")
        return tuple(issues)

    @staticmethod
    def _raw_manifest_matches_row(manifest: RawArtifactManifest, row: sqlite3.Row) -> bool:
        identity = manifest.identity
        return (
            manifest.artifact_id == str(row["artifact_id"])
            and identity.request_spec_hash == str(row["request_spec_hash"])
            and identity.page_ordinal == int(row["page_ordinal"])
            and identity.page_relation_hash == str(row["page_relation_hash"])
            and identity.content_sha256 == str(row["content_sha256"])
            and identity.byte_count == int(row["byte_count"])
            and identity.media_type == str(row["media_type"])
            and identity.content_encoding == str(row["content_encoding"])
        )

    def _verify_canonical_catalog(self) -> DiagnosticCheck:
        issues: list[str] = []
        issue_count = 0
        checked_count = 0
        try:
            with self._store.read_only_connection() as connection:
                rows = connection.execute(
                    """
                    SELECT batch.*, spec.provider, spec.dataset,
                           manifest.manifest_content_sha256,
                           manifest.manifest_byte_count,
                           manifest.manifest_schema_version
                    FROM canonical_batches AS batch
                    JOIN batch_contexts AS context
                      ON context.batch_context_id = batch.batch_context_id
                    JOIN request_specs AS spec
                      ON spec.request_spec_id = context.request_spec_id
                    LEFT JOIN canonical_batch_manifests AS manifest
                      ON manifest.canonical_batch_id = batch.canonical_batch_id
                    WHERE batch.state <> 'PURGED'
                    ORDER BY batch.canonical_batch_id
                    """
                ).fetchall()
                checked_count = len(rows)
                file_rows = connection.execute(
                    """
                    SELECT * FROM canonical_files
                    ORDER BY canonical_batch_id, file_ordinal
                    """
                ).fetchall()
            files_by_batch: dict[str, list[sqlite3.Row]] = {}
            for file_row in file_rows:
                files_by_batch.setdefault(str(file_row["canonical_batch_id"]), []).append(file_row)
            for row in rows:
                row_issues = self._verify_one_canonical(
                    row, files_by_batch.get(str(row["canonical_batch_id"]), [])
                )
                if row_issues:
                    issue_count += 1
                    issues.extend(row_issues)
        except (PrivateDataRootError, PublicationError, sqlite3.DatabaseError, ValueError):
            issue_count += 1
            issues.append("CANONICAL_CATALOG_CHECK_FAILED")
        return _check(
            "CANONICAL_CATALOG_CONTENT",
            checked_count=checked_count,
            issue_count=issue_count,
            issue_codes=issues,
        )

    def _verify_one_canonical(
        self, row: sqlite3.Row, file_rows: Sequence[sqlite3.Row]
    ) -> tuple[str, ...]:
        issues: list[str] = []
        try:
            expected_directory = _canonical_relative_directory(
                str(row["provider"]), str(row["dataset"]), str(row["canonical_batch_id"])
            )
            if (
                PurePosixPath(str(row["relative_path"])) != expected_directory
                or PurePosixPath(str(row["manifest_relative_path"]))
                != expected_directory / "manifest.json"
            ):
                return ("CANONICAL_CATALOG_PATH_MISMATCH",)
            directory = _managed_path(self._data_root, self._root_id, expected_directory)
            manifest = verify_canonical_batch_directory(
                directory,
                data_root=self._data_root,
                root_id=self._root_id,
                expected_provider=str(row["provider"]),
                expected_dataset=str(row["dataset"]),
                expected_batch_id=str(row["canonical_batch_id"]),
            )
            if manifest.row_count != int(row["row_count"]):
                issues.append("CANONICAL_ROW_COUNT_MISMATCH")
            if not self._canonical_files_match(expected_directory, manifest, file_rows):
                issues.append("CANONICAL_FILE_CATALOG_MISMATCH")
            if row["manifest_content_sha256"] is None:
                issues.append("CANONICAL_MANIFEST_CATALOG_MISSING")
            else:
                manifest_hash, manifest_bytes = file_integrity(directory / "manifest.json")
                if (
                    manifest_hash != str(row["manifest_content_sha256"])
                    or manifest_bytes != int(row["manifest_byte_count"])
                    or int(row["manifest_schema_version"]) != manifest.schema_version
                ):
                    issues.append("CANONICAL_MANIFEST_CHECKSUM_MISMATCH")
            if str(row["state"]) != "VERIFIED" or row["verified_at"] is None:
                issues.append("CANONICAL_NOT_VERIFIED")
        except (OSError, PrivateDataRootError, PublicationError, ValueError):
            issues.append("CANONICAL_FILE_OR_MANIFEST_INVALID")
        return tuple(issues)

    @staticmethod
    def _canonical_files_match(
        directory: PurePosixPath,
        manifest: CanonicalBatchManifest,
        rows: Sequence[sqlite3.Row],
    ) -> bool:
        if tuple(int(row["file_ordinal"]) for row in rows) != tuple(range(len(rows))):
            return False
        actual = tuple(
            (
                str(row["relative_path"]),
                str(row["content_sha256"]),
                int(row["byte_count"]),
                int(row["row_count"]),
                str(row["schema_fingerprint"]),
            )
            for row in rows
        )
        expected = tuple(
            (
                (directory / file.relative_path).as_posix(),
                file.sha256,
                file.byte_count,
                file.row_count,
                file.schema_sha256,
            )
            for file in manifest.files
        )
        return actual == expected

    def _verify_quarantine_catalog(self) -> DiagnosticCheck:
        """Verify metadata-only findings without returning validation codes or private paths."""

        issues: list[str] = []
        issue_count = 0
        checked_count = 0
        try:
            with self._store.read_only_connection() as connection:
                rows = connection.execute(
                    """
                    SELECT artifact.*, request.request_spec_hash,
                           status.status AS policy_status,
                           status.unavailable_at,
                           snapshot.provider AS snapshot_provider,
                           snapshot.dataset AS snapshot_dataset
                    FROM quarantine_artifacts AS artifact
                    JOIN request_specs AS request
                      ON request.request_spec_id = artifact.request_spec_id
                    LEFT JOIN dataset_policy_status AS status
                      ON status.provider = artifact.provider
                     AND status.dataset = artifact.dataset
                    JOIN policy_snapshots AS snapshot
                      ON snapshot.policy_snapshot_id = artifact.policy_snapshot_id
                    WHERE artifact.state <> 'PURGED'
                    ORDER BY artifact.quarantine_artifact_id
                    """
                ).fetchall()
            checked_count = len(rows)
            for row in rows:
                row_issues = self._verify_one_quarantine(row)
                if row_issues:
                    issue_count += 1
                    issues.extend(row_issues)
        except (OSError, PrivateDataRootError, PublicationError, sqlite3.DatabaseError, ValueError):
            issue_count += 1
            issues.append("QUARANTINE_CATALOG_CHECK_FAILED")
        return _check(
            "QUARANTINE_CATALOG_CONTENT",
            checked_count=checked_count,
            issue_count=issue_count,
            issue_codes=issues,
        )

    def _verify_one_quarantine(self, row: sqlite3.Row) -> tuple[str, ...]:
        issues: list[str] = []
        try:
            expected_directory = quarantine_artifact_relative_directory(
                str(row["provider"]),
                str(row["dataset"]),
                str(row["quarantine_artifact_id"]),
            )
            if PurePosixPath(str(row["relative_path"])) != expected_directory:
                return ("QUARANTINE_CATALOG_PATH_MISMATCH",)
            directory = _managed_path(self._data_root, self._root_id, expected_directory)
            manifest = verify_quarantine_artifact_directory(
                directory,
                data_root=self._data_root,
                root_id=self._root_id,
            )
            if not self._quarantine_manifest_matches_row(manifest, row):
                issues.append("QUARANTINE_MANIFEST_CATALOG_MISMATCH")
            manifest_hash, manifest_bytes = file_integrity(directory / "manifest.json")
            if manifest_hash != str(row["manifest_content_sha256"]) or manifest_bytes != int(
                row["manifest_byte_count"]
            ):
                issues.append("QUARANTINE_MANIFEST_CHECKSUM_MISMATCH")
            if str(row["state"]) != "VERIFIED" or row["invalidated_at"] is not None:
                issues.append("QUARANTINE_NOT_VERIFIED")
            if (
                str(row["policy_status"]) != DatasetPolicyStatus.ACTIVE.value
                or row["unavailable_at"] is not None
                or (str(row["snapshot_provider"]), str(row["snapshot_dataset"]))
                != (str(row["provider"]), str(row["dataset"]))
            ):
                issues.append("QUARANTINE_POLICY_INVALID")
            policy = self._catalog.lookup(str(row["provider"]), str(row["dataset"]))
            if not policy.normalized.quarantine_allowed:
                issues.append("QUARANTINE_POLICY_DENIED")
        except (OSError, DatasetPolicyDenied, PrivateDataRootError, PublicationError, ValueError):
            issues.append("QUARANTINE_FILE_OR_MANIFEST_INVALID")
        return tuple(issues)

    @staticmethod
    def _quarantine_manifest_matches_row(
        manifest: QuarantineArtifactManifest,
        row: sqlite3.Row,
    ) -> bool:
        return (
            manifest.quarantine_artifact_id == str(row["quarantine_artifact_id"])
            and manifest.provider == str(row["provider"])
            and manifest.dataset == str(row["dataset"])
            and manifest.request_specification.request_spec_id == str(row["request_spec_id"])
            and manifest.request_specification.request_spec_hash == str(row["request_spec_hash"])
            and manifest.batch_context.batch_context_id == str(row["batch_context_id"])
        )

    def _verify_staging(self) -> DiagnosticCheck:
        try:
            inspections = PublicationRecoveryInspector(self._data_root).inspect_staging()
        except (OSError, PrivateDataRootError, PublicationError, ValueError):
            return _check(
                "STAGING_STATE",
                checked_count=0,
                issue_count=1,
                issue_codes=("STAGING_INSPECTION_FAILED",),
            )
        quarantine_count = 0
        quarantine_invalid = False
        try:
            quarantine_staging = _managed_path(
                self._data_root, self._root_id, "staging/quarantine-artifacts"
            )
            if quarantine_staging.exists():
                candidates = tuple(quarantine_staging.iterdir())
                quarantine_count = len(candidates)
                for candidate in candidates:
                    relative = candidate.relative_to(self._data_root.root)
                    checked = _managed_path(self._data_root, self._root_id, relative)
                    if checked != candidate or not candidate.is_dir():
                        quarantine_invalid = True
                        break
                    tuple(iter_safe_regular_files(candidate))
        except (OSError, PrivateDataRootError, PublicationError, ValueError):
            quarantine_invalid = True
            quarantine_count = max(1, quarantine_count)
        transport_count = 0
        transport_invalid = False
        try:
            transport_inspections = TransportSpoolStore(
                self._data_root
            ).inspect_transient_attempts()
            transport_count = len(transport_inspections)
            transport_invalid = any(
                inspection.state is TransportSpoolInspectionState.INVALID
                for inspection in transport_inspections
            )
        except (
            OSError,
            PrivateDataRootError,
            PublicationError,
            TransportSpoolIntegrityError,
            ValueError,
        ):
            transport_invalid = True
            transport_count = 1
        if not inspections and quarantine_count == 0 and transport_count == 0:
            return _check("STAGING_STATE", checked_count=0)
        invalid = sum(
            inspection.state is RecoveryInspectionState.INVALID for inspection in inspections
        )
        issue_codes = {"STAGING_RECOVERY_REQUIRED"}
        if invalid:
            issue_codes.add("INVALID_STAGING_ENTRY")
        if quarantine_count:
            issue_codes.add("QUARANTINE_STAGING_RECOVERY_REQUIRED")
        if quarantine_invalid:
            issue_codes.add("INVALID_QUARANTINE_STAGING_ENTRY")
        if transport_count:
            issue_codes.add("TRANSPORT_STAGING_RECOVERY_REQUIRED")
        if transport_invalid:
            issue_codes.add("INVALID_TRANSPORT_STAGING_ENTRY")
        return _check(
            "STAGING_STATE",
            checked_count=len(inspections) + quarantine_count + transport_count,
            issue_count=len(inspections) + quarantine_count + transport_count,
            issue_codes=issue_codes,
            warning=invalid == 0 and not quarantine_invalid and not transport_invalid,
        )

    def _verify_orphans(self) -> DiagnosticCheck:
        issues: list[str] = []
        warnings: list[str] = []
        issue_count = 0
        checked_count = 0
        try:
            raw_physical, raw_layout_issues = self._physical_publications("raw", _RAW_DIRECTORY)
            canonical_physical, canonical_layout_issues = self._physical_publications(
                "normalized", _CANONICAL_DIRECTORY
            )
            quarantine_physical, quarantine_layout_issues = self._physical_publications(
                "quarantine", _QUARANTINE_DIRECTORY
            )
            issues.extend(raw_layout_issues)
            issues.extend(canonical_layout_issues)
            issues.extend(quarantine_layout_issues)
            issue_count += (
                len(raw_layout_issues)
                + len(canonical_layout_issues)
                + len(quarantine_layout_issues)
            )
            with self._store.read_only_connection() as connection:
                raw_catalog = {
                    PurePosixPath(str(row[0])).parent.as_posix()
                    for row in connection.execute(
                        "SELECT manifest_relative_path FROM raw_artifacts WHERE state <> 'PURGED'"
                    ).fetchall()
                }
                canonical_catalog = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT relative_path FROM canonical_batches WHERE state <> 'PURGED'"
                    ).fetchall()
                }
                quarantine_catalog = {
                    str(row[0])
                    for row in connection.execute(
                        """
                        SELECT relative_path FROM quarantine_artifacts
                        WHERE state <> 'PURGED'
                        """
                    ).fetchall()
                }
            checked_count = len(raw_physical) + len(canonical_physical) + len(quarantine_physical)
            for relative in sorted(raw_physical - raw_catalog):
                issue_count += 1
                directory = _managed_path(self._data_root, self._root_id, relative)
                try:
                    verify_raw_artifact_directory(directory)
                except (OSError, PublicationError, ValueError):
                    issues.append("INVALID_UNCATALOGED_RAW")
                else:
                    warnings.append("UNCATALOGED_RAW_PUBLICATION")
            for relative in sorted(canonical_physical - canonical_catalog):
                issue_count += 1
                directory = _managed_path(self._data_root, self._root_id, relative)
                try:
                    verify_canonical_batch_directory(
                        directory,
                        data_root=self._data_root,
                        root_id=self._root_id,
                    )
                except (OSError, PublicationError, ValueError):
                    issues.append("INVALID_UNCATALOGED_CANONICAL")
                else:
                    warnings.append("UNCATALOGED_CANONICAL_PUBLICATION")
            for relative in sorted(quarantine_physical - quarantine_catalog):
                issue_count += 1
                directory = _managed_path(self._data_root, self._root_id, relative)
                try:
                    verify_quarantine_artifact_directory(
                        directory,
                        data_root=self._data_root,
                        root_id=self._root_id,
                    )
                except (OSError, PublicationError, ValueError):
                    issues.append("INVALID_UNCATALOGED_QUARANTINE")
                else:
                    warnings.append("UNCATALOGED_QUARANTINE_PUBLICATION")
            missing = (
                (raw_catalog - raw_physical)
                | (canonical_catalog - canonical_physical)
                | (quarantine_catalog - quarantine_physical)
            )
            if missing:
                issue_count += len(missing)
                issues.append("CATALOGED_PUBLICATION_ABSENT")
        except (OSError, PrivateDataRootError, PublicationError, sqlite3.DatabaseError, ValueError):
            issue_count += 1
            issues.append("ORPHAN_INSPECTION_FAILED")
        issue_codes = (*issues, *warnings)
        return _check(
            "PUBLISHED_ORPHANS",
            checked_count=checked_count,
            issue_count=issue_count,
            issue_codes=issue_codes,
            warning=not issues,
        )

    def _physical_publications(
        self, namespace: str, pattern: re.Pattern[str]
    ) -> tuple[set[str], tuple[str, ...]]:
        root = _managed_path(self._data_root, self._root_id, namespace)
        if not root.exists():
            return set(), ()
        files = tuple(iter_safe_regular_files(root))
        directories: set[str] = set()
        issues: list[str] = []
        for file in files:
            if file.name != "manifest.json":
                continue
            relative = file.parent.relative_to(self._data_root.root).as_posix()
            if pattern.fullmatch(relative) is None:
                issues.append("UNEXPECTED_PUBLICATION_LAYOUT")
            else:
                directories.add(relative)
        # A payload or Parquet tree without its completion manifest is not a
        # publication and must never disappear from the orphan audit merely
        # because there is no manifest to discover it from.
        publication_roots = tuple(
            self._data_root.root.joinpath(*PurePosixPath(relative).parts)
            for relative in directories
        )
        for file in files:
            if not any(
                file == candidate or candidate in file.parents for candidate in publication_roots
            ):
                issues.append("UNEXPECTED_PUBLICATION_LAYOUT")
        return directories, tuple(issues)

    def _verify_watermarks(self) -> DiagnosticCheck:
        issues: list[str] = []
        issue_count = 0
        checked_count = 0
        try:
            with self._store.read_only_connection() as connection:
                watermarks = connection.execute(
                    """
                    SELECT watermark.*, stream.timeframe, calendar.state AS calendar_state,
                           batch.state AS batch_state,
                           active.status AS policy_status,
                           active.policy_snapshot_id AS active_policy_snapshot_id,
                           active.retention_mode AS active_retention_mode,
                           active.expires_at AS active_expires_at,
                           active.unavailable_at AS active_unavailable_at
                    FROM watermarks AS watermark
                    JOIN stream_keys AS stream ON stream.stream_id = watermark.stream_id
                    JOIN calendar_snapshots AS calendar
                      ON calendar.calendar_snapshot_id = watermark.calendar_snapshot_id
                    JOIN canonical_batches AS batch
                      ON batch.canonical_batch_id = watermark.last_batch_id
                    JOIN policy_snapshots AS policy
                      ON policy.policy_snapshot_id = watermark.policy_snapshot_id
                    LEFT JOIN dataset_policy_status AS active
                      ON active.provider = policy.provider AND active.dataset = policy.dataset
                    ORDER BY watermark.stream_id
                    """
                ).fetchall()
                checked_count = len(watermarks)
                for watermark in watermarks:
                    row_issues = self._verify_one_watermark(connection, watermark)
                    if row_issues:
                        issue_count += 1
                        issues.extend(row_issues)
        except (sqlite3.DatabaseError, ValueError):
            issue_count += 1
            issues.append("WATERMARK_CHECK_FAILED")
        return _check(
            "WATERMARK_COVERAGE",
            checked_count=checked_count,
            issue_count=issue_count,
            issue_codes=issues,
        )

    def _verify_one_watermark(
        self, connection: sqlite3.Connection, watermark: sqlite3.Row
    ) -> tuple[str, ...]:
        state = str(watermark["verification_state"])
        if state != "VERIFIED":
            return () if watermark["invalidated_at"] is not None else ("INVALIDATION_MISSING",)
        issues: list[str] = []
        start = _parse_utc(watermark["coverage_start"])
        frontier = _parse_utc(watermark["exclusive_frontier"])
        if frontier <= start or watermark["invalidated_at"] is not None:
            issues.append("VERIFIED_WATERMARK_STATE_INVALID")
        if (
            str(watermark["calendar_state"]) != "CURRENT"
            or str(watermark["batch_state"]) != "VERIFIED"
        ):
            issues.append("WATERMARK_SUPPORT_NOT_CURRENT")
        if not self._watermark_policy_current(watermark):
            issues.append("WATERMARK_POLICY_INVALID")
        segments = connection.execute(
            """
            SELECT coverage.*,
                   EXISTS(
                       SELECT 1 FROM coverage_request_proofs AS proof
                       WHERE proof.coverage_id = coverage.coverage_id
                         AND proof.terminal_page_verified = 1
                         AND proof.canonical_batch_verified = 1
                         AND proof.relational_provenance_verified = 1
                   ) AS exact_proof
            FROM coverage_segments AS coverage
            JOIN canonical_batches AS batch
              ON batch.canonical_batch_id = coverage.canonical_batch_id
            WHERE coverage.stream_id = ?
              AND coverage.policy_snapshot_id = ?
              AND coverage.calendar_snapshot_id = ?
              AND coverage.verification_state = 'VERIFIED'
              AND coverage.retained = 1
              AND coverage.invalidated_at IS NULL
              AND batch.state = 'VERIFIED'
            ORDER BY coverage.interval_start, coverage.interval_end
            """,
            (
                str(watermark["stream_id"]),
                str(watermark["policy_snapshot_id"]),
                str(watermark["calendar_snapshot_id"]),
            ),
        ).fetchall()
        supporting_segments = tuple(segment for segment in segments if bool(segment["exact_proof"]))
        if not supporting_segments:
            issues.append("WATERMARK_HAS_NO_ELIGIBLE_SUPPORT")
            issues.append("WATERMARK_FRONTIER_NOT_CONTIGUOUS")
            return tuple(issues)
        origins = tuple(
            sorted({_parse_utc(segment["coverage_start"]) for segment in supporting_segments})
        )
        authoritative_start = origins[0]
        if len(origins) != 1:
            issues.append("COVERAGE_ORIGIN_INCONSISTENT")
        if start != authoritative_start:
            issues.append("WATERMARK_COVERAGE_START_MISMATCH")
        # Reconstruct over every currently retained proof, not merely the
        # watermark's claimed prefix.  Including the claimed frontier lets an
        # over-advanced row fail at its first unsupported calendar slot; using
        # the furthest retained segment exposes a row tampered backward.
        domain_end = max(
            frontier,
            *(_parse_utc(segment["interval_end"]) for segment in supporting_segments),
        )
        slots, calendar_valid = self._watermark_slots(
            connection,
            watermark,
            authoritative_start,
            domain_end,
        )
        if not calendar_valid:
            issues.append("CALENDAR_SLOT_CONTRACT_INVALID")
        if not slots:
            issues.append("WATERMARK_HAS_NO_ELIGIBLE_SUPPORT")
            return tuple(issues)
        active_gaps = connection.execute(
            """
            SELECT gap_id, interval_start, interval_end FROM gaps
            WHERE stream_id = ? AND blocking = 1 AND status IN ('OPEN', 'REPAIRING')
            ORDER BY interval_start, interval_end, gap_id
            """,
            (str(watermark["stream_id"]),),
        ).fetchall()
        parsed_gaps = tuple(
            (_parse_utc(gap["interval_start"]), _parse_utc(gap["interval_end"]))
            for gap in active_gaps
        )
        if int(watermark["blocking_gap_count"]) != len(active_gaps):
            issues.append("BLOCKING_GAP_COUNT_MISMATCH")
        covered: list[tuple[datetime, datetime, date, set[str]]] = []
        for slot_start, slot_end, session_date in slots:
            if any(
                gap_start < slot_end and gap_end > slot_start for gap_start, gap_end in parsed_gaps
            ):
                break
            supports = {
                str(segment["canonical_batch_id"])
                for segment in supporting_segments
                if _parse_utc(segment["interval_start"]) <= slot_start
                and _parse_utc(segment["interval_end"]) >= slot_end
            }
            if not supports:
                break
            covered.append((slot_start, slot_end, session_date, supports))
        if not covered:
            issues.append("WATERMARK_FRONTIER_NOT_CONTIGUOUS")
        else:
            reconstructed_frontier = (
                domain_end if len(covered) == len(slots) else slots[len(covered)][0]
            )
            if frontier != reconstructed_frontier:
                issues.append("WATERMARK_FRONTIER_MISMATCH")
            if str(watermark["last_verified_session"]) != covered[-1][2].isoformat():
                issues.append("LAST_VERIFIED_SESSION_MISMATCH")
            if str(watermark["last_batch_id"]) not in covered[-1][3]:
                issues.append("LAST_BATCH_DOES_NOT_SUPPORT_FRONTIER")
        if any(
            gap_start < frontier and gap_end > authoritative_start
            for gap_start, gap_end in parsed_gaps
        ):
            issues.append("BLOCKING_GAP_BEFORE_FRONTIER")
        return tuple(issues)

    def _watermark_policy_current(self, row: sqlite3.Row) -> bool:
        if str(row["policy_status"]) != "ACTIVE" or str(row["active_policy_snapshot_id"]) != str(
            row["policy_snapshot_id"]
        ):
            return False
        try:
            mode = RetentionMode(str(row["active_retention_mode"]))
        except ValueError:
            return False
        if mode not in _DURABLE_MODES or row["active_unavailable_at"] is not None:
            return False
        return not (
            row["active_expires_at"] is not None
            and _parse_utc(row["active_expires_at"]) <= self._now()
        )

    @staticmethod
    def _watermark_slots(
        connection: sqlite3.Connection,
        watermark: sqlite3.Row,
        start: datetime,
        frontier: datetime,
    ) -> tuple[tuple[tuple[datetime, datetime, date], ...], bool]:
        sessions = connection.execute(
            """
            SELECT session_date, open_at, close_at, expected_1d_count, expected_5m_count
            FROM calendar_sessions WHERE calendar_snapshot_id = ?
            ORDER BY session_date
            """,
            (str(watermark["calendar_snapshot_id"]),),
        ).fetchall()
        timeframe = str(watermark["timeframe"])
        valid = timeframe in {"1d", "5m"}
        slots: list[tuple[datetime, datetime, date]] = []
        for session in sessions:
            opened = _parse_utc(session["open_at"])
            closed = _parse_utc(session["close_at"])
            session_date = date.fromisoformat(str(session["session_date"]))
            candidates: list[tuple[datetime, datetime]] = []
            if timeframe == "1d":
                if int(session["expected_1d_count"]) != 1:
                    valid = False
                candidates.append((opened, closed))
            elif timeframe == "5m":
                expected_count = int(session["expected_5m_count"])
                if opened + timedelta(minutes=5 * expected_count) != closed:
                    valid = False
                candidates.extend(
                    (
                        opened + timedelta(minutes=5 * index),
                        opened + timedelta(minutes=5 * (index + 1)),
                    )
                    for index in range(expected_count)
                )
            for slot_start, slot_end in candidates:
                if slot_start >= start and slot_end <= frontier:
                    slots.append((slot_start, slot_end, session_date))
        return tuple(slots), valid

    def _verify_retention(self) -> DiagnosticCheck:
        issues: list[str] = []
        issue_count = 0
        checked_count = 0
        try:
            with self._store.read_only_connection() as connection:
                status_rows = connection.execute(
                    """
                    SELECT status.*, snapshot.policy_id, snapshot.revision,
                           snapshot.policy_hash, snapshot.entitlement_active
                    FROM dataset_policy_status AS status
                    LEFT JOIN policy_snapshots AS snapshot
                      ON snapshot.policy_snapshot_id = status.policy_snapshot_id
                    ORDER BY status.provider, status.dataset
                    """
                ).fetchall()
                status_by_key = {
                    (str(row["provider"]), str(row["dataset"])): row for row in status_rows
                }
                for row in status_rows:
                    checked_count += 1
                    row_issues = self._check_policy_status(row)
                    if row_issues:
                        issue_count += 1
                        issues.extend(row_issues)
                retained = self._retained_dataset_layers(connection)
                for provider, dataset, layer in retained:
                    checked_count += 1
                    row_issues = self._check_retained_layer(
                        provider,
                        dataset,
                        layer,
                        status_by_key.get((provider, dataset)),
                    )
                    if row_issues:
                        issue_count += 1
                        issues.extend(row_issues)
        except (sqlite3.DatabaseError, ValueError):
            issue_count += 1
            issues.append("RETENTION_CHECK_FAILED")
        return _check(
            "RETENTION_POLICY",
            checked_count=checked_count,
            issue_count=issue_count,
            issue_codes=issues,
        )

    def _check_policy_status(self, row: sqlite3.Row) -> tuple[str, ...]:
        try:
            policy = self._catalog.lookup(str(row["provider"]), str(row["dataset"]))
        except DatasetPolicyDenied:
            return ("UNKNOWN_DATASET_POLICY",)
        if str(row["status"]) != DatasetPolicyStatus.ACTIVE.value:
            return ()
        if (
            policy.status is not DatasetPolicyStatus.ACTIVE
            or row["policy_snapshot_id"] is None
            or str(row["policy_id"]) != policy.policy_id
            or int(row["revision"]) != policy.revision
            or str(row["policy_hash"]) != policy.content_hash
            or str(row["retention_mode"]) != policy.mode.value
        ):
            return ("ACTIVE_POLICY_SNAPSHOT_MISMATCH",)
        return ()

    @staticmethod
    def _retained_dataset_layers(
        connection: sqlite3.Connection,
    ) -> set[tuple[str, str, RetentionLayer | str]]:
        values: set[tuple[str, str, RetentionLayer | str]] = set()
        for row in connection.execute(
            """
            SELECT DISTINCT spec.provider, spec.dataset
            FROM raw_artifacts AS artifact
            JOIN request_specs AS spec ON spec.request_spec_id = artifact.request_spec_id
            WHERE artifact.state <> 'PURGED'
            """
        ).fetchall():
            values.add((str(row[0]), str(row[1]), RetentionLayer.RAW))
        for row in connection.execute(
            """
            SELECT DISTINCT spec.provider, spec.dataset
            FROM canonical_batches AS batch
            JOIN batch_contexts AS context ON context.batch_context_id = batch.batch_context_id
            JOIN request_specs AS spec ON spec.request_spec_id = context.request_spec_id
            WHERE batch.state <> 'PURGED'
            """
        ).fetchall():
            values.add((str(row[0]), str(row[1]), RetentionLayer.NORMALIZED))
        for row in connection.execute(
            """
            SELECT DISTINCT provider, dataset FROM quarantine_artifacts
            WHERE state <> 'PURGED'
            """
        ).fetchall():
            values.add((str(row[0]), str(row[1]), RetentionLayer.NORMALIZED))
        for table in ("coverage_segments", "watermarks"):
            condition = (
                "record.retained = 1 AND record.verification_state = 'VERIFIED'"
                if table == "coverage_segments"
                else "record.verification_state = 'VERIFIED'"
            )
            for row in connection.execute(
                f"""
                SELECT DISTINCT stream.provider, stream.dataset
                FROM {table} AS record
                JOIN stream_keys AS stream ON stream.stream_id = record.stream_id
                WHERE {condition}
                """
            ).fetchall():
                values.add((str(row[0]), str(row[1]), "WATERMARK"))
        return values

    def _check_retained_layer(
        self,
        provider: str,
        dataset: str,
        layer: RetentionLayer | str,
        status: sqlite3.Row | None,
    ) -> tuple[str, ...]:
        try:
            policy = self._catalog.lookup(provider, dataset)
        except DatasetPolicyDenied:
            return ("RETAINED_UNKNOWN_DATASET",)
        if status is None or str(status["status"]) != DatasetPolicyStatus.ACTIVE.value:
            return ("RETAINED_DATASET_NOT_ACTIVE",)
        if status["unavailable_at"] is not None:
            return ("RETAINED_DATASET_UNAVAILABLE",)
        if status["expires_at"] is not None and _parse_utc(status["expires_at"]) <= self._now():
            return ("RETAINED_DATASET_EXPIRED",)
        mode = policy.layer(layer).mode if isinstance(layer, RetentionLayer) else policy.mode
        if mode not in _DURABLE_MODES:
            return ("RETENTION_LAYER_NOT_DURABLE",)
        if mode is RetentionMode.SUBSCRIPTION_BOUND and status["entitlement_active"] != 1:
            return ("SUBSCRIPTION_ENTITLEMENT_INACTIVE",)
        return self._check_policy_status(status)


__all__ = [
    "DatasetPolicySummary",
    "DiagnosticCheck",
    "DiagnosticStatus",
    "LatestErrorSummary",
    "LatestRunSummary",
    "OperationalStatusSnapshot",
    "Phase2OperationalDiagnostics",
    "Phase2VerificationReport",
    "StreamStatusSummary",
]
