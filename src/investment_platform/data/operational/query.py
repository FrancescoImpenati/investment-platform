"""Catalog-driven Phase 2 queries and same-provider revision selection.

SQLite decides which immutable Parquet files may be read.  The filesystem is
never globbed: every canonical and supporting raw target is taken from a
VERIFIED, current-policy catalog row and checksum-verified before DuckDB opens
it.  Semantic duplicates retain provenance but do not become extra revisions.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import stat
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Final
from uuid import UUID

import duckdb
import polars as pl

from investment_platform.data.ingestion.identity import (
    ObservationIdentity,
    PriceBarSemanticValue,
    ProcessingSignature,
    SemanticObservation,
    StreamKey,
)
from investment_platform.data.models import PriceBar
from investment_platform.data.operational.store import (
    OperationalStateError,
    OperationalStateStore,
    _parse_utc,
)
from investment_platform.data.retention import (
    DatasetPolicyDenied,
    DatasetRuntimeStatus,
    RetentionLayer,
    RetentionMode,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage.canonical_batches import (
    CanonicalParquetPart,
    CanonicalStreamOutcome,
    StreamPublicationOutcome,
)
from investment_platform.data.storage.market_bars import (
    PRICE_BAR_SCHEMA,
    BarQuery,
    empty_price_bar_frame,
    price_bars_to_frame,
)
from investment_platform.data_root import PrivateDataRoot
from investment_platform.runtime import RuntimeEnvironment

_CANONICAL_COLUMNS: Final = tuple(PRICE_BAR_SCHEMA)
_MINIMUM_TIME: Final = datetime.min.replace(tzinfo=UTC)


class CatalogBarQueryError(OperationalStateError):
    """Base error for fail-closed catalog-driven canonical queries."""


class CatalogBarQueryPolicyError(CatalogBarQueryError):
    """The current exact dataset policy does not authorize query visibility."""


class CatalogBarQueryIntegrityError(CatalogBarQueryError):
    """Catalog, filesystem, or canonical semantics failed verification."""


class CatalogRevisionView(StrEnum):
    """Queryable semantic views over immutable same-provider revisions."""

    CURRENT = "current"
    ALL_VERSIONS = "all_versions"


class CandidateBatchDisposition(StrEnum):
    """Semantic effect of a fully normalized candidate batch."""

    SEMANTIC_NO_OP = "semantic_no_op"
    BLOCKED = "blocked"
    NEW = "new"
    REVISION = "revision"
    MIXED = "mixed"


@dataclass(frozen=True, slots=True)
class RevisionProvenance:
    """One immutable raw/canonical path supporting a semantic revision."""

    canonical_batch_id: str
    raw_artifact_id: str
    raw_batch_id: UUID
    retrieved_at: datetime
    processing_signature_hash: str
    provider_revision_sequence: int | None = None
    provider_revision_at: datetime | None = None

    @property
    def order_key(self) -> tuple[object, ...]:
        """Apply trusted provider ordering first, then the approved stable fallback."""

        if self.provider_revision_sequence is not None:
            provider_rank = 2
            provider_value: object = self.provider_revision_sequence
        elif self.provider_revision_at is not None:
            provider_rank = 1
            provider_value = self.provider_revision_at
        else:
            provider_rank = 0
            provider_value = 0
        return (
            provider_rank,
            provider_value,
            self.provider_revision_at or _MINIMUM_TIME,
            self.retrieved_at,
            self.raw_artifact_id,
            self.canonical_batch_id,
        )


@dataclass(frozen=True, slots=True)
class CanonicalBarRevision:
    """One unique semantic value plus every retained provenance link for it."""

    observation_id: str
    stream_id: str
    value_fingerprint: str
    revision_number: int
    is_current: bool
    bar: PriceBar
    provenances: tuple[RevisionProvenance, ...]


@dataclass(frozen=True, slots=True)
class CatalogBarQueryResult:
    """Versioned observations returned only from query-visible catalog entries."""

    provider: str
    dataset: str
    revisions: tuple[CanonicalBarRevision, ...]
    canonical_file_count: int

    @property
    def current(self) -> tuple[CanonicalBarRevision, ...]:
        return tuple(value for value in self.revisions if value.is_current)

    def frame(
        self,
        view: CatalogRevisionView = CatalogRevisionView.CURRENT,
    ) -> pl.DataFrame:
        selected = self.current if view is CatalogRevisionView.CURRENT else self.revisions
        if not selected:
            return empty_price_bar_frame()
        return price_bars_to_frame(value.bar for value in selected)


@dataclass(frozen=True, slots=True)
class CandidateStreamComparison:
    stream_id: str
    semantic_duplicate_count: int
    new_observation_count: int
    revision_count: int


@dataclass(frozen=True, slots=True)
class SemanticDuplicateSlot:
    """One exact candidate slot already represented by the same semantic value."""

    observation_id: str
    value_fingerprint: str
    stream_id: str
    start: datetime
    end: datetime
    matching_canonical_batch_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CandidateBatchComparison:
    """Pre-publication semantic comparison against current policy-valid history."""

    disposition: CandidateBatchDisposition
    semantic_duplicate_count: int
    new_observation_count: int
    revision_count: int
    matching_canonical_batch_ids: tuple[str, ...]
    semantic_duplicate_slots: tuple[SemanticDuplicateSlot, ...]
    has_blocked_streams: bool
    streams: tuple[CandidateStreamComparison, ...]

    @property
    def publication_required(self) -> bool:
        return self.new_observation_count > 0 or self.revision_count > 0

    def apply_stream_counts(
        self,
        outcomes: Sequence[CanonicalStreamOutcome],
    ) -> tuple[CanonicalStreamOutcome, ...]:
        by_stream = {value.stream_id: value for value in self.streams}
        if set(by_stream) != {value.stream_id for value in outcomes}:
            raise CatalogBarQueryIntegrityError(
                "candidate comparison does not cover the exact stream outcomes"
            )
        return tuple(
            outcome.model_copy(
                update={
                    "semantic_duplicate_count": by_stream[
                        outcome.stream_id
                    ].semantic_duplicate_count,
                    "revision_count": by_stream[outcome.stream_id].revision_count,
                }
            )
            for outcome in outcomes
        )


@dataclass(frozen=True, slots=True)
class _ActivePolicyProof:
    provider: str
    dataset: str
    policy_snapshot_id: str
    policy_id: str
    policy_revision: int
    policy_hash: str
    retention_mode: str
    effective_at: str
    expires_at: str | None
    unavailable_at: str | None
    last_checked_at: str


@dataclass(frozen=True, slots=True)
class _CatalogFile:
    canonical_batch_id: str
    batch_context_id: str
    relative_path: str
    content_sha256: str
    byte_count: int
    manifest_relative_path: str
    manifest_sha256: str
    manifest_byte_count: int
    processing_signature: ProcessingSignature
    processing_signature_hash: str
    streams: tuple[StreamKey, ...]
    raw_by_batch_id: Mapping[str, tuple[str, datetime]]


@dataclass(frozen=True, slots=True)
class _ObservedCandidate:
    observation_id: str
    stream_id: str
    value_fingerprint: str
    bar: PriceBar
    provenance: RevisionProvenance


class CatalogBarQueryRepository:
    """Read verified canonical bars through exact SQLite catalog membership."""

    def __init__(
        self,
        store: OperationalStateStore,
        data_root: PrivateDataRoot,
        policy_enforcer: RetentionPolicyEnforcer,
        *,
        environment: RuntimeEnvironment,
    ) -> None:
        sentinel = data_root.validate(expected_root_id=UUID(store.root_id))
        if str(sentinel.root_id) != store.root_id:
            raise CatalogBarQueryIntegrityError("query root differs from operational state root")
        self._store = store
        self._data_root = data_root
        self._root_id = sentinel.root_id
        self._policy_enforcer = policy_enforcer
        self._environment = environment

    def query(
        self,
        provider: str,
        dataset: str,
        query: BarQuery | None = None,
    ) -> CatalogBarQueryResult:
        """Return semantic versions from explicit VERIFIED, policy-valid Parquet files."""

        self._require_exact_dataset_key(provider, dataset)
        policy_proof = self._authorize_current_policy(provider, dataset)
        files = self._catalog_files(provider, dataset, query or BarQuery())
        if not files:
            self._assert_policy_unchanged(policy_proof)
            return CatalogBarQueryResult(
                provider=provider,
                dataset=dataset,
                revisions=(),
                canonical_file_count=0,
            )

        verified_paths: dict[str, Path] = {}
        integrity_cache: set[tuple[str, str, int]] = set()
        for item in files:
            self._verify_managed_file(
                item.manifest_relative_path,
                expected_sha256=item.manifest_sha256,
                expected_bytes=item.manifest_byte_count,
                cache=integrity_cache,
            )
            verified_paths[item.relative_path] = self._verify_managed_file(
                item.relative_path,
                expected_sha256=item.content_sha256,
                expected_bytes=item.byte_count,
                cache=integrity_cache,
            )

        observed: list[_ObservedCandidate] = []
        with duckdb.connect(":memory:") as connection:
            connection.execute("SET TimeZone = 'UTC'")
            for item in files:
                path = verified_paths[item.relative_path]
                try:
                    relation = connection.read_parquet(str(path))
                    relation.create_view("_cataloged_price_bars", replace=True)
                    frame = connection.execute(
                        self._query_sql(query or BarQuery()),
                        self._query_parameters(query or BarQuery()),
                    ).pl()
                except (duckdb.Error, ValueError) as error:
                    raise CatalogBarQueryIntegrityError(
                        "cataloged canonical Parquet failed DuckDB reopening"
                    ) from error
                observed.extend(self._observed_rows(frame, item))

        # Detect replacement between pre-read checksum and completed analytical reopen.
        for item in files:
            self._verify_managed_file(
                item.relative_path,
                expected_sha256=item.content_sha256,
                expected_bytes=item.byte_count,
                cache=set(),
            )
        self._data_root.validate(expected_root_id=self._root_id)
        self._assert_policy_unchanged(policy_proof)
        revisions = self._semantic_revisions(observed)
        return CatalogBarQueryResult(
            provider=provider,
            dataset=dataset,
            revisions=revisions,
            canonical_file_count=len(files),
        )

    @staticmethod
    def _require_exact_dataset_key(provider: str, dataset: str) -> None:
        for label, value in (("provider", provider), ("dataset", dataset)):
            if (
                not value
                or value != value.casefold()
                or value != value.strip()
                or any(character.isspace() for character in value)
            ):
                raise CatalogBarQueryPolicyError(f"{label} must be the exact lowercase catalog key")

    def classify_candidate(
        self,
        *,
        provider: str,
        dataset: str,
        parts: Sequence[CanonicalParquetPart],
        stream_outcomes: Sequence[CanonicalStreamOutcome],
        processing_signature: ProcessingSignature,
    ) -> CandidateBatchComparison:
        """Compare a deterministic normalized candidate before filesystem publication."""

        publishable = tuple(value for value in stream_outcomes if value.row_count > 0)
        frames = tuple(part.frame for part in parts)
        if not publishable or not frames or sum(frame.height for frame in frames) == 0:
            raise CatalogBarQueryIntegrityError(
                "semantic comparison requires at least one publishable candidate row"
            )
        streams = tuple(value.stream for value in publishable)
        timeframes = {value.timeframe for value in streams}
        sessions = {value.session for value in streams}
        adjustments = {value.adjustment for value in streams}
        if len(timeframes) != 1 or len(sessions) != 1 or len(adjustments) != 1:
            raise CatalogBarQueryIntegrityError(
                "one candidate comparison requires uniform request series dimensions"
            )
        start = min(value.request_start for value in publishable)
        end = max(value.request_end for value in publishable)
        existing = self.query(
            provider,
            dataset,
            BarQuery(
                instrument_ids=tuple(value.instrument_id for value in streams),
                timeframe=next(iter(timeframes)),
                session=next(iter(sessions)),
                adjustment_state=next(iter(adjustments)),
                start=start,
                end=end,
            ),
        )
        fingerprints: dict[str, set[str]] = defaultdict(set)
        batches_by_pair: dict[tuple[str, str], set[str]] = defaultdict(set)
        for revision in existing.revisions:
            fingerprints[revision.observation_id].add(revision.value_fingerprint)
            batches_by_pair[(revision.observation_id, revision.value_fingerprint)].update(
                value.canonical_batch_id for value in revision.provenances
            )

        counts: dict[str, list[int]] = {outcome.stream_id: [0, 0, 0] for outcome in stream_outcomes}
        seen: set[str] = set()
        matching_batches: set[str] = set()
        duplicate_slots: list[SemanticDuplicateSlot] = []
        for frame in frames:
            for row in frame.iter_rows(named=True):
                bar = self._bar(row)
                stream = self._stream_for_bar(bar, streams)
                semantic = self._semantic_observation(bar, stream, processing_signature)
                observation_id = semantic.identity.observation_id
                if observation_id in seen:
                    raise CatalogBarQueryIntegrityError(
                        "candidate contains a duplicate canonical observation identity"
                    )
                seen.add(observation_id)
                prior = fingerprints.get(observation_id)
                stream_counts = counts[stream.stream_id]
                if prior is None:
                    stream_counts[1] += 1
                elif semantic.value_fingerprint in prior:
                    stream_counts[0] += 1
                    slot_batches = tuple(
                        sorted(batches_by_pair[(observation_id, semantic.value_fingerprint)])
                    )
                    matching_batches.update(slot_batches)
                    duplicate_slots.append(
                        SemanticDuplicateSlot(
                            observation_id=observation_id,
                            value_fingerprint=semantic.value_fingerprint,
                            stream_id=stream.stream_id,
                            start=bar.timestamp_start,
                            end=bar.timestamp_end,
                            matching_canonical_batch_ids=slot_batches,
                        )
                    )
                else:
                    stream_counts[2] += 1

        stream_results = tuple(
            CandidateStreamComparison(
                stream_id=outcome.stream_id,
                semantic_duplicate_count=counts[outcome.stream_id][0],
                new_observation_count=counts[outcome.stream_id][1],
                revision_count=counts[outcome.stream_id][2],
            )
            for outcome in sorted(stream_outcomes, key=lambda value: value.stream_id)
        )
        duplicates = sum(value.semantic_duplicate_count for value in stream_results)
        new = sum(value.new_observation_count for value in stream_results)
        revisions = sum(value.revision_count for value in stream_results)
        has_blocked_streams = any(
            value.outcome is StreamPublicationOutcome.BLOCKED for value in stream_outcomes
        )
        if new and revisions:
            disposition = CandidateBatchDisposition.MIXED
        elif revisions:
            disposition = CandidateBatchDisposition.REVISION
        elif new:
            disposition = CandidateBatchDisposition.NEW
        elif has_blocked_streams:
            disposition = CandidateBatchDisposition.BLOCKED
        else:
            disposition = CandidateBatchDisposition.SEMANTIC_NO_OP
        return CandidateBatchComparison(
            disposition=disposition,
            semantic_duplicate_count=duplicates,
            new_observation_count=new,
            revision_count=revisions,
            matching_canonical_batch_ids=tuple(sorted(matching_batches)),
            semantic_duplicate_slots=tuple(
                sorted(
                    duplicate_slots,
                    key=lambda value: (value.start, value.stream_id, value.observation_id),
                )
            ),
            has_blocked_streams=has_blocked_streams,
            streams=stream_results,
        )

    def _authorize_current_policy(self, provider: str, dataset: str) -> _ActivePolicyProof:
        with self._store.read_only_connection() as connection:
            row = connection.execute(
                """
                SELECT status.*, snapshot.policy_id, snapshot.revision,
                       snapshot.policy_hash, snapshot.retention_mode AS snapshot_mode,
                       snapshot.entitlement_active
                FROM dataset_policy_status AS status
                JOIN policy_snapshots AS snapshot
                  ON snapshot.policy_snapshot_id = status.policy_snapshot_id
                 AND snapshot.provider = status.provider
                 AND snapshot.dataset = status.dataset
                WHERE status.provider = ? AND status.dataset = ?
                """,
                (provider, dataset),
            ).fetchone()
        if row is None or str(row["status"]) != "ACTIVE":
            raise CatalogBarQueryPolicyError("exact dataset policy is not active")
        if row["unavailable_at"] is not None:
            raise CatalogBarQueryPolicyError("dataset became unavailable before query")
        expires_at = str(row["expires_at"]) if row["expires_at"] is not None else None
        if expires_at is not None and _parse_utc(expires_at) <= self._store._now():
            raise CatalogBarQueryPolicyError("dataset policy expired before query")
        try:
            # The enforcer compares this exact identity with the committed catalog.  Runtime
            # state may only narrow it and is reconstructed from SQLite, never caller input.
            committed = self._policy_enforcer.catalog.lookup(provider, dataset)
            runtime_status = DatasetRuntimeStatus.for_policy(
                committed,
                enabled=True,
                entitlement_active=(
                    bool(row["entitlement_active"])
                    if row["entitlement_active"] is not None
                    else None
                ),
                retention_started_at=_parse_utc(str(row["effective_at"])),
                expires_at=_parse_utc(expires_at) if expires_at is not None else None,
            )
            policy = self._policy_enforcer.authorize_query(
                provider,
                dataset,
                environment=self._environment,
                layer=RetentionLayer.NORMALIZED,
                runtime_status=runtime_status,
            )
        except (DatasetPolicyDenied, ValueError) as error:
            raise CatalogBarQueryPolicyError("retention policy denied canonical query") from error
        actual = (
            str(row["policy_id"]),
            int(row["revision"]),
            str(row["policy_hash"]),
            str(row["snapshot_mode"]),
            str(row["retention_mode"]),
        )
        expected = (
            policy.policy_id,
            policy.revision,
            policy.content_hash,
            policy.mode.value,
            policy.mode.value,
        )
        if actual != expected or policy.mode in {RetentionMode.PROHIBITED, RetentionMode.EPHEMERAL}:
            raise CatalogBarQueryPolicyError(
                "SQLite policy snapshot differs from the current exact catalog"
            )
        return _ActivePolicyProof(
            provider=provider,
            dataset=dataset,
            policy_snapshot_id=str(row["policy_snapshot_id"]),
            policy_id=policy.policy_id,
            policy_revision=policy.revision,
            policy_hash=policy.content_hash,
            retention_mode=policy.mode.value,
            effective_at=str(row["effective_at"]),
            expires_at=expires_at,
            unavailable_at=None,
            last_checked_at=str(row["last_checked_at"]),
        )

    def _assert_policy_unchanged(self, expected: _ActivePolicyProof) -> None:
        with self._store.read_only_connection() as connection:
            row = connection.execute(
                """
                SELECT status.*, snapshot.policy_id, snapshot.revision,
                       snapshot.policy_hash, snapshot.retention_mode AS snapshot_mode
                FROM dataset_policy_status AS status
                JOIN policy_snapshots AS snapshot
                  ON snapshot.policy_snapshot_id = status.policy_snapshot_id
                WHERE status.provider = ? AND status.dataset = ?
                """,
                (expected.provider, expected.dataset),
            ).fetchone()
        if row is None:
            raise CatalogBarQueryPolicyError("dataset policy disappeared during query")
        actual = _ActivePolicyProof(
            provider=str(row["provider"]),
            dataset=str(row["dataset"]),
            policy_snapshot_id=str(row["policy_snapshot_id"]),
            policy_id=str(row["policy_id"]),
            policy_revision=int(row["revision"]),
            policy_hash=str(row["policy_hash"]),
            retention_mode=str(row["snapshot_mode"]),
            effective_at=str(row["effective_at"]),
            expires_at=str(row["expires_at"]) if row["expires_at"] is not None else None,
            unavailable_at=(
                str(row["unavailable_at"]) if row["unavailable_at"] is not None else None
            ),
            last_checked_at=str(row["last_checked_at"]),
        )
        if str(row["status"]) != "ACTIVE" or actual != expected:
            raise CatalogBarQueryPolicyError("dataset policy changed during query")
        if actual.expires_at is not None and _parse_utc(actual.expires_at) <= self._store._now():
            raise CatalogBarQueryPolicyError("dataset policy expired during query")

    def _catalog_files(
        self,
        provider: str,
        dataset: str,
        query: BarQuery,
    ) -> tuple[_CatalogFile, ...]:
        predicates = [
            "policy.provider = ?",
            "policy.dataset = ?",
            "batch.state = 'VERIFIED'",
            "batch.verified_at IS NOT NULL",
            "active.status = 'ACTIVE'",
            "active.policy_snapshot_id = batch.policy_snapshot_id",
            "active.policy_snapshot_id = policy.policy_snapshot_id",
            "active.retention_mode = policy.retention_mode",
            "active.unavailable_at IS NULL",
            "policy.retention_mode NOT IN ('PROHIBITED', 'EPHEMERAL')",
            "file.interval_start IS NOT NULL",
            "file.interval_end IS NOT NULL",
        ]
        parameters: list[object] = [provider, dataset]
        if query.start is not None:
            predicates.append("file.interval_end > ?")
            parameters.append(query.start.isoformat().replace("+00:00", "Z"))
        if query.end is not None:
            predicates.append("file.interval_start < ?")
            parameters.append(query.end.isoformat().replace("+00:00", "Z"))
        now = self._store._now().isoformat().replace("+00:00", "Z")
        predicates.append("(active.expires_at IS NULL OR active.expires_at > ?)")
        parameters.append(now)
        with self._store.read_only_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT batch.canonical_batch_id, batch.batch_context_id,
                       batch.manifest_relative_path,
                       manifest.manifest_content_sha256,
                       manifest.manifest_byte_count,
                       file.relative_path, file.content_sha256, file.byte_count,
                       contract.processing_signature_json,
                       contract.processing_signature_hash
                FROM canonical_batches AS batch
                JOIN canonical_files AS file
                  ON file.canonical_batch_id = batch.canonical_batch_id
                JOIN canonical_batch_manifests AS manifest
                  ON manifest.canonical_batch_id = batch.canonical_batch_id
                JOIN batch_context_processing_contracts AS contract
                  ON contract.batch_context_id = batch.batch_context_id
                JOIN policy_snapshots AS policy
                  ON policy.policy_snapshot_id = batch.policy_snapshot_id
                JOIN dataset_policy_status AS active
                  ON active.provider = policy.provider AND active.dataset = policy.dataset
                WHERE {" AND ".join(predicates)}
                ORDER BY batch.canonical_batch_id, file.file_ordinal
                """,
                parameters,
            ).fetchall()
            files: list[_CatalogFile] = []
            batch_cache: dict[
                str, tuple[tuple[StreamKey, ...], Mapping[str, tuple[str, datetime]]]
            ] = {}
            for row in rows:
                batch_id = str(row["canonical_batch_id"])
                context_id = str(row["batch_context_id"])
                if batch_id not in batch_cache:
                    batch_cache[batch_id] = (
                        self._load_streams(connection, batch_id),
                        self._load_raw_provenance(connection, context_id),
                    )
                streams, raw_by_batch_id = batch_cache[batch_id]
                try:
                    processing = ProcessingSignature.model_validate_json(
                        str(row["processing_signature_json"])
                    )
                except ValueError as error:
                    raise CatalogBarQueryIntegrityError(
                        "cataloged processing signature is invalid"
                    ) from error
                processing_hash = hashlib.sha256(
                    json.dumps(
                        processing.model_dump(mode="json"),
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("utf-8")
                ).hexdigest()
                if processing_hash != str(row["processing_signature_hash"]):
                    raise CatalogBarQueryIntegrityError(
                        "cataloged processing signature hash is inconsistent"
                    )
                files.append(
                    _CatalogFile(
                        canonical_batch_id=batch_id,
                        batch_context_id=context_id,
                        relative_path=str(row["relative_path"]),
                        content_sha256=str(row["content_sha256"]),
                        byte_count=int(row["byte_count"]),
                        manifest_relative_path=str(row["manifest_relative_path"]),
                        manifest_sha256=str(row["manifest_content_sha256"]),
                        manifest_byte_count=int(row["manifest_byte_count"]),
                        processing_signature=processing,
                        processing_signature_hash=processing_hash,
                        streams=streams,
                        raw_by_batch_id=raw_by_batch_id,
                    )
                )
        return tuple(files)

    @staticmethod
    def _load_streams(
        connection: sqlite3.Connection,
        canonical_batch_id: str,
    ) -> tuple[StreamKey, ...]:
        rows = connection.execute(
            """
            SELECT stream.stream_id, stream.stream_hash, stream.dimensions_json
            FROM canonical_batch_streams AS batch_stream
            JOIN stream_keys AS stream ON stream.stream_id = batch_stream.stream_id
            WHERE batch_stream.canonical_batch_id = ?
              AND batch_stream.outcome = 'PUBLISHABLE'
            ORDER BY stream.stream_id
            """,
            (canonical_batch_id,),
        ).fetchall()
        streams: list[StreamKey] = []
        try:
            for row in rows:
                envelope = json.loads(str(row["dimensions_json"]))
                if envelope.get("kind") != "stream-key" or not isinstance(
                    envelope.get("payload"), dict
                ):
                    raise ValueError
                stream = StreamKey.model_validate(envelope["payload"])
                if stream.stream_id != str(row["stream_id"]) or stream.stream_hash != str(
                    row["stream_hash"]
                ):
                    raise ValueError
                streams.append(stream)
        except (json.JSONDecodeError, TypeError, ValueError) as error:
            raise CatalogBarQueryIntegrityError("cataloged stream identity is invalid") from error
        if not streams:
            raise CatalogBarQueryIntegrityError(
                "verified canonical batch has no publishable cataloged stream"
            )
        return tuple(streams)

    def _load_raw_provenance(
        self,
        connection: sqlite3.Connection,
        batch_context_id: str,
    ) -> Mapping[str, tuple[str, datetime]]:
        rows = connection.execute(
            """
            SELECT link.ordinal, artifact.artifact_id, artifact.state,
                   artifact.relative_path, artifact.manifest_relative_path,
                   artifact.content_sha256, artifact.byte_count,
                   manifest.manifest_content_sha256,
                   manifest.manifest_byte_count,
                   replay.raw_batch_id, replay.retrieved_at
            FROM batch_context_artifacts AS link
            JOIN raw_artifacts AS artifact ON artifact.artifact_id = link.artifact_id
            JOIN raw_artifact_manifests AS manifest
              ON manifest.artifact_id = artifact.artifact_id
            JOIN raw_replay_provenance AS replay
              ON replay.artifact_id = artifact.artifact_id
            WHERE link.batch_context_id = ?
            ORDER BY link.ordinal, replay.raw_batch_id, replay.attempt_id
            """,
            (batch_context_id,),
        ).fetchall()
        if not rows:
            raise CatalogBarQueryIntegrityError(
                "verified canonical batch lacks retained raw provenance"
            )
        mapping: dict[str, tuple[str, datetime]] = {}
        verified_artifacts: set[str] = set()
        integrity_cache: set[tuple[str, str, int]] = set()
        for row in rows:
            artifact_id = str(row["artifact_id"])
            if str(row["state"]) != "VERIFIED":
                raise CatalogBarQueryIntegrityError(
                    "canonical batch is supported by non-verified raw evidence"
                )
            if artifact_id not in verified_artifacts:
                self._verify_managed_file(
                    str(row["relative_path"]),
                    expected_sha256=str(row["content_sha256"]),
                    expected_bytes=int(row["byte_count"]),
                    cache=integrity_cache,
                )
                self._verify_managed_file(
                    str(row["manifest_relative_path"]),
                    expected_sha256=str(row["manifest_content_sha256"]),
                    expected_bytes=int(row["manifest_byte_count"]),
                    cache=integrity_cache,
                )
                verified_artifacts.add(artifact_id)
            raw_batch_id = str(row["raw_batch_id"])
            candidate = (artifact_id, _parse_utc(str(row["retrieved_at"])))
            existing = mapping.get(raw_batch_id)
            if existing is not None and existing != candidate:
                raise CatalogBarQueryIntegrityError(
                    "raw batch provenance maps to conflicting immutable artifacts"
                )
            mapping[raw_batch_id] = candidate
        return mapping

    def _verify_managed_file(
        self,
        relative_path: str,
        *,
        expected_sha256: str,
        expected_bytes: int,
        cache: set[tuple[str, str, int]],
    ) -> Path:
        identity = (relative_path, expected_sha256, expected_bytes)
        path = self._data_root.managed_path(
            Path(relative_path),
            expected_root_id=self._root_id,
        )
        if identity in cache:
            return path
        try:
            details = path.lstat()
        except OSError as error:
            raise CatalogBarQueryIntegrityError("cataloged query file is missing") from error
        attributes = getattr(details, "st_file_attributes", 0)
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        if (
            not stat.S_ISREG(details.st_mode)
            or path.is_symlink()
            or bool(reparse_flag and attributes & reparse_flag)
            or details.st_nlink != 1
            or not self._store._managed_file_matches_catalog(
                relative_path,
                expected_sha256=expected_sha256,
                expected_bytes=expected_bytes,
            )
        ):
            raise CatalogBarQueryIntegrityError(
                "cataloged query file is unsafe or differs from its verified checksum"
            )
        cache.add(identity)
        return path

    @staticmethod
    def _query_sql(query: BarQuery) -> str:
        predicates: list[str] = []
        if query.instrument_ids:
            placeholders = ", ".join("?" for _ in query.instrument_ids)
            predicates.append(f"instrument_id IN ({placeholders})")
        if query.timeframe is not None:
            predicates.append("timeframe = ?")
        if query.source_ids:
            placeholders = ", ".join("?" for _ in query.source_ids)
            predicates.append(f"source_id IN ({placeholders})")
        if query.session is not None:
            predicates.append("session = ?")
        if query.adjustment_state is not None:
            predicates.append("adjustment_state = ?")
        if query.start is not None:
            predicates.append("timestamp_start >= ?")
        if query.end is not None:
            predicates.append("timestamp_start < ?")
        selected = ", ".join(f'"{column}"' for column in _CANONICAL_COLUMNS)
        where = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        return (
            f"SELECT {selected} FROM _cataloged_price_bars{where} "
            "ORDER BY timestamp_start, instrument_id, raw_batch_id"
        )

    @staticmethod
    def _query_parameters(query: BarQuery) -> list[object]:
        parameters: list[object] = []
        parameters.extend(str(value) for value in query.instrument_ids)
        if query.timeframe is not None:
            parameters.append(
                query.timeframe.value if hasattr(query.timeframe, "value") else str(query.timeframe)
            )
        parameters.extend(str(value) for value in query.source_ids)
        if query.session is not None:
            parameters.append(
                query.session.value if hasattr(query.session, "value") else str(query.session)
            )
        if query.adjustment_state is not None:
            parameters.append(
                query.adjustment_state.value
                if hasattr(query.adjustment_state, "value")
                else str(query.adjustment_state)
            )
        if query.start is not None:
            parameters.append(query.start)
        if query.end is not None:
            parameters.append(query.end)
        return parameters

    def _observed_rows(
        self,
        frame: pl.DataFrame,
        catalog: _CatalogFile,
    ) -> Iterable[_ObservedCandidate]:
        if frame.is_empty():
            return ()
        if frame.schema != PRICE_BAR_SCHEMA:
            raise CatalogBarQueryIntegrityError(
                "reopened canonical Parquet differs from the implemented price-bar schema"
            )
        observed: list[_ObservedCandidate] = []
        for row in frame.iter_rows(named=True):
            bar = self._bar(row)
            stream = self._stream_for_bar(bar, catalog.streams)
            raw = catalog.raw_by_batch_id.get(str(bar.raw_batch_id))
            if raw is None:
                raise CatalogBarQueryIntegrityError(
                    "canonical row lacks exact raw artifact provenance"
                )
            raw_artifact_id, retrieved_at = raw
            if retrieved_at != bar.retrieved_at:
                raise CatalogBarQueryIntegrityError(
                    "canonical row retrieval time differs from fixed raw provenance"
                )
            semantic = self._semantic_observation(
                bar,
                stream,
                catalog.processing_signature,
            )
            observed.append(
                _ObservedCandidate(
                    observation_id=semantic.identity.observation_id,
                    stream_id=stream.stream_id,
                    value_fingerprint=semantic.value_fingerprint,
                    bar=bar,
                    provenance=RevisionProvenance(
                        canonical_batch_id=catalog.canonical_batch_id,
                        raw_artifact_id=raw_artifact_id,
                        raw_batch_id=bar.raw_batch_id,
                        retrieved_at=retrieved_at,
                        processing_signature_hash=catalog.processing_signature_hash,
                    ),
                )
            )
        return tuple(observed)

    @staticmethod
    def _bar(row: Mapping[str, object]) -> PriceBar:
        try:
            return PriceBar.model_validate(dict(row))
        except ValueError as error:
            raise CatalogBarQueryIntegrityError(
                "cataloged canonical row violates the PriceBar contract"
            ) from error

    @staticmethod
    def _stream_for_bar(bar: PriceBar, streams: Sequence[StreamKey]) -> StreamKey:
        matches = tuple(
            value
            for value in streams
            if value.instrument_id == bar.instrument_id
            and value.timeframe is bar.timeframe
            and value.session is bar.session
            and value.adjustment is bar.adjustment_state
            and value.currency == bar.currency
        )
        if len(matches) != 1:
            raise CatalogBarQueryIntegrityError(
                "canonical row does not resolve to one exact cataloged stream"
            )
        return matches[0]

    @staticmethod
    def _semantic_observation(
        bar: PriceBar,
        stream: StreamKey,
        processing_signature: ProcessingSignature,
    ) -> SemanticObservation:
        return SemanticObservation.create(
            ObservationIdentity(
                stream=stream,
                start=bar.timestamp_start,
                end=bar.timestamp_end,
            ),
            PriceBarSemanticValue(
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                vwap=bar.vwap,
                currency=bar.currency,
                available_at=bar.available_at,
                quality_flags=bar.quality_flags,
            ),
            processing_signature,
        )

    @staticmethod
    def _semantic_revisions(
        values: Sequence[_ObservedCandidate],
    ) -> tuple[CanonicalBarRevision, ...]:
        by_observation: dict[str, dict[str, list[_ObservedCandidate]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for value in values:
            by_observation[value.observation_id][value.value_fingerprint].append(value)

        revisions: list[CanonicalBarRevision] = []
        for observation_id in sorted(by_observation):
            fingerprint_groups = by_observation[observation_id]
            ordered_groups: list[tuple[tuple[object, ...], str, list[_ObservedCandidate]]] = []
            for fingerprint, candidates in fingerprint_groups.items():
                ordered = sorted(candidates, key=lambda value: value.provenance.order_key)
                semantic_values = {
                    (
                        value.bar.open,
                        value.bar.high,
                        value.bar.low,
                        value.bar.close,
                        value.bar.volume,
                        value.bar.vwap,
                        value.bar.currency,
                        value.bar.available_at,
                        value.bar.quality_flags,
                    )
                    for value in ordered
                }
                if len(semantic_values) != 1:
                    raise CatalogBarQueryIntegrityError(
                        "one semantic fingerprint resolves to conflicting canonical values"
                    )
                ordered_groups.append((ordered[-1].provenance.order_key, fingerprint, ordered))
            ordered_groups.sort(key=lambda value: (value[0], value[1]))
            for index, (_, fingerprint, candidates) in enumerate(ordered_groups, start=1):
                selected = candidates[-1]
                unique_provenance = {
                    (
                        value.provenance.canonical_batch_id,
                        value.provenance.raw_artifact_id,
                        value.provenance.raw_batch_id,
                    ): value.provenance
                    for value in candidates
                }
                revisions.append(
                    CanonicalBarRevision(
                        observation_id=observation_id,
                        stream_id=selected.stream_id,
                        value_fingerprint=fingerprint,
                        revision_number=index,
                        is_current=index == len(ordered_groups),
                        bar=selected.bar,
                        provenances=tuple(
                            sorted(unique_provenance.values(), key=lambda value: value.order_key)
                        ),
                    )
                )
        return tuple(
            sorted(
                revisions,
                key=lambda value: (
                    value.bar.timestamp_start,
                    value.stream_id,
                    value.revision_number,
                ),
            )
        )


__all__ = [
    "CandidateBatchComparison",
    "CandidateBatchDisposition",
    "CandidateStreamComparison",
    "CanonicalBarRevision",
    "CatalogBarQueryError",
    "CatalogBarQueryIntegrityError",
    "CatalogBarQueryPolicyError",
    "CatalogBarQueryRepository",
    "CatalogBarQueryResult",
    "CatalogRevisionView",
    "RevisionProvenance",
    "SemanticDuplicateSlot",
]
