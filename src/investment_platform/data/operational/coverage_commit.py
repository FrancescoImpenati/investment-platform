"""Build the retention-aware coverage effect of one verified publication.

This module is deliberately deterministic and does no I/O.  The caller first
publishes and reopens the canonical directory, then supplies the same frozen
parts and manifest here.  The resulting :class:`CoverageCommit` is consumed by
the short SQLite publication transaction.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Final
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import polars as pl

from investment_platform.data.calendar import CalendarSnapshot
from investment_platform.data.ingestion.coverage import (
    CoverageRequestTerminalState,
    CoverageSegment,
    CoverageStreamOutcome,
    GapFinding,
    GapStatus,
    GapType,
    MaterializedWatermark,
    MissingSlotReason,
    WatermarkChangeContext,
    materialize_watermark,
    reconstruct_frontier,
    transition_gap,
)
from investment_platform.data.ingestion.identity import RequestSpecification, StreamKey
from investment_platform.data.ingestion.planner import (
    CoverageClassification,
    CoverageVerificationState,
)
from investment_platform.data.market_time import to_utc
from investment_platform.data.operational.execution import (
    CoverageCommit,
    SemanticNoOpObservationProof,
    SemanticNoOpReconciliation,
)
from investment_platform.data.retention import DatasetRetentionPolicy, DatasetRuntimeStatus
from investment_platform.data.storage import (
    CanonicalBatchManifest,
    CanonicalParquetPart,
    CanonicalStreamOutcome,
    StreamPublicationOutcome,
)

_COVERAGE_ID_VERSION: Final = "coverage_v1"
_GAP_ID_VERSION: Final = "gap_v1"


class CoverageCommitBuildError(RuntimeError):
    """Verified publication facts cannot produce a safe coverage commit."""


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _coverage_id(
    *,
    canonical_batch_id: str,
    stream_id: str,
    start: datetime,
    end: datetime,
) -> str:
    digest = _canonical_hash(
        {
            "batch": canonical_batch_id,
            "stream": stream_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "classification": "OBSERVED",
        }
    )
    return f"{_COVERAGE_ID_VERSION}_{digest}"


def _gap_id(
    *,
    stream_id: str,
    start: datetime,
    end: datetime,
    gap_type: GapType,
    episode_id: str,
) -> str:
    digest = _canonical_hash(
        {
            "stream": stream_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "type": gap_type.value,
            "episode": episode_id,
        }
    )
    return f"{_GAP_ID_VERSION}_{digest}"


def _blocked_gap_type(outcome: CanonicalStreamOutcome) -> GapType:
    if outcome.validation_codes == ("NO_CANONICAL_ROWS",):
        return GapType.EXPECTED_OBSERVATION
    return GapType.INTEGRITY


def build_blocking_integrity_gaps(
    *,
    stream_outcomes: Sequence[CanonicalStreamOutcome],
    request_instance_id: UUID,
    detected_at: datetime,
) -> tuple[GapFinding, ...]:
    """Create deterministic durable findings for streams blocked before publication.

    This is the no-publication companion to :func:`build_publication_coverage_commit`.
    It deliberately creates neither coverage nor watermarks.  A stream with no
    canonical rows is an expected-observation gap; other deterministic validation
    failures remain integrity gaps over the exact bounded request interval.
    """

    now = to_utc(detected_at)
    gaps: list[GapFinding] = []
    for outcome in stream_outcomes:
        if outcome.outcome is not StreamPublicationOutcome.BLOCKED:
            continue
        gap_type = _blocked_gap_type(outcome)
        gaps.append(
            GapFinding(
                gap_id=_gap_id(
                    stream_id=outcome.stream_id,
                    start=outcome.request_start,
                    end=outcome.request_end,
                    gap_type=gap_type,
                    episode_id=str(request_instance_id),
                ),
                stream_id=outcome.stream_id,
                start=outcome.request_start,
                end=outcome.request_end,
                gap_type=gap_type,
                status=GapStatus.OPEN,
                blocking=True,
                detected_at=now,
                request_instance_id=str(request_instance_id),
            )
        )
    if len({gap.gap_id for gap in gaps}) != len(gaps):
        raise CoverageCommitBuildError("blocked stream outcomes contain duplicate identities")
    return tuple(sorted(gaps, key=lambda gap: gap.gap_id))


def build_blocking_acquisition_gaps(
    *,
    streams: Sequence[StreamKey],
    start: datetime,
    end: datetime,
    request_instance_id: UUID,
    detected_at: datetime,
) -> tuple[GapFinding, ...]:
    """Create one exact terminal acquisition gap for every bounded request stream."""

    if end <= start:
        raise CoverageCommitBuildError("acquisition gap bounds must be half-open and non-empty")
    now = to_utc(detected_at)
    gaps = tuple(
        GapFinding(
            gap_id=_gap_id(
                stream_id=stream.stream_id,
                start=start,
                end=end,
                gap_type=GapType.ACQUISITION,
                episode_id=str(request_instance_id),
            ),
            stream_id=stream.stream_id,
            start=start,
            end=end,
            gap_type=GapType.ACQUISITION,
            status=GapStatus.OPEN,
            blocking=True,
            detected_at=now,
            request_instance_id=str(request_instance_id),
        )
        for stream in streams
    )
    if not gaps or len({gap.gap_id for gap in gaps}) != len(gaps):
        raise CoverageCommitBuildError(
            "terminal acquisition gaps require unique exact request streams"
        )
    return tuple(sorted(gaps, key=lambda gap: gap.gap_id))


def _frame(parts: Sequence[CanonicalParquetPart]) -> pl.DataFrame:
    if not parts:
        raise CoverageCommitBuildError("published canonical batch has no in-memory parts")
    return pl.concat([part.frame for part in parts], how="vertical", rechunk=False)


def _contiguous_runs(
    indexes: Sequence[int],
) -> tuple[tuple[int, int], ...]:
    if not indexes:
        return ()
    ordered = sorted(set(indexes))
    if len(ordered) != len(indexes):
        raise CoverageCommitBuildError("one stream contains a duplicate eligible slot")
    runs: list[tuple[int, int]] = []
    first = previous = ordered[0]
    for index in ordered[1:]:
        if index != previous + 1:
            runs.append((first, previous))
            first = index
        previous = index
    runs.append((first, previous))
    return tuple(runs)


def _replace_gaps(
    existing: Sequence[GapFinding],
    changed: Sequence[GapFinding],
) -> tuple[GapFinding, ...]:
    values = {gap.gap_id: gap for gap in existing}
    values.update({gap.gap_id: gap for gap in changed})
    return tuple(sorted(values.values(), key=lambda value: value.gap_id))


def _existing_coverage_start(
    requested: datetime,
    existing_segments: Sequence[CoverageSegment],
    existing_watermarks: Sequence[MaterializedWatermark],
) -> datetime:
    candidate = to_utc(requested)
    starts = {
        *(segment.coverage_start for segment in existing_segments),
        *(watermark.coverage_start for watermark in existing_watermarks),
    }
    if len(starts) > 1:
        raise CoverageCommitBuildError(
            "durable coverage contains conflicting authoritative stream origins"
        )
    if not starts:
        return candidate
    durable = next(iter(starts))
    if candidate > durable:
        raise CoverageCommitBuildError(
            "coverage_start cannot move forward and discard verified history"
        )
    return min(candidate, durable)


def _snapshot_covers_interval(
    manifest: CanonicalBatchManifest,
    *,
    start: datetime,
    end: datetime,
) -> bool:
    """Return whether the immutable snapshot can prove every session in an interval."""

    try:
        timezone = ZoneInfo(manifest.calendar_snapshot.timezone_name)
    except (ZoneInfoNotFoundError, ValueError):
        return False
    first_date = start.astimezone(timezone).date()
    last_date = (end - timedelta(microseconds=1)).astimezone(timezone).date()
    return (
        manifest.calendar_snapshot.range_start <= first_date
        and manifest.calendar_snapshot.range_end > last_date
    )


def _gap_is_fully_observed(
    gap: GapFinding,
    *,
    manifest: CanonicalBatchManifest,
    stream_id: str,
    calendar_snapshot_id: str,
    policy_snapshot_id: str,
    existing_segments: Sequence[CoverageSegment],
    current_segments: Sequence[CoverageSegment],
) -> bool:
    """Require complete slot evidence before resolving a possibly larger prior gap.

    A repair request may cover only a proper subset of a durable gap. Merely
    observing every slot that overlaps the current request cannot close the
    unrequested remainder. Resolution therefore uses the union of retained
    prior coverage and this batch, and only when the frozen calendar covers the
    whole finding.
    """

    if not _snapshot_covers_interval(manifest, start=gap.start, end=gap.end):
        return False
    stream = next(
        (outcome.stream for outcome in manifest.streams if outcome.stream_id == stream_id),
        None,
    )
    if stream is None:
        return False
    eligible_slots = tuple(
        slot
        for slot in manifest.calendar_snapshot.expected_slots(stream.timeframe)
        if slot.start_utc >= gap.start and slot.end_utc <= gap.end
    )
    if not eligible_slots:
        return False

    def valid_observed(segment: CoverageSegment) -> bool:
        return (
            segment.stream_id == stream_id
            and segment.calendar_snapshot_id == calendar_snapshot_id
            and segment.calendar_snapshot_checksum == manifest.calendar_snapshot.checksum
            and segment.policy_snapshot_id == policy_snapshot_id
            and segment.classification is CoverageClassification.OBSERVED
            and segment.verification_state is CoverageVerificationState.VERIFIED
            and segment.retained
            and segment.invalidated_at is None
            and segment.artifacts_present
            and segment.artifact_integrity_verified
            and segment.interval_verified
            and segment.request_completed
            and segment.pagination_verified
            and segment.terminal_page_verified
            and segment.canonical_batch_verified
            and segment.relational_provenance_verified
        )

    current = tuple(segment for segment in current_segments if valid_observed(segment))
    if not any(segment.start < gap.end and segment.end > gap.start for segment in current):
        return False
    support = tuple(
        segment for segment in (*existing_segments, *current_segments) if valid_observed(segment)
    )
    return all(
        any(segment.start <= slot.start_utc and segment.end >= slot.end_utc for segment in support)
        for slot in eligible_slots
    )


def _terminal_state(
    manifest: CanonicalBatchManifest,
    changed_gaps: Sequence[GapFinding],
) -> CoverageRequestTerminalState:
    blocked_stream = any(
        stream.outcome is StreamPublicationOutcome.BLOCKED for stream in manifest.streams
    )
    if blocked_stream or any(gap.actively_blocks for gap in changed_gaps):
        return CoverageRequestTerminalState.PARTIAL
    return CoverageRequestTerminalState.SUCCESS


def _existing_segment_supports_slot(
    segment: CoverageSegment,
    *,
    stream_id: str,
    start: datetime,
    end: datetime,
    calendar_snapshot_id: str,
    calendar_snapshot_checksum: str,
    policy_snapshot_id: str,
) -> bool:
    """Recognize a retained exact-snapshot proof behind a semantic duplicate."""

    return (
        segment.stream_id == stream_id
        and segment.start <= start
        and segment.end >= end
        and segment.calendar_snapshot_id == calendar_snapshot_id
        and segment.calendar_snapshot_checksum == calendar_snapshot_checksum
        and segment.policy_snapshot_id == policy_snapshot_id
        and segment.classification is CoverageClassification.OBSERVED
        and segment.verification_state is CoverageVerificationState.VERIFIED
        and segment.retained
        and segment.invalidated_at is None
        and segment.artifacts_present
        and segment.artifact_integrity_verified
        and segment.interval_verified
        and segment.request_completed
        and segment.pagination_verified
        and segment.terminal_page_verified
        and segment.canonical_batch_verified
        and segment.relational_provenance_verified
    )


def build_semantic_noop_reconciliation(
    *,
    specification: RequestSpecification,
    duplicate_observations: Sequence[SemanticNoOpObservationProof],
    calendar_snapshot: CalendarSnapshot,
    calendar_snapshot_id: str,
    policy_snapshot_id: str,
    policy: DatasetRetentionPolicy,
    runtime_status: DatasetRuntimeStatus | None,
    run_id: UUID,
    coverage_start: datetime,
    frontier_domain_end: datetime,
    verified_at: datetime,
    existing_segments: Sequence[CoverageSegment],
    existing_gaps: Sequence[GapFinding],
    existing_watermarks: Sequence[MaterializedWatermark],
) -> SemanticNoOpReconciliation | None:
    """Rebuild only streams whose complete gap is proven by retained equal values."""

    now = to_utc(verified_at)
    origin = _existing_coverage_start(
        coverage_start,
        existing_segments,
        existing_watermarks,
    )
    domain_end = max(to_utc(frontier_domain_end), specification.end)
    watermarks_by_stream = {value.stream_id: value for value in existing_watermarks}
    if len(watermarks_by_stream) != len(existing_watermarks):
        raise CoverageCommitBuildError("existing watermark projection contains duplicates")
    proofs_by_stream: dict[
        str,
        dict[tuple[datetime, datetime], SemanticNoOpObservationProof],
    ] = {}
    for proof in duplicate_observations:
        proof_slot = (proof.start, proof.end)
        stream_proofs = proofs_by_stream.setdefault(proof.stream_id, {})
        if proof_slot in stream_proofs:
            raise CoverageCommitBuildError("semantic duplicate slot proofs contain duplicates")
        stream_proofs[proof_slot] = proof

    prior_gaps: list[GapFinding] = []
    resolved_gaps: list[GapFinding] = []
    prior_watermarks: list[MaterializedWatermark] = []
    watermarks: list[MaterializedWatermark] = []
    for stream in specification.stream_keys():
        prior_watermark = watermarks_by_stream.get(stream.stream_id)
        if prior_watermark is None:
            continue
        stream_segments = tuple(
            segment for segment in existing_segments if segment.stream_id == stream.stream_id
        )
        stream_gaps = tuple(gap for gap in existing_gaps if gap.stream_id == stream.stream_id)
        stream_proofs = proofs_by_stream.get(stream.stream_id, {})
        proposed: list[GapFinding] = []
        for gap in stream_gaps:
            if (
                not gap.actively_blocks
                or gap.start < specification.start
                or gap.end > specification.end
            ):
                continue
            slots = tuple(
                slot
                for slot in calendar_snapshot.expected_slots(stream.timeframe)
                if slot.start_utc >= gap.start and slot.end_utc <= gap.end
            )
            slot_keys = {(slot.start_utc, slot.end_utc) for slot in slots}
            gap_proof_keys = {
                slot for slot in stream_proofs if slot[0] >= gap.start and slot[1] <= gap.end
            }
            if (
                not slots
                or slots[0].start_utc != gap.start
                or slots[-1].end_utc != gap.end
                or gap_proof_keys != slot_keys
            ):
                continue
            common_batches: set[str] | None = None
            fully_proven = True
            for calendar_slot in slots:
                proof = stream_proofs[(calendar_slot.start_utc, calendar_slot.end_utc)]
                supported = {
                    segment.canonical_batch_id
                    for segment in stream_segments
                    if segment.canonical_batch_id in proof.matching_supporting_batch_ids
                    and _existing_segment_supports_slot(
                        segment,
                        stream_id=stream.stream_id,
                        start=calendar_slot.start_utc,
                        end=calendar_slot.end_utc,
                        calendar_snapshot_id=calendar_snapshot_id,
                        calendar_snapshot_checksum=calendar_snapshot.checksum,
                        policy_snapshot_id=policy_snapshot_id,
                    )
                }
                if not supported:
                    fully_proven = False
                    break
                common_batches = supported if common_batches is None else common_batches & supported
                if not common_batches:
                    fully_proven = False
                    break
            if not fully_proven or not common_batches:
                continue
            common_batches = {
                batch_id
                for batch_id in common_batches
                if any(
                    segment.canonical_batch_id == batch_id
                    and _existing_segment_supports_slot(
                        segment,
                        stream_id=stream.stream_id,
                        start=gap.start,
                        end=gap.end,
                        calendar_snapshot_id=calendar_snapshot_id,
                        calendar_snapshot_checksum=calendar_snapshot.checksum,
                        policy_snapshot_id=policy_snapshot_id,
                    )
                    for segment in stream_segments
                )
            }
            if not common_batches:
                continue
            supporting_batch_id = next(iter(sorted(common_batches)))
            proposed.append(
                transition_gap(
                    gap,
                    GapStatus.RESOLVED,
                    transitioned_at=now,
                ).model_copy(update={"canonical_batch_id": supporting_batch_id})
            )
        if not proposed:
            continue

        projected_gaps = _replace_gaps(stream_gaps, proposed)
        evaluation = reconstruct_frontier(
            stream=stream,
            calendar_snapshot=calendar_snapshot,
            calendar_snapshot_id=calendar_snapshot_id,
            policy=policy,
            policy_snapshot_id=policy_snapshot_id,
            runtime_status=runtime_status,
            coverage_start=origin,
            domain_end=max(
                domain_end,
                prior_watermark.exclusive_frontier,
                *(segment.end for segment in stream_segments),
                *(gap.end for gap in stream_gaps),
            ),
            coverage=stream_segments,
            gaps=projected_gaps,
            evaluated_at=now,
        )
        candidate = evaluation.candidate
        resolution_batches = {
            value.canonical_batch_id for value in proposed if value.canonical_batch_id is not None
        }
        changing_batches = (
            set(candidate.supporting_batch_ids) & resolution_batches
            if candidate is not None
            else set()
        )
        if (
            candidate is None
            or candidate.exclusive_frontier < prior_watermark.exclusive_frontier
            or not changing_batches
        ):
            continue
        rebuilt = materialize_watermark(
            candidate,
            WatermarkChangeContext(
                generation=prior_watermark.generation + 1,
                last_run_id=str(run_id),
                last_batch_id=next(iter(sorted(changing_batches))),
                computed_at=now,
            ),
        )
        prior_gaps.extend(
            sorted(
                (gap for gap in stream_gaps if gap.gap_id in {value.gap_id for value in proposed}),
                key=lambda value: value.gap_id,
            )
        )
        resolved_gaps.extend(sorted(proposed, key=lambda value: value.gap_id))
        prior_watermarks.append(prior_watermark)
        watermarks.append(rebuilt)

    if not resolved_gaps:
        return None
    ordered_prior_gaps = tuple(sorted(prior_gaps, key=lambda value: value.gap_id))
    ordered_resolved_gaps = tuple(sorted(resolved_gaps, key=lambda value: value.gap_id))
    return SemanticNoOpReconciliation(
        calendar_snapshot_id=calendar_snapshot_id,
        policy_snapshot_id=policy_snapshot_id,
        prior_gaps=ordered_prior_gaps,
        resolved_gaps=ordered_resolved_gaps,
        prior_watermarks=tuple(sorted(prior_watermarks, key=lambda value: value.stream_id)),
        watermarks=tuple(sorted(watermarks, key=lambda value: value.stream_id)),
    )


def build_publication_coverage_commit(
    *,
    manifest: CanonicalBatchManifest,
    parts: Sequence[CanonicalParquetPart],
    calendar_snapshot_id: str,
    policy_snapshot_id: str,
    policy: DatasetRetentionPolicy,
    runtime_status: DatasetRuntimeStatus | None,
    run_id: UUID,
    request_instance_id: UUID,
    coverage_start: datetime,
    frontier_domain_end: datetime,
    verified_at: datetime,
    existing_segments: Sequence[CoverageSegment] = (),
    existing_gaps: Sequence[GapFinding] = (),
    existing_watermarks: Sequence[MaterializedWatermark] = (),
    semantic_duplicate_slots: Sequence[tuple[str, datetime, datetime]] = (),
) -> CoverageCommit:
    """Derive segments, gaps, and changing watermarks from verified canonical rows.

    Alpaca omission semantics are intentionally absent.  A missing eligible slot
    therefore creates an ``EXPECTED_OBSERVATION`` gap and never a durable empty
    coverage fact.
    """

    now = to_utc(verified_at)
    domain_end = to_utc(frontier_domain_end)
    origin = _existing_coverage_start(
        coverage_start,
        existing_segments,
        existing_watermarks,
    )
    durable_existing_watermarks = tuple(existing_watermarks)
    existing_segments = tuple(
        segment
        if segment.coverage_start == origin
        else segment.model_copy(update={"coverage_start": origin})
        for segment in existing_segments
    )
    existing_watermarks = tuple(
        watermark
        if watermark.coverage_start == origin
        else watermark.model_copy(update={"coverage_start": origin})
        for watermark in existing_watermarks
    )
    if domain_end <= origin:
        raise CoverageCommitBuildError("frontier domain must extend beyond coverage_start")
    if (manifest.provider, manifest.dataset) != (policy.provider, policy.dataset):
        raise CoverageCommitBuildError("manifest and exact retention policy differ")
    if manifest.manifest_created_at > now:
        raise CoverageCommitBuildError("coverage verification predates the batch manifest")

    frame = _frame(parts)
    if frame.height != manifest.row_count:
        raise CoverageCommitBuildError("coverage input rows differ from the verified manifest")
    slots = manifest.eligible_slots
    slot_index = {(slot.start_utc, slot.end_utc): index for index, slot in enumerate(slots)}
    if len(slot_index) != len(slots):
        raise CoverageCommitBuildError("manifest calendar contains duplicate eligible slots")
    stream_ids = {outcome.stream_id for outcome in manifest.streams}
    duplicate_slots = {
        (stream_id, to_utc(start), to_utc(end))
        for stream_id, start, end in semantic_duplicate_slots
    }
    if len(duplicate_slots) != len(semantic_duplicate_slots):
        raise CoverageCommitBuildError("semantic duplicate slot proofs contain duplicates")
    if any(
        stream_id not in stream_ids or (start, end) not in slot_index
        for stream_id, start, end in duplicate_slots
    ):
        raise CoverageCommitBuildError(
            "semantic duplicate proof is outside the exact manifest stream/slot domain"
        )

    existing_by_id = {segment.coverage_id: segment for segment in existing_segments}
    existing_watermark_by_stream = {
        watermark.stream_id: watermark for watermark in durable_existing_watermarks
    }
    if len(existing_watermark_by_stream) != len(durable_existing_watermarks):
        raise CoverageCommitBuildError("existing watermark projection contains duplicates")

    segments: list[CoverageSegment] = []
    changed_gaps: list[GapFinding] = []
    for outcome in manifest.streams:
        stream = outcome.stream
        stream_id = stream.stream_id
        if outcome.outcome is StreamPublicationOutcome.BLOCKED:
            gap_type = _blocked_gap_type(outcome)
            identifier = _gap_id(
                stream_id=stream_id,
                start=outcome.request_start,
                end=outcome.request_end,
                gap_type=gap_type,
                episode_id=str(request_instance_id),
            )
            existing_gap = next(
                (gap for gap in existing_gaps if gap.gap_id == identifier),
                None,
            )
            changed_gaps.append(
                existing_gap
                or GapFinding(
                    gap_id=identifier,
                    stream_id=stream_id,
                    start=outcome.request_start,
                    end=outcome.request_end,
                    gap_type=gap_type,
                    status=GapStatus.OPEN,
                    blocking=True,
                    detected_at=now,
                    request_instance_id=str(request_instance_id),
                )
            )
            continue

        instrument_rows = frame.filter(pl.col("instrument_id") == str(stream.instrument_id)).select(
            "timestamp_start", "timestamp_end"
        )
        observed_indexes: list[int] = []
        for row_start, row_end in instrument_rows.iter_rows():
            if not isinstance(row_start, datetime) or not isinstance(row_end, datetime):
                raise CoverageCommitBuildError("canonical slot timestamps are not datetimes")
            index = slot_index.get((to_utc(row_start), to_utc(row_end)))
            if index is None:
                raise CoverageCommitBuildError(
                    "canonical row is not an exact manifest-eligible slot"
                )
            observed_indexes.append(index)
        if len(observed_indexes) != outcome.row_count:
            raise CoverageCommitBuildError("stream row count differs from the manifest outcome")

        stream_existing = tuple(
            segment for segment in existing_segments if segment.stream_id == stream_id
        )
        next_generation = (
            max(
                (segment.generation for segment in stream_existing),
                default=0,
            )
            + 1
        )
        for first, last in _contiguous_runs(observed_indexes):
            start = slots[first].start_utc
            end = slots[last].end_utc
            identifier = _coverage_id(
                canonical_batch_id=manifest.canonical_batch_id,
                stream_id=stream_id,
                start=start,
                end=end,
            )
            prior = existing_by_id.get(identifier)
            segment_candidate = CoverageSegment(
                coverage_id=identifier,
                stream_id=stream_id,
                canonical_batch_id=manifest.canonical_batch_id,
                calendar_snapshot_id=calendar_snapshot_id,
                calendar_snapshot_checksum=manifest.calendar_snapshot.checksum,
                policy_snapshot_id=policy_snapshot_id,
                policy_id=policy.policy_id,
                policy_revision=policy.revision,
                policy_hash=policy.content_hash,
                coverage_start=origin,
                start=start,
                end=end,
                classification=CoverageClassification.OBSERVED,
                verification_state=CoverageVerificationState.VERIFIED,
                retained=True,
                row_count=last - first + 1,
                artifact_count=len(manifest.ordered_raw_artifacts),
                artifacts_present=True,
                artifact_integrity_verified=True,
                interval_verified=True,
                request_completed=True,
                # Finalized after all request gaps are known.  SUCCESS is only
                # a construction placeholder and never escapes this builder.
                request_terminal_state=CoverageRequestTerminalState.SUCCESS,
                stream_outcome=CoverageStreamOutcome.PUBLISHABLE,
                pagination_verified=True,
                terminal_page_verified=True,
                canonical_batch_verified=True,
                canonical_file_count=len(manifest.files),
                raw_artifact_count=len(manifest.ordered_raw_artifacts),
                relational_provenance_verified=True,
                generation=prior.generation if prior is not None else next_generation,
                verified_at=prior.verified_at if prior is not None else now,
            )
            if prior is not None and prior != segment_candidate:
                raise CoverageCommitBuildError(
                    "deterministic coverage identity collides with different facts"
                )
            segments.append(segment_candidate)

        observed = set(observed_indexes)
        missing = [
            index
            for index, slot in enumerate(slots)
            if index not in observed
            and not (
                (stream_id, slot.start_utc, slot.end_utc) in duplicate_slots
                and any(
                    _existing_segment_supports_slot(
                        segment,
                        stream_id=stream_id,
                        start=slot.start_utc,
                        end=slot.end_utc,
                        calendar_snapshot_id=calendar_snapshot_id,
                        calendar_snapshot_checksum=manifest.calendar_snapshot.checksum,
                        policy_snapshot_id=policy_snapshot_id,
                    )
                    for segment in stream_existing
                )
            )
        ]
        for first, last in _contiguous_runs(missing):
            start = slots[first].start_utc
            end = slots[last].end_utc
            identifier = _gap_id(
                stream_id=stream_id,
                start=start,
                end=end,
                gap_type=GapType.EXPECTED_OBSERVATION,
                episode_id=str(request_instance_id),
            )
            existing_gap = next(
                (gap for gap in existing_gaps if gap.gap_id == identifier),
                None,
            )
            changed_gaps.append(
                existing_gap
                or GapFinding(
                    gap_id=identifier,
                    stream_id=stream_id,
                    start=start,
                    end=end,
                    gap_type=GapType.EXPECTED_OBSERVATION,
                    status=GapStatus.OPEN,
                    blocking=True,
                    detected_at=now,
                    request_instance_id=str(request_instance_id),
                )
            )

        for gap in existing_gaps:
            if gap.stream_id != stream_id or not gap.actively_blocks:
                continue
            current_stream_segments = tuple(
                segment for segment in segments if segment.stream_id == stream_id
            )
            if _gap_is_fully_observed(
                gap,
                manifest=manifest,
                stream_id=stream_id,
                calendar_snapshot_id=calendar_snapshot_id,
                policy_snapshot_id=policy_snapshot_id,
                existing_segments=stream_existing,
                current_segments=current_stream_segments,
            ):
                changed_gaps.append(
                    transition_gap(
                        gap,
                        GapStatus.RESOLVED,
                        transitioned_at=now,
                    ).model_copy(update={"canonical_batch_id": manifest.canonical_batch_id})
                )

    if not segments:
        raise CoverageCommitBuildError(
            "a filesystem publication must contribute at least one observed coverage segment"
        )
    provisional_segments = tuple(
        sorted(
            segments,
            key=lambda value: value.coverage_id,
        )
    )
    all_segments = tuple(existing_segments) + tuple(
        segment for segment in provisional_segments if segment.coverage_id not in existing_by_id
    )
    all_gaps = _replace_gaps(existing_gaps, changed_gaps)

    # A bounded publication can extend the verified coverage hull beyond an
    # earlier contiguous frontier.  Reconstructing over the complete durable
    # hull is what reveals an eligible slot between two retained segments; the
    # current request's own eligible-slot list cannot contain that inter-batch
    # hole.  Only materialize NO_VALID_COVERAGE slots with verified support on
    # both sides, so an initial/later target boundary never becomes a gap merely
    # because it is outside this request.
    for outcome in manifest.streams:
        if outcome.outcome is not StreamPublicationOutcome.PUBLISHABLE:
            continue
        stream = outcome.stream
        stream_segments = tuple(
            segment for segment in all_segments if segment.stream_id == stream.stream_id
        )
        stream_gaps = tuple(gap for gap in all_gaps if gap.stream_id == stream.stream_id)
        evaluation = reconstruct_frontier(
            stream=stream,
            calendar_snapshot=manifest.calendar_snapshot,
            calendar_snapshot_id=calendar_snapshot_id,
            policy=policy,
            policy_snapshot_id=policy_snapshot_id,
            runtime_status=runtime_status,
            coverage_start=origin,
            domain_end=domain_end,
            coverage=stream_segments,
            gaps=stream_gaps,
            evaluated_at=now,
        )
        uncovered_without_finding = {
            (uncovered.slot.start_utc, uncovered.slot.end_utc)
            for uncovered in evaluation.uncovered_slots
            if uncovered.reason is MissingSlotReason.NO_VALID_COVERAGE
        }
        domain_slots = tuple(
            slot
            for slot in manifest.calendar_snapshot.expected_slots(stream.timeframe)
            if slot.start_utc >= origin and slot.end_utc <= domain_end
        )
        supported = tuple(
            any(
                _existing_segment_supports_slot(
                    segment,
                    stream_id=stream.stream_id,
                    start=slot.start_utc,
                    end=slot.end_utc,
                    calendar_snapshot_id=calendar_snapshot_id,
                    calendar_snapshot_checksum=manifest.calendar_snapshot.checksum,
                    policy_snapshot_id=policy_snapshot_id,
                )
                for segment in stream_segments
            )
            for slot in domain_slots
        )
        internal_missing = [
            index
            for index, slot in enumerate(domain_slots)
            if (slot.start_utc, slot.end_utc) in uncovered_without_finding
            and any(supported[:index])
            and any(supported[index + 1 :])
        ]
        for first, last in _contiguous_runs(internal_missing):
            start = domain_slots[first].start_utc
            end = domain_slots[last].end_utc
            identifier = _gap_id(
                stream_id=stream.stream_id,
                start=start,
                end=end,
                gap_type=GapType.EXPECTED_OBSERVATION,
                episode_id=str(request_instance_id),
            )
            changed_gaps.append(
                GapFinding(
                    gap_id=identifier,
                    stream_id=stream.stream_id,
                    start=start,
                    end=end,
                    gap_type=GapType.EXPECTED_OBSERVATION,
                    status=GapStatus.OPEN,
                    blocking=True,
                    detected_at=now,
                    request_instance_id=str(request_instance_id),
                )
            )

    terminal_state = _terminal_state(manifest, changed_gaps)
    ordered_segments = tuple(
        segment.model_copy(update={"request_terminal_state": terminal_state})
        for segment in provisional_segments
    )
    all_segments = tuple(existing_segments) + tuple(
        segment for segment in ordered_segments if segment.coverage_id not in existing_by_id
    )
    all_gaps = _replace_gaps(existing_gaps, changed_gaps)

    watermarks: list[MaterializedWatermark] = []
    for outcome in manifest.streams:
        if outcome.outcome is not StreamPublicationOutcome.PUBLISHABLE:
            continue
        stream = outcome.stream
        stream_segments = tuple(
            segment for segment in all_segments if segment.stream_id == stream.stream_id
        )
        stream_gaps = tuple(gap for gap in all_gaps if gap.stream_id == stream.stream_id)
        evaluation = reconstruct_frontier(
            stream=stream,
            calendar_snapshot=manifest.calendar_snapshot,
            calendar_snapshot_id=calendar_snapshot_id,
            policy=policy,
            policy_snapshot_id=policy_snapshot_id,
            runtime_status=runtime_status,
            coverage_start=origin,
            domain_end=domain_end,
            coverage=stream_segments,
            gaps=stream_gaps,
            evaluated_at=now,
        )
        watermark_candidate = evaluation.candidate
        if watermark_candidate is None:
            continue
        existing = existing_watermark_by_stream.get(stream.stream_id)
        current_batch_supports_frontier = (
            manifest.canonical_batch_id in watermark_candidate.supporting_batch_ids
        )
        retains_existing_frontier = (
            existing is not None
            and existing.verification_state is CoverageVerificationState.VERIFIED
            and existing.invalidated_at is None
            and existing.coverage_start == watermark_candidate.coverage_start
            and existing.exclusive_frontier <= watermark_candidate.exclusive_frontier
            and existing.calendar_snapshot_id == calendar_snapshot_id
            and existing.policy_snapshot_id == policy_snapshot_id
            and existing.last_verified_session == watermark_candidate.last_verified_session
            and existing.last_batch_id in watermark_candidate.supporting_batch_ids
        )
        if not current_batch_supports_frontier and not retains_existing_frontier:
            continue
        committed_candidate = watermark_candidate
        context_run_id = str(run_id)
        context_batch_id = manifest.canonical_batch_id
        if not current_batch_supports_frontier:
            if existing is None:  # pragma: no cover - narrowed by the proof above
                raise CoverageCommitBuildError(
                    "watermark evidence refresh lacks its durable predecessor"
                )
            context_run_id = existing.last_run_id
            context_batch_id = existing.last_batch_id
        if (
            existing is not None
            and existing.verification_state is CoverageVerificationState.VERIFIED
            and committed_candidate.exclusive_frontier < existing.exclusive_frontier
        ):
            # A newly discovered blocking gap is an invalidation event, not a
            # verified watermark that moves backward.  The operational commit
            # derives that invalidation atomically from the durable active gap.
            continue
        if (
            existing is not None
            and existing.verification_state is CoverageVerificationState.VERIFIED
            and existing.exclusive_frontier == committed_candidate.exclusive_frontier
            and existing.coverage_start == committed_candidate.coverage_start
            and existing.calendar_snapshot_id == calendar_snapshot_id
            and existing.policy_snapshot_id == policy_snapshot_id
            and existing.last_verified_session == committed_candidate.last_verified_session
            and existing.blocking_gap_count == committed_candidate.blocking_gap_count
        ):
            continue
        watermarks.append(
            materialize_watermark(
                committed_candidate,
                WatermarkChangeContext(
                    generation=1 if existing is None else existing.generation + 1,
                    last_run_id=context_run_id,
                    last_batch_id=context_batch_id,
                    computed_at=now,
                ),
            )
        )

    return CoverageCommit(
        calendar_snapshot_id=calendar_snapshot_id,
        policy_snapshot_id=policy_snapshot_id,
        segments=ordered_segments,
        gaps=tuple(
            sorted(
                {gap.gap_id: gap for gap in changed_gaps}.values(),
                key=lambda value: value.gap_id,
            )
        ),
        watermarks=tuple(sorted(watermarks, key=lambda value: value.stream_id)),
    )


__all__ = [
    "CoverageCommitBuildError",
    "build_blocking_acquisition_gaps",
    "build_blocking_integrity_gaps",
    "build_publication_coverage_commit",
    "build_semantic_noop_reconciliation",
]
