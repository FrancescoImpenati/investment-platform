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

from investment_platform.data.ingestion.coverage import (
    CoverageRequestTerminalState,
    CoverageSegment,
    CoverageStreamOutcome,
    GapFinding,
    GapStatus,
    GapType,
    MaterializedWatermark,
    WatermarkChangeContext,
    materialize_watermark,
    reconstruct_frontier,
    transition_gap,
)
from investment_platform.data.ingestion.planner import (
    CoverageClassification,
    CoverageVerificationState,
)
from investment_platform.data.market_time import to_utc
from investment_platform.data.operational.execution import CoverageCommit
from investment_platform.data.retention import DatasetRetentionPolicy, DatasetRuntimeStatus
from investment_platform.data.storage import (
    CanonicalBatchManifest,
    CanonicalParquetPart,
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


def _gap_id(*, stream_id: str, start: datetime, end: datetime, gap_type: GapType) -> str:
    digest = _canonical_hash(
        {
            "stream": stream_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "type": gap_type.value,
        }
    )
    return f"{_GAP_ID_VERSION}_{digest}"


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
    if starts and starts != {candidate}:
        raise CoverageCommitBuildError(
            "coverage_start is an authoritative stream origin and cannot be rebased"
        )
    return candidate


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


def _terminal_state(manifest: CanonicalBatchManifest) -> CoverageRequestTerminalState:
    if any(stream.outcome is StreamPublicationOutcome.BLOCKED for stream in manifest.streams):
        return CoverageRequestTerminalState.PARTIAL
    return CoverageRequestTerminalState.SUCCESS


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

    existing_by_id = {segment.coverage_id: segment for segment in existing_segments}
    existing_watermark_by_stream = {
        watermark.stream_id: watermark for watermark in existing_watermarks
    }
    if len(existing_watermark_by_stream) != len(existing_watermarks):
        raise CoverageCommitBuildError("existing watermark projection contains duplicates")

    segments: list[CoverageSegment] = []
    changed_gaps: list[GapFinding] = []
    terminal_state = _terminal_state(manifest)

    for outcome in manifest.streams:
        stream = outcome.stream
        stream_id = stream.stream_id
        if outcome.outcome is StreamPublicationOutcome.BLOCKED:
            identifier = _gap_id(
                stream_id=stream_id,
                start=outcome.request_start,
                end=outcome.request_end,
                gap_type=GapType.INTEGRITY,
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
                    gap_type=GapType.INTEGRITY,
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
                request_terminal_state=terminal_state,
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
        missing = [index for index in range(len(slots)) if index not in observed]
        for first, last in _contiguous_runs(missing):
            start = slots[first].start_utc
            end = slots[last].end_utc
            identifier = _gap_id(
                stream_id=stream_id,
                start=start,
                end=end,
                gap_type=GapType.EXPECTED_OBSERVATION,
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
    ordered_segments = tuple(sorted(segments, key=lambda value: value.coverage_id))
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
        if (
            watermark_candidate is None
            or manifest.canonical_batch_id not in watermark_candidate.supporting_batch_ids
        ):
            continue
        existing = existing_watermark_by_stream.get(stream.stream_id)
        if (
            existing is not None
            and existing.verification_state is CoverageVerificationState.VERIFIED
            and watermark_candidate.exclusive_frontier < existing.exclusive_frontier
        ):
            # A newly discovered blocking gap is an invalidation event, not a
            # verified watermark that moves backward.  The operational commit
            # derives that invalidation atomically from the durable active gap.
            continue
        if (
            existing is not None
            and existing.verification_state is CoverageVerificationState.VERIFIED
            and existing.exclusive_frontier == watermark_candidate.exclusive_frontier
            and existing.calendar_snapshot_id == calendar_snapshot_id
            and existing.policy_snapshot_id == policy_snapshot_id
        ):
            continue
        watermarks.append(
            materialize_watermark(
                watermark_candidate,
                WatermarkChangeContext(
                    generation=1 if existing is None else existing.generation + 1,
                    last_run_id=str(run_id),
                    last_batch_id=manifest.canonical_batch_id,
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
    "build_publication_coverage_commit",
]
