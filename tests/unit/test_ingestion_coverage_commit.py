"""Coverage commit derivation from verified canonical publication facts."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from uuid import UUID

from investment_platform.data.ingestion.coverage import (
    CoverageSegment,
    GapFinding,
    GapStatus,
    MaterializedWatermark,
)
from investment_platform.data.ingestion.identity import RequestSpecification
from investment_platform.data.ingestion.processing import PreparedCanonicalBatch
from investment_platform.data.operational.coverage_commit import (
    build_publication_coverage_commit,
)
from investment_platform.data.operational.execution import CoverageCommit
from investment_platform.data.operational.planning import deterministic_policy_snapshot_id
from investment_platform.data.retention import RetentionPolicyCatalog
from investment_platform.data.storage import CanonicalBatchManifest, CanonicalFileManifest
from tests.unit.test_ingestion_processing import (
    _A,
    _B,
    _NOW,
    _OPEN,
    _bar,
    _prepare,
    _specification,
)

_RUN_ID = UUID("50000000-0000-4000-8000-000000000001")
_REQUEST_ID = UUID("60000000-0000-4000-8000-000000000001")
_CALENDAR_ID = "calendar-synthetic-xnys"


def _manifest(prepared: PreparedCanonicalBatch) -> CanonicalBatchManifest:
    # Keep the fixture tied to the production Pydantic contract while avoiding
    # filesystem I/O: the publisher independently exercises real checksums.
    frame = prepared.parts[0].frame
    start = frame.get_column("timestamp_start").min()
    end = frame.get_column("timestamp_end").max()
    assert isinstance(start, datetime) and isinstance(end, datetime)
    expectation = prepared.expectation
    slots = tuple(
        slot
        for slot in expectation.calendar_snapshot.expected_slots(
            expectation.specification.timeframe
        )
        if slot.start_utc >= expectation.specification.start
        and slot.end_utc <= expectation.specification.end
    )
    return CanonicalBatchManifest(
        canonical_batch_id=prepared.batch_context.canonical_batch_id,
        batch_context_id=prepared.batch_context.batch_context_id,
        provider=expectation.specification.provider,
        dataset=expectation.specification.dataset,
        request_spec_hash=expectation.specification.request_spec_hash,
        ordered_raw_artifacts=prepared.batch_context.batch_identity.ordered_artifacts,
        processing_signature=prepared.batch_context.batch_identity.processing_signature,
        provenance=prepared.provenance,
        calendar_snapshot=expectation.calendar_snapshot,
        eligible_slots=slots,
        fixed_ingested_at=prepared.batch_context.fixed_ingested_at,
        manifest_created_at=prepared.batch_context.manifest_created_at,
        files=(
            CanonicalFileManifest(
                relative_path=prepared.parts[0].relative_path,
                sha256="a" * 64,
                byte_count=1,
                row_count=frame.height,
                schema_sha256="b" * 64,
                timestamp_start_min=start,
                timestamp_end_max=end,
            ),
        ),
        streams=prepared.stream_outcomes,
        row_count=frame.height,
    )


def _commit(
    prepared: PreparedCanonicalBatch,
    *,
    frontier_domain_end: datetime | None = None,
    existing_segments: Sequence[CoverageSegment] = (),
    existing_gaps: Sequence[GapFinding] = (),
    existing_watermarks: Sequence[MaterializedWatermark] = (),
) -> CoverageCommit:
    catalog = RetentionPolicyCatalog.load_default()
    policy = catalog.lookup("alpaca", "price_bars_sip")
    policy_snapshot = catalog.snapshot("alpaca", "price_bars_sip", captured_at=_NOW)
    return build_publication_coverage_commit(
        manifest=_manifest(prepared),
        parts=prepared.parts,
        calendar_snapshot_id=_CALENDAR_ID,
        policy_snapshot_id=deterministic_policy_snapshot_id(policy_snapshot),
        policy=policy,
        runtime_status=None,
        run_id=_RUN_ID,
        request_instance_id=_REQUEST_ID,
        coverage_start=_OPEN,
        frontier_domain_end=frontier_domain_end or _OPEN + timedelta(minutes=10),
        verified_at=_NOW + timedelta(minutes=3),
        existing_segments=existing_segments,
        existing_gaps=existing_gaps,
        existing_watermarks=existing_watermarks,
    )


def test_complete_observations_create_contiguous_coverage_and_watermark() -> None:
    prepared = _prepare(
        _specification(),
        {
            "XPH1": [
                _bar("2025-07-02T13:30:00Z"),
                _bar("2025-07-02T13:35:00Z"),
            ]
        },
    )

    commit = _commit(prepared)

    assert len(commit.segments) == 1
    assert commit.segments[0].row_count == 2
    assert commit.segments[0].coverage_start == _OPEN
    assert commit.gaps == ()
    assert len(commit.watermarks) == 1
    assert commit.watermarks[0].exclusive_frontier == _OPEN + timedelta(minutes=10)


def test_absent_alpaca_bar_is_blocking_gap_not_verified_empty() -> None:
    prepared = _prepare(
        _specification(),
        {"XPH1": [_bar("2025-07-02T13:30:00Z")]},
    )

    commit = _commit(prepared)

    assert len(commit.segments) == 1
    assert commit.segments[0].classification.value == "OBSERVED"
    assert len(commit.gaps) == 1
    assert commit.gaps[0].status is GapStatus.OPEN
    assert commit.gaps[0].start == _OPEN + timedelta(minutes=5)
    assert commit.watermarks[0].exclusive_frontier == _OPEN + timedelta(minutes=5)


def test_new_gap_below_existing_frontier_requires_invalidation_not_backward_watermark() -> None:
    complete = _prepare(
        _specification(),
        {
            "XPH1": [
                _bar("2025-07-02T13:30:00Z"),
                _bar("2025-07-02T13:35:00Z"),
            ]
        },
    )
    complete_commit = _commit(complete)
    correction = _prepare(
        _specification(),
        {"XPH1": [_bar("2025-07-02T13:30:00Z")]},
    )

    changed = _commit(
        correction,
        existing_segments=complete_commit.segments,
        existing_watermarks=complete_commit.watermarks,
    )

    assert len(changed.gaps) == 1
    assert changed.gaps[0].status is GapStatus.OPEN
    assert changed.watermarks == ()


def test_multi_stream_partial_request_updates_streams_independently() -> None:
    prepared = _prepare(
        _specification(mappings=((_A, "XPH1"), (_B, "XPH2"))),
        {
            "XPH1": [
                _bar("2025-07-02T13:30:00Z"),
                _bar("2025-07-02T13:35:00Z"),
            ],
            "XPH2": [],
        },
    )

    commit = _commit(prepared)

    assert len(commit.segments) == 1
    assert commit.segments[0].request_terminal_state.value == "PARTIAL"
    assert len(commit.gaps) == 1
    assert commit.gaps[0].gap_type.value == "INTEGRITY"
    assert commit.gaps[0].stream_id != commit.segments[0].stream_id
    assert len(commit.watermarks) == 1


def test_repair_resolves_gap_and_advances_from_existing_coverage() -> None:
    first = _prepare(
        _specification(),
        {"XPH1": [_bar("2025-07-02T13:30:00Z")]},
    )
    first_commit = _commit(first)

    base = _specification()
    repair_spec = RequestSpecification.model_validate(
        {
            **base.model_dump(mode="python"),
            "start": _OPEN + timedelta(minutes=5),
            "end": _OPEN + timedelta(minutes=10),
        }
    )
    repair = _prepare(
        repair_spec,
        {"XPH1": [_bar("2025-07-02T13:35:00Z")]},
    )
    repaired = _commit(
        repair,
        existing_segments=first_commit.segments,
        existing_gaps=first_commit.gaps,
        existing_watermarks=first_commit.watermarks,
    )

    assert repaired.segments[0].stream_id == first_commit.segments[0].stream_id
    assert repaired.gaps[0].status is GapStatus.RESOLVED
    assert repaired.gaps[0].canonical_batch_id == _manifest(repair).canonical_batch_id
    assert repaired.watermarks[0].exclusive_frontier == _OPEN + timedelta(minutes=10)
    assert repaired.watermarks[0].generation == 2


def test_partial_repair_does_not_resolve_unrequested_remainder_of_larger_gap() -> None:
    base = _specification()
    initial_spec = RequestSpecification.model_validate(
        {
            **base.model_dump(mode="python"),
            "end": _OPEN + timedelta(minutes=15),
        }
    )
    first = _prepare(
        initial_spec,
        {"XPH1": [_bar("2025-07-02T13:30:00Z")]},
    )
    first_commit = _commit(
        first,
        frontier_domain_end=_OPEN + timedelta(minutes=15),
    )
    assert first_commit.gaps[0].start == _OPEN + timedelta(minutes=5)
    assert first_commit.gaps[0].end == _OPEN + timedelta(minutes=15)

    partial_spec = RequestSpecification.model_validate(
        {
            **base.model_dump(mode="python"),
            "start": _OPEN + timedelta(minutes=5),
            "end": _OPEN + timedelta(minutes=10),
        }
    )
    partial = _prepare(
        partial_spec,
        {"XPH1": [_bar("2025-07-02T13:35:00Z")]},
    )
    partial_commit = _commit(
        partial,
        frontier_domain_end=_OPEN + timedelta(minutes=15),
        existing_segments=first_commit.segments,
        existing_gaps=first_commit.gaps,
        existing_watermarks=first_commit.watermarks,
    )

    assert partial_commit.gaps == ()
    assert partial_commit.watermarks == ()

    final_spec = RequestSpecification.model_validate(
        {
            **base.model_dump(mode="python"),
            "start": _OPEN + timedelta(minutes=10),
            "end": _OPEN + timedelta(minutes=15),
        }
    )
    final = _prepare(
        final_spec,
        {"XPH1": [_bar("2025-07-02T13:40:00Z")]},
    )
    final_commit = _commit(
        final,
        frontier_domain_end=_OPEN + timedelta(minutes=15),
        existing_segments=(*first_commit.segments, *partial_commit.segments),
        existing_gaps=first_commit.gaps,
        existing_watermarks=first_commit.watermarks,
    )

    assert final_commit.gaps[0].status is GapStatus.RESOLVED
    assert final_commit.watermarks[0].exclusive_frontier == _OPEN + timedelta(minutes=15)
