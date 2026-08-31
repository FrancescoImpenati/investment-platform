"""Offline tests for bounded, retention-aware raw acquisition."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from investment_platform.data.calendar import CalendarSession, CalendarSnapshot
from investment_platform.data.ingestion.acquisition import (
    RawAcquisitionError,
    RawAcquisitionService,
    RawPageInspectionError,
    RawPageTooLargeError,
    inspect_alpaca_sip_bar_page,
    specification_to_bar_request,
)
from investment_platform.data.ingestion.identity import (
    BarSemantics,
    DataKind,
    ProviderInstrumentMapping,
    RequestSpecification,
)
from investment_platform.data.ingestion.processing import (
    CanonicalProcessingError,
    processing_pages_from_acquisition,
)
from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.provenance import RawBatch
from investment_platform.data.providers import AlpacaCredentials, AlpacaFeed, AlpacaProvider
from investment_platform.data.providers.base import BarRequest
from investment_platform.data.providers.errors import ProviderResponseError
from investment_platform.data.providers.http import HttpResponse
from investment_platform.data.retention import (
    RequestPolicyAuthorization,
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage import RawArtifactPublisher
from investment_platform.data_root import PrivateDataRoot
from investment_platform.runtime import RuntimeEnvironment
from tests.provider_fakes import QueueHttpTransport

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
_START = datetime(2025, 7, 2, 13, 30, tzinfo=UTC)
_END = datetime(2025, 7, 2, 13, 40, tzinfo=UTC)
_INSTRUMENT_ID = UUID("10000000-0000-4000-8000-000000000001")


class _StaticPageProvider:
    provider_name = "alpaca"

    def __init__(self, *batches: RawBatch) -> None:
        self._batches = batches

    def get_bars(self, request: BarRequest) -> Iterator[RawBatch]:
        del request
        yield from self._batches


def _payload(timestamp: datetime, *, next_token: str | None) -> bytes:
    return json.dumps(
        {
            "bars": {
                "XPH1": [
                    {
                        "t": timestamp.isoformat().replace("+00:00", "Z"),
                        "o": 100.0,
                        "h": 101.0,
                        "l": 99.0,
                        "c": 100.5,
                        "v": 1000,
                        "vw": 100.25,
                    }
                ]
            },
            "next_page_token": next_token,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _specification() -> RequestSpecification:
    return RequestSpecification(
        provider="alpaca",
        dataset="price_bars_sip",
        data_kind=DataKind.PRICE_BAR,
        instrument_mappings=(
            ProviderInstrumentMapping(
                instrument_id=_INSTRUMENT_ID,
                provider_identifier="XPH1",
            ),
        ),
        timeframe=Timeframe.FIVE_MINUTES,
        session=TradingSession.REGULAR,
        adjustment=AdjustmentState.UNADJUSTED,
        currency="USD",
        bar_semantics=BarSemantics.PROVIDER_AGGREGATED_OHLCV,
        start=_START,
        end=_END,
        mapping_semantic_version="alpaca-sip-bars-v1",
    )


def _calendar() -> CalendarSnapshot:
    return CalendarSnapshot.create(
        library_name="synthetic-calendar",
        library_version="1",
        tzdata_version="synthetic",
        calendar_name="XNYS",
        timezone_name="America/New_York",
        range_start=_START.date(),
        range_end=_START.date() + timedelta(days=1),
        generated_at=_NOW,
        sessions=(
            CalendarSession(
                session_date=_START.date(),
                open_utc=_START,
                close_utc=datetime(2025, 7, 2, 20, 0, tzinfo=UTC),
            ),
        ),
    )


def _root(tmp_path: Path) -> PrivateDataRoot:
    repository = Path(__file__).parents[2].resolve(strict=True)
    root = PrivateDataRoot(
        tmp_path.parent / f"p2-acquisition-{uuid4().hex[:8]}",
        repository,
        allow_temporary_for_tests=True,
    )
    root.initialize(created_at=_NOW)
    return root


def _provider(responses: list[HttpResponse]) -> AlpacaProvider:
    batch_ids = iter(
        (
            UUID("30000000-0000-4000-8000-000000000001"),
            UUID("30000000-0000-4000-8000-000000000002"),
            UUID("30000000-0000-4000-8000-000000000003"),
        )
    )
    return AlpacaProvider(
        AlpacaCredentials("synthetic-id", "synthetic-secret"),
        feed=AlpacaFeed.SIP,
        transport=QueueHttpTransport(responses),
        clock=lambda: _NOW,
        batch_id_factory=batch_ids.__next__,
    )


def _raw_batch(payload: bytes) -> RawBatch:
    return next(
        _provider([HttpResponse(200, payload)]).get_bars(
            specification_to_bar_request(_specification())
        )
    )


def _service_for_provider(
    root: PrivateDataRoot,
    provider: _StaticPageProvider,
) -> tuple[RawAcquisitionService, RetentionPolicyEnforcer]:
    enforcer = RetentionPolicyEnforcer(
        RetentionPolicyCatalog.load_default(),
        clock=lambda: _NOW,
    )
    return (
        RawAcquisitionService(
            provider,
            RawArtifactPublisher(root, enforcer),
            enforcer,
            clock=lambda: _NOW,
        ),
        enforcer,
    )


def _service(
    root: PrivateDataRoot,
    responses: list[HttpResponse],
    *,
    max_page_bytes: int = 64 * 1024 * 1024,
) -> tuple[RawAcquisitionService, RetentionPolicyEnforcer]:
    enforcer = RetentionPolicyEnforcer(
        RetentionPolicyCatalog.load_default(),
        clock=lambda: _NOW,
    )
    return (
        RawAcquisitionService(
            _provider(responses),
            RawArtifactPublisher(root, enforcer),
            enforcer,
            clock=lambda: _NOW,
            max_page_bytes=max_page_bytes,
        ),
        enforcer,
    )


def _authorization(
    enforcer: RetentionPolicyEnforcer,
    specification: RequestSpecification,
) -> RequestPolicyAuthorization:
    return enforcer.authorize_request(
        specification.provider,
        specification.dataset,
        environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        start=specification.start,
        end=specification.end,
        request_spec_hash=specification.request_spec_hash,
    )


def test_complete_pagination_publishes_ordered_raw_pages(tmp_path: Path) -> None:
    root = _root(tmp_path)
    first = _payload(_START, next_token="SYNTHETIC_PAGE_2")
    second = _payload(_START + timedelta(minutes=5), next_token=None)
    service, enforcer = _service(
        root,
        [HttpResponse(200, first), HttpResponse(200, second)],
    )
    specification = _specification()
    observed: list[int] = []

    completed = service.acquire(
        specification,
        _authorization(enforcer, specification),
        _calendar(),
        on_page_persisted=lambda page: observed.append(page.page_ordinal),
    )

    assert observed == [0, 1]
    assert tuple(page.identity.page_relation for page in completed.pages) == ("root", "after:0")
    assert tuple(page.published.created for page in completed.pages) == (True, True)
    assert completed.authorization.pagination_complete is True
    assert completed.authorization.terminal_page_verified is True
    assert tuple(page.inspection.pagination_terminal for page in completed.pages) == (False, True)
    assert len(tuple((root.root / "raw").rglob("payload.bin"))) == 2

    replay_service, replay_enforcer = _service(
        root,
        [HttpResponse(200, first), HttpResponse(200, second)],
    )
    replay = replay_service.acquire(
        specification,
        _authorization(replay_enforcer, specification),
        _calendar(),
    )
    assert tuple(page.published.created for page in replay.pages) == (False, False)
    assert len(tuple((root.root / "raw").rglob("payload.bin"))) == 2
    with pytest.raises(CanonicalProcessingError, match="operational-catalog replay provenance"):
        processing_pages_from_acquisition(replay)


def test_iterator_failure_never_claims_complete_pagination(tmp_path: Path) -> None:
    root = _root(tmp_path)
    repeated = _payload(_START, next_token="REPEATED")
    service, enforcer = _service(
        root,
        [HttpResponse(200, repeated), HttpResponse(200, repeated)],
    )
    specification = _specification()

    with pytest.raises(ProviderResponseError, match="repeated next_page_token"):
        service.acquire(
            specification,
            _authorization(enforcer, specification),
            _calendar(),
        )

    # The two stable page relations remain distinct raw evidence, but there is
    # no CompletedRawAcquisition and therefore no coverage/watermark proof.
    assert len(tuple((root.root / "raw").rglob("payload.bin"))) == 2


def test_out_of_bounds_response_is_rejected_before_raw_persistence(tmp_path: Path) -> None:
    root = _root(tmp_path)
    outside = _payload(_END, next_token=None)
    service, enforcer = _service(root, [HttpResponse(200, outside)])
    specification = _specification()

    with pytest.raises(RawPageInspectionError, match="outside the bounded request"):
        service.acquire(
            specification,
            _authorization(enforcer, specification),
            _calendar(),
        )

    assert not tuple((root.root / "raw").rglob("payload.bin"))


def test_transient_page_size_is_hard_bounded(tmp_path: Path) -> None:
    root = _root(tmp_path)
    payload = _payload(_START, next_token=None)
    service, enforcer = _service(
        root,
        [HttpResponse(200, payload)],
        max_page_bytes=len(payload) - 1,
    )
    specification = _specification()

    with pytest.raises(RawPageTooLargeError, match="byte ceiling"):
        service.acquire(
            specification,
            _authorization(enforcer, specification),
            _calendar(),
        )
    assert not tuple((root.root / "raw").rglob("payload.bin"))


def test_empty_page_is_inspectable_but_does_not_assert_verified_empty() -> None:
    batch = next(
        _provider([HttpResponse(200, b'{"bars":{},"next_page_token":null}')]).get_bars(
            specification_to_bar_request(_specification())
        )
    )

    inspected = inspect_alpaca_sip_bar_page(batch, _specification(), _calendar())

    assert inspected.observed_start is None
    assert inspected.observed_end is None
    assert inspected.pagination_terminal is True


def test_iterator_exhaustion_without_terminal_page_is_not_complete(tmp_path: Path) -> None:
    root = _root(tmp_path)
    batch = _raw_batch(_payload(_START, next_token="UNFOLLOWED"))
    service, enforcer = _service_for_provider(root, _StaticPageProvider(batch))
    specification = _specification()

    with pytest.raises(RawAcquisitionError, match="before a terminal pagination response"):
        service.acquire(
            specification,
            _authorization(enforcer, specification),
            _calendar(),
        )

    assert len(tuple((root.root / "raw").rglob("payload.bin"))) == 1


def test_page_after_terminal_response_is_rejected_before_persistence(tmp_path: Path) -> None:
    root = _root(tmp_path)
    terminal = _raw_batch(_payload(_START, next_token=None))
    extra = _raw_batch(_payload(_START + timedelta(minutes=5), next_token=None))
    service, enforcer = _service_for_provider(root, _StaticPageProvider(terminal, extra))
    specification = _specification()

    with pytest.raises(RawAcquisitionError, match="after pagination termination"):
        service.acquire(
            specification,
            _authorization(enforcer, specification),
            _calendar(),
        )

    assert len(tuple((root.root / "raw").rglob("payload.bin"))) == 1


def test_request_mapping_metadata_is_bound_before_raw_persistence(tmp_path: Path) -> None:
    root = _root(tmp_path)
    original = _raw_batch(_payload(_START, next_token=None))
    metadata = original.metadata.model_copy(
        update={
            "request_metadata": {
                **original.metadata.request_metadata,
                "instrument_count": 2,
            }
        }
    )
    service, enforcer = _service_for_provider(
        root,
        _StaticPageProvider(RawBatch(metadata=metadata, payload=original.payload)),
    )
    specification = _specification()

    with pytest.raises(RawPageInspectionError, match="metadata does not match"):
        service.acquire(
            specification,
            _authorization(enforcer, specification),
            _calendar(),
        )

    assert not tuple((root.root / "raw").rglob("payload.bin"))


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"currency": "EUR"}, "USD stream dimension"),
        (
            {"bar_semantics": BarSemantics.CANONICAL_SESSION_OHLCV},
            "provider-aggregated OHLCV semantics",
        ),
    ],
)
def test_unmapped_stream_dimensions_fail_before_provider_dispatch(
    update: dict[str, object],
    message: str,
) -> None:
    specification = _specification().model_copy(update=update)

    with pytest.raises(RawAcquisitionError, match=message):
        specification_to_bar_request(specification)
