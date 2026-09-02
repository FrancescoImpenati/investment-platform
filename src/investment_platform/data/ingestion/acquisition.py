"""Bounded raw-page acquisition for the Phase 2 living-ingestion pipeline.

The provider adapter remains mode-agnostic.  This module converts one already
authorized request specification into its provider request, inspects every
transient response before persistence, and publishes only policy-authorized
bytes.  Exhausting the provider iterator plus observing its explicit terminal
response is the pagination-completion proof; receiving one page is deliberately
not sufficient.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import BinaryIO, Protocol, runtime_checkable
from uuid import UUID

from investment_platform.data.calendar import CalendarSnapshot
from investment_platform.data.ingestion.identity import (
    BarSemantics,
    DataKind,
    RawArtifactIdentity,
    RequestSpecification,
    canonical_page_relation,
)
from investment_platform.data.market_time import US_EASTERN, to_utc
from investment_platform.data.models import Timeframe, TradingSession
from investment_platform.data.normalization.common import (
    NormalizationError,
    require_matching_request_metadata,
)
from investment_platform.data.provenance import RawBatch
from investment_platform.data.providers.alpaca import ALPACA_SIP_BAR_SOURCE, AlpacaFeed
from investment_platform.data.providers.base import (
    BarRequest,
    ProviderInstrumentRef,
    instrument_refs_fingerprint,
    instrument_refs_manifest,
)
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    DatasetPolicySnapshot,
    DatasetRuntimeStatus,
    RequestPolicyAuthorization,
    ResponsePageAuthorization,
    RetentionPolicyEnforcer,
)
from investment_platform.data.storage import PublishedRawArtifact, RawArtifactPublisher
from investment_platform.data.storage._publication import FaultInjector
from investment_platform.data.storage.transport_spool import (
    TransportSpoolError,
    TransportSpoolFaultInjector,
    TransportSpoolPayload,
)

_FIVE_MINUTES = timedelta(minutes=5)
_DEFAULT_MAX_PAGE_BYTES = 64 * 1024 * 1024
_READ_CHUNK_BYTES = 1024 * 1024
_MAX_ACQUISITION_PAGES = 1000


class RawAcquisitionError(RuntimeError):
    """The bounded provider response could not become authorized raw evidence."""


class RawPageTooLargeError(RawAcquisitionError):
    """A provider page exceeded the explicit transient inspection ceiling."""


class RawPageInspectionError(RawAcquisitionError):
    """A transient response could not be proven to fit the authorized interval."""


class BarPageProvider(Protocol):
    """Small provider boundary needed by the acquisition service."""

    @property
    def provider_name(self) -> str: ...

    def get_bars(self, request: BarRequest) -> Iterator[RawBatch]: ...


@runtime_checkable
class AttemptScopedBarPageProvider(Protocol):
    """Optional provider capability for private-root file-backed transport."""

    def transport_attempt(
        self,
        attempt_id: UUID,
        *,
        fault_injector: TransportSpoolFaultInjector | None = None,
    ) -> AbstractContextManager[None]: ...


@dataclass(frozen=True, slots=True)
class InspectedRawPage:
    """Content facts established before any raw or quarantine persistence."""

    payload_sha256: str
    payload_size_bytes: int
    canonical_media_type: str
    content_encoding: str
    observed_start: datetime | None
    observed_end: datetime | None
    pagination_terminal: bool


@dataclass(frozen=True, slots=True)
class AcquiredRawPage:
    """One immutable raw page plus its transient attempt provenance."""

    page_ordinal: int
    raw_batch: RawBatch
    inspection: InspectedRawPage
    authorization: ResponsePageAuthorization
    identity: RawArtifactIdentity
    published: PublishedRawArtifact


@dataclass(frozen=True, slots=True)
class CompletedRawAcquisition:
    """A complete 0-based page chain whose terminal state was verified."""

    specification: RequestSpecification
    pages: tuple[AcquiredRawPage, ...]
    authorization: AcquisitionPolicyAuthorization

    @property
    def ordered_artifacts(self) -> tuple[RawArtifactIdentity, ...]:
        return tuple(page.identity for page in self.pages)


type PagePersistedHook = Callable[[AcquiredRawPage], None]
type BeforeDispatchHook = Callable[[int], None]
type PageInspector = Callable[
    [RawBatch, RequestSpecification, CalendarSnapshot],
    InspectedRawPage,
]


def _stable_policy_snapshot_identity(value: DatasetPolicySnapshot) -> tuple[object, ...]:
    return (
        value.catalog_id,
        value.catalog_revision,
        value.catalog_hash,
        value.policy_id,
        value.policy_revision,
        value.policy_hash,
        value.provider,
        value.dataset,
        value.mode,
        value.status,
        value.verified_on,
    )


def specification_to_bar_request(specification: RequestSpecification) -> BarRequest:
    """Translate the provider-neutral deterministic request at the provider boundary."""

    if (specification.provider, specification.dataset) != ("alpaca", "price_bars_sip"):
        raise RawAcquisitionError("Phase 2 acquisition requires exact historical Alpaca SIP bars")
    if specification.data_kind is not DataKind.PRICE_BAR:
        raise RawAcquisitionError("only price-bar request specifications are supported")
    if specification.session is not TradingSession.REGULAR:
        raise RawAcquisitionError("Phase 2 living ingestion supports regular sessions only")
    if specification.currency != "USD":
        raise RawAcquisitionError("Alpaca SIP US stock bars require the USD stream dimension")
    if specification.bar_semantics is not BarSemantics.PROVIDER_AGGREGATED_OHLCV:
        raise RawAcquisitionError("Alpaca SIP bars require provider-aggregated OHLCV semantics")
    if specification.additional_dimensions:
        raise RawAcquisitionError("unmapped additional stream dimensions are not supported")
    return BarRequest(
        instruments=tuple(
            ProviderInstrumentRef(
                instrument_id=mapping.instrument_id,
                provider_identifier=mapping.provider_identifier,
            )
            for mapping in specification.instrument_mappings
        ),
        timeframe=specification.timeframe,
        start=specification.start,
        end=specification.end,
        session=specification.session,
        adjustment_state=specification.adjustment,
    )


def _hash_bounded(reader: BinaryIO, *, max_bytes: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    while True:
        requested = min(_READ_CHUNK_BYTES, max_bytes + 1 - byte_count)
        chunk = reader.read(requested)
        if not isinstance(chunk, bytes) or len(chunk) > requested:
            raise RawPageInspectionError(
                "provider payload violated the bounded binary-reader contract"
            )
        if not chunk:
            return digest.hexdigest(), byte_count
        byte_count += len(chunk)
        if byte_count > max_bytes:
            raise RawPageTooLargeError(
                f"provider page exceeds the configured {max_bytes}-byte ceiling"
            )
        digest.update(chunk)


def _parse_provider_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise RawPageInspectionError("Alpaca bar timestamp must be an RFC3339 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return to_utc(parsed)
    except ValueError as error:
        raise RawPageInspectionError("Alpaca bar timestamp is not valid RFC3339") from error


def _canonical_observation_bounds(
    timestamp: datetime,
    *,
    timeframe: Timeframe,
    calendar_snapshot: CalendarSnapshot,
) -> tuple[datetime, datetime]:
    if timeframe is Timeframe.FIVE_MINUTES:
        session_date = timestamp.astimezone(US_EASTERN).date()
        session = next(
            (
                candidate
                for candidate in calendar_snapshot.sessions
                if candidate.session_date == session_date
            ),
            None,
        )
        end = timestamp + _FIVE_MINUTES
        if (
            session is None
            or timestamp < session.open_utc
            or end > session.close_utc
            or (timestamp - session.open_utc) % _FIVE_MINUTES
        ):
            raise RawPageInspectionError(
                "Alpaca 5m response is not an exact XNYS regular-session slot"
            )
        return timestamp, end
    if timeframe is not Timeframe.ONE_DAY:
        raise RawPageInspectionError("unsupported Alpaca historical-bar timeframe")
    session_date = timestamp.astimezone(US_EASTERN).date()
    session = next(
        (
            candidate
            for candidate in calendar_snapshot.sessions
            if candidate.session_date == session_date
        ),
        None,
    )
    if session is None:
        raise RawPageInspectionError(
            "Alpaca daily response references a session absent from the calendar snapshot"
        )
    return session.open_utc, session.close_utc


def inspect_alpaca_sip_bar_page(
    batch: RawBatch,
    specification: RequestSpecification,
    calendar_snapshot: CalendarSnapshot,
    *,
    max_page_bytes: int = _DEFAULT_MAX_PAGE_BYTES,
) -> InspectedRawPage:
    """Strictly inspect one Alpaca SIP page before it may be retained.

    This is intentionally stricter than normalization: every provider record
    must have a parseable in-range timestamp, even if a later recoverable value
    validation would flag the record.  Empty pages remain representable, but do
    not acquire ``VERIFIED_EMPTY`` semantics here.
    """

    if max_page_bytes <= 0:
        raise ValueError("max_page_bytes must be positive")
    if (specification.provider, specification.dataset) != ("alpaca", "price_bars_sip"):
        raise RawPageInspectionError("Alpaca SIP inspector requires the exact Phase 2 dataset")
    if (
        calendar_snapshot.calendar_name != "XNYS"
        or calendar_snapshot.timezone_name != "America/New_York"
    ):
        raise RawPageInspectionError("Alpaca SIP RTH inspection requires the XNYS calendar")
    if batch.metadata.source != ALPACA_SIP_BAR_SOURCE:
        raise RawPageInspectionError("provider response is not historical Alpaca SIP bars")
    if batch.metadata.media_type.strip().casefold() != "application/json":
        raise RawPageInspectionError("Alpaca historical bars require application/json")

    request = specification_to_bar_request(specification)
    try:
        require_matching_request_metadata(
            batch,
            {
                "instrument_count": len(request.instruments),
                "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
                "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
                "timeframe": request.timeframe.value,
                "start": request.start.isoformat(),
                "end_exclusive": request.end.isoformat(),
                "session": request.session.value,
                "adjustment_state": request.adjustment_state.value,
                "provider_adjustment": (
                    "raw" if request.adjustment_state.value == "unadjusted" else "split"
                ),
                "canonical_persistence_eligible": True,
                "feed": AlpacaFeed.SIP.value,
                "currency": "USD",
                "symbol_mapping_as_of": "-",
            },
            provider="alpaca",
            dataset="price_bars",
        )
    except NormalizationError as error:
        raise RawPageInspectionError(
            "Alpaca response metadata does not match the authorized request"
        ) from error
    try:
        with batch.payload.open_binary() as reader:
            payload_sha256, payload_size_bytes = _hash_bounded(
                reader,
                max_bytes=max_page_bytes,
            )
        if isinstance(batch.payload, TransportSpoolPayload) and (
            payload_sha256 != batch.payload.content_sha256
            or payload_size_bytes != batch.payload.byte_count
        ):
            raise RawPageInspectionError(
                "file-backed response changed after bounded transport completion"
            )
        # A second bounded resource pass lets the JSON decoder work directly
        # from a file-backed response.  The exact bytes are never assembled as
        # one Python ``bytes`` object; raw publication later rechecks the hash.
        with batch.payload.open_binary() as reader:
            root = json.load(reader)
    except RawPageTooLargeError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RawPageInspectionError("Alpaca historical-bar page is not valid JSON") from error
    if not isinstance(root, dict) or not all(isinstance(key, str) for key in root):
        raise RawPageInspectionError("Alpaca historical-bar page must be a JSON object")
    bars = root.get("bars")
    if not isinstance(bars, dict):
        raise RawPageInspectionError("Alpaca historical-bar page has no bars object")
    token = root.get("next_page_token")
    if token is not None and not isinstance(token, str):
        raise RawPageInspectionError("Alpaca next_page_token must be a string or null")

    allowed_identifiers = {
        mapping.provider_identifier for mapping in specification.instrument_mappings
    }
    observed: list[tuple[datetime, datetime]] = []
    for identifier, records in bars.items():
        if not isinstance(identifier, str) or identifier not in allowed_identifiers:
            raise RawPageInspectionError("Alpaca page contains an unrequested instrument")
        if not isinstance(records, list):
            raise RawPageInspectionError("Alpaca bars entries must contain arrays")
        for record in records:
            if not isinstance(record, dict):
                raise RawPageInspectionError("Alpaca bar record must be an object")
            start, end = _canonical_observation_bounds(
                _parse_provider_timestamp(record.get("t")),
                timeframe=specification.timeframe,
                calendar_snapshot=calendar_snapshot,
            )
            if start < specification.start or end > specification.end:
                raise RawPageInspectionError(
                    "Alpaca page contains an observation outside the bounded request"
                )
            observed.append((start, end))

    return InspectedRawPage(
        payload_sha256=payload_sha256,
        payload_size_bytes=payload_size_bytes,
        canonical_media_type="application/json",
        content_encoding="identity",
        observed_start=min((start for start, _ in observed), default=None),
        observed_end=max((end for _, end in observed), default=None),
        pagination_terminal=token is None or token == "",
    )


class RawAcquisitionService:
    """Execute one bounded request with at-least-once, idempotent raw effects."""

    def __init__(
        self,
        provider: BarPageProvider,
        publisher: RawArtifactPublisher,
        policy_enforcer: RetentionPolicyEnforcer,
        *,
        clock: Callable[[], datetime] | None = None,
        max_page_bytes: int = _DEFAULT_MAX_PAGE_BYTES,
        page_inspector: Callable[..., InspectedRawPage] = inspect_alpaca_sip_bar_page,
    ) -> None:
        if max_page_bytes <= 0:
            raise ValueError("max_page_bytes must be positive")
        self._provider = provider
        self._publisher = publisher
        self._policy_enforcer = policy_enforcer
        self._clock = clock or (lambda: datetime.now(UTC))
        self._max_page_bytes = max_page_bytes
        self._page_inspector = page_inspector

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise RawAcquisitionError("acquisition clock must return an aware datetime")
        return value.astimezone(UTC)

    def acquire(
        self,
        specification: RequestSpecification,
        request_authorization: RequestPolicyAuthorization,
        calendar_snapshot: CalendarSnapshot,
        *,
        runtime_status: DatasetRuntimeStatus | None = None,
        attempt_id: UUID | None = None,
        max_pages: int = _MAX_ACQUISITION_PAGES,
        max_calls: int = _MAX_ACQUISITION_PAGES,
        before_dispatch: BeforeDispatchHook | None = None,
        on_page_persisted: PagePersistedHook | None = None,
        fault_injector: FaultInjector | None = None,
        transport_fault_injector: TransportSpoolFaultInjector | None = None,
    ) -> CompletedRawAcquisition:
        """Consume the complete provider iterator and authorize its terminal page chain."""

        if max_pages <= 0 or max_calls <= 0:
            raise ValueError("acquisition page and call bounds must be positive")
        if self._provider.provider_name != specification.provider:
            raise RawAcquisitionError("provider adapter differs from the request specification")
        if request_authorization.request_spec_hash != specification.request_spec_hash:
            raise RawAcquisitionError("request authorization differs from the specification")

        # Re-evaluate the exact active policy immediately before any transport
        # scope can create a file or dispatch a request.  The response check
        # remains mandatory because the policy may still change after dispatch.
        current = self._policy_enforcer.authorize_request(
            specification.provider,
            specification.dataset,
            environment=request_authorization.environment,
            start=specification.start,
            end=specification.end,
            request_spec_hash=specification.request_spec_hash,
            runtime_status=runtime_status,
        )
        if (
            _stable_policy_snapshot_identity(current.policy_snapshot)
            != _stable_policy_snapshot_identity(request_authorization.policy_snapshot)
            or request_authorization.request_start != specification.start
            or request_authorization.request_end != specification.end
        ):
            raise RawAcquisitionError("request policy authorization is no longer current")

        request = specification_to_bar_request(specification)
        transport_scope: AbstractContextManager[None] = nullcontext()
        if attempt_id is not None and isinstance(
            self._provider,
            AttemptScopedBarPageProvider,
        ):
            transport_scope = self._provider.transport_attempt(
                attempt_id,
                fault_injector=transport_fault_injector,
            )
        try:
            with transport_scope:
                return self._acquire_pages(
                    specification,
                    request_authorization,
                    calendar_snapshot,
                    request,
                    max_pages=min(max_pages, _MAX_ACQUISITION_PAGES),
                    max_calls=min(max_calls, _MAX_ACQUISITION_PAGES),
                    runtime_status=runtime_status,
                    before_dispatch=before_dispatch,
                    on_page_persisted=on_page_persisted,
                    fault_injector=fault_injector,
                )
        except TransportSpoolError as error:
            raise RawAcquisitionError("transient provider response spool failed safely") from error

    def _acquire_pages(
        self,
        specification: RequestSpecification,
        request_authorization: RequestPolicyAuthorization,
        calendar_snapshot: CalendarSnapshot,
        request: BarRequest,
        *,
        max_pages: int,
        max_calls: int,
        runtime_status: DatasetRuntimeStatus | None,
        before_dispatch: BeforeDispatchHook | None,
        on_page_persisted: PagePersistedHook | None,
        fault_injector: FaultInjector | None,
    ) -> CompletedRawAcquisition:
        """Consume pages while an optional attempt-scoped transport is active."""

        pages: list[AcquiredRawPage] = []
        dispatch_bound = min(max_pages, max_calls)
        iterator = iter(self._provider.get_bars(request))
        # The iterator may raise after yielding a page (for example while
        # validating the next token).  In that case raw pages remain evidence,
        # but this method never creates a completion authorization.
        for page_ordinal in range(dispatch_bound):
            if pages and pages[-1].inspection.pagination_terminal:
                # A terminal provider token means the adapter's iterator must
                # already be locally exhausted.  Verify that contract without
                # claiming another network dispatch; Alpaca's generator returns
                # before issuing transport I/O when the token is absent.
                try:
                    next(iterator)
                except StopIteration:
                    break
                raise RawAcquisitionError("provider yielded a page after pagination termination")
            if before_dispatch is not None:
                before_dispatch(page_ordinal)
            try:
                batch = next(iterator)
            except StopIteration:
                break
            try:
                require_matching_request_metadata(
                    batch,
                    {"page_number": page_ordinal + 1},
                    provider="alpaca",
                    dataset="price_bars",
                )
            except NormalizationError as error:
                raise RawPageInspectionError(
                    "Alpaca response page number does not match the deterministic page chain"
                ) from error
            inspection = self._page_inspector(
                batch,
                specification,
                calendar_snapshot,
                max_page_bytes=self._max_page_bytes,
            )
            relation = canonical_page_relation(page_ordinal)
            page_authorization = self._policy_enforcer.authorize_response_page(
                request_authorization,
                page_ordinal=page_ordinal,
                page_relation=relation,
                payload_sha256=inspection.payload_sha256,
                payload_size_bytes=inspection.payload_size_bytes,
                canonical_media_type=inspection.canonical_media_type,
                content_encoding=inspection.content_encoding,
                observed_start=inspection.observed_start,
                observed_end=inspection.observed_end,
                runtime_status=runtime_status,
            )
            identity = RawArtifactIdentity.from_digest(
                request_spec_hash=specification.request_spec_hash,
                page_ordinal=page_ordinal,
                page_relation=relation,
                media_type=inspection.canonical_media_type,
                content_encoding=inspection.content_encoding,
                content_sha256=inspection.payload_sha256,
                byte_count=inspection.payload_size_bytes,
            )
            first_persisted_at = max(
                self._now(),
                page_authorization.authorized_at,
                batch.metadata.retrieved_at,
            )
            with batch.payload.open_binary() as payload:
                published = self._publisher.publish(
                    specification,
                    payload,
                    page_ordinal=page_ordinal,
                    media_type=inspection.canonical_media_type,
                    content_encoding=inspection.content_encoding,
                    authorization=page_authorization,
                    first_persisted_at=first_persisted_at,
                    runtime_status=runtime_status,
                    fault_injector=fault_injector,
                )
            acquired = AcquiredRawPage(
                page_ordinal=page_ordinal,
                raw_batch=batch,
                inspection=inspection,
                authorization=page_authorization,
                identity=identity,
                published=published,
            )
            pages.append(acquired)
            if on_page_persisted is not None:
                on_page_persisted(acquired)
            if len(pages) == dispatch_bound and not inspection.pagination_terminal:
                # Do not probe the iterator for another page: for a network-backed
                # provider ``next()`` is itself the next dispatch boundary.
                raise RawAcquisitionError(
                    "provider pagination exceeded the deterministic request page/call bound"
                )

        if not pages:
            raise RawAcquisitionError("provider returned no response page")
        if not pages[-1].inspection.pagination_terminal:
            raise RawAcquisitionError(
                "provider iterator ended before a terminal pagination response"
            )
        authorization = self._policy_enforcer.authorize_completed_acquisition(
            request_authorization,
            tuple(page.authorization for page in pages),
            pagination_complete=True,
            terminal_page_verified=True,
            runtime_status=runtime_status,
        )
        return CompletedRawAcquisition(
            specification=specification,
            pages=tuple(pages),
            authorization=authorization,
        )


__all__ = [
    "AcquiredRawPage",
    "AttemptScopedBarPageProvider",
    "BarPageProvider",
    "BeforeDispatchHook",
    "CompletedRawAcquisition",
    "InspectedRawPage",
    "PageInspector",
    "PagePersistedHook",
    "RawAcquisitionError",
    "RawAcquisitionService",
    "RawPageInspectionError",
    "RawPageTooLargeError",
    "inspect_alpaca_sip_bar_page",
    "specification_to_bar_request",
]
