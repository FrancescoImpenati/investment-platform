"""Alpaca Market Data adapter with explicit SIP/IEX feed provenance."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from urllib.parse import quote
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.provenance import DataSource, LicenseClassification, RawBatch
from investment_platform.data.providers._support import (
    BatchIdFactory,
    Clock,
    new_batch_id,
    parse_json_response,
    raw_batch_from_response,
    require_success,
    safe_numeric_header,
    utc_now,
)
from investment_platform.data.providers.base import (
    BarRequest,
    CorporateActionRequest,
    ProviderInstrumentRef,
    instrument_refs_fingerprint,
    instrument_refs_manifest,
)
from investment_platform.data.providers.errors import (
    ProviderCapabilityError,
    ProviderConfigurationError,
    ProviderEntitlementError,
    ProviderResponseError,
)
from investment_platform.data.providers.http import (
    AttemptScopedHttpTransport,
    HttpResponse,
    HttpTransport,
    QueryParameters,
    UrllibHttpTransport,
)
from investment_platform.data.storage.transport_spool import TransportSpoolFaultInjector

_PROVIDER = "alpaca"
_DEFAULT_DATA_BASE_URL = "https://data.alpaca.markets"
_DEFAULT_TRADING_BASE_URL = "https://paper-api.alpaca.markets"
_NEW_YORK = ZoneInfo("America/New_York")


class AlpacaFeed(StrEnum):
    """US equity feeds are different datasets and never implicit fallbacks."""

    SIP = "sip"
    IEX = "iex"


class AlpacaEvidenceAdjustment(StrEnum):
    """Provider-only adjustment probes that are deliberately not canonical series."""

    DIVIDEND = "dividend"
    ALL = "all"


class AlpacaSipPreflightOutcome(StrEnum):
    """Sanitized classifications for the one-request entitlement probe."""

    AUTHORIZED = "authorized"
    ENTITLEMENT_DENIED = "entitlement_denied"
    AUTHENTICATION_FAILED = "authentication_failed"
    AUTH_OR_PERMISSION_DENIED = "authentication_or_permission_denied"
    RATE_LIMITED = "rate_limited"
    HTTP_ERROR = "http_error"
    MALFORMED_RESPONSE = "malformed_response"


ALPACA_SIP_BAR_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:alpaca:stocks:bars:sip"),
    provider=_PROVIDER,
    dataset="price_bars_sip",
    logical_endpoint="v2/stocks/bars",
    license_classification=LicenseClassification.PRIVATE,
)
ALPACA_IEX_BAR_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:alpaca:stocks:bars:iex"),
    provider=_PROVIDER,
    dataset="price_bars_iex",
    logical_endpoint="v2/stocks/bars",
    license_classification=LicenseClassification.PRIVATE,
)
ALPACA_INSTRUMENT_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:alpaca:stocks:assets"),
    provider=_PROVIDER,
    dataset="instruments",
    logical_endpoint="v2/assets",
    license_classification=LicenseClassification.PRIVATE,
)
ALPACA_CORPORATE_ACTION_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:alpaca:stocks:corporate-actions"),
    provider=_PROVIDER,
    dataset="corporate_actions",
    logical_endpoint="v1/corporate-actions",
    license_classification=LicenseClassification.PRIVATE,
)


@dataclass(frozen=True, slots=True)
class AlpacaCredentials:
    """Alpaca key pair whose representation never reveals either value."""

    key_id: str = field(repr=False)
    secret_key: str = field(repr=False)

    def __post_init__(self) -> None:
        key_id = self.key_id.strip()
        secret_key = self.secret_key.strip()
        if not key_id or not secret_key:
            raise ProviderConfigurationError(
                _PROVIDER,
                "configuration",
                "APCA_API_KEY_ID and APCA_API_SECRET_KEY are both required",
            )
        object.__setattr__(self, "key_id", key_id)
        object.__setattr__(self, "secret_key", secret_key)

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> AlpacaCredentials:
        values = os.environ if environ is None else environ
        return cls(
            key_id=values.get("APCA_API_KEY_ID", ""),
            secret_key=values.get("APCA_API_SECRET_KEY", ""),
        )


@dataclass(frozen=True, slots=True)
class AlpacaSipPreflightResult:
    """Non-persistent, sanitized result of one historical SIP request."""

    outcome: AlpacaSipPreflightOutcome
    status_code: int
    requested_feed: AlpacaFeed
    timeframe: Timeframe
    start: datetime
    end_exclusive: datetime
    instrument_count: int
    observation_count: int | None
    checked_at: datetime
    rate_limit_capacity: int | float | None
    rate_limit_remaining: int | float | None
    rate_limit_reset: int | float | None
    raw_retention_authorized: bool | None


def _rfc3339(value: datetime) -> str:
    rendered = value.isoformat(timespec="microseconds")
    return rendered.replace("+00:00", "Z")


def _inclusive_end(request: BarRequest) -> datetime:
    return _inclusive_end_for_bounds(request.start, request.end)


def _inclusive_end_for_bounds(start: datetime, end: datetime) -> datetime:
    inclusive = end - timedelta(microseconds=1)
    if inclusive < start:
        raise ProviderCapabilityError(
            _PROVIDER,
            "price_bars",
            "requested interval is shorter than Alpaca timestamp precision",
        )
    return inclusive


def _provider_bar_query_bounds(request: BarRequest) -> tuple[datetime, datetime]:
    """Map canonical RTH bounds to Alpaca's provider timestamp convention.

    Alpaca daily stock bars are labeled at midnight America/New_York.  The
    deterministic request identity remains bounded by the canonical XNYS
    session open/close; only the wire query expands to the midnight envelope
    containing the same first and last session dates.
    """

    if request.timeframe is not Timeframe.ONE_DAY:
        return request.start, request.end
    first_session_date = request.start.astimezone(_NEW_YORK).date()
    last_session_date = (request.end - timedelta(microseconds=1)).astimezone(
        _NEW_YORK
    ).date()
    provider_start = datetime.combine(
        first_session_date,
        time.min,
        tzinfo=_NEW_YORK,
    ).astimezone(UTC)
    provider_end = datetime.combine(
        last_session_date + timedelta(days=1),
        time.min,
        tzinfo=_NEW_YORK,
    ).astimezone(UTC)
    return provider_start, provider_end


def _timeframe(timeframe: Timeframe) -> str:
    if timeframe is Timeframe.ONE_DAY:
        return "1Day"
    if timeframe is Timeframe.FIVE_MINUTES:
        return "5Min"
    raise ProviderCapabilityError(_PROVIDER, "price_bars", f"unsupported timeframe {timeframe}")


def _adjustment(adjustment: AdjustmentState) -> str:
    if adjustment is AdjustmentState.UNADJUSTED:
        return "raw"
    if adjustment is AdjustmentState.SPLIT_ADJUSTED:
        return "split"
    raise ProviderCapabilityError(
        _PROVIDER,
        "price_bars",
        f"Alpaca cannot map canonical adjustment state {adjustment.value} explicitly",
    )


def _joined_identifiers(
    instruments: tuple[ProviderInstrumentRef, ...],
    *,
    dataset: str,
) -> str:
    identifiers = [instrument.provider_identifier for instrument in instruments]
    if any(
        "," in value or any(ord(character) < 32 or ord(character) == 127 for character in value)
        for value in identifiers
    ):
        raise ProviderCapabilityError(
            _PROVIDER,
            dataset,
            "provider identifiers must not contain commas or control characters",
        )
    return ",".join(identifiers)


class AlpacaProvider:
    """Synchronous Alpaca adapter with explicit feed identity and no feed fallback."""

    def __init__(
        self,
        credentials: AlpacaCredentials,
        *,
        feed: AlpacaFeed = AlpacaFeed.SIP,
        transport: HttpTransport | None = None,
        data_base_url: str = _DEFAULT_DATA_BASE_URL,
        trading_base_url: str = _DEFAULT_TRADING_BASE_URL,
        timeout_seconds: float = 30.0,
        clock: Clock = utc_now,
        batch_id_factory: BatchIdFactory = new_batch_id,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if data_base_url != _DEFAULT_DATA_BASE_URL:
            raise ValueError("Alpaca market-data credentials require the official data origin")
        if trading_base_url != _DEFAULT_TRADING_BASE_URL:
            raise ValueError("Alpaca asset credentials require the official paper API origin")
        self._credentials = credentials
        self._feed = feed
        self._transport = transport or UrllibHttpTransport()
        self._data_base_url = data_base_url
        self._trading_base_url = trading_base_url
        self._timeout_seconds = timeout_seconds
        self._clock = clock
        self._batch_id_factory = batch_id_factory

    @classmethod
    def from_environment(
        cls,
        *,
        feed: AlpacaFeed = AlpacaFeed.SIP,
        transport: HttpTransport | None = None,
    ) -> AlpacaProvider:
        return cls(
            AlpacaCredentials.from_environment(),
            feed=feed,
            transport=transport,
        )

    @property
    def provider_name(self) -> str:
        return _PROVIDER

    @property
    def feed(self) -> AlpacaFeed:
        return self._feed

    def transport_attempt(
        self,
        attempt_id: UUID,
        *,
        fault_injector: TransportSpoolFaultInjector | None = None,
    ) -> AbstractContextManager[None]:
        """Bind file-backed transport to one attempt; legacy fakes need no scope."""

        if isinstance(self._transport, AttemptScopedHttpTransport):
            return self._transport.attempt_scope(
                attempt_id,
                fault_injector=fault_injector,
            )
        return nullcontext()

    def _headers(self) -> Mapping[str, str]:
        return {
            "APCA-API-KEY-ID": self._credentials.key_id,
            "APCA-API-SECRET-KEY": self._credentials.secret_key,
        }

    def _get(
        self,
        *,
        dataset: str,
        base_url: str,
        path: str,
        query: QueryParameters,
    ) -> HttpResponse:
        response = self._transport.get(
            provider=_PROVIDER,
            dataset=dataset,
            base_url=base_url,
            path=path,
            query=query,
            headers=self._headers(),
            timeout_seconds=self._timeout_seconds,
        )
        if response.status_code == 422:
            try:
                provider_error = parse_json_response(_PROVIDER, dataset, response)
            except ProviderResponseError:
                provider_error = {}
            if provider_error.get("code") == 42210000:
                raise ProviderEntitlementError(
                    _PROVIDER,
                    dataset,
                    status_code=response.status_code,
                )
        require_success(_PROVIDER, dataset, response)
        return response

    def _pages(
        self,
        *,
        dataset: str,
        source: DataSource,
        path: str,
        query: QueryParameters,
        request_metadata: Mapping[str, str | int | float | bool | None],
    ) -> Iterator[RawBatch]:
        base_query = tuple((key, value) for key, value in query if key != "page_token")
        page_token: str | None = None
        seen_tokens: set[str] = set()
        page_number = 1
        while True:
            current_query = base_query
            if page_token is not None:
                current_query += (("page_token", page_token),)
            response = self._get(
                dataset=dataset,
                base_url=self._data_base_url,
                path=path,
                query=current_query,
            )
            batch = raw_batch_from_response(
                source=source,
                response=response,
                request_metadata={**request_metadata, "page_number": page_number},
                clock=self._clock,
                batch_id_factory=self._batch_id_factory,
            )
            yield batch
            parsed = parse_json_response(_PROVIDER, dataset, response)
            token_value = parsed.get("next_page_token")
            if token_value is None or token_value == "":
                return
            if not isinstance(token_value, str):
                raise ProviderResponseError(
                    _PROVIDER,
                    dataset,
                    "next_page_token must be a string or null",
                )
            if token_value in seen_tokens:
                raise ProviderResponseError(
                    _PROVIDER,
                    dataset,
                    "pagination returned a repeated next_page_token",
                )
            seen_tokens.add(token_value)
            page_token = token_value
            page_number += 1
            if page_number > 1000:
                raise ProviderResponseError(
                    _PROVIDER,
                    dataset,
                    "pagination exceeded the 1000-page safety bound",
                )

    def get_instruments(self, *, as_of: date | None = None) -> Iterator[RawBatch]:
        if as_of is not None:
            raise ProviderCapabilityError(
                _PROVIDER,
                "instruments",
                "the Alpaca assets endpoint has no point-in-time as_of filter",
            )
        response = self._get(
            dataset="instruments",
            base_url=self._trading_base_url,
            path="/v2/assets",
            query=(("status", "active"), ("asset_class", "us_equity")),
        )
        yield raw_batch_from_response(
            source=ALPACA_INSTRUMENT_SOURCE,
            response=response,
            request_metadata={"as_of": None},
            clock=self._clock,
            batch_id_factory=self._batch_id_factory,
        )

    def get_instrument(self, provider_identifier: str) -> Iterator[RawBatch]:
        """Retrieve one current asset without claiming point-in-time identifier validity."""

        identifier = provider_identifier.strip()
        if not identifier:
            raise ValueError("provider_identifier must not be blank")
        response = self._get(
            dataset="instruments",
            base_url=self._trading_base_url,
            path=f"/v2/assets/{quote(identifier, safe='')}",
            query=(),
        )
        yield raw_batch_from_response(
            source=ALPACA_INSTRUMENT_SOURCE,
            response=response,
            request_metadata={
                "provider_identifier": identifier,
                "as_of": None,
                "lookup_scope": "single_instrument",
            },
            clock=self._clock,
            batch_id_factory=self._batch_id_factory,
        )

    def _bar_pages(
        self,
        request: BarRequest,
        *,
        symbol_mapping_as_of: str,
        provider_adjustment: str,
        canonical_persistence_eligible: bool,
    ) -> Iterator[RawBatch]:
        if request.session not in {TradingSession.REGULAR, TradingSession.UNKNOWN}:
            raise ProviderCapabilityError(
                _PROVIDER,
                "price_bars",
                f"session {request.session.value} is not explicitly supported",
            )
        identifiers = _joined_identifiers(request.instruments, dataset="price_bars")
        provider_start, provider_end = _provider_bar_query_bounds(request)
        query = (
            ("symbols", identifiers),
            ("timeframe", _timeframe(request.timeframe)),
            ("start", _rfc3339(provider_start)),
            ("end", _rfc3339(_inclusive_end_for_bounds(provider_start, provider_end))),
            ("limit", "10000"),
            ("adjustment", provider_adjustment),
            ("asof", symbol_mapping_as_of),
            ("feed", self._feed.value),
            ("currency", "USD"),
            ("sort", "asc"),
        )
        source = ALPACA_SIP_BAR_SOURCE if self._feed is AlpacaFeed.SIP else ALPACA_IEX_BAR_SOURCE
        yield from self._pages(
            dataset="price_bars",
            source=source,
            path="/v2/stocks/bars",
            query=query,
            request_metadata={
                "instrument_count": len(request.instruments),
                "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
                "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
                "timeframe": request.timeframe.value,
                "start": request.start.isoformat(),
                "end_exclusive": request.end.isoformat(),
                "session": request.session.value,
                "adjustment_state": (
                    request.adjustment_state.value
                    if canonical_persistence_eligible
                    else "provider_evidence_only"
                ),
                "provider_adjustment": provider_adjustment,
                "canonical_persistence_eligible": canonical_persistence_eligible,
                "feed": self._feed.value,
                "currency": "USD",
                "symbol_mapping_as_of": symbol_mapping_as_of,
            },
        )

    def get_bars(self, request: BarRequest) -> Iterator[RawBatch]:
        """Retrieve bars with provider ticker remapping explicitly disabled."""

        yield from self._bar_pages(
            request,
            symbol_mapping_as_of="-",
            provider_adjustment=_adjustment(request.adjustment_state),
            canonical_persistence_eligible=True,
        )

    def get_bars_as_of(self, request: BarRequest, *, as_of: date) -> Iterator[RawBatch]:
        """Retrieve a provider-specific point-in-time symbol-mapping evidence control."""

        yield from self._bar_pages(
            request,
            symbol_mapping_as_of=as_of.isoformat(),
            provider_adjustment=_adjustment(request.adjustment_state),
            canonical_persistence_eligible=True,
        )

    def get_adjustment_evidence(
        self,
        request: BarRequest,
        *,
        adjustment: AlpacaEvidenceAdjustment,
    ) -> Iterator[RawBatch]:
        """Retrieve dividend/all provider evidence that cannot be labeled canonically."""

        if request.adjustment_state is not AdjustmentState.UNADJUSTED:
            raise ValueError(
                "provider-only adjustment evidence requires an unadjusted base request"
            )
        yield from self._bar_pages(
            request,
            symbol_mapping_as_of="-",
            provider_adjustment=adjustment.value,
            canonical_persistence_eligible=False,
        )

    def get_corporate_actions(self, request: CorporateActionRequest) -> Iterator[RawBatch]:
        inclusive_end = request.end - timedelta(days=1)
        identifiers = _joined_identifiers(request.instruments, dataset="corporate_actions")
        yield from self._pages(
            dataset="corporate_actions",
            source=ALPACA_CORPORATE_ACTION_SOURCE,
            path="/v1/corporate-actions",
            query=(
                ("symbols", identifiers),
                ("region", "us"),
                ("start", request.start.isoformat()),
                ("end", inclusive_end.isoformat()),
                ("limit", "1000"),
                ("data_quality", "complete"),
                ("sort", "asc"),
            ),
            request_metadata={
                "instrument_count": len(request.instruments),
                "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
                "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
                "start": request.start.isoformat(),
                "end_exclusive": request.end.isoformat(),
                "date_basis": "process_date",
                "effective_date_filter_complete": False,
            },
        )

    def preflight_sip_entitlement(self, request: BarRequest) -> AlpacaSipPreflightResult:
        """Make one transient SIP request and return no provider payload or credential."""

        if len(request.instruments) != 1 or request.timeframe is not Timeframe.FIVE_MINUTES:
            raise ValueError("SIP preflight requires exactly one instrument and a 5-minute request")
        if request.end - request.start != timedelta(minutes=5):
            raise ValueError("SIP preflight interval must be exactly one 5-minute bar")
        if (
            request.session is not TradingSession.REGULAR
            or request.adjustment_state is not AdjustmentState.UNADJUSTED
        ):
            raise ValueError("SIP preflight requires regular-session unadjusted semantics")
        if request.end > self._clock() - timedelta(minutes=15):
            raise ValueError("SIP preflight end must be at least 15 minutes in the past")

        identifier = _joined_identifiers(
            request.instruments,
            dataset="sip_entitlement_preflight",
        )
        query = (
            ("symbols", identifier),
            ("timeframe", "5Min"),
            ("start", _rfc3339(request.start)),
            ("end", _rfc3339(_inclusive_end(request))),
            ("limit", "1"),
            ("adjustment", "raw"),
            ("asof", "-"),
            ("feed", AlpacaFeed.SIP.value),
            ("currency", "USD"),
            ("sort", "asc"),
        )
        response = self._transport.get(
            provider=_PROVIDER,
            dataset="sip_entitlement_preflight",
            base_url=self._data_base_url,
            path="/v2/stocks/bars",
            query=query,
            headers=self._headers(),
            timeout_seconds=self._timeout_seconds,
        )
        outcome = AlpacaSipPreflightOutcome.HTTP_ERROR
        observation_count: int | None = None
        if response.status_code == 200:
            try:
                parsed = parse_json_response(
                    _PROVIDER,
                    "sip_entitlement_preflight",
                    response,
                )
                bars = parsed.get("bars")
                if not isinstance(bars, dict):
                    raise ProviderResponseError(
                        _PROVIDER,
                        "sip_entitlement_preflight",
                        "bars must be an object",
                    )
                candidate_observation_count = 0
                for key, value in bars.items():
                    if key != identifier or not isinstance(value, list):
                        raise ProviderResponseError(
                            _PROVIDER,
                            "sip_entitlement_preflight",
                            "bars entries must be arrays for the requested identifier",
                        )
                    candidate_observation_count += len(value)
                observation_count = candidate_observation_count
                outcome = AlpacaSipPreflightOutcome.AUTHORIZED
            except ProviderResponseError:
                outcome = AlpacaSipPreflightOutcome.MALFORMED_RESPONSE
        elif response.status_code == 401:
            outcome = AlpacaSipPreflightOutcome.AUTHENTICATION_FAILED
        elif response.status_code == 403:
            outcome = AlpacaSipPreflightOutcome.AUTH_OR_PERMISSION_DENIED
        elif response.status_code == 422:
            try:
                provider_error = parse_json_response(
                    _PROVIDER,
                    "sip_entitlement_preflight",
                    response,
                )
            except ProviderResponseError:
                provider_error = {}
            if provider_error.get("code") == 42210000:
                outcome = AlpacaSipPreflightOutcome.ENTITLEMENT_DENIED
        elif response.status_code == 429:
            outcome = AlpacaSipPreflightOutcome.RATE_LIMITED

        return AlpacaSipPreflightResult(
            outcome=outcome,
            status_code=response.status_code,
            requested_feed=AlpacaFeed.SIP,
            timeframe=request.timeframe,
            start=request.start,
            end_exclusive=request.end,
            instrument_count=1,
            observation_count=observation_count,
            checked_at=self._clock(),
            rate_limit_capacity=safe_numeric_header(response, "x-ratelimit-limit"),
            rate_limit_remaining=safe_numeric_header(response, "x-ratelimit-remaining"),
            rate_limit_reset=safe_numeric_header(response, "x-ratelimit-reset"),
            raw_retention_authorized=None,
        )


__all__ = [
    "ALPACA_CORPORATE_ACTION_SOURCE",
    "ALPACA_IEX_BAR_SOURCE",
    "ALPACA_INSTRUMENT_SOURCE",
    "ALPACA_SIP_BAR_SOURCE",
    "AlpacaCredentials",
    "AlpacaEvidenceAdjustment",
    "AlpacaFeed",
    "AlpacaProvider",
    "AlpacaSipPreflightOutcome",
    "AlpacaSipPreflightResult",
]
