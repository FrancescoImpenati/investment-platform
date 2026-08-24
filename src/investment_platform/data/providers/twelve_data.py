"""Twelve Data Basic adapter for bounded daily and five-minute bar evidence."""

from __future__ import annotations

import math
import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import StrEnum
from uuid import NAMESPACE_URL, uuid5

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.provenance import DataSource, LicenseClassification, RawBatch
from investment_platform.data.providers._support import (
    BatchIdFactory,
    Clock,
    new_batch_id,
    parse_json_object,
    raw_batch_from_response,
    require_success,
    retry_after_seconds,
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
    ProviderAuthenticationError,
    ProviderCapabilityError,
    ProviderConfigurationError,
    ProviderEntitlementError,
    ProviderHttpError,
    ProviderRateLimitError,
    ProviderResponseError,
)
from investment_platform.data.providers.http import (
    HttpResponse,
    HttpTransport,
    QueryParameters,
    UrllibHttpTransport,
)

_PROVIDER = "twelve_data"
_DEFAULT_BASE_URL = "https://api.twelvedata.com"
_MAX_SYMBOLS_PER_BATCH = 8
_MAX_POINTS_PER_SYMBOL = 5000

TWELVE_DATA_DAILY_BAR_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:twelve-data:stocks:bars:us-daily"),
    provider=_PROVIDER,
    dataset="price_bars_us_daily",
    logical_endpoint="time_series",
    license_classification=LicenseClassification.PRIVATE,
)
TWELVE_DATA_INTRADAY_BAR_SOURCE = DataSource(
    source_id=uuid5(
        NAMESPACE_URL,
        "investment-platform:twelve-data:stocks:bars:standard-us-intraday",
    ),
    provider=_PROVIDER,
    dataset="price_bars_standard_us_intraday",
    logical_endpoint="time_series",
    license_classification=LicenseClassification.PRIVATE,
)
TWELVE_DATA_INSTRUMENT_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:twelve-data:symbol-search"),
    provider=_PROVIDER,
    dataset="instruments",
    logical_endpoint="symbol_search",
    license_classification=LicenseClassification.PRIVATE,
)


class TwelveDataEvidenceAdjustment(StrEnum):
    """Provider-only adjustment modes without exact Phase 0 canonical semantics."""

    DIVIDENDS = "dividends"
    ALL = "all"


@dataclass(frozen=True, slots=True)
class TwelveDataCredentials:
    """Twelve Data API key whose representation never reveals the value."""

    api_key: str = field(repr=False)

    def __post_init__(self) -> None:
        normalized = self.api_key.strip()
        if not normalized:
            raise ProviderConfigurationError(_PROVIDER, "configuration", "API key is missing")
        object.__setattr__(self, "api_key", normalized)

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> TwelveDataCredentials:
        values = os.environ if environ is None else environ
        return cls(api_key=values.get("TWELVE_DATA_API_KEY", ""))


def _timeframe(timeframe: Timeframe) -> str:
    if timeframe is Timeframe.ONE_DAY:
        return "1day"
    if timeframe is Timeframe.FIVE_MINUTES:
        return "5min"
    raise ProviderCapabilityError(_PROVIDER, "price_bars", f"unsupported timeframe {timeframe}")


def _adjustment(adjustment: AdjustmentState) -> str:
    if adjustment is AdjustmentState.UNADJUSTED:
        return "none"
    if adjustment is AdjustmentState.SPLIT_ADJUSTED:
        return "splits"
    raise ProviderCapabilityError(
        _PROVIDER,
        "price_bars",
        f"Twelve Data cannot map canonical adjustment state {adjustment.value} explicitly",
    )


def _wire_datetime_floor(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _wire_datetime_ceiling(value: datetime) -> str:
    if value.microsecond:
        value = value + timedelta(seconds=1)
    return value.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def _wire_bounds(request: BarRequest) -> tuple[str, str]:
    if request.timeframe is Timeframe.ONE_DAY:
        inclusive_end = request.end - timedelta(microseconds=1)
        return request.start.date().isoformat(), inclusive_end.date().isoformat()
    return _wire_datetime_floor(request.start), _wire_datetime_ceiling(request.end)


def _maximum_possible_points(request: BarRequest) -> int:
    if request.timeframe is Timeframe.ONE_DAY:
        inclusive_end = request.end - timedelta(microseconds=1)
        return (inclusive_end.date() - request.start.date()).days + 1
    return math.ceil((request.end - request.start) / timedelta(minutes=5)) + 1


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


def _chunks(
    instruments: tuple[ProviderInstrumentRef, ...],
) -> Iterator[tuple[ProviderInstrumentRef, ...]]:
    for offset in range(0, len(instruments), _MAX_SYMBOLS_PER_BATCH):
        yield instruments[offset : offset + _MAX_SYMBOLS_PER_BATCH]


class TwelveDataProvider:
    """Synchronous Twelve Data adapter with bounded Basic-tier request batches."""

    def __init__(
        self,
        credentials: TwelveDataCredentials,
        *,
        transport: HttpTransport | None = None,
        base_url: str = _DEFAULT_BASE_URL,
        timeout_seconds: float = 30.0,
        clock: Clock = utc_now,
        batch_id_factory: BatchIdFactory = new_batch_id,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if base_url != _DEFAULT_BASE_URL:
            raise ValueError("Twelve Data credentials require the official API origin")
        self._credentials = credentials
        self._transport = transport or UrllibHttpTransport()
        self._base_url = base_url
        self._timeout_seconds = timeout_seconds
        self._clock = clock
        self._batch_id_factory = batch_id_factory

    @classmethod
    def from_environment(cls) -> TwelveDataProvider:
        return cls(TwelveDataCredentials.from_environment())

    @property
    def provider_name(self) -> str:
        return _PROVIDER

    def _headers(self) -> Mapping[str, str]:
        return {"Authorization": f"apikey {self._credentials.api_key}"}

    def _raise_body_error(self, dataset: str, response: HttpResponse) -> None:
        root = parse_json_object(_PROVIDER, dataset, response.body)
        status = root.get("status")
        if status is None or status == "ok":
            return
        if status != "error":
            raise ProviderResponseError(
                _PROVIDER,
                dataset,
                "response status must be 'ok' or 'error' when present",
            )
        code = root.get("code")
        if not isinstance(code, int) or isinstance(code, bool) or not 100 <= code <= 599:
            raise ProviderResponseError(
                _PROVIDER,
                dataset,
                "error response did not contain a valid provider status code",
            )
        error_type: type[ProviderHttpError]
        if code == 401:
            error_type = ProviderAuthenticationError
        elif code == 403:
            error_type = ProviderEntitlementError
        elif code == 429:
            error_type = ProviderRateLimitError
        else:
            error_type = ProviderHttpError
        raise error_type(
            _PROVIDER,
            dataset,
            status_code=code,
            retry_after_seconds=retry_after_seconds(response),
        )

    def _get(self, *, dataset: str, path: str, query: QueryParameters) -> HttpResponse:
        response = self._transport.get(
            provider=_PROVIDER,
            dataset=dataset,
            base_url=self._base_url,
            path=path,
            query=query,
            headers=self._headers(),
            timeout_seconds=self._timeout_seconds,
        )
        if response.status_code == 403:
            raise ProviderEntitlementError(
                _PROVIDER,
                dataset,
                status_code=response.status_code,
                retry_after_seconds=retry_after_seconds(response),
            )
        require_success(_PROVIDER, dataset, response)
        return response

    def get_instruments(self, *, as_of: date | None = None) -> Iterator[RawBatch]:
        del as_of
        raise ProviderCapabilityError(
            _PROVIDER,
            "instruments",
            "the unbounded instrument inventory is outside the Phase 1 sample; use get_instrument",
        )

    def get_instrument(self, provider_identifier: str) -> Iterator[RawBatch]:
        """Retrieve bounded current symbol-search evidence without enumerating the catalog."""

        identifier = provider_identifier.strip()
        if not identifier:
            raise ValueError("provider_identifier must not be blank")
        response = self._get(
            dataset="instruments",
            path="/symbol_search",
            query=(("symbol", identifier), ("outputsize", "30")),
        )
        yield raw_batch_from_response(
            source=TWELVE_DATA_INSTRUMENT_SOURCE,
            response=response,
            request_metadata={
                "provider_identifier": identifier,
                "as_of": None,
                "lookup_scope": "single_instrument_search",
            },
            clock=self._clock,
            batch_id_factory=self._batch_id_factory,
        )
        # Inspect only after the caller had an opportunity to persist the provider-native body.
        self._raise_body_error("instruments", response)

    def _bar_batches(
        self,
        request: BarRequest,
        *,
        provider_adjustment: str,
        canonical_persistence_eligible: bool,
    ) -> Iterator[RawBatch]:
        if request.session not in {TradingSession.REGULAR, TradingSession.UNKNOWN}:
            raise ProviderCapabilityError(
                _PROVIDER,
                "price_bars",
                f"session {request.session.value} is not explicitly supported",
            )
        if _maximum_possible_points(request) > _MAX_POINTS_PER_SYMBOL:
            raise ProviderCapabilityError(
                _PROVIDER,
                "price_bars",
                "requested interval could exceed the 5000-point per-symbol response limit",
            )
        interval = _timeframe(request.timeframe)
        start_date, end_date = _wire_bounds(request)
        source = (
            TWELVE_DATA_DAILY_BAR_SOURCE
            if request.timeframe is Timeframe.ONE_DAY
            else TWELVE_DATA_INTRADAY_BAR_SOURCE
        )
        chunks = tuple(_chunks(request.instruments))
        for chunk_number, chunk in enumerate(chunks, start=1):
            response = self._get(
                dataset="price_bars",
                path="/time_series",
                query=(
                    ("symbol", _joined_identifiers(chunk, dataset="price_bars")),
                    ("interval", interval),
                    ("start_date", start_date),
                    ("end_date", end_date),
                    ("order", "asc"),
                    ("timezone", "UTC"),
                    ("prepost", "false"),
                    ("adjust", provider_adjustment),
                    ("format", "JSON"),
                ),
            )
            yield raw_batch_from_response(
                source=source,
                response=response,
                request_metadata={
                    "instrument_count": len(request.instruments),
                    "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
                    "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
                    "chunk_number": chunk_number,
                    "chunk_count": len(chunks),
                    "chunk_instrument_count": len(chunk),
                    "chunk_instrument_refs_fingerprint": instrument_refs_fingerprint(chunk),
                    "chunk_instrument_refs_manifest": instrument_refs_manifest(chunk),
                    "api_credit_cost": len(chunk),
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
                    "requested_output_timezone": "UTC",
                    "provider_time_basis": (
                        "exchange_market_date" if request.timeframe is Timeframe.ONE_DAY else "UTC"
                    ),
                    "prepost": False,
                    "order": "asc",
                    "page_number": 1,
                    "pagination_supported": False,
                },
                clock=self._clock,
                batch_id_factory=self._batch_id_factory,
            )
            # Body-level errors are provider wire semantics, so raw evidence is yielded first.
            self._raise_body_error("price_bars", response)

    def get_bars(self, request: BarRequest) -> Iterator[RawBatch]:
        yield from self._bar_batches(
            request,
            provider_adjustment=_adjustment(request.adjustment_state),
            canonical_persistence_eligible=True,
        )

    def get_adjustment_evidence(
        self,
        request: BarRequest,
        *,
        adjustment: TwelveDataEvidenceAdjustment,
    ) -> Iterator[RawBatch]:
        """Retrieve provider-native dividend/all adjustment evidence only."""

        if request.adjustment_state is not AdjustmentState.UNADJUSTED:
            raise ValueError(
                "provider-only adjustment evidence requires an unadjusted base request"
            )
        yield from self._bar_batches(
            request,
            provider_adjustment=adjustment.value,
            canonical_persistence_eligible=False,
        )

    def get_corporate_actions(self, request: CorporateActionRequest) -> Iterator[RawBatch]:
        del request
        raise ProviderCapabilityError(
            _PROVIDER,
            "corporate_actions",
            "Twelve Data Basic does not entitle the /splits or /dividends endpoints",
        )


__all__ = [
    "TWELVE_DATA_DAILY_BAR_SOURCE",
    "TWELVE_DATA_INSTRUMENT_SOURCE",
    "TWELVE_DATA_INTRADAY_BAR_SOURCE",
    "TwelveDataCredentials",
    "TwelveDataEvidenceAdjustment",
    "TwelveDataProvider",
]
