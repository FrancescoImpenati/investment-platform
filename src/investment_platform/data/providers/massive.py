"""Massive REST adapter that emits unchanged, paginated raw JSON batches."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from urllib.parse import parse_qsl, quote, urlsplit
from uuid import NAMESPACE_URL, uuid5

from investment_platform.data.models import AdjustmentState, Timeframe, TradingSession
from investment_platform.data.provenance import (
    DataSource,
    JsonScalar,
    LicenseClassification,
    RawBatch,
)
from investment_platform.data.providers._support import (
    BatchIdFactory,
    Clock,
    new_batch_id,
    parse_json_object,
    raw_batch_from_response,
    require_success,
    utc_now,
)
from investment_platform.data.providers.base import (
    BarRequest,
    CorporateActionRequest,
    instrument_refs_fingerprint,
    instrument_refs_manifest,
)
from investment_platform.data.providers.errors import (
    ProviderCapabilityError,
    ProviderConfigurationError,
    ProviderResponseError,
)
from investment_platform.data.providers.http import (
    HttpResponse,
    HttpTransport,
    QueryParameters,
    UrllibHttpTransport,
)

_PROVIDER = "massive"
_DEFAULT_BASE_URL = "https://api.massive.com"

MASSIVE_BAR_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:massive:stocks:bars"),
    provider=_PROVIDER,
    dataset="price_bars",
    logical_endpoint="v2/aggs/ticker/range",
    license_classification=LicenseClassification.PRIVATE,
)
MASSIVE_INSTRUMENT_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:massive:stocks:tickers"),
    provider=_PROVIDER,
    dataset="instruments",
    logical_endpoint="v3/reference/tickers",
    license_classification=LicenseClassification.PRIVATE,
)
MASSIVE_SPLIT_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:massive:stocks:splits"),
    provider=_PROVIDER,
    dataset="corporate_actions",
    logical_endpoint="stocks/v1/splits",
    license_classification=LicenseClassification.PRIVATE,
)
MASSIVE_DIVIDEND_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:massive:stocks:dividends"),
    provider=_PROVIDER,
    dataset="corporate_actions",
    logical_endpoint="stocks/v1/dividends",
    license_classification=LicenseClassification.PRIVATE,
)
MASSIVE_TICKER_EVENT_SOURCE = DataSource(
    source_id=uuid5(NAMESPACE_URL, "investment-platform:massive:stocks:ticker-events"),
    provider=_PROVIDER,
    dataset="corporate_actions",
    logical_endpoint="vX/reference/tickers/events",
    license_classification=LicenseClassification.PRIVATE,
)


@dataclass(frozen=True, slots=True)
class MassiveCredentials:
    """Massive API key whose representation never reveals the value."""

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
    ) -> MassiveCredentials:
        values = os.environ if environ is None else environ
        value = values.get("MASSIVE_API_KEY", "")
        return cls(api_key=value)


def _epoch_microseconds(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value - epoch
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _epoch_milliseconds_floor(value: datetime) -> int:
    return _epoch_microseconds(value) // 1000


def _epoch_milliseconds_ceiling(value: datetime) -> int:
    return (_epoch_microseconds(value) + 999) // 1000


def _bar_range(request: BarRequest) -> tuple[int, int]:
    start = _epoch_milliseconds_floor(request.start)
    inclusive_end = _epoch_milliseconds_ceiling(request.end) - 1
    if inclusive_end < start:
        raise ProviderCapabilityError(
            _PROVIDER,
            "price_bars",
            "requested interval is shorter than Massive millisecond precision",
        )
    return start, inclusive_end


def _timeframe_parts(timeframe: Timeframe) -> tuple[str, str]:
    if timeframe is Timeframe.ONE_DAY:
        return "1", "day"
    if timeframe is Timeframe.FIVE_MINUTES:
        return "5", "minute"
    raise ProviderCapabilityError(_PROVIDER, "price_bars", f"unsupported timeframe {timeframe}")


def _adjusted_parameter(adjustment: AdjustmentState) -> str:
    if adjustment is AdjustmentState.UNADJUSTED:
        return "false"
    if adjustment is AdjustmentState.SPLIT_ADJUSTED:
        return "true"
    raise ProviderCapabilityError(
        _PROVIDER,
        "price_bars",
        f"Massive cannot represent canonical adjustment state {adjustment.value}",
    )


class MassiveProvider:
    """Synchronous Massive adapter with explicit pages and no retry/fallback behavior."""

    def __init__(
        self,
        credentials: MassiveCredentials,
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
            raise ValueError("Massive credentials may be sent only to the official API origin")
        self._credentials = credentials
        self._transport = transport or UrllibHttpTransport()
        self._base_url = base_url
        self._timeout_seconds = timeout_seconds
        self._clock = clock
        self._batch_id_factory = batch_id_factory

    @classmethod
    def from_environment(cls) -> MassiveProvider:
        return cls(MassiveCredentials.from_environment())

    @property
    def provider_name(self) -> str:
        return _PROVIDER

    def _get(self, *, dataset: str, path: str, query: QueryParameters) -> HttpResponse:
        response = self._transport.get(
            provider=_PROVIDER,
            dataset=dataset,
            base_url=self._base_url,
            path=path,
            query=query,
            headers={"Authorization": f"Bearer {self._credentials.api_key}"},
            timeout_seconds=self._timeout_seconds,
        )
        require_success(_PROVIDER, dataset, response)
        return response

    def _next_page(
        self,
        dataset: str,
        payload: bytes,
        *,
        expected_path: str,
    ) -> tuple[str, QueryParameters] | None:
        parsed = parse_json_object(_PROVIDER, dataset, payload)
        next_url = parsed.get("next_url")
        if next_url is None:
            return None
        if not isinstance(next_url, str):
            raise ProviderResponseError(_PROVIDER, dataset, "next_url must be a string")
        base = urlsplit(self._base_url)
        next_page = urlsplit(next_url)
        if (
            next_page.scheme != "https"
            or next_page.netloc != base.netloc
            or next_page.username is not None
            or next_page.password is not None
            or next_page.fragment
        ):
            raise ProviderResponseError(
                _PROVIDER,
                dataset,
                "next_url did not point to the configured Massive HTTPS origin",
            )
        if next_page.path != expected_path:
            raise ProviderResponseError(
                _PROVIDER,
                dataset,
                "next_url changed the expected Massive endpoint path",
            )
        query = tuple(parse_qsl(next_page.query, keep_blank_values=True))
        query_keys = [key.casefold() for key, _ in query]
        if any(key in {"apikey", "api_key"} for key in query_keys):
            raise ProviderResponseError(
                _PROVIDER,
                dataset,
                "next_url contained a credential-bearing query parameter",
            )
        if query_keys != ["cursor"] or not query[0][1]:
            raise ProviderResponseError(
                _PROVIDER,
                dataset,
                "next_url changed request semantics instead of supplying one cursor",
            )
        return next_page.path, query

    def _pages(
        self,
        *,
        dataset: str,
        source: DataSource,
        path: str,
        query: QueryParameters,
        request_metadata: Mapping[str, str | int | float | bool | None],
    ) -> Iterator[RawBatch]:
        page_number = 1
        expected_path = path
        seen_pages: set[tuple[str, QueryParameters]] = {(path, tuple(sorted(query)))}
        while True:
            response = self._get(dataset=dataset, path=path, query=query)
            next_page = self._next_page(dataset, response.body, expected_path=expected_path)
            batch = raw_batch_from_response(
                source=source,
                response=response,
                request_metadata={**request_metadata, "page_number": page_number},
                clock=self._clock,
                batch_id_factory=self._batch_id_factory,
            )
            yield batch
            if next_page is None:
                return
            path, query = next_page
            page_identity = (path, tuple(sorted(query)))
            if page_identity in seen_pages:
                raise ProviderResponseError(
                    _PROVIDER,
                    dataset,
                    "pagination returned a repeated next_url",
                )
            seen_pages.add(page_identity)
            page_number += 1
            if page_number > 1000:
                raise ProviderResponseError(
                    _PROVIDER,
                    dataset,
                    "pagination exceeded the 1000-page safety bound",
                )

    def get_instruments(self, *, as_of: date | None = None) -> Iterator[RawBatch]:
        query: list[tuple[str, str]] = [
            ("market", "stocks"),
            ("active", "true"),
            ("limit", "1000"),
            ("sort", "ticker"),
            ("order", "asc"),
        ]
        if as_of is not None:
            query.append(("date", as_of.isoformat()))
        yield from self._pages(
            dataset="instruments",
            source=MASSIVE_INSTRUMENT_SOURCE,
            path="/v3/reference/tickers",
            query=tuple(query),
            request_metadata={"as_of": as_of.isoformat() if as_of is not None else None},
        )

    def get_instrument(
        self,
        provider_identifier: str,
        *,
        as_of: date | None = None,
    ) -> Iterator[RawBatch]:
        """Retrieve one ticker detail without enumerating the provider inventory."""

        identifier = provider_identifier.strip()
        if not identifier:
            raise ValueError("provider_identifier must not be blank")
        query: QueryParameters = ()
        if as_of is not None:
            query = (("date", as_of.isoformat()),)
        response = self._get(
            dataset="instruments",
            path=f"/v3/reference/tickers/{quote(identifier, safe='')}",
            query=query,
        )
        yield raw_batch_from_response(
            source=MASSIVE_INSTRUMENT_SOURCE,
            response=response,
            request_metadata={
                "provider_identifier": identifier,
                "as_of": as_of.isoformat() if as_of is not None else None,
                "lookup_scope": "single_instrument",
            },
            clock=self._clock,
            batch_id_factory=self._batch_id_factory,
        )

    def get_bars(self, request: BarRequest) -> Iterator[RawBatch]:
        if request.session not in {TradingSession.REGULAR, TradingSession.UNKNOWN}:
            raise ProviderCapabilityError(
                _PROVIDER,
                "price_bars",
                f"session {request.session.value} is not explicitly supported",
            )
        multiplier, timespan = _timeframe_parts(request.timeframe)
        start, inclusive_end = _bar_range(request)
        adjusted = _adjusted_parameter(request.adjustment_state)
        for instrument in request.instruments:
            identifier = quote(instrument.provider_identifier, safe="")
            path = (
                f"/v2/aggs/ticker/{identifier}/range/{multiplier}/{timespan}/"
                f"{start}/{inclusive_end}"
            )
            yield from self._pages(
                dataset="price_bars",
                source=MASSIVE_BAR_SOURCE,
                path=path,
                query=(("adjusted", adjusted), ("sort", "asc"), ("limit", "50000")),
                request_metadata={
                    "instrument_id": str(instrument.instrument_id),
                    "provider_identifier": instrument.provider_identifier,
                    "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
                    "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
                    "timeframe": request.timeframe.value,
                    "start": request.start.isoformat(),
                    "end_exclusive": request.end.isoformat(),
                    "session": request.session.value,
                    "adjustment_state": request.adjustment_state.value,
                },
            )

    def get_corporate_actions(self, request: CorporateActionRequest) -> Iterator[RawBatch]:
        action_end = request.end.isoformat()
        for instrument in request.instruments:
            common_metadata: dict[str, JsonScalar] = {
                "instrument_id": str(instrument.instrument_id),
                "provider_identifier": instrument.provider_identifier,
                "start": request.start.isoformat(),
                "end_exclusive": action_end,
                "date_basis": "effective_date",
                "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
                "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
            }
            yield from self._pages(
                dataset="corporate_actions",
                source=MASSIVE_SPLIT_SOURCE,
                path="/stocks/v1/splits",
                query=(
                    ("ticker", instrument.provider_identifier),
                    ("execution_date.gte", request.start.isoformat()),
                    ("execution_date.lt", action_end),
                    ("limit", "1000"),
                    ("sort", "execution_date.asc"),
                ),
                request_metadata={**common_metadata, "action_family": "split"},
            )
            yield from self._pages(
                dataset="corporate_actions",
                source=MASSIVE_DIVIDEND_SOURCE,
                path="/stocks/v1/dividends",
                query=(
                    ("ticker", instrument.provider_identifier),
                    ("ex_dividend_date.gte", request.start.isoformat()),
                    ("ex_dividend_date.lt", action_end),
                    ("limit", "1000"),
                    ("sort", "ex_dividend_date.asc"),
                ),
                request_metadata={**common_metadata, "action_family": "cash_dividend"},
            )

    def get_ticker_events(self, request: CorporateActionRequest) -> Iterator[RawBatch]:
        """Retrieve experimental ticker timelines as bounded-sample raw supersets."""

        for instrument in request.instruments:
            identifier = quote(instrument.provider_identifier, safe="")
            yield from self._pages(
                dataset="corporate_actions",
                source=MASSIVE_TICKER_EVENT_SOURCE,
                path=f"/vX/reference/tickers/{identifier}/events",
                query=(("types", "ticker_change"),),
                request_metadata={
                    "instrument_id": str(instrument.instrument_id),
                    "provider_identifier": instrument.provider_identifier,
                    "start": request.start.isoformat(),
                    "end_exclusive": request.end.isoformat(),
                    "date_basis": "timeline_superset",
                    "experimental_endpoint": True,
                    "instrument_refs_fingerprint": instrument_refs_fingerprint(request.instruments),
                    "instrument_refs_manifest": instrument_refs_manifest(request.instruments),
                },
            )


__all__ = [
    "MASSIVE_BAR_SOURCE",
    "MASSIVE_DIVIDEND_SOURCE",
    "MASSIVE_INSTRUMENT_SOURCE",
    "MASSIVE_SPLIT_SOURCE",
    "MASSIVE_TICKER_EVENT_SOURCE",
    "MassiveCredentials",
    "MassiveProvider",
]
