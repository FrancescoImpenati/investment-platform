"""Small synchronous HTTP boundary used by provider adapters and offline fakes."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from email.message import Message
from ssl import TLSVersion, create_default_context
from time import perf_counter
from types import MappingProxyType
from typing import IO, Protocol, runtime_checkable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import (
    BaseHandler,
    HTTPRedirectHandler,
    HTTPSHandler,
    OpenerDirector,
    Request,
    build_opener,
)
from uuid import UUID

from investment_platform.data.provenance import BytesRawPayload, RawPayload
from investment_platform.data.providers.errors import ProviderTransportError
from investment_platform.data.storage.transport_spool import (
    AttemptTransportSpool,
    TransportSpoolError,
    TransportSpoolFaultInjector,
    TransportSpoolPayload,
    TransportSpoolStore,
)
from investment_platform.data_root import PrivateDataRoot

QueryParameters = tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True, init=False)
class HttpResponse:
    """Bounded response resource; headers stay private and names are normalized.

    In-memory fakes retain the historical ``body`` interface.  A durable live
    attempt instead supplies a reopenable file-backed payload.  Such a response
    intentionally refuses ``body`` materialization: provider code must consume
    ``raw_payload`` through bounded readers.
    """

    status_code: int
    _raw_payload: RawPayload = field(repr=False)
    headers: Mapping[str, str] = field(default_factory=dict)
    elapsed_ms: float = 0.0
    _inline_body: bytes | None = field(default=None, repr=False)

    def __init__(
        self,
        status_code: int,
        body: bytes | RawPayload,
        headers: Mapping[str, str] | None = None,
        elapsed_ms: float = 0.0,
    ) -> None:
        if not 100 <= status_code <= 599:
            raise ValueError("status_code must be a valid HTTP status")
        if elapsed_ms < 0:
            raise ValueError("elapsed_ms must not be negative")
        normalized = {str(key).lower(): str(value) for key, value in (headers or {}).items()}
        if isinstance(body, bytes):
            inline_body: bytes | None = body
            payload: RawPayload = BytesRawPayload(body)
        elif isinstance(body, RawPayload):
            inline_body = None
            payload = body
        else:
            raise TypeError("HTTP response body must be bytes or a reopenable raw payload")
        object.__setattr__(self, "status_code", status_code)
        object.__setattr__(self, "headers", MappingProxyType(normalized))
        object.__setattr__(self, "elapsed_ms", elapsed_ms)
        object.__setattr__(self, "_inline_body", inline_body)
        object.__setattr__(self, "_raw_payload", payload)

    @property
    def body(self) -> bytes:
        """Return legacy inline bytes, refusing implicit file materialization."""

        if self._inline_body is None:
            raise ResponseBodyNotMaterializedError(
                "file-backed HTTP responses must be consumed through raw_payload"
            )
        return self._inline_body

    @property
    def raw_payload(self) -> RawPayload:
        return self._raw_payload

    @property
    def is_file_backed(self) -> bool:
        return self._inline_body is None


@runtime_checkable
class HttpTransport(Protocol):
    """Injectable GET transport; tests provide deterministic in-memory responses."""

    def get(
        self,
        *,
        provider: str,
        dataset: str,
        base_url: str,
        path: str,
        query: QueryParameters,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        """Perform one request without retries or implicit pagination."""

        ...


@runtime_checkable
class AttemptScopedHttpTransport(Protocol):
    """Optional transport capability used only by durable living ingestion."""

    def attempt_scope(
        self,
        attempt_id: UUID,
        *,
        fault_injector: TransportSpoolFaultInjector | None = None,
    ) -> AbstractContextManager[None]: ...


class ResponseTooLargeError(RuntimeError):
    """A response exceeded the adapter's explicit in-memory safety bound."""


class ResponseBodyNotMaterializedError(RuntimeError):
    """A caller attempted to turn a spooled response back into one bytes object."""


class _Readable(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...


class _RejectRedirectHandler(HTTPRedirectHandler):
    """Prevent credential-bearing requests from following any provider redirect."""

    def redirect_request(
        self,
        req: Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> Request | None:
        del req, fp, code, msg, headers, newurl
        return None


def _validated_url(base_url: str, path: str, query: QueryParameters) -> str:
    parsed = urlsplit(base_url)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("base_url must be a credential-free HTTPS origin")
    if not path.startswith("/") or "://" in path or "?" in path or "#" in path:
        raise ValueError("path must be an absolute URL path without query or fragment")
    rendered_query = urlencode(query)
    return urlunsplit(("https", parsed.netloc, path, rendered_query, ""))


def _read_bounded(reader: _Readable, *, maximum_bytes: int, chunk_size: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while chunk := reader.read(min(chunk_size, maximum_bytes - total + 1)):
        total += len(chunk)
        if total > maximum_bytes:
            raise ResponseTooLargeError(
                f"HTTP response exceeded the configured {maximum_bytes}-byte bound"
            )
        chunks.append(chunk)
    return b"".join(chunks)


class UrllibHttpTransport:
    """Standard-library transport with bounded reads and no retry policy."""

    def __init__(self, *, maximum_response_bytes: int = 16 * 1024 * 1024) -> None:
        if maximum_response_bytes <= 0:
            raise ValueError("maximum_response_bytes must be positive")
        self._maximum_response_bytes = maximum_response_bytes
        self._redirect_handler = _RejectRedirectHandler()
        self._opener: OpenerDirector = build_opener(self._redirect_handler)

    def get(
        self,
        *,
        provider: str,
        dataset: str,
        base_url: str,
        path: str,
        query: QueryParameters,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        url = _validated_url(base_url, path, query)
        request = Request(url, headers=dict(headers), method="GET")
        started = perf_counter()
        try:
            with self._opener.open(request, timeout=timeout_seconds) as response:
                body = _read_bounded(
                    response,
                    maximum_bytes=self._maximum_response_bytes,
                    chunk_size=64 * 1024,
                )
                response_headers = dict(response.headers.items())
                status_code = response.status
        except HTTPError as error:
            try:
                try:
                    body = _read_bounded(
                        error,
                        maximum_bytes=self._maximum_response_bytes,
                        chunk_size=64 * 1024,
                    )
                    response_headers = dict(error.headers.items()) if error.headers else {}
                    status_code = error.code
                except (OSError, ResponseTooLargeError) as body_error:
                    raise ProviderTransportError(
                        provider,
                        dataset,
                        "HTTP error response could not be read within the configured bound",
                    ) from body_error
            finally:
                error.close()
        except (OSError, ResponseTooLargeError, URLError) as error:
            raise ProviderTransportError(
                provider,
                dataset,
                "HTTP transport failed before a bounded response was available",
            ) from error

        return HttpResponse(
            status_code=status_code,
            body=body,
            headers=response_headers,
            elapsed_ms=(perf_counter() - started) * 1000,
        )


class SpoolingUrllibHttpTransport:
    """Standard-library transport that writes bounded bodies to an attempt spool.

    The transport is intentionally unusable outside ``attempt_scope``.  This
    prevents a live response from being written without an exact ingestion
    attempt identity and makes restart cleanup deterministic.
    """

    def __init__(
        self,
        data_root: PrivateDataRoot,
        *,
        maximum_response_bytes: int = 64 * 1024 * 1024,
        spool_store: TransportSpoolStore | None = None,
        tls_maximum_version: TLSVersion | None = None,
    ) -> None:
        if maximum_response_bytes <= 0:
            raise ValueError("maximum_response_bytes must be positive")
        self._maximum_response_bytes = maximum_response_bytes
        self._spool_store = spool_store or TransportSpoolStore(data_root)
        self._redirect_handler = _RejectRedirectHandler()
        handlers: list[BaseHandler] = [self._redirect_handler]
        if tls_maximum_version is not None:
            context = create_default_context()
            context.maximum_version = tls_maximum_version
            handlers.append(HTTPSHandler(context=context))
        self._opener: OpenerDirector = build_opener(*handlers)
        self._active_spool: AttemptTransportSpool | None = None

    @contextmanager
    def attempt_scope(
        self,
        attempt_id: UUID,
        *,
        fault_injector: TransportSpoolFaultInjector | None = None,
    ) -> Iterator[None]:
        """Bind every response in one provider page chain to an exact attempt."""

        if self._active_spool is not None:
            raise RuntimeError("transport attempt scopes must not be nested")
        with self._spool_store.attempt(
            attempt_id,
            fault_injector=fault_injector,
        ) as spool:
            self._active_spool = spool
            try:
                yield
            finally:
                self._active_spool = None

    def recover_transient_attempts(self) -> tuple[UUID, ...]:
        """Expose explicit startup recovery without making spool files durable state."""

        if self._active_spool is not None:
            raise RuntimeError("cannot recover transport spools during an active attempt")
        return self._spool_store.recover_transient_attempts()

    def _spool(self, reader: _Readable) -> TransportSpoolPayload:
        if not isinstance(self._active_spool, AttemptTransportSpool):
            raise ProviderTransportError(
                "transport",
                "unbound",
                "file-backed HTTP transport requires an exact attempt scope",
            )
        return self._active_spool.spool(
            reader,
            maximum_bytes=self._maximum_response_bytes,
        )

    def get(
        self,
        *,
        provider: str,
        dataset: str,
        base_url: str,
        path: str,
        query: QueryParameters,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self._active_spool is None:
            raise ProviderTransportError(
                provider,
                dataset,
                "file-backed HTTP transport requires an exact attempt scope",
            )
        url = _validated_url(base_url, path, query)
        request = Request(url, headers=dict(headers), method="GET")
        started = perf_counter()
        try:
            with self._opener.open(request, timeout=timeout_seconds) as response:
                payload = self._spool(response)
                response_headers = dict(response.headers.items())
                status_code = response.status
        except HTTPError as error:
            try:
                try:
                    payload = self._spool(error)
                    response_headers = dict(error.headers.items()) if error.headers else {}
                    status_code = error.code
                except (OSError, TransportSpoolError) as body_error:
                    raise ProviderTransportError(
                        provider,
                        dataset,
                        "HTTP error response could not be read within the configured bound",
                    ) from body_error
            finally:
                error.close()
        except (OSError, TransportSpoolError, URLError) as error:
            raise ProviderTransportError(
                provider,
                dataset,
                "HTTP transport failed before a bounded response was available",
            ) from error

        return HttpResponse(
            status_code=status_code,
            body=payload,
            headers=response_headers,
            elapsed_ms=(perf_counter() - started) * 1000,
        )


__all__ = [
    "AttemptScopedHttpTransport",
    "HttpResponse",
    "HttpTransport",
    "QueryParameters",
    "ResponseBodyNotMaterializedError",
    "ResponseTooLargeError",
    "SpoolingUrllibHttpTransport",
    "UrllibHttpTransport",
]
