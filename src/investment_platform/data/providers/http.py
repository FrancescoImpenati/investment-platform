"""Small synchronous HTTP boundary used by provider adapters and offline fakes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from email.message import Message
from time import perf_counter
from types import MappingProxyType
from typing import IO, Protocol, runtime_checkable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, OpenerDirector, Request, build_opener

from investment_platform.data.providers.errors import ProviderTransportError

QueryParameters = tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """Bounded response data; header names are normalized and values remain private."""

    status_code: int
    body: bytes
    headers: Mapping[str, str] = field(default_factory=dict)
    elapsed_ms: float = 0.0

    def __post_init__(self) -> None:
        if not 100 <= self.status_code <= 599:
            raise ValueError("status_code must be a valid HTTP status")
        if self.elapsed_ms < 0:
            raise ValueError("elapsed_ms must not be negative")
        normalized = {str(key).lower(): str(value) for key, value in self.headers.items()}
        object.__setattr__(self, "headers", MappingProxyType(normalized))


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


class ResponseTooLargeError(RuntimeError):
    """A response exceeded the adapter's explicit in-memory safety bound."""


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


__all__ = [
    "HttpResponse",
    "HttpTransport",
    "QueryParameters",
    "ResponseTooLargeError",
    "UrllibHttpTransport",
]
