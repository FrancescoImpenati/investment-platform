"""Security and boundedness tests for the shared provider HTTP transport."""

from email.message import Message
from io import BytesIO
from typing import Never, cast
from urllib.error import HTTPError
from urllib.request import OpenerDirector, Request

import pytest

from investment_platform.data.providers._support import retry_after_seconds
from investment_platform.data.providers.errors import ProviderTransportError
from investment_platform.data.providers.http import (
    HttpResponse,
    UrllibHttpTransport,
    _read_bounded,
)

pytestmark = pytest.mark.unit


class _OversizedErrorOpener:
    def open(self, request: Request, *, timeout: float) -> Never:
        del timeout
        raise HTTPError(
            request.full_url,
            500,
            "Synthetic error",
            Message(),
            BytesIO(b"12345"),
        )


def test_transport_rejects_redirects_before_credentials_can_cross_origins() -> None:
    transport = UrllibHttpTransport()
    request = Request(
        "https://data.alpaca.markets/v2/stocks/bars",
        headers={"APCA-API-SECRET-KEY": "synthetic-secret"},
    )
    response_headers = Message()
    response_headers["Location"] = "https://attacker.invalid/collect"

    redirected = transport._redirect_handler.redirect_request(
        request,
        BytesIO(),
        302,
        "Found",
        response_headers,
        response_headers["Location"],
    )

    assert redirected is None
    assert request.get_header("Apca-api-secret-key") == "synthetic-secret"


def test_bounded_reader_stops_before_returning_an_oversized_body() -> None:
    with pytest.raises(RuntimeError, match="exceeded"):
        _read_bounded(BytesIO(b"12345"), maximum_bytes=4, chunk_size=2)

    assert _read_bounded(BytesIO(b"1234"), maximum_bytes=4, chunk_size=2) == b"1234"


def test_oversized_http_error_body_is_a_typed_transport_failure() -> None:
    transport = UrllibHttpTransport(maximum_response_bytes=4)
    transport._opener = cast(OpenerDirector, _OversizedErrorOpener())

    with pytest.raises(ProviderTransportError, match="configured bound"):
        transport.get(
            provider="synthetic-provider",
            dataset="synthetic-dataset",
            base_url="https://provider.invalid",
            path="/bounded",
            query=(),
            headers={"Authorization": "synthetic-secret"},
            timeout_seconds=1,
        )


@pytest.mark.parametrize("value", ["Infinity", "NaN", "-1", "not-a-number"])
def test_retry_after_rejects_non_finite_negative_or_malformed_values(value: str) -> None:
    assert retry_after_seconds(HttpResponse(429, b"", {"Retry-After": value})) is None
