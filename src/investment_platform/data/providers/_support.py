"""Shared mechanics for provider adapters; no vendor semantics live here."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import cast
from uuid import UUID, uuid4

from investment_platform.data.provenance import (
    BytesRawPayload,
    DataSource,
    JsonScalar,
    RawBatch,
    RawBatchMetadata,
)
from investment_platform.data.providers.errors import (
    ProviderAccessDeniedError,
    ProviderAuthenticationError,
    ProviderHttpError,
    ProviderRateLimitError,
    ProviderResponseError,
)
from investment_platform.data.providers.http import HttpResponse

type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type Clock = Callable[[], datetime]
type BatchIdFactory = Callable[[], UUID]

_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:-]{1,256}$")


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_batch_id() -> UUID:
    return uuid4()


def retry_after_seconds(response: HttpResponse) -> float | None:
    raw_value = response.headers.get("retry-after")
    if raw_value is None:
        return None
    try:
        parsed = float(raw_value)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def safe_numeric_header(response: HttpResponse, *names: str) -> int | float | None:
    """Return a finite non-negative response scalar without retaining a header value string."""

    raw_value = next(
        (
            response.headers.get(name.casefold())
            for name in names
            if response.headers.get(name.casefold()) is not None
        ),
        None,
    )
    if raw_value is None:
        return None
    try:
        parsed = float(raw_value)
    except ValueError:
        return None
    if not math.isfinite(parsed) or parsed < 0:
        return None
    return int(parsed) if parsed.is_integer() else parsed


def require_success(provider: str, dataset: str, response: HttpResponse) -> None:
    if 200 <= response.status_code < 300:
        return
    retry_after = retry_after_seconds(response)
    if response.status_code == 401:
        raise ProviderAuthenticationError(
            provider,
            dataset,
            status_code=response.status_code,
            retry_after_seconds=retry_after,
        )
    if response.status_code == 403:
        raise ProviderAccessDeniedError(
            provider,
            dataset,
            status_code=response.status_code,
            retry_after_seconds=retry_after,
        )
    if response.status_code == 429:
        raise ProviderRateLimitError(
            provider,
            dataset,
            status_code=response.status_code,
            retry_after_seconds=retry_after,
        )
    raise ProviderHttpError(
        provider,
        dataset,
        status_code=response.status_code,
        retry_after_seconds=retry_after,
    )


def parse_json_object(provider: str, dataset: str, content: bytes) -> dict[str, JsonValue]:
    try:
        parsed = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderResponseError(provider, dataset, "response is not valid JSON") from error
    if not isinstance(parsed, dict) or not all(isinstance(key, str) for key in parsed):
        raise ProviderResponseError(provider, dataset, "response JSON must be an object")
    return cast(dict[str, JsonValue], parsed)


def safe_provider_request_id(response: HttpResponse) -> str | None:
    for header_name in ("x-request-id", "request-id", "apca-request-id"):
        value = response.headers.get(header_name)
        if value is not None and _SAFE_REQUEST_ID.fullmatch(value):
            return value
    try:
        parsed = json.loads(response.body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    raw_value = parsed.get("request_id")
    if isinstance(raw_value, str):
        value = raw_value
    elif isinstance(raw_value, int) and not isinstance(raw_value, bool):
        value = str(raw_value)
    else:
        return None
    return value if _SAFE_REQUEST_ID.fullmatch(value) else None


def raw_batch_from_response(
    *,
    source: DataSource,
    response: HttpResponse,
    request_metadata: Mapping[str, JsonScalar],
    clock: Clock,
    batch_id_factory: BatchIdFactory,
) -> RawBatch:
    response_metadata: dict[str, JsonScalar] = {
        **request_metadata,
        "response_status": response.status_code,
        "latency_ms": round(response.elapsed_ms, 3),
    }
    header_mappings = {
        "rate_limit_capacity": ("x-ratelimit-limit", "x-rate-limit-limit"),
        "rate_limit_remaining": ("x-ratelimit-remaining", "x-rate-limit-remaining"),
        "rate_limit_reset": ("x-ratelimit-reset", "x-rate-limit-reset"),
    }
    for metadata_key, header_names in header_mappings.items():
        value = safe_numeric_header(response, *header_names)
        if value is not None:
            response_metadata[metadata_key] = value

    metadata = RawBatchMetadata(
        batch_id=batch_id_factory(),
        source=source,
        retrieved_at=clock(),
        media_type="application/json",
        file_extension="json",
        provider_request_id=safe_provider_request_id(response),
        request_metadata=response_metadata,
    )
    return RawBatch(metadata=metadata, payload=BytesRawPayload(response.body))


__all__ = [
    "BatchIdFactory",
    "Clock",
    "JsonValue",
    "new_batch_id",
    "parse_json_object",
    "raw_batch_from_response",
    "require_success",
    "retry_after_seconds",
    "safe_numeric_header",
    "safe_provider_request_id",
    "utc_now",
]
