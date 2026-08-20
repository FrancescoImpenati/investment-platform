"""Deterministic HTTP fakes shared by offline provider tests."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from investment_platform.data.providers.http import HttpResponse, QueryParameters


@dataclass(frozen=True, slots=True)
class CapturedRequest:
    provider: str
    dataset: str
    base_url: str
    path: str
    query: QueryParameters
    headers: Mapping[str, str]
    timeout_seconds: float


class QueueHttpTransport:
    """Return queued responses and retain request objects only inside a test."""

    def __init__(self, responses: list[HttpResponse]) -> None:
        self._responses = list(responses)
        self.requests: list[CapturedRequest] = []

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
        self.requests.append(
            CapturedRequest(
                provider=provider,
                dataset=dataset,
                base_url=base_url,
                path=path,
                query=query,
                headers=dict(headers),
                timeout_seconds=timeout_seconds,
            )
        )
        if not self._responses:
            raise AssertionError("fake transport has no queued response")
        return self._responses.pop(0)


__all__ = ["CapturedRequest", "QueueHttpTransport"]
