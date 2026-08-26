"""Sanitized errors exposed by real market-data provider adapters."""

from __future__ import annotations


class ProviderError(RuntimeError):
    """Base provider failure that never embeds request URLs or credentials."""

    def __init__(self, provider: str, dataset: str, message: str) -> None:
        self.provider = provider
        self.dataset = dataset
        self.safe_message = message
        super().__init__(f"{provider} {dataset}: {message}")


class ProviderConfigurationError(ProviderError):
    """Raised when required local configuration is missing or invalid."""


class ProviderCapabilityError(ProviderError):
    """Raised when a provider cannot honor a canonical request explicitly."""


class ProviderTransportError(ProviderError):
    """Raised when no HTTP response can be obtained safely."""


class ProviderResponseError(ProviderError):
    """Raised when a successful response is not valid provider JSON."""


class ProviderHttpError(ProviderError):
    """A non-success HTTP result with sanitized operational fields."""

    def __init__(
        self,
        provider: str,
        dataset: str,
        *,
        status_code: int,
        retry_after_seconds: float | None = None,
    ) -> None:
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        super().__init__(provider, dataset, f"HTTP request failed with status {status_code}")


class ProviderAccessDeniedError(ProviderHttpError):
    """Access was denied without enough evidence to separate auth from entitlement."""


class ProviderAuthenticationError(ProviderHttpError):
    """Authentication was missing or rejected."""


class ProviderEntitlementError(ProviderHttpError):
    """The authenticated account is not entitled to the requested dataset."""


class ProviderRateLimitError(ProviderHttpError):
    """The provider rejected a request because its rate limit was reached."""


__all__ = [
    "ProviderAccessDeniedError",
    "ProviderAuthenticationError",
    "ProviderCapabilityError",
    "ProviderConfigurationError",
    "ProviderEntitlementError",
    "ProviderError",
    "ProviderHttpError",
    "ProviderRateLimitError",
    "ProviderResponseError",
    "ProviderTransportError",
]
