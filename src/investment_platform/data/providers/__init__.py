"""Vendor-independent market-data provider contracts."""

from investment_platform.data.providers.alpaca import (
    AlpacaCredentials,
    AlpacaEvidenceAdjustment,
    AlpacaFeed,
    AlpacaProvider,
    AlpacaSipPreflightOutcome,
    AlpacaSipPreflightResult,
)
from investment_platform.data.providers.base import (
    BarRequest,
    CorporateActionRequest,
    MarketDataProvider,
    ProviderInstrumentRef,
)
from investment_platform.data.providers.errors import (
    ProviderAccessDeniedError,
    ProviderAuthenticationError,
    ProviderCapabilityError,
    ProviderConfigurationError,
    ProviderEntitlementError,
    ProviderError,
    ProviderHttpError,
    ProviderRateLimitError,
    ProviderResponseError,
    ProviderTransportError,
)
from investment_platform.data.providers.massive import MassiveCredentials, MassiveProvider
from investment_platform.data.providers.twelve_data import (
    TwelveDataCredentials,
    TwelveDataEvidenceAdjustment,
    TwelveDataProvider,
)

__all__ = [
    "AlpacaCredentials",
    "AlpacaEvidenceAdjustment",
    "AlpacaFeed",
    "AlpacaProvider",
    "AlpacaSipPreflightOutcome",
    "AlpacaSipPreflightResult",
    "BarRequest",
    "CorporateActionRequest",
    "MarketDataProvider",
    "MassiveCredentials",
    "MassiveProvider",
    "ProviderAccessDeniedError",
    "ProviderAuthenticationError",
    "ProviderCapabilityError",
    "ProviderConfigurationError",
    "ProviderEntitlementError",
    "ProviderError",
    "ProviderHttpError",
    "ProviderInstrumentRef",
    "ProviderRateLimitError",
    "ProviderResponseError",
    "ProviderTransportError",
    "TwelveDataCredentials",
    "TwelveDataEvidenceAdjustment",
    "TwelveDataProvider",
]
