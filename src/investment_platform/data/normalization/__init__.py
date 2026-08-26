"""Provider-specific normalization into the Phase 0 canonical contracts."""

from investment_platform.data.normalization.alpaca import (
    normalize_alpaca_bars,
    normalize_alpaca_corporate_actions,
)
from investment_platform.data.normalization.common import (
    BarNormalizationResult,
    CorporateActionNormalizationResult,
    DailyBarSemantics,
    NormalizationError,
    NormalizationIssue,
    NormalizationIssueCode,
    NormalizationSeverity,
    SessionBounds,
    StaticSessionSchedule,
)
from investment_platform.data.normalization.massive import (
    normalize_massive_bars,
    normalize_massive_corporate_actions,
)
from investment_platform.data.normalization.twelve_data import normalize_twelve_data_bars

__all__ = [
    "BarNormalizationResult",
    "CorporateActionNormalizationResult",
    "DailyBarSemantics",
    "NormalizationError",
    "NormalizationIssue",
    "NormalizationIssueCode",
    "NormalizationSeverity",
    "SessionBounds",
    "StaticSessionSchedule",
    "normalize_alpaca_bars",
    "normalize_alpaca_corporate_actions",
    "normalize_massive_bars",
    "normalize_massive_corporate_actions",
    "normalize_twelve_data_bars",
]
