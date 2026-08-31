"""Explicit runtime profiles and their non-secret capability boundary."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Final

ENVIRONMENT_VARIABLE: Final = "INVESTMENT_PLATFORM_ENV"
DATA_ROOT_VARIABLE: Final = "INVESTMENT_PLATFORM_DATA_ROOT"


class RuntimeConfigurationError(RuntimeError):
    """Raised when the selected runtime profile is missing or unsafe."""


class RuntimeCapabilityError(RuntimeError):
    """Raised when code asks a runtime profile for a forbidden capability."""


class RuntimeEnvironment(StrEnum):
    """Named execution profiles; names remain distinct even where behavior is shared."""

    TEST = "test"
    CI = "ci"
    DEVELOPMENT = "development"
    PRIVATE_RESEARCH = "private_research"
    DEMO = "demo"


@dataclass(frozen=True, slots=True)
class RuntimeCapabilities:
    """The maximum capabilities granted by one profile.

    A caller may choose to use fewer capabilities. It cannot expand this row at runtime.
    """

    network: bool
    development_live_preflight: bool
    provider_credentials: bool
    private_root: bool
    durable_real_data: bool
    synthetic_only: bool
    restrictions_overridable: bool


_CAPABILITIES: Final[Mapping[RuntimeEnvironment, RuntimeCapabilities]] = MappingProxyType(
    {
        RuntimeEnvironment.TEST: RuntimeCapabilities(
            network=False,
            development_live_preflight=False,
            provider_credentials=False,
            private_root=False,
            durable_real_data=False,
            synthetic_only=True,
            restrictions_overridable=False,
        ),
        RuntimeEnvironment.CI: RuntimeCapabilities(
            network=False,
            development_live_preflight=False,
            provider_credentials=False,
            private_root=False,
            durable_real_data=False,
            synthetic_only=True,
            restrictions_overridable=False,
        ),
        RuntimeEnvironment.DEVELOPMENT: RuntimeCapabilities(
            network=False,
            development_live_preflight=True,
            provider_credentials=True,
            private_root=False,
            durable_real_data=False,
            synthetic_only=False,
            restrictions_overridable=False,
        ),
        RuntimeEnvironment.PRIVATE_RESEARCH: RuntimeCapabilities(
            network=True,
            development_live_preflight=False,
            provider_credentials=True,
            private_root=True,
            durable_real_data=True,
            synthetic_only=False,
            restrictions_overridable=False,
        ),
        RuntimeEnvironment.DEMO: RuntimeCapabilities(
            network=False,
            development_live_preflight=False,
            provider_credentials=False,
            private_root=False,
            durable_real_data=False,
            synthetic_only=True,
            restrictions_overridable=False,
        ),
    }
)


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """Resolved non-secret settings for one process.

    Provider credentials deliberately do not belong to this object. A permitted provider
    constructor reads its existing environment variables only after the capability gate passes.
    """

    environment: RuntimeEnvironment
    data_root: Path | None
    environment_was_explicit: bool

    @property
    def capabilities(self) -> RuntimeCapabilities:
        return _CAPABILITIES[self.environment]

    def require_private_root(self) -> Path:
        if not self.capabilities.private_root or self.data_root is None:
            raise RuntimeCapabilityError(
                f"profile {self.environment.value!r} cannot access a private data root"
            )
        return self.data_root

    def require_provider_access(self, *, development_preflight: bool = False) -> None:
        """Fail before credentials are read or a provider is constructed."""

        if self.capabilities.network:
            return
        if development_preflight and self.capabilities.development_live_preflight:
            return
        raise RuntimeCapabilityError(
            f"profile {self.environment.value!r} does not permit provider network access"
        )

    def require_durable_real_data(self) -> Path:
        if not self.capabilities.durable_real_data:
            raise RuntimeCapabilityError(
                f"profile {self.environment.value!r} cannot persist real provider data"
            )
        return self.require_private_root()


def capability_matrix() -> Mapping[RuntimeEnvironment, RuntimeCapabilities]:
    """Expose an immutable capability matrix for diagnostics and policy checks."""

    return _CAPABILITIES


def resolve_runtime_settings(
    environ: Mapping[str, str] | None = None,
    *,
    require_explicit_environment: bool = False,
) -> RuntimeSettings:
    """Resolve the two Phase 2 settings without loading dotenv files or credentials.

    ``development`` is the safe interactive default. Mutating CLI commands pass
    ``require_explicit_environment=True`` so a missing selector cannot accidentally turn into a
    different execution mode.
    """

    values = os.environ if environ is None else environ
    environment_value = values.get(ENVIRONMENT_VARIABLE)
    if environment_value is None or not environment_value.strip():
        environment_was_explicit = False
        if require_explicit_environment:
            raise RuntimeConfigurationError(f"{ENVIRONMENT_VARIABLE} is required for this command")
        environment = RuntimeEnvironment.DEVELOPMENT
    else:
        environment_was_explicit = True
        try:
            environment = RuntimeEnvironment(environment_value.strip().lower())
        except ValueError as error:
            allowed = ", ".join(member.value for member in RuntimeEnvironment)
            raise RuntimeConfigurationError(
                f"unsupported {ENVIRONMENT_VARIABLE}; expected one of: {allowed}"
            ) from error

    # Test and demo deliberately never resolve or inspect the host's possibly stale root value.
    if environment in {RuntimeEnvironment.TEST, RuntimeEnvironment.DEMO}:
        return RuntimeSettings(
            environment=environment,
            data_root=None,
            environment_was_explicit=environment_was_explicit,
        )

    if environment is RuntimeEnvironment.CI:
        if DATA_ROOT_VARIABLE in values:
            raise RuntimeConfigurationError(
                f"{DATA_ROOT_VARIABLE} must not be configured in the ci profile"
            )
        return RuntimeSettings(
            environment=environment,
            data_root=None,
            environment_was_explicit=environment_was_explicit,
        )

    if environment is RuntimeEnvironment.DEVELOPMENT:
        # Merely having a host path configured must never enable durable data in development.
        return RuntimeSettings(
            environment=environment,
            data_root=None,
            environment_was_explicit=environment_was_explicit,
        )

    root_value = values.get(DATA_ROOT_VARIABLE, "").strip()
    if not root_value:
        raise RuntimeConfigurationError(
            f"{DATA_ROOT_VARIABLE} is required in the private_research profile"
        )
    data_root = Path(root_value)
    if not data_root.is_absolute():
        raise RuntimeConfigurationError(f"{DATA_ROOT_VARIABLE} must be an absolute path")
    return RuntimeSettings(
        environment=environment,
        data_root=data_root,
        environment_was_explicit=environment_was_explicit,
    )


__all__ = [
    "DATA_ROOT_VARIABLE",
    "ENVIRONMENT_VARIABLE",
    "RuntimeCapabilities",
    "RuntimeCapabilityError",
    "RuntimeConfigurationError",
    "RuntimeEnvironment",
    "RuntimeSettings",
    "capability_matrix",
    "resolve_runtime_settings",
]
