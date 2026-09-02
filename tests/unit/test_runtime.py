"""Unit tests for explicit runtime profiles and capability gates."""

from collections.abc import Iterator, Mapping
from pathlib import Path

import pytest

from investment_platform.runtime import (
    DATA_ROOT_VARIABLE,
    ENVIRONMENT_VARIABLE,
    RuntimeCapabilities,
    RuntimeCapabilityError,
    RuntimeConfigurationError,
    RuntimeEnvironment,
    capability_matrix,
    resolve_runtime_settings,
)

pytestmark = pytest.mark.unit


class _RootAccessSentinel(Mapping[str, str]):
    """Expose an environment selector while failing if the data-root key is inspected."""

    def __init__(self, environment: RuntimeEnvironment) -> None:
        self._environment: str = environment.value

    def __getitem__(self, key: str) -> str:
        if key == DATA_ROOT_VARIABLE:
            raise AssertionError("the selected profile must not inspect the data-root setting")
        if key == ENVIRONMENT_VARIABLE:
            return self._environment
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        yield ENVIRONMENT_VARIABLE

    def __len__(self) -> int:
        return 1


_EXPECTED_CAPABILITIES = {
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


def test_capability_matrix_covers_every_profile_with_the_approved_gates() -> None:
    matrix = capability_matrix()

    assert dict(matrix) == _EXPECTED_CAPABILITIES
    assert set(matrix) == set(RuntimeEnvironment)


@pytest.mark.parametrize("environ", [{}, {ENVIRONMENT_VARIABLE: "   "}])
def test_development_is_the_non_explicit_default(environ: Mapping[str, str]) -> None:
    settings = resolve_runtime_settings(environ)

    assert settings.environment is RuntimeEnvironment.DEVELOPMENT
    assert settings.data_root is None
    assert settings.environment_was_explicit is False


@pytest.mark.parametrize("environ", [{}, {ENVIRONMENT_VARIABLE: "   "}])
def test_mutating_callers_can_require_an_explicit_environment(
    environ: Mapping[str, str],
) -> None:
    with pytest.raises(RuntimeConfigurationError, match=ENVIRONMENT_VARIABLE):
        resolve_runtime_settings(environ, require_explicit_environment=True)


def test_unknown_environment_fails_closed_and_lists_supported_profiles() -> None:
    with pytest.raises(RuntimeConfigurationError, match="unsupported") as caught:
        resolve_runtime_settings({ENVIRONMENT_VARIABLE: "production"})

    for environment in RuntimeEnvironment:
        assert environment.value in str(caught.value)


@pytest.mark.parametrize("root_value", [None, "", "   "])
def test_private_research_requires_a_configured_data_root(root_value: str | None) -> None:
    environ = {ENVIRONMENT_VARIABLE: RuntimeEnvironment.PRIVATE_RESEARCH.value}
    if root_value is not None:
        environ[DATA_ROOT_VARIABLE] = root_value

    with pytest.raises(RuntimeConfigurationError, match=f"{DATA_ROOT_VARIABLE} is required"):
        resolve_runtime_settings(environ)


def test_private_research_rejects_a_relative_data_root() -> None:
    with pytest.raises(RuntimeConfigurationError, match="absolute path"):
        resolve_runtime_settings(
            {
                ENVIRONMENT_VARIABLE: RuntimeEnvironment.PRIVATE_RESEARCH.value,
                DATA_ROOT_VARIABLE: str(Path("relative") / "private-root"),
            }
        )


def test_private_research_accepts_an_absolute_data_root(tmp_path: Path) -> None:
    root = tmp_path / "private-root"

    settings = resolve_runtime_settings(
        {
            ENVIRONMENT_VARIABLE: RuntimeEnvironment.PRIVATE_RESEARCH.value,
            DATA_ROOT_VARIABLE: str(root),
        }
    )

    assert settings.environment is RuntimeEnvironment.PRIVATE_RESEARCH
    assert settings.data_root == root
    assert settings.environment_was_explicit is True


@pytest.mark.parametrize("root_value", ["", "configured-but-forbidden"])
def test_ci_rejects_any_configured_data_root(root_value: str) -> None:
    with pytest.raises(RuntimeConfigurationError, match="must not be configured"):
        resolve_runtime_settings(
            {
                ENVIRONMENT_VARIABLE: RuntimeEnvironment.CI.value,
                DATA_ROOT_VARIABLE: root_value,
            }
        )


@pytest.mark.parametrize("environment", [RuntimeEnvironment.TEST, RuntimeEnvironment.DEMO])
def test_synthetic_profiles_do_not_read_the_data_root_key(
    environment: RuntimeEnvironment,
) -> None:
    settings = resolve_runtime_settings(_RootAccessSentinel(environment))

    assert settings.environment is environment
    assert settings.data_root is None
    assert settings.capabilities.synthetic_only is True


def test_development_ignores_the_data_root_key() -> None:
    settings = resolve_runtime_settings(_RootAccessSentinel(RuntimeEnvironment.DEVELOPMENT))

    assert settings.environment is RuntimeEnvironment.DEVELOPMENT
    assert settings.data_root is None
    assert settings.capabilities.private_root is False
    assert settings.capabilities.durable_real_data is False


def test_private_research_capability_gates_return_its_root(tmp_path: Path) -> None:
    root = tmp_path / "private-root"
    settings = resolve_runtime_settings(
        {
            ENVIRONMENT_VARIABLE: RuntimeEnvironment.PRIVATE_RESEARCH.value,
            DATA_ROOT_VARIABLE: str(root),
        }
    )

    settings.require_provider_access()
    settings.require_provider_access(development_preflight=True)
    assert settings.require_private_root() == root
    assert settings.require_durable_real_data() == root


def test_development_allows_only_the_explicit_live_preflight_gate() -> None:
    settings = resolve_runtime_settings({ENVIRONMENT_VARIABLE: "development"})

    with pytest.raises(RuntimeCapabilityError, match="network access"):
        settings.require_provider_access()
    settings.require_provider_access(development_preflight=True)
    with pytest.raises(RuntimeCapabilityError, match="private data root"):
        settings.require_private_root()
    with pytest.raises(RuntimeCapabilityError, match="persist real provider data"):
        settings.require_durable_real_data()


@pytest.mark.parametrize(
    "environment",
    [RuntimeEnvironment.TEST, RuntimeEnvironment.CI, RuntimeEnvironment.DEMO],
)
def test_offline_profiles_reject_provider_root_and_durable_data_capabilities(
    environment: RuntimeEnvironment,
) -> None:
    settings = resolve_runtime_settings({ENVIRONMENT_VARIABLE: environment.value})

    with pytest.raises(RuntimeCapabilityError, match="network access"):
        settings.require_provider_access()
    with pytest.raises(RuntimeCapabilityError, match="network access"):
        settings.require_provider_access(development_preflight=True)
    with pytest.raises(RuntimeCapabilityError, match="private data root"):
        settings.require_private_root()
    with pytest.raises(RuntimeCapabilityError, match="persist real provider data"):
        settings.require_durable_real_data()
