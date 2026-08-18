"""Suite-wide safety fixtures."""

import socket
from collections.abc import Generator
from typing import NoReturn

import pytest


def _blocked_network(*_args: object, **_kwargs: object) -> NoReturn:
    raise AssertionError("network access is forbidden in the test suite")


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
    """Fail tests that attempt to open a network connection."""

    monkeypatch.setattr(socket, "create_connection", _blocked_network)
    monkeypatch.setattr(socket.socket, "connect", _blocked_network)
    monkeypatch.setattr(socket.socket, "connect_ex", _blocked_network)
    yield
