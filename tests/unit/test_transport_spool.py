"""Offline safety tests for attempt-scoped transient provider spooling."""

from __future__ import annotations

from email.message import Message
from pathlib import Path
from typing import Self, cast
from urllib.request import OpenerDirector, Request
from uuid import UUID, uuid4

import pytest

from investment_platform.data.providers.errors import ProviderTransportError
from investment_platform.data.providers.http import (
    ResponseBodyNotMaterializedError,
    SpoolingUrllibHttpTransport,
)
from investment_platform.data.storage.transport_spool import (
    TransportSpoolFaultPoint,
    TransportSpoolInspectionState,
    TransportSpoolIntegrityError,
    TransportSpoolPayload,
    TransportSpoolStore,
    TransportSpoolTooLargeError,
)
from investment_platform.data_root import PrivateDataRoot

pytestmark = pytest.mark.unit

_NOW_ATTEMPT = UUID("60000000-0000-4000-8000-000000000001")
_NEXT_ATTEMPT = UUID("60000000-0000-4000-8000-000000000002")
_RESPONSE_ID = UUID("70000000-0000-4000-8000-000000000001")


def _root(tmp_path: Path) -> PrivateDataRoot:
    repository = Path(__file__).parents[2].resolve(strict=True)
    root = PrivateDataRoot(
        tmp_path.parent / f"p2-transport-{uuid4().hex[:8]}",
        repository,
        allow_temporary_for_tests=True,
    )
    root.initialize()
    return root


class _BoundedReader:
    def __init__(self, content: bytes) -> None:
        self._content = content
        self._offset = 0
        self.requested_sizes: list[int] = []

    def read(self, size: int = -1, /) -> bytes:
        if size <= 0:
            raise AssertionError("transport must issue positive bounded reads")
        self.requested_sizes.append(size)
        start = self._offset
        self._offset += size
        return self._content[start : self._offset]


class _SyntheticResponse(_BoundedReader):
    status = 200

    def __init__(self, content: bytes) -> None:
        super().__init__(content)
        self.headers = Message()
        self.headers["X-Request-ID"] = "synthetic-request"

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        del args


class _SyntheticOpener:
    def __init__(self, response: _SyntheticResponse) -> None:
        self.response = response
        self.requests: list[Request] = []

    def open(self, request: Request, *, timeout: float) -> _SyntheticResponse:
        assert timeout > 0
        self.requests.append(request)
        return self.response


class _InjectedCrash(BaseException):
    pass


def test_spool_streams_bounded_bytes_and_cleans_after_attempt(tmp_path: Path) -> None:
    root = _root(tmp_path)
    store = TransportSpoolStore(
        root,
        chunk_size=3,
        uuid_factory=lambda: _RESPONSE_ID,
    )
    reader = _BoundedReader(b"synthetic-payload")

    with store.attempt(_NOW_ATTEMPT) as attempt:
        payload = attempt.spool(reader, maximum_bytes=64)
        path = root.root / Path(payload.relative_path)
        assert path.is_file()
        assert str(_NOW_ATTEMPT) in payload.relative_path
        assert "synthetic-payload" not in payload.relative_path
        assert max(reader.requested_sizes) <= 3
        with payload.open_binary() as reopened:
            assert reopened.read() == b"synthetic-payload"

    assert not (root.root / "staging" / "transport-attempts" / str(_NOW_ATTEMPT)).exists()


def test_abrupt_spool_crash_is_removed_before_next_attempt(tmp_path: Path) -> None:
    root = _root(tmp_path)
    store = TransportSpoolStore(
        root,
        chunk_size=4,
        uuid_factory=lambda: _RESPONSE_ID,
    )

    def crash(point: TransportSpoolFaultPoint) -> None:
        if point is TransportSpoolFaultPoint.DURING_WRITE:
            raise _InjectedCrash

    with (
        pytest.raises(_InjectedCrash),
        store.attempt(_NOW_ATTEMPT, fault_injector=crash) as attempt,
    ):
        attempt.spool(_BoundedReader(b"synthetic-payload"), maximum_bytes=64)

    orphan = root.root / "staging" / "transport-attempts" / str(_NOW_ATTEMPT)
    assert tuple(orphan.glob("response-*.part"))

    with store.attempt(_NEXT_ATTEMPT):
        assert not orphan.exists()

    assert not (root.root / "staging" / "transport-attempts" / str(_NEXT_ATTEMPT)).exists()


def test_read_only_inspection_reports_valid_residual_attempt_without_deleting_it(
    tmp_path: Path,
) -> None:
    root = _root(tmp_path)
    store = TransportSpoolStore(root)

    def crash(point: TransportSpoolFaultPoint) -> None:
        if point is TransportSpoolFaultPoint.ATTEMPT_READY:
            raise _InjectedCrash

    with pytest.raises(_InjectedCrash), store.attempt(_NOW_ATTEMPT, fault_injector=crash):
        pass
    attempt = root.root / "staging" / "transport-attempts" / str(_NOW_ATTEMPT)
    owner_before = (attempt / ".attempt-owner.json").read_bytes()

    inspections = store.inspect_transient_attempts()

    assert tuple(value.state for value in inspections) == (
        TransportSpoolInspectionState.RECOVERY_REQUIRED,
    )
    assert attempt.is_dir()
    assert (attempt / ".attempt-owner.json").read_bytes() == owner_before


@pytest.mark.parametrize("corruption", ["name", "owner", "hardlink", "special"])
def test_read_only_inspection_fails_closed_for_invalid_attempt_entries(
    tmp_path: Path,
    corruption: str,
) -> None:
    root = _root(tmp_path)
    store = TransportSpoolStore(root)

    def crash(point: TransportSpoolFaultPoint) -> None:
        if point is TransportSpoolFaultPoint.ATTEMPT_READY:
            raise _InjectedCrash

    with pytest.raises(_InjectedCrash), store.attempt(_NOW_ATTEMPT, fault_injector=crash):
        pass
    parent = root.root / "staging" / "transport-attempts"
    attempt = parent / str(_NOW_ATTEMPT)
    if corruption == "name":
        (parent / "not-an-attempt").mkdir()
    elif corruption == "owner":
        (attempt / ".attempt-owner.json").write_text("{}", encoding="utf-8")
    elif corruption == "hardlink":
        source = root.root / "logs" / "synthetic-hardlink-source.bin"
        source.write_bytes(b"synthetic")
        (attempt / f"response-{_RESPONSE_ID.hex}.bin").hardlink_to(source)
    else:
        (attempt / f"response-{_RESPONSE_ID.hex}.bin").mkdir()

    inspections = store.inspect_transient_attempts()

    assert TransportSpoolInspectionState.INVALID in {value.state for value in inspections}
    assert attempt.exists()


def test_expected_oversize_failure_leaves_no_transient_attempt(tmp_path: Path) -> None:
    root = _root(tmp_path)
    store = TransportSpoolStore(root, chunk_size=3)

    with (
        pytest.raises(TransportSpoolTooLargeError, match="byte ceiling"),
        store.attempt(_NOW_ATTEMPT) as attempt,
    ):
        attempt.spool(_BoundedReader(b"12345"), maximum_bytes=4)

    assert not (root.root / "staging" / "transport-attempts" / str(_NOW_ATTEMPT)).exists()


def test_recovery_fails_closed_on_unowned_spool_filename(tmp_path: Path) -> None:
    root = _root(tmp_path)
    store = TransportSpoolStore(root)
    attempt = root.root / "staging" / "transport-attempts" / str(_NOW_ATTEMPT)
    attempt.mkdir(parents=True)
    (attempt / "do-not-delete.txt").write_text("synthetic", encoding="utf-8")

    with pytest.raises(TransportSpoolIntegrityError, match="unowned filename"):
        store.recover_transient_attempts()

    assert (attempt / "do-not-delete.txt").is_file()


def test_spooling_http_response_is_file_backed_only_inside_attempt(tmp_path: Path) -> None:
    root = _root(tmp_path)
    response_reader = _SyntheticResponse(b'{"bars":{},"next_page_token":null}')
    opener = _SyntheticOpener(response_reader)
    transport = SpoolingUrllibHttpTransport(root, maximum_response_bytes=1024)
    transport._opener = cast(OpenerDirector, opener)

    with pytest.raises(ProviderTransportError, match="attempt scope"):
        transport.get(
            provider="alpaca",
            dataset="price_bars_sip",
            base_url="https://data.alpaca.markets",
            path="/v2/stocks/bars",
            query=(),
            headers={"APCA-API-KEY-ID": "synthetic-secret"},
            timeout_seconds=1,
        )

    with transport.attempt_scope(_NOW_ATTEMPT):
        response = transport.get(
            provider="alpaca",
            dataset="price_bars_sip",
            base_url="https://data.alpaca.markets",
            path="/v2/stocks/bars",
            query=(("symbols", "XPH1"),),
            headers={"APCA-API-KEY-ID": "synthetic-secret"},
            timeout_seconds=1,
        )
        assert response.is_file_backed is True
        with pytest.raises(ResponseBodyNotMaterializedError):
            _ = response.body
        with response.raw_payload.open_binary() as reader:
            assert reader.read() == b'{"bars":{},"next_page_token":null}'
        assert isinstance(response.raw_payload, TransportSpoolPayload)
        assert "synthetic-secret" not in response.raw_payload.relative_path

    with (
        pytest.raises(TransportSpoolIntegrityError, match="absent"),
        response.raw_payload.open_binary() as reader,
    ):
        reader.read(1)
