"""Attempt-scoped transient transport spooling for living ingestion.

Transport spools are deliberately *not* raw evidence.  They exist only below the
validated private root while one provider attempt is inspecting and adopting a
bounded response.  Immutable raw publication remains the first durable use of
provider bytes.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Final, Protocol
from uuid import UUID, uuid4

from investment_platform.data.storage._publication import (
    PublicationError,
    PublicationIntegrityError,
    assert_direct_owned_directory,
    ensure_directory,
    fsync_directory,
    managed_path,
    remove_owned_staging_directory,
    write_file_durably,
)
from investment_platform.data_root import PrivateDataRoot, UnsafePrivateDataRootError

_TRANSPORT_PARENT: Final = PurePosixPath("staging/transport-attempts")
_OWNER_NAME: Final = ".attempt-owner.json"
_OWNER_TEMP_NAME: Final = ".attempt-owner.tmp"
_RESPONSE_NAME: Final = re.compile(r"^response-[0-9a-f]{32}\.(?:part|bin)$")
_DEFAULT_CHUNK_SIZE: Final = 64 * 1024


class TransportSpoolError(RuntimeError):
    """Transient bytes could not be handled within the private spool contract."""


class TransportSpoolTooLargeError(TransportSpoolError):
    """A response crossed its explicit transport byte ceiling."""


class TransportSpoolIntegrityError(TransportSpoolError):
    """The transient namespace is no longer an exact platform-owned target."""


class TransportSpoolFaultPoint(StrEnum):
    """Stable crash boundaries for offline transport fault injection."""

    ATTEMPT_READY = "attempt_ready"
    DURING_WRITE = "during_write"
    FILE_DURABLE = "file_durable"


class TransportSpoolInspectionState(StrEnum):
    """Sanitized read-only state of one residual attempt namespace entry."""

    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
    INVALID = "INVALID"


type TransportSpoolFaultInjector = Callable[[TransportSpoolFaultPoint], None]
type UuidFactory = Callable[[], UUID]


class _Readable(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...


@dataclass(frozen=True, slots=True)
class TransportSpoolInspection:
    """One identity-free inspection result safe for operational diagnostics."""

    state: TransportSpoolInspectionState


def _invoke(
    injector: TransportSpoolFaultInjector | None,
    point: TransportSpoolFaultPoint,
) -> None:
    if injector is not None:
        injector(point)


def _is_link_or_reparse(path: Path, details: os.stat_result) -> bool:
    attributes = getattr(details, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(
        path.is_symlink()
        or getattr(path, "is_junction", lambda: False)()
        or (reparse_flag and attributes & reparse_flag)
    )


@dataclass(frozen=True, slots=True)
class TransportSpoolPayload:
    """Reopen one bounded transient file while its attempt scope is active."""

    attempt_id: UUID
    relative_path: str
    content_sha256: str
    byte_count: int
    _store: TransportSpoolStore = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        expected_prefix = f"staging/transport-attempts/{self.attempt_id}/"
        if not self.relative_path.startswith(expected_prefix):
            raise ValueError("transport payload is outside its exact attempt")
        if not re.fullmatch(r"[0-9a-f]{64}", self.content_sha256):
            raise ValueError("transport payload checksum is invalid")
        if self.byte_count < 0:
            raise ValueError("transport payload byte count must not be negative")

    def open_binary(self) -> AbstractContextManager[BinaryIO]:
        """Return a validated reader; no response bytes are materialized here."""

        return self._store.open_payload(self)


@dataclass(frozen=True, slots=True)
class AttemptTransportSpool:
    """One prepared attempt directory with bounded response writers."""

    attempt_id: UUID
    _store: TransportSpoolStore = field(repr=False, compare=False)
    _fault_injector: TransportSpoolFaultInjector | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def spool(
        self,
        reader: _Readable,
        *,
        maximum_bytes: int,
    ) -> TransportSpoolPayload:
        return self._store.spool(
            self.attempt_id,
            reader,
            maximum_bytes=maximum_bytes,
            fault_injector=self._fault_injector,
        )


class TransportSpoolStore:
    """Own the non-durable, single-writer transport namespace."""

    def __init__(
        self,
        data_root: PrivateDataRoot,
        *,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
        uuid_factory: UuidFactory = uuid4,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        sentinel = data_root.validate()
        self._data_root = data_root
        self._root_id = sentinel.root_id
        self._chunk_size = chunk_size
        self._uuid_factory = uuid_factory

    @property
    def root_id(self) -> UUID:
        return self._root_id

    def _parent(self, *, create: bool) -> Path:
        if create:
            return ensure_directory(self._data_root, self._root_id, _TRANSPORT_PARENT)
        parent = managed_path(self._data_root, self._root_id, _TRANSPORT_PARENT)
        if parent.exists() or parent.is_symlink():
            assert_direct_owned_directory(parent, parent=parent.parent)
        return parent

    def _attempt_path(self, attempt_id: UUID) -> Path:
        return managed_path(
            self._data_root,
            self._root_id,
            _TRANSPORT_PARENT / str(attempt_id),
        )

    @staticmethod
    def _owner_payload(root_id: UUID, attempt_id: UUID) -> bytes:
        value = {
            "attempt_id": str(attempt_id),
            "root_id": str(root_id),
            "schema_version": 1,
        }
        return (
            json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode()

    def _assert_attempt_entries(
        self,
        attempt: Path,
        attempt_id: UUID,
        *,
        require_owner: bool,
    ) -> None:
        parent = self._parent(create=False)
        if attempt.parent != parent:
            raise UnsafePrivateDataRootError("transport attempt escaped its exact parent")
        try:
            assert_direct_owned_directory(attempt, parent=parent)
        except PublicationIntegrityError as error:
            raise TransportSpoolIntegrityError("transport attempt is absent or invalid") from error
        try:
            entries = tuple(attempt.iterdir())
        except OSError as error:
            raise TransportSpoolIntegrityError(
                "transport attempt cannot be inspected safely"
            ) from error
        owner = attempt / _OWNER_NAME
        owner_present = False
        for entry in entries:
            try:
                details = entry.lstat()
            except OSError as error:
                raise TransportSpoolIntegrityError(
                    "transport attempt entry cannot be inspected safely"
                ) from error
            if (
                _is_link_or_reparse(entry, details)
                or not stat.S_ISREG(details.st_mode)
                or details.st_nlink != 1
            ):
                raise TransportSpoolIntegrityError(
                    "transport attempt contains a link, hardlink, or special file"
                )
            if entry.name == _OWNER_NAME:
                owner_present = True
            elif entry.name != _OWNER_TEMP_NAME and not _RESPONSE_NAME.fullmatch(entry.name):
                raise TransportSpoolIntegrityError("transport attempt contains an unowned filename")
        if require_owner:
            if not owner_present:
                raise TransportSpoolIntegrityError("transport attempt owner record is absent")
            try:
                actual = owner.read_bytes()
            except OSError as error:
                raise TransportSpoolIntegrityError(
                    "transport attempt owner record is unreadable"
                ) from error
            if actual != self._owner_payload(self._root_id, attempt_id):
                raise TransportSpoolIntegrityError("transport attempt owner record is inconsistent")

    def _prepare_attempt(self, attempt_id: UUID) -> Path:
        parent = self._parent(create=True)
        attempt = self._attempt_path(attempt_id)
        if attempt.exists() or attempt.is_symlink():
            raise TransportSpoolIntegrityError(
                "transport attempt already exists and must be recovered first"
            )
        try:
            attempt.mkdir(exist_ok=False)
            fsync_directory(parent)
            write_file_durably(
                attempt / _OWNER_TEMP_NAME,
                self._owner_payload(self._root_id, attempt_id),
            )
            os.rename(attempt / _OWNER_TEMP_NAME, attempt / _OWNER_NAME)
            fsync_directory(attempt)
        except (OSError, PublicationError) as error:
            raise TransportSpoolError("transport attempt could not be prepared") from error
        self._data_root.validate(expected_root_id=self._root_id)
        self._assert_attempt_entries(attempt, attempt_id, require_owner=True)
        return attempt

    def _safe_unlink(self, path: Path, *, attempt: Path) -> None:
        if path.parent != attempt or not _RESPONSE_NAME.fullmatch(path.name):
            raise UnsafePrivateDataRootError("refusing to remove a non-response spool path")
        self._data_root.validate(expected_root_id=self._root_id)
        if path.exists() or path.is_symlink():
            details = path.lstat()
            if (
                _is_link_or_reparse(path, details)
                or not stat.S_ISREG(details.st_mode)
                or details.st_nlink != 1
            ):
                raise TransportSpoolIntegrityError(
                    "transient response is not a direct single-link regular file"
                )
            path.unlink()
            fsync_directory(attempt)

    def spool(
        self,
        attempt_id: UUID,
        reader: _Readable,
        *,
        maximum_bytes: int,
        fault_injector: TransportSpoolFaultInjector | None = None,
    ) -> TransportSpoolPayload:
        """Stream one response to a bounded file without retaining its bytes in memory."""

        if maximum_bytes <= 0:
            raise ValueError("maximum_bytes must be positive")
        attempt = self._attempt_path(attempt_id)
        self._data_root.validate(expected_root_id=self._root_id)
        self._assert_attempt_entries(attempt, attempt_id, require_owner=True)
        response_id = self._uuid_factory().hex
        partial = attempt / f"response-{response_id}.part"
        complete = attempt / f"response-{response_id}.bin"
        digest = hashlib.sha256()
        byte_count = 0
        try:
            with partial.open("xb") as writer:
                while True:
                    requested = min(self._chunk_size, maximum_bytes - byte_count + 1)
                    chunk = reader.read(requested)
                    if not isinstance(chunk, bytes) or len(chunk) > requested:
                        raise TransportSpoolIntegrityError(
                            "transport reader violated its bounded binary contract"
                        )
                    if not chunk:
                        break
                    byte_count += len(chunk)
                    if byte_count > maximum_bytes:
                        raise TransportSpoolTooLargeError(
                            "provider response crossed its configured transport byte ceiling"
                        )
                    writer.write(chunk)
                    digest.update(chunk)
                    _invoke(fault_injector, TransportSpoolFaultPoint.DURING_WRITE)
                writer.flush()
                os.fsync(writer.fileno())
            os.rename(partial, complete)
            fsync_directory(attempt)
        except (TransportSpoolTooLargeError, TransportSpoolIntegrityError):
            self._safe_unlink(partial, attempt=attempt)
            raise
        except OSError as error:
            self._safe_unlink(partial, attempt=attempt)
            raise TransportSpoolError("provider response could not be spooled safely") from error
        _invoke(fault_injector, TransportSpoolFaultPoint.FILE_DURABLE)
        self._data_root.validate(expected_root_id=self._root_id)
        self._assert_attempt_entries(attempt, attempt_id, require_owner=True)
        return TransportSpoolPayload(
            attempt_id=attempt_id,
            relative_path=complete.relative_to(self._data_root.root).as_posix(),
            content_sha256=digest.hexdigest(),
            byte_count=byte_count,
            _store=self,
        )

    @contextmanager
    def open_payload(self, payload: TransportSpoolPayload) -> Iterator[BinaryIO]:
        """Open only the exact complete response described by ``payload``."""

        if payload._store is not self:
            raise TransportSpoolIntegrityError("transport payload belongs to another root")
        attempt = self._attempt_path(payload.attempt_id)
        self._assert_attempt_entries(attempt, payload.attempt_id, require_owner=True)
        path = managed_path(
            self._data_root,
            self._root_id,
            PurePosixPath(payload.relative_path),
        )
        if path.parent != attempt or path.suffix != ".bin":
            raise TransportSpoolIntegrityError("transport payload path is not complete")
        try:
            details = path.lstat()
        except OSError as error:
            raise TransportSpoolIntegrityError("transport payload is absent") from error
        if (
            _is_link_or_reparse(path, details)
            or not stat.S_ISREG(details.st_mode)
            or details.st_nlink != 1
            or details.st_size != payload.byte_count
        ):
            raise TransportSpoolIntegrityError(
                "transport payload is not an exact single-link regular file"
            )
        try:
            with path.open("rb") as reader:
                yield reader
        except OSError as error:
            raise TransportSpoolIntegrityError("transport payload cannot be read") from error
        self._data_root.validate(expected_root_id=self._root_id)

    def cleanup_attempt(self, attempt_id: UUID) -> None:
        """Delete one exact, validated transient attempt idempotently."""

        parent = self._parent(create=False)
        attempt = self._attempt_path(attempt_id)
        if not (attempt.exists() or attempt.is_symlink()):
            return
        self._assert_attempt_entries(attempt, attempt_id, require_owner=True)
        remove_owned_staging_directory(
            self._data_root,
            self._root_id,
            attempt,
            staging_parent=parent,
        )

    def inspect_transient_attempts(self) -> tuple[TransportSpoolInspection, ...]:
        """Inspect residual transport attempts without adopting or deleting them.

        A structurally complete, platform-owned attempt is recoverable after a
        process crash and therefore produces ``RECOVERY_REQUIRED``.  Any
        non-canonical attempt name, missing/inconsistent owner record, link,
        hardlink, special file, or unknown descendant fails closed as
        ``INVALID``.  Results intentionally omit paths and attempt identities.
        """

        parent = self._parent(create=False)
        if not parent.exists():
            return ()
        try:
            entries = tuple(parent.iterdir())
        except OSError as error:
            raise TransportSpoolIntegrityError(
                "transport namespace cannot be enumerated"
            ) from error
        inspections: list[TransportSpoolInspection] = []
        for entry in sorted(entries, key=lambda value: value.name):
            state = TransportSpoolInspectionState.INVALID
            try:
                attempt_id = UUID(entry.name)
                if str(attempt_id) != entry.name:
                    raise ValueError("transport attempt directory is not canonical")
                self._assert_attempt_entries(entry, attempt_id, require_owner=True)
                if (entry / _OWNER_TEMP_NAME).exists() or (entry / _OWNER_TEMP_NAME).is_symlink():
                    raise TransportSpoolIntegrityError(
                        "transport attempt contains an incomplete owner record"
                    )
            except (OSError, ValueError, TransportSpoolIntegrityError):
                pass
            else:
                state = TransportSpoolInspectionState.RECOVERY_REQUIRED
            inspections.append(TransportSpoolInspection(state=state))
        return tuple(inspections)

    def recover_transient_attempts(self) -> tuple[UUID, ...]:
        """Remove only structurally safe orphan attempts before another dispatch.

        The living-ingestion writer lease guarantees no concurrent active attempt.
        A partial owner record is safe to remove only because both the attempt
        directory and every permitted descendant have exact platform-owned names.
        Unknown entries fail closed.
        """

        parent = self._parent(create=False)
        if not parent.exists():
            return ()
        recovered: list[UUID] = []
        try:
            entries = tuple(parent.iterdir())
        except OSError as error:
            raise TransportSpoolIntegrityError(
                "transport namespace cannot be enumerated"
            ) from error
        for entry in sorted(entries, key=lambda value: value.name):
            try:
                attempt_id = UUID(entry.name)
            except ValueError as error:
                raise TransportSpoolIntegrityError(
                    "transport namespace contains an unknown entry"
                ) from error
            if str(attempt_id) != entry.name:
                raise TransportSpoolIntegrityError("transport attempt directory is not canonical")
            self._assert_attempt_entries(entry, attempt_id, require_owner=False)
            remove_owned_staging_directory(
                self._data_root,
                self._root_id,
                entry,
                staging_parent=parent,
            )
            recovered.append(attempt_id)
        return tuple(recovered)

    @contextmanager
    def attempt(
        self,
        attempt_id: UUID,
        *,
        fault_injector: TransportSpoolFaultInjector | None = None,
    ) -> Iterator[AttemptTransportSpool]:
        """Prepare one attempt and clean it after adoption or an ordinary failure.

        ``BaseException`` deliberately leaves the transient directory in place,
        modelling abrupt process termination.  The next attempt scope recovers it
        before any provider dispatch.
        """

        self.recover_transient_attempts()
        self._prepare_attempt(attempt_id)
        _invoke(fault_injector, TransportSpoolFaultPoint.ATTEMPT_READY)
        scope = AttemptTransportSpool(attempt_id, self, fault_injector)
        try:
            yield scope
        except Exception:
            self.cleanup_attempt(attempt_id)
            raise
        except BaseException:
            raise
        else:
            self.cleanup_attempt(attempt_id)


__all__ = [
    "AttemptTransportSpool",
    "TransportSpoolError",
    "TransportSpoolFaultInjector",
    "TransportSpoolFaultPoint",
    "TransportSpoolInspection",
    "TransportSpoolInspectionState",
    "TransportSpoolIntegrityError",
    "TransportSpoolPayload",
    "TransportSpoolStore",
    "TransportSpoolTooLargeError",
]
