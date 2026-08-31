"""Shared crash-safe filesystem primitives for Phase 2 publication."""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import re
import shutil
import stat
import sys
from collections.abc import Callable, Iterator
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Final
from uuid import UUID

from investment_platform.data_root import PrivateDataRoot, UnsafePrivateDataRootError

_SAFE_PARTITION_VALUE: Final = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_SAFE_PATH_PART: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._=-]{0,191}$")
_DEFAULT_CHUNK_SIZE: Final = 1024 * 1024


class PublicationError(RuntimeError):
    """Base error for immutable Phase 2 filesystem publication."""


class PublicationCollisionError(PublicationError):
    """An immutable identity already exists with different or corrupt bytes."""


class PublicationIntegrityError(PublicationError):
    """A staged or published artifact failed structural or checksum verification."""


class PublicationFaultPoint(StrEnum):
    """Stable fault-injection boundaries used by offline crash-recovery tests."""

    RAW_WRITE = "raw_write"
    STAGING = "staging"
    MANIFEST = "manifest"
    STAGED_MANIFEST_VERIFIED = "staged_manifest_verified"
    RENAME = "rename"
    REOPEN = "reopen"


type FaultInjector = Callable[[PublicationFaultPoint], None]


def invoke_fault(injector: FaultInjector | None, point: PublicationFaultPoint) -> None:
    if injector is not None:
        injector(point)


def json_bytes(value: object) -> bytes:
    rendered = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"{rendered}\n".encode()


def file_integrity(path: Path, *, chunk_size: int = _DEFAULT_CHUNK_SIZE) -> tuple[str, int]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    digest = hashlib.sha256()
    byte_count = 0
    try:
        with path.open("rb") as reader:
            while chunk := reader.read(chunk_size):
                digest.update(chunk)
                byte_count += len(chunk)
    except OSError as error:
        raise PublicationIntegrityError("published file is missing or unreadable") from error
    return digest.hexdigest(), byte_count


def write_file_durably(path: Path, payload: bytes) -> None:
    try:
        with path.open("xb") as writer:
            writer.write(payload)
            writer.flush()
            os.fsync(writer.fileno())
    except OSError as error:
        raise PublicationError("failed to write an immutable publication file") from error


def fsync_file(path: Path) -> None:
    try:
        # Windows rejects fsync on some read-only descriptors. These files are
        # newly created and platform-owned, so a read/write descriptor is safe.
        with path.open("r+b") as file:
            file.flush()
            os.fsync(file.fileno())
    except OSError as error:
        raise PublicationError("failed to flush a publication file") from error


def fsync_directory(path: Path) -> None:
    """Flush directory entries where the host exposes a supported directory descriptor."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        if os.name == "nt":
            return
        raise
    try:
        os.fsync(descriptor)
    except OSError:
        if os.name != "nt":
            raise
    finally:
        os.close(descriptor)


def safe_partition_value(value: str, *, label: str) -> str:
    """Keep exact provider/dataset keys in paths without lossy slug collisions."""

    if not _SAFE_PARTITION_VALUE.fullmatch(value):
        raise PublicationError(f"{label} is not safe for an exact partition path")
    return value


def safe_relative_file(value: str, *, suffix: str | None = None) -> PurePosixPath:
    if "\\" in value:
        raise PublicationError("publication paths must use canonical forward slashes")
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise PublicationError("publication file path must be a safe relative path")
    if any(not _SAFE_PATH_PART.fullmatch(part) for part in path.parts):
        raise PublicationError("publication file path contains an unsafe segment")
    if suffix is not None and path.suffix.casefold() != suffix.casefold():
        raise PublicationError(f"publication file must use the {suffix} suffix")
    return path


def managed_path(
    data_root: PrivateDataRoot,
    root_id: UUID,
    relative: PurePosixPath | Path | str,
) -> Path:
    if isinstance(relative, PurePosixPath):
        relative = Path(*relative.parts)
    return data_root.managed_path(relative, expected_root_id=root_id)


def ensure_directory(
    data_root: PrivateDataRoot,
    root_id: UUID,
    relative: PurePosixPath | Path | str,
) -> Path:
    if isinstance(relative, PurePosixPath):
        relative = Path(*relative.parts)
    return data_root.ensure_directory(relative, expected_root_id=root_id)


def assert_direct_owned_directory(path: Path, *, parent: Path) -> None:
    """Reject links/reparse points and targets outside one exact parent."""

    if path.parent != parent:
        raise UnsafePrivateDataRootError("publication directory is outside its exact parent")
    try:
        details = path.lstat()
    except OSError as error:
        raise PublicationIntegrityError("publication directory is missing") from error
    attributes = getattr(details, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    if (
        path.is_symlink()
        or (getattr(path, "is_junction", lambda: False)())
        or (reparse_flag and attributes & reparse_flag)
        or not stat.S_ISDIR(details.st_mode)
    ):
        raise PublicationIntegrityError("publication target must be a direct directory")


def ensure_direct_subdirectory(base: Path, relative: PurePosixPath) -> Path:
    """Create a platform-owned descendant without traversing links or reparse points."""

    assert_direct_owned_directory(base, parent=base.parent)
    current = base
    for part in relative.parts:
        if not _SAFE_PATH_PART.fullmatch(part):
            raise PublicationError("publication directory contains an unsafe segment")
        candidate = current / part
        try:
            candidate.mkdir(exist_ok=True)
        except OSError as error:
            raise PublicationError("failed to create a publication subdirectory") from error
        assert_direct_owned_directory(candidate, parent=current)
        current = candidate
    return current


def assert_owned_staging_candidate(
    data_root: PrivateDataRoot,
    root_id: UUID,
    candidate: Path,
    *,
    staging_parent: Path,
) -> None:
    """Revalidate the root, staging chain, and direct candidate after fault boundaries."""

    data_root.validate(expected_root_id=root_id)
    try:
        parent_relative = staging_parent.relative_to(data_root.root)
        candidate_relative = candidate.relative_to(data_root.root)
    except ValueError as error:
        raise PublicationIntegrityError("staging candidate escaped the private root") from error
    try:
        checked_parent = managed_path(data_root, root_id, parent_relative)
        checked_candidate = managed_path(data_root, root_id, candidate_relative)
    except UnsafePrivateDataRootError as error:
        raise PublicationIntegrityError(
            "staging path traverses a link, junction, or reparse point"
        ) from error
    if checked_parent != staging_parent or checked_candidate != candidate:
        raise PublicationIntegrityError("staging candidate path changed during publication")
    assert_direct_owned_directory(staging_parent, parent=staging_parent.parent)
    assert_direct_owned_directory(candidate, parent=staging_parent)


def _is_reparse_or_link(path: Path, details: os.stat_result | None = None) -> bool:
    try:
        details = details or path.lstat()
    except OSError as error:
        raise PublicationIntegrityError("publication path cannot be inspected safely") from error
    attributes = getattr(details, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(
        path.is_symlink()
        or getattr(path, "is_junction", lambda: False)()
        or (reparse_flag and attributes & reparse_flag)
    )


def iter_safe_regular_files(directory: Path) -> Iterator[Path]:
    """Walk a publication without following links or accepting special files."""

    try:
        root_details = directory.lstat()
    except OSError as error:
        raise PublicationIntegrityError("publication directory is missing") from error
    if _is_reparse_or_link(directory, root_details) or not stat.S_ISDIR(root_details.st_mode):
        raise PublicationIntegrityError("publication root must be a direct directory")

    def walk(current: Path) -> Iterator[Path]:
        try:
            entries = tuple(os.scandir(current))
        except OSError as error:
            raise PublicationIntegrityError(
                "publication tree cannot be enumerated safely"
            ) from error
        for entry in sorted(entries, key=lambda value: value.name):
            path = Path(entry.path)
            try:
                details = path.lstat()
            except OSError as error:
                raise PublicationIntegrityError(
                    "publication entry cannot be inspected safely"
                ) from error
            if _is_reparse_or_link(path, details):
                raise PublicationIntegrityError("publication tree contains a link or reparse point")
            if stat.S_ISDIR(details.st_mode):
                yield from walk(path)
            elif stat.S_ISREG(details.st_mode):
                yield path
            else:
                raise PublicationIntegrityError("publication tree contains a special file")

    yield from walk(directory)


def assert_same_volume(source: Path, destination_parent: Path) -> None:
    try:
        source_device = source.stat().st_dev
        destination_device = destination_parent.stat().st_dev
    except OSError as error:
        raise PublicationError("cannot verify same-volume atomic publication") from error
    if source_device != destination_device:
        raise PublicationError("staging and publication target are not on the same volume")


def atomic_rename_directory(source: Path, destination: Path) -> None:
    """Rename a complete directory without ever selecting overwrite semantics."""

    assert_direct_owned_directory(source, parent=source.parent)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    assert_same_volume(source, destination.parent)
    if os.name == "nt":
        # MoveFileEx without MOVEFILE_REPLACE_EXISTING is the behavior exposed
        # by os.rename on Windows.
        os.rename(source, destination)
        return
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        if renameat2 is None:
            raise PublicationError("host lacks atomic no-replace directory rename")
        renameat2.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        renameat2.restype = ctypes.c_int
        result = renameat2(
            -100,  # AT_FDCWD
            os.fsencode(source),
            -100,
            os.fsencode(destination),
            1,  # RENAME_NOREPLACE
        )
        if result == 0:
            return
        error_number = ctypes.get_errno()
        if error_number in {17, 39}:  # EEXIST / ENOTEMPTY
            raise FileExistsError(destination)
        raise OSError(error_number, os.strerror(error_number), destination)
    if sys.platform == "darwin":
        libc = ctypes.CDLL(None, use_errno=True)
        renamex_np = getattr(libc, "renamex_np", None)
        if renamex_np is None:
            raise PublicationError("host lacks atomic no-replace directory rename")
        renamex_np.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        renamex_np.restype = ctypes.c_int
        result = renamex_np(
            os.fsencode(source),
            os.fsencode(destination),
            0x00000004,  # RENAME_EXCL
        )
        if result == 0:
            return
        error_number = ctypes.get_errno()
        if error_number in {17, 39}:
            raise FileExistsError(destination)
        raise OSError(error_number, os.strerror(error_number), destination)
    # Fail closed on unverified host semantics instead of allowing POSIX
    # rename to replace an existing empty destination directory.
    raise PublicationError("atomic no-replace directory rename is unsupported on this host")


def remove_owned_staging_directory(
    data_root: PrivateDataRoot,
    root_id: UUID,
    path: Path,
    *,
    staging_parent: Path,
) -> None:
    """Remove only an exact candidate directory created below a validated staging parent."""

    data_root.validate(expected_root_id=root_id)
    checked_parent = managed_path(
        data_root,
        root_id,
        staging_parent.relative_to(data_root.root),
    )
    checked = managed_path(data_root, root_id, path.relative_to(data_root.root))
    if checked_parent != staging_parent or checked.parent != staging_parent:
        raise UnsafePrivateDataRootError("refusing to remove a non-candidate staging path")
    if checked.exists() or checked.is_symlink():
        assert_direct_owned_directory(checked, parent=staging_parent)
        # Refuse destructive recovery if any descendant was redirected.
        tuple(iter_safe_regular_files(checked))
        shutil.rmtree(checked)
        fsync_directory(staging_parent)
    data_root.validate(expected_root_id=root_id)


__all__ = [
    "FaultInjector",
    "PublicationCollisionError",
    "PublicationError",
    "PublicationFaultPoint",
    "PublicationIntegrityError",
]
