"""Validated external private-data root and sentinel contract."""

from __future__ import annotations

import json
import os
import stat
import tempfile
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Final, Literal, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

SENTINEL_NAME: Final = ".investment-platform-root.json"
SENTINEL_PURPOSE: Final = "investment_platform_private_research"
ALPACA_EVIDENCE_RELATIVE_PATH: Final = Path("governance/evidence/alpaca/ticket-342496")
MANAGED_NAMESPACES: Final = frozenset(
    {
        "curated",
        "features",
        "governance",
        "logs",
        "normalized",
        "operational",
        "quarantine",
        "raw",
        "staging",
    }
)
_GENERIC_LEAF_NAMES: Final = frozenset({"data", "files", "logs", "storage", "temp", "tmp"})


class PrivateDataRootError(RuntimeError):
    """Base error for unsafe, uninitialized, or inconsistent private roots."""


class UnsafePrivateDataRootError(PrivateDataRootError):
    """Raised when a path cannot safely act as the platform-owned root."""


class PrivateDataRootSentinelError(PrivateDataRootError):
    """Raised when the direct-child ownership sentinel is absent or invalid."""


class PrivateRootSentinel(BaseModel):
    """Immutable ownership record stored directly below a private root."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    schema_version: Literal[1] = 1
    purpose: Literal["investment_platform_private_research"] = SENTINEL_PURPOSE
    root_id: UUID
    canonical_path: Annotated[str, Field(min_length=1)]
    created_at: datetime

    @field_validator("created_at", mode="after")
    @classmethod
    def require_aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("canonical_path", mode="after")
    @classmethod
    def require_absolute_canonical_path(cls, value: str) -> str:
        if value.startswith(("\\\\", "//")) or not Path(value).is_absolute():
            raise ValueError("canonical_path must be an absolute local path")
        return value


def _normalized_path_text(path: Path) -> str:
    return os.path.normcase(os.path.abspath(str(path)))


def _is_same_or_below(candidate: Path, parent: Path) -> bool:
    candidate_text = _normalized_path_text(candidate)
    parent_text = _normalized_path_text(parent)
    try:
        return os.path.commonpath((candidate_text, parent_text)) == parent_text
    except ValueError:
        return False


def _is_reparse_or_link(path: Path) -> bool:
    try:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        if is_junction is not None and is_junction():
            return True
        details = path.lstat()
    except OSError as error:
        raise UnsafePrivateDataRootError(f"cannot inspect path component safely: {path}") from error
    attributes = getattr(details, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(reparse_flag and attributes & reparse_flag)


def _existing_components(path: Path) -> Iterable[Path]:
    components: list[Path] = []
    current = path
    while True:
        if current.exists() or current.is_symlink():
            components.append(current)
        if current.parent == current:
            break
        current = current.parent
    return reversed(components)


def _critical_roots() -> tuple[Path, ...]:
    values: list[Path] = [Path(tempfile.gettempdir())]
    for variable in (
        "SystemRoot",
        "WINDIR",
        "ProgramFiles",
        "ProgramFiles(x86)",
        "ProgramData",
    ):
        value = os.environ.get(variable)
        if value:
            values.append(Path(value))
    if os.name != "nt":
        values.extend(
            Path(value)
            for value in (
                "/bin",
                "/boot",
                "/dev",
                "/etc",
                "/lib",
                "/proc",
                "/root",
                "/run",
                "/sbin",
                "/sys",
                "/usr",
                "/var",
            )
        )
    return tuple(value.resolve(strict=False) for value in values)


def _reject_windows_remote_drive(path: Path) -> None:
    if os.name != "nt":
        return
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_drive_type = kernel32.GetDriveTypeW
    get_drive_type.argtypes = [ctypes.c_wchar_p]
    get_drive_type.restype = ctypes.c_uint
    drive_remote = 4
    if get_drive_type(path.anchor) == drive_remote:
        raise UnsafePrivateDataRootError(
            "mapped network drives are not supported for the Phase 2 private root"
        )
    query_dos_device = kernel32.QueryDosDeviceW
    query_dos_device.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint]
    query_dos_device.restype = ctypes.c_uint
    target_buffer = ctypes.create_unicode_buffer(32768)
    if query_dos_device(path.drive, target_buffer, len(target_buffer)) == 0:
        raise UnsafePrivateDataRootError("cannot verify the physical Windows volume mapping")
    if target_buffer.value.startswith("\\??\\"):
        raise UnsafePrivateDataRootError(
            "SUBST and path-backed drive aliases are not supported for the private root"
        )


def _unescape_mountinfo_path(value: str) -> str:
    return (
        value.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def _reject_posix_network_filesystem(
    path: Path,
    *,
    allow_temporary_for_tests: bool,
) -> None:
    if os.name == "nt":
        return
    mountinfo = Path("/proc/self/mountinfo")
    if not mountinfo.is_file():
        if allow_temporary_for_tests and _is_same_or_below(
            path,
            Path(tempfile.gettempdir()).resolve(strict=False),
        ):
            return
        raise UnsafePrivateDataRootError(
            "cannot verify a supported local filesystem for the private root"
        )
    try:
        lines = mountinfo.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise UnsafePrivateDataRootError("cannot verify local filesystem mount metadata") from error
    supported_local_types = {
        "apfs",
        "bcachefs",
        "btrfs",
        "exfat",
        "ext2",
        "ext3",
        "ext4",
        "f2fs",
        "hfsplus",
        "msdos",
        "ntfs",
        "ntfs3",
        "ufs",
        "vfat",
        "xfs",
        "zfs",
    }
    candidates: list[tuple[int, str, Path]] = []
    for line in lines:
        left, separator, right = line.partition(" - ")
        if not separator:
            continue
        left_fields = left.split()
        right_fields = right.split()
        if len(left_fields) < 5 or not right_fields:
            continue
        mount_path = Path(_unescape_mountinfo_path(left_fields[4])).resolve(strict=False)
        if _is_same_or_below(path, mount_path):
            candidates.append((len(mount_path.parts), right_fields[0].casefold(), mount_path))
    if not candidates:
        raise UnsafePrivateDataRootError("cannot identify the private root filesystem mount")
    _, filesystem_type, mount_path = max(candidates, key=lambda value: value[0])
    injected_test_types = {"overlay", "tmpfs"}
    if filesystem_type not in supported_local_types and not (
        allow_temporary_for_tests
        and filesystem_type in injected_test_types
        and _is_same_or_below(path, Path(tempfile.gettempdir()).resolve(strict=False))
    ):
        raise UnsafePrivateDataRootError(
            "private data root filesystem is not in the supported local allowlist: "
            f"{filesystem_type} at {mount_path}"
        )


def validate_private_root_location(
    root: Path,
    repository_root: Path,
    *,
    allow_temporary_for_tests: bool = False,
) -> Path:
    """Return a canonical safe root location without creating it.

    The test-only temporary exception exists because normal tests must use injected disposable
    roots. Production call sites never enable it.
    """

    root = Path(root)
    repository_root = Path(repository_root)
    rendered = str(root)
    if rendered.startswith(("\\\\", "//")):
        raise UnsafePrivateDataRootError("UNC and network roots are not supported in Phase 2")
    if not root.is_absolute():
        raise UnsafePrivateDataRootError("private data root must be an absolute path")
    if not repository_root.is_absolute():
        raise ValueError("repository_root must be absolute")

    for component in _existing_components(root):
        if _is_reparse_or_link(component):
            raise UnsafePrivateDataRootError(
                "private data root must not traverse a symlink, junction, or reparse point: "
                f"{component}"
            )

    canonical = root.resolve(strict=False)
    repository = repository_root.resolve(strict=True)
    _reject_windows_remote_drive(canonical)
    _reject_posix_network_filesystem(
        canonical,
        allow_temporary_for_tests=allow_temporary_for_tests,
    )
    anchor = Path(canonical.anchor)
    if canonical == anchor:
        raise UnsafePrivateDataRootError("private data root must not be a drive/filesystem root")
    if canonical.exists() and os.path.ismount(canonical):
        raise UnsafePrivateDataRootError(
            "private data root must be a dedicated directory, not a mount point"
        )

    home = Path.home().resolve(strict=False)
    if canonical == home:
        raise UnsafePrivateDataRootError("private data root must not be the home/profile directory")
    if _is_same_or_below(canonical, repository) or _is_same_or_below(repository, canonical):
        raise UnsafePrivateDataRootError(
            "private data root must be physically separate from the Git repository"
        )
    if canonical.name.casefold() in _GENERIC_LEAF_NAMES:
        raise UnsafePrivateDataRootError(
            "private data root leaf name is too generic for destructive-operation safety"
        )

    temporary_root = Path(tempfile.gettempdir()).resolve(strict=False)
    using_injected_test_temp = allow_temporary_for_tests and _is_same_or_below(
        canonical,
        temporary_root,
    )
    for critical in _critical_roots():
        if _is_same_or_below(canonical, critical):
            if using_injected_test_temp:
                continue
            raise UnsafePrivateDataRootError(
                "private data root must not be inside a system or general temporary directory: "
                f"{critical}"
            )
    return canonical


class PrivateDataRoot:
    """Platform-owned external root; every mutation revalidates location and sentinel."""

    def __init__(
        self,
        root: Path,
        repository_root: Path,
        *,
        allow_temporary_for_tests: bool = False,
    ) -> None:
        self._repository_root = Path(repository_root).resolve(strict=True)
        self._allow_temporary_for_tests = allow_temporary_for_tests
        self._root = validate_private_root_location(
            Path(root),
            self._repository_root,
            allow_temporary_for_tests=allow_temporary_for_tests,
        )

    @property
    def root(self) -> Path:
        return self._root

    @property
    def sentinel_path(self) -> Path:
        return self._root / SENTINEL_NAME

    @property
    def alpaca_evidence_directory(self) -> Path:
        return self._root / ALPACA_EVIDENCE_RELATIVE_PATH

    def _revalidate_location(self) -> None:
        current = validate_private_root_location(
            self._root,
            self._repository_root,
            allow_temporary_for_tests=self._allow_temporary_for_tests,
        )
        if _normalized_path_text(current) != _normalized_path_text(self._root):
            raise UnsafePrivateDataRootError("private data root resolved location changed")

    def read_sentinel(self) -> PrivateRootSentinel:
        self._revalidate_location()
        if not self.sentinel_path.exists() and not self.sentinel_path.is_symlink():
            raise PrivateDataRootSentinelError("private data root sentinel is missing")
        try:
            before = self.sentinel_path.lstat()
        except OSError as error:
            raise PrivateDataRootSentinelError(
                "private data root sentinel cannot be inspected"
            ) from error
        if (
            _is_reparse_or_link(self.sentinel_path)
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
        ):
            raise PrivateDataRootSentinelError(
                "private data root sentinel must be a direct regular non-link file"
            )
        try:
            raw = self.sentinel_path.read_bytes()
            after = self.sentinel_path.lstat()
            value = json.loads(raw)
            sentinel = PrivateRootSentinel.model_validate(value)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValidationError) as error:
            raise PrivateDataRootSentinelError(
                "private data root sentinel is missing, corrupt, or incompatible"
            ) from error
        before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if before_identity != after_identity or _is_reparse_or_link(self.sentinel_path):
            raise PrivateDataRootSentinelError("private data root sentinel changed while read")
        if _normalized_path_text(Path(sentinel.canonical_path)) != _normalized_path_text(
            self._root
        ):
            raise PrivateDataRootSentinelError(
                "private data root sentinel canonical path does not match this root"
            )
        return sentinel

    def initialize(self, *, created_at: datetime | None = None) -> PrivateRootSentinel:
        """Intentionally initialize a nonexistent or empty dedicated directory, idempotently."""

        self._revalidate_location()
        if self._root.exists() and not self._root.is_dir():
            raise UnsafePrivateDataRootError("private data root exists but is not a directory")
        if not self._root.exists():
            try:
                self._root.mkdir(parents=False, exist_ok=False)
            except OSError as error:
                raise PrivateDataRootError(
                    "could not create the dedicated private root; its parent must already exist"
                ) from error

        if self.sentinel_path.exists():
            sentinel = self.read_sentinel()
            self._ensure_approved_layout(expected_root_id=sentinel.root_id)
            return sentinel
        try:
            entries = tuple(self._root.iterdir())
        except OSError as error:
            raise PrivateDataRootError("cannot inspect the private data root") from error
        if entries:
            raise PrivateDataRootSentinelError(
                "a nonempty directory without the exact sentinel cannot be adopted"
            )

        timestamp = created_at or datetime.now(UTC)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        sentinel = PrivateRootSentinel(
            root_id=uuid4(),
            canonical_path=str(self._root),
            created_at=timestamp,
        )
        payload = (
            json.dumps(
                sentinel.model_dump(mode="json"),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode()
        temporary = self._root / f".{SENTINEL_NAME}.{uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as writer:
                writer.write(payload)
                writer.flush()
                os.fsync(writer.fileno())
            os.link(temporary, self.sentinel_path)
            if os.name != "nt":
                directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                directory_descriptor = os.open(self._root, directory_flags)
                try:
                    os.fsync(directory_descriptor)
                finally:
                    os.close(directory_descriptor)
        except FileExistsError:
            confirmed = self.read_sentinel()
            self._ensure_approved_layout(expected_root_id=confirmed.root_id)
            return confirmed
        except OSError as error:
            raise PrivateDataRootError(
                "failed to publish the private data root sentinel"
            ) from error
        finally:
            temporary.unlink(missing_ok=True)

        confirmed = self.read_sentinel()
        self._ensure_approved_layout(expected_root_id=confirmed.root_id)
        return confirmed

    def _ensure_approved_layout(self, *, expected_root_id: UUID) -> None:
        """Materialize the complete, fixed Phase 2 namespace layout below an owned root."""

        for namespace in sorted(MANAGED_NAMESPACES):
            self.ensure_directory(namespace, expected_root_id=expected_root_id)
        self.ensure_directory(
            ALPACA_EVIDENCE_RELATIVE_PATH,
            expected_root_id=expected_root_id,
        )

    def validate(self, *, expected_root_id: UUID | None = None) -> PrivateRootSentinel:
        sentinel = self.read_sentinel()
        if expected_root_id is not None and sentinel.root_id != expected_root_id:
            raise PrivateDataRootSentinelError("private data root ID changed")
        return sentinel

    def managed_path(
        self,
        relative_path: str | Path,
        *,
        expected_root_id: UUID | None = None,
    ) -> Path:
        """Resolve one exact root-relative target after validating ownership and traversal."""

        self.validate(expected_root_id=expected_root_id)
        relative = Path(relative_path)
        if (
            relative.is_absolute()
            or relative.drive
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
        ):
            raise UnsafePrivateDataRootError("managed paths must be exact safe relative paths")
        target = self._root.joinpath(relative)
        for component in _existing_components(target):
            if _is_reparse_or_link(component):
                raise UnsafePrivateDataRootError(
                    "managed path traverses a symlink, junction, or reparse point"
                )
        if relative.parts[0] not in MANAGED_NAMESPACES:
            raise UnsafePrivateDataRootError(
                "managed path is outside the approved private-runtime namespaces"
            )
        canonical = target.resolve(strict=False)
        if canonical == self._root or not _is_same_or_below(canonical, self._root):
            raise UnsafePrivateDataRootError("managed path escapes the private data root")
        return canonical

    def ensure_directory(
        self,
        relative_path: str | Path,
        *,
        expected_root_id: UUID | None = None,
    ) -> Path:
        """Create one managed directory only after immediate sentinel/path revalidation."""

        starting_sentinel = self.validate(expected_root_id=expected_root_id)
        stable_root_id = expected_root_id or starting_sentinel.root_id
        target = self.managed_path(relative_path, expected_root_id=stable_root_id)
        try:
            target.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise PrivateDataRootError(
                f"failed to create managed directory: {relative_path}"
            ) from error
        # Detect a replacement or redirect introduced during the mutation.
        self.validate(expected_root_id=stable_root_id)
        checked = self.managed_path(relative_path, expected_root_id=stable_root_id)
        if checked != target or not checked.is_dir():
            raise UnsafePrivateDataRootError("managed directory changed during creation")
        return target

    def with_root(self, root: Path) -> Self:
        """Return a validator for the same repository and safety policy at another root."""

        return type(self)(
            root,
            self._repository_root,
            allow_temporary_for_tests=self._allow_temporary_for_tests,
        )


__all__ = [
    "ALPACA_EVIDENCE_RELATIVE_PATH",
    "MANAGED_NAMESPACES",
    "SENTINEL_NAME",
    "PrivateDataRoot",
    "PrivateDataRootError",
    "PrivateDataRootSentinelError",
    "PrivateRootSentinel",
    "UnsafePrivateDataRootError",
    "validate_private_root_location",
]
