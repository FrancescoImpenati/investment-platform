"""Tests for the validated external private-data root boundary."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from investment_platform.data_root import (
    ALPACA_EVIDENCE_RELATIVE_PATH,
    MANAGED_NAMESPACES,
    SENTINEL_NAME,
    SENTINEL_PURPOSE,
    PrivateDataRoot,
    PrivateDataRootSentinelError,
    PrivateRootSentinel,
    UnsafePrivateDataRootError,
    validate_private_root_location,
)

pytestmark = pytest.mark.unit

_CREATED_AT = datetime(2026, 8, 31, 10, 15, tzinfo=UTC)


@pytest.fixture
def repository_root() -> Path:
    return Path(__file__).parents[2].resolve(strict=True)


@pytest.fixture
def initialized_private_root(
    tmp_path: Path,
    repository_root: Path,
) -> tuple[PrivateDataRoot, PrivateRootSentinel]:
    private_root = PrivateDataRoot(
        tmp_path / "dedicated-investment-platform-private",
        repository_root,
        allow_temporary_for_tests=True,
    )
    sentinel = private_root.initialize(created_at=_CREATED_AT)
    return private_root, sentinel


def _sentinel_document(private_root: PrivateDataRoot) -> dict[str, object]:
    value = json.loads(private_root.sentinel_path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _replace_sentinel_fields(private_root: PrivateDataRoot, **updates: object) -> None:
    value = _sentinel_document(private_root)
    value.update(updates)
    private_root.sentinel_path.write_text(
        json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_initialize_is_idempotent_and_persists_the_exact_sentinel_contract(
    tmp_path: Path,
    repository_root: Path,
) -> None:
    configured = tmp_path / "dedicated-investment-platform-private"
    private_root = PrivateDataRoot(
        configured,
        repository_root,
        allow_temporary_for_tests=True,
    )

    first = private_root.initialize(created_at=_CREATED_AT)
    first_bytes = private_root.sentinel_path.read_bytes()
    first_stat = private_root.sentinel_path.stat()
    second = private_root.initialize(created_at=datetime(2030, 1, 1, tzinfo=UTC))
    document = _sentinel_document(private_root)

    assert first == second
    assert second.root_id == first.root_id
    assert private_root.sentinel_path.read_bytes() == first_bytes
    second_stat = private_root.sentinel_path.stat()
    assert (second_stat.st_dev, second_stat.st_ino, second_stat.st_mtime_ns) == (
        first_stat.st_dev,
        first_stat.st_ino,
        first_stat.st_mtime_ns,
    )
    assert first.schema_version == 1
    assert first.purpose == SENTINEL_PURPOSE
    assert first.created_at == _CREATED_AT
    assert isinstance(first.root_id, UUID)
    assert first.canonical_path == str(configured.resolve(strict=True))
    assert private_root.sentinel_path == private_root.root / SENTINEL_NAME
    assert document == first.model_dump(mode="json")
    assert UUID(str(document["root_id"])) == first.root_id


def test_initialize_creates_the_complete_approved_layout_and_empty_evidence_locator(
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, _ = initialized_private_root

    assert private_root.alpaca_evidence_directory == (
        private_root.root / ALPACA_EVIDENCE_RELATIVE_PATH
    )
    assert private_root.alpaca_evidence_directory.is_dir()
    assert not tuple(private_root.alpaca_evidence_directory.iterdir())
    assert sorted(path.name for path in private_root.root.iterdir()) == sorted(
        (SENTINEL_NAME, *MANAGED_NAMESPACES)
    )
    assert all((private_root.root / namespace).is_dir() for namespace in MANAGED_NAMESPACES)


def test_relative_private_root_is_rejected(repository_root: Path) -> None:
    with pytest.raises(UnsafePrivateDataRootError, match="absolute"):
        validate_private_root_location(Path("relative-private-root"), repository_root)


def test_filesystem_root_and_home_are_rejected(repository_root: Path) -> None:
    filesystem_root = Path(repository_root.anchor)

    with pytest.raises(UnsafePrivateDataRootError, match="filesystem root"):
        validate_private_root_location(filesystem_root, repository_root)
    with pytest.raises(UnsafePrivateDataRootError, match="home/profile"):
        validate_private_root_location(Path.home(), repository_root)


@pytest.mark.parametrize("relation", ["equal", "ancestor", "descendant"])
def test_repository_equal_ancestor_and_descendant_are_rejected(
    relation: str,
    repository_root: Path,
) -> None:
    candidates = {
        "equal": repository_root,
        "ancestor": repository_root.parent,
        "descendant": repository_root / "dedicated-private-runtime",
    }

    with pytest.raises(UnsafePrivateDataRootError, match="Git repository"):
        validate_private_root_location(candidates[relation], repository_root)


def test_unc_root_is_rejected_before_network_resolution(repository_root: Path) -> None:
    with pytest.raises(UnsafePrivateDataRootError, match="UNC and network"):
        validate_private_root_location(
            Path(r"\\synthetic-server\synthetic-share\dedicated-private-runtime"),
            repository_root,
        )


def test_general_temporary_root_is_rejected(repository_root: Path) -> None:
    candidate = Path(tempfile.gettempdir()) / "dedicated-investment-platform-private"

    with pytest.raises(UnsafePrivateDataRootError, match="temporary"):
        validate_private_root_location(candidate, repository_root)


@pytest.mark.parametrize("leaf_name", ["data", "files", "logs", "storage", "temp", "tmp"])
def test_generic_leaf_names_are_rejected(leaf_name: str, repository_root: Path) -> None:
    candidate = repository_root.parent / leaf_name

    with pytest.raises(UnsafePrivateDataRootError, match="too generic"):
        validate_private_root_location(candidate, repository_root)


def test_nonempty_unowned_directory_is_not_adopted(
    tmp_path: Path,
    repository_root: Path,
) -> None:
    candidate = tmp_path / "dedicated-unowned-private-runtime"
    candidate.mkdir()
    (candidate / "unrelated.txt").write_text("not platform-owned", encoding="utf-8")
    private_root = PrivateDataRoot(
        candidate,
        repository_root,
        allow_temporary_for_tests=True,
    )

    with pytest.raises(PrivateDataRootSentinelError, match="nonempty directory"):
        private_root.initialize()

    assert not private_root.sentinel_path.exists()


def test_corrupt_sentinel_fails_closed(
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, _ = initialized_private_root
    private_root.sentinel_path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(PrivateDataRootSentinelError, match="missing, corrupt, or incompatible"):
        private_root.validate()


def test_sentinel_symlink_to_external_file_is_rejected_when_supported(
    tmp_path: Path,
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, _ = initialized_private_root
    external_sentinel = tmp_path / "external-sentinel.json"
    external_sentinel.write_bytes(private_root.sentinel_path.read_bytes())
    private_root.sentinel_path.unlink()
    try:
        private_root.sentinel_path.symlink_to(external_sentinel)
    except OSError as error:
        pytest.skip(f"file symlinks are unavailable: {error}")

    with pytest.raises(PrivateDataRootSentinelError, match="regular non-link file"):
        private_root.validate()


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("schema_version", 2),
        ("purpose", "synthetic_wrong_purpose"),
    ],
)
def test_wrong_schema_or_purpose_fails_closed(
    field: str,
    replacement: object,
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, _ = initialized_private_root
    _replace_sentinel_fields(private_root, **{field: replacement})

    with pytest.raises(PrivateDataRootSentinelError, match="missing, corrupt, or incompatible"):
        private_root.validate()


def test_moved_sentinel_cannot_authorize_another_root(
    tmp_path: Path,
    repository_root: Path,
) -> None:
    original = PrivateDataRoot(
        tmp_path / "dedicated-original-private-runtime",
        repository_root,
        allow_temporary_for_tests=True,
    )
    original.initialize(created_at=_CREATED_AT)
    moved_path = tmp_path / "dedicated-moved-private-runtime"
    moved_path.mkdir()
    (moved_path / SENTINEL_NAME).write_bytes(original.sentinel_path.read_bytes())
    moved = PrivateDataRoot(
        moved_path,
        repository_root,
        allow_temporary_for_tests=True,
    )

    with pytest.raises(PrivateDataRootSentinelError, match="canonical path"):
        moved.validate()


def test_relative_canonical_path_in_sentinel_is_rejected(
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, _ = initialized_private_root
    _replace_sentinel_fields(private_root, canonical_path="relative/private-runtime")

    with pytest.raises(PrivateDataRootSentinelError, match="missing, corrupt, or incompatible"):
        private_root.validate()


def test_expected_root_id_detects_sentinel_replacement(
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, sentinel = initialized_private_root
    replacement_id = uuid4()
    assert replacement_id != sentinel.root_id
    _replace_sentinel_fields(private_root, root_id=str(replacement_id))

    with pytest.raises(PrivateDataRootSentinelError, match="ID changed"):
        private_root.validate(expected_root_id=sentinel.root_id)


@pytest.mark.parametrize("relative_path", ["", ".", "..", "../escape", "raw/../escape"])
def test_managed_path_rejects_empty_and_traversal_segments(
    relative_path: str,
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, sentinel = initialized_private_root

    with pytest.raises(UnsafePrivateDataRootError, match="safe relative paths"):
        private_root.managed_path(relative_path, expected_root_id=sentinel.root_id)


def test_managed_path_rejects_absolute_target(
    tmp_path: Path,
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, sentinel = initialized_private_root

    with pytest.raises(UnsafePrivateDataRootError, match="safe relative paths"):
        private_root.managed_path(tmp_path / "escape", expected_root_id=sentinel.root_id)


def test_managed_path_rejects_unknown_top_level_namespace(
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, sentinel = initialized_private_root

    with pytest.raises(UnsafePrivateDataRootError, match="approved private-runtime namespaces"):
        private_root.managed_path(
            "unmanaged/provider-artifact",
            expected_root_id=sentinel.root_id,
        )

    assert not (private_root.root / "unmanaged").exists()


def test_managed_directory_creation_is_idempotent_and_root_bound(
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, sentinel = initialized_private_root

    first = private_root.ensure_directory(
        "raw/provider=alpaca",
        expected_root_id=sentinel.root_id,
    )
    second = private_root.ensure_directory(
        Path("raw") / "provider=alpaca",
        expected_root_id=sentinel.root_id,
    )

    assert first == second == private_root.root / "raw" / "provider=alpaca"
    assert first.is_dir()
    assert first.is_relative_to(private_root.root)


def test_sentinel_replacement_during_managed_directory_creation_is_detected(
    monkeypatch: pytest.MonkeyPatch,
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, sentinel = initialized_private_root
    target = private_root.root / "staging" / "batch"
    original_mkdir = Path.mkdir

    def replacing_mkdir(
        path: Path,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        original_mkdir(path, mode=mode, parents=parents, exist_ok=exist_ok)
        if path == target:
            _replace_sentinel_fields(private_root, root_id=str(uuid4()))

    monkeypatch.setattr(Path, "mkdir", replacing_mkdir)

    with pytest.raises(PrivateDataRootSentinelError, match="ID changed"):
        private_root.ensure_directory(
            "staging/batch",
            expected_root_id=sentinel.root_id,
        )


def test_managed_path_rejects_symlink_escape_when_supported(
    tmp_path: Path,
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    private_root, sentinel = initialized_private_root
    outside = tmp_path / "outside-symlink-target"
    outside.mkdir()
    link = private_root.root / "raw-link"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"directory symlinks are unavailable: {error}")

    with pytest.raises(UnsafePrivateDataRootError, match="symlink, junction, or reparse"):
        private_root.managed_path("raw-link/child", expected_root_id=sentinel.root_id)


@pytest.mark.skipif(os.name != "nt", reason="Windows junction semantics")
def test_managed_path_rejects_junction_escape_when_supported(
    tmp_path: Path,
    initialized_private_root: tuple[PrivateDataRoot, PrivateRootSentinel],
) -> None:
    if not hasattr(Path, "is_junction"):
        pytest.skip("pathlib junction inspection is unavailable")
    private_root, sentinel = initialized_private_root
    outside = tmp_path / "outside-junction-target"
    outside.mkdir()
    junction = private_root.root / "raw-junction"
    command_processor = os.environ.get("COMSPEC", "cmd.exe")
    result = subprocess.run(
        [command_processor, "/d", "/c", "mklink", "/J", str(junction), str(outside)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip("directory junction creation is unavailable")
    assert junction.is_junction()

    with pytest.raises(UnsafePrivateDataRootError, match="symlink, junction, or reparse"):
        private_root.managed_path("raw-junction/child", expected_root_id=sentinel.root_id)
