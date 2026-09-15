#!/usr/bin/env python3
"""Promote the reviewed Sunlit candidate into its fixed inactive runtime layout."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, ContextManager, Iterator

from game_control.modpack_update import AssemblyError, activate_release


VERSION = "1.1.2-SSV4.1.4"
MANIFEST_SHA256: str | None = "640513b201296f35bdc106f07237acabf0bf2afa7597114e9c8a6ad9f44ac5c3"
STAGING_ROOT = Path("/srv/game-servers/.horizon-update-staging/sunlit-1.1.2-SSV4.1.4")
CANDIDATE = STAGING_ROOT / "candidate-v2"
MANIFEST = STAGING_ROOT / "manifest-v2.json"
RELEASE_ROOT = Path("/opt/game-servers/minecraft-sunlit-cobblemon/releases")
RELEASE = RELEASE_ROOT / VERSION
STATE_ROOT = Path("/srv/game-servers/minecraft-sunlit-cobblemon-state")
ACTIVE_LINK = Path("/srv/game-servers/minecraft-sunlit-cobblemon-current")
SLOT = Path("/run/game-slot/slot.json")
LIBRARIES = Path("/opt/game-servers/minecraft-sunlit-cobblemon/libraries")


class PromotionError(ValueError):
    pass


class PublicationRefused(PromotionError):
    """A publication fence refused or failed after the action was attempted."""


class PublicationAction(str, Enum):
    STATE = "state"
    RELEASE = "release"
    VERSION_STATE = "version_state"
    METADATA = "metadata"
    ACTIVE_LINK = "active_link"
    ROLLBACK_STATE = "rollback_state"
    ROLLBACK_RELEASE = "rollback_release"
    ROLLBACK_VERSION_STATE = "rollback_version_state"
    ROLLBACK_METADATA = "rollback_metadata"
    ROLLBACK_ACTIVE_LINK = "rollback_active_link"


PublicationGuard = Callable[[PublicationAction], ContextManager[None]]


@dataclass(frozen=True, slots=True)
class PromotionContext:
    """Immutable policy and paths for one promotion operation.

    A promotion is also a public in-process API used by the updater.  Keeping
    its complete path/version identity in a value object prevents concurrent
    calls from borrowing one another's module state.
    """

    version: str
    manifest_sha256: str | None
    staging_root: Path
    candidate: Path
    manifest: Path
    release_root: Path
    release: Path
    state_root: Path
    active_link: Path
    slot: Path
    libraries: Path
    publication_guard: PublicationGuard | None = None


@dataclass(frozen=True, slots=True)
class _PathIdentity:
    device: int
    inode: int
    file_type: int
    owner_uid: int
    owner_gid: int
    mode: int


def _validate_version(version: str) -> str:
    if not isinstance(version, str) or not version or "/" in version or version in {".", ".."}:
        raise PromotionError("promotion version is unsafe")
    return version


def _default_context() -> PromotionContext:
    """Snapshot compatibility defaults without mutating module globals."""
    version = _validate_version(VERSION)
    release_root = Path(RELEASE_ROOT)
    candidate = Path(CANDIDATE)
    return PromotionContext(
        version=version,
        manifest_sha256=MANIFEST_SHA256,
        staging_root=Path(STAGING_ROOT),
        candidate=candidate,
        manifest=Path(MANIFEST),
        release_root=release_root,
        release=release_root / version,
        state_root=Path(STATE_ROOT),
        active_link=Path(ACTIVE_LINK),
        slot=Path(SLOT),
        libraries=Path(LIBRARIES),
    )


def promote_candidate(
    *,
    version: str,
    manifest: Path,
    candidate_root: Path,
    manifest_sha256: str | None = None,
    publication_guard: PublicationGuard,
) -> dict:
    """Promote a staged candidate using the package-owned policy.

    The command-line compatibility front door supplies these same values.  The
    updater calls this typed entry point directly, so promotion does not rely
    on an installed checkout helper or a subprocess boundary.
    """
    version = _validate_version(version)
    candidate = Path(candidate_root)
    release_root = Path(RELEASE_ROOT)
    context = PromotionContext(
        version=version,
        manifest_sha256=manifest_sha256,
        staging_root=candidate.parent,
        candidate=candidate,
        manifest=Path(manifest),
        release_root=release_root,
        release=release_root / version,
        state_root=Path(STATE_ROOT),
        active_link=Path(ACTIVE_LINK),
        slot=Path(SLOT),
        libraries=Path(LIBRARIES),
        publication_guard=publication_guard,
    )
    return promote(context)


def _fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _regular_json(
    path: Path,
    *,
    maximum: int,
    owners: tuple[tuple[int, int], ...] = ((0, 0),),
) -> dict:
    info = path.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or (info.st_uid, info.st_gid) not in owners
        or info.st_mode & 0o022
        or info.st_size > maximum
    ):
        raise PromotionError("promotion metadata is unsafe")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PromotionError("promotion metadata is unreadable") from exc
    if not isinstance(value, dict):
        raise PromotionError("promotion metadata is malformed")
    return value


def _manifest(context: PromotionContext) -> dict:
    document = _regular_json(context.manifest, maximum=8 * 1024 * 1024)
    supplied = document.pop("manifest_sha256", None)
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    calculated = hashlib.sha256(encoded).hexdigest()
    document["manifest_sha256"] = supplied
    if (
        supplied != calculated
        or (context.manifest_sha256 is not None and supplied != context.manifest_sha256)
        or document.get("profile_id") != "minecraft-sunlit-cobblemon"
        or document.get("artifact", {}).get("version") != context.version
    ):
        raise PromotionError("promotion manifest identity mismatch")
    return document


def _inactive(context: PromotionContext) -> None:
    result = subprocess.run(
        ["/usr/bin/systemctl", "is-active", "minecraft-sunlit-cobblemon.service"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    if result.stdout.strip() != "inactive" or context.slot.exists() or context.slot.is_symlink():
        raise PromotionError("Sunlit is not in the required inactive state")


@contextmanager
def _publication(context: PromotionContext, action: PublicationAction) -> Iterator[None]:
    """Enter the caller's atomic ownership fence for one bounded publication.

    The source-only compatibility path has no caller authority and therefore
    fails closed.  The updater must provide a reservation-aware guard
    explicitly; an inactive observation alone is not a publication fence.
    """
    if context.publication_guard is None:
        raise PromotionError("publication guard is required")
    guarded = context.publication_guard(action)
    if not hasattr(guarded, "__enter__") or not hasattr(guarded, "__exit__"):
        raise PromotionError("publication guard is malformed")
    try:
        with guarded:
            yield
    except (OSError, ValueError) as exc:
        # Once an action reaches its guard, cleanup cannot safely assume that
        # the rename did not happen.  Preserve the durable partial state and
        # let the next owned invocation resume it.
        raise PublicationRefused(str(exc) or "publication action was refused") from exc


def _guarded_replace(
    context: PromotionContext,
    action: PublicationAction,
    source: Path,
    destination: Path,
    directory: Path,
    *,
    precondition: Callable[[], None] | None = None,
) -> None:
    with _publication(context, action):
        if precondition is not None:
            precondition()
        os.replace(source, destination)
        _fsync_dir(directory)


def _guarded_activate(
    context: PromotionContext,
    action: PublicationAction,
    release: Path,
    *,
    expected_prior: str | None,
    precondition: Callable[[], None] | None = None,
) -> None:
    with _publication(context, action):
        if precondition is not None:
            precondition()
        activate_release(
            context.active_link,
            context.release_root,
            release,
            expected_prior=expected_prior,
        )


def _guarded_unlink_active(context: PromotionContext) -> None:
    with _publication(context, PublicationAction.ROLLBACK_ACTIVE_LINK):
        context.active_link.unlink(missing_ok=True)
        _fsync_dir(context.active_link.parent)


def _trusted_directory(
    path: Path,
    *,
    owners: tuple[tuple[int, int], ...] = ((0, 0),),
) -> os.stat_result:
    info = path.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or (info.st_uid, info.st_gid) not in owners
        or info.st_mode & 0o022
    ):
        raise PromotionError("promotion directory is unsafe")
    return info


def _path_identity(path: Path) -> _PathIdentity:
    try:
        info = path.lstat()
    except OSError as exc:
        raise PromotionError("publication path identity is unavailable") from exc
    return _PathIdentity(
        device=info.st_dev,
        inode=info.st_ino,
        file_type=stat.S_IFMT(info.st_mode),
        owner_uid=info.st_uid,
        owner_gid=info.st_gid,
        mode=stat.S_IMODE(info.st_mode),
    )


def _require_path_identity(path: Path, expected: _PathIdentity) -> None:
    if _path_identity(path) != expected:
        raise PromotionError("publication path identity changed")


def _require_path_absent(path: Path) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise PromotionError("publication destination cannot be proven absent") from exc
    raise PromotionError("publication destination is not absent")


def _expected_links(document: dict, context: PromotionContext) -> dict[str, str]:
    try:
        policy = document["runtime_policy"]
        state_paths = (
            list(policy["persistent_dirs"])
            + list(policy["persistent_files"])
            + list(policy["mutable_vendor_dirs"])
            + list(policy["empty_mutable_dirs"])
        )
        fixed = dict(policy["fixed_symlinks"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PromotionError("promotion runtime policy is malformed") from exc
    expected: dict[str, str] = {}
    versioned = set(policy["mutable_vendor_dirs"]) | set(policy["empty_mutable_dirs"])
    for relative in state_paths:
        if not isinstance(relative, str) or not relative or "/" in relative or relative in expected:
            raise PromotionError("promotion runtime policy is unsafe")
        target = context.state_root / (Path(".versions") / context.version / relative if relative in versioned else relative)
        expected[relative] = str(target)
    for relative, target in fixed.items():
        if relative in expected or relative != "libraries" or target != str(context.libraries):
            raise PromotionError("promotion fixed-link policy is unsafe")
        expected[relative] = target
    return expected


def _replace_link(path: Path, target: str) -> None:
    temporary = path.parent / f".{path.name}.promote.{os.getpid()}"
    if temporary.exists() or temporary.is_symlink():
        raise PromotionError("promotion temporary link already exists")
    try:
        os.symlink(target, temporary, target_is_directory=True)
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _bind_candidate_links(runtime: Path, document: dict, context: PromotionContext) -> None:
    expected = _expected_links(document, context)
    actual: dict[str, str] = {}
    for path in runtime.rglob("*"):
        if path.is_symlink():
            relative = path.relative_to(runtime).as_posix()
            if "/" in relative:
                raise PromotionError("nested candidate symlink is not approved")
            actual[relative] = os.readlink(path)
    if set(actual) != set(expected):
        raise PromotionError("candidate symlink set is not exact")
    staging_state = str(context.candidate / "state")
    for relative, target in expected.items():
        current = actual[relative]
        if relative == "libraries":
            if current != target:
                raise PromotionError("candidate libraries link changed")
        elif current != target and not current.startswith(staging_state + "/"):
            raise PromotionError("candidate state link changed")
        if current != target:
            _replace_link(runtime / relative, target)


def _write_metadata(
    state: Path,
    document: dict,
    context: PromotionContext,
    *,
    publication_action: PublicationAction | None = None,
    owner: tuple[int, int] | None = None,
) -> None:
    metadata = state / ".horizon"
    if metadata.exists() or metadata.is_symlink():
        if metadata.is_symlink() or not metadata.is_dir():
            raise PromotionError("candidate metadata directory is unsafe")
        if {path.name for path in metadata.iterdir()} - {"manifest.json", "release.json"}:
            raise PromotionError("candidate metadata directory contains unexpected files")
    else:
        metadata.mkdir(mode=0o700)
    manifest_target = metadata / "manifest.json"
    manifest_temporary = metadata / ".manifest.json.promote"
    shutil.copyfile(context.manifest, manifest_temporary, follow_symlinks=False)
    os.chmod(manifest_temporary, 0o600)
    release = {
        "profile_id": "minecraft-sunlit-cobblemon",
        "version": context.version,
        "manifest_sha256": document["manifest_sha256"],
        "archive_sha256": document["artifact"]["archive"]["sha256"],
    }
    release_target = metadata / "release.json"
    release_temporary = metadata / ".release.json.promote"
    release_temporary.write_text(json.dumps(release, sort_keys=True, separators=(",", ":")) + "\n", encoding="ascii")
    os.chmod(release_temporary, 0o600)
    if owner is not None:
        for path in (manifest_temporary, release_temporary):
            os.chown(path, owner[0], owner[1], follow_symlinks=False)
            os.chmod(path, 0o640, follow_symlinks=False)
    for path in (manifest_temporary, release_temporary):
        with path.open("rb") as stream:
            os.fsync(stream.fileno())

    def publish() -> None:
        os.replace(manifest_temporary, manifest_target)
        os.replace(release_temporary, release_target)
        if owner is not None:
            os.chown(metadata, owner[0], owner[1], follow_symlinks=False)
            os.chmod(metadata, 0o750, follow_symlinks=False)
        _fsync_dir(metadata)

    try:
        if publication_action is None:
            publish()
        else:
            with _publication(context, publication_action):
                publish()
    finally:
        manifest_temporary.unlink(missing_ok=True)
        release_temporary.unlink(missing_ok=True)


def _sunlit_ids() -> tuple[int, int]:
    try:
        import pwd
        account = pwd.getpwnam("svc-sunlit")
    except (ImportError, KeyError) as exc:
        raise PromotionError("Sunlit account is unavailable") from exc
    return account.pw_uid, account.pw_gid


def _prepare_state_ownership(root: Path) -> None:
    uid, gid = _sunlit_ids()
    pending = [root]
    paths: list[tuple[Path, os.stat_result]] = []
    while pending:
        current = pending.pop()
        info = current.lstat()
        if stat.S_ISLNK(info.st_mode) or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            raise PromotionError("candidate state contains an unsafe member")
        paths.append((current, info))
        if stat.S_ISDIR(info.st_mode):
            pending.extend(Path(entry.path) for entry in os.scandir(current))
    for path, info in reversed(paths):
        os.chown(path, uid, gid, follow_symlinks=False)
        os.chmod(path, 0o750 if stat.S_ISDIR(info.st_mode) else 0o640, follow_symlinks=False)


def _normalize_release_permissions(root: Path) -> None:
    """Make an immutable release readable by the unprivileged game user."""
    for path in [root, *root.rglob("*")]:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            continue
        if info.st_uid != 0 or info.st_gid != 0:
            raise PromotionError("candidate release ownership is unsafe")
        if stat.S_ISDIR(info.st_mode):
            mode = 0o755
        elif stat.S_ISREG(info.st_mode):
            mode = 0o755 if info.st_mode & 0o111 else 0o644
        else:
            raise PromotionError("candidate release ownership is unsafe")
        os.chmod(path, mode, follow_symlinks=False)


def _verify_release_ownership(root: Path) -> None:
    for path in [root, *root.rglob("*")]:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            continue
        if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)) or info.st_uid != 0 or info.st_gid != 0 or info.st_mode & 0o022:
            raise PromotionError("candidate release ownership is unsafe")
        if stat.S_ISDIR(info.st_mode) and info.st_mode & 0o005 != 0o005:
            raise PromotionError("candidate release is not searchable")
        if stat.S_ISREG(info.st_mode) and not info.st_mode & 0o004:
            raise PromotionError("candidate release is not readable")


def _fsync_tree(root: Path) -> None:
    if not root.is_dir() or root.is_symlink():
        raise PromotionError("durability root is unsafe")
    os.sync()


def _tree_digest(root: Path, *, include_mode: bool = True) -> str:
    digest = hashlib.sha256()
    for path in sorted([root, *root.rglob("*")], key=lambda item: item.relative_to(root).as_posix() if item != root else ""):
        relative = "." if path == root else path.relative_to(root).as_posix()
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            kind = b"l"
            payload = os.readlink(path).encode("utf-8")
        elif stat.S_ISDIR(info.st_mode):
            kind = b"d"
            payload = b""
        elif stat.S_ISREG(info.st_mode):
            kind = b"f"
            file_digest = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    file_digest.update(chunk)
            payload = file_digest.digest()
        else:
            raise PromotionError("promotion tree contains an unsafe member")
        digest.update(kind + b"\0" + relative.encode("utf-8") + b"\0")
        if include_mode:
            digest.update(str(stat.S_IMODE(info.st_mode)).encode("ascii") + b"\0")
        digest.update(payload + b"\0")
    return digest.hexdigest()


def _report(context: PromotionContext, document: dict | None = None) -> dict:
    if document is None:
        document = _manifest(context)
    return {
        "active": True,
        "profile_id": "minecraft-sunlit-cobblemon",
        "version": context.version,
        "manifest_sha256": document["manifest_sha256"],
        "release": str(context.release),
        "state": str(context.state_root),
        "active_link": str(context.active_link),
    }


def _publish_report(context: PromotionContext, document: dict | None = None) -> dict:
    report = _report(context, document)
    record = context.candidate / "promotion.json"
    record.write_text(json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n", encoding="ascii")
    os.chmod(record, 0o600)
    with record.open("rb") as stream:
        os.fsync(stream.fileno())
    _fsync_dir(context.candidate)
    return report


def _already_promoted(context: PromotionContext, document: dict, sunlit_owner: tuple[int, int]) -> dict | None:
    if not context.active_link.is_symlink():
        return None
    try:
        active_release = (context.active_link.parent / os.readlink(context.active_link)).resolve(strict=True)
        expected_release = context.release.resolve(strict=True)
    except OSError:
        return None
    if active_release != expected_release or active_release.parent != context.release_root.resolve():
        return None
    _trusted_directory(context.release)
    _trusted_directory(context.state_root, owners=(sunlit_owner,))
    runtime = context.candidate / "runtime"
    state = context.candidate / "state"
    if runtime.exists() or runtime.is_symlink():
        _trusted_directory(runtime)
        _bind_candidate_links(runtime, document, context)
        if _tree_digest(runtime) != _tree_digest(context.release):
            raise PromotionError("active promotion retained a mismatched candidate")
        shutil.rmtree(runtime)
    if state.exists() or state.is_symlink():
        _trusted_directory(state, owners=((0, 0), sunlit_owner))
        _candidate_matches_existing_state(
            context, state, document, allow_moved_version=True,
        )
        shutil.rmtree(state)
    # Activation is the commit point.  If a crash occurred after activation
    # but before metadata publication, repair metadata only under its guard.
    stable_record = context.state_root / ".horizon/release.json"
    try:
        current = _regular_json(stable_record, maximum=64 * 1024, owners=((0, 0), sunlit_owner))
    except (OSError, PromotionError):
        current = None
    if (
        current is None
        or current.get("profile_id") != "minecraft-sunlit-cobblemon"
        or current.get("version") != context.version
        or current.get("manifest_sha256") != document["manifest_sha256"]
    ):
        _write_metadata(
            context.state_root,
            document,
            context,
            publication_action=PublicationAction.METADATA,
            owner=sunlit_owner,
        )
    record_path = context.candidate / "promotion.json"
    if not record_path.exists() and not record_path.is_symlink():
        return _publish_report(context, document)
    record = _regular_json(record_path, maximum=64 * 1024)
    if record != _report(context, document):
        raise PromotionError("active promotion record mismatch")
    return record


def _resume_published_release(context: PromotionContext, runtime: Path, document: dict, sunlit_owner: tuple[int, int]) -> dict | None:
    # A release with no active link is a fresh-install publication checkpoint.
    # Exactly one state source must exist: the prepared candidate before STATE,
    # or the fixed production tree after STATE.  Every other combination is an
    # ambiguous/colliding partial and fails closed.
    if not (
        context.release.is_dir()
        and not context.release.is_symlink()
        and runtime.is_dir()
        and not runtime.is_symlink()
        and not context.active_link.exists()
        and not context.active_link.is_symlink()
    ):
        return None
    candidate_state = context.candidate / "state"
    candidate_present = candidate_state.exists() or candidate_state.is_symlink()
    production_present = context.state_root.exists() or context.state_root.is_symlink()
    if candidate_present and production_present:
        raise PromotionError("candidate and production state collide")
    _verify_release_ownership(context.release)
    _bind_candidate_links(runtime, document, context)
    if _tree_digest(runtime) != _tree_digest(context.release):
        raise PromotionError("published release does not match the reviewed candidate")
    if candidate_present:
        candidate_identity = _verify_fresh_state_tree(
            context, candidate_state, document, sunlit_owner,
        )
        _guarded_replace(
            context,
            PublicationAction.STATE,
            candidate_state,
            context.state_root,
            context.state_root.parent,
            precondition=lambda: _assert_fresh_state_move(
                candidate_state, candidate_identity, context.state_root,
            ),
        )
        production_identity = _verify_fresh_state_tree(
            context, context.state_root, document, sunlit_owner,
        )
    elif production_present:
        production_identity = _verify_fresh_state_tree(
            context, context.state_root, document, sunlit_owner,
        )
    else:
        raise PromotionError("fresh publication state is unavailable")
    _guarded_activate(
        context,
        PublicationAction.ACTIVE_LINK,
        context.release,
        expected_prior=None,
        precondition=lambda: _assert_fresh_activation(
            context, candidate_state, production_identity, document,
            sunlit_owner,
        ),
    )
    shutil.rmtree(runtime)
    _fsync_dir(context.candidate)
    return _publish_report(context, document)


def _candidate_matches_existing_state(
    context: PromotionContext,
    state: Path,
    document: dict,
    *,
    allow_moved_version: bool = False,
) -> Path | None:
    """Prove the staged copy did not alter stable state; return only new version state."""
    try:
        policy = document["runtime_policy"]
        stable = tuple(policy["persistent_dirs"]) + tuple(policy["persistent_files"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PromotionError("promotion runtime policy is malformed") from exc
    expected_top = {
        Path(relative).parts[0]
        for relative in stable
        if (context.state_root / relative).exists()
    } | {".versions"}
    actual_top = {path.name for path in state.iterdir()}
    if actual_top != expected_top:
        raise PromotionError("candidate state member set is not exact")
    for relative in stable:
        if not isinstance(relative, str) or not relative or "/" in relative:
            raise PromotionError("promotion runtime policy is unsafe")
        candidate_path = state / relative
        current_path = context.state_root / relative
        if candidate_path.exists() != current_path.exists():
            raise PromotionError("candidate stable state is incomplete")
        if not candidate_path.exists():
            continue
        if candidate_path.is_symlink() or current_path.is_symlink():
            raise PromotionError("candidate stable state is incomplete")
        # Staging intentionally runs as root and may not preserve directory
        # mode bits from the service-owned state tree. It must preserve every
        # member, type, link target, and byte; production state is never
        # replaced by this copy.
        if _tree_digest(candidate_path, include_mode=False) != _tree_digest(current_path, include_mode=False):
            raise PromotionError(f"candidate stable state changed: {relative}")
    candidate_versions = state / ".versions"
    if candidate_versions.is_symlink() or not candidate_versions.is_dir():
        raise PromotionError("candidate version state is unsafe")
    members = list(candidate_versions.iterdir())
    if allow_moved_version and not members:
        return None
    if len(members) != 1 or members[0].name != context.version or members[0].is_symlink() or not members[0].is_dir():
        raise PromotionError("candidate version state is not exact")
    return members[0]


def _expected_version_state_members(document: dict) -> set[str]:
    try:
        policy = document["runtime_policy"]
        names = set(policy["mutable_vendor_dirs"]) | set(policy["empty_mutable_dirs"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PromotionError("promotion runtime policy is malformed") from exc
    if any(not isinstance(name, str) or not name or "/" in name for name in names):
        raise PromotionError("promotion runtime policy is unsafe")
    return names


def _verify_version_state_tree(
    version_state: Path,
    versions_root: Path,
    document: dict,
    sunlit_owner: tuple[int, int],
) -> Path:
    """Prove a version-state tree remains a direct child of its fixed root."""
    if version_state.parent != versions_root:
        raise PromotionError("version state escaped its fixed root")
    try:
        versions_resolved = versions_root.resolve(strict=True)
        version_state_resolved = version_state.resolve(strict=True)
        root_info = version_state.lstat()
    except OSError as exc:
        raise PromotionError("version state is unavailable") from exc
    if (
        version_state_resolved.parent != versions_resolved
        or stat.S_ISLNK(root_info.st_mode)
        or not stat.S_ISDIR(root_info.st_mode)
        or (root_info.st_uid, root_info.st_gid) != sunlit_owner
        or stat.S_IMODE(root_info.st_mode) != 0o750
    ):
        raise PromotionError("version state is unsafe")
    expected_top = _expected_version_state_members(document)
    try:
        actual_top = {entry.name for entry in os.scandir(version_state)}
    except OSError as exc:
        raise PromotionError("version state is unavailable") from exc
    if actual_top != expected_top:
        raise PromotionError("version state member set is not exact")
    pending = [version_state / name for name in sorted(expected_top)]
    while pending:
        current = pending.pop()
        try:
            info = current.lstat()
        except OSError as exc:
            raise PromotionError("version state is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or (info.st_uid, info.st_gid) != sunlit_owner:
            raise PromotionError("version state is unsafe")
        if stat.S_ISDIR(info.st_mode):
            if stat.S_IMODE(info.st_mode) != 0o750:
                raise PromotionError("version state is unsafe")
            try:
                pending.extend(Path(entry.path) for entry in os.scandir(current))
            except OSError as exc:
                raise PromotionError("version state is unavailable") from exc
        elif stat.S_ISREG(info.st_mode):
            if info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o640:
                raise PromotionError("version state is unsafe")
        else:
            raise PromotionError("version state is unsafe")
    return version_state


def _verify_production_version_state(
    context: PromotionContext,
    document: dict,
    sunlit_owner: tuple[int, int],
) -> Path:
    """Prove an already-moved version state before selecting its release."""
    versions_root = context.state_root / ".versions"
    return _verify_version_state_tree(
        versions_root / context.version, versions_root, document, sunlit_owner,
    )


def _verify_fresh_state_tree(
    context: PromotionContext,
    state: Path,
    document: dict,
    sunlit_owner: tuple[int, int],
) -> _PathIdentity:
    """Prove a fully prepared fresh state tree before moving or activating it."""
    fixed_parent = (
        context.candidate if state == context.candidate / "state"
        else context.state_root.parent if state == context.state_root
        else None
    )
    if fixed_parent is None or state.parent != fixed_parent:
        raise PromotionError("fresh state escaped its fixed root")
    try:
        parent_resolved = fixed_parent.resolve(strict=True)
        state_resolved = state.resolve(strict=True)
        state_info = state.lstat()
    except OSError as exc:
        raise PromotionError("fresh state is unavailable") from exc
    if (
        state_resolved.parent != parent_resolved
        or stat.S_ISLNK(state_info.st_mode)
        or not stat.S_ISDIR(state_info.st_mode)
        or (state_info.st_uid, state_info.st_gid) != sunlit_owner
        or stat.S_IMODE(state_info.st_mode) != 0o750
    ):
        raise PromotionError("fresh state is unsafe")
    try:
        policy = document["runtime_policy"]
        persistent_dirs = list(policy["persistent_dirs"])
        persistent_files = list(policy["persistent_files"])
        stable_names = persistent_dirs + persistent_files
        required_names = list(policy["required_paths"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PromotionError("promotion runtime policy is malformed") from exc
    if (
        any(
            not isinstance(name, str)
            or not name
            or "/" in name
            or name in {".", "..", ".versions", ".horizon"}
            for name in stable_names + required_names
        )
        or len(stable_names) != len(set(stable_names))
        or len(required_names) != len(set(required_names))
        or not set(required_names).issubset(stable_names)
    ):
        raise PromotionError("promotion runtime policy is unsafe")
    required_top = set(required_names) | {".versions", ".horizon"}
    allowed_top = set(stable_names) | {".versions", ".horizon"}
    try:
        actual_top = {entry.name for entry in os.scandir(state)}
    except OSError as exc:
        raise PromotionError("fresh state is unavailable") from exc
    if not required_top.issubset(actual_top) or not actual_top.issubset(allowed_top):
        raise PromotionError("fresh state member set is not exact")
    for name in persistent_dirs:
        path = state / name
        if (path.exists() or path.is_symlink()) and (path.is_symlink() or not path.is_dir()):
            raise PromotionError("fresh persistent directory is unsafe")
    for name in persistent_files:
        path = state / name
        if (path.exists() or path.is_symlink()) and (path.is_symlink() or not path.is_file()):
            raise PromotionError("fresh persistent file is unsafe")
    pending = [state / name for name in sorted(actual_top)]
    while pending:
        current = pending.pop()
        try:
            info = current.lstat()
        except OSError as exc:
            raise PromotionError("fresh state is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or (info.st_uid, info.st_gid) != sunlit_owner:
            raise PromotionError("fresh state is unsafe")
        if stat.S_ISDIR(info.st_mode):
            if stat.S_IMODE(info.st_mode) != 0o750:
                raise PromotionError("fresh state is unsafe")
            try:
                pending.extend(Path(entry.path) for entry in os.scandir(current))
            except OSError as exc:
                raise PromotionError("fresh state is unavailable") from exc
        elif stat.S_ISREG(info.st_mode):
            if info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o640:
                raise PromotionError("fresh state is unsafe")
        else:
            raise PromotionError("fresh state is unsafe")
    versions_root = state / ".versions"
    try:
        version_members = {entry.name for entry in os.scandir(versions_root)}
    except OSError as exc:
        raise PromotionError("fresh state is unavailable") from exc
    if version_members != {context.version}:
        raise PromotionError("fresh version state member set is not exact")
    _verify_version_state_tree(
        versions_root / context.version, versions_root, document, sunlit_owner,
    )
    metadata = state / ".horizon"
    try:
        metadata_members = {entry.name for entry in os.scandir(metadata)}
    except OSError as exc:
        raise PromotionError("fresh state metadata is unavailable") from exc
    if metadata_members != {"manifest.json", "release.json"}:
        raise PromotionError("fresh state metadata member set is not exact")
    manifest_record = _regular_json(
        metadata / "manifest.json", maximum=8 * 1024 * 1024,
        owners=(sunlit_owner,),
    )
    release_record = _regular_json(
        metadata / "release.json", maximum=64 * 1024,
        owners=(sunlit_owner,),
    )
    expected_release = {
        "profile_id": "minecraft-sunlit-cobblemon",
        "version": context.version,
        "manifest_sha256": document["manifest_sha256"],
        "archive_sha256": document["artifact"]["archive"]["sha256"],
    }
    if manifest_record != document or release_record != expected_release:
        raise PromotionError("fresh state metadata identity mismatch")
    return _PathIdentity(
        device=state_info.st_dev,
        inode=state_info.st_ino,
        file_type=stat.S_IFMT(state_info.st_mode),
        owner_uid=state_info.st_uid,
        owner_gid=state_info.st_gid,
        mode=stat.S_IMODE(state_info.st_mode),
    )


def _assert_fresh_state_move(
    candidate_state: Path,
    candidate_identity: _PathIdentity,
    production_state: Path,
) -> None:
    """Fence a fresh STATE rename to its previously validated source."""
    _require_path_identity(candidate_state, candidate_identity)
    _require_path_absent(production_state)


def _assert_fresh_activation(
    context: PromotionContext,
    candidate_state: Path,
    production_identity: _PathIdentity,
    document: dict,
    sunlit_owner: tuple[int, int],
) -> None:
    """Repeat the complete state proof inside the ACTIVE_LINK owner fence."""
    _require_path_identity(context.state_root, production_identity)
    _require_path_absent(candidate_state)
    _require_path_absent(context.active_link)
    verified = _verify_fresh_state_tree(
        context, context.state_root, document, sunlit_owner,
    )
    if verified != production_identity:
        raise PromotionError("production state identity changed during validation")
    # Keep the final pointer prerequisites adjacent to activation even when the
    # recursive proof scanned a large world tree.
    _require_path_identity(context.state_root, production_identity)
    _require_path_absent(candidate_state)
    _require_path_absent(context.active_link)


def _metadata_backup(context: PromotionContext, sunlit_owner: tuple[int, int]) -> dict[str, tuple[bytes, int, int, int] | None]:
    metadata = context.state_root / ".horizon"
    if metadata.exists() or metadata.is_symlink():
        _trusted_directory(metadata, owners=((0, 0), sunlit_owner))
    result: dict[str, tuple[bytes, int, int, int] | None] = {}
    for name in ("manifest.json", "release.json"):
        path = metadata / name
        if path.exists() or path.is_symlink():
            _regular_json(path, maximum=8 * 1024 * 1024, owners=((0, 0), sunlit_owner))
            info = path.stat()
            result[name] = (path.read_bytes(), stat.S_IMODE(info.st_mode), info.st_uid, info.st_gid)
        else:
            result[name] = None
    return result


def _restore_metadata(context: PromotionContext, backup: dict[str, tuple[bytes, int, int, int] | None]) -> None:
    metadata = context.state_root / ".horizon"
    metadata.mkdir(mode=0o700, exist_ok=True)
    prepared: dict[str, Path] = {}
    for name, value in backup.items():
        if value is None:
            continue
        temporary = metadata / f".{name}.rollback"
        temporary.write_bytes(value[0])
        os.chmod(temporary, value[1])
        os.chown(temporary, value[2], value[3])
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        prepared[name] = temporary
    try:
        with _publication(context, PublicationAction.ROLLBACK_METADATA):
            for name, value in backup.items():
                path = metadata / name
                if value is None:
                    path.unlink(missing_ok=True)
                else:
                    os.replace(prepared[name], path)
            _fsync_dir(metadata)
    finally:
        for temporary in prepared.values():
            temporary.unlink(missing_ok=True)


def _resume_upgrade_publication(
    context: PromotionContext,
    runtime: Path,
    state: Path,
    document: dict,
    sunlit_owner: tuple[int, int],
) -> dict | None:
    """Resume an upgrade after one of its guarded publications completed."""
    if not (
        context.state_root.is_dir()
        and not context.state_root.is_symlink()
        and context.active_link.is_symlink()
        and context.release.is_dir()
        and not context.release.is_symlink()
    ):
        return None
    _trusted_directory(context.state_root, owners=(sunlit_owner,))
    _verify_release_ownership(context.release)
    active_target = os.readlink(context.active_link)
    active_release = (context.active_link.parent / active_target).resolve(strict=True)
    versions_root = context.state_root / ".versions"
    _trusted_directory(versions_root, owners=(sunlit_owner,))
    production_version_state = versions_root / context.version
    candidate_version_state = state / ".versions" / context.version
    production_present = production_version_state.exists() or production_version_state.is_symlink()
    candidate_present = candidate_version_state.exists() or candidate_version_state.is_symlink()
    if active_release == context.release.resolve(strict=True):
        # Active link is already committed.  Metadata and candidate cleanup
        # are resumable post-commit work and remain guarded where stable state
        # is changed.
        if not production_present:
            raise PromotionError("active upgrade version state is unavailable")
        if candidate_present:
            raise PromotionError("candidate and production version state collide")
        _verify_production_version_state(context, document, sunlit_owner)
        _write_metadata(
            context.state_root,
            document,
            context,
            publication_action=PublicationAction.METADATA,
            owner=sunlit_owner,
        )
        if runtime.exists() or runtime.is_symlink():
            _trusted_directory(runtime)
            _bind_candidate_links(runtime, document, context)
            if _tree_digest(runtime) != _tree_digest(context.release):
                raise PromotionError("active upgrade retained a mismatched candidate")
            shutil.rmtree(runtime)
        if state.exists() or state.is_symlink():
            _trusted_directory(state, owners=((0, 0), sunlit_owner))
            _candidate_matches_existing_state(
                context, state, document, allow_moved_version=True,
            )
            shutil.rmtree(state)
        _fsync_dir(context.candidate)
        return _publish_report(context, document)
    prior_release = (context.active_link.parent / active_target).resolve()
    if prior_release.parent != context.release_root.resolve() or not prior_release.is_dir() or prior_release.is_symlink():
        raise PromotionError("active upgrade target is unsafe")
    if production_present:
        if candidate_present:
            raise PromotionError("candidate and production version state collide")
        if state.exists() or state.is_symlink():
            _candidate_matches_existing_state(
                context, state, document, allow_moved_version=True,
            )
        _verify_production_version_state(context, document, sunlit_owner)
    else:
        if not state.is_dir() or state.is_symlink():
            raise PromotionError("upgrade version state is unavailable for resume")
        version_state = _candidate_matches_existing_state(context, state, document)
        if version_state is None:
            raise PromotionError("upgrade version state is unavailable for resume")
        _prepare_state_ownership(version_state)
        _guarded_replace(
            context,
            PublicationAction.VERSION_STATE,
            version_state,
            production_version_state,
            versions_root,
        )
        _verify_production_version_state(context, document, sunlit_owner)
    _guarded_activate(
        context,
        PublicationAction.ACTIVE_LINK,
        context.release,
        expected_prior=active_target,
    )
    _write_metadata(
        context.state_root,
        document,
        context,
        publication_action=PublicationAction.METADATA,
        owner=sunlit_owner,
    )
    if runtime.exists() or runtime.is_symlink():
        _trusted_directory(runtime)
        _bind_candidate_links(runtime, document, context)
        if _tree_digest(runtime) != _tree_digest(context.release):
            raise PromotionError("resumed upgrade retained a mismatched candidate")
        shutil.rmtree(runtime)
    if state.exists() or state.is_symlink():
        _trusted_directory(state, owners=((0, 0), sunlit_owner))
        _candidate_matches_existing_state(
            context, state, document, allow_moved_version=True,
        )
        shutil.rmtree(state)
    _fsync_dir(context.candidate)
    return _publish_report(context, document)


def _promote_upgrade(context: PromotionContext, runtime: Path, state: Path, document: dict, sunlit_owner: tuple[int, int]) -> dict:
    _trusted_directory(context.state_root, owners=(sunlit_owner,))
    if not context.active_link.is_symlink() or context.release.exists() or context.release.is_symlink():
        raise PromotionError("existing deployment is not eligible for an upgrade")
    prior_target = os.readlink(context.active_link)
    prior_release = (context.active_link.parent / prior_target).resolve()
    release_root = context.release_root.resolve()
    if prior_release.parent != release_root or prior_release.is_symlink() or not prior_release.is_dir():
        raise PromotionError("active release target is unsafe")
    version_state = _candidate_matches_existing_state(context, state, document)
    if version_state is None:
        raise PromotionError("candidate version state is unavailable")
    versions_root = context.state_root / ".versions"
    _trusted_directory(versions_root, owners=(sunlit_owner,))
    production_version_state = versions_root / context.version
    if production_version_state.exists() or production_version_state.is_symlink():
        raise PromotionError("production version state already exists")
    if version_state.stat().st_dev != versions_root.stat().st_dev:
        raise PromotionError("version state promotion requires one filesystem")
    _bind_candidate_links(runtime, document, context)
    _normalize_release_permissions(runtime)
    _verify_release_ownership(runtime)
    metadata_backup = _metadata_backup(context, sunlit_owner)
    release_stage = context.release_root / f".{context.version}.promote.{os.getpid()}"
    release_published = False
    version_state_moved = False
    metadata_attempted = False
    activated = False
    if release_stage.exists() or release_stage.is_symlink():
        # A guard refusal before the rename can leave a private copy behind.
        # It is safe to rebuild that exact fixed-path staging copy because the
        # canonical release has not yet been published.
        if context.release.exists() or release_stage.is_symlink():
            raise PromotionError("release staging path already exists")
        _trusted_directory(release_stage)
        shutil.rmtree(release_stage)
    try:
        shutil.copytree(runtime, release_stage, symlinks=True)
        _verify_release_ownership(release_stage)
        _fsync_tree(release_stage)
        _prepare_state_ownership(version_state)
        _guarded_replace(
            context,
            PublicationAction.RELEASE,
            release_stage,
            context.release,
            context.release_root,
        )
        release_published = True
        _guarded_replace(
            context,
            PublicationAction.VERSION_STATE,
            version_state,
            production_version_state,
            versions_root,
        )
        version_state_moved = True
        _verify_production_version_state(context, document, sunlit_owner)
        _guarded_activate(
            context,
            PublicationAction.ACTIVE_LINK,
            context.release,
            expected_prior=prior_target,
        )
        activated = True
        # Active-link activation is the commit point.  Metadata is repaired
        # last, so a refused activation leaves the prior release and prior
        # metadata coherent for lifecycle startup.
        metadata_attempted = True
        _write_metadata(
            context.state_root,
            document,
            context,
            publication_action=PublicationAction.METADATA,
            owner=sunlit_owner,
        )
    except BaseException as exc:
        # A sticky ownership refusal is itself a resumable checkpoint.  Do
        # not attempt rollback through the same refused owner fence; leaving
        # the forward publication record intact lets the next owned retry
        # converge without claiming a version that was never activated.
        if isinstance(exc, PublicationRefused):
            raise
        if activated and context.active_link.is_symlink():
            _guarded_activate(
                context,
                PublicationAction.ROLLBACK_ACTIVE_LINK,
                prior_release,
                expected_prior=os.readlink(context.active_link),
            )
        if metadata_attempted:
            _restore_metadata(context, metadata_backup)
        if version_state_moved and production_version_state.exists() and not version_state.exists():
            _guarded_replace(
                context,
                PublicationAction.ROLLBACK_VERSION_STATE,
                production_version_state,
                version_state,
                versions_root,
            )
        if release_published and context.release.exists():
            rollback_release = context.release_root / f".{context.version}.rollback.{os.getpid()}"
            if rollback_release.exists() or rollback_release.is_symlink():
                raise PromotionError("release rollback path already exists")
            _guarded_replace(
                context,
                PublicationAction.ROLLBACK_RELEASE,
                context.release,
                rollback_release,
                context.release_root,
            )
            shutil.rmtree(rollback_release)
        if release_stage.exists():
            shutil.rmtree(release_stage)
        raise
    shutil.rmtree(runtime)
    shutil.rmtree(state)
    _fsync_dir(context.candidate)
    return _publish_report(context, document)


def _promote(context: PromotionContext) -> dict:
    if os.geteuid() != 0:
        raise PromotionError("promotion requires root")
    _inactive(context)
    document = _manifest(context)
    _trusted_directory(context.candidate)
    candidate = _regular_json(context.candidate / "candidate.json", maximum=64 * 1024)
    if candidate.get("active") is not False or candidate.get("version") != context.version or candidate.get("manifest_sha256") != document["manifest_sha256"]:
        raise PromotionError("candidate record identity mismatch")
    _trusted_directory(context.release_root)
    _trusted_directory(context.state_root.parent)
    _trusted_directory(context.active_link.parent)
    sunlit_owner = _sunlit_ids()
    completed = _already_promoted(context, document, sunlit_owner)
    if completed is not None:
        return completed
    runtime = context.candidate / "runtime"
    state = context.candidate / "state"
    _trusted_directory(runtime)
    resumed = _resume_published_release(context, runtime, document, sunlit_owner)
    if resumed is not None:
        return resumed
    resumed = _resume_upgrade_publication(
        context, runtime, state, document, sunlit_owner,
    )
    if resumed is not None:
        return resumed
    _trusted_directory(state, owners=((0, 0), sunlit_owner))
    if context.state_root.exists() and context.active_link.is_symlink():
        return _promote_upgrade(context, runtime, state, document, sunlit_owner)
    if any(path.exists() or path.is_symlink() for path in (context.release, context.state_root, context.active_link)):
        raise PromotionError("production candidate destination already exists")
    if state.stat().st_dev != context.state_root.parent.stat().st_dev:
        raise PromotionError("state promotion requires one filesystem")
    _bind_candidate_links(runtime, document, context)
    _normalize_release_permissions(runtime)
    _write_metadata(state, document, context)
    _prepare_state_ownership(state)
    _verify_release_ownership(runtime)
    state_moved = False
    release_published = False
    release_stage = context.release_root / f".{context.version}.promote.{os.getpid()}"
    if release_stage.exists() or release_stage.is_symlink():
        if context.release.exists() or release_stage.is_symlink():
            raise PromotionError("release staging path already exists")
        _trusted_directory(release_stage)
        shutil.rmtree(release_stage)
    try:
        # Build, verify, and durably flush the potentially large release while
        # the caller's short operation-lock publication fence remains free.
        shutil.copytree(runtime, release_stage, symlinks=True)
        _verify_release_ownership(release_stage)
        _fsync_tree(release_stage)
        _guarded_replace(
            context,
            PublicationAction.RELEASE,
            release_stage,
            context.release,
            context.release_root,
        )
        release_published = True
        candidate_identity = _verify_fresh_state_tree(
            context, state, document, sunlit_owner,
        )
        _guarded_replace(
            context,
            PublicationAction.STATE,
            state,
            context.state_root,
            context.state_root.parent,
            precondition=lambda: _assert_fresh_state_move(
                state, candidate_identity, context.state_root,
            ),
        )
        state_moved = True
        production_identity = _verify_fresh_state_tree(
            context, context.state_root, document, sunlit_owner,
        )
        _guarded_activate(
            context,
            PublicationAction.ACTIVE_LINK,
            context.release,
            expected_prior=None,
            precondition=lambda: _assert_fresh_activation(
                context, state, production_identity, document, sunlit_owner,
            ),
        )
    except BaseException as exc:
        if isinstance(exc, PublicationRefused):
            raise
        if context.active_link.is_symlink():
            _guarded_unlink_active(context)
        if release_published and context.release.exists():
            rollback_release = context.release_root / f".{context.version}.rollback.{os.getpid()}"
            if rollback_release.exists() or rollback_release.is_symlink():
                raise PromotionError("release rollback path already exists")
            _guarded_replace(
                context,
                PublicationAction.ROLLBACK_RELEASE,
                context.release,
                rollback_release,
                context.release_root,
            )
            shutil.rmtree(rollback_release)
        if release_stage.exists():
            shutil.rmtree(release_stage)
        if state_moved and context.state_root.exists() and not state.exists():
            _guarded_replace(
                context,
                PublicationAction.ROLLBACK_STATE,
                context.state_root,
                state,
                state.parent,
            )
        raise
    shutil.rmtree(runtime)
    _fsync_dir(context.candidate)
    return _publish_report(context, document)


def promote(context: PromotionContext | None = None) -> dict:
    """Promote one candidate only with explicit publication authority."""
    if context is None or context.publication_guard is None:
        raise PromotionError("publication guard is required")
    return _promote(context)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", default=VERSION)
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--candidate-root", type=Path, default=CANDIDATE)
    parser.add_argument("--manifest-sha256")
    args = parser.parse_args(argv)
    # These source-only compatibility names are historical fixed-policy front
    # doors.  The normal updater uses promote_candidate() and may supply its
    # reviewed staging root; this CLI must never turn arbitrary caller paths
    # into a promotion authority.
    if (
        args.version != VERSION
        or args.manifest != Path(MANIFEST)
        or args.candidate_root != Path(CANDIDATE)
        or (args.manifest_sha256 is not None and args.manifest_sha256 != MANIFEST_SHA256)
    ):
        return 2
    print("error: historical promotion front door is unsupported", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
