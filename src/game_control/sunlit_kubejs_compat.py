#!/usr/bin/env python3
"""Hash/version-bound compatibility transforms for staged Sunlit releases.

Sunlit ``1.1.4-SSV4.1.5`` enables one redundant ``JsonIO.write`` in
``kubejs/server_scripts/cobblemon/datagen/generateLootTables.js``.  Releases are
published read-only, so that write raises ``EROFS`` and aborts the generator.
The artifact already ships the generator's 18 ``badge_reward`` tables, so the
single statement can be neutralised while staging without changing the rewards
players receive -- but only after the exact reviewed generator and every baked
table have been verified against the reviewed generator output.

The transform never guesses.  An unrecognised generator digest, an unreviewed
artifact version, or a reward set that no longer matches the reviewed generator
output stops the stage with an actionable error instead of silently disabling
generation that may still be meaningful.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

TRANSFORM_ID = "sunlit-kubejs-gym-loot-write-guard"
GENERATOR_PATH = "kubejs/server_scripts/cobblemon/datagen/generateLootTables.js"
REWARD_ROOT = "kubejs/data/sunlit_cobblemon/loot_tables/badge_reward"
REWARD_SUFFIX = "_type_gym.json"
# Reviewed gym types baked by the generator.
REWARD_KINDS = (
    "bug", "dark", "dragon", "electric", "fairy", "fighting", "fire", "flying",
    "ghost", "grass", "ground", "ice", "normal", "poison", "psychic", "rock",
    "steel", "water",
)
MAX_GENERATOR_BYTES = 1024 * 1024
MAX_TABLE_BYTES = 256 * 1024
HEX64 = frozenset("0123456789abcdef")

# Reviewed upstream bytes of the artifact generator.  These are digests of a
# third-party build, not deployment configuration; the reviewed replacement is
# the single line of vendor source this transform is allowed to change.
REVIEWED_ENABLED_SHA256 = "25cb09d72986672bcb88ec25832c8c27a0a361597d8aab3f2bb0a225c1a78b72"
REVIEWED_GUARDED_SHA256 = "5a95e607e513667157f761c358bf97452c2e0cad77a4dc6d795b8b09205be930"
# Generators that already ship without the active write: the guarded output
# above plus the retained previous release (whole generator body disabled).
REVIEWED_DISABLED_SHA256 = (
    REVIEWED_GUARDED_SHA256,
    "91269a070b2645aac130fc91d206603501ea9a8a85e0a6242b1da6fd21725a15",
)
REVIEWED_VERSIONS = ("1.1.4-SSV4.1.5",)
GYM_LOOT_STATEMENT = (
    "    JsonIO.write(`kubejs/data/sunlit_cobblemon/loot_tables/badge_reward/"
    "${type.type}_type_gym.json`, lootTable);"
)
GYM_LOOT_REPLACEMENT = "    // " + GYM_LOOT_STATEMENT.lstrip(" ")


class CompatTransformError(ValueError):
    """A compatibility transform could not be validated or applied."""


def _valid_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and value == value.lower()
        and all(char in HEX64 for char in value)
    )


def _safe_rel(raw: Any, label: str) -> str:
    if not isinstance(raw, str) or not raw or "\\" in raw:
        raise CompatTransformError(f"{label} is not a safe relative path")
    # Explicit lexical validation: nothing is silently normalised away.
    if raw != raw.strip() or raw.startswith("/") or raw.endswith("/") or "//" in raw:
        raise CompatTransformError(f"{label} is not a safe relative path")
    if any(part in {"", ".", ".."} for part in raw.split("/")):
        raise CompatTransformError(f"{label} is not a safe relative path")
    return raw


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class GymLootGuard:
    """One reviewed generator guard carried by a release manifest."""

    transform_id: str = TRANSFORM_ID
    path: str = GENERATOR_PATH
    reward_root: str = REWARD_ROOT
    reward_kinds: tuple[str, ...] = REWARD_KINDS
    versions: tuple[str, ...] = ()
    enabled_sha256: str = ""
    guarded_sha256: str = ""
    known_disabled_sha256: tuple[str, ...] = ()
    statement: str = ""
    replacement: str = ""

    @classmethod
    def from_policy(cls, value: Any) -> "GymLootGuard":
        if not isinstance(value, Mapping):
            raise CompatTransformError("compatibility transform entry is malformed")
        transform_id = value.get("transform_id")
        if transform_id != TRANSFORM_ID:
            raise CompatTransformError(
                f"unsupported compatibility transform {transform_id!r}; "
                f"only {TRANSFORM_ID!r} is reviewed for this staging profile"
            )
        try:
            path = _safe_rel(value["path"], "compatibility transform path")
            reward_root = _safe_rel(value["reward_root"], "compatibility reward root")
        except KeyError as exc:
            raise CompatTransformError("compatibility transform entry is incomplete") from exc
        raw_kinds = value.get("reward_kinds")
        if not isinstance(raw_kinds, (list, tuple)) or not raw_kinds:
            raise CompatTransformError("compatibility transform reward kinds are malformed")
        kinds: list[str] = []
        for kind in raw_kinds:
            safe = _safe_rel(kind, "compatibility reward kind")
            if "/" in safe:
                raise CompatTransformError("compatibility reward kind is not one path component")
            kinds.append(safe)
        raw_versions = value.get("versions")
        if not isinstance(raw_versions, (list, tuple)):
            raise CompatTransformError("compatibility transform versions are malformed")
        versions: list[str] = []
        for version in raw_versions:
            safe = _safe_rel(version, "compatibility transform version")
            if "/" in safe:
                raise CompatTransformError("compatibility transform version is not one path component")
            versions.append(safe)
        enabled = value.get("enabled_sha256")
        guarded = value.get("guarded_sha256")
        known = value.get("known_disabled_sha256")
        if not _valid_sha256(enabled) or not _valid_sha256(guarded):
            raise CompatTransformError("compatibility transform digests are malformed")
        if not isinstance(known, (list, tuple)) or not known or not all(_valid_sha256(item) for item in known):
            raise CompatTransformError("compatibility transform disabled digests are malformed")
        if guarded not in known:
            raise CompatTransformError("compatibility transform guarded digest is not in its disabled set")
        statement = value.get("statement")
        replacement = value.get("replacement")
        if not isinstance(statement, str) or not statement or not isinstance(replacement, str):
            raise CompatTransformError("compatibility transform replacement is malformed")
        if replacement == statement:
            raise CompatTransformError("compatibility transform replacement is a no-op")
        return cls(
            transform_id=TRANSFORM_ID, path=path, reward_root=reward_root,
            reward_kinds=tuple(kinds), versions=tuple(versions), enabled_sha256=enabled,
            guarded_sha256=guarded, known_disabled_sha256=tuple(known),
            statement=statement, replacement=replacement,
        )

    def as_policy(self) -> dict[str, Any]:
        return {
            "transform_id": self.transform_id,
            "path": self.path,
            "reward_root": self.reward_root,
            "reward_kinds": list(self.reward_kinds),
            "versions": list(self.versions),
            "enabled_sha256": self.enabled_sha256,
            "guarded_sha256": self.guarded_sha256,
            "known_disabled_sha256": list(self.known_disabled_sha256),
            "statement": self.statement,
            "replacement": self.replacement,
        }


def _assert_reviewed(guard: GymLootGuard) -> GymLootGuard:
    """Fail closed at import if the code-reviewed record is not self-consistent."""
    if (
        guard.transform_id != TRANSFORM_ID
        or guard.path != GENERATOR_PATH
        or guard.reward_root != REWARD_ROOT
        or len(guard.reward_kinds) != 18
        or len(set(guard.reward_kinds)) != 18
        or not guard.versions
        or guard.enabled_sha256 == guard.guarded_sha256
        or guard.guarded_sha256 not in guard.known_disabled_sha256
        or guard.statement == guard.replacement
    ):
        raise RuntimeError("reviewed KubeJS compatibility record is inconsistent")
    return guard


# The code-reviewed authority.  A manifest may only restate this record; its
# self-digest is integrity, not authority, so a declared guard that differs in
# any field is refused instead of being adopted.
_REVIEWED_GYM_LOOT_GUARD = _assert_reviewed(GymLootGuard(
    versions=REVIEWED_VERSIONS,
    enabled_sha256=REVIEWED_ENABLED_SHA256,
    guarded_sha256=REVIEWED_GUARDED_SHA256,
    known_disabled_sha256=REVIEWED_DISABLED_SHA256,
    statement=GYM_LOOT_STATEMENT,
    replacement=GYM_LOOT_REPLACEMENT,
))


def reviewed_gym_loot_guard() -> GymLootGuard:
    """The reviewed staging guard for the Sunlit gym-reward generator."""
    return _REVIEWED_GYM_LOOT_GUARD


def reviewed_guard_registry() -> dict[str, GymLootGuard]:
    """Every transform the code review authorises, by transform id."""
    guard = reviewed_gym_loot_guard()
    return {guard.transform_id: guard}


def _altered_fields(declared: GymLootGuard, reviewed: GymLootGuard) -> list[str]:
    declared_record = declared.as_policy()
    reviewed_record = reviewed.as_policy()
    return sorted(key for key in reviewed_record if declared_record.get(key) != reviewed_record[key])


def reviewed_compatibility_transforms() -> list[dict[str, Any]]:
    """Fresh policy records for every reviewed staging transform."""
    return [reviewed_gym_loot_guard().as_policy()]


def expected_reward_table(kind: str) -> dict[str, Any]:
    """Reconstruct the reviewed generator's gym reward table for one type."""
    groups = (
        (("sunlit_cobblemon:sun_drops", "cobblemon:relic_coin", "cobblemon:exp_candy_s"), 6),
        ((f"sunlit_cobblemon:pristine_{kind}_gem",), 1),
        (("sunlit_cobblemon:mystica_branch", "cobblemon:rare_candy"), 1),
        ((f"simpletms:type_{kind}_tm",), 1),
    )
    entries: list[dict[str, Any]] = []
    for index, (items, scale) in enumerate(groups):
        for item in items:
            tagged = "simpletms" in item
            entry: dict[str, Any] = {
                "type": "minecraft:tag" if tagged else "minecraft:item",
                "weight": 1 if tagged else (6 if index == 1 else 2 if index >= 2 else 4) * 50,
                "name": item,
            }
            if tagged:
                entry["expand"] = True
            if scale > 1:
                entry["functions"] = [{
                    "function": "minecraft:set_count",
                    "count": {"type": "minecraft:uniform", "min": 1, "max": scale},
                }]
            entries.append(entry)
    return {"type": "minecraft:chest", "pools": [{"rolls": 1, "entries": entries}]}


def _load_guarded_file(root: Path, relative: str, label: str) -> tuple[Path, bytes]:
    path = Path(root).joinpath(*relative.split("/"))
    if Path(root).is_symlink() or not Path(root).is_dir():
        raise CompatTransformError(f"compatibility root is missing or unsafe: {root}")
    if path.is_symlink() or not path.is_file():
        raise CompatTransformError(f"{label} is missing or unsafe: {relative}")
    real_root = os.path.realpath(root)
    real_path = os.path.realpath(path)
    if real_path != os.path.join(real_root, *relative.split("/")):
        raise CompatTransformError(f"{label} escapes its release root: {relative}")
    payload = _read_bounded(path, label, MAX_GENERATOR_BYTES)
    return path, payload


def _read_bounded(path: Path, label: str, maximum: int) -> bytes:
    """Read a regular file without following links, bounded before allocating."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise CompatTransformError(f"{label} is unreadable or unsafe: {path.name}") from exc
    try:
        info = os.fstat(descriptor)
        # A staged artifact member is a private single-link regular file; a
        # symlink or hardlink here would escape or alias the reviewed bytes.
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > maximum:
            raise CompatTransformError(f"{label} is unsafe or oversized: {path.name}")
        payload = b""
        while len(payload) <= maximum:
            chunk = os.read(descriptor, min(1024 * 1024, maximum + 1 - len(payload)))
            if not chunk:
                break
            payload += chunk
    except OSError as exc:
        raise CompatTransformError(f"{label} is unreadable: {path.name}") from exc
    finally:
        os.close(descriptor)
    if len(payload) > maximum:
        raise CompatTransformError(f"{label} is oversized: {path.name}")
    return payload


def verify_reward_tables(root: Path, guard: GymLootGuard) -> dict[str, Any]:
    """Prove every baked reward table matches the reviewed generator output."""
    directory = Path(root).joinpath(*guard.reward_root.split("/"))
    if directory.is_symlink() or not directory.is_dir():
        raise CompatTransformError(
            f"compatibility reward directory is missing or unsafe: {guard.reward_root}"
        )
    try:
        members = sorted(directory.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise CompatTransformError(
            f"compatibility reward directory is unreadable: {guard.reward_root}"
        ) from exc
    found: dict[str, Path] = {}
    for member in members:
        if not member.name.endswith(REWARD_SUFFIX):
            continue
        if member.is_symlink() or not member.is_file():
            raise CompatTransformError(f"gym reward table is missing or unsafe: {member.name}")
        kind = member.name[: -len(REWARD_SUFFIX)]
        if not kind or kind in found:
            raise CompatTransformError(f"gym reward table name is malformed: {member.name}")
        found[kind] = member
    expected = tuple(guard.reward_kinds)
    missing = tuple(kind for kind in expected if kind not in found)
    extra = tuple(kind for kind in sorted(found) if kind not in set(expected))
    if missing or extra:
        raise CompatTransformError(
            f"{guard.transform_id} expected exactly {len(expected)} {REWARD_SUFFIX} tables under "
            f"{guard.reward_root}; missing {list(missing)} and unexpected {list(extra)}. "
            "Review the new artifact's baked rewards and update the reviewed transform before "
            "staging."
        )
    digest = hashlib.sha256()
    for kind in expected:
        member = found[kind]
        payload = _read_bounded(member, "gym reward table", MAX_TABLE_BYTES)
        try:
            document = json.loads(payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise CompatTransformError(f"gym reward table is not valid JSON: {member.name}") from exc
        if document != expected_reward_table(kind):
            raise CompatTransformError(
                f"gym reward table does not match the reviewed generator output: {member.name}. "
                "The baked rewards changed, so the redundant write is no longer presumed safe."
            )
        digest.update(kind.encode("utf-8"))
        digest.update(b"\0")
        digest.update(_sha256(payload).encode("ascii"))
        digest.update(b"\n")
    return {"count": len(expected), "kinds": list(expected), "sha256": digest.hexdigest()}


def _generator_relative(guard: GymLootGuard) -> str:
    return guard.path


def _plan(root: Path, guard: GymLootGuard, version: str) -> dict[str, Any]:
    if not isinstance(version, str):
        raise CompatTransformError("compatibility transform version is malformed")
    safe_version = _safe_rel(version, "artifact version")
    if "/" in safe_version:
        raise CompatTransformError("artifact version is not one path component")
    relative = _generator_relative(guard)
    path, payload = _load_guarded_file(root, relative, "KubeJS gym-loot generator")
    if len(payload) > MAX_GENERATOR_BYTES:
        raise CompatTransformError(f"KubeJS gym-loot generator is oversized: {relative}")
    digest = _sha256(payload)
    tables = verify_reward_tables(root, guard)
    record: dict[str, Any] = {
        "transform_id": guard.transform_id,
        "path": relative,
        "version": safe_version,
        "generator_sha256_before": digest,
        "reward_tables_checked": tables["count"],
        "reward_tables_sha256": tables["sha256"],
    }
    if digest in guard.known_disabled_sha256:
        record["action"] = "already_disabled"
        record["generator_sha256_after"] = digest
        return {"path": path, "mode": 0, "payload": None, "record": record}
    if digest != guard.enabled_sha256:
        raise CompatTransformError(
            f"unrecognised KubeJS gym-loot generator: {relative} has sha256 {digest}, which is "
            f"neither a reviewed generator with the write enabled nor a reviewed already-disabled "
            f"generator. {guard.transform_id} will not disable a write it does not recognise; "
            "review the new generator and extend the reviewed digests before staging."
        )
    if safe_version not in guard.versions:
        reviewed = ", ".join(guard.versions) or "<none>"
        raise CompatTransformError(
            f"{guard.transform_id} is bound to artifact version(s) {reviewed}, but "
            f"{safe_version} ships the reviewed generator with the write still enabled. Confirm "
            "the baked badge_reward tables and add the version to the reviewed transform before "
            "staging."
        )
    if payload.count(guard.statement.encode("utf-8")) != 1:
        raise CompatTransformError(
            f"KubeJS gym-loot write statement is not unique in {relative}; refusing an ambiguous "
            "replacement."
        )
    updated = payload.replace(guard.statement.encode("utf-8"), guard.replacement.encode("utf-8"), 1)
    if _sha256(updated) != guard.guarded_sha256:
        raise CompatTransformError(
            f"KubeJS gym-loot replacement does not produce the reviewed generator bytes for "
            f"{relative}."
        )
    record["action"] = "disabled_write"
    record["generator_sha256_after"] = guard.guarded_sha256
    try:
        mode = path.stat(follow_symlinks=False).st_mode & 0o777
    except OSError as exc:
        raise CompatTransformError(f"KubeJS gym-loot generator is unavailable: {relative}") from exc
    return {"path": path, "mode": mode, "payload": updated, "record": record}


def _write_plan(guard: GymLootGuard, plan: Mapping[str, Any]) -> dict[str, Any]:
    target: Path = plan["path"]
    payload = plan["payload"]
    record = plan["record"]
    if payload is None:
        return record
    temporary: str | None = None
    try:
        fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
        os.fchmod(fd, plan["mode"])
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        temporary = None
        dirfd = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
    except BaseException as exc:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
        if isinstance(exc, CompatTransformError):
            raise
        raise CompatTransformError(
            f"cannot apply {guard.transform_id} to {guard.path}: {exc}"
        ) from exc
    written = _sha256(_read_bounded(target, "KubeJS gym-loot generator", MAX_GENERATOR_BYTES))
    if written != guard.guarded_sha256:
        raise CompatTransformError(f"KubeJS gym-loot generator changed after transform: {guard.path}")
    record["generator_sha256_after"] = written
    return record


def apply_guard(root: Path, guard: GymLootGuard, version: str) -> dict[str, Any]:
    """Verify, then apply, one guard; a failed preflight changes nothing."""
    return _write_plan(guard, _plan(Path(root), guard, version))


def parse_policy_transforms(policy: Any) -> tuple[GymLootGuard, ...]:
    """Return the code-reviewed transforms a manifest policy is allowed to declare.

    The manifest entry is an assertion about reviewed work, never its source.
    Every declared field (target paths, reward set, version allowlist, digests
    and the exact statement/replacement) must match the reviewed record; the
    returned guard is that reviewed record.
    """
    if not isinstance(policy, Mapping):
        raise CompatTransformError("runtime policy is malformed")
    declared = policy.get("compatibility_transforms", ())
    if declared is None:
        declared = ()
    if isinstance(declared, (str, bytes)) or not isinstance(declared, (list, tuple)):
        raise CompatTransformError("runtime policy compatibility transforms are malformed")
    parsed = tuple(GymLootGuard.from_policy(value) for value in declared)
    if len({guard.transform_id for guard in parsed}) != len(parsed):
        raise CompatTransformError("runtime policy declares a transform twice")
    if len({guard.path for guard in parsed}) != len(parsed):
        raise CompatTransformError("runtime policy targets one path twice")
    registry = reviewed_guard_registry()
    guards: list[GymLootGuard] = []
    for guard in parsed:
        reviewed = registry.get(guard.transform_id)
        if reviewed is None:
            raise CompatTransformError(
                f"unsupported compatibility transform {guard.transform_id!r}; "
                f"reviewed transforms are {sorted(registry)}"
            )
        altered = _altered_fields(guard, reviewed)
        if altered:
            raise CompatTransformError(
                f"manifest declares an altered compatibility transform {guard.transform_id!r} "
                f"(differs from the code-reviewed record in {', '.join(altered)}); a manifest "
                "self-digest is integrity, not authority. Unknown variants require a code "
                "review before they may be declared."
            )
        guards.append(reviewed)
    return tuple(guards)


def apply_transforms(root: Path, guards: Sequence[GymLootGuard], version: str) -> list[dict[str, Any]]:
    """Apply validated guards in order, before any of them may write."""
    root = Path(root)
    plans = [(_plan(root, guard, version), guard) for guard in guards]
    return [_write_plan(guard, plan) for plan, guard in plans]


def apply_policy_transforms(root: Path, policy: Any, version: str) -> list[dict[str, Any]]:
    """Verify and apply every transform a manifest policy declares."""
    return apply_transforms(Path(root), parse_policy_transforms(policy), version)


__all__ = [
    "CompatTransformError",
    "GENERATOR_PATH",
    "GymLootGuard",
    "REWARD_KINDS",
    "REWARD_ROOT",
    "REVIEWED_DISABLED_SHA256",
    "REVIEWED_ENABLED_SHA256",
    "REVIEWED_GUARDED_SHA256",
    "REVIEWED_VERSIONS",
    "TRANSFORM_ID",
    "apply_guard",
    "apply_policy_transforms",
    "apply_transforms",
    "expected_reward_table",
    "parse_policy_transforms",
    "reviewed_compatibility_transforms",
    "reviewed_gym_loot_guard",
    "reviewed_guard_registry",
    "verify_reward_tables",
]
