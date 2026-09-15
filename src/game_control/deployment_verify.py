#!/usr/bin/env python3
"""Read-only deployment verifier for the dedicated Horizon VM stack.

The verifier deliberately has no credential-loading path.  It reads only the
fixed, non-secret deployment files listed below, opens SQLite databases with
``mode=ro``, and uses bounded local status probes.  It never starts, stops,
switches, updates, backs up, restores, clones, or writes a database.

Exit status:
  0  all checks passed
  1  one or more checks failed
  2  a protected check could not run without root
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import pwd
import grp
import re
import socket
import sqlite3
import stat
import subprocess
import tomllib
import urllib.error
import urllib.request
import uuid
from urllib.parse import urlsplit
from pathlib import Path
from typing import Any

from .deployment_manifest import (
    ABSENT_LIBEXEC_NAMES,
    COMPATIBILITY_LIBEXEC_NAMES,
    DeploymentManifest,
    get_manifest,
)


TARGET_ROOT = Path("/")
_DEPLOYMENT_MANIFEST = get_manifest()
ETC = TARGET_ROOT / "etc/game-control"
PROFILES = ETC / "profiles.d"
RUNNERS = ETC / "runner.d"
UNITS = TARGET_ROOT / "etc/systemd/system"
ROOT_CONFIG = ETC / "game-control.toml"
PUBLIC_ORIGIN_CONFIG = ETC / "public-origin.conf"
RUN = TARGET_ROOT / "run/game-control"
SOCKET = RUN / "control.sock"
SLOT_METADATA = TARGET_ROOT / "run/game-slot/slot.json"
STATE_DB = TARGET_ROOT / "var/lib/game-control/state.db"
WEB_DB = TARGET_ROOT / "var/lib/game-control-web/web.db"
WEB_SOURCE = TARGET_ROOT / "opt/game-control/src/game_control"
DEPLOYED_PYTHON = TARGET_ROOT / "opt/game-control/.venv/bin/python"
API_URL = os.environ.get("HORIZON_VERIFY_API_URL", "http://192.0.2.10:8444/api/v1/status")
PERF_API_URL = os.environ.get("HORIZON_VERIFY_PERF_API_URL", "http://192.0.2.10:8444/api/v1/perf")
AUTH_API_BASE_URL = os.environ.get("HORIZON_VERIFY_API_BASE_URL")
PROXY_CREDENTIAL = TARGET_ROOT / "run/credentials/game-control-web.service/proxy-token"
STATUS_P50_BUDGET_MS = 150.0

PROFILE_IDS = tuple(profile.id for profile in _DEPLOYMENT_MANIFEST.profiles)
EXPECTED_SERVICES = ("game-slotd.service", "game-control-web.service")
RETIRED_SERVICES = tuple(name for name in _DEPLOYMENT_MANIFEST.retired.names if name.endswith(".service"))
RELAY_STATE_EXPECTATIONS = {
    mode.name: dict(mode.expectations) for mode in _DEPLOYMENT_MANIFEST.relay_modes
}
PROFILE_UNITS = {profile.id: profile.unit for profile in _DEPLOYMENT_MANIFEST.profiles}
_SYSTEMD_NAMESPACE = next(ns for ns in _DEPLOYMENT_MANIFEST.namespaces if ns.name == "systemd")
_LIBEXEC_NAMESPACE = next(ns for ns in _DEPLOYMENT_MANIFEST.namespaces if ns.name == "libexec")
EXPECTED_UNIT_FILES = frozenset(_SYSTEMD_NAMESPACE.exact)
# Only names in these explicit Horizon namespaces are owned by this package.
# The systemd unit directory is shared with the host, so unrelated regular
# unit files (for example distro-provided dbus aliases) must not invalidate
# Horizon's package manifest.  Keep this policy narrow: a new Horizon-owned
# name must still be added to EXPECTED_UNIT_FILES before it can pass.
HORIZON_UNIT_NAMES = EXPECTED_UNIT_FILES
HORIZON_UNIT_PREFIXES = _SYSTEMD_NAMESPACE.prefixes
EXPECTED_PROFILE_FILES = frozenset(next(ns for ns in _DEPLOYMENT_MANIFEST.namespaces if ns.name == "profiles").exact)
EXPECTED_RUNNER_FILES = frozenset(next(ns for ns in _DEPLOYMENT_MANIFEST.namespaces if ns.name == "runners").exact)
EXPECTED_DIRECTORIES = {spec.target.removeprefix("/"): spec.mode for spec in _DEPLOYMENT_MANIFEST.directories}
EXPECTED_SYMLINKS = {spec.target.removeprefix("/"): spec.link_target for spec in _DEPLOYMENT_MANIFEST.symlinks}
LEGACY_TARGET_FILES = frozenset(_DEPLOYMENT_MANIFEST.retired.names)
LEGACY_STRINGS = ("crafty", "pzuser", "/opt/pzserver", "/home/pzuser")
EXPECTED_STATE_TABLES = {
    "events",
    "audit",
    "jobs",
    "confirmations",
    "backups",
    "backup_protections",
    "notification_deliveries",
    "notification_rules",
    "rpc_idempotency",
    "updates",
}
EXPECTED_APPEND_ONLY = {
    "audit_append_only_delete",
    "audit_append_only_update",
    "events_append_only_delete",
    "events_append_only_update",
}

SYSTEMD_ADAPTER_MARKERS = (
    "class SystemdAdapter",
    "create_subprocess_exec",
    "close_fds=True",
    '"/usr/bin/systemctl"',
    '"/usr/bin/journalctl"',
    "def observe",
    "def recent_logs",
)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


_HTTP_OPENER = urllib.request.build_opener(_NoRedirect())


def _open_http(request: urllib.request.Request, timeout: float):
    return _HTTP_OPENER.open(request, timeout=timeout)


def _source_python_modules() -> tuple[str, ...]:
    """Return package modules from the canonical manifest, in stable order."""
    modules = []
    for source in _DEPLOYMENT_MANIFEST.runtime_sources:
        if not source.startswith("src/game_control/") or not source.endswith(".py"):
            continue
        relative = source[len("src/"):-3]
        if relative.endswith("/__init__"):
            relative = relative[:-len("/__init__")]
        modules.append(relative.replace("/", "."))
    return tuple(sorted(set(modules)))


def _valid_endpoint_host(value: object) -> bool:
    """Validate a root-controlled deployment host without fixing one topology."""

    if not isinstance(value, str) or not value or len(value) > 253:
        return False
    if any(char.isspace() or ord(char) < 33 for char in value):
        return False
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        labels = value.removesuffix(".").split(".")
        return bool(
            len(labels) >= 2
            and all(
                label
                and len(label) <= 63
                and label[0].isalnum()
                and label[-1].isalnum()
                and all(char.isalnum() or char == "-" for char in label)
                for label in labels
            )
        )
    return not (address.is_unspecified or address.is_multicast)


def _valid_public_origin(value: object) -> bool:
    if not isinstance(value, str) or not value or value != value.strip() or any(ord(char) < 33 or char.isspace() for char in value):
        return False
    try:
        parsed = urlsplit(value)
        parsed.port
    except (TypeError, ValueError):
        return False
    return bool(
        parsed.scheme == "https"
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
        and not parsed.path
        and not parsed.query
        and not parsed.fragment
    )


def _unit_environment_host(unit: str, variable: str, *, ip_only: bool = False) -> str | None:
    try:
        text = (UNITS / unit).read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None
    matches = re.findall(rf"^Environment={re.escape(variable)}=([^\s]+)$", text, re.MULTILINE)
    if len(matches) != 1 or not _valid_endpoint_host(matches[0]):
        return None
    if ip_only:
        try:
            address = ipaddress.ip_address(matches[0])
        except ValueError:
            return None
        if address.version != 4 or address.is_loopback:
            return None
    return matches[0]


class Checks:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []
        self.missing_privilege = False

    def add(self, check_id: str, ok: bool, reason: str = "ok", **safe: Any) -> None:
        item: dict[str, Any] = {"id": check_id, "ok": bool(ok), "reason": reason}
        for key, value in safe.items():
            if key in {"count", "expected", "actual", "profiles", "state", "status", "mode"}:
                item[key] = value
        self.items.append(item)

    def unavailable(self, check_id: str, privilege: str = "root") -> None:
        self.missing_privilege = True
        self.add(check_id, False, "missing_privilege", privilege=privilege)

    @property
    def failed(self) -> bool:
        return any(not item["ok"] for item in self.items)


def _run(args: list[str], timeout: float = 3.0) -> subprocess.CompletedProcess[str] | None:
    """Run a fixed argv probe without exposing stdout/stderr to the terminal."""
    try:
        return subprocess.run(
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None


def _systemd_active(unit: str) -> bool | None:
    result = _run(["systemctl", "is-active", "--quiet", unit])
    return None if result is None else result.returncode == 0


def _systemd_properties(unit: str, properties: tuple[str, ...]) -> dict[str, str] | None:
    result = _run(
        ["systemctl", "show", "--no-pager", f"--property={','.join(properties)}", unit]
    )
    if result is None or result.returncode != 0:
        return None
    values: dict[str, str] = {}
    for line in result.stdout.splitlines():
        name, separator, value = line.partition("=")
        if separator and name in properties:
            values[name] = value
    return values


def _active_block_schedulers() -> tuple[str, ...] | None:
    """Read active scheduler names; return unavailable for incomplete inventory."""
    scheduler_root = TARGET_ROOT / "sys/block"
    try:
        paths = sorted(scheduler_root.glob("*/queue/scheduler"))
    except OSError:
        return None
    if not paths:
        return None
    active: list[str] = []
    for path in paths:
        try:
            value = path.read_text(encoding="ascii")
        except OSError:
            return None
        selected = next((part.strip("[]") for part in value.split() if part.startswith("[")), None)
        if selected is None:
            return None
        active.append(selected)
    return tuple(active)


def _check_block_schedulers(checks: Checks) -> None:
    schedulers = _active_block_schedulers()
    if schedulers is None:
        checks.unavailable("effective.block_schedulers")
    elif "none" in schedulers:
        checks.add(
            "effective.block_schedulers",
            True,
            "best_effort_no_ionice",
            actual=list(schedulers),
        )
    else:
        checks.add(
            "effective.block_schedulers",
            True,
            "best_effort_ionice_eligible",
            actual=list(schedulers),
        )


def _check_effective_controls(checks: Checks) -> None:
    """Report effective live systemd controls and distinguish probe gaps."""
    properties = (
        "CPUAccounting",
        "CPUWeight",
        "IOAccounting",
        "MemoryAccounting",
        "MemoryHigh",
        "MemoryMax",
        "MemorySwapMax",
        "Slice",
        "ControlGroup",
    )
    expected = {
        "games.slice": {
            "CPUAccounting": "yes",
            "CPUWeight": "200",
            "IOAccounting": "yes",
            "MemoryAccounting": "yes",
            "MemoryHigh": "9663676416",
            "MemoryMax": "10737418240",
            "MemorySwapMax": "0",
        },
        "minecraft-sunlit-cobblemon.service": {
            "CPUAccounting": "yes",
            "IOAccounting": "yes",
            "MemoryAccounting": "yes",
            "MemoryHigh": "8589934592",
            "MemoryMax": "9663676416",
            "MemorySwapMax": "0",
            "Slice": "games.slice",
        },
        # These services intentionally do not claim games.slice memory
        # controls; their accounting is explicit and their memory policy is
        # outside that slice boundary.
        "game-slotd.service": {
            "CPUAccounting": "yes",
            "IOAccounting": "yes",
            "MemoryAccounting": "yes",
            "Slice": "horizon.slice",
            "ControlGroup": "/horizon.slice/game-slotd.service",
        },
        "game-control-web.service": {
            "CPUAccounting": "yes",
            "IOAccounting": "yes",
            "MemoryAccounting": "yes",
            "Slice": "horizon.slice",
            "ControlGroup": "/horizon.slice/game-control-web.service",
        },
        "terraria-vanilla.service": {
            "CPUAccounting": "yes",
            "IOAccounting": "yes",
            "MemoryAccounting": "yes",
            "MemorySwapMax": "0",
            "Slice": "games.slice",
        },
        "terraria-tmod.service": {
            "CPUAccounting": "yes",
            "IOAccounting": "yes",
            "MemoryAccounting": "yes",
            "MemorySwapMax": "0",
            "Slice": "games.slice",
        },
    }
    for unit, required in expected.items():
        actual = _systemd_properties(unit, properties)
        if actual is None:
            checks.unavailable("effective." + unit.removesuffix(".service").replace(".", "_"))
            continue
        ok = all(actual.get(name) == value for name, value in required.items())
        checks.add(
            "effective." + unit.removesuffix(".service").replace(".", "_"),
            ok,
            "ok" if ok else "effective_control_gap",
            expected=required,
            actual={name: actual.get(name) for name in required},
        )


def _file_mode(path: Path) -> tuple[os.stat_result, int] | None:
    try:
        info = path.lstat()
    except OSError:
        return None
    return info, stat.S_IMODE(info.st_mode)


def _owner_mode(path: Path, uid: int, gid: int, mode: int, regular: bool = True) -> bool:
    value = _file_mode(path)
    if value is None:
        return False
    info, actual_mode = value
    is_type = stat.S_ISREG(info.st_mode) if regular else stat.S_ISSOCK(info.st_mode)
    return is_type and info.st_uid == uid and info.st_gid == gid and actual_mode == mode and info.st_nlink == 1


def _direct_regular_unit_names(directory: Path) -> set[str] | None:
    """List only direct regular unit files without following symlink aliases."""

    try:
        entries = directory.iterdir()
    except OSError:
        return None
    names: set[str] = set()
    try:
        for path in entries:
            if path.suffix not in {".service", ".slice", ".timer"}:
                continue
            if stat.S_ISREG(path.lstat().st_mode):
                names.add(path.name)
    except OSError:
        return None
    return names


def _direct_regular_names(directory: Path) -> set[str] | None:
    try:
        entries = directory.iterdir()
        names: set[str] = set()
        for path in entries:
            info = path.lstat()
            if stat.S_ISREG(info.st_mode):
                names.add(path.name)
        return names
    except OSError:
        return None


def _horizon_owned_unit_names(unit_names: set[str]) -> set[str]:
    """Select only regular unit names belonging to Horizon's namespaces."""

    return {
        name
        for name in unit_names
        if name in HORIZON_UNIT_NAMES or name.startswith(HORIZON_UNIT_PREFIXES)
    }


def _check_target_package(checks: Checks) -> None:
    """Validate the immutable VM package without probing live services."""

    for relative, expected_mode in EXPECTED_DIRECTORIES.items():
        path = TARGET_ROOT / relative
        value = _file_mode(path)
        expected_spec = next(spec for spec in _DEPLOYMENT_MANIFEST.directories if spec.target.removeprefix("/") == relative)
        if TARGET_ROOT != Path("/"):
            expected_uid = 0 if expected_spec.staged_owner == "root" else -1
            expected_gid = 0 if expected_spec.staged_group == "root" else -1
        else:
            try:
                expected_uid = pwd.getpwnam(expected_spec.owner).pw_uid
                expected_gid = grp.getgrnam(expected_spec.group).gr_gid
            except KeyError:
                expected_uid = expected_gid = -1
        try:
            actual_children = {
                child.name for child in path.iterdir()
                if stat.S_ISDIR(child.lstat().st_mode) or stat.S_ISREG(child.lstat().st_mode)
            }
        except OSError:
            actual_children = None
        declared_children = {
            child.target.rsplit("/", 1)[-1]
            for child in _DEPLOYMENT_MANIFEST.directories
            if Path(child.target).parent == Path(expected_spec.target)
        }
        declared_children.update(
            child.target.rsplit("/", 1)[-1]
            for child in _DEPLOYMENT_MANIFEST.files
            if Path(child.target).parent == Path(expected_spec.target)
        )
        declared_children.update(
            child.target.rsplit("/", 1)[-1]
            for child in _DEPLOYMENT_MANIFEST.symlinks
            if Path(child.target).parent == Path(expected_spec.target)
        )
        allowed_children = declared_children | set(expected_spec.allowed_children)
        ok = bool(
            value
            and stat.S_ISDIR(value[0].st_mode)
            and value[1] == expected_mode
            and value[0].st_uid == expected_uid
            and value[0].st_gid == expected_gid
            and actual_children is not None
            and (
                expected_spec.allow_unmanaged_children
                or actual_children <= allowed_children
            )
        )
        checks.add(
            "target.directory." + relative.replace("/", "."),
            ok,
            "ok" if ok else "directory_mode_or_presence_invalid",
            expected=oct(expected_mode),
        )
    file_problems: list[str] = []
    for spec in _DEPLOYMENT_MANIFEST.files:
        path = TARGET_ROOT / spec.target.lstrip("/")
        value = _file_mode(path)
        if value is None:
            file_problems.append(f"missing:{spec.target}")
            continue
        info, actual_mode = value
        if TARGET_ROOT != Path("/"):
            expected_uid = 0 if spec.staged_owner == "root" else -1
            expected_gid = 0 if spec.staged_group == "root" else -1
        else:
            try:
                expected_uid = pwd.getpwnam(spec.owner).pw_uid
                expected_gid = grp.getgrnam(spec.group).gr_gid
            except KeyError:
                file_problems.append(f"owner-unavailable:{spec.target}")
                continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            file_problems.append(f"type-or-links:{spec.target}")
        elif actual_mode != spec.mode or info.st_uid != expected_uid or info.st_gid != expected_gid:
            file_problems.append(f"metadata:{spec.target}")
    checks.add(
        "target.files.manifest",
        not file_problems,
        "ok" if not file_problems else "file_manifest_mismatch",
        expected=len(_DEPLOYMENT_MANIFEST.files),
        actual=None if not file_problems else file_problems[:10],
    )
    for relative, expected_target in EXPECTED_SYMLINKS.items():
        path = TARGET_ROOT / relative
        try:
            actual_target = os.readlink(path)
        except OSError:
            actual_target = None
        ok = path.is_symlink() and actual_target == expected_target
        checks.add(
            "target.symlink." + relative.replace("/", "."),
            ok,
            "ok" if ok else "symlink_target_invalid",
            expected=expected_target,
            actual=actual_target,
        )

    profile_names = {path.name for path in PROFILES.glob("*.toml")} if PROFILES.is_dir() else set()
    runner_names = {path.name for path in RUNNERS.glob("*.json")} if RUNNERS.is_dir() else set()
    unit_names = _direct_regular_unit_names(UNITS)
    owned_unit_names = None if unit_names is None else _horizon_owned_unit_names(unit_names)
    checks.add(
        "target.profiles.manifest",
        profile_names == EXPECTED_PROFILE_FILES,
        "ok" if profile_names == EXPECTED_PROFILE_FILES else "profile_manifest_mismatch",
        expected=sorted(EXPECTED_PROFILE_FILES),
        actual=sorted(profile_names),
    )
    checks.add(
        "target.runners.manifest",
        runner_names == EXPECTED_RUNNER_FILES,
        "ok" if runner_names == EXPECTED_RUNNER_FILES else "runner_manifest_mismatch",
        expected=sorted(EXPECTED_RUNNER_FILES),
        actual=sorted(runner_names),
    )
    checks.add(
        "target.units.manifest",
        owned_unit_names == EXPECTED_UNIT_FILES,
        "ok" if owned_unit_names == EXPECTED_UNIT_FILES else ("unit_manifest_unavailable" if owned_unit_names is None else "unit_manifest_mismatch"),
        expected=sorted(EXPECTED_UNIT_FILES),
        actual=None if owned_unit_names is None else sorted(owned_unit_names),
    )
    libexec_names = _direct_regular_names(TARGET_ROOT / _LIBEXEC_NAMESPACE.path.lstrip("/"))
    owned_libexec_names = (
        None
        if libexec_names is None
        else {name for name in libexec_names if name in _LIBEXEC_NAMESPACE.exact or name.startswith(_LIBEXEC_NAMESPACE.prefixes)}
    )
    allowed_libexec_names = set(_LIBEXEC_NAMESPACE.exact) | set(COMPATIBILITY_LIBEXEC_NAMES)
    libexec_ok = bool(
        libexec_names is not None
        and owned_libexec_names is not None
        and set(_LIBEXEC_NAMESPACE.exact) <= owned_libexec_names
        and owned_libexec_names <= allowed_libexec_names
        and set(libexec_names).isdisjoint(ABSENT_LIBEXEC_NAMES)
    )
    checks.add(
        "target.libexec.manifest",
        libexec_ok,
        "ok" if libexec_ok else "libexec_manifest_mismatch",
        expected=sorted(allowed_libexec_names),
        actual=None if owned_libexec_names is None else sorted(owned_libexec_names),
    )
    web_specs = tuple(spec for spec in _DEPLOYMENT_MANIFEST.files if spec.target.startswith("/opt/game-control/web/"))
    web_names = _direct_regular_names(TARGET_ROOT / "opt/game-control/web")
    checks.add(
        "target.web.manifest",
        web_names == {spec.target.rsplit("/", 1)[-1] for spec in web_specs},
        "ok" if web_names == {spec.target.rsplit("/", 1)[-1] for spec in web_specs} else "web_manifest_mismatch",
        expected=sorted(spec.target.rsplit("/", 1)[-1] for spec in web_specs),
        actual=None if web_names is None else sorted(web_names),
    )
    legacy_unit_names = unit_names if unit_names is not None else set()
    legacy = sorted(LEGACY_TARGET_FILES.intersection(profile_names | runner_names | legacy_unit_names))
    checks.add(
        "target.legacy_artifacts_absent",
        not legacy,
        "ok" if not legacy else "legacy_target_artifacts_present",
        actual=legacy,
    )

    try:
        root_config = ROOT_CONFIG.read_text(encoding="utf-8")
        forbidden = [marker for marker in (*LEGACY_STRINGS, "crafty-token", "relay_unit") if marker in root_config.lower()]
        checks.add("target.root_config", not forbidden, "ok" if not forbidden else "retired_dependency_present", actual=forbidden)
    except OSError:
        checks.add("target.root_config", False, "root_config_missing")

    for profile_id in PROFILE_IDS:
        profile_path = PROFILES / f"{profile_id}.toml"
        try:
            raw = tomllib.loads(profile_path.read_text(encoding="utf-8"))
            paths = raw.get("paths", {})
            serialized = json.dumps(raw).lower()
            no_retired = not any(marker in serialized for marker in LEGACY_STRINGS)
            systemd = raw.get("adapter") == "systemd" and raw.get("systemd_unit") == PROFILE_UNITS[profile_id]
            mutable_root = paths.get("mutable_root")
            install_root = paths.get("install_root")
            split_backup = (
                paths.get("data_roots") == (
                    [mutable_root, install_root, "/srv/game-servers/minecraft-sunlit-cobblemon-current"]
                    if profile_id == "minecraft-sunlit-cobblemon"
                    else [mutable_root, install_root]
                )
                and paths.get("backup_roots") == [mutable_root]
            )
            if profile_id == "minecraft-sunlit-cobblemon":
                split_roots = (
                    paths.get("install_root") == "/opt/game-servers/minecraft-sunlit-cobblemon/releases"
                    and paths.get("mutable_root") == "/srv/game-servers/minecraft-sunlit-cobblemon-state"
                )
                endpoint = raw.get("public_endpoint")
                fixed_relay = (
                    isinstance(endpoint, dict)
                    and set(endpoint) == {"host", "port", "protocol", "relay_unit"}
                    and _valid_endpoint_host(endpoint.get("host"))
                    and endpoint.get("port") == 25565
                    and endpoint.get("protocol") == "tcp"
                    and endpoint.get("relay_unit") == "bore-minecraft-fenced.service"
                )
                ports = raw.get("ports")
                fixed_backend = ports == [{"protocol": "tcp", "port": 25566, "required": True}]
                ok = systemd and no_retired and split_roots and split_backup and fixed_relay and fixed_backend
                reason = "ok" if ok else "sunlit_target_contract_invalid"
            else:
                ok = systemd and no_retired and split_backup
                reason = "ok" if ok else "retained_profile_contract_invalid"
            checks.add("target.profile." + profile_id, ok, reason)
        except (OSError, tomllib.TOMLDecodeError, TypeError, ValueError):
            checks.add("target.profile." + profile_id, False, "invalid_profile")

        runner_path = RUNNERS / f"{profile_id}.json"
        try:
            raw_runner = json.loads(runner_path.read_text(encoding="utf-8"))
            serialized = json.dumps(raw_runner).lower()
            no_retired = not any(marker in serialized for marker in LEGACY_STRINGS)
            expected_user = "svc-sunlit" if profile_id == "minecraft-sunlit-cobblemon" else ("terraria-vanilla" if profile_id == "terraria-vanilla" else "tmodloader")
            ok = raw_runner.get("user") == expected_user and raw_runner.get("group") == expected_user and no_retired
            if profile_id == "minecraft-sunlit-cobblemon":
                argv = raw_runner.get("argv")
                ok &= (
                    raw_runner.get("cwd") == "/srv/game-servers/minecraft-sunlit-cobblemon-current"
                    and isinstance(argv, list)
                    and argv[:3] == [
                        "/usr/bin/java",
                        "-Duser.home=/srv/game-servers/minecraft-sunlit-cobblemon-state/local",
                        "@/srv/game-servers/minecraft-sunlit-cobblemon-current/user_jvm_args.txt",
                    ]
                    and argv[3] == "@/opt/game-servers/minecraft-sunlit-cobblemon/libraries/net/minecraftforge/forge/1.20.1-47.4.0/unix_args.txt"
                    and argv[-1] == "nogui"
                    and raw_runner.get("environment") == {
                        "HOME": "/srv/game-servers/minecraft-sunlit-cobblemon-state/local"
                    }
                )
            checks.add("target.runner." + profile_id, ok, "ok" if ok else "runner_contract_invalid")
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            checks.add("target.runner." + profile_id, False, "invalid_runner")

    slice_path = UNITS / "games.slice"
    try:
        slice_text = slice_path.read_text(encoding="utf-8")
        required = (
            "CPUAccounting=yes",
            "CPUWeight=200",
            "IOAccounting=yes",
            "MemoryAccounting=yes",
            "MemoryHigh=9G",
            "MemoryMax=10G",
            "MemorySwapMax=0",
        )
        ok = all(item in slice_text for item in required)
        checks.add("target.games_slice", ok, "ok" if ok else "games_slice_limits_invalid")
    except OSError:
        checks.add("target.games_slice", False, "games_slice_missing")

    for unit_name in PROFILE_UNITS.values():
        path = UNITS / unit_name
        try:
            text = path.read_text(encoding="utf-8")
            required = (
                "Environment=GAME_SLOT_REQUIRE_RESERVATION=1",
                "ExecStart=/usr/local/libexec/game-slot-run ",
                "Restart=no",
                "Slice=games.slice",
                "CPUAccounting=yes",
                "MemoryAccounting=yes",
                "IOAccounting=yes",
                "MemorySwapMax=0",
            )
            ok = all(item in text for item in required) and "WantedBy=" not in text
            if unit_name == "minecraft-sunlit-cobblemon.service":
                ok &= all(item in text for item in (
                    "User=svc-sunlit",
                    "MemoryHigh=8G",
                    "MemoryMax=9G",
                    "OOMPolicy=stop",
                    "ExecStartPre=/usr/local/libexec/game-sunlit-prepare",
                    "LoadCredential=minecraft-rcon-password:/etc/game-control/secrets.d/minecraft-rcon-password",
                    "ExecStartPre=/usr/local/libexec/game-sunlit-rcon-prepare",
                    "ExecStop=-/usr/local/libexec/game-sunlit-stop minecraft-sunlit-cobblemon $MAINPID",
                    "ConditionPathExists=/usr/lib/jvm/java-17-openjdk-amd64/bin/java",
                    "Environment=JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64",
                    "WorkingDirectory=/srv/game-servers/minecraft-sunlit-cobblemon-current",
                    "ReadWritePaths=/run/game-control /run/game-slot /srv/game-servers/minecraft-sunlit-cobblemon-state",
                ))
            checks.add("target.unit." + unit_name.removesuffix(".service"), ok, "ok" if ok else "game_unit_contract_invalid")
        except OSError:
            checks.add("target.unit." + unit_name.removesuffix(".service"), False, "game_unit_missing")

    platform_files = {
        "ops/nftables/horizon.nft": (0o644, ("policy drop", "tcp dport 25575 drop", 'iifname "wg-hzn-terraria"')),
        "ops/lazymc/lazymc.toml": (
            0o644,
            (
                "address = \"0.0.0.0:25565\"",
                "address = \"127.0.0.1:25566\"",
                "command = \"/usr/local/libexec/horizon-lazymc-wake\"",
                "freeze_process = false",
                "wake_on_crash = false",
                "sleep_after = 4294967295",
                "rewrite_server_properties = false",
                "version = \"0.2.11\"",
            ),
        ),
        "ops/journald/horizon.conf": (0o644, ("Storage=persistent", "SystemMaxUse=1G", "MaxRetentionSec=14day")),
        "ops/journald/horizon-private-measurement.conf": (0o644, ("RateLimitIntervalSec=0", "RateLimitBurst=0")),
        "ops/bin/game-sunlit-prepare": (0o755, ("MUTABLE_ROOT", "TARGET")),
        "ops/bin/horizon-lazymc-wake": (0o755, ("run_wake_hook",)),
        "ops/bin/game-sunlit-rcon-prepare": (0o755, ("CREDENTIALS_DIRECTORY", "rcon.password")),
        "ops/bin/game-sunlit-stop": (0o755, ("STOP_MARKERS",)),
        "ops/bin/horizon-alert-notify": (0o755, ("ALERTMANAGER_URL", "TARGETS")),
        "ops/bin/horizon-bore-liveness": (
            0o755,
            (
                "bore-minecraft-fenced.service",
                "lazymc-minecraft.service",
                "FAILURE_WINDOW_SECONDS",
                "_local_client_sessions_present",
            ),
        ),
        "web/app.js": (0o644, ("const PROFILE_FALLBACK",)),
        "web/commands.js": (0o644, ("window.HORIZON_COMMANDS",)),
        "web/index.html": (0o644, ('<main id="main-content"', "/app.js")),
        "web/palette.js": (0o644, ("window.HORIZON_PALETTE",)),
        "web/styles.css": (0o644, (".active-slot",)),
    }
    platform_ok = True
    for source, (_declared_mode, markers) in platform_files.items():
        spec = next(spec for spec in _DEPLOYMENT_MANIFEST.files if spec.source == source)
        relative = spec.target.removeprefix("/")
        expected_mode = spec.mode
        path = TARGET_ROOT / relative
        value = _file_mode(path)
        try:
            contents = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            contents = ""
        platform_ok &= bool(
            value
            and stat.S_ISREG(value[0].st_mode)
            and value[1] == expected_mode
            and all(marker in contents for marker in markers)
        )
    control_units = {
        "game-slotd.service": (
            "OnFailure=horizon-alert-notify@controller.service",
            "LogNamespace=horizon",
            "Slice=horizon.slice",
            "CPUAccounting=yes",
            "MemoryAccounting=yes",
            "IOAccounting=yes",
        ),
        "game-control-web.service": (
            "OnFailure=horizon-alert-notify@web.service",
            "LogNamespace=horizon",
            "Slice=horizon.slice",
            "CPUAccounting=yes",
            "MemoryAccounting=yes",
            "IOAccounting=yes",
            "Environment=HorizonWebHost=",
            "EnvironmentFile=/etc/game-control/public-origin.conf",
        ),
        "lazymc-minecraft.service": (
            "User=svc-lazymc",
            "LoadCredential=wake-token:/etc/game-control/secrets.d/lazymc-waker.token",
            "ExecStart=/usr/local/bin/lazymc start",
            "Restart=no",
            "StartLimitBurst=1",
        ),
        "bore-minecraft-fenced.service": (
            "ConditionPathExists=/etc/game-control/arm/bore-minecraft",
            "Environment=BoreRemoteHost=",
            "/usr/local/bin/bore local 25565 --local-host 127.0.0.1 --to ${BoreRemoteHost} --port 25565",
            "Restart=always",
            "RestartSec=15",
            "StartLimitIntervalSec=0",
        ),
        "horizon-terraria-relay.service": (
            "ConditionPathExists=/etc/game-control/arm/horizon-terraria",
            "/etc/wireguard/wg-hzn-terraria.conf",
            "Restart=no",
        ),
        "horizon-alert-notify@.service": ("StartLimitBurst=3", "Restart=on-failure"),
        "horizon-alert-drill@.service": ("OnFailure=horizon-alert-notify@drill-%i.service", "Restart=no"),
        "horizon-bore-liveness.service": (
            "ExecStart=/usr/local/libexec/horizon-bore-liveness",
            "RuntimeDirectoryPreserve=yes",
            "TimeoutStartSec=60",
            "CapabilityBoundingSet=CAP_DAC_READ_SEARCH CAP_SYS_PTRACE",
        ),
        "horizon-bore-liveness.timer": (
            "OnBootSec=2min",
            "OnUnitActiveSec=30s",
            "WantedBy=timers.target",
        ),
    }
    for name, markers in control_units.items():
        try:
            contents = (UNITS / name).read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            contents = ""
        platform_ok &= all(marker in contents for marker in markers)
        if name in {"bore-minecraft-fenced.service", "horizon-terraria-relay.service"}:
            platform_ok &= "[Install]" not in contents
    platform_ok &= _unit_environment_host(
        "game-control-web.service", "HorizonWebHost", ip_only=True
    ) is not None
    platform_ok &= _unit_environment_host(
        "bore-minecraft-fenced.service", "BoreRemoteHost"
    ) is not None
    checks.add(
        "target.platform_controls",
        bool(platform_ok),
        "ok" if platform_ok else "platform_control_contract_invalid",
    )


def _gid(name: str) -> int | None:
    try:
        return grp.getgrnam(name).gr_gid
    except KeyError:
        return None


def _parse_ss() -> set[tuple[str, int, str]]:
    result = _run(["ss", "-H", "-ltnup"], timeout=5.0)
    if result is None:
        return set()
    listeners: set[tuple[str, int, str]] = set()
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) < 5:
            continue
        protocol = fields[0].lower()
        if protocol not in {"tcp", "udp"}:
            continue
        local = fields[4]
        match = re.search(r":(\d+)$", local)
        if match:
            listeners.add((protocol, int(match.group(1)), local.rsplit(":", 1)[0]))
    return listeners


def _check_services(checks: Checks) -> None:
    for unit in EXPECTED_SERVICES:
        active = _systemd_active(unit)
        if active is None:
            checks.add("service." + unit.removesuffix(".service"), False, "status_probe_unavailable")
        else:
            checks.add("service." + unit.removesuffix(".service"), active, "active_required" if not active else "ok")
    for unit in RETIRED_SERVICES:
        active = _systemd_active(unit)
        checks.add(
            "service.retired." + unit.removesuffix(".service"),
            active is False,
            "ok" if active is False else "status_probe_unavailable" if active is None else "retired_service_active",
        )


def _check_relay_state(checks: Checks, expected: str) -> None:
    expectations = RELAY_STATE_EXPECTATIONS[expected]
    for unit, require_active in expectations.items():
        active = _systemd_active(unit)
        ok = active is require_active
        checks.add(
            "relay." + unit.removesuffix(".service"),
            ok,
            "ok"
            if ok
            else "status_probe_unavailable"
            if active is None
            else "active_required"
            if require_active
            else "inactive_required",
            expected="active" if require_active else "inactive",
            actual="unknown" if active is None else "active" if active else "inactive",
        )


def _listener_profiles(profiles: dict[str, dict[str, Any]], protocol: str, port: int) -> tuple[str, ...]:
    matched: list[str] = []
    for profile_id, raw in profiles.items():
        ports = raw.get("ports", [])
        if any(
            isinstance(spec, dict)
            and spec.get("protocol") == protocol
            and spec.get("port") == port
            for spec in ports if isinstance(ports, list)
        ):
            matched.append(profile_id)
    return tuple(sorted(matched))


def _check_listeners(
    checks: Checks,
    profiles: dict[str, dict[str, Any]],
    owner: str | None,
) -> set[tuple[str, int, str]]:
    listeners = _parse_ss()
    web = {(p, n, host) for p, n, host in listeners if p == "tcp" and n == 8444}
    expected_web_host = _unit_environment_host(
        "game-control-web.service", "HorizonWebHost", ip_only=True
    )
    ok = expected_web_host is not None and any(
        host in {expected_web_host, expected_web_host + "%"} for _, _, host in web
    )
    checks.add("listener.private_web", ok, "listener_missing" if not ok else "ok", count=len(web))
    for protocol, port, label in (("tcp", 25566, "sunlit-backend"), ("tcp", 7777, "terraria")):
        matching = {host for p, n, host in listeners if p == protocol and n == port}
        count = len(matching)
        profile_ids = _listener_profiles(profiles, protocol, port)
        active_owner = owner is not None and owner in profile_ids
        listener_ok = matching == {"127.0.0.1"} if label == "sunlit-backend" else bool(matching)
        check_id = (
            "listener.active." + label + "." + str(port)
            if active_owner
            else "listener.stopped." + label + "." + str(port)
        )
        checks.add(
            check_id,
            listener_ok if active_owner else count == 0,
            "active_owner_listening" if active_owner and listener_ok else
            "active_owner_listener_invalid" if active_owner and count else
            "active_owner_listener_missing" if active_owner else
            "must_be_stopped" if count else "ok",
            count=count,
            profiles=profile_ids,
        )
    proxy_active = _systemd_active("lazymc-minecraft.service")
    public_proxy_listeners = {
        local
        for protocol, port, local in listeners
        if protocol == "tcp" and port == 25565
    }
    public_proxy_hosts = {"0.0.0.0", "*", "[::]", "::"}
    proxy_count = len(public_proxy_listeners)
    exact_public_proxy = proxy_count == 1 and public_proxy_listeners <= public_proxy_hosts
    checks.add(
        "listener.lazymc.public.25565",
        exact_public_proxy if proxy_active else proxy_count == 0,
        "public_proxy_listening" if proxy_active and exact_public_proxy else
        "public_proxy_listener_ownership_invalid" if proxy_active else
        "public_proxy_listener_missing" if proxy_active else
        "must_be_stopped" if proxy_count else "ok",
        count=proxy_count,
        listeners=sorted(public_proxy_listeners),
    )
    for protocol, port, label in (
        ("tcp", 8443, "crafty"),
        ("udp", 16261, "pz"),
        ("udp", 16262, "pz"),
    ):
        count = sum(1 for p, n, _ in listeners if p == protocol and n == port)
        checks.add(
            "listener.retired." + label + "." + str(port),
            count == 0,
            "ok" if count == 0 else "retired_listener_present",
            count=count,
        )
    return listeners


def _check_runtime_files(checks: Checks) -> None:
    gamecontrol_gid = _gid("gamecontrol")
    gameslot_gid = _gid("gameslot")
    if gamecontrol_gid is None or gameslot_gid is None:
        checks.unavailable("runtime.lock_group_ids")
        return
    ok = _owner_mode(SOCKET, 0, gamecontrol_gid, 0o660, regular=False)
    checks.add("runtime.control_socket", ok, "ok" if ok else "secure_socket_required")
    for name in ("operation.lock", "slot.lock"):
        path = RUN / name
        ok = _owner_mode(path, 0, gameslot_gid, 0o660)
        checks.add("runtime." + name, ok, "ok" if ok else "secure_lock_required")
    value = _file_mode(RUN)
    ok = bool(value and stat.S_ISDIR(value[0].st_mode) and value[0].st_uid == 0 and value[1] == 0o755)
    checks.add(
        "runtime.directory",
        ok,
        "ok" if ok else "root_0755_required",
    )
    reservation = RUN / "reservation.json"
    if reservation.exists():
        ok = _owner_mode(reservation, 0, 0, 0o644)
        checks.add("runtime.reservation", ok, "ok" if ok else "secure_reservation_required")
    else:
        checks.add("runtime.reservation", True, "absent")


def _check_service_account_access(checks: Checks) -> None:
    """Verify the account that launches Sunlit can traverse managed JVM args."""
    if TARGET_ROOT != Path("/"):
        return
    try:
        pwd.getpwnam("svc-sunlit")
    except KeyError:
        checks.unavailable("runtime.jvm_account_access")
        return
    result = _run(["runuser", "--user", "svc-sunlit", "--", "test", "-x", "/etc/game-control/jvm"])
    ok = result is not None and result.returncode == 0
    checks.add(
        "runtime.jvm_account_access",
        ok,
        "ok" if ok else "svc_sunlit_cannot_traverse_jvm",
    )


def _read_public_origin_config() -> str | None:
    fd = None
    try:
        fd = os.open(PUBLIC_ORIGIN_CONFIG, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
        info = os.fstat(fd)
        if not (stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o600 and info.st_size <= 4096):
            return None
        raw = os.read(fd, 4097).decode("ascii")
    except (OSError, UnicodeError):
        return None
    finally:
        if fd is not None:
            os.close(fd)
    lines = [line for line in raw.splitlines() if line and not line.startswith("#")]
    value = lines[0].partition("=")[2] if len(lines) == 1 and lines[0].startswith("HORIZON_PUBLIC_ORIGIN=") else None
    return value if _valid_public_origin(value) else None


def _check_public_origin_config(checks: Checks) -> None:
    """Require the root-controlled private origin overlay used by the web unit."""
    if TARGET_ROOT != Path("/"):
        return
    value = _read_public_origin_config()
    ok = _valid_public_origin(value)
    checks.add("target.public_origin", ok, "ok" if ok else "public_origin_config_invalid")


def _public_origin() -> str | None:
    """Read the validated origin overlay without exposing its contents."""
    return _read_public_origin_config()


def _check_installed_package_mirror(checks: Checks) -> None:
    """Compare bytes imported by the target venv with the source projection.

    Reading source files alone cannot detect a stale wheel.  The subprocess has
    PYTHONPATH removed and imports through the target interpreter, then reports
    only module names and hashes.
    """
    if TARGET_ROOT != Path("/"):
        checks.add("package.installed_mirror", True, "staged_root_not_applicable")
        return
    modules = _source_python_modules()
    if not modules:
        checks.add("package.installed_mirror", False, "manifest_module_inventory_empty")
        return
    probe = (
        "import hashlib, importlib, json\n"
        f"mods = {modules!r}\n"
        "out = {}\n"
        "for name in mods:\n"
        "    m = importlib.import_module(name)\n"
        "    p = getattr(m, '__file__', None)\n"
        "    if not p or not p.endswith('.py'):\n"
        "        raise RuntimeError(name)\n"
        "    with open(p, 'rb') as stream: out[name] = hashlib.sha256(stream.read()).hexdigest()\n"
        "print(json.dumps(out, sort_keys=True, separators=(',', ':')))\n"
    )
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    try:
        result = subprocess.run(
            [str(DEPLOYED_PYTHON), "-I", "-c", probe], stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            timeout=15, check=False, env=env,
        )
        imported = json.loads(result.stdout) if result.returncode == 0 else None
        if not isinstance(imported, dict):
            raise ValueError("probe")
        mismatches = []
        for module in modules:
            relative = module.removeprefix("game_control").lstrip(".").replace(".", "/")
            source = WEB_SOURCE / (relative + ".py" if relative else "__init__.py")
            if not source.is_file() and relative:
                source = WEB_SOURCE / relative / "__init__.py"
            try:
                expected = hashlib.sha256(source.read_bytes()).hexdigest()
            except OSError:
                expected = None
            if expected is None or imported.get(module) != expected:
                mismatches.append(module)
        checks.add("package.installed_mirror", not mismatches, "ok" if not mismatches else "installed_source_mismatch", actual=mismatches[:10])
    except (OSError, subprocess.SubprocessError, TypeError, ValueError, json.JSONDecodeError):
        checks.add("package.installed_mirror", False, "installed_import_probe_unavailable")


def _check_authenticated_origin_probe(checks: Checks) -> None:
    """Opt-in, non-lifecycle probe for origin enforcement and stale cookies."""
    credential = _load_proxy_credential()
    origin = _public_origin()
    base = AUTH_API_BASE_URL or API_URL.rsplit("/api/v1/", 1)[0]
    if credential is None or origin is None:
        checks.add("web.authenticated_origin_probe", False, "probe_configuration_unavailable")
        return

    def request(path: str, *, headers: dict[str, str] | None = None, body: bytes | None = None):
        request_obj = urllib.request.Request(
            base + path, headers={"X-Game-Control-Proxy": credential, "X-authentik-username": "verify-deployed", **(headers or {})}, data=body,
        )
        try:
            return _open_http(request_obj, timeout=5)
        except urllib.error.HTTPError as exc:
            return exc

    try:
        with request("/api/v1/session") as response:
            bootstrap_status = int(response.status)
            raw_payload = response.read(65537)
            if len(raw_payload) > 65536:
                raise ValueError("response too large")
            payload = json.loads(raw_payload.decode("utf-8"))
            cookie = response.headers.get("Set-Cookie", "").split(";", 1)[0]
        csrf = payload.get("csrf_token") if isinstance(payload, dict) else None
        if bootstrap_status != 200 or not cookie.startswith("game_control_session=") or not isinstance(csrf, str):
            raise ValueError("bootstrap")
        common = {"Cookie": cookie, "X-CSRF-Token": csrf, "Origin": origin, "Content-Type": "application/json", "Idempotency-Key": str(uuid.uuid4())}
        with request("/api/v1/profiles/minecraft-sunlit-cobblemon/start", headers=common, body=b'{"unexpected_acceptance_field":true}') as response:
            correct_status = int(response.status)
        wrong = dict(common, Origin="https://wrong-origin.invalid", **{"Idempotency-Key": str(uuid.uuid4())})
        with request("/api/v1/profiles/minecraft-sunlit-cobblemon/start", headers=wrong, body=b'{"unexpected_acceptance_field":true}') as response:
            wrong_status = int(response.status)
        with request("/api/v1/session", headers={"Cookie": "game_control_session=invalid-acceptance-session"}) as response:
            stale_status = int(response.status)
            cleared = "Max-Age=0" in response.headers.get("Set-Cookie", "")
        with request("/api/v1/session") as response:
            fresh_status = int(response.status)
            fresh_cookie = response.headers.get("Set-Cookie", "").split(";", 1)[0]
        passed = correct_status == 422 and wrong_status == 403 and stale_status == 401 and cleared and fresh_status == 200 and fresh_cookie.startswith("game_control_session=")
        checks.add("web.authenticated_origin_probe", passed, "ok" if passed else "acceptance_boundary_invalid", status={"correct_origin": correct_status, "wrong_origin": wrong_status, "stale_cookie": stale_status, "fresh_bootstrap": fresh_status})
    except (OSError, urllib.error.URLError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
        checks.add("web.authenticated_origin_probe", False, "probe_request_failed")


def _load_profiles(checks: Checks) -> dict[str, dict[str, Any]]:
    loaded: dict[str, dict[str, Any]] = {}
    try:
        files = sorted(PROFILES.glob("*.toml"))
    except OSError:
        checks.unavailable("profiles.read")
        return loaded
    for path in files:
        try:
            with path.open("rb") as stream:
                raw = tomllib.load(stream)
        except (OSError, tomllib.TOMLDecodeError):
            checks.add("profile." + path.stem, False, "invalid_profile")
            continue
        profile_id = raw.get("id")
        if isinstance(profile_id, str):
            loaded[profile_id] = raw
    ok = len(loaded) == len(PROFILE_IDS)
    checks.add("profiles.count", ok, "ok" if ok else "expected_three_profiles", count=len(loaded), profiles=sorted(loaded))
    ok = set(loaded) == set(PROFILE_IDS)
    checks.add("profiles.ids", ok, "ok" if ok else "profile_set_mismatch")
    return loaded


def _check_target_registry(checks: Checks, profiles: dict[str, dict[str, Any]]) -> None:
    expected_units = set(PROFILE_UNITS.values())
    actual_units = {
        raw.get("systemd_unit")
        for profile_id, raw in profiles.items()
        if profile_id in PROFILE_IDS and isinstance(raw, dict)
    }
    ok = (
        set(profiles) == set(PROFILE_IDS)
        and actual_units == expected_units
        and all(raw.get("adapter") == "systemd" for raw in profiles.values())
    )
    checks.add("profiles.target_registry", ok, "ok" if ok else "target_registry_contract_invalid")
    no_legacy = not any(marker in json.dumps(profiles).lower() for marker in LEGACY_STRINGS)
    checks.add(
        "profiles.retired_dependencies_absent",
        no_legacy,
        "ok" if no_legacy else "retired_dependency_present",
    )

    expected_runner = {
        "user": "svc-sunlit",
        "group": "svc-sunlit",
        "cwd": "/srv/game-servers/minecraft-sunlit-cobblemon-current",
        "argv": [
            "/usr/bin/java",
            "-Duser.home=/srv/game-servers/minecraft-sunlit-cobblemon-state/local",
            "@/srv/game-servers/minecraft-sunlit-cobblemon-current/user_jvm_args.txt",
            "@/opt/game-servers/minecraft-sunlit-cobblemon/libraries/net/minecraftforge/forge/1.20.1-47.4.0/unix_args.txt",
            "nogui",
        ],
        "environment": {
            "HOME": "/srv/game-servers/minecraft-sunlit-cobblemon-state/local",
        },
    }
    try:
        runner = json.loads((RUNNERS / "minecraft-sunlit-cobblemon.json").read_text(encoding="utf-8"))
        link = TARGET_ROOT / "srv/game-servers/minecraft-sunlit-cobblemon-current"
        release_root = TARGET_ROOT / "opt/game-servers/minecraft-sunlit-cobblemon/releases"
        state = TARGET_ROOT / "srv/game-servers/minecraft-sunlit-cobblemon-state"
        raw_target = os.readlink(link)
        version = Path(raw_target).name
        expected_target = f"../../opt/game-servers/minecraft-sunlit-cobblemon/releases/{version}"
        release = (link.parent / raw_target).resolve(strict=True)
        link_ok = (
            link.is_symlink()
            and raw_target == expected_target
            and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,126}", version) is not None
            and release.parent == release_root.resolve(strict=True)
        )
        layout_ok = (
            release.is_dir()
            and not release.is_symlink()
            and state.is_dir()
            and not state.is_symlink()
            and stat.S_IMODE(release.stat().st_mode) == 0o755
            and stat.S_IMODE(state.stat().st_mode) == 0o750
        )
        launch_ok = runner == expected_runner and link_ok and layout_ok
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        launch_ok = False
    checks.add("profile.sunlit.launch_layout", launch_ok, "ok" if launch_ok else "sunlit_launch_layout_invalid")


def _proc_start_ticks(pid: int) -> int | None:
    try:
        _, rest = Path(f"/proc/{pid}/stat").read_text(encoding="ascii").split(") ", 1)
        return int(rest.split()[19])
    except (OSError, ValueError, IndexError):
        return None


def _controller_owned_profile() -> str | None:
    """Return the live profile recorded by the controller-owned slot runner."""

    if _systemd_active("game-slotd.service") is not True:
        return None
    try:
        raw = json.loads(SLOT_METADATA.read_text(encoding="utf-8"))
        profile = raw["profile_id"]
        pid = raw["pid"]
        ticks = raw["proc_start_ticks"]
        if profile not in PROFILE_IDS or not isinstance(pid, int) or pid <= 0 or not isinstance(ticks, int):
            return None
        if _proc_start_ticks(pid) != ticks:
            return None
        return profile
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _check_profile_status(checks: Checks, profiles: dict[str, dict[str, Any]], listeners: set[tuple[str, int, str]]) -> None:
    active: dict[str, bool | None] = {}
    for profile_id in PROFILE_IDS:
        raw = profiles.get(profile_id)
        if raw is None:
            continue
        unit = PROFILE_UNITS.get(profile_id)
        if unit:
            active_state = _systemd_active(unit)
            active[profile_id] = active_state
        else:
            port_open = any(protocol == "tcp" and port == 25565 for protocol, port, _ in listeners)
            active[profile_id] = port_open
    active_ids = [profile_id for profile_id, value in active.items() if value is True]
    owner = _controller_owned_profile()
    ownership_ok = len(active_ids) == 0 or (len(active_ids) == 1 and owner == active_ids[0])
    ownership_reason = (
        "ok" if ownership_ok and not active_ids else
        "controller_owned_active" if ownership_ok else
        "multiple_active_profiles" if len(active_ids) > 1 else
        "active_profile_not_controller_owned"
    )
    checks.add(
        "slot.active_profile_count",
        ownership_ok,
        ownership_reason,
        count=len(active_ids),
        actual=active_ids,
    )
    for profile_id, value in active.items():
        raw = profiles.get(profile_id)
        if raw is None:
            continue
        if value is None:
            state = "unknown"
            checks.add("profile." + profile_id + ".status", False, "status_probe_unavailable", state=state)
        else:
            state = "running" if value else "stopped"
            allowed = not value or ownership_ok and owner == profile_id
            checks.add(
                "profile." + profile_id + ".status",
                allowed,
                "ok" if allowed else ownership_reason,
                state=state,
            )
        ports = raw.get("ports", [])
        valid_ports = (
            isinstance(ports, list)
            and all(isinstance(item, dict) and isinstance(item.get("port"), int) for item in ports)
            and raw.get("adapter") == "systemd"
            and raw.get("systemd_unit") == PROFILE_UNITS.get(profile_id)
            and not any(marker in json.dumps(raw).lower() for marker in LEGACY_STRINGS)
        )
        checks.add("profile." + profile_id + ".schema", valid_ports, "ports_schema_invalid" if not valid_ports else "ok")


def _check_private_auth(checks: Checks) -> None:
    request = urllib.request.Request(API_URL, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
    except (OSError, urllib.error.URLError):
        checks.add("web.private_auth", False, "private_endpoint_unreachable")
        return
    ok = status in {401, 403}
    checks.add("web.private_auth", ok, "ok" if ok else "unauthenticated_request_not_rejected", status=status)


def _load_proxy_credential() -> str | None:
    try:
        value = PROXY_CREDENTIAL.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not value or any(char in value for char in "\x00\r\n"):
        return None
    return value


def _check_status_perf(checks: Checks) -> None:
    credential = _load_proxy_credential()
    if credential is None:
        checks.add("perf.status_p50_ms", False, "perf_credential_unavailable")
        return
    request = urllib.request.Request(
        PERF_API_URL,
        headers={
            "X-Game-Control-Proxy": credential,
            "X-authentik-username": "verify-deployed",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        checks.add("perf.status_p50_ms", False, "perf_request_http_error", status=int(exc.code))
        return
    except (OSError, urllib.error.URLError, UnicodeError, ValueError, TypeError):
        checks.add("perf.status_p50_ms", False, "perf_response_unavailable")
        return
    ring = payload.get("GET /api/v1/status") if isinstance(payload, dict) else None
    p50 = ring.get("p50_ms") if isinstance(ring, dict) else None
    if isinstance(p50, bool) or not isinstance(p50, (int, float)):
        checks.add("perf.status_p50_ms", False, "p50_unavailable")
        return
    ok = float(p50) < STATUS_P50_BUDGET_MS
    checks.add(
        "perf.status_p50_ms",
        ok,
        "ok" if ok else "budget_exceeded",
        expected=STATUS_P50_BUDGET_MS,
        actual=float(p50),
    )


def _check_systemd_adapter_contract(checks: Checks) -> None:
    """Confirm the deployed adapter keeps fixed-argv, bounded systemd I/O."""

    try:
        text = (WEB_SOURCE / "adapters/systemd.py").read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        checks.unavailable("adapter.systemd.contract")
        return
    ok = all(marker in text for marker in SYSTEMD_ADAPTER_MARKERS)
    checks.add(
        "adapter.systemd.contract",
        ok,
        "ok" if ok else "systemd_adapter_contract_missing",
    )


def _check_peer_rejection(checks: Checks) -> None:
    if os.geteuid() != 0:
        checks.unavailable("rpc.peer_rejection")
        return
    try:
        nobody = pwd.getpwnam("nobody")
    except KeyError:
        checks.add("rpc.peer_rejection", False, "nobody_account_unavailable")
        return
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        outcome = b"transport_denied"
        try:
            os.setgroups([])
            os.setgid(nobody.pw_gid)
            os.setuid(nobody.pw_uid)
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.settimeout(2)
            client.connect(str(SOCKET))
            client.sendall(b"{}\n")
            data = client.recv(4096)
            outcome = b"unauthorized_peer" if b"unauthorized_peer" in data else b"unexpected_accept"
            client.close()
        except PermissionError:
            pass
        except (OSError, ValueError):
            outcome = b"probe_error"
        try:
            os.write(write_fd, outcome)
        finally:
            os.close(write_fd)
            os._exit(0)
    os.close(write_fd)
    try:
        outcome = os.read(read_fd, 64).decode("ascii", "replace")
        os.waitpid(pid, 0)
    finally:
        os.close(read_fd)
    checks.add("rpc.peer_rejection", outcome in {"unauthorized_peer", "transport_denied"}, "unauthorized_peer_accepted" if outcome == "unexpected_accept" else ("peer_probe_error" if outcome == "probe_error" else "ok"))


def _check_schema_and_redaction(checks: Checks) -> None:
    expected = {
        "api.py": ("ROUTE_ACTIONS", "GET /api/v1/status", "POST /api/v1/switch/prepare", "StrictBody"),
        "protocol.py": ("MAX_REQUEST_BYTES", "ConfigDict(extra=\"forbid\"", "UNAUTHORIZED_PEER", "RpcRequest", "RpcResponse"),
        "auth.py": ("authenticate_proxy", "verify_csrf", "PROXY_CREDENTIAL_PATH"),
        "slotd_main.py": ("SO_PEERCRED", "authorize_peer", "UNAUTHORIZED_PEER"),
        "redaction.py": ("class Redactor", "[REDACTED]", "Bearer"),
    }
    for filename, markers in expected.items():
        path = WEB_SOURCE / filename
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            checks.unavailable("schema." + filename)
            continue
        ok = all(marker in text for marker in markers)
        checks.add("schema." + filename, ok, "ok" if ok else "schema_marker_missing")
    try:
        probe_code = (
            f"import sys;sys.path.insert(0,{str(WEB_SOURCE.parent)!r});"
            "from game_control.redaction import Redactor;"
            "v=Redactor().redact('Bearer synthetic-token-12345678 password=synthetic');"
            "print('ok' if 'synthetic-token-12345678' not in v and 'synthetic' not in v else 'fail')"
        )
        result = _run([str(DEPLOYED_PYTHON), "-c", probe_code])
        ok = result is not None and result.returncode == 0 and result.stdout.strip() == "ok"
        checks.add("redaction.synthetic", ok, "ok" if ok else "redaction_failed")
    except Exception:
        checks.add("redaction.synthetic", False, "redactor_unavailable")


def _check_database(checks: Checks, path: Path, expected_tables: set[str], trigger_names: set[str] | None, label: str, uid: int, gid: int) -> None:
    ownership_ok = _owner_mode(path, uid, gid, 0o600)
    ownership_reason = "ok" if ownership_ok else ("root_0600_required" if label == "state" else "web_owner_0600_required")
    checks.add("db." + label + ".ownership", ownership_ok, ownership_reason)
    try:
        db = sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True)
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        triggers = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        if label == "state":
            sql = {row[0]: row[1] or "" for row in db.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger'")}
            append_only = trigger_names is not None and trigger_names <= triggers and all("RAISE" in sql.get(name, "").upper() for name in trigger_names)
            checks.add("db.state.append_only", append_only, "ok" if append_only else "append_only_triggers_missing")
        schema_ok = expected_tables <= tables
        checks.add("db." + label + ".schema", schema_ok, "ok" if schema_ok else "schema_missing", count=len(tables))
        checks.add("db." + label + ".integrity", integrity, "ok" if integrity else "integrity_failed")
        db.close()
    except (OSError, sqlite3.Error):
        checks.unavailable("db." + label + ".read")


def _check_db_separation(checks: Checks) -> None:
    try:
        state = sqlite3.connect("file:" + str(STATE_DB) + "?mode=ro", uri=True)
        web = sqlite3.connect("file:" + str(WEB_DB) + "?mode=ro", uri=True)
        state_tables = {row[0] for row in state.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        web_tables = {row[0] for row in web.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        state.close()
        web.close()
        ok = not state_tables.intersection(web_tables)
        checks.add("db.schema_separation", ok, "ok" if ok else "database_tables_overlap")
    except (OSError, sqlite3.Error):
        checks.unavailable("db.schema_separation")


def _check_manifest(checks: Checks, manifest: Path | None) -> None:
    """Validate, but never execute, a future root-only mutation manifest."""
    if manifest is None:
        checks.add("mutation.manifest", True, "unused")
        return
    if os.geteuid() != 0:
        checks.unavailable("mutation.manifest")
        return
    value = _file_mode(manifest)
    secure = bool(value and value[0].st_uid == 0 and value[1] == 0o600 and stat.S_ISREG(value[0].st_mode))
    if not secure:
        checks.add("mutation.manifest", False, "manifest_must_be_root_0600")
        return
    try:
        with manifest.open("rb") as stream:
            payload = json.load(stream)
        allowed = payload.get("allow_mutation") is True and isinstance(payload.get("operations"), list)
        checks.add("mutation.manifest", allowed and not payload["operations"], "manifest_not_empty_or_explicit" if not allowed or payload["operations"] else "validated_not_executed")
    except (OSError, ValueError, TypeError):
        checks.add("mutation.manifest", False, "manifest_invalid")


def _set_root(root: Path) -> None:
    global TARGET_ROOT, ETC, PROFILES, RUNNERS, UNITS, ROOT_CONFIG, PUBLIC_ORIGIN_CONFIG, RUN, SOCKET
    global SLOT_METADATA, STATE_DB, WEB_DB, WEB_SOURCE, DEPLOYED_PYTHON, PROXY_CREDENTIAL
    TARGET_ROOT = root
    ETC = root / "etc/game-control"
    PROFILES = ETC / "profiles.d"
    RUNNERS = ETC / "runner.d"
    UNITS = root / "etc/systemd/system"
    ROOT_CONFIG = ETC / "game-control.toml"
    PUBLIC_ORIGIN_CONFIG = ETC / "public-origin.conf"
    RUN = root / "run/game-control"
    SOCKET = RUN / "control.sock"
    SLOT_METADATA = root / "run/game-slot/slot.json"
    STATE_DB = root / "var/lib/game-control/state.db"
    WEB_DB = root / "var/lib/game-control-web/web.db"
    WEB_SOURCE = root / "opt/game-control/src/game_control"
    DEPLOYED_PYTHON = root / "opt/game-control/.venv/bin/python"
    PROXY_CREDENTIAL = root / "run/credentials/game-control-web.service/proxy-token"


def verify_static(target_root: Path, manifest: DeploymentManifest) -> int:
    """Run static verification with an explicitly trusted package manifest."""
    if manifest is not _DEPLOYMENT_MANIFEST:
        raise ValueError("static verification requires the package manifest")
    _set_root(Path(target_root))
    return main(["--static"])


def verify_live(live_context: object | None = None) -> int:
    """Run the read-only live verifier; context is reserved for test fakes."""
    del live_context
    _set_root(Path("/"))
    return main([])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="read-only Horizon game-control verifier")
    parser.add_argument("--root", type=Path, default=None, help="target root for staged static verification")
    parser.add_argument("--static", action="store_true", help="only inspect package files under --root")
    parser.add_argument(
        "--relay-state",
        choices=("private", "production"),
        default="private",
        help="validate per-relay private or production activation state",
    )
    parser.add_argument(
        "--authenticated-probe",
        action="store_true",
        help="opt in to a session-creating invalid-body origin probe (never lifecycle actions)",
    )
    parser.add_argument("--mutation-manifest", type=Path, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.root is not None:
        _set_root(args.root)
    checks = Checks()
    if args.static or args.root is not None and args.root != Path("/"):
        _check_target_package(checks)
    else:
        _check_services(checks)
        _check_effective_controls(checks)
        _check_block_schedulers(checks)
        _check_relay_state(checks, args.relay_state)
        profiles = _load_profiles(checks)
        _check_target_registry(checks, profiles)
        owner = _controller_owned_profile()
        listeners = _check_listeners(checks, profiles, owner)
        _check_runtime_files(checks)
        _check_service_account_access(checks)
        _check_public_origin_config(checks)
        _check_installed_package_mirror(checks)
        _check_profile_status(checks, profiles, listeners)
        _check_private_auth(checks)
        if args.authenticated_probe:
            _check_authenticated_origin_probe(checks)
        _check_status_perf(checks)
        _check_peer_rejection(checks)
        _check_systemd_adapter_contract(checks)
        _check_schema_and_redaction(checks)
        _check_database(checks, STATE_DB, EXPECTED_STATE_TABLES, EXPECTED_APPEND_ONLY, "state", 0, 0)
        try:
            web_uid = pwd.getpwnam("gamecontrol").pw_uid
            web_gid = grp.getgrnam("gamecontrol").gr_gid
        except KeyError:
            web_uid = web_gid = -1
        _check_database(checks, WEB_DB, {"web_sessions"}, set(), "web", web_uid, web_gid)
        _check_db_separation(checks)
        _check_manifest(checks, args.mutation_manifest)
    payload = {"ok": not checks.failed, "checks": checks.items, "check_count": len(checks.items)}
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 2 if checks.missing_privilege else (1 if checks.failed else 0)


if __name__ == "__main__":
    raise SystemExit(main())
