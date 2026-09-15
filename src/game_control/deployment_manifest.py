"""The single, typed declaration of the Horizon deployment package.

This module is deliberately boring: it is stdlib-only, has no import-time
filesystem or process effects, and contains policy rather than a signature.
The installer and the static verifier load it from their own source trees.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
import json
from pathlib import Path
import stat
from typing import Iterable

_CANONICAL_ALLOWED_DIRECTORY_PARENTS: frozenset[str] = frozenset()

def _relative(value: str, label: str) -> None:
    if not isinstance(value, str) or not value or value in {".", ".."} or "\\" in value:
        raise ValueError(f"invalid {label} path")
    path = Path(value)
    if path.is_absolute() or str(path) != value:
        raise ValueError(f"invalid {label} path")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"invalid {label} path")


def _target(value: str, label: str = "target") -> None:
    if not isinstance(value, str) or not value.startswith("/") or "\\" in value:
        raise ValueError(f"invalid {label} path")
    path = Path(value)
    if value == "/" or str(path) != value or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"invalid {label} path")


def _mode(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value & ~0o7777:
        raise ValueError("invalid mode")


def _tuple(value: object, label: str) -> tuple:
    if not isinstance(value, tuple):
        raise ValueError(f"{label} must be an immutable tuple")
    return value


def _text(value: object, label: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"invalid {label}")


@dataclass(frozen=True, slots=True)
class ProfileSpec:
    id: str
    profile_source: str
    runner_source: str
    unit: str

    def __post_init__(self) -> None:
        _text(self.id, "profile id")
        _relative(self.profile_source, "profile source")
        _relative(self.runner_source, "runner source")
        _text(self.unit, "profile unit")


@dataclass(frozen=True, slots=True)
class FileSpec:
    source: str
    target: str
    mode: int
    owner: str = "root"
    group: str = "root"
    category: str = "static"
    staged_owner: str = "root"
    staged_group: str = "root"

    def __post_init__(self) -> None:
        _relative(self.source, "source")
        _target(self.target)
        _mode(self.mode)
        _text(self.owner, "file owner")
        _text(self.group, "file group")
        if self.category not in {"static", "runtime"}:
            raise ValueError("invalid file category")
        _text(self.staged_owner, "staged file owner")
        _text(self.staged_group, "staged file group")

    def source_path(self, package_root: Path) -> Path:
        return package_root / self.source

    def target_path(self, root: Path = Path("/")) -> Path:
        return _under_root(root, self.target)


@dataclass(frozen=True, slots=True)
class DirectorySpec:
    target: str
    mode: int
    owner: str = "root"
    group: str = "root"
    allowed_children: tuple[str, ...] = ()
    allow_unmanaged_children: bool = False
    staged_owner: str = "root"
    staged_group: str = "root"

    def __post_init__(self) -> None:
        _target(self.target)
        _mode(self.mode)
        _text(self.owner, "directory owner")
        _text(self.group, "directory group")
        _text(self.staged_owner, "staged directory owner")
        _text(self.staged_group, "staged directory group")
        _tuple(self.allowed_children, "directory allowed children")
        if not isinstance(self.allow_unmanaged_children, bool):
            raise ValueError("directory allow_unmanaged_children must be bool")
        if any(not isinstance(child, str) or not child or "/" in child for child in self.allowed_children):
            raise ValueError("invalid directory allowed child")

    def target_path(self, root: Path = Path("/")) -> Path:
        return _under_root(root, self.target)


@dataclass(frozen=True, slots=True)
class SymlinkSpec:
    target: str
    link_target: str

    def __post_init__(self) -> None:
        _target(self.target)
        _target(self.link_target)

    def target_path(self, root: Path = Path("/")) -> Path:
        return _under_root(root, self.target)


@dataclass(frozen=True, slots=True)
class NamespaceSpec:
    name: str
    path: str
    exact: tuple[str, ...] = ()
    prefixes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _text(self.name, "namespace name")
        _target(self.path)
        _tuple(self.exact, "namespace exact")
        _tuple(self.prefixes, "namespace prefixes")
        if any(not isinstance(value, str) or not value for value in (*self.exact, *self.prefixes)):
            raise ValueError("invalid namespace entry")
        if len(set(self.exact)) != len(self.exact) or len(set(self.prefixes)) != len(self.prefixes):
            raise ValueError("invalid namespace")


@dataclass(frozen=True, slots=True)
class RetiredSpec:
    paths: tuple[str, ...]
    names: tuple[str, ...]

    def __post_init__(self) -> None:
        _tuple(self.paths, "retired paths")
        _tuple(self.names, "retired names")
        if len(set(self.paths)) != len(self.paths) or len(set(self.names)) != len(self.names):
            raise ValueError("duplicate retired artifact")
        for path in self.paths:
            _target(path)
        if any(not isinstance(name, str) or not name for name in self.names):
            raise ValueError("invalid retired name")


@dataclass(frozen=True, slots=True)
class SecretSpec:
    name: str
    target: str
    mode: int = 0o600
    owner: str = "root"
    group: str = "root"

    def __post_init__(self) -> None:
        _text(self.name, "secret name")
        _target(self.target)
        _mode(self.mode)
        _text(self.owner, "secret owner")
        _text(self.group, "secret group")


@dataclass(frozen=True, slots=True)
class DatabaseSpec:
    name: str
    target: str
    read_only: bool = True

    def __post_init__(self) -> None:
        _text(self.name, "database name")
        _target(self.target)
        if not isinstance(self.read_only, bool):
            raise ValueError("database read_only must be bool")


@dataclass(frozen=True, slots=True)
class RuntimeManifestSpec:
    target: str
    version: str
    mode: int = 0o600
    owner: str = "root"
    group: str = "root"

    def __post_init__(self) -> None:
        _target(self.target)
        _text(self.version, "runtime manifest version")
        _mode(self.mode)
        _text(self.owner, "runtime manifest owner")
        _text(self.group, "runtime manifest group")


@dataclass(frozen=True, slots=True)
class GeneratedEntryPointSpec:
    name: str
    target: str
    module: str
    mode: int = 0o755
    owner: str = "root"
    group: str = "root"

    def __post_init__(self) -> None:
        if not self.name or "/" in self.name or "\\" in self.name:
            raise ValueError("invalid generated entry-point name")
        _target(self.target, "generated entry-point target")
        if not self.module or ":" not in self.module or any(char.isspace() for char in self.module):
            raise ValueError("invalid generated entry-point module")
        _mode(self.mode)
        if not self.owner or not self.group:
            raise ValueError("generated entry-point owner/group must be non-empty")


@dataclass(frozen=True, slots=True)
class RelayModeSpec:
    name: str
    expectations: tuple[tuple[str, bool], ...]

    def __post_init__(self) -> None:
        _text(self.name, "relay mode name")
        _tuple(self.expectations, "relay expectations")
        for item in self.expectations:
            if not isinstance(item, tuple) or len(item) != 2 or not isinstance(item[0], str) or not item[0] or not isinstance(item[1], bool):
                raise ValueError("invalid relay expectation")


@dataclass(frozen=True, slots=True)
class DeploymentManifest:
    schema_version: int
    profiles: tuple[ProfileSpec, ...]
    files: tuple[FileSpec, ...]
    directories: tuple[DirectorySpec, ...]
    symlinks: tuple[SymlinkSpec, ...]
    namespaces: tuple[NamespaceSpec, ...]
    retired: RetiredSpec
    secrets: tuple[SecretSpec, ...]
    databases: tuple[DatabaseSpec, ...]
    runtime_manifest: RuntimeManifestSpec
    generated_entry_point: GeneratedEntryPointSpec
    relay_modes: tuple[RelayModeSpec, ...]
    runtime_sources: tuple[str, ...]
    runtime_support: tuple[FileSpec, ...]

    def __post_init__(self) -> None:
        if isinstance(self.schema_version, bool) or not isinstance(self.schema_version, int):
            raise ValueError("invalid schema version")
        if self.schema_version != 1:
            raise ValueError("unsupported manifest schema")
        for value, label in ((self.profiles, "profiles"), (self.files, "files"), (self.directories, "directories"), (self.symlinks, "symlinks"), (self.namespaces, "namespaces"), (self.secrets, "secrets"), (self.databases, "databases"), (self.relay_modes, "relay modes"), (self.runtime_sources, "runtime sources"), (self.runtime_support, "runtime support")):
            _tuple(value, label)
        if not all(isinstance(item, ProfileSpec) for item in self.profiles):
            raise ValueError("profiles must contain ProfileSpec records")
        if not all(isinstance(item, FileSpec) for item in self.files + self.runtime_support):
            raise ValueError("files must contain FileSpec records")
        if not all(isinstance(item, DirectorySpec) for item in self.directories):
            raise ValueError("directories must contain DirectorySpec records")
        if not all(isinstance(item, SymlinkSpec) for item in self.symlinks):
            raise ValueError("symlinks must contain SymlinkSpec records")
        if not all(isinstance(item, NamespaceSpec) for item in self.namespaces):
            raise ValueError("namespaces must contain NamespaceSpec records")
        if not isinstance(self.retired, RetiredSpec) or not isinstance(self.runtime_manifest, RuntimeManifestSpec):
            raise ValueError("invalid manifest metadata record")
        if not all(isinstance(item, SecretSpec) for item in self.secrets) or not all(isinstance(item, DatabaseSpec) for item in self.databases) or not all(isinstance(item, RelayModeSpec) for item in self.relay_modes):
            raise ValueError("invalid manifest metadata records")
        if len({p.id for p in self.profiles}) != len(self.profiles):
            raise ValueError("duplicate profile id")
        if len({f.target for f in self.files}) != len(self.files):
            raise ValueError("duplicate file target")
        if len({d.target for d in self.directories}) != len(self.directories):
            raise ValueError("duplicate directory target")
        if len({s.target for s in self.symlinks}) != len(self.symlinks):
            raise ValueError("duplicate symlink target")
        declarations = [(f.target, "file") for f in self.files] + [(d.target, "directory") for d in self.directories] + [(s.target, "symlink") for s in self.symlinks]
        runtime_targets = [spec.target for spec in self.runtime_files_for()]
        declarations.extend((target, "runtime") for target in runtime_targets)
        for index, (target, kind) in enumerate(declarations):
            for other, other_kind in declarations[index + 1:]:
                if target == other:
                    if {kind, other_kind} <= {"file", "runtime"}:
                        continue
                    raise ValueError("deployment target collision")
                target_path, other_path = Path(target), Path(other)
                if target_path in other_path.parents or other_path in target_path.parents:
                    ancestor_kind = kind if target_path in other_path.parents else other_kind
                    if ancestor_kind != "directory":
                        raise ValueError("deployment target parent collision")
                    if _CANONICAL_ALLOWED_DIRECTORY_PARENTS and target not in _CANONICAL_ALLOWED_DIRECTORY_PARENTS and other not in _CANONICAL_ALLOWED_DIRECTORY_PARENTS:
                        raise ValueError("deployment target parent collision")
        for profile in self.profiles:
            if profile.profile_source != f"config/profiles/{profile.id}.toml":
                raise ValueError("profile source mismatch")
            if profile.runner_source != f"config/runner/{profile.id}.json":
                raise ValueError("runner source mismatch")
            if profile.unit != f"{profile.id}.service":
                raise ValueError("profile unit mismatch")
        for source in self.runtime_sources:
            _relative(source, "runtime source")
        if len(set(self.runtime_sources)) != len(self.runtime_sources):
            raise ValueError("duplicate runtime source")
        if len({s.name for s in self.secrets}) != len(self.secrets):
            raise ValueError("duplicate secret name")
        if len({db.name for db in self.databases}) != len(self.databases):
            raise ValueError("duplicate database name")

    @staticmethod
    def _project(records: Iterable[FileSpec | DirectorySpec | SymlinkSpec], root: Path):
        return tuple(replace(record, target=str(record.target_path(root))) for record in records)

    def files_for(self, root: Path = Path("/")) -> tuple[FileSpec, ...]:
        return self._project(self.files, root)

    def directories_for(self, root: Path = Path("/")) -> tuple[DirectorySpec, ...]:
        return self._project(self.directories, root)

    def links_for(self, root: Path = Path("/")) -> tuple[SymlinkSpec, ...]:
        return self._project(self.symlinks, root)

    def secret_metadata_for(self, root: Path = Path("/")) -> tuple[SecretSpec, ...]:
        return tuple(replace(secret, target=str(_under_root(root, secret.target))) for secret in self.secrets)

    def namespace_policy_for(self, root: Path = Path("/")) -> tuple[NamespaceSpec, ...]:
        return tuple(replace(namespace, path=str(_under_root(root, namespace.path))) for namespace in self.namespaces)

    def runtime_files_for(self, root: Path = Path("/")) -> tuple[FileSpec, ...]:
        static_sources = tuple(
            FileSpec(source=file.source, target=f"/opt/game-control/{file.source}", mode=file.mode, category="runtime")
            for file in self.files
        )
        sources = tuple(
            FileSpec(source=source, target=f"/opt/game-control/{source}", mode=0o644, category="runtime")
            for source in (*self.runtime_sources, "src/game_control/deployment_manifest.py")
        )
        verifier = FileSpec("scripts/verify-deployed.py", "/opt/game-control/scripts/verify-deployed.py", 0o600, category="runtime")
        return self._project((*static_sources, *sources, *self.runtime_support, verifier), root)

    def validate(self, package_root: Path | None = None) -> None:
        """Validate declarations and, when supplied, every source boundary."""
        self.__post_init__()
        if package_root is None:
            return
        for file in (*self.files, *self.runtime_support):
            _source_regular(file.source_path(package_root))
        for source in self.runtime_sources:
            _source_regular(package_root / source)
        _source_regular(package_root / "src/game_control/deployment_manifest.py")
        _source_regular(package_root / "scripts/verify-deployed.py")


def _source_regular(path: Path) -> None:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError(f"package source is not a regular single-link file: {path}")


def _under_root(root: Path, target: str) -> Path:
    return root / target.lstrip("/")


_PROFILES = (
    ProfileSpec("minecraft-sunlit-cobblemon", "config/profiles/minecraft-sunlit-cobblemon.toml", "config/runner/minecraft-sunlit-cobblemon.json", "minecraft-sunlit-cobblemon.service"),
    ProfileSpec("terraria-vanilla", "config/profiles/terraria-vanilla.toml", "config/runner/terraria-vanilla.json", "terraria-vanilla.service"),
    ProfileSpec("terraria-tmod", "config/profiles/terraria-tmod.toml", "config/runner/terraria-tmod.json", "terraria-tmod.service"),
)


def _file(source: str, target: str, mode: int = 0o644, category: str = "static") -> FileSpec:
    return FileSpec(source, target, mode, category=category)


FIXED_LIBEXEC_NAMES = (
    "game-slot-run", "game-console-stop", "game-console-command",
    "game-sunlit-prepare", "game-sunlit-rcon-prepare", "game-sunlit-stop",
    "horizon-alert-notify", "horizon-bore-liveness", "horizon-lazymc-wake",
    "horizon-sunlit-auto-update", "horizon-sunlit-update-rpc",
)
ABSENT_LIBEXEC_NAMES = (
    "horizon-sunlit-manifest", "horizon-sunlit-stage", "horizon-sunlit-promote",
    "horizon_journal.py", "horizon-state-migrate", "horizon-telemetry-migrate",
    "horizon-memory-drill", "horizon-phase2-threshold", "horizon-phase2-collect",
    "horizon-phase2-browser-evidence", "horizon-phase2-live-acceptance",
)
COMPATIBILITY_LIBEXEC_NAMES = (
    "horizon-backup-reconcile",
    "horizon-capability-issue",
    "horizon-jvm-args",
    "horizon-session-revoke-all",
    "horizon-journal-evidence",
    "horizon-journal-finalize",
)

_FILES = (
    *tuple(_file(p.profile_source, f"/etc/game-control/profiles.d/{p.id}.toml") for p in _PROFILES),
    *tuple(_file(p.runner_source, f"/etc/game-control/runner.d/{p.id}.json") for p in _PROFILES),
    *tuple(_file(f"web/{name}", f"/opt/game-control/web/{name}") for name in ("app.js", "commands.js", "index.html", "palette.js", "styles.css")),
    *tuple(_file(f"ops/systemd/{name}", f"/etc/systemd/system/{name}") for name in (
        "game-control-web.service", "game-slotd.service", "horizon-bore-liveness.service", "horizon-bore-liveness.timer",
        "horizon-sunlit-auto-update.service", "horizon-sunlit-auto-update.timer", "lazymc-minecraft.service",
        "bore-minecraft-fenced.service", "horizon-alert-drill@.service", "horizon-alert-notify@.service",
        "horizon-terraria-relay.service", "minecraft-sunlit-cobblemon.service", "terraria-tmod.service", "terraria-vanilla.service")),
    _file("ops/systemd/game-slotd.service.d/io-metrics.conf", "/etc/systemd/system/game-slotd.service.d/io-metrics.conf"),
    _file("ops/systemd/minecraft-sunlit-cobblemon.service.d/gc-telemetry.conf", "/etc/systemd/system/minecraft-sunlit-cobblemon.service.d/gc-telemetry.conf"),
    *tuple(_file(f"ops/systemd/{name}", f"/etc/systemd/system/{name}") for name in ("games.slice", "horizon.slice", "maintenance.slice")),
    _file("ops/tmpfiles/game-control.conf", "/usr/lib/tmpfiles.d/game-control.conf"),
    *tuple(_file(f"ops/bin/{name}", f"/usr/local/libexec/{name}", 0o755) for name in FIXED_LIBEXEC_NAMES),
    _file("config/game-control.toml", "/etc/game-control/game-control.toml", 0o600),
    _file("ops/lazymc/lazymc.toml", "/etc/game-control/lazymc/lazymc.toml"),
    _file("ops/lazymc/server.properties", "/etc/game-control/lazymc/server.properties"),
    _file("ops/nftables/horizon.nft", "/etc/nftables.conf"),
    _file("ops/journald/horizon.conf", "/etc/systemd/journald@horizon.conf"),
    _file("ops/journald/horizon-private-measurement.conf", "/usr/local/share/horizon/horizon-private-measurement.conf"),
)


_DIRECTORIES = (
    ("etc/game-control", 0o755, "root", "root"), ("etc/game-control/profiles.d", 0o755, "root", "root"),
    ("etc/game-control/runner.d", 0o755, "root", "root"), ("etc/game-control/secrets.d", 0o700, "root", "root"),
    ("etc/game-control/arm", 0o700, "root", "root"), ("etc/game-control/jvm", 0o755, "root", "root"), ("etc/game-control/lazymc", 0o755, "root", "root"),
    ("etc/systemd/journald@horizon.conf.d", 0o700, "root", "root"), ("etc/wireguard", 0o700, "root", "root"),
    ("usr/local/share/horizon", 0o755, "root", "root"), ("usr/local/libexec", 0o755, "root", "root"),
    ("opt/game-control/web", 0o755, "root", "root"), ("var/lib/game-control", 0o700, "root", "root"), ("var/lib/game-control/log-checkpoints", 0o700, "root", "root"),
    ("var/lib/game-control/alerts", 0o700, "root", "root"), ("var/lib/game-control/horizon-journal", 0o700, "root", "root"),
    ("var/lib/game-control/migrations", 0o700, "root", "root"), ("var/lib/game-control-web", 0o700, "gamecontrol", "gamecontrol"),
    ("run/game-control", 0o755, "root", "root"), ("run/game-slot", 0o770, "root", "gameslot"),
    ("opt/game-servers", 0o755, "root", "root"), ("srv/game-servers", 0o755, "root", "root"),
    ("opt/game-servers/minecraft-sunlit-cobblemon", 0o755, "root", "root"), ("opt/game-servers/minecraft-sunlit-cobblemon/releases", 0o755, "root", "root"),
    ("srv/game-servers/minecraft-sunlit-cobblemon", 0o750, "svc-sunlit", "svc-sunlit"),
    ("opt/game-servers/terraria-vanilla", 0o755, "root", "root"), ("opt/game-servers/terraria-tmod", 0o755, "root", "root"),
    ("srv/game-servers/terraria-vanilla", 0o750, "terraria-vanilla", "terraria-vanilla"), ("srv/game-servers/terraria-tmod", 0o750, "tmodloader", "tmodloader"),
    *((f"srv/game-servers/{profile}/{subdir}", 0o750, user, user) for profile, user in (("terraria-vanilla", "terraria-vanilla"), ("terraria-tmod", "tmodloader")) for subdir in ("config", "worlds", "mods", "logs", "backups")),
    *((f"srv/game-servers/{profile}/.local/{suffix}", 0o700, user, user) for profile, user in (("terraria-vanilla", "terraria-vanilla"), ("terraria-tmod", "tmodloader")) for suffix in ("", "share", "share/Terraria")),
    ("srv/game-servers/terraria-tmod/logs/tModLoader-Logs", 0o750, "tmodloader", "tmodloader"),
    ("var/backups/game-servers/minecraft-sunlit-cobblemon", 0o700, "root", "root"), ("var/backups/game-servers/terraria-vanilla", 0o700, "root", "root"),
    ("var/backups/game-servers/terraria-tmod", 0o700, "root", "root"), ("var/backups/game-servers", 0o700, "root", "root"),
)

_RUNTIME_SOURCES = tuple(
    "src/" + name for name in (
        "game_control/__init__.py", "game_control/adapters/__init__.py", "game_control/adapters/base.py", "game_control/adapters/crafty.py", "game_control/adapters/systemd.py", "game_control/runtime/__init__.py", "game_control/runtime/alerts.py", "game_control/runtime/compatibility.py", "game_control/runtime/protocols.py", "game_control/runtime/telemetry.py", "game_control/alert_policy.py", "game_control/api.py", "game_control/auth.py", "game_control/_fixed_helper.py", "game_control/backup_command.py", "game_control/backup_reconcile.py", "game_control/backups.py", "game_control/benchmark_safety.py", "game_control/benchmarks.py", "game_control/capability.py", "game_control/capability_evidence.py", "game_control/capability_issue.py", "game_control/cli.py", "game_control/controller.py", "game_control/db_telemetry.py", "game_control/deployment_verify.py", "game_control/driver_preflight.py", "game_control/errors.py", "game_control/gc_telemetry.py", "game_control/health.py", "game_control/history_queries.py", "game_control/idle_stop.py", "game_control/maintenance_process.py", "game_control/introspection.py", "game_control/journal_evidence.py", "game_control/jvm_args.py", "game_control/lazymc.py", "game_control/log_follower.py", "game_control/logs.py", "game_control/managed_tuning.py", "game_control/metrics.py", "game_control/models.py", "game_control/modpack_update.py", "game_control/notifications.py", "game_control/perf.py", "game_control/players.py", "game_control/profile.py", "game_control/profile_config.py", "game_control/protocol.py", "game_control/push.py", "game_control/rcon.py", "game_control/rcon_telemetry.py", "game_control/redaction.py", "game_control/root_state.py", "game_control/schedule.py", "game_control/schedule_config.py", "game_control/service_container.py", "game_control/service_wiring.py", "game_control/session_revoke.py", "game_control/session_store.py", "game_control/sessions.py", "game_control/slot.py", "game_control/slotd_main.py", "game_control/state_db.py", "game_control/stats_queries.py", "game_control/status.py", "game_control/sunlit_manifest.py", "game_control/sunlit_promote.py", "game_control/sunlit_stage.py", "game_control/sunlit_update.py", "game_control/telemetry_db.py", "game_control/telemetry_sampler.py", "game_control/tick_telemetry.py", "game_control/tps.py", "game_control/updates.py", "game_control/web_db.py", "game_control/web_main.py", "game_control/worlds.py"))

_DIRECTORY_SPECS = tuple(
    replace(
        DirectorySpec("/" + item[0].rstrip("/"), *item[1:]),
        allowed_children=("log-checkpoints",) if item[0] == "var/lib/game-control" else
        ("jvm",) if item[0] == "etc/game-control" else
        ("operation.lock", "slot.lock", "reservation.json") if item[0] == "run/game-control" else
        ("slot.json",) if item[0] == "run/game-slot" else
        ("server.log",) if item[0] in {
            "srv/game-servers/terraria-vanilla/logs",
            "srv/game-servers/terraria-tmod/logs",
        } else
        (".horizon-restore-anchor",) if item[0] == "srv/game-servers/terraria-tmod/logs/tModLoader-Logs" else (),
        allow_unmanaged_children=(
            item[0] in {
                "etc/game-control",
                "etc/game-control/secrets.d",
                "etc/game-control/arm",
                "etc/game-control/jvm",
                "etc/game-control/lazymc",
                "etc/systemd/journald@horizon.conf.d",
                "etc/wireguard",
                "usr/local/share/horizon",
                "usr/local/libexec",
                "var/lib/game-control",
                "var/lib/game-control/log-checkpoints",
                "var/lib/game-control/alerts",
                "var/lib/game-control/horizon-journal",
                "var/lib/game-control/migrations",
                "var/lib/game-control-web",
                "opt/game-servers",
                "srv/game-servers",
                "opt/game-servers/minecraft-sunlit-cobblemon",
                "opt/game-servers/minecraft-sunlit-cobblemon/releases",
                "srv/game-servers/minecraft-sunlit-cobblemon",
                "opt/game-servers/terraria-vanilla",
                "opt/game-servers/terraria-tmod",
                "srv/game-servers/terraria-vanilla",
                "srv/game-servers/terraria-tmod",
                "var/backups/game-servers",
                "var/backups/game-servers/minecraft-sunlit-cobblemon",
                "var/backups/game-servers/terraria-vanilla",
                "var/backups/game-servers/terraria-tmod",
            }
            or item[0].startswith("srv/game-servers/terraria-vanilla/")
            or item[0].startswith("srv/game-servers/terraria-tmod/")
        ),
    )
    for item in _DIRECTORIES
)
_RUNTIME_SOURCES += (
    "src/game_control/origin_config.py",
    "src/game_control/resource_capacity.py",
    "src/game_control/retirement.py",
    "src/game_control/retirement_command.py",
)

_MANIFEST = DeploymentManifest(
    1, _PROFILES, _FILES, _DIRECTORY_SPECS,
    (SymlinkSpec("/srv/game-servers/minecraft-sunlit-cobblemon/libraries", "/opt/game-servers/minecraft-sunlit-cobblemon/libraries"),),
    (NamespaceSpec("profiles", "/etc/game-control/profiles.d", tuple(f"{p.id}.toml" for p in _PROFILES)), NamespaceSpec("runners", "/etc/game-control/runner.d", tuple(f"{p.id}.json" for p in _PROFILES)), NamespaceSpec("systemd", "/etc/systemd/system", tuple(f.target.removeprefix("/etc/systemd/system/") for f in _FILES if f.target.startswith("/etc/systemd/system/") and "/" not in f.target.removeprefix("/etc/systemd/system/")), ("game-control-", "game-slotd", "horizon-", "lazymc-", "bore-minecraft-fenced")), NamespaceSpec("libexec", "/usr/local/libexec", tuple(f.target.removeprefix("/usr/local/libexec/") for f in _FILES if f.target.startswith("/usr/local/libexec/")), ("game-", "horizon-"))),
    RetiredSpec(("/etc/game-control/profiles.d/minecraft.toml", "/etc/game-control/profiles.d/pz-rising.toml", "/etc/game-control/runner.d/minecraft.json", "/etc/game-control/runner.d/pz-rising.json", "/etc/systemd/system/pz-rising.service", "/etc/game-control/secrets.d/crafty-token"), ("minecraft.toml", "pz-rising.toml", "minecraft.json", "pz-rising.json", "pz-rising.service", "crafty.service")),
    (SecretSpec("b2", "/etc/game-control/secrets.d/horizon-b2-rclone.conf"), SecretSpec("rcon", "/etc/game-control/secrets.d/minecraft-rcon-password")),
    (DatabaseSpec("state", "/var/lib/game-control/state.db"), DatabaseSpec("web", "/var/lib/game-control-web/web.db")),
    RuntimeManifestSpec("/opt/game-control/.horizon-runtime-manifest", "1"),
    GeneratedEntryPointSpec("horizon", "/opt/game-control/.venv/bin/horizon", "game_control.cli:main"),
    (RelayModeSpec("private", (("bore-minecraft-fenced.service", False), ("horizon-terraria-relay.service", False))), RelayModeSpec("production", (("bore-minecraft-fenced.service", True), ("horizon-terraria-relay.service", False)))),
    _RUNTIME_SOURCES,
    (FileSpec("pyproject.toml", "/opt/game-control/pyproject.toml", 0o644, category="runtime"), FileSpec("ops/install.py", "/opt/game-control/ops/install.py", 0o755, category="runtime")),
)
_CANONICAL_ALLOWED_DIRECTORY_PARENTS = frozenset(directory.target for directory in _MANIFEST.directories)


def get_manifest() -> DeploymentManifest:
    return _MANIFEST


def manifest_digest() -> str:
    payload = json.dumps(asdict(_MANIFEST), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(payload).hexdigest()
