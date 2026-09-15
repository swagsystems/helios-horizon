"""Verified backups and security-first restore operations.

The services in this module deliberately do not accept filesystem paths from an
operator.  Paths come from a validated ``Profile`` and every archive member is
validated before an extraction can occur.
"""

from __future__ import annotations

import hashlib
import asyncio
import inspect
import json
import errno
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Protocol

from .errors import SafeError
from .maintenance_process import maintenance_argv, maintenance_popen
from .models import BackupDestination, ProfileId
from .protocol import BackupPage, BackupSummary, JobAccepted
from .retirement import (
    MANIFEST_DIR,
    RETIREMENT_PHASES,
    SENDER_LOCK_PATH,
    PayloadState,
    RetirementService,
    SenderInterlock,
    blocking_backup_ids,
    noreplace_supported,
)


def _rpc_key(value: Any) -> str:
    value = getattr(value, "id", value)
    value = getattr(value, "value", value)
    return str(value)


def _rpc_database_path(database: Any) -> Any | None:
    path = getattr(database, "path", None)
    opener = getattr(type(database), "open", None)
    if path is None or not callable(opener):
        return None
    try:
        return opener(path)
    except Exception:
        return None


def _rpc_close_database(database: Any | None) -> None:
    if database is not None and hasattr(database, "close"):
        database.close()


def _rpc_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if value is None:
        return datetime.fromtimestamp(0, timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return datetime.fromtimestamp(0, timezone.utc)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


_MANIFEST = "manifest.json"
_ARCHIVE_SUFFIX = ".tar.zst"
_MARGIN = 1.10
_B2_RETENTION = 2
_B2_REMOTE = "helios-b2-crypt"
_B2_CONFIG = Path("/etc/game-control/secrets.d/horizon-b2-rclone.conf")
_B2_PREFIX_ROOT = "helios/horizon/app"
FULL_LXC_PREFIX = "helios/horizon/full-lxc"
_BACKUP_ID = r"[0-9]{8}T[0-9]{12}Z-[0-9a-f]{12}"
_MAX_ID = (1 << 32) - 1
_B2_PROFILES = frozenset(
    {
        ProfileId.MINECRAFT_SUNLIT_COBBLEMON.value,
        ProfileId.TERRARIA_VANILLA.value,
        ProfileId.TERRARIA_TMOD.value,
    }
)


def _root_identity(root: Path) -> str:
    return hashlib.sha256(str(root).encode("utf-8")).hexdigest()


def _root_id(root: Path) -> str:
    return "root-" + _root_identity(root)[:24]
# Sunlit's server-side backup mod writes complete rolling world archives here.
# Horizon protects its own verified application archives instead, so retaining
# those nested archives would recursively amplify every generation. A restore
# intentionally discards this redundant tree; the server-side mod recreates it.
_SUNLIT_REDUNDANT_BACKUP_DIRS = frozenset({"backups"})


class BackupClass(StrEnum):
    APPLICATION = "application"
    FULL_LXC = "full-lxc"


class OnlineSaveTransport(Protocol):
    def save_off(self) -> Any: ...

    def save_all_flush(self) -> Any: ...

    def save_on(self) -> Any: ...


@dataclass(frozen=True)
class B2DestinationConfig:
    """Source-owned B2 configuration; no operator input reaches these values."""

    destination_id: BackupDestination = BackupDestination.HORIZON_B2
    remote_name: str = _B2_REMOTE
    prefix_root: str = _B2_PREFIX_ROOT
    credential_config: Path = _B2_CONFIG
    retention: int = _B2_RETENTION

    def __post_init__(self) -> None:
        if (
            self.destination_id is not BackupDestination.HORIZON_B2
            or self.remote_name != _B2_REMOTE
            or self.prefix_root != _B2_PREFIX_ROOT
            or self.credential_config != _B2_CONFIG
            or self.retention != _B2_RETENTION
        ):
            raise ValueError("B2 destination configuration is source-owned")


B2_DESTINATION = B2DestinationConfig()


def _validate_b2_credentials() -> None:
    """Validate the fixed credential path without opening or parsing it."""
    for directory in (
        Path("/"),
        Path("/etc"),
        Path("/etc/game-control"),
        Path("/etc/game-control/secrets.d"),
    ):
        try:
            info = os.lstat(directory)
        except OSError as exc:
            raise SafeError(
                "backup_protection_failed", "remote backup credentials are unavailable"
            ) from exc
        mode = stat.S_IMODE(info.st_mode)
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != 0
            or info.st_gid != 0
            or mode & 0o022
            or (directory == Path("/etc/game-control/secrets.d") and mode != 0o700)
        ):
            raise SafeError(
                "backup_protection_failed", "remote backup credentials are unavailable"
            )
    try:
        info = os.lstat(_B2_CONFIG)
    except OSError as exc:
        raise SafeError(
            "backup_protection_failed", "remote backup credentials are unavailable"
        ) from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != 0
        or info.st_gid != 0
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise SafeError(
            "backup_protection_failed", "remote backup credentials are unavailable"
        )


@dataclass(frozen=True)
class RemoteObject:
    key: str
    size_bytes: int = 0


class B2Transport(Protocol):
    def upload(self, source: Path, key: str) -> None: ...

    def download(self, key: str, destination: Path) -> None: ...

    def verify(self, source: Path, key: str) -> None: ...

    def list(self, prefix: str) -> Iterable[RemoteObject]: ...

    def delete(self, key: str) -> None: ...


class B2CommandTransport:
    """Client-side encrypted B2 transport using a fixed rclone config.

    Credentials are read by rclone from the root-owned config file. They are
    never represented in argv, returned output, exceptions, or audit state.
    """

    def __init__(
        self,
        *,
        destination: B2DestinationConfig = B2_DESTINATION,
        runner: Callable[..., Any] | None = None,
        credential_validator: Callable[[], None] | None = None,
    ) -> None:
        self.destination = destination
        self.runner = runner or subprocess.run
        self.credential_validator = credential_validator or _validate_b2_credentials

    def _run(self, argv: list[str]) -> Any:
        try:
            self.credential_validator()
            return self.runner(
                maintenance_argv(argv, slice_name="maintenance.slice"),
                check=True,
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except SafeError:
            raise
        except (OSError, subprocess.SubprocessError) as exc:
            raise SafeError("backup_protection_failed", "remote backup operation failed") from exc
        except Exception as exc:
            # Test seams and alternate runners must receive the same safe
            # boundary as subprocess failures; never surface their text.
            raise SafeError("backup_protection_failed", "remote backup operation failed") from exc

    def _base(self, operation: str) -> list[str]:
        return [
            "/usr/bin/rclone",
            "--config",
            str(self.destination.credential_config),
            operation,
        ]

    def _remote(self, key: str) -> str:
        return f"{self.destination.remote_name}:{key}"

    @staticmethod
    def _approved_prefix(prefix: str) -> str:
        if not isinstance(prefix, str) or prefix not in {b2_prefix(profile) for profile in _B2_PROFILES}:
            raise SafeError("invalid_backup_destination", "backup destination is not approved")
        return prefix

    @staticmethod
    def _approved_key(key: str) -> str:
        if not isinstance(key, str):
            raise SafeError("invalid_backup_destination", "backup object is not approved")
        parts = key.split("/")
        if len(parts) != 5 or parts[:3] != ["helios", "horizon", "app"]:
            raise SafeError("invalid_backup_destination", "backup object is not approved")
        if parts[3] not in _B2_PROFILES:
            raise SafeError("invalid_backup_destination", "backup object is not approved")
        # Canonical generated keys have exactly app/<profile>/<archive>.
        canonical_prefix = b2_prefix(parts[3])
        if not key.startswith(canonical_prefix + "/"):
            raise SafeError("invalid_backup_destination", "backup object is not approved")
        basename = key.removeprefix(canonical_prefix + "/")
        if not re.fullmatch(_BACKUP_ID + re.escape(_ARCHIVE_SUFFIX), basename):
            raise SafeError("invalid_backup_destination", "backup object is not approved")
        return key

    def upload(self, source: Path, key: str) -> None:
        key = self._approved_key(key)
        if source.name != key.rsplit("/", 1)[-1] or not source.is_file():
            raise SafeError("invalid_backup_destination", "backup object is not approved")
        self._run(self._base("copyto") + [str(source), self._remote(key), "--immutable"])

    def download(self, key: str, destination: Path) -> None:
        key = self._approved_key(key)
        if (
            destination.name != key.rsplit("/", 1)[-1]
            or destination.exists()
            or not destination.parent.is_dir()
        ):
            raise SafeError("invalid_backup_destination", "backup staging path is not approved")
        self._run(self._base("copyto") + [self._remote(key), str(destination), "--immutable"])

    def verify(self, source: Path, key: str) -> None:
        key = self._approved_key(key)
        if source.name != key.rsplit("/", 1)[-1] or not source.is_file():
            raise SafeError("invalid_backup_destination", "backup object is not approved")
        prefix = key.rsplit("/", 1)[0]
        self._run(
            self._base("cryptcheck")
            + [str(source.parent), self._remote(prefix), "--one-way", "--fast-list", "--include", source.name]
        )

    def list(self, prefix: str) -> tuple[RemoteObject, ...]:
        prefix = self._approved_prefix(prefix)
        result = self._run(self._base("lsjson") + ["--files-only", "--recursive", self._remote(prefix)])
        try:
            payload = json.loads(result.stdout.decode("utf-8"))
        except (AttributeError, UnicodeError, json.JSONDecodeError) as exc:
            raise SafeError("backup_protection_failed", "remote listing could not be verified") from exc
        if not isinstance(payload, list):
            raise SafeError("backup_protection_failed", "remote listing could not be verified")
        objects = []
        for item in payload:
            if not isinstance(item, dict) or not isinstance(item.get("Name"), str):
                raise SafeError("backup_protection_failed", "remote listing could not be verified")
            name = item["Name"].lstrip("/")
            key = name if name.startswith(prefix.rstrip("/") + "/") else f"{prefix.rstrip('/')}/{name}"
            self._approved_key(key)
            objects.append(RemoteObject(key=key, size_bytes=max(0, int(item.get("Size", 0)))))
        return tuple(objects)

    def delete(self, key: str) -> None:
        key = self._approved_key(key)
        self._run(self._base("deletefile") + [self._remote(key)])


@dataclass(frozen=True)
class ProtectionRecord:
    backup_id: str
    profile_id: str
    destination_id: BackupDestination
    backup_class: BackupClass
    remote_key: str
    local_sha256: str
    local_verified: bool
    upload_state: str
    remote_verified: bool
    comparison_state: str
    prune_state: str
    error_code: str | None = None


def b2_prefix(profile_id: str, backup_class: BackupClass = BackupClass.APPLICATION) -> str:
    """Return a fixed prefix for an allowlisted Horizon profile/class."""
    profile = str(getattr(profile_id, "value", profile_id))
    try:
        backup_class = BackupClass(backup_class)
    except (TypeError, ValueError) as exc:
        raise SafeError("invalid_backup_destination", "backup class is not approved") from exc
    if profile not in _B2_PROFILES or backup_class is not BackupClass.APPLICATION:
        raise SafeError("invalid_backup_destination", "backup destination is not approved")
    return f"{_B2_PREFIX_ROOT}/{profile}"


def full_lxc_prefix() -> str:
    """Return the fixed P0 full-LXC boundary, owned outside application B2."""
    return FULL_LXC_PREFIX


def validate_destination(destination: BackupDestination | str, profile_id: str) -> BackupDestination:
    try:
        value = BackupDestination(destination)
    except (TypeError, ValueError) as exc:
        raise SafeError("invalid_backup_destination", "backup destination is not approved") from exc
    if value is BackupDestination.HORIZON_B2:
        b2_prefix(profile_id)
    return value


class B2ProtectionService:
    """Protect one locally verified archive and prune only verified B2 peers."""

    def __init__(
        self,
        *,
        database: Any | None = None,
        transport: B2Transport,
        destination: B2DestinationConfig = B2_DESTINATION,
        clock: Callable[[], datetime] | None = None,
        availability: Callable[[str], str] | None = None,
    ) -> None:
        self.database = database
        self.transport = transport
        self.destination = destination
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.availability = availability
        self._memory: dict[tuple[str, str, str], ProtectionRecord] = {}
        self._lock = threading.RLock()

    def protect(
        self,
        record: BackupRecord,
        *,
        backup_class: BackupClass = BackupClass.APPLICATION,
    ) -> ProtectionRecord:
        with self._lock:
            if self.database is None:
                raise SafeError("backup_protection_failed", "durable backup state is unavailable")
            return self._protect(record, backup_class=backup_class)

    def _protect(
        self,
        record: BackupRecord,
        *,
        backup_class: BackupClass = BackupClass.APPLICATION,
    ) -> ProtectionRecord:
        try:
            backup_class = BackupClass(backup_class)
        except (TypeError, ValueError) as exc:
            raise SafeError("invalid_backup_destination", "backup class is not approved") from exc
        validate_destination(self.destination.destination_id, record.profile_id)
        prefix = b2_prefix(record.profile_id, backup_class)
        remote_key = f"{prefix}/{record.id}{_ARCHIVE_SUFFIX}"
        # Admission must precede any read/hash of the payload.
        if self.availability is not None and self.availability(record.id) != PayloadState.PRESENT.value:
            raise SafeError("backup_protection_failed", "local backup payload is not available")
        digest = _sha256(record.path)
        if not record.verified or not record.path.is_file():
            self._save(record, backup_class, remote_key, digest, "not_started", False, "not_started", "not_started", "local_unverified")
            raise SafeError("backup_protection_failed", "local backup is not verified")
        self._save(record, backup_class, remote_key, digest, "pending", False, "pending", "not_started", None)
        try:
            self.transport.upload(record.path, remote_key)
        except SafeError:
            self._save(record, backup_class, remote_key, digest, "failed", False, "failed", "not_started", "upload_failed")
            raise
        except Exception as exc:
            self._save(record, backup_class, remote_key, digest, "failed", False, "failed", "not_started", "upload_failed")
            raise SafeError("backup_protection_failed", "remote backup upload failed") from exc
        self._save(record, backup_class, remote_key, digest, "succeeded", False, "pending", "not_started", None)
        try:
            self.transport.verify(record.path, remote_key)
        except SafeError:
            self._save(record, backup_class, remote_key, digest, "succeeded", False, "failed", "not_started", "remote_check_failed")
            raise
        except Exception as exc:
            self._save(record, backup_class, remote_key, digest, "succeeded", False, "failed", "not_started", "remote_check_failed")
            raise SafeError("backup_protection_failed", "remote backup verification failed") from exc
        self._save(record, backup_class, remote_key, digest, "succeeded", True, "verified", "pending", None)
        try:
            self._prune(record, backup_class, prefix, remote_key)
        except SafeError:
            self._save(record, backup_class, remote_key, digest, "succeeded", True, "verified", "failed", "prune_failed")
            raise
        except Exception as exc:
            self._save(record, backup_class, remote_key, digest, "succeeded", True, "verified", "failed", "prune_failed")
            raise SafeError("backup_protection_failed", "remote backup pruning failed") from exc
        return self._save(record, backup_class, remote_key, digest, "succeeded", True, "verified", "succeeded", None)

    def _prune(self, current: BackupRecord, backup_class: BackupClass, prefix: str, current_key: str) -> None:
        objects = tuple(self.transport.list(prefix))
        exact = {item.key: item for item in objects if item.key.startswith(prefix + "/")}
        records = [
            item for item in self._records(current.profile_id, backup_class)
            if item.remote_verified
            and item.comparison_state == "verified"
            and item.remote_key in exact
            and item.remote_key.startswith(prefix + "/")
        ]
        if not any(item.remote_key == current_key for item in records):
            raise SafeError("backup_protection_failed", "new remote generation is not verified")
        records.sort(key=lambda item: _backup_generation(item.backup_id), reverse=True)
        # Local protection controls only the local archive. Remote retention is
        # always exactly the newest verified generations in this owned prefix.
        for item in records[self.destination.retention :]:
            self.transport.delete(item.remote_key)
            self._save_by_protection(item, prune_state="deleted")

    def _records(self, profile_id: str, backup_class: BackupClass) -> list[ProtectionRecord]:
        if self.database is not None and hasattr(self.database, "connection"):
            rows = self.database.connection.execute(
                "SELECT backup_id,profile_id,destination_id,backup_class,remote_key,local_sha256,"
                "local_verified,upload_state,remote_verified,comparison_state,prune_state,error_code "
                "FROM backup_protections WHERE profile_id=? AND destination_id=? AND backup_class=?",
                (profile_id, self.destination.destination_id.value, backup_class.value),
            ).fetchall()
            return [self._row(row) for row in rows]
        return [item for item in self._memory.values() if item.profile_id == profile_id and item.backup_class is backup_class]

    def _local_records(self, profile_id: str) -> list[BackupRecord]:
        if self.database is not None and hasattr(self.database, "list_backups"):
            result = []
            for item in self.database.list_backups(profile_id):
                if isinstance(item, BackupRecord):
                    result.append(item)
                elif hasattr(item, "keys"):
                    result.append(BackupRecord(
                        id=str(item["id"]), profile_id=str(item["profile_id"]),
                        created_at=datetime.fromisoformat(str(item["created_at"]).replace("Z", "+00:00")),
                        size_bytes=int(item["size_bytes"]), verified=bool(item["verified"]),
                        protected=bool(item["protected"]), path=Path(str(item.get("path", ""))),
                    ))
                else:
                    result.append(BackupRecord(
                        id=str(item[0]), profile_id=str(item[1]),
                        created_at=datetime.fromisoformat(str(item[2]).replace("Z", "+00:00")),
                        size_bytes=int(item[3]), verified=bool(item[4]), protected=bool(item[5]), path=Path(""),
                    ))
            return result
        if self.database is not None and hasattr(self.database, "connection"):
            rows = self.database.connection.execute(
                "SELECT id,profile_id,created_at,size_bytes,verified,protected FROM backups WHERE profile_id=?",
                (profile_id,),
            ).fetchall()
            return [BackupRecord(
                id=str(row[0]), profile_id=str(row[1]),
                created_at=datetime.fromisoformat(str(row[2]).replace("Z", "+00:00")),
                size_bytes=int(row[3]), verified=bool(row[4]), protected=bool(row[5]), path=Path(""),
            ) for row in rows]
        return []

    def _row(self, row: Any) -> ProtectionRecord:
        return ProtectionRecord(
            backup_id=str(row[0]), profile_id=str(row[1]), destination_id=BackupDestination(row[2]),
            backup_class=BackupClass(row[3]), remote_key=str(row[4]), local_sha256=str(row[5]),
            local_verified=bool(row[6]), upload_state=str(row[7]), remote_verified=bool(row[8]),
            comparison_state=str(row[9]), prune_state=str(row[10]), error_code=row[11],
        )

    def _save(
        self,
        record: BackupRecord,
        backup_class: BackupClass,
        remote_key: str,
        digest: str,
        upload_state: str,
        remote_verified: bool,
        comparison_state: str,
        prune_state: str,
        error_code: str | None,
    ) -> ProtectionRecord:
        value = ProtectionRecord(
            backup_id=record.id, profile_id=record.profile_id, destination_id=self.destination.destination_id,
            backup_class=backup_class, remote_key=remote_key, local_sha256=digest, local_verified=record.verified,
            upload_state=upload_state, remote_verified=remote_verified, comparison_state=comparison_state,
            prune_state=prune_state, error_code=error_code,
        )
        self._memory[(record.id, self.destination.destination_id.value, backup_class.value)] = value
        if self.database is not None and hasattr(self.database, "connection"):
            self.database.connection.execute(
                "INSERT INTO backup_protections(backup_id,profile_id,destination_id,backup_class,remote_key,"
                "local_sha256,local_verified,upload_state,remote_verified,comparison_state,prune_state,updated_at,error_code) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(backup_id,destination_id,backup_class) DO UPDATE SET "
                "remote_key=excluded.remote_key,local_sha256=excluded.local_sha256,local_verified=excluded.local_verified,"
                "upload_state=excluded.upload_state,remote_verified=excluded.remote_verified,comparison_state=excluded.comparison_state,"
                "prune_state=excluded.prune_state,updated_at=excluded.updated_at,error_code=excluded.error_code",
                (value.backup_id, value.profile_id, value.destination_id.value, value.backup_class.value, value.remote_key,
                 value.local_sha256, int(value.local_verified), value.upload_state, int(value.remote_verified),
                 value.comparison_state, value.prune_state, _iso(self.clock()), value.error_code),
            )
            self.database.connection.commit()
        return value

    def _save_by_protection(self, value: ProtectionRecord, *, prune_state: str) -> None:
        updated = ProtectionRecord(**{**value.__dict__, "prune_state": prune_state})
        self._memory[(value.backup_id, value.destination_id.value, value.backup_class.value)] = updated
        if self.database is not None and hasattr(self.database, "connection"):
            self.database.connection.execute(
                "UPDATE backup_protections SET prune_state=?,updated_at=? WHERE backup_id=? AND destination_id=? AND backup_class=?",
                (prune_state, _iso(self.clock()), value.backup_id, value.destination_id.value, value.backup_class.value),
            )
            self.database.connection.commit()


def _backup_generation(backup_id: str) -> str:
    return backup_id.split("-", 1)[0]


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


@dataclass(frozen=True)
class BackupRecord:
    id: str
    profile_id: str
    created_at: datetime
    size_bytes: int
    verified: bool
    protected: bool
    path: Path


@dataclass(frozen=True)
class RestoreResult:
    backup_id: str
    rollback: Path | None
    destination: Path
    rollbacks: tuple[Path | None, ...] = ()
    destinations: tuple[Path, ...] = ()
    journal: Path | None = None


@dataclass(frozen=True)
class _RestoreTarget:
    path: Path
    uid: int
    gid: int
    device: int
    inode: int
    parent_device: int = 0
    parent_inode: int = 0
    parent_uid: int = 0
    parent_gid: int = 0


class BackupService:
    """Create, enumerate, protect, and prune verified profile archives."""

    def __init__(
        self,
        profile: Any,
        *,
        database: Any | None = None,
        stopped_check: Callable[[], bool] | None = None,
        free_space: Callable[[Path], int] | None = None,
        clock: Callable[[], datetime] | None = None,
        tar_runner: Callable[..., Any] | None = None,
        protection_service: B2ProtectionService | None = None,
        online_transport: OnlineSaveTransport | None = None,
        telemetry_db: Any | None = None,
        lease_check: Callable[[], bool] | None = None,
    ) -> None:
        self.profile = profile
        self.database = database
        self.stopped_check = stopped_check
        self.free_space = free_space or (lambda path: shutil.disk_usage(path).free)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.tar_runner = tar_runner or subprocess.run
        self.protection_service = protection_service
        self.online_transport = online_transport
        self.telemetry_db = telemetry_db
        self.lease_check = lease_check

    def _assert_lease(self) -> None:
        if self.lease_check is not None and not self.lease_check():
            raise SafeError("slot_conflict", "operation lease was lost before backup publication", retryable=True)

    @property
    def backup_root(self) -> Path:
        return Path(self.profile.paths.backup_root)

    def create(
        self,
        action: Any | None = None,
        actor: str | None = None,
        request_id: Any | None = None,
        *,
        protected: bool | None = None,
        destination: BackupDestination | str | None = None,
    ) -> BackupRecord:
        if self.stopped_check is None or not self.stopped_check():
            raise SafeError("profile_running", "profile is running; it must be stopped before backup")
        if protected is None:
            protected = bool(getattr(action, "protected", False))
        if destination is None:
            destination = getattr(action, "destination", BackupDestination.LOCAL)
        destination = validate_destination(destination, str(self.profile.id))
        if destination is BackupDestination.HORIZON_B2 and (
            self.protection_service is None or self.protection_service.database is None
        ):
            raise SafeError("backup_protection_failed", "durable backup state is unavailable")
        configured_backup_roots = tuple(
            Path(root) for root in getattr(self.profile.paths, "backup_roots", ())
        )
        roots = configured_backup_roots or tuple(
            Path(root) for root in self.profile.paths.data_roots
        )
        self.backup_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.backup_root, 0o700)
        estimated = self._estimate(roots)
        if self.free_space(self.backup_root) < max(1, int(estimated * _MARGIN)):
            raise SafeError("insufficient_space", "insufficient free space for backup")

        now = self.clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        backup_id = f"{now.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:12]}"
        partial = self.backup_root / f"{backup_id}.partial"
        final = self.backup_root / f"{backup_id}{_ARCHIVE_SUFFIX}"
        staging = Path(tempfile.mkdtemp(prefix=".stage-", dir=self.backup_root))
        catalog_inserted = False
        remote_started = False
        try:
            os.chmod(staging, 0o700)
            entries = self._snapshot(roots, staging)
            manifest = {
                "schema": 2 if len(roots) > 1 else 1,
                "profile_id": str(self.profile.id),
                "backup_id": backup_id,
                "created_at": now.isoformat(),
                "entries": entries,
            }
            if len(roots) > 1:
                manifest["roots"] = [{"id": _root_id(root), "identity": _root_identity(root)} for root in roots]
            manifest_path = staging / _MANIFEST
            _write_json_fsync(manifest_path, manifest)
            filelist = staging / ".filelist"
            paths = [_MANIFEST] + [entry["archive_path"] for entry in entries]
            _write_filelist(filelist, paths)
            argv = [
                "/usr/bin/tar",
                "--zstd",
                "--create",
                "--file",
                str(partial),
                "--directory",
                str(staging),
                "--null",
                "--files-from",
                str(filelist),
            ]
            self.tar_runner(maintenance_argv(argv, slice_name="maintenance.slice"), check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            _fsync_file(partial)
            self._verify_archive(partial, manifest)
            self._assert_lease()
            os.replace(partial, final)
            _fsync_dir(self.backup_root)
            record = BackupRecord(
                id=backup_id,
                profile_id=str(self.profile.id),
                created_at=now,
                size_bytes=final.stat().st_size,
                verified=True,
                protected=bool(protected),
                path=final,
            )
            self._assert_lease()
            self._insert(record)
            catalog_inserted = True
            if destination is BackupDestination.HORIZON_B2:
                self._assert_lease()
                if self.protection_service is None:
                    raise SafeError("backup_protection_failed", "remote backup service unavailable")
                remote_started = True
                self.protection_service.protect(record)
                self._assert_lease()
            self._assert_lease()
            return record
        except SafeError:
            partial.unlink(missing_ok=True)
            if not remote_started:
                final.unlink(missing_ok=True)
                if catalog_inserted:
                    self._delete(backup_id)
            _fsync_dir(self.backup_root)
            raise
        except (OSError, subprocess.SubprocessError, tarfile.TarError, ValueError) as exc:
            partial.unlink(missing_ok=True)
            if not remote_started:
                final.unlink(missing_ok=True)
                if catalog_inserted:
                    self._delete(backup_id)
            _fsync_dir(self.backup_root)
            raise SafeError("backup_failed", "backup could not be verified") from exc
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    def create_online(
        self,
        action: Any | None = None,
        actor: str | None = None,
        request_id: Any | None = None,
        *,
        protected: bool | None = None,
        destination: BackupDestination | str | None = None,
        max_snapshot_seconds: float = 120.0,
    ) -> BackupRecord:
        """Create a Sunlit backup while briefly quiescing Minecraft saves.

        The save-on call is attempted after every save-off attempt, including
        failed flush/copy branches. Compression and remote protection do not
        begin until the game is writing again.
        """
        quiesce_started = time.monotonic()
        quiesce_success = False
        if self.online_transport is None:
            raise SafeError("backup_quiesce_failed", "online backup transport is unavailable")
        if str(getattr(self.profile.id, "value", self.profile.id)) != "minecraft-sunlit-cobblemon":
            raise SafeError("backup_quiesce_failed", "online backup is not approved for this profile")
        if protected is None:
            protected = bool(getattr(action, "protected", False))
        if destination is None:
            destination = getattr(action, "destination", BackupDestination.LOCAL)
        destination = validate_destination(destination, str(self.profile.id))
        if destination is BackupDestination.HORIZON_B2 and (
            self.protection_service is None or self.protection_service.database is None
        ):
            raise SafeError("backup_protection_failed", "durable backup state is unavailable")
        roots = tuple(Path(root) for root in getattr(self.profile.paths, "backup_roots", ()))
        roots = roots or tuple(Path(root) for root in self.profile.paths.data_roots)
        self.backup_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.backup_root, 0o700)
        estimated = self._estimate(roots)
        if self.free_space(self.backup_root) < max(1, int(estimated * _MARGIN)):
            raise SafeError("insufficient_space", "insufficient free space for backup")
        now = self.clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        backup_id = f"{now.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:12]}"
        partial = self.backup_root / f"{backup_id}.partial"
        final = self.backup_root / f"{backup_id}{_ARCHIVE_SUFFIX}"
        staging = Path(tempfile.mkdtemp(prefix=".online-stage-", dir=self.backup_root))
        catalog_inserted = False
        remote_started = False
        failure: BaseException | None = None
        entries: list[dict[str, Any]] = []
        try:
            os.chmod(staging, 0o700)
            deadline = time.monotonic() + max(1.0, min(float(max_snapshot_seconds), 900.0))
            try:
                self._call_online("save_off")
                self._call_online("save_all_flush")
                entries = self._snapshot(roots, staging, copy_files=True, deadline=deadline)
            except BaseException as exc:
                failure = exc
            try:
                self._call_online("save_on")
            except BaseException as exc:
                failure = exc
            if failure is not None:
                raise SafeError("backup_quiesce_failed", "online backup quiesce could not be completed") from failure
            quiesce_success = True

            manifest = {
                "schema": 2 if len(roots) > 1 else 1,
                "profile_id": str(self.profile.id),
                "backup_id": backup_id,
                "created_at": now.isoformat(),
                "online_consistent": True,
                "entries": entries,
            }
            if len(roots) > 1:
                manifest["roots"] = [{"id": _root_id(root), "identity": _root_identity(root)} for root in roots]
            manifest_path = staging / _MANIFEST
            _write_json_fsync(manifest_path, manifest)
            filelist = staging / ".filelist"
            _write_filelist(filelist, [_MANIFEST] + [entry["archive_path"] for entry in entries])
            argv = [
                "/usr/bin/tar", "--zstd", "--create", "--file", str(partial),
                "--directory", str(staging), "--null", "--files-from", str(filelist),
            ]
            self.tar_runner(maintenance_argv(argv, slice_name="maintenance.slice"), check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            _fsync_file(partial)
            self._verify_archive(partial, manifest)
            self._assert_lease()
            os.replace(partial, final)
            _fsync_dir(self.backup_root)
            record = BackupRecord(
                id=backup_id,
                profile_id=str(self.profile.id),
                created_at=now,
                size_bytes=final.stat().st_size,
                verified=True,
                protected=bool(protected),
                path=final,
            )
            self._assert_lease()
            self._insert(record)
            catalog_inserted = True
            if destination is BackupDestination.HORIZON_B2:
                self._assert_lease()
                if self.protection_service is None:
                    raise SafeError("backup_protection_failed", "remote backup service unavailable")
                remote_started = True
                self.protection_service.protect(record)
                self._assert_lease()
            self._assert_lease()
            return record
        except SafeError:
            partial.unlink(missing_ok=True)
            if not remote_started:
                final.unlink(missing_ok=True)
                if catalog_inserted:
                    self._delete(backup_id)
            _fsync_dir(self.backup_root)
            raise
        except (OSError, subprocess.SubprocessError, tarfile.TarError, ValueError) as exc:
            partial.unlink(missing_ok=True)
            if not remote_started:
                final.unlink(missing_ok=True)
                if catalog_inserted:
                    self._delete(backup_id)
            _fsync_dir(self.backup_root)
            raise SafeError("backup_failed", "backup could not be verified") from exc
        finally:
            recorder = getattr(self.telemetry_db, "enqueue_sample", None)
            if callable(recorder):
                try:
                    recorder(
                        self.profile.id,
                        "backup_quiesce_duration",
                        min(900_000.0, max(0.0, (time.monotonic() - quiesce_started) * 1000.0)),
                        ts_ms=int(time.time() * 1000),
                        state="available",
                        labels={"result": "success" if quiesce_success else "failure", "source": "slotd"},
                    )
                except Exception:
                    pass
            shutil.rmtree(staging, ignore_errors=True)

    def _call_online(self, method: str) -> Any:
        value = getattr(self.online_transport, method)()
        if inspect.isawaitable(value):
            return asyncio.run(value)
        return value

    def list(self, action: Any | None = None, actor: str | None = None, request_id: Any | None = None):
        rows = self._rows()
        records = tuple(self._record(row) for row in rows if self._record(row) is not None)
        page = getattr(action, "page", None)
        if page is not None:
            from .protocol import BackupPage

            limit = int(getattr(page, "limit", 50))
            return BackupPage(
                items=tuple(
                    __import__("game_control.protocol", fromlist=["BackupSummary"]).BackupSummary(
                        id=item.id,
                        profile_id=item.profile_id,
                        created_at=item.created_at,
                        size_bytes=item.size_bytes,
                        verified=item.verified,
                        protected=item.protected,
                    )
                    for item in records[:limit]
                ),
                next_cursor=None,
            )
        return records

    def protect(self, backup_id: str, protected: bool = True) -> BackupRecord:
        record = self._find(backup_id)
        if record is None:
            raise SafeError("backup_not_found", "backup was not found")
        if self.database is not None and hasattr(self.database, "protect_backup"):
            self.database.protect_backup(backup_id, protected)
        elif self.database is not None and hasattr(self.database, "connection"):
            self.database.connection.execute(
                "UPDATE backups SET protected = ? WHERE id = ? AND profile_id = ?",
                (int(protected), backup_id, str(self.profile.id)),
            )
            self.database.connection.commit()
        return BackupRecord(**{**record.__dict__, "protected": protected})

    def prune(self, keep: int = 2) -> tuple[BackupRecord, ...]:
        records = [record for record in self.list() if record.verified and record.path.exists()]
        records.sort(key=lambda item: item.created_at, reverse=True)
        protected = [item for item in records if item.protected]
        retained = list(protected)
        for item in records:
            if item not in retained and len(retained) < max(2, keep):
                retained.append(item)
        candidates = [item for item in records if item not in retained]
        if len(records) == 1 and candidates:
            raise SafeError("backup_retention", "refusing to delete the only verified backup")
        # Fail closed for the whole batch as soon as any candidate is
        # catalog-backed: the legacy path has no lease, no ledger, and no
        # reference protection, so deleting a catalogued payload cannot be made
        # safe here.  Only a purely filesystem-level archive may be pruned.
        self._assert_legacy_prune_safe(candidates)
        for item in candidates:
            item.path.unlink(missing_ok=True)
            self._delete(item.id)
        if records and not any(item.path.exists() for item in retained):
            raise SafeError("backup_retention", "refusing to delete the only verified backup")
        return tuple(sorted((item for item in retained if item.path.exists()), key=lambda x: x.created_at, reverse=True))

    def _assert_legacy_prune_safe(self, candidates: list[BackupRecord]) -> None:
        if not candidates or self.database is None:
            return
        connection = getattr(self.database, "connection", None)
        if connection is None:
            # A durable catalog exists but cannot be inspected: fail closed.
            raise SafeError(
                "backup_retention",
                "backup catalog could not be inspected; local prune is disabled",
            )
        catalogued = [
            item.id
            for item in candidates
            if connection.execute(
                "SELECT 1 FROM backups WHERE id = ? LIMIT 1", (item.id,)
            ).fetchone()
            is not None
        ]
        if catalogued:
            raise SafeError(
                "backup_retention",
                "catalog-backed archives require the retirement operation",
            )

    def _estimate(self, roots: Iterable[Path]) -> int:
        total = 0
        for root in roots:
            if not root.exists() or root.is_symlink():
                continue
            for path in self._walk_source(root):
                try:
                    total += path.stat(follow_symlinks=False).st_size
                except OSError as exc:
                    raise SafeError("backup_failed", "backup source could not be read") from exc
        return total + 65536

    def _walk_source(self, root: Path) -> Iterable[Path]:
        profile_id = str(getattr(self.profile.id, "value", self.profile.id))
        mutable_root = Path(self.profile.paths.mutable_root)
        excluded = (
            _SUNLIT_REDUNDANT_BACKUP_DIRS
            if profile_id == ProfileId.MINECRAFT_SUNLIT_COBBLEMON.value and root == mutable_root
            else frozenset()
        )
        paths = _walk(root, excluded_top_level=excluded)
        if profile_id != ProfileId.MINECRAFT_SUNLIT_COBBLEMON.value or root != mutable_root:
            return paths

        def without_versioned_server_backups() -> Iterable[Path]:
            for path in paths:
                relative = path.relative_to(root)
                parts = relative.parts
                if len(parts) >= 4 and parts[0] == ".versions" and parts[2] == "backups":
                    continue
                yield path

        return without_versioned_server_backups()

    def _snapshot(
        self,
        roots: tuple[Path, ...],
        staging: Path,
        *,
        copy_files: bool = False,
        deadline: float | None = None,
    ) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for index, root in enumerate(roots):
            root_id = _root_id(root)
            if root.is_symlink():
                raise SafeError("backup_failed", "profile data root is a symlink")
            payload_root = staging / "payload" / (root_id if len(roots) > 1 else "")
            payload_root.mkdir(parents=True, mode=0o700)
            if not root.exists():
                continue
            for source in self._walk_source(root):
                if deadline is not None and time.monotonic() > deadline:
                    raise SafeError("backup_quiesce_timeout", "online backup staging exceeded its bound")
                relative = source.relative_to(root)
                if any(part.endswith(".partial") for part in relative.parts):
                    continue
                if source.is_symlink():
                    continue
                destination = payload_root / relative
                destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                try:
                    if copy_files:
                        digest = _stage_file_copy(source, destination, deadline=deadline)
                        _check_deadline(deadline)
                        os.chmod(destination, 0o400)
                        _check_deadline(deadline)
                        metadata_path = destination
                    else:
                        _stage_file(source, destination)
                        metadata_path = source
                        digest = _sha256(source)
                    source_info = metadata_path.stat(follow_symlinks=False)
                except OSError as exc:
                    raise SafeError("backup_failed", "backup source could not be read") from exc
                entries.append(
                    {
                        "path": relative.as_posix(),
                        "root_id": root_id,
                        "archive_path": (f"payload/{relative.as_posix()}" if len(roots) == 1 else f"payload/{root_id}/{relative.as_posix()}"),
                        "size": source_info.st_size,
                        "mode": stat.S_IMODE(source_info.st_mode),
                        "uid": source_info.st_uid,
                        "gid": source_info.st_gid,
                        "sha256": digest,
                    }
                )
        entries.sort(key=lambda item: item["path"])
        return entries

    def _verify_archive(self, archive_path: Path, manifest: dict[str, Any]) -> None:
        try:
            with tarfile.open(archive_path, mode="r:*") as archive:
                members = archive.getmembers()
                for member in members:
                    _validate_member(member)
                names = {member.name for member in members}
                if _MANIFEST not in names:
                    raise SafeError("backup_failed", "backup manifest is missing")
                expected = {_MANIFEST} | {entry["archive_path"] for entry in manifest["entries"]}
                if names != expected:
                    raise SafeError("backup_failed", "backup archive contents changed")
                for entry in manifest["entries"]:
                    member = archive.getmember(entry["archive_path"])
                    if not member.isfile():
                        raise SafeError("backup_failed", "backup contains an invalid member")
                    uid, gid = _entry_owner(entry, error_code="backup_failed")
                    if member.uid != uid or member.gid != gid:
                        raise SafeError("backup_failed", "backup ownership metadata changed")
                    stream = archive.extractfile(member)
                    if stream is None or _hash_stream(stream) != entry["sha256"]:
                        raise SafeError("backup_failed", "backup checksum verification failed")
        except tarfile.TarError:
            # Python builds without libzstd use one fixed decompressor process
            # and consume its tar stream sequentially; never spawn per-file
            # extractors for large worlds.
            _verify_zstd_stream(archive_path, manifest)
        except (KeyError, TypeError) as exc:
            raise SafeError("backup_failed", "backup archive could not be verified") from exc

    def _insert(self, record: BackupRecord) -> None:
        if self.database is None:
            return
        row = {
            "id": record.id,
            "profile_id": record.profile_id,
            "created_at": record.created_at.isoformat(),
            "size_bytes": record.size_bytes,
            "verified": record.verified,
            "protected": record.protected,
            "path": str(record.path),
        }
        if hasattr(self.database, "insert_backup"):
            self.database.insert_backup(**row)
        elif hasattr(self.database, "connection"):
            self.database.connection.execute(
                "INSERT INTO backups(id, profile_id, created_at, size_bytes, verified, protected) VALUES(?,?,?,?,?,?)",
                (
                    record.id,
                    record.profile_id,
                    record.created_at.isoformat(),
                    record.size_bytes,
                    int(record.verified),
                    int(record.protected),
                ),
            )
            self.database.connection.commit()

    def _rows(self) -> list[Any]:
        if self.database is not None and hasattr(self.database, "list_backups"):
            return list(self.database.list_backups(str(self.profile.id)))
        if self.database is not None and hasattr(self.database, "connection"):
            return list(
                self.database.connection.execute(
                    "SELECT id, profile_id, created_at, size_bytes, verified, protected FROM backups WHERE profile_id = ? ORDER BY created_at DESC",
                    (str(self.profile.id),),
                )
            )
        return [
            {
                "id": path.name.removesuffix(_ARCHIVE_SUFFIX),
                "profile_id": str(self.profile.id),
                "created_at": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
                "size_bytes": path.stat().st_size,
                "verified": True,
                "protected": False,
                "path": str(path),
            }
            for path in self.backup_root.glob(f"*{_ARCHIVE_SUFFIX}")
        ]

    def _record(self, row: Any) -> BackupRecord | None:
        if isinstance(row, BackupRecord):
            return row
        if hasattr(row, "keys"):
            value = row
            get = value.__getitem__
        else:
            names = ("id", "profile_id", "created_at", "size_bytes", "verified", "protected", "path")
            value = dict(zip(names, row))
            get = value.__getitem__
        path_value = value.get("path") if hasattr(value, "get") else None
        path = Path(path_value) if path_value else self.backup_root / f"{get('id')}{_ARCHIVE_SUFFIX}"
        created = get("created_at")
        if isinstance(created, str):
            created = datetime.fromisoformat(created.replace("Z", "+00:00"))
        return BackupRecord(
            id=str(get("id")),
            profile_id=str(get("profile_id")),
            created_at=created,
            size_bytes=int(get("size_bytes")),
            verified=bool(get("verified")),
            protected=bool(get("protected")),
            path=path,
        )

    def _find(self, backup_id: str) -> BackupRecord | None:
        return next((item for item in self.list() if item.id == backup_id), None)

    def _delete(self, backup_id: str) -> None:
        if self.database is not None and hasattr(self.database, "delete_backup"):
            self.database.delete_backup(backup_id)
        elif self.database is not None and hasattr(self.database, "connection"):
            self.database.connection.execute("DELETE FROM backups WHERE id = ?", (backup_id,))
            self.database.connection.commit()


class RestoreService:
    def __init__(
        self,
        profile: Any,
        *,
        backup_service: BackupService,
        stopped_check: Callable[[], bool] | None = None,
        free_space: Callable[[Path], int] | None = None,
        health_check: Callable[[Path], bool] | None = None,
        lease_check: Callable[[], bool] | None = None,
        availability_check: Callable[[str], str] | None = None,
    ) -> None:
        self.profile = profile
        self.backup_service = backup_service
        self.stopped_check = stopped_check
        self.free_space = free_space or (lambda path: shutil.disk_usage(path).free)
        self.health_check = health_check
        self.lease_check = lease_check
        self.availability_check = availability_check

    def _assert_available(self, backup_id: str) -> None:
        if self.availability_check is None:
            return
        if self.availability_check(backup_id) != PayloadState.PRESENT.value:
            raise SafeError("backup_not_found", "backup payload is not locally available")

    def _assert_lease(self) -> None:
        if self.lease_check is not None and not self.lease_check():
            raise SafeError("slot_conflict", "operation lease was lost before publication")

    @property
    def backup_root(self) -> Path:
        return Path(self.profile.paths.backup_root)

    def restore(
        self,
        archive: str | os.PathLike[str],
        actor: str | None = None,
        request_id: Any | None = None,
        **_: Any,
    ) -> RestoreResult:
        if self.stopped_check is None or not self.stopped_check():
            raise SafeError("profile_running", "profile is running; it must be stopped before restore")
        archive_path = Path(archive)
        root = Path(self.profile.paths.backup_root)
        if archive_path.is_symlink() or not _within(archive_path, root):
            raise SafeError("invalid_backup", "backup archive is not approved")
        self._assert_available(archive_path.name.removesuffix(_ARCHIVE_SUFFIX))
        if not archive_path.is_file():
            raise SafeError("backup_not_found", "backup archive was not found")
        try:
            try:
                source: Any = tarfile.open(archive_path, mode="r:*")
                external = False
            except tarfile.TarError:
                source = _ExternalArchive(archive_path)
                external = True
            with source:
                members, manifest = (
                    self._validate_external(source)
                    if external
                    else self._validate_archive(source)
                )
                targets = self._read_targets(manifest)
                required_by_root = {root_id: 65536 for root_id in targets}
                for item in manifest["entries"]:
                    root_id = item.get("root_id", "root-0")
                    if root_id not in targets and len(targets) == 1:
                        root_id = next(iter(targets))
                    required_by_root[root_id] = required_by_root.get(root_id, 65536) + int(item.get("size", 0))
                if any(self.free_space(targets[root_id].path.parent) < int(amount * _MARGIN) for root_id, amount in required_by_root.items()):
                    raise SafeError("insufficient_space", "insufficient free space for restore")
                # Validation is complete before the pre-restore backup or any
                # destination path is created.
                self.backup_service.create(protected=True)
                stagings = {root_id: Path(tempfile.mkdtemp(prefix=f".restore-{root_id}-", dir=target.path.parent)) for root_id, target in targets.items()}
                journal = self.backup_root / f".restore-journal-{uuid.uuid4().hex}.json"
                _write_json_fsync(journal, {"phase": "staged", "backup_id": manifest["backup_id"], "roots": [{"root_id": rid, "destination": str(target.path), "staging": str(stagings[rid]), "rollback": None, "original_exists": target.inode != 0} for rid, target in targets.items()]})
                for staging in stagings.values():
                    os.chmod(staging, 0o700)
                try:
                    if external:
                        self._extract_external_multi(source, manifest, stagings, targets)
                    else:
                        self._extract_multi(source, members, manifest, stagings, targets)
                    for root_id, staging in stagings.items():
                        os.chmod(staging, 0o750)
                        _chown_tree(staging, targets[root_id].uid, targets[root_id].gid)
                        self._assert_target_unchanged(targets[root_id])
                    rollbacks: list[Path | None] = []
                    rollback_by_root: dict[str, Path | None] = {}
                    destinations = [target.path for target in targets.values()]
                    displaced: list[str] = []
                    activated: list[str] = []
                    try:
                        for root_id, target in targets.items():
                            rollback = target.path.parent / f".rollback-{uuid.uuid4().hex}"
                            rollback_by_root[root_id] = rollback
                            _write_json_fsync(journal, {"phase": "displacing", "backup_id": manifest["backup_id"], "displaced": list(displaced), "roots": [{"root_id": rid, "destination": str(targets[rid].path), "staging": str(stagings[rid]), "rollback": str(rollback_by_root.get(rid)) if rollback_by_root.get(rid) else None, "original_exists": targets[rid].inode != 0} for rid in targets]})
                            self._assert_lease()
                            if target.path.exists():
                                os.replace(target.path, rollback)
                            else:
                                rollback_by_root[root_id] = None
                            rollbacks.append(rollback_by_root[root_id])
                            displaced.append(root_id)
                            _write_json_fsync(journal, {"phase": "displacing", "backup_id": manifest["backup_id"], "displaced": list(displaced), "roots": [{"root_id": rid, "destination": str(targets[rid].path), "staging": str(stagings[rid]), "rollback": str(rollback_by_root.get(rid)) if rollback_by_root.get(rid) else None, "original_exists": targets[rid].inode != 0} for rid in targets]})
                        _write_json_fsync(journal, {"phase": "displaced", "backup_id": manifest["backup_id"], "roots": [{"root_id": rid, "destination": str(targets[rid].path), "staging": str(stagings[rid]), "rollback": str(rollback_by_root[rid]) if rollback_by_root[rid] else None, "original_exists": targets[rid].inode != 0} for rid in targets]})
                        for root_id, target in targets.items():
                            _write_json_fsync(journal, {"phase": "publishing", "backup_id": manifest["backup_id"], "activated": list(activated), "roots": [{"root_id": rid, "destination": str(targets[rid].path), "staging": str(stagings[rid]), "rollback": str(rollback_by_root[rid]) if rollback_by_root[rid] else None, "original_exists": targets[rid].inode != 0} for rid in targets]})
                            self._assert_lease()
                            os.replace(stagings[root_id], target.path)
                            activated.append(root_id)
                            _write_json_fsync(journal, {"phase": "publishing", "backup_id": manifest["backup_id"], "activated": list(activated), "roots": [{"root_id": rid, "destination": str(targets[rid].path), "staging": str(stagings[rid]), "rollback": str(rollback_by_root[rid]) if rollback_by_root[rid] else None, "original_exists": targets[rid].inode != 0} for rid in targets]})
                    except Exception:
                        for root_id in activated:
                            target = targets[root_id]
                            if target.path.exists():
                                shutil.rmtree(target.path, ignore_errors=True)
                        for root_id in displaced:
                            target = targets[root_id]
                            rollback = rollback_by_root[root_id]
                            if rollback is not None and rollback.exists():
                                os.replace(rollback, target.path)
                        raise
                    destination = targets[next(iter(targets))].path
                    result = RestoreResult(
                        backup_id=str(manifest["backup_id"]),
                        rollback=next((item for item in rollbacks if item is not None), None),
                        destination=destination,
                        rollbacks=tuple(rollbacks),
                        destinations=tuple(destinations),
                        journal=journal,
                    )
                    _write_json_fsync(journal, {"phase": "activated", "backup_id": manifest["backup_id"], "roots": [{"root_id": rid, "destination": str(targets[rid].path), "staging": str(stagings[rid]), "rollback": str(rollback_by_root[rid]) if rollback_by_root[rid] else None, "original_exists": targets[rid].inode != 0} for rid in targets]})
                    if self.health_check is not None and all(
                        self.health_check(item) for item in destinations
                    ):
                        self.finalize(result)
                    return result
                except SafeError:
                    for staging in stagings.values():
                        shutil.rmtree(staging, ignore_errors=True)
                    raise
                finally:
                    for staging in stagings.values():
                        if staging.exists():
                            shutil.rmtree(staging, ignore_errors=True)
        except SafeError:
            raise
        except (OSError, tarfile.TarError, ValueError, KeyError) as exc:
            raise SafeError("restore_failed", "restore could not be completed") from exc

    def finalize(self, result: RestoreResult) -> None:
        for rollback in result.rollbacks or ((result.rollback,) if result.rollback is not None else ()):
            if rollback is None:
                continue
            shutil.rmtree(rollback, ignore_errors=True)
            if rollback.exists():
                raise SafeError("restore_finalize_failed", "restore rollback cleanup failed")
            _fsync_dir(rollback.parent)
        if result.journal is not None:
            try:
                result.journal.unlink(missing_ok=True)
            except OSError as exc:
                raise SafeError("restore_finalize_failed", "restore journal cleanup failed") from exc
            _fsync_dir(self.backup_root)

    def rollback(self, result: RestoreResult) -> None:
        destinations = result.destinations or ((result.destination,) if result.destination is not None else ())
        rollbacks = result.rollbacks or ((result.rollback,) if result.rollback is not None else ())
        for destination, rollback in zip(destinations, rollbacks):
            if rollback is None:
                if destination.exists() or destination.is_symlink():
                    shutil.rmtree(destination, ignore_errors=True)
                    _fsync_dir(destination.parent)
                continue
            if rollback is not None and rollback.exists():
                if destination.exists():
                    shutil.rmtree(destination)
                os.replace(rollback, destination)
                _fsync_dir(destination.parent)
        if result.journal is not None:
            result.journal.unlink(missing_ok=True)

    def reconcile(self) -> None:
        """Finish or roll back interrupted restore publications idempotently."""
        for journal in self.backup_root.glob(".restore-journal-*.json"):
            try:
                info = os.lstat(journal)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_gid != 0 or
                    stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
                    raise SafeError("restore_reconcile_failed", "restore journal is not trusted")
                fd = os.open(journal, os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    bound = os.fstat(fd)
                    if bound.st_dev != info.st_dev or bound.st_ino != info.st_ino or bound.st_size > 1_048_576:
                        raise SafeError("restore_reconcile_failed", "restore journal changed")
                    record = json.loads(os.read(fd, bound.st_size).decode("utf-8"))
                finally:
                    os.close(fd)
                phase, roots = self._validate_journal(record)
                if phase == "staged":
                    for item in roots:
                        staging = Path(item["staging"])
                        if staging.exists():
                            shutil.rmtree(staging, ignore_errors=True)
                elif phase in {"displacing", "displaced", "publishing", "activated"}:
                    if phase == "activated" and self.health_check is not None and all(self.health_check(Path(item["destination"])) for item in roots):
                        for item in roots:
                            if item.get("rollback"):
                                rollback = Path(item["rollback"])
                                shutil.rmtree(rollback, ignore_errors=True)
                                if rollback.exists():
                                    raise SafeError("restore_reconcile_failed", "restore rollback cleanup failed")
                    else:
                        activated = set(record.get("activated", []))
                        displaced = set(record.get("displaced", []))
                        for item in roots:
                            destination = Path(item["destination"])
                            rollback = item.get("rollback")
                            if rollback and Path(rollback).exists() and (phase not in {"publishing", "displacing"} or item["root_id"] in activated or item["root_id"] in displaced or not destination.exists()):
                                if destination.exists():
                                    shutil.rmtree(destination, ignore_errors=True)
                                os.replace(rollback, destination)
                            elif not item.get("original_exists", True) and (item["root_id"] in activated or item["root_id"] in displaced):
                                if destination.exists() or destination.is_symlink():
                                    shutil.rmtree(destination, ignore_errors=True)
                                    _fsync_dir(destination.parent)
                            staging = Path(item["staging"])
                            if staging.exists():
                                shutil.rmtree(staging, ignore_errors=True)
                else:
                    continue
                journal.unlink(missing_ok=True)
                if journal.exists():
                    raise SafeError("restore_reconcile_failed", "restore journal cleanup failed")
                _fsync_dir(self.backup_root)
            except SafeError:
                raise
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                raise SafeError("restore_reconcile_failed", "restore journal is corrupt") from exc

    def _validate_journal(self, record: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
        if not isinstance(record, dict) or record.get("backup_id") is None:
            raise SafeError("restore_reconcile_failed", "restore journal is corrupt")
        phase = record.get("phase")
        if phase not in {"staged", "displacing", "displaced", "publishing", "activated"}:
            raise SafeError("restore_reconcile_failed", "restore journal phase is invalid")
        approved = self._approved_roots()
        roots = record.get("roots")
        if not isinstance(roots, list) or len(roots) != len(approved):
            raise SafeError("restore_reconcile_failed", "restore journal roots are invalid")
        expected = {str(path): _root_id(path) for path in approved}
        seen: set[str] = set()
        for item in roots:
            if not isinstance(item, dict) or item.get("root_id") in seen:
                raise SafeError("restore_reconcile_failed", "restore journal roots are invalid")
            rid, destination = item.get("root_id"), item.get("destination")
            if rid not in set(expected.values()) or destination not in expected or expected[destination] != rid:
                raise SafeError("restore_reconcile_failed", "restore journal destination is invalid")
            if not isinstance(item.get("original_exists"), bool):
                raise SafeError("restore_reconcile_failed", "restore journal original state is invalid")
            parent = Path(destination).parent
            for field in ("staging", "rollback"):
                value = item.get(field)
                if value is None and field == "rollback":
                    continue
                candidate = Path(value)
                prefix = f".restore-{rid}-" if field == "staging" else ".rollback-"
                if candidate.parent != parent or not candidate.name.startswith(prefix) or candidate.name == prefix:
                    raise SafeError("restore_reconcile_failed", "restore journal path is invalid")
            seen.add(rid)
        if seen != set(expected.values()):
            raise SafeError("restore_reconcile_failed", "restore journal roots are incomplete")
        for field in ("activated", "displaced"):
            values = record.get(field, [])
            if not isinstance(values, list) or len(values) != len(set(values)) or not set(values).issubset(seen):
                raise SafeError("restore_reconcile_failed", "restore journal state is invalid")
        return phase, roots

    def _approved_roots(self) -> tuple[Path, ...]:
        roots = tuple(Path(root) for root in getattr(self.profile.paths, "backup_roots", ()))
        return roots or tuple(Path(root) for root in self.profile.paths.data_roots)

    def _read_targets(self, manifest: dict[str, Any]) -> dict[str, _RestoreTarget]:
        roots = self._approved_roots()
        schema = manifest.get("schema")
        if schema == 1:
            if len(roots) != 1:
                raise SafeError("invalid_backup", "legacy backup is ambiguous for multiple roots")
            root_ids = ["root-0"]
        elif schema == 2:
            descriptors = manifest.get("roots")
            roots_by_identity = {_root_identity(root): root for root in roots}
            identities = set(roots_by_identity)
            valid_descriptors = (
                isinstance(descriptors, list)
                and all(isinstance(item, dict) for item in descriptors)
            )
            if not valid_descriptors:
                raise SafeError("invalid_backup", "backup root mapping is invalid")
            if any(not isinstance(item.get("identity"), str) for item in descriptors):
                raise SafeError("invalid_backup", "backup root mapping is invalid")
            descriptor_identities = {item.get("identity") for item in descriptors}
            if (
                descriptor_identities != identities
                or len(descriptor_identities) != len(roots)
                or any(
                    not isinstance(item.get("id"), str)
                    or item["id"] != _root_id(roots_by_identity[item["identity"]])
                    for item in descriptors
                )
            ):
                raise SafeError("invalid_backup", "backup root mapping is invalid")
            root_ids = [str(item["id"]) for item in descriptors]
            roots = tuple(roots_by_identity[str(item["identity"])] for item in descriptors)
        else:
            raise SafeError("invalid_backup", "unsupported backup schema")
        result = {}
        for root_id, root in zip(root_ids, roots):
            target = self._read_target(root)
            result[root_id] = target
        return result

    def _read_target(self, destination: Path | None = None) -> _RestoreTarget:
        destination = destination or Path(self.profile.paths.mutable_root)
        try:
            info = os.lstat(destination)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                try:
                    parent = os.lstat(destination.parent)
                except OSError as parent_exc:
                    raise SafeError("invalid_destination", "restore destination is unavailable") from parent_exc
                return _RestoreTarget(destination, parent.st_uid, parent.st_gid, parent.st_dev, 0, parent.st_dev, parent.st_ino, parent.st_uid, parent.st_gid)
            raise SafeError("invalid_destination", "restore destination is unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise SafeError("invalid_destination", "restore destination is not a directory")
        if info.st_nlink < 2:
            raise SafeError("invalid_destination", "restore destination has unsafe links")
        try:
            child_directories = sum(
                entry.is_dir(follow_symlinks=False) for entry in os.scandir(destination)
            )
        except OSError as exc:
            raise SafeError("invalid_destination", "restore destination cannot be trusted") from exc
        if info.st_nlink != 2 + child_directories:
            raise SafeError("invalid_destination", "restore destination has unsafe links")
        uid = _bounded_id(info.st_uid, error_code="invalid_destination")
        gid = _bounded_id(info.st_gid, error_code="invalid_destination")
        parent = os.lstat(destination.parent)
        return _RestoreTarget(destination, uid, gid, info.st_dev, info.st_ino, parent.st_dev, parent.st_ino, parent.st_uid, parent.st_gid)

    def _assert_target_unchanged(self, target: _RestoreTarget) -> None:
        if target.inode == 0:
            try:
                parent = os.lstat(target.path.parent)
            except OSError as exc:
                raise SafeError("invalid_destination", "restore destination changed") from exc
            if (target.path.exists() or target.path.is_symlink() or parent.st_dev != target.parent_device or parent.st_ino != target.parent_inode or parent.st_uid != target.parent_uid or parent.st_gid != target.parent_gid):
                raise SafeError("invalid_destination", "restore destination changed")
            return
        try:
            info = os.lstat(target.path)
        except OSError as exc:
            raise SafeError("invalid_destination", "restore destination changed") from exc
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_dev != target.device
            or info.st_ino != target.inode
            or info.st_uid != target.uid
            or info.st_gid != target.gid
        ):
            raise SafeError("invalid_destination", "restore destination changed")
        parent = os.lstat(target.path.parent)
        if (parent.st_dev, parent.st_ino, parent.st_uid, parent.st_gid) != (target.parent_device, target.parent_inode, target.parent_uid, target.parent_gid):
            raise SafeError("invalid_destination", "restore destination changed")

    def _validate_archive(self, archive: tarfile.TarFile) -> tuple[list[tarfile.TarInfo], dict[str, Any]]:
        members = archive.getmembers()
        if len({member.name for member in members}) != len(members):
            raise SafeError("invalid_backup", "backup contains duplicate members")
        for member in members:
            _validate_member(member)
        manifest_member = next((member for member in members if member.name == _MANIFEST), None)
        if manifest_member is None:
            raise SafeError("invalid_backup", "backup manifest is missing")
        stream = archive.extractfile(manifest_member)
        if stream is None:
            raise SafeError("invalid_backup", "backup manifest is invalid")
        try:
            manifest = json.load(stream)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise SafeError("invalid_backup", "backup manifest is invalid") from exc
        if manifest.get("schema") not in (1, 2) or manifest.get("profile_id") != str(self.profile.id):
            raise SafeError("wrong_profile", "backup belongs to another profile")
        entries = manifest.get("entries")
        if not isinstance(entries, list):
            raise SafeError("invalid_backup", "backup manifest is invalid")
        if any(not isinstance(entry, dict) for entry in entries):
            raise SafeError("invalid_backup", "backup manifest is invalid")
        by_name = {member.name: member for member in members}
        for entry in entries:
            archive_name = entry.get("archive_path")
            if not isinstance(archive_name, str) or archive_name not in by_name:
                raise SafeError("invalid_backup", "backup manifest is invalid")
            relative_path = entry.get("path")
            if (
                not isinstance(relative_path, str)
                or not relative_path
                or "\x00" in relative_path
                or PurePosixPath(relative_path).is_absolute()
                or ".." in PurePosixPath(relative_path).parts
            ):
                raise SafeError("invalid_backup", "backup manifest contains an unsafe path")
            member = by_name[archive_name]
            if manifest.get("schema") == 2 and entry.get("root_id") not in {item.get("id") for item in manifest.get("roots", ())}:
                raise SafeError("invalid_backup", "backup root mapping is invalid")
            if member.issym() or member.islnk() or not member.isfile():
                raise SafeError("invalid_backup", "backup contains an invalid member")
            uid, gid = _entry_owner(entry, error_code="ownership_mismatch")
            if archive_name == _MANIFEST or member.uid != uid or member.gid != gid:
                raise SafeError("ownership_mismatch", "backup ownership does not match profile")
            if int(entry.get("size", -1)) != int(member.size):
                raise SafeError("checksum_mismatch", "backup size verification failed")
            content = archive.extractfile(member)
            if content is None or _hash_stream(content) != entry.get("sha256"):
                raise SafeError("checksum_mismatch", "backup checksum verification failed")
        if (
            len({entry["archive_path"] for entry in entries}) != len(entries)
            or any(entry["archive_path"] == _MANIFEST for entry in entries)
            or set(by_name) != {_MANIFEST} | {entry["archive_path"] for entry in entries}
        ):
            raise SafeError("invalid_backup", "backup manifest does not match archive")
        if manifest.get("schema") == 2 and {entry.get("root_id") for entry in entries} - {item.get("id") for item in manifest.get("roots", ())}:
            raise SafeError("invalid_backup", "backup root mapping is invalid")
        if not isinstance(manifest.get("backup_id"), str) or not manifest["backup_id"]:
            raise SafeError("invalid_backup", "backup manifest is invalid")
        return members, manifest

    def _extract(
        self,
        archive: tarfile.TarFile,
        members: list[tarfile.TarInfo],
        manifest: dict[str, Any],
        staging: Path,
        uid: int,
        gid: int,
    ) -> None:
        entries = {entry["archive_path"]: entry for entry in manifest["entries"]}
        for member in members:
            if member.name == _MANIFEST:
                continue
            entry = entries[member.name]
            relative = entry["path"]
            if manifest.get("schema") == 1 and relative.startswith("0/"):
                relative = relative[2:]
            target = staging / PurePosixPath(relative)
            if not _within(target, staging):
                raise SafeError("invalid_backup", "backup path escapes destination")
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
            os.chown(target.parent, uid, gid, follow_symlinks=False)
            source = archive.extractfile(member)
            if source is None:
                raise SafeError("restore_failed", "backup member could not be read")
            with target.open("xb") as destination:
                shutil.copyfileobj(source, destination)
            os.chmod(target, 0o640)
            os.chown(target, uid, gid, follow_symlinks=False)

    def _extract_multi(
        self, archive: tarfile.TarFile, members: list[tarfile.TarInfo], manifest: dict[str, Any],
        stagings: dict[str, Path], targets: dict[str, _RestoreTarget],
    ) -> None:
        if manifest.get("schema") == 1:
            return self._extract(archive, members, manifest, stagings["root-0"], targets["root-0"].uid, targets["root-0"].gid)
        entries = {entry["archive_path"]: entry for entry in manifest["entries"]}
        for member in members:
            if member.name == _MANIFEST:
                continue
            entry = entries[member.name]
            root_id = entry["root_id"]
            staging = stagings[root_id]
            relative = entry["path"]
            if manifest.get("schema") == 1 and relative.startswith("0/"):
                relative = relative[2:]
            target = staging / PurePosixPath(relative)
            if not _within(target, staging):
                raise SafeError("invalid_backup", "backup path escapes destination")
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
            os.chown(target.parent, targets[root_id].uid, targets[root_id].gid, follow_symlinks=False)
            source = archive.extractfile(member)
            if source is None:
                raise SafeError("restore_failed", "backup member could not be read")
            with target.open("xb") as destination:
                shutil.copyfileobj(source, destination)
            os.chmod(target, 0o640)
            os.chown(target, targets[root_id].uid, targets[root_id].gid, follow_symlinks=False)

    def _validate_external(self, archive: "_ExternalArchive") -> tuple[list["_ExternalMember"], dict[str, Any]]:
        members = archive.members
        if len({member.name for member in members}) != len(members):
            raise SafeError("invalid_backup", "backup contains duplicate members")
        by_name = {member.name: member for member in members}
        if _MANIFEST not in by_name:
            raise SafeError("invalid_backup", "backup manifest is missing")
        try:
            manifest = json.loads(archive.read(_MANIFEST))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise SafeError("invalid_backup", "backup manifest is invalid") from exc
        if manifest.get("schema") not in (1, 2) or manifest.get("profile_id") != str(self.profile.id):
            raise SafeError("wrong_profile", "backup belongs to another profile")
        entries = manifest.get("entries")
        if not isinstance(entries, list):
            raise SafeError("invalid_backup", "backup manifest is invalid")
        if any(not isinstance(entry, dict) for entry in entries):
            raise SafeError("invalid_backup", "backup manifest is invalid")
        for entry in entries:
            archive_name = entry.get("archive_path")
            relative_path = entry.get("path")
            if (
                not isinstance(archive_name, str)
                or archive_name not in by_name
                or not isinstance(relative_path, str)
                or not relative_path
                or PurePosixPath(relative_path).is_absolute()
                or ".." in PurePosixPath(relative_path).parts
            ):
                raise SafeError("invalid_backup", "backup manifest contains an unsafe path")
            member = by_name[archive_name]
            if manifest.get("schema") == 2 and entry.get("root_id") not in {item.get("id") for item in manifest.get("roots", ())}:
                raise SafeError("invalid_backup", "backup root mapping is invalid")
            if not member.isfile:
                raise SafeError("invalid_backup", "backup contains an invalid member")
            uid, gid = _entry_owner(entry, error_code="ownership_mismatch")
            if member.uid != uid or member.gid != gid:
                raise SafeError("ownership_mismatch", "backup ownership does not match profile")
            if member.size != int(entry.get("size", -1)):
                raise SafeError("checksum_mismatch", "backup size verification failed")
            with archive.open(archive_name) as content:
                digest = _hash_stream(content)
            if digest != entry.get("sha256"):
                raise SafeError("checksum_mismatch", "backup checksum verification failed")
        if (
            len({entry.get("archive_path") for entry in entries}) != len(entries)
            or any(entry.get("archive_path") == _MANIFEST for entry in entries)
            or set(by_name) != {_MANIFEST} | {entry.get("archive_path") for entry in entries}
        ):
            raise SafeError("invalid_backup", "backup manifest does not match archive")
        if manifest.get("schema") == 2 and {entry.get("root_id") for entry in entries} - {item.get("id") for item in manifest.get("roots", ())}:
            raise SafeError("invalid_backup", "backup root mapping is invalid")
        if not isinstance(manifest.get("backup_id"), str) or not manifest["backup_id"]:
            raise SafeError("invalid_backup", "backup manifest is invalid")
        return members, manifest

    def _extract_external(
        self,
        archive: "_ExternalArchive",
        manifest: dict[str, Any],
        staging: Path,
        uid: int,
        gid: int,
    ) -> None:
        for entry in manifest["entries"]:
            target = staging / PurePosixPath(entry["path"])
            if not _within(target, staging):
                raise SafeError("invalid_backup", "backup path escapes destination")
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
            os.chown(target.parent, uid, gid, follow_symlinks=False)
            with archive.open(entry["archive_path"]) as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination)
            os.chmod(target, 0o640)
            os.chown(target, uid, gid, follow_symlinks=False)

    def _extract_external_multi(
        self, archive: "_ExternalArchive", manifest: dict[str, Any],
        stagings: dict[str, Path], targets: dict[str, _RestoreTarget],
    ) -> None:
        if manifest.get("schema") == 1:
            return self._extract_external(archive, manifest, stagings["root-0"], targets["root-0"].uid, targets["root-0"].gid)
        for entry in manifest["entries"]:
            root_id = entry["root_id"]
            staging = stagings[root_id]
            target = staging / PurePosixPath(entry["path"])
            if not _within(target, staging):
                raise SafeError("invalid_backup", "backup path escapes destination")
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
            os.chown(target.parent, targets[root_id].uid, targets[root_id].gid, follow_symlinks=False)
            with archive.open(entry["archive_path"]) as source, target.open("xb") as destination:
                shutil.copyfileobj(source, destination)
            os.chmod(target, 0o640)
            os.chown(target, targets[root_id].uid, targets[root_id].gid, follow_symlinks=False)


def _walk(root: Path, *, excluded_top_level: frozenset[str] = frozenset()) -> Iterable[Path]:
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as scan:
                children = sorted(scan, key=lambda item: item.name, reverse=True)
                for item in children:
                    if current == root and item.name in excluded_top_level:
                        continue
                    if item.name.endswith(".partial"):
                        continue
                    path = Path(item.path)
                    if item.is_symlink():
                        continue
                    if item.is_dir(follow_symlinks=False):
                        stack.append(path)
                    elif item.is_file(follow_symlinks=False):
                        yield path
        except OSError as exc:
            raise SafeError("backup_failed", "backup source could not be read") from exc


def _stage_file(source: Path, destination: Path) -> None:
    """Snapshot a stopped source without copying multi-gigabyte worlds."""

    def copy_with_owner() -> None:
        source_info = source.stat(follow_symlinks=False)
        shutil.copy2(source, destination, follow_symlinks=False)
        # copy2 preserves mode and timestamps, but not numeric ownership.  A
        # VM with separate bind mounts for mutable and backup roots takes this
        # fallback even when both binds share one underlying filesystem.
        os.chown(
            destination,
            source_info.st_uid,
            source_info.st_gid,
            follow_symlinks=False,
        )

    try:
        os.link(source, destination, follow_symlinks=False)
    except (TypeError, NotImplementedError):
        copy_with_owner()
    except OSError as exc:
        if exc.errno not in (errno.EXDEV, errno.EOPNOTSUPP, errno.ENOTSUP, errno.EPERM):
            raise
        copy_with_owner()


def _stage_file_copy(source: Path, destination: Path, *, deadline: float | None = None) -> str:
    """Copy an online-quiesced file; hardlinks would keep a live inode."""
    source_info = source.stat(follow_symlinks=False)
    digest = hashlib.sha256()
    with source.open("rb") as source_stream, destination.open("wb") as destination_stream:
        while True:
            _check_deadline(deadline)
            chunk = source_stream.read(1024 * 1024)
            if not chunk:
                break
            destination_stream.write(chunk)
            digest.update(chunk)
        destination_stream.flush()
        os.fsync(destination_stream.fileno())
    _check_deadline(deadline)
    os.chmod(destination, stat.S_IMODE(source_info.st_mode), follow_symlinks=False)
    os.utime(
        destination,
        ns=(source_info.st_atime_ns, source_info.st_mtime_ns),
        follow_symlinks=False,
    )
    os.chown(destination, source_info.st_uid, source_info.st_gid, follow_symlinks=False)
    return digest.hexdigest()


def _validate_member(member: tarfile.TarInfo) -> None:
    name = member.name
    path = PurePosixPath(name)
    if not name or "\x00" in name or path.is_absolute() or ".." in path.parts:
        raise SafeError("invalid_backup", "backup contains an unsafe path")
    if member.issym() or member.islnk() or member.isdev() or member.isfifo() or member.ischr() or member.isblk():
        raise SafeError("invalid_backup", "backup contains an unsafe member")
    if not member.isfile() and name != _MANIFEST:
        raise SafeError("invalid_backup", "backup contains an unsupported member")
    _bounded_id(member.uid, error_code="invalid_backup")
    _bounded_id(member.gid, error_code="invalid_backup")


def _bounded_id(value: Any, *, error_code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _MAX_ID:
        raise SafeError(error_code, "ownership metadata is invalid")
    return value


def _entry_owner(entry: dict[str, Any], *, error_code: str) -> tuple[int, int]:
    return (
        _bounded_id(entry.get("uid"), error_code=error_code),
        _bounded_id(entry.get("gid"), error_code=error_code),
    )


def _chown_tree(root: Path, uid: int, gid: int) -> None:
    pending = [root]
    paths = []
    while pending:
        current = pending.pop()
        try:
            info = os.lstat(current)
        except OSError as exc:
            raise SafeError("restore_failed", "staged restore is not trustworthy") from exc
        if stat.S_ISLNK(info.st_mode) or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            raise SafeError("restore_failed", "staged restore contains an unsafe member")
        paths.append(current)
        if stat.S_ISDIR(info.st_mode):
            try:
                children = list(os.scandir(current))
            except OSError as exc:
                raise SafeError("restore_failed", "staged restore is not trustworthy") from exc
            for child in children:
                pending.append(Path(child.path))
    for path in reversed(paths):
        try:
            os.chown(path, uid, gid, follow_symlinks=False)
        except OSError as exc:
            raise SafeError("restore_failed", "staged restore ownership could not be set") from exc


def _within(path: Path, root: Path) -> bool:
    try:
        path.absolute().relative_to(root.absolute())
        return True
    except ValueError:
        return False


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return _hash_stream(stream)


def _check_deadline(deadline: float | None) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        raise SafeError("backup_quiesce_timeout", "online backup staging exceeded its bound")


def _hash_stream(stream: Any) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def _write_json_fsync(path: Path, value: Any) -> None:
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    os.chmod(path, 0o600)
    _fsync_dir(path.parent)


def _write_filelist(path: Path, values: Iterable[str]) -> None:
    with path.open("wb") as stream:
        for value in values:
            stream.write(value.encode() + b"\0")
        stream.flush()
        os.fsync(stream.fileno())


def _fsync_file(path: Path) -> None:
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def _fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _verify_zstd_stream(archive_path: Path, manifest: dict[str, Any]) -> None:
    expected = {_MANIFEST} | {entry["archive_path"] for entry in manifest["entries"]}
    entries = {entry["archive_path"]: entry for entry in manifest["entries"]}
    for entry in manifest["entries"]:
        _entry_owner(entry, error_code="backup_failed")
    process: subprocess.Popen[bytes] | None = None
    seen: set[str] = set()
    try:
        process = maintenance_popen(
            ["/usr/bin/zstd", "-dc", "--", str(archive_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if process.stdout is None:
            raise SafeError("backup_failed", "backup decompressor was unavailable")
        with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
            for member in archive:
                if len(seen) >= 1_000_000 or len(member.name) > 4096:
                    raise SafeError("backup_failed", "backup archive is too large")
                if member.name in seen:
                    raise SafeError("backup_failed", "backup contains duplicate members")
                seen.add(member.name)
                _validate_member(member)
                if member.name == _MANIFEST:
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise SafeError("backup_failed", "backup manifest is missing")
                    json.loads(stream.read())
                    continue
                entry = entries.get(member.name)
                if entry is None or not member.isfile() or int(member.size) != int(entry["size"]):
                    raise SafeError("backup_failed", "backup archive contents changed")
                uid, gid = _entry_owner(entry, error_code="backup_failed")
                if member.uid != uid or member.gid != gid:
                    raise SafeError("backup_failed", "backup ownership metadata changed")
                stream = archive.extractfile(member)
                if stream is None or _hash_stream(stream) != entry["sha256"]:
                    raise SafeError("backup_failed", "backup checksum verification failed")
        returncode = process.wait()
        if returncode != 0:
            raise SafeError("backup_failed", "backup decompression failed")
        if seen != expected:
            raise SafeError("backup_failed", "backup archive contents changed")
    except SafeError:
        if process is not None and process.poll() is None:
            process.kill()
        if process is not None:
            process.wait()
        raise
    except (OSError, subprocess.SubprocessError, tarfile.TarError, json.JSONDecodeError) as exc:
        if process is not None and process.poll() is None:
            process.kill()
        if process is not None:
            process.wait()
        raise SafeError("backup_failed", "backup archive could not be verified") from exc


@dataclass(frozen=True)
class _ExternalMember:
    name: str
    isfile: bool
    size: int
    uid: int
    gid: int
    staged_path: Path | None


class _ExternalArchive:
    """Read a zstd tar stream once for Python builds lacking native zstd."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._staging = Path(tempfile.mkdtemp(prefix=".zstd-", dir=path.parent))
        process: subprocess.Popen[bytes] | None = None
        try:
            process = maintenance_popen(
                ["/usr/bin/zstd", "-dc", "--", str(path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if process.stdout is None:
                raise SafeError("invalid_backup", "backup decompressor was unavailable")
            members: list[_ExternalMember] = []
            seen: set[str] = set()
            with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
                for member in archive:
                    if len(seen) >= 1_000_000 or len(member.name) > 4096:
                        raise SafeError("invalid_backup", "backup archive is too large")
                    if member.name in seen:
                        raise SafeError("invalid_backup", "backup contains duplicate members")
                    seen.add(member.name)
                    _validate_member(member)
                    staged_path: Path | None = None
                    if member.isfile():
                        staged_path = self._staging / str(len(members))
                        with staged_path.open("wb") as output:
                            source = archive.extractfile(member)
                            if source is None:
                                raise SafeError("invalid_backup", "backup member could not be read")
                            shutil.copyfileobj(source, output)
                    members.append(
                        _ExternalMember(
                            member.name,
                            member.isfile(),
                            int(member.size),
                            int(member.uid),
                            int(member.gid),
                            staged_path,
                        )
                    )
            if process.wait() != 0:
                raise SafeError("invalid_backup", "backup decompression failed")
            self.members = tuple(members)
        except SafeError:
            if process is not None and process.poll() is None:
                process.kill()
            if process is not None:
                process.wait()
            self.close()
            raise
        except (OSError, subprocess.SubprocessError, tarfile.TarError) as exc:
            if process is not None and process.poll() is None:
                process.kill()
            if process is not None:
                process.wait()
            self.close()
            raise SafeError("invalid_backup", "backup archive could not be read") from exc

    def __enter__(self) -> "_ExternalArchive":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def open(self, name: str):
        member = next((item for item in self.members if item.name == name), None)
        if member is None or member.staged_path is None:
            raise SafeError("invalid_backup", "backup member could not be read")
        return member.staged_path.open("rb")

    def read(self, name: str) -> bytes:
        with self.open(name) as source:
            return source.read()

    def close(self) -> None:
        shutil.rmtree(self._staging, ignore_errors=True)


class BackupRpcFacade:
    """Typed RPC translator; backup policy remains in ``BackupService``."""

    def __init__(
        self,
        profiles: Mapping[str, Any],
        adapters: Mapping[Any, Any],
        database: Any,
        *,
        b2_transport: Any | None = None,
        sunlit_online_backup: Any | None = None,
        telemetry_db: Any | None = None,
        backup_service_factory: Callable[..., BackupService] = BackupService,
        restore_service_factory: Callable[..., RestoreService] = RestoreService,
        isolated_database_factory: Callable[[Any], Any | None] = _rpc_database_path,
        close_database: Callable[[Any | None], None] = _rpc_close_database,
        retirement_manifest_dir: Path | str = MANIFEST_DIR,
        sender_lock_path: Path | str = SENDER_LOCK_PATH,
    ) -> None:
        self.profiles = profiles
        self.adapters = adapters
        self.database = database
        self.b2_transport = b2_transport or B2CommandTransport()
        self.sunlit_online_backup = sunlit_online_backup
        self._backup_service_factory = backup_service_factory
        self._restore_service_factory = restore_service_factory
        self._isolated_database_factory = isolated_database_factory
        self._close_database = close_database
        self._retirement_manifest_dir = Path(retirement_manifest_dir)
        self._sender_lock_path = Path(sender_lock_path)
        self.retirement = RetirementService(
            profiles,
            database,
            manifest_dir=self._retirement_manifest_dir,
            sender_lock_path=self._sender_lock_path,
        )
        self.protection = B2ProtectionService(
            database=database,
            transport=self.b2_transport,
            availability=lambda backup_id: self._availability(backup_id),
        )
        self._profile_locks = {key: asyncio.Lock() for key in profiles}
        self.services = {
            key: backup_service_factory(
                profile,
                database=database,
                stopped_check=lambda profile=profile: self._stopped_sync(profile),
                protection_service=self.protection,
                telemetry_db=telemetry_db,
            )
            for key, profile in profiles.items()
        }
        self.restores = {
            key: restore_service_factory(
                profile,
                backup_service=self.services[key],
                stopped_check=lambda profile=profile: self._stopped_sync(profile),
                availability_check=lambda backup_id: self._availability(backup_id),
            )
            for key, profile in profiles.items()
        }

    def _retirement_service(self, database: Any) -> RetirementService:
        return RetirementService(
            self.profiles,
            database,
            manifest_dir=self._retirement_manifest_dir,
            sender_lock_path=self._sender_lock_path,
        )

    def _availability_with(self, database: Any, backup_id: str) -> str:
        """Fail-closed availability for one catalog payload on a given database."""

        connection = getattr(database, "connection", None)
        profile: Any | None = None
        if connection is not None:
            row = connection.execute(
                "SELECT profile_id FROM backups WHERE id = ? LIMIT 1", (backup_id,)
            ).fetchone()
            if row is not None:
                profile = self.profiles.get(str(row[0]))
        else:
            # No durable ledger is configured (narrow injected seams only).
            # Production facades always carry a StateDatabase connection.
            for key, service in self.services.items():
                lister = getattr(service, "list", None)
                if not callable(lister):
                    continue
                try:
                    records = lister()
                except Exception:
                    continue
                if any(getattr(record, "id", None) == backup_id for record in records):
                    profile = self.profiles.get(key)
                    break
        if profile is None:
            # Fail closed only when a real ledger says nothing exists; a
            # durable-less test seam keeps its historical presence default.
            return PayloadState.MISSING.value if connection is not None else PayloadState.PRESENT.value
        return self._retirement_service(database).payload_state(profile, backup_id)

    def _availability(self, backup_id: str) -> str:
        return self._availability_with(self.database, backup_id)

    def availability(self, backup_id: str) -> str:
        """Public fail-closed availability seam for the controller."""

        return self._availability(backup_id)

    def _service(self, profile_id: Any) -> tuple[Any, BackupService]:
        key = _rpc_key(profile_id)
        try:
            return self.profiles[key], self.services[key]
        except KeyError as exc:
            raise SafeError("profile_not_found", "profile was not found") from exc

    async def _stopped(self, profile: Any) -> None:
        adapter = self.adapters.get(getattr(profile, "id", None)) or self.adapters.get(_rpc_key(profile))
        if adapter is None or not hasattr(adapter, "observe"):
            raise SafeError("profile_unavailable", "profile state could not be proven")
        try:
            value = adapter.observe(profile)
            observation = await value if inspect.isawaitable(value) else value
        except Exception as exc:
            raise SafeError("profile_unavailable", "profile state could not be proven") from exc
        if bool(getattr(observation, "running", False)):
            raise SafeError("profile_running", "profile is running; it must be stopped before backup")

    def _stopped_sync(self, profile: Any) -> bool:
        adapter = self.adapters.get(getattr(profile, "id", None)) or self.adapters.get(_rpc_key(profile))
        if adapter is None or not hasattr(adapter, "observe"):
            raise SafeError("profile_unavailable", "profile state could not be proven")
        try:
            value = adapter.observe(profile)
            if inspect.isawaitable(value):
                value = asyncio.run(value)
        except SafeError:
            raise
        except Exception as exc:
            raise SafeError("profile_unavailable", "profile state could not be proven") from exc
        if bool(getattr(value, "running", False)):
            raise SafeError("profile_running", "profile is running; it must be stopped before backup")
        return True

    async def list(self, action: Any, actor: str | None = None, request_id: Any = None) -> BackupPage:
        profile, service = self._service(action.profile_id)
        result = service.list(action, actor, request_id)
        if isinstance(result, BackupPage):
            # The typed service page carries the catalog default (present); the
            # ledger-aware availability lookup belongs on this path too.
            return BackupPage(
                items=tuple(
                    item.model_copy(update={
                        "local_payload_state": self._availability(str(getattr(item, "id", "")))}
                    )
                    for item in result.items
                ),
                next_cursor=result.next_cursor,
            )
        return BackupPage(items=tuple(
            BackupSummary(
                id=str(getattr(record, "id", "")), profile_id=profile.id,
                created_at=_rpc_timestamp(getattr(record, "created_at", None)), size_bytes=max(0, int(getattr(record, "size_bytes", 0))),
                verified=bool(getattr(record, "verified", False)), protected=bool(getattr(record, "protected", False)),
                local_payload_state=self._availability(str(getattr(record, "id", ""))),
            ) for record in result
        ), next_cursor=None)

    def retirement_status(self, operation_id: str | None = None):
        from .protocol import (
            RetirementEntryStatus,
            RetirementReconcileEntry,
            RetirementStatus,
        )

        report = self.retirement.status(operation_id)
        # Read-only probe: never creates or truncates the sender lock file.
        lock_available = SenderInterlock(self._sender_lock_path).probe()
        return RetirementStatus(
            operation_id=report.operation_id,
            counts=report.counts,
            entries=tuple(
                RetirementEntryStatus(
                    backup_id=str(item["backup_id"]),
                    profile_id=str(item["profile_id"]),
                    state=str(item["state"]),
                    error_code=item.get("error_code"),
                )
                for item in report.entries
            ),
            reconciled=tuple(
                RetirementReconcileEntry(
                    backup_id=str(item["backup_id"]),
                    profile_id=str(item["profile_id"]),
                    ledger_state=str(item["ledger_state"]),
                    classification=str(item["classification"]),
                )
                for item in report.reconciled
            ),
            sender_lock_available=lock_available,
            noreplace_supported=noreplace_supported(),
        )

    def retirement_prepare(self, manifest_sha256: str, phase: str) -> dict[str, Any]:
        """Synchronous engine prepare; the async facade wrapper offloads it."""

        return self.retirement.prepare(manifest_sha256, phase)

    async def retirement_prepare_async(self, manifest_sha256: str, phase: str, *, profile_ids=None) -> dict[str, Any]:
        """Prepare off the event loop with a fresh isolated database."""

        def work() -> dict[str, Any]:
            worker_db = self._isolated_database_factory(self.database)
            database = worker_db if worker_db is not None else self.database
            service = self._retirement_service(database)
            try:
                return service.prepare(manifest_sha256, phase)
            finally:
                if worker_db is not None:
                    self._close_database(worker_db)

        return await asyncio.to_thread(work)

    async def retirement_confirm(
        self,
        action: Any = None,
        actor: str | None = None,
        request_id: Any = None,
        lease_check: Any = None,
        payload: Mapping[str, Any] | None = None,
        job_id: str | None = None,
    ):
        """Execute one confirmed retirement phase off the event loop."""

        payload = payload or {}
        operation_id = str(payload.get("operation_id", ""))
        phase = str(payload.get("phase", ""))
        if phase not in RETIREMENT_PHASES:
            raise SafeError("retirement_failed", "retirement phase is not approved")
        if lease_check is None:
            # Production retirement runs under the controller maintenance lease;
            # refuse to mutate payloads without a lease assertion.
            raise SafeError("retirement_failed", "operation lease is required")

        def work():
            worker_db = self._isolated_database_factory(self.database)
            if worker_db is None:
                raise SafeError("retirement_failed", "durable ledger isolation is unavailable")
            service = self._retirement_service(worker_db)
            service.lease_check = lease_check
            try:
                operation = {
                    "quarantine": service.quarantine,
                    "purge": service.purge,
                    "rollback": service.rollback,
                }[phase]
                return operation(
                    operation_id,
                    own_job_id=job_id,
                    own_confirmation_id=str(payload.get("confirmation_id") or "") or None,
                )
            finally:
                self._close_database(worker_db)

        return await asyncio.to_thread(work)

    def reconcile_startup_retirement(self) -> list[dict[str, Any]]:
        """Read-only ledger classification; never mutates filesystem or ledger."""

        return self.retirement.reconcile()

    async def create(self, action: Any, actor: str | None = None, request_id: Any = None, lease_check: Any = None) -> JobAccepted:
        profile, service = self._service(action.profile_id)
        async with self._profile_locks.setdefault(_rpc_key(profile), asyncio.Lock()):
            adapter = self.adapters.get(getattr(profile, "id", None)) or self.adapters.get(_rpc_key(profile))
            if adapter is None or not hasattr(adapter, "observe"):
                raise SafeError("profile_unavailable", "profile state could not be proven")
            try:
                observed = adapter.observe(profile)
                observed = await observed if inspect.isawaitable(observed) else observed
            except Exception as exc:
                raise SafeError("profile_unavailable", "profile state could not be proven") from exc
            online = bool(getattr(observed, "running", False))
            sunlit = _rpc_key(profile) == ProfileId.MINECRAFT_SUNLIT_COBBLEMON.value
            if online and (not sunlit or self.sunlit_online_backup is None):
                raise SafeError("profile_running", "profile is running; it must be stopped before backup")
            if not online:
                await self._stopped(profile)
            def work():
                worker_db = self._isolated_database_factory(self.database)
                if action.destination is BackupDestination.HORIZON_B2 and worker_db is None:
                    raise SafeError("backup_protection_failed", "durable backup state is unavailable")
                worker = self._backup_service_factory(
                    profile, database=worker_db, stopped_check=lambda: self._stopped_sync(profile),
                    free_space=service.free_space, clock=service.clock, tar_runner=service.tar_runner,
                    protection_service=B2ProtectionService(
                        database=worker_db,
                        transport=self.b2_transport,
                        clock=service.clock,
                        availability=(
                            (lambda backup_id: self._availability_with(worker_db, backup_id))
                            if worker_db is not None
                            else None
                        ),
                    ),
                    lease_check=lease_check,
                )
                try:
                    if online:
                        worker.online_transport = self.sunlit_online_backup
                        result = worker.create_online(action, actor, request_id, protected=bool(action.protected), max_snapshot_seconds=240.0)
                    else:
                        result = worker.create(action, actor, request_id, protected=bool(action.protected))
                    return result, worker_db is not None
                finally:
                    self._close_database(worker_db)
            record, isolated = await asyncio.to_thread(work)
            if not isolated:
                service._insert(record)
            return JobAccepted(job_id=getattr(record, "id", uuid.uuid4().hex), state="running")

    def protect(self, profile_id: Any, backup_id: str, protected: bool = True):
        _profile, service = self._service(profile_id)
        return service.protect(backup_id, protected)

    async def confirm_restore(self, action: Any, actor: str | None = None, request_id: Any = None, payload: Mapping[str, Any] | None = None, lease_check: Any = None) -> JobAccepted:
        payload = payload or {}
        profile, _service = self._service(payload.get("profile_id", getattr(action, "profile_id", None)))
        await self._stopped(profile)
        backup_id = str(payload.get("backup_id", ""))
        if not backup_id or "/" in backup_id or "\\" in backup_id or backup_id in {".", ".."}:
            raise SafeError("invalid_backup", "backup archive is not approved")
        if self._availability(backup_id) != PayloadState.PRESENT.value:
            raise SafeError("backup_not_found", "backup payload is not locally available")
        archive = Path(profile.paths.backup_root) / f"{backup_id}.tar.zst"
        source_backup = self.services[_rpc_key(profile)]
        source_restore = self.restores[_rpc_key(profile)]
        def work():
            worker_db = self._isolated_database_factory(self.database)
            worker_backup = self._backup_service_factory(profile, database=worker_db, stopped_check=lambda: self._stopped_sync(profile), free_space=source_backup.free_space, clock=source_backup.clock, tar_runner=source_backup.tar_runner, lease_check=lease_check)
            worker_restore = self._restore_service_factory(
                profile,
                backup_service=worker_backup,
                stopped_check=lambda: self._stopped_sync(profile),
                free_space=source_restore.free_space,
                health_check=None,
                lease_check=lease_check,
                availability_check=lambda backup_id: self._availability_with(
                    worker_db if worker_db is not None else self.database, backup_id
                ),
            )
            try:
                result = worker_restore.restore(archive, actor, request_id)
                destinations = result.destinations or (result.destination,)
                if all(destination.is_dir() for destination in destinations):
                    worker_restore.finalize(result)
                else:
                    worker_restore.rollback(result)
                    raise SafeError("restore_health_failed", "restored profile failed health validation")
                return result
            finally:
                self._close_database(worker_db)
        result = await asyncio.to_thread(work)
        return JobAccepted(job_id=getattr(result, "backup_id", uuid.uuid4().hex), state="running")

    restore = confirm_restore

    def reconcile_startup(self) -> None:
        for restore in self.restores.values():
            restore.reconcile()
        # Observation-only classification of the retirement ledger; it never
        # unlinks, renames, or finishes an interrupted destructive operation.
        self.reconcile_startup_retirement()
