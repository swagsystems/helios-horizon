#!/usr/bin/env python3
"""Discover, stage, back up, and atomically promote official Sunlit releases."""
from __future__ import annotations

import argparse
import concurrent.futures
from contextlib import contextmanager
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .sunlit_manifest import (
    MAX_ENTRIES as MANIFEST_MAX_ENTRIES,
    MAX_MEMBER_SIZE as MANIFEST_MAX_MEMBER_SIZE,
    ManifestError,
    atomic_write,
    make_manifest,
    safe_member_name,
)
from .modpack_update import AssemblyError, MAX_TOTAL_SIZE
from .protocol import ProfileId, UpdateStatus
from .slot import OperationLock, ReservationStore
from .state_db import _configure as _configure_state_connection
from .sunlit_promote import PromotionError, promote_candidate
from .sunlit_stage import StageError, stage

PROFILE = "minecraft-sunlit-cobblemon"
UPDATER_ACTOR = "sunlit-auto-update"
PROJECT_ID = "1495800"
API_ROOT = f"https://www.curseforge.com/api/v1/mods/{PROJECT_ID}"
STAGING_ROOT = Path("/srv/game-servers/.horizon-update-staging")
STATE_ROOT = Path("/srv/game-servers/minecraft-sunlit-cobblemon-state")
RELEASE_ROOT = Path("/opt/game-servers/minecraft-sunlit-cobblemon/releases")
ACTIVE_LINK = Path("/srv/game-servers/minecraft-sunlit-cobblemon-current")
SLOT = Path("/run/game-slot/slot.json")
DATABASE = Path("/var/lib/game-control/state.db")
OVERLAY_RELATIVE = Path("mods/Prometheus-Exporter-1.20.1-forge-1.2.1.jar")
OVERLAY_DESTINATION = OVERLAY_RELATIVE.as_posix()
OVERLAY_SHA256 = "6b09f59ea84d4fd96a7687cafde5bbfe34795d0ceb5357a870b331e7412e6b34"
BACKUP_ROOT = Path("/var/backups/game-servers/minecraft-sunlit-cobblemon")
CHECK_CACHE_SECONDS = 60.0
CHECK_STALE_SECONDS = 600.0
CHECK_WAIT_SECONDS = 2.0
CHECK_FAILURE_BACKOFF_SECONDS = 15.0
PROBE_DEADLINE_SECONDS = 75.0
PROBE_KILL_WAIT_SECONDS = 5.0
CHECK_MESSAGE_LIMIT = 200
BACKUP_ENTRY_OVERHEAD = 8192
BACKUP_ARCHIVE_OVERHEAD = 1024 * 1024
# The backup RPC is an explicit protected boundary.  Manifest, staging, and
# promotion are package-owned and must not cross an absent helper boundary.
RPC_HELPER = Path("/usr/local/libexec/horizon-sunlit-update-rpc")
PYTHON = Path("/opt/game-control/.venv/bin/python")
VERSION_RE = re.compile(r"^SERVER-PACK-Society-Sunlit-Cobblemon-([A-Za-z0-9][A-Za-z0-9._-]{0,126})\.zip$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CAPABILITY_RE = re.compile(r"^[0-9a-f]{64}$")
# Any 32+ byte hex run is treated as a token-shaped value when surfacing helper
# diagnostics so a capability can never reach a log or exception message.
TOKEN_LIKE_RE = re.compile(r"[0-9a-fA-F]{32,}")
MAX_JSON = 4 * 1024 * 1024
MAX_ARCHIVE = 2 * 1024 * 1024 * 1024
SPACE_MARGIN = 2 * 1024 * 1024 * 1024
MAX_OVERLAY_SIZE = 256 * 1024 * 1024
OPERATION_LOCK = Path("/run/game-control/operation.lock")
RESERVATION_FILE = Path("/run/game-control/reservation.json")
RESERVATION_TTL = 30.0
RESERVATION_RENEW_INTERVAL = 10.0
TRUSTED_FS_ROOT = Path("/")


class UpdateError(ValueError):
    pass


@dataclass(frozen=True)
class TrustedOverlay:
    path: Path
    size: int
    sha256: str


@dataclass(frozen=True)
class _CheckSnapshot:
    state: str
    installed: str | None
    available: str | None
    message: str | None
    checked_at: datetime
    observed_at: float


@dataclass(frozen=True)
class _ExistingStage:
    manifest: dict | None
    archive_trusted: bool
    candidate_reusable: bool


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def _fetch_json(url: str) -> dict:
    request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "Horizon-Sunlit-Updater/1"})
    try:
        with urllib.request.build_opener(_NoRedirect).open(request, timeout=30) as response:
            if response.status != 200 or response.geturl() != url:
                raise UpdateError("upstream metadata response is not exact")
            raw = response.read(MAX_JSON + 1)
    except (OSError, urllib.error.URLError) as exc:
        raise UpdateError("upstream metadata is unavailable") from exc
    if len(raw) > MAX_JSON:
        raise UpdateError("upstream metadata exceeds bound")
    try:
        value = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise UpdateError("upstream metadata is malformed") from exc
    if not isinstance(value, dict):
        raise UpdateError("upstream metadata is malformed")
    return value


def discover() -> dict:
    files = _fetch_json(f"{API_ROOT}/files").get("data")
    if not isinstance(files, list):
        raise UpdateError("upstream file list is malformed")
    eligible = []
    for item in files:
        if not isinstance(item, dict):
            continue
        versions = item.get("gameVersions")
        if (
            item.get("releaseType") == 1
            and item.get("fileStatus") in (None, 4)
            and item.get("hasServerPack") is True
            and isinstance(versions, list)
            and "1.20.1" in versions
            and "Forge" in versions
            and isinstance(item.get("id"), int)
        ):
            eligible.append(item)
    if not eligible:
        raise UpdateError("no eligible official Sunlit release was found")
    main = max(eligible, key=lambda item: item["id"])
    additional = _fetch_json(f"{API_ROOT}/files/{main['id']}/additional-files").get("data")
    if not isinstance(additional, list) or len(additional) != 1 or not isinstance(additional[0], dict):
        raise UpdateError("official server-pack relationship is not exact")
    server = additional[0]
    name = server.get("fileName")
    match = VERSION_RE.fullmatch(name) if isinstance(name, str) else None
    file_id = server.get("id")
    size = server.get("fileLength")
    if match is None or not isinstance(file_id, int) or file_id <= 0 or not isinstance(size, int) or not 0 < size <= MAX_ARCHIVE:
        raise UpdateError("official server-pack identity is malformed")
    return {
        "version": match.group(1),
        "main_file_id": str(main["id"]),
        "file_id": str(file_id),
        "file_name": name,
        "size": size,
        "url": f"{API_ROOT}/files/{file_id}/download",
    }


def _digest(path: Path) -> tuple[int, str]:
    total = 0
    value = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            total += len(chunk)
            value.update(chunk)
    return total, value.hexdigest()


def _trusted_directory(path: Path, label: str) -> os.stat_result:
    try:
        info = path.lstat()
    except OSError as exc:
        raise UpdateError(f"{label} is unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != 0
        or info.st_gid != 0
        or info.st_mode & 0o022
    ):
        raise UpdateError(f"{label} is unsafe")
    return info


def _trusted_ancestry(path: Path, label: str) -> None:
    current = path
    while True:
        _trusted_directory(current, label)
        if current == TRUSTED_FS_ROOT:
            return
        if TRUSTED_FS_ROOT not in current.parents:
            raise UpdateError(f"{label} has no trusted filesystem root")
        current = current.parent


def _active_release_path() -> Path:
    release_root = RELEASE_ROOT.resolve(strict=False)
    _trusted_ancestry(RELEASE_ROOT, "Sunlit release path")
    _trusted_ancestry(ACTIVE_LINK.parent, "Sunlit active-link path")
    try:
        link_info = ACTIVE_LINK.lstat()
        if (
            not stat.S_ISLNK(link_info.st_mode)
            or link_info.st_uid != 0
            or link_info.st_gid != 0
        ):
            raise UpdateError("installed Sunlit release is unavailable")
        target = (ACTIVE_LINK.parent / os.readlink(ACTIVE_LINK)).resolve(strict=True)
    except OSError as exc:
        raise UpdateError("installed Sunlit release is unavailable") from exc
    if target.parent != release_root:
        raise UpdateError("installed Sunlit release is outside its fixed root")
    _trusted_directory(target, "installed Sunlit release")
    return target


def _open_trusted_overlay(overlay: Path, expected: Path) -> tuple[int, str]:
    try:
        fd = os.open(overlay, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise UpdateError("Sunlit overlay is unavailable") from exc
    try:
        before = os.fstat(fd)
        if not _trusted_overlay_info(before):
            raise UpdateError("Sunlit overlay is unsafe")
        try:
            opened = Path(f"/proc/self/fd/{fd}").resolve(strict=True)
        except OSError as exc:
            raise UpdateError("Sunlit overlay open identity is unavailable") from exc
        if opened != expected:
            raise UpdateError("Sunlit overlay open identity changed")
        digest = hashlib.sha256()
        total = 0
        while chunk := os.read(fd, 1024 * 1024):
            total += len(chunk)
            digest.update(chunk)
        after = os.fstat(fd)
    finally:
        os.close(fd)
    if (
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        or total != before.st_size
    ):
        raise UpdateError("Sunlit overlay changed while it was read")
    return total, digest.hexdigest()


def _trusted_release_overlay() -> TrustedOverlay:
    """Resolve the monitoring JAR from the root-owned immutable active release."""
    release = _active_release_path()
    current = release
    for part in OVERLAY_RELATIVE.parts[:-1]:
        current = current / part
        _trusted_directory(current, "Sunlit overlay ancestry")
    overlay = current / OVERLAY_RELATIVE.name
    try:
        info = overlay.lstat()
    except OSError as exc:
        raise UpdateError("Sunlit overlay is unavailable") from exc
    if not _trusted_overlay_info(info):
        raise UpdateError("Sunlit overlay is unsafe")
    try:
        resolved = overlay.resolve(strict=True)
        expected = release.resolve(strict=True) / OVERLAY_RELATIVE
    except OSError as exc:
        raise UpdateError("Sunlit overlay is unavailable") from exc
    if resolved != expected:
        raise UpdateError("Sunlit overlay escapes its immutable release")
    size, digest = _open_trusted_overlay(overlay, expected)
    if size != info.st_size or digest != OVERLAY_SHA256:
        raise UpdateError("Sunlit overlay content is not trusted")
    return TrustedOverlay(overlay, size, digest)


def _trusted_overlay_info(info: os.stat_result) -> bool:
    return (
        not stat.S_ISLNK(info.st_mode)
        and stat.S_ISREG(info.st_mode)
        and info.st_nlink == 1
        and info.st_uid == 0
        and info.st_gid == 0
        and not info.st_mode & 0o022
        and 0 < info.st_size <= MAX_OVERLAY_SIZE
    )


_SAFE_CHECK_ERRORS = frozenset(
    {
        "upstream metadata response is not exact",
        "upstream metadata is unavailable",
        "upstream metadata exceeds bound",
        "upstream metadata is malformed",
        "upstream file list is malformed",
        "no eligible official Sunlit release was found",
        "official server-pack relationship is not exact",
        "official server-pack identity is malformed",
        "local update metadata is missing",
        "local update metadata is unsafe",
        "local update metadata is unreadable",
        "local update metadata is malformed",
        "installed Sunlit version cannot be verified",
        "update check returned malformed state",
        "update check returned unsupported state",
        "update check exceeded its deadline",
        "update check was cancelled",
    }
)


def _safe_check_message(value: BaseException) -> str:
    if isinstance(value, UpdateError):
        text = str(value).strip()
        if text in _SAFE_CHECK_ERRORS:
            return text
    return "Update check failed."


class SunlitUpdateChecker:
    """Bounded single-flight read-only check shared by the updater UI seam."""

    def __init__(
        self,
        *,
        probe: Callable[[], dict] | None = None,
        installed_probe: Callable[[], str | None] | None = None,
        monotonic: Callable[[], float] | None = None,
        now: Callable[[], datetime] | None = None,
        cache_seconds: float = CHECK_CACHE_SECONDS,
        stale_seconds: float = CHECK_STALE_SECONDS,
        wait_seconds: float = CHECK_WAIT_SECONDS,
        failure_backoff_seconds: float = CHECK_FAILURE_BACKOFF_SECONDS,
        probe_deadline_seconds: float = PROBE_DEADLINE_SECONDS,
        probe_kill_wait_seconds: float = PROBE_KILL_WAIT_SECONDS,
        popen: Callable[..., Any] | None = None,
    ) -> None:
        self._probe = probe
        self._installed_probe = installed_probe or _installed_version
        self._monotonic = monotonic or time.monotonic
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._cache_seconds = max(0.0, float(cache_seconds))
        self._stale_seconds = max(self._cache_seconds, float(stale_seconds))
        self._wait_seconds = max(0.0, float(wait_seconds))
        self._failure_backoff_seconds = max(0.0, float(failure_backoff_seconds))
        self._probe_deadline_seconds = max(0.1, float(probe_deadline_seconds))
        self._probe_kill_wait_seconds = max(0.1, float(probe_kill_wait_seconds))
        self._popen = popen or subprocess.Popen
        self._lock = threading.Lock()
        self._cache: _CheckSnapshot | None = None
        self._failure: _CheckSnapshot | None = None
        self._future: concurrent.futures.Future[dict] | None = None
        self._process: Any | None = None
        self._cancel = threading.Event()
        self._closed = False

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._cancel.set()
            process = self._process
        if process is not None:
            self._kill_process(process)

    def _kill_process(self, process: Any) -> None:
        if process.poll() is not None:
            return
        try:
            process.kill()
        except OSError:
            pass
        try:
            process.wait(timeout=self._probe_kill_wait_seconds)
        except (subprocess.TimeoutExpired, OSError):
            pass

    def _installed(self) -> str | None:
        try:
            value = self._installed_probe()
        except Exception:
            return None
        return value if isinstance(value, str) and value else None

    def _status(self, snapshot: _CheckSnapshot, *, state: str | None = None, message: str | None = None) -> UpdateStatus:
        return UpdateStatus(
            profile_id=ProfileId.MINECRAFT_SUNLIT_COBBLEMON,
            strategy="manual",
            installed_version=snapshot.installed,
            available_version=snapshot.available,
            restart_required=False,
            apply_supported=False,
            state=state or snapshot.state,
            message=message if message is not None else snapshot.message,
            checked_at=snapshot.checked_at,
        )

    def _checking(self, message: str) -> UpdateStatus:
        snapshot = self._cache or self._failure
        return UpdateStatus(
            profile_id=ProfileId.MINECRAFT_SUNLIT_COBBLEMON,
            strategy="manual",
            installed_version=snapshot.installed if snapshot is not None else self._installed(),
            available_version=snapshot.available if snapshot is not None else None,
            restart_required=False,
            apply_supported=False,
            state="checking",
            message=message[:CHECK_MESSAGE_LIMIT],
            checked_at=self._now(),
        )

    def _snapshot_from_result(self, result: object, observed_at: float) -> _CheckSnapshot:
        if not isinstance(result, dict):
            raise UpdateError("update check returned malformed state")
        state = result.get("state")
        if state not in {"current", "available", "deferred"}:
            raise UpdateError("update check returned unsupported state")
        installed = result.get("installed")
        available = result.get("available")
        installed = installed if isinstance(installed, str) and installed else None
        available = available if isinstance(available, str) and available else None
        if state == "current":
            available = None
        return _CheckSnapshot(
            state=state,
            installed=installed,
            available=available,
            message=None,
            checked_at=self._now(),
            observed_at=observed_at,
        )

    def _failure_snapshot(self, exc: BaseException, observed_at: float) -> _CheckSnapshot:
        message = _safe_check_message(exc)
        cached = self._cache
        if cached is not None and observed_at - cached.observed_at <= self._stale_seconds:
            return _CheckSnapshot(
                state="stale",
                installed=cached.installed,
                available=cached.available,
                message=f"Update check failed; showing the last result: {message}"[:CHECK_MESSAGE_LIMIT],
                checked_at=self._now(),
                observed_at=observed_at,
            )
        return _CheckSnapshot(
            state="failed",
            installed=self._installed(),
            available=None,
            message=message,
            checked_at=self._now(),
            observed_at=observed_at,
        )

    def _finish_future(self, future: concurrent.futures.Future[dict]) -> None:
        with self._lock:
            if self._future is not future:
                return
            self._future = None
            observed_at = self._monotonic()
            try:
                result = future.result()
            except BaseException as exc:
                self._failure = self._failure_snapshot(exc, observed_at)
                return
            try:
                snapshot = self._snapshot_from_result(result, observed_at)
            except BaseException as exc:
                self._failure = self._failure_snapshot(exc, observed_at)
                return
            self._cache = snapshot
            self._failure = None

    def _run_probe(self, future: concurrent.futures.Future[dict]) -> None:
        try:
            if self._probe is not None:
                future.set_result(self._probe())
            else:
                future.set_result(self._run_probe_subprocess())
        except BaseException as exc:
            future.set_exception(exc)

    @staticmethod
    def _probe_command() -> list[str]:
        return [sys.executable, "-B", "-m", "game_control.sunlit_update", "--probe"]

    def _run_probe_subprocess(self) -> dict:
        if self._cancel.is_set():
            raise UpdateError("update check was cancelled")
        process = self._popen(
            self._probe_command(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            close_fds=True,
        )
        with self._lock:
            closed = self._closed
            if not closed:
                self._process = process
        if closed:
            self._kill_process(process)
            raise UpdateError("update check was cancelled")
        try:
            try:
                stdout, _stderr = process.communicate(timeout=self._probe_deadline_seconds)
            except subprocess.TimeoutExpired as exc:
                self._kill_process(process)
                raise UpdateError("update check exceeded its deadline") from exc
        finally:
            with self._lock:
                if self._process is process:
                    self._process = None
        if self._cancel.is_set():
            raise UpdateError("update check was cancelled")
        if len(stdout) > MAX_JSON:
            raise UpdateError("update check returned malformed state")
        try:
            payload = json.loads(stdout)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise UpdateError("update check returned malformed state") from exc
        if not isinstance(payload, dict):
            raise UpdateError("update check returned malformed state")
        if payload.get("state") == "failed":
            message = payload.get("message")
            if isinstance(message, str) and message in _SAFE_CHECK_ERRORS:
                raise UpdateError(message)
            raise UpdateError("update check returned malformed state")
        if process.returncode != 0:
            raise UpdateError("update check returned malformed state")
        return payload

    def _latest_status(self) -> UpdateStatus | None:
        snapshot = self._failure or self._cache
        return self._status(snapshot) if snapshot is not None else None

    def check(self, _profile_id: object = ProfileId.MINECRAFT_SUNLIT_COBBLEMON) -> UpdateStatus:
        with self._lock:
            observed = self._monotonic()
            if self._closed:
                return self._checking("Update checks are unavailable while Horizon is shutting down.")
            future = self._future
            if future is not None and not future.done():
                return self._checking("An update check is in progress.")
            initiator = future is None
            if initiator:
                cached = self._cache
                if cached is not None and 0 <= observed - cached.observed_at <= self._cache_seconds:
                    return self._status(cached)
                failure = self._failure
                if failure is not None and 0 <= observed - failure.observed_at <= self._failure_backoff_seconds:
                    return self._status(failure)
                future = concurrent.futures.Future()
                self._future = future
                thread = threading.Thread(
                    target=self._run_probe,
                    args=(future,),
                    name="horizon-sunlit-check",
                    daemon=True,
                )
                thread.start()
        if not initiator:
            self._finish_future(future)
            return self._latest_status() or self._checking("An update check is in progress.")
        try:
            future.result(timeout=self._wait_seconds)
        except concurrent.futures.TimeoutError:
            return self._checking("An update check is in progress.")
        except BaseException:
            pass
        self._finish_future(future)
        return self._latest_status() or self._checking("An update check is in progress.")


def _load_json(path: Path, maximum: int = MAX_JSON) -> dict:
    try:
        info = path.lstat()
    except OSError as exc:
        raise UpdateError("local update metadata is missing") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > maximum:
        raise UpdateError("local update metadata is unsafe")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise UpdateError("local update metadata is unreadable") from exc
    if not isinstance(value, dict):
        raise UpdateError("local update metadata is malformed")
    return value


def _installed_version() -> str | None:
    record = STATE_ROOT / ".horizon/release.json"
    try:
        if not record.exists():
            return None
        value = _load_json(record, 64 * 1024).get("version")
        if not isinstance(value, str) or not value:
            return None
        # Stable metadata is not the commit record.  The active link is the
        # publication point; never report a version that was only copied into
        # state before activation completed.
        if not ACTIVE_LINK.is_symlink():
            return None
        release_root = RELEASE_ROOT.resolve()
        release = release_root / value
        target = (ACTIVE_LINK.parent / os.readlink(ACTIVE_LINK)).resolve(strict=True)
        if target != release.resolve() or target.parent != release_root:
            return None
        info = release.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            return None
        return value
    except UpdateError:
        raise
    except OSError as exc:
        raise UpdateError("installed Sunlit version cannot be verified") from exc


def _inactive() -> bool:
    """Prove that no lifecycle owner or service is active.

    This function is also used as the reservation precondition.  It must not
    leak implementation errors into the systemd timer: unreadable service,
    slot, or state-database observations are bounded updater failures.
    """
    try:
        result = subprocess.run(
            ["/usr/bin/systemctl", "is-active", "minecraft-sunlit-cobblemon.service"],
            check=False, capture_output=True, text=True,
        )
        state = result.stdout.strip()
        try:
            slot_claimed = SLOT.lstat()
        except FileNotFoundError:
            slot_claimed = None
        if state != "inactive" or slot_claimed is not None:
            return False
        with sqlite3.connect(f"file:{DATABASE}?mode=ro", uri=True, timeout=2) as db:
            db.execute("PRAGMA query_only=ON")
            active_session = db.execute(
                "SELECT 1 FROM player_sessions WHERE profile_id=? AND ended_at IS NULL LIMIT 1", (PROFILE,)
            ).fetchone()
            active_job = db.execute(
                "SELECT 1 FROM jobs WHERE profile_id=? AND state IN ('accepted','running') LIMIT 1", (PROFILE,)
            ).fetchone()
        return active_session is None and active_job is None
    except (OSError, sqlite3.Error, AttributeError, TypeError) as exc:
        raise UpdateError("Sunlit inactive state cannot be verified") from exc


def _state_generation() -> int:
    """Read the root generation through the same query-only state boundary."""
    try:
        with sqlite3.connect(f"file:{DATABASE}?mode=ro", uri=True, timeout=2) as db:
            db.execute("PRAGMA query_only=ON")
            row = db.execute("PRAGMA application_id").fetchone()
        value = row[0] if row is not None and len(row) == 1 else None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("malformed root generation")
        return value
    except (OSError, sqlite3.Error, TypeError, ValueError, IndexError) as exc:
        raise UpdateError("Sunlit root generation cannot be verified") from exc


def _tree_bytes(path: Path, *, skip_symlinks: bool = False) -> int:
    """Return regular-file bytes without following links or hiding races."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return 0
    except OSError as exc:
        raise UpdateError("persistent state size cannot be proven") from exc
    if stat.S_ISLNK(info.st_mode):
        if skip_symlinks:
            return 0
        raise UpdateError("persistent state root is a symlink")
    if stat.S_ISREG(info.st_mode):
        return info.st_size
    if not stat.S_ISDIR(info.st_mode):
        raise UpdateError("persistent state contains an unsafe member")
    total = 0
    try:
        entries = list(os.scandir(path))
    except OSError as exc:
        raise UpdateError("persistent state size cannot be proven") from exc
    for entry in entries:
        total += _tree_bytes(Path(entry.path), skip_symlinks=skip_symlinks)
    return total


class _UpdateLease:
    """Keep the shared lifecycle reservation alive for long updater stages."""

    def __init__(
        self,
        store: ReservationStore,
        operation_id: str,
        state_generation: int = 0,
        *,
        controller_pid: int,
        controller_start_ticks: int,
        capability_token: str | None = None,
    ):
        self.store = store
        self.operation_id = operation_id
        self.state_generation = state_generation
        self.controller_pid = controller_pid
        self.controller_start_ticks = controller_start_ticks
        self.capability_token = capability_token
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread: threading.Thread | None = None
        self._released = False

    def start(self) -> None:
        self._thread = threading.Thread(target=self._renew, name="horizon-sunlit-update-lease", daemon=True)
        self._thread.start()

    def _renew(self) -> None:
        while not self._stop.wait(RESERVATION_RENEW_INTERVAL):
            try:
                self.store.renew_if_owned(
                    PROFILE, self.operation_id, RESERVATION_TTL,
                    state_generation=self.state_generation,
                    operation_kind="update",
                    controller_pid=self.controller_pid,
                    controller_start_ticks=self.controller_start_ticks,
                )
            except BaseException:
                self._lost.set()
                return

    def assert_owned(self) -> None:
        if self._lost.is_set():
            raise UpdateError("Sunlit update reservation was lost")
        try:
            owned = self.store.owns_live(
                PROFILE, self.operation_id, self.state_generation, operation_kind="update",
                controller_pid=self.controller_pid,
                controller_start_ticks=self.controller_start_ticks,
            )
        except (OSError, ValueError, PermissionError, BlockingIOError) as exc:
            raise UpdateError("Sunlit update reservation cannot be verified") from exc
        if not owned:
            self._lost.set()
            raise UpdateError("Sunlit update reservation was lost")

    def assert_owned_locked(self) -> None:
        if self._lost.is_set():
            raise UpdateError("Sunlit update reservation was lost")
        try:
            owned = self.store.owns_live_locked(
                PROFILE, self.operation_id, self.state_generation,
                controller_pid=self.controller_pid,
                controller_start_ticks=self.controller_start_ticks,
            )
        except (OSError, ValueError, PermissionError) as exc:
            raise UpdateError("Sunlit update reservation cannot be verified") from exc
        if not owned:
            self._lost.set()
            raise UpdateError("Sunlit update reservation was lost")

    @contextmanager
    def publication_guard(self, action):
        """Fence one bounded promotion rename with operation.lock ownership."""
        action_value = getattr(action, "value", action)
        if action_value not in {
            "state", "release", "version_state", "metadata", "active_link",
            "rollback_state", "rollback_release", "rollback_version_state",
            "rollback_metadata", "rollback_active_link",
        }:
            raise UpdateError("Sunlit publication action is not approved")
        try:
            with OperationLock(OPERATION_LOCK):
                self.assert_owned_locked()
                if not _inactive():
                    raise UpdateError("Sunlit became active before publication")
                yield
        except UpdateError:
            raise
        except (OSError, ValueError) as exc:
            raise UpdateError("Sunlit publication lock is unavailable") from exc

    def close(self) -> None:
        self.pause()
        if self._released:
            return
        try:
            released = self.store.release_if_owned(
                PROFILE, self.operation_id, self.state_generation, operation_kind="update",
                controller_pid=self.controller_pid,
                controller_start_ticks=self.controller_start_ticks,
            )
        except (OSError, ValueError, PermissionError, BlockingIOError) as exc:
            raise UpdateError("Sunlit update reservation could not be released") from exc
        if not released and not self._lost.is_set():
            raise UpdateError("Sunlit update reservation ownership changed")

    def release_locked(self) -> None:
        if self._released:
            return
        try:
            released = self.store.release_if_owned_locked(
                PROFILE, self.operation_id, self.state_generation,
                operation_kind="update",
                controller_pid=self.controller_pid,
                controller_start_ticks=self.controller_start_ticks,
            )
        except (OSError, ValueError, PermissionError) as exc:
            raise UpdateError("Sunlit update reservation could not be released") from exc
        if not released:
            raise UpdateError("Sunlit update reservation ownership changed")
        self._released = True

    def pause(self) -> None:
        """Stop renewals before taking the final exclusive operation lock."""
        self._stop.set()
        if self._thread is not None:
            # Renewal must be fully drained before ownership is released.  A
            # timed join could leave a worker behind that rewrites a lease
            # after close() has returned.
            self._thread.join()
            self._thread = None


@dataclass(frozen=True)
class _SpacePlan:
    phase: str
    archive_bytes: int
    expanded_bytes: int
    overlay_bytes: int
    persistent_bytes: int
    mutable_bytes: int
    stage_peak_bytes: int
    staged_candidate_bytes: int
    release_bytes: int
    backup_bytes: int
    required_by_device: tuple[tuple[int, int], ...]
    free_by_device: tuple[tuple[int, int], ...]


def _space_probe(path: Path) -> tuple[int, int]:
    current = path
    while not current.exists():
        if current.parent == current:
            raise UpdateError("filesystem identity is unavailable")
        current = current.parent
    try:
        info = current.stat()
        return info.st_dev, shutil.disk_usage(current).free
    except OSError as exc:
        raise UpdateError("filesystem capacity is unavailable") from exc


@dataclass(frozen=True)
class _BackupMeasurement:
    source_bytes: int
    file_count: int

    @property
    def peak_bytes(self) -> int:
        """Snapshot copy fallback and compressed archive coexist."""
        archive_bound = (
            self.source_bytes
            + self.file_count * BACKUP_ENTRY_OVERHEAD
            + BACKUP_ARCHIVE_OVERHEAD
        )
        return self.source_bytes + archive_bound


def _backup_measurement(path: Path) -> _BackupMeasurement:
    def walk(current: Path, parts: tuple[str, ...]) -> tuple[int, int]:
        try:
            info = current.lstat()
        except FileNotFoundError:
            return 0, 0
        except OSError as exc:
            raise UpdateError("backup source size cannot be proven") from exc
        if stat.S_ISLNK(info.st_mode):
            return 0, 0
        if stat.S_ISREG(info.st_mode):
            return info.st_size, 1
        if not stat.S_ISDIR(info.st_mode):
            raise UpdateError("backup source contains an unsafe member")
        total = 0
        files = 0
        try:
            entries = list(os.scandir(current))
        except OSError as exc:
            raise UpdateError("backup source size cannot be proven") from exc
        for entry in entries:
            child_parts = parts + (entry.name,)
            if not parts and entry.name == "backups":
                continue
            if entry.name.endswith(".partial"):
                continue
            if len(child_parts) >= 3 and child_parts[0] == ".versions" and child_parts[2] == "backups":
                continue
            child_bytes, child_files = walk(Path(entry.path), child_parts)
            total += child_bytes
            files += child_files
        return total, files

    source_bytes, file_count = walk(path, ())
    return _BackupMeasurement(source_bytes, file_count)


def _manifest_policy(document: dict) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    policy = document.get("runtime_policy")
    if not isinstance(policy, dict):
        raise UpdateError("runtime policy is unavailable")
    result = []
    for key in ("persistent_dirs", "persistent_files", "mutable_vendor_dirs"):
        value = policy.get(key)
        if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
            raise UpdateError("runtime policy is malformed")
        try:
            safe = tuple(safe_member_name(item) for item in value)
        except ManifestError as exc:
            raise UpdateError("runtime policy is malformed") from exc
        result.append(safe)
    return result[0], result[1], result[2]


def _manifest_measurements(document: dict) -> tuple[int, int, int]:
    archive = document.get("archive")
    expanded = archive.get("total_uncompressed_size") if isinstance(archive, dict) else None
    entries = archive.get("entries") if isinstance(archive, dict) else None
    if (
        isinstance(expanded, bool)
        or not isinstance(expanded, int)
        or not 0 < expanded <= MAX_TOTAL_SIZE
        or not isinstance(entries, list)
    ):
        raise UpdateError("archive expansion bound is unavailable")
    persistent_dirs, persistent_files, mutable_dirs = _manifest_policy(document)
    persistent = 0
    for relative in persistent_dirs + persistent_files:
        persistent += _tree_bytes(STATE_ROOT / relative)
    mutable = 0
    for entry in entries:
        if not isinstance(entry, dict):
            raise UpdateError("archive entry metadata is malformed")
        path = entry.get("path")
        size = entry.get("size")
        if not isinstance(path, str) or isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise UpdateError("archive entry metadata is malformed")
        if any(path == root or path.startswith(root + "/") for root in mutable_dirs):
            mutable += size
    return expanded, persistent, mutable


def _manifest_identity(document: dict, release: dict, overlay: TrustedOverlay) -> dict:
    if (
        not isinstance(document, dict)
        or document.get("manifest_version") != 1
        or document.get("profile_id") != PROFILE
    ):
        raise UpdateError("existing staging manifest is malformed")
    artifact = document.get("artifact")
    archive_record = artifact.get("archive") if isinstance(artifact, dict) else None
    overlay_record = document.get("overlay")
    if (
        not isinstance(artifact, dict)
        or artifact.get("project_id") != PROJECT_ID
        or artifact.get("file_id") != release.get("file_id")
        or artifact.get("version") != release.get("version")
        or not isinstance(archive_record, dict)
        or archive_record.get("size") != release.get("size")
        or not isinstance(archive_record.get("sha256"), str)
        or not SHA256_RE.fullmatch(archive_record["sha256"])
        or not isinstance(overlay_record, dict)
        or overlay_record.get("source") != str(overlay.path)
        or overlay_record.get("size") != overlay.size
        or overlay_record.get("sha256") != overlay.sha256
    ):
        raise UpdateError("existing staging identity does not match upstream")
    supplied = document.get("manifest_sha256")
    body = dict(document)
    body.pop("manifest_sha256", None)
    calculated = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()
    if supplied != calculated:
        raise UpdateError("existing staging manifest self-digest mismatch")
    archive = document.get("archive")
    entries = archive.get("entries") if isinstance(archive, dict) else None
    if not isinstance(entries, list) or len(entries) > MANIFEST_MAX_ENTRIES:
        raise UpdateError("existing staging manifest entry bound is malformed")
    total = 0
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise UpdateError("existing staging manifest entry is malformed")
        path = entry.get("path")
        size = entry.get("size")
        if not isinstance(path, str):
            raise UpdateError("existing staging manifest path is malformed")
        try:
            safe_member_name(path)
        except ManifestError as exc:
            raise UpdateError("existing staging manifest path is unsafe") from exc
        if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= MANIFEST_MAX_MEMBER_SIZE:
            raise UpdateError("existing staging manifest entry size is malformed")
        key = path.casefold()
        if key in seen:
            raise UpdateError("existing staging manifest path duplicates")
        seen.add(key)
        total += size
        if total > MAX_TOTAL_SIZE:
            raise UpdateError("existing staging manifest total bound is malformed")
    if archive.get("total_uncompressed_size") != total:
        raise UpdateError("existing staging manifest total is inconsistent")
    _manifest_policy(document)
    return document


def _trusted_existing_stage(release: dict, overlay: TrustedOverlay) -> _ExistingStage:
    root = _staging_identity_root(release)
    manifest_path = root / "manifest.json"
    manifest: dict | None = None
    if manifest_path.exists() or manifest_path.is_symlink():
        if manifest_path.is_symlink():
            raise UpdateError("existing staging manifest is unsafe")
        manifest = _manifest_identity(
            _load_json(manifest_path, 8 * 1024 * 1024), release, overlay,
        )
    candidate = root / "candidate"
    record = candidate / "candidate.json"
    candidate_reusable = False
    if candidate.exists() or candidate.is_symlink():
        try:
            candidate_info = candidate.lstat()
        except OSError as exc:
            raise UpdateError("existing staged candidate is unavailable") from exc
        if stat.S_ISLNK(candidate_info.st_mode) or not stat.S_ISDIR(candidate_info.st_mode):
            raise UpdateError("existing staged candidate is unsafe")
    if record.exists() or record.is_symlink():
        if record.is_symlink():
            raise UpdateError("existing staged candidate is unsafe")
        candidate_record = _load_json(record, 64 * 1024)
        if manifest is None:
            raise UpdateError("existing staged candidate has no manifest")
        if (
            candidate_record.get("version") != release.get("version")
            or candidate_record.get("manifest_sha256") != manifest.get("manifest_sha256")
        ):
            raise UpdateError("existing staged candidate identity mismatch")
        candidate_reusable = True
    archive_trusted = False
    if manifest is not None:
        archive = root / "server-pack.zip"
        try:
            info = archive.lstat()
        except FileNotFoundError:
            info = None
        except OSError as exc:
            raise UpdateError("existing staging archive is unavailable") from exc
        if info is not None:
            if (
                stat.S_ISLNK(info.st_mode)
                or not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_uid != 0
                or info.st_gid != 0
                or info.st_mode & 0o022
                or info.st_size != release.get("size")
            ):
                raise UpdateError("existing staging archive is unsafe")
            size, digest = _digest(archive)
            expected = manifest["artifact"]["archive"]["sha256"]
            if size != release.get("size") or digest != expected:
                raise UpdateError("existing staging archive identity mismatch")
            archive_trusted = True
        elif not candidate_reusable:
            raise UpdateError("existing staging archive is unavailable")
    return _ExistingStage(
        manifest=manifest,
        archive_trusted=archive_trusted,
        candidate_reusable=candidate_reusable,
    )


def _staging_identity_root(release: dict) -> Path:
    version = release.get("version")
    if not isinstance(version, str) or not VERSION_RE.fullmatch(
        f"SERVER-PACK-Society-Sunlit-Cobblemon-{version}.zip"
    ):
        raise UpdateError("staging release identity is unsafe")
    return STAGING_ROOT / f"sunlit-{version}"


def _space_plan(release: dict, *, manifest: dict | None, phase: str) -> _SpacePlan:
    try:
        archive_size = release["size"]
        if (
            isinstance(archive_size, bool)
            or not isinstance(archive_size, int)
            or not 0 < archive_size <= MAX_ARCHIVE
        ):
            raise ValueError("invalid archive size")
        overlay = _trusted_release_overlay()
        existing = _trusted_existing_stage(release, overlay)
        if phase == "download":
            if manifest is not None:
                raise ValueError("download phase does not accept a manifest")
            archive_new = (
                0
                if existing.archive_trusted or existing.candidate_reusable
                else archive_size
            )
            required = {STAGING_ROOT: archive_new}
            return _finish_space_plan(
                phase, archive_size, 0, overlay.size, 0, 0, archive_size,
                archive_size, 0, 0, required,
            )
        if manifest is None:
            raise ValueError("manifest is required for measured space checks")
        manifest = _manifest_identity(manifest, release, overlay)
        if existing.manifest is not None and existing.manifest != manifest:
            raise UpdateError("existing staging manifest changed during preflight")
        expanded, persistent, mutable = _manifest_measurements(manifest)
        total_expanded = expanded + overlay.size
        stage_peak = archive_size + 2 * total_expanded + 2 * persistent + mutable
        # The completed candidate contains the physical runtime (archive
        # members with mutable config moved to version state) plus that
        # version-state copy and the retained compressed archive.
        staged_candidate = archive_size + total_expanded
        release_bytes = max(0, total_expanded - mutable)
        backup_bytes = _backup_measurement(STATE_ROOT).peak_bytes
        archive_credit = archive_size if existing.archive_trusted else 0
        if phase == "stage":
            staging = 0 if existing.candidate_reusable else max(0, stage_peak - archive_credit)
            required = {STAGING_ROOT: staging}
        elif phase == "promotion":
            staged = 0 if existing.candidate_reusable else max(0, staged_candidate - archive_credit)
            required = {
                STAGING_ROOT: staged,
                RELEASE_ROOT: release_bytes,
                BACKUP_ROOT: backup_bytes,
            }
        elif phase == "post_backup":
            required = {RELEASE_ROOT: release_bytes}
        else:
            raise ValueError("unsupported space phase")
        return _finish_space_plan(
            phase, archive_size, expanded, overlay.size, persistent, mutable,
            stage_peak, staged_candidate, release_bytes, backup_bytes, required,
        )
    except UpdateError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise UpdateError("free space for Sunlit update cannot be proven") from exc


def _finish_space_plan(
    phase: str,
    archive: int,
    expanded: int,
    overlay: int,
    persistent: int,
    mutable: int,
    stage_peak: int,
    staged_candidate: int,
    release_bytes: int,
    backup_bytes: int,
    required: dict[Path, int],
) -> _SpacePlan:
    required_by_device: dict[int, int] = {}
    free_by_device: dict[int, int] = {}
    devices: set[int] = set()
    for path, amount in required.items():
        device, free = _space_probe(path)
        free_by_device[device] = min(free_by_device.get(device, free), free)
        devices.add(device)
        if amount > 0:
            required_by_device[device] = required_by_device.get(device, 0) + amount
    for device in devices:
        required_by_device[device] = required_by_device.get(device, 0) + SPACE_MARGIN
    return _SpacePlan(
        phase=phase,
        archive_bytes=archive,
        expanded_bytes=expanded,
        overlay_bytes=overlay,
        persistent_bytes=persistent,
        mutable_bytes=mutable,
        stage_peak_bytes=stage_peak,
        staged_candidate_bytes=staged_candidate,
        release_bytes=release_bytes,
        backup_bytes=backup_bytes,
        required_by_device=tuple(sorted(required_by_device.items())),
        free_by_device=tuple(sorted(free_by_device.items())),
    )


def _require_space(release: dict, *, manifest: dict | None = None, phase: str) -> None:
    plan = _space_plan(release, manifest=manifest, phase=phase)
    free = dict(plan.free_by_device)
    for device, required in plan.required_by_device:
        available = free.get(device, 0)
        if available < required:
            raise UpdateError(
                f"insufficient free space for Sunlit {phase} on filesystem {device}: "
                f"need {required} bytes, have {available}"
            )


def _safe_helper_detail(stderr: str | None, *, secret: str | None = None) -> str:
    """Return one bounded helper stderr line, never exposing the input token.

    The exact private input is removed first, then every token-shaped run is
    redacted, and only then is the line truncated.  Truncating first could keep
    a capability prefix that straddles the cutoff.
    """
    if not stderr:
        return ""
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    if not lines:
        return ""
    detail = lines[-1]
    token = (secret or "").strip()
    if token:
        detail = detail.replace(token, "[redacted]")
        detail = detail.replace(token.upper(), "[redacted]")
    detail = TOKEN_LIKE_RE.sub("[redacted]", detail)
    return detail[:200]


def _run(
    argv: list[str],
    *,
    timeout: int,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a fixed helper, optionally feeding private stdin.

    ``input_text`` is delivered through a private pipe, never argv or the
    environment.  On failure the raised error carries only a bounded, sanitized
    helper stderr line so the input can never surface in an exception.
    """
    try:
        return subprocess.run(
            argv,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
            input=input_text,
        )
    except subprocess.CalledProcessError as exc:
        # ``from None`` keeps the chained CalledProcessError (whose .stderr
        # holds raw helper output) out of any traceback rendering.
        detail = _safe_helper_detail(exc.stderr, secret=input_text)
        message = "update helper failed: sunlit backup RPC"
        if detail:
            message = f"{message}: {detail}"
        raise UpdateError(message) from None
    except (OSError, subprocess.TimeoutExpired) as exc:
        # TimeoutExpired can also carry captured .stderr; never chain it.
        raise UpdateError("update helper failed: sunlit backup RPC") from None


def _download(release: dict, target: Path) -> str:
    temporary = target.parent / f".{target.name}.download"
    if temporary.exists() or temporary.is_symlink():
        raise UpdateError("partial archive already exists")
    request = urllib.request.Request(release["url"], headers={"Accept": "application/octet-stream", "User-Agent": "Horizon-Sunlit-Updater/1"})
    total = 0
    digest = hashlib.sha256()
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            final = urllib.parse.urlsplit(response.geturl())
            source = urllib.parse.urlsplit(release["url"])
            trusted_final = (
                final.scheme == "https"
                and not final.username
                and not final.password
                and not final.fragment
                and (
                    response.geturl() == release["url"]
                    or (final.hostname is not None and final.hostname.endswith(".forgecdn.net"))
                )
                and source.scheme == "https"
            )
            if response.status != 200 or not trusted_final:
                raise UpdateError("server-pack download response is not exact")
            with temporary.open("xb") as output:
                os.chmod(temporary, 0o600)
                while chunk := response.read(1024 * 1024):
                    total += len(chunk)
                    if total > release["size"] or total > MAX_ARCHIVE:
                        raise UpdateError("server-pack download exceeds bound")
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
        if total != release["size"]:
            raise UpdateError("server-pack download size mismatch")
        os.replace(temporary, target)
        return digest.hexdigest()
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _staging_root(release: dict) -> Path:
    version = release.get("version")
    if not isinstance(version, str) or not VERSION_RE.fullmatch(
        f"SERVER-PACK-Society-Sunlit-Cobblemon-{version}.zip"
    ):
        raise UpdateError("staging release identity is unsafe")
    try:
        root_info = STAGING_ROOT.lstat()
    except FileNotFoundError:
        try:
            STAGING_ROOT.mkdir(parents=True, mode=0o700)
            root_info = STAGING_ROOT.lstat()
        except OSError as exc:
            raise UpdateError("update staging root is unavailable") from exc
    except OSError as exc:
        raise UpdateError("update staging root is unavailable") from exc
    if (
        stat.S_ISLNK(root_info.st_mode)
        or not stat.S_ISDIR(root_info.st_mode)
        or root_info.st_uid != 0
        or root_info.st_gid != 0
        or root_info.st_mode & 0o022
    ):
        raise UpdateError("update staging root is unsafe")
    root = STAGING_ROOT / f"sunlit-{version}"
    try:
        info = root.lstat()
    except FileNotFoundError:
        try:
            root.mkdir(mode=0o700)
            info = root.lstat()
        except OSError as exc:
            raise UpdateError("update staging root is unavailable") from exc
    except OSError as exc:
        raise UpdateError("update staging root is unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != 0
        or info.st_gid != 0
        or info.st_mode & 0o022
    ):
        raise UpdateError("existing staging root is unsafe")
    return root


def _staged_operation_id(root: Path) -> str:
    """Load one durable UUID for all retries of a staged release."""
    path = root / "update-operation-id"
    try:
        info = path.lstat()
    except FileNotFoundError:
        # A populated pre-Wave-5 staging directory cannot be safely adopted:
        # creating a new owner would permit a restarted updater to take over
        # work whose original reservation may still be live.
        try:
            populated = any(root.iterdir())
        except OSError as exc:
            raise UpdateError("staged update identity is unavailable") from exc
        if populated:
            raise UpdateError("staged update identity is missing")
        operation_id = str(uuid.uuid4())
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
            with os.fdopen(fd, "w", encoding="ascii") as stream:
                stream.write(operation_id + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except (OSError, UnicodeError) as exc:
            path.unlink(missing_ok=True)
            raise UpdateError("staged update identity is unavailable") from exc
        return operation_id
    except OSError as exc:
        raise UpdateError("staged update identity is unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != 0
        or info.st_gid != 0
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_size > 128
    ):
        raise UpdateError("staged update identity is unsafe")
    try:
        value = path.read_text(encoding="ascii").strip()
        parsed = uuid.UUID(value)
    except (OSError, UnicodeError, ValueError) as exc:
        raise UpdateError("staged update identity is malformed") from exc
    if str(parsed) != value:
        raise UpdateError("staged update identity is malformed")
    return value


def _stage(release: dict) -> tuple[Path, dict]:
    root = _staging_root(release)
    _staged_operation_id(root)
    archive = root / "server-pack.zip"
    manifest_path = root / "manifest.json"
    overlay = _trusted_release_overlay()
    try:
        if manifest_path.exists() or manifest_path.is_symlink():
            manifest = _load_json(manifest_path)
            artifact = manifest.get("artifact", {})
            overlay_record = manifest.get("overlay", {})
            if (
                artifact.get("project_id") != PROJECT_ID
                or artifact.get("file_id") != release["file_id"]
                or artifact.get("version") != release["version"]
                or artifact.get("archive", {}).get("size") != release["size"]
                or overlay_record.get("source") != str(overlay.path)
                or overlay_record.get("size") != overlay.size
                or overlay_record.get("sha256") != overlay.sha256
            ):
                raise UpdateError("existing staging identity does not match upstream")
        else:
            if archive.is_symlink():
                raise UpdateError("existing server-pack archive is unsafe")
            if archive.exists():
                size, sha256 = _digest(archive)
                if size != release["size"]:
                    raise UpdateError("existing server-pack archive is unsafe")
            else:
                sha256 = _download(release, archive)
            try:
                manifest = make_manifest(argparse.Namespace(
                    archive=archive,
                    version=release["version"],
                    project_id=PROJECT_ID,
                    file_id=release["file_id"],
                    url=release["url"],
                    archive_size=release["size"],
                    archive_sha256=sha256,
                    overlay_source=overlay.path,
                    overlay_destination=OVERLAY_DESTINATION,
                    overlay_sha256=overlay.sha256,
                ))
                atomic_write(manifest_path, (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode())
            except (ManifestError, OSError) as exc:
                raise UpdateError("manifest generation failed") from exc
        _require_space(release, manifest=manifest, phase="stage")
        candidate_path = root / "candidate/candidate.json"
        if candidate_path.exists() or candidate_path.is_symlink():
            candidate = _load_json(candidate_path, 64 * 1024)
            if candidate.get("version") != release["version"] or candidate.get("manifest_sha256") != manifest.get("manifest_sha256"):
                raise UpdateError("existing staged candidate identity mismatch")
        else:
            if not archive.is_file() or archive.is_symlink():
                raise UpdateError("staging archive is unavailable")
            if _digest(archive) != (
                manifest["artifact"]["archive"]["size"],
                manifest["artifact"]["archive"]["sha256"],
            ):
                raise UpdateError("staging archive changed after manifest creation")
            try:
                stage(argparse.Namespace(
                    manifest=manifest_path,
                    archive=archive,
                    prior_runtime=STATE_ROOT,
                    candidate_root=root / "candidate",
                ))
            except (AssemblyError, StageError, OSError, KeyError, TypeError, ValueError) as exc:
                raise UpdateError("candidate staging failed") from exc
        return root, manifest
    except BaseException:
        # Preserve a completed manifest/archive for forensic inspection, but
        # never leave a partial candidate that a later run could promote.
        candidate = root / "candidate"
        if candidate.exists() and not (candidate / "candidate.json").is_file():
            shutil.rmtree(candidate, ignore_errors=True)
        raise


def _write_private_file(path: Path, payload: bytes) -> None:
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}"
    try:
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as exc:
        temporary.unlink(missing_ok=True)
        raise UpdateError("backup request state is unavailable") from exc


def _read_private_uuid(path: Path) -> str:
    info = path.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != 0
        or info.st_gid != 0
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_size > 128
    ):
        raise UpdateError("backup request state is unsafe")
    try:
        value = path.read_text(encoding="ascii").strip()
        parsed = uuid.UUID(value)
    except (OSError, UnicodeError, ValueError) as exc:
        raise UpdateError("backup request state is malformed") from exc
    if str(parsed) != value:
        raise UpdateError("backup request state is malformed")
    return value


def _read_private_json(path: Path, *, maximum: int = 64 * 1024) -> dict:
    info = path.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != 0
        or info.st_gid != 0
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_size > maximum
    ):
        raise UpdateError("backup request state is unsafe")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise UpdateError("backup request state is malformed") from exc
    if not isinstance(value, dict):
        raise UpdateError("backup request state is malformed")
    return value


def _expected_backup_action(value: object) -> dict | None:
    """Return the one fixed protected action, or None when it does not match."""
    if not isinstance(value, dict):
        return None
    if (
        value.get("kind") != "create_backup"
        or value.get("profile_id") != PROFILE
        or value.get("protected") is not True
        or value.get("destination") != "horizon-b2"
    ):
        return None
    return {
        "kind": "create_backup",
        "profile_id": PROFILE,
        "protected": True,
        "destination": "horizon-b2",
    }


def _backup_request_classification(request_id: str) -> tuple[str, dict, dict]:
    try:
        with sqlite3.connect(f"file:{DATABASE}?mode=ro", uri=True, timeout=2) as db:
            db.execute("PRAGMA query_only=ON")
            row = db.execute(
                "SELECT canonical_request,response,status FROM rpc_idempotency WHERE request_id=?",
                (request_id,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise UpdateError("backup request state is unavailable") from exc
    if row is None:
        return "missing", {}, {}
    canonical_text, response_text, status = row
    if (
        status != "completed"
        or not isinstance(canonical_text, str)
        or not isinstance(response_text, str)
        or not response_text
        or len(canonical_text) > MAX_JSON
        or len(response_text) > MAX_JSON
    ):
        return "ambiguous", {}, {}
    try:
        canonical = json.loads(canonical_text)
        response = json.loads(response_text)
    except (TypeError, json.JSONDecodeError):
        return "ambiguous", {}, {}
    if not isinstance(canonical, dict) or not isinstance(response, dict):
        return "ambiguous", {}, {}
    action = _expected_backup_action(canonical.get("action"))
    # Bind the durable row to this exact request id and actor before trusting
    # any outcome; a same-key row written by anything else is not ours, and the
    # evidence we keep is limited to the fixed action and safe error fields.
    if (
        action is None
        or canonical.get("actor") != UPDATER_ACTOR
        or canonical.get("request_id") != request_id
        or response.get("request_id") != request_id
    ):
        return "ambiguous", {}, {}
    safe_canonical = {"request_id": request_id, "actor": UPDATER_ACTOR, "action": action}
    if response.get("ok") is True:
        return "success", safe_canonical, {"request_id": request_id, "ok": True}
    error = response.get("error")
    if (
        response.get("ok") is False
        and isinstance(error, dict)
        and error.get("code") == "slot_conflict"
        and error.get("message") == "game slot is reserved"
        and error.get("retryable") is False
    ):
        safe_response = {
            "request_id": request_id,
            "ok": False,
            "error": {
                "code": "slot_conflict",
                "message": "game slot is reserved",
                "retryable": False,
            },
        }
        return "prejob_slot_conflict", safe_canonical, safe_response
    return "terminal_failure", safe_canonical, {}


def _request_authority_path(root: Path) -> Path:
    return root / "backup-request-authority.json"


def _write_request_authority(root: Path, request_id: str, lease: _UpdateLease) -> None:
    token = lease.capability_token
    if token is None:
        raise UpdateError("backup request authority is unavailable")
    payload = {
        "schema": 1,
        "request_id": request_id,
        "operation_id": lease.operation_id,
        "capability_sha256": hashlib.sha256(token.encode("ascii")).hexdigest(),
    }
    _write_private_file(
        _request_authority_path(root),
        (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )


def _request_bound_to_live_lease(root: Path, request_id: str, lease: _UpdateLease) -> bool:
    """Prove an existing completed request belongs to this exact live reservation."""
    token = lease.capability_token
    if token is None:
        return False
    try:
        authority = _read_private_json(_request_authority_path(root))
    except (UpdateError, OSError):
        return False
    if (
        authority.get("schema") != 1
        or authority.get("request_id") != request_id
        or authority.get("operation_id") != lease.operation_id
    ):
        return False
    recorded = authority.get("capability_sha256")
    expected = hashlib.sha256(token.encode("ascii")).hexdigest()
    return isinstance(recorded, str) and hmac.compare_digest(recorded, expected)


def _backup_request_id(root: Path, lease: _UpdateLease) -> str:
    request_file = root / "backup-request-id"
    if not request_file.exists() and not request_file.is_symlink():
        request_id = str(uuid.uuid4())
        _write_private_file(request_file, (request_id + "\n").encode("ascii"))
        _write_request_authority(root, request_id, lease)
        return request_id
    request_id = _read_private_uuid(request_file)
    classification, canonical, response = _backup_request_classification(request_id)
    if classification == "prejob_slot_conflict":
        replacement = str(uuid.uuid4())
        evidence = {
            "schema": 1,
            "old_request_id": request_id,
            "classification": classification,
            "canonical_request": canonical,
            "response": response,
            "replacement_request_id": replacement,
        }
        _write_private_file(
            root / f"backup-request-recovery-{request_id}.json",
            (json.dumps(evidence, sort_keys=True, indent=2) + "\n").encode("utf-8"),
        )
        _write_private_file(request_file, (replacement + "\n").encode("ascii"))
        _write_request_authority(root, replacement, lease)
        return replacement
    if classification == "success":
        # A completed backup from an earlier run cannot by itself prove a fresh
        # pre-update snapshot.  Reuse it only when it is bound to this exact,
        # continuously-held reservation; otherwise fail closed without creating
        # a new request or touching the durable row.
        if not _request_bound_to_live_lease(root, request_id, lease):
            raise UpdateError(
                "a completed pre-update backup from an earlier run cannot be "
                "proven fresh; refusing to reuse it"
            )
        return request_id
    if classification == "missing":
        raise UpdateError("backup request outcome is unknown")
    if classification == "ambiguous":
        raise UpdateError("backup request outcome is ambiguous; refusing to act")
    raise UpdateError("backup request previously failed")


def _request_backup(root: Path, lease: _UpdateLease | None = None) -> str:
    if lease is None or lease.capability_token is None:
        raise UpdateError("backup request authority is unavailable")
    request_id = _backup_request_id(root, lease)
    result = _run([
        "/usr/sbin/runuser", "-u", "gamecontrol", "--", "/usr/bin/env", "PYTHONPATH=/opt/game-control/src",
        str(PYTHON), str(RPC_HELPER), "backup", "--request-id", request_id,
    ], timeout=7_300, input_text=lease.capability_token + "\n")
    try:
        backup_id = json.loads(result.stdout)["job_id"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise UpdateError("backup response is malformed") from exc
    try:
        with sqlite3.connect(f"file:{DATABASE}?mode=ro", uri=True, timeout=2) as db:
            db.execute("PRAGMA query_only=ON")
            row = db.execute(
                "SELECT b.verified,b.protected,p.upload_state,p.remote_verified,p.comparison_state "
                "FROM backups b JOIN backup_protections p ON p.backup_id=b.id "
                "WHERE b.id=? AND b.profile_id=? AND p.profile_id=? AND p.destination_id='horizon-b2' AND p.backup_class='application'",
                (backup_id, PROFILE, PROFILE),
            ).fetchone()
    except sqlite3.Error as exc:
        raise UpdateError("backup protection state is unavailable") from exc
    if row != (1, 1, "succeeded", 1, "verified"):
        raise UpdateError("pre-update backup is not fully protected")
    return str(backup_id)


def _record(prior: str | None, new: str) -> None:
    """Append one successful-update row using a configured state connection.

    The ``updates`` schema validates ``created_at`` with the
    ``is_rfc3339_timestamp`` SQL function, which only exists on a connection
    configured by ``state_db``.  A bare ``sqlite3.connect`` therefore fails the
    CHECK with "unknown function: is_rfc3339_timestamp" and the promotion's
    history write is lost.  Reuse the established configured connection path.
    """
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(DATABASE, timeout=5.0)
        _configure_state_connection(connection)
        connection.execute(
            "INSERT INTO updates(id,profile_id,created_at,strategy,prior_version,new_version,state) "
            "VALUES(?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'),'curated_modpack',?,?,?)",
            (uuid.uuid4().hex, PROFILE, prior, new, "succeeded"),
        )
        connection.commit()
    except sqlite3.Error as exc:
        raise UpdateError("update history state is unavailable") from exc
    finally:
        if connection is not None:
            connection.close()


def _reserve_update(operation_id: str) -> _UpdateLease | None:
    """Atomically reserve the profile only while its stopped state is proven."""
    try:
        capability_token = secrets.token_hex(32)
        capability_sha256 = hashlib.sha256(capability_token.encode("ascii")).hexdigest()
        store = ReservationStore(
            operation_path=OPERATION_LOCK,
            reservation_path=RESERVATION_FILE,
        )
        reservation = store.reserve_if_available(
            PROFILE,
            operation_id,
            RESERVATION_TTL,
            state_generation=0,
            availability_check=_inactive,
            operation_kind="update",
            generation_provider=_state_generation,
            capability_sha256=capability_sha256,
        )
    except BlockingIOError:
        return None
    except UpdateError:
        raise
    except (OSError, ValueError, PermissionError, sqlite3.Error) as exc:
        raise UpdateError("Sunlit update reservation is unavailable") from exc
    lease = _UpdateLease(
        store,
        operation_id,
        reservation.state_generation,
        controller_pid=reservation.controller_pid,
        controller_start_ticks=reservation.controller_start_ticks,
        capability_token=capability_token,
    )
    lease.start()
    return lease


def run(*, check_only: bool) -> dict:
    release = discover()
    installed = _installed_version()
    if installed == release["version"]:
        return {"state": "current", "installed": installed, "available": None}
    if check_only:
        return {"state": "available", "installed": installed, "available": release["version"], "file_id": release["file_id"]}
    if os.geteuid() != 0:
        raise UpdateError("automatic update requires root")
    if not _inactive():
        return {"state": "deferred", "installed": installed, "available": release["version"]}
    # Establish or validate the fixed staging identity before taking the
    # reservation.  This is metadata-only and lets retries reuse one UUID;
    # the reservation precondition below still closes a lifecycle race before
    # any archive or candidate mutation begins.
    staging_root = _staging_root(release)
    operation_id = _staged_operation_id(staging_root)
    lease = _reserve_update(operation_id)
    if lease is None:
        return {"state": "deferred", "installed": installed, "available": release["version"]}
    primary_error: BaseException | None = None
    try:
        lease.assert_owned()
        _require_space(release, phase="download")
        try:
            root, manifest = _stage(release)
        except UpdateError:
            raise
        except (AssemblyError, OSError, KeyError, TypeError, ValueError) as exc:
            raise UpdateError("candidate staging failed") from exc
        lease.assert_owned()
        _require_space(release, manifest=manifest, phase="promotion")
        backup_id = _request_backup(root, lease)
        lease.assert_owned()
        _require_space(release, manifest=manifest, phase="post_backup")
        try:
            promotion = promote_candidate(
                version=release["version"],
                manifest=root / "manifest.json",
                candidate_root=root / "candidate",
                manifest_sha256=manifest["manifest_sha256"],
                publication_guard=lease.publication_guard,
            )
        except UpdateError:
            raise
        except (AssemblyError, OSError, PromotionError, KeyError, TypeError, ValueError) as exc:
            raise UpdateError("candidate promotion failed") from exc
        lease.assert_owned()
        with lease.publication_guard("metadata"):
            _record(installed, release["version"])
            lease.release_locked()
        (root / "server-pack.zip").unlink(missing_ok=True)
        return {"state": "promoted", "installed": release["version"], "available": None, "backup_id": backup_id}
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            lease.close()
        except BaseException:
            if primary_error is None:
                raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--probe", action="store_true")
    args = parser.parse_args(argv)
    if args.probe:
        try:
            result = run(check_only=True)
        except UpdateError as exc:
            result = {"state": "failed", "message": _safe_check_message(exc)}
        print(json.dumps(result, sort_keys=True))
        return 0
    lock_path = Path("/run/lock/horizon-sunlit-update.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"state": "deferred", "reason": "update already running"}, sort_keys=True))
            return 0
        try:
            print(json.dumps(run(check_only=args.check), sort_keys=True))
        except UpdateError as exc:
            print(f"error: {exc}", file=os.sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
