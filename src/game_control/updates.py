"""Fixed, root-profile-selected update strategies with explicit rollback."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

import httpx

from .adapters.crafty import parse_version_text
from .errors import SafeError
from .maintenance_process import maintenance_argv
from .protocol import UpdateStatus
from .protocol import JobAccepted
from .backups import BackupService

STEAMCMD_ARGV = (
    "/opt/steamcmd/steamcmd.sh",
    "+force_install_dir",
    "/opt/pzserver",
    "+login",
    "anonymous",
    "+app_update",
    "380870",
    "-beta",
    "unstable",
    "validate",
    "+quit",
)

_MAX_CANDIDATE_URL_LENGTH = 256
_UNKNOWN_VERSION = "unknown"
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
MAX_DOWNLOAD_BYTES = 512 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 4096
MAX_MEMBER_BYTES = 256 * 1024 * 1024
MAX_EXTRACTED_BYTES = 1024 * 1024 * 1024
MAX_COMPRESSION_RATIO = 100


def _id(profile: Any) -> str:
    value = getattr(profile, "id", profile)
    return getattr(value, "value", str(value))


def _isolated_database(database: Any) -> Any | None:
    path = getattr(database, "path", None)
    opener = getattr(type(database), "open", None)
    if path is None or not callable(opener):
        return None
    try:
        return opener(path)
    except Exception:
        return None


def _close_database(database: Any | None) -> None:
    if database is not None and hasattr(database, "close"):
        database.close()


@dataclass(frozen=True)
class UpdateResult:
    state: str
    profile_id: str
    strategy: str
    prior_version: str | None = None
    new_version: str | None = None


class UpdateService:
    def __init__(
        self,
        profiles: Mapping[str, Any] | Any,
        *,
        database: Any | None = None,
        backup_service: Any | None = None,
        runner: Callable[..., Any] | None = None,
        downloader: Callable[..., Any] | None = None,
        stage_release: Callable[..., Any] | None = None,
        verify_release: Callable[..., bool] | None = None,
        stopped_check: Callable[[Any], bool] | Callable[[], bool] | None = None,
        running_check: Callable[[Any], bool] | Callable[[], bool] | None = None,
        http_client: Any | None = None,
        clock: Callable[[], float] | None = None,
        lease_check: Callable[[], bool] | None = None,
        manual_checker: Callable[[Any], UpdateStatus] | None = None,
    ) -> None:
        if isinstance(profiles, Mapping):
            self.profiles = profiles
        elif hasattr(profiles, "id"):
            self.profiles = {_id(profiles): profiles}
        else:
            self.profiles = {_id(item): item for item in profiles}
        self.database = database
        self.backup_service = backup_service
        self.runner = runner or subprocess.run
        self.downloader = downloader
        self.stage_release = stage_release
        self.verify_release = verify_release
        self.stopped_check = stopped_check
        self.running_check = running_check
        # Injected clients are borrowed even when a test double deliberately
        # implements falsey truthiness.  Only the default client is owned.
        self._owns_http_client = http_client is None
        self.http_client = httpx.Client(timeout=10.0) if http_client is None else http_client
        self._http_client_closed = False
        self._close_task: asyncio.Task[None] | None = None
        self.clock = clock or time.time
        self.lease_check = lease_check
        self.manual_checker = manual_checker

    async def _close_impl(self) -> None:
        if self._http_client_closed or not self._owns_http_client:
            return
        close = getattr(self.http_client, "aclose", None)
        if callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result
        else:
            close = getattr(self.http_client, "close", None)
            if callable(close):
                result = await asyncio.to_thread(close)
                if inspect.isawaitable(result):
                    await result
        self._http_client_closed = True

    async def aclose(self) -> None:
        """Close the default HTTP client once, preserving cancellation."""

        task = self._close_task
        if task is None or task.done():
            task = asyncio.create_task(self._close_impl(), name="horizon-update-close")
            self._close_task = task
        cancelled = False
        while True:
            try:
                await asyncio.shield(task)
                break
            except asyncio.CancelledError:
                cancelled = True
                if task.done():
                    break
                continue
        # Consume an ordinary close failure even when the caller was
        # cancelled, so no task exception becomes unretrieved.
        error: BaseException | None = None
        try:
            task.result()
        except BaseException as exc:
            error = exc
        if cancelled or isinstance(error, asyncio.CancelledError):
            raise asyncio.CancelledError
        if error is not None:
            raise error

    def _assert_lease(self) -> None:
        if self.lease_check is not None and not self.lease_check():
            raise SafeError("slot_conflict", "operation lease was lost before publication")

    def _profile(self, value: Any) -> Any:
        key = _id(getattr(value, "profile_id", value))
        try:
            return self.profiles[key]
        except (KeyError, TypeError) as exc:
            raise SafeError("profile_not_found", "profile was not found") from exc

    def _is_running(self, profile: Any) -> bool:
        check = self.running_check
        if check is not None:
            try:
                return bool(check(profile))
            except TypeError:
                return bool(check())
        if self.stopped_check is not None:
            try:
                return not bool(self.stopped_check(profile))
            except TypeError:
                return not bool(self.stopped_check())
        state = getattr(profile, "state", None)
        return str(getattr(state, "value", state)) in {"running", "starting", "stopping"}

    def _strategy(self, profile: Any) -> str:
        return str(profile.update.kind)

    def check(self, profile: Any) -> UpdateStatus:
        profile_obj = self._profile(profile)
        strategy = self._strategy(profile_obj)
        installed = self._installed_version(profile_obj)
        if strategy == "manual" and self.manual_checker is not None:
            try:
                result = self.manual_checker(profile_obj.id)
            except Exception:
                return UpdateStatus(
                    profile_id=profile_obj.id,
                    strategy="manual",
                    installed_version=installed,
                    available_version=None,
                    restart_required=False,
                    apply_supported=False,
                    state="failed",
                    message="Update check failed.",
                )
            if isinstance(result, UpdateStatus) and result.profile_id == profile_obj.id:
                return result
            return UpdateStatus(
                profile_id=profile_obj.id,
                strategy="manual",
                installed_version=installed,
                available_version=None,
                restart_required=False,
                apply_supported=False,
                state="failed",
                message="Update check returned an invalid result.",
            )
        available = self._candidate_version(profile_obj)
        if available == installed:
            available = None
        if strategy == "manual":
            state = "unsupported"
            message = "Update checks are not available for this profile."
        elif available is None:
            state = "current"
            message = None
        else:
            state = "available"
            message = None
        return UpdateStatus(
            profile_id=profile_obj.id,
            strategy=strategy,
            installed_version=installed,
            available_version=available,
            restart_required=strategy != "manual",
            apply_supported=strategy != "manual",
            state=state,
            message=message,
        )

    @staticmethod
    def _candidate_version(profile: Any) -> str | None:
        """Return a bounded candidate version only for fixed release URLs."""
        try:
            strategy = str(profile.update.kind)
            if strategy == "curated_modpack":
                value = str(profile.update.curated.version)
                return value if 1 <= len(value) <= 128 else None
            if strategy != "release_symlink":
                return None
            raw_url = profile.update.download_url
            if raw_url is None:
                return None
            url = str(raw_url)
            if len(url) > _MAX_CANDIDATE_URL_LENGTH:
                return None
            parsed = urlsplit(url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc or not parsed.path:
                return None
            version = parse_version_text(parsed.path)
            return None if version in {None, _UNKNOWN_VERSION} else version
        except Exception:
            return None

    def _installed_version(self, profile: Any) -> str | None:
        if self._strategy(profile) == "curated_modpack":
            try:
                target = os.readlink(Path(profile.update.curated.active_link))
                return Path(target).name[:128] or None
            except OSError:
                return None
        path = Path(profile.paths.version_file)
        try:
            return parse_version_text(path.read_text(encoding="utf-8"))
        except OSError:
            current = Path(profile.paths.install_root) / "current"
            try:
                return Path(os.readlink(current)).name
            except OSError:
                return None

    def _backup(self, profile: Any) -> Any:
        service = self.backup_service
        if service is None:
            raise SafeError("backup_failed", "verified pre-update backup is required")
        record = None
        create = getattr(service, "create", None)
        if create is not None:
            try:
                record = create(protected=True)
            except TypeError:
                try:
                    record = create(profile, protected=True)
                except TypeError:
                    record = create()
            except SafeError:
                raise
            except Exception as exc:
                raise SafeError("backup_failed", "verified pre-update backup is required") from exc
        if record is None:
            records = getattr(service, "list", lambda: ())()
            record = next((item for item in records if bool(getattr(item, "verified", False))), None)
        verified = record.get("verified", False) if isinstance(record, Mapping) else getattr(record, "verified", False)
        if record is None or not bool(verified):
            raise SafeError("backup_failed", "verified pre-update backup is required")
        path = record.get("path") if isinstance(record, Mapping) else getattr(record, "path", None)
        if path is not None and (not Path(path).is_file() or Path(path).stat().st_size <= 0):
            raise SafeError("backup_failed", "verified pre-update backup is required")
        return record

    def _record(self, profile: Any, state: str, prior: str | None, new: str | None) -> None:
        connection = getattr(self.database, "connection", self.database)
        if connection is None:
            return
        try:
            connection.execute(
                "INSERT INTO updates(id,profile_id,created_at,strategy,prior_version,new_version,state)"
                " VALUES(?,?,?,?,?,?,?)",
                (
                    uuid.uuid4().hex,
                    _id(profile),
                    datetime.now(timezone.utc).isoformat(),
                    self._strategy(profile),
                    prior,
                    new,
                    state,
                ),
            )
            connection.commit()
        except Exception:
            # State recording must never turn a successful update into an
            # unsafe partial rollback; production schema is created by StateDB.
            pass

    def _run(self, argv: list[str] | tuple[str, ...], **kwargs: Any) -> Any:
        kwargs.setdefault("check", True)
        kwargs.setdefault("shell", False)
        return self.runner(maintenance_argv(argv, slice_name="maintenance.slice"), **kwargs)

    def apply(self, profile: Any) -> UpdateResult:
        profile_obj = self._profile(profile)
        if self._is_running(profile_obj):
            raise SafeError("profile_running", "profile is running; it must be stopped before update")
        strategy = self._strategy(profile_obj)
        if strategy == "manual":
            return UpdateResult("manual_only", _id(profile_obj), strategy)
        backup = self._backup(profile_obj)
        if strategy == "steamcmd_in_place":
            try:
                self._run(STEAMCMD_ARGV)
            except Exception as exc:
                self._record(profile_obj, "failed", None, None)
                raise SafeError("update_failed", "update failed") from exc
            self._record(profile_obj, "succeeded", None, None)
            return UpdateResult("succeeded", _id(profile_obj), strategy)
        if strategy == "release_symlink":
            return self._apply_release(profile_obj, backup)
        raise SafeError("update_failed", "update strategy is unavailable")

    @staticmethod
    def _fsync_file(path: Path) -> None:
        with path.open("rb") as stream:
            os.fsync(stream.fileno())

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        fd = os.open(path, flags)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    @classmethod
    def _fsync_tree(cls, root: Path) -> None:
        """Make every regular extracted file and directory durable bottom-up."""
        entries = tuple(root.rglob("*"))
        # Path.rglob() does not promise traversal order.  Flush all regular
        # files first, then directories deepest-first, with a stable lexical
        # tie-breaker so a nested directory can never precede a same-depth
        # top-level file on a different filesystem/provider.
        files = sorted(
            (path for path in entries if path.is_file() and not path.is_symlink()),
            key=lambda path: path.as_posix(),
        )
        directories = sorted(
            (path for path in entries if path.is_dir() and not path.is_symlink()),
            key=lambda path: (-len(path.parts), path.as_posix()),
        )
        for path in files:
            cls._fsync_file(path)
        for path in directories:
            cls._fsync_dir(path)
        cls._fsync_dir(root)

    def _trusted_checksum(self, profile: Any) -> str:
        configured = getattr(profile.update, "sha256", None)
        if not isinstance(configured, str) or not _SHA256_RE.fullmatch(configured):
            raise SafeError("update_failed", "trusted release checksum is required")
        return configured.lower()

    def _download(self, profile: Any, destination: Path) -> None:
        # Resolve trust before invoking any user-supplied downloader or network
        # client. A missing/invalid digest must not cause an untrusted fetch.
        expected_checksum = self._trusted_checksum(profile)
        if self.downloader is not None:
            try:
                self.downloader(profile, destination)
            except TypeError:
                self.downloader(str(profile.update.download_url), destination)
        else:
            with self.http_client.stream("GET", str(profile.update.download_url), timeout=10.0) as response:
                response.raise_for_status()
                with destination.open("wb") as output:
                    total = 0
                    for chunk in response.iter_bytes():
                        if chunk:
                            total += len(chunk)
                            if total > MAX_DOWNLOAD_BYTES:
                                raise SafeError("update_failed", "release download exceeds size limit")
                            output.write(chunk)
        try:
            if destination.stat().st_size > MAX_DOWNLOAD_BYTES:
                raise SafeError("update_failed", "release download exceeds size limit")
        except OSError as exc:
            raise SafeError("update_failed", "release download is unavailable") from exc
        self._fsync_file(destination)
        self._fsync_dir(destination.parent)
        digest = hashlib.sha256()
        with destination.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        if not hmac.compare_digest(digest.hexdigest(), expected_checksum):
            raise SafeError("update_failed", "release checksum verification failed")

    def _safe_extract(self, archive_path: Path, staging: Path, expected_relative: str) -> None:
        archive_size = archive_path.stat().st_size
        try:
            free = shutil.disk_usage(staging).free
            if free < MAX_EXTRACTED_BYTES + MAX_MEMBER_BYTES:
                raise SafeError("update_failed", "insufficient free space for release extraction")
        except OSError as exc:
            raise SafeError("update_failed", "unable to verify release free space") from exc
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path) as archive:
                members = archive.infolist()
                if len(members) > MAX_ARCHIVE_MEMBERS:
                    raise SafeError("update_failed", "release archive has too many members")
                total = 0
                for member in members:
                    if member.file_size > MAX_MEMBER_BYTES:
                        raise SafeError("update_failed", "release archive member is too large")
                    total += member.file_size
                    if total > MAX_EXTRACTED_BYTES or (archive_size and total > archive_size * MAX_COMPRESSION_RATIO):
                        raise SafeError("update_failed", "release archive expansion exceeds limit")
                    target = (staging / member.filename).resolve()
                    if staging.resolve() not in target.parents and target != staging.resolve():
                        raise SafeError("update_failed", "release archive is invalid")
                    if (member.external_attr >> 16) & 0o170000 == 0o120000:
                        raise SafeError("update_failed", "release archive is invalid")
                    archive.extract(member, staging)
            return
        try:
            with tarfile.open(archive_path, "r:*") as archive:
                members = archive.getmembers()
                if len(members) > MAX_ARCHIVE_MEMBERS:
                    raise SafeError("update_failed", "release archive has too many members")
                total = 0
                for member in members:
                    if member.size > MAX_MEMBER_BYTES:
                        raise SafeError("update_failed", "release archive member is too large")
                    total += member.size
                    if total > MAX_EXTRACTED_BYTES or (archive_size and total > archive_size * MAX_COMPRESSION_RATIO):
                        raise SafeError("update_failed", "release archive expansion exceeds limit")
                    target = (staging / member.name).resolve()
                    if staging.resolve() not in target.parents and target != staging.resolve():
                        raise SafeError("update_failed", "release archive is invalid")
                    if member.issym() or member.islnk():
                        raise SafeError("update_failed", "release archive is invalid")
                archive.extractall(staging)
        except tarfile.TarError:
            # A single executable payload is also accepted by fixed profiles.
            target = staging / expected_relative
            with archive_path.open("rb") as source, target.open("wb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
            self._fsync_file(target)
            self._fsync_dir(target.parent)

    def _apply_release(self, profile: Any, backup: Any) -> UpdateResult:
        install = Path(profile.paths.install_root)
        releases = install / "releases"
        releases.mkdir(parents=True, exist_ok=True, mode=0o750)
        current = install / "current"
        prior_target: str | None = os.readlink(current) if current.is_symlink() else None
        prior_version = Path(prior_target).name if prior_target else None
        staging = Path(tempfile.mkdtemp(prefix=".release-", dir=install))
        new_version: str | None = None
        swapped = False
        try:
            if self.stage_release is not None:
                try:
                    value = self.stage_release(profile, staging)
                except TypeError:
                    value = self.stage_release(staging)
                new_version = str(value) if value else None
            else:
                payload = staging / ".download"
                self._download(profile, payload)
                self._safe_extract(payload, staging, str(profile.update.executable_relative_path))
                self._fsync_tree(staging)
            relative = str(profile.update.executable_relative_path)
            if ".." in Path(relative).parts:
                raise SafeError("update_failed", "release executable is invalid")
            executable = staging / relative
            if not executable.is_file():
                raise SafeError("update_failed", "release executable verification failed")
            if profile.update.version_command:
                command = [
                    (str(argument).replace("{staged_executable}", str(executable))
                     if "{staged_executable}" in str(argument) else str(executable)
                     if "/current/" in str(argument) else argument)
                    for argument in profile.update.version_command
                ]
                if not any(str(argument) == str(executable) for argument in command):
                    command.append(str(executable))
                completed = subprocess.run(command, check=True, capture_output=True, text=True, shell=False)
                if not completed.stdout.strip():
                    raise SafeError("update_failed", "release version verification failed")
                if new_version is None:
                    new_version = completed.stdout.strip().splitlines()[0][:128]
            if new_version is None:
                new_version = f"release-{int(self.clock())}"
            destination = releases / new_version
            if destination.exists():
                destination = releases / f"{new_version}-{uuid.uuid4().hex[:8]}"
            self._assert_lease()
            os.replace(staging, destination)
            self._fsync_dir(releases)
            staging = destination
            temporary_link = install / f".current-{uuid.uuid4().hex}"
            os.symlink(os.path.relpath(destination, install), temporary_link, target_is_directory=True)
            self._fsync_dir(install)
            self._assert_lease()
            os.replace(temporary_link, current)
            swapped = True
            self._fsync_dir(install)
            if self.verify_release is not None and not self.verify_release(profile, destination):
                self._record(profile, "failed", prior_version, new_version)
                self._rollback_link(current, prior_target)
                swapped = False
                self._record(profile, "rolled_back", new_version, prior_version)
                raise SafeError("update_failed", "release verification failed; update rolled back")
            self._record(profile, "succeeded", prior_version, new_version)
            return UpdateResult("succeeded", _id(profile), "release_symlink", prior_version, new_version)
        except SafeError:
            if swapped:
                self._rollback_link(current, prior_target)
            if staging.name.startswith(".release-"):
                shutil.rmtree(staging, ignore_errors=True)
            raise
        except BaseException as exc:
            if swapped:
                self._rollback_link(current, prior_target)
                if isinstance(exc, Exception):
                    self._record(profile, "failed", prior_version, new_version)
                    self._record(profile, "rolled_back", new_version, prior_version)
            if staging.name.startswith(".release-"):
                shutil.rmtree(staging, ignore_errors=True)
            if isinstance(exc, BaseException) and not isinstance(exc, Exception):
                raise
            raise SafeError("update_failed", "release update failed") from exc

    @staticmethod
    def _rollback_link(current: Path, prior_target: str | None) -> None:
        temporary = current.parent / f".rollback-{uuid.uuid4().hex}"
        if prior_target is None:
            current.unlink(missing_ok=True)
            UpdateService._fsync_dir(current.parent)
        else:
            os.symlink(prior_target, temporary, target_is_directory=True)
            UpdateService._fsync_dir(current.parent)
            os.replace(temporary, current)
            UpdateService._fsync_dir(current.parent)

    def rollback(self, profile: Any, target: str | None = None) -> UpdateResult:
        profile_obj = self._profile(profile)
        current = Path(profile_obj.paths.install_root) / "current"
        if target is None:
            raise SafeError("update_failed", "rollback target is unavailable")
        target_path = Path(profile_obj.paths.install_root) / "releases" / target
        if not target_path.is_dir() or target_path.is_symlink():
            raise SafeError("update_failed", "rollback target is unavailable")
        self._rollback_link(current, os.path.relpath(target_path, current.parent))
        self._record(profile_obj, "rolled_back", None, target)
        return UpdateResult("rolled_back", _id(profile_obj), self._strategy(profile_obj), None, target)


class UpdateRpcFacade:
    """Typed RPC translator; update policy remains in ``UpdateService``."""

    def __init__(self, services: Mapping[str, UpdateService], profiles: Mapping[str, Any], adapters: Mapping[Any, Any], *, update_service_factory: Callable[..., UpdateService] = UpdateService):
        self.services, self.profiles, self.adapters = services, profiles, adapters
        self._update_service_factory = update_service_factory

    async def _stopped(self, profile: Any) -> None:
        adapter = self.adapters.get(getattr(profile, "id", None)) or self.adapters.get(_id(profile))
        if adapter is None or not hasattr(adapter, "observe"):
            raise SafeError("profile_unavailable", "profile state could not be proven")
        value = adapter.observe(profile)
        observation = await value if inspect.isawaitable(value) else value
        if bool(getattr(observation, "running", False)):
            raise SafeError("profile_running", "profile is running; it must be stopped before update")

    def _stopped_sync(self, profile: Any) -> bool:
        adapter = self.adapters.get(getattr(profile, "id", None)) or self.adapters.get(_id(profile))
        if adapter is None or not hasattr(adapter, "observe"):
            raise SafeError("profile_unavailable", "profile state could not be proven")
        value = adapter.observe(profile)
        if inspect.isawaitable(value):
            value = asyncio.run(value)
        if bool(getattr(value, "running", False)):
            raise SafeError("profile_running", "profile is running; it must be stopped before update")
        return True

    async def check(self, action: Any, actor: str | None = None, request_id: Any = None) -> UpdateStatus:
        key = _id(action.profile_id)
        try:
            return await asyncio.to_thread(self.services[key].check, action.profile_id)
        except KeyError as exc:
            raise SafeError("profile_not_found", "profile was not found") from exc

    async def confirm(self, action: Any, actor: str | None = None, request_id: Any = None, payload: Mapping[str, Any] | None = None, lease_check: Any = None) -> JobAccepted:
        key = _id((payload or {}).get("profile_id", action.profile_id))
        try:
            service = self.services[key]
            profile = self.profiles[key]
        except KeyError as exc:
            raise SafeError("profile_not_found", "profile was not found") from exc
        await self._stopped(profile)
        source_backup = service.backup_service
        def work():
            worker_db = _isolated_database(service.database)
            worker_backup = BackupService(profile, database=worker_db, stopped_check=lambda: self._stopped_sync(profile), free_space=source_backup.free_space, clock=source_backup.clock, tar_runner=source_backup.tar_runner)
            worker_service = self._update_service_factory({key: profile}, database=worker_db, backup_service=worker_backup, runner=service.runner, downloader=service.downloader, stage_release=service.stage_release, verify_release=service.verify_release, stopped_check=lambda *_: self._stopped_sync(profile), http_client=service.http_client, clock=service.clock, lease_check=lease_check)
            try:
                return worker_service.apply(profile), worker_db is not None
            finally:
                _close_database(worker_db)
        result, isolated = await asyncio.to_thread(work)
        if not isolated:
            service._record(profile, getattr(result, "state", "succeeded"), getattr(result, "prior_version", None), getattr(result, "new_version", None))
        return JobAccepted(job_id=__import__("uuid").uuid4().hex, state="running")

    apply = confirm


__all__ = ["UpdateService", "UpdateResult", "STEAMCMD_ARGV"]
