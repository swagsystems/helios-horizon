from __future__ import annotations

import errno
import fcntl
import grp
import hashlib
import hmac
import json
import math
import os
import stat
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator

from .models import ProfileId


OPERATION_LOCK = Path("/run/game-control/operation.lock")
SLOT_LOCK = Path("/run/game-control/slot.lock")
SLOT_METADATA = Path("/run/game-slot/slot.json")
RESERVATION_FILE = Path("/run/game-control/reservation.json")
MAX_RESERVATION_TTL = 30.0


def proc_start_ticks(pid: int) -> int | None:
    """Return Linux's process start time (field 22 of /proc/<pid>/stat)."""
    try:
        text = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, PermissionError, OSError):
        return None
    try:
        _, fields = text.split(") ", 1)
        # fields starts at stat field 3, so field 22 is offset 19.
        return int(fields.split()[19])
    except (ValueError, IndexError):
        return None


def _profile(value: str | ProfileId) -> ProfileId:
    try:
        return value if isinstance(value, ProfileId) else ProfileId(value)
    except ValueError as exc:
        raise ValueError("invalid profile id") from exc


@dataclass(frozen=True)
class SlotObservation:
    owner: str | None
    pid: int | None = None
    proc_start_ticks: int | None = None
    inconsistent: bool = False


@dataclass(frozen=True)
class Reservation:
    profile_id: ProfileId
    operation_id: str
    state_generation: int
    controller_pid: int
    controller_start_ticks: int
    expires_at: float
    operation_kind: str = "lifecycle"
    capability_sha256: str | None = None


def _read_json(path: Path) -> dict[str, object] | None:
    try:
        raw = json.loads(path.read_text())
    except (FileNotFoundError, PermissionError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _reservation_from_json(raw: dict[str, object] | None) -> Reservation | None:
    if raw is None:
        return None
    try:
        profile = _profile(raw["profile_id"])
        operation_id = raw["operation_id"]
        generation = raw["state_generation"]
        pid = raw["controller_pid"]
        ticks = raw["controller_start_ticks"]
        expires = raw["expires_at"]
        operation_kind = raw.get("operation_kind", "lifecycle")
        capability = raw.get("capability_sha256")
        if (
            not isinstance(operation_id, str)
            or not operation_id
            or not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 0
            or not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or not isinstance(ticks, int)
            or isinstance(ticks, bool)
            or ticks < 0
            or not isinstance(expires, (int, float))
            or isinstance(expires, bool)
            or operation_kind not in {"lifecycle", "update"}
            or (
                capability is not None
                and (
                    operation_kind != "update"
                    or not isinstance(capability, str)
                    or len(capability) != 64
                    or any(char not in "0123456789abcdef" for char in capability)
                )
            )
        ):
            return None
        try:
            expiry = float(expires)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(expiry):
            return None
        return Reservation(
            profile, operation_id, generation, pid, ticks, expiry, operation_kind,
            capability,
        )
    except (KeyError, TypeError, ValueError):
        return None


def _lock_open(path: Path, flags: int) -> int:
    # Lock files are installed by tmpfiles; never create or replace one here.
    fd = os.open(path, flags | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        mode = stat.S_IMODE(info.st_mode)
        if Path(path) in (SLOT_LOCK, OPERATION_LOCK):
            try:
                gameslot_gid = grp.getgrnam("gameslot").gr_gid
            except KeyError as exc:
                raise ValueError("gameslot group is unavailable") from exc
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != 0
                or info.st_gid != gameslot_gid
                or mode != 0o660
                or info.st_nlink != 1
            ):
                raise ValueError("slot lock is not root:gameslot 0660")
        elif (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != 0
            or mode & 0o002
            or mode & 0o600 != 0o600
        ):
            raise ValueError("lock is not root-owned and secure")
    except BaseException:
        os.close(fd)
        raise
    return fd


@contextmanager
def operation_transaction(path: Path = OPERATION_LOCK) -> Iterator[int]:
    fd = _lock_open(path, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield fd
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


class OperationLock:
    """A bounded-scope operation lock transaction.

    The slot runner uses a shared transaction directly; root transitions use
    the exclusive mode exposed here.  Neither mode ever acquires the slot.
    """

    def __init__(self, path: Path = OPERATION_LOCK, *, shared: bool = False):
        self.path = Path(path)
        self.shared = shared
        self.fd: int | None = None

    def __enter__(self) -> int:
        self.fd = _lock_open(self.path, os.O_RDWR)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_SH if self.shared else fcntl.LOCK_EX)
        except BaseException:
            os.close(self.fd)
            self.fd = None
            raise
        return self.fd

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


def _atomic_json(path: Path, payload: dict[str, object], mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        os.fchmod(fd, mode)
        try:
            os.fchown(fd, 0, 0)
        except PermissionError:
            pass
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary_path.unlink(missing_ok=True)


def _capability_digest(token: str) -> str:
    if (
        not isinstance(token, str)
        or len(token) != 64
        or any(char not in "0123456789abcdef" for char in token)
    ):
        raise ValueError("handoff capability is malformed")
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def _reservation_payload(reservation: Reservation) -> dict[str, object]:
    payload: dict[str, object] = {
        "profile_id": reservation.profile_id.value,
        "operation_id": reservation.operation_id,
        "state_generation": reservation.state_generation,
        "controller_pid": reservation.controller_pid,
        "controller_start_ticks": reservation.controller_start_ticks,
        "expires_at": reservation.expires_at,
        "operation_kind": reservation.operation_kind,
    }
    if reservation.capability_sha256 is not None:
        payload["capability_sha256"] = reservation.capability_sha256
    return payload


class ReservationStore:
    def __init__(
        self,
        operation_path: Path = OPERATION_LOCK,
        reservation_path: Path = RESERVATION_FILE,
        *,
        clock=time.time,
        pid_start_ticks=proc_start_ticks,
    ):
        self.operation_path = Path(operation_path)
        self.reservation_path = Path(reservation_path)
        self.clock = clock
        self.pid_start_ticks = pid_start_ticks

    def read(self) -> Reservation | None:
        return _reservation_from_json(_read_json(self.reservation_path))

    def _read_for_admission(self) -> Reservation | None:
        """Read the reservation without treating corruption as absence."""
        try:
            info = self.reservation_path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ValueError("reservation state is unavailable") from exc
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_uid != 0
            or stat.S_IMODE(info.st_mode) & 0o022
            or not stat.S_IMODE(info.st_mode) & 0o400
            or info.st_size > 64 * 1024
        ):
            raise ValueError("reservation state is unsafe")
        value = _reservation_from_json(_read_json(self.reservation_path))
        if value is None:
            raise ValueError("reservation state is malformed")
        return value

    def _expected_controller(
        self,
        controller_pid: int | None,
        controller_start_ticks: int | None,
    ) -> tuple[int, int]:
        pid = os.getpid() if controller_pid is None else controller_pid
        ticks = self.pid_start_ticks(pid) if controller_start_ticks is None else controller_start_ticks
        if (
            isinstance(pid, bool)
            or not isinstance(pid, int)
            or pid <= 0
            or isinstance(ticks, bool)
            or not isinstance(ticks, int)
            or ticks < 0
        ):
            raise ValueError("controller identity cannot be proven")
        return pid, ticks

    def reserve(
        self,
        profile: str | ProfileId,
        operation_id: str,
        ttl: float,
        *,
        state_generation: int = 0,
        controller_pid: int | None = None,
        controller_start_ticks: int | None = None,
        operation_kind: str = "lifecycle",
        capability_sha256: str | None = None,
    ) -> Reservation:
        if os.geteuid() != 0:
            raise PermissionError("only root may create reservations")
        profile_id = _profile(profile)
        if not isinstance(operation_id, str) or not operation_id:
            raise ValueError("invalid operation id")
        if not isinstance(ttl, (int, float)) or isinstance(ttl, bool) or not math.isfinite(float(ttl)):
            raise ValueError("invalid reservation ttl")
        if not 0 < ttl <= MAX_RESERVATION_TTL:
            raise ValueError("reservation ttl must be at most 30 seconds")
        if not isinstance(state_generation, int) or state_generation < 0:
            raise ValueError("invalid state generation")
        if operation_kind not in {"lifecycle", "update"}:
            raise ValueError("invalid operation kind")
        if capability_sha256 is not None:
            if (
                operation_kind != "update"
                or not isinstance(capability_sha256, str)
                or len(capability_sha256) != 64
                or any(char not in "0123456789abcdef" for char in capability_sha256)
            ):
                raise ValueError("invalid handoff capability")
        pid = os.getpid() if controller_pid is None else controller_pid
        ticks = self.pid_start_ticks(pid) if controller_start_ticks is None else controller_start_ticks
        if ticks is None:
            raise ValueError("controller process does not exist")
        with operation_transaction(self.operation_path):
            reservation = Reservation(
                profile_id,
                operation_id,
                state_generation,
                pid,
                ticks,
                self.clock() + ttl,
                operation_kind,
                capability_sha256,
            )
            _atomic_json(self.reservation_path, _reservation_payload(reservation))
        return reservation

    def reserve_if_available(
        self,
        profile: str | ProfileId,
        operation_id: str,
        ttl: float,
        *,
        state_generation: int = 0,
        controller_pid: int | None = None,
        controller_start_ticks: int | None = None,
        availability_check: Callable[[], bool] | None = None,
        operation_kind: str = "lifecycle",
        generation_provider: Callable[[], int] | None = None,
        capability_sha256: str | None = None,
    ) -> Reservation:
        """Atomically check the live owner and commit a reservation.

        Controllers must not perform a read followed by ``reserve``: another
        writer can win the gap between those calls.  This method keeps both
        operations inside the same exclusive operation-lock transaction.
        """
        if os.geteuid() != 0:
            raise PermissionError("only root may create reservations")
        profile_id = _profile(profile)
        if not isinstance(operation_id, str) or not operation_id:
            raise ValueError("invalid operation id")
        if not isinstance(ttl, (int, float)) or isinstance(ttl, bool) or not math.isfinite(float(ttl)):
            raise ValueError("invalid reservation ttl")
        if not 0 < ttl <= MAX_RESERVATION_TTL:
            raise ValueError("reservation ttl must be at most 30 seconds")
        if not isinstance(state_generation, int) or state_generation < 0:
            raise ValueError("invalid state generation")
        if operation_kind not in {"lifecycle", "update"}:
            raise ValueError("invalid operation kind")
        if capability_sha256 is not None:
            if (
                operation_kind != "update"
                or not isinstance(capability_sha256, str)
                or len(capability_sha256) != 64
                or any(char not in "0123456789abcdef" for char in capability_sha256)
            ):
                raise ValueError("invalid handoff capability")
        pid = os.getpid() if controller_pid is None else controller_pid
        ticks = self.pid_start_ticks(pid) if controller_start_ticks is None else controller_start_ticks
        if ticks is None:
            raise ValueError("controller process does not exist")
        with operation_transaction(self.operation_path):
            current = self._read_for_admission()
            if current is not None and self._live(current):
                # Acquisition never takes over a live lease, even when a
                # durable operation id is reused.  PID/start-ticks are part
                # of ownership and a restarted process must wait for stale
                # reconciliation instead of inheriting the old lease.
                raise BlockingIOError("reservation is already live")
            # A lifecycle caller can supply its stopped-state predicate here so
            # the observation and reservation commit share the same exclusive
            # operation transaction.  This closes the read-then-reserve race
            # where a start could win between two independent calls.
            if availability_check is not None and not availability_check():
                raise BlockingIOError("reservation precondition is not satisfied")
            if generation_provider is not None:
                generated = generation_provider()
                if isinstance(generated, bool) or not isinstance(generated, int) or generated < 0:
                    raise ValueError("invalid state generation")
                state_generation = generated
            reservation = Reservation(
                profile_id, operation_id, state_generation, pid, ticks,
                self.clock() + ttl,
                operation_kind,
                capability_sha256,
            )
            _atomic_json(self.reservation_path, _reservation_payload(reservation))
        return reservation

    def release_if_owned(
        self,
        profile: str | ProfileId,
        operation_id: str,
        state_generation: int = 0,
        *,
        operation_kind: str | None = None,
        controller_pid: int | None = None,
        controller_start_ticks: int | None = None,
    ) -> bool:
        profile_id = _profile(profile)
        controller_pid, controller_start_ticks = self._expected_controller(
            controller_pid, controller_start_ticks,
        )
        with operation_transaction(self.operation_path):
            current = _reservation_from_json(_read_json(self.reservation_path))
            if (
                current is None
                or current.profile_id != profile_id
                or current.operation_id != operation_id
                or current.state_generation != state_generation
                or (operation_kind is not None and current.operation_kind != operation_kind)
                or current.controller_pid != controller_pid
                or current.controller_start_ticks != controller_start_ticks
            ):
                return False
            self.reservation_path.unlink(missing_ok=True)
            return True

    def release_if_owned_locked(
        self,
        profile: str | ProfileId,
        operation_id: str,
        state_generation: int = 0,
        *,
        operation_kind: str | None = None,
        controller_pid: int | None = None,
        controller_start_ticks: int | None = None,
    ) -> bool:
        """Release an exact reservation while operation.lock is held."""
        profile_id = _profile(profile)
        controller_pid, controller_start_ticks = self._expected_controller(
            controller_pid, controller_start_ticks,
        )
        current = _reservation_from_json(_read_json(self.reservation_path))
        if (
            current is None
            or current.profile_id != profile_id
            or current.operation_id != operation_id
            or current.state_generation != state_generation
            or (operation_kind is not None and current.operation_kind != operation_kind)
            or current.controller_pid != controller_pid
            or current.controller_start_ticks != controller_start_ticks
        ):
            return False
        self.reservation_path.unlink(missing_ok=True)
        return True

    def owns_live(
        self,
        profile: str | ProfileId,
        operation_id: str,
        state_generation: int = 0,
        *,
        operation_kind: str | None = None,
        controller_pid: int | None = None,
        controller_start_ticks: int | None = None,
    ) -> bool:
        """Atomically verify exact ownership, expiry, and controller liveness."""
        profile_id = _profile(profile)
        controller_pid, controller_start_ticks = self._expected_controller(
            controller_pid, controller_start_ticks,
        )
        with operation_transaction(self.operation_path):
            current = _reservation_from_json(_read_json(self.reservation_path))
            return bool(
                current is not None
                and current.profile_id == profile_id
                and current.operation_id == operation_id
                and current.state_generation == state_generation
                and (operation_kind is None or current.operation_kind == operation_kind)
                and current.controller_pid == controller_pid
                and current.controller_start_ticks == controller_start_ticks
                and self._live(current)
            )

    def owns_live_locked(
        self,
        profile: str | ProfileId,
        operation_id: str,
        state_generation: int = 0,
        *,
        controller_pid: int | None = None,
        controller_start_ticks: int | None = None,
    ) -> bool:
        """Verify ownership while the caller already holds operation.lock.

        This deliberately performs no nested flock acquisition. It is for a
        final check-to-publish critical section that has already acquired an
        exclusive ``OperationLock``.
        """
        profile_id = _profile(profile)
        controller_pid, controller_start_ticks = self._expected_controller(
            controller_pid, controller_start_ticks,
        )
        current = _reservation_from_json(_read_json(self.reservation_path))
        return bool(
            current is not None
            and current.profile_id == profile_id
            and current.operation_id == operation_id
            and current.state_generation == state_generation
            and current.operation_kind == "update"
            and current.controller_pid == controller_pid
            and current.controller_start_ticks == controller_start_ticks
            and self._live(current)
        )

    def transfer_if_owned(
        self,
        profile: str | ProfileId,
        operation_id: str,
        state_generation: int,
        target_profile: str | ProfileId,
        target_operation_id: str,
        ttl: float = 30.0,
        *,
        controller_pid: int | None = None,
        controller_start_ticks: int | None = None,
    ) -> Reservation:
        """Atomically hand a lease to rollback/source ownership."""
        source = _profile(profile)
        target = _profile(target_profile)
        controller_pid, controller_start_ticks = self._expected_controller(
            controller_pid, controller_start_ticks,
        )
        with operation_transaction(self.operation_path):
            current = _reservation_from_json(_read_json(self.reservation_path))
            if (
                current is None
                or current.profile_id != source
                or current.operation_id != operation_id
                or current.state_generation != state_generation
                or current.controller_pid != controller_pid
                or current.controller_start_ticks != controller_start_ticks
            ):
                raise BlockingIOError("reservation ownership changed")
            renewed = Reservation(
                target, target_operation_id, state_generation,
                current.controller_pid, current.controller_start_ticks,
                self.clock() + ttl,
                current.operation_kind,
                current.capability_sha256,
            )
            _atomic_json(self.reservation_path, _reservation_payload(renewed))
            return renewed

    def renew_if_owned(
        self,
        profile: str | ProfileId,
        operation_id: str,
        ttl: float,
        *,
        state_generation: int = 0,
        operation_kind: str | None = None,
        controller_pid: int | None = None,
        controller_start_ticks: int | None = None,
    ) -> Reservation:
        """Extend only the lease this operation currently owns."""
        profile_id = _profile(profile)
        controller_pid, controller_start_ticks = self._expected_controller(
            controller_pid, controller_start_ticks,
        )
        with operation_transaction(self.operation_path):
            current = _reservation_from_json(_read_json(self.reservation_path))
            if (
                current is None
                or current.profile_id != profile_id
                or current.operation_id != operation_id
                or current.state_generation != state_generation
                or (operation_kind is not None and current.operation_kind != operation_kind)
                or current.controller_pid != controller_pid
                or current.controller_start_ticks != controller_start_ticks
                or not self._live(current)
            ):
                raise BlockingIOError("reservation ownership changed")
            renewed = Reservation(
                current.profile_id, current.operation_id, current.state_generation,
                current.controller_pid, current.controller_start_ticks,
                self.clock() + ttl,
                current.operation_kind,
                current.capability_sha256,
            )
            _atomic_json(self.reservation_path, _reservation_payload(renewed))
            return renewed

    def authorize_handoff(
        self,
        profile: str | ProfileId,
        capability_token: str,
        *,
        operation_kind: str = "update",
    ) -> Reservation:
        """Authorize a bounded cross-process action under the exact live update lease."""
        profile_id = _profile(profile)
        if operation_kind not in {"lifecycle", "update"}:
            raise ValueError("invalid operation kind")
        expected = _capability_digest(capability_token)
        with operation_transaction(self.operation_path):
            current = self._read_for_admission()
            if (
                current is None
                or not self._live(current)
                or current.profile_id != profile_id
                or current.operation_kind != operation_kind
                or current.capability_sha256 is None
                or not hmac.compare_digest(current.capability_sha256, expected)
            ):
                raise BlockingIOError("handoff capability does not match the live reservation")
            return current

    def valid_for_runner(self, profile: str | ProfileId) -> bool | None:
        """Return True for a matching reservation, False for a live mismatch.

        None means absent or stale.  Runners deliberately do not rewrite this
        root-owned file; slotd's reconciliation removes stale records.
        """
        requested = _profile(profile)
        reservation = self.read()
        if reservation is None or not self._live(reservation):
            return None
        return reservation.profile_id == requested and reservation.operation_kind == "lifecycle"

    validate_for_runner = valid_for_runner

    def live_update(self, profile: str | ProfileId | None = None) -> Reservation | None:
        """Return the live ``operation_kind="update"`` reservation, if any.

        Only the updater handoff reservation qualifies: a lifecycle reservation
        held by a start/stop or a generic maintenance job is not an update, and
        an expired or dead-controller record is reported as absent so a stale
        reservation cannot permanently gray a profile.
        """
        try:
            requested = None if profile is None else _profile(profile)
        except ValueError:
            return None
        reservation = self.read()
        if reservation is None or reservation.operation_kind != "update":
            return None
        if requested is not None and reservation.profile_id != requested:
            return None
        try:
            if not self._live(reservation):
                return None
        except (OSError, ValueError, PermissionError):
            return None
        return reservation

    def reconcile(self) -> bool:
        """Remove an invalid, expired, or dead-controller reservation."""
        if os.geteuid() != 0:
            raise PermissionError("only root may reconcile reservations")
        reservation = self.read()
        if reservation is not None and self._live(reservation):
            return False
        with operation_transaction(self.operation_path):
            current = _reservation_from_json(_read_json(self.reservation_path))
            if current is None or not self._live(current):
                if self.reservation_path.exists():
                    self.reservation_path.unlink(missing_ok=True)
                    return True
        return False

    def _live(self, reservation: Reservation) -> bool:
        now = self.clock()
        return (
            now < reservation.expires_at <= now + MAX_RESERVATION_TTL
            and self.pid_start_ticks(reservation.controller_pid) == reservation.controller_start_ticks
        )


class SlotInspector:
    def __init__(
        self,
        slot_path: Path = SLOT_LOCK,
        metadata_path: Path = SLOT_METADATA,
        *,
        pid_start_ticks=proc_start_ticks,
    ):
        self.slot_path = Path(slot_path)
        self.metadata_path = Path(metadata_path)
        self.pid_start_ticks = pid_start_ticks

    def _clear_metadata(self) -> None:
        try:
            self.metadata_path.unlink()
        except FileNotFoundError:
            pass

    def observe(self) -> SlotObservation:
        fd = _lock_open(self.slot_path, os.O_RDWR)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return self._observe_held_slot()
            self._clear_metadata()
            return SlotObservation(owner=None)
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _observe_held_slot(self) -> SlotObservation:
        raw = _read_json(self.metadata_path)
        if raw is None:
            return SlotObservation(owner=None, inconsistent=True)
        try:
            owner = _profile(raw.get("profile_id", raw.get("profile")))
            pid = raw["pid"]
            ticks = raw["proc_start_ticks"]
            if not isinstance(pid, int) or pid <= 0 or not isinstance(ticks, int) or ticks < 0:
                raise ValueError
            if self.pid_start_ticks(pid) != ticks:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            return SlotObservation(owner=None, inconsistent=True)
        return SlotObservation(owner=owner.value, pid=pid, proc_start_ticks=ticks)
