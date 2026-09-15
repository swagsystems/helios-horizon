"""Controller-owned local-payload retirement.

Retirement removes local backup *payloads* while preserving the ``backups`` and
``backup_protections`` catalog history forever.  The RPC boundary never carries
filesystem paths or manifest entries: an operator supplies only a root-trusted
manifest digest (which doubles as the operation id), a phase, and a
confirmation id.  Archive paths are derived from the trusted profile
``backup_root`` and the validated backup id.

Enforced rules:

* Additive durable ledger only.  No ``DELETE`` of catalog/protection/history
  rows, no foreign-key bypass, no journal cleanup.
* Quarantine is a same-filesystem, no-overwrite rename
  (``renameat2(RENAME_NOREPLACE)``); if the kernel/libc cannot provide it the
  operation fails closed rather than falling back to ``os.replace``.
* Every eligibility and identity check is repeated before the first mutation
  and again immediately before each file mutation; lease ownership is asserted
  at every boundary (including after a long content hash).
* Purge is separately confirmed, requires a durable per-entry purge intent, and
  requires the immutable, content-bound verified-copy receipt to match exact
  membership.  No destination (B2, laptop vault, ...) is assumed: the receipt
  names its own bounded destination identity.
* Read-only status/reconcile never creates tables, files, or filesystem state.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from .errors import SafeError
from .state_db import RETIREMENT_LEDGER_DDL


MANIFEST_DIR = Path("/etc/game-control/retirement.d")
SENDER_LOCK_PATH = Path("/var/lib/horizon-laptop-backup/sender.lock")

MANIFEST_SCHEMA = 1
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_MANIFEST_ENTRIES = 4096
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_BACKUP_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
# Bounded, opaque destination identity.  Nothing in generic code assumes B2.
_DESTINATION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

_AT_FDCWD = -100
_RENAME_NOREPLACE = 1


class PayloadState(StrEnum):
    PRESENT = "present"
    MISSING = "missing"
    PREPARED = "prepared"
    QUARANTINED = "quarantined"
    PURGE_PREPARED = "purge_prepared"
    PURGED = "purged"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


LEDGER_STATES = frozenset(
    {
        PayloadState.PREPARED,
        PayloadState.QUARANTINED,
        PayloadState.PURGE_PREPARED,
        PayloadState.PURGED,
        PayloadState.ROLLED_BACK,
        PayloadState.FAILED,
        PayloadState.AMBIGUOUS,
    }
)

# Any of these makes a payload unavailable for local restore/protection.
BLOCKING_STATES = frozenset(
    {
        PayloadState.PREPARED,
        PayloadState.QUARANTINED,
        PayloadState.PURGE_PREPARED,
        PayloadState.PURGED,
        PayloadState.FAILED,
        PayloadState.AMBIGUOUS,
    }
)

RETIREMENT_PHASES = ("quarantine", "purge", "rollback")

# Allowed durable state transitions.  Terminal ``purged`` is never reset, and
# ``purge_prepared`` can only advance to ``purged``/``failed``.
_TRANSITIONS: dict[str, frozenset[str]] = {
    PayloadState.PREPARED.value: frozenset(
        {PayloadState.PREPARED.value, PayloadState.QUARANTINED.value, PayloadState.FAILED.value}
    ),
    PayloadState.QUARANTINED.value: frozenset(
        {
            PayloadState.QUARANTINED.value,
            PayloadState.PURGE_PREPARED.value,
            PayloadState.ROLLED_BACK.value,
            PayloadState.FAILED.value,
        }
    ),
    PayloadState.PURGE_PREPARED.value: frozenset(
        {PayloadState.PURGE_PREPARED.value, PayloadState.PURGED.value, PayloadState.FAILED.value}
    ),
    PayloadState.PURGED.value: frozenset({PayloadState.PURGED.value}),
    PayloadState.ROLLED_BACK.value: frozenset(
        {
            PayloadState.ROLLED_BACK.value,
            PayloadState.PREPARED.value,
            PayloadState.FAILED.value,
        }
    ),
    PayloadState.FAILED.value: frozenset({PayloadState.FAILED.value, PayloadState.PREPARED.value}),
    PayloadState.AMBIGUOUS.value: frozenset(
        {PayloadState.AMBIGUOUS.value, PayloadState.PREPARED.value}
    ),
}

# Phase admission: which existing per-entry states may be resumed or retried.
_PHASE_RESUMABLE: dict[str, frozenset[str]] = {
    "quarantine": frozenset(
        {
            PayloadState.PREPARED.value,
            PayloadState.QUARANTINED.value,
            PayloadState.ROLLED_BACK.value,
        }
    ),
    "purge": frozenset(
        {
            PayloadState.QUARANTINED.value,
            PayloadState.PURGE_PREPARED.value,
            PayloadState.PURGED.value,
        }
    ),
    "rollback": frozenset(
        {
            PayloadState.PREPARED.value,
            PayloadState.QUARANTINED.value,
            PayloadState.ROLLED_BACK.value,
        }
    ),
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


# --------------------------------------------------------------------------- #
# no-overwrite rename
# --------------------------------------------------------------------------- #


_libc: Any = None


def _load_libc() -> Any:
    global _libc
    if _libc is None:
        name = ctypes.util.find_library("c") or "libc.so.6"
        _libc = ctypes.CDLL(name, use_errno=True)
    return _libc


def noreplace_supported() -> bool:
    """Return whether the platform exposes ``renameat2`` for fail-closed use."""

    try:
        return hasattr(_load_libc(), "renameat2")
    except OSError:
        return False


def rename_noreplace(source: Path, destination: Path) -> None:
    """Atomically rename with ``RENAME_NOREPLACE``; never clobber a target."""

    libc = _load_libc()
    function = getattr(libc, "renameat2", None)
    if function is None:
        raise SafeError("retirement_failed", "no-overwrite rename is unavailable on this host")
    function.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    function.restype = ctypes.c_int
    result = function(
        _AT_FDCWD,
        os.fsencode(str(source)),
        _AT_FDCWD,
        os.fsencode(str(destination)),
        _RENAME_NOREPLACE,
    )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        raise SafeError("retirement_failed", "target already exists")
    if error in (errno.ENOSYS, errno.EINVAL, errno.ENOTSUP):
        raise SafeError("retirement_failed", "no-overwrite rename is unavailable on this host")
    raise SafeError("retirement_failed", "no-overwrite rename failed") from OSError(
        error, os.strerror(error)
    )


# --------------------------------------------------------------------------- #
# trusted manifest
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ManifestEntry:
    backup_id: str
    profile_id: str
    device: int
    inode: int
    size_bytes: int
    mtime_ns: int
    ctime_ns: int
    sha256: str


@dataclass(frozen=True)
class RemoteMember:
    backup_id: str
    profile_id: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class RemoteReceipt:
    destination_id: str
    verified: bool
    verified_at: str
    receipt_sha256: str
    members: tuple[RemoteMember, ...]


@dataclass(frozen=True)
class RetirementManifest:
    operation_id: str
    proposal_sha256: str
    entries: tuple[ManifestEntry, ...]
    receipt: RemoteReceipt

    @property
    def total_bytes(self) -> int:
        return sum(entry.size_bytes for entry in self.entries)

    @property
    def profile_ids(self) -> tuple[str, ...]:
        return tuple(sorted({entry.profile_id for entry in self.entries}))


def _reject(condition: bool, message: str) -> None:
    if condition:
        raise SafeError("retirement_failed", message)


def _assert_no_symlink_ancestors(path: Path) -> None:
    current = Path(path)
    for candidate in [current, *current.parents]:
        try:
            info = os.lstat(candidate)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise SafeError("retirement_failed", "path is unavailable") from exc
        if stat.S_ISLNK(info.st_mode):
            raise SafeError("retirement_failed", "path has a symlink ancestor")


def _assert_trusted_directory(directory: Path, owner: int) -> None:
    _assert_no_symlink_ancestors(directory)
    try:
        info = os.lstat(directory)
    except OSError as exc:
        raise SafeError("retirement_failed", "retirement manifest directory is unavailable") from exc
    if not stat.S_ISDIR(info.st_mode):
        raise SafeError("retirement_failed", "retirement manifest directory is not a directory")
    if info.st_uid != owner or stat.S_IMODE(info.st_mode) & 0o022:
        raise SafeError("retirement_failed", "retirement manifest directory is not trusted")


def _load_bytes(path: Path, owner: int) -> bytes:
    try:
        before = os.lstat(path)
    except OSError as exc:
        raise SafeError("retirement_failed", "retirement manifest was not found") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise SafeError("retirement_failed", "retirement manifest is not a regular file")
    if before.st_nlink != 1:
        raise SafeError("retirement_failed", "retirement manifest is multiply linked")
    if before.st_uid != owner or stat.S_IMODE(before.st_mode) & 0o022:
        raise SafeError("retirement_failed", "retirement manifest is not trusted")
    if before.st_size > MAX_MANIFEST_BYTES:
        raise SafeError("retirement_failed", "retirement manifest is too large")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        bound = os.fstat(fd)
        if (bound.st_dev, bound.st_ino) != (before.st_dev, before.st_ino):
            raise SafeError("retirement_failed", "retirement manifest changed while opening")
        data = b""
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            data += chunk
            if len(data) > MAX_MANIFEST_BYTES:
                raise SafeError("retirement_failed", "retirement manifest is too large")
    finally:
        os.close(fd)
    return data


def _parse_entry(value: Any) -> ManifestEntry:
    _reject(not isinstance(value, dict), "manifest entry is not an object")
    expected = {
        "backup_id", "profile_id", "device", "inode", "size_bytes",
        "mtime_ns", "ctime_ns", "sha256",
    }
    _reject(set(value) != expected, "manifest entry has an unexpected shape")
    backup_id = value["backup_id"]
    _reject(not isinstance(backup_id, str) or not _BACKUP_ID_RE.fullmatch(backup_id), "manifest entry id is invalid")
    profile_id = value["profile_id"]
    _reject(not isinstance(profile_id, str) or not profile_id or len(profile_id) > 64, "manifest profile is invalid")
    numbers: dict[str, int] = {}
    for key in ("device", "inode", "size_bytes", "mtime_ns", "ctime_ns"):
        raw = value[key]
        _reject(isinstance(raw, bool) or not isinstance(raw, int) or raw < 0, f"manifest {key} is invalid")
        numbers[key] = int(raw)
    sha = value["sha256"]
    _reject(not isinstance(sha, str) or not _DIGEST_RE.fullmatch(sha), "manifest sha256 is invalid")
    return ManifestEntry(
        backup_id=backup_id,
        profile_id=profile_id,
        device=numbers["device"],
        inode=numbers["inode"],
        size_bytes=numbers["size_bytes"],
        mtime_ns=numbers["mtime_ns"],
        ctime_ns=numbers["ctime_ns"],
        sha256=sha,
    )


def _parse_member(value: Any) -> RemoteMember:
    _reject(not isinstance(value, dict), "receipt member is not an object")
    expected = {"backup_id", "profile_id", "size_bytes", "sha256"}
    _reject(set(value) != expected, "receipt member has an unexpected shape")
    backup_id = value["backup_id"]
    _reject(not isinstance(backup_id, str) or not _BACKUP_ID_RE.fullmatch(backup_id), "receipt member id is invalid")
    profile_id = value["profile_id"]
    _reject(not isinstance(profile_id, str) or not profile_id or len(profile_id) > 64, "receipt member profile is invalid")
    size = value["size_bytes"]
    _reject(isinstance(size, bool) or not isinstance(size, int) or size < 0, "receipt member size is invalid")
    sha = value["sha256"]
    _reject(not isinstance(sha, str) or not _DIGEST_RE.fullmatch(sha), "receipt member sha256 is invalid")
    return RemoteMember(backup_id=backup_id, profile_id=profile_id, size_bytes=int(size), sha256=sha)


def receipt_digest(destination_id: str, verified_at: str, members: Iterable[RemoteMember | Mapping[str, Any]]) -> str:
    """Return the content digest binding a verified-copy receipt."""

    normalized = []
    for member in members:
        if isinstance(member, RemoteMember):
            normalized.append({
                "backup_id": member.backup_id,
                "profile_id": member.profile_id,
                "size_bytes": member.size_bytes,
                "sha256": member.sha256,
            })
        else:
            normalized.append(dict(member))
    normalized.sort(key=lambda item: (item["backup_id"], item["profile_id"]))
    return hashlib.sha256(
        _canonical({"destination_id": destination_id, "verified_at": verified_at, "members": normalized})
    ).hexdigest()


def parse_manifest(data: bytes) -> tuple[str, tuple[ManifestEntry, ...], RemoteReceipt]:
    """Parse and validate a manifest body; no filesystem or network access."""

    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SafeError("retirement_failed", "retirement manifest is not valid JSON") from exc
    _reject(not isinstance(value, dict), "retirement manifest is not an object")
    _reject(
        set(value) != {"schema", "proposal_sha256", "entries", "receipt"},
        "retirement manifest has an unexpected shape",
    )
    _reject(value["schema"] != MANIFEST_SCHEMA, "retirement manifest schema is not supported")
    proposal = value["proposal_sha256"]
    _reject(not isinstance(proposal, str) or not _DIGEST_RE.fullmatch(proposal), "retirement proposal digest is invalid")
    raw_entries = value["entries"]
    _reject(not isinstance(raw_entries, list) or not 1 <= len(raw_entries) <= MAX_MANIFEST_ENTRIES, "retirement manifest entries are invalid")
    entries = tuple(_parse_entry(item) for item in raw_entries)
    ids = [item.backup_id for item in entries]
    _reject(len(set(ids)) != len(ids), "retirement manifest has duplicate entries")
    pairs = [(item.profile_id, item.backup_id) for item in entries]
    _reject(len(set(pairs)) != len(pairs), "retirement manifest has duplicate profile entries")

    raw_receipt = value["receipt"]
    _reject(not isinstance(raw_receipt, dict), "receipt is not an object")
    _reject(
        set(raw_receipt) != {"destination_id", "verified", "verified_at", "receipt_sha256", "members"},
        "receipt has an unexpected shape",
    )
    destination = raw_receipt["destination_id"]
    _reject(not isinstance(destination, str) or not _DESTINATION_RE.fullmatch(destination), "receipt destination is invalid")
    _reject(raw_receipt["verified"] is not True, "receipt is not verified")
    verified_at = raw_receipt["verified_at"]
    _reject(not isinstance(verified_at, str) or not _is_rfc3339(verified_at), "receipt verification time is invalid")
    raw_members = raw_receipt["members"]
    _reject(not isinstance(raw_members, list), "receipt members are invalid")
    members = tuple(_parse_member(item) for item in raw_members)
    by_id = {member.backup_id: member for member in members}
    _reject(len(by_id) != len(members), "receipt has duplicate members")
    _reject(set(by_id) != set(ids), "receipt membership does not match the plan")
    for entry in entries:
        member = by_id[entry.backup_id]
        _reject(member.profile_id != entry.profile_id, "receipt profile does not match the plan")
        _reject(member.size_bytes != entry.size_bytes, "receipt size does not match the plan")
        _reject(member.sha256 != entry.sha256, "receipt digest does not match the plan")
    bound = raw_receipt["receipt_sha256"]
    _reject(not isinstance(bound, str) or not _DIGEST_RE.fullmatch(bound), "receipt digest is invalid")
    _reject(
        receipt_digest(destination, verified_at, members) != bound,
        "receipt digest does not bind its content",
    )
    receipt = RemoteReceipt(
        destination_id=destination, verified=True, verified_at=verified_at,
        receipt_sha256=bound, members=members,
    )
    return proposal, entries, receipt


def _is_rfc3339(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def load_manifest(
    operation_id: str,
    *,
    directory: Path | str = MANIFEST_DIR,
    owner: int = 0,
) -> RetirementManifest:
    """Load one immutable, root-trusted retirement manifest by content digest."""

    _reject(not isinstance(operation_id, str) or not _DIGEST_RE.fullmatch(operation_id), "retirement operation id is invalid")
    base = Path(directory)
    _assert_trusted_directory(base, owner)
    path = base / f"{operation_id}.json"
    data = _load_bytes(path, owner)
    digest = hashlib.sha256(data).hexdigest()
    _reject(digest != operation_id, "retirement manifest digest does not match its name")
    proposal, entries, receipt = parse_manifest(data)
    return RetirementManifest(
        operation_id=operation_id, proposal_sha256=proposal, entries=entries, receipt=receipt
    )


# --------------------------------------------------------------------------- #
# sender interlock
# --------------------------------------------------------------------------- #


class SenderInterlock:
    """Exclusive ``flock`` interlock with the standalone laptop-sender lock."""

    def __init__(self, path: Path | str = SENDER_LOCK_PATH, *, owner: int = 0) -> None:
        self.path = Path(path)
        self.owner = owner
        self._fd: int | None = None

    def _assert_parent(self) -> None:
        parent = self.path.parent
        _assert_no_symlink_ancestors(parent)
        try:
            info = os.lstat(parent)
        except OSError as exc:
            raise SafeError("retirement_failed", "sender lock directory is unavailable") from exc
        if not stat.S_ISDIR(info.st_mode):
            raise SafeError("retirement_failed", "sender lock parent is not a directory")
        if info.st_uid != self.owner or stat.S_IMODE(info.st_mode) & 0o022:
            raise SafeError("retirement_failed", "sender lock parent is not trusted")

    def probe(self) -> bool:
        """Read-only availability probe.

        Opens the EXISTING lock ``O_RDONLY|O_NOFOLLOW`` (Linux ``flock`` works
        on a read-only descriptor) and never creates, repairs, or truncates the
        file.  A missing lock is fail-closed (unavailable), matching the
        controller's read-only sandbox where the lock is not writable.
        """

        try:
            self._assert_parent()
        except SafeError:
            return False
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return False
        except OSError:
            return False
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                return False
            if info.st_uid != self.owner or stat.S_IMODE(info.st_mode) & 0o077:
                return False
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return False
            fcntl.flock(fd, fcntl.LOCK_UN)
            return True
        finally:
            os.close(fd)

    def acquire(self) -> None:
        if self._fd is not None:
            return
        self._assert_parent()
        try:
            # Never create or repair the lock: the controller sandbox mounts
            # the lock parent read-only, and a missing lock is fail-closed.
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError as exc:
            raise SafeError("retirement_failed", "sender lock is unavailable") from exc
        except OSError as exc:
            raise SafeError("retirement_failed", "sender lock could not be opened") from exc
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise SafeError("retirement_failed", "sender lock is not a trusted file")
            if info.st_uid != self.owner or stat.S_IMODE(info.st_mode) & 0o077:
                raise SafeError("retirement_failed", "sender lock mode is not trusted")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise SafeError("retirement_failed", "laptop sender is active", retryable=True) from exc
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "SenderInterlock":
        self.acquire()
        return self

    def __exit__(self, *_exc: Any) -> bool:
        self.release()
        return False


# --------------------------------------------------------------------------- #
# ledger
# --------------------------------------------------------------------------- #


_LEDGER_FIELDS = (
    "operation_id", "backup_id", "profile_id", "state", "path", "quarantine_path",
    "expected_device", "expected_inode", "expected_size", "expected_mtime_ns",
    "expected_ctime_ns", "expected_sha256", "manifest_sha256", "created_at",
    "updated_at", "error_code",
)
_IMMUTABLE_FIELDS = (
    "profile_id", "path", "expected_device", "expected_inode", "expected_size",
    "expected_mtime_ns", "expected_ctime_ns", "expected_sha256", "manifest_sha256",
)


def ledger_present(connection: Any) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='backup_payload_retirement'"
    ).fetchone()
    return row is not None


def ensure_ledger(connection: Any) -> None:
    """Create the additive ledger if missing (write paths only)."""

    _register_timestamp(connection)
    if ledger_present(connection):
        return
    connection.execute(RETIREMENT_LEDGER_DDL)
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_backup_payload_retirement_backup"
        " ON backup_payload_retirement(backup_id, state)"
    )
    connection.commit()


def _register_timestamp(connection: Any) -> None:
    try:
        connection.execute("SELECT is_rfc3339_timestamp('1970-01-01T00:00:00Z')")
    except Exception:
        from .state_db import _is_rfc3339_timestamp

        connection.create_function("is_rfc3339_timestamp", 1, _is_rfc3339_timestamp)


def ledger_rows(connection: Any, operation_id: str | None = None) -> list[dict[str, Any]]:
    """Read ledger rows.  Never creates the table or writes to the database."""

    if not ledger_present(connection):
        return []
    if operation_id is None:
        cursor = connection.execute(
            f"SELECT {','.join(_LEDGER_FIELDS)} FROM backup_payload_retirement"
        )
    else:
        cursor = connection.execute(
            f"SELECT {','.join(_LEDGER_FIELDS)} FROM backup_payload_retirement WHERE operation_id=?",
            (operation_id,),
        )
    return [dict(zip(_LEDGER_FIELDS, row)) for row in cursor.fetchall()]


def blocking_backup_ids(connection: Any) -> set[str]:
    """Return every backup id with at least one blocking ledger row."""

    if not ledger_present(connection):
        return set()
    placeholders = ",".join("?" for _ in BLOCKING_STATES)
    rows = connection.execute(
        "SELECT DISTINCT backup_id FROM backup_payload_retirement"
        f" WHERE state IN ({placeholders})",
        tuple(sorted(state.value for state in BLOCKING_STATES)),
    ).fetchall()
    return {str(row[0]) for row in rows}


def _same_identity(existing: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
    return all(existing[name] == row[name] for name in _IMMUTABLE_FIELDS)


def ledger_write(connection: Any, row: Mapping[str, Any]) -> None:
    """Validate identity/transition, then upsert one ledger row."""

    ensure_ledger(connection)
    existing = connection.execute(
        f"SELECT {','.join(_LEDGER_FIELDS)} FROM backup_payload_retirement"
        " WHERE operation_id=? AND backup_id=?",
        (row["operation_id"], row["backup_id"]),
    ).fetchone()
    if existing is not None:
        current = dict(zip(_LEDGER_FIELDS, existing))
        if not _same_identity(current, row):
            raise SafeError("retirement_failed", "ledger identity does not match the plan")
        target = str(row["state"])
        if target not in _TRANSITIONS.get(str(current["state"]), frozenset()):
            raise SafeError("retirement_failed", "ledger state transition is not permitted")
    else:
        other = connection.execute(
            f"SELECT state FROM backup_payload_retirement WHERE backup_id=? AND operation_id<>?",
            (row["backup_id"], row["operation_id"]),
        ).fetchall()
        if any(str(item[0]) in {state.value for state in BLOCKING_STATES} for item in other):
            raise SafeError("retirement_failed", "payload belongs to another active retirement operation")
    placeholders = ",".join("?" for _ in _LEDGER_FIELDS)
    mutable = tuple(name for name in _LEDGER_FIELDS[3:] if name != "created_at")
    updates = ",".join(f"{name}=excluded.{name}" for name in mutable)
    connection.execute(
        f"INSERT INTO backup_payload_retirement({','.join(_LEDGER_FIELDS)})"
        f" VALUES({placeholders}) ON CONFLICT(operation_id,backup_id) DO UPDATE SET {updates}",
        tuple(row[name] for name in _LEDGER_FIELDS),
    )
    connection.commit()


# --------------------------------------------------------------------------- #
# engine
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class PlanEntry:
    entry: ManifestEntry
    archive: Path
    quarantine: Path


@dataclass
class RetirementReport:
    operation_id: str
    phase: str
    counts: dict[str, int]
    entries: list[dict[str, Any]]
    reconciled: list[dict[str, Any]]
    availability: dict[str, str]
    partial: bool = False


class RetirementService:
    """Plan and execute local-payload retirement under a controller lease."""

    def __init__(
        self,
        profiles: Mapping[str, Any],
        database: Any,
        *,
        manifest_dir: Path | str = MANIFEST_DIR,
        sender_lock_path: Path | str = SENDER_LOCK_PATH,
        clock: Callable[[], datetime] | None = None,
        owner: int = 0,
        lease_check: Callable[[], bool] | None = None,
        digest_fn: Callable[[Path], str] | None = None,
    ) -> None:
        self.profiles = dict(profiles)
        self.database = database
        self.manifest_dir = Path(manifest_dir)
        self.sender_lock_path = Path(sender_lock_path)
        self.clock = clock or _now
        self.owner = owner
        self.lease_check = lease_check
        self._digest_fn = digest_fn

    # -- connection / ids ------------------------------------------------- #

    @property
    def connection(self) -> Any | None:
        return getattr(self.database, "connection", None)

    def _assert_lease(self) -> None:
        if self.lease_check is not None and not self.lease_check():
            raise SafeError("slot_conflict", "operation lease was lost before payload retirement", retryable=True)

    def _require_connection(self) -> Any:
        connection = self.connection
        if connection is None:
            raise SafeError("retirement_failed", "durable ledger is unavailable")
        return connection

    def _profile(self, profile_id: str) -> Any:
        try:
            return self.profiles[profile_id]
        except KeyError as exc:
            raise SafeError("retirement_failed", "profile is not registered") from exc

    def _archive(self, profile: Any, backup_id: str) -> Path:
        return Path(profile.paths.backup_root) / f"{backup_id}.tar.zst"

    def _quarantine_root(self, profile: Any, operation_id: str) -> Path:
        return Path(profile.paths.backup_root) / ".retired" / operation_id

    # -- load / plan ------------------------------------------------------ #

    def load(self, operation_id: str) -> RetirementManifest:
        return load_manifest(operation_id, directory=self.manifest_dir, owner=self.owner)

    def _plan(self, manifest: RetirementManifest) -> list[PlanEntry]:
        planned: list[PlanEntry] = []
        for entry in manifest.entries:
            profile = self._profile(entry.profile_id)
            archive = self._archive(profile, entry.backup_id)
            root = Path(profile.paths.backup_root)
            if archive.parent != root:
                raise SafeError("retirement_failed", "archive path is not approved")
            quarantine = self._quarantine_root(profile, manifest.operation_id) / f"{entry.backup_id}.tar.zst"
            planned.append(PlanEntry(entry=entry, archive=archive, quarantine=quarantine))
        return planned

    # -- catalog / eligibility -------------------------------------------- #

    def _assert_catalog_eligible(self, connection: Any, entry: ManifestEntry) -> None:
        row = connection.execute(
            "SELECT profile_id,size_bytes,verified,protected FROM backups WHERE id=?",
            (entry.backup_id,),
        ).fetchone()
        if row is None:
            raise SafeError("retirement_failed", "backup is not in the catalog")
        if str(row[0]) != entry.profile_id:
            raise SafeError("retirement_failed", "backup profile does not match the plan")
        if int(row[1]) != entry.size_bytes:
            raise SafeError("retirement_failed", "backup size does not match the plan")
        if int(row[2]) != 1 or int(row[3]) != 0:
            raise SafeError("retirement_failed", "backup is not an unprotected verified catalog row")
        # Conservative: reject ANY protection row that is not a historical
        # deleted row (active, pending, failed, or unknown alike).
        active = connection.execute(
            "SELECT 1 FROM backup_protections WHERE backup_id=? AND prune_state<>'deleted' LIMIT 1",
            (entry.backup_id,),
        ).fetchone()
        if active is not None:
            raise SafeError("retirement_failed", "backup still has a remote protection row")
        confirmation = connection.execute(
            "SELECT 1 FROM confirmations WHERE action='restore' AND consumed_at IS NULL"
            " AND payload LIKE ? LIMIT 1",
            (f'%{entry.backup_id}%',),
        ).fetchone()
        if confirmation is not None:
            raise SafeError("retirement_failed", "backup has a live restore confirmation")

    def _assert_jobs_idle(self, connection: Any, own_job_id: str | None) -> None:
        if own_job_id:
            busy = connection.execute(
                "SELECT 1 FROM jobs WHERE finished_at IS NULL AND id<>? LIMIT 1",
                (own_job_id,),
            ).fetchone()
        else:
            busy = connection.execute(
                "SELECT 1 FROM jobs WHERE finished_at IS NULL LIMIT 1"
            ).fetchone()
        if busy is not None:
            raise SafeError("retirement_failed", "an unrelated operation is still running", retryable=True)

    def _assert_confirmations_clear(self, connection: Any, own_confirmation_id: str | None) -> None:
        # The caller's own retirement confirmation is already consumed
        # (consumed_at set) before this runs, so any live restore confirmation
        # at this point is unrelated and blocks the operation.
        row = connection.execute(
            "SELECT 1 FROM confirmations WHERE action='restore' AND consumed_at IS NULL LIMIT 1"
        ).fetchone()
        if row is not None:
            raise SafeError("retirement_failed", "a restore confirmation is pending")

    def _assert_operation_binding(
        self, connection: Any, manifest: RetirementManifest, *, exact: bool
    ) -> None:
        rows = ledger_rows(connection, manifest.operation_id)
        if not rows:
            return
        planned = {entry.backup_id: entry for entry in manifest.entries}
        observed = {str(row["backup_id"]) for row in rows}
        # A resumable operation may hold any subset of the immutable manifest
        # (no extra or substituted member); purge requires the exact full set.
        if not observed <= set(planned) or (exact and observed != set(planned)):
            raise SafeError("retirement_failed", "ledger membership does not match the plan")
        for row in rows:
            entry = planned[str(row["backup_id"])]
            if str(row["profile_id"]) != entry.profile_id or str(row["expected_sha256"]) != entry.sha256:
                raise SafeError("retirement_failed", "ledger entry is bound to a different plan")
            if (
                int(row["expected_device"]) != entry.device
                or int(row["expected_inode"]) != entry.inode
                or int(row["expected_size"]) != entry.size_bytes
                or int(row["expected_mtime_ns"]) != entry.mtime_ns
                or int(row["expected_ctime_ns"]) != entry.ctime_ns
            ):
                raise SafeError("retirement_failed", "ledger identity does not match the plan")

    def _assert_receipt(self, manifest: RetirementManifest) -> None:
        receipt = manifest.receipt
        if not receipt.verified:
            raise SafeError("retirement_failed", "verified-copy receipt is not verified")
        by_id = {member.backup_id: member for member in receipt.members}
        if set(by_id) != {entry.backup_id for entry in manifest.entries}:
            raise SafeError("retirement_failed", "verified-copy receipt membership does not match the plan")
        for entry in manifest.entries:
            member = by_id[entry.backup_id]
            if member.profile_id != entry.profile_id or member.size_bytes != entry.size_bytes or member.sha256 != entry.sha256:
                raise SafeError("retirement_failed", "verified-copy receipt does not match the plan")

    # -- identity --------------------------------------------------------- #

    def _digest_file(self, path: Path) -> str:
        if self._digest_fn is not None:
            return self._digest_fn(path)
        digest = hashlib.sha256()
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            while True:
                chunk = os.read(fd, 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        finally:
            os.close(fd)
        return digest.hexdigest()

    def _verify_payload(self, entry: ManifestEntry, path: Path, *, check_ctime: bool) -> None:
        """Pinned identity + content check, re-validated around the hash."""

        try:
            link_info = os.lstat(path)
        except OSError as exc:
            raise SafeError("retirement_failed", "backup payload is unavailable") from exc
        if stat.S_ISLNK(link_info.st_mode) or not stat.S_ISREG(link_info.st_mode):
            raise SafeError("retirement_failed", "backup payload is not a regular file")
        if link_info.st_nlink != 1:
            raise SafeError("retirement_failed", "backup payload has multiple links")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            before = os.fstat(fd)
            if (before.st_dev, before.st_ino) != (link_info.st_dev, link_info.st_ino):
                raise SafeError("retirement_failed", "backup payload changed while opening")
            digest = self._digest_file(path)
            after = os.fstat(fd)
        finally:
            os.close(fd)
        try:
            final = os.lstat(path)
        except OSError as exc:
            raise SafeError("retirement_failed", "backup payload changed during verification") from exc
        marks = {(before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns),
                 (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                 (final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns, final.st_ctime_ns)}
        if len(marks) != 1:
            raise SafeError("retirement_failed", "backup payload changed during verification")
        if (final.st_dev, final.st_ino) != (entry.device, entry.inode):
            raise SafeError("retirement_failed", "backup payload identity changed")
        if final.st_size != entry.size_bytes or final.st_mtime_ns != entry.mtime_ns:
            raise SafeError("retirement_failed", "backup payload metadata changed")
        if check_ctime and final.st_ctime_ns != entry.ctime_ns:
            raise SafeError("retirement_failed", "backup payload metadata changed")
        if digest != entry.sha256:
            raise SafeError("retirement_failed", "backup payload digest changed")

    # -- ledger helpers --------------------------------------------------- #

    def _prior_ops(self, connection: Any) -> set[tuple[str, str]]:
        if not ledger_present(connection):
            return set()
        return {
            (str(row["operation_id"]), str(row["backup_id"]))
            for row in ledger_rows(connection)
        }

    def _row(self, entry: ManifestEntry, manifest: RetirementManifest, state: str,
             archive: Path, quarantine: Path, error: str | None = None) -> dict[str, Any]:
        stamp = _iso(self.clock())
        return {
            "operation_id": manifest.operation_id,
            "backup_id": entry.backup_id,
            "profile_id": entry.profile_id,
            "state": state,
            "path": archive.name,
            "quarantine_path": str(quarantine.relative_to(archive.parent)),
            "expected_device": entry.device,
            "expected_inode": entry.inode,
            "expected_size": entry.size_bytes,
            "expected_mtime_ns": entry.mtime_ns,
            "expected_ctime_ns": entry.ctime_ns,
            "expected_sha256": entry.sha256,
            "manifest_sha256": manifest.operation_id,
            "created_at": stamp,
            "updated_at": stamp,
            "error_code": error,
        }

    def ledger_write(self, connection: Any, row: Mapping[str, Any]) -> None:
        ledger_write(connection, row)

    # -- availability ----------------------------------------------------- #

    def availability_map(self) -> dict[str, str]:
        """Map backup id -> fail-closed availability state (read-only)."""

        connection = self.connection
        if connection is None or not ledger_present(connection):
            return {}
        states: dict[str, str] = {}
        for row in ledger_rows(connection):
            backup_id = str(row["backup_id"])
            state = str(row["state"])
            if state not in {item.value for item in LEDGER_STATES}:
                states[backup_id] = PayloadState.AMBIGUOUS.value
                continue
            if state in {item.value for item in BLOCKING_STATES}:
                states[backup_id] = state
                continue
            # rolled_back: only locally present when the expected identity is
            # actually observed on disk.
            if state == PayloadState.ROLLED_BACK.value:
                profile = self.profiles.get(str(row["profile_id"]))
                if profile is None:
                    states[backup_id] = PayloadState.FAILED.value
                elif not self._matches(self._archive(profile, backup_id), row):
                    states[backup_id] = PayloadState.FAILED.value
        return states

    def payload_state(self, profile: Any, backup_id: str, *, path: Path | None = None) -> str:
        """Return the fail-closed availability state for one backup payload."""

        blocking = self.availability_map().get(backup_id)
        if blocking is not None:
            return blocking
        target = path if path is not None else self._archive(profile, backup_id)
        try:
            info = os.lstat(target)
        except OSError:
            return PayloadState.MISSING.value
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            return PayloadState.MISSING.value
        return PayloadState.PRESENT.value

    # -- phase admission -------------------------------------------------- #

    def _states_by_id(self, connection: Any, manifest: RetirementManifest) -> dict[str, str]:
        return {
            str(row["backup_id"]): str(row["state"])
            for row in ledger_rows(connection, manifest.operation_id)
        }

    def _assert_phase_admissible(self, connection: Any, manifest: RetirementManifest, phase: str) -> None:
        rows = self._states_by_id(connection, manifest)
        if not rows:
            return
        allowed = _PHASE_RESUMABLE[phase]
        for backup_id, state in rows.items():
            if state not in allowed:
                raise SafeError(
                    "retirement_failed",
                    f"operation cannot resume phase {phase} from ledger state {state}",
                )

    # -- prepare (read-only, hashes; caller runs this off the event loop) -- #

    def prepare(self, operation_id: str, phase: str) -> dict[str, Any]:
        if phase not in RETIREMENT_PHASES:
            raise SafeError("retirement_failed", "retirement phase is not approved")
        manifest = self.load(operation_id)
        connection = self._require_connection()
        self._assert_operation_binding(connection, manifest, exact=phase == "purge")
        self._assert_phase_admissible(connection, manifest, phase)
        if phase == "quarantine":
            self._assert_preflight(connection, manifest)
        elif phase == "purge":
            self._assert_receipt(manifest)
            self._assert_purge_ready(connection, manifest)
        else:
            self._assert_no_purge_intent(connection, manifest)
        return {
            "operation_id": manifest.operation_id,
            "phase": phase,
            "profile_ids": list(manifest.profile_ids),
            "count": len(manifest.entries),
            "bytes": manifest.total_bytes,
            "destination_id": manifest.receipt.destination_id,
        }

    def _assert_preflight(self, connection: Any, manifest: RetirementManifest) -> None:
        for entry in manifest.entries:
            self._assert_catalog_eligible(connection, entry)
        prior = self._prior_ops(connection)
        for item in self._plan(manifest):
            root = Path(item.archive.parent)
            archive_present = not self._absent(root, item.archive)
            quarantine_present = not self._absent(root, item.quarantine)
            if archive_present and quarantine_present:
                raise SafeError("retirement_failed", "both source and quarantine payloads exist")
            if quarantine_present:
                self._verify_payload(item.entry, item.quarantine, check_ctime=False)
            elif archive_present:
                self._verify_payload(
                    item.entry,
                    item.archive,
                    check_ctime=(manifest.operation_id, item.entry.backup_id) not in prior,
                )
            else:
                raise SafeError("retirement_failed", "backup payload is unavailable")

    def _assert_purge_ready(self, connection: Any, manifest: RetirementManifest) -> None:
        states = self._states_by_id(connection, manifest)
        if set(states) != {entry.backup_id for entry in manifest.entries}:
            raise SafeError("retirement_failed", "purge plan does not match the quarantined set")
        for item in self._plan(manifest):
            state = states[item.entry.backup_id]
            root = Path(item.archive.parent)
            quarantine_absent = self._absent(root, item.quarantine)
            archive_absent = self._absent(root, item.archive)
            if state == PayloadState.PURGED.value:
                if not quarantine_absent:
                    raise SafeError("retirement_failed", "a purged payload reappeared on disk")
                continue
            if state == PayloadState.PURGE_PREPARED.value:
                if not quarantine_absent:
                    self._verify_payload(item.entry, item.quarantine, check_ctime=False)
                elif not archive_absent:
                    # Durable purge intent exists, but the source is still
                    # present: not proven purged, and never unlink anything.
                    raise SafeError("retirement_failed", "source payload is still present before purge")
                continue
            if quarantine_absent:
                raise SafeError("retirement_failed", "quarantined payload is missing before purge")
            self._verify_payload(item.entry, item.quarantine, check_ctime=False)

    def _assert_no_purge_intent(self, connection: Any, manifest: RetirementManifest) -> None:
        for state in self._states_by_id(connection, manifest).values():
            if state in {PayloadState.PURGE_PREPARED.value, PayloadState.PURGED.value}:
                raise SafeError("retirement_failed", "rollback is impossible after purge intent")

    # -- quarantine ------------------------------------------------------- #

    def _assert_boundaries(self, connection: Any, own_job_id: str | None, own_confirmation_id: str | None) -> None:
        self._assert_jobs_idle(connection, own_job_id)
        self._assert_confirmations_clear(connection, own_confirmation_id)

    def quarantine(self, operation_id: str, *, own_job_id: str | None = None,
                   own_confirmation_id: str | None = None) -> RetirementReport:
        manifest = self.load(operation_id)
        connection = self._require_connection()
        self._assert_operation_binding(connection, manifest, exact=False)
        self._assert_phase_admissible(connection, manifest, "quarantine")
        with SenderInterlock(self.sender_lock_path, owner=self.owner):
            self._assert_boundaries(connection, own_job_id, own_confirmation_id)
            prior = self._prior_ops(connection)
            self._assert_preflight(connection, manifest)
            plan = self._plan(manifest)
            self._assert_boundaries(connection, own_job_id, own_confirmation_id)
            for item in plan:
                root = Path(item.archive.parent)
                self._ensure_private_dir(item.quarantine.parent.parent, root)
                self._ensure_private_dir(item.quarantine.parent, root)
            for item in plan:
                state = self._states_by_id(connection, manifest).get(item.entry.backup_id)
                if state is None or state == PayloadState.ROLLED_BACK.value:
                    self._assert_lease()
                    self.ledger_write(connection, self._row(
                        item.entry, manifest, PayloadState.PREPARED.value,
                        item.archive, item.quarantine,
                    ))
            for item in plan:
                self._assert_lease()
                self._assert_catalog_eligible(connection, item.entry)
                state = self._states_by_id(connection, manifest).get(item.entry.backup_id)
                if state == PayloadState.QUARANTINED.value:
                    self._verify_payload(item.entry, item.quarantine, check_ctime=False)
                    continue
                if item.quarantine.exists():
                    # Crash between the no-overwrite rename and the ledger
                    # update: the quarantine copy is authoritative once it
                    # verifies; complete the transition.
                    self._verify_payload(item.entry, item.quarantine, check_ctime=False)
                    self._assert_lease()
                    self.ledger_write(connection, self._row(
                        item.entry, manifest, PayloadState.QUARANTINED.value,
                        item.archive, item.quarantine,
                    ))
                    continue
                self._verify_payload(
                    item.entry,
                    item.archive,
                    check_ctime=(manifest.operation_id, item.entry.backup_id) not in prior,
                )
                self._assert_lease()
                rename_noreplace(item.archive, item.quarantine)
                self._fsync_dir(item.archive.parent)
                self._fsync_dir(item.quarantine.parent)
                self.ledger_write(connection, self._row(
                    item.entry, manifest, PayloadState.QUARANTINED.value,
                    item.archive, item.quarantine,
                ))
        return self.status(operation_id)

    # -- purge ------------------------------------------------------------ #

    def purge(self, operation_id: str, *, own_job_id: str | None = None,
              own_confirmation_id: str | None = None) -> RetirementReport:
        manifest = self.load(operation_id)
        connection = self._require_connection()
        self._assert_operation_binding(connection, manifest, exact=True)
        self._assert_phase_admissible(connection, manifest, "purge")
        with SenderInterlock(self.sender_lock_path, owner=self.owner):
            self._assert_boundaries(connection, own_job_id, own_confirmation_id)
            self._assert_receipt(manifest)
            # Eligibility is rechecked here and again immediately before each
            # unlink so a new protection/confirmation cannot slip through.
            for entry in manifest.entries:
                self._assert_catalog_eligible(connection, entry)
            self._assert_purge_ready(connection, manifest)
            plan = self._plan(manifest)
            self._assert_boundaries(connection, own_job_id, own_confirmation_id)
            # Validate every quarantined identity/hash and exact ledger binding
            # before the first durable purge intent.
            for item in plan:
                state = self._states_by_id(connection, manifest)[item.entry.backup_id]
                if state == PayloadState.QUARANTINED.value:
                    self._assert_lease()
                    self.ledger_write(connection, self._row(
                        item.entry, manifest, PayloadState.PURGE_PREPARED.value,
                        item.archive, item.quarantine,
                    ))
            for item in plan:
                self._assert_lease()
                self._assert_catalog_eligible(connection, item.entry)
                state = self._states_by_id(connection, manifest)[item.entry.backup_id]
                if state == PayloadState.PURGED.value:
                    if not self._absent(Path(item.archive.parent), item.quarantine):
                        raise SafeError("retirement_failed", "a purged payload reappeared on disk")
                    continue
                root = Path(item.archive.parent)
                if self._absent(root, item.quarantine):
                    # Proven absence of the quarantine.  This only counts as
                    # purged when a durable intent exists AND the source is
                    # also proven absent; anything else is ambiguous/failed.
                    if state != PayloadState.PURGE_PREPARED.value:
                        raise SafeError("retirement_failed", "payload missing without a purge intent")
                    if not self._absent(root, item.archive):
                        raise SafeError("retirement_failed", "source payload is still present before purge")
                    self.ledger_write(connection, self._row(
                        item.entry, manifest, PayloadState.PURGED.value,
                        item.archive, item.quarantine,
                    ))
                    continue
                self._verify_payload(item.entry, item.quarantine, check_ctime=False)
                self._assert_lease()
                os.unlink(item.quarantine)
                self._fsync_dir(item.quarantine.parent)
                self.ledger_write(connection, self._row(
                    item.entry, manifest, PayloadState.PURGED.value,
                    item.archive, item.quarantine,
                ))
        return self.status(operation_id)

    # -- rollback --------------------------------------------------------- #

    def rollback(self, operation_id: str, *, own_job_id: str | None = None,
                 own_confirmation_id: str | None = None) -> RetirementReport:
        manifest = self.load(operation_id)
        connection = self._require_connection()
        self._assert_operation_binding(connection, manifest, exact=False)
        self._assert_phase_admissible(connection, manifest, "rollback")
        partial = False
        with SenderInterlock(self.sender_lock_path, owner=self.owner):
            self._assert_boundaries(connection, own_job_id, own_confirmation_id)
            self._assert_no_purge_intent(connection, manifest)
            prior = self._prior_ops(connection)
            for item in self._plan(manifest):
                self._assert_lease()
                state = self._states_by_id(connection, manifest).get(item.entry.backup_id)
                if state not in {PayloadState.PREPARED.value, PayloadState.QUARANTINED.value}:
                    continue
                root = Path(item.archive.parent)
                quarantine_absent = self._absent(root, item.quarantine)
                archive_absent = self._absent(root, item.archive)
                if not quarantine_absent:
                    self._verify_payload(item.entry, item.quarantine, check_ctime=False)
                    self._assert_lease()
                    rename_noreplace(item.quarantine, item.archive)
                    self._fsync_dir(item.archive.parent)
                    self._fsync_dir(item.quarantine.parent)
                    self.ledger_write(connection, self._row(
                        item.entry, manifest, PayloadState.ROLLED_BACK.value,
                        item.archive, item.quarantine,
                    ))
                elif not archive_absent:
                    self._verify_payload(
                        item.entry,
                        item.archive,
                        check_ctime=(manifest.operation_id, item.entry.backup_id) not in prior,
                    )
                    self.ledger_write(connection, self._row(
                        item.entry, manifest, PayloadState.ROLLED_BACK.value,
                        item.archive, item.quarantine,
                    ))
                else:
                    partial = True
                    self.ledger_write(connection, self._row(
                        item.entry, manifest, PayloadState.FAILED.value,
                        item.archive, item.quarantine, error="payload_missing",
                    ))
        report = self.status(operation_id)
        report.partial = partial
        return report

    # -- observation-only reconciliation ---------------------------------- #

    def status(self, operation_id: str | None = None) -> RetirementReport:
        connection = self._require_connection()
        rows = ledger_rows(connection, operation_id)
        counts: dict[str, int] = {}
        entries: list[dict[str, Any]] = []
        for row in rows:
            state = str(row["state"])
            counts[state] = counts.get(state, 0) + 1
            entries.append({
                "backup_id": row["backup_id"],
                "profile_id": row["profile_id"],
                "state": state,
                "error_code": row["error_code"],
            })
        return RetirementReport(
            operation_id=operation_id or "",
            phase="status",
            counts=counts,
            entries=sorted(entries, key=lambda item: item["backup_id"]),
            reconciled=self._classify(rows),
            availability=self.availability_map(),
        )

    def _classify(self, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        reports: list[dict[str, Any]] = []
        for row in rows:
            state = str(row["state"])
            if state not in {
                PayloadState.PREPARED.value,
                PayloadState.QUARANTINED.value,
                PayloadState.PURGE_PREPARED.value,
            }:
                continue
            profile = self.profiles.get(str(row["profile_id"]))
            if profile is None:
                classification = "unknown_profile"
            else:
                archive = self._archive(profile, str(row["backup_id"]))
                quarantine = self._quarantine_path(profile, row)
                archive_ok = self._matches(archive, row)
                quarantine_ok = quarantine is not None and self._matches(quarantine, row)
                if state == PayloadState.PREPARED.value:
                    classification = (
                        "prepared_source_intact" if archive_ok
                        else "quarantine_completed" if quarantine_ok
                        else "failed"
                    )
                elif state == PayloadState.QUARANTINED.value:
                    classification = (
                        "quarantined_intact" if quarantine_ok
                        else "source_present" if archive_ok
                        else "failed"
                    )
                else:  # purge_prepared
                    # Proven purged requires PROVEN ABSENCE of both the source
                    # and the quarantine path.  A foreign, malformed, symlinked,
                    # or inaccessible object at either path is ambiguous.
                    root = Path(profile.paths.backup_root)
                    archive_absent = self._absent(root, archive)
                    quarantine_absent = quarantine is not None and self._absent(root, quarantine)
                    if quarantine_ok:
                        classification = "purge_pending"
                    elif archive_absent and quarantine_absent:
                        classification = "purged_proven"
                    elif not archive_absent or not quarantine_absent:
                        classification = "ambiguous"
                    else:
                        classification = "ambiguous"
            reports.append({
                "backup_id": row["backup_id"],
                "profile_id": row["profile_id"],
                "ledger_state": state,
                "classification": classification,
            })
        return sorted(reports, key=lambda item: item["backup_id"])

    def reconcile(self) -> list[dict[str, Any]]:
        """Read-only classification for startup; no filesystem or DB change."""

        connection = self.connection
        if connection is None or not ledger_present(connection):
            return []
        return self._classify(ledger_rows(connection))

    # -- helpers ---------------------------------------------------------- #

    def _quarantine_path(self, profile: Any, row: Mapping[str, Any]) -> Path | None:
        value = row.get("quarantine_path")
        if not value:
            return None
        root = Path(profile.paths.backup_root)
        candidate = root / str(value)
        if not _within(candidate, root) or ".." in Path(str(value)).parts:
            return None
        return candidate

    def _ensure_private_dir(self, directory: Path, root: Path) -> None:
        _assert_no_symlink_ancestors(directory)
        if not _within(directory, root):
            raise SafeError("retirement_failed", "quarantine directory is not approved")
        parent = directory.parent
        if not parent.exists():
            raise SafeError("retirement_failed", "quarantine parent is unavailable")
        parent_info = os.lstat(parent)
        if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
            raise SafeError("retirement_failed", "quarantine parent is not a directory")
        if parent_info.st_uid != self.owner or stat.S_IMODE(parent_info.st_mode) & 0o022:
            raise SafeError("retirement_failed", "quarantine parent is not trusted")
        if directory.exists():
            info = os.lstat(directory)
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise SafeError("retirement_failed", "quarantine directory is not trusted")
            if info.st_uid != self.owner or stat.S_IMODE(info.st_mode) & 0o077:
                raise SafeError("retirement_failed", "quarantine directory permissions are not trusted")
            return
        os.mkdir(directory, 0o700)
        info = os.lstat(directory)
        if info.st_uid != self.owner or stat.S_IMODE(info.st_mode) & 0o077:
            raise SafeError("retirement_failed", "quarantine directory could not be secured")
        self._fsync_dir(parent)

    @staticmethod
    def _matches(path: Path, row: Mapping[str, Any]) -> bool:
        try:
            info = os.lstat(path)
        except OSError:
            return False
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            return False
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) == (
            int(row["expected_device"]),
            int(row["expected_inode"]),
            int(row["expected_size"]),
            int(row["expected_mtime_ns"]),
        )

    def _absent(self, root: Path, path: Path) -> bool:
        """True only when ``path`` is *provably* absent.

        Proof requires a trusted, symlink-free ancestor chain from the profile
        backup root down to the leaf's parent, and then a leaf ``lstat`` that
        fails with ENOENT/ENOTDIR.  A missing ancestor proves absence; a
        symlinked/non-directory ancestor, an existing leaf (regular, symlink,
        hardlink, ...), or any permission/I/O error is uncertainty and returns
        False, so it is never treated as proven absent.
        """

        if not _within(path, root):
            return False
        try:
            root_info = os.lstat(root)
        except OSError:
            return False
        if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
            return False
        try:
            parts = path.relative_to(root).parts
        except ValueError:
            return False
        current = root
        for part in parts[:-1]:
            current = current / part
            try:
                info = os.lstat(current)
            except FileNotFoundError:
                return True  # an ancestor is missing => the leaf cannot exist
            except OSError:
                return False
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                return False
        try:
            os.lstat(path)
        except FileNotFoundError:
            return True
        except NotADirectoryError:
            return True
        except OSError:
            return False
        return False

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    _assert_no_symlink_ancestors = staticmethod(_assert_no_symlink_ancestors)


def _within(path: Path, root: Path) -> bool:
    try:
        path.absolute().relative_to(root.absolute())
        return True
    except ValueError:
        return False
