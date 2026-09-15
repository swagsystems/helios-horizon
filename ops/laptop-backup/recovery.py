#!/usr/bin/env python3
"""Verified download of a laptop vault object into a local quarantine.

Transport is injected by the operator: no shell is used, no path is embedded,
and no new SSH authority is created. The fetch is bound to an explicit source
digest, byte size and manifest hash, streamed in bounded chunks into an
``O_EXCL`` temporary file, re-hashed with SHA-256, zstd-verified, and published
with a hard link that never overwrites. Object and manifest selectors are
separate pinned argv templates so a manifest digest cannot be fetched through
the object selector (or vice versa).

Rehydration and ledger reconciliation are owned by the controller and are not
implemented here. A purged payload is terminal in the retirement ledger, so this
helper never claims to restore a retired catalog row.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import selectors
import shutil
import stat
import subprocess
import time
import signal
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

MAX_OBJECT_BYTES = 16 * 1024 * 1024 * 1024
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MANIFEST_SCHEMA = 1
DEFAULT_CHUNK_BYTES = 1024 * 1024
OBJECT_SUFFIX = ".tar.zst"
DIGEST = re.compile(r"^[0-9a-f]{64}$")
TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class RecoveryError(RuntimeError):
    """Base class for a refused or failed recovery operation."""


class BindingError(RecoveryError):
    pass


class TransportError(RecoveryError):
    pass


class IntegrityError(RecoveryError):
    pass


class ConflictError(RecoveryError):
    pass


def _validate_digest(value: Any, name: str) -> str:
    if not isinstance(value, str) or DIGEST.match(value) is None:
        raise BindingError(f"{name} is not a sha256 digest")
    return value


@dataclass(frozen=True)
class SourceBinding:
    """Explicitly binds a fetch to one digest, size and manifest hash."""

    digest: str
    size_bytes: int
    manifest_sha256: str

    def __post_init__(self) -> None:
        _validate_digest(self.digest, "digest")
        _validate_digest(self.manifest_sha256, "manifest_sha256")
        if isinstance(self.size_bytes, bool) or not isinstance(self.size_bytes, int):
            raise BindingError("size_bytes is invalid")
        if not 0 < self.size_bytes <= MAX_OBJECT_BYTES:
            raise BindingError("size_bytes is out of range")


def _object_name(digest: str) -> str:
    return _validate_digest(digest, "digest") + OBJECT_SUFFIX


def _open_directory(path: str) -> int:
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise RecoveryError("quarantine path is not a real directory")
    descriptor = os.open(
        path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    )
    opened = os.fstat(descriptor)
    if not stat.S_ISDIR(opened.st_mode) or (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
        os.close(descriptor)
        raise RecoveryError("quarantine directory changed during open")
    return descriptor


def _fd_path(dir_fd: int, name: str) -> str:
    if os.path.isdir("/proc/self/fd"):
        return os.path.join("/proc/self/fd", str(dir_fd), name)
    raise RecoveryError("durable quarantine handle is unavailable")


def _exists_at(dir_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def _unlink_at(dir_fd: int, name: str) -> None:
    try:
        os.unlink(name, dir_fd=dir_fd)
    except FileNotFoundError:
        return


def _default_zstd_check(path: str, *, pass_fds: Sequence[int] = ()) -> None:
    binary = shutil.which("zstd")
    if binary is None:
        raise IntegrityError("zstd is unavailable")
    completed = subprocess.run(
        [binary, "-t", "--quiet", "--", path],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=1800,
        # The pinned directory descriptor is O_CLOEXEC, so it must be passed
        # explicitly for a /proc/self/fd path to resolve inside the child.
        pass_fds=tuple(pass_fds),
        check=False,
    )
    if completed.returncode != 0:
        raise IntegrityError("zstd integrity check failed")


def _check_zstd(zstd_check: Callable[..., None], dir_fd: int, name: str) -> None:
    target = _fd_path(dir_fd, name)
    if zstd_check is _default_zstd_check:
        _default_zstd_check(target, pass_fds=(dir_fd,))
    else:
        zstd_check(target)


def _verify_existing(dir_fd: int, name: str, binding: SourceBinding) -> bool:
    try:
        descriptor = os.open(
            name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=dir_fd
        )
    except OSError:
        return False
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_size != binding.size_bytes:
            return False
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, DEFAULT_CHUNK_BYTES)
            if not chunk:
                break
            total += len(chunk)
            if total > binding.size_bytes:
                return False
            digest.update(chunk)
        final = os.fstat(descriptor)
    except OSError:
        return False
    finally:
        os.close(descriptor)
    if (final.st_dev, final.st_ino, final.st_size) != (opened.st_dev, opened.st_ino, opened.st_size):
        return False
    return total == binding.size_bytes and digest.hexdigest() == binding.digest


def _existing_is_usable(dir_fd: int, name: str, binding: SourceBinding, zstd_check: Callable[[str], None]) -> bool:
    if not _verify_existing(dir_fd, name, binding):
        return False
    _check_zstd(zstd_check, dir_fd, name)
    return True


def download_to_quarantine(
    transport: Any,
    binding: SourceBinding,
    quarantine_dir: str,
    *,
    chunk_bytes: int = DEFAULT_CHUNK_BYTES,
    max_bytes: int = MAX_OBJECT_BYTES,
    zstd_check: Callable[[str], None] = _default_zstd_check,
    manifest_members: Callable[[bytes], Iterable[tuple[str, int]]] | None = None,
) -> dict[str, Any]:
    """Stream ``binding`` from ``transport`` into ``quarantine_dir`` safely."""

    if isinstance(chunk_bytes, bool) or not isinstance(chunk_bytes, int) or not 0 < chunk_bytes <= 8 * 1024 * 1024:
        raise BindingError("chunk_bytes is out of range")
    if binding.size_bytes > max_bytes:
        raise BindingError("binding exceeds the configured byte cap")

    manifest = _read_manifest(transport, binding)
    parser = manifest_members or default_manifest_members
    members = list(parser(manifest))
    if not any(digest == binding.digest and size == binding.size_bytes for digest, size in members):
        raise IntegrityError("manifest does not reference the requested object")

    dir_fd = _open_directory(quarantine_dir)
    temp_name: str | None = None
    try:
        final_name = _object_name(binding.digest)
        if _exists_at(dir_fd, final_name):
            if _existing_is_usable(dir_fd, final_name, binding, zstd_check):
                return {"status": "already_present", "path": os.path.join(quarantine_dir, final_name), "digest": binding.digest}
            raise ConflictError("quarantine already holds a different object for this digest")

        temp_name = f".{binding.digest}.{os.getpid()}.{os.urandom(4).hex()}.tmp"
        descriptor = os.open(
            temp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=dir_fd,
        )
        hasher = hashlib.sha256()
        total = 0
        handle = os.fdopen(descriptor, "wb", closefd=True)
        try:
            reader = transport.open(binding.digest)
            try:
                cap = min(binding.size_bytes, max_bytes)
                while True:
                    chunk = reader.read(min(chunk_bytes, cap - total) or 1)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > cap:
                        raise IntegrityError("source stream exceeded the bound size")
                    hasher.update(chunk)
                    handle.write(chunk)
            finally:
                _close_reader(reader)
            if total != binding.size_bytes:
                raise IntegrityError("source stream size did not match the binding")
            if hasher.hexdigest() != binding.digest:
                raise IntegrityError("source stream digest did not match the binding")
            handle.flush()
            os.fsync(handle.fileno())
        finally:
            handle.close()
        _check_zstd(zstd_check, dir_fd, temp_name)
        published = os.stat(temp_name, dir_fd=dir_fd, follow_symlinks=False)
        if not stat.S_ISREG(published.st_mode) or published.st_size != binding.size_bytes:
            raise IntegrityError("temporary object is not a regular file of the bound size")
        try:
            os.link(temp_name, final_name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        except FileExistsError as exc:
            _unlink_at(dir_fd, temp_name)
            temp_name = None
            if _existing_is_usable(dir_fd, final_name, binding, zstd_check):
                return {"status": "already_present", "path": os.path.join(quarantine_dir, final_name), "digest": binding.digest}
            raise ConflictError("quarantine already holds a different object for this digest") from exc
        _unlink_at(dir_fd, temp_name)
        temp_name = None
        os.fsync(dir_fd)
    except BaseException:
        if temp_name is not None:
            _unlink_at(dir_fd, temp_name)
        raise
    finally:
        os.close(dir_fd)
    return {
        "status": "downloaded",
        "path": os.path.join(quarantine_dir, _object_name(binding.digest)),
        "digest": binding.digest,
        "size_bytes": binding.size_bytes,
        "manifest_sha256": binding.manifest_sha256,
        "rehydration": "controller-owned; not performed",
    }


def _close_reader(reader: Any) -> None:
    close = getattr(reader, "close", None)
    if close is not None:
        close()


def _read_manifest(transport: Any, binding: SourceBinding) -> bytes:
    reader = transport.open_manifest(binding.manifest_sha256)
    try:
        blob = reader.read(MAX_MANIFEST_BYTES + 1)
    finally:
        _close_reader(reader)
    if not isinstance(blob, (bytes, bytearray)):
        raise TransportError("manifest reader did not return bytes")
    blob = bytes(blob)
    if len(blob) > MAX_MANIFEST_BYTES:
        raise IntegrityError("manifest exceeds the bounded size")
    if hashlib.sha256(blob).hexdigest() != binding.manifest_sha256:
        raise IntegrityError("manifest hash did not match the binding")
    return blob


def default_manifest_members(blob: bytes) -> list[tuple[str, int]]:
    """Parse the sender's real manifest schema strictly."""

    try:
        document = json.loads(blob.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrityError("manifest is not valid JSON") from exc
    if not isinstance(document, dict):
        raise IntegrityError("manifest is not an object")
    schema = document.get("schema")
    if isinstance(schema, bool) or not isinstance(schema, int) or schema != MANIFEST_SCHEMA:
        raise IntegrityError("manifest schema is unsupported")
    source = document.get("source")
    if not isinstance(source, str) or TOKEN.match(source) is None:
        raise IntegrityError("manifest source is invalid")
    archives = document.get("archives")
    if not isinstance(archives, list):
        raise IntegrityError("manifest does not expose an archives list")
    members: list[tuple[str, int]] = []
    seen_backup: set[str] = set()
    seen_digest: dict[str, int] = {}
    for entry in archives:
        if not isinstance(entry, dict):
            raise IntegrityError("manifest entry is malformed")
        backup_id = entry.get("backup_id")
        if not isinstance(backup_id, str) or TOKEN.match(backup_id) is None or backup_id in seen_backup:
            raise IntegrityError("manifest entry backup_id is invalid or duplicated")
        seen_backup.add(backup_id)
        digest = entry.get("sha256")
        size = entry.get("size_bytes")
        if not isinstance(digest, str) or DIGEST.match(digest) is None:
            raise IntegrityError("manifest entry digest is invalid")
        if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= MAX_OBJECT_BYTES:
            raise IntegrityError("manifest entry size is invalid")
        if digest in seen_digest and seen_digest[digest] != size:
            raise IntegrityError("manifest entry digest has conflicting sizes")
        seen_digest[digest] = size
        members.append((digest, size))
    return members


class CommandTransport:
    """Operator-pinned transport with separate object and manifest selectors."""

    def __init__(
        self,
        object_argv: Sequence[str],
        manifest_argv: Sequence[str] | None = None,
        *,
        timeout: float = 3600.0,
    ) -> None:
        manifest_argv = object_argv if manifest_argv is None else manifest_argv
        for argv in (object_argv, manifest_argv):
            if not argv or any(not isinstance(item, str) or not item for item in argv):
                raise TransportError("argv template is invalid")
            if not any("{digest}" in item for item in argv):
                raise TransportError("argv template has no {digest} placeholder")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 86400:
            raise TransportError("timeout is out of range")
        self._object_argv = tuple(object_argv)
        self._manifest_argv = tuple(manifest_argv)
        self._timeout = float(timeout)

    def _spawn(self, template: Sequence[str], selector: str, *, max_bytes: int) -> "_ProcessReader":
        argv = [item.replace("{digest}", selector) for item in template]
        process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
        )
        try:
            return _ProcessReader(process, timeout=self._timeout, max_bytes=max_bytes)
        except BaseException:
            _kill_process(process)
            raise

    def open(self, digest: str) -> "_ProcessReader":
        selector = _validate_digest(digest, "digest")
        return self._spawn(self._object_argv, selector, max_bytes=MAX_OBJECT_BYTES)

    def open_manifest(self, manifest_sha256: str) -> "_ProcessReader":
        selector = _validate_digest(manifest_sha256, "manifest_sha256")
        return self._spawn(self._manifest_argv, selector, max_bytes=MAX_MANIFEST_BYTES)


class _ProcessReader:
    """Bounded, deadline-driven reader over a pinned transport subprocess.

    Reads are driven by a selector with a per-iteration remaining time derived
    from the single overall deadline, so a slow trickle, a hung child or a
    descendant that holds stdout open cannot exceed the deadline. The child runs
    in its own session and is killed as a process group on any failure.
    """

    def __init__(self, process: Any, *, timeout: float, max_bytes: int) -> None:
        self._process = process
        self._deadline = time.monotonic() + timeout
        self._max_bytes = max_bytes
        self._read = 0
        self._finished = False
        self._eof = False
        self._fd = process.stdout.fileno()
        os.set_blocking(self._fd, False)
        self._selector = selectors.DefaultSelector()
        self._selector.register(self._fd, selectors.EVENT_READ)

    def read(self, size: int = -1) -> bytes:
        if self._finished or self._eof:
            return b""
        want = size if isinstance(size, int) and size > 0 else None
        buffer = bytearray()
        while True:
            remaining = self._deadline - time.monotonic()
            if remaining <= 0:
                self._terminate()
                raise TransportError("transport deadline exceeded")
            if want is not None and len(buffer) >= want:
                break
            read_size = 65536 if want is None else min(want - len(buffer), 65536)
            events = self._selector.select(min(remaining, 0.25))
            if not events:
                continue
            try:
                chunk = os.read(self._fd, read_size)
            except BlockingIOError:
                continue
            except OSError as exc:
                self._eof = True
                self._finish()
                raise TransportError("transport stream failed") from exc
            if not chunk:
                self._eof = True
                self._finish()
                break
            self._read += len(chunk)
            if self._read > self._max_bytes:
                self._terminate()
                raise IntegrityError("source stream exceeded the byte cap")
            buffer.extend(chunk)
        return bytes(buffer)

    def _finish(self) -> None:
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            self._terminate()
            raise TransportError("transport deadline exceeded")
        try:
            code = self._process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            self._terminate()
            raise TransportError("transport process did not exit") from None
        self._finished = True
        self._close_pipes()
        if code != 0:
            raise TransportError("pinned transport command failed")

    def _terminate(self) -> None:
        self._finished = True
        _kill_process(self._process)
        self._close_pipes()
        self._close_selector()

    def close(self) -> None:
        try:
            if not self._finished:
                self._terminate()
        finally:
            self._close_pipes()
            self._close_selector()

    def _close_pipes(self) -> None:
        for stream in (self._process.stdout, self._process.stderr):
            close = getattr(stream, "close", None)
            if close is not None:
                close()

    def _close_selector(self) -> None:
        try:
            self._selector.close()
        except OSError:
            pass


def _kill_process(process: Any) -> None:
    # Kill the whole session group so a descendant cannot keep running (or hold
    # stdout open) after the direct child is gone.
    try:
        if os.getpgid(process.pid) == process.pid:
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except (ProcessLookupError, PermissionError, OSError):
        try:
            process.kill()
        except OSError:
            pass
    try:
        process.wait(timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        pass
    for stream in (getattr(process, "stdout", None), getattr(process, "stderr", None)):
        close = getattr(stream, "close", None)
        if close is not None:
            close()


class MappingTransport:
    """Local transport over in-memory mappings; used by tests and dry runs."""

    def __init__(self, objects: Mapping[str, bytes], manifests: Mapping[str, bytes]) -> None:
        self._objects = dict(objects)
        self._manifests = dict(manifests)

    def open(self, digest: str) -> "BytesReader":
        return BytesReader(self._objects[digest])

    def open_manifest(self, manifest_sha256: str) -> "BytesReader":
        return BytesReader(self._manifests[manifest_sha256])


class BytesReader:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._payload) - self._offset
        chunk = self._payload[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def close(self) -> None:
        self._offset = len(self._payload)
