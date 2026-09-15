#!/usr/bin/env python3
"""Build a deterministic, fail-closed manifest for the Sunlit ZIP artifact."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import tempfile
from typing import NoReturn
import urllib.parse
import zipfile
from pathlib import Path, PurePosixPath

PROFILE = "minecraft-sunlit-cobblemon"
PERSISTENT_DIRS = ["world", "battle_logs", "easy_npc", "journeymap", "local", "trainers"]
REQUIRED_PERSISTENT_FILES = [
    "server.properties", "user_jvm_args.txt", "eula.txt", "ops.json",
    "whitelist.json", "banned-ips.json", "banned-players.json",
    "server-icon.png", "usercache.json", "usernamecache.json",
]
PERSISTENT_FILES = [*REQUIRED_PERSISTENT_FILES, "ears-debug.log", "rhino.local.properties"]
EMPTY_MUTABLE_DIRS = [
    "logs", "crash-reports", "backups", "showdown", "modernfix",
    "moonlight-global-datapacks",
]
SEASON_OVERRIDE = {
    "path": "config/sereneseasons/seasons.toml",
    "before_sha256": "7a67adb30a867dc62920b5c7aa729381fe8fba5df59164044383540c2d9000cd",
    "after_sha256": "48ba3ff8cbf05957195774b8e42da656f126ee982fac3aeaec4c23205b2471c8",
    "old": "sub_season_duration = 10",
    "new": "sub_season_duration = 15",
}
MAX_ENTRIES = 100_000
MAX_MEMBER_SIZE = 512 * 1024 * 1024
MAX_TOTAL_SIZE = 8 * 1024 * 1024 * 1024
MAX_RATIO = 1000
HEX64 = set("0123456789abcdef")


class ManifestError(ValueError):
    pass


def fail(message: str) -> "NoReturn":
    raise ManifestError(message)


def digest_file(path: Path) -> tuple[int, str]:
    total = 0
    h = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                total += len(chunk)
                h.update(chunk)
    except OSError as exc:
        fail(f"cannot read {path}: {exc}")
    return total, h.hexdigest()


def valid_sha(value: str, label: str) -> str:
    if len(value) != 64 or any(ch not in HEX64 for ch in value):
        fail(f"invalid {label} SHA256")
    return value


def safe_member_name(name: str) -> str:
    if not name or "\\" in name or "\x00" in name:
        fail(f"unsafe archive path: {name!r}")
    if name.startswith("/") or name.startswith("~"):
        fail(f"unsafe archive path: {name!r}")
    path = PurePosixPath(name)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        fail(f"unsafe archive path: {name!r}")
    normalized = "/".join(path.parts)
    if normalized != name:
        fail(f"unsafe archive path: {name!r}")
    return name


def validate_zip_member_type(info: zipfile.ZipInfo) -> None:
    if info.flag_bits & 0x1:
        fail(f"encrypted archive member: {info.filename!r}")
    mode = (info.external_attr >> 16) & 0xFFFF
    kind = stat.S_IFMT(mode)
    expected_kind = stat.S_IFDIR if info.is_dir() else stat.S_IFREG
    if kind and kind != expected_kind:
        fail(f"special archive member: {info.filename!r}")
    # Unix symlink entries have S_IFLNK; DOS directory/special attributes are
    # not accepted either. ZIP hardlinks have no portable representation.
    if info.create_system == 3 and kind and kind != expected_kind:
        fail(f"non-regular archive member: {info.filename!r}")
    if info.file_size < 0 or info.file_size > MAX_MEMBER_SIZE:
        fail(f"archive member exceeds size bound: {info.filename!r}")
    if info.compress_size < 0:
        fail(f"invalid compressed size: {info.filename!r}")
    if info.file_size and info.file_size > max(1, info.compress_size) * MAX_RATIO:
        fail(f"archive member compression ratio exceeds bound: {info.filename!r}")


def make_manifest(args: argparse.Namespace) -> dict:
    archive = Path(args.archive)
    if not archive.is_file() or archive.is_symlink():
        fail("archive must be a regular file")
    size, sha = digest_file(archive)
    expected_size = args.archive_size
    if expected_size < 0 or size != expected_size:
        fail(f"archive size mismatch: expected {expected_size}, got {size}")
    expected_sha = valid_sha(args.archive_sha256, "archive")
    if sha != expected_sha:
        fail("archive SHA256 mismatch")
    if not args.version or any(ord(c) < 0x21 or ord(c) > 0x7e for c in args.version):
        fail("invalid version")
    if not args.project_id or not args.file_id or any(c not in "0123456789" for c in args.project_id + args.file_id):
        fail("invalid project/file ID")
    parsed = urllib.parse.urlparse(args.url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.fragment:
        fail("invalid artifact URL")
    overlay_source = Path(args.overlay_source)
    if not overlay_source.is_file() or overlay_source.is_symlink():
        fail("overlay source must be a regular file")
    overlay_destination = safe_member_name(args.overlay_destination)
    overlay_size, overlay_sha = digest_file(overlay_source)
    if valid_sha(args.overlay_sha256, "overlay") != overlay_sha:
        fail("overlay SHA256 mismatch")
    try:
        with zipfile.ZipFile(archive) as zf:
            infos = zf.infolist()
            if not infos or len(infos) > MAX_ENTRIES:
                fail("archive entry-count bound exceeded")
            total = 0
            entries = []
            seen = set()
            for info in infos:
                raw_name = info.filename
                if info.is_dir():
                    if not raw_name.endswith("/") or raw_name.endswith("//"):
                        fail(f"unsafe archive directory: {raw_name!r}")
                    raw_name = raw_name[:-1]
                name = safe_member_name(raw_name)
                key = name.casefold()
                if key in seen:
                    fail(f"duplicate/case-collision archive path: {name!r}")
                seen.add(key)
                validate_zip_member_type(info)
                if info.is_dir():
                    continue
                total += info.file_size
                if total > MAX_TOTAL_SIZE:
                    fail("archive total-uncompressed-size bound exceeded")
                h = hashlib.sha256()
                read = 0
                try:
                    with zf.open(info, "r") as stream:
                        while chunk := stream.read(1024 * 1024):
                            read += len(chunk)
                            h.update(chunk)
                except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
                    fail(f"cannot read archive member {name!r}: {exc}")
                if read != info.file_size:
                    fail(f"archive member size mismatch: {name!r}")
                entries.append({"path": name, "size": read, "sha256": h.hexdigest()})
    except (OSError, zipfile.BadZipFile) as exc:
        fail(f"invalid ZIP archive: {exc}")
    if overlay_destination.casefold() in {item["path"].casefold() for item in entries}:
        fail("overlay destination collides with archive member")
    if digest_file(archive) != (size, sha):
        fail("archive changed while manifest was being built")
    if digest_file(overlay_source) != (overlay_size, overlay_sha):
        fail("overlay changed while manifest was being built")
    entries.sort(key=lambda item: item["path"])
    roots = sorted({item["path"].split("/", 1)[0] for item in entries}, key=lambda x: (x.casefold(), x))
    manifest = {
        "manifest_version": 1,
        "profile_id": PROFILE,
        "artifact": {
            "version": args.version,
            "project_id": args.project_id,
            "file_id": args.file_id,
            "url": args.url,
            "archive": {"size": size, "sha256": sha},
        },
        "archive": {"entry_count": len(entries), "total_uncompressed_size": sum(x["size"] for x in entries), "roots": roots, "entries": entries},
        "runtime_policy": {
            "service_user": "svc-sunlit",
            "service_group": "svc-sunlit",
            "release_root": "/opt/game-servers/minecraft-sunlit-cobblemon/releases",
            "state_root": "/srv/game-servers/minecraft-sunlit-cobblemon-state",
            "active_link": "/srv/game-servers/minecraft-sunlit-cobblemon-current",
            "persistent_dirs": PERSISTENT_DIRS,
            "persistent_files": PERSISTENT_FILES,
            "required_paths": ["world", *REQUIRED_PERSISTENT_FILES],
            "mutable_vendor_dirs": ["config"],
            "empty_mutable_dirs": EMPTY_MUTABLE_DIRS,
            "fixed_symlinks": {"libraries": "/opt/game-servers/minecraft-sunlit-cobblemon/libraries"},
            "text_overrides": [SEASON_OVERRIDE],
        },
        "overlay": {"source": str(overlay_source.resolve()), "destination": overlay_destination, "size": overlay_size, "sha256": overlay_sha},
    }
    canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    manifest["manifest_sha256"] = hashlib.sha256(canonical).hexdigest()
    return manifest


def atomic_write(output: Path, data: bytes) -> None:
    if output.exists() or output.is_symlink():
        fail("output already exists or is unsafe")
    parent = output.parent
    if not parent.is_dir() or parent.is_symlink():
        fail("output parent must be a real directory")
    fd, tmp = tempfile.mkstemp(prefix=f".{output.name}.", dir=parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, output)
        dirfd = os.open(parent, os.O_DIRECTORY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
    except Exception:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--file-id", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--archive-size", required=True, type=int)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--overlay-source", required=True)
    parser.add_argument("--overlay-destination", required=True)
    parser.add_argument("--overlay-sha256", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        manifest = make_manifest(args)
        atomic_write(Path(args.output), (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode())
    except ManifestError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
