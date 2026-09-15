#!/usr/bin/env python3
"""Build a verified, inactive Sunlit candidate from a pinned manifest."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path

from game_control.modpack_update import (
    AssemblyError,
    AssemblySpec,
    Overlay,
    TextOverride,
    assemble,
    assemble_versioned_runtime,
)


class StageError(ValueError):
    pass


def _canonical_manifest(document: dict) -> str:
    body = dict(document)
    supplied = body.pop("manifest_sha256", None)
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    calculated = hashlib.sha256(encoded).hexdigest()
    if supplied != calculated:
        raise StageError("manifest self-digest mismatch")
    return calculated


def _load_manifest(path: Path) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 8 * 1024 * 1024:
        raise StageError("manifest must be a bounded regular file")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise StageError("manifest is unreadable") from exc
    if not isinstance(document, dict) or document.get("manifest_version") != 1:
        raise StageError("manifest version is unsupported")
    if document.get("profile_id") != "minecraft-sunlit-cobblemon":
        raise StageError("manifest profile is not Sunlit")
    _canonical_manifest(document)
    return document


def _policy_values(document: dict):
    try:
        policy = document["runtime_policy"]
        persistent_dirs = tuple(policy["persistent_dirs"])
        persistent_files = tuple(policy["persistent_files"])
        required_paths = tuple(policy["required_paths"])
        mutable_vendor_dirs = tuple(policy["mutable_vendor_dirs"])
        empty_mutable_dirs = tuple(policy["empty_mutable_dirs"])
        fixed_symlinks = {key: Path(value) for key, value in policy["fixed_symlinks"].items()}
        overrides = tuple(TextOverride(**value) for value in policy["text_overrides"])
    except (KeyError, TypeError, ValueError) as exc:
        raise StageError("runtime policy is malformed") from exc
    return persistent_dirs, persistent_files, required_paths, mutable_vendor_dirs, empty_mutable_dirs, fixed_symlinks, overrides


def stage(args: argparse.Namespace) -> dict:
    document = _load_manifest(args.manifest)
    candidate_root = args.candidate_root
    if candidate_root.exists() or candidate_root.is_symlink():
        raise StageError("candidate root must not exist")
    if not candidate_root.parent.is_dir() or candidate_root.parent.is_symlink():
        raise StageError("candidate parent must be a real directory")
    candidate_root.mkdir(mode=0o700)
    try:
        artifact = document["artifact"]["archive"]
        entries = {value["path"]: value for value in document["archive"]["entries"]}
        overlay_value = document["overlay"]
        spec = AssemblySpec(
            archive=args.archive,
            archive_sha256=artifact["sha256"],
            archive_size=artifact["size"],
            members=entries,
            vendor_roots=tuple(document["archive"]["roots"]),
            overlays=(Overlay(Path(overlay_value["source"]), overlay_value["destination"], overlay_value["sha256"]),),
        )
        vendor = candidate_root / "vendor-release"
        vendor_report = assemble(spec, args.prior_runtime, vendor)
        policy = _policy_values(document)
        runtime = candidate_root / "runtime"
        state = candidate_root / "state"
        runtime_report = assemble_versioned_runtime(
            vendor, state, document["artifact"]["version"], args.prior_runtime, runtime,
            persistent_dirs=policy[0], persistent_files=policy[1], required_paths=policy[2],
            mutable_vendor_dirs=policy[3], empty_mutable_dirs=policy[4],
            fixed_symlinks=policy[5], text_overrides=policy[6],
        )
        shutil.rmtree(vendor)
        dirfd = os.open(candidate_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try: os.fsync(dirfd)
        finally: os.close(dirfd)
        report = {
            "profile_id": document["profile_id"],
            "version": document["artifact"]["version"],
            "manifest_sha256": document["manifest_sha256"],
            "archive_sha256": artifact["sha256"],
            "vendor_files": len(vendor_report.files),
            "vendor_retained": False,
            "runtime_files_and_links": len(runtime_report.files),
            "preserved": list(runtime_report.preserved),
            "mutable_and_fixed": list(runtime_report.overlays),
            "active": False,
        }
        output = candidate_root / "candidate.json"
        payload = (json.dumps(report, sort_keys=True, indent=2) + "\n").encode()
        fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload); stream.flush(); os.fsync(stream.fileno())
        dirfd = os.open(candidate_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try: os.fsync(dirfd)
        finally: os.close(dirfd)
        return report
    except BaseException:
        shutil.rmtree(candidate_root, ignore_errors=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--prior-runtime", required=True, type=Path)
    parser.add_argument("--candidate-root", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(stage(args), sort_keys=True))
    except (AssemblyError, StageError, KeyError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=os.sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
