from __future__ import annotations

import asyncio
import json
import os
import threading
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from game_control import sunlit_update as MODULE
from game_control import web_main
from game_control.protocol import (
    CheckUpdate,
    ProfileId,
    RpcRequest,
    RpcSuccess,
    parse_request_line,
)
from game_control.updates import UpdateRpcFacade, UpdateService
from game_control.web_main import UnixRpcClient


ROOT = Path(__file__).parents[1]


def _trusted_overlay(tmp_path: Path, monkeypatch, payload: bytes = b"overlay") -> tuple[Path, Path]:
    releases = tmp_path / "releases"
    release = releases / "1.1.3-SSV4.1.4"
    mods = release / MODULE.OVERLAY_RELATIVE.parent
    mods.mkdir(parents=True)
    overlay = release / MODULE.OVERLAY_RELATIVE
    overlay.write_bytes(payload)
    overlay.chmod(0o644)
    active = tmp_path / "active"
    active.symlink_to(release, target_is_directory=True)
    monkeypatch.setattr(MODULE, "RELEASE_ROOT", releases)
    monkeypatch.setattr(MODULE, "ACTIVE_LINK", active)
    monkeypatch.setattr(MODULE, "TRUSTED_FS_ROOT", tmp_path)
    monkeypatch.setattr(MODULE, "OVERLAY_SHA256", MODULE.hashlib.sha256(payload).hexdigest())
    return release, overlay


def _manifest(overlay: Path, *, archive_size: int = 5) -> dict:
    document = {
        "manifest_version": 1,
        "profile_id": MODULE.PROFILE,
        "artifact": {
            "project_id": MODULE.PROJECT_ID,
            "file_id": "42",
            "version": "1.1.4-SSV4.1.5",
            "archive": {
                "size": archive_size,
                "sha256": "b" * 64,
            },
        },
        "archive": {
            "total_uncompressed_size": 10,
            "entries": [
                {"path": "config/a.txt", "size": 2},
                {"path": "mods/b.jar", "size": 8},
            ],
        },
        "runtime_policy": {
            "persistent_dirs": ["world"],
            "persistent_files": [],
            "mutable_vendor_dirs": ["config"],
        },
        "overlay": {
            "source": str(overlay),
            "destination": MODULE.OVERLAY_DESTINATION,
            "size": overlay.stat().st_size,
            "sha256": MODULE.hashlib.sha256(overlay.read_bytes()).hexdigest(),
        },
    }
    document["manifest_sha256"] = _manifest_digest(document)
    return document


def _manifest_digest(document: dict) -> str:
    body = dict(document)
    body.pop("manifest_sha256", None)
    return MODULE.hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


def _real_stage(tmp_path: Path, monkeypatch, *, version: str = "1.1.4-SSV4.1.5") -> tuple[dict, dict, Path]:
    _release_root, overlay = _trusted_overlay(tmp_path, monkeypatch)
    archive = tmp_path / "server-pack.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as bundle:
        bundle.writestr("config/a.txt", b"cfg")
        bundle.writestr("mods/b.jar", b"mod")
    archive_size = archive.stat().st_size
    manifest = MODULE.make_manifest(SimpleNamespace(
        archive=archive,
        version=version,
        project_id=MODULE.PROJECT_ID,
        file_id="42",
        url="https://example.invalid/archive.zip",
        archive_size=archive_size,
        archive_sha256=MODULE.hashlib.sha256(archive.read_bytes()).hexdigest(),
        overlay_source=overlay,
        overlay_destination=MODULE.OVERLAY_DESTINATION,
        overlay_sha256=MODULE.hashlib.sha256(overlay.read_bytes()).hexdigest(),
    ))
    root = MODULE.STAGING_ROOT / f"sunlit-{version}"
    root.mkdir(parents=True)
    (root / "server-pack.zip").write_bytes(archive.read_bytes())
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return manifest, {"version": version, "file_id": "42", "size": archive_size}, root


def _space_paths(tmp_path: Path, monkeypatch) -> dict[str, Path]:
    state = tmp_path / "state"
    staging = tmp_path / "staging"
    backup = tmp_path / "backup"
    state.mkdir()
    staging.mkdir()
    backup.mkdir()
    monkeypatch.setattr(MODULE, "STATE_ROOT", state)
    monkeypatch.setattr(MODULE, "STAGING_ROOT", staging)
    monkeypatch.setattr(MODULE, "BACKUP_ROOT", backup)
    monkeypatch.setattr(MODULE, "SPACE_MARGIN", 3)
    monkeypatch.setattr(MODULE, "BACKUP_ENTRY_OVERHEAD", 3)
    monkeypatch.setattr(MODULE, "BACKUP_ARCHIVE_OVERHEAD", 7)
    return {"state": state, "staging": staging, "backup": backup}


def test_discovers_latest_exact_official_server_pack(monkeypatch) -> None:
    calls = []

    def fetch(url: str):
        calls.append(url)
        if url.endswith("/files"):
            return {
                "data": [
                    {"id": 10, "releaseType": 2, "fileStatus": 4, "hasServerPack": True, "gameVersions": ["1.20.1", "Forge"]},
                    {"id": 20, "releaseType": 1, "fileStatus": None, "hasServerPack": True, "gameVersions": ["1.20.1", "Forge"]},
                ]
            }
        return {
            "data": [
                {
                    "id": 21,
                    "fileName": "SERVER-PACK-Society-Sunlit-Cobblemon-1.2.3-SSV4.1.4.zip",
                    "fileLength": 123456,
                }
            ]
        }

    monkeypatch.setattr(MODULE, "_fetch_json", fetch)
    release = MODULE.discover()

    assert release["version"] == "1.2.3-SSV4.1.4"
    assert release["file_id"] == "21"
    assert calls[-1].endswith("/files/20/additional-files")


def test_check_reports_current_without_mutation(tmp_path: Path, monkeypatch) -> None:
    state = tmp_path / "state/.horizon"
    state.mkdir(parents=True)
    (state / "release.json").write_text(json.dumps({"version": "v2"}), encoding="utf-8")
    releases = tmp_path / "releases"
    releases.mkdir()
    (releases / "v2").mkdir()
    active = tmp_path / "active"
    active.symlink_to(releases / "v2", target_is_directory=True)
    monkeypatch.setattr(MODULE, "STATE_ROOT", tmp_path / "state")
    monkeypatch.setattr(MODULE, "RELEASE_ROOT", releases)
    monkeypatch.setattr(MODULE, "ACTIVE_LINK", active)
    monkeypatch.setattr(MODULE, "discover", lambda: {"version": "v2"})
    monkeypatch.setattr(MODULE, "_stage", lambda _release: (_ for _ in ()).throw(AssertionError("must not stage")))

    assert MODULE.run(check_only=False) == {"state": "current", "installed": "v2", "available": None}


def test_installed_version_requires_active_release_commit(tmp_path: Path, monkeypatch) -> None:
    state = tmp_path / "state/.horizon"
    state.mkdir(parents=True)
    (state / "release.json").write_text(json.dumps({"version": "v2"}), encoding="utf-8")
    releases = tmp_path / "releases"
    releases.mkdir()
    (releases / "v2").mkdir()
    active = tmp_path / "active"
    monkeypatch.setattr(MODULE, "STATE_ROOT", tmp_path / "state")
    monkeypatch.setattr(MODULE, "RELEASE_ROOT", releases)
    monkeypatch.setattr(MODULE, "ACTIVE_LINK", active)
    assert MODULE._installed_version() is None
    active.symlink_to(releases / "v2", target_is_directory=True)
    assert MODULE._installed_version() == "v2"


def test_inactive_gate_defers_before_staging(monkeypatch) -> None:
    monkeypatch.setattr(MODULE, "discover", lambda: {"version": "v2", "file_id": "2"})
    monkeypatch.setattr(MODULE, "_installed_version", lambda: "v1")
    monkeypatch.setattr(MODULE, "_inactive", lambda: False)
    monkeypatch.setattr(MODULE, "_stage", lambda _release: (_ for _ in ()).throw(AssertionError("must not stage")))
    monkeypatch.setattr(MODULE.os, "geteuid", lambda: 0)

    assert MODULE.run(check_only=False) == {"state": "deferred", "installed": "v1", "available": "v2"}


def test_run_checks_download_and_promotion_space_before_the_matching_actions(
    tmp_path: Path, monkeypatch,
) -> None:
    events = []
    release = {
        "version": "1.1.4-SSV4.1.5",
        "file_id": "42",
        "size": 5,
        "url": "https://example.invalid/42",
    }
    root = tmp_path / "staging/sunlit-1.1.4-SSV4.1.5"
    root.mkdir(parents=True)
    manifest = {"manifest_sha256": "a" * 64}

    class Lease:
        def assert_owned(self):
            return None

        def publication_guard(self, _action):
            from contextlib import nullcontext

            return nullcontext()

        def release_locked(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(MODULE, "discover", lambda: release)
    monkeypatch.setattr(MODULE, "_installed_version", lambda: "1.1.3-SSV4.1.4")
    monkeypatch.setattr(MODULE.os, "geteuid", lambda: 0)
    monkeypatch.setattr(MODULE, "_inactive", lambda: True)
    monkeypatch.setattr(MODULE, "_staging_root", lambda _release: root)
    monkeypatch.setattr(MODULE, "_staged_operation_id", lambda _root: "operation")
    monkeypatch.setattr(MODULE, "_reserve_update", lambda _operation: Lease())
    monkeypatch.setattr(
        MODULE,
        "_require_space",
        lambda _release, **kwargs: events.append(("space", kwargs["phase"])),
    )

    def fake_stage(_release):
        events.append(("stage",))
        return root, manifest

    monkeypatch.setattr(MODULE, "_stage", fake_stage)
    monkeypatch.setattr(MODULE, "_request_backup", lambda _root, _lease=None: events.append(("backup",)) or "backup")
    monkeypatch.setattr(
        MODULE,
        "promote_candidate",
        lambda **_kwargs: events.append(("promote",)) or {"active": True},
    )
    monkeypatch.setattr(MODULE, "_record", lambda *_args: None)

    assert MODULE.run(check_only=False)["state"] == "promoted"
    assert events == [
        ("space", "download"),
        ("stage",),
        ("space", "promotion"),
        ("backup",),
        ("space", "post_backup"),
        ("promote",),
    ]


def test_inactive_systemd_failure_is_bounded(monkeypatch) -> None:
    def broken_run(*_args, **_kwargs):
        raise OSError("systemd unavailable")

    monkeypatch.setattr(MODULE.subprocess, "run", broken_run)
    with pytest.raises(MODULE.UpdateError, match="inactive state"):
        MODULE._inactive()


def test_inactive_database_failure_is_bounded(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(MODULE, "SLOT", tmp_path / "slot.json")
    monkeypatch.setattr(MODULE, "DATABASE", tmp_path / "state.db")
    monkeypatch.setattr(
        MODULE.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(stdout="inactive\n"),
    )

    def broken_connect(*_args, **_kwargs):
        raise MODULE.sqlite3.OperationalError("database unavailable")

    monkeypatch.setattr(MODULE.sqlite3, "connect", broken_connect)
    with pytest.raises(MODULE.UpdateError, match="inactive state"):
        MODULE._inactive()


def test_space_plan_charges_measured_copies_and_excludes_backup_history(
    monkeypatch, tmp_path: Path,
) -> None:
    _release_root, overlay = _trusted_overlay(tmp_path, monkeypatch)
    paths = _space_paths(tmp_path, monkeypatch)
    (paths["state"] / "world").write_bytes(b"x" * 10)
    versioned_backup = paths["state"] / ".versions/1.1.3-SSV4.1.4/backups"
    versioned_backup.mkdir(parents=True)
    (versioned_backup / "large.bin").write_bytes(b"x" * 1000)
    top_backups = paths["state"] / "backups"
    top_backups.mkdir()
    (top_backups / "large.bin").write_bytes(b"x" * 1000)
    (paths["state"] / "partial.partial").write_bytes(b"x" * 1000)
    devices = {
        paths["staging"]: (10, 1000),
        MODULE.RELEASE_ROOT: (11, 1000),
        paths["backup"]: (12, 1000),
    }
    monkeypatch.setattr(MODULE, "_space_probe", lambda path: devices[path])

    plan = MODULE._space_plan(
        {"version": "1.1.4-SSV4.1.5", "file_id": "42", "size": 5},
        manifest=_manifest(overlay),
        phase="promotion",
    )

    assert plan.persistent_bytes == 10
    assert plan.mutable_bytes == 2
    assert plan.stage_peak_bytes == 61
    assert plan.staged_candidate_bytes == 22
    assert plan.release_bytes == 15
    assert plan.backup_bytes == 30
    assert dict(plan.required_by_device) == {10: 25, 11: 18, 12: 33}


def test_space_plan_charges_snapshot_copy_fallback_on_same_device(
    monkeypatch, tmp_path: Path,
) -> None:
    _release_root, overlay = _trusted_overlay(tmp_path, monkeypatch)
    paths = _space_paths(tmp_path, monkeypatch)
    (paths["state"] / "world").write_bytes(b"x" * 10)
    monkeypatch.setattr(MODULE, "_space_probe", lambda _path: (7, 1000))

    promotion = MODULE._space_plan(
        {"version": "1.1.4-SSV4.1.5", "file_id": "42", "size": 5},
        manifest=_manifest(overlay),
        phase="promotion",
    )

    assert dict(promotion.required_by_device) == {7: 70}


def test_space_plan_credits_only_verified_real_retry_artifacts(
    monkeypatch, tmp_path: Path,
) -> None:
    paths = _space_paths(tmp_path, monkeypatch)
    manifest, release, root = _real_stage(tmp_path, monkeypatch)
    (paths["state"] / "world").write_bytes(b"x" * 10)
    monkeypatch.setattr(MODULE, "_space_probe", lambda _path: (7, 1000))

    download = MODULE._space_plan(release, manifest=None, phase="download")
    stage = MODULE._space_plan(release, manifest=manifest, phase="stage")
    assert dict(download.required_by_device) == {7: MODULE.SPACE_MARGIN}
    assert dict(stage.required_by_device) == {
        7: max(0, stage.stage_peak_bytes - release["size"]) + MODULE.SPACE_MARGIN
    }

    candidate = root / "candidate"
    candidate.mkdir()
    (candidate / "candidate.json").write_text(
        json.dumps({
            "version": release["version"],
            "manifest_sha256": manifest["manifest_sha256"],
        }),
        encoding="utf-8",
    )
    (root / "server-pack.zip").unlink()
    download_candidate = MODULE._space_plan(release, manifest=None, phase="download")
    assert dict(download_candidate.required_by_device) == {7: MODULE.SPACE_MARGIN}
    promotion = MODULE._space_plan(release, manifest=manifest, phase="promotion")
    assert dict(promotion.required_by_device) == {
        7: promotion.release_bytes + promotion.backup_bytes + MODULE.SPACE_MARGIN
    }


def test_retry_manifest_and_archive_integrity_fail_closed(
    monkeypatch, tmp_path: Path,
) -> None:
    _space_paths(tmp_path, monkeypatch)
    manifest, release, root = _real_stage(tmp_path, monkeypatch)
    archive = root / "server-pack.zip"
    archive.write_bytes(archive.read_bytes() + b"tamper")
    with pytest.raises(MODULE.UpdateError, match="archive is unsafe"):
        MODULE._space_plan(release, manifest=None, phase="download")

    manifest["archive"]["total_uncompressed_size"] += 1
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(MODULE.UpdateError, match="self-digest"):
        MODULE._space_plan(release, manifest=None, phase="download")

    manifest["archive"]["entries"][0]["path"] = "../escape"
    manifest["manifest_sha256"] = _manifest_digest(manifest)
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(MODULE.UpdateError, match="path is unsafe"):
        MODULE._space_plan(release, manifest=None, phase="download")


def test_space_preflight_rejects_unproven_or_oversized_archive(
    monkeypatch, tmp_path: Path,
) -> None:
    _trusted_overlay(tmp_path, monkeypatch)
    _space_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(MODULE, "_space_probe", lambda _path: (1, 1000))
    with pytest.raises(MODULE.UpdateError, match="free space"):
        MODULE._space_plan({"version": "1.1.4-SSV4.1.5", "file_id": "42", "size": "unknown"}, manifest=None, phase="download")
    with pytest.raises(MODULE.UpdateError, match="free space"):
        MODULE._space_plan(
            {"version": "1.1.4-SSV4.1.5", "file_id": "42", "size": MODULE.MAX_ARCHIVE + 1},
            manifest=None,
            phase="download",
        )


def test_space_preflight_reports_the_insufficient_phase_and_device(
    monkeypatch, tmp_path: Path,
) -> None:
    _release_root, overlay = _trusted_overlay(tmp_path, monkeypatch)
    paths = _space_paths(tmp_path, monkeypatch)
    (paths["state"] / "world").write_bytes(b"x" * 10)
    monkeypatch.setattr(MODULE, "_space_probe", lambda path: (21 if path != paths["backup"] else 22, 1))
    with pytest.raises(MODULE.UpdateError, match=r"Sunlit promotion on filesystem 2[12]"):
        MODULE._require_space(
            {"version": "1.1.4-SSV4.1.5", "file_id": "42", "size": 5},
            manifest=_manifest(overlay),
            phase="promotion",
        )


def test_staged_operation_id_is_durable_and_rejects_populated_legacy_root(tmp_path: Path, monkeypatch) -> None:
    staging = tmp_path / "staging"
    monkeypatch.setattr(MODULE, "STAGING_ROOT", staging)
    release = {"version": "v2"}
    root = MODULE._staging_root(release)
    first = MODULE._staged_operation_id(root)
    assert MODULE._staged_operation_id(root) == first
    assert str(MODULE.uuid.UUID(first)) == first
    (root / "update-operation-id").write_text("not-a-uuid\n", encoding="ascii")
    (root / "update-operation-id").chmod(0o600)
    with pytest.raises(MODULE.UpdateError, match="identity is malformed"):
        MODULE._staged_operation_id(root)
    (root / "update-operation-id").unlink()
    (root / "candidate").mkdir()
    with pytest.raises(MODULE.UpdateError, match="identity is missing"):
        MODULE._staged_operation_id(root)


@pytest.mark.parametrize("unsafe", ["symlink", "fifo"])
def test_space_preflight_rejects_unsafe_state_members(tmp_path: Path, monkeypatch, unsafe: str) -> None:
    _release_root, overlay = _trusted_overlay(tmp_path, monkeypatch)
    paths = _space_paths(tmp_path, monkeypatch)
    state = paths["state"]
    if unsafe == "symlink":
        (state / "world").symlink_to(tmp_path / "outside")
    else:
        os.mkfifo(state / "world")
    monkeypatch.setattr(MODULE, "_space_probe", lambda _path: (1, 1000))
    with pytest.raises(MODULE.UpdateError, match="persistent state"):
        MODULE._space_plan(
            {"version": "1.1.4-SSV4.1.5", "file_id": "42", "size": 1},
            manifest=_manifest(overlay, archive_size=1),
            phase="stage",
        )


@pytest.mark.parametrize("unsafe", ["symlink", "fifo", "writable"])
def test_space_preflight_rejects_unsafe_overlay(tmp_path: Path, monkeypatch, unsafe: str) -> None:
    _release_root, overlay = _trusted_overlay(tmp_path, monkeypatch)
    overlay.unlink()
    if unsafe == "symlink":
        overlay.symlink_to(tmp_path / "outside.jar")
    elif unsafe == "fifo":
        os.mkfifo(overlay)
    else:
        overlay.write_bytes(b"overlay")
        overlay.chmod(0o666)
    with pytest.raises(MODULE.UpdateError, match="overlay"):
        MODULE._trusted_release_overlay()


@pytest.mark.parametrize(
    "changes",
    [
        {"st_uid": 994},
        {"st_gid": 993},
        {"st_nlink": 2},
        {"st_mode": MODULE.stat.S_IFREG | 0o666},
        {"st_size": MODULE.MAX_OVERLAY_SIZE + 1},
    ],
)
def test_trusted_overlay_info_rejects_owner_link_mode_and_size(changes) -> None:
    info = SimpleNamespace(
        st_mode=MODULE.stat.S_IFREG | 0o644,
        st_nlink=1,
        st_uid=0,
        st_gid=0,
        st_size=7,
    )
    for key, value in changes.items():
        setattr(info, key, value)
    assert MODULE._trusted_overlay_info(info) is False


def test_trusted_release_overlay_selects_active_release_copy(tmp_path: Path, monkeypatch) -> None:
    release, overlay = _trusted_overlay(tmp_path, monkeypatch)
    selected = MODULE._trusted_release_overlay()
    assert selected.path == overlay
    assert selected.size == overlay.stat().st_size
    assert selected.sha256 == MODULE.OVERLAY_SHA256
    assert release in selected.path.parents


def test_trusted_overlay_rejects_hardlinks_changed_content_and_symlinked_ancestry(
    tmp_path: Path, monkeypatch,
) -> None:
    _release_root, overlay = _trusted_overlay(tmp_path, monkeypatch)
    os.link(overlay, overlay.parent / "alias.jar")
    with pytest.raises(MODULE.UpdateError, match="overlay is unsafe"):
        MODULE._trusted_release_overlay()
    (overlay.parent / "alias.jar").unlink()

    overlay.write_bytes(b"changed")
    with pytest.raises(MODULE.UpdateError, match="content is not trusted"):
        MODULE._trusted_release_overlay()

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / overlay.name).write_bytes(b"outside")
    overlay.unlink()
    overlay.parent.rmdir()
    overlay.parent.symlink_to(outside, target_is_directory=True)
    with pytest.raises(MODULE.UpdateError, match="ancestry is unsafe"):
        MODULE._trusted_release_overlay()


def test_trusted_overlay_rejects_unsafe_higher_ancestor(tmp_path: Path, monkeypatch) -> None:
    unsafe = tmp_path / "unsafe-root"
    unsafe.mkdir()
    unsafe.chmod(0o777)
    releases = unsafe / "releases"
    release = releases / "1.1.3-SSV4.1.4"
    mods = release / MODULE.OVERLAY_RELATIVE.parent
    mods.mkdir(parents=True)
    overlay = release / MODULE.OVERLAY_RELATIVE
    overlay.write_bytes(b"overlay")
    overlay.chmod(0o644)
    active = tmp_path / "active"
    active.symlink_to(release, target_is_directory=True)
    monkeypatch.setattr(MODULE, "RELEASE_ROOT", releases)
    monkeypatch.setattr(MODULE, "ACTIVE_LINK", active)
    monkeypatch.setattr(MODULE, "TRUSTED_FS_ROOT", tmp_path)
    monkeypatch.setattr(MODULE, "OVERLAY_SHA256", MODULE.hashlib.sha256(b"overlay").hexdigest())

    with pytest.raises(MODULE.UpdateError, match="release path is unsafe"):
        MODULE._trusted_release_overlay()


def test_trusted_overlay_detects_source_swap_at_open(tmp_path: Path, monkeypatch) -> None:
    _release_root, overlay = _trusted_overlay(tmp_path, monkeypatch)
    real_open = MODULE.os.open

    def swapping_open(path, flags, *args, **kwargs):
        if Path(path) == overlay:
            overlay.unlink()
            overlay.write_bytes(b"swapped")
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(MODULE.os, "open", swapping_open)
    with pytest.raises(MODULE.UpdateError, match="content is not trusted"):
        MODULE._trusted_release_overlay()


def test_sunlit_checker_reports_available_without_manual_apply_and_caches() -> None:
    calls = []
    checker = MODULE.SunlitUpdateChecker(
        probe=lambda: calls.append("probe") or {
            "state": "available",
            "installed": "1.1.3-SSV4.1.4",
            "available": "1.1.4-SSV4.1.5",
        },
        installed_probe=lambda: "1.1.3-SSV4.1.4",
    )

    first = checker.check()
    second = checker.check()

    assert calls == ["probe"]
    assert first.state == "available"
    assert first.available_version == "1.1.4-SSV4.1.5"
    assert first.apply_supported is False
    assert second.available_version == "1.1.4-SSV4.1.5"


def test_sunlit_checker_single_flights_concurrent_checks() -> None:
    entered = threading.Event()
    release = threading.Event()

    def probe():
        entered.set()
        release.wait(timeout=2)
        return {
            "state": "current",
            "installed": "1.1.3-SSV4.1.4",
            "available": None,
        }

    checker = MODULE.SunlitUpdateChecker(
        probe=probe,
        installed_probe=lambda: "1.1.3-SSV4.1.4",
        wait_seconds=0.01,
    )
    result = []
    worker = threading.Thread(target=lambda: result.append(checker.check()))
    worker.start()
    assert entered.wait(timeout=2)
    concurrent = checker.check()
    worker.join(timeout=2)
    release.set()
    time.sleep(0.05)

    assert concurrent.state == "checking"
    assert concurrent.available_version is None
    assert result[0].state == "checking"
    assert checker.check().state == "current"


def test_sunlit_checker_failure_is_not_current_and_old_success_is_stale() -> None:
    clock = [0.0]
    fail = [False]

    def probe():
        if fail[0]:
            raise MODULE.UpdateError("upstream metadata is unavailable")
        return {"state": "current", "installed": "1.1.3-SSV4.1.4", "available": None}

    checker = MODULE.SunlitUpdateChecker(
        probe=probe,
        installed_probe=lambda: "1.1.3-SSV4.1.4",
        monotonic=lambda: clock[0],
        cache_seconds=10,
        stale_seconds=60,
    )
    assert checker.check().state == "current"
    clock[0] = 20
    fail[0] = True
    stale = checker.check()
    assert stale.state == "stale"
    assert stale.available_version is None

    fresh = MODULE.SunlitUpdateChecker(
        probe=lambda: (_ for _ in ()).throw(MODULE.UpdateError("upstream metadata is unavailable")),
        installed_probe=lambda: "1.1.3-SSV4.1.4",
    )
    failed = fresh.check()
    assert failed.state == "failed"
    assert failed.available_version is None


def test_sunlit_checker_backs_off_and_sanitizes_unknown_errors() -> None:
    clock = [0.0]
    calls = []

    def probe():
        calls.append(clock[0])
        raise RuntimeError("internal diagnostic sentinel")

    checker = MODULE.SunlitUpdateChecker(
        probe=probe,
        installed_probe=lambda: "1.1.3-SSV4.1.4",
        monotonic=lambda: clock[0],
        failure_backoff_seconds=10,
    )
    first = checker.check()
    second = checker.check()
    assert first.state == "failed"
    assert first.message == "Update check failed."
    assert second.state == "failed"
    assert calls == [0.0]

    clock[0] = 11
    third = checker.check()
    assert third.state == "failed"
    assert calls == [0.0, 11.0]
    assert "sentinel" not in (first.message or "") + (second.message or "") + (third.message or "")


def test_sunlit_checker_followers_return_without_waiting_for_active_probe() -> None:
    entered = threading.Event()
    release = threading.Event()
    calls = []

    def probe():
        calls.append("probe")
        entered.set()
        release.wait(timeout=2)
        return {"state": "current", "installed": "1.1.3-SSV4.1.4", "available": None}

    checker = MODULE.SunlitUpdateChecker(
        probe=probe,
        installed_probe=lambda: "1.1.3-SSV4.1.4",
        wait_seconds=0.25,
    )
    initiator = []
    worker = threading.Thread(target=lambda: initiator.append(checker.check()))
    worker.start()
    assert entered.wait(timeout=2)

    started = time.monotonic()
    follower = checker.check()
    elapsed = time.monotonic() - started
    assert elapsed < 0.1
    assert follower.state == "checking"
    assert calls == ["probe"]

    release.set()
    worker.join(timeout=2)
    assert checker.check().state == "current"
    assert calls == ["probe"]


def test_sunlit_checker_hung_probe_deadline_recovers_after_backoff() -> None:
    clock = [0.0]
    processes = []

    class HungProcess:
        def __init__(self):
            self.killed = False
            self.reaped = False

        def communicate(self, timeout=None):
            time.sleep(0.03)
            raise MODULE.subprocess.TimeoutExpired("probe", timeout)

        def kill(self):
            self.killed = True

        def wait(self, timeout=None):
            self.reaped = True
            return 0

        def poll(self):
            return -9 if self.killed else None

    def popen(*_args, **_kwargs):
        process = HungProcess()
        processes.append(process)
        return process

    checker = MODULE.SunlitUpdateChecker(
        installed_probe=lambda: "1.1.3-SSV4.1.4",
        monotonic=lambda: clock[0],
        wait_seconds=0.01,
        probe_deadline_seconds=0.1,
        failure_backoff_seconds=10,
        popen=popen,
    )
    assert checker.check().state == "checking"
    time.sleep(0.05)
    failed = checker.check()
    assert failed.state == "failed"
    assert failed.message == "update check exceeded its deadline"
    assert len(processes) == 1
    assert processes[0].killed and processes[0].reaped

    assert checker.check().state == "failed"
    assert len(processes) == 1
    clock[0] = 11
    assert checker.check().state == "checking"
    assert len(processes) == 2


def test_sunlit_checker_close_reaps_active_probe_without_blocking() -> None:
    started = threading.Event()
    killed = threading.Event()

    class BlockingProcess:
        def __init__(self):
            self.reaped = False

        def communicate(self, timeout=None):
            started.set()
            killed.wait(timeout=2)
            return "", ""

        def kill(self):
            killed.set()

        def wait(self, timeout=None):
            self.reaped = True
            return 0

        def poll(self):
            return -9 if killed.is_set() else None

    process = BlockingProcess()
    checker = MODULE.SunlitUpdateChecker(
        installed_probe=lambda: "1.1.3-SSV4.1.4",
        wait_seconds=0.01,
        probe_deadline_seconds=5,
        popen=lambda *_args, **_kwargs: process,
    )
    assert checker.check().state == "checking"
    assert started.wait(timeout=2)
    before = time.monotonic()
    checker.close()
    elapsed = time.monotonic() - before
    assert elapsed < 0.5
    assert process.reaped
    assert checker.check().state == "checking"


def test_probe_entrypoint_does_not_create_the_automatic_update_lock(
    monkeypatch, capsys,
) -> None:
    monkeypatch.setattr(
        MODULE,
        "run",
        lambda **_kwargs: {
            "state": "available",
            "installed": "1.1.3-SSV4.1.4",
            "available": "1.1.4-SSV4.1.5",
        },
    )
    monkeypatch.setattr(MODULE.fcntl, "flock", lambda *_args: (_ for _ in ()).throw(AssertionError("lock")))
    assert MODULE.main(["--probe"]) == 0
    assert json.loads(capsys.readouterr().out)["available"] == "1.1.4-SSV4.1.5"


@pytest.mark.asyncio
async def test_slow_checker_returns_checking_before_real_rpc_deadline(
    tmp_path: Path, monkeypatch,
) -> None:
    release_started = threading.Event()
    release_probe = threading.Event()
    socket_path = tmp_path / "control.sock"

    def probe():
        release_started.set()
        release_probe.wait(timeout=2)
        return {
            "state": "available",
            "installed": "1.1.3-SSV4.1.4",
            "available": "1.1.4-SSV4.1.5",
        }

    checker = MODULE.SunlitUpdateChecker(
        probe=probe,
        installed_probe=lambda: "1.1.3-SSV4.1.4",
        wait_seconds=0.05,
    )
    profile = SimpleNamespace(
        id=ProfileId.MINECRAFT_SUNLIT_COBBLEMON,
        update=SimpleNamespace(kind="manual"),
        paths=SimpleNamespace(
            version_file=tmp_path / "version",
            install_root=tmp_path / "install",
        ),
    )
    service = UpdateService(
        {profile.id.value: profile},
        manual_checker=checker.check,
    )
    facade = UpdateRpcFacade(
        {profile.id.value: service},
        {profile.id.value: profile},
        {},
    )

    async def handler(reader, writer):
        try:
            request = parse_request_line(await reader.readline())
            result = await facade.check(request.action)
            writer.write(RpcSuccess(request_id=request.request_id, result=result).model_dump_json().encode() + b"\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    monkeypatch.setattr(web_main, "CONTROL_SOCKET", socket_path)
    server = await asyncio.start_unix_server(handler, path=str(socket_path))
    try:
        client = UnixRpcClient(socket_path, timeout=5.0)
        request = RpcRequest(
            request_id=uuid4(),
            actor="operator",
            action=CheckUpdate(
                kind="check_update",
                profile_id=ProfileId.MINECRAFT_SUNLIT_COBBLEMON,
            ),
        )
        started = time.monotonic()
        response = await asyncio.wait_for(client(request), timeout=5.0)
        elapsed = time.monotonic() - started
        assert release_started.wait(timeout=2)
        assert response.ok
        payload = response.result if isinstance(response.result, dict) else response.result.model_dump(mode="json")
        assert payload["state"] == "checking"
        assert elapsed < 5.0
    finally:
        release_probe.set()
        checker.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_update_facade_burst_does_not_starve_unrelated_thread_work(
    tmp_path: Path,
) -> None:
    probe_started = threading.Event()
    release_probe = threading.Event()
    calls = []

    def probe():
        calls.append("probe")
        probe_started.set()
        release_probe.wait(timeout=2)
        return {"state": "current", "installed": "1.1.3-SSV4.1.4", "available": None}

    checker = MODULE.SunlitUpdateChecker(
        probe=probe,
        installed_probe=lambda: "1.1.3-SSV4.1.4",
        wait_seconds=0.25,
    )
    profile = SimpleNamespace(
        id=ProfileId.MINECRAFT_SUNLIT_COBBLEMON,
        update=SimpleNamespace(kind="manual"),
        paths=SimpleNamespace(
            version_file=tmp_path / "version",
            install_root=tmp_path / "install",
        ),
    )
    service = UpdateService(
        {profile.id.value: profile},
        manual_checker=checker.check,
    )
    facade = UpdateRpcFacade(
        {profile.id.value: service},
        {profile.id.value: profile},
        {},
    )
    action = SimpleNamespace(profile_id=profile.id)
    initiator = asyncio.create_task(facade.check(action))
    assert await asyncio.to_thread(probe_started.wait, 2)

    started = time.monotonic()
    followers = await asyncio.gather(*(facade.check(action) for _ in range(12)))
    follower_elapsed = time.monotonic() - started
    unrelated = await asyncio.wait_for(asyncio.to_thread(time.monotonic), timeout=0.25)

    assert all(item.state == "checking" for item in followers)
    assert follower_elapsed < 0.15
    assert unrelated is not None
    assert calls == ["probe"]

    release_probe.set()
    await asyncio.wait_for(initiator, timeout=1)
    checker.close()


def test_update_lease_pause_fully_drains_renewal_thread(monkeypatch) -> None:
    entered = threading.Event()
    unblock = threading.Event()

    class Store:
        def renew_if_owned(self, *_args, **_kwargs):
            entered.set()
            assert unblock.wait(timeout=2)

    monkeypatch.setattr(MODULE, "RESERVATION_RENEW_INTERVAL", 0)
    lease = MODULE._UpdateLease(
        Store(), "operation", 0, controller_pid=os.getpid(),
        controller_start_ticks=1,
    )
    lease.start()
    assert entered.wait(timeout=2)
    pause_done = threading.Event()

    def pause() -> None:
        lease.pause()
        pause_done.set()

    waiter = threading.Thread(target=pause)
    waiter.start()
    time.sleep(0.05)
    assert not pause_done.is_set()
    unblock.set()
    waiter.join(timeout=2)
    assert pause_done.is_set()
    assert lease._thread is None


def test_space_preflight_rejects_inconsistent_measured_expansion(monkeypatch, tmp_path: Path) -> None:
    _release_root, overlay = _trusted_overlay(tmp_path, monkeypatch)
    _space_paths(tmp_path, monkeypatch)
    monkeypatch.setattr(MODULE, "_space_probe", lambda _path: (1, 1000))
    manifest = _manifest(overlay)
    manifest["archive"]["total_uncompressed_size"] = "unknown"
    manifest["manifest_sha256"] = _manifest_digest(manifest)
    with pytest.raises(MODULE.UpdateError, match="total is inconsistent"):
        MODULE._space_plan(
            {"version": "1.1.4-SSV4.1.5", "file_id": "42", "size": 5},
            manifest=manifest,
            phase="stage",
        )


def test_package_update_has_no_retired_manifest_stage_or_promote_dispatch() -> None:
    source = Path(MODULE.__file__).read_text(encoding="utf-8")
    assert "MANIFEST_HELPER" not in source
    assert "STAGE_HELPER" not in source
    assert "PROMOTE_HELPER" not in source
    assert "horizon-sunlit-manifest" not in source
    assert "horizon-sunlit-stage" not in source
    assert "horizon-sunlit-promote" not in source


def test_retired_sunlit_front_doors_are_not_kept_as_source_authority() -> None:
    for name in (
        "horizon-sunlit-manifest",
        "horizon-sunlit-stage",
        "horizon-sunlit-promote",
    ):
        assert not (ROOT / "ops/bin" / name).exists()


def test_stage_uses_package_policy_without_retired_helper_spawn(tmp_path: Path, monkeypatch) -> None:
    archive = tmp_path / "server-pack.zip"
    archive.write_bytes(b"archive")
    overlay = tmp_path / "overlay.jar"
    overlay.write_bytes(b"overlay")
    staging = tmp_path / "staging"
    staging.mkdir()
    events = []
    manifest = {
        "manifest_version": 1,
        "profile_id": "minecraft-sunlit-cobblemon",
        "artifact": {
            "project_id": MODULE.PROJECT_ID,
            "file_id": "42",
            "version": "v2",
            "archive": {"size": archive.stat().st_size, "sha256": MODULE.hashlib.sha256(archive.read_bytes()).hexdigest()},
        },
        "manifest_sha256": "a" * 64,
    }
    monkeypatch.setattr(MODULE, "STAGING_ROOT", staging)
    monkeypatch.setattr(MODULE, "STATE_ROOT", tmp_path / "state")
    monkeypatch.setattr(
        MODULE,
        "_trusted_release_overlay",
        lambda: MODULE.TrustedOverlay(overlay, overlay.stat().st_size, MODULE.hashlib.sha256(overlay.read_bytes()).hexdigest()),
    )
    monkeypatch.setattr(MODULE, "make_manifest", lambda _args: manifest)
    monkeypatch.setattr(
        MODULE,
        "_require_space",
        lambda _release, **kwargs: events.append(("space", kwargs["phase"])),
    )
    monkeypatch.setattr(MODULE, "_download", lambda _release, target: (target.write_bytes(b"archive"), MODULE.hashlib.sha256(b"archive").hexdigest())[1])
    monkeypatch.setattr(MODULE, "_run", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("retired helper spawned")))

    def fake_stage(args):
        events.append("stage")
        args.candidate_root.mkdir()
        (args.candidate_root / "candidate.json").write_text(
            json.dumps({"version": "v2", "manifest_sha256": "a" * 64}), encoding="utf-8"
        )
        return {"active": False}

    monkeypatch.setattr(MODULE, "stage", fake_stage)
    root, loaded = MODULE._stage({"version": "v2", "file_id": "42", "size": archive.stat().st_size, "url": "https://example.invalid/42"})
    assert root == staging / "sunlit-v2"
    assert loaded["manifest_sha256"] == "a" * 64
    assert events == [("space", "stage"), "stage"]


def test_systemd_timer_and_installer_are_wired() -> None:
    service = (ROOT / "ops/systemd/horizon-sunlit-auto-update.service").read_text(encoding="utf-8")
    timer = (ROOT / "ops/systemd/horizon-sunlit-auto-update.timer").read_text(encoding="utf-8")
    from game_control.deployment_manifest import get_manifest

    installed_names = {
        spec.target.rsplit("/", 1)[-1]
        for spec in get_manifest().files
        if spec.target.startswith("/usr/local/libexec/")
    }
    assert "ExecStart=/usr/local/libexec/horizon-sunlit-auto-update" in service
    assert "TimeoutStartSec=4h" in service
    assert "OnCalendar=*-*-* 05:00:00 America/New_York" in timer
    assert "Persistent=true" in timer
    for name in (
        "horizon-sunlit-auto-update",
        "horizon-sunlit-update-rpc",
    ):
        assert name in installed_names
    assert "horizon-sunlit-manifest" not in installed_names
    assert "horizon-sunlit-stage" not in installed_names


def test_installed_updater_uses_deployed_venv_interpreter() -> None:
    helper = (ROOT / "ops/bin/horizon-sunlit-auto-update").read_text(encoding="utf-8")
    assert helper.splitlines()[0] == "#!/opt/game-control/.venv/bin/python"
    assert "from game_control.sunlit_update import main" in helper
