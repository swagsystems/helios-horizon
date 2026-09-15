from __future__ import annotations

import hashlib
import json
import os
import errno
import subprocess
import sqlite3
import tarfile
import io
import time
from pathlib import Path

import pytest

from game_control.backups import BackupService, RestoreService, _ExternalArchive
from game_control.errors import SafeError
from game_control.models import (
    AdapterKind,
    OperationName,
    PathSpec,
    PortSpec,
    ProcessSpec,
    Profile,
    ProfileId,
    BackupDestination,
    UpdateSpec,
)


def _profile(tmp_path: Path) -> Profile:
    data = tmp_path / "data"
    backup = tmp_path / "backups"
    data.mkdir()
    return Profile(
        id=ProfileId.TERRARIA_VANILLA,
        display_name="Vanilla",
        adapter=AdapterKind.SYSTEMD,
        systemd_unit="terraria-vanilla.service",
        process=ProcessSpec(executable=Path("/usr/bin/false")),
        ports=(PortSpec(protocol="tcp", port=7777),),
        start_timeout_seconds=5,
        stop_timeout_seconds=5,
        health_timeout_seconds=5,
        paths=PathSpec(
            data_roots=(data,),
            mutable_root=data,
            backup_root=backup,
            install_root=data,
            version_file=data / "version",
        ),
        min_available_memory_bytes=1,
        min_free_disk_bytes=1,
        operations=frozenset({OperationName.BACKUP, OperationName.RESTORE}),
        update=UpdateSpec(kind="manual"),
    )


def _sunlit_profile(tmp_path: Path) -> Profile:
    profile = _profile(tmp_path).model_copy(
        update={
            "id": ProfileId.MINECRAFT_SUNLIT_COBBLEMON,
            "display_name": "Sunlit",
            "systemd_unit": "minecraft-sunlit-cobblemon.service",
            "ports": (PortSpec(protocol="tcp", port=25565),),
        }
    )
    return profile


class _Db:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def insert_backup(self, **row):
        self.rows.append(row)

    def delete_backup(self, backup_id):
        self.rows[:] = [row for row in self.rows if row["id"] != backup_id]

    def list_backups(self, profile_id):
        return list(self.rows)


class _Catalog:
    """Real scratch catalog exposing the ``.connection`` the prune fence inspects."""

    def __init__(self, path: Path) -> None:
        self.connection = sqlite3.connect(path)
        self.connection.execute("CREATE TABLE backups (id TEXT PRIMARY KEY)")
        self.rows: list[dict] = []

    def insert_backup(self, **row):
        self.rows.append(row)
        self.connection.execute("INSERT OR REPLACE INTO backups (id) VALUES (?)", (row["id"],))
        self.connection.commit()

    def delete_backup(self, backup_id):
        self.rows[:] = [row for row in self.rows if row["id"] != backup_id]
        self.connection.execute("DELETE FROM backups WHERE id = ?", (backup_id,))
        self.connection.commit()

    def list_backups(self, profile_id):
        return list(self.rows)


def test_create_writes_deterministic_manifest_and_verified_archive(tmp_path: Path):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "z.txt").write_text("z")
    (profile.paths.mutable_root / "a.txt").write_text("a")
    db = _Db()

    result = BackupService(profile, database=db, stopped_check=lambda: True).create()

    assert result.verified is True
    assert result.path.exists()
    assert not list(profile.paths.backup_root.glob("*.partial"))
    manifest = json.loads(
        subprocess.run(
            ["/usr/bin/tar", "--zstd", "--extract", "--to-stdout", "--file", str(result.path), "manifest.json"],
            check=True,
            capture_output=True,
        ).stdout
    )
    assert manifest["profile_id"] == "terraria-vanilla"
    assert [entry["path"] for entry in manifest["entries"]] == ["a.txt", "z.txt"]
    assert manifest["entries"][0]["sha256"] == hashlib.sha256(b"a").hexdigest()
    assert db.rows and db.rows[0]["verified"] is True


def test_offline_archive_runner_receives_maintenance_prefix(tmp_path: Path, monkeypatch):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_text("world")
    calls = []

    monkeypatch.setattr("game_control.maintenance_process.active_block_schedulers", lambda: ("none",))

    def fail_tar(argv, **_kwargs):
        calls.append(argv)
        raise subprocess.CalledProcessError(1, argv)

    with pytest.raises(SafeError, match="could not be verified"):
        BackupService(profile, stopped_check=lambda: True, tar_runner=fail_tar).create()

    assert calls
    assert calls[0][:6] == ["/usr/bin/systemd-run", "--wait", "--pipe", "--quiet", "--service-type=exec", "--slice=maintenance.slice"]
    assert "/usr/bin/tar" in calls[0]


def test_online_archive_runner_receives_maintenance_prefix(tmp_path: Path, monkeypatch):
    profile = _sunlit_profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_text("world")
    calls = []

    monkeypatch.setattr("game_control.maintenance_process.active_block_schedulers", lambda: ("none",))

    class Online:
        def save_off(self):
            return None

        def save_all_flush(self):
            return None

        def save_on(self):
            return None

    def fail_tar(argv, **_kwargs):
        calls.append(argv)
        raise subprocess.CalledProcessError(1, argv)

    with pytest.raises(SafeError, match="could not be verified"):
        BackupService(profile, online_transport=Online(), tar_runner=fail_tar).create_online()

    assert calls
    assert calls[0][:6] == ["/usr/bin/systemd-run", "--wait", "--pipe", "--quiet", "--service-type=exec", "--slice=maintenance.slice"]
    assert "/usr/bin/tar" in calls[0]


def test_external_zstd_reader_receives_maintenance_prefix(tmp_path: Path, monkeypatch):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_text("world")
    result = BackupService(profile, stopped_check=lambda: True).create()
    import game_control.backups as backups

    observed = []
    real_popen = backups.subprocess.Popen
    monkeypatch.setattr("game_control.maintenance_process.active_block_schedulers", lambda: ("none",))

    def recording_popen(argv, *args, **kwargs):
        observed.append(argv)
        return real_popen(argv, *args, **kwargs)

    monkeypatch.setattr(backups.subprocess, "Popen", recording_popen)
    with _ExternalArchive(result.path) as archive:
        assert archive.members

    assert observed
    assert observed[0][:6] == ["/usr/bin/systemd-run", "--wait", "--pipe", "--quiet", "--service-type=exec", "--slice=maintenance.slice"]
    assert "/usr/bin/zstd" in observed[0]


def test_create_uses_explicit_mutable_backup_roots_not_immutable_data_roots(tmp_path: Path):
    profile = _profile(tmp_path)
    immutable = tmp_path / "immutable"
    immutable.mkdir()
    (profile.paths.mutable_root / "world.wld").write_text("world")
    (immutable / "release.jar").write_text("release")
    profile = profile.model_copy(
        update={
            "paths": PathSpec(
                data_roots=(profile.paths.mutable_root, immutable),
                backup_roots=(profile.paths.mutable_root,),
                mutable_root=profile.paths.mutable_root,
                backup_root=profile.paths.backup_root,
                install_root=immutable,
                version_file=immutable / "version",
            )
        }
    )

    result = BackupService(profile, stopped_check=lambda: True).create()
    manifest = json.loads(
        subprocess.run(
            [
                "/usr/bin/tar",
                "--zstd",
                "--extract",
                "--to-stdout",
                "--file",
                str(result.path),
                "manifest.json",
            ],
            check=True,
            capture_output=True,
        ).stdout
    )

    assert [entry["path"] for entry in manifest["entries"]] == ["world.wld"]
    assert b"release.jar" not in result.path.read_bytes()


def test_create_excludes_symlink_and_partial_and_checks_free_space(tmp_path: Path):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_text("world")
    (profile.paths.mutable_root / "escape").symlink_to("/etc/passwd")
    (profile.paths.mutable_root / "leftover.partial").write_text("ignore")
    with pytest.raises(SafeError, match="free space"):
        BackupService(
            profile,
            free_space=lambda _path: 0,
            stopped_check=lambda: True,
        ).create()
    assert not list(profile.paths.backup_root.glob("*.partial"))


def test_create_requires_stopped_profile(tmp_path: Path):
    profile = _profile(tmp_path)

    with pytest.raises(SafeError, match="stopped"):
        BackupService(profile, stopped_check=lambda: False).create()


def test_create_fences_final_publication_when_lease_is_lost(tmp_path: Path):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_bytes(b"world")
    db = _Db()

    with pytest.raises(SafeError, match="lease was lost") as raised:
        BackupService(
            profile,
            database=db,
            stopped_check=lambda: True,
            lease_check=lambda: False,
        ).create()

    assert raised.value.code == "slot_conflict"
    assert not list(profile.paths.backup_root.glob("*.tar.zst"))
    assert not list(profile.paths.backup_root.glob("*.partial"))
    assert db.rows == []


def test_create_rolls_back_local_catalog_if_lease_lost_after_insert(tmp_path: Path):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_bytes(b"world")
    db = _Db()
    checks = iter((True, True, False))
    with pytest.raises(SafeError, match="lease was lost"):
        BackupService(
            profile, database=db, stopped_check=lambda: True,
            lease_check=lambda: next(checks),
        ).create()
    assert db.rows == []
    assert not list(profile.paths.backup_root.glob("*.tar.zst"))


def test_create_preserves_catalog_after_remote_side_effect_on_lease_loss(tmp_path: Path):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_bytes(b"world")
    db = _Db()

    class Protection:
        database = db

        def protect(self, record):
            self.record = record

    protection = Protection()
    checks = iter((True, True, True, False))
    with pytest.raises(SafeError, match="lease was lost"):
        BackupService(
            profile, database=db, stopped_check=lambda: True,
            protection_service=protection, lease_check=lambda: next(checks),
        ).create(destination=BackupDestination.HORIZON_B2)
    assert db.rows and db.rows[0]["verified"] is True
    assert protection.record.path.exists()


def test_create_preserves_catalog_when_remote_protect_fails_after_side_effect(tmp_path: Path):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_bytes(b"world")
    db = _Db()

    class Protection:
        database = db

        def protect(self, record):
            self.record = record
            raise SafeError("backup_protection_failed", "remote operation failed")

    with pytest.raises(SafeError, match="remote operation failed"):
        BackupService(
            profile, database=db, stopped_check=lambda: True,
            protection_service=Protection(), lease_check=lambda: True,
        ).create(destination=BackupDestination.HORIZON_B2)
    assert db.rows and db.rows[0]["verified"] is True


def test_online_create_orders_save_controls_and_copies_live_inodes(tmp_path: Path):
    profile = _sunlit_profile(tmp_path)
    source = profile.paths.mutable_root / "world.wld"
    source.write_bytes(b"world")
    events = []

    class Online:
        def save_off(self): events.append("save-off")
        def save_all_flush(self): events.append("save-all flush")
        def save_on(self): events.append("save-on")

    result = BackupService(profile, online_transport=Online()).create_online(protected=True)

    assert result.verified is True
    assert events == ["save-off", "save-all flush", "save-on"]
    assert result.path.exists()


def test_sunlit_application_backup_excludes_only_redundant_top_level_backups(tmp_path: Path):
    profile = _sunlit_profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_bytes(b"world")
    redundant = profile.paths.mutable_root / "backups"
    redundant.mkdir()
    (redundant / "redundant.bin").write_bytes(b"redundant")
    nested = profile.paths.mutable_root / "world" / "backups"
    nested.mkdir(parents=True)
    (nested / "retained.bin").write_bytes(b"retained")
    logs = profile.paths.mutable_root / "logs"
    logs.mkdir()
    (logs / "latest.log").write_bytes(b"log")

    online = type(
        "Online",
        (),
        {"save_off": lambda self: None, "save_all_flush": lambda self: None, "save_on": lambda self: None},
    )()
    result = BackupService(profile, online_transport=online).create_online()
    manifest = json.loads(
        subprocess.run(
            ["/usr/bin/tar", "--zstd", "--extract", "--to-stdout", "--file", str(result.path), "manifest.json"],
            check=True,
            capture_output=True,
        ).stdout
    )

    paths = [entry["path"] for entry in manifest["entries"]]
    assert paths == ["logs/latest.log", "world.wld", "world/backups/retained.bin"]
    expected_size = sum(
        (profile.paths.mutable_root / relative).stat().st_size for relative in paths
    ) + 65536
    assert BackupService(profile)._estimate((profile.paths.mutable_root,)) == expected_size


def test_sunlit_application_backup_excludes_versioned_server_backups(tmp_path: Path):
    profile = _sunlit_profile(tmp_path)
    root = profile.paths.mutable_root
    (root / ".versions/v1/backups").mkdir(parents=True)
    (root / ".versions/v1/backups/redundant.zip").write_bytes(b"redundant")
    (root / ".versions/v1/config").mkdir(parents=True)
    (root / ".versions/v1/config/kept.toml").write_bytes(b"kept")

    service = BackupService(profile, stopped_check=lambda: True)
    paths = [path.relative_to(root).as_posix() for path in service._walk_source(root)]

    assert paths == [".versions/v1/config/kept.toml"]


def test_non_sunlit_application_backup_keeps_top_level_backups_directory(tmp_path: Path):
    profile = _profile(tmp_path)
    nested = profile.paths.mutable_root / "backups"
    nested.mkdir()
    (nested / "retained.bin").write_bytes(b"retained")

    result = BackupService(profile, stopped_check=lambda: True).create()
    manifest = json.loads(
        subprocess.run(
            ["/usr/bin/tar", "--zstd", "--extract", "--to-stdout", "--file", str(result.path), "manifest.json"],
            check=True,
            capture_output=True,
        ).stdout
    )

    assert [entry["path"] for entry in manifest["entries"]] == ["backups/retained.bin"]


def test_sunlit_restore_intentionally_discards_redundant_server_backup_tree(tmp_path: Path):
    profile = _sunlit_profile(tmp_path)
    world = profile.paths.mutable_root / "world.wld"
    world.write_bytes(b"protected-world")
    redundant = profile.paths.mutable_root / "backups"
    redundant.mkdir()
    (redundant / "server-copy.zip").write_bytes(b"redundant")
    online = type(
        "Online",
        (),
        {"save_off": lambda self: None, "save_all_flush": lambda self: None, "save_on": lambda self: None},
    )()
    archive = BackupService(profile, online_transport=online).create_online().path
    world.write_bytes(b"mutated-world")

    stopped_backup = BackupService(profile, stopped_check=lambda: True)
    restored = RestoreService(
        profile,
        backup_service=stopped_backup,
        stopped_check=lambda: True,
    ).restore(archive)

    assert restored.destination == profile.paths.mutable_root
    assert (profile.paths.mutable_root / "world.wld").read_bytes() == b"protected-world"
    assert not (profile.paths.mutable_root / "backups").exists()


def test_online_manifest_uses_immutable_staged_bytes_after_source_mutates(tmp_path: Path, monkeypatch):
    profile = _sunlit_profile(tmp_path)
    source = profile.paths.mutable_root / "world.wld"
    original = b"immutable-world"
    source.write_bytes(original)
    import game_control.backups as backups

    real_copy = backups._stage_file_copy

    def copy_then_mutate(source_path, destination_path, *, deadline=None):
        digest = real_copy(source_path, destination_path, deadline=deadline)
        source_path.write_bytes(b"mutated-live-source")
        return digest

    def reject_staged_hash(path):
        if ".online-stage-" in str(path):
            pytest.fail("online snapshot re-read staged bytes before save-on")
        raise AssertionError("unexpected online hash")

    monkeypatch.setattr(backups, "_stage_file_copy", copy_then_mutate)
    monkeypatch.setattr(backups, "_sha256", reject_staged_hash)
    result = BackupService(profile, online_transport=type(
        "Online",
        (),
        {"save_off": lambda self: None, "save_all_flush": lambda self: None, "save_on": lambda self: None},
    )()).create_online()

    manifest = json.loads(subprocess.run(
        ["/usr/bin/tar", "--zstd", "--extract", "--to-stdout", "--file", str(result.path), "manifest.json"],
        check=True,
        capture_output=True,
    ).stdout)
    entry = manifest["entries"][0]
    assert entry["size"] == len(original)
    assert entry["mode"] == 0o400
    assert entry["sha256"] == hashlib.sha256(original).hexdigest()


def test_online_chunk_copy_enforces_deadline_after_final_file(tmp_path: Path, monkeypatch):
    import game_control.backups as backups

    source = tmp_path / "world.wld"
    destination = tmp_path / "staged.wld"
    source.write_bytes(b"world")
    ticks = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(backups.time, "monotonic", lambda: next(ticks))

    with pytest.raises(SafeError, match="staging exceeded"):
        backups._stage_file_copy(source, destination, deadline=1.0)


def test_online_create_always_attempts_save_on_after_flush_failure(tmp_path: Path):
    profile = _sunlit_profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_bytes(b"world")
    events = []

    class Online:
        def save_off(self): events.append("save-off")
        def save_all_flush(self):
            events.append("save-all flush")
            raise RuntimeError("not safe to expose")
        def save_on(self): events.append("save-on")

    with pytest.raises(SafeError, match="quiesce"):
        BackupService(profile, online_transport=Online()).create_online()
    assert events == ["save-off", "save-all flush", "save-on"]
    assert not list(profile.paths.backup_root.glob("*.tar.zst"))


def test_online_create_bounds_copy_window_and_resumes_saving(tmp_path: Path, monkeypatch):
    profile = _sunlit_profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_bytes(b"world")
    events = []

    class Online:
        def save_off(self): events.append("save-off")
        def save_all_flush(self): events.append("save-all flush")
        def save_on(self): events.append("save-on")

    def timeout(*_args, **_kwargs):
        raise SafeError("backup_quiesce_timeout", "online backup staging exceeded its bound")

    monkeypatch.setattr(BackupService, "_snapshot", timeout)

    with pytest.raises(SafeError, match="quiesce"):
        BackupService(profile, online_transport=Online()).create_online(max_snapshot_seconds=0.01)
    assert events == ["save-off", "save-all flush", "save-on"]


def test_create_failure_removes_partial_archive_and_staging(tmp_path: Path):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_text("world")

    def fail_tar(*_args, **_kwargs):
        raise subprocess.CalledProcessError(1, ["tar"])

    with pytest.raises(SafeError, match="could not be verified"):
        BackupService(profile, stopped_check=lambda: True, tar_runner=fail_tar).create()

    assert not list(profile.paths.backup_root.glob("*.partial"))
    assert not list(profile.paths.backup_root.glob(".stage-*"))
    assert not list(profile.paths.backup_root.glob("*.tar.zst"))


def test_create_rejects_symlink_data_root(tmp_path: Path):
    profile = _profile(tmp_path)
    data_root = profile.paths.data_roots[0]
    outside = tmp_path / "outside"
    outside.mkdir()
    data_root.rmdir()
    data_root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(SafeError, match="data root is a symlink"):
        BackupService(profile, stopped_check=lambda: True).create()


def test_create_skips_partial_files_and_symlinks_in_a_verified_backup(tmp_path: Path):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_text("world")
    (profile.paths.mutable_root / "leftover.partial").write_text("ignore")
    (profile.paths.mutable_root / "escape").symlink_to("/etc/passwd")

    result = BackupService(profile, stopped_check=lambda: True).create()
    manifest = json.loads(
        subprocess.run(
            ["/usr/bin/tar", "--zstd", "--extract", "--to-stdout", "--file", str(result.path), "manifest.json"],
            check=True,
            capture_output=True,
        ).stdout
    )

    assert [entry["path"] for entry in manifest["entries"]] == ["world.wld"]


def test_create_reports_source_read_failure_and_cleans_staging(tmp_path: Path, monkeypatch):
    profile = _profile(tmp_path)
    (profile.paths.mutable_root / "world.wld").write_text("world")
    import game_control.backups as backups

    monkeypatch.setattr(backups, "_stage_file", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("read failed")))
    with pytest.raises(SafeError, match="source could not be read"):
        BackupService(profile, stopped_check=lambda: True).create()

    assert not list(profile.paths.backup_root.glob(".stage-*"))


def test_create_rejects_unverified_archive_and_removes_partial(tmp_path: Path):
    profile = _profile(tmp_path)

    def write_invalid_archive(argv, **_kwargs):
        archive_path = argv[argv.index("--file") + 1]
        with tarfile.open(archive_path, "w") as archive:
            info = tarfile.TarInfo("payload/world")
            payload = b"not a manifest"
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))

    with pytest.raises(SafeError, match="manifest is missing"):
        BackupService(profile, stopped_check=lambda: True, tar_runner=write_invalid_archive).create()

    assert not list(profile.paths.backup_root.glob("*.partial"))


def test_protect_rejects_unknown_backup(tmp_path: Path):
    profile = _profile(tmp_path)

    with pytest.raises(SafeError, match="not found"):
        BackupService(profile, stopped_check=lambda: True).protect("missing")


def test_snapshot_uses_hardlinks_on_same_filesystem(tmp_path: Path, monkeypatch):
    profile = _profile(tmp_path)
    source = profile.paths.mutable_root / "world.wld"
    source.write_bytes(b"world")
    import game_control.backups as backups

    linked: list[tuple[Path, Path]] = []
    original_link = backups.os.link

    def link(src, dst, **kwargs):
        linked.append((Path(src), Path(dst)))
        return original_link(src, dst, **kwargs)

    monkeypatch.setattr(backups.os, "link", link)
    monkeypatch.setattr(backups.shutil, "copy2", lambda *_args, **_kwargs: pytest.fail("copy2 used"))
    BackupService(profile, stopped_check=lambda: True).create()
    assert linked and linked[0][0] == source


def test_snapshot_falls_back_to_copy_on_cross_device_link(tmp_path: Path, monkeypatch):
    profile = _profile(tmp_path)
    source = profile.paths.mutable_root / "world.wld"
    source.write_bytes(b"world")
    import game_control.backups as backups

    copied = []
    ownership = []
    monkeypatch.setattr(backups.os, "link", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError(errno.EXDEV, "cross-device")))
    original_copy = backups.shutil.copy2
    original_chown = backups.os.chown
    monkeypatch.setattr(backups.shutil, "copy2", lambda *args, **kwargs: (copied.append(args), original_copy(*args, **kwargs))[1])
    monkeypatch.setattr(
        backups.os,
        "chown",
        lambda *args, **kwargs: (
            ownership.append((args, kwargs)),
            original_chown(*args, **kwargs),
        )[1],
    )
    BackupService(profile, stopped_check=lambda: True).create()
    assert copied
    assert ownership
    args, kwargs = ownership[0]
    assert args[1:] == (source.stat().st_uid, source.stat().st_gid)
    assert kwargs == {"follow_symlinks": False}


def test_protected_retention_keeps_two_verified_and_never_sole_verified(tmp_path: Path):
    profile = _profile(tmp_path)
    # The filesystem-only legacy path has no persisted protection flags, but
    # retention still keeps at least two verified payloads.
    uncatalogued = BackupService(profile, database=None, stopped_check=lambda: True)
    plain = [uncatalogued.create() for _ in range(3)]
    uncatalogued.prune(keep=2)
    assert sum(result.path.exists() for result in plain) == 2
    uncatalogued.prune(keep=0)
    assert sum(result.path.exists() for result in plain) == 2

    # Catalogued path: a durable catalog the fence can inspect must refuse
    # legacy deletion of a catalog-backed payload instead of deleting it.
    catalog = _Catalog(tmp_path / "catalog.db")
    catalogued = BackupService(profile, database=catalog, stopped_check=lambda: True)
    results = [catalogued.create(protected=index == 0) for index in range(3)]
    assert next(item for item in catalogued.list() if item.id == results[0].id).protected
    with pytest.raises(SafeError, match="retirement operation"):
        catalogued.prune(keep=1)
    assert all(result.path.exists() for result in results)
    catalog.connection.close()

    # A catalog reporting retained rows but lacking an inspectable connection
    # also fails closed. An empty fake would exercise the no-op branch only.
    opaque = BackupService(profile, database=_Db(), stopped_check=lambda: True)
    opaque_results = [opaque.create(protected=index == 0) for index in range(3)]
    with pytest.raises(SafeError, match="could not be inspected"):
        opaque.prune(keep=1)
    assert all(result.path.exists() for result in opaque_results)
