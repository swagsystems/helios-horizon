"""Tiny local regressions for release execution and restore error recovery."""

from __future__ import annotations

import hashlib
import io
import os
import shutil
import stat
import tarfile
import zipfile
from pathlib import Path

import pytest

import game_control.backups as backups
import game_control.updates as updates
from game_control.backups import BackupService, RestoreService
from game_control.errors import SafeError
from game_control.models import (
    AdapterKind,
    OperationName,
    PathSpec,
    PortSpec,
    ProcessSpec,
    Profile,
    ProfileId,
    UpdateSpec,
)
from game_control.updates import UpdateService


def _profile(tmp_path: Path) -> Profile:
    data = tmp_path / "data"
    install = tmp_path / "install"
    data.mkdir()
    install.mkdir()
    return Profile(
        id=ProfileId.TERRARIA_TMOD,
        display_name="Fixture game",
        adapter=AdapterKind.SYSTEMD,
        systemd_unit="fixture-game.service",
        process=ProcessSpec(executable=Path("/usr/bin/false")),
        ports=(PortSpec(protocol="tcp", port=7778),),
        start_timeout_seconds=5,
        stop_timeout_seconds=5,
        health_timeout_seconds=5,
        paths=PathSpec(
            data_roots=(data,),
            mutable_root=data,
            backup_root=tmp_path / "backups",
            install_root=install,
            version_file=install / "version",
        ),
        min_available_memory_bytes=1,
        min_free_disk_bytes=1,
        operations=frozenset({OperationName.BACKUP, OperationName.RESTORE}),
        update=UpdateSpec(kind="manual"),
    )


def _plain_tar_runner(argv, **_kwargs):
    """Use a supported native tar fixture, never a transient systemd service."""
    destination = Path(argv[argv.index("--file") + 1])
    staging = Path(argv[argv.index("--directory") + 1])
    filelist = Path(argv[argv.index("--files-from") + 1])
    with tarfile.open(destination, "w") as archive:
        for name in filelist.read_bytes().split(b"\0"):
            if name:
                relative = os.fsdecode(name)
                archive.add(staging / relative, arcname=relative, recursive=False)


@pytest.fixture
def local_backup_transport(monkeypatch: pytest.MonkeyPatch):
    # Only the archive transport is replaced; snapshot/validation/publication
    # and every restore filesystem operation remain the production writer.
    monkeypatch.setattr(backups, "maintenance_argv", lambda argv, **_kwargs: argv)


def _backup(profile: Profile) -> BackupService:
    return BackupService(
        profile,
        stopped_check=lambda: True,
        tar_runner=_plain_tar_runner,
    )


@pytest.mark.parametrize("archive_kind", ["zip", "tar"])
def test_release_keeps_executable_for_direct_version_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    local_backup_transport,
    archive_kind: str,
):
    # Keep quota checks real but byte-sized, independent of host free space.
    monkeypatch.setattr(updates, "MAX_DOWNLOAD_BYTES", 16_384)
    monkeypatch.setattr(updates, "MAX_MEMBER_BYTES", 1_024)
    monkeypatch.setattr(updates, "MAX_EXTRACTED_BYTES", 4_096)
    profile = _profile(tmp_path)
    prior = profile.paths.install_root / "releases" / "prior"
    prior.mkdir(parents=True)
    (prior / "game").write_text("prior release")
    current = profile.paths.install_root / "current"
    current.symlink_to(prior, target_is_directory=True)
    archive_path = tmp_path / f"release.{archive_kind}"
    script = b"#!/bin/sh\nprintf '2.0\\n'\n"
    if archive_kind == "zip":
        member = zipfile.ZipInfo("game")
        member.create_system = 3  # Unix ZIP metadata, not a DOS permission guess.
        member.external_attr = (stat.S_IFREG | 0o755) << 16
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr(member, script)
    else:
        member = tarfile.TarInfo("game")
        member.mode = 0o755
        member.size = len(script)
        with tarfile.open(archive_path, "w") as archive:
            archive.addfile(member, io.BytesIO(script))
    profile = profile.model_copy(update={
        "update": UpdateSpec(
            kind="release_symlink",
            download_url=f"https://updates.example.invalid/release.{archive_kind}",
            sha256=hashlib.sha256(archive_path.read_bytes()).hexdigest(),
            executable_relative_path="game",
            version_command=(str(current / "game"), "--version"),
        ),
        "operations": frozenset({
            OperationName.BACKUP, OperationName.RESTORE,
            OperationName.UPDATE_CHECK, OperationName.UPDATE_APPLY,
        }),
    })
    service = UpdateService(
        profile,
        backup_service=_backup(profile),
        downloader=lambda _profile, destination: shutil.copyfile(archive_path, destination),
        stopped_check=lambda _profile: True,
        http_client=object(),  # Borrowed; the pinned local downloader is used.
    )

    result = service.apply(profile.id)

    assert result.state == "succeeded"
    assert os.access(current / "game", os.X_OK)
    assert (current / "game").read_bytes() == script
    assert (prior / "game").read_text() == "prior release"


@pytest.mark.parametrize("failure_phase", ["displace", "publish"])
def test_restore_fsync_error_rolls_back_all_roots_before_returning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    local_backup_transport,
    failure_phase: str,
):
    profile = _profile(tmp_path)
    roots = (profile.paths.mutable_root, profile.paths.install_root)
    profile = profile.model_copy(update={
        "paths": profile.paths.model_copy(update={
            "data_roots": roots, "backup_roots": roots,
        }),
    })
    for index, root in enumerate(roots):
        (root / "world").write_text(f"archive-{index}")
    service_backup = _backup(profile)
    archive = service_backup.create()
    for index, root in enumerate(roots):
        (root / "world").write_text(f"current-{index}")

    original_fsync_dir = backups._fsync_dir
    injected = False

    def fail_after_rename(path: Path):
        nonlocal injected
        first = roots[0]
        displaced = not first.exists() and bool(list(tmp_path.glob(".rollback-*")))
        published = first.is_dir() and (first / "world").read_text() == "archive-0"
        reached_phase = displaced if failure_phase == "displace" else published
        if not injected and path == first.parent and reached_phase:
            injected = True
            raise OSError("injected one-shot directory fsync failure after rename")
        original_fsync_dir(path)

    restore = RestoreService(
        profile, backup_service=service_backup, stopped_check=lambda: True,
    )
    with monkeypatch.context() as fault:
        fault.setattr(backups, "_fsync_dir", fail_after_rename)
        with pytest.raises(SafeError, match="could not be completed"):
            restore.restore(archive.path)
    assert injected
    immediate_contents = tuple(
        (root / "world").read_text() if root.is_dir() else "<missing root>"
        for root in roots
    )

    # The existing durable journal can repair it on a later daemon startup.
    # That is distinct from the failed RPC's immediate rollback contract.
    restore.reconcile()
    assert tuple((root / "world").read_text() for root in roots) == (
        "current-0", "current-1",
    )
    assert immediate_contents == ("current-0", "current-1")


@pytest.mark.parametrize(
    ("create_system", "archive_mode", "expected_mode"),
    [
        (3, stat.S_IFREG | 0o755, 0o755),
        (3, stat.S_IFREG | 0o644, 0o644),
        (3, stat.S_IFREG | 0o7777, 0o755),
        (3, stat.S_IFREG | 0o6666, 0o644),
        (0, stat.S_IFREG | 0o755, 0o644),
        (3, 0o755, 0o644),
    ],
    ids=["unix-executable", "unix-data", "unix-privileged", "unix-writable",
         "dos-mode-is-not-unix", "missing-regular-type"],
)
def test_zip_permissions_only_keep_safe_unix_regular_execute_bits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    create_system: int,
    archive_mode: int,
    expected_mode: int,
):
    monkeypatch.setattr(updates, "MAX_MEMBER_BYTES", 128)
    monkeypatch.setattr(updates, "MAX_EXTRACTED_BYTES", 256)
    archive_path = tmp_path / "release.zip"
    member = zipfile.ZipInfo("game")
    member.create_system = create_system
    member.external_attr = archive_mode << 16
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(member, b"tiny fixture")
    staging = tmp_path / "staging"
    staging.mkdir()

    UpdateService({}, http_client=object())._safe_extract(archive_path, staging, "game")

    assert stat.S_IMODE((staging / "game").stat().st_mode) == expected_mode


@pytest.mark.parametrize(
    "rollback_failure", ["remove-published", "restore-original", "sync-restored"],
)
def test_restore_retains_journal_when_error_rollback_also_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    local_backup_transport,
    rollback_failure: str,
):
    profile = _profile(tmp_path)
    roots = (profile.paths.mutable_root, profile.paths.install_root)
    profile = profile.model_copy(update={
        "paths": profile.paths.model_copy(update={
            "data_roots": roots, "backup_roots": roots,
        }),
    })
    for index, root in enumerate(roots):
        (root / "world").write_text(f"archive-{index}")
    service_backup = _backup(profile)
    archive = service_backup.create()
    for index, root in enumerate(roots):
        (root / "world").write_text(f"current-{index}")
    restore = RestoreService(
        profile, backup_service=service_backup, stopped_check=lambda: True,
    )
    original_fsync = backups._fsync_dir
    original_rmtree = backups.shutil.rmtree
    original_replace = backups.os.replace
    injected_publication = False
    injected_rollback = False

    def fail_publication_sync(path):
        nonlocal injected_publication, injected_rollback
        world = roots[0] / "world"
        if (not injected_publication and path == roots[0].parent
                and world.is_file() and world.read_text() == "archive-0"):
            injected_publication = True
            raise OSError("injected publication directory sync failure")
        if (rollback_failure == "sync-restored" and injected_publication
                and path == roots[0].parent and world.is_file()
                and world.read_text() == "current-0"):
            injected_rollback = True
            raise OSError("injected restored directory sync failure")
        original_fsync(path)

    def fail_remove(path, *args, **kwargs):
        nonlocal injected_rollback
        if rollback_failure == "remove-published" and Path(path) == roots[0]:
            injected_rollback = True
            raise OSError("injected rollback removal failure")
        return original_rmtree(path, *args, **kwargs)

    def fail_restore(source, destination):
        nonlocal injected_rollback
        if (rollback_failure == "restore-original"
                and Path(source).name.startswith(".rollback-")):
            injected_rollback = True
            raise OSError("injected rollback rename failure")
        return original_replace(source, destination)

    with monkeypatch.context() as fault:
        fault.setattr(backups, "_fsync_dir", fail_publication_sync)
        fault.setattr(backups.shutil, "rmtree", fail_remove)
        fault.setattr(backups.os, "replace", fail_restore)
        with pytest.raises(SafeError, match="could not be completed"):
            restore.restore(archive.path)
    assert injected_publication
    assert injected_rollback
    assert len(list(profile.paths.backup_root.glob(".restore-journal-*.json"))) == 1
    expected_rollbacks = 1 if rollback_failure == "sync-restored" else 2
    assert len(list(tmp_path.glob(".rollback-*"))) == expected_rollbacks

    restore.reconcile()

    assert tuple((root / "world").read_text() for root in roots) == (
        "current-0", "current-1",
    )
    assert not list(profile.paths.backup_root.glob(".restore-journal-*.json"))
    assert not list(tmp_path.glob(".rollback-*"))


@pytest.mark.parametrize("unsafe_member", ["symlink", "../outside"])
def test_zip_permission_handling_does_not_allow_unsafe_members(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_member: str,
):
    monkeypatch.setattr(updates, "MAX_MEMBER_BYTES", 128)
    monkeypatch.setattr(updates, "MAX_EXTRACTED_BYTES", 256)
    archive_path = tmp_path / "release.zip"
    member = zipfile.ZipInfo("game" if unsafe_member == "symlink" else unsafe_member)
    member.create_system = 3
    file_type = stat.S_IFLNK if unsafe_member == "symlink" else stat.S_IFREG
    member.external_attr = (file_type | 0o755) << 16
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(member, b"outside")
    staging = tmp_path / "staging"
    staging.mkdir()

    with pytest.raises(SafeError, match="release archive is invalid"):
        UpdateService({}, http_client=object())._safe_extract(archive_path, staging, "game")

    assert not list(staging.iterdir())
    assert not (tmp_path / "outside").exists()
