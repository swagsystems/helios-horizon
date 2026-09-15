from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import sqlite3
import tarfile
import threading
import zipfile
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace

import pytest

from game_control.errors import SafeError
from game_control.models import (
    AdapterKind,
    CuratedModpackSpec,
    OperationName,
    PathSpec,
    PortSpec,
    ProcessSpec,
    Profile,
    ProfileId,
    UpdateSpec,
)
from game_control.protocol import UpdateStatus
from game_control.updates import MAX_EXTRACTED_BYTES, MAX_MEMBER_BYTES, UpdateService


_DiskUsage = namedtuple("_DiskUsage", "total used free")


@pytest.fixture
def ample_extraction_space(monkeypatch: pytest.MonkeyPatch):
    """Deterministic free space so extraction cases exercise their own behavior.

    The production guard is unchanged: it is only fed a bounded fake reading so
    a host with little free space does not pre-empt the success/invalid-archive
    assertions. The low-disk refusal itself is still proven below.
    """
    monkeypatch.setattr(
        shutil, "disk_usage", lambda _path: _DiskUsage(10**12, 0, 10**12)
    )


class _FalseyClient:
    def __init__(self):
        self.closed = 0

    def __bool__(self):
        return False

    def close(self):
        self.closed += 1


@pytest.mark.asyncio
async def test_falsey_injected_client_is_borrowed_and_remains_open(tmp_path: Path):
    client = _FalseyClient()
    service = UpdateService({}, http_client=client)
    assert service.http_client is client
    await service.aclose()
    await service.aclose()
    assert client.closed == 0


@pytest.mark.asyncio
async def test_default_client_is_closed_once_and_close_cancellation_is_safe():
    service = UpdateService({})
    calls = 0
    started = threading.Event()
    release = threading.Event()

    def close():
        nonlocal calls
        started.set()
        release.wait(2)
        calls += 1

    service.http_client.close = close
    closing = asyncio.create_task(service.aclose())
    assert await asyncio.to_thread(started.wait, 1)
    closing.cancel()
    closing.cancel()
    await asyncio.sleep(0)
    assert not closing.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    await service.aclose()
    assert calls == 1


def _profile(
    tmp_path: Path,
    profile_id=ProfileId.TERRARIA_TMOD,
    kind="release_symlink",
    version_command=("/usr/bin/printf", "2.0"),
    download_url="https://updates.example.invalid/release",
    sha256="a" * 64,
    include_apply=True,
):
    data = tmp_path / "data"
    install = tmp_path / "install"
    backup = tmp_path / "backups"
    data.mkdir()
    install.mkdir()
    return Profile(
        id=profile_id,
        display_name="Game",
        adapter=AdapterKind.SYSTEMD,
        systemd_unit="game.service",
        process=ProcessSpec(executable=Path("/usr/bin/false")),
        ports=(PortSpec(protocol="tcp", port=7777),),
        start_timeout_seconds=5,
        stop_timeout_seconds=5,
        health_timeout_seconds=5,
        paths=PathSpec(
            data_roots=(data,),
            mutable_root=data,
            backup_root=backup,
            install_root=install,
            version_file=install / "version",
        ),
        min_available_memory_bytes=1,
        min_free_disk_bytes=1,
        operations=frozenset(
            {OperationName.UPDATE_CHECK}
            if kind == "manual" or not include_apply
            else {OperationName.UPDATE_CHECK, OperationName.UPDATE_APPLY}
        ),
        update=(
            UpdateSpec(
                kind=kind,
                download_url=download_url,
                executable_relative_path="game",
                version_command=version_command,
                sha256=sha256,
            )
            if kind == "release_symlink"
            else UpdateSpec(kind=kind, app_id=380870)
            if kind == "steamcmd_in_place"
            else UpdateSpec(
                kind="curated_modpack",
                download_url=download_url,
                sha256=sha256,
                curated=CuratedModpackSpec(
                    version="1.1.2-SSV4.1.4",
                    project_id=1495800,
                    file_id=8717959,
                    size_bytes=687280069,
                    manifest_path=tmp_path / "manifest.json",
                    release_root=install / "releases",
                    state_root=data / "state",
                    active_link=data / "current",
                ),
            )
            if kind == "curated_modpack"
            else UpdateSpec(kind="manual")
        ),
    )


class _Backup:
    def __init__(self, path: Path, verified: bool = True):
        self.path, self.verified, self.id = path, verified, "backup-id"

    def create(self, **_kwargs):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(b"full rollback archive")
        return self


def test_release_update_atomically_replaces_current_and_keeps_prior(tmp_path: Path):
    profile = _profile(tmp_path)
    releases = profile.paths.install_root / "releases"
    prior = releases / "prior"
    prior.mkdir(parents=True)
    (prior / "game").write_text("old")
    current = profile.paths.install_root / "current"
    current.symlink_to(prior, target_is_directory=True)

    def stage(_profile, staging: Path):
        (staging / "game").write_text("new")
        return "new"

    service = UpdateService(
        profiles={profile.id.value: profile},
        backup_service=_Backup(tmp_path / "backups" / "pre.tar.zst"),
        stage_release=stage,
        verify_release=lambda _p, _r: True,
        stopped_check=lambda _p: True,
    )
    result = service.apply(profile.id)
    assert result.state == "succeeded"
    assert current.resolve().name != "prior"
    assert prior.exists()


def test_failed_release_verification_rolls_back_and_records_both_outcomes(tmp_path: Path):
    profile = _profile(tmp_path)
    releases = profile.paths.install_root / "releases"
    prior = releases / "prior"
    prior.mkdir(parents=True)
    (prior / "game").write_text("old")
    current = profile.paths.install_root / "current"
    current.symlink_to(prior, target_is_directory=True)

    db = sqlite3.connect(":memory:")
    service = UpdateService(
        profiles={profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        stage_release=lambda _p, staging: (staging / "game").write_text("new") or "new",
        verify_release=lambda _p, _r: False,
        stopped_check=lambda _p: True,
        database=db,
    )
    service.database.execute(
        "CREATE TABLE updates(id TEXT,profile_id TEXT,created_at TEXT,strategy TEXT,prior_version TEXT,new_version TEXT,state TEXT)"
    )
    with pytest.raises(SafeError, match="verification"):
        service.apply(profile.id)
    assert current.resolve() == prior
    states = [row[0] for row in service.database.execute("SELECT state FROM updates")]
    assert states == ["failed", "rolled_back"]
    db.close()


def test_running_profile_and_missing_backup_abort_before_updater(tmp_path: Path):
    profile = _profile(
        tmp_path,
        sha256=hashlib.sha256(b"raw executable payload").hexdigest(),
    )
    called = False

    def updater(_argv):
        nonlocal called
        called = True

    service = UpdateService(
        profiles={profile.id.value: profile},
        backup_service=None,
        runner=updater,
        stopped_check=lambda _p: False,
    )
    with pytest.raises(SafeError, match="running"):
        service.apply(profile.id)
    assert called is False


def test_minecraft_is_manual_only(tmp_path: Path):
    profile = _profile(tmp_path, ProfileId.MINECRAFT, "manual")
    called = False
    service = UpdateService(
        profiles={profile.id.value: profile},
        runner=lambda _argv: called,
        stopped_check=lambda _p: True,
    )
    result = service.apply(profile.id)
    assert result.state == "manual_only"
    assert called is False


def test_pz_uses_fixed_steamcmd_argv(tmp_path: Path, monkeypatch):
    profile = _profile(tmp_path, ProfileId.PZ_RISING, "steamcmd_in_place")
    monkeypatch.setattr("game_control.maintenance_process.active_block_schedulers", lambda: ("none",))
    seen: list[list[str]] = []
    service = UpdateService(
        profiles={profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        runner=lambda argv, **_kwargs: seen.append(argv),
        stopped_check=lambda _p: True,
    )
    result = service.apply(profile.id)
    assert result.state == "succeeded"
    assert seen[0][:6] == ["/usr/bin/systemd-run", "--wait", "--pipe", "--quiet", "--service-type=exec", "--slice=maintenance.slice"]
    assert seen[0][seen[0].index("--") + 1:] == [
        "/usr/bin/nice", "-n", "10", "/opt/steamcmd/steamcmd.sh", "+force_install_dir", "/opt/pzserver",
        "+login", "anonymous", "+app_update", "380870", "-beta", "unstable",
        "validate", "+quit",
    ]


def test_update_check_rejects_unknown_profile_and_keeps_malformed_version_bounded(tmp_path: Path):
    profile = _profile(tmp_path)
    profile.paths.version_file.write_text("VERSION=not-a-version\n" + ("x" * 4096))
    service = UpdateService({profile.id.value: profile})

    status = service.check(profile.id)

    assert status.installed_version == "unknown"
    assert "\n" not in status.installed_version
    with pytest.raises(SafeError, match="not found") as exc:
        service.check(ProfileId.MINECRAFT)
    assert exc.value.code == "profile_not_found"


def test_update_check_falls_back_to_current_release_pointer(tmp_path: Path):
    profile = _profile(tmp_path)
    release = profile.paths.install_root / "releases" / "2026.07.16"
    release.mkdir(parents=True)
    (profile.paths.install_root / "current").symlink_to(release, target_is_directory=True)

    status = UpdateService({profile.id.value: profile}).check(profile.id)

    assert status.installed_version == "2026.07.16"


def test_curated_modpack_status_uses_pinned_candidate_and_active_link(tmp_path: Path):
    profile = _profile(tmp_path, kind="curated_modpack")
    release = profile.update.curated.release_root / "1.0.9-SSV4.0.9"
    release.mkdir(parents=True)
    profile.update.curated.active_link.symlink_to(release, target_is_directory=True)

    status = UpdateService({profile.id.value: profile}).check(profile.id)

    assert status.strategy == "curated_modpack"
    assert status.installed_version == "1.0.9-SSV4.0.9"
    assert status.available_version == "1.1.2-SSV4.1.4"
    assert status.restart_required is True
    assert status.apply_supported is True


def test_manual_profile_uses_bounded_read_only_checker(tmp_path: Path):
    profile = _profile(tmp_path, ProfileId.MINECRAFT_SUNLIT_COBBLEMON, "manual")
    calls = []

    def checker(profile_id):
        calls.append(profile_id)
        return UpdateStatus(
            profile_id=profile_id,
            strategy="manual",
            installed_version="1.1.3-SSV4.1.4",
            available_version="1.1.4-SSV4.1.5",
            restart_required=False,
            apply_supported=False,
            state="available",
            message=None,
        )

    status = UpdateService(
        {profile.id.value: profile},
        manual_checker=checker,
    ).check(profile.id)

    assert calls == [profile.id]
    assert status.state == "available"
    assert status.available_version == "1.1.4-SSV4.1.5"
    assert status.apply_supported is False


def test_manual_profile_without_checker_is_unsupported_not_current(tmp_path: Path):
    profile = _profile(tmp_path, ProfileId.MINECRAFT, "manual")
    status = UpdateService({profile.id.value: profile}).check(profile.id)
    assert status.state == "unsupported"
    assert status.available_version is None


@pytest.mark.parametrize(
    ("profile_id", "kind", "installed", "download_url", "expected"),
    [
        (ProfileId.MINECRAFT, "manual", "1.21.8", None, None),
        (ProfileId.PZ_RISING, "steamcmd_in_place", "42.13", None, None),
        (
            ProfileId.TERRARIA_TMOD,
            "release_symlink",
            "2026.05.2.0",
            "https://github.com/tModLoader/tModLoader/releases/download/v2026.05.3.0/tModLoader.zip",
            "2026.05.3.0",
        ),
        (
            ProfileId.TERRARIA_TMOD,
            "release_symlink",
            "2026.05.3.0",
            "https://github.com/tModLoader/tModLoader/releases/download/v2026.05.3.0/tModLoader.zip",
            None,
        ),
        (
            ProfileId.TERRARIA_TMOD,
            "release_symlink",
            "2026.06.0.0",
            "https://github.com/tModLoader/tModLoader/releases/download/v2026.05.3.0/tModLoader.zip",
            "2026.05.3.0",
        ),
        (
            ProfileId.TERRARIA_VANILLA,
            "release_symlink",
            "1.4.5.5",
            "https://terraria.org/api/download/pc-dedicated-server/terraria-server-1456.zip",
            None,
        ),
    ],
)
def test_update_check_compares_only_well_defined_profile_candidates(
    tmp_path: Path,
    profile_id: ProfileId,
    kind: str,
    installed: str,
    download_url: str | None,
    expected: str | None,
):
    profile = _profile(
        tmp_path,
        profile_id,
        kind,
        download_url=download_url or "https://updates.example.invalid/release",
    )
    profile.paths.version_file.write_text(installed)

    status = UpdateService({profile.id.value: profile}).check(profile.id)

    assert status.available_version == expected


@pytest.mark.parametrize(
    "candidate",
    [
        None,
        123,
        "not-a-url",
        "https://updates.example.invalid/releases/1.2.3/" + ("x" * 300),
        "https://[invalid/releases/1.2.3.zip",
    ],
)
def test_update_check_ignores_malformed_or_oversized_candidates(candidate):
    profile = SimpleNamespace(
        update=SimpleNamespace(kind="release_symlink", download_url=candidate),
    )

    assert UpdateService({})._candidate_version(profile) is None


def test_update_check_ignores_candidate_with_hostile_string_conversion():
    class HostileCandidate:
        def __str__(self):
            raise RuntimeError("hostile candidate")

    profile = SimpleNamespace(
        update=SimpleNamespace(kind="release_symlink", download_url=HostileCandidate()),
    )

    assert UpdateService({})._candidate_version(profile) is None


def test_failed_steamcmd_update_is_safe_and_recorded(tmp_path: Path):
    profile = _profile(tmp_path, ProfileId.PZ_RISING, "steamcmd_in_place")
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE updates(id TEXT,profile_id TEXT,created_at TEXT,strategy TEXT,prior_version TEXT,new_version TEXT,state TEXT)")

    def fail(_argv, **_kwargs):
        raise RuntimeError("upstream failed")

    service = UpdateService(
        {profile.id.value: profile},
        database=db,
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        runner=fail,
        stopped_check=lambda _p: True,
    )

    with pytest.raises(SafeError, match="update failed") as exc:
        service.apply(profile.id)

    assert exc.value.code == "update_failed"
    assert db.execute("SELECT state FROM updates").fetchone() == ("failed",)
    db.close()


def test_release_update_accepts_raw_payload_and_uses_clock_version(
    tmp_path: Path, ample_extraction_space
):
    profile = _profile(
        tmp_path,
        version_command=(),
        sha256=hashlib.sha256(b"raw executable payload").hexdigest(),
    )

    class Response:
        def raise_for_status(self):
            pass

        def iter_bytes(self):
            yield b"raw executable "
            yield b"payload"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    class Client:
        def stream(self, _method, _url, **_kwargs):
            return Response()

    service = UpdateService(
        {profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        http_client=Client(),
        stopped_check=lambda _p: True,
        clock=lambda: 42.0,
    )

    result = service.apply(profile.id)

    assert result.new_version == "release-42"
    assert (profile.paths.install_root / "current" / "game").read_bytes() == b"raw executable payload"


def test_release_download_requires_explicit_trusted_checksum(tmp_path: Path):
    profile = _profile(tmp_path, version_command=(), sha256=None, include_apply=False)
    calls: list[str] = []

    class Response:
        def raise_for_status(self):
            pass

        def iter_bytes(self):
            yield b"payload"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    class Client:
        def stream(self, _method, _url, **_kwargs):
            calls.append("http")
            return Response()

    service = UpdateService(
        {profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        http_client=Client(),
        stopped_check=lambda _p: True,
    )
    with pytest.raises(SafeError, match="trusted release checksum"):
        service.apply(profile.id)
    assert calls == []
    assert not (profile.paths.install_root / "current").exists()


def test_release_download_validates_checksum_before_custom_downloader(tmp_path: Path):
    profile = _profile(tmp_path, version_command=(), sha256=None, include_apply=False)
    calls: list[str] = []

    def downloader(_profile, _destination):
        calls.append("downloader")

    service = UpdateService(
        {profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        downloader=downloader,
        stopped_check=lambda _p: True,
    )
    with pytest.raises(SafeError, match="trusted release checksum"):
        service.apply(profile.id)
    assert calls == []


def test_release_download_checksum_mismatch_does_not_publish(tmp_path: Path):
    profile = _profile(
        tmp_path,
        version_command=(),
        sha256=hashlib.sha256(b"expected").hexdigest(),
    )
    releases = profile.paths.install_root / "releases"
    prior = releases / "prior"
    prior.mkdir(parents=True)
    (prior / "game").write_text("old")
    current = profile.paths.install_root / "current"
    current.symlink_to(prior, target_is_directory=True)

    class Response:
        def raise_for_status(self):
            pass

        def iter_bytes(self):
            yield b"untrusted"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    class Client:
        def stream(self, _method, _url, **_kwargs):
            return Response()

    service = UpdateService(
        {profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        http_client=Client(),
        stopped_check=lambda _p: True,
    )
    with pytest.raises(SafeError, match="checksum"):
        service.apply(profile.id)
    assert current.resolve() == prior


def test_cancelled_release_cleans_staging_and_preserves_current(tmp_path: Path):
    profile = _profile(tmp_path)
    releases = profile.paths.install_root / "releases"
    prior = releases / "prior"
    prior.mkdir(parents=True)
    (prior / "game").write_text("old")
    current = profile.paths.install_root / "current"
    current.symlink_to(prior, target_is_directory=True)

    def cancel(_profile, _staging):
        raise asyncio.CancelledError

    service = UpdateService(
        {profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        stage_release=cancel,
        stopped_check=lambda _p: True,
    )
    with pytest.raises(asyncio.CancelledError):
        service.apply(profile.id)
    assert current.resolve() == prior
    assert not list(profile.paths.install_root.glob(".release-*"))


def test_release_publication_fsync_failure_rolls_back_durably(tmp_path: Path, monkeypatch):
    profile = _profile(tmp_path, version_command=())
    releases = profile.paths.install_root / "releases"
    prior = releases / "prior"
    prior.mkdir(parents=True)
    (prior / "game").write_text("old")
    current = profile.paths.install_root / "current"
    current.symlink_to(prior, target_is_directory=True)
    calls = []
    original_fsync_dir = UpdateService._fsync_dir

    def fail_after_publication(path: Path):
        calls.append(path)
        if path == profile.paths.install_root and len(calls) == 2:
            raise OSError("publication fsync failed")
        original_fsync_dir(path)

    monkeypatch.setattr(UpdateService, "_fsync_dir", staticmethod(fail_after_publication))
    service = UpdateService(
        {profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        stage_release=lambda _p, staging: (staging / "game").write_text("new") or "new",
        stopped_check=lambda _p: True,
    )

    with pytest.raises(SafeError, match="release update failed"):
        service.apply(profile.id)

    assert current.resolve() == prior
    assert not list(profile.paths.install_root.glob(".rollback-*"))


def test_rollback_without_prior_target_fsyncs_unlink(tmp_path: Path, monkeypatch):
    profile = _profile(tmp_path, version_command=())
    current = profile.paths.install_root / "current"
    current.symlink_to(profile.paths.install_root / "releases" / "old", target_is_directory=True)
    calls = []
    monkeypatch.setattr(UpdateService, "_fsync_dir", staticmethod(lambda path: calls.append(path)))

    UpdateService._rollback_link(current, None)

    assert not current.exists()
    assert calls == [current.parent]


def test_release_tree_fsyncs_files_and_directories_bottom_up(tmp_path: Path, monkeypatch):
    root = tmp_path / "release"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (root / "top").write_bytes(b"top")
    (nested / "leaf").write_bytes(b"leaf")
    calls: list[tuple[str, Path]] = []
    monkeypatch.setattr(
        UpdateService,
        "_fsync_file",
        staticmethod(lambda path: calls.append(("file", path))),
    )
    monkeypatch.setattr(
        UpdateService,
        "_fsync_dir",
        staticmethod(lambda path: calls.append(("dir", path))),
    )

    UpdateService._fsync_tree(root)

    assert calls == [
        ("file", nested / "leaf"),
        ("file", root / "top"),
        ("dir", nested),
        ("dir", root),
    ]


def test_release_update_rejects_missing_executable_without_moving_current(tmp_path: Path):
    profile = _profile(tmp_path)
    prior = profile.paths.install_root / "releases" / "prior"
    prior.mkdir(parents=True)
    (prior / "game").write_text("old")
    current = profile.paths.install_root / "current"
    current.symlink_to(prior, target_is_directory=True)

    service = UpdateService(
        {profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        stage_release=lambda _p, _staging: "new",
        stopped_check=lambda _p: True,
    )

    with pytest.raises(SafeError, match="executable verification"):
        service.apply(profile.id)

    assert current.resolve() == prior


def test_release_update_rejects_zip_path_traversal_without_moving_current(
    tmp_path: Path, ample_extraction_space
):
    profile = _profile(tmp_path, version_command=())
    prior = profile.paths.install_root / "releases" / "prior"
    prior.mkdir(parents=True)
    (prior / "game").write_text("old")
    current = profile.paths.install_root / "current"
    current.symlink_to(prior, target_is_directory=True)
    archive_path = tmp_path / "release.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("../outside", "escape")

    class Response:
        def raise_for_status(self):
            pass

        def iter_bytes(self):
            yield archive_path.read_bytes()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    class Client:
        def stream(self, _method, _url, **_kwargs):
            return Response()

    profile = profile.model_copy(update={"update": profile.update.model_copy(update={
        "sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
    })})
    service = UpdateService(
        {profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        http_client=Client(),
        stopped_check=lambda _p: True,
    )

    with pytest.raises(SafeError, match="archive is invalid"):
        service.apply(profile.id)

    assert current.resolve() == prior
    assert not (profile.paths.install_root / "outside").exists()


def test_release_update_rejects_tar_symlink_without_moving_current(
    tmp_path: Path, ample_extraction_space
):
    profile = _profile(tmp_path, version_command=())
    prior = profile.paths.install_root / "releases" / "prior"
    prior.mkdir(parents=True)
    (prior / "game").write_text("old")
    current = profile.paths.install_root / "current"
    current.symlink_to(prior, target_is_directory=True)
    archive_path = tmp_path / "release.tar"
    with tarfile.open(archive_path, "w") as archive:
        info = tarfile.TarInfo("game")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        archive.addfile(info)

    class Response:
        def raise_for_status(self):
            pass

        def iter_bytes(self):
            yield archive_path.read_bytes()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    class Client:
        def stream(self, _method, _url, **_kwargs):
            return Response()

    profile = profile.model_copy(update={"update": profile.update.model_copy(update={
        "sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
    })})
    service = UpdateService(
        {profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        http_client=Client(),
        stopped_check=lambda _p: True,
    )

    with pytest.raises(SafeError, match="archive is invalid"):
        service.apply(profile.id)

    assert current.resolve() == prior


def test_rollback_requires_real_release_directory_and_can_switch_pointer(tmp_path: Path):
    profile = _profile(tmp_path)
    target = profile.paths.install_root / "releases" / "previous"
    target.mkdir(parents=True)
    (target / "game").write_text("old")
    current = profile.paths.install_root / "current"
    current.symlink_to(target, target_is_directory=True)
    service = UpdateService({profile.id.value: profile})

    with pytest.raises(SafeError, match="target is unavailable"):
        service.rollback(profile.id, "missing")
    with pytest.raises(SafeError, match="target is unavailable"):
        service.rollback(profile.id, "current")

    newer = profile.paths.install_root / "releases" / "newer"
    newer.mkdir()
    result = service.rollback(profile.id, "newer")

    assert result.state == "rolled_back"
    assert current.resolve() == newer


def test_release_extraction_refuses_insufficient_free_space(tmp_path: Path, monkeypatch):
    """The production guard still refuses extraction when free space is short."""
    profile = _profile(tmp_path, version_command=())
    prior = profile.paths.install_root / "releases" / "prior"
    prior.mkdir(parents=True)
    (prior / "game").write_text("old")
    current = profile.paths.install_root / "current"
    current.symlink_to(prior, target_is_directory=True)
    archive_path = tmp_path / "release.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("game", "new")

    class Response:
        def raise_for_status(self):
            pass

        def iter_bytes(self):
            yield archive_path.read_bytes()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    class Client:
        def stream(self, _method, _url, **_kwargs):
            return Response()

    profile = profile.model_copy(update={"update": profile.update.model_copy(update={
        "sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
    })})
    service = UpdateService(
        {profile.id.value: profile},
        backup_service=_Backup(tmp_path / "pre.tar.zst"),
        http_client=Client(),
        stopped_check=lambda _p: True,
    )

    # One byte below the production headroom: the guard must refuse.
    monkeypatch.setattr(
        shutil, "disk_usage",
        lambda _path: _DiskUsage(1, 0, MAX_EXTRACTED_BYTES + MAX_MEMBER_BYTES - 1),
    )
    with pytest.raises(SafeError, match="insufficient free space"):
        service.apply(profile.id)
    assert current.resolve() == prior
