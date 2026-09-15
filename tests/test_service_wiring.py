from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import asyncio
import threading
import sqlite3
import time
from contextlib import suppress
from types import SimpleNamespace

import pytest

from game_control.models import (
    AdapterKind,
    BackupDestination,
    HealthState,
    NotificationEvent,
    OperationName,
    ProfileId,
    UpdateSpec,
)
from game_control.notifications import NotificationService
from game_control.protocol import (
    CreateBackup,
    GetLogs,
    GetStatus,
    ListAudit,
    ListEvents,
    LogOptions,
    PageOptions,
    TestNotification as _NotificationAction,
)
from game_control.redaction import Redactor, SecretRegistry
from game_control.errors import SafeError
import game_control.service_wiring as wiring
from game_control.service_wiring import (
    _AuditFacade,
    _BackupFacade,
    _LogsFacade,
    _NotificationFacade,
    _UpdateFacade,
    _WorldFacade,
    _ProfilesFacade,
    _StatusFacade,
)
from game_control.status import StatusService
from game_control.state_db import StateDatabase
from game_control.telemetry_db import TelemetryDatabase
from game_control.tps import TpsSampler
from game_control.updates import UpdateService
from game_control import slotd_main


class _Adapter:
    async def aclose(self):
        return None

    async def observe(self, profile):
        return SimpleNamespace(
            running=False,
            healthy=None,
            pid=None,
            started_at=None,
            required_ports_ready=False,
        )

    async def recent_logs(self, profile, limit):
        return []


class _Registry:
    def __init__(self, profiles):
        self.profiles = tuple(profiles)

    def __iter__(self):
        return iter(self.profiles)


@pytest.mark.asyncio
async def test_status_facade_coalesces_demand_but_forces_maintenance_refresh():
    calls = []

    class Adapter:
        async def observe(self, _profile):
            calls.append("observe")
            return SimpleNamespace(running=False, healthy=None)

    service = StatusService([SimpleNamespace(id="minecraft")], adapter=Adapter())
    facade = _StatusFacade(service)
    action = GetStatus(kind="get_status")

    first, second = await asyncio.gather(
        facade.snapshot(action), facade.snapshot(action)
    )
    assert first is second
    assert calls == ["observe"]

    forced = await facade.snapshot(GetStatus(kind="get_status", refresh=True))
    assert forced is not first
    assert calls == ["observe", "observe"]

    maintenance = await facade.snapshot(action, maintenance=True)
    assert maintenance is not forced
    assert calls == ["observe", "observe", "observe"]


def _profile(profile_id: ProfileId, adapter: AdapterKind):
    return SimpleNamespace(
        id=profile_id,
        display_name=profile_id.value,
        adapter=adapter,
        process=SimpleNamespace(executable="/usr/bin/game", argv_contains=()),
        ports=(SimpleNamespace(protocol="tcp", port=25565, required=True),),
        paths=SimpleNamespace(
            data_roots=("/var/lib/game",),
            mutable_root="/var/lib/game",
            log_files=(),
            backup_root="/var/backups/game",
            install_root="/opt/game",
            version_file="/var/lib/game/version",
        ),
        operations=frozenset(OperationName),
        update=SimpleNamespace(kind="manual"),
        notification_events=frozenset(),
        health_timeout_seconds=5,
    )


class _State:
    def __init__(self):
        self.connection = SimpleNamespace()

    def close(self):
        return None


@pytest.mark.asyncio
async def test_build_controller_wires_real_typed_service_seams(monkeypatch, tmp_path):
    profiles = [
        _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY),
        _profile(ProfileId.MINECRAFT_SUNLIT_COBBLEMON, AdapterKind.SYSTEMD),
        _profile(ProfileId.PZ_RISING, AdapterKind.SYSTEMD),
        _profile(ProfileId.TERRARIA_VANILLA, AdapterKind.SYSTEMD),
        _profile(ProfileId.TERRARIA_TMOD, AdapterKind.SYSTEMD),
    ]
    registry = _Registry(profiles)
    monkeypatch.setattr(slotd_main, "ProfileRegistry", SimpleNamespace(load=lambda path: registry))
    monkeypatch.setattr(slotd_main, "StateDatabase", SimpleNamespace(open=lambda path: _State()))
    monkeypatch.setattr(slotd_main, "CraftyAdapter", lambda *args, **kwargs: _Adapter())
    systemd_arguments = []

    def systemd_adapter(*args, **kwargs):
        systemd_arguments.append((args, kwargs))
        return _Adapter()

    monkeypatch.setattr(slotd_main, "SystemdAdapter", systemd_adapter)
    monkeypatch.setattr(
        slotd_main,
        "SlotInspector",
        lambda: SimpleNamespace(observe=lambda: SimpleNamespace(owner=None, inconsistent=False)),
    )
    config = tmp_path / "controller.toml"
    config.write_text("[crafty]\nbase_url='https://127.0.0.1:8443'\ntoken_path='/dev/null'\n")

    assembly = await slotd_main.build_controller_assembly(config)
    controller = assembly.controller
    assert controller.services.status is not None
    assert controller.services.logs is not None
    assert controller.services.backups is not None
    assert controller.services.updates is not None
    assert callable(
        controller.services.updates.services[
            ProfileId.MINECRAFT_SUNLIT_COBBLEMON.value
        ].manual_checker
    )
    assert controller.services.notifications is not None
    assert controller.services.audit is not None

    snapshot = await controller.services.status.snapshot(GetStatus(kind="get_status"))
    assert len(snapshot.profiles) == len(profiles)
    assert {item.profile_id for item in snapshot.profiles} == {profile.id for profile in profiles}
    assert all(item.health is HealthState.UNKNOWN for item in snapshot.profiles)
    assert sum("rcon" in kwargs for _args, kwargs in systemd_arguments) == 1
    assert all(not args for args, _kwargs in systemd_arguments)

    checker = controller.services.status.service.health_checker[ProfileId.MINECRAFT]
    supplied = SimpleNamespace(pid=123)
    monkeypatch.setattr(wiring.MetricSampler, "sample", lambda *_args, **_kwargs: pytest.fail("resampled"))
    assert checker.process_checker(profiles[0], SimpleNamespace(pid=123), process_metrics=supplied) is True


@pytest.mark.asyncio
async def test_notification_facade_keeps_sqlite_audit_on_event_loop(tmp_path):
    import sqlite3

    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    profile.notification_events = frozenset({NotificationEvent.START})
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE audit(id TEXT PRIMARY KEY,timestamp TEXT,actor TEXT,action TEXT,"
        "profile_id TEXT,result TEXT,error_code TEXT,detail TEXT)"
    )
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    secret = secret_dir / "discord"
    secret.write_text("https://discordapp.com/api/webhooks/123/token")
    secret.chmod(0o600)
    service = NotificationService(
        {ProfileId.MINECRAFT.value: profile},
        secret_dir=secret_dir,
        database=connection,
    )
    service._post = lambda channel, value, message: None
    facade = _NotificationFacade(service)

    result = await facade.test(
        _NotificationAction(kind="test_notification", channel="discord", profile_id=ProfileId.MINECRAFT),
        actor="operator",
    )
    assert result.state == "running"
    assert connection.execute("SELECT count(*) FROM audit").fetchone()[0] == 1
    connection.close()


@pytest.mark.asyncio
async def test_notification_facade_send_keeps_sqlite_on_event_loop(tmp_path):
    import sqlite3

    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    profile.notification_events = frozenset({NotificationEvent.START})
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE notification_rules(profile_id TEXT, event TEXT, enabled INTEGER,
                                        PRIMARY KEY(profile_id,event));
        CREATE TABLE notification_deliveries(
          id TEXT PRIMARY KEY, profile_id TEXT, event TEXT, state_generation INTEGER,
          channel TEXT, delivered_at TEXT, error_code TEXT);
        """
    )
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    secret = secret_dir / "discord"
    secret.write_text("https://discordapp.com/api/webhooks/123/token")
    secret.chmod(0o600)
    service = NotificationService(
        {ProfileId.MINECRAFT.value: profile},
        secret_dir=secret_dir,
        database=connection,
    )
    main_thread = threading.get_ident()
    post_threads = []
    service._post = lambda channel, value, message: post_threads.append(threading.get_ident())
    facade = _NotificationFacade(service)

    assert await facade.send(ProfileId.MINECRAFT, NotificationEvent.START, 9, "started")
    assert post_threads and post_threads[0] != main_thread
    assert connection.execute("SELECT count(*) FROM notification_deliveries").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_backup_state_database_opens_inside_worker(monkeypatch):
    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    adapter = _Adapter()
    main_thread = threading.get_ident()
    opened_on = []

    class _FakeBackup:
        def __init__(self, profile, *, database, **kwargs):
            self.database = database
            self.free_space = lambda _path: 1
            self.clock = lambda: datetime.now(timezone.utc)
            self.tar_runner = lambda *args, **kwargs: None

        def create(self, *args, **kwargs):
            return SimpleNamespace(id="backup-worker")

        def _insert(self, record):
            raise AssertionError("isolated worker should persist directly")

    def isolated(_database):
        opened_on.append(threading.get_ident())
        return object()

    monkeypatch.setattr(wiring, "BackupService", _FakeBackup)
    monkeypatch.setattr(wiring, "_isolated_database", isolated)
    monkeypatch.setattr(wiring, "_close_database", lambda _database: None)
    facade = _BackupFacade(
        {ProfileId.MINECRAFT.value: profile},
        {ProfileId.MINECRAFT.value: adapter},
        object(),
    )
    result = await facade.create(CreateBackup(kind="create_backup", profile_id=ProfileId.MINECRAFT))
    assert result.job_id == "backup-worker"
    assert opened_on and opened_on[0] != main_thread


@pytest.mark.asyncio
async def test_sunlit_online_backup_uses_fixed_production_snapshot_window(monkeypatch):
    profile = _profile(ProfileId.MINECRAFT_SUNLIT_COBBLEMON, AdapterKind.SYSTEMD)
    captured = []

    class _Running:
        async def observe(self, _profile):
            return SimpleNamespace(running=True)

    class _FakeBackup:
        def __init__(self, profile, **kwargs):
            self.profile = profile
            self.free_space = lambda _path: 1
            self.clock = lambda: datetime.now(timezone.utc)
            self.tar_runner = lambda *args, **kwargs: None
            self.online_transport = None

        def create_online(self, *args, **kwargs):
            captured.append(kwargs)
            return SimpleNamespace(id="online-backup-worker")

    monkeypatch.setattr(wiring, "BackupService", _FakeBackup)
    monkeypatch.setattr(wiring, "_isolated_database", lambda _database: object())
    monkeypatch.setattr(wiring, "_close_database", lambda _database: None)
    facade = _BackupFacade(
        {profile.id.value: profile},
        {profile.id: _Running()},
        object(),
        sunlit_online_backup=object(),
    )

    result = await facade.create(CreateBackup(kind="create_backup", profile_id=profile.id))

    assert result.job_id == "online-backup-worker"
    assert captured == [{
        "protected": False,
        "max_snapshot_seconds": wiring.SUNLIT_ONLINE_SNAPSHOT_SECONDS,
    }]
    assert wiring.SUNLIT_ONLINE_SNAPSHOT_SECONDS == 240.0


def test_worker_database_helpers_fail_closed_and_close_initialized_worker():
    class _BrokenDatabase:
        path = "/state/game-control.db"

        @classmethod
        def open(cls, _path):
            raise OSError("database permission denied")

    assert wiring._isolated_database(_BrokenDatabase()) is None
    assert wiring._isolated_database(object()) is None

    class _Closable:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    worker = _Closable()
    wiring._close_database(worker)
    wiring._close_database(None)
    assert worker.closed


@pytest.mark.asyncio
async def test_b2_backup_fails_closed_when_worker_database_cannot_open(monkeypatch, tmp_path):
    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    profile.paths.backup_root = str(tmp_path)
    facade = _BackupFacade(
        {profile.id.value: profile},
        {profile.id: _Adapter()},
        object(),
    )
    monkeypatch.setattr(wiring, "_isolated_database", lambda _database: None)
    action = CreateBackup(
        kind="create_backup",
        profile_id=ProfileId.MINECRAFT,
        destination=BackupDestination.HORIZON_B2,
    )
    with pytest.raises(SafeError, match="durable backup state") as error:
        await facade.create(action)
    assert error.value.code == "backup_protection_failed"
    assert not list(Path(profile.paths.backup_root).glob("*.tar.zst"))


@pytest.mark.asyncio
async def test_logs_facade_rejects_unknown_profile_before_adapter_use():
    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    facade = _LogsFacade(
        {profile.id.value: profile},
        {profile.id: _Adapter()},
        Redactor(SecretRegistry()),
    )

    action = GetLogs(
        kind="get_logs",
        profile_id=ProfileId.PZ_RISING,
        page=LogOptions(),
    )
    with pytest.raises(SafeError, match="profile was not found") as error:
        await facade.page(action)
    assert error.value.code == "profile_not_found"


@pytest.mark.asyncio
async def test_backup_facade_fails_closed_when_adapter_is_missing_or_unreadable():
    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    action = CreateBackup(kind="create_backup", profile_id=ProfileId.MINECRAFT)

    missing = _BackupFacade({profile.id.value: profile}, {}, object())
    with pytest.raises(SafeError, match="state could not be proven") as error:
        await missing.create(action)
    assert error.value.code == "profile_unavailable"

    class _Unreadable:
        async def observe(self, _profile):
            raise OSError("adapter unavailable")

    unreadable = _BackupFacade({profile.id.value: profile}, {profile.id: _Unreadable()}, object())
    with pytest.raises(SafeError) as error:
        await unreadable.create(action)
    assert error.value.code == "profile_unavailable"

    class _Running:
        async def observe(self, _profile):
            return SimpleNamespace(running=True)

    running = _BackupFacade({profile.id.value: profile}, {profile.id: _Running()}, object())
    with pytest.raises(SafeError) as error:
        await running.create(action)
    assert error.value.code == "profile_running"


@pytest.mark.asyncio
async def test_backup_worker_database_is_closed_when_create_fails(monkeypatch):
    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    closed = []

    class _WorkerDatabase:
        def close(self):
            closed.append(self)

    class _FakeBackup:
        def __init__(self, *_args, **kwargs):
            self.database = kwargs.get("database")
            self.free_space = lambda _path: 1
            self.clock = lambda: datetime.now(timezone.utc)
            self.tar_runner = lambda *args, **kwargs: None

        def create(self, *args, **kwargs):
            raise RuntimeError("worker failed after initialization")

    monkeypatch.setattr(wiring, "BackupService", _FakeBackup)
    monkeypatch.setattr(wiring, "_isolated_database", lambda _database: _WorkerDatabase())
    facade = _BackupFacade(
        {profile.id.value: profile},
        {profile.id: _Adapter()},
        object(),
    )

    with pytest.raises(RuntimeError, match="worker failed"):
        await facade.create(CreateBackup(kind="create_backup", profile_id=ProfileId.MINECRAFT))
    assert len(closed) == 1


@pytest.mark.asyncio
async def test_backup_restore_rejects_unapproved_archive_name():
    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    facade = _BackupFacade(
        {profile.id.value: profile},
        {profile.id: _Adapter()},
        object(),
    )

    for backup_id in ("", "../escape", "nested/name", "\\windows", ".", ".."):
        with pytest.raises(SafeError, match="archive is not approved") as error:
            await facade.confirm_restore(
                SimpleNamespace(profile_id=ProfileId.MINECRAFT),
                payload={"backup_id": backup_id},
            )
        assert error.value.code == "invalid_backup"


@pytest.mark.asyncio
async def test_confirm_restore_uses_prepared_payload_profile_without_action_profile_id():
    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    facade = _BackupFacade(
        {profile.id.value: profile},
        {profile.id: _Adapter()},
        object(),
    )

    with pytest.raises(SafeError, match="archive is not approved") as error:
        await facade.confirm_restore(
            SimpleNamespace(),
            payload={"profile_id": ProfileId.MINECRAFT.value, "backup_id": ""},
        )
    assert error.value.code == "invalid_backup"


@pytest.mark.asyncio
async def test_confirm_restore_passes_lease_to_pre_restore_backup(monkeypatch, tmp_path):
    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    profile.paths.backup_root = tmp_path
    captured = []

    class FakeBackup:
        def __init__(self, *_args, **kwargs):
            captured.append(kwargs)
            self.database = kwargs.get("database")
            self.free_space = lambda _path: 1
            self.clock = lambda: datetime.now(timezone.utc)
            self.tar_runner = lambda *args, **kwargs: None

    class FakeRestore:
        health_check = None
        free_space = staticmethod(lambda _path: 1)

        def __init__(self, *_args, **kwargs):
            self.captured = kwargs

        def restore(self, *_args, **_kwargs):
            return type("Result", (), {
                "destinations": (tmp_path,), "destination": tmp_path, "backup_id": "backup",
            })()

        def finalize(self, _result):
            return None

    monkeypatch.setattr(wiring, "BackupService", FakeBackup)
    monkeypatch.setattr(wiring, "RestoreService", FakeRestore)
    facade = _BackupFacade({profile.id.value: profile}, {profile.id: _Adapter()}, object())
    lease_check = lambda: True
    await facade.confirm_restore(
        SimpleNamespace(profile_id=profile.id),
        payload={"backup_id": "backup"},
        lease_check=lease_check,
    )
    assert captured[-1]["lease_check"] is lease_check


@pytest.mark.asyncio
async def test_confirm_restore_lease_loss_blocks_pre_restore_backup_publication(monkeypatch, tmp_path):
    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    profile.paths.backup_root = tmp_path
    published = []

    class FakeBackup:
        def __init__(self, *_args, **kwargs):
            self.database = kwargs.get("database")
            self.free_space = lambda _path: 1
            self.clock = lambda: datetime.now(timezone.utc)
            self.tar_runner = lambda *args, **kwargs: None
            self.lease_check = kwargs.get("lease_check")

        def create(self, **kwargs):
            if not self.lease_check():
                raise SafeError("slot_conflict", "operation lease was lost before publication")
            published.append("archive")

    class FakeRestore:
        health_check = None
        free_space = staticmethod(lambda _path: 1)

        def __init__(self, _profile, *, backup_service, **_kwargs):
            self.backup_service = backup_service

        def restore(self, *_args, **_kwargs):
            self.backup_service.create(protected=True)
            published.append("restore")

        def finalize(self, _result):
            published.append("finalize")

    monkeypatch.setattr(wiring, "BackupService", FakeBackup)
    monkeypatch.setattr(wiring, "RestoreService", FakeRestore)
    facade = _BackupFacade({profile.id.value: profile}, {profile.id: _Adapter()}, object())
    with pytest.raises(SafeError, match="lease was lost"):
        await facade.confirm_restore(
            SimpleNamespace(profile_id=profile.id),
            payload={"backup_id": "backup"},
            lease_check=lambda: False,
        )
    assert published == []


@pytest.mark.asyncio
async def test_world_facade_rejects_missing_or_running_terraria_profile():
    vanilla = _profile(ProfileId.TERRARIA_VANILLA, AdapterKind.SYSTEMD)
    tmod = _profile(ProfileId.TERRARIA_TMOD, AdapterKind.SYSTEMD)
    service = SimpleNamespace()

    missing = _WorldFacade(service, {vanilla.id.value: vanilla}, {vanilla.id: _Adapter()})
    with pytest.raises(SafeError) as error:
        await missing.confirm_clone(SimpleNamespace(), payload={})
    assert error.value.code == "profile_unavailable"

    class _Running:
        async def observe(self, _profile):
            return SimpleNamespace(running=True)

    running = _WorldFacade(
        service,
        {vanilla.id.value: vanilla, tmod.id.value: tmod},
        {vanilla.id: _Adapter(), tmod.id: _Running()},
    )
    with pytest.raises(SafeError) as error:
        await running.confirm_clone(SimpleNamespace(), payload={})
    assert error.value.code == "profile_running"


@pytest.mark.asyncio
async def test_update_facade_rejects_unavailable_adapter_and_unknown_check():
    profile = _profile(ProfileId.PZ_RISING, AdapterKind.SYSTEMD)
    facade = _UpdateFacade({}, {profile.id.value: profile}, {})

    with pytest.raises(SafeError) as error:
        await facade._stopped(profile)
    assert error.value.code == "profile_unavailable"

    with pytest.raises(SafeError) as error:
        await facade.check(SimpleNamespace(profile_id=ProfileId.PZ_RISING))
    assert error.value.code == "profile_not_found"


@pytest.mark.asyncio
async def test_update_facade_keeps_blocking_check_off_the_event_loop():
    profile = _profile(ProfileId.MINECRAFT_SUNLIT_COBBLEMON, AdapterKind.SYSTEMD)

    class Service:
        def check(self, _profile):
            time.sleep(0.1)
            return SimpleNamespace(available_version="1.1.4-SSV4.1.5")

    facade = _UpdateFacade(
        {profile.id.value: Service()},
        {profile.id.value: profile},
        {},
    )
    heartbeats = 0

    async def heartbeat():
        nonlocal heartbeats
        for _ in range(10):
            await asyncio.sleep(0.01)
            heartbeats += 1

    task = asyncio.create_task(facade.check(SimpleNamespace(profile_id=profile.id)))
    await heartbeat()
    result = await task

    assert heartbeats == 10
    assert result.available_version == "1.1.4-SSV4.1.5"


def test_release_digest_flows_through_profile_into_update_service_without_public_leakage():
    digest = "b" * 64
    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    profile.public_endpoint = None
    profile.update = UpdateSpec(
        kind="release_symlink",
        download_url="https://example.test/game.tar.gz",
        executable_relative_path="bin/game",
        sha256=digest,
    )
    service = UpdateService({profile.id.value: profile})

    assert service._trusted_checksum(profile) == digest
    public = _ProfilesFacade({profile.id.value: profile}).public_profiles()[0]
    assert "sha256" not in public.model_dump()


def test_legacy_telemetry_config_keeps_no_tick_when_no_approved_profile_exists():
    config = wiring._legacy_telemetry_config(
        {
            "exporter_url": "http://127.0.0.1:19565/metrics",
            "legacy_tps_mode": "disabled",
        },
        ("other",),
    )
    assert config.exporters == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode, exporter, legacy_rows, modern_exporters, reason",
    [
        ("disabled", False, False, False, None),
        ("disabled", True, False, True, None),
        ("enabled", False, True, False, None),
        ("enabled", True, True, False, "legacy_tps_override"),
    ],
)
async def test_legacy_exporter_matrix_has_one_store_and_explicit_task_policy(
    mode, exporter, legacy_rows, modern_exporters, reason
):
    stats = {"legacy_tps_mode": mode}
    if exporter:
        stats["exporter_url"] = "http://127.0.0.1:19565/metrics"
    config = wiring._legacy_telemetry_config(
        stats, ("minecraft", "minecraft-sunlit-cobblemon")
    )
    assert bool(config.exporters) is modern_exporters
    assert config.resource_interval_seconds == 5.0
    task_names = {"horizon-telemetry-supervisor"}
    if legacy_rows:
        task_names.add("horizon-legacy-tps-supervisor")
    assert ("horizon-legacy-tps-supervisor" in task_names) is legacy_rows
    health = wiring._StatusFacade(
        SimpleNamespace(telemetry_health=lambda: {"ok": True}),
        telemetry_health_reason=reason,
    ).telemetry_health()
    assert health.get("reason") == reason
    if not legacy_rows:
        return

    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def raise_for_status(self):
            return None

        async def aiter_bytes(self):
            yield b"mc_tick_seconds{quantile=\"0.5\"} 0.05\n"

    class Client:
        def stream(self, *_args, **_kwargs):
            return Response()

        async def aclose(self):
            return None

    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE metric_samples(profile_id TEXT, metric TEXT, ts TEXT, value REAL)")
    sampler = TpsSampler(
        connection,
        client=Client(),
        clock=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert await sampler.run_once()
    rows = connection.execute("SELECT profile_id, metric, ts FROM metric_samples").fetchall()
    assert len(rows) == 2
    assert {row[0] for row in rows} == {"minecraft"}
    assert len({row[2] for row in rows}) == 1
    await sampler.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode, exporter, has_legacy, has_modern_tick",
    [("disabled", False, False, False), ("disabled", True, False, True),
     ("enabled", False, True, False), ("enabled", True, True, False)],
)
async def test_actual_composition_matrix_runs_tasks_and_isolated_sqlite(
    monkeypatch, tmp_path, mode, exporter, has_legacy, has_modern_tick
):
    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def raise_for_status(self):
            return None

        async def aiter_bytes(self):
            yield b"mc_tick_seconds{quantile=\"0.5\"} 0.05\n"

    class Client:
        def stream(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr("game_control.state_db.STATE_DB_PATH", tmp_path / "state.db")
    state_db = StateDatabase.open(tmp_path / "state.db")
    connection = state_db.connection
    profiles = tuple(_profile(profile_id, AdapterKind.SYSTEMD) for profile_id in (
        ProfileId.MINECRAFT, ProfileId.MINECRAFT_SUNLIT_COBBLEMON,
    ))
    adapters = {profile.id: _Adapter() for profile in profiles}
    stats = {"legacy_tps_mode": mode}
    if exporter:
        stats["exporter_url"] = "http://127.0.0.1:19565/metrics"
    writer = TelemetryDatabase.open(tmp_path / "telemetry.db")
    services = wiring.build_service_seams(
        profiles, adapters, state_db,
        SimpleNamespace(observe=lambda: SimpleNamespace(owner=None, inconsistent=False)),
        stats_config=stats, telemetry_db=writer,
    )
    collector = services.telemetry_collectors
    collector._monotonic = lambda: 100.0
    collector._wall_clock_ms = lambda: 123456
    collector._fetch_exporter = lambda _url: _async_text(
        "mc_tick_seconds{quantile=\"0.5\"} 0.05\n"
    )
    snapshot = SimpleNamespace(profiles=tuple(
        SimpleNamespace(profile_id=profile.id, state="running", pid=None, rss_bytes=None)
        for profile in profiles
    ))
    await services.telemetry_runtime.cycle(snapshot)
    if has_legacy:
        legacy = services.tps_sampler
        await legacy._client.aclose()
        legacy._client = Client()
        legacy._owns_client = False
        assert await legacy.run_once()
    assert writer.drain(2.0)
    modern_tick_rows = writer.connection.execute(
        "SELECT s.ts_ms, r.metric FROM telemetry_samples s "
        "JOIN telemetry_series r USING(series_id) WHERE r.metric LIKE 'mspt_p%'"
    ).fetchall()
    assert bool(modern_tick_rows) is has_modern_tick
    if modern_tick_rows:
        assert len({row[0] for row in modern_tick_rows}) == 1
    legacy_rows = connection.execute("SELECT profile_id, metric, ts FROM metric_samples").fetchall()
    assert len(legacy_rows) == (2 if has_legacy else 0)
    if legacy_rows:
        assert len({row[2] for row in legacy_rows}) == 1
    assert services.telemetry_runtime.sampler.interval_seconds == 5.0
    assert services.status.telemetry_health().get("reason") == (
        "legacy_tps_override" if mode == "enabled" and exporter else None
    )

    built_services, built_profiles, built_adapters = services, profiles, adapters
    class Controller:
        services = built_services
        profiles = built_profiles
        adapters = built_adapters
        slot_inspector = SimpleNamespace(observe=lambda: SimpleNamespace(owner=None))
        performance = SimpleNamespace(record_event_loop_lag=lambda _value: None)

        async def reconcile_startup(self):
            return None

        async def maintenance_tick(self):
            await asyncio.sleep(3600)

    class Assembly:
        controller = Controller()

        async def aclose(self):
            return None

    class Server:
        def __init__(self, _controller):
            self._server = self

        async def start(self):
            return None

        async def serve_forever(self):
            return None

        async def close(self):
            return None

    names = []
    original_create_task = asyncio.create_task

    def create_task(coro, *args, **kwargs):
        names.append(kwargs.get("name"))
        return original_create_task(coro, *args, **kwargs)

    async def build():
        return Assembly()

    monkeypatch.setattr(slotd_main, "build_controller_assembly", build)
    monkeypatch.setattr(slotd_main, "UnixRpcServer", Server)
    monkeypatch.setattr(slotd_main.asyncio, "create_task", create_task)
    with pytest.raises(RuntimeError, match="RPC server task exited unexpectedly"):
        await slotd_main.serve()
    assert "horizon-telemetry-supervisor" in names
    assert ("horizon-legacy-tps-supervisor" in names) is has_legacy
    with suppress(asyncio.CancelledError):
        await services.telemetry_runtime.close()
    await services.alerts.close()
    await services.audit.history.aclose()
    for update in services.updates.services.values():
        await update.aclose()
    writer.drain(2.0)
    writer.close()
    state_db.close()


async def _async_text(value):
    return value


@pytest.mark.asyncio
async def test_legacy_collector_ownership_bridge_closes_only_explicit_rcon():
    events = []

    class Database:
        def __init__(self):
            self.drains = 0
            self.closes = 0

        def drain(self, _timeout=None):
            events.append("drain")
            self.drains += 1
            return True

        def close(self):
            self.closes += 1

    class Rcon:
        profile_id = "minecraft"

        def __init__(self):
            self.closes = 0

        async def close(self):
            events.append("rconclose")
            self.closes += 1

    database, rcon = Database(), Rcon()
    collector = wiring._BoundTelemetryCollectors(
        profiles=(), database=database, stats={},
        rcon=wiring.ResourceRef.owned(rcon), player_tracker=SimpleNamespace(),
    )
    await collector.close()
    assert events == ["drain", "rconclose"]
    assert rcon.closes == 1
    assert database.drains == 1
    assert database.closes == 0


@pytest.mark.asyncio
async def test_injected_telemetry_and_rcon_refs_remain_borrowed_after_runtime_close():
    from game_control.runtime.telemetry import (
        ResourceRef, TelemetryCollector, TelemetryRuntime, TelemetryRuntimeConfig,
    )

    class Database:
        def __init__(self):
            self.drains = 0
            self.closed = False

        def drain(self, _timeout=None):
            self.drains += 1
            return True

        def close(self):
            self.closed = True

        def enqueue_sample(self, *_args, **_kwargs):
            return True

    class Rcon:
        profile_id = "minecraft"
        closed = False

        async def close(self):
            self.closed = True

        async def execute(self, _command):
            raise RuntimeError("not used")

    database, rcon = Database(), Rcon()
    collector = TelemetryCollector(
        profiles=(), config=TelemetryRuntimeConfig(),
        database=ResourceRef.borrowed(database), rcon=ResourceRef.borrowed(rcon),
        player_tracker=SimpleNamespace(),
    )
    runtime = TelemetryRuntime(
        SimpleNamespace(snapshot=lambda **_kwargs: SimpleNamespace(profiles=())), collector,
        database=ResourceRef.borrowed(database), rcon=ResourceRef.borrowed(rcon),
        sampler=ResourceRef.borrowed(SimpleNamespace()),
    )
    await runtime.close()
    assert database.drains == 1
    assert database.closed is False
    assert rcon.closed is False
    assert database.enqueue_sample("minecraft", "players", 1, ts_ms=1)


@pytest.mark.asyncio
async def test_legacy_collector_drain_failure_continues_rcon_and_fails_closed():
    events = []

    class Database:
        def drain(self, _timeout=None):
            events.append("drain")
            return False

    class Rcon:
        profile_id = "minecraft"

        async def close(self):
            events.append("rconclose")

    collector = wiring._BoundTelemetryCollectors(
        profiles=(), database=Database(), stats={},
        rcon=wiring.ResourceRef.owned(Rcon()), player_tracker=SimpleNamespace(),
    )
    with pytest.raises(RuntimeError, match="drain failed"):
        await collector.close()
    assert events == ["drain", "rconclose"]

    events.clear()
    collector = wiring._BoundTelemetryCollectors(
        profiles=(), database=SimpleNamespace(), stats={},
        rcon=wiring.ResourceRef.owned(Rcon()), player_tracker=SimpleNamespace(),
    )
    with pytest.raises(RuntimeError, match="drain is unavailable"):
        await collector.close()
    assert events == ["rconclose"]

    class NoCloseRcon:
        profile_id = "minecraft"

    collector = wiring._BoundTelemetryCollectors(
        profiles=(), database=None, stats={},
        rcon=wiring.ResourceRef.owned(NoCloseRcon()), player_tracker=SimpleNamespace(),
    )
    with pytest.raises(RuntimeError, match="RCON close is unavailable"):
        await collector.close()


@pytest.mark.asyncio
async def test_legacy_collector_drain_cancellation_is_drained_before_rcon_close():
    started = asyncio.Event()
    release = threading.Event()
    events = []

    class Database:
        def drain(self, _timeout=None):
            started.set()
            release.wait()
            events.append("drain")
            return True

    class Rcon:
        profile_id = "minecraft"

        async def close(self):
            events.append("rconclose")

    collector = wiring._BoundTelemetryCollectors(
        profiles=(), database=Database(), stats={},
        rcon=wiring.ResourceRef.owned(Rcon()), player_tracker=SimpleNamespace(),
    )
    closing = asyncio.create_task(collector.close())
    await started.wait()
    closing.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert events == ["drain", "rconclose"]


@pytest.mark.asyncio
async def test_rcon_collector_uses_one_performance_request_for_tps_and_mspt():
    calls = []
    writes = []

    class Rcon:
        profile_id = "minecraft-sunlit-cobblemon"

        async def execute(self, command):
            calls.append(command)
            if command is wiring.TelemetryCommand.PLAYER_COUNT:
                return wiring.PlayerCountResult(2, 20)
            return wiring.PerformanceResult(tps=19.9, mspt=12.5)

    database = SimpleNamespace(enqueue_sample=lambda *args, **kwargs: writes.append((args, kwargs)) or True)
    collector = wiring._BoundTelemetryCollectors(
        profiles=(), database=database, stats={}, rcon=Rcon(), player_tracker=SimpleNamespace()
    )
    await collector._collect_rcon()
    assert calls == [wiring.TelemetryCommand.PLAYER_COUNT, wiring.TelemetryCommand.PERFORMANCE]
    assert [(args[1], args[2]) for args, _ in writes] == [("players", 2), ("tps", 19.9), ("mspt", 12.5)]
    assert len({kwargs["ts_ms"] for _, kwargs in writes}) == 1


@pytest.mark.asyncio
async def test_tick_inactive_emits_expected_percentiles_and_known_histogram_without_zeroes():
    writes = []
    database = SimpleNamespace(enqueue_sample=lambda *args, **kwargs: writes.append((args, kwargs)) or True)
    collector = wiring._BoundTelemetryCollectors(
        profiles=(), database=database, stats={}, rcon=None, player_tracker=SimpleNamespace()
    )
    collector._tick_buckets = (5.0, float("inf"))
    await collector._collect_tick("minecraft-sunlit-cobblemon", False)
    assert {args[1] for args, _ in writes} == {"mspt", "mspt_p50", "mspt_p95", "mspt_p99", "tick_ms_bucket"}
    assert all(args[2] is None and kwargs["state"] == "inactive" for args, kwargs in writes)


@pytest.mark.asyncio
async def test_host_collector_emits_inactive_and_failure_states_without_fabricated_zeroes():
    writes = []
    database = SimpleNamespace(enqueue_sample=lambda *args, **kwargs: writes.append((args, kwargs)) or True)
    profile = SimpleNamespace(id="minecraft-sunlit-cobblemon", systemd_unit="minecraft.service")
    collector = wiring._BoundTelemetryCollectors(
        profiles=(profile,), database=database, stats={}, rcon=None, player_tracker=SimpleNamespace()
    )
    collector._tick_profile = "minecraft-sunlit-cobblemon"
    await collector.collect(running={"minecraft-sunlit-cobblemon": False})
    inactive = [(args, kwargs) for args, kwargs in writes if args[1] in wiring._HOST_TELEMETRY_METRICS]
    assert len(inactive) == len(wiring._HOST_TELEMETRY_METRICS)
    assert all(args[2] is None and kwargs["state"] == "inactive" for args, kwargs in inactive)

    writes.clear()
    collector._host.collect = lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("unavailable"))
    await collector.collect(running={"minecraft-sunlit-cobblemon": True})
    unavailable = [(args, kwargs) for args, kwargs in writes if args[1] in wiring._HOST_TELEMETRY_METRICS]
    assert len(unavailable) == len(wiring._HOST_TELEMETRY_METRICS)
    assert all(args[2] is None and kwargs["state"] == "unavailable" for args, kwargs in unavailable)


@pytest.mark.asyncio
async def test_notification_facade_audits_delivery_failure(tmp_path):
    import sqlite3

    profile = _profile(ProfileId.MINECRAFT, AdapterKind.CRAFTY)
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE audit(id TEXT PRIMARY KEY,timestamp TEXT,actor TEXT,action TEXT,"
        "profile_id TEXT,result TEXT,error_code TEXT,detail TEXT)"
    )
    service = NotificationService(
        {ProfileId.MINECRAFT.value: profile},
        secret_dir=tmp_path,
        database=connection,
    )

    def fail_secret(_channel):
        raise SafeError("secret_unavailable", "notification secret is unavailable")

    service._secret = fail_secret
    facade = _NotificationFacade(service)
    with pytest.raises(SafeError) as error:
        await facade.test(
            _NotificationAction(kind="test_notification", channel="discord", profile_id=ProfileId.MINECRAFT),
            actor="operator",
        )
    assert error.value.code == "secret_unavailable"
    assert connection.execute("SELECT result,error_code FROM audit").fetchone() == (
        "failed",
        "secret_unavailable",
    )
    connection.close()


@pytest.mark.asyncio
async def test_history_pages_use_keyset_cursor_and_bounded_worker(tmp_path):
    """A deep history page stays index-backed and does not run on the loop."""
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    query_threads = []
    connection.set_trace_callback(lambda _sql: query_threads.append(threading.get_ident()))
    connection.executescript(
        """
        CREATE TABLE events(id TEXT PRIMARY KEY, timestamp TEXT NOT NULL,
                            profile_id TEXT, code TEXT NOT NULL, message TEXT NOT NULL);
        CREATE INDEX idx_events_history_cursor ON events(timestamp DESC, id DESC);
        CREATE TABLE audit(id TEXT PRIMARY KEY, timestamp TEXT NOT NULL,
                           actor TEXT NOT NULL, action TEXT NOT NULL,
                           profile_id TEXT, result TEXT NOT NULL,
                           error_code TEXT, detail TEXT NOT NULL);
        CREATE INDEX idx_audit_history_cursor ON audit(timestamp DESC, id DESC);
        """
    )
    connection.executemany(
        "INSERT INTO events VALUES (?, ?, NULL, 'tick', 'message')",
        [(f"event-{i:06d}", f"2026-08-29T00:{i // 60:02d}:{i % 60:02d}Z") for i in range(20_000)],
    )
    connection.commit()
    facade = _AuditFacade(connection)
    query_threads.clear()

    first = await facade.list_events(ListEvents(kind="list_events", page=PageOptions(limit=50)))
    assert first.next_cursor
    second = await facade.list_events(
        ListEvents(kind="list_events", page=PageOptions(limit=50, cursor=first.next_cursor))
    )
    assert second.items[0].id != first.items[-1].id
    assert not ({item.id for item in first.items} & {item.id for item in second.items})
    # Raw in-memory connections are an explicit injected seam; keep them on
    # their owning thread. Production file-backed state uses the worker below.
    assert query_threads and all(thread_id == threading.get_ident() for thread_id in query_threads)
    # Walk a representative deep page (15k rows) without OFFSET work.
    cursor = second.next_cursor
    for _ in range(29):
        deep = await facade.list_events(
            ListEvents(kind="list_events", page=PageOptions(limit=500, cursor=cursor))
        )
        assert deep.items
        cursor = deep.next_cursor
    assert deep.items[0].id.startswith("event-")
    plan = connection.execute(
        "EXPLAIN QUERY PLAN SELECT id,timestamp,profile_id,code,message FROM events "
        "WHERE (timestamp,id) < (?,?) "
        "ORDER BY timestamp DESC,id DESC LIMIT ?",
        ("2026-08-29T00:10:00Z", "event-000000", 50),
    ).fetchall()
    assert any("SEARCH events USING INDEX idx_events_history_cursor" in str(row[-1]) for row in plan)
    audit_plan = connection.execute(
        "EXPLAIN QUERY PLAN SELECT id,timestamp,actor,action,profile_id,result,error_code,detail "
        "FROM audit WHERE (timestamp,id) < (?,?) ORDER BY timestamp DESC,id DESC LIMIT ?",
        ("2026-08-29T00:10:00Z", "audit-000000", 50),
    ).fetchall()
    assert any("SEARCH audit USING INDEX idx_audit_history_cursor" in str(row[-1]) for row in audit_plan)
    connection.close()


@pytest.mark.asyncio
async def test_history_file_queries_are_read_only_and_never_fallback_to_writer(tmp_path, monkeypatch):
    db_path = tmp_path / "state.db"
    writer = sqlite3.connect(db_path)
    writer.executescript(
        """
        CREATE TABLE events(id TEXT PRIMARY KEY, timestamp TEXT NOT NULL,
                            profile_id TEXT, code TEXT NOT NULL, message TEXT NOT NULL);
        CREATE INDEX idx_events_history_cursor ON events(timestamp DESC, id DESC);
        INSERT INTO events VALUES ('event-1', '2026-08-29T00:00:00Z', NULL, 'tick', 'message');
        """
    )
    writer.commit()
    writer_queries = []
    writer.set_trace_callback(lambda sql: writer_queries.append(sql))

    class _WriterDatabase:
        path = db_path
        connection = writer

        @classmethod
        def open(cls, _path):
            raise AssertionError("history query must not open writable StateDatabase")

    monkeypatch.setattr(wiring, "_AUDIT_STATE_DB_PATH", db_path)
    page = await _AuditFacade(_WriterDatabase()).list_events(
        ListEvents(kind="list_events", page=PageOptions(limit=10))
    )
    assert [item.id for item in page.items] == ["event-1"]
    assert writer_queries == []
    writer.close()


@pytest.mark.asyncio
async def test_history_query_opener_failure_is_typed_and_fail_closed(tmp_path):
    writer = sqlite3.connect(":memory:", check_same_thread=False)
    writer_queries = []
    writer.set_trace_callback(lambda sql: writer_queries.append(sql))

    class _UnavailableDatabase:
        path = tmp_path / "missing-state.db"
        connection = writer

    with pytest.raises(SafeError) as error:
        await _AuditFacade(_UnavailableDatabase()).list_events(
            ListEvents(kind="list_events", page=PageOptions(limit=10))
        )
    assert error.value.code == "state_unavailable"
    assert writer_queries == []
    writer.close()


@pytest.mark.asyncio
async def test_audit_facade_fails_closed_without_state_database():
    facade = _AuditFacade(object())
    with pytest.raises(SafeError, match="operational state is unavailable") as error:
        await facade.list_events(ListEvents(kind="list_events", page=PageOptions()))
    assert error.value.code == "state_unavailable"
    with pytest.raises(SafeError) as error:
        await facade.list_audit(ListAudit(kind="list_audit", page=PageOptions()))
    assert error.value.code == "state_unavailable"
