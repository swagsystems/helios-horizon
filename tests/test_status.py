from pathlib import Path
from types import SimpleNamespace
import asyncio
import sqlite3
import threading
import time

import pytest

from game_control.models import ObservedState
from game_control.capability_evidence import WakeSafetyEvidence
from game_control.status import StatusService, derive_state


class _Reservation:
    def __init__(
        self,
        profile_id="minecraft",
        operation_id="update-1",
        operation_kind="update",
        expires_at=1_800_000_000.0,
    ):
        self.profile_id = profile_id
        self.operation_id = operation_id
        self.operation_kind = operation_kind
        self.expires_at = expires_at


@pytest.mark.asyncio
async def test_status_projects_update_activity_from_live_reservation():
    service = StatusService(
        [SimpleNamespace(id="minecraft")],
        active_jobs={},
        update_reservations=lambda profile_id: _Reservation() if str(profile_id) == "minecraft" else None,
    )
    snapshot = await service.cached_snapshot()
    (status,) = snapshot.profiles
    assert status.update is not None
    assert status.update.source == "reservation"
    assert status.update.operation_id == "update-1"
    assert status.update.expires_at is not None
    assert status.state is ObservedState.STOPPED


@pytest.mark.asyncio
async def test_status_projects_update_activity_from_update_job_only():
    service = StatusService(
        [SimpleNamespace(id="minecraft"), SimpleNamespace(id="pz-rising")],
        active_jobs={"minecraft": "update", "pz-rising": "backup"},
    )
    snapshot = await service.cached_snapshot()
    statuses = {status.profile_id: status for status in snapshot.profiles}
    assert statuses["minecraft"].update is not None
    assert statuses["minecraft"].update.source == "job"
    assert statuses["minecraft"].update.operation_id is None
    # A generic backup job is not an update.
    assert statuses["pz-rising"].update is None


@pytest.mark.asyncio
async def test_status_update_activity_clears_when_records_disappear():
    reservation = {"value": _Reservation()}
    service = StatusService(
        [SimpleNamespace(id="minecraft")],
        active_jobs={},
        update_reservations=lambda profile_id: reservation["value"],
    )
    first = await service.cached_snapshot()
    assert first.profiles[0].update is not None
    # A released or expired reservation must not permanently gray the profile.
    reservation["value"] = None
    second = await service.cached_snapshot()
    assert second.profiles[0].update is None


@pytest.mark.asyncio
async def test_initializing_projection_survives_unreadable_job_provider():
    def explode(*_args, **_kwargs):
        raise RuntimeError("jobs table is unavailable")

    service = StatusService(
        [SimpleNamespace(id="minecraft")],
        active_jobs=explode,
        update_reservations=lambda _profile_id: (_ for _ in ()).throw(RuntimeError("reservation unreadable")),
    )
    # The cold/initializing projection must stay a bounded status payload
    # instead of failing the request; unknown evidence never becomes "update".
    snapshot = await service.cached_snapshot()
    (status,) = snapshot.profiles
    assert status.state in set(ObservedState)
    assert status.active_job_id is None
    assert status.update is None


def test_typed_telemetry_health_provider_precedes_legacy_storage():
    service = StatusService(
        [],
        telemetry_db=SimpleNamespace(health=lambda: {"ok": False}),
        telemetry_health_provider=lambda: {"ok": True, "source": "runtime"},
    )
    assert service.telemetry_health() == {"ok": True, "source": "runtime"}


@pytest.mark.asyncio
async def test_cached_snapshot_stays_responsive_while_sync_maintenance_probe_blocks():
    entered = threading.Event()
    release = threading.Event()

    def blocking_slot():
        entered.set()
        release.wait(2)
        return SimpleNamespace(owner=None)

    service = StatusService([SimpleNamespace(id="minecraft")], slot_observer=blocking_slot)
    maintenance = asyncio.create_task(service.snapshot())
    assert await asyncio.to_thread(entered.wait, 1)
    started = time.monotonic()
    cached = await asyncio.wait_for(service.cached_snapshot(), timeout=0.2)
    elapsed = time.monotonic() - started
    assert cached.profiles[0].state is ObservedState.STOPPED
    assert elapsed < 0.15
    release.set()
    await maintenance


@pytest.mark.asyncio
async def test_sync_external_stage_returning_awaitable_is_awaited_off_loop():
    entered = threading.Event()
    release = asyncio.Event()

    async def delayed_slot_result():
        await release.wait()
        return SimpleNamespace(owner=None)

    def sync_slot():
        entered.set()
        return delayed_slot_result()

    service = StatusService([SimpleNamespace(id="minecraft")], slot_observer=sync_slot)
    task = asyncio.create_task(service.snapshot())
    assert await asyncio.to_thread(entered.wait, 1)
    await asyncio.wait_for(service.cached_snapshot(), timeout=0.2)
    release.set()
    await task


@pytest.mark.asyncio
async def test_cancelled_blocking_probe_releases_single_flight_after_thread_completion():
    entered = threading.Event()
    release = threading.Event()
    completed = threading.Event()
    calls = 0
    active = 0
    max_active = 0

    def blocking_slot():
        nonlocal calls, active, max_active
        calls += 1
        active += 1
        max_active = max(max_active, active)
        entered.set()
        release.wait(2)
        active -= 1
        completed.set()
        return SimpleNamespace(owner=None)

    service = StatusService([SimpleNamespace(id="minecraft")], slot_observer=blocking_slot)
    first = asyncio.create_task(service.snapshot())
    assert await asyncio.to_thread(entered.wait, 1)
    first.cancel()
    # Cancellation must not release the single-flight lock while the worker
    # still owns the external probe.  Let it finish before awaiting the
    # cancelled task, then a second refresh can safely run.
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert completed.is_set()
    second = asyncio.create_task(service.snapshot())
    await asyncio.wait_for(second, timeout=1)
    assert calls == 2
    assert max_active == 1


@pytest.mark.asyncio
async def test_cancelled_late_provider_failure_is_drained_without_stale_cache_or_overlap():
    entered = threading.Event()
    release = threading.Event()
    calls = 0
    active = 0
    max_active = 0

    def provider():
        nonlocal calls, active, max_active
        calls += 1
        active += 1
        max_active = max(max_active, active)
        entered.set()
        release.wait(2)
        active -= 1
        if calls == 1:
            raise RuntimeError("late provider failure")
        return SimpleNamespace(owner=None)

    service = StatusService([SimpleNamespace(id="minecraft")], slot_observer=provider)
    loop = asyncio.get_running_loop()
    loop_errors = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
    try:
        first = asyncio.create_task(service.snapshot())
        assert await asyncio.to_thread(entered.wait, 1)
        first.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert service._snapshot_cache is None

        second = asyncio.create_task(service.snapshot())
        await asyncio.wait_for(second, timeout=1)
        assert calls == 2
        assert max_active == 1
        await asyncio.sleep(0)
        assert loop_errors == []
    finally:
        loop.set_exception_handler(previous_handler)


@pytest.mark.asyncio
async def test_status_demand_coalesces_concurrent_clients_and_expires_short_cache():
    calls = 0
    now = [100.0]
    entered = asyncio.Event()
    release = asyncio.Event()

    class Adapter:
        async def observe(self, _profile):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return SimpleNamespace(running=False, healthy=None)

    service = StatusService(
        [SimpleNamespace(id="minecraft")], adapter=Adapter(), monotonic=lambda: now[0]
    )
    first = asyncio.create_task(service.snapshot())
    await entered.wait()
    second = asyncio.create_task(service.snapshot())
    release.set()
    first_snapshot, second_snapshot = await asyncio.gather(first, second)

    assert first_snapshot is second_snapshot
    assert calls == 1

    assert await service.snapshot() is first_snapshot
    assert calls == 1
    now[0] += 2.001
    assert await service.snapshot() is not first_snapshot
    assert calls == 2


@pytest.mark.asyncio
async def test_forced_status_snapshot_bypasses_demand_cache():
    calls = 0

    class Adapter:
        async def observe(self, _profile):
            nonlocal calls
            calls += 1
            return SimpleNamespace(running=False, healthy=None)

    service = StatusService([SimpleNamespace(id="minecraft")], adapter=Adapter())
    first = await service.snapshot()
    forced = await service.snapshot(force=True)

    assert forced is not first
    assert calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["connection", "adapter", "health", "metrics", "players", "disk", "version"])
async def test_each_sync_external_stage_does_not_block_pure_reads(stage, tmp_path, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    profile = SimpleNamespace(id="minecraft", ports=())

    async def running_slot():
        return SimpleNamespace(owner="minecraft")

    def blocking(*_args, **_kwargs):
        entered.set()
        release.wait(2)
        return SimpleNamespace(pid=11, rss_bytes=1, cpu_percent=1)

    kwargs = {"slot_observer": running_slot}
    if stage == "connection":
        profile.ports = (SimpleNamespace(protocol="tcp", port=25565),)
        kwargs["connection_provider"] = blocking
    elif stage == "adapter":
        class Adapter:
            observe = blocking
        kwargs["adapter"] = Adapter()
    elif stage == "health":
        class Adapter:
            async def observe(self, _profile):
                return SimpleNamespace(running=True, pid=11)
        class Health:
            def check(self, *_args, **_kwargs):
                blocking(*_args, **_kwargs)
                return SimpleNamespace(state="healthy", process_alive=True, required_ports=True)
        kwargs.update(adapter=Adapter(), health_checker=Health())
    elif stage == "metrics":
        class Adapter:
            async def observe(self, _profile):
                return SimpleNamespace(running=True, pid=11)
        class Metrics:
            sample = blocking
        kwargs.update(adapter=Adapter(), metrics=Metrics())
    elif stage == "players":
        class Adapter:
            async def observe(self, _profile):
                return SimpleNamespace(running=True, pid=11)
        class Players:
            def count(self, *_args, **_kwargs):
                blocking(*_args, **_kwargs)
                return 0
        kwargs.update(adapter=Adapter(), player_tracker=Players())
    elif stage == "disk":
        stopped = SimpleNamespace(id="pz-rising", ports=())
        profile = SimpleNamespace(id="minecraft", ports=())
        class Metrics:
            cached_disk_metrics = blocking
        kwargs.update(profiles=[profile, stopped], metrics=Metrics())
    elif stage == "version":
        version_file = tmp_path / "version"
        version_file.write_text("42.13\n")
        profile.paths = SimpleNamespace(version_file=version_file)
        real_read_text = Path.read_text
        def read_text(path, *args, **kwargs):
            if path == version_file:
                entered.set()
                release.wait(2)
            return real_read_text(path, *args, **kwargs)
        monkeypatch.setattr(Path, "read_text", read_text)

    profiles = kwargs.pop("profiles", [profile])
    service = StatusService(profiles, **kwargs)
    maintenance = asyncio.create_task(service.snapshot())
    assert await asyncio.to_thread(entered.wait, 1), stage
    await asyncio.wait_for(service.cached_snapshot(), timeout=0.2)
    release.set()
    await asyncio.wait_for(maintenance, timeout=1)


@pytest.mark.asyncio
async def test_status_coalesces_socket_enumeration_per_protocol_per_snapshot():
    calls: list[str] = []
    tcp_rows = [SimpleNamespace(laddr=("127.0.0.1", 25565), status="LISTEN", pid=11)]
    udp_rows = [SimpleNamespace(laddr=("127.0.0.1", 16261), status="NONE", pid=12)]

    def connections(*, kind):
        calls.append(kind)
        return tcp_rows if kind == "tcp" else udp_rows

    class Adapter:
        async def observe(self, _profile):
            return SimpleNamespace(running=True, healthy=True, pid=11)

    class Health:
        async def check(self, _profile, *, connections=None):
            assert connections == {"tcp": tcp_rows, "udp": udp_rows}
            return SimpleNamespace(state="healthy", process_alive=True, required_ports=True)

    class Metrics:
        def sample(self, _profile, *, pid=None, connections=None):
            assert connections == {"tcp": tcp_rows, "udp": udp_rows}
            return SimpleNamespace(pid=pid)

    profiles = [
        SimpleNamespace(id="minecraft", ports=(SimpleNamespace(protocol="tcp", port=25565),)),
        SimpleNamespace(id="pz-rising", ports=(SimpleNamespace(protocol="udp", port=16261),)),
    ]
    snapshot = await StatusService(
        profiles,
        adapter=Adapter(),
        health_checker=Health(),
        metrics=Metrics(),
        connection_provider=connections,
    ).snapshot()

    assert len(snapshot.profiles) == 2
    assert calls == ["tcp", "udp"]


@pytest.mark.asyncio
async def test_status_reuses_adapter_observation_for_health_check():
    observations = []

    class Adapter:
        async def observe(self, _profile):
            observation = SimpleNamespace(running=True, healthy=True, pid=11, players_online=2)
            observations.append(observation)
            return observation

    class Health:
        async def check(self, _profile, *, observation=None, connections=None):
            assert observation is observations[0]
            return SimpleNamespace(state="healthy", process_alive=True, required_ports=True)

    snapshot = await StatusService(
        [SimpleNamespace(id="minecraft", ports=())],
        adapter=Adapter(),
        health_checker=Health(),
    ).snapshot()

    assert len(observations) == 1
    assert snapshot.profiles[0].players_online == 2


@pytest.mark.asyncio
async def test_status_reuses_one_metric_sample_for_health_and_projection():
    samples = []

    class Adapter:
        async def observe(self, _profile):
            return SimpleNamespace(running=True, healthy=True, pid=11)

    class Metrics:
        def sample(self, _profile, *, pid=None, connections=None):
            sample = SimpleNamespace(pid=pid, rss_bytes=123, cpu_percent=4.5)
            samples.append(sample)
            return sample

    class Health:
        async def check(
            self,
            _profile,
            *,
            observation=None,
            connections=None,
            process_metrics=None,
        ):
            assert process_metrics is samples[0]
            return SimpleNamespace(state="healthy", process_alive=True, required_ports=True)

    snapshot = await StatusService(
        [SimpleNamespace(id="minecraft", ports=())],
        adapter=Adapter(),
        health_checker=Health(),
        metrics=Metrics(),
    ).snapshot()

    assert len(samples) == 1
    assert snapshot.profiles[0].rss_bytes == 123


@pytest.mark.asyncio
async def test_status_skips_full_probes_for_profiles_outside_active_slot():
    observe_calls: list[str] = []
    health_calls: list[str] = []
    metric_calls: list[str] = []
    connection_calls: list[str] = []

    owner = SimpleNamespace(id="minecraft", ports=(SimpleNamespace(protocol="tcp", port=25565),))
    stopped = SimpleNamespace(
        id="pz-rising",
        installed_version="42.13",
        ports=(SimpleNamespace(protocol="udp", port=16261),),
    )

    class Adapter:
        def __init__(self, profile_id):
            self.profile_id = profile_id

        async def observe(self, _profile):
            observe_calls.append(self.profile_id)
            return SimpleNamespace(running=self.profile_id == "minecraft", healthy=True, pid=41)

    class Health:
        def __init__(self, profile_id):
            self.profile_id = profile_id

        async def check(self, _profile, *, connections=None):
            health_calls.append(self.profile_id)
            return SimpleNamespace(state="healthy", process_alive=True, required_ports=True)

    class Metrics:
        def sample(self, profile, *, pid=None, connections=None):
            metric_calls.append(profile.id)
            return SimpleNamespace(pid=pid, rss_bytes=123, cpu_percent=1)

    def connections(*, kind):
        connection_calls.append(kind)
        return []

    snapshot = await StatusService(
        [owner, stopped],
        adapters={"minecraft": Adapter("minecraft"), "pz-rising": Adapter("pz-rising")},
        slot_observer=lambda: SimpleNamespace(owner="minecraft"),
        health_checker={"minecraft": Health("minecraft"), "pz-rising": Health("pz-rising")},
        metrics=Metrics(),
        connection_provider=connections,
    ).snapshot()

    assert observe_calls == ["minecraft"]
    assert health_calls == ["minecraft"]
    assert metric_calls == ["minecraft"]
    assert connection_calls == ["tcp"]
    assert snapshot.profiles[0].state.value == "running"
    assert snapshot.profiles[1].state.value == "blocked"
    assert snapshot.profiles[1].installed_version == "42.13"
    assert snapshot.profiles[1].pid is None
    assert snapshot.profiles[1].rss_bytes is None
    assert snapshot.profiles[1].cpu_percent is None


@pytest.mark.asyncio
async def test_status_uses_cached_disk_metrics_for_non_owner_profiles():
    owner = SimpleNamespace(id="minecraft", ports=())
    stopped = SimpleNamespace(id="pz-rising", ports=())

    class Metrics:
        def cached_disk_metrics(self, profile):
            return SimpleNamespace(profile_data_free_bytes=987654321)

        def sample(self, profile, **kwargs):
            return SimpleNamespace(pid=None)

    snapshot = await StatusService(
        [owner, stopped],
        slot_observer=lambda: SimpleNamespace(owner="minecraft"),
        metrics=Metrics(),
    ).snapshot()

    assert snapshot.profiles[1].disk_free_bytes == 987654321
    assert snapshot.profiles[1].disk_read_bps is None
    assert snapshot.profiles[1].disk_write_bps is None


@pytest.mark.asyncio
async def test_status_caches_idle_installed_version_by_file_mtime(tmp_path, monkeypatch):
    version_file = tmp_path / "version"
    version_file.write_text("42.13\n")
    reads: list[Path] = []
    real_read_text = Path.read_text

    def read_text(path, *args, **kwargs):
        if path == version_file:
            reads.append(path)
        return real_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_text)
    profile = SimpleNamespace(
        id="pz-rising",
        paths=SimpleNamespace(version_file=version_file),
    )
    service = StatusService(
        [profile],
        slot_observer=lambda: SimpleNamespace(owner="minecraft"),
    )

    first = await service.snapshot()
    second = await service.snapshot()

    assert first.profiles[0].installed_version == "42.13"
    assert second.profiles[0].installed_version == "42.13"
    assert reads == [version_file]


@pytest.mark.asyncio
async def test_status_parses_crafty_variables_file_instead_of_publishing_dump(tmp_path):
    version_file = tmp_path / "variables.txt"
    version_file.write_text("# Forge variables\nVERSION=1.20.1\nFORGE_VERSION=47.3.0\n")
    profile = SimpleNamespace(
        id="minecraft",
        paths=SimpleNamespace(version_file=version_file),
    )

    snapshot = await StatusService(
        [profile],
        slot_observer=lambda: SimpleNamespace(owner=None),
    ).snapshot()

    assert snapshot.profiles[0].installed_version == "1.20.1"
    assert "\n" not in snapshot.profiles[0].installed_version


@pytest.mark.asyncio
async def test_cached_snapshot_does_not_resample_the_status_pipeline():
    calls = 0

    class Adapter:
        async def observe(self, _profile):
            nonlocal calls
            calls += 1
            return type("Observation", (), {"running": False, "healthy": None})()

    profile = type("Profile", (), {"id": "minecraft"})()
    service = StatusService([profile], adapter=Adapter())

    fresh = await service.snapshot()
    cached = await service.cached_snapshot()

    assert cached is fresh
    assert calls == 1


@pytest.mark.asyncio
async def test_cached_snapshot_projects_starting_job_without_probe_or_fake_players():
    entered = asyncio.Event()
    release = asyncio.Event()
    jobs = {"minecraft": None}

    class Adapter:
        async def observe(self, _profile):
            entered.set()
            await release.wait()
            return SimpleNamespace(running=False, healthy=None, players_online=None)

    profile = SimpleNamespace(id="minecraft")
    service = StatusService([profile], adapter=Adapter(), active_jobs=jobs)
    initial = await service.cached_snapshot()
    jobs["minecraft"] = "start"

    slow_refresh = asyncio.create_task(service.snapshot(force=True))
    await entered.wait()
    responsive = await asyncio.wait_for(service.cached_snapshot(), timeout=0.2)

    status = responsive.profiles[0]
    assert status.state is ObservedState.STARTING
    assert status.active_job_id == "start"
    assert status.health.value == "unknown"
    assert status.players_online is None
    assert initial.profiles[0].state is ObservedState.STOPPED

    release.set()
    await slow_refresh


@pytest.mark.asyncio
async def test_benchmark_eligibility_forces_fresh_projection_instead_of_cached_stopped_status(tmp_path):
    observed = {"running": False}
    calls = 0

    class Adapter:
        async def observe(self, _profile):
            nonlocal calls
            calls += 1
            return SimpleNamespace(running=observed["running"], healthy=True)

    class Health:
        def check(self, *_args, **_kwargs):
            return SimpleNamespace(process_alive=observed["running"], state="healthy")

    sessions = sqlite3.connect(":memory:")
    sessions.execute("CREATE TABLE player_sessions (ended_at TEXT)")
    service = StatusService(
        [SimpleNamespace(id="minecraft")],
        adapter=Adapter(),
        health_checker=Health(),
        session_store=SimpleNamespace(connection=sessions),
        ups_health=lambda: True,
        storage_paths=(str(tmp_path),),
    )

    cached_stopped = await service.snapshot(persist=False, force=True)
    assert cached_stopped.profiles[0].state is ObservedState.STOPPED
    observed["running"] = True

    evidence = await service.benchmark_eligibility(
        maintenance_window=True,
        rollback_safe=True,
        public_wake_policy="safe",
    )

    assert calls == 2
    assert evidence["no_conflicting_jobs"] is False


@pytest.mark.asyncio
async def test_benchmark_eligibility_rejects_bool_wake_evidence():
    sessions = sqlite3.connect(":memory:")
    sessions.execute("CREATE TABLE player_sessions (ended_at TEXT)")
    service = StatusService(
        [SimpleNamespace(id="minecraft")],
        session_store=SimpleNamespace(connection=sessions),
        capability_evidence=lambda: True,
        ups_health=lambda: True,
        storage_paths=("/",),
    )

    rejected = await service.benchmark_eligibility(
        maintenance_window=True,
        rollback_safe=True,
        public_wake_policy="safe",
    )
    assert rejected["no_wake_session"] is False

    service.benchmark_safety.wake_evidence = lambda: WakeSafetyEvidence(True, True)
    accepted = await service.benchmark_eligibility(
        maintenance_window=True,
        rollback_safe=True,
        public_wake_policy="safe",
    )
    assert accepted["no_wake_session"] is True


@pytest.mark.asyncio
async def test_cached_snapshot_does_not_probe_when_empty():
    calls = 0

    class Adapter:
        async def observe(self, _profile):
            nonlocal calls
            calls += 1
            return type("Observation", (), {"running": False, "healthy": None})()

    cached = await StatusService(
        [SimpleNamespace(id="minecraft")],
        adapter=Adapter(),
    ).cached_snapshot()

    assert len(cached.profiles) == 1
    assert cached.profiles[0].state is ObservedState.STOPPED
    assert calls == 0


@pytest.mark.asyncio
async def test_failed_refresh_does_not_replace_last_good_cached_projection():
    should_fail = False

    def slot_observer():
        if should_fail:
            raise RuntimeError("publisher input unavailable")
        return None

    service = StatusService([SimpleNamespace(id="minecraft")], slot_observer=slot_observer)
    first = await service.snapshot()
    should_fail = True

    with pytest.raises(RuntimeError, match="publisher input unavailable"):
        await service.snapshot(force=True)

    assert await service.cached_snapshot() is first


@pytest.mark.parametrize(
    ("job", "process", "slot_other", "expected"),
    [
        ("start", False, False, "starting"),
        (None, True, False, "running"),
        ("stop", True, False, "stopping"),
        (None, False, True, "blocked"),
        ("failed", False, False, "failed"),
        (None, False, False, "stopped"),
    ],
)
def test_state_precedence(job, process, slot_other, expected):
    assert derive_state(
        active_job=job,
        process_alive=process,
        conflicting_slot_owner=slot_other,
    ) is ObservedState(expected)


def test_controller_process_does_not_make_minecraft_running():
    assert derive_state(active_job=None, process_alive=False, conflicting_slot_owner=False) is ObservedState.STOPPED
    assert derive_state(active_job=None, process_alive=True, conflicting_slot_owner=False) is ObservedState.RUNNING


@pytest.mark.asyncio
async def test_status_keeps_live_process_running_when_health_fails():
    from game_control.adapters.base import AdapterObservation
    from game_control.health import HealthResult
    from game_control.models import HealthState

    profile = type("Profile", (), {"id": "minecraft"})()

    class Adapter:
        async def observe(self, _profile):
            return AdapterObservation(running=True, healthy=False, pid=42)

    class Health:
        async def check(self, _profile):
            return HealthResult(state=HealthState.UNHEALTHY, process_alive=True)

    snapshot = await StatusService(
        [profile],
        adapter=Adapter(),
        health_checker=Health(),
    ).snapshot()
    assert snapshot.profiles[0].state is ObservedState.RUNNING
    assert snapshot.profiles[0].health is HealthState.UNHEALTHY


@pytest.mark.asyncio
async def test_status_uses_validated_health_process_identity():
    from game_control.adapters.base import AdapterObservation
    from game_control.health import HealthResult
    from game_control.models import HealthState

    profile = type("Profile", (), {"id": "minecraft"})()

    class Adapter:
        async def observe(self, _profile):
            # Crafty controller says "running", but no validated JVM exists.
            return AdapterObservation(running=True, healthy=None, pid=None)

    class Health:
        async def check(self, _profile):
            return HealthResult(state=HealthState.UNKNOWN, process_alive=False)

    snapshot = await StatusService([profile], adapter=Adapter(), health_checker=Health()).snapshot()
    assert snapshot.profiles[0].state is ObservedState.STOPPED


@pytest.mark.asyncio
async def test_status_reports_only_sampled_pid():
    from game_control.adapters.base import AdapterObservation
    from game_control.health import HealthResult
    from game_control.models import HealthState

    profile = type("Profile", (), {"id": "minecraft"})()
    adapter = type("Adapter", (), {"observe": lambda self, p: AdapterObservation(running=True, pid=99)})()
    health = type("Health", (), {"check": lambda self, p: HealthResult(HealthState.HEALTHY, True)})()
    metrics = type("Metrics", (), {"sample": lambda self, p, pid=None: type("M", (), {"pid": None})()})()
    snapshot = await StatusService([profile], adapter=adapter, health_checker=health, metrics=metrics).snapshot()
    assert snapshot.profiles[0].pid is None


@pytest.mark.asyncio
async def test_status_missing_validated_checker_fails_closed():
    from game_control.adapters.base import AdapterObservation

    profile = type("Profile", (), {"id": "minecraft"})()
    adapter = type("Adapter", (), {"observe": lambda self, p: AdapterObservation(running=True, pid=42)})()
    snapshot = await StatusService([profile], adapter=adapter).snapshot()
    assert snapshot.profiles[0].state is ObservedState.STOPPED


@pytest.mark.asyncio
async def test_status_adapter_error_degrades_only_failing_profile():
    from game_control.adapters.base import AdapterError, AdapterObservation
    from game_control.models import HealthState

    failing = type("Profile", (), {"id": "minecraft"})()
    healthy = type("Profile", (), {"id": "pz-rising"})()

    class FailingAdapter:
        async def observe(self, _profile):
            raise AdapterError("journal unavailable")

    class HealthyAdapter:
        async def observe(self, _profile):
            return AdapterObservation(running=True, healthy=True)

    snapshot = await StatusService(
        [failing, healthy],
        adapters={"minecraft": FailingAdapter(), "pz-rising": HealthyAdapter()},
    ).snapshot()
    assert snapshot.profiles[0].state is ObservedState.STOPPED
    assert snapshot.profiles[0].health is HealthState.UNKNOWN
    assert snapshot.profiles[1].health is HealthState.HEALTHY


@pytest.mark.asyncio
async def test_status_health_error_degrades_only_failing_profile():
    from game_control.adapters.base import AdapterObservation
    from game_control.health import HealthResult
    from game_control.models import HealthState

    failing = type("Profile", (), {"id": "minecraft"})()
    healthy = type("Profile", (), {"id": "pz-rising"})()

    class Adapter:
        async def observe(self, _profile):
            return AdapterObservation(running=True, healthy=None)

    class Health:
        async def check(self, profile):
            if profile.id == "minecraft":
                raise RuntimeError("health unavailable")
            return HealthResult(HealthState.HEALTHY, process_alive=True)

    snapshot = await StatusService(
        [failing, healthy],
        adapter=Adapter(),
        health_checker=Health(),
    ).snapshot()
    assert snapshot.profiles[0].state is ObservedState.STOPPED
    assert snapshot.profiles[0].health is HealthState.UNKNOWN
    assert snapshot.profiles[1].state is ObservedState.RUNNING
    assert snapshot.profiles[1].health is HealthState.HEALTHY
