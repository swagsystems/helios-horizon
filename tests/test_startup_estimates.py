"""Focused coverage for the experimental startup estimate projection."""
from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from game_control.controller import Controller, _MemoryDb, _MemoryLock
from game_control.models import (
    AdapterKind,
    HealthState,
    ObservedState,
    OperationName,
    PathSpec,
    PortSpec,
    ProcessSpec,
    Profile,
    ProfileId,
    UpdateSpec,
)
from game_control.protocol import GetStatus, ProfileStatus, RpcRequest, Start, StatusSnapshot
from game_control.startup_estimates import (
    MAX_SAMPLES,
    MAX_VERSION_BUCKETS,
    StartupEstimateStore,
    installed_version_for_profile,
    median_seconds,
    read_bounded_version_file,
    validated_duration_ms,
)


VERSION = "1.21.8"
SERIES_VERSION = "1.20.4"


def _connection(tmp_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(tmp_path / "state.db", check_same_thread=False)


def _store(tmp_path: Path) -> tuple[StartupEstimateStore, sqlite3.Connection]:
    connection = _connection(tmp_path)
    return StartupEstimateStore(lambda: connection), connection


def _profile(version_file: Path) -> Profile:
    return Profile(
        id=ProfileId.MINECRAFT,
        display_name="Minecraft",
        adapter=AdapterKind.SYSTEMD,
        systemd_unit="minecraft.service",
        process=ProcessSpec(executable=Path("/usr/bin/java")),
        ports=(PortSpec(protocol="tcp", port=25565),),
        start_timeout_seconds=5,
        stop_timeout_seconds=5,
        health_timeout_seconds=5,
        paths=PathSpec(
            data_roots=(Path("/var/lib/game-control/minecraft"),),
            mutable_root=Path("/var/lib/game-control/minecraft"),
            backup_root=Path("/var/backups/game-control/minecraft"),
            install_root=Path("/opt/game-control/minecraft"),
            version_file=version_file,
        ),
        min_available_memory_bytes=1,
        min_free_disk_bytes=1,
        operations=frozenset({OperationName.START, OperationName.STOP}),
        update=UpdateSpec(kind="manual"),
    )


def _starting_status(profile: Profile, *, version: str | None = VERSION) -> StatusSnapshot:
    return StatusSnapshot(
        generation=1,
        observed_at="2026-09-14T12:00:00Z",
        profiles=(
            ProfileStatus(
                profile_id=profile.id,
                state=ObservedState.STARTING,
                health=HealthState.UNKNOWN,
                slot_owner=None,
                active_job_id="job-1",
                pid=None,
                started_at=None,
                uptime_seconds=None,
                cpu_percent=None,
                rss_bytes=None,
                players_online=None,
                installed_version=version,
                restart_required=False,
                required_ports_ready=False,
            ),
        ),
    )


class _StatusStub:
    def __init__(self, profile: Profile, *, version: str | None = VERSION):
        self.profile = profile
        self.version = version

    def _snapshot(self) -> StatusSnapshot:
        return _starting_status(self.profile, version=self.version)

    async def snapshot(self, action=None, actor=None, request_id=None, *, maintenance=False):
        return self._snapshot()

    async def cached_snapshot(self, action=None, actor=None, request_id=None):
        return self._snapshot()


class _GatedAdapter:
    def __init__(self):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.started = 0

    async def start(self, profile):
        self.started += 1
        self.entered.set()
        await self.release.wait()

    async def graceful_stop(self, profile):
        await asyncio.sleep(0)

    async def force_stop(self, profile):
        await asyncio.sleep(0)


def _controller(tmp_path: Path, profile: Profile, adapter, *, ready=True, **kwargs) -> Controller:
    return Controller(
        profiles={profile.id: profile},
        state_db=_MemoryDb(tmp_path / "controller.db"),
        adapters={profile.id: adapter},
        operation_lock_factory=_MemoryLock,
        await_free_slot=lambda: True,
        await_ready=lambda _profile: ready,
        **kwargs,
    )


def _start_request(profile: Profile, request_id=None) -> RpcRequest:
    return RpcRequest(
        request_id=request_id or uuid4(),
        actor="operator",
        action=Start(kind="start", profile_id=profile.id),
    )


# --- estimator unit coverage -------------------------------------------------


def test_median_prefers_median_over_outlier_mean() -> None:
    assert median_seconds([10.0, 11.0, 12.0, 13.0, 1000.0]) == 12.0
    assert median_seconds([10.0, 12.0]) == 11.0
    with pytest.raises(ValueError):
        median_seconds([])


@pytest.mark.parametrize(
    "value",
    [-1.0, 0.0, float("nan"), float("inf"), float("-inf"), "1000", None, True, 900_001.0],
)
def test_invalid_durations_are_rejected_not_clamped(value) -> None:
    assert validated_duration_ms(value) is None


def test_five_sample_boundary_and_malformed_values(tmp_path: Path) -> None:
    store, connection = _store(tmp_path)
    for index in range(4):
        assert store.record_sample("minecraft", VERSION, 60_000 + index * 1_000, run_key=f"run-{index}")
    summary = store.summary("minecraft", VERSION)
    assert (summary.sample_count, summary.median_seconds) == (4, None)
    for bad in (-5.0, 0.0, float("nan"), float("inf"), "70000", True):
        assert store.record_sample("minecraft", VERSION, bad, run_key=f"bad-{bad}") is False
    assert store.summary("minecraft", VERSION).sample_count == 4
    assert store.record_sample("minecraft", VERSION, 64_000, run_key="run-4")
    summary = store.summary("minecraft", VERSION)
    assert summary.sample_count == 5
    assert summary.median_seconds == 62.0
    assert connection.execute("SELECT COUNT(*) FROM startup_estimate_samples").fetchone()[0] == 5


def test_history_is_capped_at_twenty_five_newest_samples(tmp_path: Path) -> None:
    store, connection = _store(tmp_path)
    for index in range(MAX_SAMPLES + 10):
        store.record_sample("minecraft", VERSION, 1_000 + index, run_key=f"run-{index}")
    assert store.summary("minecraft", VERSION).sample_count == MAX_SAMPLES
    rows = connection.execute(
        "SELECT duration_ms FROM startup_estimate_samples ORDER BY id"
    ).fetchall()
    assert [row[0] for row in rows][0] == 1_000 + 10
    assert connection.execute("SELECT COUNT(*) FROM startup_estimate_samples").fetchone()[0] == MAX_SAMPLES


def test_profile_and_version_history_stay_partitioned(tmp_path: Path) -> None:
    store, _connection = _store(tmp_path)
    for index in range(5):
        store.record_sample("minecraft", VERSION, 60_000, run_key=f"mc-{index}")
    for index in range(5):
        store.record_sample("minecraft", SERIES_VERSION, 90_000, run_key=f"old-{index}")
    store.record_sample("terraria-vanilla", VERSION, 10_000, run_key="tv-1")
    assert store.summary("minecraft", VERSION).median_seconds == 60.0
    assert store.summary("minecraft", SERIES_VERSION).median_seconds == 90.0
    assert store.summary("terraria-vanilla", VERSION).sample_count == 1
    # Unknown or absent versions never reuse a known version's history.
    assert store.summary("minecraft", None).sample_count == 0
    assert store.summary("minecraft", "").sample_count == 0
    assert store.summary("unknown-profile", VERSION).sample_count == 0


def test_old_version_buckets_are_bounded(tmp_path: Path) -> None:
    store, connection = _store(tmp_path)
    versions = [f"1.{index}" for index in range(MAX_VERSION_BUCKETS + 2)]
    for index, version in enumerate(versions):
        store.record_sample("minecraft", version, 1_000 + index, run_key=f"{version}-run")
    retained = {
        row[0] for row in connection.execute("SELECT DISTINCT version FROM startup_estimate_samples")
    }
    assert retained == set(versions[-MAX_VERSION_BUCKETS:])
    assert store.summary("minecraft", versions[0]).sample_count == 0
    assert store.summary("minecraft", versions[-1]).sample_count == 1


def test_run_key_deduplicates_repeated_successes(tmp_path: Path) -> None:
    store, connection = _store(tmp_path)
    assert store.record_sample("minecraft", VERSION, 61_000, run_key="attempt-1")
    assert store.record_sample("minecraft", VERSION, 62_000, run_key="attempt-1")
    assert connection.execute("SELECT COUNT(*) FROM startup_estimate_samples").fetchone()[0] == 1
    assert store.summary("minecraft", VERSION).sample_count == 1


def test_history_survives_store_reopen(tmp_path: Path) -> None:
    store, connection = _store(tmp_path)
    for index in range(5):
        store.record_sample("minecraft", VERSION, 60_000, run_key=f"run-{index}")
    connection.commit()
    reopened = StartupEstimateStore(lambda: connection)
    assert reopened.summary("minecraft", VERSION).median_seconds == 60.0
    connection2 = sqlite3.connect(tmp_path / "state.db", check_same_thread=False)
    try:
        fresh = StartupEstimateStore(lambda: connection2)
        summary = fresh.summary("minecraft", VERSION)
        assert (summary.sample_count, summary.median_seconds) == (5, 60.0)
    finally:
        connection2.close()


def test_unavailable_history_never_raises(tmp_path: Path) -> None:
    def broken():
        raise sqlite3.OperationalError("history unavailable")

    store = StartupEstimateStore(broken)
    assert store.record_sample("minecraft", VERSION, 60_000, run_key="run") is False
    assert store.summary("minecraft", VERSION).sample_count == 0


def test_unknown_version_is_not_recorded(tmp_path: Path) -> None:
    store, _connection = _store(tmp_path)
    assert store.record_sample("minecraft", None, 60_000, run_key="run") is False
    assert store.record_sample("minecraft", "", 60_000, run_key="run") is False
    assert store.summary("minecraft", VERSION).sample_count == 0


def test_installed_version_reads_bounded_profile_file(tmp_path: Path) -> None:
    version_file = tmp_path / "version"
    version_file.write_text("SERVER_VERSION=1.21.8\n", encoding="utf-8")
    assert installed_version_for_profile(_profile(version_file)) == "1.21.8"
    assert installed_version_for_profile(_profile(tmp_path / "missing")) is None


def test_version_file_reader_rejects_symlinks_and_oversized_files(tmp_path: Path) -> None:
    real = tmp_path / "version"
    real.write_text("SERVER_VERSION=1.21.8\n", encoding="utf-8")
    link = tmp_path / "version-link"
    link.symlink_to(real)
    assert read_bounded_version_file(link) is None
    big = tmp_path / "big-version"
    big.write_bytes(b"SERVER_VERSION=" + b"1" * 200_000)
    assert read_bounded_version_file(big) is None
    assert read_bounded_version_file(tmp_path) is None


def test_read_side_rejects_corrupt_persisted_rows(tmp_path: Path) -> None:
    store, connection = _store(tmp_path)
    assert store.ensure_schema() is True
    connection.execute(
        "INSERT INTO startup_estimate_samples(profile_id, version, duration_ms, finished_at, run_key)"
        " VALUES ('minecraft', ?, ?, '2026-09-14T12:00:00Z', 'raw')",
        (VERSION, float("inf")),
    )
    connection.execute(
        "INSERT INTO startup_estimate_samples(profile_id, version, duration_ms, finished_at, run_key)"
        " VALUES ('minecraft', ?, 10_000_000.0, '2026-09-14T12:00:00Z', 'raw-big')",
        (VERSION,),
    )
    for index in range(5):
        store.record_sample("minecraft", VERSION, 60_000, run_key=f"ok-{index}")
    summary = store.summary("minecraft", VERSION)
    assert summary.sample_count == 5
    assert summary.median_seconds == 60.0


def test_write_invalidates_every_cached_bucket_for_the_profile(tmp_path: Path) -> None:
    store, _connection = _store(tmp_path)
    versions = [f"2.{index}" for index in range(MAX_VERSION_BUCKETS - 1)]
    for index, version in enumerate(versions):
        assert store.record_sample("minecraft", version, 1_000 + index, run_key=f"{version}-run")
    # Populate the read cache for the oldest bucket, then push it out of the
    # bounded version window; the cached entry must not survive the write.
    assert store.summary("minecraft", versions[0]).sample_count == 1
    assert store.record_sample("minecraft", "2.four", 4_000, run_key="four-run")
    assert store.summary("minecraft", versions[0]).sample_count == 1
    assert store.record_sample("minecraft", "2.newest", 5_000, run_key="newest-run")
    assert store.summary("minecraft", versions[0]).sample_count == 0
    assert store.summary("minecraft", "2.newest").sample_count == 1


# --- controller integration -------------------------------------------------


@pytest.mark.asyncio
async def test_successful_start_populates_history_and_status_projection(tmp_path: Path) -> None:
    version_file = tmp_path / "version"
    version_file.write_text("SERVER_VERSION=1.21.8\n", encoding="utf-8")
    profile = _profile(version_file)
    adapter = _GatedAdapter()
    controller = _controller(tmp_path, profile, adapter)
    # The additive table is created once at construction, so ordinary status
    # reads never run DDL.
    assert controller._startup_estimates._schema_ready is True
    assert controller._db().execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='startup_estimate_samples'"
    ).fetchone() is not None
    for index in range(5):
        controller._startup_estimates.record_sample(profile.id, VERSION, 60_000, run_key=f"seed-{index}")
    controller.services = SimpleNamespace(status=_StatusStub(profile))

    task = asyncio.create_task(controller.execute(_start_request(profile)))
    await asyncio.wait_for(adapter.entered.wait(), timeout=5)
    snapshot = await controller.execute(
        RpcRequest(request_id=uuid4(), actor="operator", action=GetStatus(kind="get_status"))
    )
    status = snapshot.result.profiles[0]
    estimate = status.startup_estimate
    assert estimate is not None
    assert estimate.sample_count == 5
    assert estimate.median_seconds == 60.0
    assert isinstance(estimate.attempt_id, str) and len(estimate.attempt_id) == 32
    assert estimate.elapsed_seconds is not None and estimate.elapsed_seconds >= 0
    serialized = snapshot.result.model_dump(mode="json")
    assert serialized["profiles"][0]["startup_estimate"]["median_seconds"] == 60.0
    assert serialized["profiles"][0]["startup_estimate"]["attempt_id"] == estimate.attempt_id
    # Ordinary status reads stay read-only.
    assert controller._db().execute("SELECT COUNT(*) FROM startup_estimate_samples").fetchone()[0] == 5

    adapter.release.set()
    response = await asyncio.wait_for(task, timeout=5)
    assert response.ok
    rows = controller._db().execute(
        "SELECT version, duration_ms FROM startup_estimate_samples WHERE run_key NOT LIKE 'seed-%'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == VERSION
    assert rows[0][1] > 0
    assert controller._startup_attempts == {}


@pytest.mark.asyncio
async def test_idempotent_replay_records_a_single_sample(tmp_path: Path) -> None:
    version_file = tmp_path / "version"
    version_file.write_text("SERVER_VERSION=1.21.8\n", encoding="utf-8")
    profile = _profile(version_file)
    adapter = _GatedAdapter()
    adapter.release.set()
    controller = _controller(tmp_path, profile, adapter)
    request = _start_request(profile)
    assert (await controller.execute(request)).ok
    assert (await controller.execute(request)).ok
    assert adapter.started == 1
    assert controller._db().execute("SELECT COUNT(*) FROM startup_estimate_samples").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_failed_start_is_never_trained(tmp_path: Path) -> None:
    version_file = tmp_path / "version"
    version_file.write_text("SERVER_VERSION=1.21.8\n", encoding="utf-8")
    profile = _profile(version_file)
    adapter = _GatedAdapter()
    adapter.release.set()
    controller = _controller(tmp_path, profile, adapter, ready=False)
    response = await controller.execute(_start_request(profile))
    assert not response.ok
    assert controller._db().execute("SELECT COUNT(*) FROM startup_estimate_samples").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_unknown_version_is_learning_and_never_trained(tmp_path: Path) -> None:
    profile = _profile(tmp_path / "missing-version")
    adapter = _GatedAdapter()
    adapter.release.set()
    controller = _controller(tmp_path, profile, adapter)
    assert (await controller.execute(_start_request(profile))).ok
    assert controller._db().execute("SELECT COUNT(*) FROM startup_estimate_samples").fetchone()[0] == 0
    summary = controller._startup_estimates.summary(profile.id, None)
    assert summary.sample_count == 0 and summary.median_seconds is None


@pytest.mark.asyncio
async def test_unavailable_history_does_not_break_start_or_status(tmp_path: Path) -> None:
    version_file = tmp_path / "version"
    version_file.write_text("SERVER_VERSION=1.21.8\n", encoding="utf-8")
    profile = _profile(version_file)
    adapter = _GatedAdapter()
    adapter.release.set()
    controller = _controller(tmp_path, profile, adapter)

    def broken():
        raise sqlite3.OperationalError("state database is gone")

    controller._startup_estimates._connection_provider = broken
    controller._startup_estimates._schema_ready = False
    controller.services = SimpleNamespace(status=_StatusStub(profile))
    assert (await controller.execute(_start_request(profile))).ok
    snapshot = await controller.execute(
        RpcRequest(request_id=uuid4(), actor="operator", action=GetStatus(kind="get_status"))
    )
    assert snapshot.ok
    assert snapshot.result.profiles[0].state is ObservedState.STARTING
    # A degraded history degrades to "no estimate"; it never fabricates one.
    assert snapshot.result.profiles[0].startup_estimate is None


class _StaticStatus:
    def __init__(self, snapshot: StatusSnapshot):
        self.snapshot = snapshot

    async def snapshot(self, action=None, actor=None, request_id=None, *, maintenance=False):
        return self.snapshot

    async def cached_snapshot(self, action=None, actor=None, request_id=None):
        return self.snapshot


@pytest.mark.asyncio
async def test_status_outside_starting_has_no_estimate(tmp_path: Path) -> None:
    version_file = tmp_path / "version"
    version_file.write_text("SERVER_VERSION=1.21.8\n", encoding="utf-8")
    profile = _profile(version_file)
    adapter = _GatedAdapter()
    controller = _controller(tmp_path, profile, adapter)
    for index in range(5):
        controller._startup_estimates.record_sample(profile.id, VERSION, 60_000, run_key=f"seed-{index}")
    ready = _starting_status(profile)
    stopped = ready.model_copy(
        update={"profiles": (ready.profiles[0].model_copy(update={"state": ObservedState.STOPPED}),)}
    )
    controller.services = SimpleNamespace(status=_StaticStatus(stopped))
    response = await controller.execute(
        RpcRequest(request_id=uuid4(), actor="operator", action=GetStatus(kind="get_status"))
    )
    assert response.ok
    assert response.result.profiles[0].startup_estimate is None


@pytest.mark.asyncio
async def test_status_projection_never_runs_ddl_after_failed_initialization(tmp_path: Path, monkeypatch) -> None:
    from game_control import state_db as state_db_module

    version_file = tmp_path / "version"
    version_file.write_text("SERVER_VERSION=1.21.8\n", encoding="utf-8")
    profile = _profile(version_file)
    adapter = _GatedAdapter()
    controller = _controller(tmp_path, profile, adapter)
    calls = []

    def denied(connection):
        calls.append(connection)
        raise sqlite3.OperationalError("schema is read-only here")

    monkeypatch.setattr(state_db_module, "ensure_additive_state_tables", denied)
    controller._startup_estimates._schema_ready = False
    for index in range(5):
        assert controller._startup_estimates.record_sample(
            profile.id, VERSION, 60_000, run_key=f"seed-{index}"
        ) is False
    controller.services = SimpleNamespace(status=_StatusStub(profile))
    response = await controller.execute(
        RpcRequest(request_id=uuid4(), actor="operator", action=GetStatus(kind="get_status"))
    )
    assert response.ok
    assert response.result.profiles[0].startup_estimate is None
    # Only the record path attempted additive DDL; the status read stayed read-only.
    assert len(calls) == 5


@pytest.mark.asyncio
async def test_estimate_write_failure_leaves_successful_lifecycle_intact(tmp_path: Path) -> None:
    version_file = tmp_path / "version"
    version_file.write_text("SERVER_VERSION=1.21.8\n", encoding="utf-8")
    profile = _profile(version_file)
    adapter = _GatedAdapter()
    adapter.release.set()
    controller = _controller(tmp_path, profile, adapter)
    controller._startup_estimates._schema_ready = True

    def explode(*args, **kwargs):
        raise sqlite3.OperationalError("estimate write failed")

    controller._startup_estimates.record_sample = explode
    response = await controller.execute(_start_request(profile))
    assert response.ok and response.result.state == "running"
    jobs = controller._db().execute("SELECT state FROM jobs").fetchall()
    assert [row[0] for row in jobs] == ["succeeded"]
    assert controller._db().execute("SELECT COUNT(*) FROM startup_estimate_samples").fetchone()[0] == 0
    assert controller._db().execute("PRAGMA integrity_check").fetchone()[0] == "ok"


@pytest.mark.asyncio
async def test_managed_transaction_recording_commits_with_controller_ownership(tmp_path: Path) -> None:
    version_file = tmp_path / "version"
    version_file.write_text("SERVER_VERSION=1.21.8\n", encoding="utf-8")
    profile = _profile(version_file)
    adapter = _GatedAdapter()
    adapter.release.set()
    controller = _controller(tmp_path, profile, adapter)
    assert (await controller.execute(_start_request(profile))).ok
    rows = controller._db().execute(
        "SELECT profile_id, version, duration_ms FROM startup_estimate_samples"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "minecraft" and rows[0][1] == VERSION
    assert rows[0][2] > 0
    # The projected attempt binds to the version the attempt actually observed.
    status = _starting_status(profile, version="9.9.9").profiles[0]
    attempt = controller._begin_startup_attempt(profile.id, "1.21.8")
    try:
        estimate = controller._startup_estimate_for(status)
    finally:
        controller._end_startup_attempt(profile.id, attempt)
    assert estimate is not None
    assert estimate.version == "1.21.8"
    assert estimate.median_seconds is None
