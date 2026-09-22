"""Regression coverage for controller lock ordering and scheduled switch admission."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import multiprocessing
from pathlib import Path
import threading
from uuid import uuid4

import pytest

from game_control.controller import Controller
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
from game_control.protocol import StatusSnapshot
from game_control.schedule import ScheduleBook, parse_schedule
from game_control.slot import OperationLock, ReservationStore


def _profile(tmp_path: Path, profile_id: ProfileId) -> Profile:
    root = tmp_path / profile_id.value
    return Profile(
        id=profile_id,
        display_name=profile_id.value,
        adapter=AdapterKind.SYSTEMD,
        systemd_unit=f"{profile_id.value}.service",
        process=ProcessSpec(executable=Path("/usr/bin/java")),
        ports=(PortSpec(protocol="tcp", port=25565),),
        start_timeout_seconds=5,
        stop_timeout_seconds=5,
        health_timeout_seconds=5,
        paths=PathSpec(
            data_roots=(root / "data",),
            mutable_root=root / "data",
            backup_root=root / "backups",
            install_root=root / "install",
            version_file=root / "data" / "version",
        ),
        min_available_memory_bytes=1,
        min_free_disk_bytes=1,
        operations=frozenset({OperationName.START, OperationName.STOP}),
        update=UpdateSpec(kind="manual"),
    )


def _controller(tmp_path: Path) -> tuple[Controller, ReservationStore, Path]:
    operation = tmp_path / "operation.lock"
    operation.touch(mode=0o600)
    store = ReservationStore(operation, tmp_path / "reservation.json")
    controller = Controller.for_testing(tmp_path)
    controller.reservation_store = store
    controller._operation_lock_factory = lambda: OperationLock(operation)
    return controller, store, operation


def _lock_contention_child(tmp_path: Path, connection, operation_kind: str) -> None:
    """Keep a possible event-loop deadlock out of the pytest process."""

    async def exercise() -> None:
        controller, store, operation = _controller(tmp_path)
        profile = _profile(tmp_path, ProfileId.MINECRAFT)
        if operation_kind == "release":
            store.reserve_if_available(profile.id, "contender", 30)
        acquired = threading.Event()
        return_from_enter = threading.Event()

        class OrderedLock(OperationLock):
            def __enter__(self):
                result = super().__enter__()
                acquired.set()
                # This barrier selects an ordinary interleaving: the flock
                # worker has acquired the lock, but its event-loop continuation
                # has not executed the transaction callback/release yet.
                if not return_from_enter.wait(2):
                    raise AssertionError("transaction interleaving was not released")
                return result

        controller._operation_lock_factory = lambda: OrderedLock(operation)
        transaction = asyncio.create_task(controller._transaction(lambda: "committed"))
        assert await asyncio.to_thread(acquired.wait, 2)
        # _reserve is also reached from _start and scheduled/maintenance jobs.
        # Queue its admission before allowing the flock worker to return.
        contender = asyncio.create_task(
            controller._reserve(profile, "start", "contender")
            if operation_kind == "reserve"
            else controller._clear_reservation((profile.id, "contender", 0))
        )
        connection.send("transaction holds operation.lock; reservation contender queued")
        return_from_enter.set()
        committed, lease = await asyncio.gather(transaction, contender)
        assert committed == "committed"
        if operation_kind == "reserve":
            assert store.read().operation_id == "contender"
            await controller._clear_reservation(lease)
        assert store.read() is None
        connection.send("completed")

    try:
        asyncio.run(exercise())
    except BaseException as exc:
        connection.send(f"child failed: {type(exc).__name__}: {exc}")
    finally:
        connection.close()


@pytest.mark.parametrize("operation_kind", ["reserve", "release"])
def test_reservation_contention_does_not_deadlock_transaction_event_loop(tmp_path, operation_kind):
    context = multiprocessing.get_context("fork")
    receiver, sender = context.Pipe(duplex=False)
    child = context.Process(target=_lock_contention_child, args=(tmp_path, sender, operation_kind))
    child.start()
    sender.close()
    try:
        assert receiver.poll(5), "child did not reach the controlled lock interleaving"
        phase = receiver.recv()
        assert phase == "transaction holds operation.lock; reservation contender queued"
        assert receiver.poll(2), (
            "reservation admission blocked the event loop behind operation.lock; "
            "the transaction holding it cannot run its callback or release it"
        )
        assert receiver.recv() == "completed"
        child.join(timeout=2)
        assert child.exitcode == 0
    finally:
        if child.is_alive():
            child.terminate()
        child.join(timeout=2)
        receiver.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation_kind", ["reserve", "release"])
async def test_cancelled_reservation_io_drains_and_leaves_no_owned_lease(tmp_path, operation_kind):
    controller, _store, operation = _controller(tmp_path)
    profile = _profile(tmp_path, ProfileId.MINECRAFT)
    entered = threading.Event()
    release = threading.Event()

    class GatedStore(ReservationStore):
        def reserve_if_available(self, *args, **kwargs):
            result = super().reserve_if_available(*args, **kwargs)
            if operation_kind == "reserve":
                entered.set()
                release.wait(2)
            return result

        def release_if_owned(self, *args, **kwargs):
            if operation_kind == "release":
                entered.set()
                release.wait(2)
            return super().release_if_owned(*args, **kwargs)

    store = GatedStore(operation, tmp_path / "reservation.json")
    controller.reservation_store = store
    lease = (profile.id, "cancelled-io", 0)
    if operation_kind == "release":
        store.reserve_if_available(*lease[:2], 30)
    task = asyncio.create_task(
        controller._reserve(profile, "start", lease[1])
        if operation_kind == "reserve"
        else controller._clear_reservation(lease)
    )
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        assert not task.done(), "file-store work must not block the event loop"
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()  # Repeated shutdown/caller cancellation must still drain.
        await asyncio.sleep(0)
        assert not task.done(), "cancellation must wait for the file-store worker"
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert store.read() is None, "cancelled admission must release a late-acquired lease"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_acquisition_drains_cancellation_during_exact_owner_cleanup(tmp_path):
    controller, _store, operation = _controller(tmp_path)
    profile = _profile(tmp_path, ProfileId.MINECRAFT)
    acquired = threading.Event()
    return_acquire = threading.Event()
    releasing = threading.Event()
    finish_release = threading.Event()
    mutations = []

    class GatedStore(ReservationStore):
        def reserve_if_available(self, *args, **kwargs):
            result = super().reserve_if_available(*args, **kwargs)
            mutations.append("acquired")
            acquired.set()
            assert return_acquire.wait(2)
            return result

        def release_if_owned(self, *args, **kwargs):
            releasing.set()
            assert finish_release.wait(2)
            result = super().release_if_owned(*args, **kwargs)
            mutations.append("released")
            return result

    store = GatedStore(operation, tmp_path / "reservation.json")
    controller.reservation_store = store
    task = asyncio.create_task(controller._reserve(profile, "start", "cancelled-acquire"))
    try:
        assert await asyncio.to_thread(acquired.wait, 2)
        task.cancel()
        return_acquire.set()
        assert await asyncio.to_thread(releasing.wait, 2)
        assert store.read().operation_id == "cancelled-acquire"
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done(), "cancelled admission must retain ownership until cleanup finishes"
        finish_release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert store.read() is None
        assert mutations == ["acquired", "released"]
        # All worker completions are already observed before the caller exits;
        # subsequent executor/event-loop turns cannot resurrect the lease.
        await asyncio.to_thread(lambda: None)
        assert store.read() is None
        assert mutations == ["acquired", "released"]
    finally:
        return_acquire.set()
        finish_release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_rejected_admission_does_not_release_an_existing_lease(tmp_path):
    controller, store, operation = _controller(tmp_path)
    profile = _profile(tmp_path, ProfileId.MINECRAFT)
    existing = store.reserve_if_available(profile.id, "same-operation-id", 30)
    entered = threading.Event()
    release = threading.Event()

    class GatedStore(ReservationStore):
        def reserve_if_available(self, *args, **kwargs):
            entered.set()
            assert release.wait(2)
            return super().reserve_if_available(*args, **kwargs)

    controller.reservation_store = GatedStore(operation, tmp_path / "reservation.json")
    task = asyncio.create_task(controller._reserve(profile, "start", existing.operation_id))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert store.read() == existing, "only a successfully acquired lease belongs to cleanup"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_closing", [False, True])
async def test_release_drains_inflight_renewal_before_removing_lease(tmp_path, cancel_closing):
    controller, _store, operation = _controller(tmp_path)
    profile = _profile(tmp_path, ProfileId.MINECRAFT)
    renewed = threading.Event()
    finish_renewal = threading.Event()
    mutations = []

    class GatedStore(ReservationStore):
        def renew_if_owned(self, *args, **kwargs):
            result = super().renew_if_owned(*args, **kwargs)
            renewed.set()
            assert finish_renewal.wait(2)
            mutations.append("renewal_finished")
            return result

        def release_if_owned(self, *args, **kwargs):
            mutations.append("released")
            return super().release_if_owned(*args, **kwargs)

    store = GatedStore(operation, tmp_path / "reservation.json")
    controller.reservation_store = store
    lease = await controller._reserve(profile, "start", "renew-before-release")
    renewal = asyncio.create_task(controller._reservation_io(
        store.renew_if_owned, *lease[:2], ttl=30, state_generation=lease[2],
    ))
    closing = None
    try:
        assert await asyncio.to_thread(renewed.wait, 2)
        closing = asyncio.create_task(controller._release_lease(lease, renewal))
        await asyncio.sleep(0)
        assert mutations == []
        assert store.read() is not None
        if cancel_closing:
            closing.cancel()
            await asyncio.sleep(0)
            closing.cancel()
            await asyncio.sleep(0)
            assert not closing.done()
        finish_renewal.set()
        if cancel_closing:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(closing, 2)
        else:
            await asyncio.wait_for(closing, 2)
        assert mutations == ["renewal_finished", "released"]
        assert store.read() is None
        assert renewal.done()
    finally:
        finish_renewal.set()
        await asyncio.gather(renewal, *([closing] if closing else []), return_exceptions=True)


def _running_snapshot(source: Profile) -> StatusSnapshot:
    return StatusSnapshot(
        generation=0,
        observed_at=datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc),
        profiles=({
            "profile_id": source.id,
            "state": "running",
            "health": "healthy",
            "slot_owner": source.id,
            "active_job_id": None,
            "pid": 42,
            "started_at": None,
            "uptime_seconds": 1,
            "cpu_percent": 0,
            "rss_bytes": 1,
            "players_online": 0,
            "installed_version": None,
            "restart_required": False,
            "required_ports_ready": True,
        },),
    )


@pytest.mark.asyncio
async def test_duplicate_due_switch_entries_execute_one_lifecycle_transition(tmp_path):
    controller, store, _operation = _controller(tmp_path)
    source = _profile(tmp_path, ProfileId.MINECRAFT)
    target = _profile(tmp_path, ProfileId.PZ_RISING)
    controller.profiles = {profile.id: profile for profile in (source, target)}
    controller._schedule = ScheduleBook(parse_schedule([
        {"cron": "* * * * *", "profile": target.id.value},
        {"cron": "* * * * *", "profile": target.id.value},
    ]))
    owner = source.id
    calls = []

    class Adapter:
        async def graceful_stop(self, profile):
            nonlocal owner
            calls.append(("stop", profile.id))
            # systemctl stop of an already stopped unit succeeds too.
            if owner == profile.id:
                owner = None

        async def start(self, profile):
            nonlocal owner
            assert owner is None, "the single-slot adapter cannot start over another owner"
            assert store.valid_for_runner(profile.id) is True
            calls.append(("start", profile.id))
            owner = profile.id

    adapter = Adapter()
    controller.adapters = {source.id: adapter, target.id: adapter}
    controller.await_free_slot = lambda: owner is None
    controller.await_ready = lambda profile: owner == profile.id
    snapshot = _running_snapshot(source)

    await controller._apply_schedules(snapshot, uuid4())

    assert owner == target.id
    assert store.read() is None
    assert controller._db().execute(
        "SELECT COUNT(*) FROM events WHERE code='scheduled_fire'"
    ).fetchone() == (1,), "the real durable writer must claim this fire once"
    assert calls == [("stop", source.id), ("start", target.id)], (
        "an already-claimed duplicate switch must not stop the source again"
    )
    assert controller._db().execute(
        "SELECT state FROM jobs WHERE operation='switch'"
    ).fetchall() == [("succeeded",)]


@pytest.mark.asyncio
@pytest.mark.parametrize("reload_book", [False, True])
async def test_claimed_switch_is_not_retried_in_same_minute(tmp_path, reload_book):
    controller, store, _operation = _controller(tmp_path)
    source = _profile(tmp_path, ProfileId.MINECRAFT)
    target = _profile(tmp_path, ProfileId.PZ_RISING)
    controller.profiles = {profile.id: profile for profile in (source, target)}
    entries = parse_schedule([{"cron": "* * * * *", "profile": target.id.value}])
    controller._schedule = ScheduleBook(entries)
    stops = []

    class Adapter:
        async def graceful_stop(self, profile):
            stops.append(profile.id)
            raise RuntimeError("simulated stop failure leaves the source running")

    controller.adapters = {source.id: Adapter()}
    controller.await_free_slot = lambda: False
    snapshot = _running_snapshot(source)
    await controller._apply_schedules(snapshot, uuid4())
    assert stops == [source.id]
    assert store.read() is None
    if reload_book:
        # Schedule edits and daemon startup build a fresh ScheduleBook while
        # the durable events table still contains this minute's fire claim.
        controller._schedule = ScheduleBook(entries)
    second_tick = snapshot.model_copy(update={
        "observed_at": snapshot.observed_at + timedelta(seconds=30),
    })
    await controller._apply_schedules(second_tick, uuid4())

    assert stops == [source.id], "a claimed switch must not run again in the same minute"
    assert controller._db().execute(
        "SELECT state FROM jobs WHERE operation='switch'"
    ).fetchall() == [("failed",)]
