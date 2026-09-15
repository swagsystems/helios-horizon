"""Public state derivation and status snapshots."""

from __future__ import annotations

import asyncio
import inspect
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping

import psutil

from .adapters.base import AdapterError
from .adapters.crafty import parse_version_text
from .benchmark_safety import BenchmarkPreflight
from .models import HealthState, ObservedState
from .introspection import signature_parameters
from .protocol import ProfileStatus, StatusSnapshot, UpdateActivity


def _expiry_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


# Slotd owns telemetry cadence separately from its 30-second maintenance tick.
# Keep the historical constants for compatibility with callers that import
# them, but do not use them as a write gate: TelemetrySampler is the sole
# cadence owner.
TELEMETRY_TARGET_INTERVAL_SECONDS = 30.0
TELEMETRY_MIN_INTERVAL_SECONDS = 20.0
TELEMETRY_MAX_INTERVAL_SECONDS = 45.0

# Browser/API status demand shares a short-lived projection.  Maintenance and
# lifecycle preflights can explicitly bypass it.
STATUS_SNAPSHOT_TTL_SECONDS = 2.0


def derive_state(
    *,
    active_job: str | None,
    process_alive: bool,
    conflicting_slot_owner: bool | str | None,
) -> ObservedState:
    """Apply the closed state precedence contract.

    Process state is independent from health: a live process remains RUNNING
    even if its health adapter reports UNHEALTHY.
    """

    job = getattr(active_job, "value", active_job)
    if job == "start":
        return ObservedState.STARTING
    if job == "stop":
        return ObservedState.STOPPING
    if job == "failed":
        return ObservedState.FAILED
    if process_alive:
        return ObservedState.RUNNING
    if conflicting_slot_owner:
        return ObservedState.BLOCKED
    return ObservedState.STOPPED


class StatusService:
    def __init__(
        self,
        profiles: Iterable[Any],
        *,
        adapters: Mapping[Any, Any] | None = None,
        adapter: Any | None = None,
        slot_observer: Callable[[], Any] | None = None,
        active_jobs: Mapping[Any, str | None] | Callable[[Any], str | None] | None = None,
        health_checker: Any | None = None,
        metrics: Any | None = None,
        player_tracker: Any | None = None,
        session_store: Any | None = None,
        connection_provider: Callable[..., Any] = psutil.net_connections,
        generation: int | Callable[[], int] = 0,
        clock: Callable[[], datetime] | None = None,
        telemetry_db: Any | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        telemetry_sampler: Any | None = None,
        telemetry_health_provider: Callable[[], Mapping[str, Any]] | None = None,
        capability_evidence: Callable[[], bool] | None = None,
        ups_health: Callable[[], bool] | None = None,
        benchmark_safety: BenchmarkPreflight | None = None,
        storage_paths: Iterable[str] = ("/srv/game-servers", "/var/lib/game-control"),
        update_reservations: Callable[[Any], Any] | None = None,
    ):
        self.profiles = tuple(profiles)
        self.adapters = adapters or {}
        self.default_adapter = adapter
        self.slot_observer = slot_observer
        self.active_jobs = active_jobs or {}
        self.health_checker = health_checker
        self.metrics = metrics
        self.player_tracker = player_tracker
        self.session_store = session_store
        self.connection_provider = connection_provider
        self.generation = generation
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._last_jobs: dict[Any, str | None] = {}
        self._last_running: dict[str, bool] = {}
        self._version_cache: dict[str, tuple[tuple[int, int], str | None]] = {}
        self._snapshot_cache: StatusSnapshot | None = None
        self._snapshot_cached_at: float | None = None
        self._refresh_lock = asyncio.Lock()
        self.telemetry_db = telemetry_db
        self.telemetry_sampler = telemetry_sampler
        self.telemetry_health_provider = telemetry_health_provider
        self.capability_evidence = capability_evidence
        self.ups_health = ups_health
        self.benchmark_safety = benchmark_safety or BenchmarkPreflight(
            storage_paths=tuple(storage_paths),
            ups_health=ups_health,
            session_store=session_store,
            wake_evidence=capability_evidence,
            clock=self.clock,
        )
        self.storage_paths = tuple(storage_paths)
        # Root-owned projection of the live updater handoff reservation.  A
        # missing provider simply means "no reservation evidence".
        self.update_reservations = update_reservations
        # Production maintenance is fixed at a 30-second target. Keep this
        # contract non-configurable so callers cannot create 60-second aliasing
        # or intervals outside the documented 20–45 second envelope.
        self.telemetry_min_interval_seconds = TELEMETRY_MIN_INTERVAL_SECONDS
        self._monotonic = monotonic
        self._telemetry_last_sample: dict[str, float] = {}

    async def snapshot(self, *, persist: bool = False, force: bool = False) -> StatusSnapshot:
        """Return one short-lived projection, with one in-flight probe.

        The lock is held through the probe so concurrent demand observes the
        just-published result instead of starting a second adapter probe.
        Lifecycle gates use ``force`` when stale status is unacceptable.
        """
        async with self._refresh_lock:
            # Persistence cadence already owns its own monotonic clock read;
            # only demand snapshots participate in the short TTL cache.
            now = self._monotonic() if not persist else None
            if (
                not persist
                and not force
                and self._snapshot_cache is not None
                and self._snapshot_cached_at is not None
                and now is not None
                and now - self._snapshot_cached_at < STATUS_SNAPSHOT_TTL_SECONDS
            ):
                return self._snapshot_cache
            async def supervise_sample() -> tuple[bool, StatusSnapshot | BaseException]:
                try:
                    return True, await self._sample(persist=persist)
                except BaseException as error:
                    # Keep the shielded task itself non-throwing.  A provider
                    # can fail after its caller is cancelled; storing the
                    # outcome prevents Python from reporting a late shield
                    # Future exception while preserving normal error behavior.
                    return False, error

            sample_task = asyncio.create_task(supervise_sample())
            try:
                ok, outcome = await asyncio.shield(sample_task)
            except asyncio.CancelledError:
                # Do not release the single-flight lock while a worker thread
                # is still probing external state. Await completion so a
                # subsequent refresh cannot overlap or publish stale output.
                # Drain both futures: a provider may fail after cancellation,
                # but shutdown must retain the caller's CancelledError and
                # never leave an unhandled task exception behind.
                try:
                    await sample_task
                except BaseException:
                    pass
                raise
            if not ok:
                raise outcome
            snapshot = outcome
            self._snapshot_cache = snapshot
            # Anchor the short demand TTL at probe start.  This avoids a
            # second clock read (important for deterministic injected clocks)
            # and never extends freshness across a slow observation.
            self._snapshot_cached_at = now
            return snapshot

    async def cached_snapshot(self) -> StatusSnapshot:
        """Return the last sampled projection without running probes."""
        cached = self._snapshot_cache
        if cached is not None:
            # Lifecycle intent is root-owned and cheap to project.  Overlay it
            # on the last sample so a slow adapter/RCON probe cannot make a
            # newly accepted start look stopped.  Do not infer health, PID, or
            # players here: those remain exactly as last observed (or None).
            return self._overlay_active_jobs(cached)
        # Pure API reads must not unexpectedly probe adapters or enqueue
        # telemetry.  The slotd sampler/maintenance path is responsible for
        # establishing the first authoritative snapshot.
        now = self.clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        generation = self.generation() if callable(self.generation) else self.generation
        # A cold cached read is deliberately projection-only: do not perform
        # version-file or adapter probes while maintenance is in flight.
        profile_keys = tuple(
            (getattr(profile, "id"), getattr(getattr(profile, "id"), "value", getattr(profile, "id")))
            for profile in self.profiles
        )
        raw_jobs = tuple(self._raw_job_for(profile_id, key) for profile_id, key in profile_keys)
        jobs = tuple(job if job in {"start", "stop", "failed"} else None for job in raw_jobs)
        versions = tuple(
            parse_version_text(value) if isinstance(value := getattr(profile, "installed_version", None), str) and value else None
            for profile in self.profiles
        )
        profiles = tuple(
            ProfileStatus(
                profile_id=profile_id,
                state=derive_state(active_job=job, process_alive=False, conflicting_slot_owner=False),
                health=HealthState.UNKNOWN, slot_owner=None,
                active_job_id=job,
                pid=None, started_at=None, uptime_seconds=None, cpu_percent=None,
                rss_bytes=None, players_online=None,
                installed_version=version,
                restart_required=False, required_ports_ready=False,
                update=self._update_activity(profile_id, key, raw_job),
            )
            for version, (profile_id, key), job, raw_job in zip(
                versions, profile_keys, jobs, raw_jobs
            )
        )
        return StatusSnapshot(generation=int(generation), observed_at=now, profiles=profiles)

    def _safe_job_for(self, profile_id: Any, key: Any) -> str | None:
        value = self._raw_job_for(profile_id, key)
        return value if value in {"start", "stop", "failed"} else None

    def _raw_job_for(self, profile_id: Any, key: Any) -> str | None:
        """Fail-safe raw job read: an unreadable job table never breaks status."""
        try:
            value = self._job_for(profile_id, key)
        except Exception:
            return None
        return value if isinstance(value, str) and value else None

    def _update_activity(self, profile_id: Any, key: Any, job: str | None) -> UpdateActivity | None:
        """Project authoritative update activity from root-owned records.

        The live updater reservation (``operation_kind="update"``) wins because
        it carries the operation id and expiry.  An accepted/running controller
        job whose operation is ``update`` is the fallback.  Backup, benchmark or
        lifecycle jobs never produce an update record.
        """
        provider = self.update_reservations
        if provider is not None:
            try:
                reservation = provider(profile_id)
            except Exception:
                reservation = None
            if reservation is not None and getattr(reservation, "operation_kind", None) == "update":
                operation_id = getattr(reservation, "operation_id", None)
                return UpdateActivity(
                    source="reservation",
                    operation_id=operation_id if isinstance(operation_id, str) and operation_id else None,
                    expires_at=_expiry_datetime(getattr(reservation, "expires_at", None)),
                )
        if job == "update":
            return UpdateActivity(source="job")
        return None

    def _overlay_active_jobs(self, snapshot: StatusSnapshot) -> StatusSnapshot:
        """Project accepted/running lifecycle intent without external probes."""
        profiles: list[ProfileStatus] = []
        changed = False
        for status in snapshot.profiles:
            key = getattr(status.profile_id, "value", status.profile_id)
            raw_job = self._raw_job_for(status.profile_id, key)
            job = self._safe_job_for(status.profile_id, key)
            update = self._update_activity(status.profile_id, key, raw_job)
            if update != status.update:
                status = status.model_copy(update={"update": update})
                changed = True
            if job is None:
                profiles.append(status)
                continue
            state = derive_state(active_job=job, process_alive=False, conflicting_slot_owner=False)
            if status.active_job_id == job and status.state is state:
                profiles.append(status)
                continue
            profiles.append(status.model_copy(update={"state": state, "active_job_id": job}))
            changed = True
        return snapshot if not changed else snapshot.model_copy(update={"profiles": tuple(profiles)})

    async def benchmark_eligibility(
        self,
        *,
        maintenance_window: bool,
        rollback_safe: bool,
        public_wake_policy: str,
        snapshot: StatusSnapshot | None = None,
    ) -> dict[str, bool]:
        """Bounded, secret-free evidence for scheduled benchmark execution."""
        # Benchmark safety cannot be satisfied by a stopped projection cached
        # before a profile starts.  Maintenance already owns a fresh,
        # non-persisting sample; callers pass it through to avoid a duplicate
        # sampler probe.  Direct callers force the same non-persisting probe.
        if snapshot is None:
            snapshot = await self.snapshot(persist=False, force=True)
        evidence = await self.benchmark_safety.evaluate(
            snapshot=snapshot,
            maintenance_window=maintenance_window,
            rollback_safe=rollback_safe,
            public_wake_policy=public_wake_policy,
        )
        return evidence.legacy_mapping()

    async def _sample(self, *, persist: bool = False) -> StatusSnapshot:
        now = self.clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        slot = await self._call(self.slot_observer) if self.slot_observer else None
        owner = getattr(slot, "owner", None)
        owner_value = getattr(owner, "value", owner)
        contexts = []
        for profile in self.profiles:
            profile_id = getattr(profile, "id", None)
            key = getattr(profile_id, "value", profile_id)
            job = self._raw_job_for(profile_id, key)
            full_probe = (
                self.slot_observer is None
                or job is not None
                or bool(owner_value and owner_value == key)
            )
            contexts.append((profile, profile_id, key, job, full_probe))
        invalidate = getattr(self.metrics, "invalidate_cgroup", None)
        if callable(invalidate):
            for profile, _profile_id, key, job, _full_probe in contexts:
                previous_job = self._last_jobs.get(key, object())
                if previous_job != job and job in {"start", "stop"}:
                    await self._call(invalidate, getattr(profile, "systemd_unit", None))
                self._last_jobs[key] = job
        connections = await self._connections_for_snapshot(
            profile for profile, _profile_id, _key, _job, full_probe in contexts if full_probe
        )
        statuses: list[ProfileStatus] = []
        for profile, profile_id, key, job, full_probe in contexts:
            conflicting = bool(owner_value and owner_value != key)
            if not full_probe:
                cached_disk = None
                cached_disk_provider = getattr(self.metrics, "cached_disk_metrics", None) if self.metrics is not None else None
                if callable(cached_disk_provider):
                    cached_disk = await self._call(cached_disk_provider, profile)
                statuses.append(
                    ProfileStatus(
                        profile_id=profile_id,
                        state=derive_state(
                            active_job=job,
                            process_alive=False,
                            conflicting_slot_owner=conflicting,
                        ),
                        health=HealthState.UNKNOWN,
                        slot_owner=owner if owner_value else None,
                        active_job_id=job,
                        pid=None,
                        started_at=None,
                        uptime_seconds=None,
                        cpu_percent=None,
                        rss_bytes=None,
                        players_online=None,
                        installed_version=await self._cached_installed_version(profile),
                        restart_required=False,
                        required_ports_ready=False,
                        disk_free_bytes=getattr(cached_disk, "profile_data_free_bytes", None),
                        disk_read_bps=None,
                        disk_write_bps=None,
                        update=self._update_activity(profile_id, key, job),
                    )
                )
                if persist:
                    self._record_telemetry(profile_id, None, now=now, state="inactive")
                continue
            adapter = self.adapters.get(profile_id, self.adapters.get(key, self.default_adapter))
            observation_error = False
            try:
                observation = await self._observe(adapter, profile)
            except (AdapterError, RuntimeError):
                observation_error = True
                observation = type("Observation", (), {"running": False, "healthy": None})()
            running = bool(getattr(observation, "running", False))
            if not observation_error and not running:
                if persist and self.session_store is not None and self._last_running.get(key, False):
                    self.session_store.profile_stopped(key, now=_iso(now))
                if self.player_tracker is not None:
                    reset = getattr(self.player_tracker, "reset", None)
                    if callable(reset):
                        # PlayerTracker state is owned by the event loop and is
                        # updated by ingest_event on that same loop.  Keep
                        # reset serialized with those mutations; only
                        # genuinely blocking injected probes use _call.
                        reset(key)
            self._last_running[key] = running if not observation_error else self._last_running.get(key, False)
            process_alive = False
            health = None
            required_ports = getattr(observation, "required_ports_ready", None)
            health_error = False
            sampled = None
            # Avoid process/cgroup probes for stopped profiles.  Inactive
            # samples are represented explicitly below and the sampler can
            # continue collecting other profiles independently.
            if running and self.metrics is not None:
                sampled = await self._call_optional(
                    self.metrics.sample,
                    profile,
                    pid=getattr(observation, "pid", None),
                    connections=connections,
                )
            if self.health_checker is not None and not observation_error:
                checker = self.health_checker
                if isinstance(checker, Mapping):
                    checker = checker.get(profile_id, checker.get(key))
                if checker is not None:
                    try:
                        result = await self._call_optional(
                            checker.check,
                            profile,
                            observation=observation,
                            connections=connections,
                            process_metrics=sampled,
                        )
                    except (AdapterError, RuntimeError):
                        health_error = True
                    else:
                        health = getattr(result, "state", result)
                        required_ports = getattr(result, "required_ports", required_ports)
                        validated_alive = getattr(result, "process_alive", None)
                        if isinstance(validated_alive, bool):
                            process_alive = validated_alive
            state = derive_state(
                active_job=job,
                process_alive=process_alive,
                conflicting_slot_owner=conflicting,
            )
            if health is None:
                observed_health = None if observation_error or health_error else getattr(observation, "healthy", None)
                health = (
                    HealthState.HEALTHY
                    if observed_health is True
                    else HealthState.UNHEALTHY
                    if observed_health is False
                    else HealthState.UNKNOWN
                )
            health = HealthState(getattr(health, "value", health))
            pid = getattr(sampled, "pid", None)
            rss = getattr(sampled, "rss_bytes", None)
            cpu = getattr(sampled, "cpu_percent", None)
            started_at = getattr(observation, "started_at", None)
            uptime = None
            if started_at is not None:
                try:
                    uptime = max(0, int((now - started_at).total_seconds()))
                except (TypeError, ValueError):
                    uptime = None
            players = getattr(observation, "players_online", None)
            if players is None and self.player_tracker is not None:
                players = await self._call(
                    self.player_tracker.count,
                    profile,
                    adapter,
                    running=bool(getattr(observation, "running", False)),
                )
            if persist and running and self.session_store is not None:
                names = getattr(observation, "player_names", None)
                if names is None and self.player_tracker is not None:
                    names = self.player_tracker.names(key)
                source = "crafty" if getattr(getattr(profile, "adapter", None), "value", getattr(profile, "adapter", None)) == "crafty" else "log"
                self.session_store.record(
                    key,
                    set(names) if names is not None else None,
                    players,
                    now=_iso(now),
                    source=source,
                )
            version = getattr(observation, "installed_version", None)
            if version is None:
                version = await self._cached_installed_version(profile)
            telemetry_state = "inactive" if not running else "available" if sampled is not None and any(
                getattr(sampled, name, None) is not None
                for name in ("cpu_percent", "rss_bytes", "disk_read_bps", "disk_write_bps")
            ) else "unavailable"
            if persist:
                self._record_telemetry(profile_id, sampled, now=now, state=telemetry_state)
            statuses.append(
                ProfileStatus(
                    profile_id=profile_id,
                    state=state,
                    health=health,
                    slot_owner=owner if owner_value else None,
                    active_job_id=job,
                    pid=pid,
                    started_at=started_at,
                    uptime_seconds=uptime,
                    cpu_percent=cpu,
                    rss_bytes=rss,
                    players_online=players,
                    installed_version=version,
                    restart_required=False,
                    required_ports_ready=bool(required_ports),
                    disk_free_bytes=getattr(getattr(sampled, "disk", None), "profile_data_free_bytes", None),
                    disk_read_bps=getattr(sampled, "disk_read_bps", None),
                    disk_write_bps=getattr(sampled, "disk_write_bps", None),
                    update=self._update_activity(profile_id, key, job),
                )
            )
        generation = self.generation() if callable(self.generation) else self.generation
        return StatusSnapshot(generation=int(generation), observed_at=now, profiles=tuple(statuses))

    def _record_telemetry(self, profile_id: Any, sample: Any | None, *, now: datetime, state: str) -> None:
        if self.telemetry_db is None:
            return
        key = getattr(profile_id, "value", profile_id)
        current = self._monotonic()
        ts_ms = int(now.timestamp() * 1000)
        try:
            enqueue = getattr(self.telemetry_db, "enqueue_process_sample", None)
            if callable(enqueue):
                accepted = enqueue(profile_id, sample, ts_ms=ts_ms, state=state)
                if accepted:
                    self._telemetry_last_sample[str(key)] = current
            else:
                recorder = getattr(self.telemetry_db, "record_failure", None)
                if callable(recorder):
                    recorder(RuntimeError("telemetry writer enqueue interface is required"))
        except Exception as error:
            recorder = getattr(self.telemetry_db, "record_failure", None)
            if callable(recorder):
                recorder(error)

    def telemetry_health(self) -> dict[str, Any]:
        if self.telemetry_health_provider is not None:
            try:
                return dict(self.telemetry_health_provider())
            except Exception as error:
                return {"ok": False, "last_sample_age_ms": None, "last_error": type(error).__name__[:64]}
        if self.telemetry_db is None:
            health: dict[str, Any] = {
                "ok": False,
                "last_sample_age_ms": None,
                "last_error": "not_configured",
            }
            sampler = self.telemetry_sampler
            if sampler is not None and hasattr(sampler, "health"):
                health["sampler"] = dict(sampler.health())
            collectors = getattr(self, "telemetry_collectors", None)
            if collectors is not None and hasattr(collectors, "health"):
                health["collectors"] = dict(collectors.health())
            return health
        try:
            health = dict(self.telemetry_db.health())
            sampler = self.telemetry_sampler
            if sampler is not None and hasattr(sampler, "health"):
                health["sampler"] = dict(sampler.health())
            collectors = getattr(self, "telemetry_collectors", None)
            if collectors is not None and hasattr(collectors, "health"):
                health["collectors"] = dict(collectors.health())
            return health
        except Exception as error:
            return {"ok": False, "last_sample_age_ms": None, "last_error": type(error).__name__[:64]}

    def _job_for(self, profile_id: Any, key: Any) -> str | None:
        if callable(self.active_jobs):
            return self.active_jobs(profile_id)
        value = self.active_jobs.get(profile_id, self.active_jobs.get(key))
        if isinstance(value, Mapping):
            value = value.get("operation", value.get("state"))
        return getattr(value, "value", value)

    async def _cached_installed_version(self, profile: Any) -> str | None:
        direct = getattr(profile, "installed_version", None)
        if isinstance(direct, str) and direct:
            return parse_version_text(direct)
        paths = getattr(profile, "paths", None)
        raw_path = getattr(paths, "version_file", None) if paths is not None else None
        if raw_path is None:
            return None
        path = Path(raw_path)
        key = str(path)
        try:
            stat = await asyncio.to_thread(path.stat)
            fingerprint = (int(stat.st_mtime_ns), int(stat.st_size))
        except OSError:
            return None
        cached = self._version_cache.get(key)
        if cached is not None and cached[0] == fingerprint:
            return cached[1]
        try:
            value = parse_version_text(await asyncio.to_thread(path.read_text, encoding="utf-8"))
        except OSError:
            value = None
        self._version_cache[key] = (fingerprint, value)
        return value

    async def _observe(self, adapter: Any, profile: Any) -> Any:
        if adapter is None:
            return type("Observation", (), {"running": False, "healthy": None})()
        return await self._call(adapter.observe, profile)

    async def _connections_for_snapshot(self, profiles: Iterable[Any]) -> dict[str, list[Any]]:
        protocols = {
            getattr(spec, "protocol", None)
            for profile in profiles
            for spec in tuple(getattr(profile, "ports", ()))[:16]
            if getattr(spec, "protocol", None) in {"tcp", "udp"}
        }
        rows: dict[str, list[Any]] = {}
        for protocol in ("tcp", "udp"):
            if protocol not in protocols:
                continue
            try:
                value = await self._call(self.connection_provider, kind=protocol)
                rows[protocol] = list(value)
            except (OSError, psutil.Error, TypeError, ValueError):
                rows[protocol] = []
        return rows

    async def _call_optional(self, function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            parameters = signature_parameters(function)
            accepts_kwargs = any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters)
            supported = {parameter.name for parameter in parameters}
        except (TypeError, ValueError):
            accepts_kwargs = True
            supported = set()
        filtered = kwargs if accepts_kwargs else {key: value for key, value in kwargs.items() if key in supported}
        return await self._call(function, *args, **filtered)

    @staticmethod
    async def _call(function: Callable[..., Any] | None, *args: Any, **kwargs: Any) -> Any:
        if function is None:
            return None
        callable_async = inspect.iscoroutinefunction(function) or inspect.iscoroutinefunction(getattr(function, "__call__", None))
        value = function(*args, **kwargs) if callable_async else await asyncio.to_thread(function, *args, **kwargs)
        return await value if inspect.isawaitable(value) else value


derive_observed_state = derive_state

__all__ = ["StatusService", "derive_state", "derive_observed_state"]


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
