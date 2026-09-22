"""Serialized root-side RPC controller.

Only the narrow typed protocol reaches this module.  Heavy work is performed
after the short reservation/state transaction and never while the operation
lock is held.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import math
import os
import sqlite3
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping
from uuid import UUID, uuid4

from .adapters.base import Adapter, AdapterError
from .errors import SafeError
from .health import ReadinessCoordinator, ReadinessOutcome
from .managed_tuning import readiness_event
from .idle_stop import IdleStopTracker
from .models import BackupDestination, NotificationEvent, OperationName, Profile, ProfileId
from .schedule import ScheduleBook, ScheduleEntry, parse_schedule
from .schedule_config import ScheduleConfigError, write_schedule_config
from .profile_config import ConfigValidationError, get_profile_config, set_profile_config
from .protocol import (
    AuditPage,
    BackupPage,
    CheckUpdate,
    Command,
    ConfirmForceStop,
    ConfirmRestore,
    ConfirmSwitch,
    ConfirmUpdate,
    ConfirmWorldClone,
    ConfirmationSummary,
    CreateBackup,
    ErrorCode,
    EventPage,
    GetPerf,
    GetLogs,
    GetNotificationConfig,
    GetProfiles,
    GetStatus,
    Watch,
    WaitReadiness,
    GetStatsHeatmap,
    GetStatsSummary,
    GetStatsTps,
    GetProfileConfig,
    GetBenchmarks,
    ExportBenchmarks,
    BenchmarkExport,
    GetSchedules,
    ScheduleResponse,
    ScheduleView,
    SetSchedules,
    SetProfileConfig,
    JobAccepted,
    ReadinessResult,
    ListAudit,
    ListBackups,
    ListAggregateBackups,
    ListEvents,
    GetRetirementStatus,
    PrepareRetirement,
    ConfirmRetirement,
    RetirementConfirmation,
    LogPage,
    NotificationConfig,
    PrepareForceStop,
    PrepareRestore,
    PrepareSwitch,
    PrepareUpdate,
    PrepareWorldClone,
    PageOptions,
    PerfSnapshot,
    PublicProfile,
    PublicEndpoint,
    Restart,
    RunBenchmark,
    CancelBenchmark,
    RpcAction,
    RpcFailure,
    RpcProvenance,
    RpcRequest,
    RpcResponse,
    RpcSuccess,
    SafeDetails,
    Start,
    StartupEstimate,
    StatusSnapshot,
    ProfileConfigEntry,
    ProfileConfigResponse,
    BenchmarkOverview,
    Stop,
    SetIdleStop,
    SwitchOptions,
    SwitchConfirmation,
    ForceStopConfirmation,
    RestoreConfirmation,
    UpdateConfirmation,
    UpdateStatus,
    WorldCloneConfirmation,
    MAX_RESPONSE_BYTES,
    response_from_json,
    response_json,
    success,
    failure,
)
from .slot import SlotInspector, OperationLock
from .perf import PerformanceTracker
from .db_telemetry import collect_perf_databases_async
from .introspection import signature_parameters
from .runtime.protocols import AlertObservation
from .startup_estimates import (
    StartupAttempt,
    StartupEstimateStore,
    StartupEstimateSummary,
    installed_version_for_profile,
    normalize_profile_key,
)

_LOG = logging.getLogger(__name__)

# Bounded wait for the reviewed version-file read; a slow or hostile path only
# loses the estimate hint.
_STARTUP_VERSION_HINT_TIMEOUT_SECONDS = 2.0


DISPATCH: dict[type, str] = {
    GetStatus: "_get_status",
    WaitReadiness: "_wait_readiness",
    GetPerf: "_get_perf",
    GetProfiles: "_get_profiles",
    GetLogs: "_get_logs",
    ListBackups: "_list_backups",
    ListAggregateBackups: "_list_aggregate_backups",
    ListEvents: "_list_events",
    GetStatsSummary: "_get_stats_summary",
    GetStatsHeatmap: "_get_stats_heatmap",
    GetStatsTps: "_get_stats_tps",
    GetProfileConfig: "_get_profile_config",
    GetBenchmarks: "_get_benchmarks",
    ExportBenchmarks: "_export_benchmarks",
    RunBenchmark: "_run_benchmark",
    CancelBenchmark: "_cancel_benchmark",
    SetProfileConfig: "_set_profile_config",
    GetSchedules: "_get_schedules",
    SetSchedules: "_set_schedules",
    ListAudit: "_list_audit",
    Start: "_start",
    Stop: "_stop",
    Restart: "_restart",
    PrepareSwitch: "_prepare_switch",
    ConfirmSwitch: "_confirm_switch",
    PrepareForceStop: "_prepare_force_stop",
    ConfirmForceStop: "_confirm_force_stop",
    CreateBackup: "_create_backup",
    PrepareRestore: "_prepare_restore",
    ConfirmRestore: "_confirm_restore",
    PrepareWorldClone: "_prepare_world_clone",
    ConfirmWorldClone: "_confirm_world_clone",
    CheckUpdate: "_check_update",
    PrepareUpdate: "_prepare_update",
    ConfirmUpdate: "_confirm_update",
    GetNotificationConfig: "_get_notification_config",
    GetRetirementStatus: "_get_retirement_status",
    PrepareRetirement: "_prepare_retirement",
    ConfirmRetirement: "_confirm_retirement",
    Command: "_command",
    # Notification implementations belong to later tasks; these are typed
    # seams and deliberately have no arbitrary service dispatch.
    # The concrete action classes are still present in this total map.
}
# Keep the complete map explicit, including later-task service seams.
from .protocol import SetNotificationRule, TestNotification

DISPATCH.update({
    SetNotificationRule: "_set_notification_rule",
    TestNotification: "_test_notification",
    SetIdleStop: "_set_idle_stop",
})


class _ActionClass(StrEnum):
    PURE_READ = "pure_read"
    MUTATION = "mutation"


# Concrete action types, rather than caller-controlled kind strings, determine
# whether a request participates in durable idempotency. Unknown action types
# stay on the mutation path until deliberately classified.
ACTION_CLASSES: dict[type, _ActionClass] = {
    GetStatus: _ActionClass.PURE_READ,
    WaitReadiness: _ActionClass.PURE_READ,
    GetPerf: _ActionClass.PURE_READ,
    GetProfiles: _ActionClass.PURE_READ,
    GetLogs: _ActionClass.PURE_READ,
    ListBackups: _ActionClass.PURE_READ,
    ListAggregateBackups: _ActionClass.PURE_READ,
    ListEvents: _ActionClass.PURE_READ,
    GetStatsSummary: _ActionClass.PURE_READ,
    GetStatsHeatmap: _ActionClass.PURE_READ,
    GetStatsTps: _ActionClass.PURE_READ,
    GetProfileConfig: _ActionClass.PURE_READ,
    GetBenchmarks: _ActionClass.PURE_READ,
    ExportBenchmarks: _ActionClass.PURE_READ,
    GetSchedules: _ActionClass.PURE_READ,
    ListAudit: _ActionClass.PURE_READ,
    GetNotificationConfig: _ActionClass.PURE_READ,
    RunBenchmark: _ActionClass.MUTATION,
    CancelBenchmark: _ActionClass.MUTATION,
    SetSchedules: _ActionClass.MUTATION,
    SetProfileConfig: _ActionClass.MUTATION,
    Start: _ActionClass.MUTATION,
    Stop: _ActionClass.MUTATION,
    Restart: _ActionClass.MUTATION,
    PrepareSwitch: _ActionClass.MUTATION,
    ConfirmSwitch: _ActionClass.MUTATION,
    PrepareForceStop: _ActionClass.MUTATION,
    ConfirmForceStop: _ActionClass.MUTATION,
    CreateBackup: _ActionClass.MUTATION,
    PrepareRestore: _ActionClass.MUTATION,
    ConfirmRestore: _ActionClass.MUTATION,
    PrepareWorldClone: _ActionClass.MUTATION,
    ConfirmWorldClone: _ActionClass.MUTATION,
    CheckUpdate: _ActionClass.MUTATION,
    PrepareUpdate: _ActionClass.MUTATION,
    ConfirmUpdate: _ActionClass.MUTATION,
    SetNotificationRule: _ActionClass.MUTATION,
    TestNotification: _ActionClass.MUTATION,
    SetIdleStop: _ActionClass.MUTATION,
    GetRetirementStatus: _ActionClass.PURE_READ,
    PrepareRetirement: _ActionClass.MUTATION,
    ConfirmRetirement: _ActionClass.MUTATION,
    Command: _ActionClass.MUTATION,
}

# ``Watch`` is a long-lived socket protocol handled by slotd_main after peer
# authorization; it is intentionally not a controller dispatch action. Keep
# that exception explicit so a future RpcAction cannot silently bypass the
# controller exhaustiveness gate.
STREAM_ACTIONS = frozenset({Watch})


def _action_types() -> set[type]:
    from typing import Annotated, get_args, get_origin

    args = get_args(RpcAction)
    union = args[0] if args and get_origin(RpcAction) is Annotated else RpcAction
    return set(get_args(union))


def dispatch_is_exhaustive() -> bool:
    action_types = _action_types()
    controller_actions = action_types - STREAM_ACTIONS
    return controller_actions == set(DISPATCH) == set(ACTION_CLASSES) and STREAM_ACTIONS <= action_types


def _action_class(action: Any) -> _ActionClass:
    return ACTION_CLASSES.get(type(action), _ActionClass.MUTATION)


class _MemoryLock:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class _MemoryDb:
    def __init__(self, path: Path):
        self.path = path
        self.connection = sqlite3.connect(":memory:", check_same_thread=False)
        self.connection.executescript(
            """
            CREATE TABLE rpc_idempotency(request_id TEXT PRIMARY KEY, canonical_request TEXT NOT NULL, response TEXT, status TEXT NOT NULL DEFAULT 'completed', created_at TEXT NOT NULL);
            CREATE TABLE jobs(id TEXT PRIMARY KEY, profile_id TEXT, operation TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, finished_at TEXT, completion_seq INTEGER, detail TEXT);
            CREATE TABLE audit(id TEXT PRIMARY KEY, timestamp TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL, profile_id TEXT, result TEXT NOT NULL, error_code TEXT, detail TEXT NOT NULL);
            CREATE TABLE events(id TEXT PRIMARY KEY, timestamp TEXT NOT NULL, profile_id TEXT, code TEXT NOT NULL, message TEXT NOT NULL);
            CREATE TABLE confirmations(id TEXT PRIMARY KEY, actor TEXT NOT NULL, action TEXT NOT NULL, profile_id TEXT, payload TEXT NOT NULL, expires_at TEXT NOT NULL, consumed_at TEXT, state_generation INTEGER NOT NULL DEFAULT 0);
            """
        )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class _PendingReplay:
    pass


_PENDING = _PendingReplay()


class Controller:
    def __init__(
        self,
        profiles: Any | None = None,
        state_db: Any | None = None,
        reservation_store: Any | None = None,
        adapters: Mapping[ProfileId | str, Adapter] | None = None,
        *,
        await_ready: Callable[[Profile], Awaitable[Any] | Any] | None = None,
        await_free_slot: Callable[[], Awaitable[Any] | Any] | None = None,
        services: Any | None = None,
        slot_inspector: SlotInspector | None = None,
        operation_lock_factory: Callable[[], Any] | None = None,
        clock: Callable[[], datetime] = _utcnow,
        performance: PerformanceTracker | None = None,
        boot_profile: str | None = None,
        boot_autostart: bool = False,
        schedules: tuple[ScheduleEntry, ...] = (),
        schedule_config_path: str | os.PathLike[str] | None = None,
    ):
        if state_db is None and profiles is not None and operation_lock_factory is None:
            raise ValueError("production Controller requires a root StateDatabase")
        self.profiles = profiles
        self.state_db = state_db or _MemoryDb(Path(":memory:"))
        self.reservation_store = reservation_store
        self.adapters = adapters or {}
        self.await_ready = await_ready or (lambda _profile: False)
        self.await_free_slot = await_free_slot or (lambda: True)
        self.services = services
        self.slot_inspector = slot_inspector
        self._operation_lock_factory = operation_lock_factory or (lambda: OperationLock())
        self._clock = clock
        self._idle_stop = IdleStopTracker(clock=self._clock)
        self._schedule = ScheduleBook(schedules)
        self.schedule_config_path = Path(schedule_config_path) if schedule_config_path is not None else None
        self.performance = performance or PerformanceTracker()
        self.boot_profile = boot_profile
        self.boot_autostart = boot_autostart
        self._boot_autostart_attempted = False
        self._transition_lock = asyncio.Lock()
        self._maintenance_lock = asyncio.Lock()
        self.initializing = True
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._background_close_task: asyncio.Task[None] | None = None
        self._background_closed = False
        self._readiness = ReadinessCoordinator()
        # Experimental startup estimates are controller-owned and boundary
        # safe: a missing history or additive table degrades to "no estimate"
        # and never changes lifecycle or slot authority.
        self._startup_estimates = StartupEstimateStore(self._db)
        self._startup_attempts: dict[str, StartupAttempt] = {}
        try:
            self._startup_estimates.ensure_schema()
        except Exception:
            _LOG.debug("startup estimate schema unavailable", exc_info=True)

    @staticmethod
    def _consume_background_task(task: asyncio.Task[Any]) -> None:
        """Observe terminal workers without releasing their close handles."""

        try:
            task.result()
        except asyncio.CancelledError:
            return
        except BaseException:
            # The close ledger observes the same task again.  Consuming here
            # prevents an unexpected worker exit from becoming an event-loop
            # warning while retaining the handle for deterministic draining.
            _LOG.error("controller background worker failed", exc_info=True)

    @staticmethod
    def _consume_background_close_task(task: asyncio.Task[Any]) -> None:
        try:
            task.exception()
        except BaseException:
            return

    async def _close_background_tasks(self) -> None:
        workers = tuple(self._background_tasks)
        for worker in workers:
            if not worker.done():
                worker.cancel()
        if not workers:
            return
        results = await asyncio.gather(*workers, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                raise result

    async def aclose(self) -> None:
        """Cancel and drain every accepted worker exactly once.

        Worker finally blocks own reservation release, so the close operation
        waits for those blocks before returning.  The internal close task is
        shielded from caller cancellation and therefore remains retryable and
        warning-free.
        """

        self._background_closed = True
        task = self._background_close_task
        task_succeeded = False
        if task is not None and task.done() and not task.cancelled():
            try:
                task_succeeded = task.exception() is None
            except BaseException:
                task_succeeded = False
        if task is None or (task.done() and not task_succeeded):
            task = asyncio.create_task(self._close_background_tasks(), name="horizon-controller-close")
            task.add_done_callback(self._consume_background_close_task)
            self._background_close_task = task
        elif task_succeeded:
            return
        cancelled = False
        while True:
            try:
                await asyncio.shield(task)
                break
            except asyncio.CancelledError:
                cancelled = True
                if task.done():
                    break
                continue
        error: BaseException | None = None
        try:
            task.result()
        except BaseException as exc:
            error = exc
        if cancelled:
            raise asyncio.CancelledError
        if error is not None:
            raise error

    def _record_lifecycle_latency(self, metric: str, profile_id: Any, started: float, *, success: bool, result: str | None = None) -> None:
        """Best-effort bounded terminal lifecycle sample; never alter control flow."""
        database = getattr(getattr(self, "services", None), "telemetry_db", None)
        recorder = getattr(database, "enqueue_sample", None)
        duration_ms = min(900_000.0, max(0.0, (time.monotonic() - started) * 1000.0))
        if callable(recorder):
            try:
                event = readiness_event(str(getattr(profile_id, "value", profile_id)), int(duration_ms), success)
                recorder(
                    profile_id,
                    metric,
                    event.duration_ms,
                    ts_ms=int(time.time() * 1000),
                    state="available",
                    labels={"result": result or ("success" if success else "failure"), "source": "slotd"},
                )
            except Exception:
                _LOG.debug("lifecycle latency sample dropped", exc_info=True)
        alerts = getattr(getattr(self, "services", None), "alerts", None)
        if metric == "wake_duration" and alerts is not None:
            try:
                alerts.observe(AlertObservation(
                    profile_id=str(getattr(profile_id, "value", profile_id)),
                    profile_state="running" if success else "starting",
                    now=time.monotonic(),
                    wake_duration_ms=duration_ms,
                ))
            except Exception:
                _LOG.debug("wake alert evaluation dropped", exc_info=True)

    def _begin_startup_attempt(self, profile_id: Any, version: str | None = None) -> StartupAttempt:
        """Register one genuine start attempt for status projection and training."""

        attempt = StartupAttempt(attempt_id=uuid4().hex, started=time.monotonic(), version=version)
        key = normalize_profile_key(profile_id)
        if key is not None:
            self._startup_attempts[key] = attempt
        return attempt

    def _end_startup_attempt(self, profile_id: Any, attempt: StartupAttempt | None) -> None:
        key = normalize_profile_key(profile_id)
        if key is None or attempt is None:
            return
        if self._startup_attempts.get(key) is attempt:
            del self._startup_attempts[key]

    async def _installed_version_hint(self, profile: Profile) -> str | None:
        """Best-effort installed version from bounded reviewed metadata.

        A cached status projection may predate a just-applied modpack update, so
        only freshly read, size-bounded profile metadata is trusted.  Any
        failure returns ``None`` and never delays or fails the lifecycle.
        """

        try:
            return await asyncio.wait_for(
                asyncio.to_thread(installed_version_for_profile, profile),
                timeout=_STARTUP_VERSION_HINT_TIMEOUT_SECONDS,
            )
        except Exception:
            _LOG.debug("installed version hint unavailable", exc_info=True)
            return None

    async def _record_startup_estimate(
        self,
        profile_id: Any,
        attempt: StartupAttempt | None,
        *,
        success: bool,
    ) -> None:
        """Train only on a genuine successful start with a known version.

        The elapsed window is snapshotted before the state transaction so lock
        or write latency never inflates a sample, and the write shares the
        controller's state-database ownership rather than committing on its own.
        """

        if attempt is None or not success:
            return
        version = attempt.version
        if not version:
            return
        duration_ms = (time.monotonic() - attempt.started) * 1000.0
        stamp = _iso(self._clock())

        def write() -> None:
            try:
                self._startup_estimates.record_sample(
                    profile_id,
                    version,
                    duration_ms,
                    run_key=attempt.attempt_id,
                    finished_at=stamp,
                    managed_transaction=True,
                )
            except Exception:
                _LOG.debug("startup estimate sample dropped", exc_info=True)

        try:
            await self._transaction(write)
        except Exception:
            _LOG.debug("startup estimate transaction unavailable", exc_info=True)

    def _startup_estimate_for(self, status: Any) -> StartupEstimate | None:
        """Project the bounded history plus any in-flight attempt for one profile."""

        key = normalize_profile_key(getattr(status, "profile_id", None))
        attempt = self._startup_attempts.get(key) if key is not None else None
        state = getattr(status, "state", None)
        starting = getattr(state, "value", state) == "starting"
        if attempt is None and not starting:
            return None
        # Bind the bucket to the attempt's own version: an in-flight attempt
        # with unknown or new version metadata must never read a cached old
        # version's history.
        version = attempt.version if attempt is not None else getattr(status, "installed_version", None)
        summary = StartupEstimateSummary()
        if isinstance(version, str) and version.strip():
            summary = self._startup_estimates.summary(status.profile_id, version)
        if attempt is None and summary.sample_count == 0:
            return None
        median = summary.median_seconds
        if median is not None and (not math.isfinite(median) or median <= 0.0 or median > 900.0):
            median = None
        elapsed = None if attempt is None else attempt.elapsed_seconds()
        if elapsed is not None and (not math.isfinite(elapsed) or elapsed < 0.0 or elapsed > 86400.0):
            elapsed = None
        return StartupEstimate(
            sample_count=max(0, min(25, int(summary.sample_count))),
            median_seconds=median,
            attempt_id=None if attempt is None else attempt.attempt_id,
            elapsed_seconds=elapsed,
            version=version if isinstance(version, str) and version.strip() else None,
        )

    def _with_startup_estimates(self, snapshot: StatusSnapshot) -> StatusSnapshot:
        """Overlay the estimate projection without disturbing other fields."""

        try:
            profiles = []
            changed = False
            for status in snapshot.profiles:
                estimate = self._startup_estimate_for(status)
                if estimate == status.startup_estimate:
                    profiles.append(status)
                    continue
                profiles.append(status.model_copy(update={"startup_estimate": estimate}))
                changed = True
            return snapshot if not changed else snapshot.model_copy(update={"profiles": tuple(profiles)})
        except Exception:
            _LOG.debug("startup estimate projection unavailable", exc_info=True)
            return snapshot

    @classmethod
    def for_testing(cls, tmp_path: Path) -> "Controller":
        return cls(
            profiles={},
            state_db=_MemoryDb(tmp_path / "state.db"),
            reservation_store=None,
            operation_lock_factory=_MemoryLock,
        )

    async def execute(self, request: RpcRequest) -> RpcResponse:
        started = time.monotonic()
        try:
            return await self._execute(request)
        finally:
            ended = time.monotonic()
            self.performance.record_rpc(
                (ended - started) * 1000.0,
                action=request.action,
                monotonic_start=started,
                monotonic_end=ended,
            )
            if not isinstance(request.action, GetPerf):
                try:
                    self.performance.flush_if_due(self._db(), now=self._clock())
                except Exception:
                    _LOG.exception("performance aggregate flush skipped")

    async def maintenance_tick(self, request_id: UUID | None = None) -> None:
        """Run controller-owned schedule, idle-stop, and retention maintenance."""
        async with self._maintenance_lock:
            request_id = request_id or uuid4()
            service_group = getattr(self.services, "status", None)
            if service_group is not None:
                action = GetStatus(kind="get_status", refresh=True)
                snapshot_method = service_group.snapshot
                try:
                    parameters = signature_parameters(snapshot_method)
                    supports_maintenance = any(
                        parameter.kind is inspect.Parameter.VAR_KEYWORD
                        or parameter.name == "maintenance"
                        for parameter in parameters
                    )
                except (TypeError, ValueError):
                    supports_maintenance = False
                if supports_maintenance:
                    result = snapshot_method(
                        action, "system:maintenance", request_id, maintenance=True
                    )
                else:
                    # Preserve older injected status seams used during rolling
                    # upgrades; those seams have no persistence mode.
                    result = snapshot_method(action, "system:maintenance", request_id)
                result = await result if inspect.isawaitable(result) else result
                if isinstance(result, StatusSnapshot):
                    result = result.model_copy(update={"initializing": self.initializing})
                    await self._apply_idle_stops(result, request_id)
                    await self._apply_schedules(result, request_id)
            session_store = getattr(self.services, "session_store", None)
            maintain = getattr(session_store, "maintain", None)
            if callable(maintain):
                maintain(now=_iso(self._clock()))

    async def _execute(self, request: RpcRequest) -> RpcResponse:
        canonical = self._canonical(request)
        if _action_class(request.action) is _ActionClass.PURE_READ:
            return await self._dispatch(request)
        claim = await self._claim_request(request.request_id, canonical)
        if claim is not None:
            if claim is not _PENDING:
                return claim
            return await self._wait_for_replay(request.request_id, canonical)
        response = await self._dispatch(request)
        await self._store_replay(request.request_id, canonical, response)
        return response

    async def _dispatch(self, request: RpcRequest) -> RpcResponse:
        try:
            handler_name = DISPATCH[type(request.action)]
        except KeyError:
            return failure(request.request_id, ErrorCode.INVALID_REQUEST, "invalid request")
        try:
            if isinstance(request.action, Command):
                result = self._command(
                    request.action,
                    request.actor,
                    request.request_id,
                    request.provenance,
                )
            else:
                result = getattr(self, handler_name)(request.action, request.actor, request.request_id)
            result = await result if inspect.isawaitable(result) else result
            response: RpcResponse = result if isinstance(result, (RpcSuccess, RpcFailure)) else success(request.request_id, result)
        except _ControllerFailure as exc:
            response = failure(request.request_id, exc.code, exc.message, retryable=exc.retryable, details=exc.details)
        except Exception:
            incident = hashlib.sha256(f"{request.request_id}:{self._clock().timestamp()}".encode()).hexdigest()[:16]
            _LOG.exception("controller incident %s", incident)
            response = failure(
                request.request_id,
                ErrorCode.INTERNAL_ERROR,
                "internal controller error",
                details=SafeDetails(incident_id=incident),
            )
        return response

    def execute_sync(self, request: RpcRequest) -> RpcResponse:
        return asyncio.run(self.execute(request))

    # Public typed handlers are the only callable mutation surface used by
    # fixed dispatch; each takes its concrete action, actor, and request UUID.
    async def start(self, action: Start, actor: str, request_id: UUID) -> JobAccepted:
        return await self._start(action, actor, request_id)

    async def stop(self, action: Stop, actor: str, request_id: UUID) -> JobAccepted:
        return await self._stop(action, actor, request_id)

    async def restart(self, action: Restart, actor: str, request_id: UUID) -> JobAccepted:
        return await self._restart(action, actor, request_id)

    async def prepare_switch(self, action: PrepareSwitch, actor: str, request_id: UUID) -> ConfirmationSummary:
        return await self._prepare_switch(action, actor, request_id)

    async def confirm_switch(self, action: ConfirmSwitch, actor: str, request_id: UUID) -> JobAccepted:
        return await self._confirm_switch(action, actor, request_id)

    async def prepare_force_stop(self, action: PrepareForceStop, actor: str, request_id: UUID) -> ConfirmationSummary:
        return await self._prepare_force_stop(action, actor, request_id)

    async def confirm_force_stop(self, action: ConfirmForceStop, actor: str, request_id: UUID) -> JobAccepted:
        return await self._confirm_force_stop(action, actor, request_id)

    async def reconcile_startup(self) -> None:
        """Fail jobs interrupted by a daemon restart and reconcile reservations."""

        def mark_incomplete() -> int:
            interrupted_at = _iso(self._clock())
            cursor = self._db().execute(
                "UPDATE jobs SET state='failed', finished_at=?, detail=? "
                "WHERE state IN ('accepted','running')",
                (interrupted_at, "controller restarted"),
            )
            benchmark_changed = 0
            if self._db().execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='benchmark_runs'"
            ).fetchone() is not None:
                benchmark_cursor = self._db().execute(
                    "UPDATE benchmark_runs SET state='failed',finished_at=?,error_code=? "
                    "WHERE state='running'",
                    (interrupted_at, "controller_restarted"),
                )
                benchmark_changed = benchmark_cursor.rowcount
            pending = self._db().execute(
                "SELECT request_id FROM rpc_idempotency WHERE status='pending'"
            ).fetchall()
            for (request_id,) in pending:
                stored = failure(
                    UUID(request_id),
                    ErrorCode.INTERNAL_ERROR,
                    "request was interrupted by controller restart",
                    retryable=True,
                )
                self._db().execute(
                    "UPDATE rpc_idempotency SET status='completed',response=? WHERE request_id=? AND status='pending'",
                    (response_json(stored).decode().rstrip("\n"), request_id),
                )
            changed = cursor.rowcount + benchmark_changed
            if changed:
                self._bump_generation()
            self._db().commit()
            return changed

        changed = await self._transaction(mark_incomplete)
        reconcile = getattr(self.reservation_store, "reconcile", None)
        if reconcile is not None:
            result = await self._reservation_io(reconcile)
            changed = changed or bool(result)
        if self.slot_inspector is not None:
            self.slot_inspector.observe()
        running_profiles: set[str] = set()
        for profile in getattr(self.profiles, "profiles", ()):
            adapter = self.adapters.get(profile.id)
            if adapter is not None:
                try:
                    observed = adapter.observe(profile)
                    if inspect.isawaitable(observed):
                        observed = await observed
                    if bool(getattr(observed, "running", False)):
                        running_profiles.add(getattr(profile.id, "value", profile.id))
                except Exception:
                    changed = True
        if self.boot_autostart and not self._boot_autostart_attempted:
            self._boot_autostart_attempted = True
            profile_id = self._resume_profile_id()
            if profile_id is not None and getattr(profile_id, "value", profile_id) not in running_profiles:
                try:
                    await self._start(Start(kind="start", profile_id=profile_id), "boot", uuid4())
                except Exception:
                    _LOG.exception("boot profile autostart failed for %s", profile_id)
        # A successful daemon restart is itself a state transition: every
        # pre-restart confirmation must be invalidated even when observations
        # happen to match the previous process.
        await self._transaction(lambda: (self._bump_generation(), self._db().commit()))
        self.initializing = False

    def _resume_profile_id(self) -> ProfileId | None:
        if self.boot_profile and self.boot_profile not in {"resume", "resume-last-active"}:
            try:
                candidate = ProfileId(self.boot_profile)
            except ValueError:
                return None
            return candidate
        row = self._db().execute(
            "SELECT 1 FROM jobs "
            "WHERE operation IN ('start','switch','stop','force_stop') "
            "AND state='succeeded' AND completion_seq IS NULL LIMIT 1"
        ).fetchone()
        if row:
            return None
        row = self._db().execute(
            "SELECT operation, profile_id FROM jobs "
            "WHERE operation IN ('start','switch','stop','force_stop') "
            "AND state='succeeded' AND profile_id IS NOT NULL "
            "ORDER BY completion_seq DESC LIMIT 1"
        ).fetchone()
        if not row or row[0] not in {"start", "switch"}:
            return None
        try:
            return ProfileId(row[1])
        except ValueError:
            return None

    @staticmethod
    def _canonical(request: RpcRequest) -> str:
        payload = request.model_dump(mode="json")
        # Provenance is a trusted execution-context input, not part of the
        # durable request identity. Keep replay keys compatible with requests
        # written before the context marker existed.
        payload.pop("provenance", None)
        action = payload.get("action")
        if isinstance(action, dict) and action.get("kind") == "get_status" and action.get("refresh") is False:
            # Keep the idempotency key stable for requests written before the
            # optional internal refresh flag was added.
            action.pop("refresh", None)
        if isinstance(action, dict) and action.get("kind") == "command":
            command = action.pop("command", "")
            action["command_digest"] = hashlib.sha256(command.encode("utf-8")).hexdigest()
        if isinstance(action, dict) and action.get("kind") == "create_backup":
            # The capability is a transport authorization token, not part of
            # the durable backup request identity.
            action.pop("reservation_capability", None)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    async def _claim_request(self, request_id: UUID, canonical: str) -> RpcResponse | _PendingReplay | None:
        """Claim an idempotency key before running any side effect."""
        def claim() -> RpcResponse | _PendingReplay | None:
            db = self._db()
            row = db.execute(
                "SELECT canonical_request,response,status FROM rpc_idempotency WHERE request_id=?",
                (str(request_id),),
            ).fetchone()
            if row is None:
                db.execute(
                    "INSERT INTO rpc_idempotency(request_id,canonical_request,response,status,created_at) VALUES (?,?,'','pending',?)",
                    (str(request_id), canonical, _iso(self._clock())),
                )
                db.commit()
                return None
            if row[0] != canonical:
                return failure(request_id, ErrorCode.REQUEST_ID_CONFLICT, "request id was already used")
            if row[2] == "pending":
                return _PENDING
            if not row[1]:
                return failure(request_id, ErrorCode.INTERNAL_ERROR, "stored response unavailable")
            try:
                return response_from_json(row[1].encode())
            except Exception:
                return failure(request_id, ErrorCode.INTERNAL_ERROR, "stored response unavailable")

        return await self._transaction(claim)

    async def _wait_for_replay(self, request_id: UUID, canonical: str) -> RpcResponse:
        # The first claimant may be in another process, so waiting is a DB
        # poll rather than an in-memory event.
        for _ in range(1000):
            result = await self._claim_request(request_id, canonical)
            if result is _PENDING:
                await asyncio.sleep(0.01)
                continue
            if result is None:
                # A crashed claimant left a pending row.  Never execute a
                # second side effect; report a safe transient failure.
                return failure(request_id, ErrorCode.INTERNAL_ERROR, "request is still in progress", retryable=True)
            return result
        return failure(request_id, ErrorCode.INTERNAL_ERROR, "request is still in progress", retryable=True)

    async def _store_replay(self, request_id: UUID, canonical: str, response: RpcResponse) -> None:
        def write_replay() -> None:
            db = self._db()
            db.execute(
                "UPDATE rpc_idempotency SET response=?,status='completed' WHERE request_id=? AND canonical_request=? AND status='pending'",
                (response_json(response).decode().rstrip("\n"), str(request_id), canonical),
            )
            db.commit()

        await self._transaction(write_replay)

    def _db(self) -> sqlite3.Connection:
        return self.state_db.connection

    def _current_generation(self) -> int:
        row = self._db().execute("PRAGMA application_id").fetchone()
        return max(0, int(row[0])) if row else 0

    def _bump_generation(self) -> int:
        db = self._db()
        generation = self._current_generation() + 1
        db.execute(f"PRAGMA application_id = {generation}")
        return generation

    def _profile(self, profile_id: ProfileId) -> Profile:
        try:
            if hasattr(self.profiles, "require"):
                return self.profiles.require(profile_id)
            return self.profiles[profile_id] if profile_id in self.profiles else self.profiles[profile_id.value]
        except (KeyError, TypeError, AttributeError):
            raise _ControllerFailure(ErrorCode.INVALID_REQUEST, "unknown profile")

    def _adapter(self, profile: Profile) -> Adapter:
        adapter = self.adapters.get(profile.id) or self.adapters.get(profile.id.value)
        if adapter is None:
            raise _ControllerFailure(ErrorCode.INTERNAL_ERROR, "adapter unavailable")
        return adapter

    def _require_operation(self, profile: Profile, operation: OperationName, actor: str = "system") -> None:
        if operation not in profile.operations:
            self._audit_rejection_sync(
                actor,
                operation.value,
                profile.id,
                ErrorCode.INVALID_REQUEST,
                "operation is not permitted for profile",
            )
            raise _ControllerFailure(
                ErrorCode.INVALID_REQUEST,
                "operation is not permitted for profile",
                details=SafeDetails(profile_id=profile.id, allowed_actions=tuple(sorted(profile.operations, key=lambda item: item.value))),
            )

    def _audit_rejection_sync(
        self,
        actor: str,
        action: str,
        profile_id: ProfileId | None,
        code: ErrorCode,
        detail: str,
    ) -> None:
        self._audit(actor, action, profile_id, "rejected", code, detail)
        self._db().commit()

    async def _transaction(self, callback: Callable[[], Any]) -> Any:
        async with self._transition_lock:
            operation_lock = self._operation_lock_factory()
            entered = False
            enter_task = asyncio.create_task(asyncio.to_thread(operation_lock.__enter__))
            try:
                try:
                    await asyncio.shield(enter_task)
                    entered = True
                except asyncio.CancelledError:
                    # flock acquisition cannot be cancelled in its worker
                    # thread.  Wait for it, then release it before propagating
                    # cancellation so a late acquisition is never leaked.
                    await enter_task
                    entered = True
                    raise
                return callback()
            finally:
                if entered:
                    exit_task = asyncio.create_task(
                        asyncio.to_thread(operation_lock.__exit__, None, None, None)
                    )
                    try:
                        await asyncio.shield(exit_task)
                    except asyncio.CancelledError:
                        await exit_task
                        raise

    def _job_intent(self, actor: str, action: str, profile_id: ProfileId | None) -> str:
        job_id = uuid4().hex
        now = _iso(self._clock())
        self._db().execute(
            "INSERT INTO jobs(id,profile_id,operation,state,created_at,detail) VALUES (?,?,?,?,?,?)",
            (job_id, profile_id.value if profile_id else None, action, "accepted", now, None),
        )
        self._audit(actor, action, profile_id, "accepted", None, "job accepted")
        self._db().commit()
        return job_id

    def _finish(
        self,
        job_id: str,
        actor: str,
        action: str,
        profile_id: ProfileId | None,
        *,
        ok: bool,
        code: ErrorCode | None = None,
        detail: str = "",
        state: str | None = None,
    ) -> None:
        now = _iso(self._clock())
        final_state = state or ("succeeded" if ok else "failed")
        self._db().execute(
            "UPDATE jobs SET state=?,finished_at=?,completion_seq=("
            "SELECT COALESCE(MAX(completion_seq), 0) + 1 FROM jobs"
            "),detail=? WHERE id=?",
            (final_state, now, detail[:512], job_id),
        )
        # Audit responses intentionally retain their stable result vocabulary;
        # the richer deferred outcome belongs to the job row.
        audit_result = "succeeded" if ok else "failed"
        self._audit(actor, action, profile_id, audit_result, code, detail or "completed")
        self._bump_generation()
        self._db().commit()

    def _audit(self, actor: str, action: str, profile_id: ProfileId | None, result: str, code: ErrorCode | None, detail: str) -> None:
        self._db().execute(
            "INSERT INTO audit(id,timestamp,actor,action,profile_id,result,error_code,detail) VALUES (?,?,?,?,?,?,?,?)",
            (uuid4().hex, _iso(self._clock()), actor, action, profile_id.value if profile_id else None, result, code.value if code else None, detail[:512]),
        )

    def _lease_renewal(self, lease: tuple[ProfileId, str, int]):
        if self.reservation_store is None or not hasattr(self.reservation_store, "renew_if_owned"):
            return asyncio.create_task(asyncio.sleep(10**9))
        async def renew():
            while True:
                await asyncio.sleep(5.0)
                await self._reservation_io(
                    self.reservation_store.renew_if_owned,
                    *lease[:2], ttl=30.0, state_generation=lease[2],
                )
        return asyncio.create_task(renew())

    async def _reservation_io(
        self,
        callback: Callable[..., Any],
        *args: Any,
        cancel_cleanup: Callable[[], Awaitable[Any]] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Run file-store flock work off-loop and drain it before cancellation.

        Transaction callbacks and SQLite must stay on the owning event loop.
        Only reservation-store work belongs here. A successful acquisition or
        transfer can require exact-owner cleanup if its caller was cancelled
        before receiving the new lease.
        """
        async def invoke() -> Any:
            if inspect.iscoroutinefunction(callback):
                return await callback(*args, **kwargs)
            result = await asyncio.to_thread(callback, *args, **kwargs)
            return await result if inspect.isawaitable(result) else result

        task = asyncio.create_task(invoke())
        cancelled = False
        while True:
            try:
                result = await asyncio.shield(task)
                break
            except asyncio.CancelledError:
                if task.cancelled():
                    raise
                cancelled = True
            except BaseException:
                if cancelled:
                    raise asyncio.CancelledError
                raise
        if cancelled:
            if cancel_cleanup is not None:
                try:
                    # The same drain rule also protects cleanup against a
                    # second cancellation arriving while it waits for flock.
                    await self._reservation_io(cancel_cleanup)
                except asyncio.CancelledError:
                    pass
                except BaseException:
                    _LOG.warning("cancelled reservation cleanup failed")
            raise asyncio.CancelledError
        return result

    async def _assert_lease(self, task) -> None:
        if task.done():
            try:
                task.result()
                raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "slot reservation was lost")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "slot reservation was lost") from exc

    async def _drain_renewal(self, renewal: asyncio.Task | None) -> BaseException | None:
        """Stop and drain renewal, returning an unhandled renewal failure."""
        if renewal is None:
            return None
        renewal.cancel()

        async def drain() -> BaseException | None:
            try:
                await renewal
                return None
            except asyncio.CancelledError:
                return None
            except BaseException:
                return sys.exc_info()[1]

        # Renewal's intentional cancellation is handled inside drain, while
        # cancellation of the releasing caller remains distinguishable.
        return await self._reservation_io(drain)

    async def _release_lease(self, lease, renewal: asyncio.Task | None) -> None:
        """Always release a reservation after renewal task termination."""
        primary_active = sys.exc_info()[0] is not None
        try:
            renewal_error = await self._drain_renewal(renewal)
        except asyncio.CancelledError:
            renewal_error = sys.exc_info()[1]
        cleanup_error = None
        if lease is not None:
            try:
                await self._reservation_io(self._clear_reservation, lease)
            except BaseException:
                cleanup_error = sys.exc_info()[1]
        if primary_active:
            if renewal_error is not None or cleanup_error is not None:
                _LOG.warning("lease cleanup failed while preserving primary error")
            return
        if cleanup_error is not None:
            raise cleanup_error
        if renewal_error is not None:
            raise renewal_error

    @asynccontextmanager
    async def _operation_lease(self, profile: Profile, operation: str, request_id: UUID, *, actor: str):
        """Hold the durable slot reservation for the complete heavy operation.

        The reservation is the shared authority consumed by the direct slot
        runner as well as this controller.  Keeping it alive across worker
        threads closes the preflight-to-use gap and serializes maintenance
        with lifecycle starts in other profiles.
        """
        lease = await self._reserve(profile, operation, str(request_id), actor=actor)
        renewal = self._lease_renewal(lease)
        try:
            await self._assert_lease(renewal)
            yield lease, renewal
        finally:
            await self._release_lease(lease, renewal)

    async def _await_lease(self, awaitable, renewal_task, *, drain_on_renewal: bool = False):
        if renewal_task is None:
            # No renewable reservation is owned by this process (a handoff runs
            # under another process's live update reservation).  Cancelling a
            # coroutine that awaits ``asyncio.to_thread`` does not stop the
            # worker thread, so drain the work before the caller's cleanup can
            # run and release anything the worker still depends on.
            work = asyncio.ensure_future(awaitable)
            try:
                return await asyncio.shield(work)
            except asyncio.CancelledError:
                try:
                    await asyncio.shield(work)
                except BaseException:
                    pass
                raise
        work = asyncio.create_task(awaitable)
        try:
            done, _ = await asyncio.wait((work, renewal_task), return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            # Cancellation of a coroutine awaiting asyncio.to_thread does not
            # stop the worker thread. Drain it before the caller's lease
            # cleanup runs, otherwise the worker can mutate after ownership
            # has been released.
            try:
                await asyncio.shield(work)
            except BaseException:
                pass
            raise
        if renewal_task in done and work not in done:
            if drain_on_renewal:
                try:
                    await asyncio.shield(work)
                except BaseException:
                    pass
            else:
                work.cancel()
                try:
                    await work
                except asyncio.CancelledError:
                    pass
            await self._assert_lease(renewal_task)
        result = await work
        await self._assert_lease(renewal_task)
        return result

    async def _wait_for_free_slot(self, profile: Profile, renewal_task=None) -> bool:
        """Wait for a consistent free slot using the stopping profile budget."""
        budget = max(0.0, float(profile.stop_timeout_seconds))
        callback = self.await_free_slot
        try:
            parameters = signature_parameters(callback)
        except (TypeError, ValueError):
            parameters = ()
        free = callback(budget) if parameters else callback()
        wait = asyncio.wait_for(free, timeout=budget) if inspect.isawaitable(free) else free
        if inspect.isawaitable(wait):
            return bool(await self._await_lease(wait, renewal_task))
        return bool(wait)

    async def _probe_start_health(self, profile: Profile, renewal_task=None) -> bool:
        """Run the existing bounded start/health producer directly."""
        ready = self.await_ready(profile)
        if inspect.isawaitable(ready):
            ready = await self._await_lease(
                asyncio.wait_for(ready, timeout=profile.health_timeout_seconds),
                renewal_task,
            )
        return ready is not False

    async def _cleanup_failed_start(
        self,
        profile: Profile,
        actor: str,
        renewal_task: asyncio.Task | None,
    ) -> None:
        """Stop only the process started under the still-owned reservation.

        A service can fail before opening its health port while a non-daemon
        child keeps the systemd unit alive.  Leaving that process behind blocks
        every later start and also makes the normal stop preflight impossible.
        Keep this cleanup bound to the original reservation and never replace
        the primary start failure with cleanup detail.
        """
        try:
            await self._assert_lease(renewal_task)
            await self._await_lease(self._adapter(profile).force_stop(profile), renewal_task)
        except asyncio.CancelledError:
            raise
        except Exception:
            code = "start_cleanup_failed"
            message = "Failed start left a process that Horizon could not stop automatically."
        else:
            code = "start_cleanup_succeeded"
            message = "Horizon stopped the process left by a failed start."
        try:
            await self._record_event(profile.id, code, message)
        except Exception:
            # Recovery is best-effort telemetry.  Never replace the primary
            # start error if the audit/event store is independently degraded.
            pass

    async def _start(self, action: Start, actor: str, request_id: UUID) -> JobAccepted:
        profile = self._profile(action.profile_id)
        lifecycle_started = time.monotonic()
        lifecycle_success = False
        self._require_operation(profile, OperationName.START, actor)
        if self._db().execute(
            "SELECT 1 FROM jobs WHERE operation='benchmark' AND state IN ('accepted','running') LIMIT 1"
        ).fetchone() is not None:
            self._audit_rejection_sync(
                actor,
                "start",
                profile.id,
                ErrorCode.INVALID_STATE,
                "benchmark owns the idle game slot",
            )
            raise _ControllerFailure(
                ErrorCode.INVALID_STATE,
                "a benchmark is using the idle game slot",
                retryable=True,
                details=SafeDetails(profile_id=profile.id, retry_after_seconds=60),
            )
        lease = None
        renewal_task = None
        job_id = None
        readiness_ticket = None
        start_attempted_by_request = False
        startup_attempt: StartupAttempt | None = None
        try:
            lease = await self._reserve(profile, "start", str(request_id), actor=actor)
            renewal_task = self._lease_renewal(lease)
            job_id = await self._transaction(lambda: self._job_intent(actor, "start", profile.id))
            free = await self._wait_for_free_slot(profile, renewal_task)
            if free is False:
                raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "slot is occupied")
            # Publish a generation only after this request owns the slot and is
            # actually entering the start transition.  A rejected concurrent
            # contender must never replace the genuine start's wait target.
            readiness_ticket = self._readiness.begin(profile.id)
            start_attempted_by_request = True
            # Measure the genuine attempt from the adapter start, not from the
            # accepted request or the slot wait, so wait time never trains.
            startup_attempt = self._begin_startup_attempt(
                profile.id, await self._installed_version_hint(profile)
            )
            await self._await_lease(self._adapter(profile).start(profile), renewal_task)
            await self._assert_lease(renewal_task)
            ready = await self._probe_start_health(profile, renewal_task)
            if ready is False:
                raise RuntimeError("start readiness failed")
            await self._transaction(lambda: self._finish(job_id, actor, "start", profile.id, ok=True))
            lifecycle_success = True
            await self._record_startup_estimate(profile.id, startup_attempt, success=True)
            self._readiness.notify(readiness_ticket, ReadinessOutcome.SUCCESS)
            return JobAccepted(
                job_id=job_id,
                state="running",
                readiness_generation=readiness_ticket.generation,
                readiness=ReadinessOutcome.SUCCESS.value,
            )
        except asyncio.TimeoutError:
            if start_attempted_by_request:
                await self._cleanup_failed_start(profile, actor, renewal_task)
            if readiness_ticket is not None:
                self._readiness.notify(readiness_ticket, ReadinessOutcome.TIMEOUT)
            if job_id is not None:
                await self._transaction(lambda: self._finish(job_id, actor, "start", profile.id, ok=False, code=ErrorCode.START_TIMEOUT, detail="start timed out"))
            raise _ControllerFailure(ErrorCode.START_TIMEOUT, "start timed out", retryable=True)
        except Exception as exc:
            if start_attempted_by_request:
                await self._cleanup_failed_start(profile, actor, renewal_task)
            if readiness_ticket is not None:
                self._readiness.notify(readiness_ticket, ReadinessOutcome.FAILURE)
            code = exc.code if isinstance(exc, _ControllerFailure) else ErrorCode.HEALTH_FAILED
            detail = exc.message if isinstance(exc, _ControllerFailure) else "start failed"
            if job_id is not None:
                await self._transaction(lambda: self._finish(job_id, actor, "start", profile.id, ok=False, code=code, detail=detail))
            raise _ControllerFailure(code, detail, retryable=code is ErrorCode.HEALTH_FAILED) from exc
        finally:
            if readiness_ticket is not None:
                self._readiness.finish(readiness_ticket)
            self._end_startup_attempt(profile.id, startup_attempt)
            self._record_lifecycle_latency("wake_duration", profile.id, lifecycle_started, success=lifecycle_success)
            await self._release_lease(lease, renewal_task)

    async def _fresh_stop_preflight(
        self,
        profile: Profile,
        actor: str,
        request_id: UUID,
        *,
        allow_players: bool,
    ) -> None:
        """Re-read stop invariants after acquiring the controller lease."""
        service_group = getattr(self.services, "status", None)
        if service_group is None:
            raise _ControllerFailure(ErrorCode.HEALTH_FAILED, "stop preflight status unavailable", retryable=True)
        snapshot_method = getattr(service_group, "snapshot", None)
        if snapshot_method is None:
            raise _ControllerFailure(ErrorCode.HEALTH_FAILED, "stop preflight status unavailable", retryable=True)
        try:
            parameters = signature_parameters(snapshot_method)
        except (TypeError, ValueError):
            parameters = ()
        action = GetStatus(kind="get_status", refresh=True)
        if not parameters:
            fresh = snapshot_method()
        elif len(parameters) == 1:
            fresh = snapshot_method(action)
        else:
            fresh = snapshot_method(action, actor, request_id)
        if inspect.isawaitable(fresh):
            fresh = await fresh
        if not isinstance(fresh, StatusSnapshot):
            raise _ControllerFailure(ErrorCode.HEALTH_FAILED, "stop preflight status unavailable", retryable=True)
        current = next((item for item in fresh.profiles if item.profile_id == profile.id), None)
        if current is None:
            raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "stop preflight profile is not observed")
        if getattr(current.state, "value", current.state) != "running":
            raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "stop preflight profile is not running")
        if current.slot_owner != profile.id:
            raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "stop preflight slot owner changed")
        if current.active_job_id is not None:
            raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "stop preflight found an active job")
        if not allow_players and (current.players_online is None or current.players_online != 0):
            raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "stop preflight found active players")
        if not allow_players and current.required_ports_ready is not True:
            raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "stop preflight listener is not ready")
        if self.slot_inspector is not None:
            observed = self.slot_inspector.observe()
            if inspect.isawaitable(observed):
                observed = await observed
            if observed is None or observed.inconsistent or observed.owner != profile.id.value:
                raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "stop preflight slot ownership changed")

    async def _acquire_stop_fence(
        self,
        profile: Profile,
        actor: str,
        request_id: UUID,
        *,
        allow_players: bool,
    ) -> tuple[tuple[ProfileId, str, int] | None, asyncio.Task | None]:
        if self.reservation_store is None:
            return None, None
        lease = None
        renewal_task = None
        try:
            lease = await self._reserve(profile, "stop", str(request_id), actor=actor)
            renewal_task = self._lease_renewal(lease)
            await self._assert_lease(renewal_task)
            await self._fresh_stop_preflight(profile, actor, request_id, allow_players=allow_players)
            await self._assert_lease(renewal_task)
            return lease, renewal_task
        except (Exception, asyncio.CancelledError):
            await self._release_lease(lease, renewal_task)
            raise

    async def _stop_with_fence(
        self,
        profile: Profile,
        actor: str,
        request_id: UUID,
        *,
        force: bool,
        allow_players: bool,
    ) -> JobAccepted:
        operation = "force_stop" if force else "stop"
        lease = None
        renewal_task = None
        job_id = None
        lifecycle_started = time.monotonic()
        lifecycle_success = False
        try:
            lease, renewal_task = await self._acquire_stop_fence(
                profile, actor, request_id, allow_players=allow_players
            )
            job_id = await self._transaction(lambda: self._job_intent(actor, operation, profile.id))
            stop = self._adapter(profile).force_stop(profile) if force else self._adapter(profile).graceful_stop(profile)
            timeout = None if force else profile.stop_timeout_seconds
            work = stop if timeout is None else asyncio.wait_for(stop, timeout=timeout)
            await self._await_lease(work, renewal_task)
            await self._transaction(lambda: self._finish(job_id, actor, operation, profile.id, ok=True))
            lifecycle_success = True
            return JobAccepted(job_id=job_id, state="running")
        except asyncio.TimeoutError as exc:
            if job_id is not None:
                await self._transaction(lambda: self._finish(job_id, actor, operation, profile.id, ok=False, code=ErrorCode.GRACE_TIMEOUT, detail="graceful stop timed out"))
            raise _ControllerFailure(ErrorCode.GRACE_TIMEOUT, "graceful stop timed out") from exc
        except Exception as exc:
            detail = exc.message if isinstance(exc, _ControllerFailure) else ("force stop failed" if force else "stop failed")
            if job_id is not None:
                code = exc.code if isinstance(exc, _ControllerFailure) else ErrorCode.INTERNAL_ERROR
                await self._transaction(lambda: self._finish(job_id, actor, operation, profile.id, ok=False, code=code, detail=detail))
            if isinstance(exc, _ControllerFailure):
                raise
            raise _ControllerFailure(ErrorCode.INTERNAL_ERROR, detail) from exc
        finally:
            self._record_lifecycle_latency("stop_duration", profile.id, lifecycle_started, success=lifecycle_success)
            await self._release_lease(lease, renewal_task)

    async def _stop(self, action: Stop, actor: str, request_id: UUID) -> JobAccepted:
        profile = self._profile(action.profile_id)
        self._require_operation(profile, OperationName.STOP, actor)
        return await self._stop_with_fence(profile, actor, request_id, force=False, allow_players=False)

    async def _restart(self, action: Restart, actor: str, request_id: UUID) -> JobAccepted:
        await self._stop(Stop(kind="stop", profile_id=action.profile_id), actor, request_id)
        return await self._start(Start(kind="start", profile_id=action.profile_id), actor, request_id)

    async def _command(
        self,
        action: Command,
        actor: str,
        request_id: UUID,
        provenance: RpcProvenance = RpcProvenance.SERVICE,
    ) -> JobAccepted:
        profile = self._profile(action.profile_id)
        self._require_operation(profile, OperationName.COMMAND, actor)
        if (
            profile.id == ProfileId.MINECRAFT_SUNLIT_COBBLEMON
            and provenance is not RpcProvenance.WEB_HUMAN
        ):
            self._audit_rejection_sync(
                actor,
                OperationName.COMMAND.value,
                profile.id,
                ErrorCode.INVALID_REQUEST,
                "authenticated browser command required",
            )
            raise _ControllerFailure(
                ErrorCode.INVALID_REQUEST,
                "authenticated browser command required",
            )
        job_id = await self._transaction(lambda: self._job_intent(actor, "command", profile.id))
        try:
            await self._adapter(profile).send_command(profile, action.command)
        except AdapterError as exc:
            code = ErrorCode.UPSTREAM_UNAVAILABLE if exc.retryable else ErrorCode.INVALID_REQUEST
            detail = "command transport failed" if exc.retryable else "command unsupported"
            await self._transaction(
                lambda: self._finish(
                    job_id,
                    actor,
                    "command",
                    profile.id,
                    ok=False,
                    code=code,
                    detail=detail,
                )
            )
            raise _ControllerFailure(
                code,
                detail,
                retryable=exc.retryable,
            ) from exc
        except Exception as exc:
            await self._transaction(
                lambda: self._finish(
                    job_id,
                    actor,
                    "command",
                    profile.id,
                    ok=False,
                    code=ErrorCode.INTERNAL_ERROR,
                    detail="command failed",
                )
            )
            raise _ControllerFailure(ErrorCode.INTERNAL_ERROR, "command failed") from exc
        await self._transaction(
            lambda: self._finish(job_id, actor, "command", profile.id, ok=True)
        )
        return JobAccepted(job_id=job_id, state="running")

    async def _reserve(
        self,
        profile: Profile,
        operation: str,
        operation_id: str | None = None,
        *,
        actor: str = "system",
    ) -> tuple[ProfileId, str, int]:
        generation = self._current_generation()
        lease_id = operation_id or uuid4().hex
        if self.reservation_store is None:
            return profile.id, lease_id, generation
        lease = (profile.id, lease_id, generation)
        existing = None

        def reserve() -> Any:
            nonlocal existing
            reserve_atomic = getattr(self.reservation_store, "reserve_if_available", None)
            if reserve_atomic is not None:
                reservation_id = lease_id
                try:
                    return reserve_atomic(
                        profile.id, reservation_id, 30.0,
                        state_generation=generation,
                    )
                except TypeError:
                    # Narrow test seams may predate the generation keyword;
                    # they still provide an atomic reserve operation.
                    return reserve_atomic(profile.id, reservation_id, 30.0)
            else:
                # Test doubles from Task 2 may only expose reserve; keep the
                # lock transaction around their check/commit if possible.
                with self._operation_lock_factory():
                    existing = self.reservation_store.read()
                    if existing is not None and existing.profile_id != profile.id:
                        raise BlockingIOError
                    return self.reservation_store.reserve(profile.id, lease_id, 30.0)

        try:
            await self._reservation_io(
                reserve, cancel_cleanup=lambda: self._clear_reservation(lease),
            )
        except Exception as exc:
            await self._record_event(profile.id, ErrorCode.SLOT_CONFLICT.value, "game slot is unavailable")
            await self._record_rejected_audit(
                actor,
                operation,
                profile.id,
                ErrorCode.SLOT_CONFLICT,
                "game slot is reserved",
            )
            owner = getattr(existing, "profile_id", None)
            raise _ControllerFailure(
                ErrorCode.SLOT_CONFLICT,
                "game slot is reserved",
                details=SafeDetails(current_owner=owner),
            ) from exc
        return lease

    async def _clear_reservation(self, lease: tuple[ProfileId, str, int] | None = None) -> None:
        store = self.reservation_store
        if store is None:
            return
        if lease is not None:
            release = getattr(store, "release_if_owned", None)
            if release is not None:
                await self._reservation_io(release, *lease)
                return
        clear = getattr(store, "clear", None)
        if clear:
            await self._reservation_io(clear)
            return
        path = getattr(store, "reservation_path", None)
        if path is not None:
            def clear_path() -> None:
                try:
                    Path(path).unlink(missing_ok=True)
                except OSError:
                    pass

            await self._transaction(clear_path)

    async def _record_rejected_audit(
        self,
        actor: str,
        action: str,
        profile_id: ProfileId | None,
        code: ErrorCode,
        detail: str,
    ) -> None:
        await self._record_audit(actor, action, profile_id, "rejected", code, detail)

    async def _record_audit(
        self,
        actor: str,
        action: str,
        profile_id: ProfileId | None,
        result: str,
        code: ErrorCode | None,
        detail: str,
    ) -> None:
        def write_audit() -> None:
            self._audit(actor, action, profile_id, result, code, detail)
            self._db().commit()

        await self._transaction(write_audit)

    async def _record_event(self, profile_id: ProfileId | None, code: str, message: str) -> None:
        def write_event() -> None:
            existing = self._db().execute(
                "SELECT 1 FROM events WHERE profile_id IS ? AND code=? AND message=? AND timestamp>=? LIMIT 1",
                (
                    profile_id.value if profile_id else None,
                    code,
                    message[:512],
                    _iso(self._clock() - timedelta(minutes=5)),
                ),
            ).fetchone()
            if existing is None:
                self._db().execute(
                    "INSERT INTO events(id,timestamp,profile_id,code,message) VALUES (?,?,?,?,?)",
                    (uuid4().hex, _iso(self._clock()), profile_id.value if profile_id else None, code, message[:512]),
                )
                self._db().commit()

        await self._transaction(write_event)

    async def _prepare_switch(self, action: PrepareSwitch, actor: str, request_id: UUID) -> ConfirmationSummary:
        if action.current_profile_id == action.target_profile_id:
            raise _ControllerFailure(ErrorCode.INVALID_REQUEST, "profiles must differ")
        source = self._profile(action.current_profile_id)
        target = self._profile(action.target_profile_id)
        self._require_operation(source, OperationName.STOP, actor)
        self._require_operation(target, OperationName.START, actor)
        return await self._create_confirmation(
            actor,
            "switch",
            action.current_profile_id,
            {
                "source_profile_id": action.current_profile_id.value,
                "target_profile_id": action.target_profile_id.value,
                **action.options.model_dump(mode="json"),
            },
            lambda confirmation_id, expires, summary_hash, generation: SwitchConfirmation(
                confirmation_id=confirmation_id,
                expires_at=expires,
                summary_hash=summary_hash,
                state_generation=generation,
                action="switch",
                source_profile_id=action.current_profile_id,
                target_profile_id=action.target_profile_id,
                **action.options.model_dump(),
            ),
        )

    async def _confirm_switch(self, action: ConfirmSwitch, actor: str, request_id: UUID) -> JobAccepted:
        payload = await self._consume_confirmation(actor, "switch", action.confirmation_id)
        source = ProfileId(payload["source_profile_id"])
        target = ProfileId(payload["target_profile_id"])
        source_profile = self._profile(source)
        target_profile = self._profile(target)
        job_id = await self._transaction(lambda: self._job_intent(actor, "switch", target))
        renewal_task = None
        lease = None
        target_started = False
        target_attempt: StartupAttempt | None = None
        rollback_lease = None
        rollback_task = None
        lifecycle_started = time.monotonic()
        lifecycle_success = False
        try:
            lease = await self._reserve(target_profile, "switch", action.confirmation_id, actor=actor)
            renewal_task = self._lease_renewal(lease)
            try:
                await self._await_lease(
                    asyncio.wait_for(
                        self._adapter(source_profile).graceful_stop(source_profile),
                        timeout=source_profile.stop_timeout_seconds,
                    ),
                    renewal_task,
                )
            except asyncio.TimeoutError:
                if not payload.get("force_after_timeout", False):
                    raise _ControllerFailure(ErrorCode.GRACE_TIMEOUT, "graceful stop timed out")
                await self._await_lease(
                    self._adapter(source_profile).force_stop(source_profile),
                    renewal_task,
                )
            await self._assert_lease(renewal_task)
            free = await self._wait_for_free_slot(source_profile, renewal_task)
            if free is False:
                raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "slot did not become free")
            await self._assert_lease(renewal_task)
            if payload.get("create_backup"):
                backup = self._service("backups", "create")
                if backup is None:
                    raise _ControllerFailure(ErrorCode.BACKUP_FAILED, "backup service unavailable")
                await self._await_lease(
                    self._invoke(backup, CreateBackup(kind="create_backup", profile_id=source, protected=True), actor, request_id),
                    renewal_task,
                )
                await self._assert_lease(renewal_task)
            target_started = True
            target_attempt = self._begin_startup_attempt(
                target, await self._installed_version_hint(target_profile)
            )
            await self._start_with_free_retry(target_profile, renewal_task)
            await self._assert_lease(renewal_task)
            ready = await self._probe_start_health(target_profile, renewal_task)
            if ready is False:
                raise RuntimeError("target readiness failed")
            await self._transaction(lambda: self._finish(job_id, actor, "switch", target, ok=True))
            lifecycle_success = True
            await self._record_startup_estimate(target, target_attempt, success=True)
            return JobAccepted(job_id=job_id, state="running")
        except Exception as exc:
            if payload.get("rollback_on_failure", True):
                await self._record_event(source, "rollback_attempt", "attempting to restore previous profile")
                await self._record_audit(actor, "rollback", source, "accepted", None, "rollback attempt started")
                try:
                    if target_started:
                        try:
                            await self._await_lease(
                                asyncio.wait_for(
                                    self._adapter(target_profile).graceful_stop(target_profile),
                                    timeout=target_profile.stop_timeout_seconds,
                                ),
                                renewal_task,
                            )
                        except asyncio.TimeoutError:
                            if payload.get("force_after_timeout", False):
                                await self._await_lease(
                                    self._adapter(target_profile).force_stop(target_profile),
                                    renewal_task,
                                )
                            else:
                                raise
                    if lease is not None and hasattr(self.reservation_store, "transfer_if_owned"):
                        rollback_id = uuid4().hex
                        transferred_lease = (source_profile.id, rollback_id, lease[2])
                        await self._reservation_io(
                            self.reservation_store.transfer_if_owned,
                            lease[0], lease[1], lease[2], source_profile.id, rollback_id,
                            cancel_cleanup=lambda: self._clear_reservation(transferred_lease),
                        )
                        rollback_lease = transferred_lease
                        lease = None
                    else:
                        if lease is not None:
                            await self._clear_reservation(lease)
                            lease = None
                        rollback_lease = await self._reserve(source_profile, "rollback", uuid4().hex, actor=actor)
                    rollback_task = self._lease_renewal(rollback_lease)
                    free = await self._wait_for_free_slot(source_profile, rollback_task)
                    if free is False:
                        raise RuntimeError("slot did not become free for rollback")
                    await self._start_with_free_retry(source_profile, rollback_task)
                    ready = await self._probe_start_health(source_profile, rollback_task)
                    if ready is False:
                        raise RuntimeError("rollback readiness failed")
                except Exception:
                    await self._record_event(source, "rollback_failed", "rollback did not restore the previous profile")
                    await self._record_audit(
                        actor,
                        "rollback",
                        source,
                        "failed",
                        ErrorCode.HEALTH_FAILED,
                        "rollback failed",
                    )
                else:
                    await self._record_event(source, "rollback_succeeded", "previous profile restored")
                    await self._record_audit(actor, "rollback", source, "succeeded", None, "rollback completed")
            code = exc.code if isinstance(exc, _ControllerFailure) else ErrorCode.HEALTH_FAILED
            detail = exc.message if isinstance(exc, _ControllerFailure) else "switch failed"
            await self._transaction(lambda: self._finish(job_id, actor, "switch", target, ok=False, code=code, detail=detail))
            raise _ControllerFailure(code, detail) from exc
        finally:
            self._end_startup_attempt(target, target_attempt)
            self._record_lifecycle_latency("switch_duration", target, lifecycle_started, success=lifecycle_success)
            await self._release_lease(lease, renewal_task)
            await self._release_lease(rollback_lease, rollback_task)

    async def _start_with_free_retry(self, profile: Profile, renewal_task=None) -> None:
        try:
            if renewal_task is None:
                await self._adapter(profile).start(profile)
            else:
                await self._await_lease(self._adapter(profile).start(profile), renewal_task)
        except Exception as exc:
            code = getattr(exc, "returncode", getattr(exc, "code", None))
            if code != 75:
                raise
            free = await self._wait_for_free_slot(profile, renewal_task)
            if free is False:
                raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "slot did not become free")
            if renewal_task is None:
                await self._adapter(profile).start(profile)
            else:
                await self._await_lease(self._adapter(profile).start(profile), renewal_task)

    async def _prepare_force_stop(self, action: PrepareForceStop, actor: str, request_id: UUID) -> ConfirmationSummary:
        profile = self._profile(action.profile_id)
        self._require_operation(profile, OperationName.FORCE_STOP, actor)
        return await self._create_confirmation(
            actor,
            "force_stop",
            action.profile_id,
            {},
            lambda confirmation_id, expires, summary_hash, generation: ForceStopConfirmation(
                confirmation_id=confirmation_id, expires_at=expires, summary_hash=summary_hash,
                state_generation=generation, action="force_stop", profile_id=action.profile_id
            ),
        )

    async def _confirm_force_stop(self, action: ConfirmForceStop, actor: str, request_id: UUID) -> JobAccepted:
        payload = await self._consume_confirmation(actor, "force_stop", action.confirmation_id)
        profile = self._profile(ProfileId(payload["profile_id"]))
        self._require_operation(profile, OperationName.FORCE_STOP, actor)
        return await self._stop_with_fence(profile, actor, request_id, force=True, allow_players=True)

    async def _create_confirmation(self, actor: str, action: str, profile_id: ProfileId, payload: dict[str, Any], build: Callable[..., ConfirmationSummary]) -> ConfirmationSummary:
        confirmation_id = uuid4().hex
        expires = self._clock() + timedelta(minutes=5)
        generation = self._current_generation()
        encoded = json.dumps(
            {"action": action, "profile_id": profile_id.value, "payload": payload},
            sort_keys=True,
            separators=(",", ":"),
        )
        summary_hash = hashlib.sha256(encoded.encode()).hexdigest()
        row_payload = json.dumps({**payload, "profile_id": profile_id.value}, sort_keys=True, separators=(",", ":"))
        def write_confirmation() -> None:
            self._db().execute(
                "INSERT INTO confirmations(id,actor,action,profile_id,payload,expires_at,state_generation) VALUES (?,?,?,?,?,?,?)",
                (confirmation_id, actor, action, profile_id.value, row_payload, _iso(expires), generation),
            )
            self._db().commit()

        await self._transaction(write_confirmation)
        return build(confirmation_id, expires, summary_hash, generation)

    async def _consume_confirmation(self, actor: str, action: str, confirmation_id: str) -> dict[str, Any]:
        def consume():
            row = self._db().execute(
                "SELECT actor,action,payload,expires_at,consumed_at,state_generation,profile_id FROM confirmations WHERE id=?",
                (confirmation_id,),
            ).fetchone()
            if row is None or row[1] != action or row[0] != actor:
                self._audit_rejection_sync(actor, action, None, ErrorCode.CONFIRMATION_MISMATCH, "confirmation does not match")
                raise _ControllerFailure(ErrorCode.CONFIRMATION_MISMATCH, "confirmation does not match")
            if row[4] is not None:
                self._audit_rejection_sync(actor, action, ProfileId(row[6]), ErrorCode.CONFIRMATION_MISMATCH, "confirmation was already consumed")
                raise _ControllerFailure(ErrorCode.CONFIRMATION_MISMATCH, "confirmation was already consumed")
            expires = datetime.fromisoformat(row[3].replace("Z", "+00:00"))
            if expires <= self._clock():
                self._audit_rejection_sync(actor, action, ProfileId(row[6]), ErrorCode.CONFIRMATION_EXPIRED, "confirmation expired")
                raise _ControllerFailure(ErrorCode.CONFIRMATION_EXPIRED, "confirmation expired")
            if row[5] != self._current_generation():
                self._audit_rejection_sync(actor, action, ProfileId(row[6]), ErrorCode.CONFIRMATION_MISMATCH, "confirmation is stale")
                raise _ControllerFailure(ErrorCode.CONFIRMATION_MISMATCH, "confirmation is stale")
            updated = self._db().execute(
                "UPDATE confirmations SET consumed_at=? WHERE id=? AND consumed_at IS NULL AND state_generation=?",
                (_iso(self._clock()), confirmation_id, row[5]),
            )
            if updated.rowcount != 1:
                self._audit_rejection_sync(actor, action, ProfileId(row[6]), ErrorCode.CONFIRMATION_MISMATCH, "confirmation was already consumed")
                raise _ControllerFailure(ErrorCode.CONFIRMATION_MISMATCH, "confirmation was already consumed")
            self._db().commit()
            return json.loads(row[2])

        return await self._transaction(consume)

    # Typed seams for later services.  They intentionally return safe empty
    # views rather than accepting arbitrary mappings.
    async def _get_status(self, action: GetStatus, actor: str, request_id: UUID) -> StatusSnapshot:
        started = time.monotonic()
        try:
            return await self._get_status_timed(action, actor, request_id)
        finally:
            self.performance.record_cycle((time.monotonic() - started) * 1000.0)

    async def _wait_readiness(self, action: WaitReadiness, actor: str, request_id: UUID) -> ReadinessResult:
        del actor, request_id
        try:
            ticket = self._readiness.latest(action.profile_id, generation=action.generation)
            outcome = await self._readiness.wait(ticket, timeout=action.timeout_seconds)
        except asyncio.TimeoutError as exc:
            raise _ControllerFailure(ErrorCode.START_TIMEOUT, "start readiness timed out", retryable=True) from exc
        except (RuntimeError, ValueError) as exc:
            raise _ControllerFailure(ErrorCode.HEALTH_FAILED, "start readiness is unavailable", retryable=True) from exc
        return ReadinessResult(
            profile_id=action.profile_id,
            generation=ticket.generation,
            outcome=outcome.value,
        )

    async def _get_status_timed(self, action: GetStatus, actor: str, request_id: UUID) -> StatusSnapshot:
        service_group = getattr(self.services, "status", None)
        if service_group is not None:
            method = "snapshot" if action.refresh else "cached_snapshot"
            result = getattr(service_group, method)(action, actor, request_id)
            result = await result if inspect.isawaitable(result) else result
        else:
            result = StatusSnapshot(generation=0, observed_at=self._clock(), profiles=())
        if isinstance(result, StatusSnapshot):
            result = self._with_startup_estimates(result)
            result = result.model_copy(update={"initializing": self.initializing})
            return result
        return result

    async def _apply_idle_stops(self, snapshot: StatusSnapshot, request_id: UUID) -> None:
        """Apply due idle stops from the existing fresh status sample."""
        for status in snapshot.profiles:
            try:
                profile = self._profile(status.profile_id)
            except _ControllerFailure:
                continue
            if not self._idle_stop.observe(profile, status):
                continue
            await self._record_event(profile.id, "idle_stop", "idle-stop threshold reached")
            notifier = self._service("notifications", "send")
            if notifier is not None:
                try:
                    result = notifier(
                        profile.id,
                        NotificationEvent.IDLE_STOP,
                        self._current_generation(),
                        "auto-stop requested after the configured idle period",
                    )
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    _LOG.warning("idle-stop notification failed for %s", profile.id, exc_info=True)
            try:
                await self._stop(
                    Stop(kind="stop", profile_id=profile.id),
                    "system:idle-stop",
                    request_id,
                )
            except Exception:
                await self._record_event(profile.id, "idle_stop", "idle-stop request failed")
                _LOG.warning("idle-stop failed for %s", profile.id, exc_info=True)

    async def _run_scheduled_backup(self, action: CreateBackup, request_id: UUID) -> JobAccepted:
        """Run a scheduled backup under the same durable lease as manual work."""
        profile = self._profile(action.profile_id)
        service = self._service("backups", "create")
        if service is None:
            raise _ControllerFailure(ErrorCode.BACKUP_FAILED, "backup service unavailable")
        async with self._operation_lease(
            profile, "scheduled_backup", request_id, actor="system:schedule"
        ) as (lease, renewal):
            job_id = await self._transaction(
                lambda: self._job_intent("system:schedule", "scheduled_backup", profile.id)
            )
            try:
                await self._await_lease(
                    self._invoke(
                        service,
                        action,
                        "system:schedule",
                        request_id,
                        lease_check=lambda: self._lease_owned_sync(lease),
                    ),
                    renewal,
                    drain_on_renewal=True,
                )
            except asyncio.CancelledError:
                await self._transaction(lambda: self._finish(
                    job_id, "system:schedule", "scheduled_backup", profile.id,
                    ok=False, code=ErrorCode.INTERNAL_ERROR,
                    detail="scheduled backup cancelled",
                ))
                raise
            except SafeError as exc:
                deferred = exc.code == "profile_running"
                await self._transaction(lambda: self._finish(
                    job_id, "system:schedule", "scheduled_backup", profile.id,
                    ok=False,
                    code=ErrorCode.INVALID_STATE if deferred else ErrorCode.BACKUP_FAILED,
                    detail="profile_running" if deferred else "backup_failed",
                    state="deferred" if deferred else "failed",
                ))
                raise
            except Exception:
                await self._transaction(lambda: self._finish(
                    job_id, "system:schedule", "scheduled_backup", profile.id,
                    ok=False, code=ErrorCode.BACKUP_FAILED, detail="backup_failed",
                ))
                raise
            await self._transaction(lambda: self._finish(
                job_id, "system:schedule", "scheduled_backup", profile.id,
                ok=True, detail="scheduled backup completed",
            ))
            return JobAccepted(job_id=job_id, state="succeeded")

    async def _apply_schedules(self, snapshot: StatusSnapshot, request_id: UUID) -> None:
        """Run due backup jobs and config-only switches from the fresh status cycle."""
        if not self._schedule.entries:
            return
        due = self._schedule.due(snapshot.observed_at)
        if not due:
            return
        benchmark_due = tuple(entry for entry in due if entry.operation == "benchmark")
        if benchmark_due:
            statuses = tuple(snapshot.profiles)
            evidence_reader = self._service("status", "benchmark_eligibility")
            evidence = None
            if callable(evidence_reader):
                try:
                    policy_entry = benchmark_due[0]
                    evidence = evidence_reader(
                        maintenance_window=policy_entry.maintenance_window,
                        rollback_safe=policy_entry.rollback_safe,
                        public_wake_policy=policy_entry.public_wake_policy,
                        snapshot=snapshot,
                    )
                    if inspect.isawaitable(evidence):
                        evidence = await evidence
                except Exception:
                    evidence = None
            evidence_ok = isinstance(evidence, Mapping) and all(evidence.get(key) is True for key in (
                "maintenance_window", "storage_acceptable", "ups_acceptable",
                "quiet_period", "no_wake_session", "no_conflicting_jobs", "rollback_safe_public_wake",
            ))
            eligible = (
                not any(item.players_online != 0 or item.active_job_id is not None or getattr(item, "state", "") not in {"stopped", "failed", "blocked", "unknown"} for item in statuses)
                and not any(getattr(item, "slot_owner", None) for item in statuses)
                and evidence_ok
                and all(getattr(entry, "maintenance_window", False) and getattr(entry, "rollback_safe", False) and getattr(entry, "public_wake_policy", "disabled") == "safe" for entry in benchmark_due)
            )
            for entry in benchmark_due:
                if self._schedule_fire_seen(entry, snapshot.observed_at):
                    continue
                await self._record_event(entry.profile, "scheduled_fire", self._schedule_fire_key(entry, snapshot.observed_at))
                if not eligible:
                    await self._record_event(entry.profile, "scheduled_benchmark_skipped", "benchmark eligibility evidence unavailable")
                    continue
                try:
                    await self._run_benchmark(
                        RunBenchmark(kind="run_benchmark", profile_id=entry.profile,
                                     baseline_preset=entry.baseline_preset or "", candidate_preset=entry.candidate_preset or ""),
                        "system:schedule", request_id,
                    )
                    await self._record_event(entry.profile, "scheduled_benchmark", "scheduled benchmark dispatched")
                except Exception:
                    await self._record_event(entry.profile, "scheduled_benchmark_failed", "scheduled benchmark dispatch failed")
            due = tuple(entry for entry in due if entry.operation != "benchmark")
            if not due:
                return
        switch_due = []
        for entry in due:
            # Claim each fire immediately before executing it. This is
            # intentionally sequential: duplicate config entries in one due
            # tick must observe the marker written by the first entry.
            if self._schedule_fire_seen(entry, snapshot.observed_at):
                continue
            await self._record_event(entry.profile, "scheduled_fire", self._schedule_fire_key(entry, snapshot.observed_at))
            if entry.operation == "switch":
                switch_due.append(entry)
            if entry.operation != "backup" or entry.backup_destination is None:
                continue
            action = CreateBackup(
                kind="create_backup",
                profile_id=entry.profile,
                protected=entry.backup_destination is BackupDestination.HORIZON_B2,
                destination=entry.backup_destination,
            )
            try:
                await self._run_scheduled_backup(action, request_id)
                await self._record_event(entry.profile, "scheduled_backup", "scheduled backup completed")
            except Exception as exc:
                # A running profile is intentionally reported as deferred; this
                # path never switches, stops, or restarts a game. A deferred
                # attempt is retried at the next scheduled fire.
                deferred = getattr(exc, "code", None) == "profile_running"
                message = "scheduled backup deferred" if deferred else "scheduled backup failed"
                await self._record_event(entry.profile, message.replace(" ", "_"), message)
                if not deferred:
                    notifier = self._service("notifications", "send")
                    if notifier is not None:
                        try:
                            result = notifier(
                                entry.profile,
                                NotificationEvent.BACKUP_FAILURE,
                                self._current_generation(),
                                "scheduled backup failed",
                            )
                            if inspect.isawaitable(result):
                                await result
                        except Exception:
                            _LOG.warning("scheduled backup notification failed for %s", entry.profile.value, exc_info=True)
                _LOG.warning(
                    "scheduled backup %s for %s",
                    "deferred" if deferred else "failed",
                    entry.profile.value,
                )
        due = tuple(switch_due)
        if not due:
            return
        statuses = {item.profile_id: item for item in snapshot.profiles}
        owner = next((item.slot_owner for item in snapshot.profiles if item.slot_owner), None)
        current = statuses.get(owner) if owner is not None else None
        if owner is None or current is None:
            for entry in due:
                await self._record_event(entry.profile, "scheduled_switch_skipped", "no active owner")
            return
        if current.players_online != 0 or current.active_job_id is not None:
            for entry in due:
                if entry.profile != owner:
                    await self._record_event(entry.profile, "scheduled_switch_skipped", "current owner has players or an active job")
            return
        for entry in due:
            if entry.profile == owner:
                await self._record_event(entry.profile, "scheduled_switch", "scheduled profile is already the owner")
                continue
            try:
                prepared = await self._prepare_switch(
                    PrepareSwitch(
                        kind="prepare_switch",
                        current_profile_id=owner,
                        target_profile_id=entry.profile,
                        options=SwitchOptions(),
                    ),
                    "system:schedule",
                    request_id,
                )
                confirmation_id = getattr(prepared, "confirmation_id", None)
                if not isinstance(confirmation_id, str):
                    raise RuntimeError("scheduled switch confirmation unavailable")
                await self._confirm_switch(
                    ConfirmSwitch(kind="confirm_switch", confirmation_id=confirmation_id),
                    "system:schedule",
                    request_id,
                )
                await self._record_event(entry.profile, "scheduled_switch", "scheduled switch completed")
            except Exception:
                await self._record_event(entry.profile, "scheduled_switch", "scheduled switch failed")
                _LOG.warning("scheduled switch failed for %s", entry.profile, exc_info=True)

    @staticmethod
    def _schedule_fire_key(entry: ScheduleEntry, observed_at: datetime) -> str:
        minute = observed_at.astimezone(timezone.utc).replace(second=0, microsecond=0)
        operation = entry.operation
        destination = entry.backup_destination.value if entry.backup_destination is not None else "none"
        return f"{minute.isoformat()}:{entry.profile.value}:{operation}:{destination}:{entry.cron}:{entry.campaign or 'none'}"

    def _schedule_fire_seen(self, entry: ScheduleEntry, observed_at: datetime) -> bool:
        row = self._db().execute(
            "SELECT 1 FROM events WHERE profile_id=? AND code='scheduled_fire' AND message=? LIMIT 1",
            (entry.profile.value, self._schedule_fire_key(entry, observed_at)),
        ).fetchone()
        return row is not None

    async def _get_perf(self, action: GetPerf, actor: str, request_id: UUID) -> PerfSnapshot:
        snapshot = self.performance.snapshot()
        services = getattr(self, "services", None)
        snapshot["databases"] = await collect_perf_databases_async(
            state=getattr(self, "state_db", None),
            telemetry=getattr(services, "telemetry_db", None),
        )
        return PerfSnapshot.model_validate(snapshot)

    async def _get_profiles(self, action: GetProfiles, actor: str, request_id: UUID) -> tuple[PublicProfile, ...]:
        service = self._service("profiles", "public_profiles")
        if service is not None:
            result = service(action, actor, request_id)
            return await result if inspect.isawaitable(result) else result
        profiles = getattr(self.profiles, "profiles", ()) if self.profiles is not None else ()
        return tuple(
            PublicProfile(
                id=p.id,
                display_name=p.display_name,
                adapter=p.adapter,
                operations=p.operations,
                idle_stop_minutes=getattr(p, "idle_stop_minutes", 0) or 0,
                public_endpoint=(
                    PublicEndpoint(
                        host=p.public_endpoint.host,
                        port=p.public_endpoint.port,
                        protocol=p.public_endpoint.protocol,
                        reachable=None,
                    ) if p.public_endpoint is not None else None
                ),
            )
            for p in profiles
        )

    async def _get_logs(self, action: GetLogs, actor: str, request_id: UUID) -> LogPage:
        try:
            offset = self._log_offset(action.page.cursor)
        except ValueError as exc:
            raise _ControllerFailure(ErrorCode.INVALID_REQUEST, "invalid log cursor") from exc
        service = self._service("logs", "page")
        if service is not None:
            result = service(action, actor, request_id)
            result = await result if inspect.isawaitable(result) else result
            return self._bound_log_page(
                LogPage(items=tuple(result.items[offset:]), next_cursor=None),
                action.page,
                offset=offset,
            )
        profile = self._profile(action.profile_id)
        adapter = self._adapter(profile)
        requested = min(5000, action.page.limit + offset)
        if action.page.since is None and action.page.until is None:
            logs = await adapter.recent_logs(profile, requested)
        else:
            logs = await adapter.recent_logs(
                profile,
                requested,
                since=action.page.since,
                until=action.page.until,
            )
        return self._bound_log_page(LogPage(items=tuple(logs[offset:]), next_cursor=None), action.page, offset=offset)

    @staticmethod
    def _log_offset(cursor: str | None) -> int:
        if not cursor:
            return 0
        if not isinstance(cursor, str) or len(cursor) > 4 or not cursor.isascii() or not cursor.isdecimal() or (len(cursor) > 1 and cursor.startswith("0")):
            raise ValueError("invalid log cursor")
        value = int(cursor, 10)
        if value > 5000:
            raise ValueError("invalid log cursor")
        return value

    @staticmethod
    def _bound_log_page(page: LogPage, options, *, offset: int | None = None) -> LogPage:
        """Keep serialized log responses below the explicit RPC frame budget."""
        start = 0 if offset is None else offset
        items = tuple(page.items)
        selected: list[Any] = []
        encoded_items: list[bytes] = []
        encoded_total = 0
        # Encode each record once. This avoids repeatedly serializing the
        # entire growing page (which was quadratic for small records).
        for item in items:
            encoded_item = json.dumps(item.model_dump(mode="json"), separators=(",", ":")).encode()
            projected = 32 + encoded_total + len(encoded_items) + len(encoded_item)
            if projected > MAX_RESPONSE_BYTES - 2048:
                break
            selected.append(item)
            encoded_items.append(encoded_item)
            encoded_total += len(encoded_item)
        more = len(selected) < len(items)
        # The controller owns one cursor contract for logs: a decimal offset
        # into the bounded result set. Never expose an opaque backend cursor
        # that this endpoint cannot validate on the next request.
        next_cursor = str(start + len(selected)) if more else None
        result = LogPage(items=tuple(selected), next_cursor=next_cursor)
        # Account for exact wrapper/cursor/escaping bytes once, then trim only
        # the tail if the typed envelope itself crosses the hard budget.
        while selected and len(json.dumps(result.model_dump(mode="json"), separators=(",", ":")).encode()) > MAX_RESPONSE_BYTES:
            selected.pop()
            encoded_items.pop()
            next_cursor = str(start + len(selected))
            result = LogPage(items=tuple(selected), next_cursor=next_cursor)
        return result

    async def _list_backups(self, action: ListBackups, actor: str, request_id: UUID) -> BackupPage:
        service = self._service("backups", "list")
        if service is not None:
            result = service(action, actor, request_id)
            return await result if inspect.isawaitable(result) else result
        return BackupPage(items=(), next_cursor=None)

    async def _list_aggregate_backups(self, action: ListAggregateBackups, actor: str, request_id: UUID) -> BackupPage:
        items = []
        per_profile = min(200, action.page.limit)
        profile_ids = (getattr(profile, "id", profile) for profile in self.profiles)
        for profile_id in sorted(profile_ids, key=lambda item: getattr(item, "value", item)):
            page = await self._list_backups(
                ListBackups(kind="list_backups", profile_id=profile_id,
                            page=PageOptions(limit=per_profile)),
                actor, request_id,
            )
            items.extend(page.items)
        items.sort(key=lambda item: (str(item.created_at), str(item.id)), reverse=True)
        return BackupPage(items=tuple(items[:action.page.limit]), next_cursor=None)

    async def _list_events(self, action: ListEvents, actor: str, request_id: UUID) -> EventPage:
        service = self._service("audit", "list_events")
        if service is not None:
            result = service(action, actor, request_id)
            return await result if inspect.isawaitable(result) else result
        return EventPage(items=(), next_cursor=None)

    async def _get_stats_summary(self, action: GetStatsSummary, actor: str, request_id: UUID) -> dict[str, Any]:
        service = self._service("stats", "stats_summary")
        if service is None:
            raise _ControllerFailure(ErrorCode.INTERNAL_ERROR, "history query service unavailable")
        return await self._invoke(service, action, actor, request_id, now=_iso(self._clock()))

    async def _get_stats_heatmap(self, action: GetStatsHeatmap, actor: str, request_id: UUID) -> dict[str, Any]:
        service = self._service("stats", "stats_heatmap")
        if service is None:
            raise _ControllerFailure(ErrorCode.INTERNAL_ERROR, "history query service unavailable")
        return await self._invoke(service, action, actor, request_id, now=_iso(self._clock()))

    async def _get_stats_tps(self, action: GetStatsTps, actor: str, request_id: UUID) -> dict[str, Any]:
        if action.profile_id.value not in {"minecraft", "minecraft-sunlit-cobblemon"}:
            raise _ControllerFailure(ErrorCode.INVALID_REQUEST, "tick telemetry is only available for Minecraft")
        service = self._service("stats", "tps")
        if service is not None:
            return await self._invoke(service, action, actor, request_id, now=_iso(self._clock()))
        raise _ControllerFailure(ErrorCode.INTERNAL_ERROR, "history query service unavailable")

    async def _get_profile_config(self, action: GetProfileConfig, actor: str, request_id: UUID) -> ProfileConfigResponse:
        profile = self._profile(action.profile_id)
        try:
            settings = tuple(ProfileConfigEntry.model_validate(item) for item in get_profile_config(profile))
        except ConfigValidationError as exc:
            raise _ControllerFailure(ErrorCode.INVALID_REQUEST, str(exc)) from exc
        return ProfileConfigResponse(profile_id=profile.id, settings=settings)

    async def _get_benchmarks(self, action: GetBenchmarks, actor: str, request_id: UUID) -> BenchmarkOverview:
        self._profile(action.profile_id)
        service = self._service("benchmarks", "overview")
        if service is None:
            return BenchmarkOverview(profile_id=action.profile_id, available=False, presets=(), runs=())
        return await self._invoke(service, action, actor, request_id)

    async def _export_benchmarks(self, action: ExportBenchmarks, actor: str, request_id: UUID) -> BenchmarkExport:
        self._profile(action.profile_id)
        service = self._service("benchmarks", "export_history")
        if service is None:
            raise _ControllerFailure(ErrorCode.BENCHMARK_FAILED, "benchmark service unavailable")
        result = service(action.profile_id, fmt=action.format, limit=action.limit)
        if inspect.isawaitable(result):
            result = await result
        return BenchmarkExport(format=action.format, content=str(result))

    async def _run_benchmark(self, action: RunBenchmark, actor: str, request_id: UUID) -> JobAccepted:
        profile = self._profile(action.profile_id)
        self._require_operation(profile, OperationName.BENCHMARK, actor)
        prove_idle = self._service("benchmarks", "prove_idle")
        preflight = self._service("benchmarks", "preflight")
        prepare_frozen = self._service("benchmarks", "prepare_frozen")
        run = self._service("benchmarks", "run")
        fail = self._service("benchmarks", "fail")
        if None in {prove_idle, preflight, prepare_frozen, run, fail}:
            raise _ControllerFailure(ErrorCode.BENCHMARK_FAILED, "benchmark service unavailable")
        lease, renewal_task = await self._operation_lease_acquire(profile, "benchmark", request_id, actor=actor)
        try:
            result = prove_idle(action.profile_id)
            if inspect.isawaitable(result):
                await self._await_lease(result, renewal_task)
        except SafeError as exc:
            await self._operation_lease_release(lease, renewal_task)
            code = ErrorCode.INVALID_STATE if exc.code == "invalid_state" else ErrorCode.BENCHMARK_FAILED
            raise _ControllerFailure(code, exc.message, retryable=code is ErrorCode.INVALID_STATE) from exc
        except Exception:
            await self._operation_lease_release(lease, renewal_task)
            raise

        active = self._db().execute(
            "SELECT operation FROM jobs WHERE state IN ('accepted','running') LIMIT 1"
        ).fetchone()
        if active is not None:
            await self._operation_lease_release(lease, renewal_task)
            raise _ControllerFailure(ErrorCode.INVALID_STATE, "another game operation is active", retryable=True)
        job_id = await self._transaction(lambda: self._job_intent(actor, "benchmark", profile.id))
        try:
            frozen = await self._await_lease(
                asyncio.to_thread(preflight, action, job_id),
                renewal_task,
                drain_on_renewal=True,
            )
            if not isinstance(frozen, Mapping):
                raise SafeError("benchmark_failed", "benchmark preflight provenance is unavailable")
            await self._transaction(
                lambda: prepare_frozen(action, job_id, frozen)
            )
        except SafeError as exc:
            await self._transaction(lambda: self._finish(
                job_id, actor, "benchmark", profile.id, ok=False,
                code=ErrorCode.BENCHMARK_FAILED, detail="benchmark request validation failed",
            ))
            await self._operation_lease_release(lease, renewal_task)
            raise _ControllerFailure(ErrorCode.BENCHMARK_FAILED, exc.message) from exc
        except asyncio.CancelledError:
            await self._transaction(lambda: self._finish(
                job_id, actor, "benchmark", profile.id, ok=False,
                code=ErrorCode.INTERNAL_ERROR, detail="benchmark preparation cancelled",
            ))
            await self._operation_lease_release(lease, renewal_task)
            raise
        except Exception:
            await self._transaction(lambda: self._finish(
                job_id, actor, "benchmark", profile.id, ok=False,
                code=ErrorCode.BENCHMARK_FAILED, detail="benchmark request validation failed",
            ))
            await self._operation_lease_release(lease, renewal_task)
            raise
        if self._background_closed:
            await self._operation_lease_release(lease, renewal_task)
            raise _ControllerFailure(ErrorCode.INVALID_STATE, "controller is closed")
        task = asyncio.create_task(
            self._benchmark_worker(action, actor, request_id, job_id, run, fail, lease, renewal_task),
            name=f"benchmark-{job_id}",
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._consume_background_task)
        return JobAccepted(job_id=job_id, state="accepted")

    async def _operation_lease_acquire(self, profile: Profile, operation: str, request_id: UUID, *, actor: str):
        lease = await self._reserve(profile, operation, str(request_id), actor=actor)
        renewal = self._lease_renewal(lease)
        try:
            await self._assert_lease(renewal)
        except (Exception, asyncio.CancelledError):
            await self._release_lease(lease, renewal)
            raise
        return lease, renewal

    async def _operation_lease_release(self, lease, renewal) -> None:
        await self._release_lease(lease, renewal)

    async def _cancel_benchmark(self, action: CancelBenchmark, actor: str, request_id: UUID) -> JobAccepted:
        del request_id
        row = self._db().execute(
            "SELECT profile_id,state FROM jobs WHERE id=? AND operation='benchmark'",
            (action.job_id,),
        ).fetchone()
        if row is None or row[1] not in {"accepted", "running"}:
            raise _ControllerFailure(ErrorCode.INVALID_STATE, "benchmark job is not active")
        service = self._service("benchmarks", "cancel")
        if service is None or not service(action.job_id):
            raise _ControllerFailure(ErrorCode.INVALID_STATE, "benchmark process is not cancellable")
        failed = self._service("benchmarks", "fail")
        if failed is not None:
            result = failed(action.job_id, "cancelled")
            if inspect.isawaitable(result):
                await result
        await self._transaction(
            lambda: self._finish(
                action.job_id,
                actor,
                "benchmark",
                ProfileId(row[0]),
                ok=False,
                code=ErrorCode.BENCHMARK_FAILED,
                detail="benchmark cancelled",
            )
        )
        return JobAccepted(job_id=action.job_id, state="cancelled")

    async def _benchmark_worker(
        self,
        action: RunBenchmark,
        actor: str,
        request_id: UUID,
        job_id: str,
        run: Any,
        fail: Any,
        lease: tuple[ProfileId, str, int],
        renewal_task: asyncio.Task,
    ) -> None:
        del request_id
        try:
            await self._assert_lease(renewal_task)
            result = run(action, job_id)
            if inspect.isawaitable(result):
                result = await self._await_lease(result, renewal_task, drain_on_renewal=True)
        except asyncio.CancelledError:
            await self._operation_lease_release(lease, renewal_task)
            raise
        except Exception:
            try:
                failed = fail(job_id, "benchmark_failed")
                if inspect.isawaitable(failed):
                    await failed
            except Exception:
                _LOG.exception("benchmark history failure for %s", job_id)
            await self._transaction(
                lambda: self._finish(
                    job_id,
                    actor,
                    "benchmark",
                    action.profile_id,
                    ok=False,
                    code=ErrorCode.BENCHMARK_FAILED,
                    detail="benchmark failed",
                )
            )
            _LOG.exception("benchmark job %s failed", job_id)
            await self._operation_lease_release(lease, renewal_task)
            return
        try:
            alerts = getattr(getattr(self, "services", None), "alerts", None)
            regression = False
            benchmark_service = self._service("benchmarks", "evaluate_completed_regression")
            if benchmark_service is not None:
                try:
                    evaluated = benchmark_service(action.profile_id, result)
                    regression = await evaluated if inspect.isawaitable(evaluated) else bool(evaluated)
                except Exception:
                    _LOG.warning("rolling benchmark regression evaluation failed", exc_info=True)
            if alerts is not None:
                try:
                    alerts.observe(AlertObservation(
                        profile_id=str(getattr(action.profile_id, "value", action.profile_id)),
                        profile_state="stopped",
                        now=time.monotonic(),
                        benchmark_regression=regression,
                    ))
                except Exception:
                    _LOG.debug("benchmark alert evaluation dropped", exc_info=True)
            await self._transaction(
                lambda: self._finish(
                    job_id,
                    actor,
                    "benchmark",
                    action.profile_id,
                    ok=True,
                    detail="benchmark completed",
                )
            )
        finally:
            await self._operation_lease_release(lease, renewal_task)

    async def _set_profile_config(self, action: SetProfileConfig, actor: str, request_id: UUID) -> ProfileConfigResponse:
        profile = self._profile(action.profile_id)
        async with self._operation_lease(profile, "set_profile_config", request_id, actor=actor) as (_lease, renewal):
            status_service = getattr(self.services, "status", None)
            if status_service is None:
                raise _ControllerFailure(ErrorCode.HEALTH_FAILED, "status is unavailable", retryable=True)
            snapshot = await self._authoritative_status_snapshot(actor, request_id)
            current = next((item for item in snapshot.profiles if item.profile_id == profile.id), None)
            if current is None:
                raise _ControllerFailure(
                    ErrorCode.HEALTH_FAILED,
                    "profile status is unavailable",
                    retryable=True,
                )
            state = getattr(getattr(current, "state", None), "value", getattr(current, "state", None))
            if state in {"starting", "stopping"}:
                raise _ControllerFailure(ErrorCode.INVALID_STATE, "profile config cannot change while the profile is transitioning")
            try:
                result = await self._await_lease(
                    asyncio.to_thread(
                        set_profile_config, profile, action.changes,
                        lease_check=lambda: self._lease_owned_sync(_lease),
                    ), renewal,
                    drain_on_renewal=True,
                )
                settings = tuple(ProfileConfigEntry.model_validate(item) for item in get_profile_config(profile))
            except ConfigValidationError as exc:
                raise _ControllerFailure(ErrorCode.INVALID_REQUEST, str(exc)) from exc
            except RuntimeError as exc:
                if str(exc) != "operation lease was lost before config publication":
                    raise
                raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, str(exc), retryable=True) from exc
            changed = tuple(result["changed"])
            restart_required = tuple(result["restart_required"])
            await self._record_event(profile.id, "config_changed", f"config changed: {', '.join(changed) or 'no changes'}")
            await self._record_audit(actor, "set_profile_config", profile.id, "succeeded", None, f"changed keys: {', '.join(changed) or 'none'}; restart required: {', '.join(restart_required) or 'none'}")
            return ProfileConfigResponse(profile_id=profile.id, settings=settings, changed=changed, restart_required=restart_required)

    async def _authoritative_status_snapshot(self, actor: str, request_id: UUID) -> StatusSnapshot:
        """Read a fresh status projection for a safety decision."""
        service_group = getattr(self.services, "status", None)
        snapshot_method = getattr(service_group, "snapshot", None)
        if snapshot_method is None:
            raise _ControllerFailure(ErrorCode.HEALTH_FAILED, "status is unavailable", retryable=True)
        action = GetStatus(kind="get_status", refresh=True)
        try:
            parameters = signature_parameters(snapshot_method)
        except (TypeError, ValueError):
            parameters = ()
        supports_maintenance = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD or parameter.name == "maintenance"
            for parameter in parameters
        )
        try:
            if not parameters:
                fresh = snapshot_method()
            elif supports_maintenance:
                fresh = snapshot_method(action, actor, request_id, maintenance=True)
            elif len(parameters) == 1:
                fresh = snapshot_method(action)
            else:
                fresh = snapshot_method(action, actor, request_id)
            if inspect.isawaitable(fresh):
                fresh = await fresh
        except Exception as exc:
            raise _ControllerFailure(ErrorCode.HEALTH_FAILED, "status is unavailable", retryable=True) from exc
        if not isinstance(fresh, StatusSnapshot):
            raise _ControllerFailure(ErrorCode.HEALTH_FAILED, "status is unavailable", retryable=True)
        return fresh

    def _schedule_response(self) -> ScheduleResponse:
        now = self._clock()
        return ScheduleResponse(schedules=tuple(
            ScheduleView(
                cron=entry.cron,
                profile=entry.profile,
                next_fire=entry.next_fire(now),
                enabled=entry.enabled,
                backup_destination=entry.backup_destination,
                operation=entry.operation,
                baseline_preset=entry.baseline_preset,
                candidate_preset=entry.candidate_preset,
                campaign=entry.campaign,
                maintenance_window=entry.maintenance_window,
                rollback_safe=entry.rollback_safe,
                public_wake_policy=entry.public_wake_policy,
            )
            for entry in self._schedule.entries
        ))

    async def _get_schedules(self, action: GetSchedules, actor: str, request_id: UUID) -> ScheduleResponse:
        return self._schedule_response()

    async def _set_schedules(self, action: SetSchedules, actor: str, request_id: UUID) -> ScheduleResponse:
        entries = [item.model_dump(mode="json") for item in action.entries]
        for item in action.entries:
            self._profile(item.profile)
        try:
            parsed = parse_schedule(entries)
            if self.schedule_config_path is None:
                raise ScheduleConfigError("schedule config is unavailable")
            write_schedule_config(self.schedule_config_path, entries)
        except (ScheduleConfigError, ValueError) as exc:
            raise _ControllerFailure(ErrorCode.INVALID_REQUEST, str(exc)) from exc
        self._schedule = ScheduleBook(parsed)
        await self._record_event(None, "schedules_changed", f"schedules replaced: {len(parsed)} entries")
        await self._record_audit(actor, "set_schedules", None, "succeeded", None, f"replaced schedules: {len(parsed)} entries")
        return self._schedule_response()

    async def _list_audit(self, action: ListAudit, actor: str, request_id: UUID) -> AuditPage:
        service = self._service("audit", "list_audit")
        if service is not None:
            result = service(action, actor, request_id)
            return await result if inspect.isawaitable(result) else result
        return AuditPage(items=(), next_cursor=None)

    def _lease_owned_sync(self, lease: tuple[ProfileId, str, int]) -> bool:
        store = self.reservation_store
        if store is None:
            return True
        owns_live = getattr(store, "owns_live", None)
        if callable(owns_live):
            return bool(owns_live(*lease))
        if not hasattr(store, "read"):
            return True
        current = store.read()
        return current is not None and (
            current.profile_id, current.operation_id, current.state_generation
        ) == lease

    async def _run_maintenance_job(
        self, profile: Profile, operation: str, action: Any, actor: str, request_id: UUID,
        service: Any, **extra: Any,
    ) -> JobAccepted:
        """Run one synchronous maintenance worker under a durable controller job."""
        async with self._operation_lease(profile, operation, request_id, actor=actor) as (lease, renewal):
            return await self._run_bound_maintenance_job(
                profile, operation, action, actor, request_id, service,
                lease_check=lambda: self._lease_owned_sync(lease),
                renewal=renewal,
                **extra,
            )

    async def _run_handoff_maintenance_job(
        self, profile: Profile, operation: str, action: Any, actor: str, request_id: UUID,
        service: Any, *, capability: str, handoff: Any, **extra: Any,
    ) -> JobAccepted:
        """Run one job under an exact live update reservation, without reacquiring it."""
        def lease_check() -> bool:
            store = self.reservation_store
            authorize = getattr(store, "authorize_handoff", None)
            if not callable(authorize):
                return False
            try:
                current = authorize(profile.id, capability, operation_kind="update")
            except Exception:
                return False
            return (
                current.profile_id == handoff.profile_id
                and current.operation_id == handoff.operation_id
                and current.state_generation == handoff.state_generation
                and current.controller_pid == handoff.controller_pid
                and current.controller_start_ticks == handoff.controller_start_ticks
            )

        if not await self._reservation_io(lease_check):
            raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "update handoff is unavailable")
        return await self._run_bound_maintenance_job(
            profile, operation, action, actor, request_id, service,
            lease_check=lease_check,
            renewal=None,
            return_service_result=True,
            **extra,
        )

    async def _run_bound_maintenance_job(
        self, profile: Profile, operation: str, action: Any, actor: str, request_id: UUID,
        service: Any, *, lease_check: Callable[[], bool], renewal: asyncio.Task | None,
        return_service_result: bool = False,
        **extra: Any,
    ) -> JobAccepted:
        job_id = await self._transaction(lambda: self._job_intent(actor, operation, profile.id))
        try:
            result = await self._await_lease(
                self._invoke(service, action, actor, request_id,
                             lease_check=lease_check,
                             job_id=job_id, **extra),
                renewal,
                drain_on_renewal=True,
            )
        except asyncio.CancelledError:
            await self._transaction(lambda: self._finish(
                job_id, actor, operation, profile.id, ok=False,
                code=ErrorCode.INTERNAL_ERROR, detail="maintenance cancelled",
            ))
            raise
        except Exception as exc:
            failure = self._maintenance_failure(operation, exc)
            await self._transaction(lambda: self._finish(
                job_id, actor, operation, profile.id, ok=False,
                code=failure.code, detail=failure.message,
            ))
            raise failure from exc
        await self._transaction(lambda: self._finish(
            job_id, actor, operation, profile.id, ok=True, detail=f"{operation} completed",
        ))
        if return_service_result and isinstance(result, JobAccepted):
            return result
        return JobAccepted(job_id=job_id, state="succeeded")

    @staticmethod
    def _maintenance_failure(operation: str, exc: Exception) -> _ControllerFailure:
        if isinstance(exc, _ControllerFailure):
            return exc
        defaults = {
            "backup": ErrorCode.BACKUP_FAILED,
            "restore": ErrorCode.RESTORE_FAILED,
            "update": ErrorCode.UPDATE_FAILED,
            "world_clone": ErrorCode.INVALID_REQUEST,
            "retirement": ErrorCode.RETIREMENT_FAILED,
        }
        if isinstance(exc, SafeError):
            try:
                code = ErrorCode(exc.code)
            except ValueError:
                code = defaults.get(operation, ErrorCode.INTERNAL_ERROR)
            allowed = {
                "backup": {ErrorCode.BACKUP_FAILED, ErrorCode.INVALID_REQUEST, ErrorCode.SLOT_CONFLICT},
                "restore": {ErrorCode.RESTORE_FAILED, ErrorCode.INVALID_REQUEST, ErrorCode.SLOT_CONFLICT},
                "update": {ErrorCode.UPDATE_FAILED, ErrorCode.INVALID_REQUEST, ErrorCode.SLOT_CONFLICT},
                "world_clone": {ErrorCode.INVALID_REQUEST, ErrorCode.SLOT_CONFLICT},
                "retirement": {
                    ErrorCode.RETIREMENT_FAILED,
                    ErrorCode.RETIREMENT_UNAVAILABLE,
                    ErrorCode.INVALID_REQUEST,
                    ErrorCode.SLOT_CONFLICT,
                },
            }
            if code not in allowed.get(operation, set()):
                code = defaults.get(operation, ErrorCode.INTERNAL_ERROR)
            return _ControllerFailure(code, exc.message, retryable=exc.retryable)
        return _ControllerFailure(defaults.get(operation, ErrorCode.INTERNAL_ERROR), f"{operation} failed")

    async def _create_backup(self, action: CreateBackup, actor: str, request_id: UUID) -> JobAccepted:
        profile = self._profile(action.profile_id)
        self._require_operation(profile, OperationName.BACKUP, actor)
        service = self._service("backups", "create")
        if service is None:
            raise _ControllerFailure(ErrorCode.BACKUP_FAILED, "backup service unavailable")
        if action.reservation_capability is not None:
            if (
                profile.id != ProfileId.MINECRAFT_SUNLIT_COBBLEMON
                or not action.protected
                or action.destination is not BackupDestination.HORIZON_B2
            ):
                raise _ControllerFailure(ErrorCode.INVALID_REQUEST, "reservation handoff is not permitted")
            authorize = getattr(self.reservation_store, "authorize_handoff", None)
            if not callable(authorize):
                raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "update handoff is unavailable")
            try:
                handoff = await self._reservation_io(
                    authorize,
                    profile.id,
                    action.reservation_capability,
                    operation_kind="update",
                )
            except (BlockingIOError, OSError, ValueError, PermissionError) as exc:
                raise _ControllerFailure(ErrorCode.SLOT_CONFLICT, "update handoff is unavailable") from exc
            return await self._run_handoff_maintenance_job(
                profile, "backup", action, actor, request_id, service,
                capability=action.reservation_capability,
                handoff=handoff,
            )
        return await self._run_maintenance_job(profile, "backup", action, actor, request_id, service)

    async def _prepare_restore(self, action: PrepareRestore, actor: str, request_id: UUID) -> ConfirmationSummary:
        profile = self._profile(action.profile_id)
        self._require_operation(profile, OperationName.RESTORE, actor)
        availability = self._service("backups", "availability")
        if availability is not None and availability(action.backup_id) != "present":
            raise _ControllerFailure(ErrorCode.RESTORE_FAILED, "backup payload is not locally available")
        return await self._create_confirmation(
            actor,
            "restore",
            action.profile_id,
            {"backup_id": action.backup_id},
            lambda confirmation_id, expires, summary_hash, generation: RestoreConfirmation(
                confirmation_id=confirmation_id,
                expires_at=expires,
                summary_hash=summary_hash,
                state_generation=generation,
                action="restore",
                profile_id=action.profile_id,
                backup_id=action.backup_id,
            ),
        )

    async def _confirm_restore(self, action: ConfirmRestore, actor: str, request_id: UUID) -> JobAccepted:
        payload = await self._consume_confirmation(actor, "restore", action.confirmation_id)
        service = self._service("backups", "confirm_restore")
        if service is None:
            service = self._service("backups", "restore")
        if service is None:
            raise _ControllerFailure(ErrorCode.RESTORE_FAILED, "restore service unavailable")
        profile_id = payload.get("profile_id", getattr(action, "profile_id", None))
        profile = self._profile(profile_id)
        availability = self._service("backups", "availability")
        backup_id = str(payload.get("backup_id", ""))
        if availability is not None and backup_id and availability(backup_id) != "present":
            raise _ControllerFailure(ErrorCode.RESTORE_FAILED, "backup payload is not locally available")
        return await self._run_maintenance_job(profile, "restore", action, actor, request_id, service, payload=payload)

    async def _get_retirement_status(self, action: GetRetirementStatus, actor: str, request_id: UUID):
        service = self._service("backups", "retirement_status")
        if service is None:
            raise _ControllerFailure(ErrorCode.RETIREMENT_FAILED, "retirement service unavailable")
        result = service(action.operation_id)
        return await result if inspect.isawaitable(result) else result

    async def _prepare_retirement(self, action: PrepareRetirement, actor: str, request_id: UUID) -> ConfirmationSummary:
        service = self._service("backups", "retirement_prepare_async")
        if service is None:
            service = self._service("backups", "retirement_prepare")
        if service is None:
            raise _ControllerFailure(ErrorCode.RETIREMENT_FAILED, "retirement service unavailable")
        summary = await self._invoke_prepare(service, action)
        profile_ids = tuple(str(item) for item in summary.get("profile_ids", ()))
        if not profile_ids:
            raise _ControllerFailure(ErrorCode.RETIREMENT_FAILED, "retirement plan has no profiles")
        for value in profile_ids:
            self._require_operation(self._profile(ProfileId(value)), OperationName.RESTORE, actor)
        profile_id = ProfileId(profile_ids[0])
        scoped = {
            "operation_id": action.operation_id,
            "phase": action.phase,
            "destination_id": str(summary["destination_id"]),
            "count": int(summary["count"]),
            "bytes": int(summary["bytes"]),
            "profile_ids": list(profile_ids),
        }
        return await self._create_confirmation(
            actor,
            "retirement",
            profile_id,
            scoped,
            lambda confirmation_id, expires, summary_hash, generation: RetirementConfirmation(
                confirmation_id=confirmation_id,
                expires_at=expires,
                summary_hash=summary_hash,
                state_generation=generation,
                action="retirement",
                operation_id=action.operation_id,
                phase=action.phase,
                destination_id=scoped["destination_id"],
                count=scoped["count"],
                bytes=scoped["bytes"],
                profile_ids=profile_ids,
            ),
        )

    async def _invoke_prepare(self, service: Any, action: PrepareRetirement) -> dict[str, Any]:
        try:
            names = tuple(parameter.name for parameter in signature_parameters(service))
        except (TypeError, ValueError):
            names = ()
        if "manifest_sha256" in names:
            result = service(action.operation_id, action.phase)
        else:
            result = service(action)
        result = await result if inspect.isawaitable(result) else result
        if not isinstance(result, dict):
            raise _ControllerFailure(ErrorCode.RETIREMENT_FAILED, "retirement plan is unavailable")
        return result

    async def _confirm_retirement(self, action: ConfirmRetirement, actor: str, request_id: UUID) -> JobAccepted:
        payload = await self._consume_confirmation(actor, "retirement", action.confirmation_id)
        payload = {**payload, "confirmation_id": action.confirmation_id}
        service = self._service("backups", "retirement_confirm")
        if service is None:
            raise _ControllerFailure(ErrorCode.RETIREMENT_FAILED, "retirement service unavailable")
        profile = self._profile(ProfileId(str(payload.get("profile_id"))))
        return await self._run_maintenance_job(
            profile, "retirement", action, actor, request_id, service, payload=payload
        )

    async def _prepare_world_clone(self, action: PrepareWorldClone, actor: str, request_id: UUID) -> ConfirmationSummary:
        source = self._profile(ProfileId.TERRARIA_VANILLA)
        target = self._profile(ProfileId.TERRARIA_TMOD)
        self._require_operation(source, OperationName.CLONE_SOURCE, actor)
        self._require_operation(target, OperationName.CLONE_TARGET, actor)
        return await self._create_confirmation(
            actor,
            "world_clone",
            ProfileId.TERRARIA_VANILLA,
            {
                "source_profile_id": ProfileId.TERRARIA_VANILLA.value,
                "target_profile_id": ProfileId.TERRARIA_TMOD.value,
                "source_world_id": action.source_world_id,
                "destination_name": action.destination_name,
            },
            lambda confirmation_id, expires, summary_hash, generation: WorldCloneConfirmation(
                confirmation_id=confirmation_id,
                expires_at=expires,
                summary_hash=summary_hash,
                state_generation=generation,
                action="world_clone",
                source_profile_id=ProfileId.TERRARIA_VANILLA,
                target_profile_id=ProfileId.TERRARIA_TMOD,
                source_world_id=action.source_world_id,
                destination_name=action.destination_name,
            ),
        )

    async def _confirm_world_clone(self, action: ConfirmWorldClone, actor: str, request_id: UUID) -> JobAccepted:
        payload = await self._consume_confirmation(actor, "world_clone", action.confirmation_id)
        service = self._service("worlds", "confirm_clone")
        if service is None:
            service = self._service("worlds", "clone")
        if service is None:
            raise _ControllerFailure(ErrorCode.INVALID_REQUEST, "world clone service unavailable")
        source = self._profile(ProfileId.TERRARIA_VANILLA)
        return await self._run_maintenance_job(source, "world_clone", action, actor, request_id, service, payload=payload)

    async def _check_update(self, action: CheckUpdate, actor: str, request_id: UUID) -> UpdateStatus:
        profile = self._profile(action.profile_id)
        self._require_operation(profile, OperationName.UPDATE_CHECK, actor)
        service = self._service("updates", "check")
        if service is None:
            raise _ControllerFailure(ErrorCode.UPDATE_FAILED, "update service unavailable")
        return await self._invoke(service, action, actor, request_id)

    async def _prepare_update(self, action: PrepareUpdate, actor: str, request_id: UUID) -> ConfirmationSummary:
        profile = self._profile(action.profile_id)
        self._require_operation(profile, OperationName.UPDATE_APPLY, actor)
        return await self._create_confirmation(
            actor,
            "update",
            action.profile_id,
            {"installed_version": None, "available_version": None},
            lambda confirmation_id, expires, summary_hash, generation: UpdateConfirmation(
                confirmation_id=confirmation_id,
                expires_at=expires,
                summary_hash=summary_hash,
                state_generation=generation,
                action="update",
                profile_id=action.profile_id,
                installed_version=None,
                available_version=None,
            ),
        )

    async def _confirm_update(self, action: ConfirmUpdate, actor: str, request_id: UUID) -> JobAccepted:
        payload = await self._consume_confirmation(actor, "update", action.confirmation_id)
        service = self._service("updates", "confirm")
        if service is None:
            service = self._service("updates", "apply")
        if service is None:
            raise _ControllerFailure(ErrorCode.UPDATE_FAILED, "update service unavailable")
        profile = self._profile(payload.get("profile_id", getattr(action, "profile_id", None)))
        return await self._run_maintenance_job(profile, "update", action, actor, request_id, service, payload=payload)

    async def _get_notification_config(self, action: GetNotificationConfig, actor: str, request_id: UUID) -> NotificationConfig:
        service = self._service("notifications", "get_config")
        if service is not None:
            result = service(action, actor, request_id)
            return await result if inspect.isawaitable(result) else result
        raise _ControllerFailure(ErrorCode.INTERNAL_ERROR, "notification service unavailable")

    async def _set_notification_rule(self, action, actor: str, request_id: UUID) -> NotificationConfig:
        service = self._service("notifications", "set_rule")
        if service is None:
            raise _ControllerFailure(ErrorCode.INTERNAL_ERROR, "notification service unavailable")
        return await self._invoke(service, action, actor, request_id)

    async def _test_notification(self, action, actor: str, request_id: UUID) -> JobAccepted:
        service = self._service("notifications", "test")
        if service is None:
            raise _ControllerFailure(ErrorCode.INTERNAL_ERROR, "notification service unavailable")
        return await self._invoke(service, action, actor, request_id)

    async def _set_idle_stop(self, action: SetIdleStop, actor: str, request_id: UUID) -> dict[str, Any]:
        updater = getattr(self.profiles, "update_idle_stop", None)
        if updater is None:
            raise _ControllerFailure(ErrorCode.INTERNAL_ERROR, "profile configuration is unavailable")
        try:
            profile = updater(action.profile_id, action.minutes)
        except (OSError, RuntimeError, ValueError) as exc:
            raise _ControllerFailure(ErrorCode.INVALID_REQUEST, "idle-stop setting could not be saved") from exc
        for group_name in ("profiles", "notifications"):
            group = getattr(self.services, group_name, None)
            mapping = getattr(group, "profiles", None)
            if isinstance(mapping, dict):
                mapping[profile.id.value] = profile
        await self._record_audit(
            actor,
            "set_idle_stop",
            profile.id,
            "succeeded",
            None,
            "idle-stop setting updated",
        )
        return {"profile_id": profile.id.value, "idle_stop_minutes": profile.idle_stop_minutes}

    def _service(self, group: str, method: str):
        service_group = getattr(self.services, group, None)
        return getattr(service_group, method, None) if service_group is not None else None

    async def _invoke(self, service, action, actor: str, request_id: UUID, **extra):
        """Call an injected typed seam without forwarding arbitrary RPC maps."""
        try:
            names = tuple(parameter.name for parameter in signature_parameters(service))
        except (TypeError, ValueError):
            names = ()
        kwargs = {name: value for name, value in extra.items() if name in names}
        if "actor" in names:
            kwargs["actor"] = actor
        if "request_id" in names:
            kwargs["request_id"] = request_id
        if kwargs:
            result = service(action, **kwargs)
        else:
            positional = [action]
            if len(names) >= 3:
                positional.extend((actor, request_id))
            result = service(*positional)
        return await result if inspect.isawaitable(result) else result


class _ControllerFailure(Exception):
    def __init__(self, code: ErrorCode, message: str, *, retryable: bool = False, details: SafeDetails | None = None):
        self.code, self.message, self.retryable, self.details = code, message, retryable, details
        super().__init__(message)


__all__ = ["Controller", "DISPATCH", "STREAM_ACTIONS", "dispatch_is_exhaustive"]
