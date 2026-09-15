"""Composition-root adapters for the Horizon controller service groups.

Policy lives in the owning domain modules.  This module only normalizes the
fixed root configuration, constructs dependencies, and preserves the legacy
service-group names used by older callers.
"""

from __future__ import annotations

import asyncio
import inspect
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from .backups import BackupRpcFacade, BackupService, RestoreService
from .benchmark_safety import BenchmarkPreflight, prometheus_ups_provider
from .benchmarks import BenchmarkService, parse_benchmark_plans
from .capability_evidence import RootWakeSafetyEvidence
from .errors import SafeError
from .health import HealthChecker
from .history_queries import HistoryQueryService
from .logs import LogService
from .metrics import MetricSampler
from .players import PlayerTracker
from .session_store import SessionStore
from .models import ProfileId
from .notifications import DEFAULT_SECRET_DIR, NotificationRpcFacade, NotificationService
from .protocol import (
    AuditPage, EventPage, LogPage, PublicEndpoint, PublicProfile,
)
from .redaction import Redactor, SecretRegistry
from .root_state import RootActiveJobsReader, RootGenerationReader
from .runtime.alerts import AlertRuntime
from .runtime.compatibility import (
    BoundTelemetryCollectors as _BoundTelemetryCollectors,
    legacy_telemetry_config as _legacy_telemetry_config,
)
from .runtime.protocols import AlertObservation
from .runtime.telemetry import (
    DEFAULT_HOST_METRICS, LegacyTpsMode, ResourceRef, TelemetryCollector,
    TelemetryRuntime, TelemetryRuntimeConfig,
)
from .rcon_telemetry import PerformanceResult as _PerformanceResult
from .rcon_telemetry import PlayerCountResult as _PlayerCountResult
from .rcon_telemetry import TelemetryCommand as _TelemetryCommand
PerformanceResult = _PerformanceResult
PlayerCountResult = _PlayerCountResult
TelemetryCommand = _TelemetryCommand
from .state_db import STATE_DB_PATH
from .status import StatusService
from .sunlit_update import SunlitUpdateChecker
from .tps import FIXED_EXPORTER_URL, TpsSampler
from .telemetry_db import TelemetryDatabase
from .updates import UpdateRpcFacade, UpdateService
from .worlds import WorldRpcFacade, WorldService

SUNLIT_ONLINE_SNAPSHOT_SECONDS = 240.0
_HOST_TELEMETRY_METRICS = DEFAULT_HOST_METRICS
_AUDIT_STATE_DB_PATH = STATE_DB_PATH
_prometheus_ups_provider = prometheus_ups_provider


def _key(value: Any) -> str:
    value = getattr(value, "id", value)
    value = getattr(value, "value", value)
    return str(value)


def _connection(database: Any) -> Any | None:
    value = getattr(database, "connection", database)
    return value if hasattr(value, "execute") else None


def _isolated_database(database: Any) -> Any | None:
    path = getattr(database, "path", None)
    opener = getattr(type(database), "open", None)
    if path is None or not callable(opener):
        return None
    try:
        return opener(path)
    except Exception:
        return None


def _close_database(database: Any | None) -> None:
    if database is not None and hasattr(database, "close"):
        database.close()


class _StatusFacade:
    def __init__(self, service: StatusService, *, telemetry_health_reason: str | None = None):
        self.service = service
        self.telemetry_health_reason = telemetry_health_reason

    async def snapshot(self, action: Any = None, actor: str | None = None, request_id: Any = None, *, maintenance: bool = False):
        return await self.service.snapshot(persist=False, force=bool(maintenance or getattr(action, "refresh", False)))

    async def cached_snapshot(self, action: Any = None, actor: str | None = None, request_id: Any = None):
        return await self.service.cached_snapshot()

    def telemetry_health(self) -> dict[str, Any]:
        health = self.service.telemetry_health()
        if self.telemetry_health_reason is not None:
            health["reason"] = self.telemetry_health_reason
        return health

    async def benchmark_eligibility(self, *, maintenance_window: bool, rollback_safe: bool, public_wake_policy: str, snapshot: Any = None):
        return await self.service.benchmark_eligibility(maintenance_window=maintenance_window, rollback_safe=rollback_safe, public_wake_policy=public_wake_policy, snapshot=snapshot)


class _PerformanceAlerts:
    """Compatibility translator into the one AlertRuntime owner."""
    def __init__(self, profiles: Mapping[str, Any], notifications: Any, *, max_pending: int = 8):
        self._runtime = AlertRuntime(profiles, notifications, max_pending=max_pending)

    def observe(self, profile_id: Any, **values: Any) -> None:
        self._runtime.observe(AlertObservation(profile_id=_key(profile_id), profile_state=values.get("profile_state", "running"), now=values.get("now", 0), mspt_p95=values.get("mspt_p95"), rss_bytes=values.get("rss_bytes"), wake_duration_ms=values.get("wake_duration_ms"), benchmark_regression=values.get("benchmark_regression")))

    async def close(self) -> None:
        await self._runtime.close()


class _LogsFacade:
    def __init__(self, profiles: Mapping[str, Any], adapters: Mapping[Any, Any], redactor: Redactor):
        self.profiles, self.adapters, self.redactor = profiles, adapters, redactor
        self._services = {key: LogService(adapters.get(key) or adapters.get(getattr(profile, "id", None)), redactor=redactor) for key, profile in profiles.items()}

    async def page(self, action: Any, actor: str | None = None, request_id: Any = None) -> LogPage:
        key = _key(action.profile_id)
        if key not in self.profiles:
            raise SafeError("profile_not_found", "profile was not found")
        options = action.page
        lines = await self._services[key].tail(self.profiles[key], limit=options.limit, since=options.since, until=options.until)
        severity = getattr(options, "severity", "all")
        if severity != "all":
            lines = tuple(line for line in lines if line.severity == severity)
        return LogPage(items=tuple(lines), next_cursor=None)

    async def tail(self, profile: Any, *, limit: int = 500, since: datetime | None = None):
        return await self._services[_key(profile)].tail(self.profiles[_key(profile)], limit=limit, since=since)

    async def search(self, profile: Any, query: str, *, limit: int = 100, regex: bool = False):
        return await self._services[_key(profile)].search(self.profiles[_key(profile)], query, limit=limit, regex=regex)


class _AuditFacade:
    """Import-compatible, SQL-free adapter over the history owner."""
    _decode_cursor = staticmethod(HistoryQueryService._decode_cursor)
    _encode_cursor = staticmethod(HistoryQueryService._encode_cursor)
    _page_cursor = classmethod(HistoryQueryService._page_cursor.__func__)

    def __init__(self, database: Any, history: HistoryQueryService | None = None):
        self.database = database
        self.history = history or HistoryQueryService(database, approved_state_path=_AUDIT_STATE_DB_PATH)

    async def list_events(self, action: Any, actor: str | None = None, request_id: Any = None) -> EventPage:
        return await self.history.list_events(action, actor, request_id)

    async def list_audit(self, action: Any, actor: str | None = None, request_id: Any = None) -> AuditPage:
        return await self.history.list_audit(action, actor, request_id)


class _ProfilesFacade:
    def __init__(self, profiles: Mapping[str, Any]):
        self.profiles = profiles

    def public_profiles(self, action: Any = None, actor: str | None = None, request_id: Any = None):
        return tuple(PublicProfile(id=profile.id, display_name=profile.display_name, adapter=profile.adapter, operations=profile.operations, idle_stop_minutes=getattr(profile, "idle_stop_minutes", 0) or 0, public_endpoint=(PublicEndpoint(host=profile.public_endpoint.host, port=profile.public_endpoint.port, protocol=profile.public_endpoint.protocol, reachable=None) if profile.public_endpoint is not None else None)) for profile in self.profiles.values())


class _StatsFacade:
    """Import-compatible adapter; all production queries belong to history."""
    def __init__(self, state_database: Any, telemetry_database: Any | None = None, history: HistoryQueryService | None = None):
        self.state_database = state_database
        self.telemetry_database = telemetry_database
        self.history = history or HistoryQueryService(state_database, telemetry_database, approved_state_path=_AUDIT_STATE_DB_PATH)

    def tps(self, action: Any, actor: str | None = None, request_id: Any = None, *, now: str) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return self.history.tps_sync(action, now=now)
        return self.history.tps(action, actor, request_id, now=now)

    async def stats_summary(self, action: Any, actor: str | None = None, request_id: Any = None, *, now: str) -> dict[str, Any]:
        return await self.history.stats_summary(action, actor, request_id, now=now)

    async def stats_heatmap(self, action: Any, actor: str | None = None, request_id: Any = None, *, now: str) -> dict[str, Any]:
        return await self.history.stats_heatmap(action, actor, request_id, now=now)


class _ContainerSlot:
    def __init__(self):
        self._container: Any | None = None

    @property
    def container(self) -> Any | None:
        return self._container

    def bind(self, container: Any) -> None:
        if self._container is not None:
            raise RuntimeError("service container is already bound")
        self._container = container


class ServiceSeams:
    """Complete public service-group surface plus additive runtime ownership."""
    def __init__(self, *, status: Any, logs: Any, backups: Any, worlds: Any, updates: Any, notifications: Any, audit: Any, profiles: Any, benchmarks: Any, session_store: Any | None = None, tps_sampler: Any | None = None, telemetry_db: Any | None = None, telemetry_sampler: Any | None = None, telemetry_db_owned: bool = False, telemetry_collectors: Any | None = None, stats: Any | None = None, alerts: Any | None = None, telemetry_runtime: Any | None = None, container: Any | None = None, container_slot: _ContainerSlot | None = None, crafty_adapters: tuple[Any, ...] = ()):
        self.status, self.logs, self.backups, self.worlds, self.updates = status, logs, backups, worlds, updates
        self.notifications, self.audit, self.profiles, self.benchmarks = notifications, audit, profiles, benchmarks
        self.session_store, self.tps_sampler, self.telemetry_db, self.telemetry_sampler = session_store, tps_sampler, telemetry_db, telemetry_sampler
        self.telemetry_collectors, self.stats, self.alerts = telemetry_collectors, stats, alerts
        self.telemetry_runtime = telemetry_runtime
        self._crafty_adapters = tuple(crafty_adapters)
        self._container_slot = container_slot or _ContainerSlot()
        if container is not None:
            self._container_slot.bind(container)
        self._close_owner = container

    @property
    def container(self) -> Any | None:
        return self._container_slot.container

    def _finalize_container(self, container: Any) -> None:
        self._container_slot.bind(container)
        self._close_owner = container

    def close(self) -> None:
        if self._close_owner is None:
            raise RuntimeError("service container close owner is not finalized")
        self._close_owner.close()

    async def aclose(self) -> None:
        if self._close_owner is None:
            raise RuntimeError("service container close owner is not finalized")
        result = self._close_owner.aclose()
        if inspect.isawaitable(result):
            await result


def build_service_seams(profiles: Any, adapters: Mapping[Any, Any], state_db: Any, slot_inspector: Any, *, secret_dir: str | Path = DEFAULT_SECRET_DIR, secret_values: tuple[str, ...] = (), stats_config: Mapping[str, Any] | None = None, b2_transport: Any | None = None, sunlit_online_backup: Any | None = None, benchmark_config: Any = None, telemetry_db: Any | None = None, rcon_telemetry: Any | None = None, reservation_store: Any | None = None, telemetry_runtime: TelemetryRuntime | None = None, alert_runtime: AlertRuntime | None = None, history_queries: HistoryQueryService | None = None, container_slot: _ContainerSlot | None = None, crafty_adapters: tuple[Any, ...] = (), own_telemetry_resources: bool = False, register_owned: Callable[[Any, Any], Any] | None = None, _boundary_hook: Callable[[str], None] | None = None) -> ServiceSeams:
    def boundary(label: str) -> None:
        if _boundary_hook is not None:
            _boundary_hook(label)

    profile_items = tuple(profiles)
    profile_map = {_key(profile): profile for profile in profile_items}
    adapter_map = {_key(profile): adapters.get(getattr(profile, "id", None), adapters.get(_key(profile))) for profile in profile_items}
    sampler = MetricSampler()
    player_tracker = PlayerTracker()
    connection = _connection(state_db)
    session_store = SessionStore(connection) if connection is not None else None
    root_jobs, root_generation = RootActiveJobsReader(state_db), RootGenerationReader(state_db)
    root_wake = RootWakeSafetyEvidence(root_jobs, reservation_store)
    stats = stats_config if isinstance(stats_config, Mapping) else {}
    config = _legacy_telemetry_config(stats, tuple(profile_map))
    if telemetry_db is None and getattr(state_db, "path", None) is not None:
        telemetry_db = TelemetryDatabase.open(Path(state_db.path).with_name("telemetry.db"))
        if register_owned is not None:
            register_owned(telemetry_db, telemetry_db.close)
        boundary("telemetry_db")
    notification_service = NotificationService(profile_map, secret_dir=secret_dir, database=state_db, redactor=Redactor(SecretRegistry(secret_values)))
    if register_owned is not None:
        register_owned(notification_service, notification_service.close)
    boundary("notification")
    alert_was_injected = alert_runtime is not None
    alert_runtime = alert_runtime or AlertRuntime(profile_map, notification_service)
    if register_owned is not None and not alert_was_injected:
        register_owned(alert_runtime, alert_runtime.close)
    if not alert_was_injected:
        boundary("alert")
    collector = TelemetryCollector(profiles=profile_items, config=config, database=telemetry_db, rcon=rcon_telemetry, player_tracker=player_tracker, alert_sink=alert_runtime)
    def process_checker(current_profile: Any, observation: Any, *, connections: Mapping[str, list[Any]] | None = None, process_metrics: Any | None = None) -> bool:
        pid = getattr(observation, "pid", None)
        if not isinstance(pid, int) or pid <= 0:
            pid = None
        sampled = process_metrics
        if sampled is None:
            sampled = sampler.sample(current_profile, pid=pid, track_rates=False, connections=connections)
        sampled_pid = getattr(sampled, "pid", None)
        return sampled_pid == pid if pid is not None else sampled_pid is not None
    status_health = {profile.id: HealthChecker(adapter, process_checker=process_checker) for profile, adapter in ((profile, adapter_map[_key(profile)]) for profile in profile_items)}
    status_service = StatusService(profile_items, adapters=adapter_map, slot_observer=slot_inspector.observe, active_jobs=root_jobs, health_checker=status_health, metrics=sampler, player_tracker=player_tracker, session_store=session_store, generation=root_generation, telemetry_db=telemetry_db, capability_evidence=root_wake, ups_health=_prometheus_ups_provider(stats.get("benchmark_ups")), benchmark_safety=BenchmarkPreflight(storage_paths=("/srv/game-servers", "/var/lib/game-control"), ups_health=_prometheus_ups_provider(stats.get("benchmark_ups")), session_store=session_store, wake_evidence=root_wake), storage_paths=("/srv/game-servers", "/var/lib/game-control"))
    runtime_was_injected = telemetry_runtime is not None
    telemetry_runtime = telemetry_runtime or TelemetryRuntime(status_service, collector, database=(ResourceRef.owned(telemetry_db) if own_telemetry_resources else ResourceRef.borrowed(telemetry_db)) if telemetry_db is not None else None, rcon=(ResourceRef.owned(rcon_telemetry) if own_telemetry_resources else ResourceRef.borrowed(rcon_telemetry)) if rcon_telemetry is not None else None)
    if register_owned is not None and not runtime_was_injected:
        register_owned(telemetry_runtime, telemetry_runtime.close)
    if not runtime_was_injected:
        boundary("telemetry_runtime")
    status_service.telemetry_collectors, status_service.telemetry_sampler = collector, telemetry_runtime.sampler
    logs = _LogsFacade(profile_map, adapter_map, Redactor(SecretRegistry(secret_values)))
    backups = BackupRpcFacade(profile_map, adapter_map, state_db, b2_transport=b2_transport, sunlit_online_backup=sunlit_online_backup, telemetry_db=telemetry_db)
    sunlit_checker = (
        SunlitUpdateChecker()
        if ProfileId.MINECRAFT_SUNLIT_COBBLEMON.value in profile_map
        else None
    )
    if sunlit_checker is not None and register_owned is not None:
        register_owned(sunlit_checker, sunlit_checker.close)
    update_services: dict[str, UpdateService] = {}
    for key, profile in profile_map.items():
        update = UpdateService(
            {key: profile},
            database=state_db,
            backup_service=backups.services[key],
            manual_checker=(
                sunlit_checker.check
                if sunlit_checker is not None
                and key == ProfileId.MINECRAFT_SUNLIT_COBBLEMON.value
                else None
            ),
        )
        update_services[key] = update
        if register_owned is not None:
            register_owned(update, update.aclose)
        boundary(f"update:{key}")
    worlds = WorldService(profile_map.get(ProfileId.TERRARIA_VANILLA.value), profile_map.get(ProfileId.TERRARIA_TMOD.value), backup_service=backups.services.get(ProfileId.TERRARIA_VANILLA.value), stopped_check=lambda *_: True)
    benchmark_service = BenchmarkService(parse_benchmark_plans(benchmark_config), database=state_db, adapters=adapter_map, profiles=profile_map, slot_inspector=slot_inspector)
    history_was_injected = history_queries is not None
    history_queries = history_queries or HistoryQueryService(state_db, telemetry_db, approved_state_path=_AUDIT_STATE_DB_PATH, approved_telemetry_path=Path(_AUDIT_STATE_DB_PATH).with_name("telemetry.db"))
    if register_owned is not None and not history_was_injected:
        register_owned(history_queries, history_queries.aclose)
    if not history_was_injected:
        boundary("history")
    legacy_sampler = None
    if config.legacy_tps_mode is LegacyTpsMode.ENABLED and connection is not None:
        legacy_profile = ProfileId.MINECRAFT_SUNLIT_COBBLEMON.value if ProfileId.MINECRAFT_SUNLIT_COBBLEMON.value in profile_map else next(iter(profile_map), "minecraft")
        legacy_sampler = TpsSampler(connection, url=FIXED_EXPORTER_URL, profile_id=legacy_profile, interval_seconds=config.legacy_tps_interval_seconds)
    health_reason = "legacy_tps_override" if config.legacy_tps_mode is LegacyTpsMode.ENABLED and stats.get("exporter_url") is not None else None
    services = ServiceSeams(status=_StatusFacade(status_service, telemetry_health_reason=health_reason), logs=logs, backups=backups, worlds=WorldRpcFacade(worlds, profile_map, adapter_map), updates=UpdateRpcFacade(update_services, profile_map, adapter_map), notifications=NotificationRpcFacade(notification_service), audit=_AuditFacade(state_db, history_queries), profiles=_ProfilesFacade(profile_map), benchmarks=benchmark_service, session_store=session_store, tps_sampler=legacy_sampler, telemetry_db=telemetry_db, telemetry_sampler=telemetry_runtime.sampler, telemetry_runtime=telemetry_runtime, telemetry_collectors=collector, stats=_StatsFacade(state_db, telemetry_db, history_queries), alerts=alert_runtime, container_slot=container_slot, crafty_adapters=crafty_adapters)
    boundary("service_seams")
    return services


StatsCompatibilityFacade = _StatsFacade
class _BackupFacade(BackupRpcFacade):
    """Legacy test/import name with injectable module-level factories."""
    def __init__(self, *args: Any, **kwargs: Any):
        kwargs.setdefault("backup_service_factory", BackupService)
        kwargs.setdefault("restore_service_factory", RestoreService)
        kwargs.setdefault("isolated_database_factory", _isolated_database)
        kwargs.setdefault("close_database", _close_database)
        super().__init__(*args, **kwargs)


class _WorldFacade(WorldRpcFacade):
    pass


class _UpdateFacade(UpdateRpcFacade):
    def __init__(self, *args: Any, **kwargs: Any):
        kwargs.setdefault("update_service_factory", UpdateService)
        super().__init__(*args, **kwargs)


_NotificationFacade = NotificationRpcFacade

# Historical tests and downstream source lanes imported the implementation
# seam directly; keep the name as a policy-free compatibility alias.
_build_service_seams_impl = build_service_seams

__all__ = ["HistoryQueryService", "PerformanceResult", "PlayerCountResult", "ResourceRef", "ServiceSeams", "StatsCompatibilityFacade", "TelemetryCommand", "TelemetryRuntime", "TelemetryRuntimeConfig", "_AuditFacade", "_BackupFacade", "_BoundTelemetryCollectors", "_ContainerSlot", "_LogsFacade", "_NotificationFacade", "_PerformanceAlerts", "_ProfilesFacade", "_StatsFacade", "_StatusFacade", "_UpdateFacade", "_WorldFacade", "build_service_seams"]
