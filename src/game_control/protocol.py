"""Closed, safe RPC contract for the root game-control broker.

The protocol deliberately contains no paths, commands, credentials, URLs, or
unvalidated upstream values.  It is the boundary shared by the web client and
the privileged controller.
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictBool, TypeAdapter, ValidationError, field_validator, model_validator

from .models import (
    AdapterKind,
    BackupDestination,
    HealthState,
    NotificationEvent,
    ObservedState,
    OperationName,
    ProfileId,
)

MAX_REQUEST_BYTES = 64 * 1024
# Explicit framing budget shared by the privileged writer and web client.
MAX_RESPONSE_BYTES = 1024 * 1024


class RpcModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RpcProvenance(StrEnum):
    """Trusted source classification carried alongside a typed RPC request."""

    SERVICE = "service"
    WEB_HUMAN = "web-human"


class PageOptions(RpcModel):
    cursor: str | None = Field(default=None, max_length=256)
    limit: int = Field(default=100, ge=1, le=500)


class LogOptions(PageOptions):
    limit: int = Field(default=500, ge=1, le=5000)
    severity: Literal["all", "debug", "info", "warning", "error"] = "all"
    since: datetime | None = None
    until: datetime | None = None

    @field_validator("since", "until")
    @classmethod
    def require_aware_datetime(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("log range datetimes must include a timezone")
        return value


class SwitchOptions(RpcModel):
    create_backup: bool = False
    force_after_timeout: bool = False
    rollback_on_failure: bool = True


class GetStatus(RpcModel):
    kind: Literal["get_status"]
    refresh: bool = False


class Watch(RpcModel):
    """Long-lived read-only Unix watch subscription."""

    kind: Literal["watch"]
    cursor: int = Field(default=0, ge=0, le=2**63 - 1)
    generation: int = Field(default=0, ge=0)


class WaitReadiness(RpcModel):
    kind: Literal["wait_readiness"]
    profile_id: ProfileId
    generation: int | None = Field(default=None, ge=1)
    timeout_seconds: float = Field(default=300.0, ge=1, le=600)


class GetPerf(RpcModel):
    kind: Literal["get_perf"]


class GetProfiles(RpcModel):
    kind: Literal["get_profiles"]


class GetLogs(RpcModel):
    kind: Literal["get_logs"]
    profile_id: ProfileId
    page: LogOptions


class ListBackups(RpcModel):
    kind: Literal["list_backups"]
    profile_id: ProfileId
    page: PageOptions


class ListAggregateBackups(RpcModel):
    kind: Literal["list_aggregate_backups"]
    page: PageOptions


class ListEvents(RpcModel):
    kind: Literal["list_events"]
    page: PageOptions


class GetStatsSummary(RpcModel):
    kind: Literal["get_stats_summary"]
    profile_id: ProfileId
    days: int | None = Field(default=None, ge=1, le=3650)
    hours: int | None = Field(default=None, ge=1, le=87600)


class GetStatsHeatmap(RpcModel):
    kind: Literal["get_stats_heatmap"]
    profile_id: ProfileId
    days: int = Field(default=90, ge=1, le=365)
    hours: int | None = Field(default=None, ge=1, le=8760)


class GetStatsTps(RpcModel):
    kind: Literal["get_stats_tps"]
    profile_id: ProfileId
    window: Literal["1h", "6h", "24h", "7d", "30d", "1y"] = "6h"
    resolution: Literal["raw", "1m", "5m", "1h", "auto"] = "auto"
    limit: int = Field(default=500, ge=1, le=2000)


class GetProfileConfig(RpcModel):
    kind: Literal["get_profile_config"]
    profile_id: ProfileId


class GetBenchmarks(RpcModel):
    kind: Literal["get_benchmarks"]
    profile_id: ProfileId
    cursor: str | None = Field(default=None, max_length=128)
    limit: int = Field(default=20, ge=1, le=100)
    format: Literal["json", "csv"] = "json"


class ExportBenchmarks(RpcModel):
    kind: Literal["export_benchmarks"]
    profile_id: ProfileId
    format: Literal["json", "csv"] = "json"
    limit: int = Field(default=100, ge=1, le=100)


class BenchmarkExport(RpcModel):
    format: Literal["json", "csv"]
    content: str = Field(max_length=2_000_000)


class RunBenchmark(RpcModel):
    kind: Literal["run_benchmark"]
    profile_id: ProfileId
    baseline_preset: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")
    candidate_preset: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")

    @model_validator(mode="after")
    def presets_must_differ(self):
        if self.baseline_preset == self.candidate_preset:
            raise ValueError("baseline and candidate presets must differ")
        return self


class CancelBenchmark(RpcModel):
    kind: Literal["cancel_benchmark"]
    job_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


class SetProfileConfig(RpcModel):
    kind: Literal["set_profile_config"]
    profile_id: ProfileId
    changes: dict[str, Any] = Field(min_length=1, max_length=16)


class ScheduleSpec(RpcModel):
    cron: str = Field(min_length=9, max_length=128)
    profile: ProfileId
    enabled: StrictBool = True
    backup_destination: BackupDestination | None = None
    operation: Literal["backup", "switch", "benchmark"] = "backup"
    baseline_preset: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")
    candidate_preset: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")
    campaign: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
    maintenance_window: bool = False
    rollback_safe: bool = False
    public_wake_policy: Literal["disabled", "safe"] = "disabled"

    @field_validator("cron")
    @classmethod
    def validate_cron_text(cls, value: str) -> str:
        if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
            raise ValueError("cron contains a control character")
        return value.strip()


class GetSchedules(RpcModel):
    kind: Literal["get_schedules"]


class SetSchedules(RpcModel):
    kind: Literal["set_schedules"]
    entries: tuple[ScheduleSpec, ...] = Field(max_length=64)


class ScheduleView(RpcModel):
    cron: str
    profile: ProfileId
    next_fire: datetime | None
    enabled: bool = True
    backup_destination: BackupDestination | None = None
    operation: Literal["backup", "switch", "benchmark"] = "backup"
    baseline_preset: str | None = None
    candidate_preset: str | None = None
    campaign: str | None = None


class ScheduleResponse(RpcModel):
    schedules: tuple[ScheduleView, ...]


class ProfileConfigEntry(RpcModel):
    key: str
    value: Any | None
    configured: bool | None = None
    type: Literal["str", "int", "enum", "bool"]
    bounds: dict[str, Any]
    restart_required: bool


class ProfileConfigResponse(RpcModel):
    profile_id: ProfileId
    settings: tuple[ProfileConfigEntry, ...]
    changed: tuple[str, ...] = ()
    restart_required: tuple[str, ...] = ()


class BenchmarkPresetView(RpcModel):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")
    label: str = Field(min_length=1, max_length=64)


class BenchmarkMetricView(RpcModel):
    name: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,128}$")
    baseline_median: float = Field(alias="baselineMedian")
    candidate_median: float = Field(alias="candidateMedian")
    delta: float
    delta_percent: float = Field(alias="deltaPercent")
    ci_low: float = Field(alias="ciLow")
    ci_high: float = Field(alias="ciHigh")
    verdict: Literal["better", "worse", "inconclusive"]


class BenchmarkDiagnosticsView(RpcModel):
    dominant_bottleneck: str | None = Field(default=None, alias="dominantBottleneck", pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    leak_suspected: bool | None = Field(default=None, alias="leakSuspected")
    post_gc_slope_bytes_per_minute_median: float | None = Field(default=None, alias="postGcSlopeBytesPerMinuteMedian")
    load_reached_target: bool | None = Field(default=None, alias="loadReachedTarget")
    peak_connected_clients_median: float | None = Field(default=None, alias="peakConnectedClientsMedian", ge=0)
    process_duration_seconds_median: float | None = Field(default=None, alias="processDurationSecondsMedian", ge=0)


class BenchmarkRunSummary(RpcModel):
    id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,128}$")
    profile_id: ProfileId
    baseline_preset: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")
    candidate_preset: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")
    state: Literal["running", "succeeded", "failed"]
    created_at: datetime
    finished_at: datetime | None
    overall_verdict: Literal["better", "worse", "mixed", "inconclusive"] | None
    metrics: tuple[BenchmarkMetricView, ...] = Field(default=(), max_length=64)
    baseline_diagnostics: BenchmarkDiagnosticsView | None = None
    candidate_diagnostics: BenchmarkDiagnosticsView | None = None
    error_code: str | None = Field(default=None, pattern=r"^[a-z0-9_]{1,64}$")


class BenchmarkTrendMetric(RpcModel):
    name: str = Field(min_length=1, max_length=64)
    baseline_median: float | None = None
    candidate_median: float | None = None
    delta_percent: float | None = None
    verdict: Literal["better", "worse", "mixed", "inconclusive"]


class BenchmarkTrendPoint(RpcModel):
    id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,128}$")
    finished_at: datetime | None = None
    verdict: Literal["better", "worse", "mixed", "inconclusive"]
    metrics: tuple[BenchmarkTrendMetric, ...] = Field(default=(), max_length=16)


class BenchmarkOverview(RpcModel):
    profile_id: ProfileId
    available: bool
    presets: tuple[BenchmarkPresetView, ...] = Field(max_length=16)
    runs: tuple[BenchmarkRunSummary, ...] = Field(max_length=20)
    next_cursor: str | None = None
    corrupt_runs: int = 0
    trends: tuple[BenchmarkTrendPoint, ...] = Field(default=(), max_length=20)


class ListAudit(RpcModel):
    kind: Literal["list_audit"]
    page: PageOptions


class Start(RpcModel):
    kind: Literal["start"]
    profile_id: ProfileId


class Stop(RpcModel):
    kind: Literal["stop"]
    profile_id: ProfileId


class SetIdleStop(RpcModel):
    kind: Literal["set_idle_stop"]
    profile_id: ProfileId
    minutes: int = Field(ge=0, le=1440)

    @field_validator("minutes")
    @classmethod
    def validate_minutes(cls, value: int) -> int:
        if 0 < value < 5:
            raise ValueError("idle stop must be disabled or between 5 and 1440 minutes")
        return value


class Restart(RpcModel):
    kind: Literal["restart"]
    profile_id: ProfileId
    confirmation_id: str | None = None


class Command(RpcModel):
    kind: Literal["command"]
    profile_id: ProfileId
    command: str = Field(max_length=512)

    @field_validator("command", mode="before")
    @classmethod
    def validate_command(cls, value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("command must be text")
        if len(value) > 512:
            raise ValueError("command is too long")
        if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
            raise ValueError("command contains a control character")
        normalized = value.strip(" ")
        if not normalized:
            raise ValueError("command must not be empty")
        return normalized


class PrepareSwitch(RpcModel):
    kind: Literal["prepare_switch"]
    current_profile_id: ProfileId
    target_profile_id: ProfileId
    options: SwitchOptions


class ConfirmSwitch(RpcModel):
    kind: Literal["confirm_switch"]
    confirmation_id: str = Field(min_length=32, max_length=128)


class PrepareForceStop(RpcModel):
    kind: Literal["prepare_force_stop"]
    profile_id: ProfileId


class ConfirmForceStop(RpcModel):
    kind: Literal["confirm_force_stop"]
    confirmation_id: str = Field(min_length=32, max_length=128)


class CreateBackup(RpcModel):
    kind: Literal["create_backup"]
    profile_id: ProfileId
    protected: bool = False
    destination: BackupDestination = BackupDestination.LOCAL
    # Transport-only authorization token for the Sunlit update handoff.  It is
    # never part of the durable request identity, and ``repr=False`` keeps it
    # out of model/validation repr output and error text.
    reservation_capability: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$", repr=False
    )


class PrepareRestore(RpcModel):
    kind: Literal["prepare_restore"]
    profile_id: ProfileId
    backup_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,128}$")


class ConfirmRestore(RpcModel):
    kind: Literal["confirm_restore"]
    confirmation_id: str = Field(min_length=32, max_length=128)


class PrepareWorldClone(RpcModel):
    kind: Literal["prepare_world_clone"]
    source_world_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,128}$")
    destination_name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,63}$")


class ConfirmWorldClone(RpcModel):
    kind: Literal["confirm_world_clone"]
    confirmation_id: str = Field(min_length=32, max_length=128)


class CheckUpdate(RpcModel):
    kind: Literal["check_update"]
    profile_id: ProfileId


class PrepareUpdate(RpcModel):
    kind: Literal["prepare_update"]
    profile_id: ProfileId


class ConfirmUpdate(RpcModel):
    kind: Literal["confirm_update"]
    confirmation_id: str = Field(min_length=32, max_length=128)


class GetNotificationConfig(RpcModel):
    kind: Literal["get_notification_config"]
    profile_id: ProfileId


RetirementPhase = Literal["quarantine", "purge", "rollback"]
PayloadAvailability = Literal[
    "present", "missing", "prepared", "quarantined", "purge_prepared",
    "purged", "rolled_back", "failed", "ambiguous",
]


class GetRetirementStatus(RpcModel):
    kind: Literal["get_retirement_status"]
    operation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class PrepareRetirement(RpcModel):
    kind: Literal["prepare_retirement"]
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    phase: RetirementPhase


class ConfirmRetirement(RpcModel):
    kind: Literal["confirm_retirement"]
    confirmation_id: str = Field(min_length=32, max_length=128)


class SetNotificationRule(RpcModel):
    kind: Literal["set_notification_rule"]
    profile_id: ProfileId
    event: NotificationEvent
    enabled: bool


class TestNotification(RpcModel):
    kind: Literal["test_notification"]
    channel: Literal["discord", "telegram"]
    profile_id: ProfileId


RpcAction: TypeAlias = Annotated[
    GetStatus
    | Watch
    | WaitReadiness
    | GetPerf
    | GetProfiles
    | GetLogs
    | ListBackups
    | ListAggregateBackups
    | ListEvents
    | GetStatsSummary
    | GetStatsHeatmap
    | GetStatsTps
    | GetProfileConfig
    | GetBenchmarks
    | ExportBenchmarks
    | RunBenchmark
    | CancelBenchmark
    | SetProfileConfig
    | GetSchedules
    | SetSchedules
    | ListAudit
    | Start
    | Stop
    | SetIdleStop
    | Restart
    | Command
    | PrepareSwitch
    | ConfirmSwitch
    | PrepareForceStop
    | ConfirmForceStop
    | CreateBackup
    | PrepareRestore
    | ConfirmRestore
    | PrepareWorldClone
    | ConfirmWorldClone
    | CheckUpdate
    | PrepareUpdate
    | ConfirmUpdate
    | GetNotificationConfig
    | SetNotificationRule
    | TestNotification
    | GetRetirementStatus
    | PrepareRetirement
    | ConfirmRetirement,
    Field(discriminator="kind"),
]


class RpcRequest(RpcModel):
    request_id: UUID
    actor: str = Field(pattern=r"^[A-Za-z0-9@._-]{1,128}$")
    provenance: RpcProvenance = RpcProvenance.SERVICE
    action: RpcAction


class ErrorCode(StrEnum):
    SLOT_CONFLICT = "slot_conflict"
    PROFILE_RESERVED = "profile_reserved"
    LOW_DISK = "low_disk"
    LOW_MEMORY = "low_memory"
    REQUIRED_FILE_MISSING = "required_file_missing"
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"
    START_TIMEOUT = "start_timeout"
    HEALTH_FAILED = "health_failed"
    GRACE_TIMEOUT = "grace_timeout"
    BACKUP_FAILED = "backup_failed"
    RESTORE_FAILED = "restore_failed"
    UPDATE_FAILED = "update_failed"
    BENCHMARK_FAILED = "benchmark_failed"
    CONFIRMATION_EXPIRED = "confirmation_expired"
    CONFIRMATION_MISMATCH = "confirmation_mismatch"
    REQUEST_ID_CONFLICT = "request_id_conflict"
    RETIREMENT_FAILED = "retirement_failed"
    RETIREMENT_UNAVAILABLE = "retirement_unavailable"
    UNAUTHORIZED_PEER = "unauthorized_peer"
    INVALID_REQUEST = "invalid_request"
    INVALID_STATE = "invalid_state"
    INTERNAL_ERROR = "internal_error"


class SafeDetails(RpcModel):
    profile_id: ProfileId | None = None
    current_owner: ProfileId | None = None
    target_profile_id: ProfileId | None = None
    expected_timeout_seconds: int | None = Field(default=None, ge=0, le=1800)
    last_backup_id: str | None = Field(
        default=None, pattern=r"^[A-Za-z0-9_-]{1,128}$"
    )
    allowed_actions: tuple[OperationName, ...] = ()
    incident_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_-]{1,64}$")
    retry_after_seconds: int | None = Field(default=None, ge=0, le=3600)


class RpcError(RpcModel):
    code: ErrorCode
    message: str = Field(max_length=512)
    retryable: bool
    details: SafeDetails | None = None


class PublicEndpoint(RpcModel):
    host: str
    port: int
    protocol: Literal["tcp", "udp"]
    reachable: bool | None


class PublicProfile(RpcModel):
    id: ProfileId
    display_name: str
    adapter: AdapterKind
    operations: frozenset[OperationName]
    public_endpoint: PublicEndpoint | None
    idle_stop_minutes: int = Field(ge=0, le=1440)


class StartupEstimate(RpcModel):
    """Controller-owned startup estimate for the current run.

    ``sample_count`` and ``median_seconds`` come from the bounded per-profile,
    per-version history.  ``attempt_id``/``elapsed_seconds`` identify the
    authoritative in-flight attempt so an observing browser can resume the
    same estimate instead of restarting at zero.  The fields are absent for
    producers that do not collect them.
    """

    sample_count: int = Field(default=0, ge=0, le=25)
    median_seconds: float | None = Field(default=None, gt=0, le=900)
    attempt_id: str | None = Field(default=None, max_length=64)
    elapsed_seconds: float | None = Field(default=None, ge=0, le=86400)
    version: str | None = Field(default=None, max_length=64)


class ProfileStatus(RpcModel):
    profile_id: ProfileId
    state: ObservedState
    health: HealthState
    slot_owner: ProfileId | None
    active_job_id: str | None
    pid: int | None
    started_at: datetime | None
    uptime_seconds: int | None
    cpu_percent: float | None
    rss_bytes: int | None
    players_online: int | None
    installed_version: str | None
    restart_required: bool
    required_ports_ready: bool
    disk_free_bytes: int | None = None
    disk_read_bps: float | None = None
    disk_write_bps: float | None = None
    startup_estimate: StartupEstimate | None = None


class StatusSnapshot(RpcModel):
    generation: int = Field(ge=0)
    observed_at: datetime
    profiles: tuple[ProfileStatus, ...]
    initializing: bool = False


class PerfAggregate(RpcModel):
    count: int = Field(ge=0)
    avg_ms: float | None = Field(default=None, ge=0)
    p95_ms: float | None = Field(default=None, ge=0)
    max_ms: float | None = Field(default=None, ge=0)


class PerfDatabaseTable(RpcModel):
    name: str
    row_count: int = Field(ge=0)
    oldest_timestamp: datetime | None = None


class PerfDatabase(RpcModel):
    state: Literal["available", "inactive", "unavailable"]
    tables: tuple[PerfDatabaseTable, ...] = ()
    page_count: int | None = Field(default=None, ge=0)
    page_size: int | None = Field(default=None, ge=0)
    freelist_pages: int | None = Field(default=None, ge=0)
    wal_bytes: int | None = Field(default=None, ge=0)
    query_ms: PerfAggregate = Field(default_factory=lambda: PerfAggregate(count=0))


class PerfSnapshot(RpcModel):
    cycle: PerfAggregate
    rpc: PerfAggregate
    maintenance: PerfAggregate = Field(default_factory=lambda: PerfAggregate(count=0))
    maintenance_ms: tuple[float, ...] = ()
    maintenance_sequence: dict[str, int] = Field(default_factory=lambda: {"start": 0, "end": 0})
    event_loop_lag_ms: tuple[float, ...] = ()
    event_loop_lag_sequence: dict[str, int] = Field(default_factory=lambda: {"start": 0, "end": 0})
    databases: dict[str, PerfDatabase] = Field(default_factory=dict)


class LogLine(RpcModel):
    timestamp: datetime
    severity: Literal["debug", "info", "warning", "error"]
    message: str = Field(max_length=8192)


class LogPage(RpcModel):
    items: tuple[LogLine, ...]
    next_cursor: str | None


class BackupSummary(RpcModel):
    id: str
    profile_id: ProfileId
    created_at: datetime
    size_bytes: int = Field(ge=0)
    verified: bool
    protected: bool
    local_payload_state: PayloadAvailability = "present"


class BackupPage(RpcModel):
    items: tuple[BackupSummary, ...]
    next_cursor: str | None


class EventSummary(RpcModel):
    id: str
    timestamp: datetime
    profile_id: ProfileId | None
    code: str = Field(max_length=64)
    message: str = Field(max_length=512)


class EventPage(RpcModel):
    items: tuple[EventSummary, ...]
    next_cursor: str | None


class AuditSummary(RpcModel):
    id: str
    timestamp: datetime
    actor: str
    action: str
    profile_id: ProfileId | None
    result: Literal["accepted", "succeeded", "failed", "rejected"]
    error_code: ErrorCode | None
    detail: str = Field(max_length=512)


class AuditPage(RpcModel):
    items: tuple[AuditSummary, ...]
    next_cursor: str | None


class JobAccepted(RpcModel):
    job_id: str
    state: Literal["accepted", "running", "succeeded", "failed", "cancelled"]
    readiness_generation: int | None = Field(default=None, ge=1)
    readiness: Literal["success", "failure", "timeout"] | None = None


class ReadinessResult(RpcModel):
    profile_id: ProfileId
    generation: int = Field(ge=1)
    outcome: Literal["success", "failure", "timeout"]


class ConfirmationBase(RpcModel):
    confirmation_id: str
    expires_at: datetime
    summary_hash: str
    state_generation: int = Field(ge=0)


class SwitchConfirmation(ConfirmationBase):
    action: Literal["switch"]
    source_profile_id: ProfileId
    target_profile_id: ProfileId
    create_backup: bool
    force_after_timeout: bool
    rollback_on_failure: bool


class ForceStopConfirmation(ConfirmationBase):
    action: Literal["force_stop"]
    profile_id: ProfileId


class RestoreConfirmation(ConfirmationBase):
    action: Literal["restore"]
    profile_id: ProfileId
    backup_id: str


class WorldCloneConfirmation(ConfirmationBase):
    action: Literal["world_clone"]
    source_profile_id: Literal[ProfileId.TERRARIA_VANILLA]
    target_profile_id: Literal[ProfileId.TERRARIA_TMOD]
    source_world_id: str
    destination_name: str


class UpdateConfirmation(ConfirmationBase):
    action: Literal["update"]
    profile_id: ProfileId
    installed_version: str | None
    available_version: str | None


class RetirementConfirmation(ConfirmationBase):
    action: Literal["retirement"]
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    phase: RetirementPhase
    destination_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    count: int = Field(ge=1)
    bytes: int = Field(ge=0)
    profile_ids: tuple[str, ...] = ()


class RetirementEntryStatus(RpcModel):
    backup_id: str
    profile_id: str
    state: str = Field(max_length=32)
    error_code: str | None = Field(default=None, max_length=64)


class RetirementReconcileEntry(RpcModel):
    backup_id: str
    profile_id: str
    ledger_state: str = Field(max_length=32)
    classification: str = Field(max_length=32)


class RetirementStatus(RpcModel):
    operation_id: str = ""
    counts: dict[str, int] = Field(default_factory=dict)
    entries: tuple[RetirementEntryStatus, ...] = ()
    reconciled: tuple[RetirementReconcileEntry, ...] = ()
    sender_lock_available: bool = True
    noreplace_supported: bool = True


ConfirmationSummary: TypeAlias = Annotated[
    SwitchConfirmation
    | ForceStopConfirmation
    | RestoreConfirmation
    | WorldCloneConfirmation
    | UpdateConfirmation
    | RetirementConfirmation,
    Field(discriminator="action"),
]


class UpdateStatus(RpcModel):
    profile_id: ProfileId
    strategy: Literal["manual", "steamcmd_in_place", "release_symlink", "curated_modpack"]
    installed_version: str | None
    available_version: str | None
    restart_required: bool
    apply_supported: bool
    state: Literal[
        "unknown", "current", "available", "deferred", "stale", "failed",
        "checking", "unsupported",
    ] = "unknown"
    message: str | None = Field(default=None, max_length=200)
    checked_at: datetime | None = None


class NotificationTarget(RpcModel):
    channel: Literal["discord", "telegram"]
    configured: bool
    label: str | None


class NotificationConfig(RpcModel):
    profile_id: ProfileId
    targets: tuple[NotificationTarget, ...]
    rules: dict[NotificationEvent, bool]


RpcResult: TypeAlias = (
    StatusSnapshot
    | ReadinessResult
    | PerfSnapshot
    | tuple[PublicProfile, ...]
    | LogPage
    | BackupPage
    | EventPage
    | AuditPage
    | JobAccepted
    | ConfirmationSummary
    | UpdateStatus
    | RetirementStatus
    | NotificationConfig
    | ProfileConfigResponse
    | BenchmarkOverview
    | BenchmarkExport
    | ScheduleResponse
    | dict[str, Any]
)


class RpcSuccess(RpcModel):
    request_id: UUID
    ok: Literal[True] = True
    result: RpcResult


class RpcFailure(RpcModel):
    request_id: UUID
    ok: Literal[False] = False
    error: RpcError


RpcResponse: TypeAlias = Annotated[
    RpcSuccess | RpcFailure, Field(discriminator="ok")
]


_REQUEST_ADAPTER = TypeAdapter(RpcRequest)
_RESPONSE_ADAPTER = TypeAdapter(RpcResponse)


def parse_request_line(data: bytes) -> RpcRequest:
    """Parse exactly one bounded JSON object from a newline-delimited frame."""

    if not isinstance(data, bytes):
        raise TypeError("request frame must be bytes")
    if len(data) > MAX_REQUEST_BYTES:
        raise ValueError("request too large")
    if not data.endswith(b"\n"):
        raise ValueError("request must end with newline")
    body = data[:-1]
    if not body or b"\n" in body or b"\r" in body:
        raise ValueError("request must contain one JSON object")
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid JSON request") from exc
    if not isinstance(value, dict):
        raise ValueError("request must be a JSON object")
    try:
        return _REQUEST_ADAPTER.validate_python(value)
    except ValidationError as exc:
        raise ValueError("invalid RPC request") from exc


def response_from_json(data: bytes) -> RpcSuccess | RpcFailure:
    """Validate a response envelope before writing it to a socket."""

    return _RESPONSE_ADAPTER.validate_json(data)


def response_json(response: RpcSuccess | RpcFailure) -> bytes:
    return _RESPONSE_ADAPTER.dump_json(response) + b"\n"


def success(request_id: UUID, result: RpcResult) -> RpcSuccess:
    return RpcSuccess(request_id=request_id, result=result)


def failure(
    request_id: UUID,
    code: ErrorCode,
    message: str,
    *,
    retryable: bool = False,
    details: SafeDetails | None = None,
) -> RpcFailure:
    return RpcFailure(
        request_id=request_id,
        error=RpcError(code=code, message=message[:512], retryable=retryable, details=details),
    )


__all__ = [
    "MAX_REQUEST_BYTES",
    "MAX_RESPONSE_BYTES",
    "RpcModel",
    "PageOptions",
    "LogOptions",
    "SwitchOptions",
    "RpcAction",
    "Command",
    "RpcRequest",
    "RpcResponse",
    "RpcSuccess",
    "RpcFailure",
    "RpcResult",
    "ErrorCode",
    "SafeDetails",
    "RpcError",
    "PublicEndpoint",
    "PublicProfile",
    "ProfileStatus",
    "StatusSnapshot",
    "WaitReadiness",
    "Watch",
    "ReadinessResult",
    "PerfAggregate",
    "PerfDatabaseTable",
    "PerfDatabase",
    "PerfSnapshot",
    "LogLine",
    "LogPage",
    "BackupSummary",
    "BackupPage",
    "ListAggregateBackups",
    "GetRetirementStatus",
    "PrepareRetirement",
    "ConfirmRetirement",
    "RetirementConfirmation",
    "RetirementEntryStatus",
    "RetirementReconcileEntry",
    "RetirementStatus",
    "RetirementPhase",
    "PayloadAvailability",
    "EventSummary",
    "EventPage",
    "GetStatsSummary",
    "GetStatsHeatmap",
    "GetStatsTps",
    "GetProfileConfig",
    "SetProfileConfig",
    "GetSchedules",
    "SetSchedules",
    "ScheduleSpec",
    "ScheduleView",
    "ScheduleResponse",
    "AuditSummary",
    "AuditPage",
    "JobAccepted",
    "ConfirmationSummary",
    "SwitchConfirmation",
    "ForceStopConfirmation",
    "RestoreConfirmation",
    "WorldCloneConfirmation",
    "UpdateConfirmation",
    "UpdateStatus",
    "NotificationTarget",
    "NotificationConfig",
    "ProfileConfigEntry",
    "ProfileConfigResponse",
    "parse_request_line",
    "response_from_json",
    "response_json",
    "success",
    "failure",
]
