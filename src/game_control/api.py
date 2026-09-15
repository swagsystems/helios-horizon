"""Closed REST projection of the typed Unix RPC protocol.

There is intentionally no filesystem, subprocess, database, or URL handling
in this module.  Every endpoint constructs one closed ``RpcAction`` and makes
one call through the injected RPC client.
"""

from __future__ import annotations

import inspect
import json
import asyncio
from typing import Any, Callable, Literal, Mapping
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse, Response as FastAPIResponse
import csv
import io
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .protocol import (
    CheckUpdate,
    Command,
    ConfirmForceStop,
    ConfirmRestore,
    ConfirmSwitch,
    ConfirmUpdate,
    ConfirmWorldClone,
    CreateBackup,
    ConfirmRetirement,
    ErrorCode,
    GetLogs,
    GetNotificationConfig,
    GetProfiles,
    GetStatus,
    GetStatsHeatmap,
    GetStatsSummary,
    GetStatsTps,
    GetProfileConfig,
    GetBenchmarks,
    ExportBenchmarks,
    SetProfileConfig,
    GetSchedules,
    ScheduleSpec,
    SetSchedules,
    ListAudit,
    ListBackups,
    ListAggregateBackups,
    PrepareRetirement,
    GetRetirementStatus,
    ListEvents,
    LogOptions,
    PageOptions,
    PrepareForceStop,
    PrepareRestore,
    PrepareSwitch,
    PrepareUpdate,
    PrepareWorldClone,
    Restart,
    RunBenchmark,
    CancelBenchmark,
    RpcAction,
    RpcFailure,
    RpcProvenance,
    RpcRequest,
    RpcResponse,
    RpcSuccess,
    SetNotificationRule,
    SetIdleStop,
    Start,
    Stop,
    SwitchOptions,
    TestNotification,
)
from .models import BackupDestination, NotificationEvent, ProfileId
from .introspection import signature_parameters


class StrictBody(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SwitchBody(StrictBody):
    current_profile_id: ProfileId
    target_profile_id: ProfileId
    create_backup: bool = False
    force_after_timeout: bool = False
    rollback_on_failure: bool = True


class ConfirmBody(StrictBody):
    confirmation_id: str = Field(min_length=32, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")


class BackupBody(StrictBody):
    protected: bool = False
    destination: BackupDestination = BackupDestination.LOCAL


class RestoreBody(StrictBody):
    backup_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")


class RetirementBody(StrictBody):
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    phase: Literal["quarantine", "purge", "rollback"]


class CloneBody(StrictBody):
    source_world_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    destination_name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,63}$")


class NotificationRuleBody(StrictBody):
    event: NotificationEvent
    enabled: bool


class NotificationTestBody(StrictBody):
    channel: str = Field(pattern=r"^(discord|telegram)$")
    profile_id: ProfileId | None = None


class IdleStopBody(StrictBody):
    minutes: int = Field(ge=0, le=1440)

    @field_validator("minutes")
    @classmethod
    def validate_minutes(cls, value: int) -> int:
        if 0 < value < 5:
            raise ValueError("idle stop must be disabled or between 5 and 1440 minutes")
        return value


class SchedulesBody(StrictBody):
    entries: tuple[ScheduleSpec, ...] = Field(max_length=64)


class BenchmarkBody(StrictBody):
    baseline_preset: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")
    candidate_preset: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")

    @field_validator("candidate_preset")
    @classmethod
    def presets_differ(cls, value: str, info):
        if value == info.data.get("baseline_preset"):
            raise ValueError("baseline and candidate presets must differ")
        return value


# Log records are bounded by a byte budget in the controller; retain the
# protocol's public count limit for compatibility and cursor pagination.
MAX_LOG_PAGE_RECORDS = 5000
MAX_MUTATION_BODY_BYTES = 64 * 1024
BODY_READ_TIMEOUT_SECONDS = 5.0


async def _read_mutation_body(request: Request) -> dict[str, Any]:
    """Read a mutation body with one total deadline and hard byte bound."""
    declared_values = request.headers.getlist("content-length")
    if len(declared_values) > 1:
        raise HTTPException(400, "invalid content length")
    declared = declared_values[0] if declared_values else None
    if declared is not None and not declared.isdigit():
        raise HTTPException(400, "invalid content length")
    if declared is not None and int(declared) > MAX_MUTATION_BODY_BYTES:
        raise HTTPException(413, "request body too large")
    raw = bytearray()
    try:
        async with asyncio.timeout(BODY_READ_TIMEOUT_SECONDS):
            async for chunk in request.stream():
                if len(raw) + len(chunk) > MAX_MUTATION_BODY_BYTES:
                    raise HTTPException(413, "request body too large")
                raw.extend(chunk)
    except TimeoutError as exc:
        raise HTTPException(408, "request body read timed out") from exc
    if declared is not None and int(declared) != len(raw):
        raise HTTPException(400, "content length mismatch")
    try:
        payload = json.loads(bytes(raw)) if raw else {}
    except Exception as exc:
        raise HTTPException(422, "invalid request") from exc
    if not isinstance(payload, dict):
        raise HTTPException(422, "invalid request")
    return payload


ROUTE_ACTIONS: dict[str, type] = {
    "GET /api/v1/status": GetStatus,
    "GET /api/v1/profiles": GetProfiles,
    "GET /api/v1/profiles/{profile_id}": GetProfiles,
    "GET /api/v1/profiles/{profile_id}/logs": GetLogs,
    "GET /api/v1/profiles/{profile_id}/backups": ListBackups,
    "GET /api/v1/backups": ListAggregateBackups,
    "GET /api/v1/profiles/{profile_id}/stats/summary": GetStatsSummary,
    "GET /api/v1/profiles/{profile_id}/stats/heatmap": GetStatsHeatmap,
    "GET /api/v1/profiles/{profile_id}/stats/tps": GetStatsTps,
    "GET /api/v1/profiles/{profile_id}/config": GetProfileConfig,
    "GET /api/v1/profiles/{profile_id}/benchmarks": GetBenchmarks,
    "GET /api/v1/benchmarks/{profile_id}/export": ExportBenchmarks,
    "GET /api/v1/schedules": GetSchedules,
    "GET /api/v1/events": ListEvents,
    "GET /api/v1/audit": ListAudit,
    "POST /api/v1/profiles/{profile_id}/start": Start,
    "POST /api/v1/profiles/{profile_id}/stop": Stop,
    "POST /api/v1/profiles/{profile_id}/restart": Restart,
    "POST /api/v1/profiles/{profile_id}/command": Command,
    "POST /api/v1/switch/prepare": PrepareSwitch,
    "POST /api/v1/switch/confirm": ConfirmSwitch,
    "POST /api/v1/profiles/{profile_id}/force-stop/prepare": PrepareForceStop,
    "POST /api/v1/profiles/{profile_id}/force-stop": PrepareForceStop,
    "POST /api/v1/force-stop/confirm": ConfirmForceStop,
    "POST /api/v1/profiles/{profile_id}/backups": CreateBackup,
    "POST /api/v1/profiles/{profile_id}/restore/prepare": PrepareRestore,
    "POST /api/v1/profiles/{profile_id}/restore": PrepareRestore,
    "POST /api/v1/restore/confirm": ConfirmRestore,
    "GET /api/v1/retirement": GetRetirementStatus,
    "POST /api/v1/retirement/prepare": PrepareRetirement,
    "POST /api/v1/retirement/confirm": ConfirmRetirement,
    "POST /api/v1/world-clone/prepare": PrepareWorldClone,
    "POST /api/v1/worlds/clone": PrepareWorldClone,
    "POST /api/v1/world-clone/confirm": ConfirmWorldClone,
    "GET /api/v1/profiles/{profile_id}/update": CheckUpdate,
    "POST /api/v1/profiles/{profile_id}/update/prepare": PrepareUpdate,
    "POST /api/v1/profiles/{profile_id}/update": PrepareUpdate,
    "POST /api/v1/update/confirm": ConfirmUpdate,
    "GET /api/v1/profiles/{profile_id}/notifications": GetNotificationConfig,
    "POST /api/v1/profiles/{profile_id}/notifications/rule": SetNotificationRule,
    "PATCH /api/v1/profiles/{profile_id}/idle-stop": SetIdleStop,
    "POST /api/v1/profiles/{profile_id}/notifications/test": TestNotification,
    "POST /api/v1/profiles/{profile_id}/config": SetProfileConfig,
    "POST /api/v1/profiles/{profile_id}/benchmarks": RunBenchmark,
    "POST /api/v1/benchmarks/{job_id}/cancel": CancelBenchmark,
    "POST /api/v1/schedules": SetSchedules,
    "POST /api/v1/notifications/test": TestNotification,
}


def _page(request: Request, *, logs: bool = False):
    try:
        limit = int(request.query_params.get("limit", "100"))
    except ValueError as exc:
        raise HTTPException(422, "invalid pagination") from exc
    if not 1 <= limit <= (MAX_LOG_PAGE_RECORDS if logs else 500):
        raise HTTPException(422, "invalid pagination")
    cursor = request.query_params.get("cursor")
    if cursor is not None and (len(cursor) > 256 or "\x00" in cursor):
        raise HTTPException(422, "invalid pagination")
    try:
        if logs:
            severity = request.query_params.get("severity", "all")
            return LogOptions(
                limit=limit,
                cursor=cursor,
                severity=severity,
                since=request.query_params.get("since"),
                until=request.query_params.get("until"),
            )
        return PageOptions(limit=limit, cursor=cursor)
    except ValidationError as exc:
        raise HTTPException(422, "invalid pagination") from exc


def _action(path: str, method: str, profile_id: ProfileId | None, payload: Mapping[str, Any], request: Request) -> RpcAction:
    key = f"{method.upper()} {path}"
    action_type = ROUTE_ACTIONS.get(key)
    if action_type is None:
        method_prefix = f"{method.upper()} "
        for template, candidate in ROUTE_ACTIONS.items():
            if not template.startswith(method_prefix):
                continue
            template_path = template[len(method_prefix):].split("/")
            actual_path = path.split("/")
            if len(template_path) != len(actual_path):
                continue
            if all(expected == actual or expected.startswith("{") for expected, actual in zip(template_path, actual_path)):
                action_type = candidate
                break
    if action_type is None:
        raise HTTPException(404, "not found")
    try:
        if action_type is GetStatus:
            return GetStatus(kind="get_status")
        if action_type is GetProfiles:
            if payload:
                raise HTTPException(422, "invalid request")
            return GetProfiles(kind="get_profiles")
        if action_type is GetLogs:
            return GetLogs(kind="get_logs", profile_id=profile_id, page=_page(request, logs=True))
        if action_type is ListBackups:
            return ListBackups(kind="list_backups", profile_id=profile_id, page=_page(request))
        if action_type is ListAggregateBackups:
            return ListAggregateBackups(kind="list_aggregate_backups", page=_page(request))
        if action_type is ListEvents:
            return ListEvents(kind="list_events", page=_page(request))
        if action_type is GetStatsSummary:
            return GetStatsSummary(
                kind="get_stats_summary",
                profile_id=profile_id,
                days=request.query_params.get("days"),
                hours=request.query_params.get("hours"),
            )
        if action_type is GetStatsHeatmap:
            return GetStatsHeatmap(
                kind="get_stats_heatmap",
                profile_id=profile_id,
                days=request.query_params.get("days", 90),
                hours=request.query_params.get("hours"),
            )
        if action_type is GetStatsTps:
            return GetStatsTps(
                kind="get_stats_tps",
                profile_id=profile_id,
                window=request.query_params.get("window", "6h"),
                resolution=request.query_params.get("resolution", "auto"),
                limit=request.query_params.get("limit", 500),
            )
        if action_type is GetProfileConfig:
            if payload:
                raise HTTPException(422, "invalid request")
            return GetProfileConfig(kind="get_profile_config", profile_id=profile_id)
        if action_type is GetBenchmarks:
            return GetBenchmarks(kind="get_benchmarks", profile_id=profile_id, **{key: value for key, value in payload.items() if value is not None})
        if action_type is ExportBenchmarks:
            return ExportBenchmarks(kind="export_benchmarks", profile_id=profile_id, **{key: value for key, value in payload.items() if value is not None})
        if action_type is RunBenchmark:
            body = BenchmarkBody.model_validate(payload)
            return RunBenchmark(kind="run_benchmark", profile_id=profile_id, **body.model_dump())
        if action_type is CancelBenchmark:
            if set(payload) != {"job_id"}:
                raise HTTPException(422, "invalid request")
            return CancelBenchmark(kind="cancel_benchmark", job_id=str(payload["job_id"]))
        if action_type is GetSchedules:
            if payload:
                raise HTTPException(422, "invalid request")
            return GetSchedules(kind="get_schedules")
        if action_type is SetProfileConfig:
            return SetProfileConfig(kind="set_profile_config", profile_id=profile_id, changes=payload.get("changes", {}))
        if action_type is SetSchedules:
            return SetSchedules(kind="set_schedules", entries=SchedulesBody.model_validate(payload).entries)
        if action_type is ListAudit:
            return ListAudit(kind="list_audit", page=_page(request))
        if action_type is Start:
            if payload:
                raise HTTPException(422, "invalid request")
            return Start(kind="start", profile_id=profile_id)
        if action_type is Stop:
            if payload:
                raise HTTPException(422, "invalid request")
            return Stop(kind="stop", profile_id=profile_id)
        if action_type is Restart:
            return Restart(kind="restart", profile_id=profile_id, **payload)
        if action_type is Command:
            return Command(kind="command", profile_id=profile_id, **payload)
        if action_type is PrepareSwitch:
            body = SwitchBody.model_validate(payload)
            return PrepareSwitch(
                kind="prepare_switch",
                current_profile_id=body.current_profile_id,
                target_profile_id=body.target_profile_id,
                options=SwitchOptions(
                    create_backup=body.create_backup,
                    force_after_timeout=body.force_after_timeout,
                    rollback_on_failure=body.rollback_on_failure,
                ),
            )
        if action_type is ConfirmSwitch:
            return ConfirmSwitch(kind="confirm_switch", **ConfirmBody.model_validate(payload).model_dump())
        if action_type is PrepareForceStop:
            if payload:
                raise HTTPException(422, "invalid request")
            return PrepareForceStop(kind="prepare_force_stop", profile_id=profile_id)
        if action_type is ConfirmForceStop:
            return ConfirmForceStop(kind="confirm_force_stop", **ConfirmBody.model_validate(payload).model_dump())
        if action_type is CreateBackup:
            return CreateBackup(kind="create_backup", profile_id=profile_id, **BackupBody.model_validate(payload).model_dump())
        if action_type is PrepareRestore:
            return PrepareRestore(kind="prepare_restore", profile_id=profile_id, **RestoreBody.model_validate(payload).model_dump())
        if action_type is ConfirmRestore:
            return ConfirmRestore(kind="confirm_restore", **ConfirmBody.model_validate(payload).model_dump())
        if action_type is GetRetirementStatus:
            if payload:
                raise HTTPException(422, "invalid request")
            operation_id = request.query_params.get("operation_id")
            return GetRetirementStatus(kind="get_retirement_status", operation_id=operation_id)
        if action_type is PrepareRetirement:
            return PrepareRetirement(kind="prepare_retirement", **RetirementBody.model_validate(payload).model_dump())
        if action_type is ConfirmRetirement:
            return ConfirmRetirement(kind="confirm_retirement", **ConfirmBody.model_validate(payload).model_dump())
        if action_type is PrepareWorldClone:
            body = CloneBody.model_validate(payload)
            return PrepareWorldClone(kind="prepare_world_clone", **body.model_dump())
        if action_type is ConfirmWorldClone:
            return ConfirmWorldClone(kind="confirm_world_clone", **ConfirmBody.model_validate(payload).model_dump())
        if action_type is CheckUpdate:
            if payload:
                raise HTTPException(422, "invalid request")
            return CheckUpdate(kind="check_update", profile_id=profile_id)
        if action_type is PrepareUpdate:
            if payload:
                raise HTTPException(422, "invalid request")
            return PrepareUpdate(kind="prepare_update", profile_id=profile_id)
        if action_type is ConfirmUpdate:
            return ConfirmUpdate(kind="confirm_update", **ConfirmBody.model_validate(payload).model_dump())
        if action_type is GetNotificationConfig:
            if payload:
                raise HTTPException(422, "invalid request")
            return GetNotificationConfig(kind="get_notification_config", profile_id=profile_id)
        if action_type is SetNotificationRule:
            return SetNotificationRule(kind="set_notification_rule", profile_id=profile_id, **NotificationRuleBody.model_validate(payload).model_dump())
        if action_type is SetIdleStop:
            return SetIdleStop(kind="set_idle_stop", profile_id=profile_id, **IdleStopBody.model_validate(payload).model_dump())
        if action_type is TestNotification:
            body = NotificationTestBody.model_validate(payload)
            selected_profile = profile_id or body.profile_id
            if selected_profile is None:
                raise HTTPException(422, "invalid request")
            if profile_id is not None and body.profile_id is not None and profile_id != body.profile_id:
                raise HTTPException(422, "invalid request")
            return TestNotification(kind="test_notification", channel=body.channel, profile_id=selected_profile)
    except (ValidationError, TypeError, ValueError) as exc:
        raise HTTPException(422, "invalid request") from exc
    raise HTTPException(404, "not found")


def _status(response: RpcResponse) -> int:
    if isinstance(response, RpcSuccess):
        return 200
    return {
        ErrorCode.SLOT_CONFLICT: 409,
        ErrorCode.CONFIRMATION_EXPIRED: 410,
        ErrorCode.CONFIRMATION_MISMATCH: 409,
        ErrorCode.REQUEST_ID_CONFLICT: 409,
        ErrorCode.UNAUTHORIZED_PEER: 403,
        ErrorCode.INVALID_REQUEST: 400,
    }.get(response.error.code, 503 if response.error.retryable else 400)


class ApiService:
    def __init__(self, rpc: Callable[..., Any]):
        self.rpc = rpc

    async def call(
        self,
        actor: str,
        action: RpcAction,
        *,
        provenance: RpcProvenance = RpcProvenance.SERVICE,
        request_id: UUID | None = None,
    ) -> RpcResponse:
        request = RpcRequest(
            request_id=request_id or uuid4(),
            actor=actor,
            provenance=provenance,
            action=action,
        )
        result = self.rpc(actor, action) if _accepts_two(self.rpc) else self.rpc(request)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, (RpcSuccess, RpcFailure)):
            return result
        # A typed test seam may return the result payload directly.
        return RpcSuccess(request_id=request.request_id, result=result)


def _accepts_two(callback: Callable[..., Any]) -> bool:
    try:
        return len(signature_parameters(callback)) >= 2
    except (TypeError, ValueError):
        return True


def _idempotency_key(request: Request) -> UUID | None:
    """Parse one caller-supplied operation key for a mutation."""

    values = request.headers.getlist("idempotency-key")
    if not values:
        return None
    if len(values) != 1:
        raise HTTPException(422, "invalid idempotency key")
    value = values[0]
    if len(value) > 128 or not value:
        raise HTTPException(422, "invalid idempotency key")
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError) as exc:
        raise HTTPException(422, "invalid idempotency key") from exc
    if str(parsed) != value:
        raise HTTPException(422, "invalid idempotency key")
    return parsed


def add_api_routes(
    router: APIRouter,
    service: ApiService,
    auth_dependency: Callable,
    mutation_dependency: Callable,
    *,
    on_mutation: Callable[[], Any] | None = None,
) -> None:
    async def invoke(request: Request, response: Response, *, profile_id: ProfileId | None, payload: Mapping[str, Any], mutation: bool = False):
        actor = await auth_dependency(request, response, mutation=mutation)
        action = _action(request.url.path, request.method, profile_id, payload, request)
        request_id = _idempotency_key(request) if mutation else None
        try:
            rpc_response = await service.call(
                actor,
                action,
                provenance=RpcProvenance.WEB_HUMAN,
                request_id=request_id,
            )
        except Exception:
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "upstream_unavailable", "message": "control service unavailable", "retryable": True, "details": None}},
            )
        if isinstance(rpc_response, RpcFailure):
            return JSONResponse(
                status_code=_status(rpc_response),
                content={
                    "error": {
                        "code": rpc_response.error.code.value,
                        "message": rpc_response.error.message,
                        "retryable": rpc_response.error.retryable,
                        "details": rpc_response.error.details.model_dump(mode="json") if rpc_response.error.details else None,
                    }
                },
            )
        if mutation and on_mutation is not None:
            on_mutation()
        return rpc_response.result

    @router.get("/status")
    async def status(request: Request, response: Response):
        return await invoke(request, response, profile_id=None, payload={})

    @router.get("/profiles")
    async def profiles(request: Request, response: Response):
        return await invoke(request, response, profile_id=None, payload={})

    @router.get("/profiles/{profile_id}")
    async def profile(profile_id: ProfileId, request: Request, response: Response):
        result = await invoke(request, response, profile_id=profile_id, payload={})
        if isinstance(result, (list, tuple)):
            match = next((item for item in result if getattr(item, "id", None) == profile_id), None)
            if match is None:
                raise HTTPException(404, "profile not found")
            return match
        return result

    @router.get("/profiles/{profile_id}/logs")
    async def logs(profile_id: ProfileId, request: Request, response: Response):
        return await invoke(request, response, profile_id=profile_id, payload={})

    @router.get("/profiles/{profile_id}/backups")
    async def backups(profile_id: ProfileId, request: Request, response: Response):
        return await invoke(request, response, profile_id=profile_id, payload={})

    @router.get("/backups")
    async def aggregate_backups(request: Request, response: Response):
        return await invoke(request, response, profile_id=None, payload={})

    @router.get("/retirement")
    async def retirement_status(request: Request, response: Response):
        return await invoke(request, response, profile_id=None, payload={})

    @router.get("/profiles/{profile_id}/stats/summary")
    async def stats_summary(profile_id: ProfileId, request: Request, response: Response):
        return await invoke(request, response, profile_id=profile_id, payload={})

    @router.get("/profiles/{profile_id}/stats/heatmap")
    async def stats_heatmap(profile_id: ProfileId, request: Request, response: Response):
        return await invoke(request, response, profile_id=profile_id, payload={})

    @router.get("/profiles/{profile_id}/stats/tps")
    async def stats_tps(profile_id: ProfileId, request: Request, response: Response):
        return await invoke(request, response, profile_id=profile_id, payload={})

    @router.get("/events")
    async def events(request: Request, response: Response):
        return await invoke(request, response, profile_id=None, payload={})

    @router.get("/audit")
    async def audit(request: Request, response: Response):
        return await invoke(request, response, profile_id=None, payload={})

    @router.get("/profiles/{profile_id}/update")
    async def update_check(profile_id: ProfileId, request: Request, response: Response):
        return await invoke(request, response, profile_id=profile_id, payload={})

    @router.get("/profiles/{profile_id}/notifications")
    async def notifications(profile_id: ProfileId, request: Request, response: Response):
        return await invoke(request, response, profile_id=profile_id, payload={})

    @router.get("/profiles/{profile_id}/config")
    async def profile_config(profile_id: ProfileId, request: Request, response: Response):
        return await invoke(request, response, profile_id=profile_id, payload={})

    @router.get("/benchmarks/{profile_id}/export")
    async def benchmark_export_first(profile_id: ProfileId, request: Request, response: Response):
        fmt = request.query_params.get("format", "json")
        if fmt not in {"json", "csv"}:
            raise HTTPException(422, "format must be json or csv")
        try:
            limit = int(request.query_params.get("limit", "100"))
        except ValueError as exc:
            raise HTTPException(422, "invalid pagination") from exc
        if not 1 <= limit <= 100:
            raise HTTPException(422, "invalid pagination")
        result = await invoke(request, response, profile_id=profile_id, payload={"limit": limit, "format": fmt})
        if isinstance(result, FastAPIResponse):
            return result
        model = result.model_dump(mode="json") if hasattr(result, "model_dump") else result
        if fmt == "json":
            return FastAPIResponse(content=model.get("content", "{}"), media_type="application/json")
        output = io.StringIO(); writer = csv.writer(output, lineterminator="\n")
        return FastAPIResponse(content=model.get("content", ""), media_type="text/csv", headers={"Content-Disposition": 'attachment; filename="horizon-benchmarks.csv"', "X-Content-Type-Options": "nosniff"})

    @router.get("/profiles/{profile_id}/benchmarks")
    async def benchmarks(profile_id: ProfileId, request: Request, response: Response):
        query = request.query_params
        payload = {
            "cursor": query.get("cursor"),
            "limit": query.get("limit", "20"),
            "format": query.get("format", "json"),
        }
        return await invoke(request, response, profile_id=profile_id, payload=payload)

    @router.get("/schedules")
    async def schedules(request: Request, response: Response):
        return await invoke(request, response, profile_id=None, payload={})

    async def mutation(request: Request, response: Response, profile_id: ProfileId | None = None, job_id: str | None = None):
        payload = await _read_mutation_body(request)
        if job_id is not None:
            payload = {**payload, "job_id": job_id}
        return await invoke(request, response, profile_id=profile_id, payload=payload, mutation=True)

    for route, action_type in ROUTE_ACTIONS.items():
        method, path = route.split(" ", 1)
        if method == "GET" or path in {"/api/v1/status", "/api/v1/profiles", "/api/v1/events", "/api/v1/audit"}:
            continue
        route_path = path.removeprefix("/api/v1")
        router.add_api_route(route_path, mutation, methods=[method])


__all__ = [
    "ApiService",
    "ROUTE_ACTIONS",
    "StrictBody",
    "add_api_routes",
]
