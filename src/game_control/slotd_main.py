"""Privileged newline-delimited Unix-socket controller entrypoint."""

from __future__ import annotations

import asyncio
import grp
import errno
import inspect
import json
import logging
import os
import pwd
import socket
import stat
import struct
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from .controller import Controller
from .adapters.crafty import CraftyAdapter
from .adapters.systemd import SystemdAdapter
from .profile import ProfileRegistry
from .slot import ReservationStore, SlotInspector
from .state_db import StateDatabase, STATE_DB_PATH
from .models import AdapterKind, HealthState
from .service_wiring import build_service_seams
from .service_wiring import _ContainerSlot
from .service_container import ServiceContainer
from .runtime.telemetry import ResourceRef
from .runtime.telemetry import TelemetryRuntime
from .rcon import RCON_HOST, RCON_PASSWORD_PATH, RCON_PORT, RconClient, SunlitRconTransport
from .rcon_telemetry import PersistentRconTelemetry
from .schedule import parse_schedule
from .protocol import (
    MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES,
    ErrorCode,
    RpcFailure as _RpcFailure,
    RpcResponse,
    RpcSuccess,
    SafeDetails,
    StatusSnapshot,
    Watch,
    failure,
    parse_request_line,
    response_json,
)
from .push import WatchCursor, WatchHub

CONTROL_SOCKET = Path("/run/game-control/control.sock")
ROOT_CONFIG = Path("/etc/game-control/game-control.toml")
PROFILES_DIR = Path("/etc/game-control/profiles.d")
CRAFTY_TOKEN_PATH = Path("/etc/game-control/secrets.d/crafty-token")
READ_TIMEOUT_SECONDS = 10.0
WRITE_TIMEOUT_SECONDS = 10.0
_LOG = logging.getLogger(__name__)
RpcFailure = _RpcFailure


class UnixRpcServer:
    def __init__(
        self,
        controller: Controller,
        *,
        socket_path: Path = CONTROL_SOCKET,
        gamecontrol_user: str = "gamecontrol",
        gamecontrol_group: str = "gamecontrol",
        uid: int | None = None,
        gid: int | None = None,
        primary_gid: int | None = None,
    ):
        self.controller = controller
        self.socket_path = Path(socket_path)
        self.gamecontrol_user = gamecontrol_user
        self.gamecontrol_group = gamecontrol_group
        account = None
        if uid is not None and gid is not None and primary_gid is None:
            primary_gid = gid
        if uid is None or primary_gid is None:
            try:
                account = pwd.getpwnam(gamecontrol_user)
            except KeyError as exc:
                if uid is None or primary_gid is None:
                    raise RuntimeError("configured gamecontrol user is unavailable") from exc
        self.uid = account.pw_uid if uid is None else uid
        self.peer_gid = (
            account.pw_gid if primary_gid is None and account is not None
            else (gid if primary_gid is None else primary_gid)
        )
        if gid is None:
            try:
                self.gid = grp.getgrnam(gamecontrol_group).gr_gid
            except KeyError as exc:
                raise RuntimeError("configured socket group is unavailable") from exc
        else:
            self.gid = gid
        self._server: asyncio.AbstractServer | None = None
        self._bound_inode: int | None = None
        self.watch_hub = WatchHub()

    def authorize_peer(self, uid: int, gid: int) -> bool:
        return uid == self.uid and gid == self.peer_gid

    @staticmethod
    def peer_credentials(sock: socket.socket) -> tuple[int, int, int]:
        raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        return struct.unpack("3i", raw)

    def _check_existing_socket(self) -> None:
        if self.socket_path.is_symlink():
            raise PermissionError("control socket symlink is not allowed")
        if self.socket_path.exists() and not stat.S_ISSOCK(self.socket_path.stat().st_mode):
            raise PermissionError("control socket path is not a socket")
        parent = self.socket_path.parent
        if parent.is_symlink():
            raise PermissionError("control socket directory symlink is not allowed")
        if parent.exists() and not parent.is_dir():
            raise PermissionError("control socket parent is not a directory")
        parent.mkdir(parents=True, exist_ok=True)

    async def start(self) -> None:
        self._check_existing_socket()
        if self.socket_path.exists():
            before = self.socket_path.stat()
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                probe.settimeout(0.2)
                probe.connect(str(self.socket_path))
            except OSError as exc:
                if exc.errno not in (errno.ECONNREFUSED, errno.ENOENT):
                    raise RuntimeError("control socket is live") from exc
            else:
                raise RuntimeError("control socket is live")
            finally:
                probe.close()
            # Recheck the inode after probing so a concurrent replacement is
            # never unlinked.
            try:
                if self.socket_path.stat().st_ino != before.st_ino:
                    raise RuntimeError("control socket changed while probing")
                self.socket_path.unlink()
            except FileNotFoundError:
                pass
        self._server = await asyncio.start_unix_server(
            self.handle_client, path=str(self.socket_path), limit=MAX_REQUEST_BYTES + 1
        )
        try:
            os.chown(self.socket_path, 0, self.gid)
            os.chmod(self.socket_path, 0o660)
        except OSError as exc:
            await self.close()
            raise PermissionError("unable to secure control socket") from exc
        info = self.socket_path.stat()
        if (
            not stat.S_ISSOCK(info.st_mode)
            or info.st_uid != 0
            or info.st_gid != self.gid
            or stat.S_IMODE(info.st_mode) != 0o660
        ):
            await self.close()
            raise PermissionError("insecure control socket ownership")
        self._bound_inode = info.st_ino

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        try:
            if (
                self.socket_path.is_socket()
                and self._bound_inode is not None
                and self.socket_path.stat().st_ino == self._bound_inode
            ):
                self.socket_path.unlink()
        except FileNotFoundError:
            pass

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        sock = writer.get_extra_info("socket")
        request_id = UUID(int=0)
        request_kind = "unknown"
        actor = "unknown"
        try:
            if sock is None:
                raise PermissionError("peer socket unavailable")
            _pid, uid, gid = self.peer_credentials(sock)
            if not self.authorize_peer(uid, gid):
                response = failure(request_id, ErrorCode.UNAUTHORIZED_PEER, "unauthorized peer")
                await self._write(
                    writer,
                    response_json(response),
                    request_kind="unauthorized_peer",
                    actor=actor,
                )
                return
            try:
                data = await asyncio.wait_for(
                    reader.readuntil(b"\n"), timeout=READ_TIMEOUT_SECONDS
                )
            except (asyncio.LimitOverrunError, asyncio.IncompleteReadError, asyncio.TimeoutError):
                response = failure(request_id, ErrorCode.INVALID_REQUEST, "invalid request")
                await self._write(
                    writer,
                    response_json(response),
                    request_kind="invalid_request",
                    actor=actor,
                )
                return
            if len(data) > MAX_REQUEST_BYTES:
                response = failure(request_id, ErrorCode.INVALID_REQUEST, "request too large")
                await self._write(
                    writer,
                    response_json(response),
                    request_kind="invalid_request",
                    actor=actor,
                )
                return
            try:
                request = parse_request_line(data)
                request_id = request.request_id
                request_kind = request.action.kind
                actor = request.actor
            except ValueError:
                response = failure(request_id, ErrorCode.INVALID_REQUEST, "invalid request")
            else:
                if isinstance(request.action, Watch):
                    await self._watch_client(request, writer)
                    return
                response = await self.controller.execute(request)
                await self._publish_response(request.action.kind, response)
            await self._write(
                writer,
                response_json(response),
                request_kind=request_kind,
                actor=actor,
                request_id=request_id,
            )
        except Exception:
            incident = os.urandom(8).hex()
            _LOG.exception("socket incident %s", incident)
            response = failure(
                request_id, ErrorCode.INTERNAL_ERROR, "internal controller error",
                details=SafeDetails(incident_id=incident),
            )
            try:
                await self._write(
                    writer,
                    response_json(response),
                    request_kind=request_kind,
                    actor=actor,
                    request_id=request_id,
                )
            except Exception:
                pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _publish_response(self, kind: str, response: RpcResponse) -> None:
        """Project safe controller results to authenticated watch clients."""
        if not isinstance(response, RpcSuccess):
            return
        result = response.result
        if hasattr(result, "model_dump"):
            payload = result.model_dump(mode="json")
        elif isinstance(result, dict):
            payload = result
        else:
            return
        # Read-only responses such as GetPerf do not carry a state generation.
        # Project those observations at the hub's current generation so an
        # unrelated response cannot regress the watch stream and fail the RPC.
        # A response that explicitly carries a generation remains subject to
        # the hub's monotonicity contract.
        generation = (
            int(payload["generation"])
            if isinstance(payload, dict) and "generation" in payload
            else self.watch_hub.generation
        )
        await self.watch_hub.publish(
            kind, payload, generation=generation, full=isinstance(result, StatusSnapshot)
        )

    async def _watch_client(self, request, writer: asyncio.StreamWriter) -> None:
        client = await self.watch_hub.subscribe(
            WatchCursor(sequence=request.action.cursor, generation=request.action.generation)
        )
        try:
            while not client.disconnected and not writer.is_closing():
                event = await self.watch_hub.heartbeat(client, timeout=15.0)
                frame = {"sequence": event.sequence, "generation": event.generation,
                         "kind": event.kind, "full": event.full, "payload": dict(event.payload)}
                writer.write((json.dumps(frame, separators=(",", ":")) + "\n").encode())
                await asyncio.wait_for(writer.drain(), WRITE_TIMEOUT_SECONDS)
        finally:
            await self.watch_hub.unsubscribe(client)

    @staticmethod
    async def _write(
        writer: asyncio.StreamWriter,
        data: bytes,
        *,
        request_kind: str,
        actor: str,
        request_id: UUID | None = None,
    ) -> None:
        try:
            if len(data) > MAX_RESPONSE_BYTES:
                data = response_json(
                    failure(request_id or UUID(int=0), ErrorCode.INTERNAL_ERROR, "response exceeds framing budget")
                )
            writer.write(data)
            await asyncio.wait_for(writer.drain(), timeout=WRITE_TIMEOUT_SECONDS)
        except (BrokenPipeError, ConnectionResetError):
            _LOG.warning("RPC peer disconnected while writing %s for actor %s", request_kind, actor)


RpcServer = UnixRpcServer


def _build_controller_unmanaged(config_path: str | os.PathLike[str] = ROOT_CONFIG, *, provisional: _ProvisionalOwner | None = None, boundary_hook: Any | None = None) -> Controller:
    """Construct the production root controller from a closed root config."""
    def boundary(label: str) -> None:
        if boundary_hook is not None:
            boundary_hook(label)

    config_file = Path(config_path)
    if config_file.is_symlink() or not config_file.is_file():
        raise RuntimeError("root controller configuration is unavailable")
    try:
        with config_file.open("rb") as stream:
            config = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise RuntimeError("invalid root controller configuration") from exc
    if not isinstance(config, dict):
        raise RuntimeError("invalid root controller configuration")
    boot_profile = config.get("boot_profile")
    if boot_profile is not None and not isinstance(boot_profile, str):
        raise RuntimeError("invalid boot profile")
    boot_autostart = config.get("boot_autostart", False)
    if not isinstance(boot_autostart, bool):
        raise RuntimeError("invalid boot autostart flag")
    try:
        schedules = parse_schedule(config.get("schedule"))
    except ValueError as exc:
        raise RuntimeError("invalid schedule configuration") from exc
    profiles_dir = Path(config.get("profiles_dir", config.get("profile_dir", PROFILES_DIR)))
    state_path = Path(config.get("state_db", STATE_DB_PATH))
    reservation_path = Path(config.get("reservation_path", "/run/game-control/reservation.json"))
    registry = ProfileRegistry.load(profiles_dir)
    state_db = StateDatabase.open(state_path)
    if provisional is not None:
        provisional.register(state_db, state_db.close)
    boundary("state_db")
    crafty_cfg = config.get("crafty", {})
    if not crafty_cfg and ("crafty_base_url" in config or "crafty_token_path" in config):
        crafty_cfg = {
            "base_url": config.get("crafty_base_url"),
            "token_path": config.get("crafty_token_path", CRAFTY_TOKEN_PATH),
            "ca_path": config.get("crafty_ca_path"),
        }
    if not isinstance(crafty_cfg, dict):
        raise RuntimeError("invalid Crafty configuration")
    rcon_config = config.get("rcon", {})
    if not isinstance(rcon_config, dict):
        raise RuntimeError("invalid RCON configuration")
    rcon_host = rcon_config.get("host", RCON_HOST)
    rcon_port = rcon_config.get("port", RCON_PORT)
    rcon_password_path = Path(rcon_config.get("password_path", RCON_PASSWORD_PATH))
    if not isinstance(rcon_host, str) or isinstance(rcon_port, bool) or not isinstance(rcon_port, int):
        raise RuntimeError("invalid RCON configuration")
    rcon_kwargs = {"host": rcon_host, "port": rcon_port, "password_path": rcon_password_path}
    if any(
        (
            rcon_kwargs["host"] != RCON_HOST,
            rcon_kwargs["port"] != RCON_PORT,
            rcon_kwargs["password_path"] != RCON_PASSWORD_PATH,
        )
    ):
        raise RuntimeError("RCON endpoint is not approved")
    adapters = {}
    crafty = None
    secret_values: list[str] = []
    sunlit_rcon = None
    rcon_telemetry = None
    for profile in registry:
        if profile.adapter is AdapterKind.CRAFTY:
            if crafty is None:
                token_path = Path(crafty_cfg.get("token_path", CRAFTY_TOKEN_PATH))
                token = token_path.read_text(encoding="utf-8").strip()
                secret_values.append(token)
                base_url = crafty_cfg.get("base_url")
                if not isinstance(base_url, str) or not base_url.startswith(("https://", "http://")):
                    raise RuntimeError("invalid Crafty endpoint")
                crafty = CraftyAdapter(
                    base_url,
                    token,
                    verify=crafty_cfg.get("verify", crafty_cfg.get("ca_path")),
                )
                if provisional is not None:
                    provisional.register(crafty, crafty.aclose)
                boundary("crafty")
            adapters[profile.id] = crafty
        else:
            if profile.id.value == "minecraft-sunlit-cobblemon":
                sunlit_rcon = RconClient(**rcon_kwargs)
                if provisional is not None:
                    provisional.register(sunlit_rcon, lambda: None)
                boundary("one_shot_rcon")
                rcon_telemetry = PersistentRconTelemetry(profile.id.value, **rcon_kwargs)
                if provisional is not None:
                    provisional.register(rcon_telemetry, rcon_telemetry.close)
                boundary("persistent_rcon")
                adapters[profile.id] = SystemdAdapter(rcon=sunlit_rcon)
            else:
                adapters[profile.id] = SystemdAdapter()
    inspector = SlotInspector()
    reservation_store = ReservationStore(reservation_path=reservation_path)

    async def await_free_slot(timeout_seconds: float = 30.0):
        return await _await_free_slot(inspector, timeout_seconds)

    async def await_ready(profile):
        adapter = adapters[profile.id]
        if profile.adapter is AdapterKind.SYSTEMD:
            return await _await_systemd_profile_ready(
                services.status,
                inspector,
                profile,
                profile.health_timeout_seconds,
            )
        deadline = asyncio.get_running_loop().time() + profile.health_timeout_seconds
        while True:
            try:
                observation = await adapter.observe(profile)
                if (
                    observation.running
                    and observation.healthy is True
                    and observation.required_ports_ready is not False
                ):
                    return True
            except Exception:
                pass
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.25, remaining))

    stats_config = config.get("stats", {})
    if not isinstance(stats_config, dict):
        raise RuntimeError("invalid stats configuration")
    benchmark_config = config.get("benchmark")
    if benchmark_config is not None and not isinstance(benchmark_config, list):
        raise RuntimeError("invalid benchmark configuration")
    services = build_service_seams(
        registry,
        adapters,
        state_db,
        inspector,
        secret_values=tuple(secret_values),
        stats_config=stats_config,
        sunlit_online_backup=SunlitRconTransport(sunlit_rcon) if sunlit_rcon is not None else None,
        benchmark_config=benchmark_config,
        rcon_telemetry=rcon_telemetry,
        reservation_store=reservation_store,
        crafty_adapters=(crafty,) if crafty is not None else (),
        own_telemetry_resources=True,
        register_owned=provisional.register if provisional is not None else None,
        _boundary_hook=boundary_hook,
    )
    boundary("service_seams_return")
    if services.session_store is not None:
        services.session_store.recover(now=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    boundary("session_recovery")
    controller = Controller(
        profiles=registry,
        state_db=state_db,
        reservation_store=reservation_store,
        adapters=adapters,
        services=services,
        slot_inspector=inspector,
        await_free_slot=await_free_slot,
        await_ready=await_ready,
        boot_profile=boot_profile,
        boot_autostart=boot_autostart,
        schedules=schedules,
        schedule_config_path=config_file,
    )
    if provisional is not None:
        provisional.register(controller, controller.aclose)
    boundary("controller")
    return controller


class _ProvisionalOwner:
    """Transactional ledger for root-created resources before publication."""
    def __init__(self) -> None:
        self._resources: list[tuple[Any, Any]] = []
        self._seen: set[int] = set()
        self._transferred = False

    def register(self, value: Any, close: Any) -> Any:
        if value is not None and id(value) not in self._seen:
            self._seen.add(id(value))
            self._resources.append((value, close))
            if isinstance(value, TelemetryRuntime):
                for reference in (value._sampler_ref, value._rcon_ref, value._database_ref):
                    if reference is not None and reference.owns_value:
                        self.discard(reference.value)
        return value

    def transfer(self) -> None:
        self._transferred = True
        self._resources.clear()

    def discard(self, value: Any) -> None:
        identity = id(value)
        self._seen.discard(identity)
        self._resources = [(item, close) for item, close in self._resources if item is not value]

    async def aclose(self) -> None:
        if self._transferred:
            return
        # Tear down in dependency order: typed owners first, root state last.
        resources = tuple(reversed(self._resources))
        self._resources.clear()
        first: BaseException | None = None
        for _resource, close in resources:
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except BaseException as exc:
                if first is None:
                    first = exc
        if first is not None:
            raise first


async def _drain_provisional(owner: _ProvisionalOwner) -> BaseException | None:
    """Drain root construction cleanup despite caller cancellation."""
    cleanup = asyncio.ensure_future(owner.aclose())
    interrupted = False
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            interrupted = True
            continue
        except BaseException:
            if cleanup.done():
                break
            raise
    try:
        cleanup.result()
    except BaseException as exc:
        if interrupted and isinstance(exc, asyncio.CancelledError):
            return asyncio.CancelledError()
        return exc
    return asyncio.CancelledError() if interrupted else None


class _RootAssembly:
    def __init__(self, controller: Controller, services: Any, container: ServiceContainer):
        self.controller, self.services, self.container = controller, services, container

    async def aclose(self) -> None:
        await self.container.aclose()


async def build_controller_assembly(config_path: str | os.PathLike[str] = ROOT_CONFIG, *, _boundary_hook: Any | None = None) -> _RootAssembly:
    """Build the complete root graph and publish it only after finalization."""
    provisional = _ProvisionalOwner()
    try:
        build_kwargs: dict[str, Any] = {"provisional": provisional}
        # The private boundary hook is test-only fault injection; production
        # keeps the same typed acquisition sequence without a callback.
        if _boundary_hook is not None:
            build_kwargs["boundary_hook"] = _boundary_hook
        controller = _build_controller_unmanaged(config_path, **build_kwargs)
        services = controller.services
        updates = tuple(services.updates.services.values())
        history = services.audit.history
        notification_service = services.notifications.service
        container = ServiceContainer(
            controller=controller,
            state_database=ResourceRef.owned(controller.state_db),
            telemetry_runtime=ResourceRef.owned(services.telemetry_runtime),
            alert_runtime=ResourceRef.owned(services.alerts),
            history_queries=ResourceRef.owned(history),
            crafty_adapters=tuple(ResourceRef.owned(adapter) for adapter in services._crafty_adapters),
            update_services=tuple(ResourceRef.owned(update) for update in updates),
            notification_service=ResourceRef.owned(notification_service),
            legacy_tps_sampler=ResourceRef.owned(services.tps_sampler) if services.tps_sampler is not None else None,
        )
        if _boundary_hook is not None:
            _boundary_hook("container")
        if not isinstance(services._container_slot, _ContainerSlot):
            raise RuntimeError("service container slot is unavailable")
        services._finalize_container(container)
        if _boundary_hook is not None:
            _boundary_hook("finalizer")
        provisional.transfer()
        return _RootAssembly(controller, services, container)
    except BaseException as original:
        cleanup_error = await _drain_provisional(provisional)
        # Construction's primary failure, especially cancellation, is never
        # replaced by a later owner cleanup failure.
        if cleanup_error is not None and not isinstance(original, asyncio.CancelledError):
            raise original from cleanup_error
        raise


def build_controller(config_path: str | os.PathLike[str] = ROOT_CONFIG) -> Controller:
    """Synchronous compatibility wrapper; reject nested event-loop use."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(build_controller_assembly(config_path)).controller
    raise RuntimeError("build_controller() cannot run inside an event loop; await build_controller_assembly()")


async def _await_free_slot(
    inspector: SlotInspector,
    timeout_seconds: float,
    *,
    poll_interval: float = 0.5,
) -> bool:
    """Poll until the kernel slot is free and metadata is consistent."""
    deadline = asyncio.get_running_loop().time() + max(0.0, timeout_seconds)
    while True:
        observation = inspector.observe()
        if observation.owner is None and not observation.inconsistent:
            return True
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(poll_interval, remaining))


async def _await_systemd_profile_ready(
    status_service,
    inspector: SlotInspector,
    profile,
    timeout_seconds: float,
    *,
    poll_interval: float = 0.25,
) -> bool:
    """Require slot, process identity, health, and listeners before lease release."""

    deadline = asyncio.get_running_loop().time() + max(0.0, timeout_seconds)
    expected_profile = getattr(profile.id, "value", profile.id)
    try:
        parameters = inspect.signature(status_service.snapshot).parameters.values()
        supports_force = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            or parameter.name == "force"
            for parameter in parameters
        )
    except (TypeError, ValueError):
        supports_force = False
    while True:
        try:
            snapshot = status_service.snapshot(**({"force": True} if supports_force else {}))
            if inspect.isawaitable(snapshot):
                snapshot = await snapshot
            status = next(
                item
                for item in snapshot.profiles
                if getattr(getattr(item, "profile_id", None), "value", getattr(item, "profile_id", None))
                == expected_profile
            )
            slot = inspector.observe()
            if inspect.isawaitable(slot):
                slot = await slot
            owner = getattr(getattr(slot, "owner", None), "value", getattr(slot, "owner", None))
            health = getattr(getattr(status, "health", None), "value", getattr(status, "health", None))
            pid = getattr(status, "pid", None)
            if (
                owner == expected_profile
                and not bool(getattr(slot, "inconsistent", False))
                and isinstance(pid, int)
                and not isinstance(pid, bool)
                and pid > 0
                and getattr(slot, "pid", None) == pid
                and health == HealthState.HEALTHY.value
                and getattr(status, "required_ports_ready", False) is True
            ):
                return True
        except Exception:
            pass
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(poll_interval, remaining))


async def serve() -> None:
    assembly = await build_controller_assembly()
    controller = assembly.controller
    server: UnixRpcServer | None = None
    owned_tasks: list[asyncio.Task[Any]] = []
    task_roles: dict[asyncio.Task[Any], str] = {}
    observed_tasks: set[asyncio.Task[Any]] = set()

    def create_owned_task(coroutine: Any, *, name: str) -> asyncio.Task[Any]:
        """Create a supervised task without leaking its coroutine on failure."""
        try:
            return asyncio.create_task(coroutine, name=name)
        except BaseException:
            close = getattr(coroutine, "close", None)
            if callable(close):
                close()
            raise

    def observe_task(task: asyncio.Task[Any]) -> BaseException | None:
        if task in observed_tasks:
            return None
        observed_tasks.add(task)
        if task.cancelled():
            return asyncio.CancelledError()
        return task.exception()

    async def minecraft_running() -> bool:
        minecraft_id = (
            "minecraft-sunlit-cobblemon"
            if any(
                getattr(getattr(item, "id", None), "value", getattr(item, "id", None))
                == "minecraft-sunlit-cobblemon"
                for item in controller.profiles
            )
            else "minecraft"
        )
        inspector = controller.slot_inspector
        if inspector is not None:
            slot = inspector.observe()
            owner = getattr(slot, "owner", None)
            if getattr(owner, "value", owner) != minecraft_id:
                return False
        profile = next(
            (
                item
                for item in controller.profiles
                if getattr(getattr(item, "id", None), "value", getattr(item, "id", None)) == minecraft_id
            ),
            None,
        )
        if profile is None:
            return False
        adapter = controller.adapters.get(getattr(profile, "id", None))
        if adapter is None:
            return False
        observed = adapter.observe(profile)
        if inspect.isawaitable(observed):
            observed = await observed
        return bool(getattr(observed, "running", False))

    try:
        server = UnixRpcServer(controller)
        backups = getattr(controller.services, "backups", None)
        if backups is not None and hasattr(backups, "reconcile_startup"):
            backups.reconcile_startup()
        await controller.reconcile_startup()
        await server.start()
        initialization = create_owned_task(asyncio.sleep(0), name="horizon-initialization")
        owned_tasks.append(initialization)
        task_roles[initialization] = "initialization"
        maintenance_task = create_owned_task(_maintenance_loop(controller, initialization=initialization), name="horizon-maintenance-supervisor")
        owned_tasks.append(maintenance_task)
        task_roles[maintenance_task] = "maintenance"
        loop_lag_task = create_owned_task(_event_loop_lag_loop(controller), name="horizon-event-loop-lag-supervisor")
        owned_tasks.append(loop_lag_task)
        task_roles[loop_lag_task] = "event_loop_lag"
        telemetry_sampler = getattr(getattr(controller, "services", None), "telemetry_sampler", None)
        if telemetry_sampler is not None:
            telemetry_task = create_owned_task(_run_telemetry_sampler(telemetry_sampler, initialization), name="horizon-telemetry-supervisor")
            owned_tasks.append(telemetry_task)
            task_roles[telemetry_task] = "telemetry"
        tps_sampler = getattr(controller.services, "tps_sampler", None)
        if tps_sampler is not None:
            tps_task = create_owned_task(tps_sampler.run(minecraft_running), name="horizon-legacy-tps-supervisor")
            owned_tasks.append(tps_task)
            task_roles[tps_task] = "legacy_tps"
        server_task = create_owned_task(server._server.serve_forever(), name="horizon-rpc-supervisor")  # type: ignore[union-attr]
        owned_tasks.append(server_task)
        task_roles[server_task] = "rpc"
        supervised = set(owned_tasks)
        while supervised:
            done, _pending = await asyncio.wait(supervised, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                supervised.discard(task)
                error = observe_task(task)
                if isinstance(error, asyncio.CancelledError):
                    raise error
                role = task_roles[task]
                if role == "initialization":
                    if error is not None:
                        raise error
                    continue
                if role == "maintenance":
                    if error is not None:
                        raise error
                    raise RuntimeError("slotd maintenance task exited unexpectedly")
                if role == "telemetry":
                    if error is not None:
                        raise error
                    raise RuntimeError("slotd telemetry sampler exited unexpectedly")
                if role == "legacy_tps":
                    if error is not None:
                        raise error
                    raise RuntimeError("slotd tick telemetry task exited unexpectedly")
                if role == "rpc":
                    if error is not None:
                        raise error
                    raise RuntimeError("slotd RPC server task exited unexpectedly")
                if role == "event_loop_lag":
                    if error is not None:
                        raise error
                    raise RuntimeError("slotd event-loop-lag task exited unexpectedly")
                if error is not None:
                    raise error
            if not supervised:
                return
    finally:
        active_failure = sys.exc_info()[0] is not None
        cleanup_error: BaseException | None = None
        caller_cancelled = False

        async def drain(task: asyncio.Future[Any]) -> None:
            nonlocal caller_cancelled
            while not task.done():
                try:
                    await asyncio.shield(task)
                    break
                except asyncio.CancelledError:
                    # A child that cancelled itself propagates CancelledError
                    # through shield too; only an interruption of this
                    # cleanup task is the caller's cancellation.
                    if task.done():
                        continue
                    caller_cancelled = True
                    continue
        for task in owned_tasks:
            if not task.done():
                task.cancel()
        pending_tasks = tuple(task for task in owned_tasks if not task.done())
        if pending_tasks:
            drain_task = asyncio.ensure_future(asyncio.gather(*pending_tasks, return_exceptions=True))
            await drain(drain_task)
        for task in owned_tasks:
            if task.done():
                observe_task(task)
        if server is not None:
            close_task = asyncio.ensure_future(server.close())
            await drain(close_task)
            try:
                close_task.result()
            except BaseException as exc:
                cleanup_error = exc
        close_assembly = asyncio.ensure_future(assembly.aclose())
        await drain(close_assembly)
        try:
            close_assembly.result()
        except BaseException as exc:
            if cleanup_error is None:
                cleanup_error = exc
        if caller_cancelled:
            raise asyncio.CancelledError
        if cleanup_error is not None and not active_failure:
            raise cleanup_error


async def _event_loop_lag_loop(controller: Any, interval: float = 0.25) -> None:
    """Low-overhead monotonic scheduler lag ring for the read-only perf RPC."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + interval
    while True:
        await asyncio.sleep(max(0.0, deadline - loop.time()))
        now = loop.time()
        controller.performance.record_event_loop_lag(max(0.0, (now - deadline) * 1000.0))
        # Do not catch up missed periods: one long stall is one observation,
        # not a burst of synthetic samples at decreasing lag values.
        deadline = now + interval


async def _run_telemetry_sampler(sampler: Any, initialization: asyncio.Task[Any]) -> None:
    """Start the fixed-cadence sampler only after startup reconciliation."""
    await initialization
    task = sampler.start()
    result = await task
    return result


async def _maintenance_loop(
    controller: Any,
    *,
    interval_seconds: float = 30.0,
    initialization: asyncio.Task[Any] | None = None,
) -> None:
    """Keep controller maintenance alive without a web/status caller."""
    if initialization is not None:
        await initialization
    delay = min(max(0.1, float(interval_seconds)), 300.0)
    while True:
        started = asyncio.get_running_loop().time()
        try:
            await controller.maintenance_tick()
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger(__name__).exception("slotd maintenance tick failed")
        finally:
            try:
                performance = getattr(controller, "performance", None)
                recorder = getattr(performance, "record_maintenance", None)
                if callable(recorder):
                    recorder((asyncio.get_running_loop().time() - started) * 1000.0)
            except BaseException:
                logging.getLogger(__name__).debug("maintenance timing record dropped", exc_info=True)
        await asyncio.sleep(delay)


def main() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    main()


__all__ = ["UnixRpcServer", "RpcServer", "CONTROL_SOCKET", "serve", "main"]
