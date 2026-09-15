from __future__ import annotations

import asyncio
import hashlib
import importlib.machinery
import importlib.util
import io
import json
import os
import secrets
import sqlite3
import sys
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from game_control import sunlit_update as updater
from game_control.backups import BackupRpcFacade
from game_control.controller import Controller, _ControllerFailure, _MemoryLock
from game_control.errors import SafeError
from game_control.models import (
    AdapterKind,
    BackupDestination,
    OperationName,
    PathSpec,
    PortSpec,
    ProcessSpec,
    Profile,
    ProfileId,
    UpdateSpec,
)
from game_control.protocol import (
    CreateBackup,
    ErrorCode,
    JobAccepted,
    RpcRequest,
    parse_request_line,
    response_json,
)
from game_control.slot import ReservationStore


PROFILE = ProfileId.MINECRAFT_SUNLIT_COBBLEMON
RPC_HELPER = Path(__file__).parents[1] / "ops/bin/horizon-sunlit-update-rpc"


def _profile(tmp_path: Path) -> Profile:
    root = tmp_path / "sunlit"
    return Profile(
        id=PROFILE,
        display_name="Sunlit Cobblemon",
        adapter=AdapterKind.SYSTEMD,
        systemd_unit="minecraft-sunlit-cobblemon.service",
        process=ProcessSpec(executable=Path("/usr/bin/java")),
        ports=(PortSpec(protocol="tcp", port=25566),),
        start_timeout_seconds=5,
        stop_timeout_seconds=5,
        health_timeout_seconds=5,
        paths=PathSpec(
            data_roots=(root,),
            mutable_root=root,
            backup_root=tmp_path / "backups",
            install_root=root / "releases",
            version_file=root / "release.json",
        ),
        min_available_memory_bytes=1,
        min_free_disk_bytes=1,
        operations=frozenset({OperationName.BACKUP}),
        update=UpdateSpec(kind="manual"),
    )


def _store(tmp_path: Path) -> ReservationStore:
    operation = tmp_path / "operation.lock"
    operation.touch(mode=0o600)
    return ReservationStore(operation, tmp_path / "reservation.json")


def _reserve(store: ReservationStore, token: str, *, profile: ProfileId = PROFILE) -> tuple[str, int]:
    operation_id = uuid4().hex
    generation = 7
    store.reserve_if_available(
        profile,
        operation_id,
        ttl=30,
        state_generation=generation,
        controller_pid=os.getpid(),
        controller_start_ticks=store.pid_start_ticks(os.getpid()),
        operation_kind="update",
        capability_sha256=hashlib.sha256(token.encode("ascii")).hexdigest(),
    )
    return operation_id, generation


def _lease(token: str, operation_id: str = "op-1") -> SimpleNamespace:
    return SimpleNamespace(capability_token=token, operation_id=operation_id)


class _Backups:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = 0
        self.lease_checks: list[bool] = []

    def create(self, action, actor=None, request_id=None, lease_check=None, job_id=None):
        self.calls += 1
        self.lease_checks.append(bool(lease_check()) if lease_check is not None else False)
        if self.fail:
            raise SafeError("backup_failed", "synthetic backup failure")
        return JobAccepted(job_id="backup-record-id", state="running")


def _controller(tmp_path: Path, store: ReservationStore, backups: _Backups) -> Controller:
    profile = _profile(tmp_path)
    return Controller(
        profiles={profile.id: profile},
        reservation_store=store,
        services=SimpleNamespace(backups=backups),
        operation_lock_factory=_MemoryLock,
    )


def _action(token: str) -> CreateBackup:
    return CreateBackup(
        kind="create_backup",
        profile_id=PROFILE,
        protected=True,
        destination=BackupDestination.HORIZON_B2,
        reservation_capability=token,
    )


@pytest.mark.asyncio
async def test_capability_handoff_uses_existing_update_reservation_and_replays(tmp_path: Path):
    token = secrets.token_hex(32)
    store = _store(tmp_path)
    operation_id, generation = _reserve(store, token)
    backups = _Backups()
    controller = _controller(tmp_path, store, backups)
    request = RpcRequest(request_id=uuid4(), actor="sunlit-auto-update", action=_action(token))

    first = await controller.execute(request)
    second = await controller.execute(request)

    assert first.ok and second.ok
    assert first.result.job_id == "backup-record-id"
    assert second.result.job_id == "backup-record-id"
    assert backups.calls == 1
    assert backups.lease_checks == [True]
    assert controller._db().execute(
        "SELECT state FROM jobs WHERE operation='backup'"
    ).fetchone() == ("succeeded",)
    assert store.owns_live(
        PROFILE,
        operation_id,
        generation,
        operation_kind="update",
        controller_pid=os.getpid(),
        controller_start_ticks=store.pid_start_ticks(os.getpid()),
    )


@pytest.mark.asyncio
async def test_capability_handoff_rejects_wrong_token_and_foreign_lease(tmp_path: Path):
    token = secrets.token_hex(32)
    store = _store(tmp_path)
    _reserve(store, token)
    backups = _Backups()
    controller = _controller(tmp_path, store, backups)

    wrong = await controller.execute(RpcRequest(
        request_id=uuid4(),
        actor="sunlit-auto-update",
        action=_action(secrets.token_hex(32)),
    ))
    assert not wrong.ok and wrong.error.code is ErrorCode.SLOT_CONFLICT
    assert backups.calls == 0

    store.reservation_path.unlink()
    foreign = secrets.token_hex(32)
    _reserve(store, foreign, profile=ProfileId.PZ_RISING)
    foreign_response = await controller.execute(RpcRequest(
        request_id=uuid4(),
        actor="sunlit-auto-update",
        action=_action(foreign),
    ))
    assert not foreign_response.ok and foreign_response.error.code is ErrorCode.SLOT_CONFLICT
    assert backups.calls == 0


@pytest.mark.asyncio
async def test_capability_handoff_rejects_generation_drift_before_backup(tmp_path: Path):
    class DriftingStore(ReservationStore):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.calls = 0

        def authorize_handoff(self, profile, capability_token, *, operation_kind="update"):
            current = super().authorize_handoff(
                profile, capability_token, operation_kind=operation_kind,
            )
            self.calls += 1
            if self.calls == 1:
                payload = json.loads(self.reservation_path.read_text())
                payload["state_generation"] += 1
                self.reservation_path.write_text(json.dumps(payload))
            return current

    token = secrets.token_hex(32)
    operation = tmp_path / "operation.lock"
    operation.touch(mode=0o600)
    store = DriftingStore(operation, tmp_path / "reservation.json")
    _reserve(store, token)
    backups = _Backups()
    controller = _controller(tmp_path, store, backups)

    response = await controller.execute(RpcRequest(
        request_id=uuid4(),
        actor="sunlit-auto-update",
        action=_action(token),
    ))
    assert not response.ok and response.error.code is ErrorCode.SLOT_CONFLICT
    assert backups.calls == 0


@pytest.mark.asyncio
async def test_capability_handoff_keeps_reservation_live_on_backup_failure_and_replay(tmp_path: Path):
    token = secrets.token_hex(32)
    store = _store(tmp_path)
    operation_id, generation = _reserve(store, token)
    backups = _Backups(fail=True)
    controller = _controller(tmp_path, store, backups)
    request = RpcRequest(request_id=uuid4(), actor="sunlit-auto-update", action=_action(token))

    first = await controller.execute(request)
    second = await controller.execute(request)

    assert not first.ok and first.error.code is ErrorCode.BACKUP_FAILED
    assert not second.ok and second.error.code is ErrorCode.BACKUP_FAILED
    assert backups.calls == 1
    assert store.owns_live(
        PROFILE,
        operation_id,
        generation,
        operation_kind="update",
        controller_pid=os.getpid(),
        controller_start_ticks=store.pid_start_ticks(os.getpid()),
    )


def _state_db(
    path: Path,
    old_request: str,
    *,
    kind: str,
    actor: str = "sunlit-auto-update",
    canonical_request_id: str | None = None,
    response_request_id: str | None = None,
    extra_action: dict | None = None,
    status: str = "completed",
) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE rpc_idempotency(request_id TEXT PRIMARY KEY, canonical_request TEXT NOT NULL, response TEXT, status TEXT NOT NULL, created_at TEXT NOT NULL)"
    )
    action = {
        "kind": "create_backup",
        "profile_id": PROFILE.value,
        "protected": True,
        "destination": "horizon-b2",
    }
    if extra_action:
        action.update(extra_action)
    canonical = {
        "request_id": canonical_request_id or old_request,
        "actor": actor,
        "action": action,
    }
    if kind == "prejob":
        response = {
            "request_id": response_request_id or old_request,
            "ok": False,
            "error": {
                "code": "slot_conflict",
                "message": "game slot is reserved",
                "retryable": False,
                "details": None,
            },
        }
    else:
        response = {
            "request_id": response_request_id or old_request,
            "ok": True,
            "result": {"job_id": "backup-record-id"},
        }
    connection.execute(
        "INSERT INTO rpc_idempotency(request_id,canonical_request,response,status,created_at) VALUES(?,?,?,?,?)",
        (old_request, json.dumps(canonical), json.dumps(response), status, "2026-09-13T00:00:00Z"),
    )
    connection.commit()
    return connection


def test_cached_prejob_slot_conflict_gets_new_request_and_keeps_evidence(tmp_path: Path, monkeypatch):
    old_request = str(uuid4())
    token = secrets.token_hex(32)
    database = tmp_path / "state.db"
    connection = _state_db(
        database,
        old_request,
        kind="prejob",
        extra_action={"reservation_capability": token, "internal_note": "must-not-be-copied"},
    )
    root = tmp_path / "staging"
    root.mkdir()
    updater._write_private_file(root / "backup-request-id", (old_request + "\n").encode())
    monkeypatch.setattr(updater, "DATABASE", database)

    replacement = updater._backup_request_id(root, _lease(token))

    assert replacement != old_request
    assert (root / "backup-request-id").read_text().strip() == replacement
    raw_evidence = (root / f"backup-request-recovery-{old_request}.json").read_text()
    evidence = json.loads(raw_evidence)
    assert evidence["old_request_id"] == old_request
    assert evidence["replacement_request_id"] == replacement
    assert evidence["response"]["error"]["code"] == "slot_conflict"
    # Evidence is the fixed action plus safe error fields only: no capability or
    # unrelated canonical/response fields are copied through.
    assert evidence["canonical_request"]["action"] == {
        "kind": "create_backup",
        "profile_id": PROFILE.value,
        "protected": True,
        "destination": "horizon-b2",
    }
    assert "must-not-be-copied" not in raw_evidence
    assert token not in raw_evidence
    assert set(evidence["response"]["error"]) == {"code", "message", "retryable"}
    assert connection.execute(
        "SELECT response FROM rpc_idempotency WHERE request_id=?", (old_request,)
    ).fetchone()[0]
    connection.close()


def test_ambiguous_cached_request_is_not_retried(tmp_path: Path, monkeypatch):
    old_request = str(uuid4())
    token = secrets.token_hex(32)
    database = tmp_path / "state.db"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE rpc_idempotency(request_id TEXT PRIMARY KEY, canonical_request TEXT NOT NULL, response TEXT, status TEXT NOT NULL, created_at TEXT NOT NULL)"
    )
    connection.commit()
    connection.close()
    root = tmp_path / "staging"
    root.mkdir()
    updater._write_private_file(root / "backup-request-id", (old_request + "\n").encode())
    monkeypatch.setattr(updater, "DATABASE", database)

    with pytest.raises(updater.UpdateError, match="outcome is unknown"):
        updater._backup_request_id(root, _lease(token))
    assert (root / "backup-request-id").read_text().strip() == old_request


def test_cached_success_bound_to_live_lease_is_reused(tmp_path: Path, monkeypatch):
    old_request = str(uuid4())
    token = secrets.token_hex(32)
    database = tmp_path / "state.db"
    connection = _state_db(database, old_request, kind="success")
    connection.close()
    root = tmp_path / "staging"
    root.mkdir()
    updater._write_private_file(root / "backup-request-id", (old_request + "\n").encode())
    # The authority sidecar records the creating reservation's capability hash.
    updater._write_request_authority(root, old_request, _lease(token, "op-1"))
    monkeypatch.setattr(updater, "DATABASE", database)

    assert updater._backup_request_id(root, _lease(token, "op-1")) == old_request


def test_cross_run_cached_success_fails_closed(tmp_path: Path, monkeypatch):
    """A backup that succeeded under a dead run must never be reused as fresh."""
    old_request = str(uuid4())
    dead_token = secrets.token_hex(32)
    live_token = secrets.token_hex(32)
    database = tmp_path / "state.db"
    connection = _state_db(database, old_request, kind="success")
    connection.close()
    root = tmp_path / "staging"
    root.mkdir()
    updater._write_private_file(root / "backup-request-id", (old_request + "\n").encode())
    # Written by the previous updater process (now dead) with a different lease.
    updater._write_request_authority(root, old_request, _lease(dead_token, "op-old"))
    monkeypatch.setattr(updater, "DATABASE", database)

    with pytest.raises(updater.UpdateError, match="cannot be proven fresh"):
        updater._backup_request_id(root, _lease(live_token, "op-old"))

    # No new request, no authority rewrite, and the durable row is untouched.
    assert (root / "backup-request-id").read_text().strip() == old_request
    assert not (root / f"backup-request-recovery-{old_request}.json").exists()
    authority = json.loads((root / "backup-request-authority.json").read_text())
    assert authority["capability_sha256"] == hashlib.sha256(dead_token.encode()).hexdigest()
    check = sqlite3.connect(database)
    assert check.execute(
        "SELECT status FROM rpc_idempotency WHERE request_id=?", (old_request,)
    ).fetchone() == ("completed",)
    check.close()


def test_cross_run_cached_success_without_authority_fails_closed(tmp_path: Path, monkeypatch):
    old_request = str(uuid4())
    database = tmp_path / "state.db"
    connection = _state_db(database, old_request, kind="success")
    connection.close()
    root = tmp_path / "staging"
    root.mkdir()
    updater._write_private_file(root / "backup-request-id", (old_request + "\n").encode())
    monkeypatch.setattr(updater, "DATABASE", database)

    with pytest.raises(updater.UpdateError, match="cannot be proven fresh"):
        updater._backup_request_id(root, _lease(secrets.token_hex(32), "op-1"))


def test_foreign_actor_or_request_id_row_is_ambiguous(tmp_path: Path, monkeypatch):
    """A same-key row that is not our actor/request id is never trusted."""
    old_request = str(uuid4())
    database = tmp_path / "state.db"
    for actor, canonical_id, response_id in (
        ("someone-else", old_request, old_request),
        ("sunlit-auto-update", str(uuid4()), old_request),
        ("sunlit-auto-update", old_request, str(uuid4())),
    ):
        if database.exists():
            database.unlink()
        connection = _state_db(
            database,
            old_request,
            kind="prejob",
            actor=actor,
            canonical_request_id=canonical_id,
            response_request_id=response_id,
        )
        connection.close()
        root = tmp_path / f"staging-{actor}-{canonical_id[:4]}-{response_id[:4]}"
        root.mkdir()
        updater._write_private_file(root / "backup-request-id", (old_request + "\n").encode())
        monkeypatch.setattr(updater, "DATABASE", database)
        with pytest.raises(updater.UpdateError, match="outcome is ambiguous"):
            updater._backup_request_id(root, _lease(secrets.token_hex(32), "op-1"))
        assert not (root / f"backup-request-recovery-{old_request}.json").exists()


def test_request_backup_passes_capability_and_verifies_protection(tmp_path: Path, monkeypatch):
    token = secrets.token_hex(32)
    database = tmp_path / "state.db"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE backups(id TEXT PRIMARY KEY, profile_id TEXT, verified INTEGER, protected INTEGER);
        CREATE TABLE backup_protections(
            backup_id TEXT, profile_id TEXT, destination_id TEXT,
            upload_state TEXT, remote_verified INTEGER, comparison_state TEXT,
            backup_class TEXT
        );
        """
    )
    connection.execute(
        "INSERT INTO backups VALUES('backup-record-id', ?, 1, 1)", (PROFILE.value,)
    )
    connection.execute(
        "INSERT INTO backup_protections VALUES('backup-record-id', ?, 'horizon-b2', "
        "'succeeded', 1, 'verified', 'application')",
        (PROFILE.value,),
    )
    connection.commit()
    connection.close()
    root = tmp_path / "staging"
    root.mkdir()
    commands: list[list[str]] = []
    inputs: list[str | None] = []

    def fake_run(argv, *, timeout, input_text=None):
        commands.append(list(argv))
        inputs.append(input_text)
        return SimpleNamespace(stdout=json.dumps({"job_id": "backup-record-id", "state": "running"}))

    monkeypatch.setattr(updater, "DATABASE", database)
    monkeypatch.setattr(updater, "_run", fake_run)
    result = updater._request_backup(root, _lease(token))

    assert result == "backup-record-id"
    # The capability travels over the private stdin pipe, never argv.
    assert token not in commands[0]
    assert "--reservation-capability" not in commands[0]
    assert inputs[0] == token + "\n"
    # Only the capability hash is persisted alongside the request identity.
    authority_text = (root / "backup-request-authority.json").read_text()
    assert token not in authority_text
    assert hashlib.sha256(token.encode("ascii")).hexdigest() in authority_text


def _load_helper():
    loader = importlib.machinery.SourceFileLoader("sunlit_rpc_helper", str(RPC_HELPER))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_helper_request_reaches_real_controller_reservation(monkeypatch):
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hzn") as directory:
        root = Path(directory)
        token = secrets.token_hex(32)
        store = ReservationStore(root / "op.lock", root / "reservation.json")
        (root / "op.lock").write_bytes(b"")
        os.chmod(root / "op.lock", 0o600)
        operation_id, generation = _reserve(store, token)
        backups = _Backups()
        profile = _profile(root)
        controller = Controller(
            profiles={profile.id: profile},
            reservation_store=store,
            services=SimpleNamespace(backups=backups),
            operation_lock_factory=_MemoryLock,
        )
        socket_path = root / "s"

        async def handler(reader, writer):
            try:
                request = parse_request_line(await reader.readline())
                response = await controller.execute(request)
                writer.write(response_json(response))
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_unix_server(handler, path=str(socket_path))
        try:
            helper = _load_helper()
            result = await helper.request_backup(
                uuid4(), token, socket_path=str(socket_path),
            )
            assert result["job_id"] == "backup-record-id"
            assert backups.calls == 1
            assert backups.lease_checks == [True]
            assert store.owns_live(
                PROFILE,
                operation_id,
                generation,
                operation_kind="update",
                controller_pid=os.getpid(),
                controller_start_ticks=store.pid_start_ticks(os.getpid()),
            )
        finally:
            server.close()
            await server.wait_closed()


def test_helper_read_capability_rejects_and_never_echoes_input():
    helper = _load_helper()
    marker = b"internal-diagnostic-sentinel"
    with pytest.raises(helper.MalformedCapability) as info:
        helper.read_capability(io.BytesIO(marker + b"\n"))
    assert "internal-diagnostic-sentinel" not in str(info.value)
    token = secrets.token_hex(32).encode()
    for bad in (
        b"",  # nothing at all (closed pipe)
        token,  # no trailing newline
        token + b"\n\n",  # trailing newline input
        token + b"\ngarbage",  # trailing input after the newline
        token[:-1] + b"\n",  # 63 hex characters
        token.upper() + b"\n",  # uppercase hex is not accepted
        b"G" * 64 + b"\n",  # 64 bytes but not lowercase hex
    ):
        with pytest.raises(helper.MalformedCapability) as info:
            helper.read_capability(io.BytesIO(bad))
        assert token.decode() not in str(info.value)


def test_helper_read_capability_accepts_exactly_one_line():
    helper = _load_helper()
    token = secrets.token_hex(32)
    assert helper.read_capability(io.BytesIO((token + "\n").encode())) == token


def test_helper_main_malformed_stdin_stays_safe(monkeypatch, capsys):
    helper = _load_helper()
    marker = b"internal-diagnostic-sentinel"
    monkeypatch.setattr(helper.os, "geteuid", lambda: 1000)
    monkeypatch.setattr(
        helper.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(marker + b"\n"))
    )
    rc = helper.main(["backup", "--request-id", str(uuid4())])
    captured = capsys.readouterr()
    assert rc == 2
    assert "internal-diagnostic-sentinel" not in captured.out
    assert "internal-diagnostic-sentinel" not in captured.err
    assert "malformed" in captured.err


def test_helper_main_consumes_capability_from_stdin(monkeypatch, capsys):
    helper = _load_helper()
    token = secrets.token_hex(32)
    seen: dict[str, str] = {}

    async def fake_request_backup(request_id, reservation_capability, *, socket_path=None):
        seen["capability"] = reservation_capability
        return {"job_id": "backup-record-id", "state": "running"}

    monkeypatch.setattr(helper.os, "geteuid", lambda: 1000)
    monkeypatch.setattr(helper, "request_backup", fake_request_backup)
    monkeypatch.setattr(
        helper.sys, "stdin", SimpleNamespace(buffer=io.BytesIO((token + "\n").encode()))
    )
    rc = helper.main(["backup", "--request-id", str(uuid4())])
    captured = capsys.readouterr()
    assert rc == 0
    assert seen["capability"] == token
    assert json.loads(captured.out)["job_id"] == "backup-record-id"


def test_safe_helper_detail_redacts_token_prefix_near_cutoff():
    token = secrets.token_hex(32)
    # "error: " (7) + 183 x's puts the token at column 190, so its first ten
    # characters fall inside a truncate-to-200 pass.  Redacting after truncating
    # would leak them (ten hex characters is below the token-shape threshold).
    stderr = f"error: {'x' * 183}{token} tail\n"
    detail = updater._safe_helper_detail(stderr, secret=token + "\n")
    assert token not in detail
    assert token[:10] not in detail
    assert len(detail) <= 200


def test_safe_helper_detail_removes_exact_secret_even_below_token_shape():
    # A short secret is not a "token-like" run, so it must be removed explicitly.
    detail = updater._safe_helper_detail("error: abcdef failed\n", secret="abcdef\n")
    assert "abcdef" not in detail
    assert "[redacted]" in detail


def test_run_failure_never_exposes_input_and_suppresses_chaining():
    token = secrets.token_hex(32)
    script = (
        "import sys; sys.stderr.write('boom ' + sys.stdin.read().strip() + '\\n'); "
        "raise SystemExit(3)"
    )
    with pytest.raises(updater.UpdateError) as info:
        updater._run([sys.executable, "-c", script], timeout=30, input_text=token + "\n")
    message = str(info.value)
    assert token not in message
    assert token[:10] not in message
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__ is True


def test_canonical_request_identity_excludes_capability():
    token = secrets.token_hex(32)
    request = RpcRequest(request_id=uuid4(), actor="sunlit-auto-update", action=_action(token))
    canonical = Controller._canonical(request)
    assert token not in canonical
    assert "reservation_capability" not in canonical
    assert repr(request.action) != "" and token not in repr(request.action)


@pytest.mark.asyncio
async def test_handoff_cancellation_drains_worker_before_finishing(tmp_path: Path, monkeypatch):
    token = secrets.token_hex(32)
    store = _store(tmp_path)
    operation_id, generation = _reserve(store, token)

    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    class BlockingBackups:
        def __init__(self):
            self.calls = 0

        async def create(self, action, actor=None, request_id=None, lease_check=None, job_id=None):
            self.calls += 1

            def work():
                started.set()
                release.wait(5)
                finished.set()

            await asyncio.to_thread(work)
            return JobAccepted(job_id="backup-record-id", state="running")

    backups = BlockingBackups()
    controller = _controller(tmp_path, store, backups)
    request = RpcRequest(request_id=uuid4(), actor="sunlit-auto-update", action=_action(token))
    task = asyncio.ensure_future(controller.execute(request))
    await asyncio.to_thread(started.wait, 5)
    task.cancel()
    # The coroutine must not complete while its worker thread is still running.
    await asyncio.sleep(0.2)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    # The controller never releases the updater-owned reservation, and the job
    # is recorded as failed rather than succeeded.
    assert store.owns_live(
        PROFILE,
        operation_id,
        generation,
        operation_kind="update",
        controller_pid=os.getpid(),
        controller_start_ticks=store.pid_start_ticks(os.getpid()),
    )
    assert controller._db().execute(
        "SELECT state FROM jobs WHERE operation='backup'"
    ).fetchone() == ("failed",)


@pytest.mark.asyncio
async def test_handoff_lost_owner_prevents_successful_completion(tmp_path: Path):
    token = secrets.token_hex(32)
    store = _store(tmp_path)
    _reserve(store, token)

    entered = threading.Event()
    proceed = threading.Event()
    checks: list[bool] = []

    class LeaseCheckingBackups:
        async def create(self, action, actor=None, request_id=None, lease_check=None, job_id=None):
            entered.set()
            await asyncio.to_thread(proceed.wait, 5)
            ok = bool(lease_check()) if lease_check is not None else False
            checks.append(ok)
            if not ok:
                raise SafeError("slot_conflict", "operation lease was lost before backup publication")
            return JobAccepted(job_id="backup-record-id", state="running")

    backups = LeaseCheckingBackups()
    controller = _controller(tmp_path, store, backups)
    request = RpcRequest(request_id=uuid4(), actor="sunlit-auto-update", action=_action(token))
    task = asyncio.ensure_future(controller.execute(request))
    await asyncio.to_thread(entered.wait, 5)
    # The updater's reservation disappears while the worker is mid-flight.
    store.reservation_path.unlink()
    proceed.set()
    response = await task
    assert not response.ok
    assert response.error.code is ErrorCode.SLOT_CONFLICT
    assert checks == [False]
    assert controller._db().execute(
        "SELECT state FROM jobs WHERE operation='backup'"
    ).fetchone() == ("failed",)


class _FacadeBackupService:
    """Minimal BackupService double used only through the real facade code."""

    def __init__(self, record_id: str):
        self.record_id = record_id
        self.free_space = lambda path: 1 << 40
        self.clock = None
        self.tar_runner = None
        self.lease_checks: list[bool] = []
        self._inserted = False

    def create(self, action, actor=None, request_id=None, *, protected=None, destination=None):
        return SimpleNamespace(id=self.record_id)

    def _insert(self, record):  # pragma: no cover - isolated worker path skips this
        self._inserted = True


@pytest.mark.asyncio
async def test_real_backup_facade_returns_record_id_through_handoff(tmp_path: Path):
    token = secrets.token_hex(32)
    store = _store(tmp_path)
    operation_id, generation = _reserve(store, token)
    profile = _profile(tmp_path)
    record_id = "20260913T000000000000Z-abcdef012345"

    class _Adapter:
        def observe(self, profile):
            return SimpleNamespace(running=False)

    seen: dict[str, list[bool]] = {"checks": []}

    def factory(profile, *, database=None, lease_check=None, **kwargs):
        service = _FacadeBackupService(record_id)

        def _create(action, actor=None, request_id=None, *, protected=None, destination=None):
            seen["checks"].append(bool(lease_check()) if lease_check is not None else False)
            return SimpleNamespace(id=record_id)

        service.create = _create
        return service

    facade = BackupRpcFacade(
        profiles={PROFILE.value: profile},
        adapters={profile.id: _Adapter()},
        database=SimpleNamespace(path=tmp_path / "state.db"),
        backup_service_factory=factory,
        isolated_database_factory=lambda database: SimpleNamespace(),
        close_database=lambda database: None,
    )
    controller = Controller(
        profiles={profile.id: profile},
        reservation_store=store,
        services=SimpleNamespace(backups=facade),
        operation_lock_factory=_MemoryLock,
    )
    response = await controller.execute(
        RpcRequest(request_id=uuid4(), actor="sunlit-auto-update", action=_action(token))
    )
    assert response.ok
    # The response job id is the real backup record id the updater protects.
    assert response.result.job_id == record_id
    assert seen["checks"] == [True]
    assert store.owns_live(
        PROFILE,
        operation_id,
        generation,
        operation_kind="update",
        controller_pid=os.getpid(),
        controller_start_ticks=store.pid_start_ticks(os.getpid()),
    )
