from __future__ import annotations

import importlib.util
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).parents[1]
HELPER = ROOT / "ops/bin/horizon-bore-liveness"


def _module():
    loader = SourceFileLoader("horizon_bore_liveness", str(HELPER))
    spec = importlib.util.spec_from_loader("horizon_bore_liveness", loader)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bore_target_is_derived_from_the_fixed_live_process_contract():
    module = _module()
    target = module._bore_target(
        (
            "/usr/local/bin/bore",
            "local",
            "25565",
            "--local-host",
            "127.0.0.1",
            "--to",
            "203.0.113.10",
            "--port",
            "25565",
        )
    )
    assert target == ("203.0.113.10", 25565)


def test_six_failures_over_three_minutes_trigger_one_restart():
    module = _module()
    state = {"failures": 0, "first_failure": 0, "last_restart": 0}
    decisions = []
    for now in (100, 136, 172, 208, 244, 280):
        decision, state = module._failure_decision(state, now)
        decisions.append(decision)
    assert decisions == ["observe"] * 5 + ["restart"]
    assert state == {"failures": 6, "first_failure": 100, "last_restart": 0}


def test_arm_marker_contract_requires_root_owned_regular_0600_single_link():
    module = _module()
    valid = SimpleNamespace(st_mode=0o100600, st_uid=0, st_gid=0, st_nlink=1)
    assert module._valid_arm_stat(valid)
    assert not module._valid_arm_stat(SimpleNamespace(**{**vars(valid), "st_mode": 0o100644}))
    assert not module._valid_arm_stat(SimpleNamespace(**{**vars(valid), "st_uid": 1000}))
    assert not module._valid_arm_stat(SimpleNamespace(**{**vars(valid), "st_nlink": 2}))
    assert not module._valid_arm_stat(SimpleNamespace(**{**vars(valid), "st_mode": 0o120600}))


def test_restart_cooldown_prevents_a_recovery_loop():
    module = _module()
    state = {"failures": 0, "first_failure": 0, "last_restart": 220}
    decisions = []
    for now in (280, 340, 400, 460):
        decision, state = module._failure_decision(state, now)
        decisions.append(decision)
    assert decisions == ["cooldown", "cooldown", "cooldown", "cooldown"]
    assert state["last_restart"] == 220


def _configure_runtime(module, tmp_path, monkeypatch):
    module.STATE_DIR = tmp_path
    module.STATE_FILE = tmp_path / "state.json"
    module.LOCK_FILE = tmp_path / "lock"
    monkeypatch.setattr(module, "_arm_marker_state", lambda: "valid")
    monkeypatch.setattr(module, "_service_active", lambda _unit: True)
    monkeypatch.setattr(module, "_service_main_pid", lambda _unit: 123)
    monkeypatch.setattr(module, "_local_listener_owned", lambda _pid: True)
    monkeypatch.setattr(module, "_bore_control_owned", lambda _pid: True)
    monkeypatch.setattr(module, "_local_client_sessions_present", lambda: False)
    monkeypatch.setattr(
        module,
        "_process_argv",
        lambda _pid: (
            "/usr/local/bin/bore",
            "local",
            "25565",
            "--local-host",
            "127.0.0.1",
            "--to",
            "203.0.113.10",
            "--port",
            "25565",
        ),
    )


def test_control_plane_outage_never_restarts_bore(tmp_path, monkeypatch):
    module = _module()
    _configure_runtime(module, tmp_path, monkeypatch)
    module._save_state({"failures": 5, "first_failure": 820, "last_restart": 0})
    monkeypatch.setattr(
        module,
        "_tcp_open",
        lambda host, port: (host, port) == (module.LOCAL_HOST, module.LOCAL_PORT),
    )
    monkeypatch.setattr(
        module,
        "_restart_bore",
        lambda: (_ for _ in ()).throw(AssertionError("must not restart during an outage")),
    )
    assert module._run_once() == 0
    assert module._load_state() == {"failures": 0, "first_failure": 0, "last_restart": 0}


def test_sustained_public_failure_restarts_only_bore_and_verifies_recovery(tmp_path, monkeypatch):
    module = _module()
    _configure_runtime(module, tmp_path, monkeypatch)
    module._save_state({"failures": 5, "first_failure": 820, "last_restart": 0})
    restarted = {"value": False}

    def tcp_open(host, port):
        if (host, port) in {
            (module.LOCAL_HOST, module.LOCAL_PORT),
            ("203.0.113.10", module.CONTROL_PORT),
        }:
            return True
        return restarted["value"]

    monkeypatch.setattr(module, "_tcp_open", tcp_open)
    monkeypatch.setattr(module, "_restart_bore", lambda: restarted.update(value=True))
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(module.time, "monotonic", lambda: 1000.0)
    assert module._run_once() == 0
    assert restarted["value"]
    assert module._load_state() == {"failures": 0, "first_failure": 0, "last_restart": 1000}


def test_pre_restart_marker_race_aborts_without_touching_bore(tmp_path, monkeypatch):
    module = _module()
    _configure_runtime(module, tmp_path, monkeypatch)
    module._save_state({"failures": 5, "first_failure": 820, "last_restart": 0})
    marker_states = iter(("valid", "absent"))
    monkeypatch.setattr(module, "_arm_marker_state", lambda: next(marker_states))
    monkeypatch.setattr(
        module,
        "_tcp_open",
        lambda host, port: (host, port)
        in {
            (module.LOCAL_HOST, module.LOCAL_PORT),
            ("203.0.113.10", module.CONTROL_PORT),
        },
    )
    monkeypatch.setattr(
        module,
        "_restart_bore",
        lambda: (_ for _ in ()).throw(AssertionError("must not restart after disarm")),
    )
    monkeypatch.setattr(module.time, "monotonic", lambda: 1000.0)
    assert module._run_once() == 0
    assert module._load_state() == {"failures": 0, "first_failure": 0, "last_restart": 0}


def test_active_local_session_suppresses_recovery(tmp_path, monkeypatch):
    module = _module()
    _configure_runtime(module, tmp_path, monkeypatch)
    module._save_state({"failures": 5, "first_failure": 820, "last_restart": 0})
    monkeypatch.setattr(module, "_local_client_sessions_present", lambda: True)
    monkeypatch.setattr(
        module,
        "_tcp_open",
        lambda host, port: (host, port)
        in {
            (module.LOCAL_HOST, module.LOCAL_PORT),
            ("203.0.113.10", module.CONTROL_PORT),
        },
    )
    monkeypatch.setattr(
        module,
        "_restart_bore",
        lambda: (_ for _ in ()).throw(AssertionError("must not disconnect an active session")),
    )
    monkeypatch.setattr(module.time, "monotonic", lambda: 1000.0)
    assert module._run_once() == 0
    assert module._load_state() == {"failures": 0, "first_failure": 0, "last_restart": 0}


def test_malformed_state_fails_closed(tmp_path):
    module = _module()
    module.STATE_FILE = tmp_path / "state.json"
    module.STATE_FILE.write_text("not-json")
    try:
        module._load_state()
    except RuntimeError as error:
        assert "invalid" in str(error)
    else:
        raise AssertionError("malformed state must not reset the restart budget")


def test_state_metadata_drift_and_symlink_fail_closed(tmp_path):
    module = _module()
    module.STATE_DIR = tmp_path
    module.STATE_FILE = tmp_path / "state.json"
    module._save_state({"failures": 0, "first_failure": 0, "last_restart": 0})
    module.STATE_FILE.chmod(0o644)
    with pytest.raises(RuntimeError, match="metadata is invalid"):
        module._load_state()

    module.STATE_FILE.unlink()
    target = tmp_path / "target.json"
    target.write_text("{}")
    target.chmod(0o600)
    module.STATE_FILE.symlink_to(target)
    with pytest.raises(RuntimeError, match="state is invalid"):
        module._load_state()


@pytest.mark.parametrize("failure_helper", ("_write_all", "_sync_descriptor"))
def test_failed_state_write_or_fsync_cleans_temporary_file(tmp_path, monkeypatch, failure_helper):
    module = _module()
    module.STATE_DIR = tmp_path
    module.STATE_FILE = tmp_path / "state.json"
    monkeypatch.setattr(
        module,
        failure_helper,
        lambda *_args: (_ for _ in ()).throw(OSError("synthetic state failure")),
    )
    with pytest.raises(OSError, match="synthetic state failure"):
        module._save_state({"failures": 0, "first_failure": 0, "last_restart": 0})
    assert not module.STATE_FILE.exists()
    assert not tuple(tmp_path.glob(".state.*"))


def test_lock_contention_is_a_non_mutating_skip(tmp_path, monkeypatch):
    module = _module()
    module.STATE_DIR = tmp_path
    module.STATE_FILE = tmp_path / "state.json"
    module.LOCK_FILE = tmp_path / "lock"
    descriptor = module.os.open(module.LOCK_FILE, module.os.O_RDWR | module.os.O_CREAT, 0o600)
    try:
        module.fcntl.flock(descriptor, module.fcntl.LOCK_EX | module.fcntl.LOCK_NB)
        monkeypatch.setattr(
            module,
            "_arm_marker_state",
            lambda: (_ for _ in ()).throw(AssertionError("contended run must not inspect state")),
        )
        assert module._run_once() == 0
    finally:
        module.os.close(descriptor)


def test_recovery_wait_uses_a_monotonic_deadline(monkeypatch):
    module = _module()
    ticks = iter((0.0, 0.0, 16.0, 16.0))
    monkeypatch.setattr(module.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(module, "_edge_gate", lambda *_args, **_kwargs: "public_listener_missing")
    assert module._wait_for_recovery("203.0.113.10", 25565) == (
        False,
        "restart_did_not_restore_edge",
    )


def test_post_restart_public_socket_is_not_enough_without_local_proxy(tmp_path, monkeypatch):
    module = _module()
    _configure_runtime(module, tmp_path, monkeypatch)
    module._save_state({"failures": 5, "first_failure": 820, "last_restart": 0})
    restarted = {"value": False}

    def tcp_open(host, port):
        if (host, port) == ("203.0.113.10", module.CONTROL_PORT):
            return True
        if (host, port) == ("203.0.113.10", 25565):
            return restarted["value"]
        if (host, port) == (module.LOCAL_HOST, module.LOCAL_PORT):
            return not restarted["value"]
        return False

    monkeypatch.setattr(module, "_tcp_open", tcp_open)
    monkeypatch.setattr(module, "_restart_bore", lambda: restarted.update(value=True))
    monkeypatch.setattr(module.time, "monotonic", lambda: 1000.0)
    assert module._run_once() == 1
    assert restarted["value"]


def test_kernel_socket_ownership_and_player_session_gates(monkeypatch):
    module = _module()
    listener = {
        "family": "ipv4",
        "local_address": "00000000",
        "local_port": 25565,
        "remote_port": 0,
        "state": "0A",
        "inode": 41,
    }
    control = {
        "family": "ipv4",
        "local_address": "0100007F",
        "local_port": 41000,
        "remote_port": 7835,
        "state": "01",
        "inode": 42,
    }
    monkeypatch.setattr(module, "_tcp_rows", lambda: (listener, control))
    monkeypatch.setattr(module, "_pid_socket_inodes", lambda pid: frozenset({41 if pid == 10 else 42}))
    assert module._local_listener_owned(10)
    assert module._bore_control_owned(20)
    assert not module._local_client_sessions_present()

    client = {**listener, "state": "01", "inode": 43}
    monkeypatch.setattr(module, "_tcp_rows", lambda: (listener, control, client))
    assert module._local_client_sessions_present()


def test_restart_subprocess_is_one_fixed_bore_argv(monkeypatch):
    module = _module()
    observed = {}

    def run(argv, **kwargs):
        observed["argv"] = argv
        observed["kwargs"] = kwargs
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", run)
    module._restart_bore()
    assert observed["argv"] == [
        "/usr/bin/systemctl",
        "restart",
        "bore-minecraft-fenced.service",
    ]
    assert observed["kwargs"]["check"] is True
    assert observed["kwargs"]["timeout"] == module.SYSTEMCTL_RESTART_TIMEOUT_SECONDS
    assert observed["kwargs"]["stdin"] is module.subprocess.DEVNULL


def test_units_are_timer_driven_and_never_own_the_java_backend():
    helper = HELPER.read_text()
    service = (ROOT / "ops/systemd/horizon-bore-liveness.service").read_text()
    timer = (ROOT / "ops/systemd/horizon-bore-liveness.timer").read_text()
    assert "bore-minecraft-fenced.service" in helper
    assert "lazymc-minecraft.service" in helper
    assert "minecraft-sunlit-cobblemon.service" not in helper
    assert "systemctl\", \"restart\", BORE_UNIT" in helper
    assert "Requires=" not in service
    assert "ConditionPathExists=" not in service
    assert "_arm_marker_state()" in helper
    assert "RuntimeDirectory=horizon-bore-liveness" in service
    assert "RuntimeDirectoryPreserve=yes" in service
    assert "CapabilityBoundingSet=CAP_DAC_READ_SEARCH CAP_SYS_PTRACE" in service
    assert "Description=Check the Horizon Minecraft Bore relay every 30 seconds" in timer
    assert "OnUnitActiveSec=30s" in timer
    assert "Persistent=" not in timer


def test_fenced_relay_recovers_clean_and_failed_exits_with_bounded_retries():
    unit = (ROOT / "ops/systemd/bore-minecraft-fenced.service").read_text()
    unit_section = unit.split("[Unit]", 1)[1].split("[Service]", 1)[0]
    # `Restart=always` covers the clean exit a dropped or rebooted remote relay
    # produces; `systemctl stop` is still the untouched intentional-stop path
    # because systemd never restarts a unit after an explicit stop request.
    assert "Restart=always" in unit
    assert "RestartSec=15" in unit
    restart_sec = int(unit.split("RestartSec=", 1)[1].splitlines()[0])
    assert 5 <= restart_sec <= 60
    # An unbounded start rate limit (0) keeps a long remote outage from
    # stranding the client in `failed`, which is what previously required a
    # manual start. Retries stay paced by RestartSec, so this is not a hot loop.
    assert "StartLimitIntervalSec=0" in unit_section
    assert "StartLimitBurst" not in unit_section
    # The existing fences that gate every start job must survive the change.
    assert "ConditionPathExists=/etc/game-control/arm/bore-minecraft" in unit
    assert "ExecStartPre=+/usr/bin/sh -c" in unit
    assert "stat -c %%u:%%a" in unit and "0:600" in unit
    assert "OnFailure=horizon-alert-notify@bore-minecraft-fenced.service" in unit
    assert "[Install]" not in unit


def test_inactive_or_failed_relay_is_never_started_indiscriminately(tmp_path, monkeypatch):
    module = _module()
    _configure_runtime(module, tmp_path, monkeypatch)
    module._save_state({"failures": 4, "first_failure": 700, "last_restart": 0})
    # is-active is false for both an operator stop and a failed unit, so the
    # timer must leave recovery to the unit's restart policy and the operator.
    monkeypatch.setattr(
        module,
        "_service_active",
        lambda unit: False if unit == module.BORE_UNIT else True,
    )
    monkeypatch.setattr(
        module,
        "_restart_bore",
        lambda: (_ for _ in ()).throw(AssertionError("must not start an inactive relay")),
    )
    assert module._run_once() == 0
    assert module._load_state() == {"failures": 0, "first_failure": 0, "last_restart": 0}
    helper = HELPER.read_text()
    assert "\"restart\"" in helper
    assert "\"start\"" not in helper
