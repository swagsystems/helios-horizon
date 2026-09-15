from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path
from importlib.machinery import SourceFileLoader

import pytest


ROOT = Path(__file__).parents[1]
OPS_BIN = ROOT / "ops/bin"


def _load(name: str, path: Path):
    loader = SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


alerts = _load("horizon_alert_notify", OPS_BIN / "horizon-alert-notify")
journal = _load("horizon_journal", OPS_BIN / "horizon_journal.py")


def _entries(invocation: str, timestamps: list[int], *, complete: bool = True):
    result = []
    for index, timestamp in enumerate(timestamps):
        entry = {
            "_SYSTEMD_UNIT": journal.SUNLIT_UNIT,
            "_SYSTEMD_INVOCATION_ID": invocation,
            "__REALTIME_TIMESTAMP": str(timestamp),
            "MESSAGE": "Done (1.0 seconds)" if complete and index == len(timestamps) - 1 else "line",
        }
        result.append(entry)
    return result


def test_alert_target_allowlist_and_fixed_unit_queries(monkeypatch, tmp_path):
    queried = []
    posted = []

    monkeypatch.setattr(alerts, "_unit_state", lambda unit: (queried.append(unit) or ("failed", "dead")))
    monkeypatch.setattr(alerts, "_post", lambda payload: (posted.append(payload) or (True, 1)))
    log = tmp_path / "alerts" / "target-failures.jsonl"
    log.parent.mkdir(mode=0o700)
    assert alerts.notify("controller", log_path=log) == 0
    assert queried == ["game-slotd.service"]
    assert posted[0][0]["labels"]["target_id"] == "controller"
    assert posted[0][0]["labels"]["hostname"]
    assert posted[0][0]["labels"]["active_state"] == "failed"
    record = json.loads(log.read_text())
    assert record["unit"] == "game-slotd.service"
    assert record["event_id"]
    assert record["delivery_ok"] is True

    for target_id, unit in (
        ("lazymc-minecraft", "lazymc-minecraft.service"),
        ("bore-minecraft-fenced", "bore-minecraft-fenced.service"),
        ("horizon-terraria-relay", "horizon-terraria-relay.service"),
    ):
        queried.clear()
        assert alerts.notify(target_id, log_path=log) == 0
        assert queried == [unit]
        expected_kind = "proxy" if target_id == "lazymc-minecraft" else "relay"
        assert posted[-1][0]["labels"]["kind"] == expected_kind
        assert posted[-1][0]["labels"]["target_id"] == target_id

    assert alerts.main(["--unit=evil.service"]) == 2
    assert alerts.main(["https://evil.invalid"]) == 2


def test_alert_delivery_failure_is_durable_and_bounded(monkeypatch, tmp_path):
    monkeypatch.setattr(alerts, "ALERTMANAGER_URL", "http://127.0.0.1:9093/api/v2/alerts")
    log = tmp_path / "alerts" / "target-failures.jsonl"
    log.parent.mkdir(mode=0o700)
    sleeps = []
    attempts = []

    def fail(_request, timeout):
        attempts.append(timeout)
        raise OSError("transport failure")

    monkeypatch.setattr(alerts, "urlopen", fail)
    monkeypatch.setattr(alerts.time, "sleep", sleeps.append)
    assert alerts._post([{"fixed": True}]) == (False, alerts.MAX_ATTEMPTS)
    assert len(attempts) == alerts.MAX_ATTEMPTS
    assert len(sleeps) == alerts.MAX_ATTEMPTS - 1

    monkeypatch.setattr(alerts, "_unit_state", lambda unit: ("failed", "dead"))
    monkeypatch.setattr(alerts, "_post", lambda payload: (False, 3))
    assert alerts.notify("web", log_path=log) == 1
    saved = json.loads(log.read_text().splitlines()[-1])
    assert saved["delivery_ok"] is False
    assert saved["delivery_attempts"] == 3
    assert "secret" not in log.read_text().lower()


def test_alert_post_uses_only_fixed_endpoint_and_secret_free_body(monkeypatch):
    monkeypatch.setattr(alerts, "ALERTMANAGER_URL", "http://127.0.0.1:9093/api/v2/alerts")
    seen = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, _size):
            return b""

    def accept(request, timeout):
        seen.append((request.full_url, timeout, request.data.decode()))
        return Response()

    monkeypatch.setattr(alerts, "urlopen", accept)
    assert alerts._post([{"labels": {"target_id": "web"}}]) == (True, 1)
    assert seen[0][0] == alerts.ALERTMANAGER_URL
    assert seen[0][1] == alerts.HTTP_TIMEOUT_SECONDS
    assert "secret" not in seen[0][2].lower()


def test_notifier_and_drill_units_have_retry_and_no_restart_coupling():
    notifier = (ROOT / "ops/systemd/horizon-alert-notify@.service").read_text()
    drill = (ROOT / "ops/systemd/horizon-alert-drill@.service").read_text()
    assert "RefuseManualStart=yes" in notifier
    assert "StartLimitBurst=3" in notifier
    assert "Restart=on-failure" in notifier
    assert "ExecStart=/usr/local/libexec/horizon-alert-notify %i" in notifier
    assert "Restart=no" in drill
    assert "OnFailure=horizon-alert-notify@drill-%i.service" in drill
    assert "minecraft|controller|web" in drill
    assert "ExecStart=/usr/bin/false" in drill
    assert "EnvironmentFile=/etc/game-control/alertmanager.conf" in notifier


def test_notifier_requires_deployment_endpoint_without_reference_fallback(monkeypatch):
    monkeypatch.delenv("HORIZON_ALERTMANAGER_URL", raising=False)
    module = _load("horizon_alert_notify_missing_endpoint", OPS_BIN / "horizon-alert-notify")
    assert module.ALERTMANAGER_URL == ""


def test_journal_capture_uses_fixed_sunlit_and_peak_math(monkeypatch, tmp_path):
    invocation = "a" * 32
    entries = _entries(invocation, [0, 10_000_000, 20_000_000, 31_000_001])
    monkeypatch.setattr(journal, "_journal_entries", lambda value: entries)
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(mode=0o700)
    evidence = evidence_dir / "boot-evidence.jsonl"
    record = journal.capture(invocation, evidence_path=evidence)
    assert record["profile"] == journal.SUNLIT_PROFILE
    assert record["unit"] == journal.SUNLIT_UNIT
    assert record["complete_boot"] is True
    assert record["peak_30s_lines"] == 3
    assert json.loads(evidence.read_text())["invocation_id"] == invocation


def test_journal_recognizes_prefixed_minecraft_done_message() -> None:
    entries = _entries("f" * 32, [1], complete=False)
    entries[0]["MESSAGE"] = (
        '[18:33:56] [Server thread/INFO] [minecraft/DedicatedServer]: '
        'Done (2.246s)! For help, type "help"'
    )

    assert journal._is_complete_boot(entries) is True


def test_journal_query_contains_only_fixed_namespace_unit_and_invocation(monkeypatch):
    invocation = "e" * 32
    calls = []

    def run(command, **_kwargs):
        calls.append(command)

        class Result:
            stdout = json.dumps(_entries(invocation, [1])[0]) + "\n"

        return Result()

    monkeypatch.setattr(journal.subprocess, "run", run)
    assert journal._journal_entries(invocation)
    assert calls[0] == [
        journal.JOURNALCTL,
        "--namespace=horizon",
        "--output=json",
        "--no-pager",
        f"_SYSTEMD_UNIT={journal.SUNLIT_UNIT}",
        f"_SYSTEMD_INVOCATION_ID={invocation}",
    ]


def test_journal_finalize_requires_two_distinct_complete_boots_and_ceil_math(tmp_path):
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(mode=0o700)
    evidence = evidence_dir / "boot-evidence.jsonl"
    records = [
        {
            "profile": journal.SUNLIT_PROFILE,
            "unit": journal.SUNLIT_UNIT,
            "invocation_id": "a" * 32,
            "complete_boot": True,
            "peak_30s_lines": 2,
        }
    ]
    evidence.write_text("\n".join(json.dumps(item) for item in records) + "\n")
    os.chmod(evidence, 0o600)
    override_dir = tmp_path / "override"
    override_dir.mkdir(mode=0o700)
    override = override_dir / "30-runtime-rate-limit.conf"
    with pytest.raises(RuntimeError, match="two distinct"):
        journal.finalize(evidence_path=evidence, override_path=override)

    records.append({**records[0], "invocation_id": "b" * 32, "peak_30s_lines": 3})
    evidence.write_text("\n".join(json.dumps(item) for item in records) + "\n")
    os.chmod(evidence, 0o600)
    assert journal.finalize(evidence_path=evidence, override_path=override) == 5
    assert override.read_text() == "[Journal]\nRateLimitIntervalSec=30s\nRateLimitBurst=5\n"
    assert (os.stat(override).st_mode & 0o777) == 0o600


def test_journal_rejects_malformed_and_symlinked_evidence(tmp_path):
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(mode=0o700)
    real = evidence_dir / "real.jsonl"
    real.write_text("not json\n")
    os.chmod(real, 0o600)
    symlink = evidence_dir / "boot-evidence.jsonl"
    symlink.symlink_to(real)
    with pytest.raises(RuntimeError):
        journal._load_evidence(symlink)
    symlink.unlink()
    with pytest.raises(RuntimeError, match="malformed"):
        journal._load_evidence(real)


def test_zero_suppression_requires_finalized_policy_and_no_namespace_suppression(
    monkeypatch, tmp_path
):
    invocation = "c" * 32
    base = _entries(invocation, [1_000_000, 2_000_000], complete=False)
    monkeypatch.setattr(journal, "_journal_entries", lambda value: base)
    dropin = tmp_path / "30-runtime-rate-limit.conf"
    monkeypatch.setattr(journal, "FINAL_DROPIN", dropin)
    with pytest.raises((FileNotFoundError, RuntimeError)):
        journal.verify_zero_suppression(invocation)
    dropin.write_text("[Journal]\nRateLimitIntervalSec=30s\nRateLimitBurst=5\n")
    os.chmod(dropin, 0o600)
    monkeypatch.setattr(journal, "_namespace_window_entries", lambda start, end: [])
    assert journal.verify_zero_suppression(invocation) is True
    monkeypatch.setattr(
        journal,
        "_namespace_window_entries",
        lambda start, end: [{"MESSAGE": "Suppressed 9 messages from minecraft-sunlit-cobblemon.service"}],
    )
    with pytest.raises(RuntimeError, match="suppression"):
        journal.verify_zero_suppression(invocation)


def test_journal_cli_has_closed_profile_and_invocation_arguments(monkeypatch):
    monkeypatch.setattr(journal, "_journal_entries", lambda _value: (_ for _ in ()).throw(RuntimeError("fixed test")))
    assert journal.main(["capture", "--profile", journal.SUNLIT_PROFILE, "d" * 32]) != 0
    with pytest.raises(SystemExit):
        journal.main(["capture", "--profile", "other", "d" * 32])
    with pytest.raises(ValueError):
        journal.validate_invocation_id("../../etc/passwd")


def test_journald_namespace_contract_and_fenced_relays():
    base = (ROOT / "ops/journald/horizon.conf").read_text()
    measurement = (ROOT / "ops/journald/horizon-private-measurement.conf").read_text()
    assert "Storage=persistent" in base
    assert "SystemMaxUse=1G" in base
    assert "MaxRetentionSec=14day" in base
    assert "RateLimitIntervalSec=0" not in base
    assert "RateLimitIntervalSec=0" in measurement
    assert "RateLimitBurst=0" in measurement

    bore = (ROOT / "ops/systemd/bore-minecraft-fenced.service").read_text()
    assert "ConditionPathExists=/etc/game-control/arm/bore-minecraft" in bore
    assert "stat -c %%u:%%a" in bore and "0:600" in bore
    assert "ExecStartPre=+/usr/bin/sh" in bore
    assert "EnvironmentFile=/etc/game-control/secrets.d/bore-minecraft.env" in bore
    assert "ExecStart=/usr/local/bin/bore local 25565 --local-host 127.0.0.1 --to ${BoreRemoteHost} --port 25565" in bore
    assert "User=svc-bore" in bore
    assert "OnFailure=horizon-alert-notify@bore-minecraft-fenced.service" in bore
    # A dropped or rebooted remote relay can end the client with status 0, so
    # the unit must recover unexpected exits itself while an explicit stop and
    # the arm-marker fence keep suppressing intentional shutdowns.
    assert "Restart=always" in bore
    assert "RestartSec=15" in bore
    assert "StartLimitIntervalSec=0" in bore.split("[Service]", 1)[0]
    assert "[Install]" not in bore
    assert "SECRET" not in bore.upper() or "EnvironmentFile" in bore

    terraria = (ROOT / "ops/systemd/horizon-terraria-relay.service").read_text()
    assert "ConditionPathExists=/etc/game-control/arm/horizon-terraria" in terraria
    assert "/etc/wireguard/wg-hzn-terraria.conf" in terraria
    assert "stat -c %%u:%%a" in terraria
    assert "wg-quick up /etc/wireguard/wg-hzn-terraria.conf" in terraria
    assert len("wg-hzn-terraria") <= 15
    assert "OnFailure=horizon-alert-notify@horizon-terraria-relay.service" in terraria
    assert (ROOT / "ops/systemd/horizon-terraria-relay.service").name == "horizon-terraria-relay.service"
    assert "Restart=no" in terraria
    assert "[Install]" not in terraria
