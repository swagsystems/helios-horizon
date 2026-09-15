"""Focused checks for the finite bounded capture runner (no network, no secrets)."""

import argparse
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools" / "acceptance"))
import stall_capture  # noqa: E402
import stall_diagnostics  # noqa: E402

PROXY_TOKEN = "test-proxy-token-value"
BEARER_TOKEN = "test-bearer-token-value"


class _Response:
    def __init__(self, body: bytes):
        self._body = body

    def read(self, size=None):
        return self._body if size is None else self._body[:size]

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _Opener:
    def __init__(self, body=b'{"slotd":{}}', error=None):
        self.body = body
        self.error = error
        self.requests = []
        self.timeouts = []

    def open(self, request, timeout=None):
        self.requests.append(request)
        self.timeouts.append(timeout)
        if self.error is not None:
            raise self.error
        return _Response(self.body)


def _args(tmp_path, **overrides) -> argparse.Namespace:
    values = {
        "url": "https://horizon.invalid/api/v1/perf",
        "proxy_token_file": None,
        "actor": None,
        "token_file": None,
        "origin": None,
        "duration": "3",
        "interval": "1",
        "timeout": "5",
        "output": str(tmp_path / "captures.jsonl"),
        "state": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _write(tmp_path, name: str, value: str) -> str:
    path = tmp_path / name
    path.write_text(value, encoding="utf-8")
    return str(path)


def _clock(values, tail=None):
    queue = list(values)
    last = tail if tail is not None else (queue[-1] if queue else 0.0)

    def tick():
        return queue.pop(0) if queue else last

    return tick


def _perf_payload(instance: str, items: list[dict], start: int, end: int, dropped: int = 0) -> bytes:
    return json.dumps({"slotd": {"rpc_events": {
        "instance": instance, "sequence": {"start": start, "end": end},
        "capacity": 256, "dropped": dropped, "rejected": 0, "items": items}}}).encode()


def _event(sequence: int, action: str = "status") -> dict:
    return {"sequence": sequence, "action": action, "duration_ms": 1.0,
            "ended_at": "2026-09-15T00:00:00Z",
            "monotonic_start": 100.0 + sequence, "monotonic_end": 101.0 + sequence}


def test_proxy_mode_sends_the_private_header_and_actor_and_no_bearer():
    request = stall_capture.build_request(
        "https://horizon.invalid/api/v1/perf", proxy_token=PROXY_TOKEN,
        actor="verify-deployed", bearer_token=None, origin="https://horizon.invalid")

    assert request.get_header("X-game-control-proxy") == PROXY_TOKEN
    assert request.get_header("X-authentik-username") == "verify-deployed"
    assert request.get_header("Authorization") is None
    assert request.get_header("Origin") == "https://horizon.invalid"


def test_bearer_mode_is_separate_and_mutually_exclusive_with_proxy_mode(tmp_path):
    request = stall_capture.build_request("https://horizon.invalid/api/v1/perf",
                                          proxy_token=None, actor=None,
                                          bearer_token=BEARER_TOKEN, origin=None)
    assert request.get_header("Authorization") == f"Bearer {BEARER_TOKEN}"
    assert request.get_header("X-game-control-proxy") is None

    proxy_file = _write(tmp_path, "proxy.token", PROXY_TOKEN)
    with pytest.raises(SystemExit) as error:
        stall_capture.main(["--url", "https://horizon.invalid/api/v1/perf",
                            "--proxy-token-file", proxy_file, "--actor", "verify-deployed",
                            "--token-file", _write(tmp_path, "bearer.token", BEARER_TOKEN),
                            "--duration", "5", "--output", str(tmp_path / "out.jsonl")])
    assert error.value.code == 2


def test_credentials_attach_a_no_redirect_opener(monkeypatch):
    seen = {}

    def fake_build_opener(*handlers):
        seen["handlers"] = handlers
        return _Opener()

    monkeypatch.setattr(stall_capture.urllib.request, "build_opener", fake_build_opener)
    stall_capture.capture("https://horizon.invalid/api/v1/perf", proxy_token=PROXY_TOKEN,
                          actor="verify-deployed", timeout=1.0)
    assert any(isinstance(handler, stall_capture._NoRedirect) for handler in seen["handlers"])

    stall_capture.capture("https://horizon.invalid/api/v1/perf", timeout=1.0)
    assert seen["handlers"] == ()

    with pytest.raises(stall_capture.RedirectRefused):
        stall_capture._NoRedirect().redirect_request(None, None, 302, "Found", {}, "https://elsewhere.invalid")


def test_response_body_cap_and_timeout_are_enforced():
    oversized = _Opener(body=b"x" * (stall_capture.MAX_RESPONSE_BYTES + 1))
    with pytest.raises(ValueError, match="bounded body cap"):
        stall_capture.capture("https://horizon.invalid/api/v1/perf", bearer_token=BEARER_TOKEN,
                              timeout=2.5, opener=oversized)
    assert oversized.timeouts == [2.5]


def test_run_is_deadline_and_capture_bounded_and_flushes_each_capture(tmp_path):
    opener = _Opener()
    ticks = iter([index * 0.1 for index in range(1, 200)])
    summary = stall_capture.run(_args(tmp_path, duration="3", interval="1",
                                      output=str(tmp_path / "first.jsonl")),
                                opener=opener, clock=lambda: next(ticks), sleep=lambda _s: None)
    lines = (tmp_path / "first.jsonl").read_text(encoding="utf-8").strip().splitlines()

    assert summary["captures"] == 4 == len(lines)  # int(duration // interval) + 1
    assert summary["stop_reason"] == "capture_cap" and summary["elapsed_seconds"] >= 0
    assert summary["requested_seconds"] == 3.0 and summary["byte_cap"] == stall_capture.MAX_OUTPUT_BYTES
    assert len(opener.requests) == 4
    assert all(json.loads(line)["report"]["schema"] == "stall-diagnostics/2" for line in lines)

    fast = _Opener()
    jumps = iter([0.0] + [50.0] * 10)
    expired = stall_capture.run(_args(tmp_path, duration="1", interval="1",
                                      output=str(tmp_path / "second.jsonl")),
                                opener=fast, clock=lambda: next(jumps), sleep=lambda _s: None)
    assert expired["captures"] == 0 and fast.requests == []
    assert expired["stop_reason"] == "deadline"


def test_run_counts_failures_without_leaking_the_token_and_saves_state(tmp_path):
    opener = _Opener(error=stall_capture.RedirectRefused("redirect refused: 302"))
    state = str(tmp_path / "state.json")
    summary = stall_capture.run(
        _args(tmp_path, proxy_token_file=_write(tmp_path, "proxy.token", PROXY_TOKEN),
              actor="verify-deployed", duration="2", interval="1", state=state),
        opener=opener, clock=lambda: 0.0, sleep=lambda _s: None)
    contents = (tmp_path / "captures.jsonl").read_text(encoding="utf-8")

    assert summary["captures"] == 0 and summary["errors"] == 3
    assert PROXY_TOKEN not in contents and "Authorization" not in contents
    assert json.loads(contents.splitlines()[0])["error"] == "RedirectRefused"


def test_failures_exit_nonzero_and_malformed_bounds_are_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(stall_capture, "run", lambda _args: {"captures": 0, "errors": 5,
                                                             "duration_seconds": 5.0,
                                                             "interval_seconds": 1.0,
                                                             "stop_reason": "deadline"})
    base = ["--url", "https://horizon.invalid/api/v1/perf", "--duration", "5",
            "--output", str(tmp_path / "out.jsonl")]
    assert stall_capture.main(base) == 1
    monkeypatch.undo()
    # Malformed bounds are rejected before any request is attempted.
    for bad in ("nan", "inf", "abc", "0", "99999"):
        assert stall_capture.main(["--url", "https://horizon.invalid/api/v1/perf",
                                   "--duration", bad, "--output", str(tmp_path / "out.jsonl")]) == 2
    for name in ("interval", "timeout"):
        assert stall_capture.main(base + [f"--{name}", "nan"]) == 2


def test_token_and_state_paths_fail_closed(tmp_path):
    with pytest.raises(ValueError, match="bounded cap"):
        stall_capture.read_token(_write(tmp_path, "big.token", "x" * (stall_capture.MAX_TOKEN_BYTES + 1)),
                                 header="X-Game-Control-Proxy")
    with pytest.raises(ValueError, match="not readable"):
        stall_capture.read_token(str(tmp_path / "missing.token"), header="X-Game-Control-Proxy")
    assert stall_capture.read_token("", header="X-Game-Control-Proxy") is None

    broker = _Opener()
    # A missing cursor is a legitimate first capture, not a failure.
    jumps = iter([0.0] + [50.0] * 10)
    first = stall_capture.run(_args(tmp_path, duration="1", state=str(tmp_path / "missing.json")),
                              opener=broker, clock=lambda: next(jumps), sleep=lambda _s: None)
    assert first["captures"] == 0
    assert broker.requests == []

    (tmp_path / "state.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(ValueError):
        stall_capture.run(_args(tmp_path, state=str(tmp_path / "state.json")),
                          opener=broker, clock=lambda: 0.0, sleep=lambda _s: None)


def test_two_successful_captures_resume_the_persisted_per_stream_cursor(tmp_path):
    """Finding 1 regression: a real capture run must consume its own cursor file."""

    class _Sequenced(_Opener):
        def __init__(self, bodies):
            super().__init__()
            self._bodies = list(bodies)

        def open(self, request, timeout=None):
            self.requests.append(request)
            self.timeouts.append(timeout)
            return _Response(self._bodies.pop(0))

    output = str(tmp_path / "run.jsonl")
    state = str(tmp_path / "cursor.json")
    first_body = _perf_payload("inst-a", [_event(1)], 1, 2)
    jump_body = _perf_payload("inst-a", [_event(100), _event(101), _event(102)], 100, 103, dropped=97)
    one_capture = [0.0, 0.1, 0.2, 50.0, 50.0]

    first = stall_capture.run(_args(tmp_path, duration="1", interval="1", output=output, state=state),
                              opener=_Sequenced([first_body]),
                              clock=_clock(one_capture), sleep=lambda _s: None)
    saved = json.loads(Path(state).read_text(encoding="utf-8"))
    second = stall_capture.run(_args(tmp_path, duration="1", interval="1", output=output, state=state,
                                    append=True),
                               opener=_Sequenced([jump_body]),
                               clock=_clock(one_capture), sleep=lambda _s: None)

    reports = [json.loads(line)["report"] for line in
               Path(output).read_text(encoding="utf-8").strip().splitlines()]
    first_rpc = next(entry for entry in reports[0]["loss"] if entry["stream"] == "rpc")
    second_rpc = next(entry for entry in reports[1]["loss"] if entry["stream"] == "rpc")

    assert first["captures"] == 1 and second["captures"] == 1
    assert saved["rpc"] == {"instance": "inst-a", "next_sequence": 2}
    assert first_rpc["baseline_capture"] is True
    # Same instance, ring jumped to 100: exactly 98 records were evicted unread.
    assert second_rpc["instance_reset"] is False and second_rpc["baseline_capture"] is False
    assert second_rpc["evicted_since_last_capture"] == 98
    assert second_rpc["unobserved_records"] == 98 and second_rpc["next_sequence"] == 103


def test_instance_change_and_empty_first_stream_are_explicit(tmp_path):
    cursor = {"rpc": {"instance": "inst-a", "next_sequence": 2},
              "event_loop_lag": {"instance": "inst-a", "next_sequence": 1},
              "maintenance": {"instance": "inst-a", "next_sequence": 1}}
    payload = {"slotd": {
        "rpc_events": {"instance": "inst-b", "sequence": {"start": 1, "end": 3},
                       "capacity": 256, "dropped": 0, "items": [_event(1), _event(2)]},
        "event_loop_lag_events": {"instance": "inst-b", "sequence": {"start": 1, "end": 1},
                                  "capacity": 256, "dropped": 0, "items": []}}}
    report = stall_diagnostics.summarize(payload, state=cursor)
    streams = {entry["stream"]: entry for entry in report["loss"]}

    assert streams["rpc"]["instance_reset"] is True
    assert streams["rpc"]["unobserved_records"] is None  # unknowable across a restart
    assert streams["rpc"]["evicted_since_last_capture"] == 0
    assert streams["event_loop_lag"]["instance_reset"] is True
    # A cursor for a stream whose window is absent reports a reset, not a fake zero gap.
    assert streams["maintenance"]["instance_reset"] is True
    assert streams["maintenance"]["unobserved_records"] is None

    fresh = stall_diagnostics.summarize({"slotd": {"rpc_events": {
        "instance": "inst-c", "sequence": {"start": 1, "end": 1}, "capacity": 256, "items": []}}})
    entry = next(item for item in fresh["loss"] if item["stream"] == "rpc")
    assert entry["baseline_capture"] is True and entry["unobserved_records"] is None
    assert entry["next_sequence"] == 1 and entry["evicted_since_last_capture"] == 0
    assert "consumed" not in entry


def test_invalid_state_shapes_fail_closed_without_traceback(tmp_path):
    for bad in ("[1, 2]", '"scalar"', "{}", '{"rpc": []}',
                '{"rpc": {"instance": 5, "next_sequence": 1}}',
                '{"rpc": {"instance": "a", "next_sequence": true}}',
                '{"rpc": {"instance": "a", "next_sequence": 0}}',
                '{"rpc": {"instance": "a", "next_sequence": 1, "extra": 1}}',
                '{"unknown": {"instance": "a", "next_sequence": 1}}'):
        path = _write(tmp_path, "bad.json", bad)
        with pytest.raises(ValueError):
            stall_diagnostics._load_state(path)

    oversized = _write(tmp_path, "big.json", '{"rpc": {"instance": "' + "a" * (stall_diagnostics.MAX_STATE_BYTES) + '"}}')
    with pytest.raises(ValueError, match="bounded cap"):
        stall_diagnostics._load_state(oversized)

    assert stall_diagnostics._load_state(str(tmp_path / "absent.json")) is None
    assert stall_diagnostics._load_state(None) is None


def test_window_above_the_capture_cap_is_refused_before_any_file_write(tmp_path):
    output = tmp_path / "never.jsonl"

    with pytest.raises(ValueError, match="above the 2880 cap"):
        stall_capture.run(_args(tmp_path, duration="21600", interval="1", output=str(output)),
                          opener=_Opener(), clock=lambda: 0.0, sleep=lambda _s: None)
    assert not output.exists()

    # A valid combination still records an honest stop reason and elapsed time.
    ok = stall_capture.run(_args(tmp_path, duration="3", interval="1", output=str(tmp_path / "ok.jsonl")),
                           opener=_Opener(), clock=_clock([0.0, 0.1, 0.2, 50.0, 4.0]),
                           sleep=lambda _s: None)
    assert ok["stop_reason"] == "deadline" and ok["elapsed_seconds"] == 4.0
    assert ok["captures"] == 1


def test_byte_cap_and_exclusive_output_do_not_overwrite_or_advance_the_cursor(tmp_path, monkeypatch):
    monkeypatch.setattr(stall_capture, "MAX_OUTPUT_BYTES", 16)
    state = str(tmp_path / "cursor.json")
    capped = stall_capture.run(_args(tmp_path, duration="3", interval="1", state=state,
                                     output=str(tmp_path / "capped.jsonl")),
                               opener=_Opener(body=_perf_payload("inst-a", [_event(1)], 1, 2)),
                               clock=_clock([0.0, 0.1, 0.2, 50.0]), sleep=lambda _s: None)
    assert capped["captures"] == 0 and capped["stop_reason"] == "byte_cap"
    assert not Path(state).exists()  # no cursor advance without a durable line
    assert Path(tmp_path / "capped.jsonl").read_text(encoding="utf-8") == ""

    monkeypatch.setattr(stall_capture, "MAX_OUTPUT_BYTES", 64 * 1024 * 1024)
    existing = tmp_path / "exists.jsonl"
    existing.write_text('{"prior": true}\n', encoding="utf-8")
    with pytest.raises(OSError):
        stall_capture.run(_args(tmp_path, duration="1", interval="1", output=str(existing)),
                          opener=_Opener(), clock=lambda: 0.0, sleep=lambda _s: None)
    assert existing.read_text(encoding="utf-8") == '{"prior": true}\n'

    existing.chmod(0o600)
    resumed = stall_capture.run(_args(tmp_path, duration="1", interval="1", output=str(existing),
                                      append=True),
                                opener=_Opener(), clock=_clock([0.0, 0.1, 0.2, 50.0]),
                                sleep=lambda _s: None)
    assert resumed["captures"] == 1 and existing.read_text(encoding="utf-8").startswith('{"prior": true}')
    # New and explicitly resumed output both require private mode 0600.
    assert (Path(tmp_path / "capped.jsonl").stat().st_mode & 0o777) == 0o600


def test_error_records_obey_the_same_byte_cap(tmp_path, monkeypatch):
    monkeypatch.setattr(stall_capture, "MAX_OUTPUT_BYTES", 100)
    clock_value = [0.0]

    def sleep(seconds):
        clock_value[0] += seconds

    result = stall_capture.run(_args(tmp_path), opener=_Opener(error=TimeoutError()),
                               clock=lambda: clock_value[0], sleep=sleep)
    actual_size = Path(_args(tmp_path).output).stat().st_size
    assert result["stop_reason"] == "byte_cap"
    assert result["bytes_written"] == actual_size <= 100
    assert result["captures"] == 0


@pytest.mark.parametrize("kind", ["public", "hardlink", "fifo"])
def test_append_refuses_nonprivate_or_nonregular_outputs(tmp_path, kind):
    target = tmp_path / "target.jsonl"
    if kind == "fifo":
        os.mkfifo(target, 0o600)
    else:
        target.write_bytes(b"original")
        target.chmod(0o644 if kind == "public" else 0o600)
        if kind == "hardlink":
            os.link(target, tmp_path / "alias.jsonl")
    with pytest.raises((ValueError, OSError)):
        stall_capture._open_output(str(target), append=True)
    if kind != "fifo":
        assert target.read_bytes() == b"original"


def test_in_memory_cursor_advances_without_a_state_file(tmp_path):
    class Sequenced(_Opener):
        def open(self, request, timeout=None):
            body = (_perf_payload("same", [_event(1)], 1, 2) if not self.requests
                    else _perf_payload("same", [_event(100)], 100, 101))
            self.requests.append(request)
            return _Response(body)

    now = [0.0]

    def sleep(seconds):
        now[0] += seconds

    args = _args(tmp_path, duration="2")
    result = stall_capture.run(args, opener=Sequenced(), clock=lambda: now[0], sleep=sleep)
    reports = [json.loads(line)["report"] for line in Path(args.output).read_text().splitlines()]
    loss = next(item for item in reports[1]["loss"] if item["stream"] == "rpc")
    assert result["captures"] == 2
    assert loss["unobserved_records"] == 98
    assert not loss["instance_reset"]
