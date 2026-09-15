import json
import shutil
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

from game_control.protocol import GetPerf, PerfSnapshot, RpcSuccess, StatusSnapshot
from game_control.perf import PerformanceTracker
from game_control.schedule import parse_schedule
from game_control.web_main import BoundedTimingRing, EventHub, create_app


HEADERS = {
    "X-Game-Control-Proxy": "secret",
    "X-authentik-username": "operator",
}


def test_phase_zero_web_client_uses_visibility_gated_incremental_paths():
    app = (Path(__file__).resolve().parents[1] / "web" / "app.js").read_text()

    assert "const pageVisible = () => document.visibilityState === \"visible\";" in app
    assert "if (!pageVisible()) return;" in app
    assert "cursor" in app, "cursor-based log polling must remain explicit"
    assert "params.set(\"cursor\", item.nextCursor)" in app
    assert "consoleOutput.replaceChildren();" in app
    assert "item?.consoleKey !== consoleKey" in app
    assert "Math.min(20, latest.tps)" not in app
    assert "drawFlightRecorder" in app
    assert 'state.detail.statsTimer = window.setInterval' in app
    visibility = app[app.index('document.addEventListener("visibilitychange"'):]
    assert 'if (document.visibilityState !== "visible") {' in visibility
    assert "state.detail.statsAbort?.abort();" in visibility
    assert "suspendStream();" in visibility
    assert "resumeStream();" in visibility
    assert "function suspendStream()" in app
    assert "stream.suspended = true;" in app
    assert "window.clearTimeout(stream.reconnectTimer)" in app
    assert "window.clearInterval(stream.watchdog)" in app
    assert "stopFallbackPolling();" in app
    assert "if (source) source.close();" in app
    assert "function resumeStream()" in app
    assert 'applyStatus(await api("/api/v1/status"), { confirmed: true })' in app
    assert "if (!state.statusConfirmed)" in app
    assert "generation < state.lastGeneration" in app
    assert "if (!pageVisible() || stream.suspended) return;" in app
    assert "stream.source !== source" in app
    assert "function refreshVisiblePanels()" in app
    assert "refreshVisiblePanels();" in app
    assert "if (!pageVisible() || !id || state.detail.tab !== \"stats\") return;" in app
    assert "if (!pageVisible() || !id) return;" in app
    assert "if (item.loading) return;" in app
    assert "summary?hours=${hours}" in app
    assert "heatmap?hours=${hours}" in app
    assert "Offline time is shaded, not plotted as zero." in app
    assert "resolution=${encodeURIComponent(resolution)}&limit=720" in app


def _schedule_payload_probe(items: list[dict]) -> list[dict]:
    """Run the shipped ``schedulePayload`` in node against real view objects."""
    app = (Path(__file__).resolve().parents[1] / "web" / "app.js").read_text()
    start = app.index("function schedulePayload(item) {")
    end = app.index("\n}\n", start) + len("\n}\n")
    source = f"""
{app[start:end]}
const items = {json.dumps(items)};
console.log(JSON.stringify(items.map((item) => schedulePayload(item))));
"""
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is required for the schedule payload probe")
    result = subprocess.run([node, "-e", source], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def test_schedule_ui_preserves_operation_and_policy_fields():
    """Whole-book replacement must re-emit every stored field unchanged.

    The exact payload is asserted (no coercion, no injected defaults and no
    inference), and the emitted entries are then fed to the controller's own
    parser so policy coverage is proven behaviourally, not by string matching.
    """
    view = [
        {
            "cron": "10 3 * * *",
            "profile": "minecraft",
            "next_fire": "2026-09-16T03:10:00Z",
            "enabled": True,
            "operation": "backup",
            "backup_destination": "horizon-b2",
        },
        {
            "cron": "20 3 * * *",
            "profile": "terraria-vanilla",
            "next_fire": "2026-09-16T03:20:00Z",
            "enabled": False,
            "operation": "switch",
        },
        {
            "cron": "40 3 * * *",
            "profile": "terraria-tmod",
            "next_fire": None,
            "enabled": False,
            "operation": "benchmark",
            "baseline_preset": "baseline",
            "candidate_preset": "candidate",
            "campaign": "weekly",
            "maintenance_window": True,
            "rollback_safe": True,
            "public_wake_policy": "safe",
        },
    ]
    assert _schedule_payload_probe(view) == [
        {
            "cron": "10 3 * * *",
            "profile": "minecraft",
            "enabled": True,
            "operation": "backup",
            "backup_destination": "horizon-b2",
        },
        {
            "cron": "20 3 * * *",
            "profile": "terraria-vanilla",
            "enabled": False,
            "operation": "switch",
        },
        {
            "cron": "40 3 * * *",
            "profile": "terraria-tmod",
            "enabled": False,
            "operation": "benchmark",
            "baseline_preset": "baseline",
            "candidate_preset": "candidate",
            "campaign": "weekly",
            "maintenance_window": True,
            "rollback_safe": True,
            "public_wake_policy": "safe",
        },
    ]

    # A legacy view omits ``operation``; nothing is invented for it, and the
    # controller infers the same operation it inferred before the edit.
    legacy = [{"cron": "0 20 * * 5", "profile": "minecraft", "next_fire": "2026-09-18T20:00:00Z", "enabled": True}]
    assert _schedule_payload_probe(legacy) == [
        {"cron": "0 20 * * 5", "profile": "minecraft", "enabled": True},
    ]

    parsed = parse_schedule(_schedule_payload_probe(view))
    assert parsed[0].operation == "backup" and parsed[0].backup_destination.value == "horizon-b2"
    assert parsed[1].operation == "switch" and parsed[1].enabled is False
    assert parsed[2].operation == "benchmark"
    assert parsed[2].maintenance_window is True
    assert parsed[2].rollback_safe is True
    assert parsed[2].public_wake_policy == "safe"
    assert parsed[2].campaign == "weekly"
    assert parsed[2].baseline_preset == "baseline" and parsed[2].candidate_preset == "candidate"
    assert parse_schedule(_schedule_payload_probe(legacy))[0].operation == "switch"


def test_session_expiry_is_single_flight_and_stops_reconnect_without_misclassifying_403():
    app = (Path(__file__).resolve().parents[1] / "web" / "app.js").read_text()

    assert "let sessionRefreshPromise = null;" in app
    assert "if (sessionRefreshPromise) return sessionRefreshPromise;" in app
    assert "const SESSION_EXPIRED_MESSAGE = \"Session expired. Redirecting to sign in.\";" in app
    assert "let sessionNoticeShown = false;" in app
    assert "if (message === SESSION_EXPIRED_MESSAGE && sessionNoticeShown) return;" in app
    assert "function expireSession()" in app
    assert "if (sessionExpired) return;" in app
    assert "if (sessionExpired) throw new Error(SESSION_EXPIRED_MESSAGE);" in app
    assert "suspendStream();" in app
    assert "window.setTimeout(() => window.location.assign(\"/\"), 0);" in app
    assert "if (response.status === 401 && path === \"/api/v1/session\")" in app
    assert "if (response.status === 401) {\n    expireSession();" in app
    assert "if (sessionExpired) return;\n    connectStream();" in app
    assert "if (!sessionExpired) {\n        setConnState(\"reconnecting\");\n        scheduleReconnect();\n      }" in app

    # A normal typed 403 is still handled by the response error path; only
    # CSRF-specific 403s enter the shared refresh flow.
    csrf_gate = app.index("if (response.status === 403)")
    refresh_gate = app.index("if ((response.status === 401 || csrfFailure)")
    assert csrf_gate < refresh_gate
    assert "csrfFailure = String(detail).toLowerCase().includes(\"csrf validation failed\")" in app
    assert "const typed = body?.error?.message || body?.detail;" in app
    assert "if (typeof typed === \"string\" && typed.trim()) detail = typed.slice(0, 300);" in app
    assert "if (!pageVisible() || stream.suspended || sessionExpired) return;" in app
    assert "if (!pageVisible() || sessionExpired) return;" in app


def test_watch_resync_is_authoritative_and_duplicate_safe():
    source = (Path(__file__).resolve().parents[1] / "src/game_control/web_main.py").read_text()
    assert 'if kind == "full_resync":' in source
    assert 'GetStatus(kind="get_status", refresh=True)' in source
    assert 'if sequence <= cursor or frame_generation < generation:' in source
    assert "watch_connected.clear()" in source


def test_bounded_timing_ring_keeps_only_recent_samples_and_computes_percentiles():
    ring = BoundedTimingRing(maxlen=3)
    for value in (1.0, 2.0, 3.0, 4.0):
        ring.record(value)

    stats = ring.snapshot()

    assert stats["count"] == 3
    assert stats["p50_ms"] == pytest.approx(3.0)
    assert stats["p95_ms"] == pytest.approx(3.9)
    assert stats["max_ms"] == pytest.approx(4.0)


def test_event_loop_snapshot_has_bounded_monotonic_sequence_window():
    tracker = PerformanceTracker(maxlen=3)
    for value in range(16_385):
        tracker.record_event_loop_lag(value)
    snapshot = tracker.snapshot()
    assert len(snapshot["event_loop_lag_ms"]) == 3
    assert snapshot["event_loop_lag_ms"][0] == 16_382.0
    assert snapshot["event_loop_lag_sequence"] == {"start": 16_382, "end": 16_385}


@pytest.mark.asyncio
async def test_event_hub_reports_connected_clients_and_publish_flush_lag():
    hub = EventHub(max_history=4)
    client = await hub.subscribe()

    item = await hub.publish("status", {"generation": 1})
    queued = await client.queue.get()
    assert queued["id"] == item["id"]
    hub.record_flush(queued)

    stats = hub.perf_snapshot()
    assert stats["connected_clients"] == 1
    assert stats["publish_flush_lag_ms"]["count"] == 1


def test_authenticated_perf_endpoint_reports_route_and_rpc_timing():
    async def rpc(_actor, _action):
        if isinstance(_action, GetPerf):
            return RpcSuccess(
                request_id=uuid4(),
                result=PerfSnapshot(
                    cycle={"count": 1, "avg_ms": 2.0, "p95_ms": 2.0, "max_ms": 2.0},
                    rpc={"count": 1, "avg_ms": 1.0, "p95_ms": 1.0, "max_ms": 1.0},
                ),
            )
        return RpcSuccess(
            request_id=uuid4(),
            result=StatusSnapshot(
                generation=0,
                observed_at="2026-01-01T00:00:00Z",
                profiles=(),
            ),
        )

    client = TestClient(
        create_app(
            rpc=rpc,
            proxy_credential="secret",
            session_db=":memory:",
            web_root=Path(__file__).resolve().parents[1] / "web",
        )
    )

    assert client.get("/api/v1/perf").status_code in {401, 403}
    session = client.get("/api/v1/session", headers=HEADERS)
    assert session.status_code == 200
    response = client.get("/api/v1/status", headers=HEADERS)
    assert response.status_code == 200

    perf = client.get("/api/v1/perf", headers=HEADERS)

    assert perf.status_code == 200
    body = perf.json()
    assert body["GET /api/v1/status"]["count"] >= 1
    assert body["rpc"]["count"] >= 1
    assert body["sse"]["connected_clients"] == 0
    assert body["slotd"]["cycle"]["p95_ms"] == 2.0


def test_client_performance_summary_is_authenticated_bounded_and_identity_free():
    async def rpc(_actor, action):
        if isinstance(action, GetPerf):
            empty = {"count": 0, "avg_ms": None, "p95_ms": None, "max_ms": None}
            return RpcSuccess(request_id=uuid4(), result=PerfSnapshot(cycle=empty, rpc=empty))
        return RpcSuccess(request_id=uuid4(), result=StatusSnapshot(generation=0, profiles=()))

    app = create_app(rpc=rpc, proxy_credential="secret", session_db=":memory:",
                     allowed_origins={"https://games.example.com"})
    with TestClient(app, base_url="https://games.example.com") as client:
        assert client.post("/api/v1/perf/client", json={"samples": [
            {"metric": "stats_fetch", "duration_ms": 1},
        ]}).status_code in {401, 403}
        session = client.get("/api/v1/session", headers=HEADERS)
        csrf = session.json()["csrf_token"]
        mutation_headers = {**HEADERS, "X-CSRF-Token": csrf, "Origin": "https://games.example.com"}
        response = client.post("/api/v1/perf/client", headers=mutation_headers, json={"samples": [
            {"metric": "recorder_draw", "duration_ms": 4.25},
            {"metric": "stats_fetch", "duration_ms": 18.5},
        ]})
        assert response.status_code == 200 and response.json() == {"accepted": 2}
        assert client.post("/api/v1/perf/client", headers=mutation_headers, json={"samples": [
            {"metric": "recorder_draw", "duration_ms": 1, "session": "forbidden"},
        ]}).status_code == 422
        assert client.post("/api/v1/perf/client", headers=mutation_headers, json={"samples": [
            {"metric": "raw_url", "duration_ms": 1},
        ]}).status_code == 422
        assert client.post("/api/v1/perf/client", headers=mutation_headers, json={"samples": [
            {"metric": "stats_fetch", "duration_ms": 1} for _ in range(17)
        ]}).status_code == 422
        assert client.post(
            "/api/v1/perf/client", headers=mutation_headers,
            content=b"{" + (b" " * 4096) + b"}",
        ).status_code == 413
        for _ in range(59):
            assert client.post("/api/v1/perf/client", headers=mutation_headers, json={"samples": [
                {"metric": "first_status_paint", "duration_ms": 1},
            ]}).status_code == 200
        assert client.post("/api/v1/perf/client", headers=mutation_headers, json={"samples": [
            {"metric": "first_status_paint", "duration_ms": 1},
        ]}).status_code == 429
        snapshot = client.get("/api/v1/perf", headers=HEADERS).json()
        assert snapshot["client"]["recorder_draw"]["count"] == 1
        assert set(snapshot["client"]) == {"stats_fetch", "recorder_draw", "first_status_paint"}


def test_stream_delivers_cached_snapshot_without_per_subscriber_controller_read(monkeypatch):
    async def disconnected(_request):
        return True

    monkeypatch.setattr(Request, "is_disconnected", disconnected)
    hub = EventHub()
    import asyncio

    asyncio.run(hub.publish("status", {"generation": 41, "profiles": []}))
    calls = []

    async def rpc(_actor, action):
        calls.append(action)
        raise AssertionError("cached stream subscriber must not issue GetStatus")

    client = TestClient(create_app(rpc=rpc, hub=hub, proxy_credential="secret", session_db=":memory:"))
    headers = {"X-Game-Control-Proxy": "secret", "X-Authentik-Username": "operator"}
    with client.stream("GET", "/api/v1/stream", headers=headers) as response:
        body = b"".join(response.iter_bytes())

    assert response.status_code == 200
    assert b'"generation":41' in body
    assert calls == []
