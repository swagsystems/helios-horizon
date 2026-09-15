from __future__ import annotations

import shutil
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import pytest
from playwright.sync_api import Page, sync_playwright


ROOT = Path(__file__).resolve().parents[2]


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *_args):
        pass


@pytest.fixture
def sunlit_page():
    handler = lambda *args, **kwargs: _QuietHandler(*args, directory=str(ROOT / "web"), **kwargs)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            executable_path=shutil.which("google-chrome-stable") or shutil.which("google-chrome")
        )
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        context.add_init_script("window.__HORIZON_TEST__ = {}")
        page = context.new_page()
        status = {
            "generation": 1,
            "observed_at": "2026-08-06T12:00:00Z",
            "profiles": [
                {"profile_id": "minecraft-sunlit-cobblemon", "state": "running", "health": "healthy", "slot_owner": "minecraft-sunlit-cobblemon", "players_online": 1, "required_ports_ready": True},
                {"profile_id": "terraria-vanilla", "state": "stopped", "health": "unknown", "slot_owner": None, "players_online": None, "required_ports_ready": False},
                {"profile_id": "terraria-tmod", "state": "stopped", "health": "unknown", "slot_owner": None, "players_online": None, "required_ports_ready": False},
            ],
        }
        profiles = [
            {"id": "minecraft-sunlit-cobblemon", "display_name": "Sunlit Cobblemon", "adapter": "systemd", "operations": ["start", "stop", "restart", "command", "backup"]},
            {"id": "terraria-vanilla", "display_name": "Terraria Vanilla", "adapter": "systemd", "operations": ["start", "stop", "restart", "command"]},
            {"id": "terraria-tmod", "display_name": "Terraria tModLoader", "adapter": "systemd", "operations": ["start", "stop", "restart", "command"]},
        ]

        def fulfill(route):
            request = route.request
            path = urlparse(request.url).path
            if path == "/api/v1/session":
                return route.fulfill(json={"actor": "operator", "csrf_token": "csrf", "expires_at": None})
            if path == "/api/v1/profiles":
                return route.fulfill(json=profiles)
            if path == "/api/v1/status":
                return route.fulfill(json=status)
            if path == "/api/v1/stream":
                return route.fulfill(status=200, headers={"Content-Type": "text/event-stream"}, body="event: status\ndata: {}\n\n")
            if path == "/api/v1/schedules":
                return route.fulfill(json={"schedules": []})
            if path == "/api/v1/perf":
                return route.fulfill(json={})
            if path.endswith("/logs"):
                return route.fulfill(json={"items": [{"timestamp": "2026-08-06T12:00:00Z", "severity": "info", "message": "server ready"}], "next_cursor": None})
            if path.endswith("/stats/summary"):
                return route.fulfill(json={"total_hours": 0, "unique_players": 0, "leaderboard": [], "player_tracking": "names", "occupancy": {"latest": 1, "samples": []}})
            if path.endswith("/stats/heatmap"):
                return route.fulfill(json={"buckets": [[0 for _ in range(24)] for _ in range(7)]})
            if path.endswith("/stats/tps"):
                return route.fulfill(json={"window": "24h", "samples": [{"ts": "2026-08-06T11:59:30Z", "tps": 20, "mspt": 12}], "stale": False, "state": "ok"})
            if path.endswith("/backups"):
                return route.fulfill(json={"items": [], "next_cursor": None})
            if path.endswith("/config"):
                return route.fulfill(json={"settings": []})
            return route.fulfill(json={"ok": True})

        page.route("**/api/v1/**", fulfill)
        page.goto(f"http://127.0.0.1:{server.server_port}/#/servers/minecraft-sunlit-cobblemon/console")
        page.wait_for_selector("#detail-view:not([hidden])")
        yield page
        context.close()
        browser.close()
    server.shutdown()
    thread.join(timeout=2)


def test_sunlit_console_input_output_and_tps_state_are_truthful(sunlit_page: Page):
    page = sunlit_page
    command = "say private-message-that-must-not-echo"
    assert page.locator("#command-input").is_enabled()
    assert "available" in page.locator("#command-note").inner_text().lower()
    with page.expect_request(lambda request: request.method == "POST" and request.url.endswith("/minecraft-sunlit-cobblemon/command")):
        page.locator("#command-input").fill(command)
        page.locator("#command-send").click()
    page.wait_for_function("document.querySelector('#command-input').value === ''")
    assert page.locator("#command-input").input_value() == ""
    assert command not in page.locator("#console-output").inner_text()

    page.goto(f"{page.url.split('#')[0]}#/servers/minecraft-sunlit-cobblemon/stats")
    page.wait_for_selector("#detail-view:not([hidden])")
    page.wait_for_selector("#stats-tps-current")
    assert page.locator("#stats-tps-current").inner_text() == "20.00 TPS"


def test_sunlit_start_key_retires_after_resolved_operation(sunlit_page: Page):
    page = sunlit_page
    page.wait_for_selector("#detail-view:not([hidden])")
    keys = []
    attempts = {"count": 0}

    def start_route(route):
        attempts["count"] += 1
        keys.append(route.request.headers.get("idempotency-key"))
        if attempts["count"] <= 2:
            return route.fulfill(status=503, json={"error": {"message": "upstream unavailable", "outcome_unknown": True}})
        return route.fulfill(json={"job_id": "job-new", "state": "starting"})

    page.route(
        "**/api/v1/profiles/minecraft-sunlit-cobblemon/start",
        start_route,
    )
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 2, profiles: [{profile_id: 'minecraft-sunlit-cobblemon', state: 'stopped',
            health: 'unknown', slot_owner: null, required_ports_ready: false}]
        }}))"""
    )
    page.get_by_role("button", name="Start", exact=True).wait_for()
    with page.expect_request(lambda request: request.method == "POST" and request.url.endswith("/minecraft-sunlit-cobblemon/start")):
        page.get_by_role("button", name="Start", exact=True).click()
    page.wait_for_function("document.querySelector('#session-operation').dataset.result === 'unknown'")
    first_key = keys[0]
    page.get_by_role("button", name="Start", exact=True).click()
    page.wait_for_function("document.querySelector('#session-operation').dataset.result === 'unknown'")
    assert keys[1] == first_key
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 3, profiles: [{profile_id: 'minecraft-sunlit-cobblemon', state: 'running',
            health: 'healthy', slot_owner: 'minecraft-sunlit-cobblemon', required_ports_ready: true}]
        }}))"""
    )
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 4, profiles: [{profile_id: 'minecraft-sunlit-cobblemon', state: 'stopped',
            health: 'unknown', slot_owner: null, required_ports_ready: false}]
        }}))"""
    )
    page.get_by_role("button", name="Start", exact=True).click()
    page.wait_for_function("document.querySelector('#session-operation').dataset.result === 'accepted'")
    assert attempts["count"] == 3
    assert first_key != keys[2]


def test_sunlit_legacy_start_replay_is_settled_truthfully(sunlit_page: Page):
    page = sunlit_page
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 2, profiles: [{profile_id: 'minecraft-sunlit-cobblemon', state: 'stopped',
            health: 'unknown', slot_owner: null, required_ports_ready: false}]
        }}))"""
    )
    page.evaluate(
        """() => sessionStorage.setItem(
          'horizon-operation:POST:/api/v1/profiles/minecraft-sunlit-cobblemon/start:{}',
          crypto.randomUUID())"""
    )
    posts = []
    page.route(
        "**/api/v1/status",
        lambda route: route.fulfill(json={"generation": 3, "profiles": [{
            "profile_id": "minecraft-sunlit-cobblemon", "state": "stopped",
            "health": "unknown", "slot_owner": None, "required_ports_ready": False,
        }]}),
    )
    page.route(
        "**/api/v1/profiles/minecraft-sunlit-cobblemon/start",
        lambda route: (posts.append(route.request), route.fulfill(json={"job_id": "old-job", "state": "running"}))[1],
    )
    page.get_by_role("button", name="Start", exact=True).click()
    page.wait_for_function("document.querySelector('#session-operation').dataset.result === 'resolved'")
    assert len(posts) == 1
    assert "previous Start resolved" in page.locator("#session-operation").text_content()
    assert "currently stopped" in page.locator("#session-operation").text_content()


def test_sunlit_late_response_preserves_new_operation_key(sunlit_page: Page):
    page = sunlit_page
    page.evaluate(
        """() => sessionStorage.setItem(
          'horizon-operation:POST:/api/v1/profiles/minecraft-sunlit-cobblemon/start:{}',
          crypto.randomUUID())"""
    )
    result = page.evaluate(
        """async () => {
          const keyName = 'horizon-operation:POST:/api/v1/profiles/minecraft-sunlit-cobblemon/start:{}';
          const oldKey = sessionStorage.getItem(keyName);
          const newerKey = crypto.randomUUID();
          const realFetch = window.fetch.bind(window);
          window.fetch = (input, options = {}) => {
            if (!String(input).endsWith('/minecraft-sunlit-cobblemon/start')) return realFetch(input, options);
            sessionStorage.setItem(keyName, newerKey);
            return Promise.resolve(new Response(JSON.stringify({job_id: 'old-response', state: 'starting'}), {
              status: 200, headers: {'Content-Type': 'application/json'}
            }));
          };
          await window.__horizonTest.api('/api/v1/profiles/minecraft-sunlit-cobblemon/start', {method: 'POST', body: '{}', idempotencyKey: oldKey});
          return {oldKey, newerKey, stored: sessionStorage.getItem(keyName)};
        }"""
    )
    assert result["oldKey"] != result["newerKey"]
    assert result["stored"] == result["newerKey"]


def test_sunlit_csrf_retry_keeps_key_when_storage_throws(sunlit_page: Page):
    page = sunlit_page
    keys = []
    attempts = {"count": 0}

    def start_route(route):
        attempts["count"] += 1
        keys.append(route.request.headers.get("idempotency-key"))
        if attempts["count"] == 1:
            return route.fulfill(status=403, json={"detail": "CSRF validation failed"})
        return route.fulfill(json={"job_id": "csrf-retry", "state": "starting"})

    page.route("**/api/v1/profiles/minecraft-sunlit-cobblemon/start", start_route)
    result = page.evaluate(
        """async () => {
          const original = {
            getItem: Storage.prototype.getItem,
            setItem: Storage.prototype.setItem,
            removeItem: Storage.prototype.removeItem,
          };
          Storage.prototype.getItem = () => { throw new Error('storage unavailable'); };
          Storage.prototype.setItem = () => { throw new Error('storage unavailable'); };
          Storage.prototype.removeItem = () => { throw new Error('storage unavailable'); };
          try {
            await window.__horizonTest.api('/api/v1/profiles/minecraft-sunlit-cobblemon/start', {method: 'POST', body: '{}'});
            return 'ok';
          } finally {
            Storage.prototype.getItem = original.getItem;
            Storage.prototype.setItem = original.setItem;
            Storage.prototype.removeItem = original.removeItem;
          }
        }"""
    )
    assert result == "ok"
    assert attempts["count"] == 2
    assert keys[0] is not None
    assert keys[0] == keys[1]


def test_session_recovery_bootstraps_after_restart_without_refresh_loop(sunlit_page: Page):
    page = sunlit_page
    # Replace the already-loaded session/status routes with a restart-shaped
    # sequence: the first bootstrap is a gateway failure, two concurrent API
    # requests see an expired session, and the trusted bootstrap then succeeds.
    counts = {"session": 0, "status": 0}

    def session_route(route):
        counts["session"] += 1
        if counts["session"] == 1:
            return route.fulfill(status=503, json={"detail": "web restarting"})
        if counts["session"] == 2:
            return route.fulfill(status=403, json={"detail": "bootstrap not ready"})
        return route.fulfill(json={"actor": "operator", "csrf_token": "fresh-csrf", "expires_at": None})

    def status_route(route):
        counts["status"] += 1
        if counts["status"] <= 2:
            return route.fulfill(status=401, json={"detail": "expired"})
        return route.fulfill(json={
            "generation": 9,
            "profiles": [{"profile_id": "minecraft-sunlit-cobblemon", "state": "running",
                          "health": "healthy", "slot_owner": "minecraft-sunlit-cobblemon",
                          "required_ports_ready": True}],
        })

    page.route("**/api/v1/session**", session_route)
    page.route("**/api/v1/status", status_route)
    result = page.evaluate("""async () => {
      const calls = [
        window.__horizonTest.api('/api/v1/status'),
        window.__horizonTest.api('/api/v1/status'),
      ];
      return await Promise.all(calls);
    }""")
    assert len(result) == 2
    assert counts["session"] == 3
    assert counts["status"] == 4
    state = page.evaluate("() => window.__horizonTest.sessionState()")
    assert state["expired"] is False
    assert state["expiryCount"] == 0
    assert page.locator("#conn-state").inner_text() in {"Live", "Polling", "Reconnecting…"}


def test_prolonged_gateway_failure_stays_recoverable_and_retry_loads(sunlit_page: Page):
    page = sunlit_page
    attempts = {"session": 0}

    def session_route(route):
        attempts["session"] += 1
        if attempts["session"] <= 3:
            return route.fulfill(status=503, json={"detail": "web still restarting"})
        return route.fulfill(json={"actor": "operator", "csrf_token": "recovered-csrf", "expires_at": None})

    page.route("**/api/v1/session**", session_route)
    page.evaluate("() => window.__horizonTest.load()")
    state = page.evaluate("() => window.__horizonTest.sessionState()")
    assert state["expired"] is False
    assert state["expiryCount"] == 0
    page.evaluate("() => window.__horizonTest.load()")
    page.wait_for_function("document.querySelector('#session-note').textContent.includes('Signed in')")
    assert page.evaluate("() => window.__horizonTest.sessionState().expired") is False
