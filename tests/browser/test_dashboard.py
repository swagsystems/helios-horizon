from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pytest
from playwright.sync_api import Page, expect

from browser_harness import browser_page, suspend_background_status


ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def page(web_server):
    with browser_page(
        viewport={"width": 1280, "height": 900},
        init_script="window.__HORIZON_TEST__ = { preventNavigation: true }",
    ) as page:
        status = {
            "generation": 1,
            "observed_at": "2026-07-11T12:00:00Z",
            "profiles": [
                {
                    "profile_id": "minecraft",
                    "state": "running",
                    "health": "healthy",
                    "slot_owner": "minecraft",
                    "active_job_id": None,
                    "pid": 101,
                    "started_at": "2026-07-11T10:00:00Z",
                    "uptime_seconds": 7200,
                    "cpu_percent": 7.4,
                    "rss_bytes": 128000000,
                    "players_online": 4,
                    "installed_version": "1.21.8",
                    "restart_required": False,
                    "required_ports_ready": True,
                },
                {
                    "profile_id": "pz-rising",
                    "state": "stopped",
                    "health": "unknown",
                    "slot_owner": None,
                    "active_job_id": None,
                    "pid": None,
                    "started_at": None,
                    "uptime_seconds": None,
                    "cpu_percent": None,
                    "rss_bytes": None,
                    "players_online": 0,
                    "installed_version": "42.13",
                    "restart_required": False,
                    "required_ports_ready": False,
                },
                {
                    "profile_id": "terraria-vanilla",
                    "state": "blocked",
                    "health": "unhealthy",
                    "slot_owner": None,
                    "active_job_id": None,
                    "pid": None,
                    "started_at": None,
                    "uptime_seconds": None,
                    "cpu_percent": None,
                    "rss_bytes": None,
                    "players_online": None,
                    "installed_version": "1.4.4.9",
                    "restart_required": False,
                    "required_ports_ready": False,
                },
                {
                    "profile_id": "terraria-tmod",
                    "state": "failed",
                    "health": "unhealthy",
                    "slot_owner": None,
                    "active_job_id": None,
                    "pid": None,
                    "started_at": None,
                    "uptime_seconds": None,
                    "cpu_percent": None,
                    "rss_bytes": None,
                    "players_online": None,
                    "installed_version": "2024.12",
                    "restart_required": True,
                    "required_ports_ready": False,
                },
            ],
        }
        names = [
            {"id": "minecraft", "display_name": "Minecraft", "adapter": "crafty", "operations": ["start", "stop", "restart"], "public_endpoint": {"host": "mc.example.test", "port": 25565}},
            {"id": "pz-rising", "display_name": "Project Zomboid", "adapter": "systemd", "operations": ["start", "stop", "restart"]},
            {"id": "terraria-tmod", "display_name": "Terraria tModLoader", "adapter": "systemd", "operations": ["start", "stop", "restart", "command"]},
            {"id": "terraria-vanilla", "display_name": "Terraria Vanilla", "adapter": "systemd", "operations": ["start", "stop", "restart", "command"]},
            {"id": "future-game", "display_name": "Future Game", "adapter": "systemd", "operations": ["start", "stop", "restart"]},
        ]
        latest = {"status": status}
        schedules = [
            {"cron": "0 8 * * 4", "profile": "pz-rising", "next_fire": "2026-07-16T08:00:00Z", "enabled": False},
            {"cron": "0 20 * * 5", "profile": "minecraft", "next_fire": "2026-07-17T20:00:00Z", "enabled": True, "operation": "backup", "backup_destination": "horizon-b2"},
        ]

        def fulfill(route):
            request = route.request
            path = urlparse(request.url).path
            if path == "/api/v1/session":
                return route.fulfill(json={"actor": "operator@example.test", "csrf_token": "test-token", "expires_at": None})
            if path == "/api/v1/status":
                return route.fulfill(json=latest["status"])
            if path == "/api/v1/schedules":
                if request.method == "POST":
                    schedules[:] = [{
                        **item,
                        "next_fire": "2026-07-17T20:00:00Z" if item.get("enabled", True) else None,
                    } for item in request.post_data_json["entries"]]
                return route.fulfill(json={"schedules": schedules})
            if path == "/api/v1/perf":
                return route.fulfill(json={
                    "GET /api/v1/status": {"count": 50, "p50_ms": 12.0, "p95_ms": 24.0, "max_ms": 31.0},
                    "rpc": {"count": 50, "p50_ms": 8.0, "p95_ms": 18.0, "max_ms": 22.0},
                    "sse": {"connected_clients": 1, "publish_flush_lag_ms": {"count": 20, "p50_ms": 3.0, "p95_ms": 6.0, "max_ms": 9.0}},
                    "slotd": {"cycle": {"count": 50, "avg_ms": 7.0, "p95_ms": 14.0, "max_ms": 18.0}, "rpc": {"count": 50, "avg_ms": 4.0, "p95_ms": 8.0, "max_ms": 11.0}},
                })
            if path == "/api/v1/profiles":
                return route.fulfill(json=names)
            if path == "/api/v1/audit":
                return route.fulfill(json={"items": [
                    {"id": "a5", "timestamp": "2026-07-11T12:05:00Z", "actor": "operator", "action": "backup", "profile_id": "minecraft", "result": "succeeded", "error_code": None, "detail": "completed"},
                    {"id": "a4", "timestamp": "2026-07-11T12:04:00Z", "actor": "operator", "action": "start", "profile_id": "minecraft", "result": "failed", "error_code": "start_timeout", "detail": "start timed out"},
                    {"id": "a3", "timestamp": "2026-07-11T12:03:00Z", "actor": "operator", "action": "stop", "profile_id": "minecraft", "result": "rejected", "error_code": "slot_conflict", "detail": "player fence"},
                    {"id": "a2", "timestamp": "2026-07-11T12:02:00Z", "actor": "operator", "action": "backup", "profile_id": "minecraft", "result": "failed", "error_code": "backup_failed", "detail": "backup failed"},
                    {"id": "a1", "timestamp": "2026-07-11T12:01:00Z", "actor": "operator", "action": "backup", "profile_id": "minecraft", "result": "failed", "error_code": "backup_failed", "detail": "backup failed"},
                ], "next_cursor": None})
            if path == "/api/v1/events":
                return route.fulfill(json={"items": [{"id": "e1", "timestamp": "2026-07-11T12:04:30Z", "profile_id": "minecraft", "code": "start_cleanup_succeeded", "message": "Horizon stopped the process left by a failed start."}], "next_cursor": None})
            if path.endswith("/stats/heatmap"):
                return route.fulfill(json={"buckets": [[(day + hour) % 4 for hour in range(24)] for day in range(7)]})
            if path.endswith("/stats/tps"):
                return route.fulfill(json={"samples": [{"timestamp": "2026-07-11T12:00:00Z", "tps": 20, "mspt": 12}]})
            if path == "/api/v1/stream":
                payload = json.dumps(latest["status"], separators=(",", ":"))
                return route.fulfill(
                    status=200,
                    headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"},
                    body=f"event: status\ndata: {payload}\n\n",
                )
            if path == "/api/v1/profiles/minecraft/logs":
                return route.fulfill(json={"items": [
                    {"timestamp": "2026-07-11T12:00:00Z", "severity": "info", "message": "server ready"},
                    {"timestamp": "2026-07-11T12:01:00Z", "severity": "error", "message": "redacted failure"},
                    {"timestamp": "2026-07-11T12:01:01Z", "severity": "info", "message": "Thread RCON Client /127.0.0.1 started"},
                    {"timestamp": "2026-07-11T12:01:01Z", "severity": "info", "message": "Thread RCON Client /127.0.0.1 shutting down"},
                ], "next_cursor": None})
            if path == "/api/v1/profiles/minecraft/backups":
                return route.fulfill(json={"items": [
                    {"id": "backup-1", "profile_id": "minecraft", "created_at": "2026-07-10T12:00:00Z", "size_bytes": 1073741824, "verified": True, "protected": False},
                ], "next_cursor": None})
            if path.endswith("/stats/summary"):
                profile_id = path.split("/")[4]
                if profile_id == "pz-rising":
                    return route.fulfill(json={
                        "total_hours": 0,
                        "unique_players": 0,
                        "leaderboard": [],
                        "player_tracking": "count",
                        "occupancy": {"latest": 3, "samples": []},
                    })
                return route.fulfill(json={
                    "total_hours": 1,
                    "unique_players": 1,
                    "leaderboard": [{"player": "Guest", "hours": 1, "sessions": 1, "last_seen": "2026-07-11T12:00:00Z"}],
                    "player_tracking": "names",
                    "occupancy": {"latest": 1, "samples": []},
                })
            if path.endswith("/config"):
                profile_id = path.split("/")[4]
                if request.method == "POST":
                    return route.fulfill(json={"profile_id": profile_id, "settings": [], "changed": ["motd"], "restart_required": ["motd"]})
                return route.fulfill(json={"profile_id": profile_id, "settings": [
                    {"key": "motd", "value": "Hello", "type": "str", "bounds": {"max_length": 59}, "restart_required": True},
                    {"key": "max-players", "value": 8, "type": "int", "bounds": {"min": 1, "max": 64}, "restart_required": True},
                    {"key": "pvp", "value": True, "type": "bool", "bounds": {}, "restart_required": True},
                ]})
            if "/stats/" in path:
                return route.fulfill(json={"window": "24h", "samples": [], "buckets": []})
            if path == "/api/v1/backups":
                return route.fulfill(json={"items": [
                    {"id": f"{profile['id']}-backup", "profile_id": profile["id"], "created_at": "2026-07-10T12:00:00Z", "size_bytes": 1073741824, "verified": True, "protected": False}
                    for profile in names
                ], "next_cursor": None})
            if path.endswith("/backups"):
                profile_id = path.split("/")[4]
                return route.fulfill(json={"items": [
                    {"id": f"{profile_id}-backup", "profile_id": profile_id, "created_at": "2026-07-10T12:00:00Z", "size_bytes": 1073741824, "verified": True, "protected": False},
                ], "next_cursor": None})
            return route.fulfill(json={"ok": True})

        page.route("**/api/v1/**", fulfill)
        console_errors = []
        page_errors = []
        page.on("console", lambda message: console_errors.append(message.text) if message.type == "error" else None)
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.goto(web_server)
        page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
        page._dashboard_fixture = latest  # type: ignore[attr-defined]
        yield page
        if not getattr(page, "_allow_expected_http_errors", False):
            assert console_errors == []
        assert page_errors == []


def test_dashboard_contract_and_stable_card_updates(page: Page):
    assert page.locator("main").get_by_role("heading", name="Dashboard").is_visible()
    assert page.get_by_text("current session", exact=True).is_visible()
    assert page.get_by_role("button", name="Switch server…", exact=True).is_visible()
    assert page.locator('[data-profile-id]').count() == 5
    assert page.locator("#active-slot-title").is_visible()
    assert page.locator("#active-slot-title").inner_text() == "Minecraft"
    assert page.locator("#active-slot-summary").inner_text() == "The server is healthy and its game port is ready."
    page.wait_for_function("document.querySelector('#session-automation').textContent.includes('Minecraft')")
    assert page.locator("#active-manage").get_attribute("href") == "#/servers/minecraft/console"
    assert page.locator("#active-manage").is_visible()
    assert page.get_by_text("Minecraft", exact=True).count() >= 1
    assert page.get_by_text("Project Zomboid", exact=True).count() >= 1
    assert page.get_by_text("Terraria Vanilla", exact=True).count() >= 1
    assert page.get_by_text("Terraria tModLoader", exact=True).count() >= 1
    assert page.get_by_text("Running", exact=True).is_visible()
    assert page.get_by_text("Stopped", exact=True).is_visible()
    assert page.get_by_text("Blocked", exact=True).is_visible()
    assert page.get_by_text("Failed", exact=True).is_visible()
    assert page.locator("header").count() == 0
    assert page.locator("main").count() == 1
    assert page.locator("aside.sidebar").count() == 1
    assert page.locator("aside.sidebar").evaluate("(node) => Math.round(node.getBoundingClientRect().width)") == 200
    assert page.get_by_role("navigation").get_by_text("Dashboard", exact=True).is_visible()
    assert page.get_by_role("navigation").get_by_text("SERVERS", exact=True).is_visible()
    assert page.get_by_role("navigation").get_by_text("system", exact=True).is_visible()
    assert page.get_by_text("Signed in as operator@example.test", exact=True).is_visible()
    assert page.locator('[aria-live="polite"]').count() >= 2
    assert page.evaluate("Math.min(...[...document.querySelectorAll('button')].filter((node) => node.offsetParent).map((node) => node.getBoundingClientRect().height)) >= 34")
    assert page.get_by_role("button", name="Stop Minecraft", exact=True).is_visible()
    assert page.get_by_role("button", name="Restart Minecraft", exact=True).is_visible()
    assert page.get_by_role("link", name="Manage Project Zomboid").is_visible()
    assert page.locator('[data-profile-id="minecraft"] .cpu-sparkline').is_visible()
    assert page.get_by_role("button", name="Start Project Zomboid", exact=True).is_visible()
    assert page.get_by_role("button", name="Start Terraria Vanilla", exact=True).is_visible()
    assert page.get_by_role("button", name="Start Terraria tModLoader", exact=True).is_visible()
    stopped_button = page.locator('[data-profile-id="pz-rising"] .action-stop')
    assert stopped_button.get_attribute("hidden") is not None
    blocked_start = page.get_by_role("button", name="Start Terraria Vanilla", exact=True)
    assert blocked_start.is_disabled()
    assert "Switch active server" in (blocked_start.get_attribute("title") or "")

    card = page.locator('[data-profile-id="minecraft"]')
    page.evaluate("window.__cardBefore = document.querySelector('[data-profile-id=\"minecraft\"]')")
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {
            data: {generation: 2, profiles: [{profile_id: 'minecraft', state: 'running',
            health: 'healthy', players_online: 9, cpu_percent: 12.5, rss_bytes: 128000000,
            required_ports_ready: true, restart_required: false}]}
        }))"""
    )
    assert page.evaluate("window.__cardBefore === document.querySelector('[data-profile-id=\"minecraft\"]')")
    assert card.get_by_text("9 players", exact=True).is_visible()
    assert page.locator('[data-profile-id="minecraft"] .cpu-sparkline').get_attribute("aria-label") == "CPU usage 12.5 percent"


def test_incident_fault_rail_groups_failures_and_marks_later_success(page: Page):
    page.wait_for_selector("#incident-list-compact .incident-item")
    assert page.locator("#incident-list-compact .incident-item").count() == 3
    assert page.locator("#incident-summary").inner_text() == "2 needing review · 1 resolved in the recent record"
    assert page.locator("#incident-list-compact").get_by_text("Start timed out", exact=True).is_visible()
    assert page.locator("#incident-list-compact").get_by_text("Backup failed", exact=True).is_visible()
    assert page.locator("#incident-list-compact").get_by_text("2 occurrences", exact=False).is_visible()
    assert page.locator('#incident-list-compact .incident-item[data-state="resolved"]').count() == 1

    page.get_by_role("navigation").get_by_text("Incidents", exact=True).click()
    page.wait_for_selector("#events-view:not([hidden])")
    assert page.get_by_role("heading", name="Controller failures", exact=True).is_visible()
    assert page.locator("#incident-list .incident-item").count() == 3
    assert page.locator("#events-list").get_by_text("start_cleanup_succeeded", exact=False).is_visible()
    assert page.locator("#incident-history-summary").is_visible()
    assert page.get_by_text("latest 200 audit records", exact=True).is_visible()


def test_session_deck_desktop_layout_stays_compact(page: Page, tmp_path: Path):
    page.set_viewport_size({"width": 1368, "height": 826})
    page.reload()
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
    hero = page.locator("#active-slot")
    shortcuts = page.locator(".session-shortcuts")
    assert hero.evaluate("(node) => node.getBoundingClientRect().height < 420")
    assert shortcuts.evaluate("(node) => node.getBoundingClientRect().height <= 44")
    assert shortcuts.locator("a").evaluate_all(
        "(links) => links.every((link) => link.getBoundingClientRect().width >= 60 && link.getBoundingClientRect().height <= 44)"
    )
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    page.screenshot(path=str(tmp_path / "horizon-session-desktop-1368x826.png"))

    connection_state = page.locator("#conn-state")
    connection_state.evaluate("(node) => { node.dataset.state = 'live'; node.textContent = 'Live'; }")
    assert connection_state.evaluate("(node) => node.getBoundingClientRect().width <= 1")
    connection_state.evaluate("(node) => { node.dataset.state = 'offline'; node.textContent = 'Offline'; }")
    assert connection_state.evaluate("(node) => node.getBoundingClientRect().width > 1")


@pytest.mark.parametrize("width", [320, 375, 390])
def test_phone_next_automation_label_wraps_without_clipping(page: Page, width: int):
    # The compound "operation (destination) · profile · next run" label is wider
    # than a phone column; it must wrap inside the deck instead of clipping, and
    # every part of the label must still be present.
    page.set_viewport_size({"width": width, "height": 760})
    page.reload()
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
    summary = page.locator("#session-automation")
    expect(summary).to_contain_text("Backup (Horizon B2) · Minecraft ·")
    parts = [part.strip() for part in summary.inner_text().split("·")]
    assert len(parts) == 3
    assert parts[0].startswith("Backup (Horizon B2)")
    assert parts[1] == "Minecraft"
    assert parts[2]
    assert page.evaluate(
        """() => {
          const node = document.querySelector('#session-automation');
          const box = node.getBoundingClientRect();
          const cell = node.closest('div').getBoundingClientRect();
          return node.scrollWidth <= node.clientWidth + 1
            && box.right <= cell.right + 1
            && box.right <= window.innerWidth;
        }"""
    )
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")


def test_profile_cards_follow_fallback_and_future_append_order(page: Page):
    assert page.locator("#profile-cards").locator("[data-profile-id]").evaluate_all(
        "(cards) => cards.map((card) => card.dataset.profileId)"
    ) == ["minecraft", "terraria-vanilla", "terraria-tmod", "pz-rising", "future-game"]


def test_dashboard_automation_uses_soonest_enabled_schedule(page: Page):
    summary = page.locator("#session-automation")
    # The home summary names the operation, not just the target and the time.
    assert summary.inner_text().startswith("Backup (Horizon B2) · Minecraft · ")
    assert "Project Zomboid" not in summary.inner_text()


def test_dashboard_automation_empty_state(page: Page):
    base = page.url.split("#", 1)[0]
    page.goto(f"{base}#/servers/minecraft/config")
    page.wait_for_selector("#schedule-list [data-schedule-row]")
    for _ in range(2):
        page.once("dialog", lambda dialog: dialog.accept())
        with page.expect_response(lambda response: response.request.method == "POST" and response.url.endswith("/api/v1/schedules")):
            page.locator("#schedule-list [data-schedule-remove]").first.click()
    page.goto(f"{base}#/")
    expect(page.locator("#session-automation")).to_have_text("None scheduled")


def test_dashboard_automation_rerenders_after_schedule_mutation(page: Page):
    base = page.url.split("#", 1)[0]
    page.goto(f"{base}#/servers/minecraft/config")
    toggle = page.get_by_role("switch", name="Disable schedule for Minecraft at 0 20 * * 5")
    page.once("dialog", lambda dialog: dialog.accept())
    with page.expect_response(lambda response: response.request.method == "POST" and response.url.endswith("/api/v1/schedules")):
        toggle.click()
    expect(page.locator("#session-automation")).to_have_text("None scheduled")
    page.goto(f"{base}#/")
    assert page.locator("#session-automation").inner_text() == "None scheduled"


def test_profile_cards_are_grouped_by_derived_family(page: Page):
    families = page.locator("#profile-cards > [data-profile-family]")
    assert families.evaluate_all(
        """(groups) => groups.map((group) => ({
            family: group.dataset.profileFamily,
            cards: [...group.querySelectorAll('[data-profile-id]')].map((card) => card.dataset.profileId),
            count: group.querySelector('[data-family-count]').textContent,
            owner: group.querySelector('[data-family-owner]').textContent,
            singleton: group.classList.contains('is-singleton'),
        }))"""
    ) == [
        {"family": "minecraft", "cards": ["minecraft"], "count": "1 member", "owner": "Slot: Minecraft", "singleton": True},
        {"family": "terraria", "cards": ["terraria-vanilla", "terraria-tmod"], "count": "2 members", "owner": "Slot: —", "singleton": False},
        {"family": "project-zomboid", "cards": ["pz-rising"], "count": "1 member", "owner": "Slot: —", "singleton": True},
        {"family": "other", "cards": ["future-game"], "count": "1 member", "owner": "Slot: —", "singleton": True},
    ]


def test_family_member_switch_preselects_existing_switch_dialog(page: Page):
    page.get_by_role("button", name="Switch to Terraria Vanilla", exact=True).click()
    dialog = page.get_by_role("dialog", name="Switch active server")
    assert dialog.get_by_label("Target profile", exact=True).input_value() == "terraria-vanilla"
    assert page.locator("#switch-target-summary").inner_text() == "Terraria Vanilla"
    page.keyboard.press("Escape")


def test_sentinel_version_and_null_rss_render_as_unknown_metrics(page: Page):
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 4, profiles: [{profile_id: 'minecraft', state: 'stopped',
            health: 'unknown', slot_owner: null, installed_version: 'False', rss_bytes: null}]
        }}))"""
    )
    card = page.locator('[data-profile-id="minecraft"]')
    assert card.locator(".metric-memory").inner_text() == "—"
    assert card.locator(".metric-version").inner_text() == "—"

    page.goto(f"{page.url}#/servers/minecraft/metrics")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert page.locator("#rail-memory").inner_text() == "—"
    assert page.locator("#metric-memory-current").inner_text() == "—"
    assert page.locator("#rail-version").inner_text() == "—"


def test_blocked_start_is_toasted_without_post(page: Page):
    requests = []
    page.on("request", lambda request: requests.append(request) if request.method == "POST" else None)
    button = page.get_by_role("button", name="Start Project Zomboid", exact=True)
    button.click()
    assert "Switch active server" in page.locator("#toast-region").inner_text()
    assert not any("/profiles/pz-rising/start" in request.url for request in requests)


def test_empty_slot_copy_and_owner_dot(page: Page):
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {
            data: {generation: 3, observed_at: '2026-07-11T12:05:00Z',
            profiles: [
              {profile_id: 'minecraft', state: 'stopped', health: 'unknown', slot_owner: null, cpu_percent: null},
              {profile_id: 'pz-rising', state: 'stopped', health: 'unknown', slot_owner: null, cpu_percent: null},
              {profile_id: 'terraria-vanilla', state: 'stopped', health: 'unknown', slot_owner: null, cpu_percent: null},
              {profile_id: 'terraria-tmod', state: 'stopped', health: 'unknown', slot_owner: null, cpu_percent: null}
            ]}
        }))"""
    )
    assert page.locator("#active-slot-title").inner_text() == "Minecraft"
    assert page.get_by_text("Offline. Horizon can start it now.", exact=True).is_visible()
    assert page.locator("#session-primary").inner_text() == "Start Minecraft"
    assert page.locator(".server-dot.is-owner").count() == 0


def test_confirmed_offline_profile_shows_offline_for_players_uptime_and_health(page: Page):
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 50, profiles: [{profile_id: 'minecraft', state: 'stopped', health: 'unknown',
            slot_owner: null, pid: null, players_online: 0, uptime_seconds: 0, required_ports_ready: false}]
        }}))"""
    )
    assert page.locator('[data-profile-id="minecraft"] .metric-players').inner_text() == "Offline"
    assert page.locator("#active-players").inner_text() == "Offline"
    assert page.locator("#active-uptime").inner_text() == "Offline"
    assert page.locator("#active-health").inner_text() == "Offline"
    assert page.locator("#active-slot-summary").inner_text() == "Offline. Horizon can start it now."

    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/metrics")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert "state-stopped" in page.locator("#detail-status").get_attribute("class")
    assert page.locator("#rail-players").inner_text() == "Offline"
    assert page.locator("#rail-players-note").inner_text() == "Server is offline"
    assert page.locator("#metric-players-current").inner_text() == "Offline"
    assert page.locator("#metric-uptime").inner_text() == "Offline"


def test_running_profile_with_zero_players_is_not_offline(page: Page):
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 51, profiles: [{profile_id: 'minecraft', state: 'running', health: 'healthy',
            slot_owner: 'minecraft', pid: 202, players_online: 0, uptime_seconds: 0,
            required_ports_ready: true}]
        }}))"""
    )
    assert page.locator('[data-profile-id="minecraft"] .metric-players').inner_text() == "0 players"
    assert page.locator("#active-players").inner_text() == "0"
    assert page.locator("#active-uptime").inner_text() == "0m"
    assert page.locator("#active-health").inner_text() == "Healthy"

    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/metrics")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert page.locator("#rail-players").inner_text() == "0"
    assert page.locator("#metric-players-current").inner_text() == "0"
    assert page.locator("#metric-uptime").inner_text() == "0m"
    assert page.locator("#detail-view").get_by_text("Offline", exact=True).count() == 0


@pytest.mark.parametrize("state", ["starting", "stopping", "unknown"])
def test_transition_and_unknown_states_do_not_become_offline(page: Page, state: str):
    page.evaluate(
        """state => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 52, profiles: [{profile_id: 'minecraft', state, health: 'unknown',
            slot_owner: state === 'stopping' ? 'minecraft' : null, pid: state === 'stopping' ? 202 : null,
            players_online: 0, uptime_seconds: 0, required_ports_ready: false}]
        }}))""",
        state,
    )
    assert page.locator("#active-players").inner_text() == "0"
    assert page.locator("#active-uptime").inner_text() == "0m"
    assert page.locator("#active-health").inner_text() == "Unknown"
    assert page.locator("#active-players").inner_text() != "Offline"


def test_session_deck_explains_readiness_and_recovery_states(page: Page):
    assert page.locator("#session-endpoint").inner_text() == "mc.example.test"
    assert page.get_by_role("button", name="Copy Minecraft join address", exact=True).is_visible()
    assert page.locator('#session-runway [data-state="complete"]').count() == 3
    page.wait_for_function("document.querySelector('#session-backup').textContent.includes('Verified')")
    assert page.locator("#session-backup").inner_text().startswith("Verified ")

    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 5, profiles: [{profile_id: 'minecraft', state: 'starting',
            health: 'unknown', slot_owner: 'minecraft', active_job_id: 'start', pid: null,
            players_online: null, required_ports_ready: false}]
        }}))"""
    )
    assert page.locator("#active-slot-summary").inner_text() == "Start accepted. Waiting for the server process."
    assert page.locator('[data-session-phase="process"]').get_attribute("data-state") == "active"
    assert page.locator('[data-session-phase="process"]').get_attribute("aria-current") == "step"
    assert page.locator("#session-readiness-summary").text_content() == "Readiness: 1 of 3 stages complete · starting in progress"
    assert page.locator("#active-players").inner_text() == "Not observed"

    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 6, profiles: [{profile_id: 'minecraft', state: 'starting',
            health: 'unknown', slot_owner: 'minecraft', active_job_id: 'start', pid: 202,
            players_online: null, required_ports_ready: false}]
        }}))"""
    )
    assert page.locator("#active-slot-summary").inner_text() == "The server process is loading. Waiting for the game port."
    assert page.locator('[data-session-phase="process"]').get_attribute("data-state") == "active"
    assert page.locator('[data-session-phase="port"]').count() == 0
    assert page.locator('[data-session-phase="ready"]').get_attribute("data-state") == "waiting"

    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 7, profiles: [
            {profile_id: 'minecraft', state: 'blocked', health: 'unknown', slot_owner: 'pz-rising', pid: null, required_ports_ready: false},
            {profile_id: 'pz-rising', state: 'running', health: 'healthy', slot_owner: 'pz-rising', pid: 303, players_online: 2, required_ports_ready: true}
          ]
        }}))"""
    )
    assert page.locator("#active-slot-title").inner_text() == "Project Zomboid"
    assert page.locator("#active-slot-summary").inner_text() == "The server is healthy and its game port is ready."
    assert page.locator("#active-manage").get_attribute("href") == "#/servers/pz-rising/console"
    assert page.locator("#session-primary").inner_text() == "Open console"
    assert page.locator("#active-manage").is_hidden()
    page.wait_for_function("document.querySelector('#session-backup').textContent.includes('Verified')")
    assert page.locator("#session-backup").inner_text().startswith("Verified ")


def test_session_deck_failed_state_has_explicit_safe_retry(page: Page):
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 71, profiles: [{profile_id: 'minecraft', state: 'failed', health: 'unhealthy',
            slot_owner: null, pid: null, players_online: null, required_ports_ready: false}]
        }}))"""
    )
    assert page.locator("#active-slot-summary").inner_text() == "The last operation failed. Horizon will preserve the evidence if you retry."
    assert page.locator("#session-primary").inner_text() == "Retry start"


def test_session_deck_refuses_join_copy_without_health_and_ownership(page: Page):
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 10, profiles: [{profile_id: 'minecraft', state: 'running', health: 'unhealthy',
            slot_owner: 'minecraft', pid: 202, players_online: null, required_ports_ready: true}]
        }}))"""
    )
    assert page.locator("#active-slot-summary").inner_text() == "The game port is open, but health is unhealthy."
    assert page.locator("#session-primary").inner_text() == "Diagnose health"
    assert page.locator("#session-copy-endpoint").is_disabled()
    assert page.locator('[data-session-phase="ready"]').get_attribute("data-state") == "waiting"

    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 11, profiles: [{profile_id: 'minecraft', state: 'running', health: 'healthy',
            slot_owner: null, pid: 202, players_online: null, required_ports_ready: true}]
        }}))"""
    )
    assert page.locator("#active-slot-summary").inner_text() == "The process is running without active-slot ownership. Review events before joining."
    assert page.locator("#session-primary").inner_text() == "Review ownership"
    assert page.locator("#session-copy-endpoint").is_disabled()


def test_session_deck_primary_start_uses_existing_typed_mutation(page: Page):
    page.route(
        "**/api/v1/profiles/minecraft/start",
        lambda route: route.fulfill(json={"job_id": "job-42", "state": "running"}),
    )
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 8, profiles: [{profile_id: 'minecraft', state: 'stopped', health: 'unknown', slot_owner: null,
            pid: null, players_online: null, required_ports_ready: false}]
        }}))"""
    )
    requests = []
    page.on("request", lambda request: requests.append(request) if request.method == "POST" else None)
    page.locator("#session-primary").click()
    assert any("/profiles/minecraft/start" in request.url for request in requests)
    page.wait_for_function("document.querySelector('#session-operation').dataset.result === 'accepted'")
    operation = page.locator("#session-operation").inner_text()
    assert "Start accepted by Horizon. Job job-42." in operation
    assert "Horizon" in operation
    assert "This tab notice is transient; the Audit trail is durable." not in operation


def test_session_deck_keeps_mutation_failure_visible(page: Page):
    page.evaluate(
        """() => {
          const realFetch = window.fetch.bind(window);
          window.fetch = (input, options = {}) => String(input).includes('/api/v1/profiles/minecraft/start')
            ? Promise.reject(new Error('simulated connection loss'))
            : realFetch(input, options);
        }"""
    )
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 9, profiles: [{profile_id: 'minecraft', state: 'stopped', health: 'unknown', slot_owner: null,
            pid: null, players_online: null, required_ports_ready: false}]
        }}))"""
    )
    page.locator("#session-primary").click()
    operation = page.locator("#session-operation")
    page.wait_for_function("document.querySelector('#session-operation').dataset.result === 'unknown'")
    assert operation.get_attribute("data-result") == "unknown"
    assert operation.inner_text() == "Start outcome is unknown. Horizon is reconciling the original operation; observed status will settle this notice."
    assert page.locator("#active-slot-summary").inner_text() == "Offline. Horizon can start it now."


def test_observed_completion_is_not_overwritten_by_late_post_ack(page: Page):
    page.evaluate(
        """() => {
          const realFetch = window.fetch.bind(window);
          window.__resolveStart = null;
          window.fetch = (input, options = {}) => {
            if (!String(input).includes('/api/v1/profiles/minecraft/start')) return realFetch(input, options);
            return new Promise((resolve) => {
              window.__resolveStart = () => resolve(new Response(JSON.stringify({ok: true}), {
                status: 200, headers: {'Content-Type': 'application/json'}
              }));
            });
          };
        }"""
    )
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 12, profiles: [{profile_id: 'minecraft', state: 'stopped', health: 'unknown', slot_owner: null,
            pid: null, players_online: null, required_ports_ready: false}]
        }}))"""
    )
    page.locator("#session-primary").click()
    page.wait_for_function("typeof window.__resolveStart === 'function'")
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 13, profiles: [{profile_id: 'minecraft', state: 'running', health: 'healthy', slot_owner: 'minecraft',
            pid: 202, players_online: 0, required_ports_ready: true}]
        }}))"""
    )
    assert page.locator("#session-operation").get_attribute("data-result") == "complete"
    assert page.locator("#session-operation").inner_text() == "Minecraft is ready."
    page.evaluate("window.__resolveStart()")
    page.wait_for_timeout(50)
    assert page.locator("#session-operation").get_attribute("data-result") == "complete"
    assert page.locator("#session-operation").inner_text() == "Minecraft is ready."


def test_update_check_requires_explicit_review_before_mutation(page: Page):
    posts = []

    def update_route(route):
        request = route.request
        path = urlparse(request.url).path
        if request.method == "GET":
            return route.fulfill(json={"apply_supported": True, "installed_version": "1.21.8", "available_version": "1.21.9"})
        posts.append(path)
        if path.endswith("/prepare"):
            return route.fulfill(json={"confirmation_id": "update-confirmation"})
        return route.fulfill(json={"ok": True})

    page.route("**/api/v1/profiles/minecraft/update**", update_route)
    page.route("**/api/v1/update/confirm", update_route)
    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/console")
    page.wait_for_selector("#detail-view:not([hidden])")
    page.get_by_role("button", name="Check for updates", exact=True).click()
    page.wait_for_selector("#update-dialog[open]")
    dialog = page.get_by_role("dialog", name="Apply server update")
    assert dialog.is_visible()
    assert dialog.get_by_text("1.21.8", exact=True).is_visible()
    assert dialog.get_by_text("1.21.9", exact=True).is_visible()
    assert posts == []
    dialog.get_by_role("button", name="Apply update", exact=True).click()
    page.wait_for_function("document.querySelector('#update-dialog').open === false")
    assert posts == ["/api/v1/profiles/minecraft/update/prepare", "/api/v1/update/confirm"]


def test_automatic_update_status_shows_available_without_generic_apply(page: Page):
    posts = []

    def update_route(route):
        request = route.request
        if request.method == "GET":
            return route.fulfill(json={
                "profile_id": "minecraft",
                "strategy": "manual",
                "installed_version": "1.1.3-SSV4.1.4",
                "available_version": "1.1.4-SSV4.1.5",
                "restart_required": False,
                "apply_supported": False,
                "state": "available",
                "message": None,
                "checked_at": "2026-09-13T18:00:00Z",
            })
        posts.append(urlparse(request.url).path)
        return route.fulfill(json={"ok": True})

    page.route("**/api/v1/profiles/minecraft/update**", update_route)
    page.route("**/api/v1/update/confirm", update_route)
    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/console")
    page.wait_for_selector("#detail-view:not([hidden])")
    page.get_by_role("button", name="Check for updates", exact=True).click()
    page.wait_for_selector("#update-dialog[open]")

    dialog = page.get_by_role("dialog", name="Automatic update available")
    assert dialog.get_by_text("1.1.4-SSV4.1.5", exact=True).is_visible()
    assert dialog.get_by_role("button", name="Apply update", exact=True).is_hidden()
    assert posts == []


@pytest.mark.parametrize(
    ("state", "message", "expected"),
    [
        ("failed", "Update check failed.", "Update check failed."),
        ("checking", "An update check is already in progress.", "An update check is already in progress."),
        ("deferred", "The automatic update is deferred.", "The automatic update is deferred."),
        ("stale", "The update status is stale.", "The update status is stale."),
    ],
)
def test_update_check_non_current_states_are_not_reported_as_no_update(
    page: Page, state: str, message: str, expected: str,
):
    def update_route(route):
        return route.fulfill(json={
            "profile_id": "minecraft",
            "strategy": "manual",
            "installed_version": "1.1.3-SSV4.1.4",
            "available_version": None,
            "restart_required": False,
            "apply_supported": False,
            "state": state,
            "message": message,
        })

    page.route("**/api/v1/profiles/minecraft/update**", update_route)
    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/console")
    page.wait_for_selector("#detail-view:not([hidden])")
    page.evaluate("window.__horizonTest.setUpdatePollTiming(1, 2)")
    page.get_by_role("button", name="Check for updates", exact=True).click()

    expect(page.locator("#status-announcer")).to_have_text(expected)
    assert page.locator("#update-dialog[open]").count() == 0


def test_update_check_polling_finishes_slow_discovery_without_second_click(page: Page):
    calls = 0

    def update_route(route):
        nonlocal calls
        calls += 1
        if calls == 1:
            return route.fulfill(json={
                "profile_id": "minecraft",
                "strategy": "manual",
                "installed_version": "1.1.3-SSV4.1.4",
                "available_version": None,
                "restart_required": False,
                "apply_supported": False,
                "state": "checking",
                "message": "An update check is in progress.",
            })
        return route.fulfill(json={
            "profile_id": "minecraft",
            "strategy": "manual",
            "installed_version": "1.1.3-SSV4.1.4",
            "available_version": "1.1.4-SSV4.1.5",
            "restart_required": False,
            "apply_supported": False,
            "state": "available",
            "message": None,
        })

    page.route("**/api/v1/profiles/minecraft/update**", update_route)
    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/console")
    page.wait_for_selector("#detail-view:not([hidden])")
    page.evaluate("window.__horizonTest.setUpdatePollTiming(1, 3)")
    page.get_by_role("button", name="Check for updates", exact=True).click()
    page.wait_for_selector("#update-dialog[open]")

    assert calls >= 2
    page.get_by_role("dialog", name="Automatic update available").get_by_text("1.1.4-SSV4.1.5", exact=True).is_visible()


def test_schedule_management_updates_live_automation_summary(page: Page):
    page.on("dialog", lambda dialog: dialog.accept())
    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/config")
    page.wait_for_selector("#schedule-list [data-schedule-row]")
    assert page.locator("#schedule-list [data-schedule-row]").count() == 2
    assert page.locator("#schedule-list").get_by_text("Minecraft", exact=True).is_visible()

    page.locator("#schedule-cron").fill("30 21 * * 6")
    page.locator("#schedule-profile").select_option("terraria-vanilla")
    with page.expect_response(lambda response: response.request.method == "POST" and urlparse(response.url).path == "/api/v1/schedules"):
        page.locator("#schedule-form").get_by_role("button", name="Add profile switch", exact=True).click()
    expect(page.locator("#schedule-list [data-schedule-row]")).to_have_count(3)
    assert page.locator("#schedule-cron").input_value() == ""
    assert page.evaluate("document.activeElement === document.querySelector('#schedule-cron')")

    second = page.locator('#schedule-list [data-schedule-row="2"]')
    with page.expect_response(lambda response: response.request.method == "POST" and urlparse(response.url).path == "/api/v1/schedules"):
        second.get_by_role("switch").click()
    assert second.get_attribute("data-enabled") == "false"
    assert second.get_by_text("Disabled · no next fire", exact=True).is_visible()
    assert "Minecraft" in page.locator("#session-automation").inner_text()


def test_sse_patches_dashboard_nodes_and_bounds_cpu_samples(page: Page):
    page.evaluate(
        """() => {
          window.__heroBefore = document.querySelector('#active-slot');
          window.__dotBefore = document.querySelector('[data-profile-nav="minecraft"] .server-dot');
          window.__lineBefore = document.querySelector('[data-profile-id="minecraft"] .cpu-sparkline-line');
          for (let i = 0; i < 140; i++) {
            window.dispatchEvent(new MessageEvent('game-control-status', {data: {
              generation: i + 10, observed_at: '2026-07-11T12:00:00Z',
              profiles: [{profile_id: 'minecraft', state: 'running', health: 'healthy',
                slot_owner: 'minecraft', players_online: i, cpu_percent: i % 101}]
            }}));
          }
        }"""
    )
    assert page.evaluate("window.__heroBefore === document.querySelector('#active-slot')")
    assert page.evaluate("window.__dotBefore === document.querySelector('[data-profile-nav=\"minecraft\"] .server-dot')")
    assert page.evaluate("window.__lineBefore === document.querySelector('[data-profile-id=\"minecraft\"] .cpu-sparkline-line')")
    assert page.locator('[data-profile-id="minecraft"] .metric-players').inner_text() == "139 players"
    assert page.locator('[data-profile-id="minecraft"] .cpu-sparkline-line').evaluate(
        "(node) => node.getAttribute('points').trim().split(/\\s+/).length <= 90"
    )
    assert page.locator("#last-updated").inner_text().startswith("Updated ")


def test_settings_surfaces_one_shot_performance_snapshot_and_browser_marks(page: Page):
    base = page.url.split("#", 1)[0]
    page.goto(f"{base}#/")
    page.get_by_role("button", name="Stop Minecraft", exact=True).click()
    page.evaluate(
        "() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {generation: 9, profiles: [{profile_id: 'minecraft', state: 'stopped', health: 'unknown', slot_owner: null}]}}))"
    )
    assert page.evaluate(
        "() => performance.getEntriesByName('horizon-mutation-click-to-card-reflect', 'measure').length >= 1"
    )

    page.goto(f"{base}#/settings")
    page.wait_for_selector("#performance-panel")
    assert page.get_by_role("heading", name="Performance", exact=True).is_visible()
    assert page.get_by_text("GET /api/v1/status", exact=True).is_visible()
    assert page.get_by_text("p50 12.0 ms · p95 24.0 ms · max 31.0 ms", exact=True).is_visible()
    assert page.get_by_text("Terraria", exact=False).count() >= 1
    assert page.evaluate(
        "() => performance.getEntriesByName('horizon-load-to-first-status-paint', 'measure').length >= 1"
    )


def test_mutations_reflect_transitional_state_before_server_sse(page: Page):
    page.goto(f"{page.url}#/servers/minecraft/console")
    page.wait_for_selector("#detail-view:not([hidden])")
    page.get_by_role("button", name="Stop", exact=True).click()

    assert page.locator('[data-profile-id="minecraft"] .status-text').inner_text() == "Stopping…"
    assert page.locator('[data-profile-id="minecraft"]').get_attribute("class").find("state-stopping") >= 0
    assert page.locator("#detail-status .status-text").text_content() == "Stopping…"
    assert page.locator("#active-slot").get_attribute("class").find("is-transitional") >= 0
    assert page.get_by_role("button", name="Restart", exact=True).is_hidden()
    assert page.evaluate(
        "() => performance.getEntriesByName('horizon-mutation-click-to-optimistic-reflect', 'measure').length >= 1"
    )

    page.evaluate(
        "() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {generation: 99, profiles: [{profile_id: 'minecraft', state: 'stopped', health: 'unknown', slot_owner: null}]}}))"
    )
    assert page.locator('[data-profile-id="minecraft"] .status-text').inner_text() == "Stopped"
    assert page.locator("#detail-status .status-text").text_content() == "Stopped"


def test_first_load_skeleton_contract_and_carbon_shell(page: Page):
    assert page.title() == "Helios Control"
    favicon = page.locator('link[rel="icon"]')
    assert favicon.count() == 1
    assert (favicon.get_attribute("href") or "").startswith("data:image/svg+xml")
    assert page.locator(".content-grid .eyebrow").count() == 0
    assert page.locator("#profile-cards [data-skeleton]").count() == 0


def test_skeleton_placeholders_are_declared_in_shell():
    html = (ROOT / "web" / "index.html").read_text(encoding="utf-8")
    css = (ROOT / "web" / "styles.css").read_text(encoding="utf-8")
    assert 'class="skeleton-card" data-skeleton' in html
    assert 'class="detail-skeleton" data-detail-skeleton' in html
    assert ".skeleton-block" in css


def test_stopped_profile_sparkline_is_flat_zero(page: Page):
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 300, profiles: [{profile_id: 'minecraft', state: 'stopped',
            health: 'unknown', slot_owner: null, cpu_percent: null}]
        }}))"""
    )
    points = page.locator('[data-profile-id="minecraft"] .cpu-sparkline-line').get_attribute("points")
    assert points
    assert len(points.split()) >= 2
    assert len(set(point.split(",")[1] for point in points.split())) == 1


def test_switch_confirmation_contains_required_summary_and_text(page: Page):
    opener = page.get_by_role("button", name="Switch server…")
    opener.click()
    dialog = page.get_by_role("dialog", name="Switch active server")
    assert dialog.is_visible()
    assert dialog.get_by_text("Current server").is_visible()
    assert dialog.get_by_text("Expected maximum downtime").is_visible()
    assert dialog.get_by_text("Last backup").is_visible()
    assert dialog.get_by_label("Target profile", exact=True).is_visible()
    assert dialog.get_by_label("Type the target profile name to confirm").is_visible()
    assert dialog.get_by_role("button", name="Confirm switch").is_disabled()
    page.keyboard.press("Escape")
    assert dialog.is_hidden()
    assert page.evaluate("document.activeElement === document.querySelector('#switch-active')")


@pytest.mark.parametrize("width,height", [(1280, 900), (390, 844), (375, 812)])
def test_dashboard_has_no_horizontal_overflow(page: Page, width: int, height: int):
    page.set_viewport_size({"width": width, "height": height})
    page.reload()
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    assert page.locator("main").get_by_role("heading", name="Dashboard").is_visible()


def test_log_filter_and_pause_are_preserved(page: Page):
    page.evaluate("window.__horizonOpenLogs('minecraft')")
    dialog = page.get_by_role("dialog", name="Logs for Minecraft")
    dialog.get_by_label("Severity").select_option("error")
    dialog.get_by_role("button", name="Pause live logs").click()
    page.keyboard.press("Escape")
    page.evaluate("window.__horizonOpenLogs('minecraft')")
    dialog = page.get_by_role("dialog", name="Logs for Minecraft")
    assert dialog.get_by_label("Severity").input_value() == "error"
    assert dialog.get_by_role("button", name="Resume live logs").is_visible()


def test_theme_tokens_persist_and_paper_keeps_console_dark(page: Page):
    expected = {
        "ember": {"--ink": "#e6ebed", "--surface": "#0b0d0e", "--accent": "#8fe53c"},
        "frost": {"--ink": "#eff4f8", "--surface": "#0e141b", "--accent": "#7ab5ee"},
        "moss": {"--ink": "#f0f5ee", "--surface": "#0f1410", "--accent": "#a4c97c"},
        "aurora": {"--ink": "#f2f2f8", "--surface": "#12121b", "--accent": "#b09df0"},
        "paper": {"--ink": "#20242a", "--surface": "#f3f0e9", "--accent": "#2e8f63"},
    }
    page.get_by_role("button", name="Settings").click()
    picker = page.get_by_label("Theme")
    for theme, tokens in expected.items():
        picker.select_option(theme)
        values = page.evaluate(
            """() => {
                const styles = getComputedStyle(document.documentElement);
                const consoleNode = document.querySelector('.log-list');
                return {
                    ink: styles.getPropertyValue('--ink').trim(),
                    surface: styles.getPropertyValue('--surface').trim(),
                    accent: styles.getPropertyValue('--accent').trim(),
                    console: getComputedStyle(consoleNode).backgroundColor,
                };
            }"""
        )
        assert values["ink"] == tokens["--ink"]
        assert values["surface"] == tokens["--surface"]
        assert values["accent"] == tokens["--accent"]
        assert values["console"] in {"rgb(8, 10, 11)", "#080a0b"}
    assert page.evaluate("localStorage.getItem('helios-theme')") == "paper"
    page.reload()
    assert page.evaluate("document.documentElement.dataset.theme") == "paper"


def test_command_palette_opens_with_shortcuts_and_returns_focus(page: Page):
    trigger = page.get_by_role("button", name="Command palette")
    trigger.focus()
    page.keyboard.press("Control+k")
    dialog = page.get_by_role("dialog", name="Command palette")
    assert dialog.is_visible()
    assert page.evaluate("document.activeElement === document.querySelector('#palette-search')")
    page.keyboard.press("Escape")
    assert dialog.is_hidden()
    assert page.evaluate("document.activeElement === document.querySelector('#palette-trigger')")

    page.keyboard.press("Meta+k")
    assert dialog.is_visible()
    close = page.get_by_role("button", name="Close command palette")
    close.focus()
    page.keyboard.press("Shift+Tab")
    assert page.evaluate("document.activeElement?.getAttribute('role') === 'option'")
    page.keyboard.press("Tab")
    assert page.evaluate("document.activeElement === document.querySelector('#palette-close')")
    close.click()
    assert page.evaluate("document.activeElement === document.querySelector('#palette-trigger')")


def test_command_palette_fuzzy_filters_server_tabs(page: Page):
    page.get_by_role("button", name="Command palette").click()
    search = page.get_by_role("searchbox", name="Filter commands")
    search.fill("terraria-tmod backups")
    options = page.get_by_role("option")
    assert options.count() == 1
    assert options.first.inner_text().startswith("Terraria tModLoader / Backups")


def test_command_palette_arrows_and_enter_navigate_to_server_tab(page: Page):
    page.get_by_role("button", name="Command palette").click()
    search = page.get_by_role("searchbox", name="Filter commands")
    search.fill("terraria-tmod backups")
    search.press("ArrowDown")
    search.press("Enter")
    page.wait_for_url("**/#/servers/terraria-tmod/backups")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert page.get_by_role("heading", name="Terraria tModLoader", exact=True).is_visible()
    assert page.locator("#tab-backups").get_attribute("aria-selected") == "true"


def test_command_palette_switch_uses_existing_confirmation_dialog(page: Page):
    page.get_by_role("button", name="Command palette").click()
    search = page.get_by_role("searchbox", name="Filter commands")
    search.fill("switch terraria tmodloader")
    page.get_by_role("option").first.click()
    switch = page.get_by_role("dialog", name="Switch active server")
    assert switch.is_visible()
    assert switch.get_by_label("Target profile", exact=True).input_value() == "terraria-tmod"
    assert switch.get_by_role("button", name="Confirm switch").is_disabled()
    page.keyboard.press("Escape")
    page.goto(f"{page.url.split('#')[0]}#/")
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)


def test_mobile_drawer_and_server_group_are_keyboard_accessible(page: Page):
    page.set_viewport_size({"width": 375, "height": 760})
    page.reload()
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
    burger = page.get_by_role("button", name="Open navigation")
    assert burger.is_visible()
    assert burger.get_attribute("aria-expanded") == "false"
    burger.focus()
    burger.click()
    drawer = page.locator("#sidebar")
    assert drawer.get_attribute("aria-hidden") == "false"
    expect(page.locator("#drawer-close")).to_be_focused()
    assert page.locator("#drawer-overlay").get_attribute("hidden") is None
    assert page.get_by_role("button", name="Close navigation").is_visible()
    assert page.get_by_role("button", name="Servers").get_attribute("aria-expanded") == "true"
    page.get_by_role("button", name="Servers").click()
    assert page.get_by_role("button", name="Servers").get_attribute("aria-expanded") == "false"
    assert page.locator("#server-nav").get_attribute("hidden") is not None
    page.get_by_role("button", name="Close navigation").click()
    assert drawer.get_attribute("aria-hidden") == "true"
    assert page.locator("#drawer-overlay").get_attribute("hidden") is not None
    assert page.evaluate("document.activeElement === document.querySelector('#menu-toggle')")
    burger.click()
    page.locator("#drawer-overlay").click(position={"x": 350, "y": 12})
    assert drawer.get_attribute("aria-hidden") == "true"
    page.keyboard.press("Escape")
    assert page.evaluate("document.activeElement === document.querySelector('#menu-toggle')")
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    assert page.evaluate(
        "Math.min(...[...document.querySelectorAll('button, a, select, input')].filter((node) => node.offsetParent).map((node) => node.getBoundingClientRect().height)) >= 44"
    )


def test_server_detail_route_has_tabs_and_constant_dark_console(page: Page):
    page.goto(f"{page.url}#/servers/minecraft/console")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert page.get_by_role("heading", name="Minecraft", exact=True).is_visible()
    assert page.get_by_text("Servers / Minecraft", exact=True).is_visible()
    tabs = page.get_by_role("tab")
    assert [tabs.nth(i).inner_text() for i in range(tabs.count())] == ["Console", "Metrics", "Stats", "Logs", "Backups", "Config"]
    assert page.get_by_role("tabpanel", name="Console").is_visible()
    console = page.locator("#console-output")
    assert console.is_visible()
    assert page.evaluate("getComputedStyle(document.querySelector('#console-output')).backgroundColor") in {"rgb(8, 10, 11)", "#080a0b"}
    page.get_by_role("tab", name="Console").focus()
    page.keyboard.press("ArrowRight")
    expect(page.get_by_role("tab", name="Metrics")).to_have_attribute("aria-selected", "true")
    assert page.url.endswith("#/servers/minecraft/metrics")


def test_detail_logs_pause_filter_and_backup_restore_prefill(page: Page):
    page.goto(f"{page.url}#/servers/minecraft/logs")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert page.get_by_role("tabpanel", name="Logs").is_visible()
    page.get_by_role("tabpanel", name="Logs").get_by_label("Severity", exact=True).select_option("error")
    page.get_by_role("button", name="Pause live logs").click()
    assert page.get_by_role("button", name="Resume live logs").is_visible()
    page.get_by_role("tab", name="Backups").click()
    page.wait_for_selector("#backup-list")
    restore = page.get_by_role("button", name="Restore backup-1")
    restore.click()
    assert page.get_by_label("Backup ID").input_value() == "backup-1"


def test_restore_cancel_never_posts_or_requires_confirmation(page: Page):
    posts = []
    page.on("request", lambda request: posts.append(request.url) if request.method == "POST" else None)
    page.goto(f"{page.url}#/servers/minecraft/backups")
    page.wait_for_selector("#backup-list")
    page.get_by_role("button", name="Restore backup-1").click()
    dialog = page.get_by_role("dialog", name="Restore backup")
    dialog.get_by_role("button", name="Cancel", exact=True).click()
    page.wait_for_function("document.querySelector('#restore-dialog').open === false")
    assert posts == []


def test_detail_command_unsupported_is_honest_and_config_sanitized(page: Page):
    page.goto(f"{page.url}#/servers/minecraft/console")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert "not available" in page.locator("#command-note").inner_text().lower()
    assert page.locator("#command-input").is_disabled()
    page.get_by_role("tab", name="Config").click()
    expect(page.get_by_role("heading", name="Auto-stop")).to_be_visible()
    expect(page.locator("#idle-stop-enabled")).to_be_visible()
    expect(page.locator("#idle-stop-minutes")).to_be_visible()
    page.wait_for_selector("#config-panel [data-config-key]")
    assert page.get_by_role("button", name="Apply changes", exact=True).is_disabled()
    assert not page.locator("#config-panel").inner_text().lower().find("password") >= 0


def test_config_tab_is_typed_diff_apply_and_restart_aware(page: Page):
    page.goto(f"{page.url.split('#')[0]}#/servers/minecraft/config")
    page.wait_for_selector("#config-panel [data-config-key]")
    motd = page.locator('[data-config-key="motd"]')
    motd.fill("New MOTD")
    assert "1 change" in page.locator("#config-diff").inner_text()
    page.once("dialog", lambda dialog: dialog.accept())
    with page.expect_response(lambda response: response.request.method == "POST" and response.url.endswith("/api/v1/profiles/minecraft/config")) as response_info:
        page.get_by_role("button", name="Apply changes", exact=True).click()
    assert response_info.value.json()["restart_required"] == ["motd"]
    expect(page.get_by_text("Restart required to take effect", exact=False)).to_be_visible()


def test_stats_are_game_relevant_and_omit_tick_tiles_for_non_minecraft(page: Page):
    base = page.url.split("#")[0]
    page.goto(f"{base}#/servers/minecraft/stats")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert page.get_by_role("heading", name="Server flight recorder", exact=True).is_visible()

    page.goto(f"{base}#/servers/terraria-tmod/stats")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert not page.get_by_role("heading", name="Server flight recorder", exact=True).is_visible()
    assert page.get_by_role("heading", name="Leaderboard", exact=True).is_visible()

    page.goto(f"{base}#/servers/pz-rising/stats")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert not page.get_by_role("heading", name="Server flight recorder", exact=True).is_visible()
    page.wait_for_selector("#stats-occupancy-block:not([hidden])", timeout=5000)
    assert page.get_by_role("heading", name="Occupancy", exact=True).is_visible()
    assert page.get_by_text("3 online", exact=True).is_visible()
    assert not page.get_by_role("heading", name="Leaderboard", exact=True).is_visible()


def test_stats_window_requests_preserve_one_six_and_24_hour_selection(page: Page):
    requests = []
    page.on("request", lambda request: requests.append(request.url) if "/stats/summary" in request.url else None)
    base = page.url.split("#")[0]
    page.goto(f"{base}#/servers/minecraft/stats")
    page.wait_for_selector("#stats-window")
    for value in ("1h", "6h", "24h"):
        page.locator("#stats-window").select_option(value)
        page.wait_for_timeout(50)
    hours = [urlparse(url).query for url in requests]
    assert any("hours=1" in query for query in hours)
    assert any("hours=6" in query for query in hours)
    assert any("hours=24" in query for query in hours)


def test_detail_command_hint_matches_available_running_profile(page: Page):
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 400, profiles: [{profile_id: 'terraria-vanilla', state: 'running',
            health: 'healthy', slot_owner: 'terraria-vanilla', cpu_percent: 1}]
        }}))"""
    )
    page.goto(f"{page.url}#/servers/terraria-vanilla/console")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert page.locator("#command-input").is_enabled()
    note = page.locator("#command-note").inner_text().lower()
    assert "available" in note
    assert "unavailable" not in note


def test_detail_hash_back_forward_and_sse_keep_tab_focus_and_nodes(page: Page):
    page.goto(f"{page.url}#/servers/minecraft/console")
    page.wait_for_selector("#detail-view:not([hidden])")
    page.get_by_role("tab", name="Logs").click()
    assert page.url.endswith("#/servers/minecraft/logs")
    page.get_by_role("tab", name="Logs").focus()
    page.evaluate("window.__detailBefore = document.querySelector('#detail-view'); window.__logListBefore = document.querySelector('#detail-log-list')")
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 99, profiles: [{profile_id: 'minecraft', state: 'running', health: 'healthy',
          slot_owner: 'minecraft', cpu_percent: 31.2, rss_bytes: 140000000, players_online: 8,
          installed_version: '1.21.8'}]}}))"""
    )
    assert page.evaluate("window.__detailBefore === document.querySelector('#detail-view')")
    assert page.evaluate("window.__logListBefore === document.querySelector('#detail-log-list')")
    assert page.get_by_role("tabpanel", name="Logs").is_visible()
    page.go_back()
    assert page.url.endswith("#/servers/minecraft/console")
    page.go_forward()
    assert page.url.endswith("#/servers/minecraft/logs")


def test_detail_mobile_layout_has_no_overflow(page: Page):
    page.set_viewport_size({"width": 390, "height": 844})
    page.goto(f"{page.url}#/servers/minecraft/metrics")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    assert page.get_by_role("heading", name="Live rail").is_visible()


def test_375px_layout_audit_and_mobile_evidence(page: Page, tmp_path: Path):
    screenshot_dir = tmp_path / "browser-evidence"
    screenshot_dir.mkdir()
    page.set_viewport_size({"width": 375, "height": 760})
    page.reload()
    base = page.url.split("#")[0]
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    assert page.locator(".profile-card").evaluate_all(
        "(cards) => cards.every((card) => card.getBoundingClientRect().right <= window.innerWidth)"
    )
    assert page.evaluate(
        "Math.min(...[...document.querySelectorAll('button, a, select, input')].filter((node) => node.offsetParent).map((node) => node.getBoundingClientRect().height)) >= 44"
    )
    page.wait_for_timeout(300)
    page.screenshot(path=str(tmp_path / "horizon-session-mobile-375x760.png"))
    page.goto(f"{base}#/servers/minecraft/console")
    page.wait_for_selector("#detail-view:not([hidden])")
    assert page.locator(".detail-tabs").evaluate("(node) => node.scrollWidth > node.clientWidth")
    assert page.locator("#console-output").evaluate("(node) => node.clientWidth <= node.parentElement.clientWidth")
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    page.goto(f"{base}#/servers/minecraft/stats")
    page.wait_for_selector("#detail-view:not([hidden])")
    page.wait_for_selector("#stats-heatmap .heatmap-row")
    assert page.locator("#stats-heatmap").evaluate("(node) => node.scrollWidth > node.clientWidth")
    assert page.locator(".stats-table-wrap").last.evaluate("(node) => node.scrollWidth > node.clientWidth")
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    page.screenshot(path=str(screenshot_dir / "2026-07-15-horizon-mobile-375x760.png"))

    page.set_viewport_size({"width": 768, "height": 1024})
    page.goto(f"{base}#/")
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
    assert page.locator("aside.sidebar").evaluate("(node) => Math.round(node.getBoundingClientRect().width)") == 200
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    page.screenshot(path=str(screenshot_dir / "2026-07-15-horizon-desktop-768x1024.png"))


def test_accessibility_live_regions_and_reduced_motion(page: Page):
    page.emulate_media(reduced_motion="reduce")
    page.set_viewport_size({"width": 375, "height": 760})
    page.reload()
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
    assert page.locator("#toast-region").get_attribute("aria-hidden") == "true"
    assert page.locator("#profile-cards").get_attribute("aria-live") is None
    assert page.locator("#log-list").get_attribute("aria-live") is None
    assert page.locator("#status-announcer").get_attribute("role") == "status"
    live_ids = page.locator('[aria-live="polite"]').evaluate_all("(nodes) => nodes.map((node) => node.id)")
    assert all(live_ids), f"every polite live region needs a stable id: {live_ids}"
    assert len(live_ids) == len(set(live_ids)), f"duplicate live regions announce twice: {live_ids}"
    page.locator("#active-slot").evaluate("(node) => node.classList.add('is-transitional')")
    assert page.locator("#active-slot .slot-mark").evaluate("(node) => getComputedStyle(node).animationName") == "none"
    assert page.locator(".skeleton-block").first.evaluate("(node) => getComputedStyle(node).animationName") == "none"


def test_breakpoint_crossing_resynchronizes_sidebar_state(page: Page):
    page.set_viewport_size({"width": 390, "height": 844})
    page.reload()
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
    page.get_by_role("button", name="Open navigation").click()
    page.set_viewport_size({"width": 1280, "height": 900})
    sidebar = page.locator("#sidebar")
    assert sidebar.get_attribute("aria-hidden") == "false"
    assert sidebar.get_attribute("inert") is None
    expect(page.locator("#drawer-overlay")).to_be_hidden()
    page.get_by_role("button", name="Settings").click()
    assert page.get_by_role("heading", name="Settings").is_visible()
    page.set_viewport_size({"width": 390, "height": 844})
    expect(sidebar).to_have_attribute("aria-hidden", "true")
    expect(sidebar).to_have_attribute("inert", "")
    expect(page.get_by_role("button", name="Open navigation")).to_have_attribute("aria-expanded", "false")


def test_desktop_shell_uses_internal_main_scrolling(page: Page):
    page.set_viewport_size({"width": 1440, "height": 1000})
    page.reload()
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
    assert page.locator(".app-shell").evaluate("(node) => node.getBoundingClientRect().height") == 1000
    assert page.locator("main").evaluate("(node) => node.clientHeight") == 1000
    assert page.locator("main").evaluate("(node) => node.scrollHeight > node.clientHeight")
    assert page.evaluate("document.documentElement.scrollHeight <= window.innerHeight + 1")


def test_mobile_servers_breadcrumb_has_touch_target(page: Page):
    page.set_viewport_size({"width": 390, "height": 844})
    page.goto(f"{page.url}#/servers/minecraft/metrics")
    page.wait_for_selector("#detail-view:not([hidden])")
    box = page.get_by_role("link", name="Servers", exact=True).bounding_box()
    assert box and box["width"] >= 44 and box["height"] >= 44


def test_aggregate_backups_replaces_rows_without_duplicates(page: Page):
    requests = []
    page.on("request", lambda request: requests.append(urlparse(request.url).path) if "/backups" in request.url else None)
    page.goto(f"{page.url}#/backups")
    page.wait_for_selector("#aggregate-backup-list .backup-row")
    assert page.locator("#aggregate-backup-list .backup-row").count() == 5
    page.goto(f"{page.url}#/")
    page.goto(f"{page.url}#/backups")
    page.wait_for_timeout(100)
    assert page.locator("#aggregate-backup-list .backup-row").count() == 5
    assert set(requests) == {"/api/v1/backups"}


def test_flight_recorder_stops_polling_while_document_is_hidden(page: Page):
    requests = []
    client_perf = []
    page.on("request", lambda request: requests.append(request.url) if "/stats/tps" in request.url else None)
    page.on("request", lambda request: client_perf.append(request.url) if "/perf/client" in request.url else None)
    page.goto(f"{page.url}#/servers/minecraft/stats")
    page.wait_for_selector("#stats-tps-chart")
    page.wait_for_function("() => document.querySelector('#stats-tps-title').closest('.stats-tps-block').getAttribute('aria-busy') === 'false'")
    page.evaluate("""() => {
      window.__testVisibility = 'hidden';
      Object.defineProperty(document, 'visibilityState', {configurable: true, get: () => window.__testVisibility});
      document.dispatchEvent(new Event('visibilitychange'));
    }""")
    before = len(requests)
    perf_before = len(client_perf)
    page.wait_for_timeout(4300)
    assert len(requests) == before
    assert len(client_perf) == perf_before


def test_visible_resume_pauses_mutations_until_fresh_status_arrives(page: Page):
    suspend_background_status(page)
    pending = []

    def hold_status(route):
        pending.append(route)

    page.route("**/api/v1/status", hold_status)
    page.evaluate("""() => {
      window.__testVisibility = 'hidden';
      Object.defineProperty(document, 'visibilityState', {configurable: true, get: () => window.__testVisibility});
      document.dispatchEvent(new Event('visibilitychange'));
      window.__testVisibility = 'visible';
      document.dispatchEvent(new Event('visibilitychange'));
    }""")
    page.wait_for_timeout(100)

    assert len(pending) == 1
    assert page.locator("#session-primary").is_disabled()
    assert page.locator("#session-primary").text_content() == "Checking status…"
    assert "Actions remain paused" in page.locator("#active-slot-summary").text_content()

    pending[0].fulfill(json={
        "generation": 2,
        "observed_at": "2026-07-11T12:01:00Z",
        "profiles": [{
            "profile_id": "minecraft", "state": "stopped", "health": "unknown",
            "slot_owner": None, "active_job_id": None, "pid": None,
            "started_at": None, "uptime_seconds": None, "cpu_percent": None,
            "rss_bytes": None, "players_online": 0, "installed_version": "1.21.8",
            "restart_required": False, "required_ports_ready": False,
        }],
    })
    page.wait_for_function("document.querySelector('#session-primary').textContent === 'Start Minecraft'")
    assert not page.locator("#session-primary").is_disabled()


def test_hidden_document_closes_sse_and_visible_resume_converges_once(page: Page):
    latest = {
        "generation": 30,
        "observed_at": "2026-07-11T12:00:00Z",
        "profiles": [{
            "profile_id": "minecraft", "state": "running", "health": "healthy",
            "slot_owner": "minecraft", "active_job_id": None, "pid": 101,
            "started_at": "2026-07-11T10:00:00Z", "uptime_seconds": 7200,
            "cpu_percent": 7.4, "rss_bytes": 128000000, "players_online": 4,
            "installed_version": "1.21.8", "restart_required": False,
            "required_ports_ready": True,
        }],
    }
    status_requests = []

    def status_route(route):
        status_requests.append(route.request.url)
        route.fulfill(json=latest)

    page.route("**/api/v1/status", status_route)
    page.add_init_script("""
      window.__visibility = 'visible';
      Object.defineProperty(document, 'visibilityState', {configurable: true, get: () => window.__visibility});
      window.__mockSse = {opens: 0, closes: 0, instances: []};
      window.EventSource = class MockEventSource {
        static CONNECTING = 0; static OPEN = 1; static CLOSED = 2;
        constructor(url) {
          this.url = url; this.readyState = 1; this.listeners = new Map();
          window.__mockSse.opens += 1; window.__mockSse.instances.push(this);
          queueMicrotask(() => { if (this.readyState === 1 && this.onopen) this.onopen(new Event('open')); });
        }
        addEventListener(type, callback) {
          const current = this.listeners.get(type) || []; current.push(callback); this.listeners.set(type, current);
        }
        close() { if (this.readyState !== 2) { this.readyState = 2; window.__mockSse.closes += 1; } }
        emit(type, payload) {
          if (this.readyState !== 1) return;
          const event = new MessageEvent(type, {data: JSON.stringify(payload)});
          (this.listeners.get(type) || []).forEach((callback) => callback(event));
          if (type === 'message' && this.onmessage) this.onmessage(event);
        }
      };
    """)
    page.reload()
    page.wait_for_function("window.__mockSse?.opens >= 1")
    assert page.evaluate("window.__mockSse.opens") == 1
    page.evaluate("""() => {
      window.__appliedSnapshots = 0;
      window.addEventListener('horizon:status-applied', () => { window.__appliedSnapshots += 1; });
      window.__visibility = 'hidden';
      document.dispatchEvent(new Event('visibilitychange'));
    }""")
    page.wait_for_function("window.__mockSse.closes === 1")
    assert page.evaluate("window.__mockSse.instances.filter(source => source.readyState !== EventSource.CLOSED).length") == 0
    hidden_request_count = len(status_requests)
    page.evaluate("window.__mockSse.instances[0].emit('status', {generation: 31, profiles: []})")
    page.wait_for_timeout(700)
    assert len(status_requests) == hidden_request_count
    assert page.evaluate("window.__appliedSnapshots") == 0

    latest["generation"] = 32
    latest["profiles"][0]["players_online"] = 9
    page.evaluate("window.__visibility = 'visible'; document.dispatchEvent(new Event('visibilitychange'))")
    page.wait_for_function("window.__mockSse.opens === 2")
    expect(page.locator('[data-profile-id="minecraft"]')).to_contain_text("9 players")
    assert len(status_requests) == hidden_request_count + 1
    assert page.evaluate("window.__mockSse.closes") == 1
    assert page.evaluate("window.__mockSse.instances.filter(source => source.readyState !== EventSource.CLOSED).length") == 1

    before_emit = page.evaluate("window.__appliedSnapshots")
    latest_event = {**latest, "generation": 33, "profiles": [{**latest["profiles"][0], "players_online": 10}]}
    page.evaluate("payload => window.__mockSse.instances[1].emit('status', payload)", latest_event)
    expect(page.locator('[data-profile-id="minecraft"]')).to_contain_text("10 players")
    assert page.evaluate("window.__appliedSnapshots") == before_emit + 1


def test_sse_replacements_carry_cursor_and_accept_only_newer_ids(page: Page):
    page.add_init_script("""
      window.__visibility = 'visible';
      Object.defineProperty(document, 'visibilityState', {configurable: true, get: () => window.__visibility});
      window.__mockSse = {opens: 0, closes: 0, instances: []};
      window.EventSource = class MockEventSource {
        static CONNECTING = 0; static OPEN = 1; static CLOSED = 2;
        constructor(url) {
          this.url = url; this.readyState = 1; this.listeners = new Map();
          window.__mockSse.opens += 1; window.__mockSse.instances.push(this);
          queueMicrotask(() => { if (this.readyState === 1 && this.onopen) this.onopen(new Event('open')); });
        }
        addEventListener(type, callback) {
          const current = this.listeners.get(type) || []; current.push(callback); this.listeners.set(type, current);
        }
        close() { if (this.readyState !== 2) { this.readyState = 2; window.__mockSse.closes += 1; } }
        emit(type, payload, lastEventId = '') {
          if (this.readyState !== 1) return;
          const event = new MessageEvent(type, {data: JSON.stringify(payload), lastEventId});
          (this.listeners.get(type) || []).forEach((callback) => callback(event));
          if (type === 'message' && this.onmessage) this.onmessage(event);
        }
      };
    """)
    page.reload()
    page.wait_for_function("window.__mockSse?.opens === 1")
    # Empty cursor-only events intentionally do not reset data-recovery backoff.
    page.evaluate("window.__horizonTest.setReconnectTestTiming(1)")
    page.evaluate("window.__appliedGenerations = []; window.addEventListener('horizon:status-applied', event => window.__appliedGenerations.push(event.detail.generation))")
    page.evaluate("window.__mockSse.instances[0].emit('status', {generation: 1, profiles: []}, '41')")
    page.evaluate("""() => {
      const source = window.__mockSse.instances[0];
      source.readyState = EventSource.CLOSED;
      source.onerror(new Event('error'));
    }""")
    page.wait_for_function("window.__mockSse.opens === 2", timeout=7000)
    assert page.evaluate("window.__mockSse.instances[1].url") == "/api/v1/stream?after=41"

    page.evaluate("window.__visibility = 'hidden'; document.dispatchEvent(new Event('visibilitychange'))")
    page.wait_for_function("window.__mockSse.closes === 1")
    page.evaluate("window.__visibility = 'visible'; document.dispatchEvent(new Event('visibilitychange'))")
    page.wait_for_function("window.__mockSse.opens === 3")
    assert page.evaluate("window.__mockSse.instances[2].url") == "/api/v1/stream?after=41"
    page.evaluate("window.__mockSse.instances[2].emit('status', {generation: 2, profiles: []}, '42')")
    assert page.evaluate("window.__appliedGenerations.includes(2)")
    page.evaluate("""() => {
      const source = window.__mockSse.instances[2];
      source.readyState = EventSource.CLOSED;
      source.onerror(new Event('error'));
    }""")
    page.wait_for_function("window.__mockSse.opens === 4", timeout=7000)
    assert page.evaluate("window.__mockSse.instances[3].url") == "/api/v1/stream?after=42"
    page.evaluate("""() => {
      const stale = window.__mockSse.instances[2];
      const callback = stale.listeners.get('status')[0];
      callback(new MessageEvent('status', {data: JSON.stringify({generation: 99, profiles: []}), lastEventId: '99'}));
      const source = window.__mockSse.instances[3];
      source.readyState = EventSource.CLOSED;
      source.onerror(new Event('error'));
    }""")
    page.wait_for_function("window.__mockSse.opens === 5", timeout=7000)
    assert page.evaluate("window.__mockSse.instances[4].url") == "/api/v1/stream?after=42"


def test_flight_recorder_keeps_cached_layout_and_values_on_poll_failure(page: Page):
    calls = []

    def tps(route):
        calls.append(route.request.url)
        if len(calls) == 1:
            return route.fulfill(json={"window": "6h", "resolution": "raw", "limit": 720,
                "samples": [{"ts": "2026-07-11T12:00:00Z", "state": "available", "tps": 19.75, "mspt": 14.5}],
                "stale": False, "state": "ok"})
        return route.fulfill(status=200, headers={"Content-Type": "application/json"}, body="{")

    page.route("**/api/v1/profiles/minecraft/stats/tps**", tps)
    page.goto(f"{page.url}#/servers/minecraft/stats")
    expect(page.locator("#stats-tps-current")).to_have_text("19.75 TPS")
    page.evaluate("window.__recorderNode = document.querySelector('#stats-tps-chart'); window.__recorderHeight = document.querySelector('.flight-recorder').getBoundingClientRect().height")
    page.wait_for_timeout(4300)
    expect(page.locator("#stats-tps-current")).to_have_text("19.75 TPS")
    expect(page.locator("#stats-live-state")).to_have_text("Stale")
    assert page.evaluate("window.__recorderNode === document.querySelector('#stats-tps-chart')")
    assert page.evaluate("Math.abs(window.__recorderHeight - document.querySelector('.flight-recorder').getBoundingClientRect().height) < 1")


def test_flight_recorder_preserves_null_offline_values_and_redraws_on_resize(page: Page):
    def tps(route):
        route.fulfill(json={"window": "6h", "resolution": "raw", "limit": 720,
            "samples": [{"ts": "2026-07-11T12:00:00Z", "state": "inactive", "tps": None, "mspt": None}],
            "comparisons": {"previous": {"samples": [{"ts": "bad", "state": "available", "tps": None, "mspt": None}]}},
            "stale": True, "state": "unknown"})

    page.route("**/api/v1/profiles/minecraft/stats/tps**", tps)
    page.goto(f"{page.url}#/servers/minecraft/stats")
    expect(page.locator("#stats-live-state")).to_have_text("Server stopped")
    expect(page.locator("#stats-tps-current")).to_have_text("—")
    recorder_text = page.locator("#stats-recorder-table").text_content()
    assert "inactive" in recorder_text and "0.00" not in recorder_text
    before = page.locator("#stats-tps-chart").evaluate("canvas => canvas.width")
    page.set_viewport_size({"width": 390, "height": 844})
    page.wait_for_function("before => document.querySelector('#stats-tps-chart').width !== before", arg=before)
    after = page.locator("#stats-tps-chart").evaluate("canvas => canvas.width")
    assert 280 <= after < before
    page.locator("#stats-comparison").select_option("previous")
    assert page.locator("#stats-tps-chart").is_visible()


def test_flight_recorder_exposes_real_overlays_and_three_accessible_comparisons(page: Page):
    def tps(route):
        route.fulfill(json={
            "window": "6h", "resolution": "raw", "limit": 720, "stale": False, "state": "ok",
            "samples": [
                {"ts": "2026-07-11T12:00:00Z", "state": "available", "tps": 18, "mspt": 24},
                {"ts": "2026-07-11T12:01:00Z", "state": "available", "tps": 20, "mspt": 12},
            ],
            "context": {
                "series": {"cpu_percent": [], "rss_bytes": [], "gc_pause": [
                    {"ts": "2026-07-11T12:00:00Z", "state": "available", "value": 87}
                ]},
                "jobs": [
                    {"kind": "backup", "state": "succeeded", "started_at": "2026-07-11T12:01:00Z", "ended_at": "2026-07-11T12:01:00Z"},
                    {"kind": "update", "state": "succeeded", "started_at": "2026-07-11T12:02:00Z", "ended_at": "2026-07-11T12:03:00Z"},
                    {"kind": "benchmark", "state": "succeeded", "started_at": "2026-07-11T12:04:00Z", "ended_at": "2026-07-11T12:05:00Z"},
                ],
            },
            "comparisons": {
                "yesterday": {"label": "Yesterday at the same time", "samples": [
                    {"ts": "2026-07-10T12:00:00Z", "state": "available", "tps": 17, "mspt": 28}
                ]},
                "restart": {"label": "Before the latest restart", "split_at": "2026-07-11T11:59:00Z", "samples": [
                    {"ts": "2026-07-11T11:58:00Z", "state": "available", "tps": 16, "mspt": 30}
                ]},
                "preset": {"label": "Benchmark presets", "baseline_preset": "current", "candidate_preset": "candidate",
                    "verdict": "better", "metrics": {"mspt_p95": {"baseline": 20, "candidate": 15, "unit": "ms"}}},
            },
            "time_basis": {"active_runtime_seconds": 3600, "wall_clock_seconds": 21600},
        })

    page.route("**/api/v1/profiles/minecraft/stats/tps**", tps)
    page.goto(f"{page.url}#/servers/minecraft/stats")
    page.wait_for_function("() => document.querySelector('#stats-tps-title').closest('.stats-tps-block').getAttribute('aria-busy') === 'false'")

    comparison = page.get_by_label("Compare")
    assert comparison.locator('option[value="yesterday"]').is_enabled()
    assert comparison.locator('option[value="restart"]').is_enabled()
    assert comparison.locator('option[value="preset"]').is_enabled()
    comparison.select_option("yesterday")
    expect(page.locator("#stats-comparison-note")).to_have_text("Yesterday at the same time")
    assert page.locator("#stats-tps-chart").get_attribute("data-comparison-alignment") == "plus-24h"
    assert page.locator("#stats-tps-chart").get_attribute("data-comparison-start") == str(
        int(datetime.fromisoformat("2026-07-11T12:00:00+00:00").timestamp() * 1000)
    )
    comparison.select_option("restart")
    expect(page.locator("#stats-comparison-note")).to_have_text("Before the latest restart")
    assert page.locator("#stats-tps-chart").get_attribute("data-comparison-alignment") == "wall-clock"
    assert page.locator("#stats-tps-chart").get_attribute("data-comparison-start") == str(
        int(datetime.fromisoformat("2026-07-11T11:58:00+00:00").timestamp() * 1000)
    )
    comparison.select_option("preset")
    expect(page.locator("#stats-comparison-note")).to_have_text("current 20.00 ms p95 · candidate 15.00 ms p95")
    table = page.locator("#stats-recorder-table").text_content()
    assert "GC pause" in table and "backup" in table
    assert "backup / update / benchmark windows" in page.get_by_label("Chart legend").text_content()


def test_detail_metric_history_only_changes_on_status_snapshot(page: Page):
    page.goto(f"{page.url}#/servers/minecraft/metrics")
    page.wait_for_selector("#detail-view:not([hidden])")
    page.wait_for_timeout(100)
    before = page.locator("#metric-cpu-chart .chart-line").get_attribute("points")
    page.goto(f"{page.url}#/servers/minecraft/console")
    page.wait_for_timeout(100)
    page.goto(f"{page.url}#/servers/minecraft/metrics")
    page.wait_for_timeout(100)
    assert page.locator("#metric-cpu-chart .chart-line").get_attribute("points") == before


def test_detail_log_severity_widening_refilters_all_cached_rows(page: Page):
    page.goto(f"{page.url}#/servers/minecraft/logs")
    page.wait_for_selector("#detail-view:not([hidden])")
    logs = page.get_by_role("tabpanel", name="Logs")
    logs.get_by_label("Severity", exact=True).select_option("error")
    assert logs.locator(".log-line").count() == 1
    logs.get_by_label("Severity", exact=True).select_option("all")
    assert logs.locator(".log-line").count() == 2
    assert page.locator("#detail-log-footer").inner_text() == "2 lines · 2 network-noise lines hidden · secrets redacted"


def test_hidden_detail_polling_pauses_and_visibility_return_refreshes_once(page: Page):
    log_requests = []
    page.on("request", lambda request: log_requests.append(request) if "/profiles/minecraft/logs" in request.url else None)
    page.goto(f"{page.url}#/servers/minecraft/logs")
    page.wait_for_selector("#detail-view:not([hidden])")
    page.wait_for_timeout(100)
    before_hidden = len(log_requests)
    page.evaluate(
        """() => {
          Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' });
          document.dispatchEvent(new Event('visibilitychange'));
        }"""
    )
    page.wait_for_timeout(3500)
    assert len(log_requests) == before_hidden
    page.evaluate(
        """() => {
          Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' });
          document.dispatchEvent(new Event('visibilitychange'));
        }"""
    )
    page.wait_for_timeout(300)
    assert len(log_requests) == before_hidden + 1


def test_detail_logs_timestamp_since_append_deduplicates_and_preserves_list_node(page: Page):
    responses = [
        {"items": [
            {"timestamp": "2026-07-11T12:00:00Z", "severity": "info", "message": "boot"},
            {"timestamp": "2026-07-11T12:01:00Z", "severity": "error", "message": "ready"},
        ], "next_cursor": None},
        {"items": [
            {"timestamp": "2026-07-11T12:01:00Z", "severity": "error", "message": "ready"},
            {"timestamp": "2026-07-11T12:02:00Z", "severity": "info", "message": "steady"},
        ], "next_cursor": None},
    ]
    calls = []

    def logs(route):
        calls.append(route.request.url)
        route.fulfill(json=responses[min(len(calls) - 1, 1)])

    page.route("**/api/v1/profiles/minecraft/logs**", logs)
    page.goto(f"{page.url}#/servers/minecraft/logs")
    page.wait_for_selector("#detail-log-list .log-line")
    page.evaluate("window.__detailLogListBefore = document.querySelector('#detail-log-list')")
    page.evaluate(
        "window.__detailLogRowsBefore = [...document.querySelectorAll('#detail-log-list .log-line')]"
    )
    page.wait_for_timeout(3200)
    assert len(calls) >= 2
    assert "since=2026-07-11T12%3A01%3A00Z" in calls[1] or "since=2026-07-11T12:01:00Z" in calls[1]
    assert page.evaluate("window.__detailLogListBefore === document.querySelector('#detail-log-list')")
    assert page.evaluate(
        "window.__detailLogRowsBefore.every((node, index) => node === document.querySelectorAll('#detail-log-list .log-line')[index])"
    )
    assert page.locator("#detail-log-list .log-line").count() == 3
    assert page.locator("#detail-log-list").inner_text().count("ready") == 1
    assert page.locator("#detail-log-list").inner_text().count("steady") == 1
    page.evaluate("window.__detailLogRowsAfterAppend = [...document.querySelectorAll('#detail-log-list .log-line')]")
    page.wait_for_timeout(3200)
    assert page.evaluate(
        "window.__detailLogRowsAfterAppend.every((node, index) => node === document.querySelectorAll('#detail-log-list .log-line')[index])"
    )


def test_detail_logs_buffer_trims_old_rows_and_bounds_reconciliation_cache(page: Page):
    initial = [
        {"timestamp": f"2026-07-11T12:{index // 60:02d}:{index % 60:02d}Z", "severity": "info", "message": f"line-{index}"}
        for index in range(200)
    ]
    responses = [
        {"items": initial, "next_cursor": None},
        {"items": [initial[-1],
                    {"timestamp": "2026-07-11T12:03:20Z", "severity": "info", "message": "line-200"},
                    {"timestamp": "2026-07-11T12:03:21Z", "severity": "error", "message": "line-201"}], "next_cursor": None},
    ]
    calls = []

    def logs(route):
        calls.append(route.request.url)
        route.fulfill(json=responses[min(len(calls) - 1, 1)])

    page.route("**/api/v1/profiles/minecraft/logs**", logs)
    page.goto(f"{page.url}#/servers/minecraft/logs")
    page.wait_for_selector("#detail-log-list .log-line")
    page.evaluate("window.__retainedLogRow = document.querySelectorAll('#detail-log-list .log-line')[2]")
    page.wait_for_timeout(3200)
    assert len(calls) >= 2
    assert page.locator("#detail-log-list .log-line").count() == 200
    assert "line-0" not in page.locator("#detail-log-list").inner_text()
    assert "line-2" in page.locator("#detail-log-list").inner_text()
    assert "line-201" in page.locator("#detail-log-list").inner_text()
    assert page.evaluate("window.__retainedLogRow === document.querySelectorAll('#detail-log-list .log-line')[0]")
    page.locator("#detail-log-severity").select_option("error")
    page.locator("#detail-log-severity").select_option("all")
    assert page.evaluate("window.__retainedLogRow === document.querySelectorAll('#detail-log-list .log-line')[0]")
    assert page.locator("#detail-log-list .log-line").count() == 200


def test_tps_above_20_and_mspt_render_without_display_clamp(page: Page):
    def tps(route):
        route.fulfill(json={"window": "24h", "samples": [
            {"ts": "2026-07-11T12:00:00Z", "tps": 24.5, "mspt": 37.25},
            {"ts": "2026-07-11T12:00:30Z", "tps": 22.0, "mspt": 41.0},
        ], "stale": False, "state": "ok"})

    page.route("**/api/v1/profiles/minecraft/stats/tps**", tps)
    page.goto(f"{page.url}#/servers/minecraft/stats")
    page.wait_for_selector("#detail-view:not([hidden])")
    expect(page.locator("#stats-tps-current")).to_have_text("22.00 TPS")
    expect(page.locator("#stats-mspt-current")).to_have_text("41.00 ms/tick")
    assert page.locator("#stats-tps-chart").evaluate("canvas => canvas.width >= canvas.clientWidth && canvas.height >= canvas.clientHeight")
    assert "Server flight recorder" in page.locator("#stats-tps-chart").get_attribute("aria-label")
    assert page.locator(".chart-legend-tps").inner_text() == "TPS"
    assert page.locator(".chart-legend-mspt").inner_text() == "MSPT"
    expect(page.locator("#stats-recorder-table tr")).to_have_count(2)
    assert "24.50" in page.locator("#stats-recorder-table").text_content()


def test_tps_empty_window_renders_explicit_stale_latest_observation(page: Page):
    def tps(route):
        route.fulfill(json={
            "window": "24h",
            "resolution": "raw",
            "samples": [],
            "latest_ts": "2026-07-09T12:00:00Z",
            "latest_observation": {
                "ts": "2026-07-09T12:00:00Z",
                "tps": 19.25,
                "mspt": 18.5,
                "stale": True,
                "staleness_seconds": 172800,
            },
            "stale": True,
            "state": "unknown",
        })

    page.route("**/api/v1/profiles/minecraft/stats/tps**", tps)
    page.goto(f"{page.url}#/servers/minecraft/stats")

    expect(page.locator("#stats-live-state")).to_have_text("Stale")
    expect(page.locator("#stats-tps-current")).to_have_text("19.25 TPS")
    expect(page.locator("#stats-mspt-current")).to_have_text("18.50 ms/tick")
    assert "Last observation:" in page.locator("#stats-tps-note").inner_text()
    expect(page.locator("#stats-recorder-table tr")).to_have_count(1)
    # The empty state names the selected window instead of a bare "this window".
    expect(page.locator("#stats-recorder-table td")).to_have_text(
        "No telemetry samples are recorded in the selected window (last 6 hours)."
    )


def test_malformed_and_unknown_server_hashes_fall_back_to_dashboard(page: Page):
    page.goto(f"{page.url}#/servers/%zz/console")
    page.wait_for_selector("#dashboard-view:not([hidden])")
    assert page.get_by_role("heading", name="Dashboard").is_visible()
    page.goto(f"{page.url}#/servers/not-a-profile/console")
    page.wait_for_selector("#dashboard-view:not([hidden])")
    assert page.get_by_role("heading", name="Dashboard").is_visible()


def test_session_refresh_is_single_flight_and_preserves_mutation_key(page: Page):
    suspend_background_status(page)
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    counts = {"session": 0, "status": 0, "mutation": 0}

    def session(route):
        counts["session"] += 1
        route.fulfill(json={"actor": "operator@example.test", "csrf_token": "refreshed-token", "expires_at": None})

    def status(route):
        counts["status"] += 1
        if counts["status"] <= 5:
            route.fulfill(status=401, json={"error": {"message": "session changed"}})
        else:
            route.fulfill(json=page._dashboard_fixture["status"])  # type: ignore[attr-defined]

    mutation_keys = []

    def mutation(route):
        counts["mutation"] += 1
        mutation_keys.append(route.request.headers.get("idempotency-key"))
        if counts["mutation"] == 1:
            route.fulfill(status=401, json={"error": {"message": "session changed"}})
        else:
            route.fulfill(json={"job_id": "job-refresh"})

    page.route("**/api/v1/session", session)
    page.route("**/api/v1/status", status)
    page.route("**/api/v1/profiles/minecraft/start", mutation)
    result = page.evaluate("""async () => {
        const calls = await Promise.all(Array.from({length: 5}, () => window.__horizonTest.api('/api/v1/status')));
        await window.__horizonTest.api('/api/v1/profiles/minecraft/start', {
            method: 'POST', body: '{}'
        });
        return calls.length;
    }""")
    assert result == 5
    assert counts["session"] == 2  # one shared refresh for the five reads, one for the mutation
    assert counts["status"] == 10
    assert counts["mutation"] == 2
    assert mutation_keys[0] == mutation_keys[1]


def test_typed_non_csrf_403_does_not_probe_or_expire_session(page: Page):
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    session_probes = []

    def session(route):
        session_probes.append(route.request.url)
        route.fulfill(json={"actor": "operator@example.test", "csrf_token": "test-token", "expires_at": None})

    def forbidden(route):
        route.fulfill(status=403, json={"error": {"message": "origin is not allowed"}})

    page.route("**/api/v1/session", session)
    page.route("**/api/v1/status", forbidden)
    result = page.evaluate("""async () => {
        try { await window.__horizonTest.api('/api/v1/status'); return 'unexpected-success'; }
        catch (error) { return error.message; }
    }""")
    assert result == "origin is not allowed"
    assert session_probes == []
    assert page.evaluate("window.__horizonTest.sessionState()") == {
        "expired": False, "refreshing": False, "noticeShown": False, "expiryCount": 0, "generation": 1
    }


def test_concurrent_opaque_redirects_have_one_expiry_transition(page: Page):
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    suspend_background_status(page)
    redirects = 0

    def redirect(route):
        nonlocal redirects
        redirects += 1
        route.fulfill(status=302, headers={"Location": "/"})

    page.route("**/api/v1/status", redirect)
    result = page.evaluate("""async () => {
        const calls = await Promise.allSettled(Array.from({length: 5}, () =>
            window.__horizonTest.api('/api/v1/status')));
        return calls.map(item => item.status);
    }""")
    assert result == ["rejected"] * 5
    assert redirects == 5
    assert page.evaluate("window.__horizonTest.sessionState()") == {
        "expired": True, "refreshing": False, "noticeShown": True, "expiryCount": 1, "generation": 1
    }


def test_transient_session_refresh_failure_schedules_next_reconnect(page: Page):
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    probes = 0

    def session(route):
        nonlocal probes
        probes += 1
        if probes == 1:
            route.fulfill(status=503, json={"detail": "temporarily unavailable"})
        else:
            route.fulfill(json={"actor": "operator@example.test", "csrf_token": "test-token", "expires_at": None})

    page.route("**/api/v1/session", session)
    page.evaluate("window.__horizonTest.setReconnectTestTiming(1)")
    page.evaluate("window.__horizonTest.scheduleReconnect()")
    page.wait_for_function("window.__horizonTest.sessionState().generation >= 2")
    assert probes >= 2


def test_late_old_generation_401_retries_without_second_session_refresh(page: Page):
    suspend_background_status(page)
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    session_probes = 0
    status_calls = 0

    def session(route):
        nonlocal session_probes
        session_probes += 1
        route.fulfill(json={"actor": "operator@example.test", "csrf_token": "new-token", "expires_at": None})
    def gated_status(route):
        nonlocal status_calls
        status_calls += 1
        if status_calls <= 2:
            route.fulfill(status=401, json={"error": {"message": "refresh me"}})
        else:
            route.fulfill(json=page._dashboard_fixture["status"])  # type: ignore[attr-defined]

    page.route("**/api/v1/session", session)
    page.route("**/api/v1/status", gated_status)
    page.evaluate("window.__horizonTest.delayNextApiResponse()")
    result = page.evaluate("""async () => Promise.allSettled([
        window.__horizonTest.api('/api/v1/status'),
        window.__horizonTest.api('/api/v1/status'),
    ]).then(items => items.map(item => item.status))""")
    assert result == ["fulfilled", "fulfilled"]
    assert session_probes == 1
    assert status_calls >= 4


def test_terminal_401_after_refresh_retries_once_then_expires(page: Page):
    suspend_background_status(page)
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    session_probes = 0
    status_calls = 0

    def session(route):
        nonlocal session_probes
        session_probes += 1
        route.fulfill(json={"actor": "operator@example.test", "csrf_token": "new-token", "expires_at": None})

    def status(route):
        nonlocal status_calls
        status_calls += 1
        route.fulfill(status=401, json={"error": {"message": "still unauthorized"}})

    page.route("**/api/v1/session", session)
    page.route("**/api/v1/status", status)
    result = page.evaluate("""async () => {
        try { await window.__horizonTest.api('/api/v1/status'); return 'unexpected-success'; }
        catch (error) { return error.message; }
    }""")
    assert result == "Session expired. Redirecting to sign in."
    assert status_calls == 2
    assert session_probes == 1
    assert page.evaluate("window.__horizonTest.sessionState()")['expiryCount'] == 1


def test_api_401_with_transient_session_failure_does_not_expire_and_recovers(page: Page):
    suspend_background_status(page)
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    session_probes = 0
    status_calls = 0

    def session(route):
        nonlocal session_probes
        session_probes += 1
        if session_probes == 1:
            route.fulfill(status=503, json={"detail": "temporarily unavailable"})
        else:
            route.fulfill(json={"actor": "operator@example.test", "csrf_token": "recovered-token", "expires_at": None})

    def status(route):
        nonlocal status_calls
        status_calls += 1
        if status_calls == 1:
            route.fulfill(status=401, json={"error": {"message": "stale token"}})
        else:
            route.fulfill(json=page._dashboard_fixture["status"])  # type: ignore[attr-defined]

    page.route("**/api/v1/session", session)
    page.route("**/api/v1/status", status)
    first = page.evaluate("""async () => {
        try { await window.__horizonTest.api('/api/v1/status'); return 'unexpected-success'; }
        catch (error) { return error.message; }
    }""")
    assert first == "unexpected-success"
    assert page.evaluate("window.__horizonTest.sessionState()")['expired'] is False
    assert session_probes == 2
    assert status_calls == 2
