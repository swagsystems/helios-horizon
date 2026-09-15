"""Schedule usability and unconfigured-profile rendering contracts.

These tests cover the accepted findings from the 2026-09-15 first-use review:
the schedule section names its operation instead of calling every entry a
"switch", cron has an explicit timezone plus a draft next-run preview, stored
schedule fields survive an unrelated edit, fallback profile placeholders have
no lifecycle controls, and the schedule editor is reachable from Settings and
the command palette.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import pytest
from playwright.sync_api import Page, expect

from browser_harness import browser_page
from game_control.schedule import parse_schedule


CONFIGURED = ["minecraft-sunlit-cobblemon", "terraria-vanilla", "terraria-tmod"]

BACKUP_HORIZON = {
    "cron": "10 3 * * *",
    "profile": "minecraft-sunlit-cobblemon",
    "next_fire": "2026-09-16T03:10:00Z",
    "enabled": True,
    "operation": "backup",
    "backup_destination": "horizon-b2",
}
BACKUP_LOCAL = {
    "cron": "20 3 * * *",
    "profile": "terraria-vanilla",
    "next_fire": "2026-09-16T03:20:00Z",
    "enabled": True,
    "operation": "backup",
    "backup_destination": "local",
}
BENCHMARK_ENTRY = {
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
}


def _status(profiles):
    return {
        "generation": 1,
        "observed_at": "2026-09-15T12:00:00Z",
        "profiles": [
            {
                "profile_id": profile_id,
                "state": "stopped",
                "health": "unknown",
                "slot_owner": None,
                "active_job_id": None,
                "pid": None,
                "started_at": None,
                "uptime_seconds": None,
                "cpu_percent": None,
                "rss_bytes": None,
                "players_online": None,
                "installed_version": "1.1.4",
                "restart_required": False,
                "required_ports_ready": False,
            }
            for profile_id in profiles
        ],
    }


def _names():
    return [
        {"id": "minecraft-sunlit-cobblemon", "display_name": "Sunlit Cobblemon", "adapter": "crafty", "operations": ["start", "stop", "restart"]},
        {"id": "terraria-vanilla", "display_name": "Terraria Vanilla", "adapter": "systemd", "operations": ["start", "stop", "restart", "command"]},
        {"id": "terraria-tmod", "display_name": "Terraria tModLoader", "adapter": "systemd", "operations": ["start", "stop", "restart", "command", "benchmark"]},
    ]


class _Api:
    """Stateful fake controller API for the schedule surface."""

    def __init__(self, *, schedules=None, names=None, fail_always=False):
        self.schedules = [dict(entry) for entry in (schedules if schedules is not None else [BACKUP_HORIZON, BACKUP_LOCAL, BENCHMARK_ENTRY])]
        self.names = names if names is not None else _names()
        self.posted = []
        self.mutations = []
        self.fail_always = fail_always
        self.failing = fail_always

    def close(self):
        self.failing = True

    def status_payload(self):
        ids = [item.get("id") for item in self.names if isinstance(item, dict) and item.get("id")]
        return _status(ids)

    def handle(self, route):
        request = route.request
        path = urlparse(request.url).path
        if request.method != "GET":
            self.mutations.append(f"{request.method} {path}")
        if path == "/api/v1/session":
            return route.fulfill(json={"actor": "operator@example.test", "csrf_token": "test-token", "expires_at": None})
        if self.failing and path in {"/api/v1/status", "/api/v1/profiles"}:
            return route.fulfill(status=503, json={"error": {"message": "upstream unavailable", "retryable": True}})
        if path == "/api/v1/profiles":
            return route.fulfill(json=self.names)
        if path == "/api/v1/status":
            return route.fulfill(json=self.status_payload())
        if path == "/api/v1/schedules":
            if request.method == "POST":
                entries = request.post_data_json["entries"]
                self.posted.append(entries)
                self.schedules = [
                    {**item, "next_fire": "2026-09-16T03:10:00Z" if item.get("enabled", True) else None}
                    for item in entries
                ]
            return route.fulfill(json={"schedules": self.schedules})
        if path == "/api/v1/perf":
            return route.fulfill(json={})
        if path == "/api/v1/stream":
            payload = json.dumps(self.status_payload(), separators=(",", ":"))
            return route.fulfill(status=200, headers={"Content-Type": "text/event-stream"}, body=f"event: status\ndata: {payload}\n\n")
        if path.endswith("/config"):
            return route.fulfill(json={"profile_id": "minecraft-sunlit-cobblemon", "settings": []})
        if path.endswith("/notifications"):
            return route.fulfill(json={"rules": {}})
        return route.fulfill(json={"ok": True})


@pytest.fixture
def schedules_page(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}, init_script="window.__HORIZON_TEST__ = {}") as page:
        api = _Api()
        page.route("**/api/v1/**", api.handle)
        page.goto(f"{web_server}#/servers/minecraft-sunlit-cobblemon/config")
        page.wait_for_selector("#schedule-list [data-schedule-row]")
        page.on("dialog", lambda dialog: dialog.accept())
        page.fake_api = api  # type: ignore[attr-defined]
        yield page


def test_schedule_rows_and_home_summary_name_the_operation(schedules_page: Page):
    page = schedules_page
    assert page.get_by_role("heading", name="Scheduled automations", exact=True).is_visible()
    expect(page.locator("#schedule-list [data-schedule-row]")).to_have_count(3)

    first = page.locator('[data-schedule-row="0"]')
    assert first.locator("code").inner_text() == "10 3 * * *"
    assert first.locator(".schedule-operation").inner_text() == "Backup (Horizon B2)"
    assert first.locator(".schedule-target").inner_text() == "Sunlit Cobblemon"
    assert first.get_attribute("data-schedule-operation") == "backup"
    # Next run is stated in UTC and in the browser's local timezone.
    next_text = first.locator(".schedule-state").inner_text()
    assert next_text.startswith("next: ")
    assert "UTC" in next_text
    assert page.evaluate("Intl.DateTimeFormat().resolvedOptions().timeZone") in next_text

    second = page.locator('[data-schedule-row="1"]')
    assert second.locator(".schedule-operation").inner_text() == "Backup (Local disk)"
    third = page.locator('[data-schedule-row="2"]')
    assert third.locator(".schedule-operation").inner_text() == "Benchmark (weekly)"
    assert third.locator(".schedule-target").inner_text() == "Terraria tModLoader"
    assert third.locator(".schedule-state").inner_text() == "Disabled · no next fire"

    timezone_note = page.locator("#schedule-timezone").inner_text()
    assert "UTC" in timezone_note
    assert page.evaluate("Intl.DateTimeFormat().resolvedOptions().timeZone") in timezone_note

    page.goto(f"{page.url.split('#', 1)[0]}#/")
    summary = page.locator("#session-automation")
    expect(summary).to_contain_text("Backup (Horizon B2) · Sunlit Cobblemon · ")


def test_draft_preview_matches_the_controller_cron_matching(schedules_page: Page):
    page = schedules_page
    cases = [
        ("10 3 * * *", "2026-09-15T12:00:00Z"),
        ("30 23 * * 5", "2026-09-15T12:00:00Z"),
        ("*/15 * * * *", "2026-09-15T23:52:30Z"),
        ("0 3 1-7 * 1", "2026-09-15T12:00:00Z"),
        ("5,35 2-4 * * 0,6", "2026-09-15T12:00:00Z"),
        ("0 0 29 2 *", "2026-09-15T12:00:00Z"),
        ("0 0 29 2 *", "2027-03-01T00:00:00Z"),
        ("0 12 13 * 5", "2026-09-15T12:00:00Z"),
        ("0 0 1 * 1", "2026-09-15T12:00:00Z"),
        ("0 20 * * 5", "2026-09-15T23:00:00Z"),
        # Exact minute boundary: an unelapsed matching minute is the next run,
        # and one second later it rolls to the following occurrence.
        ("10 3 * * *", "2026-09-16T03:10:00Z"),
        ("10 3 * * *", "2026-09-16T03:10:01Z"),
        ("10 3 * * *", "2026-09-16T03:09:59Z"),
    ]
    for expression, iso_from in cases:
        from_dt = datetime.fromisoformat(iso_from.replace("Z", "+00:00"))
        expected_dt = parse_schedule([{"cron": expression, "profile": "minecraft"}])[0].next_fire(from_dt)
        expected = expected_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        actual = page.evaluate("([cron, from]) => window.__horizonTest.cronPreview(cron, from)", [expression, iso_from])
        assert actual == expected, expression

    cron = page.get_by_label("Cron", exact=True)
    cron.fill("10 3 * * *")
    preview = page.locator("#schedule-preview").inner_text()
    assert preview.startswith("Next run ")
    assert "UTC" in preview
    assert page.evaluate("Intl.DateTimeFormat().resolvedOptions().timeZone") in preview
    assert "3:10" in preview

    # Unsupported or impossible expressions say so instead of guessing, and the
    # add button follows the same contract the controller enforces.
    cron.fill("0 20 * *")
    unsupported = page.locator("#schedule-preview").inner_text()
    assert "Unsupported in this preview" in unsupported
    # The copy must not claim the controller rejects other spellings it may accept.
    assert "controller accepts" not in unsupported
    assert page.locator("#schedule-submit").is_disabled()
    assert page.evaluate("window.__horizonTest.cronPreview('0 20 * *', null)") is None
    cron.fill("60 * * * *")
    assert "Unsupported in this preview" in page.locator("#schedule-preview").inner_text()
    assert page.locator("#schedule-submit").is_disabled()
    cron.fill("0 0 31 2 *")
    assert "No run matches this expression within the next four years" in page.locator("#schedule-preview").inner_text()
    assert page.locator("#schedule-submit").is_disabled()
    cron.fill("")
    assert page.locator("#schedule-submit").is_disabled()
    cron.fill("30 23 * * 5")
    assert page.locator("#schedule-submit").is_enabled()
    # The add form states the operation it creates instead of hiding it until
    # the confirmation dialog.
    assert page.locator("#schedule-submit").inner_text() == "Add profile switch"
    assert "profile switches" in page.locator("#schedule-form-action").inner_text()


def test_schedule_edits_preserve_operation_and_every_policy_field(schedules_page: Page):
    page = schedules_page
    api = page.fake_api  # type: ignore[attr-defined]

    page.locator('[data-schedule-row="2"]').get_by_role("switch", name="Enable schedule for Terraria tModLoader at 40 3 * * *").click()
    assert api.posted, "toggle should post the whole schedule book"
    posted = api.posted[-1]
    assert posted[0] == {
        "cron": "10 3 * * *",
        "profile": "minecraft-sunlit-cobblemon",
        "enabled": True,
        "operation": "backup",
        "backup_destination": "horizon-b2",
    }
    assert posted[1] == {
        "cron": "20 3 * * *",
        "profile": "terraria-vanilla",
        "enabled": True,
        "operation": "backup",
        "backup_destination": "local",
    }
    assert posted[2] == {
        "cron": "40 3 * * *",
        "profile": "terraria-tmod",
        "enabled": True,
        "operation": "benchmark",
        "baseline_preset": "baseline",
        "candidate_preset": "candidate",
        "campaign": "weekly",
        "maintenance_window": True,
        "rollback_safe": True,
        "public_wake_policy": "safe",
    }

    # Removing an unrelated row keeps the benchmark entry's policy intact.
    page.locator('[data-schedule-row="0"]').get_by_role("button", name="Remove schedule for Sunlit Cobblemon at 10 3 * * *").click()
    latest = api.posted[-1]
    assert len(latest) == 2
    assert latest[1]["operation"] == "benchmark"
    assert latest[1]["maintenance_window"] is True and latest[1]["rollback_safe"] is True
    assert latest[1]["public_wake_policy"] == "safe"
    assert latest[1]["campaign"] == "weekly"
    # No lifecycle mutation was attempted while editing schedules.
    assert set(api.mutations) == {"POST /api/v1/schedules"}


def test_unconfigured_fallback_profiles_have_no_lifecycle_controls(schedules_page: Page):
    page = schedules_page
    page.goto(f"{page.url.split('#', 1)[0]}#/")
    page.wait_for_selector('[data-profile-id="minecraft"]')

    assert page.evaluate("window.__horizonTest.configuredProfiles()") == CONFIGURED
    for profile_id in ("minecraft", "pz-rising"):
        card = page.locator(f'[data-profile-id="{profile_id}"]')
        assert "is-unavailable" in card.get_attribute("class")
        assert card.locator(".status-text").inner_text().lower() == "not configured"
        for selector in (".action-start", ".action-stop", ".action-restart", ".action-switch", ".manage-link"):
            assert card.locator(selector).is_hidden()
        assert card.locator(".card-reason").inner_text().startswith("Horizon has no configuration for this profile")
    assert page.get_by_role("button", name="Start Minecraft", exact=True).count() == 0
    assert page.get_by_role("button", name="Start Project Zomboid", exact=True).count() == 0

    nav_item = page.locator('[data-profile-nav="minecraft"]')
    assert nav_item.get_attribute("aria-disabled") == "true"
    assert "not configured" in nav_item.inner_text()
    assert page.locator('[data-profile-nav="minecraft-sunlit-cobblemon"]').get_attribute("aria-disabled") is None

    # The switch dialog offers configured servers only.
    page.locator("#switch-active").click()
    options = page.evaluate("() => [...document.querySelectorAll('#switch-target option')].map((option) => option.value)")
    assert set(options) <= set(CONFIGURED)
    page.locator('#switch-dialog button[value="cancel"]').first.click()

    # An unconfigured or unknown detail route falls back to the dashboard.
    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/console")
    page.wait_for_selector("#dashboard-view:not([hidden])")
    assert page.get_by_role("heading", name="Dashboard").is_visible()


def test_schedule_editor_excludes_unconfigured_servers(schedules_page: Page):
    page = schedules_page
    options = page.evaluate("() => [...document.querySelectorAll('#schedule-profile option')].map((option) => option.value)")
    assert options == CONFIGURED


def test_palette_and_notification_destinations_exclude_unconfigured_profiles(schedules_page: Page):
    """Placeholder ids stay dashboard-only: no palette route/action, no alert target."""
    page = schedules_page
    page.goto(f"{page.url.split('#', 1)[0]}#/")
    page.wait_for_selector('[data-profile-id="minecraft"]')

    page.get_by_role("button", name="Command palette").click()
    page.wait_for_selector("#command-palette[open]")
    entries = page.evaluate(
        "() => [...document.querySelectorAll('#palette-list [role=option]')].map((node) => node.textContent)"
    )
    palette_ids = page.evaluate("() => window.HORIZON_PALETTE.getCommands().map((command) => command.id)")
    assert entries, "palette should list configured destinations"
    placeholder_prefixes = ("minecraft-", "pz-rising-")
    offenders = [
        item for item in palette_ids
        if item.startswith(placeholder_prefixes)
        and not any(item.startswith(f"{configured}-") for configured in CONFIGURED)
    ]
    assert offenders == []
    assert not [text for text in entries if "Minecraft" in text or "Project Zomboid" in text]
    assert [item for item in palette_ids if item.startswith("terraria-")]
    assert [item for item in palette_ids if item.startswith("minecraft-sunlit-cobblemon-")]
    page.locator("#palette-close").click()

    page.goto(f"{page.url.split('#', 1)[0]}#/settings")
    page.wait_for_selector("#settings-view:not([hidden])")
    options = page.evaluate("() => [...document.querySelectorAll('#notification-profile option')].map((option) => option.value)")
    assert options == CONFIGURED


def test_forward_unknown_operation_is_not_shown_as_a_profile_switch(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}, init_script="window.__HORIZON_TEST__ = {}") as page:
        unknown = {
            "cron": "0 2 * * *",
            "profile": "minecraft-sunlit-cobblemon",
            "next_fire": "2026-09-16T02:00:00Z",
            "enabled": True,
            "operation": "restore",
        }
        api = _Api(schedules=[unknown, BACKUP_HORIZON])
        page.route("**/api/v1/**", api.handle)
        page.on("dialog", lambda dialog: dialog.accept())
        page.goto(f"{web_server}#/servers/minecraft-sunlit-cobblemon/config")
        page.wait_for_selector("#schedule-list [data-schedule-row]")
        row = page.locator('[data-schedule-row="0"]')
        assert row.get_attribute("data-schedule-operation") == "unknown"
        assert row.locator(".schedule-operation").inner_text() == "Unsupported operation"
        assert row.get_by_role("switch").is_disabled()
        assert row.get_by_role("button", name=re.compile(r"^Remove schedule")).is_disabled()
        expect(page.locator("#session-automation")).to_contain_text("Unsupported operation · Sunlit Cobblemon")
        row.get_by_role("switch").click(force=True)
        row.get_by_role("button", name=re.compile(r"^Remove schedule")).click(force=True)
        assert api.mutations == []


def test_card_transitions_back_when_a_profile_becomes_configured(schedules_page: Page):
    page = schedules_page
    api = page.fake_api  # type: ignore[attr-defined]
    page.goto(f"{page.url.split('#', 1)[0]}#/")
    page.wait_for_selector('[data-profile-id="minecraft"]')
    card = page.locator('[data-profile-id="minecraft"]')
    assert "is-unavailable" in card.get_attribute("class")
    assert card.locator(".manage-link").is_hidden()

    api.names = _names() + [
        {"id": "minecraft", "display_name": "Minecraft", "adapter": "crafty", "operations": ["start", "stop", "restart"]},
        {"id": "pz-rising", "display_name": "Project Zomboid", "adapter": "systemd", "operations": ["start", "stop", "restart"]},
    ]
    page.evaluate("() => window.__horizonTest.load()")
    expect(card.locator(".profile-name")).to_have_text("Minecraft")
    expect(card.locator(".status-text")).to_have_text("Stopped")
    assert "is-unavailable" not in card.get_attribute("class")
    expect(card.locator(".manage-link")).to_be_visible()
    assert not card.locator(".action-switch").is_hidden()
    expect(card.locator(".action-start")).to_be_visible()
    expect(card.locator(".action-start")).to_be_enabled()


def test_malformed_profile_list_keeps_the_last_confirmed_set(schedules_page: Page):
    page = schedules_page
    api = page.fake_api  # type: ignore[attr-defined]
    api.names = [{"display_name": "no id here"}]
    page.evaluate("() => window.__horizonTest.load()")
    assert page.evaluate("window.__horizonTest.configuredProfiles()") == CONFIGURED
    card = page.locator('[data-profile-id="minecraft-sunlit-cobblemon"]')
    assert "is-unavailable" not in card.get_attribute("class")


def test_transient_api_failure_keeps_last_known_configured_view(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}, init_script="window.__HORIZON_TEST__ = {}") as page:
        api = _Api()
        page.route("**/api/v1/**", api.handle)
        page.goto(f"{web_server}#/")
        page.wait_for_selector('[data-profile-id="minecraft-sunlit-cobblemon"]')
        expect(page.locator('[data-profile-id="minecraft-sunlit-cobblemon"] .profile-name')).to_have_text("Sunlit Cobblemon")

        api.close()
        page.evaluate("() => window.__horizonTest.load()")
        page.wait_for_selector("#retry-load:not([hidden])")

        # Answered failures never turn a configured server into a phantom
        # control panel, and never promote a placeholder either.
        card = page.locator('[data-profile-id="minecraft-sunlit-cobblemon"]')
        expect(card.locator(".profile-name")).to_have_text("Sunlit Cobblemon")
        assert "is-unavailable" not in card.get_attribute("class")
        assert card.locator(".manage-link").is_visible()
        fallback = page.locator('[data-profile-id="minecraft"]')
        assert "is-unavailable" in fallback.get_attribute("class")
        assert page.get_by_role("button", name="Start Minecraft", exact=True).count() == 0


def test_initial_api_failure_renders_no_phantom_lifecycle_controls(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}, init_script="window.__HORIZON_TEST__ = {}") as page:
        api = _Api(fail_always=True)
        page.route("**/api/v1/**", api.handle)
        page.goto(f"{web_server}#/")
        page.wait_for_selector('[data-profile-id="minecraft"]')
        page.wait_for_selector("#retry-load:not([hidden])")
        assert page.evaluate("window.__horizonTest.configuredProfiles()") == []
        for profile_id in ("minecraft", "pz-rising", "terraria-vanilla", "terraria-tmod"):
            card = page.locator(f'[data-profile-id="{profile_id}"]')
            assert "is-unavailable" in card.get_attribute("class")
            assert card.locator(".action-start").is_hidden()
            assert card.locator(".action-switch").is_hidden()
        assert page.get_by_role("button", name="Start Minecraft", exact=True).count() == 0
        assert page.get_by_role("button", name="Switch to Minecraft", exact=True).count() == 0


def test_scheduled_automations_are_discoverable_without_side_effects(schedules_page: Page):
    page = schedules_page
    api = page.fake_api  # type: ignore[attr-defined]
    api.mutations.clear()

    page.goto(f"{page.url.split('#', 1)[0]}#/settings")
    page.wait_for_selector("#settings-view:not([hidden])")
    panel = page.locator(".schedule-discover")
    assert panel.get_by_role("heading", name="Scheduled automations", exact=True).is_visible()
    link = page.locator("#settings-schedules-link")
    assert link.get_attribute("href") == "#/servers/minecraft-sunlit-cobblemon/config"
    assert link.get_attribute("aria-label") == "Open scheduled automations for Sunlit Cobblemon"
    link.click()
    page.wait_for_selector("#panel-config:not([hidden])")
    page.wait_for_selector("#schedule-list [data-schedule-row]")
    assert page.url.endswith("#/servers/minecraft-sunlit-cobblemon/config")
    # The link lands on the schedule section itself, not the config tab top.
    expect(page.locator("#schedules-title")).to_be_focused()
    assert page.evaluate(
        "() => { const box = document.getElementById('schedules-title').getBoundingClientRect(); return box.top >= 0 && box.top < window.innerHeight; }"
    )

    page.goto(f"{page.url.split('#', 1)[0]}#/settings")
    page.wait_for_selector("#settings-view:not([hidden])")
    page.get_by_role("button", name="Command palette").click()
    page.wait_for_selector("#command-palette[open]")
    page.locator("#palette-search").fill("schedul")
    option = page.get_by_role("option", name="Open scheduled automations")
    assert option.is_visible()
    option.click()
    page.wait_for_selector("#panel-config:not([hidden])")
    assert page.url.endswith("#/servers/minecraft-sunlit-cobblemon/config")
    expect(page.locator("#schedules-title")).to_be_focused()

    assert [entry for entry in api.mutations if "/schedules" in entry or "start" in entry or "stop" in entry or "switch" in entry] == []
