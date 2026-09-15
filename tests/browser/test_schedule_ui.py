from __future__ import annotations

import json
import re
from urllib.parse import urlparse

import pytest
from playwright.sync_api import Page, expect

from browser_harness import browser_page


@pytest.fixture
def schedule_page(web_server):
    with browser_page(has_touch=True, viewport={"width": 375, "height": 760}) as page:
        schedules = [{"cron": "0 20 * * 5", "profile": "minecraft", "next_fire": "2026-07-17T20:00:00Z", "enabled": True}]
        status = {"generation": 1, "observed_at": "2026-07-11T12:00:00Z", "profiles": [{
            "profile_id": "minecraft", "state": "running", "health": "healthy", "slot_owner": "minecraft",
            "active_job_id": None, "pid": 1, "started_at": "2026-07-11T10:00:00Z", "uptime_seconds": 7200,
            "cpu_percent": 1, "rss_bytes": 1, "players_online": 0, "installed_version": "1.21.8",
            "restart_required": False, "required_ports_ready": True,
        }]}
        names = [{"id": "minecraft", "display_name": "Minecraft", "adapter": "crafty", "operations": ["start", "stop", "restart"]}]

        def fulfill(route):
            path = urlparse(route.request.url).path
            if path == "/api/v1/session":
                return route.fulfill(json={"actor": "operator@example.test", "csrf_token": "test-token", "expires_at": None})
            if path == "/api/v1/status":
                return route.fulfill(json=status)
            if path == "/api/v1/profiles":
                return route.fulfill(json=names)
            if path == "/api/v1/schedules":
                if route.request.method == "POST":
                    schedules[:] = [{**item, "next_fire": "2026-07-17T20:00:00Z" if item.get("enabled", True) else None} for item in route.request.post_data_json["entries"]]
                return route.fulfill(json={"schedules": schedules})
            if path == "/api/v1/profiles/minecraft/config":
                return route.fulfill(json={"profile_id": "minecraft", "settings": []})
            if path == "/api/v1/stream":
                payload = json.dumps(status, separators=(",", ":"))
                return route.fulfill(status=200, headers={"Content-Type": "text/event-stream"}, body=f"event: status\ndata: {payload}\n\n")
            return route.fulfill(json={"ok": True})

        page.route("**/api/v1/**", fulfill)
        page.goto(f"{web_server}#/servers/minecraft/config")
        page.wait_for_selector("#panel-config:not([hidden])")
        page.on("dialog", lambda dialog: dialog.accept())
        yield page


def test_schedule_list_add_and_remove_contract(schedule_page: Page):
    page = schedule_page
    assert page.get_by_role("heading", name="Scheduled automations", exact=True).is_visible()
    expect(page.locator("#schedule-list [data-schedule-row]")).to_have_count(1)
    assert page.get_by_text("next: Fri, Jul 17", exact=False).is_visible()
    # The row names the operation it runs, not just the cron and the target.
    assert page.locator("[data-schedule-row]").first.get_attribute("data-schedule-operation") == "switch"
    assert page.get_by_text("Profile switch", exact=True).is_visible()
    assert "evaluated in UTC" in page.locator("#schedule-timezone").inner_text()
    status = page.locator("#schedule-status")
    assert page.locator(".schedule-settings [aria-live='polite']").count() == 1
    assert page.locator("#schedule-list").get_attribute("aria-live") is None
    assert status.inner_text() == "Schedule changes apply without restarting Horizon."

    page.get_by_label("Cron", exact=True).fill("15 9 * * 1")
    page.get_by_role("button", name="Add profile switch", exact=True).click()
    expect(page.locator("#schedule-list [data-schedule-row]")).to_have_count(2)
    assert page.get_by_text("15 9 * * 1", exact=True).is_visible()
    assert status.inner_text() == "Schedule changes applied live."
    assert page.evaluate("document.activeElement === document.querySelector('#schedule-cron')")

    remove = page.locator('[data-schedule-row]').first.get_by_role("button", name=re.compile(r"^Remove schedule for "))
    assert remove.get_attribute("aria-label") == "Remove schedule for Minecraft at 0 20 * * 5"
    remove.click()
    expect(page.locator("#schedule-list [data-schedule-row]")).to_have_count(1)
    assert status.inner_text() == "Schedule changes applied live."
    assert page.locator('[data-schedule-row]').first.get_by_role("button", name=re.compile(r"^Remove schedule for ")).evaluate("(node) => node === document.activeElement")


def test_schedule_toggle_round_trip_and_focus_contract(schedule_page: Page):
    page = schedule_page
    toggle = page.get_by_role("switch", name="Disable schedule for Minecraft at 0 20 * * 5")
    assert toggle.get_attribute("aria-checked") == "true"
    toggle.focus()
    toggle.click()
    disabled = page.get_by_role("switch", name="Enable schedule for Minecraft at 0 20 * * 5")
    expect(disabled).to_have_attribute("aria-checked", "false")
    assert "is-disabled" in page.locator('[data-schedule-row]').first.get_attribute("class")
    assert page.get_by_text("Disabled · no next fire", exact=True).is_visible()
    assert page.locator("#schedule-status").inner_text() == "Schedule disabled for Minecraft."
    assert disabled.evaluate("(node) => node === document.activeElement")


def test_schedule_mobile_accessibility_contract(schedule_page: Page):
    page = schedule_page
    assert page.evaluate("matchMedia('(pointer: coarse)').matches")
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    assert page.locator(".schedule-settings").evaluate("(node) => Math.round(node.getBoundingClientRect().right) <= window.innerWidth")
    assert page.locator("#schedule-form input, #schedule-form select, #schedule-form button, #schedule-list button").evaluate_all(
        "(nodes) => nodes.every((node) => Math.round(node.getBoundingClientRect().height) >= 44)"
    )
    assert page.locator("#schedule-cron").get_attribute("aria-label") is None
    schedule = page.locator(".schedule-settings")
    cron = schedule.get_by_label("Cron", exact=True)
    assert cron.is_visible()
    assert schedule.get_by_label("Server", exact=True).is_visible()
    cron.focus()
    assert page.evaluate("document.activeElement.matches(':focus-visible')")
    assert page.evaluate("getComputedStyle(document.activeElement).outlineWidth") == "3px"
    page.emulate_media(reduced_motion="reduce")
    assert page.evaluate("getComputedStyle(document.querySelector('.schedule-settings')).animationName") == "none"
