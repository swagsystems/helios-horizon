from __future__ import annotations

from datetime import datetime, timezone

import pytest
from playwright.sync_api import Page

from browser_harness import browser_page


def _status(**overrides) -> dict:
    base = {
        "generation": 1,
        "profiles": [{
            "profile_id": "minecraft",
            "state": "running",
            "health": "healthy",
            "pid": 42,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "required_ports_ready": True,
        }],
    }
    base["profiles"][0].update(overrides)
    return base


def test_rail_version_note_reports_offline_when_stopped(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, _status(state="stopped", pid=None, required_ports_ready=False))
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#panel-metrics:not([hidden])")
        assert page.locator("#rail-version-note").inner_text() == "Offline"


def test_rail_version_note_reports_updating_not_accepted(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, _status(state="running", update={"source": "job"}))
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#panel-metrics:not([hidden])")
        assert page.locator("#rail-version-note").inner_text() == "Updating…"


def test_rail_version_note_keeps_ready_and_starting_copy(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, _status(state="running", required_ports_ready=True))
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#panel-metrics:not([hidden])")
        assert page.locator("#rail-version-note").inner_text() == "Ready on required ports"
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, _status(state="starting", required_ports_ready=False))
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#panel-metrics:not([hidden])")
        assert page.locator("#rail-version-note").inner_text() == "Accepted; waiting for readiness"


def _route(page: Page, status: dict) -> None:
    def fulfill(route):
        path = route.request.url.split("/api/v1", 1)[-1].split("?", 1)[0]
        if path == "/session":
            return route.fulfill(json={"actor": "operator", "csrf_token": "csrf"})
        if path == "/profiles":
            return route.fulfill(json=[{"id": "minecraft", "display_name": "Minecraft",
                                        "operations": ["start", "stop"]}])
        if path == "/status":
            return route.fulfill(json=status)
        if path.endswith("/resource-capacity"):
            return route.fulfill(json={"cpu_capacity_percent": 400,
                                       "memory_capacity_bytes": 12 * 1024**3})
        if path.endswith("/logs"):
            return route.fulfill(json={"items": []})
        return route.fulfill(json={})

    page.route("**/api/v1/**", fulfill)
