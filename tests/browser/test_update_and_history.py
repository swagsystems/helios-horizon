"""Authoritative Updating state and retained/recent stopped-profile metrics."""

from __future__ import annotations

import copy
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from playwright.sync_api import Page, expect

from browser_harness import browser_page


# Screenshots are private review artifacts and never belong in the public repo.
ARTIFACTS = Path(os.environ.get("HORIZON_UI_ARTIFACTS", "/root/horizon-overnight-20260914-lITzxz/ui-artifacts"))

UPDATE_NOTICE = "Updating… Start is unavailable until the update finishes."


STREAM_STUB = """
  window.__sources = [];
  window.EventSource = class {
    static CONNECTING = 0; static OPEN = 1; static CLOSED = 2;
    constructor(url) {
      this.url = url; this.readyState = 1; this.listeners = {};
      window.__sources.push(this);
      queueMicrotask(() => this.onopen?.(new Event('open')));
    }
    addEventListener(type, callback) { this.listeners[type] = callback; }
    close() { this.readyState = 2; }
    emit(type, payload) { this.listeners[type]?.(new MessageEvent(type, {data: JSON.stringify(payload)})); }
  };
"""


def profile(profile_id="minecraft", **overrides):
    base = {
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
        "installed_version": "1.21.8",
        "restart_required": False,
        "required_ports_ready": False,
    }
    base.update(overrides)
    return base


def build_page(page: Page, web_server, *, running=True):
    started = datetime.now(timezone.utc) - timedelta(minutes=30)
    series = {
        "cpu_percent": [
            {"ts": (started + timedelta(minutes=index)).isoformat(), "value": 10 + index, "state": "available"}
            for index in range(3)
        ],
        "rss_bytes": [
            {"ts": (started + timedelta(minutes=index)).isoformat(), "value": (4 + index) * 1024**3, "state": "available"}
            for index in range(3)
        ],
    }
    minecraft = profile(
        state="running", health="healthy", slot_owner="minecraft", pid=42,
        started_at=started.isoformat(), uptime_seconds=1800, cpu_percent=43.0,
        rss_bytes=6 * 1024**3, players_online=2, required_ports_ready=True,
    ) if running else profile()
    model = {
        "generation": 1,
        "status": {
            "generation": 1,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "profiles": [minecraft, profile("pz-rising")],
        },
        "history": {"resolution": "1m", "context": {"series": series}},
        "history_failure": False,
    }

    def fulfill(route):
        path = route.request.url.split("/api/v1", 1)[-1].split("?", 1)[0]
        if path == "/session":
            return route.fulfill(json={"actor": "operator@example.test", "csrf_token": "csrf"})
        if path == "/profiles":
            return route.fulfill(json=[
                {"id": "minecraft", "display_name": "Minecraft", "operations": ["start", "stop", "restart"]},
                {"id": "pz-rising", "display_name": "Project Zomboid", "operations": ["start", "stop", "restart"]},
            ])
        if path == "/status":
            return route.fulfill(json=model["status"])
        if path.endswith("/stats/tps"):
            if model["history_failure"]:
                return route.fulfill(status=500, json={"detail": "unavailable"})
            return route.fulfill(json=model["history"])
        if path.endswith("/stats/summary"):
            return route.fulfill(json={"occupancy": {"latest": 2, "samples": []}})
        if path.endswith("/resource-capacity"):
            return route.fulfill(json={
                "pid": 42, "started_at": started.isoformat(),
                "cpu_capacity_percent": 400, "memory_capacity_bytes": 12 * 1024**3,
            })
        if path.endswith("/logs"):
            return route.fulfill(json={"items": []})
        return route.fulfill(json={})

    page.route("**/api/v1/**", fulfill)
    page.goto(f"{web_server}#/servers/minecraft/metrics")
    page.wait_for_selector("#panel-metrics:not([hidden])")
    page.wait_for_function("window.__sources && window.__sources.length === 1")
    page._horizon_model = model  # type: ignore[attr-defined]
    return page


@pytest.fixture
def update_page(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}, init_script=STREAM_STUB) as page:
        build_page(page, web_server, running=True)
        page.wait_for_function("document.querySelector('#metric-cpu-current').textContent.includes('43.0%')")
        yield page


@pytest.fixture
def cold_stopped_page(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}, init_script=STREAM_STUB) as page:
        build_page(page, web_server, running=False)
        yield page


def emit(page: Page, snapshot: dict) -> None:
    model = page._horizon_model  # type: ignore[attr-defined]
    model["generation"] += 1
    payload = copy.deepcopy(snapshot)
    payload["generation"] = model["generation"]
    payload["observed_at"] = datetime.now(timezone.utc).isoformat()
    model["status"] = payload
    page.evaluate("data => window.__sources.at(-1).emit('status', data)", payload)


def stopped_status(**overrides) -> dict:
    return {"profiles": [profile("minecraft", **overrides), profile("pz-rising")]}


def test_update_reservation_marks_homepage_detail_and_disables_start(update_page: Page):
    page = update_page
    emit(page, stopped_status(update={
        "source": "reservation",
        "operation_id": "update-op-1",
        "expires_at": "2026-09-15T12:00:30Z",
    }))
    card = page.locator('[data-profile-id="minecraft"]')
    expect(card.locator(".status-text")).to_have_text("Updating")
    expect(card.locator(".action-start")).to_be_disabled()
    expect(card.locator(".card-reason")).to_have_text(UPDATE_NOTICE)
    expect(card).to_have_class(re.compile(r"\bstate-updating\b"))
    expect(page.locator("#session-primary")).to_have_text("Updating…")
    expect(page.locator("#session-primary")).to_be_disabled()
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(ARTIFACTS / "update-state-homepage.png"), full_page=True)
    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/console")
    expect(page.locator("#detail-status .status-text")).to_have_text("Updating")
    expect(page.locator("#detail-start")).to_be_disabled()
    note = page.locator("#detail-update-note")
    expect(note).to_be_visible()
    expect(note).to_have_text(UPDATE_NOTICE)
    # Plain copy only: no backend jargon in the operator-facing text.
    for jargon in ("reservation", "lease", "operation_id", "capability"):
        assert jargon not in note.inner_text().lower()
    page.screenshot(path=str(ARTIFACTS / "update-state-detail.png"), full_page=True)


def test_update_pauses_other_profile_starts_and_switch_dialog(update_page: Page):
    page = update_page
    other = page.locator('[data-profile-id="pz-rising"] .action-start')
    expect(other).to_be_enabled()
    emit(page, stopped_status(active_job_id="update"))
    expect(page.locator('[data-profile-id="minecraft"] .status-text')).to_have_text("Updating")
    expect(other).to_be_disabled()
    page.goto(f"{page.url.split('#', 1)[0]}#/")
    page.locator("#switch-active").click()
    expect(page.locator("#switch-target-summary")).to_have_text(
        "Updating Minecraft… Start is unavailable until the update finishes."
    )
    expect(page.locator("#switch-confirm")).to_be_disabled()
    page.locator('#switch-dialog button[value="cancel"]').first.click()
    expect(page.locator("#switch-dialog")).to_be_hidden()
    assert page.evaluate("() => document.getElementById('switch-dialog').open") is False
    # Completion re-enables every paused start entrypoint.
    emit(page, stopped_status())
    expect(other).to_be_enabled()
    expect(page.locator('[data-profile-id="minecraft"] .action-start')).to_be_enabled()
    expect(page.locator("#session-primary")).to_have_text("Start Minecraft")
    page.goto(f"{page.url.split('#', 1)[0]}#/")
    page.locator("#switch-active").click()
    expect(page.locator("#switch-target-summary")).not_to_contain_text("Updating")
    assert page.locator("#switch-target-summary").inner_text() in {"Minecraft", "Project Zomboid"}
    page.locator('#switch-dialog button[value="cancel"]').first.click()


def test_stale_update_reservation_does_not_gray_profile(update_page: Page):
    page = update_page
    card = page.locator('[data-profile-id="minecraft"]')
    emit(page, stopped_status())
    expect(card.locator(".status-text")).to_have_text("Stopped")
    expect(card.locator(".action-start")).to_be_enabled()
    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/console")
    expect(page.locator("#detail-start")).to_be_enabled()
    expect(page.locator("#detail-update-note")).to_be_hidden()


def test_stopped_profile_retains_historical_metrics(update_page: Page):
    page = update_page
    emit(page, stopped_status())
    note = page.locator("#metrics-history-note")
    expect(note).to_be_visible()
    expect(note).to_contain_text("Historical")
    expect(note).to_contain_text("last observed")
    # The range end is the last real sample, never the stop-status timestamp.
    expect(note).not_to_contain_text("→ now")
    expect(page.locator("#metric-cpu-current")).to_have_text("12.0% (historical)")
    expect(page.locator("#metric-memory-current")).to_have_text("6.0 GiB (historical)")
    expect(page.locator("#metric-players-current")).to_have_text("Offline")
    expect(page.locator("#metric-uptime")).to_have_text("Offline")
    expect(page.locator("#metrics-run-note")).to_contain_text("(last observed sample)")
    assert "now" not in page.locator("#metrics-run-note").inner_text().split("→")[-1]
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(ARTIFACTS / "offline-history-metrics.png"), full_page=True)
    # History survives tab navigation.
    page.locator("#tab-console").click()
    page.locator("#tab-metrics").click()
    expect(note).to_be_visible()
    expect(page.locator("#metric-cpu-current")).to_have_text("12.0% (historical)")


def test_new_run_replaces_historical_view(update_page: Page):
    page = update_page
    emit(page, stopped_status())
    expect(page.locator("#metrics-history-note")).to_be_visible()
    started = datetime.now(timezone.utc) - timedelta(minutes=2)
    emit(page, {
        "profiles": [
            profile(
                state="running", health="healthy", slot_owner="minecraft", pid=77,
                started_at=started.isoformat(), uptime_seconds=60, cpu_percent=8.0,
                rss_bytes=2 * 1024**3, players_online=1, required_ports_ready=True,
            ),
            profile("pz-rising"),
        ]
    })
    expect(page.locator("#metrics-history-note")).to_be_hidden()
    expect(page.locator("#metric-cpu-current")).to_contain_text("8.0%")
    expect(page.locator("#metric-cpu-current")).not_to_contain_text("historical")


def test_cold_stopped_page_shows_recent_history(cold_stopped_page: Page):
    page = cold_stopped_page
    note = page.locator("#metrics-history-note")
    expect(note).to_be_visible(timeout=10000)
    expect(note).to_have_attribute("data-state", "recent")
    expect(note).to_contain_text("Recent history · last observed")
    # A bounded server-side view, explicitly not claimed as one exact run.
    expect(note).to_contain_text("may include more than one server run")
    expect(page.locator("#metric-cpu-current")).to_have_text("12.0% (historical)")
    expect(page.locator("#metric-memory-current")).to_have_text("6.0 GiB (historical)")
    expect(page.locator("#metrics-run-note")).to_contain_text("(last observed sample)")
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(ARTIFACTS / "cold-stopped-recent-history.png"), full_page=True)


def test_recent_history_ignores_unavailable_points_and_uses_real_last_sample(cold_stopped_page: Page):
    page = cold_stopped_page
    expect(page.locator("#metrics-history-note")).to_be_visible(timeout=10000)
    started = datetime.now(timezone.utc) - timedelta(hours=1)

    def stamp(minutes):
        return (started + timedelta(minutes=minutes)).isoformat()

    page._horizon_model["history"] = {  # type: ignore[attr-defined]
        "resolution": "1h",
        "context": {
            "series": {
                "cpu_percent": [
                    {"ts": stamp(0), "value": 12.0, "state": "available"},
                    {"ts": stamp(30), "value": None, "state": "unavailable"},
                ],
                "rss_bytes": [{"ts": stamp(5), "value": 5 * 1024**3, "state": "available"}],
            }
        },
    }
    page.reload()
    page.wait_for_selector("#panel-metrics:not([hidden])")
    page.wait_for_function("window.__sources && window.__sources.length === 1")
    note = page.locator("#metrics-history-note")
    expect(note).to_be_visible(timeout=10000)
    # The unavailable null point never becomes a zero or the historical value.
    expect(page.locator("#metric-cpu-current")).to_have_text("12.0% (historical)")
    expect(page.locator("#metric-memory-current")).to_have_text("5.0 GiB (historical)")
    # The range end is the newest real sample, not the stop status or the null.
    expected = page.evaluate("ts => new Date(ts).toLocaleString()", stamp(5))
    expect(note).to_contain_text(f"last observed {expected}")
    assert "NaN" not in (page.locator("#metric-cpu-chart").text_content() or "")


def test_cold_page_reports_unavailable_and_no_data(cold_stopped_page: Page):
    page = cold_stopped_page
    expect(page.locator("#metrics-history-note")).to_be_visible(timeout=10000)
    # A failed bounded query is remembered and reported without fabricating values.
    page._horizon_model["history_failure"] = True  # type: ignore[attr-defined]
    page.reload()
    page.wait_for_selector("#panel-metrics:not([hidden])")
    page.wait_for_function("window.__sources && window.__sources.length === 1")
    note = page.locator("#metrics-history-note")
    expect(note).to_be_visible(timeout=10000)
    expect(note).to_have_attribute("data-state", "unavailable")
    expect(note).to_contain_text("Recent history is unavailable right now.")
    expect(page.locator("#metric-cpu-current")).to_have_text("—")
    # An empty bounded range is reported as no observations, not as zero/healthy.
    page._horizon_model["history_failure"] = False  # type: ignore[attr-defined]
    page._horizon_model["history"] = {"resolution": "1h", "context": {"series": {}}}  # type: ignore[attr-defined]
    page.reload()
    page.wait_for_selector("#panel-metrics:not([hidden])")
    page.wait_for_function("window.__sources && window.__sources.length === 1")
    expect(page.locator("#metrics-history-note")).to_be_visible(timeout=10000)
    expect(page.locator("#metrics-history-note")).to_contain_text("Recent history has no usable observations")
    expect(page.locator("#metric-cpu-current")).to_have_text("—")
    assert "Offline" in page.locator("#metric-players-current").inner_text()
