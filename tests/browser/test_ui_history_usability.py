"""Retained telemetry history access, period labels, and player-activity links.

Synthetic UI coverage for the history usability findings: the recorder table
must expose every retained sample in the selected chart window (not only the
newest page of rows), player totals and empty states must name the selected
period, and the Player activity shortcut must land near player information.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import pytest
from playwright.sync_api import Page, expect

from browser_harness import browser_page


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

WINDOW_HOURS = {"1h": 1, "6h": 6, "24h": 24, "7d": 168, "30d": 720}
SAMPLE_STEP_SECONDS = 120
SAMPLE_TOTAL = 672
ACTIVE_MINUTES = 167


def _stamp(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _samples() -> list[dict]:
    """672 raw samples: the oldest ~167 minutes active, the newest 120 inactive."""
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    newest_age_seconds = (SAMPLE_TOTAL - 1 - 0) * SAMPLE_STEP_SECONDS
    active_from_age = newest_age_seconds - ACTIVE_MINUTES * 60
    rows = []
    for index in range(SAMPLE_TOTAL):
        at = end - timedelta(seconds=(SAMPLE_TOTAL - 1 - index) * SAMPLE_STEP_SECONDS)
        age = (SAMPLE_TOTAL - 1 - index) * SAMPLE_STEP_SECONDS
        active = age >= active_from_age
        rows.append({
            "ts": _stamp(at),
            "state": "available" if active else "inactive",
            "tps": 20.0 if active else None,
            "mspt": 18.0 if active else None,
        })
    return rows


def _profile(profile_id: str, **overrides) -> dict:
    base = {
        "profile_id": profile_id, "state": "running", "health": "healthy",
        "slot_owner": profile_id, "pid": 42, "uptime_seconds": 900,
        "started_at": _stamp(datetime.now(timezone.utc) - timedelta(minutes=15)),
        "players_online": 1, "required_ports_ready": True,
        "installed_version": "1.21.8", "restart_required": False,
    }
    base.update(overrides)
    return base


def _install_history_routes(page: Page, web_server: str, control: dict) -> None:
    """Serve the history fixture; ``control`` can delay or fail the base calls."""
    samples = _samples()

    def fulfill(route):
        request = route.request
        url = urlparse(request.url)
        path = url.path
        control["requests"].append(request.url)
        if path == "/api/v1/session":
            return route.fulfill(json={"actor": "operator@example.test", "csrf_token": "test-token"})
        if path == "/api/v1/profiles":
            return route.fulfill(json=[
                {"id": "minecraft", "display_name": "Minecraft", "operations": ["start", "stop"]},
                {"id": "pz-rising", "display_name": "Project Zomboid", "operations": ["start", "stop"]},
            ])
        if path == "/api/v1/status":
            return route.fulfill(json={
                "generation": 1, "observed_at": _stamp(datetime.now(timezone.utc)),
                "profiles": [
                    _profile("minecraft"),
                    _profile("pz-rising", state="stopped", slot_owner=None, pid=None, started_at=None,
                             uptime_seconds=None, players_online=None, required_ports_ready=False),
                ],
            })
        if path.endswith("/stats/summary") or path.endswith("/stats/heatmap"):
            if control.get("delay_ms"):
                time.sleep(control["delay_ms"] / 1000)
            if control.get("base_fail"):
                return route.fulfill(status=503, json={"detail": "gateway unavailable"})
        if path.endswith("/stats/summary"):
            profile_id = path.split("/")[4]
            if profile_id == "pz-rising":
                return route.fulfill(json={
                    "total_hours": 0, "unique_players": 0, "leaderboard": [],
                    "player_tracking": "count", "occupancy": {"latest": 1, "samples": []},
                })
            hours = int(parse_qs(url.query).get("hours", ["6"])[0])
            if hours >= 24:
                return route.fulfill(json={
                    "total_hours": 3.4686, "unique_players": 2,
                    "leaderboard": [
                        {"player": "Guest", "hours": 2.5, "sessions": 2, "last_seen": _stamp(datetime.now(timezone.utc))},
                        {"player": "Builder", "hours": 0.97, "sessions": 1, "last_seen": _stamp(datetime.now(timezone.utc))},
                    ],
                    "player_tracking": "names", "occupancy": {"latest": None, "samples": []},
                })
            return route.fulfill(json={
                "total_hours": 0, "unique_players": 0, "leaderboard": [],
                "player_tracking": "names", "occupancy": {"latest": None, "samples": []},
            })
        if path.endswith("/stats/heatmap"):
            hours = int(parse_qs(url.query).get("hours", ["6"])[0])
            # Backend semantics: the `hours` request governs the cutoff; the
            # echoed `days` default is not the coverage.
            return route.fulfill(json={
                "days": 90,
                "buckets": [[float((day + int(hours)) % 4) for _ in range(24)] for day in range(7)],
                "as_of": _stamp(datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)),
                "truncated": False, "cache_strategy": "hour_bucket_bounded_on_demand",
            })
        if path.endswith("/stats/tps"):
            query = parse_qs(url.query)
            window = query.get("window", ["6h"])[0]
            cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS[window])
            visible = [row for row in samples if datetime.fromisoformat(row["ts"].replace("Z", "+00:00")) >= cutoff]
            latest = next((row for row in reversed(samples) if row["state"] == "available"), None)
            payload = {
                "window": window, "resolution": "raw", "limit": 720,
                "samples": visible, "stale": False, "state": "ok",
                "time_basis": {"active_runtime_seconds": ACTIVE_MINUTES * 60, "wall_clock_seconds": WINDOW_HOURS[window] * 3600},
            }
            if latest is not None and latest not in visible:
                payload["latest_observation"] = {**latest, "stale": False}
                payload["latest_ts"] = latest["ts"]
            return route.fulfill(json=payload)
        if path.endswith("/logs"):
            return route.fulfill(json={"items": []})
        return route.fulfill(json={"ok": True})

    page.route("**/api/v1/**", fulfill)
    page.history_requests = control["requests"]  # type: ignore[attr-defined]
    page.history_control = control  # type: ignore[attr-defined]
    page.goto(f"{web_server}#/servers/minecraft/stats")
    page.wait_for_selector("#panel-stats:not([hidden])")


@pytest.fixture
def history_page(web_server):
    control = {"requests": [], "base_fail": False, "delay_ms": 0}
    with browser_page(viewport={"width": 1280, "height": 900}, init_script=STREAM_STUB) as page:
        _install_history_routes(page, web_server, control)
        yield page


@pytest.fixture
def recovery_page(web_server):
    control = {"requests": [], "base_fail": False, "delay_ms": 0}
    with browser_page(viewport={"width": 1280, "height": 900}, init_script=STREAM_STUB) as page:
        _install_history_routes(page, web_server, control)
        yield page


def test_recorder_table_pages_every_retained_sample_and_reaches_active_rows(history_page: Page):
    page = history_page
    page.locator("#stats-window").select_option("24h")
    expect(page.locator("#recorder-coverage")).to_contain_text("672")
    page.locator(".recorder-table-disclosure summary").click()
    expect(page.locator("#recorder-coverage")).to_be_visible()
    coverage = page.locator("#recorder-coverage").inner_text()
    assert "last 24 hours" in coverage
    assert "newest first" in coverage
    assert "Coverage" in coverage
    assert "\u2192" in coverage  # explicit start -> end time coverage
    assert "84 active" in coverage and "588 inactive" in coverage

    expect(page.locator("#recorder-page-status")).to_contain_text("of 672")
    expect(page.locator("#recorder-page-status")).to_contain_text("Rows 1\u201350")
    # Bounded DOM: one page of rows for a 672-sample window.
    assert page.locator("#stats-recorder-table tr").count() == 50
    first_page = page.locator("#stats-recorder-table").inner_text()
    assert "inactive" in first_page and "available" not in first_page

    # Page backward through the window until the older active run is reachable.
    found_active = False
    for _ in range(20):
        if "available" in page.locator("#stats-recorder-table").inner_text():
            found_active = True
            break
        if page.locator("#recorder-next").is_disabled():
            break
        page.locator("#recorder-next").click()
    assert found_active, "pagination must reach the active samples older than the newest page"
    assert page.locator("#stats-recorder-table tr").count() <= 50
    expect(page.locator("#recorder-page-status")).to_contain_text("of 672")

    # The state filter is a direct route to the same retained active samples.
    page.locator("#recorder-filter").select_option("available")
    expect(page.locator("#recorder-page-status")).to_contain_text("of 84")
    filtered_coverage = page.locator("#recorder-coverage").inner_text()
    # Chart coverage and the filtered table count must not contradict each other.
    assert "The chart plots 672 samples" in filtered_coverage
    assert "filtered to 84 available samples" in filtered_coverage
    assert "pages all of them" not in filtered_coverage
    assert "84 active" in filtered_coverage and "588 inactive" in filtered_coverage
    active_text = page.locator("#stats-recorder-table").inner_text()
    assert "available" in active_text and "inactive" not in active_text
    expect(page.locator("#recorder-prev")).to_be_disabled()

    # Returning to "All samples" restores the all-rows sentence.
    page.locator("#recorder-filter").select_option("all")
    expect(page.locator("#recorder-page-status")).to_contain_text("of 672")
    all_coverage = page.locator("#recorder-coverage").inner_text()
    assert "pages all of them newest first" in all_coverage
    assert "filtered to" not in all_coverage

    # Rows per page is a bounded control, not an unbounded dump.
    page.locator("#recorder-filter").select_option("available")
    page.locator("#recorder-page-size").select_option("200")
    assert page.locator("#stats-recorder-table tr").count() == 84
    expect(page.locator("#recorder-page-status")).to_contain_text("of 84")


def test_player_totals_empty_states_and_heatmap_name_the_selected_window(history_page: Page):
    page = history_page
    expect(page.locator("#stats-total-hours")).to_have_text("0.00")
    expect(page.locator("#stats-period-note")).to_contain_text("last 6 hours")
    expect(page.locator("#stats-total-hours-period")).to_have_text("(last 6 hours)")
    expect(page.locator("#stats-unique-players-period")).to_have_text("(last 6 hours)")
    empty = page.locator("#stats-leaderboard").inner_text()
    assert "last 6 hours" in empty
    assert "not all-time history" in empty
    expect(page.locator("#stats-heatmap-meta")).to_contain_text("UTC \u00b7 last 6 hours")
    assert "90 days" not in page.locator("#panel-stats").inner_text()

    heatmap_requests = [url for url in page.history_requests if "/stats/heatmap" in url]  # type: ignore[attr-defined]
    assert any("hours=6" in url for url in heatmap_requests)

    page.locator("#stats-window").select_option("24h")
    expect(page.locator("#stats-total-hours")).to_have_text("3.47")
    expect(page.locator("#stats-total-hours-period")).to_have_text("(last 24 hours)")
    expect(page.locator("#stats-period-note")).to_contain_text("last 24 hours")
    expect(page.locator("#stats-leaderboard-meta")).to_contain_text("last 24 hours")
    expect(page.locator("#stats-leaderboard tr")).to_have_count(2)
    expect(page.locator("#stats-heatmap-meta")).to_contain_text("UTC \u00b7 last 24 hours")
    assert any("hours=24" in url for url in
               [u for u in page.history_requests if "/stats/heatmap" in u])  # type: ignore[attr-defined]

    page.locator("#stats-window").select_option("6h")
    expect(page.locator("#stats-total-hours")).to_have_text("0.00")
    empty = page.locator("#stats-leaderboard").inner_text()
    assert "last 6 hours" in empty and "24 hours" not in empty


def test_last_observed_tick_values_are_marked_outside_the_selected_window(history_page: Page):
    page = history_page
    page.locator("#stats-window").select_option("1h")
    expect(page.locator("#stats-last-observed")).to_be_visible()
    note = page.locator("#stats-last-observed").inner_text()
    assert "outside the selected window" in note
    assert "last 1 hour" in note
    expect(page.locator("#stats-tps-current")).to_have_text("20.00 TPS")
    assert page.locator("#stats-tps-cell").get_attribute("data-outside") == "true"
    assert page.locator("#stats-mspt-cell").get_attribute("data-outside") == "true"
    background = page.evaluate("getComputedStyle(document.querySelector('#stats-tps-cell')).backgroundColor")
    assert background not in ("", "transparent", "rgba(0, 0, 0, 0)")

    # Once the newest observation is inside the window, the marker is removed.
    page.locator("#stats-window").select_option("24h")
    expect(page.locator("#stats-last-observed")).to_be_hidden()
    assert page.locator("#stats-tps-cell").get_attribute("data-outside") == "false"


def test_player_activity_shortcut_focuses_the_player_section(history_page: Page):
    page = history_page
    page.goto(f"{page.url.split('#')[0]}#/")
    expect(page.locator("#session-activity-link")).to_have_attribute(
        "href", "#/servers/minecraft/stats/players"
    )
    page.locator("#session-activity-link").click()
    expect(page).to_have_url(re.compile(r"#/servers/minecraft/stats/players$"))
    expect(page.locator("#stats-total-hours")).to_have_text("0.00")
    page.wait_for_function("document.activeElement && document.activeElement.id === 'stats-summary'")

    # Navigate away and back: the focus target survives history navigation.
    page.locator("#tab-console").click()
    expect(page).to_have_url(re.compile(r"#/servers/minecraft/console$"))
    page.go_back()
    page.wait_for_function("document.activeElement && document.activeElement.id === 'stats-summary'")

    # A count-only game focuses the occupancy section it actually renders.
    page.goto(f"{page.url.split('#')[0]}#/servers/pz-rising/stats/players")
    page.wait_for_selector("#stats-occupancy-block:not([hidden])")
    page.wait_for_function("document.activeElement && document.activeElement.id === 'stats-occupancy-block'")


def test_window_change_failure_keeps_labels_on_shown_data_and_retries(recovery_page: Page):
    """6h data stays labelled 6h while a 24h base request fails, then recovers."""
    page = recovery_page
    control = page.history_control  # type: ignore[attr-defined]
    expect(page.locator("#stats-total-hours")).to_have_text("0.00")
    expect(page.locator("#stats-total-hours-period")).to_have_text("(last 6 hours)")
    assert "last 6 hours" in page.locator("#stats-leaderboard-meta").inner_text()

    # A pending request for another window is stated explicitly, not relabelled.
    control["delay_ms"] = 300
    control["base_fail"] = True
    page.locator("#stats-window").select_option("24h")
    expect(page.locator("#stats-period-note")).to_contain_text(
        "Loading the last 24 hours player summary; showing the last 6 hours data"
    )
    expect(page.locator("#stats-total-hours")).to_have_text("0.00")
    expect(page.locator("#stats-total-hours-period")).to_have_text("(last 6 hours)")

    # After the failure the old window stays labelled as itself and never as 24h.
    expect(page.locator("#stats-period-note")).to_contain_text(
        "because the last 24 hours request failed", timeout=10000
    )
    expect(page.locator("#stats-total-hours")).to_have_text("0.00")
    expect(page.locator("#stats-total-hours-period")).to_have_text("(last 6 hours)")
    assert "last 24 hours" not in page.locator("#stats-leaderboard-meta").inner_text()
    assert "last 24 hours" not in page.locator("#stats-heatmap-meta").inner_text()

    # Other cached-TPS consumers (resize, comparison change) must not re-render
    # the 6h recorder payload under the newly selected 24h labels.
    page.set_viewport_size({"width": 1100, "height": 800})
    page.evaluate("document.querySelector('#stats-comparison').dispatchEvent(new Event('change'))")
    page.wait_for_timeout(250)
    failed_coverage = page.locator("#recorder-coverage").text_content()
    assert "last 6 hours" in failed_coverage
    assert "last 24 hours" not in failed_coverage
    assert "last 24 hours" not in page.locator("#stats-leaderboard-meta").inner_text()
    assert "last 24 hours" not in page.locator("#stats-heatmap-meta").inner_text()

    # Leaving and re-entering the tab while still failing keeps data and labels
    # aligned, and must not mark the base as loaded for the 24h window.
    page.locator("#tab-console").click()
    page.goto(f"{page.url.split('#')[0]}#/servers/minecraft/stats")
    page.wait_for_selector("#panel-stats:not([hidden])")
    expect(page.locator("#stats-total-hours")).to_have_text("0.00")
    expect(page.locator("#stats-total-hours-period")).to_have_text("(last 6 hours)")
    expect(page.locator("#stats-period-note")).to_contain_text(
        "because the last 24 hours request failed", timeout=10000
    )

    # Transport recovery: the periodic poll retries the base for the new window.
    control["delay_ms"] = 0
    control["base_fail"] = False
    expect(page.locator("#stats-total-hours")).to_have_text("3.47", timeout=15000)
    expect(page.locator("#stats-total-hours-period")).to_have_text("(last 24 hours)")
    expect(page.locator("#stats-period-note")).to_contain_text("last 24 hours")
    expect(page.locator("#stats-leaderboard-meta")).to_contain_text("last 24 hours")
    expect(page.locator("#stats-heatmap-meta")).to_contain_text("UTC \u00b7 last 24 hours")
    expect(page.locator("#stats-leaderboard tr")).to_have_count(2)

    # Count-only games keep their occupancy layout after a window switch.
    page.goto(f"{page.url.split('#')[0]}#/servers/pz-rising/stats")
    page.wait_for_selector("#stats-occupancy-block:not([hidden])")
    assert page.locator("#stats-summary").is_hidden()
    assert page.locator("#stats-leaderboard").is_hidden()

    # A failed request for a count-only game keeps the occupancy layout it showed
    # instead of switching to the named-player layout.
    control["base_fail"] = True
    page.locator("#stats-window").select_option("7d")
    expect(page.locator("#stats-period-note")).to_contain_text("last 7 days", timeout=10000)
    assert page.locator("#stats-occupancy-block").is_visible()
    assert page.locator("#stats-summary").is_hidden()
    assert page.locator("#stats-leaderboard").is_hidden()


def test_history_panel_wraps_empty_state_and_values_on_phone(web_server):
    with browser_page(viewport={"width": 390, "height": 844}, init_script=STREAM_STUB) as page:
        samples = _samples()

        def fulfill(route):
            url = urlparse(route.request.url)
            path = url.path
            if path == "/api/v1/session":
                return route.fulfill(json={"actor": "operator@example.test", "csrf_token": "test-token"})
            if path == "/api/v1/profiles":
                return route.fulfill(json=[{"id": "minecraft", "display_name": "Minecraft", "operations": ["start", "stop"]}])
            if path == "/api/v1/status":
                return route.fulfill(json={
                    "generation": 1, "observed_at": _stamp(datetime.now(timezone.utc)),
                    "profiles": [_profile("minecraft")],
                })
            if path.endswith("/stats/summary"):
                return route.fulfill(json={
                    "total_hours": 0, "unique_players": 0, "leaderboard": [],
                    "player_tracking": "names", "occupancy": {"latest": None, "samples": []},
                })
            if path.endswith("/stats/heatmap"):
                return route.fulfill(json={"days": 90, "buckets": [[0.0] * 24 for _ in range(7)]})
            if path.endswith("/stats/tps"):
                return route.fulfill(json={"window": "6h", "resolution": "raw", "limit": 720,
                                           "samples": samples[-120:], "stale": False, "state": "ok"})
            return route.fulfill(json={"ok": True})

        page.route("**/api/v1/**", fulfill)
        page.goto(f"{web_server}#/servers/minecraft/stats")
        page.wait_for_selector("#panel-stats:not([hidden])")
        expect(page.locator("#stats-total-hours")).to_have_text("0.00")

        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        empty = page.locator("#stats-leaderboard .empty-state")
        assert empty.evaluate("(node) => node.scrollWidth <= node.clientWidth + 1")
        assert empty.evaluate("(node) => node.getBoundingClientRect().right <= window.innerWidth")
        assert empty.evaluate("(node) => node.getBoundingClientRect().left >= 0")
        assert empty.evaluate("(node) => getComputedStyle(node).whiteSpace") == "normal"
        # Long technical values wrap instead of clipping the panel.
        for selector in ("#stats-period-note", "#stats-effective-resolution", "#stats-total-hours-period"):
            assert page.locator(selector).evaluate("(node) => node.scrollWidth <= node.clientWidth + 1")
