from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from playwright.sync_api import Page

from browser_harness import browser_page


@pytest.fixture
def metrics_page(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}, init_script="window.__HORIZON_TEST__ = {}") as page:
        started = datetime.now(timezone.utc) - timedelta(minutes=40)
        status = {"generation": 1, "profiles": [{"profile_id": "minecraft", "state": "running", "health": "healthy", "pid": 42, "started_at": started.isoformat(), "cpu_percent": 43, "rss_bytes": 6 * 1024**3, "players_online": 2, "required_ports_ready": True}]}
        series = {"cpu_percent": [{"ts": (started + timedelta(minutes=i)).isoformat(), "value": 10 + i, "state": "available"} for i in range(3)], "rss_bytes": [{"ts": (started + timedelta(minutes=i)).isoformat(), "value": (4 + i) * 1024**3, "state": "available"} for i in range(3)]}

        def fulfill(route):
            path = route.request.url.split("/api/v1", 1)[-1].split("?", 1)[0]
            if path == "/session": return route.fulfill(json={"actor": "operator", "csrf_token": "csrf"})
            if path == "/profiles": return route.fulfill(json=[{"id": "minecraft", "display_name": "Minecraft", "operations": ["start", "stop"]}])
            if path == "/status": return route.fulfill(json=status)
            if path.endswith("/stats/tps"): return route.fulfill(json={"context": {"series": series}})
            if path.endswith("/resource-capacity"): return route.fulfill(json={"pid": 42, "started_at": started.isoformat(), "cpu_capacity_percent": 400, "memory_capacity_bytes": 12 * 1024**3, "cpu_source": "process_affinity", "memory_source": "cgroup"})
            if path.endswith("/logs"): return route.fulfill(json={"items": []})
            return route.fulfill(json={})

        page.route("**/api/v1/**", fulfill)
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#panel-metrics:not([hidden])")
        page.wait_for_function("document.querySelector('#rail-cpu').textContent.includes('400%')")
        page._metrics_status = status
        page._metrics_series = series
        yield page


def test_metrics_use_retained_current_run_and_capacity(metrics_page: Page):
    page = metrics_page
    assert page.locator("#metric-cpu-current").inner_text() == "43.0% / 400%"
    assert page.locator("#metric-memory-current").inner_text() == "6.0 / 12.0 GiB"
    assert page.locator("#metric-cpu-capacity").inner_text() == "Current / available CPU capacity"
    assert page.locator("#rail-cpu-capacity").get_attribute("aria-valuemax") == "400"
    assert page.locator("#metric-cpu-chart .chart-x0").text_content() != "—"
    assert page.locator("#metric-cpu-chart .chart-ymax").text_content() == "400%"
    assert page.locator("#metric-memory-chart .chart-ymax").text_content() == "12.0 GiB"


def test_metrics_are_responsive_without_fake_null_points(metrics_page: Page):
    page = metrics_page
    for width in (320, 768, 1920):
        page.set_viewport_size({"width": width, "height": 900})
        if width == 320 and page.locator("#drawer-close").is_visible():
            page.locator("#drawer-close").evaluate("node => node.click()")
            page.wait_for_timeout(300)
        assert page.locator("#panel-metrics").bounding_box()["width"] <= width
        if width in (320, 1920):
            page.screenshot(path=f"/tmp/helios-metrics-{width}.png", full_page=True)
    assert page.locator("#metric-cpu-chart .chart-line").first.get_attribute("points")


def test_new_run_clears_old_capacity_and_nulls_do_not_become_zero(metrics_page: Page):
    page = metrics_page
    page.evaluate("""() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
      generation: 2, profiles: [{profile_id: 'minecraft', state: 'running', pid: 43,
      started_at: '2026-09-09T00:00:00Z', cpu_percent: null, rss_bytes: null,
      players_online: null, required_ports_ready: false}]
    }}))""")
    assert page.locator("#metric-cpu-current").inner_text() == "—"
    assert page.locator("#metric-memory-current").inner_text() == "—"
    assert page.locator("#metric-cpu-capacity").inner_text().startswith("Capacity unavailable")
    assert "NaN" not in (page.locator("#metric-cpu-chart").text_content() or "")


def test_readiness_connector_never_intersects_text(metrics_page: Page):
    page = metrics_page
    page.goto(page.url.split("#")[0])
    for width in (320, 768, 1920):
        page.set_viewport_size({"width": width, "height": 900})
        geometry = page.locator("#session-runway li").evaluate_all("""nodes => nodes.map(node => {
          const text=node.querySelector('strong').getBoundingClientRect();
          const mark=node.querySelector('.runway-mark').getBoundingClientRect();
          return {textTop:text.top,textLeft:text.left,markBottom:mark.bottom,markRight:mark.right};
        })""")
        assert all(g["textLeft"] >= g["markRight"] or g["textTop"] >= g["markBottom"] for g in geometry)


def test_chart_has_real_gaps_and_time_spacing_after_reload(metrics_page: Page):
    page = metrics_page
    series = page._metrics_series["cpu_percent"]
    start = datetime.fromisoformat(page._metrics_status["profiles"][0]["started_at"])
    series[:] = [{"ts": (start + timedelta(minutes=i)).isoformat(), "value": value, "state": "available"}
                 for i, value in [(1, 10), (2, 20), (3, None), (4, 0), (5, 15), (30, 80), (31, 70)]]
    page.reload()
    page.wait_for_function("document.querySelector('#metrics-run-note').textContent.includes('Retained')")
    lines = page.locator("#metric-cpu-chart .chart-line").evaluate_all("nodes=>nodes.map(node=>node.getAttribute('points'))")
    assert len(lines) >= 3
    assert all("NaN" not in line for line in lines)
    assert len(lines[0].split()) == 2
    assert any(",90.0" in line for line in lines)
    assert "since" in page.locator("#metric-cpu-chart").get_attribute("aria-label")


def test_stopped_and_unknown_capacity_do_not_fabricate_history(metrics_page: Page):
    page = metrics_page
    item = page._metrics_status["profiles"][0]
    item.update(state="stopped", pid=None, started_at=None, cpu_percent=None, rss_bytes=None)
    page.reload()
    # A stopped cold load renders "Server stopped · no active run" only until the
    # bounded cold history fetch resolves; asserting that transient line races
    # the route latency, so wait for the settled, truthful history label.
    page.wait_for_function(
        """() => {
          const note = document.querySelector('#metrics-history-note');
          const text = note ? note.textContent : '';
          return text.includes('last observed') || text.includes('no usable observations')
            || text.includes('unavailable right now');
        }"""
    )
    assert page.locator('[data-metric="cpu"]').is_visible()
    # This fixture serves retained history, so the settled view is the labelled
    # recent view: real last-observed timestamps, no fabricated offline zeros,
    # and still-unknown capacity for a stopped profile.
    note = page.locator("#metrics-history-note").inner_text()
    assert "last observed" in note
    assert "may include more than one server run" in note
    run_note = page.locator("#metrics-run-note").inner_text()
    assert "(last observed sample)" in run_note
    assert "→ now" not in run_note
    points = page.locator("#metric-cpu-chart .chart-line").get_attribute("points")
    assert points and "NaN" not in points
    assert page.locator("#metric-cpu-current").inner_text() == "12.0% (historical)"
    assert page.locator("#metric-memory-current").inner_text() == "6.0 GiB (historical)"
    assert page.locator("#metric-players-current").inner_text() == "Offline"
    assert page.locator("#rail-cpu-capacity").get_attribute("aria-valuenow") is None
    assert "/ 0" not in page.locator("#rail-cpu").inner_text()


def test_capacity_on_console_does_not_fetch_history(metrics_page: Page):
    page = metrics_page
    requests = []
    page.on("request", lambda r: requests.append(r.url))
    page.goto(page.url.split("#")[0] + "#/servers/minecraft/console")
    page.wait_for_function("document.querySelector('#rail-cpu').textContent.includes('400%')")
    assert not any("/stats/tps" in url for url in requests)
