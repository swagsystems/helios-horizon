"""Late responses must not redirect visible controls or lose schedule edits."""

from __future__ import annotations

import copy

import pytest
from playwright.sync_api import Page, expect

from test_dashboard import page  # noqa: F401


def isolate_responses(page: Page):
    """Keep unrelated SSE/poll refreshes from concealing a stale repaint."""
    page.add_init_script("""(() => {
        window.EventSource = class {
            static CONNECTING = 0; static OPEN = 1; static CLOSED = 2;
            constructor() { this.readyState = 1; }
            addEventListener() {}
            close() { this.readyState = 2; }
        };
        window.setInterval = () => 0;
        window.__consumedResponses = [];
        const realFetch = window.fetch.bind(window);
        window.fetch = async (...args) => {
            const response = await realFetch(...args);
            const readJson = response.json.bind(response);
            response.json = async () => {
                const body = await readJson();
                // Run after the awaiting API/render microtasks have completed.
                setTimeout(() => window.__consumedResponses.push(String(args[0])), 0);
                return body;
            };
            return response;
        };
    })();""")
    page.reload()
    page.wait_for_function("window.__horizonTest !== undefined")
    page.evaluate("window.__horizonTest.load()")


@pytest.mark.parametrize("tab", ["console", "logs"])
def test_late_logs_cannot_repaint_a_different_selected_profile(page: Page, tab: str):
    isolate_responses(page)
    held = []

    def hold_logs(route):
        held.append(route)
        page.evaluate("window.__heldLogs = true")

    page.route("**/api/v1/profiles/minecraft/logs?*", hold_logs)
    page.route("**/api/v1/profiles/pz-rising/logs?*", lambda route: route.fulfill(json={
        "items": [{"timestamp": "2026-09-21T12:00:00Z", "severity": "info", "message": "Current profile log"}],
        "next_cursor": None,
    }))

    with page.expect_request("**/api/v1/profiles/minecraft/logs?*"):
        page.evaluate("tab => { location.hash = '#/servers/minecraft/' + tab; }", tab)
    page.wait_for_function("window.__heldLogs === true")
    assert len(held) == 1
    page.evaluate("tab => { location.hash = '#/servers/pz-rising/' + tab; }", tab)
    page.wait_for_function("window.__consumedResponses.some(path => path.includes('/pz-rising/logs?'))")
    expect(page.locator("#detail-title")).to_have_text("Project Zomboid")
    expect(page.locator("#detail-status .status-text")).to_have_text("Stopped")

    held[0].fulfill(json={
        "items": [{"timestamp": "2026-09-21T11:59:00Z", "severity": "info", "message": "Previous profile log"}],
        "next_cursor": None,
    })
    page.wait_for_function("window.__consumedResponses.some(path => path.includes('/minecraft/logs?'))")

    actual = page.evaluate("""() => ({
        hash: location.hash,
        title: document.querySelector('#detail-title').textContent,
        status: document.querySelector('#detail-status .status-text').textContent,
        stopVisible: !document.querySelector('#detail-stop').hidden,
        log: document.querySelector('#detail-log-list').textContent,
    })""")
    assert {key: actual[key] for key in ("hash", "title", "status", "stopVisible")} == {
        "hash": f"#/servers/pz-rising/{tab}",
        "title": "Project Zomboid",
        "status": "Stopped",
        "stopVisible": False,
    }, actual
    assert "Current profile log" in actual["log"]
    assert "Previous profile log" not in actual["log"]


def test_schedule_editor_prevents_overlapping_whole_book_replacements(page: Page):
    isolate_responses(page)
    entries = [
        {"cron": "0 1 * * *", "profile": "minecraft", "operation": "backup", "backup_destination": "local", "enabled": True},
        {"cron": "0 2 * * *", "profile": "pz-rising", "operation": "backup", "backup_destination": "local", "enabled": True},
    ]
    posted = []
    held = []

    def schedules(route):
        if route.request.method == "POST":
            posted.append(copy.deepcopy(route.request.post_data_json["entries"]))
            held.append(route)
        else:
            route.fulfill(json={"schedules": entries})

    page.route("**/api/v1/schedules", schedules)
    page.on("dialog", lambda dialog: dialog.accept())
    page.evaluate("location.hash = '#/servers/minecraft/config'")
    expect(page.locator("[data-schedule-toggle]")).to_have_count(2)
    with page.expect_request(lambda request: request.method == "POST" and request.url.endswith("/schedules")):
        page.locator('[data-schedule-toggle="0"]').click()

    # A native click on a disabled control is inert; an unlocked editor will
    # instead submit another complete book based on the pre-save snapshot.
    page.locator('[data-schedule-toggle="1"]').evaluate("button => button.click()")
    page.evaluate("() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))")
    concurrent = copy.deepcopy(posted)
    for index, route in enumerate(held):
        route.fulfill(json={"schedules": posted[index]})
    assert len(concurrent) == 1, {
        "message": "A second whole-book write can silently undo the pending edit",
        "submitted_enabled_values": [[entry["enabled"] for entry in book] for book in concurrent],
    }

    expect(page.locator('[data-schedule-toggle="0"]')).to_have_attribute("aria-checked", "false")
    expect(page.locator('[data-schedule-toggle="1"]')).to_be_enabled()
    with page.expect_request(lambda request: request.method == "POST" and request.url.endswith("/schedules")):
        page.locator('[data-schedule-toggle="1"]').click()
    held[-1].fulfill(json={"schedules": posted[-1]})
    assert [entry["enabled"] for entry in posted[-1]] == [False, False]
    expect(page.locator('[data-schedule-toggle="1"]')).to_have_attribute("aria-checked", "false")


@pytest.mark.parametrize("destination", ["#/servers/pz-rising/logs", "#/servers/minecraft/metrics"])
def test_late_log_errors_do_not_interrupt_another_view_and_can_retry(page: Page, destination: str):
    isolate_responses(page)
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    held = []

    def logs(route):
        held.append(route)
        page.evaluate("count => { window.__heldLogCount = count; }", len(held))

    page.route("**/api/v1/profiles/minecraft/logs?*", logs)
    page.evaluate("location.hash = '#/servers/minecraft/logs'")
    page.wait_for_function("window.__heldLogCount === 1")
    page.evaluate("hash => { location.hash = hash; }", destination)
    selected_title = "Project Zomboid" if "pz-rising" in destination else "Minecraft"
    expect(page.locator("#detail-title")).to_have_text(selected_title)
    page.evaluate("document.querySelector('#status-announcer').textContent = 'Current view notice'")
    held[0].fulfill(status=400, json={"error": {"message": "Previous profile logs unavailable"}})
    page.wait_for_function("window.__consumedResponses.some(path => path.includes('/minecraft/logs?'))")
    assert page.locator("#detail-title").text_content() == selected_title
    assert page.locator("#status-announcer").text_content() == "Current view notice"

    page.evaluate("location.hash = '#/servers/minecraft/logs'")
    page.wait_for_function("window.__heldLogCount === 2")
    held[1].fulfill(json={"items": [{"timestamp": "2026-09-21T12:00:00Z", "severity": "info", "message": "Recovered log"}], "next_cursor": None})
    expect(page.locator("#detail-log-list")).to_contain_text("Recovered log")


def test_late_log_success_is_cached_and_navigation_back_fetches_again(page: Page):
    isolate_responses(page)
    held = []

    def logs(route):
        held.append(route)
        page.evaluate("count => { window.__heldLogCount = count; }", len(held))

    page.route("**/api/v1/profiles/minecraft/logs?*", logs)
    page.evaluate("location.hash = '#/servers/minecraft/logs'")
    page.wait_for_function("window.__heldLogCount === 1")
    page.evaluate("location.hash = '#/'")
    expect(page.locator("#dashboard-view")).to_be_visible()
    held[0].fulfill(json={"items": [{"timestamp": "2026-09-21T12:00:00Z", "severity": "info", "message": "Cached log"}], "next_cursor": None})
    page.wait_for_function("window.__consumedResponses.some(path => path.includes('/minecraft/logs?'))")
    page.evaluate("location.hash = '#/servers/minecraft/logs'")
    page.wait_for_function("window.__heldLogCount === 2")
    assert "since=" in held[1].request.url
    held[1].fulfill(json={"items": [], "next_cursor": None})
    expect(page.locator("#detail-log-list")).to_contain_text("Cached log")


@pytest.fixture
def schedule_editor(page: Page):
    isolate_responses(page)
    entries = [
        {"cron": "0 1 * * *", "profile": "minecraft", "operation": "backup", "backup_destination": "local", "enabled": True},
        {"cron": "0 2 * * *", "profile": "pz-rising", "operation": "backup", "backup_destination": "local", "enabled": True},
        {"cron": "0 3 * * *", "profile": "minecraft", "operation": "future-operation", "enabled": True},
    ]
    server = {"entries": entries, "posts": [], "gets": [], "hold_gets": False}

    def schedules(route):
        if route.request.method == "POST":
            server["posts"].append(route)
            page.evaluate("count => { window.__schedulePosts = count; }", len(server["posts"]))
        elif server["hold_gets"]:
            server["gets"].append(route)
            page.evaluate("count => { window.__scheduleGets = count; }", len(server["gets"]))
        else:
            route.fulfill(json={"schedules": server["entries"]})

    page.route("**/api/v1/schedules", schedules)
    page.on("dialog", lambda dialog: dialog.accept())
    page.evaluate("location.hash = '#/servers/minecraft/config'")
    expect(page.locator("[data-schedule-toggle]")).to_have_count(3)
    page.locator("#schedule-cron").fill("0 4 * * *")
    return page, server


def test_schedule_busy_state_covers_all_edit_controls_and_failure_unlocks(schedule_editor):
    page, server = schedule_editor
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    page.locator('[data-schedule-toggle="0"]').click()
    page.wait_for_function("window.__schedulePosts === 1")
    controls = "#schedule-form input, #schedule-form select, #schedule-form button, #schedule-list button"
    assert page.locator(controls).evaluate_all("nodes => nodes.every(node => node.disabled)")
    expect(page.locator("#schedule-status")).to_have_text("Saving schedule changes…")
    expect(page.locator(".schedule-settings")).to_have_attribute("aria-busy", "true")
    page.locator("#schedule-form").evaluate("form => form.dispatchEvent(new Event('submit', {bubbles:true, cancelable:true}))")
    server["posts"][0].fulfill(status=400, json={"error": {"message": "Schedule save rejected"}})
    expect(page.locator("#schedule-status")).to_have_text("Schedule save rejected")
    assert len(server["posts"]) == 1
    expect(page.locator(".schedule-settings")).to_have_attribute("aria-busy", "false")
    expect(page.locator("#schedule-submit")).to_be_enabled()
    expect(page.locator('[data-schedule-toggle="0"]')).to_be_enabled()
    expect(page.locator('[data-schedule-toggle="2"]')).to_be_disabled()
    expect(page.locator('[data-schedule-remove="2"]')).to_be_disabled()
    assert page.locator("#schedule-cron").input_value() == "0 4 * * *"


@pytest.mark.parametrize("response_status", [200, 400])
def test_late_schedule_refresh_cannot_overwrite_a_successful_edit(schedule_editor, response_status: int):
    page, server = schedule_editor
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    server["hold_gets"] = True
    page.evaluate("location.hash = '#/servers/pz-rising/config'")
    page.wait_for_function("window.__scheduleGets === 1")
    page.locator('[data-schedule-toggle="0"]').click()
    page.wait_for_function("window.__schedulePosts === 1")
    post = server["posts"][0]
    saved = post.request.post_data_json["entries"]
    post.fulfill(json={"schedules": saved})
    expect(page.locator('[data-schedule-toggle="0"]')).to_have_attribute("aria-checked", "false")
    page.evaluate("window.__consumedResponses = []")
    if response_status == 200:
        server["gets"][0].fulfill(json={"schedules": server["entries"]})
    else:
        server["gets"][0].fulfill(status=400, json={"error": {"message": "Stale list error"}})
    page.wait_for_function("window.__consumedResponses.includes('/api/v1/schedules')")
    assert page.locator('[data-schedule-toggle="0"]').get_attribute("aria-checked") == "false"
    assert page.locator("#schedule-status").text_content() == "Schedule disabled for Minecraft."
    page.locator('[data-schedule-toggle="1"]').click()
    page.wait_for_function("window.__schedulePosts === 2")
    assert [entry["enabled"] for entry in server["posts"][1].request.post_data_json["entries"]] == [False, False, True]
    server["posts"][1].fulfill(json={"schedules": server["posts"][1].request.post_data_json["entries"]})


def test_schedule_navigation_during_save_keeps_editor_locked(schedule_editor):
    page, server = schedule_editor
    page.locator('[data-schedule-toggle="0"]').click()
    page.wait_for_function("window.__schedulePosts === 1")
    server["hold_gets"] = True
    page.evaluate("location.hash = '#/servers/pz-rising/config'")
    expect(page.locator("#detail-title")).to_have_text("Project Zomboid")
    expect(page.locator("#config-panel [data-config-key]")).to_have_count(3)
    assert page.locator('[data-schedule-toggle="0"]').is_disabled()
    assert server["gets"] == []
    saved = server["posts"][0].request.post_data_json["entries"]
    server["posts"][0].fulfill(json={"schedules": saved})
    expect(page.locator('[data-schedule-toggle="0"]')).to_have_attribute("aria-checked", "false")
    expect(page.locator('[data-schedule-toggle="0"]')).to_be_enabled()


def test_cancelled_schedule_confirmation_leaves_editor_usable(schedule_editor):
    page, server = schedule_editor
    # Replace acceptance without relying on a platform-native dialog.
    page.evaluate("window.confirm = () => false")
    page.locator('[data-schedule-toggle="0"]').click()
    assert server["posts"] == []
    expect(page.locator('[data-schedule-toggle="0"]')).to_be_enabled()
    expect(page.locator("#schedule-submit")).to_be_enabled()
