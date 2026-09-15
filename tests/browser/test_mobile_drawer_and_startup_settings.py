"""Regressions for the mobile nav drawer and the startup-estimate setting.

Coverage is deliberately behavioural: real clicks, real checkbox changes, real
same-origin tabs, and real visibility transitions against the served shell.
"""
from __future__ import annotations

from pathlib import Path

from playwright.sync_api import Page, expect

from test_dashboard import page  # noqa: F401  (fixture)
from test_startup_estimates import (  # noqa: F401  (fixtures/helpers)
    FAKE_CLOCK_AND_STREAM,
    enable_estimate,
    estimate_page,
    starting_snapshot,
)


SENTENCE = "This tab notice is transient; the Audit trail is durable."
REPO_ROOT = Path(__file__).resolve().parents[2]


def mobile_page(page: Page, width: int, height: int) -> Page:
    page.set_viewport_size({"width": width, "height": height})
    page.reload()
    page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
    return page


def open_drawer(page: Page) -> None:
    page.get_by_role("button", name="Open navigation").click()
    expect(page.locator("#sidebar")).to_have_attribute("aria-hidden", "false")
    # The drawer slides in; wait for the transform to settle before pointing the
    # mouse or asserting reachability.
    page.wait_for_function("() => document.querySelector('#sidebar').getBoundingClientRect().x >= 0")


def dispatch_status(page: Page, snapshot: dict) -> None:
    """Feed one status snapshot through the app's existing status event."""
    page.evaluate("data => window.dispatchEvent(new MessageEvent('game-control-status', {data}))", snapshot)


def open_settings(page: Page) -> None:
    page.evaluate("() => { window.location.hash = '#/settings'; }")
    page.wait_for_function("() => !document.querySelector('#settings-view').hidden")


def open_dashboard(page: Page) -> None:
    page.evaluate("() => { window.location.hash = '#/'; }")
    page.wait_for_function("() => !document.querySelector('#dashboard-view').hidden")


def emit_status(page: Page, snapshot: dict) -> None:
    """Push one live status frame from the newest stream (reconnects create one)."""
    page.evaluate("data => window.__sources.at(-1).emit('status', data)", snapshot)
    page.wait_for_function("document.querySelector('#conn-state').dataset.state === 'live'")


def raw_emit(page: Page, event: str, payload: dict) -> None:
    """Push one frame without requiring the app to accept or react to it."""
    page.evaluate("args => window.__sources.at(-1).emit(args[0], args[1])", [event, payload])


def hide_tab(page: Page) -> None:
    page.evaluate("""() => {
        window.__visibility = 'hidden';
        document.dispatchEvent(new Event('visibilitychange'));
    }""")


def become_visible(page: Page) -> None:
    """Return from a hidden tab and wait for the resumed stream to exist."""
    open_sources = page.evaluate("() => window.__sources.length")
    page.evaluate("""() => {
        window.__visibility = 'visible';
        document.dispatchEvent(new Event('visibilitychange'));
    }""")
    page.wait_for_function("count => window.__sources.length > count", arg=open_sources)


def learning_snapshot(page: Page) -> dict:
    return starting_snapshot(page, startup_estimate={
        "sample_count": 3,
        "median_seconds": None,
        "attempt_id": "attempt-one",
        "elapsed_seconds": 12.0,
    })


def test_mobile_drawer_scrolls_to_reach_the_footer(page: Page):
    mobile_page(page, 320, 480)
    open_drawer(page)
    drawer = page.locator("#sidebar")
    metrics = drawer.evaluate("el => ({scrollHeight: el.scrollHeight, clientHeight: el.clientHeight})")
    assert metrics["scrollHeight"] > metrics["clientHeight"], metrics
    drawer.evaluate("el => { el.scrollTop = 0; }")
    page.mouse.move(160, 240)
    page.mouse.wheel(0, 400)
    page.wait_for_function("() => document.querySelector('#sidebar').scrollTop > 0")
    assert drawer.evaluate("el => el.scrollTop") > 0
    # The footer session note is reachable inside the drawer instead of being cut
    # off below the viewport.
    drawer.evaluate("el => { el.scrollTop = el.scrollHeight; }")
    footer = page.locator("#sidebar .sidebar-session").bounding_box()
    assert footer is not None and footer["y"] >= 0 and footer["y"] + footer["height"] <= 481, footer
    assert page.evaluate("() => window.scrollY") == 0


def test_mobile_drawer_scrolls_by_keyboard_in_landscape(page: Page):
    mobile_page(page, 480, 320)
    open_drawer(page)
    drawer = page.locator("#sidebar")
    assert drawer.evaluate("el => el.scrollHeight") > drawer.evaluate("el => el.clientHeight")
    assert page.evaluate("() => document.activeElement.id") == "drawer-close"
    for _ in range(4):
        page.keyboard.press("ArrowDown")
    page.wait_for_timeout(50)
    assert drawer.evaluate("el => el.scrollTop") > 0


def test_mobile_drawer_escape_restores_focus_and_page_scroll(page: Page):
    mobile_page(page, 320, 480)
    open_drawer(page)
    assert page.evaluate("() => document.body.classList.contains('drawer-open')")
    assert page.evaluate("() => getComputedStyle(document.body).overflow") == "hidden"
    assert page.evaluate("() => document.querySelector('#sidebar').inert") is False
    page.keyboard.press("Escape")
    expect(page.locator("#sidebar")).to_have_attribute("aria-hidden", "true")
    assert page.evaluate("() => document.activeElement.id") == "menu-toggle"
    assert page.evaluate("() => document.querySelector('#sidebar').inert") is True
    page.evaluate("() => window.scrollTo(0, 300)")
    assert page.evaluate("() => window.scrollY") > 0


def test_desktop_sidebar_keeps_static_layout_and_page_scroll(page: Page):
    assert page.locator("#menu-toggle").is_hidden()
    assert page.evaluate("() => getComputedStyle(document.querySelector('#sidebar')).overflowY") == "visible"
    page.evaluate("() => { document.querySelector('.main-pane').scrollTop = 400; }")
    assert page.evaluate("() => document.querySelector('.main-pane').scrollTop") > 0
    assert page.evaluate("() => document.body.classList.contains('drawer-open')") is False


def test_tab_notice_sentence_is_removed_and_audit_stays_reachable(page: Page):
    app_js = (REPO_ROOT / "web" / "app.js").read_text()
    assert SENTENCE not in app_js
    assert SENTENCE not in page.content()
    assert page.locator("[data-view='audit']").first.is_visible()


def test_start_and_readiness_messages_stay_meaningful_without_the_sentence(page: Page):
    page.route(
        "**/api/v1/profiles/minecraft/start",
        lambda route: route.fulfill(json={"job_id": "job-42", "state": "running"}),
    )
    dispatch_status(page, starting_snapshot(
        page, state="stopped", health="unknown", slot_owner=None, pid=None,
        required_ports_ready=False, startup_estimate=None,
    ))
    page.locator("#session-primary").click()
    page.wait_for_function("document.querySelector('#session-operation').dataset.result === 'accepted'")
    operation = page.locator("#session-operation").inner_text()
    assert "Start accepted by Horizon. Job job-42." in operation
    assert SENTENCE not in operation
    dispatch_status(page, starting_snapshot(
        page, state="running", health="healthy", slot_owner="minecraft", pid=202,
        required_ports_ready=True, startup_estimate=None,
    ))
    assert page.locator("#session-operation").inner_text() == "Minecraft is ready."


def test_write_denied_storage_keeps_the_local_choice_over_a_stored_off_value(estimate_page: Page):
    page = estimate_page
    open_settings(page)
    page.evaluate("""() => {
        localStorage.setItem('helios-startup-estimate', '0');
        Object.getPrototypeOf(localStorage).setItem = function () {
            throw new DOMException('denied', 'SecurityError');
        };
    }""")
    page.locator("#startup-estimate-toggle").check()
    # The write really failed, so the stored value still reads "off".
    assert page.evaluate("() => localStorage.getItem('helios-startup-estimate')") == "0"
    assert page.evaluate("() => document.querySelector('#startup-estimate-toggle').checked") is True
    open_dashboard(page)
    emit_status(page, learning_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_visible()
    track = page.locator("#startup-estimate-track")
    expect(track).to_have_attribute("data-mode", "learning")
    assert track.get_attribute("aria-valuenow") is None


def test_setting_syncs_across_same_origin_tabs(estimate_page: Page):
    page = estimate_page
    open_settings(page)
    page.locator("#startup-estimate-toggle").check()
    other = page.context.new_page()
    other.add_init_script(FAKE_CLOCK_AND_STREAM)
    other.goto(page.url)
    other.wait_for_selector("#startup-estimate-toggle")
    expect(other.locator("#startup-estimate-toggle")).to_be_checked()

    other.locator("#startup-estimate-toggle").uncheck()
    page.wait_for_function("() => document.querySelector('#startup-estimate-toggle').checked === false")
    assert page.evaluate("() => window.__horizonTest.startupEstimateState().enabled") is False
    open_dashboard(page)
    emit_status(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_hidden()

    other.locator("#startup-estimate-toggle").check()
    page.wait_for_function("() => document.querySelector('#startup-estimate-toggle').checked === true")
    assert page.evaluate("() => window.__horizonTest.startupEstimateState().enabled") is True
    emit_status(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate-track")).to_have_attribute("data-mode", "estimated")
    other.close()


def test_estimate_recovers_after_the_tab_was_hidden_and_wake_up_fails(estimate_page: Page):
    page = estimate_page
    # The wake-up path deliberately fails one REST request, so the expected
    # network console noise is tolerated here.
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    enable_estimate(page)
    emit_status(page, starting_snapshot(page))
    track = page.locator("#startup-estimate-track")
    expect(track).to_have_attribute("data-mode", "estimated")

    page.evaluate("""() => {
        window.__visibility = 'hidden';
        document.dispatchEvent(new Event('visibilitychange'));
    }""")
    expect(page.locator("#startup-estimate")).to_be_hidden()

    # Wake-up where the one-shot resume snapshot cannot be fetched while the live
    # stream returns: the read-only track must recover without a reload.
    page.route("**/api/v1/status", lambda route: route.abort())
    become_visible(page)
    emit_status(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_visible()
    expect(track).to_have_attribute("data-mode", "estimated")

    # Focus recovery follows the same path once the snapshot succeeds again.
    page.evaluate("""() => {
        window.__visibility = 'hidden';
        document.dispatchEvent(new Event('visibilitychange'));
    }""")
    expect(page.locator("#startup-estimate")).to_be_hidden()
    page.unroute("**/api/v1/status")
    become_visible(page)
    page.evaluate("() => window.dispatchEvent(new Event('focus'))")
    emit_status(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_visible()
    expect(track).to_have_attribute("data-mode", "estimated")


def test_estimate_setting_survives_routes_and_reload(estimate_page: Page):
    page = estimate_page
    open_settings(page)
    page.locator("#startup-estimate-toggle").check()
    open_dashboard(page)
    emit_status(page, learning_snapshot(page))
    track = page.locator("#startup-estimate-track")
    expect(page.locator("#startup-estimate")).to_be_visible()
    expect(track).to_have_attribute("data-mode", "learning")

    page.evaluate("() => { window.location.hash = '#/backups'; }")
    page.evaluate("() => { window.location.hash = '#/'; }")
    expect(page.locator("#startup-estimate")).to_be_visible()
    expect(track).to_have_attribute("data-mode", "learning")

    page.reload()
    page.wait_for_function("window.__sources?.length >= 1")
    expect(page.locator("#startup-estimate-toggle")).to_be_checked()
    emit_status(page, learning_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_visible()
    expect(track).to_have_attribute("data-mode", "learning")


def test_learning_copy_states_the_five_start_requirement(estimate_page: Page):
    page = estimate_page
    enable_estimate(page)
    emit_status(page, learning_snapshot(page))
    note = page.locator("#startup-estimate-note")
    expect(note).to_have_text("Learning startup time… 3 of 5 starts recorded")
    assert page.locator("#startup-estimate-track").get_attribute("aria-valuenow") is None
    # No recorded count is reported as unknown, never as a fabricated number.
    emit_status(page, starting_snapshot(page, startup_estimate={
        "attempt_id": "attempt-two",
        "elapsed_seconds": 4.0,
    }))
    expect(note).to_have_text("Learning startup time… 5 successful starts needed")


def test_hidden_tab_stays_hidden_through_preference_changes_and_rerenders(estimate_page: Page):
    page = estimate_page
    enable_estimate(page)
    emit_status(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate-track")).to_have_attribute("data-mode", "estimated")
    hide_tab(page)
    expect(page.locator("#startup-estimate")).to_be_hidden()
    # The cached pre-hide status is still inside the freshness window, so only
    # the fence keeps incidental work from re-displaying it.
    page.evaluate("() => window.__horizonTest.patchActiveSlot()")
    open_settings(page)
    page.locator("#startup-estimate-toggle").uncheck()
    page.locator("#startup-estimate-toggle").check()
    open_dashboard(page)
    page.evaluate("() => window.__horizonTest.patchActiveSlot()")
    expect(page.locator("#startup-estimate")).to_be_hidden()
    # Only a newly accepted status sample brings it back.
    become_visible(page)
    emit_status(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_visible()


def test_resume_without_a_new_sample_stays_hidden(estimate_page: Page):
    page = estimate_page
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    enable_estimate(page)
    emit_status(page, starting_snapshot(page))
    hide_tab(page)
    page.route("**/api/v1/status", lambda route: route.abort())
    become_visible(page)
    page.evaluate("() => window.__horizonTest.patchActiveSlot()")
    raw_emit(page, "heartbeat", {})
    expect(page.locator("#startup-estimate")).to_be_hidden()
    emit_status(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_visible()


def test_disconnect_then_rerender_stays_hidden_until_a_new_sample(estimate_page: Page):
    page = estimate_page
    enable_estimate(page)
    emit_status(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate-track")).to_have_attribute("data-mode", "estimated")
    # A dropped stream invalidates the cached sample even before it ages out.
    page.evaluate("() => window.__sources.at(-1).onerror(new Event('error'))")
    page.wait_for_function("() => ['reconnecting', 'offline'].includes(document.querySelector('#conn-state').dataset.state)")
    expect(page.locator("#startup-estimate")).to_be_hidden()
    open_settings(page)
    page.locator("#startup-estimate-toggle").uncheck()
    page.locator("#startup-estimate-toggle").check()
    open_dashboard(page)
    page.evaluate("() => window.__horizonTest.patchActiveSlot()")
    expect(page.locator("#startup-estimate")).to_be_hidden()
    emit_status(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_visible()


def test_malformed_unrelated_and_heartbeat_frames_never_revive_the_track(estimate_page: Page):
    page = estimate_page
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    enable_estimate(page)
    emit_status(page, starting_snapshot(page))
    hide_tab(page)
    page.route("**/api/v1/status", lambda route: route.abort())
    become_visible(page)
    malformed = starting_snapshot(page)
    malformed["profiles"][0]["state"] = "exploded"
    raw_emit(page, "status", malformed)
    unrelated = starting_snapshot(page)
    unrelated["profiles"] = [
        {**profile, "profile_id": f"not-configured-{index}"}
        for index, profile in enumerate(unrelated["profiles"])
    ]
    raw_emit(page, "status", unrelated)
    raw_emit(page, "heartbeat", {})
    page.wait_for_timeout(100)
    expect(page.locator("#startup-estimate")).to_be_hidden()
    emit_status(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_visible()


def test_foreign_storage_events_are_ignored_and_clears_reset_the_default(estimate_page: Page):
    page = estimate_page
    enable_estimate(page)
    emit_status(page, starting_snapshot(page))
    # A sessionStorage event from another frame carries the same key but is not
    # this opt-in's storage area.
    page.evaluate("""() => {
        window.dispatchEvent(new StorageEvent('storage', {
            key: 'helios-startup-estimate', oldValue: '1', newValue: '0', storageArea: sessionStorage,
        }));
    }""")
    expect(page.locator("#startup-estimate-toggle")).to_be_checked()
    expect(page.locator("#startup-estimate")).to_be_visible()
    # A real localStorage clear in another tab resets this tab to the default.
    other = page.context.new_page()
    other.add_init_script(FAKE_CLOCK_AND_STREAM)
    other.goto(page.url)
    other.wait_for_selector("#startup-estimate-toggle", state="attached")
    other.evaluate("() => localStorage.clear()")
    page.wait_for_function("() => document.querySelector('#startup-estimate-toggle').checked === false")
    assert page.evaluate("() => window.__horizonTest.startupEstimateState().enabled") is False
    other.close()


def test_denied_storage_getter_keeps_the_explicit_choice_through_foreign_events(estimate_page: Page):
    page = estimate_page
    open_settings(page)
    page.evaluate("""() => {
        Object.defineProperty(window, 'localStorage', {
            configurable: true,
            get() { throw new DOMException('denied', 'SecurityError'); },
        });
    }""")
    page.locator("#startup-estimate-toggle").check()
    # An area-less or sessionStorage event is not this opt-in's storage area and
    # must not clear the explicit in-memory choice.
    page.evaluate("""() => {
        window.dispatchEvent(new StorageEvent('storage', {
            key: 'helios-startup-estimate', oldValue: null, newValue: '0', storageArea: sessionStorage,
        }));
        window.dispatchEvent(new StorageEvent('storage', {
            key: 'helios-startup-estimate', oldValue: null, newValue: '0',
        }));
    }""")
    assert page.evaluate("() => document.querySelector('#startup-estimate-toggle').checked") is True
    assert page.evaluate("() => window.__horizonTest.startupEstimateState().enabled") is True
    open_dashboard(page)
    emit_status(page, learning_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_visible()
    expect(page.locator("#startup-estimate-track")).to_have_attribute("data-mode", "learning")
