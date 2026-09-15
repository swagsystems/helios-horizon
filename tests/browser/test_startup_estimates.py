"""Browser coverage for the experimental startup estimate track."""
from __future__ import annotations

import copy
import os
from pathlib import Path

import pytest
from playwright.sync_api import Page, expect

from test_dashboard import page  # noqa: F401


ARTIFACT_DIR_ENV = "HORIZON_STARTUP_ESTIMATE_ARTIFACTS"


FAKE_CLOCK_AND_STREAM = """
  window.__now = 1789000000000; Date.now = () => window.__now;
  window.__visibility = 'visible';
  Object.defineProperty(document, 'visibilityState', {get: () => window.__visibility});
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
    emit(type, payload) {
      this.listeners[type]?.(new MessageEvent(type, {data: JSON.stringify(payload)}));
    }
  };
"""


@pytest.fixture
def estimate_page(page: Page):
    page.add_init_script(FAKE_CLOCK_AND_STREAM)
    page.reload()
    page.wait_for_function("window.__sources?.length === 1 && window.__horizonTest")
    yield page


def enable_estimate(page: Page) -> None:
    page.evaluate("() => localStorage.setItem('helios-startup-estimate', '1')")
    page.reload()
    page.wait_for_function("window.__sources?.length === 1 && window.__horizonTest")


def emit(page: Page, snapshot: dict) -> None:
    page.evaluate("data => window.__sources[0].emit('status', data)", snapshot)
    page.wait_for_function("document.querySelector('#conn-state').dataset.state === 'live'")


def starting_snapshot(page: Page, **overrides) -> dict:
    snapshot = copy.deepcopy(page._dashboard_fixture["status"])  # type: ignore[attr-defined]
    generation = int(getattr(page, "_estimate_generation", 10)) + 1
    page._estimate_generation = generation  # type: ignore[attr-defined]
    snapshot["generation"] = generation
    profile = snapshot["profiles"][0]
    profile.update({
        "state": "starting",
        "health": "unknown",
        "slot_owner": None,
        "pid": None,
        "uptime_seconds": None,
        "started_at": None,
        "required_ports_ready": False,
        "startup_estimate": {
            "sample_count": 6,
            "median_seconds": 60.0,
            "attempt_id": "attempt-one",
            "elapsed_seconds": 10.0,
        },
    })
    profile.update(overrides)
    return snapshot


def estimate_state(page: Page) -> dict:
    return page.evaluate("() => window.__horizonTest.startupEstimateState()")


def test_estimate_is_hidden_until_the_experimental_setting_is_enabled(estimate_page: Page):
    emit(estimate_page, starting_snapshot(estimate_page))
    expect(estimate_page.locator("#startup-estimate")).to_be_hidden()


def test_setting_persists_across_reload_and_defaults_off(estimate_page: Page):
    base = estimate_page.url.split("#", 1)[0]
    estimate_page.goto(f"{base}#/settings")
    toggle = estimate_page.locator("#startup-estimate-toggle")
    expect(toggle).not_to_be_checked()
    toggle.check()
    assert estimate_page.evaluate("() => localStorage.getItem('helios-startup-estimate')") == "1"
    estimate_page.reload()
    estimate_page.wait_for_function("window.__horizonTest")
    expect(estimate_page.locator("#startup-estimate-toggle")).to_be_checked()
    estimate_page.locator("#startup-estimate-toggle").focus()
    assert estimate_page.evaluate("() => document.activeElement.id") == "startup-estimate-toggle"
    estimate_page.locator("#startup-estimate-toggle").uncheck()
    assert estimate_page.evaluate("() => localStorage.getItem('helios-startup-estimate')") == "0"


def test_learning_before_five_samples_has_no_numeric_progress(estimate_page: Page):
    enable_estimate(estimate_page)
    emit(estimate_page, starting_snapshot(
        estimate_page,
        startup_estimate={
            "sample_count": 3,
            "median_seconds": None,
            "attempt_id": "attempt-one",
            "elapsed_seconds": 12.0,
        },
    ))
    track = estimate_page.locator("#startup-estimate-track")
    expect(estimate_page.locator("#startup-estimate")).to_be_visible()
    expect(track).to_have_attribute("data-mode", "learning")
    assert track.get_attribute("aria-valuenow") is None
    expect(estimate_page.locator("#startup-estimate-note")).to_have_text("Learning startup time… 3 of 5 starts recorded")
    expect(estimate_page.locator("#startup-estimate-fill")).to_have_css("width", "0px")


def test_four_of_five_stays_learning_and_five_enables_monotonic_estimate(estimate_page: Page):
    enable_estimate(estimate_page)
    emit(estimate_page, starting_snapshot(
        estimate_page,
        startup_estimate={
            "sample_count": 4,
            "median_seconds": None,
            "attempt_id": "attempt-four",
            "elapsed_seconds": 12.0,
        },
    ))
    track = estimate_page.locator("#startup-estimate-track")
    expect(estimate_page.locator("#startup-estimate-note")).to_have_text("Learning startup time… 4 of 5 starts recorded")
    expect(track).to_have_attribute("data-mode", "learning")
    assert track.get_attribute("aria-valuenow") is None
    # The fifth recorded start turns on numeric progress without training the
    # live server or changing the documented 5-success threshold.
    emit(estimate_page, starting_snapshot(
        estimate_page,
        startup_estimate={
            "sample_count": 5,
            "median_seconds": 60.0,
            "attempt_id": "attempt-five",
            "elapsed_seconds": 30.0,
        },
    ))
    expect(track).to_have_attribute("data-mode", "estimated")
    first = int(track.get_attribute("aria-valuenow"))
    assert 0 < first <= 99
    # Progress is monotonic: more elapsed time never lowers the percentage.
    estimate_page.evaluate("() => { window.__now += 6000; window.__horizonTest.startupEstimateTick(); }")
    advanced = int(track.get_attribute("aria-valuenow"))
    assert advanced > first
    # A later authoritative ready status ends the numeric track at 100.
    ready = copy.deepcopy(estimate_page._dashboard_fixture["status"])  # type: ignore[attr-defined]
    ready["generation"] = int(getattr(estimate_page, "_estimate_generation", 10)) + 1
    estimate_page._estimate_generation = ready["generation"]  # type: ignore[attr-defined]
    emit(estimate_page, ready)
    expect(track).to_have_attribute("data-mode", "ready")
    expect(track).to_have_attribute("aria-valuenow", "100")
    assert "100%" in (estimate_page.locator("#startup-estimate-fill").get_attribute("style") or "")


def test_setting_survives_tab_navigation(estimate_page: Page):
    base = estimate_page.url.split("#", 1)[0]
    estimate_page.goto(f"{base}#/settings")
    toggle = estimate_page.locator("#startup-estimate-toggle")
    toggle.check()
    assert estimate_page.evaluate("() => localStorage.getItem('helios-startup-estimate')") == "1"
    estimate_page.goto(f"{base}#/")
    estimate_page.goto(f"{base}#/settings")
    expect(estimate_page.locator("#startup-estimate-toggle")).to_be_checked()
    assert estimate_page.evaluate("() => window.__horizonTest.startupEstimateState().enabled") is True


def test_missing_elapsed_never_fabricates_progress(estimate_page: Page):
    enable_estimate(estimate_page)
    emit(estimate_page, starting_snapshot(
        estimate_page,
        startup_estimate={
            "sample_count": 9,
            "median_seconds": 60.0,
            "attempt_id": None,
            "elapsed_seconds": None,
        },
    ))
    track = estimate_page.locator("#startup-estimate-track")
    expect(track).to_have_attribute("data-mode", "learning")
    assert track.get_attribute("aria-valuenow") is None


def test_estimate_advances_with_time_and_caps_below_ready(estimate_page: Page):
    enable_estimate(estimate_page)
    emit(estimate_page, starting_snapshot(estimate_page))
    track = estimate_page.locator("#startup-estimate-track")
    expect(track).to_have_attribute("data-mode", "estimated")
    first = int(track.get_attribute("aria-valuenow"))
    assert 15 <= first <= 20
    assert "remaining" in estimate_page.locator("#startup-estimate-note").inner_text()
    # The local timer advances the same render path; a re-anchored status must
    # resume from the server elapsed instead of restarting at zero.
    estimate_page.evaluate("() => { window.__now += 20000; }")
    estimate_page.evaluate("() => window.__horizonTest.startupEstimateTick()")
    advanced = int(track.get_attribute("aria-valuenow"))
    assert advanced == 50
    assert estimate_state(estimate_page)["elapsedAtAnchor"] == 10.0
    expect(track).to_have_attribute("aria-valuenow", "50")
    # A resuming observer adopts the server's elapsed time instead of zero.
    emit(estimate_page, starting_snapshot(
        estimate_page,
        startup_estimate={
            "sample_count": 6,
            "median_seconds": 60.0,
            "attempt_id": "attempt-one",
            "elapsed_seconds": 30.0,
        },
    ))
    assert estimate_state(estimate_page)["elapsedAtAnchor"] == 30.0
    expect(track).to_have_attribute("aria-valuenow", "50")


def test_overrun_shows_bounded_text_and_never_negative_countdown(estimate_page: Page):
    enable_estimate(estimate_page)
    emit(estimate_page, starting_snapshot(
        estimate_page,
        startup_estimate={
            "sample_count": 6,
            "median_seconds": 30.0,
            "attempt_id": "attempt-one",
            "elapsed_seconds": 45.0,
        },
    ))
    track = estimate_page.locator("#startup-estimate-track")
    expect(track).to_have_attribute("data-mode", "overrun")
    expect(track).to_have_attribute("aria-valuenow", "95")
    expect(estimate_page.locator("#startup-estimate-note")).to_have_text("Taking longer than usual…")


def test_ready_gate_requires_authoritative_signals(estimate_page: Page):
    enable_estimate(estimate_page)
    track = estimate_page.locator("#startup-estimate-track")
    # Starting with otherwise-ready fields is NOT readiness.
    emit(estimate_page, starting_snapshot(estimate_page, pid=4242, health="healthy", required_ports_ready=True, slot_owner="minecraft"))
    expect(track).to_have_attribute("data-mode", "estimated")
    assert track.get_attribute("aria-valuenow") != "100"
    # The genuine Starting -> running/healthy/ports/owner transition reaches 100.
    running = starting_snapshot(estimate_page, state="running", pid=4242, health="healthy", required_ports_ready=True, slot_owner="minecraft", startup_estimate=None)
    emit(estimate_page, running)
    expect(track).to_have_attribute("data-mode", "ready")
    expect(track).to_have_attribute("aria-valuenow", "100")
    expect(estimate_page.locator("#startup-estimate-note")).to_have_text("Ready to join")
    assert estimate_state(estimate_page)["ready"] is True
    # Wrong owner, failure, and stop all clear the completed run.
    emit(estimate_page, starting_snapshot(estimate_page, state="failed", startup_estimate=None))
    expect(estimate_page.locator("#startup-estimate")).to_be_hidden()


def test_new_attempt_resets_and_other_states_hide_the_track(estimate_page: Page):
    enable_estimate(estimate_page)
    emit(estimate_page, starting_snapshot(estimate_page))
    assert estimate_state(estimate_page)["attemptId"] == "attempt-one"
    emit(estimate_page, starting_snapshot(
        estimate_page,
        startup_estimate={
            "sample_count": 6,
            "median_seconds": 60.0,
            "attempt_id": "attempt-two",
            "elapsed_seconds": 2.0,
        },
    ))
    assert estimate_state(estimate_page)["attemptId"] == "attempt-two"
    assert estimate_state(estimate_page)["elapsedAtAnchor"] == 2.0
    running = starting_snapshot(estimate_page, state="running", startup_estimate=None)
    emit(estimate_page, running)
    expect(estimate_page.locator("#startup-estimate")).to_be_hidden()


def test_incidental_rerender_keeps_advanced_progress_and_version_resets(estimate_page: Page):
    enable_estimate(estimate_page)
    emit(estimate_page, starting_snapshot(estimate_page))
    track = estimate_page.locator("#startup-estimate-track")
    estimate_page.evaluate("() => { window.__now += 15000; }")
    estimate_page.evaluate("() => window.__horizonTest.startupEstimateTick()")
    expect(track).to_have_attribute("aria-valuenow", "42")
    # An incidental patchActiveSlot (for example a backup response) must not
    # re-anchor the unchanged server sample and pull progress backwards.
    estimate_page.evaluate("() => { window.__now += 5000; }")
    estimate_page.evaluate("() => window.__horizonTest.patchActiveSlot()")
    assert int(track.get_attribute("aria-valuenow")) >= 42
    expect(track).to_have_attribute("aria-valuenow", "50")
    # A version change is a new learning bucket and re-anchors from the server.
    emit(estimate_page, starting_snapshot(
        estimate_page,
        installed_version="1.22.0",
        startup_estimate={
            "sample_count": 6,
            "median_seconds": 60.0,
            "attempt_id": "attempt-one",
            "elapsed_seconds": 5.0,
            "version": "1.22.0",
        },
    ))
    assert estimate_state(estimate_page)["version"] == "1.22.0"
    assert estimate_state(estimate_page)["elapsedAtAnchor"] == 5.0
    expect(track).to_have_attribute("aria-valuenow", "8")


def test_missing_elapsed_metadata_and_stale_tick_stay_non_numeric(estimate_page: Page):
    enable_estimate(estimate_page)
    emit(estimate_page, starting_snapshot(estimate_page, installed_version=None, startup_estimate={
        "sample_count": 0,
        "median_seconds": None,
        "attempt_id": "attempt-one",
        "elapsed_seconds": None,
        "version": None,
    }))
    track = estimate_page.locator("#startup-estimate-track")
    expect(track).to_have_attribute("data-mode", "learning")
    assert track.get_attribute("aria-valuenow") is None
    emit(estimate_page, starting_snapshot(estimate_page))
    expect(track).to_have_attribute("aria-valuenow", "17")
    estimate_page.evaluate("() => { window.__now += 31000; }")
    estimate_page.evaluate("() => window.__horizonTest.startupEstimateTick()")
    expect(estimate_page.locator("#startup-estimate")).to_be_hidden()


def test_new_attempt_without_elapsed_becomes_learning_not_stale_numeric(estimate_page: Page):
    enable_estimate(estimate_page)
    track = estimate_page.locator("#startup-estimate-track")
    emit(estimate_page, starting_snapshot(estimate_page))
    expect(track).to_have_attribute("aria-valuenow", "17")
    emit(estimate_page, starting_snapshot(
        estimate_page,
        startup_estimate={
            "sample_count": 8,
            "median_seconds": 60.0,
            "attempt_id": "attempt-two",
            "elapsed_seconds": None,
            "version": None,
        },
    ))
    expect(track).to_have_attribute("data-mode", "learning")
    assert track.get_attribute("aria-valuenow") is None
    assert estimate_state(estimate_page)["elapsedAtAnchor"] is None


def test_watchdog_clears_ready_estimate_without_a_tick(estimate_page: Page):
    enable_estimate(estimate_page)
    track = estimate_page.locator("#startup-estimate-track")
    # Ready latch (timer stopped) then a stale watchdog pass hides it.
    emit(estimate_page, starting_snapshot(estimate_page))
    emit(estimate_page, starting_snapshot(
        estimate_page, state="running", pid=4242, health="healthy", required_ports_ready=True,
        slot_owner="minecraft", startup_estimate=None,
    ))
    expect(track).to_have_attribute("aria-valuenow", "100")
    estimate_page.evaluate("() => { window.__now += 31000; window.__horizonTest.checkStreamFreshness(); }")
    expect(estimate_page.locator("#startup-estimate")).to_be_hidden()


def test_watchdog_clears_learning_estimate_without_a_tick(estimate_page: Page):
    enable_estimate(estimate_page)
    track = estimate_page.locator("#startup-estimate-track")
    # Learning (no elapsed, no timer) must also drop a stale estimate.
    emit(estimate_page, starting_snapshot(estimate_page, startup_estimate={
        "sample_count": 1,
        "median_seconds": None,
        "attempt_id": "attempt-three",
        "elapsed_seconds": None,
        "version": None,
    }))
    expect(track).to_have_attribute("data-mode", "learning")
    estimate_page.evaluate("() => { window.__now += 31000; window.__horizonTest.checkStreamFreshness(); }")
    expect(estimate_page.locator("#startup-estimate")).to_be_hidden()


def test_interval_dispatch_advances_a_fresh_estimate(estimate_page: Page):
    enable_estimate(estimate_page)
    track = estimate_page.locator("#startup-estimate-track")
    emit(estimate_page, starting_snapshot(estimate_page))
    expect(track).to_have_attribute("aria-valuenow", "17")
    estimate_page.evaluate("() => { window.__now += 6000; }")
    # The real one-second interval advances the estimate; no test tick helper.
    expect(track).to_have_attribute("aria-valuenow", "27")


def test_storage_denied_toggle_still_enables_the_estimate(estimate_page: Page):
    estimate_page.add_init_script(
        "Object.defineProperty(window, 'localStorage', {configurable: true, get() { throw new Error('denied'); }});"
    )
    estimate_page.reload()
    estimate_page.wait_for_function("window.__sources?.length === 1 && window.__horizonTest")
    base = estimate_page.url.split("#", 1)[0]
    estimate_page.goto(f"{base}#/settings")
    toggle = estimate_page.locator("#startup-estimate-toggle")
    expect(toggle).not_to_be_checked()
    toggle.check()
    expect(toggle).to_be_checked()
    estimate_page.goto(f"{base}#/")
    emit(estimate_page, starting_snapshot(estimate_page))
    expect(estimate_page.locator("#startup-estimate")).to_be_visible()
    failed = starting_snapshot(estimate_page, state="failed", startup_estimate=None)
    emit(estimate_page, failed)
    expect(estimate_page.locator("#startup-estimate")).to_be_hidden()


def test_stale_status_clears_the_estimate(estimate_page: Page):
    enable_estimate(estimate_page)
    emit(estimate_page, starting_snapshot(estimate_page))
    expect(estimate_page.locator("#startup-estimate")).to_be_visible()
    estimate_page.evaluate("""() => {
      window.__now += 31000;
      window.__sources.at(-1).emit('heartbeat', {});
      window.__horizonTest.checkStreamFreshness();
    }""")
    estimate_page.evaluate("() => window.__horizonTest.startupEstimateTick()")
    expect(estimate_page.locator("#startup-estimate")).to_be_hidden()


def test_no_extra_lifecycle_requests_and_reduced_motion(estimate_page: Page):
    posts: list[str] = []
    estimate_page.on("request", lambda request: posts.append(request.url) if request.method == "POST" else None)
    estimate_page.emulate_media(reduced_motion="reduce")
    enable_estimate(estimate_page)
    emit(estimate_page, starting_snapshot(estimate_page))
    estimate_page.evaluate("() => { window.__now += 15000; }")
    estimate_page.evaluate("() => window.__horizonTest.startupEstimateTick()")
    expect(estimate_page.locator("#startup-estimate-track")).to_have_attribute("aria-valuenow", "42")
    duration = estimate_page.evaluate(
        "() => getComputedStyle(document.querySelector('.startup-estimate-fill')).transitionDuration"
    )
    assert float(duration.rstrip("s")) <= 0.001
    assert [url for url in posts if "/api/v1/profiles/" in url] == []


@pytest.mark.parametrize("viewport,label", [({"width": 1280, "height": 900}, "desktop"), ({"width": 320, "height": 720}, "mobile")])
def test_render_at_supported_widths_and_capture(page: Page, tmp_path: Path, viewport: dict, label: str):
    page.set_viewport_size(viewport)
    page.add_init_script(FAKE_CLOCK_AND_STREAM)
    page.reload()
    page.wait_for_function("window.__sources?.length === 1 && window.__horizonTest")
    enable_estimate(page)
    emit(page, starting_snapshot(page))
    expect(page.locator("#startup-estimate")).to_be_visible()
    expect(page.locator("#session-runway")).to_be_visible()
    if viewport["width"] == 320:
        assert page.evaluate("() => document.documentElement.scrollWidth") <= 320
        assert page.evaluate(
            "() => Math.round(document.querySelector('#startup-estimate').getBoundingClientRect().width)"
        ) <= 300
    # Screenshots are test artifacts: write under the test temp directory, or to
    # an explicit review directory when the runner sets the override.
    override = os.environ.get(ARTIFACT_DIR_ENV)
    target = Path(override) if override else tmp_path
    target.mkdir(parents=True, exist_ok=True)
    shot = target / f"startup-estimate-{label}.png"
    page.screenshot(path=str(shot))
    assert shot.is_file() and shot.stat().st_size > 0
