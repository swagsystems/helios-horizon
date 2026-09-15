"""Transport heartbeats must not conceal missing application status updates."""
import copy

import pytest
from playwright.sync_api import Page, expect

from test_dashboard import page  # noqa: F401


@pytest.fixture
def stream_page(page: Page):
    page.add_init_script("""
      window.__now = Date.now(); Date.now = () => window.__now;
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
    """)
    page.reload()
    page.wait_for_function("window.__sources?.length === 1 && window.__horizonTest")
    snapshot = copy.deepcopy(page._dashboard_fixture["status"])
    snapshot["generation"] = 20
    starting = copy.deepcopy(snapshot)
    starting["profiles"][0].update(state="starting", pid=202, required_ports_ready=False, health="unknown")
    page.evaluate("data => window.__sources[0].emit('status', data)", starting)
    expect(page.locator("#conn-state")).to_have_attribute("data-state", "live")
    yield page, snapshot


def stale(page):
    page.evaluate("""() => {
      window.__now += 31000;
      window.__sources.at(-1).emit('heartbeat', {});
      window.__horizonTest.checkStreamFreshness();
    }""")


def test_heartbeat_only_stream_recovers_readiness_without_reload(stream_page):
    page, ready = stream_page
    requests = []
    page.route("**/api/v1/status", lambda route: (requests.append(route.request.url), route.fulfill(json=ready)))
    stale(page)
    expect(page.locator('[data-session-phase="ready"]')).to_have_attribute("data-state", "complete")
    expect(page.locator("#conn-state")).to_have_attribute("data-state", "polling")
    assert len(requests) == 1
    assert page.evaluate("window.__sources[0].readyState") == 2


@pytest.mark.parametrize("bad", [None, {}, {"generation": 21, "profiles": []},
    {"generation": 19, "profiles": [{"profile_id": "minecraft", "state": "starting"}]},
    {"generation": 21, "profiles": [{"profile_id": "minecraft"}]}])
def test_invalid_or_regressive_status_does_not_postpone_recovery(stream_page, bad):
    page, ready = stream_page
    page.route("**/api/v1/status", lambda route: route.fulfill(json={**ready, "generation": 30}))
    page.evaluate("window.__now += 20000")
    page.evaluate("data => window.__sources[0].emit('status', data)", bad)
    page.evaluate("window.__now += 11000; window.__horizonTest.checkStreamFreshness()")
    expect(page.locator('[data-session-phase="ready"]')).to_have_attribute("data-state", "complete")


def hold_status_fetch(page):
    page.evaluate("""() => {
      const realFetch = window.fetch.bind(window);
      window.__statusFetches = 0; window.__resolveStatus = null;
      window.fetch = (url, options) => {
        if (!String(url).includes('/api/v1/status')) return realFetch(url, options);
        window.__statusFetches += 1;
        return new Promise((resolve, reject) => {
          options?.signal?.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), {once: true});
          window.__resolveStatus = data => resolve(new Response(JSON.stringify(data), {
          status: 200, headers: {'Content-Type': 'application/json'}
        })); });
      };
    }""")


def test_connecting_error_polls_single_flight_and_heartbeats_do_not_cancel_it(stream_page):
    page, ready = stream_page
    hold_status_fetch(page)
    page.evaluate("""() => {
      const source = window.__sources[0]; source.readyState = EventSource.CONNECTING;
      source.onerror(new Event('error'));
      source.emit('heartbeat', {});
      void window.__horizonTest.pollStatus(); void window.__horizonTest.pollStatus();
    }""")
    assert page.evaluate("window.__statusFetches") == 1
    page.evaluate("data => window.__resolveStatus(data)", ready)
    expect(page.locator("#conn-state")).to_have_attribute("data-state", "polling")
    # A heartbeat after REST success still cannot turn polling off or claim Live.
    page.evaluate("window.__sources[0].emit('heartbeat', {}); void window.__horizonTest.pollStatus()")
    page.wait_for_function("window.__statusFetches === 2")
    page.evaluate("data => window.__resolveStatus(data)", ready)


@pytest.mark.parametrize("finish", ["hidden", "fresh_status"])
def test_late_poll_cannot_overwrite_hidden_or_recovered_ui(stream_page, finish):
    page, ready = stream_page
    hold_status_fetch(page)
    page.evaluate("window.__sources[0].onerror(new Event('error'))")
    if finish == "hidden":
        page.evaluate("window.__visibility = 'hidden'; document.dispatchEvent(new Event('visibilitychange'))")
    else:
        page.evaluate("data => window.__sources[0].emit('status', data)", {**ready, "generation": 30})
    stale_result = copy.deepcopy(ready)
    stale_result["generation"] = 99
    stale_result["profiles"][0].update(state="failed", required_ports_ready=False)
    page.evaluate("data => window.__resolveStatus(data)", stale_result)
    page.wait_for_timeout(100)
    assert page.locator('[data-session-phase="request"]').get_attribute("data-state") != "failed"
    if finish == "fresh_status":
        expect(page.locator("#conn-state")).to_have_attribute("data-state", "live")
    else:
        page.evaluate("void window.__horizonTest.pollStatus(); window.__horizonTest.checkStreamFreshness()")
        assert page.evaluate("window.__statusFetches") == 1


def test_failed_poll_keeps_retrying_until_current_status_arrives(stream_page):
    page, ready = stream_page
    page.evaluate("""() => {
      const realFetch = window.fetch.bind(window);
      window.fetch = (url, options) => String(url).includes('/api/v1/status')
        ? Promise.reject(new Error('temporary transport failure')) : realFetch(url, options);
      window.__restoreFetch = () => { window.fetch = realFetch; };
    }""")
    stale(page)
    expect(page.locator("#conn-state")).to_have_attribute("data-state", "offline")
    page.route("**/api/v1/status", lambda route: route.fulfill(json=ready))
    page.evaluate("window.__restoreFetch(); void window.__horizonTest.pollStatus()")
    expect(page.locator('[data-session-phase="ready"]')).to_have_attribute("data-state", "complete")


def test_hung_poll_is_aborted_and_next_poll_can_recover(stream_page):
    page, ready = stream_page
    hold_status_fetch(page)
    page.evaluate("""() => {
      const timer = window.setTimeout.bind(window);
      window.setTimeout = (callback, ms, ...args) => {
        if (ms === 10000) window.__abortPoll = callback;
        return timer(callback, ms, ...args);
      };
      window.__sources[0].onerror(new Event('error'));
      window.__abortPoll();
    }""")
    expect(page.locator("#conn-state")).to_have_attribute("data-state", "offline")
    assert page.evaluate("window.__statusFetches") == 1
    page.evaluate("void window.__horizonTest.pollStatus()")
    page.wait_for_function("window.__statusFetches === 2")
    page.evaluate("data => window.__resolveStatus(data)", ready)
    expect(page.locator('[data-session-phase="ready"]')).to_have_attribute("data-state", "complete")
