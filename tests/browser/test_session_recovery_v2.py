from __future__ import annotations

from test_dashboard import page  # re-use the authenticated browser fixture
from browser_harness import suspend_background_status


def test_hung_session_body_is_aborted_at_bootstrap_deadline(page):
    suspend_background_status(page)
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    page.route("**/api/v1/status", lambda route: route.fulfill(status=401, json={"error": {"message": "stale"}}))
    result = page.evaluate("""async () => {
        const original = window.fetch; let probes = 0; const began = performance.now();
        window.fetch = (url, options) => String(url).includes('/api/v1/session')
            ? (probes++, Promise.resolve({ok:true, status:200, json:()=>new Promise(()=>{})}))
            : original(url, options);
        try { await window.__horizonTest.api('/api/v1/status'); return 'unexpected-success'; }
        catch (error) { return {message: error.message, expired: window.__horizonTest.sessionState().expired,
            probes, elapsed: performance.now()-began}; }
        finally { window.fetch = original; }
    }""")
    assert result["expired"] is False
    assert result["message"] in {"Session bootstrap response timed out.", "The operation was aborted.", "signal is aborted without reason", "Session bootstrap failed."}
    assert result["probes"] >= 1
    assert result["elapsed"] < 8500


def test_invalid_session_bootstrap_body_remains_recoverable(page):
    page._allow_expected_http_errors = True  # type: ignore[attr-defined]
    probes = 0

    def session(route):
        nonlocal probes
        probes += 1
        route.fulfill(json={"actor": "operator@example.test"})

    page.route("**/api/v1/session", session)
    page.route("**/api/v1/status", lambda route: route.fulfill(status=401, json={"error": {"message": "stale"}}))
    result = page.evaluate("""async () => {
        try { await window.__horizonTest.api('/api/v1/status'); return 'unexpected-success'; }
        catch (error) { return {message: error.message, state: window.__horizonTest.sessionState()}; }
    }""")
    assert result["message"] == "Invalid session bootstrap."
    assert result["state"]["expired"] is False
    assert probes == 3


def test_focus_recovery_shares_load_and_does_not_duplicate_config_controls(page):
    page.goto(f"{page.url.split('#', 1)[0]}#/servers/minecraft/config")
    page.wait_for_selector('#config-panel [data-config-key="motd"]')
    page.evaluate("""async () => {
        await Promise.all([
            window.__horizonTest.load(),
            window.__horizonTest.load(),
        ]);
    }""")
    page.wait_for_selector('#config-panel [data-config-key="motd"]')
    assert page.locator('#config-panel [data-config-key="motd"]').count() == 1
