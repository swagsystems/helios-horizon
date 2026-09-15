"""Focused regressions for incident provenance and secondary usability polish.

All API traffic is synthetic route fulfillment, so nothing here touches a live
controller, game profile or schedule.
"""

from __future__ import annotations

from urllib.parse import urlparse

from playwright.sync_api import Page

from browser_harness import browser_page


def _profile(name: str, version: str) -> dict:
    return {
        "id": name,
        "display_name": "Minecraft" if name == "minecraft" else name,
        "adapter": "crafty" if name == "minecraft" else "systemd",
        "operations": ["start", "stop", "restart"],
        "public_endpoint": {"host": "mc.example.test", "port": 25565},
        "installed_version": version,
    }


def _status(name: str = "minecraft", version: str = "1.21.8") -> dict:
    return {
        "generation": 1,
        "observed_at": "2026-09-15T12:00:00Z",
        "profiles": [{
            "profile_id": name,
            "state": "stopped",
            "health": "unknown",
            "slot_owner": None,
            "active_job_id": None,
            "pid": None,
            "started_at": None,
            "uptime_seconds": None,
            "cpu_percent": None,
            "rss_bytes": None,
            "players_online": 0,
            "installed_version": version,
            "restart_required": False,
            "required_ports_ready": False,
        }],
    }


def _route(page: Page, *, audit: list, update: dict | None = None, calls: list | None = None,
           profiles: list | None = None, version: str = "1.21.8", status: dict | None = None) -> None:
    update_calls = calls if calls is not None else []
    names = profiles or [
        _profile("minecraft", version),
        {**_profile("terraria-vanilla", version), "adapter": "systemd", "display_name": "Terraria Vanilla"},
    ]
    snapshot = status or _status(version=version)

    def fulfill(route):
        request = route.request
        path = urlparse(request.url).path
        if path == "/api/v1/session":
            return route.fulfill(json={"actor": "operator@example.test", "csrf_token": "test-token"})
        if path == "/api/v1/status":
            return route.fulfill(json=snapshot)
        if path == "/api/v1/profiles":
            return route.fulfill(json=names)
        if path == "/api/v1/audit":
            return route.fulfill(json={"items": audit, "next_cursor": None})
        if path == "/api/v1/events":
            return route.fulfill(json={"items": [], "next_cursor": None})
        if path == "/api/v1/schedules":
            return route.fulfill(json={"schedules": []})
        if path.endswith("/update"):
            update_calls.append(path)
            return route.fulfill(json=update or {})
        if path.endswith("/resource-capacity"):
            return route.fulfill(json={"cpu_capacity_percent": 400, "memory_capacity_bytes": 12 * 1024**3})
        return route.fulfill(json={})

    page.route("**/api/v1/**", fulfill)


def _open_incidents(page: Page, web_server: str, audit: list, **route_kwargs) -> None:
    _route(page, audit=audit, **route_kwargs)
    page.goto(f"{web_server}#/events")
    page.wait_for_selector("#incident-list .incident-item")


def _entry(action: str, result: str, timestamp: str, *, profile: str = "minecraft",
           code: str | None = None, actor: str = "operator@example.test") -> dict:
    return {
        "id": f"{action}-{timestamp}",
        "timestamp": timestamp,
        "actor": actor,
        "action": action,
        "profile_id": profile,
        "result": result,
        "error_code": code,
        "detail": f"{action} {result}",
    }


def test_incident_disclosure_explains_derived_resolution_without_leaking_detail(web_server):
    audit = [
        _entry("start", "failed", "2026-09-15T10:00:00Z", code="start_timeout", actor="operator@example.test"),
        _entry("start", "succeeded", "2026-09-15T10:20:00Z"),
    ]
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _open_incidents(page, web_server, audit)
        row = page.locator("#incident-list .incident-item")
        assert row.count() == 1
        assert row.get_attribute("data-state") == "resolved"
        disclosure = row.locator(".incident-detail")
        assert not disclosure.locator(".incident-breakdown").is_visible()
        disclosure.locator("summary").click()
        text = disclosure.locator(".incident-breakdown").inner_text()
        assert "later start succeeded" in text
        assert "derived from Horizon's typed audit record" in text
        assert "not proof that every symptom is fixed" in text
        # The denial/jargon repetition was removed in review.
        assert "human acknowledgement" not in text
        assert "clearing event" not in text
        # Raw arguments, log lines and the actor identity never reach the rail.
        assert "operator@example.test" not in text
        assert "start timed out" not in text


def test_incident_disclosure_keyboard_operable(web_server):
    audit = [
        _entry("backup", "failed", "2026-09-15T09:00:00Z", code="backup_failed"),
        _entry("backup", "failed", "2026-09-15T09:10:00Z", code="backup_failed"),
    ]
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _open_incidents(page, web_server, audit)
        summary = page.locator("#incident-list .incident-detail summary")
        summary.focus()
        assert page.evaluate("document.activeElement === document.querySelector('#incident-list .incident-detail summary')")
        page.keyboard.press("Enter")
        assert page.locator("#incident-list .incident-item").get_attribute("data-state") == "critical"
        breakdown = page.locator("#incident-list .incident-breakdown")
        assert breakdown.is_visible()
        text = breakdown.inner_text()
        assert "2 occurrences" in page.locator("#incident-list .incident-item .incident-meta").inner_text()
        assert "2 occurrences" in text or text.count("2026") == 2
        assert "no later successful backup for Minecraft" in text
        assert "not proof that every symptom is fixed" in text
        assert "latest 200 entries" in text


def test_incident_needs_review_ignores_success_for_another_profile_or_action(web_server):
    audit = [
        _entry("backup", "failed", "2026-09-15T10:00:00Z", profile="minecraft", code="backup_failed"),
        _entry("backup", "succeeded", "2026-09-15T10:05:00Z", profile="terraria-vanilla"),
        _entry("start", "succeeded", "2026-09-15T10:06:00Z", profile="minecraft"),
        _entry("backup", "succeeded", "2026-09-15T10:07:00Z", profile="minecraft"),
    ]
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _open_incidents(page, web_server, audit)
        rows = page.locator("#incident-list .incident-item")
        assert rows.count() == 1
        resolved = rows.first
        # The matching profile+action success wins over the earlier mismatches.
        assert resolved.get_attribute("data-state") == "resolved"
        resolved.locator(".incident-detail summary").click()
        assert "later backup succeeded for Minecraft" in resolved.locator(".incident-breakdown").inner_text()


def test_incident_mismatched_only_success_stays_needs_review(web_server):
    audit = [
        _entry("backup", "failed", "2026-09-15T10:00:00Z", profile="minecraft", code="backup_failed"),
        _entry("backup", "succeeded", "2026-09-15T10:05:00Z", profile="terraria-vanilla"),
    ]
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _open_incidents(page, web_server, audit)
        row = page.locator("#incident-list .incident-item")
        assert row.get_attribute("data-state") == "critical"
        row.locator(".incident-detail summary").click()
        text = row.locator(".incident-breakdown").inner_text()
        assert "no later successful backup for Minecraft" in text
        assert "latest 200 entries" in text
        assert "human acknowledgement" not in text


def test_incident_fresh_failure_after_resolution_needs_review_again(web_server):
    audit = [
        _entry("backup", "failed", "2026-09-15T08:00:00Z", code="backup_failed"),
        _entry("backup", "succeeded", "2026-09-15T08:30:00Z"),
        _entry("backup", "failed", "2026-09-15T09:00:00Z", code="backup_failed"),
    ]
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _open_incidents(page, web_server, audit)
        row = page.locator("#incident-list .incident-item")
        assert row.get_attribute("data-state") == "critical"
        assert row.locator(".incident-state").inner_text() == "NEEDS REVIEW"
        row.locator(".incident-detail summary").click()
        text = row.locator(".incident-breakdown").inner_text()
        assert "no later successful backup for Minecraft" in text
        assert "2026-09-15T08:00:00Z" not in text


def test_incident_lists_each_bounded_occurrence_timestamp(web_server):
    audit = [
        _entry("backup", "failed", "2026-09-15T07:00:00Z", code="backup_failed"),
        _entry("backup", "rejected", "2026-09-15T07:05:00Z", code="backup_failed"),
        _entry("backup", "failed", "2026-09-15T07:10:00Z", code="backup_failed"),
    ]
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _open_incidents(page, web_server, audit)
        row = page.locator("#incident-list .incident-item")
        assert "3 occurrences" in row.locator(".incident-meta").inner_text()
        row.locator(".incident-detail summary").click()
        text = row.locator(".incident-breakdown").inner_text()
        assert text.count("2026") == 3
        assert "Occurrences in this window (3)" in text


def test_update_result_persists_across_tabs_with_provenance_times(web_server):
    calls: list[str] = []
    status = {
        "profile_id": "minecraft",
        "strategy": "curated_modpack",
        "installed_version": "1.1.4-SSV4.1.5",
        "available_version": "1.1.4-SSV4.1.5",
        "restart_required": False,
        "apply_supported": True,
        "state": "current",
        "message": "No update available for Minecraft.",
        "checked_at": "2026-09-15T11:00:00Z",
    }
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[], update=status, calls=calls)
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#detail-view:not([hidden])")
        assert "Not checked in this tab yet." in page.locator("#update-result-message").inner_text()
        page.get_by_role("button", name="Check for updates", exact=True).click()
        page.wait_for_function("document.querySelector('#update-result-time').hidden === false")
        message = page.locator("#update-result-message").inner_text()
        assert message == "No update available for Minecraft."
        hint = page.locator("#update-result-time").inner_text()
        assert "Update check ran " in hint
        assert "result retrieved " in hint
        assert calls == ["/api/v1/profiles/minecraft/update"]

        page.get_by_role("tab", name="Logs").click()
        page.get_by_role("tab", name="Metrics").click()
        page.wait_for_selector("#panel-metrics:not([hidden])")
        assert page.locator("#update-result-message").inner_text() == message
        assert page.locator("#update-result-time").inner_text() == hint
        assert calls == ["/api/v1/profiles/minecraft/update"]


def test_update_result_is_scoped_per_profile_and_keeps_failed_state(web_server):
    calls: list[str] = []
    other = {
        "profile_id": "terraria-vanilla",
        "strategy": "manual",
        "installed_version": "1.4.4.9",
        "available_version": None,
        "restart_required": False,
        "apply_supported": False,
        "state": "failed",
        "message": "Curated modpack metadata is missing.",
    }
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[], update=other, calls=calls)
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#detail-view:not([hidden])")
        page.get_by_role("button", name="Check for updates", exact=True).click()
        page.wait_for_function("document.querySelector('#update-result-time').hidden === false")
        assert page.locator("#update-result-message").inner_text() == "Curated modpack metadata is missing."
        assert calls == ["/api/v1/profiles/minecraft/update"]

        # The Minecraft result must not follow the reader to a different profile.
        page.goto(f"{web_server}#/servers/terraria-vanilla/metrics")
        page.wait_for_selector("#detail-view:not([hidden])")
        assert "Not checked in this tab yet." in page.locator("#update-result-message").inner_text()
        assert page.locator("#update-result-time").is_hidden()


def test_update_result_distinguishes_upstream_check_time_from_retrieval(web_server):
    status = {
        "profile_id": "minecraft",
        "strategy": "manual",
        "installed_version": "1.21.8",
        "available_version": None,
        "restart_required": False,
        "apply_supported": False,
        "state": "deferred",
        "message": "The automatic update is deferred until the next safe window.",
        "checked_at": "2026-09-14T22:00:00Z",
    }
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[], update=status)
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#detail-view:not([hidden])")
        page.get_by_role("button", name="Check for updates", exact=True).click()
        page.wait_for_function("document.querySelector('#update-result-time').hidden === false")
        hint = page.locator("#update-result-time").inner_text()
        checked = page.evaluate("() => new Date('2026-09-14T22:00:00Z').toLocaleString()")
        retrieved = page.evaluate("() => new Date(Date.now()).toLocaleString()")
        assert f"Update check ran {checked}" in hint
        assert f"result retrieved {retrieved}" in hint
        assert checked != retrieved
        assert page.locator("#update-result-message").inner_text() == status["message"]


def test_available_update_without_message_compares_versions_inline(web_server):
    status = {
        "profile_id": "minecraft",
        "strategy": "manual",
        "installed_version": "1.21.8",
        "available_version": "1.21.9",
        "restart_required": False,
        "apply_supported": False,
        "state": "available",
        "message": None,
        "checked_at": "2026-09-15T11:00:00Z",
    }
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[], update=status)
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#detail-view:not([hidden])")
        page.get_by_role("button", name="Check for updates", exact=True).click()
        page.wait_for_function("document.querySelector('#update-result-time').hidden === false")
        assert page.locator("#update-result-message").inner_text() == (
            "Update 1.21.9 is available; installed version is 1.21.8."
        )


def test_unknown_update_state_stays_unknown_without_invented_message(web_server):
    status = {
        "profile_id": "minecraft",
        "strategy": "manual",
        "installed_version": "1.21.8",
        "available_version": None,
        "restart_required": False,
        "apply_supported": False,
        "state": "unknown",
        "message": None,
    }
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[], update=status)
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#detail-view:not([hidden])")
        page.get_by_role("button", name="Check for updates", exact=True).click()
        page.wait_for_function("document.querySelector('#update-result-time').hidden === false")
        message = page.locator("#update-result-message").inner_text()
        assert message == "The controller did not report a supported update state."
        assert "is available" not in message


def test_delayed_update_response_for_left_profile_caches_without_opening_dialog(web_server):
    calls: list[str] = []
    available = {
        "profile_id": "minecraft",
        "strategy": "manual",
        "installed_version": "1.21.8",
        "available_version": "1.21.9",
        "restart_required": False,
        "apply_supported": True,
        "state": "available",
        "message": None,
        "checked_at": "2026-09-15T11:00:00Z",
    }

    def update_route(route):
        if route.request.method != "GET":
            return route.fulfill(json={"ok": True})
        calls.append(urlparse(route.request.url).path)
        # First poll says "still checking" so the test can leave the profile
        # before the authoritative response lands.
        if len(calls) == 1:
            return route.fulfill(json={
                "profile_id": "minecraft",
                "strategy": "manual",
                "installed_version": "1.21.8",
                "available_version": None,
                "restart_required": False,
                "apply_supported": True,
                "state": "checking",
                "message": "An update check is in progress.",
            })
        return route.fulfill(json=available)

    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[])
        page.route("**/api/v1/profiles/minecraft/update**", update_route)
        page.goto(f"{web_server}#/servers/minecraft/console")
        page.wait_for_selector("#detail-view:not([hidden])")
        page.get_by_role("button", name="Check for updates", exact=True).click()

        # Leave for another profile while the check for minecraft is pending.
        page.goto(f"{web_server}#/servers/terraria-vanilla/metrics")
        page.wait_for_selector("#detail-view:not([hidden])")
        # The default poll delay is 1.2s, so wait past the authoritative poll.
        page.wait_for_timeout(2500)
        assert len(calls) >= 2
        # The late response must not open a dialog for the profile on screen.
        assert page.locator("#update-dialog[open]").count() == 0
        assert "is available" not in page.locator("#update-result-message").inner_text()

        # Returning to the requested profile shows the cached inline result
        # without re-querying and still without a surprise dialog.
        before = len(calls)
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#detail-view:not([hidden])")
        assert page.locator("#update-result-message").inner_text() == (
            "Update 1.21.9 is available; installed version is 1.21.8."
        )
        assert page.locator("#update-dialog[open]").count() == 0
        assert len(calls) == before

        # An intended check on the profile now on screen still opens normally.
        page.get_by_role("button", name="Check for updates", exact=True).click()
        page.wait_for_selector("#update-dialog[open]")
        dialog = page.get_by_role("dialog", name="Apply server update")
        assert dialog.get_by_text("1.21.8", exact=True).is_visible()
        assert dialog.get_by_text("1.21.9", exact=True).is_visible()


def test_startup_estimate_link_is_visible_when_preference_off_and_focuses_toggle(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[])
        page.goto(f"{web_server}#/")
        page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)

        # The preference is off by default and the estimate panel stays hidden;
        # the readiness-area link must still be discoverable.
        estimate_link = page.get_by_role("link", name="Show the estimate in Settings", exact=True)
        assert estimate_link.is_visible()
        assert page.locator("#startup-estimate").is_hidden()
        assert page.evaluate("() => localStorage.getItem('helios-startup-estimate')") in (None, "0")
        assert estimate_link.get_attribute("data-settings-jump") == "startup-estimate-settings"

        estimate_link.click()
        page.wait_for_selector("#settings-view:not([hidden])")
        assert page.locator("#startup-estimate-settings").is_visible()
        # Navigation must complete before focus; the toggle is a real focus target.
        assert page.evaluate("() => document.activeElement === document.querySelector('#startup-estimate-toggle')")
        assert page.evaluate("() => document.querySelector('#startup-estimate-toggle').offsetParent !== null")


def test_client_join_help_reports_runtime_and_modpack_guidance(web_server):
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[], version="1.1.4-SSV4.1.5")
        page.goto(f"{web_server}#/")
        page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
        details = page.locator("#session-client-help")
        assert details.get_by_text("Client setup / join help", exact=True).is_visible()
        details.locator("summary").click()
        assert details.locator("#session-client-help-runtime").inner_text() == (
            "Minecraft · installed runtime version 1.1.4-SSV4.1.5."
        )
        copy = details.locator("#session-client-help-copy").inner_text()
        assert "modpack" in copy
        assert "usual reason" not in copy
        # No fixed release promise, no private address, no hardcoded host.
        assert "compatible" not in copy
        assert "192.168." not in copy
        assert "example.test" not in copy


def test_vanilla_terraria_help_does_not_mention_tmodloader(web_server):
    status = _status(name="terraria-vanilla", version="1.4.4.9")
    status["profiles"][0].update({"slot_owner": "terraria-vanilla"})
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[], status=status, profiles=[{
            **_profile("terraria-vanilla", "1.4.4.9"), "adapter": "systemd", "display_name": "Terraria Vanilla",
        }])
        page.goto(f"{web_server}#/")
        page.wait_for_selector('[data-profile-id="terraria-vanilla"]', timeout=5000)
        details = page.locator("#session-client-help")
        details.locator("summary").click()
        copy = details.locator("#session-client-help-copy").inner_text()
        assert "Terraria version" in copy
        assert "tModLoader" not in copy
        assert "usual reason" not in copy
        assert details.locator("#session-client-help-runtime").inner_text() == (
            "Terraria Vanilla · installed runtime version 1.4.4.9."
        )


def test_tmodloader_profile_help_mentions_tmodloader(web_server):
    status = _status(name="terraria-tmod", version="2024.12")
    status["profiles"][0].update({"slot_owner": "terraria-tmod"})
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[], status=status, profiles=[{
            **_profile("terraria-tmod", "2024.12"), "adapter": "systemd", "display_name": "Terraria tModLoader",
        }])
        page.goto(f"{web_server}#/")
        page.wait_for_selector('[data-profile-id="terraria-tmod"]', timeout=5000)
        details = page.locator("#session-client-help")
        details.locator("summary").click()
        copy = details.locator("#session-client-help-copy").inner_text()
        assert "tModLoader" in copy
        assert "usual reason" not in copy


def test_help_without_installed_version_says_unavailable(web_server):
    status = _status()
    status["profiles"][0]["installed_version"] = None
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _route(page, audit=[], status=status)
        page.goto(f"{web_server}#/")
        page.wait_for_selector('[data-profile-id="minecraft"]', timeout=5000)
        details = page.locator("#session-client-help")
        details.locator("summary").click()
        runtime = details.locator("#session-client-help-runtime").inner_text()
        assert runtime == "Minecraft · installed runtime version unavailable."
        assert "—" not in runtime


def test_live_rail_version_wraps_at_phone_width(web_server):
    long_version = "1.1.4-SSV4.1.5-hotfix-2026-09-15-build.12345"
    with browser_page(viewport={"width": 390, "height": 844}) as page:
        _route(page, audit=[], version=long_version)
        page.goto(f"{web_server}#/servers/minecraft/metrics")
        page.wait_for_selector("#detail-view:not([hidden])")
        version = page.locator("#rail-version")
        assert version.inner_text() == long_version
        assert version.evaluate(
            "(node) => node.getBoundingClientRect().right <= node.closest('.rail-card').getBoundingClientRect().right + 1"
        )
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")


def test_incident_rail_payload_keeps_identity_out_of_rendered_markup(web_server):
    audit = [
        _entry("start", "failed", "2026-09-15T10:00:00Z", code="start_timeout", actor="player@example.test"),
        _entry("start", "succeeded", "2026-09-15T10:05:00Z"),
    ]
    with browser_page(viewport={"width": 1280, "height": 900}) as page:
        _open_incidents(page, web_server, audit)
        page.locator("#incident-list .incident-detail summary").click()
        markup = page.locator("#incident-list").inner_html()
        assert "player@example.test" not in markup
        assert "start failed" not in markup
        assert "start timed out" not in markup
