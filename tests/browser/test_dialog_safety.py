"""Browser regressions for dialog cancellation safety.

Every dialog close/Cancel control must close its dialog without submitting a
lifecycle or export action, with empty, partial, wrong or correct confirmation
text. Intended submissions must still work. All API calls are satisfied by
fake in-page routes; no live endpoint is contacted and no game lifecycle action
is issued.
"""

from __future__ import annotations

import json
from urllib.parse import urlparse

import pytest
from playwright.sync_api import Page, expect

from browser_harness import browser_page


RUNNING = {
    "profile_id": "minecraft",
    "state": "running",
    "health": "healthy",
    "slot_owner": "minecraft",
    "active_job_id": None,
    "pid": 101,
    "started_at": "2026-07-11T10:00:00Z",
    "uptime_seconds": 7200,
    "cpu_percent": 7.4,
    "rss_bytes": 128000000,
    "players_online": 4,
    "installed_version": "1.21.8",
    "restart_required": False,
    "required_ports_ready": True,
}

STOPPED = {
    **RUNNING,
    "profile_id": "pz-rising",
    "state": "stopped",
    "health": "unknown",
    "slot_owner": None,
    "pid": None,
    "started_at": None,
    "uptime_seconds": None,
    "players_online": 0,
}

PROFILE_NAMES = [
    {"id": "minecraft", "display_name": "Minecraft", "adapter": "crafty", "operations": ["start", "stop", "restart"]},
    {"id": "pz-rising", "display_name": "Project Zomboid", "adapter": "systemd", "operations": ["start", "stop", "restart"]},
]

# Deterministic fake delay: a test can hold one matching request open, observe
# the pending dialog, then release it and assert nothing further is dispatched.
HOLD_SCRIPT = """
  window.__holds = [];
  const realFetch = window.fetch.bind(window);
  window.fetch = async (input, init) => {
    const url = String(typeof input === "string" ? input : (input && input.url) || "");
    const entry = window.__holds.find((candidate) => !candidate.used && url.includes(candidate.match));
    if (entry) { entry.used = true; await entry.gate; }
    return realFetch(input, init);
  };
  window.__holdPrepare = (match) => {
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    window.__holds.push({ match, gate, release, used: false });
    return true;
  };
  window.__releasePrepare = () => {
    const entry = window.__holds.find((candidate) => candidate.used);
    if (!entry) return false;
    entry.release();
    return true;
  };
"""


@pytest.fixture
def dialog_page(web_server):
    """Serve the real shell with recording fake APIs for every dialog."""

    with browser_page(viewport={"width": 1280, "height": 900}, init_script=HOLD_SCRIPT) as page:
        status = {"generation": 1, "observed_at": "2026-07-11T12:00:00Z", "profiles": [RUNNING, STOPPED]}
        posts: list[dict] = []
        flags = {"backup_failure": False, "backup_empty": False}

        def fulfill(route):
            request = route.request
            path = urlparse(request.url).path
            if request.method == "POST":
                posts.append({"path": path, "body": request.post_data_json})
            if path == "/api/v1/session":
                return route.fulfill(json={"actor": "operator@example.test", "csrf_token": "test-token", "expires_at": None})
            if path == "/api/v1/status":
                return route.fulfill(json=status)
            if path == "/api/v1/profiles":
                return route.fulfill(json=PROFILE_NAMES)
            if path == "/api/v1/schedules":
                return route.fulfill(json={"schedules": []})
            if path == "/api/v1/stream":
                payload = json.dumps(status, separators=(",", ":"))
                return route.fulfill(
                    status=200,
                    headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"},
                    body=f"event: status\ndata: {payload}\n\n",
                )
            if path.endswith("/logs"):
                return route.fulfill(json={"items": [], "next_cursor": None})
            if path.endswith("/backups"):
                if flags["backup_failure"]:
                    return route.fulfill(status=503, json={"error": {"message": "backup list unavailable"}})
                if flags["backup_empty"]:
                    return route.fulfill(json={"items": [], "next_cursor": None})
                return route.fulfill(json={"items": [
                    {
                        "id": "backup-1",
                        "profile_id": "minecraft",
                        "created_at": "2026-07-10T12:00:00Z",
                        "size_bytes": 1073741824,
                        "verified": True,
                        "protected": False,
                    },
                ], "next_cursor": None})
            if path.endswith("/force-stop/prepare"):
                return route.fulfill(json={"confirmation_id": "force-confirmation-1"})
            if path == "/api/v1/force-stop/confirm":
                return route.fulfill(json={"ok": True})
            if path.endswith("/restore/prepare"):
                return route.fulfill(json={"confirmation_id": "restore-confirmation-1"})
            if path == "/api/v1/restore/confirm":
                return route.fulfill(json={"ok": True})
            if path == "/api/v1/switch/prepare":
                return route.fulfill(json={"confirmation_id": "switch-confirmation-1"})
            if path == "/api/v1/switch/confirm":
                return route.fulfill(json={"ok": True})
            return route.fulfill(json={"ok": True})

        page.route("**/api/v1/**", fulfill)
        console_errors = []
        page_errors = []
        page.on("console", lambda message: console_errors.append(message.text) if message.type == "error" else None)
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.goto(web_server)
        expect(page.locator('[data-profile-id="minecraft"]')).to_be_visible(timeout=5000)
        page._dialog_posts = posts  # type: ignore[attr-defined]
        page._dialog_flags = flags  # type: ignore[attr-defined]
        yield page
        assert [message for message in console_errors if "503" not in message] == []
        assert page_errors == []


def open_force(page: Page):
    page.goto(f"{page.url.split('#')[0]}#/servers/minecraft/console")
    expect(page.locator("#detail-view:not([hidden])")).to_be_visible()
    page.locator("#detail-force").click()
    dialog = page.get_by_role("dialog", name="Force stop server")
    expect(dialog).to_be_visible()
    return dialog


def open_switch(page: Page):
    page.goto(f"{page.url.split('#')[0]}#/")
    page.locator("#switch-active").click()
    dialog = page.get_by_role("dialog", name="Switch active server")
    expect(dialog).to_be_visible()
    return dialog


def open_restore(page: Page):
    page.goto(f"{page.url.split('#')[0]}#/servers/minecraft/backups")
    expect(page.locator("#backup-list")).to_be_visible()
    page.get_by_role("button", name="Restore backup-1", exact=True).click()
    dialog = page.get_by_role("dialog", name="Restore backup")
    expect(dialog).to_be_visible()
    return dialog


def posts(page: Page) -> list[dict]:
    return page._dialog_posts  # type: ignore[attr-defined]


# Empty, partial, wrong and correct text must all be inert on a close control.
@pytest.mark.parametrize("typed", ["", "s", "not Minecraft", "Minecraft"])
def test_force_cancel_never_reaches_lifecycle_api(dialog_page: Page, typed: str):
    page = dialog_page
    dialog = open_force(page)
    if typed:
        dialog.get_by_label("Type the profile name to confirm", exact=True).fill(typed)
    dialog.get_by_role("button", name="Cancel", exact=True).click()
    expect(page.get_by_role("dialog", name="Force stop server")).to_be_hidden()
    assert posts(page) == []


@pytest.mark.parametrize("typed", ["", "s", "not Minecraft", "Minecraft"])
def test_force_header_close_never_reaches_lifecycle_api(dialog_page: Page, typed: str):
    page = dialog_page
    dialog = open_force(page)
    if typed:
        dialog.get_by_label("Type the profile name to confirm", exact=True).fill(typed)
    dialog.get_by_role("button", name="Close force stop dialog", exact=True).click()
    expect(page.get_by_role("dialog", name="Force stop server")).to_be_hidden()
    assert posts(page) == []


def test_force_escape_and_backdrop_close_without_api_calls(dialog_page: Page):
    page = dialog_page
    dialog = open_force(page)
    dialog.get_by_label("Type the profile name to confirm", exact=True).fill("Minecraft")
    page.keyboard.press("Escape")
    expect(page.get_by_role("dialog", name="Force stop server")).to_be_hidden()

    dialog = open_force(page)
    dialog.get_by_label("Type the profile name to confirm", exact=True).fill("Minecraft")
    page.mouse.click(4, 4)
    expect(page.get_by_role("dialog", name="Force stop server")).to_be_hidden()
    assert posts(page) == []


def test_force_intended_submission_still_prepares_and_confirms(dialog_page: Page):
    page = dialog_page
    dialog = open_force(page)
    typed = dialog.get_by_label("Type the profile name to confirm", exact=True)
    confirm = dialog.get_by_role("button", name="Force stop", exact=True)
    typed.fill("not Minecraft")
    expect(confirm).to_be_disabled()
    typed.fill("Minecraft")
    expect(confirm).to_be_enabled()
    confirm.click()
    expect(page.get_by_role("dialog", name="Force stop server")).to_be_hidden()
    assert posts(page) == [
        {"path": "/api/v1/profiles/minecraft/force-stop/prepare", "body": {}},
        {"path": "/api/v1/force-stop/confirm", "body": {"confirmation_id": "force-confirmation-1"}},
    ]
    expect(page.locator("#status-announcer")).to_have_text("Force stop requested for Minecraft.")


def test_force_handler_rejects_wrong_text_even_when_the_button_is_enabled(dialog_page: Page):
    page = dialog_page
    dialog = open_force(page)
    typed = dialog.get_by_label("Type the profile name to confirm", exact=True)
    confirm = dialog.get_by_role("button", name="Force stop", exact=True)
    typed.fill("not Minecraft")
    expect(confirm).to_be_disabled()

    # Force the action button enabled, then click it: the handler's own exact
    # text check must still refuse to prepare a force stop.
    page.evaluate("() => { document.getElementById('force-confirm').disabled = false; }")
    confirm.click()
    page.wait_for_timeout(150)
    expect(page.get_by_role("dialog", name="Force stop server")).to_be_visible()
    expect(page.locator("#status-announcer")).to_have_text("Type Minecraft exactly to confirm the force stop.")
    assert posts(page) == []


def test_force_handler_rejects_scripted_submit_with_empty_text(dialog_page: Page):
    page = dialog_page
    dialog = open_force(page)
    typed = dialog.get_by_label("Type the profile name to confirm", exact=True)
    typed.fill("")
    page.evaluate(
        """() => {
          const form = document.getElementById('force-form');
          const button = document.getElementById('force-confirm');
          button.disabled = false;
          form.dispatchEvent(new SubmitEvent('submit', {submitter: button, bubbles: true, cancelable: true}));
        }"""
    )
    page.wait_for_timeout(150)
    expect(page.get_by_role("dialog", name="Force stop server")).to_be_visible()
    assert posts(page) == []


def test_force_enter_key_confirmation_still_submits(dialog_page: Page):
    page = dialog_page
    dialog = open_force(page)
    typed = dialog.get_by_label("Type the profile name to confirm", exact=True)
    typed.fill("Minecraft")
    typed.press("Enter")
    expect(page.get_by_role("dialog", name="Force stop server")).to_be_hidden()
    assert [item["path"] for item in posts(page)] == [
        "/api/v1/profiles/minecraft/force-stop/prepare",
        "/api/v1/force-stop/confirm",
    ]


def test_restore_handler_rejects_scripted_submit_with_wrong_text(dialog_page: Page):
    page = dialog_page
    dialog = open_restore(page)
    dialog.get_by_label("Backup ID", exact=True).fill("backup-1")
    dialog.get_by_label("Type the profile name to confirm", exact=True).fill("not Minecraft")
    page.evaluate(
        """() => {
          const form = document.getElementById('restore-form');
          const button = document.getElementById('restore-confirm');
          button.disabled = false;
          form.dispatchEvent(new SubmitEvent('submit', {submitter: button, bubbles: true, cancelable: true}));
        }"""
    )
    page.wait_for_timeout(150)
    expect(page.get_by_role("dialog", name="Restore backup")).to_be_visible()
    assert posts(page) == []


# The restore close controls must not depend on the required confirmation field.
@pytest.mark.parametrize("control", ["Cancel", "Close restore dialog"])
def test_restore_close_controls_close_with_empty_fields(dialog_page: Page, control: str):
    page = dialog_page
    dialog = open_restore(page)
    dialog.get_by_role("button", name=control, exact=True).click()
    expect(page.get_by_role("dialog", name="Restore backup")).to_be_hidden()
    assert posts(page) == []


def test_restore_cancel_with_filled_wrong_text_does_not_post(dialog_page: Page):
    page = dialog_page
    dialog = open_restore(page)
    dialog.get_by_label("Backup ID", exact=True).fill("backup-1")
    dialog.get_by_label("Type the profile name to confirm", exact=True).fill("Minecraft")
    dialog.get_by_role("button", name="Cancel", exact=True).click()
    expect(page.get_by_role("dialog", name="Restore backup")).to_be_hidden()
    assert posts(page) == []


def test_restore_copy_states_world_replacement_and_preservation(dialog_page: Page):
    page = dialog_page
    dialog = open_restore(page)
    description = dialog.locator("#restore-description")
    text = description.inner_text()
    assert "backup backup-1" in text
    assert "must be stopped first" in text
    assert "does not stop it for you" in text
    assert "protected backup of the current world" in text
    assert "changes made after the chosen backup are not in the restored world" in text
    assert "start the server again yourself" in text
    # Horizon never stops or starts the profile for a restore.
    assert "Horizon stops the profile" not in text
    assert "Horizon will start" not in text


def test_restore_intended_submission_still_prepares_and_confirms(dialog_page: Page):
    page = dialog_page
    dialog = open_restore(page)
    dialog.get_by_label("Backup ID", exact=True).fill("backup-1")
    dialog.get_by_label("Type the profile name to confirm", exact=True).fill("Minecraft")
    dialog.get_by_role("button", name="Restore backup", exact=True).click()
    expect(page.get_by_role("dialog", name="Restore backup")).to_be_hidden()
    assert posts(page) == [
        {"path": "/api/v1/profiles/minecraft/restore/prepare", "body": {"backup_id": "backup-1"}},
        {"path": "/api/v1/restore/confirm", "body": {"confirmation_id": "restore-confirmation-1"}},
    ]


def test_switch_copy_names_running_profile_and_backup_owner(dialog_page: Page):
    page = dialog_page
    dialog = open_switch(page)
    expect(dialog.locator("#switch-current")).to_have_text("Minecraft")
    expect(dialog.locator("#switch-backup-owner")).to_have_text("Last backup (Minecraft)")
    created = page.evaluate("() => new Date('2026-07-10T12:00:00Z').toLocaleString()")
    expect(dialog.locator("#switch-backup")).to_have_text(created)
    consequence = dialog.locator("#switch-consequence")
    expect(consequence).to_contain_text("Switching stops Minecraft, then starts Project Zomboid")
    expect(consequence).to_contain_text("belongs to Minecraft")
    expect(dialog.get_by_text("Current server (running, owns the active slot)")).to_be_visible()


def test_switch_without_a_running_server_is_explicit(dialog_page: Page):
    page = dialog_page
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 90,
          observed_at: '2026-07-11T12:30:00Z',
          profiles: [
            {profile_id: 'minecraft', state: 'stopped', health: 'unknown', slot_owner: null, cpu_percent: null},
            {profile_id: 'pz-rising', state: 'stopped', health: 'unknown', slot_owner: null, cpu_percent: null}
          ]
        }}))"""
    )
    dialog = open_switch(page)
    dialog.get_by_label("Target profile", exact=True).select_option("pz-rising")
    expect(dialog.locator("#switch-current")).to_have_text("No active server")
    expect(dialog.locator("#switch-backup")).to_have_text("No source backup needed — no server is running")
    consequence = dialog.locator("#switch-consequence")
    expect(consequence).to_contain_text("No server is running, so confirming just sends a start request for Project Zomboid")
    expect(consequence).to_contain_text("no switch is performed")
    expect(consequence).not_to_contain_text("stops first")


def all_stopped(page: Page) -> None:
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 91,
          observed_at: '2026-07-11T12:31:00Z',
          profiles: [
            {profile_id: 'minecraft', state: 'stopped', health: 'unknown', slot_owner: null, cpu_percent: null},
            {profile_id: 'pz-rising', state: 'stopped', health: 'unknown', slot_owner: null, cpu_percent: null}
          ]
        }}))"""
    )


def test_switch_without_running_server_dispatches_only_a_start(dialog_page: Page):
    page = dialog_page
    all_stopped(page)
    dialog = open_switch(page)
    dialog.get_by_label("Target profile", exact=True).select_option("pz-rising")
    typed = dialog.get_by_label("Type the target profile name to confirm", exact=True)
    confirm = dialog.get_by_role("button", name="Confirm switch", exact=True)
    typed.fill("not Project Zomboid")
    expect(confirm).to_be_disabled()
    typed.fill("Project Zomboid")
    expect(confirm).to_be_enabled()
    confirm.click()
    expect(page.get_by_role("dialog", name="Switch active server")).to_be_hidden()
    assert posts(page) == [{"path": "/api/v1/profiles/pz-rising/start", "body": {}}]


def test_switch_without_running_server_cancel_and_close_are_inert(dialog_page: Page):
    page = dialog_page
    all_stopped(page)
    dialog = open_switch(page)
    dialog.get_by_label("Target profile", exact=True).select_option("pz-rising")
    dialog.get_by_label("Type the target profile name to confirm", exact=True).fill("Project Zomboid")
    dialog.get_by_role("button", name="Cancel", exact=True).click()
    expect(page.get_by_role("dialog", name="Switch active server")).to_be_hidden()
    assert posts(page) == []

    dialog = open_switch(page)
    dialog.get_by_label("Target profile", exact=True).select_option("pz-rising")
    dialog.get_by_label("Type the target profile name to confirm", exact=True).fill("Project Zomboid")
    dialog.get_by_role("button", name="Close switch dialog", exact=True).click()
    expect(page.get_by_role("dialog", name="Switch active server")).to_be_hidden()
    assert posts(page) == []


def test_switch_confirmation_is_blocked_while_an_update_owns_the_slot(dialog_page: Page):
    page = dialog_page
    page.evaluate(
        """() => window.dispatchEvent(new MessageEvent('game-control-status', {data: {
          generation: 92,
          observed_at: '2026-07-11T12:32:00Z',
          profiles: [
            {profile_id: 'minecraft', state: 'running', health: 'healthy', slot_owner: 'minecraft', active_job_id: 'update', cpu_percent: 1},
            {profile_id: 'pz-rising', state: 'stopped', health: 'unknown', slot_owner: null, cpu_percent: null}
          ]
        }}))"""
    )
    dialog = open_switch(page)
    dialog.get_by_label("Target profile", exact=True).select_option("pz-rising")
    dialog.get_by_label("Type the target profile name to confirm", exact=True).fill("Project Zomboid")
    confirm = dialog.get_by_role("button", name="Confirm switch", exact=True)
    expect(confirm).to_be_disabled()
    page.evaluate("() => { document.getElementById('switch-confirm').disabled = false; }")
    confirm.click()
    page.wait_for_timeout(150)
    expect(page.get_by_role("dialog", name="Switch active server")).to_be_visible()
    assert posts(page) == []


def test_switch_cancel_during_pending_prepare_does_not_confirm(dialog_page: Page):
    page = dialog_page
    dialog = open_switch(page)
    dialog.get_by_label("Target profile", exact=True).select_option("pz-rising")
    dialog.get_by_label("Type the target profile name to confirm", exact=True).fill("Project Zomboid")
    page.evaluate("() => window.__holdPrepare('/switch/prepare')")
    dialog.get_by_role("button", name="Confirm switch", exact=True).click()
    page.wait_for_function("() => window.__holds.some((entry) => entry.used)")
    page.keyboard.press("Escape")
    expect(page.get_by_role("dialog", name="Switch active server")).to_be_hidden()
    assert page.evaluate("() => window.__releasePrepare()") is True
    page.wait_for_timeout(400)
    assert [item["path"] for item in posts(page)] == ["/api/v1/switch/prepare"]


def test_force_cancel_during_pending_prepare_does_not_confirm(dialog_page: Page):
    page = dialog_page
    dialog = open_force(page)
    dialog.get_by_label("Type the profile name to confirm", exact=True).fill("Minecraft")
    page.evaluate("() => window.__holdPrepare('/force-stop/prepare')")
    dialog.get_by_role("button", name="Force stop", exact=True).click()
    page.wait_for_function("() => window.__holds.some((entry) => entry.used)")
    dialog.get_by_role("button", name="Cancel", exact=True).click()
    expect(page.get_by_role("dialog", name="Force stop server")).to_be_hidden()
    assert page.evaluate("() => window.__releasePrepare()") is True
    page.wait_for_timeout(400)
    assert [item["path"] for item in posts(page)] == ["/api/v1/profiles/minecraft/force-stop/prepare"]


def test_switch_backup_row_distinguishes_unavailable_from_empty(dialog_page: Page):
    page = dialog_page
    page._dialog_flags["backup_failure"] = True  # type: ignore[attr-defined]
    dialog = open_switch(page)
    expect(dialog.locator("#switch-backup-owner")).to_have_text("Last backup (Minecraft)")
    expect(dialog.locator("#switch-backup")).to_have_text("Unavailable — backup list could not be read")

    page._dialog_flags["backup_failure"] = False  # type: ignore[attr-defined]
    page._dialog_flags["backup_empty"] = True  # type: ignore[attr-defined]
    page.keyboard.press("Escape")
    expect(page.get_by_role("dialog", name="Switch active server")).to_be_hidden()
    dialog = open_switch(page)
    expect(dialog.locator("#switch-backup")).to_have_text("None recorded for Minecraft")


def test_switch_cancel_and_confirmation_contract(dialog_page: Page):
    page = dialog_page
    dialog = open_switch(page)
    dialog.get_by_label("Type the target profile name to confirm", exact=True).fill("Project Zomboid")
    dialog.get_by_role("button", name="Cancel", exact=True).click()
    expect(page.get_by_role("dialog", name="Switch active server")).to_be_hidden()
    assert posts(page) == []

    dialog = open_switch(page)
    target = dialog.get_by_label("Target profile", exact=True)
    target.select_option("pz-rising")
    dialog.get_by_label("Type the target profile name to confirm", exact=True).fill("Project Zomboid")
    confirm = dialog.get_by_role("button", name="Confirm switch", exact=True)
    expect(confirm).to_be_enabled()
    confirm.click()
    expect(page.get_by_role("dialog", name="Switch active server")).to_be_hidden()
    assert [item["path"] for item in posts(page)] == ["/api/v1/switch/prepare", "/api/v1/switch/confirm"]


def test_console_export_cancel_never_starts_a_download(dialog_page: Page):
    page = dialog_page
    page.goto(f"{page.url.split('#')[0]}#/servers/minecraft/console")
    expect(page.locator("#detail-view:not([hidden])")).to_be_visible()
    downloads: list[str] = []
    page.on("download", lambda download: downloads.append(download.suggested_filename))

    page.locator("#console-save-as").click()
    dialog = page.get_by_role("dialog", name="Save console range")
    expect(dialog).to_be_visible()
    dialog.get_by_label("From", exact=True).fill("2026-07-11T10:00")
    dialog.get_by_label("To", exact=True).fill("2026-07-11T12:00")
    dialog.get_by_role("button", name="Cancel", exact=True).click()
    expect(page.get_by_role("dialog", name="Save console range")).to_be_hidden()
    page.wait_for_timeout(200)
    assert downloads == []

    page.locator("#console-save-as").click()
    dialog = page.get_by_role("dialog", name="Save console range")
    dialog.get_by_label("From", exact=True).fill("2026-07-11T10:00")
    dialog.get_by_label("To", exact=True).fill("2026-07-11T12:00")
    with page.expect_download() as download_info:
        dialog.get_by_role("button", name="Save", exact=True).click()
    assert download_info.value.suggested_filename.endswith(".txt")
    expect(page.get_by_role("dialog", name="Save console range")).to_be_hidden()
