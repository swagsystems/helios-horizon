"""Static wiring checks for dialog cancellation safety.

The browser regressions in ``tests/browser/test_dialog_safety.py`` drive the
force/restore/switch/console dialogs. These source checks keep the same
guarantee for every dialog, including the ones a test cannot open without
fabricating update or log state: each close/Cancel control is an explicit
``type="button"`` dismissal, and each dialog form requires an explicit action
submitter before it can reach a lifecycle or export action.
"""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HTML = (ROOT / "web" / "index.html").read_text(encoding="utf-8")
APP = (ROOT / "web" / "app.js").read_text(encoding="utf-8")

DIALOGS_WITH_FORMS = {
    "switch-dialog",
    "force-dialog",
    "logs-dialog",
    "restore-dialog",
    "update-dialog",
    "console-save-as-dialog",
}

BUTTON_RE = re.compile(r"<button\b(?P<attrs>[^>]*)>(?P<text>[^<]*)</button>", re.S)
DIALOG_RE = re.compile(r"<dialog\b[^>]*id=\"(?P<id>[^\"]+)\"[^>]*>(?P<body>.*?)</dialog>", re.S)


def dialogs() -> dict[str, str]:
    return {match.group("id"): match.group("body") for match in DIALOG_RE.finditer(HTML)}


def close_controls(body: str) -> list[tuple[str, str]]:
    controls: list[tuple[str, str]] = []
    for match in BUTTON_RE.finditer(body):
        attrs = match.group("attrs")
        text = match.group("text").strip()
        label = re.search(r'aria-label="([^"]*)"', attrs)
        if (label and label.group(1).startswith("Close ")) or text in {"Cancel", "Done"}:
            controls.append((attrs, text or (label.group(1) if label else "")))
    return controls


def test_every_dialog_close_control_is_an_explicit_button():
    found = dialogs()
    for dialog_id in DIALOGS_WITH_FORMS:
        assert dialog_id in found, f"{dialog_id} is missing"
        controls = close_controls(found[dialog_id])
        assert controls, f"{dialog_id} has no close control"
        for attrs, label in controls:
            assert 'type="button"' in attrs, f"{dialog_id} {label} is not type=button"
            assert "data-dialog-close" in attrs, f"{dialog_id} {label} lacks data-dialog-close"


def test_no_dialog_close_control_is_a_submit_button():
    for match in BUTTON_RE.finditer(HTML):
        attrs = match.group("attrs")
        if 'value="cancel"' not in attrs:
            continue
        assert 'type="button"' in attrs, f"cancel control still submits: {match.group(0)[:120]}"


def test_action_buttons_declare_explicit_intent():
    found = dialogs()
    for dialog_id in DIALOGS_WITH_FORMS:
        for match in BUTTON_RE.finditer(found[dialog_id]):
            attrs = match.group("attrs")
            if 'value="default"' in attrs:
                assert 'type="button"' not in attrs, f"{dialog_id} action button must stay a submit control"


def test_dialog_forms_require_the_action_submitter():
    assert 'event.submitter?.value === "cancel"' not in APP
    assert "data-dialog-close" in APP
    assert "submitter !== actionButton" in APP
    for statement in (
        'if (!actionSubmitter(event, switchDialog, switchButton)) return;',
        'if (!actionSubmitter(event, forceDialog, byId("force-confirm"))) return;',
        'if (!actionSubmitter(event, byId("restore-dialog"), byId("restore-confirm"))) return;',
        'if (!actionSubmitter(event, byId("update-dialog"), byId("update-confirm"))) return;',
        'if (!actionSubmitter(event, byId("console-save-as-dialog"), byId("console-export-submit"))) return;',
    ):
        assert statement in APP, f"missing submit intent guard: {statement}"


def test_lifecycle_calls_come_after_the_cancel_guard():
    for guard, action in (
        ('if (!actionSubmitter(event, forceDialog, byId("force-confirm"))) return;', "force-stop/prepare"),
        ('if (!actionSubmitter(event, switchDialog, switchButton)) return;', "/switch/prepare"),
        ('if (!actionSubmitter(event, byId("restore-dialog"), byId("restore-confirm"))) return;', "/restore/prepare"),
    ):
        guard_at = APP.index(guard)
        action_at = APP.index(action)
        assert guard_at < action_at, f"{action} is reachable before its cancel guard"


def test_confirmation_handlers_revalidate_typed_text():
    force = APP[APP.index('if (!actionSubmitter(event, forceDialog, byId("force-confirm"))) return;'):]
    force = force[: force.index("} catch (error)")]
    assert 'byId("force-confirm-text").value.trim().toLowerCase() !== expected.trim().toLowerCase()' in force
    assert 'byId("force-confirm").disabled' in force

    restore = APP[APP.index('if (!actionSubmitter(event, byId("restore-dialog"), byId("restore-confirm"))) return;'):]
    restore = restore[: restore.index("} catch (error)")]
    assert 'byId("restore-confirm-text").value.trim().toLowerCase() !== expected.trim().toLowerCase()' in restore
    assert 'byId("restore-confirm").disabled' in restore
    assert "!backupId" in restore


def test_restore_copy_matches_backend_semantics():
    assert "must be stopped first" in APP and "does not stop it for you" in APP
    assert "protected backup of the current world" in APP
    assert "changes made after the chosen backup are not in the restored world" in APP
    assert "are not part of the restored world" not in APP
