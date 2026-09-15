#!/usr/bin/env python3
"""Emit bounded Prometheus textfile metrics for the laptop archive sender.

Input is one bounded JSON status document on standard input (the sender's local
status receipt). Output is a Prometheus textfile exposition on standard output.
The module embeds no path, host, unit or credential: a private, root-owned
wrapper reads the document from the trusted host and installs the output.

A collector failure publishes an explicit failure series and a fresh failure
timestamp, and deliberately does not re-emit a previously fresh success
timestamp, so an unreadable status document cannot masquerade as fresh success.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime, timezone
from typing import Any

MAX_INPUT_BYTES = 64 * 1024
SCHEMA = 1
METRIC_PREFIX = "helios_laptop_backup_"
FUTURE_TOLERANCE_SECONDS = 300.0

# Fixed status-class taxonomy. The sender writes "success", "running", "empty"
# (completed with no selected archives) and "failed"; anything else fails the
# collector so an unknown state cannot be silently classified.
STATE_CLASS = {
    "success": "success",
    "running": "running",
    "empty": "completed",
    "failed": "failure",
}

# Fixed reason taxonomy. Raw reason strings are never echoed into metrics.
KNOWN_REASONS = frozenset(
    {
        "verified",
        "no_archives_selected",
        "in_progress",
        "non_ac",
        "low_battery",
        "wrong_receiver",
        "capacity_budget",
        "capacity_reserve",
        "malformed_response",
        "remote_error",
        "integrity_failed",
        "transport_error",
        "local_error",
        "unknown",
    }
)
DEFERRAL_REASONS = frozenset({"non_ac", "low_battery"})
UNKNOWN_REASON = "unknown"


class CollectorError(RuntimeError):
    """The status document could not be read as a valid, bounded receipt."""


def _parse_timestamp(value: Any) -> float | None:
    """Return a finite UTC epoch, or None for invalid/naive/overflowing input."""

    if not isinstance(value, str) or not value or len(value) > 64:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except (ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None  # reject naive timestamps rather than assume a zone
    try:
        epoch = parsed.timestamp()
    except (OverflowError, OSError, ValueError):
        return None
    if not math.isfinite(epoch):
        return None
    return epoch


def _require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CollectorError(f"{name} is not an object")
    return value


def _bounded_timestamp(value: Any, *, now: float, name: str) -> float | None:
    if value is None:
        return None
    parsed = _parse_timestamp(value)
    if parsed is None:
        raise CollectorError(f"{name} timestamp is invalid")
    if parsed > now + FUTURE_TOLERANCE_SECONDS:
        raise CollectorError(f"{name} timestamp is in the future")
    return parsed


def classify(document: Any, *, now: float | None = None) -> dict[str, Any]:
    """Validate one status document and return the allowlisted metric view."""

    now = time.time() if now is None else now
    root = _require_mapping(document, "status")
    if root.get("schema") != SCHEMA:
        raise CollectorError("unsupported status schema")
    attempt = _require_mapping(root.get("attempt"), "attempt")
    last_success = root.get("last_success")
    if last_success is not None:
        last_success = _require_mapping(last_success, "last_success")

    state = attempt.get("state")
    if not isinstance(state, str) or state not in STATE_CLASS:
        raise CollectorError("attempt state is unrecognised")
    classification = STATE_CLASS[state]

    raw_reason = attempt.get("reason")
    reason = raw_reason if isinstance(raw_reason, str) and raw_reason in KNOWN_REASONS else UNKNOWN_REASON
    deferred = classification == "failure" and reason in DEFERRAL_REASONS

    success = 1.0 if classification == "success" else 0.0
    running = 1.0 if classification == "running" else 0.0
    hard_failure = 1.0 if classification == "failure" and not deferred else 0.0

    last_success_at = None
    if last_success is not None:
        last_success_at = _bounded_timestamp(
            last_success.get("finished_at"), now=now, name="last_success"
        )
    attempt_at = _bounded_timestamp(attempt.get("finished_at"), now=now, name="attempt")
    if attempt_at is None:
        attempt_at = _bounded_timestamp(attempt.get("started_at"), now=now, name="attempt")

    return {
        "success": success,
        "running": running,
        "deferred": 1.0 if deferred else 0.0,
        "hard_failure": hard_failure,
        "reason": reason,
        "last_success_timestamp": last_success_at,
        "attempt_timestamp": attempt_at,
    }


def _series(name: str, value: float, help_text: str, *, kind: str = "gauge") -> list[str]:
    return [
        f"# HELP {name} {help_text}",
        f"# TYPE {name} {kind}",
        f"{name} {value:.6f}",
    ]


def render(view: dict[str, Any], *, now: float) -> list[str]:
    if not math.isfinite(now):
        raise CollectorError("now is invalid")
    lines = _series(
        f"{METRIC_PREFIX}collector_success",
        1.0,
        "1 when the laptop backup status collector parsed a bounded receipt.",
    )
    lines += _series(
        f"{METRIC_PREFIX}collector_timestamp_seconds",
        now,
        "Unix time of the latest successful collector run.",
    )
    if view["last_success_timestamp"] is not None:
        lines += _series(
            f"{METRIC_PREFIX}last_success_timestamp_seconds",
            view["last_success_timestamp"],
            "Unix time of the latest verified laptop backup run.",
        )
    if view["attempt_timestamp"] is not None:
        lines += _series(
            f"{METRIC_PREFIX}last_attempt_timestamp_seconds",
            view["attempt_timestamp"],
            "Unix time of the latest laptop backup attempt.",
        )
    lines += _series(
        f"{METRIC_PREFIX}last_attempt_success",
        view["success"],
        "1 when the latest laptop backup attempt succeeded.",
    )
    lines += _series(
        f"{METRIC_PREFIX}last_attempt_running",
        view["running"],
        "1 while a laptop backup attempt is in progress.",
    )
    lines += _series(
        f"{METRIC_PREFIX}last_attempt_deferred",
        view["deferred"],
        "1 when the latest attempt was an intentional AC or battery deferral.",
    )
    lines += _series(
        f"{METRIC_PREFIX}last_attempt_hard_failure",
        view["hard_failure"],
        "1 when the latest attempt failed for a reason other than AC or battery deferral.",
    )
    lines.append("# HELP " + METRIC_PREFIX + "reason_info Latest attempt reason from a fixed taxonomy.")
    lines.append("# TYPE " + METRIC_PREFIX + "reason_info gauge")
    lines.append(f'{METRIC_PREFIX}reason_info{{code="{view["reason"]}"}} 1.000000')
    return lines


def _safe_now(value: float | None) -> float:
    if value is None:
        return time.time()
    try:
        number = float(value)
    except (TypeError, ValueError):
        return time.time()
    return number if math.isfinite(number) else time.time()


def render_failure(*, now: float | None = None) -> list[str]:
    safe_now = _safe_now(now)
    lines = _series(
        f"{METRIC_PREFIX}collector_success",
        0.0,
        "1 when the laptop backup status collector parsed a bounded receipt.",
    )
    lines += _series(
        f"{METRIC_PREFIX}collector_timestamp_seconds",
        safe_now,
        "Unix time of the latest successful collector run.",
    )
    return lines


def load(stream: Any, *, limit: int = MAX_INPUT_BYTES) -> Any:
    raw = stream.read(limit + 1)
    if not isinstance(raw, (bytes, bytearray)):
        raise CollectorError("status input is not bytes")
    if len(raw) > limit:
        raise CollectorError("status input exceeds the bounded limit")
    try:
        return json.loads(bytes(raw).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CollectorError("status input is not valid JSON") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="horizon-laptop-backup-collector")
    parser.add_argument("--now", type=float, default=None, help="override the collector timestamp")
    args = parser.parse_args(argv)
    now = _safe_now(args.now)
    try:
        document = load(sys.stdin.buffer)
        view = classify(document, now=now)
        lines = render(view, now=now)
    except CollectorError:
        lines = render_failure(now=now)
    sys.stdout.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
