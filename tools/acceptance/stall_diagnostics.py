#!/usr/bin/env python3
"""Bounded, privacy-safe offline reporting for the timestamped perf rings.

Reads one captured ``/api/v1/perf`` payload (stdin or file, size- and row-capped
before allocation) and emits a bounded JSON summary that keeps long lifecycle
RPC separate from status reads, stalled event-loop observations and maintenance
spans. Only the finite action allowlist, durations, sequence numbers and clock
stamps survive; unknown keys, payloads, identities, paths and error text are
dropped rather than echoed.

Cross-ring correlation (maintenance vs lag vs RPC) is computed here, offline and
bounded, and is labelled correlation-only. The running tracker never scans all
pairs on the request path.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:  # pragma: no cover - shim for direct script execution
    from game_control.perf import ACTION_ENUM, LONG_LIFECYCLE_RPC_ACTIONS
except ImportError:  # pragma: no cover
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from game_control.perf import ACTION_ENUM, LONG_LIFECYCLE_RPC_ACTIONS

MAX_INPUT_BYTES = 1 << 20
MAX_ROWS = 4096
MAX_RETAINED_EVENTS = 256
MAX_OVERLAP_OUTPUT = 32
OVERLAP_LABEL = "correlation-only"
RPC_ACTION_STATUS = "status"
RPC_ACTION_OTHER = "other"
RPC_BUCKETS = ("status", "lifecycle", "other", "perf", "watch", "wait_readiness", "maintenance")
STATE_STREAMS = ("rpc", "event_loop_lag", "maintenance")
STATE_ENTRY_KEYS = frozenset({"instance", "next_sequence"})
MAX_STATE_BYTES = 4096


def validate_state(raw: Any) -> dict[str, dict[str, Any]] | None:
    """Fail closed on a cursor file that is not the documented shape."""

    if raw is None:
        return None
    if not isinstance(raw, dict) or not raw:
        raise ValueError("state must be a non-empty JSON object of stream cursors")
    if set(raw) & set(STATE_STREAMS):
        unsupported = sorted(set(raw) - set(STATE_STREAMS))
        if unsupported:
            raise ValueError(f"unsupported state key: {unsupported[0]}")
        entries = raw
    else:
        entries = {"rpc": raw}  # flat single-stream entry, applied by instance
    normalized: dict[str, dict[str, Any]] = {}
    for name, entry in entries.items():
        if not isinstance(entry, dict):
            raise ValueError(f"state entry for {name} must be an object")
        extra = sorted(set(entry) - STATE_ENTRY_KEYS)
        if extra:
            raise ValueError(f"unsupported state field for {name}: {extra[0]}")
        instance = entry.get("instance", "")
        if not isinstance(instance, str) or len(instance) > 64:
            raise ValueError(f"state instance for {name} must be a short string")
        next_sequence = entry.get("next_sequence")
        if not isinstance(next_sequence, int) or isinstance(next_sequence, bool) or next_sequence < 1:
            raise ValueError(f"state next_sequence for {name} must be an integer >= 1")
        normalized[name] = {"instance": instance, "next_sequence": next_sequence}
    return normalized


def _cursor_for(name: str, window: dict[str, Any], state: dict[str, dict[str, Any]] | None):
    """A cursor is indexed by stream; a flat state file resumes the rpc stream."""

    if not state:
        return None
    return state.get(name)


def load_payload(raw: str | bytes) -> dict[str, Any]:
    """Parse a capture without ever buffering more than the byte cap."""

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    if len(raw.encode("utf-8", "replace")) > MAX_INPUT_BYTES:
        raise ValueError("capture exceeds the bounded input cap")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("capture must be a JSON object")
    return payload


def read_capped(path: str | None) -> str:
    """Read at most MAX_INPUT_BYTES + 1 characters; never slurp an unbounded file."""

    limit = MAX_INPUT_BYTES + 1
    if path:
        with open(path, encoding="utf-8") as handle:
            data = handle.read(limit)
    else:
        data = sys.stdin.read(limit)
    if len(data.encode("utf-8", "replace")) > MAX_INPUT_BYTES:
        raise ValueError("capture exceeds the bounded input cap")
    return data


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _timestamp(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def sanitize_event(raw: Any) -> dict[str, Any] | None:
    """Keep a bounded timing record, or drop it. Never copies unknown fields."""

    if not isinstance(raw, dict):
        return None
    sequence = raw.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        return None
    action = raw.get("action")
    if not isinstance(action, str) or action not in ACTION_ENUM:
        action = RPC_ACTION_OTHER
    duration = _finite(raw.get("duration_ms"))
    if duration is None or duration < 0:
        return None
    ended_at = _timestamp(raw.get("ended_at"))
    if ended_at is None:
        return None
    start = _finite(raw.get("monotonic_start"))
    end = _finite(raw.get("monotonic_end"))
    if start is not None and end is not None and end < start:
        return None
    return {
        "sequence": sequence,
        "action": action,
        "duration_ms": duration,
        "ended_at": ended_at,
        "monotonic_start": start,
        "monotonic_end": end,
    }


def sanitize_window(raw: Any) -> dict[str, Any]:
    """Bounded window with half-open sequence, capacity and loss accounting."""

    result = {"items": [], "instance": "", "sequence": {"start": 1, "end": 1},
              "capacity": 0, "dropped": 0, "rejected": 0, "truncated": False}
    if not isinstance(raw, dict):
        return result
    raw_items = raw.get("items")
    if isinstance(raw_items, (list, tuple)):
        # Row cap is applied before allocating the retained list.
        for index, candidate in enumerate(raw_items):
            if index >= MAX_ROWS:
                result["truncated"] = True
                break
            event = sanitize_event(candidate)
            if event is not None:
                result["items"].append(event)
    if len(result["items"]) > MAX_RETAINED_EVENTS:
        result["items"] = result["items"][-MAX_RETAINED_EVENTS:]
        result["truncated"] = True
    instance = raw.get("instance")
    if isinstance(instance, str) and 0 < len(instance) <= 64:
        result["instance"] = instance
    sequence = raw.get("sequence")
    if isinstance(sequence, dict):
        start, end = sequence.get("start"), sequence.get("end")
        if isinstance(start, int) and isinstance(end, int) and not isinstance(start, bool):
            result["sequence"] = {"start": start, "end": end}
    for key, cap in (("capacity", 4096), ("dropped", None), ("rejected", None)):
        value = raw.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            result[key] = min(value, cap) if cap else value
    return result


def _stats(items: Iterable[dict[str, Any]]) -> dict[str, Any]:
    durations = sorted(item["duration_ms"] for item in items)
    if not durations:
        return {"count": 0, "max_ms": None, "total_ms": None}
    return {"count": len(durations), "max_ms": durations[-1], "total_ms": sum(durations)}


def _spans(items: Iterable[dict[str, Any]], *, prefix: str) -> list[tuple[str, float, float]]:
    spans = []
    for item in items:
        start, end = item.get("monotonic_start"), item.get("monotonic_end")
        if start is not None and end is not None:
            # Monotonic clocks are only comparable within one process instance.
            spans.append((f"{prefix}:{item['sequence']}", start, end))
    return spans


def correlate(streams: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Offline overlap correlation across streams; bounded and correlation-only."""

    spans: list[tuple[str, float, float]] = []
    for name, items in streams.items():
        spans.extend(_spans(items, prefix=name))
    spans = spans[:MAX_ROWS]
    overlaps: list[dict[str, Any]] = []
    truncated = False
    for index, (name_a, start_a, end_a) in enumerate(spans):
        for name_b, start_b, end_b in spans[index + 1:]:
            if start_a < end_b and start_b < end_a:
                if len(overlaps) >= MAX_OVERLAP_OUTPUT:
                    truncated = True
                    return {"overlaps": overlaps, "overlap_label": OVERLAP_LABEL,
                            "overlap_truncated": truncated, "overlap_candidates": len(spans)}
                overlaps.append({"a": name_a, "b": name_b, "label": OVERLAP_LABEL})
    return {"overlaps": overlaps, "overlap_label": OVERLAP_LABEL,
            "overlap_truncated": truncated, "overlap_candidates": len(spans)}


def ingest(window: dict[str, Any], state: dict[str, Any] | None = None) -> dict[str, Any]:
    """Consume one stream's window with that stream's cursor entry.

    ``state`` is a single per-stream entry (``{"instance": ..., "next_sequence":
    ...}``), never the whole per-stream mapping. It separates records the ring
    evicted before this read from records the previous read already consumed.
    """

    instance = window.get("instance") or ""
    sequence = window.get("sequence") or {"start": 1, "end": 1}
    start, end = sequence.get("start", 1), sequence.get("end", 1)
    items = window.get("items") or []
    prior_instance = (state or {}).get("instance")
    prior_next = (state or {}).get("next_sequence")
    reset = bool(state) and prior_instance != instance
    baseline = prior_next is None
    if baseline:
        expected = start
        evicted_since_capture = 0
        unobserved: int | None = None
    elif reset:
        # A new process instance invalidates the old cursor entirely: the
        # number of records missed across the restart is not knowable.
        expected = start
        evicted_since_capture = 0
        unobserved = None
    else:
        expected = max(start, prior_next)
        evicted_since_capture = max(0, start - prior_next)
        unobserved = evicted_since_capture
    fresh = [item for item in items if item["sequence"] >= expected]
    duplicates = 0
    if prior_next is not None and not reset:
        duplicates = max(0, min(prior_next, end) - start) if end < prior_next else 0
    next_sequence = max([end] + [item["sequence"] + 1 for item in fresh]) if fresh else end
    return {
        "instance": instance,
        "instance_reset": reset,
        "baseline_capture": baseline,
        "evicted_since_last_capture": evicted_since_capture,
        "unobserved_records": unobserved,
        "dropped_lifetime": window.get("dropped", 0),
        "duplicates_skipped": duplicates,
        "consumed": fresh,
        "next_sequence": next_sequence,
    }


def summarize(payload: dict[str, Any], *, slotd_key: str = "slotd", state: dict[str, Any] | None = None) -> dict[str, Any]:
    """Bounded report; absent rings surface as explicit gaps, not zeros."""

    slotd = payload.get(slotd_key) if isinstance(payload, dict) else None
    if not isinstance(slotd, dict):
        slotd = {}
    rpc = sanitize_window(slotd.get("rpc_events"))
    lag = sanitize_window(slotd.get("event_loop_lag_events"))
    maintenance = sanitize_window(slotd.get("maintenance_events"))
    buckets: dict[str, list[dict[str, Any]]] = {key: [] for key in RPC_BUCKETS}
    for event in rpc["items"]:
        action = event["action"]
        if action in LONG_LIFECYCLE_RPC_ACTIONS:
            buckets["lifecycle"].append(event)
        elif action in buckets:
            buckets[action].append(event)
        else:
            buckets["other"].append(event)
    state = validate_state(state)
    loss = []
    for name, window in (("rpc", rpc), ("event_loop_lag", lag), ("maintenance", maintenance)):
        cursor = _cursor_for(name, window, state)
        entry = ingest(window, cursor)
        entry["stream"] = name
        entry.pop("consumed", None)
        loss.append(entry)
    correlation = correlate({"rpc": rpc["items"], "lag": lag["items"], "maintenance": maintenance["items"]})
    gaps = [
        "status_ui_latency_ms has no source in the perf ring",
        "monotonic spans are only comparable within one process instance",
        "dropped_lifetime counts ring evictions; evicted_since_last_capture is what a cursor missed",
        "an overlap is timing correlation, not causation or attribution",
    ]
    if not rpc["items"] and not lag["items"] and not maintenance["items"]:
        gaps.append("no timestamped diagnostic ring present in this capture")
    if rpc["truncated"] or lag["truncated"] or maintenance["truncated"]:
        gaps.append("input rows exceeded the bounded cap and were truncated")
    return {
        "schema": "stall-diagnostics/2",
        "instance": rpc["instance"] or lag["instance"] or maintenance["instance"],
        "rpc": {name: _stats(events) for name, events in buckets.items()},
        "rpc_events": rpc["items"],
        "stalled_loop": _stats(lag["items"]),
        "event_loop_lag_events": lag["items"],
        "maintenance": _stats(maintenance["items"]),
        "maintenance_events": maintenance["items"],
        "loss": loss,
        "gaps": gaps,
        **correlation,
    }


def _load_state(path: str | None) -> dict[str, Any] | None:
    """Load and validate a cursor file; missing means a first capture."""

    if not path:
        return None
    handle = Path(path)
    if not handle.exists():
        return None
    with open(path, "rb") as raw:
        data = raw.read(MAX_STATE_BYTES + 1)
    if len(data) > MAX_STATE_BYTES:
        raise ValueError("state file exceeds the bounded cap")
    try:
        parsed = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("state file is not valid JSON")
    return validate_state(parsed)


def _save_state(path: str | None, report: dict[str, Any]) -> None:
    """Persist cursors atomically (0600, fsynced) so a kill cannot corrupt them."""

    if not path:
        return
    state = {entry["stream"]: entry for entry in report["loss"]}
    payload = {name: {"instance": entry["instance"], "next_sequence": entry["next_sequence"]}
               for name, entry in state.items()}
    target = Path(path)
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    if len(blob) > MAX_STATE_BYTES:
        raise ValueError("state payload exceeds the bounded cap")
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(blob)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    os.replace(temporary, target)
    directory = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize timestamped perf diagnostics")
    parser.add_argument("--input", help="perf snapshot JSON file; default stdin")
    parser.add_argument("--slotd-key", default="slotd")
    parser.add_argument("--state", help="optional small cursor file for repeated captures")
    args = parser.parse_args(argv)
    report = summarize(load_payload(read_capped(args.input)), slotd_key=args.slotd_key,
                       state=_load_state(args.state))
    _save_state(args.state, report)
    json.dump(report, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
