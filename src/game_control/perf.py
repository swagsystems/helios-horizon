"""In-memory performance measurements for the slot daemon."""

from __future__ import annotations

import math
import sqlite3
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4


FLUSH_INTERVAL = timedelta(minutes=5)

# Retained rows per timestamped stream. This is the runtime cap and must stay
# equal to PerfEventWindow.items max_length so a full ring always serialises.
MAX_EVENT_ROWS = 256

# Finite allowlist of operation/action labels for timestamped diagnostics.
# Unknown/malformed request kinds are mapped to ``other`` and never echoed, so
# the retained evidence cannot carry caller-controlled cardinality or identity.
RPC_ACTION_OTHER = "other"
RPC_ACTION_STATUS = "status"
RPC_ACTION_PERF = "perf"
RPC_ACTION_WATCH = "watch"
RPC_ACTION_WAIT_READINESS = "wait_readiness"
RPC_ACTION_START = "start"
RPC_ACTION_STOP = "stop"
RPC_ACTION_RESTART = "restart"
RPC_ACTION_BACKUP = "backup"
RPC_ACTION_RESTORE = "restore"
RPC_ACTION_UPDATE = "update"
RPC_ACTION_SWITCH = "switch"
RPC_ACTION_RETIREMENT = "retirement"
RPC_ACTION_WORLD_CLONE = "world_clone"
RPC_ACTION_BENCHMARK = "benchmark"
RPC_ACTION_COMMAND = "command"
RPC_ACTION_CONFIG = "config"
RPC_ACTION_NOTIFICATION = "notification"
RPC_ACTION_MAINTENANCE = "maintenance"

# Lifecycle actions can block the controller for minutes; status reads must
# stay separable from them in any retained evidence.
LONG_LIFECYCLE_RPC_ACTIONS = frozenset({
    RPC_ACTION_START, RPC_ACTION_STOP, RPC_ACTION_RESTART, RPC_ACTION_BACKUP,
    RPC_ACTION_RESTORE, RPC_ACTION_UPDATE, RPC_ACTION_SWITCH, RPC_ACTION_RETIREMENT,
    RPC_ACTION_WORLD_CLONE, RPC_ACTION_BENCHMARK,
})

_ACTION_ENUM_BY_KIND: dict[str, str] = {
    "get_status": RPC_ACTION_STATUS,
    "get_perf": RPC_ACTION_PERF,
    "watch": RPC_ACTION_WATCH,
    "wait_readiness": RPC_ACTION_WAIT_READINESS,
    "start": RPC_ACTION_START,
    "stop": RPC_ACTION_STOP,
    "prepare_force_stop": RPC_ACTION_STOP,
    "confirm_force_stop": RPC_ACTION_STOP,
    "restart": RPC_ACTION_RESTART,
    "create_backup": RPC_ACTION_BACKUP,
    "list_backups": RPC_ACTION_BACKUP,
    "list_aggregate_backups": RPC_ACTION_BACKUP,
    "prepare_restore": RPC_ACTION_RESTORE,
    "confirm_restore": RPC_ACTION_RESTORE,
    "check_update": RPC_ACTION_UPDATE,
    "prepare_update": RPC_ACTION_UPDATE,
    "confirm_update": RPC_ACTION_UPDATE,
    "prepare_switch": RPC_ACTION_SWITCH,
    "confirm_switch": RPC_ACTION_SWITCH,
    "prepare_retirement": RPC_ACTION_RETIREMENT,
    "confirm_retirement": RPC_ACTION_RETIREMENT,
    "get_retirement_status": RPC_ACTION_RETIREMENT,
    "prepare_world_clone": RPC_ACTION_WORLD_CLONE,
    "confirm_world_clone": RPC_ACTION_WORLD_CLONE,
    "run_benchmark": RPC_ACTION_BENCHMARK,
    "cancel_benchmark": RPC_ACTION_BENCHMARK,
    "get_benchmarks": RPC_ACTION_BENCHMARK,
    "export_benchmarks": RPC_ACTION_BENCHMARK,
    "command": RPC_ACTION_COMMAND,
    "get_profiles": RPC_ACTION_CONFIG,
    "get_profile_config": RPC_ACTION_CONFIG,
    "set_profile_config": RPC_ACTION_CONFIG,
    "get_schedules": RPC_ACTION_CONFIG,
    "set_schedules": RPC_ACTION_CONFIG,
    "set_idle_stop": RPC_ACTION_CONFIG,
    "get_notification_config": RPC_ACTION_NOTIFICATION,
    "set_notification_rule": RPC_ACTION_NOTIFICATION,
    "test_notification": RPC_ACTION_NOTIFICATION,
    "list_audit": RPC_ACTION_OTHER,
    "list_events": RPC_ACTION_OTHER,
    "get_logs": RPC_ACTION_OTHER,
    "get_stats_summary": RPC_ACTION_OTHER,
    "get_stats_heatmap": RPC_ACTION_OTHER,
    "get_stats_tps": RPC_ACTION_OTHER,
}

ACTION_ENUM = frozenset(_ACTION_ENUM_BY_KIND.values()) | {RPC_ACTION_MAINTENANCE}


def classify_rpc_action(action: Any) -> str:
    """Map a typed controller action to a finite, identity-free label."""

    kind = getattr(action, "kind", None)
    if not isinstance(kind, str):
        return RPC_ACTION_OTHER
    return _ACTION_ENUM_BY_KIND.get(kind, RPC_ACTION_OTHER)


class _Window:
    def __init__(self, *, maxlen: int = 1024):
        self.samples: deque[float] = deque(maxlen=maxlen)
        self.interval_samples: deque[float] = deque(maxlen=maxlen)
        self.interval_count = 0
        self.interval_total = 0.0
        self.interval_max = 0.0
        self.total_count = 0

    def record(self, value: float) -> None:
        value = max(0.0, float(value))
        self.samples.append(value)
        self.total_count += 1
        self.interval_samples.append(value)
        self.interval_count += 1
        self.interval_total += value
        self.interval_max = max(self.interval_max, value)

    def snapshot(self) -> dict[str, float | int | None]:
        return _aggregate(self.samples)

    def interval_snapshot(self) -> dict[str, float | int | None]:
        values = self.interval_samples
        if not values:
            return {"count": 0, "avg_ms": None, "p95_ms": None, "max_ms": None}
        ordered = sorted(values)
        return {
            "count": self.interval_count,
            "avg_ms": self.interval_total / self.interval_count,
            "p95_ms": _percentile(ordered, 0.95),
            "max_ms": self.interval_max,
        }

    def reset_interval(self) -> None:
        self.interval_samples.clear()
        self.interval_count = 0
        self.interval_total = 0.0
        self.interval_max = 0.0


def _aggregate(values: Any) -> dict[str, float | int | None]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "avg_ms": None, "p95_ms": None, "max_ms": None}
    return {
        "count": len(ordered),
        "avg_ms": sum(ordered) / len(ordered),
        "p95_ms": _percentile(ordered, 0.95),
        "max_ms": ordered[-1],
    }


def _percentile(values: list[float], fraction: float) -> float:
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] + (values[upper] - values[lower]) * weight


def _span(duration_ms: Any, monotonic_start: Any, monotonic_end: Any) -> tuple[float, float]:
    """Derive a monotonic span from the caller's value or the recorded duration."""

    end = _finite_or_none(monotonic_end)
    start = _finite_or_none(monotonic_start)
    if end is None:
        end = time.monotonic()
    if start is None:
        try:
            seconds = max(0.0, float(duration_ms)) / 1000.0
        except (TypeError, ValueError):
            seconds = 0.0
        start = end - seconds
    return start, end


class TimestampedEventRing:
    """Bounded ring of timestamped events with explicit loss accounting.

    ``record`` is O(1); ``window`` is bounded O(n) over the retained entries and
    performs no pairwise or cross-ring work.

    Sequence numbers are 1-based and the retained window is reported half-open:
    ``sequence = {"start": s, "end": e}`` means the retained items carry the
    sequences ``s .. e-1`` and ``e`` is the next sequence to be assigned. An
    empty window is therefore ``{"start": 1, "end": 1}`` and a single recorded
    item is ``{"start": 1, "end": 2}``. ``dropped`` counts records evicted by
    the cap over the ring's lifetime, which a collector must separate from the
    records it actually missed between captures.
    """

    def __init__(self, *, maxlen: int = 256, instance: str = ""):
        if not 1 <= maxlen <= 65536:
            raise ValueError("performance event ring bound out of range")
        self.maxlen = maxlen
        self.instance = instance
        self.events: deque[dict[str, Any]] = deque(maxlen=maxlen)
        self.total_count = 0
        self.rejected = 0

    @property
    def dropped(self) -> int:
        """Records evicted by the cap; a collector must resynchronise."""

        return self.total_count - len(self.events)

    def reject(self) -> None:
        """Count an observation that never entered the ring."""

        self.rejected += 1

    def record(
        self,
        *,
        action: str,
        duration_ms: Any,
        now: datetime,
        monotonic_start: float | None = None,
        monotonic_end: float | None = None,
    ) -> bool:
        if not isinstance(action, str) or action not in ACTION_ENUM:
            action = RPC_ACTION_OTHER
        now = _coerce_now(now)
        if now is None:
            self.rejected += 1
            return False
        duration = _finite_or_none(duration_ms)
        if duration is None:
            self.rejected += 1
            return False
        duration = max(0.0, duration)
        start = _finite_or_none(monotonic_start)
        end = _finite_or_none(monotonic_end)
        if start is not None and end is not None and end < start:
            self.rejected += 1
            return False
        self.total_count += 1
        self.events.append({
            "sequence": self.total_count,
            "action": action,
            "duration_ms": duration,
            "ended_at": _iso(now),
            "monotonic_start": start,
            "monotonic_end": end,
        })
        return True

    def window(self) -> dict[str, Any]:
        items = list(self.events)
        # Half-open: retained sequences are start..next-1.
        start = self.total_count - len(items) + 1
        return {
            "items": items,
            "instance": self.instance,
            "sequence": {"start": start, "end": self.total_count + 1},
            "capacity": self.maxlen,
            "dropped": self.dropped,
            "rejected": self.rejected,
        }


def _coerce_now(value: Any) -> datetime | None:
    """Return an aware UTC datetime, or None when the caller's clock is invalid."""

    if not isinstance(value, datetime):
        return None
    try:
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _finite_or_none(value: Any) -> float | None:
    # Only real numbers are accepted: booleans and numeric-looking strings are
    # type errors, not measurements.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


class PerformanceTracker:
    """Append-only collection (O(1) per record) with bounded O(n) snapshots."""

    def __init__(self, *, maxlen: int = 1024, event_maxlen: int = 256):
        if not 1 <= maxlen <= 65536:
            raise ValueError("performance ring bound out of range")
        # Must not exceed PerfEventWindow.items max_length on the wire.
        if not 1 <= event_maxlen <= MAX_EVENT_ROWS:
            raise ValueError("performance event ring bound out of range")
        self.cycle = _Window(maxlen=maxlen)
        self.rpc = _Window(maxlen=maxlen)
        # Keep the response bounded. The performance collector uses the monotonic
        # sequence below to consume unseen values and rejects an overwrite.
        self.event_loop_lag = _Window(maxlen=maxlen)
        self.maintenance = _Window(maxlen=maxlen)
        # Additive timestamped diagnostics. These carry timing only: a finite
        # action label, a duration, a UTC timestamp and monotonic span. No
        # payload, identity, path or error text is retained.
        self.instance = uuid4().hex
        self.rpc_events = TimestampedEventRing(maxlen=event_maxlen, instance=self.instance)
        self.event_loop_lag_events = TimestampedEventRing(maxlen=event_maxlen, instance=self.instance)
        self.maintenance_events = TimestampedEventRing(maxlen=event_maxlen, instance=self.instance)
        self._last_flush: datetime | None = None

    def record_cycle(self, duration_ms: float) -> None:
        self.cycle.record(duration_ms)

    def record_rpc(
        self,
        duration_ms: float,
        *,
        action: Any = None,
        now: datetime | None = None,
        monotonic_start: float | None = None,
        monotonic_end: float | None = None,
    ) -> None:
        value = _finite_or_none(duration_ms)
        if value is None:
            self.rpc_events.reject()
            return
        self.rpc.record(value)
        start, end = _span(duration_ms, monotonic_start, monotonic_end)
        self.rpc_events.record(
            action=classify_rpc_action(action),
            duration_ms=value,
            now=now if now is not None else datetime.now(timezone.utc),
            monotonic_start=start,
            monotonic_end=end,
        )

    def record_event_loop_lag(self, duration_ms: float, *, now: datetime | None = None) -> None:
        value = _finite_or_none(duration_ms)
        if value is None:
            self.event_loop_lag_events.reject()
            return
        self.event_loop_lag.record(value)
        start, end = _span(duration_ms, None, None)
        self.event_loop_lag_events.record(
            action=RPC_ACTION_OTHER,
            duration_ms=value,
            now=now if now is not None else datetime.now(timezone.utc),
            monotonic_start=start,
            monotonic_end=end,
        )

    def record_maintenance(
        self,
        duration_ms: float,
        *,
        now: datetime | None = None,
        monotonic_start: float | None = None,
        monotonic_end: float | None = None,
    ) -> None:
        value = _finite_or_none(duration_ms)
        if value is None:
            self.maintenance_events.reject()
            return
        self.maintenance.record(value)
        start, end = _span(duration_ms, monotonic_start, monotonic_end)
        self.maintenance_events.record(
            action=RPC_ACTION_MAINTENANCE,
            duration_ms=value,
            now=now if now is not None else datetime.now(timezone.utc),
            monotonic_start=start,
            monotonic_end=end,
        )

    def snapshot(self) -> dict[str, dict[str, float | int | None] | list[float] | dict[str, int]]:
        event_values = list(self.event_loop_lag.samples)
        event_end = self.event_loop_lag.total_count
        maintenance_values = list(self.maintenance.samples)
        maintenance_end = self.maintenance.total_count
        return {"cycle": self.cycle.snapshot(), "rpc": self.rpc.snapshot(),
                "maintenance": self.maintenance.snapshot(),
                "maintenance_ms": maintenance_values,
                "maintenance_sequence": {
                    "start": maintenance_end - len(maintenance_values), "end": maintenance_end,
                },
                "event_loop_lag_ms": event_values,
                "event_loop_lag_sequence": {
                    "start": event_end - len(event_values), "end": event_end,
                },
                "instance": self.instance,
                "rpc_events": self.rpc_events.window(),
                "event_loop_lag_events": self.event_loop_lag_events.window(),
                "maintenance_events": self.maintenance_events.window()}

    def flush_if_due(
        self,
        connection: sqlite3.Connection,
        *,
        now: datetime,
        profile_id: str = "slotd",
    ) -> int:
        now = _aware(now)
        if self._last_flush is None:
            self._last_flush = now
            return 0
        if now - self._last_flush < FLUSH_INTERVAL:
            return 0
        aggregates = {
            "perf.cycle_avg_ms": self.cycle.interval_snapshot()["avg_ms"],
            "perf.cycle_p95_ms": self.cycle.interval_snapshot()["p95_ms"],
            "perf.cycle_max_ms": self.cycle.interval_snapshot()["max_ms"],
            "perf.rpc_avg_ms": self.rpc.interval_snapshot()["avg_ms"],
            "perf.rpc_p95_ms": self.rpc.interval_snapshot()["p95_ms"],
            "perf.rpc_max_ms": self.rpc.interval_snapshot()["max_ms"],
        }
        rows = [
            (profile_id, metric, _iso(now), float(value))
            for metric, value in aggregates.items()
            if value is not None
        ]
        if rows:
            with connection:
                connection.executemany(
                    "INSERT INTO metric_samples(profile_id, metric, ts, value) VALUES (?, ?, ?, ?)",
                    rows,
                )
        self.cycle.reset_interval()
        self.rpc.reset_interval()
        self._last_flush = now
        return len(rows)


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo and value.utcoffset() is not None else value.replace(tzinfo=timezone.utc)


def _iso(value: datetime) -> str:
    return _aware(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = ["FLUSH_INTERVAL", "MAX_EVENT_ROWS", "PerformanceTracker"]
