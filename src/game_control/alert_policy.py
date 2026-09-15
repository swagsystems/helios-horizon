"""Bounded, inactive-aware performance alert evaluation and ownership.

Slotd evaluates only game-scoped signals. Host disk pressure remains owned by
Prometheus/Alertmanager and is intentionally not accepted by the evaluator.
"""

from __future__ import annotations

import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Callable, Mapping


MSPT_P95_THRESHOLD_MS = 50.0
MSPT_SUSTAINED_SECONDS = 30.0
MSPT_MIN_SAMPLES = 4
MEMORY_GROWTH_WINDOW_SECONDS = 600.0
MEMORY_GROWTH_THRESHOLD_BYTES = 512 * 1024 * 1024
MEMORY_RECOVERY_BYTES = 256 * 1024 * 1024
WAKE_SLO_THRESHOLD_MS = 180_000.0
MAX_RUNTIME_SAMPLES = 256


class AlertSignal(StrEnum):
    SUSTAINED_MSPT = "sustained_mspt"
    MEMORY_GROWTH = "memory_growth"
    DISK_PRESSURE = "disk_pressure"
    WAKE_SLO = "wake_slo"
    BENCHMARK_REGRESSION = "benchmark_regression"


class AlertOwner(StrEnum):
    SLOTD = "slotd"
    PROMETHEUS_ALERTMANAGER = "prometheus_alertmanager"


class ActivityPolicy(StrEnum):
    RUNNING_ONLY = "running_only"
    STARTING_OR_RUNNING = "starting_or_running"
    ALWAYS = "always"


@dataclass(frozen=True, slots=True)
class AlertRule:
    signal: AlertSignal
    owner: AlertOwner
    activity: ActivityPolicy
    dedup_group: str
    priority: int
    threshold: float | None
    window_seconds: float


@dataclass(frozen=True, slots=True)
class AlertEmission:
    signal: AlertSignal
    generation: int
    message: str


ALERT_RULES = (
    AlertRule(AlertSignal.SUSTAINED_MSPT, AlertOwner.SLOTD, ActivityPolicy.RUNNING_ONLY,
              "runtime_pressure", 10, MSPT_P95_THRESHOLD_MS, MSPT_SUSTAINED_SECONDS),
    AlertRule(AlertSignal.MEMORY_GROWTH, AlertOwner.SLOTD, ActivityPolicy.RUNNING_ONLY,
              "runtime_pressure", 20, MEMORY_GROWTH_THRESHOLD_BYTES, MEMORY_GROWTH_WINDOW_SECONDS),
    AlertRule(AlertSignal.DISK_PRESSURE, AlertOwner.PROMETHEUS_ALERTMANAGER, ActivityPolicy.ALWAYS,
              "host_storage", 20, None, 0),
    AlertRule(AlertSignal.WAKE_SLO, AlertOwner.SLOTD, ActivityPolicy.STARTING_OR_RUNNING,
              "wake_path", 20, WAKE_SLO_THRESHOLD_MS, 0),
    AlertRule(AlertSignal.BENCHMARK_REGRESSION, AlertOwner.SLOTD, ActivityPolicy.ALWAYS,
              "benchmark", 20, None, 0),
)


def _build_registry() -> Mapping[AlertSignal, AlertRule]:
    registry: dict[AlertSignal, AlertRule] = {}
    for rule in ALERT_RULES:
        if rule.signal in registry:
            raise RuntimeError("duplicate alert signal ownership")
        if not rule.dedup_group or not 0 <= rule.priority <= 100:
            raise RuntimeError("invalid alert policy")
        registry[rule.signal] = rule
    if set(registry) != set(AlertSignal):
        raise RuntimeError("incomplete alert signal ownership")
    return MappingProxyType(registry)


ALERT_POLICY = _build_registry()

_MESSAGES = MappingProxyType({
    AlertSignal.SUSTAINED_MSPT: "Sunlit tick time has remained above the 50 ms p95 threshold.",
    AlertSignal.MEMORY_GROWTH: "Sunlit resident memory grew by at least 512 MiB over ten minutes.",
    AlertSignal.WAKE_SLO: "Sunlit wake readiness exceeded the 180 second objective.",
    AlertSignal.BENCHMARK_REGRESSION: "The latest Sunlit benchmark crossed its declared regression threshold.",
})


def _finite(value: float | int | None) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number >= 0 else None


class PerformanceAlertEvaluator:
    """Evaluate fixed slotd rules and emit once per root-cause incident."""

    def __init__(self, *, generation_clock: Callable[[], int] | None = None) -> None:
        self._mspt: dict[str, deque[tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=MAX_RUNTIME_SAMPLES)
        )
        self._rss: dict[str, deque[tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=MAX_RUNTIME_SAMPLES)
        )
        self._breached: dict[str, dict[AlertSignal, bool]] = defaultdict(dict)
        self._active_groups: dict[str, set[str]] = defaultdict(set)
        self._memory_incident_peak: dict[str, float] = {}
        self._generation_clock = generation_clock or (lambda: time.time_ns() // 1_000_000)
        self._last_generation = 0

    def _generation(self) -> int:
        candidate = int(self._generation_clock())
        self._last_generation = max(self._last_generation + 1, candidate)
        return self._last_generation

    def observe(
        self,
        profile_id: str,
        *,
        profile_state: str,
        now: float,
        mspt_p95: float | None = None,
        rss_bytes: float | None = None,
        wake_duration_ms: float | None = None,
        benchmark_regression: bool | None = None,
    ) -> tuple[AlertEmission, ...]:
        if not isinstance(profile_id, str) or not profile_id or len(profile_id) > 128:
            raise ValueError("invalid alert profile")
        timestamp = _finite(now)
        if timestamp is None:
            raise ValueError("invalid alert timestamp")
        current = self._breached[profile_id]
        running = profile_state == "running"

        if not running:
            self._mspt.pop(profile_id, None)
            self._rss.pop(profile_id, None)
            self._memory_incident_peak.pop(profile_id, None)
            current[AlertSignal.SUSTAINED_MSPT] = False
            current[AlertSignal.MEMORY_GROWTH] = False
        else:
            mspt = _finite(mspt_p95)
            if mspt is not None:
                samples = self._mspt[profile_id]
                samples.append((timestamp, mspt))
                while samples and samples[0][0] < timestamp - MSPT_SUSTAINED_SECONDS:
                    samples.popleft()
                current[AlertSignal.SUSTAINED_MSPT] = bool(
                    len(samples) >= MSPT_MIN_SAMPLES
                    and samples[0][0] <= timestamp - MSPT_SUSTAINED_SECONDS
                    and all(value >= MSPT_P95_THRESHOLD_MS for _stamp, value in samples)
                )

            rss = _finite(rss_bytes)
            if rss is not None:
                samples = self._rss[profile_id]
                samples.append((timestamp, rss))
                while samples and samples[0][0] < timestamp - MEMORY_GROWTH_WINDOW_SECONDS:
                    samples.popleft()
                span = timestamp - samples[0][0] if samples else 0
                growth = rss - samples[0][1] if samples else 0
                was_breached = current.get(AlertSignal.MEMORY_GROWTH, False)
                if was_breached:
                    peak = max(self._memory_incident_peak.get(profile_id, rss), rss)
                    self._memory_incident_peak[profile_id] = peak
                    current[AlertSignal.MEMORY_GROWTH] = peak - rss < MEMORY_RECOVERY_BYTES
                    if not current[AlertSignal.MEMORY_GROWTH]:
                        self._memory_incident_peak.pop(profile_id, None)
                else:
                    current[AlertSignal.MEMORY_GROWTH] = (
                        span >= MEMORY_GROWTH_WINDOW_SECONDS and growth >= MEMORY_GROWTH_THRESHOLD_BYTES
                    )
                    if current[AlertSignal.MEMORY_GROWTH]:
                        self._memory_incident_peak[profile_id] = rss

        wake = _finite(wake_duration_ms)
        if wake is not None:
            current[AlertSignal.WAKE_SLO] = wake > WAKE_SLO_THRESHOLD_MS
        elif profile_state not in {"starting", "running"}:
            current[AlertSignal.WAKE_SLO] = False
        if benchmark_regression is not None:
            if not isinstance(benchmark_regression, bool):
                raise ValueError("invalid benchmark regression state")
            current[AlertSignal.BENCHMARK_REGRESSION] = benchmark_regression

        candidates: dict[str, AlertRule] = {}
        for signal, breached in current.items():
            rule = ALERT_POLICY[signal]
            if rule.owner is not AlertOwner.SLOTD or not breached:
                continue
            if rule.activity is ActivityPolicy.RUNNING_ONLY and not running:
                continue
            if rule.activity is ActivityPolicy.STARTING_OR_RUNNING and profile_state not in {"starting", "running"}:
                continue
            selected = candidates.get(rule.dedup_group)
            if selected is None or rule.priority > selected.priority:
                candidates[rule.dedup_group] = rule

        active_groups = self._active_groups[profile_id]
        breached_groups = set(candidates)
        active_groups.intersection_update(breached_groups)
        emissions = []
        for group, rule in candidates.items():
            if group in active_groups:
                continue
            active_groups.add(group)
            emissions.append(AlertEmission(rule.signal, self._generation(), _MESSAGES[rule.signal]))
        return tuple(sorted(emissions, key=lambda item: item.signal.value))

    def active_signals(self, profile_id: str) -> tuple[AlertSignal, ...]:
        """Return a bounded enum-only health view for tests and diagnostics."""
        return tuple(sorted(
            (signal for signal, active in self._breached.get(profile_id, {}).items() if active),
            key=lambda signal: signal.value,
        ))


__all__ = [
    "ALERT_POLICY", "ALERT_RULES", "AlertEmission", "AlertOwner", "AlertRule", "AlertSignal",
    "MEMORY_GROWTH_THRESHOLD_BYTES", "MEMORY_GROWTH_WINDOW_SECONDS", "MSPT_P95_THRESHOLD_MS",
    "MSPT_SUSTAINED_SECONDS", "PerformanceAlertEvaluator", "WAKE_SLO_THRESHOLD_MS",
]
