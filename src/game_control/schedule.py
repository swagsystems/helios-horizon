"""Small, dependency-free cron matching for startup-loaded slot schedules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from .models import BackupDestination, ProfileId


@dataclass(frozen=True)
class _CronField:
    values: frozenset[int]
    minimum: int
    maximum: int

    @classmethod
    def parse(cls, value: str, minimum: int, maximum: int) -> "_CronField":
        if not isinstance(value, str) or not value:
            raise ValueError("invalid cron field")
        result: set[int] = set()
        for part in value.split(","):
            base, _, step_text = part.partition("/")
            step = int(step_text) if step_text else 1
            if step <= 0:
                raise ValueError("invalid cron step")
            if base == "*":
                start, end = minimum, maximum
            elif "-" in base:
                left, right = base.split("-", 1)
                start, end = int(left), int(right)
            else:
                start = end = int(base)
            if start < minimum or end > maximum or start > end:
                raise ValueError("cron field out of range")
            result.update(range(start, end + 1, step))
        return cls(frozenset(result), minimum, maximum)

    def matches(self, value: int) -> bool:
        return value in self.values


@dataclass(frozen=True)
class ScheduleEntry:
    cron: str
    profile: ProfileId
    minute: _CronField
    hour: _CronField
    day: _CronField
    month: _CronField
    weekday: _CronField
    enabled: bool = True
    backup_destination: BackupDestination | None = None
    operation: str = "backup"
    baseline_preset: str | None = None
    candidate_preset: str | None = None
    campaign: str | None = None
    maintenance_window: bool = False
    rollback_safe: bool = False
    public_wake_policy: str = "disabled"

    def matches(self, now: datetime) -> bool:
        return (
            self.minute.matches(now.minute)
            and self.hour.matches(now.hour)
            and self.day.matches(now.day)
            and self.month.matches(now.month)
            and self.weekday.matches((now.weekday() + 1) % 7)
        )

    def next_fire(self, now: datetime) -> datetime | None:
        """Return the next matching minute that has not already elapsed."""
        if not self.enabled:
            return None
        return _find_next_fire(self, now, days=366 * 4)


def _find_next_fire(entry: ScheduleEntry, now: datetime, *, days: int) -> datetime:
    """Find a fire time in a bounded horizon without scanning every minute."""
    candidate = now.replace(second=0, microsecond=0)
    if now.second or now.microsecond:
        candidate += timedelta(minutes=1)
    endpoint = candidate + timedelta(days=days)
    day = candidate.replace(hour=0, minute=0)
    while day <= endpoint:
        if entry.month.matches(day.month) and entry.day.matches(day.day) and entry.weekday.matches((day.weekday() + 1) % 7):
            for hour in sorted(entry.hour.values):
                for minute in sorted(entry.minute.values):
                    possible = day.replace(hour=hour, minute=minute)
                    if candidate <= possible <= endpoint:
                        return possible
        day += timedelta(days=1)
    raise ValueError("schedule has no fire time in the search horizon")


def parse_schedule(raw: Any) -> tuple[ScheduleEntry, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError("schedule must be an array")
    entries = []
    for item in raw:
        if (
            not isinstance(item, dict)
            or not {"cron", "profile"}.issubset(item)
            or not set(item).issubset({"cron", "profile", "enabled", "backup_destination", "operation", "baseline_preset", "candidate_preset", "campaign", "maintenance_window", "rollback_safe", "public_wake_policy"})
        ):
            raise ValueError("invalid schedule entry")
        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("invalid schedule enabled")
        destination = item.get("backup_destination")
        if destination is not None:
            try:
                destination = BackupDestination(destination)
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid backup destination") from exc
        cron = item["cron"]
        operation = item.get("operation", "backup" if destination is not None else "switch")
        if operation not in {"backup", "switch", "benchmark"}:
            raise ValueError("invalid schedule operation")
        if operation == "backup" and destination is None:
            raise ValueError("backup schedule requires a destination")
        baseline = item.get("baseline_preset")
        candidate = item.get("candidate_preset")
        campaign = item.get("campaign")
        maintenance = item.get("maintenance_window", False)
        rollback_safe = item.get("rollback_safe", False)
        wake_policy = item.get("public_wake_policy", "disabled")
        if operation == "benchmark" and (not isinstance(baseline, str) or not isinstance(candidate, str) or baseline == candidate or not isinstance(campaign, str) or maintenance is not True or rollback_safe is not True or wake_policy != "safe"):
            raise ValueError("benchmark schedule requires complete maintenance and rollback policy")
        fields = cron.split() if isinstance(cron, str) else []
        if len(fields) != 5:
            raise ValueError("cron must have five fields")
        try:
            profile = ProfileId(item["profile"])
            parsed = tuple(
                _CronField.parse(value, low, high)
                for value, low, high in zip(fields, (0, 0, 1, 1, 0), (59, 23, 31, 12, 6))
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid schedule entry") from exc
        entry = ScheduleEntry(cron, profile, *parsed, enabled, destination, operation, baseline, candidate, campaign, maintenance, rollback_safe, wake_policy)
        try:
            _find_next_fire(entry, datetime.now(timezone.utc), days=366 * 4)
        except ValueError as exc:
            raise ValueError("schedule has no fire time within the next four years") from exc
        entries.append(entry)
    return tuple(entries)


class ScheduleBook:
    def __init__(self, entries: Iterable[ScheduleEntry] = ()):
        self.entries = tuple(entries)
        self._last_minute: tuple[int, int, int, int, int] | None = None

    def due(self, now: datetime) -> tuple[ScheduleEntry, ...]:
        key = (now.year, now.month, now.day, now.hour, now.minute)
        if key == self._last_minute:
            return ()
        self._last_minute = key
        return tuple(entry for entry in self.entries if entry.enabled and entry.matches(now))


__all__ = ["ScheduleBook", "ScheduleEntry", "parse_schedule"]
