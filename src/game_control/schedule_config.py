"""Fail-closed, atomic editing of root game-control schedule entries."""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .schedule import parse_schedule


class ScheduleConfigError(ValueError):
    pass


def _validate_path(path: Path) -> None:
    if path.is_symlink() or not path.is_file() or path.parent.is_symlink():
        raise ScheduleConfigError("schedule config is unavailable")


def _schedule_block(entries: Iterable[dict[str, Any]]) -> str:
    blocks = []
    for entry in entries:
        block = (
            "[[schedule]]\n"
            f'cron = "{entry["cron"]}"\n'
            f'profile = "{entry["profile"]}"\n'
        )
        if entry.get("backup_destination") is not None:
            block += f'backup_destination = "{entry["backup_destination"]}"\n'
        if entry.get("operation") not in {None, "backup" if entry.get("backup_destination") is not None else "switch"}:
            block += f'operation = "{entry["operation"]}"\n'
        for key in ("baseline_preset", "candidate_preset", "campaign"):
            if entry.get(key) is not None:
                block += f'{key} = "{entry[key]}"\n'
        if entry.get("maintenance_window"):
            block += "maintenance_window = true\n"
        if entry.get("rollback_safe"):
            block += "rollback_safe = true\n"
        if entry.get("public_wake_policy") not in {None, "disabled"}:
            block += f'public_wake_policy = "{entry["public_wake_policy"]}"\n'
        if not entry.get("enabled", True):
            block += "enabled = false\n"
        blocks.append(block)
    return "\n".join(blocks)


def _without_schedule_blocks(raw: str) -> str:
    lines = raw.splitlines(keepends=True)
    output: list[str] = []
    skipping = False
    for line in lines:
        stripped = line.strip()
        if stripped == "[[schedule]]":
            skipping = True
            continue
        if skipping and stripped.startswith("["):
            skipping = False
        if not skipping:
            output.append(line)
    return "".join(output)


def _validate_entries(entries: Any) -> list[dict[str, Any]]:
    if not isinstance(entries, (list, tuple)):
        raise ScheduleConfigError("schedules must be an array")
    normalized: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ScheduleConfigError("invalid schedule entry")
        if not {"cron", "profile"}.issubset(entry) or not set(entry).issubset({"cron", "profile", "enabled", "backup_destination", "operation", "baseline_preset", "candidate_preset", "campaign", "maintenance_window", "rollback_safe", "public_wake_policy"}):
            raise ScheduleConfigError("unknown schedule field")
        enabled = entry.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ScheduleConfigError("invalid schedule enabled")
        if not isinstance(entry["cron"], str) or any(ord(char) < 0x20 or ord(char) == 0x7F for char in entry["cron"]):
            raise ScheduleConfigError("invalid schedule cron")
        profile = getattr(entry["profile"], "value", entry["profile"])
        if not isinstance(profile, str):
            raise ScheduleConfigError("invalid schedule profile")
        destination = entry.get("backup_destination")
        if destination is not None:
            destination = getattr(destination, "value", destination)
            if destination not in {"local", "horizon-b2"}:
                raise ScheduleConfigError("invalid backup destination")
        normalized.append({
            "cron": entry["cron"],
            "profile": profile,
            "enabled": enabled,
            **({"backup_destination": destination} if destination is not None else {}),
            **{key: entry[key] for key in ("operation", "baseline_preset", "candidate_preset", "campaign", "maintenance_window", "rollback_safe", "public_wake_policy") if key in entry},
        })
    try:
        parse_schedule(normalized)
    except ValueError as exc:
        raise ScheduleConfigError(str(exc)) from exc
    return normalized


def write_schedule_config(path: str | os.PathLike[str], entries: Any) -> None:
    target = Path(path)
    _validate_path(target)
    normalized = _validate_entries(entries)
    original = target.read_text(encoding="utf-8")
    body = _without_schedule_blocks(original)
    marker = next((index for index, line in enumerate(body.splitlines(keepends=True)) if line.lstrip().startswith("[")), None)
    if normalized:
        block = _schedule_block(normalized)
        if marker is None:
            body = body.rstrip("\n") + "\n\n" + block + "\n"
        else:
            lines = body.splitlines(keepends=True)
            body = "".join(lines[:marker]) + block + "\n\n" + "".join(lines[marker:])
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backup = target.with_name(f"{target.name}.{stamp}.bak")
    shutil.copy2(target, backup)
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent, text=True)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary_path, target.stat().st_mode & 0o777)
        os.replace(temporary_path, target)
    finally:
        temporary_path.unlink(missing_ok=True)


__all__ = ["ScheduleConfigError", "write_schedule_config"]
