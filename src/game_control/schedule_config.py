"""Fail-closed, atomic editing of root game-control schedule entries."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .schedule import ScheduleEntry, parse_schedule


class ScheduleConfigError(ValueError):
    pass


def _validate_path(path: Path) -> None:
    if path.is_symlink() or path.parent.is_symlink() or not path.parent.is_dir() or (path.exists() and not path.is_file()):
        raise ScheduleConfigError("schedule config is unavailable")


_TABLE_HEADER = re.compile(r"^\s*\[\[?[^\]]+\]\]?\s*$")


def _without_toml_comment(line: str) -> str:
    quote: str | None = None
    index = 0
    while index < len(line):
        char = line[index]
        if quote is not None:
            if char == "\\" and quote == '"':
                index += 2
                continue
            if line.startswith(quote, index):
                index += len(quote)
                quote = None
                continue
        elif line.startswith('"""', index) or line.startswith("'''", index):
            quote = line[index:index + 3]
            index += 3
            continue
        elif char in {'"', "'"}:
            quote = char
        elif char == "#":
            return line[:index]
        index += 1
    return line


def _advance_multiline(raw: str, quote: str | None) -> str | None:
    """Track TOML multiline strings so header-looking text is not rewritten."""
    index = 0
    while index < len(raw):
        if quote is not None:
            end = raw.find(quote, index)
            if end < 0:
                return quote
            index = end + 3
            quote = None
            continue
        if raw.startswith('"""', index) or raw.startswith("'''", index):
            quote = raw[index:index + 3]
            index += 3
            continue
        if raw[index] == "#":
            break
        if raw[index] == '"':
            index += 2 if index + 1 < len(raw) and raw[index - 1:index + 1] == '\\' else 1
        else:
            index += 1
    return quote


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
    multiline: str | None = None
    for line in lines:
        stripped = _without_toml_comment(line).strip()
        header = bool(_TABLE_HEADER.match(stripped))
        schedule_header = stripped == "[[schedule]]"
        if multiline is None and schedule_header:
            skipping = True
        elif skipping and multiline is None and header:
            skipping = False
        if not skipping:
            output.append(line)
        multiline = _advance_multiline(line, multiline)
    return "".join(output)


def _first_table_marker(raw: str) -> int | None:
    multiline: str | None = None
    for index, line in enumerate(raw.splitlines(keepends=True)):
        stripped = _without_toml_comment(line).strip()
        if multiline is None and _TABLE_HEADER.match(stripped):
            return index
        multiline = _advance_multiline(line, multiline)
    return None


def _validate_entries(entries: Any) -> tuple[list[dict[str, Any]], tuple[ScheduleEntry, ...]]:
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
        parsed = parse_schedule(normalized)
    except ValueError as exc:
        raise ScheduleConfigError(str(exc)) from exc
    return normalized, parsed


def write_schedule_config(path: str | os.PathLike[str], entries: Any) -> None:
    target = Path(path)
    _validate_path(target)
    normalized, expected = _validate_entries(entries)
    original = target.read_text(encoding="utf-8") if target.exists() else ""
    body = _without_schedule_blocks(original)
    marker = _first_table_marker(body)
    if normalized:
        block = _schedule_block(normalized)
        if marker is None:
            body = body.rstrip("\n") + "\n\n" + block + "\n"
        else:
            lines = body.splitlines(keepends=True)
            body = "".join(lines[:marker]) + block + "\n\n" + "".join(lines[marker:])
    try:
        original_parsed = tomllib.loads(original) if original else {}
        parsed = tomllib.loads(body)
        actual = parse_schedule(parsed.get("schedule", []))
        original_without_schedule = {key: value for key, value in original_parsed.items() if key != "schedule"}
        candidate_without_schedule = {key: value for key, value in parsed.items() if key != "schedule"}
        if actual != expected or candidate_without_schedule != original_without_schedule:
            raise ScheduleConfigError("schedule replacement could not be verified")
    except tomllib.TOMLDecodeError as exc:
        raise ScheduleConfigError("schedule replacement produced invalid TOML") from exc
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    if target.exists():
        backup = target.with_name(f"{target.name}.{stamp}.bak")
        shutil.copy2(target, backup)
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent, text=True)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary_path, target.stat().st_mode & 0o777 if target.exists() else 0o640)
        os.replace(temporary_path, target)
        directory_fd = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary_path.unlink(missing_ok=True)


def load_schedule_entries(config: dict[str, Any], override_path: str | os.PathLike[str]) -> Any:
    """Load the writable sidecar, failing closed when it exists but is bad."""
    override = Path(override_path)
    if override.is_symlink():
        raise ScheduleConfigError("schedule override is unavailable")
    if not override.exists():
        return config.get("schedule")
    if not override.is_file() or override.parent.is_symlink():
        raise ScheduleConfigError("schedule override is unavailable")
    try:
        with override.open("rb") as stream:
            payload = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ScheduleConfigError("invalid schedule override") from exc
    if not isinstance(payload, dict) or set(payload) - {"schedule"}:
        raise ScheduleConfigError("invalid schedule override")
    return payload.get("schedule", [])


__all__ = ["ScheduleConfigError", "load_schedule_entries", "write_schedule_config"]
