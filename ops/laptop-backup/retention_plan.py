#!/usr/bin/env python3
"""Plan-only candidate enumeration for the laptop archive vault.

This helper implements no deletion. It enumerates safe eligibility, exclusions
and capacity pressure, and refuses to propose candidates until an explicit
policy input is supplied. Any eventual deletion belongs to a separate reviewed
execution path with its own approval gate.

The vault is content-addressed. A digest is a candidate only when *every*
catalog alias for that digest is outside the retention floor; an unprotected
alias never makes a shared object eligible. Remote objects referenced by any
preserved manifest are never candidates, and local payload retirement requires a
remote manifest copy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from typing import Any

SCHEMA = 1
SCOPES = ("remote-vault", "local-payload")
DIGEST = re.compile(r"^[0-9a-f]{64}$")
# Existing verified-recoverable-copy floor: never propose below this.
MIN_KEEP_NEWEST = 2

# Ledger states that make a payload unavailable for restore or protection. Any
# of these excludes the backup from every scope.
BLOCKING_LEDGER_STATES = frozenset(
    {"prepared", "quarantined", "purge_prepared", "purged", "failed", "ambiguous"}
)
KNOWN_LEDGER_STATES = BLOCKING_LEDGER_STATES | {"present", "rolled_back", "missing"}


class PlanRefused(RuntimeError):
    """The inventory is ambiguous, malformed or missing required policy."""


def _int(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise PlanRefused(f"{name} is invalid")
    return value


def _positive(value: Any, name: str) -> int:
    number = _int(value, name)
    if number <= 0:
        raise PlanRefused(f"{name} must be positive")
    return number


def _digest(value: Any, name: str) -> str:
    if not isinstance(value, str) or DIGEST.match(value) is None:
        raise PlanRefused(f"{name} is not a sha256 digest")
    return value


def _created_at(value: Any) -> float:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise PlanRefused("catalog created_at is invalid")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except (ValueError, OverflowError) as exc:
        raise PlanRefused("catalog created_at is invalid") from exc
    if parsed.tzinfo is None:
        raise PlanRefused("catalog created_at is naive")
    return parsed.astimezone(timezone.utc).timestamp()


def _section_digest(section: Any) -> str:
    return hashlib.sha256(
        json.dumps(section, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def build_plan(inventory: Any) -> dict[str, Any]:
    if not isinstance(inventory, dict):
        raise PlanRefused("inventory is not an object")
    if inventory.get("schema") != SCHEMA:
        raise PlanRefused("unsupported inventory schema")

    budget = _positive(inventory.get("budget_bytes"), "budget_bytes")
    reserve = _positive(inventory.get("reserve_bytes"), "reserve_bytes")
    used = _int(inventory.get("used_bytes"), "used_bytes")
    available = _int(inventory.get("available_bytes"), "available_bytes")

    policy = inventory.get("policy")
    if not isinstance(policy, dict):
        raise PlanRefused("explicit retention policy is required")
    if "keep_newest_per_profile" not in policy:
        raise PlanRefused("explicit keep_newest_per_profile policy is required")
    keep = _int(policy.get("keep_newest_per_profile"), "keep_newest_per_profile")
    if keep < MIN_KEEP_NEWEST:
        raise PlanRefused("keep_newest_per_profile must be >= 2")
    scope = policy.get("scope")
    if scope not in SCOPES:
        raise PlanRefused("policy scope must be remote-vault or local-payload")

    catalog = inventory.get("catalog")
    if not isinstance(catalog, list) or not catalog:
        raise PlanRefused("catalog is empty or invalid")
    entries: dict[str, dict[str, Any]] = {}
    order: dict[str, list[dict[str, Any]]] = {}
    digest_sizes: dict[str, int] = {}
    aliases: dict[str, set[str]] = {}
    for raw in catalog:
        if not isinstance(raw, dict):
            raise PlanRefused("catalog entry is not an object")
        backup_id = raw.get("backup_id")
        profile_id = raw.get("profile_id")
        if not isinstance(backup_id, str) or not backup_id or not isinstance(profile_id, str) or not profile_id:
            raise PlanRefused("catalog entry identity is invalid")
        if backup_id in entries:
            raise PlanRefused("catalog contains a duplicate backup_id")
        size = _int(raw.get("size_bytes"), "size_bytes")
        digest = _digest(raw.get("digest"), "digest")
        if digest in digest_sizes and digest_sizes[digest] != size:
            raise PlanRefused("content-addressed digest has conflicting sizes")
        digest_sizes[digest] = size
        aliases.setdefault(digest, set()).add(backup_id)
        entry = {
            "backup_id": backup_id,
            "profile_id": profile_id,
            "size_bytes": size,
            "digest": digest,
            "created_at": raw.get("created_at"),
            "created_at_epoch": _created_at(raw.get("created_at")),
        }
        entries[backup_id] = entry
        order.setdefault(profile_id, []).append(entry)
    for items in order.values():
        # Newest first by validated timestamp, deterministic tiebreak by id.
        items.sort(key=lambda item: (item["created_at_epoch"], item["backup_id"]), reverse=True)

    protections = inventory.get("protected_backup_ids")
    if not isinstance(protections, list) or any(not isinstance(item, str) for item in protections):
        raise PlanRefused("protected_backup_ids is invalid")
    if len(set(protections)) != len(protections):
        raise PlanRefused("protected_backup_ids contains a duplicate row")
    protected = set(protections)
    if protected - set(entries):
        raise PlanRefused("a protection row references an unknown backup")

    ledger = inventory.get("ledger")
    if not isinstance(ledger, list):
        raise PlanRefused("ledger is invalid")
    ledger_states: dict[str, str] = {}
    blocking: set[str] = set()
    for row in ledger:
        if not isinstance(row, dict):
            raise PlanRefused("ledger row is not an object")
        backup_id = row.get("backup_id")
        state = row.get("state")
        if not isinstance(backup_id, str) or not isinstance(state, str):
            raise PlanRefused("ledger row is malformed")
        if state not in KNOWN_LEDGER_STATES:
            raise PlanRefused("ledger row has an unknown state")
        if backup_id not in entries:
            raise PlanRefused("ledger row references an unknown backup")
        if backup_id in ledger_states and ledger_states[backup_id] != state:
            raise PlanRefused("ledger has conflicting rows for one backup")
        ledger_states[backup_id] = state
        if state in BLOCKING_LEDGER_STATES:
            blocking.add(backup_id)

    manifests = inventory.get("manifests")
    if not isinstance(manifests, list):
        raise PlanRefused("manifests is invalid")
    referenced: dict[str, int] = {}
    for member in manifests:
        if not isinstance(member, dict):
            raise PlanRefused("manifest member is not an object")
        digest = _digest(member.get("digest"), "manifest digest")
        size = _int(member.get("size_bytes"), "manifest size_bytes")
        if digest in referenced and referenced[digest] != size:
            raise PlanRefused("manifest digest has conflicting sizes")
        referenced[digest] = size
    for digest, size in referenced.items():
        if digest in digest_sizes and digest_sizes[digest] != size:
            raise PlanRefused("catalog and manifest disagree on an object size")

    within_keep: set[str] = set()
    for items in order.values():
        within_keep.update(item["backup_id"] for item in items[:keep])

    reason_by_backup: dict[str, str] = {}
    for backup_id, entry in entries.items():
        if backup_id in within_keep:
            reason_by_backup[backup_id] = "within_keep_newest"
        elif backup_id in protected:
            reason_by_backup[backup_id] = "protected"
        elif backup_id in blocking:
            reason_by_backup[backup_id] = "blocking_ledger_state"
        elif scope == "remote-vault" and entry["digest"] in referenced:
            reason_by_backup[backup_id] = "remote_object_referenced_by_manifest"
        elif scope == "local-payload" and entry["digest"] not in referenced:
            reason_by_backup[backup_id] = "no_remote_manifest_copy"

    candidates: list[dict[str, Any]] = []
    excluded: dict[str, str] = dict(reason_by_backup)
    # Any alias of a retained digest inherits that digest's exclusion reason, so
    # a shared object is never reported as selectable through a bare alias.
    for digest, backup_ids in aliases.items():
        inherited = next((reason_by_backup[alias] for alias in backup_ids if alias in reason_by_backup), None)
        if inherited is not None:
            for alias in backup_ids:
                excluded.setdefault(alias, inherited)
    for digest, backup_ids in aliases.items():
        # A digest is eligible only when every alias is eligible; an
        # unprotected alias never makes a shared object selectable.
        if not all(alias not in reason_by_backup for alias in backup_ids):
            continue
        candidates.append(
            {
                "digest": digest,
                "size_bytes": digest_sizes[digest],
                "backup_ids": sorted(backup_ids),
            }
        )
    candidates.sort(key=lambda item: item["digest"])
    freed = sum(item["size_bytes"] for item in candidates)

    catalog_view = [
        [e["backup_id"], e["profile_id"], e["created_at"], e["size_bytes"], e["digest"]]
        for e in sorted(entries.values(), key=lambda item: item["backup_id"])
    ]
    ledger_view = [[backup_id, state] for backup_id, state in sorted(ledger_states.items())]
    manifest_view = [[digest, size] for digest, size in sorted(referenced.items())]
    identities = {
        "catalog_sha256": _section_digest(catalog_view),
        "protections_sha256": _section_digest(sorted(protected)),
        "ledger_sha256": _section_digest(ledger_view),
        "manifests_sha256": _section_digest(manifest_view),
    }
    canonical = json.dumps(
        {
            "policy": {"keep_newest_per_profile": keep, "scope": scope},
            "budget_bytes": budget,
            "reserve_bytes": reserve,
            "used_bytes": used,
            "available_bytes": available,
            "pressure": {
                "over_budget": max(0, used - budget),
                "under_reserve": max(0, reserve - available),
            },
            "identities": identities,
            "candidates": [[item["digest"], item["size_bytes"], item["backup_ids"]] for item in candidates],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "schema": SCHEMA,
        "status": "PLAN_ONLY",
        "executed": False,
        "scope": scope,
        "policy": {"keep_newest_per_profile": keep, "scope": scope},
        "plan_sha256": hashlib.sha256(canonical).hexdigest(),
        "identities": identities,
        "candidate_count": len(candidates),
        "candidate_bytes": freed,
        "candidates": candidates,
        "excluded": excluded,
        "manifest_referenced_objects": manifest_view,
        "pressure": {
            "budget_bytes": budget,
            "reserve_bytes": reserve,
            "used_bytes": used,
            "available_bytes": available,
            "over_budget": max(0, used - budget),
            "under_reserve": max(0, reserve - available),
        },
        "gates": [
            "explicit retention policy input",
            "operator approval of plan_sha256",
            "reviewed execution path (not implemented by this helper)",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="horizon-laptop-backup-retention-plan")
    parser.add_argument("--input", required=True, help="path to a bounded JSON inventory document")
    args = parser.parse_args(argv)
    try:
        with open(args.input, "rb") as handle:
            raw = handle.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            raise PlanRefused("inventory exceeds the bounded limit")
        inventory = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        print(json.dumps({"status": "REFUSED", "reason": "inventory is unreadable"}))
        return 2
    try:
        plan = build_plan(inventory)
    except PlanRefused as exc:
        print(json.dumps({"status": "REFUSED", "reason": str(exc)}))
        return 2
    print(json.dumps(plan, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
