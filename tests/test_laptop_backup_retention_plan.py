from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

OPS = Path(__file__).resolve().parents[1] / "ops" / "laptop-backup"
sys.path.insert(0, str(OPS))

import retention_plan  # noqa: E402


def _digest(seed: str) -> str:
    return (seed * 64)[:64]


def _entry(backup_id: str, day: int, size: int, seed: str) -> dict:
    return {
        "backup_id": backup_id,
        "profile_id": "p1",
        "created_at": f"2026-01-0{day}T00:00:00+00:00",
        "size_bytes": size,
        "digest": _digest(seed),
    }


def _inventory(scope: str) -> dict:
    return {
        "schema": 1,
        "budget_bytes": 250,
        "reserve_bytes": 100,
        "used_bytes": 260,
        "available_bytes": 80,
        "policy": {"keep_newest_per_profile": 2, "scope": scope},
        # Newest first by validated created_at: c4, c3, c2, c1.
        "catalog": [
            _entry("c4", 4, 40, "a"),
            _entry("c3", 3, 30, "b"),
            _entry("c2", 2, 20, "c"),
            _entry("c1", 1, 10, "d"),
        ],
        "protected_backup_ids": ["c2"],
        "ledger": [{"backup_id": "c1", "state": "purged"}],
        "manifests": [{"digest": _digest("c"), "size_bytes": 20}],
    }


def test_remote_scope_excludes_keep_protection_and_ledger() -> None:
    plan = retention_plan.build_plan(_inventory("remote-vault"))
    assert plan["status"] == "PLAN_ONLY"
    assert plan["executed"] is False
    assert plan["candidate_count"] == 0
    assert plan["excluded"]["c4"] == "within_keep_newest"
    assert plan["excluded"]["c3"] == "within_keep_newest"
    assert plan["excluded"]["c2"] == "protected"
    assert plan["excluded"]["c1"] == "blocking_ledger_state"


def test_shared_object_is_protected_through_any_alias() -> None:
    inventory = _inventory("remote-vault")
    inventory["ledger"] = []
    inventory["manifests"] = []
    # c0 is an unprotected older alias of the protected object d(c2).
    alias = _entry("c0", 1, 20, "c")
    alias["created_at"] = "2025-12-31T00:00:00+00:00"
    inventory["catalog"].append(alias)
    plan = retention_plan.build_plan(inventory)
    assert all(item["digest"] != _digest("c") for item in plan["candidates"])
    assert plan["excluded"]["c0"] == "protected"
    assert plan["excluded"]["c2"] == "protected"


def test_local_payload_requires_a_remote_manifest_copy() -> None:
    inventory = _inventory("local-payload")
    inventory["protected_backup_ids"] = []
    inventory["ledger"] = []
    plan = retention_plan.build_plan(inventory)
    assert plan["excluded"]["c4"] == "within_keep_newest"
    assert plan["excluded"]["c3"] == "within_keep_newest"
    assert plan["excluded"]["c1"] == "no_remote_manifest_copy"
    assert [item["digest"] for item in plan["candidates"]] == [_digest("c")]
    assert plan["candidate_bytes"] == 20


def test_policy_floor_and_positive_constraints_are_required() -> None:
    inventory = _inventory("remote-vault")
    del inventory["policy"]
    with pytest.raises(retention_plan.PlanRefused):
        retention_plan.build_plan(inventory)
    for keep in (0, 1):
        inventory = _inventory("remote-vault")
        inventory["policy"]["keep_newest_per_profile"] = keep
        with pytest.raises(retention_plan.PlanRefused):
            retention_plan.build_plan(inventory)
    inventory = _inventory("remote-vault")
    inventory["budget_bytes"] = 0
    with pytest.raises(retention_plan.PlanRefused):
        retention_plan.build_plan(inventory)
    inventory = _inventory("remote-vault")
    inventory["reserve_bytes"] = 0
    with pytest.raises(retention_plan.PlanRefused):
        retention_plan.build_plan(inventory)


def test_ambiguous_or_malformed_state_is_refused() -> None:
    for mutate in (
        lambda inv: inv.__setitem__("ledger", [{"backup_id": "c1", "state": "mystery"}]),
        lambda inv: inv.__setitem__("ledger", [{"backup_id": "c1", "state": "present"}, {"backup_id": "c1", "state": "purged"}]),
        lambda inv: inv.__setitem__("protected_backup_ids", ["c2", "c2"]),
        lambda inv: inv.__setitem__("protected_backup_ids", ["does-not-exist"]),
        lambda inv: inv["catalog"].append(_entry("c4", 5, 1, "e")),
        lambda inv: inv["catalog"].append({**_entry("c0", 9, 20, "c"), "created_at": "not-a-date"}),
    ):
        inventory = _inventory("remote-vault")
        mutate(inventory)
        with pytest.raises(retention_plan.PlanRefused):
            retention_plan.build_plan(inventory)


def test_catalog_manifest_size_disagreement_is_refused() -> None:
    inventory = _inventory("remote-vault")
    inventory["manifests"] = [{"digest": _digest("c"), "size_bytes": 999}]
    with pytest.raises(retention_plan.PlanRefused):
        retention_plan.build_plan(inventory)


def test_plan_hash_binds_validated_identities() -> None:
    first = retention_plan.build_plan(_inventory("remote-vault"))
    second = retention_plan.build_plan(_inventory("remote-vault"))
    assert first["plan_sha256"] == second["plan_sha256"]
    assert set(first["identities"]) == {
        "catalog_sha256",
        "protections_sha256",
        "ledger_sha256",
        "manifests_sha256",
    }
    changed = _inventory("remote-vault")
    changed["catalog"][0]["created_at"] = "2026-02-04T00:00:00+00:00"
    assert retention_plan.build_plan(changed)["plan_sha256"] != first["plan_sha256"]
    # Capacity/pressure fields are bound too: the approved hash covers the
    # pressure an operator reviewed, not just the candidate set.
    capacity = _inventory("remote-vault")
    capacity["used_bytes"] = 1
    capacity["available_bytes"] = 999999
    changed_plan = retention_plan.build_plan(capacity)
    assert changed_plan["plan_sha256"] != first["plan_sha256"]
    assert changed_plan["pressure"] != first["pressure"]
    assert first["pressure"]["over_budget"] == 10
    assert first["pressure"]["under_reserve"] == 20
    assert len(first["gates"]) == 3
