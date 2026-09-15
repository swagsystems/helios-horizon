"""Focused checks for the bounded timestamped stall diagnostics."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from game_control.controller import Controller
from game_control.perf import (
    ACTION_ENUM,
    MAX_EVENT_ROWS,
    PerformanceTracker,
    RPC_ACTION_OTHER,
    TimestampedEventRing,
    classify_rpc_action,
)
from game_control.protocol import (
    MAX_RESPONSE_BYTES,
    GetPerf,
    GetStatus,
    PerfEventAction,
    PerfEventWindow,
    PerfSnapshot,
    RpcRequest,
    Start,
    Stop,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools" / "acceptance"))
import stall_diagnostics  # noqa: E402


def _wire(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _window(sequence: int = 1, action: str = "status", duration: float = 1.0) -> dict:
    return {"sequence": sequence, "action": action, "duration_ms": duration,
            "ended_at": "2026-09-10T01:00:00Z", "monotonic_start": 1.0, "monotonic_end": 2.0}


def test_action_mapping_separates_status_reads_from_lifecycle_actions():
    assert classify_rpc_action(GetStatus(kind="get_status")) == "status"
    assert classify_rpc_action(GetPerf(kind="get_perf")) == "perf"
    assert classify_rpc_action(Start(kind="start", profile_id="minecraft-sunlit-cobblemon")) == "start"
    assert classify_rpc_action(Stop(kind="stop", profile_id="minecraft-sunlit-cobblemon")) == "stop"
    assert classify_rpc_action(object()) == RPC_ACTION_OTHER
    assert classify_rpc_action(type("X", (), {"kind": "profile:user-supplied"})()) == RPC_ACTION_OTHER
    # The wire enum and the tracker allowlist are pinned to each other.
    assert len(set(PerfEventAction.__args__) ^ ACTION_ENUM) == 0


def test_ring_half_open_sequence_semantics_for_empty_single_and_wrapped_windows():
    empty = PerformanceTracker().snapshot()["rpc_events"]
    assert empty["sequence"] == {"start": 1, "end": 1}
    assert empty["items"] == [] and empty["dropped"] == 0

    single = PerformanceTracker()
    single.record_rpc(2.0)
    window = single.snapshot()["rpc_events"]
    assert window["sequence"] == {"start": 1, "end": 2}
    assert [item["sequence"] for item in window["items"]] == [1]

    wrapped = PerformanceTracker(maxlen=4, event_maxlen=3)
    for value in (1.0, 2.0, 3.0, 4.0, 5.0):
        wrapped.record_rpc(value, action=GetStatus(kind="get_status"))
    window = wrapped.snapshot()["rpc_events"]
    assert window["sequence"] == {"start": 3, "end": 6}
    assert [item["sequence"] for item in window["items"]] == [3, 4, 5]
    assert window["dropped"] == 2 and window["capacity"] == 3
    assert len(window["instance"]) == 32 and window["instance"] == wrapped.instance


def test_invalid_durations_clocks_and_actions_are_rejected_without_raising():
    tracker = PerformanceTracker()
    for bad in (float("nan"), float("inf"), float("-inf"), "not-a-number", None, True):
        tracker.record_rpc(bad)
    tracker.record_rpc(5.0, monotonic_start=10.0, monotonic_end=1.0)
    window = tracker.snapshot()["rpc_events"]
    assert window["items"] == [] and window["rejected"] == 7 and window["dropped"] == 0

    for bad_action in (["status"], {"action": "start"}, 5, None):
        tracker.record_rpc(1.0, action=bad_action)
    assert all(item["action"] == RPC_ACTION_OTHER for item in tracker.snapshot()["rpc_events"]["items"])

    # An invalid clock rejects the timestamped record without breaking the caller.
    before = len(tracker.snapshot()["rpc_events"]["items"])
    tracker.record_rpc(1.0, now=datetime(2026, 1, 1))
    tracker.record_rpc(1.0, now="2026-01-01T00:00:00Z")
    window = tracker.snapshot()["rpc_events"]
    assert len(window["items"]) == before and window["rejected"] >= 9
    assert tracker.snapshot()["rpc"]["count"] >= 1  # durability of the aggregate ring

    tracker.record_rpc(-5.0)
    event = tracker.snapshot()["rpc_events"]["items"][-1]
    assert event["duration_ms"] == 0.0 and event["monotonic_end"] >= event["monotonic_start"]


def test_cursor_ingestion_distinguishes_eviction_no_loss_restart_and_duplicates():
    window = {"instance": "a" * 32, "sequence": {"start": 3, "end": 6},
              "capacity": 3, "dropped": 2, "rejected": 0,
              "items": [_window(3), _window(4), _window(5)]}

    baseline = stall_diagnostics.ingest(window, None)
    assert baseline["baseline_capture"] is True and baseline["evicted_since_last_capture"] == 0
    assert baseline["dropped_lifetime"] == 2  # ring lifetime eviction is not a read miss
    assert baseline["next_sequence"] == 6

    no_loss = stall_diagnostics.ingest({"instance": "a" * 32, "sequence": {"start": 6, "end": 8},
                                        "capacity": 3, "dropped": 2, "items": [_window(6), _window(7)]},
                                       {"instance": "a" * 32, "next_sequence": 6})
    assert no_loss["evicted_since_last_capture"] == 0
    assert [item["sequence"] for item in no_loss["consumed"]] == [6, 7]
    assert no_loss["next_sequence"] == 8

    missed = stall_diagnostics.ingest({"instance": "a" * 32, "sequence": {"start": 9, "end": 10},
                                       "capacity": 3, "items": [_window(9)]},
                                      {"instance": "a" * 32, "next_sequence": 6})
    assert missed["evicted_since_last_capture"] == 3 and missed["unobserved_records"] == 3

    replay = stall_diagnostics.ingest({"instance": "a" * 32, "sequence": {"start": 6, "end": 8},
                                       "capacity": 3, "items": [_window(6), _window(7)]},
                                      {"instance": "a" * 32, "next_sequence": 9})
    assert replay["duplicates_skipped"] == 2 and replay["consumed"] == []

    restarted = stall_diagnostics.ingest({"instance": "b" * 32, "sequence": {"start": 1, "end": 2},
                                          "capacity": 3, "items": [_window(1)]},
                                         {"instance": "a" * 32, "next_sequence": 9})
    assert restarted["instance_reset"] is True and restarted["evicted_since_last_capture"] == 0


def test_event_timestamps_are_timezone_aware_and_utc_normalised():
    before = datetime.now(timezone.utc)
    tracker = PerformanceTracker()
    tracker.record_maintenance(12.5)
    tracker.record_event_loop_lag(300.0)
    after = datetime.now(timezone.utc)
    for key in ("maintenance_events", "event_loop_lag_events"):
        event = tracker.snapshot()[key]["items"][-1]
        stamp = datetime.fromisoformat(event["ended_at"].replace("Z", "+00:00"))
        assert stamp.tzinfo is not None and stamp.utcoffset() == timedelta(0)
        assert before <= stamp <= after and event["duration_ms"] >= 0


def test_snapshot_stays_backward_compatible_with_the_frozen_perf_contract():
    tracker = PerformanceTracker(maxlen=2)
    tracker.record_cycle(3.0)
    tracker.record_rpc(4.0)
    tracker.record_maintenance(5.0)
    tracker.record_event_loop_lag(6.0)
    snapshot = tracker.snapshot()
    assert snapshot["cycle"]["count"] == 1 and snapshot["rpc"]["max_ms"] == 4.0
    assert snapshot["maintenance_ms"] == [5.0] and snapshot["event_loop_lag_ms"] == [6.0]
    assert snapshot["maintenance_sequence"] == {"start": 0, "end": 1}
    assert snapshot["event_loop_lag_sequence"] == {"start": 0, "end": 1}
    legacy = {key: snapshot[key] for key in
              ("cycle", "rpc", "maintenance", "maintenance_ms", "maintenance_sequence",
               "event_loop_lag_ms", "event_loop_lag_sequence")}
    assert PerfSnapshot.model_validate(legacy).rpc_events.items == ()
    assert PerfSnapshot.model_validate(snapshot).rpc_events.items[-1].action == "other"


def test_full_window_serialises_within_the_rpc_frame_cap():
    tracker = PerformanceTracker()
    for _ in range(300):
        tracker.record_rpc(1.0, action=GetStatus(kind="get_status"))
        tracker.record_event_loop_lag(2.0)
        tracker.record_maintenance(3.0, monotonic_start=1.0, monotonic_end=2.0)
    snapshot = PerfSnapshot.model_validate(tracker.snapshot())
    assert len(snapshot.rpc_events.items) == 256
    assert len(snapshot.event_loop_lag_events.items) == 256
    assert len(snapshot.maintenance_events.items) == 256
    size = len(_wire(tracker.snapshot()).encode())
    assert size < 256 * 1024 < MAX_RESPONSE_BYTES


@pytest.mark.asyncio
async def test_failed_and_cancelled_actions_are_still_attributed(tmp_path):
    controller = Controller.for_testing(tmp_path)

    async def boom(_request):
        raise RuntimeError("transport failed")

    controller._execute = boom
    request = RpcRequest(request_id=uuid4(), actor="operator",
                         action=Start(kind="start", profile_id="minecraft-sunlit-cobblemon"))
    with pytest.raises(RuntimeError):
        await controller.execute(request)
    events = controller.performance.snapshot()["rpc_events"]["items"]
    assert events[-1]["action"] == "start" and events[-1]["duration_ms"] >= 0
    assert all(event["action"] != "status" for event in events)


def test_reporting_utility_is_bounded_and_drops_unrecognised_fields():
    items = [_window(1, "start", 145827.0), _window(2, "status", 12.5),
             _window(3, "operator-supplied", 5.0), _window(4, "status", float("nan"))]
    items[0].update({"player": "someone", "path": "/var/lib/secret", "error": "boom"})
    payload = {"slotd": {"rpc_events": {
        "items": items, "instance": "c" * 32, "sequence": {"start": 1, "end": 5},
        "capacity": 256, "dropped": 0, "rejected": 1,
    }, "event_loop_lag_events": {"items": [_window(1, "other", 522.988)], "instance": "c" * 32,
                                 "sequence": {"start": 1, "end": 2}, "capacity": 256}}}

    report = stall_diagnostics.summarize(payload)
    rendered = str(report)
    assert report["rpc"]["lifecycle"]["max_ms"] == 145827.0
    assert report["rpc"]["status"]["count"] == 1 and report["rpc"]["other"]["count"] == 1
    assert report["stalled_loop"]["max_ms"] == 522.988
    assert report["instance"] == "c" * 32
    assert "someone" not in rendered and "/var/lib/secret" not in rendered and "boom" not in rendered
    assert set(report["rpc_events"][0]) == {
        "sequence", "action", "duration_ms", "ended_at", "monotonic_start", "monotonic_end"}
    assert report["gaps"]
    for value in report["rpc_events"] + report["event_loop_lag_events"]:
        assert isinstance(value["duration_ms"], float) and value["duration_ms"] >= 0


def test_utility_retains_the_full_bounded_ring_and_caps_rows_and_bytes():
    full = {"slotd": {"rpc_events": {"items": [_window(index) for index in range(1, 257)],
                                     "instance": "d" * 32, "sequence": {"start": 1, "end": 257},
                                     "capacity": 256, "dropped": 0, "rejected": 0}}}
    report = stall_diagnostics.summarize(full)
    assert len(report["rpc_events"]) == 256  # the whole bounded ring survives reporting
    assert all(entry["stream"] for entry in report["loss"])

    oversized = {"slotd": {"rpc_events": {"items": [_window(index) for index in range(1, 3000)],
                                          "sequence": {"start": 1, "end": 3000}}}}
    capped = stall_diagnostics.summarize(oversized)
    assert len(capped["rpc_events"]) == 256
    assert any("truncated" in gap for gap in capped["gaps"])

    with pytest.raises(ValueError, match="bounded input cap"):
        stall_diagnostics.load_payload("x" * (stall_diagnostics.MAX_INPUT_BYTES + 1))


def test_offline_overlap_correlation_crosses_streams_and_is_labeled():
    lag = [_window(1, "other", 300.0)]
    lag[0].update({"monotonic_start": 100.0, "monotonic_end": 400.0})
    maintenance = [_window(1, "maintenance", 50.0), _window(2, "maintenance", 10.0)]
    maintenance[0].update({"monotonic_start": 120.0, "monotonic_end": 170.0})
    maintenance[1].update({"monotonic_start": 1200.0, "monotonic_end": 1210.0})
    correlation = stall_diagnostics.correlate({"lag": lag, "maintenance": maintenance})
    assert correlation["overlaps"] == [{"a": "lag:1", "b": "maintenance:1", "label": "correlation-only"}]
    assert correlation["overlap_label"] == "correlation-only" and correlation["overlap_truncated"] is False
    assert "cause" not in str(correlation)


def test_runtime_cap_matches_the_wire_cap_and_rejects_larger_rings():
    # 256 rows is the accepted maximum; the wire model must agree exactly.
    assert MAX_EVENT_ROWS == 256
    assert PerfEventWindow.model_fields["items"].metadata
    largest = PerformanceTracker(event_maxlen=MAX_EVENT_ROWS)
    for _ in range(MAX_EVENT_ROWS + 50):
        largest.record_rpc(1.0)
    snapshot = PerfSnapshot.model_validate(largest.snapshot())
    assert len(snapshot.rpc_events.items) == MAX_EVENT_ROWS
    for rejected in (MAX_EVENT_ROWS + 1, 4096):
        with pytest.raises(ValueError, match="event ring bound"):
            PerformanceTracker(event_maxlen=rejected)


def test_ring_record_hardens_its_own_reject_path():
    ring = TimestampedEventRing(maxlen=4)
    aware = datetime(2026, 9, 10, tzinfo=timezone.utc)
    for bad_action in ({"action": "start"}, ["start"], 5, None):
        assert ring.record(action=bad_action, duration_ms=1.0, now=aware) is True
    for bad_duration in (True, False, "1.0", None, float("nan")):
        assert ring.record(action="status", duration_ms=bad_duration, now=aware) is False
    assert ring.record(action="status", duration_ms=1.0, now=datetime(2026, 1, 1)) is False
    window = ring.window()
    assert all(item["action"] == RPC_ACTION_OTHER for item in window["items"])
    assert window["rejected"] == 6 and window["sequence"]["end"] == 5
