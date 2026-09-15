from __future__ import annotations

import io
import math
import sys
from pathlib import Path

import pytest

OPS = Path(__file__).resolve().parents[1] / "ops" / "laptop-backup"
sys.path.insert(0, str(OPS))

import collector  # noqa: E402

# 2026-09-14T05:00:00Z; the sender receipts below are earlier the same day.
NOW = 1789362000.0


def _status(state: str, reason: str, finished: str = "2026-09-14T04:39:04.812744+00:00") -> dict:
    return {
        "schema": 1,
        "last_success": {"finished_at": finished, "archives_verified": 1},
        "attempt": {"state": state, "reason": reason, "finished_at": finished, "started_at": finished},
    }


def test_success_is_fresh_and_not_a_failure() -> None:
    view = collector.classify(_status("success", "verified"), now=NOW)
    assert (view["success"], view["running"], view["deferred"], view["hard_failure"]) == (1.0, 0.0, 0.0, 0.0)
    assert view["reason"] == "verified"
    assert view["last_success_timestamp"] is not None


def test_running_is_never_a_hard_failure() -> None:
    view = collector.classify(_status("running", "in_progress"), now=NOW)
    assert view["running"] == 1.0
    assert view["hard_failure"] == 0.0
    assert view["success"] == 0.0
    rendered = "\n".join(collector.render(view, now=NOW))
    assert "helios_laptop_backup_last_attempt_running 1.000000" in rendered
    assert "helios_laptop_backup_last_attempt_hard_failure 0.000000" in rendered


def test_empty_completion_is_neutral() -> None:
    view = collector.classify(_status("empty", "no_archives_selected"), now=NOW)
    assert (view["success"], view["running"], view["hard_failure"]) == (0.0, 0.0, 0.0)


def test_ac_deferral_is_informational_not_a_hard_failure() -> None:
    for reason in ("non_ac", "low_battery"):
        view = collector.classify(_status("failed", reason), now=NOW)
        assert view["deferred"] == 1.0
        assert view["hard_failure"] == 0.0
        assert view["reason"] == reason


def test_hard_failure_reason_is_not_a_deferral() -> None:
    view = collector.classify(_status("failed", "capacity_reserve"), now=NOW)
    assert view["deferred"] == 0.0
    assert view["hard_failure"] == 1.0


def test_unknown_reason_and_state_fail_closed() -> None:
    view = collector.classify(_status("failed", "totally-made-up reason with spaces"), now=NOW)
    assert view["reason"] == "unknown"
    assert view["hard_failure"] == 1.0
    rendered = "\n".join(collector.render(view, now=NOW))
    assert "totally-made-up" not in rendered
    assert '{code="unknown"}' in rendered
    with pytest.raises(collector.CollectorError):
        collector.classify(_status("mystery", "verified"), now=NOW)
    with pytest.raises(collector.CollectorError):
        collector.classify({"schema": 1, "attempt": {"state": 7, "reason": "verified"}}, now=NOW)


def test_naive_and_future_timestamps_are_refused() -> None:
    naive = _status("success", "verified", "2026-09-14T04:39:04.812744")
    with pytest.raises(collector.CollectorError):
        collector.classify(naive, now=NOW)
    future = _status("success", "verified", "2099-01-01T00:00:00+00:00")
    with pytest.raises(collector.CollectorError):
        collector.classify(future, now=NOW)
    # A large but bounded clock skew is tolerated rather than masking staleness.
    skewed = _status("success", "verified", "2026-09-14T05:01:00+00:00")
    assert collector.classify(skewed, now=NOW)["last_success_timestamp"] is not None


def test_failure_output_omits_fresh_success_and_never_emits_nan() -> None:
    lines = "\n".join(collector.render_failure(now=1234.0))
    assert "helios_laptop_backup_collector_success 0.000000" in lines
    assert "last_success_timestamp_seconds" not in lines
    for bad in (float("nan"), float("inf"), None):
        rendered = "\n".join(collector.render_failure(now=bad))
        assert "nan" not in rendered.lower()
        assert "inf" not in rendered.lower()


def test_invalid_and_oversized_input_are_refused() -> None:
    with pytest.raises(collector.CollectorError):
        collector.load(io.BytesIO(b"not json"))
    with pytest.raises(collector.CollectorError):
        collector.load(io.BytesIO(b"x" * (collector.MAX_INPUT_BYTES + 1)))
    with pytest.raises(collector.CollectorError):
        collector.classify({"schema": 2, "attempt": {"state": "success"}}, now=NOW)


def test_safe_now_is_finite() -> None:
    assert math.isfinite(collector._safe_now(None))
    assert math.isfinite(collector._safe_now(float("nan")))
    assert collector._safe_now(1234.0) == 1234.0
