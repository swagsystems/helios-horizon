"""Read-only SQL-backed aggregations for Horizon player and TPS stats."""

from __future__ import annotations

import math
import sqlite3
import json
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .tps import TPS_STALE_AFTER_SECONDS


_PROFILE_CAPABILITIES: dict[str, dict[str, Any]] = {
    "minecraft": {"player_tracking": "names", "occupancy": True, "tick_telemetry": True},
    "minecraft-sunlit-cobblemon": {"player_tracking": "names", "occupancy": True, "tick_telemetry": True},
    "terraria-vanilla": {"player_tracking": "names", "occupancy": True, "tick_telemetry": False},
    "terraria-tmod": {"player_tracking": "names", "occupancy": True, "tick_telemetry": False},
    "pz-rising": {"player_tracking": "count", "occupancy": True, "tick_telemetry": False},
}
_RESOLUTION_MS = {"raw": 0, "1m": 60_000, "5m": 300_000, "1h": 3_600_000}
_WINDOW_SECONDS = {"1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800, "30d": 2592000, "1y": 31536000}
_HEATMAP_MAX_SESSIONS = 2_048
_HEATMAP_MAX_HOUR_SEGMENTS = 100_000
_HEATMAP_CACHE: OrderedDict[tuple[Any, ...], tuple[list[list[float]], bool]] = OrderedDict()


def stats_profile_capabilities(profile_id: str) -> dict[str, Any]:
    """Return the closed, UI-facing stat capabilities for one profile."""
    capabilities = _PROFILE_CAPABILITIES.get(str(profile_id))
    if capabilities is None:
        return {"player_tracking": "unavailable", "occupancy": False, "tick_telemetry": False}
    return dict(capabilities)


def stats_summary(
    connection: sqlite3.Connection,
    profile_id: str,
    days: int | None,
    *,
    hours: int | None = None,
    now: str,
) -> dict[str, Any]:
    current = _parse(now)
    cutoff = current - timedelta(hours=hours) if hours is not None else (current - timedelta(days=days) if days is not None else None)
    capabilities = stats_profile_capabilities(profile_id)
    grouped = [] if capabilities["player_tracking"] == "count" else _summary_groups(
        connection, profile_id, cutoff, current
    )
    total_hours = sum(float(row[1]) for row in grouped)
    leaderboard = [
        {
            "player": str(player),
            "hours": round(float(hours), 4),
            "sessions": int(sessions),
            "last_seen": _iso(_from_julian(float(last_seen))),
        }
        for player, hours, sessions, last_seen in grouped[:20]
    ]
    return {
        "total_hours": round(total_hours, 4),
        "unique_players": len(grouped),
        "leaderboard": leaderboard,
        "player_tracking": capabilities["player_tracking"],
        "occupancy": _occupancy(connection, profile_id, cutoff),
    }


def _occupancy(
    connection: sqlite3.Connection, profile_id: str, cutoff: datetime | None
) -> dict[str, Any] | None:
    if not stats_profile_capabilities(profile_id)["occupancy"]:
        return None
    query = "SELECT ts, value FROM metric_samples WHERE profile_id=? AND metric='players'"
    params: list[Any] = [profile_id]
    if cutoff is not None:
        query += " AND julianday(ts) >= julianday(?)"
        params.append(_iso(cutoff))
    rows = connection.execute(
        query + " ORDER BY julianday(ts) DESC LIMIT 500", params
    ).fetchall()
    rows.reverse()
    samples = [{"ts": str(timestamp), "count": int(value)} for timestamp, value in rows]
    return {"latest": samples[-1]["count"] if samples else None, "samples": samples}


def _summary_groups(
    connection: sqlite3.Connection,
    profile_id: str,
    cutoff: datetime | None,
    current: datetime,
) -> list[tuple[Any, ...]]:
    """Aggregate clipped sessions in SQLite, retaining offset-aware ordering."""
    now_iso = _iso(current)
    params: list[Any] = []
    where = (
        "profile_id=? AND julianday(started_at) < julianday(?) "
        "AND (ended_at IS NULL OR julianday(ended_at) > julianday(?) )"
    )
    if cutoff is None:
        # The third predicate is disabled without a lower window bound.
        where = "profile_id=? AND julianday(started_at) < julianday(?)"
    start_sql = "julianday(started_at)"
    if cutoff is not None:
        start_sql = "max(julianday(started_at), julianday(?))"
        params.append(_iso(cutoff))
    params.extend([now_iso, now_iso, now_iso, now_iso, profile_id, now_iso])
    if cutoff is not None:
        params.append(_iso(cutoff))
    query = f"""
        SELECT player,
               sum((ended_jd - started_jd) * 24.0) AS hours,
               count(*) AS sessions,
               max(seen_jd) AS last_seen
        FROM (
            SELECT player,
                   {start_sql} AS started_jd,
                   min(coalesce(julianday(ended_at), julianday(?)), julianday(?)) AS ended_jd,
                   min(coalesce(julianday(ended_at), julianday(?)), julianday(?)) AS seen_jd
            FROM player_sessions
            WHERE {where}
        )
        WHERE ended_jd > started_jd
        GROUP BY player
        ORDER BY hours DESC, player ASC
    """
    return connection.execute(query, params).fetchall()


def stats_heatmap(
    connection: sqlite3.Connection,
    profile_id: str,
    days: int,
    *,
    hours: int | None = None,
    now: str,
) -> dict[str, Any]:
    current = _parse(now)
    # Player activity is a historical navigation aid, not a live counter.  Anchor
    # both the query and cache to the UTC hour so ordinary polling reuses one
    # bounded on-demand materialization instead of expanding every session on
    # every request.  The response names this strategy honestly; slotd does not
    # yet maintain a durable heatmap table.
    materialized_at = current.replace(minute=0, second=0, microsecond=0)
    cutoff = materialized_at - (timedelta(hours=hours) if hours is not None else timedelta(days=days))
    generation = connection.execute(
        "SELECT count(*),coalesce(max(julianday(coalesce(ended_at,started_at))),0) FROM player_sessions WHERE profile_id=?",
        (profile_id,),
    ).fetchone()
    cache_key = (id(connection), connection.total_changes, profile_id, _iso(cutoff), _iso(materialized_at), generation)
    cached = _HEATMAP_CACHE.get(cache_key)
    if cached is not None:
        _HEATMAP_CACHE.move_to_end(cache_key)
        cached_buckets, truncated = cached
        return {"days": days, "buckets": [row[:] for row in cached_buckets],
                "as_of": _iso(materialized_at), "truncated": truncated,
                "cache_strategy": "hour_bucket_bounded_on_demand"}
    buckets = [[0.0 for _ in range(24)] for _ in range(7)]
    rows, truncated = _bounded_heatmap_rows(connection, profile_id, cutoff)
    segments = 0
    for started_at, ended_at in rows:
        interval = _heatmap_interval(started_at, ended_at, cutoff, materialized_at)
        if interval is None:
            continue
        started, ended = interval
        cursor = started.replace(minute=0, second=0, microsecond=0)
        while cursor < ended:
            if segments >= _HEATMAP_MAX_HOUR_SEGMENTS:
                truncated = True
                break
            next_hour = cursor + timedelta(hours=1)
            overlap = max(0.0, (min(next_hour, ended) - max(cursor, started)).total_seconds())
            buckets[cursor.weekday()][cursor.hour] += overlap / 3600.0
            cursor = next_hour
            segments += 1
        if segments >= _HEATMAP_MAX_HOUR_SEGMENTS:
            break
    _HEATMAP_CACHE[cache_key] = ([row[:] for row in buckets], truncated)
    while len(_HEATMAP_CACHE) > 128:
        _HEATMAP_CACHE.popitem(last=False)
    return {"days": days, "buckets": buckets, "as_of": _iso(materialized_at),
            "truncated": truncated, "cache_strategy": "hour_bucket_bounded_on_demand"}


def _bounded_heatmap_rows(
    connection: sqlite3.Connection, profile_id: str, cutoff: datetime
) -> tuple[list[tuple[str, str | None]], bool]:
    """Load a privacy-safe, strictly bounded recent session window."""

    rows = connection.execute(
        "SELECT started_at, ended_at FROM player_sessions "
        "WHERE profile_id=? AND (ended_at IS NULL OR julianday(ended_at) > julianday(?)) "
        "ORDER BY julianday(started_at) DESC, id DESC LIMIT ?",
        (profile_id, _iso(cutoff), _HEATMAP_MAX_SESSIONS + 1),
    ).fetchall()
    truncated = len(rows) > _HEATMAP_MAX_SESSIONS
    selected = rows[:_HEATMAP_MAX_SESSIONS]
    selected.reverse()
    return [(str(started_at), None if ended_at is None else str(ended_at)) for started_at, ended_at in selected], truncated


def _heatmap_interval(
    started_at: str, ended_at: str | None, cutoff: datetime, current: datetime
) -> tuple[datetime, datetime] | None:
    started = max(_parse(started_at), cutoff)
    ended = min(_parse(ended_at) if ended_at is not None else current, current)
    return None if ended <= started else (started, ended)


def stats_tps(
    connection: sqlite3.Connection,
    profile_id: str,
    window: str,
    *,
    now: str,
    telemetry: sqlite3.Connection | None = None,
    resolution: str = "auto",
    limit: int = 500,
) -> dict[str, Any]:
    current = _parse(now)
    if window not in _WINDOW_SECONDS or resolution not in {*_RESOLUTION_MS, "auto"}:
        raise ValueError("invalid telemetry query")
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 2000:
        raise ValueError("invalid telemetry query")
    if telemetry is not None:
        result = _stats_tps_v2(telemetry, profile_id, window, current, resolution, limit)
        result["limit"] = limit
        result["context"] = _timeline_context(
            telemetry, connection, profile_id, current, _WINDOW_SECONDS[window], result["resolution"], limit
        )
        comparison = _previous_available_run(result["samples"])
        comparisons: dict[str, Any] = {} if comparison is None else {"previous": comparison}
        yesterday = _historical_comparison(
            telemetry, profile_id, current, _WINDOW_SECONDS[window], result["resolution"], limit
        )
        if yesterday is not None:
            comparisons["yesterday"] = yesterday
        restart = _restart_comparison(result["samples"], result["context"]["jobs"])
        if restart is not None:
            comparisons["restart"] = restart
        preset = _benchmark_comparison(connection, profile_id)
        if preset is not None:
            comparisons["preset"] = preset
        result["comparisons"] = comparisons
        return result
    cutoff = current - timedelta(seconds=_WINDOW_SECONDS[window])
    rows = connection.execute(
        "SELECT ts, metric, value FROM metric_samples "
        "WHERE profile_id=? AND metric IN ('tps', 'mspt') AND julianday(ts) >= julianday(?) "
        "ORDER BY julianday(ts), ts",
        (profile_id, _iso(cutoff)),
    ).fetchall()
    paired: dict[str, dict[str, float]] = defaultdict(dict)
    for timestamp, metric, value in rows:
        paired[str(timestamp)][str(metric)] = float(value)
    samples = [
        {"ts": timestamp, "tps": values["tps"], "mspt": values["mspt"]}
        for timestamp, values in sorted(paired.items(), key=lambda item: _parse(item[0]))
        if "tps" in values and "mspt" in values
    ]
    latest_observation = _latest_legacy_observation(connection, profile_id, current)
    latest = latest_observation["ts"] if latest_observation is not None else None
    age_seconds = (
        latest_observation["staleness_seconds"] if latest_observation is not None else None
    )
    stale = latest_observation is None or bool(latest_observation["stale"])
    return {
        "window": window,
        "samples": _downsample(samples, limit=limit),
        "latest_ts": latest,
        "latest_observation": latest_observation,
        "stale": stale,
        "state": "unknown" if stale else "ok",
        "staleness_seconds": age_seconds,
        "resolution": "raw",
        "time_basis": {
            "wall_clock_seconds": _WINDOW_SECONDS[window],
            "active_runtime_seconds": None,
            "inactive_seconds": None,
            "unavailable_seconds": None,
        },
    }


def _stats_tps_v2(connection: sqlite3.Connection, profile_id: str, window: str,
                  current: datetime, resolution: str, limit: int) -> dict[str, Any]:
    window_seconds = _WINDOW_SECONDS[window]
    requested = resolution
    chosen = ("raw" if window_seconds <= 48 * 3600 else "1h") if resolution == "auto" else resolution
    # Only hourly rollups survive the raw-retention boundary.  Never label an
    # hourly result as requested 1m/5m granularity.
    if window_seconds > 48 * 3600 and chosen in {"1m", "5m"}:
        chosen = "1h"
    raw_truncated = chosen == "raw" and window_seconds > 48 * 3600
    effective_seconds = min(window_seconds, 48 * 3600) if chosen == "raw" else window_seconds
    cutoff_ms = int((current - timedelta(seconds=effective_seconds)).timestamp() * 1000)
    now_ms = int(current.timestamp() * 1000)
    points: list[dict[str, Any]]
    if chosen == "raw" or (chosen in _RESOLUTION_MS and window_seconds <= 48 * 3600):
        rows = connection.execute(
            """SELECT x.ts_ms,r.metric,x.value,x.state,r.labels_json
               FROM telemetry_samples x JOIN telemetry_series r USING(series_id)
               WHERE r.profile_id=? AND r.metric IN ('tps','mspt') AND x.ts_ms>=? AND x.ts_ms<=?
               ORDER BY x.ts_ms,r.metric,r.labels_json LIMIT 100000""",
            (profile_id, cutoff_ms, now_ms),
        ).fetchall()
        points = _raw_points(rows)
        if chosen != "raw":
            points = _aggregate_resolution(points, _RESOLUTION_MS[chosen])
    else:
        rows = connection.execute(
            """SELECT u.bucket_start_ms,r.metric,u.min,u.max,u.sum,u.count
               FROM telemetry_rollups u JOIN telemetry_series r USING(series_id)
               WHERE r.profile_id=? AND r.metric IN ('tps','mspt')
                 AND u.bucket_start_ms>=? AND u.bucket_start_ms<=?
               ORDER BY u.bucket_start_ms,r.metric LIMIT 10000""",
            (profile_id, cutoff_ms, now_ms),
        ).fetchall()
        state_rows = connection.execute(
            """SELECT u.bucket_start_ms,r.metric,u.available_ms,u.inactive_ms,u.unavailable_ms,
                      u.available_count,u.inactive_count,u.unavailable_count
               FROM telemetry_state_rollups u JOIN telemetry_series r USING(series_id)
               WHERE r.profile_id=? AND r.metric IN ('tps','mspt')
                 AND u.bucket_start_ms>=? AND u.bucket_start_ms<=?
               ORDER BY u.bucket_start_ms,r.metric LIMIT 10000""",
            (profile_id, cutoff_ms, now_ms),
        ).fetchall()
        points = _rollup_points(rows, _RESOLUTION_MS[chosen], state_rows)
        raw_rows = connection.execute(
            """SELECT x.ts_ms,r.metric,x.value,x.state,r.labels_json
               FROM telemetry_samples x JOIN telemetry_series r USING(series_id)
               WHERE r.profile_id=? AND r.metric IN ('tps','mspt')
                 AND x.ts_ms>=? AND x.ts_ms<=?
               ORDER BY x.ts_ms,r.metric,r.labels_json LIMIT 100000""",
            (profile_id, cutoff_ms, now_ms),
        ).fetchall()
        points = _merge_points(points, _aggregate_resolution(_raw_points(raw_rows), _RESOLUTION_MS[chosen], end_ms=now_ms))
        for point in points:
            point.pop("_metric_counts", None)
    time_basis = _time_basis(points, effective_seconds)
    points, boundaries_truncated = _bounded_stateful(points, limit)
    latest_observation = _latest_v2_observation(connection, profile_id, current)
    latest_ms = (
        int(latest_observation["ts_ms"]) if latest_observation is not None else None
    )
    age_seconds = (
        latest_observation["staleness_seconds"] if latest_observation is not None else None
    )
    stale = latest_observation is None or bool(latest_observation["stale"])
    samples = [{key: value for key, value in item.items() if key not in {"ts_ms", "_metric_counts"}} for item in points]
    return {
        "window": window,
        "resolution": chosen,
        "requested_resolution": requested,
        "samples": samples,
        "latest_ts": None if latest_ms is None else _iso(datetime.fromtimestamp(latest_ms / 1000, timezone.utc)),
        "latest_observation": (
            None
            if latest_observation is None
            else {key: value for key, value in latest_observation.items() if key != "ts_ms"}
        ),
        "stale": stale,
        "state": "unknown" if stale else "ok",
        "staleness_seconds": age_seconds,
        "raw_truncated_to_48h": raw_truncated,
        "state_boundaries_truncated": boundaries_truncated,
        "time_basis": time_basis,
    }


def _latest_legacy_observation(
    connection: sqlite3.Connection, profile_id: str, current: datetime
) -> dict[str, Any] | None:
    row = connection.execute(
        """SELECT ts,
                  max(CASE WHEN metric='tps' THEN value END),
                  max(CASE WHEN metric='mspt' THEN value END)
           FROM metric_samples
           WHERE profile_id=? AND metric IN ('tps','mspt')
           GROUP BY ts
           HAVING count(DISTINCT metric)=2
              AND max(CASE WHEN metric='tps' THEN value END) IS NOT NULL
              AND max(CASE WHEN metric='mspt' THEN value END) IS NOT NULL
           ORDER BY julianday(ts) DESC, ts DESC LIMIT 1""",
        (profile_id,),
    ).fetchone()
    if row is None:
        return None
    timestamp, tps, mspt = row
    try:
        age_seconds = max(0.0, (current - _parse(str(timestamp))).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None
    return {
        "ts": str(timestamp),
        "tps": float(tps),
        "mspt": float(mspt),
        "stale": age_seconds > TPS_STALE_AFTER_SECONDS,
        "staleness_seconds": age_seconds,
    }


def _latest_v2_observation(
    connection: sqlite3.Connection, profile_id: str, current: datetime
) -> dict[str, Any] | None:
    rows = connection.execute(
        """WITH latest(ts_ms) AS (
               SELECT x.ts_ms
               FROM telemetry_samples x JOIN telemetry_series r USING(series_id)
               WHERE r.profile_id=? AND r.metric IN ('tps','mspt')
                 AND x.state='available' AND x.value IS NOT NULL
               GROUP BY x.ts_ms
               HAVING count(DISTINCT r.metric)=2
               ORDER BY x.ts_ms DESC LIMIT 1
           )
           SELECT x.ts_ms,r.metric,x.value,x.state,r.labels_json
           FROM telemetry_samples x
           JOIN telemetry_series r USING(series_id)
           JOIN latest ON latest.ts_ms=x.ts_ms
           WHERE r.profile_id=? AND r.metric IN ('tps','mspt')
             AND x.state='available' AND x.value IS NOT NULL
           ORDER BY r.metric,r.labels_json""",
        (profile_id, profile_id),
    ).fetchall()
    points = _raw_points(rows)
    if not points:
        return None
    point = points[-1]
    if point.get("tps") is None or point.get("mspt") is None:
        return None
    ts_ms = int(point["ts_ms"])
    age_seconds = max(0.0, (current.timestamp() * 1000 - ts_ms) / 1000.0)
    return {
        "ts_ms": ts_ms,
        "ts": point["ts"],
        "tps": float(point["tps"]),
        "mspt": float(point["mspt"]),
        "stale": age_seconds > TPS_STALE_AFTER_SECONDS,
        "staleness_seconds": age_seconds,
    }


def _raw_points(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    paired: dict[int, dict[str, Any]] = defaultdict(dict)
    priority = {"prometheus": 3, "rcon": 2, "": 1}
    selected: dict[tuple[int, str], int] = {}
    for ts_ms, metric, value, state, labels_json in rows:
        try:
            source = json.loads(labels_json).get("source", "")
        except Exception:
            source = ""
        key = (int(ts_ms), str(metric))
        rank = priority.get(str(source), 0)
        if rank < selected.get(key, -1):
            continue
        selected[key] = rank
        paired[int(ts_ms)][str(metric)] = (value, str(state))
    output = []
    for ts_ms, values in sorted(paired.items()):
        states = [values[name][1] for name in ("tps", "mspt") if name in values]
        state = "available" if len(states) == 2 and all(item == "available" for item in states) else (
            "inactive" if states and all(item == "inactive" for item in states) else "unavailable"
        )
        item = {"ts_ms": ts_ms, "ts": _iso(datetime.fromtimestamp(ts_ms / 1000, timezone.utc)), "state": state,
                "tps": None, "mspt": None}
        if state == "available":
            item["tps"], item["mspt"] = float(values["tps"][0]), float(values["mspt"][0])
        output.append(item)
    return output


def _aggregate_resolution(points: list[dict[str, Any]], width_ms: int, *, end_ms: int | None = None) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for point in points:
        grouped[(point["ts_ms"] // width_ms) * width_ms].append(point)
    output = []
    ordered = sorted(points, key=lambda item: item["ts_ms"])
    following = {id(point): ordered[index + 1]["ts_ms"] if index + 1 < len(ordered) else end_ms
                 for index, point in enumerate(ordered)}
    for ts_ms, group in sorted(grouped.items()):
        item = _aggregate_group(ts_ms, group)
        durations = {"available": 0.0, "inactive": 0.0, "unavailable": 0.0}
        for point in group:
            stop = following[id(point)]
            if stop is None:
                continue
            stop = min(stop, ts_ms + width_ms)
            if stop > point["ts_ms"]:
                durations[point["state"]] += (stop - point["ts_ms"]) / 1000.0
        if any(durations.values()):
            item["state_duration_seconds"] = durations
            total = sum(durations.values())
            item.update({f"{name}_fraction": durations[name] / total for name in durations})
        output.append(item)
    return output


def _aggregate_group(ts_ms: int, group: list[dict[str, Any]]) -> dict[str, Any]:
    available = [item for item in group if item["state"] == "available"]
    states = {item["state"] for item in group}
    state = "available" if available else ("inactive" if states == {"inactive"} else "unavailable")
    result = {"ts_ms": ts_ms, "ts": _iso(datetime.fromtimestamp(ts_ms / 1000, timezone.utc)),
              "state": state, "tps": None, "mspt": None}
    counts = {name: sum(1 for item in group if item["state"] == name)
              for name in ("available", "inactive", "unavailable")}
    total_states = max(1, sum(counts.values()))
    result.update({f"{name}_fraction": counts[name] / total_states
                   for name in ("available", "inactive", "unavailable")})
    if available:
        result["_metric_counts"] = {}
        for metric in ("tps", "mspt"):
            values = [float(item[metric]) for item in available if item[metric] is not None]
            if values:
                result[metric] = sum(values) / len(values)
                result[f"{metric}_min"], result[f"{metric}_max"] = min(values), max(values)
                result["_metric_counts"][metric] = len(values)
    return result


def _rollup_points(rows: list[tuple[Any, ...]], width_ms: int,
                   state_rows: list[tuple[Any, ...]] | None = None) -> list[dict[str, Any]]:
    grouped: dict[int, dict[str, list[tuple[float, float, float, int]]]] = defaultdict(lambda: defaultdict(list))
    for bucket, metric, minimum, maximum, total, count in rows:
        grouped[(int(bucket) // width_ms) * width_ms][str(metric)].append(
            (float(minimum), float(maximum), float(total), int(count))
        )
    states: dict[int, dict[str, list[tuple[int, int, int, int, int, int]]]] = defaultdict(lambda: defaultdict(list))
    for bucket, metric, available_ms, inactive_ms, unavailable_ms, available_count, inactive_count, unavailable_count in (state_rows or []):
        states[(int(bucket) // width_ms) * width_ms][str(metric)].append(
            (int(available_ms), int(inactive_ms), int(unavailable_ms), int(available_count), int(inactive_count), int(unavailable_count))
        )
    output = []
    for ts_ms in sorted(set(grouped) | set(states)):
        metrics = grouped.get(ts_ms, {})
        item = {"ts_ms": ts_ms, "ts": _iso(datetime.fromtimestamp(ts_ms / 1000, timezone.utc)),
                "state": "unavailable", "tps": None, "mspt": None}
        if all(name in metrics for name in ("tps", "mspt")):
            item["state"] = "available"
            item["_metric_counts"] = {}
            for metric in ("tps", "mspt"):
                values = metrics[metric]
                count = sum(value[3] for value in values)
                item[metric] = sum(value[2] for value in values) / count
                item[f"{metric}_min"] = min(value[0] for value in values)
                item[f"{metric}_max"] = max(value[1] for value in values)
                item["_metric_counts"][metric] = count
        duration = {"available": 0, "inactive": 0, "unavailable": 0}
        for metric_values in states.get(ts_ms, {}).values():
            totals = tuple(sum(value[index] for value in metric_values) for index in range(3))
            duration["available"] = max(duration["available"], totals[0])
            duration["inactive"] = max(duration["inactive"], totals[1])
            duration["unavailable"] = max(duration["unavailable"], totals[2])
        duration_total = sum(duration.values())
        if duration_total:
            item["state_duration_seconds"] = {name: value / 1000.0 for name, value in duration.items()}
            item.update({f"{name}_fraction": value / duration_total for name, value in duration.items()})
            item["state"] = max(duration, key=duration.get)
            if duration["available"] and item["tps"] is not None and item["mspt"] is not None:
                item["state"] = "available"
        output.append(item)
    return output


def _merge_points(*point_sets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge rollup/raw buckets with weighted values on the cutoff bucket."""
    merged: dict[int, dict[str, Any]] = {}
    for points in point_sets:
        for point in points:
            key = int(point["ts_ms"])
            previous = merged.get(key)
            if previous is None:
                merged[key] = point
                continue
            counts = dict(previous.get("_metric_counts", {}))
            incoming_counts = point.get("_metric_counts", {})
            for metric in ("tps", "mspt", "value"):
                left_count = counts.get(metric, 0)
                right_count = incoming_counts.get(metric, 0)
                left = previous.get(metric)
                right = point.get(metric)
                if right is None:
                    continue
                if left is None or left_count == 0:
                    previous[metric] = right
                    counts[metric] = right_count
                elif right_count:
                    previous[metric] = (left * left_count + right * right_count) / (left_count + right_count)
                    counts[metric] = left_count + right_count
                for suffix in ("min", "max"):
                    name = f"{metric}_{suffix}"
                    incoming_name = name if name in point else (suffix if metric == "value" else name)
                    if incoming_name in point:
                        previous[incoming_name] = (min if suffix == "min" else max)(
                            previous.get(incoming_name, right), point[incoming_name]
                        )
            previous["_metric_counts"] = counts
            if "state_duration_seconds" in point:
                durations = previous.setdefault("state_duration_seconds", {})
                for name, value in point["state_duration_seconds"].items():
                    durations[name] = durations.get(name, 0.0) + float(value)
                total = sum(durations.values()) or 1.0
                for name in ("available", "inactive", "unavailable"):
                    previous[f"{name}_fraction"] = durations.get(name, 0.0) / total
            for name in ("available", "inactive", "unavailable"):
                fraction = f"{name}_fraction"
                if fraction in point and fraction not in previous:
                    previous[fraction] = point[fraction]
            if previous.get("state") != "available" and point.get("state") == "available":
                previous["state"] = "available"
    return [merged[key] for key in sorted(merged)]


def _bounded_stateful(points: list[dict[str, Any]], limit: int) -> tuple[list[dict[str, Any]], bool]:
    if len(points) <= limit:
        return points, False
    anchors = {0, len(points) - 1}
    boundaries: set[int] = set()
    for index in range(1, len(points)):
        if points[index]["state"] != points[index - 1]["state"]:
            boundaries.update((index - 1, index))
    for metric in ("tps", "mspt"):
        candidates = [(float(item[metric]), index) for index, item in enumerate(points) if item.get(metric) is not None]
        if candidates:
            anchors.update((min(candidates)[1], max(candidates)[1]))
    mandatory = anchors | boundaries
    truncated = len(mandatory) > limit
    chosen = sorted(anchors)
    if len(chosen) > limit:
        priority = [len(points) - 1, 0]
        priority.extend(index for index in sorted(anchors) if index not in priority)
        chosen = sorted(priority[:limit])
    elif len(chosen) < limit and boundaries:
        candidates = sorted(boundaries - set(chosen))
        slots = min(limit - len(chosen), len(candidates))
        if slots:
            selected = {candidates[round(index * (len(candidates) - 1) / max(1, slots - 1))]
                        for index in range(slots)}
            chosen.extend(sorted(selected))
    if len(chosen) < limit:
        remaining = [index for index in range(len(points)) if index not in set(chosen)]
        step = max(1, math.ceil(len(remaining) / (limit - len(chosen))))
        chosen.extend(remaining[::step][: limit - len(chosen)])
    return [points[index] for index in sorted(chosen)], truncated


def _time_basis(points: list[dict[str, Any]], wall_seconds: int) -> dict[str, Any]:
    totals = {"available": 0.0, "inactive": 0.0, "unavailable": 0.0}
    duration_points = [point for point in points if isinstance(point.get("state_duration_seconds"), dict)]
    if duration_points:
        for point in duration_points:
            for name in totals:
                value = point["state_duration_seconds"].get(name, 0)
                if isinstance(value, (int, float)) and math.isfinite(value) and value >= 0:
                    totals[name] += float(value)
    else:
        remaining = float(wall_seconds)
        for current, following in zip(points, points[1:]):
            duration = min(remaining, max(0.0, (following["ts_ms"] - current["ts_ms"]) / 1000.0))
            totals[current["state"]] += duration
            remaining -= duration
            if remaining <= 0:
                break
    observed = sum(totals.values())
    return {"wall_clock_seconds": wall_seconds, "active_runtime_seconds": totals["available"],
            "inactive_seconds": totals["inactive"], "unavailable_seconds": totals["unavailable"],
            "observed_seconds": observed, "unknown_wall_clock_seconds": max(0.0, wall_seconds - observed)}


_CONTEXT_METRICS = ("cpu_percent", "rss_bytes", "gc_pause")
_JOB_KIND = {
    "start": "start", "stop": "stop", "restart": "restart", "switch": "switch",
    "scheduled_backup": "backup", "backup": "backup", "update": "update", "benchmark": "benchmark",
}
_JOB_STATE = {
    "accepted": "running", "running": "running", "succeeded": "succeeded",
    "completed": "succeeded", "failed": "failed",
}
_BENCHMARK_METRICS = {
    "tick.p95Nanos": ("mspt_p95", 1e-6, "ms"),
    "tick.p99Nanos": ("mspt_p99", 1e-6, "ms"),
    "gc.pause.p95Nanos": ("gc_pause_p95", 1e-6, "ms"),
    "rss.peakBytes": ("rss_peak", 1.0, "bytes"),
}


def _timeline_context(telemetry: sqlite3.Connection, state: sqlite3.Connection, profile_id: str,
                      current: datetime, window_seconds: int, resolution: str, limit: int) -> dict[str, Any]:
    """Return bounded, identity-free causes on the same wall-clock as tick data."""
    effective_seconds = min(window_seconds, 48 * 3600) if resolution == "raw" else window_seconds
    cutoff_ms = int((current - timedelta(seconds=effective_seconds)).timestamp() * 1000)
    now_ms = int(current.timestamp() * 1000)
    width_ms = _RESOLUTION_MS.get(resolution, 0)
    series: dict[str, list[dict[str, Any]]] = {}
    for metric in _CONTEXT_METRICS:
        if resolution == "raw" or window_seconds <= 48 * 3600:
            rows = telemetry.execute(
                """SELECT x.ts_ms,x.value,x.state FROM telemetry_samples x
                   JOIN telemetry_series r USING(series_id)
                   WHERE r.profile_id=? AND r.metric=? AND x.ts_ms>=? AND x.ts_ms<=?
                   ORDER BY x.ts_ms LIMIT 100000""",
                (profile_id, metric, cutoff_ms, now_ms),
            ).fetchall()
            points = [{"ts_ms": int(ts), "ts": _iso(datetime.fromtimestamp(int(ts) / 1000, timezone.utc)),
                       "value": None if value is None else float(value), "state": str(sample_state)}
                      for ts, value, sample_state in rows]
            if width_ms:
                points = _aggregate_scalar(points, width_ms, end_ms=now_ms)
        else:
            rows = telemetry.execute(
                """SELECT u.bucket_start_ms,u.min,u.max,u.sum,u.count FROM telemetry_rollups u
                   JOIN telemetry_series r USING(series_id)
                 WHERE r.profile_id=? AND r.metric=? AND u.bucket_start_ms>=? AND u.bucket_start_ms<=?
                   ORDER BY u.bucket_start_ms LIMIT 10000""",
                (profile_id, metric, cutoff_ms, now_ms),
            ).fetchall()
            points = [{"ts_ms": int(ts), "ts": _iso(datetime.fromtimestamp(int(ts) / 1000, timezone.utc)),
                       "value": float(total) / int(count), "min": float(minimum), "max": float(maximum),
                       "state": "available", "_metric_counts": {"value": int(count)}}
                      for ts, minimum, maximum, total, count in rows if int(count) > 0]
            raw_rows = telemetry.execute(
                """SELECT x.ts_ms,x.value,x.state FROM telemetry_samples x
                   JOIN telemetry_series r USING(series_id)
                   WHERE r.profile_id=? AND r.metric=? AND x.ts_ms>=? AND x.ts_ms<=?
                   ORDER BY x.ts_ms LIMIT 100000""",
                (profile_id, metric, cutoff_ms, now_ms),
            ).fetchall()
            raw_points = [{"ts_ms": int(ts), "ts": _iso(datetime.fromtimestamp(int(ts) / 1000, timezone.utc)),
                           "value": None if value is None else float(value), "state": str(sample_state)}
                          for ts, value, sample_state in raw_rows]
            points = _merge_points(points, _aggregate_scalar(raw_points, _RESOLUTION_MS[resolution], end_ms=now_ms))
        bounded, _ = _bounded_scalar(points, min(limit, 720))
        series[metric] = [{key: value for key, value in point.items() if key not in {"ts_ms", "_metric_counts"}} for point in bounded]
    jobs: list[dict[str, Any]] = []
    try:
        rows = state.execute(
            """SELECT operation,state,created_at,finished_at FROM jobs
               WHERE profile_id=? AND julianday(created_at)<=julianday(?)
                 AND (finished_at IS NULL OR julianday(finished_at)>=julianday(?))
               ORDER BY julianday(created_at),id LIMIT 200""",
            (profile_id, _iso(current), _iso(current - timedelta(seconds=window_seconds))),
        ).fetchall()
    except sqlite3.Error:
        rows = []
    for operation, job_state, started, ended in rows:
        kind = _JOB_KIND.get(str(operation))
        safe_state = _JOB_STATE.get(str(job_state))
        if kind is not None and safe_state is not None:
            jobs.append({"kind": kind, "state": safe_state, "started_at": str(started),
                         "ended_at": None if ended is None else str(ended)})
    # Manual backups are not controller jobs, so project their verified record
    # as a zero-duration marker without exposing the backup ID or destination.
    try:
        backup_rows = state.execute(
            "SELECT created_at,verified FROM backups WHERE profile_id=? "
            "AND julianday(created_at)>=julianday(?) AND julianday(created_at)<=julianday(?) "
            "ORDER BY julianday(created_at) LIMIT 200",
            (profile_id, _iso(current - timedelta(seconds=window_seconds)), _iso(current)),
        ).fetchall()
    except sqlite3.Error:
        backup_rows = []
    for created_at, verified in backup_rows:
        if any(item["kind"] == "backup" and abs((_parse(item["started_at"]) - _parse(str(created_at))).total_seconds()) <= 60
               for item in jobs):
            continue
        jobs.append({"kind": "backup", "state": "succeeded" if int(verified) == 1 else "failed",
                     "started_at": str(created_at), "ended_at": str(created_at)})
    jobs.sort(key=lambda item: _parse(item["started_at"]))
    return {"series": series, "jobs": jobs}


def _historical_comparison(
    telemetry: sqlite3.Connection, profile_id: str, current: datetime,
    window_seconds: int, resolution: str, limit: int,
) -> dict[str, Any] | None:
    """Return the same bounded wall-clock window one day earlier."""

    if window_seconds > 24 * 3600:
        return None
    end = current - timedelta(days=1)
    start = end - timedelta(seconds=window_seconds)
    start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    rows = telemetry.execute(
        """SELECT x.ts_ms,r.metric,x.value,x.state,r.labels_json
           FROM telemetry_samples x JOIN telemetry_series r USING(series_id)
           WHERE r.profile_id=? AND r.metric IN ('tps','mspt') AND x.ts_ms>=? AND x.ts_ms<=?
           ORDER BY x.ts_ms,r.metric,r.labels_json LIMIT 100000""",
        (profile_id, start_ms, end_ms),
    ).fetchall()
    points = _raw_points(rows)
    width_ms = _RESOLUTION_MS.get(resolution, 0)
    if width_ms:
        points = _aggregate_resolution(points, width_ms)
    points, truncated = _bounded_stateful(points, min(limit, 720)) if points else ([], False)
    samples = [{key: value for key, value in point.items() if key != "ts_ms"} for point in points]
    if not any(item.get("state") == "available" for item in samples):
        return None
    return {"label": "Yesterday at the same time", "samples": samples, "truncated": truncated}


def _restart_comparison(samples: list[dict[str, Any]], jobs: list[dict[str, Any]]) -> dict[str, Any] | None:
    restarts = [item for item in jobs if item.get("kind") == "restart"]
    if not restarts:
        return None
    split_at = restarts[-1]["started_at"]
    try:
        split = _parse(split_at)
    except (TypeError, ValueError):
        return None
    before = [dict(item) for item in samples if item.get("state") == "available" and _parse(item["ts"]) < split]
    after = [item for item in samples if item.get("state") == "available" and _parse(item["ts"]) >= split]
    if not before or not after:
        return None
    return {"label": "Before the latest restart", "samples": before[-720:], "split_at": split_at,
            "truncated": len(before) > 720}


def _benchmark_comparison(state: sqlite3.Connection, profile_id: str) -> dict[str, Any] | None:
    try:
        row = state.execute(
            "SELECT baseline_preset,candidate_preset,overall_verdict,summary_json FROM benchmark_runs "
            "WHERE profile_id=? AND state='succeeded' ORDER BY julianday(finished_at) DESC,id DESC LIMIT 1",
            (profile_id,),
        ).fetchone()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    baseline, candidate, verdict, raw = row
    if not isinstance(baseline, str) or not isinstance(candidate, str) or len(baseline) > 32 or len(candidate) > 32:
        return None
    if verdict not in {"better", "worse", "mixed", "inconclusive"}:
        return None
    try:
        payload = json.loads(raw or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    metrics: dict[str, dict[str, Any]] = {}
    for item in payload.get("metrics", ()) if isinstance(payload, dict) else ():
        if not isinstance(item, dict) or item.get("name") not in _BENCHMARK_METRICS:
            continue
        output_name, scale, unit = _BENCHMARK_METRICS[item["name"]]
        raw_baseline = item.get("baselineMedian", item.get("baseline_median"))
        raw_candidate = item.get("candidateMedian", item.get("candidate_median"))
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
               for value in (raw_baseline, raw_candidate)):
            continue
        values = (float(raw_baseline) * scale, float(raw_candidate) * scale)
        if min(values) < 0 or max(values) > 1e15:
            continue
        metrics[output_name] = {"baseline": values[0], "candidate": values[1], "unit": unit}
    if not metrics:
        return None
    return {"label": "Benchmark presets", "baseline_preset": baseline,
            "candidate_preset": candidate, "verdict": verdict, "metrics": metrics}


def _aggregate_scalar(points: list[dict[str, Any]], width_ms: int, *, end_ms: int | None = None) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for point in points:
        grouped[(point["ts_ms"] // width_ms) * width_ms].append(point)
    output = []
    ordered = sorted(points, key=lambda item: item["ts_ms"])
    following = {id(point): ordered[index + 1]["ts_ms"] if index + 1 < len(ordered) else end_ms
                 for index, point in enumerate(ordered)}
    for ts_ms, group in sorted(grouped.items()):
        available = [point for point in group if point["state"] == "available" and point["value"] is not None]
        states = {point["state"] for point in group}
        state = "available" if available else ("inactive" if states == {"inactive"} else "unavailable")
        item = {"ts_ms": ts_ms, "ts": _iso(datetime.fromtimestamp(ts_ms / 1000, timezone.utc)),
                "value": None, "state": state}
        if available:
            values = [float(point["value"]) for point in available]
            item.update(value=sum(values) / len(values), min=min(values), max=max(values),
                        _metric_counts={"value": len(values)})
        durations = {"available": 0.0, "inactive": 0.0, "unavailable": 0.0}
        for point in group:
            stop = following[id(point)]
            if stop is None:
                continue
            stop = min(stop, ts_ms + width_ms)
            if stop > point["ts_ms"]:
                durations[point["state"]] += (stop - point["ts_ms"]) / 1000.0
        if any(durations.values()):
            item["state_duration_seconds"] = durations
            total = sum(durations.values())
            item.update({f"{name}_fraction": durations[name] / total for name in durations})
        output.append(item)
    return output


def _bounded_scalar(points: list[dict[str, Any]], limit: int) -> tuple[list[dict[str, Any]], bool]:
    if len(points) <= limit:
        return points, False
    mandatory = {0, len(points) - 1}
    for index in range(1, len(points)):
        if points[index]["state"] != points[index - 1]["state"]:
            mandatory.update((index - 1, index))
    values = [(float(point["value"]), index) for index, point in enumerate(points) if point.get("value") is not None]
    if values:
        mandatory.update((min(values)[1], max(values)[1]))
    chosen = sorted(mandatory)[:limit]
    if len(chosen) < limit:
        remaining = [index for index in range(len(points)) if index not in mandatory]
        step = max(1, math.ceil(len(remaining) / (limit - len(chosen))))
        chosen.extend(remaining[::step][: limit - len(chosen)])
    return [points[index] for index in sorted(chosen)], len(mandatory) > limit


def _previous_available_run(samples: list[dict[str, Any]]) -> dict[str, Any] | None:
    runs: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for sample in samples:
        if sample.get("state") == "available":
            current.append(dict(sample))
        elif current:
            runs.append(current); current = []
    if current:
        runs.append(current)
    if len(runs) < 2:
        return None
    return {"label": "Previous active run", "samples": runs[-2]}


def _session_rows(
    connection: sqlite3.Connection, profile_id: str, cutoff: datetime | None
) -> list[tuple[Any, ...]]:
    if cutoff is None:
        return connection.execute(
            "SELECT id, profile_id, player, started_at, ended_at FROM player_sessions "
            "WHERE profile_id=? ORDER BY started_at, id",
            (profile_id,),
        ).fetchall()
    return connection.execute(
        "SELECT id, profile_id, player, started_at, ended_at FROM player_sessions "
            "WHERE profile_id=? AND (ended_at IS NULL OR julianday(ended_at) > julianday(?)) "
            "ORDER BY julianday(started_at), id",
        (profile_id, _iso(cutoff)),
    ).fetchall()


def _interval(
    row: tuple[Any, ...], cutoff: datetime | None, current: datetime
) -> tuple[datetime, datetime] | None:
    started = _parse(row[3])
    ended = min(_parse(row[4]) if row[4] is not None else current, current)
    if cutoff is not None:
        started = max(started, cutoff)
    if ended <= started:
        return None
    return started, ended


def _downsample(samples: list[dict[str, Any]], *, limit: int = 500) -> list[dict[str, Any]]:
    if len(samples) <= limit:
        return samples
    chunk_size = math.ceil(len(samples) / limit)
    output: list[dict[str, Any]] = []
    for start in range(0, len(samples), chunk_size):
        chunk = samples[start : start + chunk_size]
        output.append(
            {
                "ts": chunk[0]["ts"],
                "tps": sum(item["tps"] for item in chunk) / len(chunk),
                "mspt": sum(item["mspt"] for item in chunk) / len(chunk),
            }
        )
    return output


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _from_julian(value: float) -> datetime:
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
        seconds=round((value - 2440587.5) * 86400.0, 3)
    )


__all__ = ["stats_heatmap", "stats_profile_capabilities", "stats_summary", "stats_tps"]
