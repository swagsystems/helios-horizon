#!/usr/bin/env python3
"""Read-only companion summaries; never regrade or rewrite sealed evidence.

Resolve logical telemetry through the series catalog, retaining label variants.
Coverage means presence in time buckets, not proof of every scheduled sample.
Event-only series have no cadence denominator. Exporter GC deltas are lower
bounds over contiguous observations, not an assertion about unobserved time.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics

MAX_WINDOW_MS = 48 * 3600 * 1000
MAX_ROWS = 200_000
MAX_EXPORTER_BYTES = 64 * 1024 * 1024
METRICS = ("players", "tps", "mspt", "cpu_percent", "rss_bytes", "gc_pause")


def stats(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "min": None, "median": None, "p95": None, "max": None}
    return {"count": len(ordered), "min": ordered[0], "median": statistics.median(ordered),
            "p95": ordered[math.floor((len(ordered) - 1) * .95)], "max": ordered[-1]}


def telemetry_report(connection, profile_id, start_ms, end_ms, *, metrics=METRICS, bucket_ms=60_000):
    if not 0 < end_ms - start_ms <= MAX_WINDOW_MS or not 1000 <= bucket_ms <= 3_600_000:
        raise ValueError("invalid bounded telemetry window")
    if not metrics or len(metrics) > 32 or len(set(metrics)) != len(metrics):
        raise ValueError("invalid metric selection")
    result = {}
    for metric in metrics:
        # IDs may acquire a label hash; never construct resource.<profile>.<metric>.
        variants = connection.execute(
            "SELECT series_id,unit,kind,labels_json FROM telemetry_series "
            "WHERE profile_id=? AND metric=? ORDER BY series_id LIMIT 65", (profile_id, metric)
        ).fetchall()
        if len(variants) > 64:
            raise ValueError("too many series variants")
        reports = []
        for sid, unit, kind, labels_json in variants:
            rows = connection.execute(
                "SELECT ts_ms,value,state FROM telemetry_samples "
                "WHERE series_id=? AND ts_ms>=? AND ts_ms<? ORDER BY ts_ms LIMIT ?",
                (sid, start_ms, end_ms, MAX_ROWS + 1),
            ).fetchall()
            if len(rows) > MAX_ROWS:
                raise ValueError("telemetry row limit exceeded")
            values = [value for _, value, state in rows if state == "available"
                      and isinstance(value, (int, float)) and math.isfinite(value)]
            present = {(ts - start_ms) // bucket_ms for ts, _, _ in rows}
            numeric = {(ts - start_ms) // bucket_ms for ts, value, state in rows
                       if state == "available" and isinstance(value, (int, float)) and math.isfinite(value)}
            expected = math.ceil((end_ms - start_ms) / bucket_ms)
            labels = json.loads(labels_json)
            # Only known measurement provenance is included; arbitrary label bodies
            # stay private. The digest still distinguishes every label variant.
            source = labels.get("source", "unlabeled")
            if source not in {"rcon", "process", "controller", "journal", "unlabeled"}:
                source = "other"
            reports.append({
                "series_id": sid, "source": source, "labels_sha256": hashlib.sha256(labels_json.encode()).hexdigest(),
                "unit": unit, "kind": kind, "status": "available" if values else "not_observed",
                "states": dict(Counter(row[2] for row in rows)), "values": stats(values),
                "first_sample_ms": rows[0][0] if rows else None, "last_sample_ms": rows[-1][0] if rows else None,
                "cadence_coverage": None if kind in {"observation", "duration"} else {
                    "method": "distinct window-aligned buckets per exact series; raw samples only",
                    "bucket_ms": bucket_ms, "expected_buckets": expected,
                    "present_buckets": len(present), "numeric_buckets": len(numeric),
                    "presence_ratio": len(present) / expected, "numeric_ratio": len(numeric) / expected,
                },
            })
        result[metric] = {"status": "registered" if variants else "missing_series", "variants": reports}
    return {"schema_version": "observation.telemetry.v1", "start_ms": start_ms, "end_ms": end_ms,
            "scope": "raw samples only; missing or compacted samples are not reconstructed",
            "metrics": result}


def gc_report(records, start_seconds, end_seconds, *, max_gap_seconds=45):
    if not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
               for v in (start_seconds, end_seconds, max_gap_seconds)) or not 0 < end_seconds - start_seconds <= MAX_WINDOW_MS / 1000 or not 0 < max_gap_seconds <= 3600:
        raise ValueError("invalid bounded exporter window")
    previous = {}
    totals = {}
    previous_ts = None
    samples = 0
    unavailable = 0
    for index, record in enumerate(records):
        if index >= MAX_ROWS:
            raise ValueError("exporter row limit exceeded")
        ts = record["ts"]
        if not isinstance(ts, (float, int)) or isinstance(ts, bool) or not math.isfinite(ts):
            raise ValueError("invalid exporter timestamp")
        if previous_ts is not None and ts <= previous_ts:
            raise ValueError("exporter timestamps must increase")
        previous_ts = ts
        if not start_seconds <= ts < end_seconds:
            continue
        samples += 1
        if record.get("available") is not True:
            unavailable += 1
            previous = {}
            continue
        current = {}
        for metric in record.get("metrics", []):
            name = metric["name"]
            if name not in {"jvm_gc_collection_seconds_count", "jvm_gc_collection_seconds_sum"}:
                continue
            group = metric.get("labels", {}).get("gc")
            value = metric["value"]
            if not isinstance(group, str) or not group or len(group) > 128 or not isinstance(value, (float, int)) or isinstance(value, bool) or not math.isfinite(value) or value < 0:
                raise ValueError("invalid GC counter")
            field = "count" if name.endswith("_count") else "seconds"
            if field == "count" and not float(value).is_integer():
                raise ValueError("GC count must be integral")
            if field in current.setdefault(group, {}):
                raise ValueError("duplicate GC counter")
            current[group][field] = value
        next_previous = {}
        for group, counters in current.items():
            if set(counters) != {"count", "seconds"}:
                continue
            if group not in totals and len(totals) >= 64:
                raise ValueError("too many GC groups")
            total = totals.setdefault(group, {"collections": 0, "seconds": 0, "intervals": 0, "observed_seconds": 0, "resets": 0})
            prior = previous.get(group)
            if prior and ts - prior[0] <= max_gap_seconds:
                count = counters["count"] - prior[1]["count"]
                seconds = counters["seconds"] - prior[1]["seconds"]
                if count < 0 or seconds < 0:
                    total["resets"] += 1
                else:
                    total["collections"] += count
                    total["seconds"] += seconds
                    total["intervals"] += 1
                    total["observed_seconds"] += ts - prior[0]
            next_previous[group] = (ts, counters)
        previous = next_previous
    intervals = sum(group["intervals"] for group in totals.values())
    collections = sum(group["collections"] for group in totals.values())
    return {"schema_version": "observation.gc.v1", "samples": samples, "unavailable_samples": unavailable,
            "status": "collections_observed" if collections else "no_increase_observed" if intervals else "unavailable",
            "scope": "lower-bound deltas over contiguous samples; gaps and resets excluded; not a full-window no-GC claim",
            "groups": totals}


def read_exporter(path):
    with Path(path).open("rb") as handle:
        size = 0
        for line in iter(lambda: handle.readline(256 * 1024 + 1), b""):
            size += len(line)
            if size > MAX_EXPORTER_BYTES or len(line) > 256 * 1024:
                raise ValueError("exporter input too large")
            yield json.loads(line)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--start-ms", type=int, required=True)
    parser.add_argument("--end-ms", type=int, required=True)
    parser.add_argument("--exporter", type=Path)
    args = parser.parse_args()
    # mode=ro participates in SQLite WAL visibility; immutable=1 would miss live WAL data.
    connection = sqlite3.connect(args.database.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        report = {"telemetry": telemetry_report(connection, args.profile, args.start_ms, args.end_ms)}
    finally:
        connection.close()
    if args.exporter:
        report["gc"] = gc_report(read_exporter(args.exporter), args.start_ms / 1000, args.end_ms / 1000)
    print(json.dumps(report, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
