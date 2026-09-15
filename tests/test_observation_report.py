import sqlite3

import pytest

from tools.acceptance.observation_report import gc_report, telemetry_report


@pytest.fixture
def db():
    connection = sqlite3.connect(":memory:")
    connection.executescript("""
        CREATE TABLE telemetry_series(series_id,profile_id,metric,unit,kind,labels_json);
        CREATE TABLE telemetry_samples(series_id,ts_ms,value,state);
        INSERT INTO telemetry_series VALUES('players.lhash','game','players','players','gauge','{"source":"rcon"}');
        INSERT INTO telemetry_series VALUES('players','game','players','players','gauge','{}');
        INSERT INTO telemetry_series VALUES('gc','game','gc_pause','milliseconds','observation','{}');
        INSERT INTO telemetry_series VALUES('noise','other','players','players','gauge','{}');
    """)
    yield connection
    connection.close()


def test_labeled_series_are_resolved_and_not_merged_with_legacy(db):
    db.executemany("INSERT INTO telemetry_samples VALUES(?,?,?,?)", [
        ("players.lhash", 0, 2, "available"), ("players.lhash", 1000, 2, "available"),
        ("players.lhash", 60000, 1, "available"), ("players", 0, None, "unavailable"),
        ("players.lhash", 120000, 99, "available"),  # Exclusive end boundary.
    ])
    report = telemetry_report(db, "game", 0, 120000)
    legacy, labeled = report["metrics"]["players"]["variants"]
    assert labeled["source"] == "rcon"
    assert labeled["values"]["max"] == 2
    assert labeled["cadence_coverage"]["presence_ratio"] == 1
    assert legacy["status"] == "not_observed"
    assert legacy["values"]["max"] is None
    assert legacy["cadence_coverage"]["numeric_ratio"] == 0


def test_duplicates_or_other_series_cannot_fill_missing_buckets(db):
    db.executemany("INSERT INTO telemetry_samples VALUES(?,?,?,?)",
                   [("players.lhash", i, 2, "available") for i in range(100)] +
                   [("noise", 60000 + i, 20, "available") for i in range(100)])
    variant = telemetry_report(db, "game", 0, 120000)["metrics"]["players"]["variants"][1]
    assert variant["cadence_coverage"]["presence_ratio"] == .5


def test_missing_event_samples_and_missing_catalog_are_not_zero(db):
    report = telemetry_report(db, "game", 0, 120000)
    gc = report["metrics"]["gc_pause"]["variants"][0]
    assert gc["status"] == "not_observed"
    assert gc["values"]["max"] is None
    assert gc["cadence_coverage"] is None
    assert report["metrics"]["tps"]["status"] == "missing_series"


def test_inactive_and_unavailable_remain_distinct(db):
    db.executemany("INSERT INTO telemetry_samples VALUES(?,?,?,?)", [
        ("players.lhash", 0, None, "inactive"), ("players.lhash", 60000, None, "unavailable")])
    item = telemetry_report(db, "game", 0, 120000)["metrics"]["players"]["variants"][1]
    assert item["states"] == {"inactive": 1, "unavailable": 1}
    assert item["cadence_coverage"]["presence_ratio"] == 1
    assert item["cadence_coverage"]["numeric_ratio"] == 0


def counter(ts, count, seconds=1):
    return {"ts": ts, "available": True, "metrics": [
        {"name": "jvm_gc_collection_seconds_count", "labels": {"gc": "young"}, "value": count},
        {"name": "jvm_gc_collection_seconds_sum", "labels": {"gc": "young"}, "value": seconds},
    ]}


def test_gc_counter_delta_proves_collections_without_pause_series():
    report = gc_report([counter(0, 10, 1), counter(15, 13, 1.2)], 0, 60)
    assert report["status"] == "collections_observed"
    assert report["groups"]["young"]["collections"] == 3
    assert report["groups"]["young"]["seconds"] == pytest.approx(.2)


def test_gc_missing_data_is_not_no_gc():
    assert gc_report([], 0, 60)["status"] == "unavailable"
    assert gc_report([counter(0, 10)], 0, 60)["status"] == "unavailable"
    assert gc_report([counter(0, 10), counter(15, 10)], 0, 60)["status"] == "no_increase_observed"


def test_gc_gaps_missing_counters_and_resets_are_not_bridged():
    report = gc_report([counter(0, 10), {"ts": 15, "available": False}, counter(30, 100),
                        counter(90, 200), counter(105, 0, 0), counter(120, 1, .1)], 0, 150)
    group = report["groups"]["young"]
    assert group["collections"] == 1
    assert group["intervals"] == 1
    assert group["resets"] == 1
    assert group["observed_seconds"] == 15
    missing = counter(15, 99)
    missing["metrics"].pop()
    assert gc_report([counter(0, 10), missing, counter(30, 200)], 0, 60)["status"] == "unavailable"


def test_gc_rejects_nonfinite_or_duplicate_out_of_order_evidence():
    with pytest.raises(ValueError):
        gc_report([counter(0, float("nan"))], 0, 60)
    with pytest.raises(ValueError):
        gc_report([counter(15, 1), counter(15, 2)], 0, 60)
    duplicate = counter(0, 1)
    duplicate["metrics"].append(duplicate["metrics"][0])
    with pytest.raises(ValueError):
        gc_report([duplicate], 0, 60)


def test_telemetry_window_is_bounded(db):
    for end in [0, 49 * 3600 * 1000]:
        with pytest.raises(ValueError):
            telemetry_report(db, "game", 0, end)
