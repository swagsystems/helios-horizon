from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest

from game_control import state_db
from tools.migrations import state_migrate as migration


UTILITY_PATH = Path(__file__).parents[2] / "tools/migrations/state_migrate.py"


TS = "2026-08-05T12:00:00Z"


def make_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    state_db._configure(connection)
    state_db._migrate_state(connection)
    connection.commit()
    connection.close()
    path.chmod(0o600)


def make_legacy_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    state_db._configure(connection)
    for table in (
        "events",
        "audit",
        "jobs",
        "confirmations",
        "backups",
        "notification_rules",
        "notification_deliveries",
        "updates",
        "rpc_idempotency",
        "player_sessions",
        "metric_samples",
    ):
        connection.execute(migration.LEGACY_TABLE_SQL[table])
    for index_name in sorted(migration.LEGACY_SOURCE_INDEXES):
        connection.execute(migration.EXPECTED_INDEX_SQL[index_name])
    for trigger_name in sorted(migration.EXPECTED_TRIGGERS):
        connection.execute(migration.EXPECTED_TRIGGER_SQL[trigger_name])
    connection.execute("PRAGMA user_version = 2")
    connection.commit()
    connection.close()
    path.chmod(0o600)


@pytest.fixture(autouse=True)
def no_test_process_is_a_writer(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(migration, "_writer_processes", lambda: [])


def _seed_ledger(connection: sqlite3.Connection, rows) -> None:
    connection.executemany(
        "INSERT INTO backup_payload_retirement (operation_id, backup_id, profile_id, state,"
        " path, quarantine_path, expected_device, expected_inode, expected_size,"
        " expected_mtime_ns, expected_ctime_ns, expected_sha256, manifest_sha256,"
        " created_at, updated_at, error_code) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )


def test_terminal_retirement_ledger_is_preserved_row_for_row(tmp_path: Path):
    """A nonempty terminal ledger must survive migration byte-for-byte."""
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    connection = sqlite3.connect(source)
    state_db._configure(connection)
    connection.execute("INSERT INTO backups VALUES (?,?,?,?,?,?)", ("b1", "terraria-tmod", TS, 10, 1, 1))
    rows = [
        ("op-purged", "b1", "terraria-tmod", "purged",
         "/var/backups/game-servers/terraria-tmod/b1.tar.zst", None,
         2049, 4242, 10, 1, 2, "c" * 64, "d" * 64, TS, TS, None),
        ("op-rolled", "b1", "terraria-tmod", "rolled_back",
         "/var/backups/game-servers/terraria-tmod/b1.tar.zst",
         "/var/lib/game-control/quarantine/b1", 2049, 4242, 10, 1, 2,
         "c" * 64, "d" * 64, TS, TS, "rollback-verified"),
    ]
    _seed_ledger(connection, rows)
    # A real derived projection table must not block the migration either.
    from game_control.startup_estimates import STARTUP_ESTIMATE_DDL

    for statement in STARTUP_ESTIMATE_DDL:
        connection.execute(statement)
    connection.execute(
        "INSERT INTO startup_estimate_samples (profile_id, version, duration_ms, finished_at, run_key)"
        " VALUES (?,?,?,?,?)",
        ("terraria-tmod", "1.0", 1234, TS, "run-1"),
    )
    connection.commit()
    connection.close()

    result = run(source, target, report, retired)
    assert result["quick_check"] == {"source": "ok", "target": "ok"}
    target_db = sqlite3.connect(target)
    preserved = target_db.execute(
        "SELECT * FROM backup_payload_retirement ORDER BY operation_id"
    ).fetchall()
    assert preserved == sorted(rows, key=lambda row: row[0])
    assert target_db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='backup_payload_retirement'"
    ).fetchone()[0] == migration.EXPECTED_TABLE_SQL[migration.LEDGER_TABLE]
    assert target_db.execute("SELECT COUNT(*) FROM backup_payload_retirement").fetchone()[0] == 2
    assert target_db.execute("PRAGMA foreign_key_check").fetchall() == []
    # Derived startup samples are disposable and intentionally not carried.
    tables = {row[0] for row in target_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "startup_estimate_samples" not in tables
    target_db.close()


def test_unknown_table_still_fails_closed(tmp_path: Path):
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    connection = sqlite3.connect(source)
    connection.execute("CREATE TABLE mystery_ledger (id TEXT PRIMARY KEY)")
    connection.commit()
    connection.close()
    with pytest.raises(migration.MigrationError, match="unsupported SQLite table set"):
        run(source, target, report, retired)
    assert not target.exists()


def test_empty_retirement_ledger_is_preserved_not_dropped(tmp_path: Path):
    """An empty safety ledger must still exist in the target with its identity."""
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    connection = sqlite3.connect(source)
    connection.execute("SELECT COUNT(*) FROM backup_payload_retirement")
    connection.commit()
    connection.close()
    run(source, target, report, retired)
    target_db = sqlite3.connect(target)
    assert target_db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='backup_payload_retirement'"
    ).fetchone()[0] == migration.EXPECTED_TABLE_SQL[migration.LEDGER_TABLE]
    assert target_db.execute("SELECT COUNT(*) FROM backup_payload_retirement").fetchone()[0] == 0
    assert target_db.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index'"
        " AND name='idx_backup_payload_retirement_backup'"
    ).fetchone()[0] == 1
    target_db.close()


def _make_scratch_schema(path: Path, table_sql: dict, index_names, version: int | None = None) -> None:
    connection = sqlite3.connect(path)
    connection.create_function(
        "is_rfc3339_timestamp", 1, migration._is_rfc3339_timestamp, deterministic=True
    )
    connection.execute(f"PRAGMA application_id = {migration.APP_ID}")
    for _name, sql in table_sql.items():
        connection.execute(sql)
    for index_name in sorted(index_names):
        connection.execute(migration.EXPECTED_INDEX_SQL[index_name])
    for trigger_name in sorted(migration.EXPECTED_TRIGGERS):
        connection.execute(migration.EXPECTED_TRIGGER_SQL[trigger_name])
    connection.execute(f"PRAGMA user_version = {migration.SCHEMA_VERSION if version is None else version}")
    connection.commit()
    connection.close()
    path.chmod(0o600)


def _assert_ledger_gained(target: Path) -> None:
    target_db = sqlite3.connect(target)
    assert target_db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='backup_payload_retirement'"
    ).fetchone()[0] == migration.EXPECTED_TABLE_SQL[migration.LEDGER_TABLE]
    assert target_db.execute("SELECT COUNT(*) FROM backup_payload_retirement").fetchone()[0] == 0
    assert target_db.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index'"
        " AND name='idx_backup_payload_retirement_backup'"
    ).fetchone()[0] == 1
    assert target_db.execute("PRAGMA foreign_key_check").fetchall() == []
    target_db.close()


def test_clean_pre_ledger_schema4_upgrades_and_gains_empty_ledger(tmp_path: Path):
    """The real fc726e9 schema-4 shape: benchmark table, no ledger/derived tables."""
    source, target, report, retired = paths(tmp_path)
    tables = {k: v for k, v in migration.EXPECTED_TABLE_SQL.items()
              if k != migration.LEDGER_TABLE}
    indexes = [name for name in migration.EXPECTED_INDEXES
               if name != "idx_backup_payload_retirement_backup"]
    _make_scratch_schema(source, tables, indexes)
    result = run(source, target, report, retired)
    assert result["quick_check"] == {"source": "ok", "target": "ok"}
    _assert_ledger_gained(target)


def test_schema3_legacy_benchmark_without_ledger_upgrades(tmp_path: Path):
    """Pre-benchmark schema-3 shape (older benchmark_runs), still no ledger."""
    source, target, report, retired = paths(tmp_path)
    tables = {k: v for k, v in migration.EXPECTED_TABLE_SQL.items()
              if k != migration.LEDGER_TABLE}
    tables["benchmark_runs"] = migration.LEGACY_BENCHMARK_TABLE_SQL
    indexes = [name for name in migration.EXPECTED_INDEXES
               if name != "idx_backup_payload_retirement_backup"]
    _make_scratch_schema(source, tables, indexes, version=3)
    result = run(source, target, report, retired)
    assert result["quick_check"] == {"source": "ok", "target": "ok"}
    _assert_ledger_gained(target)


def test_pre_ledger_schema_with_unknown_extra_table_still_fails_closed(tmp_path: Path):
    source, target, report, retired = paths(tmp_path)
    tables = {k: v for k, v in migration.EXPECTED_TABLE_SQL.items()
              if k != migration.LEDGER_TABLE}
    indexes = [name for name in migration.EXPECTED_INDEXES
               if name != "idx_backup_payload_retirement_backup"]
    _make_scratch_schema(source, tables, indexes)
    connection = sqlite3.connect(source)
    connection.execute("CREATE TABLE smuggled_history (id TEXT PRIMARY KEY)")
    connection.commit()
    connection.close()
    with pytest.raises(migration.MigrationError, match="unsupported SQLite table set"):
        run(source, target, report, retired)
    assert not target.exists()


def paths(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    source = tmp_path / "source.db"
    target = tmp_path / "target.db"
    report = tmp_path / "report.json"
    retired = tmp_path / "retired.json"
    return source, target, report, retired


def run(source: Path, target: Path, report: Path, retired: Path, **kwargs):
    return migration.migrate(
        source,
        target,
        report,
        retired,
        **kwargs,
    )


def test_cli_argument_names_map_to_migration_paths(tmp_path: Path):
    source, target, report, retired = paths(tmp_path)
    make_db(source)

    assert migration.main([
        "--source", str(source),
        "--target", str(target),
        "--report", str(report),
        "--retired-export", str(retired),
    ]) == 0
    assert target.is_file()
    assert report.is_file()
    assert retired.is_file()


def test_migrates_every_actual_state_table_and_exact_retained_filter(tmp_path: Path):
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    connection = sqlite3.connect(source)
    state_db._configure(connection)
    connection.executemany(
        "INSERT INTO events VALUES (?,?,?,?,?)",
        [("e-good", TS, "minecraft-sunlit-cobblemon", "start", "ok"), ("e-old", TS, "minecraft", "start", "old")],
    )
    connection.execute("INSERT INTO audit VALUES (?,?,?,?,?,?,?,?)", ("a-good", TS, "operator", "start", "minecraft-sunlit-cobblemon", "ok", None, "done"))
    connection.executemany(
        "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?)",
        [("j-good", "terraria-vanilla", "stop", "succeeded", TS, TS, 1, "done"), ("j-running", "terraria-vanilla", "start", "running", TS, None, None, "live")],
    )
    connection.execute("INSERT INTO confirmations VALUES (?,?,?,?,?,?,?,?)", ("c1", "operator", "stop", "terraria-tmod", "opaque-secret", TS, None, 1))
    connection.execute("INSERT INTO backups VALUES (?,?,?,?,?,?)", ("b1", "terraria-tmod", TS, 10, 1, 1))
    connection.execute(
        "INSERT INTO backup_protections VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "b1",
            "terraria-tmod",
            "horizon-b2",
            "application",
            "remote/b1",
            "a" * 64,
            1,
            "succeeded",
            1,
            "verified",
            "succeeded",
            TS,
            None,
        ),
    )
    connection.execute("INSERT INTO notification_rules VALUES (?,?,?)", ("minecraft-sunlit-cobblemon", "failed", 1))
    connection.execute("INSERT INTO notification_deliveries VALUES (?,?,?,?,?,?,?)", ("d1", "terraria-vanilla", "failed", 2, "mail", TS, None))
    connection.execute("INSERT INTO updates VALUES (?,?,?,?,?,?,?)", ("u1", "terraria-tmod", TS, "image", "1", "2", "succeeded"))
    connection.execute("INSERT INTO rpc_idempotency VALUES (?,?,?,?,?)", ("r1", "{}", "secret-response", "completed", TS))
    connection.execute("INSERT INTO player_sessions VALUES (?,?,?,?,?,?)", ("s1", "minecraft-sunlit-cobblemon", "Player", TS, TS, "log"))
    connection.execute("INSERT INTO player_sessions VALUES (?,?,?,?,?,?)", ("s2", "minecraft-sunlit-cobblemon", "Current", TS, None, "log"))
    connection.execute("INSERT INTO metric_samples VALUES (?,?,?,?)", ("terraria-tmod", "players", TS, 2.0))
    # Terminal safety-ledger row: destructive-payload provenance that must
    # survive the migration exactly.
    connection.execute(
        "INSERT INTO backup_payload_retirement (operation_id, backup_id, profile_id, state, path,"
        " expected_device, expected_inode, expected_size, expected_mtime_ns, expected_ctime_ns,"
        " expected_sha256, manifest_sha256, created_at, updated_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "op-purged-1", "b1", "terraria-tmod", "purged",
            "/var/backups/game-servers/terraria-tmod/b1.tar.zst",
            2049, 4242, 10, 1, 2, "c" * 64, "d" * 64, TS, TS,
        ),
    )
    connection.executemany(
        "INSERT INTO benchmark_runs(id,profile_id,baseline_preset,candidate_preset,state,created_at,finished_at,overall_verdict,summary_json,artifact_path,error_code) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("br-good", "minecraft-sunlit-cobblemon", "current", "balanced-g1", "succeeded", TS, TS, "inconclusive", "{}", "/var/lib/game-control/benchmarks/br-good", None),
            ("br-running", "minecraft-sunlit-cobblemon", "current", "balanced-g1", "running", TS, None, None, None, None, None),
        ],
    )
    connection.commit()
    connection.close()

    result = run(source, target, report, retired)
    assert result["quick_check"] == {"source": "ok", "target": "ok"}
    target_db = sqlite3.connect(target)
    counts = {
        table: target_db.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        for table in migration.EXPECTED_TABLE_SQL
    }
    target_db.close()
    assert counts == {
        "events": 1,
        "audit": 1,
        "jobs": 1,
        "confirmations": 0,
        "backups": 1,
        "backup_protections": 1,
        "notification_rules": 1,
        "notification_deliveries": 1,
        "updates": 1,
        "rpc_idempotency": 0,
        "player_sessions": 1,
        "metric_samples": 1,
        "benchmark_runs": 1,
        "backup_payload_retirement": 1,
    }
    assert result["excluded_counts_by_table_reason"]["jobs"] == {"unfinished_job": 1}
    assert result["excluded_counts_by_table_reason"]["confirmations"] == {"transient_confirmation": 1}
    assert result["excluded_counts_by_table_reason"]["rpc_idempotency"] == {"transient_idempotency": 1}
    assert result["excluded_counts_by_table_reason"]["player_sessions"] == {"active_session": 1}
    assert result["excluded_counts_by_table_reason"]["benchmark_runs"] == {"unfinished_benchmark": 1}
    assert json.loads(retired.read_text())["row_count"] == 1
    target_check = sqlite3.connect(target)
    assert target_check.execute("PRAGMA foreign_key_check").fetchall() == []
    assert target_check.execute(
        "SELECT remote_key FROM backup_protections"
    ).fetchone() == ("remote/b1",)
    assert target_check.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='backup_protections'"
    ).fetchone()[0] == migration.EXPECTED_TABLE_SQL["backup_protections"]
    assert target_check.execute("PRAGMA foreign_key_list(backup_protections)").fetchall() == [
        (0, 0, "backups", "backup_id", "id", "NO ACTION", "RESTRICT", "NONE")
    ]
    assert target_check.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_backup_protections_scope'"
    ).fetchone()[0] == migration.EXPECTED_INDEX_SQL["idx_backup_protections_scope"]
    for trigger_name, trigger_sql in migration.EXPECTED_TRIGGER_SQL.items():
        assert target_check.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (trigger_name,)
        ).fetchone()[0] == trigger_sql
    target_check.close()


def test_legacy_and_unknown_evidence_never_contains_secret_text(tmp_path: Path):
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    connection = sqlite3.connect(source)
    state_db._configure(connection)
    connection.execute("INSERT INTO audit VALUES (?,?,?,?,?,?,?,?)", ("legacy", TS, "operator", "start", "minecraft", "failed", None, "TOP-SECRET token=do-not-export"))
    connection.execute("INSERT INTO events VALUES (?,?,?,?,?)", ("unknown", TS, "mystery-profile", "start", "PRIVATE-MATERIAL"))
    connection.commit()
    connection.close()
    result = run(source, target, report, retired)
    evidence = retired.read_bytes()
    assert b"TOP-SECRET" not in evidence
    assert b"PRIVATE-MATERIAL" not in evidence
    assert b"do-not-export" not in evidence
    assert b"TOP-SECRET" not in report.read_bytes()
    assert b"PRIVATE-MATERIAL" not in report.read_bytes()
    assert retired.stat().st_mode & 0o777 == 0o400
    assert result["retired_export_sha256"]


def test_unknown_schema_lock_and_link_safety_fail_closed(tmp_path: Path):
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    connection = sqlite3.connect(source)
    connection.execute("CREATE TABLE future_table (id TEXT)")
    connection.commit()
    connection.close()
    with pytest.raises(migration.MigrationError, match="table set"):
        run(source, target, report, retired)

    source.unlink()
    make_db(source)
    lock = sqlite3.connect(source, isolation_level=None)
    lock.execute("BEGIN EXCLUSIVE")
    with pytest.raises(migration.MigrationError, match="lock"):
        run(source, target, report, retired)
    lock.rollback()
    lock.close()

    source.unlink()
    make_db(source)
    hardlink = tmp_path / "hardlink.db"
    os.link(source, hardlink)
    with pytest.raises(migration.MigrationError, match="hard-linked"):
        run(hardlink, target, report, retired)
    symlink = tmp_path / "symlink.db"
    symlink.symlink_to(source)
    with pytest.raises(migration.MigrationError, match="regular"):
        run(symlink, target, report, retired)


def test_atomic_failure_leaves_no_target_or_temp_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    original_replace = migration.os.replace

    def fail_replace(old, new):
        if Path(new) == target:
            raise OSError("injected atomic failure")
        return original_replace(old, new)

    monkeypatch.setattr(migration.os, "replace", fail_replace)
    with pytest.raises(OSError, match="atomic failure"):
        run(source, target, report, retired)
    assert not target.exists()
    assert not list(tmp_path.glob(".target.db.tmp-*"))


def test_hashes_quick_check_and_generated_target_guard(tmp_path: Path):
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    first = run(source, target, report, retired)
    assert first["source_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert first["target_sha256"] == hashlib.sha256(target.read_bytes()).hexdigest()
    with pytest.raises(migration.MigrationError, match="non-empty target"):
        run(source, target, tmp_path / "second-report.json", tmp_path / "second-retired.json")
    second = run(source, target, tmp_path / "second-report.json", tmp_path / "second-retired.json", replace_empty_generated_target=True)
    assert second["target_sha256"]


def test_writer_gate_and_wal_shm_safety(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    monkeypatch.setattr(migration, "_writer_processes", lambda: [1234])
    with pytest.raises(migration.MigrationError, match="writer process"):
        run(source, target, report, retired)
    monkeypatch.setattr(migration, "_writer_processes", lambda: [])
    Path(f"{source}-shm").write_bytes(b"unsafe")
    Path(f"{source}-shm").chmod(0o600)
    with pytest.raises(migration.MigrationError, match="without WAL"):
        run(source, target, report, retired)


def test_exact_live_legacy_schema_upgrades_to_canonical_b2_target(tmp_path: Path):
    source, target, report, retired = paths(tmp_path)
    make_legacy_db(source)
    connection = sqlite3.connect(source)
    state_db._configure(connection)
    state_db._configure(connection)
    connection.execute(
        "INSERT INTO jobs(id,profile_id,operation,state,created_at,finished_at,detail,completion_seq) "
        "VALUES (?,?,?,?,?,?,?,?)",
        ("legacy-job", "terraria-vanilla", "stop", "succeeded", TS, TS, "done", 4),
    )
    connection.execute("INSERT INTO backups VALUES (?,?,?,?,?,?)", ("legacy-backup", "terraria-vanilla", TS, 8, 1, 0))
    connection.commit()
    connection.close()

    result = run(source, target, report, retired)
    assert result["included_counts_by_table"]["backup_protections"] == 0
    target_db = sqlite3.connect(target)
    assert [row[1] for row in target_db.execute("PRAGMA table_info(jobs)")] == [
        column[0] for column in migration.EXPECTED_COLUMNS["jobs"]
    ]
    assert target_db.execute("SELECT COUNT(*) FROM backup_protections").fetchone()[0] == 0
    assert target_db.execute("SELECT COUNT(*) FROM backups").fetchone()[0] == 1
    assert target_db.execute("PRAGMA foreign_key_list(backup_protections)").fetchall()
    assert target_db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='backups'"
    ).fetchone()[0] == migration.EXPECTED_TABLE_SQL["backups"]
    target_db.close()

    rerun = run(
        source,
        target,
        tmp_path / "rerun-report.json",
        tmp_path / "rerun-retired.json",
        replace_empty_generated_target=True,
    )
    assert rerun["target_sha256"]


def test_offline_jobs_migration_backfills_absolute_chronological_completion_sequence(tmp_path: Path):
    source, target, report, retired = paths(tmp_path)
    make_legacy_db(source)
    connection = sqlite3.connect(source)
    state_db._configure(connection)
    rows = [
        ("j-late", "terraria-vanilla", "stop", "succeeded", "2026-08-05T12:00:00Z", "2026-08-05T14:00:00Z", "done", None),
        ("j-tie-b", "terraria-vanilla", "stop", "succeeded", "2026-08-05T12:00:00Z", "2026-08-05T13:00:00+01:00", "done", 999),
        ("j-tie-a", "terraria-vanilla", "stop", "succeeded", "2026-08-05T12:00:00Z", "2026-08-05T12:00:00Z", "done", 2),
        ("j-early", "terraria-vanilla", "stop", "succeeded", "2026-08-05T12:00:00Z", "2026-08-05T11:00:00Z", "done", 1),
        ("j-open", "terraria-vanilla", "start", "running", "2026-08-05T12:00:00Z", None, "live", 77),
    ]
    connection.executemany("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?)", rows)
    connection.commit(); connection.close()
    run(source, target, report, retired)
    target_db = sqlite3.connect(target)
    assert target_db.execute("SELECT id, completion_seq FROM jobs WHERE state='succeeded' ORDER BY completion_seq").fetchall() == [("j-early", 1), ("j-tie-a", 2), ("j-tie-b", 3), ("j-late", 4)]
    assert target_db.execute("SELECT completion_seq FROM jobs WHERE id='j-open'").fetchone() is None
    target_db.close()


@pytest.mark.parametrize("malicious_object", ["index", "trigger", "foreign_key", "ddl"])
def test_malicious_schema_objects_and_ddl_are_rejected(tmp_path: Path, malicious_object: str):
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    connection = sqlite3.connect(source)
    if malicious_object == "index":
        connection.execute("CREATE INDEX malicious_index ON events(id)")
    elif malicious_object == "trigger":
        connection.execute(
            "CREATE TRIGGER malicious_trigger AFTER INSERT ON events BEGIN SELECT 1; END"
        )
    else:
        connection.execute("PRAGMA writable_schema = ON")
        if malicious_object == "foreign_key":
            table = "backup_protections"
            current = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()[0]
            current = current.replace(
                "UNIQUE (remote_key)",
                "FOREIGN KEY (profile_id) REFERENCES backups(profile_id), UNIQUE (remote_key)",
            )
        else:
            table = "events"
            current = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()[0]
            current = current.replace("message TEXT NOT NULL", "message TEXT")
        connection.execute(
            "UPDATE sqlite_master SET sql=? WHERE type='table' AND name=?",
            (current, table),
        )
        connection.execute("PRAGMA writable_schema = OFF")
    connection.commit()
    connection.close()
    with pytest.raises(migration.MigrationError):
        run(source, target, report, retired)


def test_source_evidence_records_wal_sidecars_and_logical_hash(tmp_path: Path):
    source, target, report, retired = paths(tmp_path)
    make_db(source)
    result = run(source, target, report, retired)
    assert result["source_wal_sha256"]
    assert result["source_wal_bytes"] is not None
    assert result["source_shm_sha256"]
    assert result["source_shm_bytes"] is not None
    assert len(result["source_logical_sha256"]) == 64


def test_retired_row_hash_ignores_secret_and_free_text_changes(tmp_path: Path):
    def migrate_secret(secret: str, suffix: str) -> str:
        source = tmp_path / f"source-{suffix}.db"
        target = tmp_path / f"target-{suffix}.db"
        report = tmp_path / f"report-{suffix}.json"
        retired = tmp_path / f"retired-{suffix}.json"
        make_legacy_db(source)
        connection = sqlite3.connect(source)
        state_db._configure(connection)
        connection.execute(
            "INSERT INTO audit VALUES (?,?,?,?,?,?,?,?)",
            ("legacy", TS, "operator", "start", "minecraft", "failed", None, secret),
        )
        connection.commit()
        connection.close()
        run(source, target, report, retired)
        return json.loads(retired.read_text())["rows"][0]["row_sha256"]

    first = migrate_secret("secret-one", "one")
    second = migrate_secret("secret-two", "two")
    assert first == second
