import os
import sqlite3
import stat
from pathlib import Path

import pytest

import game_control.state_db as state_db_module
import game_control.web_db as web_db_module
from game_control.state_db import StateDatabase
from game_control.web_db import WebDatabase


def _chmod_tree(path: Path, mode: int = 0o700) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(mode)


def _assert_db_files_secure(path: Path):
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        if candidate.exists():
            assert stat.S_IMODE(candidate.stat().st_mode) == 0o600


def test_state_database_migrates_with_append_only_operational_tables(tmp_path: Path, monkeypatch):
    directory = tmp_path / "state"
    _chmod_tree(directory)
    path = directory / "state.db"
    monkeypatch.setattr("game_control.state_db.STATE_DB_PATH", path)
    db = StateDatabase.open(path)
    expected = {
        "events",
        "audit",
        "jobs",
        "confirmations",
        "backups",
        "backup_protections",
        "notification_rules",
        "notification_deliveries",
        "updates",
        "rpc_idempotency",
        "player_sessions",
        "metric_samples",
        "benchmark_runs",
        # Additive, disposable audit ledger created idempotently at open. It is
        # intentionally outside the canonical version-4 schema. (The startup
        # estimate projection table is created lazily by the sampler, not here.)
        "backup_payload_retirement",
    }
    assert set(db.connection.execute("select name from sqlite_master where type='table'").fetchall()) == {
        (name,) for name in expected
    }
    assert db.connection.execute("pragma journal_mode").fetchone()[0] == "wal"
    assert db.connection.execute("pragma foreign_keys").fetchone()[0] == 1
    _assert_db_files_secure(path)
    db.connection.execute(
        "insert into events (id, timestamp, code, message) values (?, ?, ?, ?)",
        ("e1", "2026-07-11T12:00:00+00:00", "started", "ok"),
    )
    db.close()


def test_fresh_jobs_schema_has_integer_completion_sequence(tmp_path: Path, monkeypatch):
    directory = tmp_path / "state"
    _chmod_tree(directory)
    path = directory / "state.db"
    monkeypatch.setattr(state_db_module, "STATE_DB_PATH", path)

    db = StateDatabase.open(path)

    columns = {
        row[1]: row[2]
        for row in db.connection.execute("PRAGMA table_info(jobs)")
    }
    assert columns["completion_seq"].upper() == "INTEGER"
    assert db.connection.execute("PRAGMA user_version").fetchone()[0] == 4
    db.close()


def test_backup_protection_schema_rejects_orphans_invalid_states_and_duplicate_keys(
    tmp_path: Path, monkeypatch
):
    directory = tmp_path / "state"
    _chmod_tree(directory)
    path = directory / "state.db"
    monkeypatch.setattr(state_db_module, "STATE_DB_PATH", path)
    db = StateDatabase.open(path)
    timestamp = "2026-08-05T00:00:00+00:00"
    with pytest.raises(sqlite3.IntegrityError):
        db.connection.execute(
            "INSERT INTO backup_protections(backup_id,profile_id,destination_id,backup_class,remote_key,"
            "local_sha256,local_verified,upload_state,remote_verified,comparison_state,prune_state,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            ("missing", "minecraft-sunlit-cobblemon", "horizon-b2", "application", "helios/horizon/app/minecraft-sunlit-cobblemon/x.tar.zst", "sha", 1, "succeeded", 1, "verified", "succeeded", timestamp),
        )
    db.connection.execute(
        "INSERT INTO backups(id,profile_id,created_at,size_bytes,verified,protected) VALUES(?,?,?,?,?,?)",
        ("b1", "minecraft-sunlit-cobblemon", timestamp, 1, 1, 0),
    )
    db.connection.execute(
        "INSERT INTO backups(id,profile_id,created_at,size_bytes,verified,protected) VALUES(?,?,?,?,?,?)",
        ("b2", "minecraft-sunlit-cobblemon", timestamp, 1, 1, 0),
    )
    values = ("b1", "minecraft-sunlit-cobblemon", "horizon-b2", "application", "helios/horizon/app/minecraft-sunlit-cobblemon/x.tar.zst", "sha", 1, "succeeded", 1, "verified", "succeeded", timestamp)
    db.connection.execute(
        "INSERT INTO backup_protections(backup_id,profile_id,destination_id,backup_class,remote_key,"
        "local_sha256,local_verified,upload_state,remote_verified,comparison_state,prune_state,updated_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        values,
    )
    with pytest.raises(sqlite3.IntegrityError):
        db.connection.execute(
            "INSERT INTO backup_protections(backup_id,profile_id,destination_id,backup_class,remote_key,"
            "local_sha256,local_verified,upload_state,remote_verified,comparison_state,prune_state,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            ("b2", "minecraft-sunlit-cobblemon", "horizon-b2", "application", values[4], "sha", 1, "bogus", 1, "verified", "succeeded", timestamp),
        )
    db.close()


def test_legacy_jobs_require_offline_migration(
    tmp_path: Path, monkeypatch
):
    directory = tmp_path / "state"
    _chmod_tree(directory)
    path = directory / "state.db"
    monkeypatch.setattr(state_db_module, "STATE_DB_PATH", path)
    raw = sqlite3.connect(path)
    raw.execute(
        """
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            profile_id TEXT,
            operation TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at TEXT NOT NULL,
            finished_at TEXT,
            detail TEXT
        )
        """
    )
    raw.execute("PRAGMA user_version = 1")
    raw.executemany(
        "INSERT INTO jobs(id,profile_id,operation,state,created_at,finished_at,detail) VALUES (?,?,?,?,?,?,?)",
        [
            (
                "row-a",
                "minecraft",
                "start",
                "succeeded",
                "2026-01-01T00:00:00Z",
                "2026-01-01T02:00:00+02:00",
                "",
            ),
            (
                "row-b",
                "minecraft",
                "switch",
                "succeeded",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T01:00:00+01:00",
                "",
            ),
            (
                "row-c",
                "minecraft",
                "stop",
                "failed",
                "2026-01-01T01:00:00+02:00",
                "2026-01-01T00:00:00Z",
                "",
            ),
        ],
    )
    raw.commit()
    raw.close()
    path.chmod(0o600)

    with pytest.raises(RuntimeError, match="tools/migrations/state_migrate.py"):
        StateDatabase.open(path)


def test_state_audit_and_events_are_append_only(tmp_path: Path, monkeypatch):
    directory = tmp_path / "state"
    _chmod_tree(directory)
    monkeypatch.setattr("game_control.state_db.STATE_DB_PATH", directory / "state.db")
    db = StateDatabase.open(directory / "state.db")
    db.connection.execute(
        "insert into events (id, timestamp, code, message) values (?, ?, ?, ?)",
        ("e1", "2026-07-11T12:00:00+00:00", "started", "ok"),
    )
    db.connection.execute(
        "insert into audit (id, timestamp, actor, action, result, detail) values (?, ?, ?, ?, ?, ?)",
        ("a1", "2026-07-11T12:00:00+00:00", "operator", "start", "succeeded", "ok"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        db.connection.execute("update events set message='bad' where id='e1'")
    with pytest.raises(sqlite3.IntegrityError):
        db.connection.execute("delete from audit where id='a1'")
    db.close()


def test_insert_or_replace_cannot_rewrite_events_or_audit(tmp_path: Path, monkeypatch):
    directory = tmp_path / "state"
    _chmod_tree(directory)
    monkeypatch.setattr("game_control.state_db.STATE_DB_PATH", directory / "state.db")
    db = StateDatabase.open(directory / "state.db")
    timestamp = "2026-07-11T12:00:00+00:00"
    db.connection.execute(
        "insert into events (id, timestamp, code, message) values (?, ?, ?, ?)",
        ("e1", timestamp, "started", "original"),
    )
    db.connection.execute(
        "insert into audit (id, timestamp, actor, action, result, detail) values (?, ?, ?, ?, ?, ?)",
        ("a1", timestamp, "operator", "start", "succeeded", "original"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        db.connection.execute(
            "insert or replace into events (id, timestamp, code, message) values (?, ?, ?, ?)",
            ("e1", timestamp, "started", "rewritten"),
        )
    with pytest.raises(sqlite3.IntegrityError):
        db.connection.execute(
            "insert or replace into audit (id, timestamp, actor, action, result, detail) values (?, ?, ?, ?, ?, ?)",
            ("a1", timestamp, "operator", "start", "succeeded", "rewritten"),
        )
    assert db.connection.execute("select message from events where id='e1'").fetchone()[0] == "original"
    assert db.connection.execute("select detail from audit where id='a1'").fetchone()[0] == "original"
    db.close()


def test_state_database_rejects_non_rfc3339_timestamp(tmp_path: Path, monkeypatch):
    directory = tmp_path / "state"
    _chmod_tree(directory)
    monkeypatch.setattr("game_control.state_db.STATE_DB_PATH", directory / "state.db")
    db = StateDatabase.open(directory / "state.db")
    with pytest.raises(sqlite3.IntegrityError):
        db.connection.execute(
            "insert into events (id, timestamp, code, message) values (?, ?, ?, ?)",
            ("bad", "2026-07-11 12:00:00", "started", "bad"),
        )
    db.close()


def test_state_migration_rolls_back_partial_ddl(tmp_path: Path, monkeypatch):
    directory = tmp_path / "state"
    _chmod_tree(directory)
    path = directory / "state.db"
    monkeypatch.setattr(state_db_module, "STATE_DB_PATH", path)
    monkeypatch.setattr(
        state_db_module,
        "_STATE_TABLES",
        ("CREATE TABLE partial (id TEXT)", "CREATE TABLE broken ("),
    )
    with pytest.raises(sqlite3.OperationalError):
        StateDatabase.open(path)
    raw = sqlite3.connect(path)
    assert raw.execute("pragma user_version").fetchone()[0] == 0
    assert raw.execute(
        "select count(*) from sqlite_master where type='table' and name='partial'"
    ).fetchone()[0] == 0
    raw.close()


def test_state_accepts_arbitrary_rfc3339_secfrac(tmp_path: Path, monkeypatch):
    directory = tmp_path / "state"
    _chmod_tree(directory)
    path = directory / "state.db"
    monkeypatch.setattr(state_db_module, "STATE_DB_PATH", path)
    db = StateDatabase.open(path)
    db.connection.execute(
        "insert into events (id, timestamp, code, message) values (?, ?, ?, ?)",
        ("fractional", "2026-07-11T12:00:00.123456789+00:00", "started", "ok"),
    )
    db.close()


def test_web_database_isolated_and_idempotent(tmp_path: Path, monkeypatch):
    directory = tmp_path / "web"
    _chmod_tree(directory)
    path = directory / "web.db"
    monkeypatch.setattr("game_control.web_db.WEB_DB_PATH", path)
    monkeypatch.setattr(WebDatabase, "_owner_ids", staticmethod(lambda: (os.getuid(), os.getgid())))
    db = WebDatabase.open(path)
    assert set(db.connection.execute("select name from sqlite_master where type='table'").fetchall()) == {
        ("web_sessions",)
    }
    assert db.connection.execute("pragma journal_mode").fetchone()[0] == "wal"
    assert db.connection.execute("pragma foreign_keys").fetchone()[0] == 1
    _assert_db_files_secure(path)
    db.close()
    second = WebDatabase.open(path)
    assert second.connection.execute("select count(*) from web_sessions").fetchone()[0] == 0
    second.close()


def test_web_database_rejects_non_rfc3339_timestamp(tmp_path: Path, monkeypatch):
    directory = tmp_path / "web"
    _chmod_tree(directory)
    path = directory / "web.db"
    monkeypatch.setattr("game_control.web_db.WEB_DB_PATH", path)
    monkeypatch.setattr(WebDatabase, "_owner_ids", staticmethod(lambda: (os.getuid(), os.getgid())))
    db = WebDatabase.open(path)
    with pytest.raises(sqlite3.IntegrityError):
        db.connection.execute(
            "insert into web_sessions (id, actor, csrf_token, created_at, expires_at) values (?, ?, ?, ?, ?)",
            ("s1", "operator", "csrf", "2026-07-11T12:00:00", "2026-07-11T13:00:00+00:00"),
        )
    db.close()


def test_web_migration_rolls_back_partial_ddl(tmp_path: Path, monkeypatch):
    directory = tmp_path / "web"
    _chmod_tree(directory)
    path = directory / "web.db"
    monkeypatch.setattr(web_db_module, "WEB_DB_PATH", path)
    monkeypatch.setattr(WebDatabase, "_owner_ids", staticmethod(lambda: (os.getuid(), os.getgid())))
    monkeypatch.setattr(
        web_db_module,
        "_WEB_MIGRATIONS",
        ("CREATE TABLE partial (id TEXT)", "CREATE TABLE broken ("),
    )
    with pytest.raises(sqlite3.OperationalError):
        WebDatabase.open(path)
    raw = sqlite3.connect(path)
    assert raw.execute("pragma user_version").fetchone()[0] == 0
    assert raw.execute(
        "select count(*) from sqlite_master where type='table' and name='partial'"
    ).fetchone()[0] == 0
    raw.close()


def test_web_accepts_arbitrary_rfc3339_secfrac(tmp_path: Path, monkeypatch):
    directory = tmp_path / "web"
    _chmod_tree(directory)
    path = directory / "web.db"
    monkeypatch.setattr(web_db_module, "WEB_DB_PATH", path)
    monkeypatch.setattr(WebDatabase, "_owner_ids", staticmethod(lambda: (os.getuid(), os.getgid())))
    db = WebDatabase.open(path)
    db.connection.execute(
        "insert into web_sessions (id, actor, csrf_token, created_at, expires_at) values (?, ?, ?, ?, ?)",
        (
            "fractional",
            "operator",
            "csrf",
            "2026-07-11T12:00:00.123456789+00:00",
            "2026-07-11T13:00:00.123456789Z",
        ),
    )
    db.close()


def test_database_rejects_unapproved_paths_and_permissions(tmp_path: Path):
    with pytest.raises(PermissionError):
        StateDatabase.open(tmp_path / "state.db")
    with pytest.raises(PermissionError):
        WebDatabase.open(tmp_path / "web.db")


def test_state_database_refuses_insecure_directory(tmp_path: Path, monkeypatch):
    directory = tmp_path / "state"
    _chmod_tree(directory, 0o755)
    path = directory / "state.db"
    monkeypatch.setattr("game_control.state_db.STATE_DB_PATH", path)
    with pytest.raises(PermissionError):
        StateDatabase.open(path)


def test_web_database_refuses_wrong_owner(tmp_path: Path, monkeypatch):
    directory = tmp_path / "web"
    _chmod_tree(directory)
    path = directory / "web.db"
    monkeypatch.setattr("game_control.web_db.WEB_DB_PATH", path)
    monkeypatch.setattr(WebDatabase, "_owner_ids", staticmethod(lambda: (os.getuid() + 1, os.getgid())))
    with pytest.raises(PermissionError):
        WebDatabase.open(path)


def test_database_connections_never_attach_other_store(tmp_path: Path, monkeypatch):
    state_dir = tmp_path / "state"
    web_dir = tmp_path / "web"
    _chmod_tree(state_dir)
    _chmod_tree(web_dir)
    monkeypatch.setattr("game_control.state_db.STATE_DB_PATH", state_dir / "state.db")
    monkeypatch.setattr("game_control.web_db.WEB_DB_PATH", web_dir / "web.db")
    monkeypatch.setattr(WebDatabase, "_owner_ids", staticmethod(lambda: (os.getuid(), os.getgid())))
    state = StateDatabase.open(state_dir / "state.db")
    web = WebDatabase.open(web_dir / "web.db")
    with pytest.raises(sqlite3.DatabaseError):
        state.connection.execute(f"attach database '{web_dir / 'web.db'}' as web")
    with pytest.raises(sqlite3.DatabaseError):
        web.connection.execute(f"attach database '{state_dir / 'state.db'}' as state")
    state.close()
    web.close()
