"""The Sunlit updater's update-history write must use the configured schema."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from game_control import state_db
from game_control import sunlit_update as updater


def _updates_ddl() -> str:
    """Return the REAL ``updates`` table DDL from the state schema."""
    for statement in state_db._STATE_TABLES:
        if "CREATE TABLE IF NOT EXISTS updates" in statement:
            return statement
    raise AssertionError("canonical updates DDL was not found")


def _configured_db(path: Path) -> sqlite3.Connection:
    """Create the real schema on a configured connection (the controller path)."""
    connection = sqlite3.connect(path)
    state_db._configure(connection)
    connection.execute(_updates_ddl())
    connection.commit()
    return connection


def _insert(connection: sqlite3.Connection, prior: str, new: str) -> None:
    connection.execute(
        "INSERT INTO updates(id,profile_id,created_at,strategy,prior_version,new_version,state) "
        "VALUES(?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'),'curated_modpack',?,?,?)",
        ("row-" + prior, updater.PROFILE, prior, new, "succeeded"),
    )


def test_real_updates_ddl_uses_the_is_rfc3339_timestamp_function():
    ddl = _updates_ddl()
    assert "is_rfc3339_timestamp(created_at)" in ddl


def test_bare_connection_cannot_satisfy_real_updates_check(tmp_path: Path):
    """Prove the exact defect: an unconfigured connection fails the CHECK."""
    database = tmp_path / "state.db"
    _configured_db(database).close()

    bare = sqlite3.connect(database, timeout=5)
    try:
        with pytest.raises(sqlite3.OperationalError, match="is_rfc3339_timestamp"):
            _insert(bare, "1.1.3-SSV4.1.4", "1.1.4-SSV4.1.5")
    finally:
        bare.close()


def test_record_writes_history_on_the_real_schema(tmp_path: Path, monkeypatch):
    database = tmp_path / "state.db"
    _configured_db(database).close()
    monkeypatch.setattr(updater, "DATABASE", database)

    updater._record("1.1.3-SSV4.1.4", "1.1.4-SSV4.1.5")

    reader = sqlite3.connect(database)
    try:
        rows = reader.execute(
            "SELECT profile_id,created_at,strategy,prior_version,new_version,state FROM updates"
        ).fetchall()
    finally:
        reader.close()
    assert len(rows) == 1
    profile_id, created_at, strategy, prior, new, state = rows[0]
    assert profile_id == updater.PROFILE
    assert strategy == "curated_modpack"
    assert (prior, new, state) == ("1.1.3-SSV4.1.4", "1.1.4-SSV4.1.5", "succeeded")
    # The stored value satisfies the schema's own validator.
    assert state_db._is_rfc3339_timestamp(created_at) == 1


def test_record_wraps_schema_failure_as_safe_update_error(tmp_path: Path, monkeypatch):
    database = tmp_path / "empty.db"
    sqlite3.connect(database).close()
    monkeypatch.setattr(updater, "DATABASE", database)
    with pytest.raises(updater.UpdateError, match="update history state is unavailable"):
        updater._record("1.1.3-SSV4.1.4", "1.1.4-SSV4.1.5")


def test_record_depends_on_the_configured_connection(tmp_path: Path, monkeypatch):
    """Removing the configure step restores the exact production failure."""
    database = tmp_path / "state.db"
    _configured_db(database).close()
    monkeypatch.setattr(updater, "DATABASE", database)
    monkeypatch.setattr(updater, "_configure_state_connection", lambda connection: None)
    with pytest.raises(updater.UpdateError, match="update history state is unavailable"):
        updater._record("1.1.3-SSV4.1.4", "1.1.4-SSV4.1.5")
