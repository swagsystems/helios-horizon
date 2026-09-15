from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator


STATE_DB_PATH = Path("/var/lib/game-control/state.db")

# Durable, append-forever local-payload retirement ledger.  Rows are written and
# re-classified but never deleted: the `backups` and `backup_protections`
# history stays intact and no foreign key is bypassed.  Availability is derived
# from the rows plus the on-disk payload; the table itself is the audit trail.
RETIREMENT_LEDGER_DDL = """
    CREATE TABLE IF NOT EXISTS backup_payload_retirement (
        operation_id TEXT NOT NULL,
        backup_id TEXT NOT NULL REFERENCES backups(id) ON DELETE RESTRICT,
        profile_id TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN (
            'prepared', 'quarantined', 'purge_prepared', 'purged',
            'rolled_back', 'failed', 'ambiguous'
        )),
        path TEXT NOT NULL,
        quarantine_path TEXT,
        expected_device INTEGER NOT NULL,
        expected_inode INTEGER NOT NULL,
        expected_size INTEGER NOT NULL,
        expected_mtime_ns INTEGER NOT NULL,
        expected_ctime_ns INTEGER NOT NULL,
        expected_sha256 TEXT NOT NULL,
        manifest_sha256 TEXT NOT NULL,
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1),
        updated_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(updated_at) = 1),
        error_code TEXT,
        PRIMARY KEY (operation_id, backup_id)
    )
"""

# States that make a payload unavailable for local restore/protection.  A
# ``rolled_back`` row is deliberately absent: after an identity-verified
# rollback the payload is present again.
RETIREMENT_BLOCKING_STATES = (
    "prepared",
    "quarantined",
    "purge_prepared",
    "purged",
    "failed",
    "ambiguous",
)

_STATE_TABLES = (
    """
    CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL CHECK (is_rfc3339_timestamp(timestamp) = 1),
        profile_id TEXT,
        code TEXT NOT NULL,
        message TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS audit (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL CHECK (is_rfc3339_timestamp(timestamp) = 1),
        actor TEXT NOT NULL,
        action TEXT NOT NULL,
        profile_id TEXT,
        result TEXT NOT NULL,
        error_code TEXT,
        detail TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        profile_id TEXT,
        operation TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1),
        finished_at TEXT CHECK (finished_at IS NULL OR is_rfc3339_timestamp(finished_at) = 1),
        completion_seq INTEGER,
        detail TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS confirmations (
        id TEXT PRIMARY KEY,
        actor TEXT NOT NULL,
        action TEXT NOT NULL,
        profile_id TEXT,
        payload TEXT NOT NULL,
        expires_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(expires_at) = 1),
        consumed_at TEXT CHECK (consumed_at IS NULL OR is_rfc3339_timestamp(consumed_at) = 1),
        state_generation INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS backups (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1),
        size_bytes INTEGER NOT NULL,
        verified INTEGER NOT NULL CHECK (verified IN (0, 1)),
        protected INTEGER NOT NULL CHECK (protected IN (0, 1))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS backup_protections (
        backup_id TEXT NOT NULL REFERENCES backups(id) ON DELETE RESTRICT,
        profile_id TEXT NOT NULL,
        destination_id TEXT NOT NULL CHECK (destination_id IN ('horizon-b2')),
        backup_class TEXT NOT NULL CHECK (backup_class IN ('application', 'full-lxc')),
        remote_key TEXT NOT NULL,
        local_sha256 TEXT NOT NULL,
        local_verified INTEGER NOT NULL CHECK (local_verified IN (0, 1)),
        upload_state TEXT NOT NULL CHECK (upload_state IN ('not_started', 'pending', 'succeeded', 'failed')),
        remote_verified INTEGER NOT NULL CHECK (remote_verified IN (0, 1)),
        comparison_state TEXT NOT NULL CHECK (comparison_state IN ('not_started', 'pending', 'verified', 'failed')),
        prune_state TEXT NOT NULL CHECK (prune_state IN ('not_started', 'pending', 'succeeded', 'failed', 'deleted')),
        updated_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(updated_at) = 1),
        error_code TEXT,
        PRIMARY KEY (backup_id, destination_id, backup_class),
        UNIQUE (remote_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS notification_rules (
        profile_id TEXT NOT NULL,
        event TEXT NOT NULL,
        enabled INTEGER NOT NULL,
        PRIMARY KEY (profile_id, event)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS notification_deliveries (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        event TEXT NOT NULL,
        state_generation INTEGER NOT NULL,
        channel TEXT NOT NULL,
        delivered_at TEXT CHECK (delivered_at IS NULL OR is_rfc3339_timestamp(delivered_at) = 1),
        error_code TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS updates (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1),
        strategy TEXT NOT NULL,
        prior_version TEXT,
        new_version TEXT,
        state TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rpc_idempotency (
        request_id TEXT PRIMARY KEY,
        canonical_request TEXT NOT NULL,
        response TEXT,
        status TEXT NOT NULL DEFAULT 'completed',
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS player_sessions (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        player TEXT NOT NULL,
        started_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(started_at) = 1),
        ended_at TEXT CHECK (ended_at IS NULL OR is_rfc3339_timestamp(ended_at) = 1),
        source TEXT NOT NULL CHECK (source IN ('crafty', 'log', 'recovered'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS metric_samples (
        profile_id TEXT NOT NULL,
        metric TEXT NOT NULL CHECK (metric IN ('tps', 'mspt', 'players') OR metric LIKE 'perf.%'),
        ts TEXT NOT NULL CHECK (is_rfc3339_timestamp(ts) = 1),
        value REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS benchmark_runs (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        baseline_preset TEXT NOT NULL,
        candidate_preset TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('running', 'succeeded', 'failed')),
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1),
        finished_at TEXT CHECK (finished_at IS NULL OR is_rfc3339_timestamp(finished_at) = 1),
        overall_verdict TEXT CHECK (overall_verdict IS NULL OR overall_verdict IN ('better', 'worse', 'mixed', 'inconclusive')),
        summary_json TEXT,
        artifact_path TEXT,
        artifact_sha256 TEXT,
        error_code TEXT,
        provenance_json TEXT,
        planned_pairs INTEGER NOT NULL DEFAULT 0,
        completed_pairs INTEGER NOT NULL DEFAULT 0,
        primary_endpoints_json TEXT,
        thresholds_json TEXT,
        driver_verdict TEXT,
        failure_category TEXT NOT NULL DEFAULT 'none',
        stage TEXT NOT NULL DEFAULT 'prepared',
        progress INTEGER NOT NULL DEFAULT 0
    )
    """,
    RETIREMENT_LEDGER_DDL,
)


class StateDatabase:
    def __init__(self, connection: sqlite3.Connection, path: Path):
        self.connection = connection
        self.path = path

    @classmethod
    def open(cls, path: str | os.PathLike[str]) -> "StateDatabase":
        requested = Path(path)
        if requested.absolute() != STATE_DB_PATH.absolute():
            raise PermissionError("state database path is not approved")
        if os.geteuid() != 0:
            raise PermissionError("state database requires root")
        _reject_symlink(requested.parent)
        _prepare_directory(requested.parent, 0, 0)
        _check_owner_mode(requested.parent, 0, 0, 0o700)
        _check_existing_file(requested, 0, 0)
        _check_existing_sidecars(requested, 0, 0)
        connection = sqlite3.connect(requested, timeout=5.0)
        _configure(connection)
        try:
            connection.execute("BEGIN IMMEDIATE")
            _migrate_state(connection)
            connection.commit()
            _secure_file(requested, 0, 0)
            _secure_sidecars(requested, 0, 0)
        except Exception:
            connection.rollback()
            connection.close()
            raise
        return cls(connection, requested)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            yield self.connection
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def close(self) -> None:
        self.connection.close()


def _migrate_state(connection: sqlite3.Connection) -> None:
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    # StateDatabase only opens the canonical schema.  Historical databases are
    # upgraded by the offline, locked tools/migrations/state_migrate.py utility; doing
    # ALTER TABLE here made a controller startup an implicit migration.
    if version not in (0, 4):
        raise RuntimeError(
            f"unsupported state database schema version {version}; run tools/migrations/state_migrate.py"
        )
    for statement in _STATE_TABLES:
        connection.execute(statement)
    required = {
        "metric_samples": {"profile_id", "metric", "ts", "value"},
        "rpc_idempotency": {"request_id", "canonical_request", "response", "status", "created_at"},
        "confirmations": {"id", "state_generation"},
        "jobs": {"id", "completion_seq"},
        "benchmark_runs": {
            "id", "provenance_json", "planned_pairs", "completed_pairs",
            "primary_endpoints_json", "thresholds_json", "driver_verdict",
            "failure_category", "stage", "progress", "artifact_sha256",
        },
    }
    for table, expected in required.items():
        actual = {row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')}
        if not expected.issubset(actual):
            raise RuntimeError(
                f"non-canonical state database table {table}; run tools/migrations/state_migrate.py"
            )
    connection.execute(
        "CREATE TRIGGER IF NOT EXISTS events_append_only_update "
        "BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT, 'events are append-only'); END"
    )
    connection.execute(
        "CREATE TRIGGER IF NOT EXISTS events_append_only_delete "
        "BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'events are append-only'); END"
    )
    connection.execute(
        "CREATE TRIGGER IF NOT EXISTS audit_append_only_update "
        "BEFORE UPDATE ON audit BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END"
    )
    connection.execute(
        "CREATE TRIGGER IF NOT EXISTS audit_append_only_delete "
        "BEFORE DELETE ON audit BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_player_sessions_profile"
        " ON player_sessions(profile_id, started_at)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_metric_samples"
        " ON metric_samples(profile_id, metric, ts)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_backup_protections_scope"
        " ON backup_protections(profile_id, destination_id, backup_class, remote_verified, comparison_state)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_benchmark_runs_profile"
        " ON benchmark_runs(profile_id, created_at DESC)"
    )
    # History pages use (timestamp,id) keyset cursors.  The id tie-breaker
    # makes equal-timestamp rows deterministic and avoids deep OFFSET scans.
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_history_cursor"
        " ON events(timestamp DESC, id DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_history_cursor"
        " ON audit(timestamp DESC, id DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_backup_payload_retirement_backup"
        " ON backup_payload_retirement(backup_id, state)"
    )
    connection.execute("PRAGMA user_version = 4")


def retirement_blocking_states() -> tuple[str, ...]:
    """Return the ledger states that make a local payload unavailable."""

    return RETIREMENT_BLOCKING_STATES


def ensure_additive_state_tables(connection: sqlite3.Connection) -> None:
    """Create additive feature tables that are not part of the canonical schema.

    ``_STATE_TABLES`` is the exact version-4 schema, and the offline migration
    utility (``tools/migrations/state_migrate.py``) snapshots a source database
    as an exact table set.  Feature tables that arrive after a deployment are
    therefore created idempotently on the already-configured controller
    connection instead of being appended to that canonical tuple.  They hold
    bounded, disposable projection data and never gate lifecycle authority.
    """

    from .startup_estimates import STARTUP_ESTIMATE_DDL

    for statement in STARTUP_ESTIMATE_DDL:
        connection.execute(statement)


def prune_metric_samples(
    connection: sqlite3.Connection, *, now: str, max_age_days: int = 30
) -> int:
    cursor = connection.execute(
        "DELETE FROM metric_samples"
        " WHERE julianday(ts) < julianday(?) - ?",
        (now, max_age_days),
    )
    return cursor.rowcount


def prune_completed_rpc_idempotency(
    connection: sqlite3.Connection, *, now: str, max_age_hours: int = 48
) -> int:
    """Delete expired completed replays while preserving every pending claim.

    ``created_at`` accepts RFC3339 offsets, so chronological comparison stays
    on SQLite's date parser until the epoch migration rather than using unsafe
    lexical ordering.
    """
    if max_age_hours < 1:
        raise ValueError("max_age_hours must be positive")
    cursor = connection.execute(
        "DELETE FROM rpc_idempotency"
        " WHERE status = 'completed'"
        " AND julianday(created_at) < julianday(?) - (? / 24.0)",
        (now, max_age_hours),
    )
    return cursor.rowcount


def _configure(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA recursive_triggers = ON")
    connection.create_function("is_rfc3339_timestamp", 1, _is_rfc3339_timestamp, deterministic=True)
    connection.set_authorizer(_deny_attach)


def _deny_attach(action: int, _arg1, _arg2, _db, _source) -> int:
    return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_ATTACH else sqlite3.SQLITE_OK


_RFC3339 = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


def _is_rfc3339_timestamp(value: object) -> int:
    if not isinstance(value, str) or not _RFC3339.fullmatch(value):
        return 0
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return 0
    return int(parsed.tzinfo is not None and parsed.utcoffset() is not None)


def _prepare_directory(path: Path, uid: int, gid: int) -> None:
    if path.exists():
        return
    path.mkdir(parents=True, mode=0o700)
    os.chown(path, uid, gid)


def _check_owner_mode(path: Path, uid: int, gid: int, mode: int) -> None:
    info = path.stat()
    if info.st_uid != uid or info.st_gid != gid or (info.st_mode & 0o777) != mode:
        raise PermissionError(f"insecure permissions on {path}")


def _check_existing_file(path: Path, uid: int, gid: int) -> None:
    if path.is_symlink():
        raise PermissionError(f"symlink is not allowed: {path}")
    if path.exists():
        _check_owner_mode(path, uid, gid, 0o600)


def _check_existing_sidecars(path: Path, uid: int, gid: int) -> None:
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{path}{suffix}")
        _check_existing_file(sidecar, uid, gid)


def _reject_symlink(path: Path) -> None:
    if path.is_symlink():
        raise PermissionError(f"symlink is not allowed: {path}")


def _secure_file(path: Path, uid: int, gid: int) -> None:
    os.chown(path, uid, gid)
    os.chmod(path, 0o600)


def _secure_sidecars(path: Path, uid: int, gid: int) -> None:
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists():
            _secure_file(sidecar, uid, gid)
