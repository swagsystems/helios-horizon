#!/usr/bin/env python3
"""Offline, fail-closed exporter for the Horizon root state database.

This utility intentionally has no service, shell, or profile-discovery hooks.
The operator supplies three absolute artifact paths and the utility only reads
the source while holding an SQLite exclusive transaction, then creates the
target through a same-directory atomic replace.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


APP_ID = 0x484F5249  # ASCII "HORI"; marks a generated Horizon migration DB.
SCHEMA_VERSION = 4
RETAINED_PROFILES = frozenset(
    {"minecraft-sunlit-cobblemon", "terraria-vanilla", "terraria-tmod"}
)
LEGACY_PROFILES = frozenset({"minecraft", "pz-rising"})

# Bounded, disposable *projection* table created idempotently on the live
# controller connection (`state_db.ensure_additive_state_tables`). It is not
# historical authority: the migration intentionally omits it, and the table
# simply relearns from future observations -- earlier training rows are not
# reconstructed or replayed. It is ignored rather than refusing the database.
#
# The `backup_payload_retirement` ledger is deliberately NOT in this set: it is
# a safety ledger with terminal states, it is part of the canonical schema
# below, and every row must survive the migration. Anything else is still a
# hard fail-closed error.
ADDITIVE_FEATURE_TABLES = frozenset({"startup_estimate_samples"})
LEDGER_TABLE = "backup_payload_retirement"
WRITER_MARKERS = (
    "game_control.controller",
    "game_control.slotd_main",
    "game_control.web_main",
    "game-slotd",
    "game-control-web",
)
SENSITIVE_COLUMNS = re.compile(
    r"(?:secret|token|password|credential|csrf|cookie|authorization|private|seed|payload|canonical_request|response)$",
    re.IGNORECASE,
)
SENSITIVE_TEXT_COLUMNS = frozenset({
    "action",
    "actor",
    "detail",
    "message",
    "payload",
    "response",
})
RFC3339 = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


class MigrationError(RuntimeError):
    """A safe, operator-actionable migration refusal."""


def _normal_sql(sql: str | None) -> str:
    compact = re.sub(r"\s+", " ", (sql or "").strip()).lower()
    return re.sub(r"\s*([(),])\s*", r"\1", compact)


# These are the post-migration schemas emitted by src/game_control/state_db.py.
# The shape is deliberately duplicated here so this script remains a fixed,
# independently reviewable offline tool and does not import application code.
EXPECTED_TABLE_SQL = {
    "events": """CREATE TABLE events (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL CHECK (is_rfc3339_timestamp(timestamp) = 1),
        profile_id TEXT,
        code TEXT NOT NULL,
        message TEXT NOT NULL
    )""",
    "audit": """CREATE TABLE audit (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL CHECK (is_rfc3339_timestamp(timestamp) = 1),
        actor TEXT NOT NULL,
        action TEXT NOT NULL,
        profile_id TEXT,
        result TEXT NOT NULL,
        error_code TEXT,
        detail TEXT NOT NULL
    )""",
    "jobs": """CREATE TABLE jobs (
        id TEXT PRIMARY KEY,
        profile_id TEXT,
        operation TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1),
        finished_at TEXT CHECK (finished_at IS NULL OR is_rfc3339_timestamp(finished_at) = 1),
        completion_seq INTEGER,
        detail TEXT
    )""",
    "confirmations": """CREATE TABLE confirmations (
        id TEXT PRIMARY KEY,
        actor TEXT NOT NULL,
        action TEXT NOT NULL,
        profile_id TEXT,
        payload TEXT NOT NULL,
        expires_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(expires_at) = 1),
        consumed_at TEXT CHECK (consumed_at IS NULL OR is_rfc3339_timestamp(consumed_at) = 1), state_generation INTEGER NOT NULL DEFAULT 0
    )""",
    "backups": """CREATE TABLE backups (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1),
        size_bytes INTEGER NOT NULL,
        verified INTEGER NOT NULL CHECK (verified IN (0, 1)),
        protected INTEGER NOT NULL CHECK (protected IN (0, 1))
    )""",
    "backup_protections": """CREATE TABLE backup_protections (
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
    )""",
    "notification_rules": """CREATE TABLE notification_rules (
        profile_id TEXT NOT NULL,
        event TEXT NOT NULL,
        enabled INTEGER NOT NULL,
        PRIMARY KEY (profile_id, event)
    )""",
    "notification_deliveries": """CREATE TABLE notification_deliveries (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        event TEXT NOT NULL,
        state_generation INTEGER NOT NULL,
        channel TEXT NOT NULL,
        delivered_at TEXT CHECK (delivered_at IS NULL OR is_rfc3339_timestamp(delivered_at) = 1),
        error_code TEXT
    )""",
    "updates": """CREATE TABLE updates (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1),
        strategy TEXT NOT NULL,
        prior_version TEXT,
        new_version TEXT,
        state TEXT NOT NULL
    )""",
    "rpc_idempotency": """CREATE TABLE rpc_idempotency (
        request_id TEXT PRIMARY KEY,
        canonical_request TEXT NOT NULL,
        response TEXT,
        status TEXT NOT NULL DEFAULT 'completed',
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1)
    )""",
    "player_sessions": """CREATE TABLE player_sessions (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        player TEXT NOT NULL,
        started_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(started_at) = 1),
        ended_at TEXT CHECK (ended_at IS NULL OR is_rfc3339_timestamp(ended_at) = 1),
        source TEXT NOT NULL CHECK (source IN ('crafty', 'log', 'recovered'))
    )""",
    "metric_samples": """CREATE TABLE metric_samples (
        profile_id TEXT NOT NULL,
        metric TEXT NOT NULL CHECK (metric IN ('tps', 'mspt', 'players') OR metric LIKE 'perf.%'),
        ts TEXT NOT NULL CHECK (is_rfc3339_timestamp(ts) = 1),
        value REAL NOT NULL
    )""",
    "benchmark_runs": """CREATE TABLE benchmark_runs (
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
    )""",
    # Safety ledger: typed retirement state per local backup payload.  Its rows
    # are provenance for destructive operations and must never be dropped,
    # recreated empty, or filtered by the offline migration.
    "backup_payload_retirement": """CREATE TABLE backup_payload_retirement (
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
    )""",
}
EXPECTED_COLUMNS = {
    "events": [("id", "TEXT", 0, None, 1), ("timestamp", "TEXT", 1, None, 0), ("profile_id", "TEXT", 0, None, 0), ("code", "TEXT", 1, None, 0), ("message", "TEXT", 1, None, 0)],
    "audit": [("id", "TEXT", 0, None, 1), ("timestamp", "TEXT", 1, None, 0), ("actor", "TEXT", 1, None, 0), ("action", "TEXT", 1, None, 0), ("profile_id", "TEXT", 0, None, 0), ("result", "TEXT", 1, None, 0), ("error_code", "TEXT", 0, None, 0), ("detail", "TEXT", 1, None, 0)],
    "jobs": [("id", "TEXT", 0, None, 1), ("profile_id", "TEXT", 0, None, 0), ("operation", "TEXT", 1, None, 0), ("state", "TEXT", 1, None, 0), ("created_at", "TEXT", 1, None, 0), ("finished_at", "TEXT", 0, None, 0), ("completion_seq", "INTEGER", 0, None, 0), ("detail", "TEXT", 0, None, 0)],
    "confirmations": [("id", "TEXT", 0, None, 1), ("actor", "TEXT", 1, None, 0), ("action", "TEXT", 1, None, 0), ("profile_id", "TEXT", 0, None, 0), ("payload", "TEXT", 1, None, 0), ("expires_at", "TEXT", 1, None, 0), ("consumed_at", "TEXT", 0, None, 0), ("state_generation", "INTEGER", 1, "0", 0)],
    "backups": [("id", "TEXT", 0, None, 1), ("profile_id", "TEXT", 1, None, 0), ("created_at", "TEXT", 1, None, 0), ("size_bytes", "INTEGER", 1, None, 0), ("verified", "INTEGER", 1, None, 0), ("protected", "INTEGER", 1, None, 0)],
    "backup_protections": [("backup_id", "TEXT", 1, None, 1), ("profile_id", "TEXT", 1, None, 0), ("destination_id", "TEXT", 1, None, 2), ("backup_class", "TEXT", 1, None, 3), ("remote_key", "TEXT", 1, None, 0), ("local_sha256", "TEXT", 1, None, 0), ("local_verified", "INTEGER", 1, None, 0), ("upload_state", "TEXT", 1, None, 0), ("remote_verified", "INTEGER", 1, None, 0), ("comparison_state", "TEXT", 1, None, 0), ("prune_state", "TEXT", 1, None, 0), ("updated_at", "TEXT", 1, None, 0), ("error_code", "TEXT", 0, None, 0)],
    "notification_rules": [("profile_id", "TEXT", 1, None, 1), ("event", "TEXT", 1, None, 2), ("enabled", "INTEGER", 1, None, 0)],
    "notification_deliveries": [("id", "TEXT", 0, None, 1), ("profile_id", "TEXT", 1, None, 0), ("event", "TEXT", 1, None, 0), ("state_generation", "INTEGER", 1, None, 0), ("channel", "TEXT", 1, None, 0), ("delivered_at", "TEXT", 0, None, 0), ("error_code", "TEXT", 0, None, 0)],
    "updates": [("id", "TEXT", 0, None, 1), ("profile_id", "TEXT", 1, None, 0), ("created_at", "TEXT", 1, None, 0), ("strategy", "TEXT", 1, None, 0), ("prior_version", "TEXT", 0, None, 0), ("new_version", "TEXT", 0, None, 0), ("state", "TEXT", 1, None, 0)],
    "rpc_idempotency": [("request_id", "TEXT", 0, None, 1), ("canonical_request", "TEXT", 1, None, 0), ("response", "TEXT", 0, None, 0), ("status", "TEXT", 1, "'completed'", 0), ("created_at", "TEXT", 1, None, 0)],
    "player_sessions": [("id", "TEXT", 0, None, 1), ("profile_id", "TEXT", 1, None, 0), ("player", "TEXT", 1, None, 0), ("started_at", "TEXT", 1, None, 0), ("ended_at", "TEXT", 0, None, 0), ("source", "TEXT", 1, None, 0)],
    "metric_samples": [("profile_id", "TEXT", 1, None, 0), ("metric", "TEXT", 1, None, 0), ("ts", "TEXT", 1, None, 0), ("value", "REAL", 1, None, 0)],
    "benchmark_runs": [("id", "TEXT", 0, None, 1), ("profile_id", "TEXT", 1, None, 0), ("baseline_preset", "TEXT", 1, None, 0), ("candidate_preset", "TEXT", 1, None, 0), ("state", "TEXT", 1, None, 0), ("created_at", "TEXT", 1, None, 0), ("finished_at", "TEXT", 0, None, 0), ("overall_verdict", "TEXT", 0, None, 0), ("summary_json", "TEXT", 0, None, 0), ("artifact_path", "TEXT", 0, None, 0), ("artifact_sha256", "TEXT", 0, None, 0), ("error_code", "TEXT", 0, None, 0), ("provenance_json", "TEXT", 0, None, 0), ("planned_pairs", "INTEGER", 1, "0", 0), ("completed_pairs", "INTEGER", 1, "0", 0), ("primary_endpoints_json", "TEXT", 0, None, 0), ("thresholds_json", "TEXT", 0, None, 0), ("driver_verdict", "TEXT", 0, None, 0), ("failure_category", "TEXT", 1, "'none'", 0), ("stage", "TEXT", 1, "'prepared'", 0), ("progress", "INTEGER", 1, "0", 0)],
    "backup_payload_retirement": [("operation_id", "TEXT", 1, None, 1), ("backup_id", "TEXT", 1, None, 2), ("profile_id", "TEXT", 1, None, 0), ("state", "TEXT", 1, None, 0), ("path", "TEXT", 1, None, 0), ("quarantine_path", "TEXT", 0, None, 0), ("expected_device", "INTEGER", 1, None, 0), ("expected_inode", "INTEGER", 1, None, 0), ("expected_size", "INTEGER", 1, None, 0), ("expected_mtime_ns", "INTEGER", 1, None, 0), ("expected_ctime_ns", "INTEGER", 1, None, 0), ("expected_sha256", "TEXT", 1, None, 0), ("manifest_sha256", "TEXT", 1, None, 0), ("created_at", "TEXT", 1, None, 0), ("updated_at", "TEXT", 1, None, 0), ("error_code", "TEXT", 0, None, 0)],
}
EXPECTED_INDEXES = {
    "idx_events_history_cursor": ("events", ("timestamp", "id")),
    "idx_audit_history_cursor": ("audit", ("timestamp", "id")),
    "idx_player_sessions_profile": ("player_sessions", ("profile_id", "started_at")),
    "idx_metric_samples": ("metric_samples", ("profile_id", "metric", "ts")),
    "idx_backup_protections_scope": ("backup_protections", ("profile_id", "destination_id", "backup_class", "remote_verified", "comparison_state")),
    "idx_benchmark_runs_profile": ("benchmark_runs", ("profile_id", "created_at")),
    "idx_backup_payload_retirement_backup": ("backup_payload_retirement", ("backup_id", "state")),
}
EXPECTED_TRIGGERS = {
    "events_append_only_update",
    "events_append_only_delete",
    "audit_append_only_update",
    "audit_append_only_delete",
}

EXPECTED_INDEX_SQL = {
    "idx_events_history_cursor": "CREATE INDEX idx_events_history_cursor ON events(timestamp DESC, id DESC)",
    "idx_audit_history_cursor": "CREATE INDEX idx_audit_history_cursor ON audit(timestamp DESC, id DESC)",
    "idx_player_sessions_profile": "CREATE INDEX idx_player_sessions_profile ON player_sessions(profile_id, started_at)",
    "idx_metric_samples": "CREATE INDEX idx_metric_samples ON metric_samples(profile_id, metric, ts)",
    "idx_backup_protections_scope": "CREATE INDEX idx_backup_protections_scope ON backup_protections(profile_id, destination_id, backup_class, remote_verified, comparison_state)",
    "idx_benchmark_runs_profile": "CREATE INDEX idx_benchmark_runs_profile ON benchmark_runs(profile_id, created_at DESC)",
    "idx_backup_payload_retirement_backup": "CREATE INDEX idx_backup_payload_retirement_backup ON backup_payload_retirement(backup_id, state)",
}
EXPECTED_TRIGGER_SQL = {
    "events_append_only_update": "CREATE TRIGGER events_append_only_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT, 'events are append-only'); END",
    "events_append_only_delete": "CREATE TRIGGER events_append_only_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'events are append-only'); END",
    "audit_append_only_update": "CREATE TRIGGER audit_append_only_update BEFORE UPDATE ON audit BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END",
    "audit_append_only_delete": "CREATE TRIGGER audit_append_only_delete BEFORE DELETE ON audit BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END",
}
EXPECTED_FOREIGN_KEYS = {
    "backup_protections": [("backups", "backup_id", "id", "NO ACTION", "RESTRICT", "NONE")],
    "backup_payload_retirement": [("backups", "backup_id", "id", "NO ACTION", "RESTRICT", "NONE")],
}

# The deployed pre-B2 database is one exact schema, not a collection of
# independently tolerated migrations.  In particular, the live ALTER TABLE
# added jobs.completion_seq after detail, while a fresh/latest database has it
# before detail.  Keep that distinction explicit so mixed or guessed schemas
# fail closed.
LEGACY_TABLE_SQL = dict(EXPECTED_TABLE_SQL)
LEGACY_TABLE_SQL["backups"] = """CREATE TABLE backups (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1),
        size_bytes INTEGER NOT NULL,
        verified INTEGER NOT NULL,
        protected INTEGER NOT NULL
    )"""
LEGACY_TABLE_SQL["jobs"] = """CREATE TABLE jobs (
        id TEXT PRIMARY KEY,
        profile_id TEXT,
        operation TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL CHECK (is_rfc3339_timestamp(created_at) = 1),
        finished_at TEXT CHECK (finished_at IS NULL OR is_rfc3339_timestamp(finished_at) = 1),
        detail TEXT,
        completion_seq INTEGER
    )"""
LEGACY_COLUMNS = dict(EXPECTED_COLUMNS)
LEGACY_COLUMNS["jobs"] = (
    ("id", "TEXT", 0, None, 1),
    ("profile_id", "TEXT", 0, None, 0),
    ("operation", "TEXT", 1, None, 0),
    ("state", "TEXT", 1, None, 0),
    ("created_at", "TEXT", 1, None, 0),
    ("finished_at", "TEXT", 0, None, 0),
    ("detail", "TEXT", 0, None, 0),
    ("completion_seq", "INTEGER", 0, None, 0),
)
# Horizon schema v3 benchmark rows predate the durable provenance and
# lifecycle columns.  They are accepted as a source only and are padded with
# canonical defaults while creating the v4 target.
LEGACY_BENCHMARK_TABLE_SQL = """CREATE TABLE benchmark_runs (
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
        error_code TEXT
    )"""
# The v3 benchmark table predates artifact_sha256 but still retains the
# trailing error_code column.  Selecting the first ten canonical columns and
# then error_code models that exact live schema instead of accidentally
# expecting the post-v4 column in the legacy source.
LEGACY_BENCHMARK_COLUMNS = EXPECTED_COLUMNS["benchmark_runs"][:10] + EXPECTED_COLUMNS["benchmark_runs"][11:12]
# The pre-B2 schemas predate the retirement ledger, so it is excluded from the
# legacy fingerprints.  A legacy source therefore still matches, and a source
# that already carries the ledger is treated as the current schema instead.
PRE_BENCHMARK_SOURCE_TABLES = frozenset(EXPECTED_TABLE_SQL) - {"benchmark_runs", LEDGER_TABLE}
PRE_BENCHMARK_SOURCE_INDEXES = frozenset(EXPECTED_INDEXES) - {"idx_benchmark_runs_profile", "idx_backup_payload_retirement_backup"}
LEGACY_SOURCE_TABLES = PRE_BENCHMARK_SOURCE_TABLES - {"backup_protections"}
LEGACY_SOURCE_INDEXES = PRE_BENCHMARK_SOURCE_INDEXES - {"idx_backup_protections_scope"}
# Clean schema-4 databases written before the retirement ledger existed carry
# the current ``benchmark_runs`` table but no ledger, so the ledger must be
# excluded from the pre-ledger fingerprints.  ``LEGACY_BENCHMARK_TABLES`` is
# the same table set with the older schema-3 ``benchmark_runs`` shape.
SCHEMA4_PRE_LEDGER_TABLES = frozenset(EXPECTED_TABLE_SQL) - {LEDGER_TABLE}
LEGACY_BENCHMARK_TABLES = SCHEMA4_PRE_LEDGER_TABLES


def _sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            count += len(chunk)
    return digest.hexdigest(), count


def _reject_symlink_components(path: Path) -> None:
    current = Path(path.anchor) if path.is_absolute() else Path(".")
    for part in path.parts[1:] if path.is_absolute() else path.parts:
        current /= part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise MigrationError(f"symlink path component is not allowed: {current}")


def _check_directory(path: Path) -> None:
    _reject_symlink_components(path)
    try:
        info = os.lstat(path)
    except FileNotFoundError as exc:
        raise MigrationError(f"artifact directory does not exist: {path}") from exc
    if not stat.S_ISDIR(info.st_mode):
        raise MigrationError(f"artifact parent is not a directory: {path}")
    if info.st_uid != os.geteuid() or info.st_gid != os.getegid() or info.st_mode & 0o077:
        raise MigrationError(f"insecure artifact directory ownership/mode: {path}")


def _check_file(path: Path, *, allow_missing: bool, reject_nonempty: bool = False) -> os.stat_result | None:
    _reject_symlink_components(path.parent)
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        if allow_missing:
            return None
        raise MigrationError(f"database does not exist: {path}")
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise MigrationError(f"database must be a regular non-symlink file: {path}")
    if info.st_nlink != 1:
        raise MigrationError(f"hard-linked database is not allowed: {path}")
    if info.st_uid != os.geteuid() or info.st_gid != os.getegid() or info.st_mode & 0o077:
        raise MigrationError(f"insecure database ownership/mode: {path}")
    if reject_nonempty and info.st_size:
        raise MigrationError(f"refusing to overwrite non-empty artifact: {path}")
    return info


def _check_sidecars(path: Path, *, source: bool) -> None:
    wal = Path(f"{path}-wal")
    shm = Path(f"{path}-shm")
    wal_info = _check_file(wal, allow_missing=True)
    shm_info = _check_file(shm, allow_missing=True)
    if source:
        if shm_info is not None and wal_info is None:
            raise MigrationError("SQLite SHM exists without WAL; refusing unsafe source")
    elif wal_info is not None or shm_info is not None:
        raise MigrationError("target sidecar exists; refusing unsafe atomic replacement")


def _writer_processes() -> list[int]:
    found: list[int] = []
    try:
        entries = list(os.scandir("/proc"))
    except OSError as exc:
        raise MigrationError("cannot inspect /proc for controller writers") from exc
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            raw = Path(entry.path, "cmdline").read_bytes()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise MigrationError("cannot inspect controller process state") from exc
        command = raw.replace(b"\x00", b" ").decode("utf-8", "replace")
        if any(marker in command for marker in WRITER_MARKERS):
            found.append(int(entry.name))
    return found


def _assert_writers_absent() -> None:
    pids = _writer_processes()
    if pids:
        raise MigrationError("controller/writer process is present; stop both writers before migration")


def _is_rfc3339_timestamp(value: object) -> int:
    if not isinstance(value, str) or not RFC3339.fullmatch(value):
        return 0
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return 0
    return int(parsed.tzinfo is not None and parsed.utcoffset() is not None)


def _quick_check(connection: sqlite3.Connection, label: str) -> str:
    try:
        rows = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
    except sqlite3.Error as exc:
        raise MigrationError(f"{label} SQLite quick_check failed") from exc
    if rows != ["ok"]:
        raise MigrationError(f"{label} SQLite quick_check failed")
    try:
        fk_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
    except sqlite3.Error as exc:
        raise MigrationError(f"{label} foreign_key_check failed") from exc
    if fk_rows:
        raise MigrationError(f"{label} foreign_key_check failed")
    return "ok"


def _schema_snapshot(connection: sqlite3.Connection, *, target: bool = False) -> dict[str, Any]:
    rows = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
    ).fetchall()
    # Drop additive feature tables (and their indexes/triggers) so the exact
    # canonical table set is still enforced for everything else.
    objects = [
        (kind, name, sql)
        for kind, name, parent, sql in rows
        if parent not in ADDITIVE_FEATURE_TABLES
    ]
    tables = {name: sql for kind, name, sql in objects if kind == "table"}
    table_names = set(tables)
    if target:
        expected_tables = frozenset(EXPECTED_TABLE_SQL)
        expected_sql = EXPECTED_TABLE_SQL
        expected_columns = EXPECTED_COLUMNS
        expected_indexes = frozenset(EXPECTED_INDEXES)
        expected_foreign_keys = EXPECTED_FOREIGN_KEYS
        expected_version = SCHEMA_VERSION
    elif table_names == LEGACY_SOURCE_TABLES:
        expected_tables = LEGACY_SOURCE_TABLES
        expected_sql = LEGACY_TABLE_SQL
        expected_columns = LEGACY_COLUMNS
        expected_indexes = LEGACY_SOURCE_INDEXES
        expected_foreign_keys = {}
        expected_version = 2
    elif table_names == PRE_BENCHMARK_SOURCE_TABLES:
        expected_tables = PRE_BENCHMARK_SOURCE_TABLES
        expected_sql = EXPECTED_TABLE_SQL
        expected_columns = EXPECTED_COLUMNS
        expected_indexes = PRE_BENCHMARK_SOURCE_INDEXES
        expected_foreign_keys = EXPECTED_FOREIGN_KEYS
        expected_version = 2
    elif table_names == LEGACY_BENCHMARK_TABLES and _normal_sql(tables.get("benchmark_runs")) == _normal_sql(LEGACY_BENCHMARK_TABLE_SQL):
        expected_tables = LEGACY_BENCHMARK_TABLES
        expected_sql = dict(EXPECTED_TABLE_SQL)
        expected_sql["benchmark_runs"] = LEGACY_BENCHMARK_TABLE_SQL
        expected_columns = dict(EXPECTED_COLUMNS)
        expected_columns["benchmark_runs"] = LEGACY_BENCHMARK_COLUMNS
        expected_indexes = frozenset(EXPECTED_INDEXES) - {"idx_backup_payload_retirement_backup"}
        expected_foreign_keys = {
            table: keys for table, keys in EXPECTED_FOREIGN_KEYS.items() if table != LEDGER_TABLE
        }
        expected_version = 3
    elif table_names == SCHEMA4_PRE_LEDGER_TABLES:
        # Clean schema-4 database written before the retirement ledger existed.
        # It is accepted exactly (no unknown tables tolerated) and the target
        # gains the canonical, empty safety ledger.
        expected_tables = SCHEMA4_PRE_LEDGER_TABLES
        expected_sql = EXPECTED_TABLE_SQL
        expected_columns = EXPECTED_COLUMNS
        expected_indexes = frozenset(EXPECTED_INDEXES) - {"idx_backup_payload_retirement_backup"}
        expected_foreign_keys = {
            table: keys for table, keys in EXPECTED_FOREIGN_KEYS.items() if table != LEDGER_TABLE
        }
        expected_version = SCHEMA_VERSION
    elif table_names == frozenset(EXPECTED_TABLE_SQL):
        expected_tables = frozenset(EXPECTED_TABLE_SQL)
        expected_sql = EXPECTED_TABLE_SQL
        expected_columns = EXPECTED_COLUMNS
        expected_indexes = frozenset(EXPECTED_INDEXES)
        expected_foreign_keys = EXPECTED_FOREIGN_KEYS
        expected_version = SCHEMA_VERSION
    else:
        raise MigrationError("unsupported SQLite table set")
    if frozenset(table_names) != expected_tables:
        raise MigrationError("unsupported SQLite table set")
    if {kind for kind, _, _ in objects} - {"table", "index", "trigger"}:
        raise MigrationError("unsupported SQLite object type")
    for table, sql in tables.items():
        if _normal_sql(sql) != _normal_sql(expected_sql[table]):
            raise MigrationError(f"unsupported schema SQL for table {table}")
        columns = [
            (name, typ, notnull, default, pk)
            for _, name, typ, notnull, default, pk in connection.execute(f'PRAGMA table_info("{table}")')
        ]
        if tuple(columns) != tuple(expected_columns[table]):
            raise MigrationError(f"unsupported columns for table {table}")
        foreign_keys = [
            (foreign_table, source_column, target_column, on_update, on_delete, match)
            for _, _, foreign_table, source_column, target_column, on_update, on_delete, match
            in connection.execute(f'PRAGMA foreign_key_list("{table}")')
        ]
        if foreign_keys != expected_foreign_keys.get(table, []):
            raise MigrationError(f"unsupported foreign keys for table {table}")
    indexes = {}
    for kind, name, sql in objects:
        if kind != "index" or name.startswith("sqlite_autoindex"):
            continue
        columns = tuple(row[2] for row in connection.execute(f'PRAGMA index_info("{name}")'))
        indexes[name] = (sql, columns)
    index_names = set(indexes)
    if index_names != set(expected_indexes):
        raise MigrationError("unsupported SQLite index set")
    for name in index_names:
        _table, columns = EXPECTED_INDEXES[name]
        if indexes[name][1] != columns:
            raise MigrationError(f"unsupported index shape: {name}")
        if _normal_sql(indexes[name][0]) != _normal_sql(EXPECTED_INDEX_SQL[name]):
            raise MigrationError(f"unsupported index SQL: {name}")
    triggers = {name: sql for kind, name, sql in objects if kind == "trigger"}
    if set(triggers) != EXPECTED_TRIGGERS:
        raise MigrationError("unsupported SQLite trigger set")
    for name, sql in triggers.items():
        if _normal_sql(sql) != _normal_sql(EXPECTED_TRIGGER_SQL[name]):
            raise MigrationError(f"unsupported trigger SQL: {name}")
    version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if version != expected_version:
        raise MigrationError(f"unsupported SQLite user_version: {version}")
    return {
        "user_version": version,
        "tables": {
            table: {
                "sql": _normal_sql(sql),
                "columns": expected_columns[table],
            }
            for table, sql in sorted(tables.items())
        },
        "indexes": {
            name: {"sql": _normal_sql(indexes[name][0]), "columns": indexes[name][1]}
            for name in sorted(indexes)
        },
        "triggers": {name: _normal_sql(triggers[name]) for name in sorted(triggers)},
    }


def _schema_fingerprint(snapshot: dict[str, Any]) -> str:
    payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _open_locked(path: Path) -> sqlite3.Connection:
    try:
        connection = sqlite3.connect(path, timeout=0, isolation_level=None)
        connection.execute("PRAGMA foreign_keys = ON")
        connection.create_function("is_rfc3339_timestamp", 1, _is_rfc3339_timestamp, deterministic=True)
        connection.execute("BEGIN EXCLUSIVE")
    except sqlite3.Error as exc:
        try:
            connection.close()  # type: ignore[has-type]
        except (NameError, sqlite3.Error):
            pass
        raise MigrationError(f"could not acquire immediate/exclusive SQLite lock: {path}") from exc
    return connection


def _row_dicts(connection: sqlite3.Connection, table: str, columns: list[str]) -> Iterable[tuple[Any, ...]]:
    return connection.execute(f'SELECT {", ".join(_ident(c) for c in columns)} FROM {_ident(table)}')


def _ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _profile_column(columns: list[str]) -> int | None:
    try:
        return columns.index("profile_id")
    except ValueError:
        return None


def _classify(table: str, columns: list[str], row: tuple[Any, ...]) -> tuple[bool, str | None, list[str]]:
    reasons: list[str] = []
    profile_index = _profile_column(columns)
    profile = row[profile_index] if profile_index is not None else None
    if table == "confirmations":
        reasons.append("transient_confirmation")
    elif table == "rpc_idempotency":
        reasons.append("transient_idempotency")
    if table == "jobs" and row[columns.index("finished_at")] is None:
        reasons.append("unfinished_job")
    if table == "benchmark_runs" and row[columns.index("finished_at")] is None:
        reasons.append("unfinished_benchmark")
    if table == "player_sessions" and row[columns.index("ended_at")] is None:
        reasons.append("active_session")
    if profile is not None and profile not in RETAINED_PROFILES:
        reasons.append("legacy_profile" if profile in LEGACY_PROFILES else "unknown_profile")
    return not reasons, reasons[0] if reasons else None, reasons


def _safe_evidence_value(column: str, value: Any) -> Any:
    if SENSITIVE_COLUMNS.search(column) or column in SENSITIVE_TEXT_COLUMNS:
        return None
    if isinstance(value, bytes):
        return {"blob_sha256": hashlib.sha256(value).hexdigest(), "bytes": len(value)}
    return value


def _evidence_row(table: str, columns: list[str], row: tuple[Any, ...], reasons: list[str]) -> dict[str, Any]:
    safe = {}
    redacted_columns = []
    for column, value in zip(columns, row):
        safe_value = _safe_evidence_value(column, value)
        if safe_value is None:
            redacted_columns.append(column)
        else:
            safe[column] = safe_value
    safe_structure = {
        "table": table,
        "reasons": reasons,
        "safe_columns": safe,
        "redacted_columns": redacted_columns,
    }
    row_json = json.dumps(safe_structure, sort_keys=True, separators=(",", ":")).encode()
    return {
        "table": table,
        "reasons": reasons,
        "safe_columns": safe,
        "redacted_columns": redacted_columns,
        "row_sha256": hashlib.sha256(row_json).hexdigest(),
    }


def _write_atomic_json(path: Path, payload: dict[str, Any], *, mode: int) -> tuple[str, int]:
    _check_directory(path.parent)
    _check_file(path, allow_missing=True, reject_nonempty=True)
    data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, mode)
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write while creating JSON artifact")
            view = view[written:]
        os.fsync(fd)
        os.close(fd)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        os.chmod(path, mode)
        return hashlib.sha256(data).hexdigest(), len(data)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _write_readonly_evidence(path: Path, payload: dict[str, Any]) -> tuple[str, int]:
    return _write_atomic_json(path, payload, mode=0o400)


def _row_key(row: tuple[Any, ...]) -> bytes:
    return json.dumps([repr(value) for value in row], separators=(",", ":")).encode()


def _logical_json_default(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__bytes_hex__": value.hex()}
    raise TypeError(f"unsupported SQLite value for logical evidence: {type(value).__name__}")


def _logical_source_sha256(snapshot: dict[str, Any], rows: dict[str, list[list[Any]]]) -> str:
    payload = json.dumps(
        {"schema": snapshot, "rows": rows},
        sort_keys=True,
        separators=(",", ":"),
        default=_logical_json_default,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _stable_artifact_hash(path: Path, info: os.stat_result | None, label: str) -> tuple[str | None, int | None]:
    if info is None:
        return None, None
    digest, size = _sha256(path)
    current = os.stat(path)
    if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
    ):
        raise MigrationError(f"{label} changed while source was locked")
    return digest, size


def _target_is_not_newer(target: sqlite3.Connection, source: sqlite3.Connection, tables: list[str], columns_by_table: dict[str, list[str]], source_columns_by_table: dict[str, list[str]] | None = None) -> None:
    for table in tables:
        if source.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is None:
            if target.execute(f'SELECT COUNT(*) FROM {_ident(table)}').fetchone()[0]:
                raise MigrationError("existing generated target contains rows absent from source")
            continue
        columns = (source_columns_by_table or {}).get(table, columns_by_table[table])
        compare_columns = tuple(column for column in columns if not (table == "jobs" and column == "completion_seq"))
        source_rows = Counter(_row_key(tuple(row)) for row in _row_dicts(source, table, compare_columns))
        target_rows = Counter(_row_key(tuple(row)) for row in _row_dicts(target, table, compare_columns))
        if target_rows - source_rows:
            raise MigrationError("existing generated target contains rows absent from source")


def _validate_existing_target(path: Path, source: sqlite3.Connection, snapshot: dict[str, Any], tables: list[str], columns_by_table: dict[str, list[str]], source_columns_by_table: dict[str, list[str]] | None = None) -> sqlite3.Connection:
    _check_file(path, allow_missing=False)
    _check_sidecars(path, source=False)
    target = _open_locked(path)
    try:
        if int(target.execute("PRAGMA application_id").fetchone()[0]) != APP_ID:
            raise MigrationError("non-empty target was not generated by this tool")
        _quick_check(target, "existing target")
        _schema_snapshot(target, target=True)
        _target_is_not_newer(target, source, tables, columns_by_table, source_columns_by_table)
    except Exception:
        target.rollback()
        target.close()
        raise
    return target


def _create_target(
    path: Path,
    source: sqlite3.Connection,
    snapshot: dict[str, Any],
    tables: list[str],
    columns_by_table: dict[str, list[str]],
    included_rows: dict[str, list[tuple[Any, ...]]],
) -> None:
    _check_directory(path.parent)
    _check_file(path, allow_missing=True)
    _check_sidecars(path, source=False)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=path.parent)
    temporary = Path(temporary_name)
    connection: sqlite3.Connection | None = None
    try:
        os.fchmod(fd, 0o600)
        os.close(fd)
        connection = sqlite3.connect(temporary, isolation_level=None)
        connection.create_function("is_rfc3339_timestamp", 1, _is_rfc3339_timestamp, deterministic=True)
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("BEGIN")
        connection.execute(f"PRAGMA application_id = {APP_ID}")
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        table_order = [
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
            # Safety ledger: created last and always preserved row-for-row.
            LEDGER_TABLE,
        ]
        for table in table_order:
            connection.execute(EXPECTED_TABLE_SQL[table])
        for table in tables:
            columns = columns_by_table[table]
            if not included_rows[table]:
                continue
            placeholders = ",".join("?" for _ in columns)
            sql = f"INSERT INTO {_ident(table)} ({', '.join(_ident(c) for c in columns)}) VALUES ({placeholders})"
            connection.executemany(sql, included_rows[table])
        for index_name in sorted(EXPECTED_INDEXES):
            connection.execute(EXPECTED_INDEX_SQL[index_name])
        for trigger_name in sorted(EXPECTED_TRIGGERS):
            connection.execute(EXPECTED_TRIGGER_SQL[trigger_name])
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("COMMIT")
        _quick_check(connection, "new target")
        connection.close()
        connection = None
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        os.chmod(path, 0o600)
    except Exception:
        if connection is not None:
            try:
                connection.rollback()
                connection.close()
            except sqlite3.Error:
                pass
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def migrate(source_path: str | os.PathLike[str], target_path: str | os.PathLike[str], report_path: str | os.PathLike[str], retired_export_path: str | os.PathLike[str], *, replace_empty_generated_target: bool = False) -> dict[str, Any]:
    source = Path(source_path)
    target = Path(target_path)
    report = Path(report_path)
    retired = Path(retired_export_path)
    paths = [source, target, report, retired]
    if any(not path.is_absolute() for path in paths):
        raise MigrationError("source, target, report, and retired-export paths must be absolute")
    if len({str(path) for path in paths}) != len(paths):
        raise MigrationError("source, target, report, and retired-export paths must be distinct")
    _check_directory(source.parent)
    _check_directory(target.parent)
    _check_directory(report.parent)
    _check_directory(retired.parent)
    source_info = _check_file(source, allow_missing=False)
    target_info = _check_file(target, allow_missing=True)
    if target_info is not None and (source_info.st_dev, source_info.st_ino) == (target_info.st_dev, target_info.st_ino):
        raise MigrationError("source and target are the same inode")
    _check_sidecars(source, source=True)
    _check_sidecars(target, source=False)
    if target_info is not None and target_info.st_size and not replace_empty_generated_target:
        raise MigrationError("refusing to overwrite non-empty target without fixed safety switch")

    _assert_writers_absent()
    source_connection = _open_locked(source)
    existing_target: sqlite3.Connection | None = None
    try:
        database_filename = source_connection.execute("PRAGMA database_list").fetchone()[2]
        locked_info = os.stat(database_filename)
        if (locked_info.st_dev, locked_info.st_ino) != (source_info.st_dev, source_info.st_ino):
            raise MigrationError("source changed while acquiring its exclusive lock")
        source_hash, source_bytes = _stable_artifact_hash(source, source_info, "source database")
        assert source_hash is not None and source_bytes is not None
        journal_mode = str(source_connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        wal_path = Path(f"{source}-wal")
        shm_path = Path(f"{source}-shm")
        wal_info = _check_file(wal_path, allow_missing=True)
        shm_info = _check_file(shm_path, allow_missing=True)
        if journal_mode == "wal" and ((wal_info is None) != (shm_info is None)):
            raise MigrationError("WAL source is missing exactly one WAL/SHM sidecar")
        if journal_mode != "wal" and (wal_info is not None or shm_info is not None):
            raise MigrationError("source has WAL/SHM sidecars but is not in WAL mode")
        source_wal_hash, source_wal_bytes = _stable_artifact_hash(wal_path, wal_info, "source WAL")
        source_shm_hash, source_shm_bytes = _stable_artifact_hash(shm_path, shm_info, "source SHM")
        quick = _quick_check(source_connection, "source")
        snapshot = _schema_snapshot(source_connection)
        fingerprint = _schema_fingerprint(snapshot)
        tables = sorted(EXPECTED_TABLE_SQL)
        target_columns_by_table = {
            table: [column[0] for column in EXPECTED_COLUMNS[table]] for table in tables
        }
        source_columns_by_table = {
            table: [column[0] for column in snapshot["tables"][table]["columns"]]
            for table in snapshot["tables"]
        }
        included_rows: dict[str, list[tuple[Any, ...]]] = {table: [] for table in tables}
        included_counts: dict[str, int] = {table: 0 for table in tables}
        excluded_counts: dict[str, Counter[str]] = {table: Counter() for table in tables}
        evidence_rows: list[dict[str, Any]] = []
        logical_rows: dict[str, list[list[Any]]] = {}
        source_tables = set(snapshot["tables"])
        for table in tables:
            if table not in source_tables:
                continue
            columns = source_columns_by_table.get(table, target_columns_by_table[table])
            table_rows: list[list[Any]] = []
            for raw_row in _row_dicts(source_connection, table, columns):
                row = tuple(raw_row)
                table_rows.append(list(row))
                include, reason, reasons = _classify(table, columns, row)
                if include:
                    source_values = dict(zip(columns, row))
                    row = tuple(source_values.get(column) for column in target_columns_by_table[table])
                    if table == "benchmark_runs" and len(columns) == 11:
                        # Fields introduced by schema v4 are intentionally
                        # conservative: an old completed row is retained as
                        # history, while no running state is fabricated.
                        row = row[:10] + (None, source_values.get("error_code"), None, 0, 0, None, None, None, "none", "prepared", 0)
                    included_rows[table].append(row)
                    included_counts[table] += 1
                else:
                    excluded_counts[table][reason or "excluded"] += 1
                    if "legacy_profile" in reasons or "unknown_profile" in reasons:
                        evidence_rows.append(_evidence_row(table, columns, row, reasons))
            logical_rows[table] = table_rows
        # Canonical v4 completion sequence is an absolute, deterministic
        # ordering of retained completed jobs. Reassign it offline so legacy
        # NULLs, timezone offsets, and conflicting source values cannot change
        # resume-last-active behavior. Unfinished jobs remain NULL.
        jobs = [list(row) for row in included_rows.get("jobs", [])]
        included_rows["jobs"] = jobs
        if jobs:
            job_columns = target_columns_by_table["jobs"]
            state_index = job_columns.index("state")
            finished_index = job_columns.index("finished_at")
            id_index = job_columns.index("id")
            seq_index = job_columns.index("completion_seq")
            completed = [row for row in jobs if row[state_index] == "succeeded" and row[finished_index]]
            def _job_order(row):
                value = str(row[finished_index]).replace("Z", "+00:00")
                try:
                    instant = datetime.fromisoformat(value).astimezone(timezone.utc)
                except ValueError:
                    instant = datetime.min.replace(tzinfo=timezone.utc)
                return instant, str(row[id_index])
            for sequence, row in enumerate(sorted(completed, key=_job_order), 1):
                row[seq_index] = sequence
            for row in jobs:
                if row not in completed:
                    row[seq_index] = None
        logical_source_hash = _logical_source_sha256(snapshot, logical_rows)

        if target_info is not None and target_info.st_size and replace_empty_generated_target:
            existing_target = _validate_existing_target(target, source_connection, snapshot, tables, target_columns_by_table, source_columns_by_table)
            existing_target.rollback()
            existing_target.close()
            existing_target = None
        _create_target(target, source_connection, snapshot, tables, target_columns_by_table, included_rows)
        target_hash, target_bytes = _sha256(target)
    finally:
        if existing_target is not None:
            existing_target.rollback()
            existing_target.close()
        source_connection.rollback()
        source_connection.close()

    evidence_payload = {
        "format": "horizon-retired-profile-evidence-v1",
        "profiles": sorted(LEGACY_PROFILES),
        "rows": evidence_rows,
        "row_count": len(evidence_rows),
        "secret_policy": "credential-bearing columns and free-text fields omitted",
    }
    retired_hash, retired_bytes = _write_readonly_evidence(retired, evidence_payload)
    report_payload = {
        "format": "horizon-state-migration-report-v1",
        "source_sha256": source_hash,
        "source_bytes": source_bytes,
        "source_wal_sha256": source_wal_hash,
        "source_wal_bytes": source_wal_bytes,
        "source_shm_sha256": source_shm_hash,
        "source_shm_bytes": source_shm_bytes,
        "source_logical_sha256": logical_source_hash,
        "target_sha256": target_hash,
        "target_bytes": target_bytes,
        "retired_export_sha256": retired_hash,
        "retired_export_bytes": retired_bytes,
        "schema_version": SCHEMA_VERSION,
        "schema_fingerprint": fingerprint,
        "quick_check": {"source": quick, "target": "ok"},
        "included_counts_by_table": included_counts,
        "excluded_counts_by_table_reason": {
            table: dict(sorted(counts.items())) for table, counts in excluded_counts.items()
        },
        "retained_profiles": sorted(RETAINED_PROFILES),
        "atomic_target": True,
        "writer_gate": "passed",
    }
    _write_atomic_json(report, report_payload, mode=0o600)
    print(json.dumps(report_payload, indent=2, sort_keys=True))
    return report_payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="absolute source state.db path")
    parser.add_argument("--target", required=True, help="absolute new state.db path")
    parser.add_argument("--report", required=True, help="absolute JSON report path")
    parser.add_argument("--retired-export", required=True, help="absolute legacy evidence JSON path")
    parser.add_argument(
        "--replace-empty-generated-target",
        action="store_true",
        help="permit replacing a non-empty target only when this tool marker, exact schema, and source-subset checks pass",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        migrate(
            args.source,
            args.target,
            args.report,
            args.retired_export,
            replace_empty_generated_target=args.replace_empty_generated_target,
        )
    except (MigrationError, OSError, sqlite3.Error) as exc:
        print(f"migration refused: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
