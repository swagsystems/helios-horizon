"""Experimental startup-duration estimates learned from genuine starts.

The controller owns a small, bounded history of *validated successful* start
durations keyed by profile and installed version.  The browser only renders the
projection: it never trains the model, and it never sees logs or identities.
History lives in the controller's configured state-database connection as an
additive table (see ``state_db.ensure_additive_state_tables``); a missing or
broken history degrades to "no estimate" and never touches lifecycle
authority.
"""

from __future__ import annotations

import logging
import math
import os
import stat
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from .adapters.crafty import parse_version_text

_LOG = logging.getLogger(__name__)

# Five genuine starts are required before the projection reports a median.
MIN_SAMPLES = 5
# Bounded per (profile, version) bucket; older samples are dropped on insert.
MAX_SAMPLES = 25
# Old-version buckets stay bounded per profile so a profile cannot accumulate
# unbounded history across installs.
MAX_VERSION_BUCKETS = 4
# A real attempt beyond this bound is not a plausible startup and is rejected
# rather than clamped.
MAX_DURATION_MS = 900_000.0
MAX_PROFILE_KEY_LENGTH = 128
MAX_VERSION_LENGTH = 64
MAX_RUN_KEY_LENGTH = 64
MAX_VERSION_FILE_BYTES = 64 * 1024

STARTUP_ESTIMATE_DDL = (
    """
    CREATE TABLE IF NOT EXISTS startup_estimate_samples (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        profile_id TEXT NOT NULL,
        version TEXT NOT NULL,
        duration_ms REAL NOT NULL CHECK (duration_ms > 0),
        finished_at TEXT NOT NULL,
        run_key TEXT,
        UNIQUE (profile_id, run_key)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_startup_estimate_bucket
        ON startup_estimate_samples(profile_id, version, id)
    """,
)


@dataclass(frozen=True, slots=True)
class StartupEstimateSummary:
    """Bounded projection of one profile/version learning bucket."""

    sample_count: int = 0
    median_seconds: float | None = None


@dataclass(slots=True)
class StartupAttempt:
    """Opaque identity and monotonic start of one genuine start attempt."""

    attempt_id: str
    started: float
    version: str | None = None

    def elapsed_seconds(self, now: float | None = None) -> float:
        current = time.monotonic() if now is None else now
        return max(0.0, current - self.started)


def median_seconds(values: Iterable[float]) -> float:
    """Return the arithmetic median of a non-empty bounded sample set."""

    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise ValueError("median requires at least one sample")
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def normalize_profile_key(profile_id: Any) -> str | None:
    value = getattr(profile_id, "value", profile_id)
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate or len(candidate) > MAX_PROFILE_KEY_LENGTH:
        return None
    return candidate


def normalize_version(version: Any) -> str | None:
    """Return a bounded version bucket key, or ``None`` when unknown."""

    if not isinstance(version, str):
        return None
    candidate = version.strip()
    if not candidate or len(candidate) > MAX_VERSION_LENGTH:
        return None
    return candidate


def validated_duration_ms(value: Any) -> float | None:
    """Return a plausible duration in milliseconds, or ``None``.

    Booleans, non-numbers, non-finite values, zero, negatives, and implausible
    oversized durations are rejected outright.  Nothing is clamped into a
    trainable sample.
    """

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    duration = float(value)
    if not math.isfinite(duration) or duration <= 0.0 or duration > MAX_DURATION_MS:
        return None
    return duration


def normalize_run_key(run_key: Any) -> str | None:
    if not isinstance(run_key, str):
        return None
    candidate = run_key.strip()
    if not candidate or len(candidate) > MAX_RUN_KEY_LENGTH:
        return None
    return candidate


def installed_version_for_profile(profile: Any) -> str | None:
    """Best-effort installed version hint for one profile.

    Prefers an explicit profile attribute, then the bounded profile version
    file.  Reads happen off the event loop and failures return ``None`` so an
    unknown version never reuses another version's history.
    """

    direct = getattr(profile, "installed_version", None)
    if isinstance(direct, str) and direct.strip():
        return parse_version_text(direct) or normalize_version(direct)
    paths = getattr(profile, "paths", None)
    raw_path = getattr(paths, "version_file", None) if paths is not None else None
    if raw_path is None:
        return None
    return read_bounded_version_file(raw_path)


def read_bounded_version_file(path: Any, *, max_bytes: int = MAX_VERSION_FILE_BYTES) -> str | None:
    """Read one bounded regular version file without following symlinks.

    Rejects non-regular files and oversized payloads before reading, so a
    hostile or unusual path cannot stall a start attempt.
    """

    try:
        descriptor = os.open(str(path), os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
    except (OSError, ValueError):
        return None
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size > max_bytes:
            return None
        payload = os.read(descriptor, max_bytes)
    except OSError:
        return None
    finally:
        os.close(descriptor)
    return parse_version_text(payload.decode("utf-8", errors="replace"))


class StartupEstimateStore:
    """Bounded, best-effort durable history of successful startup durations."""

    def __init__(
        self,
        connection_provider: Callable[[], Any],
        *,
        min_samples: int = MIN_SAMPLES,
        max_samples: int = MAX_SAMPLES,
        max_version_buckets: int = MAX_VERSION_BUCKETS,
        clock: Callable[[], float] = time.time,
    ):
        self._connection_provider = connection_provider
        self._min_samples = max(1, int(min_samples))
        self._max_samples = max(1, int(max_samples))
        self._max_version_buckets = max(1, int(max_version_buckets))
        self._clock = clock
        self._schema_ready = False
        self._cache: dict[tuple[str, str], StartupEstimateSummary] = {}

    def ensure_schema(self) -> bool:
        """Create the additive feature table once on the configured connection."""

        if self._schema_ready:
            return True
        try:
            connection = self._connection_provider()
            from .state_db import ensure_additive_state_tables

            ensure_additive_state_tables(connection)
        except Exception:
            _LOG.debug("startup estimate schema unavailable", exc_info=True)
            return False
        self._schema_ready = True
        return True

    def record_sample(
        self,
        profile_id: Any,
        version: Any,
        duration_ms: Any,
        *,
        run_key: Any = None,
        finished_at: str | None = None,
        managed_transaction: bool = False,
    ) -> bool:
        """Persist one validated successful start; never raises for bad data.

        ``managed_transaction`` writes inside a caller-owned transaction using a
        savepoint, so an estimate failure can never commit or roll back
        unrelated controller state.
        """

        profile_key = normalize_profile_key(profile_id)
        version_key = normalize_version(version)
        duration = validated_duration_ms(duration_ms)
        if profile_key is None or version_key is None or duration is None:
            return False
        if not self.ensure_schema():
            return False
        stamp = finished_at if isinstance(finished_at, str) and finished_at else _iso_now(self._clock)
        try:
            connection = self._connection_provider()
            if managed_transaction:
                connection.execute("SAVEPOINT startup_estimate_sample")
                try:
                    self._insert(connection, profile_key, version_key, duration, stamp, run_key)
                    self._trim_bucket(connection, profile_key, version_key)
                    self._trim_versions(connection, profile_key)
                except BaseException:
                    connection.execute("ROLLBACK TO startup_estimate_sample")
                    raise
                finally:
                    connection.execute("RELEASE startup_estimate_sample")
            else:
                with connection:
                    self._insert(connection, profile_key, version_key, duration, stamp, run_key)
                    self._trim_bucket(connection, profile_key, version_key)
                    self._trim_versions(connection, profile_key)
        except Exception:
            _LOG.debug("startup estimate sample dropped", exc_info=True)
            return False
        self._invalidate_profile(profile_key)
        return True

    def _insert(
        self,
        connection: Any,
        profile_key: str,
        version_key: str,
        duration: float,
        stamp: str,
        run_key: Any,
    ) -> None:
        connection.execute(
            "INSERT OR IGNORE INTO startup_estimate_samples"
            " (profile_id, version, duration_ms, finished_at, run_key)"
            " VALUES (?, ?, ?, ?, ?)",
            (profile_key, version_key, duration, stamp, normalize_run_key(run_key)),
        )

    def summary(self, profile_id: Any, version: Any) -> StartupEstimateSummary:
        """Read the bounded bucket projection; unavailable history is empty.

        Strictly read-only: schema creation belongs to startup and the record
        path, so an ordinary status read never executes DDL.
        """

        profile_key = normalize_profile_key(profile_id)
        version_key = normalize_version(version)
        if profile_key is None or version_key is None:
            return StartupEstimateSummary()
        key = (profile_key, version_key)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        try:
            connection = self._connection_provider()
            rows = connection.execute(
                "SELECT duration_ms FROM startup_estimate_samples"
                " WHERE profile_id = ? AND version = ? ORDER BY id DESC LIMIT ?",
                (profile_key, version_key, self._max_samples),
            ).fetchall()
        except Exception:
            _LOG.debug("startup estimate history unavailable", exc_info=True)
            return StartupEstimateSummary()
        durations = []
        for row in rows:
            duration = validated_duration_ms(row[0] if row else None)
            if duration is not None:
                durations.append(duration)
        count = len(durations)
        summary = StartupEstimateSummary(
            sample_count=count,
            median_seconds=median_seconds(durations) / 1000.0 if count >= self._min_samples else None,
        )
        if len(self._cache) > 64:
            self._cache.clear()
        self._cache[key] = summary
        return summary

    def _invalidate_profile(self, profile_key: str) -> None:
        for key in [key for key in self._cache if key[0] == profile_key]:
            del self._cache[key]

    def _trim_bucket(self, connection: Any, profile_key: str, version_key: str) -> None:
        connection.execute(
            "DELETE FROM startup_estimate_samples WHERE profile_id = ? AND version = ?"
            " AND id NOT IN (SELECT id FROM startup_estimate_samples"
            " WHERE profile_id = ? AND version = ? ORDER BY id DESC LIMIT ?)",
            (profile_key, version_key, profile_key, version_key, self._max_samples),
        )

    def _trim_versions(self, connection: Any, profile_key: str) -> None:
        connection.execute(
            "DELETE FROM startup_estimate_samples WHERE profile_id = ? AND version NOT IN ("
            " SELECT version FROM startup_estimate_samples WHERE profile_id = ?"
            " GROUP BY version ORDER BY MAX(id) DESC LIMIT ?)",
            (profile_key, profile_key, self._max_version_buckets),
        )


def _iso_now(clock: Callable[[], float]) -> str:
    return (
        datetime.fromtimestamp(float(clock()), tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
