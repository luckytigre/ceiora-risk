"""Local SQLite cache for computed analytics results."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any

from backend import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cache (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""
_SCHEMA_READY = False
_SCHEMA_READY_PATH: str | None = None
_SCHEMA_LOCK = threading.Lock()
_SNAPSHOT_POINTER_KEY = "__cache_snapshot_active"
_SNAPSHOT_KEY_PREFIX = "__snap__:"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(config.SQLITE_PATH, timeout=float(config.SQLITE_TIMEOUT_SECONDS))
    conn.execute(f"PRAGMA busy_timeout={int(config.SQLITE_BUSY_TIMEOUT_MS)}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _ensure_schema() -> None:
    global _SCHEMA_READY, _SCHEMA_READY_PATH
    db_path = str(config.SQLITE_PATH)
    if _SCHEMA_READY and _SCHEMA_READY_PATH == db_path:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY and _SCHEMA_READY_PATH == db_path:
            return
        conn = _conn()
        try:
            conn.execute(_SCHEMA)
            conn.commit()
            _SCHEMA_READY = True
            _SCHEMA_READY_PATH = db_path
        finally:
            conn.close()


def _snapshot_key(snapshot_id: str, key: str) -> str:
    return f"{_SNAPSHOT_KEY_PREFIX}{snapshot_id}:{key}"


def _snapshot_id_from_key(raw_key: str) -> str | None:
    key = str(raw_key or "")
    if not key.startswith(_SNAPSHOT_KEY_PREFIX):
        return None
    rest = key[len(_SNAPSHOT_KEY_PREFIX):]
    if ":" not in rest:
        return None
    snap_id, _ = rest.split(":", 1)
    snap_id = snap_id.strip()
    return snap_id or None


def _decode_active_snapshot(raw: Any) -> str | None:
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    if isinstance(raw, dict):
        sid = str(raw.get("snapshot_id") or "").strip()
        return sid or None
    return None


def _read_active_snapshot_id(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT value FROM cache WHERE key = ?",
        (_SNAPSHOT_POINTER_KEY,),
    ).fetchone()
    if row is None:
        return None
    try:
        payload = json.loads(row[0])
    except Exception:
        return None
    return _decode_active_snapshot(payload)


def _run_with_lock_retry(fn):
    attempts = int(config.SQLITE_CACHE_RETRY_ATTEMPTS)
    delay_s = float(config.SQLITE_CACHE_RETRY_DELAY_MS) / 1000.0
    for attempt in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if "locked" not in msg and "busy" not in msg:
                raise
            if attempt >= attempts - 1:
                raise
            time.sleep(delay_s * (attempt + 1))


def cache_get(key: str) -> Any | None:
    """Retrieve a cached value, or None if missing."""
    _ensure_schema()

    def _work() -> Any | None:
        conn = _conn()
        try:
            if key == _SNAPSHOT_POINTER_KEY:
                row = conn.execute(
                    "SELECT value FROM cache WHERE key = ?", (key,)
                ).fetchone()
                if row is None:
                    return None
                return json.loads(row[0])

            active_snapshot_id = _read_active_snapshot_id(conn)
            if active_snapshot_id:
                row = conn.execute(
                    "SELECT value FROM cache WHERE key = ?",
                    (_snapshot_key(active_snapshot_id, key),),
                ).fetchone()
                if row is not None:
                    return json.loads(row[0])

            row = conn.execute(
                "SELECT value FROM cache WHERE key = ?", (key,)
            ).fetchone()
            if row is None:
                return None
            return json.loads(row[0])
        finally:
            conn.close()

    return _run_with_lock_retry(_work)


def cache_get_live(key: str) -> Any | None:
    """Retrieve a cache value by raw key, ignoring the active snapshot pointer."""
    _ensure_schema()

    def _work() -> Any | None:
        conn = _conn()
        try:
            row = conn.execute(
                "SELECT value FROM cache WHERE key = ?",
                (key,),
            ).fetchone()
            if row is None:
                return None
            return json.loads(row[0])
        finally:
            conn.close()

    return _run_with_lock_retry(_work)


def cache_set(key: str, value: Any, *, snapshot_id: str | None = None) -> None:
    """Store a JSON-serializable value in the cache."""
    _ensure_schema()
    write_key = _snapshot_key(str(snapshot_id).strip(), key) if snapshot_id else key

    def _work() -> None:
        conn = _conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO cache (key, value, updated_at) VALUES (?, ?, ?)",
                (write_key, json.dumps(value, default=str), time.time()),
            )
            conn.commit()
        finally:
            conn.close()

    _run_with_lock_retry(_work)


def cache_publish_snapshot(snapshot_id: str) -> None:
    """Atomically publish a staged snapshot by switching active pointer."""
    clean = str(snapshot_id).strip()
    if not clean:
        raise ValueError("snapshot_id is required")
    _ensure_schema()

    def _work() -> None:
        conn = _conn()
        try:
            payload = {
                "snapshot_id": clean,
                "published_at": time.time(),
            }
            conn.execute(
                "INSERT OR REPLACE INTO cache (key, value, updated_at) VALUES (?, ?, ?)",
                (_SNAPSHOT_POINTER_KEY, json.dumps(payload, default=str), time.time()),
            )
            _prune_old_snapshots(conn, active_snapshot_id=clean)
            conn.commit()
        finally:
            conn.close()

    _run_with_lock_retry(_work)


def _prune_old_snapshots(conn: sqlite3.Connection, *, active_snapshot_id: str | None) -> int:
    retain = int(config.SQLITE_CACHE_SNAPSHOT_RETENTION)
    if retain < 1:
        retain = 1

    rows = conn.execute(
        "SELECT key, updated_at FROM cache WHERE key LIKE ?",
        (f"{_SNAPSHOT_KEY_PREFIX}%",),
    ).fetchall()
    if not rows:
        return 0

    latest_ts_by_snapshot: dict[str, float] = {}
    for key, ts in rows:
        snap_id = _snapshot_id_from_key(str(key))
        if not snap_id:
            continue
        cur = float(ts or 0.0)
        prev = latest_ts_by_snapshot.get(snap_id)
        if prev is None or cur > prev:
            latest_ts_by_snapshot[snap_id] = cur

    if not latest_ts_by_snapshot:
        return 0

    ordered = sorted(
        latest_ts_by_snapshot.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    keep = {sid for sid, _ in ordered[:retain]}
    if active_snapshot_id:
        keep.add(str(active_snapshot_id).strip())

    deleted = 0
    for sid in latest_ts_by_snapshot:
        if sid in keep:
            continue
        prefix = _snapshot_key(sid, "")
        before = conn.total_changes
        conn.execute("DELETE FROM cache WHERE key LIKE ?", (f"{prefix}%",))
        deleted += int(conn.total_changes - before)
    return deleted


def get_cache_age() -> float | None:
    """Return seconds since last cache update, or None if empty."""
    _ensure_schema()

    def _work() -> float | None:
        conn = _conn()
        try:
            row = conn.execute(
                "SELECT MAX(updated_at) AS latest FROM cache"
            ).fetchone()
            if row is None or row[0] is None:
                return None
            return time.time() - row[0]
        finally:
            conn.close()

    return _run_with_lock_retry(_work)


def clear_cache() -> None:
    """Clear all cached data."""
    _ensure_schema()

    def _work() -> None:
        conn = _conn()
        try:
            conn.execute("DELETE FROM cache")
            conn.commit()
        finally:
            conn.close()

    _run_with_lock_retry(_work)
