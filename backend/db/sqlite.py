"""Local SQLite cache for computed analytics results."""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any

import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cache (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(config.SQLITE_PATH)
    conn.execute(_SCHEMA)
    conn.commit()
    return conn


def cache_set(key: str, value: Any) -> None:
    """Store a JSON-serializable value in the cache."""
    conn = _conn()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO cache (key, value, updated_at) VALUES (?, ?, ?)",
            (key, json.dumps(value, default=str), time.time()),
        )
        conn.commit()
    finally:
        conn.close()


def cache_get(key: str) -> Any | None:
    """Retrieve a cached value, or None if missing."""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT value FROM cache WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])
    finally:
        conn.close()


def get_cache_age() -> float | None:
    """Return seconds since last cache update, or None if empty."""
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


def clear_cache() -> None:
    """Clear all cached data."""
    conn = _conn()
    try:
        conn.execute("DELETE FROM cache")
        conn.commit()
    finally:
        conn.close()
