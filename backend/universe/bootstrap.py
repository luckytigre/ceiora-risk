"""Canonical cUSE4 bootstrap.

Legacy bootstrap from compatibility views has been retired. This module now
only ensures canonical tables exist and reports current row counts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any

from backend.universe.schema import (
    ESTU_MEMBERSHIP_TABLE,
    FUNDAMENTALS_HISTORY_TABLE,
    PRICES_TABLE,
    SECURITY_MASTER_TABLE,
    TRBC_HISTORY_TABLE,
    ensure_cuse4_schema,
)


def _count(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0] or 0) if row else 0


def bootstrap_cuse4_source_tables(
    *,
    db_path: Path,
    replace_all: bool = True,
) -> dict[str, Any]:
    """Ensure canonical schema exists and return current canonical row counts.

    Args:
        db_path: Path to the canonical SQLite DB.
        replace_all: Kept for API compatibility; no destructive behavior is
            performed in canonical mode.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    job_run_id = f"cuse4_bootstrap_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")

    try:
        ensure_cuse4_schema(conn)
        conn.commit()

        return {
            "status": "ok",
            "mode": "canonical_noop",
            "db_path": str(db_path),
            "replace_all": bool(replace_all),
            "job_run_id": job_run_id,
            "updated_at": now_iso,
            "security_master_rows": _count(conn, SECURITY_MASTER_TABLE),
            "security_fundamentals_pit_rows": _count(conn, FUNDAMENTALS_HISTORY_TABLE),
            "security_classification_pit_rows": _count(conn, TRBC_HISTORY_TABLE),
            "prices_rows": _count(conn, PRICES_TABLE),
            "estu_membership_rows": _count(conn, ESTU_MEMBERSHIP_TABLE),
        }
    finally:
        conn.close()
