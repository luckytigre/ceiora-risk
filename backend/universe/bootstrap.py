"""Canonical cUSE4 bootstrap.

Legacy bootstrap from compatibility views has been retired. Canonical bootstrap
now ensures tables exist, syncs the committed universe registry into
the registry-first universe surfaces, maintains legacy compatibility tables,
and reports current row counts.
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
    SECURITY_INGEST_AUDIT_TABLE,
    SECURITY_INGEST_RUNS_TABLE,
    SECURITY_MASTER_COMPAT_CURRENT_TABLE,
    SECURITY_MASTER_TABLE,
    SECURITY_POLICY_CURRENT_TABLE,
    SECURITY_REGISTRY_TABLE,
    SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
    SECURITY_TAXONOMY_CURRENT_TABLE,
    TRBC_HISTORY_TABLE,
    ensure_cuse4_schema,
)
from backend.universe.security_master_sync import (
    DEFAULT_SECURITY_MASTER_SEED_PATH as DEFAULT_SECURITY_REGISTRY_BOOTSTRAP_SEED_PATH,
    sync_security_master_seed as sync_registry_bootstrap_surfaces,
)


def _count(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0] or 0) if row else 0


def bootstrap_cuse4_source_tables(
    *,
    db_path: Path,
    seed_path: Path = DEFAULT_SECURITY_REGISTRY_BOOTSTRAP_SEED_PATH,
) -> dict[str, Any]:
    """Ensure canonical schema exists, sync the committed registry seed, and report row counts.

    Args:
        db_path: Path to the canonical SQLite DB.
        seed_path: Versioned registry seed artifact to upsert into the DB.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    job_run_id = f"cuse4_bootstrap_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")

    try:
        ensure_cuse4_schema(conn)
        seed_sync = sync_registry_bootstrap_surfaces(conn, seed_path=seed_path)
        conn.commit()

        return {
            "status": "ok",
            "mode": "bootstrap_only",
            "db_path": str(db_path),
            "job_run_id": job_run_id,
            "updated_at": now_iso,
            "seed_sync": seed_sync,
            "security_master_rows": _count(conn, SECURITY_MASTER_TABLE),
            "security_registry_rows": _count(conn, SECURITY_REGISTRY_TABLE),
            "security_taxonomy_current_rows": _count(conn, SECURITY_TAXONOMY_CURRENT_TABLE),
            "security_policy_current_rows": _count(conn, SECURITY_POLICY_CURRENT_TABLE),
            "security_source_observation_daily_rows": _count(conn, SECURITY_SOURCE_OBSERVATION_DAILY_TABLE),
            "security_ingest_runs_rows": _count(conn, SECURITY_INGEST_RUNS_TABLE),
            "security_ingest_audit_rows": _count(conn, SECURITY_INGEST_AUDIT_TABLE),
            "security_master_compat_current_rows": _count(conn, SECURITY_MASTER_COMPAT_CURRENT_TABLE),
            "security_fundamentals_pit_rows": _count(conn, FUNDAMENTALS_HISTORY_TABLE),
            "security_classification_pit_rows": _count(conn, TRBC_HISTORY_TABLE),
            "prices_rows": _count(conn, PRICES_TABLE),
            "estu_membership_rows": _count(conn, ESTU_MEMBERSHIP_TABLE),
        }
    finally:
        conn.close()
