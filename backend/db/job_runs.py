"""Run-stage persistence for orchestrated model jobs."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TABLE = "job_run_status"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


def ensure_schema(db_path: Path) -> None:
    conn = _connect(db_path)
    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                run_id TEXT NOT NULL,
                profile TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                stage_order INTEGER NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                details_json TEXT,
                error_type TEXT,
                error_message TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, stage_name)
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_run_order ON {TABLE}(run_id, stage_order)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_profile_completed ON {TABLE}(profile, completed_at)"
        )
        conn.commit()
    finally:
        conn.close()


def begin_stage(
    *,
    db_path: Path,
    run_id: str,
    profile: str,
    stage_name: str,
    stage_order: int,
) -> None:
    conn = _connect(db_path)
    now_iso = _now_iso()
    try:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {TABLE} (
                run_id,
                profile,
                stage_name,
                stage_order,
                status,
                started_at,
                completed_at,
                details_json,
                error_type,
                error_message,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                profile,
                stage_name,
                int(stage_order),
                "running",
                now_iso,
                None,
                None,
                None,
                None,
                now_iso,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def finish_stage(
    *,
    db_path: Path,
    run_id: str,
    stage_name: str,
    status: str,
    details: dict[str, Any] | None = None,
    error: dict[str, str] | None = None,
) -> None:
    conn = _connect(db_path)
    now_iso = _now_iso()
    try:
        conn.execute(
            f"""
            UPDATE {TABLE}
            SET
                status = ?,
                completed_at = ?,
                details_json = ?,
                error_type = ?,
                error_message = ?,
                updated_at = ?
            WHERE run_id = ?
              AND stage_name = ?
            """,
            (
                str(status),
                now_iso,
                json.dumps(details or {}, sort_keys=True),
                (error or {}).get("type"),
                (error or {}).get("message"),
                now_iso,
                run_id,
                stage_name,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def completed_stages(*, db_path: Path, run_id: str) -> set[str]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            f"""
            SELECT stage_name
            FROM {TABLE}
            WHERE run_id = ?
              AND status IN ('completed', 'skipped')
            """,
            (run_id,),
        ).fetchall()
        return {str(r[0]) for r in rows if r and r[0]}
    finally:
        conn.close()


def run_rows(*, db_path: Path, run_id: str) -> list[dict[str, Any]]:
    conn = _connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT *
            FROM {TABLE}
            WHERE run_id = ?
            ORDER BY stage_order ASC
            """,
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
