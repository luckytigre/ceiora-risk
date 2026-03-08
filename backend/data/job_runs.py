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


def summarize_run_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "run_id": None,
            "profile": None,
            "status": "missing",
            "started_at": None,
            "finished_at": None,
            "updated_at": None,
            "stage_count": 0,
            "completed_stage_count": 0,
            "failed_stage_count": 0,
            "running_stage_count": 0,
            "stages": [],
        }

    stage_statuses = [str(r.get("status") or "") for r in rows]
    if any(s == "failed" for s in stage_statuses):
        overall_status = "failed"
    elif any(s == "running" for s in stage_statuses):
        overall_status = "running"
    elif any(s == "completed" for s in stage_statuses):
        overall_status = "ok"
    elif all(s == "skipped" for s in stage_statuses):
        overall_status = "skipped"
    else:
        overall_status = "unknown"

    def _min_nonempty(key: str) -> str | None:
        vals = [str(r.get(key)) for r in rows if r.get(key)]
        return min(vals) if vals else None

    def _max_nonempty(key: str) -> str | None:
        vals = [str(r.get(key)) for r in rows if r.get(key)]
        return max(vals) if vals else None

    ordered = sorted(rows, key=lambda r: int(r.get("stage_order") or 0))
    return {
        "run_id": str(ordered[0].get("run_id") or ""),
        "profile": str(ordered[0].get("profile") or ""),
        "status": overall_status,
        "started_at": _min_nonempty("started_at"),
        "finished_at": _max_nonempty("completed_at"),
        "updated_at": _max_nonempty("updated_at"),
        "stage_count": int(len(ordered)),
        "completed_stage_count": int(sum(1 for s in stage_statuses if s == "completed")),
        "failed_stage_count": int(sum(1 for s in stage_statuses if s == "failed")),
        "running_stage_count": int(sum(1 for s in stage_statuses if s == "running")),
        "stages": [
            {
                "stage_name": str(r.get("stage_name") or ""),
                "stage_order": int(r.get("stage_order") or 0),
                "status": str(r.get("status") or ""),
                "started_at": r.get("started_at"),
                "completed_at": r.get("completed_at"),
                "error_type": r.get("error_type"),
                "error_message": r.get("error_message"),
            }
            for r in ordered
        ],
    }


def latest_run_summary_by_profile(
    *,
    db_path: Path,
    profiles: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    ensure_schema(db_path)
    conn = _connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        clause = ""
        params: list[Any] = []
        if profiles:
            clean = [str(p).strip() for p in profiles if str(p).strip()]
            if clean:
                placeholders = ",".join("?" for _ in clean)
                clause = f"WHERE profile IN ({placeholders})"
                params.extend(clean)
        try:
            rows = conn.execute(
                f"""
                SELECT
                    profile,
                    run_id,
                    MAX(COALESCE(completed_at, started_at, updated_at)) AS last_ts
                FROM {TABLE}
                {clause}
                GROUP BY profile, run_id
                ORDER BY profile ASC, last_ts DESC
                """,
                params,
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
    finally:
        conn.close()

    latest_by_profile: dict[str, str] = {}
    for row in rows:
        profile = str(row["profile"] or "")
        run_id = str(row["run_id"] or "")
        if profile and run_id and profile not in latest_by_profile:
            latest_by_profile[profile] = run_id

    out: dict[str, dict[str, Any]] = {}
    for profile, run_id in latest_by_profile.items():
        out[profile] = summarize_run_rows(run_rows(db_path=db_path, run_id=run_id))
    return out
