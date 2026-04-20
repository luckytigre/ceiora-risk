"""Run-stage persistence for orchestrated model jobs."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config


TABLE = "job_run_status"
_SCHEMA_SQL = f"""
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


def default_db_path() -> Path:
    return Path(config.DATA_DB_PATH)


def _create_schema_objects(conn: sqlite3.Connection) -> None:
    conn.execute(_SCHEMA_SQL)
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_run_order ON {TABLE}(run_id, stage_order)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_profile_completed ON {TABLE}(profile, completed_at)"
    )


def _reset_corrupt_table(conn: sqlite3.Connection) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
    conn.execute(f"DROP TABLE IF EXISTS {TABLE}__repair")
    _create_schema_objects(conn)
    conn.commit()


def ensure_schema(db_path: Path) -> None:
    conn = _connect(db_path)
    try:
        try:
            _create_schema_objects(conn)
        except sqlite3.DatabaseError:
            _reset_corrupt_table(conn)
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
    details: dict[str, Any] | None = None,
) -> None:
    conn = _connect(db_path)
    now_iso = _now_iso()
    try:
        params = (
            run_id,
            profile,
            stage_name,
            int(stage_order),
            "running",
            now_iso,
            None,
            json.dumps(details or {}, sort_keys=True),
            None,
            None,
            now_iso,
        )
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
                params,
            )
        except sqlite3.DatabaseError:
            _reset_corrupt_table(conn)
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
                params,
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
        merged_details: dict[str, Any] = {}
        try:
            row = conn.execute(
                f"""
                SELECT details_json
                FROM {TABLE}
                WHERE run_id = ?
                  AND stage_name = ?
                """,
                (str(run_id), str(stage_name)),
            ).fetchone()
        except sqlite3.DatabaseError:
            _reset_corrupt_table(conn)
            row = None
        if row and row[0]:
            try:
                decoded = json.loads(str(row[0]))
                if isinstance(decoded, dict):
                    merged_details.update(decoded)
            except Exception:
                merged_details = {}
        merged_details.update(details or {})
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
                json.dumps(merged_details, sort_keys=True),
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


def heartbeat_stage(
    *,
    db_path: Path,
    run_id: str,
    stage_name: str,
    details: dict[str, Any] | None = None,
) -> None:
    conn = _connect(db_path)
    now_iso = _now_iso()
    try:
        try:
            row = conn.execute(
                f"""
                SELECT details_json
                FROM {TABLE}
                WHERE run_id = ?
                  AND stage_name = ?
                """,
                (str(run_id), str(stage_name)),
            ).fetchone()
        except sqlite3.DatabaseError:
            _reset_corrupt_table(conn)
            row = None
        merged_details: dict[str, Any] = {}
        if row and row[0]:
            try:
                decoded = json.loads(str(row[0]))
                if isinstance(decoded, dict):
                    merged_details.update(decoded)
            except Exception:
                merged_details = {}
        merged_details.update(details or {})
        merged_details["heartbeat_at"] = now_iso
        conn.execute(
            f"""
            UPDATE {TABLE}
            SET
                details_json = ?,
                updated_at = ?
            WHERE run_id = ?
              AND stage_name = ?
            """,
            (
                json.dumps(merged_details, sort_keys=True),
                now_iso,
                str(run_id),
                str(stage_name),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def fail_stale_running_stages(
    *,
    db_path: Path,
    stale_after_seconds: int = 6 * 60 * 60,
) -> int:
    """Mark long-abandoned running stages as failed so status summaries stay truthful."""
    if stale_after_seconds <= 0:
        return 0
    conn = _connect(db_path)
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    updated = 0
    try:
        try:
            rows = conn.execute(
                f"""
                SELECT run_id, stage_name, started_at, updated_at
                FROM {TABLE}
                WHERE status = 'running'
                """
            ).fetchall()
        except sqlite3.DatabaseError:
            _reset_corrupt_table(conn)
            return 0
        for run_id, stage_name, started_at, updated_at in rows:
            anchor_raw = updated_at or started_at
            try:
                anchor = datetime.fromisoformat(str(anchor_raw))
            except (TypeError, ValueError):
                anchor = None
            if anchor is None:
                continue
            age_seconds = (now - anchor).total_seconds()
            if age_seconds < float(stale_after_seconds):
                continue
            before = conn.total_changes
            conn.execute(
                f"""
                UPDATE {TABLE}
                SET
                    status = 'failed',
                    completed_at = ?,
                    error_type = ?,
                    error_message = ?,
                    updated_at = ?
                WHERE run_id = ?
                  AND stage_name = ?
                  AND status = 'running'
                """,
                (
                    now_iso,
                    "stale_running_stage",
                    "Marked failed after exceeding stale running-stage threshold.",
                    now_iso,
                    str(run_id),
                    str(stage_name),
                ),
            )
            updated += int(conn.total_changes > before)
        conn.commit()
        return updated
    finally:
        conn.close()


def completed_stages(*, db_path: Path, run_id: str) -> set[str]:
    conn = _connect(db_path)
    try:
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
        except sqlite3.DatabaseError:
            _reset_corrupt_table(conn)
            return set()
        return {str(r[0]) for r in rows if r and r[0]}
    finally:
        conn.close()


def run_rows(*, db_path: Path, run_id: str) -> list[dict[str, Any]]:
    conn = _connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
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
        except sqlite3.DatabaseError:
            _reset_corrupt_table(conn)
            return []
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _duration_seconds(started_at: Any, completed_at: Any) -> float | None:
    start_dt = _parse_iso_datetime(started_at)
    end_dt = _parse_iso_datetime(completed_at)
    if start_dt is None or end_dt is None:
        return None
    delta = (end_dt - start_dt).total_seconds()
    if delta < 0:
        return None
    return round(float(delta), 3)


def _details_payload(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("details_json")
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        decoded = json.loads(str(raw))
    except Exception:
        return {}
    return decoded if isinstance(decoded, dict) else {}


_ROW_COUNT_KEYS = {
    "row_count",
    "rows_upserted",
    "rows_written",
    "payload_count",
    "published_payload_count",
    "factor_rows_flushed",
    "residual_rows_flushed",
    "eligibility_rows_flushed",
    "verified_row_count",
    "expected_row_count",
}

_COUNTER_KEYS = {
    "dates_processed",
    "computed_dates",
    "items_processed",
    "items_total",
}


def _coerce_nonnegative_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def _record_metric(target: dict[str, int], key: str, value: Any) -> None:
    parsed = _coerce_nonnegative_int(value)
    if parsed is None:
        return
    target[str(key)] = parsed


def _extract_stage_row_counts(
    value: Any,
    *,
    row_counts: dict[str, int],
    counters: dict[str, int],
    prefix: str = "",
) -> None:
    if isinstance(value, dict):
        table = value.get("table")
        has_table_row_count = table is not None and "row_count" in value
        if has_table_row_count:
            _record_metric(row_counts, str(table), value.get("row_count"))
        for key, child in value.items():
            key_str = str(key)
            if key_str == "metrics":
                continue
            if key_str == "row_counts" and isinstance(child, dict):
                for nested_key, nested_value in child.items():
                    metric_name = f"{prefix}{nested_key}" if prefix else str(nested_key)
                    _record_metric(row_counts, metric_name, nested_value)
                continue
            if key_str in _ROW_COUNT_KEYS:
                if has_table_row_count and key_str == "row_count":
                    continue
                metric_name = f"{prefix}{key_str}" if prefix else key_str
                _record_metric(row_counts, metric_name, child)
                continue
            if key_str in _COUNTER_KEYS:
                metric_name = f"{prefix}{key_str}" if prefix else key_str
                _record_metric(counters, metric_name, child)
                continue
            if isinstance(child, dict):
                child_prefix = f"{prefix}{key_str}." if prefix else f"{key_str}."
                _extract_stage_row_counts(
                    child,
                    row_counts=row_counts,
                    counters=counters,
                    prefix=child_prefix,
                )
            elif isinstance(child, list):
                _extract_stage_row_counts(
                    child,
                    row_counts=row_counts,
                    counters=counters,
                    prefix=prefix,
                )
    elif isinstance(value, list):
        for item in value:
            _extract_stage_row_counts(
                item,
                row_counts=row_counts,
                counters=counters,
                prefix=prefix,
            )


def normalize_stage_metrics(
    details: dict[str, Any] | None,
    *,
    duration_seconds: float | None = None,
) -> dict[str, Any]:
    payload = dict(details or {})
    metrics = payload.get("metrics")
    if isinstance(metrics, dict):
        normalized = dict(metrics)
    else:
        normalized = {}
    row_counts: dict[str, int] = {}
    counters: dict[str, int] = {}
    _extract_stage_row_counts(payload, row_counts=row_counts, counters=counters)
    if row_counts:
        normalized["row_counts"] = row_counts
    else:
        normalized.pop("row_counts", None)
    if counters:
        normalized["counters"] = counters
    else:
        normalized.pop("counters", None)
    effective_duration = duration_seconds
    if effective_duration is None:
        try:
            existing_duration = payload.get("duration_seconds")
            if existing_duration is not None:
                effective_duration = round(float(existing_duration), 3)
        except (TypeError, ValueError):
            effective_duration = None
    if effective_duration is not None:
        normalized["duration_seconds"] = round(float(effective_duration), 3)
    else:
        normalized.pop("duration_seconds", None)
    progress: dict[str, Any] = {}
    unit = payload.get("unit")
    if unit not in (None, ""):
        progress["unit"] = unit
    for key in ("items_processed", "items_total", "progress_pct"):
        if payload.get(key) is not None:
            progress[key] = payload.get(key)
    if progress:
        normalized["progress"] = progress
    else:
        normalized.pop("progress", None)
    return normalized


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
            "current_stage": None,
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
    started_at = _min_nonempty("started_at")
    finished_at = _max_nonempty("completed_at")
    stage_summaries: list[dict[str, Any]] = []
    stage_duration_sum = 0.0
    now_iso = _now_iso()
    for r in ordered:
        details = _details_payload(r)
        stage_duration = details.get("duration_seconds")
        if stage_duration is None:
            duration_end = r.get("completed_at")
            if str(r.get("status") or "") == "running" and not duration_end:
                duration_end = now_iso
            stage_duration = _duration_seconds(r.get("started_at"), duration_end)
        if stage_duration is not None:
            try:
                stage_duration = round(float(stage_duration), 3)
                stage_duration_sum += float(stage_duration)
            except (TypeError, ValueError):
                stage_duration = None
        details["metrics"] = normalize_stage_metrics(details, duration_seconds=stage_duration)
        stage_summaries.append(
            {
                "stage_name": str(r.get("stage_name") or ""),
                "stage_order": int(r.get("stage_order") or 0),
                "status": str(r.get("status") or ""),
                "started_at": r.get("started_at"),
                "completed_at": r.get("completed_at"),
                "duration_seconds": stage_duration,
                "heartbeat_at": details.get("heartbeat_at") or r.get("updated_at"),
                "details": details,
                "metrics": details.get("metrics") or {},
                "error_type": r.get("error_type"),
                "error_message": r.get("error_message"),
            }
        )
    run_duration = _duration_seconds(started_at, finished_at)
    if run_duration is None and stage_duration_sum > 0:
        run_duration = round(float(stage_duration_sum), 3)
    completed_stage_durations = [
        float(item["duration_seconds"])
        for item in stage_summaries
        if item.get("duration_seconds") is not None
    ]
    slowest_stage = None
    if stage_summaries and completed_stage_durations:
        slowest_stage = max(
            (item for item in stage_summaries if item.get("duration_seconds") is not None),
            key=lambda item: float(item["duration_seconds"]),
        )
    current_stage = next((item for item in stage_summaries if item.get("status") == "running"), None)
    return {
        "run_id": str(ordered[0].get("run_id") or ""),
        "profile": str(ordered[0].get("profile") or ""),
        "status": overall_status,
        "started_at": started_at,
        "finished_at": finished_at,
        "updated_at": _max_nonempty("updated_at"),
        "duration_seconds": run_duration,
        "stage_count": int(len(ordered)),
        "completed_stage_count": int(sum(1 for s in stage_statuses if s == "completed")),
        "failed_stage_count": int(sum(1 for s in stage_statuses if s == "failed")),
        "running_stage_count": int(sum(1 for s in stage_statuses if s == "running")),
        "stage_duration_seconds_total": round(float(stage_duration_sum), 3),
        "current_stage": current_stage,
        "slowest_stage": (
            {
                "stage_name": str(slowest_stage.get("stage_name") or ""),
                "duration_seconds": float(slowest_stage.get("duration_seconds") or 0.0),
            }
            if slowest_stage is not None
            else None
        ),
        "stages": stage_summaries,
    }


def latest_run_summary_by_profile(
    *,
    db_path: Path,
    profiles: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    ensure_schema(db_path)
    fail_stale_running_stages(db_path=db_path)
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


def recent_run_summaries_by_profile(
    *,
    db_path: Path,
    profiles: list[str] | None = None,
    limit_per_profile: int = 8,
) -> dict[str, list[dict[str, Any]]]:
    ensure_schema(db_path)
    fail_stale_running_stages(db_path=db_path)
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

    run_ids_by_profile: dict[str, list[str]] = {}
    for row in rows:
        profile = str(row["profile"] or "")
        run_id = str(row["run_id"] or "")
        if not profile or not run_id:
            continue
        lst = run_ids_by_profile.setdefault(profile, [])
        if len(lst) < max(1, int(limit_per_profile)):
            lst.append(run_id)

    out: dict[str, list[dict[str, Any]]] = {}
    for profile, run_ids in run_ids_by_profile.items():
        out[profile] = [summarize_run_rows(run_rows(db_path=db_path, run_id=run_id)) for run_id in run_ids]
    return out
