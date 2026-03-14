"""GET /api/data/diagnostics — data freshness and engine observability."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.api.auth import require_role
from backend.data.sqlite import cache_get, cache_get_live_first

from fastapi import APIRouter, Header, Query

router = APIRouter()

DATA_DB = Path(config.DATA_DB_PATH)
CACHE_DB = Path(config.SQLITE_PATH)
_INTERNAL_CACHE_PREFIXES = ("__snap__:",)
_INTERNAL_CACHE_KEYS = {"__cache_snapshot_active"}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _first_existing(cols: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return None


def _approx_row_count_from_stats(conn: sqlite3.Connection, table: str) -> int | None:
    if not _table_exists(conn, "sqlite_stat1"):
        return None
    rows = conn.execute(
        "SELECT stat FROM sqlite_stat1 WHERE tbl = ?",
        (table,),
    ).fetchall()
    estimates: list[int] = []
    for (stat,) in rows:
        if not stat:
            continue
        head = str(stat).split(" ", 1)[0].strip()
        if not head:
            continue
        try:
            estimates.append(int(head))
        except ValueError:
            continue
    if not estimates:
        return None
    return max(estimates)


def _table_stats(
    conn: sqlite3.Connection,
    table: str,
    include_exact_row_counts: bool = False,
    include_expensive_checks: bool = False,
) -> dict[str, Any]:
    if not _table_exists(conn, table):
        return {"table": table, "exists": False}
    cols = _table_columns(conn, table)
    date_col = _first_existing(cols, ["as_of_date", "fetch_date", "date", "snapshot_date", "start_date"])
    updated_col = _first_existing(cols, ["updated_at"])
    ticker_col = "ticker" if "ticker" in cols else None
    job_col = "job_run_id" if "job_run_id" in cols else None

    approx_row_count = _approx_row_count_from_stats(conn, table)
    row_count: int | None
    row_count_mode: str
    if include_exact_row_counts:
        row_count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
        row_count_mode = "exact"
    else:
        row_count = int(approx_row_count) if approx_row_count is not None else None
        row_count_mode = "approx" if approx_row_count is not None else "unknown"
    if ticker_col and include_expensive_checks:
        ticker_count = int(conn.execute(f"SELECT COUNT(DISTINCT {ticker_col}) FROM {table}").fetchone()[0] or 0)
    else:
        ticker_count = None
    min_date = max_date = None
    if date_col:
        # Use indexed ORDER BY probes instead of MIN/MAX aggregates to avoid
        # full scans on very large SQLite tables.
        min_row = conn.execute(
            f"""
            SELECT {date_col}
            FROM {table}
            WHERE {date_col} IS NOT NULL
            ORDER BY {date_col} ASC
            LIMIT 1
            """
        ).fetchone()
        max_row = conn.execute(
            f"""
            SELECT {date_col}
            FROM {table}
            WHERE {date_col} IS NOT NULL
            ORDER BY {date_col} DESC
            LIMIT 1
            """
        ).fetchone()
        min_date = min_row[0] if min_row else None
        max_date = max_row[0] if max_row else None
    last_updated_at = None
    if updated_col and include_expensive_checks:
        last_updated_at = conn.execute(f"SELECT MAX({updated_col}) FROM {table}").fetchone()[0]
    last_job_run_id = None
    if job_col and include_expensive_checks:
        if updated_col:
            row = conn.execute(
                f"""
                SELECT {job_col}
                FROM {table}
                WHERE {job_col} IS NOT NULL AND TRIM({job_col}) <> ''
                ORDER BY {updated_col} DESC
                LIMIT 1
                """
            ).fetchone()
            last_job_run_id = row[0] if row else None
        else:
            row = conn.execute(
                f"""
                SELECT {job_col}
                FROM {table}
                WHERE {job_col} IS NOT NULL AND TRIM({job_col}) <> ''
                ORDER BY {job_col} DESC
                LIMIT 1
                """
            ).fetchone()
            last_job_run_id = row[0] if row else None

    return {
        "table": table,
        "exists": True,
        "row_count": row_count,
        "row_count_mode": row_count_mode,
        "ticker_count": ticker_count,
        "date_column": date_col,
        "min_date": str(min_date) if min_date is not None else None,
        "max_date": str(max_date) if max_date is not None else None,
        "last_updated_at": str(last_updated_at) if last_updated_at is not None else None,
        "last_job_run_id": str(last_job_run_id) if last_job_run_id is not None else None,
    }


def _exposure_duplicate_stats(conn: sqlite3.Connection, table: str) -> dict[str, Any]:
    if not _table_exists(conn, table):
        return {"table": table, "exists": False}
    cols = _table_columns(conn, table)
    if "ticker" not in cols or "as_of_date" not in cols:
        return {"table": table, "exists": True, "duplicate_groups": 0, "duplicate_extra_rows": 0}
    dup_groups, dup_extra = conn.execute(
        f"""
        SELECT COUNT(*), COALESCE(SUM(cnt - 1), 0)
        FROM (
            SELECT ticker, as_of_date, COUNT(*) AS cnt
            FROM {table}
            GROUP BY ticker, as_of_date
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    return {
        "table": table,
        "exists": True,
        "duplicate_groups": int(dup_groups or 0),
        "duplicate_extra_rows": int(dup_extra or 0),
    }


def _cache_rows() -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(CACHE_DB))
    try:
        if not _table_exists(conn, "cache"):
            return []
        rows = conn.execute(
            """
            SELECT key, updated_at
            FROM cache
            ORDER BY updated_at DESC
            """
        ).fetchall()
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for key, ts in rows:
        key_txt = str(key)
        if key_txt in _INTERNAL_CACHE_KEYS or key_txt.startswith(_INTERNAL_CACHE_PREFIXES):
            continue
        iso = None
        if ts is not None:
            iso = datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
        out.append(
            {
                "key": key_txt,
                "updated_at_unix": float(ts) if ts is not None else None,
                "updated_at_utc": str(iso) if iso is not None else None,
            }
        )
    return out


def _resolve_exposure_source(conn: sqlite3.Connection) -> dict[str, Any]:
    table = "barra_raw_cross_section_history"
    latest_asof = None
    if _table_exists(conn, table):
        row = conn.execute(f"SELECT MAX(as_of_date) FROM {table}").fetchone()
        latest_asof = row[0] if row else None
    return {
        "table": table,
        "selection_mode": "canonical_latest_raw_history",
        "is_dynamic": False,
        "latest_asof": str(latest_asof) if latest_asof is not None else None,
        "plain_english": "The analytics engine always takes the latest row per RIC from barra_raw_cross_section_history.",
    }


@router.get("/data/diagnostics")
def get_data_diagnostics(
    include_paths: bool = Query(False),
    include_exact_row_counts: bool = Query(False),
    include_expensive_checks: bool = Query(False),
    x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    authorization: str | None = Header(default=None),
):
    if bool(include_paths or include_exact_row_counts or include_expensive_checks):
        require_role(
            "operator",
            x_operator_token=x_operator_token,
            authorization=authorization,
        )
    data_conn = sqlite3.connect(str(DATA_DB))
    cache_conn = sqlite3.connect(str(CACHE_DB))
    try:
        canonical_tables = {
            "security_master": "security_master",
            "security_fundamentals_pit": "security_fundamentals_pit",
            "security_classification_pit": "security_classification_pit",
            "security_prices_eod": "security_prices_eod",
            "estu_membership_daily": "estu_membership_daily",
            "barra_raw_cross_section_history": "barra_raw_cross_section_history",
            "universe_cross_section_snapshot": "universe_cross_section_snapshot",
        }
        source_tables = {
            label: (
                _table_stats(
                    data_conn,
                    table,
                    include_exact_row_counts=include_exact_row_counts,
                    include_expensive_checks=include_expensive_checks,
                )
                if _table_exists(data_conn, table)
                else None
            )
            for label, table in canonical_tables.items()
        }

        exposure_source = _resolve_exposure_source(data_conn)
        exposure_source_table = str(exposure_source.get("table") or "")

        if include_expensive_checks:
            dup_stats = {
                "active_exposure_source": {
                    **_exposure_duplicate_stats(data_conn, exposure_source_table),
                    "computed": True,
                },
            }
        else:
            dup_stats = {
                "active_exposure_source": {
                    "table": exposure_source_table,
                    "exists": _table_exists(data_conn, exposure_source_table),
                    "duplicate_groups": None,
                    "duplicate_extra_rows": None,
                    "computed": False,
                },
            }

        elig_summary = {
            "available": False,
            "latest": None,
            "min_structural_eligible_n": None,
            "max_structural_eligible_n": None,
            "min_regression_member_n": None,
            "max_regression_member_n": None,
        }
        if _table_exists(cache_conn, "daily_universe_eligibility_summary"):
            latest = cache_conn.execute(
                """
                SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                       structural_coverage, regression_coverage, alert_level
                FROM daily_universe_eligibility_summary
                ORDER BY date DESC
                LIMIT 1
                """
            ).fetchone()
            mins = cache_conn.execute(
                """
                SELECT MIN(structural_eligible_n), MAX(structural_eligible_n),
                       MIN(regression_member_n), MAX(regression_member_n)
                FROM daily_universe_eligibility_summary
                """
            ).fetchone()
            elig_summary["available"] = True
            if latest:
                elig_summary["latest"] = {
                    "date": str(latest[0]),
                    "exp_date": str(latest[1]) if latest[1] is not None else None,
                    "exposure_n": int(latest[2] or 0),
                    "structural_eligible_n": int(latest[3] or 0),
                    "regression_member_n": int(latest[4] or 0),
                    "structural_coverage_pct": round(100.0 * float(latest[5] or 0.0), 2),
                    "regression_coverage_pct": round(100.0 * float(latest[6] or 0.0), 2),
                    "alert_level": str(latest[7] or ""),
                }
            if mins:
                elig_summary["min_structural_eligible_n"] = int(mins[0] or 0)
                elig_summary["max_structural_eligible_n"] = int(mins[1] or 0)
                elig_summary["min_regression_member_n"] = int(mins[2] or 0)
                elig_summary["max_regression_member_n"] = int(mins[3] or 0)

        factor_cross_section = {"available": False, "latest": None, "min_cross_section_n": None, "max_cross_section_n": None}
        if _table_exists(cache_conn, "daily_factor_returns"):
            latest = cache_conn.execute(
                """
                SELECT date, MIN(cross_section_n), MAX(cross_section_n), MIN(eligible_n), MAX(eligible_n)
                FROM daily_factor_returns
                WHERE date = (SELECT MAX(date) FROM daily_factor_returns)
                """
            ).fetchone()
            mm = cache_conn.execute(
                "SELECT MIN(cross_section_n), MAX(cross_section_n), MIN(eligible_n), MAX(eligible_n) FROM daily_factor_returns"
            ).fetchone()
            factor_cross_section["available"] = True
            if latest:
                factor_cross_section["latest"] = {
                    "date": str(latest[0]) if latest[0] is not None else None,
                    "cross_section_n_min": int(latest[1] or 0),
                    "cross_section_n_max": int(latest[2] or 0),
                    "eligible_n_min": int(latest[3] or 0),
                    "eligible_n_max": int(latest[4] or 0),
                }
            if mm:
                factor_cross_section["min_cross_section_n"] = int(mm[0] or 0)
                factor_cross_section["max_cross_section_n"] = int(mm[1] or 0)
                factor_cross_section["min_eligible_n"] = int(mm[2] or 0)
                factor_cross_section["max_eligible_n"] = int(mm[3] or 0)

        payload = {
            "status": "ok",
            "database_path": DATA_DB.name,
            "cache_db_path": CACHE_DB.name,
            "diagnostic_scope": {
                "source": "local_sqlite_and_cache",
                "plain_english": (
                    "Detailed diagnostics reflect this backend instance's local SQLite/cache state. "
                    "Use the Health page for live operator truth, lane status, and Neon health."
                ),
            },
            "truth_surfaces": {
                "dashboard_serving": {
                    "source": "durable_serving_payloads",
                    "plain_english": (
                        "Risk, Explore, Positions, Health, and other user-facing pages should read compact durable serving payloads "
                        "instead of rebuilding directly from raw source tables."
                    ),
                },
                "operator_status": {
                    "source": "runtime_status_and_job_runs",
                    "plain_english": (
                        "Operator status is the live control-room truth for lane status, holdings dirty state, active snapshot, "
                        "and Neon mirror/parity health."
                    ),
                },
                "local_diagnostics": {
                    "source": "local_sqlite_and_cache",
                    "plain_english": (
                        "This diagnostics endpoint inspects the current backend instance and its local SQLite/cache files. "
                        "Treat it as a deep diagnostics panel, not the live operator control room."
                    ),
                },
            },
            "exposure_source_table": exposure_source_table,
            "exposure_source": exposure_source,
            "source_tables": source_tables,
            "exposure_duplicates": dup_stats,
            "cross_section_usage": {
                "eligibility_summary": elig_summary,
                "factor_cross_section": factor_cross_section,
            },
            "risk_engine_meta": cache_get_live_first("risk_engine_meta") or {},
            "cuse4_foundation": cache_get("cuse4_foundation") or {},
            "cache_outputs": _cache_rows(),
        }
        if include_paths:
            payload["database_path"] = str(DATA_DB)
            payload["cache_db_path"] = str(CACHE_DB)
        return payload
    finally:
        data_conn.close()
        cache_conn.close()
