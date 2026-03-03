"""GET /api/data/diagnostics — data freshness and engine observability."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import config
from db.sqlite import cache_get

from fastapi import APIRouter, Query

router = APIRouter()

DATA_DB = Path(config.DATA_DB_PATH)
CACHE_DB = Path(config.SQLITE_PATH)


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


def _table_stats(conn: sqlite3.Connection, table: str) -> dict[str, Any]:
    if not _table_exists(conn, table):
        return {"table": table, "exists": False}
    cols = _table_columns(conn, table)
    date_col = _first_existing(cols, ["as_of_date", "fetch_date", "date", "snapshot_date", "start_date"])
    updated_col = _first_existing(cols, ["updated_at"])
    ticker_col = "ticker" if "ticker" in cols else None
    job_col = "job_run_id" if "job_run_id" in cols else None

    row_count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
    ticker_count = (
        int(conn.execute(f"SELECT COUNT(DISTINCT UPPER({ticker_col})) FROM {table}").fetchone()[0] or 0)
        if ticker_col
        else None
    )
    min_date = max_date = None
    if date_col:
        min_date, max_date = conn.execute(
            f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}"
        ).fetchone()
    last_updated_at = None
    if updated_col:
        last_updated_at = conn.execute(f"SELECT MAX({updated_col}) FROM {table}").fetchone()[0]
    last_job_run_id = None
    if job_col:
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
            SELECT UPPER(ticker) AS ticker, as_of_date, COUNT(*) AS cnt
            FROM {table}
            GROUP BY UPPER(ticker), as_of_date
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
        iso = None
        if ts is not None:
            c = sqlite3.connect(":memory:")
            try:
                iso = c.execute("SELECT datetime(?, 'unixepoch')", (float(ts),)).fetchone()[0]
            finally:
                c.close()
        out.append(
            {
                "key": str(key),
                "updated_at_unix": float(ts) if ts is not None else None,
                "updated_at_utc": str(iso) if iso is not None else None,
            }
        )
    return out


def _resolve_exposure_source_table(conn: sqlite3.Connection) -> str:
    return "barra_raw_cross_section_history"


@router.get("/data/diagnostics")
async def get_data_diagnostics(include_paths: bool = Query(False)):
    data_conn = sqlite3.connect(str(DATA_DB))
    cache_conn = sqlite3.connect(str(CACHE_DB))
    try:
        source_tables = {
            "universe_eligibility_summary": _table_stats(data_conn, "universe_eligibility_summary")
            if _table_exists(data_conn, "universe_eligibility_summary")
            else None,
            "universe_constituent_snapshots": _table_stats(data_conn, "universe_constituent_snapshots")
            if _table_exists(data_conn, "universe_constituent_snapshots")
            else None,
            "fundamental_history": _table_stats(data_conn, "fundamental_snapshots"),
            "trbc_history": _table_stats(data_conn, "trbc_industry_history"),
            "price_history": _table_stats(data_conn, "prices_daily"),
            "pit_cross_section_snapshot": _table_stats(data_conn, "universe_cross_section_snapshot")
            if _table_exists(data_conn, "universe_cross_section_snapshot")
            else None,
            "barra_raw_cross_section_history": _table_stats(data_conn, "barra_raw_cross_section_history")
            if _table_exists(data_conn, "barra_raw_cross_section_history")
            else None,
            "security_master": _table_stats(data_conn, "security_master")
            if _table_exists(data_conn, "security_master")
            else None,
            "fundamentals_history": _table_stats(data_conn, "fundamentals_history")
            if _table_exists(data_conn, "fundamentals_history")
            else None,
            "trbc_industry_country_history": _table_stats(data_conn, "trbc_industry_country_history")
            if _table_exists(data_conn, "trbc_industry_country_history")
            else None,
            "estu_membership_daily": _table_stats(data_conn, "estu_membership_daily")
            if _table_exists(data_conn, "estu_membership_daily")
            else None,
        }

        exposure_source_table = _resolve_exposure_source_table(data_conn)

        dup_stats = {
            "active_exposure_source": _exposure_duplicate_stats(data_conn, exposure_source_table),
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
            "exposure_source_table": exposure_source_table,
            "source_tables": source_tables,
            "exposure_duplicates": dup_stats,
            "cross_section_usage": {
                "eligibility_summary": elig_summary,
                "factor_cross_section": factor_cross_section,
            },
            "risk_engine_meta": cache_get("risk_engine_meta") or {},
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
