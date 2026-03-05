"""Relational persistence for Layer B model outputs."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _drop_if_columns_missing(conn: sqlite3.Connection, *, table: str, required: set[str]) -> None:
    cols = _table_columns(conn, table)
    if cols and not required.issubset(cols):
        conn.execute(f"DROP TABLE IF EXISTS {table}")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    _drop_if_columns_missing(
        conn,
        table="model_specific_risk_daily",
        required={"as_of_date", "ric", "ticker", "specific_var", "specific_vol", "obs", "trbc_industry_group", "run_id", "updated_at"},
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_factor_returns_daily (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL,
            r_squared REAL NOT NULL,
            residual_vol REAL NOT NULL,
            cross_section_n INTEGER NOT NULL DEFAULT 0,
            eligible_n INTEGER NOT NULL DEFAULT 0,
            coverage REAL NOT NULL DEFAULT 0.0,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (date, factor_name)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_factor_covariance_daily (
            as_of_date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_name_2 TEXT NOT NULL,
            covariance REAL NOT NULL,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (as_of_date, factor_name, factor_name_2)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_specific_risk_daily (
            as_of_date TEXT NOT NULL,
            ric TEXT NOT NULL,
            ticker TEXT,
            specific_var REAL NOT NULL,
            specific_vol REAL NOT NULL,
            obs INTEGER NOT NULL DEFAULT 0,
            trbc_industry_group TEXT,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (as_of_date, ric)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_run_metadata (
            run_id TEXT PRIMARY KEY,
            refresh_mode TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            factor_returns_asof TEXT,
            source_dates_json TEXT NOT NULL,
            params_json TEXT NOT NULL,
            risk_engine_state_json TEXT NOT NULL,
            row_counts_json TEXT NOT NULL,
            error_type TEXT,
            error_message TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_model_factor_returns_daily_date ON model_factor_returns_daily(date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_model_factor_covariance_daily_asof ON model_factor_covariance_daily(as_of_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_model_specific_risk_daily_asof ON model_specific_risk_daily(as_of_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_model_run_metadata_completed ON model_run_metadata(completed_at)"
    )
    # Residual history is intentionally cache-only (daily_specific_residuals in cache.db).
    # Remove deprecated duplicated persistence table from data.db when present.
    conn.execute("DROP TABLE IF EXISTS model_specific_residuals_daily")


def _load_factor_returns(cache_db: Path, *, date_from: str | None = None) -> pd.DataFrame:
    conn = sqlite3.connect(str(cache_db))
    try:
        cols = _table_columns(conn, "daily_factor_returns")
        if not cols:
            return pd.DataFrame()
        coverage_col = "coverage" if "coverage" in cols else None
        cross_col = "cross_section_n" if "cross_section_n" in cols else None
        elig_col = "eligible_n" if "eligible_n" in cols else None
        where_sql = ""
        params: list[Any] = []
        if date_from:
            where_sql = "WHERE date >= ?"
            params.append(str(date_from))
        df = pd.read_sql_query(
            f"""
            SELECT
                date,
                factor_name,
                factor_return,
                r_squared,
                residual_vol,
                {cross_col if cross_col else '0'} AS cross_section_n,
                {elig_col if elig_col else '0'} AS eligible_n,
                {coverage_col if coverage_col else '0.0'} AS coverage
            FROM daily_factor_returns
            {where_sql}
            ORDER BY date, factor_name
            """,
            conn,
            params=params,
        )
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = df["date"].astype(str)
    df["factor_name"] = df["factor_name"].astype(str)
    for col in ["factor_return", "r_squared", "residual_vol", "coverage"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
    for col in ["cross_section_n", "eligible_n"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df.dropna(subset=["date", "factor_name", "factor_return", "r_squared", "residual_vol"])


def _upsert_factor_returns(
    conn: sqlite3.Connection,
    *,
    rows: pd.DataFrame,
    run_id: str,
    updated_at: str,
) -> int:
    if rows.empty:
        return 0
    payload = [
        (
            str(r.date),
            str(r.factor_name),
            float(r.factor_return),
            float(r.r_squared),
            float(r.residual_vol),
            int(r.cross_section_n),
            int(r.eligible_n),
            float(r.coverage),
            run_id,
            updated_at,
        )
        for r in rows.itertuples(index=False)
    ]
    conn.executemany(
        """
        INSERT OR REPLACE INTO model_factor_returns_daily (
            date, factor_name, factor_return, r_squared, residual_vol,
            cross_section_n, eligible_n, coverage, run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def _upsert_covariance(
    conn: sqlite3.Connection,
    *,
    as_of_date: str,
    cov: pd.DataFrame,
    run_id: str,
    updated_at: str,
) -> int:
    if cov is None or cov.empty:
        return 0
    factors = [str(c) for c in cov.columns]
    payload: list[tuple[Any, ...]] = []
    for f1 in factors:
        for f2 in factors:
            payload.append(
                (
                    as_of_date,
                    f1,
                    f2,
                    float(cov.loc[f1, f2]),
                    run_id,
                    updated_at,
                )
            )
    conn.executemany(
        """
        INSERT OR REPLACE INTO model_factor_covariance_daily (
            as_of_date, factor_name, factor_name_2, covariance, run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def _upsert_specific_risk(
    conn: sqlite3.Connection,
    *,
    as_of_date: str,
    specific_risk_by_ticker: dict[str, dict[str, float | int | str]],
    run_id: str,
    updated_at: str,
) -> int:
    if not specific_risk_by_ticker:
        return 0
    payload: list[tuple[Any, ...]] = []
    for key, row in specific_risk_by_ticker.items():
        ric = str(row.get("ric") or key or "").upper().strip()
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ric:
            continue
        spec_var = float(row.get("specific_var", 0.0) or 0.0)
        spec_vol = float(row.get("specific_vol", 0.0) or 0.0)
        obs = int(row.get("obs", 0) or 0)
        industry = str(row.get("trbc_industry_group") or "").strip()
        payload.append(
            (
                as_of_date,
                ric,
                ticker or None,
                spec_var,
                spec_vol,
                obs,
                industry,
                run_id,
                updated_at,
            )
        )
    if not payload:
        return 0
    conn.executemany(
        """
        INSERT OR REPLACE INTO model_specific_risk_daily (
            as_of_date, ric, ticker, specific_var, specific_vol, obs, trbc_industry_group, run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def _latest_date(conn: sqlite3.Connection, *, table: str, col: str) -> str | None:
    row = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])


def persist_model_outputs(
    *,
    data_db: Path,
    cache_db: Path,
    run_id: str,
    refresh_mode: str,
    status: str,
    started_at: str,
    completed_at: str,
    source_dates: dict[str, Any],
    params: dict[str, Any],
    risk_engine_state: dict[str, Any],
    cov: pd.DataFrame,
    specific_risk_by_ticker: dict[str, dict[str, float | int | str]],
    error: dict[str, str] | None = None,
) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    as_of_date = str(
        risk_engine_state.get("factor_returns_latest_date")
        or source_dates.get("exposures_asof")
        or source_dates.get("fundamentals_asof")
        or completed_at[:10]
    )

    conn = sqlite3.connect(str(data_db), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    try:
        _ensure_schema(conn)
        factor_date_from = _latest_date(conn, table="model_factor_returns_daily", col="date")

        factor_returns = _load_factor_returns(cache_db, date_from=factor_date_from)

        n_factor = _upsert_factor_returns(conn, rows=factor_returns, run_id=run_id, updated_at=now_iso)
        n_cov = _upsert_covariance(
            conn,
            as_of_date=as_of_date,
            cov=cov,
            run_id=run_id,
            updated_at=now_iso,
        )
        n_spec = _upsert_specific_risk(
            conn,
            as_of_date=as_of_date,
            specific_risk_by_ticker=specific_risk_by_ticker,
            run_id=run_id,
            updated_at=now_iso,
        )
        row_counts = {
            "model_factor_returns_daily": int(n_factor),
            "model_factor_covariance_daily": int(n_cov),
            "model_specific_risk_daily": int(n_spec),
        }
        quality_errors: list[str] = []
        if str(status).lower() == "ok":
            if n_factor <= 0:
                quality_errors.append("factor_returns_empty")
            if n_cov <= 0:
                quality_errors.append("factor_covariance_empty")
            if n_spec <= 0:
                quality_errors.append("specific_risk_empty")
        effective_status = "failed" if quality_errors else str(status)
        effective_error = dict(error or {})
        if quality_errors:
            effective_error = {
                "type": "quality_gate_failed",
                "message": " | ".join(quality_errors),
            }
        conn.execute(
            """
            INSERT OR REPLACE INTO model_run_metadata (
                run_id,
                refresh_mode,
                status,
                started_at,
                completed_at,
                factor_returns_asof,
                source_dates_json,
                params_json,
                risk_engine_state_json,
                row_counts_json,
                error_type,
                error_message,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                str(refresh_mode),
                effective_status,
                str(started_at),
                str(completed_at),
                as_of_date,
                json.dumps(source_dates or {}, sort_keys=True),
                json.dumps(params or {}, sort_keys=True),
                json.dumps(risk_engine_state or {}, sort_keys=True),
                json.dumps(row_counts, sort_keys=True),
                effective_error.get("type"),
                effective_error.get("message"),
                now_iso,
            ),
        )
        conn.commit()
        if quality_errors:
            raise RuntimeError(f"model output quality gate failed: {'; '.join(quality_errors)}")
        return {
            "status": "ok",
            "run_id": run_id,
            "factor_returns_asof": as_of_date,
            "row_counts": row_counts,
        }
    finally:
        conn.close()
