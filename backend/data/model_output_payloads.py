"""Payload shaping helpers for durable model-output persistence."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.data.model_output_schema import table_columns


def load_factor_returns(cache_db: Path, *, date_from: str | None = None) -> pd.DataFrame:
    conn = sqlite3.connect(str(cache_db))
    try:
        cols = table_columns(conn, "daily_factor_returns")
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
                {('robust_se' if 'robust_se' in cols else '0.0')} AS robust_se,
                {('t_stat' if 't_stat' in cols else '0.0')} AS t_stat,
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
    for col in ["factor_return", "robust_se", "t_stat", "r_squared", "residual_vol", "coverage"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
    for col in ["cross_section_n", "eligible_n"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df.dropna(subset=["date", "factor_name", "factor_return", "r_squared", "residual_vol"])


def factor_returns_payload(
    *,
    rows: pd.DataFrame,
    run_id: str,
    updated_at: str,
) -> list[tuple[Any, ...]]:
    if rows.empty:
        return []
    return [
        (
            str(r.date),
            str(r.factor_name),
            float(r.factor_return),
            float(r.robust_se),
            float(r.t_stat),
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


def covariance_payload(
    *,
    as_of_date: str,
    cov: pd.DataFrame,
    run_id: str,
    updated_at: str,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    if cov is None or cov.empty:
        return [], []
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
    return factors, payload


def specific_risk_payload(
    *,
    as_of_date: str,
    specific_risk_by_ticker: dict[str, dict[str, float | int | str]],
    run_id: str,
    updated_at: str,
) -> list[tuple[Any, ...]]:
    if not specific_risk_by_ticker:
        return []
    payload: list[tuple[Any, ...]] = []
    for key, row in specific_risk_by_ticker.items():
        ric = str(row.get("ric") or key or "").upper().strip()
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ric:
            continue
        payload.append(
            (
                as_of_date,
                ric,
                ticker or None,
                float(row.get("specific_var", 0.0) or 0.0),
                float(row.get("specific_vol", 0.0) or 0.0),
                int(row.get("obs", 0) or 0),
                str(row.get("trbc_business_sector") or "").strip() or None,
                run_id,
                updated_at,
            )
        )
    return payload


def quality_gate_errors(
    *,
    status: str,
    n_factor: int,
    n_cov: int,
    n_spec: int,
) -> list[str]:
    quality_errors: list[str] = []
    if str(status).lower() == "ok":
        if n_factor <= 0:
            quality_errors.append("factor_returns_empty")
        if n_cov <= 0:
            quality_errors.append("factor_covariance_empty")
        if n_spec <= 0:
            quality_errors.append("specific_risk_empty")
    return quality_errors


def metadata_values(
    *,
    run_id: str,
    refresh_mode: str,
    status: str,
    started_at: str,
    completed_at: str,
    factor_returns_asof: str,
    source_dates: dict[str, Any],
    params: dict[str, Any],
    risk_engine_state: dict[str, Any],
    row_counts: dict[str, int],
    error: dict[str, str] | None,
    updated_at: str,
) -> tuple[Any, ...]:
    return (
        run_id,
        str(refresh_mode),
        str(status),
        str(started_at),
        str(completed_at),
        factor_returns_asof,
        json.dumps(source_dates or {}, sort_keys=True),
        json.dumps(params or {}, sort_keys=True),
        json.dumps(risk_engine_state or {}, sort_keys=True),
        json.dumps(row_counts, sort_keys=True),
        (error or {}).get("type"),
        (error or {}).get("message"),
        updated_at,
    )
