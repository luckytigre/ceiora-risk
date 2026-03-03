"""Bootstrap cUSE4 canonical source tables from legacy local tables."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any

from cuse4.schema import (
    FUNDAMENTALS_HISTORY_TABLE,
    SECURITY_MASTER_TABLE,
    TRBC_HISTORY_TABLE,
    ensure_cuse4_schema,
)


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


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null", "<na>"}:
        return None
    return s


def _is_real_permid(permid: str | None) -> bool:
    p = (_norm_text(permid) or "").upper()
    if not p:
        return False
    return not p.startswith("RIC::")


def _sid_from_identity(permid: str | None, ric: str | None, ticker: str | None) -> str:
    p = _norm_text(permid)
    r = _norm_text(ric)
    t = (_norm_text(ticker) or "UNKNOWN").upper()
    if p:
        return f"PERMID::{p.upper()}" if _is_real_permid(p) else f"SYN::{p.upper()}"
    if r:
        return f"RIC::{r.upper()}"
    return f"TICKER::{t}"


def _distinct_tickers(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    rows = conn.execute(
        f"""
        SELECT DISTINCT UPPER(ticker)
        FROM {table}
        WHERE ticker IS NOT NULL AND TRIM(ticker) <> ''
        """
    ).fetchall()
    return {str(r[0]).upper() for r in rows if r and r[0]}


def _load_latest_universe_rows(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not _table_exists(conn, "universe_eligibility_summary"):
        return {}
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                UPPER(ticker) AS ticker,
                permid,
                current_ric,
                common_name,
                exchange_name,
                updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(ticker)
                    ORDER BY
                        COALESCE(in_current_snapshot, 0) DESC,
                        COALESCE(current_snapshot_date, '') DESC,
                        COALESCE(updated_at, '') DESC,
                        rowid DESC
                ) AS rn
            FROM universe_eligibility_summary
            WHERE ticker IS NOT NULL AND TRIM(ticker) <> ''
        )
        SELECT ticker, permid, current_ric, common_name, exchange_name, updated_at
        FROM ranked
        WHERE rn = 1
        """
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for ticker, permid, current_ric, common_name, exchange_name, updated_at in rows:
        out[str(ticker).upper()] = {
            "permid": permid,
            "current_ric": current_ric,
            "common_name": common_name,
            "exchange_name": exchange_name,
            "updated_at": updated_at,
        }
    return out


def _load_ric_map_rows(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not _table_exists(conn, "ticker_ric_map"):
        return {}
    rows = conn.execute(
        """
        SELECT UPPER(ticker) AS ticker, ric, classification_ok, updated_at
        FROM ticker_ric_map
        WHERE ticker IS NOT NULL AND TRIM(ticker) <> ''
        """
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for ticker, ric, classification_ok, updated_at in rows:
        out[str(ticker).upper()] = {
            "ric": ric,
            "classification_ok": int(classification_ok or 0),
            "updated_at": updated_at,
        }
    return out


def _replace_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    replace_all: bool,
) -> int:
    if replace_all:
        conn.execute(f"DELETE FROM {table}")
    if not rows:
        return 0
    placeholders = ",".join("?" for _ in columns)
    conn.executemany(
        f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
        rows,
    )
    return int(len(rows))


def _build_security_master_rows(conn: sqlite3.Connection, *, now_iso: str, job_run_id: str) -> list[tuple[Any, ...]]:
    tickers = set()
    for table in [
        "ticker_ric_map",
        "universe_eligibility_summary",
        "fundamental_snapshots",
        "trbc_industry_history",
        "prices_daily",
    ]:
        tickers.update(_distinct_tickers(conn, table))

    universe_rows = _load_latest_universe_rows(conn)
    ric_rows = _load_ric_map_rows(conn)

    payload: list[tuple[Any, ...]] = []
    for ticker in sorted(tickers):
        u = universe_rows.get(ticker, {})
        r = ric_rows.get(ticker, {})
        permid = _norm_text(u.get("permid"))
        ric = _norm_text(r.get("ric")) or _norm_text(u.get("current_ric"))
        classification_ok = int(r.get("classification_ok") or 0)
        is_equity_eligible = int(classification_ok == 1 and _is_real_permid(permid))
        sid = _sid_from_identity(permid, ric, ticker)

        payload.append(
            (
                sid,
                permid,
                ric,
                ticker,
                None,
                None,
                None,
                _norm_text(u.get("exchange_name")),
                classification_ok,
                is_equity_eligible,
                "cuse4_bootstrap_legacy",
                job_run_id,
                _norm_text(u.get("updated_at")) or _norm_text(r.get("updated_at")) or now_iso,
            )
        )
    return payload


def _expr_text(alias: str, column: str, columns: set[str], fallback: str = "NULL") -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _expr_real(alias: str, column: str, columns: set[str], fallback: str = "NULL") -> str:
    if column not in columns:
        return fallback
    return f"CAST({alias}.{column} AS REAL)"


def _expr_int(alias: str, column: str, columns: set[str], fallback: str = "NULL") -> str:
    if column not in columns:
        return fallback
    return f"CAST({alias}.{column} AS INTEGER)"


def _bootstrap_fundamentals_history(
    conn: sqlite3.Connection,
    *,
    replace_all: bool,
    now_iso: str,
    job_run_id: str,
) -> int:
    if not _table_exists(conn, "fundamental_snapshots"):
        if replace_all:
            conn.execute(f"DELETE FROM {FUNDAMENTALS_HISTORY_TABLE}")
        return 0

    if replace_all:
        conn.execute(f"DELETE FROM {FUNDAMENTALS_HISTORY_TABLE}")

    cols = _table_columns(conn, "fundamental_snapshots")
    fetch_date = _expr_text("f", "fetch_date", cols)
    period_end = _expr_text("f", "fundamental_period_end_date", cols)
    source_expr = _expr_text("f", "source", cols)
    job_expr = _expr_text("f", "job_run_id", cols)
    updated_expr = _expr_text("f", "updated_at", cols)

    total_assets = _expr_real("f", "total_assets", cols)
    net_income = _expr_real("f", "net_income", cols)

    sql = f"""
    INSERT OR REPLACE INTO {FUNDAMENTALS_HISTORY_TABLE} (
        sid, as_of_date, stat_date, period_end_date, fiscal_year, period_type, report_currency,
        market_cap, shares_outstanding, dividend_yield, book_value_per_share, total_assets,
        total_debt, cash_and_equivalents, long_term_debt, operating_cashflow, capital_expenditures,
        trailing_eps, forward_eps, revenue, ebitda, ebit, roe_pct, roa_pct,
        operating_margin_pct, common_name, source, job_run_id, updated_at
    )
    SELECT
        sm.sid,
        TRIM({fetch_date}) AS as_of_date,
        COALESCE(NULLIF(TRIM({period_end}), ''), TRIM({fetch_date})) AS stat_date,
        NULLIF(TRIM({period_end}), '') AS period_end_date,
        {_expr_int('f', 'fiscal_year', cols)},
        {_expr_text('f', 'period_type', cols)},
        {_expr_text('f', 'report_currency', cols)},
        {_expr_real('f', 'market_cap', cols)},
        {_expr_real('f', 'shares_outstanding', cols)},
        {_expr_real('f', 'dividend_yield', cols)},
        {_expr_real('f', 'book_value', cols)},
        {total_assets},
        {_expr_real('f', 'total_debt', cols)},
        {_expr_real('f', 'cash_and_equivalents', cols)},
        {_expr_real('f', 'long_term_debt', cols)},
        {_expr_real('f', 'operating_cashflow', cols)},
        {_expr_real('f', 'capital_expenditures', cols)},
        {_expr_real('f', 'trailing_eps', cols)},
        {_expr_real('f', 'forward_eps', cols)},
        {_expr_real('f', 'revenue', cols)},
        {_expr_real('f', 'ebitda', cols)},
        {_expr_real('f', 'ebit', cols)},
        {_expr_real('f', 'return_on_equity', cols)},
        CASE
            WHEN {total_assets} IS NOT NULL
             AND ABS({total_assets}) > 1e-12
             AND {net_income} IS NOT NULL
            THEN {net_income} / {total_assets}
            ELSE NULL
        END AS roa_pct,
        {_expr_real('f', 'operating_margins', cols)},
        {_expr_text('f', 'common_name', cols)},
        COALESCE(NULLIF(TRIM({source_expr}), ''), ?),
        COALESCE(NULLIF(TRIM({job_expr}), ''), ?),
        COALESCE(NULLIF(TRIM({updated_expr}), ''), ?)
    FROM fundamental_snapshots f
    JOIN {SECURITY_MASTER_TABLE} sm
      ON UPPER(TRIM(f.ticker)) = sm.ticker
    WHERE {fetch_date} IS NOT NULL
      AND TRIM({fetch_date}) <> ''
    """
    conn.execute(sql, ("legacy_fundamental_snapshots", job_run_id, now_iso))
    row = conn.execute("SELECT changes()").fetchone()
    return int(row[0] or 0) if row else 0


def _bootstrap_trbc_history(
    conn: sqlite3.Connection,
    *,
    replace_all: bool,
    now_iso: str,
    job_run_id: str,
) -> int:
    if not _table_exists(conn, "trbc_industry_history"):
        if replace_all:
            conn.execute(f"DELETE FROM {TRBC_HISTORY_TABLE}")
        return 0

    if replace_all:
        conn.execute(f"DELETE FROM {TRBC_HISTORY_TABLE}")

    cols = _table_columns(conn, "trbc_industry_history")
    as_of_date = _expr_text("h", "as_of_date", cols)
    source_expr = _expr_text("h", "source", cols)
    job_expr = _expr_text("h", "job_run_id", cols)
    updated_expr = _expr_text("h", "updated_at", cols)

    hq_country = _expr_text("h", "hq_country_code", cols)
    fallback_country = (
        "CASE "
        "WHEN sm.ric LIKE '%.N' OR sm.ric LIKE '%.O' OR sm.ric LIKE '%.A' "
        "  OR sm.ric LIKE '%.K' OR sm.ric LIKE '%.P' OR sm.ric LIKE '%.PK' OR sm.ric LIKE '%.Q' "
        "THEN 'US' ELSE NULL END"
    )

    sql = f"""
    INSERT OR REPLACE INTO {TRBC_HISTORY_TABLE} (
        sid, as_of_date, trbc_economic_sector, trbc_business_sector,
        trbc_industry_group, trbc_industry, trbc_activity, hq_country_code,
        source, job_run_id, updated_at
    )
    SELECT
        sm.sid,
        TRIM({as_of_date}) AS as_of_date,
        {_expr_text('h', 'trbc_economic_sector', cols)},
        {_expr_text('h', 'trbc_business_sector', cols)},
        {_expr_text('h', 'trbc_industry_group', cols)},
        {_expr_text('h', 'trbc_industry', cols)},
        {_expr_text('h', 'trbc_activity', cols)},
        COALESCE(NULLIF(UPPER(TRIM({hq_country})), ''), {fallback_country}) AS hq_country_code,
        COALESCE(NULLIF(TRIM({source_expr}), ''), ?),
        COALESCE(NULLIF(TRIM({job_expr}), ''), ?),
        COALESCE(NULLIF(TRIM({updated_expr}), ''), ?)
    FROM trbc_industry_history h
    JOIN {SECURITY_MASTER_TABLE} sm
      ON UPPER(TRIM(h.ticker)) = sm.ticker
    WHERE {as_of_date} IS NOT NULL
      AND TRIM({as_of_date}) <> ''
    """
    conn.execute(sql, ("legacy_trbc_industry_history", job_run_id, now_iso))
    row = conn.execute("SELECT changes()").fetchone()
    return int(row[0] or 0) if row else 0


def bootstrap_cuse4_source_tables(
    *,
    db_path: Path,
    replace_all: bool = True,
) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    job_run_id = f"cuse4_bootstrap_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")

    try:
        ensure_cuse4_schema(conn)

        security_rows = _build_security_master_rows(conn, now_iso=now_iso, job_run_id=job_run_id)
        n_security = _replace_rows(
            conn,
            table=SECURITY_MASTER_TABLE,
            columns=[
                "sid",
                "permid",
                "ric",
                "ticker",
                "isin",
                "instrument_type",
                "asset_category_description",
                "exchange_name",
                "classification_ok",
                "is_equity_eligible",
                "source",
                "job_run_id",
                "updated_at",
            ],
            rows=security_rows,
            replace_all=replace_all,
        )

        n_fundamentals = _bootstrap_fundamentals_history(
            conn,
            replace_all=replace_all,
            now_iso=now_iso,
            job_run_id=job_run_id,
        )
        n_trbc = _bootstrap_trbc_history(
            conn,
            replace_all=replace_all,
            now_iso=now_iso,
            job_run_id=job_run_id,
        )

        conn.commit()
        return {
            "status": "ok",
            "db_path": str(db_path),
            "replace_all": bool(replace_all),
            "job_run_id": job_run_id,
            "security_master_rows": n_security,
            "fundamentals_history_rows": n_fundamentals,
            "trbc_industry_country_history_rows": n_trbc,
        }
    finally:
        conn.close()
