"""Download data into local SQLite for dashboard analytics.

Default gatherer is LSEG (via jl-lseg-toolkit) when DATA_GATHERER=lseg.
Legacy Postgres snapshot mode remains available with DATA_GATHERER=postgres.

Legacy Postgres mode pulls full barra universe:
  - barra_exposures
  - prices_daily
  - fundamental_snapshots
"""

import os
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg
from psycopg.rows import dict_row

import config

LOCAL_DB = os.path.join(os.path.dirname(__file__), "..", "data.db")

# Columns that should be stored as REAL (not TEXT)
REAL_COLUMNS = {
    # barra_exposures
    "beta_score", "momentum_score", "size_score", "nonlinear_size_score",
    "short_term_reversal_score", "resid_vol_score", "liquidity_score",
    "book_to_price_score", "earnings_yield_score", "value_score",
    "leverage_score", "growth_score", "profitability_score",
    "investment_score", "dividend_yield_score", "idio_var_daily",
    # prices_daily
    "open", "high", "low", "close", "adj_close", "volume",
    # fundamental_snapshots
    "market_cap", "book_value", "forward_eps", "trailing_eps",
    "debt_to_equity", "total_debt", "operating_cashflow", "dividend_yield",
    "avg_volume", "shares_outstanding", "beta_yahoo", "revenue_growth",
    "earnings_growth", "return_on_equity", "return_on_assets",
    "gross_margins", "operating_margins", "profit_margins", "asset_growth",
}

BATCH_SIZE = 5000


def pg_connect():
    return psycopg.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        dbname=config.PG_DB,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        connect_timeout=10,
        row_factory=dict_row,
    )


def _col_type(col_name: str) -> str:
    return "REAL" if col_name in REAL_COLUMNS else "TEXT"


def _coerce(val, col_name: str):
    """Coerce a value for SQLite insertion."""
    if val is None:
        return None
    if col_name in REAL_COLUMNS:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
    return str(val)


def _batch_insert(local, table: str, cols: list[str], rows: list[dict], label: str):
    """Insert rows in batches with progress."""
    placeholders = ",".join("?" for _ in cols)
    sql = f"INSERT INTO {table} VALUES ({placeholders})"
    total = len(rows)
    t0 = time.time()
    for i in range(0, total, BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        local.executemany(
            sql,
            [tuple(_coerce(r[c], c) for c in cols) for r in batch],
        )
        done = min(i + BATCH_SIZE, total)
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        print(f"\r  {label}: {done:,}/{total:,} ({rate:,.0f} rows/s)", end="", flush=True)
    print()


def download():
    pg = pg_connect()
    local = sqlite3.connect(LOCAL_DB)
    local.execute("PRAGMA journal_mode=WAL")
    local.execute("PRAGMA synchronous=NORMAL")

    # ── Get barra universe tickers ────────────────────────────────────────
    print("Resolving barra universe tickers...")
    cur = pg.execute("SELECT DISTINCT ticker FROM barra_exposures ORDER BY ticker")
    barra_tickers = [r["ticker"] for r in cur.fetchall()]
    print(f"  {len(barra_tickers)} tickers in barra universe")
    placeholders = ",".join(["%s"] * len(barra_tickers))

    # ── barra_exposures (all rows) ────────────────────────────────────────
    print("Downloading barra_exposures...")
    cur = pg.execute("SELECT * FROM barra_exposures ORDER BY as_of_date, ticker")
    rows = cur.fetchall()
    if rows:
        cols = list(rows[0].keys())
        local.execute("DROP TABLE IF EXISTS barra_exposures")
        col_defs = ", ".join(f'"{c}" {_col_type(c)}' for c in cols)
        local.execute(f"CREATE TABLE barra_exposures ({col_defs})")
        _batch_insert(local, "barra_exposures", cols, rows, "barra_exposures")
        n_dates = len(set(r["as_of_date"] for r in rows))
        n_tickers = len(set(r["ticker"] for r in rows))
        print(f"  {len(rows):,} rows, {n_tickers} tickers, {n_dates} dates")

    # ── fundamental_snapshots (full barra universe) ───────────────────────
    print("Downloading fundamental_snapshots (full barra universe)...")
    cur = pg.execute(
        f"SELECT * FROM fundamental_snapshots WHERE ticker IN ({placeholders}) ORDER BY ticker, fetch_date",
        barra_tickers,
    )
    rows = cur.fetchall()
    if rows:
        cols = list(rows[0].keys())
        local.execute("DROP TABLE IF EXISTS fundamental_snapshots")
        col_defs = ", ".join(f'"{c}" {_col_type(c)}' for c in cols)
        local.execute(f"CREATE TABLE fundamental_snapshots ({col_defs})")
        _batch_insert(local, "fundamental_snapshots", cols, rows, "fundamentals")
        print(f"  {len(rows):,} rows, {len(set(r['ticker'] for r in rows))} tickers")

    # ── prices_daily (full barra universe, since 2016-01-01) ──────────────
    print("Downloading prices_daily (full barra universe)...")
    print("  This may take a few minutes for ~7M rows...")
    cur = pg.execute(
        f"""SELECT * FROM prices_daily
            WHERE ticker IN ({placeholders})
              AND date >= '2016-01-01'
            ORDER BY ticker, date""",
        barra_tickers,
    )
    rows = cur.fetchall()
    if rows:
        cols = list(rows[0].keys())
        local.execute("DROP TABLE IF EXISTS prices_daily")
        col_defs = ", ".join(f'"{c}" {_col_type(c)}' for c in cols)
        local.execute(f"CREATE TABLE prices_daily ({col_defs})")
        _batch_insert(local, "prices_daily", cols, rows, "prices")
        n_tickers = len(set(r["ticker"] for r in rows))
        print(f"  {len(rows):,} rows, {n_tickers} tickers")

    # ── Indexes ───────────────────────────────────────────────────────────
    print("Creating indexes...")
    local.execute("CREATE INDEX IF NOT EXISTS idx_exp_ticker ON barra_exposures(ticker)")
    local.execute("CREATE INDEX IF NOT EXISTS idx_exp_date ON barra_exposures(as_of_date)")
    local.execute("CREATE INDEX IF NOT EXISTS idx_fund_ticker ON fundamental_snapshots(ticker)")
    local.execute("CREATE INDEX IF NOT EXISTS idx_fund_date ON fundamental_snapshots(fetch_date)")
    local.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker ON prices_daily(ticker)")
    local.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON prices_daily(date)")
    local.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices_daily(ticker, date)")

    local.commit()
    local.close()
    pg.close()

    size_mb = os.path.getsize(LOCAL_DB) / 1024 / 1024
    print(f"\nDone! Saved to {LOCAL_DB} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    gatherer = os.getenv("DATA_GATHERER", "lseg").strip().lower()
    if gatherer == "lseg":
        try:
            from download_data_lseg import download_from_lseg
        except Exception as exc:
            print("Failed to load LSEG gatherer (download_data_lseg.py).")
            print(f"Error: {exc}")
            print("Set DATA_GATHERER=postgres to use legacy snapshot mode.")
            raise
        download_from_lseg(db_path=Path(LOCAL_DB))
    else:
        download()
