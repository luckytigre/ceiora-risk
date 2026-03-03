"""Canonical cUSE4 source and audit table schemas."""

from __future__ import annotations

import sqlite3


SECURITY_MASTER_TABLE = "security_master"
FUNDAMENTALS_HISTORY_TABLE = "security_fundamentals_pit"
TRBC_HISTORY_TABLE = "security_classification_pit"
PRICES_TABLE = "security_prices_eod"
ESTU_MEMBERSHIP_TABLE = "estu_membership_daily"


def ensure_cuse4_schema(conn: sqlite3.Connection) -> dict[str, str]:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SECURITY_MASTER_TABLE} (
            sid TEXT PRIMARY KEY,
            permid TEXT,
            ric TEXT,
            ticker TEXT,
            isin TEXT,
            instrument_type TEXT,
            asset_category_description TEXT,
            exchange_name TEXT,
            classification_ok INTEGER NOT NULL DEFAULT 0,
            is_equity_eligible INTEGER NOT NULL DEFAULT 0,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SECURITY_MASTER_TABLE}_ticker ON {SECURITY_MASTER_TABLE}(ticker)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SECURITY_MASTER_TABLE}_permid ON {SECURITY_MASTER_TABLE}(permid)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SECURITY_MASTER_TABLE}_ric ON {SECURITY_MASTER_TABLE}(ric)"
    )

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {FUNDAMENTALS_HISTORY_TABLE} (
            sid TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            stat_date TEXT NOT NULL,
            period_end_date TEXT,
            fiscal_year INTEGER,
            period_type TEXT,
            report_currency TEXT,
            market_cap REAL,
            shares_outstanding REAL,
            dividend_yield REAL,
            book_value_per_share REAL,
            total_assets REAL,
            total_debt REAL,
            cash_and_equivalents REAL,
            long_term_debt REAL,
            operating_cashflow REAL,
            capital_expenditures REAL,
            trailing_eps REAL,
            forward_eps REAL,
            revenue REAL,
            ebitda REAL,
            ebit REAL,
            roe_pct REAL,
            roa_pct REAL,
            operating_margin_pct REAL,
            common_name TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (sid, as_of_date, stat_date)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{FUNDAMENTALS_HISTORY_TABLE}_sid_asof ON {FUNDAMENTALS_HISTORY_TABLE}(sid, as_of_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{FUNDAMENTALS_HISTORY_TABLE}_asof ON {FUNDAMENTALS_HISTORY_TABLE}(as_of_date)"
    )

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TRBC_HISTORY_TABLE} (
            sid TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            trbc_economic_sector TEXT,
            trbc_business_sector TEXT,
            trbc_industry_group TEXT,
            trbc_industry TEXT,
            trbc_activity TEXT,
            hq_country_code TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (sid, as_of_date)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TRBC_HISTORY_TABLE}_sid_asof ON {TRBC_HISTORY_TABLE}(sid, as_of_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TRBC_HISTORY_TABLE}_asof ON {TRBC_HISTORY_TABLE}(as_of_date)"
    )

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PRICES_TABLE} (
            sid TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume REAL,
            currency TEXT,
            exchange TEXT,
            source TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (sid, date)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{PRICES_TABLE}_sid_date ON {PRICES_TABLE}(sid, date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{PRICES_TABLE}_date ON {PRICES_TABLE}(date)"
    )

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ESTU_MEMBERSHIP_TABLE} (
            date TEXT NOT NULL,
            sid TEXT NOT NULL,
            estu_flag INTEGER NOT NULL DEFAULT 0,
            drop_reason TEXT,
            drop_reason_detail TEXT,
            mcap REAL,
            price_close REAL,
            adv_20d REAL,
            has_required_price_history INTEGER NOT NULL DEFAULT 0,
            has_required_fundamentals INTEGER NOT NULL DEFAULT 0,
            has_required_trbc INTEGER NOT NULL DEFAULT 0,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (date, sid)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{ESTU_MEMBERSHIP_TABLE}_date_flag ON {ESTU_MEMBERSHIP_TABLE}(date, estu_flag)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{ESTU_MEMBERSHIP_TABLE}_sid_date ON {ESTU_MEMBERSHIP_TABLE}(sid, date)"
    )

    return {
        "security_master": SECURITY_MASTER_TABLE,
        "security_fundamentals_pit": FUNDAMENTALS_HISTORY_TABLE,
        "security_classification_pit": TRBC_HISTORY_TABLE,
        "security_prices_eod": PRICES_TABLE,
        "estu_membership_daily": ESTU_MEMBERSHIP_TABLE,
        # Backward-compat mapping keys for callers not yet migrated.
        "fundamentals_history": FUNDAMENTALS_HISTORY_TABLE,
        "trbc_industry_country_history": TRBC_HISTORY_TABLE,
    }
