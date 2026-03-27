"""Canonical cUSE4 source and audit table schemas."""

from __future__ import annotations

import sqlite3


SECURITY_MASTER_TABLE = "security_master"
SECURITY_REGISTRY_TABLE = "security_registry"
SECURITY_TAXONOMY_CURRENT_TABLE = "security_taxonomy_current"
SECURITY_POLICY_CURRENT_TABLE = "security_policy_current"
SECURITY_SOURCE_OBSERVATION_DAILY_TABLE = "security_source_observation_daily"
SECURITY_INGEST_RUNS_TABLE = "security_ingest_runs"
SECURITY_INGEST_AUDIT_TABLE = "security_ingest_audit"
SECURITY_MASTER_COMPAT_CURRENT_TABLE = "security_master_compat_current"
FUNDAMENTALS_HISTORY_TABLE = "security_fundamentals_pit"
TRBC_HISTORY_TABLE = "security_classification_pit"
PRICES_TABLE = "security_prices_eod"
ESTU_MEMBERSHIP_TABLE = "estu_membership_daily"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _pk_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    if not _table_exists(conn, table):
        return []
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows if int(r[5] or 0) > 0]


def _drop_table_if_exists(conn: sqlite3.Connection, table: str) -> None:
    if _table_exists(conn, table):
        conn.execute(f"DROP TABLE IF EXISTS {table}")


def _drop_index_if_exists(conn: sqlite3.Connection, index_name: str) -> None:
    conn.execute(f"DROP INDEX IF EXISTS {index_name}")


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if column not in _table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _create_security_master_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SECURITY_MASTER_TABLE} (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            classification_ok INTEGER NOT NULL DEFAULT 0,
            is_equity_eligible INTEGER NOT NULL DEFAULT 0,
            coverage_role TEXT NOT NULL DEFAULT 'native_equity',
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SECURITY_MASTER_TABLE}_ticker ON {SECURITY_MASTER_TABLE}(ticker)"
    )
    _drop_index_if_exists(conn, f"idx_{SECURITY_MASTER_TABLE}_permid")
    _drop_index_if_exists(conn, f"idx_{SECURITY_MASTER_TABLE}_sid")


def _create_security_registry_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SECURITY_REGISTRY_TABLE} (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            tracking_status TEXT NOT NULL DEFAULT 'active',
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SECURITY_REGISTRY_TABLE}_ticker ON {SECURITY_REGISTRY_TABLE}(ticker)"
    )


def _create_security_taxonomy_current_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SECURITY_TAXONOMY_CURRENT_TABLE} (
            ric TEXT PRIMARY KEY,
            instrument_kind TEXT,
            vehicle_structure TEXT,
            issuer_country_code TEXT,
            listing_country_code TEXT,
            model_home_market_scope TEXT,
            is_single_name_equity INTEGER NOT NULL DEFAULT 0,
            classification_ready INTEGER NOT NULL DEFAULT 0,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )


def _create_security_policy_current_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SECURITY_POLICY_CURRENT_TABLE} (
            ric TEXT PRIMARY KEY,
            price_ingest_enabled INTEGER NOT NULL DEFAULT 1,
            pit_fundamentals_enabled INTEGER NOT NULL DEFAULT 0,
            pit_classification_enabled INTEGER NOT NULL DEFAULT 0,
            allow_cuse_native_core INTEGER NOT NULL DEFAULT 0,
            allow_cuse_fundamental_projection INTEGER NOT NULL DEFAULT 0,
            allow_cuse_returns_projection INTEGER NOT NULL DEFAULT 0,
            allow_cpar_core_target INTEGER NOT NULL DEFAULT 0,
            allow_cpar_extended_target INTEGER NOT NULL DEFAULT 0,
            policy_source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )


def _create_security_source_observation_daily_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SECURITY_SOURCE_OBSERVATION_DAILY_TABLE} (
            as_of_date TEXT NOT NULL,
            ric TEXT NOT NULL,
            classification_ready INTEGER NOT NULL DEFAULT 0,
            is_equity_eligible INTEGER NOT NULL DEFAULT 0,
            price_ingest_enabled INTEGER NOT NULL DEFAULT 0,
            pit_fundamentals_enabled INTEGER NOT NULL DEFAULT 0,
            pit_classification_enabled INTEGER NOT NULL DEFAULT 0,
            has_price_history_as_of_date INTEGER NOT NULL DEFAULT 0,
            has_fundamentals_history_as_of_date INTEGER NOT NULL DEFAULT 0,
            has_classification_history_as_of_date INTEGER NOT NULL DEFAULT 0,
            latest_price_date TEXT,
            latest_fundamentals_as_of_date TEXT,
            latest_classification_as_of_date TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (as_of_date, ric)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SECURITY_SOURCE_OBSERVATION_DAILY_TABLE}_ric ON {SECURITY_SOURCE_OBSERVATION_DAILY_TABLE}(ric)"
    )
    _ensure_column(
        conn,
        SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
        "has_price_history_as_of_date",
        "INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
        "has_fundamentals_history_as_of_date",
        "INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
        "has_classification_history_as_of_date",
        "INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
        "latest_price_date",
        "TEXT",
    )
    _ensure_column(
        conn,
        SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
        "latest_fundamentals_as_of_date",
        "TEXT",
    )
    _ensure_column(
        conn,
        SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
        "latest_classification_as_of_date",
        "TEXT",
    )


def _create_security_ingest_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SECURITY_INGEST_RUNS_TABLE} (
            job_run_id TEXT PRIMARY KEY,
            source TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT,
            notes TEXT
        )
        """
    )


def _create_security_ingest_audit_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SECURITY_INGEST_AUDIT_TABLE} (
            job_run_id TEXT NOT NULL,
            ric TEXT NOT NULL,
            artifact_name TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (job_run_id, ric, artifact_name)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SECURITY_INGEST_AUDIT_TABLE}_ric ON {SECURITY_INGEST_AUDIT_TABLE}(ric)"
    )


def _create_security_master_compat_current_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SECURITY_MASTER_COMPAT_CURRENT_TABLE} (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            classification_ok INTEGER NOT NULL DEFAULT 0,
            is_equity_eligible INTEGER NOT NULL DEFAULT 0,
            coverage_role TEXT NOT NULL DEFAULT 'native_equity',
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SECURITY_MASTER_COMPAT_CURRENT_TABLE}_ticker ON {SECURITY_MASTER_COMPAT_CURRENT_TABLE}(ticker)"
    )


def _ensure_security_master_schema(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, SECURITY_MASTER_TABLE):
        _create_security_master_table(conn)
        return

    cols = _table_columns(conn, SECURITY_MASTER_TABLE)
    pk_cols = _pk_cols(conn, SECURITY_MASTER_TABLE)
    expected_cols = {
        "ric",
        "ticker",
        "isin",
        "exchange_name",
        "classification_ok",
        "is_equity_eligible",
        "coverage_role",
        "source",
        "job_run_id",
        "updated_at",
    }
    if pk_cols == ["ric"] and (expected_cols - {"coverage_role"}).issubset(cols):
        # Migrate: add coverage_role column if missing from an older schema.
        if "coverage_role" not in cols:
            conn.execute(
                f"ALTER TABLE {SECURITY_MASTER_TABLE} ADD COLUMN coverage_role TEXT NOT NULL DEFAULT 'native_equity'"
            )
        _create_security_master_table(conn)
        # Remove stale migration artifacts so index names can be reused on active table.
        _drop_table_if_exists(conn, f"{SECURITY_MASTER_TABLE}__legacy_pre_ric_pk")
        _create_security_master_table(conn)
        if {"sid", "permid", "instrument_type", "asset_category_description"} & cols:
            legacy = f"{SECURITY_MASTER_TABLE}__legacy_pre_trim"
            conn.execute(f"DROP TABLE IF EXISTS {legacy}")
            conn.execute(f"ALTER TABLE {SECURITY_MASTER_TABLE} RENAME TO {legacy}")
            _create_security_master_table(conn)
            lcols_trim = _table_columns(conn, legacy)
            coverage_role_expr_trim = (
                "COALESCE(NULLIF(TRIM(coverage_role), ''), 'native_equity')"
                if "coverage_role" in lcols_trim
                else "'native_equity'"
            )
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {SECURITY_MASTER_TABLE} (
                    ric, ticker, isin, exchange_name, classification_ok,
                    is_equity_eligible, coverage_role, source, job_run_id, updated_at
                )
                SELECT
                    UPPER(TRIM(ric)) AS ric,
                    NULLIF(UPPER(TRIM(ticker)), '') AS ticker,
                    NULLIF(TRIM(isin), '') AS isin,
                    NULLIF(TRIM(exchange_name), '') AS exchange_name,
                    COALESCE(CAST(classification_ok AS INTEGER), 0) AS classification_ok,
                    COALESCE(CAST(is_equity_eligible AS INTEGER), 0) AS is_equity_eligible,
                    {coverage_role_expr_trim} AS coverage_role,
                    NULLIF(TRIM(source), '') AS source,
                    NULLIF(TRIM(job_run_id), '') AS job_run_id,
                    COALESCE(NULLIF(TRIM(updated_at), ''), datetime('now')) AS updated_at
                FROM {legacy}
                WHERE ric IS NOT NULL AND TRIM(ric) <> ''
                """
            )
            _drop_table_if_exists(conn, legacy)
        return

    legacy = f"{SECURITY_MASTER_TABLE}__legacy_pre_ric_pk"
    conn.execute(f"DROP TABLE IF EXISTS {legacy}")
    conn.execute(f"ALTER TABLE {SECURITY_MASTER_TABLE} RENAME TO {legacy}")
    _create_security_master_table(conn)

    lcols = _table_columns(conn, legacy)
    ticker_expr = "NULL"
    if "ticker" in lcols:
        ticker_expr = "NULLIF(UPPER(TRIM(ticker)), '')"
    isin_expr = "NULL"
    if "isin" in lcols:
        isin_expr = "NULLIF(TRIM(isin), '')"
    exchange_expr = "NULL"
    if "exchange_name" in lcols:
        exchange_expr = "NULLIF(TRIM(exchange_name), '')"
    class_expr = "0"
    if "classification_ok" in lcols:
        class_expr = "COALESCE(CAST(classification_ok AS INTEGER), 0)"
    equity_expr = "0"
    if "is_equity_eligible" in lcols:
        equity_expr = "COALESCE(CAST(is_equity_eligible AS INTEGER), 0)"
    coverage_role_expr = "'native_equity'"
    if "coverage_role" in lcols:
        coverage_role_expr = "COALESCE(NULLIF(TRIM(coverage_role), ''), 'native_equity')"
    source_expr = "NULL"
    if "source" in lcols:
        source_expr = "NULLIF(TRIM(source), '')"
    job_expr = "NULL"
    if "job_run_id" in lcols:
        job_expr = "NULLIF(TRIM(job_run_id), '')"
    updated_expr = "datetime('now')"
    updated_sort_expr = "datetime('now')"
    if "updated_at" in lcols:
        updated_expr = "COALESCE(NULLIF(TRIM(updated_at), ''), datetime('now'))"
        updated_sort_expr = "COALESCE(NULLIF(TRIM(updated_at), ''), datetime('now'))"

    conn.execute(
        f"""
        INSERT OR REPLACE INTO {SECURITY_MASTER_TABLE} (
            ric, ticker, isin, exchange_name, classification_ok,
            is_equity_eligible, coverage_role, source, job_run_id, updated_at
        )
        SELECT
            ric, ticker, isin, exchange_name, classification_ok,
            is_equity_eligible, coverage_role, source, job_run_id, updated_at
        FROM (
            SELECT
                UPPER(TRIM(ric)) AS ric,
                {ticker_expr} AS ticker,
                {isin_expr} AS isin,
                {exchange_expr} AS exchange_name,
                {class_expr} AS classification_ok,
                {equity_expr} AS is_equity_eligible,
                {coverage_role_expr} AS coverage_role,
                {source_expr} AS source,
                {job_expr} AS job_run_id,
                {updated_expr} AS updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(TRIM(ric))
                    ORDER BY {updated_sort_expr} DESC, rowid DESC
                ) AS rn
            FROM {legacy}
            WHERE ric IS NOT NULL AND TRIM(ric) <> ''
        ) ranked
        WHERE rn = 1
        """
    )
    _drop_table_if_exists(conn, legacy)
    _create_security_master_table(conn)


def _create_prices_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PRICES_TABLE} (
            ric TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume REAL,
            currency TEXT,
            source TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (ric, date)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{PRICES_TABLE}_date ON {PRICES_TABLE}(date)"
    )


def _ensure_prices_schema(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, PRICES_TABLE):
        _create_prices_table(conn)
        return

    cols = _table_columns(conn, PRICES_TABLE)
    pk_cols = _pk_cols(conn, PRICES_TABLE)
    expected_cols = {
        "ric",
        "date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "currency",
        "source",
        "updated_at",
    }
    if pk_cols == ["ric", "date"] and expected_cols.issubset(cols) and "exchange" not in cols:
        _create_prices_table(conn)
        return

    legacy = f"{PRICES_TABLE}__legacy_pre_no_exchange"
    conn.execute(f"DROP TABLE IF EXISTS {legacy}")
    conn.execute(f"ALTER TABLE {PRICES_TABLE} RENAME TO {legacy}")
    _create_prices_table(conn)

    lcols = _table_columns(conn, legacy)
    if "ric" not in lcols or "date" not in lcols:
        conn.execute(f"DROP TABLE IF EXISTS {legacy}")
        return

    open_expr = "CAST(open AS REAL)" if "open" in lcols else "NULL"
    high_expr = "CAST(high AS REAL)" if "high" in lcols else "NULL"
    low_expr = "CAST(low AS REAL)" if "low" in lcols else "NULL"
    close_expr = "CAST(close AS REAL)" if "close" in lcols else "NULL"
    adj_expr = "CAST(adj_close AS REAL)" if "adj_close" in lcols else close_expr
    volume_expr = "CAST(volume AS REAL)" if "volume" in lcols else "NULL"
    ccy_expr = "NULLIF(TRIM(currency), '')" if "currency" in lcols else "NULL"
    source_expr = "NULLIF(TRIM(source), '')" if "source" in lcols else "NULL"
    updated_expr = "datetime('now')"
    updated_sort_expr = "datetime('now')"
    if "updated_at" in lcols:
        updated_expr = "COALESCE(NULLIF(TRIM(updated_at), ''), datetime('now'))"
        updated_sort_expr = "COALESCE(NULLIF(TRIM(updated_at), ''), datetime('now'))"

    conn.execute(
        f"""
        INSERT OR REPLACE INTO {PRICES_TABLE} (
            ric, date, open, high, low, close, adj_close, volume, currency, source, updated_at
        )
        SELECT
            ric, date, open, high, low, close, adj_close, volume, currency, source, updated_at
        FROM (
            SELECT
                UPPER(TRIM(ric)) AS ric,
                TRIM(date) AS date,
                {open_expr} AS open,
                {high_expr} AS high,
                {low_expr} AS low,
                {close_expr} AS close,
                {adj_expr} AS adj_close,
                {volume_expr} AS volume,
                {ccy_expr} AS currency,
                {source_expr} AS source,
                {updated_expr} AS updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(TRIM(ric)), TRIM(date)
                    ORDER BY {updated_sort_expr} DESC, rowid DESC
                ) AS rn
            FROM {legacy}
            WHERE ric IS NOT NULL AND TRIM(ric) <> ''
              AND date IS NOT NULL AND TRIM(date) <> ''
        ) ranked
        WHERE rn = 1
        """
    )
    conn.execute(f"DROP TABLE IF EXISTS {legacy}")


def ensure_cuse4_schema(conn: sqlite3.Connection) -> dict[str, str]:
    _ensure_security_master_schema(conn)
    _create_security_registry_table(conn)
    _create_security_taxonomy_current_table(conn)
    _create_security_policy_current_table(conn)
    _create_security_source_observation_daily_table(conn)
    _create_security_ingest_runs_table(conn)
    _create_security_ingest_audit_table(conn)
    _create_security_master_compat_current_table(conn)

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {FUNDAMENTALS_HISTORY_TABLE} (
            ric TEXT NOT NULL,
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
            operating_margin_pct REAL,
            common_name TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (ric, as_of_date, stat_date)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{FUNDAMENTALS_HISTORY_TABLE}_asof ON {FUNDAMENTALS_HISTORY_TABLE}(as_of_date)"
    )

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TRBC_HISTORY_TABLE} (
            ric TEXT NOT NULL,
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
            PRIMARY KEY (ric, as_of_date)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TRBC_HISTORY_TABLE}_asof ON {TRBC_HISTORY_TABLE}(as_of_date)"
    )

    _ensure_prices_schema(conn)

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ESTU_MEMBERSHIP_TABLE} (
            date TEXT NOT NULL,
            ric TEXT NOT NULL,
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
            PRIMARY KEY (date, ric)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{ESTU_MEMBERSHIP_TABLE}_date_flag ON {ESTU_MEMBERSHIP_TABLE}(date, estu_flag)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{ESTU_MEMBERSHIP_TABLE}_ric_date ON {ESTU_MEMBERSHIP_TABLE}(ric, date)"
    )

    # Drop redundant indexes where PRIMARY KEY already covers the same key pattern.
    _drop_index_if_exists(conn, "idx_security_prices_eod_ric_date")
    _drop_index_if_exists(conn, "idx_security_fundamentals_pit_ric_asof")
    _drop_index_if_exists(conn, "idx_security_classification_pit_ric_asof")
    _drop_index_if_exists(conn, "idx_barra_raw_cross_section_history_ric")

    return {
        "security_master": SECURITY_MASTER_TABLE,
        "security_registry": SECURITY_REGISTRY_TABLE,
        "security_taxonomy_current": SECURITY_TAXONOMY_CURRENT_TABLE,
        "security_policy_current": SECURITY_POLICY_CURRENT_TABLE,
        "security_source_observation_daily": SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
        "security_ingest_runs": SECURITY_INGEST_RUNS_TABLE,
        "security_ingest_audit": SECURITY_INGEST_AUDIT_TABLE,
        "security_master_compat_current": SECURITY_MASTER_COMPAT_CURRENT_TABLE,
        "security_fundamentals_pit": FUNDAMENTALS_HISTORY_TABLE,
        "security_classification_pit": TRBC_HISTORY_TABLE,
        "security_prices_eod": PRICES_TABLE,
        "estu_membership_daily": ESTU_MEMBERSHIP_TABLE,
    }
