"""Schema helpers for durable cPAR persistence."""

from __future__ import annotations

import sqlite3
from pathlib import Path

TABLE_PACKAGE_RUNS = "cpar_package_runs"
TABLE_PROXY_RETURNS = "cpar_proxy_returns_weekly"
TABLE_PROXY_TRANSFORM = "cpar_proxy_transform_weekly"
TABLE_FACTOR_COVARIANCE = "cpar_factor_covariance_weekly"
TABLE_INSTRUMENT_FITS = "cpar_instrument_fits_weekly"
TABLE_PACKAGE_UNIVERSE_MEMBERSHIP = "cpar_package_universe_membership"
TABLE_RUNTIME_COVERAGE = "cpar_instrument_runtime_coverage_weekly"
TABLES = (
    TABLE_PACKAGE_RUNS,
    TABLE_PROXY_RETURNS,
    TABLE_PROXY_TRANSFORM,
    TABLE_FACTOR_COVARIANCE,
    TABLE_INSTRUMENT_FITS,
    TABLE_PACKAGE_UNIVERSE_MEMBERSHIP,
    TABLE_RUNTIME_COVERAGE,
)

_POSTGRES_SCHEMA_SQL = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "reference"
    / "migrations"
    / "neon"
    / "NEON_CPAR_SCHEMA.sql"
)


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def table_primary_key_columns(conn: sqlite3.Connection, table: str) -> tuple[str, ...]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    keyed = sorted((int(row[5]), str(row[1])) for row in rows if int(row[5] or 0) > 0)
    return tuple(name for _, name in keyed)


def drop_if_schema_mismatch(
    conn: sqlite3.Connection,
    *,
    table: str,
    required_columns: set[str],
    required_primary_key: tuple[str, ...],
) -> None:
    cols = table_columns(conn, table)
    if not cols:
        return
    primary_key = table_primary_key_columns(conn, table)
    if not required_columns.issubset(cols) or primary_key != required_primary_key:
        conn.execute(f"DROP TABLE IF EXISTS {table}")


def ensure_sqlite_schema(conn: sqlite3.Connection) -> None:
    drop_if_schema_mismatch(
        conn,
        table=TABLE_PACKAGE_RUNS,
        required_columns={
            "package_run_id",
            "package_date",
            "profile",
            "status",
            "started_at",
            "completed_at",
            "method_version",
            "factor_registry_version",
            "lookback_weeks",
            "half_life_weeks",
            "min_observations",
            "proxy_price_rule",
            "source_prices_asof",
            "classification_asof",
            "universe_count",
            "fit_ok_count",
            "fit_limited_count",
            "fit_insufficient_count",
            "data_authority",
            "error_type",
            "error_message",
            "updated_at",
        },
        required_primary_key=("package_run_id",),
    )
    drop_if_schema_mismatch(
        conn,
        table=TABLE_PROXY_RETURNS,
        required_columns={
            "package_run_id",
            "package_date",
            "week_end",
            "factor_id",
            "factor_group",
            "proxy_ric",
            "proxy_ticker",
            "return_value",
            "weight_value",
            "price_field_used",
            "updated_at",
        },
        required_primary_key=("package_run_id", "week_end", "factor_id"),
    )
    drop_if_schema_mismatch(
        conn,
        table=TABLE_PROXY_TRANSFORM,
        required_columns={
            "package_run_id",
            "package_date",
            "factor_id",
            "factor_group",
            "proxy_ric",
            "proxy_ticker",
            "market_alpha",
            "market_beta",
            "updated_at",
        },
        required_primary_key=("package_run_id", "factor_id"),
    )
    drop_if_schema_mismatch(
        conn,
        table=TABLE_FACTOR_COVARIANCE,
        required_columns={
            "package_run_id",
            "package_date",
            "factor_id",
            "factor_id_2",
            "covariance",
            "correlation",
            "updated_at",
        },
        required_primary_key=("package_run_id", "factor_id", "factor_id_2"),
    )
    drop_if_schema_mismatch(
        conn,
        table=TABLE_INSTRUMENT_FITS,
        required_columns={
            "package_run_id",
            "package_date",
            "ric",
            "ticker",
            "display_name",
            "fit_status",
            "warnings_json",
            "observed_weeks",
            "lookback_weeks",
            "longest_gap_weeks",
            "price_field_used",
            "hq_country_code",
            "market_step_alpha",
            "market_step_beta",
            "block_alpha",
            "spy_trade_beta_raw",
            "raw_loadings_json",
            "thresholded_loadings_json",
            "factor_variance_proxy",
            "factor_volatility_proxy",
            "specific_variance_proxy",
            "specific_volatility_proxy",
            "updated_at",
        },
        required_primary_key=("package_run_id", "ric"),
    )
    drop_if_schema_mismatch(
        conn,
        table=TABLE_PACKAGE_UNIVERSE_MEMBERSHIP,
        required_columns={
            "package_run_id",
            "package_date",
            "ric",
            "ticker",
            "universe_scope",
            "target_scope",
            "basis_role",
            "build_reason_code",
            "warnings_json",
            "updated_at",
        },
        required_primary_key=("package_run_id", "ric"),
    )
    drop_if_schema_mismatch(
        conn,
        table=TABLE_RUNTIME_COVERAGE,
        required_columns={
            "package_run_id",
            "package_date",
            "ric",
            "ticker",
            "price_on_package_date_status",
            "fit_row_status",
            "fit_quality_status",
            "portfolio_use_status",
            "ticker_detail_use_status",
            "hedge_use_status",
            "fit_family",
            "fit_status",
            "reason_code",
            "quality_label",
            "warnings_json",
            "updated_at",
        },
        required_primary_key=("package_run_id", "ric"),
    )

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PACKAGE_RUNS} (
            package_run_id TEXT PRIMARY KEY,
            package_date TEXT NOT NULL,
            profile TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            method_version TEXT NOT NULL,
            factor_registry_version TEXT NOT NULL,
            lookback_weeks INTEGER NOT NULL,
            half_life_weeks INTEGER NOT NULL,
            min_observations INTEGER NOT NULL,
            proxy_price_rule TEXT NOT NULL,
            source_prices_asof TEXT,
            classification_asof TEXT,
            universe_count INTEGER NOT NULL DEFAULT 0,
            fit_ok_count INTEGER NOT NULL DEFAULT 0,
            fit_limited_count INTEGER NOT NULL DEFAULT 0,
            fit_insufficient_count INTEGER NOT NULL DEFAULT 0,
            data_authority TEXT NOT NULL,
            error_type TEXT,
            error_message TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PROXY_RETURNS} (
            package_run_id TEXT NOT NULL,
            package_date TEXT NOT NULL,
            week_end TEXT NOT NULL,
            factor_id TEXT NOT NULL,
            factor_group TEXT NOT NULL,
            proxy_ric TEXT NOT NULL,
            proxy_ticker TEXT NOT NULL,
            return_value REAL NOT NULL,
            weight_value REAL NOT NULL,
            price_field_used TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (package_run_id, week_end, factor_id)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PROXY_TRANSFORM} (
            package_run_id TEXT NOT NULL,
            package_date TEXT NOT NULL,
            factor_id TEXT NOT NULL,
            factor_group TEXT NOT NULL,
            proxy_ric TEXT NOT NULL,
            proxy_ticker TEXT NOT NULL,
            market_alpha REAL NOT NULL,
            market_beta REAL NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (package_run_id, factor_id)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_FACTOR_COVARIANCE} (
            package_run_id TEXT NOT NULL,
            package_date TEXT NOT NULL,
            factor_id TEXT NOT NULL,
            factor_id_2 TEXT NOT NULL,
            covariance REAL NOT NULL,
            correlation REAL NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (package_run_id, factor_id, factor_id_2)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_INSTRUMENT_FITS} (
            package_run_id TEXT NOT NULL,
            package_date TEXT NOT NULL,
            ric TEXT NOT NULL,
            ticker TEXT,
            display_name TEXT,
            fit_status TEXT NOT NULL,
            warnings_json TEXT NOT NULL,
            observed_weeks INTEGER NOT NULL,
            lookback_weeks INTEGER NOT NULL,
            longest_gap_weeks INTEGER NOT NULL,
            price_field_used TEXT NOT NULL,
            hq_country_code TEXT,
            market_step_alpha REAL,
            market_step_beta REAL,
            block_alpha REAL,
            spy_trade_beta_raw REAL,
            raw_loadings_json TEXT NOT NULL,
            thresholded_loadings_json TEXT NOT NULL,
            factor_variance_proxy REAL,
            factor_volatility_proxy REAL,
            specific_variance_proxy REAL,
            specific_volatility_proxy REAL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (package_run_id, ric)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PACKAGE_UNIVERSE_MEMBERSHIP} (
            package_run_id TEXT NOT NULL,
            package_date TEXT NOT NULL,
            ric TEXT NOT NULL,
            ticker TEXT,
            universe_scope TEXT NOT NULL,
            target_scope TEXT NOT NULL,
            basis_role TEXT NOT NULL,
            build_reason_code TEXT,
            warnings_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (package_run_id, ric)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_RUNTIME_COVERAGE} (
            package_run_id TEXT NOT NULL,
            package_date TEXT NOT NULL,
            ric TEXT NOT NULL,
            ticker TEXT,
            price_on_package_date_status TEXT NOT NULL,
            fit_row_status TEXT NOT NULL,
            fit_quality_status TEXT NOT NULL,
            portfolio_use_status TEXT NOT NULL,
            ticker_detail_use_status TEXT NOT NULL,
            hedge_use_status TEXT NOT NULL,
            fit_family TEXT NOT NULL,
            fit_status TEXT NOT NULL,
            reason_code TEXT,
            quality_label TEXT NOT NULL,
            warnings_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (package_run_id, ric)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PACKAGE_RUNS}_date_status ON {TABLE_PACKAGE_RUNS}(package_date, status)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PACKAGE_RUNS}_completed ON {TABLE_PACKAGE_RUNS}(completed_at)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PROXY_RETURNS}_package_run ON {TABLE_PROXY_RETURNS}(package_run_id)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PROXY_RETURNS}_package_date ON {TABLE_PROXY_RETURNS}(package_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PROXY_RETURNS}_factor ON {TABLE_PROXY_RETURNS}(factor_id)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PROXY_TRANSFORM}_package_run ON {TABLE_PROXY_TRANSFORM}(package_run_id)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PROXY_TRANSFORM}_package_date ON {TABLE_PROXY_TRANSFORM}(package_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_FACTOR_COVARIANCE}_package_run ON {TABLE_FACTOR_COVARIANCE}(package_run_id)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_FACTOR_COVARIANCE}_package_date ON {TABLE_FACTOR_COVARIANCE}(package_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_FACTOR_COVARIANCE}_factor ON {TABLE_FACTOR_COVARIANCE}(factor_id)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_INSTRUMENT_FITS}_package_run ON {TABLE_INSTRUMENT_FITS}(package_run_id)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_INSTRUMENT_FITS}_package_date ON {TABLE_INSTRUMENT_FITS}(package_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_INSTRUMENT_FITS}_ticker ON {TABLE_INSTRUMENT_FITS}(ticker)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_INSTRUMENT_FITS}_status ON {TABLE_INSTRUMENT_FITS}(fit_status)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}_package_run "
        f"ON {TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}(package_run_id)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}_package_date "
        f"ON {TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}(package_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}_ticker "
        f"ON {TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}(ticker)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_RUNTIME_COVERAGE}_package_run "
        f"ON {TABLE_RUNTIME_COVERAGE}(package_run_id)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_RUNTIME_COVERAGE}_package_date "
        f"ON {TABLE_RUNTIME_COVERAGE}(package_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_RUNTIME_COVERAGE}_portfolio_use "
        f"ON {TABLE_RUNTIME_COVERAGE}(portfolio_use_status)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_RUNTIME_COVERAGE}_ticker "
        f"ON {TABLE_RUNTIME_COVERAGE}(ticker)"
    )


def postgres_schema_sql() -> str:
    return _POSTGRES_SCHEMA_SQL.read_text(encoding="utf-8") + "\n" + _additional_postgres_schema_sql()


def _additional_postgres_schema_sql() -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {TABLE_PACKAGE_UNIVERSE_MEMBERSHIP} (
    package_run_id TEXT NOT NULL,
    package_date DATE NOT NULL,
    ric TEXT NOT NULL,
    ticker TEXT,
    universe_scope TEXT NOT NULL,
    target_scope TEXT NOT NULL,
    basis_role TEXT NOT NULL,
    build_reason_code TEXT,
    warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_run_id, ric)
);

CREATE INDEX IF NOT EXISTS idx_{TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}_package_run
ON {TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}(package_run_id);
CREATE INDEX IF NOT EXISTS idx_{TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}_package_date
ON {TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}(package_date);
CREATE INDEX IF NOT EXISTS idx_{TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}_ticker
ON {TABLE_PACKAGE_UNIVERSE_MEMBERSHIP}(ticker);

CREATE TABLE IF NOT EXISTS {TABLE_RUNTIME_COVERAGE} (
    package_run_id TEXT NOT NULL,
    package_date DATE NOT NULL,
    ric TEXT NOT NULL,
    ticker TEXT,
    price_on_package_date_status TEXT NOT NULL,
    fit_row_status TEXT NOT NULL,
    fit_quality_status TEXT NOT NULL,
    portfolio_use_status TEXT NOT NULL,
    ticker_detail_use_status TEXT NOT NULL,
    hedge_use_status TEXT NOT NULL,
    fit_family TEXT NOT NULL,
    fit_status TEXT NOT NULL,
    reason_code TEXT,
    quality_label TEXT NOT NULL,
    warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_run_id, ric)
);

CREATE INDEX IF NOT EXISTS idx_{TABLE_RUNTIME_COVERAGE}_package_run
ON {TABLE_RUNTIME_COVERAGE}(package_run_id);
CREATE INDEX IF NOT EXISTS idx_{TABLE_RUNTIME_COVERAGE}_package_date
ON {TABLE_RUNTIME_COVERAGE}(package_date);
CREATE INDEX IF NOT EXISTS idx_{TABLE_RUNTIME_COVERAGE}_portfolio_use
ON {TABLE_RUNTIME_COVERAGE}(portfolio_use_status);
CREATE INDEX IF NOT EXISTS idx_{TABLE_RUNTIME_COVERAGE}_ticker
ON {TABLE_RUNTIME_COVERAGE}(ticker);
"""


def ensure_postgres_schema(pg_conn) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(postgres_schema_sql())
