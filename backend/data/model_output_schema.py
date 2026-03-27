"""Schema helpers for durable model-output persistence."""

from __future__ import annotations

import sqlite3


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def drop_if_columns_missing(conn: sqlite3.Connection, *, table: str, required: set[str]) -> None:
    cols = table_columns(conn, table)
    if cols and not required.issubset(cols):
        conn.execute(f"DROP TABLE IF EXISTS {table}")


def ensure_schema(conn: sqlite3.Connection) -> None:
    drop_if_columns_missing(
        conn,
        table="model_factor_returns_daily",
        required={"date", "factor_name", "factor_return", "robust_se", "t_stat", "r_squared", "residual_vol", "run_id", "updated_at"},
    )
    drop_if_columns_missing(
        conn,
        table="model_specific_risk_daily",
        required={"as_of_date", "ric", "ticker", "specific_var", "specific_vol", "obs", "trbc_business_sector", "run_id", "updated_at"},
    )
    drop_if_columns_missing(
        conn,
        table="cuse_security_membership_daily",
        required={
            "as_of_date",
            "ric",
            "ticker",
            "policy_path",
            "realized_role",
            "output_status",
            "projection_candidate_status",
            "projection_output_status",
            "reason_code",
            "quality_label",
            "source_snapshot_status",
            "projection_method",
            "projection_basis_status",
            "projection_source_package_date",
            "served_exposure_available",
            "run_id",
            "updated_at",
        },
    )
    drop_if_columns_missing(
        conn,
        table="cuse_security_stage_results_daily",
        required={"as_of_date", "ric", "stage_name", "stage_state", "reason_code", "detail_json", "run_id", "updated_at"},
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_factor_returns_daily (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL,
            robust_se REAL NOT NULL DEFAULT 0.0,
            t_stat REAL NOT NULL DEFAULT 0.0,
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
            trbc_business_sector TEXT,
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
        """
        CREATE TABLE IF NOT EXISTS cuse_security_membership_daily (
            as_of_date TEXT NOT NULL,
            ric TEXT,
            ticker TEXT NOT NULL,
            policy_path TEXT NOT NULL,
            realized_role TEXT NOT NULL,
            output_status TEXT NOT NULL,
            projection_candidate_status TEXT NOT NULL,
            projection_output_status TEXT NOT NULL,
            reason_code TEXT,
            quality_label TEXT NOT NULL,
            source_snapshot_status TEXT NOT NULL,
            projection_method TEXT,
            projection_basis_status TEXT NOT NULL,
            projection_source_package_date TEXT,
            served_exposure_available INTEGER NOT NULL DEFAULT 0,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (as_of_date, ticker)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cuse_security_stage_results_daily (
            as_of_date TEXT NOT NULL,
            ric TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            stage_state TEXT NOT NULL,
            reason_code TEXT,
            detail_json TEXT NOT NULL,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (as_of_date, ric, stage_name)
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
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cuse_security_membership_daily_date ON cuse_security_membership_daily(as_of_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cuse_security_membership_daily_ric ON cuse_security_membership_daily(ric, as_of_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cuse_security_stage_results_daily_date ON cuse_security_stage_results_daily(as_of_date, stage_name)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cuse_security_stage_results_daily_ric ON cuse_security_stage_results_daily(ric, as_of_date)"
    )
    conn.execute("DROP TABLE IF EXISTS model_specific_residuals_daily")
