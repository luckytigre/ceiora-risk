"""SQLite and Postgres writers for durable model-output persistence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.data.model_output_schema import ensure_schema

_CANONICAL_SCHEMA_SQL = (
    Path(__file__).resolve().parents[2] / "docs" / "migrations" / "neon" / "NEON_CANONICAL_SCHEMA.sql"
)


def ensure_postgres_schema(pg_conn) -> None:
    script = _CANONICAL_SCHEMA_SQL.read_text(encoding="utf-8")
    with pg_conn.cursor() as cur:
        cur.execute(script)


def write_model_outputs_sqlite(
    conn,
    *,
    factor_returns_payload: list[tuple[Any, ...]],
    factor_returns_min_date: str | None,
    covariance_factors: list[str],
    covariance_payload: list[tuple[Any, ...]],
    specific_risk_payload: list[tuple[Any, ...]],
    metadata_values: tuple[Any, ...],
    as_of_date: str,
) -> dict[str, Any]:
    ensure_schema(conn)
    if factor_returns_payload and factor_returns_min_date:
        conn.execute("DELETE FROM model_factor_returns_daily WHERE date >= ?", (factor_returns_min_date,))
        conn.executemany(
            """
            INSERT OR REPLACE INTO model_factor_returns_daily (
                date, factor_name, factor_return, robust_se, t_stat, r_squared, residual_vol,
                cross_section_n, eligible_n, coverage, run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            factor_returns_payload,
        )

    if covariance_payload:
        if covariance_factors:
            placeholders = ",".join("?" for _ in covariance_factors)
            conn.execute(
                f"""
                DELETE FROM model_factor_covariance_daily
                WHERE factor_name NOT IN ({placeholders})
                   OR factor_name_2 NOT IN ({placeholders})
                """,
                [*covariance_factors, *covariance_factors],
            )
        conn.execute("DELETE FROM model_factor_covariance_daily WHERE as_of_date = ?", (as_of_date,))
        conn.executemany(
            """
            INSERT OR REPLACE INTO model_factor_covariance_daily (
                as_of_date, factor_name, factor_name_2, covariance, run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            covariance_payload,
        )

    if specific_risk_payload:
        conn.execute("DELETE FROM model_specific_risk_daily WHERE as_of_date = ?", (as_of_date,))
        conn.executemany(
            """
            INSERT OR REPLACE INTO model_specific_risk_daily (
                as_of_date, ric, ticker, specific_var, specific_vol, obs, trbc_business_sector, run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            specific_risk_payload,
        )

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
        metadata_values,
    )
    conn.commit()
    return {"status": "ok"}


def write_model_outputs_postgres(
    pg_conn,
    *,
    factor_returns_payload: list[tuple[Any, ...]],
    factor_returns_min_date: str | None,
    covariance_factors: list[str],
    covariance_payload: list[tuple[Any, ...]],
    specific_risk_payload: list[tuple[Any, ...]],
    metadata_values: tuple[Any, ...],
    as_of_date: str,
) -> dict[str, Any]:
    ensure_postgres_schema(pg_conn)
    with pg_conn.cursor() as cur:
        if factor_returns_payload and factor_returns_min_date:
            cur.execute("DELETE FROM model_factor_returns_daily WHERE date >= %s", (factor_returns_min_date,))
            cur.executemany(
                """
                INSERT INTO model_factor_returns_daily (
                    date, factor_name, factor_return, robust_se, t_stat, r_squared, residual_vol,
                    cross_section_n, eligible_n, coverage, run_id, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, factor_name) DO UPDATE SET
                    factor_return = EXCLUDED.factor_return,
                    robust_se = EXCLUDED.robust_se,
                    t_stat = EXCLUDED.t_stat,
                    r_squared = EXCLUDED.r_squared,
                    residual_vol = EXCLUDED.residual_vol,
                    cross_section_n = EXCLUDED.cross_section_n,
                    eligible_n = EXCLUDED.eligible_n,
                    coverage = EXCLUDED.coverage,
                    run_id = EXCLUDED.run_id,
                    updated_at = EXCLUDED.updated_at
                """,
                factor_returns_payload,
            )

        if covariance_payload:
            if covariance_factors:
                cur.execute(
                    """
                    DELETE FROM model_factor_covariance_daily
                    WHERE factor_name <> ALL(%s)
                       OR factor_name_2 <> ALL(%s)
                    """,
                    (covariance_factors, covariance_factors),
                )
            cur.execute("DELETE FROM model_factor_covariance_daily WHERE as_of_date = %s", (as_of_date,))
            cur.executemany(
                """
                INSERT INTO model_factor_covariance_daily (
                    as_of_date, factor_name, factor_name_2, covariance, run_id, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (as_of_date, factor_name, factor_name_2) DO UPDATE SET
                    covariance = EXCLUDED.covariance,
                    run_id = EXCLUDED.run_id,
                    updated_at = EXCLUDED.updated_at
                """,
                covariance_payload,
            )

        if specific_risk_payload:
            cur.execute("DELETE FROM model_specific_risk_daily WHERE as_of_date = %s", (as_of_date,))
            cur.executemany(
                """
                INSERT INTO model_specific_risk_daily (
                    as_of_date, ric, ticker, specific_var, specific_vol, obs, trbc_business_sector, run_id, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (as_of_date, ric) DO UPDATE SET
                    ticker = EXCLUDED.ticker,
                    specific_var = EXCLUDED.specific_var,
                    specific_vol = EXCLUDED.specific_vol,
                    obs = EXCLUDED.obs,
                    trbc_business_sector = EXCLUDED.trbc_business_sector,
                    run_id = EXCLUDED.run_id,
                    updated_at = EXCLUDED.updated_at
                """,
                specific_risk_payload,
            )

        cur.execute(
            """
            INSERT INTO model_run_metadata (
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
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                refresh_mode = EXCLUDED.refresh_mode,
                status = EXCLUDED.status,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                factor_returns_asof = EXCLUDED.factor_returns_asof,
                source_dates_json = EXCLUDED.source_dates_json,
                params_json = EXCLUDED.params_json,
                risk_engine_state_json = EXCLUDED.risk_engine_state_json,
                row_counts_json = EXCLUDED.row_counts_json,
                error_type = EXCLUDED.error_type,
                error_message = EXCLUDED.error_message,
                updated_at = EXCLUDED.updated_at
            """,
            metadata_values,
        )
    pg_conn.commit()
    return {"status": "ok"}
