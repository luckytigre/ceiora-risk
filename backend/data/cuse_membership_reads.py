"""Read persisted cUSE membership truth from durable model-output stores."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from backend import config
from backend.data.model_outputs import _resolve_data_db
from backend.data.neon import connect, resolve_dsn

_MEMBERSHIP_COLUMNS = (
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
)

_STAGE_COLUMNS = (
    "as_of_date",
    "ric",
    "stage_name",
    "stage_state",
    "reason_code",
    "detail_json",
    "run_id",
    "updated_at",
)


def _sqlite_table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def _sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
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


def _neon_membership_reads_enabled() -> bool:
    return bool(str(config.neon_dsn() or "").strip())


def _prefer_neon_reads() -> bool:
    return bool(
        _neon_membership_reads_enabled()
        and config.neon_primary_model_data_enabled()
    )


def _dict_rows(columns: tuple[str, ...], rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    return [
        {columns[idx]: row[idx] for idx in range(len(columns))}
        for row in rows
    ]


def _resolve_date_clause(
    dates: list[str] | None,
    *,
    placeholder: str,
    table: str,
    cast_sql: str = "",
) -> tuple[str, list[Any]]:
    clean = sorted({str(value).strip() for value in (dates or []) if str(value).strip()})
    if clean:
        placeholders = ",".join(placeholder for _ in clean)
        return f"WHERE as_of_date IN ({placeholders})", clean
    latest_expr = "MAX(as_of_date)"
    if cast_sql:
        latest_expr = cast_sql.format(expr=latest_expr)
    return f"WHERE as_of_date = (SELECT {latest_expr} FROM {table})", []


def _load_membership_rows_sqlite(
    *,
    data_db: Path,
    as_of_dates: list[str] | None = None,
) -> list[dict[str, Any]]:
    db_path = _resolve_data_db(data_db)
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    try:
        if not _sqlite_table_exists(conn, "cuse_security_membership_daily"):
            return []
        cols = _sqlite_table_columns(conn, "cuse_security_membership_daily")
        if not set(_MEMBERSHIP_COLUMNS).issubset(cols):
            return []
        where_sql, params = _resolve_date_clause(as_of_dates, placeholder="?", table="cuse_security_membership_daily")
        rows = conn.execute(
            f"""
            SELECT
                as_of_date,
                ric,
                ticker,
                policy_path,
                realized_role,
                output_status,
                projection_candidate_status,
                projection_output_status,
                reason_code,
                quality_label,
                source_snapshot_status,
                projection_method,
                projection_basis_status,
                projection_source_package_date,
                served_exposure_available,
                run_id,
                updated_at
            FROM cuse_security_membership_daily
            {where_sql}
            ORDER BY as_of_date, ticker, ric
            """,
            params,
        ).fetchall()
    finally:
        conn.close()
    return _dict_rows(_MEMBERSHIP_COLUMNS, rows)


def _load_stage_rows_sqlite(
    *,
    data_db: Path,
    as_of_dates: list[str] | None = None,
) -> list[dict[str, Any]]:
    db_path = _resolve_data_db(data_db)
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    try:
        if not _sqlite_table_exists(conn, "cuse_security_stage_results_daily"):
            return []
        cols = _sqlite_table_columns(conn, "cuse_security_stage_results_daily")
        if not set(_STAGE_COLUMNS).issubset(cols):
            return []
        where_sql, params = _resolve_date_clause(as_of_dates, placeholder="?", table="cuse_security_stage_results_daily")
        rows = conn.execute(
            f"""
            SELECT
                as_of_date,
                ric,
                stage_name,
                stage_state,
                reason_code,
                detail_json,
                run_id,
                updated_at
            FROM cuse_security_stage_results_daily
            {where_sql}
            ORDER BY as_of_date, ric, stage_name
            """,
            params,
        ).fetchall()
    finally:
        conn.close()
    return _dict_rows(_STAGE_COLUMNS, rows)


def _load_membership_rows_postgres(
    *,
    as_of_dates: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not _neon_membership_reads_enabled():
        return []
    conn = connect(dsn=resolve_dsn(None), autocommit=True)
    try:
        where_sql, params = _resolve_date_clause(
            as_of_dates,
            placeholder="%s",
            table="cuse_security_membership_daily",
            cast_sql="{expr}::text",
        )
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    as_of_date::text,
                    ric,
                    ticker,
                    policy_path,
                    realized_role,
                    output_status,
                    projection_candidate_status,
                    projection_output_status,
                    reason_code,
                    quality_label,
                    source_snapshot_status,
                    projection_method,
                    projection_basis_status,
                    projection_source_package_date::text,
                    served_exposure_available,
                    run_id,
                    updated_at::text
                FROM cuse_security_membership_daily
                {where_sql}
                ORDER BY as_of_date, ticker, ric
                """,
                params,
            )
            rows = cur.fetchall()
    except Exception:
        return []
    finally:
        conn.close()
    return _dict_rows(_MEMBERSHIP_COLUMNS, rows)


def _load_stage_rows_postgres(
    *,
    as_of_dates: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not _neon_membership_reads_enabled():
        return []
    conn = connect(dsn=resolve_dsn(None), autocommit=True)
    try:
        where_sql, params = _resolve_date_clause(
            as_of_dates,
            placeholder="%s",
            table="cuse_security_stage_results_daily",
            cast_sql="{expr}::text",
        )
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    as_of_date::text,
                    ric,
                    stage_name,
                    stage_state,
                    reason_code,
                    detail_json,
                    run_id,
                    updated_at::text
                FROM cuse_security_stage_results_daily
                {where_sql}
                ORDER BY as_of_date, ric, stage_name
                """,
                params,
            )
            rows = cur.fetchall()
    except Exception:
        return []
    finally:
        conn.close()
    return _dict_rows(_STAGE_COLUMNS, rows)


def load_cuse_membership_rows(
    *,
    data_db: Path | None = None,
    as_of_dates: list[str] | None = None,
) -> list[dict[str, Any]]:
    db_path = _resolve_data_db(data_db)
    if _prefer_neon_reads():
        rows = _load_membership_rows_postgres(as_of_dates=as_of_dates)
        if rows:
            return rows
    rows = _load_membership_rows_sqlite(data_db=db_path, as_of_dates=as_of_dates)
    if rows:
        return rows
    if not _prefer_neon_reads():
        return _load_membership_rows_postgres(as_of_dates=as_of_dates)
    return []


def load_cuse_stage_result_rows(
    *,
    data_db: Path | None = None,
    as_of_dates: list[str] | None = None,
) -> list[dict[str, Any]]:
    db_path = _resolve_data_db(data_db)
    if _prefer_neon_reads():
        rows = _load_stage_rows_postgres(as_of_dates=as_of_dates)
        if rows:
            return rows
    rows = _load_stage_rows_sqlite(data_db=db_path, as_of_dates=as_of_dates)
    if rows:
        return rows
    if not _prefer_neon_reads():
        return _load_stage_rows_postgres(as_of_dates=as_of_dates)
    return []


def load_cuse_membership_lookup(
    *,
    data_db: Path | None = None,
    as_of_dates: list[str] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    rows = load_cuse_membership_rows(data_db=data_db, as_of_dates=as_of_dates)
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        as_of_date = str(row.get("as_of_date") or "").strip()
        ticker = str(row.get("ticker") or "").strip().upper()
        ric = str(row.get("ric") or "").strip().upper()
        if as_of_date and ticker:
            out[(as_of_date, ticker)] = row
        if as_of_date and ric:
            out[(as_of_date, ric)] = row
    return out
