"""Post-refresh Neon mirror sync/parity/prune workflow."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from psycopg import sql

from backend import config
from backend.data.neon import connect, resolve_dsn
from backend.services.neon_source_sync_cycle import run_neon_source_sync_cycle
from backend.trading_calendar import previous_or_same_xnys_session
from backend.services.neon_stage2 import (
    apply_sql_file,
    canonical_tables,
    ensure_target_columns_from_sqlite,
)


_CANONICAL_SCHEMA_SQL = (
    Path(__file__).resolve().parents[2] / "docs" / "reference" / "migrations" / "neon" / "NEON_CANONICAL_SCHEMA.sql"
)


def _cutoff_iso(*, years: int, as_of: date | None = None) -> str:
    base = as_of or datetime.now(timezone.utc).date()
    return (base - timedelta(days=365 * max(1, int(years)))).isoformat()


def _parse_iso_date(value: str | None) -> date | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return date.fromisoformat(txt[:10])
    except ValueError:
        return None


def _canonical_date_key(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text
    normalized = text.replace("Z", "+00:00")
    if len(normalized) >= 3 and normalized[-3:] in {"+00", "-00"}:
        normalized = f"{normalized}:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return text
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat()


def _pit_latest_closed_anchor(*, frequency: str) -> str:
    ny_date = datetime.now(ZoneInfo("America/New_York")).date()
    if frequency == "quarterly":
        quarter_start_month = (((ny_date.month - 1) // 3) * 3) + 1
        period_start = date(ny_date.year, quarter_start_month, 1)
    else:
        period_start = date(ny_date.year, ny_date.month, 1)
    return previous_or_same_xnys_session((period_start - timedelta(days=1)).isoformat())


def _sqlite_duplicate_key_groups(
    conn: sqlite3.Connection,
    *,
    table: str,
    key_cols: list[str],
    date_col: str | None = None,
    cutoff: str | None = None,
) -> int:
    where_sql = ""
    params: list[Any] = []
    if date_col and cutoff:
        where_sql = f" WHERE {date_col} >= ?"
        params.append(str(cutoff))
    key_sql = ", ".join(key_cols)
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT {key_sql}
            FROM {table}
            {where_sql}
            GROUP BY {key_sql}
            HAVING COUNT(*) > 1
        )
        """,
        params,
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _pg_duplicate_key_groups(
    pg_conn,
    *,
    table: str,
    key_cols: list[str],
    date_col: str | None = None,
    cutoff: str | None = None,
) -> int:
    params: list[Any] = []
    where_sql = sql.SQL("")
    if date_col and cutoff:
        where_sql = sql.SQL(" WHERE {} >= %s").format(sql.Identifier(date_col))
        params.append(str(cutoff))
    group_cols = sql.SQL(", ").join(sql.Identifier(col) for col in key_cols)
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT {group_cols}
                    FROM {table}
                    {where_sql}
                    GROUP BY {group_cols}
                    HAVING COUNT(*) > 1
                ) dupes
                """
            ).format(
                group_cols=group_cols,
                table=sql.Identifier(table),
                where_sql=where_sql,
            ),
            params,
        )
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _sqlite_pit_period_health(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    cutoff: str | None,
    frequency: str,
    latest_closed_anchor: str,
) -> dict[str, int]:
    where_sql = ""
    params: list[Any] = []
    if cutoff:
        where_sql = f"WHERE {date_col} >= ?"
        params.append(str(cutoff))
    period_expr = (
        f"substr({date_col}, 1, 4) || '-Q' || CAST((((CAST(substr({date_col}, 6, 2) AS INTEGER) - 1) / 3) + 1) AS INTEGER)"
        if frequency == "quarterly"
        else f"substr({date_col}, 1, 7)"
    )
    multi_row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT {period_expr} AS period_key
            FROM {table}
            {where_sql}
            GROUP BY period_key
            HAVING COUNT(DISTINCT {date_col}) > 1
        )
        """,
        params,
    ).fetchone()
    open_row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE {date_col} > ?
        """,
        (latest_closed_anchor,),
    ).fetchone()
    return {
        "periods_with_multiple_anchors": int(multi_row[0] or 0) if multi_row else 0,
        "open_period_rows": int(open_row[0] or 0) if open_row else 0,
    }


def _pg_pit_period_health(
    pg_conn,
    *,
    table: str,
    date_col: str,
    cutoff: str | None,
    frequency: str,
    latest_closed_anchor: str,
) -> dict[str, int]:
    params: list[Any] = []
    where_sql = sql.SQL("")
    if cutoff:
        where_sql = sql.SQL("WHERE {} >= %s").format(sql.Identifier(date_col))
        params.append(str(cutoff))
    period_expr = (
        sql.SQL(
            "LEFT({date_col}::text, 4) || '-Q' || CAST((((EXTRACT(MONTH FROM {date_col})::int - 1) / 3) + 1) AS int)::text"
        ).format(date_col=sql.Identifier(date_col))
        if frequency == "quarterly"
        else sql.SQL("LEFT({date_col}::text, 7)").format(date_col=sql.Identifier(date_col))
    )
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT {period_expr} AS period_key
                    FROM {table}
                    {where_sql}
                    GROUP BY period_key
                    HAVING COUNT(DISTINCT {date_col}) > 1
                ) anchors
                """
            ).format(
                period_expr=period_expr,
                table=sql.Identifier(table),
                where_sql=where_sql,
                date_col=sql.Identifier(date_col),
            ),
            params,
        )
        multi_row = cur.fetchone()
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {} WHERE {} > %s").format(
                sql.Identifier(table),
                sql.Identifier(date_col),
            ),
            (latest_closed_anchor,),
        )
        open_row = cur.fetchone()
    return {
        "periods_with_multiple_anchors": int(multi_row[0] or 0) if multi_row else 0,
        "open_period_rows": int(open_row[0] or 0) if open_row else 0,
    }


def _pg_table_exists(pg_conn, table: str) -> bool:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
            LIMIT 1
            """,
            (table,),
        )
        return cur.fetchone() is not None


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(row[1]) for row in rows]


def _pg_columns(pg_conn, table: str) -> list[str]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def _sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (str(table),),
    ).fetchone()
    return row is not None


def ensure_neon_canonical_schema(*, dsn: str | None = None) -> dict[str, Any]:
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    try:
        result = apply_sql_file(pg_conn, sql_path=_CANONICAL_SCHEMA_SQL)
        return {
            "status": "ok",
            "canonical_schema_path": str(_CANONICAL_SCHEMA_SQL),
            **result,
        }
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()


def sync_factor_returns_to_neon(
    *,
    cache_path: Path,
    dsn: str | None = None,
    mode: str = "incremental",
    overlap_days: int = 14,
    batch_size: int = 25_000,
    analytics_years: int = 5,
) -> dict[str, Any]:
    cache_db = Path(cache_path).expanduser().resolve()
    if not cache_db.exists():
        raise FileNotFoundError(f"cache db not found: {cache_db}")
    return {
        "status": "skipped",
        "reason": "deprecated_cache_source_use_durable_model_factor_returns_daily",
        "cache_path": str(cache_db),
        "table": "model_factor_returns_daily",
        "source_table": "daily_factor_returns",
    }


def prune_neon_history(
    *,
    dsn: str | None = None,
    source_years: int = 10,
    analytics_years: int = 5,
) -> dict[str, Any]:
    source_cutoff = _cutoff_iso(years=source_years)
    analytics_cutoff = _cutoff_iso(years=analytics_years)

    source_specs = [
        ("security_prices_eod", "date"),
        ("security_fundamentals_pit", "as_of_date"),
        ("security_classification_pit", "as_of_date"),
    ]
    analytics_specs = [
        ("estu_membership_daily", "date"),
        ("universe_cross_section_snapshot", "as_of_date"),
        ("barra_raw_cross_section_history", "as_of_date"),
        ("model_factor_returns_daily", "date"),
        ("model_factor_covariance_daily", "as_of_date"),
        ("model_specific_risk_daily", "as_of_date"),
        ("model_run_metadata", "completed_at"),
    ]

    out: dict[str, Any] = {
        "status": "ok",
        "source_cutoff": source_cutoff,
        "analytics_cutoff": analytics_cutoff,
        "tables": {},
    }

    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    try:
        with pg_conn.cursor() as cur:
            for table, col in source_specs:
                exists = _pg_table_exists(pg_conn, table)
                if not exists:
                    out["tables"][table] = {
                        "exists": False,
                        "deleted": 0,
                    }
                    continue
                cur.execute(
                    sql.SQL("DELETE FROM {} WHERE {} < %s").format(
                        sql.Identifier(table),
                        sql.Identifier(col),
                    ),
                    (source_cutoff,),
                )
                out["tables"][table] = {
                    "exists": True,
                    "deleted": int(cur.rowcount or 0),
                    "cutoff": source_cutoff,
                }

            for table, col in analytics_specs:
                exists = _pg_table_exists(pg_conn, table)
                if not exists:
                    out["tables"][table] = {
                        "exists": False,
                        "deleted": 0,
                    }
                    continue
                cur.execute(
                    sql.SQL("DELETE FROM {} WHERE {} < %s").format(
                        sql.Identifier(table),
                        sql.Identifier(col),
                    ),
                    (analytics_cutoff,),
                )
                out["tables"][table] = {
                    "exists": True,
                    "deleted": int(cur.rowcount or 0),
                    "cutoff": analytics_cutoff,
                }
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()

    return out


def _sqlite_count_window(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str | None,
    cutoff: str | None,
    distinct_col: str | None = "ric",
) -> dict[str, Any]:
    params: list[Any] = []
    where = ""
    if date_col and cutoff:
        where = f" WHERE {date_col} >= ?"
        params.append(cutoff)
    row = conn.execute(f"SELECT COUNT(*) FROM {table}{where}", params).fetchone()
    count = int(row[0] or 0) if row else 0

    min_date = max_date = None
    raw_max_date = None
    latest_distinct = None
    if date_col:
        row = conn.execute(
            f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}{where}",
            params,
        ).fetchone()
        if row:
            min_date = _canonical_date_key(row[0])
            max_date = _canonical_date_key(row[1])
            raw_max_date = row[1]
        if raw_max_date is not None and distinct_col:
            latest_row = conn.execute(
                f"SELECT COUNT(DISTINCT {distinct_col}) FROM {table} WHERE {date_col} = ?",
                (raw_max_date,),
            ).fetchone()
            latest_distinct = int(latest_row[0] or 0) if latest_row else 0

    return {
        "row_count": count,
        "min_date": min_date,
        "max_date": max_date,
        "latest_distinct": latest_distinct,
    }


def _pg_count_window(
    pg_conn,
    *,
    table: str,
    date_col: str | None,
    cutoff: str | None,
    distinct_col: str | None = "ric",
) -> dict[str, Any]:
    params: list[Any] = []
    where_sql = sql.SQL("")
    if date_col and cutoff:
        where_sql = sql.SQL(" WHERE {} >= %s").format(sql.Identifier(date_col))
        params.append(cutoff)

    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}{}")
            .format(sql.Identifier(table), where_sql),
            params,
        )
        count = int(cur.fetchone()[0] or 0)

        min_date = max_date = None
        raw_max_date = None
        latest_distinct = None
        if date_col:
            cur.execute(
                sql.SQL("SELECT MIN({})::text, MAX({})::text FROM {}{}")
                .format(sql.Identifier(date_col), sql.Identifier(date_col), sql.Identifier(table), where_sql),
                params,
            )
            row = cur.fetchone()
            if row:
                min_date = _canonical_date_key(row[0])
                max_date = _canonical_date_key(row[1])
                raw_max_date = row[1]
            if raw_max_date is not None and distinct_col:
                cur.execute(
                    sql.SQL("SELECT COUNT(DISTINCT {}) FROM {} WHERE {} = %s").format(
                        sql.Identifier(str(distinct_col)),
                        sql.Identifier(table),
                        sql.Identifier(date_col),
                    ),
                    (raw_max_date,),
                )
                latest_distinct = int(cur.fetchone()[0] or 0)

    return {
        "row_count": count,
        "min_date": min_date,
        "max_date": max_date,
        "latest_distinct": latest_distinct,
    }


def _sqlite_non_null_counts(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    where_sql: str = "",
    params: tuple[Any, ...] = (),
) -> dict[str, int]:
    if not columns:
        return {}
    select_sql = ", ".join(
        f'SUM(CASE WHEN "{col}" IS NOT NULL THEN 1 ELSE 0 END) AS "{col}"'
        for col in columns
    )
    row = conn.execute(f"SELECT {select_sql} FROM {table} {where_sql}", params).fetchone()
    if not row:
        return {str(col): 0 for col in columns}
    return {str(col): int(row[idx] or 0) for idx, col in enumerate(columns)}


def _pg_non_null_counts(
    pg_conn,
    *,
    table: str,
    columns: list[str],
    date_col: str | None = None,
    cutoff: str | None = None,
) -> dict[str, int]:
    if not columns:
        return {}
    where_sql = sql.SQL("")
    params: list[Any] = []
    if date_col and cutoff:
        where_sql = sql.SQL(" WHERE {} >= %s").format(sql.Identifier(date_col))
        params.append(cutoff)
    select_sql = sql.SQL(", ").join(
        sql.SQL("SUM(CASE WHEN {} IS NOT NULL THEN 1 ELSE 0 END) AS {}").format(
            sql.Identifier(col),
            sql.Identifier(col),
        )
        for col in columns
    )
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT {} FROM {}{}").format(
                select_sql,
                sql.Identifier(table),
                where_sql,
            ),
            params,
        )
        row = cur.fetchone()
    if not row:
        return {str(col): 0 for col in columns}
    return {str(col): int(row[idx] or 0) for idx, col in enumerate(columns)}


def _sqlite_recent_dates(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    cutoff: str | None,
    limit: int = 3,
) -> list[str]:
    params: list[Any] = []
    where = ""
    if cutoff:
        where = f" WHERE {date_col} >= ?"
        params.append(cutoff)
    rows = conn.execute(
        f"""
        SELECT DISTINCT {date_col}
        FROM {table}
        {where}
        ORDER BY {date_col} DESC
        LIMIT ?
        """,
        [*params, int(limit)],
    ).fetchall()
    return [
        canonical
        for row in rows
        if row and row[0] is not None
        for canonical in [_canonical_date_key(row[0])]
        if canonical is not None
    ]


def _sqlite_factor_return_values(
    conn: sqlite3.Connection,
    *,
    table: str,
    dates: list[str],
) -> dict[tuple[str, str], tuple[float, ...]]:
    if not dates:
        return {}
    placeholders = ",".join("?" for _ in dates)
    rows = conn.execute(
        f"""
        SELECT
            date,
            factor_name,
            factor_return,
            COALESCE(robust_se, 0.0),
            COALESCE(t_stat, 0.0),
            r_squared,
            residual_vol,
            COALESCE(cross_section_n, 0),
            COALESCE(eligible_n, 0),
            COALESCE(coverage, 0.0)
        FROM {table}
        WHERE date IN ({placeholders})
        ORDER BY date, factor_name
        """,
        tuple(dates),
    ).fetchall()
    return {
        (str(row[0]), str(row[1])): (
            float(row[2] or 0.0),
            float(row[3] or 0.0),
            float(row[4] or 0.0),
            float(row[5] or 0.0),
            float(row[6] or 0.0),
            float(row[7] or 0),
            float(row[8] or 0),
            float(row[9] or 0.0),
        )
        for row in rows
    }


def _pg_factor_return_values(
    pg_conn,
    *,
    table: str,
    dates: list[str],
) -> dict[tuple[str, str], tuple[float, ...]]:
    if not dates:
        return {}
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT
                    date::text,
                    factor_name,
                    factor_return,
                    COALESCE(robust_se, 0.0),
                    COALESCE(t_stat, 0.0),
                    r_squared,
                    residual_vol,
                    COALESCE(cross_section_n, 0),
                    COALESCE(eligible_n, 0),
                    COALESCE(coverage, 0.0)
                FROM {}
                WHERE date = ANY(%s)
                ORDER BY date, factor_name
                """
            ).format(sql.Identifier(table)),
            (dates,),
        )
        rows = cur.fetchall()
    return {
        (str(row[0]), str(row[1])): (
            float(row[2] or 0.0),
            float(row[3] or 0.0),
            float(row[4] or 0.0),
            float(row[5] or 0.0),
            float(row[6] or 0.0),
            float(row[7] or 0),
            float(row[8] or 0),
            float(row[9] or 0.0),
        )
        for row in rows
    }


def _sqlite_group_count_by_date(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    dates: list[str],
) -> dict[str, int]:
    if not dates:
        return {}
    placeholders = ",".join("?" for _ in dates)
    rows = conn.execute(
        f"""
        SELECT {date_col}, COUNT(*)
        FROM {table}
        WHERE {date_col} IN ({placeholders})
        GROUP BY {date_col}
        """,
        tuple(dates),
    ).fetchall()
    return {
        str(canonical): int(row[1] or 0)
        for row in rows
        for canonical in [_canonical_date_key(row[0])]
        if canonical is not None
    }


def _pg_group_count_by_date(
    pg_conn,
    *,
    table: str,
    date_col: str,
    dates: list[str],
) -> dict[str, int]:
    if not dates:
        return {}
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT {}::text, COUNT(*)
                FROM {}
                WHERE {} = ANY(%s)
                GROUP BY {}
                """
            ).format(
                sql.Identifier(date_col),
                sql.Identifier(table),
                sql.Identifier(date_col),
                sql.Identifier(date_col),
            ),
            (dates,),
        )
        rows = cur.fetchall()
    return {
        str(canonical): int(row[1] or 0)
        for row in rows
        for canonical in [_canonical_date_key(row[0])]
        if canonical is not None
    }


def _value_maps_match(
    source: dict[tuple[str, str], tuple[float, ...]],
    target: dict[tuple[str, str], tuple[float, ...]],
    *,
    tolerance: float = 1e-9,
) -> tuple[bool, list[str]]:
    issues: list[str] = []
    for key in sorted(set(source.keys()) | set(target.keys())):
        if key not in source:
            issues.append(f"unexpected_target_row:{key[0]}:{key[1]}")
            continue
        if key not in target:
            issues.append(f"missing_target_row:{key[0]}:{key[1]}")
            continue
        lhs = source[key]
        rhs = target[key]
        if len(lhs) != len(rhs):
            issues.append(f"shape_mismatch:{key[0]}:{key[1]}")
            continue
        for idx, (lv, rv) in enumerate(zip(lhs, rhs, strict=True)):
            if abs(float(lv) - float(rv)) > tolerance:
                issues.append(f"value_mismatch:{key[0]}:{key[1]}:col{idx}")
                break
    return (not issues), issues


def _expected_target_history_superset(
    source: dict[str, Any],
    target: dict[str, Any],
) -> dict[str, Any] | None:
    source_min = _canonical_date_key(source.get("min_date"))
    target_min = _canonical_date_key(target.get("min_date"))
    source_max = _canonical_date_key(source.get("max_date"))
    target_max = _canonical_date_key(target.get("max_date"))
    if not source_min or not target_min or not source_max or not target_max:
        return None
    if target_min >= source_min:
        return None
    if target_max != source_max:
        return None
    return {
        "status": "ok",
        "source_slice_min_date": source_min,
        "target_retained_min_date": target_min,
    }


def _bounded_sqlite_to_pg_table_audit(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    *,
    source_table: str,
    target_table: str,
    date_col: str | None,
    cutoff: str | None,
    distinct_col: str | None,
    key_cols: list[str] | None = None,
    required_cols: list[str] | None = None,
    non_null_cols: list[str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    issues: list[str] = []
    source_exists = _sqlite_table_exists(sqlite_conn, source_table)
    target_exists = _pg_table_exists(pg_conn, target_table)
    if not source_exists or not target_exists:
        reasons: list[str] = []
        if not source_exists:
            reasons.append(f"missing_source_table:{source_table}")
        if not target_exists:
            reasons.append(f"missing_target_table:{target_table}")
        issues.append(f"mismatch:{target_table}")
        return (
            {
                "status": "mismatch",
                "reason": ",".join(reasons),
                "source_exists": bool(source_exists),
                "target_exists": bool(target_exists),
                "cutoff": cutoff,
            },
            issues,
        )

    source = _sqlite_count_window(
        sqlite_conn,
        table=source_table,
        date_col=date_col,
        cutoff=cutoff,
        distinct_col=distinct_col,
    )
    target = _pg_count_window(
        pg_conn,
        table=target_table,
        date_col=date_col,
        cutoff=cutoff,
        distinct_col=distinct_col,
    )
    compare_cutoff = cutoff
    target_compare = target
    expected_history_superset = None
    if date_col:
        expected_history_superset = _expected_target_history_superset(source, target)
        if expected_history_superset is not None:
            compare_cutoff = str(expected_history_superset["source_slice_min_date"])
            target_compare = _pg_count_window(
                pg_conn,
                table=target_table,
                date_col=date_col,
                cutoff=compare_cutoff,
                distinct_col=distinct_col,
            )
    mismatch = source != target_compare
    if mismatch:
        issues.append(f"mismatch:{target_table}")

    table_out: dict[str, Any] = {
        "source_table": source_table,
        "source": source,
        "target": target,
        "cutoff": cutoff,
        "compare_cutoff": compare_cutoff,
        "status": "ok" if not mismatch else "mismatch",
    }
    if expected_history_superset is not None:
        table_out["expected_target_history_superset"] = expected_history_superset
        table_out["target_compare_window"] = target_compare

    if key_cols:
        source_dupes = _sqlite_duplicate_key_groups(
            sqlite_conn,
            table=source_table,
            key_cols=key_cols,
            date_col=date_col,
            cutoff=compare_cutoff,
        )
        target_dupes = _pg_duplicate_key_groups(
            pg_conn,
            table=target_table,
            key_cols=key_cols,
            date_col=date_col,
            cutoff=compare_cutoff,
        )
        table_out["duplicate_key_groups"] = {
            "source": source_dupes,
            "target": target_dupes,
        }
        if source_dupes or target_dupes:
            issues.append(f"duplicate_keys:{target_table}")
            table_out["status"] = "mismatch"

    if required_cols:
        source_cols = set(_sqlite_columns(sqlite_conn, source_table))
        target_cols = set(_pg_columns(pg_conn, target_table))
        source_missing_required = sorted(set(required_cols) - source_cols)
        target_missing_required = sorted(set(required_cols) - target_cols)
        if source_missing_required:
            issues.append(f"missing_source_columns:{target_table}")
            table_out["status"] = "mismatch"
        if target_missing_required:
            issues.append(f"missing_target_columns:{target_table}")
            table_out["status"] = "mismatch"
        table_out["source_missing_required_columns"] = source_missing_required
        table_out["target_missing_required_columns"] = target_missing_required

    if non_null_cols:
        where_sql = ""
        params: tuple[Any, ...] = ()
        if date_col and compare_cutoff:
            where_sql = f"WHERE {date_col} >= ?"
            params = (compare_cutoff,)
        source_non_null = _sqlite_non_null_counts(
            sqlite_conn,
            table=source_table,
            columns=non_null_cols,
            where_sql=where_sql,
            params=params,
        )
        target_non_null = _pg_non_null_counts(
            pg_conn,
            table=target_table,
            columns=non_null_cols,
            date_col=date_col,
            cutoff=compare_cutoff,
        )
        table_out["source_non_null_counts"] = source_non_null
        table_out["target_non_null_counts"] = target_non_null
        if source_non_null != target_non_null:
            issues.append(f"nonnull_mismatch:{target_table}")
            table_out["status"] = "mismatch"

    if date_col:
        sample_dates = _sqlite_recent_dates(
            sqlite_conn,
            table=source_table,
            date_col=date_col,
            cutoff=compare_cutoff,
            limit=3,
        )
        source_counts = _sqlite_group_count_by_date(
            sqlite_conn,
            table=source_table,
            date_col=date_col,
            dates=sample_dates,
        )
        target_counts = _pg_group_count_by_date(
            pg_conn,
            table=target_table,
            date_col=date_col,
            dates=sample_dates,
        )
        table_out["sample_dates"] = sample_dates
        table_out["source_counts_by_date"] = source_counts
        table_out["target_counts_by_date"] = target_counts
        if source_counts != target_counts:
            issues.append(f"group_count_mismatch:{target_table}")
            table_out["status"] = "mismatch"

    return table_out, issues


def _coerce_jsonish(value: Any, *, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    txt = str(value).strip()
    if not txt:
        return default
    try:
        return json.loads(txt)
    except Exception:
        return default


def _audit_source_sync_metadata(
    *,
    sqlite_conn: sqlite3.Connection,
    pg_conn,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    out: dict[str, dict[str, Any]] = {}
    issues: list[str] = []
    required_tables = (
        "source_sync_runs",
        "source_sync_watermarks",
        "security_source_status_current",
    )
    missing = [table for table in required_tables if not _pg_table_exists(pg_conn, table)]
    for table in missing:
        issues.append(f"missing_table:{table}")
        out[table] = {
            "status": "mismatch",
            "reason": "missing_in_neon",
        }
    if missing:
        return out, issues

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT sync_run_id, status, selected_tables_json, completed_at
            FROM source_sync_runs
            WHERE status = 'ok'
            ORDER BY completed_at DESC NULLS LAST, updated_at DESC
            LIMIT 1
            """
        )
        latest_run = cur.fetchone()
    if latest_run is None:
        issues.append("missing_latest_sync_run:source_sync_runs")
        out["source_sync_runs"] = {
            "status": "mismatch",
            "source": {"row_count": 1},
            "target": {"row_count": 0},
            "reason": "no_successful_sync_run",
        }
        return out, issues

    latest_sync_run_id = str(latest_run[0] or "").strip()
    selected_tables = [
        str(item).strip()
        for item in _coerce_jsonish(latest_run[2], default=[])
        if str(item).strip()
    ]
    out["source_sync_runs"] = {
        "status": "ok",
        "source": {"row_count": 1},
        "target": {
            "row_count": 1,
            "latest_sync_run_id": latest_sync_run_id,
            "selected_table_count": len(selected_tables),
            "completed_at": str(latest_run[3]) if latest_run[3] is not None else None,
        },
    }

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM source_sync_watermarks
            WHERE sync_run_id = %s
            """,
            (latest_sync_run_id,),
        )
        watermark_row = cur.fetchone()
    expected_watermark_count = 0
    for table_name in selected_tables:
        row = sqlite_conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='table' AND name=?
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()
        if row is not None:
            expected_watermark_count += 1
    actual_watermark_count = int(watermark_row[0] or 0) if watermark_row else 0
    watermark_status = "ok" if actual_watermark_count >= expected_watermark_count else "mismatch"
    if watermark_status != "ok":
        issues.append("mismatch:source_sync_watermarks")
    out["source_sync_watermarks"] = {
        "status": watermark_status,
        "source": {
            "row_count": expected_watermark_count,
            "sync_run_id": latest_sync_run_id,
        },
        "target": {
            "row_count": actual_watermark_count,
            "sync_run_id": latest_sync_run_id,
        },
    }

    active_registry_row = sqlite_conn.execute(
        """
        SELECT COUNT(DISTINCT UPPER(TRIM(ric)))
        FROM security_registry
        WHERE ric IS NOT NULL
          AND TRIM(ric) <> ''
          AND COALESCE(NULLIF(TRIM(tracking_status), ''), 'active') <> 'disabled'
        """
    ).fetchone()
    source_status_row = sqlite_conn.execute(
        """
        SELECT MAX(as_of_date)
        FROM security_source_observation_daily
        WHERE as_of_date IS NOT NULL
          AND TRIM(as_of_date) <> ''
        """
    ).fetchone()
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*),
                MAX(observation_as_of_date),
                COUNT(DISTINCT source_sync_run_id)
            FROM security_source_status_current
            """
        )
        status_row = cur.fetchone()
    expected_status_count = int(active_registry_row[0] or 0) if active_registry_row else 0
    actual_status_count = int(status_row[0] or 0) if status_row else 0
    source_observation_max = str(source_status_row[0] or "").strip() or None
    target_observation_max = str(status_row[1] or "").strip() or None if status_row else None
    distinct_sync_runs = int(status_row[2] or 0) if status_row else 0
    status_ok = (
        actual_status_count == expected_status_count
        and target_observation_max == source_observation_max
        and distinct_sync_runs == 1
    )
    if not status_ok:
        issues.append("mismatch:security_source_status_current")
    out["security_source_status_current"] = {
        "status": "ok" if status_ok else "mismatch",
        "source": {
            "row_count": expected_status_count,
            "observation_max_date": source_observation_max,
            "sync_run_id": latest_sync_run_id,
        },
        "target": {
            "row_count": actual_status_count,
            "observation_max_date": target_observation_max,
            "distinct_sync_run_ids": distinct_sync_runs,
        },
    }
    return out, issues


def run_bounded_parity_audit(
    *,
    sqlite_path: Path,
    cache_path: Path | None = None,
    dsn: str | None = None,
    source_years: int = 10,
    analytics_years: int = 5,
) -> dict[str, Any]:
    source_cutoff = _cutoff_iso(years=source_years)
    analytics_cutoff = _cutoff_iso(years=analytics_years)

    data_specs = [
        ("security_registry", "security_registry", None, None, None),
        ("security_taxonomy_current", "security_taxonomy_current", None, None, None),
        ("security_policy_current", "security_policy_current", None, None, None),
        (
            "security_source_observation_daily",
            "security_source_observation_daily",
            "as_of_date",
            source_cutoff,
            "ric",
        ),
        ("security_master_compat_current", "security_master_compat_current", None, None, None),
        ("security_ingest_runs", "security_ingest_runs", "started_at", source_cutoff, None),
        ("security_ingest_audit", "security_ingest_audit", "updated_at", source_cutoff, "ric"),
        ("security_prices_eod", "security_prices_eod", "date", source_cutoff, "ric"),
        (
            "security_fundamentals_pit",
            "security_fundamentals_pit",
            "as_of_date",
            source_cutoff,
            "ric",
        ),
        (
            "security_classification_pit",
            "security_classification_pit",
            "as_of_date",
            source_cutoff,
            "ric",
        ),
        (
            "estu_membership_daily",
            "estu_membership_daily",
            "date",
            analytics_cutoff,
            "ric",
        ),
        (
            "universe_cross_section_snapshot",
            "universe_cross_section_snapshot",
            "as_of_date",
            analytics_cutoff,
            "ric",
        ),
        (
            "barra_raw_cross_section_history",
            "barra_raw_cross_section_history",
            "as_of_date",
            analytics_cutoff,
            "ric",
        ),
    ]
    durable_model_specs = [
        {
            "target_table": "model_factor_returns_daily",
            "source_table": "model_factor_returns_daily",
            "date_col": "date",
            "cutoff": analytics_cutoff,
            "distinct_col": None,
            "key_cols": ["date", "factor_name"],
            "required_cols": [
                "date",
                "factor_name",
                "factor_return",
                "robust_se",
                "t_stat",
                "r_squared",
                "residual_vol",
                "cross_section_n",
                "eligible_n",
                "coverage",
                "run_id",
                "updated_at",
            ],
            "non_null_cols": [
                "factor_return",
                "robust_se",
                "t_stat",
                "run_id",
                "updated_at",
            ],
        },
        {
            "target_table": "model_factor_covariance_daily",
            "source_table": "model_factor_covariance_daily",
            "date_col": "as_of_date",
            "cutoff": analytics_cutoff,
            "distinct_col": None,
            "key_cols": ["as_of_date", "factor_name", "factor_name_2"],
            "required_cols": [
                "as_of_date",
                "factor_name",
                "factor_name_2",
                "covariance",
                "run_id",
                "updated_at",
            ],
            "non_null_cols": ["covariance", "run_id", "updated_at"],
        },
        {
            "target_table": "model_specific_risk_daily",
            "source_table": "model_specific_risk_daily",
            "date_col": "as_of_date",
            "cutoff": analytics_cutoff,
            "distinct_col": "ric",
            "key_cols": ["as_of_date", "ric"],
            "required_cols": [
                "as_of_date",
                "ric",
                "specific_var",
                "specific_vol",
                "obs",
                "run_id",
                "updated_at",
            ],
            "non_null_cols": ["specific_var", "specific_vol", "obs", "run_id", "updated_at"],
        },
        {
            "target_table": "model_run_metadata",
            "source_table": "model_run_metadata",
            "date_col": "completed_at",
            "cutoff": analytics_cutoff,
            "distinct_col": None,
            "key_cols": ["run_id"],
            "required_cols": [
                "run_id",
                "refresh_mode",
                "status",
                "started_at",
                "completed_at",
                "source_dates_json",
                "params_json",
                "risk_engine_state_json",
                "row_counts_json",
                "updated_at",
            ],
            "non_null_cols": [
                "refresh_mode",
                "status",
                "started_at",
                "completed_at",
                "source_dates_json",
                "params_json",
                "risk_engine_state_json",
                "row_counts_json",
                "updated_at",
            ],
        },
    ]

    sqlite_db = Path(sqlite_path).expanduser().resolve()
    if not sqlite_db.exists():
        raise FileNotFoundError(f"sqlite db not found: {sqlite_db}")

    out: dict[str, Any] = {
        "status": "ok",
        "source_cutoff": source_cutoff,
        "analytics_cutoff": analytics_cutoff,
        "sqlite_path": str(sqlite_db),
        "cache_path": str(Path(cache_path).expanduser().resolve()) if cache_path else None,
        "tables": {},
        "issues": [],
    }

    sqlite_conn = sqlite3.connect(str(sqlite_db))
    cache_conn = None
    cache_db = None
    if cache_path is not None:
        cache_db = Path(cache_path).expanduser().resolve()
        if not cache_db.exists():
            raise FileNotFoundError(f"cache db not found: {cache_db}")
        cache_conn = sqlite3.connect(str(cache_db))
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    try:
        pit_frequency = str(config.SOURCE_DAILY_PIT_FREQUENCY or "monthly").strip().lower()
        latest_closed_anchor = _pit_latest_closed_anchor(frequency=pit_frequency)
        for label, source_table, date_col, cutoff, distinct_col in data_specs:
            if date_col is None:
                srow = sqlite_conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()
                source = {"row_count": int(srow[0] or 0)}
                with pg_conn.cursor() as cur:
                    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(label)))
                    target = {"row_count": int(cur.fetchone()[0] or 0)}
            else:
                source = _sqlite_count_window(
                    sqlite_conn,
                    table=source_table,
                    date_col=date_col,
                    cutoff=cutoff,
                    distinct_col=distinct_col,
                )
                target = _pg_count_window(
                    pg_conn,
                    table=label,
                    date_col=date_col,
                    cutoff=cutoff,
                    distinct_col=distinct_col,
                )

            mismatch = source != target
            if mismatch:
                out["issues"].append(f"mismatch:{label}")
            out["tables"][label] = {
                "source": source,
                "target": target,
                "cutoff": cutoff,
                "status": "ok" if not mismatch else "mismatch",
            }
            if label == "security_prices_eod":
                source_dupes = _sqlite_duplicate_key_groups(
                    sqlite_conn,
                    table=source_table,
                    key_cols=["ric", "date"],
                    date_col=date_col,
                    cutoff=cutoff,
                )
                target_dupes = _pg_duplicate_key_groups(
                    pg_conn,
                    table=label,
                    key_cols=["ric", "date"],
                    date_col=date_col,
                    cutoff=cutoff,
                )
                out["tables"][label]["duplicate_key_groups"] = {
                    "source": source_dupes,
                    "target": target_dupes,
                }
                if source_dupes or target_dupes:
                    out["issues"].append(f"duplicate_keys:{label}")
                    out["tables"][label]["status"] = "mismatch"
            if label in {"security_fundamentals_pit", "security_classification_pit"}:
                source_health = _sqlite_pit_period_health(
                    sqlite_conn,
                    table=source_table,
                    date_col=date_col,
                    cutoff=cutoff,
                    frequency=pit_frequency,
                    latest_closed_anchor=latest_closed_anchor,
                )
                target_health = _pg_pit_period_health(
                    pg_conn,
                    table=label,
                    date_col=date_col,
                    cutoff=cutoff,
                    frequency=pit_frequency,
                    latest_closed_anchor=latest_closed_anchor,
                )
                out["tables"][label]["period_policy"] = {
                    "frequency": pit_frequency,
                    "latest_closed_anchor": latest_closed_anchor,
                    "source": source_health,
                    "target": target_health,
                }
                if source_health != target_health:
                    out["issues"].append(f"period_policy_mismatch:{label}")
                    out["tables"][label]["status"] = "mismatch"
                if any(int(v or 0) > 0 for v in source_health.values()) or any(int(v or 0) > 0 for v in target_health.values()):
                    out["issues"].append(f"period_policy_violation:{label}")
                    out["tables"][label]["status"] = "mismatch"

        for spec in durable_model_specs:
            table_out, table_issues = _bounded_sqlite_to_pg_table_audit(
                sqlite_conn,
                pg_conn,
                source_table=str(spec["source_table"]),
                target_table=str(spec["target_table"]),
                date_col=str(spec["date_col"]) if spec.get("date_col") else None,
                cutoff=str(spec["cutoff"]) if spec.get("cutoff") else None,
                distinct_col=str(spec["distinct_col"]) if spec.get("distinct_col") else None,
                key_cols=list(spec.get("key_cols") or []),
                required_cols=list(spec.get("required_cols") or []),
                non_null_cols=list(spec.get("non_null_cols") or []),
            )
            out["tables"][str(spec["target_table"])] = table_out
            out["issues"].extend(table_issues)
            if str(spec["target_table"]) == "model_factor_returns_daily":
                sample_dates = _sqlite_recent_dates(
                    sqlite_conn,
                    table="model_factor_returns_daily",
                    date_col="date",
                    cutoff=analytics_cutoff,
                    limit=3,
                )
                source_values = _sqlite_factor_return_values(
                    sqlite_conn,
                    table="model_factor_returns_daily",
                    dates=sample_dates,
                )
                target_values = _pg_factor_return_values(
                    pg_conn,
                    table="model_factor_returns_daily",
                    dates=sample_dates,
                )
                values_ok, value_issues = _value_maps_match(source_values, target_values)
                if not values_ok:
                    out["issues"].append("value_mismatch:model_factor_returns_daily")
                    table_out["status"] = "mismatch"
                table_out["value_check_status"] = "ok" if values_ok else "mismatch"
                table_out["value_check_issues"] = value_issues[:20]

        metadata_tables, metadata_issues = _audit_source_sync_metadata(
            sqlite_conn=sqlite_conn,
            pg_conn=pg_conn,
        )
        out["tables"].update(metadata_tables)
        out["issues"].extend(metadata_issues)

        out["status"] = "ok" if not out["issues"] else "mismatch"
        return out
    finally:
        sqlite_conn.close()
        if cache_conn is not None:
            cache_conn.close()
        pg_conn.close()


def run_neon_mirror_cycle(
    *,
    sqlite_path: Path,
    cache_path: Path | None = None,
    dsn: str | None = None,
    mode: str = "incremental",
    tables: list[str] | None = None,
    batch_size: int = 25_000,
    parity_enabled: bool = True,
    prune_enabled: bool = True,
    source_years: int = 10,
    analytics_years: int = 5,
) -> dict[str, Any]:
    out = run_neon_source_sync_cycle(
        sqlite_path=Path(sqlite_path),
        dsn=dsn,
        mode=str(mode),
        tables=(tables or canonical_tables()),
        batch_size=int(batch_size),
    )
    out["prune"] = None
    out["parity"] = None

    if prune_enabled:
        out["prune"] = prune_neon_history(
            dsn=dsn,
            source_years=int(source_years),
            analytics_years=int(analytics_years),
        )

    if parity_enabled:
        out["parity"] = run_bounded_parity_audit(
            sqlite_path=Path(sqlite_path),
            cache_path=(Path(cache_path) if cache_path is not None else None),
            dsn=dsn,
            source_years=int(source_years),
            analytics_years=int(analytics_years),
        )
        if str(out["parity"].get("status")) != "ok":
            out["status"] = "mismatch"

    return out
