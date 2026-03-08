"""Post-refresh Neon mirror sync/parity/prune workflow."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from psycopg import sql

from backend.data.neon import connect, resolve_dsn
from backend.services.neon_stage2 import canonical_tables, sync_from_sqlite_to_neon


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

    table = "model_factor_returns_daily"
    source_table = "daily_factor_returns"
    mode_norm = str(mode or "incremental").strip().lower()
    if mode_norm not in {"full", "incremental"}:
        mode_norm = "incremental"
    sync_cutoff = _cutoff_iso(years=analytics_years)

    sqlite_conn = sqlite3.connect(str(cache_db))
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    try:
        if not _sqlite_table_exists(sqlite_conn, source_table):
            return {
                "status": "skipped",
                "reason": f"missing_source_table:{source_table}",
                "cache_path": str(cache_db),
                "table": table,
            }
        if not _pg_table_exists(pg_conn, table):
            raise RuntimeError(f"target table missing in Neon: {table}")

        where_sql = ""
        where_params: tuple[Any, ...] = ()
        action = "truncate_and_reload"
        source_count_sql = f"SELECT COUNT(*) FROM {source_table} WHERE date >= ?"
        source_count_params: tuple[Any, ...] = (sync_cutoff,)
        if mode_norm == "full":
            with pg_conn.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
            where_sql = "WHERE date >= ?"
            where_params = (sync_cutoff,)
        else:
            with pg_conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COUNT(*), MAX(date)::text FROM {} WHERE date >= %s"
                    ).format(sql.Identifier(table)),
                    (sync_cutoff,),
                )
                row = cur.fetchone()
                target_count = int(row[0] or 0) if row else 0
                max_date_txt = str(row[1]) if row and row[1] is not None else ""
            src_count_row = sqlite_conn.execute(source_count_sql, source_count_params).fetchone()
            source_window_rows = int(src_count_row[0] or 0) if src_count_row else 0
            max_date = _parse_iso_date(max_date_txt)
            if max_date is None:
                with pg_conn.cursor() as cur:
                    cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
                action = "target_empty_truncate_and_reload"
                where_sql = "WHERE date >= ?"
                where_params = (sync_cutoff,)
            elif target_count != source_window_rows:
                with pg_conn.cursor() as cur:
                    cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
                where_sql = "WHERE date >= ?"
                where_params = (sync_cutoff,)
                action = "bounded_full_reload_after_count_mismatch"
            else:
                cutoff = max_date - timedelta(days=max(0, int(overlap_days)))
                cutoff_txt = max(cutoff.isoformat(), sync_cutoff)
                with pg_conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("DELETE FROM {} WHERE date >= %s").format(
                            sql.Identifier(table)
                        ),
                        (cutoff_txt,),
                    )
                where_sql = "WHERE date >= ?"
                where_params = (cutoff_txt,)
                action = "incremental_overlap_reload"

        src_count = sqlite_conn.execute(
            f"SELECT COUNT(*) FROM {source_table} {where_sql}",
            where_params,
        ).fetchone()
        source_rows = int(src_count[0] or 0) if src_count else 0

        select_sql = f"""
            SELECT
                date,
                factor_name,
                factor_return,
                r_squared,
                residual_vol,
                cross_section_n,
                eligible_n,
                coverage
            FROM {source_table}
            {where_sql}
            ORDER BY date, factor_name
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        insert_sql = sql.SQL(
            """
            INSERT INTO {} (
                date,
                factor_name,
                factor_return,
                r_squared,
                residual_vol,
                cross_section_n,
                eligible_n,
                coverage,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, factor_name) DO UPDATE SET
                factor_return = EXCLUDED.factor_return,
                r_squared = EXCLUDED.r_squared,
                residual_vol = EXCLUDED.residual_vol,
                cross_section_n = EXCLUDED.cross_section_n,
                eligible_n = EXCLUDED.eligible_n,
                coverage = EXCLUDED.coverage,
                updated_at = EXCLUDED.updated_at
            """
        ).format(sql.Identifier(table))

        loaded = 0
        chunk: list[tuple[Any, ...]] = []
        with pg_conn.cursor() as cur:
            for row in sqlite_conn.execute(select_sql, where_params):
                chunk.append(
                    (
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                        now_iso,
                    )
                )
                if len(chunk) >= max(500, int(batch_size)):
                    cur.executemany(insert_sql, chunk)
                    loaded += len(chunk)
                    chunk = []
            if chunk:
                cur.executemany(insert_sql, chunk)
                loaded += len(chunk)
        pg_conn.commit()

        out: dict[str, Any] = {
            "status": "ok",
            "mode": mode_norm,
            "cache_path": str(cache_db),
            "table": table,
            "source_table": source_table,
            "action": action,
            "source_rows": int(source_rows),
            "rows_loaded": int(loaded),
        }
        if where_sql:
            out["where_sql"] = where_sql
            out["where_params"] = [str(v) for v in where_params]
        return out
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()


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
        ("barra_raw_cross_section_history", "as_of_date"),
        ("model_factor_returns_daily", "date"),
        ("model_factor_covariance_daily", "as_of_date"),
        ("model_specific_risk_daily", "as_of_date"),
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
    latest_distinct = None
    if date_col:
        row = conn.execute(
            f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}{where}",
            params,
        ).fetchone()
        if row:
            min_date = str(row[0]) if row[0] is not None else None
            max_date = str(row[1]) if row[1] is not None else None
        if max_date and distinct_col:
            latest_row = conn.execute(
                f"SELECT COUNT(DISTINCT {distinct_col}) FROM {table} WHERE {date_col} = ?",
                (max_date,),
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
        latest_distinct = None
        if date_col:
            cur.execute(
                sql.SQL("SELECT MIN({})::text, MAX({})::text FROM {}{}")
                .format(sql.Identifier(date_col), sql.Identifier(date_col), sql.Identifier(table), where_sql),
                params,
            )
            row = cur.fetchone()
            if row:
                min_date = str(row[0]) if row[0] is not None else None
                max_date = str(row[1]) if row[1] is not None else None
            if max_date and distinct_col:
                cur.execute(
                    sql.SQL("SELECT COUNT(DISTINCT {}) FROM {} WHERE {} = %s").format(
                        sql.Identifier(str(distinct_col)),
                        sql.Identifier(table),
                        sql.Identifier(date_col),
                    ),
                    (max_date,),
                )
                latest_distinct = int(cur.fetchone()[0] or 0)

    return {
        "row_count": count,
        "min_date": min_date,
        "max_date": max_date,
        "latest_distinct": latest_distinct,
    }


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
        ("security_master", "security_master", None, None, None),
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
            "barra_raw_cross_section_history",
            "barra_raw_cross_section_history",
            "as_of_date",
            analytics_cutoff,
            "ric",
        ),
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

        if cache_conn is not None:
            source_table = "daily_factor_returns"
            target_table = "model_factor_returns_daily"
            source_exists = _sqlite_table_exists(cache_conn, source_table)
            target_exists = _pg_table_exists(pg_conn, target_table)
            if source_exists and target_exists:
                source = _sqlite_count_window(
                    cache_conn,
                    table=source_table,
                    date_col="date",
                    cutoff=analytics_cutoff,
                    distinct_col=None,
                )
                target = _pg_count_window(
                    pg_conn,
                    table=target_table,
                    date_col="date",
                    cutoff=analytics_cutoff,
                    distinct_col=None,
                )
                mismatch = source != target
                if mismatch:
                    out["issues"].append(f"mismatch:{target_table}")
                out["tables"][target_table] = {
                    "source_table": source_table,
                    "source": source,
                    "target": target,
                    "cutoff": analytics_cutoff,
                    "status": "ok" if not mismatch else "mismatch",
                }
            else:
                status = "skipped"
                reason = None
                if source_exists and not target_exists:
                    status = "mismatch"
                    reason = f"missing_target_table:{target_table}"
                    out["issues"].append(f"mismatch:{target_table}")
                elif not source_exists:
                    reason = f"missing_source_table:{source_table}"
                out["tables"][target_table] = {
                    "status": status,
                    "reason": reason,
                    "source_exists": bool(source_exists),
                    "target_exists": bool(target_exists),
                }

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
    selected_tables = tables or canonical_tables()
    out: dict[str, Any] = {
        "status": "ok",
        "mode": str(mode),
        "tables": selected_tables,
        "sync": None,
        "factor_returns_sync": None,
        "prune": None,
        "parity": None,
    }

    out["sync"] = sync_from_sqlite_to_neon(
        sqlite_path=Path(sqlite_path),
        dsn=dsn,
        tables=selected_tables,
        mode=str(mode),
        batch_size=int(batch_size),
    )

    if cache_path is not None:
        out["factor_returns_sync"] = sync_factor_returns_to_neon(
            cache_path=Path(cache_path),
            dsn=dsn,
            mode=str(mode),
            batch_size=int(batch_size),
            analytics_years=int(analytics_years),
        )

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
