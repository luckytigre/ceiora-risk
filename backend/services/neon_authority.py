"""Helpers for Neon-authoritative rebuild prerequisites and scratch workspaces."""

from __future__ import annotations

import shutil
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from psycopg import sql

from backend import config
from backend.data.neon import connect, resolve_dsn
from backend.trading_calendar import previous_or_same_xnys_session

WORKSPACE_SOURCE_TABLES = (
    "security_registry",
    "security_taxonomy_current",
    "security_policy_current",
    "security_source_observation_daily",
    "security_master_compat_current",
    "security_source_status_current",
    "security_prices_eod",
    "security_fundamentals_pit",
    "security_classification_pit",
    "estu_membership_daily",
    "universe_cross_section_snapshot",
    "barra_raw_cross_section_history",
    "model_factor_returns_daily",
)

LOCAL_MIRROR_DATA_TABLES = (
    "barra_raw_cross_section_history",
    "model_factor_returns_daily",
    "model_factor_covariance_daily",
    "model_specific_risk_daily",
    "model_run_metadata",
    "projected_instrument_loadings",
    "projected_instrument_meta",
)

LOCAL_MIRROR_CACHE_TABLES = (
    "daily_factor_returns",
    "daily_specific_residuals",
    "daily_universe_eligibility_summary",
    "daily_factor_returns_meta",
)

LOCAL_MIRROR_CACHE_KEYS = (
    "risk_engine_cov",
    "risk_engine_specific_risk",
    "risk_engine_meta",
)

_CORE_HISTORY_SLACK_DAYS = 31


@dataclass(frozen=True)
class WorkspacePaths:
    root_dir: Path
    data_db: Path
    cache_db: Path


def _parse_iso_date(value: str | None) -> date | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return date.fromisoformat(txt[:10])
    except ValueError:
        return None


def _table_exists_pg(pg_conn, table: str) -> bool:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
            LIMIT 1
            """,
            (str(table),),
        )
        return cur.fetchone() is not None


def _table_exists_sqlite(conn: sqlite3.Connection, table: str) -> bool:
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


def _pg_column_defs(pg_conn, table: str) -> list[dict[str, str]]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (str(table),),
        )
        return [
            {
                "name": str(row[0]),
                "data_type": str(row[1] or ""),
            }
            for row in cur.fetchall()
        ]


def _sqlite_type_for_pg(data_type: str) -> str:
    clean = str(data_type or "").strip().lower()
    if clean in {"smallint", "integer", "bigint"}:
        return "INTEGER"
    if clean in {"boolean"}:
        return "INTEGER"
    if clean in {"real", "double precision", "numeric", "decimal"}:
        return "REAL"
    return "TEXT"


def _workspace_index_sql(table: str) -> list[str]:
    if table == "security_prices_eod":
        return [
            "CREATE INDEX IF NOT EXISTS idx_workspace_prices_ric_date ON security_prices_eod(ric, date)",
            "CREATE INDEX IF NOT EXISTS idx_workspace_prices_date ON security_prices_eod(date)",
        ]
    if table == "security_fundamentals_pit":
        return [
            "CREATE INDEX IF NOT EXISTS idx_workspace_fund_ric_asof ON security_fundamentals_pit(ric, as_of_date)",
        ]
    if table == "security_classification_pit":
        return [
            "CREATE INDEX IF NOT EXISTS idx_workspace_class_ric_asof ON security_classification_pit(ric, as_of_date)",
        ]
    if table == "barra_raw_cross_section_history":
        return [
            "CREATE INDEX IF NOT EXISTS idx_workspace_barra_ric_asof ON barra_raw_cross_section_history(ric, as_of_date)",
            "CREATE INDEX IF NOT EXISTS idx_workspace_barra_asof ON barra_raw_cross_section_history(as_of_date)",
        ]
    return []


def _drop_and_recreate_sqlite_table(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[dict[str, str]],
) -> None:
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    cols_sql = ", ".join(
        f'"{col["name"]}" {_sqlite_type_for_pg(col["data_type"])}'
        for col in columns
    )
    conn.execute(f'CREATE TABLE "{table}" ({cols_sql})')
    for stmt in _workspace_index_sql(table):
        conn.execute(stmt)


def _copy_pg_table_to_sqlite(
    pg_conn,
    sqlite_conn: sqlite3.Connection,
    *,
    table: str,
    batch_size: int = 10_000,
) -> dict[str, Any]:
    columns = _pg_column_defs(pg_conn, table)
    if not columns:
        return {"status": "skipped", "reason": "missing_columns", "table": str(table)}
    _drop_and_recreate_sqlite_table(sqlite_conn, table=table, columns=columns)
    column_names = [str(col["name"]) for col in columns]
    placeholders = ",".join("?" for _ in column_names)
    insert_sql = f'INSERT INTO "{table}" ({", ".join(f"""\"{name}\"""" for name in column_names)}) VALUES ({placeholders})'
    copied = 0
    with pg_conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            sqlite_conn.executemany(insert_sql, [tuple(row) for row in rows])
            copied += len(rows)
    sqlite_conn.commit()
    return {
        "status": "ok",
        "table": str(table),
        "row_count": int(copied),
        "columns": column_names,
    }


def _pg_date_stats(pg_conn, *, table: str, date_col: str | None) -> dict[str, Any]:
    if not _table_exists_pg(pg_conn, table):
        return {"exists": False, "row_count": 0, "min_date": None, "max_date": None}
    with pg_conn.cursor() as cur:
        if date_col:
            cur.execute(
                sql.SQL("SELECT COUNT(*), MIN({})::text, MAX({})::text FROM {}").format(
                    sql.Identifier(date_col),
                    sql.Identifier(date_col),
                    sql.Identifier(table),
                )
            )
            row = cur.fetchone()
            return {
                "exists": True,
                "row_count": int(row[0] or 0) if row else 0,
                "min_date": str(row[1]) if row and row[1] is not None else None,
                "max_date": str(row[2]) if row and row[2] is not None else None,
            }
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
        row = cur.fetchone()
        return {
            "exists": True,
            "row_count": int(row[0] or 0) if row else 0,
            "min_date": None,
            "max_date": None,
        }


def _latest_source_anchor(table_stats: dict[str, dict[str, Any]]) -> str | None:
    candidates = [
        str((table_stats.get(table) or {}).get("max_date") or "").strip()
        for table in ("security_prices_eod", "security_fundamentals_pit", "security_classification_pit")
    ]
    clean = [value for value in candidates if value]
    if not clean:
        return None
    return min(clean)


def _latest_closed_pit_anchor(*, reference_date: str | None, frequency: str) -> str | None:
    parsed = _parse_iso_date(reference_date)
    if parsed is None:
        return None
    if frequency == "quarterly":
        quarter_start_month = (((parsed.month - 1) // 3) * 3) + 1
        current_period_start = date(parsed.year, quarter_start_month, 1)
    else:
        current_period_start = date(parsed.year, parsed.month, 1)
    return previous_or_same_xnys_session((current_period_start - timedelta(days=1)).isoformat())


def _expected_pit_lag_matches_source_dates(source_latest_dates: dict[str, str]) -> tuple[bool, str | None]:
    price_latest = str(source_latest_dates.get("security_prices_eod") or "").strip()
    expected_pit_anchor = _latest_closed_pit_anchor(
        reference_date=price_latest or None,
        frequency=str(config.SOURCE_DAILY_PIT_FREQUENCY or "monthly").strip().lower(),
    )
    if not expected_pit_anchor:
        return False, None
    fundamentals_latest = str(source_latest_dates.get("security_fundamentals_pit") or "").strip()
    classification_latest = str(source_latest_dates.get("security_classification_pit") or "").strip()
    matches = (
        bool(price_latest)
        and fundamentals_latest == expected_pit_anchor
        and classification_latest == expected_pit_anchor
        and expected_pit_anchor <= price_latest
    )
    return matches, expected_pit_anchor


def _history_anchor_for_profile(
    *,
    profile_key: str,
    source_anchor: str | None,
    table_stats: dict[str, dict[str, Any]],
) -> str | None:
    if profile_key == "cold-core":
        return source_anchor
    raw_max = str((table_stats.get("barra_raw_cross_section_history") or {}).get("max_date") or "").strip()
    if raw_max:
        return raw_max
    return source_anchor


def _assess_neon_rebuild_readiness(
    *,
    profile: str,
    table_stats: dict[str, dict[str, Any]],
    analytics_years: int,
) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    profile_key = str(profile or "").strip().lower()
    required_tables = [
        "security_registry",
        "security_taxonomy_current",
        "security_policy_current",
        "security_source_observation_daily",
        "security_master_compat_current",
        "security_source_status_current",
        "source_sync_runs",
        "source_sync_watermarks",
        "security_prices_eod",
        "security_fundamentals_pit",
        "security_classification_pit",
    ]
    required_model_tables = [
        "model_factor_returns_daily",
        "model_factor_covariance_daily",
        "model_specific_risk_daily",
        "model_run_metadata",
    ]
    if profile_key != "cold-core":
        required_tables.append("barra_raw_cross_section_history")

    for table in required_tables:
        stats = table_stats.get(table) or {}
        if not bool(stats.get("exists")):
            issues.append(f"missing_table:{table}")
            continue
        if int(stats.get("row_count") or 0) <= 0:
            issues.append(f"empty_table:{table}")

    for table in required_model_tables:
        stats = table_stats.get(table) or {}
        if not bool(stats.get("exists")):
            issues.append(f"missing_table:{table}")
            continue
        if int(stats.get("row_count") or 0) <= 0:
            issues.append(f"empty_table:{table}")

    source_anchor = _latest_source_anchor(table_stats)
    if not source_anchor:
        issues.append("missing_source_anchor")

    source_latest_dates = {
        table: str((table_stats.get(table) or {}).get("max_date") or "")
        for table in ("security_prices_eod", "security_fundamentals_pit", "security_classification_pit")
    }
    source_observation_max = str(
        (table_stats.get("security_source_observation_daily") or {}).get("max_date") or ""
    ).strip()
    unique_latest_dates = {value for value in source_latest_dates.values() if value}
    if len(unique_latest_dates) > 1:
        expected_pit_lag, expected_pit_anchor = _expected_pit_lag_matches_source_dates(source_latest_dates)
        if expected_pit_lag:
            warnings.append(
                f"latest_date_mismatch:source_tables_expected_pit_lag:{expected_pit_anchor}"
            )
        else:
            issues.append("latest_date_mismatch:source_tables")
    observation_required_date = str(source_latest_dates.get("security_prices_eod") or source_anchor or "").strip()
    if not source_observation_max:
        issues.append("missing_max_date:security_source_observation_daily")
    elif observation_required_date and source_observation_max < observation_required_date:
        issues.append(
            "stale_table:security_source_observation_daily:"
            f"{source_observation_max}<{observation_required_date}"
        )

    analytics_cutoff = None
    history_anchor = _history_anchor_for_profile(
        profile_key=profile_key,
        source_anchor=source_anchor,
        table_stats=table_stats,
    )
    anchor_date = _parse_iso_date(history_anchor)
    if anchor_date is not None:
        analytics_cutoff = (anchor_date - timedelta(days=365 * max(1, int(analytics_years)))).isoformat()
        cutoff_date = _parse_iso_date(analytics_cutoff)
        history_slack_days = 0 if profile_key == "cold-core" else _CORE_HISTORY_SLACK_DAYS
        for table in ("security_prices_eod", "security_fundamentals_pit", "security_classification_pit"):
            min_date = _parse_iso_date(str((table_stats.get(table) or {}).get("min_date") or "").strip())
            if (
                cutoff_date is not None
                and min_date is not None
                and min_date > (cutoff_date + timedelta(days=history_slack_days))
            ):
                issues.append(f"insufficient_history:{table}:{analytics_cutoff}")
        if profile_key != "cold-core":
            raw_min = _parse_iso_date(
                str((table_stats.get("barra_raw_cross_section_history") or {}).get("min_date") or "").strip()
            )
            raw_max = str((table_stats.get("barra_raw_cross_section_history") or {}).get("max_date") or "").strip()
            if (
                cutoff_date is not None
                and raw_min is not None
                and raw_min > (cutoff_date + timedelta(days=history_slack_days))
            ):
                issues.append(f"insufficient_history:barra_raw_cross_section_history:{analytics_cutoff}")
            if raw_max and source_anchor and raw_max < source_anchor:
                issues.append("stale_raw_history_vs_sources")

    return {
        "status": "ok" if not issues else "error",
        "profile": profile_key,
        "issues": issues,
        "warnings": warnings,
        "source_anchor_date": source_anchor,
        "history_anchor_date": history_anchor,
        "required_analytics_cutoff": analytics_cutoff,
        "tables": table_stats,
    }


def validate_neon_rebuild_readiness(
    *,
    profile: str,
    dsn: str | None = None,
    analytics_years: int = 5,
) -> dict[str, Any]:
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=True)
    try:
        table_stats = {
            "security_registry": _pg_date_stats(pg_conn, table="security_registry", date_col=None),
            "security_taxonomy_current": _pg_date_stats(pg_conn, table="security_taxonomy_current", date_col=None),
            "security_policy_current": _pg_date_stats(pg_conn, table="security_policy_current", date_col=None),
            "security_source_observation_daily": _pg_date_stats(pg_conn, table="security_source_observation_daily", date_col="as_of_date"),
            "security_master_compat_current": _pg_date_stats(pg_conn, table="security_master_compat_current", date_col=None),
            "security_source_status_current": _pg_date_stats(pg_conn, table="security_source_status_current", date_col="observation_as_of_date"),
            "source_sync_runs": _pg_date_stats(pg_conn, table="source_sync_runs", date_col="completed_at"),
            "source_sync_watermarks": _pg_date_stats(pg_conn, table="source_sync_watermarks", date_col=None),
            "security_prices_eod": _pg_date_stats(pg_conn, table="security_prices_eod", date_col="date"),
            "security_fundamentals_pit": _pg_date_stats(pg_conn, table="security_fundamentals_pit", date_col="as_of_date"),
            "security_classification_pit": _pg_date_stats(pg_conn, table="security_classification_pit", date_col="as_of_date"),
            "barra_raw_cross_section_history": _pg_date_stats(pg_conn, table="barra_raw_cross_section_history", date_col="as_of_date"),
            "model_factor_returns_daily": _pg_date_stats(pg_conn, table="model_factor_returns_daily", date_col="date"),
            "model_factor_covariance_daily": _pg_date_stats(pg_conn, table="model_factor_covariance_daily", date_col="as_of_date"),
            "model_specific_risk_daily": _pg_date_stats(pg_conn, table="model_specific_risk_daily", date_col="as_of_date"),
            "model_run_metadata": _pg_date_stats(pg_conn, table="model_run_metadata", date_col="completed_at"),
        }
        return _assess_neon_rebuild_readiness(
            profile=profile,
            table_stats=table_stats,
            analytics_years=analytics_years,
        )
    finally:
        pg_conn.close()


def prepare_neon_rebuild_workspace(
    *,
    profile: str,
    workspace_root: Path,
    dsn: str | None = None,
    analytics_years: int = 5,
) -> dict[str, Any]:
    readiness = validate_neon_rebuild_readiness(
        profile=profile,
        dsn=dsn,
        analytics_years=analytics_years,
    )
    if str(readiness.get("status") or "") != "ok":
        issues = ", ".join(str(item) for item in (readiness.get("issues") or []))
        raise RuntimeError(f"Neon rebuild readiness failed: {issues or 'unknown_issue'}")

    root = Path(workspace_root).expanduser().resolve()
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    data_db = root / "data.db"
    cache_db = root / "cache.db"
    sqlite_conn = sqlite3.connect(str(data_db))
    sqlite_conn.execute("PRAGMA journal_mode=WAL")
    sqlite_conn.execute("PRAGMA synchronous=NORMAL")
    sqlite_conn.commit()
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=True)
    copied_tables: list[dict[str, Any]] = []
    try:
        for table in WORKSPACE_SOURCE_TABLES:
            if not _table_exists_pg(pg_conn, table):
                continue
            copied_tables.append(
                _copy_pg_table_to_sqlite(
                    pg_conn,
                    sqlite_conn,
                    table=table,
                )
            )
        sqlite3.connect(str(cache_db)).close()
    finally:
        sqlite_conn.close()
        pg_conn.close()

    return {
        "status": "ok",
        "workspace": {
            "root_dir": str(root),
            "data_db": str(data_db),
            "cache_db": str(cache_db),
        },
        "readiness": readiness,
        "copied_tables": copied_tables,
    }


def _sqlite_table_sql(conn: sqlite3.Connection, table: str) -> str | None:
    row = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (str(table),),
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def _sqlite_index_sqls(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='index'
          AND tbl_name=?
          AND sql IS NOT NULL
        """,
        (str(table),),
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0]]


def _copy_sqlite_table(
    source_conn: sqlite3.Connection,
    target_conn: sqlite3.Connection,
    *,
    table: str,
    batch_size: int = 10_000,
) -> dict[str, Any]:
    if not _table_exists_sqlite(source_conn, table):
        return {"status": "skipped", "reason": "missing_source_table", "table": str(table)}
    create_sql = _sqlite_table_sql(source_conn, table)
    if not create_sql:
        return {"status": "skipped", "reason": "missing_create_sql", "table": str(table)}
    cols = [
        str(row[1])
        for row in source_conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    ]
    target_conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    target_conn.execute(create_sql)
    rows_cur = source_conn.execute(f'SELECT * FROM "{table}"')
    placeholders = ",".join("?" for _ in cols)
    insert_sql = f'INSERT INTO "{table}" VALUES ({placeholders})'
    copied = 0
    while True:
        rows = rows_cur.fetchmany(batch_size)
        if not rows:
            break
        target_conn.executemany(insert_sql, rows)
        copied += len(rows)
    for stmt in _sqlite_index_sqls(source_conn, table):
        target_conn.execute(stmt)
    target_conn.commit()
    return {
        "status": "ok",
        "table": str(table),
        "row_count": int(copied),
    }


def _ensure_cache_table(target_conn: sqlite3.Connection) -> None:
    target_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    target_conn.commit()


def _copy_cache_keys(
    source_conn: sqlite3.Connection,
    target_conn: sqlite3.Connection,
    *,
    keys: tuple[str, ...],
) -> dict[str, Any]:
    if not _table_exists_sqlite(source_conn, "cache"):
        return {"status": "skipped", "reason": "missing_source_cache"}
    _ensure_cache_table(target_conn)
    placeholders = ",".join("?" for _ in keys)
    rows = source_conn.execute(
        f"SELECT key, value, updated_at FROM cache WHERE key IN ({placeholders})",
        tuple(keys),
    ).fetchall()
    if rows:
        target_conn.execute(
            f"DELETE FROM cache WHERE key IN ({placeholders})",
            tuple(keys),
        )
        target_conn.executemany(
            "INSERT OR REPLACE INTO cache (key, value, updated_at) VALUES (?, ?, ?)",
            rows,
        )
        target_conn.commit()
    return {
        "status": "ok",
        "row_count": int(len(rows)),
        "keys": list(keys),
    }


def sync_workspace_derivatives_to_local_mirror(
    *,
    workspace_data_db: Path,
    workspace_cache_db: Path,
    local_data_db: Path,
    local_cache_db: Path,
) -> dict[str, Any]:
    source_data = sqlite3.connect(str(Path(workspace_data_db).expanduser().resolve()))
    source_cache = sqlite3.connect(str(Path(workspace_cache_db).expanduser().resolve()))
    target_data = sqlite3.connect(str(Path(local_data_db).expanduser().resolve()))
    target_cache = sqlite3.connect(str(Path(local_cache_db).expanduser().resolve()))
    copied_data: list[dict[str, Any]] = []
    copied_cache_tables: list[dict[str, Any]] = []
    try:
        for table in LOCAL_MIRROR_DATA_TABLES:
            copied_data.append(_copy_sqlite_table(source_data, target_data, table=table))
        for table in LOCAL_MIRROR_CACHE_TABLES:
            copied_cache_tables.append(_copy_sqlite_table(source_cache, target_cache, table=table))
        copied_cache_keys = _copy_cache_keys(
            source_cache,
            target_cache,
            keys=LOCAL_MIRROR_CACHE_KEYS,
        )
    finally:
        source_data.close()
        source_cache.close()
        target_data.close()
        target_cache.close()

    return {
        "status": "ok",
        "data_tables": copied_data,
        "cache_tables": copied_cache_tables,
        "cache_keys": copied_cache_keys,
    }


def cleanup_workspace(root_dir: Path) -> None:
    root = Path(root_dir).expanduser().resolve()
    if root.exists():
        shutil.rmtree(root)


def prune_rebuild_workspaces(
    *,
    workspaces_root: Path,
    keep: int,
    preserve: Path | None = None,
) -> dict[str, Any]:
    root = Path(workspaces_root).expanduser().resolve()
    keep_count = max(0, int(keep))
    if keep_count <= 0:
        return {"status": "skipped", "reason": "retention_disabled", "kept": [], "removed": []}
    if not root.exists():
        return {"status": "skipped", "reason": "missing_root", "kept": [], "removed": []}

    preserve_path = Path(preserve).expanduser().resolve() if preserve is not None else None
    candidates = sorted(
        (
            child.resolve()
            for child in root.iterdir()
            if child.is_dir() and child.name.startswith("job_")
        ),
        key=lambda path: path.name,
        reverse=True,
    )
    kept: list[Path] = []
    if preserve_path is not None and preserve_path in candidates:
        kept.append(preserve_path)
    for candidate in candidates:
        if candidate in kept:
            continue
        if len(kept) >= keep_count:
            break
        kept.append(candidate)

    removed: list[str] = []
    for candidate in candidates:
        if candidate in kept:
            continue
        shutil.rmtree(candidate)
        removed.append(str(candidate))

    return {
        "status": "ok",
        "keep_count": keep_count,
        "kept": [str(path) for path in kept],
        "removed": removed,
    }
