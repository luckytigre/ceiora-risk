"""Read-only shared-source queries used by cPAR package building."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
import sqlite3
from typing import Any

from backend import config
from backend.data import core_read_backend as core_backend

DATA_DB = Path(config.DATA_DB_PATH)


class CparSourceReadError(RuntimeError):
    """Raised when the shared source-read surface is unavailable."""


def _resolve_data_db(data_db: Path | None = None) -> Path:
    return Path(data_db or DATA_DB).expanduser().resolve()


def _fetch_rows(
    sql: str,
    params: list[Any] | None = None,
    *,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    try:
        return core_backend.fetch_rows(
            sql,
            params,
            data_db=_resolve_data_db(data_db),
            neon_enabled=core_backend.use_neon_core_reads(),
        )
    except Exception as exc:
        raise CparSourceReadError(
            f"Shared cPAR source read failed: {type(exc).__name__}: {exc}"
        ) from exc


def _normalize_tokens(values: Iterable[str] | None) -> list[str]:
    clean: list[str] = []
    seen: set[str] = set()
    for raw in values or ():
        token = str(raw or "").strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        clean.append(token)
    return clean


def _in_clause(values: list[str]) -> tuple[str, list[Any]]:
    if not values:
        raise ValueError("at least one filter value is required")
    return ",".join("?" for _ in values), list(values)


def _sqlite_table_exists(data_db: Path, table: str) -> bool:
    if core_backend.use_neon_core_reads():
        return False
    resolved = _resolve_data_db(data_db)
    if not resolved.exists():
        return False
    conn = sqlite3.connect(str(resolved))
    try:
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
    finally:
        conn.close()


def _pg_tables_exist(*, data_db: Path | None = None, tables: tuple[str, ...]) -> bool:
    try:
        rows = _fetch_rows(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            """,
            [list(tables)],
            data_db=data_db,
        )
    except CparSourceReadError:
        return False
    found = {str(row.get("table_name") or "").strip() for row in rows}
    return all(table in found for table in tables)


def _registry_query_tables_available(
    *,
    data_db: Path | None = None,
    required_tables: tuple[str, ...],
) -> bool:
    if core_backend.use_neon_core_reads():
        return _pg_tables_exist(data_db=data_db, tables=required_tables)
    resolved = _resolve_data_db(data_db)
    if not all(_sqlite_table_exists(resolved, table) for table in required_tables):
        return False
    return True


def _compat_metadata_available(*, data_db: Path | None = None) -> bool:
    return _registry_query_tables_available(
        data_db=data_db,
        required_tables=("security_master_compat_current",),
    )


def _load_registry_factor_proxy_rows(
    *,
    placeholders: str,
    params: list[Any],
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    compat_join = ""
    equity_priority_expr = "0"
    compat_select = (
        "0 AS classification_ok,\n            0 AS is_equity_eligible,\n"
        "            reg.source AS source,\n            reg.job_run_id AS job_run_id,\n            reg.updated_at AS updated_at"
    )
    if _compat_metadata_available(data_db=data_db):
        compat_join = """
        LEFT JOIN security_master_compat_current compat
          ON UPPER(COALESCE(compat.ric, '')) = UPPER(COALESCE(reg.ric, ''))
        """
        compat_select = """
            COALESCE(compat.classification_ok, 0) AS classification_ok,
            COALESCE(compat.is_equity_eligible, 0) AS is_equity_eligible,
            COALESCE(reg.source, compat.source) AS source,
            COALESCE(reg.job_run_id, compat.job_run_id) AS job_run_id,
            COALESCE(reg.updated_at, compat.updated_at) AS updated_at
        """.strip()
        equity_priority_expr = "COALESCE(compat.is_equity_eligible, 0)"
    return _fetch_rows(
        f"""
        WITH ranked_candidates AS (
            SELECT
                reg.ric,
                reg.ticker,
                reg.isin,
                reg.exchange_name,
                {compat_select},
                COALESCE(pol.allow_cpar_core_target, 0) AS allow_cpar_core_target,
                COALESCE(pol.allow_cpar_extended_target, 0) AS allow_cpar_extended_target,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(COALESCE(reg.ticker, ''))
                    ORDER BY
                        COALESCE(pol.allow_cpar_core_target, 0) DESC,
                        COALESCE(pol.allow_cpar_extended_target, 0) DESC,
                        {equity_priority_expr} DESC,
                        CASE
                            WHEN UPPER(COALESCE(reg.exchange_name, '')) LIKE '%%CONSOLIDATED%%' THEN 1
                            ELSE 0
                        END ASC,
                        UPPER(COALESCE(reg.exchange_name, '')) ASC,
                        reg.ric ASC
                ) AS proxy_rank
            FROM security_registry reg
            LEFT JOIN security_policy_current pol
              ON UPPER(COALESCE(pol.ric, '')) = UPPER(COALESCE(reg.ric, ''))
            {compat_join}
            WHERE UPPER(COALESCE(reg.ticker, '')) IN ({placeholders})
              AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') = 'active'
        )
        SELECT
            ric,
            ticker,
            isin,
            exchange_name,
            classification_ok,
            is_equity_eligible,
            source,
            job_run_id,
            updated_at
        FROM ranked_candidates
        WHERE proxy_rank = 1
        ORDER BY UPPER(COALESCE(ticker, '')), ric
        """,
        params,
        data_db=data_db,
    )


def _load_registry_build_universe_rows(*, data_db: Path | None = None) -> list[dict[str, Any]]:
    compat_join = ""
    compat_select = (
        "0 AS classification_ok,\n            0 AS is_equity_eligible,\n"
        "            reg.source AS source,\n            reg.job_run_id AS job_run_id,\n            reg.updated_at AS updated_at"
    )
    core_target_expr = "COALESCE(pol.allow_cpar_core_target, 0)"
    extended_target_expr = "COALESCE(pol.allow_cpar_extended_target, 0)"
    single_name_expr = "0"
    if _compat_metadata_available(data_db=data_db):
        compat_join = """
        LEFT JOIN security_master_compat_current compat
          ON UPPER(COALESCE(compat.ric, '')) = UPPER(COALESCE(reg.ric, ''))
        """
        compat_select = """
            COALESCE(compat.classification_ok, 0) AS classification_ok,
            COALESCE(compat.is_equity_eligible, 0) AS is_equity_eligible,
            COALESCE(reg.source, compat.source) AS source,
            COALESCE(reg.job_run_id, compat.job_run_id) AS job_run_id,
            COALESCE(reg.updated_at, compat.updated_at) AS updated_at
        """.strip()
        single_name_expr = "CASE WHEN COALESCE(compat.is_equity_eligible, 0) = 1 THEN 1 ELSE 0 END"
    return _fetch_rows(
        """
        SELECT
            reg.ric,
            reg.ticker,
            reg.isin,
            reg.exchange_name,
            """
        + core_target_expr
        + """ AS allow_cpar_core_target,
            """
        + extended_target_expr
        + """ AS allow_cpar_extended_target,
            """
        + single_name_expr
        + """ AS is_single_name_equity,
            """
        + compat_select
        + """
        FROM security_registry reg
        LEFT JOIN security_policy_current pol
          ON UPPER(COALESCE(pol.ric, '')) = UPPER(COALESCE(reg.ric, ''))
        """
        + compat_join
        + """
        WHERE TRIM(COALESCE(reg.ticker, '')) <> ''
          AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') = 'active'
          AND (
            """
        + core_target_expr
        + """ = 1
            OR """
        + extended_target_expr
        + """ = 1
          )
        ORDER BY UPPER(COALESCE(reg.ticker, '')), reg.ric
        """,
        data_db=data_db,
    )

def resolve_factor_proxy_rows(
    factor_tickers: Iterable[str],
    *,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    clean = _normalize_tokens(factor_tickers)
    if not clean:
        return []
    placeholders, params = _in_clause(clean)
    if not _registry_query_tables_available(data_db=data_db, required_tables=("security_registry",)):
        raise CparSourceReadError(
            "Shared cPAR factor-proxy read requires security_registry to be present."
        )
    return _load_registry_factor_proxy_rows(
        placeholders=placeholders,
        params=params,
        data_db=data_db,
    )


def load_build_universe_rows(*, data_db: Path | None = None) -> list[dict[str, Any]]:
    if not _registry_query_tables_available(
        data_db=data_db,
        required_tables=("security_registry", "security_policy_current"),
    ):
        raise CparSourceReadError(
            "Shared cPAR build-universe read requires security_registry and security_policy_current."
        )
    return _load_registry_build_universe_rows(data_db=data_db)


def load_price_rows_for_rics(
    rics: Iterable[str],
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    clean_rics = _normalize_tokens(rics)
    if not clean_rics:
        return []
    placeholders, params = _in_clause(clean_rics)
    clauses = [f"UPPER(COALESCE(ric, '')) IN ({placeholders})"]
    if date_from:
        clauses.append("date >= ?")
        params.append(str(date_from))
    if date_to:
        clauses.append("date <= ?")
        params.append(str(date_to))
    where_clause = " AND ".join(clauses)
    return _fetch_rows(
        f"""
        SELECT ric, date, open, high, low, close, adj_close, volume, currency, source, updated_at
        FROM security_prices_eod
        WHERE {where_clause}
        ORDER BY ric, date
        """,
        params,
        data_db=data_db,
    )


def load_latest_price_rows(
    rics: Iterable[str],
    *,
    as_of_date: str,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    clean_rics = _normalize_tokens(rics)
    if not clean_rics:
        return []
    placeholders, ric_params = _in_clause(clean_rics)
    params = [str(as_of_date), *ric_params]
    return _fetch_rows(
        f"""
        WITH ranked AS (
            SELECT
                p.ric,
                p.date,
                p.open,
                p.high,
                p.low,
                p.close,
                p.adj_close,
                p.volume,
                p.currency,
                p.source,
                p.updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(COALESCE(p.ric, ''))
                    ORDER BY p.date DESC, p.updated_at DESC
                ) AS rn
            FROM security_prices_eod p
            WHERE p.date <= ?
              AND UPPER(COALESCE(p.ric, '')) IN ({placeholders})
        )
        SELECT ric, date, open, high, low, close, adj_close, volume, currency, source, updated_at
        FROM ranked
        WHERE rn = 1
        ORDER BY ric
        """,
        params,
        data_db=data_db,
    )


def load_latest_classification_rows(
    rics: Iterable[str],
    *,
    as_of_date: str,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    clean_rics = _normalize_tokens(rics)
    if not clean_rics:
        return []
    placeholders, ric_params = _in_clause(clean_rics)
    params = [str(as_of_date), *ric_params]
    return _fetch_rows(
        f"""
        WITH ranked AS (
            SELECT
                c.ric,
                c.as_of_date,
                c.trbc_economic_sector,
                c.trbc_business_sector,
                c.trbc_industry_group,
                c.trbc_industry,
                c.trbc_activity,
                c.hq_country_code,
                c.source,
                c.job_run_id,
                c.updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(COALESCE(c.ric, ''))
                    ORDER BY c.as_of_date DESC, c.updated_at DESC
                ) AS rn
            FROM security_classification_pit c
            WHERE c.as_of_date <= ?
              AND UPPER(COALESCE(c.ric, '')) IN ({placeholders})
        )
        SELECT ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group,
               trbc_industry, trbc_activity, hq_country_code, source, job_run_id, updated_at
        FROM ranked
        WHERE rn = 1
        ORDER BY ric
        """,
        params,
        data_db=data_db,
    )


def load_latest_common_name_rows(
    rics: Iterable[str],
    *,
    as_of_date: str,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    clean_rics = _normalize_tokens(rics)
    if not clean_rics:
        return []
    placeholders, ric_params = _in_clause(clean_rics)
    params = [str(as_of_date), *ric_params]
    return _fetch_rows(
        f"""
        WITH ranked AS (
            SELECT
                f.ric,
                f.as_of_date,
                f.common_name,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(COALESCE(f.ric, ''))
                    ORDER BY f.as_of_date DESC, f.stat_date DESC, f.updated_at DESC
                ) AS rn
            FROM security_fundamentals_pit f
            WHERE f.as_of_date <= ?
              AND UPPER(COALESCE(f.ric, '')) IN ({placeholders})
        )
        SELECT ric, as_of_date, common_name
        FROM ranked
        WHERE rn = 1
        ORDER BY ric
        """,
        params,
        data_db=data_db,
    )
