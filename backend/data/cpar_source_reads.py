"""Read-only shared-source queries used by cPAR package building."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

from backend import config
from backend.data import core_read_backend as core_backend

DATA_DB = Path(config.DATA_DB_PATH)


def _resolve_data_db(data_db: Path | None = None) -> Path:
    return Path(data_db or DATA_DB).expanduser().resolve()


def _fetch_rows(
    sql: str,
    params: list[Any] | None = None,
    *,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    return core_backend.fetch_rows(
        sql,
        params,
        data_db=_resolve_data_db(data_db),
        neon_enabled=core_backend.use_neon_core_reads(),
    )


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


def resolve_factor_proxy_rows(
    factor_tickers: Iterable[str],
    *,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    clean = _normalize_tokens(factor_tickers)
    if not clean:
        return []
    placeholders, params = _in_clause(clean)
    return _fetch_rows(
        f"""
        SELECT ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        FROM security_master
        WHERE UPPER(COALESCE(ticker, '')) IN ({placeholders})
        ORDER BY UPPER(COALESCE(ticker, '')), ric
        """,
        params,
        data_db=data_db,
    )


def load_build_universe_rows(*, data_db: Path | None = None) -> list[dict[str, Any]]:
    return _fetch_rows(
        """
        SELECT ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        FROM security_master
        WHERE TRIM(COALESCE(ticker, '')) <> ''
        ORDER BY UPPER(COALESCE(ticker, '')), ric
        """,
        data_db=data_db,
    )


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
