"""Registry-backed quote/search reads for explore and what-if surfaces."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend import config
from backend.data import core_read_backend as core_backend

DATA_DB = Path(config.DATA_DB_PATH)

_REQUIRED_TABLES = ("security_registry", "security_policy_current")
_OPTIONAL_TABLES = (
    "security_taxonomy_current",
    "security_source_observation_daily",
    "security_classification_pit",
    "security_fundamentals_pit",
    "security_prices_eod",
)


class RegistryQuoteReadError(RuntimeError):
    """Raised when the registry-backed quote/search read surface is unavailable."""


def _resolve_data_db(data_db: Path | None = None) -> Path:
    return Path(data_db or DATA_DB).expanduser().resolve()


def _today_iso() -> str:
    return date.today().isoformat()


def _clean_tokens(values: Iterable[str] | None) -> list[str]:
    clean: list[str] = []
    seen: set[str] = set()
    for raw in values or ():
        token = str(raw or "").strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        clean.append(token)
    return clean


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
        raise RegistryQuoteReadError(
            f"Registry quote read failed: {type(exc).__name__}: {exc}"
        ) from exc


def _table_exists(table: str, *, data_db: Path | None = None) -> bool:
    return core_backend.table_exists(
        table,
        fetch_rows_fn=lambda sql, params=None: _fetch_rows(sql, params, data_db=data_db),
        neon_enabled=core_backend.use_neon_core_reads(),
    )


@lru_cache(maxsize=16)
def _cached_available_tables(
    data_db_key: str,
    data_db_revision: tuple[int | None, int | None],
    neon_enabled: bool,
) -> tuple[str, ...]:
    resolved_data_db = Path(data_db_key)

    def _table_exists_cached(table: str) -> bool:
        return core_backend.table_exists(
            table,
            fetch_rows_fn=lambda sql, params=None: _fetch_rows(sql, params, data_db=resolved_data_db),
            neon_enabled=neon_enabled,
        )

    available = tuple(table for table in _OPTIONAL_TABLES if _table_exists_cached(table))
    missing = tuple(table for table in _REQUIRED_TABLES if not _table_exists_cached(table))
    if missing:
        raise RegistryQuoteReadError(
            "Registry quote read requires tables: " + ", ".join(_REQUIRED_TABLES)
            + f"; missing {', '.join(missing)}"
        )
    return available


def _ensure_required_tables(*, data_db: Path | None = None) -> set[str]:
    resolved_data_db = _resolve_data_db(data_db)
    try:
        stat = resolved_data_db.stat()
        data_db_revision = (int(stat.st_mtime_ns), int(stat.st_size), int(stat.st_ino))
    except OSError:
        data_db_revision = (None, None, None)
    neon_enabled = bool(core_backend.use_neon_core_reads())
    return set(_cached_available_tables(str(resolved_data_db), data_db_revision, neon_enabled))


def _query_registry_rows(
    *,
    q: str | None = None,
    limit: int | None = None,
    tickers: Iterable[str] | None = None,
    rics: Iterable[str] | None = None,
    as_of_date: str | None = None,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    available = _ensure_required_tables(data_db=data_db)
    clean_q = str(q or "").strip().upper()
    needle = f"%{clean_q}%" if clean_q else None
    clean_tickers = _clean_tokens(tickers)
    clean_rics = _clean_tokens(rics)
    search_limit = int(limit or 0)
    anchor_date = str(as_of_date or _today_iso()).strip() or _today_iso()

    params: list[Any] = []
    ctes: list[str] = []
    joins: list[str] = [
        """
        LEFT JOIN security_policy_current pol
          ON UPPER(TRIM(COALESCE(pol.ric, ''))) = UPPER(TRIM(COALESCE(reg.ric, '')))
        """
    ]
    selects: list[str] = [
        "UPPER(TRIM(COALESCE(reg.ric, ''))) AS ric",
        "NULLIF(UPPER(TRIM(COALESCE(reg.ticker, ''))), '') AS ticker",
        "NULLIF(TRIM(COALESCE(reg.isin, '')), '') AS isin",
        "NULLIF(TRIM(COALESCE(reg.exchange_name, '')), '') AS exchange_name",
        "COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') AS tracking_status",
        "COALESCE(pol.price_ingest_enabled, 0) AS price_ingest_enabled",
        "COALESCE(pol.pit_fundamentals_enabled, 0) AS pit_fundamentals_enabled",
        "COALESCE(pol.pit_classification_enabled, 0) AS pit_classification_enabled",
        "COALESCE(pol.allow_cuse_native_core, 0) AS allow_cuse_native_core",
        "COALESCE(pol.allow_cuse_fundamental_projection, 0) AS allow_cuse_fundamental_projection",
        "COALESCE(pol.allow_cuse_returns_projection, 0) AS allow_cuse_returns_projection",
        "COALESCE(pol.allow_cpar_core_target, 0) AS allow_cpar_core_target",
        "COALESCE(pol.allow_cpar_extended_target, 0) AS allow_cpar_extended_target",
    ]

    if "security_taxonomy_current" in available:
        joins.append(
            """
            LEFT JOIN security_taxonomy_current tax
              ON UPPER(TRIM(COALESCE(tax.ric, ''))) = UPPER(TRIM(COALESCE(reg.ric, '')))
            """
        )
        selects.extend(
            [
                "NULLIF(TRIM(COALESCE(tax.instrument_kind, '')), '') AS instrument_kind",
                "NULLIF(TRIM(COALESCE(tax.vehicle_structure, '')), '') AS vehicle_structure",
                "NULLIF(TRIM(COALESCE(tax.issuer_country_code, '')), '') AS issuer_country_code",
                "NULLIF(TRIM(COALESCE(tax.listing_country_code, '')), '') AS listing_country_code",
                "NULLIF(TRIM(COALESCE(tax.model_home_market_scope, '')), '') AS model_home_market_scope",
                "COALESCE(tax.is_single_name_equity, 0) AS is_single_name_equity",
                "COALESCE(tax.classification_ready, 0) AS classification_ready",
            ]
        )
    else:
        selects.extend(
            [
                "NULL AS instrument_kind",
                "NULL AS vehicle_structure",
                "NULL AS issuer_country_code",
                "NULL AS listing_country_code",
                "NULL AS model_home_market_scope",
                "0 AS is_single_name_equity",
                "0 AS classification_ready",
            ]
        )

    if "security_source_observation_daily" in available:
        ctes.append(
            """
            latest_observation AS (
                SELECT
                    ric,
                    as_of_date,
                    has_price_history_as_of_date,
                    has_fundamentals_history_as_of_date,
                    has_classification_history_as_of_date,
                    latest_price_date,
                    latest_fundamentals_as_of_date,
                    latest_classification_as_of_date
                FROM (
                    SELECT
                        UPPER(TRIM(COALESCE(obs.ric, ''))) AS ric,
                        obs.as_of_date,
                        COALESCE(obs.has_price_history_as_of_date, 0) AS has_price_history_as_of_date,
                        COALESCE(obs.has_fundamentals_history_as_of_date, 0) AS has_fundamentals_history_as_of_date,
                        COALESCE(obs.has_classification_history_as_of_date, 0) AS has_classification_history_as_of_date,
                        obs.latest_price_date,
                        obs.latest_fundamentals_as_of_date,
                        obs.latest_classification_as_of_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(TRIM(COALESCE(obs.ric, '')))
                            ORDER BY obs.as_of_date DESC, obs.updated_at DESC
                        ) AS rn
                    FROM security_source_observation_daily obs
                    WHERE obs.as_of_date <= ?
                ) ranked
                WHERE rn = 1
            )
            """
        )
        params.append(anchor_date)
        joins.append(
            """
            LEFT JOIN latest_observation obs
              ON obs.ric = UPPER(TRIM(COALESCE(reg.ric, '')))
            """
        )
        selects.extend(
            [
                "obs.as_of_date AS observation_as_of_date",
                "COALESCE(obs.has_price_history_as_of_date, 0) AS has_price_history_as_of_date",
                "COALESCE(obs.has_fundamentals_history_as_of_date, 0) AS has_fundamentals_history_as_of_date",
                "COALESCE(obs.has_classification_history_as_of_date, 0) AS has_classification_history_as_of_date",
                "obs.latest_price_date AS latest_price_date",
                "obs.latest_fundamentals_as_of_date AS latest_fundamentals_as_of_date",
                "obs.latest_classification_as_of_date AS latest_classification_as_of_date",
            ]
        )
    else:
        selects.extend(
            [
                "NULL AS observation_as_of_date",
                "0 AS has_price_history_as_of_date",
                "0 AS has_fundamentals_history_as_of_date",
                "0 AS has_classification_history_as_of_date",
                "NULL AS latest_price_date",
                "NULL AS latest_fundamentals_as_of_date",
                "NULL AS latest_classification_as_of_date",
            ]
        )

    if "security_fundamentals_pit" in available:
        ctes.append(
            """
            latest_common_name AS (
                SELECT ric, as_of_date, common_name
                FROM (
                    SELECT
                        UPPER(TRIM(COALESCE(f.ric, ''))) AS ric,
                        f.as_of_date,
                        NULLIF(TRIM(COALESCE(f.common_name, '')), '') AS common_name,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(TRIM(COALESCE(f.ric, '')))
                            ORDER BY f.as_of_date DESC, f.stat_date DESC, f.updated_at DESC
                        ) AS rn
                    FROM security_fundamentals_pit f
                    WHERE f.as_of_date <= ?
                ) ranked
                WHERE rn = 1
            )
            """
        )
        params.append(anchor_date)
        joins.append(
            """
            LEFT JOIN latest_common_name nm
              ON nm.ric = UPPER(TRIM(COALESCE(reg.ric, '')))
            """
        )
        selects.extend(
            [
                "nm.common_name AS common_name",
                "nm.as_of_date AS common_name_as_of_date",
            ]
        )
        name_search_expr = "UPPER(COALESCE(nm.common_name, '')) LIKE ?"
    else:
        selects.extend(
            [
                "NULL AS common_name",
                "NULL AS common_name_as_of_date",
            ]
        )
        name_search_expr = "0 = 1"

    if "security_classification_pit" in available:
        ctes.append(
            """
            latest_classification AS (
                SELECT
                    ric,
                    as_of_date,
                    trbc_economic_sector,
                    trbc_business_sector,
                    trbc_industry_group,
                    trbc_industry,
                    trbc_activity,
                    hq_country_code
                FROM (
                    SELECT
                        UPPER(TRIM(COALESCE(c.ric, ''))) AS ric,
                        c.as_of_date,
                        NULLIF(TRIM(COALESCE(c.trbc_economic_sector, '')), '') AS trbc_economic_sector,
                        NULLIF(TRIM(COALESCE(c.trbc_business_sector, '')), '') AS trbc_business_sector,
                        NULLIF(TRIM(COALESCE(c.trbc_industry_group, '')), '') AS trbc_industry_group,
                        NULLIF(TRIM(COALESCE(c.trbc_industry, '')), '') AS trbc_industry,
                        NULLIF(TRIM(COALESCE(c.trbc_activity, '')), '') AS trbc_activity,
                        NULLIF(TRIM(COALESCE(c.hq_country_code, '')), '') AS hq_country_code,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(TRIM(COALESCE(c.ric, '')))
                            ORDER BY c.as_of_date DESC, c.updated_at DESC
                        ) AS rn
                    FROM security_classification_pit c
                    WHERE c.as_of_date <= ?
                ) ranked
                WHERE rn = 1
            )
            """
        )
        params.append(anchor_date)
        joins.append(
            """
            LEFT JOIN latest_classification cls
              ON cls.ric = UPPER(TRIM(COALESCE(reg.ric, '')))
            """
        )
        selects.extend(
            [
                "cls.as_of_date AS classification_as_of_date",
                "cls.trbc_economic_sector AS trbc_economic_sector",
                "cls.trbc_business_sector AS trbc_business_sector",
                "cls.trbc_industry_group AS trbc_industry_group",
                "cls.trbc_industry AS trbc_industry",
                "cls.trbc_activity AS trbc_activity",
                "cls.hq_country_code AS hq_country_code",
            ]
        )
    else:
        selects.extend(
            [
                "NULL AS classification_as_of_date",
                "NULL AS trbc_economic_sector",
                "NULL AS trbc_business_sector",
                "NULL AS trbc_industry_group",
                "NULL AS trbc_industry",
                "NULL AS trbc_activity",
                "NULL AS hq_country_code",
            ]
        )

    if "security_prices_eod" in available:
        ctes.append(
            """
            latest_price AS (
                SELECT
                    ric,
                    date,
                    price,
                    price_field_used,
                    currency
                FROM (
                    SELECT
                        UPPER(TRIM(COALESCE(p.ric, ''))) AS ric,
                        p.date,
                        CASE
                            WHEN p.adj_close IS NOT NULL THEN p.adj_close
                            ELSE p.close
                        END AS price,
                        CASE
                            WHEN p.adj_close IS NOT NULL THEN 'adj_close'
                            WHEN p.close IS NOT NULL THEN 'close'
                            ELSE NULL
                        END AS price_field_used,
                        p.currency,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(TRIM(COALESCE(p.ric, '')))
                            ORDER BY p.date DESC, p.updated_at DESC
                        ) AS rn
                    FROM security_prices_eod p
                    WHERE p.date <= ?
                ) ranked
                WHERE rn = 1
            )
            """
        )
        params.append(anchor_date)
        joins.append(
            """
            LEFT JOIN latest_price px
              ON px.ric = UPPER(TRIM(COALESCE(reg.ric, '')))
            """
        )
        selects.extend(
            [
                "px.date AS price_date",
                "px.price AS price",
                "px.price_field_used AS price_field_used",
                "px.currency AS price_currency",
            ]
        )
    else:
        selects.extend(
            [
                "NULL AS price_date",
                "NULL AS price",
                "NULL AS price_field_used",
                "NULL AS price_currency",
            ]
        )

    where_clauses = [
        "reg.ric IS NOT NULL",
        "TRIM(COALESCE(reg.ric, '')) <> ''",
        "COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') = 'active'",
    ]
    if clean_q:
        where_clauses.append(
            "(\n"
            "                UPPER(COALESCE(reg.ticker, '')) LIKE ?\n"
            "                OR UPPER(COALESCE(reg.ric, '')) LIKE ?\n"
            f"                OR {name_search_expr}\n"
            "            )"
        )
        params.extend([needle, needle])
        if "security_fundamentals_pit" in available:
            params.append(needle)
    if clean_tickers:
        placeholders = ",".join("?" for _ in clean_tickers)
        where_clauses.append(f"UPPER(COALESCE(reg.ticker, '')) IN ({placeholders})")
        params.extend(clean_tickers)
    if clean_rics:
        placeholders = ",".join("?" for _ in clean_rics)
        where_clauses.append(f"UPPER(COALESCE(reg.ric, '')) IN ({placeholders})")
        params.extend(clean_rics)

    sql = ""
    if ctes:
        sql += "WITH " + ",\n".join(ctes) + "\n"
    sql += """
    SELECT
        """
    sql += ",\n        ".join(selects)
    sql += """
    FROM security_registry reg
    """
    sql += "\n".join(joins)
    sql += "\nWHERE " + "\n  AND ".join(where_clauses)
    sql += "\nORDER BY UPPER(COALESCE(reg.ticker, '')) ASC, UPPER(COALESCE(reg.ric, '')) ASC"
    if search_limit > 0:
        sql += "\nLIMIT ?"
        params.append(search_limit)

    return _fetch_rows(sql, params, data_db=data_db)


def search_registry_quote_rows(
    q: str,
    *,
    limit: int,
    as_of_date: str | None = None,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    clean_q = str(q or "").strip()
    if not clean_q:
        return []
    return _query_registry_rows(
        q=clean_q,
        limit=max(int(limit), 1),
        as_of_date=as_of_date,
        data_db=data_db,
    )


def load_registry_quote_rows_for_tickers(
    tickers: Iterable[str],
    *,
    as_of_date: str | None = None,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    clean_tickers = _clean_tokens(tickers)
    if not clean_tickers:
        return []
    return _query_registry_rows(
        tickers=clean_tickers,
        as_of_date=as_of_date,
        data_db=data_db,
    )


def load_registry_quote_rows_for_rics(
    rics: Iterable[str],
    *,
    as_of_date: str | None = None,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    clean_rics = _clean_tokens(rics)
    if not clean_rics:
        return []
    return _query_registry_rows(
        rics=clean_rics,
        as_of_date=as_of_date,
        data_db=data_db,
    )
