"""Source-observation backfill helpers for the evolving universe model."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from backend.universe.classification_policy import NON_EQUITY_ECONOMIC_SECTORS
from backend.universe.registry_sync import (
    normalize_optional_text,
    normalize_ric,
    policy_defaults_for_legacy_coverage_role,
)
from backend.universe.taxonomy_builder import materialize_security_master_compat_current as _materialize_security_master_compat_current
from backend.universe.schema import (
    SECURITY_MASTER_COMPAT_CURRENT_TABLE,
    SECURITY_POLICY_CURRENT_TABLE,
    SECURITY_REGISTRY_TABLE,
    SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
)


def _legacy_policy_default_sql(flag: str) -> str:
    coverage_role = "COALESCE(NULLIF(TRIM(comp.coverage_role), ''), 'native_equity')"
    native_defaults = policy_defaults_for_legacy_coverage_role("native_equity")
    projection_defaults = policy_defaults_for_legacy_coverage_role("projection_only")
    if flag not in native_defaults or flag not in projection_defaults:
        raise KeyError(f"unsupported legacy source-observation flag: {flag}")
    native_default = int(native_defaults[flag])
    projection_default = int(projection_defaults[flag])
    if native_default == projection_default:
        default_expr = str(native_default)
    else:
        default_expr = (
            f"CASE WHEN {coverage_role} = 'projection_only' "
            f"THEN {projection_default} ELSE {native_default} END"
        )
    return f"COALESCE(pol.{flag}, {default_expr})"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
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


def _table_has_rows(conn: sqlite3.Connection, table: str) -> bool:
    if not _table_exists(conn, table):
        return False
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _resolve_as_of_date(conn: sqlite3.Connection, explicit_as_of_date: str | None = None) -> str:
    if explicit_as_of_date:
        return str(explicit_as_of_date)
    row = conn.execute(
        """
        SELECT MAX(latest_date)
        FROM (
            SELECT MAX(date) AS latest_date FROM security_prices_eod
            UNION ALL
            SELECT MAX(as_of_date) AS latest_date FROM security_fundamentals_pit
            UNION ALL
            SELECT MAX(as_of_date) AS latest_date FROM security_classification_pit
        )
        """
    ).fetchone()
    if row and row[0]:
        return str(row[0])
    return datetime.now(timezone.utc).date().isoformat()


def refresh_security_source_observation_daily(
    conn: sqlite3.Connection,
    *,
    as_of_date: str | None = None,
    rics: list[str] | None = None,
) -> int:
    effective_as_of_date = _resolve_as_of_date(conn, explicit_as_of_date=as_of_date)
    ric_filter = ""
    params: list[object] = []
    clean_rics = [normalize_ric(ric) for ric in (rics or []) if normalize_ric(ric)]
    use_registry_authority = _table_has_rows(conn, SECURITY_REGISTRY_TABLE)
    use_compat_authority = _table_has_rows(conn, SECURITY_MASTER_COMPAT_CURRENT_TABLE)
    if clean_rics:
        placeholders = ",".join("?" for _ in clean_rics)
        anchor_alias = "reg" if use_registry_authority else "comp"
        ric_filter = f" AND UPPER(TRIM({anchor_alias}.ric)) IN ({placeholders})"
        params.extend(clean_rics)
    base_query = f"""
        WITH latest_price AS (
            SELECT
                UPPER(TRIM(ric)) AS ric,
                MAX(TRIM(date)) AS latest_price_date
            FROM security_prices_eod
            WHERE date IS NOT NULL
              AND TRIM(date) <> ''
              AND TRIM(date) <= ?
            GROUP BY UPPER(TRIM(ric))
        ),
        latest_fundamentals AS (
            SELECT
                UPPER(TRIM(ric)) AS ric,
                MAX(TRIM(as_of_date)) AS latest_fundamentals_as_of_date
            FROM security_fundamentals_pit
            WHERE as_of_date IS NOT NULL
              AND TRIM(as_of_date) <> ''
              AND TRIM(as_of_date) <= ?
            GROUP BY UPPER(TRIM(ric))
        ),
        latest_classification AS (
            SELECT
                ric,
                as_of_date AS latest_classification_as_of_date,
                trbc_economic_sector,
                trbc_business_sector,
                trbc_industry_group,
                trbc_industry,
                trbc_activity,
                hq_country_code
            FROM (
                SELECT
                    UPPER(TRIM(ric)) AS ric,
                    TRIM(as_of_date) AS as_of_date,
                    NULLIF(TRIM(trbc_economic_sector), '') AS trbc_economic_sector,
                    NULLIF(TRIM(trbc_business_sector), '') AS trbc_business_sector,
                    NULLIF(TRIM(trbc_industry_group), '') AS trbc_industry_group,
                    NULLIF(TRIM(trbc_industry), '') AS trbc_industry,
                    NULLIF(TRIM(trbc_activity), '') AS trbc_activity,
                    NULLIF(TRIM(hq_country_code), '') AS hq_country_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(ric))
                        ORDER BY TRIM(as_of_date) DESC, updated_at DESC
                    ) AS rn
                FROM security_classification_pit
                WHERE as_of_date IS NOT NULL
                  AND TRIM(as_of_date) <> ''
                  AND TRIM(as_of_date) <= ?
            )
            WHERE rn = 1
        )
    """
    if use_registry_authority:
        rows = conn.execute(
            base_query
            + f"""
        SELECT
            UPPER(TRIM(reg.ric)) AS ric,
            CASE
                WHEN COALESCE(cr.trbc_economic_sector, cr.trbc_business_sector, cr.trbc_industry_group, cr.trbc_industry, cr.trbc_activity, cr.hq_country_code) IS NOT NULL THEN 1
                ELSE 0
            END AS classification_ready_base,
            CASE
                WHEN COALESCE(cr.trbc_economic_sector, cr.trbc_business_sector, cr.trbc_industry_group, cr.trbc_industry, cr.trbc_activity, cr.hq_country_code) IS NULL THEN 0
                WHEN COALESCE(cr.trbc_economic_sector, '') IN ({",".join("?" for _ in NON_EQUITY_ECONOMIC_SECTORS)}) THEN 0
                ELSE 1
            END AS is_equity_eligible_base,
            pol.price_ingest_enabled AS price_ingest_enabled,
            pol.pit_fundamentals_enabled AS pit_fundamentals_enabled,
            pol.pit_classification_enabled AS pit_classification_enabled,
            pol.policy_source AS policy_source,
            COALESCE(reg.source, pol.policy_source, '') AS source,
            COALESCE(reg.job_run_id, pol.job_run_id) AS job_run_id,
            COALESCE(reg.updated_at, pol.updated_at) AS updated_at,
            pr.latest_price_date,
            fr.latest_fundamentals_as_of_date,
            cr.latest_classification_as_of_date
        FROM {SECURITY_REGISTRY_TABLE} reg
        LEFT JOIN {SECURITY_POLICY_CURRENT_TABLE} pol
          ON UPPER(TRIM(pol.ric)) = UPPER(TRIM(reg.ric))
        LEFT JOIN latest_price pr
          ON pr.ric = UPPER(TRIM(reg.ric))
        LEFT JOIN latest_fundamentals fr
          ON fr.ric = UPPER(TRIM(reg.ric))
        LEFT JOIN latest_classification cr
          ON cr.ric = UPPER(TRIM(reg.ric))
        WHERE reg.ric IS NOT NULL AND TRIM(reg.ric) <> ''
          AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
          {ric_filter}
        """,
            [
                effective_as_of_date,
                effective_as_of_date,
                effective_as_of_date,
                *sorted(NON_EQUITY_ECONOMIC_SECTORS),
                *params,
            ],
        ).fetchall()
    elif use_compat_authority:
        rows = conn.execute(
            base_query
            + f"""
        SELECT
            UPPER(TRIM(comp.ric)) AS ric,
            COALESCE(comp.classification_ok, 0) AS classification_ready_base,
            COALESCE(comp.is_equity_eligible, 0) AS is_equity_eligible_base,
            {_legacy_policy_default_sql('price_ingest_enabled')} AS price_ingest_enabled,
            {_legacy_policy_default_sql('pit_fundamentals_enabled')} AS pit_fundamentals_enabled,
            {_legacy_policy_default_sql('pit_classification_enabled')} AS pit_classification_enabled,
            NULL AS policy_source,
            COALESCE(comp.source, '') AS source,
            comp.job_run_id,
            comp.updated_at,
            pr.latest_price_date,
            fr.latest_fundamentals_as_of_date,
            cr.latest_classification_as_of_date
        FROM {SECURITY_MASTER_COMPAT_CURRENT_TABLE} comp
        LEFT JOIN {SECURITY_POLICY_CURRENT_TABLE} pol
          ON UPPER(TRIM(pol.ric)) = UPPER(TRIM(comp.ric))
        LEFT JOIN latest_price pr
          ON pr.ric = UPPER(TRIM(comp.ric))
        LEFT JOIN latest_fundamentals fr
          ON fr.ric = UPPER(TRIM(comp.ric))
        LEFT JOIN latest_classification cr
          ON cr.ric = UPPER(TRIM(comp.ric))
        WHERE comp.ric IS NOT NULL AND TRIM(comp.ric) <> ''
          {ric_filter}
        """,
            [effective_as_of_date, effective_as_of_date, effective_as_of_date, *params],
        ).fetchall()
    else:
        rows = []
    payload: list[tuple[object, ...]] = []
    for row in rows:
        ric = normalize_ric(row[0])
        if not ric:
            continue
        latest_price_date = normalize_optional_text(row[10])
        latest_fundamentals_as_of_date = normalize_optional_text(row[11])
        latest_classification_as_of_date = normalize_optional_text(row[12])
        has_price_history_as_of_date = 1 if latest_price_date else 0
        has_fundamentals_history_as_of_date = 1 if latest_fundamentals_as_of_date else 0
        has_classification_history_as_of_date = 1 if latest_classification_as_of_date else 0
        derived_equity_eligible = 1 if int(row[2] or 0) == 1 and has_classification_history_as_of_date == 1 else 0
        price_ingest_enabled = int(row[3]) if row[3] is not None else 1
        pit_fundamentals_enabled = (
            int(row[4])
            if row[4] is not None
            else derived_equity_eligible
        )
        pit_classification_enabled = (
            int(row[5])
            if row[5] is not None
            else derived_equity_eligible
        )
        payload.append(
            (
                effective_as_of_date,
                ric,
                1 if int(row[1] or 0) == 1 and has_classification_history_as_of_date == 1 else 0,
                derived_equity_eligible,
                price_ingest_enabled,
                pit_fundamentals_enabled,
                pit_classification_enabled,
                has_price_history_as_of_date,
                has_fundamentals_history_as_of_date,
                has_classification_history_as_of_date,
                latest_price_date,
                latest_fundamentals_as_of_date,
                latest_classification_as_of_date,
                normalize_optional_text(row[7]),
                normalize_optional_text(row[8]),
                normalize_optional_text(row[9]),
            )
        )
    if not payload:
        return 0
    if clean_rics:
        placeholders = ",".join("?" for _ in clean_rics)
        conn.execute(
            f"DELETE FROM {SECURITY_SOURCE_OBSERVATION_DAILY_TABLE} WHERE as_of_date = ? AND ric IN ({placeholders})",
            [effective_as_of_date, *clean_rics],
        )
    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {SECURITY_SOURCE_OBSERVATION_DAILY_TABLE} (
            as_of_date,
            ric,
            classification_ready,
            is_equity_eligible,
            price_ingest_enabled,
            pit_fundamentals_enabled,
            pit_classification_enabled,
            has_price_history_as_of_date,
            has_fundamentals_history_as_of_date,
            has_classification_history_as_of_date,
            latest_price_date,
            latest_fundamentals_as_of_date,
            latest_classification_as_of_date,
            source,
            job_run_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def materialize_security_master_compat_current(
    conn: sqlite3.Connection,
    *,
    rics: list[str] | None = None,
) -> int:
    return _materialize_security_master_compat_current(conn, rics=rics)
