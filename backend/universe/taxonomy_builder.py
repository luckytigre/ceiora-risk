"""Taxonomy and compatibility materialization helpers."""

from __future__ import annotations

import sqlite3

from backend.universe.classification_policy import NON_EQUITY_ECONOMIC_SECTORS
from backend.universe.registry_sync import (
    normalize_optional_text,
    normalize_ric,
)
from backend.universe.schema import (
    SECURITY_MASTER_COMPAT_CURRENT_TABLE,
    SECURITY_MASTER_TABLE,
    SECURITY_POLICY_CURRENT_TABLE,
    SECURITY_REGISTRY_TABLE,
    SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
    SECURITY_TAXONOMY_CURRENT_TABLE,
)


def _latest_classification_by_ric(conn: sqlite3.Connection) -> dict[str, dict[str, str | None]]:
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                UPPER(TRIM(ric)) AS ric,
                NULLIF(TRIM(trbc_economic_sector), '') AS trbc_economic_sector,
                NULLIF(TRIM(trbc_business_sector), '') AS trbc_business_sector,
                NULLIF(TRIM(trbc_industry_group), '') AS trbc_industry_group,
                NULLIF(TRIM(trbc_industry), '') AS trbc_industry,
                NULLIF(TRIM(trbc_activity), '') AS trbc_activity,
                NULLIF(TRIM(hq_country_code), '') AS hq_country_code,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(TRIM(ric))
                    ORDER BY TRIM(as_of_date) DESC, rowid DESC
                ) AS rn
            FROM security_classification_pit
            WHERE ric IS NOT NULL AND TRIM(ric) <> ''
        )
        SELECT
            ric,
            trbc_economic_sector,
            trbc_business_sector,
            trbc_industry_group,
            trbc_industry,
            trbc_activity,
            hq_country_code
        FROM ranked
        WHERE rn = 1
        """
    ).fetchall()
    return {
        normalize_ric(row[0]): {
            "trbc_economic_sector": normalize_optional_text(row[1]),
            "trbc_business_sector": normalize_optional_text(row[2]),
            "trbc_industry_group": normalize_optional_text(row[3]),
            "trbc_industry": normalize_optional_text(row[4]),
            "trbc_activity": normalize_optional_text(row[5]),
            "hq_country_code": normalize_optional_text(row[6]),
        }
        for row in rows
        if row and row[0]
    }


def _classification_flags(classification: dict[str, str | None]) -> tuple[int, int]:
    has_classification = 1 if any(
        normalize_optional_text(classification.get(field))
        for field in (
            "trbc_economic_sector",
            "trbc_business_sector",
            "trbc_industry_group",
            "trbc_industry",
            "trbc_activity",
            "hq_country_code",
        )
    ) else 0
    sector = normalize_optional_text(classification.get("trbc_economic_sector"))
    is_equity_eligible = 1 if has_classification == 1 and sector not in NON_EQUITY_ECONOMIC_SECTORS else 0
    return has_classification, is_equity_eligible


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


def refresh_security_taxonomy_current(
    conn: sqlite3.Connection,
    *,
    rics: list[str] | None = None,
) -> int:
    ric_filter = ""
    params: list[object] = []
    clean_rics = [normalize_ric(ric) for ric in (rics or []) if normalize_ric(ric)]
    if clean_rics:
        placeholders = ",".join("?" for _ in clean_rics)
        ric_filter = f" AND UPPER(TRIM(reg.ric)) IN ({placeholders})"
        params.extend(clean_rics)
    classification_by_ric = _latest_classification_by_ric(conn)
    rows = conn.execute(
        f"""
        WITH latest_obs AS (
            SELECT
                ric,
                source,
                job_run_id,
                updated_at
            FROM (
                SELECT
                    UPPER(TRIM(ric)) AS ric,
                    source,
                    job_run_id,
                    updated_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(ric))
                        ORDER BY as_of_date DESC, updated_at DESC
                    ) AS rn
                FROM {SECURITY_SOURCE_OBSERVATION_DAILY_TABLE}
                WHERE ric IS NOT NULL AND TRIM(ric) <> ''
            )
            WHERE rn = 1
        )
        SELECT
            UPPER(TRIM(reg.ric)) AS ric,
            UPPER(TRIM(reg.ticker)) AS ticker,
            COALESCE(pol.allow_cuse_returns_projection, 0) AS allow_cuse_returns_projection,
            pol.pit_fundamentals_enabled AS pit_fundamentals_enabled,
            pol.pit_classification_enabled AS pit_classification_enabled,
            pol.policy_source AS policy_source,
            COALESCE(reg.source, pol.policy_source, obs.source, '') AS source,
            COALESCE(reg.job_run_id, pol.job_run_id, obs.job_run_id) AS job_run_id,
            COALESCE(reg.updated_at, pol.updated_at, obs.updated_at) AS updated_at
        FROM {SECURITY_REGISTRY_TABLE} reg
        LEFT JOIN {SECURITY_POLICY_CURRENT_TABLE} pol
          ON UPPER(TRIM(pol.ric)) = UPPER(TRIM(reg.ric))
        LEFT JOIN latest_obs obs
          ON obs.ric = UPPER(TRIM(reg.ric))
        WHERE reg.ric IS NOT NULL AND TRIM(reg.ric) <> ''
          AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
        {ric_filter}
        """
    , params).fetchall()
    payload: list[tuple[object, ...]] = []
    for row in rows:
        ric = normalize_ric(row[0])
        if not ric:
            continue
        classification = classification_by_ric.get(ric, {})
        classification_ready, is_equity_eligible = _classification_flags(classification)
        allow_cuse_returns_projection = int(row[2] or 0)
        pit_fundamentals_enabled = (
            int(row[3])
            if row[3] is not None
            else (1 if is_equity_eligible == 1 else 0)
        )
        pit_classification_enabled = (
            int(row[4])
            if row[4] is not None
            else (1 if is_equity_eligible == 1 else 0)
        )
        hq_country_code = str(classification.get("hq_country_code") or "").strip().upper() or None
        classification_sector = normalize_optional_text(classification.get("trbc_economic_sector"))
        projection_only_policy = (
            allow_cuse_returns_projection == 1
            and pit_fundamentals_enabled == 0
            and pit_classification_enabled == 0
        )
        policy_implies_single_name_equity = (
            pit_fundamentals_enabled == 1
            and pit_classification_enabled == 1
        )
        if (
            projection_only_policy
        ):
            instrument_kind = "fund_vehicle"
            vehicle_structure = "projection_only_vehicle"
            is_single_name_equity = 0
        elif classification_sector in NON_EQUITY_ECONOMIC_SECTORS:
            instrument_kind = "fund_vehicle"
            vehicle_structure = "classified_non_equity"
            is_single_name_equity = 0
        elif is_equity_eligible == 1 or (classification_ready == 0 and policy_implies_single_name_equity):
            instrument_kind = "single_name_equity"
            vehicle_structure = "equity_security"
            is_single_name_equity = 1
        else:
            instrument_kind = "other"
            vehicle_structure = "other"
            is_single_name_equity = 0
        model_home_market_scope = "us" if hq_country_code == "US" else ("ex_us" if hq_country_code else "unknown")
        payload.append(
            (
                ric,
                instrument_kind,
                vehicle_structure,
                hq_country_code,
                None,
                model_home_market_scope,
                is_single_name_equity,
                classification_ready,
                normalize_optional_text(row[6]),
                normalize_optional_text(row[7]),
                normalize_optional_text(row[8]),
            )
        )
    if not payload:
        return 0
    conn.executemany(
        f"""
        INSERT INTO {SECURITY_TAXONOMY_CURRENT_TABLE} (
            ric,
            instrument_kind,
            vehicle_structure,
            issuer_country_code,
            listing_country_code,
            model_home_market_scope,
            is_single_name_equity,
            classification_ready,
            source,
            job_run_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ric) DO UPDATE SET
            instrument_kind = excluded.instrument_kind,
            vehicle_structure = excluded.vehicle_structure,
            issuer_country_code = excluded.issuer_country_code,
            listing_country_code = excluded.listing_country_code,
            model_home_market_scope = excluded.model_home_market_scope,
            is_single_name_equity = excluded.is_single_name_equity,
            classification_ready = excluded.classification_ready,
            source = COALESCE(NULLIF(excluded.source, ''), {SECURITY_TAXONOMY_CURRENT_TABLE}.source),
            job_run_id = COALESCE(NULLIF(excluded.job_run_id, ''), {SECURITY_TAXONOMY_CURRENT_TABLE}.job_run_id),
            updated_at = COALESCE(NULLIF(excluded.updated_at, ''), {SECURITY_TAXONOMY_CURRENT_TABLE}.updated_at)
        """,
        payload,
    )
    return len(payload)


def materialize_security_master_compat_current(
    conn: sqlite3.Connection,
    *,
    rics: list[str] | None = None,
) -> int:
    ric_filter = ""
    params: list[object] = []
    clean_rics = [normalize_ric(ric) for ric in (rics or []) if normalize_ric(ric)]
    if clean_rics:
        placeholders = ",".join("?" for _ in clean_rics)
        ric_filter = f" AND UPPER(TRIM(reg.ric)) IN ({placeholders})"
        params.extend(clean_rics)
    if _table_has_rows(conn, SECURITY_REGISTRY_TABLE):
        rows = conn.execute(
            f"""
            WITH latest_obs AS (
                SELECT
                    ric,
                    classification_ready,
                    is_equity_eligible,
                    source,
                    job_run_id,
                    updated_at
                FROM (
                    SELECT
                        UPPER(TRIM(ric)) AS ric,
                        classification_ready,
                        is_equity_eligible,
                        source,
                        job_run_id,
                        updated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(TRIM(ric))
                            ORDER BY as_of_date DESC, updated_at DESC
                        ) AS rn
                    FROM {SECURITY_SOURCE_OBSERVATION_DAILY_TABLE}
                    WHERE ric IS NOT NULL AND TRIM(ric) <> ''
                )
                WHERE rn = 1
            )
            SELECT
                UPPER(TRIM(reg.ric)) AS ric,
                UPPER(TRIM(reg.ticker)) AS ticker,
                reg.isin,
                reg.exchange_name,
                COALESCE(tax.classification_ready, obs.classification_ready, 0) AS classification_ok,
                CASE
                    WHEN COALESCE(tax.is_single_name_equity, obs.is_equity_eligible, 0) = 1 THEN 1
                    ELSE 0
                END AS is_equity_eligible,
                CASE
                    WHEN COALESCE(pol.allow_cuse_returns_projection, 0) = 1
                     AND COALESCE(pol.pit_fundamentals_enabled, 0) = 0
                     AND COALESCE(pol.pit_classification_enabled, 0) = 0
                    THEN 'projection_only'
                    ELSE 'native_equity'
                END AS coverage_role,
                COALESCE(reg.source, tax.source, obs.source, '') AS source,
                COALESCE(reg.job_run_id, tax.job_run_id, obs.job_run_id) AS job_run_id,
                COALESCE(reg.updated_at, tax.updated_at, obs.updated_at) AS updated_at
            FROM {SECURITY_REGISTRY_TABLE} reg
            LEFT JOIN {SECURITY_POLICY_CURRENT_TABLE} pol
              ON UPPER(TRIM(pol.ric)) = UPPER(TRIM(reg.ric))
            LEFT JOIN {SECURITY_TAXONOMY_CURRENT_TABLE} tax
              ON UPPER(TRIM(tax.ric)) = UPPER(TRIM(reg.ric))
            LEFT JOIN latest_obs obs
              ON obs.ric = UPPER(TRIM(reg.ric))
            WHERE reg.ric IS NOT NULL AND TRIM(reg.ric) <> ''
              AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
              {ric_filter}
            """,
            params,
        ).fetchall()
    else:
        if clean_rics:
            placeholders = ",".join("?" for _ in clean_rics)
            ric_filter = f" AND UPPER(TRIM(ric)) IN ({placeholders})"
        rows = conn.execute(
            f"""
            SELECT
                UPPER(TRIM(ric)) AS ric,
                UPPER(TRIM(ticker)) AS ticker,
                isin,
                exchange_name,
                COALESCE(classification_ok, 0) AS classification_ok,
                COALESCE(is_equity_eligible, 0) AS is_equity_eligible,
                COALESCE(coverage_role, 'native_equity') AS coverage_role,
                source,
                job_run_id,
                updated_at
            FROM {SECURITY_MASTER_TABLE}
            WHERE ric IS NOT NULL AND TRIM(ric) <> ''
            {ric_filter}
            """,
            params,
        ).fetchall()
    payload = [
        (
            normalize_ric(row[0]),
            normalize_optional_text(row[1]),
            normalize_optional_text(row[2]),
            normalize_optional_text(row[3]),
            int(row[4] or 0),
            int(row[5] or 0),
            normalize_optional_text(row[6]) or "native_equity",
            normalize_optional_text(row[7]),
            normalize_optional_text(row[8]),
            normalize_optional_text(row[9]),
        )
        for row in rows
        if row and row[0]
    ]
    if not payload:
        return 0
    if clean_rics:
        placeholders = ",".join("?" for _ in clean_rics)
        conn.execute(
            f"DELETE FROM {SECURITY_MASTER_COMPAT_CURRENT_TABLE} WHERE ric IN ({placeholders})",
            clean_rics,
        )
    else:
        conn.execute(f"DELETE FROM {SECURITY_MASTER_COMPAT_CURRENT_TABLE}")
    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {SECURITY_MASTER_COMPAT_CURRENT_TABLE} (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)
