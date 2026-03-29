"""Current-table authority loading for runtime rows."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any

from backend.universe.normalize import normalize_optional_text, normalize_ric, normalize_ticker
from backend.universe.schema import (
    SECURITY_POLICY_CURRENT_TABLE,
    SECURITY_REGISTRY_TABLE,
    SECURITY_SOURCE_OBSERVATION_DAILY_TABLE,
    SECURITY_TAXONOMY_CURRENT_TABLE,
)


@dataclass(frozen=True)
class RuntimeAuthorityState:
    registry_table_exists: bool
    registry_rows: dict[str, dict[str, Any]]
    policy_rows: dict[str, dict[str, Any]]
    taxonomy_rows: dict[str, dict[str, Any]]
    observation_rows: dict[str, dict[str, Any]]


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type IN ('table', 'view') AND name=?
        LIMIT 1
        """,
        (table,),
    ).fetchone()
    return row is not None


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def _load_registry_rows(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not table_exists(conn, SECURITY_REGISTRY_TABLE):
        return {}
    rows = conn.execute(
        f"""
        SELECT
            UPPER(TRIM(ric)) AS ric,
            UPPER(TRIM(COALESCE(ticker, ''))) AS ticker,
            isin,
            exchange_name,
            tracking_status,
            source,
            job_run_id,
            updated_at
        FROM {SECURITY_REGISTRY_TABLE}
        WHERE ric IS NOT NULL AND TRIM(ric) <> ''
        """
    ).fetchall()
    return {
        normalize_ric(row[0]): {
            "ric": normalize_ric(row[0]),
            "ticker": normalize_ticker(row[1]),
            "isin": normalize_optional_text(row[2]),
            "exchange_name": normalize_optional_text(row[3]),
            "tracking_status": normalize_optional_text(row[4]) or "active",
            "source": normalize_optional_text(row[5]),
            "job_run_id": normalize_optional_text(row[6]),
            "updated_at": normalize_optional_text(row[7]),
        }
        for row in rows
        if row and row[0]
    }


def _load_policy_rows(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not table_exists(conn, SECURITY_POLICY_CURRENT_TABLE):
        return {}
    rows = conn.execute(
        f"""
        SELECT
            UPPER(TRIM(ric)) AS ric,
            price_ingest_enabled,
            pit_fundamentals_enabled,
            pit_classification_enabled,
            allow_cuse_native_core,
            allow_cuse_fundamental_projection,
            allow_cuse_returns_projection,
            allow_cpar_core_target,
            allow_cpar_extended_target,
            policy_source,
            job_run_id,
            updated_at
        FROM {SECURITY_POLICY_CURRENT_TABLE}
        WHERE ric IS NOT NULL AND TRIM(ric) <> ''
        """
    ).fetchall()
    return {
        normalize_ric(row[0]): {
            "price_ingest_enabled": int(row[1] or 0),
            "pit_fundamentals_enabled": int(row[2] or 0),
            "pit_classification_enabled": int(row[3] or 0),
            "allow_cuse_native_core": int(row[4] or 0),
            "allow_cuse_fundamental_projection": int(row[5] or 0),
            "allow_cuse_returns_projection": int(row[6] or 0),
            "allow_cpar_core_target": int(row[7] or 0),
            "allow_cpar_extended_target": int(row[8] or 0),
            "policy_source": normalize_optional_text(row[9]),
            "job_run_id": normalize_optional_text(row[10]),
            "updated_at": normalize_optional_text(row[11]),
        }
        for row in rows
        if row and row[0]
    }


def _load_taxonomy_rows(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not table_exists(conn, SECURITY_TAXONOMY_CURRENT_TABLE):
        return {}
    rows = conn.execute(
        f"""
        SELECT
            UPPER(TRIM(ric)) AS ric,
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
        FROM {SECURITY_TAXONOMY_CURRENT_TABLE}
        WHERE ric IS NOT NULL AND TRIM(ric) <> ''
        """
    ).fetchall()
    return {
        normalize_ric(row[0]): {
            "instrument_kind": normalize_optional_text(row[1]),
            "vehicle_structure": normalize_optional_text(row[2]),
            "issuer_country_code": normalize_optional_text(row[3]),
            "listing_country_code": normalize_optional_text(row[4]),
            "model_home_market_scope": normalize_optional_text(row[5]),
            "is_single_name_equity": int(row[6] or 0),
            "classification_ready": int(row[7] or 0),
            "source": normalize_optional_text(row[8]),
            "job_run_id": normalize_optional_text(row[9]),
            "updated_at": normalize_optional_text(row[10]),
        }
        for row in rows
        if row and row[0]
    }


def _load_latest_source_observations(
    conn: sqlite3.Connection,
    *,
    as_of_date: str | None,
) -> dict[str, dict[str, Any]]:
    if not table_exists(conn, SECURITY_SOURCE_OBSERVATION_DAILY_TABLE):
        return {}
    params: list[Any] = []
    where = ""
    if normalize_optional_text(as_of_date):
        where = "WHERE as_of_date <= ?"
        params.append(str(as_of_date))
    rows = conn.execute(
        f"""
        WITH ranked AS (
            SELECT
                as_of_date,
                UPPER(TRIM(ric)) AS ric,
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
                updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(TRIM(ric))
                    ORDER BY as_of_date DESC, updated_at DESC
                ) AS rn
            FROM {SECURITY_SOURCE_OBSERVATION_DAILY_TABLE}
            {where}
        )
        SELECT
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
        FROM ranked
        WHERE rn = 1
        """,
        params,
    ).fetchall()
    return {
        normalize_ric(row[1]): {
            "observation_as_of_date": normalize_optional_text(row[0]),
            "classification_ready": int(row[2] or 0),
            "is_equity_eligible": int(row[3] or 0),
            "price_ingest_enabled": int(row[4] or 0),
            "pit_fundamentals_enabled": int(row[5] or 0),
            "pit_classification_enabled": int(row[6] or 0),
            "has_price_history_as_of_date": int(row[7] or 0),
            "has_fundamentals_history_as_of_date": int(row[8] or 0),
            "has_classification_history_as_of_date": int(row[9] or 0),
            "latest_price_date": normalize_optional_text(row[10]),
            "latest_fundamentals_as_of_date": normalize_optional_text(row[11]),
            "latest_classification_as_of_date": normalize_optional_text(row[12]),
            "source": normalize_optional_text(row[13]),
            "job_run_id": normalize_optional_text(row[14]),
            "updated_at": normalize_optional_text(row[15]),
        }
        for row in rows
        if row and row[1]
    }


def load_runtime_authority_state(
    conn: sqlite3.Connection,
    *,
    as_of_date: str | None,
) -> RuntimeAuthorityState:
    return RuntimeAuthorityState(
        registry_table_exists=table_exists(conn, SECURITY_REGISTRY_TABLE),
        registry_rows=_load_registry_rows(conn),
        policy_rows=_load_policy_rows(conn),
        taxonomy_rows=_load_taxonomy_rows(conn),
        observation_rows=_load_latest_source_observations(conn, as_of_date=as_of_date),
    )
