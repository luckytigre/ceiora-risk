"""Merged runtime-universe rows with additive-table authority and legacy fallback."""

from __future__ import annotations

from datetime import date
import sqlite3
from typing import Any

from backend.universe.classification_policy import NON_EQUITY_ECONOMIC_SECTORS
from backend.universe.normalize import (
    normalize_optional_text,
    normalize_ric,
    normalize_ticker,
    ticker_from_ric,
)
from backend.universe.registry_sync import (
    derive_policy_flags_from_structure,
    policy_defaults_for_legacy_coverage_role,
    policy_source_is_explicit_override,
)
from backend.universe import runtime_authority
from backend.universe.schema import (
    SECURITY_MASTER_COMPAT_CURRENT_TABLE,
    SECURITY_MASTER_TABLE,
)


_POLICY_FLAG_FIELDS = (
    "price_ingest_enabled",
    "pit_fundamentals_enabled",
    "pit_classification_enabled",
    "allow_cuse_native_core",
    "allow_cuse_fundamental_projection",
    "allow_cuse_returns_projection",
    "allow_cpar_core_target",
    "allow_cpar_extended_target",
)

def _runtime_instrument_fallback(
    *,
    legacy_coverage_role: str,
    legacy_is_equity_eligible: int,
) -> tuple[str, str, int]:
    if legacy_coverage_role == "projection_only":
        return "fund_vehicle", "projection_only_vehicle", 0
    if int(legacy_is_equity_eligible or 0) == 1:
        return "single_name_equity", "equity_security", 1
    return "other", "other", 0


def _iso_date_key(value: Any) -> str | None:
    text = normalize_optional_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(str(text)[:10]).isoformat()
    except ValueError:
        return None


def _updated_not_future(updated_at: Any, as_of_date: str | None) -> bool:
    as_of_key = _iso_date_key(as_of_date)
    updated_key = _iso_date_key(updated_at)
    return bool(as_of_key and updated_key and updated_key <= as_of_key)


def _load_compat_rows_from_table(conn: sqlite3.Connection, table: str) -> dict[str, dict[str, Any]]:
    if not runtime_authority.table_exists(conn, table):
        return {}
    cols = runtime_authority.table_columns(conn, table)
    ticker_expr = "UPPER(TRIM(COALESCE(ticker, '')))" if "ticker" in cols else "''"
    isin_expr = "isin" if "isin" in cols else "NULL"
    exchange_expr = "exchange_name" if "exchange_name" in cols else "NULL"
    classification_expr = "COALESCE(classification_ok, 0)" if "classification_ok" in cols else "0"
    equity_expr = "COALESCE(is_equity_eligible, 0)" if "is_equity_eligible" in cols else "0"
    coverage_expr = "COALESCE(coverage_role, 'native_equity')" if "coverage_role" in cols else "'native_equity'"
    source_expr = "source" if "source" in cols else "NULL"
    job_run_expr = "job_run_id" if "job_run_id" in cols else "NULL"
    updated_expr = "updated_at" if "updated_at" in cols else "NULL"
    rows = conn.execute(
        f"""
        SELECT
            UPPER(TRIM(ric)) AS ric,
            {ticker_expr} AS ticker,
            {isin_expr} AS isin,
            {exchange_expr} AS exchange_name,
            {classification_expr} AS classification_ok,
            {equity_expr} AS is_equity_eligible,
            {coverage_expr} AS coverage_role,
            {source_expr} AS source,
            {job_run_expr} AS job_run_id,
            {updated_expr} AS updated_at
        FROM {table}
        WHERE ric IS NOT NULL AND TRIM(ric) <> ''
        """
    ).fetchall()
    return {
        normalize_ric(row[0]): {
            "ric": normalize_ric(row[0]),
            "ticker": normalize_ticker(row[1]),
            "isin": normalize_optional_text(row[2]),
            "exchange_name": normalize_optional_text(row[3]),
            "classification_ok": int(row[4] or 0),
            "is_equity_eligible": int(row[5] or 0),
            "coverage_role": normalize_optional_text(row[6]) or "native_equity",
            "source": normalize_optional_text(row[7]),
            "job_run_id": normalize_optional_text(row[8]),
            "updated_at": normalize_optional_text(row[9]),
        }
        for row in rows
        if row and row[0]
    }


def _load_compat_rows(
    conn: sqlite3.Connection,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    compat_current_rows: dict[str, dict[str, Any]] = {}
    legacy_master_rows: dict[str, dict[str, Any]] = {}
    if runtime_authority.table_exists(conn, SECURITY_MASTER_COMPAT_CURRENT_TABLE):
        compat_current_rows = _load_compat_rows_from_table(
            conn,
            SECURITY_MASTER_COMPAT_CURRENT_TABLE,
        )
    if runtime_authority.table_exists(conn, SECURITY_MASTER_TABLE):
        legacy_master_rows = _load_compat_rows_from_table(conn, SECURITY_MASTER_TABLE)
    return compat_current_rows, legacy_master_rows


def _load_historical_classification_rows(
    conn: sqlite3.Connection,
    *,
    as_of_date: str | None,
) -> dict[str, dict[str, Any]]:
    as_of_key = _iso_date_key(as_of_date)
    if not as_of_key or not runtime_authority.table_exists(conn, "security_classification_pit"):
        return {}
    cols = runtime_authority.table_columns(conn, "security_classification_pit")
    sector_expr = "NULL"
    if "trbc_economic_sector" in cols:
        sector_expr = "NULLIF(TRIM(trbc_economic_sector), '')"
    hq_expr = "NULL"
    if "hq_country_code" in cols:
        hq_expr = "NULLIF(TRIM(hq_country_code), '')"
    updated_expr = "''"
    if "updated_at" in cols:
        updated_expr = "COALESCE(updated_at, '')"
    rows = conn.execute(
        f"""
        WITH ranked AS (
            SELECT
                UPPER(TRIM(ric)) AS ric,
                {sector_expr} AS trbc_economic_sector,
                {hq_expr} AS hq_country_code,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(TRIM(ric))
                    ORDER BY TRIM(as_of_date) DESC, {updated_expr} DESC
                ) AS rn
            FROM security_classification_pit
            WHERE ric IS NOT NULL
              AND TRIM(ric) <> ''
              AND as_of_date IS NOT NULL
              AND TRIM(as_of_date) <> ''
              AND TRIM(as_of_date) <= ?
        )
        SELECT ric, trbc_economic_sector, hq_country_code
        FROM ranked
        WHERE rn = 1
        """,
        (as_of_key,),
    ).fetchall()
    return {
        normalize_ric(row[0]): {
            "trbc_economic_sector": normalize_optional_text(row[1]),
            "hq_country_code": normalize_optional_text(row[2]),
        }
        for row in rows
        if row and row[0]
    }


def _derive_structural_row(
    *,
    legacy_coverage_role: str,
    legacy_classification_ok: int,
    legacy_is_equity_eligible: int,
    observation: dict[str, Any],
    classification: dict[str, Any],
) -> dict[str, Any]:
    sector = normalize_optional_text(classification.get("trbc_economic_sector"))
    issuer_country_code = normalize_optional_text(classification.get("hq_country_code"))
    snapshot_classification_ready = 1 if (sector or issuer_country_code) else 0
    snapshot_is_equity_eligible = (
        1 if snapshot_classification_ready == 1 and sector not in NON_EQUITY_ECONOMIC_SECTORS else 0
    )
    if snapshot_classification_ready == 1:
        classification_ready = snapshot_classification_ready
        is_equity_eligible = snapshot_is_equity_eligible
    else:
        classification_ready = int(
            observation.get("classification_ready", legacy_classification_ok) or 0
        )
        is_equity_eligible = int(
            observation.get("is_equity_eligible", legacy_is_equity_eligible) or 0
        )
    if legacy_coverage_role == "projection_only":
        instrument_kind = "fund_vehicle"
        vehicle_structure = "projection_only_vehicle"
        is_single_name_equity = 0
    elif sector in NON_EQUITY_ECONOMIC_SECTORS:
        instrument_kind = "fund_vehicle"
        vehicle_structure = "classified_non_equity"
        is_single_name_equity = 0
    elif classification_ready == 1 and is_equity_eligible == 1:
        instrument_kind = "single_name_equity"
        vehicle_structure = "equity_security"
        is_single_name_equity = 1
    else:
        instrument_kind = "other"
        vehicle_structure = "other"
        is_single_name_equity = 0
    model_home_market_scope = (
        "us"
        if issuer_country_code == "US"
        else ("ex_us" if issuer_country_code else "unknown")
    )
    return {
        "instrument_kind": instrument_kind,
        "vehicle_structure": vehicle_structure,
        "issuer_country_code": issuer_country_code,
        "listing_country_code": None,
        "model_home_market_scope": model_home_market_scope,
        "is_single_name_equity": is_single_name_equity,
        "classification_ready": classification_ready,
    }


def _derive_policy_flags(
    *,
    legacy_coverage_role: str,
    structural_row: dict[str, Any],
) -> dict[str, int]:
    return derive_policy_flags_from_structure(
        legacy_coverage_role=legacy_coverage_role,
        instrument_kind=structural_row.get("instrument_kind"),
        model_home_market_scope=structural_row.get("model_home_market_scope"),
        is_single_name_equity=structural_row.get("is_single_name_equity"),
    )


def _resolve_current_structural_row(
    *,
    taxonomy: dict[str, Any],
    legacy_coverage_role: str,
    legacy_classification_ok: int,
    legacy_is_equity_eligible: int,
    observation: dict[str, Any],
) -> dict[str, Any]:
    if taxonomy:
        return taxonomy
    return _derive_structural_row(
        legacy_coverage_role=legacy_coverage_role,
        legacy_classification_ok=legacy_classification_ok,
        legacy_is_equity_eligible=legacy_is_equity_eligible,
        observation=observation,
        classification={},
    )


def _resolve_effective_policy_row(
    *,
    policy_row: dict[str, Any],
    observation: dict[str, Any],
    legacy_coverage_role: str,
    structural_row: dict[str, Any],
    as_of_key: str | None,
) -> dict[str, int]:
    defaults = _derive_policy_flags(
        legacy_coverage_role=legacy_coverage_role,
        structural_row=structural_row,
    )
    resolved = {field: int(defaults.get(field, 0) or 0) for field in _POLICY_FLAG_FIELDS}
    if not policy_row:
        return resolved
    if as_of_key and not _updated_not_future(policy_row.get("updated_at"), as_of_key):
        return resolved
    for field in _POLICY_FLAG_FIELDS:
        if policy_row.get(field) is not None:
            resolved[field] = int(policy_row.get(field) or 0)
    return resolved


def _requested_registry_rics(
    *,
    registry_rows: dict[str, dict[str, Any]],
    requested_rics: set[str],
    requested_tickers: set[str],
) -> set[str]:
    if requested_rics:
        return {ric for ric in requested_rics if ric in registry_rows}
    if requested_tickers:
        return {
            ric
            for ric, row in registry_rows.items()
            if normalize_ticker(row.get("ticker")) in requested_tickers
        }
    return {
        ric
        for ric, row in registry_rows.items()
        if (normalize_optional_text(row.get("tracking_status")) or "active") == "active"
    }


def _candidate_runtime_rics(
    *,
    registry_table_exists: bool,
    registry_rows: dict[str, dict[str, Any]],
    compat_current_rows: dict[str, dict[str, Any]],
    legacy_master_rows: dict[str, dict[str, Any]],
    policy_rows: dict[str, dict[str, Any]],
    taxonomy_rows: dict[str, dict[str, Any]],
    observation_rows: dict[str, dict[str, Any]],
    requested_rics: set[str],
    requested_tickers: set[str],
    allow_empty_registry_fallback: bool,
) -> set[str]:
    requested = bool(requested_rics or requested_tickers)
    requested_compat_current_rics = _requested_registry_rics(
        registry_rows=compat_current_rows,
        requested_rics=requested_rics,
        requested_tickers=requested_tickers,
    )
    requested_legacy_master_rics = _requested_registry_rics(
        registry_rows=legacy_master_rows,
        requested_rics=requested_rics,
        requested_tickers=requested_tickers,
    )
    requested_compat_fallback_rics = set(requested_compat_current_rics)
    requested_compat_fallback_rics.update(
        requested_legacy_master_rics - set(requested_compat_current_rics)
    )
    if registry_table_exists:
        if not registry_rows:
            if allow_empty_registry_fallback:
                fallback_rics = (
                    set(compat_current_rows)
                    or set(legacy_master_rows)
                    or set(policy_rows)
                    or set(taxonomy_rows)
                    or set(observation_rows)
                )
                if requested:
                    return fallback_rics | requested_compat_fallback_rics
                return fallback_rics
            return requested_compat_fallback_rics if requested else set()
        scoped_registry_rics = _requested_registry_rics(
            registry_rows=registry_rows,
            requested_rics=requested_rics,
            requested_tickers=requested_tickers,
        )
        if requested:
            return scoped_registry_rics | (requested_compat_fallback_rics - scoped_registry_rics)
        return scoped_registry_rics
    fallback_rics = set(policy_rows) | set(taxonomy_rows) | set(observation_rows)
    if compat_current_rows:
        fallback_rics |= set(compat_current_rows)
    else:
        fallback_rics |= set(legacy_master_rows)
    if requested:
        fallback_rics |= requested_compat_fallback_rics
    return fallback_rics


def load_security_runtime_rows(
    conn: sqlite3.Connection,
    *,
    rics: list[str] | None = None,
    tickers: list[str] | None = None,
    as_of_date: str | None = None,
    include_disabled: bool = False,
    allow_empty_registry_fallback: bool = False,
) -> list[dict[str, Any]]:
    as_of_key = _iso_date_key(as_of_date)
    authority_state = runtime_authority.load_runtime_authority_state(
        conn,
        as_of_date=as_of_date,
    )
    compat_current_rows, legacy_master_rows = _load_compat_rows(conn)
    compat_rows = dict(compat_current_rows)
    for ric, row in legacy_master_rows.items():
        compat_rows.setdefault(ric, row)
    policy_rows = authority_state.policy_rows
    taxonomy_rows = authority_state.taxonomy_rows
    observation_rows = authority_state.observation_rows
    historical_classification_rows = _load_historical_classification_rows(conn, as_of_date=as_of_date)
    registry_rows = authority_state.registry_rows
    registry_table_exists = authority_state.registry_table_exists

    requested_rics = {normalize_ric(value) for value in (rics or []) if normalize_ric(value)}
    requested_tickers = {normalize_ticker(value) for value in (tickers or []) if normalize_ticker(value)}

    candidate_rics = _candidate_runtime_rics(
        registry_table_exists=registry_table_exists,
        registry_rows=registry_rows,
        compat_current_rows=compat_current_rows,
        legacy_master_rows=legacy_master_rows,
        policy_rows=policy_rows,
        taxonomy_rows=taxonomy_rows,
        observation_rows=observation_rows,
        requested_rics=requested_rics,
        requested_tickers=requested_tickers,
        allow_empty_registry_fallback=allow_empty_registry_fallback,
    )
    if requested_rics:
        candidate_rics &= requested_rics
    rows: list[dict[str, Any]] = []
    for ric in sorted(candidate_rics):
        registry = registry_rows.get(ric, {})
        compat = compat_rows.get(ric, {})
        policy = policy_rows.get(ric, {})
        taxonomy = taxonomy_rows.get(ric, {})
        observation = observation_rows.get(ric, {})

        ticker = (
            normalize_ticker(registry.get("ticker"))
            or normalize_ticker(compat.get("ticker"))
            or ticker_from_ric(ric)
        )
        if requested_tickers and ticker not in requested_tickers:
            continue
        tracking_status = normalize_optional_text(registry.get("tracking_status")) or "active"
        if not include_disabled and tracking_status == "disabled":
            continue

        legacy_coverage_role = normalize_optional_text(compat.get("coverage_role")) or "native_equity"
        defaults = policy_defaults_for_legacy_coverage_role(legacy_coverage_role)
        legacy_classification_ok = int(compat.get("classification_ok") or 0)
        legacy_is_equity_eligible = int(compat.get("is_equity_eligible") or 0)
        fallback_instrument_kind, fallback_vehicle_structure, fallback_is_single_name_equity = _runtime_instrument_fallback(
            legacy_coverage_role=legacy_coverage_role,
            legacy_is_equity_eligible=legacy_is_equity_eligible,
        )
        historical_structural = (
            _derive_structural_row(
                legacy_coverage_role=legacy_coverage_role,
                legacy_classification_ok=legacy_classification_ok,
                legacy_is_equity_eligible=legacy_is_equity_eligible,
                observation=observation,
                classification=historical_classification_rows.get(ric, {}),
            )
            if as_of_key
            else {}
        )
        structural = historical_structural or _resolve_current_structural_row(
            taxonomy=taxonomy,
            legacy_coverage_role=legacy_coverage_role,
            legacy_classification_ok=legacy_classification_ok,
            legacy_is_equity_eligible=legacy_is_equity_eligible,
            observation=observation,
        )
        effective_policy = _resolve_effective_policy_row(
            policy_row=policy,
            observation=observation,
            legacy_coverage_role=legacy_coverage_role,
            structural_row=historical_structural or structural,
            as_of_key=as_of_key,
        )
        classification_ready_value = (
            int(structural.get("classification_ready", legacy_classification_ok) or 0)
            if as_of_key and historical_structural
            else int(observation.get("classification_ready", structural.get("classification_ready", legacy_classification_ok)) or 0)
        )

        row = {
            "ric": ric,
            "ticker": ticker,
            "isin": normalize_optional_text(registry.get("isin")) or normalize_optional_text(compat.get("isin")),
            "exchange_name": normalize_optional_text(registry.get("exchange_name")) or normalize_optional_text(compat.get("exchange_name")),
            "tracking_status": tracking_status,
            "source": (
                normalize_optional_text(registry.get("source"))
                or normalize_optional_text(taxonomy.get("source"))
                or normalize_optional_text(observation.get("source"))
                or normalize_optional_text(compat.get("source"))
            ),
            "job_run_id": (
                normalize_optional_text(registry.get("job_run_id"))
                or normalize_optional_text(policy.get("job_run_id"))
                or normalize_optional_text(taxonomy.get("job_run_id"))
                or normalize_optional_text(observation.get("job_run_id"))
                or normalize_optional_text(compat.get("job_run_id"))
            ),
            "updated_at": (
                normalize_optional_text(registry.get("updated_at"))
                or (normalize_optional_text(policy.get("updated_at")) if policy_source_is_explicit_override(policy.get("policy_source")) else None)
                or normalize_optional_text(taxonomy.get("updated_at"))
                or normalize_optional_text(observation.get("updated_at"))
                or normalize_optional_text(compat.get("updated_at"))
            ),
            "legacy_coverage_role": legacy_coverage_role,
            "legacy_classification_ok": legacy_classification_ok,
            "legacy_is_equity_eligible": legacy_is_equity_eligible,
            "price_ingest_enabled": int(effective_policy.get("price_ingest_enabled", defaults["price_ingest_enabled"]) or 0),
            "pit_fundamentals_enabled": int(effective_policy.get("pit_fundamentals_enabled", defaults["pit_fundamentals_enabled"]) or 0),
            "pit_classification_enabled": int(effective_policy.get("pit_classification_enabled", defaults["pit_classification_enabled"]) or 0),
            "allow_cuse_native_core": int(effective_policy.get("allow_cuse_native_core", defaults["allow_cuse_native_core"]) or 0),
            "allow_cuse_fundamental_projection": int(effective_policy.get("allow_cuse_fundamental_projection", defaults["allow_cuse_fundamental_projection"]) or 0),
            "allow_cuse_returns_projection": int(effective_policy.get("allow_cuse_returns_projection", defaults["allow_cuse_returns_projection"]) or 0),
            "allow_cpar_core_target": int(effective_policy.get("allow_cpar_core_target", defaults["allow_cpar_core_target"]) or 0),
            "allow_cpar_extended_target": int(effective_policy.get("allow_cpar_extended_target", defaults["allow_cpar_extended_target"]) or 0),
            "instrument_kind": normalize_optional_text(structural.get("instrument_kind")) or fallback_instrument_kind,
            "vehicle_structure": normalize_optional_text(structural.get("vehicle_structure")) or fallback_vehicle_structure,
            "issuer_country_code": normalize_optional_text(structural.get("issuer_country_code")),
            "listing_country_code": normalize_optional_text(structural.get("listing_country_code")),
            "model_home_market_scope": normalize_optional_text(structural.get("model_home_market_scope")) or "unknown",
            "is_single_name_equity": int(structural.get("is_single_name_equity", fallback_is_single_name_equity) or 0),
            "classification_ready": classification_ready_value,
            "observation_as_of_date": normalize_optional_text(observation.get("observation_as_of_date")),
            "has_price_history_as_of_date": int(observation.get("has_price_history_as_of_date") or 0),
            "has_fundamentals_history_as_of_date": int(observation.get("has_fundamentals_history_as_of_date") or 0),
            "has_classification_history_as_of_date": int(observation.get("has_classification_history_as_of_date") or 0),
            "latest_price_date": normalize_optional_text(observation.get("latest_price_date")),
            "latest_fundamentals_as_of_date": normalize_optional_text(observation.get("latest_fundamentals_as_of_date")),
            "latest_classification_as_of_date": normalize_optional_text(observation.get("latest_classification_as_of_date")),
        }
        rows.append(row)
    return rows


def load_security_runtime_map(
    conn: sqlite3.Connection,
    *,
    rics: list[str] | None = None,
    tickers: list[str] | None = None,
    as_of_date: str | None = None,
    include_disabled: bool = False,
    allow_empty_registry_fallback: bool = False,
) -> dict[str, dict[str, Any]]:
    return {
        str(row["ric"]): row
        for row in load_security_runtime_rows(
            conn,
            rics=rics,
            tickers=tickers,
            as_of_date=as_of_date,
            include_disabled=include_disabled,
            allow_empty_registry_fallback=allow_empty_registry_fallback,
        )
    }


def load_security_runtime_map_by_date(
    conn: sqlite3.Connection,
    *,
    rics: list[str] | None = None,
    tickers: list[str] | None = None,
    as_of_dates: list[str] | None = None,
    include_disabled: bool = False,
    allow_empty_registry_fallback: bool = False,
) -> dict[str, dict[str, dict[str, Any]]]:
    return {
        str(as_of_date): load_security_runtime_map(
            conn,
            rics=rics,
            tickers=tickers,
            as_of_date=str(as_of_date),
            include_disabled=include_disabled,
            allow_empty_registry_fallback=allow_empty_registry_fallback,
        )
        for as_of_date in sorted({str(value) for value in (as_of_dates or []) if normalize_optional_text(value)})
    }


def load_security_runtime_rows_by_dates(
    conn: sqlite3.Connection,
    *,
    rics: list[str] | None = None,
    tickers: list[str] | None = None,
    as_of_dates: list[str] | None = None,
    include_disabled: bool = False,
    allow_empty_registry_fallback: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    return {
        str(as_of_date): load_security_runtime_rows(
            conn,
            rics=rics,
            tickers=tickers,
            as_of_date=str(as_of_date),
            include_disabled=include_disabled,
            allow_empty_registry_fallback=allow_empty_registry_fallback,
        )
        for as_of_date in sorted({str(value) for value in (as_of_dates or []) if normalize_optional_text(value)})
    }
