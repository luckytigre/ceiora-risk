"""Registry and policy sync helpers for the evolving universe model."""

from __future__ import annotations

import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.universe.normalize import (
    normalize_optional_text,
    normalize_ric,
    normalize_ticker,
    ticker_from_ric,
)
from backend.universe.schema import (
    SECURITY_MASTER_COMPAT_CURRENT_TABLE,
    SECURITY_POLICY_CURRENT_TABLE,
    SECURITY_REGISTRY_TABLE,
    SECURITY_TAXONOMY_CURRENT_TABLE,
)


LEGACY_SECURITY_MASTER_SEED_PATH = (
    Path(__file__).resolve().parents[2] / "data/reference/security_master_seed.csv"
)
DEFAULT_SECURITY_REGISTRY_SEED_PATH = (
    Path(__file__).resolve().parents[2] / "data/reference/security_registry_seed.csv"
)

DEFAULT_POLICY_SOURCES = {
    "legacy_seed_defaults",
    "registry_seed_defaults",
    "security_registry_seed_defaults",
    "security_master_seed_defaults",
}
EXPLICIT_POLICY_SOURCE = "security_registry_seed_explicit_policy"
SEED_POLICY_COLUMNS = (
    "price_ingest_enabled",
    "pit_fundamentals_enabled",
    "pit_classification_enabled",
    "allow_cuse_native_core",
    "allow_cuse_fundamental_projection",
    "allow_cuse_returns_projection",
    "allow_cpar_core_target",
    "allow_cpar_extended_target",
)


def normalize_bool_int(value: Any, *, default: int = 0) -> int:
    if value is None:
        return int(default)
    text = str(value).strip().lower()
    if not text:
        return int(default)
    if text in {"1", "true", "t", "yes", "y"}:
        return 1
    if text in {"0", "false", "f", "no", "n"}:
        return 0
    try:
        return 1 if int(float(text)) else 0
    except Exception:
        return int(default)


def _resolved_seed_path(seed_path: Path | None = None) -> Path:
    candidate = Path(seed_path or DEFAULT_SECURITY_REGISTRY_SEED_PATH).expanduser().resolve()
    if candidate.exists():
        return candidate
    legacy = LEGACY_SECURITY_MASTER_SEED_PATH.expanduser().resolve()
    return legacy


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


def policy_defaults_for_legacy_coverage_role(coverage_role: str | None) -> dict[str, int]:
    role = str(coverage_role or "").strip().lower() or "native_equity"
    if role == "projection_only":
        return {
            "price_ingest_enabled": 1,
            "pit_fundamentals_enabled": 0,
            "pit_classification_enabled": 0,
            "allow_cuse_native_core": 0,
            "allow_cuse_fundamental_projection": 0,
            "allow_cuse_returns_projection": 1,
            "allow_cpar_core_target": 0,
            "allow_cpar_extended_target": 1,
        }
    return {
        "price_ingest_enabled": 1,
        "pit_fundamentals_enabled": 1,
        "pit_classification_enabled": 1,
        "allow_cuse_native_core": 1,
        "allow_cuse_fundamental_projection": 0,
        "allow_cuse_returns_projection": 0,
        "allow_cpar_core_target": 1,
        "allow_cpar_extended_target": 1,
    }


def normalize_policy_source(policy_source: str | None) -> str | None:
    clean = normalize_optional_text(policy_source)
    if not clean:
        return None
    return clean.strip().lower()


def policy_source_is_default(policy_source: str | None) -> bool:
    clean = normalize_policy_source(policy_source)
    return not clean or clean in DEFAULT_POLICY_SOURCES


def policy_source_is_explicit_override(policy_source: str | None) -> bool:
    clean = normalize_policy_source(policy_source)
    return bool(clean) and not policy_source_is_default(clean)


def seed_row_has_explicit_policy(raw: dict[str, Any]) -> bool:
    return any(normalize_optional_text(raw.get(column)) is not None for column in SEED_POLICY_COLUMNS)


def seed_policy_source_for_row(raw: dict[str, Any], *, fallback: str = "registry_seed_defaults") -> str:
    explicit_source = normalize_policy_source(raw.get("policy_source"))
    if explicit_source:
        return explicit_source
    if seed_row_has_explicit_policy(raw):
        return EXPLICIT_POLICY_SOURCE
    return fallback


def legacy_coverage_role_from_policy_flags(
    *,
    allow_cuse_returns_projection: Any,
    pit_fundamentals_enabled: Any,
    pit_classification_enabled: Any,
) -> str:
    if (
        normalize_bool_int(allow_cuse_returns_projection, default=0) == 1
        and normalize_bool_int(pit_fundamentals_enabled, default=0) == 0
        and normalize_bool_int(pit_classification_enabled, default=0) == 0
    ):
        return "projection_only"
    return "native_equity"


def derive_policy_flags_from_structure(
    *,
    legacy_coverage_role: str | None,
    instrument_kind: str | None,
    model_home_market_scope: str | None,
    is_single_name_equity: Any,
) -> dict[str, int]:
    defaults = policy_defaults_for_legacy_coverage_role(legacy_coverage_role)
    normalized_legacy_role = normalize_optional_text(legacy_coverage_role)
    if normalized_legacy_role == "projection_only":
        return defaults
    raw_kind = normalize_optional_text(instrument_kind)
    raw_scope = normalize_optional_text(model_home_market_scope)
    single_name_equity = normalize_bool_int(is_single_name_equity, default=0)
    if raw_kind is None and raw_scope is None and single_name_equity == 0:
        return defaults
    normalized_kind = raw_kind or "other"
    normalized_scope = raw_scope or "unknown"
    if normalized_kind == "fund_vehicle":
        return {
            **defaults,
            "pit_fundamentals_enabled": 0,
            "pit_classification_enabled": 0,
            "allow_cuse_native_core": 0,
            "allow_cuse_fundamental_projection": 0,
            "allow_cuse_returns_projection": 1,
            "allow_cpar_core_target": 0,
            "allow_cpar_extended_target": 1,
        }
    if single_name_equity == 1 and normalized_scope == "unknown":
        return defaults
    if single_name_equity == 1 and normalized_scope == "us":
        return {
            **defaults,
            "pit_fundamentals_enabled": 1,
            "pit_classification_enabled": 1,
            "allow_cuse_native_core": 1,
            "allow_cuse_fundamental_projection": 0,
            "allow_cuse_returns_projection": 0,
            "allow_cpar_core_target": 1,
            "allow_cpar_extended_target": 1,
        }
    if single_name_equity == 1 and normalized_scope == "ex_us":
        return {
            **defaults,
            "pit_fundamentals_enabled": 1,
            "pit_classification_enabled": 1,
            "allow_cuse_native_core": 0,
            "allow_cuse_fundamental_projection": 1,
            "allow_cuse_returns_projection": 0,
            "allow_cpar_core_target": 0,
            "allow_cpar_extended_target": 1,
        }
    return {
        **defaults,
        "pit_fundamentals_enabled": 0,
        "pit_classification_enabled": 0,
        "allow_cuse_native_core": 0,
        "allow_cuse_fundamental_projection": 0,
        "allow_cuse_returns_projection": 0,
        "allow_cpar_core_target": 0,
        "allow_cpar_extended_target": 0,
    }


def load_security_registry_seed_rows(seed_path: Path | None = None) -> list[dict[str, Any]]:
    path = _resolved_seed_path(seed_path)
    if not path.exists():
        return []

    rows_by_ric: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            ric = normalize_ric(raw.get("ric"))
            if not ric:
                continue
            explicit_coverage_role = normalize_optional_text(raw.get("coverage_role"))
            defaults = policy_defaults_for_legacy_coverage_role(explicit_coverage_role)
            price_ingest_enabled = normalize_bool_int(
                raw.get("price_ingest_enabled"),
                default=defaults["price_ingest_enabled"],
            )
            pit_fundamentals_enabled = normalize_bool_int(
                raw.get("pit_fundamentals_enabled"),
                default=defaults["pit_fundamentals_enabled"],
            )
            pit_classification_enabled = normalize_bool_int(
                raw.get("pit_classification_enabled"),
                default=defaults["pit_classification_enabled"],
            )
            allow_cuse_native_core = normalize_bool_int(
                raw.get("allow_cuse_native_core"),
                default=defaults["allow_cuse_native_core"],
            )
            allow_cuse_fundamental_projection = normalize_bool_int(
                raw.get("allow_cuse_fundamental_projection"),
                default=defaults["allow_cuse_fundamental_projection"],
            )
            allow_cuse_returns_projection = normalize_bool_int(
                raw.get("allow_cuse_returns_projection"),
                default=defaults["allow_cuse_returns_projection"],
            )
            allow_cpar_core_target = normalize_bool_int(
                raw.get("allow_cpar_core_target"),
                default=defaults["allow_cpar_core_target"],
            )
            allow_cpar_extended_target = normalize_bool_int(
                raw.get("allow_cpar_extended_target"),
                default=defaults["allow_cpar_extended_target"],
            )
            compatibility_coverage_role = explicit_coverage_role or legacy_coverage_role_from_policy_flags(
                allow_cuse_returns_projection=allow_cuse_returns_projection,
                pit_fundamentals_enabled=pit_fundamentals_enabled,
                pit_classification_enabled=pit_classification_enabled,
            )
            policy_source = seed_policy_source_for_row(raw)
            rows_by_ric[ric] = {
                "ric": ric,
                "ticker": normalize_ticker(raw.get("ticker")) or ticker_from_ric(ric),
                "isin": normalize_optional_text(raw.get("isin")),
                "exchange_name": normalize_optional_text(raw.get("exchange_name")),
                "tracking_status": normalize_optional_text(raw.get("tracking_status")) or "active",
                "legacy_coverage_role": compatibility_coverage_role,
                "policy_source": policy_source,
                "price_ingest_enabled": price_ingest_enabled,
                "pit_fundamentals_enabled": pit_fundamentals_enabled,
                "pit_classification_enabled": pit_classification_enabled,
                "allow_cuse_native_core": allow_cuse_native_core,
                "allow_cuse_fundamental_projection": allow_cuse_fundamental_projection,
                "allow_cuse_returns_projection": allow_cuse_returns_projection,
                "allow_cpar_core_target": allow_cpar_core_target,
                "allow_cpar_extended_target": allow_cpar_extended_target,
            }
    return [rows_by_ric[ric] for ric in sorted(rows_by_ric)]


def upsert_security_registry_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    source: str,
    job_run_id: str,
    updated_at: str,
) -> int:
    payload = [
        (
            normalize_ric(row.get("ric")),
            normalize_ticker(row.get("ticker")) or ticker_from_ric(row.get("ric")),
            normalize_optional_text(row.get("isin")),
            normalize_optional_text(row.get("exchange_name")),
            normalize_optional_text(row.get("tracking_status")) or "active",
            normalize_optional_text(row.get("source")) or source,
            normalize_optional_text(row.get("job_run_id")) or job_run_id,
            normalize_optional_text(row.get("updated_at")) or updated_at,
        )
        for row in rows
        if normalize_ric(row.get("ric"))
    ]
    if not payload:
        return 0
    conn.executemany(
        f"""
        INSERT INTO {SECURITY_REGISTRY_TABLE} (
            ric, ticker, isin, exchange_name, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ric) DO UPDATE SET
            ticker = COALESCE(NULLIF(excluded.ticker, ''), {SECURITY_REGISTRY_TABLE}.ticker),
            isin = COALESCE(NULLIF(excluded.isin, ''), {SECURITY_REGISTRY_TABLE}.isin),
            exchange_name = COALESCE(NULLIF(excluded.exchange_name, ''), {SECURITY_REGISTRY_TABLE}.exchange_name),
            tracking_status = COALESCE(NULLIF(excluded.tracking_status, ''), {SECURITY_REGISTRY_TABLE}.tracking_status),
            source = COALESCE(NULLIF(excluded.source, ''), {SECURITY_REGISTRY_TABLE}.source),
            job_run_id = COALESCE(NULLIF(excluded.job_run_id, ''), {SECURITY_REGISTRY_TABLE}.job_run_id),
            updated_at = COALESCE(NULLIF(excluded.updated_at, ''), {SECURITY_REGISTRY_TABLE}.updated_at)
        """,
        payload,
    )
    return len(payload)


def upsert_security_policy_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    policy_source: str,
    job_run_id: str,
    updated_at: str,
) -> int:
    payload = [
        (
            normalize_ric(row.get("ric")),
            normalize_bool_int(row.get("price_ingest_enabled"), default=1),
            normalize_bool_int(row.get("pit_fundamentals_enabled"), default=0),
            normalize_bool_int(row.get("pit_classification_enabled"), default=0),
            normalize_bool_int(row.get("allow_cuse_native_core"), default=0),
            normalize_bool_int(row.get("allow_cuse_fundamental_projection"), default=0),
            normalize_bool_int(row.get("allow_cuse_returns_projection"), default=0),
            normalize_bool_int(row.get("allow_cpar_core_target"), default=0),
            normalize_bool_int(row.get("allow_cpar_extended_target"), default=0),
            normalize_policy_source(row.get("policy_source")) or policy_source,
            normalize_optional_text(row.get("job_run_id")) or job_run_id,
            normalize_optional_text(row.get("updated_at")) or updated_at,
        )
        for row in rows
        if normalize_ric(row.get("ric"))
    ]
    if not payload:
        return 0
    conn.executemany(
        f"""
        INSERT INTO {SECURITY_POLICY_CURRENT_TABLE} (
            ric,
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ric) DO UPDATE SET
            price_ingest_enabled = excluded.price_ingest_enabled,
            pit_fundamentals_enabled = excluded.pit_fundamentals_enabled,
            pit_classification_enabled = excluded.pit_classification_enabled,
            allow_cuse_native_core = excluded.allow_cuse_native_core,
            allow_cuse_fundamental_projection = excluded.allow_cuse_fundamental_projection,
            allow_cuse_returns_projection = excluded.allow_cuse_returns_projection,
            allow_cpar_core_target = excluded.allow_cpar_core_target,
            allow_cpar_extended_target = excluded.allow_cpar_extended_target,
            policy_source = COALESCE(NULLIF(excluded.policy_source, ''), {SECURITY_POLICY_CURRENT_TABLE}.policy_source),
            job_run_id = COALESCE(NULLIF(excluded.job_run_id, ''), {SECURITY_POLICY_CURRENT_TABLE}.job_run_id),
            updated_at = COALESCE(NULLIF(excluded.updated_at, ''), {SECURITY_POLICY_CURRENT_TABLE}.updated_at)
        """,
        payload,
    )
    return len(payload)


def sync_security_registry_seed(
    conn: sqlite3.Connection,
    *,
    seed_path: Path | None = None,
    source: str = "security_registry_seed",
    policy_source: str = "registry_seed_defaults",
) -> dict[str, Any]:
    rows = load_security_registry_seed_rows(seed_path)
    resolved_path = _resolved_seed_path(seed_path)
    if not rows:
        return {
            "status": "missing",
            "seed_path": str(resolved_path),
            "seed_rows": 0,
            "registry_rows_upserted": 0,
            "policy_rows_upserted": 0,
        }

    now_iso = datetime.now(timezone.utc).isoformat()
    job_run_id = f"security_registry_seed_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    registry_rows_upserted = upsert_security_registry_rows(
        conn,
        rows,
        source=source,
        job_run_id=job_run_id,
        updated_at=now_iso,
    )
    policy_rows_upserted = upsert_security_policy_rows(
        conn,
        rows,
        policy_source=policy_source,
        job_run_id=job_run_id,
        updated_at=now_iso,
    )
    return {
        "status": "ok",
        "seed_path": str(resolved_path),
        "seed_rows": len(rows),
        "registry_rows_upserted": registry_rows_upserted,
        "policy_rows_upserted": policy_rows_upserted,
        "job_run_id": job_run_id,
        "updated_at": now_iso,
    }


def reconcile_default_security_policy_rows(
    conn: sqlite3.Connection,
    *,
    rics: list[str] | None = None,
) -> int:
    if not (_table_exists(conn, SECURITY_POLICY_CURRENT_TABLE) and _table_exists(conn, SECURITY_REGISTRY_TABLE)):
        return 0
    has_taxonomy = _table_exists(conn, SECURITY_TAXONOMY_CURRENT_TABLE)
    has_compat = _table_exists(conn, SECURITY_MASTER_COMPAT_CURRENT_TABLE)
    if not (has_taxonomy or has_compat):
        return 0
    params: list[Any] = []
    ric_filter = ""
    clean_rics = [normalize_ric(ric) for ric in (rics or []) if normalize_ric(ric)]
    if clean_rics:
        placeholders = ",".join("?" for _ in clean_rics)
        ric_filter = f" AND UPPER(TRIM(pol.ric)) IN ({placeholders})"
        params.extend(clean_rics)
    compat_join = ""
    taxonomy_join = ""
    coverage_expr = "NULL"
    instrument_kind_expr = "NULL"
    model_scope_expr = "NULL"
    single_name_expr = "0"
    if has_taxonomy:
        taxonomy_join = f"""
        LEFT JOIN {SECURITY_TAXONOMY_CURRENT_TABLE} tax
          ON UPPER(TRIM(tax.ric)) = UPPER(TRIM(pol.ric))
        """
        instrument_kind_expr = "tax.instrument_kind"
        model_scope_expr = "tax.model_home_market_scope"
        single_name_expr = "COALESCE(tax.is_single_name_equity, 0)"
    if has_compat:
        compat_join = """
        LEFT JOIN security_master_compat_current compat
          ON UPPER(TRIM(compat.ric)) = UPPER(TRIM(pol.ric))
        """
        coverage_expr = "compat.coverage_role"
        if has_taxonomy:
            single_name_expr = "COALESCE(tax.is_single_name_equity, compat.is_equity_eligible, 0)"
        else:
            single_name_expr = "COALESCE(compat.is_equity_eligible, 0)"
    rows = conn.execute(
        f"""
        SELECT
            UPPER(TRIM(pol.ric)) AS ric,
            pol.policy_source,
            {coverage_expr} AS coverage_role,
            {instrument_kind_expr} AS instrument_kind,
            {model_scope_expr} AS model_home_market_scope,
            {single_name_expr} AS is_single_name_equity
        FROM {SECURITY_POLICY_CURRENT_TABLE} pol
        JOIN {SECURITY_REGISTRY_TABLE} reg
          ON UPPER(TRIM(reg.ric)) = UPPER(TRIM(pol.ric))
        {taxonomy_join}
        {compat_join}
        WHERE pol.ric IS NOT NULL
          AND TRIM(pol.ric) <> ''
          AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
          {ric_filter}
        """,
        params,
    ).fetchall()
    payload: list[dict[str, Any]] = []
    for ric, policy_source, coverage_role, instrument_kind, model_home_market_scope, is_single_name_equity in rows:
        if not policy_source_is_default(policy_source):
            continue
        derived = derive_policy_flags_from_structure(
            legacy_coverage_role=coverage_role,
            instrument_kind=instrument_kind,
            model_home_market_scope=model_home_market_scope,
            is_single_name_equity=is_single_name_equity,
        )
        payload.append(
            {
                "ric": normalize_ric(ric),
                "policy_source": normalize_policy_source(policy_source) or "registry_seed_defaults",
                **derived,
            }
        )
    if not payload:
        return 0
    now_iso = datetime.now(timezone.utc).isoformat()
    return upsert_security_policy_rows(
        conn,
        payload,
        policy_source="registry_seed_defaults",
        job_run_id=f"policy_reconcile_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        updated_at=now_iso,
    )


def ensure_registry_rows_from_master_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    source: str = "security_master_mirror",
) -> int:
    if not rows:
        return 0
    default_updated_at = datetime.now(timezone.utc).isoformat()
    registry_rows = [
        {
            "ric": row.get("ric"),
            "ticker": row.get("ticker"),
            "isin": row.get("isin"),
            "exchange_name": row.get("exchange_name"),
            "tracking_status": row.get("tracking_status") or "active",
            "source": row.get("source") or source,
            "job_run_id": row.get("job_run_id") or "security_master_mirror",
            "updated_at": row.get("updated_at") or default_updated_at,
        }
        for row in rows
    ]
    return upsert_security_registry_rows(
        conn,
        registry_rows,
        source=source,
        job_run_id="security_master_mirror",
        updated_at=default_updated_at,
    )
