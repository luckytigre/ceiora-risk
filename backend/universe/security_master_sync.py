"""Security-master sync helpers for canonical universe bootstrap and LSEG enrichment."""

from __future__ import annotations

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
from backend.universe.registry_sync import (
    DEFAULT_SECURITY_REGISTRY_SEED_PATH,
    LEGACY_SECURITY_MASTER_SEED_PATH,
    ensure_registry_rows_from_master_rows,
    legacy_coverage_role_from_policy_flags,
    load_security_registry_seed_rows,
    reconcile_default_security_policy_rows,
    sync_security_registry_seed,
)
from backend.universe.selectors import (
    load_cuse_returns_projection_scope_rows,
    load_pit_ingest_scope_rows,
    load_price_ingest_scope_rows,
)
from backend.universe.schema import SECURITY_MASTER_TABLE
from backend.universe.source_observation import refresh_security_source_observation_daily
from backend.universe.taxonomy_builder import (
    materialize_security_master_compat_current,
    refresh_security_taxonomy_current,
)


DEFAULT_SECURITY_MASTER_SEED_PATH = DEFAULT_SECURITY_REGISTRY_SEED_PATH
_PRIMARY_SUFFIX_RANK = {
    ".N": 0,
    ".OQ": 1,
    ".O": 2,
    ".K": 3,
    ".P": 4,
}


def _seed_source_label(seed_path: Path) -> str:
    name = Path(seed_path).name.strip().lower()
    if name == LEGACY_SECURITY_MASTER_SEED_PATH.name.lower():
        return "security_master_seed"
    if name == DEFAULT_SECURITY_REGISTRY_SEED_PATH.name.lower():
        return "security_registry_seed"
    return "security_registry_seed"

def _source_suffix_rank(ric: str | None) -> int:
    text = normalize_ric(ric)
    for suffix, rank in _PRIMARY_SUFFIX_RANK.items():
        if text.endswith(suffix):
            return int(rank)
    return 99


def _source_quality_exclusion_reason(
    *,
    ric: str | None,
    ticker: str | None,
    exchange_name: str | None,
) -> str | None:
    ric_txt = normalize_ric(ric)
    ticker_txt = normalize_ticker(ticker) or ""
    exchange_txt = (normalize_optional_text(exchange_name) or "").upper()
    if not ric_txt:
        return "missing_ric"
    if "^" in ric_txt:
        return "lineage_ric"
    if "*" in ticker_txt:
        return "alias_ticker"
    if "CONSOLIDATED" in exchange_txt:
        return "consolidated_exchange"
    if "PINK SHEETS" in exchange_txt:
        return "pink_sheets"
    if "WHEN TRADING" in exchange_txt:
        return "secondary_when_trading"
    if " PSX" in exchange_txt or exchange_txt.startswith("NASDAQ OMX PSX"):
        return "secondary_psx"
    return None


def _recent_degraded_price_rics(
    conn: sqlite3.Connection,
    *,
    recent_sessions: int = 8,
) -> set[str]:
    if recent_sessions <= 0:
        return set()
    dates = [
        str(row[0])
        for row in conn.execute(
            """
            SELECT DISTINCT date
            FROM security_prices_eod
            WHERE date IS NOT NULL
            ORDER BY date DESC
            LIMIT ?
            """,
            (int(recent_sessions),),
        ).fetchall()
        if row and row[0]
    ]
    if len(dates) < 3:
        return set()
    placeholders = ",".join("?" for _ in dates)
    rows = conn.execute(
        f"""
        SELECT
            UPPER(TRIM(ric)) AS ric,
            COUNT(DISTINCT date) AS obs_count,
            COUNT(DISTINCT CAST(close AS TEXT)) AS distinct_close_count,
            MAX(COALESCE(volume, 0)) AS max_volume
        FROM security_prices_eod
        WHERE date IN ({placeholders})
        GROUP BY UPPER(TRIM(ric))
        HAVING COUNT(DISTINCT date) BETWEEN 1 AND 2
           AND COUNT(DISTINCT CAST(close AS TEXT)) = 1
           AND MAX(COALESCE(volume, 0)) <= 0
        """,
        dates,
    ).fetchall()
    return {normalize_ric(row[0]) for row in rows if row and row[0]}


def load_default_source_universe_rows(
    conn: sqlite3.Connection,
    *,
    include_pending_seed: bool = True,
    recent_sessions: int = 8,
) -> list[dict[str, str]]:
    return load_pit_ingest_scope_rows(
        conn,
        include_pending_seed=include_pending_seed,
        recent_sessions=recent_sessions,
    )


def load_projection_only_universe_rows(
    conn: sqlite3.Connection,
) -> list[dict[str, str]]:
    """Compatibility wrapper around the named returns-projection selector."""
    return load_cuse_returns_projection_scope_rows(conn)


def load_price_ingest_universe_rows(
    conn: sqlite3.Connection,
    *,
    include_pending_seed: bool = True,
    recent_sessions: int = 8,
) -> list[dict[str, str]]:
    """Compatibility wrapper around the named price-ingest selector."""
    return load_price_ingest_scope_rows(
        conn,
        include_pending_seed=include_pending_seed,
        recent_sessions=recent_sessions,
    )


def derive_security_master_flags(
    *,
    trbc_economic_sector: str | None,
    trbc_business_sector: str | None,
    trbc_industry_group: str | None,
    trbc_industry: str | None,
    trbc_activity: str | None,
    hq_country_code: str | None,
) -> tuple[int, int]:
    from backend.risk_model.eligibility import NON_EQUITY_ECONOMIC_SECTORS

    sector = normalize_optional_text(trbc_economic_sector)
    has_classification = any(
        normalize_optional_text(value)
        for value in (
            sector,
            trbc_business_sector,
            trbc_industry_group,
            trbc_industry,
            trbc_activity,
            hq_country_code,
        )
    )
    classification_ok = 1 if has_classification else 0
    is_equity_eligible = 1 if classification_ok and sector not in NON_EQUITY_ECONOMIC_SECTORS else 0
    return classification_ok, is_equity_eligible


def upsert_security_master_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    refresh_runtime_surfaces: bool = True,
) -> int:
    if not rows:
        return 0

    sql = f"""
        INSERT INTO {SECURITY_MASTER_TABLE} (
            ric,
            ticker,
            isin,
            exchange_name,
            classification_ok,
            is_equity_eligible,
            coverage_role,
            source,
            job_run_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, 'native_equity'), ?, ?, ?)
        ON CONFLICT(ric) DO UPDATE SET
            ticker = COALESCE(NULLIF(excluded.ticker, ''), {SECURITY_MASTER_TABLE}.ticker),
            isin = COALESCE(NULLIF(excluded.isin, ''), {SECURITY_MASTER_TABLE}.isin),
            exchange_name = COALESCE(NULLIF(excluded.exchange_name, ''), {SECURITY_MASTER_TABLE}.exchange_name),
            classification_ok = COALESCE(excluded.classification_ok, {SECURITY_MASTER_TABLE}.classification_ok),
            is_equity_eligible = COALESCE(excluded.is_equity_eligible, {SECURITY_MASTER_TABLE}.is_equity_eligible),
            coverage_role = CASE
                WHEN {SECURITY_MASTER_TABLE}.coverage_role = 'projection_only'
                     AND COALESCE(NULLIF(excluded.coverage_role, ''), 'native_equity') = 'native_equity'
                THEN {SECURITY_MASTER_TABLE}.coverage_role
                ELSE COALESCE(NULLIF(excluded.coverage_role, ''), {SECURITY_MASTER_TABLE}.coverage_role)
            END,
            source = COALESCE(NULLIF(excluded.source, ''), {SECURITY_MASTER_TABLE}.source),
            job_run_id = COALESCE(NULLIF(excluded.job_run_id, ''), {SECURITY_MASTER_TABLE}.job_run_id),
            updated_at = COALESCE(NULLIF(excluded.updated_at, ''), {SECURITY_MASTER_TABLE}.updated_at)
    """
    payload = [
        (
            normalize_ric(row.get("ric")),
            normalize_ticker(row.get("ticker")),
            normalize_optional_text(row.get("isin")),
            normalize_optional_text(row.get("exchange_name")),
            int(row.get("classification_ok") or 0),
            int(row.get("is_equity_eligible") or 0),
            normalize_optional_text(row.get("coverage_role")),
            normalize_optional_text(row.get("source")),
            normalize_optional_text(row.get("job_run_id")),
            normalize_optional_text(row.get("updated_at")),
        )
        for row in rows
        if normalize_ric(row.get("ric"))
    ]
    if not payload:
        return 0
    conn.executemany(sql, payload)
    touched_rics = [str(item[0]) for item in payload if item and item[0]]
    ensure_registry_rows_from_master_rows(conn, rows)
    if refresh_runtime_surfaces:
        refresh_security_taxonomy_current(conn, rics=touched_rics)
        reconcile_default_security_policy_rows(conn, rics=touched_rics)
        refresh_security_source_observation_daily(conn, rics=touched_rics)
        materialize_security_master_compat_current(conn, rics=touched_rics)
    return len(payload)


def load_security_master_seed_rows(seed_path: Path) -> list[dict[str, Any]]:
    return [
        {
            "ric": row["ric"],
            "ticker": row.get("ticker"),
            "isin": row.get("isin"),
            "exchange_name": row.get("exchange_name"),
            "coverage_role": row.get("legacy_coverage_role")
            or legacy_coverage_role_from_policy_flags(
                allow_cuse_returns_projection=row.get("allow_cuse_returns_projection"),
                pit_fundamentals_enabled=row.get("pit_fundamentals_enabled"),
                pit_classification_enabled=row.get("pit_classification_enabled"),
            ),
        }
        for row in load_security_registry_seed_rows(seed_path)
    ]


def sync_security_master_seed(
    conn: sqlite3.Connection,
    *,
    seed_path: Path = DEFAULT_SECURITY_MASTER_SEED_PATH,
    source: str | None = None,
) -> dict[str, Any]:
    seed_source = normalize_optional_text(source) or _seed_source_label(seed_path)
    registry_sync = sync_security_registry_seed(conn, seed_path=seed_path, source=seed_source)
    seed_rows = load_security_master_seed_rows(seed_path)
    if not seed_rows:
        return {
            "status": "missing",
            "seed_path": str(Path(seed_path).expanduser().resolve()),
            "seed_rows": 0,
            "rows_upserted": 0,
            "registry_sync": registry_sync,
        }

    now_iso = datetime.now(timezone.utc).isoformat()
    job_run_id = f"{seed_source}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    insert_sql = f"""
        INSERT OR IGNORE INTO {SECURITY_MASTER_TABLE} (
            ric,
            ticker,
            isin,
            exchange_name,
            classification_ok,
            is_equity_eligible,
            coverage_role,
            source,
            job_run_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    update_sql = f"""
        UPDATE {SECURITY_MASTER_TABLE}
        SET
            ticker = COALESCE(NULLIF(ticker, ''), ?),
            isin = COALESCE(NULLIF(isin, ''), ?),
            exchange_name = COALESCE(NULLIF(exchange_name, ''), ?),
            coverage_role = COALESCE(NULLIF(?, ''), coverage_role),
            source = COALESCE(source, ?),
            job_run_id = COALESCE(job_run_id, ?),
            updated_at = COALESCE(NULLIF(updated_at, ''), ?)
        WHERE ric = ?
    """
    before = conn.total_changes
    conn.executemany(
        insert_sql,
        [
            (
                normalize_ric(row.get("ric")),
                normalize_ticker(row.get("ticker")) or ticker_from_ric(row.get("ric")),
                normalize_optional_text(row.get("isin")),
                normalize_optional_text(row.get("exchange_name")),
                0,
                0,
                normalize_optional_text(row.get("coverage_role")) or "native_equity",
                seed_source,
                job_run_id,
                now_iso,
            )
            for row in seed_rows
            if normalize_ric(row.get("ric"))
        ],
    )
    conn.executemany(
        update_sql,
        [
            (
                normalize_ticker(row.get("ticker")) or ticker_from_ric(row.get("ric")),
                normalize_optional_text(row.get("isin")),
                normalize_optional_text(row.get("exchange_name")),
                normalize_optional_text(row.get("coverage_role")) or "",
                seed_source,
                job_run_id,
                now_iso,
                normalize_ric(row.get("ric")),
            )
            for row in seed_rows
            if normalize_ric(row.get("ric"))
        ],
    )
    rows_upserted = int(conn.total_changes - before)
    touched_rics = [normalize_ric(row.get("ric")) for row in seed_rows if normalize_ric(row.get("ric"))]
    refresh_security_taxonomy_current(conn, rics=touched_rics)
    reconcile_default_security_policy_rows(conn, rics=touched_rics)
    refresh_security_source_observation_daily(conn, rics=touched_rics)
    materialize_security_master_compat_current(conn, rics=touched_rics)
    return {
        "status": "ok",
        "seed_path": str(Path(seed_path).expanduser().resolve()),
        "seed_rows": len(seed_rows),
        "rows_upserted": rows_upserted,
        "registry_sync": registry_sync,
        "job_run_id": job_run_id,
        "updated_at": now_iso,
    }
