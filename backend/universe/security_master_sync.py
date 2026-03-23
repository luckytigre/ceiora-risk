"""Security-master sync helpers for canonical universe bootstrap and LSEG enrichment."""

from __future__ import annotations

import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.universe.schema import SECURITY_MASTER_TABLE


DEFAULT_SECURITY_MASTER_SEED_PATH = Path(__file__).resolve().parents[2] / "data/reference/security_master_seed.csv"
_PRIMARY_SUFFIX_RANK = {
    ".N": 0,
    ".OQ": 1,
    ".O": 2,
    ".K": 3,
    ".P": 4,
}


def normalize_ric(value: str | None) -> str:
    return str(value or "").strip().upper()


def normalize_ticker(value: str | None) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def normalize_optional_text(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return text


def ticker_from_ric(ric: str | None) -> str | None:
    text = normalize_ric(ric)
    if not text:
        return None
    return text.split(".", 1)[0]


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
    degraded_recent = _recent_degraded_price_rics(conn, recent_sessions=recent_sessions)
    rows = conn.execute(
        f"""
        SELECT
            UPPER(TRIM(ric)) AS ric,
            UPPER(TRIM(ticker)) AS ticker,
            exchange_name,
            COALESCE(classification_ok, 0) AS classification_ok,
            COALESCE(is_equity_eligible, 0) AS is_equity_eligible,
            COALESCE(source, '') AS source
        FROM {SECURITY_MASTER_TABLE}
        WHERE ric IS NOT NULL
          AND TRIM(ric) <> ''
          AND ticker IS NOT NULL
          AND TRIM(ticker) <> ''
        ORDER BY ticker, ric
        """
    ).fetchall()
    pending_rows: list[dict[str, str]] = []
    current_by_ticker: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not row:
            continue
        ric = normalize_ric(row[0])
        ticker = normalize_ticker(row[1]) or ticker_from_ric(ric)
        exchange_name = normalize_optional_text(row[2])
        classification_ok = int(row[3] or 0)
        is_equity_eligible = int(row[4] or 0)
        source = normalize_optional_text(row[5]) or ""
        if not ric or not ticker:
            continue
        if include_pending_seed and classification_ok == 0 and is_equity_eligible == 0 and source == "security_master_seed":
            pending_rows.append({"ticker": ticker, "ric": ric})
            continue
        if classification_ok != 1 or is_equity_eligible != 1:
            continue
        if ric in degraded_recent:
            continue
        if _source_quality_exclusion_reason(ric=ric, ticker=ticker, exchange_name=exchange_name):
            continue
        candidate = {
            "ticker": ticker,
            "ric": ric,
            "suffix_rank": _source_suffix_rank(ric),
        }
        existing = current_by_ticker.get(ticker)
        if existing is None or (
            int(candidate["suffix_rank"]),
            str(candidate["ric"]),
        ) < (
            int(existing["suffix_rank"]),
            str(existing["ric"]),
        ):
            current_by_ticker[ticker] = candidate
    out = pending_rows + [
        {"ticker": str(row["ticker"]), "ric": str(row["ric"])}
        for _, row in sorted(current_by_ticker.items(), key=lambda item: item[0])
    ]
    return out


def load_projection_only_universe_rows(
    conn: sqlite3.Connection,
) -> list[dict[str, str]]:
    """Load projection-only instruments (e.g. ETFs) from security_master."""
    rows = conn.execute(
        f"""
        SELECT UPPER(TRIM(ric)) AS ric, UPPER(TRIM(ticker)) AS ticker
        FROM {SECURITY_MASTER_TABLE}
        WHERE coverage_role = 'projection_only'
          AND ric IS NOT NULL AND TRIM(ric) <> ''
          AND ticker IS NOT NULL AND TRIM(ticker) <> ''
        ORDER BY ticker
        """
    ).fetchall()
    return [
        {"ticker": str(row[1]), "ric": str(row[0])}
        for row in rows
        if row and row[0] and row[1]
    ]


def load_price_ingest_universe_rows(
    conn: sqlite3.Connection,
    *,
    include_pending_seed: bool = True,
    recent_sessions: int = 8,
) -> list[dict[str, str]]:
    """Union of default source universe + projection-only instruments for price ingestion."""
    default_rows = load_default_source_universe_rows(
        conn,
        include_pending_seed=include_pending_seed,
        recent_sessions=recent_sessions,
    )
    projection_rows = load_projection_only_universe_rows(conn)
    seen = {row["ric"] for row in default_rows}
    for row in projection_rows:
        if row["ric"] not in seen:
            default_rows.append(row)
            seen.add(row["ric"])
    return default_rows


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
    return len(payload)


def load_security_master_seed_rows(seed_path: Path) -> list[dict[str, Any]]:
    path = Path(seed_path).expanduser().resolve()
    if not path.exists():
        return []

    rows_by_ric: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            ric = normalize_ric(raw.get("ric"))
            if not ric:
                continue
            coverage_role = normalize_optional_text(raw.get("coverage_role")) or "native_equity"
            rows_by_ric[ric] = {
                "ric": ric,
                "ticker": normalize_ticker(raw.get("ticker")) or ticker_from_ric(ric),
                "isin": normalize_optional_text(raw.get("isin")),
                "exchange_name": normalize_optional_text(raw.get("exchange_name")),
                "coverage_role": coverage_role,
            }
    return [rows_by_ric[ric] for ric in sorted(rows_by_ric)]


def sync_security_master_seed(
    conn: sqlite3.Connection,
    *,
    seed_path: Path = DEFAULT_SECURITY_MASTER_SEED_PATH,
    source: str = "security_master_seed",
) -> dict[str, Any]:
    seed_rows = load_security_master_seed_rows(seed_path)
    if not seed_rows:
        return {
            "status": "missing",
            "seed_path": str(Path(seed_path).expanduser().resolve()),
            "seed_rows": 0,
            "rows_upserted": 0,
        }

    now_iso = datetime.now(timezone.utc).isoformat()
    job_run_id = f"security_master_seed_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
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
                source,
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
                source,
                job_run_id,
                now_iso,
                normalize_ric(row.get("ric")),
            )
            for row in seed_rows
            if normalize_ric(row.get("ric"))
        ],
    )
    rows_upserted = int(conn.total_changes - before)
    return {
        "status": "ok",
        "seed_path": str(Path(seed_path).expanduser().resolve()),
        "seed_rows": len(seed_rows),
        "rows_upserted": rows_upserted,
        "job_run_id": job_run_id,
        "updated_at": now_iso,
    }
