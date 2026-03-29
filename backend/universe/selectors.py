"""Named universe selectors with legacy-parity fallbacks."""

from __future__ import annotations

import sqlite3
from typing import Any

from backend.universe.registry_sync import (
    normalize_optional_text,
    normalize_ric,
    normalize_ticker,
    ticker_from_ric,
)
from backend.universe.runtime_rows import load_security_runtime_rows
from backend.universe.schema import SECURITY_REGISTRY_TABLE, SECURITY_MASTER_TABLE

_PRIMARY_SUFFIX_RANK = {
    ".N": 0,
    ".OQ": 1,
    ".O": 2,
    ".K": 3,
    ".P": 4,
}


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


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
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


def _table_has_rows(conn: sqlite3.Connection, table: str) -> bool:
    if not _table_exists(conn, table):
        return False
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _load_legacy_selector_runtime_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, SECURITY_MASTER_TABLE):
        return []
    rows = conn.execute(
        f"""
        SELECT
            UPPER(TRIM(ric)) AS ric,
            UPPER(TRIM(COALESCE(ticker, ''))) AS ticker,
            exchange_name,
            COALESCE(classification_ok, 0) AS classification_ok,
            COALESCE(is_equity_eligible, 0) AS is_equity_eligible,
            COALESCE(coverage_role, 'native_equity') AS coverage_role,
            source
        FROM {SECURITY_MASTER_TABLE}
        WHERE ric IS NOT NULL
          AND TRIM(ric) <> ''
          AND ticker IS NOT NULL
          AND TRIM(ticker) <> ''
        """
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        ric = normalize_ric(row[0])
        ticker = normalize_ticker(row[1]) or ticker_from_ric(row[0])
        coverage_role = normalize_optional_text(row[5]) or "native_equity"
        classification_ready = int(row[3] or 0)
        is_equity_eligible = int(row[4] or 0)
        out.append(
            {
                "ric": ric,
                "ticker": ticker,
                "exchange_name": normalize_optional_text(row[2]),
                "tracking_status": "active",
                "source": normalize_optional_text(row[6]),
                "classification_ready": classification_ready,
                "is_single_name_equity": 0 if coverage_role == "projection_only" else is_equity_eligible,
                "price_ingest_enabled": 1,
                "pit_fundamentals_enabled": 0 if coverage_role == "projection_only" else is_equity_eligible,
                "pit_classification_enabled": 0 if coverage_role == "projection_only" else classification_ready,
                "allow_cuse_returns_projection": 1 if coverage_role == "projection_only" else 0,
            }
        )
    return out


def _load_selector_runtime_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = load_security_runtime_rows(
        conn,
        include_disabled=False,
    )
    if rows:
        return rows
    if _table_has_rows(conn, SECURITY_REGISTRY_TABLE):
        return rows
    return _load_legacy_selector_runtime_rows(conn)


def load_registry_active_rows(conn: sqlite3.Connection) -> list[dict[str, str]]:
    if _table_exists(conn, SECURITY_REGISTRY_TABLE):
        rows = conn.execute(
            f"""
            SELECT UPPER(TRIM(ric)) AS ric, UPPER(TRIM(ticker)) AS ticker
            FROM {SECURITY_REGISTRY_TABLE}
            WHERE ric IS NOT NULL AND TRIM(ric) <> ''
              AND ticker IS NOT NULL AND TRIM(ticker) <> ''
              AND COALESCE(NULLIF(TRIM(tracking_status), ''), 'active') = 'active'
            ORDER BY ticker, ric
            """
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT UPPER(TRIM(ric)) AS ric, UPPER(TRIM(ticker)) AS ticker
            FROM {SECURITY_MASTER_TABLE}
            WHERE ric IS NOT NULL AND TRIM(ric) <> ''
              AND ticker IS NOT NULL AND TRIM(ticker) <> ''
            ORDER BY ticker, ric
            """
        ).fetchall()
    return [{"ticker": str(row[1]), "ric": str(row[0])} for row in rows if row and row[0] and row[1]]


def load_pit_ingest_scope_rows(
    conn: sqlite3.Connection,
    *,
    include_pending_seed: bool = True,
    recent_sessions: int = 8,
) -> list[dict[str, str]]:
    degraded_recent = _recent_degraded_price_rics(conn, recent_sessions=recent_sessions)
    runtime_rows = _load_selector_runtime_rows(conn)
    pending_rows: list[dict[str, str]] = []
    current_by_ticker: dict[str, dict[str, Any]] = {}
    for row in runtime_rows:
        ric = normalize_ric(row.get("ric"))
        ticker = normalize_ticker(row.get("ticker")) or ticker_from_ric(ric)
        exchange_name = normalize_optional_text(row.get("exchange_name"))
        source = normalize_optional_text(row.get("source")) or ""
        tracking_status = normalize_optional_text(row.get("tracking_status")) or "active"
        pit_enabled = bool(
            int(row.get("pit_fundamentals_enabled") or 0) == 1
            or int(row.get("pit_classification_enabled") or 0) == 1
        )
        if not ric or not ticker:
            continue
        if tracking_status != "active":
            continue
        if (
            include_pending_seed
            and int(row.get("classification_ready") or 0) == 0
            and source.endswith("_seed")
            and int(row.get("allow_cuse_returns_projection") or 0) != 1
        ):
            pending_rows.append({"ticker": ticker, "ric": ric})
            continue
        if pit_enabled is False:
            continue
        if int(row.get("is_single_name_equity") or 0) != 1:
            continue
        if int(row.get("classification_ready") or 0) != 1:
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
    return pending_rows + [
        {"ticker": str(row["ticker"]), "ric": str(row["ric"])}
        for _, row in sorted(current_by_ticker.items(), key=lambda item: item[0])
    ]


def load_cuse_returns_projection_scope_rows(conn: sqlite3.Connection) -> list[dict[str, str]]:
    rows = _load_selector_runtime_rows(conn)
    out = [
        {
            "ticker": str(row.get("ticker") or ""),
            "ric": str(row.get("ric") or ""),
        }
        for row in rows
        if (normalize_optional_text(row.get("tracking_status")) or "active") == "active"
        if int(row.get("allow_cuse_returns_projection") or 0) == 1
        and row.get("ric")
        and row.get("ticker")
    ]
    return sorted(out, key=lambda row: (str(row["ticker"]), str(row["ric"])))


def load_price_ingest_scope_rows(
    conn: sqlite3.Connection,
    *,
    include_pending_seed: bool = True,
    recent_sessions: int = 8,
) -> list[dict[str, str]]:
    del recent_sessions
    runtime_rows = _load_selector_runtime_rows(conn)
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in runtime_rows:
        ric = normalize_ric(row.get("ric"))
        ticker = normalize_ticker(row.get("ticker")) or ticker_from_ric(ric)
        tracking_status = normalize_optional_text(row.get("tracking_status")) or "active"
        exchange_name = normalize_optional_text(row.get("exchange_name"))
        if not ric or not ticker:
            continue
        if tracking_status != "active":
            continue
        pending_seed_structural = (
            int(row.get("classification_ready") or 0) == 0
            and (normalize_optional_text(row.get("source")) or "").endswith("_seed")
            and int(row.get("allow_cuse_returns_projection") or 0) != 1
        )
        if not include_pending_seed and pending_seed_structural:
            continue
        if int(row.get("price_ingest_enabled") or 0) != 1:
            continue
        if _source_quality_exclusion_reason(ric=ric, ticker=ticker, exchange_name=exchange_name):
            continue
        if ric in seen:
            continue
        seen.add(ric)
        out.append({"ticker": ticker, "ric": ric})
    return sorted(out, key=lambda row: (str(row["ticker"]), str(row["ric"])))


def load_identifier_refresh_scope_rows(conn: sqlite3.Connection) -> list[dict[str, str]]:
    return load_registry_active_rows(conn)


def load_cuse_structural_candidate_scope_rows(
    conn: sqlite3.Connection,
    *,
    include_pending_seed: bool = False,
    recent_sessions: int = 8,
) -> list[dict[str, str]]:
    return load_pit_ingest_scope_rows(
        conn,
        include_pending_seed=include_pending_seed,
        recent_sessions=recent_sessions,
    )


def load_cpar_build_scope_rows(conn: sqlite3.Connection) -> list[dict[str, str]]:
    rows = load_security_runtime_rows(
        conn,
        include_disabled=False,
    )
    out = [
        {
            "ticker": str(row.get("ticker") or ""),
            "ric": str(row.get("ric") or ""),
        }
        for row in rows
        if (normalize_optional_text(row.get("tracking_status")) or "active") == "active"
        if (
            int(row.get("allow_cpar_core_target") or 0) == 1
            or int(row.get("allow_cpar_extended_target") or 0) == 1
        )
        and row.get("ric")
        and row.get("ticker")
    ]
    return sorted(out, key=lambda row: (str(row["ticker"]), str(row["ric"])))


def load_cpar_factor_basis_scope_rows(conn: sqlite3.Connection) -> list[dict[str, str]]:
    return load_cpar_build_scope_rows(conn)
