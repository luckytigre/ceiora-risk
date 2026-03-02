"""Utilities to resolve stable ticker->RIC mappings for LSEG pulls."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

import pandas as pd

RIC_MAP_TABLE = "ticker_ric_map"
DEFAULT_RIC_SUFFIXES = [".O", ".N", ".A", ".K", ".P", ".PK", ""]


def ensure_ric_map_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RIC_MAP_TABLE} (
            ticker TEXT PRIMARY KEY,
            ric TEXT NOT NULL,
            resolution_method TEXT,
            classification_ok INTEGER NOT NULL DEFAULT 0,
            as_of_date TEXT,
            source TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{RIC_MAP_TABLE}_ric ON {RIC_MAP_TABLE}(ric)")


def load_ric_map(conn: sqlite3.Connection) -> dict[str, str]:
    ensure_ric_map_table(conn)
    rows = conn.execute(f"SELECT ticker, ric FROM {RIC_MAP_TABLE}").fetchall()
    return {
        str(t).strip().upper(): str(r).strip().upper()
        for t, r in rows
        if t and r
    }


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        got = cols.get(c.lower())
        if got:
            return got
    return None


def _has_classification(value: Any) -> bool:
    if value is None or pd.isna(value):
        return False
    s = str(value).strip()
    return bool(s and s.lower() not in {"none", "nan", "null"})


def _upsert_ric_map(conn: sqlite3.Connection, rows: list[tuple[str, str, str, int, str, str, str]]) -> None:
    if not rows:
        return
    ensure_ric_map_table(conn)
    conn.executemany(
        f"""
        INSERT INTO {RIC_MAP_TABLE}
        (ticker, ric, resolution_method, classification_ok, as_of_date, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            ric = excluded.ric,
            resolution_method = excluded.resolution_method,
            classification_ok = excluded.classification_ok,
            as_of_date = excluded.as_of_date,
            source = excluded.source,
            updated_at = excluded.updated_at
        """,
        rows,
    )


def resolve_ric_map(
    *,
    client,
    conn: sqlite3.Connection,
    tickers: list[str],
    as_of_date: str,
    source: str,
    suffixes: list[str] | None = None,
    batch_size: int = 500,
) -> dict[str, str]:
    """Resolve RICs by probing multiple exchange suffixes and cache results."""
    ensure_ric_map_table(conn)
    ticker_list = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    if not ticker_list:
        return {}

    cached = load_ric_map(conn)
    resolved: dict[str, str] = {t: cached[t] for t in ticker_list if t in cached}
    unresolved = [t for t in ticker_list if t not in resolved]
    if not unresolved:
        return resolved

    probe_suffixes = list(suffixes or DEFAULT_RIC_SUFFIXES)
    probe_suffixes = [s.strip().upper() for s in probe_suffixes]
    # keep unique order
    seen: set[str] = set()
    probe_suffixes = [s for s in probe_suffixes if not (s in seen or seen.add(s))]

    fields = ["TR.TRBCEconomicSector", "TR.TRBCIndustryGroup", "TR.CommonName"]
    updated_at = datetime.now(timezone.utc).isoformat()
    upserts: list[tuple[str, str, str, int, str, str, str]] = []

    for suffix in probe_suffixes:
        if not unresolved:
            break
        candidates: list[str] = []
        candidate_to_ticker: dict[str, str] = {}
        for t in unresolved:
            if "." in t:
                inst = t
            elif suffix:
                inst = f"{t}{suffix}"
            else:
                inst = t
            inst_u = inst.strip().upper()
            candidates.append(inst_u)
            candidate_to_ticker[inst_u] = t

        for i in range(0, len(candidates), max(1, int(batch_size))):
            batch = candidates[i : i + max(1, int(batch_size))]
            try:
                part = client.get_company_data(batch, fields=fields, as_of_date=as_of_date)
            except Exception:
                continue
            if part is None or part.empty:
                continue
            instrument_col = _pick_col(part, ["Instrument"])
            sector_col = _pick_col(part, ["TRBC Economic Sector Name", "TRBC Economic Sector"])
            industry_col = _pick_col(part, ["TRBC Industry Group Name", "TRBC Industry Group"])
            if not instrument_col:
                continue
            for _, row in part.iterrows():
                instrument = str(row.get(instrument_col) or "").strip().upper()
                ticker = candidate_to_ticker.get(instrument)
                if not ticker:
                    continue
                sector = row.get(sector_col) if sector_col else None
                industry = row.get(industry_col) if industry_col else None
                ok = int(_has_classification(sector) or _has_classification(industry))
                if ok:
                    resolved[ticker] = instrument
                    upserts.append((ticker, instrument, f"suffix_probe:{suffix or 'plain'}", ok, as_of_date, source, updated_at))

        unresolved = [t for t in ticker_list if t not in resolved]

    _upsert_ric_map(conn, upserts)
    conn.commit()
    return resolved
