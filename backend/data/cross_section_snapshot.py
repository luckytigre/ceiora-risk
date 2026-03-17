"""Canonical cross-section snapshot facade keyed by (ric, as_of_date)."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from backend.data.cross_section_snapshot_build import build_snapshot_payload
from backend.data.cross_section_snapshot_schema import (
    TABLE,
    ensure_cross_section_snapshot_table,
    pk_cols as _pk_cols,
    table_columns as _table_columns,
    table_exists as _table_exists,
)
from backend.data.trbc_schema import ensure_trbc_naming


def rebuild_cross_section_snapshot(
    data_db: Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    tickers: list[str] | None = None,
    mode: str = "current",
) -> dict[str, Any]:
    conn = sqlite3.connect(str(data_db))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        ensure_trbc_naming(conn)
        ensure_cross_section_snapshot_table(conn)

        result = build_snapshot_payload(
            conn,
            start_date=start_date,
            end_date=end_date,
            tickers=tickers,
            mode=mode,
        )
        if str(result.get("status")) != "ok":
            return result

        payload = result.pop("payload")
        base_tickers = list(result.pop("base_tickers", []))
        mode_norm = str(result.get("mode") or "current").strip().lower()

        if mode_norm != "full":
            if tickers and base_tickers:
                placeholders = ",".join("?" for _ in base_tickers)
                conn.execute(
                    f"DELETE FROM {TABLE} WHERE UPPER(ticker) IN ({placeholders})",
                    base_tickers,
                )
            elif not tickers:
                conn.execute(f"DELETE FROM {TABLE}")

        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {TABLE}
            ({", ".join(payload.columns.tolist())})
            VALUES ({", ".join(['?'] * len(payload.columns))})
            """,
            payload.itertuples(index=False, name=None),
        )
        conn.commit()
        return result
    finally:
        conn.close()
