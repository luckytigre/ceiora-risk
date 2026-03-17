#!/usr/bin/env python3
"""Augment security_master from an XLSX file containing a RIC list."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.universe.schema import SECURITY_MASTER_TABLE, ensure_cuse4_schema


def _normalize_ric_series(series: pd.Series) -> pd.Series:
    out = (
        series.fillna("")
        .astype(str)
        .str.upper()
        .str.strip()
        .str.replace(r"\s+", "", regex=True)
    )
    return out[(out != "") & (out != "NAN")]


def _pick_ric_column(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "ric" in str(c).strip().lower():
            return str(c)
    return str(df.columns[0])


def _to_ticker(ric: str) -> str:
    text = str(ric or "").strip().upper()
    if not text:
        return text
    return text.split(".", 1)[0]


def run(
    *,
    db_path: Path,
    xlsx_path: Path,
    sheet: str | None,
    source: str,
    output_new_rics: Path | None,
) -> dict[str, object]:
    xlsx = Path(xlsx_path).expanduser().resolve()
    if not xlsx.exists():
        raise FileNotFoundError(f"xlsx file not found: {xlsx}")

    excel = pd.ExcelFile(xlsx)
    sheet_name = sheet if sheet else str(excel.sheet_names[0])
    frame = pd.read_excel(xlsx, sheet_name=sheet_name)
    if frame.empty:
        return {
            "status": "ok",
            "xlsx_path": str(xlsx),
            "sheet": sheet_name,
            "input_rows": 0,
            "unique_rics": 0,
            "existing_rics": 0,
            "new_rics_inserted": 0,
            "new_rics_preview": [],
            "new_rics_file": None,
        }

    ric_col = _pick_ric_column(frame)
    rics = sorted(set(_normalize_ric_series(frame[ric_col]).tolist()))

    now_iso = datetime.now(timezone.utc).isoformat()
    job_run_id = f"universe_augment_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    ensure_cuse4_schema(conn)

    try:
        existing = {
            str(r[0]).upper()
            for r in conn.execute(f"SELECT UPPER(ric) FROM {SECURITY_MASTER_TABLE}")
            if r and r[0]
        }
        new_rics = [r for r in rics if r not in existing]

        rows = [
            {
                "ric": ric,
                "ticker": _to_ticker(ric),
                "classification_ok": 0,
                "is_equity_eligible": 0,
                "source": source,
                "job_run_id": job_run_id,
                "updated_at": now_iso,
            }
            for ric in new_rics
        ]
        if rows:
            conn.executemany(
                f"""
                INSERT OR IGNORE INTO {SECURITY_MASTER_TABLE} (
                    ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["ric"],
                        row["ticker"],
                        row["classification_ok"],
                        row["is_equity_eligible"],
                        row["source"],
                        row["job_run_id"],
                        row["updated_at"],
                    )
                    for row in rows
                ],
            )

        # Touch coverage registry metadata without forcing eligibility flags.
        if rics:
            chunk = 500
            for i in range(0, len(rics), chunk):
                part = rics[i : i + chunk]
                placeholders = ",".join("?" for _ in part)
                conn.execute(
                    f"""
                    UPDATE {SECURITY_MASTER_TABLE}
                    SET
                        updated_at = ?,
                        source = COALESCE(source, ?),
                        job_run_id = COALESCE(job_run_id, ?)
                    WHERE UPPER(ric) IN ({placeholders})
                    """,
                    (now_iso, source, job_run_id, *part),
                )
        conn.commit()
    finally:
        conn.close()

    out_file = None
    if output_new_rics is not None:
        out_path = Path(output_new_rics).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join(new_rics) + ("\n" if new_rics else ""), encoding="utf-8")
        out_file = str(out_path)

    return {
        "status": "ok",
        "xlsx_path": str(xlsx),
        "sheet": sheet_name,
        "ric_column": ric_col,
        "input_rows": int(len(frame)),
        "unique_rics": int(len(rics)),
        "existing_rics": int(len(rics) - len(new_rics)),
        "new_rics_inserted": int(len(new_rics)),
        "new_rics_preview": new_rics[:25],
        "new_rics_file": out_file,
        "source": source,
        "job_run_id": job_run_id,
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", type=Path, default=Path("backend/data.db"), help="Path to SQLite DB")
    p.add_argument("--xlsx-path", type=Path, required=True, help="Path to universe XLSX")
    p.add_argument("--sheet", default=None, help="Optional sheet name (defaults to first sheet)")
    p.add_argument("--source", default="coverage_universe_xlsx", help="security_master.source value")
    p.add_argument("--output-new-rics", type=Path, default=None, help="Optional output text file for newly inserted RICs")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    result = run(
        db_path=args.db_path,
        xlsx_path=args.xlsx_path,
        sheet=args.sheet,
        source=str(args.source),
        output_new_rics=args.output_new_rics,
    )
    print(json.dumps(result, indent=2))
