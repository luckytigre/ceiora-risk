"""Recompute and patch size/nonlinear_size scores in local barra_exposures.

Why this exists:
  Some snapshots can contain heavily repeated size values for large-cap names.
  This script recalculates Size and Nonlinear Size from local market-cap data
  and patches the latest exposure snapshot used by the dashboard.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from barra.descriptors import STYLE_SCORE_ABS_CAP, build_nonlinear_size
from barra.math_utils import standardize_cap_weighted

DEFAULT_DB = Path(__file__).resolve().parent.parent / "data.db"
EPS = 1e-12


def _latest_tuple(conn: sqlite3.Connection) -> tuple[str, str, str, str]:
    row = conn.execute(
        """
        SELECT as_of_date, barra_model_version, descriptor_schema_version, assumption_set_version
        FROM barra_exposures
        ORDER BY as_of_date DESC, updated_at DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        raise RuntimeError("barra_exposures is empty")
    return str(row[0]), str(row[1]), str(row[2]), str(row[3])


def _load_caps_for_snapshot(
    conn: sqlite3.Connection,
    *,
    as_of_date: str,
    bmv: str,
    dsv: str,
    asv: str,
) -> pd.DataFrame:
    q = """
    WITH latest_caps AS (
        SELECT
            ticker,
            CAST(market_cap AS REAL) AS market_cap,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY fetch_date DESC) AS rn
        FROM fundamental_snapshots
        WHERE fetch_date <= ?
    )
    SELECT
        e.ticker,
        CAST(e.size_score AS REAL) AS old_size_score,
        CAST(e.nonlinear_size_score AS REAL) AS old_nonlinear_size_score,
        lc.market_cap
    FROM barra_exposures e
    LEFT JOIN latest_caps lc
      ON lc.ticker = e.ticker
     AND lc.rn = 1
    WHERE e.as_of_date = ?
      AND e.barra_model_version = ?
      AND e.descriptor_schema_version = ?
      AND e.assumption_set_version = ?
    ORDER BY e.ticker
    """
    return pd.read_sql_query(q, conn, params=[as_of_date, as_of_date, bmv, dsv, asv])


def patch_latest_size_scores(db_path: Path) -> dict[str, float | int | str]:
    conn = sqlite3.connect(str(db_path))
    try:
        as_of, bmv, dsv, asv = _latest_tuple(conn)
        df = _load_caps_for_snapshot(conn, as_of_date=as_of, bmv=bmv, dsv=dsv, asv=asv)
        if df.empty:
            raise RuntimeError("No rows in latest snapshot")

        work = df.copy()
        work["market_cap"] = pd.to_numeric(work["market_cap"], errors="coerce")
        work = work[(work["market_cap"] > 0) & work["market_cap"].notna()].copy()
        if work.empty:
            raise RuntimeError("No positive market caps available to recompute size scores")

        caps = work["market_cap"].to_numpy(dtype=float)
        size_raw = np.log(caps)
        size_scores = standardize_cap_weighted(size_raw, caps)
        size_scores = np.clip(size_scores, -STYLE_SCORE_ABS_CAP, STYLE_SCORE_ABS_CAP)
        size_series = pd.Series(size_scores, index=work.index, dtype=float)
        nonlinear_series = build_nonlinear_size(size_series, pd.Series(caps, index=work.index))
        nonlinear_series = nonlinear_series.clip(-STYLE_SCORE_ABS_CAP, STYLE_SCORE_ABS_CAP)

        work["new_size_score"] = size_series
        work["new_nonlinear_size_score"] = nonlinear_series

        updates = [
            (
                float(r.new_size_score),
                float(r.new_nonlinear_size_score),
                str(r.ticker),
                as_of,
                bmv,
                dsv,
                asv,
            )
            for r in work.itertuples(index=False)
        ]

        conn.executemany(
            """
            UPDATE barra_exposures
               SET size_score = ?, nonlinear_size_score = ?
             WHERE ticker = ?
               AND as_of_date = ?
               AND barra_model_version = ?
               AND descriptor_schema_version = ?
               AND assumption_set_version = ?
            """,
            updates,
        )
        conn.commit()

        old_unique = int(work["old_size_score"].round(6).nunique(dropna=True))
        new_unique = int(work["new_size_score"].round(6).nunique(dropna=True))
        old_repeat = int((work["old_size_score"] == work["old_size_score"].mode().iloc[0]).sum())
        new_repeat = int((work["new_size_score"] == work["new_size_score"].mode().iloc[0]).sum())

        return {
            "as_of_date": as_of,
            "rows_patched": int(len(work)),
            "old_unique_size_scores": old_unique,
            "new_unique_size_scores": new_unique,
            "old_mode_count": old_repeat,
            "new_mode_count": new_repeat,
        }
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Patch latest size/nonlinear_size scores in data.db")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Path to data.db")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    result = patch_latest_size_scores(Path(args.db_path))
    print(result)
