"""GET /api/exposures?mode= — per-factor values with position-level drilldown."""

from __future__ import annotations

from datetime import date, timedelta
import math
import sqlite3

from fastapi import APIRouter, Query

import config
from db.sqlite import cache_get

router = APIRouter()


@router.get("/exposures")
async def get_exposures(mode: str = Query("raw", pattern="^(raw|sensitivity|risk_contribution)$")):
    data = cache_get("exposures")
    if data is None:
        return {"mode": mode, "factors": [], "_cached": False}
    factors = data.get(mode, [])
    return {"mode": mode, "factors": factors, "_cached": True}


@router.get("/exposures/history")
async def get_exposure_history(
    factor: str = Query(..., min_length=1),
    years: int = Query(5, ge=1, le=10),
):
    conn = sqlite3.connect(config.SQLITE_PATH)
    try:
        latest_row = conn.execute("SELECT MAX(date) FROM daily_factor_returns").fetchone()
        latest = latest_row[0] if latest_row and latest_row[0] else None
        if latest is None:
            return {"factor": factor, "years": years, "points": [], "_cached": False}

        latest_dt = date.fromisoformat(str(latest))
        start_dt = latest_dt - timedelta(days=365 * years)

        rows = conn.execute(
            """
            SELECT date, factor_return
            FROM daily_factor_returns
            WHERE factor_name = ?
              AND date >= ?
            ORDER BY date
            """,
            (factor, start_dt.isoformat()),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {"factor": factor, "years": years, "points": [], "_cached": True}

    points = []
    cumulative = 1.0
    for dt, raw_ret in rows:
        r = float(raw_ret or 0.0)
        if not math.isfinite(r):
            r = 0.0
        cumulative *= (1.0 + r)
        points.append({
            "date": str(dt),
            "factor_return": round(r, 8),
            "cum_return": round(cumulative - 1.0, 8),
        })

    return {"factor": factor, "years": years, "points": points, "_cached": True}
