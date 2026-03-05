"""GET /api/exposures?mode= — per-factor values with position-level drilldown."""

from __future__ import annotations

import math

from fastapi import APIRouter, Query

from backend import config
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.history_queries import load_factor_return_history
from backend.data.sqlite import cache_get

router = APIRouter()


@router.get("/exposures")
async def get_exposures(mode: str = Query("raw", pattern="^(raw|sensitivity|risk_contribution)$")):
    data = cache_get("exposures")
    if data is None:
        raise_cache_not_ready(
            cache_key="exposures",
            message="Exposure cache is not ready yet. Run refresh and try again.",
            refresh_mode="light",
        )
    factors = data.get(mode, [])
    return {"mode": mode, "factors": factors, "_cached": True}


@router.get("/exposures/history")
async def get_exposure_history(
    factor: str = Query(..., min_length=1),
    years: int = Query(5, ge=1, le=10),
):
    latest, rows = load_factor_return_history(
        config.SQLITE_PATH,
        factor=str(factor),
        years=int(years),
    )
    if latest is None:
        raise_cache_not_ready(
            cache_key="daily_factor_returns",
            message="Historical factor returns are not available yet.",
            refresh_mode="full",
        )

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
