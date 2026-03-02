"""POST /api/refresh — refresh caches and weekly-gated risk-engine state."""

from fastapi import APIRouter
from analytics.pipeline import run_refresh
from fastapi import Query

router = APIRouter()


@router.post("/refresh")
async def refresh(force_risk_recompute: bool = Query(False)):
    result = run_refresh(force_risk_recompute=bool(force_risk_recompute))
    return result
