"""POST /api/refresh — refresh caches and weekly-gated risk-engine state."""

from fastapi import APIRouter
from analytics.pipeline import run_refresh
from fastapi import Query

router = APIRouter()


@router.post("/refresh")
async def refresh(
    force_risk_recompute: bool = Query(False),
    mode: str = Query("full"),
):
    clean_mode = str(mode or "full").strip().lower()
    if clean_mode not in {"full", "light"}:
        clean_mode = "full"
    result = run_refresh(
        force_risk_recompute=bool(force_risk_recompute),
        mode=clean_mode,
    )
    return result
