"""POST /api/refresh — profile-driven orchestrated refresh."""

from fastapi import APIRouter
from fastapi import Query
from fastapi.responses import JSONResponse

from services.refresh_manager import get_refresh_status, start_refresh

router = APIRouter()


@router.post("/refresh", status_code=202)
async def refresh(
    force_risk_recompute: bool = Query(False),
    force_core: bool = Query(False),
    mode: str = Query("full"),
    profile: str | None = Query(None),
    as_of_date: str | None = Query(None),
    resume_run_id: str | None = Query(None),
    from_stage: str | None = Query(None),
    to_stage: str | None = Query(None),
):
    clean_mode = str(mode or "full").strip().lower()
    if clean_mode not in {"full", "light"}:
        clean_mode = "full"
    try:
        started, state = start_refresh(
            force_risk_recompute=bool(force_risk_recompute),
            force_core=bool(force_core),
            mode=clean_mode,
            profile=profile,
            as_of_date=as_of_date,
            resume_run_id=resume_run_id,
            from_stage=from_stage,
            to_stage=to_stage,
        )
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={
                "status": "invalid_request",
                "message": str(exc),
            },
        )
    if not started:
        return JSONResponse(
            status_code=409,
            content={
                "status": "busy",
                "message": "A refresh is already running.",
                "refresh": state,
            },
        )
    return {
        "status": "accepted",
        "message": "Refresh started in background.",
        "refresh": state,
    }


@router.get("/refresh/status")
async def refresh_status():
    return {
        "status": "ok",
        "refresh": get_refresh_status(),
    }
