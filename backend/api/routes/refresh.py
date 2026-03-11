"""POST /api/refresh — profile-driven orchestrated refresh."""

import secrets

from fastapi import APIRouter
from fastapi import Header
from fastapi import HTTPException
from fastapi import Query
from fastapi import status
from fastapi.responses import JSONResponse

from backend import config
from backend.api.auth import require_role
from backend.services.refresh_manager import get_refresh_status, start_refresh

router = APIRouter()


def _refresh_authorized(x_refresh_token: str | None, authorization: str | None) -> bool:
    if config.cloud_mode():
        return False
    expected = config.REFRESH_API_TOKEN
    if not expected:
        return True
    candidates: list[str] = []
    if x_refresh_token:
        candidates.append(str(x_refresh_token).strip())
    if authorization and str(authorization).lower().startswith("bearer "):
        candidates.append(str(authorization)[7:].strip())
    return any(c and secrets.compare_digest(expected, c) for c in candidates)


@router.post("/refresh", status_code=202)
async def refresh(
    force_risk_recompute: bool = Query(False),
    force_core: bool = Query(False),
    mode: str | None = Query(None),
    profile: str | None = Query(None),
    as_of_date: str | None = Query(None),
    resume_run_id: str | None = Query(None),
    from_stage: str | None = Query(None),
    to_stage: str | None = Query(None),
    x_refresh_token: str | None = Header(default=None, alias="X-Refresh-Token"),
    authorization: str | None = Header(default=None),
):
    if config.cloud_mode():
        require_role(
            "operator",
            x_operator_token=x_refresh_token,
            x_refresh_token=x_refresh_token,
            authorization=authorization,
        )
    elif not _refresh_authorized(x_refresh_token, authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    requested_mode = str(mode).strip().lower() if mode is not None else None
    clean_mode = requested_mode or "full"
    if clean_mode not in {"full", "light", "cold"}:
        clean_mode = "full"
    effective_profile = profile
    if config.cloud_mode() and not str(profile or "").strip() and requested_mode is None:
        effective_profile = "serve-refresh"
        clean_mode = "light"
    try:
        started, state = start_refresh(
            force_risk_recompute=bool(force_risk_recompute),
            force_core=bool(force_core),
            mode=clean_mode,
            profile=effective_profile,
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
async def refresh_status(
    x_refresh_token: str | None = Header(default=None, alias="X-Refresh-Token"),
    authorization: str | None = Header(default=None),
):
    if config.cloud_mode():
        require_role(
            "operator",
            x_operator_token=x_refresh_token,
            x_refresh_token=x_refresh_token,
            authorization=authorization,
        )
    elif not _refresh_authorized(x_refresh_token, authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    return {
        "status": "ok",
        "refresh": get_refresh_status(),
    }
