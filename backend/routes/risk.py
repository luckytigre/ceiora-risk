"""GET /api/risk — variance decomposition, factor details, covariance matrix."""

from fastapi import APIRouter
from backend.db.sqlite import cache_get
from backend.routes.readiness import raise_cache_not_ready

router = APIRouter()


@router.get("/risk")
async def get_risk():
    data = cache_get("risk")
    if data is None:
        raise_cache_not_ready(
            cache_key="risk",
            message="Risk cache is not ready yet. Run refresh and try again.",
            refresh_mode="light",
        )
    cov = data.get("cov_matrix") if isinstance(data, dict) else {}
    factors = cov.get("factors") if isinstance(cov, dict) else []
    correlation = cov.get("correlation") if isinstance(cov, dict) else []
    matrix = cov.get("matrix") if isinstance(cov, dict) else []
    cov_rows = correlation if isinstance(correlation, list) and correlation else matrix
    risk_engine = data.get("risk_engine") if isinstance(data, dict) else {}
    specific_count = int((risk_engine or {}).get("specific_risk_ticker_count") or 0)
    if (
        not isinstance(factors, list)
        or not factors
        or not isinstance(cov_rows, list)
        or not cov_rows
        or specific_count <= 0
    ):
        raise_cache_not_ready(
            cache_key="risk",
            message="Risk cache exists but is incomplete. Run a core refresh and try again.",
            refresh_mode="full",
        )
    sanity = cache_get("model_sanity") or {"status": "no-data", "warnings": [], "checks": {}}
    return {**data, "model_sanity": sanity, "_cached": True}
