"""POST /api/refresh — re-query Postgres, recompute analytics, update SQLite cache."""

from fastapi import APIRouter
from analytics.pipeline import run_refresh

router = APIRouter()


@router.post("/refresh")
async def refresh():
    result = run_refresh()
    return result
