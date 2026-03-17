"""Universe-level cached loadings/search endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Query

from backend.api.routes.presenters import normalize_trbc_sector_fields
from backend.api.routes.readiness import raise_cache_not_ready
from backend.services import universe_service

router = APIRouter()


@router.get("/universe/ticker/{ticker}")
async def get_universe_ticker(ticker: str):
    try:
        return universe_service.load_universe_ticker_payload(
            ticker,
            row_normalizer=normalize_trbc_sector_fields,
        )
    except universe_service.UniversePayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )


@router.get("/universe/ticker/{ticker}/history")
def get_universe_ticker_history(
    ticker: str,
    years: int = Query(5, ge=1, le=20),
):
    try:
        return universe_service.load_universe_ticker_history_payload(
            ticker,
            years=int(years),
        )
    except universe_service.UniversePayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )


@router.get("/universe/search")
async def search_universe(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=200),
):
    try:
        return universe_service.search_universe_payload(
            q=q,
            limit=int(limit),
            row_normalizer=normalize_trbc_sector_fields,
        )
    except universe_service.UniversePayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )


@router.get("/universe/factors")
async def get_universe_factors():
    try:
        data = universe_service.load_universe_factors_payload()
    except universe_service.UniversePayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
    return {**data, "_cached": True}
