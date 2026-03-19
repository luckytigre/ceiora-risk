"""Read-only cPAR meta/search/ticker/hedge routes."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from backend.services import (
    cpar_hedge_service,
    cpar_meta_service,
    cpar_portfolio_hedge_service,
    cpar_search_service,
    cpar_ticker_service,
)

router = APIRouter()


def _raise_cpar_not_ready(message: str) -> None:
    raise HTTPException(
        status_code=503,
        detail={
            "status": "not_ready",
            "error": "cpar_not_ready",
            "message": str(message),
            "build_profile": "cpar-weekly",
        },
    )


def _raise_cpar_unavailable(message: str) -> None:
    raise HTTPException(
        status_code=503,
        detail={
            "status": "unavailable",
            "error": "cpar_authority_unavailable",
            "message": str(message),
        },
    )


@router.get("/cpar/meta")
async def get_cpar_meta():
    try:
        return cpar_meta_service.load_cpar_meta_payload()
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))


@router.get("/cpar/search")
async def search_cpar(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=200),
):
    try:
        return cpar_search_service.load_cpar_search_payload(q=q, limit=int(limit))
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))


@router.get("/cpar/ticker/{ticker}")
async def get_cpar_ticker(
    ticker: str,
    ric: str | None = Query(default=None),
):
    try:
        return cpar_ticker_service.load_cpar_ticker_payload(ticker=ticker, ric=ric)
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))
    except cpar_meta_service.CparTickerAmbiguous as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except cpar_meta_service.CparTickerNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/cpar/ticker/{ticker}/hedge")
async def get_cpar_hedge(
    ticker: str,
    mode: Literal["factor_neutral", "market_neutral"] = Query(default="factor_neutral"),
    ric: str | None = Query(default=None),
):
    try:
        return cpar_hedge_service.load_cpar_hedge_payload(ticker=ticker, ric=ric, mode=str(mode))
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))
    except cpar_meta_service.CparTickerAmbiguous as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except cpar_meta_service.CparTickerNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/cpar/portfolio/hedge")
async def get_cpar_portfolio_hedge(
    account_id: str = Query(..., min_length=1),
    mode: Literal["factor_neutral", "market_neutral"] = Query(default="factor_neutral"),
):
    try:
        return cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
            account_id=account_id,
            mode=str(mode),
        )
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))
    except cpar_portfolio_hedge_service.CparPortfolioAccountNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
