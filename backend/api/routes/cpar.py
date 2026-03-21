"""Read-only and preview cPAR routes."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, FiniteFloat

from backend.services import (
    cpar_explore_whatif_service,
    cpar_factor_history_service,
    cpar_meta_service,
    cpar_portfolio_hedge_service,
    cpar_portfolio_whatif_service,
    cpar_risk_service,
    cpar_search_service,
    cpar_ticker_history_service,
    cpar_ticker_service,
)

router = APIRouter()
MAX_CPAR_WHATIF_SCENARIO_ROWS = cpar_portfolio_whatif_service.MAX_CPAR_WHATIF_ROWS
MAX_CPAR_EXPLORE_WHATIF_ROWS = cpar_explore_whatif_service.MAX_CPAR_EXPLORE_WHATIF_ROWS


class CparWhatIfScenarioRow(BaseModel):
    ric: str
    quantity_delta: FiniteFloat
    ticker: str | None = None


class CparPortfolioWhatIfRequest(BaseModel):
    account_id: str
    mode: Literal["factor_neutral", "market_neutral"] = "factor_neutral"
    scenario_rows: list[CparWhatIfScenarioRow] = Field(default_factory=list)


class CparExploreWhatIfScenarioRow(BaseModel):
    account_id: str
    quantity: FiniteFloat
    ticker: str | None = None
    ric: str
    source: str | None = None


class CparExploreWhatIfRequest(BaseModel):
    scenario_rows: list[CparExploreWhatIfScenarioRow] = Field(default_factory=list)


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
        return cpar_ticker_service.load_cpar_ticker_payload(
            ticker=ticker,
            ric=ric,
        )
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))
    except cpar_ticker_service.CparTickerNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/cpar/ticker/{ticker}/history")
async def get_cpar_ticker_history(
    ticker: str,
    ric: str | None = Query(default=None),
    years: int = Query(5, ge=1, le=20),
):
    try:
        return cpar_ticker_history_service.load_cpar_ticker_history_payload(
            ticker=ticker,
            ric=ric,
            years=int(years),
        )
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))
    except cpar_ticker_service.CparTickerNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/cpar/risk")
async def get_cpar_risk():
    try:
        return cpar_risk_service.load_cpar_risk_payload()
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))


@router.get("/cpar/factors/history")
async def get_cpar_factor_history(
    factor_id: str = Query(..., min_length=1),
    years: int = Query(5, ge=1, le=10),
):
    try:
        return cpar_factor_history_service.load_cpar_factor_history_payload(
            factor_id=factor_id,
            years=int(years),
        )
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))
    except cpar_factor_history_service.CparFactorNotFound as exc:
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


@router.post("/cpar/portfolio/whatif")
async def post_cpar_portfolio_whatif(
    payload: CparPortfolioWhatIfRequest,
):
    scenario_rows = [dict(row) for row in payload.model_dump().get("scenario_rows", [])]
    if len(scenario_rows) > MAX_CPAR_WHATIF_SCENARIO_ROWS:
        raise HTTPException(
            status_code=400,
            detail=f"Too many cPAR what-if rows. Max {MAX_CPAR_WHATIF_SCENARIO_ROWS}.",
        )
    try:
        return cpar_portfolio_whatif_service.load_cpar_portfolio_whatif_payload(
            account_id=payload.account_id,
            mode=str(payload.mode),
            scenario_rows=scenario_rows,
        )
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))
    except cpar_portfolio_hedge_service.CparPortfolioAccountNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/cpar/explore/whatif")
async def post_cpar_explore_whatif(
    payload: CparExploreWhatIfRequest,
):
    scenario_rows = [dict(row) for row in payload.model_dump().get("scenario_rows", [])]
    if len(scenario_rows) > MAX_CPAR_EXPLORE_WHATIF_ROWS:
        raise HTTPException(
            status_code=400,
            detail=f"Too many cPAR explore what-if rows. Max {MAX_CPAR_EXPLORE_WHATIF_ROWS}.",
        )
    try:
        return cpar_explore_whatif_service.load_cpar_explore_whatif_payload(
            scenario_rows=scenario_rows,
        )
    except cpar_meta_service.CparReadNotReady as exc:
        _raise_cpar_not_ready(str(exc))
    except cpar_meta_service.CparReadUnavailable as exc:
        _raise_cpar_unavailable(str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
