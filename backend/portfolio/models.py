"""Pydantic models for portfolio data."""

from __future__ import annotations

from pydantic import BaseModel


class Position(BaseModel):
    ticker: str
    name: str = ""
    long_short: str = "LONG"
    shares: float
    gics_sector: str = ""
    sector: str = ""
    account: str = ""
    sleeve: str = ""
    source: str = ""
    industry_group: str = ""
    price: float = 0.0
    market_value: float = 0.0
    weight: float = 0.0
    exposures: dict[str, float] = {}
    risk_contrib_pct: float = 0.0


class Portfolio(BaseModel):
    positions: list[Position] = []
    total_value: float = 0.0
    position_count: int = 0
