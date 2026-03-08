"""Pydantic models for portfolio data."""

from __future__ import annotations

from pydantic import BaseModel


class Position(BaseModel):
    ticker: str
    name: str = ""
    long_short: str = "LONG"
    shares: float
    trbc_economic_sector_short: str = ""
    trbc_economic_sector_short_abbr: str = ""
    account: str = ""
    sleeve: str = ""
    source: str = ""
    trbc_industry_group: str = ""
    price: float = 0.0
    market_value: float = 0.0
    weight: float = 0.0
    exposures: dict[str, float] = {}
    risk_contrib_pct: float = 0.0
    risk_mix: dict[str, float] = {"country": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}


class Portfolio(BaseModel):
    positions: list[Position] = []
    total_value: float = 0.0
    position_count: int = 0
