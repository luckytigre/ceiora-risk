"""Minimal yfinance sector/industry to GICS industry-group mapping helpers."""

from __future__ import annotations

import re
from typing import Mapping

DEFAULT_SECTOR_TO_GROUP: dict[str, str] = {
    "Technology": "Software & Services",
    "Financial Services": "Diversified Financials",
    "Healthcare": "Health Care Equipment & Services",
    "Consumer Cyclical": "Consumer Discretionary Distribution",
    "Consumer Defensive": "Consumer Staples Distribution",
    "Industrials": "Capital Goods",
    "Energy": "Energy",
    "Utilities": "Utilities",
    "Real Estate": "Equity REITs",
    "Communication Services": "Media & Entertainment",
    "Basic Materials": "Materials",
}

_SECTOR_ALIASES: dict[str, str] = {
    "health care": "Healthcare",
    "financials": "Financial Services",
    "consumer discretionary": "Consumer Cyclical",
    "consumer staples": "Consumer Defensive",
    "materials": "Basic Materials",
    "communication": "Communication Services",
}

_INDUSTRY_KEYWORD_TO_GROUP: list[tuple[tuple[str, ...], str]] = [
    (("semiconductor", "semiconductors"), "Semiconductors & Semiconductor Equipment"),
    (("software",), "Software & Services"),
    (("internet", "interactive media"), "Media & Entertainment"),
    (("bank", "banks"), "Banks"),
    (("insurance",), "Insurance"),
    (("reit", "real estate"), "Equity REITs"),
    (("pharma", "biotech", "biotechnology", "healthcare", "medical"), "Health Care Equipment & Services"),
    (("oil", "gas", "energy", "upstream", "midstream"), "Energy"),
    (("metals", "mining", "steel", "chemicals"), "Materials"),
    (("airline", "rail", "shipping", "transport"), "Transportation"),
    (("utility", "utilities"), "Utilities"),
    (("retail", "consumer", "apparel"), "Consumer Discretionary Distribution"),
]


def _clean_text(value: str | None) -> str:
    text = str(value).strip() if value is not None else ""
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def _normalize_key(value: str) -> str:
    return " ".join(value.lower().strip().split())


def _map_by_ticker_pattern(ticker: str) -> tuple[str, str] | None:
    t = str(ticker or "").strip().upper()
    if not t:
        return None
    if t.endswith("-USD"):
        return ("Digital Assets", "ticker_crypto_pair")
    if t.endswith("=F"):
        return ("Commodity Derivatives", "ticker_future_contract")
    if t.endswith("=X"):
        return ("FX Derivatives", "ticker_fx_pair")
    if re.search(r"\.[A-Z]$", t):
        return ("Diversified Financials", "ticker_share_class")
    return None


def map_to_industry_group_with_reason(
    *,
    sector: str | None,
    industry: str | None,
    ticker: str | None = None,
    explicit_mapping: Mapping[tuple[str, str], str] | None = None,
) -> tuple[str, str]:
    """Map raw labels to an industry group and return (group, reason)."""
    s = _clean_text(sector)
    i = _clean_text(industry)

    if explicit_mapping:
        mapped = explicit_mapping.get((s, i))
        if mapped:
            return mapped, "explicit_exact"
        mapped_norm = explicit_mapping.get((_normalize_key(s), _normalize_key(i)))
        if mapped_norm:
            return mapped_norm, "explicit_normalized"

    norm_industry = _normalize_key(i)
    if norm_industry:
        for keywords, group in _INDUSTRY_KEYWORD_TO_GROUP:
            if any(k in norm_industry for k in keywords):
                return group, "industry_keyword"

    norm_sector = _normalize_key(s)
    if norm_sector:
        sector_key = _SECTOR_ALIASES.get(norm_sector, s)
        if sector_key in DEFAULT_SECTOR_TO_GROUP:
            return DEFAULT_SECTOR_TO_GROUP[sector_key], "sector_map"

    ticker_mapped = _map_by_ticker_pattern(str(ticker or ""))
    if ticker_mapped is not None:
        return ticker_mapped

    return "Unmapped", "unmapped"


def map_to_industry_group(
    *,
    sector: str | None,
    industry: str | None,
    ticker: str | None = None,
    explicit_mapping: Mapping[tuple[str, str], str] | None = None,
) -> str:
    """Map labels to an industry-group name."""
    group, _reason = map_to_industry_group_with_reason(
        sector=sector,
        industry=industry,
        ticker=ticker,
        explicit_mapping=explicit_mapping,
    )
    return group
