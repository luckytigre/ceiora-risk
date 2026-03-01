"""TRBC sector label helpers for compact UI output."""

from __future__ import annotations


_TRBC_SECTOR_ABBR = {
    "BASIC MATERIALS": "Matls",
    "CONSUMER CYCLICALS": "ConsCyc",
    "CONSUMER NON-CYCLICALS": "ConsDef",
    "ENERGY": "Energy",
    "FINANCIALS": "Fins",
    "HEALTHCARE": "Health",
    "INDUSTRIALS": "Inds",
    "REAL ESTATE": "RealEst",
    "TECHNOLOGY": "Tech",
    "TELECOMMUNICATION SERVICES": "Telco",
    "UTILITIES": "Utils",
}


def _normalize_sector(value: str) -> str:
    return " ".join(str(value or "").strip().upper().replace("&", " AND ").split())


def abbreviate_trbc_sector(sector: str | None) -> str:
    """Return a compact TRBC sector label for dense outputs."""
    raw = str(sector or "").strip()
    if not raw:
        return ""
    key = _normalize_sector(raw)
    abbr = _TRBC_SECTOR_ABBR.get(key)
    if abbr:
        return abbr
    tokens = [t for t in raw.replace("&", " ").split() if t]
    if not tokens:
        return raw[:8]
    if len(tokens) == 1:
        return tokens[0][:8]
    compact = "".join(t[0].upper() + t[1:3] for t in tokens)
    return compact[:8]
