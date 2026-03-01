"""Hard-coded 36-position mock portfolio (mixed long/short)."""

from __future__ import annotations

# ticker → shares held
MOCK_POSITIONS: dict[str, float] = {
    # Mega-cap tech
    "AAPL": 6.75,
    "MSFT": -46.836,
    "NVDA": 142.33,
    "GOOGL": 179.6,
    "AMZN": 120.32,
    "META": 17.8,
    "AVGO": 133.22,
    "TSLA": -36.05,
    # Financials
    "JPM": 28.88,
    "V": -69.68,
    "MA": 83.38,
    "BAC": 241.5,
    "GS": -15.62,
    # Healthcare
    "UNH": 91.82,
    "JNJ": -141.52,
    "LLY": 38.03,
    "PFE": -1063.37,
    "ABBV": -27.36,
    # Industrials
    "CAT": -26.06,
    "HON": 173.33,
    "UNP": 157.05,
    "GE": 145.5,
    # Consumer
    "PG": 193.11,
    "KO": -596.73,
    "PEP": 157.61,
    "COST": 5.01,
    "WMT": 307.52,
    "HD": -123.03,
    # Energy
    "XOM": -321.76,
    "CVX": 64.06,
    # Materials
    "LIN": 105.99,
    "APD": 55.99,
    # Utilities
    "NEE": 606.04,
    "DUK": -12.66,
    # Communication
    "DIS": -105.69,
    # Real Estate
    "AMT": -33.1,
}


def get_tickers() -> list[str]:
    return list(MOCK_POSITIONS.keys())


def get_shares() -> dict[str, float]:
    return dict(MOCK_POSITIONS)


DEFAULT_ACCOUNT = "MAIN"
DEFAULT_SLEEVE = "CORE EQUITY"
DEFAULT_SOURCE = "MOCK_PORTFOLIO"

# Optional per-ticker overrides. Keep sparse and fall back to defaults.
MOCK_POSITION_META: dict[str, dict[str, str]] = {
    "AAPL": {"account": "MAIN", "sleeve": "CORE EQUITY", "source": "CSV_UPLOAD"},
    "MSFT": {"account": "MAIN", "sleeve": "CORE EQUITY", "source": "CSV_UPLOAD"},
    "NVDA": {"account": "MAIN", "sleeve": "CORE EQUITY", "source": "CSV_UPLOAD"},
    "GOOGL": {"account": "MAIN", "sleeve": "CORE EQUITY", "source": "CSV_UPLOAD"},
    "AMZN": {"account": "MAIN", "sleeve": "CORE EQUITY", "source": "CSV_UPLOAD"},
}


def get_position_meta(ticker: str) -> dict[str, str]:
    t = ticker.upper().strip()
    base = MOCK_POSITION_META.get(t, {})
    return {
        "account": str(base.get("account") or DEFAULT_ACCOUNT),
        "sleeve": str(base.get("sleeve") or DEFAULT_SLEEVE),
        "source": str(base.get("source") or DEFAULT_SOURCE),
    }
