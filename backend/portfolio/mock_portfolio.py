"""Hard-coded 36-position mock portfolio (mixed long/short)."""

from __future__ import annotations

# ticker → shares held
MOCK_POSITIONS: dict[str, float] = {
    # Mega-cap tech
    "AAPL": 500,
    "MSFT": 400,
    "NVDA": 300,
    "GOOGL": 200,
    "AMZN": 250,
    "META": 150,
    "AVGO": 100,
    "TSLA": -120,
    # Financials
    "JPM": 300,
    "V": -200,
    "MA": 150,
    "BAC": 500,
    "GS": -80,
    # Healthcare
    "UNH": 100,
    "JNJ": -250,
    "LLY": 80,
    "PFE": -600,
    "ABBV": -200,
    # Industrials
    "CAT": -120,
    "HON": 150,
    "UNP": 100,
    "GE": 300,
    # Consumer
    "PG": 300,
    "KO": -400,
    "PEP": 200,
    "COST": 60,
    "WMT": 250,
    "HD": -100,
    # Energy
    "XOM": -350,
    "CVX": 200,
    # Materials
    "LIN": 100,
    "APD": 80,
    # Utilities
    "NEE": 200,
    "DUK": -150,
    # Communication
    "DIS": -250,
    # Real Estate
    "AMT": -100,
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
