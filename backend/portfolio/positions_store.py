"""Portfolio position store used by analytics projection.

Holdings are authoritative in Neon when a DSN is configured. The in-code mock
portfolio is only a bootstrap fallback for local development without Neon.
"""

from __future__ import annotations

from collections import defaultdict

from backend import config
from backend.data.neon import connect, resolve_dsn

# ticker -> shares held
PORTFOLIO_POSITIONS: dict[str, float] = {
    "AAPL": 6.75,
    "MSFT": -46.836,
    "NVDA": 142.33,
    "GOOGL": 179.6,
    "AMZN": 120.32,
    "META": 17.8,
    "AVGO": 133.22,
    "TSLA": -36.05,
    "JPM": 28.88,
    "V": -69.68,
    "MA": 83.38,
    "BAC": 241.5,
    "GS": -15.62,
    "UNH": 91.82,
    "JNJ": -141.52,
    "LLY": 38.03,
    "PFE": -1063.37,
    "ABBV": -27.36,
    "CAT": -26.06,
    "HON": 173.33,
    "UNP": 157.05,
    "GE": 145.5,
    "PG": 193.11,
    "KO": -596.73,
    "PEP": 157.61,
    "COST": 5.01,
    "WMT": 307.52,
    "HD": -123.03,
    "XOM": -321.76,
    "CVX": 64.06,
    "LIN": 105.99,
    "APD": 55.99,
    "NEE": 606.04,
    "DUK": -12.66,
    "DIS": -105.69,
    "AMT": -33.1,
}


def get_tickers() -> list[str]:
    shares, _ = _load_positions()
    return list(shares.keys())


def get_shares() -> dict[str, float]:
    shares, _ = _load_positions()
    return dict(shares)


DEFAULT_ACCOUNT = "MAIN"
DEFAULT_SLEEVE = "CORE EQUITY"
DEFAULT_SOURCE = "PORTFOLIO_STORE"

POSITION_META: dict[str, dict[str, str]] = {
    "AAPL": {"account": "MAIN", "sleeve": "CORE EQUITY", "source": "CSV_UPLOAD"},
    "MSFT": {"account": "MAIN", "sleeve": "CORE EQUITY", "source": "CSV_UPLOAD"},
    "NVDA": {"account": "MAIN", "sleeve": "CORE EQUITY", "source": "CSV_UPLOAD"},
    "GOOGL": {"account": "MAIN", "sleeve": "CORE EQUITY", "source": "CSV_UPLOAD"},
    "AMZN": {"account": "MAIN", "sleeve": "CORE EQUITY", "source": "CSV_UPLOAD"},
}


def get_position_meta(ticker: str) -> dict[str, str]:
    t = ticker.upper().strip()
    _, dynamic_meta = _load_positions()
    base = dynamic_meta.get(t) or POSITION_META.get(t, {})
    return {
        "account": str(base.get("account") or DEFAULT_ACCOUNT),
        "sleeve": str(base.get("sleeve") or DEFAULT_SLEEVE),
        "source": str(base.get("source") or DEFAULT_SOURCE),
    }


def _load_positions() -> tuple[dict[str, float], dict[str, dict[str, str]]]:
    try:
        return _load_positions_from_neon()
    except Exception:
        if str(config.DATA_BACKEND).strip().lower() == "neon" or str(config.NEON_DATABASE_URL).strip():
            # In Neon-backed runtime, fail closed to avoid mixing stale mock data
            # with live holdings writes.
            return {}, {}
        return dict(PORTFOLIO_POSITIONS), dict(POSITION_META)


def _load_positions_from_neon() -> tuple[dict[str, float], dict[str, dict[str, str]]]:
    dsn = resolve_dsn(None)
    conn = connect(dsn=dsn, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    UPPER(COALESCE(ticker, '')) AS ticker,
                    LOWER(account_id) AS account_id,
                    CAST(quantity AS DOUBLE PRECISION) AS quantity,
                    COALESCE(source, '') AS source
                FROM holdings_positions_current
                WHERE quantity <> 0
                  AND COALESCE(ticker, '') <> ''
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    qty_by_ticker: dict[str, float] = defaultdict(float)
    accounts_by_ticker: dict[str, set[str]] = defaultdict(set)
    source_by_ticker: dict[str, str] = {}
    for ticker_raw, account_id_raw, qty_raw, source_raw in rows:
        ticker = str(ticker_raw or "").upper().strip()
        if not ticker:
            continue
        qty = float(qty_raw or 0.0)
        if abs(qty) <= 0.0:
            continue
        qty_by_ticker[ticker] += qty
        account_id = str(account_id_raw or "").strip().lower()
        if account_id:
            accounts_by_ticker[ticker].add(account_id)
        source_txt = str(source_raw or "").strip().upper()
        if source_txt:
            source_by_ticker[ticker] = source_txt

    shares = {t: float(q) for t, q in qty_by_ticker.items() if abs(float(q)) > 0.0}
    meta: dict[str, dict[str, str]] = {}
    for ticker in shares:
        accounts = sorted(accounts_by_ticker.get(ticker) or [])
        account = accounts[0] if len(accounts) == 1 else ("multi" if len(accounts) > 1 else DEFAULT_ACCOUNT.lower())
        meta[ticker] = {
            "account": account.upper(),
            "sleeve": "NEON HOLDINGS",
            "source": source_by_ticker.get(ticker) or "NEON_HOLDINGS",
        }
    return shares, meta
