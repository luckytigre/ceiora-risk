from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import exposures as exposures_routes
from backend.api.routes import portfolio as portfolio_routes
from backend.api.routes import risk as risk_routes
from backend.api.routes import universe as universe_routes


GOLDEN_DIR = Path(__file__).resolve().parent / "golden"


def _golden(name: str) -> dict:
    return json.loads((GOLDEN_DIR / name).read_text(encoding="utf-8"))


def test_api_portfolio_matches_golden_snapshot(monkeypatch) -> None:
    payload = {
        "positions": [
            {"ticker": "AAPL", "weight": 0.6, "trbc_sector": "Technology", "exposures": {"Beta": 1.1}},
            {
                "ticker": "JPM",
                "weight": 0.4,
                "trbc_economic_sector_short": "Financials",
                "trbc_economic_sector_short_abbr": "Fins",
                "exposures": {"Beta": 0.9},
            },
        ],
        "total_value": 1000.0,
        "position_count": 2,
    }
    monkeypatch.setattr(portfolio_routes, "cache_get", lambda key: payload if key == "portfolio" else None)
    client = TestClient(app)
    res = client.get("/api/portfolio")
    assert res.status_code == 200
    assert res.json() == _golden("api_portfolio.json")


def test_api_risk_matches_golden_snapshot(monkeypatch) -> None:
    risk_payload = {
        "risk_shares": {"country": 0.0, "industry": 28.0, "style": 32.0, "idio": 40.0},
        "component_shares": {"country": 0.0, "industry": 0.467, "style": 0.533},
        "factor_details": [
            {
                "factor": "Beta",
                "category": "style",
                "exposure": 0.13,
                "factor_vol": 0.05,
                "sensitivity": 0.0065,
                "marginal_var_contrib": 0.0002,
                "pct_of_total": 3.4,
                "pct_of_systematic": 5.7,
            }
        ],
        "cov_matrix": {
            "factors": ["Beta", "Value"],
            "correlation": [[1.0, 0.2], [0.2, 1.0]],
        },
        "risk_engine": {
            "specific_risk_ticker_count": 100,
            "method_version": "v2_trbc_l2_business_sector_2026_03_05",
        },
        "r_squared": 0.41,
        "condition_number": 1100.0,
    }

    def _fake_cache_get(key: str):
        if key == "risk":
            return risk_payload
        if key == "model_sanity":
            return {"status": "ok", "warnings": [], "checks": {}}
        return None

    monkeypatch.setattr(risk_routes, "cache_get", _fake_cache_get)
    client = TestClient(app)
    res = client.get("/api/risk")
    assert res.status_code == 200
    assert res.json() == _golden("api_risk.json")


def test_api_exposures_raw_matches_golden_snapshot(monkeypatch) -> None:
    payload = {
        "raw": [
            {
                "factor": "Beta",
                "value": 0.123456,
                "factor_vol": 0.05,
                "coverage_pct": 0.99,
                "drilldown": [{"ticker": "AAPL", "weight": 0.6, "exposure": 1.1, "contribution": 0.66}],
            },
            {
                "factor": "Technology Equipment",
                "value": 0.2,
                "factor_vol": 0.1,
                "coverage_pct": 0.95,
                "drilldown": [{"ticker": "AAPL", "weight": 0.6, "exposure": 1.0, "contribution": 0.6}],
            },
        ],
        "sensitivity": [],
        "risk_contribution": [],
    }
    monkeypatch.setattr(exposures_routes, "cache_get", lambda key: payload if key == "exposures" else None)
    client = TestClient(app)
    res = client.get("/api/exposures?mode=raw")
    assert res.status_code == 200
    assert res.json() == _golden("api_exposures_raw.json")


def test_api_universe_factors_matches_golden_snapshot(monkeypatch) -> None:
    payload = {
        "factors": ["Beta", "Technology Equipment"],
        "factor_vols": {"Beta": 0.05, "Technology Equipment": 0.1},
        "ticker_count": 2,
        "eligible_ticker_count": 2,
        "factor_count": 2,
    }
    monkeypatch.setattr(universe_routes, "cache_get", lambda key: payload if key == "universe_factors" else None)
    client = TestClient(app)
    res = client.get("/api/universe/factors")
    assert res.status_code == 200
    assert res.json() == _golden("api_universe_factors.json")
