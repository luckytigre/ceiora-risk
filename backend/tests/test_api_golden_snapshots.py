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
            {
                "ticker": "AAPL",
                "weight": 0.6,
                "trbc_sector": "Technology",
                "exposures": {"style_beta_score": 1.1},
                "model_status": "core_estimated",
            },
            {
                "ticker": "JPM",
                "weight": 0.4,
                "trbc_economic_sector_short": "Financials",
                "trbc_economic_sector_short_abbr": "Fins",
                "exposures": {"style_beta_score": 0.9},
                "model_status": "core_estimated",
            },
        ],
        "total_value": 1000.0,
        "position_count": 2,
    }
    monkeypatch.setattr(
        portfolio_routes.dashboard_payload_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: payload if name == "portfolio" else None,
    )
    monkeypatch.setattr(
        portfolio_routes.dashboard_payload_service,
        "cache_get",
        lambda key: payload if key == "portfolio" else None,
    )
    client = TestClient(app)
    res = client.get("/api/portfolio")
    assert res.status_code == 200
    assert res.json() == _golden("api_portfolio.json")


def test_api_risk_matches_golden_snapshot(monkeypatch) -> None:
    risk_payload = {
        "risk_shares": {"market": 0.0, "industry": 28.0, "style": 32.0, "idio": 40.0},
        "component_shares": {"market": 0.0, "industry": 0.467, "style": 0.533},
        "factor_details": [
            {
                "factor_id": "style_beta_score",
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
            "factors": ["style_beta_score", "style_book_to_price_score"],
            "correlation": [[1.0, 0.2], [0.2, 1.0]],
        },
        "factor_catalog": [
            {
                "factor_id": "style_beta_score",
                "factor_name": "Beta",
                "short_label": "Beta",
                "family": "style",
                "block": "core_style",
                "source_column": "beta_score",
                "display_order": 1000,
                "covariance_display": True,
                "exposure_publish": True,
                "active": True,
                "method_version": "v8_use4_us_core_market_one_stage_projected_non_us_2026_03_15",
            },
            {
                "factor_id": "style_book_to_price_score",
                "factor_name": "Book-to-Price",
                "short_label": "Book-to-Price",
                "family": "style",
                "block": "core_style",
                "source_column": "book_to_price_score",
                "display_order": 1001,
                "covariance_display": True,
                "exposure_publish": True,
                "active": True,
                "method_version": "v8_use4_us_core_market_one_stage_projected_non_us_2026_03_15",
            },
        ],
        "risk_engine": {
            "specific_risk_ticker_count": 100,
            "method_version": "v8_use4_us_core_market_one_stage_projected_non_us_2026_03_15",
        },
        "r_squared": 0.41,
    }

    monkeypatch.setattr(
        risk_routes.dashboard_payload_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: risk_payload if name == "risk" else {"status": "ok", "warnings": [], "checks": {}} if name == "model_sanity" else None,
    )
    def _fake_cache_get(key: str):
        return None
    monkeypatch.setattr(risk_routes.dashboard_payload_service, "cache_get", _fake_cache_get)
    client = TestClient(app)
    res = client.get("/api/risk")
    assert res.status_code == 200
    assert res.json() == _golden("api_risk.json")


def test_api_exposures_raw_matches_golden_snapshot(monkeypatch) -> None:
    payload = {
        "raw": [
            {
                "factor_id": "style_beta_score",
                "value": 0.123456,
                "factor_vol": 0.05,
                "coverage_pct": 0.99,
                "drilldown": [{"ticker": "AAPL", "weight": 0.6, "exposure": 1.1, "contribution": 0.66}],
            },
            {
                "factor_id": "industry_technology_equipment",
                "value": 0.2,
                "factor_vol": 0.1,
                "coverage_pct": 0.95,
                "drilldown": [{"ticker": "AAPL", "weight": 0.6, "exposure": 1.0, "contribution": 0.6}],
            },
        ],
        "sensitivity": [],
        "risk_contribution": [],
    }
    monkeypatch.setattr(
        exposures_routes.dashboard_payload_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: payload if name == "exposures" else None,
    )
    monkeypatch.setattr(
        exposures_routes.dashboard_payload_service,
        "cache_get",
        lambda key: payload if key == "exposures" else None,
    )
    client = TestClient(app)
    res = client.get("/api/exposures?mode=raw")
    assert res.status_code == 200
    assert res.json() == _golden("api_exposures_raw.json")


def test_api_universe_factors_matches_golden_snapshot(monkeypatch) -> None:
    payload = {
        "factors": ["industry_technology_equipment", "style_beta_score"],
        "factor_vols": {"industry_technology_equipment": 0.1, "style_beta_score": 0.05},
        "factor_catalog": [
            {
                "factor_id": "industry_technology_equipment",
                "factor_name": "Technology Equipment",
                "short_label": "Technology Equipment",
                "family": "industry",
                "block": "core_structural",
                "source_column": None,
                "display_order": 100,
                "covariance_display": True,
                "exposure_publish": True,
                "active": True,
                "method_version": "v8_use4_us_core_market_one_stage_projected_non_us_2026_03_15",
            },
            {
                "factor_id": "style_beta_score",
                "factor_name": "Beta",
                "short_label": "Beta",
                "family": "style",
                "block": "core_style",
                "source_column": "beta_score",
                "display_order": 1000,
                "covariance_display": True,
                "exposure_publish": True,
                "active": True,
                "method_version": "v8_use4_us_core_market_one_stage_projected_non_us_2026_03_15",
            },
        ],
        "ticker_count": 2,
        "eligible_ticker_count": 2,
        "core_estimated_ticker_count": 2,
        "projected_only_ticker_count": 0,
        "ineligible_ticker_count": 0,
        "factor_count": 2,
    }
    monkeypatch.setattr(
        universe_routes.universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: payload if name == "universe_factors" else None,
    )
    monkeypatch.setattr(
        universe_routes.universe_service,
        "cache_get",
        lambda key: payload if key == "universe_factors" else None,
    )
    client = TestClient(app)
    res = client.get("/api/universe/factors")
    assert res.status_code == 200
    assert res.json() == _golden("api_universe_factors.json")
