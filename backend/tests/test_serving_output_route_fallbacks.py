from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import exposures as exposures_routes
from backend.api.routes import portfolio as portfolio_routes
from backend.api.routes import risk as risk_routes
from backend.api.routes import universe as universe_routes
from backend import config


def test_portfolio_route_uses_persisted_payload_when_cache_missing(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes, "cache_get", lambda key: None)
    monkeypatch.setattr(
        portfolio_routes,
        "load_current_payload",
        lambda key: {
            "positions": [{"ticker": "AAPL", "weight": 1.0}],
            "total_value": 100.0,
            "position_count": 1,
        }
        if key == "portfolio"
        else None,
    )
    client = TestClient(app)
    res = client.get("/api/portfolio")
    assert res.status_code == 200
    assert res.json()["position_count"] == 1


def test_risk_route_uses_persisted_payload_when_cache_incomplete(monkeypatch) -> None:
    monkeypatch.setattr(config, "SERVING_OUTPUTS_PRIMARY_READS", True)
    monkeypatch.setattr(config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")

    def _fake_cache(key: str):
        if key == "risk":
            return {"cov_matrix": {"factors": ["Beta"]}, "risk_engine": {"specific_risk_ticker_count": 0}}
        return None

    def _fake_payload(key: str):
        if key == "risk":
            return {
                "cov_matrix": {"factors": ["Beta"], "correlation": [[1.0]]},
                "risk_engine": {"specific_risk_ticker_count": 10},
                "risk_shares": {"country": 0.0, "industry": 20.0, "style": 30.0, "idio": 50.0},
                "component_shares": {"country": 0.0, "industry": 0.4, "style": 0.6},
                "factor_details": [],
                "r_squared": 0.4,
                "condition_number": 10.0,
            }
        if key == "model_sanity":
            return {"status": "ok", "warnings": [], "checks": {}}
        return None

    monkeypatch.setattr(risk_routes, "cache_get", _fake_cache)
    monkeypatch.setattr(risk_routes, "load_current_payload", _fake_payload)
    client = TestClient(app)
    res = client.get("/api/risk")
    assert res.status_code == 200
    assert res.json()["risk_engine"]["specific_risk_ticker_count"] == 10


def test_exposures_route_uses_persisted_payload_when_cache_missing(monkeypatch) -> None:
    monkeypatch.setattr(exposures_routes, "cache_get", lambda key: None)
    monkeypatch.setattr(
        exposures_routes,
        "load_current_payload",
        lambda key: {"raw": [{"factor": "Beta", "value": 1.0}], "sensitivity": [], "risk_contribution": []}
        if key == "exposures"
        else None,
    )
    client = TestClient(app)
    res = client.get("/api/exposures?mode=raw")
    assert res.status_code == 200
    assert res.json()["factors"][0]["factor"] == "Beta"


def test_universe_routes_use_persisted_payload_when_cache_missing(monkeypatch) -> None:
    payload = {
        "index": [{"ticker": "JPM", "name": "JPMORGAN CHASE", "ric": "JPM.N"}],
        "by_ticker": {"JPM": {"ticker": "JPM", "ric": "JPM.N", "name": "JPMORGAN CHASE"}},
        "factors": ["Beta"],
        "factor_vols": {"Beta": 0.1},
        "ticker_count": 1,
        "eligible_ticker_count": 1,
    }
    monkeypatch.setattr(universe_routes, "cache_get", lambda key: None)
    monkeypatch.setattr(
        universe_routes,
        "load_current_payload",
        lambda key: payload if key == "universe_loadings" else (payload if key == "universe_factors" else None),
    )
    client = TestClient(app)
    search = client.get("/api/universe/search?q=jpm&limit=20")
    ticker = client.get("/api/universe/ticker/JPM")
    factors = client.get("/api/universe/factors")
    assert search.status_code == 200
    assert ticker.status_code == 200
    assert factors.status_code == 200
    assert search.json()["results"][0]["ric"] == "JPM.N"
    assert ticker.json()["item"]["ticker"] == "JPM"
    assert factors.json()["factors"] == ["Beta"]


def test_portfolio_route_prefers_persisted_payload_in_serving_outputs_mode(monkeypatch) -> None:
    monkeypatch.setattr(config, "SERVING_OUTPUTS_PRIMARY_READS", True)
    monkeypatch.setattr(config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")
    monkeypatch.setattr(
        portfolio_routes,
        "cache_get",
        lambda key: {"positions": [{"ticker": "STALE"}], "position_count": 1, "total_value": 1.0}
        if key == "portfolio"
        else None,
    )
    monkeypatch.setattr(
        portfolio_routes,
        "load_current_payload",
        lambda key: {"positions": [{"ticker": "LIVE"}], "position_count": 1, "total_value": 100.0}
        if key == "portfolio"
        else None,
    )
    client = TestClient(app)
    res = client.get("/api/portfolio")
    assert res.status_code == 200
    assert res.json()["positions"][0]["ticker"] == "LIVE"


def test_risk_route_prefers_persisted_payload_in_serving_outputs_mode(monkeypatch) -> None:
    monkeypatch.setattr(config, "SERVING_OUTPUTS_PRIMARY_READS", True)
    monkeypatch.setattr(config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")

    def _fake_cache(key: str):
        if key == "risk":
            return {
                "cov_matrix": {"factors": ["Beta"]},
                "risk_engine": {"specific_risk_ticker_count": 0},
            }
        if key == "model_sanity":
            return {"status": "warn", "warnings": ["stale"], "checks": {}}
        return None

    def _fake_payload(key: str):
        if key == "risk":
            return {
                "cov_matrix": {"factors": ["Beta"], "correlation": [[1.0]]},
                "risk_engine": {"specific_risk_ticker_count": 10},
                "risk_shares": {"country": 1.0, "industry": 20.0, "style": 30.0, "idio": 49.0},
                "component_shares": {"country": 0.02, "industry": 0.4, "style": 0.58},
                "factor_details": [],
                "r_squared": 0.4,
                "condition_number": 10.0,
            }
        if key == "model_sanity":
            return {"status": "ok", "warnings": [], "checks": {"source": "durable"}}
        return None

    monkeypatch.setattr(risk_routes, "cache_get", _fake_cache)
    monkeypatch.setattr(risk_routes, "load_current_payload", _fake_payload)
    client = TestClient(app)
    res = client.get("/api/risk")
    assert res.status_code == 200
    body = res.json()
    assert body["risk_engine"]["specific_risk_ticker_count"] == 10
    assert body["model_sanity"]["checks"]["source"] == "durable"


def test_portfolio_route_prefers_durable_reads_in_cloud_mode_even_without_flag(monkeypatch) -> None:
    monkeypatch.setattr(config, "SERVING_OUTPUTS_PRIMARY_READS", False)
    monkeypatch.setattr(config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")
    monkeypatch.setattr(
        portfolio_routes,
        "cache_get",
        lambda key: {"positions": [{"ticker": "STALE"}], "position_count": 1, "total_value": 1.0}
        if key == "portfolio"
        else None,
    )
    monkeypatch.setattr(
        portfolio_routes,
        "load_current_payload",
        lambda key: {"positions": [{"ticker": "LIVE"}], "position_count": 1, "total_value": 100.0}
        if key == "portfolio"
        else None,
    )
    client = TestClient(app)
    res = client.get("/api/portfolio")
    assert res.status_code == 200
    assert res.json()["positions"][0]["ticker"] == "LIVE"


def test_portfolio_route_does_not_fallback_to_cache_in_cloud_mode(monkeypatch) -> None:
    monkeypatch.setattr(config, "SERVING_OUTPUTS_PRIMARY_READS", False)
    monkeypatch.setattr(config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")
    monkeypatch.setattr(
        portfolio_routes,
        "cache_get",
        lambda key: {"positions": [{"ticker": "STALE"}], "position_count": 1, "total_value": 1.0}
        if key == "portfolio"
        else None,
    )
    monkeypatch.setattr(portfolio_routes, "load_current_payload", lambda key: None)
    client = TestClient(app)
    res = client.get("/api/portfolio")
    assert res.status_code == 503
