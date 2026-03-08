from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import risk as risk_routes


def test_risk_route_accepts_correlation_cov_matrix(monkeypatch) -> None:
    payload = {
        "cov_matrix": {
            "factors": ["Beta", "Value"],
            "correlation": [[1.0, 0.2], [0.2, 1.0]],
        },
        "risk_engine": {
            "specific_risk_ticker_count": 100,
        },
        "risk_shares": {"country": 0.0, "industry": 60.0, "style": 30.0, "idio": 10.0},
        "component_shares": {"country": 0.0, "industry": 0.67, "style": 0.33},
        "factor_details": [],
        "r_squared": 0.4,
        "condition_number": 1200.0,
    }

    def fake_cache_get(key: str):
        if key == "risk":
            return payload
        if key == "model_sanity":
            return {"status": "ok", "warnings": [], "checks": {}}
        return None

    monkeypatch.setattr(risk_routes, "cache_get", fake_cache_get)
    client = TestClient(app)
    res = client.get("/api/risk")
    assert res.status_code == 200
    body = res.json()
    assert body.get("_cached") is True
    assert body["cov_matrix"]["factors"] == ["Beta", "Value"]


def test_risk_route_rejects_missing_cov_rows(monkeypatch) -> None:
    payload = {
        "cov_matrix": {"factors": ["Beta"]},
        "risk_engine": {"specific_risk_ticker_count": 100},
    }

    monkeypatch.setattr(risk_routes, "cache_get", lambda key: payload if key == "risk" else None)
    client = TestClient(app)
    res = client.get("/api/risk")
    assert res.status_code == 503
    detail = res.json().get("detail") or {}
    assert detail.get("cache_key") == "risk"
