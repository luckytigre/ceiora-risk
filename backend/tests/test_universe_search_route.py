from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api.routes import universe as universe_routes
from backend.main import app


def test_universe_search_uses_index_ric_when_present(monkeypatch) -> None:
    payload = {
        "index": [
            {"ticker": "JPM", "name": "JPMORGAN CHASE", "ric": "JPM.N"},
        ],
        "by_ticker": {
            "JPM": {"ticker": "JPM", "ric": "JPM.N"},
        },
    }
    monkeypatch.setattr(
        universe_routes.universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: payload if name == "universe_loadings" else None,
    )
    monkeypatch.setattr(universe_routes.universe_service, "cache_get", lambda key: None)

    client = TestClient(app)
    res = client.get("/api/universe/search?q=jpm&limit=20")
    assert res.status_code == 200
    body = res.json()
    assert body["total"] == 1
    assert body["results"][0]["ticker"] == "JPM"
    assert body["results"][0]["ric"] == "JPM.N"


def test_universe_search_fills_ric_from_by_ticker_when_index_missing(monkeypatch) -> None:
    payload = {
        "index": [
            {"ticker": "WMT", "name": "WALMART INC"},
        ],
        "by_ticker": {
            "WMT": {"ticker": "WMT", "ric": "WMT.N"},
        },
    }
    monkeypatch.setattr(
        universe_routes.universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: payload if name == "universe_loadings" else None,
    )
    monkeypatch.setattr(universe_routes.universe_service, "cache_get", lambda key: None)

    client = TestClient(app)
    res = client.get("/api/universe/search?q=wmt&limit=20")
    assert res.status_code == 200
    body = res.json()
    assert body["total"] == 1
    assert body["results"][0]["ticker"] == "WMT"
    assert body["results"][0]["ric"] == "WMT.N"
