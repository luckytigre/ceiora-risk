from __future__ import annotations

import ast
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.api.routes import cpar as cpar_routes


def _test_app() -> FastAPI:
    app = FastAPI()
    app.include_router(cpar_routes.router, prefix="/api")
    return app


def test_cpar_meta_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_meta_service,
        "load_cpar_meta_payload",
        lambda: {"package_run_id": "run_curr", "package_date": "2026-03-14", "factor_count": 17, "factors": []},
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/meta")

    assert res.status_code == 200
    assert res.json()["package_run_id"] == "run_curr"


def test_cpar_meta_route_returns_not_ready_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_meta_service,
        "load_cpar_meta_payload",
        lambda: (_ for _ in ()).throw(cpar_routes.cpar_meta_service.CparReadNotReady("No successful cPAR package")),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/meta")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "not_ready"
    assert res.json()["detail"]["build_profile"] == "cpar-weekly"


def test_cpar_meta_route_returns_unavailable_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_meta_service,
        "load_cpar_meta_payload",
        lambda: (_ for _ in ()).throw(cpar_routes.cpar_meta_service.CparReadUnavailable("Neon cPAR read failed")),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/meta")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "unavailable"
    assert res.json()["detail"]["error"] == "cpar_authority_unavailable"


def test_cpar_search_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_search_service,
        "load_cpar_search_payload",
        lambda **kwargs: {
            "query": kwargs["q"],
            "limit": kwargs["limit"],
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "results": [{"ticker": "AAPL", "ric": "AAPL.OQ"}],
            "total": 1,
        },
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/search?q=aapl&limit=10")

    assert res.status_code == 200
    assert res.json()["results"][0]["ric"] == "AAPL.OQ"


def test_cpar_search_route_preserves_null_ticker_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_search_service,
        "load_cpar_search_payload",
        lambda **kwargs: {
            "query": kwargs["q"],
            "limit": kwargs["limit"],
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "results": [{"ticker": None, "ric": "AAPL.NA", "display_name": "Apple Inc. Synthetic Line"}],
            "total": 1,
        },
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/search?q=aapl&limit=10")

    assert res.status_code == 200
    assert res.json()["results"][0]["ticker"] is None
    assert res.json()["results"][0]["ric"] == "AAPL.NA"


def test_cpar_search_route_maps_not_ready_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_search_service,
        "load_cpar_search_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparReadNotReady("No successful cPAR package")
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/search?q=aapl&limit=10")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "not_ready"
    assert res.json()["detail"]["error"] == "cpar_not_ready"


def test_cpar_search_route_maps_unavailable_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_search_service,
        "load_cpar_search_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparReadUnavailable("Neon cPAR read failed")
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/search?q=aapl&limit=10")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "unavailable"
    assert res.json()["detail"]["error"] == "cpar_authority_unavailable"


def test_cpar_ticker_route_maps_ambiguous_ticker_to_409(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_ticker_service,
        "load_cpar_ticker_payload",
        lambda *args, **kwargs: (_ for _ in ()).throw(cpar_routes.cpar_meta_service.CparTickerAmbiguous("Ambiguous cPAR instrument fit for ticker AAPL")),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL")

    assert res.status_code == 409
    assert "Ambiguous" in res.json()["detail"]


def test_cpar_ticker_route_maps_missing_ticker_to_404(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_ticker_service,
        "load_cpar_ticker_payload",
        lambda *args, **kwargs: (_ for _ in ()).throw(cpar_routes.cpar_meta_service.CparTickerNotFound("Ticker AAPL was not found")),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL")

    assert res.status_code == 404
    assert "not found" in res.json()["detail"].lower()


def test_cpar_ticker_route_maps_unavailable_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_ticker_service,
        "load_cpar_ticker_payload",
        lambda *args, **kwargs: (_ for _ in ()).throw(cpar_routes.cpar_meta_service.CparReadUnavailable("Neon cPAR read failed")),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "unavailable"
    assert res.json()["detail"]["error"] == "cpar_authority_unavailable"


def test_cpar_hedge_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_hedge_service,
        "load_cpar_hedge_payload",
        lambda **kwargs: {
            "mode": kwargs["mode"],
            "hedge_status": "hedge_ok",
            "hedge_legs": [{"factor_id": "SPY", "weight": -1.2}],
            "post_hedge_exposures": [{"factor_id": "SPY", "pre_beta": 1.2, "hedge_leg": -1.2, "post_beta": 0.0}],
        },
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL/hedge?mode=factor_neutral&ric=AAPL.OQ")

    assert res.status_code == 200
    assert res.json()["mode"] == "factor_neutral"
    assert res.json()["hedge_legs"][0]["factor_id"] == "SPY"


def test_cpar_hedge_route_maps_unavailable_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_hedge_service,
        "load_cpar_hedge_payload",
        lambda **kwargs: (_ for _ in ()).throw(cpar_routes.cpar_meta_service.CparReadUnavailable("Neon cPAR read failed")),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL/hedge?mode=market_neutral")

    assert res.status_code == 503
    assert res.json()["detail"]["error"] == "cpar_authority_unavailable"


def test_cpar_hedge_route_maps_not_ready_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_hedge_service,
        "load_cpar_hedge_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparReadNotReady(
                "Active cPAR package is missing covariance rows in the cloud-serve authority store."
            )
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL/hedge?mode=market_neutral")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "not_ready"
    assert res.json()["detail"]["error"] == "cpar_not_ready"


def test_cpar_hedge_route_maps_ambiguous_ticker_to_409(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_hedge_service,
        "load_cpar_hedge_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparTickerAmbiguous("Ambiguous cPAR instrument fit for ticker AAPL")
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL/hedge?mode=factor_neutral")

    assert res.status_code == 409
    assert "Ambiguous" in res.json()["detail"]


def test_cpar_hedge_route_maps_missing_ticker_to_404(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_hedge_service,
        "load_cpar_hedge_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparTickerNotFound("Ticker AAPL was not found in the active cPAR package.")
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL/hedge?mode=factor_neutral")

    assert res.status_code == 404
    assert "not found" in res.json()["detail"].lower()


def test_router_registry_includes_cpar_router() -> None:
    registry_path = Path("backend/api/router_registry.py")
    module = ast.parse(registry_path.read_text())

    cpar_imported = False
    cpar_registered = False
    for node in module.body:
        if isinstance(node, ast.ImportFrom) and node.module == "backend.api.routes.cpar":
            cpar_imported = any(alias.name == "router" and alias.asname == "cpar_router" for alias in node.names)
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "API_ROUTERS" for target in node.targets):
            value = node.value
            if isinstance(value, ast.List):
                cpar_registered = any(isinstance(element, ast.Name) and element.id == "cpar_router" for element in value.elts)

    assert cpar_imported
    assert cpar_registered
