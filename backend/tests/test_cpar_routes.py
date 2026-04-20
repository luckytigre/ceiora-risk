from __future__ import annotations

import ast
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.api.routes import cpar as cpar_routes
from backend.data.account_scope import AccountScope
from backend.data.account_scope import AccountScopeDenied


def _test_app() -> FastAPI:
    app = FastAPI()
    app.include_router(cpar_routes.router, prefix="/api")
    return app


EXPECTED_CPAR_ROUTE_SET = {
    ("GET", "/api/cpar/meta"),
    ("GET", "/api/cpar/search"),
    ("GET", "/api/cpar/ticker/{ticker}"),
    ("GET", "/api/cpar/ticker/{ticker}/history"),
    ("GET", "/api/cpar/explore/context"),
    ("GET", "/api/cpar/risk"),
    ("GET", "/api/cpar/factors/history"),
    ("GET", "/api/cpar/portfolio/hedge"),
    ("POST", "/api/cpar/portfolio/whatif"),
    ("POST", "/api/cpar/explore/whatif"),
}


def test_cpar_route_set_remains_explicit() -> None:
    app = _test_app()
    actual = {
        (method, route.path)
        for route in app.routes
        if getattr(route, "path", "").startswith("/api/cpar")
        for method in getattr(route, "methods", set())
        if method not in {"HEAD", "OPTIONS"}
    }
    assert actual == EXPECTED_CPAR_ROUTE_SET


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


def test_cpar_ticker_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_ticker_service,
        "load_cpar_ticker_payload",
        lambda **kwargs: {
            "ticker": kwargs["ticker"],
            "ric": kwargs.get("ric") or "AAPL.OQ",
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "display_loadings": [],
            "thresholded_loadings": [],
        },
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL?ric=AAPL.OQ")

    assert res.status_code == 200
    assert res.json()["ric"] == "AAPL.OQ"
    assert "display_loadings" in res.json()


def test_cpar_ticker_route_maps_not_found_to_404(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_ticker_service,
        "load_cpar_ticker_payload",
        lambda **kwargs: (_ for _ in ()).throw(cpar_routes.cpar_ticker_service.CparTickerNotFound("Ticker missing")),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL")

    assert res.status_code == 404
    assert "Ticker missing" in res.json()["detail"]


def test_cpar_ticker_history_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_ticker_history_service,
        "load_cpar_ticker_history_payload",
        lambda **kwargs: {
            "ticker": kwargs["ticker"],
            "ric": kwargs.get("ric") or "AAPL.OQ",
            "years": kwargs["years"],
            "points": [{"date": "2026-03-13", "close": 100.0}],
            "_cached": True,
        },
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/ticker/AAPL/history?years=5")

    assert res.status_code == 200
    assert res.json()["points"][0]["close"] == 100.0


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


def test_cpar_risk_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_risk_service,
        "load_cpar_risk_payload",
        lambda **kwargs: {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "scope": "all_accounts",
            "accounts_count": 3,
            "portfolio_status": "ok",
            "coverage_breakdown": {
                "covered": {"positions_count": 2, "gross_market_value": 1500.0},
                "missing_price": {"positions_count": 0, "gross_market_value": 0.0},
                "missing_cpar_fit": {"positions_count": 0, "gross_market_value": 0.0},
                "insufficient_history": {"positions_count": 0, "gross_market_value": 0.0},
            },
            "aggregate_display_loadings": [],
            "risk_shares": {"market": 62.5, "industry": 17.5, "style": 10.0, "idio": 10.0},
            "factor_variance_contributions": [],
            "display_factor_variance_contributions": [],
            "factor_chart": [],
            "display_factor_chart": [],
            "cov_matrix": {"factors": ["SPY"], "correlation": [[1.0]]},
            "display_cov_matrix": {"factors": ["SPY"], "correlation": [[1.0]]},
            "factor_variance_proxy": 0.18,
            "idio_variance_proxy": 0.02,
            "total_variance_proxy": 0.2,
            "positions": [],
        },
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/risk")

    assert res.status_code == 200
    assert res.json()["scope"] == "all_accounts"
    assert res.json()["accounts_count"] == 3
    assert "aggregate_display_loadings" in res.json()
    assert "display_cov_matrix" in res.json()
    assert res.json()["risk_shares"]["idio"] == 10.0


def test_cpar_explore_context_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_explore_context_service,
        "load_cpar_explore_context_payload",
        lambda **kwargs: {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "profile": "weekly",
            "method_version": "v1",
            "factor_registry_version": "f1",
            "data_authority": "neon",
            "lookback_weeks": 52,
            "half_life_weeks": 26,
            "min_observations": 26,
            "universe_count": 20,
            "fit_ok_count": 19,
            "fit_limited_count": 1,
            "fit_insufficient_count": 0,
            "scope": "all_accounts",
            "accounts_count": 3,
            "positions_count": 1,
            "covered_positions_count": 1,
            "excluded_positions_count": 0,
            "gross_market_value": 1500.0,
            "net_market_value": 1500.0,
            "covered_gross_market_value": 1500.0,
            "coverage_ratio": 1.0,
            "portfolio_status": "ok",
            "portfolio_reason": None,
            "held_positions": [
                {
                    "ric": "AAPL.OQ",
                    "ticker": "AAPL",
                    "quantity": 10.0,
                    "price": 150.0,
                    "market_value": 1500.0,
                    "portfolio_weight": 1.0,
                    "long_short": "LONG",
                    "fit_status": "ok",
                    "coverage": "covered",
                },
            ],
        },
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/explore/context")

    assert res.status_code == 200
    assert res.json()["scope"] == "all_accounts"
    assert res.json()["held_positions"][0]["ric"] == "AAPL.OQ"


def test_cpar_explore_context_route_forwards_account_scope(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cpar_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cpar_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_a",
            account_ids=("acct_a", "acct_b"),
        ),
    )
    def _load_payload(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "profile": "weekly",
            "method_version": "v1",
            "factor_registry_version": "f1",
            "data_authority": "neon",
            "lookback_weeks": 52,
            "half_life_weeks": 26,
            "min_observations": 26,
            "universe_count": 1,
            "fit_ok_count": 1,
            "fit_limited_count": 0,
            "fit_insufficient_count": 0,
            "scope": "restricted_accounts",
            "accounts_count": 2,
            "positions_count": 0,
            "covered_positions_count": 0,
            "excluded_positions_count": 0,
            "gross_market_value": 0.0,
            "net_market_value": 0.0,
            "covered_gross_market_value": 0.0,
            "coverage_ratio": None,
            "portfolio_status": "empty",
            "portfolio_reason": "No live holdings positions are loaded across any account.",
            "held_positions": [],
        }

    monkeypatch.setattr(
        cpar_routes.cpar_explore_context_service,
        "load_cpar_explore_context_payload",
        _load_payload,
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/explore/context")

    assert res.status_code == 200
    assert captured["kwargs"] == {"allowed_account_ids": ["acct_a", "acct_b"]}


def test_cpar_risk_route_maps_not_ready_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_risk_service,
        "load_cpar_risk_payload",
        lambda **kwargs: (_ for _ in ()).throw(cpar_routes.cpar_meta_service.CparReadNotReady("No successful cPAR package")),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/risk")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "not_ready"


def test_cpar_risk_route_forwards_allowed_account_ids_when_enforced(monkeypatch) -> None:
    captured: dict[str, object] = {}
    def _load_payload(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "scope": "all_accounts",
            "accounts_count": 2,
            "portfolio_status": "ok",
            "coverage_breakdown": {"covered": {"positions_count": 0, "gross_market_value": 0.0}},
            "aggregate_display_loadings": [],
            "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0},
            "factor_variance_contributions": [],
            "display_factor_variance_contributions": [],
            "factor_chart": [],
            "display_factor_chart": [],
            "cov_matrix": {"factors": [], "correlation": []},
            "display_cov_matrix": {"factors": [], "correlation": []},
            "factor_variance_proxy": 0.0,
            "idio_variance_proxy": 0.0,
            "total_variance_proxy": 0.0,
            "positions": [],
        }

    monkeypatch.setattr(cpar_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cpar_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_main",
            account_ids=("acct_main", "acct_alt"),
        ),
    )
    monkeypatch.setattr(
        cpar_routes.cpar_risk_service,
        "load_cpar_risk_payload",
        _load_payload,
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/risk", headers={"X-App-Session-Token": "signed"})

    assert res.status_code == 200
    assert captured["kwargs"] == {"allowed_account_ids": ["acct_main", "acct_alt"]}


def test_cpar_factor_history_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_factor_history_service,
        "load_cpar_factor_history_payload",
        lambda **kwargs: {
            "factor_id": kwargs["factor_id"],
            "factor_name": "Market",
            "history_mode": kwargs["mode"],
            "years": kwargs["years"],
            "points": [
                {"date": "2025-03-14", "factor_return": 0.01, "cum_return": 0.01},
                {"date": "2025-03-21", "factor_return": -0.02, "cum_return": -0.01},
            ],
            "_cached": True,
        },
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/factors/history?factor_id=SPY&years=5&mode=residual")

    assert res.status_code == 200
    assert res.json()["factor_id"] == "SPY"
    assert res.json()["history_mode"] == "residual"
    assert len(res.json()["points"]) == 2


def test_cpar_factor_history_route_maps_missing_factor_to_404(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_factor_history_service,
        "load_cpar_factor_history_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_factor_history_service.CparFactorNotFound("Unknown cPAR factor_id 'BAD'.")
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/factors/history?factor_id=BAD&years=5")

    assert res.status_code == 404
    assert "Unknown cPAR factor_id" in res.json()["detail"]


def test_cpar_factor_history_route_maps_not_ready_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_factor_history_service,
        "load_cpar_factor_history_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparReadNotReady("Historical cPAR factor returns are not available yet.")
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/factors/history?factor_id=SPY&years=5")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "not_ready"


def test_cpar_factor_history_route_maps_unavailable_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_factor_history_service,
        "load_cpar_factor_history_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparReadUnavailable("Neon cPAR read failed")
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/factors/history?factor_id=SPY&years=5")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "unavailable"


def test_cpar_portfolio_whatif_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_whatif_service,
        "load_cpar_portfolio_whatif_payload",
        lambda **kwargs: {
            "account_id": kwargs["account_id"],
            "mode": kwargs["mode"],
            "scenario_row_count": len(kwargs["scenario_rows"]),
            "changed_positions_count": len(kwargs["scenario_rows"]),
            "current": {
                "package_run_id": "run_curr",
                "coverage_breakdown": {
                    "covered": {"positions_count": 1, "gross_market_value": 1000.0},
                    "missing_price": {"positions_count": 0, "gross_market_value": 0.0},
                    "missing_cpar_fit": {"positions_count": 0, "gross_market_value": 0.0},
                    "insufficient_history": {"positions_count": 0, "gross_market_value": 0.0},
                },
                "factor_variance_contributions": [
                    {
                        "factor_id": "SPY",
                        "label": "Market",
                        "group": "market",
                        "display_order": 0,
                        "beta": 1.0,
                        "variance_contribution": 1.0,
                        "variance_share": 1.0,
                    }
                ],
                "positions": [
                    {
                        "ric": "AAPL.OQ",
                        "thresholded_contributions": [
                            {
                                "factor_id": "SPY",
                                "label": "Market",
                                "group": "market",
                                "display_order": 0,
                                "beta": 1.0,
                            }
                        ],
                    }
                ],
            },
            "hypothetical": {
                "package_run_id": "run_curr",
                "coverage_breakdown": {
                    "covered": {"positions_count": 2, "gross_market_value": 1500.0},
                    "missing_price": {"positions_count": 0, "gross_market_value": 0.0},
                    "missing_cpar_fit": {"positions_count": 0, "gross_market_value": 0.0},
                    "insufficient_history": {"positions_count": 0, "gross_market_value": 0.0},
                },
                "factor_variance_contributions": [
                    {
                        "factor_id": "SPY",
                        "label": "Market",
                        "group": "market",
                        "display_order": 0,
                        "beta": 1.1,
                        "variance_contribution": 1.2,
                        "variance_share": 1.0,
                    }
                ],
                "positions": [
                    {
                        "ric": "AAPL.OQ",
                        "thresholded_contributions": [
                            {
                                "factor_id": "SPY",
                                "label": "Market",
                                "group": "market",
                                "display_order": 0,
                                "beta": 1.1,
                            }
                        ],
                    }
                ],
            },
        },
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/portfolio/whatif",
        json={
            "account_id": "acct_main",
            "mode": "factor_neutral",
            "scenario_rows": [{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity_delta": 5.0}],
        },
    )

    assert res.status_code == 200
    assert res.json()["account_id"] == "acct_main"
    assert res.json()["scenario_row_count"] == 1
    assert res.json()["current"]["coverage_breakdown"]["covered"]["positions_count"] == 1
    assert res.json()["hypothetical"]["factor_variance_contributions"][0]["factor_id"] == "SPY"
    assert res.json()["hypothetical"]["positions"][0]["thresholded_contributions"][0]["beta"] == 1.1


def test_cpar_portfolio_whatif_route_maps_validation_errors_to_400(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_whatif_service,
        "load_cpar_portfolio_whatif_payload",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("At least one non-zero cPAR what-if scenario row is required.")),
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/portfolio/whatif",
        json={
            "account_id": "acct_main",
            "mode": "factor_neutral",
            "scenario_rows": [{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity_delta": 0.0}],
        },
    )

    assert res.status_code == 400
    assert "non-zero" in res.json()["detail"]


def test_cpar_portfolio_whatif_route_maps_not_ready_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_whatif_service,
        "load_cpar_portfolio_whatif_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparReadNotReady("No successful cPAR package")
        ),
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/portfolio/whatif",
        json={
            "account_id": "acct_main",
            "mode": "factor_neutral",
            "scenario_rows": [{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity_delta": 5.0}],
        },
    )

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "not_ready"


def test_cpar_portfolio_whatif_route_maps_unavailable_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_whatif_service,
        "load_cpar_portfolio_whatif_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparReadUnavailable("Neon cPAR read failed")
        ),
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/portfolio/whatif",
        json={
            "account_id": "acct_main",
            "mode": "factor_neutral",
            "scenario_rows": [{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity_delta": 5.0}],
        },
    )

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "unavailable"
    assert res.json()["detail"]["error"] == "cpar_authority_unavailable"


def test_cpar_portfolio_whatif_route_maps_missing_account_to_404(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_whatif_service,
        "load_cpar_portfolio_whatif_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_portfolio_hedge_service.CparPortfolioAccountNotFound("acct_missing")
        ),
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/portfolio/whatif",
        json={
            "account_id": "acct_missing",
            "mode": "factor_neutral",
            "scenario_rows": [{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity_delta": 5.0}],
        },
    )

    assert res.status_code == 404
    assert "acct_missing" in res.json()["detail"]


def test_cpar_portfolio_whatif_route_rejects_account_outside_scope(monkeypatch) -> None:
    monkeypatch.setattr(cpar_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cpar_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_main",
            account_ids=("acct_main",),
        ),
    )
    monkeypatch.setattr(
        cpar_routes,
        "validate_requested_account",
        lambda scope, requested_account_id: (_ for _ in ()).throw(AccountScopeDenied("cpar what-if denied")),
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/portfolio/whatif",
        json={
            "account_id": "acct_other",
            "mode": "factor_neutral",
            "scenario_rows": [{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity_delta": 5.0}],
        },
    )

    assert res.status_code == 403
    assert "cpar what-if denied" in res.json()["detail"]


def test_cpar_portfolio_whatif_route_forwards_allowed_account_ids(monkeypatch) -> None:
    captured: dict[str, object] = {}
    def _load_payload(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return {"scenario_rows": [], "current": {}, "hypothetical": {}, "holding_deltas": [], "diff": {}, "_preview_only": True}

    monkeypatch.setattr(cpar_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cpar_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_main",
            account_ids=("acct_main", "acct_alt"),
        ),
    )
    monkeypatch.setattr(cpar_routes, "validate_requested_account", lambda scope, requested_account_id: requested_account_id)
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_whatif_service,
        "load_cpar_portfolio_whatif_payload",
        _load_payload,
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/portfolio/whatif",
        json={
            "account_id": "acct_main",
            "mode": "factor_neutral",
            "scenario_rows": [{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity_delta": 5.0}],
        },
        headers={"X-App-Session-Token": "signed"},
    )

    assert res.status_code == 200
    assert captured["kwargs"]["allowed_account_ids"] == ["acct_main", "acct_alt"]


def test_cpar_risk_route_scopes_admin_reads_to_membership_accounts(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cpar_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cpar_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=True,
            subject="admin@example.com",
            default_account_id="acct_admin",
            account_ids=("acct_admin", "acct_alt"),
        ),
    )
    def _load_cpar_risk_payload(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return {"status": "ok"}

    monkeypatch.setattr(cpar_routes.cpar_risk_service, "load_cpar_risk_payload", _load_cpar_risk_payload)

    client = TestClient(_test_app())
    res = client.get("/api/cpar/risk", headers={"X-App-Session-Token": "signed"})

    assert res.status_code == 200
    assert captured["kwargs"]["allowed_account_ids"] == ["acct_admin", "acct_alt"]


def test_cpar_portfolio_whatif_route_enforces_max_rows() -> None:
    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/portfolio/whatif",
        json={
            "account_id": "acct_main",
            "mode": "factor_neutral",
            "scenario_rows": [
                {"ric": f"RIC{i}.OQ", "ticker": f"T{i}", "quantity_delta": 1.0}
                for i in range(cpar_routes.MAX_CPAR_WHATIF_SCENARIO_ROWS + 1)
            ],
        },
    )

    assert res.status_code == 400
    assert "Too many cPAR what-if rows" in res.json()["detail"]


def test_cpar_explore_whatif_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_explore_whatif_service,
        "load_cpar_explore_whatif_payload",
        lambda **kwargs: {
            "scenario_rows": kwargs["scenario_rows"],
            "current": {"risk_shares": {"market": 70.0, "industry": 20.0, "style": 10.0, "idio": 0.0}, "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []}, "positions": [], "factor_catalog": [], "position_count": 0, "total_value": 0.0, "scope": "all_accounts", "portfolio_status": "ok", "portfolio_reason": None},
            "hypothetical": {"risk_shares": {"market": 60.0, "industry": 25.0, "style": 15.0, "idio": 0.0}, "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []}, "positions": [], "factor_catalog": [], "position_count": 0, "total_value": 0.0, "scope": "all_accounts", "portfolio_status": "ok", "portfolio_reason": None},
            "holding_deltas": [],
            "diff": {"total_value": 0.0, "position_count": 0, "risk_shares": {"market": -10.0, "industry": 5.0, "style": 5.0, "idio": 0.0}, "factor_deltas": {"raw": [], "sensitivity": [], "risk_contribution": []}},
            "_preview_only": True,
        },
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/explore/whatif",
        json={"scenario_rows": [{"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 5.0}]},
    )

    assert res.status_code == 200
    assert res.json()["_preview_only"] is True
    assert res.json()["scenario_rows"][0]["account_id"] == "acct_main"


def test_cpar_explore_whatif_route_maps_validation_errors_to_400(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_explore_whatif_service,
        "load_cpar_explore_whatif_payload",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("At least one non-zero cPAR explore scenario row is required.")),
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/explore/whatif",
        json={"scenario_rows": [{"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 0.0}]},
    )

    assert res.status_code == 400
    assert "non-zero" in res.json()["detail"]


def test_cpar_explore_whatif_route_rejects_rows_outside_scope(monkeypatch) -> None:
    monkeypatch.setattr(cpar_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cpar_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_main",
            account_ids=("acct_main",),
        ),
    )
    monkeypatch.setattr(
        cpar_routes,
        "validate_requested_account",
        lambda scope, requested_account_id: (_ for _ in ()).throw(AccountScopeDenied("cpar explore denied")),
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/explore/whatif",
        json={"scenario_rows": [{"account_id": "acct_other", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 5.0}]},
    )

    assert res.status_code == 403
    assert "cpar explore denied" in res.json()["detail"]


def test_cpar_explore_whatif_route_forwards_allowed_account_ids(monkeypatch) -> None:
    captured: dict[str, object] = {}
    def _load_payload(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return {
            "scenario_rows": [],
            "current": {"risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}, "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []}, "positions": [], "factor_catalog": [], "position_count": 0, "total_value": 0.0, "scope": "all_accounts", "portfolio_status": "ok", "portfolio_reason": None},
            "hypothetical": {"risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}, "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []}, "positions": [], "factor_catalog": [], "position_count": 0, "total_value": 0.0, "scope": "all_accounts", "portfolio_status": "ok", "portfolio_reason": None},
            "holding_deltas": [],
            "diff": {"total_value": 0.0, "position_count": 0, "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}, "factor_deltas": {"raw": [], "sensitivity": [], "risk_contribution": []}},
            "_preview_only": True,
        }

    monkeypatch.setattr(cpar_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cpar_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_main",
            account_ids=("acct_main",),
        ),
    )
    monkeypatch.setattr(cpar_routes, "validate_requested_account", lambda scope, requested_account_id: requested_account_id)
    monkeypatch.setattr(
        cpar_routes.cpar_explore_whatif_service,
        "load_cpar_explore_whatif_payload",
        _load_payload,
    )

    client = TestClient(_test_app())
    res = client.post(
        "/api/cpar/explore/whatif",
        json={"scenario_rows": [{"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 5.0}]},
        headers={"X-App-Session-Token": "signed"},
    )

    assert res.status_code == 200
    assert captured["kwargs"]["allowed_account_ids"] == ["acct_main"]


def test_cpar_portfolio_hedge_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_hedge_service,
        "load_cpar_portfolio_hedge_payload",
        lambda **kwargs: {
            "account_id": kwargs["account_id"],
            "mode": kwargs["mode"],
            "portfolio_status": "ok",
            "coverage_breakdown": {
                "covered": {"positions_count": 1, "gross_market_value": 1000.0},
                "missing_price": {"positions_count": 0, "gross_market_value": 0.0},
                "missing_cpar_fit": {"positions_count": 0, "gross_market_value": 0.0},
                "insufficient_history": {"positions_count": 0, "gross_market_value": 0.0},
            },
            "factor_variance_contributions": [
                {
                    "factor_id": "SPY",
                    "label": "Market",
                    "group": "market",
                    "display_order": 0,
                    "beta": 1.0,
                    "variance_contribution": 1.0,
                    "variance_share": 1.0,
                }
            ],
            "hedge_status": "hedge_ok",
            "positions": [
                {
                    "ric": "AAPL.OQ",
                    "thresholded_contributions": [
                        {
                            "factor_id": "SPY",
                            "label": "Market",
                            "group": "market",
                            "display_order": 0,
                            "beta": 1.0,
                        }
                    ],
                }
            ],
        },
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/portfolio/hedge?account_id=acct_main&mode=factor_neutral")

    assert res.status_code == 200
    assert res.json()["account_id"] == "acct_main"
    assert res.json()["mode"] == "factor_neutral"
    assert res.json()["coverage_breakdown"]["covered"]["positions_count"] == 1
    assert res.json()["factor_variance_contributions"][0]["variance_share"] == 1.0
    assert res.json()["positions"][0]["thresholded_contributions"][0]["factor_id"] == "SPY"


def test_cpar_portfolio_hedge_route_scopes_admin_reads_to_membership_accounts(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cpar_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cpar_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=True,
            subject="admin@example.com",
            default_account_id="acct_admin",
            account_ids=("acct_admin", "acct_alt"),
        ),
    )
    monkeypatch.setattr(cpar_routes, "validate_requested_account", lambda scope, requested_account_id: requested_account_id)
    def _load_cpar_portfolio_hedge_payload(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return {
            "account_id": kwargs["account_id"],
            "mode": kwargs["mode"],
            "coverage_breakdown": {},
            "factor_variance_contributions": [],
            "positions": [],
        }

    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_hedge_service,
        "load_cpar_portfolio_hedge_payload",
        _load_cpar_portfolio_hedge_payload,
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/portfolio/hedge?account_id=acct_admin&mode=factor_neutral", headers={"X-App-Session-Token": "signed"})

    assert res.status_code == 200
    assert captured["kwargs"]["allowed_account_ids"] == ["acct_admin", "acct_alt"]


def test_cpar_portfolio_hedge_route_maps_not_ready_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_hedge_service,
        "load_cpar_portfolio_hedge_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparReadNotReady("No successful cPAR package")
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/portfolio/hedge?account_id=acct_main")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "not_ready"
    assert res.json()["detail"]["error"] == "cpar_not_ready"


def test_cpar_portfolio_hedge_route_maps_unavailable_to_503(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_hedge_service,
        "load_cpar_portfolio_hedge_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_meta_service.CparReadUnavailable("Holdings read failed")
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/portfolio/hedge?account_id=acct_main")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "unavailable"
    assert res.json()["detail"]["error"] == "cpar_authority_unavailable"


def test_cpar_portfolio_hedge_route_maps_missing_account_to_404(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_hedge_service,
        "load_cpar_portfolio_hedge_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cpar_routes.cpar_portfolio_hedge_service.CparPortfolioAccountNotFound("account missing")
        ),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/portfolio/hedge?account_id=acct_missing")

    assert res.status_code == 404
    assert "account missing" in res.json()["detail"]


def test_cpar_portfolio_hedge_route_rejects_account_outside_scope(monkeypatch) -> None:
    monkeypatch.setattr(cpar_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cpar_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_main",
            account_ids=("acct_main",),
        ),
    )
    monkeypatch.setattr(
        cpar_routes,
        "validate_requested_account",
        lambda scope, requested_account_id: (_ for _ in ()).throw(AccountScopeDenied("cpar hedge denied")),
    )

    client = TestClient(_test_app())
    res = client.get("/api/cpar/portfolio/hedge?account_id=acct_other")

    assert res.status_code == 403
    assert "cpar hedge denied" in res.json()["detail"]


def test_cpar_portfolio_hedge_route_forwards_allowed_account_ids(monkeypatch) -> None:
    captured: dict[str, object] = {}
    def _load_payload(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return {
            "account_id": kwargs["account_id"],
            "mode": kwargs["mode"],
            "portfolio_status": "ok",
            "coverage_breakdown": {"covered": {"positions_count": 0, "gross_market_value": 0.0}},
            "factor_variance_contributions": [],
            "positions": [],
        }

    monkeypatch.setattr(cpar_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cpar_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_main",
            account_ids=("acct_main", "acct_alt"),
        ),
    )
    monkeypatch.setattr(cpar_routes, "validate_requested_account", lambda scope, requested_account_id: requested_account_id)
    monkeypatch.setattr(
        cpar_routes.cpar_portfolio_hedge_service,
        "load_cpar_portfolio_hedge_payload",
        _load_payload,
    )

    client = TestClient(_test_app())
    res = client.get(
        "/api/cpar/portfolio/hedge?account_id=acct_main&mode=factor_neutral",
        headers={"X-App-Session-Token": "signed"},
    )

    assert res.status_code == 200
    assert captured["kwargs"]["allowed_account_ids"] == ["acct_main", "acct_alt"]


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
