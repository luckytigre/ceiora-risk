from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api import auth as auth_module
from backend.api.routes import portfolio as portfolio_routes
from backend.main import app


def test_portfolio_whatif_route_returns_preview_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        portfolio_routes,
        "preview_portfolio_whatif",
        lambda scenario_rows: {
            "scenario_rows": scenario_rows,
            "holding_deltas": [],
            "current": {
                "positions": [{"ticker": "AAA", "trbc_sector": "Technology"}],
                "total_value": 100.0,
                "position_count": 1,
                "risk_shares": {"market": 1.0, "industry": 2.0, "style": 3.0, "idio": 94.0},
                "component_shares": {"market": 1.0, "industry": 2.0, "style": 3.0},
                "factor_details": [],
                "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []},
                "factor_catalog": [],
            },
            "hypothetical": {
                "positions": [{"ticker": "AAA", "trbc_sector": "Technology"}],
                "total_value": 120.0,
                "position_count": 1,
                "risk_shares": {"market": 2.0, "industry": 2.0, "style": 4.0, "idio": 92.0},
                "component_shares": {"market": 2.0, "industry": 2.0, "style": 4.0},
                "factor_details": [],
                "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []},
                "factor_catalog": [],
            },
            "diff": {
                "total_value": 20.0,
                "position_count": 0,
                "risk_shares": {"market": 1.0, "industry": 0.0, "style": 1.0, "idio": -2.0},
                "factor_deltas": {"raw": [], "sensitivity": [], "risk_contribution": []},
            },
            "source_dates": {},
            "_preview_only": True,
        },
    )

    client = TestClient(app)
    res = client.post(
        "/api/portfolio/whatif",
        json={"scenario_rows": [{"account_id": "acct_a", "ticker": "AAA", "quantity": 20}]},
    )

    assert res.status_code == 200
    body = res.json()
    assert body["_preview_only"] is True
    assert body["current"]["positions"][0]["trbc_economic_sector_short"] == "Technology"
    assert body["hypothetical"]["total_value"] == 120.0


def test_portfolio_whatif_route_rejects_missing_account_id() -> None:
    client = TestClient(app)
    res = client.post(
        "/api/portfolio/whatif",
        json={"scenario_rows": [{"ticker": "AAA", "quantity": 20}]},
    )

    assert res.status_code == 422


def test_portfolio_whatif_route_rejects_missing_ticker() -> None:
    client = TestClient(app)
    res = client.post(
        "/api/portfolio/whatif",
        json={"scenario_rows": [{"account_id": "acct_a", "quantity": 20}]},
    )

    assert res.status_code == 422


def test_portfolio_whatif_route_rejects_non_finite_quantity() -> None:
    client = TestClient(app)
    res = client.post(
        "/api/portfolio/whatif",
        json={"scenario_rows": [{"account_id": "acct_a", "ticker": "AAA", "quantity": "NaN"}]},
    )

    assert res.status_code == 422


def test_portfolio_whatif_apply_route_returns_service_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        portfolio_routes.holdings_service,
        "run_whatif_apply",
        lambda **kwargs: {
            "status": "ok",
            "accepted_rows": 1,
            "rejected_rows": 0,
            "rejection_counts": {},
            "warnings": [],
            "applied_upserts": 1,
            "applied_deletes": 0,
            "row_results": [
                {
                    "account_id": "acct_a",
                    "ticker": "AAA",
                    "ric": "AAA.N",
                    "current_quantity": 10.0,
                    "applied_quantity": 20.0,
                    "action": "replace",
                }
            ],
            "rejected": [],
            "import_batch_ids": {"acct_a": "batch_1"},
        },
    )

    client = TestClient(app)
    res = client.post(
        "/api/portfolio/whatif/apply",
        json={"scenario_rows": [{"account_id": "acct_a", "ticker": "AAA", "quantity": 20}]},
    )

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "ok"
    assert body["applied_upserts"] == 1
    assert body["row_results"][0]["action"] == "replace"


def test_portfolio_whatif_preview_requires_operator_token_in_cloud(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(portfolio_routes.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(portfolio_routes, "preview_portfolio_whatif", lambda scenario_rows: {"scenario_rows": scenario_rows, "holding_deltas": [], "current": {"positions": [], "total_value": 0.0, "position_count": 0, "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}, "component_shares": {"market": 0.0, "industry": 0.0, "style": 0.0}, "factor_details": [], "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []}, "factor_catalog": []}, "hypothetical": {"positions": [], "total_value": 0.0, "position_count": 0, "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}, "component_shares": {"market": 0.0, "industry": 0.0, "style": 0.0}, "factor_details": [], "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []}, "factor_catalog": []}, "diff": {"total_value": 0.0, "position_count": 0, "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}, "factor_deltas": {"raw": [], "sensitivity": [], "risk_contribution": []}}, "_preview_only": True})

    client = TestClient(app)
    payload = {"scenario_rows": [{"account_id": "acct_a", "ticker": "AAA", "quantity": 20}]}

    assert client.post("/api/portfolio/whatif", json=payload).status_code == 401
    assert client.post("/api/portfolio/whatif", json=payload, headers={"X-Operator-Token": "op-secret"}).status_code == 200


def test_portfolio_whatif_apply_requires_editor_or_operator_token_in_cloud(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(portfolio_routes.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(portfolio_routes.config, "EDITOR_API_TOKEN", "edit-secret")
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(auth_module.config, "EDITOR_API_TOKEN", "edit-secret")
    monkeypatch.setattr(
        portfolio_routes.holdings_service,
        "run_whatif_apply",
        lambda **kwargs: {
            "status": "ok",
            "accepted_rows": 1,
            "rejected_rows": 0,
            "rejection_counts": {},
            "warnings": [],
            "applied_upserts": 1,
            "applied_deletes": 0,
            "row_results": [],
            "rejected": [],
            "import_batch_ids": {"acct_a": "batch_1"},
        },
    )

    client = TestClient(app)
    payload = {"scenario_rows": [{"account_id": "acct_a", "ticker": "AAA", "quantity": 20}]}

    assert client.post("/api/portfolio/whatif/apply", json=payload).status_code == 401
    assert client.post("/api/portfolio/whatif/apply", json=payload, headers={"X-Editor-Token": "edit-secret"}).status_code == 200
    assert client.post("/api/portfolio/whatif/apply", json=payload, headers={"X-Operator-Token": "op-secret"}).status_code == 200
