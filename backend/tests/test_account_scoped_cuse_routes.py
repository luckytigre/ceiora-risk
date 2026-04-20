from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api.routes import exposures as exposures_routes
from backend.api.routes import risk as risk_routes
from backend.data.account_scope import AccountScope
from backend.data.account_scope import AccountScopeAuthRequired
from backend.main import app


def test_exposures_route_returns_account_scoped_preview_payload(monkeypatch) -> None:
    monkeypatch.setattr(exposures_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        exposures_routes,
        "resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_a",
            account_ids=("acct_a",),
        ),
    )
    monkeypatch.setattr(
        exposures_routes,
        "preview_portfolio_whatif",
        lambda scenario_rows, **kwargs: {
            "current": {
                "exposure_modes": {
                    "raw": [{"factor_id": "style_beta_score", "value": 1.0}],
                    "sensitivity": [],
                    "risk_contribution": [],
                }
            },
            "source_dates": {"exposures_served_asof": "2026-04-18"},
            "serving_snapshot": {
                "run_id": "run_curr",
                "snapshot_id": "snap_curr",
                "refresh_started_at": "2026-04-18T12:00:00+00:00",
            },
            "_preview_only": True,
        },
    )

    client = TestClient(app)
    res = client.get("/api/exposures?mode=raw")

    assert res.status_code == 200
    body = res.json()
    assert body["_account_scoped"] is True
    assert body["_cached"] is False
    assert body["account_id"] == "acct_a"
    assert body["factors"][0]["factor_id"] == "style_beta_score"
    assert body["run_id"] == "run_curr"
    assert body["source_dates"]["exposures_served_asof"] == "2026-04-18"


def test_exposures_route_requires_auth_when_account_scope_enabled(monkeypatch) -> None:
    monkeypatch.setattr(exposures_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        exposures_routes,
        "resolve_holdings_scope",
        lambda **kwargs: (_ for _ in ()).throw(AccountScopeAuthRequired("Missing authenticated app session.")),
    )

    client = TestClient(app)
    res = client.get("/api/exposures?mode=raw")

    assert res.status_code == 401
    assert "Missing authenticated app session." in res.json()["detail"]


def test_exposures_route_scopes_admin_reads_to_membership_accounts(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(exposures_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        exposures_routes,
        "resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=True,
            subject="admin@example.com",
            default_account_id="acct_admin",
            account_ids=("acct_admin", "acct_alt"),
        ),
    )
    def _preview_portfolio_whatif(scenario_rows, **kwargs):
        captured["kwargs"] = dict(kwargs)
        return {
            "current": {"exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []}},
            "source_dates": {},
            "_preview_only": True,
        }

    monkeypatch.setattr(exposures_routes, "preview_portfolio_whatif", _preview_portfolio_whatif)
    monkeypatch.setattr(
        exposures_routes.dashboard_payload_service,
        "load_account_scoped_exposures_response",
        lambda **kwargs: {"_account_scoped": True, "account_id": kwargs["account_id"], "factors": []},
    )

    client = TestClient(app)
    res = client.get("/api/exposures?mode=raw")

    assert res.status_code == 200
    assert res.json()["_account_scoped"] is True
    assert captured["kwargs"]["account_id"] == "acct_admin"
    assert captured["kwargs"]["allowed_account_ids"] == ("acct_admin", "acct_alt")


def test_risk_route_returns_account_scoped_preview_payload(monkeypatch) -> None:
    monkeypatch.setattr(risk_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        risk_routes,
        "resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_a",
            account_ids=("acct_a",),
        ),
    )
    monkeypatch.setattr(
        risk_routes,
        "preview_portfolio_whatif",
        lambda scenario_rows, **kwargs: {
            "current": {
                "risk_shares": {"market": 1.0, "industry": 2.0, "style": 3.0, "idio": 94.0},
                "vol_scaled_shares": {"market": 11.0, "industry": 22.0, "style": 33.0, "idio": 34.0},
                "component_shares": {"market": 1.0, "industry": 2.0, "style": 3.0},
                "factor_details": [{"factor_id": "market", "category": "market"}],
                "cov_matrix": {"factors": ["market"], "correlation": [[1.0]]},
                "r_squared": 0.4,
                "risk_engine": {"specific_risk_ticker_count": 10},
            },
            "source_dates": {"risk_asof": "2026-04-18"},
            "serving_snapshot": {
                "run_id": "run_curr",
                "snapshot_id": "snap_curr",
                "refresh_started_at": "2026-04-18T12:00:00+00:00",
            },
            "_preview_only": True,
        },
    )

    client = TestClient(app)
    res = client.get("/api/risk")

    assert res.status_code == 200
    body = res.json()
    assert body["_account_scoped"] is True
    assert body["_cached"] is False
    assert body["account_id"] == "acct_a"
    assert body["risk_shares"]["market"] == 1.0
    assert body["vol_scaled_shares"]["market"] == 11.0
    assert body["model_sanity"]["status"] == "scoped-preview"
    assert body["run_id"] == "run_curr"


def test_risk_route_requires_auth_when_account_scope_enabled(monkeypatch) -> None:
    monkeypatch.setattr(risk_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        risk_routes,
        "resolve_holdings_scope",
        lambda **kwargs: (_ for _ in ()).throw(AccountScopeAuthRequired("Missing authenticated app session.")),
    )

    client = TestClient(app)
    res = client.get("/api/risk")

    assert res.status_code == 401
    assert "Missing authenticated app session." in res.json()["detail"]


def test_risk_route_scopes_admin_reads_to_membership_accounts(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(risk_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        risk_routes,
        "resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=True,
            subject="admin@example.com",
            default_account_id="acct_admin",
            account_ids=("acct_admin", "acct_alt"),
        ),
    )
    def _preview_portfolio_whatif(scenario_rows, **kwargs):
        captured["kwargs"] = dict(kwargs)
        return {
            "current": {
                "risk_shares": {"market": 1.0, "industry": 2.0, "style": 3.0, "idio": 94.0},
                "component_shares": {"market": 1.0, "industry": 2.0, "style": 3.0},
                "factor_details": [],
                "cov_matrix": {"factors": [], "correlation": []},
                "r_squared": 0.4,
                "risk_engine": {"specific_risk_ticker_count": 10},
            },
            "source_dates": {},
            "_preview_only": True,
        }

    monkeypatch.setattr(risk_routes, "preview_portfolio_whatif", _preview_portfolio_whatif)
    monkeypatch.setattr(
        risk_routes.dashboard_payload_service,
        "load_account_scoped_risk_response",
        lambda **kwargs: {"_account_scoped": True, "account_id": kwargs["account_id"], "risk_shares": {"market": 1.0}},
    )

    client = TestClient(app)
    res = client.get("/api/risk")

    assert res.status_code == 200
    assert res.json()["_account_scoped"] is True
    assert captured["kwargs"]["account_id"] == "acct_admin"
    assert captured["kwargs"]["allowed_account_ids"] == ("acct_admin", "acct_alt")
