from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api.routes import cuse_risk_page as cuse_risk_page_routes
from backend.data.account_scope import AccountScope
from backend.data.account_scope import AccountScopeAuthRequired
from backend.main import app


def test_cuse_risk_page_route_returns_account_scoped_snapshot(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cuse_risk_page_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cuse_risk_page_routes,
        "resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_a",
            account_ids=("acct_a",),
        ),
    )

    def _load_payload(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return {
            "portfolio": {"positions": [], "total_value": 100.0, "position_count": 0, "_cached": False},
            "risk": {
                "risk_shares": {"market": 1.0, "industry": 2.0, "style": 3.0, "idio": 94.0},
                "factor_details": [{"factor_id": "market_beta", "category": "market"}],
                "_cached": False,
            },
            "exposures": {
                "raw": {"mode": "raw", "factors": [], "_cached": False},
            },
            "_cached": False,
            "_account_scoped": True,
            "account_id": "acct_a",
        }

    monkeypatch.setattr(cuse_risk_page_routes.cuse4_risk_page_service, "load_cuse_risk_page_payload", _load_payload)

    client = TestClient(app)
    res = client.get("/api/cuse/risk-page")

    assert res.status_code == 200
    body = res.json()
    assert body["_account_scoped"] is True
    assert body["account_id"] == "acct_a"
    assert body["portfolio"]["_cached"] is False
    assert body["risk"]["risk_shares"]["idio"] == 94.0
    assert body["risk"]["factor_details"][0]["factor_id"] == "market_beta"
    assert body["exposures"]["raw"]["mode"] == "raw"
    assert "sensitivity" not in body["exposures"]
    assert captured["kwargs"] == {
        "account_id": "acct_a",
        "allowed_account_ids": ("acct_a",),
    }


def test_cuse_risk_page_route_requires_auth_when_account_scope_enabled(monkeypatch) -> None:
    monkeypatch.setattr(cuse_risk_page_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cuse_risk_page_routes,
        "resolve_holdings_scope",
        lambda **kwargs: (_ for _ in ()).throw(AccountScopeAuthRequired("Missing authenticated app session.")),
    )

    client = TestClient(app)
    res = client.get("/api/cuse/risk-page")

    assert res.status_code == 401
    assert "Missing authenticated app session." in res.json()["detail"]


def test_cuse_risk_page_exposure_mode_route_returns_requested_mode(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cuse_risk_page_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cuse_risk_page_routes,
        "resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_a",
            account_ids=("acct_a",),
        ),
    )

    def _load_payload(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return {"mode": "sensitivity", "factors": [], "_cached": False, "_account_scoped": True}

    monkeypatch.setattr(
        cuse_risk_page_routes.cuse4_risk_page_service,
        "load_cuse_risk_page_exposure_mode_payload",
        _load_payload,
    )

    client = TestClient(app)
    res = client.get("/api/cuse/risk-page/exposure-mode?mode=sensitivity")

    assert res.status_code == 200
    assert res.json()["mode"] == "sensitivity"
    assert captured["kwargs"] == {
        "mode": "sensitivity",
        "account_id": "acct_a",
        "allowed_account_ids": ("acct_a",),
    }


def test_cuse_risk_page_covariance_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(cuse_risk_page_routes, "account_enforcement_enabled", lambda: False)
    monkeypatch.setattr(
        cuse_risk_page_routes.cuse4_risk_page_service,
        "load_cuse_risk_page_covariance_payload",
        lambda: {
            "cov_matrix": {"factors": ["market_beta"], "correlation": [[1.0]]},
            "_cached": True,
        },
    )

    client = TestClient(app)
    res = client.get("/api/cuse/risk-page/covariance")

    assert res.status_code == 200
    assert res.json()["cov_matrix"]["factors"] == ["market_beta"]
