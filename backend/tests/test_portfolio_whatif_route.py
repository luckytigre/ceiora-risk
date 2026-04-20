from __future__ import annotations

import base64
import hashlib
import hmac
import json

from fastapi.testclient import TestClient

from backend.api import auth as auth_module
from backend.api.routes import portfolio as portfolio_routes
from backend.data.account_scope import AccountScopeAuthRequired
from backend.data.account_scope import AccountScope
from backend.data.account_scope import AccountScopeDenied
from backend.data.account_scope import AccountScopeProvisioningError
from backend.main import app


def _signed_app_session_token(
    *,
    provider: str = "shared",
    subject: str = "friend@example.com",
    email: str | None = "friend@example.com",
    is_admin: bool = False,
) -> str:
    payload = base64.urlsafe_b64encode(
        json.dumps(
            {
                "authProvider": provider,
                "username": email or subject,
                "subject": subject,
                "email": email,
                "isAdmin": is_admin,
                "issuedAt": 1,
                "expiresAt": 4743856000,
            }
        ).encode("utf-8")
    ).decode("ascii").rstrip("=")
    signature = base64.urlsafe_b64encode(
        hmac.new(b"test-secret", payload.encode("utf-8"), hashlib.sha256).digest()
    ).decode("ascii").rstrip("=")
    return f"{payload}.{signature}"


def test_portfolio_whatif_route_returns_preview_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        portfolio_routes,
        "preview_portfolio_whatif",
        lambda scenario_rows, **kwargs: {
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


def test_portfolio_route_returns_account_scoped_current_payload(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(
        portfolio_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_a",
            account_ids=("acct_a",),
        ),
    )
    monkeypatch.setattr(
        portfolio_routes,
        "preview_portfolio_whatif",
        lambda scenario_rows, **kwargs: {
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
            "source_dates": {"positions": "2026-04-18"},
            "_preview_only": True,
        },
    )

    client = TestClient(app)
    res = client.get("/api/portfolio")

    assert res.status_code == 200
    body = res.json()
    assert body["_account_scoped"] is True
    assert body["_cached"] is False
    assert body["account_id"] == "acct_a"
    assert body["source_dates"]["positions"] == "2026-04-18"
    assert body["positions"][0]["trbc_economic_sector_short"] == "Technology"


def test_portfolio_route_requires_auth_when_account_scope_enabled(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(
        portfolio_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: (_ for _ in ()).throw(AccountScopeAuthRequired("Missing authenticated app session.")),
    )

    client = TestClient(app)
    res = client.get("/api/portfolio")

    assert res.status_code == 401
    assert "Missing authenticated app session." in res.json()["detail"]


def test_portfolio_route_surfaces_provisioning_error_when_account_scope_enabled(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(
        portfolio_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: (_ for _ in ()).throw(AccountScopeProvisioningError("No account memberships found.")),
    )

    client = TestClient(app)
    res = client.get("/api/portfolio")

    assert res.status_code == 409
    assert res.json()["detail"] == "No account memberships found."


def test_portfolio_route_denies_shared_admin_when_legacy_disabled(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(
        portfolio_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: (_ for _ in ()).throw(
            AccountScopeDenied(
                "Shared sessions are not allowed to access account-scoped holdings while Neon account enforcement is enabled."
            )
        ),
    )

    client = TestClient(app)
    res = client.get("/api/portfolio")

    assert res.status_code == 403
    assert "Shared sessions are not allowed" in res.json()["detail"]


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
    monkeypatch.setenv("CEIORA_SESSION_SECRET", "test-secret")
    monkeypatch.setattr(
        portfolio_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_a",
            account_ids=("acct_a",),
        ),
    )
    monkeypatch.setattr(
        portfolio_routes,
        "validate_requested_account",
        lambda scope, requested_account_id: requested_account_id or "acct_a",
    )
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
        headers={"X-App-Session-Token": _signed_app_session_token()},
    )

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "ok"
    assert body["applied_upserts"] == 1
    assert body["row_results"][0]["action"] == "replace"


def test_portfolio_whatif_preview_does_not_require_operator_token_in_cloud(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(portfolio_routes.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(portfolio_routes, "preview_portfolio_whatif", lambda scenario_rows, **kwargs: {"scenario_rows": scenario_rows, "holding_deltas": [], "current": {"positions": [], "total_value": 0.0, "position_count": 0, "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}, "component_shares": {"market": 0.0, "industry": 0.0, "style": 0.0}, "factor_details": [], "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []}, "factor_catalog": []}, "hypothetical": {"positions": [], "total_value": 0.0, "position_count": 0, "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}, "component_shares": {"market": 0.0, "industry": 0.0, "style": 0.0}, "factor_details": [], "exposure_modes": {"raw": [], "sensitivity": [], "risk_contribution": []}, "factor_catalog": []}, "diff": {"total_value": 0.0, "position_count": 0, "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}, "factor_deltas": {"raw": [], "sensitivity": [], "risk_contribution": []}}, "_preview_only": True})

    client = TestClient(app)
    payload = {"scenario_rows": [{"account_id": "acct_a", "ticker": "AAA", "quantity": 20}]}

    assert client.post("/api/portfolio/whatif", json=payload).status_code == 200
    assert client.post("/api/portfolio/whatif", json=payload, headers={"X-Refresh-Token": "op-secret"}).status_code == 200
    assert client.post("/api/portfolio/whatif", json=payload, headers={"X-Operator-Token": "op-secret"}).status_code == 200


def test_portfolio_whatif_apply_requires_authenticated_session_in_cloud(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(portfolio_routes.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(portfolio_routes.config, "EDITOR_API_TOKEN", "edit-secret")
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(auth_module.config, "EDITOR_API_TOKEN", "edit-secret")
    monkeypatch.setenv("CEIORA_SESSION_SECRET", "test-secret")
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
    monkeypatch.setattr(
        portfolio_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_a",
            account_ids=("acct_a",),
        ),
    )

    client = TestClient(app)
    payload = {"scenario_rows": [{"account_id": "acct_a", "ticker": "AAA", "quantity": 20}]}

    assert client.post("/api/portfolio/whatif/apply", json=payload).status_code == 401
    assert client.post("/api/portfolio/whatif/apply", json=payload, headers={"X-Refresh-Token": "op-secret"}).status_code == 401
    assert (
        client.post(
            "/api/portfolio/whatif/apply",
            json=payload,
            headers={"X-App-Session-Token": _signed_app_session_token()},
        ).status_code
        == 200
    )
    assert client.post("/api/portfolio/whatif/apply", json=payload, headers={"X-Editor-Token": "edit-secret"}).status_code == 401
    assert client.post("/api/portfolio/whatif/apply", json=payload, headers={"X-Operator-Token": "op-secret"}).status_code == 401


def test_portfolio_whatif_apply_rejects_rows_outside_account_scope(monkeypatch) -> None:
    monkeypatch.setenv("CEIORA_SESSION_SECRET", "test-secret")
    monkeypatch.setattr(
        portfolio_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_a",
            account_ids=("acct_a",),
        ),
    )
    monkeypatch.setattr(
        portfolio_routes,
        "validate_requested_account",
        lambda scope, requested_account_id: (_ for _ in ()).throw(AccountScopeDenied("what-if denied")),
    )

    client = TestClient(app)
    res = client.post(
        "/api/portfolio/whatif/apply",
        json={"scenario_rows": [{"account_id": "acct_b", "ticker": "AAA", "quantity": 20}]},
        headers={"X-App-Session-Token": _signed_app_session_token()},
    )

    assert res.status_code == 403
    assert "what-if denied" in res.json()["detail"]


def test_portfolio_whatif_apply_requires_operator_token_without_app_session_under_enforcement(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(portfolio_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(portfolio_routes.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(portfolio_routes.config, "EDITOR_API_TOKEN", "edit-secret")
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(auth_module.config, "EDITOR_API_TOKEN", "edit-secret")
    monkeypatch.setattr(
        portfolio_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("token-only applies should not resolve app-session scope")),
    )
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
    res = client.post(
        "/api/portfolio/whatif/apply",
        json={"scenario_rows": [{"account_id": "acct_a", "ticker": "AAA", "quantity": 20}]},
        headers={"X-Editor-Token": "edit-secret"},
    )

    assert res.status_code == 401

    res = client.post(
        "/api/portfolio/whatif/apply",
        json={"scenario_rows": [{"account_id": "acct_a", "ticker": "AAA", "quantity": 20}]},
        headers={"X-Operator-Token": "op-secret"},
    )

    assert res.status_code == 401



def test_portfolio_whatif_preview_rejects_rows_outside_account_scope(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_routes.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(
        portfolio_routes,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="acct_a",
            account_ids=("acct_a",),
        ),
    )
    monkeypatch.setattr(
        portfolio_routes,
        "validate_requested_account",
        lambda scope, requested_account_id: (_ for _ in ()).throw(AccountScopeDenied("preview denied")),
    )

    client = TestClient(app)
    res = client.post(
        "/api/portfolio/whatif",
        json={"scenario_rows": [{"account_id": "acct_b", "ticker": "AAA", "quantity": 20}]},
    )

    assert res.status_code == 403
    assert "preview denied" in res.json()["detail"]
