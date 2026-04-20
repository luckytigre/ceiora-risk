from __future__ import annotations

import base64
import hashlib
import hmac
import json

from fastapi.testclient import TestClient

from backend.api import auth as auth_module
from backend.main import app
from backend.api.routes import holdings as holdings_route
from backend.data.account_scope import AccountScope
from backend.data.account_scope import AccountScopeDenied


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


def test_noop_position_edit_route_returns_service_payload(monkeypatch) -> None:
    monkeypatch.setenv("CEIORA_SESSION_SECRET", "test-secret")
    monkeypatch.setattr(
        holdings_route,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="main",
            account_ids=("main",),
        ),
    )
    monkeypatch.setattr(
        holdings_route,
        "validate_requested_account",
        lambda scope, requested_account_id: requested_account_id or "main",
    )
    monkeypatch.setattr(
        holdings_route.holdings_service,
        "run_position_upsert",
        lambda **kwargs: {
            "status": "ok",
            "action": "none",
            "account_id": "main",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 10.0,
            "import_batch_id": "batch_1",
        },
    )

    client = TestClient(app)
    res = client.post(
        "/api/holdings/position",
        json={
            "account_id": "main",
            "ric": "AAPL.OQ",
            "quantity": 10,
            "trigger_refresh": False,
        },
        headers={"X-App-Session-Token": _signed_app_session_token()},
    )

    assert res.status_code == 200
    assert res.json()["action"] == "none"


def test_position_remove_route_returns_service_payload(monkeypatch) -> None:
    monkeypatch.setenv("CEIORA_SESSION_SECRET", "test-secret")
    monkeypatch.setattr(
        holdings_route,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=False,
            subject="friend@example.com",
            default_account_id="main",
            account_ids=("main",),
        ),
    )
    monkeypatch.setattr(
        holdings_route,
        "validate_requested_account",
        lambda scope, requested_account_id: requested_account_id or "main",
    )
    monkeypatch.setattr(
        holdings_route.holdings_service,
        "run_position_remove",
        lambda **kwargs: {
            "status": "ok",
            "action": "removed",
            "account_id": "main",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 0.0,
            "import_batch_id": "batch_1",
        },
    )

    client = TestClient(app)
    res = client.post(
        "/api/holdings/position/remove",
        json={
            "account_id": "main",
            "ric": "AAPL.OQ",
            "trigger_refresh": False,
        },
        headers={"X-App-Session-Token": _signed_app_session_token()},
    )

    assert res.status_code == 200
    assert res.json()["action"] == "removed"


def test_holdings_positions_route_rejects_account_outside_scope(monkeypatch) -> None:
    monkeypatch.setattr(
        holdings_route,
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
        holdings_route,
        "validate_requested_account",
        lambda scope, requested_account_id: (_ for _ in ()).throw(AccountScopeDenied("out of scope")),
    )

    client = TestClient(app)
    res = client.get("/api/holdings/positions?account_id=acct_b")

    assert res.status_code == 403
    assert "out of scope" in res.json()["detail"]


def test_holdings_accounts_route_scopes_admin_reads_to_membership_accounts(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        holdings_route,
        "_resolve_holdings_scope",
        lambda **kwargs: AccountScope(
            enforced=True,
            is_admin=True,
            subject="admin@example.com",
            default_account_id="acct_admin",
            account_ids=("acct_admin", "acct_alt"),
        ),
    )
    def _load_holdings_accounts(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return []

    monkeypatch.setattr(holdings_route.holdings_service, "load_holdings_accounts", _load_holdings_accounts)

    client = TestClient(app)
    res = client.get("/api/holdings/accounts", headers={"X-App-Session-Token": _signed_app_session_token(is_admin=True)})

    assert res.status_code == 200
    assert captured["kwargs"]["allowed_account_ids"] == ("acct_admin", "acct_alt")


def test_position_edit_route_requires_operator_token_without_app_session_under_enforcement(monkeypatch) -> None:
    monkeypatch.setattr(holdings_route, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(auth_module.config, "EDITOR_API_TOKEN", "edit-secret")
    monkeypatch.setattr(
        holdings_route,
        "_resolve_holdings_scope",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("token-only writes should not resolve app-session scope")),
    )
    monkeypatch.setattr(
        holdings_route.holdings_service,
        "run_position_upsert",
        lambda **kwargs: {
            "status": "ok",
            "action": "upserted",
            "account_id": "main",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 10.0,
            "import_batch_id": "batch_1",
        },
    )

    client = TestClient(app)
    res = client.post(
        "/api/holdings/position",
        json={
            "account_id": "main",
            "ric": "AAPL.OQ",
            "quantity": 10,
            "trigger_refresh": False,
        },
        headers={"X-Editor-Token": "edit-secret"},
    )

    assert res.status_code == 401
    res = client.post(
        "/api/holdings/position",
        json={
            "account_id": "main",
            "ric": "AAPL.OQ",
            "quantity": 10,
            "trigger_refresh": False,
        },
        headers={"X-Operator-Token": "op-secret"},
    )

    assert res.status_code == 401
