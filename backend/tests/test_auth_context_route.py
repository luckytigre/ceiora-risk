from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api.auth import AppPrincipal
from backend.app_factory import create_app
from backend.api.routes import auth_context as auth_context_route


def test_auth_context_requires_authenticated_session(monkeypatch) -> None:
    app = create_app(surface="full")
    client = TestClient(app)

    res = client.get("/api/auth/context")

    assert res.status_code == 401
    assert "Authenticated app session required" in res.json()["detail"]


def test_auth_context_returns_principal_and_scope(monkeypatch) -> None:
    monkeypatch.setattr(
        auth_context_route,
        "_resolve_auth_scope",
        lambda **kwargs: (
            AppPrincipal(
                provider="neon",
                subject="auth0|friend",
                is_admin=False,
                email="friend@example.com",
                display_name="Friend",
            ),
            auth_context_route.resolve_account_scope(
                None,
                principal=AppPrincipal(
                    provider="neon",
                    subject="auth0|friend",
                    is_admin=False,
                    email="friend@example.com",
                    display_name="Friend",
                ),
            ),
        ),
    )
    monkeypatch.setattr(auth_context_route.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", False)
    monkeypatch.setattr(auth_context_route.config, "APP_ADMIN_SETTINGS_ENABLED", True)

    app = create_app(surface="full")
    client = TestClient(app)

    res = client.get("/api/auth/context", headers={"X-App-Session-Token": "signed"})

    assert res.status_code == 200
    payload = res.json()
    assert payload == {
        "auth_provider": "neon",
        "subject": "auth0|friend",
        "email": "friend@example.com",
        "display_name": "Friend",
        "is_admin": False,
        "account_enforcement_enabled": False,
        "default_account_id": None,
        "account_ids": [],
        "admin_settings_enabled": True,
    }


def test_auth_context_surfaces_scope_error(monkeypatch) -> None:
    monkeypatch.setattr(
        auth_context_route,
        "_resolve_auth_scope",
        lambda **kwargs: (_ for _ in ()).throw(auth_context_route.AccountScopeProvisioningError("No account memberships found.")),
    )

    app = create_app(surface="full")
    client = TestClient(app)

    res = client.get("/api/auth/context", headers={"X-App-Session-Token": "signed"})

    assert res.status_code == 409
    assert res.json()["detail"] == {
        "message": "No account memberships found.",
        "code": "account_provisioning_required",
    }


def test_auth_context_surfaces_bootstrap_disabled_code(monkeypatch) -> None:
    monkeypatch.setattr(
        auth_context_route,
        "_resolve_auth_scope",
        lambda **kwargs: (_ for _ in ()).throw(
            auth_context_route.AccountScopeBootstrapDisabled(
                "No account memberships found for principal 'auth0|friend'; automatic personal workspace bootstrap is disabled."
            )
        ),
    )

    app = create_app(surface="full")
    client = TestClient(app)

    res = client.get("/api/auth/context", headers={"X-App-Session-Token": "signed"})

    assert res.status_code == 409
    assert res.json()["detail"] == {
        "message": "No account memberships found for principal 'auth0|friend'; automatic personal workspace bootstrap is disabled.",
        "code": "account_bootstrap_disabled",
    }


def test_auth_context_denies_shared_admin_when_legacy_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        auth_context_route,
        "_resolve_auth_scope",
        lambda **kwargs: (_ for _ in ()).throw(
            auth_context_route.AccountScopeDenied(
                "Shared sessions are not allowed to access account-scoped holdings while Neon account enforcement is enabled."
            )
        ),
    )

    app = create_app(surface="full")
    client = TestClient(app)

    res = client.get("/api/auth/context", headers={"X-App-Session-Token": "signed"})

    assert res.status_code == 403
    assert "Shared sessions are not allowed" in res.json()["detail"]
