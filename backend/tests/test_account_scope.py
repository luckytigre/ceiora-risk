from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest

from backend.api.auth import AppPrincipal
from backend.api.auth import parse_app_principal
from backend.data import account_scope


class _FakeCursor:
    def __init__(
        self,
        rows: list[tuple[object, object]],
        *,
        neon_user: tuple[object, object, object, object] | None = None,
    ) -> None:
        self.rows = rows
        self.neon_user = neon_user
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.executed.append((sql, params))

    def fetchone(self):
        sql, _ = self.executed[-1]
        if "FROM neon_auth.user" in sql:
            return self.neon_user
        raise AssertionError(f"Unexpected fetchone for SQL: {sql}")

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class _FakeConn:
    def __init__(
        self,
        rows: list[tuple[object, object]],
        *,
        neon_user: tuple[object, object, object, object] | None = None,
    ) -> None:
        self.cursor_obj = _FakeCursor(rows, neon_user=neon_user)

    def cursor(self):
        return self.cursor_obj


def test_parse_app_principal_accepts_known_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CEIORA_SESSION_SECRET", "test-secret")
    payload = base64.urlsafe_b64encode(
        json.dumps(
            {
                "authProvider": "shared",
                "username": "friend@example.com",
                "isAdmin": True,
                "primary": True,
                "issuedAt": 1,
                "expiresAt": 4743856000,
            }
        ).encode("utf-8")
    ).decode("ascii").rstrip("=")
    signature = base64.urlsafe_b64encode(
        hmac.new(b"test-secret", payload.encode("utf-8"), hashlib.sha256).digest()
    ).decode("ascii").rstrip("=")
    token = f"{payload}.{signature}"
    principal = parse_app_principal(x_app_session_token=token)

    assert principal == AppPrincipal(provider="shared", subject="friend@example.com", is_admin=True)


def test_parse_app_principal_accepts_neon_session_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CEIORA_SESSION_SECRET", "test-secret")
    payload = base64.urlsafe_b64encode(
        json.dumps(
            {
                "authProvider": "neon",
                "username": "friend@example.com",
                "subject": "auth0|friend",
                "email": "friend@example.com",
                "displayName": "Friend",
                "isAdmin": False,
                "issuedAt": 1,
                "expiresAt": 4743856000,
            }
        ).encode("utf-8")
    ).decode("ascii").rstrip("=")
    signature = base64.urlsafe_b64encode(
        hmac.new(b"test-secret", payload.encode("utf-8"), hashlib.sha256).digest()
    ).decode("ascii").rstrip("=")
    principal = parse_app_principal(x_app_session_token=f"{payload}.{signature}")

    assert principal == AppPrincipal(
        provider="neon",
        subject="auth0|friend",
        is_admin=False,
        email="friend@example.com",
        display_name="Friend",
    )


def test_resolve_account_scope_disabled_allows_missing_principal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_scope.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", False)

    scope = account_scope.resolve_account_scope(_FakeConn([]), principal=None)

    assert scope.enforced is False
    assert scope.account_ids == ()


def test_resolve_account_scope_requires_principal_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_scope.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)

    with pytest.raises(account_scope.AccountScopeAuthRequired):
        account_scope.resolve_account_scope(_FakeConn([]), principal=None)


def test_resolve_account_scope_returns_memberships_for_shared_principal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_scope.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(account_scope.config, "APP_SHARED_AUTH_ACCEPT_LEGACY", True)
    conn = _FakeConn(
        [
            ("acct_b", False),
            ("acct_a", True),
        ]
    )

    scope = account_scope.resolve_account_scope(
        conn,
        principal=AppPrincipal(provider="shared", subject="friend@example.com", is_admin=False),
    )

    assert scope.enforced is True
    assert scope.default_account_id == "acct_a"
    assert scope.account_ids == ("acct_b", "acct_a")
    executed_sql, params = conn.cursor_obj.executed[0]
    assert "account_memberships" in executed_sql
    assert params == ("friend@example.com",)


def test_resolve_account_scope_denies_shared_principal_when_legacy_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_scope.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(account_scope.config, "APP_SHARED_AUTH_ACCEPT_LEGACY", False)

    with pytest.raises(account_scope.AccountScopeDenied, match="Shared sessions are not allowed"):
        account_scope.resolve_account_scope(
            _FakeConn([]),
            principal=AppPrincipal(provider="shared", subject="friend@example.com", is_admin=False),
        )


def test_resolve_account_scope_denies_shared_admin_when_legacy_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_scope.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(account_scope.config, "APP_SHARED_AUTH_ACCEPT_LEGACY", False)

    with pytest.raises(account_scope.AccountScopeDenied, match="Shared sessions are not allowed"):
        account_scope.resolve_account_scope(
            _FakeConn([]),
            principal=AppPrincipal(provider="shared", subject="friend@example.com", is_admin=True),
        )


def test_resolve_account_scope_scopes_admin_to_memberships(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_scope.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(account_scope.config, "APP_SHARED_AUTH_ACCEPT_LEGACY", True)
    conn = _FakeConn(
        [
            ("acct_admin", True),
            ("acct_shared", False),
        ],
        neon_user=("auth0|admin", "admin@example.com", "Admin", "user"),
    )

    scope = account_scope.resolve_account_scope(
        conn,
        principal=AppPrincipal(provider="neon", subject="auth0|admin", is_admin=True, email="admin@example.com"),
    )

    assert scope.is_admin is True
    assert scope.default_account_id == "acct_admin"
    assert scope.account_ids == ("acct_admin", "acct_shared")


def test_resolve_account_scope_bootstraps_neon_principal_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_scope.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    calls: list[str] = []

    def _load_memberships(_conn, *, principal):
        calls.append("load")
        if len(calls) == 1:
            return []
        return [account_scope.app_identity.MembershipRow(account_id="acct_new", is_default=True)]

    monkeypatch.setattr(account_scope.app_identity, "load_membership_rows", _load_memberships)
    monkeypatch.setattr(account_scope.app_identity, "bootstrap_personal_account", lambda _conn, *, principal: True)

    scope = account_scope.resolve_account_scope(
        _FakeConn([]),
        principal=AppPrincipal(provider="neon", subject="auth0|friend", is_admin=False, email="friend@example.com"),
    )

    assert scope.default_account_id == "acct_new"
    assert scope.account_ids == ("acct_new",)


def test_resolve_account_scope_raises_explicit_bootstrap_disabled_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_scope.config, "APP_ACCOUNT_ENFORCEMENT_ENABLED", True)
    monkeypatch.setattr(account_scope.app_identity, "load_membership_rows", lambda _conn, *, principal: [])
    monkeypatch.setattr(account_scope.app_identity, "bootstrap_personal_account", lambda _conn, *, principal: False)
    monkeypatch.setattr(account_scope.app_identity, "auth_bootstrap_enabled", lambda: False)

    with pytest.raises(account_scope.AccountScopeBootstrapDisabled, match="bootstrap is disabled"):
        account_scope.resolve_account_scope(
            _FakeConn([]),
            principal=AppPrincipal(provider="neon", subject="auth0|friend", is_admin=False, email="friend@example.com"),
        )


def test_resolve_effective_principal_uses_canonical_neon_email_and_admin_bootstrap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_scope.config, "NEON_AUTH_ALLOWED_EMAILS", ("friend@example.com",))
    monkeypatch.setattr(account_scope.config, "NEON_AUTH_BOOTSTRAP_ADMINS", ("friend@example.com",))
    principal = account_scope.resolve_effective_principal(
        _FakeConn([], neon_user=("auth0|friend", "friend@example.com", "Friend", "user")),
        principal=AppPrincipal(provider="neon", subject="auth0|friend", is_admin=False, email=None),
    )

    assert principal.email == "friend@example.com"
    assert principal.display_name == "Friend"
    assert principal.is_admin is True


def test_resolve_effective_principal_denies_neon_user_not_on_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_scope.config, "NEON_AUTH_ALLOWED_EMAILS", ("allowed@example.com",))

    with pytest.raises(account_scope.AccountScopeDenied, match="not allowlisted"):
        account_scope.resolve_effective_principal(
            _FakeConn([], neon_user=("auth0|friend", "friend@example.com", "Friend", "user")),
            principal=AppPrincipal(provider="neon", subject="auth0|friend", is_admin=False, email=None),
        )


def test_validate_requested_account_denies_outside_scope() -> None:
    scope = account_scope.AccountScope(
        enforced=True,
        is_admin=False,
        subject="friend@example.com",
        default_account_id="acct_a",
        account_ids=("acct_a",),
    )

    with pytest.raises(account_scope.AccountScopeDenied):
        account_scope.validate_requested_account(scope, "acct_b")
