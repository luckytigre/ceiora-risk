from __future__ import annotations

from backend.api.auth import AppPrincipal
from backend.data import app_identity


class _ScriptedCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []
        self._last_sql = ""

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.executed.append((sql, params))
        self._last_sql = sql

    def fetchone(self):
        sql = self._last_sql
        if "RETURNING user_id" in sql:
            return ("user_1",)
        if "SELECT default_account_id" in sql:
            return (None,)
        if "SELECT account_id" in sql and "FROM account_memberships" in sql:
            return ("acct_existing",)
        raise AssertionError(f"Unexpected fetchone for SQL: {sql}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class _ScriptedConn:
    def __init__(self) -> None:
        self.cursor_obj = _ScriptedCursor()
        self.autocommit = True
        self.commit_calls = 0
        self.rollback_calls = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1


def test_bootstrap_personal_account_creates_personal_account_by_default(monkeypatch) -> None:
    monkeypatch.setattr(app_identity.config, "APP_AUTH_BOOTSTRAP_ENABLED", True)
    monkeypatch.setattr(app_identity.config, "APP_AUTH_BOOTSTRAP_REUSE_EXISTING_MEMBERSHIP", False)
    conn = _ScriptedConn()

    created = app_identity.bootstrap_personal_account(
        conn,
        principal=AppPrincipal(
            provider="neon",
            subject="auth0|friend",
            is_admin=False,
            email="friend@example.com",
            display_name="Friend",
        ),
    )

    assert created is True
    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    assert conn.autocommit is True
    executed_sql = "\n".join(sql for sql, _ in conn.cursor_obj.executed)
    assert "UPDATE account_memberships" in executed_sql
    assert "UPDATE app_users" in executed_sql
    assert "INSERT INTO holdings_accounts" in executed_sql


def test_bootstrap_personal_account_can_reuse_existing_membership_when_enabled(monkeypatch) -> None:
    monkeypatch.setattr(app_identity.config, "APP_AUTH_BOOTSTRAP_ENABLED", True)
    monkeypatch.setattr(app_identity.config, "APP_AUTH_BOOTSTRAP_REUSE_EXISTING_MEMBERSHIP", True)
    conn = _ScriptedConn()

    created = app_identity.bootstrap_personal_account(
        conn,
        principal=AppPrincipal(
            provider="neon",
            subject="auth0|friend",
            is_admin=False,
            email="friend@example.com",
            display_name="Friend",
        ),
    )

    assert created is False
    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    assert conn.autocommit is True
    executed_sql = "\n".join(sql for sql, _ in conn.cursor_obj.executed)
    assert "UPDATE account_memberships" in executed_sql
    assert "UPDATE app_users" in executed_sql
    assert "INSERT INTO holdings_accounts" not in executed_sql


class _MembershipCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.executed.append((sql, params))

    def fetchall(self):
        return [("acct_1", True)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class _MembershipConn:
    def __init__(self) -> None:
        self.cursor_obj = _MembershipCursor()

    def cursor(self):
        return self.cursor_obj


def test_load_membership_rows_for_shared_provider_matches_auth_user_id_only() -> None:
    conn = _MembershipConn()

    rows = app_identity.load_membership_rows(
        conn,
        principal=AppPrincipal(
            provider="shared",
            subject="shared-user",
            is_admin=False,
        ),
    )

    assert rows == [app_identity.MembershipRow(account_id="acct_1", is_default=True)]
    executed_sql, params = conn.cursor_obj.executed[0]
    assert "LOWER(COALESCE(u.email::text, ''))" not in executed_sql
    assert params == ("shared-user",)
