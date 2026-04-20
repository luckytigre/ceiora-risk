from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api.routes import cuse_explore as cuse_explore_routes
from backend.data.account_scope import AccountScope
from backend.data.account_scope import AccountScopeAuthRequired
from backend.main import app


def test_cuse_explore_context_route_returns_account_scoped_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cuse_explore_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cuse_explore_routes,
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
            "held_positions": [],
            "_cached": False,
            "_account_scoped": True,
            "account_id": "acct_a",
        }

    monkeypatch.setattr(
        cuse_explore_routes.cuse4_explore_context_service,
        "load_cuse_explore_context_payload",
        _load_payload,
    )

    client = TestClient(app)
    res = client.get("/api/cuse/explore/context")

    assert res.status_code == 200
    assert res.json()["_account_scoped"] is True
    assert captured["kwargs"] == {
        "account_id": "acct_a",
        "allowed_account_ids": ("acct_a",),
    }


def test_cuse_explore_context_route_requires_auth_when_account_scope_enabled(monkeypatch) -> None:
    monkeypatch.setattr(cuse_explore_routes, "account_enforcement_enabled", lambda: True)
    monkeypatch.setattr(
        cuse_explore_routes,
        "resolve_holdings_scope",
        lambda **kwargs: (_ for _ in ()).throw(AccountScopeAuthRequired("Missing authenticated app session.")),
    )

    client = TestClient(app)
    res = client.get("/api/cuse/explore/context")

    assert res.status_code == 401
    assert "Missing authenticated app session." in res.json()["detail"]


def test_cuse_explore_context_route_maps_not_ready_to_503(monkeypatch) -> None:
    monkeypatch.setattr(cuse_explore_routes, "account_enforcement_enabled", lambda: False)
    monkeypatch.setattr(
        cuse_explore_routes.cuse4_explore_context_service,
        "load_cuse_explore_context_payload",
        lambda **kwargs: (_ for _ in ()).throw(
            cuse_explore_routes.cuse4_dashboard_payload_service.DashboardPayloadNotReady(
                cache_key="portfolio",
                message="Portfolio cache is not ready yet.",
            )
        ),
    )

    client = TestClient(app)
    res = client.get("/api/cuse/explore/context")

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "not_ready"
