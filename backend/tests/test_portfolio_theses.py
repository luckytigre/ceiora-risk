from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from backend.api.routes import portfolio_theses as routes
from backend.data import portfolio_thesis_store as store
from backend.services import portfolio_thesis_service as service


def test_disabled_capability_never_opens_database(monkeypatch):
    monkeypatch.delenv("PORTFOLIO_THESES_ENABLED", raising=False)
    monkeypatch.setattr(service, "connect", lambda **kw: pytest.fail("Disabled feature opened a database"))
    assert service.request(subject="owner", provider="neon", operation="capability")["available"] is False
    with pytest.raises(store.ThesisUnavailable):
        service.request(subject="owner", provider="neon", operation="read")


def test_enablement_requires_owner_and_explicit_shared_mapping(monkeypatch):
    monkeypatch.setenv("PORTFOLIO_THESES_ENABLED", "true")
    monkeypatch.setenv("PORTFOLIO_THESES_OWNER_SUBJECTS", "owner")
    monkeypatch.delenv("PORTFOLIO_THESES_ALLOW_SHARED_AUTH", raising=False)
    assert service.enabled_for("owner", "neon")
    assert not service.enabled_for("other", "neon")
    assert not service.enabled_for("owner", "shared")
    monkeypatch.setenv("PORTFOLIO_THESES_ALLOW_SHARED_AUTH", "true")
    assert service.enabled_for("owner", "shared")


@pytest.mark.parametrize("qty", ["NaN", "Infinity", "-Infinity", "0.0000001", "100000000000000.000000"])
def test_wire_quantity_rejects_nonfinite_and_out_of_schema_precision(qty):
    with pytest.raises(ValidationError):
        routes.Allocation(thesis_id=uuid4(), quantity=qty)


def test_wire_quantity_preserves_decimal_and_revision_is_strict():
    row = routes.Allocation(thesis_id=uuid4(), quantity="-0.000001")
    assert row.quantity == Decimal("-0.000001")
    for value in [True, 1.1, "1", -1]:
        with pytest.raises(ValidationError):
            routes.RevisionRequest(expected_revision=value)


def test_thesis_routes_require_app_session_even_when_disabled(monkeypatch):
    monkeypatch.setattr(service, "request", lambda **kw: pytest.fail("Anonymous route reached service"))
    for call in [lambda: routes.capability(None), lambda: routes.get_state(None), lambda: routes.create(routes.NameRequest(name="Longs", expected_revision=0), None)]:
        with pytest.raises(HTTPException) as exc:
            call()
        assert exc.value.status_code == 401


def test_domain_error_is_not_masked_as_backend_outage(monkeypatch):
    monkeypatch.setattr(routes, "require_authenticated_session", lambda **kw: SimpleNamespace(subject="owner", provider="neon"))
    monkeypatch.setattr(service, "enabled_for", lambda *a: False)
    def conflict(**kw):
        raise store.ThesisConflict("Refresh and retry.")
    monkeypatch.setattr(service, "request", conflict)
    with pytest.raises(HTTPException) as exc:
        routes.get_state("test-token")
    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "stale_thesis_revision"


def test_owned_request_rolls_back_and_closes_on_failure(monkeypatch):
    actions = []
    conn = SimpleNamespace(commit=lambda: actions.append("commit"), rollback=lambda: actions.append("rollback"), close=lambda: actions.append("close"))
    monkeypatch.setattr(service, "enabled_for", lambda *a: True)
    monkeypatch.setattr(service, "resolve_dsn", lambda _: "isolated-test")
    monkeypatch.setattr(store, "open_workspace", lambda *a, **kw: ("workspace", ["account"]))
    monkeypatch.setattr(store, "assert_revision", lambda *a: actions.append("revision"))
    def deny(*a, **kw):
        raise store.ThesisDenied("Foreign account")
    monkeypatch.setattr(store, "replace_allocations", deny)
    with pytest.raises(store.ThesisDenied):
        service.request(subject="owner", provider="neon", operation="allocate", payload={"expected_revision": 1, "account_id": "account", "ric": "X.O", "allocations": []}, connection_factory=lambda **kw: conn)
    assert actions == ["revision", "rollback", "close"]


def test_serving_router_does_not_expose_thesis_writes_on_control_surface():
    from backend.api.router_registry import routers_for_surface
    assert routes.router in routers_for_surface("serve")
    assert routes.router not in routers_for_surface("control")


def test_enabled_request_honors_canonical_principal_denial(monkeypatch):
    from backend.data.account_scope import AccountScopeDenied
    closed = []
    monkeypatch.setattr(routes, "require_authenticated_session", lambda **kw: SimpleNamespace(subject="owner",provider="neon"))
    monkeypatch.setattr(service, "enabled_for", lambda *a: True)
    monkeypatch.setattr(service, "resolve_dsn", lambda _: "isolated")
    monkeypatch.setattr(service, "connect", lambda **kw: SimpleNamespace(close=lambda: closed.append(True)))
    def deny(*args, **kw):
        raise AccountScopeDenied("Revoked allowlist")
    monkeypatch.setattr(service, "resolve_effective_principal", deny)
    monkeypatch.setattr(service, "request", lambda **kw: pytest.fail("Denied session reached owned workspace"))
    with pytest.raises(HTTPException) as exc:
        routes.get_state("test-token")
    assert exc.value.status_code == 403
    assert closed == [True]
