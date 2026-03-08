from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import holdings as holdings_route


class _FakeConn:
    def close(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_noop_position_edit_does_not_mark_dirty(monkeypatch) -> None:
    marked = {"called": False}

    monkeypatch.setattr(holdings_route, "resolve_dsn", lambda _dsn=None: "postgres://example")
    monkeypatch.setattr(holdings_route, "connect", lambda **kwargs: _FakeConn())
    monkeypatch.setattr(
        holdings_route,
        "apply_single_position_edit",
        lambda *args, **kwargs: {
            "status": "ok",
            "action": "none",
            "account_id": "main",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 10.0,
            "import_batch_id": "batch_1",
        },
    )
    monkeypatch.setattr(holdings_route, "mark_holdings_dirty", lambda **kwargs: marked.update({"called": True}))

    client = TestClient(app)
    res = client.post(
        "/api/holdings/position",
        json={
            "account_id": "main",
            "ric": "AAPL.OQ",
            "quantity": 10,
            "trigger_refresh": False,
        },
    )

    assert res.status_code == 200
    assert marked["called"] is False
