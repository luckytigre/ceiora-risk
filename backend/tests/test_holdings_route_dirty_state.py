from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import holdings as holdings_route


def test_noop_position_edit_route_returns_service_payload(monkeypatch) -> None:
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
    )

    assert res.status_code == 200
    assert res.json()["action"] == "none"


def test_position_remove_route_returns_service_payload(monkeypatch) -> None:
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
    )

    assert res.status_code == 200
    assert res.json()["action"] == "removed"
