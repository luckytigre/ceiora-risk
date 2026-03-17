from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import universe as universe_routes


def _seed_prices(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL
        )
        """
    )
    rows = [
        ("ABC.N", "2026-01-05", 10.0),  # Mon
        ("ABC.N", "2026-01-06", 11.0),  # Tue
        ("ABC.N", "2026-01-09", 12.0),  # Fri (week close)
        ("ABC.N", "2026-01-12", 13.0),  # Mon
        ("ABC.N", "2026-01-15", 14.0),  # Thu
        ("ABC.N", "2026-01-16", 15.0),  # Fri (week close)
    ]
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date, close) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_universe_history_aggregates_to_weekly_closes(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    _seed_prices(data_db)
    monkeypatch.setattr(universe_routes.universe_service, "DATA_DB", data_db)
    payload = {
        "by_ticker": {
            "ABC": {"ticker": "ABC", "ric": "ABC.N"},
        }
    }
    monkeypatch.setattr(
        universe_routes.universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: payload if name == "universe_loadings" else None,
    )
    monkeypatch.setattr(universe_routes.universe_service, "cache_get", lambda key: None)

    client = TestClient(app)
    res = client.get("/api/universe/ticker/abc/history?years=1")
    assert res.status_code == 200
    body = res.json()
    assert body["ticker"] == "ABC"
    assert body["ric"] == "ABC.N"
    assert body["years"] == 1
    assert body["_cached"] is True
    assert body["points"] == [
        {"date": "2026-01-09", "close": 12.0},
        {"date": "2026-01-16", "close": 15.0},
    ]


def test_universe_history_returns_404_for_missing_ticker(monkeypatch) -> None:
    monkeypatch.setattr(
        universe_routes.universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: {"by_ticker": {}} if name == "universe_loadings" else None,
    )
    monkeypatch.setattr(universe_routes.universe_service, "cache_get", lambda key: None)

    client = TestClient(app)
    res = client.get("/api/universe/ticker/DOES_NOT_EXIST/history")
    assert res.status_code == 404
    assert res.json()["detail"] == "Ticker not found in cached universe"
