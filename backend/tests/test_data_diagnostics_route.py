from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import data as data_routes


def _seed_data_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE security_master (
            ric TEXT PRIMARY KEY,
            ticker TEXT
        )
        """
    )
    conn.execute("INSERT INTO security_master (ric, ticker) VALUES ('AAPL.OQ', 'AAPL')")
    conn.commit()
    conn.close()


def _seed_cache_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE cache (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    conn.execute("INSERT INTO cache (key, value, updated_at) VALUES ('portfolio', '{}', 0)")
    conn.commit()
    conn.close()


def test_data_diagnostics_uses_canonical_source_table_keys(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    _seed_data_db(data_db)
    _seed_cache_db(cache_db)

    monkeypatch.setattr(data_routes, "DATA_DB", data_db)
    monkeypatch.setattr(data_routes, "CACHE_DB", cache_db)
    monkeypatch.setattr(data_routes, "cache_get", lambda _key: {})

    client = TestClient(app)
    res = client.get("/api/data/diagnostics")
    assert res.status_code == 200

    body = res.json()
    source_tables = body.get("source_tables") or {}
    assert set(source_tables.keys()) == {
        "security_master",
        "security_fundamentals_pit",
        "security_classification_pit",
        "security_prices_eod",
        "estu_membership_daily",
        "barra_raw_cross_section_history",
        "universe_cross_section_snapshot",
    }
    assert "fundamental_history" not in source_tables
    assert "trbc_history" not in source_tables
    assert "price_history" not in source_tables
