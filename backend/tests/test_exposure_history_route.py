from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from backend.api.routes import exposures as exposures_routes
from backend.data import history_queries
from backend.main import app
import backend.services.cuse4_factor_history_service as cuse4_factor_history_service


def test_exposure_history_route_resolves_factor_id_to_factor_name(monkeypatch) -> None:
    catalog = [
        {
            "factor_id": "style_beta_score",
            "factor_name": "Beta",
        }
    ]

    def _load_payload(name: str):
        if name == "risk":
            return {"factor_catalog": catalog}
        return None

    monkeypatch.setattr(
        exposures_routes,
        "load_factor_history_response",
        lambda *, factor_token, years: cuse4_factor_history_service.load_factor_history_response(
            factor_token=factor_token,
            years=years,
            payload_loader=lambda name, *, fallback_loader=None: _load_payload(name),
            fallback_loader=lambda _key: None,
            history_loader=lambda cache_db, *, factor, years: ("2026-03-03", [("2026-03-02", 0.01)])
            if factor == "Beta" and years == 5
            else (None, []),
            sqlite_path="unused.db",
        ),
    )

    client = TestClient(app)
    res = client.get("/api/exposures/history?factor_id=style_beta_score&years=5")

    assert res.status_code == 200
    body = res.json()
    assert body["factor_id"] == "style_beta_score"
    assert body["factor_name"] == "Beta"
    assert body["points"][0]["factor_return"] == 0.01


def test_exposure_history_route_resolves_factor_id_from_history_when_catalog_missing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO daily_factor_returns (date, factor_name, factor_return)
        VALUES (?, ?, ?)
        """,
        [
            ("2026-03-02", "Beta", 0.01),
            ("2026-03-03", "Beta", -0.02),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(history_queries, "_use_neon_surface", lambda surface: False)
    monkeypatch.setattr(
        exposures_routes,
        "load_factor_history_response",
        lambda *, factor_token, years: cuse4_factor_history_service.load_factor_history_response(
            factor_token=factor_token,
            years=years,
            fallback_loader=lambda _key: None,
            sqlite_path=str(cache_db),
        ),
    )

    client = TestClient(app)
    res = client.get("/api/exposures/history?factor_id=style_beta_score&years=5")

    assert res.status_code == 200
    body = res.json()
    assert body["factor_id"] == "style_beta_score"
    assert body["factor_name"] == "Beta"
    assert [point["factor_return"] for point in body["points"]] == [0.01, -0.02]


def test_exposure_history_route_resolves_punctuated_industry_name_from_history(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO daily_factor_returns (date, factor_name, factor_return)
        VALUES (?, ?, ?)
        """,
        [
            ("2026-03-02", "Software & Services", 0.03),
            ("2026-03-03", "Software & Services", -0.01),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(history_queries, "_use_neon_surface", lambda surface: False)
    monkeypatch.setattr(
        exposures_routes,
        "load_factor_history_response",
        lambda *, factor_token, years: cuse4_factor_history_service.load_factor_history_response(
            factor_token=factor_token,
            years=years,
            fallback_loader=lambda _key: None,
            sqlite_path=str(cache_db),
        ),
    )

    client = TestClient(app)
    res = client.get("/api/exposures/history?factor_id=industry_software_services&years=5")

    assert res.status_code == 200
    body = res.json()
    assert body["factor_id"] == "industry_software_services"
    assert body["factor_name"] == "Software & Services"
    assert [point["factor_return"] for point in body["points"]] == [0.03, -0.01]


def test_exposure_history_route_returns_market_history_when_neon_surface_is_stale(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO daily_factor_returns (date, factor_name, factor_return)
        VALUES (?, ?, ?)
        """,
        [
            ("2026-03-02", "Market", 0.01),
            ("2026-03-03", "Market", -0.02),
        ],
    )
    conn.commit()
    conn.close()

    class _FakeCursor:
        def __init__(self) -> None:
            self._rows = []

        def execute(self, sql: str, params=None) -> None:
            if "SELECT MAX(date)::text AS latest" in sql:
                self._rows = [{"latest": "2026-03-03"}]
            else:
                self._rows = []

        def fetchone(self):
            return self._rows[0] if self._rows else {}

        def fetchall(self):
            return list(self._rows)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeConn:
        def cursor(self, row_factory=None):
            return _FakeCursor()

        def close(self) -> None:
            return None

    monkeypatch.setattr(history_queries, "_use_neon_surface", lambda surface: True)
    monkeypatch.setattr(history_queries, "_path_matches_config", lambda path, configured: True)
    monkeypatch.setattr(history_queries, "resolve_dsn", lambda _dsn=None: "postgresql://example")
    monkeypatch.setattr(history_queries, "connect", lambda dsn=None, autocommit=True: _FakeConn())
    monkeypatch.setattr(
        exposures_routes,
        "load_factor_history_response",
        lambda *, factor_token, years: cuse4_factor_history_service.load_factor_history_response(
            factor_token=factor_token,
            years=years,
            payload_loader=lambda name, *, fallback_loader=None: {"factor_catalog": [{"factor_id": "market", "factor_name": "Market"}]}
            if name == "risk"
            else None,
            fallback_loader=lambda _key: None,
            sqlite_path=str(cache_db),
        ),
    )

    client = TestClient(app)
    res = client.get("/api/exposures/history?factor_id=market&years=5")

    assert res.status_code == 200
    body = res.json()
    assert body["factor_id"] == "market"
    assert body["factor_name"] == "Market"
    assert [point["factor_return"] for point in body["points"]] == [0.01, -0.02]
