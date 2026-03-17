from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.data import history_queries
from backend.data.history_queries import load_factor_return_history, load_price_history_rows


def test_load_factor_return_history_returns_latest_and_rows(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(db))
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
        "INSERT INTO daily_factor_returns (date, factor_name, factor_return) VALUES (?, ?, ?)",
        [
            ("2026-01-02", "Beta", 0.01),
            ("2026-01-03", "Beta", -0.02),
            ("2026-01-03", "Book-to-Price", 0.03),
        ],
    )
    conn.commit()
    conn.close()

    latest, rows = load_factor_return_history(db, factor="Beta", years=1)
    assert latest == "2026-01-03"
    assert rows == [("2026-01-02", 0.01), ("2026-01-03", -0.02)]


def test_load_price_history_rows_returns_latest_and_rows(tmp_path: Path) -> None:
    db = tmp_path / "data.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date, close) VALUES (?, ?, ?)",
        [
            ("ABC.N", "2026-01-02", 10.0),
            ("ABC.N", "2026-01-03", 11.0),
            ("XYZ.N", "2026-01-03", 12.0),
        ],
    )
    conn.commit()
    conn.close()

    latest, rows = load_price_history_rows(db, ric="ABC.N", years=1)
    assert latest == "2026-01-03"
    assert rows == [("2026-01-02", 10.0), ("2026-01-03", 11.0)]


def test_load_factor_return_history_falls_back_to_sqlite_when_neon_has_no_factor_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(db))
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
        "INSERT INTO daily_factor_returns (date, factor_name, factor_return) VALUES (?, ?, ?)",
        [
            ("2026-01-02", "Market", 0.01),
            ("2026-01-03", "Market", -0.02),
        ],
    )
    conn.commit()
    conn.close()

    class _FakeCursor:
        def __init__(self) -> None:
            self._rows = []

        def execute(self, sql: str, params=None) -> None:
            if "SELECT MAX(date)::text AS latest" in sql:
                self._rows = [{"latest": "2026-01-03"}]
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

    latest, rows = load_factor_return_history(db, factor="Market", years=5)

    assert latest == "2026-01-03"
    assert rows == [("2026-01-02", 0.01), ("2026-01-03", -0.02)]
