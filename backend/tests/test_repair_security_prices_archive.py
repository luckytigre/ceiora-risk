from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.scripts import repair_security_prices_archive as repair_script


def _create_legacy_prices_db(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_registry (ric TEXT PRIMARY KEY, ticker TEXT)")
    conn.execute("INSERT INTO security_registry (ric, ticker) VALUES ('AAA.OQ', 'AAA')")
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume REAL,
            currency TEXT,
            source TEXT,
            updated_at TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (
            ric, date, open, high, low, close, adj_close, volume, currency, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAA.OQ", "2026-03-01", 10.0, 10.0, 10.0, 10.0, 10.0, 100.0, "USD", "src", "2026-03-01T01:00:00+00:00"),
            ("AAA.OQ", "2026-03-01", 11.0, 11.0, 11.0, 11.0, 11.0, 110.0, "USD", "src", "2026-03-01T02:00:00+00:00"),
            ("BBB.OQ", "2026-03-02", 20.0, 20.0, 20.0, 20.0, 20.0, 200.0, "USD", "src", "2026-03-02T01:00:00+00:00"),
        ],
    )
    conn.commit()
    conn.close()


def test_repair_security_prices_archive_rebuilds_working_copy(tmp_path: Path) -> None:
    source_db = tmp_path / "data.db"
    working_db = tmp_path / "working.db"
    _create_legacy_prices_db(source_db)

    out = repair_script.repair_security_prices_archive(
        source_db=source_db,
        working_db=working_db,
        apply_changes=False,
    )

    assert out["status"] == "ok"
    assert out["applied"] is False
    assert Path(out["working_db"]).exists()
    assert out["repair_stats"] == {
        "source_row_count": 3,
        "rebuilt_row_count": 2,
    }
    assert out["post_repair"]["status"] == "ok"
    assert out["post_repair"]["tables"]["security_prices_eod"]["count_all"] == 2

    conn = sqlite3.connect(str(working_db))
    try:
        rows = conn.execute(
            """
            SELECT ric, date, close, updated_at
            FROM security_prices_eod
            ORDER BY ric, date
            """
        ).fetchall()
        registry_rows = conn.execute("SELECT COUNT(*) FROM security_registry").fetchone()[0]
    finally:
        conn.close()

    assert registry_rows == 1
    assert rows == [
        ("AAA.OQ", "2026-03-01", 11.0, "2026-03-01T02:00:00+00:00"),
        ("BBB.OQ", "2026-03-02", 20.0, "2026-03-02T01:00:00+00:00"),
    ]


def test_repair_security_prices_archive_can_apply_swap(tmp_path: Path) -> None:
    source_db = tmp_path / "data.db"
    working_db = tmp_path / "working.db"
    backup_dir = tmp_path / "backups"
    _create_legacy_prices_db(source_db)

    out = repair_script.repair_security_prices_archive(
        source_db=source_db,
        working_db=working_db,
        backup_dir=backup_dir,
        apply_changes=True,
    )

    assert out["status"] == "ok"
    assert out["applied"] is True
    assert Path(out["backup_path"]).exists()
    assert not working_db.exists()

    conn = sqlite3.connect(str(source_db))
    try:
        row_count = conn.execute("SELECT COUNT(*) FROM security_prices_eod").fetchone()[0]
        close_value = conn.execute(
            "SELECT close FROM security_prices_eod WHERE ric = 'AAA.OQ' AND date = '2026-03-01'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert row_count == 2
    assert close_value == 11.0
