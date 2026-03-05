from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.data.retention import prune_history_by_lookback


def _seed_table(db: Path, table: str, date_col: str) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(f"CREATE TABLE {table} ({date_col} TEXT NOT NULL, value REAL)")
    conn.executemany(
        f"INSERT INTO {table} ({date_col}, value) VALUES (?, ?)",
        [
            ("2019-01-01", 1.0),
            ("2022-01-01", 2.0),
            ("2025-01-01", 3.0),
        ],
    )
    conn.commit()
    conn.close()


def test_prune_history_by_lookback_dry_run(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"

    _seed_table(data_db, "barra_raw_cross_section_history", "as_of_date")
    _seed_table(cache_db, "daily_factor_returns", "date")

    result = prune_history_by_lookback(
        data_db=data_db,
        cache_db=cache_db,
        keep_years=3,
        as_of_date="2026-03-05",
        dry_run=True,
    )

    assert result["status"] == "ok"
    assert result["cutoff_date"] == "2023-03-06"

    data_row = next(row for row in result["data_tables"] if row["table"] == "barra_raw_cross_section_history")
    cache_row = next(row for row in result["cache_tables"] if row["table"] == "daily_factor_returns")
    assert data_row["rows_older_than_cutoff"] == 2
    assert data_row["rows_deleted"] == 0
    assert cache_row["rows_older_than_cutoff"] == 2
    assert cache_row["rows_deleted"] == 0


def test_prune_history_by_lookback_deletes_old_rows(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"

    _seed_table(data_db, "security_prices_eod", "date")
    _seed_table(cache_db, "daily_specific_residuals", "date")

    result = prune_history_by_lookback(
        data_db=data_db,
        cache_db=cache_db,
        keep_years=2,
        as_of_date="2026-03-05",
        dry_run=False,
    )

    data_row = next(row for row in result["data_tables"] if row["table"] == "security_prices_eod")
    cache_row = next(row for row in result["cache_tables"] if row["table"] == "daily_specific_residuals")
    assert data_row["rows_deleted"] == 2
    assert data_row["rows_after"] == 1
    assert cache_row["rows_deleted"] == 2
    assert cache_row["rows_after"] == 1

    data_conn = sqlite3.connect(str(data_db))
    cache_conn = sqlite3.connect(str(cache_db))
    try:
        remaining_data = data_conn.execute("SELECT COUNT(*) FROM security_prices_eod").fetchone()[0]
        remaining_cache = cache_conn.execute("SELECT COUNT(*) FROM daily_specific_residuals").fetchone()[0]
    finally:
        data_conn.close()
        cache_conn.close()

    assert remaining_data == 1
    assert remaining_cache == 1
