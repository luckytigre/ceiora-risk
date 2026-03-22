from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from backend.services import neon_stage2


def test_canonical_tables_include_durable_model_outputs() -> None:
    tables = neon_stage2.canonical_tables()

    assert "model_factor_returns_daily" in tables
    assert "model_factor_covariance_daily" in tables
    assert "model_specific_risk_daily" in tables
    assert "model_run_metadata" in tables
    assert "projected_instrument_loadings" in tables
    assert "projected_instrument_meta" in tables


def test_canonical_schema_defines_durable_model_tables() -> None:
    schema_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "reference"
        / "migrations"
        / "neon"
        / "NEON_CANONICAL_SCHEMA.sql"
    )
    schema_sql = schema_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS model_factor_returns_daily" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS model_factor_covariance_daily" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS model_specific_risk_daily" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS model_run_metadata" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS projected_instrument_loadings" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS projected_instrument_meta" in schema_sql
    assert "ADD COLUMN IF NOT EXISTS run_id TEXT" in schema_sql


class _DummyPgConn:
    class _Cursor:
        rowcount = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _query, _params=None) -> None:
            return None

    def cursor(self):
        return self._Cursor()

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


def _create_prices_sqlite(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT,
            date TEXT,
            close REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date, close) VALUES (?, ?, ?)",
        [
            ("AAA.OQ", "2026-03-01", 10.0),
            ("AAA.OQ", "2026-03-15", 11.0),
            ("BBB.OQ", "2026-03-01", 20.0),
            ("BBB.OQ", "2026-03-15", 21.0),
        ],
    )
    conn.commit()
    conn.close()


def _create_pit_sqlite(db_path: Path, *, table: str) -> None:
    conn = sqlite3.connect(str(db_path))
    if table == "security_fundamentals_pit":
        conn.execute(
            """
            CREATE TABLE security_fundamentals_pit (
                ric TEXT,
                as_of_date TEXT,
                stat_date TEXT,
                market_cap REAL
            )
            """
        )
        conn.executemany(
            "INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, market_cap) VALUES (?, ?, ?, ?)",
            [
                ("AAA.OQ", "2026-01-31", "2025-12-31", 100.0),
                ("AAA.OQ", "2026-04-30", "2026-03-31", 110.0),
                ("BBB.OQ", "2026-01-31", "2025-12-31", 200.0),
                ("BBB.OQ", "2026-04-30", "2026-03-31", 210.0),
            ],
        )
    elif table == "security_classification_pit":
        conn.execute(
            """
            CREATE TABLE security_classification_pit (
                ric TEXT,
                as_of_date TEXT,
                trbc_business_sector TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO security_classification_pit (ric, as_of_date, trbc_business_sector) VALUES (?, ?, ?)",
            [
                ("AAA.OQ", "2026-01-31", "Technology Equipment"),
                ("AAA.OQ", "2026-04-30", "Technology Equipment"),
                ("BBB.OQ", "2026-01-31", "Software"),
                ("BBB.OQ", "2026-04-30", "Software"),
            ],
        )
    else:
        raise AssertionError(f"unexpected table: {table}")
    conn.commit()
    conn.close()


def test_sync_from_sqlite_to_neon_skips_missing_projection_tables(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_master (ric TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["projected_instrument_loadings"],
    )

    assert out["status"] == "ok"
    assert out["tables"]["projected_instrument_loadings"] == {
        "status": "skipped_missing_source"
    }


def test_sync_from_sqlite_to_neon_backfills_full_history_for_partially_initialized_identifiers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "data.db"
    _create_prices_sqlite(db_path)

    copied_batches: list[list[tuple[object, ...]]] = []
    deleted_entities: list[str] = []

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_stage2,
        "ensure_target_columns_from_sqlite",
        lambda *_args, **_kwargs: {"status": "ok", "added_columns": []},
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_columns",
        lambda _pg_conn, _table: ["ric", "date", "close"],
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_max_date",
        lambda _pg_conn, **_kwargs: "2026-03-17",
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_min_date",
        lambda _pg_conn, **_kwargs: "2026-03-01",
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_entity_min_dates",
        lambda _pg_conn, **_kwargs: {"AAA.OQ": "2026-03-01", "BBB.OQ": "2026-03-15"},
    )

    def _fake_delete(_pg_conn, *, table: str, entity_col: str, entities: list[str]) -> int:
        assert table == "security_prices_eod"
        assert entity_col == "ric"
        deleted_entities.extend(entities)
        return len(entities)

    def _fake_copy(_pg_conn, *, table: str, columns: list[str], pk_cols: list[str], rows) -> int:
        assert table == "security_prices_eod"
        assert columns == ["ric", "date", "close"]
        assert pk_cols == ["ric", "date"]
        batch = list(rows)
        copied_batches.append(batch)
        return len(batch)

    monkeypatch.setattr(neon_stage2, "_delete_pg_rows_for_entities", _fake_delete)
    monkeypatch.setattr(neon_stage2, "_copy_into_postgres_idempotent", _fake_copy)

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_prices_eod"],
        mode="incremental",
    )

    table_out = out["tables"]["security_prices_eod"]
    assert table_out["action"] == "incremental_overlap_plus_identifier_backfill"
    assert table_out["identifier_backfill"] == {
        "entity_col": "ric",
        "count": 1,
        "sample": ["BBB.OQ"],
        "rows_loaded": 1,
        "rows_deleted": 1,
    }
    assert table_out["source_rows"] == 2
    assert table_out["rows_loaded"] == 3
    assert deleted_entities == ["BBB.OQ"]
    assert copied_batches == [
        [
            ("AAA.OQ", "2026-03-15", 11.0),
            ("BBB.OQ", "2026-03-15", 21.0),
        ],
        [
            ("BBB.OQ", "2026-03-01", 20.0),
        ],
    ]


@pytest.mark.parametrize(
    ("table", "expected_columns", "recent_row", "historical_row"),
    [
        (
            "security_fundamentals_pit",
            ["ric", "as_of_date", "stat_date", "market_cap"],
            ("BBB.OQ", "2026-04-30", "2026-03-31", 210.0),
            ("BBB.OQ", "2026-01-31", "2025-12-31", 200.0),
        ),
        (
            "security_classification_pit",
            ["ric", "as_of_date", "trbc_business_sector"],
            ("BBB.OQ", "2026-04-30", "Software"),
            ("BBB.OQ", "2026-01-31", "Software"),
        ),
    ],
)
def test_sync_from_sqlite_to_neon_backfills_pit_history_for_partially_initialized_identifiers(
    tmp_path: Path,
    monkeypatch,
    table: str,
    expected_columns: list[str],
    recent_row: tuple[object, ...],
    historical_row: tuple[object, ...],
) -> None:
    db_path = tmp_path / "data.db"
    _create_pit_sqlite(db_path, table=table)

    copied_batches: list[list[tuple[object, ...]]] = []
    deleted_entities: list[str] = []

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_stage2,
        "ensure_target_columns_from_sqlite",
        lambda *_args, **_kwargs: {"status": "ok", "added_columns": []},
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_columns",
        lambda _pg_conn, _table: expected_columns,
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_max_date",
        lambda _pg_conn, **_kwargs: "2026-05-15",
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_min_date",
        lambda _pg_conn, **_kwargs: "2026-01-31",
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_entity_min_dates",
        lambda _pg_conn, **_kwargs: {"AAA.OQ": "2026-01-31", "BBB.OQ": "2026-04-30"},
    )

    def _fake_delete(_pg_conn, *, table: str, entity_col: str, entities: list[str]) -> int:
        assert entity_col == "ric"
        deleted_entities.extend(entities)
        return len(entities)

    def _fake_copy(_pg_conn, *, table: str, columns: list[str], pk_cols: list[str], rows) -> int:
        assert columns == expected_columns
        assert pk_cols[:2] == ["ric", "as_of_date"]
        batch = list(rows)
        copied_batches.append(batch)
        return len(batch)

    monkeypatch.setattr(neon_stage2, "_delete_pg_rows_for_entities", _fake_delete)
    monkeypatch.setattr(neon_stage2, "_copy_into_postgres_idempotent", _fake_copy)

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=[table],
        mode="incremental",
    )

    table_out = out["tables"][table]
    assert table_out["action"] == "incremental_overlap_plus_identifier_backfill"
    assert table_out["identifier_backfill"] == {
        "entity_col": "ric",
        "count": 1,
        "sample": ["BBB.OQ"],
        "rows_loaded": 1,
        "rows_deleted": 1,
    }
    assert table_out["source_rows"] == 2
    assert table_out["rows_loaded"] == 3
    assert deleted_entities == ["BBB.OQ"]
    assert copied_batches[0][-1] == recent_row
    assert copied_batches[1] == [historical_row]
