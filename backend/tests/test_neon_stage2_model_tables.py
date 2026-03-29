from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from backend.services import neon_stage2


def test_canonical_tables_include_durable_model_outputs() -> None:
    tables = neon_stage2.canonical_tables()

    assert "security_registry" in tables
    assert "security_taxonomy_current" in tables
    assert "security_policy_current" in tables
    assert "security_source_observation_daily" in tables
    assert "security_master_compat_current" in tables
    assert "security_master" not in tables
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


def test_registry_first_cleanup_sql_exists_and_drops_legacy_security_master() -> None:
    cleanup_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "reference"
        / "migrations"
        / "neon"
        / "NEON_REGISTRY_FIRST_CLEANUP.sql"
    )
    cleanup_sql = cleanup_path.read_text(encoding="utf-8")

    assert "security_master" in cleanup_sql
    assert "security_master_legacy" in cleanup_sql
    assert "DROP INDEX IF EXISTS public.idx_security_master_ticker" in cleanup_sql
    assert "DROP INDEX IF EXISTS public.idx_security_master_permid" in cleanup_sql


class _DummyPgConn:
    class _Cursor:
        rowcount = 0

        class _Copy:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def write_row(self, _row) -> None:
                return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _query, _params=None) -> None:
            return None

        def executemany(self, _query, _params=None) -> None:
            return None

        def fetchone(self):
            return (1, "2026-03-01", "2026-03-01")

        def copy(self, _query):
            return self._Copy()

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
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_stage2,
        "_assert_post_load_row_counts",
        lambda *_args, **_kwargs: {"status": "ok"},
    )

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["projected_instrument_loadings"],
    )

    assert out["status"] == "ok"
    assert out["tables"]["projected_instrument_loadings"] == {
        "status": "skipped_missing_source"
    }


def test_sync_from_sqlite_to_neon_requires_declared_source_tables(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_registry (ric TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_stage2,
        "_assert_post_load_row_counts",
        lambda *_args, **_kwargs: {"status": "ok"},
    )

    with pytest.raises(RuntimeError, match="missing SQLite source tables: security_policy_current"):
        neon_stage2.sync_from_sqlite_to_neon(
            sqlite_path=db_path,
            dsn="postgresql://example",
            tables=["security_registry"],
            required_tables=["security_registry", "security_policy_current"],
        )


def test_sync_from_sqlite_to_neon_requires_nonempty_source_tables(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_registry (ric TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_stage2,
        "_assert_post_load_row_counts",
        lambda *_args, **_kwargs: {"status": "ok"},
    )

    with pytest.raises(RuntimeError, match="empty required SQLite source tables: security_registry"):
        neon_stage2.sync_from_sqlite_to_neon(
            sqlite_path=db_path,
            dsn="postgresql://example",
            tables=["security_registry"],
            required_tables=["security_registry"],
            required_nonempty_tables=["security_registry"],
        )


def test_sync_from_sqlite_to_neon_can_replace_security_master_compat_without_security_master(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE security_registry (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            tracking_status TEXT NOT NULL DEFAULT 'active'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_policy_current (
            ric TEXT PRIMARY KEY,
            price_ingest_enabled INTEGER NOT NULL DEFAULT 1,
            pit_fundamentals_enabled INTEGER NOT NULL DEFAULT 1,
            pit_classification_enabled INTEGER NOT NULL DEFAULT 1,
            allow_cuse_native_core INTEGER NOT NULL DEFAULT 1,
            allow_cuse_fundamental_projection INTEGER NOT NULL DEFAULT 0,
            allow_cuse_returns_projection INTEGER NOT NULL DEFAULT 0,
            allow_cpar_core_target INTEGER NOT NULL DEFAULT 1,
            allow_cpar_extended_target INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_taxonomy_current (
            ric TEXT PRIMARY KEY,
            instrument_kind TEXT,
            vehicle_structure TEXT,
            model_home_market_scope TEXT,
            is_single_name_equity INTEGER NOT NULL DEFAULT 1,
            classification_ready INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_master_compat_current (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            classification_ok INTEGER NOT NULL DEFAULT 0,
            is_equity_eligible INTEGER NOT NULL DEFAULT 0,
            coverage_role TEXT NOT NULL DEFAULT 'native_equity',
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status)
        VALUES ('SPY.P', 'SPY', 'active')
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target
        ) VALUES ('SPY.P', 1, 0, 0, 0, 0, 1, 0, 1)
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready
        ) VALUES ('SPY.P', 'fund_vehicle', 'projection_only_vehicle', 'us', 0, 1)
        """
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('SPY.P', 'SPY', 0, 0, 'projection_only', 'security_registry_seed', 'job_spy', '2026-03-15T00:00:00+00:00')
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_stage2,
        "_assert_post_load_row_counts",
        lambda *_args, **_kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        neon_stage2,
        "ensure_target_columns_from_sqlite",
        lambda *_args, **_kwargs: {"status": "ok", "added_columns": []},
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_columns",
        lambda _pg_conn, table: [
            "ric",
            "ticker",
            "isin",
            "exchange_name",
            "classification_ok",
            "is_equity_eligible",
            "coverage_role",
            "source",
            "job_run_id",
            "updated_at",
        ]
        if table == "security_master_compat_current"
        else ["ric"],
    )

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_master_compat_current"],
    )

    assert out["status"] == "ok"
    assert out["tables"]["security_master_compat_current"]["action"] == "truncate_and_reload"
    assert out["tables"]["security_master_compat_current"]["rows_loaded"] == 1


def test_sync_from_sqlite_to_neon_uses_declared_job_run_pk_for_security_ingest_runs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE security_ingest_runs (
            job_run_id TEXT PRIMARY KEY,
            source TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT,
            notes TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_ingest_runs (
            job_run_id, source, started_at, finished_at, status, notes
        ) VALUES (
            'job_ingest_1', 'lseg', '2026-03-15T00:00:00+00:00',
            '2026-03-15T00:05:00+00:00', 'ok', 'done'
        )
        """
    )
    conn.commit()
    conn.close()

    captured: dict[str, object] = {}
    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_stage2,
        "_assert_post_load_row_counts",
        lambda *_args, **_kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        neon_stage2,
        "ensure_target_columns_from_sqlite",
        lambda *_args, **_kwargs: {"status": "ok", "added_columns": []},
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_columns",
        lambda _pg_conn, table: [
            "job_run_id",
            "source",
            "started_at",
            "finished_at",
            "status",
            "notes",
        ]
        if table == "security_ingest_runs"
        else ["job_run_id"],
    )

    def _fake_upsert(
        _sqlite_conn,
        _pg_conn,
        *,
        table: str,
        columns: list[str],
        pk_cols: list[str],
        batch_size: int,
    ) -> int:
        captured["table"] = table
        captured["columns"] = columns
        captured["pk_cols"] = pk_cols
        captured["batch_size"] = batch_size
        captured["row_count"] = 1
        return 1

    monkeypatch.setattr(neon_stage2, "_upsert_table_on_pk", _fake_upsert)

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_ingest_runs"],
    )

    assert out["status"] == "ok"
    assert captured["table"] == "security_ingest_runs"
    assert captured["pk_cols"] == ["job_run_id"]
    assert captured["row_count"] == 1


def test_sync_from_sqlite_to_neon_uses_composite_pk_for_security_ingest_audit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE security_ingest_audit (
            job_run_id TEXT NOT NULL,
            ric TEXT NOT NULL,
            artifact_name TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (job_run_id, ric, artifact_name)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_ingest_audit (
            job_run_id, ric, artifact_name, status, detail, updated_at
        ) VALUES (
            'job_ingest_1', 'AAPL.OQ', 'prices', 'ok', 'loaded', '2026-03-15T00:05:00+00:00'
        )
        """
    )
    conn.commit()
    conn.close()

    captured: dict[str, object] = {}
    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_stage2,
        "_assert_post_load_row_counts",
        lambda *_args, **_kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        neon_stage2,
        "ensure_target_columns_from_sqlite",
        lambda *_args, **_kwargs: {"status": "ok", "added_columns": []},
    )
    monkeypatch.setattr(
        neon_stage2,
        "_pg_columns",
        lambda _pg_conn, table: [
            "job_run_id",
            "ric",
            "artifact_name",
            "status",
            "detail",
            "updated_at",
        ]
        if table == "security_ingest_audit"
        else ["job_run_id"],
    )

    def _fake_upsert(
        _sqlite_conn,
        _pg_conn,
        *,
        table: str,
        columns: list[str],
        pk_cols: list[str],
        batch_size: int,
    ) -> int:
        captured["table"] = table
        captured["columns"] = columns
        captured["pk_cols"] = pk_cols
        captured["batch_size"] = batch_size
        captured["row_count"] = 1
        return 1

    monkeypatch.setattr(neon_stage2, "_upsert_table_on_pk", _fake_upsert)

    out = neon_stage2.sync_from_sqlite_to_neon(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_ingest_audit"],
    )

    assert out["status"] == "ok"
    assert captured["table"] == "security_ingest_audit"
    assert captured["pk_cols"] == ["job_run_id", "ric", "artifact_name"]
    assert captured["row_count"] == 1


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
        "_assert_post_load_row_counts",
        lambda *_args, **_kwargs: {"status": "ok"},
    )
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
        "_assert_post_load_row_counts",
        lambda *_args, **_kwargs: {"status": "ok"},
    )
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


def test_sync_from_sqlite_to_neon_records_failed_run_when_source_integrity_check_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "data.db"
    _create_prices_sqlite(db_path)

    finalized_statuses: list[str] = []

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_stage2, "_require_source_sync_metadata_tables", lambda _pg_conn: None)
    monkeypatch.setattr(neon_stage2, "_record_source_sync_run_start", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        neon_stage2,
        "_inspect_sqlite_source_integrity",
        lambda *_args, **_kwargs: {
            "status": "failed",
            "issues": ["count_iter_mismatch:security_prices_eod:4!=3"],
        },
    )

    def _capture_finalize(_pg_conn, *, status: str, **_kwargs) -> None:
        finalized_statuses.append(status)

    monkeypatch.setattr(neon_stage2, "_finalize_source_sync_run", _capture_finalize)

    with pytest.raises(RuntimeError, match="source integrity check failed"):
        neon_stage2.sync_from_sqlite_to_neon(
            sqlite_path=db_path,
            dsn="postgresql://example",
            tables=["security_prices_eod"],
            verify_source_integrity=True,
        )

    assert finalized_statuses == ["failed"]


def test_assert_post_load_row_counts_validates_incremental_overlap_reload(monkeypatch) -> None:
    monkeypatch.setattr(neon_stage2, "_pg_count_table", lambda _pg_conn, _table: 3)

    out = neon_stage2._assert_post_load_row_counts(
        _DummyPgConn(),
        table="security_prices_eod",
        action="incremental_overlap_plus_identifier_backfill",
        target_rows_before=1,
        deleted_overlap_rows=0,
        identifier_backfill_deleted=1,
        rows_loaded=3,
    )

    assert out == {
        "status": "ok",
        "target_rows_before": 1,
        "target_rows_after": 3,
        "expected_target_rows_after": 3,
        "deleted_overlap_rows": 0,
        "identifier_backfill_deleted": 1,
    }


def test_assert_post_load_row_counts_raises_on_target_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(neon_stage2, "_pg_count_table", lambda _pg_conn, _table: 2)

    with pytest.raises(RuntimeError, match="target row mismatch for security_prices_eod"):
        neon_stage2._assert_post_load_row_counts(
            _DummyPgConn(),
            table="security_prices_eod",
            action="truncate_and_reload",
            target_rows_before=5,
            deleted_overlap_rows=0,
            identifier_backfill_deleted=0,
            rows_loaded=3,
        )
