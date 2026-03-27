from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.services import neon_mirror


class _DummyPgConn:
    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _query, _params=None) -> None:
            return None

        def fetchone(self):
            return (1,)

        def fetchall(self):
            return []

    def cursor(self):
        return self._Cursor()

    def close(self) -> None:
        return None


def _fake_pg_columns(table: str) -> list[str]:
    if table == "model_factor_returns_daily":
        return [
            "date",
            "factor_name",
            "factor_return",
            "robust_se",
            "t_stat",
            "r_squared",
            "residual_vol",
            "cross_section_n",
            "eligible_n",
            "coverage",
            "run_id",
            "updated_at",
        ]
    if table == "model_factor_covariance_daily":
        return [
            "as_of_date",
            "factor_name",
            "factor_name_2",
            "covariance",
            "run_id",
            "updated_at",
        ]
    if table == "model_specific_risk_daily":
        return [
            "as_of_date",
            "ric",
            "ticker",
            "specific_var",
            "specific_vol",
            "obs",
            "trbc_business_sector",
            "run_id",
            "updated_at",
        ]
    if table == "model_run_metadata":
        return [
            "run_id",
            "refresh_mode",
            "status",
            "started_at",
            "completed_at",
            "factor_returns_asof",
            "source_dates_json",
            "params_json",
            "risk_engine_state_json",
            "row_counts_json",
            "error_type",
            "error_message",
            "updated_at",
        ]
    return ["ric", "date"]


def test_canonical_date_key_normalizes_timestamp_text() -> None:
    assert neon_mirror._canonical_date_key("2026-03-15T17:04:51.946548+00:00") == "2026-03-15T17:04:51.946548+00:00"
    assert neon_mirror._canonical_date_key("2026-03-15 17:04:51.946548+00") == "2026-03-15T17:04:51.946548+00:00"
    assert neon_mirror._canonical_date_key("2026-03-15") == "2026-03-15"


def _create_sqlite_runtime(db_path: Path, cache_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_master (ric TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO security_master (ric) VALUES ('ABC.N')")
    conn.execute(
        """
        CREATE TABLE security_registry (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            tracking_status TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO security_registry (ric, ticker, tracking_status) VALUES ('ABC.N', 'ABC', 'active')"
    )
    conn.execute(
        """
        CREATE TABLE security_taxonomy_current (
            ric TEXT PRIMARY KEY,
            instrument_kind TEXT,
            vehicle_structure TEXT,
            model_home_market_scope TEXT,
            is_single_name_equity INTEGER,
            classification_ready INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready
        ) VALUES ('ABC.N', 'single_name_equity', 'equity_security', 'us', 1, 1)
        """
    )
    conn.execute(
        """
        CREATE TABLE security_policy_current (
            ric TEXT PRIMARY KEY,
            price_ingest_enabled INTEGER,
            pit_fundamentals_enabled INTEGER,
            pit_classification_enabled INTEGER,
            allow_cuse_native_core INTEGER,
            allow_cuse_fundamental_projection INTEGER,
            allow_cuse_returns_projection INTEGER,
            allow_cpar_core_target INTEGER,
            allow_cpar_extended_target INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target
        ) VALUES ('ABC.N', 1, 1, 1, 1, 0, 0, 1, 1)
        """
    )
    conn.execute(
        """
        CREATE TABLE security_master_compat_current (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            classification_ok INTEGER,
            is_equity_eligible INTEGER,
            coverage_role TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('ABC.N', 'ABC', 1, 1, 'native_equity', 'security_registry_seed', 'job-1', '2026-03-02T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        CREATE TABLE security_source_observation_daily (
            as_of_date TEXT,
            ric TEXT,
            classification_ready INTEGER,
            is_equity_eligible INTEGER,
            price_ingest_enabled INTEGER,
            pit_fundamentals_enabled INTEGER,
            pit_classification_enabled INTEGER,
            has_price_history_as_of_date INTEGER,
            has_fundamentals_history_as_of_date INTEGER,
            has_classification_history_as_of_date INTEGER,
            latest_price_date TEXT,
            latest_fundamentals_as_of_date TEXT,
            latest_classification_as_of_date TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_source_observation_daily (
            as_of_date, ric, classification_ready, is_equity_eligible, price_ingest_enabled,
            pit_fundamentals_enabled, pit_classification_enabled, has_price_history_as_of_date,
            has_fundamentals_history_as_of_date, has_classification_history_as_of_date,
            latest_price_date, latest_fundamentals_as_of_date, latest_classification_as_of_date,
            source, job_run_id, updated_at
        ) VALUES (
            '2026-03-01', 'ABC.N', 1, 1, 1, 1, 1, 1, 1, 1,
            '2026-03-01', '2026-03-01', '2026-03-01', 'security_registry_seed', 'job-1', '2026-03-02T00:00:00+00:00'
        )
        """
    )
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
            'job-1', 'lseg', '2026-03-01T00:00:00+00:00', '2026-03-01T00:05:00+00:00', 'ok', 'runtime seed'
        )
        """
    )
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
            'job-1', 'ABC.N', 'prices', 'ok', 'loaded', '2026-03-01T00:05:00+00:00'
        )
        """
    )
    conn.execute("CREATE TABLE security_prices_eod (ric TEXT, date TEXT)")
    conn.execute("INSERT INTO security_prices_eod (ric, date) VALUES ('ABC.N', '2026-03-01')")
    conn.execute(
        "CREATE TABLE security_fundamentals_pit (ric TEXT, as_of_date TEXT, stat_date TEXT)"
    )
    conn.execute(
        "INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date) VALUES ('ABC.N', '2026-03-01', '2025-12-31')"
    )
    conn.execute(
        "CREATE TABLE security_classification_pit (ric TEXT, as_of_date TEXT)"
    )
    conn.execute(
        "INSERT INTO security_classification_pit (ric, as_of_date) VALUES ('ABC.N', '2026-03-01')"
    )
    conn.execute(
        "CREATE TABLE barra_raw_cross_section_history (ric TEXT, as_of_date TEXT)"
    )
    conn.execute(
        "INSERT INTO barra_raw_cross_section_history (ric, as_of_date) VALUES ('ABC.N', '2026-03-01')"
    )
    conn.execute(
        "CREATE TABLE estu_membership_daily (ric TEXT, date TEXT)"
    )
    conn.execute(
        "INSERT INTO estu_membership_daily (ric, date) VALUES ('ABC.N', '2026-03-01')"
    )
    conn.execute(
        "CREATE TABLE universe_cross_section_snapshot (ric TEXT, as_of_date TEXT)"
    )
    conn.execute(
        "INSERT INTO universe_cross_section_snapshot (ric, as_of_date) VALUES ('ABC.N', '2026-03-01')"
    )
    conn.execute(
        """
        CREATE TABLE model_factor_returns_daily (
            date TEXT,
            factor_name TEXT,
            factor_return REAL,
            robust_se REAL,
            t_stat REAL,
            r_squared REAL,
            residual_vol REAL,
            cross_section_n INTEGER,
            eligible_n INTEGER,
            coverage REAL,
            run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO model_factor_returns_daily (
            date, factor_name, factor_return, robust_se, t_stat, r_squared,
            residual_vol, cross_section_n, eligible_n, coverage, run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'run-1', '2026-03-02T00:00:00+00:00')
        """,
        [
            ("2026-03-02", "Beta", 0.01, 0.005, 2.0, 0.3, 0.2, 100, 95, 0.95),
            ("2026-03-02", "Book-to-Price", -0.02, 0.010, -2.0, 0.3, 0.2, 100, 95, 0.95),
        ],
    )
    conn.execute(
        """
        CREATE TABLE model_factor_covariance_daily (
            as_of_date TEXT,
            factor_name TEXT,
            factor_name_2 TEXT,
            covariance REAL,
            run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO model_factor_covariance_daily (
            as_of_date, factor_name, factor_name_2, covariance, run_id, updated_at
        ) VALUES ('2026-03-02', 'Beta', 'Beta', 0.04, 'run-1', '2026-03-02T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        CREATE TABLE model_specific_risk_daily (
            as_of_date TEXT,
            ric TEXT,
            ticker TEXT,
            specific_var REAL,
            specific_vol REAL,
            obs INTEGER,
            trbc_business_sector TEXT,
            run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO model_specific_risk_daily (
            as_of_date, ric, ticker, specific_var, specific_vol, obs,
            trbc_business_sector, run_id, updated_at
        ) VALUES (
            '2026-03-02', 'ABC.N', 'ABC', 0.09, 0.30, 252,
            'Software', 'run-1', '2026-03-02T00:00:00+00:00'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE model_run_metadata (
            run_id TEXT PRIMARY KEY,
            refresh_mode TEXT,
            status TEXT,
            started_at TEXT,
            completed_at TEXT,
            factor_returns_asof TEXT,
            source_dates_json TEXT,
            params_json TEXT,
            risk_engine_state_json TEXT,
            row_counts_json TEXT,
            error_type TEXT,
            error_message TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO model_run_metadata (
            run_id, refresh_mode, status, started_at, completed_at,
            factor_returns_asof, source_dates_json, params_json,
            risk_engine_state_json, row_counts_json, error_type, error_message, updated_at
        ) VALUES (
            'run-1', 'core-weekly', 'ok', '2026-03-02T00:00:00+00:00', '2026-03-02T00:05:00+00:00',
            '2026-03-02', '{}', '{}', '{}', '{}', NULL, NULL, '2026-03-02T00:05:00+00:00'
        )
        """
    )
    conn.commit()
    conn.close()

    cache = sqlite3.connect(str(cache_path))
    cache.commit()
    cache.close()


def test_run_bounded_parity_audit_detects_factor_return_value_drift(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sqlite_path = tmp_path / "data.db"
    cache_path = tmp_path / "cache.db"
    _create_sqlite_runtime(sqlite_path, cache_path)

    def _fake_pg_count_window(_pg_conn, *, table: str, date_col: str | None, cutoff: str | None, distinct_col: str | None = "ric"):
        if table == "model_factor_returns_daily":
            return {
                "row_count": 2,
                "min_date": "2026-03-02",
                "max_date": "2026-03-02",
                "latest_distinct": None,
            }
        return {
            "row_count": 1,
            "min_date": "2026-03-01" if date_col else None,
            "max_date": "2026-03-01" if date_col else None,
            "latest_distinct": 1 if date_col else None,
        }

    monkeypatch.setattr(neon_mirror, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_mirror, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_mirror, "_pg_table_exists", lambda _pg_conn, _table: True)
    monkeypatch.setattr(neon_mirror, "_audit_source_sync_metadata", lambda **_kwargs: ({}, []))
    monkeypatch.setattr(
        neon_mirror,
        "_pg_columns",
        lambda _pg_conn, table: _fake_pg_columns(table),
    )
    monkeypatch.setattr(neon_mirror, "_pg_count_window", _fake_pg_count_window)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_non_null_counts",
        lambda _pg_conn, *, table, columns, date_col=None, cutoff=None: {col: 2 for col in columns} if table == "model_factor_returns_daily" else {col: 1 for col in columns},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_group_count_by_date",
        lambda _pg_conn, *, table, date_col, dates: {date: 2 for date in dates},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_factor_return_values",
        lambda _pg_conn, *, table, dates: {
            ("2026-03-02", "Beta"): (0.99, 0.005, 2.0, 0.3, 0.2, 100.0, 95.0, 0.95),
            ("2026-03-02", "Book-to-Price"): (-0.02, 0.010, -2.0, 0.3, 0.2, 100.0, 95.0, 0.95),
        },
    )

    out = neon_mirror.run_bounded_parity_audit(
        sqlite_path=sqlite_path,
        cache_path=cache_path,
        dsn="postgresql://example",
        analytics_years=5,
    )

    assert out["status"] == "mismatch"
    assert "value_mismatch:model_factor_returns_daily" in out["issues"]
    table_out = out["tables"]["model_factor_returns_daily"]
    assert table_out["value_check_status"] == "mismatch"
    assert any("Beta" in issue for issue in table_out["value_check_issues"])


def test_run_bounded_parity_audit_includes_security_ingest_tracking_tables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sqlite_path = tmp_path / "data.db"
    cache_path = tmp_path / "cache.db"
    _create_sqlite_runtime(sqlite_path, cache_path)

    monkeypatch.setattr(neon_mirror, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_mirror, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_mirror, "_pg_table_exists", lambda _pg_conn, _table: True)
    monkeypatch.setattr(neon_mirror, "_audit_source_sync_metadata", lambda **_kwargs: ({}, []))
    monkeypatch.setattr(
        neon_mirror,
        "_pg_columns",
        lambda _pg_conn, table: _fake_pg_columns(table),
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_count_window",
        lambda _pg_conn, *, table, date_col, cutoff, distinct_col="ric": (
            {
                "row_count": 1,
                "min_date": "2026-03-01T00:00:00+00:00",
                "max_date": "2026-03-01T00:00:00+00:00",
                "latest_distinct": None,
            }
            if table == "security_ingest_runs"
            else {
                "row_count": 1,
                "min_date": "2026-03-01T00:05:00+00:00",
                "max_date": "2026-03-01T00:05:00+00:00",
                "latest_distinct": 1,
            }
            if table == "security_ingest_audit"
            else {
                "row_count": 1,
                "min_date": "2026-03-01" if date_col else None,
                "max_date": "2026-03-01" if date_col else None,
                "latest_distinct": 1 if distinct_col else None,
            }
        ),
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_non_null_counts",
        lambda _pg_conn, *, table, columns, date_col=None, cutoff=None: {col: 1 for col in columns},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_group_count_by_date",
        lambda _pg_conn, *, table, date_col, dates: {date: 1 for date in dates},
    )
    monkeypatch.setattr(neon_mirror, "_pg_duplicate_key_groups", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_pit_period_health",
        lambda *_args, **_kwargs: {"periods_with_multiple_anchors": 0, "open_period_rows": 0},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_sqlite_pit_period_health",
        lambda *_args, **_kwargs: {"periods_with_multiple_anchors": 0, "open_period_rows": 0},
    )
    monkeypatch.setattr(neon_mirror, "_pit_latest_closed_anchor", lambda **_kwargs: "2026-02-27")
    monkeypatch.setattr(
        neon_mirror,
        "_pg_factor_return_values",
        lambda _pg_conn, *, table, dates: {
            ("2026-03-02", "Beta"): (0.01, 0.005, 2.0, 0.3, 0.2, 100.0, 95.0, 0.95),
            ("2026-03-02", "Book-to-Price"): (-0.02, 0.010, -2.0, 0.3, 0.2, 100.0, 95.0, 0.95),
        },
    )

    out = neon_mirror.run_bounded_parity_audit(
        sqlite_path=sqlite_path,
        cache_path=cache_path,
        dsn="postgresql://example",
        analytics_years=5,
    )

    assert "security_ingest_runs" in out["tables"]
    assert "security_ingest_audit" in out["tables"]
    assert out["tables"]["security_ingest_runs"]["status"] == "ok"
    assert out["tables"]["security_ingest_audit"]["status"] == "ok"


def test_run_bounded_parity_audit_reports_factor_return_inference_coverage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sqlite_path = tmp_path / "data.db"
    cache_path = tmp_path / "cache.db"
    _create_sqlite_runtime(sqlite_path, cache_path)

    def _fake_pg_count_window(_pg_conn, *, table: str, date_col: str | None, cutoff: str | None, distinct_col: str | None = "ric"):
        if table == "model_factor_returns_daily":
            return {
                "row_count": 2,
                "min_date": "2026-03-02",
                "max_date": "2026-03-02",
                "latest_distinct": None,
            }
        return {
            "row_count": 1,
            "min_date": "2026-03-01" if date_col else None,
            "max_date": "2026-03-01" if date_col else None,
            "latest_distinct": 1 if date_col else None,
        }

    monkeypatch.setattr(neon_mirror, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_mirror, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_mirror, "_pg_table_exists", lambda _pg_conn, _table: True)
    monkeypatch.setattr(neon_mirror, "_audit_source_sync_metadata", lambda **_kwargs: ({}, []))
    monkeypatch.setattr(
        neon_mirror,
        "_pg_columns",
        lambda _pg_conn, table: _fake_pg_columns(table),
    )
    monkeypatch.setattr(neon_mirror, "_pg_count_window", _fake_pg_count_window)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_non_null_counts",
        lambda _pg_conn, *, table, columns, date_col=None, cutoff=None: {
            **{col: 2 for col in columns},
            "robust_se": 0,
            "t_stat": 0,
        } if table == "model_factor_returns_daily" else {col: 1 for col in columns},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_group_count_by_date",
        lambda _pg_conn, *, table, date_col, dates: {date: 2 for date in dates},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_factor_return_values",
        lambda _pg_conn, *, table, dates: {
            ("2026-03-02", "Beta"): (0.01, 0.0, 0.0, 0.3, 0.2, 100.0, 95.0, 0.95),
            ("2026-03-02", "Book-to-Price"): (-0.02, 0.0, 0.0, 0.3, 0.2, 100.0, 95.0, 0.95),
        },
    )

    out = neon_mirror.run_bounded_parity_audit(
        sqlite_path=sqlite_path,
        cache_path=cache_path,
        dsn="postgresql://example",
        analytics_years=5,
    )

    assert out["status"] == "mismatch"
    assert "nonnull_mismatch:model_factor_returns_daily" in out["issues"]
    table_out = out["tables"]["model_factor_returns_daily"]
    assert table_out["source_non_null_counts"]["robust_se"] == 2
    assert table_out["target_non_null_counts"]["robust_se"] == 0


def test_run_bounded_parity_audit_allows_target_history_superset_for_model_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sqlite_path = tmp_path / "data.db"
    cache_path = tmp_path / "cache.db"
    _create_sqlite_runtime(sqlite_path, cache_path)

    def _fake_pg_count_window(_pg_conn, *, table: str, date_col: str | None, cutoff: str | None, distinct_col: str | None = "ric"):
        if table == "model_factor_returns_daily":
            if cutoff == "2026-03-02":
                return {
                    "row_count": 2,
                    "min_date": "2026-03-02",
                    "max_date": "2026-03-02",
                    "latest_distinct": None,
                }
            return {
                "row_count": 12,
                "min_date": "2026-02-01",
                "max_date": "2026-03-02",
                "latest_distinct": None,
            }
        if table == "model_specific_risk_daily":
            if cutoff == "2026-03-02":
                return {
                    "row_count": 1,
                    "min_date": "2026-03-02",
                    "max_date": "2026-03-02",
                    "latest_distinct": 1,
                }
            return {
                "row_count": 6,
                "min_date": "2026-02-20",
                "max_date": "2026-03-02",
                "latest_distinct": 1,
            }
        if table == "model_factor_covariance_daily":
            return {
                "row_count": 1,
                "min_date": "2026-03-02",
                "max_date": "2026-03-02",
                "latest_distinct": None,
            }
        if table == "model_run_metadata":
            return {
                "row_count": 1,
                "min_date": "2026-03-02T00:05:00+00:00",
                "max_date": "2026-03-02T00:05:00+00:00",
                "latest_distinct": None,
            }
        if table == "security_ingest_runs":
            return {
                "row_count": 1,
                "min_date": "2026-03-01T00:00:00+00:00",
                "max_date": "2026-03-01T00:00:00+00:00",
                "latest_distinct": None,
            }
        if table == "security_ingest_audit":
            return {
                "row_count": 1,
                "min_date": "2026-03-01T00:05:00+00:00",
                "max_date": "2026-03-01T00:05:00+00:00",
                "latest_distinct": 1,
            }
        return {
            "row_count": 1,
            "min_date": "2026-03-01" if date_col else None,
            "max_date": "2026-03-01" if date_col else None,
            "latest_distinct": 1 if date_col else None,
        }

    def _fake_pg_non_null_counts(_pg_conn, *, table: str, columns, date_col=None, cutoff=None):
        if table == "model_factor_returns_daily":
            return {col: (2 if cutoff == "2026-03-02" else 12) for col in columns}
        if table == "model_specific_risk_daily":
            return {col: (1 if cutoff == "2026-03-02" else 6) for col in columns}
        return {col: 1 for col in columns}

    monkeypatch.setattr(neon_mirror, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_mirror, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_mirror, "_pg_table_exists", lambda _pg_conn, _table: True)
    monkeypatch.setattr(neon_mirror, "_audit_source_sync_metadata", lambda **_kwargs: ({}, []))
    monkeypatch.setattr(neon_mirror, "_pg_columns", lambda _pg_conn, table: _fake_pg_columns(table))
    monkeypatch.setattr(neon_mirror, "_pg_count_window", _fake_pg_count_window)
    monkeypatch.setattr(neon_mirror, "_pg_non_null_counts", _fake_pg_non_null_counts)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_group_count_by_date",
        lambda _pg_conn, *, table, date_col, dates: {date: (2 if table == "model_factor_returns_daily" else 1) for date in dates},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_factor_return_values",
        lambda _pg_conn, *, table, dates: {
            ("2026-03-02", "Beta"): (0.01, 0.005, 2.0, 0.3, 0.2, 100.0, 95.0, 0.95),
            ("2026-03-02", "Book-to-Price"): (-0.02, 0.010, -2.0, 0.3, 0.2, 100.0, 95.0, 0.95),
        },
    )
    monkeypatch.setattr(neon_mirror, "_pg_duplicate_key_groups", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_pit_period_health",
        lambda *_args, **_kwargs: {"periods_with_multiple_anchors": 0, "open_period_rows": 0},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_sqlite_pit_period_health",
        lambda *_args, **_kwargs: {"periods_with_multiple_anchors": 0, "open_period_rows": 0},
    )
    monkeypatch.setattr(neon_mirror, "_pit_latest_closed_anchor", lambda **_kwargs: "2026-02-27")

    out = neon_mirror.run_bounded_parity_audit(
        sqlite_path=sqlite_path,
        cache_path=cache_path,
        dsn="postgresql://example",
        analytics_years=5,
    )

    assert out["status"] == "ok"
    assert out["issues"] == []
    factor_table = out["tables"]["model_factor_returns_daily"]
    assert factor_table["expected_target_history_superset"] == {
        "status": "ok",
        "source_slice_min_date": "2026-03-02",
        "target_retained_min_date": "2026-02-01",
    }
    assert factor_table["target_compare_window"]["row_count"] == 2
    spec_table = out["tables"]["model_specific_risk_daily"]
    assert spec_table["target_compare_window"]["row_count"] == 1


def test_run_bounded_parity_audit_detects_open_period_pit_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sqlite_path = tmp_path / "data.db"
    cache_path = tmp_path / "cache.db"
    _create_sqlite_runtime(sqlite_path, cache_path)

    monkeypatch.setattr(neon_mirror, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_mirror, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_mirror, "_pg_table_exists", lambda _pg_conn, _table: True)
    monkeypatch.setattr(neon_mirror, "_audit_source_sync_metadata", lambda **_kwargs: ({}, []))
    monkeypatch.setattr(
        neon_mirror,
        "_pg_count_window",
        lambda _pg_conn, *, table, date_col, cutoff, distinct_col="ric": {
            "row_count": 1,
            "min_date": "2026-03-01" if date_col else None,
            "max_date": "2026-03-01" if date_col else None,
            "latest_distinct": 1 if date_col else None,
        },
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_columns",
        lambda _pg_conn, table: _fake_pg_columns(table),
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_non_null_counts",
        lambda _pg_conn, *, table, columns, date_col=None, cutoff=None: {col: 1 for col in columns},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_group_count_by_date",
        lambda _pg_conn, *, table, date_col, dates: {date: 1 for date in dates},
    )
    monkeypatch.setattr(neon_mirror, "_pg_duplicate_key_groups", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_pit_period_health",
        lambda *_args, **_kwargs: {"periods_with_multiple_anchors": 0, "open_period_rows": 1},
    )
    monkeypatch.setattr(neon_mirror, "_pit_latest_closed_anchor", lambda **_kwargs: "2026-02-27")

    out = neon_mirror.run_bounded_parity_audit(
        sqlite_path=sqlite_path,
        dsn="postgresql://example",
        source_years=10,
    )

    assert out["status"] == "mismatch"
    assert "period_policy_violation:security_fundamentals_pit" in out["issues"]
    assert "period_policy_violation:security_classification_pit" in out["issues"]
    assert out["tables"]["security_fundamentals_pit"]["period_policy"]["source"]["open_period_rows"] == 1


def test_run_bounded_parity_audit_detects_multiple_monthly_pit_anchors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sqlite_path = tmp_path / "data.db"
    cache_path = tmp_path / "cache.db"
    _create_sqlite_runtime(sqlite_path, cache_path)
    conn = sqlite3.connect(str(sqlite_path))
    conn.execute(
        "INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date) VALUES ('ABC.N', '2026-02-15', '2025-12-31')"
    )
    conn.execute(
        "INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date) VALUES ('ABC.N', '2026-02-27', '2025-12-31')"
    )
    conn.execute(
        "INSERT INTO security_classification_pit (ric, as_of_date) VALUES ('ABC.N', '2026-02-15')"
    )
    conn.execute(
        "INSERT INTO security_classification_pit (ric, as_of_date) VALUES ('ABC.N', '2026-02-27')"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_mirror, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_mirror, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_mirror, "_pg_table_exists", lambda _pg_conn, _table: True)
    monkeypatch.setattr(neon_mirror, "_audit_source_sync_metadata", lambda **_kwargs: ({}, []))
    monkeypatch.setattr(
        neon_mirror,
        "_pg_count_window",
        lambda _pg_conn, *, table, date_col, cutoff, distinct_col="ric": {
            "row_count": 2 if table in {"security_fundamentals_pit", "security_classification_pit"} else 1,
            "min_date": "2026-02-15" if date_col else None,
            "max_date": "2026-03-01" if date_col else None,
            "latest_distinct": 1 if date_col else None,
        },
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_columns",
        lambda _pg_conn, table: _fake_pg_columns(table),
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_non_null_counts",
        lambda _pg_conn, *, table, columns, date_col=None, cutoff=None: {col: 1 for col in columns},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_group_count_by_date",
        lambda _pg_conn, *, table, date_col, dates: {date: 1 for date in dates},
    )
    monkeypatch.setattr(neon_mirror, "_pg_duplicate_key_groups", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_pit_period_health",
        lambda *_args, **_kwargs: {"periods_with_multiple_anchors": 1, "open_period_rows": 1},
    )
    monkeypatch.setattr(neon_mirror, "_pit_latest_closed_anchor", lambda **_kwargs: "2026-02-27")

    out = neon_mirror.run_bounded_parity_audit(
        sqlite_path=sqlite_path,
        dsn="postgresql://example",
        source_years=10,
    )

    assert out["status"] == "mismatch"
    assert "period_policy_violation:security_fundamentals_pit" in out["issues"]
    assert out["tables"]["security_fundamentals_pit"]["period_policy"]["source"]["periods_with_multiple_anchors"] == 1


def test_run_bounded_parity_audit_detects_duplicate_price_keys(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sqlite_path = tmp_path / "data.db"
    cache_path = tmp_path / "cache.db"
    _create_sqlite_runtime(sqlite_path, cache_path)
    conn = sqlite3.connect(str(sqlite_path))
    conn.execute("INSERT INTO security_prices_eod (ric, date) VALUES ('ABC.N', '2026-03-01')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_mirror, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_mirror, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_mirror, "_pg_table_exists", lambda _pg_conn, _table: True)
    monkeypatch.setattr(neon_mirror, "_audit_source_sync_metadata", lambda **_kwargs: ({}, []))
    monkeypatch.setattr(
        neon_mirror,
        "_pg_count_window",
        lambda _pg_conn, *, table, date_col, cutoff, distinct_col="ric": {
            "row_count": 2 if table == "security_prices_eod" else 1,
            "min_date": "2026-03-01" if date_col else None,
            "max_date": "2026-03-01" if date_col else None,
            "latest_distinct": 1 if date_col else None,
        },
    )
    monkeypatch.setattr(neon_mirror, "_pg_duplicate_key_groups", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_non_null_counts",
        lambda _pg_conn, *, table, columns, date_col=None, cutoff=None: {col: 1 for col in columns},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_pit_period_health",
        lambda *_args, **_kwargs: {"periods_with_multiple_anchors": 0, "open_period_rows": 0},
    )
    monkeypatch.setattr(neon_mirror, "_pit_latest_closed_anchor", lambda **_kwargs: "2026-02-27")

    out = neon_mirror.run_bounded_parity_audit(
        sqlite_path=sqlite_path,
        dsn="postgresql://example",
        source_years=10,
    )

    assert out["status"] == "mismatch"
    assert "duplicate_keys:security_prices_eod" in out["issues"]
    assert out["tables"]["security_prices_eod"]["duplicate_key_groups"]["source"] == 1
    assert out["tables"]["security_prices_eod"]["duplicate_key_groups"]["target"] == 1


def test_run_bounded_parity_audit_detects_missing_neon_model_run_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sqlite_path = tmp_path / "data.db"
    cache_path = tmp_path / "cache.db"
    _create_sqlite_runtime(sqlite_path, cache_path)

    monkeypatch.setattr(neon_mirror, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_mirror, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_table_exists",
        lambda _pg_conn, table: table != "model_run_metadata",
    )
    monkeypatch.setattr(neon_mirror, "_audit_source_sync_metadata", lambda **_kwargs: ({}, []))
    monkeypatch.setattr(
        neon_mirror,
        "_pg_count_window",
        lambda _pg_conn, *, table, date_col, cutoff, distinct_col="ric": {
            "row_count": 1,
            "min_date": "2026-03-01" if date_col else None,
            "max_date": "2026-03-02" if date_col else None,
            "latest_distinct": 1 if date_col else None,
        },
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_columns",
        lambda _pg_conn, table: _fake_pg_columns(table),
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_non_null_counts",
        lambda _pg_conn, *, table, columns, date_col=None, cutoff=None: {col: 1 for col in columns},
    )
    monkeypatch.setattr(
        neon_mirror,
        "_pg_group_count_by_date",
        lambda _pg_conn, *, table, date_col, dates: {date: 1 for date in dates},
    )
    monkeypatch.setattr(neon_mirror, "_pg_duplicate_key_groups", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        neon_mirror,
        "_pg_pit_period_health",
        lambda *_args, **_kwargs: {"periods_with_multiple_anchors": 0, "open_period_rows": 0},
    )
    monkeypatch.setattr(neon_mirror, "_pit_latest_closed_anchor", lambda **_kwargs: "2026-02-27")

    out = neon_mirror.run_bounded_parity_audit(
        sqlite_path=sqlite_path,
        dsn="postgresql://example",
        source_years=10,
        analytics_years=5,
    )

    assert out["status"] == "mismatch"
    assert "mismatch:model_run_metadata" in out["issues"]
    assert out["tables"]["model_run_metadata"]["status"] == "mismatch"
    assert out["tables"]["model_run_metadata"]["reason"] == "missing_target_table:model_run_metadata"
