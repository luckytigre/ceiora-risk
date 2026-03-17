from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import data as data_routes

svc = data_routes.data_diagnostics_service


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


def _seed_eligibility_summary(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE daily_universe_eligibility_summary (
            date TEXT PRIMARY KEY,
            exp_date TEXT,
            exposure_n INTEGER NOT NULL DEFAULT 0,
            structural_eligible_n INTEGER NOT NULL DEFAULT 0,
            core_structural_eligible_n INTEGER NOT NULL DEFAULT 0,
            regression_member_n INTEGER NOT NULL DEFAULT 0,
            projectable_n INTEGER NOT NULL DEFAULT 0,
            projected_only_n INTEGER NOT NULL DEFAULT 0,
            structural_coverage REAL NOT NULL DEFAULT 0.0,
            regression_coverage REAL NOT NULL DEFAULT 0.0,
            projectable_coverage REAL NOT NULL DEFAULT 0.0,
            drop_pct_from_prev REAL NOT NULL DEFAULT 0.0,
            alert_level TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        INSERT INTO daily_universe_eligibility_summary (
            date, exp_date, exposure_n, structural_eligible_n, core_structural_eligible_n,
            regression_member_n, projectable_n, projected_only_n,
            structural_coverage, regression_coverage, projectable_coverage, drop_pct_from_prev, alert_level
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("2026-03-03", "2026-03-03", 120, 100, 90, 80, 95, 15, 0.83, 0.89, 0.95, 0.02, ""),
    )
    conn.commit()
    conn.close()


def test_data_diagnostics_uses_canonical_source_table_keys(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    _seed_data_db(data_db)
    _seed_cache_db(cache_db)

    monkeypatch.setattr(svc, "DATA_DB", data_db)
    monkeypatch.setattr(svc, "CACHE_DB", cache_db)
    monkeypatch.setattr(svc, "cache_get", lambda _key: {})
    monkeypatch.setattr(svc, "load_runtime_payload", lambda *_args, **_kwargs: None)

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
    assert body["truth_surfaces"]["dashboard_serving"]["source"] == "durable_serving_payloads"
    assert body["truth_surfaces"]["operator_status"]["source"] == "runtime_status_and_job_runs"
    assert body["truth_surfaces"]["local_diagnostics"]["source"] == "local_sqlite_and_cache"


def test_data_diagnostics_reports_core_and_projected_counts(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    _seed_data_db(data_db)
    _seed_cache_db(cache_db)
    _seed_eligibility_summary(cache_db)

    monkeypatch.setattr(svc, "DATA_DB", data_db)
    monkeypatch.setattr(svc, "CACHE_DB", cache_db)
    monkeypatch.setattr(svc, "cache_get", lambda _key: {})
    monkeypatch.setattr(svc, "load_runtime_payload", lambda *_args, **_kwargs: None)

    client = TestClient(app)
    res = client.get("/api/data/diagnostics")
    assert res.status_code == 200

    latest = res.json()["cross_section_usage"]["eligibility_summary"]["latest"]
    assert latest["core_structural_eligible_n"] == 90
    assert latest["regression_member_n"] == 80
    assert latest["projectable_n"] == 95
    assert latest["projected_only_n"] == 15
    assert latest["projectable_coverage_pct"] == 95.0


def test_data_diagnostics_falls_back_to_durable_truth_when_cache_tables_are_absent(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    _seed_data_db(data_db)
    _seed_cache_db(cache_db)

    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE model_factor_returns_daily (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL,
            robust_se REAL NOT NULL DEFAULT 0.0,
            t_stat REAL NOT NULL DEFAULT 0.0,
            r_squared REAL NOT NULL DEFAULT 0.0,
            residual_vol REAL NOT NULL DEFAULT 0.0,
            cross_section_n INTEGER NOT NULL DEFAULT 0,
            eligible_n INTEGER NOT NULL DEFAULT 0,
            coverage REAL NOT NULL DEFAULT 0.0,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO model_factor_returns_daily (
            date, factor_name, factor_return, robust_se, t_stat, r_squared, residual_vol,
            cross_section_n, eligible_n, coverage, run_id, updated_at
        ) VALUES (?, ?, ?, 0.0, 0.0, 0.3, 0.2, ?, ?, 0.95, 'run_1', '2026-03-13T00:00:00Z')
        """,
        [
            ("2026-03-13", "market", 0.01, 3446, 3651),
            ("2026-03-13", "style_beta_score", 0.02, 3450, 3651),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(svc, "DATA_DB", data_db)
    monkeypatch.setattr(svc, "CACHE_DB", cache_db)
    monkeypatch.setattr(svc, "cache_get", lambda _key: {})
    monkeypatch.setattr(
        svc,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: (
            {
                "date": "2026-03-13",
                "exp_date": "2026-03-13",
                "exposure_n": 3651,
                "structural_eligible_n": 3651,
                "core_structural_eligible_n": 3446,
                "regression_member_n": 3446,
                "projectable_n": 3639,
                "projected_only_n": 193,
                "structural_coverage": 1.0,
                "regression_coverage": 0.944,
                "projectable_coverage": 0.997,
                "alert_level": "",
            }
            if name == "eligibility"
            else None
        ),
    )

    client = TestClient(app)
    res = client.get("/api/data/diagnostics")
    assert res.status_code == 200
    body = res.json()

    elig = body["cross_section_usage"]["eligibility_summary"]
    assert elig["available"] is True
    assert elig["latest"]["date"] == "2026-03-13"
    assert elig["latest"]["core_structural_eligible_n"] == 3446
    assert elig["latest"]["projected_only_n"] == 193

    cross = body["cross_section_usage"]["factor_cross_section"]
    assert cross["available"] is True
    assert cross["latest"]["date"] == "2026-03-13"
    assert cross["latest"]["cross_section_n_min"] == 3446
    assert cross["latest"]["cross_section_n_max"] == 3450


def test_data_diagnostics_prefers_newer_durable_truth_over_stale_cache_tables(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    _seed_data_db(data_db)
    _seed_cache_db(cache_db)
    _seed_eligibility_summary(cache_db)

    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE model_factor_returns_daily (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL,
            robust_se REAL NOT NULL DEFAULT 0.0,
            t_stat REAL NOT NULL DEFAULT 0.0,
            r_squared REAL NOT NULL DEFAULT 0.0,
            residual_vol REAL NOT NULL DEFAULT 0.0,
            cross_section_n INTEGER NOT NULL DEFAULT 0,
            eligible_n INTEGER NOT NULL DEFAULT 0,
            coverage REAL NOT NULL DEFAULT 0.0,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO model_factor_returns_daily (
            date, factor_name, factor_return, robust_se, t_stat, r_squared, residual_vol,
            cross_section_n, eligible_n, coverage, run_id, updated_at
        ) VALUES (?, ?, ?, 0.0, 0.0, 0.3, 0.2, ?, ?, 0.95, 'run_1', '2026-03-13T00:00:00Z')
        """,
        [
            ("2026-03-13", "market", 0.01, 3446, 3651),
            ("2026-03-13", "style_beta_score", 0.02, 3450, 3651),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(svc, "DATA_DB", data_db)
    monkeypatch.setattr(svc, "CACHE_DB", cache_db)
    monkeypatch.setattr(svc, "cache_get", lambda _key: {})
    monkeypatch.setattr(
        svc,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: (
            {
                "date": "2026-03-13",
                "exp_date": "2026-03-13",
                "exposure_n": 3651,
                "structural_eligible_n": 3651,
                "core_structural_eligible_n": 3446,
                "regression_member_n": 3446,
                "projectable_n": 3639,
                "projected_only_n": 193,
                "structural_coverage": 1.0,
                "regression_coverage": 0.944,
                "projectable_coverage": 0.997,
                "alert_level": "",
            }
            if name == "eligibility"
            else None
        ),
    )

    client = TestClient(app)
    res = client.get("/api/data/diagnostics")
    assert res.status_code == 200
    body = res.json()

    elig = body["cross_section_usage"]["eligibility_summary"]
    assert elig["latest"]["date"] == "2026-03-13"
    assert elig["latest"]["core_structural_eligible_n"] == 3446
    assert elig["source"] == "durable_serving_payload:eligibility"

    cross = body["cross_section_usage"]["factor_cross_section"]
    assert cross["latest"]["date"] == "2026-03-13"
    assert cross["latest"]["cross_section_n_min"] == 3446
    assert cross["source"] == "durable_model_outputs:model_factor_returns_daily"
