from __future__ import annotations

import sqlite3
import importlib
from pathlib import Path

import pandas as pd
import pytest

from backend.risk_model.daily_factor_returns import (
    _load_cached_dates,
    _save_daily_results_and_residuals,
    load_specific_residuals,
)
from backend.risk_model.raw_cross_section_history import ensure_raw_cross_section_history_table
from backend.universe.schema import ensure_cuse4_schema
from backend.data.cross_section_snapshot import ensure_cross_section_snapshot_table
from backend.data.model_outputs import persist_model_outputs
run_model_pipeline = importlib.import_module("backend.orchestration.run_model_pipeline")


def _pk_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows if int(r[5] or 0) > 0]


def test_security_master_migrates_to_ric_pk_and_cleans_synthetic_ids(tmp_path: Path) -> None:
    db = tmp_path / "schema.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE security_master (
            sid TEXT PRIMARY KEY,
            permid TEXT,
            ric TEXT,
            ticker TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_master (sid, permid, ric, ticker, updated_at)
        VALUES ('PERMID::AAPL.OQ', 'AAPL.OQ', 'AAPL.OQ', 'AAPL', '2026-03-04T00:00:00Z')
        """
    )
    ensure_cuse4_schema(conn)

    assert _pk_cols(conn, "security_master") == ["ric"]
    sid, permid = conn.execute(
        "SELECT sid, permid FROM security_master WHERE ric='AAPL.OQ'"
    ).fetchone()
    assert sid is None
    assert permid is None
    assert conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='security_master__legacy_pre_ric_pk'"
    ).fetchone()[0] == 0
    idx_rows = conn.execute(
        "SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_security_master_%'"
    ).fetchall()
    idx_by_name = {str(r[0]): str(r[1]) for r in idx_rows}
    assert idx_by_name.get("idx_security_master_ticker") == "security_master"
    assert idx_by_name.get("idx_security_master_permid") == "security_master"
    assert idx_by_name.get("idx_security_master_sid") == "security_master"
    conn.close()


def test_raw_cross_section_schema_rekeys_to_ric(tmp_path: Path) -> None:
    db = tmp_path / "raw.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE barra_raw_cross_section_history (
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            market_cap REAL,
            PRIMARY KEY (ticker, as_of_date)
        )
        """
    )
    ensure_raw_cross_section_history_table(conn)

    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(barra_raw_cross_section_history)").fetchall()}
    assert "ric" in cols
    assert _pk_cols(conn, "barra_raw_cross_section_history") == ["ric", "as_of_date"]
    conn.close()


def test_snapshot_schema_rekeys_to_ric(tmp_path: Path) -> None:
    db = tmp_path / "snapshot.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE universe_cross_section_snapshot (
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            market_cap REAL,
            price_exchange TEXT,
            updated_at TEXT,
            PRIMARY KEY (ticker, as_of_date)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO universe_cross_section_snapshot (ticker, as_of_date, market_cap, price_exchange, updated_at)
        VALUES ('AAPL', '2026-03-03', 1000.0, 'NASDAQ', '2026-03-04T00:00:00Z')
        """
    )

    ensure_cross_section_snapshot_table(conn)

    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(universe_cross_section_snapshot)").fetchall()}
    assert "ric" in cols
    assert "price_exchange" not in cols
    assert _pk_cols(conn, "universe_cross_section_snapshot") == ["ric", "as_of_date"]
    conn.close()


def test_prices_schema_drops_exchange_column(tmp_path: Path) -> None:
    db = tmp_path / "prices.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume REAL,
            currency TEXT,
            exchange TEXT,
            source TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (ric, date)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (
            ric, date, open, high, low, close, adj_close, volume, currency, exchange, source, updated_at
        ) VALUES (
            'AAPL.OQ', '2026-03-03', 100, 101, 99, 100, 100, 5000, 'USD', 'NASDAQ', 'test', '2026-03-04T00:00:00Z'
        )
        """
    )
    ensure_cuse4_schema(conn)

    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(security_prices_eod)").fetchall()}
    assert "exchange" not in cols
    row = conn.execute(
        """
        SELECT ric, date, open, high, low, close, adj_close, volume, currency, source
        FROM security_prices_eod
        WHERE ric='AAPL.OQ' AND date='2026-03-03'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == "AAPL.OQ"
    assert row[1] == "2026-03-03"
    assert row[8] == "USD"
    conn.close()


def test_model_outputs_quality_gate_fails_on_empty_payload(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"

    with pytest.raises(RuntimeError):
        persist_model_outputs(
            data_db=data_db,
            cache_db=cache_db,
            run_id="test_run",
            refresh_mode="light",
            status="ok",
            started_at="2026-03-04T00:00:00Z",
            completed_at="2026-03-04T00:01:00Z",
            source_dates={"exposures_asof": "2026-03-03", "fundamentals_asof": "2026-02-27"},
            params={},
            risk_engine_state={},
            cov=pd.DataFrame(),
            specific_risk_by_ticker={},
        )

    conn = sqlite3.connect(str(data_db))
    status, err_type = conn.execute(
        "SELECT status, error_type FROM model_run_metadata WHERE run_id='test_run'"
    ).fetchone()
    assert status == "failed"
    assert err_type == "quality_gate_failed"

    spec_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(model_specific_risk_daily)").fetchall()}
    assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='model_specific_residuals_daily'").fetchone()[0] == 0
    assert "ric" in spec_cols
    conn.close()


def test_model_outputs_persist_incremental_rows_only(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"

    cache_conn = sqlite3.connect(str(cache_db))
    cache_conn.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL,
            r_squared REAL NOT NULL,
            residual_vol REAL NOT NULL,
            cross_section_n INTEGER,
            eligible_n INTEGER,
            coverage REAL,
            PRIMARY KEY (date, factor_name)
        )
        """
    )
    for d in ("2026-03-02", "2026-03-03"):
        cache_conn.execute(
            """
            INSERT INTO daily_factor_returns (
                date, factor_name, factor_return, r_squared, residual_vol, cross_section_n, eligible_n, coverage
            ) VALUES (?, 'Beta', 0.01, 0.3, 0.2, 100, 95, 0.95)
            """,
            (d,),
        )
    cache_conn.commit()
    cache_conn.close()

    cov = pd.DataFrame([[1.0]], index=["Beta"], columns=["Beta"])
    spec = {
        "AAPL.OQ": {
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "specific_var": 0.02,
            "specific_vol": 0.14,
            "obs": 60,
            "trbc_industry_group": "Tech",
        }
    }

    first = persist_model_outputs(
        data_db=data_db,
        cache_db=cache_db,
        run_id="run_1",
        refresh_mode="full",
        status="ok",
        started_at="2026-03-04T00:00:00Z",
        completed_at="2026-03-04T00:01:00Z",
        source_dates={"exposures_asof": "2026-03-03", "fundamentals_asof": "2026-03-03"},
        params={},
        risk_engine_state={"factor_returns_latest_date": "2026-03-03"},
        cov=cov,
        specific_risk_by_ticker=spec,
    )
    assert first["row_counts"]["model_factor_returns_daily"] == 2
    assert "model_specific_residuals_daily" not in first["row_counts"]

    second = persist_model_outputs(
        data_db=data_db,
        cache_db=cache_db,
        run_id="run_2",
        refresh_mode="full",
        status="ok",
        started_at="2026-03-04T00:02:00Z",
        completed_at="2026-03-04T00:03:00Z",
        source_dates={"exposures_asof": "2026-03-03", "fundamentals_asof": "2026-03-03"},
        params={},
        risk_engine_state={"factor_returns_latest_date": "2026-03-03"},
        cov=cov,
        specific_risk_by_ticker=spec,
    )
    # Second write should only re-upsert the latest date slice.
    assert second["row_counts"]["model_factor_returns_daily"] == 1
    assert "model_specific_residuals_daily" not in second["row_counts"]


def test_ingest_stage_is_not_hardcoded_skip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "ORCHESTRATOR_ENABLE_INGEST", False)
    monkeypatch.setattr(run_model_pipeline, "DATA_DB", tmp_path / "data.db")

    out = run_model_pipeline._run_stage(
        stage="ingest",
        as_of_date="2026-03-03",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="test",
    )

    assert out.get("status") == "ok"
    assert out.get("mode") == "bootstrap_only"


def test_load_specific_residuals_backcompat_ric_fallback(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute(
        """
        CREATE TABLE daily_specific_residuals (
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            residual REAL NOT NULL,
            market_cap REAL NOT NULL,
            trbc_industry_group TEXT,
            PRIMARY KEY (date, ticker)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO daily_specific_residuals (date, ticker, residual, market_cap, trbc_industry_group)
        VALUES ('2026-03-03', 'MSFT.OQ', 0.01, 1000000000, 'Software')
        """
    )
    conn.commit()
    conn.close()

    df = load_specific_residuals(cache_db, lookback_days=0)
    assert not df.empty
    assert str(df.iloc[0]["ric"]).upper() == "MSFT.OQ"
    assert str(df.iloc[0]["ticker"]).upper() == "MSFT.OQ"


def test_cached_dates_require_factor_and_residual_rows(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL,
            r_squared REAL NOT NULL,
            residual_vol REAL NOT NULL,
            cross_section_n INTEGER NOT NULL DEFAULT 0,
            eligible_n INTEGER NOT NULL DEFAULT 0,
            coverage REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (date, factor_name)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daily_specific_residuals (
            date TEXT NOT NULL,
            ric TEXT NOT NULL,
            ticker TEXT NOT NULL,
            residual REAL NOT NULL,
            market_cap REAL NOT NULL DEFAULT 0.0,
            trbc_industry_group TEXT,
            PRIMARY KEY (date, ric)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daily_universe_eligibility_summary (
            date TEXT PRIMARY KEY,
            exp_date TEXT,
            exposure_n INTEGER NOT NULL DEFAULT 0,
            structural_eligible_n INTEGER NOT NULL DEFAULT 0,
            regression_member_n INTEGER NOT NULL DEFAULT 0,
            structural_coverage REAL NOT NULL DEFAULT 0.0,
            regression_coverage REAL NOT NULL DEFAULT 0.0,
            drop_pct_from_prev REAL NOT NULL DEFAULT 0.0,
            alert_level TEXT NOT NULL DEFAULT '',
            missing_style_n INTEGER NOT NULL DEFAULT 0,
            missing_market_cap_n INTEGER NOT NULL DEFAULT 0,
            missing_trbc_economic_sector_short_n INTEGER NOT NULL DEFAULT 0,
            missing_trbc_industry_n INTEGER NOT NULL DEFAULT 0,
            non_equity_n INTEGER NOT NULL DEFAULT 0,
            missing_return_n INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO daily_factor_returns
        (date, factor_name, factor_return, r_squared, residual_vol, cross_section_n, eligible_n, coverage)
        VALUES (?, 'Beta', 0.01, 0.5, 0.2, 100, 90, 0.9)
        """,
        [("2026-03-02",), ("2026-03-03",)],
    )
    conn.execute(
        """
        INSERT INTO daily_specific_residuals
        (date, ric, ticker, residual, market_cap, trbc_industry_group)
        VALUES ('2026-03-02', 'AAPL.OQ', 'AAPL', 0.01, 1000000000, 'Tech')
        """
    )
    conn.execute(
        """
        INSERT INTO daily_universe_eligibility_summary
        (date, exp_date, exposure_n, structural_eligible_n, regression_member_n, structural_coverage, regression_coverage, drop_pct_from_prev, alert_level, missing_style_n, missing_market_cap_n, missing_trbc_economic_sector_short_n, missing_trbc_industry_n, non_equity_n, missing_return_n)
        VALUES ('2026-03-02', '2026-03-02', 100, 90, 90, 0.9, 0.9, 0.0, '', 0, 0, 0, 0, 0, 0)
        """
    )
    conn.commit()
    conn.close()

    cached = _load_cached_dates(cache_db)
    assert cached == {"2026-03-02"}


def test_atomic_cache_write_rolls_back_on_residual_failure(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    results = [
        {
            "date": "2026-03-03",
            "factor_name": "Beta",
            "factor_return": 0.01,
            "r_squared": 0.2,
            "residual_vol": 0.3,
            "cross_section_n": 100,
            "eligible_n": 95,
            "coverage": 0.95,
        }
    ]
    # Intentionally invalid: ric is NULL for NOT NULL column.
    residuals = [
        {
            "date": "2026-03-03",
            "ric": None,
            "ticker": "AAPL",
            "residual": 0.01,
            "market_cap": 1_000_000_000.0,
            "trbc_industry_group": "Tech",
        }
    ]

    with pytest.raises(sqlite3.IntegrityError):
        _save_daily_results_and_residuals(cache_db, results, residuals)

    conn = sqlite3.connect(str(cache_db))
    factor_n = conn.execute("SELECT COUNT(*) FROM daily_factor_returns").fetchone()[0]
    resid_n = conn.execute("SELECT COUNT(*) FROM daily_specific_residuals").fetchone()[0]
    conn.close()

    assert factor_n == 0
    assert resid_n == 0
