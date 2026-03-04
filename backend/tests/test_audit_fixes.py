from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from barra.daily_factor_returns import load_specific_residuals
from barra.raw_cross_section_history import ensure_raw_cross_section_history_table
from cuse4.schema import ensure_cuse4_schema
from db.model_outputs import persist_model_outputs
from jobs import run_model_pipeline


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

    resid_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(model_specific_residuals_daily)").fetchall()}
    spec_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(model_specific_risk_daily)").fetchall()}
    assert "ric" in resid_cols
    assert "ric" in spec_cols
    conn.close()


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
