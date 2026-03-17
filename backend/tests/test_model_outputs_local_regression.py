from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from backend.data import model_outputs
from backend.data.model_outputs import persist_model_outputs


def test_model_outputs_quality_gate_fails_on_empty_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(model_outputs.config, "neon_dsn", lambda: "")
    monkeypatch.setattr(model_outputs.config, "neon_primary_model_data_enabled", lambda: False)
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
    conn.close()

    assert status == "failed"
    assert err_type == "quality_gate_failed"


def test_model_outputs_persist_incremental_rows_only_without_neon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(model_outputs.config, "neon_dsn", lambda: "")
    monkeypatch.setattr(model_outputs.config, "neon_primary_model_data_enabled", lambda: False)
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"

    cache_conn = sqlite3.connect(str(cache_db))
    cache_conn.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL,
            robust_se REAL NOT NULL DEFAULT 0.0,
            t_stat REAL NOT NULL DEFAULT 0.0,
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
                date, factor_name, factor_return, robust_se, t_stat, r_squared, residual_vol, cross_section_n, eligible_n, coverage
            ) VALUES (?, 'Beta', 0.01, 0.0, 0.0, 0.3, 0.2, 100, 95, 0.95)
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
            "trbc_business_sector": "Tech",
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

    assert first["authority_store"] == "sqlite"
    assert second["authority_store"] == "sqlite"
    assert second["factor_returns_persistence_mode"] == "incremental"
    assert second["factor_returns_reload_from"] == "2026-03-03"
    assert second["row_counts"]["model_factor_returns_daily"] == 1


def test_model_outputs_load_latest_persisted_covariance_and_specific_risk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(model_outputs.config, "DATA_DB_PATH", str(tmp_path / "data.db"))
    monkeypatch.setattr(model_outputs.config, "neon_dsn", lambda: "")
    monkeypatch.setattr(model_outputs.config, "neon_primary_model_data_enabled", lambda: False)

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE model_factor_covariance_daily (
            as_of_date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_name_2 TEXT NOT NULL,
            covariance REAL NOT NULL,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (as_of_date, factor_name, factor_name_2)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE model_specific_risk_daily (
            as_of_date TEXT NOT NULL,
            ric TEXT NOT NULL,
            ticker TEXT,
            specific_var REAL NOT NULL,
            specific_vol REAL NOT NULL,
            obs INTEGER NOT NULL DEFAULT 0,
            trbc_business_sector TEXT,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (as_of_date, ric)
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO model_factor_covariance_daily (
            as_of_date, factor_name, factor_name_2, covariance, run_id, updated_at
        ) VALUES (?, ?, ?, ?, 'run_1', '2026-03-16T00:00:00Z')
        """,
        [
            ("2026-03-13", "market", "market", 0.04),
            ("2026-03-13", "market", "style_beta_score", 0.01),
            ("2026-03-13", "style_beta_score", "market", 0.01),
            ("2026-03-13", "style_beta_score", "style_beta_score", 0.09),
        ],
    )
    conn.execute(
        """
        INSERT INTO model_specific_risk_daily (
            as_of_date, ric, ticker, specific_var, specific_vol, obs, trbc_business_sector, run_id, updated_at
        ) VALUES ('2026-03-13', 'AAPL.OQ', 'AAPL', 0.01, 0.1, 60, 'Technology', 'run_1', '2026-03-16T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    cov_payload = model_outputs.load_latest_persisted_covariance_payload()
    specific_payload = model_outputs.load_latest_persisted_specific_risk_payload()

    assert cov_payload["factors"] == ["market", "style_beta_score"]
    assert cov_payload["matrix"] == [[0.04, 0.01], [0.01, 0.09]]
    assert specific_payload["AAPL.OQ"]["ticker"] == "AAPL"
    assert specific_payload["AAPL.OQ"]["specific_var"] == 0.01
