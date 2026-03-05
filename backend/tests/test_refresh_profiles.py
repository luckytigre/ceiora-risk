from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.jobs import run_model_pipeline
from backend.services.refresh_manager import _resolve_profile


def test_mode_cold_maps_to_cold_core_profile() -> None:
    assert _resolve_profile(None, "cold") == "cold-core"


def test_cold_profile_config_enables_full_rebuild_and_cache_reset() -> None:
    cfg = run_model_pipeline.PROFILE_CONFIG["cold-core"]
    assert cfg["core_policy"] == "always"
    assert cfg["serving_mode"] == "full"
    assert cfg["raw_history_policy"] == "full-daily"
    assert bool(cfg["reset_core_cache"]) is True


def test_reset_core_caches_clears_core_tables(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute("CREATE TABLE daily_factor_returns (date TEXT, factor_name TEXT, factor_return REAL)")
    conn.execute("CREATE TABLE daily_specific_residuals (date TEXT, ric TEXT, residual REAL)")
    conn.execute("CREATE TABLE daily_universe_eligibility_summary (date TEXT, exposure_n INTEGER)")
    conn.execute("CREATE TABLE daily_factor_returns_meta (key TEXT, value TEXT)")
    conn.execute("CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT, updated_at REAL)")
    conn.execute("INSERT INTO daily_factor_returns VALUES ('2026-03-03', 'Liquidity', 0.0)")
    conn.execute("INSERT INTO daily_specific_residuals VALUES ('2026-03-03', 'AAPL.OQ', 0.01)")
    conn.execute("INSERT INTO daily_universe_eligibility_summary VALUES ('2026-03-03', 100)")
    conn.execute("INSERT INTO daily_factor_returns_meta VALUES ('method_version', 'v1')")
    conn.execute("INSERT INTO cache VALUES ('risk_engine_cov', '{}', 0)")
    conn.execute("INSERT INTO cache VALUES ('risk_engine_specific_risk', '{}', 0)")
    conn.execute("INSERT INTO cache VALUES ('risk_engine_meta', '{}', 0)")
    conn.execute("INSERT INTO cache VALUES ('unrelated', '{}', 0)")
    conn.commit()
    conn.close()

    summary = run_model_pipeline._reset_core_caches(cache_db)

    assert summary["daily_factor_returns"] == 1
    assert summary["daily_specific_residuals"] == 1
    assert summary["daily_universe_eligibility_summary"] == 1
    assert summary["daily_factor_returns_meta"] == 1
    assert summary["cache_risk_engine_keys"] == 3

    conn = sqlite3.connect(str(cache_db))
    assert conn.execute("SELECT COUNT(*) FROM daily_factor_returns").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM daily_specific_residuals").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM daily_universe_eligibility_summary").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM daily_factor_returns_meta").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM cache WHERE key='unrelated'").fetchone()[0] == 1
    conn.close()
