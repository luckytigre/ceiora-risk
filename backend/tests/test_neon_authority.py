from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.services import neon_authority


def test_assess_neon_rebuild_readiness_requires_raw_history_for_weekly_core() -> None:
    table_stats = {
        "security_master": {"exists": True, "row_count": 10, "min_date": None, "max_date": None},
        "security_prices_eod": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-14"},
        "security_fundamentals_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-14"},
        "security_classification_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-14"},
        "barra_raw_cross_section_history": {"exists": False, "row_count": 0, "min_date": None, "max_date": None},
        "model_factor_returns_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_factor_covariance_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_specific_risk_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_run_metadata": {"exists": True, "row_count": 10, "min_date": "2026-03-01T00:00:00+00:00", "max_date": "2026-03-14T00:00:00+00:00"},
    }

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="core-weekly",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "error"
    assert "missing_table:barra_raw_cross_section_history" in out["issues"]


def test_assess_neon_rebuild_readiness_allows_cold_core_without_existing_raw_history() -> None:
    table_stats = {
        "security_master": {"exists": True, "row_count": 10, "min_date": None, "max_date": None},
        "security_prices_eod": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-14"},
        "security_fundamentals_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-14"},
        "security_classification_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-14"},
        "barra_raw_cross_section_history": {"exists": False, "row_count": 0, "min_date": None, "max_date": None},
        "model_factor_returns_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_factor_covariance_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_specific_risk_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_run_metadata": {"exists": True, "row_count": 10, "min_date": "2026-03-01T00:00:00+00:00", "max_date": "2026-03-14T00:00:00+00:00"},
    }

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="cold-core",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "ok"


def test_assess_neon_rebuild_readiness_requires_model_output_tables() -> None:
    table_stats = {
        "security_master": {"exists": True, "row_count": 10, "min_date": None, "max_date": None},
        "security_prices_eod": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-14"},
        "security_fundamentals_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-14"},
        "security_classification_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-14"},
        "barra_raw_cross_section_history": {"exists": True, "row_count": 100, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_factor_returns_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_factor_covariance_daily": {"exists": False, "row_count": 0, "min_date": None, "max_date": None},
        "model_specific_risk_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_run_metadata": {"exists": False, "row_count": 0, "min_date": None, "max_date": None},
    }

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="cold-core",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "error"
    assert "missing_table:model_factor_covariance_daily" in out["issues"]
    assert "missing_table:model_run_metadata" in out["issues"]


def test_assess_neon_rebuild_readiness_allows_expected_monthly_pit_lag_with_bounded_raw_history_slack(
    monkeypatch,
) -> None:
    monkeypatch.setattr(neon_authority.config, "SOURCE_DAILY_PIT_FREQUENCY", "monthly")
    table_stats = {
        "security_master": {"exists": True, "row_count": 10, "min_date": None, "max_date": None},
        "security_prices_eod": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-17"},
        "security_fundamentals_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-02-27"},
        "security_classification_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-02-27"},
        "barra_raw_cross_section_history": {"exists": True, "row_count": 100, "min_date": "2021-03-23", "max_date": "2026-03-13"},
        "model_factor_returns_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_factor_covariance_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_specific_risk_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_run_metadata": {"exists": True, "row_count": 10, "min_date": "2026-03-01T00:00:00+00:00", "max_date": "2026-03-14T00:00:00+00:00"},
    }

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="core-weekly",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "ok"
    assert out["issues"] == []
    assert out["source_anchor_date"] == "2026-02-27"
    assert out["history_anchor_date"] == "2026-03-13"
    assert "latest_date_mismatch:source_tables_expected_pit_lag:2026-02-27" in out["warnings"]


def test_assess_neon_rebuild_readiness_still_fails_when_raw_history_is_beyond_slack(
    monkeypatch,
) -> None:
    monkeypatch.setattr(neon_authority.config, "SOURCE_DAILY_PIT_FREQUENCY", "monthly")
    table_stats = {
        "security_master": {"exists": True, "row_count": 10, "min_date": None, "max_date": None},
        "security_prices_eod": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-03-17"},
        "security_fundamentals_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-02-27"},
        "security_classification_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": "2026-02-27"},
        "barra_raw_cross_section_history": {"exists": True, "row_count": 100, "min_date": "2021-05-01", "max_date": "2026-03-13"},
        "model_factor_returns_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_factor_covariance_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_specific_risk_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_run_metadata": {"exists": True, "row_count": 10, "min_date": "2026-03-01T00:00:00+00:00", "max_date": "2026-03-14T00:00:00+00:00"},
    }

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="core-weekly",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "error"
    assert any(item.startswith("insufficient_history:barra_raw_cross_section_history:") for item in out["issues"])


def test_sync_workspace_derivatives_to_local_mirror_copies_core_outputs(tmp_path: Path) -> None:
    workspace_data = tmp_path / "workspace_data.db"
    workspace_cache = tmp_path / "workspace_cache.db"
    local_data = tmp_path / "local_data.db"
    local_cache = tmp_path / "local_cache.db"

    source_data_conn = sqlite3.connect(str(workspace_data))
    source_data_conn.execute(
        "CREATE TABLE barra_raw_cross_section_history (ric TEXT, as_of_date TEXT, ticker TEXT)"
    )
    source_data_conn.execute(
        "INSERT INTO barra_raw_cross_section_history VALUES ('AAA.OQ', '2026-03-14', 'AAA')"
    )
    source_data_conn.commit()
    source_data_conn.close()

    source_cache_conn = sqlite3.connect(str(workspace_cache))
    source_cache_conn.execute(
        "CREATE TABLE daily_factor_returns (date TEXT, factor_name TEXT, factor_return REAL)"
    )
    source_cache_conn.execute(
        "INSERT INTO daily_factor_returns VALUES ('2026-03-14', 'Beta', 0.1)"
    )
    source_cache_conn.execute(
        "CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT, updated_at REAL)"
    )
    source_cache_conn.execute(
        "INSERT INTO cache VALUES ('risk_engine_meta', '{}', 0)"
    )
    source_cache_conn.commit()
    source_cache_conn.close()

    out = neon_authority.sync_workspace_derivatives_to_local_mirror(
        workspace_data_db=workspace_data,
        workspace_cache_db=workspace_cache,
        local_data_db=local_data,
        local_cache_db=local_cache,
    )

    assert out["status"] == "ok"
    target_data_conn = sqlite3.connect(str(local_data))
    assert target_data_conn.execute(
        "SELECT COUNT(*) FROM barra_raw_cross_section_history"
    ).fetchone()[0] == 1
    target_data_conn.close()
    target_cache_conn = sqlite3.connect(str(local_cache))
    assert target_cache_conn.execute("SELECT COUNT(*) FROM daily_factor_returns").fetchone()[0] == 1
    assert target_cache_conn.execute(
        "SELECT COUNT(*) FROM cache WHERE key='risk_engine_meta'"
    ).fetchone()[0] == 1
    target_cache_conn.close()
