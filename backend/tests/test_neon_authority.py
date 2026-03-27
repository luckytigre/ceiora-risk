from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.services import neon_authority


def _registry_first_table_stats(
    *,
    raw_history_exists: bool = True,
    raw_history_min: str | None = "2021-03-01",
    raw_history_max: str | None = "2026-03-14",
    model_covariance_exists: bool = True,
    model_run_metadata_exists: bool = True,
    prices_max: str = "2026-03-14",
    fundamentals_max: str = "2026-03-14",
    classification_max: str = "2026-03-14",
    source_observation_max: str | None = None,
) -> dict[str, dict[str, object]]:
    observation_max = source_observation_max if source_observation_max is not None else prices_max
    return {
        "security_registry": {"exists": True, "row_count": 10, "min_date": None, "max_date": None},
        "security_taxonomy_current": {"exists": True, "row_count": 10, "min_date": None, "max_date": None},
        "security_policy_current": {"exists": True, "row_count": 10, "min_date": None, "max_date": None},
        "security_source_observation_daily": {
            "exists": True,
            "row_count": 10,
            "min_date": "2020-01-01" if observation_max else None,
            "max_date": observation_max,
        },
        "security_master_compat_current": {"exists": True, "row_count": 10, "min_date": None, "max_date": None},
        "security_source_status_current": {
            "exists": True,
            "row_count": 10,
            "min_date": "2020-01-01" if observation_max else None,
            "max_date": observation_max,
        },
        "source_sync_runs": {
            "exists": True,
            "row_count": 5,
            "min_date": "2026-03-01T00:00:00+00:00",
            "max_date": "2026-03-14T00:00:00+00:00",
        },
        "source_sync_watermarks": {"exists": True, "row_count": 10, "min_date": None, "max_date": None},
        "security_prices_eod": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": prices_max},
        "security_fundamentals_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": fundamentals_max},
        "security_classification_pit": {"exists": True, "row_count": 100, "min_date": "2020-01-01", "max_date": classification_max},
        "barra_raw_cross_section_history": {
            "exists": raw_history_exists,
            "row_count": 100 if raw_history_exists else 0,
            "min_date": raw_history_min,
            "max_date": raw_history_max,
        },
        "model_factor_returns_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_factor_covariance_daily": {
            "exists": model_covariance_exists,
            "row_count": 1000 if model_covariance_exists else 0,
            "min_date": "2021-03-01" if model_covariance_exists else None,
            "max_date": "2026-03-14" if model_covariance_exists else None,
        },
        "model_specific_risk_daily": {"exists": True, "row_count": 1000, "min_date": "2021-03-01", "max_date": "2026-03-14"},
        "model_run_metadata": {
            "exists": model_run_metadata_exists,
            "row_count": 10 if model_run_metadata_exists else 0,
            "min_date": "2026-03-01T00:00:00+00:00" if model_run_metadata_exists else None,
            "max_date": "2026-03-14T00:00:00+00:00" if model_run_metadata_exists else None,
        },
    }


def test_assess_neon_rebuild_readiness_requires_raw_history_for_weekly_core() -> None:
    table_stats = _registry_first_table_stats(raw_history_exists=False, raw_history_min=None, raw_history_max=None)

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="core-weekly",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "error"
    assert "missing_table:barra_raw_cross_section_history" in out["issues"]


def test_assess_neon_rebuild_readiness_allows_cold_core_without_existing_raw_history() -> None:
    table_stats = _registry_first_table_stats(raw_history_exists=False, raw_history_min=None, raw_history_max=None)

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="cold-core",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "ok"


def test_assess_neon_rebuild_readiness_requires_model_output_tables() -> None:
    table_stats = _registry_first_table_stats(model_covariance_exists=False, model_run_metadata_exists=False)

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="cold-core",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "error"
    assert "missing_table:model_factor_covariance_daily" in out["issues"]
    assert "missing_table:model_run_metadata" in out["issues"]


def test_assess_neon_rebuild_readiness_requires_non_empty_model_output_tables() -> None:
    table_stats = _registry_first_table_stats()
    table_stats["model_factor_covariance_daily"] = {
        "exists": True,
        "row_count": 0,
        "min_date": None,
        "max_date": None,
    }
    table_stats["model_run_metadata"] = {
        "exists": True,
        "row_count": 0,
        "min_date": None,
        "max_date": None,
    }

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="core-weekly",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "error"
    assert "empty_table:model_factor_covariance_daily" in out["issues"]
    assert "empty_table:model_run_metadata" in out["issues"]


def test_assess_neon_rebuild_readiness_requires_fresh_source_observation_daily() -> None:
    table_stats = _registry_first_table_stats(
        prices_max="2026-03-14",
        fundamentals_max="2026-03-14",
        classification_max="2026-03-14",
        source_observation_max="2026-03-13",
    )

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="core-weekly",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "error"
    assert "stale_table:security_source_observation_daily:2026-03-13<2026-03-14" in out["issues"]


def test_assess_neon_rebuild_readiness_requires_source_observation_max_date() -> None:
    table_stats = _registry_first_table_stats(source_observation_max="")

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="core-weekly",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "error"
    assert "missing_max_date:security_source_observation_daily" in out["issues"]


def test_assess_neon_rebuild_readiness_requires_compat_projection_surface() -> None:
    table_stats = _registry_first_table_stats()
    table_stats["security_master_compat_current"] = {
        "exists": False,
        "row_count": 0,
        "min_date": None,
        "max_date": None,
    }

    out = neon_authority._assess_neon_rebuild_readiness(
        profile="core-weekly",
        table_stats=table_stats,
        analytics_years=5,
    )

    assert out["status"] == "error"
    assert "missing_table:security_master_compat_current" in out["issues"]


def test_assess_neon_rebuild_readiness_allows_expected_monthly_pit_lag_with_bounded_raw_history_slack(
    monkeypatch,
) -> None:
    monkeypatch.setattr(neon_authority.config, "SOURCE_DAILY_PIT_FREQUENCY", "monthly")
    table_stats = _registry_first_table_stats(
        prices_max="2026-03-17",
        fundamentals_max="2026-02-27",
        classification_max="2026-02-27",
        raw_history_min="2021-03-23",
        raw_history_max="2026-03-13",
    )

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
    table_stats = _registry_first_table_stats(
        prices_max="2026-03-17",
        fundamentals_max="2026-02-27",
        classification_max="2026-02-27",
        raw_history_min="2021-05-01",
        raw_history_max="2026-03-13",
    )

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
    source_data_conn.execute(
        "CREATE TABLE projected_instrument_meta (ric TEXT, as_of_date TEXT, projection_method TEXT)"
    )
    source_data_conn.execute(
        "INSERT INTO projected_instrument_meta VALUES ('SPY.P', '2026-03-20', 'ols_returns_regression')"
    )
    source_data_conn.execute(
        "CREATE TABLE projected_instrument_loadings (ric TEXT, ticker TEXT, as_of_date TEXT, factor_name TEXT, exposure REAL)"
    )
    source_data_conn.execute(
        "INSERT INTO projected_instrument_loadings VALUES ('SPY.P', 'SPY', '2026-03-20', 'Market', 1.0)"
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
    assert target_data_conn.execute(
        "SELECT COUNT(*) FROM projected_instrument_meta"
    ).fetchone()[0] == 1
    assert target_data_conn.execute(
        "SELECT COUNT(*) FROM projected_instrument_loadings"
    ).fetchone()[0] == 1
    target_data_conn.close()
    target_cache_conn = sqlite3.connect(str(local_cache))
    assert target_cache_conn.execute("SELECT COUNT(*) FROM daily_factor_returns").fetchone()[0] == 1
    assert target_cache_conn.execute(
        "SELECT COUNT(*) FROM cache WHERE key='risk_engine_meta'"
    ).fetchone()[0] == 1
    target_cache_conn.close()


def test_workspace_source_tables_are_registry_first() -> None:
    assert "security_registry" in neon_authority.WORKSPACE_SOURCE_TABLES
    assert "security_taxonomy_current" in neon_authority.WORKSPACE_SOURCE_TABLES
    assert "security_policy_current" in neon_authority.WORKSPACE_SOURCE_TABLES
    assert "security_source_observation_daily" in neon_authority.WORKSPACE_SOURCE_TABLES
    assert "security_master_compat_current" in neon_authority.WORKSPACE_SOURCE_TABLES
    assert "security_master" not in neon_authority.WORKSPACE_SOURCE_TABLES
    assert "model_factor_returns_daily" in neon_authority.WORKSPACE_SOURCE_TABLES


def test_prune_rebuild_workspaces_keeps_current_and_one_recent(tmp_path: Path) -> None:
    root = tmp_path / "neon_rebuild_workspace"
    root.mkdir()
    old_dir = root / "job_20260322T172131Z"
    current_dir = root / "job_20260323T032136Z"
    newest_dir = root / "job_20260323T160456Z"
    adhoc_dir = root / "adhoc"
    for path in (old_dir, current_dir, newest_dir, adhoc_dir):
        path.mkdir()
        (path / "marker.txt").write_text(path.name)

    out = neon_authority.prune_rebuild_workspaces(
        workspaces_root=root,
        keep=2,
        preserve=current_dir,
    )

    assert out["status"] == "ok"
    assert old_dir.exists() is False
    assert current_dir.exists() is True
    assert newest_dir.exists() is True
    assert adhoc_dir.exists() is True
    assert str(old_dir.resolve()) in out["removed"]
