from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import operator as operator_route


def test_operator_status_route_returns_lane_matrix(monkeypatch) -> None:
    monkeypatch.setattr(
        operator_route,
        "profile_catalog",
        lambda: [
            {
                "profile": "serve-refresh",
                "label": "Serve Refresh",
                "description": "Rebuild frontend-facing caches only.",
                "core_policy": "never",
                "serving_mode": "light",
                "raw_history_policy": "none",
                "reset_core_cache": False,
                "default_stages": ["serving_refresh"],
                "enable_ingest": False,
                "aliases": [],
            }
        ],
    )
    monkeypatch.setattr(
        operator_route.job_runs,
        "latest_run_summary_by_profile",
        lambda **kwargs: {
            "serve-refresh": {
                "run_id": "job_123",
                "profile": "serve-refresh",
                "status": "ok",
                "started_at": "2026-03-08T12:00:00+00:00",
                "finished_at": "2026-03-08T12:01:00+00:00",
                "updated_at": "2026-03-08T12:01:00+00:00",
                "duration_seconds": 60.0,
                "stage_count": 1,
                "completed_stage_count": 1,
                "failed_stage_count": 0,
                "running_stage_count": 0,
                "stage_duration_seconds_total": 60.0,
                "slowest_stage": {"stage_name": "serving_refresh", "duration_seconds": 60.0},
                "stages": [],
            }
        },
    )
    monkeypatch.setattr(
        operator_route.job_runs,
        "recent_run_summaries_by_profile",
        lambda **kwargs: {
            "serve-refresh": [
                {
                    "run_id": "job_123",
                    "profile": "serve-refresh",
                    "status": "ok",
                    "started_at": "2026-03-08T12:00:00+00:00",
                    "finished_at": "2026-03-08T12:01:00+00:00",
                    "updated_at": "2026-03-08T12:01:00+00:00",
                    "duration_seconds": 60.0,
                    "stage_count": 1,
                    "completed_stage_count": 1,
                    "failed_stage_count": 0,
                    "running_stage_count": 0,
                    "stage_duration_seconds_total": 60.0,
                    "slowest_stage": {"stage_name": "serving_refresh", "duration_seconds": 60.0},
                    "stages": [],
                },
                {
                    "run_id": "job_122",
                    "profile": "serve-refresh",
                    "status": "ok",
                    "started_at": "2026-03-07T12:00:00+00:00",
                    "finished_at": "2026-03-07T12:01:30+00:00",
                    "updated_at": "2026-03-07T12:01:30+00:00",
                    "duration_seconds": 90.0,
                    "stage_count": 1,
                    "completed_stage_count": 1,
                    "failed_stage_count": 0,
                    "running_stage_count": 0,
                    "stage_duration_seconds_total": 90.0,
                    "slowest_stage": {"stage_name": "serving_refresh", "duration_seconds": 90.0},
                    "stages": [],
                },
            ]
        },
    )
    monkeypatch.setattr(
        operator_route.core_reads,
        "load_source_dates",
        lambda: {
            "prices_asof": "2026-03-07",
            "fundamentals_asof": "2026-03-01",
            "classification_asof": "2026-03-01",
            "exposures_asof": "2026-03-07",
        },
    )
    monkeypatch.setattr(operator_route, "_today_session_date", lambda: operator_route.datetime(2026, 3, 8).date())
    monkeypatch.setattr(operator_route, "_risk_recompute_due", lambda meta, today_utc: (False, "within_interval"))

    def _fake_cache_get(key: str):
        if key == "risk_engine_meta":
            return {"method_version": "v4_trbc_l2_country_us_dummy_2026_03_08", "factor_returns_latest_date": "2026-03-07"}
        if key == "refresh_status":
            return {"status": "idle"}
        if key == "neon_sync_health":
            return {"status": "ok", "artifact_path": "/tmp/report.json"}
        if key == "__cache_snapshot_active":
            return {"snapshot_id": "snap_1"}
        return None

    monkeypatch.setattr(operator_route.sqlite, "cache_get", _fake_cache_get)

    client = TestClient(app)
    res = client.get("/api/operator/status")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "ok"
    assert body["core_due"] == {"due": False, "reason": "within_interval"}
    assert body["lanes"][0]["profile"] == "serve-refresh"
    assert body["lanes"][0]["latest_run"]["run_id"] == "job_123"
    assert body["lanes"][0]["latest_run"]["duration_seconds"] == 60.0
    assert body["lanes"][0]["latest_run"]["duration_delta_seconds"] == -30.0
    assert body["lanes"][0]["latest_run"]["duration_delta_pct"] == -33.33
    assert body["lanes"][0]["latest_run"]["slowest_stage"]["stage_name"] == "serving_refresh"
    assert body["source_dates"]["prices_asof"] == "2026-03-07"
    assert body["latest_parity_artifact"] == "/tmp/report.json"
    assert body["runtime"]["canonical_serving_profile"] == "serve-refresh"
    assert body["runtime"]["dashboard_truth_surface"] == "durable_serving_payloads"
    assert body["runtime"]["diagnostics_scope"] == "local_sqlite_and_cache"


def test_latest_run_summary_by_profile_handles_empty_db(tmp_path) -> None:
    from backend.data import job_runs

    out = job_runs.latest_run_summary_by_profile(db_path=tmp_path / "data.db", profiles=["serve-refresh"])
    assert out == {}


def test_operator_status_reports_cloud_allowed_profiles(monkeypatch) -> None:
    monkeypatch.setattr(operator_route.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(operator_route.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(operator_route.job_runs, "latest_run_summary_by_profile", lambda **kwargs: {})
    monkeypatch.setattr(operator_route.job_runs, "recent_run_summaries_by_profile", lambda **kwargs: {})
    monkeypatch.setattr(operator_route.core_reads, "load_source_dates", lambda: {})
    monkeypatch.setattr(operator_route.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(operator_route, "get_holdings_sync_state", lambda: {"pending": False, "pending_count": 0})

    client = TestClient(app)
    res = client.get("/api/operator/status", headers={"X-Operator-Token": "op-secret"})

    assert res.status_code == 200
    assert res.json()["runtime"]["allowed_profiles"] == ["serve-refresh"]
    assert "source-daily" in res.json()["runtime"]["local_only_profiles"]
