from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import operator as operator_route
import backend.services.cuse4_operator_status_service as svc
from backend.analytics import pipeline


def test_operator_status_route_returns_lane_matrix(monkeypatch) -> None:
    monkeypatch.setattr(
        svc,
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
            }
        ],
    )
    monkeypatch.setattr(
        svc.job_runs,
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
        svc.core_reads,
        "load_source_dates",
        lambda: {
            "prices_asof": "2026-03-07",
            "fundamentals_asof": "2026-03-01",
            "classification_asof": "2026-03-01",
            "exposures_latest_available_asof": "2026-03-07",
            "exposures_asof": "2026-03-07",
        },
    )
    monkeypatch.setattr(svc, "_today_session_date", lambda: datetime(2026, 3, 8).date())
    monkeypatch.setattr(svc, "_risk_recompute_due", lambda meta, today_utc: (False, "within_interval"))
    monkeypatch.setattr(svc, "get_refresh_status", lambda: {"status": "idle"})

    def _fake_cache_get(key: str):
        if key == "risk_engine_meta":
            return {
                "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
                "factor_returns_latest_date": "2026-03-07",
                "last_recompute_date": "2026-03-08",
                "cross_section_min_age_days": 7,
            }
        if key == "neon_sync_health":
            return {"status": "ok", "artifact_path": "/tmp/report.json"}
        if key == "__cache_snapshot_active":
            return {"snapshot_id": "snap_1"}
        return None

    monkeypatch.setattr(
        svc.runtime_state,
        "read_runtime_state",
        lambda key, fallback_loader=None: {"status": "ok", "source": "neon", "value": _fake_cache_get(key)},
    )
    monkeypatch.setattr(svc.sqlite, "cache_get", _fake_cache_get)
    monkeypatch.setattr(svc.sqlite, "cache_get_live_first", _fake_cache_get)

    client = TestClient(app)
    res = client.get("/api/operator/status")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "ok"
    assert body["core_due"] == {"due": False, "reason": "within_interval"}
    assert body["lanes"][0]["profile"] == "serve-refresh"
    assert body["lanes"][0]["latest_run"]["run_id"] == "job_123"
    assert body["lanes"][0]["latest_run"]["duration_seconds"] == 60.0
    assert body["lanes"][0]["latest_run"]["slowest_stage"]["stage_name"] == "serving_refresh"
    assert body["source_dates"]["prices_asof"] == "2026-03-07"
    assert body["latest_parity_artifact"] == "/tmp/report.json"
    assert body["runtime"]["canonical_serving_profile"] == "serve-refresh"
    assert body["runtime"]["dashboard_truth_surface"] == "durable_serving_payloads"
    assert body["runtime"]["diagnostics_scope"] == "local_sqlite_and_cache"
    assert body["runtime"]["source_authority"] in {"local", "neon"}
    assert body["runtime"]["runtime_state_status"]["risk_engine_meta"]["status"] == "ok"
    assert body["runtime"]["runtime_state_status"]["risk_engine_meta"]["source"] == "neon"
    assert body["source_dates"]["exposures_latest_available_asof"] == "2026-03-07"
    assert body["risk_engine"]["core_state_through_date"] == "2026-03-07"
    assert body["risk_engine"]["core_rebuild_date"] == "2026-03-08"
    assert body["risk_engine"]["estimation_exposure_anchor_date"] == "2026-02-27"
    assert "neon_authoritative_rebuilds" in body["runtime"]


def test_latest_run_summary_by_profile_handles_empty_db(tmp_path) -> None:
    from backend.data import job_runs

    out = job_runs.latest_run_summary_by_profile(db_path=tmp_path / "data.db", profiles=["serve-refresh"])
    assert out == {}


def test_operator_status_reports_cloud_allowed_profiles(monkeypatch) -> None:
    monkeypatch.setattr(operator_route.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(operator_route.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(svc.job_runs, "latest_run_summary_by_profile", lambda **kwargs: {})
    monkeypatch.setattr(svc.core_reads, "load_source_dates", lambda: {})
    monkeypatch.setattr(
        svc.runtime_state,
        "read_runtime_state",
        lambda key, fallback_loader=None: {"status": "missing", "source": "neon", "value": None},
    )
    monkeypatch.setattr(svc.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(svc.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(svc, "get_holdings_sync_state", lambda: {"pending": False, "pending_count": 0})

    client = TestClient(app)
    res = client.get("/api/operator/status", headers={"X-Operator-Token": "op-secret"})

    assert res.status_code == 200
    assert res.json()["runtime"]["allowed_profiles"] == ["serve-refresh"]
    assert "source-daily" in res.json()["runtime"]["local_only_profiles"]


def test_operator_status_source_dates_use_authoritative_backend_and_expose_local_archive(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator_route.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(
        svc.core_reads.config,
        "neon_surface_enabled",
        lambda surface: surface == "core_reads",
    )

    def _load_source_dates():
        backend = svc.core_reads.core_read_backend_name()
        captured.setdefault("backends", []).append(backend)
        return {"prices_asof": "2026-03-14" if backend == "local" else "2026-03-13"}

    monkeypatch.setattr(svc.core_reads, "load_source_dates", _load_source_dates)

    authoritative = svc._load_authoritative_operator_source_dates()
    local_archive = svc._load_local_archive_source_dates()

    assert authoritative == {"prices_asof": "2026-03-13"}
    assert local_archive == {"prices_asof": "2026-03-14"}
    assert captured["backends"] == ["neon", "local"]


def test_operator_status_warns_when_local_archive_is_newer_than_authoritative_store(monkeypatch) -> None:
    monkeypatch.setattr(svc.job_runs, "latest_run_summary_by_profile", lambda **kwargs: {})
    monkeypatch.setattr(svc, "get_refresh_status", lambda: {"status": "idle"})
    monkeypatch.setattr(svc, "get_holdings_sync_state", lambda: {"pending": False, "pending_count": 0})
    monkeypatch.setattr(
        svc.runtime_state,
        "read_runtime_state",
        lambda key, fallback_loader=None: {"status": "missing", "source": "neon", "value": None},
    )
    monkeypatch.setattr(svc.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(svc.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(operator_route.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(svc.core_reads, "load_source_dates", lambda: {"prices_asof": "2026-03-13"})
    monkeypatch.setattr(
        svc,
        "_load_local_archive_source_dates",
        lambda: {"prices_asof": "2026-03-14"},
    )

    client = TestClient(app)
    res = client.get("/api/operator/status")

    assert res.status_code == 200
    warnings = res.json()["runtime"]["warnings"]
    assert any("Local LSEG/archive data is newer than Neon" in item for item in warnings)


def test_operator_status_promotes_newer_terminal_run_over_stale_refresh_cache(monkeypatch) -> None:
    monkeypatch.setattr(
        svc.job_runs,
        "latest_run_summary_by_profile",
        lambda **kwargs: {
            "core-weekly": {
                "run_id": "job_20260316T044729Z",
                "profile": "core-weekly",
                "status": "ok",
                "started_at": "2026-03-16T04:47:29+00:00",
                "finished_at": "2026-03-16T04:50:46+00:00",
                "updated_at": "2026-03-16T04:50:46+00:00",
                "duration_seconds": 197.0,
                "stage_count": 4,
                "completed_stage_count": 4,
                "failed_stage_count": 0,
                "running_stage_count": 0,
                "stage_duration_seconds_total": 197.0,
                "slowest_stage": {"stage_name": "factor_returns", "duration_seconds": 100.0},
                "stages": [],
            }
        },
    )
    monkeypatch.setattr(
        svc,
        "get_refresh_status",
        lambda: {
            "status": "failed",
            "profile": "core-weekly",
            "pipeline_run_id": "api_6d8b6d72d55f",
            "started_at": "2026-03-16T03:36:24+00:00",
            "finished_at": "2026-03-16T03:40:03+00:00",
            "result": {"status": "failed", "run_id": "api_6d8b6d72d55f", "profile": "core-weekly"},
            "error": {"type": "pipeline_failed", "message": "Orchestrated pipeline returned failed status."},
        },
    )
    monkeypatch.setattr(svc.core_reads, "load_source_dates", lambda: {})
    monkeypatch.setattr(
        svc.runtime_state,
        "read_runtime_state",
        lambda key, fallback_loader=None: {"status": "missing", "source": "neon", "value": None},
    )
    monkeypatch.setattr(svc.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(svc.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(svc, "get_holdings_sync_state", lambda: {"pending": False, "pending_count": 0})

    client = TestClient(app)
    res = client.get("/api/operator/status")

    assert res.status_code == 200
    refresh = res.json()["refresh"]
    assert refresh["status"] == "ok"
    assert refresh["pipeline_run_id"] == "job_20260316T044729Z"
    assert refresh["result"]["run_id"] == "job_20260316T044729Z"
    assert refresh["promoted_from_job_runs"] is True


def test_operator_status_reconciles_running_lane_without_live_worker(monkeypatch) -> None:
    monkeypatch.setattr(
        svc,
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
            }
        ],
    )
    monkeypatch.setattr(
        svc.job_runs,
        "latest_run_summary_by_profile",
        lambda **kwargs: {
            "serve-refresh": {
                "run_id": "api_old_running",
                "profile": "serve-refresh",
                "status": "running",
                "started_at": "2026-03-15T20:41:16+00:00",
                "finished_at": None,
                "updated_at": "2026-03-15T20:41:16+00:00",
                "duration_seconds": 60.0,
                "stage_count": 1,
                "completed_stage_count": 0,
                "failed_stage_count": 0,
                "running_stage_count": 1,
                "current_stage": {"stage_name": "serving_refresh"},
                "stage_duration_seconds_total": 60.0,
                "slowest_stage": {"stage_name": "serving_refresh", "duration_seconds": 60.0},
                "stages": [
                    {
                        "stage_name": "serving_refresh",
                        "status": "running",
                        "started_at": "2026-03-15T20:41:16+00:00",
                        "completed_at": None,
                    }
                ],
            }
        },
    )
    monkeypatch.setattr(svc, "get_refresh_status", lambda: {
        "status": "unknown",
        "profile": "source-daily",
        "pipeline_run_id": "api_newer_refresh",
        "finished_at": "2026-03-15T22:27:50+00:00",
    })
    monkeypatch.setattr(svc.core_reads, "load_source_dates", lambda: {})
    monkeypatch.setattr(svc.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(svc.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(svc, "get_holdings_sync_state", lambda: {"pending": False, "pending_count": 0})

    client = TestClient(app)
    res = client.get("/api/operator/status")

    assert res.status_code == 200
    latest = res.json()["lanes"][0]["latest_run"]
    assert latest["status"] == "unknown"
    assert latest["running_stage_count"] == 0
    assert latest["current_stage"] is None
    assert latest["reconciled_from_refresh_status"] is True


def test_operator_status_keeps_cross_profile_terminal_refresh_from_marking_lane_ok(monkeypatch) -> None:
    monkeypatch.setattr(
        svc,
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
            }
        ],
    )
    monkeypatch.setattr(
        svc.job_runs,
        "latest_run_summary_by_profile",
        lambda **kwargs: {
            "serve-refresh": {
                "run_id": "api_old_running",
                "profile": "serve-refresh",
                "status": "running",
                "started_at": "2026-03-15T20:41:16+00:00",
                "finished_at": None,
                "updated_at": "2026-03-15T20:41:16+00:00",
                "duration_seconds": 60.0,
                "stage_count": 1,
                "completed_stage_count": 0,
                "failed_stage_count": 0,
                "running_stage_count": 1,
                "current_stage": {"stage_name": "serving_refresh"},
                "stage_duration_seconds_total": 60.0,
                "slowest_stage": {"stage_name": "serving_refresh", "duration_seconds": 60.0},
                "stages": [{"stage_name": "serving_refresh", "status": "running"}],
            }
        },
    )
    monkeypatch.setattr(svc, "get_refresh_status", lambda: {
        "status": "ok",
        "profile": "source-daily",
        "pipeline_run_id": "api_source_daily_done",
        "finished_at": "2026-03-15T22:27:50+00:00",
    })
    monkeypatch.setattr(svc.core_reads, "load_source_dates", lambda: {})
    monkeypatch.setattr(svc.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(svc.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(svc, "get_holdings_sync_state", lambda: {"pending": False, "pending_count": 0})
    monkeypatch.setattr(svc, "_now_iso", lambda: "2026-03-15T23:00:00+00:00")

    client = TestClient(app)
    res = client.get("/api/operator/status")

    assert res.status_code == 200
    latest = res.json()["lanes"][0]["latest_run"]
    assert latest["status"] == "unknown"
    assert latest["reconciled_from_refresh_status"] is True
    assert latest["finished_at"] == "2026-03-15T23:00:00+00:00"
    assert latest["updated_at"] == "2026-03-15T23:00:00+00:00"
    assert latest["stages"][0]["completed_at"] == "2026-03-15T23:00:00+00:00"


def test_operator_status_does_not_reconcile_external_running_lane_without_api_prefix(monkeypatch) -> None:
    monkeypatch.setattr(
        svc,
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
            }
        ],
    )
    monkeypatch.setattr(
        svc.job_runs,
        "latest_run_summary_by_profile",
        lambda **kwargs: {
            "serve-refresh": {
                "run_id": "job_external_running",
                "profile": "serve-refresh",
                "status": "running",
                "started_at": "2026-03-15T20:41:16+00:00",
                "finished_at": None,
                "updated_at": "2026-03-15T20:41:16+00:00",
                "duration_seconds": 60.0,
                "stage_count": 1,
                "completed_stage_count": 0,
                "failed_stage_count": 0,
                "running_stage_count": 1,
                "current_stage": {"stage_name": "serving_refresh"},
                "stage_duration_seconds_total": 60.0,
                "slowest_stage": {"stage_name": "serving_refresh", "duration_seconds": 60.0},
                "stages": [{"stage_name": "serving_refresh", "status": "running"}],
            }
        },
    )
    monkeypatch.setattr(svc, "get_refresh_status", lambda: {
        "status": "unknown",
        "profile": "source-daily",
        "pipeline_run_id": "api_newer_refresh",
        "finished_at": "2026-03-15T22:27:50+00:00",
    })
    monkeypatch.setattr(svc.core_reads, "load_source_dates", lambda: {})
    monkeypatch.setattr(svc.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(svc.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(svc, "get_holdings_sync_state", lambda: {"pending": False, "pending_count": 0})

    client = TestClient(app)
    res = client.get("/api/operator/status")

    assert res.status_code == 200
    latest = res.json()["lanes"][0]["latest_run"]
    assert latest["status"] == "running"
    assert latest["running_stage_count"] == 1
    assert latest["current_stage"] == {"stage_name": "serving_refresh"}
    assert latest.get("reconciled_from_refresh_status") is None


def test_operator_status_does_not_reconcile_non_serving_api_lane_without_live_worker(monkeypatch) -> None:
    monkeypatch.setattr(
        svc,
        "profile_catalog",
        lambda: [
            {
                "profile": "source-daily-plus-core-if-due",
                "label": "Source Daily + Core If Due",
                "description": "Ingest sources and recompute core if needed.",
                "core_policy": "due",
                "serving_mode": "full",
                "raw_history_policy": "full-daily",
                "reset_core_cache": False,
                "default_stages": ["bootstrap", "feature_build", "serving_refresh"],
                "enable_ingest": True,
            }
        ],
    )
    monkeypatch.setattr(
        svc.job_runs,
        "latest_run_summary_by_profile",
        lambda **kwargs: {
            "source-daily-plus-core-if-due": {
                "run_id": "api_source_daily_running",
                "profile": "source-daily-plus-core-if-due",
                "status": "running",
                "started_at": "2026-03-15T20:41:16+00:00",
                "finished_at": None,
                "updated_at": "2026-03-15T20:41:16+00:00",
                "duration_seconds": 60.0,
                "stage_count": 1,
                "completed_stage_count": 0,
                "failed_stage_count": 0,
                "running_stage_count": 1,
                "current_stage": {"stage_name": "bootstrap"},
                "stage_duration_seconds_total": 60.0,
                "slowest_stage": {"stage_name": "bootstrap", "duration_seconds": 60.0},
                "stages": [{"stage_name": "bootstrap", "status": "running"}],
            }
        },
    )
    monkeypatch.setattr(svc, "get_refresh_status", lambda: {
        "status": "unknown",
        "profile": "serve-refresh",
        "pipeline_run_id": "api_other_refresh",
        "finished_at": "2026-03-15T22:27:50+00:00",
    })
    monkeypatch.setattr(svc.core_reads, "load_source_dates", lambda: {})
    monkeypatch.setattr(svc.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(svc.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(svc, "get_holdings_sync_state", lambda: {"pending": False, "pending_count": 0})

    client = TestClient(app)
    res = client.get("/api/operator/status")

    assert res.status_code == 200
    latest = res.json()["lanes"][0]["latest_run"]
    assert latest["status"] == "running"
    assert latest["current_stage"] == {"stage_name": "bootstrap"}
    assert latest.get("reconciled_from_refresh_status") is None


def test_job_runs_summary_includes_live_stage_details(tmp_path) -> None:
    from backend.data import job_runs

    db = tmp_path / "data.db"
    job_runs.ensure_schema(db)
    job_runs.begin_stage(
        db_path=db,
        run_id="job_live",
        profile="cold-core",
        stage_name="raw_history",
        stage_order=2,
        details={
            "stage_index": 1,
            "stage_count": 6,
            "message": "Starting raw history",
        },
    )
    job_runs.heartbeat_stage(
        db_path=db,
        run_id="job_live",
        stage_name="raw_history",
        details={
            "message": "Computing style scores through 2026-03-13",
            "items_processed": 250,
            "items_total": 800,
            "progress_pct": 31.25,
            "unit": "cross_sections",
        },
    )

    out = job_runs.latest_run_summary_by_profile(db_path=db, profiles=["cold-core"])
    latest = out["cold-core"]

    assert latest["status"] == "running"
    assert latest["current_stage"]["stage_name"] == "raw_history"
    assert latest["current_stage"]["details"]["message"] == "Computing style scores through 2026-03-13"
    assert latest["current_stage"]["details"]["items_processed"] == 250
    assert latest["current_stage"]["details"]["unit"] == "cross_sections"
    assert latest["current_stage"]["heartbeat_at"] is not None


def test_fail_stale_running_stages_uses_recent_heartbeat(tmp_path) -> None:
    from backend.data import job_runs

    db = tmp_path / "data.db"
    job_runs.ensure_schema(db)
    job_runs.begin_stage(
        db_path=db,
        run_id="job_live",
        profile="cold-core",
        stage_name="raw_history",
        stage_order=2,
        details={"message": "Starting raw history"},
    )
    stale_started_at = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    fresh_updated_at = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            f"""
            UPDATE {job_runs.TABLE}
            SET started_at = ?, updated_at = ?
            WHERE run_id = ? AND stage_name = ?
            """,
            (stale_started_at, fresh_updated_at, "job_live", "raw_history"),
        )
        conn.commit()
    finally:
        conn.close()

    updated = job_runs.fail_stale_running_stages(db_path=db, stale_after_seconds=60)
    latest = job_runs.latest_run_summary_by_profile(db_path=db, profiles=["cold-core"])["cold-core"]

    assert updated == 0
    assert latest["status"] == "running"
