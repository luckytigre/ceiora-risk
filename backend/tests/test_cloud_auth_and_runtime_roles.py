from __future__ import annotations

import importlib
import os

from fastapi.testclient import TestClient

from backend.main import app
from backend.api import auth as auth_module
from backend.api.routes import data as data_routes
from backend.api.routes import health as health_routes
from backend.api.routes import holdings as holdings_route
from backend.api.routes import operator as operator_route
from backend.api.routes import refresh as refresh_routes

orchestrator = importlib.import_module("backend.orchestration.run_model_pipeline")
refresh_manager = importlib.import_module("backend.services.refresh_manager")


class _FakeConn:
    def close(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_cloud_refresh_requires_operator_token(monkeypatch) -> None:
    monkeypatch.setattr(refresh_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_routes.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(
        refresh_routes,
        "start_refresh",
        lambda **_kwargs: (True, {"status": "running"}),
    )
    client = TestClient(app)
    assert client.post("/api/refresh").status_code == 401
    assert client.post("/api/refresh", headers={"X-Refresh-Token": "op-secret"}).status_code == 202


def test_cloud_holdings_write_requires_editor_or_operator_token(monkeypatch) -> None:
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(auth_module.config, "EDITOR_API_TOKEN", "edit-secret")
    monkeypatch.setattr(holdings_route, "resolve_dsn", lambda _dsn=None: "postgres://example")
    monkeypatch.setattr(holdings_route, "connect", lambda **kwargs: _FakeConn())
    monkeypatch.setattr(
        holdings_route,
        "apply_single_position_edit",
        lambda *args, **kwargs: {
            "status": "ok",
            "action": "upserted",
            "account_id": "main",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 10.0,
            "import_batch_id": "batch_1",
        },
    )
    monkeypatch.setattr(holdings_route, "mark_holdings_dirty", lambda **kwargs: None)
    monkeypatch.setattr(holdings_route, "_trigger_light_refresh_if_requested", lambda _trigger: None)

    client = TestClient(app)
    payload = {"account_id": "main", "ric": "AAPL.OQ", "quantity": 10, "trigger_refresh": False}
    assert client.post("/api/holdings/position", json=payload).status_code == 401
    assert client.post("/api/holdings/position", json=payload, headers={"X-Editor-Token": "edit-secret"}).status_code == 200
    assert client.post("/api/holdings/position", json=payload, headers={"X-Operator-Token": "op-secret"}).status_code == 200


def test_cloud_expensive_diagnostics_require_operator_token(monkeypatch) -> None:
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(health_routes, "cache_get", lambda key: {"status": "ok"} if key == "health_diagnostics" else None)

    client = TestClient(app)
    assert client.get("/api/data/diagnostics?include_expensive_checks=true").status_code == 401
    assert client.get("/api/data/diagnostics?include_expensive_checks=true", headers={"X-Operator-Token": "op-secret"}).status_code == 200
    assert client.get("/api/health/diagnostics").status_code == 401
    assert client.get("/api/health/diagnostics", headers={"X-Operator-Token": "op-secret"}).status_code == 200


def test_cloud_operator_status_requires_operator_token(monkeypatch) -> None:
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(operator_route.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(operator_route.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(operator_route.job_runs, "latest_run_summary_by_profile", lambda **kwargs: {})
    monkeypatch.setattr(operator_route.job_runs, "recent_run_summaries_by_profile", lambda **kwargs: {})
    monkeypatch.setattr(operator_route.core_reads, "load_source_dates", lambda: {})
    monkeypatch.setattr(operator_route.sqlite, "cache_get", lambda key: {})

    client = TestClient(app)
    assert client.get("/api/operator/status").status_code == 401
    assert client.get("/api/operator/status", headers={"X-Operator-Token": "op-secret"}).status_code == 200


def test_cloud_runtime_role_blocks_ingest_stage(monkeypatch) -> None:
    monkeypatch.setattr(orchestrator.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(orchestrator.config, "ORCHESTRATOR_ENABLE_INGEST", True)
    monkeypatch.setattr(
        orchestrator,
        "bootstrap_cuse4_source_tables",
        lambda **kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        orchestrator,
        "download_from_lseg",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not ingest in cloud mode")),
    )

    out = orchestrator._run_stage(
        stage="ingest",
        as_of_date="2026-03-09",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="test",
        raw_history_policy="none",
        reset_core_cache=False,
        enable_ingest=True,
    )

    assert out["status"] == "skipped"
    assert out["reason"] == "runtime_role_disallows_ingest"


def test_cloud_runtime_role_allows_only_serve_refresh(monkeypatch) -> None:
    monkeypatch.setattr(refresh_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_routes.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(refresh_manager.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_manager.config, "OPERATOR_API_TOKEN", "op-secret")

    with TestClient(app) as client:
        res = client.post("/api/refresh?profile=core-weekly", headers={"X-Refresh-Token": "op-secret"})

    assert res.status_code == 400
    assert "Allowed profiles: serve-refresh" in res.json()["message"]


def test_invalid_runtime_role_defaults_fail_closed() -> None:
    import backend.config as config_module

    prior = os.environ.get("APP_RUNTIME_ROLE")
    try:
        os.environ["APP_RUNTIME_ROLE"] = "totally-invalid"
        importlib.reload(config_module)
        assert config_module.APP_RUNTIME_ROLE == "cloud-serve"
    finally:
        if prior is None:
            os.environ.pop("APP_RUNTIME_ROLE", None)
        else:
            os.environ["APP_RUNTIME_ROLE"] = prior
        importlib.reload(config_module)


def test_serving_outputs_cloud_mode_does_not_fallback_to_sqlite(monkeypatch, tmp_path) -> None:
    from backend.data import serving_outputs

    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(serving_outputs.config, "SERVING_OUTPUTS_PRIMARY_READS", False)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")
    monkeypatch.setattr(serving_outputs, "_load_current_payload_neon", lambda payload_name: None)

    out = serving_outputs.load_current_payload("portfolio")

    assert out is None


def test_cloud_mode_forces_neon_surfaces_on(monkeypatch) -> None:
    import backend.config as config_module

    monkeypatch.setattr(config_module, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(config_module, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(config_module, "NEON_READ_SURFACES", set())

    assert config_module.neon_surface_enabled("serving_outputs") is True
    assert config_module.neon_surface_enabled("core_reads") is True
