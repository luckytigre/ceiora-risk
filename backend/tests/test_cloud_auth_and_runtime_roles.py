from __future__ import annotations

import base64
import hashlib
import hmac
import importlib
import json
import os

from fastapi.testclient import TestClient

from backend.app_factory import create_app
from backend.main import app
from backend.api import auth as auth_module
from backend.api.routes import data as data_routes
from backend.api.routes import health as health_routes
from backend.api.routes import holdings as holdings_route
from backend.api.routes import operator as operator_route
from backend.api.routes import refresh as refresh_routes

orchestrator = importlib.import_module("backend.orchestration.run_model_pipeline")
refresh_manager = importlib.import_module("backend.services.refresh_manager")
refresh_request_policy = importlib.import_module("backend.services.refresh_request_policy")
refresh_profile_policy = importlib.import_module("backend.services.refresh_profile_policy")


def _signed_app_session_token(
    *,
    provider: str = "shared",
    subject: str = "friend@example.com",
    email: str | None = "friend@example.com",
    is_admin: bool = False,
) -> str:
    payload = {
        "authProvider": provider,
        "username": email or subject,
        "subject": subject,
        "email": email,
        "isAdmin": is_admin,
        "issuedAt": 1,
        "expiresAt": 4743856000,
    }
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii").rstrip("=")
    signature = base64.urlsafe_b64encode(
        hmac.new(b"test-secret", encoded.encode("utf-8"), hashlib.sha256).digest()
    ).decode("ascii").rstrip("=")
    return f"{encoded}.{signature}"


def _config_snapshot_with_env(**updates: str | None) -> dict[str, object]:
    import backend.config as config_module

    prior = {key: os.environ.get(key) for key in updates}
    try:
        for key, value in updates.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(config_module)
        return {
            "APP_RUNTIME_ROLE": config_module.APP_RUNTIME_ROLE,
            "NEON_AUTHORITATIVE_REBUILDS": config_module.NEON_AUTHORITATIVE_REBUILDS,
            "neon_authoritative_rebuilds_enabled": config_module.neon_authoritative_rebuilds_enabled(),
        }
    finally:
        for key, value in prior.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(config_module)


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
    assert client.post("/api/refresh", headers={"X-Refresh-Token": "op-secret"}).status_code == 401
    assert client.post("/api/refresh", headers={"X-Operator-Token": "op-secret"}).status_code == 202


def test_cloud_refresh_status_accepts_operator_header(monkeypatch) -> None:
    monkeypatch.setattr(refresh_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_routes.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(refresh_routes, "get_refresh_status", lambda: {"status": "idle"})

    client = TestClient(app)
    assert client.get("/api/refresh/status", headers={"X-Refresh-Token": "op-secret"}).status_code == 401
    res = client.get("/api/refresh/status", headers={"X-Operator-Token": "op-secret"})
    assert res.status_code == 200
    assert res.json()["refresh"]["status"] == "idle"
    bearer_res = client.get("/api/refresh/status", headers={"Authorization": "Bearer op-secret"})
    assert bearer_res.status_code == 200
    assert bearer_res.json()["refresh"]["status"] == "idle"


def test_cloud_holdings_write_allows_authenticated_app_session(monkeypatch) -> None:
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(auth_module.config, "EDITOR_API_TOKEN", "edit-secret")
    monkeypatch.setenv("CEIORA_SESSION_SECRET", "test-secret")
    monkeypatch.setattr(
        holdings_route.holdings_service,
        "run_position_upsert",
        lambda **kwargs: {
            "status": "ok",
            "action": "upserted",
            "account_id": "main",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 10.0,
            "import_batch_id": "batch_1",
        },
    )
    monkeypatch.setattr(
        holdings_route,
        "_resolve_holdings_scope",
        lambda **kwargs: holdings_route.resolve_account_scope(
            None,
            principal=auth_module.AppPrincipal(provider="shared", subject="friend@example.com", is_admin=False),
        ),
    )
    monkeypatch.setattr(
        holdings_route,
        "validate_requested_account",
        lambda scope, requested_account_id: requested_account_id or "main",
    )

    client = TestClient(app)
    payload = {"account_id": "main", "ric": "AAPL.OQ", "quantity": 10, "trigger_refresh": False}
    assert client.post("/api/holdings/position", json=payload).status_code == 401
    assert client.post("/api/holdings/position", json=payload, headers={"X-Refresh-Token": "op-secret"}).status_code == 401
    assert (
        client.post(
            "/api/holdings/position",
            json=payload,
            headers={"X-App-Session-Token": _signed_app_session_token()},
        ).status_code
        == 200
    )
    assert client.post("/api/holdings/position", json=payload, headers={"X-Editor-Token": "edit-secret"}).status_code == 401
    assert client.post("/api/holdings/position", json=payload, headers={"X-Operator-Token": "op-secret"}).status_code == 401


def test_cloud_expensive_diagnostics_require_operator_token(monkeypatch) -> None:
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(
        data_routes.data_diagnostics_service,
        "build_data_diagnostics_payload",
        lambda **_kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        health_routes,
        "load_health_diagnostics_payload",
        lambda: {"status": "ok"},
    )

    client = TestClient(app)
    assert client.get("/api/data/diagnostics?include_expensive_checks=true").status_code == 401
    assert client.get("/api/data/diagnostics?include_expensive_checks=true", headers={"X-Operator-Token": "op-secret"}).status_code == 200
    assert client.get("/api/health/diagnostics").status_code == 401
    assert client.get("/api/health/diagnostics", headers={"X-Operator-Token": "op-secret"}).status_code == 200


def test_cloud_health_diagnostics_reports_authority_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(health_routes, "require_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        health_routes,
        "load_health_diagnostics_payload",
        lambda: (_ for _ in ()).throw(
            health_routes.health_diagnostics_service.HealthDiagnosticsUnavailable(
                message="Health diagnostics authority is unavailable from neon.",
                source="neon",
                error={"type": "OperationalError", "message": "timed out"},
            )
        ),
    )

    client = TestClient(app)
    res = client.get("/api/health/diagnostics", headers={"X-Operator-Token": "op-secret"})

    assert res.status_code == 503
    assert res.json()["detail"]["status"] == "unavailable"
    assert res.json()["detail"]["error"] == "health_diagnostics_authority_unavailable"
    assert res.json()["detail"]["source"] == "neon"


def test_cloud_operator_status_requires_operator_token(monkeypatch) -> None:
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(operator_route.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(operator_route.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(operator_route, "build_operator_status_payload", lambda: {"status": "ok"})

    client = TestClient(app)
    assert client.get("/api/operator/status").status_code == 401
    assert client.get("/api/operator/status", headers={"X-Refresh-Token": "op-secret"}).status_code == 401
    assert client.get("/api/operator/status", headers={"X-Operator-Token": "op-secret"}).status_code == 200
    assert client.get("/api/operator/status", headers={"Authorization": "Bearer op-secret"}).status_code == 200


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
        profile="source-daily",
        stage="ingest",
        as_of_date="2026-03-09",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="test",
        data_db=orchestrator.DATA_DB,
        cache_db=orchestrator.CACHE_DB,
        raw_history_policy="none",
        reset_core_cache=False,
        enable_ingest=True,
    )

    assert out["status"] == "skipped"
    assert out["reason"] == "runtime_role_disallows_ingest"


def test_cloud_runtime_role_allows_core_rebuild_profiles(monkeypatch) -> None:
    monkeypatch.setattr(refresh_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_routes.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(refresh_manager.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_manager.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(
        refresh_routes,
        "start_refresh",
        lambda **kwargs: (
            True,
            {
                "status": "running",
                "profile": kwargs.get("profile"),
                "mode": "full",
            },
        ),
    )

    with TestClient(app) as client:
        res = client.post("/api/refresh?profile=core-weekly", headers={"X-Operator-Token": "op-secret"})

    assert res.status_code == 202
    assert res.json()["refresh"]["profile"] == "core-weekly"


def test_control_surface_exposes_cpar_build_route_with_operator_token(monkeypatch) -> None:
    control_app = create_app(surface="control")
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(
        "backend.services.cpar_build_service.dispatch_cpar_build",
        lambda **kwargs: (
            True,
            {
                "status": "dispatched",
                "profile": kwargs["profile"],
                "package_date": "2026-03-27",
                "pipeline_run_id": "cpar_crj_123",
                "execution_name": "projects/p/locations/r/jobs/j/executions/e",
                "dispatch_backend": "cloud_run_job",
            },
        ),
    )

    with TestClient(control_app) as client:
        unauthorized = client.post("/api/cpar/build?profile=cpar-weekly")
        authorized = client.post(
            "/api/cpar/build?profile=cpar-weekly",
            headers={"X-Operator-Token": "op-secret"},
        )
        bearer = client.post(
            "/api/cpar/build?profile=cpar-weekly",
            headers={"Authorization": "Bearer op-secret"},
        )
        invalid = client.post(
            "/api/cpar/build?profile=cpar-weekly",
            headers={"X-Operator-Token": "wrong-secret"},
        )

    assert unauthorized.status_code == 401
    assert authorized.status_code == 202
    assert bearer.status_code == 202
    assert invalid.status_code == 401
    assert authorized.json()["dispatch_backend"] == "cloud_run_job"
    assert authorized.json()["execution_name"] == "projects/p/locations/r/jobs/j/executions/e"
    assert authorized.json()["package_date"] == "2026-03-27"


def test_control_surface_rejects_invalid_cpar_build_profile_with_400(monkeypatch) -> None:
    control_app = create_app(surface="control")
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")

    with TestClient(control_app) as client:
        res = client.post(
            "/api/cpar/build?profile=not-a-real-profile",
            headers={"X-Operator-Token": "op-secret"},
        )

    assert res.status_code == 400
    assert "Unsupported cPAR profile" in res.json()["detail"]


def test_control_surface_rejects_invalid_cpar_package_date_with_400(monkeypatch) -> None:
    control_app = create_app(surface="control")
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")

    with TestClient(control_app) as client:
        res = client.post(
            "/api/cpar/build?profile=cpar-package-date&as_of_date=2026-03-26",
            headers={"X-Operator-Token": "op-secret"},
        )

    assert res.status_code == 400
    assert "requires one explicit XNYS weekly package date" in res.json()["detail"]


def test_full_surface_does_not_mount_cpar_build_route(monkeypatch) -> None:
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")

    with TestClient(app) as client:
        res = client.post("/api/cpar/build?profile=cpar-weekly", headers={"X-Operator-Token": "op-secret"})

    assert res.status_code == 404


def test_serve_surface_does_not_mount_cpar_build_route(monkeypatch) -> None:
    serve_app = create_app(surface="serve")
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")

    with TestClient(serve_app) as client:
        res = client.post("/api/cpar/build?profile=cpar-weekly", headers={"X-Operator-Token": "op-secret"})

    assert res.status_code == 404


def test_control_surface_maps_unavailable_cpar_build_to_503(monkeypatch) -> None:
    control_app = create_app(surface="control")
    monkeypatch.setattr(auth_module.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(auth_module.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(
        "backend.services.cpar_build_service.dispatch_cpar_build",
        lambda **_kwargs: (
            False,
            {
                "status": "unavailable",
                "error": {
                    "type": "cloud_run_job_unconfigured",
                    "message": "missing cpar build job env",
                },
            },
        ),
    )

    with TestClient(control_app) as client:
        res = client.post(
            "/api/cpar/build?profile=cpar-weekly",
            headers={"X-Operator-Token": "op-secret"},
        )

    assert res.status_code == 503
    assert res.json()["detail"]["error"]["type"] == "cloud_run_job_unconfigured"


def test_cloud_refresh_defaults_to_serve_refresh_when_profile_omitted(monkeypatch) -> None:
    monkeypatch.setattr(refresh_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_routes.config, "OPERATOR_API_TOKEN", "op-secret")
    monkeypatch.setattr(refresh_manager.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(
        refresh_routes,
        "start_refresh",
        lambda **kwargs: (
            True,
            {
                "status": "running",
                "profile": kwargs.get("profile") or "serve-refresh",
                "mode": "light",
            },
        ),
    )

    with TestClient(app) as client:
        res = client.post("/api/refresh", headers={"X-Operator-Token": "op-secret"})

    assert res.status_code == 202
    body = res.json()
    assert body["refresh"]["profile"] == "serve-refresh"
    assert body["refresh"]["mode"] == "light"


def test_cloud_job_runtime_role_allows_core_weekly_resolution(monkeypatch) -> None:
    monkeypatch.setattr(refresh_profile_policy.config, "APP_RUNTIME_ROLE", "cloud-job")
    monkeypatch.setattr(refresh_profile_policy.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(refresh_profile_policy.config, "NEON_AUTHORITATIVE_REBUILDS", True)
    monkeypatch.setattr(orchestrator.config, "APP_RUNTIME_ROLE", "cloud-job")
    monkeypatch.setattr(orchestrator.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(orchestrator.config, "NEON_AUTHORITATIVE_REBUILDS", True)

    request = refresh_request_policy.resolve_refresh_request(profile="core-weekly")

    assert request["profile"] == "core-weekly"
    assert request["mode"] == "full"


def test_cloud_job_runtime_role_requires_explicit_profile_in_job_runner(monkeypatch) -> None:
    monkeypatch.setattr(refresh_profile_policy.config, "APP_RUNTIME_ROLE", "cloud-job")

    assert refresh_profile_policy.runtime_allowed_profiles() == {"serve-refresh", "core-weekly", "cold-core"}


def test_invalid_runtime_role_defaults_fail_closed() -> None:
    snapshot = _config_snapshot_with_env(APP_RUNTIME_ROLE="totally-invalid")

    assert snapshot["APP_RUNTIME_ROLE"] == "cloud-serve"


def test_neon_authoritative_rebuilds_default_on_when_neon_backend_configured() -> None:
    snapshot = _config_snapshot_with_env(
        NEON_DATABASE_URL="postgresql://example",
        DATABASE_URL=None,
        DATA_BACKEND="neon",
        NEON_AUTHORITATIVE_REBUILDS=None,
    )

    assert snapshot["NEON_AUTHORITATIVE_REBUILDS"] is True
    assert snapshot["neon_authoritative_rebuilds_enabled"] is True


def test_neon_authoritative_rebuilds_can_be_disabled_explicitly() -> None:
    snapshot = _config_snapshot_with_env(
        NEON_DATABASE_URL="postgresql://example",
        DATABASE_URL=None,
        DATA_BACKEND="neon",
        NEON_AUTHORITATIVE_REBUILDS="false",
    )

    assert snapshot["NEON_AUTHORITATIVE_REBUILDS"] is False
    assert snapshot["neon_authoritative_rebuilds_enabled"] is False


def test_neon_authoritative_rebuilds_stay_off_without_neon_backend() -> None:
    snapshot = _config_snapshot_with_env(
        NEON_DATABASE_URL="",
        DATABASE_URL="",
        DATA_BACKEND="sqlite",
        NEON_AUTHORITATIVE_REBUILDS=None,
    )

    assert snapshot["NEON_AUTHORITATIVE_REBUILDS"] is False
    assert snapshot["neon_authoritative_rebuilds_enabled"] is False


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
