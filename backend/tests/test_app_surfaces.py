from __future__ import annotations

from backend.app_factory import create_app


def _paths_for(surface: str) -> set[str]:
    app = create_app(surface=surface)
    return {route.path for route in app.routes}


def test_serve_app_exposes_public_routes_only() -> None:
    paths = _paths_for("serve")

    assert "/api/portfolio" in paths
    assert "/api/cpar/risk" in paths
    assert "/api/holdings/accounts" in paths
    assert "/api/refresh" not in paths
    assert "/api/refresh/status" not in paths
    assert "/api/operator/status" not in paths
    assert "/api/health/diagnostics" not in paths
    assert "/api/data/diagnostics" not in paths
    assert "/api/health" in paths


def test_control_app_exposes_control_routes_only() -> None:
    paths = _paths_for("control")

    assert "/api/refresh" in paths
    assert "/api/refresh/status" in paths
    assert "/api/operator/status" in paths
    assert "/api/health/diagnostics" in paths
    assert "/api/data/diagnostics" in paths
    assert "/api/portfolio" not in paths
    assert "/api/cpar/risk" not in paths
    assert "/api/holdings/accounts" not in paths
    assert "/api/health" in paths


def test_full_app_exposes_both_surfaces() -> None:
    paths = _paths_for("full")

    assert "/api/portfolio" in paths
    assert "/api/cpar/risk" in paths
    assert "/api/holdings/accounts" in paths
    assert "/api/refresh" in paths
    assert "/api/refresh/status" in paths
    assert "/api/operator/status" in paths
    assert "/api/health/diagnostics" in paths
    assert "/api/data/diagnostics" in paths
    assert "/api/health" in paths
