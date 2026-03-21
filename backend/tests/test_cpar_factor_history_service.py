from __future__ import annotations

import pytest

from backend.data import cpar_outputs
from backend.services import cpar_factor_history_service, cpar_meta_service


def test_load_cpar_factor_history_payload_returns_cumulative_points(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_meta_service,
        "require_active_package",
        lambda *, data_db=None: {"package_run_id": "pkg-1"},
    )
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_outputs,
        "load_factor_return_history",
        lambda factor_id, *, years, data_db=None: (
            "2026-03-14",
            [("2026-03-13", 0.02), ("2026-03-14", -0.00980392)],
        ),
    )

    payload = cpar_factor_history_service.load_cpar_factor_history_payload(factor_id="SPY", years=5)

    assert payload["factor_id"] == "SPY"
    assert payload["factor_name"] == "Market"
    assert payload["points"] == [
        {"date": "2026-03-13", "factor_return": 0.02, "cum_return": 0.02},
        {"date": "2026-03-14", "factor_return": -0.00980392, "cum_return": 0.01},
    ]


def test_load_cpar_factor_history_payload_rejects_unknown_factor() -> None:
    with pytest.raises(cpar_factor_history_service.CparFactorNotFound, match="Unknown cPAR factor_id"):
        cpar_factor_history_service.load_cpar_factor_history_payload(factor_id="BAD", years=5)


def test_load_cpar_factor_history_payload_maps_missing_history_to_not_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_meta_service,
        "require_active_package",
        lambda *, data_db=None: {"package_run_id": "pkg-1"},
    )
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_outputs,
        "load_factor_return_history",
        lambda factor_id, *, years, data_db=None: (None, []),
    )

    with pytest.raises(cpar_meta_service.CparReadNotReady, match="Historical cPAR factor returns are not available yet"):
        cpar_factor_history_service.load_cpar_factor_history_payload(factor_id="SPY", years=5)


def test_load_cpar_factor_history_payload_maps_authority_failures_to_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_factor_history_service.cpar_meta_service,
        "require_active_package",
        lambda *, data_db=None: (_ for _ in ()).throw(
            cpar_outputs.CparAuthorityReadError("Neon read failed")
        ),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Neon read failed"):
        cpar_factor_history_service.load_cpar_factor_history_payload(factor_id="SPY", years=5)
