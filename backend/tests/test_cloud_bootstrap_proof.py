from __future__ import annotations

from pathlib import Path

import pytest

from backend.data import cpar_outputs, runtime_state, serving_outputs
from backend.portfolio import positions_store


def _cpar_package_row() -> dict[str, object]:
    return {
        "package_run_id": "run_1",
        "package_date": "2026-03-14",
        "profile": "cpar-weekly",
        "status": "ok",
        "started_at": "2026-03-14T00:00:00Z",
        "completed_at": "2026-03-14T00:01:00Z",
        "method_version": "cPAR1",
        "factor_registry_version": "cPAR1",
        "lookback_weeks": 52,
        "half_life_weeks": 26,
        "min_observations": 39,
        "proxy_price_rule": "adj_close_fallback_close",
        "source_prices_asof": "2026-03-14",
        "classification_asof": "2026-03-14",
        "universe_count": 1,
        "fit_ok_count": 1,
        "fit_limited_count": 0,
        "fit_insufficient_count": 0,
        "data_authority": "neon",
        "error_type": None,
        "error_message": None,
    }


def test_cloud_bootstrap_reads_from_neon_without_local_sqlite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    missing_data_db = tmp_path / "missing_runtime.db"

    monkeypatch.setattr(serving_outputs, "DATA_DB", missing_data_db)
    monkeypatch.setattr(serving_outputs.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(serving_outputs.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(serving_outputs.config, "NEON_DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(serving_outputs.config, "SERVING_OUTPUTS_PRIMARY_READS", False)
    monkeypatch.setattr(
        serving_outputs.config,
        "neon_surface_enabled",
        lambda surface: surface in {"serving_outputs", "runtime_state", "core_reads"},
    )
    monkeypatch.setattr(
        serving_outputs,
        "_load_current_payloads_neon",
        lambda payload_names: {
            str(payload_name): {"payload_name": str(payload_name), "source": "neon"}
            for payload_name in payload_names
        },
    )
    monkeypatch.setattr(
        serving_outputs,
        "_load_current_payloads_sqlite",
        lambda payload_names: (_ for _ in ()).throw(
            AssertionError("cloud bootstrap should not read serving payloads from sqlite")
        ),
    )

    monkeypatch.setattr(
        runtime_state,
        "_read_neon_runtime_state",
        lambda key: {"status": "ok", "source": "neon", "value": {"state_key": key, "source": "neon"}},
    )

    monkeypatch.setattr(
        positions_store,
        "_load_positions_from_neon",
        lambda: (
            {"AAPL": 10.0},
            {"AAPL": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON_HOLDINGS"}},
        ),
    )

    monkeypatch.setattr(cpar_outputs, "_neon_fetch", lambda sql, params=None: [_cpar_package_row()])
    monkeypatch.setattr(
        cpar_outputs,
        "_sqlite_fetch_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cloud bootstrap should not read cPAR packages from sqlite")),
    )

    assert serving_outputs.load_current_payload("portfolio") == {
        "payload_name": "portfolio",
        "source": "neon",
    }
    assert runtime_state.load_runtime_state(
        "risk_engine_meta",
        fallback_loader=lambda key: (_ for _ in ()).throw(AssertionError("cloud bootstrap should not use runtime sqlite fallback")),
    ) == {"state_key": "risk_engine_meta", "source": "neon"}
    shares, meta = positions_store.load_positions_snapshot()
    assert shares == {"AAPL": 10.0}
    assert meta["AAPL"]["source"] == "NEON_HOLDINGS"
    assert cpar_outputs.load_active_package_run(data_db=missing_data_db)["package_run_id"] == "run_1"


def test_cloud_bootstrap_fails_closed_without_local_fallbacks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    missing_data_db = tmp_path / "missing_runtime.db"

    monkeypatch.setattr(serving_outputs, "DATA_DB", missing_data_db)
    monkeypatch.setattr(serving_outputs.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(serving_outputs.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(serving_outputs.config, "NEON_DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(serving_outputs.config, "SERVING_OUTPUTS_PRIMARY_READS", False)
    monkeypatch.setattr(
        serving_outputs.config,
        "neon_surface_enabled",
        lambda surface: surface in {"serving_outputs", "runtime_state", "core_reads"},
    )
    monkeypatch.setattr(
        serving_outputs,
        "_load_current_payloads_neon",
        lambda payload_names: {str(payload_name): None for payload_name in payload_names},
    )
    monkeypatch.setattr(
        serving_outputs,
        "_load_current_payloads_sqlite",
        lambda payload_names: (_ for _ in ()).throw(
            AssertionError("cloud bootstrap should not fall back to sqlite serving payloads")
        ),
    )

    monkeypatch.setattr(
        runtime_state,
        "_read_neon_runtime_state",
        lambda key: {"status": "missing", "source": "neon", "value": None},
    )

    monkeypatch.setattr(
        positions_store,
        "_load_positions_from_neon",
        lambda: (_ for _ in ()).throw(RuntimeError("dsn failed")),
    )

    monkeypatch.setattr(cpar_outputs, "_neon_fetch", lambda sql, params=None: [])
    monkeypatch.setattr(
        cpar_outputs,
        "_sqlite_fetch_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cloud bootstrap should not fall back to sqlite cPAR reads")),
    )

    assert serving_outputs.load_current_payload("portfolio") is None
    assert runtime_state.load_runtime_state(
        "risk_engine_meta",
        fallback_loader=lambda key: (_ for _ in ()).throw(AssertionError("cloud bootstrap should not use runtime sqlite fallback")),
    ) is None
    with pytest.raises(positions_store.HoldingsUnavailableError, match="Neon holdings unavailable"):
        positions_store.load_positions_snapshot()
    with pytest.raises(cpar_outputs.CparPackageNotReady, match="cloud-serve authority store"):
        cpar_outputs.load_active_package_run(data_db=missing_data_db)
