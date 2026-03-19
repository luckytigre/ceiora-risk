from __future__ import annotations

from pathlib import Path

import pytest

from backend.data import cpar_outputs


def _package_run() -> dict[str, object]:
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
        "data_authority": "sqlite",
        "error_type": None,
        "error_message": None,
    }


def _proxy_returns() -> list[dict[str, object]]:
    return [
        {
            "package_date": "2026-03-14",
            "week_end": "2026-03-13",
            "factor_id": "SPY",
            "factor_group": "market",
            "proxy_ric": "SPY.P",
            "proxy_ticker": "SPY",
            "return_value": 0.01,
            "weight_value": 0.5,
            "price_field_used": "adj_close",
            "package_run_id": "run_1",
        }
    ]


def _proxy_transforms() -> list[dict[str, object]]:
    return [
        {
            "package_date": "2026-03-14",
            "factor_id": "XLF",
            "factor_group": "sector",
            "proxy_ric": "XLF.P",
            "proxy_ticker": "XLF",
            "market_alpha": 0.001,
            "market_beta": 0.4,
            "package_run_id": "run_1",
        }
    ]


def _covariance_rows() -> list[dict[str, object]]:
    return [
        {
            "package_date": "2026-03-14",
            "factor_id": "SPY",
            "factor_id_2": "SPY",
            "covariance": 1.0,
            "correlation": 1.0,
            "package_run_id": "run_1",
        }
    ]


def _instrument_fits() -> list[dict[str, object]]:
    return [
        {
            "package_date": "2026-03-14",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "display_name": "Apple Inc.",
            "fit_status": "ok",
            "warnings": [],
            "observed_weeks": 52,
            "lookback_weeks": 52,
            "longest_gap_weeks": 0,
            "price_field_used": "adj_close",
            "hq_country_code": "US",
            "market_step_alpha": 0.01,
            "market_step_beta": 1.2,
            "block_alpha": 0.0,
            "spy_trade_beta_raw": 1.1,
            "raw_loadings": {"SPY": 1.1},
            "thresholded_loadings": {"SPY": 1.1},
            "factor_variance_proxy": 0.2,
            "factor_volatility_proxy": 0.4472135955,
            "package_run_id": "run_1",
        }
    ]


def test_cloud_mode_fails_closed_without_successful_neon_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")

    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(cpar_outputs, "_neon_fetch_rows", lambda sql, params=None, raise_on_error=False: [])

    with pytest.raises(cpar_outputs.CparPackageNotReady, match="cloud-serve authority store"):
        cpar_outputs.load_active_package_run(data_db=data_db)


def test_local_mode_can_fallback_to_sqlite_when_neon_reads_are_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")

    cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=_package_run(),
        proxy_returns=_proxy_returns(),
        proxy_transforms=_proxy_transforms(),
        covariance_rows=_covariance_rows(),
        instrument_fits=_instrument_fits(),
    )

    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(cpar_outputs, "_neon_fetch_rows", lambda sql, params=None, raise_on_error=False: [])

    out = cpar_outputs.load_active_package_run(data_db=data_db)

    assert out is not None
    assert out["package_run_id"] == "run_1"


def test_cloud_mode_persistence_is_blocked(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "cloud-serve")

    with pytest.raises(cpar_outputs.CparPersistenceNotAllowed, match="cloud-serve"):
        cpar_outputs.persist_cpar_package(
            data_db=tmp_path / "data.db",
            package_run=_package_run(),
            proxy_returns=_proxy_returns(),
            proxy_transforms=_proxy_transforms(),
            covariance_rows=_covariance_rows(),
            instrument_fits=_instrument_fits(),
        )


def test_cloud_mode_raises_authority_read_error_when_neon_read_path_breaks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "postgresql://example")
    monkeypatch.setattr(cpar_outputs, "connect", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("dsn boom")))

    with pytest.raises(cpar_outputs.CparAuthorityReadError, match="connection setup"):
        cpar_outputs.load_active_package_run()


def test_successful_package_persistence_requires_non_empty_child_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")

    with pytest.raises(ValueError, match="Successful cPAR packages require non-empty durable child rows"):
        cpar_outputs.persist_cpar_package(
            data_db=tmp_path / "data.db",
            package_run=_package_run(),
            proxy_returns=[],
            proxy_transforms=[],
            covariance_rows=[],
            instrument_fits=[],
        )


def test_active_package_covariance_rows_fail_closed_when_covariance_surface_is_partial(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")

    data_db = tmp_path / "partial_cov.db"
    cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=_package_run(),
        proxy_returns=_proxy_returns(),
        proxy_transforms=_proxy_transforms(),
        covariance_rows=_covariance_rows(),
        instrument_fits=_instrument_fits(),
    )

    with pytest.raises(cpar_outputs.CparPackageNotReady, match="incomplete covariance coverage"):
        cpar_outputs.load_active_package_covariance_rows(data_db=data_db)
