from __future__ import annotations

from pathlib import Path

import pytest

from backend.cpar.factor_registry import CPAR1_METHOD_VERSION
from backend.data import cpar_outputs


@pytest.fixture(autouse=True)
def _stub_shared_price_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_outputs.cpar_source_reads,
        "load_latest_price_rows",
        lambda *_args, **_kwargs: [],
    )


class _DummyPgConn:
    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


def _package_run() -> dict[str, object]:
    return {
        "package_run_id": "run_1",
        "package_date": "2026-03-14",
        "profile": "cpar-weekly",
        "status": "ok",
        "started_at": "2026-03-14T00:00:00Z",
        "completed_at": "2026-03-14T00:01:00Z",
        "method_version": CPAR1_METHOD_VERSION,
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


def _proxy_returns() -> list[dict[str, object]]:
    return [
        {
            "week_end": "2026-03-13",
            "factor_id": "SPY",
            "factor_group": "market",
            "proxy_ric": "SPY.P",
            "proxy_ticker": "SPY",
            "return_value": 0.01,
            "weight_value": 0.5,
            "price_field_used": "adj_close",
        }
    ]


def _proxy_transforms() -> list[dict[str, object]]:
    return [
        {
            "factor_id": "XLF",
            "factor_group": "sector",
            "proxy_ric": "XLF.P",
            "proxy_ticker": "XLF",
            "market_alpha": 0.001,
            "market_beta": 0.4,
        }
    ]


def _covariance_rows() -> list[dict[str, object]]:
    return [
        {
            "factor_id": "SPY",
            "factor_id_2": "SPY",
            "covariance": 1.0,
            "correlation": 1.0,
        }
    ]


def _instrument_fits() -> list[dict[str, object]]:
    return [
        {
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
            "specific_variance_proxy": 0.05,
            "specific_volatility_proxy": 0.2236067977,
        }
    ]


def test_persist_cpar_package_writes_neon_first_when_configured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "postgresql://example")
    monkeypatch.setattr(cpar_outputs.config, "neon_primary_model_data_enabled", lambda: True)
    monkeypatch.setattr(cpar_outputs, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(cpar_outputs, "resolve_dsn", lambda _dsn=None: "postgresql://example")

    def _fake_pg_write(_pg_conn, **kwargs):
        calls.append("neon")
        assert kwargs["package_run"]["package_run_id"] == "run_1"
        assert kwargs["package_run"]["data_authority"] == "neon"
        return {"status": "ok"}

    def _fake_sqlite_write(_conn, **kwargs):
        calls.append("sqlite")
        assert kwargs["package_run"]["package_date"] == "2026-03-14"
        assert kwargs["package_run"]["data_authority"] == "neon"
        return {"status": "ok"}

    monkeypatch.setattr(cpar_outputs.cpar_writers, "write_cpar_outputs_postgres", _fake_pg_write)
    monkeypatch.setattr(cpar_outputs.cpar_writers, "write_cpar_outputs_sqlite", _fake_sqlite_write)

    out = cpar_outputs.persist_cpar_package(
        data_db=tmp_path / "data.db",
        package_run=_package_run(),
        proxy_returns=_proxy_returns(),
        proxy_transforms=_proxy_transforms(),
        covariance_rows=_covariance_rows(),
        instrument_fits=_instrument_fits(),
    )

    assert calls == ["neon", "sqlite"]
    assert out["status"] == "ok"
    assert out["authority_store"] == "neon"
    assert out["neon_write"]["status"] == "ok"
    assert out["sqlite_mirror_write"]["status"] == "ok"


def test_persist_cpar_package_raises_when_required_neon_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "postgresql://example")
    monkeypatch.setattr(cpar_outputs.config, "neon_primary_model_data_enabled", lambda: True)
    monkeypatch.setattr(cpar_outputs, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(cpar_outputs, "resolve_dsn", lambda _dsn=None: "postgresql://example")
    monkeypatch.setattr(
        cpar_outputs.cpar_writers,
        "write_cpar_outputs_postgres",
        lambda _pg_conn, **_kwargs: calls.append("neon") or (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        cpar_outputs.cpar_writers,
        "write_cpar_outputs_sqlite",
        lambda _conn, **_kwargs: calls.append("sqlite") or {"status": "ok"},
    )

    with pytest.raises(RuntimeError, match="Neon cPAR package persistence failed"):
        cpar_outputs.persist_cpar_package(
            data_db=tmp_path / "data.db",
            package_run=_package_run(),
            proxy_returns=_proxy_returns(),
            proxy_transforms=_proxy_transforms(),
            covariance_rows=_covariance_rows(),
            instrument_fits=_instrument_fits(),
        )

    assert calls == ["neon"]


def test_persist_cpar_package_falls_back_to_sqlite_when_neon_optional(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "postgresql://example")
    monkeypatch.setattr(cpar_outputs.config, "neon_primary_model_data_enabled", lambda: False)
    monkeypatch.setattr(cpar_outputs, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(cpar_outputs, "resolve_dsn", lambda _dsn=None: "postgresql://example")
    monkeypatch.setattr(
        cpar_outputs.cpar_writers,
        "write_cpar_outputs_postgres",
        lambda _pg_conn, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        cpar_outputs.cpar_writers,
        "write_cpar_outputs_sqlite",
        lambda _conn, **kwargs: captured.update(package_run=kwargs["package_run"]) or {"status": "ok"},
    )

    out = cpar_outputs.persist_cpar_package(
        data_db=tmp_path / "data.db",
        package_run=_package_run(),
        proxy_returns=_proxy_returns(),
        proxy_transforms=_proxy_transforms(),
        covariance_rows=_covariance_rows(),
        instrument_fits=_instrument_fits(),
    )

    assert out["status"] == "ok"
    assert out["authority_store"] == "sqlite"
    assert out["neon_write"]["status"] == "error"
    assert out["sqlite_mirror_write"]["status"] == "ok"
    assert captured["package_run"]["data_authority"] == "sqlite"


def test_persist_cpar_package_uses_shared_source_prices_for_runtime_coverage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "postgresql://example")
    monkeypatch.setattr(cpar_outputs.config, "neon_primary_model_data_enabled", lambda: True)
    monkeypatch.setattr(cpar_outputs, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(cpar_outputs, "resolve_dsn", lambda _dsn=None: "postgresql://example")
    monkeypatch.setattr(
        cpar_outputs.cpar_source_reads,
        "load_latest_price_rows",
        lambda rics, **_kwargs: [
            {"ric": "AAPL.OQ", "date": "2026-03-14", "adj_close": 210.0, "close": 210.0}
            for ric in rics
            if str(ric).upper() == "AAPL.OQ"
        ],
    )

    monkeypatch.setattr(
        cpar_outputs.cpar_writers,
        "write_cpar_outputs_postgres",
        lambda _pg_conn, **kwargs: captured.update(runtime_coverage=kwargs["runtime_coverage"]) or {"status": "ok"},
    )
    monkeypatch.setattr(
        cpar_outputs.cpar_writers,
        "write_cpar_outputs_sqlite",
        lambda _conn, **_kwargs: {"status": "ok"},
    )

    out = cpar_outputs.persist_cpar_package(
        data_db=tmp_path / "data.db",
        package_run=_package_run(),
        proxy_returns=_proxy_returns(),
        proxy_transforms=_proxy_transforms(),
        covariance_rows=_covariance_rows(),
        instrument_fits=_instrument_fits(),
    )

    assert out["status"] == "ok"
    assert captured["runtime_coverage"] == [
        {
            "package_run_id": "run_1",
            "package_date": "2026-03-14",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "price_on_package_date_status": "present",
            "fit_row_status": "present",
            "fit_quality_status": "ok",
            "portfolio_use_status": "covered",
            "ticker_detail_use_status": "available",
            "hedge_use_status": "usable",
            "fit_family": "returns_regression_weekly",
            "fit_status": "ok",
            "reason_code": "ok",
            "quality_label": "ok",
            "warnings": [],
            "updated_at": captured["runtime_coverage"][0]["updated_at"],
        }
    ]
