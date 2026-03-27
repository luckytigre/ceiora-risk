from __future__ import annotations

from pathlib import Path

import pytest

from backend.cpar.factor_registry import CPAR1_METHOD_VERSION
from backend.cpar.factor_registry import ordered_factor_ids
from backend.data import cpar_outputs


def _package_run(package_run_id: str, package_date: str) -> dict[str, object]:
    return {
        "package_run_id": package_run_id,
        "package_date": package_date,
        "profile": "cpar-weekly",
        "status": "ok",
        "started_at": f"{package_date}T00:00:00Z",
        "completed_at": f"{package_date}T00:01:00Z",
        "method_version": CPAR1_METHOD_VERSION,
        "factor_registry_version": "cPAR1",
        "lookback_weeks": 52,
        "half_life_weeks": 26,
        "min_observations": 39,
        "proxy_price_rule": "adj_close_fallback_close",
        "source_prices_asof": package_date,
        "classification_asof": package_date,
        "universe_count": 2,
        "fit_ok_count": 1,
        "fit_limited_count": 1,
        "fit_insufficient_count": 0,
        "data_authority": "sqlite",
        "error_type": None,
        "error_message": None,
    }


def _proxy_returns(package_run_id: str, package_date: str) -> list[dict[str, object]]:
    return [
        {
            "package_date": package_date,
            "week_end": "2026-03-13",
            "factor_id": "SPY",
            "factor_group": "market",
            "proxy_ric": "SPY.P",
            "proxy_ticker": "SPY",
            "return_value": 0.01,
            "weight_value": 0.5,
            "price_field_used": "adj_close",
            "package_run_id": package_run_id,
        }
    ]


def _proxy_transforms(package_run_id: str, package_date: str) -> list[dict[str, object]]:
    return [
        {
            "package_date": package_date,
            "factor_id": "XLF",
            "factor_group": "sector",
            "proxy_ric": "XLF.P",
            "proxy_ticker": "XLF",
            "market_alpha": 0.001,
            "market_beta": 0.4,
            "package_run_id": package_run_id,
        }
    ]


def _covariance_rows(package_run_id: str, package_date: str, *, covariance: float) -> list[dict[str, object]]:
    factor_ids = ordered_factor_ids()
    rows: list[dict[str, object]] = []
    for left in factor_ids:
        for right in factor_ids:
            rows.append(
                {
                    "package_date": package_date,
                    "factor_id": left,
                    "factor_id_2": right,
                    "covariance": covariance if left == right else round(covariance * 0.1, 6),
                    "correlation": 1.0 if left == right else 0.1,
                    "package_run_id": package_run_id,
                }
            )
    return rows


def _instrument_fits(package_run_id: str, package_date: str, *, spy_beta: float) -> list[dict[str, object]]:
    return [
        {
            "package_date": package_date,
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
            "allow_cpar_core_target": 1,
            "allow_cpar_extended_target": 1,
            "is_single_name_equity": 1,
            "market_step_alpha": 0.01,
            "market_step_beta": 1.2,
            "block_alpha": 0.0,
            "spy_trade_beta_raw": spy_beta,
            "raw_loadings": {"SPY": spy_beta},
            "thresholded_loadings": {"SPY": spy_beta},
            "factor_variance_proxy": 0.2,
            "factor_volatility_proxy": 0.4472135955,
            "specific_variance_proxy": 0.05,
            "specific_volatility_proxy": 0.2236067977,
            "package_run_id": package_run_id,
        }
    ]


def test_local_sqlite_persist_and_read_roundtrip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_db = tmp_path / "cpar.db"
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")

    out = cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=_package_run("run_1", "2026-03-14"),
        proxy_returns=_proxy_returns("run_1", "2026-03-14"),
        proxy_transforms=_proxy_transforms("run_1", "2026-03-14"),
        covariance_rows=_covariance_rows("run_1", "2026-03-14", covariance=1.0),
        instrument_fits=_instrument_fits("run_1", "2026-03-14", spy_beta=1.1),
    )

    assert out["status"] == "ok"
    assert out["authority_store"] == "sqlite"
    assert out["neon_write"]["status"] == "skipped"
    assert out["sqlite_mirror_write"]["status"] == "ok"
    assert out["row_counts"]["cpar_instrument_fits_weekly"] == 1

    package = cpar_outputs.require_active_package_run(data_db=data_db)
    assert package["package_run_id"] == "run_1"

    fit = cpar_outputs.load_active_package_instrument_fit("AAPL", data_db=data_db)
    assert fit is not None
    assert fit["ric"] == "AAPL.OQ"
    assert fit["thresholded_loadings"] == {"SPY": 1.1}
    assert fit["target_scope"] == "core_us_equity"
    assert fit["portfolio_use_status"] == "missing_price"
    assert fit["hedge_use_status"] == "missing_price"
    assert fit["quality_label"] == "missing_price"

    cov_rows = cpar_outputs.load_active_package_covariance_rows(data_db=data_db)
    assert len(cov_rows) == len(ordered_factor_ids()) ** 2
    spy_row = next(row for row in cov_rows if row["factor_id"] == "SPY" and row["factor_id_2"] == "SPY")
    assert spy_row == {
        "factor_id": "SPY",
        "factor_id_2": "SPY",
        "covariance": 1.0,
        "correlation": 1.0,
        "package_run_id": "run_1",
        "updated_at": spy_row["updated_at"],
    }


def test_package_membership_keeps_us_non_equity_in_extended_scope() -> None:
    rows = cpar_outputs._derive_package_membership_rows(
        instrument_fits=[
            {
                "package_run_id": "run_1",
                "package_date": "2026-03-14",
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "hq_country_code": "US",
                "allow_cpar_core_target": 1,
                "is_single_name_equity": 1,
                "warnings": [],
                "updated_at": "2026-03-15T00:00:00Z",
            },
            {
                "package_run_id": "run_1",
                "package_date": "2026-03-14",
                "ric": "SPY.P",
                "ticker": "SPY",
                "hq_country_code": "US",
                "allow_cpar_core_target": 0,
                "is_single_name_equity": 0,
                "warnings": [],
                "updated_at": "2026-03-15T00:00:00Z",
            },
            {
                "package_run_id": "run_1",
                "package_date": "2026-03-14",
                "ric": "ARKK.P",
                "ticker": "ARKK",
                "hq_country_code": "US",
                "allow_cpar_core_target": 0,
                "is_single_name_equity": 0,
                "warnings": [],
                "updated_at": "2026-03-15T00:00:00Z",
            },
        ]
    )

    by_ticker = {row["ticker"]: row for row in rows}
    assert by_ticker["AAPL"]["target_scope"] == "core_us_equity"
    assert by_ticker["SPY"]["target_scope"] == "factor_basis_only"
    assert by_ticker["ARKK"]["target_scope"] == "extended_priced_instrument"


def test_persist_replaces_child_rows_for_same_package_date(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_db = tmp_path / "cpar.db"
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")

    cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=_package_run("run_1", "2026-03-14"),
        proxy_returns=_proxy_returns("run_1", "2026-03-14"),
        proxy_transforms=_proxy_transforms("run_1", "2026-03-14"),
        covariance_rows=_covariance_rows("run_1", "2026-03-14", covariance=1.0),
        instrument_fits=_instrument_fits("run_1", "2026-03-14", spy_beta=1.1),
    )
    cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=_package_run("run_2", "2026-03-14"),
        proxy_returns=_proxy_returns("run_2", "2026-03-14"),
        proxy_transforms=_proxy_transforms("run_2", "2026-03-14"),
        covariance_rows=_covariance_rows("run_2", "2026-03-14", covariance=1.5),
        instrument_fits=_instrument_fits("run_2", "2026-03-14", spy_beta=0.9),
    )

    package = cpar_outputs.require_active_package_run(data_db=data_db)
    fit = cpar_outputs.load_active_package_instrument_fit("AAPL", data_db=data_db)
    cov_rows = cpar_outputs.load_active_package_covariance_rows(data_db=data_db)

    assert package["package_run_id"] == "run_2"
    assert fit is not None
    assert fit["package_run_id"] == "run_2"
    assert fit["spy_trade_beta_raw"] == 0.9
    assert len(cov_rows) == len(ordered_factor_ids()) ** 2
    spy_row = next(row for row in cov_rows if row["factor_id"] == "SPY" and row["factor_id_2"] == "SPY")
    assert spy_row["package_run_id"] == "run_2"
    assert spy_row["covariance"] == 1.5


def test_persist_marks_us_fund_vehicle_as_extended_instead_of_core(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_db = tmp_path / "cpar.db"
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")

    instrument_fits = _instrument_fits("run_1", "2026-03-14", spy_beta=1.1) + [
        {
            "package_date": "2026-03-14",
            "ric": "SPY.P",
            "ticker": "SPY",
            "display_name": "SPDR S&P 500 ETF Trust",
            "fit_status": "ok",
            "warnings": [],
            "observed_weeks": 52,
            "lookback_weeks": 52,
            "longest_gap_weeks": 0,
            "price_field_used": "adj_close",
            "hq_country_code": "US",
            "allow_cpar_core_target": 0,
            "allow_cpar_extended_target": 1,
            "is_single_name_equity": 0,
            "market_step_alpha": 0.0,
            "market_step_beta": 1.0,
            "block_alpha": 0.0,
            "spy_trade_beta_raw": 1.0,
            "raw_loadings": {"SPY": 1.0},
            "thresholded_loadings": {"SPY": 1.0},
            "factor_variance_proxy": 0.2,
            "factor_volatility_proxy": 0.4472135955,
            "specific_variance_proxy": 0.05,
            "specific_volatility_proxy": 0.2236067977,
            "package_run_id": "run_1",
        }
    ]

    cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=_package_run("run_1", "2026-03-14"),
        proxy_returns=_proxy_returns("run_1", "2026-03-14"),
        proxy_transforms=_proxy_transforms("run_1", "2026-03-14"),
        covariance_rows=_covariance_rows("run_1", "2026-03-14", covariance=1.0),
        instrument_fits=instrument_fits,
    )

    fit = cpar_outputs.load_active_package_instrument_fit("SPY", data_db=data_db)
    assert fit is not None
    assert fit["target_scope"] == "extended_priced_instrument"


def test_same_date_failed_rerun_does_not_clobber_prior_successful_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_db = tmp_path / "cpar.db"
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")

    cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=_package_run("run_ok", "2026-03-14"),
        proxy_returns=_proxy_returns("run_ok", "2026-03-14"),
        proxy_transforms=_proxy_transforms("run_ok", "2026-03-14"),
        covariance_rows=_covariance_rows("run_ok", "2026-03-14", covariance=1.0),
        instrument_fits=_instrument_fits("run_ok", "2026-03-14", spy_beta=1.1),
    )
    failed_run = _package_run("run_failed", "2026-03-14")
    failed_run["status"] = "failed"
    failed_run["error_type"] = "RuntimeError"
    failed_run["error_message"] = "boom"
    cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=failed_run,
        proxy_returns=_proxy_returns("run_failed", "2026-03-14"),
        proxy_transforms=_proxy_transforms("run_failed", "2026-03-14"),
        covariance_rows=_covariance_rows("run_failed", "2026-03-14", covariance=1.5),
        instrument_fits=_instrument_fits("run_failed", "2026-03-14", spy_beta=0.9),
    )

    package = cpar_outputs.require_active_package_run(data_db=data_db)
    fit = cpar_outputs.load_active_package_instrument_fit("AAPL", data_db=data_db)
    cov_rows = cpar_outputs.load_active_package_covariance_rows(data_db=data_db)

    assert package["package_run_id"] == "run_ok"
    assert fit is not None
    assert fit["package_run_id"] == "run_ok"
    assert fit["spy_trade_beta_raw"] == 1.1
    spy_row = next(row for row in cov_rows if row["factor_id"] == "SPY" and row["factor_id_2"] == "SPY")
    assert spy_row["package_run_id"] == "run_ok"
    assert spy_row["covariance"] == 1.0


def test_load_factor_return_history_dedupes_by_latest_successful_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_db = tmp_path / "cpar.db"
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")

    cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=_package_run("run_1", "2026-03-14"),
        proxy_returns=[
            {
                "package_date": "2026-03-14",
                "week_end": "2026-03-07",
                "factor_id": "SPY",
                "factor_group": "market",
                "proxy_ric": "SPY.P",
                "proxy_ticker": "SPY",
                "return_value": 0.01,
                "weight_value": 1.0,
                "price_field_used": "adj_close",
                "package_run_id": "run_1",
            },
            {
                "package_date": "2026-03-14",
                "week_end": "2026-03-14",
                "factor_id": "SPY",
                "factor_group": "market",
                "proxy_ric": "SPY.P",
                "proxy_ticker": "SPY",
                "return_value": -0.02,
                "weight_value": 1.0,
                "price_field_used": "adj_close",
                "package_run_id": "run_1",
            },
        ],
        proxy_transforms=_proxy_transforms("run_1", "2026-03-14"),
        covariance_rows=_covariance_rows("run_1", "2026-03-14", covariance=1.0),
        instrument_fits=_instrument_fits("run_1", "2026-03-14", spy_beta=1.1),
    )
    cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=_package_run("run_2", "2026-03-21"),
        proxy_returns=[
            {
                "package_date": "2026-03-21",
                "week_end": "2026-03-14",
                "factor_id": "SPY",
                "factor_group": "market",
                "proxy_ric": "SPY.P",
                "proxy_ticker": "SPY",
                "return_value": -0.03,
                "weight_value": 1.0,
                "price_field_used": "adj_close",
                "package_run_id": "run_2",
            },
            {
                "package_date": "2026-03-21",
                "week_end": "2026-03-21",
                "factor_id": "SPY",
                "factor_group": "market",
                "proxy_ric": "SPY.P",
                "proxy_ticker": "SPY",
                "return_value": 0.04,
                "weight_value": 1.0,
                "price_field_used": "adj_close",
                "package_run_id": "run_2",
            },
        ],
        proxy_transforms=_proxy_transforms("run_2", "2026-03-21"),
        covariance_rows=_covariance_rows("run_2", "2026-03-21", covariance=1.0),
        instrument_fits=_instrument_fits("run_2", "2026-03-21", spy_beta=1.1),
    )

    latest, rows = cpar_outputs.load_factor_return_history("SPY", years=5, data_db=data_db)

    assert latest == "2026-03-21"
    assert rows == [
        ("2026-03-07", 0.01),
        ("2026-03-14", -0.03),
        ("2026-03-21", 0.04),
    ]


def test_child_rows_are_forced_to_parent_package_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_db = tmp_path / "cpar.db"
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")

    bad_fit = _instrument_fits("wrong_run", "2026-03-07", spy_beta=1.1)[0]
    bad_fit["package_run_id"] = "wrong_run"
    bad_fit["package_date"] = "2026-03-07"
    bad_cov_rows = _covariance_rows("wrong_run", "2026-03-07", covariance=1.0)
    for row in bad_cov_rows:
        row["package_run_id"] = "wrong_run"
        row["package_date"] = "2026-03-07"
    bad_proxy_return = _proxy_returns("wrong_run", "2026-03-07")[0]
    bad_proxy_return["package_run_id"] = "wrong_run"
    bad_proxy_return["package_date"] = "2026-03-07"
    bad_proxy_transform = _proxy_transforms("wrong_run", "2026-03-07")[0]
    bad_proxy_transform["package_run_id"] = "wrong_run"
    bad_proxy_transform["package_date"] = "2026-03-07"

    cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=_package_run("run_1", "2026-03-14"),
        proxy_returns=[bad_proxy_return],
        proxy_transforms=[bad_proxy_transform],
        covariance_rows=bad_cov_rows,
        instrument_fits=[bad_fit],
    )

    package = cpar_outputs.require_active_package_run(data_db=data_db)
    fit = cpar_outputs.load_active_package_instrument_fit("AAPL", data_db=data_db)
    cov_rows = cpar_outputs.load_active_package_covariance_rows(data_db=data_db)

    assert package["package_run_id"] == "run_1"
    assert fit is not None
    assert fit["package_run_id"] == "run_1"
    assert fit["package_date"] == "2026-03-14"
    spy_row = next(row for row in cov_rows if row["factor_id"] == "SPY" and row["factor_id_2"] == "SPY")
    assert spy_row["package_run_id"] == "run_1"
