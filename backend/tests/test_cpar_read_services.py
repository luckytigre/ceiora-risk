from __future__ import annotations

from pathlib import Path

import pytest

from backend.cpar.factor_registry import ordered_factor_ids
from backend.data import cpar_outputs
from backend.services import cpar_hedge_service, cpar_meta_service, cpar_search_service, cpar_ticker_service


def _configure_local_sqlite(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cpar_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(cpar_outputs.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cpar_outputs.config, "neon_dsn", lambda: "")


def _package_run(
    package_run_id: str,
    package_date: str,
    *,
    universe_count: int,
    fit_ok_count: int | None = None,
    fit_limited_count: int = 0,
    fit_insufficient_count: int = 0,
) -> dict[str, object]:
    return {
        "package_run_id": package_run_id,
        "package_date": package_date,
        "profile": "cpar-weekly",
        "status": "ok",
        "started_at": f"{package_date}T00:00:00Z",
        "completed_at": f"{package_date}T00:01:00Z",
        "method_version": "cPAR1",
        "factor_registry_version": "cPAR1_registry_v1",
        "lookback_weeks": 52,
        "half_life_weeks": 26,
        "min_observations": 39,
        "proxy_price_rule": "adj_close_fallback_close",
        "source_prices_asof": package_date,
        "classification_asof": package_date,
        "universe_count": universe_count,
        "fit_ok_count": universe_count if fit_ok_count is None else fit_ok_count,
        "fit_limited_count": fit_limited_count,
        "fit_insufficient_count": fit_insufficient_count,
        "data_authority": "sqlite",
        "error_type": None,
        "error_message": None,
    }


def _proxy_returns(package_run_id: str, package_date: str) -> list[dict[str, object]]:
    return [
        {
            "package_run_id": package_run_id,
            "package_date": package_date,
            "week_end": package_date,
            "factor_id": "SPY",
            "factor_group": "market",
            "proxy_ric": "SPY.P",
            "proxy_ticker": "SPY",
            "return_value": 0.01,
            "weight_value": 0.5,
            "price_field_used": "adj_close",
        }
    ]


def _proxy_transforms(package_run_id: str, package_date: str) -> list[dict[str, object]]:
    return [
        {
            "package_run_id": package_run_id,
            "package_date": package_date,
            "factor_id": "XLK",
            "factor_group": "sector",
            "proxy_ric": "XLK.P",
            "proxy_ticker": "XLK",
            "market_alpha": 0.001,
            "market_beta": 0.4,
        }
    ]


def _covariance_rows(package_run_id: str, package_date: str, factor_ids: list[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    all_factor_ids = tuple(dict.fromkeys([*ordered_factor_ids(), *factor_ids]))
    for left in all_factor_ids:
        for right in all_factor_ids:
            covariance = 1.0 if left == right else 0.2
            correlation = 1.0 if left == right else 0.2
            rows.append(
                {
                    "package_run_id": package_run_id,
                    "package_date": package_date,
                    "factor_id": left,
                    "factor_id_2": right,
                    "covariance": covariance,
                    "correlation": correlation,
                }
            )
    return rows


def _fit_row(
    package_run_id: str,
    package_date: str,
    *,
    ric: str,
    ticker: str | None,
    display_name: str,
    raw_loadings: dict[str, float],
    thresholded_loadings: dict[str, float],
    warnings: list[str] | None = None,
    fit_status: str = "ok",
    hq_country_code: str = "US",
) -> dict[str, object]:
    return {
        "package_run_id": package_run_id,
        "package_date": package_date,
        "ric": ric,
        "ticker": ticker,
        "display_name": display_name,
        "fit_status": fit_status,
        "warnings": list(warnings or []),
        "observed_weeks": 52,
        "lookback_weeks": 52,
        "longest_gap_weeks": 0,
        "price_field_used": "adj_close",
        "hq_country_code": hq_country_code,
        "market_step_alpha": 0.01,
        "market_step_beta": raw_loadings.get("SPY", 0.0),
        "block_alpha": 0.0,
        "spy_trade_beta_raw": raw_loadings.get("SPY", 0.0),
        "raw_loadings": dict(raw_loadings),
        "thresholded_loadings": dict(thresholded_loadings),
        "factor_variance_proxy": 0.2,
        "factor_volatility_proxy": 0.4472135955,
    }


def _seed_read_package_db(path: Path) -> None:
    cpar_outputs.persist_cpar_package(
        data_db=path,
        package_run=_package_run("run_prev", "2026-03-07", universe_count=1),
        proxy_returns=_proxy_returns("run_prev", "2026-03-07"),
        proxy_transforms=_proxy_transforms("run_prev", "2026-03-07"),
        covariance_rows=_covariance_rows("run_prev", "2026-03-07", ["SPY", "XLK", "XLF"]),
        instrument_fits=[
            _fit_row(
                "run_prev",
                "2026-03-07",
                ric="AAPL.OQ",
                ticker="AAPL",
                display_name="Apple Inc.",
                raw_loadings={"SPY": 1.0, "XLK": 0.30, "XLF": -0.10},
                thresholded_loadings={"SPY": 1.0, "XLK": 0.30, "XLF": -0.10},
            )
        ],
    )
    cpar_outputs.persist_cpar_package(
        data_db=path,
        package_run=_package_run(
            "run_curr",
            "2026-03-14",
            universe_count=4,
            fit_ok_count=2,
            fit_limited_count=2,
        ),
        proxy_returns=_proxy_returns("run_curr", "2026-03-14"),
        proxy_transforms=_proxy_transforms("run_curr", "2026-03-14"),
        covariance_rows=_covariance_rows("run_curr", "2026-03-14", ["SPY", "XLK", "XLF", "QUAL", "USMV"]),
        instrument_fits=[
            _fit_row(
                "run_curr",
                "2026-03-14",
                ric="AAPL.OQ",
                ticker="AAPL",
                display_name="Apple Inc.",
                raw_loadings={"SPY": 1.2, "XLK": 0.35, "XLF": -0.18, "QUAL": 0.12},
                thresholded_loadings={"SPY": 1.2, "XLK": 0.35, "XLF": -0.18, "QUAL": 0.12},
            ),
            _fit_row(
                "run_curr",
                "2026-03-14",
                ric="AAPL.L",
                ticker="AAPL",
                display_name="Apple ADR London",
                raw_loadings={"SPY": 0.9, "XLK": 0.20},
                thresholded_loadings={"SPY": 0.9, "XLK": 0.20},
                warnings=["ex_us_caution"],
                fit_status="limited_history",
                hq_country_code="GB",
            ),
            _fit_row(
                "run_curr",
                "2026-03-14",
                ric="AAPL.NA",
                ticker=None,
                display_name="Apple Inc. Synthetic Line",
                raw_loadings={"SPY": 1.05, "XLK": 0.28},
                thresholded_loadings={"SPY": 1.05, "XLK": 0.28},
                warnings=["continuity_gap"],
                fit_status="limited_history",
            ),
            _fit_row(
                "run_curr",
                "2026-03-14",
                ric="SAPG.DE",
                ticker="SAPG",
                display_name="SAP SE",
                raw_loadings={"SPY": 0.95, "XLK": 0.22, "XLF": -0.10, "USMV": 0.08},
                thresholded_loadings={"SPY": 0.95, "XLK": 0.22, "XLF": -0.10, "USMV": 0.08},
                warnings=["ex_us_caution"],
                hq_country_code="DE",
            ),
        ],
    )


def test_cpar_meta_service_returns_active_package_and_factor_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_sqlite(monkeypatch)
    data_db = tmp_path / "cpar.db"
    _seed_read_package_db(data_db)

    payload = cpar_meta_service.load_cpar_meta_payload(data_db=data_db)

    assert payload["package_run_id"] == "run_curr"
    assert payload["package_date"] == "2026-03-14"
    assert payload["factor_count"] == 17
    assert payload["factors"][0]["factor_id"] == "SPY"


def test_cpar_search_service_pins_one_package_for_the_response(monkeypatch: pytest.MonkeyPatch) -> None:
    package = _package_run("run_meta", "2026-03-14", universe_count=1)
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: package)
    monkeypatch.setattr(
        cpar_outputs,
        "search_active_package_instrument_fits",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("search_active_package_instrument_fits should not be used")),
    )

    observed: dict[str, object] = {}

    def fake_search(q: str, *, package_run_id: str, data_db=None):
        observed["package_run_id"] = package_run_id
        observed["query"] = q
        return [
            {
                "ticker": "AAPL",
                "ric": "AAPL.OQ",
                "display_name": "Apple Inc.",
                "fit_status": "ok",
                "warnings": [],
                "hq_country_code": "US",
            }
        ]

    monkeypatch.setattr(cpar_outputs, "search_package_instrument_fits", fake_search)

    payload = cpar_search_service.load_cpar_search_payload(q="aapl", limit=5)

    assert observed["package_run_id"] == "run_meta"
    assert payload["package_run_id"] == "run_meta"
    assert payload["results"][0]["ric"] == "AAPL.OQ"


def test_cpar_search_service_returns_active_package_hits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_sqlite(monkeypatch)
    data_db = tmp_path / "cpar.db"
    _seed_read_package_db(data_db)

    payload = cpar_search_service.load_cpar_search_payload(q="aapl", limit=10, data_db=data_db)

    assert payload["package_run_id"] == "run_curr"
    assert payload["total"] == 3
    assert {row["ric"] for row in payload["results"]} == {"AAPL.OQ", "AAPL.L", "AAPL.NA"}
    assert next(row for row in payload["results"] if row["ric"] == "AAPL.NA")["ticker"] is None


def test_cpar_ticker_service_requires_ric_for_ambiguous_tickers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_sqlite(monkeypatch)
    data_db = tmp_path / "cpar.db"
    _seed_read_package_db(data_db)

    with pytest.raises(cpar_meta_service.CparTickerAmbiguous, match="Ambiguous"):
        cpar_ticker_service.load_cpar_ticker_payload("AAPL", data_db=data_db)


def test_cpar_ticker_service_returns_ordered_detail_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_sqlite(monkeypatch)
    data_db = tmp_path / "cpar.db"
    _seed_read_package_db(data_db)

    payload = cpar_ticker_service.load_cpar_ticker_payload("AAPL", ric="AAPL.OQ", data_db=data_db)

    assert payload["ric"] == "AAPL.OQ"
    assert payload["beta_spy_trade"] == 1.2
    assert [row["factor_id"] for row in payload["raw_loadings"]] == ["SPY", "XLF", "XLK", "QUAL"]
    assert payload["pre_hedge_factor_variance_proxy"] == 0.2


def test_cpar_ticker_service_pins_one_package_for_the_response(monkeypatch: pytest.MonkeyPatch) -> None:
    package = _package_run("run_meta", "2026-03-14", universe_count=1)
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: package)
    monkeypatch.setattr(
        cpar_outputs,
        "load_active_package_instrument_fit",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("load_active_package_instrument_fit should not be used")),
    )

    observed: dict[str, object] = {}

    def fake_load(ticker: str, *, package_run_id: str, ric: str | None = None, data_db=None):
        observed["package_run_id"] = package_run_id
        observed["ticker"] = ticker
        observed["ric"] = ric
        return _fit_row(
            "run_meta",
            "2026-03-14",
            ric="AAPL.OQ",
            ticker="AAPL",
            display_name="Apple Inc.",
            raw_loadings={"SPY": 1.2, "XLK": 0.35},
            thresholded_loadings={"SPY": 1.2, "XLK": 0.35},
        )

    monkeypatch.setattr(cpar_outputs, "load_package_instrument_fit", fake_load)

    payload = cpar_ticker_service.load_cpar_ticker_payload("AAPL", ric="AAPL.OQ")

    assert observed["package_run_id"] == "run_meta"
    assert payload["package_run_id"] == "run_meta"
    assert payload["ric"] == "AAPL.OQ"


def test_cpar_hedge_service_returns_factor_neutral_preview_with_stability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_sqlite(monkeypatch)
    data_db = tmp_path / "cpar.db"
    _seed_read_package_db(data_db)

    payload = cpar_hedge_service.load_cpar_hedge_payload(
        "AAPL",
        ric="AAPL.OQ",
        mode="factor_neutral",
        data_db=data_db,
    )

    assert payload["mode"] == "factor_neutral"
    assert payload["hedge_status"] == "hedge_ok"
    assert payload["hedge_legs"][0]["factor_id"] == "SPY"
    assert payload["stability"]["leg_overlap_ratio"] is not None
    assert payload["post_hedge_exposures"][0]["factor_id"] == "SPY"


def test_cpar_hedge_service_pins_one_package_for_subject_and_covariance(monkeypatch: pytest.MonkeyPatch) -> None:
    package = _package_run("run_meta", "2026-03-14", universe_count=1)
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: package)
    monkeypatch.setattr(
        cpar_outputs,
        "load_active_package_instrument_fit",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("load_active_package_instrument_fit should not be used")),
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_active_package_covariance_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("load_active_package_covariance_rows should not be used")),
    )

    observed: dict[str, list[str]] = {"fit_packages": [], "covariance_packages": []}

    def fake_fit(ticker: str, *, package_run_id: str, ric: str | None = None, data_db=None):
        observed["fit_packages"].append(package_run_id)
        return _fit_row(
            "run_meta",
            "2026-03-14",
            ric="AAPL.OQ",
            ticker="AAPL",
            display_name="Apple Inc.",
            raw_loadings={"SPY": 1.2, "XLK": 0.35},
            thresholded_loadings={"SPY": 1.2, "XLK": 0.35},
        )

    def fake_covariance(package_run_id: str, *, data_db=None, require_complete: bool = False, context_label: str | None = None):
        observed["covariance_packages"].append(package_run_id)
        return _covariance_rows("run_meta", "2026-03-14", ["SPY", "XLK"])

    monkeypatch.setattr(cpar_outputs, "load_package_instrument_fit", fake_fit)
    monkeypatch.setattr(cpar_outputs, "load_package_covariance_rows", fake_covariance)
    monkeypatch.setattr(cpar_outputs, "load_previous_successful_instrument_fit", lambda **kwargs: None)

    payload = cpar_hedge_service.load_cpar_hedge_payload("AAPL", ric="AAPL.OQ", mode="factor_neutral")

    assert observed["fit_packages"] == ["run_meta"]
    assert observed["covariance_packages"] == ["run_meta"]
    assert payload["package_run_id"] == "run_meta"
    assert payload["hedge_status"] == "hedge_ok"


def test_cpar_hedge_service_maps_missing_active_covariance_to_not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cpar_meta_service,
        "require_active_package",
        lambda **kwargs: {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "profile": "cpar-weekly",
            "method_version": "cPAR1",
            "factor_registry_version": "cPAR1_registry_v1",
            "data_authority": "neon",
            "lookback_weeks": 52,
            "half_life_weeks": 26,
            "min_observations": 39,
            "source_prices_asof": "2026-03-14",
            "classification_asof": "2026-03-14",
            "universe_count": 1,
            "fit_ok_count": 1,
            "fit_limited_count": 0,
            "fit_insufficient_count": 0,
        },
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fit",
        lambda **kwargs: _fit_row(
            "run_curr",
            "2026-03-14",
            ric="AAPL.OQ",
            ticker="AAPL",
            display_name="Apple Inc.",
            raw_loadings={"SPY": 1.2, "XLK": 0.35},
            thresholded_loadings={"SPY": 1.2, "XLK": 0.35},
        ),
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_covariance_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            cpar_outputs.CparPackageNotReady(
                "Active cPAR package is missing covariance rows in the cloud-serve authority store."
            )
        ),
    )

    with pytest.raises(cpar_meta_service.CparReadNotReady, match="missing covariance rows"):
        cpar_hedge_service.load_cpar_hedge_payload("AAPL", ric="AAPL.OQ", mode="factor_neutral")


def test_cpar_hedge_service_maps_partial_active_covariance_to_not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cpar_meta_service,
        "require_active_package",
        lambda **kwargs: {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "profile": "cpar-weekly",
            "method_version": "cPAR1",
            "factor_registry_version": "cPAR1_registry_v1",
            "data_authority": "neon",
            "lookback_weeks": 52,
            "half_life_weeks": 26,
            "min_observations": 39,
            "source_prices_asof": "2026-03-14",
            "classification_asof": "2026-03-14",
            "universe_count": 1,
            "fit_ok_count": 1,
            "fit_limited_count": 0,
            "fit_insufficient_count": 0,
        },
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fit",
        lambda **kwargs: _fit_row(
            "run_curr",
            "2026-03-14",
            ric="AAPL.OQ",
            ticker="AAPL",
            display_name="Apple Inc.",
            raw_loadings={"SPY": 1.2, "XLK": 0.35},
            thresholded_loadings={"SPY": 1.2, "XLK": 0.35},
        ),
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_covariance_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            cpar_outputs.CparPackageNotReady(
                "Active cPAR package has incomplete covariance coverage for package_run_id=run_curr. Missing factor pairs include SPY/XLK."
            )
        ),
    )

    with pytest.raises(cpar_meta_service.CparReadNotReady, match="incomplete covariance coverage"):
        cpar_hedge_service.load_cpar_hedge_payload("AAPL", ric="AAPL.OQ", mode="factor_neutral")


def test_cpar_hedge_service_ignores_incomplete_previous_covariance_for_optional_stability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_meta_service,
        "require_active_package",
        lambda **kwargs: {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "profile": "cpar-weekly",
            "method_version": "cPAR1",
            "factor_registry_version": "cPAR1_registry_v1",
            "data_authority": "neon",
            "lookback_weeks": 52,
            "half_life_weeks": 26,
            "min_observations": 39,
            "source_prices_asof": "2026-03-14",
            "classification_asof": "2026-03-14",
            "universe_count": 1,
            "fit_ok_count": 1,
            "fit_limited_count": 0,
            "fit_insufficient_count": 0,
        },
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fit",
        lambda **kwargs: _fit_row(
            "run_curr",
            "2026-03-14",
            ric="AAPL.OQ",
            ticker="AAPL",
            display_name="Apple Inc.",
            raw_loadings={"SPY": 1.2, "XLK": 0.35},
            thresholded_loadings={"SPY": 1.2, "XLK": 0.35},
        ),
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_covariance_rows",
        lambda package_run_id, **kwargs: (
            _covariance_rows("run_curr", "2026-03-14", ["SPY", "XLK"])
            if package_run_id == "run_curr"
            else (_ for _ in ()).throw(
                cpar_outputs.CparPackageNotReady(
                    "Previous cPAR package used for hedge stability diagnostics has incomplete covariance coverage."
                )
            )
        ),
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_previous_successful_instrument_fit",
        lambda **kwargs: _fit_row(
            "run_prev",
            "2026-03-07",
            ric="AAPL.OQ",
            ticker="AAPL",
            display_name="Apple Inc.",
            raw_loadings={"SPY": 1.0, "XLK": 0.20},
            thresholded_loadings={"SPY": 1.0, "XLK": 0.20},
        ),
    )
    payload = cpar_hedge_service.load_cpar_hedge_payload(
        "AAPL",
        ric="AAPL.OQ",
        mode="factor_neutral",
    )

    assert payload["hedge_status"] == "hedge_ok"
    assert payload["hedge_legs"][0]["factor_id"] == "SPY"
    assert payload["stability"]["leg_overlap_ratio"] is None
    assert payload["stability"]["gross_hedge_notional_change"] is None
    assert payload["stability"]["net_hedge_notional_change"] is None


def test_cpar_hedge_service_ignores_unreadable_previous_covariance_for_optional_stability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_meta_service,
        "require_active_package",
        lambda **kwargs: {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "profile": "cpar-weekly",
            "method_version": "cPAR1",
            "factor_registry_version": "cPAR1_registry_v1",
            "data_authority": "neon",
            "lookback_weeks": 52,
            "half_life_weeks": 26,
            "min_observations": 39,
            "source_prices_asof": "2026-03-14",
            "classification_asof": "2026-03-14",
            "universe_count": 1,
            "fit_ok_count": 1,
            "fit_limited_count": 0,
            "fit_insufficient_count": 0,
        },
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fit",
        lambda **kwargs: _fit_row(
            "run_curr",
            "2026-03-14",
            ric="AAPL.OQ",
            ticker="AAPL",
            display_name="Apple Inc.",
            raw_loadings={"SPY": 1.2, "XLK": 0.35},
            thresholded_loadings={"SPY": 1.2, "XLK": 0.35},
        ),
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_covariance_rows",
        lambda package_run_id, **kwargs: (
            _covariance_rows("run_curr", "2026-03-14", ["SPY", "XLK"])
            if package_run_id == "run_curr"
            else (_ for _ in ()).throw(
                cpar_outputs.CparAuthorityReadError(
                    "Neon cPAR read failed during query execution: RuntimeError: connection lost"
                )
            )
        ),
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_previous_successful_instrument_fit",
        lambda **kwargs: _fit_row(
            "run_prev",
            "2026-03-07",
            ric="AAPL.OQ",
            ticker="AAPL",
            display_name="Apple Inc.",
            raw_loadings={"SPY": 1.0, "XLK": 0.20},
            thresholded_loadings={"SPY": 1.0, "XLK": 0.20},
        ),
    )
    payload = cpar_hedge_service.load_cpar_hedge_payload(
        "AAPL",
        ric="AAPL.OQ",
        mode="factor_neutral",
    )

    assert payload["hedge_status"] == "hedge_ok"
    assert payload["hedge_legs"][0]["factor_id"] == "SPY"
    assert payload["stability"]["leg_overlap_ratio"] is None
    assert payload["stability"]["gross_hedge_notional_change"] is None
    assert payload["stability"]["net_hedge_notional_change"] is None


def test_cpar_hedge_service_ignores_generic_previous_covariance_failure_for_optional_stability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_meta_service,
        "require_active_package",
        lambda **kwargs: {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "profile": "cpar-weekly",
            "method_version": "cPAR1",
            "factor_registry_version": "cPAR1_registry_v1",
            "data_authority": "neon",
            "lookback_weeks": 52,
            "half_life_weeks": 26,
            "min_observations": 39,
            "source_prices_asof": "2026-03-14",
            "classification_asof": "2026-03-14",
            "universe_count": 1,
            "fit_ok_count": 1,
            "fit_limited_count": 0,
            "fit_insufficient_count": 0,
        },
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fit",
        lambda **kwargs: _fit_row(
            "run_curr",
            "2026-03-14",
            ric="AAPL.OQ",
            ticker="AAPL",
            display_name="Apple Inc.",
            raw_loadings={"SPY": 1.2, "XLK": 0.35},
            thresholded_loadings={"SPY": 1.2, "XLK": 0.35},
        ),
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_covariance_rows",
        lambda package_run_id, **kwargs: (
            _covariance_rows("run_curr", "2026-03-14", ["SPY", "XLK"])
            if package_run_id == "run_curr"
            else (_ for _ in ()).throw(RuntimeError("previous covariance decode failed"))
        ),
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_previous_successful_instrument_fit",
        lambda **kwargs: _fit_row(
            "run_prev",
            "2026-03-07",
            ric="AAPL.OQ",
            ticker="AAPL",
            display_name="Apple Inc.",
            raw_loadings={"SPY": 1.0, "XLK": 0.20},
            thresholded_loadings={"SPY": 1.0, "XLK": 0.20},
        ),
    )
    payload = cpar_hedge_service.load_cpar_hedge_payload(
        "AAPL",
        ric="AAPL.OQ",
        mode="factor_neutral",
    )

    assert payload["hedge_status"] == "hedge_ok"
    assert payload["hedge_legs"][0]["factor_id"] == "SPY"
    assert payload["stability"]["leg_overlap_ratio"] is None
    assert payload["stability"]["gross_hedge_notional_change"] is None
    assert payload["stability"]["net_hedge_notional_change"] is None


def test_cpar_meta_service_fails_closed_when_no_package_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_sqlite(monkeypatch)

    with pytest.raises(cpar_meta_service.CparReadNotReady, match="No successful cPAR package"):
        cpar_meta_service.load_cpar_meta_payload(data_db=tmp_path / "empty.db")
