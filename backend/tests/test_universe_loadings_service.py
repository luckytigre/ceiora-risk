from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.analytics.services.universe_loadings import (
    build_universe_ticker_loadings,
    load_latest_factor_coverage,
)
from backend.risk_model.factor_catalog import build_factor_catalog_for_factors, factor_id_for_name
from backend.risk_model.projected_loadings import ProjectedLoadingResult
from backend.universe.schema import ensure_cuse4_schema


def test_load_latest_factor_coverage_reads_latest_day(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            cross_section_n INTEGER,
            eligible_n INTEGER,
            coverage REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO daily_factor_returns (date, factor_name, cross_section_n, eligible_n, coverage) VALUES (?, ?, ?, ?, ?)",
        [
            ("2026-03-01", "Beta", 100, 95, 0.95),
            ("2026-03-02", "Beta", 101, 96, 0.96),
            ("2026-03-02", "Book-to-Price", 101, 94, 0.93),
        ],
    )
    conn.commit()
    conn.close()

    latest, cov = load_latest_factor_coverage(cache_db)
    assert latest == "2026-03-02"
    assert cov["Beta"] == {"cross_section_n": 101, "eligible_n": 96, "coverage_pct": 0.96}
    assert cov["Book-to-Price"] == {"cross_section_n": 101, "eligible_n": 94, "coverage_pct": 0.93}


def test_load_latest_factor_coverage_prefers_durable_model_outputs(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    data_db = tmp_path / "data.db"

    conn = sqlite3.connect(str(cache_db))
    conn.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            cross_section_n INTEGER,
            eligible_n INTEGER,
            coverage REAL
        )
        """
    )
    conn.execute(
        "INSERT INTO daily_factor_returns (date, factor_name, cross_section_n, eligible_n, coverage) VALUES (?, ?, ?, ?, ?)",
        ("2017-12-22", "Beta", 2669, 2685, 0.994041),
    )
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE model_factor_returns_daily (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL,
            robust_se REAL NOT NULL DEFAULT 0.0,
            t_stat REAL NOT NULL DEFAULT 0.0,
            r_squared REAL NOT NULL DEFAULT 0.0,
            residual_vol REAL NOT NULL DEFAULT 0.0,
            cross_section_n INTEGER NOT NULL DEFAULT 0,
            eligible_n INTEGER NOT NULL DEFAULT 0,
            coverage REAL NOT NULL DEFAULT 0.0,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO model_factor_returns_daily (
            date, factor_name, factor_return, cross_section_n, eligible_n, coverage, run_id, updated_at
        ) VALUES (?, ?, 0.0, ?, ?, ?, 'run_1', '2026-03-17T00:00:00Z')
        """,
        [
            ("2026-03-13", "Beta", 3446, 3455, 0.997395),
            ("2026-03-13", "Book-to-Price", 3446, 3455, 0.997395),
        ],
    )
    conn.commit()
    conn.close()

    latest, cov = load_latest_factor_coverage(cache_db, data_db=data_db)

    assert latest == "2026-03-13"
    assert cov["Beta"] == {"cross_section_n": 3446, "eligible_n": 3455, "coverage_pct": 0.997395}
    assert cov["Book-to-Price"] == {"cross_section_n": 3446, "eligible_n": 3455, "coverage_pct": 0.997395}


def test_build_universe_ticker_loadings_empty_inputs(tmp_path: Path) -> None:
    out = build_universe_ticker_loadings(
        exposures_df=pd.DataFrame(),
        fundamentals_df=pd.DataFrame(),
        prices_df=pd.DataFrame(),
        cov=pd.DataFrame(),
        data_db=tmp_path / "data.db",
    )
    assert out["ticker_count"] == 0
    assert out["eligible_ticker_count"] == 0
    assert out["factor_count"] == 0
    assert out["factors"] == []
    assert out["by_ticker"] == {}


def test_build_universe_ticker_loadings_surfaces_projection_only_instrument_as_unavailable(tmp_path: Path) -> None:
    out = build_universe_ticker_loadings(
        exposures_df=pd.DataFrame(),
        fundamentals_df=pd.DataFrame(),
        prices_df=pd.DataFrame([{"ric": "SPY.P", "ticker": "SPY", "close": 500.0}]),
        cov=pd.DataFrame(),
        data_db=tmp_path / "data.db",
        projected_loadings={},
        projection_universe_rows=[{"ric": "SPY.P", "ticker": "SPY"}],
        projection_core_state_through_date="2026-03-13",
    )

    spy = out["by_ticker"]["SPY"]
    assert spy["exposure_origin"] == "projected"
    assert spy["model_status"] == "ineligible"
    assert spy["model_status_reason"] == "projection_unavailable"
    assert spy["projection_asof"] == "2026-03-13"
    assert spy["exposures"] == {}


def test_build_universe_ticker_loadings_excludes_non_runtime_source_ghosts(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 'active', 'security_registry_seed', '2026-03-20T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES (
            'AAPL.OQ', 1, 1, 1,
            1, 0, 0,
            1, 1, 'registry_seed_defaults', '2026-03-20T00:00:00Z'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, issuer_country_code, model_home_market_scope,
            is_single_name_equity, classification_ready, source, updated_at
        ) VALUES (
            'AAPL.OQ', 'single_name_equity', 'equity_security', 'US', 'us',
            1, 1, 'security_registry_seed', '2026-03-20T00:00:00Z'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_source_observation_daily (
            as_of_date, ric, classification_ready, is_equity_eligible,
            price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            has_price_history_as_of_date, has_fundamentals_history_as_of_date, has_classification_history_as_of_date,
            latest_price_date, latest_fundamentals_as_of_date, latest_classification_as_of_date,
            source, updated_at
        ) VALUES (
            '2026-03-20', 'AAPL.OQ', 1, 1,
            1, 1, 1,
            1, 1, 1,
            '2026-03-20', '2026-03-20', '2026-03-20',
            'security_registry_seed', '2026-03-20T00:00:00Z'
        )
        """
    )
    conn.commit()
    conn.close()

    out = build_universe_ticker_loadings(
        exposures_df=pd.DataFrame(),
        fundamentals_df=pd.DataFrame(),
        prices_df=pd.DataFrame(
            [
                {"ric": "AAPL.OQ", "ticker": "AAPL", "close": 190.0},
                {"ric": "URA.P", "ticker": "URA", "close": 45.0},
            ]
        ),
        cov=pd.DataFrame(),
        data_db=data_db,
    )

    assert "AAPL" in out["by_ticker"]
    assert "URA" not in out["by_ticker"]


def test_build_universe_ticker_loadings_prefers_primary_ric_for_duplicate_ticker_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    eligibility_df = pd.DataFrame(
        [
            {
                "ric": "IBKR.OQ",
                "is_structural_eligible": True,
                "exclusion_reason": "",
                "market_cap": 1000.0,
                "trbc_business_sector": "Banking & Investment Services",
                "trbc_industry_group": "Investment Banking & Investment Services",
                "trbc_economic_sector_short": "Financials",
                "hq_country_code": "US",
            }
        ]
    ).set_index("ric")
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.build_eligibility_context",
        lambda data_db, dates: object(),
    )
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.structural_eligibility_for_date",
        lambda ctx, as_of_date: (None, eligibility_df),
    )

    exposures_df = pd.DataFrame(
        [
            {"ric": "IBKR.O", "ticker": "IBKR", "as_of_date": "2026-03-13", "beta_score": 0.1, "size_score": 0.2},
            {"ric": "IBKR.OQ", "ticker": "IBKR", "as_of_date": "2026-03-13", "beta_score": 0.1, "size_score": 0.2},
        ]
    )
    fundamentals_df = pd.DataFrame(
        [
            {
                "ric": "IBKR.O",
                "ticker": "IBKR",
                "market_cap": 1000.0,
                "trbc_business_sector": "Banking & Investment Services",
                "trbc_industry_group": "Investment Banking & Investment Services",
                "trbc_economic_sector_short": "Financials",
                "company_name": "Interactive Brokers Group Inc",
            },
            {
                "ric": "IBKR.OQ",
                "ticker": "IBKR",
                "market_cap": 1000.0,
                "trbc_business_sector": "Banking & Investment Services",
                "trbc_industry_group": "Investment Banking & Investment Services",
                "trbc_economic_sector_short": "Financials",
                "company_name": "Interactive Brokers Group Inc",
            },
        ]
    )
    prices_df = pd.DataFrame(
        [
            {"ric": "IBKR.O", "ticker": "IBKR", "close": 100.0},
            {"ric": "IBKR.OQ", "ticker": "IBKR", "close": 100.0},
        ]
    )
    cov = pd.DataFrame(
        np.eye(4, dtype=float),
        index=["Market", "Banking & Investment Services", "Size", "Beta"],
        columns=["Market", "Banking & Investment Services", "Size", "Beta"],
    )

    out = build_universe_ticker_loadings(
        exposures_df=exposures_df,
        fundamentals_df=fundamentals_df,
        prices_df=prices_df,
        cov=cov,
        data_db=tmp_path / "data.db",
        factor_catalog_by_name=build_factor_catalog_for_factors(
            ["Market", "Banking & Investment Services", "Size", "Beta"],
            method_version="test_method",
        ),
    )

    ibkr = out["by_ticker"]["IBKR"]
    assert ibkr["ric"] == "IBKR.OQ"
    assert ibkr["model_status"] == "core_estimated"


def test_build_universe_ticker_loadings_prefers_well_covered_snapshot(monkeypatch, tmp_path: Path) -> None:
    style_cols = {
        "size_score": 0.1,
        "beta_score": 0.2,
        "growth_score": 0.3,
        "investment_score": 0.4,
        "resid_vol_score": 0.5,
    }
    exposure_rows: list[dict[str, object]] = []
    fundamentals_rows: list[dict[str, object]] = []
    prices_rows: list[dict[str, object]] = []
    eligibility_rows: list[dict[str, object]] = []

    tickers = [f"T{i:03d}" for i in range(100)] + ["LAZ"]
    for idx, ticker in enumerate(tickers):
        ric = f"{ticker}.N"
        exposure_rows.append(
            {
                "ric": ric,
                "ticker": ticker,
                "as_of_date": "2026-03-03",
                **style_cols,
            }
        )
        fundamentals_rows.append(
            {
                "ric": ric,
                "ticker": ticker,
                "market_cap": 1000.0 + idx,
                "trbc_business_sector": "Technology Equipment",
                "trbc_industry_group": "Semiconductors & Semiconductor Equipment",
                "trbc_economic_sector_short": "Technology",
                "company_name": ticker,
            }
        )
        prices_rows.append(
            {
                "ric": ric,
                "ticker": ticker,
                "close": 100.0 + idx,
            }
        )
        eligibility_rows.append(
            {
                "ric": ric,
                "is_structural_eligible": True,
                "exclusion_reason": "",
                "market_cap": 1000.0 + idx,
                "trbc_business_sector": "Technology Equipment",
                "trbc_industry_group": "Semiconductors & Semiconductor Equipment",
                "trbc_economic_sector_short": "Technology",
                "hq_country_code": "US",
            }
        )

    for ticker in ["LAZ", "APP"]:
        exposure_rows.append(
            {
                "ric": f"{ticker}.N",
                "ticker": ticker,
                "as_of_date": "2026-03-04",
                "size_score": None,
                "beta_score": None,
                "growth_score": None,
                "investment_score": None,
                "resid_vol_score": None,
            }
        )

    eligibility_df = pd.DataFrame(eligibility_rows).set_index("ric")
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.build_eligibility_context",
        lambda data_db, dates: object(),
    )
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.structural_eligibility_for_date",
        lambda ctx, as_of_date: (None, eligibility_df),
    )

    cov = pd.DataFrame(
        np.eye(7, dtype=float),
        index=[
            "Market",
            "Technology Equipment",
            "Size",
            "Beta",
            "Growth",
            "Investment",
            "Residual Volatility",
        ],
        columns=[
            "Market",
            "Technology Equipment",
            "Size",
            "Beta",
            "Growth",
            "Investment",
            "Residual Volatility",
        ],
    )

    out = build_universe_ticker_loadings(
        exposures_df=pd.DataFrame(exposure_rows),
        fundamentals_df=pd.DataFrame(fundamentals_rows),
        prices_df=pd.DataFrame(prices_rows),
        cov=cov,
        data_db=tmp_path / "data.db",
    )

    laz = out["by_ticker"]["LAZ"]
    assert laz["as_of_date"] == "2026-03-03"
    assert laz["model_status"] == "core_estimated"
    assert laz["exposures"]["market"] == 1.0
    assert laz["exposures"][factor_id_for_name("Technology Equipment", family="industry")] == 1.0
    assert factor_id_for_name("Growth", family="style", source_column="growth_score") in laz["exposures"]
    assert any(entry["factor_id"] == "market" for entry in out["factor_catalog"])


def test_build_universe_ticker_loadings_downgrades_structural_names_without_factor_vectors(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exposures_df = pd.DataFrame(
        [
            {
                "ric": "LAZ.N",
                "ticker": "LAZ",
                "as_of_date": "2026-03-03",
                "size_score": None,
                "beta_score": None,
                "growth_score": None,
                "investment_score": None,
                "resid_vol_score": None,
            }
        ]
    )
    fundamentals_df = pd.DataFrame(
        [
            {
                "ric": "LAZ.N",
                "ticker": "LAZ",
                "market_cap": 1000.0,
                "trbc_business_sector": "Banking & Investment Services",
                "trbc_industry_group": "Investment Banking & Investment Services",
                "trbc_economic_sector_short": "Financials",
                "company_name": "Lazard",
            }
        ]
    )
    prices_df = pd.DataFrame(
        [{"ric": "LAZ.N", "ticker": "LAZ", "close": 48.39}]
    )
    eligibility_df = pd.DataFrame(
        [
            {
                "ric": "LAZ.N",
                "is_structural_eligible": True,
                "exclusion_reason": "",
                "market_cap": 1000.0,
                "trbc_business_sector": "Banking & Investment Services",
                "trbc_industry_group": "Investment Banking & Investment Services",
                "trbc_economic_sector_short": "Financials",
                "hq_country_code": "US",
            }
        ]
    ).set_index("ric")
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.build_eligibility_context",
        lambda data_db, dates: object(),
    )
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.structural_eligibility_for_date",
        lambda ctx, as_of_date: (None, eligibility_df),
    )

    cov = pd.DataFrame(
        np.eye(5, dtype=float),
        index=["Size", "Beta", "Growth", "Investment", "Residual Volatility"],
        columns=["Size", "Beta", "Growth", "Investment", "Residual Volatility"],
    )

    out = build_universe_ticker_loadings(
        exposures_df=exposures_df,
        fundamentals_df=fundamentals_df,
        prices_df=prices_df,
        cov=cov,
        data_db=tmp_path / "data.db",
    )

    laz = out["by_ticker"]["LAZ"]
    assert laz["model_status"] == "ineligible"
    assert laz["exposures"] == {}
    assert laz["model_status_reason"] == "missing_factor_exposures"
    assert laz["eligibility_reason"] == "missing_factor_exposures"


def test_build_universe_ticker_loadings_marks_non_us_names_projected_only(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exposures_df = pd.DataFrame(
        [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "as_of_date": "2026-03-03",
                "size_score": 0.1,
                "beta_score": 0.2,
            },
            {
                "ric": "BABA.N",
                "ticker": "BABA",
                "as_of_date": "2026-03-03",
                "size_score": -0.1,
                "beta_score": 0.3,
            },
        ]
    )
    fundamentals_df = pd.DataFrame(
        [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "market_cap": 1000.0,
                "trbc_business_sector": "Technology Equipment",
                "trbc_industry_group": "Semiconductors & Semiconductor Equipment",
                "trbc_economic_sector_short": "Technology",
                "company_name": "Apple",
            },
            {
                "ric": "BABA.N",
                "ticker": "BABA",
                "market_cap": 900.0,
                "trbc_business_sector": "Retailers",
                "trbc_industry_group": "Internet Retail",
                "trbc_economic_sector_short": "Consumer Cyclicals",
                "company_name": "Alibaba",
            },
        ]
    )
    prices_df = pd.DataFrame(
        [
            {"ric": "AAPL.OQ", "ticker": "AAPL", "close": 190.0},
            {"ric": "BABA.N", "ticker": "BABA", "close": 75.0},
        ]
    )
    eligibility_df = pd.DataFrame(
        [
            {
                "ric": "AAPL.OQ",
                "is_structural_eligible": True,
                "exclusion_reason": "",
                "market_cap": 1000.0,
                "trbc_business_sector": "Technology Equipment",
                "trbc_industry_group": "Semiconductors & Semiconductor Equipment",
                "trbc_economic_sector_short": "Technology",
                "hq_country_code": "US",
            },
            {
                "ric": "BABA.N",
                "is_structural_eligible": True,
                "exclusion_reason": "",
                "market_cap": 900.0,
                "trbc_business_sector": "Retailers",
                "trbc_industry_group": "Internet Retail",
                "trbc_economic_sector_short": "Consumer Cyclicals",
                "hq_country_code": "CN",
            },
        ]
    ).set_index("ric")
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.build_eligibility_context",
        lambda data_db, dates: object(),
    )
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.structural_eligibility_for_date",
        lambda ctx, as_of_date: (None, eligibility_df),
    )

    cov = pd.DataFrame(
        np.eye(3, dtype=float),
        index=["Market", "Size", "Beta"],
        columns=["Market", "Size", "Beta"],
    )

    out = build_universe_ticker_loadings(
        exposures_df=exposures_df,
        fundamentals_df=fundamentals_df,
        prices_df=prices_df,
        cov=cov,
        data_db=tmp_path / "data.db",
    )

    assert out["by_ticker"]["AAPL"]["model_status"] == "core_estimated"
    assert out["by_ticker"]["BABA"]["model_status"] == "projected_only"
    assert out["eligible_ticker_count"] == 2
    assert out["core_estimated_ticker_count"] == 1
    assert out["projected_only_ticker_count"] == 1
    assert out["ineligible_ticker_count"] == 0
    assert out["by_ticker"]["AAPL"]["exposure_origin"] == "native"
    assert out["by_ticker"]["BABA"]["exposures"]["market"] == 1.0
    assert out["by_ticker"]["BABA"]["exposure_origin"] == "native"
    assert out["by_ticker"]["BABA"]["model_status_reason"] == ""
    assert "industry_retailers" not in out["by_ticker"]["BABA"]["exposures"]
    assert "specific risk" in out["by_ticker"]["BABA"]["model_warning"]
    index_by_ticker = {row["ticker"]: row for row in out["index"]}
    assert index_by_ticker["AAPL"]["model_status"] == "core_estimated"
    assert index_by_ticker["BABA"]["model_status"] == "projected_only"


def test_build_universe_ticker_loadings_preserves_factor_ids_in_covariance_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exposures_df = pd.DataFrame(
        [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "as_of_date": "2026-03-03",
                "size_score": 0.1,
                "beta_score": 0.2,
            }
        ]
    )
    fundamentals_df = pd.DataFrame(
        [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "market_cap": 1000.0,
                "trbc_business_sector": "Technology Equipment",
                "trbc_industry_group": "Semiconductors & Semiconductor Equipment",
                "trbc_economic_sector_short": "Technology",
                "company_name": "Apple",
            }
        ]
    )
    prices_df = pd.DataFrame(
        [{"ric": "AAPL.OQ", "ticker": "AAPL", "close": 190.0}]
    )
    eligibility_df = pd.DataFrame(
        [
            {
                "ric": "AAPL.OQ",
                "is_structural_eligible": True,
                "exclusion_reason": "",
                "market_cap": 1000.0,
                "trbc_business_sector": "Technology Equipment",
                "trbc_industry_group": "Semiconductors & Semiconductor Equipment",
                "trbc_economic_sector_short": "Technology",
                "hq_country_code": "US",
            }
        ]
    ).set_index("ric")
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.build_eligibility_context",
        lambda data_db, dates: object(),
    )
    monkeypatch.setattr(
        "backend.analytics.services.universe_loadings.structural_eligibility_for_date",
        lambda ctx, as_of_date: (None, eligibility_df),
    )

    cov = pd.DataFrame(
        np.eye(3, dtype=float),
        index=["market", "industry_technology_equipment", "style_beta_score"],
        columns=["market", "industry_technology_equipment", "style_beta_score"],
    )
    factor_catalog_by_name = build_factor_catalog_for_factors(
        ["Market", "Technology Equipment", "Beta"],
        method_version="test_v1",
    )

    out = build_universe_ticker_loadings(
        exposures_df=exposures_df,
        fundamentals_df=fundamentals_df,
        prices_df=prices_df,
        cov=cov,
        data_db=tmp_path / "data.db",
        factor_catalog_by_name=factor_catalog_by_name,
    )

    assert out["factor_vols"]["market"] == 1.0
    assert out["factor_vols"]["industry_technology_equipment"] == 1.0
    assert out["factor_vols"]["style_beta_score"] == 1.0
    serialized_ids = {entry["factor_id"] for entry in out["factor_catalog"]}
    assert "industry_market" not in serialized_ids
    assert "industry_style_beta_score" not in serialized_ids


# ---------------------------------------------------------------------------
# _load_admitted_runtime_tickers — Neon fallback
# ---------------------------------------------------------------------------

def test_load_admitted_runtime_tickers_neon_fallback_when_no_local_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When local data_db is absent and Neon reads are enabled, the security registry
    is queried from Neon.  Universe loadings must then exclude tickers that are not in
    that registry (e.g. ETF price-table tickers that were never model-universe members).
    """
    import pytest
    from backend.analytics.services import universe_loadings as ul_mod

    # Simulate Neon returning a registry of two tickers only.
    def _fake_neon_admitted() -> set[str] | None:
        return {"AAPL", "MSFT"}

    monkeypatch.setattr(ul_mod, "_load_admitted_runtime_tickers_neon", _fake_neon_admitted)
    monkeypatch.setattr(
        "backend.data.core_read_backend.use_neon_core_reads", lambda: True
    )

    # data_db does not exist → should trigger Neon fallback.
    result = ul_mod._load_admitted_runtime_tickers(tmp_path / "nonexistent.db")
    assert result == {"AAPL", "MSFT"}


def test_load_admitted_runtime_tickers_returns_none_when_no_local_db_and_neon_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When local data_db is absent and Neon reads are disabled, the function returns
    None (no filter — safe open set, consistent with pre-Neon behaviour).
    """
    import pytest
    from backend.analytics.services import universe_loadings as ul_mod

    monkeypatch.setattr(
        "backend.data.core_read_backend.use_neon_core_reads", lambda: False
    )

    result = ul_mod._load_admitted_runtime_tickers(tmp_path / "nonexistent.db")
    assert result is None


def test_load_admitted_runtime_tickers_neon_fallback_when_registry_absent_from_local_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When local data_db exists but has no security_registry table and Neon reads are
    enabled, the Neon fallback is used instead of returning None.
    """
    import pytest
    from backend.analytics.services import universe_loadings as ul_mod
    import sqlite3

    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE some_other_table (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    def _fake_neon_admitted() -> set[str] | None:
        return {"SPY", "QQQ"}

    monkeypatch.setattr(ul_mod, "_load_admitted_runtime_tickers_neon", _fake_neon_admitted)
    monkeypatch.setattr(
        "backend.data.core_read_backend.use_neon_core_reads", lambda: True
    )

    result = ul_mod._load_admitted_runtime_tickers(db_path)
    assert result == {"SPY", "QQQ"}


def test_build_universe_ticker_loadings_excludes_non_registry_tickers_via_neon(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end: when serve-refresh runs in a fresh container (no local SQLite),
    tickers that are in the Neon prices table but NOT in the security registry must be
    excluded from the universe so _assert_current_membership_coverage does not fire.
    """
    import pytest
    from backend.analytics.services import universe_loadings as ul_mod

    # Registry admits only AAPL and MSFT — ETF tickers AAXJ/ACWI must be excluded.
    def _fake_neon_admitted() -> set[str] | None:
        return {"AAPL", "MSFT"}

    monkeypatch.setattr(ul_mod, "_load_admitted_runtime_tickers_neon", _fake_neon_admitted)
    monkeypatch.setattr(
        "backend.data.core_read_backend.use_neon_core_reads", lambda: True
    )

    prices_df = pd.DataFrame(
        {"ticker": ["AAPL", "MSFT", "AAXJ", "ACWI"], "close": [200.0, 400.0, 50.0, 90.0]}
    )
    out = build_universe_ticker_loadings(
        exposures_df=pd.DataFrame(),
        fundamentals_df=pd.DataFrame(),
        prices_df=prices_df,
        cov=pd.DataFrame(),
        data_db=tmp_path / "nonexistent.db",  # does not exist — triggers Neon path
    )

    assert "AAXJ" not in out["by_ticker"], "ETF ticker not in registry must be excluded"
    assert "ACWI" not in out["by_ticker"], "ETF ticker not in registry must be excluded"
    # AAPL and MSFT are admitted (price data only, so ineligible but present)
    assert "AAPL" in out["by_ticker"]
    assert "MSFT" in out["by_ticker"]


def test_load_admitted_runtime_tickers_fails_closed_when_neon_authority_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.analytics.services import universe_loadings as ul_mod

    monkeypatch.setattr(
        "backend.data.core_read_backend.use_neon_core_reads", lambda: True
    )
    monkeypatch.setattr(
        ul_mod,
        "_load_admitted_runtime_tickers_neon",
        lambda: (_ for _ in ()).throw(RuntimeError("neon unavailable")),
    )

    with pytest.raises(RuntimeError, match="neon unavailable"):
        ul_mod._load_admitted_runtime_tickers(tmp_path / "nonexistent.db")


def test_build_universe_ticker_loadings_maps_projected_factor_names_through_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.analytics.services import universe_loadings as ul_mod

    monkeypatch.setattr(ul_mod, "_load_admitted_runtime_tickers", lambda _data_db: {"SPY"})
    monkeypatch.setattr(ul_mod, "_overlay_persisted_cuse_membership", lambda **kwargs: None)

    factor_catalog = build_factor_catalog_for_factors(
        ["Market", "Academic Educational Services", "Book-to-Price"],
        method_version="test",
    )
    market_id = factor_id_for_name("Market", family="market")
    industry_id = factor_id_for_name("Academic Educational Services", family="industry")
    style_id = factor_id_for_name(
        "Book-to-Price",
        family="style",
        source_column="book_to_price_score",
    )
    cov = pd.DataFrame(
        np.diag([1.0, 0.25, 0.09]),
        index=[market_id, industry_id, style_id],
        columns=[market_id, industry_id, style_id],
    )
    projected = {
        "SPY": ProjectedLoadingResult(
            ric="SPY.P",
            ticker="SPY",
            exposures={
                "Market": 1.0,
                "Academic & Educational Services": 0.2,
                "Book-to-Price": 0.05,
            },
            specific_var=0.01,
            specific_vol=0.1,
            r_squared=0.95,
            obs_count=252,
            lookback_days=252,
            projection_asof="2026-04-13",
            status="ok",
        )
    }

    out = build_universe_ticker_loadings(
        exposures_df=pd.DataFrame(),
        fundamentals_df=pd.DataFrame(),
        prices_df=pd.DataFrame([{"ticker": "SPY", "ric": "SPY.P", "close": 500.0}]),
        cov=cov,
        data_db=tmp_path / "nonexistent.db",
        factor_catalog_by_name=factor_catalog,
        projected_loadings=projected,
        projection_universe_rows=[{"ticker": "SPY", "ric": "SPY.P"}],
        projection_core_state_through_date="2026-04-13",
    )

    spy = out["by_ticker"]["SPY"]
    assert spy["model_status"] == "projected_only"
    assert spy["projection_method"] == "ols_returns_regression"
    assert spy["projection_asof"] == "2026-04-13"
    assert spy["exposures"][market_id] == 1.0
    assert spy["exposures"][industry_id] == 0.2
    assert spy["exposures"][style_id] == 0.05
