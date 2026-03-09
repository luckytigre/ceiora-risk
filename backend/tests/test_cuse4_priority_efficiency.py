from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from backend.risk_model import daily_factor_returns as dfr


def test_ensure_cache_version_invalidates_on_cross_section_age_change(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute(
        "CREATE TABLE daily_factor_returns (date TEXT, factor_name TEXT, factor_return REAL, r_squared REAL, residual_vol REAL, cross_section_n INTEGER, eligible_n INTEGER, coverage REAL)"
    )
    conn.execute(
        "CREATE TABLE daily_specific_residuals (date TEXT, ric TEXT, ticker TEXT, residual REAL, market_cap REAL, trbc_industry_group TEXT)"
    )
    conn.execute(
        "CREATE TABLE daily_universe_eligibility_summary (date TEXT, exp_date TEXT, exposure_n INTEGER, structural_eligible_n INTEGER, regression_member_n INTEGER, structural_coverage REAL, regression_coverage REAL, drop_pct_from_prev REAL, alert_level TEXT, missing_style_n INTEGER, missing_market_cap_n INTEGER, missing_trbc_economic_sector_short_n INTEGER, missing_trbc_industry_n INTEGER, non_equity_n INTEGER, missing_return_n INTEGER)"
    )
    conn.execute("CREATE TABLE daily_factor_returns_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO daily_factor_returns VALUES ('2026-03-03', 'Beta', 0.01, 0.3, 0.2, 50, 50, 1.0)")
    conn.execute("INSERT INTO daily_specific_residuals VALUES ('2026-03-03', 'AAPL.OQ', 'AAPL', 0.1, 100.0, 'Tech')")
    conn.execute("INSERT INTO daily_universe_eligibility_summary VALUES ('2026-03-03', '2026-03-03', 50, 50, 50, 1.0, 1.0, 0.0, '', 0, 0, 0, 0, 0, 0)")
    conn.execute("INSERT INTO daily_factor_returns_meta VALUES ('method_version', ?)", (dfr.CACHE_METHOD_VERSION,))
    conn.execute("INSERT INTO daily_factor_returns_meta VALUES ('cross_section_min_age_days', '5')")
    conn.commit()
    conn.close()

    dfr._ensure_cache_version(cache_db, min_cross_section_age_days=7)

    conn = sqlite3.connect(str(cache_db))
    assert conn.execute("SELECT COUNT(*) FROM daily_factor_returns").fetchone()[0] == 0
    meta = dict(conn.execute("SELECT key, value FROM daily_factor_returns_meta").fetchall())
    assert meta["method_version"] == dfr.CACHE_METHOD_VERSION
    assert meta["cross_section_min_age_days"] == "7"
    conn.close()


def test_load_cached_dates_requires_eligibility_rows(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute(dfr._DAILY_FR_SCHEMA)
    conn.execute(dfr._DAILY_FR_META_SCHEMA)
    conn.execute(dfr._DAILY_RESIDUALS_SCHEMA)
    conn.execute(dfr._DAILY_ELIGIBILITY_SUMMARY_SCHEMA)
    conn.execute(
        "INSERT INTO daily_factor_returns VALUES ('2026-03-03', 'Beta', 0.01, 0.3, 0.2, 50, 50, 1.0)"
    )
    conn.execute(
        "INSERT INTO daily_specific_residuals VALUES ('2026-03-03', 'AAPL.OQ', 'AAPL', 0.1, 100.0, 'Tech')"
    )
    conn.commit()
    conn.close()

    assert dfr._load_cached_dates(cache_db) == set()


def test_compute_daily_factor_returns_bounds_price_window_and_eligibility_dates(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        dfr,
        "_ensure_cache_version",
        lambda cache_db, *, min_cross_section_age_days: captured.setdefault(
            "cache_signature_days",
            min_cross_section_age_days,
        ),
    )
    monkeypatch.setattr(
        dfr,
        "_load_trading_dates",
        lambda _data_db: ["2026-03-03", "2026-03-04", "2026-03-05"],
    )
    monkeypatch.setattr(dfr, "_load_cached_dates", lambda _cache_db: {"2026-03-03"})

    def _load_prices_for_window(_data_db, *, start_date=None, end_date=None):
        captured["price_window"] = (start_date, end_date)
        return pd.DataFrame(
            [
                {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-03", "close": 100.0},
                {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-04", "close": 101.0},
                {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-05", "close": 102.0},
                {"ric": "MSFT.OQ", "ticker": "MSFT", "date": "2026-03-03", "close": 200.0},
                {"ric": "MSFT.OQ", "ticker": "MSFT", "date": "2026-03-04", "close": 202.0},
                {"ric": "MSFT.OQ", "ticker": "MSFT", "date": "2026-03-05", "close": 204.0},
            ]
        )

    monkeypatch.setattr(dfr, "_load_prices_for_window", _load_prices_for_window)

    exposure_snapshot = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "size_score": [0.1, -0.1],
            "trbc_industry_group": ["Tech", "Tech"],
        },
        index=pd.Index(["AAPL.OQ", "MSFT.OQ"], name="ric"),
    )
    eligibility_frame = pd.DataFrame(
        {
            "is_structural_eligible": [True, True],
            "exclusion_reason": ["", ""],
            "market_cap": [1000.0, 1200.0],
            "trbc_business_sector": ["Technology", "Technology"],
            "hq_country_code": ["US", "US"],
        },
        index=pd.Index(["AAPL.OQ", "MSFT.OQ"], name="ric"),
    )

    def _build_eligibility_context(_data_db, *, dates=None):
        captured["eligibility_dates"] = list(dates or [])
        return SimpleNamespace(
            exposure_dates=list(dates or []),
            exposure_snapshots={str(date): exposure_snapshot for date in (dates or [])},
        )

    monkeypatch.setattr(dfr, "build_eligibility_context", _build_eligibility_context)
    monkeypatch.setattr(
        dfr,
        "structural_eligibility_for_date",
        lambda ctx, date_key: (str(date_key), eligibility_frame),
    )
    monkeypatch.setattr(
        dfr,
        "canonicalize_style_scores",
        lambda **kwargs: kwargs["style_scores"],
    )
    monkeypatch.setattr(
        dfr,
        "estimate_factor_returns_two_phase",
        lambda **kwargs: SimpleNamespace(
            factor_returns={"Size": 0.01},
            r_squared=0.5,
            residual_vol=0.2,
            residuals=[0.01, -0.01],
        ),
    )
    monkeypatch.setattr(dfr, "MIN_CROSS_SECTION_SIZE", 1)
    monkeypatch.setattr(dfr, "MIN_ELIGIBLE_COVERAGE", 0.0)

    out = dfr.compute_daily_factor_returns(
        data_db=data_db,
        cache_db=cache_db,
        min_cross_section_age_days=0,
    )

    assert captured["cache_signature_days"] == 0
    assert captured["price_window"] == ("2026-03-03", "2026-03-05")
    assert captured["eligibility_dates"] == ["2026-03-04", "2026-03-05"]
    assert sorted(out["date"].unique().tolist()) == ["2026-03-04", "2026-03-05"]
