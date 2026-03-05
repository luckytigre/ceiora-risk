from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from backend.analytics.services.universe_loadings import (
    build_universe_ticker_loadings,
    load_latest_factor_coverage,
)


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
            ("2026-03-02", "Value", 101, 94, 0.93),
        ],
    )
    conn.commit()
    conn.close()

    latest, cov = load_latest_factor_coverage(cache_db)
    assert latest == "2026-03-02"
    assert cov["Beta"] == {"cross_section_n": 101, "eligible_n": 96, "coverage_pct": 0.96}
    assert cov["Value"] == {"cross_section_n": 101, "eligible_n": 94, "coverage_pct": 0.93}


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
