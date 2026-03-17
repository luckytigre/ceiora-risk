from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from backend.risk_model import daily_factor_returns as dfr


def _seed_prices(data_db: Path) -> None:
    conn = sqlite3.connect(str(data_db))
    conn.execute("CREATE TABLE security_master (ric TEXT PRIMARY KEY, ticker TEXT NOT NULL)")
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL NOT NULL,
            source TEXT,
            updated_at TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO security_master (ric, ticker) VALUES (?, ?)",
        [
            ("AAPL.OQ", "AAPL"),
            ("MSFT.OQ", "MSFT"),
            ("BABA.N", "BABA"),
        ],
    )
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date, close, source, updated_at) VALUES (?, ?, ?, ?, ?)",
        [
            ("AAPL.OQ", "2026-03-03", 100.0, "lseg_toolkit", "2026-03-03T21:00:00Z"),
            ("AAPL.OQ", "2026-03-04", 101.0, "lseg_toolkit", "2026-03-04T21:00:00Z"),
            ("AAPL.OQ", "2026-03-05", 102.0, "lseg_toolkit", "2026-03-05T21:00:00Z"),
            ("MSFT.OQ", "2026-03-03", 200.0, "lseg_toolkit", "2026-03-03T21:00:00Z"),
            ("MSFT.OQ", "2026-03-04", 202.0, "lseg_toolkit", "2026-03-04T21:00:00Z"),
            ("MSFT.OQ", "2026-03-05", 204.0, "lseg_toolkit", "2026-03-05T21:00:00Z"),
            ("BABA.N", "2026-03-03", 80.0, "lseg_toolkit", "2026-03-03T21:00:00Z"),
            ("BABA.N", "2026-03-04", 81.0, "lseg_toolkit", "2026-03-04T21:00:00Z"),
            ("BABA.N", "2026-03-05", 80.5, "lseg_toolkit", "2026-03-05T21:00:00Z"),
        ],
    )
    conn.commit()
    conn.close()


def test_compute_daily_factor_returns_keeps_projected_non_us_residuals(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    _seed_prices(data_db)

    exposure_snapshot = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "BABA"],
            "size_score": [0.1, -0.1, 0.3],
            "trbc_business_sector": ["Technology Equipment", "Software", "Retailers"],
        },
        index=pd.Index(["AAPL.OQ", "MSFT.OQ", "BABA.N"], name="ric"),
    )
    eligibility_frame = pd.DataFrame(
        {
            "is_structural_eligible": [True, True, True],
            "exclusion_reason": ["", "", ""],
            "market_cap": [1000.0, 1200.0, 900.0],
            "trbc_business_sector": ["Technology Equipment", "Software", "Retailers"],
            "hq_country_code": ["US", "US", "CN"],
        },
        index=pd.Index(["AAPL.OQ", "MSFT.OQ", "BABA.N"], name="ric"),
    )

    def _build_eligibility_context(_data_db, *, dates=None):
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
        "fit_and_apply_style_canonicalization",
        lambda **kwargs: (kwargs["style_scores"], object()),
    )
    monkeypatch.setattr(
        dfr,
        "apply_style_canonicalization",
        lambda **kwargs: kwargs["style_scores"],
    )
    monkeypatch.setattr(
        dfr,
        "estimate_factor_returns_one_stage",
        lambda **kwargs: SimpleNamespace(
            factor_returns={"Size": 0.01},
            robust_se={"Size": 0.0},
            t_stats={"Size": 0.0},
            r_squared=0.5,
            residual_vol=0.2,
            constraint_residual=0.0,
            residuals=[0.01, -0.01],
            raw_residuals=[0.01, -0.01],
        ),
    )
    monkeypatch.setattr(dfr, "MIN_CROSS_SECTION_SIZE", 1)
    monkeypatch.setattr(dfr, "MIN_ELIGIBLE_COVERAGE", 0.0)

    dfr.compute_daily_factor_returns(
        data_db=data_db,
        cache_db=cache_db,
        min_cross_section_age_days=0,
    )

    conn = sqlite3.connect(str(cache_db))
    rows = conn.execute(
        """
        SELECT date, ticker
        FROM daily_specific_residuals
        ORDER BY date, ticker
        """
    ).fetchall()
    conn.close()

    assert rows == [
        ("2026-03-04", "AAPL"),
        ("2026-03-04", "BABA"),
        ("2026-03-04", "MSFT"),
        ("2026-03-05", "AAPL"),
        ("2026-03-05", "BABA"),
        ("2026-03-05", "MSFT"),
    ]
