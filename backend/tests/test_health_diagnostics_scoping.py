from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from backend.analytics import health
from backend.risk_model.factor_catalog import STYLE_COLUMN_TO_LABEL


def test_load_style_exposure_snapshots_honors_required_dates(tmp_path) -> None:
    db_path = tmp_path / "data.db"
    style_col = next(iter(STYLE_COLUMN_TO_LABEL))
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            f"""
            CREATE TABLE barra_raw_cross_section_history (
                ric TEXT,
                ticker TEXT,
                as_of_date TEXT,
                {style_col} REAL,
                trbc_business_sector TEXT
            )
            """
        )
        conn.executemany(
            f"""
            INSERT INTO barra_raw_cross_section_history
            (ric, ticker, as_of_date, {style_col}, trbc_business_sector)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("AAA.N", "AAA", "2026-01-01", 1.0, "Tech"),
                ("AAA.N", "AAA", "2026-01-02", 2.0, "Tech"),
                ("AAA.N", "AAA", "2026-01-03", 3.0, "Tech"),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    exposure_dates, snapshots = health._load_style_exposure_snapshots(
        db_path,
        required_dates={"2026-01-02"},
    )

    assert exposure_dates == ["2026-01-01", "2026-01-02", "2026-01-03"]
    assert list(snapshots.keys()) == ["2026-01-02"]


def test_compute_exposure_turnover_only_builds_sampled_dates(monkeypatch) -> None:
    built_dates: list[str] = []

    monkeypatch.setattr(
        health,
        "_load_exposure_dates",
        lambda _path: ["2026-01-01", "2026-01-02", "2026-01-03"],
    )

    def _fake_build_eligibility_context(_path, *, dates=None):
        assert dates is not None
        built_dates.extend(str(d) for d in dates)
        date_key = str(dates[0])
        return SimpleNamespace(
            exposure_snapshots={
                date_key: pd.DataFrame(
                    {
                        "ticker": ["AAA"],
                        "trbc_business_sector": ["Tech"],
                    },
                    index=pd.Index(["AAA.N"], name="ric"),
                )
            }
        )

    monkeypatch.setattr(health, "build_eligibility_context", _fake_build_eligibility_context)
    monkeypatch.setattr(
        health,
        "structural_eligibility_for_date",
        lambda _ctx, d: (
            str(d),
            pd.DataFrame(
                {
                    "is_structural_eligible": [True],
                    "hq_country_code": ["US"],
                    "market_cap": [100.0],
                    "trbc_business_sector": ["Tech"],
                },
                index=pd.Index(["AAA.N"], name="ric"),
            ),
        ),
    )

    def _fake_build_factor_exposure_matrix(snapshot_df, *, eligibility, core_country_codes=None):
        value = 1.0 if snapshot_df["ric"].iloc[0] == "AAA.N" else 0.0
        if "2026-01-03" in built_dates[-1]:
            value = 2.0
        return pd.DataFrame({"Factor": [value]}, index=pd.Index(["AAA.N"], name="ric"))

    monkeypatch.setattr(health, "_build_factor_exposure_matrix", _fake_build_factor_exposure_matrix)

    rows = health._compute_exposure_turnover(
        Path("/tmp/unused.db"),
        ["Factor"],
        sample_dates=["2026-01-01", "2026-01-03"],
        core_country_codes={"US"},
    )

    assert built_dates == ["2026-01-01", "2026-01-03"]
    assert [row["date"] for row in rows] == ["2026-01-03"]
