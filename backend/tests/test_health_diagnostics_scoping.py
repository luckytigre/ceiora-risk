from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from backend.analytics import health
from backend.data.cross_section_snapshot import ensure_cross_section_snapshot_table
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
        return SimpleNamespace(
            exposure_snapshots={
                str(d): pd.DataFrame(
                    {
                        "ticker": ["AAA"],
                        "trbc_business_sector": ["Tech"],
                        "snapshot_date": [str(d)],
                    },
                    index=pd.Index(["AAA.N"], name="ric"),
                )
                for d in dates
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
        if snapshot_df["snapshot_date"].iloc[0] == "2026-01-03":
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


def test_load_core_runtime_identity_prefers_latest_snapshot_surface(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_cross_section_snapshot_table(conn)
        conn.executemany(
            """
            INSERT INTO universe_cross_section_snapshot (ric, ticker, as_of_date, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            [
                ("OLD.OQ", "OLD", "2026-03-14", "2026-03-14T00:00:00Z"),
                ("KEEP.OQ", "KEEP", "2026-03-21", "2026-03-21T00:00:00Z"),
            ],
        )
        conn.commit()

        out = health._load_core_runtime_identity(conn)
    finally:
        conn.close()

    assert out.to_dict("records") == [{"ric": "KEEP.OQ", "ticker": "KEEP"}]
