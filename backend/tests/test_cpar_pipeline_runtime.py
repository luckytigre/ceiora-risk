from __future__ import annotations

import argparse
import builtins
import sqlite3
from pathlib import Path

import numpy as np
import pytest

from backend.cpar.factor_registry import build_cpar1_factor_registry
from backend.cpar.weekly_anchors import generate_weekly_price_anchors
from backend.data import cpar_outputs
from backend.orchestration import run_cpar_pipeline


def _create_source_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE security_master (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            classification_ok INTEGER,
            is_equity_eligible INTEGER,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume REAL,
            currency TEXT,
            source TEXT,
            updated_at TEXT,
            PRIMARY KEY (ric, date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_classification_pit (
            ric TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            trbc_economic_sector TEXT,
            trbc_business_sector TEXT,
            trbc_industry_group TEXT,
            trbc_industry TEXT,
            trbc_activity TEXT,
            hq_country_code TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT,
            PRIMARY KEY (ric, as_of_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_fundamentals_pit (
            ric TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            stat_date TEXT NOT NULL,
            common_name TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT,
            PRIMARY KEY (ric, as_of_date, stat_date)
        )
        """
    )


def _prices_from_returns(base_price: float, returns: np.ndarray) -> list[float]:
    prices = [float(base_price)]
    for weekly_return in returns:
        prices.append(prices[-1] * (1.0 + float(weekly_return)))
    return prices


def _seed_source_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    _create_source_tables(conn)

    factor_specs = build_cpar1_factor_registry()
    factor_rics = {spec.factor_id: f"{spec.ticker}.P" for spec in factor_specs}
    universe = [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "common_name": "Apple Inc.", "hq_country_code": "US"},
        {"ric": "SAPG.DE", "ticker": "SAPG", "common_name": "SAP SE", "hq_country_code": "DE"},
    ]

    security_master_rows = [
        (
            ric,
            spec.ticker,
            f"ISIN{spec.factor_id}",
            "NYSE Arca",
            1,
            1,
            "seed",
            "job_seed",
            "2026-03-18T00:00:00Z",
        )
        for spec, ric in ((spec, factor_rics[spec.factor_id]) for spec in factor_specs)
    ]
    security_master_rows.extend(
        [
            (
                row["ric"],
                row["ticker"],
                f"ISIN{row['ticker']}",
                "Primary",
                1,
                1,
                "seed",
                "job_seed",
                "2026-03-18T00:00:00Z",
            )
            for row in universe
        ]
    )
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        security_master_rows,
    )

    package_dates = ("2026-03-06", "2026-03-13")
    anchor_dates = sorted({*generate_weekly_price_anchors(package_dates[0]), *generate_weekly_price_anchors(package_dates[1])})
    factor_ids = [spec.factor_id for spec in factor_specs]
    factor_returns_by_id: dict[str, np.ndarray] = {}
    for idx, factor_id in enumerate(factor_ids):
        steps = np.arange(len(anchor_dates) - 1, dtype=float)
        factor_returns_by_id[factor_id] = (
            0.0015
            + (idx * 0.00015)
            + 0.00045 * np.sin((steps + 1.0) * (idx + 2.0) / 7.0)
        )

    price_rows: list[tuple[object, ...]] = []
    for idx, factor_id in enumerate(factor_ids):
        prices = _prices_from_returns(100.0 + idx * 8.0, factor_returns_by_id[factor_id])
        ric = factor_rics[factor_id]
        for anchor_date, price in zip(anchor_dates, prices, strict=True):
            price_rows.append(
                (
                    ric,
                    anchor_date,
                    price,
                    price * 1.002,
                    price * 0.998,
                    price,
                    price,
                    1000000 + idx,
                    "USD",
                    "seed",
                    "2026-03-18T00:00:00Z",
                )
            )

    aapl_returns = (
        0.0008
        + 1.15 * factor_returns_by_id["SPY"]
        + 0.35 * factor_returns_by_id["XLK"]
        - 0.18 * factor_returns_by_id["XLF"]
        + 0.12 * factor_returns_by_id["QUAL"]
    )
    sapg_returns = (
        0.0006
        + 0.95 * factor_returns_by_id["SPY"]
        + 0.22 * factor_returns_by_id["XLK"]
        - 0.10 * factor_returns_by_id["XLF"]
        + 0.08 * factor_returns_by_id["USMV"]
    )
    for row, instrument_returns, base_price in (
        (universe[0], aapl_returns, 180.0),
        (universe[1], sapg_returns, 140.0),
    ):
        prices = _prices_from_returns(base_price, instrument_returns)
        for anchor_date, price in zip(anchor_dates, prices, strict=True):
            price_rows.append(
                (
                    row["ric"],
                    anchor_date,
                    price,
                    price * 1.002,
                    price * 0.998,
                    price,
                    price,
                    500000,
                    "USD",
                    "seed",
                    "2026-03-18T00:00:00Z",
                )
            )

    conn.executemany(
        """
        INSERT INTO security_prices_eod (
            ric, date, open, high, low, close, adj_close, volume, currency, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        price_rows,
    )

    classification_rows = [
        (
            row["ric"],
            "2026-03-13",
            "Tech" if row["ticker"] == "AAPL" else "Enterprise Software",
            "Tech" if row["ticker"] == "AAPL" else "Software",
            "Group",
            "Industry",
            "Activity",
            row["hq_country_code"],
            "seed",
            "job_seed",
            "2026-03-18T00:00:00Z",
        )
        for row in universe
    ]
    conn.executemany(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group,
            trbc_industry, trbc_activity, hq_country_code, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        classification_rows,
    )

    fundamentals_rows = [
        (
            row["ric"],
            "2026-03-13",
            "2025-12-31",
            row["common_name"],
            "seed",
            "job_seed",
            "2026-03-18T00:00:00Z",
        )
        for row in universe
    ]
    conn.executemany(
        """
        INSERT INTO security_fundamentals_pit (
            ric, as_of_date, stat_date, common_name, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        fundamentals_rows,
    )

    conn.commit()
    conn.close()


def _delete_price_row(path: Path, *, ric: str, date: str) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "DELETE FROM security_prices_eod WHERE ric = ? AND date = ?",
            (ric, date),
        )
        conn.commit()
    finally:
        conn.close()


def test_cloud_serve_blocks_cpar_build(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    _seed_source_db(data_db)
    monkeypatch.setattr(run_cpar_pipeline.config, "APP_RUNTIME_ROLE", "cloud-serve")

    out = run_cpar_pipeline.run_cpar_pipeline(
        profile="cpar-weekly",
        as_of_date="2026-03-18",
        data_db=data_db,
    )

    assert out["status"] == "failed"
    assert out["reason"] == "runtime_role_disallows_cpar_build"
    assert out["stage_results"] == []
    assert out["run_rows"] == []


def test_cpar_cli_returns_nonzero_when_pipeline_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        run_cpar_pipeline,
        "_parse_args",
        lambda: argparse.Namespace(
            profile="cpar-weekly",
            as_of_date="2026-03-18",
            run_id=None,
            from_stage=None,
            to_stage=None,
            log_level="INFO",
        ),
    )
    monkeypatch.setattr(
        run_cpar_pipeline,
        "run_cpar_pipeline",
        lambda **kwargs: {
            "status": "failed",
            "reason": "runtime_role_disallows_cpar_build",
        },
    )
    monkeypatch.setattr(builtins, "print", lambda *args, **kwargs: None)

    assert run_cpar_pipeline.main() == 1


def test_default_run_id_is_unique_even_with_same_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = run_cpar_pipeline.datetime(2026, 3, 19, 12, 0, 0, 123456, tzinfo=run_cpar_pipeline.timezone.utc)

    class FrozenDatetime:
        @staticmethod
        def now(tz=None):
            return fixed_now

    monkeypatch.setattr(run_cpar_pipeline, "datetime", FrozenDatetime)

    first = run_cpar_pipeline._default_run_id()
    second = run_cpar_pipeline._default_run_id()

    assert first.startswith("cpar_20260319T120000123456Z_")
    assert second.startswith("cpar_20260319T120000123456Z_")
    assert first != second


def test_local_ingest_build_persists_expected_cpar_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    _seed_source_db(data_db)
    monkeypatch.setattr(run_cpar_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_cpar_pipeline.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(run_cpar_pipeline.config, "neon_dsn", lambda: "")

    out = run_cpar_pipeline.run_cpar_pipeline(
        profile="cpar-weekly",
        as_of_date="2026-03-18",
        data_db=data_db,
    )

    assert out["status"] == "ok"
    assert out["package_date"] == "2026-03-13"
    assert out["selected_stages"] == ["source_read", "package_build", "persist_package"]
    assert [row["status"] for row in out["run_rows"]] == ["completed", "completed", "completed"]

    package = cpar_outputs.require_active_package_run(data_db=data_db)
    assert package["package_date"] == "2026-03-13"
    assert package["profile"] == "cpar-weekly"
    assert package["universe_count"] == len(build_cpar1_factor_registry()) + 2
    assert package["fit_ok_count"] == len(build_cpar1_factor_registry()) + 2
    assert package["data_authority"] == "sqlite"

    fit = cpar_outputs.load_active_package_instrument_fit("SAPG", data_db=data_db)
    assert fit is not None
    assert fit["fit_status"] == "ok"
    assert fit["warnings"] == ["ex_us_caution"]

    spy_fit = cpar_outputs.load_active_package_instrument_fit("SPY", data_db=data_db)
    assert spy_fit is not None
    assert spy_fit["fit_status"] == "ok"
    assert spy_fit["thresholded_loadings"]["SPY"] == pytest.approx(1.0)

    xlk_fit = cpar_outputs.load_active_package_instrument_fit("XLK", data_db=data_db)
    assert xlk_fit is not None
    assert xlk_fit["fit_status"] == "ok"
    assert xlk_fit["thresholded_loadings"]["XLK"] > 0.05

    iwm_fit = cpar_outputs.load_active_package_instrument_fit("IWM", data_db=data_db)
    assert iwm_fit is not None
    assert iwm_fit["fit_status"] == "ok"
    assert iwm_fit["thresholded_loadings"]["IWM"] > 0.05

    spy_search_rows = cpar_outputs.search_active_package_instrument_fits("SPY", data_db=data_db)
    assert any(row["ticker"] == "SPY" for row in spy_search_rows)

    covariance_rows = cpar_outputs.load_active_package_covariance_rows(data_db=data_db)
    assert len(covariance_rows) == len(build_cpar1_factor_registry()) ** 2

    conn = sqlite3.connect(str(data_db))
    try:
        runtime_state_tables = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name IN ('job_run_status', 'risk_engine_meta', 'neon_sync_health')
            ORDER BY name
            """
        ).fetchall()
    finally:
        conn.close()
    assert runtime_state_tables == []


def test_explicit_package_date_build_updates_latest_successful_package(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    _seed_source_db(data_db)
    monkeypatch.setattr(run_cpar_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_cpar_pipeline.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(run_cpar_pipeline.config, "neon_dsn", lambda: "")

    first = run_cpar_pipeline.run_cpar_pipeline(
        profile="cpar-package-date",
        as_of_date="2026-03-06",
        data_db=data_db,
        run_id="cpar_run_old",
    )
    second = run_cpar_pipeline.run_cpar_pipeline(
        profile="cpar-package-date",
        as_of_date="2026-03-13",
        data_db=data_db,
        run_id="cpar_run_new",
    )

    assert first["status"] == "ok"
    assert second["status"] == "ok"
    active = cpar_outputs.require_active_package_run(data_db=data_db)
    assert active["package_run_id"] == "cpar_run_new"
    assert active["package_date"] == "2026-03-13"


def test_local_build_fails_early_when_factor_proxy_anchor_week_price_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    _seed_source_db(data_db)
    _delete_price_row(data_db, ric="SPY.P", date="2026-03-13")
    monkeypatch.setattr(run_cpar_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_cpar_pipeline.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(run_cpar_pipeline.config, "neon_dsn", lambda: "")

    out = run_cpar_pipeline.run_cpar_pipeline(
        profile="cpar-weekly",
        as_of_date="2026-03-18",
        data_db=data_db,
    )

    assert out["status"] == "failed"
    assert out["stage_results"][0]["stage"] == "source_read"
    assert out["stage_results"][0]["error"]["message"].startswith(
        "Local source archive is not current through the requested cPAR package date."
    )
