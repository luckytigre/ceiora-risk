from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from backend.data import cpar_source_reads


def _seed_source_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
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
            updated_at TEXT
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
            updated_at TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SPY.P", "SPY", "USSPY", "NYSE Arca", 1, 1, "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("XLF.P", "XLF", "USXLF", "NYSE Arca", 1, 1, "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", 1, 1, "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("BLANK.X", "", "USBLNK", "OTC", 0, 0, "seed", "job_1", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (
            ric, date, open, high, low, close, adj_close, volume, currency, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SPY.P", "2026-03-06", 100.0, 101.0, 99.0, 100.0, 100.5, 1000, "USD", "seed", "2026-03-18T00:00:00Z"),
            ("SPY.P", "2026-03-13", 102.0, 103.0, 101.0, 102.0, 102.4, 1100, "USD", "seed", "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", "2026-03-06", 200.0, 201.0, 198.0, 200.0, 199.5, 2000, "USD", "seed", "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", "2026-03-13", 205.0, 206.0, 204.0, 205.0, 205.1, 2100, "USD", "seed", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group,
            trbc_industry, trbc_activity, hq_country_code, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "2026-03-01", "Tech", "Hardware", "Computers", "Devices", "Phones", "US", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", "2026-03-15", "Tech", "Hardware", "Computers", "Devices", "Phones", "US", "seed", "job_2", "2026-03-18T00:00:00Z"),
            ("SPY.P", "2026-03-10", "Funds", "Funds", "ETF", "ETF", "ETF", "US", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.executemany(
        "INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, common_name, updated_at) VALUES (?, ?, ?, ?, ?)",
        [
            ("AAPL.OQ", "2026-03-01", "2025-12-31", "Apple Inc.", "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", "2026-03-12", "2025-12-31", "Apple Incorporated", "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", "2026-03-12", "2026-01-31", "Apple Incorporated Final", "2026-03-18T00:05:00Z"),
            ("SPY.P", "2026-03-10", "2026-03-10", "SPDR S&P 500 ETF Trust", "2026-03-18T00:00:00Z"),
            ("SPY.P", "2026-03-12", "2026-03-12", "SPDR S&P 500 ETF Trust", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.commit()
    conn.close()


@pytest.fixture()
def source_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "source.db"
    _seed_source_db(path)
    monkeypatch.setattr(cpar_source_reads, "DATA_DB", path)
    monkeypatch.setattr(cpar_source_reads.core_backend, "use_neon_core_reads", lambda: False)
    return path


def test_resolve_factor_proxy_rows_filters_requested_tickers(source_db: Path) -> None:
    rows = cpar_source_reads.resolve_factor_proxy_rows(["spy", "xlf", "missing"], data_db=source_db)

    assert [row["ticker"] for row in rows] == ["SPY", "XLF"]


def test_load_build_universe_rows_excludes_blank_tickers(source_db: Path) -> None:
    rows = cpar_source_reads.load_build_universe_rows(data_db=source_db)

    assert [row["ric"] for row in rows] == ["AAPL.OQ", "SPY.P", "XLF.P"]


def test_load_price_rows_for_rics_respects_date_bounds(source_db: Path) -> None:
    rows = cpar_source_reads.load_price_rows_for_rics(
        ["aapl.oq", "spy.p"],
        date_from="2026-03-07",
        date_to="2026-03-13",
        data_db=source_db,
    )

    assert [(row["ric"], row["date"]) for row in rows] == [
        ("AAPL.OQ", "2026-03-13"),
        ("SPY.P", "2026-03-13"),
    ]


def test_load_latest_classification_rows_uses_latest_row_not_after_cutoff(source_db: Path) -> None:
    rows = cpar_source_reads.load_latest_classification_rows(
        ["aapl.oq", "spy.p"],
        as_of_date="2026-03-12",
        data_db=source_db,
    )

    assert rows == [
        {
            "ric": "AAPL.OQ",
            "as_of_date": "2026-03-01",
            "trbc_economic_sector": "Tech",
            "trbc_business_sector": "Hardware",
            "trbc_industry_group": "Computers",
            "trbc_industry": "Devices",
            "trbc_activity": "Phones",
            "hq_country_code": "US",
            "source": "seed",
            "job_run_id": "job_1",
            "updated_at": "2026-03-18T00:00:00Z",
        },
        {
            "ric": "SPY.P",
            "as_of_date": "2026-03-10",
            "trbc_economic_sector": "Funds",
            "trbc_business_sector": "Funds",
            "trbc_industry_group": "ETF",
            "trbc_industry": "ETF",
            "trbc_activity": "ETF",
            "hq_country_code": "US",
            "source": "seed",
            "job_run_id": "job_1",
            "updated_at": "2026-03-18T00:00:00Z",
        },
    ]


def test_load_latest_common_name_rows_uses_latest_row_not_after_cutoff(source_db: Path) -> None:
    rows = cpar_source_reads.load_latest_common_name_rows(
        ["aapl.oq", "spy.p"],
        as_of_date="2026-03-11",
        data_db=source_db,
    )

    assert rows == [
        {
            "ric": "AAPL.OQ",
            "as_of_date": "2026-03-01",
            "common_name": "Apple Inc.",
        },
        {
            "ric": "SPY.P",
            "as_of_date": "2026-03-10",
            "common_name": "SPDR S&P 500 ETF Trust",
        },
    ]


def test_load_latest_common_name_rows_collapses_same_asof_duplicates(source_db: Path) -> None:
    rows = cpar_source_reads.load_latest_common_name_rows(
        ["aapl.oq"],
        as_of_date="2026-03-12",
        data_db=source_db,
    )

    assert rows == [
        {
            "ric": "AAPL.OQ",
            "as_of_date": "2026-03-12",
            "common_name": "Apple Incorporated Final",
        }
    ]
