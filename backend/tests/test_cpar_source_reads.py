from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from backend.data import cpar_source_reads
from backend.data import core_read_backend as core_backend


def _seed_source_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE security_registry (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            tracking_status TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_policy_current (
            ric TEXT PRIMARY KEY,
            allow_cpar_core_target INTEGER,
            allow_cpar_extended_target INTEGER,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_master_compat_current (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            classification_ok INTEGER,
            is_equity_eligible INTEGER,
            coverage_role TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
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
        INSERT INTO security_registry (
            ric, ticker, isin, exchange_name, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SPY.P", "SPY", "USSPY", "NYSE Arca", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("XLF.P", "XLF", "USXLF", "NYSE Arca", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("BLANK.X", "", "USBLNK", "OTC", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_policy_current (
            ric, allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES (?, ?, ?, ?)
        """,
        [
            ("SPY.P", 0, 1, "2026-03-18T00:00:00Z"),
            ("XLF.P", 0, 1, "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", 1, 1, "2026-03-18T00:00:00Z"),
            ("BLANK.X", 0, 0, "2026-03-18T00:00:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SPY.P", "SPY", "USSPY", "NYSE Arca", 1, 1, "projection_only", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("XLF.P", "XLF", "USXLF", "NYSE Arca", 1, 1, "projection_only", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", 1, 1, "native_equity", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("BLANK.X", "", "USBLNK", "OTC", 0, 0, "projection_only", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ],
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


def test_cpar_shared_reads_do_not_fall_back_to_legacy_when_registry_policy_rows_are_missing(source_db: Path) -> None:
    conn = sqlite3.connect(str(source_db))
    conn.execute("DELETE FROM security_registry")
    conn.execute("DELETE FROM security_policy_current")
    conn.execute("DELETE FROM security_master_compat_current")
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, isin, exchange_name, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SPY.P", "SPY", "USSPY", "NYSE Arca", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("XLF.P", "XLF", "USXLF", "NYSE Arca", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.commit()
    conn.close()

    rows = cpar_source_reads.load_build_universe_rows(data_db=source_db)

    assert rows == []


def test_cpar_registry_reads_ignore_historical_only_rows_for_current_package_selection(source_db: Path) -> None:
    conn = sqlite3.connect(str(source_db))
    conn.execute("DELETE FROM security_registry")
    conn.execute("DELETE FROM security_policy_current")
    conn.execute("DELETE FROM security_master_compat_current")
    registry_rows = [
        ("SPY.P", "SPY", "USSPY", "NYSE Arca", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ("SPY_OLD.P", "SPY", "USSPYOLD", "NYSE Arca", "historical_only", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ("XLF.P", "XLF", "USXLF", "NYSE Arca", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
    ]
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, isin, exchange_name, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        registry_rows,
    )
    conn.executemany(
        """
        INSERT INTO security_policy_current (
            ric, allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES (?, ?, ?, ?)
        """,
        [
            ("SPY.P", 0, 1, "2026-03-18T00:00:00Z"),
            ("SPY_OLD.P", 0, 1, "2026-03-18T00:00:00Z"),
            ("XLF.P", 0, 1, "2026-03-18T00:00:00Z"),
            ("AAPL.OQ", 1, 1, "2026-03-18T00:00:00Z"),
        ],
    )
    compat_rows = [
        ("SPY.P", "SPY", "USSPY", "NYSE Arca", 1, 1, "projection_only", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ("SPY_OLD.P", "SPY", "USSPYOLD", "NYSE Arca", 1, 1, "projection_only", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ("XLF.P", "XLF", "USXLF", "NYSE Arca", 1, 1, "projection_only", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", 1, 1, "native_equity", "seed", "job_1", "2026-03-18T00:00:00Z"),
    ]
    conn.executemany(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        compat_rows,
    )
    conn.commit()
    conn.close()

    proxy_rows = cpar_source_reads.resolve_factor_proxy_rows(["SPY"], data_db=source_db)
    build_rows = cpar_source_reads.load_build_universe_rows(data_db=source_db)

    assert proxy_rows == [
        {
            "ric": "SPY.P",
            "ticker": "SPY",
            "isin": "USSPY",
            "exchange_name": "NYSE Arca",
            "classification_ok": 1,
            "is_equity_eligible": 1,
            "source": "seed",
            "job_run_id": "job_1",
            "updated_at": "2026-03-18T00:00:00Z",
        }
    ]
    assert [row["ric"] for row in build_rows] == ["AAPL.OQ", "SPY.P", "XLF.P"]


def test_resolve_factor_proxy_rows_prefers_core_target_primary_listing_over_consolidated_alias(
    source_db: Path,
) -> None:
    conn = sqlite3.connect(str(source_db))
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, isin, exchange_name, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("SPY.K", "SPY", "USSPY", "Cboe Consolidated", "active", "seed", "job_2", "2026-03-19T00:00:00Z"),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES (?, ?, ?, ?)
        """,
        ("SPY.K", 0, 1, "2026-03-19T00:00:00Z"),
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("SPY.K", "SPY", "USSPY", "Cboe Consolidated", 0, 0, "projection_only", "seed", "job_2", "2026-03-19T00:00:00Z"),
    )
    conn.commit()
    conn.close()

    rows = cpar_source_reads.resolve_factor_proxy_rows(["SPY"], data_db=source_db)

    assert rows == [
        {
            "ric": "SPY.P",
            "ticker": "SPY",
            "isin": "USSPY",
            "exchange_name": "NYSE Arca",
            "classification_ok": 1,
            "is_equity_eligible": 1,
            "source": "seed",
            "job_run_id": "job_1",
            "updated_at": "2026-03-18T00:00:00Z",
        }
    ]


def test_resolve_factor_proxy_rows_escapes_literal_percent_for_neon_fetch(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(core_backend, "use_neon_core_reads", lambda: True)

    def _fake_fetch_rows(sql: str, params, *, data_db, neon_enabled):
        captured["sql"] = sql
        captured["params"] = params
        captured["neon_enabled"] = neon_enabled
        return []

    monkeypatch.setattr(core_backend, "fetch_rows", _fake_fetch_rows)
    monkeypatch.setattr(
        cpar_source_reads,
        "_pg_tables_exist",
        lambda *, data_db=None, tables=(): True,
    )

    rows = cpar_source_reads.resolve_factor_proxy_rows(["SPY"])

    assert rows == []
    assert captured["neon_enabled"] is True
    assert "LIKE '%%CONSOLIDATED%%'" in str(captured["sql"])


def test_cpar_registry_build_universe_rows_require_policy_flags_not_compat_role_fallback(source_db: Path) -> None:
    conn = sqlite3.connect(str(source_db))
    conn.execute("DELETE FROM security_registry")
    conn.execute("DELETE FROM security_policy_current")
    conn.execute("DELETE FROM security_master_compat_current")
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, isin, exchange_name, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("SPY.P", "SPY", "USSPY", "NYSE Arca", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_policy_current (
            ric, allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES (?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", 1, 1, "2026-03-18T00:00:00Z"),
            ("SPY.P", None, 0, "2026-03-18T00:00:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", 1, 1, "native_equity", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("SPY.P", "SPY", "USSPY", "NYSE Arca", 1, 1, "native_equity", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.commit()
    conn.close()

    rows = cpar_source_reads.load_build_universe_rows(data_db=source_db)

    assert [row["ric"] for row in rows] == ["AAPL.OQ"]


def test_cpar_shared_reads_remain_registry_first_when_active_registry_policy_coverage_is_partial(source_db: Path) -> None:
    conn = sqlite3.connect(str(source_db))
    conn.execute("DELETE FROM security_registry")
    conn.execute("DELETE FROM security_policy_current")
    conn.execute("DELETE FROM security_master_compat_current")
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, isin, exchange_name, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("SPY.P", "SPY", "USSPY", "NYSE Arca", "active", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES (?, ?, ?, ?)
        """,
        ("AAPL.OQ", 1, 1, "2026-03-18T00:00:00Z"),
    )
    conn.executemany(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", 1, 1, "native_equity", "seed", "job_1", "2026-03-18T00:00:00Z"),
            ("SPY.P", "SPY", "USSPY", "NYSE Arca", 1, 1, "projection_only", "seed", "job_1", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.commit()
    conn.close()

    rows = cpar_source_reads.load_build_universe_rows(data_db=source_db)

    assert [row["ric"] for row in rows] == ["AAPL.OQ"]


def test_cpar_factor_proxy_rows_do_not_fall_back_to_legacy_when_compat_surface_is_missing(source_db: Path) -> None:
    conn = sqlite3.connect(str(source_db))
    conn.execute("DELETE FROM security_registry")
    conn.execute("DELETE FROM security_policy_current")
    conn.execute("DELETE FROM security_master_compat_current")
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, isin, exchange_name, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SPY.P", "SPY", "USSPY", "NYSE Arca", "active", "registry", "job_1", "2026-03-18T00:00:00Z"),
            ("XLF.P", "XLF", "USXLF", "NYSE Arca", "active", "registry", "job_1", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.commit()
    conn.close()

    rows = cpar_source_reads.resolve_factor_proxy_rows(["SPY"], data_db=source_db)

    assert rows == [
        {
            "ric": "SPY.P",
            "ticker": "SPY",
            "isin": "USSPY",
            "exchange_name": "NYSE Arca",
            "classification_ok": 0,
            "is_equity_eligible": 0,
            "source": "registry",
            "job_run_id": "job_1",
            "updated_at": "2026-03-18T00:00:00Z",
        }
    ]


def test_cpar_build_universe_rows_do_not_require_compat_surface_when_policy_flags_exist(source_db: Path) -> None:
    conn = sqlite3.connect(str(source_db))
    conn.execute("DELETE FROM security_registry")
    conn.execute("DELETE FROM security_policy_current")
    conn.execute("DELETE FROM security_master_compat_current")
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, isin, exchange_name, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", "USAAPL", "NASDAQ", "active", "registry", "job_1", "2026-03-18T00:00:00Z"),
            ("SPY.P", "SPY", "USSPY", "NYSE Arca", "active", "registry", "job_1", "2026-03-18T00:00:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_policy_current (
            ric, allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES (?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", 1, 1, "2026-03-18T00:00:00Z"),
            ("SPY.P", 0, 1, "2026-03-18T00:00:00Z"),
        ],
    )
    conn.commit()
    conn.close()

    rows = cpar_source_reads.load_build_universe_rows(data_db=source_db)

    assert rows == [
        {
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "isin": "USAAPL",
            "exchange_name": "NASDAQ",
            "allow_cpar_core_target": 1,
            "allow_cpar_extended_target": 1,
            "is_single_name_equity": 0,
            "classification_ok": 0,
            "is_equity_eligible": 0,
            "source": "registry",
            "job_run_id": "job_1",
            "updated_at": "2026-03-18T00:00:00Z",
        },
        {
            "ric": "SPY.P",
            "ticker": "SPY",
            "isin": "USSPY",
            "exchange_name": "NYSE Arca",
            "allow_cpar_core_target": 0,
            "allow_cpar_extended_target": 1,
            "is_single_name_equity": 0,
            "classification_ok": 0,
            "is_equity_eligible": 0,
            "source": "registry",
            "job_run_id": "job_1",
            "updated_at": "2026-03-18T00:00:00Z",
        },
    ]


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


def test_load_latest_price_rows_returns_latest_row_not_after_cutoff(source_db: Path) -> None:
    rows = cpar_source_reads.load_latest_price_rows(
        ["aapl.oq", "spy.p"],
        as_of_date="2026-03-12",
        data_db=source_db,
    )

    assert rows == [
        {
            "ric": "AAPL.OQ",
            "date": "2026-03-06",
            "open": 200.0,
            "high": 201.0,
            "low": 198.0,
            "close": 200.0,
            "adj_close": 199.5,
            "volume": 2000.0,
            "currency": "USD",
            "source": "seed",
            "updated_at": "2026-03-18T00:00:00Z",
        },
        {
            "ric": "SPY.P",
            "date": "2026-03-06",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "adj_close": 100.5,
            "volume": 1000.0,
            "currency": "USD",
            "source": "seed",
            "updated_at": "2026-03-18T00:00:00Z",
        },
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


def test_cpar_source_reads_raise_typed_error_for_infrastructure_failures(
    source_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_source_reads.core_backend,
        "fetch_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("sqlite unavailable")),
    )

    with pytest.raises(cpar_source_reads.CparSourceReadError, match="sqlite unavailable"):
        cpar_source_reads.load_latest_price_rows(["AAPL.OQ"], as_of_date="2026-03-13", data_db=source_db)
