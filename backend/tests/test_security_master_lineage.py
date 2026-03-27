from __future__ import annotations

import importlib
import sys
import sqlite3
import types
from pathlib import Path

import pandas as pd
import pytest

from backend.scripts import augment_security_master_from_ric_xlsx
from backend.scripts import download_data_lseg
from backend.scripts.export_security_master_seed import export_seed
from backend.risk_model.raw_cross_section_history import rebuild_raw_cross_section_history
from backend.universe.bootstrap import bootstrap_cuse4_source_tables
from backend.universe.schema import ensure_cuse4_schema
from backend.universe.security_master_sync import load_default_source_universe_rows


def _row_by_ric(db_path: Path, ric: str) -> sqlite3.Row:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT *
            FROM security_master
            WHERE ric = ?
            """,
            (ric,),
        ).fetchone()
        assert row is not None
        return row
    finally:
        conn.close()


def _import_backfill_prices_range_with_fake_lseg(monkeypatch, fake_lseg_data: types.ModuleType):
    fake_lseg = types.ModuleType("lseg")
    fake_lseg.__path__ = []  # Mark the fake top-level module as a package for importlib.
    fake_lseg_data.__path__ = []  # Keep lazy importers from treating the fake child as a plain module.
    fake_lseg.data = fake_lseg_data
    monkeypatch.setitem(sys.modules, "lseg", fake_lseg)
    monkeypatch.setitem(sys.modules, "lseg.data", fake_lseg_data)
    sys.modules.pop("backend.vendor.lseg_toolkit.client.session", None)
    sys.modules.pop("backend.scripts.backfill_prices_range_lseg", None)
    module = importlib.import_module("backend.scripts.backfill_prices_range_lseg")
    monkeypatch.setattr(module, "rd", fake_lseg_data)
    monkeypatch.setattr(module, "open_managed_session", lambda: None)
    monkeypatch.setattr(module, "close_managed_session", lambda _session: None)
    return module


def test_bootstrap_syncs_seed_registry_without_trusting_seed_flags(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "AAPL.OQ",
            "AAPL",
            "US0378331005",
            "NASDAQ",
            1,
            1,
            "lseg_toolkit",
            "job_1",
            "2026-03-15T00:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()

    seed_path = tmp_path / "security_master_seed.csv"
    seed_path.write_text(
        "\n".join(
            [
                "ric,ticker,sid,permid,isin,instrument_type,asset_category_description,exchange_name,classification_ok,is_equity_eligible,source,job_run_id,updated_at",
                "AAPL.OQ,AAPL,123,456,US0378331005,Common Stock,Equity,NASDAQ,0,0,seed,row1,2026-03-01T00:00:00+00:00",
                "BABA.N,BABA,789,012,US01609W1027,Common Stock,Equity,NYSE,1,1,seed,row2,2026-03-01T00:00:00+00:00",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out = bootstrap_cuse4_source_tables(db_path=data_db, seed_path=seed_path)

    assert out["status"] == "ok"
    assert out["mode"] == "bootstrap_only"
    assert out["seed_sync"]["status"] == "ok"

    existing = _row_by_ric(data_db, "AAPL.OQ")
    assert int(existing["classification_ok"]) == 1
    assert int(existing["is_equity_eligible"]) == 1
    assert existing["source"] == "lseg_toolkit"

    seeded = _row_by_ric(data_db, "BABA.N")
    assert seeded["ticker"] == "BABA"
    assert seeded["isin"] == "US01609W1027"
    assert seeded["exchange_name"] == "NYSE"
    assert int(seeded["classification_ok"]) == 0
    assert int(seeded["is_equity_eligible"]) == 0
    assert seeded["source"] == "security_master_seed"


def test_download_from_lseg_updates_security_master_for_pending_explicit_ric(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("TEST.OQ", "TEST", 0, 0, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        ("TEST.OQ", "TEST", "security_registry_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get_company_data(self, batch, *, fields, as_of_date):
            assert batch == ["TEST.OQ"]
            assert "TR.TickerSymbol" in fields
            assert "TR.ISIN" in fields
            assert "TR.ExchangeName" in fields
            assert "TR.TRBCEconomicSector" in fields
            return pd.DataFrame(
                [
                    {
                        "Instrument": "TEST.OQ",
                        "Ticker Symbol": "TEST",
                        "ISIN": "US0000000001",
                        "Exchange Name": "NASDAQ",
                        "TRBC Economic Sector Name": "Technology",
                        "TRBC Business Sector Name": "Software & IT Services",
                        "TRBC Industry Group Name": "Software & IT Services",
                        "TRBC Industry Name": "Software",
                        "TRBC Activity Name": "Application Software",
                        "Country ISO Code of Headquarters": "US",
                        "Price Open": 10.0,
                        "Price High": 11.0,
                        "Price Low": 9.5,
                        "Price Close": 10.5,
                        "Volume": 1000.0,
                        "Price Close Currency": "USD",
                    }
                ]
            )

    monkeypatch.setattr(download_data_lseg, "_load_lseg_client", lambda: FakeClient)

    out = download_data_lseg.download_from_lseg(
        db_path=data_db,
        rics_csv="TEST.OQ,MISS.OQ",
        as_of_date="2026-03-10",
        write_fundamentals=False,
        write_prices=True,
        write_classification=True,
    )

    assert out["status"] == "ok"
    assert out["security_master_rows_upserted"] == 1
    assert out["compat_rows_upserted"] == 1
    assert out["price_rows_inserted"] == 1
    assert out["classification_rows_inserted"] == 1
    assert out["matched_requested_ric_count"] == 1
    assert out["missing_requested_rics"] == ["MISS.OQ"]

    master = _row_by_ric(data_db, "TEST.OQ")
    assert master["ticker"] == "TEST"
    assert master["isin"] == "US0000000001"
    assert master["exchange_name"] == "NASDAQ"
    assert int(master["classification_ok"]) == 1
    assert int(master["is_equity_eligible"]) == 1
    assert master["source"] == "lseg_toolkit"
    conn = sqlite3.connect(str(data_db))
    try:
        compat = conn.execute(
            """
            SELECT classification_ok, is_equity_eligible, coverage_role
            FROM security_master_compat_current
            WHERE ric = 'TEST.OQ'
            """
        ).fetchone()
        taxonomy = conn.execute(
            """
            SELECT instrument_kind, is_single_name_equity, classification_ready
            FROM security_taxonomy_current
            WHERE ric = 'TEST.OQ'
            """
        ).fetchone()
    finally:
        conn.close()

    assert compat == (1, 1, "native_equity")
    assert taxonomy == ("single_name_equity", 1, 1)


def test_download_from_lseg_default_universe_includes_seeded_pending_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("AAPL.OQ", "AAPL", 0, 0, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        ("AAPL.OQ", "AAPL", "security_registry_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, exchange_name, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "BAD.O^L20",
            "BAD",
            "Consolidated Issue Listed on Nasdaq Global Select Market",
            1,
            1,
            "lseg_toolkit",
            "lineage_job",
            "2026-03-15T00:00:00+00:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        ("BAD.O^L20", "BAD", "security_registry_seed", "lineage_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("BAD.O^L20", "2026-03-04", 12.5, 0.0, "USD", "lseg_toolkit", "2026-03-16T03:25:05+00:00"),
            ("BAD.O^L20", "2026-03-13", 12.5, 0.0, "USD", "lseg_toolkit", "2026-03-16T07:35:38+00:00"),
            ("AAPL.OQ", "2026-03-12", 189.0, 1000.0, "USD", "lseg_toolkit", "2026-03-15T00:00:00+00:00"),
        ],
    )
    conn.commit()
    conn.close()

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get_company_data(self, batch, *, fields, as_of_date):
            assert batch == ["AAPL.OQ"]
            return pd.DataFrame(
                [
                    {
                        "Instrument": "AAPL.OQ",
                        "Ticker Symbol": "AAPL",
                        "ISIN": "US0378331005",
                        "Exchange Name": "NASDAQ",
                        "TRBC Economic Sector Name": "Technology",
                        "TRBC Business Sector Name": "Computers, Phones & Household Electronics",
                        "TRBC Industry Group Name": "Computers, Phones & Household Electronics",
                        "TRBC Industry Name": "Phones & Handheld Devices",
                        "TRBC Activity Name": "Phone & Handheld Devices",
                        "Country ISO Code of Headquarters": "US",
                        "Price Close": 190.0,
                        "Price Open": 189.0,
                        "Price High": 191.0,
                        "Price Low": 188.5,
                        "Volume": 1000.0,
                        "Price Close Currency": "USD",
                    }
                ]
            )

    monkeypatch.setattr(download_data_lseg, "_load_lseg_client", lambda: FakeClient)

    out = download_data_lseg.download_from_lseg(
        db_path=data_db,
        as_of_date="2026-03-10",
        write_fundamentals=False,
        write_prices=True,
        write_classification=True,
    )

    assert out["status"] == "ok"
    assert out["universe"] == 1
    assert out["security_master_rows_upserted"] == 1
    master = _row_by_ric(data_db, "AAPL.OQ")
    assert master["ticker"] == "AAPL"
    assert master["isin"] == "US0378331005"
    assert master["exchange_name"] == "NASDAQ"
    assert int(master["classification_ok"]) == 1
    assert int(master["is_equity_eligible"]) == 1
    conn = sqlite3.connect(str(data_db))
    try:
        taxonomy = conn.execute(
            """
            SELECT instrument_kind, is_single_name_equity, classification_ready
            FROM security_taxonomy_current
            WHERE ric = 'AAPL.OQ'
            """
        ).fetchone()
    finally:
        conn.close()

    assert taxonomy == ("single_name_equity", 1, 1)


def test_default_source_universe_excludes_recent_degraded_lineage_rows(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, exchange_name, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("GOOD.N", "GOOD", "New York Stock Exchange", 1, 1, "lseg_toolkit", "job_1", "2026-03-15T00:00:00+00:00"),
            ("BAD.O^L20", "BAD", "Consolidated Issue Listed on Nasdaq Global Select Market", 1, 1, "lseg_toolkit", "job_1", "2026-03-15T00:00:00+00:00"),
            ("PEND.OQ", "PEND", "NASDAQ", 0, 0, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        [
            ("GOOD.N", "GOOD", "security_registry_seed", "job_1", "2026-03-15T00:00:00+00:00"),
            ("BAD.O^L20", "BAD", "security_registry_seed", "job_1", "2026-03-15T00:00:00+00:00"),
            ("PEND.OQ", "PEND", "security_registry_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
        ],
    )
    good_rows = [
        ("GOOD.N", "2026-03-03", 10.0, 1000.0, "USD", "lseg_toolkit", "2026-03-15T00:00:00+00:00"),
        ("GOOD.N", "2026-03-04", 10.5, 1200.0, "USD", "lseg_toolkit", "2026-03-15T00:00:00+00:00"),
        ("GOOD.N", "2026-03-05", 10.6, 1100.0, "USD", "lseg_toolkit", "2026-03-15T00:00:00+00:00"),
        ("GOOD.N", "2026-03-06", 10.4, 1300.0, "USD", "lseg_toolkit", "2026-03-15T00:00:00+00:00"),
        ("GOOD.N", "2026-03-09", 10.7, 1400.0, "USD", "lseg_toolkit", "2026-03-15T00:00:00+00:00"),
        ("GOOD.N", "2026-03-10", 10.8, 1500.0, "USD", "lseg_toolkit", "2026-03-15T00:00:00+00:00"),
        ("GOOD.N", "2026-03-11", 10.9, 1600.0, "USD", "lseg_toolkit", "2026-03-15T00:00:00+00:00"),
        ("GOOD.N", "2026-03-12", 11.0, 1700.0, "USD", "lseg_toolkit", "2026-03-15T00:00:00+00:00"),
        ("GOOD.N", "2026-03-13", 11.1, 1800.0, "USD", "lseg_toolkit", "2026-03-15T00:00:00+00:00"),
        ("BAD.O^L20", "2026-03-04", 12.5, 0.0, "USD", "lseg_toolkit", "2026-03-16T03:25:05+00:00"),
        ("BAD.O^L20", "2026-03-13", 12.5, 0.0, "USD", "lseg_toolkit", "2026-03-16T07:35:38+00:00"),
    ]
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        good_rows,
    )
    conn.commit()

    try:
        rows = load_default_source_universe_rows(conn, include_pending_seed=True)
    finally:
        conn.close()

    assert rows == [
        {"ticker": "PEND", "ric": "PEND.OQ"},
        {"ticker": "GOOD", "ric": "GOOD.N"},
    ]


def test_raw_cross_section_rebuild_filters_default_source_universe(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, exchange_name, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("GOOD.N", "GOOD", "New York Stock Exchange", 1, 1, "lseg_toolkit", "job_1", "2026-03-15T00:00:00+00:00"),
            ("GOOD2.OQ", "GOOD2", "Nasdaq Stock Exchange Global Select Market", 1, 1, "lseg_toolkit", "job_1", "2026-03-15T00:00:00+00:00"),
            ("BAD.O^L20", "BAD", "Consolidated Issue Listed on Nasdaq Global Select Market", 1, 1, "lseg_toolkit", "job_1", "2026-03-15T00:00:00+00:00"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        [
            ("GOOD.N", "GOOD", "security_registry_seed", "job_1", "2026-03-15T00:00:00+00:00"),
            ("GOOD2.OQ", "GOOD2", "security_registry_seed", "job_1", "2026-03-15T00:00:00+00:00"),
            ("BAD.O^L20", "BAD", "security_registry_seed", "job_1", "2026-03-15T00:00:00+00:00"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("GOOD.N", "2025-06-20", 9.0, 900.0, "USD", "lseg_toolkit", "2025-06-20T00:00:00+00:00"),
            ("GOOD.N", "2026-02-20", 10.0, 1000.0, "USD", "lseg_toolkit", "2026-02-20T00:00:00+00:00"),
            ("GOOD.N", "2026-03-03", 10.0, 1000.0, "USD", "lseg_toolkit", "2026-03-03T00:00:00+00:00"),
            ("GOOD.N", "2026-03-04", 10.5, 1200.0, "USD", "lseg_toolkit", "2026-03-04T00:00:00+00:00"),
            ("GOOD.N", "2026-03-05", 10.6, 1100.0, "USD", "lseg_toolkit", "2026-03-05T00:00:00+00:00"),
            ("GOOD.N", "2026-03-06", 10.4, 1300.0, "USD", "lseg_toolkit", "2026-03-06T00:00:00+00:00"),
            ("GOOD.N", "2026-03-09", 10.7, 1400.0, "USD", "lseg_toolkit", "2026-03-09T00:00:00+00:00"),
            ("GOOD.N", "2026-03-10", 10.8, 1500.0, "USD", "lseg_toolkit", "2026-03-10T00:00:00+00:00"),
            ("GOOD.N", "2026-03-11", 10.9, 1600.0, "USD", "lseg_toolkit", "2026-03-11T00:00:00+00:00"),
            ("GOOD.N", "2026-03-12", 11.0, 1700.0, "USD", "lseg_toolkit", "2026-03-12T00:00:00+00:00"),
            ("GOOD.N", "2026-03-13", 11.1, 1800.0, "USD", "lseg_toolkit", "2026-03-13T00:00:00+00:00"),
            ("GOOD2.OQ", "2025-06-20", 21.0, 1900.0, "USD", "lseg_toolkit", "2025-06-20T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-02-20", 22.0, 2000.0, "USD", "lseg_toolkit", "2026-02-20T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-03-03", 22.0, 2000.0, "USD", "lseg_toolkit", "2026-03-03T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-03-04", 22.5, 2200.0, "USD", "lseg_toolkit", "2026-03-04T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-03-05", 22.6, 2100.0, "USD", "lseg_toolkit", "2026-03-05T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-03-06", 22.4, 2300.0, "USD", "lseg_toolkit", "2026-03-06T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-03-09", 22.7, 2400.0, "USD", "lseg_toolkit", "2026-03-09T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-03-10", 22.8, 2500.0, "USD", "lseg_toolkit", "2026-03-10T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-03-11", 22.9, 2600.0, "USD", "lseg_toolkit", "2026-03-11T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-03-12", 23.0, 2700.0, "USD", "lseg_toolkit", "2026-03-12T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-03-13", 23.1, 2800.0, "USD", "lseg_toolkit", "2026-03-13T00:00:00+00:00"),
            ("BAD.O^L20", "2025-06-20", 20.0, 1000.0, "USD", "lseg_toolkit", "2025-06-20T00:00:00+00:00"),
            ("BAD.O^L20", "2026-03-04", 12.5, 0.0, "USD", "lseg_toolkit", "2026-03-16T03:25:05+00:00"),
            ("BAD.O^L20", "2026-03-13", 12.5, 0.0, "USD", "lseg_toolkit", "2026-03-16T07:35:38+00:00"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_fundamentals_pit (
            ric, as_of_date, stat_date, period_end_date, market_cap, shares_outstanding, book_value_per_share,
            forward_eps, trailing_eps, total_debt, operating_cashflow, revenue, total_assets,
            roe_pct, operating_margin_pct, common_name, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("GOOD.N", "2026-02-27", "2026-02-27", "2025-12-31", 1000000.0, 100000.0, 5.0, 1.2, 1.1, 10000.0, 5000.0, 25000.0, 80000.0, 12.0, 10.0, "Good Co", "lseg_toolkit", "job_1", "2026-03-15T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-02-27", "2026-02-27", "2025-12-31", 2000000.0, 200000.0, 6.0, 1.4, 1.3, 12000.0, 7000.0, 35000.0, 120000.0, 14.0, 12.0, "Good Two Co", "lseg_toolkit", "job_1", "2026-03-15T00:00:00+00:00"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group, trbc_industry, trbc_activity,
            hq_country_code, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("GOOD.N", "2026-02-27", "Technology", "Software & IT Services", "Software & IT Services", "Software", "Application Software", "US", "lseg_toolkit", "job_1", "2026-03-15T00:00:00+00:00"),
            ("GOOD2.OQ", "2026-02-27", "Industrials", "Industrial Goods", "Industrial Goods", "Industrial Machinery", "Industrial Machinery", "US", "lseg_toolkit", "job_1", "2026-03-15T00:00:00+00:00"),
        ],
    )
    conn.commit()
    conn.close()

    out = rebuild_raw_cross_section_history(
        data_db,
        start_date="2026-03-13",
        end_date="2026-03-13",
        frequency="latest",
    )

    assert out["status"] == "ok"

    conn = sqlite3.connect(str(data_db))
    try:
        rows = conn.execute(
            """
            SELECT ric
            FROM barra_raw_cross_section_history
            WHERE as_of_date = '2026-03-13'
            ORDER BY ric
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [("GOOD.N",), ("GOOD2.OQ",)]


def test_raw_cross_section_history_uses_date_specific_runtime_eligibility(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('TEST.OQ', 'TEST', 1, 1, 'native_equity', 'seed', '2026-03-01T00:00:00+00:00')
        """
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (
            ric, date, close, volume, currency, source, updated_at
        ) VALUES (?, ?, ?, ?, 'USD', 'lseg_toolkit', ?)
        """,
        [
            ("TEST.OQ", "2025-06-01", 80.0, 1000.0, "2025-06-01T00:00:00+00:00"),
            ("TEST.OQ", "2026-03-06", 100.0, 1000.0, "2026-03-06T00:00:00+00:00"),
            ("TEST.OQ", "2026-03-13", 101.0, 1000.0, "2026-03-13T00:00:00+00:00"),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (
            ric, as_of_date, stat_date, period_end_date, market_cap, shares_outstanding,
            book_value_per_share, forward_eps, trailing_eps, total_debt, operating_cashflow,
            revenue, total_assets, roe_pct, operating_margin_pct, common_name, source, job_run_id, updated_at
        ) VALUES (
            'TEST.OQ', '2026-02-27', '2026-02-27', '2025-12-31', 1000000.0, 100000.0,
            5.0, 1.2, 1.1, 10000.0, 5000.0,
            25000.0, 80000.0, 12.0, 10.0, 'Test Co', 'lseg_toolkit', 'job_1', '2026-03-01T00:00:00+00:00'
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group, trbc_industry,
            trbc_activity, hq_country_code, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'lseg_toolkit', 'job_1', ?)
        """,
        [
            (
                "TEST.OQ",
                "2026-03-06",
                "Technology",
                "Software & IT Services",
                "Software & IT Services",
                "Software",
                "Application Software",
                "US",
                "2026-03-06T00:00:00+00:00",
            ),
            (
                "TEST.OQ",
                "2026-03-13",
                "Exchange Traded Fund",
                "Investment Services",
                "Investment Services",
                "Exchange Traded Fund",
                "Exchange Traded Fund",
                "US",
                "2026-03-13T00:00:00+00:00",
            ),
        ],
    )
    conn.commit()
    conn.close()

    out = rebuild_raw_cross_section_history(
        data_db,
        start_date="2026-03-06",
        end_date="2026-03-13",
        frequency="weekly",
    )

    assert out["status"] == "ok"

    conn = sqlite3.connect(str(data_db))
    try:
        rows = conn.execute(
            """
            SELECT as_of_date, ric
            FROM barra_raw_cross_section_history
            WHERE ric = 'TEST.OQ'
            ORDER BY as_of_date
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [("2026-03-06", "TEST.OQ")]


def test_backfill_prices_allows_explicit_pending_ric(monkeypatch, tmp_path: Path) -> None:
    fake_lseg_data = types.ModuleType("lseg.data")
    fake_lseg_data.open_session = lambda: None
    fake_lseg_data.close_session = lambda: None
    fake_lseg_data.get_data = lambda **kwargs: pd.DataFrame()
    backfill_prices_range_lseg = _import_backfill_prices_range_with_fake_lseg(monkeypatch, fake_lseg_data)

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("PEND.OQ", "PEND", 0, 0, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        ("PEND.OQ", "PEND", "security_registry_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_prices_range_lseg.rd, "open_session", lambda: None)
    monkeypatch.setattr(backfill_prices_range_lseg.rd, "close_session", lambda: None)
    monkeypatch.setattr(backfill_prices_range_lseg.rd, "get_data", fake_lseg_data.get_data)
    monkeypatch.setattr(
        backfill_prices_range_lseg.rd,
        "get_data",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "Instrument": "PEND.OQ",
                    "Date": "2026-03-10",
                    "Price Open": 20.0,
                    "Price High": 21.0,
                    "Price Low": 19.5,
                    "Price Close": 20.5,
                    "Volume": 500.0,
                    "Price Close Currency": "USD",
                }
            ]
        ),
    )

    out = backfill_prices_range_lseg.backfill_prices(
        db_path=data_db,
        start_date="2026-03-10",
        end_date="2026-03-10",
        ticker_batch_size=100,
        days_per_window=1,
        max_retries=0,
        sleep_seconds=0.0,
        rics_csv="PEND.OQ,MISS.OQ",
    )

    assert out["status"] == "ok"
    assert out["rows_upserted"] == 1
    assert out["matched_requested_ric_count"] == 1
    assert out["missing_requested_rics"] == ["MISS.OQ"]

    conn = sqlite3.connect(str(data_db))
    try:
        row = conn.execute(
            """
            SELECT close, volume, currency
            FROM security_prices_eod
            WHERE ric = ? AND date = ?
            """,
            ("PEND.OQ", "2026-03-10"),
        ).fetchone()
    finally:
        conn.close()

    assert row == (20.5, 500.0, "USD")


def test_download_from_lseg_skips_price_rows_with_missing_close(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("MISS.OQ", "MISS", 1, 1, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        ("MISS.OQ", "MISS", "security_registry_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get_company_data(self, batch, *, fields, as_of_date):
            return pd.DataFrame(
                [
                    {
                        "Instrument": "MISS.OQ",
                        "Ticker Symbol": "MISS",
                        "Price Open": 10.0,
                        "Price High": 11.0,
                        "Price Low": 9.5,
                        "Price Close": None,
                        "Volume": 1000.0,
                        "Price Close Currency": "USD",
                    }
                ]
            )

    monkeypatch.setattr(download_data_lseg, "_load_lseg_client", lambda: FakeClient)

    out = download_data_lseg.download_from_lseg(
        db_path=data_db,
        rics_csv="MISS.OQ",
        as_of_date="2026-03-10",
        write_fundamentals=False,
        write_prices=True,
        write_classification=False,
    )

    assert out["status"] == "ok"
    assert out["price_rows_inserted"] == 0
    assert out["price_rows_skipped_missing_close"] == 1

    conn = sqlite3.connect(str(data_db))
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM security_prices_eod
            WHERE ric = ? AND date = ?
            """,
            ("MISS.OQ", "2026-03-10"),
        ).fetchone()
    finally:
        conn.close()

    assert row == (0,)


def test_backfill_prices_closes_managed_session_after_failed_batch(monkeypatch, tmp_path: Path) -> None:
    fake_lseg_data = types.ModuleType("lseg.data")
    fake_lseg_data.open_session = lambda: None
    fake_lseg_data.close_session = lambda: None
    fake_lseg_data.get_data = lambda **kwargs: pd.DataFrame()
    backfill_prices_range_lseg = _import_backfill_prices_range_with_fake_lseg(monkeypatch, fake_lseg_data)

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        ("FAIL.OQ", "FAIL", "security_registry_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    opened = object()
    closed: list[object] = []
    monkeypatch.setattr(backfill_prices_range_lseg, "open_managed_session", lambda: opened)
    monkeypatch.setattr(backfill_prices_range_lseg, "close_managed_session", lambda managed: closed.append(managed))
    monkeypatch.setattr(
        backfill_prices_range_lseg.rd,
        "get_data",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("lseg failure")),
    )

    out = backfill_prices_range_lseg.backfill_prices(
        db_path=data_db,
        start_date="2026-03-10",
        end_date="2026-03-10",
        ticker_batch_size=100,
        days_per_window=1,
        max_retries=0,
        sleep_seconds=0.0,
        rics_csv="FAIL.OQ",
    )

    assert out["status"] == "ok"
    assert out["rows_upserted"] == 0
    assert out["failed_batches"] == 1
    assert closed == [opened]


def test_download_from_lseg_reports_missing_requested_rics_on_no_data(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("TEST.OQ", "TEST", 1, 1, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        ("TEST.OQ", "TEST", "security_registry_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get_company_data(self, batch, *, fields, as_of_date):
            assert batch == ["TEST.OQ"]
            return pd.DataFrame()

    monkeypatch.setattr(download_data_lseg, "_load_lseg_client", lambda: FakeClient)

    out = download_data_lseg.download_from_lseg(
        db_path=data_db,
        rics_csv="TEST.OQ,MISS.OQ",
        as_of_date="2026-03-10",
        write_fundamentals=False,
        write_prices=True,
        write_classification=False,
    )

    assert out["status"] == "no-data"
    assert out["matched_requested_ric_count"] == 1
    assert out["missing_requested_rics"] == ["MISS.OQ"]


def test_backfill_prices_skips_rows_with_missing_close(monkeypatch, tmp_path: Path) -> None:
    fake_lseg_data = types.ModuleType("lseg.data")
    fake_lseg_data.open_session = lambda: None
    fake_lseg_data.close_session = lambda: None
    fake_lseg_data.get_data = lambda **kwargs: pd.DataFrame(
        [
            {
                "Instrument": "MISS.OQ",
                "Date": "2026-03-10",
                "Price Open": 20.0,
                "Price High": 21.0,
                "Price Low": 19.5,
                "Price Close": None,
                "Volume": 500.0,
                "Price Close Currency": "USD",
            }
        ]
    )
    backfill_prices_range_lseg = _import_backfill_prices_range_with_fake_lseg(monkeypatch, fake_lseg_data)

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("MISS.OQ", "MISS", 1, 1, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        ("MISS.OQ", "MISS", "security_registry_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_prices_range_lseg.rd, "open_session", lambda: None)
    monkeypatch.setattr(backfill_prices_range_lseg.rd, "close_session", lambda: None)
    monkeypatch.setattr(backfill_prices_range_lseg.rd, "get_data", fake_lseg_data.get_data)

    out = backfill_prices_range_lseg.backfill_prices(
        db_path=data_db,
        start_date="2026-03-10",
        end_date="2026-03-10",
        ticker_batch_size=100,
        days_per_window=1,
        max_retries=0,
        sleep_seconds=0.0,
        rics_csv="MISS.OQ",
    )

    assert out["status"] == "ok"
    assert out["rows_upserted"] == 0
    assert out["price_rows_skipped_missing_close"] == 1

    conn = sqlite3.connect(str(data_db))
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM security_prices_eod
            WHERE ric = ? AND date = ?
            """,
            ("MISS.OQ", "2026-03-10"),
        ).fetchone()
    finally:
        conn.close()

    assert row == (0,)


def test_backfill_volume_only_reports_missing_requested_rics_on_no_null_volume(monkeypatch, tmp_path: Path) -> None:
    fake_lseg_data = types.ModuleType("lseg.data")
    fake_lseg_data.open_session = lambda: None
    fake_lseg_data.close_session = lambda: None
    fake_lseg_data.get_data = lambda **kwargs: pd.DataFrame()
    backfill_prices_range_lseg = _import_backfill_prices_range_with_fake_lseg(monkeypatch, fake_lseg_data)

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("PEND.OQ", "PEND", 1, 1, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        ("PEND.OQ", "PEND", "security_registry_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_prices_range_lseg, "_load_missing_volume_pairs", lambda *args, **kwargs: pd.DataFrame())

    out = backfill_prices_range_lseg.backfill_prices(
        db_path=data_db,
        start_date="2026-03-10",
        end_date="2026-03-10",
        ticker_batch_size=100,
        days_per_window=1,
        max_retries=0,
        sleep_seconds=0.0,
        rics_csv="PEND.OQ,MISS.OQ",
        volume_only=True,
        only_null_volume=True,
    )

    assert out["status"] == "no-null-volume"
    assert out["matched_requested_ric_count"] == 1
    assert out["missing_requested_rics"] == ["MISS.OQ"]


def test_backfill_reports_missing_requested_rics_on_no_date_windows(monkeypatch, tmp_path: Path) -> None:
    fake_lseg_data = types.ModuleType("lseg.data")
    fake_lseg_data.open_session = lambda: None
    fake_lseg_data.close_session = lambda: None
    fake_lseg_data.get_data = lambda **kwargs: pd.DataFrame()
    backfill_prices_range_lseg = _import_backfill_prices_range_with_fake_lseg(monkeypatch, fake_lseg_data)

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("PEND.OQ", "PEND", 1, 1, "security_master_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, 'active', ?, ?, ?)
        """,
        ("PEND.OQ", "PEND", "security_registry_seed", "seed_job", "2026-03-15T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    out = backfill_prices_range_lseg.backfill_prices(
        db_path=data_db,
        start_date="2026-03-11",
        end_date="2026-03-10",
        ticker_batch_size=100,
        days_per_window=1,
        max_retries=0,
        sleep_seconds=0.0,
        rics_csv="PEND.OQ,MISS.OQ",
    )

    assert out["status"] == "no-date-windows"
    assert out["matched_requested_ric_count"] == 1
    assert out["missing_requested_rics"] == ["MISS.OQ"]


def test_legacy_augment_security_master_from_xlsx_keeps_new_rows_pending(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    seed_xlsx = tmp_path / "new_universe.xlsx"
    seed_xlsx.write_bytes(b"placeholder")

    class FakeExcelFile:
        sheet_names = ["Sheet1"]

        def __init__(self, _path):
            self.sheet_names = ["Sheet1"]

    monkeypatch.setattr(augment_security_master_from_ric_xlsx.pd, "ExcelFile", FakeExcelFile)
    monkeypatch.setattr(
        augment_security_master_from_ric_xlsx.pd,
        "read_excel",
        lambda *_args, **_kwargs: pd.DataFrame({"RIC": ["NEW1.OQ", "NEW2.N"]}),
    )

    out = augment_security_master_from_ric_xlsx.run(
        db_path=data_db,
        xlsx_path=seed_xlsx,
        sheet=None,
        source="coverage_universe_xlsx",
        output_new_rics=None,
    )

    assert out["status"] == "ok"
    assert out["new_rics_inserted"] == 2

    conn = sqlite3.connect(str(data_db))
    try:
        master_rows = conn.execute(
            """
            SELECT ric, ticker, classification_ok, is_equity_eligible
            FROM security_master
            ORDER BY ric
            """
        ).fetchall()
        registry_rows = conn.execute(
            """
            SELECT ric, ticker, tracking_status
            FROM security_registry
            ORDER BY ric
            """
        ).fetchall()
        policy_rows = conn.execute(
            """
            SELECT ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled
            FROM security_policy_current
            ORDER BY ric
            """
        ).fetchall()
        compat_rows = conn.execute(
            """
            SELECT ric, ticker, coverage_role
            FROM security_master_compat_current
            ORDER BY ric
            """
        ).fetchall()
    finally:
        conn.close()

    assert master_rows == [
        ("NEW1.OQ", "NEW1", 0, 0),
        ("NEW2.N", "NEW2", 0, 0),
    ]
    assert registry_rows == [
        ("NEW1.OQ", "NEW1", "active"),
        ("NEW2.N", "NEW2", "active"),
    ]
    assert policy_rows == [
        ("NEW1.OQ", 1, 1, 1),
        ("NEW2.N", 1, 1, 1),
    ]
    assert compat_rows == [
        ("NEW1.OQ", "NEW1", "native_equity"),
        ("NEW2.N", "NEW2", "native_equity"),
    ]


def test_legacy_augment_security_master_from_xlsx_rolls_back_multi_surface_seed_on_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    seed_xlsx = tmp_path / "new_universe.xlsx"
    seed_xlsx.write_bytes(b"placeholder")

    class FakeExcelFile:
        sheet_names = ["Sheet1"]

        def __init__(self, _path):
            self.sheet_names = ["Sheet1"]

    monkeypatch.setattr(augment_security_master_from_ric_xlsx.pd, "ExcelFile", FakeExcelFile)
    monkeypatch.setattr(
        augment_security_master_from_ric_xlsx.pd,
        "read_excel",
        lambda *_args, **_kwargs: pd.DataFrame({"RIC": ["FAIL1.OQ"]}),
    )
    monkeypatch.setattr(
        augment_security_master_from_ric_xlsx,
        "refresh_security_taxonomy_current",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("taxonomy failure")),
    )

    with pytest.raises(RuntimeError, match="taxonomy failure"):
        augment_security_master_from_ric_xlsx.run(
            db_path=data_db,
            xlsx_path=seed_xlsx,
            sheet=None,
            source="coverage_universe_xlsx",
            output_new_rics=None,
        )

    conn = sqlite3.connect(str(data_db))
    try:
        counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "security_registry",
                "security_policy_current",
                "security_master",
                "security_master_compat_current",
            )
        }
    finally:
        conn.close()

    assert counts == {
        "security_registry": 0,
        "security_policy_current": 0,
        "security_master": 0,
        "security_master_compat_current": 0,
    }


def test_explicit_runtime_ingest_scope_respects_price_ingest_gating(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES ('SKIP.OQ', 'SKIP', 'active', '2026-03-15T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES ('SKIP.OQ', 0, 1, 1, 1, 0, 0, 1, 1, 'manual_override', '2026-03-15T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, updated_at
        ) VALUES ('SKIP.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, '2026-03-15T00:00:00Z')
        """
    )

    rows = download_data_lseg._load_runtime_ingest_scope(
        conn,
        tickers=["SKIP"],
        rics=None,
        write_fundamentals=False,
        write_prices=True,
        write_classification=False,
    )
    conn.close()

    assert rows == []


def test_download_from_lseg_default_price_refresh_uses_price_scope_not_pit_scope(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    now_iso = "2026-03-25T00:00:00+00:00"
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES ('AHL.N', 'AHL', 'active', 'security_registry_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cpar_core_target, allow_cpar_extended_target,
            policy_source, updated_at
        ) VALUES ('AHL.N', 1, 1, 1, 1, 1, 1, 'default', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, source, updated_at
        ) VALUES ('AHL.N', 'single_name_equity', 'equity_security', 'us', 1, 1, 'taxonomy_refresh', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, updated_at
        ) VALUES ('AHL.N', 'AHL', 'New York Stock Exchange', 1, 1, 'native_equity', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, ?, ?, ?, 'USD', 'lseg_toolkit', ?)
        """,
        [
            ("AHL.N", "2026-03-20", 37.5, 0.0, now_iso),
            ("AHL.N", "2026-03-25", 37.5, 0.0, now_iso),
        ],
    )
    conn.commit()
    conn.close()

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get_company_data(self, batch, *, fields, as_of_date):
            assert batch == ["AHL.N"]
            return pd.DataFrame(
                [
                    {
                        "Instrument": "AHL.N",
                        "Ticker Symbol": "AHL",
                        "ISIN": "BMG053841013",
                        "Exchange Name": "New York Stock Exchange",
                        "Price Close": 38.25,
                        "Price Open": 37.8,
                        "Price High": 38.5,
                        "Price Low": 37.7,
                        "Volume": 1250.0,
                        "Price Close Currency": "USD",
                    }
                ]
            )

    monkeypatch.setattr(download_data_lseg, "_load_lseg_client", lambda: FakeClient)

    out = download_data_lseg.download_from_lseg(
        db_path=data_db,
        as_of_date="2026-03-25",
        write_fundamentals=False,
        write_prices=True,
        write_classification=False,
    )

    assert out["status"] == "ok"
    assert out["universe"] == 1
    assert out["price_rows_inserted"] == 1
    conn = sqlite3.connect(str(data_db))
    try:
        close = conn.execute(
            """
            SELECT close
            FROM security_prices_eod
            WHERE ric = 'AHL.N' AND date = '2026-03-25'
            """
        ).fetchone()
    finally:
        conn.close()
    assert close == (38.25,)


def test_export_security_master_seed_preserves_legacy_coverage_role_columns(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "AAPL.OQ",
            "AAPL",
            "US0378331005",
            "NASDAQ",
            1,
            1,
            "native_equity",
            "lseg_toolkit",
            "job_1",
            "2026-03-15T00:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()

    output_path = tmp_path / "security_master_seed.csv"
    exported = export_seed(data_db=data_db, output_path=output_path)

    assert exported == 1
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "ric,ticker,isin,exchange_name,coverage_role",
        "AAPL.OQ,AAPL,US0378331005,NASDAQ,native_equity",
    ]


def test_export_security_master_seed_derives_projection_only_role_from_registry_policy(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, isin, exchange_name, tracking_status, source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "SPY.P",
            "SPY",
            "US78462F1030",
            "NYSE Arca",
            "active",
            "security_registry_seed",
            "job_registry",
            "2026-03-15T00:00:00+00:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric,
            price_ingest_enabled,
            pit_fundamentals_enabled,
            pit_classification_enabled,
            allow_cuse_native_core,
            allow_cuse_fundamental_projection,
            allow_cuse_returns_projection,
            allow_cpar_core_target,
            allow_cpar_extended_target,
            policy_source,
            job_run_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "SPY.P",
            1,
            0,
            0,
            0,
            0,
            1,
            0,
            1,
            "registry_seed_defaults",
            "job_policy",
            "2026-03-15T00:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()

    output_path = tmp_path / "security_master_seed.csv"
    exported = export_seed(data_db=data_db, output_path=output_path)

    assert exported == 1
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "ric,ticker,isin,exchange_name,coverage_role",
        "SPY.P,SPY,US78462F1030,NYSE Arca,projection_only",
    ]
