from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.data import core_reads, source_reads
from backend.universe.schema import ensure_cuse4_schema


def test_load_raw_cross_section_latest_prefers_well_covered_asof(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        core_reads.source_reads,
        "exposure_source_table_required",
        lambda **_kwargs: "barra_raw_cross_section_history",
    )

    def _fake_fetch(sql: str, params=None):
        if "COUNT(*) AS row_count" in sql:
            return [
                {"as_of_date": "2026-03-04", "row_count": 10},
                {"as_of_date": "2026-03-03", "row_count": 3681},
            ]
        captured["params"] = list(params or [])
        return [
            {
                "ric": "LAZ.N",
                "ticker": "LAZ",
                "as_of_date": "2026-03-03",
                "growth_score": 0.5,
            }
        ]

    monkeypatch.setattr(core_reads.core_backend, "fetch_rows", lambda sql, params=None, **_kwargs: _fake_fetch(sql, params))

    out = core_reads.load_raw_cross_section_latest(tickers=["laz"])

    assert captured["params"] == ["2026-03-03", "LAZ"]
    assert out.to_dict("records") == [
        {
            "ric": "LAZ.N",
            "ticker": "LAZ",
            "as_of_date": "2026-03-03",
            "growth_score": 0.5,
        }
    ]


def test_load_latest_prices_sqlite_refreshes_latest_price_cache(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute("CREATE TABLE security_master (ric TEXT PRIMARY KEY, ticker TEXT)")
    conn.execute(
        """
        CREATE TABLE security_prices_eod (
            ric TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL,
            PRIMARY KEY (ric, date)
        )
        """
    )
    conn.execute("INSERT INTO security_master (ric, ticker) VALUES ('AAPL.OQ', 'AAPL')")
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date, close) VALUES (?, ?, ?)",
        [
            ("AAPL.OQ", "2026-03-01", 100.0),
            ("AAPL.OQ", "2026-03-02", 101.0),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(core_reads, "DATA_DB", data_db)
    monkeypatch.setattr(core_reads.core_backend, "use_neon_core_reads", lambda: False)

    first = core_reads.load_latest_prices()
    assert first.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-02", "close": 101.0}
    ]

    conn = sqlite3.connect(str(data_db))
    conn.execute(
        "INSERT INTO security_prices_eod (ric, date, close) VALUES (?, ?, ?)",
        ("AAPL.OQ", "2026-03-03", 102.5),
    )
    conn.commit()
    conn.close()

    second = core_reads.load_latest_prices()
    assert second.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-03", "close": 102.5}
    ]


def test_load_latest_prices_sqlite_prefers_registry_runtime_surfaces(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE security_registry (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            tracking_status TEXT NOT NULL DEFAULT 'active',
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
            price_ingest_enabled INTEGER NOT NULL DEFAULT 1,
            pit_fundamentals_enabled INTEGER NOT NULL DEFAULT 1,
            pit_classification_enabled INTEGER NOT NULL DEFAULT 1,
            allow_cuse_native_core INTEGER NOT NULL DEFAULT 1,
            allow_cuse_fundamental_projection INTEGER NOT NULL DEFAULT 0,
            allow_cuse_returns_projection INTEGER NOT NULL DEFAULT 0,
            allow_cpar_core_target INTEGER NOT NULL DEFAULT 1,
            allow_cpar_extended_target INTEGER NOT NULL DEFAULT 1,
            policy_source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_taxonomy_current (
            ric TEXT PRIMARY KEY,
            instrument_kind TEXT,
            vehicle_structure TEXT,
            issuer_country_code TEXT,
            listing_country_code TEXT,
            model_home_market_scope TEXT,
            is_single_name_equity INTEGER NOT NULL DEFAULT 0,
            classification_ready INTEGER NOT NULL DEFAULT 0,
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
            close REAL,
            PRIMARY KEY (ric, date)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES ('AAPL.OQ', 'AAPL', 'active', '2026-03-01T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 1, 0, 0, 1, 1, '2026-03-01T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, updated_at
        ) VALUES ('AAPL.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, '2026-03-01T00:00:00Z')
        """
    )
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date, close) VALUES (?, ?, ?)",
        [
            ("AAPL.OQ", "2026-03-01", 100.0),
            ("AAPL.OQ", "2026-03-03", 102.5),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(core_reads, "DATA_DB", data_db)
    monkeypatch.setattr(core_reads.core_backend, "use_neon_core_reads", lambda: False)

    out = core_reads.load_latest_prices()

    assert out.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-03", "close": 102.5}
    ]


def test_load_latest_prices_prefers_compat_current_before_security_master(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'WRONG', 1, 1, 'native_equity', 'legacy_master', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 'US0378331005', 'NASDAQ', 1, 1, 'native_equity', 'compat', 'job_1', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-13', 102.5, 1000.0, 'USD', 'prices', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_prices(
        tickers=["AAPL"],
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-13", "close": 102.5}
    ]


def test_load_latest_prices_keeps_compat_rows_visible_when_registry_surfaces_are_partial(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    now = "2026-03-13T00:00:00Z"
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES (?, ?, 'active', 'registry', ?)
        """,
        [
            ("AAPL.OQ", "AAPL", now),
            ("MSFT.OQ", "MSFT", now),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES (?, 1, 1, 1, 1, 0, 0, 1, 1, ?)
        """,
        [
            ("AAPL.OQ", now),
            ("MSFT.OQ", now),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, updated_at
        ) VALUES ('AAPL.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, ?)
        """,
        (now,),
    )
    conn.executemany(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, 1, 1, 'native_equity', 'compat', 'job_1', ?)
        """,
        [
            ("AAPL.OQ", "AAPL", now),
            ("MSFT.OQ", "MSFT", now),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, '2026-03-13', ?, 1000.0, 'USD', 'prices', ?)
        """,
        [
            ("AAPL.OQ", 102.5, now),
            ("MSFT.OQ", 250.0, now),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(core_reads, "DATA_DB", data_db)
    monkeypatch.setattr(core_reads.core_backend, "use_neon_core_reads", lambda: False)

    out = core_reads.load_latest_prices()

    assert out.sort_values("ric").to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-13", "close": 102.5},
        {"ric": "MSFT.OQ", "ticker": "MSFT", "date": "2026-03-13", "close": 250.0},
    ]


def test_load_latest_prices_compat_current_drives_identity_without_security_master_fallback(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", 1, 1, "native_equity", "legacy_master", "2026-03-13T00:00:00Z"),
            ("SPY.P", "SPY", 0, 0, "projection_only", "legacy_master", "2026-03-13T00:00:00Z"),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, ?, ?, ?, 'compat', ?, '2026-03-13T00:00:00Z')
        """
        ,
        ("AAPL.OQ", "AAPL", 1, 1, "native_equity", "job_aapl"),
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, ?, ?, ?, 'compat', ?, '2026-03-13T00:00:00Z')
        """,
        ("SPY.P", "SPY", 0, 0, "projection_only", "job_spy"),
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, '2026-03-13', ?, 1000.0, 'USD', 'prices', '2026-03-13T00:00:00Z')
        """,
        [
            ("AAPL.OQ", 102.5),
            ("SPY.P", 501.0),
        ],
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_prices(
        tickers=["AAPL", "SPY"],
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out.to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "date": "2026-03-13", "close": 102.5},
        {"ric": "SPY.P", "ticker": "SPY", "date": "2026-03-13", "close": 501.0},
    ]


def test_load_latest_fundamentals_falls_back_until_runtime_surfaces_are_fully_populated(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (ric, ticker, updated_at)
        VALUES ('AAPL.OQ', 'AAPL', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES ('AAPL.OQ', 'AAPL', 'active', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', NULL, NULL, 1, 1, 'native_equity', 'compat', 'job_aapl', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, common_name, updated_at)
        VALUES ('AAPL.OQ', '2026-03-12', '2025-12-31', 'Apple Inc.', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group, updated_at
        ) VALUES ('AAPL.OQ', '2026-03-12', 'Tech', 'Hardware', 'Computers', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_fundamentals(
        tickers=["AAPL"],
        as_of_date="2026-03-15",
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out[["ric", "ticker", "common_name"]].to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "common_name": "Apple Inc."}
    ]


def test_load_latest_fundamentals_does_not_abandon_registry_reads_for_unrelated_partial_rows(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.executemany(
        """
        INSERT INTO security_master (ric, ticker, updated_at)
        VALUES (?, ?, '2026-03-13T00:00:00Z')
        """,
        [
            ("AAPL.OQ", "WRONG"),
            ("ORPH.X", "ORPH"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES (?, ?, 'active', '2026-03-13T00:00:00Z')
        """,
        [
            ("AAPL.OQ", "AAPL"),
            ("ORPH.X", "ORPH"),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 1, 0, 0, 1, 1, '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, updated_at
        ) VALUES ('AAPL.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, common_name, updated_at)
        VALUES ('AAPL.OQ', '2026-03-12', '2025-12-31', 'Apple Inc.', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group, updated_at
        ) VALUES ('AAPL.OQ', '2026-03-12', 'Tech', 'Hardware', 'Computers', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_fundamentals(
        tickers=["AAPL"],
        as_of_date="2026-03-15",
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out[["ric", "ticker", "common_name"]].to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "common_name": "Apple Inc."}
    ]


def test_load_latest_fundamentals_registry_path_falls_back_per_row_when_requested_taxonomy_is_missing(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, updated_at
        ) VALUES (?, ?, ?, ?, ?, '2026-03-13T00:00:00Z')
        """,
        [
            ("AAPL.OQ", "AAPL", 1, 1, "native_equity"),
            ("OTHER.OQ", "OTHER", 1, 1, "native_equity"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, 1, 1, 'native_equity', 'compat', 'job_1', '2026-03-13T00:00:00Z')
        """,
        [
            ("AAPL.OQ", "AAPL"),
            ("OTHER.OQ", "OTHER"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES (?, ?, 'active', '2026-03-13T00:00:00Z')
        """,
        [
            ("AAPL.OQ", "AAPL"),
            ("OTHER.OQ", "OTHER"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES (?, 1, 1, 1, 1, 0, 0, 1, 1, '2026-03-13T00:00:00Z')
        """,
        [
            ("AAPL.OQ",),
            ("OTHER.OQ",),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, updated_at
        ) VALUES ('OTHER.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, common_name, updated_at)
        VALUES ('AAPL.OQ', '2026-03-12', '2025-12-31', 'Apple Inc.', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group, updated_at
        ) VALUES ('AAPL.OQ', '2026-03-12', 'Tech', 'Hardware', 'Computers', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_fundamentals(
        tickers=["AAPL"],
        as_of_date="2026-03-15",
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out[["ric", "ticker", "common_name"]].to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "common_name": "Apple Inc."}
    ]


def test_load_latest_fundamentals_prefers_compat_current_before_security_master(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'WRONG', 1, 1, 'native_equity', 'legacy_master', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 'US0378331005', 'NASDAQ', 1, 1, 'native_equity', 'compat', 'job_1', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (
            ric, as_of_date, stat_date, common_name, market_cap, shares_outstanding, dividend_yield,
            book_value_per_share, total_assets, total_debt, cash_and_equivalents, long_term_debt,
            operating_cashflow, capital_expenditures, trailing_eps, forward_eps, revenue, ebitda, ebit,
            roe_pct, operating_margin_pct, period_end_date, report_currency, fiscal_year, period_type,
            source, job_run_id, updated_at
        ) VALUES (
            'AAPL.OQ', '2026-03-12', '2025-12-31', 'Apple Inc.', 100.0, 10.0, 0.5,
            1.5, 200.0, 50.0, 25.0, 10.0,
            12.0, 3.0, 4.0, 5.0, 80.0, 40.0, 30.0,
            15.0, 20.0, '2025-12-31', 'USD', 2025, 'FY',
            'fundamentals', 'job_f', '2026-03-13T00:00:00Z'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group,
            trbc_industry, trbc_activity, hq_country_code, source, job_run_id, updated_at
        ) VALUES (
            'AAPL.OQ', '2026-03-12', 'Technology', 'Hardware', 'Computers',
            'Computers', 'Consumer Electronics', 'US', 'class', 'job_c', '2026-03-13T00:00:00Z'
        )
        """
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_fundamentals(
        tickers=["AAPL"],
        as_of_date="2026-03-15",
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out[["ric", "ticker", "common_name"]].to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "common_name": "Apple Inc."}
    ]


def test_load_latest_fundamentals_compat_current_drives_identity_without_security_master_fallback(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", 1, 1, "native_equity", "legacy_master", "2026-03-13T00:00:00Z"),
            ("SPY.P", "SPY", 0, 0, "projection_only", "legacy_master", "2026-03-13T00:00:00Z"),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, ?, ?, ?, 'compat', ?, '2026-03-13T00:00:00Z')
        """
        ,
        ("AAPL.OQ", "AAPL", 1, 1, "native_equity", "job_aapl"),
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES (?, ?, NULL, NULL, ?, ?, ?, 'compat', ?, '2026-03-13T00:00:00Z')
        """,
        ("SPY.P", "SPY", 0, 0, "projection_only", "job_spy"),
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (
            ric, as_of_date, stat_date, common_name, market_cap, shares_outstanding, dividend_yield,
            book_value_per_share, total_assets, total_debt, cash_and_equivalents, long_term_debt,
            operating_cashflow, capital_expenditures, trailing_eps, forward_eps, revenue, ebitda, ebit,
            roe_pct, operating_margin_pct, period_end_date, report_currency, fiscal_year, period_type,
            source, job_run_id, updated_at
        ) VALUES (
            'AAPL.OQ', '2026-03-12', '2025-12-31', 'Apple Inc.', 100.0, 10.0, 0.5,
            1.5, 200.0, 50.0, 25.0, 10.0,
            12.0, 3.0, 4.0, 5.0, 80.0, 40.0, 30.0,
            15.0, 20.0, '2025-12-31', 'USD', 2025, 'FY',
            'fundamentals', 'job_f', '2026-03-13T00:00:00Z'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group,
            trbc_industry, trbc_activity, hq_country_code, source, job_run_id, updated_at
        ) VALUES (
            'AAPL.OQ', '2026-03-12', 'Technology', 'Hardware', 'Computers',
            'Computers', 'Consumer Electronics', 'US', 'class', 'job_c', '2026-03-13T00:00:00Z'
        )
        """
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_fundamentals(
        tickers=["AAPL"],
        as_of_date="2026-03-15",
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out[["ric", "ticker", "common_name"]].to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "common_name": "Apple Inc."}
    ]


def test_load_latest_fundamentals_can_read_registry_runtime_surfaces_without_security_master(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute("DELETE FROM security_master")
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES ('AAPL.OQ', 'AAPL', 'active', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 1, 0, 0, 1, 1, '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, updated_at
        ) VALUES ('AAPL.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, common_name, updated_at)
        VALUES ('AAPL.OQ', '2026-03-12', '2025-12-31', 'Apple Inc.', '2026-03-13T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group, updated_at
        ) VALUES ('AAPL.OQ', '2026-03-12', 'Tech', 'Hardware', 'Computers', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    def _fetch(sql: str, params=None):
        with sqlite3.connect(str(data_db)) as read_conn:
            read_conn.row_factory = sqlite3.Row
            return [dict(row) for row in read_conn.execute(sql, params or []).fetchall()]

    def _missing(*tables: str) -> list[str]:
        with sqlite3.connect(str(data_db)) as read_conn:
            return [
                table
                for table in tables
                if read_conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                is None
            ]

    out = source_reads.load_latest_fundamentals(
        tickers=["AAPL"],
        as_of_date="2026-03-15",
        fetch_rows_fn=_fetch,
        missing_tables_fn=_missing,
    )

    assert out[["ric", "ticker", "common_name"]].to_dict("records") == [
        {"ric": "AAPL.OQ", "ticker": "AAPL", "common_name": "Apple Inc."}
    ]


def test_core_read_backend_override_can_force_local_or_neon(monkeypatch) -> None:
    monkeypatch.setattr(core_reads.config, "neon_surface_enabled", lambda surface: surface == "core_reads")

    assert core_reads.core_read_backend_name() == "neon"
    with core_reads.core_read_backend("local"):
        assert core_reads.core_read_backend_name() == "local"
    with core_reads.core_read_backend("neon"):
        assert core_reads.core_read_backend_name() == "neon"


def test_load_source_dates_exposes_explicit_latest_available_exposure_date(monkeypatch) -> None:
    monkeypatch.setattr(core_reads.source_dates, "_pit_latest_closed_anchor", lambda: "2026-03-02")
    monkeypatch.setattr(
        core_reads.core_backend,
        "table_exists",
        lambda table, **_kwargs: table in {
            "security_fundamentals_pit",
            "security_classification_pit",
            "security_prices_eod",
            "barra_raw_cross_section_history",
        },
    )
    monkeypatch.setattr(
        core_reads.source_reads,
        "exposure_source_table_required",
        lambda **_kwargs: "barra_raw_cross_section_history",
    )

    def _fake_fetch(sql: str, params=None):
        if "security_fundamentals_pit" in sql:
            return [{"latest": "2026-03-02"}]
        if "security_classification_pit" in sql:
            return [{"latest": "2026-03-01"}]
        if "security_prices_eod" in sql:
            return [{"latest": "2026-03-03"}]
        if "barra_raw_cross_section_history" in sql:
            return [{"latest": "2026-03-04"}]
        return []

    monkeypatch.setattr(core_reads.core_backend, "fetch_rows", lambda sql, params=None, **_kwargs: _fake_fetch(sql, params))

    out = core_reads.load_source_dates()

    assert out == {
        "fundamentals_asof": "2026-03-02",
        "classification_asof": "2026-03-01",
        "prices_asof": "2026-03-03",
        "exposures_asof": "2026-03-04",
        "exposures_latest_available_asof": "2026-03-04",
    }


def test_load_source_dates_caps_pit_recency_to_latest_closed_anchor(monkeypatch) -> None:
    monkeypatch.setattr(core_reads.source_dates, "_pit_latest_closed_anchor", lambda: "2026-02-27")
    monkeypatch.setattr(
        core_reads.core_backend,
        "table_exists",
        lambda table, **_kwargs: table in {
            "security_fundamentals_pit",
            "security_classification_pit",
            "security_prices_eod",
            "barra_raw_cross_section_history",
        },
    )
    monkeypatch.setattr(
        core_reads.source_reads,
        "exposure_source_table_required",
        lambda **_kwargs: "barra_raw_cross_section_history",
    )

    def _fake_fetch(sql: str, params=None):
        if "security_fundamentals_pit" in sql and "<=" in sql:
            return [{"latest": "2026-02-27"}]
        if "security_classification_pit" in sql and "<=" in sql:
            return [{"latest": "2026-02-27"}]
        if "security_prices_eod" in sql:
            return [{"latest": "2026-03-13"}]
        if "barra_raw_cross_section_history" in sql:
            return [{"latest": "2026-03-13"}]
        return []

    monkeypatch.setattr(core_reads.core_backend, "fetch_rows", lambda sql, params=None, **_kwargs: _fake_fetch(sql, params))

    out = core_reads.load_source_dates()

    assert out == {
        "fundamentals_asof": "2026-02-27",
        "classification_asof": "2026-02-27",
        "prices_asof": "2026-03-13",
        "exposures_asof": "2026-03-13",
        "exposures_latest_available_asof": "2026-03-13",
    }
