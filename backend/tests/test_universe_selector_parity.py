from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from backend.universe.schema import ensure_cuse4_schema
from backend.universe.security_master_sync import (
    load_default_source_universe_rows,
    load_price_ingest_universe_rows,
    load_projection_only_universe_rows,
)
from backend.universe.selectors import (
    load_identifier_refresh_scope_rows,
    load_cpar_build_scope_rows,
    load_cuse_returns_projection_scope_rows,
    load_pit_ingest_scope_rows,
    load_price_ingest_scope_rows,
)
from backend.universe.runtime_rows import load_security_runtime_rows


def test_selector_parity_matches_legacy_wrappers(tmp_path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", "NASDAQ", 1, 1, "native_equity", "lseg_toolkit", now_iso),
            ("MSFT.OQ", "MSFT", "NASDAQ", 1, 1, "native_equity", "lseg_toolkit", now_iso),
            ("SPY.P", "SPY", "NYSE Arca", 0, 0, "projection_only", "security_master_seed", now_iso),
        ],
    )
    conn.commit()

    assert load_default_source_universe_rows(conn, include_pending_seed=False) == load_pit_ingest_scope_rows(
        conn,
        include_pending_seed=False,
    )
    assert load_projection_only_universe_rows(conn) == load_cuse_returns_projection_scope_rows(conn)
    assert load_price_ingest_universe_rows(conn, include_pending_seed=False) == load_price_ingest_scope_rows(
        conn,
        include_pending_seed=False,
    )
    conn.close()


def test_selectors_fall_back_per_row_when_policy_is_only_partially_populated(tmp_path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()

    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", "NASDAQ", 1, 1, "native_equity", "lseg_toolkit", now_iso),
            ("SPY.P", "SPY", "NYSE Arca", 0, 0, "projection_only", "security_master_seed", now_iso),
            ("MSFT.OQ", "MSFT", "NASDAQ", 1, 1, "native_equity", "lseg_toolkit", now_iso),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES (?, ?, 'active', ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", "security_registry_seed", now_iso),
            ("SPY.P", "SPY", "security_registry_seed", now_iso),
            ("MSFT.OQ", "MSFT", "security_registry_seed", now_iso),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES ('MSFT.OQ', 1, 1, 1, 1, 0, 1, 1, 'test', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    pit_rics = {row["ric"] for row in load_pit_ingest_scope_rows(conn, include_pending_seed=False)}
    projection_rics = {row["ric"] for row in load_cuse_returns_projection_scope_rows(conn)}
    cpar_rics = {row["ric"] for row in load_cpar_build_scope_rows(conn)}

    assert "AAPL.OQ" in pit_rics
    assert "MSFT.OQ" in pit_rics
    assert "SPY.P" not in pit_rics
    assert projection_rics == {"SPY.P"}
    assert cpar_rics == {"AAPL.OQ", "MSFT.OQ", "SPY.P"}
    conn.close()


def test_identifier_refresh_scope_does_not_fall_back_to_legacy_when_registry_table_is_present_but_empty(tmp_path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 'NASDAQ', 1, 1, 'native_equity', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    assert load_identifier_refresh_scope_rows(conn) == []
    conn.close()


def test_runtime_rows_do_not_fall_back_to_legacy_when_registry_table_is_present_but_empty(tmp_path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 'NASDAQ', 1, 1, 'native_equity', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    assert load_security_runtime_rows(conn) == []
    conn.close()


def test_pit_ingest_scope_does_not_use_legacy_equity_flag_to_mark_ready_registry_rows_pending(tmp_path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 'active', 'security_registry_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 0, 0, 'native_equity', 'security_master_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 1, 0, 0, 1, 1, ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, source, updated_at
        ) VALUES ('AAPL.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, 'taxonomy_refresh', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    rows = load_pit_ingest_scope_rows(conn, include_pending_seed=True)

    assert rows == [{"ticker": "AAPL", "ric": "AAPL.OQ"}]
    conn.close()


def test_price_ingest_scope_keeps_degraded_recent_registry_rows(tmp_path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
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
            ("FILL.N", "2026-03-24", 10.0, 1000.0, now_iso),
            ("AHL.N", "2026-03-20", 37.5, 0.0, now_iso),
            ("AHL.N", "2026-03-25", 37.5, 0.0, now_iso),
        ],
    )
    conn.commit()

    assert load_pit_ingest_scope_rows(conn, include_pending_seed=False, recent_sessions=8) == []
    assert load_price_ingest_scope_rows(conn, include_pending_seed=False, recent_sessions=8) == [
        {"ticker": "AHL", "ric": "AHL.N"}
    ]
    conn.close()
