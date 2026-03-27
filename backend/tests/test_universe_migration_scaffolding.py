from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.universe.bootstrap import bootstrap_cuse4_source_tables
from backend.universe.registry_sync import policy_defaults_for_legacy_coverage_role
from backend.universe.schema import ensure_cuse4_schema
from backend.universe.runtime_rows import load_security_runtime_rows
from backend.universe.security_master_sync import (
    load_default_source_universe_rows,
    load_price_ingest_universe_rows,
    load_projection_only_universe_rows,
    upsert_security_master_rows,
)
from backend.universe.source_observation import refresh_security_source_observation_daily
from backend.universe.taxonomy_builder import (
    materialize_security_master_compat_current,
    refresh_security_taxonomy_current,
)
from backend.universe.selectors import (
    load_cuse_returns_projection_scope_rows,
    load_pit_ingest_scope_rows,
    load_price_ingest_scope_rows,
)


def _fetchone(conn: sqlite3.Connection, sql: str, params: tuple[object, ...] = ()) -> sqlite3.Row:
    conn.row_factory = sqlite3.Row
    row = conn.execute(sql, params).fetchone()
    assert row is not None
    return row


def test_bootstrap_populates_registry_policy_and_compat_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    seed_path = tmp_path / "security_master_seed.csv"
    seed_path.write_text(
        "\n".join(
            [
                "ric,ticker,isin,exchange_name,coverage_role",
                "AAPL.OQ,AAPL,US0378331005,NASDAQ,native_equity",
                "SPY.P,SPY,US78462F1030,NYSE Arca,projection_only",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out = bootstrap_cuse4_source_tables(db_path=db_path, seed_path=seed_path)

    assert out["status"] == "ok"
    assert out["security_registry_rows"] == 2
    assert out["security_policy_current_rows"] == 2
    assert out["security_taxonomy_current_rows"] == 2
    assert out["security_source_observation_daily_rows"] == 2
    assert out["security_master_compat_current_rows"] == 2

    conn = sqlite3.connect(str(db_path))
    try:
        spy_policy = _fetchone(
            conn,
            """
            SELECT
                price_ingest_enabled,
                pit_fundamentals_enabled,
                pit_classification_enabled,
                allow_cuse_native_core,
                allow_cuse_returns_projection,
                allow_cpar_extended_target
            FROM security_policy_current
            WHERE ric = 'SPY.P'
            """,
        )
        assert tuple(spy_policy) == (1, 0, 0, 0, 1, 1)

        aapl_policy = _fetchone(
            conn,
            """
            SELECT
                price_ingest_enabled,
                pit_fundamentals_enabled,
                pit_classification_enabled,
                allow_cuse_native_core,
                allow_cuse_returns_projection,
                allow_cpar_core_target
            FROM security_policy_current
            WHERE ric = 'AAPL.OQ'
            """,
        )
        assert tuple(aapl_policy) == (1, 1, 1, 1, 0, 1)

        compat = _fetchone(
            conn,
            "SELECT coverage_role, classification_ok, is_equity_eligible FROM security_master_compat_current WHERE ric = 'SPY.P'",
        )
        assert tuple(compat) == ("projection_only", 0, 0)
    finally:
        conn.close()


def test_runtime_rows_use_shared_legacy_policy_defaults_when_only_compat_rows_exist(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", 1, 1, "native_equity", "compat", "2026-03-26T00:00:00Z"),
            ("SPY.P", "SPY", 0, 0, "projection_only", "compat", "2026-03-26T00:00:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", "active", "registry", "2026-03-26T00:00:00Z"),
            ("SPY.P", "SPY", "active", "registry", "2026-03-26T00:00:00Z"),
        ],
    )
    conn.commit()

    rows = {row["ric"]: row for row in load_security_runtime_rows(conn)}

    native_defaults = policy_defaults_for_legacy_coverage_role("native_equity")
    projection_defaults = policy_defaults_for_legacy_coverage_role("projection_only")

    assert rows["AAPL.OQ"]["price_ingest_enabled"] == native_defaults["price_ingest_enabled"]
    assert rows["AAPL.OQ"]["pit_fundamentals_enabled"] == native_defaults["pit_fundamentals_enabled"]
    assert rows["AAPL.OQ"]["pit_classification_enabled"] == native_defaults["pit_classification_enabled"]
    assert rows["AAPL.OQ"]["allow_cuse_native_core"] == native_defaults["allow_cuse_native_core"]
    assert rows["SPY.P"]["price_ingest_enabled"] == projection_defaults["price_ingest_enabled"]
    assert rows["SPY.P"]["pit_fundamentals_enabled"] == projection_defaults["pit_fundamentals_enabled"]
    assert rows["SPY.P"]["pit_classification_enabled"] == projection_defaults["pit_classification_enabled"]
    assert rows["SPY.P"]["allow_cuse_returns_projection"] == projection_defaults["allow_cuse_returns_projection"]
    conn.close()


def test_upsert_security_master_rows_backfills_registry_taxonomy_source_observation_and_compat(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('SPY.P', 'SPY', 0, 0, 'projection_only', 'security_master_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES ('SPY.P', 1, 0, 0, 0, 0, 1, 0, 1, 'registry_seed_defaults', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, hq_country_code, source, updated_at
        ) VALUES ('SPY.P', '2026-03-10', 'US', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_source_observation_daily (
            as_of_date, ric, classification_ready, is_equity_eligible, price_ingest_enabled,
            pit_fundamentals_enabled, pit_classification_enabled, has_price_history_as_of_date,
            has_fundamentals_history_as_of_date, has_classification_history_as_of_date,
            latest_price_date, latest_fundamentals_as_of_date, latest_classification_as_of_date,
            source, job_run_id, updated_at
        ) VALUES (
            '2026-03-15', 'SPY.P', 1, 0, 1, 0, 0, 1, 0, 1,
            '2026-03-14', NULL, '2026-03-10', 'source_observation', 'job_obs', ?
        )
        """,
        (now_iso,),
    )
    conn.commit()

    rows_upserted = upsert_security_master_rows(
        conn,
        [
            {
                "ric": "SPY.P",
                "ticker": "SPY",
                "classification_ok": 1,
                "is_equity_eligible": 1,
                "exchange_name": "NYSE Arca",
                "source": "lseg_toolkit",
                "job_run_id": "job_spy",
                "updated_at": now_iso,
            },
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "classification_ok": 1,
                "is_equity_eligible": 1,
                "exchange_name": "NASDAQ",
                "source": "lseg_toolkit",
                "job_run_id": "job_aapl",
                "updated_at": now_iso,
            },
        ],
    )
    conn.commit()

    assert rows_upserted == 2

    registry_count = conn.execute("SELECT COUNT(*) FROM security_registry").fetchone()[0]
    assert int(registry_count or 0) == 2

    spy_compat = _fetchone(
        conn,
        """
        SELECT coverage_role, classification_ok, is_equity_eligible
        FROM security_master_compat_current
        WHERE ric = 'SPY.P'
        """,
    )
    assert tuple(spy_compat) == ("projection_only", 1, 0)

    spy_taxonomy = _fetchone(
        conn,
        """
        SELECT instrument_kind, vehicle_structure, is_single_name_equity, classification_ready
        FROM security_taxonomy_current
        WHERE ric = 'SPY.P'
        """,
    )
    assert tuple(spy_taxonomy) == ("fund_vehicle", "projection_only_vehicle", 0, 1)

    obs = _fetchone(
        conn,
        """
        SELECT price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled
        FROM security_source_observation_daily
        WHERE ric = 'SPY.P'
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
    )
    assert tuple(obs) == (1, 0, 0)
    conn.close()


def test_source_observation_uses_only_data_known_by_as_of_date(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity', 'compat', ?)
        """,
        (now_iso,),
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "2026-03-14", 100.0, 1000.0, "USD", "lseg_toolkit", now_iso),
            ("AAPL.OQ", "2026-03-20", 105.0, 1100.0, "USD", "lseg_toolkit", now_iso),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_classification_pit (ric, as_of_date, hq_country_code, source, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "2026-03-10", "US", "lseg_toolkit", now_iso),
            ("AAPL.OQ", "2026-03-21", "US", "lseg_toolkit", now_iso),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, source, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "2026-03-12", "2025-12-31", "lseg_toolkit", now_iso),
            ("AAPL.OQ", "2026-03-22", "2026-03-31", "lseg_toolkit", now_iso),
        ],
    )
    conn.commit()

    refreshed = refresh_security_source_observation_daily(
        conn,
        as_of_date="2026-03-15",
        rics=["AAPL.OQ"],
    )
    conn.commit()

    assert refreshed == 1
    obs = _fetchone(
        conn,
        """
        SELECT
            has_price_history_as_of_date,
            has_fundamentals_history_as_of_date,
            has_classification_history_as_of_date,
            latest_price_date,
            latest_fundamentals_as_of_date,
            latest_classification_as_of_date,
            classification_ready
        FROM security_source_observation_daily
        WHERE as_of_date = '2026-03-15' AND ric = 'AAPL.OQ'
        """,
    )
    assert tuple(obs) == (1, 1, 1, "2026-03-14", "2026-03-12", "2026-03-10", 1)
    conn.close()


def test_source_observation_requires_populated_registry_before_switching_authority(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity', 'compat', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-14', 100.0, 1000.0, 'USD', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (ric, as_of_date, hq_country_code, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-10', 'US', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-12', '2025-12-31', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    refreshed = refresh_security_source_observation_daily(conn, as_of_date="2026-03-15", rics=["AAPL.OQ"])
    conn.commit()

    assert refreshed == 1
    obs = _fetchone(
        conn,
        """
        SELECT classification_ready, is_equity_eligible
        FROM security_source_observation_daily
        WHERE as_of_date = '2026-03-15' AND ric = 'AAPL.OQ'
        """,
    )
    assert tuple(obs) == (1, 1)
    conn.close()


def test_source_observation_can_materialize_registry_first_surfaces_without_security_master(
    tmp_path: Path,
) -> None:
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
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-14', 100.0, 1000.0, 'USD', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-12', '2025-12-31', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, hq_country_code, source, updated_at
        ) VALUES ('AAPL.OQ', '2026-03-10', 'US', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    refreshed = refresh_security_source_observation_daily(conn, as_of_date="2026-03-15", rics=["AAPL.OQ"])
    conn.commit()

    assert refreshed == 1
    obs = _fetchone(
        conn,
        """
        SELECT classification_ready, is_equity_eligible, price_ingest_enabled,
               pit_fundamentals_enabled, pit_classification_enabled
        FROM security_source_observation_daily
        WHERE as_of_date = '2026-03-15' AND ric = 'AAPL.OQ'
        """,
    )
    assert tuple(obs) == (1, 1, 1, 1, 1)
    conn.close()


def test_source_observation_does_not_recreate_equity_flags_from_taxonomy_without_source_classification(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES ('SPY.P', 'SPY', 'active', 'security_registry_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, is_single_name_equity, classification_ready, source, updated_at
        ) VALUES ('SPY.P', 'single_name_equity', 'equity_security', 1, 1, 'taxonomy_refresh', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES ('SPY.P', 1, 1, 1, 1, 0, 0, 1, 1, 'registry_seed_defaults', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES ('SPY.P', '2026-03-14', 500.0, 1000.0, 'USD', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    refreshed = refresh_security_source_observation_daily(conn, as_of_date="2026-03-15", rics=["SPY.P"])
    conn.commit()

    assert refreshed == 1
    obs = _fetchone(
        conn,
        """
        SELECT classification_ready, is_equity_eligible, pit_fundamentals_enabled, pit_classification_enabled
        FROM security_source_observation_daily
        WHERE as_of_date = '2026-03-15' AND ric = 'SPY.P'
        """,
    )
    assert tuple(obs) == (0, 0, 1, 1)
    conn.close()


def test_source_observation_derives_registry_first_flags_from_source_classification_when_taxonomy_is_missing(
    tmp_path: Path,
) -> None:
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
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 1, 0, 0, 1, 1, 'registry_seed_defaults', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-14', 100.0, 1000.0, 'USD', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-12', '2025-12-31', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, hq_country_code, source, updated_at
        ) VALUES ('AAPL.OQ', '2026-03-10', 'US', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    refreshed = refresh_security_source_observation_daily(conn, as_of_date="2026-03-15", rics=["AAPL.OQ"])
    conn.commit()

    assert refreshed == 1
    obs = _fetchone(
        conn,
        """
        SELECT classification_ready, is_equity_eligible, price_ingest_enabled,
               pit_fundamentals_enabled, pit_classification_enabled
        FROM security_source_observation_daily
        WHERE as_of_date = '2026-03-15' AND ric = 'AAPL.OQ'
        """,
    )
    assert tuple(obs) == (1, 1, 1, 1, 1)
    conn.close()


def test_taxonomy_refresh_keeps_classified_non_equity_non_equity_even_with_core_policy_flags(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES ('SPY.P', 'SPY', 'active', 'security_registry_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES ('SPY.P', 1, 1, 1, 1, 0, 0, 1, 1, 'manual_override', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, hq_country_code, source, updated_at
        ) VALUES ('SPY.P', '2026-03-10', 'Exchange Traded Fund', 'US', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    obs_refreshed = refresh_security_source_observation_daily(conn, as_of_date="2026-03-15", rics=["SPY.P"])
    tax_refreshed = refresh_security_taxonomy_current(conn, rics=["SPY.P"])
    conn.commit()

    assert obs_refreshed == 1
    assert tax_refreshed == 1

    obs = _fetchone(
        conn,
        """
        SELECT classification_ready, is_equity_eligible
        FROM security_source_observation_daily
        WHERE as_of_date = '2026-03-15' AND ric = 'SPY.P'
        """,
    )
    assert tuple(obs) == (1, 0)

    tax = _fetchone(
        conn,
        """
        SELECT instrument_kind, vehicle_structure, is_single_name_equity, classification_ready
        FROM security_taxonomy_current
        WHERE ric = 'SPY.P'
        """,
    )
    assert tuple(tax) == ("fund_vehicle", "classified_non_equity", 0, 1)
    conn.close()


def test_source_observation_can_materialize_registry_first_surfaces_without_security_master(
    tmp_path: Path,
) -> None:
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
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 1, 0, 0, 1, 1, 'registry_seed_defaults', ?)
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
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, volume, currency, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-14', 100.0, 1000.0, 'USD', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-12', '2025-12-31', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, hq_country_code, source, updated_at
        ) VALUES ('AAPL.OQ', '2026-03-10', 'US', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    refreshed = refresh_security_source_observation_daily(conn, as_of_date="2026-03-15", rics=["AAPL.OQ"])
    conn.commit()

    assert refreshed == 1
    obs = _fetchone(
        conn,
        """
        SELECT
            classification_ready,
            is_equity_eligible,
            price_ingest_enabled,
            pit_fundamentals_enabled,
            pit_classification_enabled,
            has_price_history_as_of_date,
            has_fundamentals_history_as_of_date,
            has_classification_history_as_of_date
        FROM security_source_observation_daily
        WHERE as_of_date = '2026-03-15' AND ric = 'AAPL.OQ'
        """,
    )
    assert tuple(obs) == (1, 1, 1, 1, 1, 1, 1, 1)
    conn.close()


def test_taxonomy_refresh_uses_latest_source_observation_per_ric(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES ('SPY.P', 'SPY', 'active', 'security_registry_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('SPY.P', 1, 0, 0, 0, 0, 1, 0, 1, ?)
        """,
        (now_iso,),
    )
    conn.executemany(
        """
        INSERT INTO security_source_observation_daily (
            as_of_date, ric, classification_ready, is_equity_eligible, price_ingest_enabled,
            pit_fundamentals_enabled, pit_classification_enabled, has_price_history_as_of_date,
            has_fundamentals_history_as_of_date, has_classification_history_as_of_date,
            latest_price_date, latest_fundamentals_as_of_date, latest_classification_as_of_date,
            source, job_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "2026-03-14",
                "SPY.P",
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                "2026-03-14",
                "2026-03-12",
                "2026-03-10",
                "source_observation",
                "job_old",
                "2026-03-14T00:00:00Z",
            ),
            (
                "2026-03-15",
                "SPY.P",
                0,
                0,
                1,
                0,
                0,
                1,
                0,
                0,
                "2026-03-15",
                None,
                None,
                "source_observation",
                "job_new",
                "2026-03-15T00:00:00Z",
            ),
        ],
    )
    conn.commit()

    refreshed = refresh_security_taxonomy_current(conn, rics=["SPY.P"])
    conn.commit()

    assert refreshed == 1
    taxonomy = _fetchone(
        conn,
        """
        SELECT instrument_kind, vehicle_structure, is_single_name_equity, classification_ready
        FROM security_taxonomy_current
        WHERE ric = 'SPY.P'
        """,
    )
    assert tuple(taxonomy) == ("fund_vehicle", "projection_only_vehicle", 0, 0)
    conn.close()


def test_runtime_rows_keep_compat_visible_when_registry_companions_are_incomplete(tmp_path: Path) -> None:
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
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", 1, 1, "native_equity", "lseg_toolkit", now_iso),
            ("ORPH.X", "ORPH", 1, 1, "native_equity", "lseg_toolkit", now_iso),
        ],
    )
    conn.commit()

    rows = load_security_runtime_rows(conn)

    assert [row["ric"] for row in rows] == ["AAPL.OQ", "ORPH.X"]
    conn.close()


def test_runtime_rows_use_historical_structural_snapshot_for_as_of_reads(tmp_path: Path) -> None:
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
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity', 'compat', 'job_compat', '2026-03-15T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 0, 1, 0, 0, 1, '2026-04-01T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, issuer_country_code, listing_country_code,
            model_home_market_scope, is_single_name_equity, classification_ready, source, job_run_id, updated_at
        ) VALUES (
            'AAPL.OQ', 'fund_vehicle', 'classified_non_equity', 'CA', NULL,
            'ex_us', 0, 1, 'taxonomy', 'job_tax', '2026-04-01T00:00:00Z'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_source_observation_daily (
            as_of_date, ric, classification_ready, is_equity_eligible, price_ingest_enabled,
            pit_fundamentals_enabled, pit_classification_enabled, has_price_history_as_of_date,
            has_fundamentals_history_as_of_date, has_classification_history_as_of_date,
            latest_price_date, latest_fundamentals_as_of_date, latest_classification_as_of_date,
            source, job_run_id, updated_at
        ) VALUES (
            '2026-03-15', 'AAPL.OQ', 1, 1, 1, 1, 1, 1, 1, 1,
            '2026-03-14', '2026-03-12', '2026-03-12', 'source_observation', 'job_obs', '2026-03-15T00:00:00Z'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, hq_country_code, source, updated_at
        ) VALUES (
            'AAPL.OQ', '2026-03-12', 'Technology', 'US', 'classification', '2026-03-12T00:00:00Z'
        )
        """
    )
    conn.commit()

    rows = load_security_runtime_rows(conn, as_of_date="2026-03-15")

    assert len(rows) == 1
    assert rows[0]["instrument_kind"] == "single_name_equity"
    assert rows[0]["is_single_name_equity"] == 1
    assert rows[0]["issuer_country_code"] == "US"
    assert rows[0]["model_home_market_scope"] == "us"
    assert rows[0]["allow_cuse_native_core"] == 1
    assert rows[0]["allow_cuse_fundamental_projection"] == 0
    conn.close()


def test_runtime_rows_historical_as_of_can_derive_readiness_from_classification_snapshot_without_observation(
    tmp_path: Path,
) -> None:
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
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 0, 0, 'native_equity', 'compat', 'job_compat', '2026-03-15T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 0, 1, 0, 0, 1, '2026-04-01T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, hq_country_code, source, updated_at
        ) VALUES (
            'AAPL.OQ', '2026-03-12', 'Technology', 'US', 'classification', '2026-03-12T00:00:00Z'
        )
        """
    )
    conn.commit()

    rows = load_security_runtime_rows(conn, as_of_date="2026-03-15")

    assert len(rows) == 1
    assert rows[0]["classification_ready"] == 1
    assert rows[0]["is_single_name_equity"] == 1
    assert rows[0]["allow_cuse_native_core"] == 1
    conn.close()


def test_runtime_rows_historical_as_of_prefers_classification_snapshot_over_conflicting_observation(
    tmp_path: Path,
) -> None:
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
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 0, 0, 'native_equity', 'compat', 'job_compat', '2026-03-15T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('AAPL.OQ', 1, 1, 1, 0, 1, 0, 0, 1, '2026-04-01T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_source_observation_daily (
            as_of_date, ric, classification_ready, is_equity_eligible, price_ingest_enabled,
            pit_fundamentals_enabled, pit_classification_enabled, has_price_history_as_of_date,
            has_fundamentals_history_as_of_date, has_classification_history_as_of_date,
            latest_price_date, latest_fundamentals_as_of_date, latest_classification_as_of_date,
            source, job_run_id, updated_at
        ) VALUES (
            '2026-03-15', 'AAPL.OQ', 0, 0, 1, 1, 1, 1, 1, 1,
            '2026-03-14', '2026-03-12', '2026-03-12', 'source_observation', 'job_obs', '2026-03-15T00:00:00Z'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, hq_country_code, source, updated_at
        ) VALUES (
            'AAPL.OQ', '2026-03-12', 'Technology', 'US', 'classification', '2026-03-12T00:00:00Z'
        )
        """
    )
    conn.commit()

    rows = load_security_runtime_rows(conn, as_of_date="2026-03-15")

    assert len(rows) == 1
    assert rows[0]["classification_ready"] == 1
    assert rows[0]["is_single_name_equity"] == 1
    assert rows[0]["allow_cuse_native_core"] == 1
    conn.close()


def test_runtime_rows_fall_back_per_row_to_security_master_when_compat_is_partial(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()

    conn.execute("DROP TABLE security_registry")
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", 1, 1, "native_equity", "legacy_master", now_iso),
            ("SPY.P", "SPY", 0, 0, "projection_only", "legacy_master", now_iso),
        ],
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('SPY.P', 'SPY', 0, 0, 'projection_only', 'compat', 'job_spy', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    rows = load_security_runtime_rows(conn)

    assert [row["ric"] for row in rows] == ["AAPL.OQ", "SPY.P"]
    conn.close()


def test_taxonomy_refresh_can_classify_registry_rows_without_security_master(tmp_path: Path) -> None:
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
        INSERT INTO security_source_observation_daily (
            as_of_date, ric, classification_ready, is_equity_eligible, price_ingest_enabled,
            pit_fundamentals_enabled, pit_classification_enabled, has_price_history_as_of_date,
            has_fundamentals_history_as_of_date, has_classification_history_as_of_date,
            latest_price_date, latest_fundamentals_as_of_date, latest_classification_as_of_date,
            source, job_run_id, updated_at
        ) VALUES (
            '2026-03-15', 'AAPL.OQ', 1, 1, 1, 1, 1, 1, 1, 1,
            '2026-03-14', '2026-03-12', '2026-03-10', 'source_observation', 'job_obs', ?
        )
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, hq_country_code, source, updated_at
        ) VALUES ('AAPL.OQ', '2026-03-10', 'US', 'lseg_toolkit', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    refreshed = refresh_security_taxonomy_current(conn, rics=["AAPL.OQ"])
    conn.commit()

    assert refreshed == 1
    row = _fetchone(
        conn,
        """
        SELECT instrument_kind, vehicle_structure, is_single_name_equity, classification_ready, model_home_market_scope
        FROM security_taxonomy_current
        WHERE ric = 'AAPL.OQ'
        """,
    )
    assert tuple(row) == ("single_name_equity", "equity_security", 1, 1, "us")
    conn.close()


def test_compat_materialization_derives_from_registry_first_surfaces(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, isin, exchange_name, tracking_status, source, updated_at
        ) VALUES ('SPY.P', 'SPY', 'USSPY', 'NYSE Arca', 'active', 'security_registry_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, updated_at
        ) VALUES ('SPY.P', 1, 0, 0, 0, 0, 1, 0, 1, ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, source, updated_at
        ) VALUES ('SPY.P', 'fund_vehicle', 'projection_only_vehicle', 'us', 0, 0, 'taxonomy_refresh', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_source_observation_daily (
            as_of_date, ric, classification_ready, is_equity_eligible, price_ingest_enabled,
            pit_fundamentals_enabled, pit_classification_enabled, has_price_history_as_of_date,
            has_fundamentals_history_as_of_date, has_classification_history_as_of_date,
            latest_price_date, latest_fundamentals_as_of_date, latest_classification_as_of_date,
            source, job_run_id, updated_at
        ) VALUES ('2026-03-15', 'SPY.P', 0, 0, 1, 0, 0, 1, 0, 0, '2026-03-14', NULL, NULL, 'source_observation', 'job_obs', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    refreshed = materialize_security_master_compat_current(conn, rics=["SPY.P"])
    conn.commit()

    assert refreshed == 1
    row = _fetchone(
        conn,
        """
        SELECT ticker, classification_ok, is_equity_eligible, coverage_role
        FROM security_master_compat_current
        WHERE ric = 'SPY.P'
        """,
    )
    assert tuple(row) == ("SPY", 0, 0, "projection_only")
    conn.close()


def test_upsert_security_master_rows_rolls_back_cleanly_when_follow_on_materialization_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)

    import backend.universe.security_master_sync as security_master_sync

    def _boom(*args, **kwargs):
        raise RuntimeError("synthetic materialization failure")

    monkeypatch.setattr(security_master_sync, "refresh_security_source_observation_daily", _boom)

    with pytest.raises(RuntimeError, match="synthetic materialization failure"):
        security_master_sync.upsert_security_master_rows(
            conn,
            [
                {
                    "ric": "AAPL.OQ",
                    "ticker": "AAPL",
                    "classification_ok": 1,
                    "is_equity_eligible": 1,
                    "source": "lseg_toolkit",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            ],
        )

    conn.rollback()
    assert conn.execute("SELECT COUNT(*) FROM security_master").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM security_registry").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM security_taxonomy_current").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM security_master_compat_current").fetchone()[0] == 0
    conn.close()


def test_legacy_selector_wrappers_match_named_selectors(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", "NASDAQ", 1, 1, "native_equity", "lseg_toolkit", now_iso),
            ("MSFT.OQ", "MSFT", "NASDAQ", 1, 1, "native_equity", "lseg_toolkit", now_iso),
            ("SPY.P", "SPY", "NYSE Arca", 0, 0, "projection_only", "security_master_seed", now_iso),
            ("PEND.OQ", "PEND", "NASDAQ", 0, 0, "native_equity", "security_master_seed", now_iso),
        ],
    )
    conn.commit()

    assert load_default_source_universe_rows(conn, include_pending_seed=True) == load_pit_ingest_scope_rows(
        conn,
        include_pending_seed=True,
    )
    assert load_projection_only_universe_rows(conn) == load_cuse_returns_projection_scope_rows(conn)
    assert load_price_ingest_universe_rows(conn, include_pending_seed=True) == load_price_ingest_scope_rows(
        conn,
        include_pending_seed=True,
    )
    conn.close()


def test_runtime_rows_reclassify_seed_default_native_equity_to_ex_us_projection(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES ('ASML.AS', 'ASML', 'active', 'security_registry_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES ('ASML.AS', 1, 1, 1, 1, 0, 0, 1, 1, 'registry_seed_defaults', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, issuer_country_code, listing_country_code,
            model_home_market_scope, is_single_name_equity, classification_ready, source, updated_at
        ) VALUES (
            'ASML.AS', 'single_name_equity', 'equity_security', 'NL', 'NL',
            'ex_us', 1, 1, 'taxonomy_refresh', ?
        )
        """,
        (now_iso,),
    )
    conn.commit()

    rows = load_security_runtime_rows(conn)

    assert len(rows) == 1
    row = rows[0]
    assert row["allow_cuse_native_core"] == 0
    assert row["allow_cuse_fundamental_projection"] == 1
    assert row["allow_cpar_core_target"] == 0
    assert row["allow_cpar_extended_target"] == 1
    conn.close()


def test_runtime_rows_keep_manual_override_over_structural_derivation(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES ('ASML.AS', 'ASML', 'active', 'security_registry_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES ('ASML.AS', 1, 1, 1, 1, 0, 0, 1, 1, 'manual_override', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, issuer_country_code, listing_country_code,
            model_home_market_scope, is_single_name_equity, classification_ready, source, updated_at
        ) VALUES (
            'ASML.AS', 'single_name_equity', 'equity_security', 'NL', 'NL',
            'ex_us', 1, 1, 'taxonomy_refresh', ?
        )
        """,
        (now_iso,),
    )
    conn.commit()

    rows = load_security_runtime_rows(conn)

    assert len(rows) == 1
    row = rows[0]
    assert row["allow_cuse_native_core"] == 1
    assert row["allow_cuse_fundamental_projection"] == 0
    assert row["allow_cpar_core_target"] == 1
    conn.close()


def test_runtime_rows_historical_as_of_does_not_borrow_current_taxonomy_when_snapshot_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES ('TEST.X', 'TEST', 'active', 'security_registry_seed', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES ('TEST.X', 1, 1, 1, 1, 0, 0, 1, 1, 'registry_seed_defaults', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('TEST.X', 'TEST', 0, 0, 'native_equity', 'compat', 'job_compat', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, issuer_country_code, listing_country_code,
            model_home_market_scope, is_single_name_equity, classification_ready, source, updated_at
        ) VALUES (
            'TEST.X', 'single_name_equity', 'equity_security', 'US', NULL,
            'us', 1, 1, 'taxonomy_refresh', ?
        )
        """,
        (now_iso,),
    )
    conn.commit()

    rows = load_security_runtime_rows(conn, as_of_date="2026-03-15")

    assert len(rows) == 1
    row = rows[0]
    assert row["instrument_kind"] == "other"
    assert row["model_home_market_scope"] == "unknown"
    assert row["is_single_name_equity"] == 0
    conn.close()
