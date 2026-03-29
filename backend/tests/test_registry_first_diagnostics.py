from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.data.health_audit import run_sqlite_health_audit
from backend.data.cross_section_snapshot import ensure_cross_section_snapshot_table
from backend.risk_model.raw_cross_section_history import ensure_raw_cross_section_history_table
from backend.services.data_diagnostics_sections import load_source_tables
from backend.universe.schema import ensure_cuse4_schema


def test_health_audit_prefers_registry_anchor_and_reports_registry_first_counts(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, source, updated_at)
        VALUES ('AAPL.OQ', 'AAPL', 'active', 'registry_seed', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity', 'compat', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-25', 100.0, 'lseg', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-25', '2025-12-31', 'lseg', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (ric, as_of_date, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-25', 'lseg', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        CREATE TABLE barra_raw_cross_section_history (
            ric TEXT,
            ticker TEXT,
            as_of_date TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO barra_raw_cross_section_history (ric, ticker, as_of_date)
        VALUES ('AAPL.OQ', 'AAPL', '2026-03-25')
        """
    )
    conn.commit()
    conn.close()

    out = run_sqlite_health_audit(db_path, include_integrity_pragmas=False)

    row_counts = out["checks"]["row_counts"]
    assert row_counts["security_registry"] == 1
    assert row_counts["security_master_compat_current"] == 1
    assert "security_master" not in row_counts
    assert out["checks"]["compat_row_counts"]["security_master"] == 0
    assert out["checks"]["orphan_anchor"] == "security_registry"
    assert out["checks"]["orphan_ric_rows"]["security_prices_eod"] == 0


def test_data_diagnostics_source_tables_are_registry_first(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, source, updated_at)
        VALUES ('AAPL.OQ', 'AAPL', 'active', 'registry_seed', '2026-03-26T00:00:00Z')
        """
    )
    conn.commit()

    out = load_source_tables(conn)

    assert "security_registry" in out
    assert "security_policy_current" in out
    assert "security_taxonomy_current" in out
    assert "security_source_observation_daily" in out
    assert "security_master_compat_current" in out
    assert "security_master" not in out
    assert out["security_registry"] is not None
    conn.close()


def test_health_audit_skips_empty_registry_and_anchors_to_populated_compat(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity', 'compat', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_prices_eod (ric, date, close, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-25', 100.0, 'lseg', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (ric, as_of_date, stat_date, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-25', '2025-12-31', 'lseg', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (ric, as_of_date, source, updated_at)
        VALUES ('AAPL.OQ', '2026-03-25', 'lseg', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        CREATE TABLE barra_raw_cross_section_history (
            ric TEXT,
            ticker TEXT,
            as_of_date TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO barra_raw_cross_section_history (ric, ticker, as_of_date)
        VALUES ('AAPL.OQ', 'AAPL', '2026-03-25')
        """
    )
    conn.commit()
    conn.close()

    out = run_sqlite_health_audit(db_path, include_integrity_pragmas=False)

    assert out["checks"]["orphan_anchor"] == "security_master_compat_current"


def test_health_audit_latest_coverage_prefers_snapshot_identity_over_runtime_flags(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    ensure_cross_section_snapshot_table(conn)
    ensure_raw_cross_section_history_table(conn)
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, source, updated_at)
        VALUES ('MISS.OQ', 'MISS', 'active', 'registry_seed', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cpar_core_target, allow_cpar_extended_target,
            policy_source, updated_at
        ) VALUES ('MISS.OQ', 1, 1, 1, 1, 1, 1, 'default', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, source, updated_at
        ) VALUES ('MISS.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, 'taxonomy_refresh', '2026-03-26T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO universe_cross_section_snapshot (ric, ticker, as_of_date, updated_at)
        VALUES ('KEEP.OQ', 'KEEP', '2026-03-25', '2026-03-26T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    out = run_sqlite_health_audit(db_path, include_integrity_pragmas=False)

    assert out["checks"]["latest_coverage"]["cuse_native_core_runtime"] == 1
