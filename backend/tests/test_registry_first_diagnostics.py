from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.data.health_audit import run_sqlite_health_audit
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
