from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.data.health_audit import run_sqlite_health_audit
from backend.scripts.demote_security_master_to_compat_view import demote_security_master
from backend.universe.schema import ensure_cuse4_schema
from backend.universe.selectors import load_registry_active_rows


def _relation_type(conn: sqlite3.Connection, name: str) -> str | None:
    row = conn.execute(
        """
        SELECT type
        FROM sqlite_master
        WHERE type IN ('table', 'view') AND name = ?
        LIMIT 1
        """,
        (name,),
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _seed_security_master_and_compat(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS barra_raw_cross_section_history (
            ric TEXT NOT NULL,
            as_of_date TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, job_run_id, updated_at
        ) VALUES (
            'AAPL.OQ', 'AAPL', 'US0378331005', 'NASDAQ', 1, 1,
            'native_equity', 'legacy_master', 'job_1', '2026-03-15T00:00:00Z'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, isin, exchange_name, classification_ok, is_equity_eligible,
            coverage_role, source, job_run_id, updated_at
        ) VALUES (
            'AAPL.OQ', 'AAPL', 'US0378331005', 'NASDAQ', 0, 0,
            'projection_only', 'compat_current', 'job_2', '2026-03-16T00:00:00Z'
        )
        """
    )
    conn.commit()
    conn.close()


def test_demote_security_master_to_compat_view_dry_run_reports_plan(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    _seed_security_master_and_compat(db_path)

    out = demote_security_master(db_path=db_path, apply=False)

    assert out["status"] == "dry_run"
    assert out["security_master_kind_before"] == "table"
    assert out["security_master_kind_after"] == "view"
    assert out["security_master_legacy_kind_after"] == "table"
    assert any("ALTER TABLE security_master RENAME TO security_master_legacy" in step for step in out["planned_actions"])


def test_demote_security_master_to_compat_view_apply_preserves_view_semantics(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    _seed_security_master_and_compat(db_path)

    out = demote_security_master(db_path=db_path, apply=True)

    assert out["status"] == "ok"
    assert out["security_master_kind_after"] == "view"
    assert out["security_master_legacy_kind_after"] == "table"
    assert out["security_master_row_count_after"] == 1
    assert out["security_master_legacy_row_count_after"] == 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        assert _relation_type(conn, "security_master") == "view"
        assert _relation_type(conn, "security_master_legacy") == "table"
        row = conn.execute("SELECT coverage_role, source FROM security_master WHERE ric = 'AAPL.OQ'").fetchone()
        assert row is not None
        assert row["coverage_role"] == "projection_only"
        assert row["source"] == "compat_current"

        ensure_cuse4_schema(conn)
        assert _relation_type(conn, "security_master") == "view"

        conn.execute("DROP TABLE security_registry")
        rows = load_registry_active_rows(conn)
        assert rows == [{"ticker": "AAPL", "ric": "AAPL.OQ"}]
    finally:
        conn.close()

    audit = run_sqlite_health_audit(db_path, include_integrity_pragmas=False)
    assert audit["checks"]["compat_row_counts"]["security_master"] == 1


def test_demote_security_master_to_compat_view_is_idempotent_once_view_exists(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    _seed_security_master_and_compat(db_path)

    first = demote_security_master(db_path=db_path, apply=True)
    second = demote_security_master(db_path=db_path, apply=True)

    assert first["status"] == "ok"
    assert second["status"] == "already_demoted"
    assert second["security_master_kind_before"] == "view"
