"""Verify projection-only RICs are excluded from core model paths."""

from __future__ import annotations

import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.universe.schema import ensure_cuse4_schema
from backend.universe.security_master_sync import (
    load_default_source_universe_rows,
    load_projection_only_universe_rows,
    load_price_ingest_universe_rows,
    sync_security_master_seed,
    upsert_security_master_rows,
)


@pytest.fixture
def db_with_mixed_universe(tmp_path):
    """Create a data.db with both native equity and projection-only instruments."""
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)

    now_iso = datetime.now(timezone.utc).isoformat()

    # Insert native equity
    conn.execute(
        """
        INSERT INTO security_master (ric, ticker, classification_ok, is_equity_eligible,
                                     coverage_role, source, updated_at)
        VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity', 'test', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_master (ric, ticker, classification_ok, is_equity_eligible,
                                     coverage_role, source, updated_at)
        VALUES ('MSFT.OQ', 'MSFT', 1, 1, 'native_equity', 'test', ?)
        """,
        (now_iso,),
    )

    # Insert projection-only ETF
    conn.execute(
        """
        INSERT INTO security_master (ric, ticker, classification_ok, is_equity_eligible,
                                     coverage_role, source, updated_at)
        VALUES ('SPY.P', 'SPY', 0, 0, 'projection_only', 'test', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_master (ric, ticker, classification_ok, is_equity_eligible,
                                     coverage_role, source, updated_at)
        VALUES ('XLE.P', 'XLE', 0, 0, 'projection_only', 'test', ?)
        """,
        (now_iso,),
    )

    conn.commit()
    yield conn
    conn.close()


class TestDefaultSourceUniverseExcludesProjectionOnly:
    def test_excludes_projection_only(self, db_with_mixed_universe):
        """load_default_source_universe_rows should NOT include projection-only instruments."""
        conn = db_with_mixed_universe
        rows = load_default_source_universe_rows(conn, include_pending_seed=False)
        tickers = {r["ticker"] for r in rows}
        rics = {r["ric"] for r in rows}

        assert "AAPL" in tickers
        assert "MSFT" in tickers
        assert "SPY" not in tickers
        assert "XLE" not in tickers
        assert "SPY.P" not in rics
        assert "XLE.P" not in rics


class TestProjectionOnlyUniverseRows:
    def test_loads_projection_only(self, db_with_mixed_universe):
        """load_projection_only_universe_rows should return only projection-only instruments."""
        conn = db_with_mixed_universe
        rows = load_projection_only_universe_rows(conn)
        tickers = {r["ticker"] for r in rows}

        assert "SPY" in tickers
        assert "XLE" in tickers
        assert "AAPL" not in tickers
        assert "MSFT" not in tickers

    def test_returns_ric_and_ticker(self, db_with_mixed_universe):
        """Each row should have both ric and ticker."""
        conn = db_with_mixed_universe
        rows = load_projection_only_universe_rows(conn)
        for row in rows:
            assert row["ric"]
            assert row["ticker"]


class TestPriceIngestUniverseRows:
    def test_includes_both_universes(self, db_with_mixed_universe):
        """load_price_ingest_universe_rows should include native + projection-only."""
        conn = db_with_mixed_universe
        rows = load_price_ingest_universe_rows(conn, include_pending_seed=False)
        tickers = {r["ticker"] for r in rows}

        assert "AAPL" in tickers
        assert "MSFT" in tickers
        assert "SPY" in tickers
        assert "XLE" in tickers

    def test_no_duplicates(self, db_with_mixed_universe):
        """Should not return duplicate RICs."""
        conn = db_with_mixed_universe
        rows = load_price_ingest_universe_rows(conn, include_pending_seed=False)
        rics = [r["ric"] for r in rows]
        assert len(rics) == len(set(rics))


def test_lseg_upsert_preserves_projection_only_coverage_role(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible,
            coverage_role, source, updated_at
        ) VALUES ('SPY.P', 'SPY', 0, 0, 'projection_only', 'security_master_seed', ?)
        """,
        (now_iso,),
    )
    conn.commit()

    upsert_security_master_rows(
        conn,
        [
            {
                "ric": "SPY.P",
                "ticker": "SPY",
                "classification_ok": 1,
                "is_equity_eligible": 1,
                "exchange_name": "NYSE Arca",
                "source": "lseg_toolkit",
                "job_run_id": "lseg_job",
                "updated_at": now_iso,
            }
        ],
    )
    conn.commit()

    row = conn.execute(
        """
        SELECT classification_ok, is_equity_eligible, coverage_role, source
        FROM security_master
        WHERE ric = 'SPY.P'
        """
    ).fetchone()
    conn.close()

    assert row == (1, 1, "projection_only", "lseg_toolkit")


class TestCoverageRoleColumnMigration:
    def test_new_table_has_coverage_role(self, tmp_path):
        """A freshly created security_master should have coverage_role column."""
        db_path = tmp_path / "data.db"
        conn = sqlite3.connect(str(db_path))
        ensure_cuse4_schema(conn)

        cols = {
            str(r[1])
            for r in conn.execute("PRAGMA table_info(security_master)").fetchall()
        }
        assert "coverage_role" in cols
        conn.close()

    def test_existing_table_gets_coverage_role_via_migration(self, tmp_path):
        """An existing security_master without coverage_role should get it via ALTER TABLE."""
        db_path = tmp_path / "data.db"
        conn = sqlite3.connect(str(db_path))

        # Create old-style table without coverage_role
        conn.execute("""
            CREATE TABLE security_master (
                ric TEXT PRIMARY KEY,
                ticker TEXT,
                isin TEXT,
                exchange_name TEXT,
                classification_ok INTEGER NOT NULL DEFAULT 0,
                is_equity_eligible INTEGER NOT NULL DEFAULT 0,
                source TEXT,
                job_run_id TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute(
            "INSERT INTO security_master (ric, ticker, updated_at) VALUES ('TEST.N', 'TEST', '2024-01-01')"
        )
        conn.commit()

        # Run schema migration
        ensure_cuse4_schema(conn)

        cols = {
            str(r[1])
            for r in conn.execute("PRAGMA table_info(security_master)").fetchall()
        }
        assert "coverage_role" in cols

        # Existing row should have default value
        row = conn.execute(
            "SELECT coverage_role FROM security_master WHERE ric = 'TEST.N'"
        ).fetchone()
        assert row[0] == "native_equity"
        conn.close()
