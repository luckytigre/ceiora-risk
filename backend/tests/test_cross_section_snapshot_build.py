from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.data.cross_section_snapshot_build import load_base_cross_sections
from backend.risk_model.raw_cross_section_history import ensure_raw_cross_section_history_table
from backend.universe.schema import ensure_cuse4_schema


def _seed_runtime_identity(
    conn: sqlite3.Connection,
    *,
    ric: str,
    ticker: str,
    price_ingest_enabled: int,
    allow_cuse_native_core: int,
    allow_cuse_returns_projection: int,
    is_single_name_equity: int,
    classification_ready: int,
) -> None:
    now_iso = "2026-03-15T00:00:00Z"
    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES (?, ?, 'active', 'security_registry_seed', ?)
        """,
        (ric, ticker, now_iso),
    )
    conn.execute(
        """
        INSERT INTO security_policy_current (
            ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
            allow_cuse_native_core, allow_cuse_fundamental_projection, allow_cuse_returns_projection,
            allow_cpar_core_target, allow_cpar_extended_target, policy_source, updated_at
        ) VALUES (?, ?, 0, 0, ?, 0, ?, 1, 1, 'default', ?)
        """,
        (
            ric,
            price_ingest_enabled,
            allow_cuse_native_core,
            allow_cuse_returns_projection,
            now_iso,
        ),
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, source, updated_at
        ) VALUES (?, 'fund_vehicle', 'fund', 'us', ?, ?, 'taxonomy_refresh', ?)
        """,
        (
            ric,
            is_single_name_equity,
            classification_ready,
            now_iso,
        ),
    )


def test_load_base_cross_sections_keeps_raw_history_membership_when_runtime_flags_exclude(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    ensure_raw_cross_section_history_table(conn)
    _seed_runtime_identity(
        conn,
        ric="PROJ.OQ",
        ticker="PROJ",
        price_ingest_enabled=1,
        allow_cuse_native_core=0,
        allow_cuse_returns_projection=1,
        is_single_name_equity=0,
        classification_ready=0,
    )
    conn.execute(
        """
        INSERT INTO barra_raw_cross_section_history (
            ric, ticker, as_of_date, updated_at
        ) VALUES ('PROJ.OQ', 'PROJ', '2026-03-13', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()

    out = load_base_cross_sections(
        conn,
        start_date="2026-03-13",
        end_date="2026-03-13",
        tickers=None,
        mode="full",
    )
    conn.close()

    assert out[["ric", "ticker", "as_of_date"]].to_dict("records") == [
        {"ric": "PROJ.OQ", "ticker": "PROJ", "as_of_date": "2026-03-13"}
    ]


def test_load_base_cross_sections_does_not_resurrect_runtime_rows_missing_from_raw_history(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    ensure_raw_cross_section_history_table(conn)
    _seed_runtime_identity(
        conn,
        ric="KEEP.OQ",
        ticker="KEEP",
        price_ingest_enabled=1,
        allow_cuse_native_core=1,
        allow_cuse_returns_projection=0,
        is_single_name_equity=1,
        classification_ready=1,
    )
    _seed_runtime_identity(
        conn,
        ric="MISS.OQ",
        ticker="MISS",
        price_ingest_enabled=1,
        allow_cuse_native_core=1,
        allow_cuse_returns_projection=0,
        is_single_name_equity=1,
        classification_ready=1,
    )
    conn.execute(
        """
        INSERT INTO barra_raw_cross_section_history (
            ric, ticker, as_of_date, updated_at
        ) VALUES ('KEEP.OQ', 'KEEP', '2026-03-13', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()

    out = load_base_cross_sections(
        conn,
        start_date="2026-03-13",
        end_date="2026-03-13",
        tickers=None,
        mode="full",
    )
    conn.close()

    assert out[["ric", "ticker", "as_of_date"]].to_dict("records") == [
        {"ric": "KEEP.OQ", "ticker": "KEEP", "as_of_date": "2026-03-13"}
    ]
