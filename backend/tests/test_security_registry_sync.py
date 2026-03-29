from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.universe.bootstrap import bootstrap_cuse4_source_tables
from backend.universe.registry_sync import (
    derive_policy_flags_from_structure,
    ensure_registry_rows_from_master_rows,
    legacy_coverage_role_from_policy_flags,
    policy_defaults_for_legacy_coverage_role,
    reconcile_default_security_policy_rows,
)
from backend.universe.schema import ensure_cuse4_schema


def test_bootstrap_populates_registry_policy_and_compat_from_legacy_seed(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.close()

    seed_path = tmp_path / "security_master_seed.csv"
    seed_path.write_text(
        "\n".join(
            [
                "ric,ticker,isin,exchange_name,coverage_role",
                "AAPL.OQ,AAPL,US0378331005,NASDAQ,native_equity",
                "SPY.P,SPY,,NYSE Arca,projection_only",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out = bootstrap_cuse4_source_tables(db_path=data_db, seed_path=seed_path)

    assert out["status"] == "ok"
    assert out["security_master_rows"] == 0
    assert out["security_registry_rows"] == 2
    assert out["security_policy_current_rows"] == 2
    assert out["security_master_compat_current_rows"] == 2

    conn = sqlite3.connect(str(data_db))
    try:
        registry_rows = conn.execute(
            "SELECT ric, ticker, tracking_status FROM security_registry ORDER BY ric"
        ).fetchall()
        assert registry_rows == [
            ("AAPL.OQ", "AAPL", "active"),
            ("SPY.P", "SPY", "active"),
        ]
        policy_rows = conn.execute(
            """
            SELECT ric, price_ingest_enabled, pit_fundamentals_enabled, pit_classification_enabled,
                   allow_cuse_native_core, allow_cuse_returns_projection
            FROM security_policy_current
            ORDER BY ric
            """
        ).fetchall()
        assert policy_rows == [
            ("AAPL.OQ", 1, 1, 1, 1, 0),
            ("SPY.P", 1, 0, 0, 0, 1),
        ]
        compat_rows = conn.execute(
            "SELECT ric, ticker, coverage_role FROM security_master_compat_current ORDER BY ric"
        ).fetchall()
        assert compat_rows == [
            ("AAPL.OQ", "AAPL", "native_equity"),
            ("SPY.P", "SPY", "projection_only"),
        ]
    finally:
        conn.close()


def test_bootstrap_derives_legacy_coverage_role_from_registry_policy_columns(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.close()

    seed_path = tmp_path / "security_registry_seed.csv"
    seed_path.write_text(
        "\n".join(
            [
                "ric,ticker,isin,exchange_name,tracking_status,price_ingest_enabled,pit_fundamentals_enabled,pit_classification_enabled,allow_cuse_native_core,allow_cuse_fundamental_projection,allow_cuse_returns_projection,allow_cpar_core_target,allow_cpar_extended_target",
                "AAPL.OQ,AAPL,US0378331005,NASDAQ,active,1,1,1,1,0,0,1,1",
                "SPY.P,SPY,,NYSE Arca,active,1,0,0,0,0,1,0,1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out = bootstrap_cuse4_source_tables(db_path=data_db, seed_path=seed_path)

    assert out["status"] == "ok"
    assert out["security_master_rows"] == 0

    conn = sqlite3.connect(str(data_db))
    try:
        registry_sources = conn.execute(
            "SELECT ric, source FROM security_registry ORDER BY ric"
        ).fetchall()
        assert registry_sources == [
            ("AAPL.OQ", "security_registry_seed"),
            ("SPY.P", "security_registry_seed"),
        ]
        compat_rows = conn.execute(
            """
            SELECT ric, ticker, coverage_role, source
            FROM security_master_compat_current
            ORDER BY ric
            """
        ).fetchall()
        assert compat_rows == [
            ("AAPL.OQ", "AAPL", "native_equity", "security_registry_seed"),
            ("SPY.P", "SPY", "projection_only", "security_registry_seed"),
        ]
    finally:
        conn.close()


def test_bootstrap_custom_registry_seed_path_stamps_registry_seed_provenance(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.close()

    seed_path = tmp_path / "custom_universe_input.csv"
    seed_path.write_text(
        "\n".join(
            [
                "ric,ticker,isin,exchange_name,tracking_status,price_ingest_enabled,pit_fundamentals_enabled,pit_classification_enabled,allow_cuse_native_core,allow_cuse_fundamental_projection,allow_cuse_returns_projection,allow_cpar_core_target,allow_cpar_extended_target",
                "AAPL.OQ,AAPL,US0378331005,NASDAQ,active,1,1,1,1,0,0,1,1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out = bootstrap_cuse4_source_tables(db_path=data_db, seed_path=seed_path)

    assert out["status"] == "ok"
    assert out["security_master_rows"] == 0

    conn = sqlite3.connect(str(data_db))
    try:
        registry_sources = conn.execute(
            "SELECT ric, source FROM security_registry ORDER BY ric"
        ).fetchall()
        assert registry_sources == [("AAPL.OQ", "security_registry_seed")]
        compat_sources = conn.execute(
            """
            SELECT ric, source
            FROM security_master_compat_current
            ORDER BY ric
            """
        ).fetchall()
        assert compat_sources == [("AAPL.OQ", "security_registry_seed")]
    finally:
        conn.close()


def test_registry_mirror_preserves_row_level_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    ensure_cuse4_schema(conn)

    mirrored = ensure_registry_rows_from_master_rows(
        conn,
        [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "source": "lseg_toolkit",
                "job_run_id": "job_aapl",
                "updated_at": "2026-03-20T00:00:00+00:00",
            },
            {
                "ric": "SPY.P",
                "ticker": "SPY",
                "source": "security_master_seed",
                "job_run_id": "job_spy",
                "updated_at": "2026-03-21T00:00:00+00:00",
            },
        ],
    )
    conn.commit()

    assert mirrored == 2
    rows = conn.execute(
        """
        SELECT ric, source, job_run_id, updated_at
        FROM security_registry
        ORDER BY ric
        """
    ).fetchall()
    assert rows == [
        ("AAPL.OQ", "lseg_toolkit", "job_aapl", "2026-03-20T00:00:00+00:00"),
        ("SPY.P", "security_master_seed", "job_spy", "2026-03-21T00:00:00+00:00"),
    ]
    conn.close()


def test_legacy_coverage_role_mapper_defaults_non_canonical_policy_combinations_to_native_equity() -> None:
    assert (
        legacy_coverage_role_from_policy_flags(
            allow_cuse_returns_projection=1,
            pit_fundamentals_enabled=1,
            pit_classification_enabled=1,
        )
        == "native_equity"
    )


def test_derive_policy_flags_from_structure_preserves_defaults_for_other_unknown_non_equity() -> None:
    assert derive_policy_flags_from_structure(
        legacy_coverage_role="native_equity",
        instrument_kind=" other ",
        model_home_market_scope=" unknown ",
        is_single_name_equity=0,
    ) == policy_defaults_for_legacy_coverage_role("native_equity")


def test_reconcile_default_security_policy_rows_derives_defaults_from_taxonomy_without_compat(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES ('AAPL.OQ', 'AAPL', 'active', '2026-03-20T00:00:00Z')
        """
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
            updated_at
        ) VALUES ('AAPL.OQ', 0, 0, 0, 0, 0, 0, 0, 0, 'registry_seed_defaults', '2026-03-20T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_taxonomy_current (
            ric, instrument_kind, vehicle_structure, model_home_market_scope,
            is_single_name_equity, classification_ready, updated_at
        ) VALUES ('AAPL.OQ', 'single_name_equity', 'equity_security', 'us', 1, 1, '2026-03-20T00:00:00Z')
        """
    )

    refreshed = reconcile_default_security_policy_rows(conn, rics=["AAPL.OQ"])

    assert refreshed == 1
    row = conn.execute(
        """
        SELECT
            price_ingest_enabled,
            pit_fundamentals_enabled,
            pit_classification_enabled,
            allow_cuse_native_core,
            allow_cuse_fundamental_projection,
            allow_cuse_returns_projection,
            allow_cpar_core_target,
            allow_cpar_extended_target
        FROM security_policy_current
        WHERE ric = 'AAPL.OQ'
        """
    ).fetchone()
    conn.close()

    assert row == (1, 1, 1, 1, 0, 0, 1, 1)


def test_reconcile_default_security_policy_rows_can_use_compat_when_taxonomy_table_is_absent(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute("DROP TABLE security_taxonomy_current")
    conn.execute(
        """
        INSERT INTO security_registry (ric, ticker, tracking_status, updated_at)
        VALUES ('SPY.P', 'SPY', 'active', '2026-03-20T00:00:00Z')
        """
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
            updated_at
        ) VALUES ('SPY.P', 1, 1, 1, 1, 0, 0, 1, 1, 'registry_seed_defaults', '2026-03-20T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO security_master_compat_current (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, job_run_id, updated_at
        ) VALUES ('SPY.P', 'SPY', 0, 0, 'projection_only', 'compat', 'job_compat', '2026-03-20T00:00:00Z')
        """
    )

    refreshed = reconcile_default_security_policy_rows(conn, rics=["SPY.P"])

    assert refreshed == 1
    row = conn.execute(
        """
        SELECT
            price_ingest_enabled,
            pit_fundamentals_enabled,
            pit_classification_enabled,
            allow_cuse_native_core,
            allow_cuse_fundamental_projection,
            allow_cuse_returns_projection,
            allow_cpar_core_target,
            allow_cpar_extended_target
        FROM security_policy_current
        WHERE ric = 'SPY.P'
        """
    ).fetchone()
    conn.close()

    assert row == (1, 0, 0, 0, 0, 1, 0, 1)
    assert (
        legacy_coverage_role_from_policy_flags(
            allow_cuse_returns_projection=None,
            pit_fundamentals_enabled=None,
            pit_classification_enabled=None,
        )
        == "native_equity"
    )
