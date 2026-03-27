from __future__ import annotations

import sqlite3

from backend.data import cpar_schema


def test_ensure_sqlite_schema_creates_expected_tables_and_columns() -> None:
    conn = sqlite3.connect(":memory:")

    cpar_schema.ensure_sqlite_schema(conn)

    for table in cpar_schema.TABLES:
        cols = cpar_schema.table_columns(conn, table)
        assert cols

    assert cpar_schema.table_columns(conn, cpar_schema.TABLE_PACKAGE_RUNS) >= {
        "package_run_id",
        "package_date",
        "profile",
        "status",
        "method_version",
        "factor_registry_version",
        "data_authority",
    }
    assert cpar_schema.table_columns(conn, cpar_schema.TABLE_PROXY_RETURNS) >= {
        "package_run_id",
        "package_date",
        "week_end",
        "factor_id",
        "return_value",
        "weight_value",
        "price_field_used",
    }
    assert cpar_schema.table_columns(conn, cpar_schema.TABLE_PROXY_TRANSFORM) >= {
        "package_run_id",
        "package_date",
        "factor_id",
        "market_alpha",
        "market_beta",
    }
    assert cpar_schema.table_columns(conn, cpar_schema.TABLE_FACTOR_COVARIANCE) >= {
        "package_run_id",
        "package_date",
        "factor_id",
        "factor_id_2",
        "covariance",
        "correlation",
    }
    assert cpar_schema.table_columns(conn, cpar_schema.TABLE_INSTRUMENT_FITS) >= {
        "package_run_id",
        "package_date",
        "ric",
        "fit_status",
        "warnings_json",
        "raw_loadings_json",
        "thresholded_loadings_json",
        "specific_variance_proxy",
        "specific_volatility_proxy",
    }
    assert cpar_schema.table_columns(conn, cpar_schema.TABLE_PACKAGE_UNIVERSE_MEMBERSHIP) >= {
        "package_run_id",
        "package_date",
        "ric",
        "universe_scope",
        "target_scope",
        "basis_role",
        "warnings_json",
    }
    assert cpar_schema.table_columns(conn, cpar_schema.TABLE_RUNTIME_COVERAGE) >= {
        "package_run_id",
        "package_date",
        "ric",
        "price_on_package_date_status",
        "fit_row_status",
        "fit_quality_status",
        "portfolio_use_status",
        "ticker_detail_use_status",
        "hedge_use_status",
        "fit_family",
        "quality_label",
    }
    assert cpar_schema.table_primary_key_columns(conn, cpar_schema.TABLE_PROXY_RETURNS) == (
        "package_run_id",
        "week_end",
        "factor_id",
    )
    assert cpar_schema.table_primary_key_columns(conn, cpar_schema.TABLE_PROXY_TRANSFORM) == (
        "package_run_id",
        "factor_id",
    )
    assert cpar_schema.table_primary_key_columns(conn, cpar_schema.TABLE_FACTOR_COVARIANCE) == (
        "package_run_id",
        "factor_id",
        "factor_id_2",
    )
    assert cpar_schema.table_primary_key_columns(conn, cpar_schema.TABLE_INSTRUMENT_FITS) == (
        "package_run_id",
        "ric",
    )
    assert cpar_schema.table_primary_key_columns(conn, cpar_schema.TABLE_PACKAGE_UNIVERSE_MEMBERSHIP) == (
        "package_run_id",
        "ric",
    )
    assert cpar_schema.table_primary_key_columns(conn, cpar_schema.TABLE_RUNTIME_COVERAGE) == (
        "package_run_id",
        "ric",
    )


def test_postgres_schema_sql_includes_all_cpar_tables() -> None:
    script = cpar_schema.postgres_schema_sql()

    for table in cpar_schema.TABLES:
        assert table in script


def test_ensure_sqlite_schema_rebuilds_stale_package_runs_table() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE cpar_package_runs (
            package_run_id TEXT PRIMARY KEY,
            package_date TEXT NOT NULL
        )
        """
    )

    cpar_schema.ensure_sqlite_schema(conn)

    assert cpar_schema.table_columns(conn, cpar_schema.TABLE_PACKAGE_RUNS) >= {
        "profile",
        "status",
        "method_version",
        "factor_registry_version",
        "data_authority",
    }
    assert cpar_schema.table_primary_key_columns(conn, cpar_schema.TABLE_PACKAGE_RUNS) == ("package_run_id",)


def test_ensure_sqlite_schema_rebuilds_stale_child_table_with_missing_columns() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE cpar_proxy_returns_weekly (
            package_run_id TEXT NOT NULL,
            package_date TEXT NOT NULL,
            week_end TEXT NOT NULL,
            factor_id TEXT NOT NULL,
            factor_group TEXT NOT NULL,
            return_value REAL NOT NULL,
            weight_value REAL NOT NULL,
            price_field_used TEXT NOT NULL,
            PRIMARY KEY (package_run_id, week_end, factor_id)
        )
        """
    )

    cpar_schema.ensure_sqlite_schema(conn)

    assert cpar_schema.table_columns(conn, cpar_schema.TABLE_PROXY_RETURNS) >= {
        "proxy_ric",
        "proxy_ticker",
        "updated_at",
    }
