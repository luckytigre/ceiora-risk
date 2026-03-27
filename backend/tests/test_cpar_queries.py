from __future__ import annotations

import sqlite3

import pytest

from backend.cpar.factor_registry import CPAR1_METHOD_VERSION
from backend.data import cpar_queries, cpar_schema


def _fetch_rows_factory(conn: sqlite3.Connection):
    def _fetch_rows(sql: str, params=None):
        rows = conn.execute(sql, params or []).fetchall()
        return [dict(row) for row in rows]

    return _fetch_rows


def _seed_query_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cpar_schema.ensure_sqlite_schema(conn)
    conn.executemany(
        """
        INSERT INTO cpar_package_runs (
            package_run_id, package_date, profile, status, started_at, completed_at, method_version,
            factor_registry_version, lookback_weeks, half_life_weeks, min_observations, proxy_price_rule,
            source_prices_asof, classification_asof, universe_count, fit_ok_count, fit_limited_count,
            fit_insufficient_count, data_authority, error_type, error_message, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("run_old", "2026-03-07", "cpar-weekly", "ok", "2026-03-08T00:00:00Z", "2026-03-08T00:01:00Z", CPAR1_METHOD_VERSION, "cPAR1", 52, 26, 39, "adj_close_fallback_close", "2026-03-07", "2026-03-07", 10, 8, 1, 1, "sqlite", None, None, "2026-03-08T00:01:00Z"),
            ("run_failed", "2026-03-14", "cpar-weekly", "failed", "2026-03-15T00:00:00Z", "2026-03-15T00:01:00Z", CPAR1_METHOD_VERSION, "cPAR1", 52, 26, 39, "adj_close_fallback_close", "2026-03-14", "2026-03-14", 10, 0, 0, 10, "sqlite", "RuntimeError", "boom", "2026-03-15T00:01:00Z"),
            ("run_new", "2026-03-14", "cpar-weekly", "ok", "2026-03-15T00:02:00Z", "2026-03-15T00:03:00Z", CPAR1_METHOD_VERSION, "cPAR1", 52, 26, 39, "adj_close_fallback_close", "2026-03-14", "2026-03-14", 11, 9, 1, 1, "neon", None, None, "2026-03-15T00:03:00Z"),
            ("run_incomplete", "2026-03-21", "cpar-weekly", "ok", "2026-03-22T00:02:00Z", "2026-03-22T00:03:00Z", CPAR1_METHOD_VERSION, "cPAR1", 52, 26, 39, "adj_close_fallback_close", "2026-03-21", "2026-03-21", 0, 0, 0, 0, "neon", None, None, "2026-03-22T00:03:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO cpar_instrument_fits_weekly (
            package_date, ric, ticker, display_name, fit_status, warnings_json, observed_weeks, lookback_weeks,
            longest_gap_weeks, price_field_used, hq_country_code, market_step_alpha, market_step_beta, block_alpha,
            spy_trade_beta_raw, raw_loadings_json, thresholded_loadings_json, factor_variance_proxy,
            factor_volatility_proxy, specific_variance_proxy, specific_volatility_proxy, package_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("2026-03-07", "AAPL.OQ", "AAPL", "Apple Inc.", "ok", '["continuity_gap"]', 45, 52, 3, "adj_close", "US", 0.01, 1.2, 0.0, 1.1, '{"SPY":1.1}', '{"SPY":1.1}', 0.2, 0.4472135955, 0.05, 0.2236067977, "run_old", "2026-03-08T00:01:00Z"),
            ("2026-03-14", "AAPL.OQ", "AAPL", "Apple Inc.", "ok", '[]', 52, 52, 0, "adj_close", "US", 0.02, 1.3, 0.0, 1.2, '{"SPY":1.2}', '{"SPY":1.2}', 0.25, 0.5, 0.06, 0.2449489743, "run_new", "2026-03-15T00:03:00Z"),
            ("2026-03-14", "AAPL.L", "AAPL", "Apple London", "limited_history", '["ex_us_caution"]', 42, 52, 2, "close", "GB", 0.01, 0.8, 0.0, 0.7, '{"SPY":0.7}', '{"SPY":0.7}', 0.15, 0.3872983346, 0.04, 0.2, "run_new", "2026-03-15T00:03:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO cpar_package_universe_membership (
            package_run_id, package_date, ric, ticker, universe_scope, target_scope, basis_role,
            build_reason_code, warnings_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("run_old", "2026-03-07", "AAPL.OQ", "AAPL", "core_us_equity", "core_us_equity", "instrument", "hq_country_us", '["continuity_gap"]', "2026-03-08T00:01:00Z"),
            ("run_new", "2026-03-14", "AAPL.OQ", "AAPL", "core_us_equity", "core_us_equity", "instrument", "hq_country_us", "[]", "2026-03-15T00:03:00Z"),
            ("run_new", "2026-03-14", "AAPL.L", "AAPL", "extended_priced_instrument", "extended_priced_instrument", "instrument", "extended_non_us_or_unknown", '["ex_us_caution"]', "2026-03-15T00:03:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO cpar_instrument_runtime_coverage_weekly (
            package_run_id, package_date, ric, ticker, price_on_package_date_status, fit_row_status,
            fit_quality_status, portfolio_use_status, ticker_detail_use_status, hedge_use_status,
            fit_family, fit_status, reason_code, quality_label, warnings_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("run_old", "2026-03-07", "AAPL.OQ", "AAPL", "present", "present", "ok", "covered", "available", "usable", "returns_regression_weekly", "ok", "ok", "ok", '["continuity_gap"]', "2026-03-08T00:01:00Z"),
            ("run_new", "2026-03-14", "AAPL.OQ", "AAPL", "present", "present", "ok", "covered", "available", "usable", "returns_regression_weekly", "ok", "ok", "ok", "[]", "2026-03-15T00:03:00Z"),
            ("run_new", "2026-03-14", "AAPL.L", "AAPL", "present", "present", "limited_history", "covered", "available", "usable", "returns_regression_weekly", "limited_history", "fit_status_limited_history", "limited_history", '["ex_us_caution"]', "2026-03-15T00:03:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO cpar_proxy_returns_weekly (
            package_run_id, package_date, week_end, factor_id, factor_group, proxy_ric, proxy_ticker,
            return_value, weight_value, price_field_used, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("run_old", "2026-03-07", "2026-03-07", "SPY", "market", "SPY.P", "SPY", 0.01, 0.5, "adj_close", "2026-03-08T00:01:00Z"),
            ("run_new", "2026-03-14", "2026-03-14", "SPY", "market", "SPY.P", "SPY", 0.02, 0.5, "adj_close", "2026-03-15T00:03:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO cpar_proxy_transform_weekly (
            package_run_id, package_date, factor_id, factor_group, proxy_ric, proxy_ticker,
            market_alpha, market_beta, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("run_old", "2026-03-07", "XLF", "sector", "XLF.P", "XLF", 0.001, 0.4, "2026-03-08T00:01:00Z"),
            ("run_new", "2026-03-14", "XLF", "sector", "XLF.P", "XLF", 0.001, 0.5, "2026-03-15T00:03:00Z"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO cpar_factor_covariance_weekly (
            package_date, factor_id, factor_id_2, covariance, correlation, package_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("2026-03-07", "SPY", "SPY", 0.9, 1.0, "run_old", "2026-03-08T00:01:00Z"),
            ("2026-03-14", "SPY", "SPY", 1.0, 1.0, "run_new", "2026-03-15T00:03:00Z"),
            ("2026-03-14", "SPY", "XLF", 0.25, 0.5, "run_new", "2026-03-15T00:03:00Z"),
        ],
    )
    conn.commit()
    return conn


def test_latest_successful_package_prefers_latest_successful_run() -> None:
    conn = _seed_query_db()

    out = cpar_queries.latest_successful_package(_fetch_rows_factory(conn))

    assert out is not None
    assert out["package_run_id"] == "run_new"
    assert out["status"] == "ok"


def test_active_package_instrument_fit_raises_on_ambiguous_ticker_without_ric() -> None:
    conn = _seed_query_db()

    with pytest.raises(cpar_queries.CparAmbiguousInstrumentFit, match="AAPL"):
        cpar_queries.active_package_instrument_fit(
            _fetch_rows_factory(conn),
            package_run_id="run_new",
            ticker="AAPL",
        )


def test_active_package_instrument_fit_returns_decoded_payload_with_ric_disambiguation() -> None:
    conn = _seed_query_db()

    out = cpar_queries.active_package_instrument_fit(
        _fetch_rows_factory(conn),
        package_run_id="run_new",
        ticker="AAPL",
        ric="AAPL.OQ",
    )

    assert out is not None
    assert out["ric"] == "AAPL.OQ"
    assert out["warnings"] == []
    assert out["raw_loadings"] == {"SPY": 1.2}
    assert out["specific_variance_proxy"] == pytest.approx(0.06)
    assert out["target_scope"] == "core_us_equity"
    assert out["portfolio_use_status"] == "covered"
    assert out["fit_family"] == "returns_regression_weekly"


def test_package_instrument_fits_for_rics_returns_matching_rows() -> None:
    conn = _seed_query_db()

    rows = cpar_queries.package_instrument_fits_for_rics(
        _fetch_rows_factory(conn),
        package_run_id="run_new",
        rics=["aapl.oq", "aapl.l", "missing"],
    )

    assert [row["ric"] for row in rows] == ["AAPL.L", "AAPL.OQ"]
    assert rows[0]["warnings"] == ["ex_us_caution"]
    assert rows[0]["target_scope"] == "extended_priced_instrument"
    assert rows[0]["fit_quality_status"] == "limited_history"


def test_previous_successful_instrument_fit_returns_prior_successful_package() -> None:
    conn = _seed_query_db()

    out = cpar_queries.previous_successful_instrument_fit(
        _fetch_rows_factory(conn),
        ric="AAPL.OQ",
        before_package_date="2026-03-14",
    )

    assert out is not None
    assert out["package_date"] == "2026-03-07"
    assert out["warnings"] == ["continuity_gap"]


def test_active_package_covariance_rows_returns_sorted_rows() -> None:
    conn = _seed_query_db()

    rows = cpar_queries.active_package_covariance_rows(
        _fetch_rows_factory(conn),
        package_run_id="run_new",
    )

    assert rows == [
        {
            "factor_id": "SPY",
            "factor_id_2": "SPY",
            "covariance": 1.0,
            "correlation": 1.0,
            "package_run_id": "run_new",
            "updated_at": "2026-03-15T00:03:00Z",
        },
        {
            "factor_id": "SPY",
            "factor_id_2": "XLF",
            "covariance": 0.25,
            "correlation": 0.5,
            "package_run_id": "run_new",
            "updated_at": "2026-03-15T00:03:00Z",
        },
    ]


def test_package_proxy_return_rows_returns_rows_grouped_by_factor_and_week() -> None:
    conn = _seed_query_db()

    rows = cpar_queries.package_proxy_return_rows(
        _fetch_rows_factory(conn),
        package_run_id="run_new",
    )

    assert rows == [
        {
            "factor_id": "SPY",
            "factor_group": "market",
            "week_end": "2026-03-14",
            "return_value": 0.02,
            "weight_value": 0.5,
            "proxy_ric": "SPY.P",
            "proxy_ticker": "SPY",
            "package_run_id": "run_new",
            "package_date": "2026-03-14",
            "updated_at": "2026-03-15T00:03:00Z",
        },
    ]


def test_package_proxy_transform_rows_returns_rows_grouped_by_factor() -> None:
    conn = _seed_query_db()

    rows = cpar_queries.package_proxy_transform_rows(
        _fetch_rows_factory(conn),
        package_run_id="run_new",
    )

    assert rows == [
        {
            "factor_id": "XLF",
            "factor_group": "sector",
            "market_alpha": 0.001,
            "market_beta": 0.5,
            "proxy_ric": "XLF.P",
            "proxy_ticker": "XLF",
            "package_run_id": "run_new",
            "package_date": "2026-03-14",
            "updated_at": "2026-03-15T00:03:00Z",
        },
    ]


def test_active_package_search_rows_returns_matching_rows_with_decoded_warnings() -> None:
    conn = _seed_query_db()

    rows = cpar_queries.active_package_search_rows(
        _fetch_rows_factory(conn),
        package_run_id="run_new",
        q="apple",
    )

    assert [row["ric"] for row in rows] == ["AAPL.L", "AAPL.OQ"]
    assert rows[0]["warnings"] == ["ex_us_caution"]
    assert rows[0]["portfolio_use_status"] == "covered"
    assert rows[0]["quality_label"] == "limited_history"


def test_latest_successful_package_ignores_incomplete_success_rows() -> None:
    conn = _seed_query_db()

    out = cpar_queries.latest_successful_package(_fetch_rows_factory(conn))

    assert out is not None
    assert out["package_run_id"] == "run_new"


def test_latest_successful_package_query_uses_postgres_safe_completed_at_ordering() -> None:
    captured: dict[str, str] = {}

    def _fetch_rows(sql: str, params=None):
        captured["sql"] = sql
        captured["params"] = str(params or [])
        return []

    cpar_queries.latest_successful_package(_fetch_rows)

    assert "COALESCE(completed_at, '')" not in captured["sql"]
    assert "(completed_at IS NULL) ASC" in captured["sql"]


def test_previous_successful_query_uses_postgres_safe_completed_at_ordering() -> None:
    captured: dict[str, str] = {}

    def _fetch_rows(sql: str, params=None):
        captured["sql"] = sql
        captured["params"] = str(params or [])
        return []

    cpar_queries.previous_successful_instrument_fit(
        _fetch_rows,
        ric="AAPL.OQ",
        before_package_date="2026-03-14",
    )

    assert "COALESCE(p.completed_at, '')" not in captured["sql"]
    assert "(p.completed_at IS NULL) ASC" in captured["sql"]
