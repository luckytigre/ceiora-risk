"""SQLite and Postgres writers for durable cPAR persistence."""

from __future__ import annotations

import json
from typing import Any

from backend.data import cpar_schema


def _json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def ensure_postgres_schema(pg_conn) -> None:
    cpar_schema.ensure_postgres_schema(pg_conn)


def write_cpar_outputs_sqlite(
    conn,
    *,
    package_run: dict[str, Any],
    proxy_returns: list[dict[str, Any]],
    proxy_transforms: list[dict[str, Any]],
    covariance_rows: list[dict[str, Any]],
    instrument_fits: list[dict[str, Any]],
    package_membership: list[dict[str, Any]] | None = None,
    runtime_coverage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cpar_schema.ensure_sqlite_schema(conn)
    package_run_id = str(package_run["package_run_id"])
    conn.execute(f"DELETE FROM {cpar_schema.TABLE_PROXY_RETURNS} WHERE package_run_id = ?", (package_run_id,))
    conn.execute(f"DELETE FROM {cpar_schema.TABLE_PROXY_TRANSFORM} WHERE package_run_id = ?", (package_run_id,))
    conn.execute(f"DELETE FROM {cpar_schema.TABLE_FACTOR_COVARIANCE} WHERE package_run_id = ?", (package_run_id,))
    conn.execute(f"DELETE FROM {cpar_schema.TABLE_INSTRUMENT_FITS} WHERE package_run_id = ?", (package_run_id,))
    conn.execute(f"DELETE FROM {cpar_schema.TABLE_PACKAGE_UNIVERSE_MEMBERSHIP} WHERE package_run_id = ?", (package_run_id,))
    conn.execute(f"DELETE FROM {cpar_schema.TABLE_RUNTIME_COVERAGE} WHERE package_run_id = ?", (package_run_id,))
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {cpar_schema.TABLE_PACKAGE_RUNS} (
            package_run_id, package_date, profile, status, started_at, completed_at, method_version,
            factor_registry_version, lookback_weeks, half_life_weeks, min_observations, proxy_price_rule,
            source_prices_asof, classification_asof, universe_count, fit_ok_count, fit_limited_count,
            fit_insufficient_count, data_authority, error_type, error_message, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            package_run["package_run_id"],
            package_run["package_date"],
            package_run["profile"],
            package_run["status"],
            package_run["started_at"],
            package_run["completed_at"],
            package_run["method_version"],
            package_run["factor_registry_version"],
            package_run["lookback_weeks"],
            package_run["half_life_weeks"],
            package_run["min_observations"],
            package_run["proxy_price_rule"],
            package_run.get("source_prices_asof"),
            package_run.get("classification_asof"),
            package_run["universe_count"],
            package_run["fit_ok_count"],
            package_run["fit_limited_count"],
            package_run["fit_insufficient_count"],
            package_run["data_authority"],
            package_run.get("error_type"),
            package_run.get("error_message"),
            package_run["updated_at"],
        ),
    )
    if proxy_returns:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {cpar_schema.TABLE_PROXY_RETURNS} (
                package_run_id, package_date, week_end, factor_id, factor_group, proxy_ric, proxy_ticker,
                return_value, weight_value, price_field_used, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["package_run_id"],
                    row["package_date"],
                    row["week_end"],
                    row["factor_id"],
                    row["factor_group"],
                    row["proxy_ric"],
                    row["proxy_ticker"],
                    row["return_value"],
                    row["weight_value"],
                    row["price_field_used"],
                    row["updated_at"],
                )
                for row in proxy_returns
            ],
        )
    if proxy_transforms:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {cpar_schema.TABLE_PROXY_TRANSFORM} (
                package_run_id, package_date, factor_id, factor_group, proxy_ric, proxy_ticker,
                market_alpha, market_beta, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["package_run_id"],
                    row["package_date"],
                    row["factor_id"],
                    row["factor_group"],
                    row["proxy_ric"],
                    row["proxy_ticker"],
                    row["market_alpha"],
                    row["market_beta"],
                    row["updated_at"],
                )
                for row in proxy_transforms
            ],
        )
    if covariance_rows:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {cpar_schema.TABLE_FACTOR_COVARIANCE} (
                package_run_id, package_date, factor_id, factor_id_2, covariance, correlation, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["package_run_id"],
                    row["package_date"],
                    row["factor_id"],
                    row["factor_id_2"],
                    row["covariance"],
                    row["correlation"],
                    row["updated_at"],
                )
                for row in covariance_rows
            ],
        )
    if instrument_fits:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {cpar_schema.TABLE_INSTRUMENT_FITS} (
                package_run_id, package_date, ric, ticker, display_name, fit_status, warnings_json, observed_weeks,
                lookback_weeks, longest_gap_weeks, price_field_used, hq_country_code, market_step_alpha,
                market_step_beta, block_alpha, spy_trade_beta_raw, raw_loadings_json,
                thresholded_loadings_json, factor_variance_proxy, factor_volatility_proxy,
                specific_variance_proxy, specific_volatility_proxy, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["package_run_id"],
                    row["package_date"],
                    row["ric"],
                    row.get("ticker"),
                    row.get("display_name"),
                    row["fit_status"],
                    _json_text(row.get("warnings", [])),
                    row["observed_weeks"],
                    row["lookback_weeks"],
                    row["longest_gap_weeks"],
                    row["price_field_used"],
                    row.get("hq_country_code"),
                    row.get("market_step_alpha"),
                    row.get("market_step_beta"),
                    row.get("block_alpha"),
                    row.get("spy_trade_beta_raw"),
                    _json_text(row.get("raw_loadings", {})),
                    _json_text(row.get("thresholded_loadings", {})),
                    row.get("factor_variance_proxy"),
                    row.get("factor_volatility_proxy"),
                    row.get("specific_variance_proxy"),
                    row.get("specific_volatility_proxy"),
                    row["updated_at"],
                )
                for row in instrument_fits
            ],
        )
    if package_membership:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {cpar_schema.TABLE_PACKAGE_UNIVERSE_MEMBERSHIP} (
                package_run_id, package_date, ric, ticker, universe_scope, target_scope, basis_role,
                build_reason_code, warnings_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["package_run_id"],
                    row["package_date"],
                    row["ric"],
                    row.get("ticker"),
                    row["universe_scope"],
                    row["target_scope"],
                    row["basis_role"],
                    row.get("build_reason_code"),
                    _json_text(row.get("warnings", [])),
                    row["updated_at"],
                )
                for row in package_membership
            ],
        )
    if runtime_coverage:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {cpar_schema.TABLE_RUNTIME_COVERAGE} (
                package_run_id, package_date, ric, ticker, price_on_package_date_status, fit_row_status,
                fit_quality_status, portfolio_use_status, ticker_detail_use_status, hedge_use_status, fit_family,
                fit_status, reason_code, quality_label, warnings_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["package_run_id"],
                    row["package_date"],
                    row["ric"],
                    row.get("ticker"),
                    row["price_on_package_date_status"],
                    row["fit_row_status"],
                    row["fit_quality_status"],
                    row["portfolio_use_status"],
                    row["ticker_detail_use_status"],
                    row["hedge_use_status"],
                    row["fit_family"],
                    row["fit_status"],
                    row.get("reason_code"),
                    row["quality_label"],
                    _json_text(row.get("warnings", [])),
                    row["updated_at"],
                )
                for row in runtime_coverage
            ],
        )
    conn.commit()
    return {"status": "ok"}


def write_cpar_outputs_postgres(
    pg_conn,
    *,
    package_run: dict[str, Any],
    proxy_returns: list[dict[str, Any]],
    proxy_transforms: list[dict[str, Any]],
    covariance_rows: list[dict[str, Any]],
    instrument_fits: list[dict[str, Any]],
    package_membership: list[dict[str, Any]] | None = None,
    runtime_coverage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    ensure_postgres_schema(pg_conn)
    package_run_id = str(package_run["package_run_id"])
    with pg_conn.cursor() as cur:
        cur.execute(f"DELETE FROM {cpar_schema.TABLE_PROXY_RETURNS} WHERE package_run_id = %s", (package_run_id,))
        cur.execute(f"DELETE FROM {cpar_schema.TABLE_PROXY_TRANSFORM} WHERE package_run_id = %s", (package_run_id,))
        cur.execute(f"DELETE FROM {cpar_schema.TABLE_FACTOR_COVARIANCE} WHERE package_run_id = %s", (package_run_id,))
        cur.execute(f"DELETE FROM {cpar_schema.TABLE_INSTRUMENT_FITS} WHERE package_run_id = %s", (package_run_id,))
        cur.execute(f"DELETE FROM {cpar_schema.TABLE_PACKAGE_UNIVERSE_MEMBERSHIP} WHERE package_run_id = %s", (package_run_id,))
        cur.execute(f"DELETE FROM {cpar_schema.TABLE_RUNTIME_COVERAGE} WHERE package_run_id = %s", (package_run_id,))
        cur.execute(
            f"""
            INSERT INTO {cpar_schema.TABLE_PACKAGE_RUNS} (
                package_run_id, package_date, profile, status, started_at, completed_at, method_version,
                factor_registry_version, lookback_weeks, half_life_weeks, min_observations, proxy_price_rule,
                source_prices_asof, classification_asof, universe_count, fit_ok_count, fit_limited_count,
                fit_insufficient_count, data_authority, error_type, error_message, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (package_run_id) DO UPDATE SET
                package_date = EXCLUDED.package_date,
                profile = EXCLUDED.profile,
                status = EXCLUDED.status,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                method_version = EXCLUDED.method_version,
                factor_registry_version = EXCLUDED.factor_registry_version,
                lookback_weeks = EXCLUDED.lookback_weeks,
                half_life_weeks = EXCLUDED.half_life_weeks,
                min_observations = EXCLUDED.min_observations,
                proxy_price_rule = EXCLUDED.proxy_price_rule,
                source_prices_asof = EXCLUDED.source_prices_asof,
                classification_asof = EXCLUDED.classification_asof,
                universe_count = EXCLUDED.universe_count,
                fit_ok_count = EXCLUDED.fit_ok_count,
                fit_limited_count = EXCLUDED.fit_limited_count,
                fit_insufficient_count = EXCLUDED.fit_insufficient_count,
                data_authority = EXCLUDED.data_authority,
                error_type = EXCLUDED.error_type,
                error_message = EXCLUDED.error_message,
                updated_at = EXCLUDED.updated_at
            """,
            (
                package_run["package_run_id"],
                package_run["package_date"],
                package_run["profile"],
                package_run["status"],
                package_run["started_at"],
                package_run["completed_at"],
                package_run["method_version"],
                package_run["factor_registry_version"],
                package_run["lookback_weeks"],
                package_run["half_life_weeks"],
                package_run["min_observations"],
                package_run["proxy_price_rule"],
                package_run.get("source_prices_asof"),
                package_run.get("classification_asof"),
                package_run["universe_count"],
                package_run["fit_ok_count"],
                package_run["fit_limited_count"],
                package_run["fit_insufficient_count"],
                package_run["data_authority"],
                package_run.get("error_type"),
                package_run.get("error_message"),
                package_run["updated_at"],
            ),
        )
        if proxy_returns:
            cur.executemany(
                f"""
                INSERT INTO {cpar_schema.TABLE_PROXY_RETURNS} (
                    package_run_id, package_date, week_end, factor_id, factor_group, proxy_ric, proxy_ticker,
                    return_value, weight_value, price_field_used, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (package_run_id, week_end, factor_id) DO UPDATE SET
                    package_date = EXCLUDED.package_date,
                    factor_group = EXCLUDED.factor_group,
                    proxy_ric = EXCLUDED.proxy_ric,
                    proxy_ticker = EXCLUDED.proxy_ticker,
                    return_value = EXCLUDED.return_value,
                    weight_value = EXCLUDED.weight_value,
                    price_field_used = EXCLUDED.price_field_used,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    (
                        row["package_run_id"],
                        row["package_date"],
                        row["week_end"],
                        row["factor_id"],
                        row["factor_group"],
                        row["proxy_ric"],
                        row["proxy_ticker"],
                        row["return_value"],
                        row["weight_value"],
                        row["price_field_used"],
                        row["updated_at"],
                    )
                    for row in proxy_returns
                ],
            )
        if proxy_transforms:
            cur.executemany(
                f"""
                INSERT INTO {cpar_schema.TABLE_PROXY_TRANSFORM} (
                    package_run_id, package_date, factor_id, factor_group, proxy_ric, proxy_ticker,
                    market_alpha, market_beta, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (package_run_id, factor_id) DO UPDATE SET
                    package_date = EXCLUDED.package_date,
                    factor_group = EXCLUDED.factor_group,
                    proxy_ric = EXCLUDED.proxy_ric,
                    proxy_ticker = EXCLUDED.proxy_ticker,
                    market_alpha = EXCLUDED.market_alpha,
                    market_beta = EXCLUDED.market_beta,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    (
                        row["package_run_id"],
                        row["package_date"],
                        row["factor_id"],
                        row["factor_group"],
                        row["proxy_ric"],
                        row["proxy_ticker"],
                        row["market_alpha"],
                        row["market_beta"],
                        row["updated_at"],
                    )
                    for row in proxy_transforms
                ],
            )
        if covariance_rows:
            cur.executemany(
                f"""
                INSERT INTO {cpar_schema.TABLE_FACTOR_COVARIANCE} (
                    package_run_id, package_date, factor_id, factor_id_2, covariance, correlation, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (package_run_id, factor_id, factor_id_2) DO UPDATE SET
                    package_date = EXCLUDED.package_date,
                    covariance = EXCLUDED.covariance,
                    correlation = EXCLUDED.correlation,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    (
                        row["package_run_id"],
                        row["package_date"],
                        row["factor_id"],
                        row["factor_id_2"],
                        row["covariance"],
                        row["correlation"],
                        row["updated_at"],
                    )
                    for row in covariance_rows
                ],
            )
        if instrument_fits:
            cur.executemany(
                f"""
                INSERT INTO {cpar_schema.TABLE_INSTRUMENT_FITS} (
                    package_run_id, package_date, ric, ticker, display_name, fit_status, warnings_json, observed_weeks,
                    lookback_weeks, longest_gap_weeks, price_field_used, hq_country_code, market_step_alpha,
                    market_step_beta, block_alpha, spy_trade_beta_raw, raw_loadings_json,
                    thresholded_loadings_json, factor_variance_proxy, factor_volatility_proxy,
                    specific_variance_proxy, specific_volatility_proxy, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s)
                ON CONFLICT (package_run_id, ric) DO UPDATE SET
                    package_date = EXCLUDED.package_date,
                    ticker = EXCLUDED.ticker,
                    display_name = EXCLUDED.display_name,
                    fit_status = EXCLUDED.fit_status,
                    warnings_json = EXCLUDED.warnings_json,
                    observed_weeks = EXCLUDED.observed_weeks,
                    lookback_weeks = EXCLUDED.lookback_weeks,
                    longest_gap_weeks = EXCLUDED.longest_gap_weeks,
                    price_field_used = EXCLUDED.price_field_used,
                    hq_country_code = EXCLUDED.hq_country_code,
                    market_step_alpha = EXCLUDED.market_step_alpha,
                    market_step_beta = EXCLUDED.market_step_beta,
                    block_alpha = EXCLUDED.block_alpha,
                    spy_trade_beta_raw = EXCLUDED.spy_trade_beta_raw,
                    raw_loadings_json = EXCLUDED.raw_loadings_json,
                    thresholded_loadings_json = EXCLUDED.thresholded_loadings_json,
                    factor_variance_proxy = EXCLUDED.factor_variance_proxy,
                    factor_volatility_proxy = EXCLUDED.factor_volatility_proxy,
                    specific_variance_proxy = EXCLUDED.specific_variance_proxy,
                    specific_volatility_proxy = EXCLUDED.specific_volatility_proxy,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    (
                        row["package_run_id"],
                        row["package_date"],
                        row["ric"],
                        row.get("ticker"),
                        row.get("display_name"),
                        row["fit_status"],
                        _json_text(row.get("warnings", [])),
                        row["observed_weeks"],
                        row["lookback_weeks"],
                        row["longest_gap_weeks"],
                        row["price_field_used"],
                        row.get("hq_country_code"),
                        row.get("market_step_alpha"),
                        row.get("market_step_beta"),
                        row.get("block_alpha"),
                        row.get("spy_trade_beta_raw"),
                        _json_text(row.get("raw_loadings", {})),
                        _json_text(row.get("thresholded_loadings", {})),
                        row.get("factor_variance_proxy"),
                        row.get("factor_volatility_proxy"),
                        row.get("specific_variance_proxy"),
                        row.get("specific_volatility_proxy"),
                        row["updated_at"],
                    )
                    for row in instrument_fits
                ],
            )
        if package_membership:
            cur.executemany(
                f"""
                INSERT INTO {cpar_schema.TABLE_PACKAGE_UNIVERSE_MEMBERSHIP} (
                    package_run_id, package_date, ric, ticker, universe_scope, target_scope, basis_role,
                    build_reason_code, warnings_json, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (package_run_id, ric) DO UPDATE SET
                    package_date = EXCLUDED.package_date,
                    ticker = EXCLUDED.ticker,
                    universe_scope = EXCLUDED.universe_scope,
                    target_scope = EXCLUDED.target_scope,
                    basis_role = EXCLUDED.basis_role,
                    build_reason_code = EXCLUDED.build_reason_code,
                    warnings_json = EXCLUDED.warnings_json,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    (
                        row["package_run_id"],
                        row["package_date"],
                        row["ric"],
                        row.get("ticker"),
                        row["universe_scope"],
                        row["target_scope"],
                        row["basis_role"],
                        row.get("build_reason_code"),
                        _json_text(row.get("warnings", [])),
                        row["updated_at"],
                    )
                    for row in package_membership
                ],
            )
        if runtime_coverage:
            cur.executemany(
                f"""
                INSERT INTO {cpar_schema.TABLE_RUNTIME_COVERAGE} (
                    package_run_id, package_date, ric, ticker, price_on_package_date_status, fit_row_status,
                    fit_quality_status, portfolio_use_status, ticker_detail_use_status, hedge_use_status,
                    fit_family, fit_status, reason_code, quality_label, warnings_json, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (package_run_id, ric) DO UPDATE SET
                    package_date = EXCLUDED.package_date,
                    ticker = EXCLUDED.ticker,
                    price_on_package_date_status = EXCLUDED.price_on_package_date_status,
                    fit_row_status = EXCLUDED.fit_row_status,
                    fit_quality_status = EXCLUDED.fit_quality_status,
                    portfolio_use_status = EXCLUDED.portfolio_use_status,
                    ticker_detail_use_status = EXCLUDED.ticker_detail_use_status,
                    hedge_use_status = EXCLUDED.hedge_use_status,
                    fit_family = EXCLUDED.fit_family,
                    fit_status = EXCLUDED.fit_status,
                    reason_code = EXCLUDED.reason_code,
                    quality_label = EXCLUDED.quality_label,
                    warnings_json = EXCLUDED.warnings_json,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    (
                        row["package_run_id"],
                        row["package_date"],
                        row["ric"],
                        row.get("ticker"),
                        row["price_on_package_date_status"],
                        row["fit_row_status"],
                        row["fit_quality_status"],
                        row["portfolio_use_status"],
                        row["ticker_detail_use_status"],
                        row["hedge_use_status"],
                        row["fit_family"],
                        row["fit_status"],
                        row.get("reason_code"),
                        row["quality_label"],
                        _json_text(row.get("warnings", [])),
                        row["updated_at"],
                    )
                    for row in runtime_coverage
                ],
            )
    pg_conn.commit()
    return {"status": "ok"}
