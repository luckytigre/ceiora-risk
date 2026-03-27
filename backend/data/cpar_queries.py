"""Query helpers for durable cPAR reads."""

from __future__ import annotations

import json
from typing import Any, Callable

FetchRowsFn = Callable[[str, list[Any] | None], list[dict[str, Any]]]

_PACKAGE_SUCCESS_ORDER_BY = """
        package_date DESC,
        (completed_at IS NULL) ASC,
        completed_at DESC,
        updated_at DESC,
        package_run_id DESC
"""


class CparAmbiguousInstrumentFit(ValueError):
    """Raised when a ticker maps to multiple active cPAR fit rows."""


def _decode_json(raw: Any, *, default: Any) -> Any:
    if raw is None:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        decoded = json.loads(str(raw))
    except Exception:
        return default
    return decoded


def _normalize_package_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "package_run_id": str(row.get("package_run_id") or ""),
        "package_date": str(row.get("package_date") or ""),
        "profile": str(row.get("profile") or ""),
        "status": str(row.get("status") or ""),
        "started_at": str(row.get("started_at") or ""),
        "completed_at": str(row.get("completed_at") or "") or None,
        "method_version": str(row.get("method_version") or ""),
        "factor_registry_version": str(row.get("factor_registry_version") or ""),
        "lookback_weeks": int(row.get("lookback_weeks") or 0),
        "half_life_weeks": int(row.get("half_life_weeks") or 0),
        "min_observations": int(row.get("min_observations") or 0),
        "proxy_price_rule": str(row.get("proxy_price_rule") or ""),
        "source_prices_asof": str(row.get("source_prices_asof") or "") or None,
        "classification_asof": str(row.get("classification_asof") or "") or None,
        "universe_count": int(row.get("universe_count") or 0),
        "fit_ok_count": int(row.get("fit_ok_count") or 0),
        "fit_limited_count": int(row.get("fit_limited_count") or 0),
        "fit_insufficient_count": int(row.get("fit_insufficient_count") or 0),
        "data_authority": str(row.get("data_authority") or ""),
        "error_type": str(row.get("error_type") or "") or None,
        "error_message": str(row.get("error_message") or "") or None,
        "updated_at": str(row.get("updated_at") or ""),
    }


def _normalize_fit_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "package_date": str(row.get("package_date") or ""),
        "ric": str(row.get("ric") or ""),
        "ticker": str(row.get("ticker") or "") or None,
        "display_name": str(row.get("display_name") or "") or None,
        "fit_status": str(row.get("fit_status") or ""),
        "warnings": _decode_json(row.get("warnings_json"), default=[]),
        "observed_weeks": int(row.get("observed_weeks") or 0),
        "lookback_weeks": int(row.get("lookback_weeks") or 0),
        "longest_gap_weeks": int(row.get("longest_gap_weeks") or 0),
        "price_field_used": str(row.get("price_field_used") or ""),
        "hq_country_code": str(row.get("hq_country_code") or "") or None,
        "market_step_alpha": None if row.get("market_step_alpha") is None else float(row.get("market_step_alpha")),
        "market_step_beta": None if row.get("market_step_beta") is None else float(row.get("market_step_beta")),
        "block_alpha": None if row.get("block_alpha") is None else float(row.get("block_alpha")),
        "spy_trade_beta_raw": None if row.get("spy_trade_beta_raw") is None else float(row.get("spy_trade_beta_raw")),
        "raw_loadings": _decode_json(row.get("raw_loadings_json"), default={}),
        "thresholded_loadings": _decode_json(row.get("thresholded_loadings_json"), default={}),
        "factor_variance_proxy": None if row.get("factor_variance_proxy") is None else float(row.get("factor_variance_proxy")),
        "factor_volatility_proxy": None if row.get("factor_volatility_proxy") is None else float(row.get("factor_volatility_proxy")),
        "specific_variance_proxy": None if row.get("specific_variance_proxy") is None else float(row.get("specific_variance_proxy")),
        "specific_volatility_proxy": None if row.get("specific_volatility_proxy") is None else float(row.get("specific_volatility_proxy")),
        "universe_scope": str(row.get("universe_scope") or "") or None,
        "target_scope": str(row.get("target_scope") or "") or None,
        "basis_role": str(row.get("basis_role") or "") or None,
        "build_reason_code": str(row.get("build_reason_code") or "") or None,
        "price_on_package_date_status": str(row.get("price_on_package_date_status") or "") or None,
        "fit_row_status": str(row.get("fit_row_status") or "") or None,
        "fit_quality_status": str(row.get("fit_quality_status") or "") or None,
        "portfolio_use_status": str(row.get("portfolio_use_status") or "") or None,
        "ticker_detail_use_status": str(row.get("ticker_detail_use_status") or "") or None,
        "hedge_use_status": str(row.get("hedge_use_status") or "") or None,
        "fit_family": str(row.get("fit_family") or "") or None,
        "reason_code": str(row.get("reason_code") or "") or None,
        "quality_label": str(row.get("quality_label") or "") or None,
        "package_run_id": str(row.get("package_run_id") or ""),
        "updated_at": str(row.get("updated_at") or ""),
    }


def _normalize_search_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "package_date": str(row.get("package_date") or ""),
        "package_run_id": str(row.get("package_run_id") or ""),
        "ric": str(row.get("ric") or ""),
        "ticker": str(row.get("ticker") or "") or None,
        "display_name": str(row.get("display_name") or "") or None,
        "fit_status": str(row.get("fit_status") or ""),
        "warnings": _decode_json(row.get("warnings_json"), default=[]),
        "hq_country_code": str(row.get("hq_country_code") or "") or None,
        "target_scope": str(row.get("target_scope") or "") or None,
        "universe_scope": str(row.get("universe_scope") or "") or None,
        "basis_role": str(row.get("basis_role") or "") or None,
        "price_on_package_date_status": str(row.get("price_on_package_date_status") or "") or None,
        "fit_row_status": str(row.get("fit_row_status") or "") or None,
        "fit_quality_status": str(row.get("fit_quality_status") or "") or None,
        "portfolio_use_status": str(row.get("portfolio_use_status") or "") or None,
        "ticker_detail_use_status": str(row.get("ticker_detail_use_status") or "") or None,
        "hedge_use_status": str(row.get("hedge_use_status") or "") or None,
        "fit_family": str(row.get("fit_family") or "") or None,
        "reason_code": str(row.get("reason_code") or "") or None,
        "quality_label": str(row.get("quality_label") or "") or None,
        "updated_at": str(row.get("updated_at") or ""),
    }


def _normalize_filter_tokens(values: list[str] | tuple[str, ...]) -> list[str]:
    clean: list[str] = []
    seen: set[str] = set()
    for raw in values:
        token = str(raw or "").strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        clean.append(token)
    return clean


def latest_successful_package(fetch_rows: FetchRowsFn) -> dict[str, Any] | None:
    rows = fetch_rows(
        f"""
        SELECT *
        FROM cpar_package_runs
        WHERE status = ?
          AND EXISTS (
              SELECT 1
              FROM cpar_proxy_returns_weekly r
              WHERE r.package_run_id = cpar_package_runs.package_run_id
          )
          AND EXISTS (
              SELECT 1
              FROM cpar_proxy_transform_weekly t
              WHERE t.package_run_id = cpar_package_runs.package_run_id
          )
          AND EXISTS (
              SELECT 1
              FROM cpar_factor_covariance_weekly c
              WHERE c.package_run_id = cpar_package_runs.package_run_id
          )
          AND EXISTS (
              SELECT 1
              FROM cpar_instrument_fits_weekly f
              WHERE f.package_run_id = cpar_package_runs.package_run_id
          )
        ORDER BY {_PACKAGE_SUCCESS_ORDER_BY}
        LIMIT 1
        """,
        ["ok"],
    )
    if not rows:
        return None
    return _normalize_package_row(rows[0])


def active_package_covariance_rows(fetch_rows: FetchRowsFn, *, package_run_id: str) -> list[dict[str, Any]]:
    return package_covariance_rows(fetch_rows, package_run_id=package_run_id)


def successful_package_factor_return_rows(
    fetch_rows: FetchRowsFn,
    *,
    factor_id: str,
) -> list[dict[str, Any]]:
    clean_factor_id = str(factor_id or "").strip().upper()
    if not clean_factor_id:
        return []
    rows = fetch_rows(
        f"""
        SELECT
            r.factor_id,
            r.week_end,
            r.return_value,
            p.package_date,
            p.updated_at,
            p.package_run_id
        FROM cpar_proxy_returns_weekly r
        JOIN cpar_package_runs p
          ON p.package_run_id = r.package_run_id
        WHERE p.status = ?
          AND UPPER(COALESCE(r.factor_id, '')) = ?
        ORDER BY
            r.week_end DESC,
            p.package_date DESC,
            p.updated_at DESC,
            p.package_run_id DESC
        """,
        ["ok", clean_factor_id],
    )
    return [
        {
            "factor_id": str(row.get("factor_id") or ""),
            "week_end": str(row.get("week_end") or ""),
            "return_value": float(row.get("return_value") or 0.0),
            "package_date": str(row.get("package_date") or ""),
            "updated_at": str(row.get("updated_at") or ""),
            "package_run_id": str(row.get("package_run_id") or ""),
        }
        for row in rows
    ]


def package_proxy_return_rows(fetch_rows: FetchRowsFn, *, package_run_id: str) -> list[dict[str, Any]]:
    rows = fetch_rows(
        """
        SELECT
            factor_id,
            factor_group,
            week_end,
            return_value,
            weight_value,
            proxy_ric,
            proxy_ticker,
            package_run_id,
            package_date,
            updated_at
        FROM cpar_proxy_returns_weekly
        WHERE package_run_id = ?
        ORDER BY factor_id, week_end
        """,
        [str(package_run_id)],
    )
    return [
        {
            "factor_id": str(row.get("factor_id") or ""),
            "factor_group": str(row.get("factor_group") or ""),
            "week_end": str(row.get("week_end") or ""),
            "return_value": float(row.get("return_value") or 0.0),
            "weight_value": float(row.get("weight_value") or 0.0),
            "proxy_ric": str(row.get("proxy_ric") or ""),
            "proxy_ticker": str(row.get("proxy_ticker") or ""),
            "package_run_id": str(row.get("package_run_id") or ""),
            "package_date": str(row.get("package_date") or ""),
            "updated_at": str(row.get("updated_at") or ""),
        }
        for row in rows
    ]


def package_proxy_transform_rows(fetch_rows: FetchRowsFn, *, package_run_id: str) -> list[dict[str, Any]]:
    rows = fetch_rows(
        """
        SELECT
            factor_id,
            factor_group,
            market_alpha,
            market_beta,
            proxy_ric,
            proxy_ticker,
            package_run_id,
            package_date,
            updated_at
        FROM cpar_proxy_transform_weekly
        WHERE package_run_id = ?
        ORDER BY factor_id
        """,
        [str(package_run_id)],
    )
    return [
        {
            "factor_id": str(row.get("factor_id") or ""),
            "factor_group": str(row.get("factor_group") or ""),
            "market_alpha": float(row.get("market_alpha") or 0.0),
            "market_beta": float(row.get("market_beta") or 0.0),
            "proxy_ric": str(row.get("proxy_ric") or ""),
            "proxy_ticker": str(row.get("proxy_ticker") or ""),
            "package_run_id": str(row.get("package_run_id") or ""),
            "package_date": str(row.get("package_date") or ""),
            "updated_at": str(row.get("updated_at") or ""),
        }
        for row in rows
    ]


def package_covariance_rows(fetch_rows: FetchRowsFn, *, package_run_id: str) -> list[dict[str, Any]]:
    rows = fetch_rows(
        """
        SELECT factor_id, factor_id_2, covariance, correlation, package_run_id, updated_at
        FROM cpar_factor_covariance_weekly
        WHERE package_run_id = ?
        ORDER BY factor_id, factor_id_2
        """,
        [str(package_run_id)],
    )
    return [
        {
            "factor_id": str(row.get("factor_id") or ""),
            "factor_id_2": str(row.get("factor_id_2") or ""),
            "covariance": float(row.get("covariance") or 0.0),
            "correlation": float(row.get("correlation") or 0.0),
            "package_run_id": str(row.get("package_run_id") or ""),
            "updated_at": str(row.get("updated_at") or ""),
        }
        for row in rows
    ]


def active_package_search_rows(
    fetch_rows: FetchRowsFn,
    *,
    package_run_id: str,
    q: str,
) -> list[dict[str, Any]]:
    clean_q = str(q or "").strip().upper()
    if not clean_q:
        return []
    like = f"%{clean_q}%"
    rows = fetch_rows(
        """
        SELECT
            f.package_date,
            f.package_run_id,
            f.ric,
            f.ticker,
            f.display_name,
            f.fit_status,
            f.warnings_json,
            f.hq_country_code,
            m.universe_scope,
            m.target_scope,
            m.basis_role,
            rc.price_on_package_date_status,
            rc.fit_row_status,
            rc.fit_quality_status,
            rc.portfolio_use_status,
            rc.ticker_detail_use_status,
            rc.hedge_use_status,
            rc.fit_family,
            rc.reason_code,
            rc.quality_label,
            f.updated_at
        FROM cpar_instrument_fits_weekly f
        LEFT JOIN cpar_package_universe_membership m
          ON m.package_run_id = f.package_run_id
         AND UPPER(COALESCE(m.ric, '')) = UPPER(COALESCE(f.ric, ''))
        LEFT JOIN cpar_instrument_runtime_coverage_weekly rc
          ON rc.package_run_id = f.package_run_id
         AND UPPER(COALESCE(rc.ric, '')) = UPPER(COALESCE(f.ric, ''))
        WHERE f.package_run_id = ?
          AND (
              UPPER(COALESCE(f.ticker, '')) LIKE ?
              OR UPPER(COALESCE(f.display_name, '')) LIKE ?
              OR UPPER(COALESCE(f.ric, '')) LIKE ?
          )
        ORDER BY UPPER(COALESCE(f.ticker, '')), f.ric
        """,
        [str(package_run_id), like, like, like],
    )
    return [_normalize_search_row(row) for row in rows]


def active_package_instrument_fit(
    fetch_rows: FetchRowsFn,
    *,
    package_run_id: str,
    ticker: str,
    ric: str | None = None,
) -> dict[str, Any] | None:
    clean_ticker = str(ticker or "").strip().upper()
    if not clean_ticker:
        raise ValueError("ticker is required")
    params: list[Any] = [str(package_run_id), clean_ticker]
    sql = """
        SELECT
            f.*,
            m.universe_scope,
            m.target_scope,
            m.basis_role,
            m.build_reason_code,
            rc.price_on_package_date_status,
            rc.fit_row_status,
            rc.fit_quality_status,
            rc.portfolio_use_status,
            rc.ticker_detail_use_status,
            rc.hedge_use_status,
            rc.fit_family,
            rc.reason_code,
            rc.quality_label
        FROM cpar_instrument_fits_weekly f
        LEFT JOIN cpar_package_universe_membership m
          ON m.package_run_id = f.package_run_id
         AND UPPER(COALESCE(m.ric, '')) = UPPER(COALESCE(f.ric, ''))
        LEFT JOIN cpar_instrument_runtime_coverage_weekly rc
          ON rc.package_run_id = f.package_run_id
         AND UPPER(COALESCE(rc.ric, '')) = UPPER(COALESCE(f.ric, ''))
        WHERE f.package_run_id = ?
          AND UPPER(COALESCE(f.ticker, '')) = ?
    """
    if ric:
        sql += " AND UPPER(COALESCE(f.ric, '')) = ?"
        params.append(str(ric).strip().upper())
    sql += " ORDER BY f.ric"
    rows = fetch_rows(sql, params)
    if not rows:
        return None
    if len(rows) > 1 and not ric:
        raise CparAmbiguousInstrumentFit(f"Ambiguous cPAR instrument fit for ticker {clean_ticker}")
    return _normalize_fit_row(rows[0])


def package_instrument_fits_for_rics(
    fetch_rows: FetchRowsFn,
    *,
    package_run_id: str,
    rics: list[str] | tuple[str, ...],
) -> list[dict[str, Any]]:
    clean_rics = _normalize_filter_tokens(rics)
    if not clean_rics:
        return []
    placeholders = ",".join("?" for _ in clean_rics)
    rows = fetch_rows(
        f"""
        SELECT
            f.*,
            m.universe_scope,
            m.target_scope,
            m.basis_role,
            m.build_reason_code,
            rc.price_on_package_date_status,
            rc.fit_row_status,
            rc.fit_quality_status,
            rc.portfolio_use_status,
            rc.ticker_detail_use_status,
            rc.hedge_use_status,
            rc.fit_family,
            rc.reason_code,
            rc.quality_label
        FROM cpar_instrument_fits_weekly f
        LEFT JOIN cpar_package_universe_membership m
          ON m.package_run_id = f.package_run_id
         AND UPPER(COALESCE(m.ric, '')) = UPPER(COALESCE(f.ric, ''))
        LEFT JOIN cpar_instrument_runtime_coverage_weekly rc
          ON rc.package_run_id = f.package_run_id
         AND UPPER(COALESCE(rc.ric, '')) = UPPER(COALESCE(f.ric, ''))
        WHERE f.package_run_id = ?
          AND UPPER(COALESCE(f.ric, '')) IN ({placeholders})
        ORDER BY f.ric
        """,
        [str(package_run_id), *clean_rics],
    )
    return [_normalize_fit_row(row) for row in rows]


def previous_successful_instrument_fit(
    fetch_rows: FetchRowsFn,
    *,
    ric: str,
    before_package_date: str,
) -> dict[str, Any] | None:
    clean_ric = str(ric or "").strip().upper()
    if not clean_ric:
        raise ValueError("ric is required")
    rows = fetch_rows(
        f"""
        SELECT
            f.*,
            m.universe_scope,
            m.target_scope,
            m.basis_role,
            m.build_reason_code,
            rc.price_on_package_date_status,
            rc.fit_row_status,
            rc.fit_quality_status,
            rc.portfolio_use_status,
            rc.ticker_detail_use_status,
            rc.hedge_use_status,
            rc.fit_family,
            rc.reason_code,
            rc.quality_label
        FROM cpar_instrument_fits_weekly f
        JOIN cpar_package_runs p
          ON p.package_run_id = f.package_run_id
        LEFT JOIN cpar_package_universe_membership m
          ON m.package_run_id = f.package_run_id
         AND UPPER(COALESCE(m.ric, '')) = UPPER(COALESCE(f.ric, ''))
        LEFT JOIN cpar_instrument_runtime_coverage_weekly rc
          ON rc.package_run_id = f.package_run_id
         AND UPPER(COALESCE(rc.ric, '')) = UPPER(COALESCE(f.ric, ''))
        WHERE UPPER(COALESCE(f.ric, '')) = ?
          AND p.status = ?
          AND p.package_date < ?
        ORDER BY {_PACKAGE_SUCCESS_ORDER_BY.replace("package_date", "p.package_date").replace("completed_at", "p.completed_at").replace("updated_at", "p.updated_at").replace("package_run_id", "p.package_run_id")}
        LIMIT 1
        """,
        [clean_ric, "ok", str(before_package_date)],
    )
    if not rows:
        return None
    return _normalize_fit_row(rows[0])
