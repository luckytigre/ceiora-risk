"""Read-only aggregate cPAR risk payload service."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from backend.data import cpar_outputs
from backend.services import cpar_display_covariance, cpar_meta_service, cpar_portfolio_snapshot_service


def build_cpar_risk_snapshot(
    *,
    package: dict[str, object],
    accounts: list[dict[str, object]],
    positions: list[dict[str, object]],
    fit_by_ric: dict[str, dict[str, object]],
    price_by_ric: dict[str, dict[str, object]],
    classification_by_ric: dict[str, dict[str, object]],
    covariance_rows: list[dict[str, object]],
    display_covariance_rows: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    position_count = int(len(positions))
    resolved_display_covariance_rows = list(display_covariance_rows or covariance_rows)

    base_payload: dict[str, object] = {
        **cpar_meta_service.package_meta_payload(package),
        "scope": "all_accounts",
        "accounts_count": int(len(accounts)),
        "portfolio_status": "empty",
        "portfolio_reason": "No live holdings positions are loaded across any account.",
        "positions_count": position_count,
        "covered_positions_count": 0,
        "excluded_positions_count": 0,
        "gross_market_value": 0.0,
        "net_market_value": 0.0,
        "covered_gross_market_value": 0.0,
        "coverage_ratio": None,
        "coverage_breakdown": cpar_portfolio_snapshot_service._coverage_breakdown([]),
        "aggregate_thresholded_loadings": [],
        "aggregate_display_loadings": [],
        "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0},
        "vol_scaled_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 100.0},
        "factor_variance_contributions": [],
        "display_factor_variance_contributions": [],
        "factor_chart": [],
        "display_factor_chart": [],
        "cov_matrix": cpar_portfolio_snapshot_service._cov_matrix_payload(
            covariance_rows=covariance_rows,
        ),
        "display_cov_matrix": cpar_portfolio_snapshot_service._cov_matrix_payload(
            covariance_rows=resolved_display_covariance_rows,
        ),
        "factor_variance_proxy": 0.0,
        "idio_variance_proxy": 0.0,
        "total_variance_proxy": 0.0,
        "positions": [],
    }
    if not positions:
        return base_payload

    provisional_rows = cpar_portfolio_snapshot_service._build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=0.0,
    )
    display_loadings_by_ric = cpar_portfolio_snapshot_service._display_loadings_by_ric(fit_by_ric)
    aggregate_loadings, covered_gross_market_value, net_market_value = cpar_portfolio_snapshot_service._aggregate_loadings(
        provisional_rows,
        loadings_by_ric={str(ric): dict((fit or {}).get("thresholded_loadings") or {}) for ric, fit in fit_by_ric.items()},
    )
    aggregate_display_loadings, _, _ = cpar_portfolio_snapshot_service._aggregate_loadings(
        provisional_rows,
        loadings_by_ric=display_loadings_by_ric,
    )
    position_rows = cpar_portfolio_snapshot_service._build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=covered_gross_market_value,
    )
    position_rows = cpar_portfolio_snapshot_service._attach_thresholded_contributions(
        position_rows,
        fit_by_ric=fit_by_ric,
    )
    position_rows = cpar_portfolio_snapshot_service._attach_display_contributions(
        position_rows,
        display_loadings_by_ric=display_loadings_by_ric,
    )
    idio_contribution_by_ric, idio_variance_proxy = cpar_portfolio_snapshot_service._specific_risk_contributions(
        position_rows,
    )
    factor_variance_rows = cpar_portfolio_snapshot_service._factor_variance_contribution_rows(
        aggregate_loadings,
        covariance_rows=resolved_display_covariance_rows,
    )
    factor_variance_proxy = cpar_portfolio_snapshot_service._factor_variance_total(factor_variance_rows)
    total_variance_proxy = float(factor_variance_proxy + idio_variance_proxy)
    factor_variance_rows = cpar_portfolio_snapshot_service._factor_variance_contribution_rows(
        aggregate_loadings,
        covariance_rows=resolved_display_covariance_rows,
        total_variance=total_variance_proxy,
    )
    display_factor_variance_rows = cpar_portfolio_snapshot_service._factor_variance_contribution_rows(
        aggregate_display_loadings,
        covariance_rows=resolved_display_covariance_rows,
    )
    display_total_variance_proxy = float(cpar_portfolio_snapshot_service._factor_variance_total(display_factor_variance_rows) + idio_variance_proxy)
    display_factor_variance_rows = cpar_portfolio_snapshot_service._factor_variance_contribution_rows(
        aggregate_display_loadings,
        covariance_rows=resolved_display_covariance_rows,
        total_variance=display_total_variance_proxy,
    )
    position_rows = cpar_portfolio_snapshot_service._attach_risk_mix(
        position_rows,
        aggregate_loadings=aggregate_loadings,
        covariance_rows=resolved_display_covariance_rows,
        contribution_field="thresholded_contributions",
        idio_contribution_by_ric=idio_contribution_by_ric,
        total_variance=total_variance_proxy,
    )

    priced_gross_market_value = sum(
        abs(float(row["market_value"]))
        for row in position_rows
        if row.get("market_value") is not None
    )
    covered_positions_count = sum(1 for row in position_rows if str(row["coverage"]) == "covered")
    excluded_positions_count = len(position_rows) - covered_positions_count
    coverage_ratio = None
    if priced_gross_market_value > 0:
        coverage_ratio = covered_gross_market_value / priced_gross_market_value

    payload = {
        **base_payload,
        "positions_count": int(len(position_rows)),
        "covered_positions_count": int(covered_positions_count),
        "excluded_positions_count": int(excluded_positions_count),
        "gross_market_value": float(priced_gross_market_value),
        "net_market_value": float(net_market_value),
        "covered_gross_market_value": float(covered_gross_market_value),
        "coverage_ratio": coverage_ratio,
        "coverage_breakdown": cpar_portfolio_snapshot_service._coverage_breakdown(position_rows),
        "positions": position_rows,
    }

    if covered_positions_count <= 0 or covered_gross_market_value <= 0:
        payload["portfolio_status"] = "unavailable"
        payload["portfolio_reason"] = (
            "No aggregated holdings rows across all accounts have both price coverage and a usable persisted cPAR fit "
            "in the active package."
        )
        return payload

    payload.update(
        {
            "portfolio_status": "partial" if excluded_positions_count > 0 else "ok",
            "portfolio_reason": (
                "Some aggregated holdings rows were excluded because they lack price coverage or a usable persisted "
                "cPAR fit."
                if excluded_positions_count > 0
                else None
            ),
            **cpar_portfolio_snapshot_service._factor_analytics_payload(
                aggregate_loadings=aggregate_loadings,
                position_rows=position_rows,
                covariance_rows=resolved_display_covariance_rows,
                contribution_field="thresholded_contributions",
                total_variance=total_variance_proxy,
            ),
            "aggregate_display_loadings": cpar_portfolio_snapshot_service._factor_rows(aggregate_display_loadings),
            "risk_shares": cpar_portfolio_snapshot_service._risk_share_payload(
                factor_variance_rows,
                idio_variance_proxy=idio_variance_proxy,
                total_variance_proxy=total_variance_proxy,
            ),
            "vol_scaled_shares": cpar_portfolio_snapshot_service._vol_scaled_share_payload(
                aggregate_display_loadings,
                covariance_rows=covariance_rows,
                idio_variance_proxy=idio_variance_proxy,
            ),
            "factor_variance_proxy": float(factor_variance_proxy),
            "idio_variance_proxy": float(idio_variance_proxy),
            "total_variance_proxy": float(total_variance_proxy),
            "factor_variance_contributions": factor_variance_rows,
            "display_factor_variance_contributions": display_factor_variance_rows,
            "display_factor_chart": cpar_portfolio_snapshot_service._factor_chart_rows(
                loadings=aggregate_display_loadings,
                variance_rows=display_factor_variance_rows,
                position_rows=position_rows,
                covariance_rows=resolved_display_covariance_rows,
                contribution_field="display_contributions",
                total_variance=display_total_variance_proxy,
            ),
        }
    )
    return payload


def load_cpar_risk_payload(
    *,
    data_db=None,
) -> dict[str, object]:
    package, accounts, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_aggregate_context(
        data_db=data_db,
    )
    rics = [str(row.get("ric") or "") for row in positions if str(row.get("ric") or "").strip()]
    with ThreadPoolExecutor(max_workers=2) as executor:
        display_covariance_future = executor.submit(
            cpar_display_covariance.load_package_display_covariance_rows,
            package_run_id=str(package["package_run_id"]),
            data_db=data_db,
        )
        support_rows_future = executor.submit(
            cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows,
            rics=rics,
            package_run_id=str(package["package_run_id"]),
            package_date=str(package["package_date"]),
            data_db=data_db,
        )
        try:
            display_covariance_rows = display_covariance_future.result()
        except cpar_outputs.CparPackageNotReady as exc:
            raise cpar_meta_service.CparReadNotReady(str(exc)) from exc
        except cpar_outputs.CparAuthorityReadError as exc:
            raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc

        fit_by_ric, price_by_ric, classification_by_ric, covariance_rows = support_rows_future.result()

    return build_cpar_risk_snapshot(
        package=package,
        accounts=accounts,
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
        display_covariance_rows=display_covariance_rows,
    )
