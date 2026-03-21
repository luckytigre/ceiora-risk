"""Read-only aggregate cPAR risk payload service."""

from __future__ import annotations

from backend.services import cpar_portfolio_snapshot_service


def load_cpar_risk_payload(
    *,
    data_db=None,
) -> dict[str, object]:
    package, accounts, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_aggregate_context(
        data_db=data_db,
    )
    rics = [str(row.get("ric") or "") for row in positions if str(row.get("ric") or "").strip()]
    fit_by_ric, price_by_ric, classification_by_ric, covariance_rows = cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
        rics=rics,
        package_run_id=str(package["package_run_id"]),
        package_date=str(package["package_date"]),
        data_db=data_db,
    )
    return cpar_portfolio_snapshot_service.build_cpar_risk_snapshot(
        package=package,
        accounts=accounts,
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
    )
