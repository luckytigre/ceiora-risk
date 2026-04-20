"""Read-only account-scoped cPAR portfolio hedge payload service."""

from __future__ import annotations

from backend.services import cpar_display_covariance, cpar_portfolio_snapshot_service

CparPortfolioAccountNotFound = cpar_portfolio_snapshot_service.CparPortfolioAccountNotFound

def load_cpar_portfolio_hedge_payload(
    *,
    account_id: str,
    mode: str,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    data_db=None,
) -> dict[str, object]:
    package, account, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_account_context(
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
        data_db=data_db,
    )
    if not positions:
        return cpar_portfolio_snapshot_service.build_cpar_portfolio_hedge_snapshot(
            package=package,
            account=account,
            positions=[],
            mode=mode,
            fit_by_ric={},
            price_by_ric={},
            classification_by_ric={},
            covariance_rows=[],
        )
    rics = [str(row.get("ric") or "") for row in positions if str(row.get("ric") or "").strip()]
    fit_by_ric, price_by_ric, classification_by_ric, covariance_rows = (
        cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
            rics=rics,
            package_run_id=str(package["package_run_id"]),
            package_date=str(package["package_date"]),
            positions=positions,
            data_db=data_db,
        )
    )
    try:
        display_covariance_rows = cpar_display_covariance.load_package_display_covariance_rows(
            package_run_id=str(package["package_run_id"]),
            data_db=data_db,
        )
    except Exception:
        display_covariance_rows = None
    return cpar_portfolio_snapshot_service.build_cpar_portfolio_hedge_snapshot(
        package=package,
        account=account,
        positions=positions,
        mode=mode,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
        display_covariance_rows=display_covariance_rows,
    )
