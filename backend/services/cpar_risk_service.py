"""Read-only aggregate cPAR risk payload service."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from backend.data import cpar_outputs
from backend.services import cpar_display_covariance, cpar_meta_service, cpar_portfolio_snapshot_service


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

    return cpar_portfolio_snapshot_service.build_cpar_risk_snapshot(
        package=package,
        accounts=accounts,
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
        display_covariance_rows=display_covariance_rows,
    )
