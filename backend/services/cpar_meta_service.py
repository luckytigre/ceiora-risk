"""Read-only cPAR meta payload service."""

from __future__ import annotations

from backend.cpar.factor_registry import serialize_factor_registry
from backend.data import cpar_outputs


class CparReadNotReady(RuntimeError):
    """Raised when no successful cPAR package is available for read surfaces."""


class CparReadUnavailable(RuntimeError):
    """Raised when the authoritative cPAR read path is unavailable."""


class CparTickerNotFound(LookupError):
    """Raised when a requested ticker/ric is not present in the active cPAR package."""


class CparTickerAmbiguous(ValueError):
    """Raised when a ticker maps to multiple active cPAR rows and ric is required."""


def require_active_package(*, data_db=None) -> dict[str, object]:
    try:
        return cpar_outputs.require_active_package_run(data_db=data_db)
    except cpar_outputs.CparPackageNotReady as exc:
        raise CparReadNotReady(str(exc)) from exc
    except cpar_outputs.CparAuthorityReadError as exc:
        raise CparReadUnavailable(str(exc)) from exc


def package_meta_payload(package: dict[str, object]) -> dict[str, object]:
    return {
        "package_run_id": str(package["package_run_id"]),
        "package_date": str(package["package_date"]),
        "profile": str(package["profile"]),
        "method_version": str(package["method_version"]),
        "factor_registry_version": str(package["factor_registry_version"]),
        "data_authority": str(package["data_authority"]),
        "lookback_weeks": int(package["lookback_weeks"]),
        "half_life_weeks": int(package["half_life_weeks"]),
        "min_observations": int(package["min_observations"]),
        "source_prices_asof": package.get("source_prices_asof"),
        "classification_asof": package.get("classification_asof"),
        "universe_count": int(package["universe_count"]),
        "fit_ok_count": int(package["fit_ok_count"]),
        "fit_limited_count": int(package["fit_limited_count"]),
        "fit_insufficient_count": int(package["fit_insufficient_count"]),
    }


def factor_registry_payload() -> list[dict[str, object]]:
    return serialize_factor_registry()


def load_cpar_meta_payload(*, data_db=None) -> dict[str, object]:
    package = require_active_package(data_db=data_db)
    factors = factor_registry_payload()
    return {
        **package_meta_payload(package),
        "factor_count": len(factors),
        "factors": factors,
    }
