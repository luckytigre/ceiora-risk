from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_SRC_DIR = REPO_ROOT / "frontend" / "src"

ALLOWED_USE_API_PREFIXES: tuple[str, ...] = ()
ALLOWED_USE_API_EXACT = {
    "hooks/useApi.ts",
    "hooks/useCuse4Api.ts",
    "hooks/useCparApi.ts",
}

ALLOWED_LIB_API_EXACT = {
    "hooks/useApi.ts",
    "lib/api.ts",
    "lib/cparApi.ts",
    "lib/cuse4Api.ts",
    "lib/refresh.ts",
}

ALLOWED_LIB_TYPES_PREFIXES: tuple[str, ...] = ()
ALLOWED_LIB_TYPES_EXACT = {
    "hooks/useApi.ts",
    "lib/analyticsTruth.ts",
    "lib/refresh.ts",
    "lib/types.ts",
}

CPAR_FRONTEND_PREFIXES = (
    "app/cpar/",
    "features/cpar/",
)
CPAR_FRONTEND_EXACT = {
    "hooks/useCparApi.ts",
    "lib/cparApi.ts",
    "lib/cparTruth.ts",
}

FORBIDDEN_CUSE4_IMPORTS = (
    "@/hooks/useCuse4Api",
    "@/lib/cuse4Api",
    "@/lib/cuse4Refresh",
    "@/lib/cuse4Truth",
    "@/lib/types/cuse4",
)

FORBIDDEN_CPAR_FEATURE_OWNER_IMPORTS = (
    "@/features/cuse4/",
    "@/features/explore/",
    "@/features/whatif/",
)

ROUTE_CUSE4_IMPORT_EXPECTATIONS = {
    "backend/api/routes/exposures.py": (
        "cuse4_dashboard_payload_service",
        "cuse4_factor_history_service",
    ),
    "backend/api/routes/risk.py": (
        "cuse4_dashboard_payload_service",
    ),
    "backend/api/routes/portfolio.py": (
        "cuse4_dashboard_payload_service",
        "cuse4_holdings_service",
        "cuse4_portfolio_whatif",
    ),
    "backend/api/routes/universe.py": (
        "cuse4_universe_service",
    ),
    "backend/api/routes/health.py": (
        "cuse4_health_diagnostics_service",
    ),
    "backend/api/routes/holdings.py": (
        "cuse4_holdings_service",
    ),
    "backend/api/routes/operator.py": (
        "cuse4_operator_status_service",
    ),
}

ROOT_CUSE4_COMPONENT_IMPORTS = (
    "@/components/ApiErrorState",
    "@/components/CovarianceHeatmap",
    "@/components/ExposureBarChart",
    "@/components/ExposurePositionsTable",
    "@/components/FactorDrilldown",
    "@/components/FactorHistoryChart",
    "@/components/RiskDecompChart",
    "@/components/TickerWeeklyPriceChart",
)

ALLOWED_ROOT_CUSE4_COMPONENT_PREFIXES = (
    "components/",
    "features/cuse4/components/",
)
ALLOWED_ROOT_CUSE4_COMPONENT_EXACT = {
    "features/cpar/components/CparCovarianceHeatmap.tsx",
    "features/cpar/components/CparFactorHistoryChart.tsx",
    "features/cpar/components/CparExposureBarChart.tsx",
    "features/cpar/components/CparTickerPriceChart.tsx",
}


def _ts_files() -> list[Path]:
    return sorted(
        path
        for path in FRONTEND_SRC_DIR.rglob("*")
        if path.suffix in {".ts", ".tsx"}
    )


def _rel(path: Path) -> str:
    return str(path.relative_to(FRONTEND_SRC_DIR))


def _contains_import(path: Path, token: str) -> bool:
    text = path.read_text(encoding="utf-8")
    return (
        f'"{token}"' in text
        or f"'{token}'" in text
    )


def _allowed(relative_path: str, *, prefixes: tuple[str, ...], exact: set[str]) -> bool:
    return relative_path in exact or any(relative_path.startswith(prefix) for prefix in prefixes)


def test_mixed_use_api_imports_are_limited_to_cpar_and_compatibility_layers() -> None:
    offenders = [
        _rel(path)
        for path in _ts_files()
        if _contains_import(path, "@/hooks/useApi")
        and not _allowed(_rel(path), prefixes=ALLOWED_USE_API_PREFIXES, exact=ALLOWED_USE_API_EXACT)
    ]
    assert offenders == []


def test_mixed_api_path_imports_are_limited_to_compatibility_layers() -> None:
    offenders = [
        _rel(path)
        for path in _ts_files()
        if _contains_import(path, "@/lib/api")
        and not _allowed(_rel(path), prefixes=(), exact=ALLOWED_LIB_API_EXACT)
    ]
    assert offenders == []


def test_mixed_type_barrel_imports_are_limited_to_cpar_and_compatibility_layers() -> None:
    offenders = [
        _rel(path)
        for path in _ts_files()
        if _contains_import(path, "@/lib/types")
        and not _allowed(_rel(path), prefixes=ALLOWED_LIB_TYPES_PREFIXES, exact=ALLOWED_LIB_TYPES_EXACT)
    ]
    assert offenders == []


def test_generic_cuse4_truth_and_refresh_helpers_are_only_imported_via_cuse4_wrappers() -> None:
    truth_offenders = [
        _rel(path)
        for path in _ts_files()
        if _contains_import(path, "@/lib/analyticsTruth")
        and _rel(path) != "lib/cuse4Truth.ts"
    ]
    refresh_offenders = [
        _rel(path)
        for path in _ts_files()
        if _contains_import(path, "@/lib/refresh")
        and _rel(path) != "lib/cuse4Refresh.ts"
    ]
    assert truth_offenders == []
    assert refresh_offenders == []


def test_cpar_frontend_surfaces_do_not_import_cuse4_specific_helpers() -> None:
    offenders: list[str] = []
    for path in _ts_files():
        relative_path = _rel(path)
        if not _allowed(relative_path, prefixes=CPAR_FRONTEND_PREFIXES, exact=CPAR_FRONTEND_EXACT):
            continue
        text = path.read_text(encoding="utf-8")
        if any(token in text for token in FORBIDDEN_CUSE4_IMPORTS):
            offenders.append(relative_path)
    assert offenders == []


def test_cpar_frontend_surfaces_do_not_import_cuse4_feature_owners() -> None:
    offenders: list[str] = []
    for path in _ts_files():
        relative_path = _rel(path)
        if not _allowed(relative_path, prefixes=CPAR_FRONTEND_PREFIXES, exact=CPAR_FRONTEND_EXACT):
            continue
        text = path.read_text(encoding="utf-8")
        if any(token in text for token in FORBIDDEN_CPAR_FEATURE_OWNER_IMPORTS):
            offenders.append(relative_path)
    assert offenders == []


def test_root_cuse4_component_imports_are_limited_to_components_and_cuse4_wrappers() -> None:
    offenders: list[str] = []
    for path in _ts_files():
        relative_path = _rel(path)
        if _allowed(relative_path, prefixes=ALLOWED_ROOT_CUSE4_COMPONENT_PREFIXES, exact=ALLOWED_ROOT_CUSE4_COMPONENT_EXACT):
            continue
        text = path.read_text(encoding="utf-8")
        if any(token in text for token in ROOT_CUSE4_COMPONENT_IMPORTS):
            offenders.append(relative_path)
    assert offenders == []


def test_default_cuse4_routes_use_explicit_cuse4_service_aliases() -> None:
    offenders: list[str] = []
    for rel_path, tokens in ROUTE_CUSE4_IMPORT_EXPECTATIONS.items():
        path = REPO_ROOT / rel_path
        text = path.read_text(encoding="utf-8")
        if any(token not in text for token in tokens):
            offenders.append(rel_path)
    assert offenders == []
