from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICE_FILES = [
    REPO_ROOT / "backend" / "services" / "cpar_meta_service.py",
    REPO_ROOT / "backend" / "services" / "cpar_search_service.py",
    REPO_ROOT / "backend" / "services" / "cpar_ticker_service.py",
    REPO_ROOT / "backend" / "services" / "cpar_hedge_service.py",
    REPO_ROOT / "backend" / "services" / "cpar_portfolio_hedge_service.py",
]
ROUTE_FILES = [
    REPO_ROOT / "backend" / "api" / "routes" / "cpar.py",
]


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(str(alias.name))
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(str(node.module))
    return modules


def _non_cpar_service_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = str(alias.name)
                if name.startswith("backend.services.") and not name.startswith("backend.services.cpar_"):
                    offenders.append(name)
        elif isinstance(node, ast.ImportFrom) and node.module == "backend.services":
            for alias in node.names:
                if not str(alias.name).startswith("cpar_"):
                    offenders.append(f"{node.module}.{alias.name}")
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("backend.services.") and not node.module.startswith("backend.services.cpar_"):
            offenders.append(str(node.module))
    return sorted(set(offenders))


def test_cpar_services_do_not_import_api_layers() -> None:
    offenders: list[str] = []
    forbidden_prefixes = ("backend.api", "backend.orchestration", "frontend", "fastapi")
    for path in SERVICE_FILES:
        imported = _imported_modules(path)
        if any(
            name == prefix or name.startswith(f"{prefix}.")
            for name in imported
            for prefix in forbidden_prefixes
        ):
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []


def test_cpar_services_only_import_cpar_service_modules() -> None:
    offenders: list[str] = []
    for path in SERVICE_FILES:
        bad_service_imports = _non_cpar_service_imports(path)
        if bad_service_imports:
            offenders.append(f"{path.relative_to(REPO_ROOT)} -> {', '.join(bad_service_imports)}")
    assert offenders == []


def test_cpar_routes_do_not_import_data_layers() -> None:
    offenders: list[str] = []
    forbidden_prefixes = ("backend.data", "backend.cpar", "frontend")
    for path in ROUTE_FILES:
        imported = _imported_modules(path)
        if any(
            name == prefix or name.startswith(f"{prefix}.")
            for name in imported
            for prefix in forbidden_prefixes
        ):
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []
