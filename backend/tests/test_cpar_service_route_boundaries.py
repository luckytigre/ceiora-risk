from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICE_FILES = [
    REPO_ROOT / "backend" / "services" / "cpar_meta_service.py",
    REPO_ROOT / "backend" / "services" / "cpar_search_service.py",
    REPO_ROOT / "backend" / "services" / "cpar_ticker_service.py",
    REPO_ROOT / "backend" / "services" / "cpar_hedge_service.py",
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


def test_cpar_services_do_not_import_api_layers() -> None:
    offenders: list[str] = []
    forbidden_prefixes = ("backend.api",)
    for path in SERVICE_FILES:
        imported = _imported_modules(path)
        if any(
            name == prefix or name.startswith(f"{prefix}.")
            for name in imported
            for prefix in forbidden_prefixes
        ):
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []


def test_cpar_routes_do_not_import_data_layers() -> None:
    offenders: list[str] = []
    forbidden_prefixes = ("backend.data",)
    for path in ROUTE_FILES:
        imported = _imported_modules(path)
        if any(
            name == prefix or name.startswith(f"{prefix}.")
            for name in imported
            for prefix in forbidden_prefixes
        ):
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []
