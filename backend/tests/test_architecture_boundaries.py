from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ROUTES_DIR = REPO_ROOT / "backend" / "api" / "routes"
OPERATOR_STATUS_SERVICE = REPO_ROOT / "backend" / "services" / "operator_status_service.py"
CUSE4_OPERATOR_STATUS_SERVICE = REPO_ROOT / "backend" / "services" / "cuse4_operator_status_service.py"
BACKEND_DIR = REPO_ROOT / "backend"
ALLOWED_VAGUE_MODULES = {
    "refresh_manager.py",
}
SERVING_WRITE_SCAN_DIRS = (
    BACKEND_DIR / "api",
    BACKEND_DIR / "services",
    BACKEND_DIR / "analytics",
    BACKEND_DIR / "orchestration",
)
FORBIDDEN_PRICE_HISTORY_WRITE_PATTERNS = (
    "INSERT INTO security_prices_eod",
    "INSERT OR REPLACE INTO security_prices_eod",
    "REPLACE INTO security_prices_eod",
    "UPDATE security_prices_eod",
    "DELETE FROM security_prices_eod",
)


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


def test_route_modules_do_not_import_backend_data_directly() -> None:
    offenders: list[str] = []
    for path in sorted(ROUTES_DIR.glob("*.py")):
        imported = _imported_modules(path)
        if any(name == "backend.data" or name.startswith("backend.data.") for name in imported):
            offenders.append(path.name)
    assert offenders == []


def test_operator_status_services_do_not_import_run_model_pipeline() -> None:
    for path in (CUSE4_OPERATOR_STATUS_SERVICE, OPERATOR_STATUS_SERVICE):
        imported = _imported_modules(path)
        assert "backend.orchestration.run_model_pipeline" not in imported


def test_backend_does_not_add_new_vague_module_names() -> None:
    offenders: list[str] = []
    for pattern in ("shared.py", "common.py", "*manager.py"):
        for path in sorted(BACKEND_DIR.rglob(pattern)):
            if ".venv" in path.parts:
                continue
            if path.name in ALLOWED_VAGUE_MODULES:
                continue
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []


def test_serving_and_orchestration_layers_do_not_write_canonical_price_history() -> None:
    offenders: list[str] = []
    for root in SERVING_WRITE_SCAN_DIRS:
        for path in sorted(root.rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            if any(pattern in text for pattern in FORBIDDEN_PRICE_HISTORY_WRITE_PATTERNS):
                offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []
