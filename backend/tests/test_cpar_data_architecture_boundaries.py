from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CPAR_DATA_FILES = sorted((REPO_ROOT / "backend" / "data").glob("cpar_*.py"))
FORBIDDEN_PREFIXES = (
    "backend.api",
    "backend.services",
    "backend.orchestration",
    "frontend",
    "fastapi",
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


def test_cpar_data_modules_do_not_import_upper_layers() -> None:
    offenders: list[str] = []

    for path in CPAR_DATA_FILES:
        imported = _imported_modules(path)
        if any(
            name == prefix or name.startswith(f"{prefix}.")
            for name in imported
            for prefix in FORBIDDEN_PREFIXES
        ):
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []
