from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_AUTHORITY = REPO_ROOT / "backend" / "universe" / "runtime_authority.py"
RUNTIME_ROWS = REPO_ROOT / "backend" / "universe" / "runtime_rows.py"
FORBIDDEN_RUNTIME_AUTHORITY_TOKENS = (
    "security_classification_pit",
    "SECURITY_MASTER_COMPAT_CURRENT_TABLE",
    "SECURITY_MASTER_TABLE",
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


def test_runtime_authority_does_not_import_runtime_rows() -> None:
    imported = _imported_modules(RUNTIME_AUTHORITY)
    assert "backend.universe.runtime_rows" not in imported


def test_runtime_authority_stays_limited_to_current_table_loading() -> None:
    text = RUNTIME_AUTHORITY.read_text(encoding="utf-8")
    assert all(token not in text for token in FORBIDDEN_RUNTIME_AUTHORITY_TOKENS)


def test_runtime_rows_remains_the_public_runtime_owner() -> None:
    text = RUNTIME_ROWS.read_text(encoding="utf-8")
    assert "def load_security_runtime_rows(" in text
    assert "load_runtime_authority_state(" in text
