from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_READ_AUTHORITY = REPO_ROOT / "backend" / "data" / "source_read_authority.py"
SOURCE_READS = REPO_ROOT / "backend" / "data" / "source_reads.py"
CORE_READS = REPO_ROOT / "backend" / "data" / "core_reads.py"
FORBIDDEN_SOURCE_READ_AUTHORITY_TOKENS = (
    "load_security_runtime_rows",
    "_LATEST_PRICES_TABLE",
    "security_master_compat_current",
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


def test_source_read_authority_does_not_import_core_reads() -> None:
    imported = _imported_modules(SOURCE_READ_AUTHORITY)
    assert "backend.data.core_reads" not in imported
    assert "backend.universe.runtime_rows" not in imported


def test_core_reads_continues_to_depend_on_source_reads_instead_of_lower_authority_module() -> None:
    imported = _imported_modules(CORE_READS)
    assert "backend.data.source_read_authority" not in imported
    text = CORE_READS.read_text(encoding="utf-8")
    assert "source_reads" in text


def test_source_reads_is_the_boundary_module_for_source_read_authority() -> None:
    text = SOURCE_READS.read_text(encoding="utf-8")
    assert "source_read_authority" in text


def test_source_read_authority_stays_limited_to_registry_first_gate_logic() -> None:
    text = SOURCE_READ_AUTHORITY.read_text(encoding="utf-8")
    assert all(token not in text for token in FORBIDDEN_SOURCE_READ_AUTHORITY_TOKENS)
