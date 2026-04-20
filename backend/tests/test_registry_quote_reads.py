from __future__ import annotations

from pathlib import Path

from backend.data import registry_quote_reads


def test_registry_quote_reads_caches_table_inventory(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []
    registry_quote_reads._cached_available_tables.cache_clear()

    monkeypatch.setattr(registry_quote_reads.core_backend, "use_neon_core_reads", lambda: False)

    def _table_exists(table: str, *, fetch_rows_fn, neon_enabled: bool) -> bool:
        calls.append(table)
        return True

    monkeypatch.setattr(registry_quote_reads.core_backend, "table_exists", _table_exists)

    data_db = tmp_path / "quotes.db"
    first = registry_quote_reads._ensure_required_tables(data_db=data_db)
    second = registry_quote_reads._ensure_required_tables(data_db=data_db)

    assert first == second
    expected_probe_count = len(registry_quote_reads._REQUIRED_TABLES) + len(registry_quote_reads._OPTIONAL_TABLES)
    assert len(calls) == expected_probe_count


def test_registry_quote_reads_invalidates_cache_when_file_revision_changes(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []
    registry_quote_reads._cached_available_tables.cache_clear()

    monkeypatch.setattr(registry_quote_reads.core_backend, "use_neon_core_reads", lambda: False)

    def _table_exists(table: str, *, fetch_rows_fn, neon_enabled: bool) -> bool:
        calls.append(table)
        return True

    monkeypatch.setattr(registry_quote_reads.core_backend, "table_exists", _table_exists)

    data_db = tmp_path / "quotes.db"
    data_db.write_text("v1", encoding="utf-8")
    registry_quote_reads._ensure_required_tables(data_db=data_db)

    first_probe_count = len(registry_quote_reads._REQUIRED_TABLES) + len(registry_quote_reads._OPTIONAL_TABLES)
    assert len(calls) == first_probe_count

    data_db.write_text("v2-more-bytes", encoding="utf-8")
    registry_quote_reads._ensure_required_tables(data_db=data_db)

    assert len(calls) == first_probe_count * 2
