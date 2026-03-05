from __future__ import annotations

from pathlib import Path
import sqlite3

from backend import config
from backend.data import sqlite as cache_sqlite


def test_cache_snapshot_publish_switches_atomically(monkeypatch, tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    monkeypatch.setattr(config, "SQLITE_PATH", str(cache_db))
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY", False)
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY_PATH", None)

    cache_sqlite.cache_set("portfolio", {"version": "old"})
    assert cache_sqlite.cache_get("portfolio") == {"version": "old"}

    cache_sqlite.cache_set("portfolio", {"version": "new"}, snapshot_id="run_1")
    # Before publish, reads should still resolve to active (old) payload.
    assert cache_sqlite.cache_get("portfolio") == {"version": "old"}

    cache_sqlite.cache_publish_snapshot("run_1")
    assert cache_sqlite.cache_get("portfolio") == {"version": "new"}


def test_cache_snapshot_falls_back_to_base_when_key_missing(monkeypatch, tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    monkeypatch.setattr(config, "SQLITE_PATH", str(cache_db))
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY", False)
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY_PATH", None)

    cache_sqlite.cache_set("risk", {"v": "base"})
    cache_sqlite.cache_set("portfolio", {"v": "snap"}, snapshot_id="run_2")
    cache_sqlite.cache_publish_snapshot("run_2")

    assert cache_sqlite.cache_get("portfolio") == {"v": "snap"}
    # Missing key in active snapshot should continue to read base key.
    assert cache_sqlite.cache_get("risk") == {"v": "base"}


def test_cache_snapshot_prunes_old_snapshots(monkeypatch, tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    monkeypatch.setattr(config, "SQLITE_PATH", str(cache_db))
    monkeypatch.setattr(config, "SQLITE_CACHE_SNAPSHOT_RETENTION", 1)
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY", False)
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY_PATH", None)

    cache_sqlite.cache_set("portfolio", {"v": "one"}, snapshot_id="run_1")
    cache_sqlite.cache_publish_snapshot("run_1")
    cache_sqlite.cache_set("portfolio", {"v": "two"}, snapshot_id="run_2")
    cache_sqlite.cache_publish_snapshot("run_2")

    assert cache_sqlite.cache_get("portfolio") == {"v": "two"}

    # run_1 staged keys should be pruned after publishing run_2 with retention=1.
    conn = sqlite3.connect(str(cache_db))
    try:
        run1_rows = conn.execute(
            "SELECT COUNT(*) FROM cache WHERE key LIKE ?",
            ("__snap__:run_1:%",),
        ).fetchone()[0]
        run2_rows = conn.execute(
            "SELECT COUNT(*) FROM cache WHERE key LIKE ?",
            ("__snap__:run_2:%",),
        ).fetchone()[0]
    finally:
        conn.close()
    assert int(run1_rows) == 0
    assert int(run2_rows) > 0
