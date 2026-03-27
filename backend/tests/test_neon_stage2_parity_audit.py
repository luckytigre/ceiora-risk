from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.services import neon_stage2


class _DummyPgConn:
    def close(self) -> None:
        return None


def test_run_parity_audit_skips_orphan_check_for_tables_without_ric(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE serving_payload_current (
            payload_name TEXT PRIMARY KEY,
            snapshot_id TEXT,
            run_id TEXT,
            refresh_mode TEXT,
            payload_json TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO serving_payload_current (
            payload_name, snapshot_id, run_id, refresh_mode, payload_json
        ) VALUES ('portfolio', 'snap_1', 'run_1', 'serve-refresh', '{}')
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_stage2, "_resolve_orphan_anchor_pg", lambda _pg_conn: (None, None))
    monkeypatch.setattr(
        neon_stage2,
        "_table_exists_pg",
        lambda _pg_conn, table: table in {"serving_payload_current", "security_prices_eod", "security_registry"},
    )
    monkeypatch.setattr(
        neon_stage2,
        "_profile_pg_table",
        lambda _pg_conn, _cfg: {"row_count": 1},
    )
    monkeypatch.setattr(neon_stage2, "_duplicate_group_count_pg", lambda _pg_conn, _cfg: 0)
    monkeypatch.setattr(
        neon_stage2,
        "_pg_columns",
        lambda _pg_conn, _table: ["payload_name", "snapshot_id", "run_id", "refresh_mode", "payload_json"],
    )

    out = neon_stage2.run_parity_audit(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["serving_payload_current"],
    )

    assert out["status"] == "ok"
    assert out["issues"] == []
    assert "orphan_ric_rows" not in out["tables"]["serving_payload_current"]


def test_run_parity_audit_treats_trimmed_neon_history_as_expected_retention_gap(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_prices_eod (ric TEXT, date TEXT)")
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date) VALUES (?, ?)",
        [
            ("AAA.OQ", "2015-01-02"),
            ("AAA.OQ", "2016-03-18"),
        ],
    )
    conn.execute("CREATE TABLE security_registry (ric TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO security_registry (ric) VALUES ('AAA.OQ')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_stage2,
        "_resolve_orphan_anchor_pg",
        lambda _pg_conn: ("security_registry", "reg"),
    )
    monkeypatch.setattr(
        neon_stage2,
        "_table_exists_pg",
        lambda _pg_conn, table: table in {"security_prices_eod", "security_master_compat_current"},
    )
    monkeypatch.setattr(
        neon_stage2,
        "_profile_pg_table",
        lambda _pg_conn, cfg: (
            {
                "row_count": 1,
                "min_date": "2016-03-18",
                "max_date": "2016-03-18",
                "latest_distinct_ric": 1,
            }
            if cfg.name == "security_prices_eod"
            else {"row_count": 1}
        ),
    )
    monkeypatch.setattr(neon_stage2, "_duplicate_group_count_pg", lambda _pg_conn, _cfg: 0)
    monkeypatch.setattr(
        neon_stage2,
        "_pg_columns",
        lambda _pg_conn, table: ["ric", "date"] if table == "security_prices_eod" else ["ric"],
    )
    monkeypatch.setattr(neon_stage2, "_orphan_ric_pg", lambda _pg_conn, _table, anchor_table=None: 0)

    out = neon_stage2.run_parity_audit(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_prices_eod"],
    )

    assert out["status"] == "ok"
    assert out["issues"] == []
    assert out["notes"] == [
        "expected_retention_gap:security_prices_eod:2015-01-02->2016-03-18"
    ]
    assert out["tables"]["security_prices_eod"]["retention_gap"] == {
        "status": "ok",
        "source_archive_min_date": "2015-01-02",
        "target_retained_min_date": "2016-03-18",
        "source_rows_in_target_window": 1,
        "target_row_count": 1,
    }


def test_run_parity_audit_uses_compat_anchor_when_registry_is_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_prices_eod (ric TEXT, date TEXT)")
    conn.executemany(
        "INSERT INTO security_prices_eod (ric, date) VALUES (?, ?)",
        [
            ("AAA.OQ", "2026-03-01"),
            ("BBB.OQ", "2026-03-01"),
        ],
    )
    conn.execute("CREATE TABLE security_master_compat_current (ric TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO security_master_compat_current (ric) VALUES ('AAA.OQ')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        neon_stage2,
        "_resolve_orphan_anchor_pg",
        lambda _pg_conn: ("security_master_compat_current", "compat"),
    )
    monkeypatch.setattr(neon_stage2, "_table_exists_pg", lambda _pg_conn, table: table == "security_prices_eod")
    monkeypatch.setattr(
        neon_stage2,
        "_profile_pg_table",
        lambda _pg_conn, cfg: (
            {"row_count": 2, "min_date": "2026-03-01", "max_date": "2026-03-01", "latest_distinct_ric": 2}
            if cfg.name == "security_prices_eod"
            else {"row_count": 1}
        ),
    )
    monkeypatch.setattr(neon_stage2, "_duplicate_group_count_pg", lambda _pg_conn, _cfg: 0)
    monkeypatch.setattr(
        neon_stage2,
        "_pg_columns",
        lambda _pg_conn, table: ["ric", "date"] if table == "security_prices_eod" else ["ric"],
    )
    monkeypatch.setattr(neon_stage2, "_orphan_ric_pg", lambda _pg_conn, _table, anchor_table=None: 1)

    out = neon_stage2.run_parity_audit(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_prices_eod"],
    )

    assert out["status"] == "ok"
    assert out["issues"] == []
    assert out["tables"]["security_prices_eod"]["orphan_ric_rows"] == {"source": 1, "target": 1}


def test_run_parity_audit_notes_when_no_anchor_tables_exist(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_prices_eod (ric TEXT, date TEXT)")
    conn.execute("INSERT INTO security_prices_eod (ric, date) VALUES ('AAA.OQ', '2026-03-01')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_stage2, "_resolve_orphan_anchor_pg", lambda _pg_conn: (None, None))
    monkeypatch.setattr(neon_stage2, "_table_exists_pg", lambda _pg_conn, _table: True)
    monkeypatch.setattr(
        neon_stage2,
        "_profile_pg_table",
        lambda _pg_conn, cfg: {
            "row_count": 1,
            "min_date": "2026-03-01",
            "max_date": "2026-03-01",
            "latest_distinct_ric": 1,
        }
        if cfg.name == "security_prices_eod"
        else {"row_count": 1},
    )
    monkeypatch.setattr(neon_stage2, "_duplicate_group_count_pg", lambda _pg_conn, _cfg: 0)
    monkeypatch.setattr(
        neon_stage2,
        "_pg_columns",
        lambda _pg_conn, table: ["ric", "date"] if table == "security_prices_eod" else ["ric"],
    )
    monkeypatch.setattr(neon_stage2, "_orphan_ric_pg", lambda *_args, **_kwargs: 0)

    out = neon_stage2.run_parity_audit(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["security_prices_eod"],
    )

    assert out["status"] == "ok"
    assert "orphan_checks_skipped:no_anchor_table" in out["notes"]


def test_run_parity_audit_normalizes_timestamp_text_for_model_run_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE model_run_metadata (
            run_id TEXT PRIMARY KEY,
            completed_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO model_run_metadata (run_id, completed_at)
        VALUES ('run-1', '2026-03-16T09:11:33.903327+00:00')
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(neon_stage2, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(neon_stage2, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(neon_stage2, "_resolve_orphan_anchor_pg", lambda _pg_conn: (None, None))
    monkeypatch.setattr(neon_stage2, "_table_exists_pg", lambda _pg_conn, _table: True)
    monkeypatch.setattr(
        neon_stage2,
        "_profile_pg_table",
        lambda _pg_conn, cfg: {
            "row_count": 1,
            "min_date": "2026-03-16 09:11:33.903327+00",
            "max_date": "2026-03-16 09:11:33.903327+00",
        },
    )
    monkeypatch.setattr(neon_stage2, "_duplicate_group_count_pg", lambda _pg_conn, _cfg: 0)
    monkeypatch.setattr(neon_stage2, "_pg_columns", lambda _pg_conn, _table: ["run_id", "completed_at"])

    out = neon_stage2.run_parity_audit(
        sqlite_path=db_path,
        dsn="postgresql://example",
        tables=["model_run_metadata"],
    )

    assert out["status"] == "ok"
    assert out["issues"] == []
