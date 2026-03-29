from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from backend.scripts.canonicalize_registry_source_history import canonicalize_registry_source_history
from backend.universe.schema import ensure_cuse4_schema


def test_registry_source_history_canonicalization_remaps_unique_aliases_without_mutating_security_master(
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    now_iso = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO security_registry (
            ric, ticker, tracking_status, source, updated_at
        ) VALUES ('ABG.N', 'ABG', 'active', 'security_registry_seed', ?)
        """,
        (now_iso,),
    )
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, exchange_name, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES (?, ?, 'NYSE', 1, 1, 'native_equity', 'lseg_toolkit', ?)
        """,
        [
            ("ABG.N", "ABG", "2026-03-24T00:00:00+00:00"),
            ("ABG", "ABG", "2026-03-25T00:00:00+00:00"),
            ("STEC.O", "STEC", now_iso),
        ],
    )
    conn.executemany(
        """
        INSERT INTO security_prices_eod (
            ric, date, open, high, low, close, adj_close, volume, currency, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'USD', 'lseg_toolkit', ?)
        """,
            [
                ("ABG", "2026-03-24", 10.0, 10.5, 9.8, 10.2, 10.2, 1000.0, "2026-03-25T00:00:00+00:00"),
                ("ABG.N", "2026-03-24", 9.9, 10.4, 9.7, 10.1, 10.1, 900.0, "2026-03-24T00:00:00+00:00"),
                ("STEC.O", "2026-03-24", 4.0, 4.0, 4.0, 4.0, 4.0, 0.0, now_iso),
            ],
        )
    conn.execute(
        """
        INSERT INTO security_fundamentals_pit (
            ric, as_of_date, stat_date, market_cap, source, job_run_id, updated_at
        ) VALUES ('ABG', '2026-03-24', '2026-03-24', 1000.0, 'lseg_toolkit', 'job_1', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_classification_pit (
            ric, as_of_date, trbc_economic_sector, trbc_business_sector, source, job_run_id, updated_at
        ) VALUES ('ABG', '2026-03-24', 'Industrials', 'Business Services', 'lseg_toolkit', 'job_1', ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT INTO security_ingest_audit (
            job_run_id, ric, artifact_name, status, detail, updated_at
        ) VALUES ('job_1', 'ABG', 'prices', 'ok', NULL, ?)
        """,
        (now_iso,),
    )
    conn.commit()
    conn.close()

    out = canonicalize_registry_source_history(
        source_db=data_db,
        backup_dir=tmp_path / "backups",
        apply_changes=True,
    )

    assert out["status"] == "ok"
    result = out["result"]
    assert result["mapped_alias_ric_count"] == 1
    assert ("ABG", "ABG.N") in result["mapped_aliases"]
    assert result["unresolved_no_candidate"] == ["STEC.O"]
    assert result["security_master_rows_deleted"] == 0

    conn = sqlite3.connect(str(data_db))
    try:
        price_rows = conn.execute(
            """
            SELECT ric, date, close
            FROM security_prices_eod
            ORDER BY ric, date
            """
        ).fetchall()
        fundamentals_rows = conn.execute(
            """
            SELECT ric, as_of_date
            FROM security_fundamentals_pit
            ORDER BY ric, as_of_date
            """
        ).fetchall()
        classification_rows = conn.execute(
            """
            SELECT ric, as_of_date
            FROM security_classification_pit
            ORDER BY ric, as_of_date
            """
        ).fetchall()
        audit_rows = conn.execute(
            """
            SELECT job_run_id, ric, artifact_name
            FROM security_ingest_audit
            ORDER BY job_run_id, ric, artifact_name
            """
        ).fetchall()
        security_master_rics = [
            row[0]
            for row in conn.execute(
                """
                SELECT ric
                FROM security_master
                ORDER BY ric
                """
            ).fetchall()
        ]
    finally:
        conn.close()

    assert price_rows == [("ABG.N", "2026-03-24", 10.2)]
    assert fundamentals_rows == [("ABG.N", "2026-03-24")]
    assert classification_rows == [("ABG.N", "2026-03-24")]
    assert audit_rows == [("job_1", "ABG.N", "prices")]
    assert security_master_rics == ["ABG", "ABG.N", "STEC.O"]
