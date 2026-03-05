"""SQLite health and migration-readiness audit helpers."""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CORE_TABLES = [
    "security_master",
    "security_prices_eod",
    "security_fundamentals_pit",
    "security_classification_pit",
    "barra_raw_cross_section_history",
    "estu_membership_daily",
    "universe_cross_section_snapshot",
    "model_factor_returns_daily",
    "model_factor_covariance_daily",
    "model_specific_risk_daily",
]


def _count(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] or 0) if row else 0


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run_sqlite_health_audit(
    db_path: Path,
    *,
    include_integrity_pragmas: bool = True,
) -> dict[str, Any]:
    """Return a structured health audit for the SQLite database."""
    db = Path(db_path).expanduser().resolve()
    if not db.exists():
        raise FileNotFoundError(f"database file not found: {db}")

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        checks: dict[str, Any] = {}
        if include_integrity_pragmas:
            checks["quick_check"] = str(conn.execute("PRAGMA quick_check").fetchone()[0])
            checks["integrity_check"] = str(conn.execute("PRAGMA integrity_check").fetchone()[0])

        row_counts: dict[str, int] = {}
        for table in CORE_TABLES:
            row_counts[table] = _count(conn, f"SELECT COUNT(*) FROM {table}")
        checks["row_counts"] = row_counts

        date_validity: dict[str, dict[str, Any]] = {}
        date_fields = [
            ("security_prices_eod", "date"),
            ("security_fundamentals_pit", "as_of_date"),
            ("security_fundamentals_pit", "stat_date"),
            ("security_classification_pit", "as_of_date"),
            ("barra_raw_cross_section_history", "as_of_date"),
        ]
        for table, col in date_fields:
            bad = _count(
                conn,
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL OR {col} NOT GLOB '????-??-??'",
            )
            min_val, max_val = conn.execute(
                f"SELECT MIN({col}), MAX({col}) FROM {table}"
            ).fetchone()
            date_validity[f"{table}.{col}"] = {
                "bad": int(bad),
                "min": min_val,
                "max": max_val,
            }
        checks["date_validity"] = date_validity

        duplicate_key_groups = {
            "security_master.ric": _count(
                conn,
                "SELECT COUNT(*) FROM (SELECT ric, COUNT(*) c FROM security_master GROUP BY ric HAVING c > 1)",
            ),
            "security_prices_eod.(ric,date)": _count(
                conn,
                "SELECT COUNT(*) FROM (SELECT ric, date, COUNT(*) c FROM security_prices_eod GROUP BY ric, date HAVING c > 1)",
            ),
            "security_fundamentals_pit.(ric,as_of_date)": _count(
                conn,
                "SELECT COUNT(*) FROM (SELECT ric, as_of_date, COUNT(*) c FROM security_fundamentals_pit GROUP BY ric, as_of_date HAVING c > 1)",
            ),
            "security_classification_pit.(ric,as_of_date)": _count(
                conn,
                "SELECT COUNT(*) FROM (SELECT ric, as_of_date, COUNT(*) c FROM security_classification_pit GROUP BY ric, as_of_date HAVING c > 1)",
            ),
            "barra_raw_cross_section_history.(ric,as_of_date)": _count(
                conn,
                "SELECT COUNT(*) FROM (SELECT ric, as_of_date, COUNT(*) c FROM barra_raw_cross_section_history GROUP BY ric, as_of_date HAVING c > 1)",
            ),
        }
        checks["duplicate_key_groups"] = duplicate_key_groups

        orphan_ric_rows: dict[str, int] = {}
        for table in [
            "security_prices_eod",
            "security_fundamentals_pit",
            "security_classification_pit",
            "barra_raw_cross_section_history",
        ]:
            orphan_ric_rows[table] = _count(
                conn,
                f"""
                SELECT COUNT(*)
                FROM {table} x
                LEFT JOIN security_master sm
                  ON sm.ric = x.ric
                WHERE sm.ric IS NULL
                """,
            )
        checks["orphan_ric_rows"] = orphan_ric_rows

        latest_dates = {
            "prices": conn.execute("SELECT MAX(date) FROM security_prices_eod").fetchone()[0],
            "fundamentals": conn.execute("SELECT MAX(as_of_date) FROM security_fundamentals_pit").fetchone()[0],
            "classification": conn.execute("SELECT MAX(as_of_date) FROM security_classification_pit").fetchone()[0],
            "barra_raw": conn.execute("SELECT MAX(as_of_date) FROM barra_raw_cross_section_history").fetchone()[0],
        }
        latest_counts = {
            "prices": _count(
                conn,
                "SELECT COUNT(DISTINCT ric) FROM security_prices_eod WHERE date = ?",
                (str(latest_dates["prices"] or ""),),
            ),
            "fundamentals": _count(
                conn,
                "SELECT COUNT(DISTINCT ric) FROM security_fundamentals_pit WHERE as_of_date = ?",
                (str(latest_dates["fundamentals"] or ""),),
            ),
            "classification": _count(
                conn,
                "SELECT COUNT(DISTINCT ric) FROM security_classification_pit WHERE as_of_date = ?",
                (str(latest_dates["classification"] or ""),),
            ),
            "barra_raw": _count(
                conn,
                "SELECT COUNT(DISTINCT ric) FROM barra_raw_cross_section_history WHERE as_of_date = ?",
                (str(latest_dates["barra_raw"] or ""),),
            ),
        }
        eligible_universe_n = _count(
            conn,
            """
            SELECT COUNT(*)
            FROM security_master
            WHERE COALESCE(classification_ok, 0) = 1
              AND COALESCE(is_equity_eligible, 0) = 1
            """,
        )
        checks["latest_coverage"] = {
            "eligible_security_master": int(eligible_universe_n),
            "latest_dates": latest_dates,
            "latest_distinct_ric_counts": latest_counts,
        }

        threshold = max(1, int(eligible_universe_n * 0.95))
        near_full_dates: dict[str, dict[str, Any]] = {}
        table_date = [
            ("security_prices_eod", "date"),
            ("security_fundamentals_pit", "as_of_date"),
            ("security_classification_pit", "as_of_date"),
            ("barra_raw_cross_section_history", "as_of_date"),
        ]
        for table, col in table_date:
            row = conn.execute(
                f"""
                SELECT {col} AS d, COUNT(DISTINCT ric) AS n
                FROM {table}
                GROUP BY {col}
                HAVING n >= ?
                ORDER BY {col} DESC
                LIMIT 1
                """,
                (threshold,),
            ).fetchone()
            near_full_dates[table] = {
                "date": (row["d"] if row else None),
                "distinct_ric": int(row["n"] if row else 0),
                "threshold": int(threshold),
            }
        checks["near_full_dates_95pct"] = near_full_dates

        checks["nulls"] = {
            "security_master.ric_null_or_blank": _count(
                conn,
                "SELECT COUNT(*) FROM security_master WHERE ric IS NULL OR TRIM(ric) = ''",
            ),
            "security_master.ticker_null_or_blank": _count(
                conn,
                "SELECT COUNT(*) FROM security_master WHERE ticker IS NULL OR TRIM(ticker) = ''",
            ),
            "security_prices_eod.close_null": _count(
                conn,
                "SELECT COUNT(*) FROM security_prices_eod WHERE close IS NULL",
            ),
        }

        checks["pragmas"] = {
            "page_count": int(conn.execute("PRAGMA page_count").fetchone()[0]),
            "freelist_count": int(conn.execute("PRAGMA freelist_count").fetchone()[0]),
            "page_size": int(conn.execute("PRAGMA page_size").fetchone()[0]),
        }

        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "db_path": str(db),
            "db_size_bytes": int(db.stat().st_size),
            "db_sha256": _sha256(db),
            "checks": checks,
        }
    finally:
        conn.close()

