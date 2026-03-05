#!/usr/bin/env python3
"""Align universe start/end dates to observed trading lifecycle."""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (table,),
    ).fetchone()
    return row is not None


def sync_lifecycle_dates(
    *,
    db_path: Path,
    active_gap_days: int = 45,
) -> dict[str, int | str | None]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        required = {"universe_eligibility_summary", "security_master", "security_prices_eod"}
        missing = [t for t in required if not _table_exists(conn, t)]
        if missing:
            raise RuntimeError(f"missing required table(s): {', '.join(sorted(missing))}")

        latest_row = conn.execute(
            """
            SELECT MAX(p.date)
            FROM security_prices_eod p
            JOIN security_master sm
              ON sm.ric = p.ric
            WHERE p.date IS NOT NULL
              AND TRIM(p.date) <> ''
              AND COALESCE(sm.is_equity_eligible, 0) = 1
              AND (p.close IS NOT NULL OR p.adj_close IS NOT NULL OR p.open IS NOT NULL OR p.high IS NOT NULL OR p.low IS NOT NULL)
            """
        ).fetchone()
        latest_trade_date = str(latest_row[0]) if latest_row and latest_row[0] else None
        if latest_trade_date is None:
            raise RuntimeError("security_prices_eod has no dated price observations")

        cutoff_date = (datetime.fromisoformat(latest_trade_date) - timedelta(days=active_gap_days)).date().isoformat()
        now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        before_rows = conn.execute("SELECT COUNT(*) FROM universe_eligibility_summary").fetchone()[0]
        before_changes = conn.total_changes
        conn.execute(
            """
            WITH px AS (
                SELECT
                    UPPER(sm.ticker) AS ticker,
                    MIN(p.date) AS first_trade_date,
                    MAX(p.date) AS last_trade_date
                FROM security_prices_eod p
                JOIN security_master sm
                  ON sm.ric = p.ric
                WHERE sm.ticker IS NOT NULL
                  AND TRIM(sm.ticker) <> ''
                  AND p.date IS NOT NULL
                  AND TRIM(p.date) <> ''
                  AND COALESCE(sm.is_equity_eligible, 0) = 1
                  AND (p.close IS NOT NULL OR p.adj_close IS NOT NULL OR p.open IS NOT NULL OR p.high IS NOT NULL OR p.low IS NOT NULL)
                GROUP BY UPPER(sm.ticker)
            )
            UPDATE universe_eligibility_summary
            SET
                start_date = COALESCE(
                    (SELECT px.first_trade_date FROM px WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)),
                    start_date
                ),
                end_date = COALESCE(
                    CASE
                        WHEN (
                            SELECT px.last_trade_date
                            FROM px
                            WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                        ) IS NULL THEN end_date
                        WHEN COALESCE(instrument_is_active, 0) = 1 THEN '9999-12-31'
                        WHEN COALESCE(TRIM(delisting_reason), '') <> '' THEN (
                            SELECT px.last_trade_date
                            FROM px
                            WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                        )
                        WHEN (
                            SELECT px.last_trade_date
                            FROM px
                            WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                        ) >= ? THEN '9999-12-31'
                        ELSE (
                            SELECT px.last_trade_date
                            FROM px
                            WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                        )
                    END,
                    end_date
                ),
                last_quote_date = COALESCE(
                    (SELECT px.last_trade_date FROM px WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)),
                    last_quote_date
                ),
                is_trading_day_active = CASE
                    WHEN COALESCE(instrument_is_active, 0) = 1 THEN 1
                    WHEN COALESCE(
                        (SELECT px.last_trade_date FROM px WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)),
                        ''
                    ) >= ? THEN 1
                    ELSE 0
                END,
                in_current_snapshot = CASE
                    WHEN COALESCE(
                        CASE
                            WHEN (
                                SELECT px.last_trade_date
                                FROM px
                                WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                            ) IS NULL THEN end_date
                            WHEN COALESCE(instrument_is_active, 0) = 1 THEN '9999-12-31'
                            WHEN COALESCE(TRIM(delisting_reason), '') <> '' THEN (
                                SELECT px.last_trade_date
                                FROM px
                                WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                            )
                            WHEN (
                                SELECT px.last_trade_date
                                FROM px
                                WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                            ) >= ? THEN '9999-12-31'
                            ELSE (
                                SELECT px.last_trade_date
                                FROM px
                                WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                            )
                        END,
                        end_date
                    ) = '9999-12-31' THEN 1
                    ELSE 0
                END,
                current_snapshot_date = CASE
                    WHEN COALESCE(
                        CASE
                            WHEN (
                                SELECT px.last_trade_date
                                FROM px
                                WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                            ) IS NULL THEN end_date
                            WHEN COALESCE(instrument_is_active, 0) = 1 THEN '9999-12-31'
                            WHEN COALESCE(TRIM(delisting_reason), '') <> '' THEN (
                                SELECT px.last_trade_date
                                FROM px
                                WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                            )
                            WHEN (
                                SELECT px.last_trade_date
                                FROM px
                                WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                            ) >= ? THEN '9999-12-31'
                            ELSE (
                                SELECT px.last_trade_date
                                FROM px
                                WHERE px.ticker = UPPER(universe_eligibility_summary.ticker)
                            )
                        END,
                        end_date
                    ) = '9999-12-31' THEN ?
                    ELSE current_snapshot_date
                END,
                updated_at = ?
            WHERE ticker IS NOT NULL
              AND TRIM(ticker) <> ''
            """,
            (
                cutoff_date,
                cutoff_date,
                cutoff_date,
                cutoff_date,
                latest_trade_date,
                now_iso,
            ),
        )
        rows_touched = int(conn.total_changes - before_changes)
        conn.commit()

        summary = conn.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT UPPER(ticker)) AS tickers,
                MIN(start_date) AS min_start,
                MAX(start_date) AS max_start,
                MIN(end_date) AS min_end,
                MAX(end_date) AS max_end,
                SUM(CASE WHEN end_date = '9999-12-31' THEN 1 ELSE 0 END) AS open_ended,
                SUM(CASE WHEN end_date <> '9999-12-31' THEN 1 ELSE 0 END) AS delisted_ended
            FROM universe_eligibility_summary
            """
        ).fetchone()

        return {
            "status": "ok",
            "db_path": str(db_path),
            "rows_before": int(before_rows or 0),
            "rows_touched": rows_touched,
            "latest_trade_date": latest_trade_date,
            "active_cutoff_date": cutoff_date,
            "rows_after": int(summary["rows"] or 0),
            "tickers_after": int(summary["tickers"] or 0),
            "min_start": summary["min_start"],
            "max_start": summary["max_start"],
            "min_end": summary["min_end"],
            "max_end": summary["max_end"],
            "open_ended": int(summary["open_ended"] or 0),
            "delisted_ended": int(summary["delisted_ended"] or 0),
        }
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("backend/data.db"),
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--active-gap-days",
        type=int,
        default=45,
        help="Treat names with recent last trade date as still active",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = sync_lifecycle_dates(
        db_path=args.db_path,
        active_gap_days=max(0, int(args.active_gap_days)),
    )
    print(result)


if __name__ == "__main__":
    main()
