"""Audit cUSE4 schema conformance and LSEG metric coverage over time."""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class TableSpec:
    name: str
    required_columns: list[str]
    pk_columns: list[str]


TABLE_SPECS: list[TableSpec] = [
    TableSpec(
        name="security_master",
        required_columns=[
            "sid",
            "permid",
            "ric",
            "ticker",
            "isin",
            "instrument_type",
            "asset_category_description",
            "exchange_name",
            "classification_ok",
            "is_equity_eligible",
            "source",
            "job_run_id",
            "updated_at",
        ],
        pk_columns=["sid"],
    ),
    TableSpec(
        name="fundamentals_history",
        required_columns=[
            "sid",
            "as_of_date",
            "stat_date",
            "period_end_date",
            "fiscal_year",
            "period_type",
            "report_currency",
            "market_cap",
            "shares_outstanding",
            "dividend_yield",
            "book_value_per_share",
            "total_assets",
            "total_debt",
            "cash_and_equivalents",
            "long_term_debt",
            "operating_cashflow",
            "capital_expenditures",
            "trailing_eps",
            "forward_eps",
            "revenue",
            "ebitda",
            "ebit",
            "roe_pct",
            "operating_margin_pct",
            "common_name",
            "source",
            "job_run_id",
            "updated_at",
        ],
        pk_columns=["sid", "as_of_date", "stat_date"],
    ),
    TableSpec(
        name="trbc_industry_country_history",
        required_columns=[
            "sid",
            "as_of_date",
            "trbc_economic_sector",
            "trbc_business_sector",
            "trbc_industry_group",
            "trbc_industry",
            "trbc_activity",
            "hq_country_code",
            "source",
            "job_run_id",
            "updated_at",
        ],
        pk_columns=["sid", "as_of_date"],
    ),
    TableSpec(
        # Spec expects sid keyed prices_daily; current implementation is ticker keyed.
        name="prices_daily",
        required_columns=[
            "sid",
            "date",
            "source",
            "updated_at",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "currency",
            "exchange",
        ],
        pk_columns=["sid", "date"],
    ),
    TableSpec(
        name="estu_membership_daily",
        required_columns=[
            "date",
            "sid",
            "estu_flag",
            "drop_reason",
            "drop_reason_detail",
            "mcap",
            "price_close",
            "adv_20d",
            "has_required_price_history",
            "has_required_fundamentals",
            "has_required_trbc",
            "source",
            "job_run_id",
            "updated_at",
        ],
        pk_columns=["date", "sid"],
    ),
]

SECURITY_METRICS = [
    "permid",
    "ric",
    "ticker",
    "isin",
    "instrument_type",
    "asset_category_description",
    "exchange_name",
]

FUNDAMENTALS_METRICS = [
    "market_cap",
    "shares_outstanding",
    "dividend_yield",
    "book_value_per_share",
    "total_assets",
    "total_debt",
    "cash_and_equivalents",
    "long_term_debt",
    "operating_cashflow",
    "capital_expenditures",
    "trailing_eps",
    "forward_eps",
    "revenue",
    "ebitda",
    "ebit",
    "roe_pct",
    "operating_margin_pct",
    "common_name",
]

TRBC_METRICS = [
    "trbc_economic_sector",
    "trbc_business_sector",
    "trbc_industry_group",
    "trbc_industry",
    "trbc_activity",
    "hq_country_code",
]

PRICES_METRICS = [
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "currency",
    "exchange",
]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    if not _table_exists(conn, table):
        return []
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows]


def _count(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] or 0) if row else 0


def _coverage_sql_expr(metric: str) -> str:
    # Text coverage treats blank strings as missing.
    return f"CASE WHEN {metric} IS NOT NULL AND TRIM(CAST({metric} AS TEXT)) <> '' THEN 1 ELSE 0 END"


def _eligible_sids_for_date(conn: sqlite3.Connection, date_key: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT sm.sid
        FROM security_master sm
        JOIN universe_eligibility_summary u
          ON UPPER(TRIM(u.ticker)) = sm.ticker
        WHERE sm.is_equity_eligible = 1
          AND COALESCE(sm.classification_ok, 0) = 1
          AND COALESCE(u.is_eligible, 0) = 1
          AND u.start_date <= ?
          AND u.end_date >= ?
        """,
        (date_key, date_key),
    ).fetchall()
    return {str(r[0]) for r in rows if r and r[0]}


def _sample_dates(conn: sqlite3.Connection, *, max_samples: int) -> list[str]:
    # Quarter-end style samples snapped to nearest available prices_daily date <= quarter end.
    min_date, max_date = conn.execute(
        "SELECT MIN(date), MAX(date) FROM prices_daily"
    ).fetchone()
    if not min_date or not max_date:
        return []

    q_ends = pd.date_range(start=str(min_date), end=str(max_date), freq="Q")
    raw = [d.date().isoformat() for d in q_ends]
    if not raw or raw[-1] != str(max_date):
        raw.append(str(max_date))

    out: list[str] = []
    seen: set[str] = set()
    for qd in raw:
        row = conn.execute(
            "SELECT MAX(date) FROM prices_daily WHERE date <= ?",
            (qd,),
        ).fetchone()
        if not row or not row[0]:
            continue
        resolved = str(row[0])
        if resolved not in seen:
            out.append(resolved)
            seen.add(resolved)

    # Keep dates where eligible universe exists.
    out = [d for d in out if len(_eligible_sids_for_date(conn, d)) > 0]
    if len(out) <= max_samples:
        return out
    step = max(1, len(out) // max_samples)
    sampled = out[::step]
    if sampled[-1] != out[-1]:
        sampled.append(out[-1])
    return sampled[:max_samples]


def run_audit(db_path: Path, output_dir: Path, max_samples: int) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA busy_timeout=120000")
    try:
        schema_rows: list[dict[str, Any]] = []
        integrity_rows: list[dict[str, Any]] = []

        for spec in TABLE_SPECS:
            exists = _table_exists(conn, spec.name)
            cols = _table_columns(conn, spec.name)
            missing = sorted(set(spec.required_columns) - set(cols))
            extra = sorted(set(cols) - set(spec.required_columns))
            row_count = _count(conn, f"SELECT COUNT(*) FROM {spec.name}") if exists else 0

            pk_dup_groups = None
            if exists and spec.pk_columns:
                pk_expr = ", ".join(spec.pk_columns)
                pk_dup_groups = _count(
                    conn,
                    f"SELECT COUNT(*) FROM (SELECT {pk_expr}, COUNT(*) c FROM {spec.name} GROUP BY {pk_expr} HAVING c > 1)",
                )

            schema_rows.append(
                {
                    "table": spec.name,
                    "exists": int(exists),
                    "row_count": row_count,
                    "required_col_count": len(spec.required_columns),
                    "actual_col_count": len(cols),
                    "missing_columns": "|".join(missing),
                    "extra_columns": "|".join(extra),
                    "pk_duplicate_groups": pk_dup_groups,
                }
            )

            if exists:
                for key_col in spec.pk_columns:
                    null_count = _count(
                        conn,
                        f"SELECT COUNT(*) FROM {spec.name} WHERE {key_col} IS NULL OR TRIM(CAST({key_col} AS TEXT)) = ''",
                    )
                    integrity_rows.append(
                        {
                            "table": spec.name,
                            "check": f"pk_null:{key_col}",
                            "value": null_count,
                        }
                    )

        # Referential integrity checks
        fk_checks = [
            (
                "fundamentals_history",
                "SELECT COUNT(*) FROM fundamentals_history f LEFT JOIN security_master s ON f.sid = s.sid WHERE s.sid IS NULL",
            ),
            (
                "trbc_industry_country_history",
                "SELECT COUNT(*) FROM trbc_industry_country_history t LEFT JOIN security_master s ON t.sid = s.sid WHERE s.sid IS NULL",
            ),
            (
                "estu_membership_daily",
                "SELECT COUNT(*) FROM estu_membership_daily e LEFT JOIN security_master s ON e.sid = s.sid WHERE s.sid IS NULL",
            ),
        ]
        for table, sql in fk_checks:
            if _table_exists(conn, table) and _table_exists(conn, "security_master"):
                integrity_rows.append(
                    {
                        "table": table,
                        "check": "orphan_sid_rows",
                        "value": _count(conn, sql),
                    }
                )

        sample_dates = _sample_dates(conn, max_samples=max_samples)
        coverage_rows: list[dict[str, Any]] = []

        for date_key in sample_dates:
            eligible_sids = _eligible_sids_for_date(conn, date_key)
            eligible_n = len(eligible_sids)
            if eligible_n == 0:
                continue

            sid_values = sorted(eligible_sids)
            placeholders = ",".join("?" for _ in sid_values)

            # security_master (static fields, evaluated on eligible SID set)
            for metric in SECURITY_METRICS:
                sql = (
                    f"SELECT SUM({_coverage_sql_expr(metric)}) "
                    f"FROM security_master WHERE sid IN ({placeholders})"
                )
                covered_n = _count(conn, sql, tuple(sid_values))
                coverage_rows.append(
                    {
                        "date": date_key,
                        "table": "security_master",
                        "metric": metric,
                        "eligible_n": eligible_n,
                        "covered_n": covered_n,
                        "coverage_pct": round((covered_n / eligible_n) * 100.0, 2),
                    }
                )

            # fundamentals_history PIT as-of <= date
            for metric in FUNDAMENTALS_METRICS:
                sql = f"""
                WITH latest AS (
                    SELECT sid, MAX(as_of_date) AS as_of_date
                    FROM fundamentals_history
                    WHERE as_of_date <= ?
                      AND sid IN ({placeholders})
                    GROUP BY sid
                )
                SELECT SUM({_coverage_sql_expr('f.' + metric)})
                FROM latest l
                JOIN fundamentals_history f
                  ON f.sid = l.sid
                 AND f.as_of_date = l.as_of_date
                """
                covered_n = _count(conn, sql, (date_key, *sid_values))
                coverage_rows.append(
                    {
                        "date": date_key,
                        "table": "fundamentals_history",
                        "metric": metric,
                        "eligible_n": eligible_n,
                        "covered_n": covered_n,
                        "coverage_pct": round((covered_n / eligible_n) * 100.0, 2),
                    }
                )

            # trbc_industry_country_history PIT as-of <= date
            for metric in TRBC_METRICS:
                sql = f"""
                WITH latest AS (
                    SELECT sid, MAX(as_of_date) AS as_of_date
                    FROM trbc_industry_country_history
                    WHERE as_of_date <= ?
                      AND sid IN ({placeholders})
                    GROUP BY sid
                )
                SELECT SUM({_coverage_sql_expr('t.' + metric)})
                FROM latest l
                JOIN trbc_industry_country_history t
                  ON t.sid = l.sid
                 AND t.as_of_date = l.as_of_date
                """
                covered_n = _count(conn, sql, (date_key, *sid_values))
                coverage_rows.append(
                    {
                        "date": date_key,
                        "table": "trbc_industry_country_history",
                        "metric": metric,
                        "eligible_n": eligible_n,
                        "covered_n": covered_n,
                        "coverage_pct": round((covered_n / eligible_n) * 100.0, 2),
                    }
                )

            # prices_daily exact date join via security_master.ticker
            for metric in PRICES_METRICS:
                sql = f"""
                SELECT SUM({_coverage_sql_expr('p.' + metric)})
                FROM security_master s
                LEFT JOIN prices_daily p
                  ON p.ticker = s.ticker
                 AND p.date = ?
                WHERE s.sid IN ({placeholders})
                """
                covered_n = _count(conn, sql, (date_key, *sid_values))
                coverage_rows.append(
                    {
                        "date": date_key,
                        "table": "prices_daily",
                        "metric": metric,
                        "eligible_n": eligible_n,
                        "covered_n": covered_n,
                        "coverage_pct": round((covered_n / eligible_n) * 100.0, 2),
                    }
                )

        schema_df = pd.DataFrame(schema_rows).sort_values("table")
        integrity_df = pd.DataFrame(integrity_rows).sort_values(["table", "check"]) if integrity_rows else pd.DataFrame()
        coverage_df = pd.DataFrame(coverage_rows).sort_values(["table", "metric", "date"]) if coverage_rows else pd.DataFrame()

        # Coverage summaries
        summary_rows: list[dict[str, Any]] = []
        if not coverage_df.empty:
            grouped = coverage_df.groupby(["table", "metric"], sort=True)
            for (table, metric), grp in grouped:
                vals = [float(v) for v in grp["coverage_pct"].tolist()]
                latest_idx = grp["date"].idxmax()
                latest_row = grp.loc[latest_idx]
                summary_rows.append(
                    {
                        "table": table,
                        "metric": metric,
                        "samples": int(len(vals)),
                        "coverage_min_pct": round(min(vals), 2),
                        "coverage_median_pct": round(median(vals), 2),
                        "coverage_max_pct": round(max(vals), 2),
                        "coverage_latest_pct": round(float(latest_row["coverage_pct"]), 2),
                        "latest_date": str(latest_row["date"]),
                    }
                )
        summary_df = pd.DataFrame(summary_rows).sort_values(["table", "metric"]) if summary_rows else pd.DataFrame()

        # Write artifacts
        schema_csv = output_dir / "schema_audit.csv"
        integrity_csv = output_dir / "integrity_checks.csv"
        coverage_csv = output_dir / "metric_coverage_by_date.csv"
        summary_csv = output_dir / "metric_coverage_summary.csv"
        report_md = output_dir / "cuse4_db_audit_report.md"

        schema_df.to_csv(schema_csv, index=False)
        if not integrity_df.empty:
            integrity_df.to_csv(integrity_csv, index=False)
        else:
            pd.DataFrame(columns=["table", "check", "value"]).to_csv(integrity_csv, index=False)
        if not coverage_df.empty:
            coverage_df.to_csv(coverage_csv, index=False)
            summary_df.to_csv(summary_csv, index=False)
        else:
            pd.DataFrame(columns=["date", "table", "metric", "eligible_n", "covered_n", "coverage_pct"]).to_csv(coverage_csv, index=False)
            pd.DataFrame(columns=["table", "metric", "samples", "coverage_min_pct", "coverage_median_pct", "coverage_max_pct", "coverage_latest_pct", "latest_date"]).to_csv(summary_csv, index=False)

        # Build concise markdown report
        lines: list[str] = []
        lines.append("# cUSE4 Database Audit Report")
        lines.append("")
        lines.append(f"- Database: `{db_path}`")
        lines.append(f"- Sample Dates Analyzed: `{len(sample_dates)}`")
        if sample_dates:
            lines.append(f"- Date Range: `{sample_dates[0]}` to `{sample_dates[-1]}`")
        lines.append("")

        lines.append("## Schema Conformance")
        lines.append("")
        if schema_df.empty:
            lines.append("No schema results.")
        else:
            for row in schema_df.to_dict("records"):
                status = "PASS"
                if int(row["exists"]) == 0 or str(row["missing_columns"]):
                    status = "FAIL"
                elif str(row["extra_columns"]):
                    status = "WARN"
                lines.append(
                    f"- `{row['table']}`: {status} | rows={row['row_count']} | "
                    f"missing=[{row['missing_columns']}] | extra=[{row['extra_columns']}] | "
                    f"pk_duplicate_groups={row['pk_duplicate_groups']}"
                )

        lines.append("")
        lines.append("## Integrity Checks")
        lines.append("")
        if integrity_df.empty:
            lines.append("No integrity checks produced.")
        else:
            for row in integrity_df.to_dict("records"):
                lines.append(f"- `{row['table']}` `{row['check']}` = {row['value']}")

        lines.append("")
        lines.append("## Coverage Summary (Percent of Eligible Universe)")
        lines.append("")
        if summary_df.empty:
            lines.append("No coverage results.")
        else:
            for table in ["security_master", "fundamentals_history", "trbc_industry_country_history", "prices_daily"]:
                sub = summary_df[summary_df["table"] == table]
                if sub.empty:
                    continue
                lines.append(f"### {table}")
                lines.append("")
                for row in sub.to_dict("records"):
                    lines.append(
                        f"- `{row['metric']}`: min={row['coverage_min_pct']}%, "
                        f"median={row['coverage_median_pct']}%, max={row['coverage_max_pct']}%, "
                        f"latest={row['coverage_latest_pct']}% ({row['latest_date']})"
                    )
                lines.append("")

        report_md.write_text("\n".join(lines), encoding="utf-8")

        return {
            "status": "ok",
            "sample_dates": sample_dates,
            "schema_rows": int(len(schema_df)),
            "integrity_rows": int(len(integrity_df)),
            "coverage_rows": int(len(coverage_df)),
            "summary_rows": int(len(summary_df)),
            "outputs": {
                "report_md": str(report_md),
                "schema_csv": str(schema_csv),
                "integrity_csv": str(integrity_csv),
                "coverage_csv": str(coverage_csv),
                "summary_csv": str(summary_csv),
            },
        }
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit cUSE4 schema + LSEG metric coverage.")
    parser.add_argument("--db-path", default="backend/data.db", help="Path to SQLite DB")
    parser.add_argument(
        "--output-dir",
        default="backend/audits/cuse4_db_audit",
        help="Directory for markdown/csv outputs",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=28,
        help="Max number of quarterly sample dates",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    result = run_audit(
        db_path=Path(args.db_path).expanduser(),
        output_dir=Path(args.output_dir).expanduser(),
        max_samples=max(4, int(args.max_samples)),
    )
    print(result)
