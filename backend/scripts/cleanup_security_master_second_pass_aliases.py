#!/usr/bin/env python3
"""Safely canonicalize second-pass security_master venue aliases.

This script is intentionally conservative. It only auto-deletes rows when:
- ticker + ISIN match
- the group has exactly two rows
- one row is a clearly preferred primary listing/quote identity
- the other row is a clearly secondary venue/consolidated alias
- no current holdings reference the alias being removed

Anything else is emitted to the manual-review report.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.data.neon import connect, resolve_dsn


@dataclass(frozen=True)
class SecurityMasterRow:
    ric: str
    ticker: str
    isin: str
    exchange_name: str
    classification_ok: int
    is_equity_eligible: int
    source: str
    coverage_role: str
    updated_at: str


@dataclass(frozen=True)
class Candidate:
    ticker: str
    isin: str
    keep_ric: str
    keep_exchange_name: str
    delete_ric: str
    delete_exchange_name: str
    rule: str


@dataclass(frozen=True)
class ManualReview:
    ticker: str
    isin: str
    reason: str
    rics: str
    exchanges: str


def _suffix(ric: str) -> str:
    parts = str(ric or "").split(".")
    return parts[-1] if len(parts) > 1 else ""


def _root(ric: str) -> str:
    return str(ric or "").split(".")[0]


def _clean_exchange(value: str | None) -> str:
    return str(value or "").strip()


def _load_local_rows(data_db: Path) -> list[SecurityMasterRow]:
    conn = sqlite3.connect(str(data_db))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                ric,
                ticker,
                isin,
                exchange_name,
                classification_ok,
                is_equity_eligible,
                source,
                coverage_role,
                updated_at
            FROM security_master
            WHERE COALESCE(is_equity_eligible, 0) = 1
              AND TRIM(COALESCE(isin, '')) <> ''
              AND TRIM(COALESCE(ticker, '')) <> ''
            ORDER BY ticker, isin, ric
            """
        ).fetchall()
    finally:
        conn.close()
    return [
        SecurityMasterRow(
            ric=str(row["ric"] or "").strip(),
            ticker=str(row["ticker"] or "").strip(),
            isin=str(row["isin"] or "").strip(),
            exchange_name=_clean_exchange(row["exchange_name"]),
            classification_ok=int(row["classification_ok"] or 0),
            is_equity_eligible=int(row["is_equity_eligible"] or 0),
            source=str(row["source"] or "").strip(),
            coverage_role=str(row["coverage_role"] or "").strip(),
            updated_at=str(row["updated_at"] or "").strip(),
        )
        for row in rows
    ]


def _group_rows(rows: list[SecurityMasterRow]) -> dict[tuple[str, str], list[SecurityMasterRow]]:
    out: dict[tuple[str, str], list[SecurityMasterRow]] = defaultdict(list)
    for row in rows:
        out[(row.ticker, row.isin)].append(row)
    return out


def _looks_like_primary_n(row: SecurityMasterRow) -> bool:
    return row.ric.endswith(".N") and "new york stock exchange" in row.exchange_name.lower()


def _looks_like_primary_nasdaq_n(row: SecurityMasterRow) -> bool:
    exchange = row.exchange_name.lower()
    return (
        row.ric.endswith(".N")
        and "nasdaq stock exchange" in exchange
        and "consolidated" not in exchange
    )


def _looks_like_primary_oq(row: SecurityMasterRow) -> bool:
    exchange = row.exchange_name.lower()
    return (
        row.ric.endswith(".OQ")
        and "nasdaq" in exchange
        and "consolidated" not in exchange
    )


def _looks_like_primary_a(row: SecurityMasterRow) -> bool:
    return row.ric.endswith(".A") and "american stock exchange" in row.exchange_name.lower()


def _looks_like_secondary_american(row: SecurityMasterRow) -> bool:
    return row.ric.endswith(".A") and "american stock exchange" in row.exchange_name.lower()


def _secondary_rule(row: SecurityMasterRow) -> str | None:
    exchange = row.exchange_name.lower()
    suffix = _suffix(row.ric)
    if suffix == "K" and ("new york consolidated" in exchange or "bats consolidated" in exchange):
        return "secondary_consolidated_nyse"
    if suffix == "K" and "amex consolidated" in exchange:
        return "secondary_consolidated_amex"
    if suffix == "K" and "consolidated issue listed on nasdaq" in exchange:
        return "secondary_consolidated_nasdaq"
    if suffix == "" and "amex consolidated" in exchange:
        return "secondary_consolidated_amex"
    if suffix == "P" and "nyse arca" in exchange:
        return "secondary_nyse_arca"
    if suffix == "PH" and "psx" in exchange:
        return "secondary_psx"
    if suffix == "B" and "boston" in exchange:
        return "secondary_boston"
    if suffix == "TH" and "third market" in exchange:
        return "secondary_third_market"
    if suffix == "C" and (
        "national se when trading" in exchange
        or "the national stock exchange" in exchange
    ):
        return "secondary_national_trading"
    if suffix == "DG" and "direct edge" in exchange:
        return "secondary_direct_edge"
    return None


def _classify_group(
    ticker: str,
    isin: str,
    group: list[SecurityMasterRow],
) -> tuple[list[Candidate], list[ManualReview]]:
    if len(group) < 2:
        return [], []
    if len(group) != 2:
        return [], [
            ManualReview(
                ticker=ticker,
                isin=isin,
                reason="group_size_not_two",
                rics=" | ".join(row.ric for row in group),
                exchanges=" | ".join(row.exchange_name or "<blank>" for row in group),
            )
        ]

    left, right = sorted(group, key=lambda row: row.ric)
    if _root(left.ric) != ticker or _root(right.ric) != ticker:
        return [], [
            ManualReview(
                ticker=ticker,
                isin=isin,
                reason="root_mismatch",
                rics=f"{left.ric} | {right.ric}",
                exchanges=f"{left.exchange_name or '<blank>'} | {right.exchange_name or '<blank>'}",
            )
        ]

    left_secondary = _secondary_rule(left)
    right_secondary = _secondary_rule(right)
    left_primary_n = _looks_like_primary_n(left)
    right_primary_n = _looks_like_primary_n(right)
    left_primary_nasdaq_n = _looks_like_primary_nasdaq_n(left)
    right_primary_nasdaq_n = _looks_like_primary_nasdaq_n(right)
    left_primary_oq = _looks_like_primary_oq(left)
    right_primary_oq = _looks_like_primary_oq(right)
    left_primary_a = _looks_like_primary_a(left)
    right_primary_a = _looks_like_primary_a(right)
    left_secondary_american = _looks_like_secondary_american(left)
    right_secondary_american = _looks_like_secondary_american(right)

    if left_primary_n and right_secondary:
        return [
            Candidate(
                ticker=ticker,
                isin=isin,
                keep_ric=left.ric,
                keep_exchange_name=left.exchange_name,
                delete_ric=right.ric,
                delete_exchange_name=right.exchange_name,
                rule=f"N_over_{right_secondary}",
            )
        ], []
    if right_primary_n and left_secondary:
        return [
            Candidate(
                ticker=ticker,
                isin=isin,
                keep_ric=right.ric,
                keep_exchange_name=right.exchange_name,
                delete_ric=left.ric,
                delete_exchange_name=left.exchange_name,
                rule=f"N_over_{left_secondary}",
            )
        ], []
    if left_primary_oq and right_secondary:
        return [
            Candidate(
                ticker=ticker,
                isin=isin,
                keep_ric=left.ric,
                keep_exchange_name=left.exchange_name,
                delete_ric=right.ric,
                delete_exchange_name=right.exchange_name,
                rule=f"OQ_over_{right_secondary}",
            )
        ], []
    if right_primary_oq and left_secondary:
        return [
            Candidate(
                ticker=ticker,
                isin=isin,
                keep_ric=right.ric,
                keep_exchange_name=right.exchange_name,
                delete_ric=left.ric,
                delete_exchange_name=left.exchange_name,
                rule=f"OQ_over_{left_secondary}",
            )
        ], []
    if left_primary_nasdaq_n and right_secondary == "secondary_consolidated_nasdaq":
        return [
            Candidate(
                ticker=ticker,
                isin=isin,
                keep_ric=left.ric,
                keep_exchange_name=left.exchange_name,
                delete_ric=right.ric,
                delete_exchange_name=right.exchange_name,
                rule="NASDAQ_N_over_secondary_consolidated_nasdaq",
            )
        ], []
    if right_primary_nasdaq_n and left_secondary == "secondary_consolidated_nasdaq":
        return [
            Candidate(
                ticker=ticker,
                isin=isin,
                keep_ric=right.ric,
                keep_exchange_name=right.exchange_name,
                delete_ric=left.ric,
                delete_exchange_name=left.exchange_name,
                rule="NASDAQ_N_over_secondary_consolidated_nasdaq",
            )
        ], []
    if left_primary_a and right_secondary == "secondary_consolidated_amex":
        return [
            Candidate(
                ticker=ticker,
                isin=isin,
                keep_ric=left.ric,
                keep_exchange_name=left.exchange_name,
                delete_ric=right.ric,
                delete_exchange_name=right.exchange_name,
                rule="A_over_secondary_consolidated_amex",
            )
        ], []
    if right_primary_a and left_secondary == "secondary_consolidated_amex":
        return [
            Candidate(
                ticker=ticker,
                isin=isin,
                keep_ric=right.ric,
                keep_exchange_name=right.exchange_name,
                delete_ric=left.ric,
                delete_exchange_name=left.exchange_name,
                rule="A_over_secondary_consolidated_amex",
            )
        ], []
    if left_primary_n and right_secondary_american:
        return [
            Candidate(
                ticker=ticker,
                isin=isin,
                keep_ric=left.ric,
                keep_exchange_name=left.exchange_name,
                delete_ric=right.ric,
                delete_exchange_name=right.exchange_name,
                rule="N_over_secondary_american",
            )
        ], []
    if right_primary_n and left_secondary_american:
        return [
            Candidate(
                ticker=ticker,
                isin=isin,
                keep_ric=right.ric,
                keep_exchange_name=right.exchange_name,
                delete_ric=left.ric,
                delete_exchange_name=left.exchange_name,
                rule="N_over_secondary_american",
            )
        ], []

    return [], [
        ManualReview(
            ticker=ticker,
            isin=isin,
            reason="no_unambiguous_primary_alias_pair",
            rics=f"{left.ric} | {right.ric}",
            exchanges=f"{left.exchange_name or '<blank>'} | {right.exchange_name or '<blank>'}",
        )
    ]


def classify_candidates(rows: list[SecurityMasterRow]) -> tuple[list[Candidate], list[ManualReview]]:
    groups = _group_rows(rows)
    candidates: list[Candidate] = []
    manual: list[ManualReview] = []
    for (ticker, isin), group in sorted(groups.items()):
        found, review = _classify_group(ticker, isin, group)
        candidates.extend(found)
        manual.extend(review)
    return candidates, manual


def _load_current_holdings_alias_hits(dsn: str, delete_rics: list[str]) -> list[dict[str, Any]]:
    if not delete_rics:
        return []
    with connect(dsn=dsn) as pg:
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT account_id, ric, COALESCE(NULLIF(TRIM(ticker), ''), '') AS ticker, quantity
                FROM holdings_positions_current
                WHERE ric = ANY(%s)
                ORDER BY account_id, ric
                """,
                (delete_rics,),
            )
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in rows]


def _timestamp_dir(base_dir: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return base_dir / stamp


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _backup_rows(
    *,
    backup_dir: Path,
    data_db: Path,
    dsn: str,
    delete_rics: list[str],
) -> None:
    backup_dir.mkdir(parents=True, exist_ok=True)
    placeholders = ",".join("?" for _ in delete_rics)
    conn = sqlite3.connect(str(data_db))
    conn.row_factory = sqlite3.Row
    try:
        local_rows = conn.execute(
            f"""
            SELECT
                ric,
                ticker,
                isin,
                exchange_name,
                classification_ok,
                is_equity_eligible,
                source,
                coverage_role,
                updated_at
            FROM security_master
            WHERE ric IN ({placeholders})
            ORDER BY ric
            """,
            delete_rics,
        ).fetchall()
    finally:
        conn.close()
    with (backup_dir / "local_security_master_rows.json").open("w", encoding="utf-8") as handle:
        json.dump([dict(row) for row in local_rows], handle, indent=2)

    with connect(dsn=dsn) as pg:
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ric,
                    ticker,
                    isin,
                    exchange_name,
                    classification_ok,
                    is_equity_eligible,
                    source,
                    coverage_role,
                    updated_at
                FROM security_master
                WHERE ric = ANY(%s)
                ORDER BY ric
                """,
                (delete_rics,),
            )
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    with (backup_dir / "neon_security_master_rows.json").open("w", encoding="utf-8") as handle:
        json.dump([dict(zip(columns, row)) for row in rows], handle, indent=2, default=str)


def _apply_seed_delete(seed_path: Path, delete_rics: set[str]) -> int:
    with seed_path.open(encoding="utf-8") as handle:
        lines = handle.readlines()
    kept: list[str] = []
    removed = 0
    for line in lines:
        ric = line.split(",", 1)[0].strip()
        if ric in delete_rics:
            removed += 1
            continue
        kept.append(line)
    with seed_path.open("w", encoding="utf-8") as handle:
        handle.writelines(kept)
    return removed


def _apply_deletes(*, data_db: Path, dsn: str, delete_rics: list[str], seed_path: Path) -> dict[str, int]:
    delete_set = set(delete_rics)
    removed_seed = _apply_seed_delete(seed_path, delete_set)

    conn = sqlite3.connect(str(data_db))
    try:
        placeholders = ",".join("?" for _ in delete_rics)
        cur = conn.cursor()
        cur.execute(f"DELETE FROM security_master WHERE ric IN ({placeholders})", delete_rics)
        local_deleted = int(cur.rowcount or 0)
        conn.commit()
    finally:
        conn.close()

    with connect(dsn=dsn) as pg:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM security_master WHERE ric = ANY(%s)", (delete_rics,))
            neon_deleted = int(cur.rowcount or 0)
        pg.commit()

    return {
        "seed_removed": removed_seed,
        "local_deleted": local_deleted,
        "neon_deleted": neon_deleted,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-db", type=Path, default=Path("backend/runtime/data.db"))
    parser.add_argument("--seed-path", type=Path, default=Path("data/reference/security_registry_seed.csv"))
    parser.add_argument("--dsn", default=None, help="Neon DSN; defaults to NEON_DATABASE_URL")
    parser.add_argument(
        "--backup-base-dir",
        type=Path,
        default=Path("/tmp/ceiora-security-master-backups/second-pass"),
        help="Base directory for dry-run/apply reports and row backups",
    )
    parser.add_argument("--apply", action="store_true", help="Apply deletes to seed + SQLite + Neon")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable summary")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    dsn = resolve_dsn(args.dsn)
    rows = _load_local_rows(args.data_db)
    candidates, manual = classify_candidates(rows)
    backup_dir = _timestamp_dir(args.backup_base_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)

    candidate_rows = [asdict(row) for row in candidates]
    manual_rows = [asdict(row) for row in manual]
    _write_csv(
        backup_dir / "delete_candidates.csv",
        candidate_rows,
        fieldnames=[
            "ticker",
            "isin",
            "keep_ric",
            "keep_exchange_name",
            "delete_ric",
            "delete_exchange_name",
            "rule",
        ],
    )
    _write_csv(
        backup_dir / "manual_review.csv",
        manual_rows,
        fieldnames=[
            "ticker",
            "isin",
            "reason",
            "rics",
            "exchanges",
        ],
    )

    delete_rics = [row.delete_ric for row in candidates]
    holdings_hits = _load_current_holdings_alias_hits(dsn, delete_rics)
    with (backup_dir / "holdings_alias_hits.json").open("w", encoding="utf-8") as handle:
        json.dump(holdings_hits, handle, indent=2, default=str)

    summary: dict[str, Any] = {
        "backup_dir": str(backup_dir),
        "candidate_count": len(candidates),
        "manual_review_count": len(manual),
        "holdings_alias_hit_count": len(holdings_hits),
        "by_rule": dict(sorted((rule, sum(1 for row in candidates if row.rule == rule)) for rule in {row.rule for row in candidates})),
        "applied": False,
    }

    if args.apply:
        if holdings_hits:
            raise SystemExit("refusing to apply: current holdings reference candidate delete RICs")
        _backup_rows(
            backup_dir=backup_dir,
            data_db=args.data_db,
            dsn=dsn,
            delete_rics=delete_rics,
        )
        summary["apply_result"] = _apply_deletes(
            data_db=args.data_db,
            dsn=dsn,
            delete_rics=delete_rics,
            seed_path=args.seed_path,
        )
        summary["applied"] = True

    with (backup_dir / "summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print(f"backup_dir={backup_dir}")
        print(f"candidate_count={summary['candidate_count']}")
        print(f"manual_review_count={summary['manual_review_count']}")
        print(f"holdings_alias_hit_count={summary['holdings_alias_hit_count']}")
        for rule, count in summary["by_rule"].items():
            print(f"rule[{rule}]={count}")
        if summary["applied"]:
            print(f"apply_result={summary['apply_result']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
