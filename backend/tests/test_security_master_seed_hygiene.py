from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


SEED_PATH = Path(__file__).resolve().parents[2] / "data" / "reference" / "security_master_seed.csv"
PRIMARY_SUFFIXES = (".N", ".OQ", ".A")
SECONDARY_SUFFIXES = ("", ".O", ".K", ".P", ".PH", ".B", ".TH", ".C", ".DG")


def _suffix(ric: str) -> str:
    text = str(ric or "").strip().upper()
    if "." not in text:
        return ""
    return "." + text.rsplit(".", 1)[1]


def _root(ric: str) -> str:
    text = str(ric or "").strip().upper()
    return text.split(".", 1)[0]


def test_security_master_seed_has_no_clean_alias_duplicates() -> None:
    groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    with SEED_PATH.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ticker = str(row.get("ticker") or "").strip().upper()
            isin = str(row.get("isin") or "").strip().upper()
            ric = str(row.get("ric") or "").strip().upper()
            if not ticker or not isin or not ric:
                continue
            groups[(ticker, isin)].append(
                {
                    "ric": ric,
                    "exchange_name": str(row.get("exchange_name") or "").strip(),
                }
            )

    offenders: list[str] = []
    for (ticker, isin), rows in sorted(groups.items()):
        rics = [row["ric"] for row in rows]
        if len(rows) != 2:
            continue
        if not all(_root(ric) == ticker for ric in rics):
            continue
        if not all(str(row["exchange_name"]).strip() for row in rows):
            continue
        suffixes = {_suffix(ric) for ric in rics}
        if any(primary in suffixes for primary in PRIMARY_SUFFIXES) and any(
            secondary in suffixes for secondary in SECONDARY_SUFFIXES
        ):
            offenders.append(f"{ticker}/{isin}: {', '.join(sorted(rics))}")

    assert offenders == []
