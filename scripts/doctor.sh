#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv_local/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "doctor: missing .venv_local"
  echo "run ./scripts/setup_local_env.sh first"
  exit 1
fi

echo "doctor: using $PYTHON_BIN"

"$PYTHON_BIN" - <<'PY'
import importlib
import csv
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

checks = [
    ("backend.main", True),
    ("backend.orchestration.run_model_pipeline", True),
    ("backend.services.refresh_manager", True),
    ("lseg.data", False),
]

failed = False
for module_name, required in checks:
    try:
        importlib.import_module(module_name)
        print(f"ok: import {module_name}")
    except Exception as exc:
        level = "error" if required else "warning"
        print(f"{level}: import {module_name} failed: {exc}")
        if required:
            failed = True

if failed:
    sys.exit(1)

ROOT = Path.cwd()
SEED_PATH = ROOT / "data" / "reference" / "security_master_seed.csv"
DATA_DB = ROOT / "backend" / "runtime" / "data.db"
PRIMARY_SUFFIXES = (".N", ".OQ", ".A")
SECONDARY_SUFFIXES = ("", ".O", ".K", ".P", ".PH", ".B", ".TH", ".C", ".DG")


def _suffix(ric: str) -> str:
    text = str(ric or "").strip().upper()
    if "." not in text:
        return ""
    return "." + text.rsplit(".", 1)[1]


def _root(ric: str) -> str:
    return str(ric or "").strip().upper().split(".", 1)[0]


def _clean_alias_offenders(rows):
    groups = defaultdict(list)
    for row in rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        isin = str(row.get("isin") or "").strip().upper()
        ric = str(row.get("ric") or "").strip().upper()
        exchange_name = str(row.get("exchange_name") or "").strip()
        if not ticker or not isin or not ric:
            continue
        groups[(ticker, isin)].append({"ric": ric, "exchange_name": exchange_name})

    offenders = []
    for (ticker, isin), group_rows in sorted(groups.items()):
        rics = [row["ric"] for row in group_rows]
        if len(group_rows) != 2:
            continue
        if not all(_root(ric) == ticker for ric in rics):
            continue
        if not all(str(row["exchange_name"]).strip() for row in group_rows):
            continue
        suffixes = {_suffix(ric) for ric in rics}
        if any(primary in suffixes for primary in PRIMARY_SUFFIXES) and any(
            secondary in suffixes for secondary in SECONDARY_SUFFIXES
        ):
            offenders.append(f"{ticker}/{isin}: {', '.join(sorted(rics))}")
    return offenders


if SEED_PATH.exists():
    with SEED_PATH.open("r", encoding="utf-8") as handle:
        seed_offenders = _clean_alias_offenders(list(csv.DictReader(handle)))
    if seed_offenders:
        print("error: clean alias duplicates remain in security_master_seed.csv")
        for item in seed_offenders[:10]:
            print(f"  - {item}")
        failed = True
    else:
        print("ok: security_master_seed clean alias audit")

if DATA_DB.exists():
    conn = sqlite3.connect(str(DATA_DB))
    conn.row_factory = sqlite3.Row
    try:
        runtime_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT ric, ticker, isin, exchange_name
                FROM security_master
                """
            ).fetchall()
        ]
    except sqlite3.OperationalError as exc:
        print(f"warning: unable to audit local security_master: {exc}")
    else:
        runtime_offenders = _clean_alias_offenders(runtime_rows)
        if runtime_offenders:
            print("error: clean alias duplicates remain in local security_master")
            for item in runtime_offenders[:10]:
                print(f"  - {item}")
            failed = True
        else:
            print("ok: local security_master clean alias audit")
    finally:
        conn.close()

if failed:
    sys.exit(1)
PY
