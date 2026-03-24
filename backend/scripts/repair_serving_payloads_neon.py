from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from backend import config
from backend.data import serving_outputs


def _parse_payload_names(raw_values: list[str] | None) -> list[str]:
    values: list[str] = []
    for raw in raw_values or []:
        for chunk in str(raw or "").split(","):
            clean = str(chunk or "").strip()
            if clean:
                values.append(clean)
    return serving_outputs._normalize_payload_names(values)


def _load_local_payloads(
    data_db: Path,
    *,
    payload_names: list[str],
) -> tuple[dict[str, Any], str, str, str]:
    conn = sqlite3.connect(str(data_db))
    try:
        placeholders = ",".join("?" for _ in payload_names)
        rows = conn.execute(
            """
            SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json
            FROM serving_payload_current
            WHERE payload_name IN ("""
            + placeholders
            + """)
            ORDER BY payload_name
            """,
            payload_names,
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        raise RuntimeError(f"no local serving payloads found in {data_db} for payload_names={payload_names}")

    payloads = {
        str(row[0]): json.loads(str(row[4]))
        for row in rows
    }
    snapshot_ids = {str(row[1] or "").strip() for row in rows}
    run_ids = {str(row[2] or "").strip() for row in rows}
    refresh_modes = {str(row[3] or "").strip() for row in rows}
    if len(snapshot_ids) != 1 or len(run_ids) != 1 or len(refresh_modes) != 1:
        raise RuntimeError(
            "selected local serving payloads are not on a single snapshot/run/mode: "
            f"snapshot_ids={sorted(snapshot_ids)} run_ids={sorted(run_ids)} refresh_modes={sorted(refresh_modes)}"
        )
    missing = sorted(set(payload_names) - set(payloads.keys()))
    if missing:
        raise RuntimeError(f"local serving payloads missing requested payloads: {missing}")
    return payloads, next(iter(snapshot_ids)), next(iter(run_ids)), next(iter(refresh_modes))


def _build_summary(
    *,
    local_manifest: dict[str, Any],
    neon_manifest: dict[str, Any],
    diff: dict[str, Any],
    result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = {
        "payload_names": list(local_manifest.get("requested_payload_names") or local_manifest.get("payload_names") or []),
        "local_snapshot_ids": list(local_manifest.get("distinct_snapshot_ids") or []),
        "local_run_ids": list(local_manifest.get("distinct_run_ids") or []),
        "local_refresh_modes": list(local_manifest.get("distinct_refresh_modes") or []),
        "neon_snapshot_ids": list(neon_manifest.get("distinct_snapshot_ids") or []),
        "neon_run_ids": list(neon_manifest.get("distinct_run_ids") or []),
        "neon_refresh_modes": list(neon_manifest.get("distinct_refresh_modes") or []),
        "canonical_payload_set_complete_local": bool(local_manifest.get("canonical_payload_set_complete")),
        "canonical_payload_set_complete_neon": bool(neon_manifest.get("canonical_payload_set_complete")),
        "diff": diff,
    }
    if result is not None:
        summary["write_result"] = result
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair Neon serving_payload_current rows from the local mirror.")
    parser.add_argument(
        "--data-db",
        default=str(config.DATA_DB_PATH),
        help="Path to local data.db with serving_payload_current",
    )
    parser.add_argument(
        "--payload-names",
        nargs="*",
        help="Optional payload names (space or comma separated). Defaults to the canonical serving payload set.",
    )
    parser.add_argument(
        "--snapshot-id",
        help="Require the selected local current payload rows to match this snapshot_id before writing.",
    )
    parser.add_argument(
        "--write-mode",
        choices=("bulk", "row_by_row"),
        default="bulk",
        help="Neon write mode. row_by_row keeps a single transaction but executes upserts one row at a time.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show local-vs-Neon drift without writing anything.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON.",
    )
    args = parser.parse_args()

    data_db = Path(args.data_db).expanduser().resolve()
    payload_names = _parse_payload_names(args.payload_names) or list(serving_outputs.canonical_serving_payload_names())
    payload_name_set = frozenset(payload_names)
    canonical_set = frozenset(serving_outputs.canonical_serving_payload_names())
    replace_all = payload_name_set == canonical_set

    local_manifest = serving_outputs.collect_current_payload_manifest(
        store="sqlite",
        payload_names=payload_names,
        data_db=data_db,
    )
    neon_manifest = serving_outputs.collect_current_payload_manifest(
        store="neon",
        payload_names=payload_names,
        data_db=data_db,
    )
    diff_before = serving_outputs.compare_current_payload_manifests(local_manifest, neon_manifest)

    payloads, snapshot_id, run_id, refresh_mode = _load_local_payloads(
        data_db,
        payload_names=payload_names,
    )
    required_snapshot_id = str(args.snapshot_id or "").strip()
    if required_snapshot_id and snapshot_id != required_snapshot_id:
        raise RuntimeError(
            f"selected local current snapshot_id {snapshot_id!r} does not match required --snapshot-id {required_snapshot_id!r}"
        )

    if args.dry_run:
        summary = _build_summary(
            local_manifest=local_manifest,
            neon_manifest=neon_manifest,
            diff=diff_before,
        )
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            print(json.dumps(summary, indent=2, sort_keys=True))
        return

    result = serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id=run_id,
        snapshot_id=snapshot_id,
        refresh_mode=refresh_mode,
        payloads=payloads,
        replace_all=replace_all,
        neon_write_mode=str(args.write_mode),
    )
    neon_manifest_after = serving_outputs.collect_current_payload_manifest(
        store="neon",
        payload_names=payload_names,
        data_db=data_db,
    )
    diff_after = serving_outputs.compare_current_payload_manifests(local_manifest, neon_manifest_after)
    summary = _build_summary(
        local_manifest=local_manifest,
        neon_manifest=neon_manifest_after,
        diff=diff_after,
        result=result,
    )
    summary.update(
        {
            "status": result.get("status"),
            "authority_store": result.get("authority_store"),
            "replace_all": replace_all,
            "snapshot_id": snapshot_id,
            "run_id": run_id,
            "refresh_mode": refresh_mode,
            "write_mode": args.write_mode,
        }
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
