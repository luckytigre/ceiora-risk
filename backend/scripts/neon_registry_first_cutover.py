#!/usr/bin/env python3
"""End-to-end registry-first Neon cutover helper."""

from __future__ import annotations

import argparse
import gc
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.data import core_reads, cpar_source_reads, holdings_reads
from backend.data.neon import connect, resolve_dsn
from backend.data.serving_outputs import load_runtime_payload
from backend.orchestration.cpar_profiles import resolve_package_date
from backend.orchestration.run_cpar_pipeline import run_cpar_pipeline
from backend.orchestration.run_model_pipeline import run_model_pipeline
from backend.services.neon_authority import validate_neon_rebuild_readiness
from backend.services.neon_stage2 import (
    apply_sql_file,
    canonical_tables,
    inspect_sqlite_source_integrity,
    sync_from_sqlite_to_neon,
)
from backend.universe.bootstrap import bootstrap_cuse4_source_tables

REQUIRED_SOURCE_SYNC_TABLES = (
    "security_registry",
    "security_taxonomy_current",
    "security_policy_current",
    "security_source_observation_daily",
    "security_ingest_runs",
    "security_ingest_audit",
    "security_master_compat_current",
    "security_prices_eod",
    "security_fundamentals_pit",
    "security_classification_pit",
    "estu_membership_daily",
    "universe_cross_section_snapshot",
)

REQUIRED_SOURCE_SYNC_NONEMPTY_TABLES = (
    "security_registry",
    "security_taxonomy_current",
    "security_policy_current",
    "security_source_observation_daily",
    "security_master_compat_current",
    "security_prices_eod",
    "estu_membership_daily",
    "universe_cross_section_snapshot",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=None, help="Neon DSN (defaults to NEON_DATABASE_URL)")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path(config.DATA_DB_PATH),
        help="Active local SQLite source DB.",
    )
    parser.add_argument(
        "--seed-path",
        type=Path,
        default=Path("data/reference/security_registry_seed.csv"),
        help="Registry seed used for local bootstrap.",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=Path("backend/runtime/cutover_snapshots"),
        help="Directory for point-in-time SQLite publication copies.",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=None,
        help="Directory for per-run cutover result artifacts (defaults under snapshot-dir).",
    )
    parser.add_argument(
        "--canonical-schema",
        type=Path,
        default=Path("docs/reference/migrations/neon/NEON_CANONICAL_SCHEMA.sql"),
        help="Canonical Neon schema SQL path.",
    )
    parser.add_argument(
        "--cpar-schema",
        type=Path,
        default=Path("docs/reference/migrations/neon/NEON_CPAR_SCHEMA.sql"),
        help="cPAR Neon schema SQL path.",
    )
    parser.add_argument(
        "--holdings-schema",
        type=Path,
        default=Path("docs/reference/migrations/neon/NEON_HOLDINGS_SCHEMA.sql"),
        help="Holdings Neon schema SQL path.",
    )
    parser.add_argument(
        "--cleanup-schema",
        type=Path,
        default=Path("docs/reference/migrations/neon/NEON_REGISTRY_FIRST_CLEANUP.sql"),
        help="Destructive cleanup SQL path.",
    )
    parser.add_argument(
        "--sync-mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Source sync mode for SQLite -> Neon publication.",
    )
    parser.add_argument(
        "--historical-cuse-samples",
        type=int,
        default=2,
        help="Number of historical cUSE dates to rebuild for validation after the latest run.",
    )
    parser.add_argument(
        "--cpar-max-backfill",
        type=int,
        default=0,
        help="Maximum historical cPAR package dates to rebuild. 0 means all retained local package dates.",
    )
    parser.add_argument(
        "--include-holdings",
        action="store_true",
        help="Also apply holdings schema.",
    )
    parser.add_argument(
        "--include-cleanup",
        action="store_true",
        help="Also apply destructive cleanup SQL after validation stages complete.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args()


def _sqlite_backup(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        target_path.unlink()
    source = sqlite3.connect(str(source_path))
    try:
        target = sqlite3.connect(str(target_path))
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()


def _latest_source_date(sqlite_path: Path) -> str:
    conn = sqlite3.connect(str(sqlite_path))
    try:
        row = conn.execute(
            "SELECT MAX(date) FROM security_prices_eod WHERE date IS NOT NULL"
        ).fetchone()
    finally:
        conn.close()
    if not row or row[0] is None:
        raise RuntimeError("could not resolve latest security_prices_eod date from local SQLite")
    return str(row[0])


def _emit_progress(event: str, **payload: Any) -> None:
    message = {"event": str(event), **payload}
    print(json.dumps(message, sort_keys=True, default=str), file=sys.stderr, flush=True)


def _sanitize_artifact_name(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    clean = clean.strip("._")
    return clean or "artifact"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _write_json_artifact(artifact_dir: Path, name: str, payload: dict[str, Any]) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{_sanitize_artifact_name(name)}.json"
    artifact_path.write_text(
        json.dumps(payload, indent=2, default=_json_default),
        encoding="utf-8",
    )
    return artifact_path


def _stage_statuses(result: dict[str, Any]) -> list[dict[str, Any]]:
    statuses: list[dict[str, Any]] = []
    for item in result.get("stage_results") or []:
        if not isinstance(item, dict):
            continue
        status = {
            "stage": str(item.get("stage") or ""),
            "status": str(item.get("status") or ""),
        }
        error = item.get("error")
        if isinstance(error, dict) and error:
            status["error_type"] = str(error.get("type") or "")
            status["error_message"] = str(error.get("message") or "")
        statuses.append(status)
    return statuses


def _summarize_pipeline_result(result: dict[str, Any], *, artifact_path: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "status": str(result.get("status") or ""),
        "run_id": str(result.get("run_id") or ""),
        "profile": str(result.get("profile") or ""),
        "profile_label": str(result.get("profile_label") or result.get("profile") or ""),
        "artifact_path": str(artifact_path),
        "selected_stage_count": len(result.get("selected_stages") or []),
        "stage_statuses": _stage_statuses(result),
        "run_row_count": len(result.get("run_rows") or []),
    }
    for key in ("as_of_date", "requested_as_of_date", "package_date", "core_reason", "raw_history_policy"):
        if key in result:
            summary[key] = result.get(key)
    if "core_will_run" in result:
        summary["core_will_run"] = bool(result.get("core_will_run"))
    workspace = result.get("workspace")
    if workspace is not None:
        summary["workspace"] = workspace
    neon_mirror = result.get("neon_mirror")
    if isinstance(neon_mirror, dict):
        summary["neon_mirror_status"] = str(neon_mirror.get("status") or "")
    local_mirror_sync = result.get("local_mirror_sync")
    if isinstance(local_mirror_sync, dict):
        summary["local_mirror_sync_status"] = str(local_mirror_sync.get("status") or "")
    return summary


def _make_stage_callback(run_key: str):
    def _callback(event: dict[str, Any]) -> None:
        payload = dict(event or {})
        _emit_progress(
            "pipeline_stage",
            run_key=run_key,
            stage=str(payload.get("stage") or ""),
            status=str(payload.get("status") or ""),
            stage_index=payload.get("stage_index"),
            stage_count=payload.get("stage_count"),
            progress_kind=str(payload.get("progress_kind") or ""),
            message=str(payload.get("message") or ""),
        )

    return _callback


def _run_and_record(
    *,
    run_key: str,
    artifact_dir: Path,
    runner,
    runner_kwargs: dict[str, Any],
) -> dict[str, Any]:
    _emit_progress("pipeline_run_start", run_key=run_key)
    runner_kwargs = dict(runner_kwargs)
    runner_kwargs["stage_callback"] = _make_stage_callback(run_key)
    result = runner(**runner_kwargs)
    artifact_path = _write_json_artifact(artifact_dir, run_key, result)
    summary = _summarize_pipeline_result(result, artifact_path=artifact_path)
    _emit_progress(
        "pipeline_run_complete",
        run_key=run_key,
        status=summary.get("status"),
        artifact_path=str(artifact_path),
    )
    del result
    gc.collect()
    return summary


def _validate_required_snapshot_tables(
    sqlite_path: Path,
    *,
    required_tables: tuple[str, ...],
    required_nonempty_tables: tuple[str, ...],
) -> dict[str, Any]:
    conn = sqlite3.connect(str(sqlite_path))
    try:
        missing_tables: list[str] = []
        empty_tables: list[str] = []
        for table in required_tables:
            row = conn.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type='table' AND name=?
                LIMIT 1
                """,
                (table,),
            ).fetchone()
            if row is None:
                missing_tables.append(str(table))
                continue
            if table in required_nonempty_tables:
                count_row = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
                if not count_row or int(count_row[0] or 0) <= 0:
                    empty_tables.append(str(table))
    finally:
        conn.close()
    if missing_tables or empty_tables:
        parts: list[str] = []
        if missing_tables:
            parts.append("missing tables: " + ", ".join(sorted(missing_tables)))
        if empty_tables:
            parts.append("empty required tables: " + ", ".join(sorted(empty_tables)))
        raise RuntimeError("snapshot preflight failed: " + "; ".join(parts))
    return {
        "status": "ok",
        "required_tables": list(required_tables),
        "required_nonempty_tables": list(required_nonempty_tables),
    }


def _pg_table_exists(dsn: str | None, table: str) -> bool:
    conn = connect(dsn=resolve_dsn(dsn), autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = %s
                LIMIT 1
                """,
                (str(table),),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _probe_live_legacy_cleanliness(*, dsn: str | None) -> dict[str, Any]:
    conn = connect(dsn=resolve_dsn(dsn), autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND column_name IN ('sid', 'permid', 'instrument_type', 'asset_category_description')
                ORDER BY table_name, column_name
                """
            )
            legacy_columns = [
                {"table_name": str(row[0]), "column_name": str(row[1])}
                for row in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT schemaname, tablename, indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND (
                    tablename = 'security_master'
                    OR indexname LIKE 'idx_security_master%%'
                    OR tablename LIKE 'security_master%%'
                  )
                ORDER BY tablename, indexname
                """
            )
            legacy_indexes = [
                {
                    "schema_name": str(row[0]),
                    "table_name": str(row[1]),
                    "index_name": str(row[2]),
                }
                for row in cur.fetchall()
                if str(row[1]) != "security_master_compat_current"
            ]
    finally:
        conn.close()
    issues: list[str] = []
    if legacy_columns:
        issues.append("legacy_columns_present")
    if legacy_indexes:
        issues.append("legacy_indexes_present")
    return {
        "status": "ok" if not issues else "failed",
        "issues": issues,
        "legacy_columns": legacy_columns,
        "legacy_indexes": legacy_indexes,
    }


def _run_post_cleanup_sync_probe(*, sqlite_path: Path, dsn: str | None) -> dict[str, Any]:
    return sync_from_sqlite_to_neon(
        sqlite_path=sqlite_path,
        dsn=dsn,
        tables=["security_registry"],
        mode="incremental",
        verify_source_integrity=False,
        run_sqlite_integrity_check=False,
    )


def _run_post_cleanup_checks(*, dsn: str | None, include_holdings: bool, sqlite_path: Path) -> dict[str, Any]:
    table_presence = {
        "security_registry": _pg_table_exists(dsn, "security_registry"),
        "security_policy_current": _pg_table_exists(dsn, "security_policy_current"),
        "security_taxonomy_current": _pg_table_exists(dsn, "security_taxonomy_current"),
        "security_master_compat_current": _pg_table_exists(dsn, "security_master_compat_current"),
        "source_sync_runs": _pg_table_exists(dsn, "source_sync_runs"),
        "source_sync_watermarks": _pg_table_exists(dsn, "source_sync_watermarks"),
        "security_source_status_current": _pg_table_exists(dsn, "security_source_status_current"),
        "security_master": _pg_table_exists(dsn, "security_master"),
    }
    if table_presence["security_master"]:
        raise RuntimeError("post-cleanup verification failed: security_master still exists in Neon")
    required_tables = [
        name
        for name in ("source_sync_runs", "source_sync_watermarks", "security_source_status_current")
        if not table_presence.get(name)
    ]
    if required_tables:
        raise RuntimeError(
            "post-cleanup verification failed: missing metadata tables: "
            + ", ".join(sorted(required_tables))
        )

    source_sync_probe = _run_post_cleanup_sync_probe(sqlite_path=sqlite_path, dsn=dsn)
    if str(source_sync_probe.get("status") or "") != "ok":
        raise RuntimeError("post-cleanup verification failed: source sync probe did not succeed")
    if not str(source_sync_probe.get("sync_run_id") or "").strip():
        raise RuntimeError("post-cleanup verification failed: source sync probe did not record sync_run_id")
    if int(source_sync_probe.get("watermark_rows_updated") or 0) <= 0:
        raise RuntimeError("post-cleanup verification failed: source sync probe did not update watermarks")
    if int(source_sync_probe.get("security_source_status_current_rows") or 0) <= 0:
        raise RuntimeError(
            "post-cleanup verification failed: source sync probe did not materialize security_source_status_current"
        )

    legacy_schema_cleanliness = _probe_live_legacy_cleanliness(dsn=dsn)
    if str(legacy_schema_cleanliness.get("status") or "") != "ok":
        raise RuntimeError("post-cleanup verification failed: legacy schema/index artifacts remain in Neon")

    latest_prices = core_reads.load_latest_prices()
    latest_fundamentals = core_reads.load_latest_fundamentals()
    cpar_build_universe = cpar_source_reads.load_build_universe_rows()
    cpar_factor_proxy_rows = cpar_source_reads.resolve_factor_proxy_rows(["SPY", "QQQ"])
    universe_payload = load_runtime_payload("universe_loadings")
    risk_payload = load_runtime_payload("risk")
    portfolio_payload = load_runtime_payload("portfolio")

    out: dict[str, Any] = {
        "status": "ok",
        "table_presence": table_presence,
        "post_cleanup_sync_probe": source_sync_probe,
        "legacy_schema_cleanliness": legacy_schema_cleanliness,
        "latest_prices_rows": int(len(latest_prices.index)),
        "latest_fundamentals_rows": int(len(latest_fundamentals.index)),
        "cpar_build_universe_rows": int(len(cpar_build_universe)),
        "cpar_factor_proxy_rows": int(len(cpar_factor_proxy_rows)),
        "universe_payload_keys": int(len(universe_payload) if isinstance(universe_payload, dict) else 0),
        "risk_payload_keys": int(len(risk_payload) if isinstance(risk_payload, dict) else 0),
        "portfolio_payload_keys": int(len(portfolio_payload) if isinstance(portfolio_payload, dict) else 0),
    }

    if include_holdings:
        accounts = holdings_reads.load_holdings_accounts()
        out["holdings_accounts"] = int(len(accounts))
        if accounts:
            account_id = str(accounts[0].get("account_id") or "")
            positions = holdings_reads.load_holdings_positions(account_id=account_id)
            out["holdings_positions_rows"] = int(len(positions))
            out["holdings_account_id_checked"] = account_id
        else:
            out["holdings_positions_rows"] = 0
            out["holdings_account_id_checked"] = None
    return out


def _historical_cuse_sample_dates(sqlite_path: Path, latest_date: str, limit: int) -> list[str]:
    if limit <= 0:
        return []
    conn = sqlite3.connect(str(sqlite_path))
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM model_factor_returns_daily
            WHERE date IS NOT NULL
              AND date < ?
            ORDER BY date DESC
            LIMIT 252
            """,
            (latest_date,),
        ).fetchall()
    finally:
        conn.close()
    dates = [str(row[0]) for row in rows if row and row[0]]
    if not dates:
        return []
    offsets = [63, 126, 189]
    out: list[str] = []
    for offset in offsets:
        if len(out) >= limit:
            break
        idx = min(len(dates) - 1, offset)
        candidate = dates[idx]
        if candidate not in out:
            out.append(candidate)
    if len(out) < limit:
        for candidate in dates:
            if candidate not in out:
                out.append(candidate)
            if len(out) >= limit:
                break
    return out[:limit]


def _historical_cpar_package_dates(sqlite_path: Path, latest_package_date: str, max_backfill: int) -> list[str]:
    conn = sqlite3.connect(str(sqlite_path))
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT package_date
            FROM cpar_package_runs
            WHERE package_date IS NOT NULL
              AND TRIM(COALESCE(status, '')) = 'ok'
              AND package_date <= ?
            ORDER BY package_date DESC
            """,
            (latest_package_date,),
        ).fetchall()
    finally:
        conn.close()
    dates = [str(row[0]) for row in rows if row and row[0]]
    if max_backfill > 0:
        return dates[: max(0, int(max_backfill))]
    return dates


def _apply_schema_stack(
    *,
    dsn: str | None,
    canonical_schema: Path,
    cpar_schema: Path,
    holdings_schema: Path,
    include_holdings: bool,
) -> list[dict[str, Any]]:
    conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    try:
        applied = [
            apply_sql_file(conn, sql_path=canonical_schema),
            apply_sql_file(conn, sql_path=cpar_schema),
        ]
        if include_holdings:
            applied.append(apply_sql_file(conn, sql_path=holdings_schema))
        return applied
    finally:
        conn.close()


def _apply_cleanup(*, dsn: str | None, cleanup_schema: Path) -> dict[str, Any]:
    conn = connect(dsn=resolve_dsn(dsn), autocommit=False)
    try:
        return apply_sql_file(conn, sql_path=cleanup_schema)
    finally:
        conn.close()


def main() -> int:
    args = _parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    seed_path = Path(args.seed_path).expanduser().resolve()
    snapshot_dir = Path(args.snapshot_dir).expanduser().resolve()
    dsn = resolve_dsn(args.dsn)

    out: dict[str, Any] = {
        "status": "ok",
        "db_path": str(db_path),
        "seed_path": str(seed_path),
    }

    bootstrap = bootstrap_cuse4_source_tables(
        db_path=db_path,
        seed_path=seed_path,
    )
    out["bootstrap"] = bootstrap

    snapshot_name = f"registry_first_cutover_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.db"
    snapshot_path = snapshot_dir / snapshot_name
    _sqlite_backup(db_path, snapshot_path)
    out["snapshot_path"] = str(snapshot_path)
    artifact_dir = (
        Path(args.artifact_dir).expanduser().resolve()
        if args.artifact_dir is not None
        else (snapshot_dir / f"{snapshot_path.stem}_artifacts").resolve()
    )
    out["artifact_dir"] = str(artifact_dir)

    out["snapshot_preflight"] = _validate_required_snapshot_tables(
        snapshot_path,
        required_tables=REQUIRED_SOURCE_SYNC_TABLES,
        required_nonempty_tables=REQUIRED_SOURCE_SYNC_NONEMPTY_TABLES,
    )
    out["source_integrity_preflight"] = inspect_sqlite_source_integrity(
        sqlite_path=snapshot_path,
        selected_tables=list(REQUIRED_SOURCE_SYNC_TABLES),
        run_sqlite_integrity_check=True,
    )
    if str(out["source_integrity_preflight"].get("status") or "") != "ok":
        raise RuntimeError(
            "cutover source-integrity preflight failed: "
            + "; ".join(list(out["source_integrity_preflight"].get("issues") or []))
        )

    out["schema_apply"] = _apply_schema_stack(
        dsn=dsn,
        canonical_schema=Path(args.canonical_schema).expanduser().resolve(),
        cpar_schema=Path(args.cpar_schema).expanduser().resolve(),
        holdings_schema=Path(args.holdings_schema).expanduser().resolve(),
        include_holdings=bool(args.include_holdings),
    )

    out["source_sync"] = sync_from_sqlite_to_neon(
        sqlite_path=snapshot_path,
        dsn=dsn,
        tables=canonical_tables(),
        mode=str(args.sync_mode),
        required_tables=list(REQUIRED_SOURCE_SYNC_TABLES),
        required_nonempty_tables=list(REQUIRED_SOURCE_SYNC_NONEMPTY_TABLES),
        verify_source_integrity=True,
        run_sqlite_integrity_check=True,
    )

    latest_date = _latest_source_date(snapshot_path)
    out["latest_source_date"] = latest_date
    out["neon_readiness_before_rebuild"] = validate_neon_rebuild_readiness(
        profile="cold-core",
        dsn=dsn,
    )

    out["cuse_latest"] = _run_and_record(
        run_key=f"cuse_latest_{latest_date}",
        artifact_dir=artifact_dir,
        runner=run_model_pipeline,
        runner_kwargs={
            "profile": "cold-core",
            "as_of_date": latest_date,
            "from_stage": "neon_readiness",
            "to_stage": "serving_refresh",
            "force_core": True,
        },
    )

    sample_dates = _historical_cuse_sample_dates(
        snapshot_path,
        latest_date=latest_date,
        limit=int(args.historical_cuse_samples),
    )
    out["cuse_historical_samples"] = []
    for sample_date in sample_dates:
        out["cuse_historical_samples"].append(
            _run_and_record(
                run_key=f"cuse_historical_{sample_date}",
                artifact_dir=artifact_dir,
                runner=run_model_pipeline,
                runner_kwargs={
                    "profile": "cold-core",
                    "as_of_date": sample_date,
                    "from_stage": "neon_readiness",
                    "to_stage": "serving_refresh",
                    "force_core": True,
                },
            )
        )

    latest_package_date = resolve_package_date(profile="cpar-weekly", as_of_date=latest_date)
    out["cpar_latest_package_date"] = latest_package_date
    out["cpar_latest"] = _run_and_record(
        run_key=f"cpar_latest_{latest_package_date}",
        artifact_dir=artifact_dir,
        runner=run_cpar_pipeline,
        runner_kwargs={
            "profile": "cpar-package-date",
            "as_of_date": latest_package_date,
        },
    )

    package_dates = _historical_cpar_package_dates(
        snapshot_path,
        latest_package_date=latest_package_date,
        max_backfill=int(args.cpar_max_backfill),
    )
    out["cpar_historical_backfill"] = []
    for package_date in package_dates:
        if package_date == latest_package_date:
            continue
        out["cpar_historical_backfill"].append(
            _run_and_record(
                run_key=f"cpar_historical_{package_date}",
                artifact_dir=artifact_dir,
                runner=run_cpar_pipeline,
                runner_kwargs={
                    "profile": "cpar-package-date",
                    "as_of_date": package_date,
                },
            )
        )

    if bool(args.include_cleanup):
        out["cleanup"] = _apply_cleanup(
            dsn=dsn,
            cleanup_schema=Path(args.cleanup_schema).expanduser().resolve(),
        )
        out["post_cleanup_checks"] = _run_post_cleanup_checks(
            dsn=dsn,
            include_holdings=bool(args.include_holdings),
            sqlite_path=snapshot_path,
        )
    else:
        out["cleanup"] = {"status": "skipped", "reason": "include_cleanup=false"}
        out["post_cleanup_checks"] = {"status": "skipped", "reason": "include_cleanup=false"}

    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
