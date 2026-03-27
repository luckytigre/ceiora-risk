"""Source-stage helpers for the model pipeline."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable


def run_source_stage(
    *,
    profile: str,
    stage: str,
    as_of_date: str,
    should_run_core: bool,
    core_reason: str,
    data_db: Path,
    cache_db: Path,
    enable_ingest: bool = False,
    workspace_root: Path | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    config_module,
    core_reads_module,
    bootstrap_cuse4_source_tables_fn: Callable[..., Any],
    download_from_lseg_fn: Callable[..., Any],
    repair_price_gap_fn: Callable[..., dict[str, Any]],
    repair_pit_gap_fn: Callable[..., dict[str, Any]],
    profile_source_sync_required_fn: Callable[..., bool],
    profile_neon_readiness_required_fn: Callable[..., bool],
    run_neon_mirror_cycle_fn: Callable[..., dict[str, Any]],
    neon_authority_module,
) -> dict[str, Any]:
    if stage == "ingest":
        if progress_callback is not None:
            progress_callback({"message": "Bootstrapping source tables", "progress_kind": "stage"})
        bootstrap = bootstrap_cuse4_source_tables_fn(
            db_path=data_db,
        )
        if not config_module.runtime_role_allows_ingest():
            return {
                "status": "skipped",
                "mode": "bootstrap_only",
                "reason": "runtime_role_disallows_ingest",
                "bootstrap": bootstrap,
                "runtime_role": str(config_module.APP_RUNTIME_ROLE),
            }
        if not enable_ingest:
            return {
                "status": "ok",
                "mode": "bootstrap_only",
                "reason": "profile_skip_lseg_ingest",
                "bootstrap": bootstrap,
                "runtime_role": str(config_module.APP_RUNTIME_ROLE),
            }
        if not bool(config_module.ORCHESTRATOR_ENABLE_INGEST):
            return {
                "status": "ok",
                "mode": "bootstrap_only",
                "reason": "ORCHESTRATOR_ENABLE_INGEST=false",
                "bootstrap": bootstrap,
                "runtime_role": str(config_module.APP_RUNTIME_ROLE),
            }
        if progress_callback is not None:
            progress_callback({"message": "Pulling latest source data from LSEG", "progress_kind": "io"})
        latest_price_date_before_ingest = _latest_price_date(data_db)
        ingest = download_from_lseg_fn(
            db_path=data_db,
            as_of_date=as_of_date,
            shard_count=1,
            shard_index=0,
            write_fundamentals=False,
            write_prices=True,
            write_classification=False,
        )
        price_gap_repair = {"status": "skipped", "reason": "ingest_not_ok"}
        pit_gap_repair = {"status": "skipped", "reason": "ingest_not_ok"}
        if str(ingest.get("status") or "").strip().lower() == "ok":
            price_gap_repair = repair_price_gap_fn(
                data_db=data_db,
                as_of_date=as_of_date,
                latest_price_date_before_ingest=latest_price_date_before_ingest,
                progress_callback=progress_callback,
            )
            pit_gap_repair = repair_pit_gap_fn(
                data_db=data_db,
                as_of_date=as_of_date,
                progress_callback=progress_callback,
            )
        return {
            "status": str(ingest.get("status") or "ok"),
            "mode": "bootstrap_plus_lseg_ingest",
            "bootstrap": bootstrap,
            "ingest": ingest,
            "price_gap_repair": price_gap_repair,
            "pit_gap_repair": pit_gap_repair,
        }

    if stage == "source_sync":
        if not profile_source_sync_required_fn(profile):
            return {
                "status": "skipped",
                "reason": "profile_skip_source_sync",
            }
        bootstrap = bootstrap_cuse4_source_tables_fn(
            db_path=data_db,
        )
        dsn = str(config_module.NEON_DATABASE_URL or "").strip()
        if not dsn:
            raise RuntimeError("source_sync requires NEON_DATABASE_URL for Neon-authoritative profiles.")
        snapshot_path = _create_source_sync_snapshot(
            db_path=data_db,
            snapshot_root=Path(config_module.APP_DATA_DIR) / "source_sync_snapshots",
        )
        local_source_dates: dict[str, Any] | None = None
        neon_source_dates: dict[str, Any] | None = None
        try:
            try:
                with core_reads_module.core_read_backend("local"):
                    local_source_dates = core_reads_module.load_source_dates(data_db=snapshot_path)
                with core_reads_module.core_read_backend("neon"):
                    neon_source_dates = core_reads_module.load_source_dates(data_db=snapshot_path)
            except Exception as exc:
                raise RuntimeError(
                    "source_sync could not load source dates for local/neon authority comparison"
                ) from exc
            required_local_fields = ("prices_asof", "fundamentals_asof", "classification_asof")
            missing_local_source_dates = [
                field
                for field in required_local_fields
                if not str((local_source_dates or {}).get(field) or "").strip()
            ]
            if missing_local_source_dates:
                raise RuntimeError(
                    "source_sync requires non-empty local source dates before syncing Neon: "
                    + ", ".join(sorted(missing_local_source_dates))
                )
            older_than_neon: list[str] = []
            newer_than_target: list[str] = []
            pit_latest_closed_anchor = _latest_closed_period_anchor(
                str(as_of_date),
                frequency=str(config_module.SOURCE_DAILY_PIT_FREQUENCY or "monthly").strip().lower(),
            )
            if isinstance(local_source_dates, dict) and isinstance(neon_source_dates, dict):
                for field in ("prices_asof", "fundamentals_asof", "classification_asof"):
                    local_value = str(local_source_dates.get(field) or "").strip()
                    neon_value = str(neon_source_dates.get(field) or "").strip()
                    allowed_ceiling = str(as_of_date)
                    if field in {"fundamentals_asof", "classification_asof"}:
                        allowed_ceiling = pit_latest_closed_anchor
                    if local_value and neon_value and neon_value > allowed_ceiling:
                        newer_than_target.append(field)
                        continue
                    if local_value and neon_value and local_value < neon_value:
                        older_than_neon.append(field)
            if newer_than_target:
                raise RuntimeError(
                    "source_sync refused to overwrite newer-than-target Neon source tables: "
                    + ", ".join(sorted(newer_than_target))
                )
            if older_than_neon:
                raise RuntimeError(
                    "source_sync refused to overwrite newer Neon source tables from an older local archive: "
                    + ", ".join(sorted(older_than_neon))
                )
            local_current_state_preflight = _validate_local_current_state_sync_surfaces(
                db_path=snapshot_path,
                required_observation_date=str((local_source_dates or {}).get("prices_asof") or "").strip() or None,
            )
            if progress_callback is not None:
                progress_callback({"message": "Syncing retained source/model window into Neon", "progress_kind": "io"})
            out = run_neon_mirror_cycle_fn(
                sqlite_path=snapshot_path,
                cache_path=cache_db,
                dsn=dsn,
                mode=str(config_module.NEON_AUTO_SYNC_MODE or "incremental"),
                tables=[
                    "security_registry",
                    "security_taxonomy_current",
                    "security_policy_current",
                    "security_source_observation_daily",
                    "security_master_compat_current",
                    "security_ingest_runs",
                    "security_ingest_audit",
                    "security_prices_eod",
                    "security_fundamentals_pit",
                    "security_classification_pit",
                    "estu_membership_daily",
                    "universe_cross_section_snapshot",
                ],
                parity_enabled=False,
                prune_enabled=False,
                source_years=int(config_module.NEON_SOURCE_RETENTION_YEARS),
                analytics_years=int(config_module.NEON_ANALYTICS_RETENTION_YEARS),
            )
            if str(out.get("status") or "") != "ok":
                raise RuntimeError(f"source_sync stage failed: {out}")
            sync_payload = dict(out.get("sync") or {})
            if not str(sync_payload.get("sync_run_id") or "").strip():
                raise RuntimeError("source_sync stage failed to persist sync_run_id metadata.")
            if int(sync_payload.get("watermark_rows_updated") or 0) <= 0:
                raise RuntimeError("source_sync stage failed to persist source_sync_watermarks.")
            if int(sync_payload.get("security_source_status_current_rows") or 0) <= 0:
                raise RuntimeError("source_sync stage failed to materialize security_source_status_current.")
            return {
                "status": "ok",
                "local_source_dates": local_source_dates,
                "neon_source_dates_before_sync": neon_source_dates,
                "ignored_newer_than_target": [],
                "local_current_state_preflight": local_current_state_preflight,
                "source_sync": out,
                "bootstrap": bootstrap,
                "snapshot_path": str(snapshot_path),
            }
        finally:
            try:
                os.unlink(snapshot_path)
            except FileNotFoundError:
                pass

    if stage == "neon_readiness":
        if not profile_neon_readiness_required_fn(profile):
            return {
                "status": "skipped",
                "reason": "profile_skip_neon_readiness",
            }
        if not should_run_core:
            return {
                "status": "skipped",
                "reason": f"core_policy_skip_{core_reason}",
            }
        root = Path(workspace_root or (Path(config_module.APP_DATA_DIR) / "neon_rebuild_workspace" / "adhoc"))
        if progress_callback is not None:
            progress_callback({"message": "Preparing Neon-authoritative scratch workspace", "progress_kind": "io"})
        out = neon_authority_module.prepare_neon_rebuild_workspace(
            profile=profile,
            workspace_root=root,
            dsn=(str(config_module.NEON_DATABASE_URL).strip() or None),
            analytics_years=int(config_module.NEON_ANALYTICS_RETENTION_YEARS),
        )
        return {
            "status": "ok",
            **out,
        }

    raise ValueError(f"Unsupported source stage: {stage}")


def _latest_price_date(db_path: Path) -> str | None:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT MAX(date) FROM security_prices_eod WHERE date IS NOT NULL").fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    return str(row[0]) if row and row[0] is not None else None


def _validate_local_current_state_sync_surfaces(
    *,
    db_path: Path,
    required_observation_date: str | None,
) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    try:
        issues: list[str] = []
        required_tables = (
            "security_registry",
            "security_policy_current",
            "security_taxonomy_current",
            "security_master_compat_current",
            "security_source_observation_daily",
        )
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
                issues.append(f"missing_table:{table}")
        if issues:
            raise RuntimeError(
                "source_sync refused to replace Neon current-state tables because local current-state surfaces are incomplete: "
                + ", ".join(sorted(issues))
            )

        registry_n = int(
            (
                conn.execute(
                    """
                    SELECT COUNT(DISTINCT UPPER(TRIM(ric)))
                    FROM security_registry
                    WHERE ric IS NOT NULL
                      AND TRIM(ric) <> ''
                      AND COALESCE(NULLIF(TRIM(tracking_status), ''), 'active') <> 'disabled'
                    """
                ).fetchone()
                or (0,)
            )[0]
            or 0
        )
        if registry_n <= 0:
            issues.append("empty_table:security_registry")
        else:
            for table in (
                "security_policy_current",
                "security_taxonomy_current",
                "security_master_compat_current",
            ):
                covered_n = int(
                    (
                        conn.execute(
                            f"""
                            SELECT COUNT(DISTINCT UPPER(TRIM(reg.ric)))
                            FROM security_registry reg
                            JOIN {table} cur
                              ON UPPER(TRIM(cur.ric)) = UPPER(TRIM(reg.ric))
                            WHERE reg.ric IS NOT NULL
                              AND TRIM(reg.ric) <> ''
                              AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
                            """
                        ).fetchone()
                        or (0,)
                    )[0]
                    or 0
                )
                if covered_n < registry_n:
                    issues.append(f"incomplete_table:{table}:{covered_n}/{registry_n}")

        observation_max = str(
            (
                conn.execute(
                    """
                    SELECT MAX(as_of_date)
                    FROM security_source_observation_daily
                    WHERE as_of_date IS NOT NULL
                      AND TRIM(as_of_date) <> ''
                    """
                ).fetchone()
                or ("",)
            )[0]
            or ""
        ).strip()
        if not observation_max:
            issues.append("missing_max_date:security_source_observation_daily")
        else:
            if required_observation_date and observation_max < required_observation_date:
                issues.append(
                    "stale_table:security_source_observation_daily:"
                    f"{observation_max}<{required_observation_date}"
                )
            if registry_n > 0:
                observed_n = int(
                    (
                        conn.execute(
                            """
                            SELECT COUNT(DISTINCT UPPER(TRIM(reg.ric)))
                            FROM security_registry reg
                            JOIN security_source_observation_daily obs
                              ON UPPER(TRIM(obs.ric)) = UPPER(TRIM(reg.ric))
                             AND obs.as_of_date = ?
                            WHERE reg.ric IS NOT NULL
                              AND TRIM(reg.ric) <> ''
                              AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
                            """,
                            (observation_max,),
                        ).fetchone()
                        or (0,)
                    )[0]
                    or 0
                )
                if observed_n < registry_n:
                    issues.append(
                        f"incomplete_table:security_source_observation_daily:{observed_n}/{registry_n}@{observation_max}"
                    )

        if issues:
            raise RuntimeError(
                "source_sync refused to replace Neon current-state tables because local current-state surfaces are incomplete: "
                + ", ".join(sorted(issues))
            )
        return {
            "status": "ok",
            "registry_rows": registry_n,
            "observation_max_date": observation_max or None,
        }
    finally:
        conn.close()


def _latest_closed_period_anchor(as_of_date: str, *, frequency: str) -> str:
    parsed = date.fromisoformat(str(as_of_date)[:10])
    if frequency == "quarterly":
        quarter_start_month = (((parsed.month - 1) // 3) * 3) + 1
        current_period_start = date(parsed.year, quarter_start_month, 1)
    else:
        current_period_start = date(parsed.year, parsed.month, 1)
    return _previous_or_same_xnys_session((current_period_start - timedelta(days=1)).isoformat())


def _previous_or_same_xnys_session(value: str) -> str:
    from backend.trading_calendar import previous_or_same_xnys_session

    return previous_or_same_xnys_session(value)


def _create_source_sync_snapshot(*, db_path: Path, snapshot_root: Path) -> Path:
    snapshot_root.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        prefix="source_sync_",
        suffix=".db",
        dir=str(snapshot_root),
        delete=False,
    )
    snapshot_path = Path(handle.name)
    handle.close()
    source = sqlite3.connect(str(db_path))
    try:
        target = sqlite3.connect(str(snapshot_path))
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()
    return snapshot_path
