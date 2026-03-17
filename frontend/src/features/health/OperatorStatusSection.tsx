"use client";

import type { OperatorStatusData } from "@/lib/types";
import { fmtTs, laneSummary, operatorTone } from "./utils";

function runtimeStatusTone(status: string | null | undefined): "" | "status-success" | "status-warning" | "status-error" {
  const clean = String(status || "").trim().toLowerCase();
  if (!clean || clean === "—") return "";
  if (
    clean === "ok" ||
    clean === "match" ||
    clean === "clean" ||
    clean === "completed" ||
    clean === "healthy" ||
    clean === "ready"
  ) {
    return "status-success";
  }
  if (
    clean === "running" ||
    clean === "pending" ||
    clean === "unknown" ||
    clean === "missing" ||
    clean === "skipped" ||
    clean.startsWith("dirty")
  ) {
    return "status-warning";
  }
  return "status-error";
}

function laneBehaviorSummary(lane: NonNullable<OperatorStatusData["lanes"]>[number]): string {
  const segments: string[] = [];
  const ingestPolicy = String(lane.ingest_policy || "none");
  const rebuildBackend = String(lane.rebuild_backend || "none");
  if (ingestPolicy === "local_lseg") segments.push("LSEG ingest -> local SQLite");
  if (lane.source_sync_required) segments.push("publish source window -> Neon");
  if (rebuildBackend === "neon") segments.push("core rebuild -> Neon");
  if (rebuildBackend === "local") segments.push("core rebuild -> local SQLite");
  if (lane.requires_neon_sync_before_core) segments.push("requires Neon sync first");
  if (lane.neon_readiness_required) segments.push("checks Neon retention/readiness");
  if (segments.length === 0) segments.push("no ingest / no core rebuild");
  return segments.join(" · ");
}

export default function OperatorStatusSection({
  data,
  error,
  isLoading,
}: {
  data?: OperatorStatusData;
  error?: unknown;
  isLoading: boolean;
}) {
  const sourceDates = data?.source_dates ?? {};
  const localArchiveSourceDates = data?.local_archive_source_dates ?? null;
  const neon = data?.neon_sync_health;
  const holdingsSync = data?.holdings_sync;

  return (
    <div className="chart-card">
      <div className="operator-header">
        <h3>Operator Status</h3>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span className="operator-header-meta">
            {data ? fmtTs(data.generated_at) : isLoading ? "Loading..." : "Unavailable"}
          </span>
          {data && (
            <span className={`operator-core-due ${data.core_due.due ? "due" : "clear"}`}>
              {data.core_due.due ? `Due: ${data.core_due.reason}` : "Up to date"}
            </span>
          )}
        </div>
      </div>

      {(data?.runtime?.dashboard_truth_plain_english
        || data?.runtime?.storage_contract_plain_english
        || data?.runtime?.source_authority_plain_english
        || data?.runtime?.rebuild_authority_plain_english
        || data?.runtime?.diagnostics_scope_plain_english
        || (Boolean(error) && !data)) && (
        <div className="operator-context-row">
          {data?.runtime?.storage_contract_plain_english && (
            <span className="operator-context-item">{data.runtime.storage_contract_plain_english}</span>
          )}
          {data?.runtime?.dashboard_truth_plain_english && (
            <span className="operator-context-item">{data.runtime.dashboard_truth_plain_english}</span>
          )}
          {data?.runtime?.source_authority_plain_english && (
            <span className="operator-context-item">{data.runtime.source_authority_plain_english}</span>
          )}
          {data?.runtime?.rebuild_authority_plain_english && (
            <span className="operator-context-item">{data.runtime.rebuild_authority_plain_english}</span>
          )}
          {data?.runtime?.diagnostics_scope_plain_english && (
            <span className="operator-context-item">{data.runtime.diagnostics_scope_plain_english}</span>
          )}
          {Boolean(error) && !data && (
            <span className="operator-context-item">Operator status endpoint is unavailable.</span>
          )}
        </div>
      )}

      {(data?.lanes ?? []).length > 0 && (
        <div className="operator-lanes">
          {(data?.lanes ?? []).map((lane) => {
            const tone = operatorTone(lane.latest_run.status);
            return (
              <div key={lane.profile} className={`operator-lane-card ${tone}`} data-tip={lane.description || undefined}>
                <div className="operator-lane-label">{lane.label}</div>
                <div className="operator-lane-status">{lane.latest_run.status}</div>
                <div className="operator-lane-detail">{laneSummary(lane)}</div>
                <div className="operator-lane-detail">{laneBehaviorSummary(lane)}</div>
                {lane.default_stages.length > 0 && (
                  <div className="operator-lane-stages">
                    {lane.default_stages.map((stage) => (
                      <span key={stage} className="operator-lane-stage-pill">{stage}</span>
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      <div className="operator-info-grid">
        <div className="operator-info-card">
          <h4>Authoritative Source Recency</h4>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Most recent trading-day close prices loaded from the market data vendor.">Prices</span>
            <span className="kv-value">{sourceDates.prices_asof ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Latest financial statement data (book value, earnings, etc.) used for value and growth factor exposures.">Fundamentals</span>
            <span className="kv-value">{sourceDates.fundamentals_asof ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Industry and sector classification mappings (e.g. TRBC) assigned to each security in the universe.">Classification</span>
            <span className="kv-value">{sourceDates.classification_asof ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Latest computed factor exposures (betas) for every security in the cross-section.">Cross Section</span>
            <span className="kv-value">{sourceDates.exposures_asof ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Most recent date for which daily factor returns have been estimated by the risk engine.">Factor Returns</span>
            <span className="kv-value">{data?.risk_engine?.factor_returns_latest_date ?? "—"}</span>
          </div>
        </div>

        {localArchiveSourceDates && (
          <div className="operator-info-card">
            <h4>Local Ingest Archive</h4>
            <div className="operator-kv-item">
              <span className="kv-label" data-tip="Local SQLite is the LSEG landing zone and deep archive retained on this machine.">Prices</span>
              <span className="kv-value">{localArchiveSourceDates.prices_asof ?? "—"}</span>
            </div>
            <div className="operator-kv-item">
              <span className="kv-label">Fundamentals</span>
              <span className="kv-value">{localArchiveSourceDates.fundamentals_asof ?? "—"}</span>
            </div>
            <div className="operator-kv-item">
              <span className="kv-label">Classification</span>
              <span className="kv-value">{localArchiveSourceDates.classification_asof ?? "—"}</span>
            </div>
            <div className="operator-kv-item">
              <span className="kv-label">Cross Section</span>
              <span className="kv-value">{localArchiveSourceDates.exposures_asof ?? "—"}</span>
            </div>
          </div>
        )}

        <div className="operator-info-card">
          <h4>Runtime Health</h4>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Whether the pipeline is currently running a data refresh cycle. 'ok' means idle; 'running' means a refresh is in progress.">Refresh</span>
            <span className={`kv-value ${runtimeStatusTone(data?.refresh?.status)}`.trim()}>
              {data?.refresh?.status ?? "—"}
            </span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Whether portfolio holdings are in sync with the database. 'Dirty' means pending changes haven't been processed yet.">Holdings</span>
            <span className={`kv-value ${runtimeStatusTone(holdingsSync?.pending ? "dirty" : "clean")}`.trim()}>
              {holdingsSync?.pending ? `Dirty (${holdingsSync.pending_count || 0})` : "Clean"}
            </span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Sync status between the local database and the Neon cloud mirror. 'match' means they agree; anything else signals drift.">Neon Mirror</span>
            <span className={`kv-value ${runtimeStatusTone(neon?.mirror_status ?? neon?.status)}`.trim()}>
              {neon?.mirror_status ?? neon?.status ?? "—"}
            </span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Which database currently acts as the operating source-of-truth for source recency and normal app reads.">Source Authority</span>
            <span className="kv-value">{data?.runtime?.source_authority ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Which database core-weekly and cold-core are currently configured to rebuild from.">Core Rebuilds</span>
            <span className="kv-value">{data?.runtime?.rebuild_authority ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="The active data snapshot ID currently being used to serve risk analytics and exposures.">Snapshot</span>
            <span className="kv-value">{data?.active_snapshot?.snapshot_id ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label" data-tip="Version of the regression and risk engine method currently active (e.g. WLS with specific factor set).">Risk Engine</span>
            <span className="kv-value">{data?.risk_engine?.method_version ?? "—"}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
