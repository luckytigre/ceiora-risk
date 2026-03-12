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

      {(data?.runtime?.dashboard_truth_plain_english || data?.runtime?.diagnostics_scope_plain_english || (Boolean(error) && !data)) && (
        <div className="operator-context-row">
          {data?.runtime?.dashboard_truth_plain_english && (
            <span className="operator-context-item">{data.runtime.dashboard_truth_plain_english}</span>
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
              <div key={lane.profile} className={`operator-lane-card ${tone}`}>
                <div className="operator-lane-label">{lane.label}</div>
                <div className="operator-lane-status">{lane.latest_run.status}</div>
                <div className="operator-lane-detail">{laneSummary(lane)}</div>
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
          <h4>Source Recency</h4>
          <div className="operator-kv-item">
            <span className="kv-label">Prices</span>
            <span className="kv-value">{sourceDates.prices_asof ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label">Fundamentals</span>
            <span className="kv-value">{sourceDates.fundamentals_asof ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label">Classification</span>
            <span className="kv-value">{sourceDates.classification_asof ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label">Cross Section</span>
            <span className="kv-value">{sourceDates.exposures_asof ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label">Factor Returns</span>
            <span className="kv-value">{data?.risk_engine?.factor_returns_latest_date ?? "—"}</span>
          </div>
        </div>

        <div className="operator-info-card">
          <h4>Runtime Health</h4>
          <div className="operator-kv-item">
            <span className="kv-label">Refresh</span>
            <span className={`kv-value ${runtimeStatusTone(data?.refresh?.status)}`.trim()}>
              {data?.refresh?.status ?? "—"}
            </span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label">Holdings</span>
            <span className={`kv-value ${runtimeStatusTone(holdingsSync?.pending ? "dirty" : "clean")}`.trim()}>
              {holdingsSync?.pending ? `Dirty (${holdingsSync.pending_count || 0})` : "Clean"}
            </span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label">Neon Mirror</span>
            <span className={`kv-value ${runtimeStatusTone(neon?.mirror_status ?? neon?.status)}`.trim()}>
              {neon?.mirror_status ?? neon?.status ?? "—"}
            </span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label">Neon Parity</span>
            <span className={`kv-value ${runtimeStatusTone(neon?.parity_status)}`.trim()}>
              {neon?.parity_status ?? "—"}
            </span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label">Snapshot</span>
            <span className="kv-value">{data?.active_snapshot?.snapshot_id ?? "—"}</span>
          </div>
          <div className="operator-kv-item">
            <span className="kv-label">Risk Engine</span>
            <span className="kv-value">{data?.risk_engine?.method_version ?? "—"}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
