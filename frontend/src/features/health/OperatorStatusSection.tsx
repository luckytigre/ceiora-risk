"use client";

import type { OperatorStatusData } from "@/lib/types";
import { fmtTs, laneSummary, operatorTone } from "./utils";

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
      <h3 style={{ marginBottom: 8 }}>Operator Status</h3>
      <div className="health-meta-row">
        <span>{data ? `Updated ${fmtTs(data.generated_at)}` : isLoading ? "Loading operator state..." : "Operator state unavailable"}</span>
        <span>
          Core due: {data ? (data.core_due.due ? `Yes (${data.core_due.reason})` : `No (${data.core_due.reason})`) : "—"}
        </span>
      </div>
      {data?.runtime?.dashboard_truth_plain_english && (
        <div className="detail-history-empty" style={{ marginTop: 10 }}>
          {data.runtime.dashboard_truth_plain_english}
        </div>
      )}
      {data?.runtime?.diagnostics_scope_plain_english && (
        <div className="detail-history-empty" style={{ marginTop: 10 }}>
          {data.runtime.diagnostics_scope_plain_english}
        </div>
      )}
      {Boolean(error) && !data && (
        <div className="detail-history-empty" style={{ marginTop: 10 }}>
          Operator status endpoint is unavailable.
        </div>
      )}
      <div className="health-kpi-strip" style={{ marginTop: 12, marginBottom: 12, flexWrap: "wrap", gap: 10 }}>
        {(data?.lanes ?? []).map((lane) => (
          <div
            key={lane.profile}
            className="health-kpi"
            style={{
              minWidth: 210,
              flex: "1 1 210px",
              border: `1px solid ${
                operatorTone(lane.latest_run.status) === "success"
                  ? "rgba(107, 207, 154, 0.22)"
                  : operatorTone(lane.latest_run.status) === "warning"
                    ? "rgba(224, 190, 92, 0.22)"
                    : "rgba(224, 87, 127, 0.24)"
              }`,
            }}
          >
            <div className="health-kpi-label">{lane.label}</div>
            <div className="health-kpi-value" style={{ textTransform: "uppercase", fontSize: 18 }}>
              {lane.latest_run.status}
            </div>
            <div className="health-kpi-subrow">{laneSummary(lane)}</div>
            <div className="health-kpi-subrow">Stages: {lane.default_stages.join(" -> ") || "—"}</div>
          </div>
        ))}
      </div>
      <div className="health-grid-2-half">
        <div className="chart-card" style={{ margin: 0 }}>
          <h4 style={{ marginBottom: 8 }}>Source Recency</h4>
          <div className="health-kpi-subrow"><strong>Prices:</strong> {sourceDates.prices_asof ?? "—"}</div>
          <div className="health-kpi-subrow"><strong>Fundamentals:</strong> {sourceDates.fundamentals_asof ?? "—"}</div>
          <div className="health-kpi-subrow"><strong>Classification:</strong> {sourceDates.classification_asof ?? "—"}</div>
          <div className="health-kpi-subrow"><strong>Cross Section:</strong> {sourceDates.exposures_asof ?? "—"}</div>
          <div className="health-kpi-subrow"><strong>Factor Returns:</strong> {data?.risk_engine?.factor_returns_latest_date ?? "—"}</div>
        </div>
        <div className="chart-card" style={{ margin: 0 }}>
          <h4 style={{ marginBottom: 8 }}>Runtime Health</h4>
          <div className="health-kpi-subrow"><strong>Refresh:</strong> {data?.refresh?.status ?? "—"}</div>
          <div className="health-kpi-subrow"><strong>Holdings dirty:</strong> {holdingsSync?.pending ? `Yes (${holdingsSync.pending_count || 0})` : "No"}</div>
          <div className="health-kpi-subrow"><strong>Neon mirror:</strong> {neon?.mirror_status ?? neon?.status ?? "—"}</div>
          <div className="health-kpi-subrow"><strong>Neon parity:</strong> {neon?.parity_status ?? "—"}</div>
          <div className="health-kpi-subrow"><strong>Snapshot:</strong> {data?.active_snapshot?.snapshot_id ?? "—"}</div>
          <div className="health-kpi-subrow"><strong>Risk Engine:</strong> {data?.risk_engine?.method_version ?? "—"}</div>
          <div className="health-kpi-subrow"><strong>Parity Artifact:</strong> {data?.latest_parity_artifact ?? "—"}</div>
        </div>
      </div>
    </div>
  );
}
