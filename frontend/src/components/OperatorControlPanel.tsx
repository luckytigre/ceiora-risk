"use client";

import { useMemo, useState } from "react";
import HelpLabel from "@/components/HelpLabel";
import ConfirmActionModal from "@/components/ConfirmActionModal";
import LaneRunHistoryStrip from "@/components/operator/LaneRunHistoryStrip";
import { triggerRefreshProfile, useOperatorStatus } from "@/hooks/useApi";
import type { OperatorLaneStatus } from "@/lib/types";

function fmtTs(v: string | null | undefined): string {
  if (!v) return "—";
  const dt = new Date(v);
  if (Number.isNaN(dt.getTime())) return v;
  return dt.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function tone(status: string | null | undefined): "success" | "warning" | "error" {
  const clean = String(status || "").toLowerCase();
  if (clean === "ok" || clean === "completed") return "success";
  if (clean === "running" || clean === "missing" || clean === "unknown" || clean === "skipped") return "warning";
  return "error";
}

function laneSummary(lane: OperatorLaneStatus): string {
  const run = lane.latest_run;
  if (run.status === "missing") return "No runs yet";
  if (run.status === "running") return `Running since ${fmtTs(run.started_at)}`;
  return `${run.status.toUpperCase()} · ${fmtTs(run.finished_at || run.updated_at)}`;
}

const LANE_HELP: Record<string, { plain: string; math: string }> = {
  "serve-refresh": {
    plain: "Rebuilds the frontend-facing portfolio, risk, exposure, and universe caches without pulling new source data or recomputing the core model.",
    math: "Holdings + current source tables + existing risk engine -> serving caches",
  },
  "source-daily": {
    plain: "Pulls the latest prices, fundamentals, and classification data into the source tables, then refreshes the UI-facing caches. It does not recalculate factor returns or covariance.",
    math: "LSEG ingest + serving refresh, no core recompute",
  },
  "source-daily-plus-core-if-due": {
    plain: "Runs the normal daily maintenance lane. It refreshes source data first and only recalculates the core model if the cadence or method-version rules say it is due.",
    math: "Source daily + conditional core recompute gate",
  },
  "core-weekly": {
    plain: "Recomputes factor returns, covariance, and specific risk from the current retained source history, then republishes serving outputs.",
    math: "Factor returns + covariance + specific risk + serving refresh",
  },
  "cold-core": {
    plain: "Use this after deep history rewrites or model-method changes. It rebuilds the structural history and then rebuilds the core model on top of it.",
    math: "Raw history rebuild + full core rebuild + serving refresh",
  },
  "universe-add": {
    plain: "Finalization lane for newly added names after you explicitly merged them into security_master and backfilled their source tables. For now this workflow remains operator-led with Codex.",
    math: "Targeted universe onboarding -> chosen refresh depth",
  },
};

export default function OperatorControlPanel({ compact = false }: { compact?: boolean }) {
  const { data, error, isLoading, mutate } = useOperatorStatus();
  const [actionState, setActionState] = useState<Record<string, "idle" | "running" | "done" | "failed">>({});
  const [confirmLane, setConfirmLane] = useState<OperatorLaneStatus | null>(null);
  const refreshRunning = String(data?.refresh?.status || "").toLowerCase() === "running";

  const neonHealth = data?.neon_sync_health;
  const holdingsSync = data?.holdings_sync;
  const sourceDates = data?.source_dates ?? {};
  const runtimeWarnings = data?.runtime?.warnings ?? [];
  const orderedLanes = useMemo(() => data?.lanes ?? [], [data?.lanes]);
  const liveStateRows = [
    ["Current refresh", data?.refresh?.status ?? "—"],
    ["Core due", data ? (data.core_due.due ? `Yes (${data.core_due.reason})` : `No (${data.core_due.reason})`) : "—"],
    ["Risk engine", data?.risk_engine?.method_version ?? "—"],
    ["Active snapshot", data?.active_snapshot?.snapshot_id ?? "—"],
    ["Holdings dirty", holdingsSync?.pending ? `Yes (${holdingsSync.pending_count || 0})` : "No"],
    ["Dirty since", fmtTs(holdingsSync?.dirty_since)],
    ["Last holdings change", holdingsSync?.last_mutation_summary ?? "—"],
    ["Neon mirror", neonHealth?.mirror_status ?? neonHealth?.status ?? "—"],
    ["Neon parity", neonHealth?.parity_status ?? "—"],
    ["Parity artifact", data?.latest_parity_artifact ?? "—"],
  ] satisfies Array<[string, string]>;
  const sourceRecencyRows = [
    ["Prices", sourceDates.prices_asof ?? "—"],
    ["Fundamentals", sourceDates.fundamentals_asof ?? "—"],
    ["Classification", sourceDates.classification_asof ?? "—"],
    ["Cross section", sourceDates.exposures_asof ?? "—"],
    ["Factor returns", data?.risk_engine?.factor_returns_latest_date ?? "—"],
    ["Data backend", data?.runtime?.data_backend ?? "—"],
    ["Neon read surfaces", (data?.runtime?.neon_read_surfaces ?? []).join(", ") || "—"],
  ] satisfies Array<[string, string]>;

  async function runLane(profile: string) {
    setActionState((prev) => ({ ...prev, [profile]: "running" }));
    try {
      await triggerRefreshProfile(profile);
      await mutate();
      setActionState((prev) => ({ ...prev, [profile]: "done" }));
    } catch {
      setActionState((prev) => ({ ...prev, [profile]: "failed" }));
    }
  }

  function requestLaneRun(lane: OperatorLaneStatus) {
    if (lane.profile === "cold-core") {
      setConfirmLane(lane);
      return;
    }
    void runLane(lane.profile);
  }

  return (
    <div className="chart-card mb-4">
      <div className="health-meta-row" style={{ marginBottom: 10 }}>
        <h3 style={{ margin: 0 }}>Operator Control Deck</h3>
        <span>
          {isLoading ? "Loading operator state..." : data ? `Updated ${fmtTs(data.generated_at)}` : "Operator state unavailable"}
        </span>
      </div>
      <div className="detail-history-empty" style={{ marginBottom: 14 }}>
        This page is the plain-English control room for your backend. Each lane below is a specific kind of update, with clear scope and current status.
      </div>
      {runtimeWarnings.length > 0 && (
        <div className="detail-history-empty" style={{ marginBottom: 14, color: "rgba(224,190,92,0.92)" }}>
          Runtime warnings: {runtimeWarnings.join(" | ")}
        </div>
      )}
      <div className="health-grid-2-half" style={{ marginBottom: 14 }}>
        <div className="chart-card" style={{ margin: 0 }}>
          <h4 style={{ marginBottom: 8 }}>Live State</h4>
          <div className="operator-kv-grid">
            {liveStateRows.map(([label, value]) => (
              <div className="operator-kv-row" key={label}>
                <strong className="operator-kv-label">{label}</strong>
                <span className="operator-kv-value">{value}</span>
              </div>
            ))}
          </div>
        </div>
        <div className="chart-card" style={{ margin: 0 }}>
          <h4 style={{ marginBottom: 8 }}>Source Recency</h4>
          <div className="operator-kv-grid">
            {sourceRecencyRows.map(([label, value]) => (
              <div className="operator-kv-row" key={label}>
                <strong className="operator-kv-label">{label}</strong>
                <span className="operator-kv-value">{value}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
      {error && !data && (
        <div className="detail-history-empty" style={{ marginBottom: 14 }}>
          Operator status endpoint is unavailable.
        </div>
      )}
      <div className={`dash-table operator-lane-table${compact ? " compact" : ""}`}>
        <table>
          <colgroup>
            <col style={{ width: compact ? "13%" : "14%" }} />
            <col style={{ width: "9%" }} />
            <col style={{ width: compact ? "22%" : "24%" }} />
            <col style={{ width: "12%" }} />
            <col style={{ width: compact ? "20%" : "19%" }} />
            <col style={{ width: compact ? "16%" : "14%" }} />
            <col style={{ width: "8%" }} />
          </colgroup>
          <thead>
            <tr>
              <th>Lane</th>
              <th>Status</th>
              <th>What It Does</th>
              <th>Recent Runs</th>
              <th>Stages</th>
              <th>Last Run</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {orderedLanes.map((lane) => {
              const state = actionState[lane.profile] || "idle";
              const disabled = refreshRunning || state === "running" || lane.profile === "universe-add";
              const help = LANE_HELP[lane.profile] || { plain: lane.description, math: lane.default_stages.join(" -> ") };
              return (
                <tr key={lane.profile}>
                  <td>
                    <HelpLabel
                      label={lane.label}
                      plain={help.plain}
                      math={help.math}
                    />
                    {lane.aliases.length > 0 && (
                      <div style={{ fontSize: 11, color: "rgba(169,182,210,0.6)", marginTop: 4 }}>
                        aliases: {lane.aliases.join(", ")}
                      </div>
                    )}
                  </td>
                  <td>
                    <span className={`status-pill ${tone(lane.latest_run.status)}`}>{lane.latest_run.status}</span>
                  </td>
                  <td className="operator-lane-copy">{help.plain}</td>
                  <td style={{ minWidth: 96 }}>
                    <LaneRunHistoryStrip runs={lane.recent_runs ?? []} />
                  </td>
                  <td className="operator-lane-copy">
                    <div style={{ marginBottom: 6 }}>{lane.default_stages.join(" -> ") || "—"}</div>
                    <details>
                      <summary style={{ cursor: "pointer", color: "rgba(169,182,210,0.82)" }}>
                        Stage detail
                      </summary>
                      <div style={{ marginTop: 8, display: "grid", gap: 6 }}>
                        {(lane.latest_run.stages ?? []).map((stage) => (
                          <div key={`${lane.profile}:${stage.stage_name}`} style={{ fontSize: 12, color: "rgba(232,237,249,0.82)" }}>
                            <strong>{stage.stage_name}</strong>: {stage.status}
                            {stage.error_message ? ` — ${stage.error_message}` : ""}
                          </div>
                        ))}
                        {(lane.latest_run.stages ?? []).length === 0 && (
                          <div style={{ fontSize: 12, color: "rgba(169,182,210,0.68)" }}>No stage records yet.</div>
                        )}
                      </div>
                    </details>
                  </td>
                  <td className="operator-lane-copy">{laneSummary(lane)}</td>
                  <td>
                    {lane.profile === "universe-add" ? (
                      <span style={{ color: "rgba(169,182,210,0.7)", fontSize: 12 }}>Manual with Codex</span>
                    ) : (
                      <button
                        className="btn btn-secondary"
                        onClick={() => requestLaneRun(lane)}
                        disabled={disabled}
                      >
                        {state === "running" ? "Starting..." : "Run"}
                      </button>
                    )}
                    {state === "done" && (
                      <div style={{ marginTop: 6, fontSize: 11, color: "rgba(107,207,154,0.9)" }}>Started</div>
                    )}
                    {state === "failed" && (
                      <div style={{ marginTop: 6, fontSize: 11, color: "rgba(204,53,88,0.9)" }}>Failed</div>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <ConfirmActionModal
        open={!!confirmLane}
        title="Confirm cold-core rebuild"
        body="This is the deepest rebuild lane. Use it after history rewrites or model-method changes. It rebuilds structural history and then rebuilds the core model on top of it."
        confirmLabel="Type to confirm"
        confirmValue="COLD-CORE"
        dangerText="Run cold-core"
        onCancel={() => setConfirmLane(null)}
        onConfirm={async () => {
          const lane = confirmLane;
          setConfirmLane(null);
          if (lane) await runLane(lane.profile);
        }}
      />
    </div>
  );
}
