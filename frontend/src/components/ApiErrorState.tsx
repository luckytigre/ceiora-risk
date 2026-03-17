"use client";

import { useState } from "react";
import {
  ApiError,
  triggerDailyMaintenanceRefresh,
  triggerRefreshProfile,
  useOperatorStatus,
} from "@/hooks/useApi";
import { runServeRefreshAndRevalidate } from "@/lib/refresh";

function parseError(error: unknown): {
  message: string;
  actionMethod?: string;
  actionEndpoint?: string;
  refreshProfile?: string;
} {
  if (error instanceof ApiError) {
    const detail = error.detail as
      | {
          message?: string;
          action?: { method?: string; endpoint?: string };
        }
      | null
      | undefined;
    const actionEndpoint = detail?.action?.endpoint;
    let refreshProfile: string | undefined;
    if (actionEndpoint) {
      try {
        const url = new URL(actionEndpoint, "http://localhost");
        const profile = String(url.searchParams.get("profile") || "").trim();
        if (profile) refreshProfile = profile;
      } catch {
        // noop
      }
    }
    return {
      message: detail?.message || error.message || "Request failed.",
      actionMethod: detail?.action?.method,
      actionEndpoint,
      refreshProfile,
    };
  }
  if (error instanceof Error) {
    return { message: error.message };
  }
  return { message: "Unknown error while loading API data." };
}

function refreshProfileLabel(profile: string | undefined, onlyServeRefreshAllowed: boolean): string {
  if (!profile) return onlyServeRefreshAllowed ? "Run serve-refresh" : "Run source sync + core if due";
  if (profile === "serve-refresh") return "Run serve-refresh";
  if (profile === "source-daily-plus-core-if-due") return "Run source sync + core if due";
  if (profile === "source-daily") return "Run source sync";
  if (profile === "core-weekly") return "Run weekly core rebuild";
  if (profile === "cold-core") return "Run cold-core rebuild";
  return `Run ${profile}`;
}

export default function ApiErrorState({
  title = "Data Not Ready",
  error,
}: {
  title?: string;
  error: unknown;
}) {
  const [refreshState, setRefreshState] = useState<"idle" | "running" | "done" | "failed">("idle");
  const parsed = parseError(error);
  const { data: operator } = useOperatorStatus();
  const allowedProfiles = new Set(operator?.runtime?.allowed_profiles ?? []);
  const onlyServeRefreshAllowed = allowedProfiles.size > 0 && allowedProfiles.size === 1 && allowedProfiles.has("serve-refresh");

  async function handleRefresh() {
    setRefreshState("running");
    try {
      if (parsed.refreshProfile === "serve-refresh" || (!parsed.refreshProfile && onlyServeRefreshAllowed)) {
        await runServeRefreshAndRevalidate();
      } else if (parsed.refreshProfile) {
        await triggerRefreshProfile(parsed.refreshProfile);
      } else {
        await triggerDailyMaintenanceRefresh();
      }
      setRefreshState("done");
    } catch {
      setRefreshState("failed");
    }
  }

  return (
    <div className="chart-card">
      <h3>{title}</h3>
      <div className="detail-history-empty">{parsed.message}</div>
      {parsed.actionEndpoint && parsed.actionMethod === "POST" && (
        <div style={{ marginTop: 10 }}>
          <button
            className="btn btn-secondary"
            onClick={handleRefresh}
            disabled={refreshState === "running"}
          >
            {refreshState === "running"
              ? "Starting refresh..."
              : refreshProfileLabel(parsed.refreshProfile, onlyServeRefreshAllowed)}
          </button>
          {refreshState === "done" && (
            <div style={{ marginTop: 8, color: "rgba(169,182,210,0.8)", fontSize: 12 }}>
              {parsed.refreshProfile === "serve-refresh" || (!parsed.refreshProfile && onlyServeRefreshAllowed)
                ? "Refresh completed."
                : "Refresh started. Reload in a few seconds."}
            </div>
          )}
          {refreshState === "failed" && (
            <div style={{ marginTop: 8, color: "rgba(204,53,88,0.9)", fontSize: 12 }}>
              Could not start refresh from this page.
            </div>
          )}
        </div>
      )}
    </div>
  );
}
