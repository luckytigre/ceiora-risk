"use client";

import { useState } from "react";
import {
  ApiError,
  triggerDailyMaintenanceRefresh,
  triggerRefreshProfile,
  triggerServeRefresh,
  useOperatorStatus,
} from "@/hooks/useApi";

function parseError(error: unknown): {
  message: string;
  actionMethod?: string;
  actionEndpoint?: string;
  refreshMode?: "full" | "light" | "cold";
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
    let refreshMode: "full" | "light" | "cold" | undefined;
    let refreshProfile: string | undefined;
    if (actionEndpoint) {
      try {
        const url = new URL(actionEndpoint, "http://localhost");
        const mode = String(url.searchParams.get("mode") || "").toLowerCase();
        const profile = String(url.searchParams.get("profile") || "").trim();
        if (mode === "full" || mode === "light" || mode === "cold") refreshMode = mode;
        if (profile) refreshProfile = profile;
      } catch {
        // noop
      }
    }
    return {
      message: detail?.message || error.message || "Request failed.",
      actionMethod: detail?.action?.method,
      actionEndpoint,
      refreshMode,
      refreshProfile,
    };
  }
  if (error instanceof Error) {
    return { message: error.message };
  }
  return { message: "Unknown error while loading API data." };
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
      if (parsed.refreshProfile) {
        await triggerRefreshProfile(parsed.refreshProfile);
      } else if (onlyServeRefreshAllowed) {
        await triggerServeRefresh();
      } else {
        if (parsed.refreshMode === "cold") {
          await triggerRefreshProfile("cold-core");
        } else if (parsed.refreshMode === "full") {
          await triggerDailyMaintenanceRefresh();
        } else {
          await triggerServeRefresh();
        }
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
              : parsed.refreshProfile
                ? `Run ${parsed.refreshProfile}`
                : onlyServeRefreshAllowed
                  ? "Run serve-refresh"
                : parsed.refreshMode === "cold"
                  ? "Run cold-core"
                  : parsed.refreshMode === "full"
                    ? "Run source-daily-plus-core-if-due"
                    : "Run serve-refresh"}
          </button>
          {refreshState === "done" && (
            <div style={{ marginTop: 8, color: "rgba(169,182,210,0.8)", fontSize: 12 }}>
              Refresh started. Reload in a few seconds.
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
