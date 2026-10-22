"use client";

import Link from "next/link";
import { useState } from "react";
import { useAuthSession } from "@/components/AuthSessionContext";
import {
  ApiError,
  triggerDailyMaintenanceRefresh,
  triggerRefreshProfile,
  useOperatorStatus,
} from "@/hooks/useCuse4Api";
import { useOperatorTokenAvailable } from "@/hooks/useOperatorTokenAvailable";
import { runServeRefreshAndRevalidate } from "@/lib/cuse4Refresh";
import { accountTypeFromSession, describeUiError, type UiAccountType } from "@/lib/uiErrors";

function parseError(error: unknown): {
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
      actionMethod: detail?.action?.method,
      actionEndpoint,
      refreshProfile,
    };
  }
  return {};
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
  title,
  surface = "data",
  error,
  operation = "read",
  accountType,
  accountName,
  onRetry,
}: {
  title?: string;
  surface?: string;
  error: unknown;
  operation?: "read" | "write" | "recalculate";
  accountType?: UiAccountType | null;
  accountName?: string | null;
  onRetry?: () => void;
}) {
  const [refreshState, setRefreshState] = useState<"idle" | "running" | "done" | "failed">("idle");
  const parsed = parseError(error);
  const { session, context } = useAuthSession();
  const isAdmin = Boolean(context?.is_admin || session?.isAdmin);
  const description = describeUiError(error, {
    surface,
    operation,
    accountType: accountType ?? accountTypeFromSession(session, context),
    accountName,
    authProvider: session?.authProvider,
    isAdmin,
  });
  const operatorTokenAvailable = useOperatorTokenAvailable();
  const canUseOperatorActions = isAdmin && operatorTokenAvailable;
  const { data: operator } = useOperatorStatus(canUseOperatorActions);
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
      <h3>{title || description.title}</h3>
      <div className="detail-history-empty" role="alert">
        {description.message}
        {description.diagnostic ? (
          <details className="ui-error-diagnostic">
            <summary>Admin details</summary>
            <code>{description.diagnostic}</code>
          </details>
        ) : null}
      </div>
      <div className="ui-error-actions">
        {description.action === "sign_in" ? (
          <Link href="/login" className="btn-action">Return to login</Link>
        ) : description.action === "retry" ? (
          <button className="btn-action" type="button" onClick={onRetry ?? (() => window.location.reload())}>
            Try again
          </button>
        ) : null}
      </div>
      {canUseOperatorActions && parsed.actionEndpoint && parsed.actionMethod === "POST" && (
        <div style={{ marginTop: 10 }}>
          <button
            className="btn-action"
            onClick={handleRefresh}
            disabled={refreshState === "running"}
          >
            {refreshState === "running"
              ? "Starting refresh…"
              : refreshProfileLabel(parsed.refreshProfile, onlyServeRefreshAllowed)}
          </button>
          {refreshState === "done" && (
            <div style={{ marginTop: 8, color: "var(--text-secondary)", fontSize: 12 }}>
              {parsed.refreshProfile === "serve-refresh" || (!parsed.refreshProfile && onlyServeRefreshAllowed)
                ? "Refresh completed."
                : "Refresh started. Reload in a few seconds."}
            </div>
          )}
          {refreshState === "failed" && (
            <div style={{ marginTop: 8, color: "var(--negative)", fontSize: 12 }}>
              The maintenance refresh did not start. Review Admin details or Operator status before retrying.
            </div>
          )}
        </div>
      )}
    </div>
  );
}
