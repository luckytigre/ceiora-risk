"use client";

import { mutate } from "swr";
import { ApiError, apiFetch, apiPath } from "@/lib/api";
import type { OperatorStatusData, RefreshStatusData, RefreshStatusState } from "@/lib/types";

const REFRESH_POLL_INTERVAL_MS = 1500;
const REFRESH_TIMEOUT_MS = 5 * 60 * 1000;
const MAX_REFRESH_ATTEMPTS = 3;

export interface ServeRefreshCompletion {
  refresh: RefreshStatusState;
  holdingsSyncVerified: boolean;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export function isTerminalRefreshStatus(refresh: RefreshStatusState | null | undefined): boolean {
  const status = String(refresh?.status || "").trim().toLowerCase();
  return status === "ok" || status === "failed" || status === "unknown";
}

export function refreshSucceeded(refresh: RefreshStatusState | null | undefined): boolean {
  return String(refresh?.status || "").trim().toLowerCase() === "ok";
}

export function refreshFailureMessage(refresh: RefreshStatusState | null | undefined): string {
  const errorMessage = typeof refresh?.error?.message === "string" ? refresh.error.message.trim() : "";
  if (errorMessage) return errorMessage;
  const resultMessage =
    refresh?.result && typeof refresh.result === "object" && typeof refresh.result.message === "string"
      ? refresh.result.message.trim()
      : "";
  if (resultMessage) return resultMessage;
  const status = String(refresh?.status || "").trim().toLowerCase();
  return status === "unknown" ? "Refresh status became unknown." : "Refresh did not complete successfully.";
}

function extractRefreshState(detail: unknown): RefreshStatusState | null {
  if (!detail || typeof detail !== "object") return null;
  const refresh = (detail as { refresh?: unknown }).refresh;
  if (!refresh || typeof refresh !== "object") return null;
  return refresh as RefreshStatusState;
}

function numericField(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function holdingsSyncStillPending(operatorStatus: OperatorStatusData | null | undefined): boolean {
  return Boolean(operatorStatus?.holdings_sync?.pending);
}

function canVerifyHoldingsSync(operatorStatus: OperatorStatusData | null | undefined): boolean {
  return Boolean(operatorStatus && operatorStatus.holdings_sync && typeof operatorStatus.holdings_sync === "object");
}

function runningRefreshCoversLatestDirtyRevision(operatorStatus: OperatorStatusData | null | undefined): boolean {
  const holdingsSync = operatorStatus?.holdings_sync;
  if (!holdingsSync || typeof holdingsSync !== "object") return false;
  const dirtyRevision = numericField(holdingsSync.dirty_revision);
  if (dirtyRevision === null) return false;
  if (dirtyRevision <= 0) return true;
  const startedDirtyRevision = numericField(holdingsSync.last_refresh_started_dirty_revision);
  return startedDirtyRevision !== null && startedDirtyRevision >= dirtyRevision;
}

async function loadOperatorStatus(): Promise<OperatorStatusData | null> {
  try {
    return await apiFetch<OperatorStatusData>(apiPath.operatorStatus());
  } catch {
    return null;
  }
}

async function requestServeRefreshAttempt(): Promise<{
  jobId: string | null;
  waitForCurrentRunThenRetry: boolean;
}> {
  let runningRefresh: RefreshStatusState | null = null;

  try {
    const response = await apiFetch<{
      status: string;
      message?: string;
      refresh?: RefreshStatusState;
    }>(apiPath.refreshProfile("serve-refresh"), { method: "POST" });
    return {
      jobId: response.refresh?.job_id ?? null,
      waitForCurrentRunThenRetry: false,
    };
  } catch (error) {
    if (!(error instanceof ApiError)) {
      throw error;
    }
    runningRefresh = extractRefreshState(error.detail);
    if (String(runningRefresh?.status || "").trim().toLowerCase() !== "running") {
      throw error;
    }
  }

  const operatorStatus = await loadOperatorStatus();
  const safeToAttach = runningRefreshCoversLatestDirtyRevision(operatorStatus);
  return {
    jobId: runningRefresh?.job_id ?? operatorStatus?.refresh?.job_id ?? null,
    waitForCurrentRunThenRetry: !safeToAttach,
  };
}

export async function revalidateServedAnalyticsViews(): Promise<void> {
  await Promise.all([
    mutate(apiPath.refreshStatus()),
    mutate(apiPath.operatorStatus()),
    mutate(apiPath.portfolio()),
    mutate(apiPath.risk()),
    mutate(apiPath.exposures("raw")),
    mutate(apiPath.exposures("sensitivity")),
    mutate(apiPath.exposures("risk_contribution")),
  ]);
}

export async function waitForRefreshTerminalState({
  jobId,
  timeoutMs = REFRESH_TIMEOUT_MS,
  pollIntervalMs = REFRESH_POLL_INTERVAL_MS,
}: {
  jobId?: string | null;
  timeoutMs?: number;
  pollIntervalMs?: number;
} = {}): Promise<RefreshStatusState> {
  const trackedJobId = String(jobId || "").trim();
  const deadline = Date.now() + timeoutMs;

  while (true) {
    const payload = await apiFetch<RefreshStatusData>(apiPath.refreshStatus());
    const refresh = payload.refresh;
    const currentJobId = String(refresh?.job_id || "").trim();

    if (!trackedJobId || !currentJobId || currentJobId === trackedJobId) {
      if (isTerminalRefreshStatus(refresh)) {
        return refresh;
      }
    }

    if (Date.now() >= deadline) {
      throw new Error("Timed out waiting for serve-refresh to finish.");
    }
    await sleep(pollIntervalMs);
  }
}

export async function runServeRefreshAndRevalidate({
  timeoutMs = REFRESH_TIMEOUT_MS,
}: {
  timeoutMs?: number;
} = {}): Promise<ServeRefreshCompletion> {
  try {
    let terminalRefresh: RefreshStatusState | null = null;

    for (let attempt = 1; attempt <= MAX_REFRESH_ATTEMPTS; attempt += 1) {
      const refreshRequest = await requestServeRefreshAttempt();
      terminalRefresh = await waitForRefreshTerminalState({
        jobId: refreshRequest.jobId,
        timeoutMs,
      });
      const operatorStatus = await loadOperatorStatus();
      const verifiedHoldingsSync = canVerifyHoldingsSync(operatorStatus);
      const pending = holdingsSyncStillPending(operatorStatus);

      if (refreshRequest.waitForCurrentRunThenRetry) {
        if (verifiedHoldingsSync && !pending) {
          return { refresh: terminalRefresh, holdingsSyncVerified: true };
        }
        if (attempt === MAX_REFRESH_ATTEMPTS) {
          throw new Error(
            verifiedHoldingsSync
              ? "RECALC finished, but newer holdings edits are still pending. Run RECALC again."
              : "RECALC finished, but holdings sync status could not be verified. Check Operator status and rerun RECALC if edits remain pending.",
          );
        }
        continue;
      }

      if (!refreshSucceeded(terminalRefresh)) {
        return { refresh: terminalRefresh, holdingsSyncVerified: verifiedHoldingsSync };
      }
      if (!verifiedHoldingsSync) {
        return { refresh: terminalRefresh, holdingsSyncVerified: false };
      }
      if (!pending) {
        return { refresh: terminalRefresh, holdingsSyncVerified: true };
      }
      if (attempt === MAX_REFRESH_ATTEMPTS) {
        throw new Error("RECALC finished, but newer holdings edits are still pending. Run RECALC again.");
      }
    }

    if (terminalRefresh) {
      return { refresh: terminalRefresh, holdingsSyncVerified: false };
    }
    throw new Error("RECALC did not start.");
  } finally {
    await revalidateServedAnalyticsViews();
  }
}
