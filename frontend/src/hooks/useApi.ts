"use client";

import useSWR from "swr";
import { ApiError, apiFetch, apiPath, type RefreshMode } from "@/lib/api";
import type {
  PortfolioData,
  HoldingsModeData,
  HoldingsAccountsData,
  HoldingsPositionsData,
  HoldingsImportMode,
  HoldingsImportResponse,
  HoldingsPositionEditResponse,
  ExposuresData,
  RiskData,
  FactorHistoryData,
  UniverseTickerData,
  UniverseTickerHistoryData,
  UniverseSearchData,
  UniverseFactorsData,
  HealthDiagnosticsData,
  DataDiagnosticsData,
  OperatorStatusData,
} from "@/lib/types";

export { ApiError };

const SWR_OPTS = {
  revalidateOnFocus: false,
  revalidateOnReconnect: false,
  shouldRetryOnError: false,
  errorRetryCount: 0,
  refreshInterval: 0,
};

const HEAVY_DIAGNOSTICS_OPTS = {
  ...SWR_OPTS,
  // Diagnostics payloads can be expensive to build on large local SQLite files.
  // Keep these on-demand/focus refreshes only instead of interval polling.
  refreshInterval: 0,
};

export function usePortfolio() {
  return useSWR<PortfolioData>(apiPath.portfolio(), apiFetch, SWR_OPTS);
}

export function useHoldingsModes() {
  return useSWR<HoldingsModeData>(apiPath.holdingsModes(), apiFetch, {
    ...SWR_OPTS,
    refreshInterval: 0,
  });
}

export function useHoldingsAccounts() {
  return useSWR<HoldingsAccountsData>(apiPath.holdingsAccounts(), apiFetch, SWR_OPTS);
}

export function useHoldingsPositions(accountId?: string | null) {
  const key = apiPath.holdingsPositions(accountId);
  return useSWR<HoldingsPositionsData>(key, apiFetch, SWR_OPTS);
}

export function useExposures(mode: string) {
  return useSWR<ExposuresData>(apiPath.exposures(mode), apiFetch, SWR_OPTS);
}

export function useFactorHistory(factor: string | null, years = 5) {
  const key = factor ? apiPath.exposureHistory(factor, years) : null;
  return useSWR<FactorHistoryData>(key, apiFetch, SWR_OPTS);
}

export function useRisk() {
  return useSWR<RiskData>(apiPath.risk(), apiFetch, SWR_OPTS);
}

export function useUniverseTicker(ticker: string | null) {
  const clean = ticker?.trim().toUpperCase() || null;
  const key = clean ? apiPath.universeTicker(clean) : null;
  return useSWR<UniverseTickerData>(key, apiFetch, SWR_OPTS);
}

export function useUniverseTickerHistory(ticker: string | null, years = 5) {
  const clean = ticker?.trim().toUpperCase() || null;
  const key = clean ? apiPath.universeTickerHistory(clean, years) : null;
  return useSWR<UniverseTickerHistoryData>(key, apiFetch, SWR_OPTS);
}

export function useUniverseSearch(query: string, limit = 8) {
  const q = query.trim();
  const key = q.length > 0 ? apiPath.universeSearch(q, limit) : null;
  return useSWR<UniverseSearchData>(key, apiFetch, SWR_OPTS);
}

export function useUniverseFactors() {
  return useSWR<UniverseFactorsData>(apiPath.universeFactors(), apiFetch, SWR_OPTS);
}

export function useHealthDiagnostics(enabled = true) {
  return useSWR<HealthDiagnosticsData>(enabled ? apiPath.healthDiagnostics() : null, apiFetch, HEAVY_DIAGNOSTICS_OPTS);
}

export function useDataDiagnostics(opts?: { includeExactRowCounts?: boolean; includeExpensiveChecks?: boolean }) {
  return useSWR<DataDiagnosticsData>(apiPath.dataDiagnostics(opts), apiFetch, HEAVY_DIAGNOSTICS_OPTS);
}

export function useOperatorStatus() {
  return useSWR<OperatorStatusData>(apiPath.operatorStatus(), apiFetch, {
    ...SWR_OPTS,
    refreshInterval: 0,
  });
}

export async function triggerRefresh(mode: RefreshMode = "full"): Promise<{ status: string }> {
  return apiFetch<{ status: string }>(apiPath.refresh(mode), { method: "POST" });
}

export async function triggerRefreshProfile(profile: string): Promise<{ status: string }> {
  return apiFetch<{ status: string }>(apiPath.refreshProfile(profile), { method: "POST" });
}

export async function triggerServeRefresh(): Promise<{ status: string }> {
  return triggerRefreshProfile("serve-refresh");
}

export async function triggerDailyMaintenanceRefresh(): Promise<{ status: string }> {
  return triggerRefreshProfile("source-daily-plus-core-if-due");
}

export async function triggerHoldingsImport(payload: {
  account_id: string;
  mode: HoldingsImportMode;
  rows: Array<{
    account_id?: string;
    ric?: string;
    ticker?: string;
    quantity: number;
    source?: string;
  }>;
  filename?: string;
  requested_by?: string;
  notes?: string;
  default_source?: string;
  dry_run?: boolean;
  trigger_refresh?: boolean;
}): Promise<HoldingsImportResponse> {
  return apiFetch<HoldingsImportResponse>(apiPath.holdingsImport(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function upsertHoldingPosition(payload: {
  account_id: string;
  quantity: number;
  ric?: string;
  ticker?: string;
  source?: string;
  requested_by?: string;
  notes?: string;
  dry_run?: boolean;
  trigger_refresh?: boolean;
}): Promise<HoldingsPositionEditResponse> {
  return apiFetch<HoldingsPositionEditResponse>(apiPath.holdingsPosition(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function removeHoldingPosition(payload: {
  account_id: string;
  ric?: string;
  ticker?: string;
  requested_by?: string;
  notes?: string;
  dry_run?: boolean;
  trigger_refresh?: boolean;
}): Promise<HoldingsPositionEditResponse> {
  return apiFetch<HoldingsPositionEditResponse>(apiPath.holdingsPositionRemove(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}
