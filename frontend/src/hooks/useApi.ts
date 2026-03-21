"use client";

// Transitional mixed-family compatibility barrel.
// New cUSE4-owned frontend code should import from `@/hooks/useCuse4Api`.
// New cPAR-owned frontend code should import from `@/hooks/useCparApi`.

import useSWR from "swr";
import { ApiError, apiFetch, apiPath } from "@/lib/api";
import type {
  FactorHistoryData,
  CparHedgeMode,
  CparFactorHistoryData,
  CparMetaData,
  CparRiskData,
  CparTickerDetailData,
  CparTickerHistoryData,
  CparExploreWhatIfData,
  CparPortfolioHedgeData,
  CparPortfolioWhatIfData,
  CparSearchData,
  PortfolioData,
  WhatIfApplyResponse,
  WhatIfPreviewData,
  WhatIfScenarioRow,
  HoldingsModeData,
  HoldingsAccountsData,
  HoldingsPositionsData,
  HoldingsImportMode,
  HoldingsImportResponse,
  HoldingsPositionEditResponse,
  ExposuresData,
  RiskData,
  UniverseTickerData,
  UniverseTickerHistoryData,
  UniverseSearchData,
  UniverseFactorsData,
  HealthDiagnosticsData,
  DataDiagnosticsData,
  OperatorStatusData,
  RefreshStatusData,
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

function refreshStatusRefreshInterval(data?: RefreshStatusData): number {
  return String(data?.refresh?.status || "").toLowerCase() === "running" ? 3000 : 0;
}

function operatorStatusRefreshInterval(data?: OperatorStatusData): number {
  const refreshRunning = String(data?.refresh?.status || "").toLowerCase() === "running";
  const laneRunning = (data?.lanes ?? []).some((lane) => String(lane.latest_run?.status || "").toLowerCase() === "running");
  return refreshRunning || laneRunning ? 3000 : 0;
}

export function usePortfolio() {
  return useSWR<PortfolioData>(apiPath.portfolio(), apiFetch, SWR_OPTS);
}

export function useCparMeta() {
  return useSWR<CparMetaData>(apiPath.cparMeta(), apiFetch, SWR_OPTS);
}

export function useCparSearch(query: string, limit = 10) {
  const q = query.trim();
  const key = q.length > 0 ? apiPath.cparSearch(q, limit) : null;
  return useSWR<CparSearchData>(key, apiFetch, SWR_OPTS);
}

export function useCparTicker(ticker: string | null, ric?: string | null) {
  const cleanTicker = ticker?.trim().toUpperCase() || null;
  const cleanRic = ric?.trim().toUpperCase() || null;
  const key = cleanTicker ? apiPath.cparTicker(cleanTicker, cleanRic) : null;
  return useSWR<CparTickerDetailData>(key, apiFetch, SWR_OPTS);
}

export function useCparTickerHistory(ticker: string | null, years = 5, ric?: string | null) {
  const cleanTicker = ticker?.trim().toUpperCase() || null;
  const cleanRic = ric?.trim().toUpperCase() || null;
  const key = cleanTicker ? apiPath.cparTickerHistory(cleanTicker, years, cleanRic) : null;
  return useSWR<CparTickerHistoryData>(key, apiFetch, SWR_OPTS);
}

export function useCparRisk(enabled = true) {
  return useSWR<CparRiskData>(enabled ? apiPath.cparRisk() : null, apiFetch, SWR_OPTS);
}

export function useCparFactorHistory(factorId: string | null, years = 5, enabled = true) {
  const cleanFactorId = factorId?.trim().toUpperCase() || null;
  const key = enabled && cleanFactorId ? apiPath.cparFactorHistory(cleanFactorId, years) : null;
  return useSWR<CparFactorHistoryData>(key, apiFetch, SWR_OPTS);
}

export function useCparPortfolioHedge(
  accountId: string | null,
  mode: CparHedgeMode,
  enabled = true,
) {
  const cleanAccountId = accountId?.trim() || null;
  const key = enabled && cleanAccountId ? apiPath.cparPortfolioHedge(cleanAccountId, mode) : null;
  return useSWR<CparPortfolioHedgeData>(key, apiFetch, SWR_OPTS);
}

export function useCparPortfolioWhatIf(
  accountId: string | null,
  mode: CparHedgeMode,
  scenarioRows: Array<{ ric: string; ticker?: string | null; quantity_delta: number }>,
  enabled = true,
) {
  const cleanAccountId = accountId?.trim() || null;
  const cleanScenarioRows = scenarioRows.map((row) => ({
    ric: row.ric.trim().toUpperCase(),
    ticker: row.ticker?.trim().toUpperCase() || null,
    quantity_delta: row.quantity_delta,
  }));
  const serializedRows = JSON.stringify(cleanScenarioRows);
  const key = enabled && cleanAccountId && cleanScenarioRows.length > 0
    ? [apiPath.cparPortfolioWhatIf(), cleanAccountId, mode, serializedRows]
    : null;
  return useSWR<CparPortfolioWhatIfData>(
    key,
    ([path, account_id, hedgeMode, rows]) => apiFetch<CparPortfolioWhatIfData>(String(path), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        account_id,
        mode: hedgeMode,
        scenario_rows: JSON.parse(String(rows)),
      }),
    }),
    SWR_OPTS,
  );
}

export async function previewCparExploreWhatIf(payload: {
  scenario_rows: Array<{
    account_id: string;
    ticker?: string | null;
    ric: string;
    quantity: number;
    source?: string | null;
  }>;
}): Promise<CparExploreWhatIfData> {
  return apiFetch<CparExploreWhatIfData>(apiPath.cparExploreWhatIf(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
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

export function useFactorHistory(factorId: string | null, years = 5) {
  const key = factorId ? apiPath.exposureHistory(factorId, years) : null;
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

export function useOperatorStatus(enabled = true) {
  return useSWR<OperatorStatusData>(enabled ? apiPath.operatorStatus() : null, apiFetch, {
    ...SWR_OPTS,
    refreshInterval: operatorStatusRefreshInterval,
  });
}

export function useRefreshStatus(enabled = true) {
  return useSWR<RefreshStatusData>(enabled ? apiPath.refreshStatus() : null, apiFetch, {
    ...SWR_OPTS,
    refreshInterval: refreshStatusRefreshInterval,
  });
}

export async function triggerRefreshProfile(profile: string): Promise<{
  status: string;
  message?: string;
  refresh?: RefreshStatusData["refresh"];
}> {
  return apiFetch(apiPath.refreshProfile(profile), { method: "POST" });
}

export async function triggerServeRefresh(): Promise<{
  status: string;
  message?: string;
  refresh?: RefreshStatusData["refresh"];
}> {
  return triggerRefreshProfile("serve-refresh");
}

export async function previewPortfolioWhatIf(payload: {
  scenario_rows: WhatIfScenarioRow[];
}): Promise<WhatIfPreviewData> {
  return apiFetch<WhatIfPreviewData>(apiPath.portfolioWhatIf(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function applyPortfolioWhatIf(payload: {
  scenario_rows: WhatIfScenarioRow[];
  requested_by?: string;
  default_source?: string;
}): Promise<WhatIfApplyResponse> {
  return apiFetch<WhatIfApplyResponse>(apiPath.portfolioWhatIfApply(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
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
