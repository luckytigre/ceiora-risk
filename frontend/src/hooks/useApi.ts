"use client";

import useSWR from "swr";
import { ApiError, apiFetch, apiPath, type RefreshMode } from "@/lib/api";
import type {
  PortfolioData,
  ExposuresData,
  RiskData,
  FactorHistoryData,
  UniverseTickerData,
  UniverseTickerHistoryData,
  UniverseSearchData,
  UniverseFactorsData,
  HealthDiagnosticsData,
  DataDiagnosticsData,
} from "@/lib/types";

export { ApiError };

const SWR_OPTS = {
  revalidateOnFocus: true,
  shouldRetryOnError: true,
  errorRetryCount: 3,
  errorRetryInterval: 5000,
  refreshInterval: 15000,
};

export function usePortfolio() {
  return useSWR<PortfolioData>(apiPath.portfolio(), apiFetch, SWR_OPTS);
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

export function useHealthDiagnostics() {
  return useSWR<HealthDiagnosticsData>(apiPath.healthDiagnostics(), apiFetch, SWR_OPTS);
}

export function useDataDiagnostics() {
  return useSWR<DataDiagnosticsData>(apiPath.dataDiagnostics(), apiFetch, SWR_OPTS);
}

export async function triggerRefresh(mode: RefreshMode = "full"): Promise<{ status: string }> {
  return apiFetch<{ status: string }>(apiPath.refresh(mode), { method: "POST" });
}
