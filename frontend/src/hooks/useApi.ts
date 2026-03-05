"use client";

import useSWR from "swr";
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

const REQUEST_TIMEOUT_MS = 30000;

export class ApiError extends Error {
  status: number;
  url: string;
  detail: unknown;

  constructor(status: number, url: string, detail: unknown) {
    const message =
      typeof detail === "string"
        ? detail
        : (detail as { message?: string } | null)?.message || `Request failed (${status}) for ${url}`;
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.url = url;
    this.detail = detail;
  }
}

async function parseErrorDetail(res: Response): Promise<unknown> {
  const text = await res.text();
  if (!text) return null;
  try {
    const payload = JSON.parse(text) as { detail?: unknown };
    return payload?.detail ?? payload;
  } catch {
    return text;
  }
}

const fetcher = async (url: string) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) {
      const detail = await parseErrorDetail(res);
      throw new ApiError(res.status, url, detail);
    }
    return res.json();
  } finally {
    clearTimeout(timer);
  }
};

const SWR_OPTS = {
  revalidateOnFocus: true,
  shouldRetryOnError: true,
  errorRetryCount: 3,
  errorRetryInterval: 5000,
  refreshInterval: 15000,
};

export function usePortfolio() {
  return useSWR<PortfolioData>("/api/portfolio", fetcher, SWR_OPTS);
}

export function useExposures(mode: string) {
  return useSWR<ExposuresData>(`/api/exposures?mode=${mode}`, fetcher, SWR_OPTS);
}

export function useFactorHistory(factor: string | null, years = 5) {
  const key = factor
    ? `/api/exposures/history?factor=${encodeURIComponent(factor)}&years=${years}`
    : null;
  return useSWR<FactorHistoryData>(key, fetcher, SWR_OPTS);
}

export function useRisk() {
  return useSWR<RiskData>("/api/risk", fetcher, SWR_OPTS);
}

export function useUniverseTicker(ticker: string | null) {
  const clean = ticker?.trim().toUpperCase() || null;
  const key = clean ? `/api/universe/ticker/${encodeURIComponent(clean)}` : null;
  return useSWR<UniverseTickerData>(key, fetcher, SWR_OPTS);
}

export function useUniverseTickerHistory(ticker: string | null, years = 5) {
  const clean = ticker?.trim().toUpperCase() || null;
  const key = clean
    ? `/api/universe/ticker/${encodeURIComponent(clean)}/history?years=${years}`
    : null;
  return useSWR<UniverseTickerHistoryData>(key, fetcher, SWR_OPTS);
}

export function useUniverseSearch(query: string, limit = 8) {
  const q = query.trim();
  const key = q.length > 0
    ? `/api/universe/search?q=${encodeURIComponent(q)}&limit=${limit}`
    : null;
  return useSWR<UniverseSearchData>(key, fetcher, SWR_OPTS);
}

export function useUniverseFactors() {
  return useSWR<UniverseFactorsData>("/api/universe/factors", fetcher, SWR_OPTS);
}

export function useHealthDiagnostics() {
  return useSWR<HealthDiagnosticsData>("/api/health/diagnostics", fetcher, SWR_OPTS);
}

export function useDataDiagnostics() {
  return useSWR<DataDiagnosticsData>("/api/data/diagnostics", fetcher, SWR_OPTS);
}

export async function triggerRefresh(mode: "full" | "light" | "cold" = "full"): Promise<{ status: string }> {
  const res = await fetch(`/api/refresh?mode=${mode}`, { method: "POST" });
  if (!res.ok) {
    const detail = await parseErrorDetail(res);
    throw new ApiError(res.status, `/api/refresh?mode=${mode}`, detail);
  }
  return res.json();
}
