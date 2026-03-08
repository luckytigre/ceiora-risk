const BASE = "";
const REQUEST_TIMEOUT_MS = 30000;

export type RefreshMode = "full" | "light" | "cold";

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

export const apiPath = {
  portfolio: () => "/api/portfolio",
  holdingsModes: () => "/api/holdings/modes",
  holdingsAccounts: () => "/api/holdings/accounts",
  holdingsPositions: (accountId?: string | null) =>
    accountId && accountId.trim().length > 0
      ? `/api/holdings/positions?account_id=${encodeURIComponent(accountId.trim())}`
      : "/api/holdings/positions",
  holdingsImport: () => "/api/holdings/import",
  holdingsPosition: () => "/api/holdings/position",
  holdingsPositionRemove: () => "/api/holdings/position/remove",
  exposures: (mode: string) => `/api/exposures?mode=${encodeURIComponent(mode)}`,
  exposureHistory: (factor: string, years: number) =>
    `/api/exposures/history?factor=${encodeURIComponent(factor)}&years=${years}`,
  risk: () => "/api/risk",
  universeTicker: (ticker: string) => `/api/universe/ticker/${encodeURIComponent(ticker)}`,
  universeTickerHistory: (ticker: string, years: number) =>
    `/api/universe/ticker/${encodeURIComponent(ticker)}/history?years=${years}`,
  universeSearch: (query: string, limit: number) =>
    `/api/universe/search?q=${encodeURIComponent(query)}&limit=${limit}`,
  universeFactors: () => "/api/universe/factors",
  healthDiagnostics: () => "/api/health/diagnostics",
  dataDiagnostics: (opts?: { includeExactRowCounts?: boolean; includeExpensiveChecks?: boolean }) => {
    const params = new URLSearchParams();
    if (opts?.includeExactRowCounts) params.set("include_exact_row_counts", "true");
    if (opts?.includeExpensiveChecks) params.set("include_expensive_checks", "true");
    const qs = params.toString();
    return qs ? `/api/data/diagnostics?${qs}` : "/api/data/diagnostics";
  },
  operatorStatus: () => "/api/operator/status",
  refresh: (mode: RefreshMode) => `/api/refresh?mode=${mode}`,
  refreshProfile: (profile: string) => `/api/refresh?profile=${encodeURIComponent(profile)}`,
  refreshStatus: () => "/api/refresh/status",
};

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

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  const url = `${BASE}${path}`;
  try {
    const res = await fetch(url, { ...init, signal: controller.signal });
    if (!res.ok) {
      const detail = await parseErrorDetail(res);
      throw new ApiError(res.status, url, detail);
    }
    return res.json();
  } finally {
    clearTimeout(timer);
  }
}
