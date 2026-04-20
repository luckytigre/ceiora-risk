import { readStoredAuthTokens } from "@/lib/authTokens";

const BASE = "";
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

function authHeadersForPath(path: string): Headers | null {
  if (typeof window === "undefined" || !path.startsWith("/api/")) return null;
  const { operatorToken, editorToken } = readStoredAuthTokens(window.localStorage);
  const headers = new Headers();
  if (operatorToken) {
    headers.set("X-Operator-Token", operatorToken);
  }
  if (editorToken) {
    headers.set("X-Editor-Token", editorToken);
  }
  return operatorToken || editorToken ? headers : null;
}

type ApiFetchOptions = RequestInit & {
  privileged?: boolean;
};

async function doApiFetch<T>(path: string, init?: ApiFetchOptions): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  const url = `${BASE}${path}`;
  const headers = new Headers(init?.headers);
  if (init?.privileged) {
    const authHeaders = authHeadersForPath(path);
    authHeaders?.forEach((value, key) => {
      headers.set(key, value);
    });
  }
  try {
    const res = await fetch(url, { ...init, headers, signal: controller.signal });
    if (!res.ok) {
      const detail = await parseErrorDetail(res);
      throw new ApiError(res.status, url, detail);
    }
    return res.json();
  } finally {
    clearTimeout(timer);
  }
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  return doApiFetch<T>(path, init);
}

export async function apiPrivilegedFetch<T>(path: string, init?: RequestInit): Promise<T> {
  return doApiFetch<T>(path, { ...init, privileged: true });
}
