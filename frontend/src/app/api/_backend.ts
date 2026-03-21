import { NextRequest, NextResponse } from "next/server";

export function backendOrigin(): string {
  return (process.env.BACKEND_API_ORIGIN || "http://127.0.0.1:8000").replace(/\/+$/, "");
}

export function controlBackendOrigin(): string {
  return (process.env.BACKEND_CONTROL_ORIGIN || process.env.BACKEND_API_ORIGIN || "http://127.0.0.1:8000").replace(
    /\/+$/,
    "",
  );
}

export function operatorHeaders(extra: Record<string, string> = {}): HeadersInit {
  const token = (process.env.OPERATOR_API_TOKEN || process.env.REFRESH_API_TOKEN || "").trim();
  if (!token) return extra;
  return { ...extra, "X-Operator-Token": token };
}

export function editorHeaders(extra: Record<string, string> = {}): HeadersInit {
  const token = (process.env.EDITOR_API_TOKEN || process.env.OPERATOR_API_TOKEN || process.env.REFRESH_API_TOKEN || "").trim();
  if (!token) return extra;
  return { ...extra, "X-Editor-Token": token };
}

export async function proxyJson(req: NextRequest, upstream: string, options?: { method?: string; headers?: HeadersInit }) {
  const body = req.method === "GET" || req.method === "HEAD" ? undefined : await req.text();
  const res = await fetch(upstream, {
    method: options?.method || req.method,
    headers: options?.headers,
    body,
    cache: "no-store",
  });
  const text = await res.text();
  return new NextResponse(text, {
    status: res.status,
    headers: { "content-type": res.headers.get("content-type") || "application/json" },
  });
}
