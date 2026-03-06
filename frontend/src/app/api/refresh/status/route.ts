import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

function backendOrigin(): string {
  return (process.env.BACKEND_API_ORIGIN || "http://127.0.0.1:8000").replace(/\/+$/, "");
}

function refreshHeaders(extra: Record<string, string> = {}): HeadersInit {
  const token = (process.env.REFRESH_API_TOKEN || "").trim();
  if (!token) return extra;
  return { ...extra, "X-Refresh-Token": token };
}

export async function GET() {
  const upstream = `${backendOrigin()}/api/refresh/status`;
  const res = await fetch(upstream, {
    method: "GET",
    headers: refreshHeaders(),
    cache: "no-store",
  });
  const body = await res.text();
  return new NextResponse(body, {
    status: res.status,
    headers: { "content-type": res.headers.get("content-type") || "application/json" },
  });
}
