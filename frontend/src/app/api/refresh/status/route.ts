import { NextResponse } from "next/server";
import { controlBackendOrigin, operatorHeaders } from "@/app/api/_backend";

export const dynamic = "force-dynamic";

export async function GET() {
  const upstream = `${controlBackendOrigin()}/api/refresh/status`;
  const res = await fetch(upstream, {
    method: "GET",
    headers: operatorHeaders(),
    cache: "no-store",
  });
  const body = await res.text();
  return new NextResponse(body, {
    status: res.status,
    headers: { "content-type": res.headers.get("content-type") || "application/json" },
  });
}
