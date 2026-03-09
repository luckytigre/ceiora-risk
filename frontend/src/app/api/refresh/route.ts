import { NextRequest, NextResponse } from "next/server";
import { backendOrigin, operatorHeaders } from "@/app/api/_backend";

export const dynamic = "force-dynamic";

export async function POST(req: NextRequest) {
  const upstream = `${backendOrigin()}/api/refresh${req.nextUrl.search}`;
  const res = await fetch(upstream, {
    method: "POST",
    headers: operatorHeaders(),
    cache: "no-store",
  });
  const body = await res.text();
  return new NextResponse(body, {
    status: res.status,
    headers: { "content-type": res.headers.get("content-type") || "application/json" },
  });
}
