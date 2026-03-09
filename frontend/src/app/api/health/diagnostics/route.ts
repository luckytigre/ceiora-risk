import { NextRequest } from "next/server";
import { backendOrigin, operatorHeaders, proxyJson } from "../../_backend";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  return proxyJson(req, `${backendOrigin()}/api/health/diagnostics${req.nextUrl.search}`, {
    method: "GET",
    headers: operatorHeaders(),
  });
}
