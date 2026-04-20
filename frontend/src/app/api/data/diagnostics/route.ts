import { NextRequest } from "next/server";
import { controlBackendOrigin, proxyJson } from "../../_backend";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  return proxyJson(req, `${controlBackendOrigin()}/api/data/diagnostics${req.nextUrl.search}`, {
    method: "GET",
    forwardPrivilegedHeaders: true,
  });
}
