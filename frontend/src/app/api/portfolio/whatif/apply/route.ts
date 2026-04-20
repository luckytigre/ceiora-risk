import { NextRequest } from "next/server";
import { backendOrigin, proxyJson } from "@/app/api/_backend";

export const dynamic = "force-dynamic";

export async function POST(req: NextRequest) {
  return proxyJson(req, `${backendOrigin()}/api/portfolio/whatif/apply`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    forwardPrivilegedHeaders: false,
  });
}
