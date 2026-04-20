import { NextRequest } from "next/server";
import { backendOrigin, proxyJson } from "../../../_backend";

export const dynamic = "force-dynamic";

export async function POST(req: NextRequest) {
  return proxyJson(req, `${backendOrigin()}/api/holdings/position/remove`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    forwardPrivilegedHeaders: false,
  });
}
