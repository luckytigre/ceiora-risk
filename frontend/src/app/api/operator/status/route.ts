import { NextRequest } from "next/server";
import { controlBackendOrigin, operatorHeaders, proxyJson } from "@/app/api/_backend";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  return proxyJson(req, `${controlBackendOrigin()}/api/operator/status${req.nextUrl.search}`, {
    headers: operatorHeaders(),
  });
}
