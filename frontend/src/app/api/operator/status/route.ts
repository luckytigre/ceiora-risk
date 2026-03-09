import { NextRequest } from "next/server";
import { backendOrigin, operatorHeaders, proxyJson } from "@/app/api/_backend";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  return proxyJson(req, `${backendOrigin()}/api/operator/status${req.nextUrl.search}`, {
    headers: operatorHeaders(),
  });
}
