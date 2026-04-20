import { NextRequest } from "next/server";
import { backendOrigin, proxyJson } from "@/app/api/_backend";

export async function GET(req: NextRequest) {
  return proxyJson(req, `${backendOrigin()}/api/cuse/risk-page/covariance${req.nextUrl.search}`);
}
