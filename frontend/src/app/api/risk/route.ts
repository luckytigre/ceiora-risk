import { NextRequest } from "next/server";
import { backendOrigin, proxyJson } from "../_backend";

export async function GET(req: NextRequest) {
  return proxyJson(req, `${backendOrigin()}/api/risk${req.nextUrl.search}`);
}
