import { NextRequest } from "next/server";
import { backendOrigin, editorHeaders, proxyJson } from "../../../_backend";

export const dynamic = "force-dynamic";

export async function POST(req: NextRequest) {
  return proxyJson(req, `${backendOrigin()}/api/holdings/position/remove`, {
    method: "POST",
    headers: editorHeaders({ "content-type": "application/json" }),
  });
}
