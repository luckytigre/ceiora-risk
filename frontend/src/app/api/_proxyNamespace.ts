import { NextRequest } from "next/server";
import { proxyJson } from "@/app/api/_backend";

export async function proxyNamespace(
  req: NextRequest,
  origin: string,
  namespace: string,
  slug: string[],
) {
  const upstreamPath = [namespace, ...slug].map((part) => encodeURIComponent(part)).join("/");
  return proxyJson(req, `${origin}/api/${upstreamPath}${req.nextUrl.search}`);
}
