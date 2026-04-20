import { NextRequest } from "next/server";
import { backendOrigin } from "@/app/api/_backend";
import { proxyNamespace } from "@/app/api/_proxyNamespace";

type Params = Promise<{ slug: string[] }>;

async function handle(req: NextRequest, params: Params) {
  const { slug } = await params;
  return proxyNamespace(req, backendOrigin(), "universe", slug);
}

export async function GET(req: NextRequest, { params }: { params: Params }) {
  return handle(req, params);
}
