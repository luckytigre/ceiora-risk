import { NextRequest } from "next/server";
import { backendOrigin } from "@/app/api/_backend";
import { proxyNamespace } from "@/app/api/_proxyNamespace";

type Params = Promise<{ slug: string[] }>;

async function handle(req: NextRequest, params: Params) {
  const { slug } = await params;
  const path = slug.join("/");
  const allowed = path === "theses" ? ["GET", "POST"]
    : path === "theses/capability" ? ["GET"]
    : /^theses\/[0-9a-f-]{36}$/i.test(path) ? ["PATCH", "DELETE"]
    : ["thesis-allocations/position", "manual-order"].includes(path) ? ["PUT"] : [];
  if (!allowed.length) return Response.json({ detail: "Unknown portfolio route." }, { status: 404 });
  if (!allowed.includes(req.method)) return Response.json({ detail: "Method not allowed." }, { status: 405, headers: { Allow: allowed.join(", ") } });
  return proxyNamespace(req, backendOrigin(), "portfolio", slug);
}

export async function GET(req: NextRequest, { params }: { params: Params }) { return handle(req, params); }
export async function POST(req: NextRequest, { params }: { params: Params }) { return handle(req, params); }
export async function PUT(req: NextRequest, { params }: { params: Params }) { return handle(req, params); }
export async function PATCH(req: NextRequest, { params }: { params: Params }) { return handle(req, params); }
export async function DELETE(req: NextRequest, { params }: { params: Params }) { return handle(req, params); }
