import { NextRequest } from "next/server";
import { backendOrigin } from "@/app/api/_backend";
import { proxyNamespace } from "@/app/api/_proxyNamespace";

type Params = Promise<{ slug: string[] }>;

async function handle(req: NextRequest, params: Params) {
  const { slug } = await params;
  return proxyNamespace(req, backendOrigin(), "holdings", slug);
}

export async function GET(req: NextRequest, { params }: { params: Params }) {
  return handle(req, params);
}

export async function POST(req: NextRequest, { params }: { params: Params }) {
  return handle(req, params);
}

export async function PUT(req: NextRequest, { params }: { params: Params }) {
  return handle(req, params);
}

export async function PATCH(req: NextRequest, { params }: { params: Params }) {
  return handle(req, params);
}

export async function DELETE(req: NextRequest, { params }: { params: Params }) {
  return handle(req, params);
}
