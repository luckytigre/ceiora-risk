import type { NextRequest } from "next/server";
import { backendOrigin, upstreamHeaders } from "@/app/api/_backend";

export interface AppAuthContextPayload {
  auth_provider: "shared" | "neon";
  subject: string;
  email: string | null;
  display_name: string | null;
  is_admin: boolean;
  account_enforcement_enabled: boolean;
  default_account_id: string | null;
  account_ids: string[];
  admin_settings_enabled: boolean;
}

export class AppAuthContextError extends Error {
  status: number;
  code: string | null;

  constructor(status: number, message: string, code?: string | null) {
    super(message);
    this.name = "AppAuthContextError";
    this.status = status;
    this.code = code ?? null;
  }
}

export async function fetchAppAuthContext(
  req: NextRequest,
  options: {
    sessionTokenOverride?: string | null;
    bestEffort?: boolean;
  } = {},
): Promise<AppAuthContextPayload | null> {
  const { sessionTokenOverride, bestEffort = false } = options;
  const upstream = `${backendOrigin()}/api/auth/context`;
  const headers = await upstreamHeaders(
    req,
    upstream,
    sessionTokenOverride ? { "x-app-session-token": sessionTokenOverride } : {},
  );
  const res = await fetch(upstream, {
    method: "GET",
    headers,
    cache: "no-store",
  });
  if (res.status === 401) return null;
  if (!res.ok) {
    if (bestEffort) return null;
    const payload = await res.json().catch(() => ({}));
    const detail = payload?.detail;
    const message =
      typeof detail === "string"
        ? detail
        : typeof detail?.message === "string"
          ? detail.message
          : "Could not load auth context.";
    const code =
      typeof detail?.code === "string"
        ? detail.code
        : typeof detail?.error === "string"
          ? detail.error
          : res.status === 409
            ? "account_provisioning_required"
            : null;
    throw new AppAuthContextError(res.status, message, code);
  }
  return (await res.json()) as AppAuthContextPayload;
}
